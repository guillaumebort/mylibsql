mod ack;
mod db;
mod log;
mod snapshot;
mod wal;

use std::{cmp::Reverse, collections::BinaryHeap, future::Future, ops::Range, sync::Arc};

pub use ack::Ack;
use ack::PendingAck;
use anyhow::Result;
use db::MylibsqlDB;
use futures::future::BoxFuture;
use libsql::replication::{Frame, FrameNo};
pub use libsql_sys::rusqlite;
pub use log::Log;
use parking_lot::Mutex;
pub use snapshot::Snapshot;
use tokio::sync::{mpsc, Semaphore};

struct Version([u16; 4]);

impl Version {
    fn current() -> Self {
        let major = env!("CARGO_PKG_VERSION_MAJOR").parse().unwrap();
        let minor = env!("CARGO_PKG_VERSION_MINOR").parse().unwrap();
        let patch = env!("CARGO_PKG_VERSION_PATCH").parse().unwrap();
        Self([0, major, minor, patch])
    }
}

pub async fn init(
    init: impl FnOnce(&rusqlite::Connection) -> Result<()> + Send + Sync + 'static,
) -> Result<(Snapshot, Log)> {
    MylibsqlDB::init(init).await
}

pub struct Frames {
    rx: mpsc::UnboundedReceiver<(Log, Range<FrameNo>)>,
    pending_acks: Arc<Mutex<BinaryHeap<Reverse<PendingAck>>>>,
}

impl Frames {
    pub async fn next(&mut self) -> Result<Option<Vec<Frame>>> {
        if let Some((log, frames_no)) = self.rx.recv().await {
            let mut frames = Vec::with_capacity(frames_no.size_hint().0);
            for frame_no in frames_no {
                frames.push(log.read_frame(frame_no).await?);
            }
            Ok(Some(frames))
        } else {
            Ok(None)
        }
    }

    pub async fn try_next(&mut self) -> Result<Vec<Frame>> {
        let (log, frames_no) = self.rx.try_recv()?;
        let mut frames = Vec::with_capacity(frames_no.size_hint().0);
        for frame_no in frames_no {
            frames.push(log.read_frame(frame_no).await?);
        }
        Ok(frames)
    }

    pub fn ack_replicated(&self, last_replicated_frame_no: FrameNo) {
        let mut pending_acks = self.pending_acks.lock();
        ack_pending(&mut pending_acks, last_replicated_frame_no);
    }
}

fn ack_pending(pending_acks: &mut BinaryHeap<Reverse<PendingAck>>, safe_frame_no: FrameNo) {
    loop {
        if let Some(Reverse(pending_ack)) = pending_acks.peek() {
            if pending_ack.is_ready(safe_frame_no) {
                let Reverse(pending_ack) = pending_acks.pop().unwrap();
                if let Err(_e) = pending_ack.ack() {
                    // TODO: log warning?
                }
            } else {
                break;
            }
        } else {
            break;
        }
    }
}

#[derive(Debug)]
pub struct Primary {
    db: MylibsqlDB,
    pending_acks: Arc<Mutex<BinaryHeap<Reverse<PendingAck>>>>,
    frames_tx: Option<mpsc::UnboundedSender<(Log, Range<FrameNo>)>>,
    checkpoint_semaphore: Arc<Semaphore>,
}

impl Primary {
    pub async fn open(last_snapshot: Snapshot, additional_logs: Vec<Log>) -> Result<Self> {
        let mut db = MylibsqlDB::open(last_snapshot).await?;
        for log in additional_logs {
            db.inject_log(log).await?;
        }
        Ok(Self {
            db,
            pending_acks: Arc::new(Mutex::new(BinaryHeap::new())),
            frames_tx: None,
            checkpoint_semaphore: Arc::new(Semaphore::new(1)),
        })
    }

    pub fn capture_frames(&mut self) -> Frames {
        let (tx, rx) = mpsc::unbounded_channel();
        self.frames_tx = Some(tx);
        Frames {
            rx,
            pending_acks: self.pending_acks.clone(),
        }
    }

    pub async fn checkpoint(
        &self,
        save_log: impl FnOnce(Log) -> BoxFuture<'static, ()> + Send + 'static,
    ) -> Result<impl Future<Output = Option<FrameNo>>> {
        let pending_acks = self.pending_acks.clone();
        let save_checkpoint = self.db.checkpoint(save_log).await?;
        let checkpoint_permit = self.checkpoint_semaphore.clone().acquire_owned().await;
        Ok(async move {
            let result = save_checkpoint.await;
            if let Some(checkpointed_frame_no) = &result {
                let mut pending_acks = pending_acks.lock();
                ack_pending(&mut pending_acks, *checkpointed_frame_no);
            }
            drop(checkpoint_permit);
            result
        })
    }

    pub async fn with_connection<A>(
        &self,
        f: impl FnOnce(&mut rusqlite::Connection) -> Result<A> + Send + 'static,
    ) -> Result<Ack<A>>
    where
        A: Send + Unpin + 'static,
    {
        let (a, log, frames_no) = self.db.with_rw_connection(f).await?;
        if let Some(last_frame_no) = frames_no.clone().last() {
            let (ack, pending_ack) = Ack::new_pending(a, last_frame_no);
            self.pending_acks.lock().push(Reverse(pending_ack));
            if let Some(frames_tx) = &self.frames_tx {
                let _ = frames_tx.send((log, frames_no));
            }
            Ok(ack)
        } else {
            Ok(Ack::new_ready(a))
        }
    }

    pub fn into_replica(self) -> Result<Replica, (anyhow::Error, Primary)> {
        if self.pending_acks.lock().is_empty() {
            Ok(Replica { db: self.db })
        } else {
            Err((
                anyhow::anyhow!("this primary database has non checkpointed writes"),
                self,
            ))
        }
    }
}

#[derive(Debug)]
pub struct Replica {
    db: MylibsqlDB,
}

impl Replica {
    pub async fn open(last_snapshot: Snapshot, additional_logs: Vec<Log>) -> Result<Self> {
        let mut db = MylibsqlDB::open(last_snapshot).await?;
        for log in additional_logs {
            db.inject_log(log).await?;
        }
        Ok(Self { db })
    }

    pub async fn with_connection<A>(
        &self,
        f: impl FnOnce(&mut rusqlite::Connection) -> Result<A> + Send + 'static,
    ) -> Result<A>
    where
        A: Send + 'static,
    {
        self.db.with_ro_connection(f).await
    }

    pub async fn replicate(&mut self, log: Log) -> Result<()> {
        self.db.inject_log(log).await
    }

    pub fn into_primary(self) -> Result<Primary> {
        Ok(Primary {
            db: self.db,
            pending_acks: Arc::new(Mutex::new(BinaryHeap::new())),
            frames_tx: None,
            checkpoint_semaphore: Arc::new(Semaphore::new(1)),
        })
    }
}
