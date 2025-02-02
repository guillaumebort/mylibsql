mod ack;
mod db;
mod log;
mod snapshot;
mod wal;

use std::{cmp::Reverse, collections::BinaryHeap};

pub use ack::Ack;
use ack::PendingAck;
use anyhow::Result;
use db::MylibsqlDB;
use futures::future::BoxFuture;
pub use libsql_sys::rusqlite;
pub use log::Log;
use parking_lot::Mutex;
pub use snapshot::Snapshot;

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
    init: impl FnOnce(&rusqlite::Connection) -> Result<()> + Send + 'static,
) -> Result<(Snapshot, Log)> {
    MylibsqlDB::init(init).await
}

#[derive(Debug)]
pub struct Primary {
    db: MylibsqlDB,
    pending_acks: Mutex<BinaryHeap<Reverse<PendingAck>>>,
}

impl Primary {
    pub async fn open(last_snapshot: Snapshot, additional_logs: Vec<Log>) -> Result<Self> {
        let mut db = MylibsqlDB::open(last_snapshot).await?;
        for log in additional_logs {
            db.inject_log(log).await?;
        }
        Ok(Self {
            db,
            pending_acks: Mutex::new(BinaryHeap::new()),
        })
    }

    pub async fn checkpoint(
        &self,
        save_log: impl FnOnce(Log) -> BoxFuture<'static, ()>,
    ) -> Result<()> {
        let checkpointed_frame_no = self.db.checkpoint(save_log).await?;
        if let Some(checkpointed_frame_no) = checkpointed_frame_no {
            let mut pending_acks = self.pending_acks.lock();
            loop {
                if let Some(Reverse(pending_ack)) = pending_acks.peek() {
                    if pending_ack.is_ready(checkpointed_frame_no) {
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
        Ok(())
    }

    pub async fn with_connection<A>(
        &self,
        f: impl FnOnce(&rusqlite::Connection) -> Result<A, libsql_sys::rusqlite::Error> + Send + 'static,
    ) -> Result<Ack<A>>
    where
        A: Send + Unpin + 'static,
    {
        let start_frame_no = self.db.current_frame_no()?;
        let a = self.db.with_rw_connection(f).await?;
        let end_frame_no = self.db.current_frame_no()?;
        if start_frame_no == end_frame_no {
            Ok(Ack::new_ready(a))
        } else if let Some(end_frame_no) = end_frame_no {
            let (ack, pending_ack) = Ack::new_pending(a, end_frame_no);
            self.pending_acks.lock().push(Reverse(pending_ack));
            Ok(ack)
        } else {
            unreachable!()
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
        f: impl FnOnce(&rusqlite::Connection) -> Result<A, libsql_sys::rusqlite::Error> + Send + 'static,
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
            pending_acks: Mutex::new(BinaryHeap::new()),
        })
    }
}
