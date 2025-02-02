//! mylibsql is a personal take on [libsql](https://github.com/tursodatabase/libsql), built for full control over
//! SQLite replication—without a dedicated server. It uses **libsql’s virtual WAL** to support a **primary-replica**
//! model where applications handle their own checkpointing and WAL log storage.
//!
//! ## Basic Usage
//!
//! ```rust
//! use mylibsql::*;
//! use anyhow::Result;
//!
//! async fn example() -> Result<()> {
//!
//!     // Initialize a new database
//!     let (snapshot, log) = init(|conn| {
//!         conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])?;
//!         Ok(())
//!     }).await?;
//!
//!     // Snapshot and logs are supposed to be stored durably for later use
//!     todo!("store the snapshot and initial log durably somewhere for later use");
//!
//!     // Open a primary database from the latest snapshot
//!     // Note: we don't specify the original log again, as it's already included in the snapshot
//!     // (but you can use it to recreate the first snapshot from a blank database)
//!     let primary = Primary::open(&snapshot, &[]).await?;
//!
//!     // Write to the primary database
//!     let ack = primary.with_connection(|conn| {
//!         conn.execute("INSERT INTO users (name) VALUES (?)", ["Alice"])?;
//!         Ok(())
//!     }).await?;
//!
//!     ack.await; // Ensures the write is safely checkpointed
//!
//!     // Note: you can also peek the query result before it is checkpointed if you want
//!     dbg!(ack.peek());
//!
//!     // Checkpoint the database by saving the log to a durable storage
//!     primary.checkpoint(|log| {
//!         Box::pin(async move {
//!             todo!("save log to storage");
//!         })
//!     }).await?;
//!
//!     // Open a read-only replica from the snapshot and logs
//!     let (last_snapshot, logs): (Snapshot, Vec<Log>) = todo!("fetch snapshot and logs from somewhere");
//!     let mut replica = Replica::open(&last_snapshot, &logs).await?;
//!
//!     // Query the replica
//!     let count: i32 = replica.with_connection(|conn| {
//!        Ok(conn.query_row("SELECT COUNT(*) FROM users", [], |row| row.get(0))?)
//!     }).await?;
//!
//!     // When you receive a new log, you can apply it to the replica
//!     let new_log = todo!("fetch log from somewhere");
//!     replica.replicate(&new_log).await?;
//!
//!     Ok(())
//! }
//! ```

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

/// Initializes a new database by running the provided initialization script and returns a snapshot and the first Log.
/// Note: The snapshot already includes the initialization script, so the log should not be reapplied.
pub async fn init(
    init: impl FnOnce(&rusqlite::Connection) -> Result<()> + Send + Sync + 'static,
) -> Result<(Snapshot, Log)> {
    MylibsqlDB::init(init).await
}
/// Live stream of frames from the primary database.
/// Useful if you need a higher granularity than the checkpointing mechanism.
pub struct Frames {
    rx: mpsc::UnboundedReceiver<(Log, Range<FrameNo>)>,
    pending_acks: Arc<Mutex<BinaryHeap<Reverse<PendingAck>>>>,
}

impl Frames {
    /// Waits and returns the next batch of frames, or `None` if the database is closed.
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

    /// Returns the next batch of frames if available, or an Error if there is no new frames availabe.
    pub async fn try_next(&mut self) -> Result<Vec<Frame>> {
        let (log, frames_no) = self.rx.try_recv()?;
        let mut frames = Vec::with_capacity(frames_no.size_hint().0);
        for frame_no in frames_no {
            frames.push(log.read_frame(frame_no).await?);
        }
        Ok(frames)
    }

    /// Acknowledges that the frames up to the given frame number have been replicated.
    /// This will resolve any pending acks waiting for these frames to be safely replicated.
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

/// A primary database with read-write access
#[derive(Debug)]
pub struct Primary {
    db: MylibsqlDB,
    pending_acks: Arc<Mutex<BinaryHeap<Reverse<PendingAck>>>>,
    frames_tx: Option<mpsc::UnboundedSender<(Log, Range<FrameNo>)>>,
    checkpoint_semaphore: Arc<Semaphore>,
}

impl Primary {
    /// Opens a primary database with the given snapshot and additional logs.
    pub async fn open(last_snapshot: &Snapshot, additional_logs: &[Log]) -> Result<Self> {
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

    /// Captures a live stream of frames from the primary database.
    pub fn capture_frames(&mut self) -> Frames {
        let (tx, rx) = mpsc::unbounded_channel();
        self.frames_tx = Some(tx);
        Frames {
            rx,
            pending_acks: self.pending_acks.clone(),
        }
    }

    /// Checkpoints the primary database and returns a future that resolves to the last safely written frame number.
    /// The `save_log` function should save the log to a durable storage, is async, and is not allowed to fail.
    /// Checkpoints are exclusive, so only one checkpoint can be in progress at a time.
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

    /// Opens a read-write connection to the primary database and runs the provided function.
    /// The provided connection uses the rusqlite API.
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

    /// Converts the primary database into a replica.
    /// Fail if not all writes have been checkpointed yet.
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

/// A replica database with read-only access
#[derive(Debug)]
pub struct Replica {
    db: MylibsqlDB,
}

impl Replica {
    /// Opens a replica database with the given snapshot and additional logs.
    pub async fn open(last_snapshot: &Snapshot, additional_logs: &[Log]) -> Result<Self> {
        let mut db = MylibsqlDB::open(last_snapshot).await?;
        for log in additional_logs {
            db.inject_log(log).await?;
        }
        Ok(Self { db })
    }

    /// Opens a read-only connection to the replica database and runs the provided function.
    /// The provided connection uses the rusqlite API.
    pub async fn with_connection<A>(
        &self,
        f: impl FnOnce(&mut rusqlite::Connection) -> Result<A> + Send + 'static,
    ) -> Result<A>
    where
        A: Send + 'static,
    {
        self.db.with_ro_connection(f).await
    }

    /// Replicates a new log into the replica database.
    pub async fn replicate(&mut self, log: &Log) -> Result<()> {
        self.db.inject_log(&log).await
    }

    /// Converts the replica database into a primary.
    pub fn into_primary(self) -> Result<Primary> {
        Ok(Primary {
            db: self.db,
            pending_acks: Arc::new(Mutex::new(BinaryHeap::new())),
            frames_tx: None,
            checkpoint_semaphore: Arc::new(Semaphore::new(1)),
        })
    }
}
