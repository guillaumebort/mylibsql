use super::*;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use futures::future::BoxFuture;
use libsql::replication::Frame;
use libsql_replication::injector::{Injector, SqliteInjector};
use libsql_sys::{
    connection::NO_AUTOCHECKPOINT,
    rusqlite::OpenFlags,
    wal::{wrapper::WrappedWal, Sqlite3Wal},
    Connection,
};
use log::Log;
use parking_lot::Mutex;
use snapshot::Snapshot;
use tempfile::NamedTempFile;
use wal::MylibsqlWal;

struct DB {
    conn: Arc<Mutex<Connection<WrappedWal<MylibsqlWal, Sqlite3Wal>>>>,
    wal: Option<MylibsqlWal>,
    injector: Option<SqliteInjector>,
    next_frame_no: Option<u64>,
}

impl DB {
    async fn init(
        init: impl FnOnce(&rusqlite::Connection) -> Result<()> + Send + 'static,
    ) -> Result<(Snapshot, Log)> {
        tokio::task::spawn_blocking(move || {
            let db = NamedTempFile::new()?;
            let (wal, wal_manager) = MylibsqlWal::new(0)?;
            let conn = Connection::open(
                db.path(),
                OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
                wal_manager,
                NO_AUTOCHECKPOINT,
                None,
            )?;
            init(&conn)?;
            Self::wal_checkpoint(&conn)?;
            drop(conn);
            let log = wal.into_log()?;
            Ok((
                Snapshot {
                    path: Box::new(db),
                    last_frame_no: log.header.last_frame_no(),
                },
                log,
            ))
        })
        .await?
    }

    async fn open(last_snapshot: Snapshot) -> Result<Self> {
        let db = (*last_snapshot.path).as_ref().to_path_buf();
        let injector = SqliteInjector::new(db.clone(), 4096, NO_AUTOCHECKPOINT, None).await?;
        let db = tokio::task::spawn_blocking(move || {
            let (wal, wal_manager) = MylibsqlWal::new(
                last_snapshot
                    .last_frame_no
                    .map(|f| f + 1)
                    .unwrap_or_default(),
            )?;
            let conn = Connection::open(
                &db,
                OpenFlags::SQLITE_OPEN_READ_WRITE,
                wal_manager,
                NO_AUTOCHECKPOINT,
                None,
            )?;
            anyhow::Ok(DB {
                conn: Arc::new(Mutex::new(conn)),
                wal: Some(wal),
                injector: Some(injector),
                next_frame_no: None,
            })
        })
        .await??;
        Ok(db)
    }

    async fn inject_log(&mut self, additional_log: Log) -> Result<()> {
        let additional_log_start_frame_no = additional_log.header().start_frame_no.get();
        if let (Some(wal), Some(injector)) = (&self.wal, &mut self.injector) {
            {
                let log = wal.log();
                let expected_frame_no = log.next_frame_no();
                if !log.is_empty() {
                    bail!("current log is not empty");
                }
                if additional_log_start_frame_no != expected_frame_no {
                    bail!("log does not start at the expected frame number (expected {expected_frame_no}, got {additional_log_start_frame_no})");
                }
            }
            for frame in additional_log.frames_iter()? {
                Self::inject_frame(injector, frame?).await?;
            }
            *wal.log() = Log::new(additional_log.next_frame_no())?;
            injector.flush().await?;
            Ok(())
        } else if let (Some(next_frame_no), Some(injector)) =
            (&mut self.next_frame_no, &mut self.injector)
        {
            if additional_log_start_frame_no != *next_frame_no {
                bail!("log does not start at the expected frame number (expected {next_frame_no}, got {additional_log_start_frame_no})");
            }
            for frame in additional_log.frames_iter()? {
                Self::inject_frame(injector, frame?).await?;
            }
            *next_frame_no = additional_log.next_frame_no();
            injector.flush().await?;
            Self::wal_checkpoint(&self.conn.lock())?;
            Ok(())
        } else {
            bail!("invalid state, cannot inject log")
        }
    }

    async fn inject_frame(injector: &mut SqliteInjector, frame: Frame) -> Result<()> {
        injector
            .inject_frame(libsql_replication::rpc::replication::Frame {
                data: frame.bytes(),
                ..Default::default()
            })
            .await?;
        Ok(())
    }

    fn wal_checkpoint(conn: &rusqlite::Connection) -> Result<()> {
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", (), |row| {
            let status: i32 = row.get(0)?;
            let wal_frames: i32 = row.get(1)?;
            let moved_frames: i32 = row.get(2)?;
            tracing::trace!(
                "WAL checkpoint successful, status: {}, WAL frames: {}, moved frames: {}",
                status,
                wal_frames,
                moved_frames
            );
            Ok(())
        })?;
        Ok(())
    }

    async fn test(&self) -> Result<()> {
        let result: usize = self
            .with_connection(|conn| conn.query_row("select 12", (), |row| row.get(0)))
            .await?;
        if result == 12 {
            Ok(())
        } else {
            Err(anyhow!("WAT? funny connection"))
        }
    }

    async fn with_connection<A>(
        &self,
        f: impl FnOnce(&rusqlite::Connection) -> Result<A, libsql_sys::rusqlite::Error> + Send + 'static,
    ) -> Result<A>
    where
        A: Send + 'static,
    {
        let conn = self.conn.clone();
        Ok(tokio::task::spawn_blocking(move || f(&*conn.lock())).await??)
    }

    async fn checkpoint(&self, f: impl FnOnce(Log) -> BoxFuture<'static, ()>) -> Result<()> {
        if let Some(wal) = &self.wal {
            let mut log = wal.log();
            Self::wal_checkpoint(&self.conn.lock())?;
            let next_log = Log::new(log.next_frame_no())?;
            let old_log = std::mem::replace(&mut *log, next_log);
            f(old_log).await;
            Ok(())
        } else {
            Err(anyhow!("nothing to checkpoint"))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use futures::FutureExt;
    use libsql_sys::wal::Sqlite3WalManager;

    use super::*;

    fn blank_db() -> Result<Snapshot> {
        let db = NamedTempFile::new()?;
        let wal_manager = Sqlite3WalManager::new();
        Connection::open(
            db.path(),
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
            wal_manager,
            NO_AUTOCHECKPOINT,
            None,
        )?;
        Ok(Snapshot {
            path: Box::new(db),
            last_frame_no: None,
        })
    }

    #[tokio::test]
    async fn create_blank_database() -> Result<()> {
        let (snapshot, log) = DB::init(|_| Ok(())).await?;
        assert!(snapshot.last_frame_no.is_none());
        assert!(log.last_commited_frame_no().is_none());
        assert_eq!(log.next_frame_no(), 0);

        // reopen from snapshot
        let db = DB::open(snapshot).await?;
        db.test().await?;

        // reopen from log
        let mut db = DB::open(blank_db()?).await?;
        db.inject_log(log).await?;
        db.test().await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_initial_database() -> Result<()> {
        let (snapshot, log) = DB::init(|conn| {
            conn.execute("create table lol(x integer)", ())?;
            conn.execute("insert into lol values (1)", ())?;
            Ok(())
        })
        .await?;
        assert!(snapshot.last_frame_no.is_some());
        assert!(log.last_commited_frame_no().is_some());
        assert_eq!(log.next_frame_no(), 3);

        // reopen from snapshot
        let db = DB::open(snapshot).await?;
        db.test().await?;
        let count: usize = db
            .with_connection(|conn| {
                conn.query_row("select count(*) from lol", (), |row| row.get(0))
            })
            .await?;
        assert_eq!(count, 1);

        // reopen from log
        let mut db = DB::open(blank_db()?).await?;
        db.inject_log(log).await?;
        db.test().await?;
        let count: usize = db
            .with_connection(|conn| {
                conn.query_row("select count(*) from lol", (), |row| row.get(0))
            })
            .await?;
        assert_eq!(count, 1);

        Ok(())
    }

    #[tokio::test]
    async fn checkpoints() -> Result<()> {
        let logs_store = Arc::new(Mutex::new(VecDeque::new()));
        let save_log = {
            let logs_store = logs_store.clone();
            move |log| {
                let logs_store = logs_store.clone();
                async move {
                    logs_store.lock().push_back(log);
                }
                .boxed()
            }
        };

        let db = DB::open(blank_db()?).await?;

        // first checkpoint (create table)
        db.with_connection(|conn| conn.execute("create table boo(x string)", ()))
            .await?;
        db.checkpoint(&save_log).await?;

        // second checkpoint (insert data)
        db.with_connection(|conn| conn.execute("insert into boo values ('YO')", ()))
            .await?;
        db.checkpoint(&save_log).await?;

        // third checkpoint (update data)
        db.with_connection(|conn| conn.execute("update boo set x = 'YOO'", ()))
            .await?;
        db.checkpoint(&save_log).await?;

        // fourth checkpoint (delete data)
        db.with_connection(|conn| conn.execute("delete from boo", ()))
            .await?;
        db.checkpoint(&save_log).await?;

        drop(save_log);
        let mut logs_store = Arc::into_inner(logs_store).unwrap().into_inner();
        assert_eq!(logs_store.len(), 4);

        // restart with a new db
        let mut db = DB::open(blank_db()?).await?;

        // apply first checkpoint
        db.inject_log(logs_store.pop_front().unwrap()).await?;
        let count: usize = db
            .with_connection(|conn| {
                conn.query_row("select count(*) from boo", (), |row| row.get(0))
            })
            .await?;
        assert_eq!(count, 0);

        // apply second checkpoint
        db.inject_log(logs_store.pop_front().unwrap()).await?;
        let yo: String = db
            .with_connection(|conn| conn.query_row("select x from boo", (), |row| row.get(0)))
            .await?;
        assert_eq!(yo, "YO");

        // apply third checkpoint
        db.inject_log(logs_store.pop_front().unwrap()).await?;
        let yo: String = db
            .with_connection(|conn| conn.query_row("select x from boo", (), |row| row.get(0)))
            .await?;
        assert_eq!(yo, "YOO");

        // apply fourth checkpoint
        db.inject_log(logs_store.pop_front().unwrap()).await?;
        let count: usize = db
            .with_connection(|conn| {
                conn.query_row("select count(*) from boo", (), |row| row.get(0))
            })
            .await?;
        assert_eq!(count, 0);

        Ok(())
    }
}
