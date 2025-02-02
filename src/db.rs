use super::*;
use std::{fmt::Debug, path::PathBuf, sync::Arc};

use anyhow::{anyhow, bail, Result};
use futures::future::BoxFuture;
use libsql::replication::{Frame, FrameNo};
use libsql_replication::injector::{Injector, SqliteInjector};
use libsql_sys::{
    connection::NO_AUTOCHECKPOINT,
    rusqlite::OpenFlags,
    wal::{wrapper::WrappedWal, Sqlite3Wal, Sqlite3WalManager},
    Connection,
};
use log::Log;
use parking_lot::Mutex;
use snapshot::Snapshot;
use tempfile::NamedTempFile;
use wal::ShadowWal;

pub struct MylibsqlDB {
    path: PathBuf,
    shadow_wal: ShadowWal,
    injector: SqliteInjector,
    rw_conn: Arc<Mutex<Connection<WrappedWal<ShadowWal, Sqlite3Wal>>>>,
    ro_conn: Arc<Mutex<Connection<Sqlite3Wal>>>,
}

impl Debug for MylibsqlDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MylibsqlDB")
            .field("path", &self.path)
            .finish()
    }
}

impl MylibsqlDB {
    pub async fn init(
        init: impl FnOnce(&rusqlite::Connection) -> Result<()> + Send + 'static,
    ) -> Result<(Snapshot, Log)> {
        let (wal, wal_manager) = ShadowWal::new(0).await?;
        tokio::task::spawn_blocking(move || {
            let db = NamedTempFile::new()?;
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

    pub async fn open(last_snapshot: Snapshot) -> Result<Self> {
        let path = (*last_snapshot.path).as_ref().to_path_buf();
        let injector = SqliteInjector::new(path.clone(), 4096, NO_AUTOCHECKPOINT, None).await?;
        let (shadow_wal, wal_manager) = ShadowWal::new(
            last_snapshot
                .last_frame_no
                .map(|f| f + 1)
                .unwrap_or_default(),
        )
        .await?;
        let db = tokio::task::spawn_blocking(move || {
            let rw_conn = Connection::open(
                &path,
                OpenFlags::SQLITE_OPEN_READ_WRITE,
                wal_manager,
                NO_AUTOCHECKPOINT,
                None,
            )?;
            let ro_conn = Connection::open(
                &path,
                OpenFlags::SQLITE_OPEN_READ_ONLY,
                Sqlite3WalManager::new(),
                NO_AUTOCHECKPOINT,
                None,
            )?;
            anyhow::Ok(MylibsqlDB {
                path,
                rw_conn: Arc::new(Mutex::new(rw_conn)),
                ro_conn: Arc::new(Mutex::new(ro_conn)),
                shadow_wal,
                injector,
            })
        })
        .await??;
        Ok(db)
    }

    pub async fn inject_log(&mut self, additional_log: Log) -> Result<()> {
        let additional_log_start_frame_no = additional_log.header().start_frame_no.get();
        {
            let log = self.shadow_wal.log();
            let expected_frame_no = log.next_frame_no();
            if !log.is_empty() {
                bail!("current log is not empty");
            }
            if additional_log_start_frame_no != expected_frame_no {
                bail!("log does not start at the expected frame number (expected {expected_frame_no}, got {additional_log_start_frame_no})");
            }
        }
        for frame in additional_log.frames_iter()? {
            Self::inject_frame(&mut self.injector, frame?).await?;
        }
        *self.shadow_wal.log() =
            tokio::task::spawn_blocking(move || Log::new(additional_log.next_frame_no())).await??;
        self.injector.flush().await?;
        Self::wal_checkpoint(&*self.rw_conn.lock())?;
        Ok(())
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
        // TODO: TRUNCATE here is likely going to fail in some cases, we need to revisit this
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

    pub async fn with_rw_connection<A>(
        &self,
        f: impl FnOnce(&mut rusqlite::Connection) -> Result<A> + Send + 'static,
    ) -> Result<A>
    where
        A: Send + 'static,
    {
        let conn = self.rw_conn.clone();
        Ok(tokio::task::spawn_blocking(move || f(&mut *conn.lock())).await??)
    }

    pub async fn with_ro_connection<A>(
        &self,
        f: impl FnOnce(&mut rusqlite::Connection) -> Result<A> + Send + 'static,
    ) -> Result<A>
    where
        A: Send + 'static,
    {
        let conn = self.ro_conn.clone();
        Ok(tokio::task::spawn_blocking(move || f(&mut *conn.lock())).await??)
    }

    pub async fn checkpoint<'a>(
        &'a self,
        f: impl FnOnce(Log) -> BoxFuture<'a, ()>,
    ) -> Result<Option<FrameNo>> {
        let old_log = self.shadow_wal.swap_log()?; // TODO: swap_log is blocking
        Self::wal_checkpoint(&*self.rw_conn.lock())?; // TODO: this is blocking as well
        let last_frame_no = old_log.last_commited_frame_no();
        f(old_log).await;
        Ok(last_frame_no)
    }

    pub fn current_frame_no(&self) -> Result<Option<FrameNo>> {
        let log = self.shadow_wal.log();
        if log.has_uncommitted_frames() {
            Err(anyhow!("in the middle of a transaction"))
        } else {
            Ok(log.last_commited_frame_no())
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
        let (snapshot, log) = MylibsqlDB::init(|_| Ok(())).await?;
        assert!(snapshot.last_frame_no.is_none());
        assert!(log.last_commited_frame_no().is_none());
        assert_eq!(log.next_frame_no(), 0);

        // reopen from snapshot
        let _ = MylibsqlDB::open(snapshot).await?;

        // reopen from log
        let mut db = MylibsqlDB::open(blank_db()?).await?;
        db.inject_log(log).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_initial_database() -> Result<()> {
        let (snapshot, log) = MylibsqlDB::init(|conn| {
            conn.execute("create table lol(x integer)", ())?;
            conn.execute("insert into lol values (1)", ())?;
            Ok(())
        })
        .await?;
        assert!(snapshot.last_frame_no.is_some());
        assert!(log.last_commited_frame_no().is_some());
        assert_eq!(log.next_frame_no(), 3);

        // reopen from snapshot
        let db = MylibsqlDB::open(snapshot).await?;
        let count: usize = db
            .with_rw_connection(|conn| {
                Ok(conn.query_row("select count(*) from lol", (), |row| row.get(0))?)
            })
            .await?;
        assert_eq!(count, 1);
        let count: usize = db
            .with_ro_connection(|conn| {
                Ok(conn.query_row("select count(*) from lol", (), |row| row.get(0))?)
            })
            .await?;
        assert_eq!(count, 1);

        // reopen from log
        let mut db = MylibsqlDB::open(blank_db()?).await?;
        db.inject_log(log).await?;
        let count: usize = db
            .with_rw_connection(|conn| {
                Ok(conn.query_row("select count(*) from lol", (), |row| row.get(0))?)
            })
            .await?;
        assert_eq!(count, 1);
        let count: usize = db
            .with_ro_connection(|conn| {
                Ok(conn.query_row("select count(*) from lol", (), |row| row.get(0))?)
            })
            .await?;
        assert_eq!(count, 1);

        Ok(())
    }

    #[tokio::test]
    async fn with_ro_connection() -> Result<()> {
        let db = MylibsqlDB::open(blank_db()?).await?;
        assert!(db
            .with_ro_connection(|conn| Ok(conn.execute("create table lol(x integer)", ())?))
            .await
            .is_err());
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

        let db = MylibsqlDB::open(blank_db()?).await?;
        assert_eq!(None, db.checkpoint(&save_log).await?);

        // first checkpoint (create table)
        db.with_rw_connection(|conn| Ok(conn.execute("create table boo(x string)", ())?))
            .await?;
        assert_eq!(Some(1), db.checkpoint(&save_log).await?);

        // second checkpoint (insert data)
        db.with_rw_connection(|conn| Ok(conn.execute("insert into boo values ('YO')", ())?))
            .await?;
        assert_eq!(Some(2), db.checkpoint(&save_log).await?);

        // third checkpoint (update data)
        db.with_rw_connection(|conn| Ok(conn.execute("update boo set x = 'YOO'", ())?))
            .await?;
        assert_eq!(Some(3), db.checkpoint(&save_log).await?);

        // fourth checkpoint (delete data)
        db.with_rw_connection(|conn| Ok(conn.execute("delete from boo", ())?))
            .await?;
        assert_eq!(Some(4), db.checkpoint(&save_log).await?);

        drop(save_log);
        let mut logs_store = Arc::into_inner(logs_store).unwrap().into_inner();
        assert_eq!(logs_store.len(), 5);

        // restart with a new db
        let mut db = MylibsqlDB::open(blank_db()?).await?;
        db.inject_log(logs_store.pop_front().unwrap()).await?; // this one is blank

        // apply first checkpoint
        db.inject_log(logs_store.pop_front().unwrap()).await?;
        let count: usize = db
            .with_rw_connection(|conn| {
                Ok(conn.query_row("select count(*) from boo", (), |row| row.get(0))?)
            })
            .await?;
        assert_eq!(count, 0);
        let count: usize = db
            .with_ro_connection(|conn| {
                Ok(conn.query_row("select count(*) from boo", (), |row| row.get(0))?)
            })
            .await?;
        assert_eq!(count, 0);

        // apply second checkpoint
        db.inject_log(logs_store.pop_front().unwrap()).await?;
        let yo: String = db
            .with_rw_connection(|conn| {
                Ok(conn.query_row("select x from boo", (), |row| row.get(0))?)
            })
            .await?;
        assert_eq!(yo, "YO");
        let yo: String = db
            .with_ro_connection(|conn| {
                Ok(conn.query_row("select x from boo", (), |row| row.get(0))?)
            })
            .await?;
        assert_eq!(yo, "YO");

        // apply third checkpoint
        db.inject_log(logs_store.pop_front().unwrap()).await?;
        let yo: String = db
            .with_rw_connection(|conn| {
                Ok(conn.query_row("select x from boo", (), |row| row.get(0))?)
            })
            .await?;
        assert_eq!(yo, "YOO");
        let yo: String = db
            .with_ro_connection(|conn| {
                Ok(conn.query_row("select x from boo", (), |row| row.get(0))?)
            })
            .await?;
        assert_eq!(yo, "YOO");

        // apply fourth checkpoint
        db.inject_log(logs_store.pop_front().unwrap()).await?;
        let count: usize = db
            .with_rw_connection(|conn| {
                Ok(conn.query_row("select count(*) from boo", (), |row| row.get(0))?)
            })
            .await?;
        assert_eq!(count, 0);
        let count: usize = db
            .with_ro_connection(|conn| {
                Ok(conn.query_row("select count(*) from boo", (), |row| row.get(0))?)
            })
            .await?;
        assert_eq!(count, 0);

        // now we can add additional data
        db.with_rw_connection(|conn| Ok(conn.execute("insert into boo values ('LOL')", ())?))
            .await?;
        let count: usize = db
            .with_ro_connection(|conn| {
                Ok(conn.query_row("select count(*) from boo", (), |row| row.get(0))?)
            })
            .await?;
        assert_eq!(count, 1);

        // and checkpoint it
        db.checkpoint(|log| {
            async move {
                assert_eq!(log.next_frame_no(), 6);
            }
            .boxed()
        })
        .await?;

        Ok(())
    }
}
