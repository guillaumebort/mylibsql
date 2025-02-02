use crate::snapshot::write_last_frame_no;

use super::*;
use std::{fmt::Debug, future::Future, ops::Range, sync::Arc};

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
use rusqlite::{DatabaseName, TransactionState};
use snapshot::Snapshot;
use tempfile::NamedTempFile;
use wal::ShadowWal;

pub struct MylibsqlDB {
    db: NamedTempFile,
    shadow_wal: ShadowWal,
    injector: SqliteInjector,
    rw_conn: Arc<Mutex<Connection<WrappedWal<ShadowWal, Sqlite3Wal>>>>,
    ro_conn: Arc<Mutex<Connection<Sqlite3Wal>>>,
}

impl Debug for MylibsqlDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MylibsqlDB")
            .field("path", &self.db.path())
            .finish()
    }
}

impl MylibsqlDB {
    pub async fn init(
        init: impl FnOnce(&rusqlite::Connection) -> Result<()> + Send + 'static,
    ) -> Result<(Snapshot, Log)> {
        let (wal, wal_manager) = ShadowWal::new(0).await?;
        let (path, log) = tokio::task::spawn_blocking(move || {
            let db = NamedTempFile::new()?;
            let conn = Connection::open(
                db.path(),
                OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
                wal_manager,
                NO_AUTOCHECKPOINT,
                None,
            )?;
            // run user code initialization script
            init(&conn)?;
            // truncate WAL
            conn.query_row_and_then("PRAGMA wal_checkpoint(TRUNCATE)", (), |row| {
                let status: i32 = row.get(0)?;
                if status != 0 {
                    Err(anyhow!("WAL checkpoint failed with status {}", status))
                } else {
                    Ok(())
                }
            })?;
            // write the last frame number in the database header
            drop(conn);
            write_last_frame_no(&db, wal.log().last_commited_frame_no())?;
            anyhow::Ok((db, wal.into_log()?))
        })
        .await??;
        Ok((Snapshot::open(path).await?, log))
    }

    pub async fn open(snapshot: &Snapshot) -> Result<Self> {
        let start_frame_no = snapshot.last_frame_no().map(|f| f + 1).unwrap_or_default();
        let db = NamedTempFile::new()?;
        tokio::fs::copy(snapshot.path(), db.path()).await?;
        let injector =
            SqliteInjector::new(db.path().to_path_buf(), 4096, NO_AUTOCHECKPOINT, None).await?;
        let (shadow_wal, wal_manager) = ShadowWal::new(start_frame_no).await?;
        let db = tokio::task::spawn_blocking(move || {
            let rw_conn = Connection::open(
                db.path(),
                OpenFlags::SQLITE_OPEN_READ_WRITE,
                wal_manager,
                NO_AUTOCHECKPOINT,
                None,
            )?;
            let ro_conn = Connection::open(
                db.path(),
                OpenFlags::SQLITE_OPEN_READ_ONLY,
                Sqlite3WalManager::new(),
                NO_AUTOCHECKPOINT,
                None,
            )?;
            anyhow::Ok(MylibsqlDB {
                db,
                rw_conn: Arc::new(Mutex::new(rw_conn)),
                ro_conn: Arc::new(Mutex::new(ro_conn)),
                shadow_wal,
                injector,
            })
        })
        .await??;
        Ok(db)
    }

    pub async fn inject_log(&mut self, additional_log: &Log) -> Result<()> {
        let additional_log_start_frame_no = additional_log.start_frame_no();
        {
            let log = self.shadow_wal.log();
            let expected_frame_no = log.next_frame_no();
            if !log.is_empty() || additional_log_start_frame_no != expected_frame_no {
                bail!("log does not start at the expected frame number (expected {expected_frame_no}, got {additional_log_start_frame_no})");
            }
        }
        for frame in additional_log.frames_iter() {
            Self::inject_frame(&mut self.injector, frame?).await?;
        }
        let shadow_wal = self.shadow_wal.clone();
        let next_frame_no = additional_log.next_frame_no();
        tokio::task::spawn_blocking(move || {
            shadow_wal.swap_log(move |_| Log::new_from(next_frame_no))
        })
        .await??;
        let rw_conn = self.rw_conn.clone();
        tokio::task::spawn_blocking(move || {
            // truncate WAL
            rw_conn
                .lock()
                .query_row_and_then("PRAGMA wal_checkpoint(TRUNCATE)", (), |row| {
                    let status: i32 = row.get(0)?;
                    if status != 0 {
                        Err(anyhow!("WAL checkpoint failed with status {}", status))
                    } else {
                        Ok(())
                    }
                })
        })
        .await??;
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

    pub async fn with_rw_connection<A>(
        &self,
        f: impl FnOnce(&mut rusqlite::Connection) -> Result<A> + Send + 'static,
    ) -> Result<(A, Log, Range<FrameNo>)>
    where
        A: Send + 'static,
    {
        let conn = self.rw_conn.clone();
        let shadow_wal = self.shadow_wal.clone();
        shadow_wal.check_poisoned()?;

        Ok(tokio::task::spawn_blocking(move || {
            let mut conn = conn.lock();

            // record the start frame number
            let start_frame_no = shadow_wal.log().next_frame_no();
            let a = f(&mut conn)?;

            // auto-rollback pending transaction
            let result = if conn.transaction_state(None) != Ok(TransactionState::None) {
                conn.execute_batch("ROLLBACK;")?;
                Err(anyhow!("a transaction was still pending"))
            } else {
                // record the end frame number
                let end_frame_no = shadow_wal.log().next_frame_no();
                let frames = start_frame_no..end_frame_no;

                Ok((a, shadow_wal.log().clone(), frames))
            };

            // sanity check
            if shadow_wal.log().has_uncommitted_frames() {
                shadow_wal.poison();
                bail!("fatal error: uncommitted frames in the log");
            }

            result
        })
        .await??)
    }

    pub async fn with_ro_connection<A>(
        &self,
        f: impl FnOnce(&mut rusqlite::Connection) -> Result<A> + Send + 'static,
    ) -> Result<A>
    where
        A: Send + 'static,
    {
        self.shadow_wal.check_poisoned()?;
        let conn = self.ro_conn.clone();
        Ok(tokio::task::spawn_blocking(move || {
            let mut conn = conn.lock();
            debug_assert!(conn.is_readonly(DatabaseName::Main)?);
            f(&mut conn)
        })
        .await??)
    }

    pub async fn checkpoint(
        &self,
        f: impl FnOnce(Log) -> BoxFuture<'static, ()>,
    ) -> Result<impl Future<Output = Option<FrameNo>>> {
        let shadow_wal = self.shadow_wal.clone();
        shadow_wal.check_poisoned()?;
        let rw_conn = self.rw_conn.clone();
        let old_log = tokio::task::spawn_blocking(move || {
            let rw_conn = rw_conn.lock();
            let old_log = shadow_wal.swap_log(|log| Log::new_from(log.next_frame_no()))?;
            // checkpoint WAL as much as possible
            rw_conn.query_row_and_then("PRAGMA wal_checkpoint(PASSIVE)", (), |row| {
                let status: i32 = row.get(0)?;
                if status != 0 {
                    shadow_wal.poison();
                    Err(anyhow!(
                        "fatal error: WAL checkpoint failed with status {}",
                        status
                    ))
                } else {
                    Ok(())
                }
            })?;
            anyhow::Ok(old_log)
        })
        .await??;
        let last_frame_no = old_log.last_commited_frame_no();
        Ok(async move {
            f(old_log).await;
            last_frame_no
        })
    }

    pub fn into_inner(self) -> (NamedTempFile, Log) {
        drop(self.rw_conn);
        drop(self.ro_conn);
        (self.db, self.shadow_wal.into_log().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use futures::FutureExt;

    use super::*;

    async fn blank_db() -> Result<Snapshot> {
        init(|_| Ok(())).await.map(|(snapshot, _)| snapshot)
    }

    #[tokio::test]
    async fn create_blank_database() -> Result<()> {
        let (snapshot, log) = MylibsqlDB::init(|_| Ok(())).await?;
        assert!(snapshot.last_frame_no().is_none());
        assert!(log.last_commited_frame_no().is_none());
        assert_eq!(log.next_frame_no(), 0);

        // reopen from snapshot
        let _ = MylibsqlDB::open(&snapshot).await?;

        // reopen from log
        let mut db = MylibsqlDB::open(&blank_db().await?).await?;
        db.inject_log(&log).await?;

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
        assert!(snapshot.last_frame_no().is_some());
        assert!(log.last_commited_frame_no().is_some());
        assert_eq!(log.next_frame_no(), 3);

        // reopen from snapshot
        let db = MylibsqlDB::open(&snapshot).await?;
        let count: usize = db
            .with_rw_connection(|conn| {
                Ok(conn.query_row("select count(*) from lol", (), |row| row.get(0))?)
            })
            .await?
            .0;
        assert_eq!(count, 1);
        let count: usize = db
            .with_ro_connection(|conn| {
                Ok(conn.query_row("select count(*) from lol", (), |row| row.get(0))?)
            })
            .await?;
        assert_eq!(count, 1);

        // reopen from log
        let mut db = MylibsqlDB::open(&blank_db().await?).await?;
        db.inject_log(&log).await?;
        let count: usize = db
            .with_rw_connection(|conn| {
                Ok(conn.query_row("select count(*) from lol", (), |row| row.get(0))?)
            })
            .await?
            .0;
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
        let db = MylibsqlDB::open(&blank_db().await?).await?;
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

        let db = MylibsqlDB::open(&blank_db().await?).await?;
        assert_eq!(None, db.checkpoint(&save_log).await?.await);

        // first checkpoint (create table)
        db.with_rw_connection(|conn| Ok(conn.execute("create table boo(x string)", ())?))
            .await?;
        assert_eq!(Some(1), db.checkpoint(&save_log).await?.await);

        // second checkpoint (insert data)
        db.with_rw_connection(|conn| Ok(conn.execute("insert into boo values ('YO')", ())?))
            .await?;
        assert_eq!(Some(2), db.checkpoint(&save_log).await?.await);

        // third checkpoint (update data)
        db.with_rw_connection(|conn| Ok(conn.execute("update boo set x = 'YOO'", ())?))
            .await?;
        assert_eq!(Some(3), db.checkpoint(&save_log).await?.await);

        // fourth checkpoint (delete data)
        db.with_rw_connection(|conn| Ok(conn.execute("delete from boo", ())?))
            .await?;
        assert_eq!(Some(4), db.checkpoint(&save_log).await?.await);

        drop(save_log);
        let mut logs_store = Arc::into_inner(logs_store).unwrap().into_inner();
        assert_eq!(logs_store.len(), 5);

        // restart with a new db
        let mut db = MylibsqlDB::open(&blank_db().await?).await?;
        db.inject_log(&logs_store.pop_front().unwrap()).await?; // this one is blank

        // apply first checkpoint
        db.inject_log(&logs_store.pop_front().unwrap()).await?;
        let count: usize = db
            .with_rw_connection(|conn| {
                Ok(conn.query_row("select count(*) from boo", (), |row| row.get(0))?)
            })
            .await?
            .0;
        assert_eq!(count, 0);
        let count: usize = db
            .with_ro_connection(|conn| {
                Ok(conn.query_row("select count(*) from boo", (), |row| row.get(0))?)
            })
            .await?;
        assert_eq!(count, 0);

        // apply second checkpoint
        db.inject_log(&logs_store.pop_front().unwrap()).await?;
        let yo: String = db
            .with_rw_connection(|conn| {
                Ok(conn.query_row("select x from boo", (), |row| row.get(0))?)
            })
            .await?
            .0;
        assert_eq!(yo, "YO");
        let yo: String = db
            .with_ro_connection(|conn| {
                Ok(conn.query_row("select x from boo", (), |row| row.get(0))?)
            })
            .await?;
        assert_eq!(yo, "YO");

        // apply third checkpoint
        db.inject_log(&logs_store.pop_front().unwrap()).await?;
        let yo: String = db
            .with_rw_connection(|conn| {
                Ok(conn.query_row("select x from boo", (), |row| row.get(0))?)
            })
            .await?
            .0;
        assert_eq!(yo, "YOO");
        let yo: String = db
            .with_ro_connection(|conn| {
                Ok(conn.query_row("select x from boo", (), |row| row.get(0))?)
            })
            .await?;
        assert_eq!(yo, "YOO");

        // apply fourth checkpoint
        db.inject_log(&logs_store.pop_front().unwrap()).await?;
        let count: usize = db
            .with_rw_connection(|conn| {
                Ok(conn.query_row("select count(*) from boo", (), |row| row.get(0))?)
            })
            .await?
            .0;
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
        .await?
        .await;

        Ok(())
    }

    #[tokio::test]
    async fn bad_transaction() -> Result<()> {
        let (snapshot, log) = MylibsqlDB::init(|conn| {
            conn.execute("create table lol(x integer)", ())?;
            Ok(())
        })
        .await?;
        assert_eq!(2, log.next_frame_no());

        let db = MylibsqlDB::open(&snapshot).await?;

        // this is not valid to keep a transaction pending like this
        assert!(db
            .with_rw_connection(|conn| {
                conn.execute_batch("begin;")?;
                conn.execute_batch("insert into lol values (1)")?;
                Ok(())
            })
            .await
            .is_err());

        // here it is ok because txn will be auto-rollback during the drop
        assert!(db
            .with_rw_connection(|conn| {
                let txn = conn.transaction()?;
                txn.execute_batch("insert into lol values (1)")?;
                Ok(())
            })
            .await
            .is_ok());

        let (_, _, frames) = db
            .with_rw_connection(|conn| {
                let txn = conn.transaction()?;
                txn.execute_batch("insert into lol values (1)")?;
                txn.commit()?;
                Ok(())
            })
            .await?;
        assert_eq!(frames, 2..3); // we should have a single frame

        Ok(())
    }
}
