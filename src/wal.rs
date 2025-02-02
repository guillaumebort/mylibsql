use super::log::Log;

use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use libsql::ffi::SQLITE_IOERR;
use libsql_sys::{
    rusqlite,
    wal::{
        wrapper::{WalWrapper, WrapWal},
        Sqlite3WalManager, Wal,
    },
};
use parking_lot::{Mutex, MutexGuard};

#[derive(Clone, Debug)]
pub struct WalPage {
    pub page_no: u32,
    /// 0 for non-commit frames
    pub size_after: u32,
    pub data: Bytes,
}

#[derive(Clone)]
pub struct ShadowWal {
    buffer: Vec<WalPage>,
    log: Arc<Mutex<Log>>,
}

impl ShadowWal {
    pub async fn new(
        start_frame_no: u64,
    ) -> Result<(Self, WalWrapper<ShadowWal, Sqlite3WalManager>)> {
        let log = tokio::task::spawn_blocking(move || Log::new(start_frame_no)).await??;
        let log = Arc::new(Mutex::new(log));
        let buffer = Vec::new();
        let wal = ShadowWal { buffer, log };
        let wal_wrapper = WalWrapper::new(wal.clone(), Sqlite3WalManager::new());
        Ok((wal, wal_wrapper))
    }

    pub fn log(&self) -> MutexGuard<Log> {
        self.log.lock()
    }

    pub(crate) fn swap_log(&self) -> Result<Log> {
        let mut log = self.log.lock();
        let next_log = Log::new(log.next_frame_no())?;
        let old_log = std::mem::replace(&mut *log, next_log);
        Ok(old_log)
    }

    pub fn into_log(self) -> Result<Log> {
        Ok(Arc::into_inner(self.log)
            .ok_or_else(|| anyhow!("log is still used by a shadow wal"))?
            .into_inner())
    }
}

impl<W: Wal> WrapWal<W> for ShadowWal {
    fn undo<U: libsql_sys::wal::UndoHandler>(
        &mut self,
        wrapped: &mut W,
        handler: Option<&mut U>,
    ) -> libsql_sys::wal::Result<()> {
        self.rollback();
        wrapped.undo(handler)
    }

    fn insert_frames(
        &mut self,
        wrapped: &mut W,
        page_size: std::ffi::c_int,
        page_headers: &mut libsql_sys::wal::PageHeaders,
        size_after: u32,
        is_commit: bool,
        sync_flags: std::ffi::c_int,
    ) -> libsql_sys::wal::Result<usize> {
        assert_eq!(page_size, 4096);
        let iter = page_headers.iter();
        for (page_no, data) in iter {
            self.write_frame(page_no, data);
        }
        if let Err(e) = self.flush(size_after) {
            tracing::error!("error writing to replication log: {e}");
            // returning IO_ERR ensure that xUndo will be called by sqlite.
            return Err(rusqlite::ffi::Error::new(SQLITE_IOERR));
        }

        let num_frames =
            wrapped.insert_frames(page_size, page_headers, size_after, is_commit, sync_flags)?;

        if is_commit {
            if let Err(e) = self.commit() {
                // If we reach this point, it means that we have committed a transaction to sqlite wal,
                // but failed to commit it to the shadow WAL, which leaves us in an inconsistent state.
                tracing::error!(
                    "fatal error: log failed to commit: inconsistent replication log: {e}"
                );
                std::process::abort();
            }
        }

        Ok(num_frames)
    }
}

impl ShadowWal {
    fn write_frame(&mut self, page_no: u32, data: &[u8]) {
        let entry = WalPage {
            page_no,
            size_after: 0,
            data: Bytes::copy_from_slice(data),
        };
        self.buffer.push(entry);
    }

    /// write buffered pages to the logger, without committing.
    fn flush(&mut self, size_after: u32) -> anyhow::Result<()> {
        let Some(last_page) = self.buffer.last_mut() else {
            return Ok(());
        };
        last_page.size_after = size_after;
        let mut log = self.log.lock();
        for page in self.buffer.iter() {
            log.push_page(page)?;
        }
        self.buffer.clear();

        Ok(())
    }

    fn commit(&self) -> anyhow::Result<()> {
        let mut log = self.log.lock();
        log.commit()?;
        Ok(())
    }

    fn rollback(&mut self) {
        let mut log = self.log.lock();
        log.rollback();
        self.buffer.clear();
    }
}

#[cfg(test)]
mod tests {
    use libsql_sys::{connection::NO_AUTOCHECKPOINT, Connection};
    use rusqlite::OpenFlags;
    use tempfile::NamedTempFile;

    use super::*;

    #[tokio::test]
    async fn test_shadow_wal() -> Result<()> {
        let (shadow_wal, wal_manager) = ShadowWal::new(12).await?;
        let tmp = NamedTempFile::new()?;
        let conn = Connection::open(
            &tmp,
            OpenFlags::SQLITE_OPEN_READ_WRITE,
            wal_manager,
            NO_AUTOCHECKPOINT,
            None,
        )?;

        assert!(shadow_wal.log().is_empty());
        conn.execute_batch("create table test (id integer primary key, name text);")?;
        assert!(!shadow_wal.log().is_empty());
        assert!(!shadow_wal.log().has_uncommitted_frames());

        Ok(())
    }

    #[tokio::test]
    async fn test_shadow_wal_transaction() -> Result<()> {
        let (shadow_wal, wal_manager) = ShadowWal::new(12).await?;
        let tmp = NamedTempFile::new()?;
        let mut conn = Connection::open(
            &tmp,
            OpenFlags::SQLITE_OPEN_READ_WRITE,
            wal_manager,
            NO_AUTOCHECKPOINT,
            None,
        )?;

        assert!(shadow_wal.log().is_empty());
        let txn = conn.transaction()?;
        txn.execute_batch("create table test (id integer primary key, name text)")?;
        txn.execute("insert into test values (1, 'lol')", ())?;
        assert!(shadow_wal.log().is_empty());
        txn.commit()?;

        assert!(!shadow_wal.log().is_empty());
        assert!(!shadow_wal.log().has_uncommitted_frames());
        assert_eq!(shadow_wal.log().last_commited_frame_no(), Some(13)); // 12 + 1

        let txn = conn.transaction()?;
        txn.execute("insert into test values (2, 'kiki')", ())?;
        assert_eq!(shadow_wal.log().last_commited_frame_no(), Some(13));
        txn.rollback()?;

        assert_eq!(shadow_wal.log().last_commited_frame_no(), Some(13));

        Ok(())
    }
}
