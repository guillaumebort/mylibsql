use std::{fs::File, os::unix::fs::FileExt, path::Path};

use anyhow::Result;
use libsql::replication::FrameNo;

use crate::{Log, Primary};

/// A snapshot of a database
pub struct Snapshot {
    path: Box<dyn AsRef<Path>>,
    last_frame_no: Option<FrameNo>,
}

impl Snapshot {
    /// Open a snapshot from a file
    pub async fn open(path: impl AsRef<Path> + 'static) -> Result<Self> {
        let last_frame_no = {
            let path = path.as_ref().to_path_buf();
            tokio::task::spawn_blocking(move || read_last_frame_no(&path)).await??
        };
        Ok(Self {
            path: Box::new(path),
            last_frame_no,
        })
    }

    /// Get the path of the snapshot
    pub fn path(&self) -> &Path {
        (*self.path).as_ref()
    }

    /// Get the last frame number
    pub fn last_frame_no(&self) -> Option<FrameNo> {
        self.last_frame_no
    }

    /// Move the snapshot to a new path
    pub async fn move_to(self, path: impl AsRef<Path>) -> Result<()> {
        tokio::fs::rename(self.path(), path).await?;
        Ok(())
    }

    /// From this existing snapshot, create a new snapshot with additional logs
    pub async fn append(&self, additional_logs: &[Log]) -> Result<Snapshot> {
        let primary = Primary::open(self, additional_logs).await?;
        let (db, log) = primary.db.into_inner();
        let last_frame_no = log.last_frame_no();
        write_last_frame_no(db.path(), last_frame_no)?;
        Self::open(db).await
    }
}

// hijack the database header to store the last frame number
// see: https://www.sqlite.org/fileformat2.html#database_header
// we use both application_id and user_version fields

pub fn write_last_frame_no(path: impl AsRef<Path>, last_frame_no: Option<FrameNo>) -> Result<()> {
    let bytes = last_frame_no.unwrap_or(u64::MAX).to_be_bytes();
    let file = File::options().write(true).open(path)?;
    file.write_at(&bytes[0..4], 60)?;
    file.write_at(&bytes[4..8], 68)?;
    Ok(())
}

pub fn read_last_frame_no(path: impl AsRef<Path>) -> Result<Option<FrameNo>> {
    let file = File::open(path)?;
    let mut bytes = [0; 8];
    file.read_at(&mut bytes[0..4], 60)?;
    file.read_at(&mut bytes[4..8], 68)?;
    match FrameNo::from_be_bytes(bytes) {
        FrameNo::MAX => Ok(None),
        frame_no => Ok(Some(frame_no)),
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use futures::FutureExt;
    use parking_lot::Mutex;

    use crate::init;

    use super::*;

    #[tokio::test]
    async fn test_snapshot() -> Result<()> {
        let (snapshot, _) = init(|conn| Ok(conn.execute_batch("create table t (x int)")?)).await?;

        let logs = Arc::new(Mutex::new(vec![]));
        let primary = Primary::open(&snapshot, &[]).await?;

        for i in 0..5 {
            let ack = primary
                .with_connection(move |conn| Ok(conn.execute("insert into t values (?)", (i,))?))
                .await?;

            {
                let logs = logs.clone();
                primary
                    .checkpoint(|log| {
                        async move {
                            logs.lock().push(log.clone());
                        }
                        .boxed()
                    })
                    .await?
                    .await;
            }

            ack.await;
        }

        let new_snapshot = snapshot.append(&logs.lock()).await?;

        let primary = Primary::open(&new_snapshot, &[]).await?;
        let count = primary
            .with_connection::<i32>(|conn| {
                Ok(conn.query_row("select count(*) from t", (), |row| row.get(0))?)
            })
            .await?;

        assert_eq!(count.await, 5);

        Ok(())
    }
}
