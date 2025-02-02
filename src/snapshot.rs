use std::path::Path;

use anyhow::Result;
use libsql::replication::FrameNo;
use tempfile::NamedTempFile;

pub struct Snapshot {
    pub path: Box<dyn AsRef<Path> + Send + 'static>,
    pub last_frame_no: Option<FrameNo>,
}

impl Snapshot {
    pub async fn open(path: impl AsRef<Path>, last_frame_no: Option<FrameNo>) -> Result<Self> {
        let tmp = NamedTempFile::new()?;
        tokio::fs::copy(path, &tmp).await?;
        Ok(Self {
            path: Box::new(tmp),
            last_frame_no,
        })
    }

    pub fn open_in_place(
        path: impl AsRef<Path> + Send + 'static,
        last_frame_no: Option<FrameNo>,
    ) -> Result<Self> {
        Ok(Self {
            path: Box::new(path),
            last_frame_no,
        })
    }

    pub async fn move_to(self, path: impl AsRef<Path>) -> Result<()> {
        tokio::fs::rename(self.path.as_ref(), path).await?;
        Ok(())
    }
}
