use super::{wal::WalPage, Version};
use std::{fs::File, io::Write, os::unix::fs::FileExt, path::Path, sync::Arc};

use anyhow::{anyhow, bail, Result};
use crc::Crc;
use libsql::replication::{Frame, FrameNo};
use libsql_replication::{
    frame::{FrameBorrowed, FrameHeader, FrameMut},
    LIBSQL_PAGE_SIZE,
};
use parking_lot::RwLock;
use tempfile::tempfile;
use zerocopy::{
    byteorder::little_endian::{I32 as li32, U16 as lu16, U32 as lu32, U64 as lu64},
    AsBytes, FromBytes,
};

// /!\ this is basically a simplified fork of the original libsql-server `ReplicationLogger`
// see: libsql/libsql-server/src/replication/primary/logger.rs

/// A log file used to store SQLite WAL frames
#[derive(Debug, Clone)]
pub struct Log(Arc<RwLock<LogFile>>);

impl Log {
    fn new(log_file: LogFile) -> Self {
        Self(Arc::new(RwLock::new(log_file)))
    }

    /// Open an existing log file
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_owned();
        tokio::task::spawn_blocking(move || Ok(Log::new(LogFile::open(path)?))).await?
    }

    /// Create a new log file starting at the given frame number
    pub async fn create(start_frame_no: FrameNo) -> Result<Self> {
        tokio::task::spawn_blocking(move || Ok(Log::new(LogFile::new(start_frame_no)?))).await?
    }

    /// Return the frame number of the first frame in the log
    pub fn start_frame_no(&self) -> FrameNo {
        self.0.read().header.start_frame_no.get()
    }

    /// Return the frame number of the next frame to be written
    pub fn next_frame_no(&self) -> FrameNo {
        self.0.read().next_frame_no()
    }

    /// Return the frame number of the last frame in the log
    pub fn last_frame_no(&self) -> Option<FrameNo> {
        self.0.read().last_frame_no()
    }

    /// Check if the log is empty
    pub fn is_empty(&self) -> bool {
        self.0.read().is_empty()
    }

    pub(crate) fn has_uncommitted_frames(&self) -> bool {
        self.0.read().has_uncommitted_frames()
    }

    pub(crate) fn last_commited_frame_no(&self) -> Option<FrameNo> {
        self.0.read().last_commited_frame_no()
    }

    /// Read a frame from the log
    pub async fn read_frame(&self, frame_no: FrameNo) -> Result<Frame> {
        let log = self.0.clone();
        tokio::task::spawn_blocking(move || log.read().read_frame(frame_no)).await?
    }

    /// Push new frames to the log
    pub async fn push_frames(&mut self, frames: Vec<Frame>) -> Result<()> {
        let log = self.0.clone();
        tokio::task::spawn_blocking(move || {
            let mut log = log.write();
            for frame in frames {
                log.push_frame(&frame)?;
            }
            log.commit()?;
            Ok(())
        })
        .await?
    }

    /// Copy the log file to the given path
    pub async fn copy_to(&self, path: impl AsRef<Path>) -> Result<()> {
        let log = self.0.clone();
        let path = path.as_ref().to_owned();
        tokio::task::spawn_blocking(move || {
            let mut log = log.write();
            let mut to = File::create(path)?;
            std::io::copy(&mut log.file, &mut to)?;
            Ok(())
        })
        .await?
    }

    /// Truncate the log file to the given frame number:
    /// Frames before the given frame number will be removed.
    pub async fn truncate(&self, start_frame_no: FrameNo) -> Result<()> {
        let log = self.0.clone();
        tokio::task::spawn_blocking(move || {
            let mut log = log.write();
            let mut truncated_log = LogFile::new(start_frame_no)?;
            if !log.is_empty() && start_frame_no >= log.header.start_frame_no.get() {
                for frame_no in
                    start_frame_no..=log.last_commited_frame_no().unwrap_or(start_frame_no)
                {
                    let frame = log.read_frame(frame_no)?;
                    truncated_log.push_frame(&frame)?;
                }
            }
            truncated_log.commit()?;
            *log = truncated_log;
            Ok(())
        })
        .await?
    }

    pub(crate) fn frames_iter(&self) -> impl Iterator<Item = Result<Frame>> + '_ {
        let mut current_frame_offset = 0;
        let log = self.0.clone();
        std::iter::from_fn(move || {
            let log = log.read();
            if current_frame_offset >= log.header.frame_count.get() {
                return None;
            }
            let read_byte_offset = LogFile::absolute_byte_offset(current_frame_offset);
            current_frame_offset += 1;
            Some(
                log.read_frame_byte_offset_mut(read_byte_offset)
                    .map(|f| f.into()),
            )
        })
    }

    pub(crate) fn push_page(&mut self, page: &WalPage) -> Result<()> {
        self.0.write().push_page(page)
    }

    pub(crate) fn new_from(start_frame_no: FrameNo) -> Result<Self> {
        Ok(Self::new(LogFile::new(start_frame_no)?))
    }

    pub(crate) fn commit(&mut self) -> Result<()> {
        self.0.write().commit()
    }

    pub(crate) fn rollback(&mut self) {
        self.0.write().rollback();
    }
}

pub const MAGIC: u64 = u64::from_le_bytes(*b"MYLIBSQL");
const CRC_64_GO_ISO: Crc<u64> = Crc::<u64>::new(&crc::CRC_64_GO_ISO);

#[derive(Debug, Clone, Copy, zerocopy::FromBytes, zerocopy::FromZeroes, zerocopy::AsBytes)]
#[repr(C)]
pub struct LogHeader {
    /// magic number: b"MYLIBSQL" as u64
    pub magic: lu64,
    /// Initial checksum value for the rolling CRC checksum
    /// computed with the 64 bits CRC_64_GO_ISO
    pub start_checksum: lu64,
    /// Frame_no of the first frame in the log
    pub start_frame_no: lu64,
    /// entry count in file
    pub frame_count: lu64,
    /// Wal file version number, currently: 2
    pub version: lu32,
    /// page size: 4096
    pub page_size: li32,
    /// mylibsql version when creating this log
    pub mylibsql_version: [lu16; 4],
}

#[derive(Debug)]
struct LogFile {
    file: File,
    pub header: LogHeader,
    /// number of frames in the log that have not been committed yet. On commit the header's frame
    /// count is incremented by that amount. New pages are written after the last
    /// header.frame_count + uncommit_frame_count.
    /// On rollback, this is reset to 0, so that everything that was written after the previous
    /// header.frame_count is ignored and can be overwritten
    uncommitted_frame_count: u64,
    uncommitted_checksum: u64,

    /// checksum of the last committed frame
    commited_checksum: u64,
}

impl LogFile {
    /// size of a single frame
    pub const FRAME_SIZE: usize = size_of::<FrameHeader>() + LIBSQL_PAGE_SIZE as usize;

    fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path)?;
        let header = Self::read_header(&file)?;
        Ok(Self {
            file,
            header,
            uncommitted_frame_count: 0,
            uncommitted_checksum: header.start_checksum.get(),
            commited_checksum: header.start_checksum.get(),
        })
    }

    fn new(start_frame_no: u64) -> Result<Self> {
        let file = tempfile()?;

        let header = LogHeader {
            version: 2.into(),
            start_frame_no: start_frame_no.into(),
            magic: MAGIC.into(),
            page_size: (LIBSQL_PAGE_SIZE as i32).into(),
            start_checksum: 0.into(),
            frame_count: 0.into(),
            mylibsql_version: Version::current().0.map(Into::into),
        };

        let mut this = Self {
            file,
            header,
            uncommitted_frame_count: 0,
            uncommitted_checksum: 0,
            commited_checksum: 0,
        };

        this.write_header()?;

        Ok(this)
    }

    fn read_header(file: &File) -> Result<LogHeader> {
        let mut buf = [0; size_of::<LogHeader>()];
        file.read_exact_at(&mut buf, 0)?;
        let header =
            LogHeader::read_from(&buf).ok_or_else(|| anyhow!("invalid log file header"))?;
        if header.magic.get() != MAGIC {
            bail!("invalid log header");
        }

        Ok(header)
    }

    fn commit(&mut self) -> Result<()> {
        self.header.frame_count += self.uncommitted_frame_count.into();
        self.uncommitted_frame_count = 0;
        self.commited_checksum = self.uncommitted_checksum;
        self.write_header()?;

        Ok(())
    }

    fn rollback(&mut self) {
        self.uncommitted_frame_count = 0;
        self.uncommitted_checksum = self.commited_checksum;
    }

    fn write_header(&mut self) -> Result<()> {
        self.file.write_all_at(self.header.as_bytes(), 0)?;
        self.file.flush()?;

        Ok(())
    }

    fn last_frame_no(&self) -> Option<FrameNo> {
        if self.header.start_frame_no.get() == 0 && self.header.frame_count.get() == 0 {
            // The log does not contain any frame yet
            None
        } else {
            Some(self.header.start_frame_no.get() + self.header.frame_count.get() - 1)
        }
    }

    fn last_commited_frame_no(&self) -> Option<FrameNo> {
        if self.header.frame_count.get() == 0 {
            None
        } else {
            Some(self.header.start_frame_no.get() + self.header.frame_count.get() - 1)
        }
    }

    fn is_empty(&self) -> bool {
        self.header.frame_count.get() + self.uncommitted_frame_count == 0
    }

    fn has_uncommitted_frames(&self) -> bool {
        self.uncommitted_frame_count > 0
    }

    fn absolute_byte_offset(nth: u64) -> u64 {
        std::mem::size_of::<LogHeader>() as u64 + nth * Self::FRAME_SIZE as u64
    }

    fn byte_offset(&self, id: FrameNo) -> anyhow::Result<Option<u64>> {
        if id < self.header.start_frame_no.get()
            || id > self.header.start_frame_no.get() + self.header.frame_count.get()
        {
            return Ok(None);
        }
        Ok(Self::absolute_byte_offset(id - self.header.start_frame_no.get()).into())
    }

    fn read_frame_byte_offset_mut(&self, offset: u64) -> Result<FrameMut> {
        use zerocopy::FromZeroes;
        let mut frame = FrameBorrowed::new_zeroed();
        self.file.read_exact_at(frame.as_bytes_mut(), offset)?;
        Ok(frame.into())
    }

    fn compute_checksum(&self, page: &WalPage) -> u64 {
        let mut digest = CRC_64_GO_ISO.digest_with_initial(self.uncommitted_checksum);
        digest.update(&page.data);
        digest.finalize()
    }

    fn next_byte_offset(&self) -> u64 {
        Self::absolute_byte_offset(self.header.frame_count.get() + self.uncommitted_frame_count)
    }

    fn next_frame_no(&self) -> FrameNo {
        self.header.start_frame_no.get()
            + self.header.frame_count.get()
            + self.uncommitted_frame_count
    }

    fn push_page(&mut self, page: &WalPage) -> Result<()> {
        let checksum = self.compute_checksum(page);
        let frame = Frame::from_parts(
            &FrameHeader {
                frame_no: self.next_frame_no().into(),
                checksum: checksum.into(),
                page_no: page.page_no.into(),
                size_after: page.size_after.into(),
            },
            &page.data,
        );

        self.push_frame(&frame)
    }

    fn push_frame(&mut self, frame: &Frame) -> Result<()> {
        if frame.header().frame_no.get() != self.next_frame_no() {
            bail!(
                "unexpected frame number {}, expected {}",
                frame.header().frame_no,
                self.next_frame_no()
            );
        }

        let byte_offset = self.next_byte_offset();
        tracing::trace!(
            "writing frame {} at offset {byte_offset}",
            frame.header().frame_no
        );
        self.file.write_all_at(frame.as_bytes(), byte_offset)?;

        self.uncommitted_frame_count += 1;
        self.uncommitted_checksum = frame.header().checksum.get();

        Ok(())
    }

    fn read_frame(&self, frame_no: FrameNo) -> Result<Frame> {
        if let Some(offset) = self.byte_offset(frame_no)? {
            let frame = self.read_frame_byte_offset_mut(offset)?;
            Ok(frame.into())
        } else {
            Err(anyhow!("frame_no out of range"))
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tempfile::NamedTempFile;

    use super::*;

    #[tokio::test]
    async fn test_log_open() -> Result<()> {
        let file = NamedTempFile::new()?;
        let header = LogHeader {
            version: 2.into(),
            start_frame_no: 0.into(),
            magic: MAGIC.into(),
            page_size: (LIBSQL_PAGE_SIZE as i32).into(),
            start_checksum: 0.into(),
            frame_count: 0.into(),
            mylibsql_version: Version::current().0.map(Into::into),
        };

        let mut log = LogFile {
            file: file.reopen()?,
            header,
            uncommitted_frame_count: 0,
            uncommitted_checksum: 0,
            commited_checksum: 0,
        };

        log.write_header()?;

        let log = LogFile::open(file.path())?;
        assert_eq!(log.header.magic.get(), MAGIC);
        assert_eq!(log.header.version.get(), 2);
        assert_eq!(log.header.page_size.get(), LIBSQL_PAGE_SIZE as i32);
        Ok(())
    }

    #[tokio::test]
    async fn test_log_new() -> Result<()> {
        let log = LogFile::new(0)?;
        assert_eq!(log.header.magic.get(), MAGIC);
        assert_eq!(log.header.version.get(), 2);
        assert_eq!(log.header.page_size.get(), LIBSQL_PAGE_SIZE as i32);
        Ok(())
    }

    #[tokio::test]
    async fn test_log_commit_and_rollback() -> Result<()> {
        let mut log = LogFile::new(0)?;
        let page = WalPage {
            page_no: 1,
            data: Bytes::from(vec![0; LIBSQL_PAGE_SIZE as usize]),
            size_after: 0,
        };

        log.push_page(&page)?;
        assert!(log.has_uncommitted_frames());

        log.commit()?;
        assert!(!log.has_uncommitted_frames());

        log.push_page(&page)?;
        assert!(log.has_uncommitted_frames());

        log.rollback();
        assert!(!log.has_uncommitted_frames());
        Ok(())
    }

    #[tokio::test]
    async fn test_log_copy_to() -> Result<()> {
        let log = Log::new_from(0)?;
        let new_path = NamedTempFile::new()?;
        log.copy_to(&new_path).await?;
        assert!(new_path.path().exists());
        Ok(())
    }

    #[tokio::test]
    async fn test_log_is_empty() -> Result<()> {
        let mut log = LogFile::new(0)?;
        assert!(log.is_empty());

        let page = WalPage {
            page_no: 1,
            data: Bytes::from(vec![0; LIBSQL_PAGE_SIZE as usize]),
            size_after: 0,
        };

        log.push_page(&page)?;
        assert!(!log.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_log_frames_iter() -> Result<()> {
        let mut log = Log::new_from(0)?;
        let page = WalPage {
            page_no: 1,
            data: Bytes::from(vec![0; LIBSQL_PAGE_SIZE as usize]),
            size_after: 0,
        };

        log.push_page(&page)?;
        log.commit()?;

        let frames: Vec<_> = log.frames_iter().collect::<Result<_>>()?;
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].header().page_no.get(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_log_truncate() -> Result<()> {
        let mut log = Log::new_from(0)?;
        let page1 = WalPage {
            page_no: 1,
            data: Bytes::from(vec![0; LIBSQL_PAGE_SIZE as usize]),
            size_after: 0,
        };
        let page2 = WalPage {
            page_no: 2,
            data: Bytes::from(vec![1; LIBSQL_PAGE_SIZE as usize]),
            size_after: 0,
        };

        log.push_page(&page1)?;
        log.push_page(&page2)?;
        log.commit()?;

        assert_eq!(log.frames_iter().count(), 2);

        log.truncate(1).await?;
        assert_eq!(log.frames_iter().count(), 1);

        let frame = log.read_frame(1).await?;
        assert_eq!(frame.header().page_no.get(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_log_truncate_no_frames() -> Result<()> {
        let log = Log::new_from(0)?;
        log.truncate(1).await?;
        assert!(log.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_log_truncate_partial() -> Result<()> {
        let mut log = Log::new_from(0)?;
        let page1 = WalPage {
            page_no: 1,
            data: Bytes::from(vec![0; LIBSQL_PAGE_SIZE as usize]),
            size_after: 0,
        };
        let page2 = WalPage {
            page_no: 2,
            data: Bytes::from(vec![1; LIBSQL_PAGE_SIZE as usize]),
            size_after: 0,
        };
        let page3 = WalPage {
            page_no: 3,
            data: Bytes::from(vec![2; LIBSQL_PAGE_SIZE as usize]),
            size_after: 0,
        };

        log.push_page(&page1)?;
        log.push_page(&page2)?;
        log.push_page(&page3)?;
        log.commit()?;

        assert_eq!(log.frames_iter().count(), 3);

        log.truncate(1).await?;
        assert_eq!(log.frames_iter().count(), 2);

        let frame = log.read_frame(1).await?;
        assert_eq!(frame.header().page_no.get(), 2);

        let frame = log.read_frame(2).await?;
        assert_eq!(frame.header().page_no.get(), 3);

        Ok(())
    }

    #[test]
    fn zero_copy_header() -> Result<()> {
        let expected =
            base64::decode(r#"TVlMSUJTUUyFGgAAAAAAADkwAAAAAAAAKywKAAAAAAACAAAAABAAAAAAAAABAAAA"#)?;

        assert_eq!(
            LogHeader {
                version: 2.into(),
                start_frame_no: 12345.into(),
                magic: MAGIC.into(),
                page_size: (LIBSQL_PAGE_SIZE as i32).into(),
                start_checksum: 6789.into(),
                frame_count: 666667.into(),
                mylibsql_version: Version([0, 0, 1, 0]).0.map(Into::into),
            }
            .as_bytes(),
            &expected
        );

        Ok(())
    }

    #[test]
    fn zero_copy_frame() -> Result<()> {
        let expected = base64::decode(r#"
        OTAAAAAAAACLpdL7rgtvEdPvCCWBOLAEDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA
        wMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM
        DAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA
        wMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM
        DAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA
        wMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM
        DAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA
        wMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM
        DAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA
        wMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM
        DAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA
        wMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM
        DAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA
        wMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM
        DAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA
        wMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM
        DAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA
        wMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM
        DAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA
        wMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM
        DAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA
        wMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM
        DAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA
        wMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM
        DAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA
        wMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM
        DAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA
        wMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM
        DAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA
        wMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM
        DAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA
        wMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM
        DAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA
        wMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM
        DAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA
        wMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM
        DAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA
        wMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA==
        "#.chars().filter(|c| !c.is_whitespace()).collect::<String>())?;

        assert_eq!(
            Frame::from_parts(
                &FrameHeader {
                    frame_no: lu64::new(12345),
                    checksum: lu64::new(1256235667236758923),
                    page_no: lu32::new(621342675),
                    size_after: lu32::new(78657665),
                },
                &[12; LIBSQL_PAGE_SIZE],
            )
            .as_bytes(),
            &expected
        );

        Ok(())
    }
}
