use super::{wal::WalPage, Version};
use std::{fs::File, io::Write, os::unix::fs::FileExt, path::Path};

use anyhow::{anyhow, bail, Result};
use crc::Crc;
use libsql::replication::{Frame, FrameNo};
use libsql_replication::{
    frame::{FrameBorrowed, FrameHeader, FrameMut},
    LIBSQL_PAGE_SIZE,
};
use tempfile::tempfile;
use zerocopy::{
    byteorder::little_endian::{I32 as li32, U16 as lu16, U32 as lu32, U64 as lu64},
    AsBytes, FromBytes,
};

// /!\ this is basically a simplified fork of the original libsql-server `ReplicationLogger`
// see: libsql/libsql-server/src/replication/primary/logger.rs

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

impl LogHeader {
    pub fn last_frame_no(&self) -> Option<FrameNo> {
        if self.start_frame_no.get() == 0 && self.frame_count.get() == 0 {
            // The log does not contain any frame yet
            None
        } else {
            Some(self.start_frame_no.get() + self.frame_count.get() - 1)
        }
    }
}

#[derive(Debug)]
pub struct Log {
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

impl Log {
    /// size of a single frame
    pub const FRAME_SIZE: usize = size_of::<FrameHeader>() + LIBSQL_PAGE_SIZE as usize;

    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_owned();
        tokio::task::spawn_blocking(|| {
            let file = File::open(path)?;
            let header = Self::read_header(&file)?;
            Ok(Self {
                file,
                header,
                uncommitted_frame_count: 0,
                uncommitted_checksum: header.start_checksum.get(),
                commited_checksum: header.start_checksum.get(),
            })
        })
        .await?
    }

    pub async fn new(start_frame_no: u64) -> Result<Self> {
        tokio::task::spawn_blocking(move || {
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
        })
        .await?
    }

    pub fn into_file(self) -> File {
        self.file
    }

    pub async fn move_to(self, path: impl AsRef<Path>) -> Result<()> {
        let mut file = tokio::fs::File::from_std(self.file);
        let mut new_file = tokio::fs::File::create(path).await?;
        tokio::io::copy(&mut file, &mut new_file).await?;
        Ok(())
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

    pub fn header(&self) -> &LogHeader {
        &self.header
    }

    pub(crate) fn commit(&mut self) -> Result<()> {
        self.header.frame_count += self.uncommitted_frame_count.into();
        self.uncommitted_frame_count = 0;
        self.commited_checksum = self.uncommitted_checksum;
        self.write_header()?;

        Ok(())
    }

    pub(crate) fn rollback(&mut self) {
        self.uncommitted_frame_count = 0;
        self.uncommitted_checksum = self.commited_checksum;
    }

    fn write_header(&mut self) -> Result<()> {
        self.file.write_all_at(self.header.as_bytes(), 0)?;
        self.file.flush()?;

        Ok(())
    }

    pub(crate) fn last_commited_frame_no(&self) -> Option<FrameNo> {
        if self.header.frame_count.get() == 0 {
            None
        } else {
            Some(self.header.start_frame_no.get() + self.header.frame_count.get() - 1)
        }
    }

    pub fn is_empty(&self) -> bool {
        self.header.frame_count.get() + self.uncommitted_frame_count == 0
    }

    pub fn has_uncommitted_frames(&self) -> bool {
        self.uncommitted_frame_count > 0
    }

    /// Returns the bytes position of the `nth` entry in the log
    fn absolute_byte_offset(nth: u64) -> u64 {
        std::mem::size_of::<LogHeader>() as u64 + nth * Self::FRAME_SIZE as u64
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
        Self::absolute_byte_offset(self.header().frame_count.get() + self.uncommitted_frame_count)
    }

    pub(crate) fn next_frame_no(&self) -> FrameNo {
        self.header().start_frame_no.get()
            + self.header().frame_count.get()
            + self.uncommitted_frame_count
    }

    pub(crate) fn push_page(&mut self, page: &WalPage) -> Result<()> {
        let checksum = self.compute_checksum(page);
        let data = &page.data;
        let frame = Frame::from_parts(
            &FrameHeader {
                frame_no: self.next_frame_no().into(),
                checksum: checksum.into(),
                page_no: page.page_no.into(),
                size_after: page.size_after.into(),
            },
            &data,
        );

        let byte_offset = self.next_byte_offset();
        tracing::trace!(
            "writing frame {} at offset {byte_offset}",
            frame.header().frame_no
        );
        self.file.write_all_at(frame.as_bytes(), byte_offset)?;

        self.uncommitted_frame_count += 1;
        self.uncommitted_checksum = checksum;

        Ok(())
    }

    pub(crate) fn frames_iter(&self) -> Result<impl Iterator<Item = Result<Frame>> + '_> {
        let mut current_frame_offset = 0;
        Ok(std::iter::from_fn(move || {
            if current_frame_offset >= self.header.frame_count.get() {
                return None;
            }
            let read_byte_offset = Self::absolute_byte_offset(current_frame_offset);
            current_frame_offset += 1;
            Some(
                self.read_frame_byte_offset_mut(read_byte_offset)
                    .map(|f| f.into()),
            )
        }))
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

        let mut log = Log {
            file: file.reopen()?,
            header,
            uncommitted_frame_count: 0,
            uncommitted_checksum: 0,
            commited_checksum: 0,
        };

        log.write_header()?;

        let log = Log::open(file.path()).await?;
        assert_eq!(log.header.magic.get(), MAGIC);
        assert_eq!(log.header.version.get(), 2);
        assert_eq!(log.header.page_size.get(), LIBSQL_PAGE_SIZE as i32);
        Ok(())
    }

    #[tokio::test]
    async fn test_log_new() -> Result<()> {
        let log = Log::new(0).await?;
        assert_eq!(log.header.magic.get(), MAGIC);
        assert_eq!(log.header.version.get(), 2);
        assert_eq!(log.header.page_size.get(), LIBSQL_PAGE_SIZE as i32);
        Ok(())
    }

    #[tokio::test]
    async fn test_log_commit_and_rollback() -> Result<()> {
        let mut log = Log::new(0).await?;
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
    async fn test_log_move_to() -> Result<()> {
        let log = Log::new(0).await?;
        let new_path = NamedTempFile::new()?;
        log.move_to(&new_path).await?;
        assert!(new_path.path().exists());
        Ok(())
    }

    #[tokio::test]
    async fn test_log_is_empty() -> Result<()> {
        let mut log = Log::new(0).await?;
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
        let mut log = Log::new(0).await?;
        let page = WalPage {
            page_no: 1,
            data: Bytes::from(vec![0; LIBSQL_PAGE_SIZE as usize]),
            size_after: 0,
        };

        log.push_page(&page)?;
        log.commit()?;

        let frames: Vec<_> = log.frames_iter()?.collect::<Result<_>>()?;
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].header().page_no.get(), 1);
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
