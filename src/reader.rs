use crate::error::{Result, SdJournalError};
#[cfg(test)]
use crate::util::checked_add_u64;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(test)]
pub(crate) mod test_io {
    use std::{
        cell::RefCell,
        collections::HashMap,
        path::{Path, PathBuf},
    };
    thread_local! {
        static OPENS: RefCell<HashMap<PathBuf, usize>> = RefCell::default();
    }
    pub(crate) fn opened(path: &Path) {
        OPENS.with(|counts| *counts.borrow_mut().entry(path.to_owned()).or_default() += 1);
    }
    pub(crate) fn reset() {
        OPENS.with(|counts| counts.borrow_mut().clear());
    }
    pub(crate) fn opens(path: &Path) -> usize {
        OPENS.with(|counts| counts.borrow().get(path).copied().unwrap_or(0))
    }
}

#[cfg(feature = "mmap")]
use memmap2::Mmap;

/// A byte buffer returned by `RandomAccess`.
///
/// When mmap is enabled and safe to use, this may borrow from an mmap'd region.
/// Otherwise, this owns the bytes read from the underlying file.
#[derive(Debug, Clone)]
pub(crate) enum ByteBuf {
    #[cfg(feature = "mmap")]
    Mmap {
        map: Arc<Mmap>,
        range: std::ops::Range<usize>,
    },
    Owned(Arc<[u8]>),
}

impl ByteBuf {
    #[cfg(feature = "mmap")]
    pub(crate) fn from_mmap(
        map: Arc<Mmap>,
        range: std::ops::Range<usize>,
        path: &Path,
    ) -> Result<Self> {
        if map.get(range.clone()).is_none() {
            return Err(SdJournalError::Corrupt {
                path: Some(path.to_path_buf()),
                offset: None,
                reason: "mmap range out of bounds".to_string(),
            });
        }
        Ok(ByteBuf::Mmap { map, range })
    }

    pub(crate) fn from_vec(v: Vec<u8>) -> Self {
        ByteBuf::Owned(Arc::<[u8]>::from(v))
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            #[cfg(feature = "mmap")]
            ByteBuf::Mmap { map, range } => &map[range.clone()],
            ByteBuf::Owned(b) => b,
        }
    }
}

impl AsRef<[u8]> for ByteBuf {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

#[derive(Clone)]
pub(crate) struct FileAccess {
    path: PathBuf,
    file: Arc<File>,
}

impl FileAccess {
    pub(crate) fn new(path: PathBuf, file: Arc<File>) -> Self {
        Self { path, file }
    }

    pub(crate) fn read_known_valid(&self, offset: u64, len: usize) -> Result<ByteBuf> {
        let mut buf = vec![0u8; len];
        if let Err(e) = read_exact_at(self.file.as_ref(), offset, &mut buf) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Err(SdJournalError::Transient {
                    path: Some(self.path.clone()),
                    reason: format!(
                        "file was truncated while reading (offset={offset}, len={len})"
                    ),
                });
            }
            return Err(SdJournalError::io("read_at", Some(self.path.clone()), e));
        }
        Ok(ByteBuf::from_vec(buf))
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> Result<u64> {
        self.file
            .metadata()
            .map_err(|e| SdJournalError::io("metadata", Some(self.path.clone()), e))
            .map(|m| m.len())
    }

    #[cfg(test)]
    pub(crate) fn read(&self, offset: u64, len: usize) -> Result<ByteBuf> {
        let end = checked_add_u64(offset, u64::try_from(len).unwrap_or(u64::MAX), "read range")?;
        let file_len = self.len()?;
        if end > file_len {
            return Err(SdJournalError::Transient {
                path: Some(self.path.clone()),
                reason: format!("read beyond end of file (file_len={file_len}, end={end})"),
            });
        }

        self.read_known_valid(offset, len)
    }
}

#[derive(Clone)]
pub(crate) struct ReopenAccess {
    path: PathBuf,
    expected_file_id: [u8; 16],
    expected_seqnum_id: [u8; 16],
}

impl ReopenAccess {
    pub(crate) fn new(
        path: PathBuf,
        expected_file_id: [u8; 16],
        expected_seqnum_id: [u8; 16],
    ) -> Self {
        Self {
            path,
            expected_file_id,
            expected_seqnum_id,
        }
    }

    pub(crate) fn read_known_valid(&self, offset: u64, len: usize) -> Result<ByteBuf> {
        let file = File::open(&self.path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::PermissionDenied {
                SdJournalError::PermissionDenied {
                    path: self.path.clone(),
                }
            } else {
                SdJournalError::io("open", Some(self.path.clone()), error)
            }
        })?;
        #[cfg(test)]
        test_io::opened(&self.path);

        let mut identity = [0u8; 88];
        if let Err(error) = read_exact_at(&file, 0, &mut identity) {
            return Err(read_failure(&self.path, 0, identity.len(), error));
        }
        let file_id = identity.get(24..40);
        let seqnum_id = identity.get(72..88);
        if file_id != Some(self.expected_file_id.as_slice())
            || seqnum_id != Some(self.expected_seqnum_id.as_slice())
        {
            return Err(SdJournalError::Transient {
                path: Some(self.path.clone()),
                reason: "journal file was replaced after discovery".to_string(),
            });
        }

        let mut buf = vec![0u8; len];
        if let Err(error) = read_exact_at(&file, offset, &mut buf) {
            return Err(read_failure(&self.path, offset, len, error));
        }
        Ok(ByteBuf::from_vec(buf))
    }
}

fn read_failure(path: &Path, offset: u64, len: usize, error: std::io::Error) -> SdJournalError {
    if error.kind() == std::io::ErrorKind::UnexpectedEof {
        SdJournalError::Transient {
            path: Some(path.to_path_buf()),
            reason: format!("file was truncated while reading (offset={offset}, len={len})"),
        }
    } else {
        SdJournalError::io("read_at", Some(path.to_path_buf()), error)
    }
}

#[cfg(feature = "mmap")]
#[derive(Clone)]
pub(crate) struct MmapAccess {
    path: PathBuf,
    #[cfg(test)]
    file: Arc<File>,
    map: Arc<Mmap>,
}

#[cfg(feature = "mmap")]
impl MmapAccess {
    pub(crate) fn new(path: PathBuf, file: Arc<File>, map: Arc<Mmap>) -> Self {
        #[cfg(not(test))]
        let _ = file;
        Self {
            path,
            #[cfg(test)]
            file,
            map,
        }
    }

    pub(crate) fn read_known_valid(&self, offset: u64, len: usize) -> Result<ByteBuf> {
        let start = usize::try_from(offset).map_err(|_| SdJournalError::Corrupt {
            path: Some(self.path.clone()),
            offset: Some(offset),
            reason: "offset out of range".to_string(),
        })?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| SdJournalError::Corrupt {
                path: Some(self.path.clone()),
                offset: Some(offset),
                reason: "range overflow".to_string(),
            })?;

        ByteBuf::from_mmap(self.map.clone(), start..end, &self.path)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> Result<u64> {
        self.file
            .metadata()
            .map_err(|e| SdJournalError::io("metadata", Some(self.path.clone()), e))
            .map(|m| m.len())
    }

    #[cfg(test)]
    pub(crate) fn read(&self, offset: u64, len: usize) -> Result<ByteBuf> {
        let end = checked_add_u64(offset, u64::try_from(len).unwrap_or(u64::MAX), "read range")?;
        let file_len = self.len()?;
        if end > file_len {
            return Err(SdJournalError::Transient {
                path: Some(self.path.clone()),
                reason: format!("mmap read beyond end of file (file_len={file_len}, end={end})"),
            });
        }

        self.read_known_valid(offset, len)
    }
}

#[cfg(unix)]
fn read_exact_at(file: &File, mut offset: u64, mut buf: &mut [u8]) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt as _;
    while !buf.is_empty() {
        let n = file.read_at(buf, offset)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected EOF",
            ));
        }
        offset = offset.saturating_add(u64::try_from(n).unwrap_or(u64::MAX));
        buf = &mut buf[n..];
    }
    Ok(())
}

#[cfg(windows)]
fn read_exact_at(file: &File, mut offset: u64, mut buf: &mut [u8]) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt as _;
    while !buf.is_empty() {
        let n = file.seek_read(buf, offset)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected EOF",
            ));
        }
        offset = offset.saturating_add(u64::try_from(n).unwrap_or(u64::MAX));
        buf = &mut buf[n..];
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn read_exact_at(file: &File, offset: u64, buf: &mut [u8]) -> std::io::Result<()> {
    use std::io::{Read as _, Seek as _, SeekFrom};
    let mut f = file.try_clone()?;
    f.seek(SeekFrom::Start(offset))?;
    f.read_exact(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use tempfile::tempdir;

    fn temp_file(bytes: &[u8]) -> (tempfile::TempDir, PathBuf, Arc<File>) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("reader.bin");
        {
            let mut file = File::create(&path).unwrap();
            file.write_all(bytes).unwrap();
            file.sync_all().unwrap();
        }
        let file = Arc::new(File::open(&path).unwrap());
        (dir, path, file)
    }

    #[test]
    fn bytebuf_from_vec_exposes_original_bytes() {
        let buf = ByteBuf::from_vec(vec![1, 2, 3, 4]);
        assert_eq!(buf.as_slice(), &[1, 2, 3, 4]);
        assert_eq!(buf.as_ref(), &[1, 2, 3, 4]);
    }

    #[test]
    fn file_access_reads_ranges_and_rejects_beyond_eof() {
        let (_dir, path, file) = temp_file(b"abcdef");
        let access = FileAccess::new(path.clone(), file);

        assert_eq!(access.len().unwrap(), 6);
        assert_eq!(access.read(1, 3).unwrap().as_slice(), b"bcd");

        match access.read(4, 3) {
            Err(SdJournalError::Transient {
                path: err_path,
                reason,
            }) => {
                assert_eq!(err_path, Some(path));
                assert_eq!(reason, "read beyond end of file (file_len=6, end=7)");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn file_access_reports_concurrent_truncation_as_transient() {
        let (_dir, path, file) = temp_file(b"abcdef");
        let access = FileAccess::new(path.clone(), file);
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(2)
            .unwrap();

        match access.read_known_valid(0, 6) {
            Err(SdJournalError::Transient {
                path: err_path,
                reason,
            }) => {
                assert_eq!(err_path, Some(path));
                assert_eq!(reason, "file was truncated while reading (offset=0, len=6)");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[cfg(feature = "mmap")]
    #[test]
    fn mmap_access_reads_ranges_and_rejects_beyond_eof() {
        let (_dir, path, file) = temp_file(b"abcdef");
        // SAFETY: The file is opened read-only for the duration of the test and is not mutated.
        let map = Arc::new(unsafe { memmap2::MmapOptions::new().map(file.as_ref()).unwrap() });
        let access = MmapAccess::new(path.clone(), file, map);

        assert_eq!(access.read(2, 2).unwrap().as_slice(), b"cd");

        match access.read(5, 2) {
            Err(SdJournalError::Transient {
                path: err_path,
                reason,
            }) => {
                assert_eq!(err_path, Some(path));
                assert_eq!(reason, "mmap read beyond end of file (file_len=6, end=7)");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[cfg(feature = "mmap")]
    #[test]
    fn bytebuf_from_mmap_rejects_out_of_bounds_range() {
        let (_dir, path, file) = temp_file(b"abcdef");
        // SAFETY: The file is opened read-only for the duration of the test and is not mutated.
        let map = Arc::new(unsafe { memmap2::MmapOptions::new().map(file.as_ref()).unwrap() });

        match ByteBuf::from_mmap(map, 2..99, &path) {
            Err(SdJournalError::Corrupt {
                path: err_path,
                offset,
                reason,
            }) => {
                assert_eq!(err_path, Some(path));
                assert_eq!(offset, None);
                assert_eq!(reason, "mmap range out of bounds");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }
}
