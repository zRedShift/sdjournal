//! Lazy snapshots share a bounded reader cache, not per-field file opens.
use super::{Access, JournalFile};
use crate::{
    config::JournalConfig,
    error::{Result, SdJournalError},
    reader::{ByteBuf, FileAccess},
};
use std::{
    collections::VecDeque,
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

pub(crate) struct ReaderCache {
    config: JournalConfig,
    entries: Mutex<VecDeque<CachedReader>>,
}

struct CachedReader {
    path: PathBuf,
    file: Arc<File>,
    access: Access,
    identity: (u64, u64),
    file_id: [u8; 16],
    seqnum_id: [u8; 16],
    mapped_len: u64,
}

#[derive(Clone)]
pub(super) struct CachedAccess {
    path: PathBuf,
    identity: (u64, u64),
    file_id: [u8; 16],
    seqnum_id: [u8; 16],
    cache: Arc<ReaderCache>,
}

fn transient(path: &Path, reason: &str) -> SdJournalError {
    SdJournalError::Transient {
        path: Some(path.to_owned()),
        reason: reason.to_owned(),
    }
}

fn open_error(path: &Path, error: std::io::Error) -> SdJournalError {
    if error.kind() == std::io::ErrorKind::PermissionDenied {
        SdJournalError::PermissionDenied {
            path: path.to_owned(),
        }
    } else {
        // Cache path validation has the same logical open-failure contract as
        // the old lazy reopen, even when no new descriptor is needed.
        SdJournalError::io("open", Some(path.to_owned()), error)
    }
}

impl ReaderCache {
    pub(crate) fn new(config: &JournalConfig) -> Arc<Self> {
        Arc::new(Self {
            config: config.clone(),
            entries: Mutex::new(VecDeque::new()),
        })
    }

    /// Opening/parsing snapshots also uses the budget: no temporary budget+1 FD.
    pub(crate) fn open_file(
        self: &Arc<Self>,
        path: PathBuf,
        file_id: [u8; 16],
        seqnum_id: [u8; 16],
    ) -> Result<JournalFile> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| transient(&path, "journal reader cache lock poisoned"))?;
        let mut reader = if let Some(index) = entries.iter().rposition(|entry| entry.path == path) {
            entries
                .remove(index)
                .ok_or_else(|| transient(&path, "journal reader cache slot disappeared"))?
        } else {
            if entries.len() == self.config.max_open_files {
                drop(entries.pop_front());
            }
            CachedReader::open(path.clone(), &self.config)?
        };
        reader.validate(&path, None, file_id, seqnum_id)?;
        // Drop an old map before parsing can create a replacement map.
        reader.access = Access::File(FileAccess::new(path.clone(), Arc::clone(&reader.file)));
        let mut snapshot = JournalFile::open_from_file(
            path.clone(),
            Arc::clone(&reader.file),
            &self.config,
            false,
        )?;
        if snapshot.file_id() != file_id || snapshot.seqnum_id() != seqnum_id {
            return Err(transient(
                &path,
                "journal file was replaced after discovery",
            ));
        }
        reader.access = snapshot.inner.access.clone();
        reader.mapped_len = snapshot.inner.file_len;
        let identity = snapshot.inner.file_identity;
        // This freshly constructed snapshot has no other owners yet.
        let inner = Arc::get_mut(&mut snapshot.inner)
            .ok_or_else(|| transient(&path, "new journal snapshot unexpectedly shared"))?;
        inner.access = Access::Cached(CachedAccess {
            path,
            identity,
            file_id,
            seqnum_id,
            cache: Arc::clone(self),
        });
        entries.push_back(reader);
        Ok(snapshot)
    }
}

impl CachedReader {
    fn open(path: PathBuf, config: &JournalConfig) -> Result<Self> {
        use std::os::unix::fs::MetadataExt as _;
        let file = Arc::new(File::open(&path).map_err(|error| open_error(&path, error))?);
        #[cfg(test)]
        crate::reader::test_io::opened(&path);
        let metadata = file
            .metadata()
            .map_err(|e| SdJournalError::io("metadata", Some(path.clone()), e))?;
        let snapshot = JournalFile::open_from_file(path.clone(), Arc::clone(&file), config, false)?;
        Ok(Self {
            path,
            file,
            access: snapshot.inner.access.clone(),
            identity: (metadata.dev(), metadata.ino()),
            file_id: snapshot.file_id(),
            seqnum_id: snapshot.seqnum_id(),
            mapped_len: metadata.len(),
        })
    }

    /// Always use explicit descriptor I/O for identity/state, never cached mmap.
    fn validate(
        &mut self,
        path: &Path,
        identity: Option<(u64, u64)>,
        file_id: [u8; 16],
        seqnum_id: [u8; 16],
    ) -> Result<(u8, u64)> {
        use std::os::unix::fs::MetadataExt as _;
        let metadata = std::fs::metadata(path).map_err(|error| open_error(path, error))?;
        if (metadata.dev(), metadata.ino()) != self.identity
            || identity.is_some_and(|id| id != self.identity)
        {
            return Err(transient(path, "journal file was replaced after discovery"));
        }
        let mut header = [0u8; 88];
        crate::reader::read_exact_at(self.file.as_ref(), 0, &mut header)
            .map_err(|error| crate::reader::read_failure(path, 0, header.len(), error))?;
        if self.file_id != file_id
            || self.seqnum_id != seqnum_id
            || header[24..40] != file_id
            || header[72..88] != seqnum_id
        {
            return Err(transient(path, "journal file was replaced after discovery"));
        }
        let state = header[16];
        if state > crate::format::STATE_ARCHIVED {
            return Err(SdJournalError::Corrupt {
                path: Some(path.to_owned()),
                offset: Some(16),
                reason: format!("unknown header state: {state}"),
            });
        }
        Ok((state, metadata.len()))
    }

    fn read(
        &mut self,
        snapshot: &CachedAccess,
        offset: u64,
        len: usize,
        config: &JournalConfig,
    ) -> Result<ByteBuf> {
        let (state, file_len) = self.validate(
            &snapshot.path,
            Some(snapshot.identity),
            snapshot.file_id,
            snapshot.seqnum_id,
        )?;
        let end = offset
            .checked_add(len as u64)
            .ok_or_else(|| transient(&self.path, "read range overflow"))?;
        if end > file_len {
            // Do not retain a mapping whose underlying file has shrunk.
            self.access = Access::File(FileAccess::new(self.path.clone(), Arc::clone(&self.file)));
            return Err(transient(&self.path, "file was truncated while reading"));
        }
        #[cfg(feature = "mmap")]
        {
            let allow_map = matches!(config.mmap_policy, crate::config::MmapPolicy::Auto)
                && (state != crate::format::STATE_ONLINE || config.allow_mmap_online);
            if !allow_map {
                self.access =
                    Access::File(FileAccess::new(self.path.clone(), Arc::clone(&self.file)));
            } else if !matches!(self.access, Access::Mmap(_)) || self.mapped_len != file_len {
                self.access =
                    Access::File(FileAccess::new(self.path.clone(), Arc::clone(&self.file)));
                // SAFETY: same stable-file/explicit-online-opt-in contract as
                // JournalFile::open_from_file; fresh state was read via the FD.
                // Concurrent malicious truncation remains outside that contract.
                let map = unsafe { memmap2::MmapOptions::new().map(self.file.as_ref()) }
                    .map_err(|e| SdJournalError::io("mmap", Some(self.path.clone()), e))?;
                self.access = Access::Mmap(crate::reader::MmapAccess::new(
                    self.path.clone(),
                    Arc::clone(&self.file),
                    Arc::new(map),
                ));
                self.mapped_len = file_len;
            }
        }
        #[cfg(not(feature = "mmap"))]
        let _ = (state, config);
        let bytes = match &self.access {
            Access::File(reader) => reader.read_known_valid(offset, len)?,
            #[cfg(feature = "mmap")]
            Access::Mmap(reader) => reader.read_known_valid(offset, len)?,
            _ => return Err(transient(&self.path, "invalid cached reader access")),
        };
        // Old lazy reads were owned. Preserve that boundary: no map/FD can
        // escape the bounded cache through a retained EntryRef/ByteBuf.
        match bytes {
            ByteBuf::Owned(_) => Ok(bytes),
            #[cfg(feature = "mmap")]
            ByteBuf::Mmap { .. } => Ok(ByteBuf::from_vec(bytes.as_slice().to_vec())),
        }
    }
}

impl CachedAccess {
    pub(super) fn read_known_valid(&self, offset: u64, len: usize) -> Result<ByteBuf> {
        let mut entries = self
            .cache
            .entries
            .lock()
            .map_err(|_| transient(&self.path, "journal reader cache lock poisoned"))?;
        let mut reader =
            if let Some(index) = entries.iter().rposition(|entry| entry.path == self.path) {
                entries
                    .remove(index)
                    .ok_or_else(|| transient(&self.path, "journal reader cache slot disappeared"))?
            } else {
                if entries.len() == self.cache.config.max_open_files {
                    drop(entries.pop_front());
                }
                CachedReader::open(self.path.clone(), &self.cache.config)?
            };
        let result = reader.read(self, offset, len, &self.cache.config);
        if result.is_ok() {
            entries.push_back(reader);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::FileExt as _;
    #[allow(dead_code)]
    mod fixture {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/synthetic_journal.rs"
        ));
    }

    fn files(count: u8) -> (tempfile::TempDir, Vec<PathBuf>) {
        let root = tempfile::tempdir().unwrap();
        let paths = (1..=count)
            .map(|seed| {
                let path = root.path().join(format!("{seed:03}.journal"));
                fixture::write_synthetic_journal_file(
                    &path,
                    &["a.service", "b.service", "a.service"],
                    seed,
                );
                patch(&path, 16, &[crate::format::STATE_OFFLINE]);
                path
            })
            .collect();
        (root, paths)
    }
    fn open(cache: &Arc<ReaderCache>, path: &Path) -> JournalFile {
        let bytes = std::fs::read(path).unwrap();
        cache
            .open_file(
                path.to_owned(),
                bytes[24..40].try_into().unwrap(),
                bytes[72..88].try_into().unwrap(),
            )
            .unwrap()
    }
    fn patch(path: &Path, offset: u64, bytes: &[u8]) {
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .write_all_at(bytes, offset)
            .unwrap();
    }
    fn snapshot(entry: crate::EntryRef) -> (String, Vec<(String, Vec<u8>)>) {
        (
            entry.cursor().unwrap().to_string(),
            entry
                .iter_fields()
                .map(|(k, v)| (k.to_owned(), v.to_vec()))
                .collect(),
        )
    }

    #[test]
    fn lazy_cache_churn_preserves_full_queries_and_retained_payloads() {
        let (root, _) = files(65);
        let eager = crate::Journal::open_dir_with_config(
            root.path(),
            JournalConfig {
                max_open_files: 65,
                ..Default::default()
            },
        )
        .unwrap();
        let reference: Vec<_> = eager
            .query()
            .iter()
            .unwrap()
            .map(|v| snapshot(v.unwrap()))
            .collect();
        let cursor = eager
            .query()
            .iter()
            .unwrap()
            .nth(87)
            .unwrap()
            .unwrap()
            .cursor()
            .unwrap();
        let mut filtered = eager.query();
        filtered
            .match_unit("a.service")
            .after_cursor(cursor.clone())
            .reverse(true);
        let expected_filtered: Vec<_> = filtered
            .iter()
            .unwrap()
            .map(|v| snapshot(v.unwrap()))
            .collect();
        drop(filtered);
        drop(eager);
        for budget in [1, 2, 64] {
            let journal = crate::Journal::open_dir_with_config(
                root.path(),
                JournalConfig {
                    max_open_files: budget,
                    ..Default::default()
                },
            )
            .unwrap();
            let weak = Arc::downgrade(&journal.inner.reader_cache);
            crate::reader::test_io::observe_root(root.path());
            let retained: Vec<_> = journal
                .query()
                .iter()
                .unwrap()
                .map(Result::unwrap)
                .collect();
            assert_eq!(
                retained.iter().cloned().map(snapshot).collect::<Vec<_>>(),
                reference
            );
            let mut selected = journal.query();
            selected
                .match_unit("a.service")
                .after_cursor(cursor.clone())
                .reverse(true);
            assert_eq!(
                selected
                    .iter()
                    .unwrap()
                    .map(|v| snapshot(v.unwrap()))
                    .collect::<Vec<_>>(),
                expected_filtered
            );
            // Access handles from all files interleave, rather than only hot-file sequential reads.
            let handles: Vec<_> = (0..journal.inner.file_count())
                .map(|i| journal.inner.open_file_by_index(i).unwrap())
                .collect();
            for file in handles.iter().rev().chain(handles.iter()) {
                assert!(matches!(file.read_bytes(0, 88).unwrap(), ByteBuf::Owned(_)));
                assert!(journal.inner.reader_cache.entries.lock().unwrap().len() <= budget);
            }
            if budget < 65 {
                assert!(
                    crate::reader::test_io::opens(&root.path().join("001.journal")) > 1,
                    "interleaved working sets larger than the cache still have cold misses"
                );
            }
            #[cfg(target_os = "linux")]
            {
                let (fds, maps) = crate::reader::test_io::peaks();
                assert!(fds > 0 && fds <= budget, "FD peak {fds}, budget {budget}");
                assert!(maps <= budget, "map peak {maps}, budget {budget}");
            }
            drop(handles);
            drop(selected);
            drop(journal);
            assert!(
                weak.upgrade().is_none(),
                "retained entries must not retain the cache/map/FD owner"
            );
            assert_eq!(
                retained.into_iter().map(snapshot).collect::<Vec<_>>(),
                reference
            );
        }
    }

    #[test]
    fn cached_state_flip_demotes_before_map_access_and_can_promote_again() {
        let (_root, paths) = files(1);
        let cache = ReaderCache::new(&JournalConfig::default());
        let file = open(&cache, &paths[0]);
        file.read_bytes(0, 88).unwrap();
        #[cfg(feature = "mmap")]
        assert!(matches!(
            cache.entries.lock().unwrap()[0].access,
            Access::Mmap(_)
        ));
        patch(&paths[0], 16, &[crate::format::STATE_ONLINE]);
        assert_eq!(
            file.read_bytes(0, 88).unwrap().as_slice()[16],
            crate::format::STATE_ONLINE
        );
        assert!(matches!(
            cache.entries.lock().unwrap()[0].access,
            Access::File(_)
        ));
        patch(&paths[0], 16, &[crate::format::STATE_ARCHIVED]);
        file.read_bytes(0, 88).unwrap();
        #[cfg(feature = "mmap")]
        assert!(matches!(
            cache.entries.lock().unwrap()[0].access,
            Access::Mmap(_)
        ));
        let never = ReaderCache::new(&JournalConfig {
            mmap_policy: crate::MmapPolicy::Never,
            ..Default::default()
        });
        open(&never, &paths[0]).read_bytes(0, 88).unwrap();
        assert!(matches!(
            never.entries.lock().unwrap()[0].access,
            Access::File(_)
        ));
    }

    #[test]
    fn cached_snapshots_do_not_expand_with_newer_shared_extent() {
        let (_root, paths) = files(1);
        let cache = ReaderCache::new(&JournalConfig::default());
        let old = open(&cache, &paths[0]);
        let bound = old.inner.used_size;
        fixture::write_synthetic_journal_file(&paths[0], &["a.service"; 30], 1);
        let newer = open(&cache, &paths[0]);
        assert!(newer.inner.used_size > bound);
        assert!(newer.read_bytes(bound, 1).is_ok());
        assert!(matches!(
            old.read_bytes(bound, 1),
            Err(SdJournalError::Corrupt { .. })
        ));
        assert!(matches!(
            old.read_bytes(u64::MAX, 2),
            Err(SdJournalError::Corrupt { .. })
        ));
    }

    #[test]
    fn cached_replacement_identity_removal_and_truncation_refuse_and_retire() {
        for fault in 0..4 {
            let (_root, paths) = files(2);
            let cache = ReaderCache::new(&JournalConfig {
                max_open_files: 2,
                ..Default::default()
            });
            let victim = open(&cache, &paths[0]);
            let healthy = open(&cache, &paths[1]);
            match fault {
                0 => {
                    let replacement = paths[0].with_extension("replacement");
                    std::fs::copy(&paths[0], &replacement).unwrap();
                    std::fs::rename(replacement, &paths[0]).unwrap();
                }
                1 => patch(&paths[0], 24, &[99; 16]),
                2 => std::fs::remove_file(&paths[0]).unwrap(),
                _ => std::fs::OpenOptions::new()
                    .write(true)
                    .open(&paths[0])
                    .unwrap()
                    .set_len(90)
                    .unwrap(),
            }
            // Truncation refuses BEFORE touching the old mmap.
            assert!(victim.read_bytes(0, 100).is_err());
            assert_eq!(cache.entries.lock().unwrap().len(), 1);
            assert!(healthy.read_bytes(0, 88).is_ok());
        }
    }

    #[test]
    fn cached_cold_miss_failure_and_poison_are_typed_and_do_not_leak_readers() {
        let (_root, paths) = files(2);
        let cache = ReaderCache::new(&JournalConfig {
            max_open_files: 1,
            ..Default::default()
        });
        let old = open(&cache, &paths[0]);
        open(&cache, &paths[1]); // evicts old
        std::fs::remove_file(&paths[0]).unwrap();
        assert!(old.read_bytes(0, 88).is_err());
        assert!(cache.entries.lock().unwrap().is_empty());
        let file = open(&cache, &paths[1]);
        let other = Arc::clone(&cache);
        assert!(
            std::thread::spawn(move || {
                let _guard = other.entries.lock().unwrap();
                panic!("test-only poison");
            })
            .join()
            .is_err()
        );
        assert!(matches!(
            file.read_bytes(0, 88),
            Err(SdJournalError::Transient { .. })
        ));
    }
}
