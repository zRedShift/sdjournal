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

#[cfg(test)]
thread_local! {
    static VALIDATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static ENTRY_END_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

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
        #[cfg(test)]
        VALIDATIONS.with(|count| count.set(count.get() + 1));
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
        self.prepare_access(state, file_len, config)?;
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

    fn prepare_access(&mut self, state: u8, file_len: u64, config: &JournalConfig) -> Result<()> {
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
        let _ = (state, file_len, config);
        Ok(())
    }
}

impl CachedAccess {
    pub(super) fn read_entry(
        &self,
        snapshot: &JournalFile,
        offset: u64,
    ) -> Result<crate::EntryRef> {
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
        let result = (|| {
            let (state, file_len) = reader.validate(
                &self.path,
                Some(self.identity),
                self.file_id,
                self.seqnum_id,
            )?;
            // Refuse before touching a stale map, with the existing truncation kind.
            if file_len < snapshot.inner.used_size {
                return Err(transient(&self.path, "file was truncated while reading"));
            }
            reader.prepare_access(state, file_len, &self.cache.config)?;
            let decoded = {
                let direct = snapshot.with_direct_access(reader.access.clone());
                direct
                    .read_entry_ref_inner(offset)
                    .map(crate::EntryRef::into_owned_payloads)
            }; // All temporary map/FD aliases die while the cache remains locked.
            #[cfg(test)]
            ENTRY_END_HOOK.with(|slot| {
                if let Some(hook) = slot.borrow_mut().take() {
                    hook();
                }
            });
            // Preserve the primary decoder/read error instead of masking it.
            let entry = decoded?;
            let (final_state, final_len) = reader.validate(
                &self.path,
                Some(self.identity),
                self.file_id,
                self.seqnum_id,
            )?;
            if final_state != state || final_len < file_len {
                return Err(transient(&self.path, "journal changed during entry read"));
            }
            Ok(entry)
        })();
        if result.is_ok() {
            entries.push_back(reader);
        }
        result
    }

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
    fn cached_entry_validates_at_entry_boundaries_not_per_field() {
        let (_root, paths) = files(65);
        for budget in [1, 2] {
            let config = JournalConfig {
                max_open_files: budget,
                ..Default::default()
            };
            let cache = ReaderCache::new(&config);
            let file = open(&cache, &paths[0]);
            let offset = file.inner.header.tail_entry_offset.unwrap();
            let reference = JournalFile::open(paths[0].clone(), &config)
                .unwrap()
                .read_entry_ref(offset)
                .unwrap();
            assert_eq!(reference.iter_fields().count(), 6);
            // Warm setup is excluded: count only the actual entry decode boundary.
            file.read_entry_ref(offset).unwrap();
            VALIDATIONS.with(|count| count.set(0));
            let actual = file.read_entry_ref(offset).unwrap();
            assert_eq!(snapshot(actual), snapshot(reference));
            let validations = VALIDATIONS.with(std::cell::Cell::get);
            assert_eq!(validations, 2, "warm six-field entry validation count");
        }
    }

    #[test]
    fn cached_entry_end_drift_refuses_and_retires_only_selected_reader() {
        for fault in 0..6 {
            let (_root, paths) = files(2);
            let cache = ReaderCache::new(&JournalConfig::default());
            let victim = open(&cache, &paths[0]);
            let healthy = open(&cache, &paths[1]);
            let offset = victim.inner.header.tail_entry_offset.unwrap();
            let path = paths[0].clone();
            ENTRY_END_HOOK.with(|slot| {
                *slot.borrow_mut() = Some(Box::new(move || match fault {
                    0 => {
                        let replacement = path.with_extension("new");
                        std::fs::copy(&path, &replacement).unwrap();
                        std::fs::rename(replacement, path).unwrap();
                    }
                    1 => patch(&path, 24, &[99; 16]),
                    2 => std::fs::remove_file(path).unwrap(),
                    3 => {
                        let len = std::fs::metadata(&path).unwrap().len();
                        std::fs::OpenOptions::new()
                            .write(true)
                            .open(path)
                            .unwrap()
                            .set_len(len - 1)
                            .unwrap();
                    }
                    4 => patch(&path, 16, &[crate::format::STATE_ONLINE]),
                    _ => patch(&path, 16, &[crate::format::STATE_ARCHIVED]),
                }))
            });
            assert!(victim.read_entry_ref(offset).is_err(), "fault {fault}");
            assert_eq!(cache.entries.lock().unwrap().len(), 1);
            assert!(
                healthy
                    .read_entry_ref(healthy.inner.header.tail_entry_offset.unwrap())
                    .is_ok()
            );
        }
    }

    #[test]
    fn cached_entry_online_start_and_append_keep_original_snapshot() {
        let (_root, paths) = files(1);
        let cache = ReaderCache::new(&JournalConfig::default());
        let old = open(&cache, &paths[0]);
        let offset = old.inner.header.tail_entry_offset.unwrap();
        let expected = snapshot(old.read_entry_ref(offset).unwrap());
        patch(&paths[0], 16, &[crate::format::STATE_ONLINE]);
        let path = paths[0].clone();
        ENTRY_END_HOOK.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                let len = std::fs::metadata(&path).unwrap().len();
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(path)
                    .unwrap()
                    .set_len(len + 4096)
                    .unwrap();
            }))
        });
        assert_eq!(snapshot(old.read_entry_ref(offset).unwrap()), expected);
        assert!(matches!(
            cache.entries.lock().unwrap()[0].access,
            Access::File(_)
        ));
        patch(&paths[0], 16, &[crate::format::STATE_ARCHIVED]);
        assert_eq!(snapshot(old.read_entry_ref(offset).unwrap()), expected);
        #[cfg(feature = "mmap")]
        assert!(matches!(
            cache.entries.lock().unwrap()[0].access,
            Access::Mmap(_)
        ));
        // New larger snapshot cannot enlarge an old entry's DATA object bounds.
        fixture::write_synthetic_journal_file(&paths[0], &["a.service"; 30], 1);
        let newer = open(&cache, &paths[0]);
        assert!(newer.inner.used_size > old.inner.used_size);
        let beyond_old = newer.inner.header.tail_entry_offset.unwrap();
        assert!(newer.read_entry_ref(beyond_old).is_ok());
        assert!(matches!(
            old.read_entry_ref(beyond_old),
            Err(SdJournalError::Corrupt { .. })
        ));
    }

    #[test]
    fn cached_entry_truncated_prefix_refuses_before_map_and_parse_error_stays_primary() {
        let (_root, paths) = files(1);
        let cache = ReaderCache::new(&JournalConfig::default());
        let file = open(&cache, &paths[0]);
        let offset = file.inner.header.tail_entry_offset.unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&paths[0])
            .unwrap()
            .set_len(file.inner.used_size - 1)
            .unwrap();
        VALIDATIONS.with(|count| count.set(0));
        assert!(matches!(
            file.read_entry_ref(offset),
            Err(SdJournalError::Transient { .. })
        ));
        assert_eq!(VALIDATIONS.with(std::cell::Cell::get), 1);
        assert!(cache.entries.lock().unwrap().is_empty());

        fixture::write_synthetic_journal_file(&paths[0], &["a.service"; 3], 1);
        let file = open(&cache, &paths[0]);
        let offset = file.inner.header.tail_entry_offset.unwrap();
        patch(&paths[0], offset, &[crate::format::OBJECT_DATA]);
        let path = paths[0].clone();
        ENTRY_END_HOOK
            .with(|slot| *slot.borrow_mut() = Some(Box::new(move || patch(&path, 16, &[255]))));
        VALIDATIONS.with(|count| count.set(0));
        match file.read_entry_ref(offset) {
            Err(SdJournalError::Corrupt { reason, .. }) => {
                assert!(reason.contains("expected ENTRY"))
            }
            other => panic!("expected primary ENTRY parse error, got {other:?}"),
        }
        assert_eq!(
            VALIDATIONS.with(std::cell::Cell::get),
            1,
            "no post-parse error masking"
        );
        assert!(cache.entries.lock().unwrap().is_empty());
    }

    #[test]
    fn cached_entry_keeps_field_limit_error_and_poison_kind() {
        let (_root, paths) = files(1);
        let cache = ReaderCache::new(&JournalConfig {
            max_fields_per_entry: 5,
            ..Default::default()
        });
        let file = open(&cache, &paths[0]);
        let offset = file.inner.header.tail_entry_offset.unwrap();
        assert!(matches!(
            file.read_entry_ref(offset),
            Err(SdJournalError::LimitExceeded {
                kind: crate::error::LimitKind::FieldsPerEntry,
                ..
            })
        ));
        assert!(cache.entries.lock().unwrap().is_empty());
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
            file.read_entry_ref(offset),
            Err(SdJournalError::Transient { .. })
        ));
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn cached_entry_compressed_payload_and_limits_match_eager() {
        let (_root, paths) = files(1);
        let bytes = std::fs::read(&paths[0]).unwrap();
        let offset = u64::from_le_bytes(bytes[264..272].try_into().unwrap());
        let item = offset as usize + 64 + 3 * 16; // MESSAGE DATA object.
        let data = u64::from_le_bytes(bytes[item..item + 8].try_into().unwrap());
        let payload = [b"MESSAGE=".as_slice(), &[b'x'; 1024]].concat();
        let mut packed = (payload.len() as u64).to_le_bytes().to_vec();
        packed.extend(lz4_flex::block::compress(&payload));
        let original_size = u64::from_le_bytes(
            bytes[data as usize + 8..data as usize + 16]
                .try_into()
                .unwrap(),
        );
        assert!(packed.len() as u64 <= original_size - 64);
        patch(
            &paths[0],
            12,
            &crate::format::HEADER_INCOMPATIBLE_COMPRESSED_LZ4.to_le_bytes(),
        );
        patch(&paths[0], data + 1, &[crate::format::OBJECT_COMPRESSED_LZ4]);
        patch(
            &paths[0],
            data + 8,
            &(64 + packed.len() as u64).to_le_bytes(),
        );
        patch(&paths[0], data + 64, &packed);
        let config = JournalConfig::default();
        let cache = ReaderCache::new(&config);
        let file = open(&cache, &paths[0]);
        let actual = file.read_entry_ref(offset).unwrap();
        assert_eq!(actual.get("MESSAGE"), Some(&payload[8..]));
        assert_eq!(
            snapshot(actual),
            snapshot(
                JournalFile::open(paths[0].clone(), &config)
                    .unwrap()
                    .read_entry_ref(offset)
                    .unwrap()
            )
        );
        for (config, kind) in [
            (
                JournalConfig {
                    max_decompressed_bytes: 64,
                    ..Default::default()
                },
                crate::error::LimitKind::DecompressedBytes,
            ),
            (
                JournalConfig {
                    max_object_size_bytes: 512,
                    ..Default::default()
                },
                crate::error::LimitKind::ObjectSizeBytes,
            ),
        ] {
            let cache = ReaderCache::new(&config);
            let file = open(&cache, &paths[0]);
            assert!(
                matches!(file.read_entry_ref(offset), Err(SdJournalError::LimitExceeded { kind: actual, .. }) if actual == kind)
            );
            assert!(cache.entries.lock().unwrap().is_empty());
        }
        // Advertised output remains valid but truncated compressed bytes fail decoding.
        patch(&paths[0], data + 8, &73u64.to_le_bytes());
        assert!(matches!(
            file.read_entry_ref(offset),
            Err(SdJournalError::DecompressFailed { .. })
        ));
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
