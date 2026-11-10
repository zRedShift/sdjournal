//! Explicit per-file progress for bounded, best-effort collection.
//!
//! This API never acknowledges delivery or writes checkpoints. Its positions
//! describe complete records; the caller commits only records it actually uses.
use std::{collections::{HashMap, VecDeque}, path::{Path, PathBuf}};

use crate::{Cursor, EntryRef, Journal, Result, SdJournalError};
use crate::cursor::{compare_entry_keys, same_entry};
use crate::file::{EntryMeta, FileEntryIter, JournalFile};
use crate::format::Header;
use crate::reader::ByteBuf;

/// A verified ENTRY location. Ordinals are one-based in the file's entry index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FilePosition {
    /// Logical journal file identifier.
    pub file_id: [u8; 16],
    /// ENTRY object offset, never a DATA/error offset.
    pub entry_offset: u64,
    /// One-based position in the validated file ENTRY_ARRAY.
    pub ordinal: u64,
    /// Sequence domain identifier.
    pub seqnum_id: [u8; 16],
    /// Expected ENTRY sequence number.
    pub seqnum: u64,
    /// Expected ENTRY payload XOR hash.
    pub xor_hash: u64,
}

impl FilePosition {
    pub(crate) fn from_meta(meta: EntryMeta, ordinal: u64) -> Self {
        Self { file_id: meta.file_id, entry_offset: meta.entry_offset, ordinal,
            seqnum_id: meta.seqnum_id, seqnum: meta.seqnum, xor_hash: meta.xor_hash }
    }
    pub(crate) fn matches(&self, meta: &EntryMeta) -> bool {
        self.file_id == meta.file_id && self.entry_offset == meta.entry_offset
            && self.seqnum_id == meta.seqnum_id && self.seqnum == meta.seqnum
            && self.xor_hash == meta.xor_hash
    }
}

/// A discovered path and, when known, its logical and physical owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileIdentity {
    /// Logical identifier; absent when opening the header failed.
    pub file_id: Option<[u8; 16]>,
    /// Actual discovery path.
    pub path: PathBuf,
    /// Observed filesystem device, when supported and readable.
    pub device: Option<u64>,
    /// Observed inode, when supported and readable.
    pub inode: Option<u64>,
}
impl FileIdentity {
    /// Observe an unknown-ID candidate. This performs one metadata operation.
    pub fn observe(path: &Path) -> Self {
        let mut result = Self { file_id: None, path: path.to_owned(), device: None, inode: None };
        #[cfg(unix)]
        if let Ok(metadata) = std::fs::metadata(path) {
            use std::os::unix::fs::MetadataExt as _;
            result.device = Some(metadata.dev());
            result.inode = Some(metadata.ino());
        }
        result
    }
    pub(crate) fn from_file(file: &JournalFile) -> Self {
        let mut identity = Self { file_id: Some(file.file_id()), path: file.path().to_owned(), device: None, inode: None };
        #[cfg(unix)]
        { let (device, inode) = file.file_identity(); identity.device = Some(device); identity.inode = Some(inode); }
        identity
    }
    pub(crate) fn defers(&self, path: &Path) -> bool {
        if self.path != path { return false; }
        let actual = Self::observe(path);
        match (self.device, self.inode, actual.device, actual.inode) {
            (Some(d), Some(i), Some(ad), Some(ai)) => d == ad && i == ai,
            _ => true,
        }
    }
}

/// Original finite file range. Contains scalar snapshot metadata, never an FD.
#[derive(Debug, Clone)]
pub struct FileBoundary {
    /// Original path/logical/physical identity.
    pub file: FileIdentity,
    /// Number of entries admitted in this snapshot.
    pub entry_count: u64,
    /// Original tail ENTRY offset when the file header supplies one.
    pub tail_offset: Option<u64>,
    pub(crate) header: Header,
    pub(crate) used_size: u64,
}

/// Explicit collection-local cache limits. The general query API never enables it.
#[derive(Debug, Clone, Copy)]
pub struct DataCacheConfig {
    /// Maximum cache-owned decoded payload bytes, excluding metadata/current entry.
    pub max_bytes: usize,
    /// Maximum unique DATA payloads retained.
    pub max_entries: usize,
}
impl Default for DataCacheConfig {
    fn default() -> Self { Self { max_bytes: 8 * 1024 * 1024, max_entries: 4096 } }
}

/// Positioned scan settings. Default behavior is uncached and unconstrained.
#[derive(Debug, Clone, Default)]
pub struct ScanOptions {
    /// Opt into immutable committed-DATA snapshot reuse for OFFLINE/ARCHIVED files.
    pub snapshot_cache: Option<DataCacheConfig>,
    /// Restrict a rediscovery to these original file identities/ranges.
    pub boundaries: Option<Vec<FileBoundary>>,
    /// Error-stopped candidates deferred during this immediate drain pass.
    pub deferred: Vec<FileIdentity>,
}

/// Cache accounting; the byte charge is not a process-memory limit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DataCacheStats {
    /// Validated payload hits.
    pub hits: u64,
    /// Eligible payload misses.
    pub misses: u64,
    /// Payloads bypassed because caching is ineligible or over budget.
    pub bypassed: u64,
    /// FIFO removals.
    pub evictions: u64,
    /// Currently retained payload byte charge.
    pub bytes: usize,
    /// Currently retained distinct payload count.
    pub entries: usize,
}

/// One owned record, omission, or bounded per-file diagnostic.
#[derive(Debug)]
pub enum ScanItem {
    /// A complete validated record. Commit positions only after using it.
    Entry {
        /// Fully owned field payloads.
        entry: EntryRef,
        /// Primary consumed position.
        position: FilePosition,
        /// Validated duplicate locations covered by the same logical record.
        aliases: Vec<FilePosition>,
    },
    /// A trusted index slot identifies one unreadable record.
    Omitted {
        /// Affected owner.
        file: FileIdentity,
        /// Trusted ENTRY slot offset.
        entry_offset: u64,
        /// Trusted one-based ordinal.
        ordinal: u64,
        /// Valid anchor only when ENTRY metadata itself was validated.
        position: Option<FilePosition>,
        /// Original typed error; never a fabricated cursor.
        error: SdJournalError,
    },
    /// Untrusted structure or transient I/O stops only this file.
    FileStopped {
        /// Affected owner, suitable for pass-local deferral.
        file: FileIdentity,
        /// Original failure.
        error: SdJournalError,
    },
    /// An invalid/missing checkpoint anchor replayed this file from its head.
    Replayed {
        /// Replayed file.
        file: FileIdentity,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct PayloadKey {
    file_id: [u8; 16],
    device: u64,
    inode: u64,
    extent: u64,
    offset: u64,
}
impl PayloadKey {
    fn new(file: &JournalFile, offset: u64) -> Self {
        #[cfg(unix)] let (device, inode) = file.file_identity();
        #[cfg(not(unix))] let (device, inode) = (0, 0);
        Self { file_id: file.file_id(), device, inode, extent: file.used_size(), offset }
    }
}

pub(crate) struct PayloadCache {
    limits: DataCacheConfig,
    values: HashMap<PayloadKey, (ByteBuf, usize)>,
    fifo: VecDeque<PayloadKey>,
    staged: Vec<(PayloadKey, ByteBuf, usize)>,
    stats: DataCacheStats,
}
impl PayloadCache {
    fn new(limits: DataCacheConfig) -> Self {
        Self { limits, values: HashMap::new(), fifo: VecDeque::new(), staged: Vec::new(), stats: DataCacheStats::default() }
    }
    pub(crate) fn begin(&mut self) { self.staged.clear(); }
    pub(crate) fn get(&mut self, file: &JournalFile, offset: u64) -> Option<(ByteBuf, usize)> {
        if file.header().state == crate::format::STATE_ONLINE {
            self.stats.bypassed = self.stats.bypassed.saturating_add(1); return None;
        }
        if let Some(value) = self.values.get(&PayloadKey::new(file, offset)) {
            self.stats.hits = self.stats.hits.saturating_add(1); return Some(value.clone());
        }
        self.stats.misses = self.stats.misses.saturating_add(1); None
    }
    pub(crate) fn stage(&mut self, file: &JournalFile, offset: u64, payload: &ByteBuf, eq: usize) {
        if file.header().state == crate::format::STATE_ONLINE { return; }
        if self.limits.max_entries == 0 || payload.as_slice().len() > self.limits.max_bytes {
            self.stats.bypassed = self.stats.bypassed.saturating_add(1); return;
        }
        let owned = match payload { ByteBuf::Owned(_) => payload.clone(), _ => ByteBuf::Owned(std::sync::Arc::from(payload.as_slice())) };
        self.staged.push((PayloadKey::new(file, offset), owned, eq));
    }
    pub(crate) fn finish(&mut self, success: bool) {
        if !success { self.staged.clear(); return; }
        for (key, payload, eq) in self.staged.drain(..) {
            if self.values.contains_key(&key) { continue; }
            let size = payload.as_slice().len();
            while self.values.len() >= self.limits.max_entries || self.stats.bytes.saturating_add(size) > self.limits.max_bytes {
                let Some(old) = self.fifo.pop_front() else { break; };
                if let Some((old, _)) = self.values.remove(&old) {
                    self.stats.bytes = self.stats.bytes.saturating_sub(old.as_slice().len());
                    self.stats.evictions = self.stats.evictions.saturating_add(1);
                }
            }
            self.stats.bytes += size;
            self.fifo.push_back(key);
            self.values.insert(key, (payload, eq));
        }
        self.stats.entries = self.values.len();
    }
}

struct FileScan {
    file: JournalFile,
    identity: FileIdentity,
    iter: FileEntryIter,
    next: Option<(EntryMeta, u64)>,
    stopped: bool,
}

/// Finite merge of independently resumed file suffixes.
///
/// Errors from one file are returned as items while other files remain eligible.
/// Retirement is a top-level error so the caller can rediscover once under its
/// original deadline. The caller alone decides which positions become durable.
pub struct PositionedScan {
    journal: Journal,
    files: Vec<FileScan>,
    notices: VecDeque<ScanItem>,
    cache: Option<PayloadCache>,
    done: bool,
}

fn omittable(error: &SdJournalError) -> bool {
    matches!(error, SdJournalError::Corrupt { .. } | SdJournalError::DecompressFailed { .. }
        | SdJournalError::LimitExceeded { .. } | SdJournalError::Unsupported { .. })
}

impl Journal {
    /// Capture original finite file tails without retaining native resources.
    pub fn snapshot_files(&self) -> Vec<FileBoundary> {
        self.inner.file_infos.iter().map(|info| FileBoundary {
            file: FileIdentity::from_file(&info.file), entry_count: info.file.header().n_entries,
            tail_offset: info.file.header().tail_entry_offset, header: info.file.header().clone(),
            used_size: info.file.used_size(),
        }).collect()
    }

    /// Resolve only the exact legacy anchor file, reading metadata rather than DATA.
    /// Other files are intentionally not inferred to have historical coverage.
    pub fn resolve_legacy_position(&self, cursor: &Cursor) -> Result<Option<FilePosition>> {
        let mut query = self.query();
        query.after_cursor(cursor.clone());
        let Some(meta) = crate::query::resolve_anchor(&query)? else { return Ok(None); };
        let Some(index) = self.inner.file_infos.iter().position(|file| file.file_id == meta.file_id) else { return Ok(None); };
        let file = self.inner.open_file_by_index(index)?;
        let (_, ordinal) = FileEntryIter::positioned(file, Some(&FilePosition::from_meta(meta, 0)))?;
        Ok(ordinal.map(|ordinal| FilePosition::from_meta(meta, ordinal)))
    }

    /// Scan only unread file suffixes using explicit acknowledged anchors.
    pub fn scan_from_positions(&self, positions: &[FilePosition], options: ScanOptions) -> Result<PositionedScan> {
        if positions.len() > self.inner.config.max_journal_files {
            return Err(SdJournalError::LimitExceeded { kind: crate::LimitKind::JournalFiles, limit: self.inner.config.max_journal_files as u64 });
        }
        let mut marks = HashMap::new();
        for position in positions {
            if position.ordinal == 0 || position.entry_offset == 0 || marks.insert(position.file_id, position).is_some() {
                return Err(SdJournalError::InvalidQuery { reason: "invalid or duplicate file position".into() });
            }
        }
        let mut scan = PositionedScan { journal: self.clone(), files: Vec::new(), notices: VecDeque::new(), cache: options.snapshot_cache.map(PayloadCache::new), done: false };
        for (index, info) in self.inner.file_infos.iter().enumerate() {
            if options.deferred.iter().any(|deferred| deferred.defers(&info.path)) { continue; }
            let boundary = options.boundaries.as_ref().and_then(|bounds| bounds.iter().find(|bound| bound.file.file_id == Some(info.file_id)));
            if options.boundaries.is_some() && boundary.is_none() { continue; }
            let mut file = self.inner.open_file_by_index(index)?;
            if let Some(boundary) = boundary { file = file.with_progress_boundary(boundary)?; }
            let identity = FileIdentity::from_file(&file);
            let position = marks.get(&info.file_id).copied();
            if let Some(position) = position
                && file.position_is_stable_tail(position)? { continue; }
            match FileEntryIter::positioned(file.clone(), position) {
                Ok((iter, matched)) => {
                    if position.is_some() && matched.is_none() { scan.notices.push_back(ScanItem::Replayed { file: identity.clone() }); }
                    scan.files.push(FileScan { file, identity, iter, next: None, stopped: false });
                }
                Err(error @ SdJournalError::Retired { .. }) => return Err(error),
                Err(error) => scan.notices.push_back(ScanItem::FileStopped { file: identity, error }),
            }
        }
        self.inner.validate_snapshot_owners()?;
        Ok(scan)
    }
}

impl PositionedScan {
    /// Current opt-in cache counters, or zeroes when disabled.
    pub fn cache_stats(&self) -> DataCacheStats { self.cache.as_ref().map_or(DataCacheStats::default(), |cache| cache.stats) }

    fn prime(&mut self, index: usize) -> Result<()> {
        let file = &mut self.files[index];
        if file.next.is_some() || file.stopped { return Ok(()); }
        loop {
            match file.iter.next_slot() {
                None => { file.stopped = true; file.file.validate_snapshot()?; return Ok(()); }
                Some(Err(error @ SdJournalError::Retired { .. })) => return Err(error),
                Some(Err(error)) => {
                    file.stopped = true; self.notices.push_back(ScanItem::FileStopped { file: file.identity.clone(), error }); return Ok(());
                }
                Some(Ok((offset, ordinal))) => match file.file.read_entry_meta(offset) {
                    Ok(meta) => { file.next = Some((meta, ordinal)); return Ok(()); }
                    Err(error @ SdJournalError::Retired { .. }) => return Err(error),
                    Err(error) if omittable(&error) => {
                        self.notices.push_back(ScanItem::Omitted { file: file.identity.clone(), entry_offset: offset, ordinal, position: None, error });
                        return Ok(());
                    }
                    Err(error) => {
                        file.stopped = true; self.notices.push_back(ScanItem::FileStopped { file: file.identity.clone(), error }); return Ok(());
                    }
                }
            }
        }
    }
}

impl Iterator for PositionedScan {
    type Item = Result<ScanItem>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done { return None; }
        if let Some(notice) = self.notices.pop_front() { return Some(Ok(notice)); }
        for index in 0..self.files.len() {
            if let Err(error) = self.prime(index) { self.done = true; return Some(Err(error)); }
            if let Some(notice) = self.notices.pop_front() { return Some(Ok(notice)); }
        }
        let mut selected: Option<usize> = None;
        for (index, file) in self.files.iter().enumerate() {
            if let Some((candidate, _)) = file.next {
                if let Some(best) = selected {
                    if let Some((current, _)) = self.files[best].next
                        && compare_entry_keys(&crate::query::meta_key(&candidate), &crate::query::meta_key(&current)).is_lt() { selected = Some(index); }
                } else { selected = Some(index); }
            }
        }
        let Some(index) = selected else {
            self.done = true; return self.journal.inner.validate_snapshot_owners().err().map(Err);
        };
        let Some((meta, ordinal)) = self.files[index].next.take() else { self.done = true; return None; };
        let position = FilePosition::from_meta(meta, ordinal);
        let result = self.files[index].file.read_entry_ref_with_cache(meta.entry_offset, self.cache.as_mut());
        match result {
            Ok(entry) => {
                let mut aliases = Vec::new();
                for (other_index, file) in self.files.iter_mut().enumerate() {
                    if other_index == index { continue; }
                    if let Some((other, ordinal)) = file.next
                        && same_entry(&crate::query::meta_key(&meta), &crate::query::meta_key(&other)) {
                        if let Err(error) = file.file.validate_snapshot() { self.done = true; return Some(Err(error)); }
                        aliases.push(FilePosition::from_meta(other, ordinal)); file.next = None;
                    }
                }
                Some(Ok(ScanItem::Entry { entry, position, aliases }))
            }
            Err(error @ SdJournalError::Retired { .. }) => { self.done = true; Some(Err(error)) }
            Err(error) if omittable(&error) => Some(Ok(ScanItem::Omitted { file: self.files[index].identity.clone(), entry_offset: meta.entry_offset, ordinal, position: Some(position), error })),
            Err(error) => {
                self.files[index].stopped = true;
                Some(Ok(ScanItem::FileStopped { file: self.files[index].identity.clone(), error }))
            }
        }
    }
}
