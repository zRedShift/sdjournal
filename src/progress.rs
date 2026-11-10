//! Explicit per-file progress for bounded, best-effort collection.
//!
//! This API never acknowledges delivery or writes checkpoints. Its positions
//! describe complete records; the caller commits only records it actually uses.
use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
};

use crate::cursor::{compare_entry_keys, same_entry};
use crate::file::{EntryMeta, FileEntryIter, JournalFile};
use crate::format::Header;
use crate::reader::ByteBuf;
use crate::{Cursor, EntryRef, Journal, Result, SdJournalError};

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
        Self {
            file_id: meta.file_id,
            entry_offset: meta.entry_offset,
            ordinal,
            seqnum_id: meta.seqnum_id,
            seqnum: meta.seqnum,
            xor_hash: meta.xor_hash,
        }
    }
    pub(crate) fn matches(&self, meta: &EntryMeta) -> bool {
        self.file_id == meta.file_id
            && self.entry_offset == meta.entry_offset
            && self.seqnum_id == meta.seqnum_id
            && self.seqnum == meta.seqnum
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
        let mut result = Self {
            file_id: None,
            path: path.to_owned(),
            device: None,
            inode: None,
        };
        #[cfg(unix)]
        if let Ok(metadata) = std::fs::metadata(path) {
            use std::os::unix::fs::MetadataExt as _;
            result.device = Some(metadata.dev());
            result.inode = Some(metadata.ino());
        }
        result
    }
    pub(crate) fn from_file(file: &JournalFile) -> Self {
        let mut identity = Self {
            file_id: Some(file.file_id()),
            path: file.path().to_owned(),
            device: None,
            inode: None,
        };
        #[cfg(unix)]
        {
            let (device, inode) = file.file_identity();
            identity.device = Some(device);
            identity.inode = Some(inode);
        }
        identity
    }
    pub(crate) fn defers(&self, path: &Path) -> bool {
        if self.path != path {
            return false;
        }
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
///
/// Hits reuse previously verified owned DATA from this captured OFFLINE/ARCHIVED
/// snapshot without rereading its backing bytes. A linked, same-inode rewrite
/// can retain the earlier value until the scan ends; a fresh scan reads the
/// changed data. This opt-in policy relies on committed DATA immutability, not
/// metadata timestamps as write epochs. Native owner/publication guards still run.
#[derive(Debug, Clone, Copy)]
pub struct DataCacheConfig {
    /// Maximum retained decoded payload bytes. This is not total process memory:
    /// bounded map/FIFO/staging metadata, the current entry and externally held
    /// Arc payload owners are excluded.
    pub max_bytes: usize,
    /// Maximum unique DATA payloads retained.
    pub max_entries: usize,
}
impl Default for DataCacheConfig {
    fn default() -> Self {
        Self {
            max_bytes: 8 * 1024 * 1024,
            max_entries: 4096,
        }
    }
}

/// Positioned scan settings. Default behavior is uncached and unconstrained.
#[derive(Debug, Clone, Default)]
pub struct ScanOptions {
    /// Opt into immutable committed-DATA snapshot reuse for OFFLINE/ARCHIVED files.
    /// See [`DataCacheConfig`] for same-snapshot rewrite visibility and byte scope.
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
        #[cfg(unix)]
        let (device, inode) = file.file_identity();
        #[cfg(not(unix))]
        let (device, inode) = (0, 0);
        Self {
            file_id: file.file_id(),
            device,
            inode,
            extent: file.used_size(),
            offset,
        }
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
        Self {
            limits,
            values: HashMap::new(),
            fifo: VecDeque::new(),
            staged: Vec::new(),
            stats: DataCacheStats::default(),
        }
    }
    pub(crate) fn begin(&mut self) {
        self.staged.clear();
    }
    pub(crate) fn get(&mut self, file: &JournalFile, offset: u64) -> Option<(ByteBuf, usize)> {
        if !matches!(
            file.header().state,
            crate::format::STATE_OFFLINE | crate::format::STATE_ARCHIVED
        ) {
            self.stats.bypassed = self.stats.bypassed.saturating_add(1);
            return None;
        }
        if let Some(value) = self.values.get(&PayloadKey::new(file, offset)) {
            self.stats.hits = self.stats.hits.saturating_add(1);
            return Some(value.clone());
        }
        self.stats.misses = self.stats.misses.saturating_add(1);
        None
    }
    pub(crate) fn stage(&mut self, file: &JournalFile, offset: u64, payload: &ByteBuf, eq: usize) {
        if !matches!(
            file.header().state,
            crate::format::STATE_OFFLINE | crate::format::STATE_ARCHIVED
        ) {
            return;
        }
        if self.limits.max_entries == 0 || payload.as_slice().len() > self.limits.max_bytes {
            self.stats.bypassed = self.stats.bypassed.saturating_add(1);
            return;
        }
        let owned = match payload {
            ByteBuf::Owned(_) => payload.clone(),
            #[cfg(feature = "mmap")]
            ByteBuf::Mmap { .. } => ByteBuf::Owned(std::sync::Arc::from(payload.as_slice())),
        };
        self.staged.push((PayloadKey::new(file, offset), owned, eq));
    }
    pub(crate) fn finish(&mut self, success: bool) {
        if !success {
            self.staged.clear();
            return;
        }
        for (key, payload, eq) in self.staged.drain(..) {
            if self.values.contains_key(&key) {
                continue;
            }
            let size = payload.as_slice().len();
            while self.values.len() >= self.limits.max_entries
                || self.stats.bytes.saturating_add(size) > self.limits.max_bytes
            {
                let Some(old) = self.fifo.pop_front() else {
                    break;
                };
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

struct ScanOwner {
    file: JournalFile,
    reported_failure: bool,
}

struct FileScan {
    owner_index: usize,
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
    owners: Vec<ScanOwner>,
    files: Vec<FileScan>,
    notices: VecDeque<ScanItem>,
    cache: Option<PayloadCache>,
    done: bool,
}

fn omittable(error: &SdJournalError) -> bool {
    matches!(
        error,
        SdJournalError::Corrupt { .. }
            | SdJournalError::DecompressFailed { .. }
            | SdJournalError::LimitExceeded { .. }
            | SdJournalError::Unsupported { .. }
    )
}

impl Journal {
    /// Capture original finite file tails without retaining native resources.
    pub fn snapshot_files(&self) -> Vec<FileBoundary> {
        self.inner
            .file_infos
            .iter()
            .map(|info| FileBoundary {
                file: FileIdentity::from_file(&info.file),
                entry_count: info.file.header().n_entries,
                tail_offset: info.file.header().tail_entry_offset,
                header: info.file.header().clone(),
                used_size: info.file.used_size(),
            })
            .collect()
    }

    /// Resolve only the exact legacy anchor file, reading metadata rather than DATA.
    /// Other files are intentionally not inferred to have historical coverage.
    pub fn resolve_legacy_position(&self, cursor: &Cursor) -> Result<Option<FilePosition>> {
        let mut query = self.query();
        query.after_cursor(cursor.clone());
        if cursor.systemd().is_none() {
            let Some(meta) = crate::query::resolve_anchor(&query)? else {
                return Ok(None);
            };
            let Some(index) = self
                .inner
                .file_infos
                .iter()
                .position(|file| file.file_id == meta.file_id)
            else {
                return Ok(None);
            };
            let file = self.inner.open_file_by_index(index)?;
            let (_, ordinal) =
                FileEntryIter::positioned(file, Some(&FilePosition::from_meta(meta, 0)))?;
            return Ok(ordinal.map(|ordinal| FilePosition::from_meta(meta, ordinal)));
        }
        let Some(fields) = cursor.systemd() else {
            return Ok(None);
        };
        let (
            Some(seqnum_id),
            Some(seqnum),
            Some(boot_id),
            Some(monotonic),
            Some(realtime),
            Some(hash),
        ) = (
            fields.seqnum_id,
            fields.seqnum,
            fields.boot_id,
            fields.monotonic_usec,
            fields.realtime_usec,
            fields.xor_hash,
        )
        else {
            return Ok(None);
        };
        let mut found = None;
        for (index, info) in self.inner.file_infos.iter().enumerate() {
            if info.seqnum_id != seqnum_id {
                continue;
            }
            let file = self.inner.open_file_by_index(index)?;
            let (mut iter, _) = FileEntryIter::positioned(file.clone(), None)?;
            while let Some(slot) = iter.next_slot() {
                let (offset, ordinal) = slot?;
                let meta = file.read_entry_meta(offset)?;
                if meta.seqnum == seqnum
                    && meta.boot_id == boot_id
                    && meta.monotonic_usec == monotonic
                    && meta.realtime_usec == realtime
                    && meta.xor_hash == hash
                {
                    if found.is_some() {
                        return Ok(None);
                    }
                    found = Some(FilePosition::from_meta(meta, ordinal));
                }
            }
            file.validate_snapshot()?;
        }
        Ok(found)
    }

    /// Scan only unread file suffixes using explicit acknowledged anchors.
    pub fn scan_from_positions(
        &self,
        positions: &[FilePosition],
        options: ScanOptions,
    ) -> Result<PositionedScan> {
        if positions.len() > self.inner.config.max_journal_files
            || options.deferred.len() > self.inner.config.max_journal_files
            || options
                .boundaries
                .as_ref()
                .is_some_and(|bounds| bounds.len() > self.inner.config.max_journal_files)
        {
            return Err(SdJournalError::LimitExceeded {
                kind: crate::LimitKind::JournalFiles,
                limit: self.inner.config.max_journal_files as u64,
            });
        }
        let mut marks = HashMap::new();
        for position in positions {
            if position.ordinal == 0
                || position.entry_offset == 0
                || marks.insert(position.file_id, position).is_some()
            {
                return Err(SdJournalError::InvalidQuery {
                    reason: "invalid or duplicate file position".into(),
                });
            }
        }
        let mut scan = PositionedScan {
            owners: Vec::new(),
            files: Vec::new(),
            notices: VecDeque::new(),
            cache: options.snapshot_cache.map(PayloadCache::new),
            done: false,
        };
        if let Some(boundaries) = &options.boundaries {
            let mut ids = std::collections::HashSet::new();
            for bound in boundaries {
                if !ids.insert(bound.file.file_id)
                    || bound.file.file_id != Some(bound.header.file_id)
                    || bound.entry_count != bound.header.n_entries
                    || bound.tail_offset != bound.header.tail_entry_offset
                {
                    return Err(SdJournalError::InvalidQuery {
                        reason: "invalid or duplicate original file boundary".into(),
                    });
                }
                if !self
                    .inner
                    .file_infos
                    .iter()
                    .any(|file| Some(file.file_id) == bound.file.file_id)
                {
                    scan.notices.push_back(ScanItem::FileStopped {
                        file: bound.file.clone(),
                        error: SdJournalError::Retired {
                            path: bound.file.path.clone(),
                            reason: crate::RetirementReason::Disappeared,
                        },
                    });
                }
            }
        }
        for (index, info) in self.inner.file_infos.iter().enumerate() {
            if options
                .deferred
                .iter()
                .any(|deferred| deferred.defers(&info.path))
            {
                continue;
            }
            let boundary = options.boundaries.as_ref().and_then(|bounds| {
                bounds
                    .iter()
                    .find(|bound| bound.file.file_id == Some(info.file_id))
            });
            if options.boundaries.is_some() && boundary.is_none() {
                continue;
            }
            let mut file = match self.inner.open_file_by_index(index) {
                Ok(file) => file,
                Err(error @ SdJournalError::Retired { .. }) => return Err(error),
                Err(error) => {
                    scan.notices.push_back(ScanItem::FileStopped {
                        file: FileIdentity::from_file(&info.file),
                        error,
                    });
                    continue;
                }
            };
            if let Some(boundary) = boundary {
                file = match file.with_progress_boundary(boundary) {
                    Ok(file) => file,
                    Err(error) => {
                        scan.notices.push_back(ScanItem::FileStopped {
                            file: boundary.file.clone(),
                            error,
                        });
                        continue;
                    }
                };
            }
            let identity = FileIdentity::from_file(&file);
            let owner_index = scan.owners.len();
            scan.owners.push(ScanOwner {
                file: file.clone(),
                reported_failure: false,
            });
            let position = marks.get(&info.file_id).copied();
            if let Some(position) = position {
                match file.position_is_stable_tail(position) {
                    Ok(true) => continue,
                    Ok(false) => {}
                    Err(error @ SdJournalError::Retired { .. }) => return Err(error),
                    Err(error) => {
                        scan.report_owner_error(owner_index, error)?;
                        continue;
                    }
                }
            }
            match FileEntryIter::positioned(file.clone(), position) {
                Ok((iter, matched)) => {
                    if position.is_some() && matched.is_none() {
                        scan.notices.push_back(ScanItem::Replayed {
                            file: identity.clone(),
                        });
                    }
                    scan.files.push(FileScan {
                        owner_index,
                        file,
                        identity,
                        iter,
                        next: None,
                        stopped: false,
                    });
                }
                Err(error @ SdJournalError::Retired { .. }) => return Err(error),
                Err(error) => scan.report_owner_error(owner_index, error)?,
            }
        }
        scan.validate_owners()?;
        Ok(scan)
    }
}

impl PositionedScan {
    // Keep retirement global, but a linked per-file failure must never poison
    // a healthy peer. A failed owner is still rechecked for actual retirement.
    fn report_owner_error(&mut self, index: usize, error: SdJournalError) -> Result<()> {
        if matches!(error, SdJournalError::Retired { .. }) {
            return Err(error);
        }
        let owner = &mut self.owners[index];
        if !owner.reported_failure {
            self.notices.push_back(ScanItem::FileStopped {
                file: FileIdentity::from_file(&owner.file),
                error,
            });
            owner.reported_failure = true;
        }
        for file in &mut self.files {
            if file.owner_index == index {
                file.stopped = true;
                file.next = None;
            }
        }
        Ok(())
    }

    fn validate_owners(&mut self) -> Result<()> {
        for index in 0..self.owners.len() {
            if let Err(error) = self.owners[index].file.validate_snapshot() {
                self.report_owner_error(index, error)?;
            }
        }
        Ok(())
    }

    /// Whether a non-stopped file has queued metadata or validated unread slots.
    /// Does not perform I/O or promise the unread payloads are healthy. A yielded
    /// record rejected by the caller's size cap is additional unconsumed work.
    pub fn has_pending_entries(&self) -> bool {
        !self.done
            && self.files.iter().any(|file| {
                !file.stopped && (file.next.is_some() || file.iter.has_remaining_slots())
            })
    }

    /// Current opt-in cache counters, or zeroes when disabled.
    pub fn cache_stats(&self) -> DataCacheStats {
        self.cache
            .as_ref()
            .map_or(DataCacheStats::default(), |cache| cache.stats)
    }

    fn prime(&mut self, index: usize) -> Result<()> {
        let file = &mut self.files[index];
        if file.next.is_some() || file.stopped {
            return Ok(());
        }
        match file.iter.next_slot() {
            None => {
                file.stopped = true;
                file.file.validate_snapshot()?;
                Ok(())
            }
            Some(Err(error)) => Err(error),
            Some(Ok((offset, ordinal))) => match file.file.read_entry_meta(offset) {
                Ok(meta) => {
                    file.next = Some((meta, ordinal));
                    Ok(())
                }
                Err(error @ SdJournalError::Retired { .. }) => Err(error),
                Err(error) if omittable(&error) => {
                    self.notices.push_back(ScanItem::Omitted {
                        file: file.identity.clone(),
                        entry_offset: offset,
                        ordinal,
                        position: None,
                        error,
                    });
                    Ok(())
                }
                Err(error) => Err(error),
            },
        }
    }
}

impl Iterator for PositionedScan {
    type Item = Result<ScanItem>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(notice) = self.notices.pop_front() {
            return Some(Ok(notice));
        }
        if self.done {
            return None;
        }
        for index in 0..self.files.len() {
            if let Err(error) = self.prime(index)
                && let Err(error) = self.report_owner_error(self.files[index].owner_index, error)
            {
                self.done = true;
                self.notices.clear();
                return Some(Err(error));
            }
            if let Some(notice) = self.notices.pop_front() {
                return Some(Ok(notice));
            }
        }
        let mut selected: Option<usize> = None;
        for (index, file) in self.files.iter().enumerate() {
            if let Some((candidate, _)) = file.next {
                if let Some(best) = selected {
                    if let Some((current, _)) = self.files[best].next
                        && compare_entry_keys(
                            &crate::query::meta_key(&candidate),
                            &crate::query::meta_key(&current),
                        )
                        .is_lt()
                    {
                        selected = Some(index);
                    }
                } else {
                    selected = Some(index);
                }
            }
        }
        let Some(index) = selected else {
            self.done = true;
            if let Err(error) = self.validate_owners() {
                self.notices.clear();
                return Some(Err(error));
            }
            return self.notices.pop_front().map(Ok);
        };
        let Some((meta, ordinal)) = self.files[index].next.take() else {
            self.done = true;
            return None;
        };
        let position = FilePosition::from_meta(meta, ordinal);
        let result = self.files[index]
            .file
            .read_entry_ref_with_cache(meta.entry_offset, self.cache.as_mut());
        match result {
            Ok(entry) => {
                let mut aliases = Vec::new();
                for other_index in 0..self.files.len() {
                    if other_index == index {
                        continue;
                    }
                    if let Some((other, ordinal)) = self.files[other_index].next
                        && same_entry(
                            &crate::query::meta_key(&meta),
                            &crate::query::meta_key(&other),
                        )
                    {
                        if let Err(error) = self.files[other_index].file.validate_snapshot() {
                            if let Err(error) =
                                self.report_owner_error(self.files[other_index].owner_index, error)
                            {
                                self.done = true;
                                self.notices.clear();
                                return Some(Err(error));
                            }
                            continue;
                        }
                        aliases.push(FilePosition::from_meta(other, ordinal));
                        self.files[other_index].next = None;
                    }
                }
                Some(Ok(ScanItem::Entry {
                    entry,
                    position,
                    aliases,
                }))
            }
            Err(error @ SdJournalError::Retired { .. }) => {
                self.done = true;
                self.notices.clear();
                Some(Err(error))
            }
            Err(error) if omittable(&error) => Some(Ok(ScanItem::Omitted {
                file: self.files[index].identity.clone(),
                entry_offset: meta.entry_offset,
                ordinal,
                position: Some(position),
                error,
            })),
            Err(error) => {
                if let Err(error) = self.report_owner_error(self.files[index].owner_index, error) {
                    self.done = true;
                    self.notices.clear();
                    return Some(Err(error));
                }
                self.notices.pop_front().map(Ok)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{JournalConfig, MmapPolicy, reader::test_io};
    #[allow(dead_code)]
    mod fixture {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/synthetic_journal.rs"
        ));
    }
    use std::os::unix::fs::FileExt as _;

    fn open(root: &Path) -> Journal {
        Journal::open_progress_dirs(
            &[root.to_owned()],
            JournalConfig {
                mmap_policy: MmapPolicy::Never,
                ..Default::default()
            },
            &[],
        )
        .unwrap()
    }
    fn number(bytes: &[u8], offset: usize) -> u64 {
        u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
    }
    fn shared_fixture(n: usize) -> fixture::SyntheticJournalFile {
        let f = fixture::SyntheticJournalFile::new(&vec!["same"; n]);
        let path = f.root().join("synthetic.journal");
        let bytes = std::fs::read(&path).unwrap();
        let array = number(&bytes, 176) as usize;
        let entries: Vec<_> = (0..n).map(|i| number(&bytes, array + 24 + i * 8)).collect();
        let owner = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        for &entry in &entries[1..] {
            for index in [0, 1, 2, 5] {
                let offset = number(&bytes, entries[0] as usize + 64 + index * 16);
                owner
                    .write_all_at(&offset.to_le_bytes(), entry + 64 + index as u64 * 16)
                    .unwrap();
            }
        }
        owner.write_all_at(&[0], 16).unwrap();
        f
    }
    #[test]
    fn actual_descriptor_reads_drop_with_validated_payload_reuse() {
        let f = shared_fixture(256);
        let journal = open(f.root());
        test_io::reset();
        let plain = journal
            .scan_from_positions(&[], ScanOptions::default())
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let uncached = test_io::reads();
        test_io::reset();
        let mut scan = journal
            .scan_from_positions(
                &[],
                ScanOptions {
                    snapshot_cache: Some(DataCacheConfig::default()),
                    ..Default::default()
                },
            )
            .unwrap();
        let cached = scan.by_ref().collect::<Result<Vec<_>>>().unwrap();
        let reads = test_io::reads();
        let fields = |items: Vec<ScanItem>| {
            items
                .into_iter()
                .map(|x| match x {
                    ScanItem::Entry { entry, .. } => entry
                        .iter_fields()
                        .map(|(name, value)| (name.to_owned(), value.to_vec()))
                        .collect::<Vec<_>>(),
                    x => panic!("{x:?}"),
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(fields(plain), fields(cached));
        assert!(
            reads * 10 < uncached * 7,
            "uncached={uncached},cached={reads}"
        );
        assert!(scan.cache_stats().hits >= 255 * 4);
        let cache = scan.cache.as_ref().unwrap();
        assert_eq!(cache.fifo.len(), cache.values.len());
        println!(
            "actual pread calls: uncached={uncached},cached={reads},stats={:?}",
            scan.cache_stats()
        );
    }
    #[test]
    fn stable_eof_uses_constant_reads_instead_of_historical_entry_headers() {
        for n in [4, 256] {
            let f = fixture::SyntheticJournalFile::new(&vec!["same"; n]);
            let journal = open(f.root());
            let mut position = None;
            let mut cursor = None;
            for item in journal
                .scan_from_positions(&[], ScanOptions::default())
                .unwrap()
            {
                if let ScanItem::Entry {
                    entry, position: p, ..
                } = item.unwrap()
                {
                    position = Some(p);
                    cursor = Some(entry.cursor().unwrap());
                }
            }
            let position = position.unwrap();
            test_io::reset();
            assert!(
                journal
                    .scan_from_positions(&[position], ScanOptions::default())
                    .unwrap()
                    .next()
                    .is_none()
            );
            let direct = test_io::reads();
            let mut query = journal.query();
            query.after_cursor(cursor.unwrap());
            test_io::reset();
            assert!(query.iter().unwrap().next().is_none());
            let legacy = test_io::reads();
            assert!(direct <= 6, "{n} entries used {direct} reads");
            assert!(legacy > direct);
            println!("tail entries={n}: positioned={direct} old-query={legacy}");
        }
    }
}

#[cfg(test)]
mod correction_tests {
    use super::*;
    use crate::{JournalConfig, MmapPolicy, reader::test_io};
    #[allow(dead_code)]
    mod fixture {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/synthetic_journal.rs"
        ));
    }
    #[test]
    fn positioned_resume_does_not_repeat_validated_anchor_search() {
        let fixture = fixture::SyntheticJournalFile::new(&vec!["same"; 256]);
        let journal = Journal::open_progress_dirs(
            &[fixture.root().to_owned()],
            JournalConfig {
                mmap_policy: MmapPolicy::Never,
                ..Default::default()
            },
            &[],
        )
        .unwrap();
        let file = journal.inner.open_file_by_index(0).unwrap();
        let mut scan = journal
            .scan_from_positions(&[], ScanOptions::default())
            .unwrap();
        let mut anchor = None;
        for _ in 0..128 {
            if let ScanItem::Entry { position, .. } = scan.next().unwrap().unwrap() {
                anchor = Some(position);
            }
        }
        let anchor = anchor.unwrap();
        test_io::reset();
        let _ = FileEntryIter::positioned(file.clone(), None).unwrap();
        let index_reads = test_io::reads();
        test_io::reset();
        let (mut resumed, ordinal) = FileEntryIter::positioned(file, Some(&anchor)).unwrap();
        let resume_reads = test_io::reads();
        assert_eq!(ordinal, Some(128));
        println!("native anchor reads: index={index_reads},resume={resume_reads}");
        assert!(
            resume_reads <= index_reads + 11,
            "index={index_reads},resume={resume_reads}: at most one 256-slot binary membership search plus confirmation/metadata"
        );
        assert_eq!(resumed.next_slot().unwrap().unwrap().1, 129);
    }
}
