mod decompress;
mod entries;
mod index;
mod iter;

use crate::config::JournalConfig;
#[cfg(feature = "mmap")]
use crate::config::MmapPolicy;
use crate::error::{LimitKind, Result, SdJournalError};
use crate::format::{Header, ObjectHeader, STATE_ARCHIVED};
#[cfg(feature = "mmap")]
use crate::format::{STATE_OFFLINE, STATE_ONLINE};
#[cfg(feature = "mmap")]
use crate::reader::MmapAccess;
use crate::reader::{ByteBuf, FileAccess, ReopenAccess};
use crate::util::checked_add_u64;
use std::fs::File;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

#[cfg(feature = "tracing")]
use tracing::debug;

pub(crate) use self::iter::{DataEntryOffsetIter, EntryMeta, EntryRange, FileEntryIter};

#[derive(Clone)]
pub(crate) struct JournalFile {
    inner: Arc<JournalFileInner>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LiveFileState {
    pub(crate) used_size: u64,
    pub(crate) file_len: u64,
    pub(crate) n_entries: u64,
    pub(crate) entry_array_offset: u64,
    pub(crate) tail_object_offset: u64,
}

struct JournalFileInner {
    path: PathBuf,
    config: JournalConfig,
    header: Header,
    used_size: u64,
    file_len: u64,
    modified_at_open: Option<SystemTime>,
    #[cfg(unix)]
    file_identity: (u64, u64),
    access: Access,
}

#[derive(Clone)]
enum Access {
    #[cfg(feature = "mmap")]
    Mmap(MmapAccess),
    File(FileAccess),
    Reopen(ReopenAccess),
}

impl JournalFile {
    pub(crate) fn open(path: PathBuf, config: &JournalConfig) -> Result<Self> {
        Self::open_with_access(path, config, false)
    }

    pub(crate) fn open_reopen(path: PathBuf, config: &JournalConfig) -> Result<Self> {
        Self::open_with_access(path, config, true)
    }

    pub(crate) fn reopen_snapshot(&self) -> Self {
        Self {
            inner: Arc::new(JournalFileInner {
                path: self.inner.path.clone(),
                config: self.inner.config.clone(),
                header: self.inner.header.clone(),
                used_size: self.inner.used_size,
                file_len: self.inner.file_len,
                modified_at_open: self.inner.modified_at_open,
                #[cfg(unix)]
                file_identity: self.inner.file_identity,
                access: Access::Reopen(ReopenAccess::new(
                    self.inner.path.clone(),
                    self.inner.header.file_id,
                    self.inner.header.seqnum_id,
                )),
            }),
        }
    }

    fn open_with_access(
        path: PathBuf,
        config: &JournalConfig,
        reopen_each_read: bool,
    ) -> Result<Self> {
        let file = Arc::new(File::open(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::PermissionDenied {
                SdJournalError::PermissionDenied { path: path.clone() }
            } else {
                SdJournalError::io("open", Some(path.clone()), e)
            }
        })?);
        #[cfg(test)]
        crate::reader::test_io::opened(&path);

        Self::open_from_file(path, file, config, reopen_each_read)
    }

    fn open_from_file(
        path: PathBuf,
        file: Arc<File>,
        config: &JournalConfig,
        reopen_each_read: bool,
    ) -> Result<Self> {
        let opening_metadata = file
            .metadata()
            .map_err(|e| SdJournalError::io("metadata", Some(path.clone()), e))?;
        let file_len = opening_metadata.len();
        let modified_at_open = opening_metadata.modified().ok();

        let file_access = FileAccess::new(path.clone(), file.clone());
        let header_len = usize::try_from(file_len).unwrap_or(usize::MAX).min(272);
        let header_buf = file_access.read_known_valid(0, header_len)?;
        let header = Header::parse(header_buf.as_slice(), &path)?;
        let arena_end = checked_add_u64(
            header.header_size,
            header.arena_size,
            "header_size+arena_size",
        )?;

        #[cfg(feature = "tracing")]
        debug!(
            path = %path.display(),
            state = header.state,
            compact = header.is_compact(),
            keyed_hash = header.is_keyed_hash(),
            sealed = header.is_sealed(),
            sealed_continuous = header.is_sealed_continuous(),
            compressed_xz = header.incompatible_flags & crate::format::HEADER_INCOMPATIBLE_COMPRESSED_XZ != 0,
            compressed_lz4 = header.incompatible_flags & crate::format::HEADER_INCOMPATIBLE_COMPRESSED_LZ4 != 0,
            compressed_zstd = header.incompatible_flags & crate::format::HEADER_INCOMPATIBLE_COMPRESSED_ZSTD != 0,
            file_len,
            arena_end,
            "parsed journal header"
        );
        if arena_end > file_len {
            return Err(SdJournalError::Transient {
                path: Some(path.clone()),
                reason: format!(
                    "file shorter than header reports (file_len={file_len}, arena_end={arena_end})"
                ),
            });
        }
        if header.state > STATE_ARCHIVED {
            return Err(SdJournalError::Corrupt {
                path: Some(path.clone()),
                offset: Some(16),
                reason: format!("unknown header state: {}", header.state),
            });
        }
        let used_size = if header.tail_object_offset == 0 {
            header.header_size
        } else {
            let tail_offset = header.tail_object_offset;
            if tail_offset < header.header_size
                || !tail_offset.is_multiple_of(8)
                || tail_offset >= arena_end
            {
                return Err(SdJournalError::Corrupt {
                    path: Some(path.clone()),
                    offset: Some(tail_offset),
                    reason: "tail_object_offset is outside the committed object arena".to_string(),
                });
            }
            let tail_header = file_access.read_known_valid(tail_offset, 16)?;
            let tail_object = ObjectHeader::parse(tail_header.as_slice(), &path, tail_offset)?;
            let raw_committed_end =
                checked_add_u64(tail_offset, tail_object.size, "tail object committed range")?;
            let committed_end = raw_committed_end
                .checked_add(7)
                .map(|end| end & !7)
                .ok_or_else(|| SdJournalError::Corrupt {
                    path: Some(path.clone()),
                    offset: Some(tail_offset),
                    reason: "tail object aligned range overflows".to_string(),
                })?;
            if committed_end > arena_end {
                return Err(SdJournalError::Corrupt {
                    path: Some(path.clone()),
                    offset: Some(tail_offset),
                    reason: format!(
                        "tail object extends beyond the object arena (end={committed_end}, arena_end={arena_end})"
                    ),
                });
            }
            committed_end
        };

        let access = if reopen_each_read {
            Access::Reopen(ReopenAccess::new(
                path.clone(),
                header.file_id,
                header.seqnum_id,
            ))
        } else {
            #[cfg(feature = "mmap")]
            {
                let use_mmap = match config.mmap_policy {
                    MmapPolicy::Never => false,
                    MmapPolicy::Auto => match header.state {
                        STATE_ONLINE => config.allow_mmap_online,
                        STATE_OFFLINE | STATE_ARCHIVED => true,
                        _ => false,
                    },
                };

                if use_mmap {
                    // SAFETY: Mapping a file is inherently `unsafe` because the file may change on disk.
                    // We reduce risk by:
                    // - validating `used_size <= file_len` before mapping;
                    // - defaulting to mmap only for STATE_OFFLINE/STATE_ARCHIVED files unless explicitly enabled;
                    // - performing bounds checks on every read.
                    let mmap = unsafe { memmap2::MmapOptions::new().map(file.as_ref()) }
                        .map_err(|e| SdJournalError::io("mmap", Some(path.clone()), e))?;
                    Access::Mmap(MmapAccess::new(path.clone(), file.clone(), Arc::new(mmap)))
                } else {
                    Access::File(file_access)
                }
            }

            #[cfg(not(feature = "mmap"))]
            {
                Access::File(file_access)
            }
        };

        Ok(Self {
            inner: Arc::new(JournalFileInner {
                path,
                config: config.clone(),
                header,
                used_size,
                file_len,
                modified_at_open,
                #[cfg(unix)]
                file_identity: (opening_metadata.dev(), opening_metadata.ino()),
                access,
            }),
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.inner.path
    }

    pub(crate) fn file_id(&self) -> [u8; 16] {
        self.inner.header.file_id
    }

    pub(crate) fn seqnum_id(&self) -> [u8; 16] {
        self.inner.header.seqnum_id
    }

    pub(crate) fn entry_range(&self) -> Result<Option<EntryRange>> {
        let has_entries = self.inner.header.n_entries != 0;
        let has_entry_array = self.inner.header.entry_array_offset != 0;
        if has_entries != has_entry_array {
            return Err(self.metadata_error(
                Some(if has_entries { 176 } else { 152 }),
                format!(
                    "inconsistent entry metadata (n_entries={}, entry_array_offset={})",
                    self.inner.header.n_entries, self.inner.header.entry_array_offset
                ),
            ));
        }
        if !has_entries {
            return Ok(None);
        }

        let Some(first) = self.first_entry_meta_from_entry_arrays()? else {
            return Ok(None);
        };

        let last = match self.fast_tail_entry_meta() {
            Ok(Some(meta)) => meta,
            Ok(None) | Err(_) => match self.last_entry_meta_from_entry_arrays()? {
                Some(meta) => meta,
                None => first,
            },
        };

        Ok(Some(EntryRange { first, last }))
    }

    fn first_entry_meta_from_entry_arrays(&self) -> Result<Option<EntryMeta>> {
        let mut current = self.inner.header.entry_array_offset;
        let mut steps = 0usize;

        while current != 0 {
            let items = self.read_entry_array_items(current)?;
            if let Some(offset) = items.into_iter().find(|offset| *offset != 0) {
                return Ok(Some(self.read_entry_meta(offset)?));
            }

            current = self.next_entry_array_offset_checked(current, &mut steps)?;
        }

        Ok(None)
    }

    fn last_entry_meta_from_entry_arrays(&self) -> Result<Option<EntryMeta>> {
        let mut current = self.inner.header.entry_array_offset;
        let mut last_offset = None;
        let mut steps = 0usize;

        while current != 0 {
            let items = self.read_entry_array_items(current)?;
            if let Some(offset) = items.into_iter().rev().find(|offset| *offset != 0) {
                last_offset = Some(offset);
            }

            current = self.next_entry_array_offset_checked(current, &mut steps)?;
        }

        last_offset
            .map(|offset| self.read_entry_meta(offset))
            .transpose()
    }

    fn next_entry_array_offset_checked(&self, current: u64, steps: &mut usize) -> Result<u64> {
        let next = self.read_entry_array_next_offset(current)?;
        *steps = steps.saturating_add(1);
        if *steps > self.inner.config.max_object_chain_steps {
            return Err(SdJournalError::LimitExceeded {
                kind: LimitKind::ObjectChainSteps,
                limit: u64::try_from(self.inner.config.max_object_chain_steps).unwrap_or(u64::MAX),
            });
        }
        Ok(next)
    }

    fn fast_tail_entry_meta(&self) -> Result<Option<EntryMeta>> {
        if let Some(offset) = self
            .inner
            .header
            .tail_entry_offset
            .filter(|offset| *offset != 0)
        {
            let meta = self.read_entry_meta(offset)?;
            if self.tail_meta_matches_header(meta) {
                return Ok(Some(meta));
            }
        }

        if self.inner.header.tail_object_offset == 0 {
            return Ok(None);
        }

        let meta = self.read_entry_meta(self.inner.header.tail_object_offset)?;
        Ok(self.tail_meta_matches_header(meta).then_some(meta))
    }

    fn tail_meta_matches_header(&self, meta: EntryMeta) -> bool {
        if let Some(seqnum) = self
            .inner
            .header
            .tail_entry_seqnum
            .filter(|seqnum| *seqnum != 0)
            && meta.seqnum != seqnum
        {
            return false;
        }

        if let Some(realtime_usec) = self
            .inner
            .header
            .tail_entry_realtime
            .filter(|realtime_usec| *realtime_usec != 0)
            && meta.realtime_usec != realtime_usec
        {
            return false;
        }

        true
    }

    pub(crate) fn live_state(&self) -> LiveFileState {
        LiveFileState {
            used_size: self.inner.used_size,
            file_len: self.inner.file_len,
            n_entries: self.inner.header.n_entries,
            entry_array_offset: self.inner.header.entry_array_offset,
            tail_object_offset: self.inner.header.tail_object_offset,
        }
    }

    pub(crate) fn modified_at_open(&self) -> Option<SystemTime> {
        self.inner.modified_at_open
    }

    pub(crate) fn max_object_chain_steps(&self) -> usize {
        self.inner.config.max_object_chain_steps
    }

    #[cfg(feature = "verify-seal")]
    pub(crate) fn header(&self) -> &Header {
        &self.inner.header
    }

    #[cfg(feature = "verify-seal")]
    pub(crate) fn used_size(&self) -> u64 {
        self.inner.used_size
    }

    #[cfg(feature = "verify-seal")]
    pub(crate) fn config(&self) -> &JournalConfig {
        &self.inner.config
    }

    pub(crate) fn entry_iter_seek_realtime(
        &self,
        reverse: bool,
        since_realtime: Option<u64>,
        until_realtime: Option<u64>,
    ) -> Result<FileEntryIter> {
        self.validate_entry_index_metadata()?;
        FileEntryIter::new_with_seek_realtime(self.clone(), reverse, since_realtime, until_realtime)
    }

    pub(crate) fn entry_iter_after_offset(
        &self,
        reverse: bool,
        entry_offset: u64,
        _since_realtime: Option<u64>,
        _until_realtime: Option<u64>,
    ) -> Result<FileEntryIter> {
        self.validate_entry_index_metadata()?;
        FileEntryIter::new_after_offset(self.clone(), reverse, entry_offset)
    }

    pub(crate) fn read_bytes(&self, offset: u64, len: usize) -> Result<ByteBuf> {
        let end = checked_add_u64(offset, u64::try_from(len).unwrap_or(u64::MAX), "read range")?;
        if end > self.inner.used_size {
            return Err(SdJournalError::Corrupt {
                path: Some(self.inner.path.clone()),
                offset: Some(offset),
                reason: "read out of bounds".to_string(),
            });
        }

        match &self.inner.access {
            Access::File(access) => access.read_known_valid(offset, len),
            Access::Reopen(access) => access.read_known_valid(offset, len),
            #[cfg(feature = "mmap")]
            Access::Mmap(access) => access.read_known_valid(offset, len),
        }
    }

    pub(crate) fn read_object(&self, offset: u64, size: u64) -> Result<ByteBuf> {
        self.validate_object_offset(offset)?;
        let max = usize::try_from(self.inner.config.max_object_size_bytes).unwrap_or(usize::MAX);
        let size_usize = usize::try_from(size).map_err(|_| SdJournalError::LimitExceeded {
            kind: LimitKind::ObjectSizeBytes,
            limit: self.inner.config.max_object_size_bytes,
        })?;
        crate::util::ensure_limit_usize(LimitKind::ObjectSizeBytes, max, size_usize)?;
        self.read_bytes(offset, size_usize)
    }

    pub(crate) fn validate_object_offset(&self, offset: u64) -> Result<()> {
        if !offset.is_multiple_of(8) {
            return Err(self.metadata_error(
                Some(offset),
                format!("object offset is not 8-byte aligned: {offset}"),
            ));
        }
        if offset < self.inner.header.header_size
            || self.inner.header.tail_object_offset == 0
            || offset > self.inner.header.tail_object_offset
        {
            return Err(self.metadata_error(
                Some(offset),
                format!(
                    "object offset is outside committed journal data (header_size={}, tail_object_offset={})",
                    self.inner.header.header_size, self.inner.header.tail_object_offset
                ),
            ));
        }
        Ok(())
    }

    pub(super) fn metadata_error(
        &self,
        offset: Option<u64>,
        reason: impl Into<String>,
    ) -> SdJournalError {
        let reason = reason.into();
        if self.inner.header.state == crate::format::STATE_ONLINE {
            SdJournalError::Transient {
                path: Some(self.inner.path.clone()),
                reason,
            }
        } else {
            SdJournalError::Corrupt {
                path: Some(self.inner.path.clone()),
                offset,
                reason,
            }
        }
    }

    // Only the file-level frozen boundary may use this exception. The caller
    // already validated every offset inside that boundary. This does not refresh
    // the snapshot, trust the appended payloads, or validate all appended entries.
    pub(super) fn confirms_appended_boundary(
        &self,
        first_unused: Option<u64>,
        next_array: Option<u64>,
        spare_slots: usize,
    ) -> bool {
        #[cfg(unix)]
        {
            self.check_appended_boundary(first_unused, next_array, spare_slots)
                .unwrap_or(false)
        }
        #[cfg(not(unix))]
        {
            // No portable same-file identity proof is available here. Preserve
            // the original strict validation instead of weakening that guard.
            let _ = (first_unused, next_array, spare_slots);
            false
        }
    }

    #[cfg(unix)]
    fn check_appended_boundary(
        &self,
        first_unused: Option<u64>,
        next_array: Option<u64>,
        spare_slots: usize,
    ) -> Result<bool> {
        let old = &self.inner.header;
        if old.state != crate::format::STATE_OFFLINE
            || (first_unused.is_none() && next_array.is_none())
            || (spare_slots != 0 && first_unused.is_none())
        {
            return Ok(false);
        }
        // One temporary fd, two <=272-byte header reads and at most three
        // 16-byte object headers. No scan, remap, retained fd, retry or cache.
        // Two equal observations are not an atomic snapshot of a live writer.
        let file =
            Arc::new(File::open(&self.inner.path).map_err(|error| {
                SdJournalError::io("open", Some(self.inner.path.clone()), error)
            })?);
        let before = file.metadata().map_err(|error| {
            SdJournalError::io("metadata", Some(self.inner.path.clone()), error)
        })?;
        if (before.dev(), before.ino()) != self.inner.file_identity
            || before.len() < self.inner.file_len
        {
            return Ok(false);
        }
        let access = FileAccess::new(self.inner.path.clone(), Arc::clone(&file));
        let header_len = old.header_size.min(272) as usize;
        let first = access.read_known_valid(0, header_len)?;
        let fresh = Header::parse(first.as_slice(), &self.inner.path)?;
        if fresh.state > crate::format::STATE_ONLINE
            || fresh.file_id != old.file_id
            || fresh.seqnum_id != old.seqnum_id
            || fresh.machine_id != old.machine_id
            || fresh.boot_id != old.boot_id
            || fresh.header_size != old.header_size
            || fresh.compatible_flags != old.compatible_flags
            || fresh.incompatible_flags != old.incompatible_flags
            || fresh.entry_array_offset != old.entry_array_offset
            || fresh.head_entry_seqnum != old.head_entry_seqnum
            || fresh.head_entry_realtime != old.head_entry_realtime
            || fresh.arena_size < old.arena_size
            || fresh.n_entries <= old.n_entries
            || fresh.tail_object_offset < self.inner.used_size
            || fresh.tail_entry_seqnum <= old.tail_entry_seqnum
        {
            return Ok(false);
        }
        if next_array.is_some() && fresh.n_entries - old.n_entries <= spare_slots as u64 {
            return Ok(false);
        }
        let Some(sequence_growth) = fresh
            .tail_entry_seqnum
            .zip(old.tail_entry_seqnum)
            .and_then(|(new, old)| new.checked_sub(old))
        else {
            return Ok(false);
        };
        if sequence_growth < fresh.n_entries - old.n_entries {
            return Ok(false);
        }
        let Some(arena_end) = fresh.header_size.checked_add(fresh.arena_size) else {
            return Ok(false);
        };
        if arena_end > before.len() {
            return Ok(false);
        }
        let read_header = |offset: u64| -> Result<Option<(ObjectHeader, u64)>> {
            if offset < self.inner.used_size
                || !offset.is_multiple_of(8)
                || offset > fresh.tail_object_offset
                || offset.checked_add(16).is_none_or(|end| end > arena_end)
            {
                return Ok(None);
            }
            let bytes = access.read_known_valid(offset, 16)?;
            let object = ObjectHeader::parse(bytes.as_slice(), &self.inner.path, offset)?;
            let end = offset
                .checked_add(object.size)
                .and_then(|end| end.checked_add(7))
                .map(|end| end & !7);
            let Some(end) = end else {
                return Ok(None);
            };
            if object.size < 16
                || object.size > self.inner.config.max_object_size_bytes
                || end > arena_end
            {
                return Ok(None);
            }
            Ok(Some((object, end)))
        };
        let Some((tail, committed_end)) = read_header(fresh.tail_object_offset)? else {
            return Ok(false);
        };
        if !(1..=7).contains(&tail.object_type) {
            return Ok(false);
        }
        for (offset, kind, minimum) in [
            (first_unused, crate::format::OBJECT_ENTRY, 64),
            (
                next_array,
                crate::format::OBJECT_ENTRY_ARRAY,
                if old.is_compact() { 28 } else { 32 },
            ),
        ] {
            if let Some(offset) = offset {
                let Some((object, end)) = read_header(offset)? else {
                    return Ok(false);
                };
                if object.object_type != kind
                    || object.flags != 0
                    || object.size < minimum
                    || end > committed_end
                {
                    return Ok(false);
                }
            }
        }
        let second = access.read_known_valid(0, header_len)?;
        let after = file.metadata().map_err(|error| {
            SdJournalError::io("metadata", Some(self.inner.path.clone()), error)
        })?;
        Ok(first.as_slice() == second.as_slice() && before.len() == after.len())
    }

    fn validate_entry_index_metadata(&self) -> Result<()> {
        let has_entries = self.inner.header.n_entries != 0;
        let has_entry_array = self.inner.header.entry_array_offset != 0;
        if has_entries != has_entry_array {
            return Err(self.metadata_error(
                Some(if has_entries { 176 } else { 152 }),
                format!(
                    "inconsistent entry metadata (n_entries={}, entry_array_offset={})",
                    self.inner.header.n_entries, self.inner.header.entry_array_offset
                ),
            ));
        }
        Ok(())
    }
}
