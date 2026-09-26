use super::JournalFile;
use super::index::DataObjectRef;
use crate::error::{LimitKind, Result, SdJournalError};
use crate::format::{OBJECT_ENTRY_ARRAY, ObjectHeader, STATE_ONLINE};
use crate::util::{read_u32_le, read_u64_le};
use std::collections::{HashSet, VecDeque};

const ENTRY_ARRAY_CHUNK_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy)]
struct EntryArrayDescriptor {
    offset: u64,
    layout: EntryArrayLayout,
    used_items: usize,
    first: u64,
    last: u64,
}

struct EntryArrayCursor {
    file: JournalFile,
    reverse: bool,
    arrays: Vec<EntryArrayDescriptor>,
    current_array_idx: isize,
    next_item_idx: usize,
    current_items: VecDeque<u64>,
    previous_offset: Option<u64>,
    first_refill_single: bool,
    exhausted: bool,
}

impl EntryArrayCursor {
    fn new(file: JournalFile, arrays: Vec<EntryArrayDescriptor>, reverse: bool) -> Self {
        let current_array_idx = if reverse {
            isize::try_from(arrays.len()).unwrap_or(0) - 1
        } else {
            0
        };
        let next_item_idx = if reverse {
            arrays.last().map_or(0, |array| array.used_items)
        } else {
            0
        };
        let exhausted = arrays.is_empty();
        Self {
            file,
            reverse,
            arrays,
            current_array_idx,
            next_item_idx,
            current_items: VecDeque::new(),
            previous_offset: None,
            first_refill_single: false,
            exhausted,
        }
    }

    fn new_after_offset(
        file: JournalFile,
        arrays: Vec<EntryArrayDescriptor>,
        reverse: bool,
        after_entry_offset: u64,
    ) -> Result<Self> {
        let Some(array_idx) = locate_entry_array_for_offset(&arrays, reverse, after_entry_offset)
        else {
            let mut cursor = Self::new(file, arrays, reverse);
            cursor.exhausted = true;
            return Ok(cursor);
        };

        let split = partition_entry_array(&file, &arrays[array_idx], after_entry_offset, reverse)?;
        let mut cursor = Self {
            file,
            reverse,
            arrays,
            current_array_idx: isize::try_from(array_idx).unwrap_or(0),
            next_item_idx: split,
            current_items: VecDeque::new(),
            previous_offset: Some(after_entry_offset),
            // A lazy query normally asks this cursor for one item and then drops it. Reading just
            // that item avoids re-reading an overlapping 4 KiB suffix chunk for every entry.
            first_refill_single: true,
            exhausted: false,
        };
        cursor.skip_exhausted_array();
        Ok(cursor)
    }

    fn skip_exhausted_array(&mut self) {
        loop {
            let Some(array_idx) = usize::try_from(self.current_array_idx)
                .ok()
                .filter(|idx| *idx < self.arrays.len())
            else {
                self.exhausted = true;
                return;
            };

            let has_items = if self.reverse {
                self.next_item_idx != 0
            } else {
                self.next_item_idx < self.arrays[array_idx].used_items
            };
            if has_items {
                return;
            }

            if self.reverse {
                self.current_array_idx -= 1;
                self.next_item_idx = usize::try_from(self.current_array_idx)
                    .ok()
                    .and_then(|idx| self.arrays.get(idx))
                    .map_or(0, |array| array.used_items);
            } else {
                self.current_array_idx += 1;
                self.next_item_idx = 0;
            }
        }
    }

    fn refill(&mut self) -> Result<()> {
        if !self.current_items.is_empty() || self.exhausted {
            return Ok(());
        }

        self.skip_exhausted_array();
        if self.exhausted {
            return Ok(());
        }

        let array_idx =
            usize::try_from(self.current_array_idx).map_err(|_| SdJournalError::Corrupt {
                path: Some(self.file.inner.path.clone()),
                offset: None,
                reason: "ENTRY_ARRAY cursor index is out of range".to_string(),
            })?;
        let array = self.arrays[array_idx];
        let items_per_chunk = if self.first_refill_single {
            1
        } else {
            (ENTRY_ARRAY_CHUNK_BYTES / array.layout.item_size).max(1)
        };
        self.first_refill_single = false;

        let (start, end) = entry_array_chunk_range(
            self.reverse,
            self.next_item_idx,
            array.used_items,
            items_per_chunk,
        );
        let mut items = read_entry_array_item_range(&self.file, &array, start, end)?;
        self.next_item_idx = if self.reverse { start } else { end };
        if self.reverse {
            items.reverse();
        }

        for &offset in &items {
            if let Some(previous) = self.previous_offset {
                let valid = if self.reverse {
                    offset < previous
                } else {
                    offset > previous
                };
                if !valid {
                    return Err(self.file.metadata_error(
                        Some(array.offset),
                        format!(
                            "ENTRY_ARRAY entry offsets are not strictly increasing ({offset} vs {previous})"
                        ),
                    ));
                }
            }
            self.previous_offset = Some(offset);
        }

        self.current_items = items.into();
        Ok(())
    }
}

impl Iterator for EntryArrayCursor {
    type Item = Result<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.exhausted {
                return None;
            }
            if let Err(error) = self.refill() {
                self.exhausted = true;
                return Some(Err(error));
            }
            if let Some(offset) = self.current_items.pop_front() {
                return Some(Ok(offset));
            }
            self.skip_exhausted_array();
        }
    }
}

pub(crate) struct DataEntryOffsetIter {
    reverse: bool,
    first_entry_offset: u64,
    first_pending: bool,
    arrays: EntryArrayCursor,
    exhausted: bool,
}

impl DataEntryOffsetIter {
    pub(crate) fn new(file: JournalFile, data: DataObjectRef, reverse: bool) -> Result<Self> {
        Self::new_after_offset(file, data, reverse, None)
    }

    pub(crate) fn new_after_offset(
        file: JournalFile,
        data: DataObjectRef,
        reverse: bool,
        after_entry_offset: Option<u64>,
    ) -> Result<Self> {
        validate_data_entry_metadata(&file, data)?;
        let descriptors = collect_entry_array_descriptors(
            &file,
            data.entry_array_offset,
            data.n_entries
                .saturating_sub(u64::from(data.entry_offset != 0)),
            true,
        )?;
        if let Some(first_array) = descriptors.first()
            && first_array.first <= data.entry_offset
        {
            return Err(file.metadata_error(
                Some(first_array.offset),
                format!(
                    "DATA entry offsets are not strictly increasing ({} <= {})",
                    first_array.first, data.entry_offset
                ),
            ));
        }
        let first_pending = data.entry_offset != 0;
        let arrays = match after_entry_offset {
            Some(after) if reverse || data.entry_offset == 0 || data.entry_offset <= after => {
                EntryArrayCursor::new_after_offset(file, descriptors, reverse, after)?
            }
            _ => EntryArrayCursor::new(file, descriptors, reverse),
        };
        let mut iter = Self {
            reverse,
            first_entry_offset: data.entry_offset,
            first_pending,
            arrays,
            exhausted: false,
        };

        if let Some(after) = after_entry_offset {
            if reverse {
                iter.first_pending =
                    iter.first_entry_offset != 0 && iter.first_entry_offset < after;
            } else if iter.first_entry_offset == 0 || iter.first_entry_offset <= after {
                iter.first_pending = false;
            }
        }

        Ok(iter)
    }
}

impl Iterator for DataEntryOffsetIter {
    type Item = Result<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }

        if !self.reverse && self.first_pending {
            self.first_pending = false;
            if self.first_entry_offset != 0 {
                self.arrays.previous_offset = Some(self.first_entry_offset);
                return Some(Ok(self.first_entry_offset));
            }
        }

        if let Some(offset) = self.arrays.next() {
            if offset.is_err() {
                self.exhausted = true;
            }
            return Some(offset);
        }

        if self.reverse && self.first_pending {
            self.first_pending = false;
            if self.first_entry_offset != 0 {
                if let Some(previous) = self.arrays.previous_offset
                    && self.first_entry_offset >= previous
                {
                    self.exhausted = true;
                    return Some(Err(self.arrays.file.metadata_error(
                        Some(self.first_entry_offset),
                        format!(
                            "DATA entry offsets are not strictly increasing ({} >= {previous})",
                            self.first_entry_offset
                        ),
                    )));
                }
                return Some(Ok(self.first_entry_offset));
            }
        }

        self.exhausted = true;
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EntryMeta {
    pub(crate) file_id: [u8; 16],
    pub(crate) entry_offset: u64,
    pub(crate) seqnum_id: [u8; 16],
    pub(crate) seqnum: u64,
    pub(crate) realtime_usec: u64,
    pub(crate) monotonic_usec: u64,
    pub(crate) boot_id: [u8; 16],
    pub(crate) xor_hash: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EntryRange {
    /// Earliest entry in the file-level journal entry array.
    pub(crate) first: EntryMeta,
    /// Latest entry in the file-level journal entry array.
    pub(crate) last: EntryMeta,
}

pub(crate) struct FileEntryIter {
    file: JournalFile,
    arrays: EntryArrayCursor,
    exhausted: bool,
}

impl FileEntryIter {
    pub(crate) fn new_with_seek_realtime(
        file: JournalFile,
        reverse: bool,
        _since_realtime: Option<u64>,
        _until_realtime: Option<u64>,
    ) -> Result<Self> {
        let descriptors = collect_entry_array_descriptors(
            &file,
            file.inner.header.entry_array_offset,
            file.inner.header.n_entries,
            false,
        )?;
        let arrays = EntryArrayCursor::new(file.clone(), descriptors, reverse);
        Ok(Self {
            file,
            arrays,
            exhausted: false,
        })
    }

    pub(crate) fn new_after_offset(
        file: JournalFile,
        reverse: bool,
        after_entry_offset: u64,
    ) -> Result<Self> {
        let descriptors = collect_entry_array_descriptors(
            &file,
            file.inner.header.entry_array_offset,
            file.inner.header.n_entries,
            false,
        )?;
        let arrays = EntryArrayCursor::new_after_offset(
            file.clone(),
            descriptors,
            reverse,
            after_entry_offset,
        )?;

        Ok(Self {
            file,
            arrays,
            exhausted: false,
        })
    }
}

impl JournalFile {
    pub(crate) fn read_entry_array_items(&self, offset: u64) -> Result<Vec<u64>> {
        Ok(read_entry_array_object(self, offset)?.items)
    }

    pub(crate) fn read_entry_array_next_offset(&self, offset: u64) -> Result<u64> {
        read_entry_array_next_offset(self, offset)
    }
}

impl Iterator for FileEntryIter {
    type Item = Result<EntryMeta>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        match self.arrays.next() {
            Some(Ok(entry_offset)) => Some(self.file.read_entry_meta(entry_offset)),
            None => {
                self.exhausted = true;
                None
            }
            Some(Err(error)) => {
                self.exhausted = true;
                Some(Err(error))
            }
        }
    }
}

#[derive(Debug)]
struct EntryArrayObject {
    items: Vec<u64>,
}

#[derive(Debug, Clone, Copy)]
struct EntryArrayLayout {
    size: u64,
    next_entry_array_offset: u64,
    item_size: usize,
    item_count: usize,
}

fn validate_data_entry_metadata(file: &JournalFile, data: DataObjectRef) -> Result<()> {
    let valid = match data.n_entries {
        0 => data.entry_offset == 0 && data.entry_array_offset == 0,
        1 => data.entry_offset != 0 && data.entry_array_offset == 0,
        _ => data.entry_offset != 0 && data.entry_array_offset != 0,
    };
    if valid {
        return Ok(());
    }

    Err(file.metadata_error(
        Some(data.entry_offset),
        format!(
            "inconsistent DATA entry metadata (n_entries={}, entry_offset={}, entry_array_offset={})",
            data.n_entries, data.entry_offset, data.entry_array_offset
        ),
    ))
}

fn collect_entry_array_descriptors(
    file: &JournalFile,
    start: u64,
    expected_items: u64,
    allow_online_snapshot_truncate: bool,
) -> Result<Vec<EntryArrayDescriptor>> {
    let mut arrays = Vec::new();
    let mut visited = HashSet::new();
    let mut current = start;
    let mut previous_last = None;
    let mut remaining = expected_items;

    while current != 0 {
        if !visited.insert(current) {
            return Err(file.metadata_error(
                Some(current),
                format!("cycle detected in ENTRY_ARRAY chain at offset {current}"),
            ));
        }
        if arrays.len() >= file.inner.config.max_object_chain_steps {
            return Err(SdJournalError::LimitExceeded {
                kind: LimitKind::ObjectChainSteps,
                limit: u64::try_from(file.inner.config.max_object_chain_steps).unwrap_or(u64::MAX),
            });
        }

        let layout = read_entry_array_layout(file, current)?;
        let item_count = u64::try_from(layout.item_count).unwrap_or(u64::MAX);
        let nominal_used_u64 = remaining.min(item_count);
        let nominal_used =
            usize::try_from(nominal_used_u64).map_err(|_| SdJournalError::LimitExceeded {
                kind: LimitKind::ObjectSizeBytes,
                limit: file.inner.config.max_object_size_bytes,
            })?;
        if nominal_used == 0 {
            if file.inner.header.state == STATE_ONLINE && allow_online_snapshot_truncate {
                break;
            }
            return Err(file.metadata_error(
                Some(current),
                "ENTRY_ARRAY chain contains more objects than entry metadata requires",
            ));
        }
        let (used_items, first, last, reached_snapshot_tail) = validate_entry_array_prefix(
            file,
            current,
            layout,
            nominal_used,
            previous_last,
            allow_online_snapshot_truncate,
        )?;
        if used_items != 0 {
            previous_last = Some(last);
            arrays.push(EntryArrayDescriptor {
                offset: current,
                layout,
                used_items,
                first,
                last,
            });
            remaining = remaining.saturating_sub(u64::try_from(used_items).unwrap_or(u64::MAX));
        }
        if reached_snapshot_tail {
            remaining = 0;
            break;
        }

        let next = layout.next_entry_array_offset;
        if next != 0 && next <= current {
            return Err(file.metadata_error(
                Some(current),
                format!("ENTRY_ARRAY next offset does not move forward ({next} <= {current})"),
            ));
        }
        let first_unused =
            if file.inner.header.state != STATE_ONLINE && used_items < layout.item_count {
                Some(read_entry_array_item(file, current, &layout, used_items)?)
            } else {
                None
            };
        if remaining == 0
            && !allow_online_snapshot_truncate
            && start == file.inner.header.entry_array_offset
            && expected_items == file.inner.header.n_entries
            && file.confirms_appended_boundary(
                first_unused.filter(|offset| *offset != 0),
                (next != 0).then_some(next),
                layout.item_count - used_items,
            )
        {
            break;
        }
        if file.inner.header.state != STATE_ONLINE && used_items < layout.item_count {
            if first_unused != Some(0) {
                return Err(file.metadata_error(
                    Some(current),
                    "ENTRY_ARRAY unused tail starts with a non-zero offset",
                ));
            }
            if next != 0 {
                return Err(file.metadata_error(
                    Some(current),
                    "partially used ENTRY_ARRAY unexpectedly links to another object",
                ));
            }
        }
        if remaining == 0 {
            if file.inner.header.state != STATE_ONLINE && next != 0 {
                return Err(file.metadata_error(
                    Some(current),
                    "ENTRY_ARRAY chain contains more objects than entry metadata requires",
                ));
            }
            break;
        }
        current = next;
    }

    if remaining != 0 {
        return Err(file.metadata_error(
            Some(start),
            format!(
                "ENTRY_ARRAY chain contains fewer offsets than entry metadata requires ({remaining} missing)"
            ),
        ));
    }
    if expected_items == 0 && !arrays.is_empty() {
        return Err(file.metadata_error(
            Some(start),
            "ENTRY_ARRAY chain is present for zero expected entries",
        ));
    }

    Ok(arrays)
}

fn validate_entry_array_prefix(
    file: &JournalFile,
    offset: u64,
    layout: EntryArrayLayout,
    nominal_used: usize,
    previous_last: Option<u64>,
    allow_online_snapshot_truncate: bool,
) -> Result<(usize, u64, u64, bool)> {
    let provisional = EntryArrayDescriptor {
        offset,
        layout,
        used_items: nominal_used,
        first: 0,
        last: 0,
    };
    let items_per_chunk = (ENTRY_ARRAY_CHUNK_BYTES / layout.item_size).max(1);
    let mut validated = 0usize;
    let mut first = None;
    let mut last = previous_last;

    while validated < nominal_used {
        let end = validated.saturating_add(items_per_chunk).min(nominal_used);
        let items = read_entry_array_item_range(file, &provisional, validated, end)?;
        for value in items {
            if value > file.inner.header.tail_object_offset {
                if file.inner.header.state == STATE_ONLINE && allow_online_snapshot_truncate {
                    return Ok((validated, first.unwrap_or(0), last.unwrap_or(0), true));
                }
                return Err(file.metadata_error(
                    Some(offset),
                    format!(
                        "ENTRY_ARRAY references an object beyond tail_object_offset ({value} > {})",
                        file.inner.header.tail_object_offset
                    ),
                ));
            }
            if value < file.inner.header.header_size || !value.is_multiple_of(8) {
                return Err(file.metadata_error(
                    Some(offset),
                    format!("ENTRY_ARRAY contains an invalid object offset: {value}"),
                ));
            }
            if let Some(previous) = last
                && value <= previous
            {
                return Err(file.metadata_error(
                    Some(offset),
                    format!(
                        "ENTRY_ARRAY entry offsets are not strictly increasing ({value} <= {previous})"
                    ),
                ));
            }
            first.get_or_insert(value);
            last = Some(value);
            validated = validated.saturating_add(1);
        }
    }

    Ok((validated, first.unwrap_or(0), last.unwrap_or(0), false))
}

fn locate_entry_array_for_offset(
    arrays: &[EntryArrayDescriptor],
    reverse: bool,
    after: u64,
) -> Option<usize> {
    if arrays.is_empty() {
        return None;
    }

    let mut left = 0usize;
    let mut right = arrays.len();
    while left < right {
        let mid = left + (right - left) / 2;
        let last = arrays[mid].last;
        let before_target = if reverse { last < after } else { last <= after };
        if before_target {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    if reverse {
        Some(left.min(arrays.len().saturating_sub(1)))
    } else {
        (left < arrays.len()).then_some(left)
    }
}

fn partition_entry_array(
    file: &JournalFile,
    array: &EntryArrayDescriptor,
    after: u64,
    reverse: bool,
) -> Result<usize> {
    let split = partition_point_result(array.used_items, |index| {
        let offset = read_entry_array_item(file, array.offset, &array.layout, index)?;
        if offset == 0 {
            return Err(file.metadata_error(
                Some(array.offset),
                "ENTRY_ARRAY contains a zero offset before its unused tail",
            ));
        }
        Ok(if reverse {
            offset < after
        } else {
            offset <= after
        })
    })?;

    let before = if split == 0 {
        None
    } else {
        Some(read_entry_array_item(
            file,
            array.offset,
            &array.layout,
            split - 1,
        )?)
    };
    let after_split = if split == array.used_items {
        None
    } else {
        Some(read_entry_array_item(
            file,
            array.offset,
            &array.layout,
            split,
        )?)
    };
    if before == Some(0) || after_split == Some(0) {
        return Err(file.metadata_error(
            Some(array.offset),
            "ENTRY_ARRAY contains a zero offset before its unused tail",
        ));
    }
    if let (Some(left), Some(right)) = (before, after_split)
        && left >= right
    {
        return Err(file.metadata_error(
            Some(array.offset),
            format!("ENTRY_ARRAY entry offsets are not strictly increasing ({left} >= {right})"),
        ));
    }

    Ok(split)
}

fn partition_point_result<E>(
    len: usize,
    mut predicate: impl FnMut(usize) -> std::result::Result<bool, E>,
) -> std::result::Result<usize, E> {
    let mut left = 0usize;
    let mut right = len;
    while left < right {
        let mid = left + (right - left) / 2;
        if predicate(mid)? {
            left = mid.saturating_add(1);
        } else {
            right = mid;
        }
    }
    Ok(left)
}

fn read_entry_array_next_offset(file: &JournalFile, offset: u64) -> Result<u64> {
    Ok(read_entry_array_layout(file, offset)?.next_entry_array_offset)
}

fn read_entry_array_layout(file: &JournalFile, offset: u64) -> Result<EntryArrayLayout> {
    file.validate_object_offset(offset)?;
    if !offset.is_multiple_of(8) {
        return Err(file.metadata_error(
            Some(offset),
            format!("ENTRY_ARRAY offset is not 8-byte aligned: {offset}"),
        ));
    }

    let buf = file.read_bytes(offset, 24)?;
    let oh = ObjectHeader::parse(buf.as_slice(), file.path(), offset)?;
    if oh.object_type != OBJECT_ENTRY_ARRAY {
        return Err(SdJournalError::Corrupt {
            path: Some(file.inner.path.clone()),
            offset: Some(offset),
            reason: format!("expected ENTRY_ARRAY object, found type {}", oh.object_type),
        });
    }
    let item_size = if file.inner.header.is_compact() {
        4usize
    } else {
        8usize
    };
    let items_size = oh
        .size
        .checked_sub(24)
        .ok_or_else(|| SdJournalError::Corrupt {
            path: Some(file.inner.path.clone()),
            offset: Some(offset),
            reason: format!("ENTRY_ARRAY object too small: {}", oh.size),
        })?;
    if items_size == 0 || !items_size.is_multiple_of(item_size as u64) {
        return Err(SdJournalError::Corrupt {
            path: Some(file.inner.path.clone()),
            offset: Some(offset),
            reason: format!("ENTRY_ARRAY object has invalid item area size: {items_size}"),
        });
    }
    if oh.size > file.inner.config.max_object_size_bytes {
        return Err(SdJournalError::LimitExceeded {
            kind: LimitKind::ObjectSizeBytes,
            limit: file.inner.config.max_object_size_bytes,
        });
    }
    let end = offset
        .checked_add(oh.size)
        .ok_or_else(|| file.metadata_error(Some(offset), "ENTRY_ARRAY object range overflow"))?;
    if end > file.inner.used_size {
        return Err(file.metadata_error(
            Some(offset),
            format!(
                "ENTRY_ARRAY extends beyond used journal data (end={end}, used_size={})",
                file.inner.used_size
            ),
        ));
    }

    let next_entry_array_offset =
        read_u64_le(buf.as_slice(), 16).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(file.inner.path.clone()),
            offset: Some(offset + 16),
            reason: "missing next_entry_array_offset".to_string(),
        })?;

    let item_count = usize::try_from(items_size / item_size as u64).map_err(|_| {
        SdJournalError::LimitExceeded {
            kind: LimitKind::ObjectSizeBytes,
            limit: file.inner.config.max_object_size_bytes,
        }
    })?;

    Ok(EntryArrayLayout {
        size: oh.size,
        next_entry_array_offset,
        item_size,
        item_count,
    })
}

fn read_entry_array_item(
    file: &JournalFile,
    array_offset: u64,
    layout: &EntryArrayLayout,
    index: usize,
) -> Result<u64> {
    if index >= layout.item_count {
        return Err(file.metadata_error(
            Some(array_offset),
            format!(
                "ENTRY_ARRAY item index out of bounds ({index} >= {})",
                layout.item_count
            ),
        ));
    }
    let relative = 24u64
        .checked_add(
            u64::try_from(index)
                .unwrap_or(u64::MAX)
                .saturating_mul(layout.item_size as u64),
        )
        .ok_or_else(|| file.metadata_error(Some(array_offset), "ENTRY_ARRAY item overflow"))?;
    let item_offset = array_offset
        .checked_add(relative)
        .ok_or_else(|| file.metadata_error(Some(array_offset), "ENTRY_ARRAY item overflow"))?;
    let bytes = file.read_bytes(item_offset, layout.item_size)?;
    if layout.item_size == 4 {
        read_u32_le(bytes.as_slice(), 0)
            .map(u64::from)
            .ok_or_else(|| file.metadata_error(Some(item_offset), "ENTRY_ARRAY item truncated"))
    } else {
        read_u64_le(bytes.as_slice(), 0)
            .ok_or_else(|| file.metadata_error(Some(item_offset), "ENTRY_ARRAY item truncated"))
    }
}

fn read_entry_array_item_range(
    file: &JournalFile,
    array: &EntryArrayDescriptor,
    start: usize,
    end: usize,
) -> Result<Vec<u64>> {
    if start > end || end > array.used_items {
        return Err(file.metadata_error(
            Some(array.offset),
            format!(
                "ENTRY_ARRAY item range is out of bounds ({start}..{end}, used={})",
                array.used_items
            ),
        ));
    }
    if start == end {
        return Ok(Vec::new());
    }

    let count = end - start;
    let byte_len = count
        .checked_mul(array.layout.item_size)
        .ok_or_else(|| file.metadata_error(Some(array.offset), "ENTRY_ARRAY range overflow"))?;
    if byte_len > ENTRY_ARRAY_CHUNK_BYTES {
        return Err(file.metadata_error(
            Some(array.offset),
            format!("ENTRY_ARRAY chunk exceeds {ENTRY_ARRAY_CHUNK_BYTES} bytes"),
        ));
    }
    let relative = 24u64
        .checked_add(
            u64::try_from(start)
                .unwrap_or(u64::MAX)
                .saturating_mul(array.layout.item_size as u64),
        )
        .ok_or_else(|| file.metadata_error(Some(array.offset), "ENTRY_ARRAY range overflow"))?;
    let chunk_offset = array
        .offset
        .checked_add(relative)
        .ok_or_else(|| file.metadata_error(Some(array.offset), "ENTRY_ARRAY range overflow"))?;
    let bytes = file.read_bytes(chunk_offset, byte_len)?;

    let mut items = Vec::with_capacity(count);
    for index in 0..count {
        let byte_index = index * array.layout.item_size;
        let value = if array.layout.item_size == 4 {
            read_u32_le(bytes.as_slice(), byte_index).map(u64::from)
        } else {
            read_u64_le(bytes.as_slice(), byte_index)
        }
        .ok_or_else(|| {
            file.metadata_error(
                Some(chunk_offset.saturating_add(byte_index as u64)),
                "ENTRY_ARRAY item truncated",
            )
        })?;
        if value == 0 {
            return Err(file.metadata_error(
                Some(chunk_offset.saturating_add(byte_index as u64)),
                "ENTRY_ARRAY contains a zero offset before its unused tail",
            ));
        }
        if let Some(previous) = items.last()
            && value <= *previous
        {
            return Err(file.metadata_error(
                Some(chunk_offset.saturating_add(byte_index as u64)),
                format!(
                    "ENTRY_ARRAY entry offsets are not strictly increasing ({value} <= {previous})"
                ),
            ));
        }
        items.push(value);
    }

    Ok(items)
}

fn entry_array_chunk_range(
    reverse: bool,
    next_item_idx: usize,
    used_items: usize,
    max_items: usize,
) -> (usize, usize) {
    if reverse {
        let end = next_item_idx.min(used_items);
        (end.saturating_sub(max_items), end)
    } else {
        let start = next_item_idx.min(used_items);
        (start, start.saturating_add(max_items).min(used_items))
    }
}

fn read_entry_array_object(file: &JournalFile, offset: u64) -> Result<EntryArrayObject> {
    let layout = read_entry_array_layout(file, offset)?;
    let obj = file.read_object(offset, layout.size)?;
    let obj = obj.as_slice();

    let items_bytes = obj.get(24..).ok_or_else(|| SdJournalError::Corrupt {
        path: Some(file.inner.path.clone()),
        offset: Some(offset + 24),
        reason: "missing ENTRY_ARRAY items".to_string(),
    })?;

    let mut items = Vec::with_capacity(layout.item_count);
    if layout.item_size == 4 {
        let mut i = 0usize;
        while i < items_bytes.len() {
            let off = read_u32_le(items_bytes, i).ok_or_else(|| SdJournalError::Corrupt {
                path: Some(file.inner.path.clone()),
                offset: Some(offset),
                reason: "ENTRY_ARRAY item truncated".to_string(),
            })?;
            items.push(u64::from(off));
            i += 4;
        }
    } else {
        let mut i = 0usize;
        while i < items_bytes.len() {
            let off = read_u64_le(items_bytes, i).ok_or_else(|| SdJournalError::Corrupt {
                path: Some(file.inner.path.clone()),
                offset: Some(offset),
                reason: "ENTRY_ARRAY item truncated".to_string(),
            })?;
            items.push(off);
            i += 8;
        }
    }

    while matches!(items.last(), Some(0)) {
        items.pop();
    }
    if items.is_empty() {
        return Err(file.metadata_error(Some(offset), "ENTRY_ARRAY contains no entry offsets"));
    }
    if items.contains(&0) {
        return Err(file.metadata_error(
            Some(offset),
            "ENTRY_ARRAY contains a zero offset before its unused tail",
        ));
    }
    if items.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(file.metadata_error(
            Some(offset),
            "ENTRY_ARRAY entry offsets are not strictly increasing",
        ));
    }

    Ok(EntryArrayObject { items })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn after_offset_partition_is_logarithmic_for_a_large_array() {
        let len = 1usize << 20;
        let boundary = len / 2;
        let calls = Cell::new(0usize);

        let split = partition_point_result(len, |index| {
            calls.set(calls.get().saturating_add(1));
            Ok::<bool, ()>(index < boundary)
        })
        .unwrap();

        assert_eq!(split, boundary);
        assert!(
            calls.get() <= 21,
            "binary positioning read {} items for {len} offsets",
            calls.get()
        );
    }

    #[test]
    fn entry_array_refills_are_bounded_and_after_offset_starts_with_one_item() {
        let used_items = 1usize << 20;
        let regular_chunk_items = ENTRY_ARRAY_CHUNK_BYTES / 8;

        let (start, end) =
            entry_array_chunk_range(false, used_items / 2, used_items, regular_chunk_items);
        assert_eq!(end - start, regular_chunk_items);

        let (start, end) =
            entry_array_chunk_range(true, used_items / 2, used_items, regular_chunk_items);
        assert_eq!(end - start, regular_chunk_items);

        let (start, end) = entry_array_chunk_range(false, used_items / 2, used_items, 1);
        assert_eq!(end - start, 1);

        let (start, end) = entry_array_chunk_range(true, used_items / 2, used_items, 1);
        assert_eq!(end - start, 1);
    }
}
