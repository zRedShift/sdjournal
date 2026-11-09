use std::{cmp::Reverse, collections::BinaryHeap};

use super::{
    JournalQuery, MatchTerm, build_branches,
    cursor::{CursorStart, build_cursor_start, entry_key_from_meta},
    entry_ref_matches_query,
};
use crate::{
    cursor::{compare_entry_keys, same_entry},
    entry::EntryRef,
    error::{Result, SdJournalError},
    file::{DataEntryOffsetIter, EntryMeta, FileEntryIter, JournalFile},
    journal::JournalFileInfo,
};

enum FileMetaIter {
    Empty,
    Single(FileBranchIter),
    Or(FileOrIter),
}

impl FileMetaIter {
    fn from_branch_iters(mut iters: Vec<FileBranchIter>, reverse: bool) -> Self {
        iters.retain(|it| !matches!(&it.kind, BranchKind::Empty));
        match iters.len() {
            0 => FileMetaIter::Empty,
            1 => FileMetaIter::Single(iters.remove(0)),
            _ => FileMetaIter::Or(FileOrIter::new(iters, reverse)),
        }
    }
}

impl Iterator for FileMetaIter {
    type Item = Result<EntryMeta>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            FileMetaIter::Empty => None,
            FileMetaIter::Single(it) => it.next(),
            FileMetaIter::Or(it) => it.next(),
        }
    }
}

struct FileOrIter {
    reverse: bool,
    forward_heap: BinaryHeap<Reverse<FileOrHeapItem>>,
    reverse_heap: BinaryHeap<FileOrHeapItem>,
    iters: Vec<FileBranchIter>,
    pending_error: Option<SdJournalError>,
    last_emitted: Option<EntryMeta>,
    done: bool,
}

#[derive(Clone, Copy)]
struct FileOrHeapItem {
    meta: EntryMeta,
    branch_idx: usize,
}

impl PartialEq for FileOrHeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.meta == other.meta && self.branch_idx == other.branch_idx
    }
}

impl Eq for FileOrHeapItem {}

impl PartialOrd for FileOrHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FileOrHeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.meta
            .seqnum
            .cmp(&other.meta.seqnum)
            .then_with(|| self.meta.entry_offset.cmp(&other.meta.entry_offset))
            .then_with(|| self.branch_idx.cmp(&other.branch_idx))
    }
}

impl FileOrIter {
    fn new(mut iters: Vec<FileBranchIter>, reverse: bool) -> Self {
        let mut pending_error = None;
        let mut forward_heap: BinaryHeap<Reverse<FileOrHeapItem>> = BinaryHeap::new();
        let mut reverse_heap: BinaryHeap<FileOrHeapItem> = BinaryHeap::new();

        for (idx, it) in iters.iter_mut().enumerate() {
            if let Some(meta) = next_ok_meta(it, &mut pending_error) {
                let item = FileOrHeapItem {
                    meta,
                    branch_idx: idx,
                };
                if reverse {
                    reverse_heap.push(item);
                } else {
                    forward_heap.push(Reverse(item));
                }
            }
        }

        Self {
            reverse,
            forward_heap,
            reverse_heap,
            iters,
            pending_error,
            last_emitted: None,
            done: false,
        }
    }

    fn pop_next(&mut self) -> Option<FileOrHeapItem> {
        if self.reverse {
            self.reverse_heap.pop()
        } else {
            self.forward_heap.pop().map(|r| r.0)
        }
    }

    fn push_next(&mut self, item: FileOrHeapItem) {
        if self.reverse {
            self.reverse_heap.push(item);
        } else {
            self.forward_heap.push(Reverse(item));
        }
    }
}

impl Iterator for FileOrIter {
    type Item = Result<EntryMeta>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            if let Some(err) = self.pending_error.take() {
                return Some(Err(err));
            }
            let item = match self.pop_next() {
                Some(item) => item,
                None => {
                    self.done = true;
                    return None;
                }
            };

            if let Some(next_meta) =
                next_ok_meta(&mut self.iters[item.branch_idx], &mut self.pending_error)
            {
                self.push_next(FileOrHeapItem {
                    meta: next_meta,
                    branch_idx: item.branch_idx,
                });
            }

            if self
                .last_emitted
                .is_some_and(|last| same_meta_entry(&last, &item.meta))
            {
                continue;
            }

            self.last_emitted = Some(item.meta);
            return Some(Ok(item.meta));
        }
    }
}

struct AndOffsetIter {
    reverse: bool,
    iters: Vec<OffsetIter>,
    cursors: Vec<Option<u64>>,
    initialized: bool,
    pending_error: Option<SdJournalError>,
    done: bool,
}

impl AndOffsetIter {
    fn new(iters: Vec<OffsetIter>, reverse: bool) -> Self {
        let cursors = vec![None; iters.len()];
        Self {
            reverse,
            iters,
            cursors,
            initialized: false,
            pending_error: None,
            done: false,
        }
    }

    fn init(&mut self) -> Option<Result<()>> {
        if self.initialized {
            return Some(Ok(()));
        }
        for i in 0..self.iters.len() {
            match self.iters[i].next() {
                Some(Ok(v)) => self.cursors[i] = Some(v),
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }
        self.initialized = true;
        Some(Ok(()))
    }

    fn target(&self) -> Option<u64> {
        let mut it = self.cursors.iter().copied();
        let mut target = it.next()??;
        for v in it {
            let v = v?;
            target = if self.reverse {
                target.min(v)
            } else {
                target.max(v)
            };
        }
        Some(target)
    }

    fn advance_to(&mut self, idx: usize, target: u64) -> Option<Result<()>> {
        loop {
            let cur = self.cursors.get(idx).copied().flatten()?;

            let needs_advance = if self.reverse {
                cur > target
            } else {
                cur < target
            };
            if !needs_advance {
                return Some(Ok(()));
            }

            match self.iters[idx].next() {
                Some(Ok(v)) => self.cursors[idx] = Some(v),
                Some(Err(e)) => return Some(Err(e)),
                None => {
                    self.cursors[idx] = None;
                    return None;
                }
            }
        }
    }
}

impl Iterator for AndOffsetIter {
    type Item = Result<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        if let Some(err) = self.pending_error.take() {
            self.done = true;
            return Some(Err(err));
        }

        match self.init() {
            Some(Ok(())) => {}
            Some(Err(e)) => {
                self.done = true;
                return Some(Err(e));
            }
            None => {
                self.done = true;
                return None;
            }
        }

        loop {
            let target = match self.target() {
                Some(v) => v,
                None => {
                    self.done = true;
                    return None;
                }
            };

            for i in 0..self.iters.len() {
                match self.advance_to(i, target) {
                    Some(Ok(())) => {}
                    Some(Err(e)) => {
                        self.done = true;
                        return Some(Err(e));
                    }
                    None => {
                        self.done = true;
                        return None;
                    }
                }
            }

            let first = self.cursors.first().copied().flatten();
            let all_equal =
                first.is_some() && self.cursors.iter().all(|v| v.is_some() && *v == first);

            if !all_equal {
                continue;
            }

            let out = first.unwrap_or(0);
            for i in 0..self.iters.len() {
                match self.iters[i].next() {
                    Some(Ok(v)) => self.cursors[i] = Some(v),
                    Some(Err(e)) => {
                        self.cursors[i] = None;
                        self.pending_error = Some(e);
                    }
                    None => self.cursors[i] = None,
                }
            }

            return Some(Ok(out));
        }
    }
}

enum OffsetIter {
    Single(DataEntryOffsetIter),
    Or(OffsetOrIter),
}

impl Iterator for OffsetIter {
    type Item = Result<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            OffsetIter::Single(iter) => iter.next(),
            OffsetIter::Or(iter) => iter.next(),
        }
    }
}

struct OffsetOrIter {
    reverse: bool,
    forward_heap: BinaryHeap<Reverse<OffsetHeapItem>>,
    reverse_heap: BinaryHeap<OffsetHeapItem>,
    iters: Vec<DataEntryOffsetIter>,
    pending_error: Option<SdJournalError>,
    last_emitted: Option<u64>,
    done: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct OffsetHeapItem {
    offset: u64,
    iter_idx: usize,
}

impl PartialOrd for OffsetHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OffsetHeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.offset
            .cmp(&other.offset)
            .then_with(|| self.iter_idx.cmp(&other.iter_idx))
    }
}

impl OffsetOrIter {
    fn new(mut iters: Vec<DataEntryOffsetIter>, reverse: bool) -> Self {
        let mut pending_error = None;
        let mut forward_heap = BinaryHeap::new();
        let mut reverse_heap = BinaryHeap::new();

        for (idx, iter) in iters.iter_mut().enumerate() {
            if let Some(offset) = next_ok_offset(iter, &mut pending_error) {
                let item = OffsetHeapItem {
                    offset,
                    iter_idx: idx,
                };
                if reverse {
                    reverse_heap.push(item);
                } else {
                    forward_heap.push(Reverse(item));
                }
            }
        }

        Self {
            reverse,
            forward_heap,
            reverse_heap,
            iters,
            pending_error,
            last_emitted: None,
            done: false,
        }
    }

    fn pop_next(&mut self) -> Option<OffsetHeapItem> {
        if self.reverse {
            self.reverse_heap.pop()
        } else {
            self.forward_heap.pop().map(|r| r.0)
        }
    }

    fn push_next(&mut self, item: OffsetHeapItem) {
        if self.reverse {
            self.reverse_heap.push(item);
        } else {
            self.forward_heap.push(Reverse(item));
        }
    }
}

impl Iterator for OffsetOrIter {
    type Item = Result<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            if let Some(err) = self.pending_error.take() {
                return Some(Err(err));
            }
            let item = match self.pop_next() {
                Some(item) => item,
                None => {
                    self.done = true;
                    return None;
                }
            };

            if let Some(offset) =
                next_ok_offset(&mut self.iters[item.iter_idx], &mut self.pending_error)
            {
                self.push_next(OffsetHeapItem {
                    offset,
                    iter_idx: item.iter_idx,
                });
            }

            if self.last_emitted == Some(item.offset) {
                continue;
            }

            self.last_emitted = Some(item.offset);
            return Some(Ok(item.offset));
        }
    }
}

struct FileBranchIter {
    file: crate::file::JournalFile,
    kind: BranchKind,
    exact_payloads: Vec<Vec<u8>>,
    since_realtime: Option<u64>,
    until_realtime: Option<u64>,
}

enum BranchKind {
    Empty,
    Cursor(Option<EntryMeta>),
    Indexed { offset_iter: AndOffsetIter },
    Scan { iter: FileEntryIter },
}

impl FileBranchIter {
    fn cursor(file: JournalFile, meta: EntryMeta) -> Self {
        Self {
            file,
            kind: BranchKind::Cursor(Some(meta)),
            exact_payloads: Vec::new(),
            since_realtime: None,
            until_realtime: None,
        }
    }

    fn new(
        file: crate::file::JournalFile,
        terms: Vec<MatchTerm>,
        reverse: bool,
        since_realtime: Option<u64>,
        until_realtime: Option<u64>,
        after_entry_offset: Option<u64>,
    ) -> Result<Self> {
        let exact_payloads: Vec<Vec<u8>> = terms
            .iter()
            .filter_map(|term| match term {
                MatchTerm::Exact { payload, .. } => Some(payload.clone()),
                MatchTerm::Present { .. } => None,
            })
            .collect();

        if exact_payloads.is_empty() {
            let iter = scan_iter(file.clone(), reverse, after_entry_offset)?;
            return Ok(Self {
                file,
                kind: BranchKind::Scan { iter },
                exact_payloads,
                since_realtime,
                until_realtime,
            });
        }

        let mut data_refs = Vec::new();
        for payload in &exact_payloads {
            match file.find_data_objects(payload) {
                Ok(refs) if refs.is_empty() => {
                    return Ok(Self {
                        file,
                        kind: BranchKind::Empty,
                        exact_payloads,
                        since_realtime,
                        until_realtime,
                    });
                }
                Ok(refs) => data_refs.push(refs),
                Err(SdJournalError::Corrupt { .. } | SdJournalError::Transient { .. }) => {
                    let iter = scan_iter(file.clone(), reverse, after_entry_offset)?;
                    return Ok(Self {
                        file,
                        kind: BranchKind::Scan { iter },
                        exact_payloads,
                        since_realtime,
                        until_realtime,
                    });
                }
                Err(error) => return Err(error),
            }
        }

        data_refs.sort_by_key(|refs| {
            refs.iter()
                .fold(0u64, |total, data| total.saturating_add(data.n_entries))
        });

        let mut iters = Vec::with_capacity(data_refs.len());
        for refs in data_refs {
            let mut term_iters = Vec::with_capacity(refs.len());
            for data_ref in refs {
                let result = match after_entry_offset {
                    Some(offset) => file.data_entry_offsets_after_offset(data_ref, reverse, offset),
                    None => file.data_entry_offsets(data_ref, reverse),
                };
                match result {
                    Ok(iter) => term_iters.push(iter),
                    Err(SdJournalError::Corrupt { .. } | SdJournalError::Transient { .. }) => {
                        let iter = scan_iter(file.clone(), reverse, after_entry_offset)?;
                        return Ok(Self {
                            file,
                            kind: BranchKind::Scan { iter },
                            exact_payloads,
                            since_realtime,
                            until_realtime,
                        });
                    }
                    Err(error) => return Err(error),
                }
            }
            match term_iters.len() {
                0 => {
                    return Ok(Self {
                        file,
                        kind: BranchKind::Empty,
                        exact_payloads,
                        since_realtime,
                        until_realtime,
                    });
                }
                1 => {
                    let Some(iter) = term_iters.pop() else {
                        return Ok(Self {
                            file,
                            kind: BranchKind::Empty,
                            exact_payloads,
                            since_realtime,
                            until_realtime,
                        });
                    };
                    iters.push(OffsetIter::Single(iter));
                }
                _ => iters.push(OffsetIter::Or(OffsetOrIter::new(term_iters, reverse))),
            }
        }

        Ok(Self {
            file,
            kind: BranchKind::Indexed {
                offset_iter: AndOffsetIter::new(iters, reverse),
            },
            exact_payloads,
            since_realtime,
            until_realtime,
        })
    }
}

impl Iterator for FileBranchIter {
    type Item = Result<EntryMeta>;

    fn next(&mut self) -> Option<Self::Item> {
        let since_realtime = self.since_realtime;
        let until_realtime = self.until_realtime;
        loop {
            match &mut self.kind {
                BranchKind::Empty => return None,
                BranchKind::Cursor(meta) => return meta.take().map(Ok),
                BranchKind::Indexed { offset_iter } => {
                    let entry_offset = match offset_iter.next()? {
                        Ok(v) => v,
                        Err(e) => {
                            self.kind = BranchKind::Empty;
                            return Some(Err(e));
                        }
                    };

                    let meta = match self.file.read_entry_meta(entry_offset) {
                        Ok(m) => m,
                        Err(e) => return Some(Err(e)),
                    };

                    let entry = match self.file.read_entry_ref(entry_offset) {
                        Ok(entry) => entry,
                        Err(error) => return Some(Err(error)),
                    };
                    if !self
                        .exact_payloads
                        .iter()
                        .all(|payload| entry_contains_payload(&entry, payload))
                    {
                        self.kind = BranchKind::Empty;
                        return Some(Err(self.file.metadata_error(
                            Some(entry_offset),
                            "DATA index references an ENTRY that does not contain the indexed field",
                        )));
                    }

                    if !time_matches(&meta, since_realtime, until_realtime) {
                        continue;
                    }

                    return Some(Ok(meta));
                }
                BranchKind::Scan { iter } => match iter.next()? {
                    Ok(meta) => {
                        if !time_matches(&meta, since_realtime, until_realtime) {
                            continue;
                        }
                        return Some(Ok(meta));
                    }
                    Err(e) => return Some(Err(e)),
                },
            }
        }
    }
}

fn scan_iter(
    file: crate::file::JournalFile,
    reverse: bool,
    after_entry_offset: Option<u64>,
) -> Result<FileEntryIter> {
    match after_entry_offset {
        Some(offset) => file.entry_iter_after_offset(reverse, offset, None, None),
        None => file.entry_iter_seek_realtime(reverse, None, None),
    }
}

fn time_matches(meta: &EntryMeta, since: Option<u64>, until: Option<u64>) -> bool {
    since.is_none_or(|value| meta.realtime_usec >= value)
        && until.is_none_or(|value| meta.realtime_usec <= value)
}

pub(super) struct JournalIter(EagerJournalIter);

impl JournalIter {
    pub(super) fn new(query: JournalQuery) -> Result<Self> {
        EagerJournalIter::new(query).map(Self)
    }
}

impl Iterator for JournalIter {
    type Item = Result<EntryRef>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

pub(super) struct EagerJournalIter {
    query: JournalQuery,
    cursor_start: Option<CursorStart>,
    cursor_reached: bool,
    produced: usize,
    last_emitted: Option<EntryMeta>,
    candidates: Vec<Option<HeapItem>>,
    iters: Vec<FileMetaIter>,
    files: Vec<JournalFile>,
    pending_error: Option<SdJournalError>,
    done: bool,
}

#[derive(Clone, Copy)]
struct HeapItem {
    meta: EntryMeta,
    iter_idx: usize,
}

fn matching_file_indexes(query: &JournalQuery) -> Vec<usize> {
    (0..query.journal.inner.file_count())
        .filter(|idx| {
            let Some(info) = query.journal.inner.file_info(*idx) else {
                return false;
            };
            info_may_match_query(info)
        })
        .collect()
}

fn info_may_match_query(info: &JournalFileInfo) -> bool {
    if !info.entry_range_known {
        return true;
    }

    info.entry_range.is_some()
}

impl EagerJournalIter {
    fn validate_files(&self) -> Result<()> {
        // Empty snapshots also participate: otherwise a retirement can turn
        // into a clean query(false) without touching an entry iterator.
        self.query.journal.inner.validate_snapshot_owners()
    }
    fn new(query: JournalQuery) -> Result<Self> {
        if matches!(query.limit, Some(0)) {
            return Self::new_inner(query);
        }
        query.journal.inner.validate_snapshot_owners()?;
        let journal = query.journal.clone();
        let result = Self::new_inner(query);
        match (result, journal.inner.validate_snapshot_owners()) {
            (_, Err(error @ SdJournalError::Retired { .. })) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (result, _) => result,
        }
    }

    fn new_inner(query: JournalQuery) -> Result<Self> {
        if matches!(query.limit, Some(0)) {
            return Ok(Self {
                query,
                cursor_start: None,
                cursor_reached: true,
                produced: 0,
                last_emitted: None,
                candidates: Vec::new(),
                iters: Vec::new(),
                files: Vec::new(),
                pending_error: None,
                done: true,
            });
        }

        let mut pending_error = None;
        let cursor_start = build_cursor_start(&query)?;
        let cursor_reached = cursor_start
            .as_ref()
            .is_none_or(|cursor| cursor.exact_meta().is_none());
        let branches = build_branches(&query);
        let file_indexes = matching_file_indexes(&query);

        let mut iters = Vec::with_capacity(file_indexes.len());
        let mut files = Vec::with_capacity(file_indexes.len());
        for file_idx in file_indexes.iter().copied() {
            let file = match query.journal.inner.open_file_by_index(file_idx) {
                Ok(file) => file,
                Err(e) => {
                    pending_error.get_or_insert(e);
                    continue;
                }
            };
            let mut branch_iters = Vec::with_capacity(branches.len());
            for terms in &branches {
                match FileBranchIter::new(
                    file.clone(),
                    terms.clone(),
                    query.reverse,
                    query.since_realtime,
                    query.until_realtime,
                    None,
                ) {
                    Ok(it) => branch_iters.push(it),
                    Err(e) => {
                        pending_error.get_or_insert(e);
                    }
                }
            }
            if let Some(cursor_meta) = cursor_start
                .as_ref()
                .and_then(CursorStart::exact_meta)
                .filter(|meta| meta.file_id == file.file_id())
            {
                branch_iters.push(FileBranchIter::cursor(file.clone(), cursor_meta));
            }
            iters.push(FileMetaIter::from_branch_iters(branch_iters, query.reverse));
            files.push(file);
        }

        let mut candidates = vec![None; iters.len()];

        for (idx, it) in iters.iter_mut().enumerate() {
            if let Some(meta) = next_ok_meta(it, &mut pending_error) {
                candidates[idx] = Some(HeapItem {
                    meta,
                    iter_idx: idx,
                });
            }
        }

        Ok(Self {
            query,
            cursor_start,
            cursor_reached,
            produced: 0,
            last_emitted: None,
            candidates,
            iters,
            files,
            pending_error,
            done: false,
        })
    }

    fn pop_next(&mut self) -> Option<HeapItem> {
        let mut selected: Option<usize> = None;
        for (idx, candidate) in self.candidates.iter().enumerate() {
            let Some(candidate) = candidate else {
                continue;
            };
            let Some(best_idx) = selected else {
                selected = Some(idx);
                continue;
            };
            let Some(best) = self.candidates[best_idx] else {
                continue;
            };
            let ordering = compare_meta(&candidate.meta, &best.meta);
            let replaces = if self.query.reverse {
                ordering == std::cmp::Ordering::Greater
                    || (ordering == std::cmp::Ordering::Equal && candidate.iter_idx < best.iter_idx)
            } else {
                ordering == std::cmp::Ordering::Less
                    || (ordering == std::cmp::Ordering::Equal && candidate.iter_idx < best.iter_idx)
            };
            if replaces {
                selected = Some(idx);
            }
        }
        selected.and_then(|idx| self.candidates[idx].take())
    }

    fn passes_filters(&self, meta: &EntryMeta) -> bool {
        if let Some(since) = self.query.since_realtime
            && meta.realtime_usec < since
        {
            return false;
        }
        if let Some(until) = self.query.until_realtime
            && meta.realtime_usec > until
        {
            return false;
        }

        true
    }

    fn passes_cursor(&mut self, meta: &EntryMeta) -> bool {
        let Some(cursor_start) = &self.cursor_start else {
            return true;
        };
        if cursor_start.exact_meta().is_none() {
            return cursor_start.accepts(meta);
        }
        if self.query.reverse {
            if self.cursor_reached {
                self.done = true;
                return false;
            }
            if cursor_start.matches_exact(meta) {
                self.cursor_reached = true;
                if !cursor_start.inclusive() {
                    self.done = true;
                }
                return cursor_start.inclusive();
            }
            return true;
        }
        if self.cursor_reached {
            !cursor_start.matches_exact(meta)
        } else if cursor_start.matches_exact(meta) {
            self.cursor_reached = true;
            cursor_start.inclusive()
        } else {
            false
        }
    }
}

impl Iterator for EagerJournalIter {
    type Item = Result<EntryRef>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            if let Some(err) = self.pending_error.take() {
                // Construction errors can have been caused by vacuum after opening.
                if let Err(retired @ SdJournalError::Retired { .. }) = self.validate_files() {
                    return Some(Err(retired));
                }
                return Some(Err(err));
            }

            if let Some(limit) = self.query.limit
                && (limit == 0 || self.produced >= limit)
            {
                self.done = true;
                return None;
            }
            let item = match self.pop_next() {
                Some(item) => item,
                None => {
                    self.done = true;
                    return self.validate_files().err().map(Err);
                }
            };

            if let Some(next_meta) =
                next_ok_meta(&mut self.iters[item.iter_idx], &mut self.pending_error)
            {
                self.candidates[item.iter_idx] = Some(HeapItem {
                    meta: next_meta,
                    iter_idx: item.iter_idx,
                });
            }

            if !self.passes_cursor(&item.meta) {
                if self.done {
                    return None;
                }
                continue;
            }
            if !self.passes_filters(&item.meta) {
                continue;
            }

            if self
                .last_emitted
                .is_some_and(|last| same_meta_entry(&last, &item.meta))
            {
                continue;
            }

            let entry = match self.files[item.iter_idx].read_entry_ref(item.meta.entry_offset) {
                Ok(e) => e,
                Err(e) => return Some(Err(e)),
            };
            if !entry_ref_matches_query(&entry, &self.query) {
                continue;
            }

            self.last_emitted = Some(item.meta);
            self.produced = self.produced.saturating_add(1);
            return Some(Ok(entry));
        }
    }
}

fn entry_contains_payload(entry: &EntryRef, payload: &[u8]) -> bool {
    let Some(eq_pos) = payload.iter().position(|byte| *byte == b'=') else {
        return false;
    };
    let Some(field) = payload.get(..eq_pos) else {
        return false;
    };
    let Some(value) = payload.get(eq_pos.saturating_add(1)..) else {
        return false;
    };

    entry
        .iter_fields()
        .any(|(name, candidate)| name.as_bytes() == field && candidate == value)
}

fn compare_meta(left: &EntryMeta, right: &EntryMeta) -> std::cmp::Ordering {
    compare_entry_keys(&entry_key_from_meta(left), &entry_key_from_meta(right))
}

fn same_meta_entry(left: &EntryMeta, right: &EntryMeta) -> bool {
    same_entry(&entry_key_from_meta(left), &entry_key_from_meta(right))
}

fn next_ok_meta<I>(it: &mut I, pending: &mut Option<SdJournalError>) -> Option<EntryMeta>
where
    I: Iterator<Item = Result<EntryMeta>>,
{
    match it.next()? {
        Ok(meta) => Some(meta),
        Err(error) => {
            pending.get_or_insert(error);
            None
        }
    }
}

fn next_ok_offset<I>(it: &mut I, pending: &mut Option<SdJournalError>) -> Option<u64>
where
    I: Iterator<Item = Result<u64>>,
{
    match it.next()? {
        Ok(offset) => Some(offset),
        Err(error) => {
            pending.get_or_insert(error);
            None
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod pending_error_tests {
    use super::*;
    use crate::{JournalConfig, MmapPolicy};
    use std::{fs::OpenOptions, os::unix::fs::FileExt as _};
    #[allow(dead_code)]
    mod fixture {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/synthetic_journal.rs"
        ));
    }

    #[test]
    fn file_or_duplicate_does_not_hide_branch_error_before_a_later_row() {
        let layout = fixture::SyntheticJournalFile::new(&["a.service"; 3]);
        let path = layout.root().join("synthetic.journal");
        let file = JournalFile::open(
            path.clone(),
            &JournalConfig {
                mmap_policy: MmapPolicy::Never,
                ..Default::default()
            },
        )
        .unwrap();
        let mut iter = FileOrIter::new(
            vec![
                FileBranchIter::new(file.clone(), vec![], false, None, None, None).unwrap(),
                FileBranchIter::new(file, vec![], false, None, None, None).unwrap(),
            ],
            false,
        );
        assert_eq!(iter.next().unwrap().unwrap().seqnum, 1);
        let owner = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let mut offset = [0; 8];
        owner.read_exact_at(&mut offset, 176).unwrap();
        let array = u64::from_le_bytes(offset);
        owner.read_exact_at(&mut offset, array + 24 + 8).unwrap();
        owner
            .write_all_at(&[0], u64::from_le_bytes(offset))
            .unwrap();
        assert!(matches!(
            iter.next(),
            Some(Err(SdJournalError::Corrupt { .. }))
        ));
    }

    #[test]
    fn offset_or_duplicate_does_not_turn_array_error_into_eof() {
        let layout = fixture::SyntheticJournalFile::new(&["a.service"; 3]);
        let path = layout.root().join("synthetic.journal");
        let owner = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut bytes = [0; 8];
        owner.read_exact_at(&mut bytes, 176).unwrap();
        let global = u64::from_le_bytes(bytes);
        let mut offsets = [0u64; 3];
        for (index, offset) in offsets.iter_mut().enumerate() {
            owner
                .read_exact_at(&mut bytes, global + 24 + index as u64 * 8)
                .unwrap();
            *offset = u64::from_le_bytes(bytes);
        }
        // A DATA index may contain one direct entry plus an ENTRY_ARRAY suffix.
        // Add that valid suffix as a real object; retain the original global index.
        let array = owner.metadata().unwrap().len();
        let mut object = [0u8; 56];
        object[0] = crate::format::OBJECT_ENTRY_ARRAY;
        object[8..16].copy_from_slice(&56u64.to_le_bytes());
        object[24..32].copy_from_slice(&offsets[1].to_le_bytes());
        object[32..40].copy_from_slice(&offsets[2].to_le_bytes());
        owner.write_all_at(&object, array).unwrap();
        owner
            .write_all_at(&(array + 56 - 272).to_le_bytes(), 96)
            .unwrap();
        owner.write_all_at(&array.to_le_bytes(), 136).unwrap();
        owner.write_all_at(&[2], 16).unwrap();
        let file = JournalFile::open(
            path,
            &JournalConfig {
                mmap_policy: MmapPolicy::Never,
                ..Default::default()
            },
        )
        .unwrap();
        let direct = file
            .find_data_objects(b"_SYSTEMD_UNIT=a.service")
            .unwrap()
            .into_iter()
            .find(|data| data.entry_offset == offsets[0])
            .unwrap();
        let mut suffix = direct;
        suffix.n_entries = 3;
        suffix.entry_array_offset = array;
        let mut iter = OffsetOrIter::new(
            vec![
                DataEntryOffsetIter::new(file.clone(), direct, false).unwrap(),
                DataEntryOffsetIter::new(file, suffix, false).unwrap(),
            ],
            false,
        );
        assert_eq!(iter.next().unwrap().unwrap(), offsets[0]);
        owner.write_all_at(&0u64.to_le_bytes(), array + 24).unwrap();
        assert!(matches!(
            iter.next(),
            Some(Err(SdJournalError::Corrupt { .. }))
        ));
    }
}
