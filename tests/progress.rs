#![cfg(target_os = "linux")]
#[allow(dead_code)]
mod support;
use sdjournal::{
    DataCacheConfig, FileIdentity, FilePosition, Journal, JournalConfig, MmapPolicy, ScanItem,
    ScanOptions, SdJournalError,
};
use std::{
    fs::{self, OpenOptions},
    os::unix::fs::FileExt,
    path::Path,
};
use support::synthetic_journal::{SyntheticJournalFile, write_synthetic_journal_file};

fn config(budget: usize) -> JournalConfig {
    JournalConfig {
        mmap_policy: MmapPolicy::Never,
        max_open_files: budget,
        ..Default::default()
    }
}
fn open(path: &Path, budget: usize) -> Journal {
    Journal::open_progress_dirs(&[path.to_owned()], config(budget), &[]).unwrap()
}
fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}
fn offsets(path: &Path) -> Vec<u64> {
    let bytes = fs::read(path).unwrap();
    let array = u64_at(&bytes, 176) as usize;
    (0..u64_at(&bytes, 152) as usize)
        .map(|index| u64_at(&bytes, array + 24 + index * 8))
        .collect()
}
fn field(path: &Path, entry: usize, index: usize) -> u64 {
    let bytes = fs::read(path).unwrap();
    u64_at(&bytes, offsets(path)[entry] as usize + 64 + index * 16)
}
fn write(path: &Path, offset: u64, bytes: &[u8]) {
    OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .write_all_at(bytes, offset)
        .unwrap();
}
fn records(
    journal: &Journal,
    marks: &[FilePosition],
    options: ScanOptions,
) -> Vec<(String, FilePosition, Vec<FilePosition>)> {
    journal
        .scan_from_positions(marks, options)
        .unwrap()
        .map(|item| match item.unwrap() {
            ScanItem::Entry {
                entry,
                position,
                aliases,
            } => (
                String::from_utf8(entry.get("MESSAGE").unwrap().to_vec()).unwrap(),
                position,
                aliases,
            ),
            other => panic!("unexpected {other:?}"),
        })
        .collect()
}
fn cache() -> ScanOptions {
    ScanOptions {
        snapshot_cache: Some(DataCacheConfig::default()),
        ..Default::default()
    }
}
// Point byte-identical fields at the same immutable DATA objects. This fixture
// targets unfiltered ENTRY traversal, not DATA's inverted match index.
fn share(path: &Path) {
    let entries = offsets(path);
    for &entry in &entries[1..] {
        for index in [0, 1, 2, 5] {
            write(
                path,
                entry + 64 + index * 16,
                &field(path, 0, index as usize).to_le_bytes(),
            );
        }
    }
    write(path, 16, &[0]);
}

#[test]
fn positions_resume_exact_suffix_and_empty_tail_in_eager_and_lazy_modes() {
    for budget in [1, 64] {
        let root = tempfile::tempdir().unwrap();
        write_synthetic_journal_file(&root.path().join("a.journal"), &["a", "b", "c"], 7);
        write_synthetic_journal_file(&root.path().join("b.journal"), &["d"], 8);
        let journal = open(root.path(), budget);
        let all = records(&journal, &[], ScanOptions::default());
        assert_eq!(all.len(), 4);
        let first = all
            .iter()
            .find(|(_, p, _)| p.ordinal == 1 && p.file_id == all[0].1.file_id)
            .unwrap()
            .1;
        let suffix = records(&journal, &[first], ScanOptions::default());
        assert_eq!(suffix.len(), 3);
        let mut tails = std::collections::HashMap::new();
        for (_, p, aliases) in all {
            tails.insert(p.file_id, p);
            for p in aliases {
                tails.insert(p.file_id, p);
            }
        }
        assert!(
            records(
                &journal,
                &tails.into_values().collect::<Vec<_>>(),
                ScanOptions::default()
            )
            .is_empty()
        );
    }
}

#[test]
fn rejected_or_unconsumed_lookahead_never_changes_resume_positions() {
    let f = SyntheticJournalFile::new(&["a", "b", "c"]);
    let journal = open(f.root(), 64);
    let mut scan = journal
        .scan_from_positions(&[], ScanOptions::default())
        .unwrap();
    let first = match scan.next().unwrap().unwrap() {
        ScanItem::Entry { position, .. } => position,
        _ => panic!(),
    };
    let unconsumed = scan.next().unwrap().unwrap();
    drop(unconsumed);
    drop(scan);
    let suffix = records(&journal, &[first], ScanOptions::default());
    assert_eq!(
        suffix.iter().map(|(_, p, _)| p.ordinal).collect::<Vec<_>>(),
        [2, 3]
    );
}

#[test]
fn invalid_anchor_replays_without_inventing_coverage() {
    let f = SyntheticJournalFile::new(&["a", "b"]);
    let journal = open(f.root(), 64);
    let mut position = records(&journal, &[], ScanOptions::default())[0].1;
    position.xor_hash ^= 1;
    let mut scan = journal
        .scan_from_positions(&[position], ScanOptions::default())
        .unwrap();
    assert!(matches!(
        scan.next().unwrap().unwrap(),
        ScanItem::Replayed { .. }
    ));
    assert_eq!(
        scan.filter(|x| matches!(x, Ok(ScanItem::Entry { .. })))
            .count(),
        2
    );
}

#[test]
fn eof_does_not_accept_a_contradictory_tail_sequence() {
    let f = SyntheticJournalFile::new(&["a", "b"]);
    let path = f.root().join("synthetic.journal");
    let position = records(&open(f.root(), 64), &[], ScanOptions::default())
        .last()
        .unwrap()
        .1;
    write(&path, 160, &999u64.to_le_bytes());
    let journal = open(f.root(), 64);
    let outcome = journal.scan_from_positions(&[position], ScanOptions::default());
    assert!(
        match outcome {
            Err(_) => true,
            Ok(mut scan) => scan.next().is_some(),
        },
        "contradictory tail became clean EOF"
    );
}

#[test]
fn corrupt_entry_header_keeps_trusted_slot_but_later_entry_supplies_anchor() {
    for budget in [1, 64] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("a.journal");
        write_synthetic_journal_file(&path, &["a", "b", "c"], 7);
        write_synthetic_journal_file(&root.path().join("b.journal"), &["healthy"], 8);
        let offset = offsets(&path)[1];
        write(&path, offset, &[0]);
        let journal = open(root.path(), budget);
        let items = journal
            .scan_from_positions(&[], ScanOptions::default())
            .unwrap()
            .collect::<Vec<_>>();
        assert!(items.iter().any(|item|matches!(item,Ok(ScanItem::Omitted{entry_offset,ordinal:2,position:None,..}) if *entry_offset==offset)));
        assert_eq!(
            items
                .iter()
                .filter(|item| matches!(item, Ok(ScanItem::Entry { .. })))
                .count(),
            3
        );
    }
}

#[test]
fn corrupt_data_has_an_entry_position_and_other_records_continue() {
    let f = SyntheticJournalFile::new(&["a", "b", "c"]);
    let path = f.root().join("synthetic.journal");
    write(&path, field(&path, 1, 3), &[0]);
    let journal = open(f.root(), 64);
    let items = journal
        .scan_from_positions(&[], cache())
        .unwrap()
        .collect::<Vec<_>>();
    assert!(items.iter().any(|item|matches!(item,Ok(ScanItem::Omitted{ordinal:2,position:Some(position),..}) if position.entry_offset==offsets(&path)[1])));
    assert_eq!(
        items
            .iter()
            .filter(|item| matches!(item, Ok(ScanItem::Entry { .. })))
            .count(),
        2
    );
}

#[test]
fn untrusted_array_stops_only_its_file() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("a.journal");
    write_synthetic_journal_file(&path, &["a", "b"], 7);
    write_synthetic_journal_file(&root.path().join("b.journal"), &["healthy"], 8);
    let array = u64_at(&fs::read(&path).unwrap(), 176);
    write(&path, array, &[0]);
    let items = open(root.path(), 64)
        .scan_from_positions(&[], ScanOptions::default())
        .unwrap()
        .collect::<Vec<_>>();
    assert!(
        items
            .iter()
            .any(|x| matches!(x, Ok(ScanItem::FileStopped { .. })))
    );
    assert_eq!(
        items
            .iter()
            .filter(|x| matches!(x, Ok(ScanItem::Entry { .. })))
            .count(),
        1
    );
}

#[test]
fn initial_bad_header_is_retained_even_when_no_file_can_open() {
    let root = tempfile::tempdir().unwrap();
    fs::write(root.path().join("bad.journal"), vec![0; 272]).unwrap();
    let journal = open(root.path(), 64);
    assert_eq!(journal.open_errors().len(), 1);
    assert!(journal.snapshot_files().is_empty());
    assert!(
        journal
            .scan_from_positions(&[], ScanOptions::default())
            .unwrap()
            .next()
            .is_none()
    );
}

#[test]
fn empty_catalog_is_clean_but_partial_discovery_errors_remain_visible() {
    let root = tempfile::tempdir().unwrap();
    let empty = Journal::open_progress_dirs(&[root.path().to_owned()], config(64), &[]).unwrap();
    assert!(empty.snapshot_files().is_empty());
    assert!(empty.open_errors().is_empty());
    write_synthetic_journal_file(&root.path().join("healthy.journal"), &["healthy"], 7);
    let invalid = root.path().join("not-a-directory");
    fs::write(&invalid, b"x").unwrap();
    let journal =
        Journal::open_progress_dirs(&[root.path().to_owned(), invalid.clone()], config(64), &[])
            .unwrap();
    assert_eq!(journal.open_errors().len(), 1);
    assert_eq!(journal.open_errors()[0].0, invalid);
    assert_eq!(records(&journal, &[], ScanOptions::default()).len(), 1);
}

#[test]
fn deferred_discovery_error_is_not_retried_on_each_batch() {
    let root = tempfile::tempdir().unwrap();
    write_synthetic_journal_file(&root.path().join("healthy.journal"), &["healthy"], 7);
    let invalid = root.path().join("not-a-directory");
    fs::write(&invalid, b"x").unwrap();
    let deferred = FileIdentity::observe(&invalid);
    let journal =
        Journal::open_progress_dirs(&[root.path().to_owned(), invalid], config(64), &[deferred])
            .unwrap();
    assert!(journal.open_errors().is_empty());
    assert_eq!(journal.deferred_files().len(), 1);
    assert_eq!(records(&journal, &[], ScanOptions::default()).len(), 1);
}

#[test]
fn deferred_unknown_path_is_not_opened_and_replacement_is_eligible() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("bad.journal");
    fs::write(&path, vec![0; 272]).unwrap();
    let deferred = FileIdentity::observe(&path);
    let journal = Journal::open_progress_dirs(
        &[root.path().to_owned()],
        config(64),
        std::slice::from_ref(&deferred),
    )
    .unwrap();
    assert!(journal.open_errors().is_empty());
    assert_eq!(journal.deferred_files().len(), 1);
    let old = OpenOptions::new().read(true).open(&path).unwrap();
    fs::remove_file(&path).unwrap();
    write_synthetic_journal_file(&path, &["new"], 9);
    let journal =
        Journal::open_progress_dirs(&[root.path().to_owned()], config(64), &[deferred]).unwrap();
    assert!(journal.deferred_files().is_empty());
    assert_eq!(records(&journal, &[], ScanOptions::default()).len(), 1);
    drop(old);
}

#[test]
fn duplicate_aliases_are_consumed_with_the_owned_entry_and_legacy_is_ambiguous() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("a.journal");
    let other = root.path().join("b.journal");
    write_synthetic_journal_file(&path, &["a", "b"], 7);
    fs::copy(&path, &other).unwrap();
    write(&other, 24, &[44; 16]);
    let journal = open(root.path(), 64);
    let all = records(&journal, &[], ScanOptions::default());
    assert_eq!(all.len(), 2);
    assert!(all.iter().all(|(_, _, a)| a.len() == 1));
    let cursor = sdjournal::Cursor::parse(
        &journal
            .query()
            .iter()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .cursor()
            .unwrap()
            .to_string(),
    )
    .unwrap();
    assert!(
        journal.resolve_legacy_position(&cursor).unwrap().is_none(),
        "native legacy cursor cannot select an arbitrary alias file"
    );
}

#[test]
fn snapshot_bounds_exclude_new_files_and_report_an_original_missing_owner() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("a.journal");
    write_synthetic_journal_file(&path, &["a"], 7);
    let original = open(root.path(), 64).snapshot_files();
    fs::remove_file(&path).unwrap();
    write_synthetic_journal_file(&root.path().join("b.journal"), &["new"], 8);
    let journal = open(root.path(), 64);
    let items = journal
        .scan_from_positions(
            &[],
            ScanOptions {
                boundaries: Some(original),
                ..Default::default()
            },
        )
        .unwrap()
        .collect::<Vec<_>>();
    assert!(
        items
            .iter()
            .any(|x| matches!(x, Ok(ScanItem::FileStopped { .. }))),
        "missing original range became complete empty"
    );
    assert!(
        !items
            .iter()
            .any(|x| matches!(x, Ok(ScanItem::Entry { .. })))
    );
}

#[test]
fn offline_payload_cache_is_opt_in_bounded_and_preserves_exact_values() {
    let units = vec!["same.service"; 64];
    let f = SyntheticJournalFile::new(&units);
    let path = f.root().join("synthetic.journal");
    share(&path);
    let journal = open(f.root(), 64);
    let plain = records(&journal, &[], ScanOptions::default());
    let mut scan = journal.scan_from_positions(&[], cache()).unwrap();
    let mut cached = Vec::new();
    for item in scan.by_ref() {
        match item.unwrap() {
            ScanItem::Entry {
                entry,
                position,
                aliases,
            } => cached.push((
                String::from_utf8(entry.get("MESSAGE").unwrap().to_vec()).unwrap(),
                position,
                aliases,
            )),
            x => panic!("{x:?}"),
        }
    }
    assert_eq!(plain, cached);
    assert!(scan.cache_stats().hits >= 63 * 4);
    assert!(scan.cache_stats().bytes <= 8 * 1024 * 1024);
    assert!(scan.cache_stats().entries <= 4096);
    let mut small = journal
        .scan_from_positions(
            &[],
            ScanOptions {
                snapshot_cache: Some(DataCacheConfig {
                    max_bytes: 128,
                    max_entries: 2,
                }),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        small
            .by_ref()
            .filter(|x| matches!(x, Ok(ScanItem::Entry { .. })))
            .count(),
        64
    );
    assert!(small.cache_stats().bytes <= 128);
    assert!(small.cache_stats().entries <= 2);
    assert!(small.cache_stats().evictions > 0);
}

#[test]
fn live_file_does_not_enter_snapshot_cache() {
    let f = SyntheticJournalFile::new(&["a", "a"]);
    let journal = open(f.root(), 64);
    let mut scan = journal.scan_from_positions(&[], cache()).unwrap();
    assert_eq!(
        scan.by_ref()
            .filter(|x| matches!(x, Ok(ScanItem::Entry { .. })))
            .count(),
        2
    );
    assert_eq!(scan.cache_stats().entries, 0);
    assert_eq!(scan.cache_stats().hits, 0);
}

#[test]
fn same_snapshot_reuses_verified_payload_but_fresh_snapshot_observes_corruption() {
    let f = SyntheticJournalFile::new(&["same", "same"]);
    let path = f.root().join("synthetic.journal");
    share(&path);
    let journal = open(f.root(), 64);
    let mut scan = journal.scan_from_positions(&[], cache()).unwrap();
    assert!(matches!(
        scan.next().unwrap().unwrap(),
        ScanItem::Entry { .. }
    ));
    write(&path, field(&path, 0, 0), &[0]);
    assert!(matches!(
        scan.next().unwrap().unwrap(),
        ScanItem::Entry { .. }
    ));
    let fresh = open(f.root(), 64)
        .scan_from_positions(&[], cache())
        .unwrap()
        .collect::<Vec<_>>();
    assert!(
        fresh
            .iter()
            .any(|x| matches!(x, Ok(ScanItem::Omitted { .. })))
    );
}

#[test]
fn cached_payloads_do_not_hide_original_owner_retirement() {
    for budget in [1, 64] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("a.journal");
        write_synthetic_journal_file(&path, &["same", "same"], 7);
        share(&path);
        write_synthetic_journal_file(&root.path().join("b.journal"), &["later"], 99);
        let journal = open(root.path(), budget);
        let mut scan = journal.scan_from_positions(&[], cache()).unwrap();
        assert!(matches!(
            scan.next().unwrap().unwrap(),
            ScanItem::Entry { .. }
        ));
        let owner = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        fs::remove_file(&path).unwrap();
        rustix::fs::fallocate(
            &owner,
            rustix::fs::FallocateFlags::PUNCH_HOLE | rustix::fs::FallocateFlags::KEEP_SIZE,
            0,
            owner.metadata().unwrap().len(),
        )
        .unwrap();
        assert!(matches!(
            scan.next().unwrap(),
            Err(SdJournalError::Retired { .. })
        ));
    }
}

#[test]
fn invalid_ordinal_replays_instead_of_claiming_tail() {
    let f = SyntheticJournalFile::new(&["a", "b", "c"]);
    let journal = open(f.root(), 64);
    let mut position = records(&journal, &[], ScanOptions::default())
        .last()
        .unwrap()
        .1;
    position.ordinal = 1;
    let mut scan = journal
        .scan_from_positions(&[position], ScanOptions::default())
        .unwrap();
    assert!(matches!(
        scan.next().unwrap().unwrap(),
        ScanItem::Replayed { .. }
    ));
    assert_eq!(
        scan.filter(|x| matches!(x, Ok(ScanItem::Entry { .. })))
            .count(),
        3
    );
}

#[test]
fn count_before_tail_does_not_become_false_empty() {
    let f = SyntheticJournalFile::new(&["a", "b"]);
    let path = f.root().join("synthetic.journal");
    let position = records(&open(f.root(), 64), &[], ScanOptions::default())
        .last()
        .unwrap()
        .1;
    write(&path, 152, &3u64.to_le_bytes());
    let result = open(f.root(), 64).scan_from_positions(&[position], ScanOptions::default());
    assert!(match result {
        Err(_) => true,
        Ok(mut scan) => scan.next().is_some(),
    });
}

#[test]
fn legacy_resolution_is_metadata_only_and_unrelated_files_are_not_acknowledged() {
    let f = SyntheticJournalFile::new(&["a", "b"]);
    let path = f.root().join("synthetic.journal");
    let original = open(f.root(), 64);
    let last = original
        .query()
        .iter()
        .unwrap()
        .last()
        .unwrap()
        .unwrap()
        .cursor()
        .unwrap();
    let cursor = sdjournal::Cursor::parse(&last.to_string()).unwrap();
    write(&path, field(&path, 0, 3), &[0]);
    let position = open(f.root(), 64)
        .resolve_legacy_position(&cursor)
        .unwrap()
        .unwrap();
    assert_eq!(position.ordinal, 2);
    write_synthetic_journal_file(&f.root().join("older.journal"), &["old"], 1);
    let journal = open(f.root(), 64);
    let output = records(&journal, &[position], ScanOptions::default());
    assert_eq!(output.len(), 1);
    assert_ne!(output[0].1.file_id, position.file_id);
}

#[test]
fn full_boundary_keeps_original_committed_count_across_later_publication() {
    let f = SyntheticJournalFile::new(&["a", "b", "c"]);
    let path = f.root().join("synthetic.journal");
    let entries = offsets(&path);
    // The third entry is allocated, but not yet published in the committed count.
    write(&path, 152, &2u64.to_le_bytes());
    write(&path, 160, &2u64.to_le_bytes());
    write(&path, 264, &entries[1].to_le_bytes());
    let boundaries = open(f.root(), 64).snapshot_files();
    write(&path, 152, &3u64.to_le_bytes());
    write(&path, 160, &3u64.to_le_bytes());
    write(&path, 264, &entries[2].to_le_bytes());
    let journal = open(f.root(), 64);
    assert_eq!(records(&journal, &[], ScanOptions::default()).len(), 3);
    assert_eq!(
        records(
            &journal,
            &[],
            ScanOptions {
                boundaries: Some(boundaries),
                ..Default::default()
            }
        )
        .len(),
        2
    );
}

#[test]
fn pending_entries_excludes_error_only_end_and_tracks_healthy_unread_slots() {
    let f = SyntheticJournalFile::new(&["a", "b", "c"]);
    let path = f.root().join("synthetic.journal");
    write(&path, *offsets(&path).last().unwrap(), &[0]);
    let journal = open(f.root(), 64);
    let mut scan = journal
        .scan_from_positions(&[], ScanOptions::default())
        .unwrap();
    assert!(scan.has_pending_entries());
    assert!(matches!(
        scan.next().unwrap().unwrap(),
        ScanItem::Entry { .. }
    ));
    assert!(scan.has_pending_entries());
    assert!(matches!(
        scan.next().unwrap().unwrap(),
        ScanItem::Entry { .. }
    ));
    assert!(scan.has_pending_entries());
    assert!(matches!(
        scan.next().unwrap().unwrap(),
        ScanItem::Omitted { position: None, .. }
    ));
    assert!(!scan.has_pending_entries());
}

#[test]
fn failed_entry_never_seeds_cache_and_evicted_entries_remain_owned() {
    let f = SyntheticJournalFile::new(&["a", "b"]);
    let path = f.root().join("synthetic.journal");
    write(&path, 16, &[0]);
    write(&path, field(&path, 0, 5), &[0]);
    let journal = open(f.root(), 64);
    let mut scan = journal.scan_from_positions(&[], cache()).unwrap();
    assert!(matches!(
        scan.next().unwrap().unwrap(),
        ScanItem::Omitted { .. }
    ));
    assert_eq!(scan.cache_stats().entries, 0);
    assert_eq!(scan.cache_stats().bytes, 0);
    assert!(matches!(
        scan.next().unwrap().unwrap(),
        ScanItem::Entry { .. }
    ));
    assert_eq!(scan.cache_stats().entries, 6);
    let f = SyntheticJournalFile::new(&["same", "same", "same"]);
    let path = f.root().join("synthetic.journal");
    share(&path);
    let journal = open(f.root(), 64);
    let mut scan = journal
        .scan_from_positions(
            &[],
            ScanOptions {
                snapshot_cache: Some(DataCacheConfig {
                    max_bytes: 128,
                    max_entries: 2,
                }),
                ..Default::default()
            },
        )
        .unwrap();
    let entry = match scan.next().unwrap().unwrap() {
        ScanItem::Entry { entry, .. } => entry,
        _ => panic!(),
    };
    let expected = entry.get("MESSAGE").unwrap().to_vec();
    assert_eq!(
        scan.by_ref()
            .filter(|x| matches!(x, Ok(ScanItem::Entry { .. })))
            .count(),
        2
    );
    assert!(scan.cache_stats().evictions > 0);
    let owner = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    fs::remove_file(&path).unwrap();
    rustix::fs::fallocate(
        &owner,
        rustix::fs::FallocateFlags::PUNCH_HOLE | rustix::fs::FallocateFlags::KEEP_SIZE,
        0,
        owner.metadata().unwrap().len(),
    )
    .unwrap();
    assert_eq!(entry.get("MESSAGE").unwrap(), expected);
}
