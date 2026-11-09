mod support;

use std::{
    fs,
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use sdjournal::{Cursor, Journal, MmapPolicy, SdJournalError};
use support::synthetic_journal::{
    SyntheticJournalFile, synthetic_message, synthetic_realtime_usec, write_synthetic_journal_file,
};

#[test]
fn user_supplied_journal_dir_supports_query_and_cursor_resume() {
    let units = ["alpha.service", "beta.service", "gamma.service"];
    let layout = SyntheticJournalFile::new(&units);
    layout.rewrite(&units);
    let journal = Journal::open_dir(layout.root()).expect("open synthetic journal directory");

    let all = journal
        .query()
        .collect_owned()
        .expect("collect all offline entries");
    assert_eq!(all.len(), 3);

    let mut alpha_query = journal.query();
    alpha_query.match_unit("alpha.service");
    let alpha_entries = alpha_query
        .collect_owned()
        .expect("query alpha entries from offline journal");
    assert_eq!(alpha_entries.len(), 1);
    assert_eq!(
        field(&alpha_entries[0], "MESSAGE"),
        synthetic_message(7, 0, "alpha.service")
    );

    let cursor_text = alpha_entries[0].cursor().expect("entry cursor").to_string();
    let cursor = Cursor::parse(&cursor_text).expect("parse cursor");

    let mut resumed = journal.query();
    resumed.after_cursor(cursor);
    let resumed_entries = resumed.collect_owned().expect("resume after stored cursor");

    assert_eq!(resumed_entries.len(), 2);
    assert_eq!(
        field(&resumed_entries[0], "MESSAGE"),
        synthetic_message(7, 1, "beta.service")
    );
    assert_eq!(
        field(&resumed_entries[1], "MESSAGE"),
        synthetic_message(7, 2, "gamma.service")
    );

    let legacy_cursor = legacy_cursor_from_journal(&layout.root().join("synthetic.journal"), 0);
    let mut legacy_resumed = journal.query();
    legacy_resumed.after_cursor(legacy_cursor);
    let legacy_entries = legacy_resumed
        .collect_owned()
        .expect("resume after legacy SJ1 cursor");
    assert_eq!(messages(&legacy_entries), messages(&resumed_entries));
}

#[test]
fn open_dirs_deduplicates_user_supplied_roots() {
    let layout = SyntheticJournalFile::new(&["alpha.service", "beta.service"]);
    let paths: Vec<PathBuf> = vec![layout.root().to_path_buf(), layout.root().to_path_buf()];

    let journal = Journal::open_dirs(&paths).expect("open duplicated roots");
    let entries = journal
        .query()
        .collect_owned()
        .expect("collect entries from deduplicated roots");

    assert_eq!(entries.len(), 2);
}

#[test]
fn exact_query_returns_all_entries_with_duplicate_data_payloads() {
    let layout = SyntheticJournalFile::new(&["alpha.service", "alpha.service", "alpha.service"]);
    let journal = Journal::open_dir(layout.root()).expect("open synthetic journal directory");

    let mut query = journal.query();
    query.match_unit("alpha.service");
    let entries = query
        .collect_owned()
        .expect("collect repeated alpha entries");

    assert_eq!(entries.len(), 3);
    for (idx, entry) in entries.iter().enumerate() {
        assert_eq!(
            field(entry, "MESSAGE"),
            synthetic_message(7, idx, "alpha.service")
        );
    }
}

#[test]
fn collect_owned_limited_applies_a_hard_result_cap() {
    let layout = SyntheticJournalFile::new(&["alpha.service", "beta.service", "gamma.service"]);
    let journal = Journal::open_dir(layout.root()).expect("open synthetic journal directory");

    let entries = journal
        .query()
        .collect_owned_limited(2)
        .expect("collect at most two entries");
    assert_eq!(entries.len(), 2);

    let mut already_limited = journal.query();
    already_limited.limit(1);
    let entries = already_limited
        .collect_owned_limited(2)
        .expect("keep the lower existing query limit");
    assert_eq!(entries.len(), 1);
}

#[test]
fn low_memory_query_merges_files_and_resumes_cursor() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    write_synthetic_journal_file(
        &layout.root().join("later.journal"),
        &["beta.service", "alpha.service"],
        9,
    );
    let cfg = sdjournal::JournalConfig {
        max_open_files: 1,
        mmap_policy: MmapPolicy::Never,
        ..Default::default()
    };
    let journal =
        Journal::open_dir_with_config(layout.root(), cfg).expect("open low-memory journal");

    let mut alpha = journal.query();
    alpha.match_unit("alpha.service");
    let entries = alpha
        .collect_owned()
        .expect("collect low-memory merged entries");
    assert_eq!(entries.len(), 2);
    assert_eq!(
        field(&entries[0], "MESSAGE"),
        synthetic_message(7, 0, "alpha.service")
    );
    assert_eq!(
        field(&entries[1], "MESSAGE"),
        synthetic_message(9, 1, "alpha.service")
    );

    let cursor = Cursor::parse(&entries[0].cursor().expect("entry cursor").to_string())
        .expect("parse cursor");
    let mut resumed = journal.query();
    resumed.match_unit("alpha.service").after_cursor(cursor);
    let resumed_entries = resumed
        .collect_owned()
        .expect("resume low-memory query after cursor");
    assert_eq!(resumed_entries.len(), 1);
    assert_eq!(
        field(&resumed_entries[0], "MESSAGE"),
        synthetic_message(9, 1, "alpha.service")
    );

    let mut newest = journal.query();
    newest.match_unit("alpha.service").reverse(true);
    let newest_entries = newest.collect_owned().expect("reverse low-memory query");
    assert_eq!(newest_entries.len(), 2);
    assert_eq!(
        field(&newest_entries[0], "MESSAGE"),
        synthetic_message(9, 1, "alpha.service")
    );
}

#[test]
fn low_memory_query_honors_or_groups_limit_and_reverse_order() {
    let layout = SyntheticJournalFile::new(&["alpha.service", "beta.service"]);
    write_synthetic_journal_file(
        &layout.root().join("later.journal"),
        &["gamma.service", "alpha.service"],
        9,
    );
    let cfg = sdjournal::JournalConfig {
        max_open_files: 1,
        mmap_policy: MmapPolicy::Never,
        ..Default::default()
    };
    let journal =
        Journal::open_dir_with_config(layout.root(), cfg).expect("open low-memory journal");

    let mut forward = journal.query();
    forward
        .or_group(|group| {
            group.match_exact("_SYSTEMD_UNIT", b"alpha.service");
        })
        .or_group(|group| {
            group.match_exact("_SYSTEMD_UNIT", b"gamma.service");
        })
        .limit(2);
    let forward_entries = forward
        .collect_owned()
        .expect("collect limited low-memory OR query");
    assert_eq!(forward_entries.len(), 2);
    assert_eq!(
        field(&forward_entries[0], "MESSAGE"),
        synthetic_message(7, 0, "alpha.service")
    );
    assert_eq!(
        field(&forward_entries[1], "MESSAGE"),
        synthetic_message(9, 0, "gamma.service")
    );

    let mut reverse = journal.query();
    reverse
        .or_group(|group| {
            group.match_exact("_SYSTEMD_UNIT", b"alpha.service");
        })
        .or_group(|group| {
            group.match_exact("_SYSTEMD_UNIT", b"gamma.service");
        })
        .reverse(true)
        .limit(2);
    let reverse_entries = reverse
        .collect_owned()
        .expect("collect reverse low-memory OR query");
    assert_eq!(reverse_entries.len(), 2);
    assert_eq!(
        field(&reverse_entries[0], "MESSAGE"),
        synthetic_message(9, 1, "alpha.service")
    );
    assert_eq!(
        field(&reverse_entries[1], "MESSAGE"),
        synthetic_message(9, 0, "gamma.service")
    );
}

#[test]
fn low_memory_query_reports_a_missing_file_instead_of_using_unsafe_time_pruning() {
    let layout = SyntheticJournalFile::new(&["old.service"]);
    let old_path = layout.root().join("synthetic.journal");
    write_synthetic_journal_file(
        &layout.root().join("recent.journal"),
        &["recent.service"],
        9,
    );
    let cfg = sdjournal::JournalConfig {
        max_open_files: 1,
        mmap_policy: MmapPolicy::Never,
        ..Default::default()
    };
    let journal =
        Journal::open_dir_with_config(layout.root(), cfg).expect("open low-memory journal");

    fs::remove_file(&old_path).expect("remove old file after discovery");

    let mut query = journal.query();
    query.since_realtime(synthetic_realtime_usec(9, 0));
    assert!(matches!(
        query.collect_owned(),
        Err(SdJournalError::Retired {
            reason: sdjournal::RetirementReason::Disappeared,
            ..
        })
    ));
}

#[test]
fn low_memory_query_reports_a_missing_file_after_cursor() {
    let layout = SyntheticJournalFile::new(&["old.service"]);
    let old_path = layout.root().join("synthetic.journal");
    write_synthetic_journal_file(
        &layout.root().join("recent.journal"),
        &["recent.service"],
        9,
    );
    let cfg = sdjournal::JournalConfig {
        max_open_files: 1,
        mmap_policy: MmapPolicy::Never,
        ..Default::default()
    };
    let journal =
        Journal::open_dir_with_config(layout.root(), cfg).expect("open low-memory journal");

    let mut old_query = journal.query();
    old_query.match_unit("old.service");
    let old_entries = old_query.collect_owned().expect("read old cursor");
    let cursor = Cursor::parse(&old_entries[0].cursor().expect("old cursor").to_string())
        .expect("parse old cursor");

    fs::remove_file(&old_path).expect("remove old file after cursor capture");

    let mut resumed = journal.query();
    resumed.after_cursor(cursor);
    assert!(matches!(
        resumed.collect_owned(),
        Err(SdJournalError::Retired {
            reason: sdjournal::RetirementReason::Disappeared,
            ..
        })
    ));
}

#[test]
fn query_order_time_bounds_and_cursor_survive_realtime_rollback() {
    let units = ["first.service", "second.service", "third.service"];
    let layout = SyntheticJournalFile::new(&units);
    let path = layout.root().join("synthetic.journal");
    let base = synthetic_realtime_usec(7, 0);
    patch_entry_timestamps(&path, &[base + 300, base + 100, base + 200]);

    let journal = Journal::open_dir(layout.root()).expect("open journal with clock rollback");
    let all = journal
        .query()
        .collect_owned()
        .expect("collect rollback journal");
    assert_eq!(
        messages(&all),
        vec![
            synthetic_message(7, 0, units[0]),
            synthetic_message(7, 1, units[1]),
            synthetic_message(7, 2, units[2]),
        ]
    );

    let mut bounded = journal.query();
    bounded.since_realtime(base + 150);
    let bounded = bounded.collect_owned().expect("apply realtime lower bound");
    assert_eq!(
        messages(&bounded),
        vec![
            synthetic_message(7, 0, units[0]),
            synthetic_message(7, 2, units[2]),
        ]
    );

    let cursor = all[0].cursor().expect("full location cursor");
    let mut resumed = journal.query();
    resumed.after_cursor(cursor);
    let resumed = resumed
        .collect_owned()
        .expect("resume across clock rollback");
    assert_eq!(
        messages(&resumed),
        vec![
            synthetic_message(7, 1, units[1]),
            synthetic_message(7, 2, units[2]),
        ]
    );
}

#[test]
fn indexed_candidates_are_verified_against_entry_payloads() {
    let layout = SyntheticJournalFile::new(&["alpha.service", "beta.service"]);
    let path = layout.root().join("synthetic.journal");
    redirect_data_index_entry(&path, b"_SYSTEMD_UNIT=alpha.service", 1);

    let journal = Journal::open_dir(layout.root()).expect("open journal with damaged index");
    let mut query = journal.query();
    query.match_exact("_SYSTEMD_UNIT", b"alpha.service");
    assert!(matches!(
        query.collect_owned(),
        Err(SdJournalError::Corrupt { reason, .. })
            | Err(SdJournalError::Transient { reason, .. })
            if reason.contains("does not contain the indexed field")
    ));
}

#[test]
fn lazy_reopen_rejects_a_replaced_journal_file() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    write_synthetic_journal_file(&layout.root().join("later.journal"), &["beta.service"], 9);
    let cfg = sdjournal::JournalConfig {
        max_open_files: 1,
        ..Default::default()
    };
    let journal = Journal::open_dir_with_config(layout.root(), cfg).expect("open lazy journal");

    write_synthetic_journal_file(
        &layout.root().join("synthetic.journal"),
        &["replacement.service"],
        11,
    );

    assert!(matches!(
        journal.query().collect_owned(),
        Err(SdJournalError::Retired {
            reason: sdjournal::RetirementReason::Replaced,
            ..
        })
    ));
}

#[test]
fn lazy_index_continuation_does_not_rescan_or_skip_prior_offsets() {
    let units = vec!["alpha.service"; 40];
    let layout = SyntheticJournalFile::new(&units);
    write_synthetic_journal_file(&layout.root().join("later.journal"), &units, 9);
    let cfg = sdjournal::JournalConfig {
        max_open_files: 1,
        mmap_policy: MmapPolicy::Never,
        ..Default::default()
    };
    let journal = Journal::open_dir_with_config(layout.root(), cfg).expect("open lazy journal");

    let mut query = journal.query();
    query.match_exact("_SYSTEMD_UNIT", b"alpha.service");
    let entries = query.collect_owned().expect("collect indexed lazy query");
    assert_eq!(entries.len(), 80);
    assert_eq!(
        field(&entries[39], "MESSAGE"),
        synthetic_message(7, 39, "alpha.service")
    );
    assert_eq!(
        field(&entries[40], "MESSAGE"),
        synthetic_message(9, 0, "alpha.service")
    );

    let mut resumed = journal.query();
    resumed
        .match_exact("_SYSTEMD_UNIT", b"alpha.service")
        .after_cursor(entries[29].cursor().expect("resume cursor"));
    let resumed = resumed.collect_owned().expect("resume indexed lazy query");
    assert_eq!(resumed.len(), 50);
    assert_eq!(
        field(&resumed[0], "MESSAGE"),
        synthetic_message(7, 30, "alpha.service")
    );
}

#[test]
fn low_memory_iterator_is_a_stable_online_snapshot_across_an_in_place_append() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let path = layout.root().join("synthetic.journal");
    write_synthetic_journal_file(&layout.root().join("later.journal"), &["later.service"], 9);
    let cfg = sdjournal::JournalConfig {
        max_open_files: 1,
        mmap_policy: MmapPolicy::Never,
        ..Default::default()
    };
    let old_journal =
        Journal::open_dir_with_config(layout.root(), cfg.clone()).expect("open old lazy snapshot");
    let mut old_iter = old_journal
        .query()
        .iter()
        .expect("create old lazy snapshot iterator");

    #[cfg(unix)]
    let inode_before = {
        use std::os::unix::fs::MetadataExt;
        fs::metadata(&path)
            .expect("stat journal before append")
            .ino()
    };

    append_duplicate_entry_in_place(&path);

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(
            fs::metadata(&path)
                .expect("stat journal after append")
                .ino(),
            inode_before,
            "the fixture must append to the same inode"
        );
    }

    let old_entries = old_iter
        .by_ref()
        .map(|item| item.map(|entry| entry.to_owned()))
        .collect::<Result<Vec<_>, _>>()
        .expect("old snapshot remains readable after append");
    assert_eq!(
        messages(&old_entries),
        vec![
            synthetic_message(7, 0, "alpha.service"),
            synthetic_message(9, 0, "later.service"),
        ],
        "an iterator created before the append must not grow its snapshot"
    );

    let new_journal =
        Journal::open_dir_with_config(layout.root(), cfg).expect("open new lazy snapshot");
    let new_entries = new_journal
        .query()
        .collect_owned()
        .expect("collect the snapshot opened after append");
    assert_eq!(
        messages(&new_entries),
        vec![
            synthetic_message(7, 0, "alpha.service"),
            synthetic_message(7, 0, "alpha.service"),
            synthetic_message(9, 0, "later.service"),
        ],
        "a newly opened snapshot must include the committed append"
    );
}

#[test]
fn entry_array_rejects_an_entry_inside_the_arena_but_past_the_committed_tail() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let path = layout.root().join("synthetic.journal");
    point_entry_array_past_committed_tail(&path);

    let journal = Journal::open_dir(layout.root()).expect("open damaged journal");
    assert!(matches!(
        journal.query().collect_owned(),
        Err(SdJournalError::Corrupt { reason, .. })
            if reason.contains("tail_object_offset")
                || reason.contains("outside committed journal data")
    ));
}

#[test]
fn data_hash_chain_self_loop_falls_back_without_hanging_or_losing_matches() {
    let layout = SyntheticJournalFile::new(&["alpha.service", "beta.service"]);
    let path = layout.root().join("synthetic.journal");
    make_first_data_hash_link_self_referential(&path);

    let journal =
        Journal::open_dir(layout.root()).expect("open journal with cyclic DATA hash link");
    let mut query = journal.query();
    query.match_exact("_SYSTEMD_UNIT", b"alpha.service");
    let entries = query
        .collect_owned()
        .expect("cyclic hash index should safely fall back to scanning");
    assert_eq!(
        messages(&entries),
        vec![synthetic_message(7, 0, "alpha.service")]
    );
}

#[test]
fn exact_cursor_excludes_cross_file_copy_of_the_same_logical_entry() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let original = layout.root().join("synthetic.journal");
    let original_journal = Journal::open_dir(layout.root()).expect("open original journal");
    let cursor = original_journal
        .query()
        .collect_owned()
        .expect("read original entry")[0]
        .cursor()
        .expect("entry cursor");

    let duplicate = layout.root().join("duplicate.journal");
    fs::copy(&original, &duplicate).expect("copy logical entry to a rotated file");
    patch_file_id(&duplicate, [0xa5; 16]);

    let journal = Journal::open_dir(layout.root()).expect("open journal and rotated copy");
    for reverse in [false, true] {
        let mut resumed = journal.query();
        resumed.after_cursor(cursor.clone()).reverse(reverse);
        assert!(
            resumed
                .collect_owned()
                .expect("resume across a copied logical entry")
                .is_empty(),
            "exclusive cursor leaked a cross-file logical duplicate (reverse={reverse})"
        );
    }
}

#[test]
fn conditional_merge_cycle_has_stable_pairwise_order_and_exact_cursor_resume() {
    let layout = SyntheticJournalFile::new(&["a.service"]);
    let a_path = layout.root().join("a.journal");
    fs::rename(layout.root().join("synthetic.journal"), &a_path).expect("rename A journal");
    let b_path = layout.root().join("b.journal");
    let c_path = layout.root().join("c.journal");
    write_synthetic_journal_file(&b_path, &["b.service"], 8);
    write_synthetic_journal_file(&c_path, &["c.service"], 9);

    let stream_one = [0x11; 16];
    let stream_two = [0x22; 16];
    let boot_one = [0x33; 16];
    let boot_two = [0x44; 16];
    patch_single_entry_key(&a_path, stream_one, 1, boot_one, 1, 300);
    patch_single_entry_key(&b_path, stream_one, 2, boot_two, 1, 100);
    patch_single_entry_key(&c_path, stream_two, 1, boot_two, 2, 200);

    // A < B by sequence, B < C by monotonic time, but C < A by realtime.
    let journal = Journal::open_dir(layout.root()).expect("open cyclic-order fixture");
    let forward = journal
        .query()
        .collect_owned()
        .expect("pairwise forward merge");
    assert_eq!(
        messages(&forward),
        vec![
            synthetic_message(9, 0, "c.service"),
            synthetic_message(7, 0, "a.service"),
            synthetic_message(8, 0, "b.service"),
        ]
    );

    let mut reverse_query = journal.query();
    reverse_query.reverse(true);
    let reverse = reverse_query
        .collect_owned()
        .expect("pairwise reverse merge");
    assert_eq!(
        messages(&reverse),
        vec![
            synthetic_message(9, 0, "c.service"),
            synthetic_message(8, 0, "b.service"),
            synthetic_message(7, 0, "a.service"),
        ]
    );

    let a_cursor = forward[1].cursor().expect("A cursor");
    let mut after_a = journal.query();
    after_a.after_cursor(a_cursor.clone());
    assert_eq!(
        messages(
            &after_a
                .collect_owned()
                .expect("forward resume within comparison cycle")
        ),
        vec![synthetic_message(8, 0, "b.service")]
    );

    let mut reverse_after_a = journal.query();
    reverse_after_a.after_cursor(a_cursor).reverse(true);
    assert_eq!(
        messages(
            &reverse_after_a
                .collect_owned()
                .expect("reverse resume within comparison cycle")
        ),
        vec![
            synthetic_message(9, 0, "c.service"),
            synthetic_message(8, 0, "b.service"),
        ]
    );
}

#[cfg(all(target_os = "linux", feature = "mmap"))]
#[test]
fn lazy_merge_heap_does_not_retain_one_mmap_per_file() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    for seed in 8..16 {
        write_synthetic_journal_file(
            &layout.root().join(format!("{seed}.journal")),
            &["alpha.service"],
            seed,
        );
    }
    let cfg = sdjournal::JournalConfig {
        max_open_files: 1,
        mmap_policy: MmapPolicy::Auto,
        ..Default::default()
    };
    let journal =
        Journal::open_dir_with_config(layout.root(), cfg).expect("open lazy mmap journal");
    let query = journal.query();
    let mut iter = query.iter().expect("build lazy iterator");

    assert!(
        mapped_files_under(layout.root()) <= 1,
        "lazy heap retained journal mappings before yielding an entry"
    );
    let entry = iter.next().expect("one item").expect("read one entry");
    assert!(
        mapped_files_under(layout.root()) <= 1,
        "lazy heap retained more mappings than the current entry"
    );
    drop(entry);
}

#[test]
fn open_rejects_zero_max_open_files() {
    let layout = SyntheticJournalFile::new(&["alpha.service"]);
    let cfg = sdjournal::JournalConfig {
        max_open_files: 0,
        ..Default::default()
    };

    match Journal::open_dir_with_config(layout.root(), cfg) {
        Err(sdjournal::SdJournalError::InvalidQuery { reason }) => {
            assert_eq!(reason, "max_open_files must be greater than zero");
        }
        Ok(_) => panic!("expected InvalidQuery"),
        Err(err) => panic!("unexpected error: {err}"),
    }
}

#[cfg(not(target_os = "linux"))]
#[test]
fn open_default_is_explicitly_linux_only() {
    match Journal::open_default() {
        Err(SdJournalError::Unsupported { reason }) => {
            assert!(reason.contains("only supported on Linux"));
        }
        Ok(_) => panic!("expected Unsupported on non-Linux, got Ok"),
        Err(err) => panic!("expected Unsupported on non-Linux, got {err}"),
    }
}

fn field(entry: &sdjournal::EntryOwned, name: &str) -> String {
    String::from_utf8_lossy(entry.get(name).expect("entry field")).into_owned()
}

fn messages(entries: &[sdjournal::EntryOwned]) -> Vec<String> {
    entries
        .iter()
        .map(|entry| field(entry, "MESSAGE"))
        .collect()
}

fn patch_entry_timestamps(path: &std::path::Path, timestamps: &[u64]) {
    let mut bytes = fs::read(path).expect("read synthetic journal");
    let entry_array_offset =
        usize::try_from(read_u64(&bytes, 176)).expect("entry array offset fits usize");
    for (index, realtime_usec) in timestamps.iter().copied().enumerate() {
        let item_offset = entry_array_offset + 24 + (index * 8);
        let entry_offset =
            usize::try_from(read_u64(&bytes, item_offset)).expect("entry offset fits usize");
        write_u64(&mut bytes, entry_offset + 24, realtime_usec);
        write_u64(
            &mut bytes,
            entry_offset + 32,
            realtime_usec.saturating_sub(123),
        );
    }
    fs::write(path, bytes).expect("write patched synthetic journal");
}

fn redirect_data_index_entry(
    path: &std::path::Path,
    target_payload: &[u8],
    replacement_entry_index: usize,
) {
    let mut bytes = fs::read(path).expect("read synthetic journal");
    let entry_array_offset =
        usize::try_from(read_u64(&bytes, 176)).expect("entry array offset fits usize");
    let replacement_entry = read_u64(
        &bytes,
        entry_array_offset + 24 + (replacement_entry_index * 8),
    );
    let hash_table_offset =
        usize::try_from(read_u64(&bytes, 104)).expect("hash table offset fits usize");
    let mut data_offset =
        usize::try_from(read_u64(&bytes, hash_table_offset)).expect("data offset fits usize");

    while data_offset != 0 {
        let size = usize::try_from(read_u64(&bytes, data_offset + 8))
            .expect("DATA object size fits usize");
        if bytes
            .get(data_offset + 64..data_offset + size)
            .is_some_and(|payload| payload == target_payload)
        {
            write_u64(&mut bytes, data_offset + 40, replacement_entry);
            fs::write(path, bytes).expect("write damaged synthetic journal index");
            return;
        }
        data_offset =
            usize::try_from(read_u64(&bytes, data_offset + 24)).expect("next DATA offset fits");
    }

    panic!("target DATA payload was not found");
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("synthetic u64 is in bounds"),
    )
}

fn write_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn append_duplicate_entry_in_place(path: &Path) {
    const HEADER_SIZE: u64 = 272;
    const ENTRY_ARRAY_ITEMS_OFFSET: u64 = 24;

    let bytes = fs::read(path).expect("read journal before append");
    assert_eq!(bytes[16], 1, "fixture must be STATE_ONLINE");
    assert_eq!(read_u64(&bytes, 152), 1, "fixture must have one entry");

    let entry_array_offset = read_u64(&bytes, 176);
    let first_entry_offset = read_u64(
        &bytes,
        usize::try_from(entry_array_offset + ENTRY_ARRAY_ITEMS_OFFSET)
            .expect("entry-array item offset fits"),
    );
    let first_entry_offset_usize =
        usize::try_from(first_entry_offset).expect("entry offset fits usize");
    let entry_size =
        usize::try_from(read_u64(&bytes, first_entry_offset_usize + 8)).expect("entry size fits");
    let entry_end = first_entry_offset_usize
        .checked_add(entry_size)
        .expect("entry range fits");
    let entry_bytes = bytes[first_entry_offset_usize..entry_end].to_vec();

    let new_entry_offset = u64::try_from(bytes.len()).expect("journal length fits u64");
    assert!(new_entry_offset.is_multiple_of(8));
    let old_realtime = read_u64(&entry_bytes, 24);
    let old_monotonic = read_u64(&entry_bytes, 32);
    let mut appended_entry = entry_bytes;
    write_u64(&mut appended_entry, 16, 2);
    write_u64(&mut appended_entry, 24, old_realtime + 100);
    write_u64(&mut appended_entry, 32, old_monotonic + 100);

    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open journal for in-place append");
    file.seek(SeekFrom::End(0))
        .expect("seek to append position");
    file.write_all(&appended_entry)
        .expect("append ENTRY object");

    write_u64_at(
        &mut file,
        entry_array_offset + ENTRY_ARRAY_ITEMS_OFFSET + 8,
        new_entry_offset,
    );
    let appended_len = file.metadata().expect("stat appended journal").len();
    write_u64_at(&mut file, 96, appended_len - HEADER_SIZE);
    write_u64_at(&mut file, 136, new_entry_offset);
    write_u64_at(&mut file, 152, 2);
    write_u64_at(&mut file, 160, 2);
    write_u64_at(&mut file, 192, old_realtime + 100);
    write_u64_at(&mut file, 264, new_entry_offset);
    file.flush().expect("flush appended journal");
}

fn point_entry_array_past_committed_tail(path: &Path) {
    const HEADER_SIZE: u64 = 272;

    let mut bytes = fs::read(path).expect("read synthetic journal");
    let entry_array_offset =
        usize::try_from(read_u64(&bytes, 176)).expect("entry array offset fits");
    let original_entry_offset =
        usize::try_from(read_u64(&bytes, entry_array_offset + 24)).expect("entry offset fits");
    let entry_size =
        usize::try_from(read_u64(&bytes, original_entry_offset + 8)).expect("entry size fits");
    let fake_offset = u64::try_from(bytes.len()).expect("file length fits");
    assert!(fake_offset.is_multiple_of(8));
    let fake_entry = bytes[original_entry_offset..original_entry_offset + entry_size].to_vec();
    bytes.extend_from_slice(&fake_entry);
    bytes[16] = 0; // STATE_OFFLINE: an uncommitted reference cannot be a concurrent writer.
    let extended_len = u64::try_from(bytes.len()).expect("extended arena fits");
    write_u64(&mut bytes, 96, extended_len - HEADER_SIZE);
    write_u64(&mut bytes, entry_array_offset + 24, fake_offset);
    fs::write(path, bytes).expect("write journal with an uncommitted ENTRY reference");
}

fn make_first_data_hash_link_self_referential(path: &Path) {
    let mut bytes = fs::read(path).expect("read synthetic journal");
    let hash_table_offset = usize::try_from(read_u64(&bytes, 104)).expect("hash-table offset fits");
    let first_data_offset =
        usize::try_from(read_u64(&bytes, hash_table_offset)).expect("DATA offset fits");
    write_u64(
        &mut bytes,
        first_data_offset + 24,
        u64::try_from(first_data_offset).expect("DATA offset fits u64"),
    );
    fs::write(path, bytes).expect("write cyclic DATA hash link");
}

fn patch_file_id(path: &Path, file_id: [u8; 16]) {
    let mut bytes = fs::read(path).expect("read journal file ID");
    bytes[24..40].copy_from_slice(&file_id);
    fs::write(path, bytes).expect("write journal file ID");
}

fn patch_single_entry_key(
    path: &Path,
    seqnum_id: [u8; 16],
    seqnum: u64,
    boot_id: [u8; 16],
    monotonic_usec: u64,
    realtime_usec: u64,
) {
    let mut bytes = fs::read(path).expect("read single-entry journal");
    assert_eq!(read_u64(&bytes, 152), 1, "fixture must have one entry");
    let entry_array_offset =
        usize::try_from(read_u64(&bytes, 176)).expect("entry-array offset fits");
    let entry_offset =
        usize::try_from(read_u64(&bytes, entry_array_offset + 24)).expect("entry offset fits");

    bytes[56..72].copy_from_slice(&boot_id);
    bytes[72..88].copy_from_slice(&seqnum_id);
    write_u64(&mut bytes, 160, seqnum);
    write_u64(&mut bytes, 168, seqnum);
    write_u64(&mut bytes, 184, realtime_usec);
    write_u64(&mut bytes, 192, realtime_usec);

    write_u64(&mut bytes, entry_offset + 16, seqnum);
    write_u64(&mut bytes, entry_offset + 24, realtime_usec);
    write_u64(&mut bytes, entry_offset + 32, monotonic_usec);
    bytes[entry_offset + 40..entry_offset + 56].copy_from_slice(&boot_id);
    fs::write(path, bytes).expect("write patched entry key");
}

fn write_u64_at(file: &mut fs::File, offset: u64, value: u64) {
    file.seek(SeekFrom::Start(offset))
        .expect("seek to journal metadata");
    file.write_all(&value.to_le_bytes())
        .expect("patch journal metadata");
}

fn legacy_cursor_from_journal(path: &Path, entry_index: usize) -> Cursor {
    let bytes = fs::read(path).expect("read journal for legacy cursor");
    let entry_array_offset =
        usize::try_from(read_u64(&bytes, 176)).expect("entry array offset fits usize");
    let entry_offset = read_u64(
        &bytes,
        entry_array_offset + 24 + entry_index.saturating_mul(8),
    );
    let entry_offset_usize = usize::try_from(entry_offset).expect("entry offset fits usize");
    let mut legacy = Vec::with_capacity(41);
    legacy.push(4);
    legacy.extend_from_slice(&bytes[24..40]);
    legacy.extend_from_slice(&entry_offset.to_le_bytes());
    legacy.extend_from_slice(&bytes[entry_offset_usize + 16..entry_offset_usize + 24]);
    legacy.extend_from_slice(&bytes[entry_offset_usize + 24..entry_offset_usize + 32]);
    let text = legacy
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Cursor::parse(&format!("SJ1:{text}")).expect("parse legacy SJ1 cursor")
}

#[cfg(all(target_os = "linux", feature = "mmap"))]
fn mapped_files_under(root: &std::path::Path) -> usize {
    let root = root.to_string_lossy();
    fs::read_to_string("/proc/self/maps")
        .expect("read process mappings")
        .lines()
        .filter(|line| line.contains(root.as_ref()) && line.contains(".journal"))
        .count()
}
