#![cfg(unix)]

mod support;

use sdjournal::{Journal, JournalConfig, MmapPolicy, SdJournalError};
use std::fs::{self, OpenOptions};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::Path;
use support::synthetic_journal::{SyntheticJournalFile, synthetic_message};

fn number(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn put(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn patch(path: &Path, offset: u64, bytes: &[u8]) {
    OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .write_all_at(bytes, offset)
        .unwrap();
}

fn config(policy: MmapPolicy) -> JournalConfig {
    JournalConfig {
        mmap_policy: policy,
        ..Default::default()
    }
}

fn messages(journal: &Journal) -> sdjournal::Result<Vec<String>> {
    journal
        .query()
        .iter()?
        .map(|entry| {
            let entry = entry?;
            Ok(std::str::from_utf8(entry.get("MESSAGE").unwrap())
                .unwrap()
                .to_owned())
        })
        .collect()
}

// Publish one complete append to the same inode. Existing DATA/ENTRY objects
// remain byte-identical; only the final array slot/link and header are changed.
// Duplicate DATA references suffice for unfiltered public-API snapshot queries.
fn append(path: &Path, completed_offline: bool) -> (u64, u64) {
    let before = fs::read(path).unwrap();
    let inode = fs::metadata(path).unwrap().ino();
    let old_count = number(&before, 152);
    let array = number(&before, 176) as usize;
    let capacity = (number(&before, array + 8) - 24) / 8;
    assert!(old_count > 0 && old_count <= capacity);
    let old_entry = number(&before, array + 24 + (old_count as usize - 1) * 8) as usize;
    let size = number(&before, old_entry + 8) as usize;
    let append_at = (old_entry + size).next_multiple_of(8);
    let mut entry = before[old_entry..old_entry + size].to_vec();
    let seqnum = number(&entry, 16) + 1;
    let realtime = number(&entry, 24) + 100;
    let monotonic = number(&entry, 32) + 100;
    put(&mut entry, 16, seqnum);
    put(&mut entry, 24, realtime);
    put(&mut entry, 32, monotonic);
    let mut tail = Vec::new();
    let next_array = if old_count == capacity {
        tail.resize(32, 0);
        tail[0] = 6; // ENTRY_ARRAY with one item.
        put(&mut tail, 8, 32);
        append_at as u64
    } else {
        0
    };
    let new_entry = (append_at + tail.len()) as u64;
    if next_array != 0 {
        put(&mut tail, 24, new_entry);
    }
    tail.extend_from_slice(&entry);
    patch(path, 16, &[1]); // OFFLINE -> ONLINE before appending.
    patch(path, append_at as u64, &tail);
    if next_array == 0 {
        patch(
            path,
            (array + 24) as u64 + old_count * 8,
            &new_entry.to_le_bytes(),
        );
    } else {
        patch(path, (array + 16) as u64, &next_array.to_le_bytes());
    }
    let mut header = before[..272].to_vec();
    header[16] = u8::from(!completed_offline);
    put(
        &mut header,
        96,
        (before.len().max(append_at + tail.len()) - 272) as u64,
    );
    put(&mut header, 136, new_entry);
    put(&mut header, 152, old_count + 1);
    put(&mut header, 160, seqnum);
    put(&mut header, 192, realtime);
    put(&mut header, 264, new_entry);
    patch(path, 0, &header);
    let after = fs::read(path).unwrap();
    assert_eq!(fs::metadata(path).unwrap().ino(), inode);
    assert_eq!(&after[old_entry..append_at], &before[old_entry..append_at]);
    (new_entry, next_array)
}

fn retained_prefix_case(initial_online: bool, completed_offline: bool, count: usize) {
    for policy in [MmapPolicy::Never, MmapPolicy::Auto] {
        let units: Vec<_> = (0..count).map(|n| format!("unit-{n}.service")).collect();
        let fixture = SyntheticJournalFile::new(&units);
        fixture.rewrite(&units);
        let path = fixture.root().join("synthetic.journal");
        patch(&path, 16, &[u8::from(initial_online)]);
        let old = Journal::open_dir_with_config(fixture.root(), config(policy)).unwrap();
        append(&path, completed_offline);
        let fresh = Journal::open_dir_with_config(fixture.root(), config(policy)).unwrap();
        let expected: Vec<_> = units
            .iter()
            .enumerate()
            .map(|(n, unit)| synthetic_message(7, n, unit))
            .collect();
        let mut after = expected.clone();
        after.push(expected.last().unwrap().clone());
        assert_eq!(messages(&fresh).unwrap(), after, "valid completed append");
        assert_eq!(messages(&old).unwrap(), expected, "frozen count and order");
        for reverse in [false, true] {
            let mut query = old.query();
            query.match_present("MESSAGE").reverse(reverse);
            let actual: Vec<_> = query
                .collect_owned()
                .unwrap()
                .iter()
                .map(|entry| {
                    std::str::from_utf8(entry.get("MESSAGE").unwrap())
                        .unwrap()
                        .to_owned()
                })
                .collect();
            let mut ordered = expected.clone();
            if reverse {
                ordered.reverse();
            }
            assert_eq!(actual, ordered, "filtered/reverse frozen order");
        }
    }
}

#[test]
fn snapshot_growth_offline_to_online_preserves_old_prefix() {
    retained_prefix_case(false, false, 2);
}

#[test]
fn snapshot_growth_offline_to_online_to_offline_preserves_old_prefix() {
    retained_prefix_case(false, true, 2);
}

#[test]
fn snapshot_growth_new_array_preserves_original_full_array() {
    retained_prefix_case(false, true, 64);
}

#[test]
fn snapshot_growth_already_online_keeps_existing_snapshot_semantics() {
    retained_prefix_case(true, true, 2);
}

#[test]
fn snapshot_growth_unchanged_offline_and_genuine_prefix_corruption() {
    for policy in [MmapPolicy::Never, MmapPolicy::Auto] {
        let fixture = SyntheticJournalFile::new(&["first.service", "second.service"]);
        let path = fixture.root().join("synthetic.journal");
        patch(&path, 16, &[0]);
        let old = Journal::open_dir_with_config(fixture.root(), config(policy)).unwrap();
        assert_eq!(messages(&old).unwrap().len(), 2);
        append(&path, true);
        patch(&path, 288 + 24, &0u64.to_le_bytes());
        assert!(matches!(
            messages(&old),
            Err(SdJournalError::Corrupt { .. })
        ));
    }
}

#[test]
fn snapshot_growth_never_excuses_a_back_reference_in_the_unused_slot() {
    for policy in [MmapPolicy::Never, MmapPolicy::Auto] {
        let fixture = SyntheticJournalFile::new(&["first.service", "second.service"]);
        let path = fixture.root().join("synthetic.journal");
        patch(&path, 16, &[0]);
        let before = fs::read(&path).unwrap();
        let old = Journal::open_dir_with_config(fixture.root(), config(policy)).unwrap();
        append(&path, true);
        patch(&path, 288 + 24 + 16, &before[288 + 24..288 + 32]);
        assert!(matches!(
            messages(&old),
            Err(SdJournalError::Corrupt { .. })
        ));
    }
}

#[test]
fn snapshot_growth_requires_same_file_identity_and_coherent_growth() {
    // Every case has a legitimate append first. None may use that fact to
    // excuse unrelated replacement, regressed/uncommitted metadata or layout.
    for (offset, value) in [
        (24, 99),        // file ID
        (72, 99),        // sequence ID
        (12, 1),         // format flags
        (88, 280),       // header layout
        (96, 8),         // arena regression
        (96, u64::MAX),  // arena arithmetic overflow
        (136, 288),      // tail regression
        (152, 2),        // entry count did not advance
        (152, u64::MAX), // count not supported by sequence progression
        (160, 1),        // sequence regression
        (176, 296),      // root ENTRY_ARRAY identity
    ] {
        let fixture = SyntheticJournalFile::new(&["first.service", "second.service"]);
        let path = fixture.root().join("synthetic.journal");
        patch(&path, 16, &[0]);
        let old = Journal::open_dir_with_config(fixture.root(), config(MmapPolicy::Never)).unwrap();
        append(&path, true);
        patch(&path, offset, &value.to_le_bytes());
        assert!(
            messages(&old).is_err(),
            "unproven growth at header offset {offset}"
        );
    }
}

#[test]
fn snapshot_growth_replacement_missing_file_and_truncation_fail_closed() {
    for mode in ["replacement", "missing", "truncated"] {
        for policy in [MmapPolicy::Never, MmapPolicy::Auto] {
            let fixture = SyntheticJournalFile::new(&["first.service", "second.service"]);
            let path = fixture.root().join("synthetic.journal");
            patch(&path, 16, &[0]);
            let before = fs::metadata(&path).unwrap();
            let old = Journal::open_dir_with_config(fixture.root(), config(policy)).unwrap();
            append(&path, true);
            match mode {
                "replacement" => {
                    let replacement = fixture.root().join("replacement.bin");
                    fs::copy(&path, &replacement).unwrap();
                    fs::rename(replacement, &path).unwrap();
                    assert_ne!(fs::metadata(&path).unwrap().ino(), before.ino());
                }
                "missing" => fs::rename(&path, fixture.root().join("removed.bin")).unwrap(),
                "truncated" => OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .unwrap()
                    .set_len(before.len() - 8)
                    .unwrap(),
                _ => unreachable!(),
            }
            assert!(messages(&old).is_err(), "must refuse {mode}");
        }
    }
}

#[test]
fn snapshot_growth_never_reopens_archived_or_accepts_invalid_suffix_objects() {
    for mode in [
        "archived",
        "oversize",
        "wrong-kind",
        "uncommitted-pointer",
        "bad-link",
    ] {
        let fixture = SyntheticJournalFile::new(&["first.service", "second.service"]);
        let path = fixture.root().join("synthetic.journal");
        patch(&path, 16, &[if mode == "archived" { 2 } else { 0 }]);
        let mut cfg = config(MmapPolicy::Never);
        cfg.max_object_size_bytes = 1024;
        let old = Journal::open_dir_with_config(fixture.root(), cfg).unwrap();
        let (new_entry, _) = append(&path, true);
        match mode {
            "oversize" => patch(&path, new_entry + 8, &u64::MAX.to_le_bytes()),
            "wrong-kind" => patch(&path, new_entry, &[6]),
            "uncommitted-pointer" => patch(&path, 288 + 24 + 16, &(new_entry + 8).to_le_bytes()),
            // A partly used array cannot have a committed successor until all
            // spare slots are represented by the fresh entry count.
            "bad-link" => patch(&path, 288 + 16, &new_entry.to_le_bytes()),
            "archived" => (),
            _ => unreachable!(),
        }
        assert!(messages(&old).is_err(), "must refuse {mode}");
    }
}

#[test]
fn snapshot_growth_low_memory_reopen_keeps_old_prefix() {
    let fixture = SyntheticJournalFile::new(&["first.service", "second.service"]);
    let path = fixture.root().join("synthetic.journal");
    patch(&path, 16, &[0]);
    support::synthetic_journal::write_synthetic_journal_file(
        &fixture.root().join("later.journal"),
        &["later.service"],
        9,
    );
    let mut cfg = config(MmapPolicy::Never);
    cfg.max_open_files = 1;
    let old = Journal::open_dir_with_config(fixture.root(), cfg.clone()).unwrap();
    // Low-memory Journal defers file opening until query creation. Pin that
    // actual iterator before append, not a later query of the same Journal.
    let old_query = old.query();
    let old_iter = old_query.iter().unwrap();
    append(&path, true);
    let actual: Vec<_> = old_iter
        .map(|entry| {
            let entry = entry.unwrap();
            std::str::from_utf8(entry.get("MESSAGE").unwrap())
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(
        actual,
        [
            synthetic_message(7, 0, "first.service"),
            synthetic_message(7, 1, "second.service"),
            synthetic_message(9, 0, "later.service"),
        ]
    );
    let fresh = Journal::open_dir_with_config(fixture.root(), cfg).unwrap();
    assert_eq!(messages(&fresh).unwrap().len(), 4);
}

#[test]
fn snapshot_growth_within_preallocated_arena_keeps_original_size_limit() {
    for policy in [MmapPolicy::Never, MmapPolicy::Auto] {
        let fixture = SyntheticJournalFile::new(&["first.service", "second.service"]);
        let path = fixture.root().join("synthetic.journal");
        let reserved_len = fs::metadata(&path).unwrap().len() + 4096;
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(reserved_len)
            .unwrap();
        patch(&path, 96, &(reserved_len - 272).to_le_bytes());
        patch(&path, 16, &[0]);
        let old = Journal::open_dir_with_config(fixture.root(), config(policy)).unwrap();
        append(&path, true);
        assert_eq!(fs::metadata(&path).unwrap().len(), reserved_len);
        assert_eq!(messages(&old).unwrap().len(), 2);
        let fresh = Journal::open_dir_with_config(fixture.root(), config(policy)).unwrap();
        assert_eq!(messages(&fresh).unwrap().len(), 3);
    }
}

#[test]
fn snapshot_growth_new_array_cannot_point_into_original_prefix() {
    let fixture = SyntheticJournalFile::new(&vec!["first.service"; 64]);
    let path = fixture.root().join("synthetic.journal");
    patch(&path, 16, &[0]);
    let old = Journal::open_dir_with_config(fixture.root(), config(MmapPolicy::Never)).unwrap();
    append(&path, true);
    // Forward of the original array, but still inside old committed data.
    patch(&path, 288 + 16, &824u64.to_le_bytes());
    assert!(matches!(
        messages(&old),
        Err(SdJournalError::Corrupt { .. })
    ));
}
