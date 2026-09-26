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
        before.len() as u64
    } else {
        0
    };
    let new_entry = (before.len() + tail.len()) as u64;
    if next_array != 0 {
        put(&mut tail, 24, new_entry);
    }
    tail.extend_from_slice(&entry);
    patch(path, 16, &[1]); // OFFLINE -> ONLINE before appending.
    patch(path, before.len() as u64, &tail);
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
    put(&mut header, 96, (before.len() + tail.len() - 272) as u64);
    put(&mut header, 136, new_entry);
    put(&mut header, 152, old_count + 1);
    put(&mut header, 160, seqnum);
    put(&mut header, 192, realtime);
    put(&mut header, 264, new_entry);
    patch(path, 0, &header);
    let after = fs::read(path).unwrap();
    assert_eq!(fs::metadata(path).unwrap().ino(), inode);
    assert_eq!(&after[old_entry..before.len()], &before[old_entry..]);
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
