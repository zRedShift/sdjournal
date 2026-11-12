use std::{
    fs,
    path::{Path, PathBuf},
};

use sdjournal::{Journal, JournalConfig, MmapPolicy, ScanItem, ScanOptions};

mod fixture {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/support/synthetic_journal.rs"
    ));
}
use fixture::{SyntheticJournalFile, synthetic_message};

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn tear_inside_last_entry(path: &Path) {
    let bytes = fs::read(path).unwrap();
    let array = usize::try_from(read_u64(&bytes, 176)).unwrap();
    let last = read_u64(&bytes, array + 24 + 2 * 8);
    fs::OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .set_len(last + 32)
        .unwrap();
}

fn config(include_journal_tilde: bool) -> JournalConfig {
    JournalConfig {
        include_journal_tilde,
        mmap_policy: MmapPolicy::Never,
        ..JournalConfig::default()
    }
}

fn messages(journal: &Journal) -> Vec<String> {
    journal
        .scan_from_positions(&[], ScanOptions::default())
        .unwrap()
        .filter_map(|item| match item.unwrap() {
            ScanItem::Entry { entry, .. } => {
                Some(String::from_utf8(entry.get("MESSAGE").unwrap().to_vec()).unwrap())
            }
            _ => None,
        })
        .collect()
}

fn torn_fixture(extension: &str) -> (SyntheticJournalFile, PathBuf) {
    let fixture = SyntheticJournalFile::new(&["one.service", "two.service", "three.service"]);
    let source = fixture.root().join("synthetic.journal");
    let target = fixture.root().join(format!("previous.{extension}"));
    fs::rename(source, &target).unwrap();
    tear_inside_last_entry(&target);
    (fixture, target)
}

#[test]
fn disposed_torn_entry_recovers_the_complete_prefix() {
    let (fixture, _) = torn_fixture("journal~");
    let journal =
        Journal::open_progress_dirs(&[fixture.root().to_owned()], config(true), &[]).unwrap();
    assert!(journal.open_errors().is_empty());
    assert_eq!(
        messages(&journal),
        [
            synthetic_message(7, 0, "one.service"),
            synthetic_message(7, 1, "two.service")
        ]
    );
}

#[test]
fn ordinary_online_file_with_the_same_torn_bytes_stays_strict() {
    let (fixture, path) = torn_fixture("journal");
    let journal =
        Journal::open_progress_dirs(&[fixture.root().to_owned()], config(false), &[]).unwrap();
    assert_eq!(journal.open_errors().len(), 1);
    assert_eq!(journal.open_errors()[0].0, path);
    assert!(messages(&journal).is_empty());
}
