use std::{
    fs,
    path::{Path, PathBuf},
};

use sdjournal::{FilePosition, Journal, JournalConfig, MmapPolicy, ScanItem, ScanOptions};

#[allow(dead_code)]
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

fn recovery_config() -> JournalConfig {
    JournalConfig {
        recover_disposed_journal_tails: true,
        mmap_policy: MmapPolicy::Never,
        ..JournalConfig::default()
    }
}

#[derive(Default)]
struct Summary {
    messages: Vec<String>,
    positions: Vec<FilePosition>,
    terminated: Vec<(u64, String)>,
    permanent_omissions: usize,
}

fn scan(journal: &Journal, positions: &[FilePosition]) -> Summary {
    let mut summary = Summary::default();
    for item in journal
        .scan_from_positions(positions, ScanOptions::default())
        .unwrap()
    {
        match item.unwrap() {
            ScanItem::Entry {
                entry, position, ..
            } => {
                summary
                    .messages
                    .push(String::from_utf8(entry.get("MESSAGE").unwrap().to_vec()).unwrap());
                summary.positions.push(position);
            }
            ScanItem::Omitted { error, .. } => {
                summary.permanent_omissions += usize::from(matches!(
                    error,
                    sdjournal::SdJournalError::PermanentDisposed { .. }
                ));
            }
            ScanItem::FileTerminated {
                last_ordinal,
                error,
                ..
            } => summary.terminated.push((last_ordinal, error.to_string())),
            ScanItem::FileStopped { error, .. } => panic!("unexpected retryable stop: {error}"),
            ScanItem::Replayed { .. } => {}
        }
    }
    summary
}

fn messages(journal: &Journal) -> Vec<String> {
    scan(journal, &[]).messages
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
        Journal::open_progress_dirs(&[fixture.root().to_owned()], recovery_config(), &[]).unwrap();
    assert!(journal.open_errors().is_empty());
    let summary = scan(&journal, &[]);
    assert_eq!(
        summary.messages,
        [
            synthetic_message(7, 0, "one.service"),
            synthetic_message(7, 1, "two.service")
        ]
    );
    assert_eq!(summary.terminated.len(), 1);
    assert_eq!(summary.terminated[0].0, 2);
    assert!(
        summary.terminated[0]
            .1
            .contains("permanent disposed journal loss")
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

#[test]
fn strict_tilde_mode_keeps_the_existing_torn_file_failure() {
    let (fixture, path) = torn_fixture("journal~");
    let journal =
        Journal::open_progress_dirs(&[fixture.root().to_owned()], config(true), &[]).unwrap();
    assert_eq!(journal.open_errors().len(), 1);
    assert_eq!(journal.open_errors()[0].0, path);
}

#[test]
fn intact_disposed_files_recover_all_raw_header_states_and_resume_suffixes() {
    for state in [0, 1, 2] {
        let fixture = SyntheticJournalFile::new(&["one.service", "two.service", "three.service"]);
        let source = fixture.root().join("synthetic.journal");
        let target = fixture.root().join(format!("state-{state}.journal~"));
        fs::rename(source, &target).unwrap();
        let mut bytes = fs::read(&target).unwrap();
        bytes[16] = state;
        fs::write(&target, bytes).unwrap();
        let journal =
            Journal::open_progress_dirs(&[fixture.root().to_owned()], recovery_config(), &[])
                .unwrap();
        let all = scan(&journal, &[]);
        assert_eq!(all.messages.len(), 3);
        assert!(all.terminated.is_empty());
        let suffix = scan(&journal, &all.positions[1..2]);
        assert_eq!(suffix.messages, [synthetic_message(7, 2, "three.service")]);
        let empty = scan(&journal, &all.positions[2..]);
        assert!(empty.messages.is_empty());
    }
}

#[test]
fn disposed_arena_padding_truncation_keeps_the_complete_committed_tail() {
    let fixture = SyntheticJournalFile::new(&["one.service", "two.service"]);
    let source = fixture.root().join("synthetic.journal");
    let target = fixture.root().join("padding.journal~");
    fs::rename(source, &target).unwrap();
    let mut bytes = fs::read(&target).unwrap();
    let arena = read_u64(&bytes, 96);
    bytes[96..104].copy_from_slice(&(arena + 4096).to_le_bytes());
    fs::write(&target, bytes).unwrap();
    let journal =
        Journal::open_progress_dirs(&[fixture.root().to_owned()], recovery_config(), &[]).unwrap();
    let summary = scan(&journal, &[]);
    assert_eq!(summary.messages.len(), 2);
    assert!(summary.terminated.is_empty());
}

#[test]
fn disposed_non_tail_entry_corruption_is_reported_while_later_slots_continue() {
    let fixture = SyntheticJournalFile::new(&["one.service", "two.service", "three.service"]);
    let source = fixture.root().join("synthetic.journal");
    let target = fixture.root().join("corrupt-entry.journal~");
    fs::rename(source, &target).unwrap();
    let mut bytes = fs::read(&target).unwrap();
    let array = usize::try_from(read_u64(&bytes, 176)).unwrap();
    let second = usize::try_from(read_u64(&bytes, array + 24 + 8)).unwrap();
    bytes[second] = 0xff;
    fs::write(&target, bytes).unwrap();
    let journal =
        Journal::open_progress_dirs(&[fixture.root().to_owned()], recovery_config(), &[]).unwrap();
    let summary = scan(&journal, &[]);
    assert_eq!(
        summary.messages,
        [
            synthetic_message(7, 0, "one.service"),
            synthetic_message(7, 2, "three.service")
        ]
    );
    assert_eq!(summary.permanent_omissions, 1);
}

#[test]
fn header_claimed_zero_frontier_is_visible_permanent_loss_after_prefix() {
    let fixture = SyntheticJournalFile::new(&["one.service", "two.service", "three.service"]);
    let source = fixture.root().join("synthetic.journal");
    let target = fixture.root().join("zero-frontier.journal~");
    fs::rename(source, &target).unwrap();
    let mut bytes = fs::read(&target).unwrap();
    bytes[152..160].copy_from_slice(&4u64.to_le_bytes());
    fs::write(&target, bytes).unwrap();
    let journal =
        Journal::open_progress_dirs(&[fixture.root().to_owned()], recovery_config(), &[]).unwrap();
    let summary = scan(&journal, &[]);
    assert_eq!(summary.messages.len(), 3);
    assert_eq!(summary.terminated.len(), 1);
    assert_eq!(summary.terminated[0].0, 3);
}

#[test]
fn short_disposed_header_is_explicit_permanent_open_loss_with_physical_identity() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("short.journal~");
    fs::write(&path, [0u8; 64]).unwrap();
    let journal =
        Journal::open_progress_dirs(&[root.path().to_owned()], recovery_config(), &[]).unwrap();
    let [(actual, error)] = journal.open_errors() else {
        panic!("one permanent open loss")
    };
    assert_eq!(actual, &path);
    assert!(matches!(
        error,
        sdjournal::SdJournalError::PermanentDisposed { .. }
    ));
    let identity = sdjournal::FileIdentity::observe(actual);
    assert!(identity.file_id.is_none());
    assert!(identity.device.is_some() && identity.inode.is_some());
}

#[test]
fn disposed_recovery_survives_lazy_reopen_above_the_fd_budget() {
    let root = tempfile::tempdir().unwrap();
    for (index, unit) in ["one.service", "two.service", "three.service"]
        .into_iter()
        .enumerate()
    {
        fixture::write_synthetic_journal_file(
            &root.path().join(format!("{index}.journal~")),
            &[unit],
            7 + index as u8,
        );
    }
    let mut config = recovery_config();
    config.max_open_files = 1;
    let journal = Journal::open_progress_dirs(&[root.path().to_owned()], config, &[]).unwrap();
    let summary = scan(&journal, &[]);
    assert_eq!(summary.messages.len(), 3);
    assert!(summary.terminated.is_empty());
}
