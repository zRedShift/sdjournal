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
use fixture::{SyntheticJournalFile, synthetic_message, synthetic_realtime_usec};

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn write_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
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
    positioned_omissions: usize,
    positionless_omissions: usize,
    stopped: Vec<String>,
    replayed: usize,
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
            ScanItem::Omitted {
                position, error, ..
            } => {
                summary.permanent_omissions += usize::from(matches!(
                    error,
                    sdjournal::SdJournalError::PermanentDisposed { .. }
                ));
                if position.is_some() {
                    summary.positioned_omissions += 1;
                } else {
                    summary.positionless_omissions += 1;
                }
            }
            ScanItem::FileTerminated {
                last_ordinal,
                error,
                ..
            } => summary.terminated.push((last_ordinal, error.to_string())),
            ScanItem::FileStopped { error, .. } => summary.stopped.push(error.to_string()),
            ScanItem::Replayed { .. } => summary.replayed += 1,
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
    assert_eq!(summary.positionless_omissions, 1);
}

#[test]
fn disposed_corrupt_data_keeps_its_real_position_and_later_entries() {
    let fixture = SyntheticJournalFile::new(&["one.service", "two.service", "three.service"]);
    let source = fixture.root().join("synthetic.journal");
    let target = fixture.root().join("corrupt-data.journal~");
    fs::rename(source, &target).unwrap();
    let mut bytes = fs::read(&target).unwrap();
    let array = usize::try_from(read_u64(&bytes, 176)).unwrap();
    let second_entry = usize::try_from(read_u64(&bytes, array + 24 + 8)).unwrap();
    let second_data = usize::try_from(read_u64(&bytes, second_entry + 64)).unwrap();
    bytes[second_data] = 0xff;
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
    assert_eq!(summary.positioned_omissions, 1);
    assert!(summary.terminated.is_empty());
}

#[test]
fn disposed_bad_slot_target_does_not_hide_a_later_valid_entry() {
    let fixture = SyntheticJournalFile::new(&["one.service", "two.service", "three.service"]);
    let source = fixture.root().join("synthetic.journal");
    let target = fixture.root().join("bad-slot.journal~");
    fs::rename(source, &target).unwrap();
    let mut bytes = fs::read(&target).unwrap();
    let array = usize::try_from(read_u64(&bytes, 176)).unwrap();
    let first_data = read_u64(&bytes, 272);
    write_u64(&mut bytes, array + 24 + 8, first_data);
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
    assert!(summary.terminated.is_empty());
}

#[test]
fn disposed_prefix_survives_broken_array_topology() {
    for topology in ["short", "nonforward", "malformed-next"] {
        let fixture = SyntheticJournalFile::new(&["one.service", "two.service", "three.service"]);
        let source = fixture.root().join("synthetic.journal");
        let target = fixture.root().join(format!("{topology}.journal~"));
        fs::rename(source, &target).unwrap();
        let mut bytes = fs::read(&target).unwrap();
        bytes[16] = 2;
        let array = usize::try_from(read_u64(&bytes, 176)).unwrap();
        let first_data = read_u64(&bytes, 272);
        write_u64(&mut bytes, array + 8, 24 + 3 * 8);
        write_u64(&mut bytes, 152, 4);
        match topology {
            "short" => {}
            "nonforward" => write_u64(&mut bytes, array + 16, array as u64),
            "malformed-next" => write_u64(&mut bytes, array + 16, first_data),
            _ => unreachable!(),
        }
        fs::write(&target, bytes).unwrap();
        let journal =
            Journal::open_progress_dirs(&[fixture.root().to_owned()], recovery_config(), &[])
                .unwrap();
        let summary = scan(&journal, &[]);
        assert_eq!(
            summary.messages.len(),
            3,
            "{topology} must publish the validated prefix before its terminal notice"
        );
        assert_eq!(summary.terminated.len(), 1, "{topology}");
        assert_eq!(summary.terminated[0].0, 3, "{topology}");
    }
}

#[test]
fn disposed_unused_array_capacity_cut_is_not_reported_as_record_loss() {
    let fixture = SyntheticJournalFile::new(&["one.service", "two.service"]);
    let source = fixture.root().join("synthetic.journal");
    let target = fixture.root().join("unused-capacity.journal~");
    fs::rename(source, &target).unwrap();
    let mut bytes = fs::read(&target).unwrap();
    let old_array = usize::try_from(read_u64(&bytes, 176)).unwrap();
    let offsets = [
        read_u64(&bytes, old_array + 24),
        read_u64(&bytes, old_array + 32),
    ];
    let new_array = bytes.len().next_multiple_of(8);
    bytes.resize(new_array + 24 + offsets.len() * 8, 0);
    bytes[new_array] = 6;
    write_u64(&mut bytes, new_array + 8, 24 + 64 * 8);
    for (index, offset) in offsets.into_iter().enumerate() {
        write_u64(&mut bytes, new_array + 24 + index * 8, offset);
    }
    let new_len = bytes.len();
    write_u64(&mut bytes, 96, (new_len - 272) as u64);
    write_u64(&mut bytes, 136, new_array as u64);
    write_u64(&mut bytes, 176, new_array as u64);
    fs::write(&target, bytes).unwrap();
    let journal =
        Journal::open_progress_dirs(&[fixture.root().to_owned()], recovery_config(), &[]).unwrap();
    let summary = scan(&journal, &[]);
    assert_eq!(summary.messages.len(), 2);
    assert!(summary.terminated.is_empty());
    assert_eq!(summary.permanent_omissions, 0);
}

#[test]
fn post_open_truncation_stays_retryable_for_eager_and_lazy_recovery() {
    for max_open_files in [1, 64] {
        let root = tempfile::tempdir().unwrap();
        for seed in 1..=2 {
            fixture::write_synthetic_journal_file(
                &root.path().join(format!("{seed}.journal~")),
                &["one.service", "two.service"],
                seed,
            );
        }
        let journal = Journal::open_progress_dirs(
            &[root.path().to_owned()],
            JournalConfig {
                recover_disposed_journal_tails: true,
                mmap_policy: MmapPolicy::Never,
                max_open_files,
                ..JournalConfig::default()
            },
            &[],
        )
        .unwrap();
        fs::OpenOptions::new()
            .write(true)
            .open(root.path().join("1.journal~"))
            .unwrap()
            .set_len(272)
            .unwrap();
        let summary = scan(&journal, &[]);
        assert_eq!(summary.stopped.len(), 1, "max_open_files={max_open_files}");
        assert!(
            summary.stopped[0].contains("truncated"),
            "max_open_files={max_open_files}: {:?}",
            summary.stopped
        );
        assert!(
            summary.terminated.is_empty(),
            "max_open_files={max_open_files}"
        );
        assert!(
            !summary.messages.is_empty(),
            "healthy peer must remain readable"
        );
    }
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
fn disposed_header_older_than_body_yields_known_slots_then_reports_the_contradiction() {
    for state in [0, 1, 2] {
        let fixture = SyntheticJournalFile::new(&["one.service", "two.service", "three.service"]);
        let source = fixture.root().join("synthetic.journal");
        let target = fixture.root().join(format!("old-header-{state}.journal~"));
        fs::rename(source, &target).unwrap();
        let mut bytes = fs::read(&target).unwrap();
        bytes[16] = state;
        write_u64(&mut bytes, 152, 2);
        fs::write(&target, bytes).unwrap();
        let journal =
            Journal::open_progress_dirs(&[fixture.root().to_owned()], recovery_config(), &[])
                .unwrap();
        let summary = scan(&journal, &[]);
        assert_eq!(summary.messages.len(), 3, "state={state}");
        assert_eq!(summary.terminated.len(), 1, "state={state}");
        assert_eq!(summary.terminated[0].0, 3, "state={state}");

        let resumed = scan(&journal, &summary.positions[1..2]);
        assert_eq!(
            resumed.messages,
            [synthetic_message(7, 2, "three.service")],
            "state={state}"
        );
        assert_eq!(resumed.terminated.len(), 1, "state={state}");
        assert_eq!(resumed.terminated[0].0, 3, "state={state}");
        assert_eq!(
            resumed.replayed, 0,
            "the exact validated anchor remains authoritative"
        );
    }
}

#[test]
fn disposed_terminal_is_pending_and_resume_reports_the_last_trusted_ordinal() {
    let (fixture, _) = torn_fixture("journal~");
    let journal =
        Journal::open_progress_dirs(&[fixture.root().to_owned()], recovery_config(), &[]).unwrap();
    let mut scan = journal
        .scan_from_positions(&[], ScanOptions::default())
        .unwrap();
    let first = scan.next().unwrap().unwrap();
    let second = scan.next().unwrap().unwrap();
    let ScanItem::Entry { position, .. } = second else {
        panic!("second validated prefix entry")
    };
    assert!(matches!(first, ScanItem::Entry { .. }));
    assert!(
        scan.has_pending_entries(),
        "the terminal loss notice is still pending"
    );
    assert!(matches!(
        scan.next().unwrap().unwrap(),
        ScanItem::FileTerminated {
            last_ordinal: 2,
            ..
        }
    ));

    let mut resumed = journal
        .scan_from_positions(&[position], ScanOptions::default())
        .unwrap();
    assert!(matches!(
        resumed.next().unwrap().unwrap(),
        ScanItem::FileTerminated {
            last_ordinal: 2,
            ..
        }
    ));
}

#[test]
fn checkpoint_beyond_surviving_prefix_replays_without_skipping_positions() {
    let fixture = SyntheticJournalFile::new(&["one.service", "two.service", "three.service"]);
    let source = fixture.root().join("synthetic.journal");
    let target = fixture.root().join("rewound.journal~");
    fs::rename(source, &target).unwrap();
    let intact =
        Journal::open_progress_dirs(&[fixture.root().to_owned()], recovery_config(), &[]).unwrap();
    let original = scan(&intact, &[]);
    let acknowledged = original.positions[2];
    drop(intact);

    let mut bytes = fs::read(&target).unwrap();
    let array = usize::try_from(read_u64(&bytes, 176)).unwrap();
    let second = read_u64(&bytes, array + 24 + 8);
    write_u64(&mut bytes, array + 24 + 16, 0);
    write_u64(&mut bytes, 152, 2);
    write_u64(&mut bytes, 136, second);
    write_u64(&mut bytes, 160, 2);
    write_u64(&mut bytes, 192, synthetic_realtime_usec(7, 1));
    write_u64(&mut bytes, 264, second);
    fs::write(&target, bytes).unwrap();

    let rewound =
        Journal::open_progress_dirs(&[fixture.root().to_owned()], recovery_config(), &[]).unwrap();
    let items = rewound
        .scan_from_positions(&[acknowledged], ScanOptions::default())
        .unwrap()
        .collect::<sdjournal::Result<Vec<_>>>()
        .unwrap();
    assert!(
        items
            .iter()
            .any(|item| matches!(item, ScanItem::Replayed { .. }))
    );
    let positions = items
        .iter()
        .filter_map(|item| match item {
            ScanItem::Entry { position, .. } => Some(position.ordinal),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        positions,
        [1, 2],
        "safe replay may duplicate but cannot skip the surviving prefix"
    );
}

#[test]
fn every_disposed_head_position_resumes_the_exact_remaining_suffix() {
    for state in [0, 1, 2] {
        for declared in 0usize..=4 {
            for tail_follows_declared_count in [false, true] {
                let fixture = SyntheticJournalFile::new(&[
                    "one.service",
                    "two.service",
                    "three.service",
                    "four.service",
                ]);
                let source = fixture.root().join("synthetic.journal");
                let target = fixture.root().join(format!(
                    "matrix-{state}-{declared}-{tail_follows_declared_count}.journal~"
                ));
                fs::rename(source, &target).unwrap();
                let mut bytes = fs::read(&target).unwrap();
                bytes[16] = state;
                write_u64(&mut bytes, 152, declared as u64);
                if tail_follows_declared_count {
                    let array = usize::try_from(read_u64(&bytes, 176)).unwrap();
                    let tail =
                        (declared != 0).then(|| read_u64(&bytes, array + 24 + (declared - 1) * 8));
                    write_u64(&mut bytes, 136, tail.unwrap_or(0));
                    write_u64(&mut bytes, 160, declared as u64);
                    write_u64(
                        &mut bytes,
                        192,
                        tail.map_or(0, |_| {
                            synthetic_realtime_usec(7, declared.saturating_sub(1))
                        }),
                    );
                    write_u64(&mut bytes, 264, tail.unwrap_or(0));
                }
                fs::write(&target, bytes).unwrap();
                let journal = Journal::open_progress_dirs(
                    &[fixture.root().to_owned()],
                    recovery_config(),
                    &[],
                )
                .unwrap();
                let head = scan(&journal, &[]);
                assert_eq!(
                    head.messages.len(),
                    4,
                    "state={state} declared={declared} tail={tail_follows_declared_count}"
                );
                assert_eq!(head.positions.len(), 4);
                for (index, position) in head.positions.iter().copied().enumerate() {
                    let resumed = scan(&journal, &[position]);
                    assert_eq!(
                        resumed.messages,
                        head.messages[index + 1..],
                        "state={state} declared={declared} tail={tail_follows_declared_count} position={}",
                        index + 1
                    );
                    assert_eq!(
                        resumed.positions,
                        head.positions[index + 1..],
                        "state={state} declared={declared} tail={tail_follows_declared_count} position={}",
                        index + 1
                    );
                    assert_eq!(resumed.replayed, 0);
                    assert!(resumed.stopped.is_empty());
                    assert_eq!(resumed.terminated, head.terminated);
                }
            }
        }
    }
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
