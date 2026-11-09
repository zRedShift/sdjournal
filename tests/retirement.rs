#![cfg(target_os = "linux")]
#[allow(dead_code)]
mod support;

use std::{
    fs::{self, OpenOptions},
    os::unix::fs::MetadataExt,
    path::Path,
};

use sdjournal::{Cursor, Journal, JournalConfig, MmapPolicy, SdJournalError};
use support::synthetic_journal::{
    SyntheticJournalFile, synthetic_message, write_synthetic_journal_file,
};

fn retire(path: &Path, unlink: bool) {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let before = file.metadata().unwrap();
    if unlink {
        fs::remove_file(path).unwrap();
    }
    rustix::fs::fallocate(
        &file,
        rustix::fs::FallocateFlags::PUNCH_HOLE | rustix::fs::FallocateFlags::KEEP_SIZE,
        0,
        before.len(),
    )
    .unwrap();
    let after = file.metadata().unwrap();
    assert_eq!(after.len(), before.len());
    assert_eq!(after.nlink(), u64::from(!unlink));
}

fn config(policy: MmapPolicy, budget: usize) -> JournalConfig {
    JournalConfig {
        mmap_policy: policy,
        max_open_files: budget,
        ..Default::default()
    }
}

fn retired(error: sdjournal::SdJournalError) {
    assert!(
        matches!(error, sdjournal::SdJournalError::Retired { .. }),
        "{error}"
    );
}

#[test]
fn linked_truncation_uses_explicit_io_without_mmap_or_false_empty() {
    for budget in [1, 64] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("a.journal");
        write_synthetic_journal_file(&path, &["a.service"], 7);
        write_synthetic_journal_file(&root.path().join("b.journal"), &["b.service"], 8);
        let journal =
            Journal::open_dir_with_config(root.path(), config(MmapPolicy::Never, budget)).unwrap();
        let mut iter = journal.query().iter().unwrap();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(0)
            .unwrap();
        assert!(matches!(
            iter.next().unwrap(),
            Err(sdjournal::SdJournalError::Transient { .. })
        ));
    }
}

#[test]
fn vacuumed_snapshot_is_retirement_in_eager_and_lazy_io() {
    for budget in [1, 64] {
        let root = tempfile::tempdir().unwrap();
        let victim = root.path().join("a.journal");
        write_synthetic_journal_file(&victim, &["a.service", "b.service"], 7);
        write_synthetic_journal_file(&root.path().join("b.journal"), &["healthy.service"], 8);
        let journal =
            Journal::open_dir_with_config(root.path(), config(MmapPolicy::Never, budget)).unwrap();
        let mut iter = journal.query().iter().unwrap();
        retire(&victim, true);
        retired(iter.next().unwrap().unwrap_err());
        assert_eq!(
            Journal::open_dir(root.path())
                .unwrap()
                .query()
                .collect_owned()
                .unwrap()
                .len(),
            1
        );
    }
}

#[test]
fn vacuum_during_query_construction_does_not_report_corruption() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("a.journal");
    write_synthetic_journal_file(&path, &["a.service"], 7);
    let journal =
        Journal::open_dir_with_config(root.path(), config(MmapPolicy::Never, 64)).unwrap();
    retire(&path, true);
    match journal.query().iter() {
        Err(error) => retired(error),
        Ok(mut iter) => retired(iter.next().unwrap().unwrap_err()),
    }
}

#[test]
fn apparent_eof_still_validates_empty_snapshot_owner() {
    for budget in [1, 64] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("a.journal");
        write_synthetic_journal_file(&path, &[] as &[&str], 7);
        {
            use std::os::unix::fs::FileExt;
            OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap()
                .write_all_at(&0u64.to_le_bytes(), 176)
                .unwrap();
        }
        let peer = root.path().join("b.journal");
        write_synthetic_journal_file(&peer, &[] as &[&str], 8);
        {
            use std::os::unix::fs::FileExt;
            OpenOptions::new()
                .write(true)
                .open(&peer)
                .unwrap()
                .write_all_at(&0u64.to_le_bytes(), 176)
                .unwrap();
        }
        let journal =
            Journal::open_dir_with_config(root.path(), config(MmapPolicy::Never, budget)).unwrap();
        let mut iter = journal.query().iter().unwrap();
        retire(&path, true);
        retired(
            iter.next()
                .expect("retired empty file must not become a clean no-logs result")
                .unwrap_err(),
        );
    }
}

#[test]
fn retirement_after_last_entry_is_not_clean_eof() {
    for budget in [1, 64] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("a.journal");
        write_synthetic_journal_file(&path, &["a.service"], 7);
        write_synthetic_journal_file(&root.path().join("b.journal"), &["b.service"], 8);
        let journal =
            Journal::open_dir_with_config(root.path(), config(MmapPolicy::Never, budget)).unwrap();
        let mut iter = journal.query().iter().unwrap();
        assert!(iter.next().unwrap().is_ok());
        assert!(iter.next().unwrap().is_ok());
        retire(&path, true);
        retired(
            iter.next()
                .expect("validate before publishing EOF")
                .unwrap_err(),
        );
        assert!(iter.next().is_none(), "EOF retirement is reported once");
    }
}

#[test]
fn published_entry_payload_and_cursor_survive_later_vacuum() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("a.journal");
    write_synthetic_journal_file(&path, &["a.service"], 7);
    {
        use std::os::unix::fs::FileExt;
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .write_all_at(&[2], 16)
            .unwrap();
    }
    let journal = Journal::open_dir(root.path()).unwrap();
    let entry = journal.query().iter().unwrap().next().unwrap().unwrap();
    let cursor = entry.cursor().unwrap().to_string();
    let fields: Vec<_> = entry
        .iter_fields()
        .map(|(k, v)| (k.to_owned(), v.to_vec()))
        .collect();
    retire(&path, true);
    assert_eq!(entry.cursor().unwrap().to_string(), cursor);
    assert_eq!(
        entry
            .iter_fields()
            .map(|(k, v)| (k.to_owned(), v.to_vec()))
            .collect::<Vec<_>>(),
        fields
    );
}

#[test]
fn bare_unlink_is_detected_even_without_zeroed_bytes() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("a.journal");
    write_synthetic_journal_file(&path, &["a.service"], 7);
    let journal =
        Journal::open_dir_with_config(root.path(), config(MmapPolicy::Never, 64)).unwrap();
    let mut iter = journal.query().iter().unwrap();
    fs::remove_file(path).unwrap();
    retired(iter.next().unwrap().unwrap_err());
}

#[test]
fn linked_zeroes_are_not_retirement() {
    for budget in [1, 64] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("a.journal");
        write_synthetic_journal_file(&path, &["a.service"], 7);
        write_synthetic_journal_file(&root.path().join("b.journal"), &["b.service"], 8);
        let journal =
            Journal::open_dir_with_config(root.path(), config(MmapPolicy::Never, budget)).unwrap();
        // Keep the file header/identity valid but corrupt the indexed object.
        {
            use std::os::unix::fs::FileExt;
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let mut offset = [0; 8];
            file.read_exact_at(&mut offset, 176).unwrap();
            file.write_all_at(&[0; 24], u64::from_le_bytes(offset))
                .unwrap();
        }
        let error = match journal.query().iter() {
            Err(error) => error,
            Ok(mut iter) => iter.next().unwrap().unwrap_err(),
        };
        assert!(
            matches!(error, sdjournal::SdJournalError::Corrupt { .. }),
            "{error}"
        );
    }
}

#[test]
fn completely_zeroed_linked_header_is_not_retirement() {
    for budget in [1, 64] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("a.journal");
        write_synthetic_journal_file(&path, &["a.service"], 7);
        write_synthetic_journal_file(&root.path().join("b.journal"), &["b.service"], 8);
        let journal =
            Journal::open_dir_with_config(root.path(), config(MmapPolicy::Never, budget)).unwrap();
        retire(&path, false);
        let error = match journal.query().iter() {
            Err(e) => e,
            Ok(mut it) => it.next().unwrap().unwrap_err(),
        };
        assert!(
            matches!(
                error,
                sdjournal::SdJournalError::Corrupt { .. }
                    | sdjournal::SdJournalError::Unsupported { .. }
            ),
            "{error}"
        );
    }
}

#[test]
fn lazy_replacement_is_retirement_not_linked_corruption() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("a.journal");
    write_synthetic_journal_file(&path, &["a.service"], 7);
    write_synthetic_journal_file(&root.path().join("b.journal"), &["b.service"], 8);
    let journal = Journal::open_dir_with_config(root.path(), config(MmapPolicy::Never, 1)).unwrap();
    let mut iter = journal.query().iter().unwrap();
    let replacement = root.path().join("replacement");
    write_synthetic_journal_file(&replacement, &["new.service"], 9);
    fs::rename(replacement, path).unwrap();
    retired(iter.next().unwrap().unwrap_err());
}

fn pending_corruption_case(later_healthy: bool) {
    use std::os::unix::fs::FileExt as _;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("a.journal");
    write_synthetic_journal_file(&path, &["a.service"; 3], 7);
    if later_healthy {
        write_synthetic_journal_file(&root.path().join("b.journal"), &["b.service"], 9);
    }
    let journal =
        Journal::open_dir_with_config(root.path(), config(MmapPolicy::Never, 64)).unwrap();
    let entries = journal.query().collect_owned().unwrap();
    let mut query = journal.query();
    query.after_cursor(entries[1].cursor().unwrap());
    let mut iter = query.iter().unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut offset = [0; 8];
    file.read_exact_at(&mut offset, 176).unwrap();
    let array = u64::from_le_bytes(offset);
    file.read_exact_at(&mut offset, array + 24 + 2 * 8).unwrap();
    file.write_all_at(&[0], u64::from_le_bytes(offset)).unwrap();
    let item = iter.next();
    assert!(
        matches!(item, Some(Err(sdjournal::SdJournalError::Corrupt { .. }))),
        "linked third-entry corruption before later_healthy={later_healthy}: {item:?}"
    );
}

#[test]
fn pending_corruption_cannot_be_hidden_at_cursor_eof() {
    pending_corruption_case(false);
}

#[test]
fn pending_corruption_cannot_be_hidden_before_later_healthy_rows() {
    pending_corruption_case(true);
}

#[test]
fn initial_bad_file_has_path_and_typed_diagnostic_without_blocking_healthy_files() {
    for budget in [1, 64] {
        let root = tempfile::tempdir().unwrap();
        let bad = root.path().join("bad.journal");
        fs::write(&bad, [0; 272]).unwrap();
        write_synthetic_journal_file(
            &root.path().join("healthy.journal"),
            &["healthy.service"],
            8,
        );
        let journal =
            Journal::open_dir_with_config(root.path(), config(MmapPolicy::Never, budget)).unwrap();
        assert_eq!(journal.open_errors().len(), 1);
        let (path, error) = &journal.open_errors()[0];
        assert_eq!(path, &bad);
        assert!(matches!(
            error,
            sdjournal::SdJournalError::Unsupported { .. }
        ));
        let entries = journal.query().collect_owned().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].get("_SYSTEMD_UNIT"),
            Some(b"healthy.service".as_slice())
        );
        assert_eq!(
            journal.open_errors().len(),
            1,
            "query does not consume the diagnostics"
        );
    }
}

#[test]
fn excluded_files_do_not_bypass_the_discovery_file_limit() {
    let root = tempfile::tempdir().unwrap();
    for n in 0..3 {
        fs::write(root.path().join(format!("{n}.journal")), [0; 272]).unwrap();
    }
    let result = Journal::open_dir_with_config(
        root.path(),
        JournalConfig {
            max_journal_files: 2,
            ..Default::default()
        },
    );
    assert!(matches!(
        result,
        Err(sdjournal::SdJournalError::LimitExceeded {
            kind: sdjournal::LimitKind::JournalFiles,
            ..
        })
    ));
}

const UNITS: [&str; 3] = ["first.service", "second.service", "third.service"];

fn poisoned_fixture(
    entry_index: usize,
    max_open_files: usize,
) -> (SyntheticJournalFile, Journal, Vec<Cursor>) {
    let layout = SyntheticJournalFile::new(&UNITS);
    let config = JournalConfig {
        mmap_policy: MmapPolicy::Never,
        max_open_files,
        ..Default::default()
    };
    let cursors = {
        let original = Journal::open_dir_with_config(layout.root(), config.clone()).unwrap();
        original
            .query()
            .collect_owned()
            .unwrap()
            .iter()
            .map(|entry| entry.cursor().unwrap())
            .collect()
    };
    let path = layout.root().join("synthetic.journal");
    let mut bytes = fs::read(&path).unwrap();
    let payload = format!(
        "MESSAGE={}",
        synthetic_message(7, entry_index, UNITS[entry_index])
    );
    let matches: Vec<_> = bytes
        .windows(payload.len())
        .enumerate()
        .filter_map(|(index, candidate)| (candidate == payload.as_bytes()).then_some(index))
        .collect();
    assert_eq!(
        matches.len(),
        1,
        "fixture has one unique MESSAGE DATA payload"
    );
    let data_offset = matches[0].checked_sub(64).unwrap();
    assert_eq!(bytes[data_offset], 1, "the actual regular DATA object");
    assert_eq!(
        u64::from_le_bytes(bytes[data_offset + 8..data_offset + 16].try_into().unwrap()),
        u64::try_from(64 + payload.len()).unwrap()
    );
    // Poison only DATA type: every ENTRY header, array offset and saved cursor
    // stays valid. Fresh opening avoids any decoded payload retained by setup.
    bytes[data_offset] = 0;
    fs::write(&path, bytes).unwrap();
    let journal = Journal::open_dir_with_config(layout.root(), config).unwrap();
    (layout, journal, cursors)
}

#[test]
fn valid_cursor_skips_consumed_data_for_empty_and_suffix_queries() {
    for budget in [1, 64] {
        let (_layout, journal, cursors) = poisoned_fixture(0, budget);
        assert!(
            matches!(
                journal.query().collect_owned(),
                Err(SdJournalError::Corrupt { .. })
            ),
            "head must actually decode and reject the poisoned consumed DATA"
        );

        let mut tail = journal.query();
        tail.after_cursor(cursors[2].clone());
        assert!(tail.collect_owned().unwrap().is_empty());

        let mut suffix = journal.query();
        suffix.after_cursor(cursors[0].clone());
        let entries = suffix.collect_owned().unwrap();
        assert_eq!(entries.len(), 2);
        for (entry, index) in entries.iter().zip(1..3) {
            assert_eq!(
                entry.get("MESSAGE").unwrap(),
                synthetic_message(7, index, UNITS[index]).as_bytes()
            );
            assert_eq!(entry.cursor().unwrap(), cursors[index]);
        }
    }
}

#[test]
fn valid_cursor_does_not_hide_corrupt_unread_data() {
    for budget in [1, 64] {
        let (_layout, journal, cursors) = poisoned_fixture(1, budget);
        let mut suffix = journal.query();
        suffix.after_cursor(cursors[0].clone());
        assert!(matches!(
            suffix.collect_owned(),
            Err(SdJournalError::Corrupt { .. })
        ));
    }
}
