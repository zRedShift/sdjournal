#[allow(dead_code)]
mod support;

use sdjournal::{Journal, JournalConfig, MmapPolicy};
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use support::synthetic_journal::write_synthetic_journal_file;

#[test]
fn journal_open_keeps_the_same_snapshot_before_query_creation_in_both_modes() {
    for max_open_files in [1, 64] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("online.journal");
        write_synthetic_journal_file(&path, &["first.service", "second.service"], 7);
        write_synthetic_journal_file(&root.path().join("other.journal"), &["other.service"], 9);

        // Model the writer having appended both objects but committed only one entry.
        let committed = fs::read(&path).unwrap();
        let array_offset = u64::from_le_bytes(committed[176..184].try_into().unwrap()) as usize;
        let first_offset = u64::from_le_bytes(
            committed[array_offset + 24..array_offset + 32]
                .try_into()
                .unwrap(),
        );
        let mut initial = committed.clone();
        initial[152..160].copy_from_slice(&1u64.to_le_bytes());
        initial[160..168].copy_from_slice(&1u64.to_le_bytes());
        initial[192..200].copy_from_slice(&committed[184..192]);
        initial[264..272].copy_from_slice(&first_offset.to_le_bytes());
        fs::write(&path, initial).unwrap();

        let config = JournalConfig {
            max_open_files,
            mmap_policy: MmapPolicy::Never,
            ..Default::default()
        };
        let journal = Journal::open_dir_with_config(root.path(), config.clone()).unwrap();

        // Commit the second entry on the same inode before creating the query iterator.
        let mut writer = OpenOptions::new().write(true).open(&path).unwrap();
        writer.seek(SeekFrom::Start(152)).unwrap();
        writer.write_all(&committed[152..272]).unwrap();
        writer.sync_all().unwrap();

        let entries = journal.query().collect_owned().unwrap();
        assert_eq!(entries.len(), 2, "max_open_files={max_open_files}");
        assert!(
            entries
                .iter()
                .all(|entry| entry.get("_SYSTEMD_UNIT") != Some(b"second.service".as_slice()))
        );

        let reopened = Journal::open_dir_with_config(root.path(), config).unwrap();
        assert_eq!(reopened.query().collect_owned().unwrap().len(), 3);
    }
}

#[test]
fn existing_snapshots_do_not_parse_a_writers_new_uncommitted_tail() {
    for max_open_files in [1, 64] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("online.journal");
        write_synthetic_journal_file(&path, &["first.service"], 7);
        write_synthetic_journal_file(&root.path().join("other.journal"), &["other.service"], 9);
        let journal = Journal::open_dir_with_config(
            root.path(),
            JournalConfig {
                max_open_files,
                mmap_policy: MmapPolicy::Never,
                ..Default::default()
            },
        )
        .unwrap();

        // Existing ENTRY/DATA bytes and identity remain valid while the new tail is incomplete.
        let end = fs::metadata(&path).unwrap().len();
        let mut writer = OpenOptions::new().write(true).open(&path).unwrap();
        writer.seek(SeekFrom::Start(136)).unwrap();
        writer.write_all(&end.to_le_bytes()).unwrap();
        writer.sync_all().unwrap();

        let entries = journal.query().collect_owned().unwrap();
        assert_eq!(entries.len(), 2, "max_open_files={max_open_files}");
    }
}

#[test]
fn evicted_snapshot_does_not_reparse_a_later_incomplete_tail() {
    for max_open_files in [1, 2] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("first.journal");
        for (name, seed) in [("first", 7), ("second", 9), ("third", 11)] {
            write_synthetic_journal_file(
                &root.path().join(format!("{name}.journal")),
                &["a.service", "b.service"],
                seed,
            );
        }
        let config = JournalConfig {
            max_open_files,
            mmap_policy: MmapPolicy::Never,
            ..Default::default()
        };
        let journal = Journal::open_dir_with_config(root.path(), config.clone()).unwrap();
        let mut iter = journal.query().iter().unwrap();
        // Construction touched all three files: the earliest file must be cold
        // when either bounded cache reads its prefetched candidate next.
        let end = fs::metadata(&path).unwrap().len();
        let mut writer = OpenOptions::new().write(true).open(&path).unwrap();
        writer.seek(SeekFrom::Start(136)).unwrap();
        writer.write_all(&end.to_le_bytes()).unwrap();
        writer.sync_all().unwrap();
        let entries = iter.by_ref().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(entries.len(), 6, "budget={max_open_files}");
        drop(iter);
        let fresh = Journal::open_dir_with_config(root.path(), config).unwrap();
        assert_eq!(fresh.open_errors().len(), 1);
        assert_eq!(fresh.open_errors()[0].0, path);
        assert_eq!(fresh.query().collect_owned().unwrap().len(), 4);
    }
}
