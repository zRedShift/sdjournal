#![cfg(feature = "mmap")]

mod support;

use sdjournal::Journal;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::io::{Seek, SeekFrom, Write};
use support::synthetic_journal::{SyntheticJournalFile, synthetic_message};

// The observer covers only the calling test thread's next() region. Fixture,
// query setup, assertion formatting and held-entry clones are outside it.
thread_local! {
    static COUNT: Cell<Option<(usize, usize)>> = const { Cell::new(None) };
}

struct CountedSystem;

fn record(reallocation: bool) {
    let _ = COUNT.try_with(|count| {
        if let Some((allocations, reallocations)) = count.get() {
            count.set(Some((allocations.saturating_add(usize::from(!reallocation)),
                            reallocations.saturating_add(usize::from(reallocation)))));
        }
    });
}

// SAFETY: Every operation delegates the original pointer/layout to System;
// the thread-local observer uses no heap allocation and cannot unwind.
unsafe impl GlobalAlloc for CountedSystem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(false);
        // SAFETY: The caller supplies the GlobalAlloc allocation contract.
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(false);
        // SAFETY: The caller supplies the GlobalAlloc allocation contract.
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record(true);
        // SAFETY: The caller supplies a System allocation and valid new size.
        unsafe { System.realloc(pointer, layout, size) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: Every pointer handled here was allocated by System.
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountedSystem = CountedSystem;

#[test]
fn mmap_entry_walk_uses_one_metadata_allocation_without_regrowth() {
    let units = vec!["alpha.service"; 64];
    let fixture = SyntheticJournalFile::new(&units);
    fixture.rewrite(&units);
    // The synthetic builder creates an online file. This fixture is finished
    // before opening, so mark it OFFLINE to exercise the real stable mmap path.
    let mut file = std::fs::OpenOptions::new().write(true)
        .open(fixture.root().join("synthetic.journal")).unwrap();
    file.seek(SeekFrom::Start(16)).unwrap();
    file.write_all(&[0]).unwrap();
    drop(file);
    let journal = Journal::open_dir(fixture.root()).unwrap();
    let mut entries = journal.query().iter().unwrap();
    let mut held = Vec::with_capacity(units.len());
    for (index, unit) in units.iter().enumerate() {
        COUNT.with(|count| count.set(Some((0, 0))));
        let item = entries.next();
        let observed = COUNT.with(|count| count.replace(None).unwrap());
        let entry = item.unwrap().unwrap();
        assert_eq!(observed, (1, 0), "entry {index} metadata allocation/reallocation");
        assert_eq!(entry.get("MESSAGE").unwrap(), synthetic_message(7, index, unit).as_bytes());
        assert_eq!(entry.iter_fields().count(), 6);
        held.push(entry);
    }
    assert!(entries.next().is_none());
    let clone = held[0].clone();
    drop(entries);
    drop(journal);
    for (index, entry) in held.iter().enumerate() {
        assert_eq!(entry.get("MESSAGE").unwrap(), synthetic_message(7, index, units[index]).as_bytes());
    }
    drop(held);
    assert_eq!(clone.get("MESSAGE").unwrap(), synthetic_message(7, 0, units[0]).as_bytes());
}
