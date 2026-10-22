use crate::cursor::{Cursor, SdJournalEntryKey};
use crate::error::Result;
use crate::reader::ByteBuf;
use std::ops::Deref;
use std::sync::Arc;

/// An owned journal entry, suitable for caching, cross-thread use, or async contexts.
///
/// `EntryOwned` stores field values in owned `Vec<u8>` buffers. Use it when an entry must outlive
/// the immediate query iteration, be retained in an application cache, or cross thread/task
/// boundaries without keeping journal storage alive.
#[derive(Debug, Clone)]
pub struct EntryOwned {
    pub(crate) file_id: [u8; 16],
    pub(crate) entry_offset: u64,
    seqnum_id: [u8; 16],
    seqnum: u64,
    realtime_usec: u64,
    monotonic_usec: u64,
    boot_id: [u8; 16],
    xor_hash: u64,
    fields_in_order: Vec<(String, Vec<u8>)>,
}

/// A shared live entry delivered by [`crate::LiveSubscription`].
///
/// This wraps an [`EntryRef`] in reference counting so one decoded journal entry can be dispatched
/// to multiple live subscribers without duplicating all field storage.
///
/// `LiveEntry` dereferences to [`EntryRef`], so methods such as [`EntryRef::get`] and
/// [`EntryRef::cursor`] can be called directly. Convert it with [`LiveEntry::into_owned`] if the
/// entry must be stored independently of live delivery internals.
#[derive(Debug, Clone)]
pub struct LiveEntry(Arc<EntryRef>);

impl LiveEntry {
    pub(crate) fn new(entry: EntryRef) -> Self {
        Self(Arc::new(entry))
    }

    /// Convert this shared live entry into an owned entry.
    ///
    /// This copies all visible field values into owned buffers.
    pub fn into_owned(self) -> EntryOwned {
        self.0.as_ref().to_owned()
    }
}

impl Deref for LiveEntry {
    type Target = EntryRef;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl AsRef<EntryRef> for LiveEntry {
    fn as_ref(&self) -> &EntryRef {
        self.0.as_ref()
    }
}

impl EntryOwned {
    pub(crate) fn new(key: SdJournalEntryKey, fields_in_order: Vec<(String, Vec<u8>)>) -> Self {
        Self {
            file_id: key.file_id,
            entry_offset: key.entry_offset,
            seqnum_id: key.seqnum_id,
            seqnum: key.seqnum,
            realtime_usec: key.realtime_usec,
            monotonic_usec: key.monotonic_usec,
            boot_id: key.boot_id,
            xor_hash: key.xor_hash,
            fields_in_order,
        }
    }

    /// Return the entry cursor.
    ///
    /// Store this cursor to resume later with [`crate::JournalQuery::after_cursor`] or
    /// [`crate::SubscriptionOptions::after_cursor`].
    pub fn cursor(&self) -> Result<Cursor> {
        Ok(Cursor::new_location_key(self.entry_key()))
    }

    /// Get the first occurrence of a field.
    ///
    /// Journal entries can contain duplicate fields. Use [`EntryOwned::iter_fields`] when duplicate
    /// values matter.
    pub fn get(&self, field: &str) -> Option<&[u8]> {
        self.fields_in_order
            .iter()
            .find(|(k, _)| k == field)
            .map(|(_, v)| v.as_slice())
    }

    /// Iterate over all fields, including duplicates, in entry order.
    pub fn iter_fields(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.fields_in_order
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_slice()))
    }

    /// Realtime timestamp, microseconds since UNIX epoch.
    pub fn realtime_usec(&self) -> u64 {
        self.realtime_usec
    }

    /// Monotonic timestamp, microseconds since boot.
    pub fn monotonic_usec(&self) -> u64 {
        self.monotonic_usec
    }

    /// Boot ID for this entry.
    pub fn boot_id(&self) -> [u8; 16] {
        self.boot_id
    }

    /// Sequence number for this entry.
    pub fn seqnum(&self) -> u64 {
        self.seqnum
    }

    pub(crate) fn entry_key(&self) -> SdJournalEntryKey {
        SdJournalEntryKey {
            file_id: self.file_id,
            entry_offset: self.entry_offset,
            seqnum_id: self.seqnum_id,
            seqnum: self.seqnum,
            boot_id: self.boot_id,
            monotonic_usec: self.monotonic_usec,
            realtime_usec: self.realtime_usec,
            xor_hash: self.xor_hash,
        }
    }
}

#[derive(Debug, Clone)]
struct FieldRef {
    payload: ByteBuf,
    eq_pos: usize,
}

impl FieldRef {
    fn name(&self) -> &str {
        let bytes = self
            .payload
            .as_slice()
            .get(..self.eq_pos)
            .unwrap_or_default();
        std::str::from_utf8(bytes).unwrap_or_default()
    }

    fn value(&self) -> &[u8] {
        let payload = self.payload.as_slice();
        payload.get(self.eq_pos.saturating_add(1)..).unwrap_or(&[])
    }
}

/// A zero-copy entry view, backed by journal file storage when possible.
///
/// Query iterators yield `EntryRef` to avoid copying field payloads by default. The value is cheap
/// to inspect during iteration, but callers should use [`EntryRef::to_owned`] before storing it in
/// long-lived application state.
#[derive(Debug, Clone)]
pub struct EntryRef {
    file_id: [u8; 16],
    entry_offset: u64,
    seqnum_id: [u8; 16],
    seqnum: u64,
    realtime_usec: u64,
    monotonic_usec: u64,
    boot_id: [u8; 16],
    xor_hash: u64,
    fields_in_order: Vec<FieldRef>,
}

impl EntryRef {
    /// Cached entry transactions must not lend their map beyond the cache lock.
    #[cfg(unix)]
    pub(crate) fn into_owned_payloads(mut self) -> Self {
        for field in &mut self.fields_in_order {
            if !matches!(field.payload, ByteBuf::Owned(_)) {
                field.payload = ByteBuf::from_vec(field.payload.as_slice().to_vec());
            }
        }
        self
    }

    pub(crate) fn new_parsed(
        key: SdJournalEntryKey,
        fields_in_order: Vec<(ByteBuf, usize)>,
    ) -> Self {
        let fields_in_order = fields_in_order
            .into_iter()
            .map(|(payload, eq_pos)| FieldRef { payload, eq_pos })
            .collect();

        Self {
            file_id: key.file_id,
            entry_offset: key.entry_offset,
            seqnum_id: key.seqnum_id,
            seqnum: key.seqnum,
            realtime_usec: key.realtime_usec,
            monotonic_usec: key.monotonic_usec,
            boot_id: key.boot_id,
            xor_hash: key.xor_hash,
            fields_in_order,
        }
    }

    /// Convert this entry to an owned representation.
    ///
    /// This copies all visible field values into an [`EntryOwned`].
    pub fn to_owned(&self) -> EntryOwned {
        let mut fields = Vec::with_capacity(self.fields_in_order.len());
        for f in &self.fields_in_order {
            fields.push((f.name().to_owned(), f.value().to_vec()));
        }

        EntryOwned::new(self.entry_key(), fields)
    }

    /// Return the entry cursor.
    ///
    /// Store this cursor to resume later with [`crate::JournalQuery::after_cursor`] or
    /// [`crate::SubscriptionOptions::after_cursor`].
    pub fn cursor(&self) -> Result<Cursor> {
        Ok(Cursor::new_location_key(self.entry_key()))
    }

    /// Get the first occurrence of a field.
    ///
    /// Journal entries can contain duplicate fields. Use [`EntryRef::iter_fields`] when duplicate
    /// values matter.
    pub fn get(&self, field: &str) -> Option<&[u8]> {
        self.fields_in_order
            .iter()
            .find(|f| f.name() == field)
            .map(|f| f.value())
    }

    /// Iterate over all fields, including duplicates, in entry order.
    pub fn iter_fields(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.fields_in_order.iter().map(|f| (f.name(), f.value()))
    }

    /// Realtime timestamp, microseconds since UNIX epoch.
    pub fn realtime_usec(&self) -> u64 {
        self.realtime_usec
    }

    /// Monotonic timestamp, microseconds since boot.
    pub fn monotonic_usec(&self) -> u64 {
        self.monotonic_usec
    }

    /// Boot ID for this entry.
    pub fn boot_id(&self) -> [u8; 16] {
        self.boot_id
    }

    /// Sequence number for this entry.
    pub fn seqnum(&self) -> u64 {
        self.seqnum
    }

    pub(crate) fn entry_key(&self) -> SdJournalEntryKey {
        SdJournalEntryKey {
            file_id: self.file_id,
            entry_offset: self.entry_offset,
            seqnum_id: self.seqnum_id,
            seqnum: self.seqnum,
            boot_id: self.boot_id,
            monotonic_usec: self.monotonic_usec,
            realtime_usec: self.realtime_usec,
            xor_hash: self.xor_hash,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(all(unix, feature = "mmap"))]
    #[test]
    fn cached_payload_conversion_releases_map_and_preserves_binary_duplicates() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("payload");
        let bytes = b"MESSAGE=\0\xffMESSAGE=second";
        std::fs::write(&path, bytes).unwrap();
        let file = std::fs::File::open(&path).unwrap();
        // Test fixture is immutable for the map lifetime.
        let map = Arc::new(unsafe { memmap2::MmapOptions::new().map(&file) }.unwrap());
        let weak = Arc::downgrade(&map);
        let entry = EntryRef::new_parsed(
            SdJournalEntryKey {
                file_id: [1; 16],
                entry_offset: 8,
                seqnum_id: [2; 16],
                seqnum: 3,
                realtime_usec: 4,
                monotonic_usec: 5,
                boot_id: [6; 16],
                xor_hash: 7,
            },
            vec![
                (
                    ByteBuf::from_mmap(Arc::clone(&map), 0..10, &path).unwrap(),
                    7,
                ),
                (
                    ByteBuf::from_mmap(Arc::clone(&map), 10..bytes.len(), &path).unwrap(),
                    7,
                ),
                (ByteBuf::from_vec(b"OTHER=owned".to_vec()), 5),
            ],
        );
        let key = entry.entry_key();
        let owned_arc = match &entry.fields_in_order[2].payload {
            ByteBuf::Owned(value) => Arc::downgrade(value),
            _ => unreachable!(),
        };
        drop(map);
        let converted = entry.into_owned_payloads();
        assert!(weak.upgrade().is_none());
        assert!(
            owned_arc.upgrade().is_some(),
            "already-owned allocation is reused"
        );
        assert_eq!(converted.entry_key(), key);
        assert_eq!(
            converted.iter_fields().collect::<Vec<_>>(),
            vec![
                ("MESSAGE", &b"\0\xff"[..]),
                ("MESSAGE", &b"second"[..]),
                ("OTHER", &b"owned"[..]),
            ]
        );
        assert!(
            converted
                .fields_in_order
                .iter()
                .all(|f| matches!(f.payload, ByteBuf::Owned(_)))
        );
    }

    #[test]
    fn entry_owned_get_prefers_first_duplicate_and_iter_preserves_order() {
        let entry = EntryOwned::new(
            SdJournalEntryKey {
                file_id: [0x11; 16],
                entry_offset: 7,
                seqnum_id: [0x12; 16],
                seqnum: 9,
                realtime_usec: 11,
                monotonic_usec: 13,
                boot_id: [0x22; 16],
                xor_hash: 15,
            },
            vec![
                ("MESSAGE".to_string(), b"first".to_vec()),
                ("PRIORITY".to_string(), b"6".to_vec()),
                ("MESSAGE".to_string(), b"second".to_vec()),
            ],
        );

        assert_eq!(entry.get("MESSAGE"), Some(&b"first"[..]));
        let fields: Vec<(&str, &[u8])> = entry.iter_fields().collect();
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0], ("MESSAGE", &b"first"[..]));
        assert_eq!(fields[1], ("PRIORITY", &b"6"[..]));
        assert_eq!(fields[2], ("MESSAGE", &b"second"[..]));
        assert_eq!(entry.realtime_usec(), 11);
        assert_eq!(entry.monotonic_usec(), 13);
        assert_eq!(entry.seqnum(), 9);
        assert_eq!(entry.boot_id(), [0x22; 16]);
    }

    #[test]
    fn entry_ref_to_owned_preserves_visible_fields_and_cursor() {
        let entry = EntryRef::new_parsed(
            SdJournalEntryKey {
                file_id: [0x33; 16],
                entry_offset: 17,
                seqnum_id: [0x34; 16],
                seqnum: 19,
                realtime_usec: 23,
                monotonic_usec: 29,
                boot_id: [0x44; 16],
                xor_hash: 31,
            },
            vec![
                (ByteBuf::from_vec(b"MESSAGE=hello".to_vec()), 7),
                (ByteBuf::from_vec(b"PRIORITY=5".to_vec()), 8),
            ],
        );

        let owned = entry.to_owned();
        assert_eq!(entry.get("MESSAGE"), Some(&b"hello"[..]));
        assert_eq!(owned.get("MESSAGE"), Some(&b"hello"[..]));
        assert_eq!(owned.get("PRIORITY"), Some(&b"5"[..]));
        assert_eq!(
            entry.cursor().unwrap().to_string(),
            owned.cursor().unwrap().to_string()
        );
        assert_eq!(owned.realtime_usec(), 23);
        assert_eq!(owned.monotonic_usec(), 29);
        assert_eq!(owned.seqnum(), 19);
        assert_eq!(owned.boot_id(), [0x44; 16]);
    }
}
