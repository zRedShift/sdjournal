use super::JournalFile;
use crate::cursor::SdJournalEntryKey;
use crate::entry::EntryRef;
use crate::error::{LimitKind, Result, SdJournalError};
use crate::format::{OBJECT_ENTRY, ObjectHeader, parse_entry_data_offsets_bytes};
use crate::util::{is_ascii_field_name, read_id128, read_u64_le};

use super::EntryMeta;

#[derive(Debug, Clone, Copy)]
struct ParsedEntryHeader {
    size: u64,
    seqnum: u64,
    realtime_usec: u64,
    monotonic_usec: u64,
    boot_id: [u8; 16],
    xor_hash: u64,
}

impl JournalFile {
    pub(crate) fn read_entry_meta(&self, entry_offset: u64) -> Result<EntryMeta> {
        let parsed = self.read_entry_header(entry_offset)?;

        Ok(EntryMeta {
            file_id: self.inner.header.file_id,
            entry_offset,
            seqnum_id: self.inner.header.seqnum_id,
            seqnum: parsed.seqnum,
            realtime_usec: parsed.realtime_usec,
            monotonic_usec: parsed.monotonic_usec,
            boot_id: parsed.boot_id,
            xor_hash: parsed.xor_hash,
        })
    }

    pub(crate) fn read_entry_ref(&self, entry_offset: u64) -> Result<EntryRef> {
        #[cfg(unix)]
        if let super::Access::Cached(access) = &self.inner.access {
            return access.read_entry(self, entry_offset);
        }
        self.read_entry_ref_inner(entry_offset)
    }

    pub(super) fn read_entry_ref_inner(&self, entry_offset: u64) -> Result<EntryRef> {
        let parsed = self.read_entry_header(entry_offset)?;
        let entry_bytes = self.read_object(entry_offset, parsed.size)?;
        let entry_bytes = entry_bytes.as_slice();

        let items_bytes = entry_bytes
            .get(64..)
            .ok_or_else(|| SdJournalError::Corrupt {
                path: Some(self.inner.path.clone()),
                offset: Some(entry_offset + 64),
                reason: "missing entry items".to_string(),
            })?;

        let data_offsets = parse_entry_data_offsets_bytes(
            items_bytes,
            self.inner.header.is_compact(),
            self.inner.config.max_fields_per_entry,
            self.path(),
            entry_offset,
        )?;

        let mut fields_in_order = Vec::with_capacity(data_offsets.len());
        let mut total_payload_bytes = 0u64;
        for data_offset in data_offsets {
            let payload = self.read_data_payload_bytes_for_object(data_offset)?;
            let (_, eq) = self.parse_data_field_name(data_offset, payload.as_slice())?;
            total_payload_bytes = total_payload_bytes
                .checked_add(u64::try_from(payload.as_slice().len()).unwrap_or(u64::MAX))
                .ok_or(SdJournalError::LimitExceeded {
                    kind: LimitKind::ObjectSizeBytes,
                    limit: self.inner.config.max_object_size_bytes,
                })?;
            if total_payload_bytes > self.inner.config.max_object_size_bytes {
                return Err(SdJournalError::LimitExceeded {
                    kind: LimitKind::ObjectSizeBytes,
                    limit: self.inner.config.max_object_size_bytes,
                });
            }
            fields_in_order.push((payload, eq));
            crate::util::ensure_limit_usize(
                LimitKind::FieldsPerEntry,
                self.inner.config.max_fields_per_entry,
                fields_in_order.len(),
            )?;
        }

        Ok(EntryRef::new_parsed(
            SdJournalEntryKey {
                file_id: self.inner.header.file_id,
                entry_offset,
                seqnum_id: self.inner.header.seqnum_id,
                seqnum: parsed.seqnum,
                boot_id: parsed.boot_id,
                monotonic_usec: parsed.monotonic_usec,
                realtime_usec: parsed.realtime_usec,
                xor_hash: parsed.xor_hash,
            },
            fields_in_order,
        ))
    }

    fn read_entry_header(&self, entry_offset: u64) -> Result<ParsedEntryHeader> {
        self.validate_object_offset(entry_offset)?;
        if !entry_offset.is_multiple_of(8) {
            return Err(self.metadata_error(
                Some(entry_offset),
                format!("ENTRY object offset is not 8-byte aligned: {entry_offset}"),
            ));
        }

        let base = self.read_bytes(entry_offset, 64)?;
        let base = base.as_slice();
        let oh = ObjectHeader::parse(base, self.path(), entry_offset)?;
        if oh.object_type != OBJECT_ENTRY {
            return Err(SdJournalError::Corrupt {
                path: Some(self.inner.path.clone()),
                offset: Some(entry_offset),
                reason: format!("expected ENTRY object, found type {}", oh.object_type),
            });
        }

        let item_size = if self.inner.header.is_compact() {
            4u64
        } else {
            16u64
        };
        let items_size = oh
            .size
            .checked_sub(64)
            .ok_or_else(|| SdJournalError::Corrupt {
                path: Some(self.inner.path.clone()),
                offset: Some(entry_offset),
                reason: format!("ENTRY object too small: {}", oh.size),
            })?;
        if items_size == 0 || !items_size.is_multiple_of(item_size) {
            return Err(SdJournalError::Corrupt {
                path: Some(self.inner.path.clone()),
                offset: Some(entry_offset),
                reason: format!("ENTRY object has invalid item area size: {items_size}"),
            });
        }
        if oh.size > self.inner.config.max_object_size_bytes {
            return Err(SdJournalError::LimitExceeded {
                kind: LimitKind::ObjectSizeBytes,
                limit: self.inner.config.max_object_size_bytes,
            });
        }
        let end = entry_offset.checked_add(oh.size).ok_or_else(|| {
            self.metadata_error(Some(entry_offset), "ENTRY object range overflow")
        })?;
        if end > self.inner.used_size {
            return Err(self.metadata_error(
                Some(entry_offset),
                format!(
                    "ENTRY object extends beyond used journal data (end={end}, used_size={})",
                    self.inner.used_size
                ),
            ));
        }

        let read_u64 = |relative_offset, name: &str| {
            read_u64_le(base, relative_offset).ok_or_else(|| SdJournalError::Corrupt {
                path: Some(self.inner.path.clone()),
                offset: Some(entry_offset.saturating_add(relative_offset as u64)),
                reason: format!("missing {name}"),
            })
        };
        let seqnum = read_u64(16, "seqnum")?;
        let realtime_usec = read_u64(24, "realtime")?;
        let monotonic_usec = read_u64(32, "monotonic")?;
        let boot_id = read_id128(base, 40).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(self.inner.path.clone()),
            offset: Some(entry_offset.saturating_add(40)),
            reason: "missing boot_id".to_string(),
        })?;
        let xor_hash = read_u64(56, "xor_hash")?;

        Ok(ParsedEntryHeader {
            size: oh.size,
            seqnum,
            realtime_usec,
            monotonic_usec,
            boot_id,
            xor_hash,
        })
    }

    fn parse_data_field_name<'a>(
        &self,
        data_offset: u64,
        payload: &'a [u8],
    ) -> Result<(&'a str, usize)> {
        let eq =
            payload
                .iter()
                .position(|&b| b == b'=')
                .ok_or_else(|| SdJournalError::Corrupt {
                    path: Some(self.inner.path.clone()),
                    offset: Some(data_offset),
                    reason: "DATA payload missing '=' separator".to_string(),
                })?;
        let name_bytes = payload.get(..eq).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(self.inner.path.clone()),
            offset: Some(data_offset),
            reason: "DATA payload missing field name".to_string(),
        })?;
        if name_bytes.is_empty() || !is_ascii_field_name(name_bytes) {
            return Err(SdJournalError::Corrupt {
                path: Some(self.inner.path.clone()),
                offset: Some(data_offset),
                reason: "DATA payload contains invalid field name".to_string(),
            });
        }
        crate::util::ensure_limit_usize(
            LimitKind::FieldNameLen,
            self.inner.config.max_field_name_len,
            name_bytes.len(),
        )?;
        let name = std::str::from_utf8(name_bytes).map_err(|_| SdJournalError::Corrupt {
            path: Some(self.inner.path.clone()),
            offset: Some(data_offset),
            reason: "DATA field name is not valid UTF-8".to_string(),
        })?;
        Ok((name, eq))
    }
}
