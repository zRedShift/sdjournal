use super::{EntryMeta, JournalFile};
use crate::{
    cursor::SdJournalEntryKey,
    entry::EntryRef,
    error::{LimitKind, Result, SdJournalError},
    format::{OBJECT_ENTRY, ObjectHeader, parse_entry_data_offsets_bytes},
    util::{is_ascii_field_name, read_id128, read_u64_le},
};

#[cfg(test)]
thread_local! {
    static PUBLICATION_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(super) fn publication_hook() {
    PUBLICATION_HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().take() {
            hook();
        }
    });
}

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
        self.validate_snapshot()?;
        let result = self.read_entry_meta_inner(entry_offset);
        self.finish_snapshot_read(result)
    }

    fn read_entry_meta_inner(&self, entry_offset: u64) -> Result<EntryMeta> {
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
        self.read_entry_ref_with_cache(entry_offset, None)
    }

    pub(crate) fn read_entry_ref_with_cache(
        &self,
        entry_offset: u64,
        mut cache: Option<&mut crate::progress::PayloadCache>,
    ) -> Result<EntryRef> {
        if let Some(cache) = cache.as_deref_mut() {
            cache.begin();
        }
        let result = self.read_entry_transaction(entry_offset, cache.as_deref_mut());
        if let Some(cache) = cache {
            cache.finish(result.is_ok());
        }
        result
    }

    fn read_entry_transaction(
        &self,
        entry_offset: u64,
        cache: Option<&mut crate::progress::PayloadCache>,
    ) -> Result<EntryRef> {
        #[cfg(unix)]
        if let super::Access::Cached(access) = &self.inner.access {
            return match cache {
                Some(cache) => access.read_entry_with_cache(self, entry_offset, Some(cache)),
                None => access.read_entry(self, entry_offset),
            };
        }
        self.validate_snapshot()?;
        let result = match cache {
            Some(cache) => self.read_entry_ref_inner_with_cache(entry_offset, Some(cache)),
            None => self.read_entry_ref_inner(entry_offset),
        }
        .map(EntryRef::into_owned_payloads);
        #[cfg(test)]
        publication_hook();
        self.finish_snapshot_read(result)
    }

    pub(super) fn read_entry_ref_inner(&self, entry_offset: u64) -> Result<EntryRef> {
        self.read_entry_ref_inner_with_cache(entry_offset, None)
    }

    pub(super) fn read_entry_ref_inner_with_cache(
        &self,
        entry_offset: u64,
        mut cache: Option<&mut crate::progress::PayloadCache>,
    ) -> Result<EntryRef> {
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
            let cached = cache
                .as_deref_mut()
                .and_then(|cache| cache.get(self, data_offset));
            let (payload, eq) = if let Some(cached) = cached {
                cached
            } else {
                let payload = self.read_data_payload_bytes_for_object(data_offset)?;
                let (_, eq) = self.parse_data_field_name(data_offset, payload.as_slice())?;
                if let Some(cache) = cache.as_deref_mut() {
                    cache.stage(self, data_offset, &payload, eq);
                }
                (payload, eq)
            };
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

#[cfg(all(test, target_os = "linux"))]
mod retirement_tests {
    use std::{fs, os::unix::fs::FileExt as _};

    use super::*;
    use crate::{Journal, JournalConfig, MmapPolicy, RetirementReason};
    #[allow(dead_code)]
    mod fixture {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/synthetic_journal.rs"
        ));
    }

    #[test]
    fn retirement_between_owned_copy_and_publication_is_refused_for_all_accesses() {
        for budget in [1, 64] {
            for policy in [MmapPolicy::Auto, MmapPolicy::Never] {
                let root = tempfile::tempdir().unwrap();
                let path = root.path().join("a.journal");
                fixture::write_synthetic_journal_file(&path, &["a.service"], 7);
                fixture::write_synthetic_journal_file(
                    &root.path().join("b.journal"),
                    &["b.service"],
                    8,
                );
                fs::OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .unwrap()
                    .write_all_at(&[2], 16)
                    .unwrap();
                let journal = Journal::open_dir_with_config(
                    root.path(),
                    JournalConfig {
                        max_open_files: budget,
                        mmap_policy: policy,
                        ..Default::default()
                    },
                )
                .unwrap();
                let mut iter = journal.query().iter().unwrap();
                PUBLICATION_HOOK.with(|slot| {
                    *slot.borrow_mut() = Some(Box::new(move || {
                        let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
                        fs::remove_file(&path).unwrap();
                        rustix::fs::fallocate(
                            &file,
                            rustix::fs::FallocateFlags::PUNCH_HOLE
                                | rustix::fs::FallocateFlags::KEEP_SIZE,
                            0,
                            file.metadata().unwrap().len(),
                        )
                        .unwrap();
                    }))
                });
                assert!(matches!(
                    iter.next().unwrap(),
                    Err(SdJournalError::Retired {
                        reason: RetirementReason::Unlinked,
                        ..
                    }) | Err(SdJournalError::Retired {
                        reason: RetirementReason::Disappeared,
                        ..
                    })
                ));
            }
        }
    }

    #[test]
    fn post_error_retirement_overrides_zero_decode_but_linked_corruption_survives() {
        for budget in [1, 64] {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("a.journal");
            fixture::write_synthetic_journal_file(&path, &["a.service"], 7);
            fixture::write_synthetic_journal_file(
                &root.path().join("b.journal"),
                &["b.service"],
                8,
            );
            let journal = Journal::open_dir_with_config(
                root.path(),
                JournalConfig {
                    max_open_files: budget,
                    mmap_policy: MmapPolicy::Never,
                    ..Default::default()
                },
            )
            .unwrap();
            let file = journal.inner.open_file_by_index(0).unwrap();
            let offset = file.inner.header.tail_entry_offset.unwrap();
            fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap()
                .write_all_at(&[0], offset)
                .unwrap();
            assert!(matches!(
                file.read_entry_ref(offset),
                Err(SdJournalError::Corrupt { .. })
            ));
            PUBLICATION_HOOK.with(|slot| {
                *slot.borrow_mut() = Some(Box::new(move || {
                    fs::remove_file(path).unwrap();
                }))
            });
            assert!(matches!(
                file.read_entry_ref(offset),
                Err(SdJournalError::Retired { .. })
            ));
        }
    }
}
