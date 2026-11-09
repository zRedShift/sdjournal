use super::decompress::{decompress_lz4, decompress_xz, decompress_zstd};
use super::{DataEntryOffsetIter, JournalFile};
use crate::error::{CompressionAlgo, LimitKind, Result, SdJournalError};
use crate::format::{
    HEADER_INCOMPATIBLE_COMPRESSED_LZ4, HEADER_INCOMPATIBLE_COMPRESSED_XZ,
    HEADER_INCOMPATIBLE_COMPRESSED_ZSTD, OBJECT_DATA, ObjectHeader, compression_from_object_flags,
};
use crate::reader::ByteBuf;
use crate::util::{checked_add_u64, read_u32_le, read_u64_le};

#[derive(Debug, Clone, Copy)]
struct HashItem {
    head_hash_offset: u64,
    #[allow(dead_code)]
    tail_hash_offset: u64,
}

#[cfg(all(test, unix))]
mod prefix_tests {
    use super::*;
    use crate::{JournalConfig, MmapPolicy, reader::test_io};
    use std::{fs::OpenOptions, os::unix::fs::FileExt as _, sync::Arc};

    #[allow(dead_code)]
    mod fixture {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/synthetic_journal.rs"
        ));
    }

    // Isolate a real ordinary/compact DATA object on an actual descriptor. No
    // entry/index lookup is needed to exercise the fixed-prefix parser itself.
    fn object(
        compact: bool,
        object_type: u8,
        declared_size: u64,
        physical_size: usize,
    ) -> (fixture::SyntheticJournalFile, JournalFile, u64) {
        let fixture = fixture::SyntheticJournalFile::new(&["prefix.service"]);
        let path = fixture.root().join("synthetic.journal");
        let mut file = JournalFile::open(
            path.clone(),
            &JournalConfig {
                mmap_policy: MmapPolicy::Never,
                ..Default::default()
            },
        )
        .unwrap();
        let owner = OpenOptions::new().write(true).open(path).unwrap();
        let offset = file.inner.used_size.next_multiple_of(8);
        let mut bytes = vec![0; physical_size];
        bytes[0] = object_type;
        bytes[8..16].copy_from_slice(&declared_size.to_le_bytes());
        let prefix = if compact { 72 } else { 64 };
        if physical_size >= prefix + 5 {
            bytes[prefix..prefix + 5].copy_from_slice(b"F=\0\xffx");
        }
        owner.write_all_at(&bytes, offset).unwrap();
        owner.set_len(offset + physical_size as u64).unwrap();
        let inner = Arc::get_mut(&mut file.inner).unwrap();
        if compact {
            inner.header.incompatible_flags |= crate::format::HEADER_INCOMPATIBLE_COMPACT;
        }
        inner.header.tail_object_offset = offset;
        inner.used_size = offset + physical_size as u64;
        (fixture, file, offset)
    }

    #[test]
    fn short_data_prefix_metadata_and_binary_payload_use_one_read() {
        let mut observed = Vec::new();
        for compact in [false, true] {
            let size = if compact { 77 } else { 69 };
            let (_fixture, file, offset) = object(compact, OBJECT_DATA, size, size as usize);
            test_io::reset();
            let payload = file.read_data_payload_bytes_for_object(offset).unwrap();
            assert_eq!(payload.as_slice(), b"F=\0\xffx");
            observed.push((compact, test_io::reads()));
        }
        assert_eq!(observed, [(false, 1), (true, 1)]);
    }

    #[test]
    fn data_sizes_around_prefix_boundary_preserve_bytes_without_rereading() {
        for compact in [false, true] {
            let header = if compact { 72 } else { 64 };
            for size in [header + 5, 127, 128, 129, 512] {
                let (_fixture, file, offset) = object(compact, OBJECT_DATA, size as u64, size);
                let mut expected = vec![0; size - header];
                expected[..5].copy_from_slice(b"F=\0\xffx");
                if expected.len() > 5 {
                    *expected.last_mut().unwrap() = 0xa5;
                    OpenOptions::new()
                        .write(true)
                        .open(file.path())
                        .unwrap()
                        .write_all_at(&[0xa5], offset + size as u64 - 1)
                        .unwrap();
                }
                test_io::reset();
                let actual = file.read_data_payload_bytes_for_object(offset).unwrap();
                assert_eq!(actual.as_slice(), expected);
                assert_eq!(test_io::reads(), if size <= 128 { 1 } else { 2 });
                assert_eq!(test_io::read_bytes(), size);
            }
        }
    }

    #[test]
    fn physical_optional_tail_failure_preserves_required_diagnostics() {
        for compact in [false, true] {
            for (kind, size, expected) in [
                (0, 16, "expected DATA"),
                (OBJECT_DATA, 16, "too small"),
                (OBJECT_DATA, 1 << 40, "limit"),
            ] {
                let (_fixture, mut file, offset) = object(compact, kind, size, 80);
                Arc::get_mut(&mut file.inner).unwrap().used_size = offset + 256;
                let error = file.read_data_payload_bytes_for_object(offset).unwrap_err();
                if expected == "limit" {
                    assert!(matches!(error, SdJournalError::LimitExceeded { .. }));
                } else {
                    assert!(
                        matches!(&error, SdJournalError::Corrupt { reason, .. } if reason.contains(expected)),
                        "{error:?}"
                    );
                }
            }
            let header = if compact { 72 } else { 64 };
            let (_fixture, mut file, offset) =
                object(compact, OBJECT_DATA, (header + 5) as u64, header + 5);
            Arc::get_mut(&mut file.inner).unwrap().used_size = offset + 256;
            let payload = file.read_data_payload_bytes_for_object(offset).unwrap();
            assert_eq!(payload.as_slice(), b"F=\0\xffx");
            assert!(matches!(
                file.finish_snapshot_read(Ok(payload)),
                Err(SdJournalError::Transient { .. })
            ));
        }
    }

    #[test]
    fn optional_raw_fault_falls_back_once_but_required_fault_still_fails() {
        for compact in [false, true] {
            let size = if compact { 77 } else { 69 };
            let (_fixture, file, offset) = object(compact, OBJECT_DATA, size, size as usize);
            test_io::read_steps([Err(std::io::ErrorKind::Other)]);
            let result = file.read_data_payload_bytes_for_object(offset);
            test_io::read_steps([]);
            assert_eq!(result.unwrap().as_slice(), b"F=\0\xffx");
            test_io::read_steps([
                Err(std::io::ErrorKind::Other),
                Err(std::io::ErrorKind::PermissionDenied),
            ]);
            let result = file.read_data_payload_bytes_for_object(offset);
            test_io::read_steps([]);
            assert!(matches!(result, Err(SdJournalError::Io { .. })));
        }
    }

    #[test]
    fn data_prefix_handles_interrupted_and_short_progress_without_payload_changes() {
        for compact in [false, true] {
            let size = if compact { 77 } else { 69 };
            let (_fixture, file, offset) = object(compact, OBJECT_DATA, size, size as usize);
            test_io::read_steps([
                Err(std::io::ErrorKind::Interrupted),
                Ok(1),
                Err(std::io::ErrorKind::Interrupted),
                Ok(usize::MAX),
            ]);
            let result = file.read_data_payload_bytes_for_object(offset);
            test_io::read_steps([]);
            assert_eq!(result.unwrap().as_slice(), b"F=\0\xffx");
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn retirement_after_prefix_or_during_remainder_overrides_decode_result() {
        for compact in [false, true] {
            for size in [96, 512] {
                let (_fixture, file, offset) = object(compact, OBJECT_DATA, size, size as usize);
                let path = file.path().to_owned();
                let owner = OpenOptions::new().write(true).open(&path).unwrap();
                test_io::reset();
                test_io::after_read(1, move || {
                    std::fs::remove_file(path).unwrap();
                    rustix::fs::fallocate(
                        &owner,
                        rustix::fs::FallocateFlags::PUNCH_HOLE
                            | rustix::fs::FallocateFlags::KEEP_SIZE,
                        0,
                        owner.metadata().unwrap().len(),
                    )
                    .unwrap();
                });
                let decoded = file.read_data_payload_bytes_for_object(offset);
                test_io::reset();
                // The same real entry-publication guard applies whether the
                // prefetched field succeeded or the remainder decoded zeroes.
                assert!(matches!(
                    file.finish_snapshot_read(decoded),
                    Err(SdJournalError::Retired { .. })
                ));
            }
        }
    }

    #[test]
    fn parsed_corruption_and_decompression_errors_do_not_fall_back() {
        for compact in [false, true] {
            let (_fixture, file, offset) = object(compact, 0, 96, 160);
            test_io::reset();
            assert!(matches!(
                file.read_data_payload_bytes_for_object(offset),
                Err(SdJournalError::Corrupt { .. })
            ));
            assert_eq!(test_io::reads(), 1);
            let (_fixture, file, offset) = object(compact, OBJECT_DATA, 96, 160);
            OpenOptions::new()
                .write(true)
                .open(file.path())
                .unwrap()
                .write_all_at(&[1], offset + 1)
                .unwrap();
            test_io::reset();
            assert!(file.read_data_payload_bytes_for_object(offset).is_err());
            assert_eq!(test_io::reads(), 1);
        }
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn short_checksums_are_validated_without_retrying_the_field() {
        let frame = [
            0x28, 0xb5, 0x2f, 0xfd, 0x24, 0x13, 0x99, 0x00, 0x00, 0x4d, 0x45, 0x53, 0x53, 0x41,
            0x47, 0x45, 0x3d, 0x63, 0x68, 0x65, 0x63, 0x6b, 0x73, 0x75, 0x6d, 0x6d, 0x65, 0x64,
            0x65, 0x4f, 0xe7, 0xb0,
        ];
        for compact in [false, true] {
            let header = if compact { 72 } else { 64 };
            let (_fixture, mut file, offset) =
                object(compact, OBJECT_DATA, (header + frame.len()) as u64, 160);
            Arc::get_mut(&mut file.inner)
                .unwrap()
                .header
                .incompatible_flags |= HEADER_INCOMPATIBLE_COMPRESSED_ZSTD;
            let owner = OpenOptions::new().write(true).open(file.path()).unwrap();
            owner.write_all_at(&[4], offset + 1).unwrap();
            owner.write_all_at(&frame, offset + header as u64).unwrap();
            test_io::reset();
            assert_eq!(
                file.read_data_payload_bytes_for_object(offset)
                    .unwrap()
                    .as_slice(),
                b"MESSAGE=checksummed"
            );
            assert_eq!(test_io::reads(), 1);
            owner
                .write_all_at(
                    &[frame[frame.len() - 1] ^ 1],
                    offset + (header + frame.len() - 1) as u64,
                )
                .unwrap();
            test_io::reset();
            assert!(matches!(
                file.read_data_payload_bytes_for_object(offset),
                Err(SdJournalError::DecompressFailed {
                    algo: CompressionAlgo::Zstd,
                    ..
                })
            ));
            assert_eq!(test_io::reads(), 1);
        }
    }

    #[test]
    fn short_logical_tail_preserves_type_size_and_limit_errors() {
        for compact in [false, true] {
            for (kind, size, expected) in [
                (0, 16, "expected DATA"),
                (OBJECT_DATA, 16, "too small"),
                (OBJECT_DATA, 1 << 40, "limit"),
                (OBJECT_DATA, 80, "extends beyond"),
            ] {
                let (_fixture, file, offset) = object(compact, kind, size, 16);
                let error = file.read_data_payload_bytes_for_object(offset).unwrap_err();
                if expected == "limit" {
                    assert!(matches!(error, SdJournalError::LimitExceeded { .. }));
                } else if expected == "extends beyond" {
                    assert!(
                        matches!(&error, SdJournalError::Transient { reason, .. } if reason.contains(expected)),
                        "{error:?}"
                    );
                } else {
                    assert!(
                        matches!(&error, SdJournalError::Corrupt { reason, .. } if reason.contains(expected)),
                        "{error:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn physical_prefix_truncation_is_not_an_empty_payload() {
        for compact in [false, true] {
            let (_fixture, mut file, offset) = object(compact, OBJECT_DATA, 80, 32);
            // Retain the opening snapshot extent while the physical file is short.
            Arc::get_mut(&mut file.inner).unwrap().used_size = offset + 80;
            assert!(matches!(
                file.read_data_payload_bytes_for_object(offset),
                Err(SdJournalError::Transient { .. })
            ));
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct DataObjectMeta {
    hash: u64,
    next_hash_offset: u64,
    #[allow(dead_code)]
    next_field_offset: u64,
    entry_offset: u64,
    entry_array_offset: u64,
    n_entries: u64,
    #[allow(dead_code)]
    tail_entry_array_offset: Option<u32>,
    #[allow(dead_code)]
    tail_entry_array_n_entries: Option<u32>,
    flags: u8,
    size: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DataObjectRef {
    pub(crate) n_entries: u64,
    pub(crate) entry_offset: u64,
    pub(crate) entry_array_offset: u64,
}

impl JournalFile {
    pub(crate) fn read_data_payload_bytes_for_object(&self, data_offset: u64) -> Result<ByteBuf> {
        // The enclosing entry transaction already owns and validates this FD.
        // Other access kinds retain their original read path and map contract.
        if let super::Access::File(access) = &self.inner.access {
            self.validate_object_offset(data_offset)?;
            let header_len = if self.inner.header.is_compact() {
                72
            } else {
                64
            };
            let remaining = self.inner.used_size.saturating_sub(data_offset);
            if remaining >= header_len as u64 {
                let prefix_len = remaining.min(128) as usize;
                let mut prefix = [0u8; 128];
                // Optional adjacent bytes must not hide the old required-range
                // diagnostics. Only a raw read failure takes this one fallback;
                // parsed corruption and decompression errors return directly.
                if crate::reader::read_exact_at(
                    &access.file,
                    data_offset,
                    &mut prefix[..prefix_len],
                )
                .is_ok()
                {
                    let meta = self.parse_data_object_meta(data_offset, &prefix[..prefix_len])?;
                    let object_len =
                        usize::try_from(meta.size).map_err(|_| SdJournalError::LimitExceeded {
                            kind: LimitKind::ObjectSizeBytes,
                            limit: self.inner.config.max_object_size_bytes,
                        })?;
                    let payload = if object_len <= prefix_len {
                        ByteBuf::Owned(std::sync::Arc::from(&prefix[header_len..object_len]))
                    } else {
                        let mut bytes = vec![0; object_len - header_len];
                        let copied = prefix_len - header_len;
                        bytes[..copied].copy_from_slice(&prefix[header_len..prefix_len]);
                        let read_offset = checked_add_u64(
                            data_offset,
                            prefix_len as u64,
                            "DATA remainder offset",
                        )?;
                        let read_len = bytes.len() - copied;
                        crate::reader::read_exact_at(
                            &access.file,
                            read_offset,
                            &mut bytes[copied..],
                        )
                        .map_err(|error| {
                            crate::reader::read_failure(self.path(), read_offset, read_len, error)
                        })?;
                        ByteBuf::from_vec(bytes)
                    };
                    return self.decode_data_payload(data_offset, meta.flags, payload);
                }
            }
        }
        let meta = self.read_data_object_meta(data_offset)?;
        self.read_data_payload_bytes(data_offset, &meta)
    }

    pub(crate) fn find_data_objects(&self, payload: &[u8]) -> Result<Vec<DataObjectRef>> {
        let table_offset = self.inner.header.data_hash_table_offset;
        let table_size = self.inner.header.data_hash_table_size;
        if (table_offset == 0) != (table_size == 0) {
            return Err(self.metadata_error(
                Some(if table_offset == 0 { 104 } else { 112 }),
                format!(
                    "inconsistent DATA_HASH_TABLE metadata (offset={table_offset}, size={table_size})"
                ),
            ));
        }
        if table_offset == 0 {
            return Ok(Vec::new());
        }
        if !table_size.is_multiple_of(16) {
            return Err(SdJournalError::Corrupt {
                path: Some(self.inner.path.clone()),
                offset: Some(table_offset),
                reason: "DATA_HASH_TABLE size is not a multiple of 16".to_string(),
            });
        }
        if !table_offset.is_multiple_of(8) {
            return Err(self.metadata_error(
                Some(table_offset),
                format!("DATA_HASH_TABLE offset is not 8-byte aligned: {table_offset}"),
            ));
        }

        let buckets = table_size / 16;
        if buckets == 0 {
            return Ok(Vec::new());
        }

        let want_hash = self.payload_hash(payload);
        let bucket = want_hash % buckets;
        let item_offset = checked_add_u64(
            table_offset,
            bucket.saturating_mul(16),
            "data_hash_table index",
        )?;
        let item = self.read_hash_item(item_offset)?;
        if (item.head_hash_offset == 0) != (item.tail_hash_offset == 0) {
            return Err(self.metadata_error(
                Some(item_offset),
                "DATA_HASH_TABLE bucket has inconsistent head/tail offsets",
            ));
        }
        if item.tail_hash_offset != 0 && item.tail_hash_offset < item.head_hash_offset {
            return Err(self.metadata_error(
                Some(item_offset),
                "DATA_HASH_TABLE bucket tail precedes its head",
            ));
        }

        let mut out = Vec::new();
        let mut current = item.head_hash_offset;
        let mut last = 0;
        let mut steps = 0usize;
        while current != 0 {
            steps = steps.saturating_add(1);
            if steps > self.inner.config.max_object_chain_steps {
                return Err(SdJournalError::LimitExceeded {
                    kind: LimitKind::ObjectChainSteps,
                    limit: u64::try_from(self.inner.config.max_object_chain_steps)
                        .unwrap_or(u64::MAX),
                });
            }

            let meta = self.read_data_object_meta(current)?;
            if meta.hash == want_hash {
                let have_payload = self.read_data_payload_bytes(current, &meta)?;
                if have_payload.as_slice() == payload {
                    out.push(DataObjectRef {
                        entry_offset: meta.entry_offset,
                        entry_array_offset: meta.entry_array_offset,
                        n_entries: meta.n_entries,
                    });
                }
            }

            last = current;
            let next = meta.next_hash_offset;
            if next != 0 && next <= current {
                return Err(self.metadata_error(
                    Some(current),
                    format!("DATA hash chain does not move forward ({next} <= {current})"),
                ));
            }
            current = next;
        }
        if last != item.tail_hash_offset {
            return Err(self.metadata_error(
                Some(item_offset),
                format!(
                    "DATA_HASH_TABLE bucket tail does not match its chain ({last} != {})",
                    item.tail_hash_offset
                ),
            ));
        }

        Ok(out)
    }

    pub(crate) fn data_entry_offsets(
        &self,
        data: DataObjectRef,
        reverse: bool,
    ) -> Result<DataEntryOffsetIter> {
        DataEntryOffsetIter::new(self.clone(), data, reverse)
    }

    pub(crate) fn data_entry_offsets_after_offset(
        &self,
        data: DataObjectRef,
        reverse: bool,
        after_entry_offset: u64,
    ) -> Result<DataEntryOffsetIter> {
        DataEntryOffsetIter::new_after_offset(self.clone(), data, reverse, Some(after_entry_offset))
    }

    fn payload_hash(&self, payload: &[u8]) -> u64 {
        if self.inner.header.is_keyed_hash() {
            crate::util::hash::siphash24(&self.inner.header.file_id, payload)
        } else {
            crate::util::hash::jenkins_hash64(payload)
        }
    }

    fn read_hash_item(&self, offset: u64) -> Result<HashItem> {
        let buf = self.read_bytes(offset, 16)?;
        let buf = buf.as_slice();
        let head_hash_offset = read_u64_le(buf, 0).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(self.inner.path.clone()),
            offset: Some(offset),
            reason: "HASH_TABLE item truncated".to_string(),
        })?;
        let tail_hash_offset = read_u64_le(buf, 8).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(self.inner.path.clone()),
            offset: Some(offset + 8),
            reason: "HASH_TABLE item truncated".to_string(),
        })?;
        Ok(HashItem {
            head_hash_offset,
            tail_hash_offset,
        })
    }

    fn read_data_object_meta(&self, offset: u64) -> Result<DataObjectMeta> {
        self.validate_object_offset(offset)?;
        if !offset.is_multiple_of(8) {
            return Err(self.metadata_error(
                Some(offset),
                format!("DATA object offset is not 8-byte aligned: {offset}"),
            ));
        }
        let min_size = if self.inner.header.is_compact() {
            72u64
        } else {
            64u64
        };
        // Parse the object header and DATA metadata from the same fixed prefix.
        // At a malformed short logical tail, keep the original header-only
        // read so type/size/limit diagnostics still precede the range error.
        let prefix_len = if offset
            .checked_add(min_size)
            .is_some_and(|end| end <= self.inner.used_size)
        {
            min_size as usize
        } else {
            16
        };
        let buf = self.read_bytes(offset, prefix_len)?;
        self.parse_data_object_meta(offset, buf.as_slice())
    }

    fn parse_data_object_meta(&self, offset: u64, buf: &[u8]) -> Result<DataObjectMeta> {
        let min_size = if self.inner.header.is_compact() {
            72u64
        } else {
            64u64
        };
        let oh = ObjectHeader::parse(buf, self.path(), offset)?;
        if oh.object_type != OBJECT_DATA {
            return Err(SdJournalError::Corrupt {
                path: Some(self.inner.path.clone()),
                offset: Some(offset),
                reason: format!("expected DATA object, found type {}", oh.object_type),
            });
        }

        if oh.size < min_size {
            return Err(SdJournalError::Corrupt {
                path: Some(self.inner.path.clone()),
                offset: Some(offset),
                reason: format!("DATA object too small: {}", oh.size),
            });
        }
        if oh.size > self.inner.config.max_object_size_bytes {
            return Err(SdJournalError::LimitExceeded {
                kind: LimitKind::ObjectSizeBytes,
                limit: self.inner.config.max_object_size_bytes,
            });
        }
        let end = offset
            .checked_add(oh.size)
            .ok_or_else(|| self.metadata_error(Some(offset), "DATA object range overflow"))?;
        if end > self.inner.used_size {
            return Err(self.metadata_error(
                Some(offset),
                format!(
                    "DATA object extends beyond used journal data (end={end}, used_size={})",
                    self.inner.used_size
                ),
            ));
        }

        let hash = read_u64_le(buf, 16).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(self.inner.path.clone()),
            offset: Some(offset + 16),
            reason: "missing DATA.hash".to_string(),
        })?;
        let next_hash_offset = read_u64_le(buf, 24).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(self.inner.path.clone()),
            offset: Some(offset + 24),
            reason: "missing DATA.next_hash_offset".to_string(),
        })?;
        let next_field_offset = read_u64_le(buf, 32).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(self.inner.path.clone()),
            offset: Some(offset + 32),
            reason: "missing DATA.next_field_offset".to_string(),
        })?;
        let entry_offset = read_u64_le(buf, 40).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(self.inner.path.clone()),
            offset: Some(offset + 40),
            reason: "missing DATA.entry_offset".to_string(),
        })?;
        let entry_array_offset = read_u64_le(buf, 48).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(self.inner.path.clone()),
            offset: Some(offset + 48),
            reason: "missing DATA.entry_array_offset".to_string(),
        })?;
        let n_entries = read_u64_le(buf, 56).ok_or_else(|| SdJournalError::Corrupt {
            path: Some(self.inner.path.clone()),
            offset: Some(offset + 56),
            reason: "missing DATA.n_entries".to_string(),
        })?;

        let (tail_entry_array_offset, tail_entry_array_n_entries) =
            if self.inner.header.is_compact() {
                let tail_entry_array_offset =
                    read_u32_le(buf, 64).ok_or_else(|| SdJournalError::Corrupt {
                        path: Some(self.inner.path.clone()),
                        offset: Some(offset + 64),
                        reason: "missing DATA.tail_entry_array_offset".to_string(),
                    })?;
                let tail_entry_array_n_entries =
                    read_u32_le(buf, 68).ok_or_else(|| SdJournalError::Corrupt {
                        path: Some(self.inner.path.clone()),
                        offset: Some(offset + 68),
                        reason: "missing DATA.tail_entry_array_n_entries".to_string(),
                    })?;
                (
                    Some(tail_entry_array_offset),
                    Some(tail_entry_array_n_entries),
                )
            } else {
                (None, None)
            };

        Ok(DataObjectMeta {
            hash,
            next_hash_offset,
            next_field_offset,
            entry_offset,
            entry_array_offset,
            n_entries,
            tail_entry_array_offset,
            tail_entry_array_n_entries,
            flags: oh.flags,
            size: oh.size,
        })
    }

    fn read_data_payload_bytes(&self, offset: u64, meta: &DataObjectMeta) -> Result<ByteBuf> {
        if meta.size > self.inner.config.max_object_size_bytes {
            return Err(SdJournalError::LimitExceeded {
                kind: LimitKind::ObjectSizeBytes,
                limit: self.inner.config.max_object_size_bytes,
            });
        }

        let payload_offset = if self.inner.header.is_compact() {
            72u64
        } else {
            64u64
        };
        if meta.size < payload_offset {
            return Err(SdJournalError::Corrupt {
                path: Some(self.inner.path.clone()),
                offset: Some(offset),
                reason: "DATA object size smaller than payload offset".to_string(),
            });
        }

        let payload_len_u64 = meta.size - payload_offset;
        let payload_len =
            usize::try_from(payload_len_u64).map_err(|_| SdJournalError::LimitExceeded {
                kind: LimitKind::ObjectSizeBytes,
                limit: self.inner.config.max_object_size_bytes,
            })?;

        let payload_offset = checked_add_u64(offset, payload_offset, "DATA payload offset")?;
        let payload = self.read_bytes(payload_offset, payload_len)?;
        self.decode_data_payload(offset, meta.flags, payload)
    }

    fn decode_data_payload(&self, offset: u64, flags: u8, payload: ByteBuf) -> Result<ByteBuf> {
        let payload = match compression_from_object_flags(flags)? {
            None => payload,
            Some(CompressionAlgo::Xz) => {
                if self.inner.header.incompatible_flags & HEADER_INCOMPATIBLE_COMPRESSED_XZ == 0 {
                    return Err(SdJournalError::Corrupt {
                        path: Some(self.inner.path.clone()),
                        offset: Some(offset),
                        reason: "DATA has XZ flag but file header missing incompatible flag"
                            .to_string(),
                    });
                }
                ByteBuf::from_vec(decompress_xz(
                    payload.as_slice(),
                    self.inner.config.max_decompressed_bytes,
                )?)
            }
            Some(CompressionAlgo::Lz4) => {
                if self.inner.header.incompatible_flags & HEADER_INCOMPATIBLE_COMPRESSED_LZ4 == 0 {
                    return Err(SdJournalError::Corrupt {
                        path: Some(self.inner.path.clone()),
                        offset: Some(offset),
                        reason: "DATA has LZ4 flag but file header missing incompatible flag"
                            .to_string(),
                    });
                }
                ByteBuf::from_vec(decompress_lz4(
                    payload.as_slice(),
                    self.inner.config.max_decompressed_bytes,
                )?)
            }
            Some(CompressionAlgo::Zstd) => {
                if self.inner.header.incompatible_flags & HEADER_INCOMPATIBLE_COMPRESSED_ZSTD == 0 {
                    return Err(SdJournalError::Corrupt {
                        path: Some(self.inner.path.clone()),
                        offset: Some(offset),
                        reason: "DATA has ZSTD flag but file header missing incompatible flag"
                            .to_string(),
                    });
                }
                ByteBuf::from_vec(decompress_zstd(
                    payload.as_slice(),
                    self.inner.config.max_decompressed_bytes,
                )?)
            }
        };

        if !payload.as_slice().contains(&b'=') {
            return Err(SdJournalError::Corrupt {
                path: Some(self.inner.path.clone()),
                offset: Some(offset),
                reason: "DATA payload missing '=' separator".to_string(),
            });
        }

        Ok(payload)
    }
}
