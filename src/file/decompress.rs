use crate::error::{Result, SdJournalError};

#[cfg(any(feature = "lz4", feature = "zstd", feature = "xz"))]
use crate::error::{CompressionAlgo, LimitKind};
#[cfg(feature = "lz4")]
use crate::util::read_u64_le;

#[cfg(feature = "lz4")]
pub(super) fn decompress_lz4(src: &[u8], max: usize) -> Result<Vec<u8>> {
    if src.len() <= 8 {
        return Err(SdJournalError::DecompressFailed {
            algo: CompressionAlgo::Lz4,
            reason: "lz4 payload too short".to_string(),
        });
    }

    let size = read_u64_le(src, 0).ok_or_else(|| SdJournalError::DecompressFailed {
        algo: CompressionAlgo::Lz4,
        reason: "missing uncompressed size".to_string(),
    })?;

    let size_usize = usize::try_from(size).map_err(|_| SdJournalError::LimitExceeded {
        kind: LimitKind::DecompressedBytes,
        limit: u64::try_from(max).unwrap_or(u64::MAX),
    })?;
    crate::util::ensure_limit_usize(LimitKind::DecompressedBytes, max, size_usize)?;

    let compressed = &src[8..];
    let output = lz4_flex::block::decompress(compressed, size_usize).map_err(|e| {
        SdJournalError::DecompressFailed {
            algo: CompressionAlgo::Lz4,
            reason: e.to_string(),
        }
    })?;
    if output.len() != size_usize {
        return Err(SdJournalError::DecompressFailed {
            algo: CompressionAlgo::Lz4,
            reason: format!(
                "lz4 output length {} differs from advertised size {size_usize}",
                output.len()
            ),
        });
    }
    Ok(output)
}

#[cfg(not(feature = "lz4"))]
pub(super) fn decompress_lz4(_src: &[u8], _max: usize) -> Result<Vec<u8>> {
    Err(SdJournalError::Unsupported {
        reason: "lz4 support is disabled (feature lz4)".to_string(),
    })
}

#[cfg(feature = "zstd")]
pub(super) fn decompress_zstd(src: &[u8], max: usize) -> Result<Vec<u8>> {
    use ruzstd::decoding::StreamingDecoder;
    use ruzstd::io::Read as _;

    const DEFAULT_WORKING_SET_LIMIT: usize = 8 * 1024 * 1024;
    let mut reader: &[u8] = src;
    let mut out = Vec::new();
    let mut buf = [0u8; 16 * 1024];
    let mut frame_index = 0usize;

    if reader.is_empty() {
        return Err(zstd_malformed("zstd frame header is truncated"));
    }

    while !reader.is_empty() {
        let magic = zstd_frame_magic(reader)?;
        if is_zstd_skippable_magic(magic) {
            reader = skip_zstd_frame(reader)?;
            frame_index = frame_index.saturating_add(1);
            continue;
        }

        let (window_size, content_size) = zstd_frame_sizes(reader)?;
        let window_limit = max.max(DEFAULT_WORKING_SET_LIMIT);
        if window_size > u64::try_from(window_limit).unwrap_or(u64::MAX) {
            return Err(SdJournalError::DecompressFailed {
                algo: CompressionAlgo::Zstd,
                reason: format!(
                    "zstd frame {frame_index} window size {window_size} exceeds decoder working-set limit {window_limit}"
                ),
            });
        }

        let input_len = reader.len();
        let frame_output_start = out.len();
        {
            let mut decoder = StreamingDecoder::new(&mut reader).map_err(|e| {
                SdJournalError::DecompressFailed {
                    algo: CompressionAlgo::Zstd,
                    reason: format!("zstd frame {frame_index}: {e}"),
                }
            })?;

            loop {
                let n = decoder
                    .read(&mut buf)
                    .map_err(|e| SdJournalError::DecompressFailed {
                        algo: CompressionAlgo::Zstd,
                        reason: format!("zstd frame {frame_index}: {e}"),
                    })?;
                if n == 0 {
                    break;
                }
                if out.len().saturating_add(n) > max {
                    return Err(SdJournalError::LimitExceeded {
                        kind: LimitKind::DecompressedBytes,
                        limit: u64::try_from(max).unwrap_or(u64::MAX),
                    });
                }
                out.extend_from_slice(&buf[..n]);
            }

            // StreamingDecoder exposes these values but does not compare them. Check only
            // after draining the frame, when its calculated checksum includes all output.
            if let Some(expected) = decoder.decoder.get_checksum_from_data()
                && decoder.decoder.get_calculated_checksum() != Some(expected)
            {
                return Err(SdJournalError::DecompressFailed {
                    algo: CompressionAlgo::Zstd,
                    reason: format!("zstd frame {frame_index} content checksum mismatch"),
                });
            }
        }

        if let Some(expected) = content_size {
            let produced = out.len() - frame_output_start;
            if u64::try_from(produced).unwrap_or(u64::MAX) != expected {
                return Err(SdJournalError::DecompressFailed {
                    algo: CompressionAlgo::Zstd,
                    reason: format!(
                        "zstd frame {frame_index} output length {produced} differs from advertised size {expected}"
                    ),
                });
            }
        }

        if reader.len() >= input_len {
            return Err(zstd_malformed("zstd decoder made no input progress"));
        }
        frame_index = frame_index.saturating_add(1);
    }

    Ok(out)
}

#[cfg(feature = "zstd")]
fn zstd_frame_sizes(src: &[u8]) -> Result<(u64, Option<u64>)> {
    const ZSTD_MAGIC: u32 = 0xfd2f_b528;

    let magic = zstd_frame_magic(src)?;
    if magic != ZSTD_MAGIC {
        return Err(zstd_malformed("invalid zstd frame magic"));
    }

    let descriptor = *src
        .get(4)
        .ok_or_else(|| zstd_malformed("zstd frame descriptor is truncated"))?;
    let single_segment = descriptor & 0x20 != 0;
    let mut pos = 5usize;

    let descriptor_window = if single_segment {
        None
    } else {
        let window_descriptor = *src
            .get(pos)
            .ok_or_else(|| zstd_malformed("zstd window descriptor is truncated"))?;
        pos = pos.saturating_add(1);
        let exponent = u32::from(window_descriptor >> 3);
        let mantissa = u64::from(window_descriptor & 0x07);
        let window_base = 1u64
            .checked_shl(10 + exponent)
            .ok_or_else(|| zstd_malformed("zstd window size overflows u64"))?;
        Some(window_base + (window_base / 8) * mantissa)
    };

    let dictionary_id_len = match descriptor & 0x03 {
        0 => 0usize,
        1 => 1,
        2 => 2,
        3 => 4,
        _ => return Err(zstd_malformed("invalid zstd dictionary-id flag")),
    };
    pos = pos
        .checked_add(dictionary_id_len)
        .ok_or_else(|| zstd_malformed("zstd frame header size overflows"))?;
    if pos > src.len() {
        return Err(zstd_malformed("zstd dictionary id is truncated"));
    }

    let frame_content_size_len = match descriptor >> 6 {
        0 if single_segment => 1usize,
        0 => 0,
        1 => 2,
        2 => 4,
        3 => 8,
        _ => return Err(zstd_malformed("invalid zstd content-size flag")),
    };
    let end = pos
        .checked_add(frame_content_size_len)
        .ok_or_else(|| zstd_malformed("zstd frame header size overflows"))?;
    let frame_content_size_bytes = src
        .get(pos..end)
        .ok_or_else(|| zstd_malformed("zstd frame content size is truncated"))?;
    let mut frame_content_size = 0u64;
    for (shift, byte) in frame_content_size_bytes.iter().enumerate() {
        frame_content_size |= u64::from(*byte) << (shift * 8);
    }
    if frame_content_size_len == 2 {
        frame_content_size = frame_content_size.saturating_add(256);
    }

    Ok((
        descriptor_window.unwrap_or(frame_content_size),
        (frame_content_size_len != 0).then_some(frame_content_size),
    ))
}

#[cfg(feature = "zstd")]
fn zstd_frame_magic(src: &[u8]) -> Result<u32> {
    let bytes = src
        .get(..4)
        .ok_or_else(|| zstd_malformed("zstd frame header is truncated"))?;
    let mut magic = [0u8; 4];
    magic.copy_from_slice(bytes);
    Ok(u32::from_le_bytes(magic))
}

#[cfg(feature = "zstd")]
fn is_zstd_skippable_magic(magic: u32) -> bool {
    (0x184d_2a50..=0x184d_2a5f).contains(&magic)
}

#[cfg(feature = "zstd")]
fn skip_zstd_frame(src: &[u8]) -> Result<&[u8]> {
    const SKIPPABLE_HEADER_SIZE: usize = 8;

    let size_bytes = src
        .get(4..SKIPPABLE_HEADER_SIZE)
        .ok_or_else(|| zstd_malformed("zstd skippable frame header is truncated"))?;
    let mut size = [0u8; 4];
    size.copy_from_slice(size_bytes);
    let payload_size = usize::try_from(u32::from_le_bytes(size))
        .map_err(|_| zstd_malformed("zstd skippable frame size does not fit usize"))?;
    let frame_size = SKIPPABLE_HEADER_SIZE
        .checked_add(payload_size)
        .ok_or_else(|| zstd_malformed("zstd skippable frame size overflows"))?;
    src.get(frame_size..)
        .ok_or_else(|| zstd_malformed("zstd skippable frame payload is truncated"))
}

#[cfg(feature = "zstd")]
fn zstd_malformed(reason: &str) -> SdJournalError {
    SdJournalError::DecompressFailed {
        algo: CompressionAlgo::Zstd,
        reason: reason.to_string(),
    }
}

#[cfg(not(feature = "zstd"))]
pub(super) fn decompress_zstd(_src: &[u8], _max: usize) -> Result<Vec<u8>> {
    Err(SdJournalError::Unsupported {
        reason: "zstd support is disabled (feature zstd)".to_string(),
    })
}

#[cfg(feature = "xz")]
pub(super) fn decompress_xz(src: &[u8], max: usize) -> Result<Vec<u8>> {
    use xz4rust::{DICT_SIZE_MIN, DICT_SIZE_PROFILE_6, XzDecoder};

    let dict_limit = max.max(DICT_SIZE_PROFILE_6);
    let mut decoder = XzDecoder::with_alloc_dict_size(DICT_SIZE_MIN, dict_limit);
    let mut input_pos = 0usize;
    let mut out = Vec::new();
    let mut buf = [0u8; 16 * 1024];

    loop {
        if input_pos >= src.len() {
            return Err(SdJournalError::DecompressFailed {
                algo: CompressionAlgo::Xz,
                reason: "unexpected end of xz stream".to_string(),
            });
        }

        let remaining = max.saturating_sub(out.len());
        let mut overflow_probe = [0u8; 1];
        let output = if remaining == 0 {
            overflow_probe.as_mut_slice()
        } else {
            let n = remaining.min(buf.len());
            &mut buf[..n]
        };

        let result = decoder.decode(&src[input_pos..], output).map_err(|e| {
            SdJournalError::DecompressFailed {
                algo: CompressionAlgo::Xz,
                reason: e.to_string(),
            }
        })?;

        input_pos = input_pos.saturating_add(result.input_consumed());
        let produced = result.output_produced();
        if remaining == 0 && produced != 0 {
            return Err(SdJournalError::LimitExceeded {
                kind: LimitKind::DecompressedBytes,
                limit: u64::try_from(max).unwrap_or(u64::MAX),
            });
        }
        out.extend_from_slice(&output[..produced]);

        if result.is_end_of_stream() {
            if input_pos == src.len() {
                return Ok(out);
            }

            let padding_start = input_pos;
            while src.get(input_pos) == Some(&0) {
                input_pos = input_pos.saturating_add(1);
            }
            let padding_len = input_pos.saturating_sub(padding_start);
            if !padding_len.is_multiple_of(4) {
                return Err(SdJournalError::DecompressFailed {
                    algo: CompressionAlgo::Xz,
                    reason: format!(
                        "xz stream padding length is not a multiple of four: {padding_len}"
                    ),
                });
            }
            if input_pos == src.len() {
                return Ok(out);
            }
            if !src[input_pos..].starts_with(&[0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00]) {
                return Err(SdJournalError::DecompressFailed {
                    algo: CompressionAlgo::Xz,
                    reason: "trailing bytes are not an xz stream".to_string(),
                });
            }

            decoder.reset();
            continue;
        }
        if !result.made_progress() {
            return Err(SdJournalError::DecompressFailed {
                algo: CompressionAlgo::Xz,
                reason: "xz decoder made no progress".to_string(),
            });
        }
    }
}

#[cfg(not(feature = "xz"))]
pub(super) fn decompress_xz(_src: &[u8], _max: usize) -> Result<Vec<u8>> {
    Err(SdJournalError::Unsupported {
        reason: "xz support is disabled (feature xz)".to_string(),
    })
}

#[cfg(test)]
mod tests {
    #[cfg(any(feature = "lz4", feature = "zstd", feature = "xz"))]
    use super::*;

    #[cfg(feature = "lz4")]
    #[test]
    fn lz4_roundtrip_and_limit_checks() {
        let plain = b"hello from lz4";
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&(plain.len() as u64).to_le_bytes());
        encoded.extend_from_slice(&lz4_flex::block::compress(plain));

        assert_eq!(decompress_lz4(&encoded, plain.len()).unwrap(), plain);

        match decompress_lz4(&encoded, plain.len() - 1) {
            Err(SdJournalError::LimitExceeded { kind, limit }) => {
                assert_eq!(kind, LimitKind::DecompressedBytes);
                assert_eq!(limit, (plain.len() - 1) as u64);
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn lz4_rejects_short_payload() {
        match decompress_lz4(&[0u8; 8], 128) {
            Err(SdJournalError::DecompressFailed { algo, reason }) => {
                assert_eq!(algo, CompressionAlgo::Lz4);
                assert_eq!(reason, "lz4 payload too short");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn zstd_rejects_invalid_payload() {
        match decompress_zstd(b"not zstd", 128) {
            Err(SdJournalError::DecompressFailed { algo, .. }) => {
                assert_eq!(algo, CompressionAlgo::Zstd);
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn lz4_rejects_a_block_shorter_than_its_advertised_size() {
        let plain = b"MESSAGE=truncated";
        let mut encoded = ((plain.len() + 1) as u64).to_le_bytes().to_vec();
        encoded.extend_from_slice(&lz4_flex::block::compress(plain));
        assert!(matches!(
            decompress_lz4(&encoded, 128),
            Err(SdJournalError::DecompressFailed {
                algo: CompressionAlgo::Lz4,
                ..
            })
        ));
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn zstd_rejects_invalid_checksums_in_every_frame() {
        let plain = b"MESSAGE=checksummed";
        let valid = zstd_fixture(plain);
        assert_ne!(valid[4] & 4, 0, "fixture must include a content checksum");
        assert_eq!(decompress_zstd(&valid, 128).unwrap(), plain);

        let mut corrupt = valid.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        for prefix in [Vec::new(), valid] {
            let mut encoded = prefix;
            encoded.extend_from_slice(&corrupt);
            assert!(matches!(
                decompress_zstd(&encoded, 128),
                Err(SdJournalError::DecompressFailed {
                    algo: CompressionAlgo::Zstd,
                    ..
                })
            ));
        }
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn zstd_validates_declared_content_size_for_every_frame() {
        for prefix in [Vec::new(), zstd_fixture(b"first")] {
            for declared_size in [0, 4, 6] {
                let mut encoded = prefix.clone();
                // A single-segment frame declaring a different size from its five-byte raw block.
                encoded.extend_from_slice(&[
                    0x28,
                    0xb5,
                    0x2f,
                    0xfd,
                    0x20,
                    declared_size,
                    0x29,
                    0,
                    0,
                ]);
                encoded.extend_from_slice(b"hello");
                assert!(
                    matches!(
                        decompress_zstd(&encoded, 128),
                        Err(SdJournalError::DecompressFailed {
                            algo: CompressionAlgo::Zstd,
                            ..
                        })
                    ),
                    "accepted invalid declared size {declared_size}"
                );
            }
        }
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn zstd_accepts_a_frame_with_an_explicit_zero_content_size() {
        let encoded = [0x28, 0xb5, 0x2f, 0xfd, 0x20, 0, 1, 0, 0];
        assert!(decompress_zstd(&encoded, 0).unwrap().is_empty());
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn zstd_decodes_concatenated_frames_and_skips_metadata_frames() {
        let first = zstd_fixture(b"first");
        let second = zstd_fixture(b"-second");
        let mut encoded = first;
        encoded.extend_from_slice(&zstd_skippable_fixture(b"metadata"));
        encoded.extend_from_slice(&second);

        assert_eq!(decompress_zstd(&encoded, 64).unwrap(), b"first-second");
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn zstd_checks_the_window_limit_of_every_frame() {
        let mut encoded = zstd_fixture(b"first");
        // Standard magic, non-single-segment frame, and a 16 MiB window descriptor.
        encoded.extend_from_slice(&[0x28, 0xb5, 0x2f, 0xfd, 0x00, 14 << 3]);

        match decompress_zstd(&encoded, 128) {
            Err(SdJournalError::DecompressFailed { algo, reason }) => {
                assert_eq!(algo, CompressionAlgo::Zstd);
                assert!(reason.contains("frame 1 window size"), "{reason}");
                assert!(reason.contains("working-set limit"), "{reason}");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn zstd_rejects_truncated_later_frames_and_skippable_frames() {
        let mut truncated_frame = zstd_fixture(b"first");
        let mut second_frame = zstd_fixture(b"second");
        second_frame.pop();
        truncated_frame.extend_from_slice(&second_frame);
        assert!(matches!(
            decompress_zstd(&truncated_frame, 128),
            Err(SdJournalError::DecompressFailed {
                algo: CompressionAlgo::Zstd,
                ..
            })
        ));

        let mut truncated_skippable = zstd_fixture(b"first");
        truncated_skippable.extend_from_slice(&0x184d_2a50u32.to_le_bytes());
        truncated_skippable.extend_from_slice(&5u32.to_le_bytes());
        truncated_skippable.extend_from_slice(b"tiny");
        match decompress_zstd(&truncated_skippable, 128) {
            Err(SdJournalError::DecompressFailed { algo, reason }) => {
                assert_eq!(algo, CompressionAlgo::Zstd);
                assert!(reason.contains("skippable frame payload is truncated"));
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn zstd_applies_the_output_limit_across_frames() {
        let mut encoded = zstd_fixture(b"first");
        encoded.extend_from_slice(&zstd_fixture(b"-second"));

        match decompress_zstd(&encoded, 11) {
            Err(SdJournalError::LimitExceeded { kind, limit }) => {
                assert_eq!(kind, LimitKind::DecompressedBytes);
                assert_eq!(limit, 11);
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn zstd_rejects_trailing_garbage_after_a_valid_frame() {
        let mut encoded = zstd_fixture(b"valid");
        encoded.extend_from_slice(b"garbage");

        match decompress_zstd(&encoded, 128) {
            Err(SdJournalError::DecompressFailed { algo, reason }) => {
                assert_eq!(algo, CompressionAlgo::Zstd);
                assert_eq!(reason, "invalid zstd frame magic");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn zstd_rejects_oversized_window_before_decoder_allocation() {
        // Standard magic, non-single-segment frame, and a 16 MiB window descriptor.
        let encoded = [0x28, 0xb5, 0x2f, 0xfd, 0x00, 14 << 3];
        match decompress_zstd(&encoded, 128) {
            Err(SdJournalError::DecompressFailed { algo, reason }) => {
                assert_eq!(algo, CompressionAlgo::Zstd);
                assert!(reason.contains("working-set limit"), "{reason}");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[cfg(feature = "zstd")]
    fn zstd_fixture(plain: &[u8]) -> Vec<u8> {
        ruzstd::encoding::compress_to_vec(plain, ruzstd::encoding::CompressionLevel::Uncompressed)
    }

    #[cfg(feature = "zstd")]
    fn zstd_skippable_fixture(payload: &[u8]) -> Vec<u8> {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&0x184d_2a50u32.to_le_bytes());
        encoded.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        encoded.extend_from_slice(payload);
        encoded
    }

    #[cfg(feature = "xz")]
    #[test]
    fn xz_roundtrip_and_limit_checks() {
        let plain = b"hello from xz";
        let encoded = xz_fixture();

        assert_eq!(decompress_xz(encoded, plain.len()).unwrap(), plain);

        match decompress_xz(encoded, plain.len() - 1) {
            Err(SdJournalError::LimitExceeded { kind, limit }) => {
                assert_eq!(kind, LimitKind::DecompressedBytes);
                assert_eq!(limit, (plain.len() - 1) as u64);
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[cfg(feature = "xz")]
    #[test]
    fn xz_rejects_invalid_payload() {
        match decompress_xz(b"not xz", 128) {
            Err(SdJournalError::DecompressFailed { algo, .. }) => {
                assert_eq!(algo, CompressionAlgo::Xz);
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[cfg(feature = "xz")]
    #[test]
    fn xz_decodes_concatenated_streams_and_stream_padding() {
        let plain = b"hello from xz";
        let mut encoded = xz_fixture().to_vec();
        encoded.extend_from_slice(&[0; 4]);
        encoded.extend_from_slice(xz_fixture());
        encoded.extend_from_slice(&[0; 8]);

        let mut expected = plain.to_vec();
        expected.extend_from_slice(plain);
        assert_eq!(decompress_xz(&encoded, expected.len()).unwrap(), expected);
    }

    #[cfg(feature = "xz")]
    #[test]
    fn xz_rejects_trailing_garbage_and_invalid_stream_padding() {
        let mut garbage = xz_fixture().to_vec();
        garbage.extend_from_slice(b"garbage");
        match decompress_xz(&garbage, 128) {
            Err(SdJournalError::DecompressFailed { algo, reason }) => {
                assert_eq!(algo, CompressionAlgo::Xz);
                assert_eq!(reason, "trailing bytes are not an xz stream");
            }
            other => panic!("unexpected result: {other:?}"),
        }

        let mut padding = xz_fixture().to_vec();
        padding.push(0);
        match decompress_xz(&padding, 128) {
            Err(SdJournalError::DecompressFailed { algo, reason }) => {
                assert_eq!(algo, CompressionAlgo::Xz);
                assert!(reason.contains("padding length is not a multiple of four"));
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[cfg(feature = "xz")]
    fn xz_fixture() -> &'static [u8] {
        &[
            0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00, 0x00, 0x04, 0xe6, 0xd6, 0xb4, 0x46, 0x02, 0x00,
            0x21, 0x01, 0x16, 0x00, 0x00, 0x00, 0x74, 0x2f, 0xe5, 0xa3, 0x01, 0x00, 0x0c, 0x68,
            0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x66, 0x72, 0x6f, 0x6d, 0x20, 0x78, 0x7a, 0x00, 0x00,
            0x00, 0x00, 0xa5, 0xb3, 0x18, 0x76, 0x67, 0x14, 0xad, 0x57, 0x00, 0x01, 0x25, 0x0d,
            0x71, 0x19, 0xc4, 0xb6, 0x1f, 0xb6, 0xf3, 0x7d, 0x01, 0x00, 0x00, 0x00, 0x00, 0x04,
            0x59, 0x5a,
        ]
    }
}
