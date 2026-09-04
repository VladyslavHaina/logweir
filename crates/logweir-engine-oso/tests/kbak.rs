use logweir_engine_oso::kbak::decode_segment;

const HEADER_SIZE: usize = 32;
const FOOTER_SIZE: usize = 8;

/// Recomputes the footer CRC32 over `bytes[..len-FOOTER_SIZE]` and writes it
/// back into the footer, so a deliberately corrupted fixture is still a
/// CRC-valid KBAK container — the corruption is caught by whatever check is
/// actually under test, not rejected earlier as a CRC mismatch. Mirrors
/// `examples/mint_segments.rs`'s own footer-writing step.
fn recompute_crc(bytes: &mut [u8]) {
    let tail = bytes.len() - FOOTER_SIZE;
    let crc = crc32fast::hash(&bytes[..tail]);
    bytes[tail..tail + 4].copy_from_slice(&crc.to_le_bytes());
}

#[test]
fn header_magic_and_version_are_checked() {
    let mut bad = b"NOPE".to_vec();
    bad.resize(32, 0);
    assert!(decode_segment(&bad).is_err());
}

#[test]
fn uncompressed_segment_decodes_every_record() {
    let bytes = std::fs::read("../../e2e/fixtures/segments/none.kbak").unwrap();
    let recs = decode_segment(&bytes).unwrap();
    // 4, not 3: record 3 is dedicated to the null-key/null-value/null-header
    // coverage added alongside the other Task 9 fixes (see
    // `a_null_key_a_null_value_and_a_null_header_value_all_decode_as_none`).
    assert_eq!(recs.len(), 4);
    assert_eq!(recs[0].offset, 100);
    assert_eq!(recs[0].key.as_deref(), Some(&b"k0"[..]));
    assert_eq!(recs[2].headers[0].0, "x-original-offset");
}

#[test]
fn zstd_and_lz4_segments_decode_to_the_same_records() {
    let a =
        decode_segment(&std::fs::read("../../e2e/fixtures/segments/none.kbak").unwrap()).unwrap();
    for f in ["zstd", "lz4"] {
        let b = decode_segment(
            &std::fs::read(format!("../../e2e/fixtures/segments/{f}.kbak")).unwrap(),
        )
        .unwrap();
        assert_eq!(a.len(), b.len(), "{f}");
        for (x, y) in a.iter().zip(b.iter()) {
            assert_eq!(x.offset, y.offset);
            assert_eq!(x.key, y.key);
            assert_eq!(x.value, y.value);
        }
    }
}

#[test]
fn a_truncated_segment_is_an_error_not_a_partial_read() {
    let mut bytes = std::fs::read("../../e2e/fixtures/segments/none.kbak").unwrap();
    bytes.truncate(bytes.len() / 2);
    assert!(decode_segment(&bytes).is_err());
}

#[test]
fn a_legacy_json_segment_returns_unsupported_not_a_panic() {
    let bytes = std::fs::read("../../e2e/fixtures/segments/legacy.json").unwrap();
    match decode_segment(&bytes) {
        Err(logweir_core::engine::EngineError::Unsupported(_)) => {}
        other => panic!("legacy JSON must degrade to Unsupported, got {other:?}"),
    }
}

#[test]
fn the_fingerprint_is_order_stable_over_headers() {
    use logweir_kafka::fingerprint::record_fingerprint;
    let h1 = vec![
        ("b".to_string(), Some(b"2".to_vec())),
        ("a".to_string(), Some(b"1".to_vec())),
    ];
    let h2 = vec![
        ("a".to_string(), Some(b"1".to_vec())),
        ("b".to_string(), Some(b"2".to_vec())),
    ];
    assert_eq!(
        record_fingerprint(Some(b"k"), Some(b"v"), &h1, 1_700_000_000_000),
        record_fingerprint(Some(b"k"), Some(b"v"), &h2, 1_700_000_000_000)
    );
}

#[test]
fn a_null_value_and_an_empty_value_fingerprint_differently() {
    use logweir_kafka::fingerprint::record_fingerprint;
    assert_ne!(
        record_fingerprint(Some(b"k"), None, &[], 1),
        record_fingerprint(Some(b"k"), Some(b""), &[], 1)
    );
}

/// Not in the brief's own test list, added per the task's scope note: "a
/// truncated segment, a wrong magic value, a bad length prefix and an
/// unsupported version must each fail with a distinguishable error." The
/// brief's `header_magic_and_version_are_checked` only exercises the magic
/// check (bytes that are not "KBAK" never reach the version branch at all),
/// so it does not actually cover an unsupported *version* on an otherwise
/// well-formed header. This constructs one directly.
#[test]
fn an_unsupported_version_is_rejected_distinctly_from_a_bad_magic() {
    let mut bytes = vec![0u8; 40]; // HEADER_SIZE (32) + FOOTER_SIZE (8), the minimum decode_segment will look at.
    bytes[0..4].copy_from_slice(b"KBAK");
    bytes[4] = 2; // a version this decoder has never supported.
    let e = decode_segment(&bytes).unwrap_err();
    let msg = format!("{e}");
    assert!(
        msg.contains("version"),
        "expected a version-specific error, got: {msg}"
    );
}

/// A bad *record* length prefix, not just a short buffer: the segment's CRC32
/// is recomputed over the corrupted bytes so this fixture is internally
/// consistent (a valid KBAK container, header and footer both intact) and the
/// decoder is forced past the integrity check into record parsing, where the
/// corrupted `key_len` must fail cleanly rather than reading garbage or a
/// neighboring field's bytes as key content.
#[test]
fn a_corrupted_key_length_prefix_fails_in_record_parsing_not_as_a_crc_mismatch() {
    let mut bytes = std::fs::read("../../e2e/fixtures/segments/none.kbak").unwrap();
    // Layout (format.rs:27-42): body starts at HEADER_SIZE=32.
    // total_len(4) + timestamp(8) + offset(8) = 20 bytes into the first
    // record, so key_len sits at byte 32 + 20 = 52 in this uncompressed fixture.
    let key_len_offset = 52;
    assert_eq!(
        i32::from_le_bytes(
            bytes[key_len_offset..key_len_offset + 4]
                .try_into()
                .unwrap()
        ),
        2,
        "fixture layout drifted: expected key_len==2 (\"k0\") at byte 52"
    );
    // A length that claims far more bytes than remain in the record stream.
    bytes[key_len_offset..key_len_offset + 4].copy_from_slice(&9_999_i32.to_le_bytes());
    recompute_crc(&mut bytes);

    let e = decode_segment(&bytes).unwrap_err();
    let msg = format!("{e}");
    assert!(
        !msg.contains("CRC32"),
        "a corrupted length prefix must not be mistaken for a CRC failure: {msg}"
    );
}

/// FIX 1 (review of Task 9): `record_count` is read from a CRC-covered
/// header, but CRC32 is unkeyed — anyone able to write into the archive
/// bucket can produce a CRC-valid header naming any `record_count`.
/// `u64::MAX` used to panic `Vec::with_capacity` with "capacity overflow";
/// this must now fail cleanly with a distinguishable error instead.
#[test]
fn a_record_count_far_exceeding_the_body_size_is_rejected_not_allocated() {
    let mut bytes = std::fs::read("../../e2e/fixtures/segments/none.kbak").unwrap();
    bytes[8..16].copy_from_slice(&u64::MAX.to_le_bytes());
    recompute_crc(&mut bytes);

    let e = decode_segment(&bytes).unwrap_err();
    let msg = format!("{e}");
    assert!(
        msg.contains("record_count"),
        "expected a record_count-specific error, got: {msg}"
    );
}

/// The same class of bug as above, at a plausible-but-still-impossible scale:
/// a `record_count` that would need far more decompressed bytes than the
/// segment actually decompresses to (previously ~9 GiB for ~10^8 records).
#[test]
fn a_plausible_but_impossible_record_count_is_rejected() {
    let mut bytes = std::fs::read("../../e2e/fixtures/segments/none.kbak").unwrap();
    bytes[8..16].copy_from_slice(&100_000_000u64.to_le_bytes());
    recompute_crc(&mut bytes);

    let e = decode_segment(&bytes).unwrap_err();
    assert!(format!("{e}").contains("record_count"));
}

/// FIX 1, the lz4 half: `lz4_flex::block::decompress_size_prepended` reads
/// its own 4-byte declared output size and allocates a buffer of exactly that
/// size before decompressing into it. An attacker-controlled prefix here is a
/// multi-gigabyte allocation paid for by four bytes of input; it must be
/// capped before ever reaching that call.
#[test]
fn an_oversized_lz4_declared_size_is_rejected_not_allocated() {
    let mut bytes = std::fs::read("../../e2e/fixtures/segments/lz4.kbak").unwrap();
    // The lz4-compressed body starts right after the 32-byte header; its
    // first 4 bytes are lz4_flex's own size-prepended declared output length.
    bytes[HEADER_SIZE..HEADER_SIZE + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    recompute_crc(&mut bytes);

    let e = decode_segment(&bytes).unwrap_err();
    let msg = format!("{e}");
    assert!(
        msg.contains("lz4") && msg.contains("cap"),
        "expected an lz4 declared-size-cap error, got: {msg}"
    );
}

/// FIX 2 (review of Task 9): `total_len` is each record's own authoritative
/// frame boundary (format.rs:27-42), not just a number to skip past. A record
/// whose declared `total_len` disagrees with what its fields actually consume
/// must fail AT that record, distinguishably, rather than silently
/// desynchronizing every record after it.
#[test]
fn a_total_len_that_disagrees_with_its_record_content_is_rejected() {
    let mut bytes = std::fs::read("../../e2e/fixtures/segments/none.kbak").unwrap();
    // The first record's total_len prefix sits at byte HEADER_SIZE (32).
    let total_len_offset = HEADER_SIZE;
    let declared = u32::from_le_bytes(
        bytes[total_len_offset..total_len_offset + 4]
            .try_into()
            .unwrap(),
    );
    // Claim one byte more than the record's fields actually use. There is
    // plenty of body left (three more records follow), so this is caught as
    // a framing disagreement, not a truncation.
    bytes[total_len_offset..total_len_offset + 4].copy_from_slice(&(declared + 1).to_le_bytes());
    recompute_crc(&mut bytes);

    let e = decode_segment(&bytes).unwrap_err();
    let msg = format!("{e}");
    assert!(
        msg.contains("total_len mismatch"),
        "expected a total_len mismatch error, got: {msg}"
    );
}

/// FIX 3 (review of Task 9): upstream picks the legacy-JSON codec from the
/// object KEY's extension (restore/helpers.rs:22-38), which decode_segment
/// never receives — but the zstd frame magic (28 b5 2f fd) is unambiguous on
/// its own, so a whole-object zstd-compressed legacy segment must be reported
/// as its own distinguishable outcome rather than "bad magic: not a KBAK
/// segment" (which reads as corruption, the wrong diagnosis for a segment
/// that is merely in an older format).
#[test]
fn a_zstd_compressed_legacy_segment_is_distinguished_from_a_corrupt_one() {
    let mut bytes = vec![0x28, 0xB5, 0x2F, 0xFD];
    bytes.extend_from_slice(b"not a real zstd frame, just the magic for this test");
    let e = decode_segment(&bytes).unwrap_err();
    let msg = format!("{e}");
    assert!(
        msg.to_lowercase().contains("zstd") && msg.to_lowercase().contains("legacy"),
        "expected a zstd-compressed-legacy-specific error, got: {msg}"
    );
}

/// An lz4-compressed legacy segment (no KBAK magic, no zstd magic — lz4_flex's
/// block format has no magic of its own) still falls into the generic bad-
/// magic branch, but that branch's message must not claim confident
/// corruption; it should name the ambiguity instead.
#[test]
fn the_generic_bad_magic_message_names_the_lz4_legacy_possibility() {
    let mut bytes = vec![0u8; 4];
    bytes.extend_from_slice(&[9u8; 40]); // arbitrary, not "KBAK", not JSON, not zstd-magic.
    let e = decode_segment(&bytes).unwrap_err();
    let msg = format!("{e}");
    assert!(
        msg.to_lowercase().contains("lz4"),
        "expected the bad-magic message to name the lz4-legacy ambiguity, got: {msg}"
    );
}

/// FIX 4 (review of Task 9): the null-vs-empty distinction the fingerprint
/// layer already guarantees (see `a_null_value_and_an_empty_value_fingerprint_differently`)
/// must actually reach the decoder. Record 3 in every fixture (added by this
/// fix) carries a null key, a null value, and a header whose value is null —
/// upstream tracks this as issue #155 with its own dedicated round-trip
/// assertion.
#[test]
fn a_null_key_a_null_value_and_a_null_header_value_all_decode_as_none() {
    let bytes = std::fs::read("../../e2e/fixtures/segments/none.kbak").unwrap();
    let recs = decode_segment(&bytes).unwrap();
    let null_rec = &recs[3];
    assert_eq!(null_rec.offset, 103);
    assert_eq!(null_rec.key, None, "key must decode as None, not empty");
    assert_eq!(null_rec.value, None, "value must decode as None, not empty");
    assert_eq!(null_rec.headers.len(), 1);
    assert_eq!(null_rec.headers[0].0, "x-null-header");
    assert_eq!(
        null_rec.headers[0].1, None,
        "header value must decode as None, not empty"
    );
    // And the contrast case: record 0's key/value are real, non-null bytes —
    // this is what None is being distinguished FROM.
    assert!(recs[0].key.is_some());
    assert!(recs[0].value.is_some());
}

/// FIX 5 (review of Task 9): upstream writes `x-original-offset` and
/// `x-original-timestamp` as little-endian i64 — each record's OWN offset and
/// timestamp, not a shared ASCII constant [VERIFIED offset_headers.rs:19-22,
/// config.rs:447-448, backup/engine.rs:2140-2147]. decode_segment treats
/// header values as opaque bytes either way, but the fixture must match the
/// real engine's encoding for Task 12 to be able to parse it.
#[test]
fn x_original_offset_and_timestamp_headers_are_little_endian_i64_matching_the_record() {
    let bytes = std::fs::read("../../e2e/fixtures/segments/none.kbak").unwrap();
    let recs = decode_segment(&bytes).unwrap();
    for rec in &recs[0..3] {
        let (_, offset_val) = rec
            .headers
            .iter()
            .find(|(k, _)| k == "x-original-offset")
            .expect("x-original-offset header present");
        let offset_bytes: [u8; 8] = offset_val.as_deref().unwrap().try_into().unwrap();
        assert_eq!(i64::from_le_bytes(offset_bytes), rec.offset);

        let (_, ts_val) = rec
            .headers
            .iter()
            .find(|(k, _)| k == "x-original-timestamp")
            .expect("x-original-timestamp header present");
        let ts_bytes: [u8; 8] = ts_val.as_deref().unwrap().try_into().unwrap();
        assert_eq!(i64::from_le_bytes(ts_bytes), rec.timestamp);
    }
}
