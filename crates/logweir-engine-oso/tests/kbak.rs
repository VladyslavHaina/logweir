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
    // 5, not 3: record 3 is dedicated to the null-key/null-value/null-header
    // coverage (see `a_null_key_a_null_value_and_a_null_header_value_all_decode_as_none`)
    // and record 4 to its empty-but-present counterpart (see
    // `a_null_field_and_its_empty_counterpart_decode_distinctly_at_every_position`).
    assert_eq!(recs.len(), 5);
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

/// The other half of FIX 4 (second review round): the null case above must
/// also be contrasted against its EMPTY counterpart, not only against a real
/// non-empty value — `key_len = 0` and `key_len = -1` are different bytes on
/// the wire, and conflating "absent" with "empty" is exactly the property
/// upstream regressed on in issue #155. Record 4 (added alongside record 3,
/// not replacing it) carries a zero-length but PRESENT key, value, and
/// header value. This asserts, at each of the three positions, that record 3
/// decodes as `None` while record 4 decodes as `Some` of an empty `Vec`.
#[test]
fn a_null_field_and_its_empty_counterpart_decode_distinctly_at_every_position() {
    let bytes = std::fs::read("../../e2e/fixtures/segments/none.kbak").unwrap();
    let recs = decode_segment(&bytes).unwrap();
    let null_rec = &recs[3];
    let empty_rec = &recs[4];

    // Position 1: key. `key_len == -1` vs `key_len == 0`.
    assert_eq!(null_rec.key, None, "null key must be None");
    assert_eq!(
        empty_rec.key,
        Some(Vec::new()),
        "empty key must be Some(empty), not None"
    );

    // Position 2: value. `value_len == -1` vs `value_len == 0`.
    assert_eq!(null_rec.value, None, "null value must be None");
    assert_eq!(
        empty_rec.value,
        Some(Vec::new()),
        "empty value must be Some(empty), not None"
    );

    // Position 3: header value. The header KEY is never optional in this
    // format — only its value is — so both records carry exactly one header,
    // differing only in that header's value.
    assert_eq!(null_rec.headers[0].0, "x-null-header");
    assert_eq!(
        null_rec.headers[0].1, None,
        "null header value must be None"
    );
    assert_eq!(empty_rec.headers[0].0, "x-empty-header");
    assert_eq!(
        empty_rec.headers[0].1,
        Some(Vec::new()),
        "empty header value must be Some(empty), not None"
    );
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

// ---------------------------------------------------------------------------
// The upstream fixture pair. `e2e/fixtures/segments/upstream-0.21.0.kbak` and
// `e2e/fixtures/manifests/0.21.json` are the only fixtures in this repo that
// are BYTES THE PINNED UPSTREAM ENGINE ACTUALLY WROTE, refreshed by
// `scripts/e2e-seed.sh` against a live archive. Everything else under
// e2e/fixtures/segments/ is minted by `examples/mint_segments.rs`, which only
// ever proves this decoder is self-consistent.
//
// These two tests are deliberately NOT `#[cfg(feature = "e2e")]`: they need no
// Docker, and their whole point is that the DEFAULT `cargo test` notices if the
// committed fixtures are garbage, are from two different seed runs, or are
// silently swapped for self-encoded ones. Without them nothing in CI would.
//
// If a re-seed legitimately changes the shape (a different RECORDS_PER_TOPIC, a
// different key format, a different compression), update the constants here in
// the same commit as the fixtures — do not delete the assertions.

const UPSTREAM_SEGMENT: &str = "../../e2e/fixtures/segments/upstream-0.21.0.kbak";
const UPSTREAM_MANIFEST: &str = "../../e2e/fixtures/manifests/0.21.json";

/// `scripts/e2e-seed.sh` produces 1000 keys `orders-000001..orders-001000` into
/// a 3-partition topic and copies the sorted-first segment, which is always
/// `orders/partition=0`. Kafka's default partitioner is murmur2 over the key
/// bytes, so that partition's membership — and therefore this count — is stable
/// across runs. Two independent seeds (implementer and reviewer) both produced
/// 338.
const UPSTREAM_RECORDS: usize = 338;

#[test]
fn the_upstream_segment_is_a_real_zstd_kbak_container() {
    let bytes = std::fs::read(UPSTREAM_SEGMENT).unwrap();

    // Header byte 4 is the format version, byte 5 the compression codec.
    // Asserting codec 1 (zstd) is what makes this fixture impossible to satisfy
    // with a copy of `none.kbak` (codec 0) or a self-encoded uncompressed
    // container: the bytes have to have gone through upstream's zstd writer.
    assert_eq!(&bytes[0..4], b"KBAK", "not a KBAK container");
    assert_eq!(bytes[4], 1, "unexpected KBAK format version");
    assert_eq!(
        bytes[5], 1,
        "compression codec must be zstd (1), the codec backup-drill.yaml asks for"
    );

    let recs = decode_segment(&bytes).expect("the real upstream segment must decode");
    assert_eq!(
        recs.len(),
        UPSTREAM_RECORDS,
        "upstream-0.21.0.kbak no longer holds {UPSTREAM_RECORDS} records; re-seed and update the constant deliberately"
    );

    // Contiguous, ascending, starting at 0 — a partition segment written from
    // the beginning of the log. A truncated or spliced file fails here.
    for (i, rec) in recs.iter().enumerate() {
        assert_eq!(rec.offset, i as i64, "offsets are not contiguous from 0");
    }

    // Upstream sets `include_offset_headers=true` by default, and Logweir's
    // whole header-based offset recovery depends on those two headers being
    // present on every archived record. Assert it on ALL of them, not a sample.
    for rec in &recs {
        let (_, off) = rec
            .headers
            .iter()
            .find(|(k, _)| k == "x-original-offset")
            .unwrap_or_else(|| panic!("record {} has no x-original-offset", rec.offset));
        let off: [u8; 8] = off.as_deref().unwrap().try_into().unwrap();
        assert_eq!(i64::from_le_bytes(off), rec.offset);

        let (_, ts) = rec
            .headers
            .iter()
            .find(|(k, _)| k == "x-original-timestamp")
            .unwrap_or_else(|| panic!("record {} has no x-original-timestamp", rec.offset));
        let ts: [u8; 8] = ts.as_deref().unwrap().try_into().unwrap();
        assert_eq!(i64::from_le_bytes(ts), rec.timestamp);
    }

    // The payloads are the seed script's own, so key and value have to agree —
    // which is what catches a fixture refreshed from some other topic or run.
    for rec in &recs {
        let key = String::from_utf8(rec.key.clone().expect("every record has a key")).unwrap();
        let id: u32 = key
            .strip_prefix("orders-")
            .unwrap_or_else(|| panic!("unexpected key {key}"))
            .parse()
            .unwrap();
        let value =
            String::from_utf8(rec.value.clone().expect("every record has a value")).unwrap();
        assert_eq!(value, format!("{{\"id\":{id},\"topic\":\"orders\"}}"));
    }
}

/// The seed writes both fixtures from ONE archive, and only after cross-checking
/// them. This is the standing version of that check: it fails if the two files
/// ever come from different runs, or if either is corrupted after the fact.
#[test]
fn the_upstream_fixture_pair_describes_itself() {
    use logweir_engine_oso::vendored::manifest::BackupManifest;

    let bytes = std::fs::read(UPSTREAM_SEGMENT).unwrap();
    let raw = std::fs::read_to_string(UPSTREAM_MANIFEST).unwrap();
    let m: BackupManifest = serde_json::from_str(&raw).expect("the real upstream manifest parses");

    let all: Vec<_> = m
        .topics
        .iter()
        .flat_map(|t| t.partitions.iter().flat_map(|p| p.segments.iter()))
        .collect();
    assert_eq!(all.len(), 6, "the archive should describe 6 segments");
    assert_eq!(
        all.iter().map(|s| s.record_count).sum::<i64>(),
        2000,
        "the archive should describe every record the seed produced"
    );
    for s in &all {
        assert!(
            !s.sha256.is_empty(),
            "segment {} has no sha256; this is not a >=0.21 manifest",
            s.key
        );
    }

    // Find the entry for the segment the seed always copies: sorted-first, i.e.
    // orders/partition=0. Located by content, not by index, so a reordered
    // manifest does not silently pass.
    let entry = all
        .iter()
        .find(|s| s.key.contains("/orders/partition=0/"))
        .expect("manifest describes orders/partition=0");

    assert_eq!(
        logweir_core::ids::sha256_hex(&bytes),
        entry.sha256,
        "upstream-0.21.0.kbak and 0.21.json are from DIFFERENT archives (or one is corrupt)"
    );
    assert_eq!(
        bytes.len() as u64,
        entry.compressed_size,
        "segment file size disagrees with the manifest's compressed_size"
    );

    let recs = decode_segment(&bytes).unwrap();
    assert_eq!(recs.len() as i64, entry.record_count);
    assert_eq!(recs[0].offset, entry.start_offset);
    assert_eq!(recs[recs.len() - 1].offset, entry.end_offset);
}
