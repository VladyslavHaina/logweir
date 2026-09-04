use logweir_engine_oso::kbak::decode_segment;

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
    assert_eq!(recs.len(), 3);
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

    // Recompute the footer CRC over the corrupted bytes so the corruption is
    // caught by record parsing, not rejected earlier as a CRC mismatch.
    let tail = bytes.len() - 8; // FOOTER_SIZE
    let crc = crc32fast::hash(&bytes[..tail]);
    bytes[tail..tail + 4].copy_from_slice(&crc.to_le_bytes());

    let e = decode_segment(&bytes).unwrap_err();
    let msg = format!("{e}");
    assert!(
        !msg.contains("CRC32"),
        "a corrupted length prefix must not be mistaken for a CRC failure: {msg}"
    );
}
