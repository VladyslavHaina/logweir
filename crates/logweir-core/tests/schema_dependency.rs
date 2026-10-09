//! **PROD-03.0 — the detector, row by row.** Every row states the payloads,
//! the verdict, and (for a control) the value that would make it fail.
//!
//! The payloads are the Confluent wire format as its serializers write it:
//! `0x00`, the schema id as a big-endian 32-bit integer, then the Avro binary
//! encoding, the JSON text, or (Protobuf) the message-index array — a single
//! `0x00` for the first message type, otherwise a zigzag varint count and
//! zigzag varint indexes — followed by the Protobuf encoding. The compose row
//! (`e2e/tests/schema_dependency.rs`) produces the same framing with a real
//! registry and real serializers; these rows pin the byte rules.

use logweir_core::backup_receipt::{
    BackupReceipt, ReceiptArchive, ReceiptAuth, ReceiptCovered, ReceiptEngine, ReceiptSource,
    TopicSchemaDependency,
};
use logweir_core::schema_dependency::{
    dependent_by_share, dependent_ids, dependent_sides, framed_schema_id, not_assessed, TopicTally,
    BASIS_COMPLETE, BASIS_SAMPLED, MAX_SCHEMA_ID, NOT_ASSESSED, NOT_DETECTED, REASON_NO_RECORDS,
    SCHEMA_DEPENDENT, SCHEMA_IDS_LISTED,
};
use std::collections::BTreeMap;

fn frame(id: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8];
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Avro: a record {name: "abc", amount: 42} — string length 3 (zigzag 6),
/// the bytes, then the long 42 (zigzag 84).
fn avro(id: u32) -> Vec<u8> {
    frame(id, &[0x06, b'a', b'b', b'c', 0x54])
}

fn json_schema(id: u32) -> Vec<u8> {
    frame(id, br#"{"name":"abc","amount":42}"#)
}

/// Protobuf, first message type: the single `0x00` index, then field 1 = 42.
fn protobuf_first(id: u32) -> Vec<u8> {
    frame(id, &[0x00, 0x08, 0x2A])
}

/// Protobuf, the second top-level message type: indexes `[1]` as a zigzag
/// count (1 -> 2) and a zigzag index (1 -> 2), then field 1 = 42.
fn protobuf_second(id: u32) -> Vec<u8> {
    frame(id, &[0x02, 0x02, 0x08, 0x2A])
}

/// Protobuf with an EMPTY message: nothing after the index byte. Still six
/// bytes, still framed.
fn protobuf_empty(id: u32) -> Vec<u8> {
    frame(id, &[0x00])
}

/// A record's key and value bytes, `None` for null.
type Raw = (Option<Vec<u8>>, Option<Vec<u8>>);

fn tally(records: &[Raw]) -> TopicTally {
    let mut t = TopicTally::default();
    for (k, v) in records {
        t.observe(k.as_deref(), v.as_deref());
    }
    t
}

/// A one-topic receipt carrying `entry`, counting `records` records — so every
/// detector output below is ALSO held to the format's arms 22-29.
fn receipt_with(entry: &TopicSchemaDependency, records: u64) -> BackupReceipt {
    BackupReceipt {
        format_version: "1.5.0".into(),
        run_id: "01J8Z9QK7V6M3F2R5T8W1XB0CD".into(),
        backup_id: "b".into(),
        requested_at: "2026-10-09T10:00:00Z".parse().unwrap(),
        started_at: "2026-10-09T10:00:01Z".parse().unwrap(),
        finished_at: "2026-10-09T10:00:02Z".parse().unwrap(),
        exit_code: 0,
        triggered_by: String::new(),
        source: ReceiptSource {
            cluster_id: "c".into(),
            bootstrap_servers: vec!["k:9092".into()],
            auth: ReceiptAuth {
                mode: "plaintext".into(),
                username: None,
            },
            topics: vec!["t".into()],
        },
        engine: ReceiptEngine {
            id: "oso-cli".into(),
            version: "0.23.3".into(),
            digest: "sha256:00".into(),
        },
        archive: ReceiptArchive {
            manifest_key: "m".into(),
            manifest_sha256: "sha256:11".into(),
            manifest_version_id: None,
            prefix: "p".into(),
        },
        records: BTreeMap::from([("t".to_string(), records)]),
        covered: ReceiptCovered {
            from_ms: 1,
            to_ms: 2,
        },
        config_coverage: None,
        topic_configuration: None,
        owner_detection: None,
        schema_dependency: Some(BTreeMap::from([("t".to_string(), entry.clone())])),
        generations: None,
    }
}

/// The verdict for `records`, judged completely, after the receipt's arms
/// accepted it.
fn judge(records: &[Raw]) -> TopicSchemaDependency {
    let entry = tally(records).finish(true);
    receipt_with(&entry, records.len() as u64)
        .validate_invariants()
        .unwrap_or_else(|e| panic!("the detector wrote an entry the arms refuse: {e}"));
    entry
}

fn s(text: &str) -> Option<Vec<u8>> {
    Some(text.as_bytes().to_vec())
}

#[test]
fn avro_json_schema_and_protobuf_values_are_framed_with_their_ids() {
    for (format, payload) in [
        ("avro", avro(1)),
        ("json schema", json_schema(2)),
        ("protobuf, first message", protobuf_first(3)),
        ("protobuf, second message", protobuf_second(4)),
        ("protobuf, empty message", protobuf_empty(5)),
    ] {
        let id = framed_schema_id(&payload).unwrap_or_else(|| panic!("{format} is framed"));
        let entry = judge(&[(s("k-1"), Some(payload.clone())), (s("k-2"), Some(payload))]);
        assert_eq!(entry.verdict, SCHEMA_DEPENDENT, "{format}");
        let value = entry.value.as_ref().unwrap();
        assert!(value.dependent, "{format}");
        assert_eq!(
            (value.framed, value.unframed, value.nulls),
            (2, 0, 0),
            "{format}"
        );
        assert_eq!(value.schema_ids, vec![id], "{format}");
        assert_eq!(value.schema_id_count, 1, "{format}");
        // NEGATIVE CONTROL: the string keys beside them are not framed.
        let key = entry.key.as_ref().unwrap();
        assert!(!key.dependent, "{format}");
        assert_eq!((key.framed, key.unframed), (0, 2), "{format}");
        assert_eq!(dependent_sides(&entry), vec!["value"], "{format}");
    }
}

#[test]
fn framed_keys_are_judged_on_their_own_side() {
    let entry = judge(&[
        (Some(avro(11)), s(r#"{"plain":"json"}"#)),
        (Some(protobuf_first(12)), s(r#"{"plain":"json"}"#)),
        (Some(json_schema(13)), None),
    ]);
    assert_eq!(entry.verdict, SCHEMA_DEPENDENT);
    let key = entry.key.as_ref().unwrap();
    assert!(key.dependent);
    assert_eq!(key.schema_ids, vec![11, 12, 13]);
    let value = entry.value.as_ref().unwrap();
    assert!(!value.dependent, "the JSON values are not framed");
    assert_eq!((value.unframed, value.nulls), (2, 1));
    assert_eq!(dependent_sides(&entry), vec!["key"]);
}

#[test]
fn unframed_payloads_are_not_detected() {
    let entry = judge(&[
        (s("order-1"), s(r#"{"amount":42}"#)),
        (s("order-2"), Some(vec![0x08, 0x2A])), // raw Protobuf: field tags are never 0
        (s("order-3"), Some(vec![0x06, b'a', b'b', b'c'])), // raw Avro, no framing
        (s("order-4"), Some(Vec::new())),       // empty, not null
        // Another registry's header: AWS Glue's magic byte 3, then bytes that
        // would read as a plausible Confluent id. Not Confluent framing.
        (s("order-5"), Some(vec![3, 0, 0, 0, 9, 0x42, 0x42])),
        (s("order-6"), Some(vec![1, 0, 0, 0, 7, 2])),
    ]);
    assert_eq!(entry.verdict, NOT_DETECTED);
    assert_eq!(entry.basis.as_deref(), Some(BASIS_COMPLETE));
    for side in [entry.key.as_ref().unwrap(), entry.value.as_ref().unwrap()] {
        assert!(!side.dependent);
        assert_eq!(side.framed, 0);
        assert!(side.schema_ids.is_empty());
        assert_eq!(side.schema_id_count, 0);
    }
    assert_eq!(entry.value.as_ref().unwrap().unframed, 6);
}

#[test]
fn nulls_and_tombstones_never_count_toward_the_share() {
    // A compacted topic: one framed value, eleven tombstones, null keys.
    let mut records = vec![(None, Some(avro(9)))];
    records.extend((0..11).map(|_| (None, None)));
    let entry = judge(&records);
    assert_eq!(
        entry.verdict, SCHEMA_DEPENDENT,
        "1 framed of 1 non-null value"
    );
    let value = entry.value.as_ref().unwrap();
    assert_eq!((value.framed, value.unframed, value.nulls), (1, 0, 11));
    let key = entry.key.as_ref().unwrap();
    assert_eq!((key.framed, key.unframed, key.nulls), (0, 0, 12));
    assert!(!key.dependent, "an all-null side is never dependent");
    // Every record null on both sides: judged, nothing depends.
    let entry = judge(&[(None, None), (None, None)]);
    assert_eq!(entry.verdict, NOT_DETECTED);
}

#[test]
fn short_records_starting_with_zero_are_not_framed() {
    // A big-endian 32-bit integer key (IntegerSerializer of 1), and a 5-byte
    // value: a zero byte and an id, with nothing after it.
    assert_eq!(framed_schema_id(&[0, 0, 0, 1]), None);
    assert_eq!(framed_schema_id(&[0, 0, 0, 0, 1]), None);
    let entry = judge(&[
        (Some(vec![0, 0, 0, 1]), Some(vec![0, 0, 0, 0, 1])),
        (Some(vec![0, 0, 0, 2]), Some(vec![0, 0, 0, 0, 2])),
    ]);
    assert_eq!(entry.verdict, NOT_DETECTED);
    // NEGATIVE CONTROL: one more byte makes the value framed.
    let entry = judge(&[(None, Some(vec![0, 0, 0, 0, 1, 0]))]);
    assert_eq!(entry.verdict, SCHEMA_DEPENDENT);
}

#[test]
fn random_ids_after_a_zero_byte_are_not_plausible() {
    // A zero byte then four random bytes whose first is not zero: an id at or
    // above 2^24, which no registry issued. A deterministic stream (no clock,
    // no RNG crate): a 64-bit LCG.
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = || {
        x = x
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (x >> 33) as u8
    };
    let mut records = Vec::new();
    for _ in 0..1000 {
        let mut v = vec![0u8, next() | 1];
        for _ in 0..10 {
            v.push(next());
        }
        records.push((None, Some(v)));
    }
    let entry = judge(&records);
    assert_eq!(entry.verdict, NOT_DETECTED, "{entry:?}");
    assert_eq!(entry.value.as_ref().unwrap().framed, 0);
    // Id 0 is never issued either: an 8-byte zero long.
    assert_eq!(framed_schema_id(&[0; 8]), None);
    // The bounds themselves.
    assert_eq!(
        framed_schema_id(&frame(MAX_SCHEMA_ID, &[1])),
        Some(MAX_SCHEMA_ID)
    );
    assert_eq!(framed_schema_id(&frame(MAX_SCHEMA_ID + 1, &[1])), None);
    assert_eq!(framed_schema_id(&frame(1, &[1])), Some(1));
}

#[test]
fn framing_shaped_records_below_the_threshold_do_not_flag_a_side() {
    // 9 framing-shaped values among 100 non-null ones: below one in ten.
    let mut records: Vec<_> = (0..9).map(|i| (None, Some(avro(i + 1)))).collect();
    records.extend((0..91).map(|_| (None, s("raw"))));
    let entry = judge(&records);
    assert_eq!(entry.verdict, NOT_DETECTED);
    let value = entry.value.as_ref().unwrap();
    assert!(!value.dependent);
    assert_eq!((value.framed, value.unframed), (9, 91));
    // The ids are still recorded: what was seen is evidence either way.
    assert_eq!(value.schema_id_count, 9);
    // NEGATIVE CONTROL: the tenth framed record (10 of 100) crosses it.
    let mut records: Vec<_> = (0..10).map(|i| (None, Some(avro(i + 1)))).collect();
    records.extend((0..90).map(|_| (None, s("raw"))));
    assert_eq!(judge(&records).verdict, SCHEMA_DEPENDENT);
}

#[test]
fn the_threshold_is_exactly_one_in_ten_and_at_least_one() {
    // Each line kills a mutant of `dependent_by_share`: the denominator moved
    // to 9 or 11, `>=` made `>`, or the at-least-one clause dropped.
    assert!(dependent_by_share(1, 9), "1 of 10");
    assert!(!dependent_by_share(1, 10), "1 of 11");
    assert!(dependent_by_share(2, 18), "2 of 20");
    assert!(!dependent_by_share(2, 19), "2 of 21");
    assert!(!dependent_by_share(0, 0), "nothing framed");
    assert!(dependent_by_share(1, 0), "1 of 1");
}

#[test]
fn a_mixed_topic_is_dependent_where_its_records_are() {
    // A topic migrating from JSON to Avro: 30 of 100 values framed, under two
    // schema versions; string keys throughout.
    let mut records: Vec<_> = (0..20)
        .map(|i| (s(&format!("k{i}")), Some(avro(41))))
        .collect();
    records.extend((0..10).map(|i| (s(&format!("j{i}")), Some(avro(42)))));
    records.extend((0..70).map(|i| (s(&format!("m{i}")), s(r#"{"legacy":true}"#))));
    let entry = judge(&records);
    assert_eq!(entry.verdict, SCHEMA_DEPENDENT);
    let value = entry.value.as_ref().unwrap();
    assert_eq!((value.framed, value.unframed), (30, 70));
    assert_eq!(value.schema_ids, vec![41, 42]);
    assert_eq!(dependent_ids(&entry), (vec![41, 42], false));
}

#[test]
fn an_empty_topic_is_not_assessed() {
    let entry = TopicTally::default().finish(true);
    assert_eq!(entry, not_assessed(REASON_NO_RECORDS));
    assert_eq!(entry.verdict, NOT_ASSESSED);
    receipt_with(&entry, 0)
        .validate_invariants()
        .expect("noRecords beside a count of 0");
    // NEGATIVE CONTROL: the arms refuse "noRecords" for a topic with records.
    assert!(receipt_with(&entry, 3).validate_invariants().is_err());
}

#[test]
fn more_than_sixteen_ids_list_the_smallest_and_count_them_all() {
    let records: Vec<_> = (0..40u32)
        .map(|i| (None, Some(avro(1000 - (i % 20)))))
        .collect();
    let entry = judge(&records);
    let value = entry.value.as_ref().unwrap();
    assert_eq!(value.schema_id_count, 20);
    assert_eq!(value.schema_ids.len(), SCHEMA_IDS_LISTED);
    assert_eq!(value.schema_ids, (981..=996).collect::<Vec<u32>>());
    let (ids, omitted) = dependent_ids(&entry);
    assert_eq!(ids.len(), SCHEMA_IDS_LISTED);
    assert!(omitted, "4 ids were not listed");
}

#[test]
fn a_sample_says_sampled_and_judges_no_more_than_the_receipt_counts() {
    let records = vec![(None, Some(avro(5))); 3];
    let entry = tally(&records).finish(false);
    assert_eq!(entry.basis.as_deref(), Some(BASIS_SAMPLED));
    receipt_with(&entry, 10)
        .validate_invariants()
        .expect("3 sampled of 10");
    // NEGATIVE CONTROL: the same tally claimed complete over 10 is refused.
    let complete = tally(&records).finish(true);
    assert!(receipt_with(&complete, 10).validate_invariants().is_err());
}

/// **The residual the contract states**: a big-endian 64-bit key holding an
/// epoch-millisecond timestamp reads as framed. Pinned so the documented
/// limit is the code's, not a guess.
#[test]
fn the_stated_residual_epoch_millisecond_long_keys_read_as_framed() {
    let key = 1_760_000_000_000_i64.to_be_bytes();
    assert_eq!(key[0], 0);
    assert!(framed_schema_id(&key).is_some());
}

#[test]
fn dependent_ids_merges_only_the_dependent_sides() {
    // Keys framed (dependent), values framed below the share (not).
    let mut records: Vec<_> = (0..10).map(|_| (Some(avro(7)), s("raw"))).collect();
    records[0].1 = Some(avro(99));
    let entry = judge(&records);
    assert!(entry.key.as_ref().unwrap().dependent);
    assert!(
        entry.value.as_ref().unwrap().dependent,
        "1 of 10 values is dependent"
    );
    let (ids, omitted) = dependent_ids(&entry);
    assert_eq!(ids, vec![7, 99]);
    assert!(!omitted);
    records.push((Some(avro(7)), s("raw")));
    let entry = judge(&records);
    assert!(!entry.value.as_ref().unwrap().dependent, "1 of 11 is not");
    assert_eq!(dependent_ids(&entry), (vec![7], false));
}
