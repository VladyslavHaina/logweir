//! **PROD-03.0 — schema dependency at backup, over real `.kbak` bytes.**
//!
//! `logweir::backup::schema_dependency::detect` reads a bounded sample of each
//! named topic's archived segments, decodes them with Logweir's own decoder
//! and judges the keys and values. These rows hand it segments encoded in the
//! engine's KBAK v1 format (`fixtures::kbak_segment`) through a
//! [`SegmentSource`] double that COUNTS its reads, so the sample bound is a
//! measured fact and not a comment. In process: no socket, no subprocess, and
//! no schema registry anywhere.
//!
//! The end-to-end row (`a_backup_run_signs_a_1_5_0_receipt_naming_the_framed_topic`)
//! runs `execute_with` over an in-memory archive whose segments the double's
//! manifest names, and reads the signed receipt back.

mod fixtures;

use logweir::backup::schema_dependency::{
    detect, detect_within, DetectionLimits, SegmentSource, MAX_DECOMPRESSED_BYTES,
    MAX_SEGMENT_BYTES, TIME_BUDGET,
};
use logweir_core::backup_receipt::BackupReceipt;
use logweir_core::engine::*;
use logweir_core::schema_dependency::{
    BASIS_COMPLETE, BASIS_SAMPLED, NOT_ASSESSED, NOT_DETECTED, REASON_NO_RECORDS,
    REASON_SEGMENT_TOO_LARGE, REASON_SEGMENT_UNREADABLE, REASON_TIME_BUDGET, SAMPLE_PARTITIONS,
    SAMPLE_RECORDS_PER_END, SCHEMA_DEPENDENT,
};
use logweir_kafka::reader::ConsumedRecord;
use std::cell::RefCell;
use std::collections::BTreeMap;

/// Confluent framing: 0x00, the id big-endian, then an Avro record body.
fn framed(id: u32) -> Vec<u8> {
    let mut v = vec![0u8];
    v.extend_from_slice(&id.to_be_bytes());
    v.extend_from_slice(&[0x06, b'a', b'b', b'c', 0x54]);
    v
}

fn plain(i: i64) -> Vec<u8> {
    format!("{{\"n\":{i}}}").into_bytes()
}

fn records(
    first_offset: i64,
    n: i64,
    value: impl Fn(i64) -> Option<Vec<u8>>,
) -> Vec<ConsumedRecord> {
    (first_offset..first_offset + n)
        .map(|o| ConsumedRecord {
            partition: 0,
            offset: o,
            timestamp_ms: 1_760_000_000_000 + o,
            key: Some(format!("k{o}").into_bytes()),
            value: value(o),
            headers: vec![],
        })
        .collect()
}

/// An in-memory segment store that counts which keys were read.
#[derive(Default)]
struct Segments {
    objects: BTreeMap<String, Vec<u8>>,
    reads: RefCell<Vec<String>>,
}

impl SegmentSource for Segments {
    fn segment_bounded(&self, key: &str, max_bytes: u64) -> Result<Option<Vec<u8>>, String> {
        let bytes = self
            .objects
            .get(key)
            .ok_or_else(|| format!("{key}: not found"))?;
        // The store's size check comes BEFORE the read: an object over the
        // cap is never fetched, so it is never counted as read.
        if bytes.len() as u64 > max_bytes {
            return Ok(None);
        }
        self.reads.borrow_mut().push(key.to_string());
        Ok(Some(bytes.clone()))
    }
}

impl Segments {
    /// Encodes `recs` as one segment at `key` and returns its manifest facts.
    fn put(&mut self, key: &str, recs: &[ConsumedRecord]) -> SegmentFacts {
        self.objects
            .insert(key.to_string(), fixtures::kbak_segment(recs));
        SegmentFacts {
            key: key.to_string(),
            start_offset: recs.first().map_or(0, |r| r.offset),
            end_offset: recs.last().map_or(0, |r| r.offset),
            start_timestamp: recs.first().map_or(0, |r| r.timestamp_ms),
            end_timestamp: recs.last().map_or(0, |r| r.timestamp_ms),
            record_count: recs.len() as i64,
            sha256: String::new(),
            uploaded_at: 0,
        }
    }
    fn reads(&self) -> Vec<String> {
        self.reads.borrow().clone()
    }
}

fn partition(id: i32, segments: Vec<SegmentFacts>) -> PartitionFacts {
    PartitionFacts {
        partition_id: id,
        segments,
        gaps: vec![],
        pruned: vec![],
    }
}

fn topic(name: &str, partitions: Vec<PartitionFacts>) -> TopicFacts {
    TopicFacts {
        name: name.into(),
        original_partition_count: Some(partitions.len() as i32),
        source_replication_factor: Some(1),
        configurations: BTreeMap::new(),
        partitions,
    }
}

fn set(topics: Vec<TopicFacts>) -> BackupSetFacts {
    BackupSetFacts {
        backup_id: "b".into(),
        created_at: "2026-10-09T00:00:00Z".parse().unwrap(),
        source_cluster_id: None,
        manifest_sha256: "sha256:00".into(),
        manifest_version_id: None,
        consumer_group_snapshot_sha256: None,
        topics,
    }
}

fn names(n: &[&str]) -> Vec<String> {
    n.iter().map(|s| s.to_string()).collect()
}

#[test]
fn every_named_topic_gets_one_verdict_from_its_bytes() {
    let mut s = Segments::default();
    let avro = s.put("t/avro/0/0", &records(0, 5, |_| Some(framed(42))));
    let json = s.put("t/json/0/0", &records(0, 5, |o| Some(plain(o))));
    // A compacted topic: framed values and tombstones.
    let compacted = s.put(
        "t/compacted/0/0",
        &records(0, 6, |o| (o % 2 == 0).then(|| framed(7))),
    );
    let archive = set(vec![
        topic("avro", vec![partition(0, vec![avro])]),
        topic("json", vec![partition(0, vec![json])]),
        topic("compacted", vec![partition(0, vec![compacted])]),
        topic("empty", vec![partition(0, vec![])]),
    ]);
    let got = detect(
        &archive,
        &names(&["avro", "json", "compacted", "empty", "absent"]),
        &s,
    );
    assert_eq!(
        got.keys().cloned().collect::<Vec<_>>(),
        names(&["absent", "avro", "compacted", "empty", "json"]),
        "one entry per NAMED topic, the archive's or not"
    );
    let avro = &got["avro"];
    assert_eq!(avro.verdict, SCHEMA_DEPENDENT);
    assert_eq!(avro.basis.as_deref(), Some(BASIS_COMPLETE));
    assert_eq!(avro.value.as_ref().unwrap().schema_ids, vec![42]);
    assert!(!avro.key.as_ref().unwrap().dependent, "string keys");
    assert_eq!(got["json"].verdict, NOT_DETECTED);
    let compacted = &got["compacted"];
    assert_eq!(compacted.verdict, SCHEMA_DEPENDENT);
    let v = compacted.value.as_ref().unwrap();
    assert_eq!((v.framed, v.unframed, v.nulls), (3, 0, 3));
    for empty in ["empty", "absent"] {
        assert_eq!(got[empty].verdict, NOT_ASSESSED, "{empty}");
        assert_eq!(
            got[empty].reason.as_deref(),
            Some(REASON_NO_RECORDS),
            "{empty}"
        );
    }
}

#[test]
fn a_segment_that_cannot_be_judged_makes_its_topic_not_assessed_never_not_detected() {
    let mut s = Segments::default();
    let ok = s.put("t/ok/0/0", &records(0, 3, |o| Some(plain(o))));
    // The manifest counts 5 records; the segment holds 3.
    let mut short = s.put("t/short/0/0", &records(0, 3, |o| Some(plain(o))));
    short.record_count = 5;
    // A segment the store does not hold.
    let mut missing = ok.clone();
    missing.key = "t/missing/0/0".into();
    // Bytes that are not a KBAK segment.
    s.objects
        .insert("t/garbage/0/0".into(), b"[{\"legacy\":1}]".to_vec());
    let mut garbage = ok.clone();
    garbage.key = "t/garbage/0/0".into();
    let archive = set(vec![
        topic("ok", vec![partition(0, vec![ok])]),
        topic("short", vec![partition(0, vec![short])]),
        topic("missing", vec![partition(0, vec![missing])]),
        topic("garbage", vec![partition(0, vec![garbage])]),
    ]);
    let got = detect(&archive, &names(&["ok", "short", "missing", "garbage"]), &s);
    assert_eq!(got["ok"].verdict, NOT_DETECTED, "the control");
    for t in ["short", "missing", "garbage"] {
        assert_eq!(got[t].verdict, NOT_ASSESSED, "{t}");
        assert_eq!(
            got[t].reason.as_deref(),
            Some(REASON_SEGMENT_UNREADABLE),
            "{t}"
        );
    }
}

/// THE SAMPLE BOUND, measured: a single segment of 1 200 records is judged on
/// its first and last 500 only. The 200 framed records in its middle are
/// never seen — so the topic reads `notDetected (sampled)`, which is what the
/// contract says a sample can miss — and a reader that judged every record
/// would call it dependent (200 of 1 200 is above one in ten).
#[test]
fn one_large_segment_is_judged_on_its_first_and_last_records_only() {
    let n = 2 * SAMPLE_RECORDS_PER_END as i64 + 200;
    let lo = SAMPLE_RECORDS_PER_END as i64;
    let hi = lo + 200;
    let mut s = Segments::default();
    let seg = s.put(
        "t/big/0/0",
        &records(0, n, |o| {
            Some(if (lo..hi).contains(&o) {
                framed(9)
            } else {
                plain(o)
            })
        }),
    );
    let archive = set(vec![topic("big", vec![partition(0, vec![seg])])]);
    let got = &detect(&archive, &names(&["big"]), &s)["big"];
    assert_eq!(got.verdict, NOT_DETECTED);
    assert_eq!(got.basis.as_deref(), Some(BASIS_SAMPLED));
    let v = got.value.as_ref().unwrap();
    assert_eq!(v.framed + v.unframed, 2 * SAMPLE_RECORDS_PER_END as u64);
    assert_eq!(s.reads(), vec!["t/big/0/0".to_string()], "read once");
}

/// Per partition, the first and the last segment by start offset — never a
/// middle one, and in archive order whatever order the manifest lists them in.
#[test]
fn a_partition_is_sampled_at_its_first_and_last_segment() {
    let mut s = Segments::default();
    let first = s.put("t/p/0/a", &records(0, 600, |o| Some(plain(o))));
    let middle = s.put("t/p/0/b", &records(600, 600, |_| Some(framed(3))));
    let last = s.put("t/p/0/c", &records(1200, 600, |o| Some(plain(o))));
    let archive = set(vec![topic(
        "p",
        vec![partition(0, vec![last, middle, first])],
    )]);
    let got = &detect(&archive, &names(&["p"]), &s)["p"];
    assert_eq!(
        got.verdict, NOT_DETECTED,
        "the framed middle segment is not read"
    );
    assert_eq!(got.basis.as_deref(), Some(BASIS_SAMPLED));
    let mut reads = s.reads();
    reads.sort();
    assert_eq!(reads, vec!["t/p/0/a".to_string(), "t/p/0/c".to_string()]);
    let v = got.value.as_ref().unwrap();
    assert_eq!(v.unframed, 2 * SAMPLE_RECORDS_PER_END as u64);
}

/// At most `SAMPLE_PARTITIONS` partitions, the LOWEST ids first: framed data
/// only in the two highest of ten partitions is outside the sample; the same
/// data in partition 0 is inside it (the control).
#[test]
fn the_lowest_partitions_are_sampled_and_no_more_than_the_cap() {
    assert_eq!(SAMPLE_PARTITIONS, 8);
    let build = |framed_in: &[i32]| {
        let mut s = Segments::default();
        let mut parts = Vec::new();
        for p in (0..10).rev() {
            let hot = framed_in.contains(&p);
            let seg = s.put(
                &format!("t/wide/{p}/0"),
                &records(0, 4, |o| Some(if hot { framed(5) } else { plain(o) })),
            );
            parts.push(partition(p, vec![seg]));
        }
        (s, set(vec![topic("wide", parts)]))
    };
    let (s, archive) = build(&[8, 9]);
    let got = &detect(&archive, &names(&["wide"]), &s)["wide"];
    assert_eq!(got.verdict, NOT_DETECTED);
    assert_eq!(got.basis.as_deref(), Some(BASIS_SAMPLED));
    let mut reads = s.reads();
    reads.sort();
    assert_eq!(
        reads,
        (0..8).map(|p| format!("t/wide/{p}/0")).collect::<Vec<_>>()
    );
    let (s, archive) = build(&[0]);
    assert_eq!(
        detect(&archive, &names(&["wide"]), &s)["wide"].verdict,
        SCHEMA_DEPENDENT,
        "1 of 8 sampled partitions framed is 4 of 32 records, above one in ten"
    );
}

/// A small topic is read whole: two segments of a few records each, every
/// partition — and the entry says `complete`.
#[test]
fn a_topic_read_whole_says_complete() {
    let mut s = Segments::default();
    let a = s.put("t/s/0/a", &records(0, 3, |_| Some(framed(1))));
    let b = s.put("t/s/0/b", &records(3, 2, |_| Some(framed(2))));
    let c = s.put("t/s/1/a", &records(0, 4, |o| Some(plain(o))));
    let archive = set(vec![topic(
        "s",
        vec![partition(0, vec![a, b]), partition(1, vec![c])],
    )]);
    let got = &detect(&archive, &names(&["s"]), &s)["s"];
    assert_eq!(got.basis.as_deref(), Some(BASIS_COMPLETE));
    let v = got.value.as_ref().unwrap();
    assert_eq!((v.framed, v.unframed), (5, 4));
    assert_eq!(v.schema_ids, vec![1, 2]);
    assert_eq!(got.verdict, SCHEMA_DEPENDENT);
}

// ===========================================================================
// End to end through `execute_with`: the receipt the run signs.
// ===========================================================================

use logweir::backup::{execute_with, BackupRunArgs};
use logweir_engine_oso::storage::Store;
use logweir_evidence::keys::SigningKey;
use logweir_kafka::reader::{ClusterReader, KafkaError, TopicMeta};

struct Reader;

impl ClusterReader for Reader {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        Ok("SOURCE-CLUSTER-0000001".into())
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        Ok(vec![])
    }
    fn end_offsets(&self, _: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        Ok(vec![])
    }
    fn topic_configs(&self, _: &str) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(BTreeMap::new())
    }
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(BTreeMap::new())
    }
    fn consume_range(
        &self,
        _: &str,
        _: i32,
        _: i64,
        _: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        Ok(vec![])
    }
}

/// Writes the manifest when it runs (FX-7: a set must be new) and describes
/// the segments the test seeded OUTSIDE the set's directory.
struct Engine<'a> {
    archive: &'a Store,
    topics: Vec<TopicFacts>,
}

impl DataEngine for Engine<'_> {
    fn id(&self) -> EngineId {
        EngineId {
            id: "double".into(),
            version: "v0.21.0".into(),
            digest: "sha256:0".into(),
        }
    }
    fn list_backup_sets(&self, _: &StorageUrl) -> Result<Vec<BackupSetRef>, EngineError> {
        unimplemented!()
    }
    fn describe(&self, set: &BackupSetRef) -> Result<BackupSetFacts, EngineError> {
        let mut facts = self::set(self.topics.clone());
        facts.backup_id = set.backup_id.clone();
        Ok(facts)
    }
    fn preflight(&self, _: &RestorePlan) -> Result<PreflightReport, EngineError> {
        unimplemented!()
    }
    fn restore(
        &self,
        _: &RestorePlan,
        _: &mut dyn PhaseObserver,
    ) -> Result<RestoreFacts, EngineError> {
        unimplemented!()
    }
    fn fingerprints(&self, _: &SampleSelection) -> Result<Vec<RecordFingerprint>, EngineError> {
        unimplemented!()
    }
    fn backup(
        &self,
        plan: &BackupPlan,
        _: &mut dyn PhaseObserver,
    ) -> Result<BackupFacts, EngineError> {
        let key = format!("logweir/{}/manifest.json", plan.backup_id);
        self.archive
            .put_create_only(&key, b"{\"topics\":[]}")
            .map_err(|e| EngineError::Operational(e.to_string()))?;
        Ok(BackupFacts {
            started_at: "2026-10-09T00:00:00Z".parse().unwrap(),
            finished_at: "2026-10-09T00:01:00Z".parse().unwrap(),
            exit_code: 0,
            unknown_key_warnings: vec![],
        })
    }
}

#[test]
fn a_backup_run_signs_a_1_5_0_receipt_naming_the_framed_topic() {
    let archive = Store::in_memory("logweir/");
    let evidence = Store::in_memory("logweir/");
    // The segments, under a prefix the FX-7 new-set check does not list.
    let put = |key: &str, recs: &[ConsumedRecord]| {
        archive
            .put_create_only(key, &fixtures::kbak_segment(recs))
            .unwrap();
        let mut s = Segments::default();
        s.put(key, recs)
    };
    let orders = put(
        "logweir/seeded/orders/0",
        &records(0, 4, |_| Some(framed(11))),
    );
    let audit = put("logweir/seeded/audit/0", &records(0, 4, |o| Some(plain(o))));
    let engine = Engine {
        archive: &archive,
        topics: vec![
            topic("orders", vec![partition(0, vec![orders])]),
            topic("audit", vec![partition(0, vec![audit])]),
        ],
    };
    let dir = tempfile::tempdir().unwrap();
    let spec = dir.path().join("backup.yaml");
    std::fs::write(
        &spec,
        "backup_id: nightly-1\n\
         source:\n\
        \x20 bootstrap_servers: [localhost:9092]\n\
        \x20 topics: [orders, audit]\n\
         storage:\n\
        \x20 backend: s3\n\
        \x20 bucket: kafka-backups\n\
        \x20 prefix: logweir/\n\
        \x20 region: us-east-1\n\
        \x20 endpoint: http://127.0.0.1:19000\n\
        \x20 path_style: true\n\
        \x20 allow_http: true\n\
         backup:\n\
        \x20 compression: zstd\n\
        \x20 segment_max_records: 1000\n\
        \x20 segment_max_bytes: 10485760\n\
        \x20 max_concurrent_partitions: 3\n",
    )
    .unwrap();
    let allowed = dir.path().join("allowed.json");
    std::fs::write(&allowed, "{\"allowed_cluster_ids\": [\"SCRATCH-1\"]}").unwrap();
    let key = SigningKey::generate_p256();
    let key_path = dir.path().join("signer.pem");
    std::fs::write(&key_path, key.to_pkcs8_pem().unwrap()).unwrap();
    let args = BackupRunArgs {
        store_contract_version: None,
        spec,
        allowed_clusters: allowed,
        signing_key: key_path,
        triggered_by: None,
        out: None,
        receipt_out: None,
        backup_id_override: None,
        kafka_topic_resources: None,
        strimzi_cluster: None,
    };
    let outcome = execute_with(&args, "01JRUN", &Reader, &engine, &archive, &evidence)
        .unwrap_or_else(|e| panic!("the run failed: {e}"));
    let (bytes, _) = evidence.get(&outcome.receipt_key).unwrap();
    let receipt: BackupReceipt = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(receipt.format_version, "1.5.0");
    receipt.validate_invariants().unwrap();
    let block = receipt
        .schema_dependency
        .as_ref()
        .expect("written on every receipt");
    assert_eq!(block["orders"].verdict, SCHEMA_DEPENDENT);
    assert_eq!(block["orders"].value.as_ref().unwrap().schema_ids, vec![11]);
    assert_eq!(block["audit"].verdict, NOT_DETECTED);
    // The catalog point copies it, topic by topic.
    let point_key = outcome.catalog_key.expect("the catalog point was written");
    let (point, _) = evidence.get(&point_key).unwrap();
    let point: serde_json::Value = serde_json::from_slice(&point).unwrap();
    let orders = point["topics"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "orders")
        .unwrap();
    assert_eq!(orders["schema_dependency"]["verdict"], "schemaDependent");
}

// ===========================================================================
// THE BOUNDS (the security review of the first version): caps, the time
// budget, the head's early stop, the tail's ring, and "never fatal".
// ===========================================================================

/// A KBAK v1 segment of `(key, value)` records, zstd-compressed when `zstd`,
/// with the frame of record `pad` (when given) declaring one byte more than
/// its fields use — a record no decoder may accept.
fn kbak(records: &[(Option<Vec<u8>>, Option<Vec<u8>>)], zstd: bool, pad: Option<usize>) -> Vec<u8> {
    let opt = |out: &mut Vec<u8>, v: &Option<Vec<u8>>| match v {
        None => out.extend_from_slice(&(-1i32).to_le_bytes()),
        Some(b) => {
            out.extend_from_slice(&(b.len() as i32).to_le_bytes());
            out.extend_from_slice(b);
        }
    };
    let mut body = Vec::new();
    for (i, (k, v)) in records.iter().enumerate() {
        let mut rec = Vec::new();
        rec.extend_from_slice(&(1_760_000_000_000i64 + i as i64).to_le_bytes());
        rec.extend_from_slice(&(i as i64).to_le_bytes());
        opt(&mut rec, k);
        opt(&mut rec, v);
        rec.extend_from_slice(&0u16.to_le_bytes());
        if pad == Some(i) {
            rec.push(0xEE);
        }
        body.extend_from_slice(&(rec.len() as u32).to_le_bytes());
        body.extend_from_slice(&rec);
    }
    envelope(
        records.len() as u64,
        if zstd { 1 } else { 0 },
        &if zstd {
            zstd::encode_all(&body[..], 3).unwrap()
        } else {
            body
        },
    )
}

/// The 32-byte header, `body`, the CRC-32 and the end magic.
fn envelope(record_count: u64, codec: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"KBAK");
    out.push(1);
    out.push(codec);
    out.extend_from_slice(&[0, 0]);
    out.extend_from_slice(&record_count.to_le_bytes());
    out.extend_from_slice(&0i64.to_le_bytes());
    out.extend_from_slice(&(record_count as i64 - 1).max(0).to_le_bytes());
    out.extend_from_slice(body);
    let crc = fixtures::crc32_ieee(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(b"BKAE");
    out
}

/// A one-record zstd segment whose value is `inflated` zero bytes: a few
/// kilobytes stored, `inflated` decompressed — a decompression bomb when that
/// is past the cap. Streamed through the encoder, so the test never holds it.
fn bomb(inflated: u64) -> Vec<u8> {
    use std::io::Write as _;
    let mut enc = zstd::stream::Encoder::new(Vec::new(), 19).unwrap();
    let frame_len = 8 + 8 + 4 + 4 + inflated + 2;
    enc.write_all(&(frame_len as u32).to_le_bytes()).unwrap();
    enc.write_all(&1_760_000_000_000i64.to_le_bytes()).unwrap();
    enc.write_all(&0i64.to_le_bytes()).unwrap();
    enc.write_all(&(-1i32).to_le_bytes()).unwrap();
    enc.write_all(&(inflated as i32).to_le_bytes()).unwrap();
    std::io::copy(
        &mut std::io::Read::take(std::io::repeat(0), inflated),
        &mut enc,
    )
    .unwrap();
    enc.write_all(&0u16.to_le_bytes()).unwrap();
    envelope(1, 1, &enc.finish().unwrap())
}

fn facts_of(key: &str, count: i64) -> SegmentFacts {
    SegmentFacts {
        key: key.into(),
        start_offset: 0,
        end_offset: count - 1,
        start_timestamp: 0,
        end_timestamp: 0,
        record_count: count,
        sha256: String::new(),
        uploaded_at: 0,
    }
}

fn one_segment_topic(s: &mut Segments, name: &str, bytes: Vec<u8>, count: i64) -> TopicFacts {
    let key = format!("t/{name}/0/0");
    s.objects.insert(key.clone(), bytes);
    topic(name, vec![partition(0, vec![facts_of(&key, count)])])
}

#[test]
fn the_production_limits_are_the_documented_ones() {
    assert_eq!(MAX_SEGMENT_BYTES, 64 << 20);
    assert_eq!(MAX_DECOMPRESSED_BYTES, 256 << 20);
    assert_eq!(TIME_BUDGET, std::time::Duration::from_secs(120));
    assert_eq!(
        DetectionLimits::default(),
        DetectionLimits {
            max_segment_bytes: MAX_SEGMENT_BYTES,
            max_decompressed_bytes: MAX_DECOMPRESSED_BYTES,
            time_budget: TIME_BUDGET,
        }
    );
}

/// A segment STORED larger than the fetch cap is never fetched (the size
/// check comes first) and its topic is `segmentTooLargeForDetection`. The
/// control: the same segment under the default cap is judged.
#[test]
fn a_segment_stored_over_the_cap_is_never_fetched() {
    let mut s = Segments::default();
    let records: Vec<_> = (0..20).map(|_| (None, Some(framed(5)))).collect();
    let t = one_segment_topic(&mut s, "big", kbak(&records, false, None), 20);
    let archive = set(vec![t]);
    let tight = DetectionLimits {
        max_segment_bytes: 64,
        ..DetectionLimits::default()
    };
    let got = &detect_within(&archive, &names(&["big"]), &s, &tight)["big"];
    assert_eq!(got.verdict, NOT_ASSESSED);
    assert_eq!(got.reason.as_deref(), Some(REASON_SEGMENT_TOO_LARGE));
    assert!(
        s.reads().is_empty(),
        "nothing over the cap is fetched: {:?}",
        s.reads()
    );
    assert_eq!(
        detect(&archive, &names(&["big"]), &s)["big"].verdict,
        SCHEMA_DEPENDENT,
        "NEGATIVE CONTROL: under the default cap it is judged"
    );
}

/// A DECOMPRESSION BOMB — a few kilobytes stored, 8 MiB decompressed — stops
/// at the decompression cap and reads `segmentTooLargeForDetection`; the same
/// bytes under a cap above 8 MiB are judged (`notDetected`: a zero-filled
/// value names id 0, which no registry issues). The memory bound is measured
/// in `schema_dependency_memory.rs`.
#[test]
fn a_decompression_bomb_stops_at_the_cap() {
    let mut s = Segments::default();
    let bytes = bomb(8 << 20);
    assert!(bytes.len() < 64 << 10, "stored small: {}", bytes.len());
    let t = one_segment_topic(&mut s, "bomb", bytes, 1);
    let archive = set(vec![t]);
    let tight = DetectionLimits {
        max_decompressed_bytes: 1 << 20,
        ..DetectionLimits::default()
    };
    let got = &detect_within(&archive, &names(&["bomb"]), &s, &tight)["bomb"];
    assert_eq!(got.verdict, NOT_ASSESSED);
    assert_eq!(got.reason.as_deref(), Some(REASON_SEGMENT_TOO_LARGE));
    assert_eq!(
        detect(&archive, &names(&["bomb"]), &s)["bomb"].verdict,
        NOT_DETECTED,
        "NEGATIVE CONTROL: under a cap it fits, the same segment is judged"
    );
}

/// Past the time budget, every topic still to judge reads
/// `detectionTimeBudgetExceeded` and nothing more is fetched; a topic with no
/// record needs no read and still says `noRecords`.
#[test]
fn past_the_time_budget_the_rest_is_not_assessed() {
    let mut s = Segments::default();
    let t = one_segment_topic(
        &mut s,
        "a",
        kbak(&[(None, Some(framed(5)))], false, None),
        1,
    );
    let archive = set(vec![t, topic("empty", vec![partition(0, vec![])])]);
    let spent = DetectionLimits {
        time_budget: std::time::Duration::ZERO,
        ..DetectionLimits::default()
    };
    let got = detect_within(&archive, &names(&["a", "empty"]), &s, &spent);
    assert_eq!(got["a"].reason.as_deref(), Some(REASON_TIME_BUDGET));
    assert_eq!(got["empty"].reason.as_deref(), Some(REASON_NO_RECORDS));
    assert!(s.reads().is_empty());
    assert_eq!(
        detect(&archive, &names(&["a"]), &s)["a"].verdict,
        SCHEMA_DEPENDENT,
        "NEGATIVE CONTROL: within the budget it is judged"
    );
}

/// A source that panics: the topic is `segmentUnreadable`, the next topic is
/// still judged, and `detect` returns — detection can never fail the backup.
#[test]
fn a_panic_inside_detection_is_a_value_not_a_failed_backup() {
    struct Panics<'a>(&'a Segments);
    impl SegmentSource for Panics<'_> {
        fn segment_bounded(&self, key: &str, max: u64) -> Result<Option<Vec<u8>>, String> {
            if key.contains("/boom/") {
                panic!("a store client that panics");
            }
            self.0.segment_bounded(key, max)
        }
    }
    let mut s = Segments::default();
    let boom = one_segment_topic(&mut s, "boom", kbak(&[(None, None)], false, None), 1);
    let fine = one_segment_topic(
        &mut s,
        "fine",
        kbak(&[(None, Some(framed(6)))], false, None),
        1,
    );
    let got = detect(
        &set(vec![boom, fine]),
        &names(&["boom", "fine"]),
        &Panics(&s),
    );
    assert_eq!(
        got["boom"].reason.as_deref(),
        Some(REASON_SEGMENT_UNREADABLE)
    );
    assert_eq!(got["fine"].verdict, SCHEMA_DEPENDENT);
}

/// THE HEAD STOPS: a first segment whose record after the head is broken is
/// still judged, because the head scan never reaches it. The control: the
/// same segment as a partition's ONLY segment is scanned whole, reaches the
/// broken record, and is `segmentUnreadable`.
#[test]
fn the_head_scan_stops_after_its_records() {
    let n = SAMPLE_RECORDS_PER_END;
    let records: Vec<_> = (0..n + 5).map(|_| (None, Some(framed(8)))).collect();
    let broken = kbak(&records, true, Some(n + 2));
    let tail = kbak(&[(None, Some(framed(8)))], false, None);
    let mut s = Segments::default();
    s.objects.insert("t/two/0/a".into(), broken.clone());
    s.objects.insert("t/two/0/b".into(), tail);
    let mut first = facts_of("t/two/0/a", (n + 5) as i64);
    first.end_offset = (n + 4) as i64;
    let mut second = facts_of("t/two/0/b", 1);
    second.start_offset = (n + 5) as i64;
    second.end_offset = second.start_offset;
    let archive = set(vec![topic("two", vec![partition(0, vec![first, second])])]);
    let got = &detect(&archive, &names(&["two"]), &s)["two"];
    assert_eq!(got.verdict, SCHEMA_DEPENDENT, "{got:?}");
    assert_eq!(got.value.as_ref().unwrap().framed, (n + 1) as u64);
    let mut s = Segments::default();
    let only = one_segment_topic(&mut s, "one", broken, (n + 5) as i64);
    let got = &detect(&set(vec![only]), &names(&["one"]), &s)["one"];
    assert_eq!(
        got.reason.as_deref(),
        Some(REASON_SEGMENT_UNREADABLE),
        "NEGATIVE CONTROL: a whole scan reaches the broken record"
    );
}
