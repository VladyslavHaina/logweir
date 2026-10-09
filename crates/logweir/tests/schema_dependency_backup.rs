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

use logweir::backup::schema_dependency::{detect, SegmentSource};
use logweir_core::backup_receipt::BackupReceipt;
use logweir_core::engine::*;
use logweir_core::schema_dependency::{
    BASIS_COMPLETE, BASIS_SAMPLED, NOT_ASSESSED, NOT_DETECTED, REASON_NO_RECORDS,
    REASON_SEGMENT_UNREADABLE, SAMPLE_PARTITIONS, SAMPLE_RECORDS_PER_END, SCHEMA_DEPENDENT,
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
    fn segment(&self, key: &str) -> Result<Vec<u8>, String> {
        self.reads.borrow_mut().push(key.to_string());
        self.objects
            .get(key)
            .cloned()
            .ok_or_else(|| format!("{key}: not found"))
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
