//! Shared by every crates/logweir/tests/*.rs. Declare with `mod fixtures;` as
//! the first line of each test file. Unused items in a given binary are
//! expected — hence the allow.
#![allow(dead_code)]

use chrono::{DateTime, Utc};
// UNCOMMENT IN TASK 16 — phase2_target::{TargetState, TopicState}
// use logweir::drill::phase2_target::{TargetState, TopicState};
// UNCOMMENT IN TASK 20 — phase8_score::Timeline
// use logweir::drill::phase8_score::Timeline;
use logweir_core::engine::*;
use logweir_core::outcome::*;
use logweir_core::scorecard::*;
use logweir_core::spec::{ObjectivesSpec, SampleSpec};
// UNCOMMENT IN TASK 16 — add `ClusterReader, TopicMeta` to the import below (used by FakeReader)
use logweir_kafka::reader::{ConsumedRecord, KafkaError, TopicDeleter};
use std::collections::BTreeMap;
use std::path::PathBuf;

pub fn ts(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

// ---------------------------------------------------------------- archive side

/// One topic "orders" with `partitions` partitions, two segments each, spanning
/// 2026-08-29T00:00:00Z .. 2026-08-30T02:00:00Z and offsets 0..500.
pub fn backup_facts_orders(partitions: i32) -> BackupSetFacts {
    let t0 = ts("2026-08-29T00:00:00Z").timestamp_millis();
    let t1 = ts("2026-08-30T02:00:00Z").timestamp_millis();
    BackupSetFacts {
        backup_id: "backup-2026-08-30T02:00:00Z".into(),
        created_at: ts("2026-08-30T02:00:00Z"),
        source_cluster_id: Some("SRC0000000000000000000".into()),
        manifest_sha256: format!("sha256:{}", "a".repeat(64)),
        manifest_version_id: None,
        consumer_group_snapshot_sha256: None,
        topics: vec![TopicFacts {
            name: "orders".into(),
            original_partition_count: Some(partitions),
            source_replication_factor: Some(3),
            configurations: source_configs(&[
                ("cleanup.policy", "compact"),
                ("retention.ms", "604800000"),
            ]),
            partitions: (0..partitions)
                .map(|p| PartitionFacts {
                    partition_id: p,
                    segments: vec![
                        SegmentFacts {
                            key: format!("drills/b/{p}/000000000000.kbak"),
                            start_offset: 0,
                            end_offset: 249,
                            start_timestamp: t0,
                            end_timestamp: (t0 + t1) / 2,
                            record_count: 250,
                            sha256: format!("sha256:{}", "b".repeat(64)),
                            uploaded_at: t1,
                        },
                        SegmentFacts {
                            key: format!("drills/b/{p}/000000000250.kbak"),
                            start_offset: 250,
                            end_offset: 499,
                            start_timestamp: (t0 + t1) / 2,
                            end_timestamp: t1,
                            record_count: 250,
                            sha256: format!("sha256:{}", "c".repeat(64)),
                            uploaded_at: t1,
                        },
                    ],
                    gaps: vec![],
                    pruned: vec![],
                })
                .collect(),
        }],
    }
}

/// Like `backup_facts_orders`, but partition 0 carries the given `gaps` and
/// `pruned` ranges instead of none — for a phase that must detect and report
/// a coverage gap (the backup could not capture a range) or an
/// operator-pruned range (retention deliberately removed one) rather than
/// treating the archive as fully covered (2026-09-04 fix review: the
/// original `backup_facts_orders` hardcoded both to empty, so no consumer
/// could build either case).
pub fn backup_facts_orders_with_coverage(
    partitions: i32,
    gaps: &[(i64, i64)],
    pruned: &[(i64, i64)],
) -> BackupSetFacts {
    let mut facts = backup_facts_orders(partitions);
    if let Some(p0) = facts.topics[0].partitions.first_mut() {
        p0.gaps = gaps.to_vec();
        p0.pruned = pruned.to_vec();
    }
    facts
}

pub fn mapping(from: &str, to: &str) -> BTreeMap<String, String> {
    [(from.to_string(), to.to_string())].into_iter().collect()
}

pub fn sample_spec(from: &str, to: &str, per_partition: usize, anchor: &str) -> SampleSpec {
    SampleSpec {
        window_start: ts(from),
        window_end: ts(to),
        records_per_partition: per_partition,
        anchor: anchor.to_string(),
        max_partitions: None,
    }
}

pub fn plan() -> RestorePlan {
    RestorePlan {
        set: BackupSetRef {
            backup_id: "backup-2026-08-30T02:00:00Z".into(),
            manifest_key: "drills/b/manifest.json".into(),
        },
        storage: StorageUrl::S3 {
            bucket: "kafka-backups".into(),
            prefix: "basic-demo".into(),
            region: Some("us-east-1".into()),
            endpoint: Some("http://minio:9000".into()),
            path_style: true,
            allow_http: true,
        },
        target_bootstrap: vec!["kafka-broker-1:9094".into()],
        topic_mapping: mapping("orders", "drill-orders"),
        time_window: (ts("2026-08-29T00:00:00Z"), ts("2026-08-30T02:00:00Z")),
        default_replication_factor: 1,
        checkpoint_state: "/tmp/logweir/checkpoint.json".into(),
        checkpoint_interval_secs: 30,
    }
}

// ---------------------------------------------------------------- target side

// UNCOMMENT IN TASK 16 — phase2_target::{TargetState, TopicState}
// `cluster_id` is a PARAMETER, not hardcoded: a caller building a
// wrong-cluster or target-equals-source fixture (phase 0's most
// safety-critical checks) needs a `TargetState` reporting a DIFFERENT
// cluster id than the "allowed" one, which a hardcoded constant makes
// impossible (2026-09-04 fix review).
// pub fn target_with(cluster_id: &str, topic: &str, partitions: i32, end_offset: i64, configs: &[(&str, &str)])
//     -> TargetState {
//     TargetState {
//         cluster_id: cluster_id.into(),
//         topics: [(topic.to_string(), TopicState {
//             partitions,
//             end_offsets: (0..partitions).map(|p| (p, if p == 0 { end_offset } else { 0 })).collect(),
//             configs: configs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
//         })].into_iter().collect(),
//     }
// }

// UNCOMMENT IN TASK 16 — phase2_target::TargetState
// pub fn empty_target(cluster_id: &str) -> TargetState {
//     TargetState { cluster_id: cluster_id.into(), topics: BTreeMap::new() }
// }

pub fn source_configs(kv: &[(&str, &str)]) -> BTreeMap<String, String> {
    kv.iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}
pub fn target_configs(kv: &[(&str, &str)]) -> BTreeMap<String, String> {
    source_configs(kv)
}

// UNCOMMENT IN TASK 16 — phase2_target::TargetState (FakeReader::state)
// /// A reader that answers from a `TargetState`, so phases 2, 6 and 7 are
// /// testable with no broker.
// pub struct FakeReader { pub state: TargetState }
//
// impl ClusterReader for FakeReader {
//     fn cluster_id(&self) -> Result<String, KafkaError> { Ok(self.state.cluster_id.clone()) }
//     fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
//         // `TopicMeta::new`, not a bare struct literal: `TopicMeta` carries a
//         // third field, `error: Option<String>` (Task 10), and a literal
//         // naming only `name`/`partitions` does not compile (2026-09-04 fix
//         // review). Every topic built from `TargetState` is reported healthy;
//         // an "errored" topic needs its own fixture once `TopicState` grows
//         // a way to express one.
//         Ok(self.state.topics.iter()
//             .map(|(n, t)| TopicMeta::new(n.clone(), t.partitions)).collect())
//     }
//     fn end_offsets(&self, topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
//         Ok(self.state.topics.get(topic).map(|t| t.end_offsets.clone()).unwrap_or_default())
//     }
//     fn topic_configs(&self, topic: &str) -> Result<BTreeMap<String, String>, KafkaError> {
//         Ok(self.state.topics.get(topic).map(|t| t.configs.clone()).unwrap_or_default())
//     }
//     fn consume_range(&self, _t: &str, _p: i32, _from: i64, _max: usize)
//         -> Result<Vec<ConsumedRecord>, KafkaError> { Ok(vec![]) }
// }

pub struct RecordingDeleter {
    pub deleted: std::sync::Mutex<Vec<String>>,
}
impl TopicDeleter for RecordingDeleter {
    fn delete_topics(
        &self,
        names: &[String],
    ) -> Result<Vec<(String, Result<(), String>)>, KafkaError> {
        self.deleted.lock().unwrap().extend_from_slice(names);
        Ok(names.iter().map(|n| (n.clone(), Ok(()))).collect())
    }
}

// ---------------------------------------------------------------- reconciliation

/// `n` archive fingerprints at offsets 0..n and the matching consumed records,
/// each carrying `x-original-offset` so `compare` matches by original offset.
pub fn matching_pair(n: usize) -> (Vec<RecordFingerprint>, Vec<ConsumedRecord>) {
    let mut arch = Vec::with_capacity(n);
    let mut cons = Vec::with_capacity(n);
    for i in 0..n {
        let off = i as i64;
        let headers = vec![(
            "x-original-offset".to_string(),
            Some(off.to_string().into_bytes()),
        )];
        let key = format!("k{i}").into_bytes();
        let value = format!("v{i}").into_bytes();
        let tsms = 1_756_425_600_000i64 + off;
        arch.push(RecordFingerprint {
            topic: "orders".into(),
            partition: 0,
            offset: off,
            sha256: logweir_kafka::fingerprint::record_fingerprint(
                Some(&key),
                Some(&value),
                &headers,
                tsms,
            ),
        });
        cons.push(ConsumedRecord {
            partition: 0,
            offset: off,
            timestamp_ms: tsms,
            key: Some(key),
            value: Some(value),
            headers,
        });
    }
    (arch, cons)
}

/// Like `matching_pair`, but the consumed record at `mismatch_at` carries a
/// DIFFERENT value than the one its archive fingerprint was computed from —
/// so a reconciliation comparison over the result must report exactly one
/// mismatch, never a false pass (2026-09-04 fix review: `matching_pair` had
/// no mismatching counterpart, so no consumer could build this case).
pub fn matching_pair_with_mismatch(
    n: usize,
    mismatch_at: usize,
) -> (Vec<RecordFingerprint>, Vec<ConsumedRecord>) {
    let (arch, mut cons) = matching_pair(n);
    if let Some(rec) = cons.get_mut(mismatch_at) {
        rec.value = Some(b"tampered".to_vec());
    }
    (arch, cons)
}

// ---------------------------------------------------------------- scoring

// UNCOMMENT IN TASK 20 — phase8_score::Timeline
// /// requested 09:00:00, approval_validated 09:00:30, restore 09:02:00..09:05:34,
// /// phase 5 duration 212s, verified 09:09:02.
// pub fn timeline() -> Timeline {
//     Timeline {
//         requested_at: ts("2026-09-03T09:00:00Z"),
//         approval_validated_at: ts("2026-09-03T09:00:30Z"),
//         restore_started_at: ts("2026-09-03T09:02:00Z"),
//         restore_finished_at: ts("2026-09-03T09:05:34Z"),
//         phase5_duration_ms: 212_000,
//         verified_at: ts("2026-09-03T09:09:02Z"),
//     }
// }

pub fn measured(rto: u64, excl: u64, rpo: i64) -> Measured {
    Measured {
        rto_seconds: Some(rto),
        rto_requested_to_verified_seconds: Some(rto + 30),
        rto_restore_only_seconds: Some(214),
        rto_excluding_preflight_seconds: Some(excl),
        rpo_seconds: Some(rpo),
        rpo_source_relative_seconds: None,
        rpo_source_relative_unmeasured_reason: Some("source cluster never contacted".into()),
    }
}

pub fn objectives(rto: u64, rpo: i64, rate: f64) -> ObjectivesSpec {
    ObjectivesSpec {
        rto_seconds: Some(rto),
        rpo_seconds: Some(rpo),
        pass_rate: Some(rate),
    }
}

fn integ(
    level: IntegrityLevel,
    result: IntegrityResult,
    matching: u64,
    sampled: u64,
    reason: Option<&str>,
    rate: Option<f64>,
) -> Integrity {
    Integrity {
        level,
        result,
        partial_reason: reason.map(str::to_string),
        records_sampled: sampled,
        records_sampled_matching: matching,
        mismatches: sampled - matching,
        pass_rate_measured: rate,
        restored_principal_could_consume: None,
    }
}

pub fn integrity_pass() -> Integrity {
    integ(
        IntegrityLevel::ByteFingerprint,
        IntegrityResult::Pass,
        75,
        75,
        None,
        Some(1.0),
    )
}
pub fn integrity_fail() -> Integrity {
    integ(
        IntegrityLevel::ByteFingerprint,
        IntegrityResult::Fail,
        74,
        75,
        None,
        Some(74.0 / 75.0),
    )
}
pub fn integrity_consume_only() -> Integrity {
    integ(
        IntegrityLevel::ConsumeOnly,
        IntegrityResult::Pass,
        0,
        0,
        None,
        None,
    )
}

/// The checked-in fixture, parsed. Reusing it keeps every scoring test aligned
/// with the document the format tests already gate.
pub fn scorecard_pass() -> Scorecard {
    serde_json::from_str(include_str!("../../../../e2e/fixtures/scorecard-pass.json")).unwrap()
}

// ---------------------------------------------------------------- storage + keys

/// Records every put so `a_signing_failure_exits_4_and_uploads_nothing` can
/// assert the bucket was never touched.
pub struct RecordingStore {
    pub inner: logweir_engine_oso::storage::Store,
    pub put_keys: std::sync::Mutex<Vec<String>>,
}

impl RecordingStore {
    pub fn puts(&self) -> Vec<String> {
        self.put_keys.lock().unwrap().clone()
    }
    pub fn put_create_only(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> Result<logweir_engine_oso::storage::PutOutcome, logweir_engine_oso::storage::StoreError>
    {
        self.put_keys.lock().unwrap().push(key.to_string());
        self.inner.put_create_only(key, bytes)
    }
}

pub fn recording_store() -> RecordingStore {
    RecordingStore {
        inner: logweir_engine_oso::storage::Store::in_memory("logweir"),
        put_keys: Default::default(),
    }
}

pub fn store_without_conditional_put() -> RecordingStore {
    RecordingStore {
        inner: logweir_engine_oso::storage::Store::in_memory_without_conditional_put("logweir"),
        put_keys: Default::default(),
    }
}

/// A PEM path plus the temp dir that owns it, so the file outlives the call.
pub struct KeyRef {
    pub path: PathBuf,
    _dir: tempfile::TempDir,
}

pub fn good_signing_key() -> KeyRef {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("signer.pem");
    std::fs::copy("../../e2e/fixtures/signed/signing.pem", &path).unwrap();
    KeyRef { path, _dir: dir }
}

pub fn unreadable_signing_key() -> KeyRef {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("broken.pem");
    std::fs::write(
        &path,
        b"-----BEGIN PRIVATE KEY-----\nnot a key\n-----END PRIVATE KEY-----\n",
    )
    .unwrap();
    KeyRef { path, _dir: dir }
}

// ---------------------------------------------------------------- approvals

pub struct ApprovalFixture {
    pub spec_text: String,
    pub approval: PathBuf,
    pub approver_pub: PathBuf,
    pub other_pub: PathBuf,
    pub approver_key_id: String,
    pub before: DateTime<Utc>,
    _dir: tempfile::TempDir,
}

// UNCOMMENT IN TASK 15 — phase1_approval::PAYLOAD_TYPE_APPROVAL
// (applies to the `PAYLOAD_TYPE_APPROVAL` line inside `approval_fixture`)
// fn approval_fixture(self_attested: bool) -> ApprovalFixture {
//     use logweir_evidence::{keys::SigningKey, sign::sign_detached};
//     let dir = tempfile::tempdir().unwrap();
//     let spec_text = std::fs::read_to_string("../../examples/drill.yaml").unwrap();
//     let doc = serde_json::json!({
//         "approver": "sre-oncall@example.com",
//         "ticket": "CHG-40881",
//         "plan_hash": logweir_core::ids::sha256_prefixed(spec_text.as_bytes()),
//         "approved_at": "2026-09-02T17:40:00Z",
//     });
//     let bytes = serde_json::to_vec_pretty(&doc).unwrap();
//     let approval = dir.path().join("approval.json");
//     std::fs::write(&approval, &bytes).unwrap();
//
//     let approver = if self_attested {
//         SigningKey::from_pem_file(std::path::Path::new("../../e2e/fixtures/signed/signing.pem")).unwrap()
//     } else {
//         SigningKey::generate_p256()
//     };
//     let side = sign_detached(&approver,
//         logweir::drill::phase1_approval::PAYLOAD_TYPE_APPROVAL, &bytes).unwrap();
//     std::fs::write(approval.with_extension("sig"), serde_json::to_vec(&side).unwrap()).unwrap();
//
//     let approver_pub = dir.path().join("approver.pub.pem");
//     write_pub(&approver, &approver_pub);
//     let other_pub = dir.path().join("other.pub.pem");
//     write_pub(&SigningKey::generate_p256(), &other_pub);
//
//     ApprovalFixture {
//         spec_text, approval, approver_pub, other_pub,
//         approver_key_id: approver.key_id(),
//         before: Utc::now(), _dir: dir,
//     }
// }

fn write_pub(k: &logweir_evidence::keys::SigningKey, to: &std::path::Path) {
    use p256::pkcs8::EncodePublicKey;
    match k.verifying_key() {
        logweir_evidence::keys::VerifyingKey::P256(v) => {
            std::fs::write(to, v.to_public_key_pem(Default::default()).unwrap()).unwrap()
        }
        logweir_evidence::keys::VerifyingKey::Ed25519(v) => {
            std::fs::write(to, v.to_public_key_pem(Default::default()).unwrap()).unwrap()
        }
    }
}

// UNCOMMENT IN TASK 15 — phase1_approval::PAYLOAD_TYPE_APPROVAL (via approval_fixture)
// /// Approver key != signing key.
// pub fn approval_ok() -> ApprovalFixture { approval_fixture(false) }
// /// Approver key == the signing key in e2e/fixtures/signed/signing.pem.
// pub fn approval_self() -> ApprovalFixture { approval_fixture(true) }

// ---------------------------------------------------------------- engine doubles

pub struct NullObserver;
impl PhaseObserver for NullObserver {
    fn phase_started(&mut self, _phase: i8, _name: &str) {}
    fn phase_finished(&mut self, _phase: i8, _outcome: &str) {}
    fn engine_line(&mut self, _stream: &str, _line: &str) {}
}

/// An engine whose `restore` sleeps, so phase 6's measured window can be
/// asserted to bracket the subprocess and nothing else.
pub struct SleepEngine {
    pub ms: u64,
}

impl DataEngine for SleepEngine {
    fn id(&self) -> EngineId {
        EngineId {
            id: "fake".into(),
            version: "v0.21.0".into(),
            digest: format!("sha256:{}", "0".repeat(64)),
        }
    }
    fn list_backup_sets(&self, _l: &StorageUrl) -> Result<Vec<BackupSetRef>, EngineError> {
        Ok(vec![])
    }
    fn describe(&self, _s: &BackupSetRef) -> Result<BackupSetFacts, EngineError> {
        Ok(backup_facts_orders(3))
    }
    fn preflight(&self, _p: &RestorePlan) -> Result<PreflightReport, EngineError> {
        Err(EngineError::Operational("not used by this fixture".into()))
    }
    fn restore(
        &self,
        _p: &RestorePlan,
        _o: &mut dyn PhaseObserver,
    ) -> Result<RestoreFacts, EngineError> {
        let started_at = Utc::now();
        std::thread::sleep(std::time::Duration::from_millis(self.ms));
        Ok(RestoreFacts {
            started_at,
            finished_at: Utc::now(),
            exit_code: 0,
            unknown_key_warnings: vec![],
        })
    }
    fn fingerprints(&self, _s: &SampleSelection) -> Result<Vec<RecordFingerprint>, EngineError> {
        Ok(matching_pair(25).0)
    }
}

// UNCOMMENT IN TASK 16 — phase2_target::TargetState (via FakeReader/target_with)
// /// (a reader whose target already holds records, an engine that sleeps `ms`).
// pub fn engine_that_sleeps_ms(ms: u64) -> (FakeReader, SleepEngine) {
//     (FakeReader { state: target_with("MkU3OEVBNTcwNTJENDM2Qk", "drill-orders", 3, 500, &[]) }, SleepEngine { ms })
// }
