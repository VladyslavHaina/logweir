//! Shared by every crates/logweir/tests/*.rs. Declare with `mod fixtures;` as
//! the first line of each test file. Unused items in a given binary are
//! expected — hence the allow.
#![allow(dead_code)]

use chrono::{DateTime, Utc};
use logweir::drill::phase2_target::{TargetState, TopicState};
use logweir::drill::phase8_score::Timeline;
use logweir_core::engine::*;
use logweir_core::outcome::*;
use logweir_core::scorecard::*;
use logweir_core::spec::{ObjectivesSpec, SampleSpec};
use logweir_kafka::reader::{ClusterReader, ConsumedRecord, KafkaError, TopicDeleter, TopicMeta};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// What `FixtureEngine::restore` writes to `plan.offset_report`, standing in
/// for the engine's `serde_json::to_string_pretty(&offset_mapping)`
/// [U:crates/kafka-backup-core/src/restore/engine.rs:1382-1388]. An EMPTY
/// mapping is the honest shape for a run rendered
/// `consumer_group_strategy: skip`, which is every run tag 1 performs.
pub const OFFSET_REPORT_BYTES: &[u8] = b"{}\n";

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

pub fn sample_spec(
    from: &str,
    to: &str,
    per_partition: usize,
    anchor: logweir_core::spec::Anchor,
) -> SampleSpec {
    SampleSpec {
        window_start: ts(from),
        window_end: ts(to),
        records_per_partition: per_partition,
        anchor,
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
        target_auth: logweir_core::engine::AuthRender::Plaintext,
        topic_mapping: mapping("orders", "drill-orders"),
        time_window: (ts("2026-08-29T00:00:00Z"), ts("2026-08-30T02:00:00Z")),
        window_floor_source: WindowFloorSource::ArchiveManifest,
        default_replication_factor: 1,
        checkpoint_state: "/tmp/logweir/checkpoint.json".into(),
        checkpoint_interval_secs: 30,
        offset_report: "/var/lib/logweir/01J9X/offsets.json".into(),
    }
}

// ---------------------------------------------------------------- target side

/// `cluster_id` is a PARAMETER, not hardcoded: a caller building a
/// wrong-cluster or target-equals-source fixture (phase 0's most
/// safety-critical checks) needs a `TargetState` reporting a DIFFERENT
/// cluster id than the "allowed" one, which a hardcoded constant makes
/// impossible (2026-09-04 fix review).
pub fn target_with(
    cluster_id: &str,
    topic: &str,
    partitions: i32,
    end_offset: i64,
    configs: &[(&str, &str)],
) -> TargetState {
    TargetState {
        cluster_id: cluster_id.into(),
        topics: [(
            topic.to_string(),
            TopicState {
                partitions,
                end_offsets: (0..partitions)
                    .map(|p| (p, if p == 0 { end_offset } else { 0 }))
                    .collect(),
                configs: configs
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            },
        )]
        .into_iter()
        .collect(),
    }
}

/// A target holding no topics — the normal case on a scratch cluster.
pub fn empty_target(cluster_id: &str) -> TargetState {
    TargetState {
        cluster_id: cluster_id.into(),
        topics: BTreeMap::new(),
    }
}

pub fn source_configs(kv: &[(&str, &str)]) -> BTreeMap<String, String> {
    kv.iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}
pub fn target_configs(kv: &[(&str, &str)]) -> BTreeMap<String, String> {
    source_configs(kv)
}

/// A reader that answers from a `TargetState`, so phases 2, 6 and 7 are
/// testable with no broker.
pub struct FakeReader {
    pub state: TargetState,
}

impl ClusterReader for FakeReader {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        Ok(self.state.cluster_id.clone())
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        // `TopicMeta::new`, not a bare struct literal: `TopicMeta` carries a
        // third field, `error: Option<String>` (Task 10), and a literal
        // naming only `name`/`partitions` does not compile (2026-09-04 fix
        // review). Every topic built from `TargetState` is reported healthy.
        // A topic present but unreadable (errored metadata) is deliberately
        // NOT buildable from this fixture: phase2_target::run fails the
        // whole read rather than recording a healthy-looking empty
        // `TopicState` for it (see phases_2_4.rs,
        // `a_topic_present_but_unreadable_is_never_recorded_as_absent_or_empty`,
        // which exercises that case with its own small `ClusterReader`
        // double, not through `TargetState`/`FakeReader`).
        Ok(self
            .state
            .topics
            .iter()
            .map(|(n, t)| TopicMeta::new(n.clone(), t.partitions))
            .collect())
    }
    fn end_offsets(&self, topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        Ok(self
            .state
            .topics
            .get(topic)
            .map(|t| t.end_offsets.clone())
            .unwrap_or_default())
    }
    fn topic_configs(&self, topic: &str) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(self
            .state
            .topics
            .get(topic)
            .map(|t| t.configs.clone())
            .unwrap_or_default())
    }
    /// Task 8 (guard **G-TS**). `TargetState` describes the target's TOPICS
    /// and carries no broker-wide configuration, so this answers with an empty
    /// map — the harmless case: phase 0's preflight then treats the broker as
    /// the Apache default (`CreateTime`) and refuses nothing. A hostile broker
    /// is modelled in `crates/logweir/tests/topic_preflight.rs`, which builds
    /// its own doubles for exactly that.
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(BTreeMap::new())
    }
    fn consume_range(
        &self,
        _t: &str,
        _p: i32,
        _from: i64,
        _max: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        Ok(vec![])
    }
}

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

/// requested 09:00:00, approval_validated 09:00:30, restore 09:02:00..09:05:34,
/// phase 5 duration 212s, verified 09:09:02.
///
/// All six fields per `task-20-addendum.md` ruling A1 — `phase5_duration_ms`
/// must be `212_000` for `rto_excluding_preflight_seconds == 512 - 212 == 300`.
pub fn timeline() -> Timeline {
    Timeline {
        requested_at: ts("2026-09-03T09:00:00Z"),
        approval_validated_at: ts("2026-09-03T09:00:30Z"),
        restore_started_at: ts("2026-09-03T09:02:00Z"),
        restore_finished_at: ts("2026-09-03T09:05:34Z"),
        phase5_duration_ms: 212_000,
        verified_at: ts("2026-09-03T09:09:02Z"),
    }
}

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
/// A byte-fingerprint integrity block whose RESULT is `pass` but whose measured
/// ratio is `matching / sampled`. Exists so `decide`'s pass-rate comparison can
/// be exercised on its own: `integrity_fail()` carries `result: Fail`, which
/// short-circuits `decide` to `FailIntegrity` before the objectives are ever
/// consulted, so it cannot reach the rate arm.
pub fn integrity_rate(matching: u64, sampled: u64) -> Integrity {
    integ(
        IntegrityLevel::ByteFingerprint,
        IntegrityResult::Pass,
        matching,
        sampled,
        None,
        Some(matching as f64 / sampled as f64),
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

// ---------------------------------------------------------------- phase 7 verify

/// A SHAPE fixture, not a scenario `phase7_verify::run` can produce.
///
/// `run` has no reachable path from a compacted target topic to
/// `IntegrityResult::Partial` — see `IntegrityResult::Partial`'s own doc
/// comment and `phase7_verify`'s module doc, both of which state the
/// limitation. This exists so the scoring and rendering layers have a
/// `Partial`-with-a-reason document to consume; do not read a test built on it
/// as end-to-end coverage of compacted topics. Task 21c confirmed against a
/// live cluster that no e2e drill can drive this row.
pub fn verify_outcome_for_compacted_topic() -> logweir::drill::phase7_verify::VerifyOutcome {
    let mut o = base_verify_outcome();
    o.integrity.result = IntegrityResult::Partial;
    o.integrity.partial_reason = Some(
        "target topic drill-orders has cleanup.policy=compact; the target \
              legitimately holds fewer records than the archive, so a \
              record-for-record reconciliation is not possible"
            .into(),
    );
    o.integrity.records_sampled_matching = 40;
    o.integrity.mismatches = 35;
    o.integrity.pass_rate_measured = Some(40.0 / 75.0);
    o
}

pub fn verify_outcome_when_fingerprints_unsupported() -> logweir::drill::phase7_verify::VerifyOutcome
{
    let mut o = base_verify_outcome();
    o.integrity.level = IntegrityLevel::ConsumeOnly;
    o.integrity.records_sampled = 0;
    o.integrity.records_sampled_matching = 0;
    o.integrity.mismatches = 0;
    o.integrity.pass_rate_measured = None;
    o
}

fn base_verify_outcome() -> logweir::drill::phase7_verify::VerifyOutcome {
    logweir::drill::phase7_verify::VerifyOutcome {
        integrity: integrity_pass(),
        topic_parity: TopicParity {
            intentionally_deviated: vec![],
            unexpected_divergence: vec![],
        },
        records_restored: 75,
        newest_restored_ts_ms: 1_756_519_200_000,
        verified_at: ts("2026-09-03T09:09:02Z"),
    }
}

// ---------------------------------------------------------------- storage + keys

/// An in-memory `Store` whose actual contents are readable, so
/// `a_signing_failure_exits_4_and_uploads_nothing` can assert the bucket was
/// never touched.
///
/// `puts()` LISTS THE BUCKET rather than replaying intercepted calls, and
/// `Deref` hands the real `&Store` to `phase8_score::run` / `phase9_teardown::
/// persist` (whose signatures take `&Store`, exactly as the brief specifies).
/// The alternative — a wrapper that records calls and delegates — could not
/// intercept anything here, because `run` receives the inner `&Store` and
/// calls it directly; a recorded list would then be empty no matter how many
/// objects were uploaded, and every "nothing was uploaded" assertion would
/// pass vacuously. Listing the bucket is also the stronger claim: it observes
/// what is actually stored, not what was attempted.
pub struct RecordingStore {
    pub inner: logweir_engine_oso::storage::Store,
}

impl RecordingStore {
    /// Every key actually present under `logweir/`, sorted.
    pub fn puts(&self) -> Vec<String> {
        self.inner
            .list_keys("logweir/")
            .expect("listing an in-memory store cannot fail")
    }
}

impl std::ops::Deref for RecordingStore {
    type Target = logweir_engine_oso::storage::Store;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub fn recording_store() -> RecordingStore {
    RecordingStore {
        inner: logweir_engine_oso::storage::Store::in_memory("logweir"),
    }
}

pub fn store_without_conditional_put() -> RecordingStore {
    RecordingStore {
        inner: logweir_engine_oso::storage::Store::in_memory_without_conditional_put("logweir"),
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
    /// The scorecard-signing key `phase1_approval::verify` compares the
    /// approver's key against, as an ACTUAL `VerifyingKey` — never a
    /// caller-supplied string (Task 15 fix round 1, review finding F2). Equal
    /// to the approver's own key in `approval_self`'s fixture, a distinct
    /// generated key otherwise.
    pub signing_key: logweir_evidence::keys::VerifyingKey,
    pub before: DateTime<Utc>,
    _dir: tempfile::TempDir,
}

fn approval_fixture(self_attested: bool) -> ApprovalFixture {
    use logweir_evidence::{keys::SigningKey, sign::sign_detached};
    let dir = tempfile::tempdir().unwrap();
    let spec_text = std::fs::read_to_string("../../examples/drill.yaml").unwrap();
    let doc = serde_json::json!({
        "approver": "sre-oncall@example.com",
        "ticket": "CHG-40881",
        "plan_hash": logweir_core::ids::sha256_prefixed(spec_text.as_bytes()),
        "approved_at": "2026-09-02T17:40:00Z",
    });
    let bytes = serde_json::to_vec_pretty(&doc).unwrap();
    let approval = dir.path().join("approval.json");
    std::fs::write(&approval, &bytes).unwrap();

    let approver = if self_attested {
        SigningKey::from_pem_file(std::path::Path::new(
            "../../e2e/fixtures/signed/signing.pem",
        ))
        .unwrap()
    } else {
        SigningKey::generate_p256()
    };
    let side = sign_detached(
        &approver,
        logweir::drill::phase1_approval::PAYLOAD_TYPE_APPROVAL,
        &bytes,
    )
    .unwrap();
    std::fs::write(
        approval.with_extension("sig"),
        serde_json::to_vec(&side).unwrap(),
    )
    .unwrap();

    let approver_pub = dir.path().join("approver.pub.pem");
    write_pub(&approver, &approver_pub);
    let other_pub = dir.path().join("other.pub.pem");
    write_pub(&SigningKey::generate_p256(), &other_pub);

    // The self-attested case's signing key must be the SAME key as the
    // approver's (both loaded from signing.pem) so the comparison inside
    // `verify` is a genuine equality, not a coincidence of two calls to
    // `generate_p256`. The non-self-attested case's signing key is an
    // independently generated key, distinct from both `approver` and
    // `other_pub`'s key.
    let signing_key = if self_attested {
        approver.verifying_key()
    } else {
        SigningKey::generate_p256().verifying_key()
    };

    ApprovalFixture {
        spec_text,
        approval,
        approver_pub,
        other_pub,
        signing_key,
        before: Utc::now(),
        _dir: dir,
    }
}

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

/// Approver key != signing key.
pub fn approval_ok() -> ApprovalFixture {
    approval_fixture(false)
}
/// Approver key == the signing key in e2e/fixtures/signed/signing.pem.
pub fn approval_self() -> ApprovalFixture {
    approval_fixture(true)
}

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

/// (a reader whose target already holds records, an engine that sleeps `ms`).
pub fn engine_that_sleeps_ms(ms: u64) -> (FakeReader, SleepEngine) {
    (
        FakeReader {
            state: target_with("MkU3OEVBNTcwNTJENDM2Qk", "drill-orders", 3, 500, &[]),
        },
        SleepEngine { ms },
    )
}

// ------------------------------------------------------------ orchestrator

/// What the orchestrator fixture should make the drill do. Everything else is
/// held identical, so a test that asserts on the difference is asserting on
/// the one behaviour it names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Drill {
    /// Every phase succeeds and the objectives are met.
    Passes,
    /// The engine's preflight reports `CoverageState::Empty` for the sampled
    /// partition, which `phase5_preflight::adjudicate` blocks on.
    BlocksAtPreflight,
    /// The restore runs, exits 0, and every selected partition on the target
    /// is still at end offset 0 — `phase6_restore::assert_post_condition`'s
    /// `RestoreNoOp`.
    RestoresNothing,
    /// Everything passes, but the engine's preflight takes over a second, so
    /// `phase5_duration_ms` is a number a test can see subtracted from the
    /// scored RTO.
    HasASlowPreflight,
    /// Everything passes, but the engine reports no version and no digest —
    /// the identity a signed scorecard must name.
    NamesNoEngine,
    /// The engine did not honour `restore.header_preflight=full`. Phase 5
    /// blocks on it, and the signed scorecard must say the lever was ignored
    /// rather than claim a matrix pass nobody established.
    IgnoresTheHeaderLever,
    /// The engine logs `Ignoring unknown config key` during the RESTORE — a
    /// readback that arrives at phase 6, after phase 5's own has already been
    /// recorded, and that has to reach the same signed list.
    DropsARenderedKeyDuringRestore,
    /// Every phase RUNS and the restore is byte-perfect, but the newest
    /// restored record lands an hour short of the requested recovery point, so
    /// the measured RPO (3600s) blows the 300s objective. This is the ordinary
    /// real-world exit 2 — "the drill worked, but it missed its objective" —
    /// and it is the only shape that reaches phase 8's SCORING and then the
    /// final non-pass gate. Scores `fail-objective`.
    MissesTheRpoObjective,
    /// Every phase RUNS, and one restored record does not reconcile against
    /// its archive fingerprint. Reaches the same final gate by the other
    /// route. Scores `fail-integrity`.
    ReconcilesWithMismatches,
    /// The drill PASSES every phase and then phase 9's deletion of the one
    /// scratch topic is refused by the broker, per topic — `delete_topics`
    /// returns `Ok(vec![("drill-orders", Err("BROKER: TOPIC_DELETION_DISABLED"))])`.
    ///
    /// This is the T0-11 shape: a drill that verified correctly and could not
    /// clean up. It is the PARTIAL failure (`phase9_teardown::run`'s per-topic
    /// branch), deliberately not `score.rs`'s `RefusingDeleter`, whose whole
    /// call fails and exercises the other branch. The deleter a drill uses
    /// comes from the target client (`c.client.as_deleter()`), not from a
    /// parameter of `execute_with`, so there is no way to run a WHOLE drill
    /// against a refusing deleter without this shape.
    LeavesATopicBehind,
}

pub const FIXTURE_CLUSTER_ID: &str = "MkU3OEVBNTcwNTJENDM2Qk";
pub const FIXTURE_MARKER_TOPIC: &str = "logweir.scratch";
/// Under `logweir/` only because seeding an in-memory `Store` goes through
/// `put_create_only`, which asserts Global Constraint 6 unconditionally and on
/// every handle. A real OSO archive key is never under `logweir/`, and nothing
/// in phase 7 cares: it reads the key back with `Store::get`, which has no
/// prefix rule at all.
pub const FIXTURE_SEGMENT_KEY: &str = "logweir/fixture-archive/orders/0/000000000000.kbak";
pub const FIXTURE_WINDOW_START: &str = "2026-08-29T00:00:00Z";
pub const FIXTURE_WINDOW_END: &str = "2026-08-30T02:00:00Z";
pub const FIXTURE_SAMPLE_RECORDS: usize = 25;
/// How many records the manifest says the sampled window holds — twenty times
/// the canary size, so the two `records_expected` figures cannot be confused.
pub const FIXTURE_WINDOW_RECORDS: i64 = 500;

/// `n` archive fingerprints at offsets 0..n and the matching consumed records,
/// with the LAST record landing exactly on `newest_ms` and the rest one second
/// apart before it. Unlike `matching_pair`, the caller chooses the timestamps,
/// because `measured.rpo_seconds` is
/// `sample.window_end - newest_restored_record` and a drill that is meant to
/// meet an RPO objective has to control both halves of that subtraction.
pub fn orchestrator_pair(
    n: usize,
    newest_ms: i64,
) -> (Vec<RecordFingerprint>, Vec<ConsumedRecord>) {
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
        let tsms = newest_ms - ((n - 1 - i) as i64) * 1000;
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

/// One topic, one partition, one segment, whose `sha256` is the real hash of
/// `segment_bytes` — so `phase7_verify::segment_evidence` reading it back out
/// of the archive store is a genuine comparison, not a fixture that agrees
/// with itself by construction.
pub fn orchestrator_facts(segment_bytes: &[u8]) -> BackupSetFacts {
    let t0 = ts(FIXTURE_WINDOW_START).timestamp_millis();
    let t1 = ts(FIXTURE_WINDOW_END).timestamp_millis();
    BackupSetFacts {
        backup_id: "backup-2026-08-30T02:00:00Z".into(),
        created_at: ts(FIXTURE_WINDOW_END),
        source_cluster_id: Some("SRC0000000000000000000".into()),
        manifest_sha256: format!("sha256:{}", "a".repeat(64)),
        manifest_version_id: None,
        consumer_group_snapshot_sha256: None,
        topics: vec![TopicFacts {
            name: "orders".into(),
            original_partition_count: Some(1),
            source_replication_factor: Some(3),
            configurations: source_configs(&[
                ("cleanup.policy", "compact"),
                ("retention.ms", "604800000"),
            ]),
            partitions: vec![PartitionFacts {
                partition_id: 0,
                segments: vec![SegmentFacts {
                    key: FIXTURE_SEGMENT_KEY.into(),
                    start_offset: 0,
                    end_offset: FIXTURE_WINDOW_RECORDS - 1,
                    start_timestamp: t0,
                    end_timestamp: t1,
                    // The MANIFEST's window total, deliberately much larger
                    // than the canary size: `Selection::records_expected` and
                    // the scorecard's `sample.records_expected` answer
                    // different questions, and a fixture where the two
                    // coincide cannot tell one from the other.
                    record_count: FIXTURE_WINDOW_RECORDS,
                    sha256: logweir_core::ids::sha256_prefixed(segment_bytes),
                    uploaded_at: t1,
                }],
                gaps: vec![],
                pruned: vec![],
            }],
        }],
    }
}

/// A `DataEngine` that answers from memory: no subprocess, no engine binary,
/// no archive credentials. Every method a phase actually calls is answered
/// from a field, so a test can move exactly one of them.
pub struct FixtureEngine {
    pub facts: BackupSetFacts,
    pub set: BackupSetRef,
    pub fingerprints: Vec<RecordFingerprint>,
    pub coverage: CoverageState,
    pub header_honoured: bool,
    /// Makes phase 5 take measurable wall-clock time, so
    /// `rto_excluding_preflight_seconds` differs from `rto_seconds`.
    pub preflight_sleep_ms: u64,
    /// What `restore` reports as dropped config keys. Phase 5's readback and
    /// phase 6's are separate, and both have to reach `engine.levers`.
    pub restore_unknown_keys: Vec<String>,
    /// What `preflight` reports as dropped config keys — the FIRST of the two
    /// readbacks, recorded before phase 6 has run at all.
    pub preflight_unknown_keys: Vec<String>,
    pub version: String,
    pub digest: String,
    /// Every `SampleSelection` `fingerprints()` was asked about, so a test can
    /// assert the orchestrator bound the real `BackupSetRef` into them first.
    pub fingerprint_calls: std::sync::Arc<std::sync::Mutex<Vec<SampleSelection>>>,
    pub restored: std::sync::Arc<std::sync::Mutex<bool>>,
}

impl FixtureEngine {
    fn new(facts: BackupSetFacts, fingerprints: Vec<RecordFingerprint>) -> Self {
        Self {
            set: BackupSetRef {
                backup_id: facts.backup_id.clone(),
                manifest_key: "drills/fixture/manifest.json".into(),
            },
            facts,
            fingerprints,
            coverage: CoverageState::Full,
            header_honoured: true,
            preflight_sleep_ms: 0,
            restore_unknown_keys: Vec::new(),
            preflight_unknown_keys: Vec::new(),
            version: "v0.21.0-fixture".into(),
            digest: format!("sha256:{}", "0".repeat(64)),
            fingerprint_calls: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            restored: std::sync::Arc::new(std::sync::Mutex::new(false)),
        }
    }
}

impl DataEngine for FixtureEngine {
    fn id(&self) -> EngineId {
        EngineId {
            id: "oso-cli".into(),
            version: self.version.clone(),
            digest: self.digest.clone(),
        }
    }
    fn list_backup_sets(&self, _l: &StorageUrl) -> Result<Vec<BackupSetRef>, EngineError> {
        Ok(vec![self.set.clone()])
    }
    fn describe(&self, _s: &BackupSetRef) -> Result<BackupSetFacts, EngineError> {
        Ok(self.facts.clone())
    }
    fn preflight(&self, _p: &RestorePlan) -> Result<PreflightReport, EngineError> {
        if self.preflight_sleep_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(self.preflight_sleep_ms));
        }
        Ok(PreflightReport {
            valid: self.coverage == CoverageState::Full,
            errors: vec![],
            warnings: vec![],
            segments_to_process: 1,
            records_to_restore: FIXTURE_SAMPLE_RECORDS as u64,
            time_range: None,
            partitions: vec![PartitionCoverage {
                topic: "orders".into(),
                partition: 0,
                state: self.coverage.clone(),
                detail: String::new(),
            }],
            header_preflight_honoured: self.header_honoured,
            unknown_key_warnings: self.preflight_unknown_keys.clone(),
            // T0-14: this double never renders a `restore.yaml`, so it has no
            // real digest to report; the all-zero placeholder says exactly
            // that. The real seam is pinned in
            // `crates/logweir-engine-oso/tests/render_equality.rs`.
            rendered_restore_sha256:
                "sha256:0000000000000000000000000000000000000000000000000000000000000000".into(),
        })
    }
    fn restore(
        &self,
        _p: &RestorePlan,
        o: &mut dyn PhaseObserver,
    ) -> Result<RestoreFacts, EngineError> {
        *self.restored.lock().unwrap() = true;
        o.phase_started(6, "restore");
        let started_at = Utc::now();
        // The offset-mapping report, written EXACTLY where the real engine
        // writes it (Task 9b): from the `Ok` arm of a completed restore, to
        // `plan.offset_report`, with a bare write whose failure it does not
        // escalate [U:crates/kafka-backup-core/src/restore/engine.rs:417-427,
        // :1382-1388]. Without this the double would make phase 8's upload
        // untestable in process — the seam would only ever see an absent file.
        //
        // `unwrap()`-free on purpose: mirroring the engine's own `warn!` means
        // a fixture whose workdir was not created behaves like the engine
        // rather than panicking, which is what lets a test assert the
        // presence-tolerant branch too.
        let _ = std::fs::write(&_p.offset_report, OFFSET_REPORT_BYTES);
        o.phase_finished(6, "ok");
        Ok(RestoreFacts {
            started_at,
            finished_at: Utc::now(),
            exit_code: 0,
            unknown_key_warnings: self.restore_unknown_keys.clone(),
        })
    }
    fn fingerprints(&self, s: &SampleSelection) -> Result<Vec<RecordFingerprint>, EngineError> {
        self.fingerprint_calls.lock().unwrap().push(s.clone());
        Ok(self.fingerprints.clone())
    }
}

/// A target cluster that both READS and DELETES, which is what
/// `logweir::drill::TargetClient` needs from one object. `FakeReader` cannot
/// serve here: it consumes nothing and deletes nothing, and phase 7 has to
/// read records back.
pub struct FixtureClient {
    pub cluster_id: String,
    pub topics: Vec<TopicMeta>,
    pub end_offsets: BTreeMap<String, Vec<(i32, i64)>>,
    pub configs: BTreeMap<String, BTreeMap<String, String>>,
    pub records: BTreeMap<String, Vec<ConsumedRecord>>,
    pub deleted: std::sync::Mutex<Vec<String>>,
    /// Every `NewTopicSpec` the drill's creation step handed to `TopicCreator`,
    /// in order (Task 8, guard **G-TS**).
    ///
    /// `Arc`, not a bare `Mutex`, because the client is moved into
    /// `Ctx::client` behind `Box<dyn TargetClient>` and is unreachable from a
    /// test afterwards — the same reason `FixtureEngine::fingerprint_calls` is
    /// shared. `fixtures::created_topics` reads it back, and
    /// `topic_preflight.rs::a_blocked_preflight_creates_no_target_topic` is
    /// what needs it: the ORDER of the creation step against phase 5's verdict
    /// is otherwise pinned only behind Docker (Task 8 review, finding 2).
    pub created: std::sync::Arc<std::sync::Mutex<Vec<logweir_kafka::reader::NewTopicSpec>>>,
    /// Topic names whose deletion the broker refuses, and the error string it
    /// refuses with. Empty for every shape but `Drill::LeavesATopicBehind`, so
    /// every other fixture drill tears down exactly as it always did.
    pub refuses_deletion_of: BTreeMap<String, String>,
}

impl ClusterReader for FixtureClient {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        Ok(self.cluster_id.clone())
    }
    /// The topics this target STARTED with, plus every topic the drill has
    /// created so far (Task 8, guard **G-TS**).
    ///
    /// The second half is not decoration. Phase 0 refuses a plan whose mapped
    /// target topics already exist (spec §6.1), so a fixture target that lists
    /// `drill-orders` from the start is a target every fixture drill is now
    /// refused against. But a double that went on claiming the topic does not
    /// exist *after* Logweir created it would be lying in the other direction,
    /// and phase 7 and phase 9 both read the target after creation. So the
    /// answer is the honest one: absent at phase 0, present from phase 6.
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        let mut out = self.topics.clone();
        for spec in self.created.lock().unwrap().iter() {
            if !out.iter().any(|t| t.name == spec.name) {
                out.push(TopicMeta::new(&spec.name, spec.num_partitions));
            }
        }
        Ok(out)
    }
    fn end_offsets(&self, topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        Ok(self.end_offsets.get(topic).cloned().unwrap_or_default())
    }
    fn topic_configs(&self, topic: &str) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(self.configs.get(topic).cloned().unwrap_or_default())
    }
    /// Task 8 (guard **G-TS**). Empty: this fixture's broker is the Apache
    /// default, so phase 0's preflight observes nothing hostile, refuses
    /// nothing, and the fixture drill still reaches phase 9 exactly as before.
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(BTreeMap::new())
    }
    fn consume_range(
        &self,
        t: &str,
        p: i32,
        from: i64,
        max: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        Ok(self
            .records
            .get(t)
            .map(|rs| {
                rs.iter()
                    .filter(|r| r.partition == p && r.offset >= from)
                    .take(max)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }
}

/// Task 8, guard **G-TS**. `TargetClient` now requires a `TopicCreator`
/// because the rendered `restore.yaml` says `create_topics: false`: the target
/// topics are Logweir's to create. Every `NewTopicSpec` is RECORDED so a test
/// can assert the exact ordered `configs` vector the drill asked for, and every
/// creation succeeds — an already-existing target topic is phase 3's collision
/// to report, and `Drill::LeavesATopicBehind` is about deletion, not creation.
impl logweir_kafka::reader::TopicCreator for FixtureClient {
    fn create_topics(
        &self,
        topics: &[logweir_kafka::reader::NewTopicSpec],
    ) -> Result<Vec<(String, Result<(), String>)>, KafkaError> {
        self.created.lock().unwrap().extend_from_slice(topics);
        Ok(topics.iter().map(|t| (t.name.clone(), Ok(()))).collect())
    }
}

impl TopicDeleter for FixtureClient {
    fn delete_topics(
        &self,
        names: &[String],
    ) -> Result<Vec<(String, Result<(), String>)>, KafkaError> {
        // Every name is still RECORDED as attempted; only the ANSWER differs.
        // A refusal the fixture hid from `deleted` would make the double
        // disagree with a real broker, which does receive the request.
        self.deleted.lock().unwrap().extend_from_slice(names);
        Ok(names
            .iter()
            .map(|n| match self.refuses_deletion_of.get(n) {
                Some(e) => (n.clone(), Err(e.clone())),
                None => (n.clone(), Ok(())),
            })
            .collect())
    }
}

/// A whole drill run, wired against doubles: `RunArgs` on real files, a `Ctx`
/// whose engine, target client and both stores answer from memory.
///
/// `logweir::drill::execute` (the public entry the CLI calls) constructs the
/// real `Ctx` itself and therefore needs a live broker and the extracted
/// engine binary — Task 21c's territory. `execute_with` is the same phase
/// sequence over a `Ctx` the caller supplies, which is what makes the
/// score-before-sign ordering testable at all here.
pub struct OrchestratorFixture {
    pub args: logweir::drill::RunArgs,
    pub run_id: String,
    pub ctx: logweir::drill::Ctx,
    /// The exact bytes the archive holds for the one fixture segment.
    pub segment_bytes: Vec<u8>,
    pub out: PathBuf,
    pub metrics: PathBuf,
    /// Shared with the `FixtureEngine` inside `ctx`, which is otherwise
    /// unreachable behind `Box<dyn DataEngine>`.
    pub fingerprint_calls: std::sync::Arc<std::sync::Mutex<Vec<SampleSelection>>>,
    /// Shared with the `FixtureClient` inside `ctx`, which is otherwise
    /// unreachable behind `Box<dyn TargetClient>` — every `NewTopicSpec` the
    /// drill's creation step asked for, in order (Task 8, guard **G-TS**).
    pub created_topics: std::sync::Arc<std::sync::Mutex<Vec<logweir_kafka::reader::NewTopicSpec>>>,
    _dir: tempfile::TempDir,
}

pub fn orchestrator_args_against_fixture_engine() -> OrchestratorFixture {
    orchestrator_fixture(Drill::Passes)
}

pub fn orchestrator_fixture(shape: Drill) -> OrchestratorFixture {
    use logweir_evidence::{keys::SigningKey, sign::sign_detached};

    let dir = tempfile::tempdir().unwrap();
    let window_end_ms = ts(FIXTURE_WINDOW_END).timestamp_millis();

    // ---- the spec, and the approval signed over its exact bytes ----
    let spec_text = format!(
        "source:\n  \
           storage:\n    backend: filesystem\n    path: /logweir-fixture-archive\n  \
           backup: latestCompleted\n  \
           topics: [orders]\n\
         target:\n  \
           bootstrap_servers: [localhost:9092]\n  \
           marker_topic: {FIXTURE_MARKER_TOPIC}\n  \
           topic_mapping_prefix: \"drill-\"\n  \
           default_replication_factor: 1\n  \
           teardown: delete\n\
         sample:\n  \
           window_start: \"{FIXTURE_WINDOW_START}\"\n  \
           window_end: \"{FIXTURE_WINDOW_END}\"\n  \
           records_per_partition: {FIXTURE_SAMPLE_RECORDS}\n  \
           anchor: head\n\
         objectives:\n  rto_seconds: 900\n  rpo_seconds: 300\n  pass_rate: 1.0\n\
         evidence:\n  backend: filesystem\n  path: /logweir-fixture-evidence\n\
         notifications:\n  webhooks: []\n"
    );
    let spec_path = dir.path().join("drill.yaml");
    std::fs::write(&spec_path, &spec_text).unwrap();

    let signing_pem = dir.path().join("signer.pem");
    std::fs::copy("../../e2e/fixtures/signed/signing.pem", &signing_pem).unwrap();
    let signer = SigningKey::from_pem_file(&signing_pem).unwrap();

    let approval_doc = serde_json::json!({
        "approver": "sre-oncall@example.com",
        "ticket": "CHG-40881",
        "plan_hash": logweir_core::ids::sha256_prefixed(spec_text.as_bytes()),
        "approved_at": "2026-09-02T17:40:00Z",
    });
    let approval_bytes = serde_json::to_vec_pretty(&approval_doc).unwrap();
    let approval = dir.path().join("approval.json");
    std::fs::write(&approval, &approval_bytes).unwrap();
    let side = sign_detached(
        &signer,
        logweir::drill::phase1_approval::PAYLOAD_TYPE_APPROVAL,
        &approval_bytes,
    )
    .unwrap();
    std::fs::write(
        approval.with_extension("sig"),
        serde_json::to_vec(&side).unwrap(),
    )
    .unwrap();
    let approver_pub = dir.path().join("approver.pub.pem");
    write_pub(&signer, &approver_pub);

    let allowed_path = dir.path().join("allowed-clusters.json");
    std::fs::write(
        &allowed_path,
        serde_json::to_vec(&serde_json::json!({
            "allowed_cluster_ids": [FIXTURE_CLUSTER_ID],
            "source_cluster_id": serde_json::Value::Null,
        }))
        .unwrap(),
    )
    .unwrap();

    // ---- the archive: one segment, stored under its own manifest key ----
    let segment_bytes = b"logweir-fixture-segment-0".to_vec();
    let archive = logweir_engine_oso::storage::Store::in_memory("");
    archive
        .put_create_only(FIXTURE_SEGMENT_KEY, &segment_bytes)
        .unwrap();

    let facts = orchestrator_facts(&segment_bytes);
    // An hour short of the requested recovery point: `measured.rpo_seconds` is
    // `sample.window_end - newest_restored_record`, so this is 3600 against a
    // 300s objective. The archive fingerprints are built from the SAME
    // timestamps, so integrity still passes — the drill is healthy and simply
    // did not recover far enough, which is what `fail-objective` means.
    let newest_ms = if shape == Drill::MissesTheRpoObjective {
        window_end_ms - 3_600_000
    } else {
        window_end_ms
    };
    let (fps, mut records) = orchestrator_pair(FIXTURE_SAMPLE_RECORDS, newest_ms);
    if shape == Drill::ReconcilesWithMismatches {
        // One record on the TARGET differs from the bytes its archive
        // fingerprint was computed over. `compare` must report exactly one
        // mismatch, and `roll_up` must turn that into `IntegrityResult::Fail`.
        records[0].value = Some(b"tampered".to_vec());
    }

    let mut engine = FixtureEngine::new(facts, fps);
    match shape {
        Drill::BlocksAtPreflight => engine.coverage = CoverageState::Empty,
        Drill::HasASlowPreflight => engine.preflight_sleep_ms = 1_100,
        Drill::NamesNoEngine => {
            engine.version = String::new();
            engine.digest = String::new();
        }
        Drill::IgnoresTheHeaderLever => engine.header_honoured = false,
        Drill::DropsARenderedKeyDuringRestore => {
            engine.preflight_unknown_keys = vec!["restore.header_preflight".into()];
            engine.restore_unknown_keys = vec!["restore.checkpoint_interval_secs".into()];
        }
        // `LeavesATopicBehind` differs from `Passes` in the TARGET CLIENT's
        // deleter only (see `refuses_deletion_of` below); the engine it runs
        // against is byte-for-byte the passing one, which is what makes "the
        // drill verified correctly and could not clean up" the single variable.
        Drill::Passes
        | Drill::RestoresNothing
        | Drill::MissesTheRpoObjective
        | Drill::ReconcilesWithMismatches
        | Drill::LeavesATopicBehind => {}
    }

    // ---- the target cluster ----
    let restored_hi = if shape == Drill::RestoresNothing {
        0
    } else {
        FIXTURE_SAMPLE_RECORDS as i64
    };
    let client = FixtureClient {
        cluster_id: FIXTURE_CLUSTER_ID.into(),
        // The MARKER only. `drill-orders` was here until Task 8's fix round 1:
        // phase 0 refuses a plan whose mapped target topics already exist
        // (spec §6.1), so a target that starts with `drill-orders` present is
        // refused at phase 0 and no fixture drill would reach phase 1. It
        // appears in `list_topics` from the moment the drill creates it, which
        // is what phases 7 and 9 read.
        topics: vec![TopicMeta::new(FIXTURE_MARKER_TOPIC, 1)],
        end_offsets: [("drill-orders".to_string(), vec![(0, restored_hi)])]
            .into_iter()
            .collect(),
        configs: [(
            "drill-orders".to_string(),
            target_configs(&[("cleanup.policy", "delete"), ("retention.ms", "604800000")]),
        )]
        .into_iter()
        .collect(),
        records: [("drill-orders".to_string(), records)]
            .into_iter()
            .collect(),
        deleted: std::sync::Mutex::new(Vec::new()),
        created: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        // T0-11. The one shape whose broker refuses a deletion; every other
        // shape gets an empty map and the deleter it always had.
        refuses_deletion_of: if shape == Drill::LeavesATopicBehind {
            [(
                "drill-orders".to_string(),
                "BROKER: TOPIC_DELETION_DISABLED".to_string(),
            )]
            .into_iter()
            .collect()
        } else {
            BTreeMap::new()
        },
    };

    let spec: logweir_core::spec::DrillSpec = serde_yaml::from_str(&spec_text).unwrap();
    let allowed: logweir_core::spec::AllowedClusters =
        serde_json::from_slice(&std::fs::read(&allowed_path).unwrap()).unwrap();

    let out = dir.path().join("scorecard.json");
    let metrics = dir.path().join("logweir.prom");
    let fingerprint_calls = engine.fingerprint_calls.clone();
    let created_topics = client.created.clone();
    OrchestratorFixture {
        args: logweir::drill::RunArgs {
            spec: spec_path,
            approval,
            approver_key: approver_pub,
            allowed_clusters: allowed_path,
            signing_key: signing_pem,
            triggered_by: Some("fixture".into()),
            out: Some(out.clone()),
            metrics_file: Some(metrics.clone()),
            // `None` — so the plan's DEFAULT applies and the offset report
            // lands beside the checkpoint in this run's own workdir, which is
            // the directory `execute_with_outcome` creates and the one
            // `FixtureEngine::restore` writes into (Task 9b).
            offset_report_out: None,
        },
        run_id: logweir::ids::new_run_id(),
        ctx: logweir::drill::Ctx {
            spec,
            spec_text,
            allowed,
            client: Box::new(client),
            engine: Box::new(engine),
            archive,
            store: logweir_engine_oso::storage::Store::in_memory("logweir"),
        },
        segment_bytes,
        out,
        metrics,
        fingerprint_calls,
        created_topics,
        _dir: dir,
    }
}

/// Every `SampleSelection` the fixture engine was asked to fingerprint, so a
/// test can assert the orchestrator bound the real `BackupSetRef` into them
/// first. Reaches through `Ctx::engine`'s trait object by keeping the
/// `FixtureEngine` reachable, which is why `orchestrator_fixture` holds one.
pub fn fingerprint_calls(f: &OrchestratorFixture) -> Vec<SampleSelection> {
    f.fingerprint_calls.lock().unwrap().clone()
}

/// Every `NewTopicSpec` the drill's creation step handed to the target client,
/// in order, so a test can assert WHEN in the phase sequence it ran — an empty
/// list after a run that ended at phase 5 is the assertion that creation
/// happens after phase 5's verdict (Task 8, guard **G-TS**). Reaches through
/// `Ctx::client`'s trait object the same way `fingerprint_calls` reaches
/// through `Ctx::engine`'s.
pub fn created_topics(f: &OrchestratorFixture) -> Vec<logweir_kafka::reader::NewTopicSpec> {
    f.created_topics.lock().unwrap().clone()
}
