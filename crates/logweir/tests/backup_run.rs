//! `logweir backup run`'s named tests (Task 4).
//!
//! **Every test here runs in process**, against a `ClusterReader` double, a
//! `DataEngine` double and `Store::in_memory` — no socket, no subprocess, no
//! broker. That is not a convenience: a binary cannot be handed an in-process
//! double, and with the compose stack down an rdkafka metadata call blocks on
//! `crates/logweir-kafka/src/rdkafka_reader.rs:16`'s
//! `const T: Duration = Duration::from_secs(20)`. The tree has already measured
//! the consequence — `crates/logweir/tests/no_network_in_unit_tests.rs:11-13`
//! records `tests/doctor.rs::check_7_…` at **26.60 s** against Global
//! Constraint 22's 15 s per-test bound. The ONE binary-level assertion in
//! Tasks 4, 5b and 6 lives in `e2e/tests/backup_argv.rs`, behind
//! `#![cfg(feature = "e2e")]`, because it runs the binary.
//!
//! `run_with` returns an `ExitCode`, so the refusal rows assert on it and the
//! outcome rows call `execute_with` — its outcome-returning half — because a
//! `BackupOutcome` FIELD is what they are about and an exit code cannot carry
//! one. Both are the seam; neither names a client constructor.
use logweir::backup::{execute_with, run_with, BackupError, BackupRunArgs};
use logweir::exit::ExitCode;
use logweir_core::backup_receipt::BackupReceipt;
use logweir_core::engine::*;
use logweir_engine_oso::storage::Store;
use logweir_evidence::keys::SigningKey;
use logweir_kafka::reader::{ClusterReader, ConsumedRecord, KafkaError, TopicMeta};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// The archive prefix every fixture uses.
///
/// It is under `logweir/` for one mechanical reason and no semantic one:
/// `Store::in_memory` is the only writable store a test can build without a
/// backend, and `Store::put_create_only` asserts `key.starts_with("logweir/")`
/// (Global Constraint 6) before it writes anything. So an in-memory ARCHIVE
/// fixture has to be seeded under that root even though a real archive never
/// is. Nothing in production reads or writes an archive at this prefix.
const ARCHIVE_PREFIX: &str = "logweir/";

/// A `BackupSpec` as an adopter writes it. `bootstrap_servers` is
/// `localhost:9092` and is DATA: it is handed to a `ClusterReader` double and
/// no client is ever constructed from it. That is the reason recorded for this
/// file in `no_network_in_unit_tests.rs`'s `ALLOWED`.
fn spec_yaml(backup_id: &str, topics: &str, extra: &str) -> String {
    format!(
        "backup_id: {backup_id}\n\
         source:\n\
        \x20 bootstrap_servers: [localhost:9092]\n\
        \x20 topics: {topics}\n\
        {extra}\
         storage:\n\
        \x20 backend: s3\n\
        \x20 bucket: kafka-backups\n\
        \x20 prefix: {ARCHIVE_PREFIX}\n\
        \x20 region: us-east-1\n\
        \x20 endpoint: http://127.0.0.1:19000\n\
        \x20 path_style: true\n\
        \x20 allow_http: true\n\
         backup:\n\
        \x20 compression: zstd\n\
        \x20 segment_max_records: 1000\n\
        \x20 segment_max_bytes: 10485760\n\
        \x20 max_concurrent_partitions: 3\n"
    )
}

fn allowed_json(ids: &[&str]) -> String {
    let list: Vec<String> = ids.iter().map(|i| format!("\"{i}\"")).collect();
    format!("{{\"allowed_cluster_ids\": [{}]}}", list.join(", "))
}

struct Fixture {
    _dir: tempfile::TempDir,
    args: BackupRunArgs,
}

fn fixture(spec: &str, allowed: &str) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let spec_path = dir.path().join("backup.yaml");
    let allowed_path = dir.path().join("allowed-clusters.json");
    std::fs::write(&spec_path, spec).unwrap();
    std::fs::write(&allowed_path, allowed).unwrap();
    // AN EPHEMERAL KEY, GENERATED HERE, and never a committed private key.
    // Task 5b signs the receipt with `--signing-key`, so every success-path
    // row needs a real key — and the throwaway fixture key under
    // `e2e/fixtures/signed/` is reserved for the corpus walkers, which verify
    // against a checked-in PUBLIC pem. This one lives and dies with the
    // `TempDir`; the matching public pem is written beside it so the I6 row
    // can verify what the run signed.
    let key = SigningKey::generate_p256();
    let key_path = dir.path().join("signer.pem");
    std::fs::write(&key_path, key.to_pkcs8_pem().unwrap()).unwrap();
    std::fs::write(
        dir.path().join("signing.pub.pem"),
        key.verifying_key().to_public_key_pem().unwrap(),
    )
    .unwrap();
    Fixture {
        _dir: dir,
        args: BackupRunArgs {
            // D2 §3.5: a standalone invocation is not under the store contract.
            store_contract_version: None,
            spec: spec_path,
            allowed_clusters: allowed_path,
            signing_key: key_path,
            triggered_by: None,
            out: None,
            receipt_out: None,
            backup_id_override: None,
        },
    }
}

/// The default happy-path fixture: one topic, an allowlist that does NOT name
/// the source cluster (rail 4 admits it).
fn ok_fixture(backup_id: &str) -> Fixture {
    fixture(
        &spec_yaml(backup_id, "[orders]", ""),
        &allowed_json(&["SCRATCH-CLUSTER-0000001"]),
    )
}

/// An in-memory archive holding exactly one manifest, for `backup_id`.
///
/// The BYTES' content is irrelevant to every assertion here — the double's
/// `describe` supplies the facts — but their DIGEST is not: `phase_run` hashes
/// the exact bytes it read back, so this fixture is what
/// `BackupOutcome.manifest_sha256` is a digest OF.
fn archive_with_one_manifest(backup_id: &str) -> (Store, String, Vec<u8>) {
    let store = Store::in_memory(ARCHIVE_PREFIX);
    let key = format!("{ARCHIVE_PREFIX}{backup_id}/manifest.json");
    let bytes = format!("{{\"backup_id\":\"{backup_id}\",\"topics\":[]}}").into_bytes();
    store.put_create_only(&key, &bytes).unwrap();
    (store, key, bytes)
}

/// An archive holding a manifest for EACH id.
///
/// Every test that must fail at ASSERTION time under a mutant seeds every id
/// the mutant could pick: with only the expected id present, a mutant that
/// chooses the other one is caught by `phase_run`'s "holds no backup set"
/// refusal and the test dies at an `unwrap` on the `Err` instead of at the
/// assertion the mutant is supposed to break. Same reason the refusal rows
/// below seed an archive at all: with an EMPTY one, deleting a guard reports
/// exit 1 (nothing to read back) and the test would pass for the wrong reason
/// rather than reporting exit 0 against an expected 3.
fn archive_with_manifests(ids: &[&str]) -> Store {
    let store = Store::in_memory(ARCHIVE_PREFIX);
    for id in ids {
        let key = format!("{ARCHIVE_PREFIX}{id}/manifest.json");
        let bytes = format!("{{\"backup_id\":\"{id}\",\"topics\":[]}}").into_bytes();
        store.put_create_only(&key, &bytes).unwrap();
    }
    store
}

/// An archive with nothing in it at all.
fn empty_archive() -> Store {
    Store::in_memory(ARCHIVE_PREFIX)
}

// ---------------------------------------------------------------------------
// Doubles
// ---------------------------------------------------------------------------

/// A `ClusterReader` whose every response is configured by the test. Scoped to
/// this file: the shared `FakeReader` in `crates/logweir/tests/fixtures/mod.rs`
/// is built around a drill's `TargetState` and carries assumptions phase −1
/// has no use for.
struct StubReader {
    cluster_id: Result<String, KafkaError>,
}

impl StubReader {
    fn answering(id: &str) -> Self {
        Self {
            cluster_id: Ok(id.to_string()),
        }
    }
    fn unreachable() -> Self {
        Self {
            cluster_id: Err(KafkaError::Unreachable("no broker answered".into())),
        }
    }
}

impl ClusterReader for StubReader {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        self.cluster_id.clone()
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        Ok(vec![])
    }
    fn end_offsets(&self, _topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        Ok(vec![])
    }
    fn topic_configs(&self, _topic: &str) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(BTreeMap::new())
    }
    /// Task 8 (guard **G-TS**) added this to `ClusterReader`. An empty map is
    /// a broker that surfaces neither `log.message.timestamp.type` nor a
    /// timestamp bound, which is the harmless case: phase 0's preflight then
    /// treats the broker as the Apache default (`CreateTime`) and refuses
    /// nothing. G-TS's own arms live in
    /// `crates/logweir/tests/topic_preflight.rs`.
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(BTreeMap::new())
    }
    fn consume_range(
        &self,
        _topic: &str,
        _partition: i32,
        _from: i64,
        _max: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        Ok(vec![])
    }
}

/// One segment, spelled out, so a test can say what the archive reports.
fn segment(record_count: i64, start_timestamp: i64, end_timestamp: i64) -> SegmentFacts {
    SegmentFacts {
        key: "seg".into(),
        start_offset: 0,
        end_offset: record_count.max(1) - 1,
        start_timestamp,
        end_timestamp,
        record_count,
        sha256: String::new(),
        uploaded_at: 0,
    }
}

fn topic_facts(name: &str, segments: Vec<SegmentFacts>) -> TopicFacts {
    TopicFacts {
        name: name.into(),
        original_partition_count: Some(1),
        source_replication_factor: Some(1),
        configurations: BTreeMap::new(),
        partitions: vec![PartitionFacts {
            partition_id: 0,
            segments,
            gaps: vec![],
            pruned: vec![],
        }],
    }
}

/// A `DataEngine` that RECORDS the `BackupPlan` it was handed and answers
/// `describe` from a value the test supplies. It renders nothing and spawns
/// nothing, which is what makes the whole default suite socket-free and
/// subprocess-free. Rendering happens inside `OsoCliEngine::backup`, not in
/// this seam, so what a SCRAM spec proves here is that the MODE reached
/// `BackupOutcome.source_auth` and the plan the engine was handed — the BYTES
/// are `crates/logweir-engine-oso/tests/render_scram.rs`'s four goldens.
struct RecordingEngine {
    plans: Mutex<Vec<BackupPlan>>,
    /// Overrides `id()`. `None` on every row but the engine-identity one.
    identity: Option<EngineId>,
    /// `None` => the engine fails, as a real one exiting non-zero does.
    facts: Option<BackupFacts>,
    topics: Vec<TopicFacts>,
    /// When set, simulates a projected Secret rotating while the engine is
    /// running. Receipt signing must still use the signer validated before
    /// this callback was reached.
    rotate_signing_file_to: Option<(PathBuf, String)>,
}

impl RecordingEngine {
    fn ok(topics: Vec<TopicFacts>) -> Self {
        Self {
            plans: Mutex::new(Vec::new()),
            identity: None,
            facts: Some(BackupFacts {
                started_at: ts("2026-09-09T00:00:00Z"),
                finished_at: ts("2026-09-09T00:01:00Z"),
                exit_code: 0,
                unknown_key_warnings: vec![],
            }),
            topics,
            rotate_signing_file_to: None,
        }
    }
    fn one_topic() -> Self {
        Self::ok(vec![topic_facts(
            "orders",
            vec![segment(7, 1_756_000_000_000, 1_756_000_060_000)],
        )])
    }
    fn failing() -> Self {
        Self {
            plans: Mutex::new(Vec::new()),
            identity: None,
            facts: None,
            topics: vec![],
            rotate_signing_file_to: None,
        }
    }
    /// The engine's IDENTITY, overridden — for the row that asserts a receipt
    /// must name the image it ran (GC7). `None` means "answer `id()` from the
    /// defaults below".
    fn with_identity(mut self, version: &str, digest: &str) -> Self {
        self.identity = Some(EngineId {
            id: "double".into(),
            version: version.into(),
            digest: digest.into(),
        });
        self
    }
    fn rotating_signing_file_to(mut self, path: &Path, replacement_pem: String) -> Self {
        self.rotate_signing_file_to = Some((path.to_path_buf(), replacement_pem));
        self
    }
    fn recorded_plan(&self) -> BackupPlan {
        self.plans
            .lock()
            .unwrap()
            .first()
            .cloned()
            .expect("the engine's backup subcommand was never reached")
    }
}

fn ts(s: &str) -> chrono::DateTime<chrono::Utc> {
    s.parse().unwrap()
}

impl DataEngine for RecordingEngine {
    fn id(&self) -> EngineId {
        self.identity.clone().unwrap_or(EngineId {
            id: "double".into(),
            version: "v0.21.0".into(),
            digest: "sha256:0".into(),
        })
    }
    fn list_backup_sets(&self, _: &StorageUrl) -> Result<Vec<BackupSetRef>, EngineError> {
        unimplemented!("the backup path lists through the Store handle, not the engine")
    }
    fn describe(&self, set: &BackupSetRef) -> Result<BackupSetFacts, EngineError> {
        Ok(BackupSetFacts {
            backup_id: set.backup_id.clone(),
            created_at: ts("2026-09-09T00:01:00Z"),
            source_cluster_id: None,
            manifest_sha256: "sha256:from-the-engines-own-handle".into(),
            manifest_version_id: None,
            consumer_group_snapshot_sha256: None,
            topics: self.topics.clone(),
        })
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
        _obs: &mut dyn PhaseObserver,
    ) -> Result<BackupFacts, EngineError> {
        self.plans.lock().unwrap().push(plan.clone());
        if let Some((path, replacement_pem)) = &self.rotate_signing_file_to {
            std::fs::write(path, replacement_pem).map_err(|e| {
                EngineError::Operational(format!("could not rotate test signing file: {e}"))
            })?;
        }
        match &self.facts {
            Some(f) => Ok(f.clone()),
            None => Err(EngineError::Operational(
                "kafka-backup backup exited 1".into(),
            )),
        }
    }
}

/// The refusal MESSAGE, for the rows that assert on it. `run_with` answers the
/// exit code and this answers the wording; a bare code assertion can pass for
/// the wrong reason.
fn guard_message(err: BackupError) -> String {
    match err {
        BackupError::Guard(refusal) => refusal.0,
        other => panic!("expected a guard refusal (exit 3), got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Phase −1: the refusals (exit 3)
// ---------------------------------------------------------------------------

/// GC18(c) rail 3 / Global Constraint 4, over the SPEC TEXT — so a forbidden
/// key survives round-tripping through a struct that has no field for it.
#[test]
fn backup_run_refuses_a_forbidden_key_in_the_spec() {
    let f = fixture(
        &spec_yaml("mvp-demo", "[orders]", "dry_run: true\n"),
        &allowed_json(&["SCRATCH-CLUSTER-0000001"]),
    );
    let reader = StubReader::answering("SOURCE-CLUSTER-00000001");
    let engine = RecordingEngine::one_topic();
    // A READABLE archive: with an empty one, deleting this guard would report
    // exit 1 ("holds no backup set") and the row would pass for the wrong
    // reason. Seeded, the mutant reports exit 0 against an expected 3.
    let store = archive_with_manifests(&["mvp-demo"]);

    assert_eq!(
        run_with(&f.args, &reader, &engine, &store),
        ExitCode::GuardRefused
    );
    let msg = guard_message(
        execute_with(&f.args, "run-1", &reader, &engine, &store, &store).unwrap_err(),
    );
    assert_eq!(
        msg,
        "forbidden key(s) present in the backup spec, at any value: dry_run. \
         purge_topics is irreversible, absent from the engine's dry run, has no \
         confirmation gate and truncates EVERY partition of each target topic \
         regardless of partition or time-window filters; dry_run would make the \
         restore a no-op and the measured RTO meaningless; header_preflight_external \
         would silently disable the header scan the drill depends on."
    );
    assert!(
        engine.plans.lock().unwrap().is_empty(),
        "a refused plan must never reach the engine"
    );
}

/// GC18(c) rail 1 / **G-GLOB**. `orders*` is one entry the engine would read
/// as a pattern, so the archive would hold topics the plan never named.
#[test]
fn backup_run_refuses_a_glob_topic() {
    let f = fixture(
        &spec_yaml("mvp-demo", "[\"orders*\"]", ""),
        &allowed_json(&["SCRATCH-CLUSTER-0000001"]),
    );
    let reader = StubReader::answering("SOURCE-CLUSTER-00000001");
    let engine = RecordingEngine::one_topic();
    let store = archive_with_manifests(&["mvp-demo"]);

    assert_eq!(
        run_with(&f.args, &reader, &engine, &store),
        ExitCode::GuardRefused
    );
    let msg = guard_message(
        execute_with(&f.args, "run-1", &reader, &engine, &store, &store).unwrap_err(),
    );
    assert!(msg.contains("source topic `orders*`"), "{msg}");
    assert!(msg.contains("glob metacharacter"), "{msg}");
    assert!(
        msg.contains("mandatory named-topic allowlist with no wildcard"),
        "{msg}"
    );
}

/// GC18(c)'s rail 1 again, from the other end: an allowlist whose absence
/// means "everything" is not an allowlist. `serde` makes the field required,
/// so the shape a templating mistake actually produces is the EMPTY list.
#[test]
fn backup_run_refuses_an_empty_topic_list() {
    let f = fixture(
        &spec_yaml("mvp-demo", "[]", ""),
        &allowed_json(&["SCRATCH-CLUSTER-0000001"]),
    );
    let reader = StubReader::answering("SOURCE-CLUSTER-00000001");
    let engine = RecordingEngine::one_topic();
    let store = archive_with_manifests(&["mvp-demo"]);

    assert_eq!(
        run_with(&f.args, &reader, &engine, &store),
        ExitCode::GuardRefused
    );
    let msg = guard_message(
        execute_with(&f.args, "run-1", &reader, &engine, &store, &store).unwrap_err(),
    );
    assert_eq!(
        msg,
        "a backup spec must name at least one topic; GC18(c) requires a mandatory \
         named-topic allowlist"
    );
}

/// **GC18(c) rail 4.** The observed SOURCE cluster appears in
/// `allowedClusterIds`, which is the restore-TARGET allowlist.
///
/// The fixture leaves `source_cluster_id` ABSENT from the allowlist file
/// (`spec.rs:336` is `Option<String>` with `#[serde(default)]`), which is what
/// makes this test kill the mutant that reads the rail from that field
/// instead: such a mutant refuses nothing.
#[test]
fn backup_run_refuses_a_source_that_is_a_permitted_target() {
    let f = fixture(
        &spec_yaml("mvp-demo", "[orders]", ""),
        &allowed_json(&["CID-A"]),
    );
    let reader = StubReader::answering("CID-A");
    let engine = RecordingEngine::one_topic();
    // Seeded, so a mutant that DELETES rail 4 — or reads it from
    // `allowed.source_cluster_id`, which this fixture leaves absent — reports
    // exit 0 against an expected 3 rather than dying on an unrelated read.
    let store = archive_with_manifests(&["mvp-demo"]);

    assert_eq!(
        run_with(&f.args, &reader, &engine, &store),
        ExitCode::GuardRefused
    );
    let msg = guard_message(
        execute_with(&f.args, "run-1", &reader, &engine, &store, &store).unwrap_err(),
    );
    assert_eq!(
        msg,
        "source cluster id CID-A is also listed in allowedClusterIds, which \
         is the restore-TARGET allowlist; a cluster cannot be both the source of an \
         archive and a permitted scratch target"
    );
    assert!(
        engine.plans.lock().unwrap().is_empty(),
        "a refused plan must never reach the engine"
    );
}

/// The allowlist file the rail reads has no `source_cluster_id` key at all —
/// pinned so the fixture above cannot quietly grow one and blunt its own
/// mutant.
#[test]
fn the_rail_4_fixture_names_no_source_cluster_id() {
    let json = allowed_json(&["CID-A"]);
    assert!(!json.contains("source_cluster_id"), "{json}");
    let parsed: logweir_core::spec::AllowedClusters = serde_json::from_str(&json).unwrap();
    assert!(parsed.source_cluster_id.is_none());
}

// ---------------------------------------------------------------------------
// Phase −1: what is OBSERVED, and what is not a refusal
// ---------------------------------------------------------------------------

/// The source cluster id is a MEASURED fact. `BackupPlan` carries no field for
/// it precisely so an adopter-supplied string cannot stand where one belongs,
/// and this test proves the code does not go looking for one: the spec text
/// contains `CID-FROM-SPEC` and the broker answers `CID-FROM-BROKER`.
#[test]
fn backup_run_reads_the_source_cluster_id_from_the_broker() {
    let f = fixture(
        &spec_yaml("CID-FROM-SPEC", "[orders]", ""),
        &allowed_json(&["SCRATCH-CLUSTER-0000001"]),
    );
    let spec_text = std::fs::read_to_string(&f.args.spec).unwrap();
    assert!(
        spec_text.contains("CID-FROM-SPEC"),
        "the fixture must offer the wrong answer for the code to prefer"
    );
    let reader = StubReader::answering("CID-FROM-BROKER");
    let engine = RecordingEngine::one_topic();
    let (store, _key, _bytes) = archive_with_one_manifest("CID-FROM-SPEC");
    // ONE STORE PER RUN: each run claims `CID-FROM-SPEC` (RECEIPT-DUP's execution
    // claim), so a second run over the same store is — correctly — refused
    // as `ExecutionAlreadyClaimed` before it reaches what this row asserts.
    let (store2, _key2, _bytes2) = archive_with_one_manifest("CID-FROM-SPEC");

    let outcome = execute_with(&f.args, "run-1", &reader, &engine, &store, &store).unwrap();
    assert_eq!(outcome.source_cluster_id, "CID-FROM-BROKER");
    assert_eq!(run_with(&f.args, &reader, &engine, &store2), ExitCode::Ok);
}

/// A broker that cannot answer is an OPERATIONAL failure (exit 1) — the plan
/// may be perfectly fine and the right action is to retry — never a refusal.
/// Phase 0 has the same pair of tests for the same reason.
#[test]
fn an_unreachable_source_cluster_is_operational_not_a_guard_refusal() {
    let f = ok_fixture("mvp-demo");
    let reader = StubReader::unreachable();
    let engine = RecordingEngine::one_topic();
    let store = empty_archive();

    assert_eq!(
        run_with(&f.args, &reader, &engine, &store),
        ExitCode::Operational
    );
    match execute_with(&f.args, "run-1", &reader, &engine, &store, &store).unwrap_err() {
        BackupError::Kafka(_) => {}
        other => panic!("expected BackupError::Kafka (exit 1), got {other:?}"),
    }
}

/// A refusal that needs no broker must not need one. The reader here answers
/// nothing at all, and the plan is still refused with the glob message.
#[test]
fn a_local_refusal_does_not_need_a_reachable_broker() {
    let f = fixture(
        &spec_yaml("mvp-demo", "[\"orders*\"]", ""),
        &allowed_json(&["SCRATCH-CLUSTER-0000001"]),
    );
    let reader = StubReader::unreachable();
    let engine = RecordingEngine::one_topic();
    let store = archive_with_manifests(&["mvp-demo"]);

    assert_eq!(
        run_with(&f.args, &reader, &engine, &store),
        ExitCode::GuardRefused
    );
    let msg = guard_message(
        execute_with(&f.args, "run-1", &reader, &engine, &store, &store).unwrap_err(),
    );
    assert!(msg.contains("glob metacharacter"), "{msg}");
}

// ---------------------------------------------------------------------------
// The run, the read-back, and I10
// ---------------------------------------------------------------------------

/// **I10.** The same fixture, run twice: once with `--backup-id-override`, once
/// without. The override replaces the derived id in the OUTCOME and in the
/// RENDERED DOCUMENT; without it the derived id is the spec's own `backup_id`
/// and `sched-7` appears nowhere.
///
/// The rendered document is produced from the plan the engine actually
/// received (`render_backup::render` is a pure function of `BackupPlan`), so
/// this asserts what the engine would have been handed as `--config`, not a
/// string the test built.
#[test]
fn backup_id_override_replaces_the_derived_id() {
    // With the override.
    let mut f = ok_fixture("mvp-demo");
    f.args.backup_id_override = Some("sched-7".to_string());
    let reader = StubReader::answering("SOURCE-CLUSTER-00000001");
    let engine = RecordingEngine::one_topic();
    // BOTH ids in the archive: a mutant that ignores the override and keeps
    // `mvp-demo` still completes the read-back, so this row fails at the
    // assertion below rather than at an `unwrap` on a missing backup set.
    let store = archive_with_manifests(&["sched-7", "mvp-demo"]);

    let outcome = execute_with(&f.args, "run-1", &reader, &engine, &store, &store).unwrap();
    assert_eq!(outcome.backup_id, "sched-7");
    let doc = logweir_engine_oso::render_backup::render(&engine.recorded_plan()).unwrap();
    assert!(
        doc.contains("backup_id: \"sched-7\""),
        "the rendered document does not carry the overridden id:\n{doc}"
    );

    // Without it.
    let f2 = ok_fixture("mvp-demo");
    let engine2 = RecordingEngine::one_topic();
    let store2 = archive_with_manifests(&["sched-7", "mvp-demo"]);
    let outcome2 = execute_with(&f2.args, "run-2", &reader, &engine2, &store2, &store2).unwrap();
    assert_eq!(outcome2.backup_id, "mvp-demo");
    let doc2 = logweir_engine_oso::render_backup::render(&engine2.recorded_plan()).unwrap();
    assert!(
        !doc2.contains("sched-7"),
        "the derived run must not carry the override's id:\n{doc2}"
    );
    assert!(doc2.contains("backup_id: \"mvp-demo\""), "{doc2}");
}

/// `BackupOutcome.source_auth` is filled from `spec.source.auth` through
/// `AuthSpec::to_render()` (Task 6 replaced Task 4's explicit `match` with that
/// call and changed nothing else), so Task 5b has a populated field.
///
/// A SCRAM spec is RECORDED faithfully here and never downgraded — and since
/// Task 6 it is also RENDERED. It used to be refused at render time by Task 3's
/// typed `RenderError::UnsupportedAuthMode` inside `OsoCliEngine::backup`; that
/// arm now emits the engine's `security:` block, and
/// `e2e/tests/backup_argv.rs::the_real_engine_accepts_the_rendered_sasl_block`
/// proves the pinned engine loads it with no dropped key and no parse error.
/// The broader SCRAM acceptance lives in `crates/logweir/tests/auth_binding.rs`.
#[test]
fn backup_run_records_the_source_auth() {
    let f = fixture(
        &spec_yaml(
            "mvp-demo",
            "[orders]",
            "  auth:\n    mode: scramSha512\n    username: logweir\n",
        ),
        &allowed_json(&["SCRATCH-CLUSTER-0000001"]),
    );
    let reader = StubReader::answering("SOURCE-CLUSTER-00000001");
    let engine = RecordingEngine::one_topic();
    let (store, _k, _b) = archive_with_one_manifest("mvp-demo");

    let outcome = execute_with(&f.args, "run-1", &reader, &engine, &store, &store).unwrap();
    assert_eq!(
        outcome.source_auth,
        AuthRender::ScramSha512 {
            username: "logweir".into(),
            tls: false,
            tls_ca_file: None,
        }
    );
    // And the plan the engine was handed carries the same value — the field is
    // not a decoration on the outcome.
    assert_eq!(
        engine.recorded_plan().source_auth,
        AuthRender::ScramSha512 {
            username: "logweir".into(),
            tls: false,
            tls_ca_file: None,
        }
    );

    // The default, for contrast: no `auth` block at all is plaintext.
    let f2 = ok_fixture("mvp-demo");
    let engine2 = RecordingEngine::one_topic();
    let (store2, _k2, _b2) = archive_with_one_manifest("mvp-demo");
    let outcome2 = execute_with(&f2.args, "run-2", &reader, &engine2, &store2, &store2).unwrap();
    assert_eq!(outcome2.source_auth, AuthRender::Plaintext);
}

/// The read-back is what turns "the engine exited 0" into a measured result: a
/// backup of empty topics also exits 0. Every value here comes from the
/// ARCHIVE, and the manifest digest is over the exact bytes this run read —
/// not over the engine handle's own number, which a receipt quoting it would
/// be attesting without ever having seen the bytes.
#[test]
fn the_read_back_reports_the_archives_own_counts_window_and_manifest_digest() {
    // The spec names BOTH topics the archive reports. Task 4's version named
    // only `orders` while the double described `orders` and `payments`, and
    // Task 5b's receipt refuses exactly that document: arm 3 requires the
    // counted set to BE the named set. Naming both keeps every assertion below
    // unchanged and makes the fixture a run the product could really have
    // taken; the filtering half — an archive topic this plan did NOT name — is
    // `the_receipt_counts_only_the_topics_the_plan_named` below.
    let f = fixture(
        &spec_yaml("mvp-demo", "[orders, payments]", ""),
        &allowed_json(&["SCRATCH-CLUSTER-0000001"]),
    );
    let reader = StubReader::answering("SOURCE-CLUSTER-00000001");
    let engine = RecordingEngine::ok(vec![
        topic_facts(
            "orders",
            vec![
                segment(4, 1_756_000_000_000, 1_756_000_030_000),
                segment(3, 1_756_000_030_001, 1_756_000_060_000),
            ],
        ),
        topic_facts(
            "payments",
            vec![segment(11, 1_755_999_000_000, 1_756_000_010_000)],
        ),
    ]);
    let (store, key, bytes) = archive_with_one_manifest("mvp-demo");

    let outcome = execute_with(&f.args, "run-1", &reader, &engine, &store, &store).unwrap();
    assert_eq!(outcome.manifest_key, key);
    assert_eq!(
        outcome.manifest_sha256,
        logweir_core::ids::sha256_prefixed(&bytes)
    );
    assert_eq!(
        outcome.records_per_topic,
        BTreeMap::from([
            ("orders".to_string(), 7u64),
            ("payments".to_string(), 11u64)
        ])
    );
    // The union of every segment's window, across every NAMED topic — not any
    // one topic's, and not the manifest's own declared range.
    //
    // `covered_to_ms` is the newest `end_timestamp` PLUS ONE MILLISECOND,
    // because the window is half-open and `to_ms` is EXCLUSIVE (interface I22;
    // `config/crd/backups.yaml` documents `status.windowCovered.toMs` that way
    // and `BackupReceipt`'s arm 4 requires `from_ms < to_ms` strictly since
    // Task 5b). The conversion is `phase_run`'s, once, where the window is
    // measured.
    assert_eq!(outcome.covered_from_ms, 1_755_999_000_000);
    assert_eq!(
        outcome.covered_to_ms, 1_756_000_060_001,
        "the newest segment ends at 1756000060000 INCLUSIVE, so the exclusive bound is one \
         millisecond later"
    );
    assert_eq!(outcome.facts.exit_code, 0);
    assert_eq!(outcome.run_id, "run-1");
}

/// The engine failing is exit 1, not a refusal: nothing about the plan was
/// found wanting.
#[test]
fn an_engine_failure_is_operational_not_a_guard_refusal() {
    let f = ok_fixture("mvp-demo");
    let reader = StubReader::answering("SOURCE-CLUSTER-00000001");
    let engine = RecordingEngine::failing();
    let (store, _k, _b) = archive_with_one_manifest("mvp-demo");
    // ONE STORE PER RUN: each run claims `mvp-demo` (RECEIPT-DUP's execution
    // claim), so a second run over the same store is — correctly — refused
    // as `ExecutionAlreadyClaimed` before it reaches what this row asserts.
    let (store2, _k2, _b2) = archive_with_one_manifest("mvp-demo");

    assert_eq!(
        run_with(&f.args, &reader, &engine, &store),
        ExitCode::Operational
    );
    match execute_with(&f.args, "run-1", &reader, &engine, &store2, &store2).unwrap_err() {
        BackupError::Engine(_) => {}
        other => panic!("expected BackupError::Engine (exit 1), got {other:?}"),
    }
}

/// A clean exit with no archive to show for it is exit 1 and says so. The
/// alternative — reporting success — is the defect `scripts/e2e-seed.sh`'s own
/// header warns about: "the command exited 0" is not evidence that an archive
/// exists.
#[test]
fn a_missing_backup_set_after_a_clean_exit_is_operational() {
    let f = ok_fixture("mvp-demo");
    let reader = StubReader::answering("SOURCE-CLUSTER-00000001");
    let engine = RecordingEngine::one_topic();
    let store = empty_archive();
    // ONE STORE PER RUN: each run claims `mvp-demo` (RECEIPT-DUP's execution
    // claim), so a second run over the same store is — correctly — refused
    // as `ExecutionAlreadyClaimed` before it reaches what this row asserts.
    let store2 = empty_archive();

    assert_eq!(
        run_with(&f.args, &reader, &engine, &store),
        ExitCode::Operational
    );
    let err = execute_with(&f.args, "run-1", &reader, &engine, &store2, &store2).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("holds no backup set `mvp-demo`"), "{msg}");
}

// ---------------------------------------------------------------------------
// The receipt: I6, I7 and Global Constraint 6 (Task 5b)
// ---------------------------------------------------------------------------

/// The two keys every receipt row expects, for a given run.
fn expected_keys(backup_id: &str, run_id: &str) -> (String, String) {
    (
        format!("logweir/backups/{backup_id}/{run_id}.receipt.json"),
        format!("logweir/backups/{backup_id}/{run_id}.receipt.sig"),
    )
}

/// `logweir drill verify --payload-type backup-receipt`, IN PROCESS, over
/// bytes on disk. `verify::run` is the exact function `main.rs` dispatches to,
/// so this is the shipped reader and not a re-implementation of it.
fn verify_receipt(dir: &Path, doc: &[u8], sidecar: &[u8], public_pem: &Path) -> ExitCode {
    let doc_path = dir.join("receipt-under-test.json");
    let sig_path = dir.join("receipt-under-test.sig");
    std::fs::write(&doc_path, doc).unwrap();
    std::fs::write(&sig_path, sidecar).unwrap();
    logweir::verify::run(&doc_path, &sig_path, public_pem, "backup-receipt")
}

/// **The acceptance for I6's evidence half, and for Global Constraint 6.**
///
/// One `run_with` against the in-memory store puts EXACTLY THREE objects under
/// `logweir/backups/<backup_id>/`, all create-only — the execution claim
/// (RECEIPT-DUP) and the receipt pair — and the pair verifies
/// under the receipt's own payload type — signature AND all five invariants,
/// which is what `--payload-type backup-receipt` checks since Task 5b.
///
/// In process, with doubles: no `logweir` binary, no subprocess, no broker
/// (critique A F1). The signing key is the fixture's EPHEMERAL key.
#[test]
fn backup_run_writes_a_signed_receipt() {
    let f = ok_fixture("mvp-demo");
    let reader = StubReader::answering("SOURCE-CLUSTER-00000001");
    let engine = RecordingEngine::one_topic();
    let (store, _k, _b) = archive_with_one_manifest("mvp-demo");

    let outcome = execute_with(&f.args, "run-1", &reader, &engine, &store, &store).unwrap();
    let (receipt_key, sidecar_key) = expected_keys("mvp-demo", "run-1");
    assert_eq!(outcome.receipt_key, receipt_key);
    assert_eq!(outcome.sidecar_key, sidecar_key);

    // EXACTLY THREE objects under this run's evidence prefix: the two the
    // outcome names, and the execution claim every run takes before its
    // engine starts (RECEIPT-DUP) — the one execution-scoped key there. The
    // archive fixture's own manifest lives at `logweir/mvp-demo/…`, outside
    // this prefix, so the count is about the evidence and nothing else.
    let mut written = store.list_keys("logweir/backups/mvp-demo/").unwrap();
    written.sort();
    assert_eq!(
        written,
        vec![
            logweir::backup::phase_run::claim_key("mvp-demo"),
            receipt_key.clone(),
            sidecar_key.clone()
        ],
        "one run writes exactly its execution claim, the receipt and its detached sidecar, \
         under `logweir/` (Global Constraint 6)"
    );

    // CREATE-ONLY. A second put of the same key is REFUSED, never overwritten,
    // so one run can never silently replace another's evidence — asserted by
    // trying it rather than by reading `put_create_only`'s source.
    let again = store.put_create_only(&receipt_key, b"{}");
    assert!(
        again.is_err(),
        "the receipt key must be create-only; a second put has to be refused"
    );

    // …and the pair verifies, through the shipped reader.
    let (doc, _v) = store.get(&receipt_key).unwrap();
    let (sig, _v2) = store.get(&sidecar_key).unwrap();
    let dir = f._dir.path();
    assert_eq!(
        verify_receipt(dir, &doc, &sig, &dir.join("signing.pub.pem")),
        ExitCode::Ok,
        "`logweir drill verify --payload-type backup-receipt` must exit 0 over the bytes \
         this run signed and put"
    );
    assert_eq!(
        outcome.receipt_sha256,
        logweir_core::ids::sha256_prefixed(&doc),
        "the reported capture digest is over the exact stored receipt bytes"
    );

    // The document says what the run measured — spot-checked on the fields an
    // auditor reads first, so a receipt full of defaults cannot pass this row.
    let receipt: BackupReceipt = serde_json::from_slice(&doc).unwrap();
    assert_eq!(receipt.format_version, "1.0.0");
    assert_eq!(receipt.run_id, "run-1");
    assert_eq!(receipt.backup_id, "mvp-demo");
    assert_eq!(receipt.source.cluster_id, "SOURCE-CLUSTER-00000001");
    assert_eq!(receipt.source.auth.mode, "plaintext");
    assert_eq!(receipt.source.auth.username, None);
    assert_eq!(receipt.source.topics, vec!["orders".to_string()]);
    assert_eq!(receipt.engine.id, "double");
    assert_eq!(receipt.engine.digest, "sha256:0");
    assert_eq!(receipt.exit_code, 0);
    assert_eq!(
        receipt.records,
        BTreeMap::from([("orders".to_string(), 7u64)])
    );
    assert_eq!(receipt.covered.from_ms, 1_756_000_000_000);
    // EXCLUSIVE (I22): the newest segment ends at …060000 inclusive.
    assert_eq!(receipt.covered.to_ms, 1_756_000_060_001);
    assert_eq!(
        receipt.archive.prefix, ARCHIVE_PREFIX,
        "the prefix is the archive's own, from the spec"
    );
}

/// Ed25519 is an explicit backup acceptance path, not inferred from shared
/// parsing or from restore coverage. The verifier consumes the exact bytes
/// retrieved from the evidence store and the public half retained before the
/// run, independently of receipt persistence.
#[test]
fn backup_ed25519_receipt_is_independently_verified() {
    let f = ok_fixture("ed25519-backup");
    let signer = SigningKey::generate_ed25519();
    let public = signer.verifying_key();
    std::fs::write(&f.args.signing_key, signer.to_pkcs8_pem().unwrap()).unwrap();

    let reader = StubReader::answering("SOURCE-CLUSTER-00000001");
    let engine = RecordingEngine::one_topic();
    let (store, _k, _b) = archive_with_one_manifest("ed25519-backup");
    let outcome = execute_with(&f.args, "ed25519-run", &reader, &engine, &store, &store)
        .expect("an Ed25519-backed backup succeeds");

    let (document, _) = store.get(&outcome.receipt_key).unwrap();
    let (sidecar_bytes, _) = store.get(&outcome.sidecar_key).unwrap();
    let sidecar: logweir_evidence::Sidecar = serde_json::from_slice(&sidecar_bytes).unwrap();
    let verified_key_id = logweir_evidence::verify::verify_detached(
        &public,
        logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT,
        &document,
        &sidecar,
    )
    .expect("the exact stored receipt bytes verify with the Ed25519 public key");

    assert_eq!(verified_key_id, public.key_id());
    assert_eq!(sidecar.signatures.len(), 1);
    assert_eq!(sidecar.signatures[0].keyid, public.key_id());
    assert!(matches!(
        public,
        logweir_evidence::keys::VerifyingKey::Ed25519(_)
    ));
}

/// **I6's local half.** `--receipt-out <path>` leaves the receipt at `<path>`
/// and its DSSE sidecar at the same path with the extension replaced by
/// `.sig` — the pairing `drill run --out` already uses — and the local pair
/// verifies on its own, read directly from disk.
#[test]
fn receipt_out_writes_both_files() {
    let mut f = ok_fixture("mvp-demo");
    let dir = f._dir.path().to_path_buf();
    let out = dir.join("receipt.json");
    f.args.receipt_out = Some(out.clone());
    let reader = StubReader::answering("SOURCE-CLUSTER-00000001");
    let engine = RecordingEngine::one_topic();
    let (store, _k, _b) = archive_with_one_manifest("mvp-demo");

    assert_eq!(run_with(&f.args, &reader, &engine, &store), ExitCode::Ok);

    let sig = dir.join("receipt.sig");
    assert!(out.exists(), "--receipt-out must write the receipt bytes");
    assert!(
        sig.exists(),
        "the DSSE sidecar lands beside it with the extension replaced by `.sig`, which is \
         the pairing `drill run --out` already uses"
    );

    // The local bytes are the SIGNED bytes: verified as read, never
    // re-serialised.
    assert_eq!(
        logweir::verify::run(&out, &sig, &dir.join("signing.pub.pem"), "backup-receipt"),
        ExitCode::Ok,
        "the local pair must verify on its own"
    );

    // And they are byte-identical to what was PUT, or an auditor holding the
    // local copy would be checking a different document from the one in the
    // bucket.
    let (receipt_key, sidecar_key) = expected_keys("mvp-demo", "");
    let keys = store.list_keys("logweir/backups/mvp-demo/").unwrap();
    let put_doc = keys
        .iter()
        .find(|k| k.ends_with(".receipt.json"))
        .expect("the receipt was put");
    let put_sig = keys
        .iter()
        .find(|k| k.ends_with(".receipt.sig"))
        .expect("the sidecar was put");
    assert!(
        put_doc.starts_with(receipt_key.trim_end_matches(".receipt.json"))
            && put_sig.starts_with(sidecar_key.trim_end_matches(".receipt.sig")),
        "the keys are under this backup's prefix"
    );
    assert_eq!(store.get(put_doc).unwrap().0, std::fs::read(&out).unwrap());
    assert_eq!(store.get(put_sig).unwrap().0, std::fs::read(&sig).unwrap());
}

/// `--out` is honoured exactly as `--receipt-out` is (they name the one
/// document this command writes), and naming two DIFFERENT paths is refused
/// locally, before anything runs.
#[test]
fn out_is_the_same_flag_and_two_different_paths_are_refused() {
    // `--out` alone works.
    let mut f = ok_fixture("mvp-demo");
    let out = f._dir.path().join("elsewhere.json");
    f.args.out = Some(out.clone());
    let reader = StubReader::answering("SOURCE-CLUSTER-00000001");
    let engine = RecordingEngine::one_topic();
    let (store, _k, _b) = archive_with_one_manifest("mvp-demo");
    assert_eq!(run_with(&f.args, &reader, &engine, &store), ExitCode::Ok);
    assert!(out.exists() && f._dir.path().join("elsewhere.sig").exists());

    // Two different paths for one document: refused, exit 1, with no engine
    // run and no broker read.
    let mut g = ok_fixture("mvp-demo");
    g.args.receipt_out = Some(g._dir.path().join("a.json"));
    g.args.out = Some(g._dir.path().join("b.json"));
    let unreachable = StubReader::unreachable();
    let engine2 = RecordingEngine::one_topic();
    let (store2, _k2, _b2) = archive_with_one_manifest("mvp-demo");
    assert_eq!(
        run_with(&g.args, &unreachable, &engine2, &store2),
        ExitCode::Operational
    );
    let msg = execute_with(&g.args, "run-1", &unreachable, &engine2, &store2, &store2)
        .unwrap_err()
        .to_string();
    assert!(msg.contains("name DIFFERENT paths"), "{msg}");
    assert!(
        !msg.contains("no broker answered"),
        "a property of the FLAGS must be refused before the network step: {msg}"
    );
    assert!(
        engine2.plans.lock().unwrap().is_empty(),
        "the engine must not run when the flags cannot be honoured"
    );

    // The same path twice is NOT a refusal: `--receipt-out` wins and the one
    // document lands where both flags asked for it.
    let mut h = ok_fixture("mvp-demo");
    let same = h._dir.path().join("same.json");
    h.args.receipt_out = Some(same.clone());
    h.args.out = Some(same.clone());
    let engine3 = RecordingEngine::one_topic();
    let (store3, _k3, _b3) = archive_with_one_manifest("mvp-demo");
    assert_eq!(run_with(&h.args, &reader, &engine3, &store3), ExitCode::Ok);
    assert!(same.exists());
}

/// Missing, malformed and unreadable signing material are prerequisite
/// failures: exit 4, no engine backup call, and no uploaded evidence. The
/// malformed value includes a sentinel that must never appear in the error.
#[test]
fn invalid_signing_material_stops_before_engine_data_work() {
    for case in ["missing", "malformed", "unreadable"] {
        let mut f = ok_fixture("mvp-demo");
        let key_path = f._dir.path().join(format!("{case}-signer.pem"));
        match case {
            "missing" => {}
            "malformed" => std::fs::write(
                &key_path,
                "-----BEGIN PRIVATE KEY-----\nDO-NOT-ECHO-KEY-MATERIAL\n-----END PRIVATE KEY-----\n",
            )
            .unwrap(),
            // A directory at the requested file path reliably fails a read on
            // Unix without depending on whether the test process is root.
            "unreadable" => std::fs::create_dir(&key_path).unwrap(),
            _ => unreachable!(),
        }
        f.args.signing_key = key_path.clone();
        let reader = StubReader::answering("SOURCE-CLUSTER-00000001");
        let engine = RecordingEngine::one_topic();
        // Seed the archive so deleting early validation lets the old late
        // failure path complete all engine/read-back work before it fails.
        let (store, _k, _b) = archive_with_one_manifest("mvp-demo");

        let err = execute_with(&f.args, "run-1", &reader, &engine, &store, &store)
            .expect_err("invalid signing material must refuse the execution");
        assert_eq!(err.exit_code(), ExitCode::SigningOrLock, "{case}: {err}");
        assert!(matches!(&err, BackupError::Signing(_)), "{case}: {err:?}");
        let msg = err.to_string();
        assert!(
            msg.contains(&key_path.display().to_string()),
            "{case}: {msg}"
        );
        assert!(
            msg.contains("Mount a readable P-256 or Ed25519 PKCS#8 PEM private key"),
            "{case}: {msg}"
        );
        assert!(
            msg.contains("No engine data operation was started"),
            "{case}: {msg}"
        );
        assert!(
            !msg.contains("DO-NOT-ECHO-KEY-MATERIAL"),
            "{case}: key contents leaked: {msg}"
        );
        assert!(
            engine.plans.lock().unwrap().is_empty(),
            "{case}: invalid signing material must be detected before DataEngine::backup"
        );
        assert!(
            store
                .list_keys("logweir/backups/mvp-demo/")
                .unwrap()
                .is_empty(),
            "{case}: prerequisite failure must upload no evidence"
        );
    }
}

/// A projected key may rotate while the engine is working. One execution
/// keeps using the signer it validated before that work, so the receipt
/// verifies with the original public key and not the replacement key.
#[test]
fn a_rotated_signing_file_does_not_change_the_validated_execution_signer() {
    let f = ok_fixture("mvp-demo");
    let replacement = SigningKey::generate_p256();
    let replacement_pem = replacement.to_pkcs8_pem().unwrap();
    let replacement_public = f._dir.path().join("replacement.pub.pem");
    std::fs::write(
        &replacement_public,
        replacement.verifying_key().to_public_key_pem().unwrap(),
    )
    .unwrap();
    let reader = StubReader::answering("SOURCE-CLUSTER-00000001");
    let engine = RecordingEngine::one_topic()
        .rotating_signing_file_to(&f.args.signing_key, replacement_pem.clone());
    let (store, _k, _b) = archive_with_one_manifest("mvp-demo");

    let outcome = execute_with(&f.args, "run-1", &reader, &engine, &store, &store).unwrap();
    assert_eq!(
        std::fs::read_to_string(&f.args.signing_key).unwrap(),
        replacement_pem,
        "the engine double must actually rotate the projected file during the run"
    );
    let (doc, _) = store.get(&outcome.receipt_key).unwrap();
    let (sidecar, _) = store.get(&outcome.sidecar_key).unwrap();
    assert_eq!(
        verify_receipt(
            f._dir.path(),
            &doc,
            &sidecar,
            &f._dir.path().join("signing.pub.pem")
        ),
        ExitCode::Ok,
        "the receipt must use the signer validated before engine work"
    );
    assert_ne!(
        verify_receipt(f._dir.path(), &doc, &sidecar, &replacement_public),
        ExitCode::Ok,
        "reopening the rotated key after engine work would make this verification pass"
    );
}

/// An empty `engine.version`/`engine.digest` is refused BEFORE the engine
/// runs: a signed receipt must name the engine image it ran (GC7 pins by
/// digest, and a receipt naming only a version would be satisfied by any
/// binary claiming it).
///
/// Task 4 left this to Task 5b in as many words, because Task 4 signed
/// nothing.
#[test]
fn an_unnamed_engine_is_refused_before_the_backup_runs() {
    for (version, digest, want) in [
        ("", "sha256:0", "engine.version"),
        ("v0.21.0", "", "engine.digest"),
    ] {
        let f = ok_fixture("mvp-demo");
        let reader = StubReader::answering("SOURCE-CLUSTER-00000001");
        let engine = RecordingEngine::one_topic().with_identity(version, digest);
        let (store, _k, _b) = archive_with_one_manifest("mvp-demo");

        assert_eq!(
            run_with(&f.args, &reader, &engine, &store),
            ExitCode::Operational
        );
        let msg = execute_with(&f.args, "run-1", &reader, &engine, &store, &store)
            .unwrap_err()
            .to_string();
        assert!(msg.contains(want), "{msg}");
        assert!(msg.contains("NO backup was taken"), "{msg}");
        assert!(
            engine.plans.lock().unwrap().is_empty(),
            "the engine must not run when the receipt could not name it"
        );
    }
}

/// The receipt counts the topics the PLAN named, and only those — which is
/// what makes `BackupReceipt`'s arm 3 satisfiable by construction.
///
/// `DataEngine::describe` answers about the whole backup SET: a set an earlier
/// run appended to holds that run's topics too. Counting them would produce a
/// receipt describing two runs at once, and arm 3 would refuse it — an archive
/// on disk with no evidence for it, on a path an adopter reaches by reusing a
/// `backup_id`. A named topic the archive does not mention keeps its `0`,
/// because a backup of an empty topic is a real backup.
#[test]
fn the_receipt_counts_only_the_topics_the_plan_named() {
    let f = fixture(
        &spec_yaml("mvp-demo", "[orders, ledger]", ""),
        &allowed_json(&["SCRATCH-CLUSTER-0000001"]),
    );
    let reader = StubReader::answering("SOURCE-CLUSTER-00000001");
    // The archive reports `orders` (named) and `payments` (NOT named); the
    // plan also names `ledger`, which the archive does not mention.
    let engine = RecordingEngine::ok(vec![
        topic_facts(
            "orders",
            vec![segment(4, 1_756_000_000_000, 1_756_000_030_000)],
        ),
        topic_facts(
            "payments",
            vec![segment(99, 1_700_000_000_000, 1_800_000_000_000)],
        ),
    ]);
    let (store, _k, _b) = archive_with_one_manifest("mvp-demo");

    let outcome = execute_with(&f.args, "run-1", &reader, &engine, &store, &store).unwrap();
    assert_eq!(
        outcome.records_per_topic,
        BTreeMap::from([("ledger".to_string(), 0u64), ("orders".to_string(), 4u64)]),
        "one entry per NAMED topic and no others; a named topic with no segment is 0"
    );
    // `payments`'s segment spans 1.7e12..1.8e12 and must not widen this run's
    // window: the window is over the named topics' segments.
    assert_eq!(outcome.covered_from_ms, 1_756_000_000_000);
    assert_eq!(outcome.covered_to_ms, 1_756_000_030_001);

    // And the receipt that comes out of it satisfies its own arm 3 — asserted
    // through the reader, because that is the claim.
    let (doc, _v) = store.get(&outcome.receipt_key).unwrap();
    let (sig, _v2) = store.get(&outcome.sidecar_key).unwrap();
    let dir = f._dir.path();
    assert_eq!(
        verify_receipt(dir, &doc, &sig, &dir.join("signing.pub.pem")),
        ExitCode::Ok
    );
}

/// **I7.** The final two stdout lines of a successful `backup run` are
/// `receipt-key=<key>` then `sidecar-key=<key>`, in that order, with nothing
/// after them.
///
/// # Why this row re-execs the test binary
///
/// The claim is about the PROCESS's stdout, and `println!` cannot be captured
/// in process without an fd redirect (which would mean a new dependency,
/// forbidden by Global Constraint 38) — while `run_with`'s doubles cannot be
/// handed to the `logweir` binary, which would additionally need a broker and
/// a bucket. So the parent runs THIS binary's `#[ignore]`d child row, which
/// performs the same in-process `run_with` against the same doubles and calls
/// `std::process::exit` so that libtest's own summary never reaches stdout
/// after the two lines under test.
///
/// The child asserts nothing about ordering; the parent asserts everything,
/// over bytes the child actually wrote to fd 1.
#[test]
fn the_runner_prints_its_two_evidence_keys_last() {
    let out = Command::new(std::env::current_exe().expect("this test binary's own path"))
        .args([
            "--ignored",
            "--exact",
            "the_i7_child_runs_one_backup_and_exits",
            "--nocapture",
        ])
        .output()
        .expect("re-exec this test binary");
    let stdout = String::from_utf8(out.stdout).expect("stdout is utf-8");
    assert_eq!(
        out.status.code(),
        Some(0),
        "the child must succeed, or its stdout is not a successful run's stdout. \
         stdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let lines: Vec<&str> = stdout.lines().collect();
    let last_two = &lines[lines.len().saturating_sub(2)..];
    assert_eq!(
        last_two.len(),
        2,
        "a successful run prints at least the two evidence keys, got:\n{stdout}"
    );
    assert!(
        last_two[0].starts_with("receipt-key=logweir/backups/i7-demo/")
            && last_two[0].ends_with(".receipt.json"),
        "the PENULTIMATE line is `receipt-key=<key>`, got {:?} in:\n{stdout}",
        last_two[0]
    );
    let digest_lines: Vec<&str> = lines
        .iter()
        .copied()
        .filter(|line| line.starts_with("receipt-sha256="))
        .collect();
    assert_eq!(
        digest_lines.len(),
        1,
        "one public capture digest is emitted before I7's final key pair: {stdout}"
    );
    let digest = digest_lines[0]
        .strip_prefix("receipt-sha256=sha256:")
        .expect("the canonical digest prefix");
    assert_eq!(digest.len(), 64);
    assert!(digest
        .bytes()
        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
    assert!(
        lines.iter().position(|line| *line == digest_lines[0])
            < lines.iter().position(|line| *line == last_two[0]),
        "the digest precedes the final two lines and does not break old consumers"
    );
    assert!(
        last_two[1].starts_with("sidecar-key=logweir/backups/i7-demo/")
            && last_two[1].ends_with(".receipt.sig"),
        "the FINAL line is `sidecar-key=<key>`, got {:?} in:\n{stdout}",
        last_two[1]
    );
    // The ORDER is the contract, and this is the assertion the "swap the two
    // lines" mutant fails at: with the two `println!`s exchanged, the
    // penultimate line is the sidecar and the two `starts_with` rows above
    // both fail.
    assert!(
        stdout.ends_with(&format!("{}\n{}\n", last_two[0], last_two[1])),
        "nothing may be printed after the two keys — a controller reads the LAST lines of \
         the pod log, which has no stream selector:\n{stdout}"
    );
    // The two keys name one run: same prefix, same run id stem.
    let stem = |line: &str| {
        line.split('=')
            .nth(1)
            .unwrap()
            .trim_end_matches(".receipt.json")
            .trim_end_matches(".receipt.sig")
            .to_string()
    };
    assert_eq!(
        stem(last_two[0]),
        stem(last_two[1]),
        "the two keys must name the same run"
    );
}

/// **D3 §2.4, the backup half.** The five named steps are announced at `-1`,
/// in order, and interface I7's two keys are still the final two lines.
///
/// The backup path has no numbered phases after admission, so §2.4 gives it
/// `admit`, `engine`, `readback`, `sign`, `upload` — the boundaries a
/// controller can actually act on: "the engine is still running" and "the
/// archive exists and the receipt is being signed" are different waits.
///
/// The same child, and the same fd-1 argument, as the I7 row above.
#[test]
fn the_backup_runner_announces_its_five_named_steps_before_the_evidence_keys() {
    let out = Command::new(std::env::current_exe().expect("this test binary's own path"))
        .args([
            "--ignored",
            "--exact",
            "the_i7_child_runs_one_backup_and_exits",
            "--nocapture",
        ])
        .output()
        .expect("re-exec this test binary");
    let stdout = String::from_utf8(out.stdout).expect("stdout is utf-8");
    assert_eq!(out.status.code(), Some(0), "{stdout}");
    let lines: Vec<&str> = stdout.lines().collect();

    assert!(
        lines.contains(&"progress-contract=2"),
        "the channel announces its version once, before the steps:\n{stdout}"
    );
    let mut at = 0usize;
    for step in logweir::backup::PROGRESS_STEPS {
        let expected = format!("progress-phase=-1:{step}");
        let found = lines[at..]
            .iter()
            .position(|l| *l == expected)
            .unwrap_or_else(|| panic!("`{expected}` is missing or out of order in:\n{stdout}"));
        at += found + 1;
    }
    // And I7 is unchanged: the progress lines all come BEFORE the keys.
    let receipt_at = lines
        .iter()
        .position(|l| l.starts_with("receipt-key="))
        .expect("interface I7's first key");
    assert!(
        at <= receipt_at,
        "every progress line comes before interface I7's pair:\n{stdout}"
    );
    assert!(
        !lines[receipt_at..]
            .iter()
            .any(|l| l.starts_with("progress-")),
        "nothing may be printed after I7's two keys:\n{stdout}"
    );
}

/// The child process of `the_runner_prints_its_two_evidence_keys_last`.
///
/// `#[ignore]`d so the default suite never runs it: on its own it asserts only
/// that the run succeeds, and it ends in `std::process::exit`, which would
/// truncate a normal `cargo test` run's own output. The parent drives it with
/// `--ignored --exact`.
#[test]
#[ignore = "driven as a child process by the_runner_prints_its_two_evidence_keys_last"]
fn the_i7_child_runs_one_backup_and_exits() {
    let f = ok_fixture("i7-demo");
    let reader = StubReader::answering("SOURCE-CLUSTER-00000001");
    let engine = RecordingEngine::one_topic();
    let (store, _k, _b) = archive_with_one_manifest("i7-demo");
    let code = run_with(&f.args, &reader, &engine, &store);
    assert_eq!(code, ExitCode::Ok, "the child must take a real backup");
    // EXIT HERE. libtest would otherwise print `test … ok` and its summary
    // AFTER the two lines the parent is asserting are last.
    std::process::exit(0);
}

// ---------------------------------------------------------------------------
// Structural claims, read off this directory's own source
// ---------------------------------------------------------------------------

fn backup_src(name: &str) -> String {
    let p = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/backup")
        .join(name);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("{}: {e}", p.display()))
}

const BACKUP_SOURCES: [&str; 3] = ["mod.rs", "phase_minus1_admit.rs", "phase_run.rs"];

/// **GC18(c) rail 2.** The backup path never scopes a deleter, and therefore
/// can delete nothing: the topic-deleting call in
/// `crates/logweir-kafka/src/rdkafka_reader.rs:446-460` refuses every name it
/// is handed until a scratch namespace has been configured, and nothing here
/// configures one.
///
/// A SOURCE-READING test, so the claim cannot drift back silently — and read
/// over the RAW text, comments included, which is why nothing under
/// `src/backup/` names either identifier even in prose (the module doc says
/// so).
#[test]
fn the_backup_path_never_scopes_a_deleter() {
    for name in BACKUP_SOURCES {
        let src = backup_src(name);
        for token in ["with_scratch_prefix", "delete_topics"] {
            assert!(
                !src.contains(token),
                "crates/logweir/src/backup/{name} names `{token}`. GC18(c) rail 2: a backup \
                 must not be able to delete a topic on the SOURCE cluster, and an unscoped \
                 reader physically cannot. If this is a comment, reword it — this test reads \
                 raw source on purpose."
            );
        }
    }
}

/// `run` builds the handles; `run_with` and everything below it take them as
/// parameters. Constructing the reader inside `run_with` would put a 20-second
/// rdkafka metadata timeout into the default suite for every test in this
/// file (`rdkafka_reader.rs:16`), which is what Global Constraint 22's 15 s
/// per-test bound and `scripts/time-unit-suite.sh` exist to catch — but a
/// timing gate reports a symptom, and this reports the cause.
///
/// **Widened in fix round 1 (review F-4).** The row as landed inspected
/// `pub fn run_with`'s body ALONE, and the reviewer's mutant C put
/// `RdKafkaReader::connect(` inside `execute_with` — the function `run_with`
/// delegates to — and survived both source-shape guards. The claim is now
/// stated as it was always meant: **exactly one function in this directory
/// constructs a client, and it is `pub fn run`.** Every OTHER function's body
/// in `mod.rs` is checked, not an enumerated three, so a new seam added
/// tomorrow is covered without editing this list.
#[test]
fn only_the_wrapper_constructs_a_client() {
    // THREE since Task 5b: the receipt needs a WRITABLE store, and
    // `Store::from_url(` is the constructor for one. It is on this list for
    // the same reason the other two are — it is a dial token in STANDING RULE
    // 18's own audit — and adding it is what keeps the claim "only the wrapper
    // constructs a client" true of the evidence handle as well as the reader
    // and the archive.
    const CONSTRUCTORS: [&str; 3] = [
        "RdKafkaReader::connect(",
        "Store::read_only_from_url(",
        "Store::from_url(",
    ];
    let mod_rs = backup_src("mod.rs");
    for c in CONSTRUCTORS {
        assert!(
            mod_rs.contains(c),
            "crates/logweir/src/backup/mod.rs is documented as the ONLY place that names \
             `{c}`, and it does not name it at all"
        );
    }
    for name in ["phase_minus1_admit.rs", "phase_run.rs"] {
        let src = backup_src(name);
        for c in CONSTRUCTORS {
            assert!(
                !src.contains(c),
                "crates/logweir/src/backup/{name} names `{c}`; the phases take \
                 `&dyn ClusterReader` / `&Store` and must construct nothing"
            );
        }
    }

    // The wrapper DOES construct both — a guard that only said "not here"
    // would pass on a directory that constructs nothing at all.
    let wrapper = fn_body(&mod_rs, "pub fn run(");
    for c in CONSTRUCTORS {
        assert!(
            wrapper.contains(c),
            "`pub fn run` is the wrapper and must be the function that names `{c}`"
        );
    }

    // Every named entry point below it names neither. `run_with` is the seam
    // every named test calls; `execute_with` is the seam's outcome-returning
    // half and is what mutant C reached through.
    for sig in ["pub fn run_with(", "pub fn execute_with("] {
        let body = fn_body(&mod_rs, sig);
        for c in CONSTRUCTORS {
            assert!(
                !body.contains(c),
                "`{sig}` names `{c}` — the seam must construct nothing, or every test that \
                 calls it inherits a 20 s rdkafka metadata timeout (rdkafka_reader.rs:16):\n\
                 {body}"
            );
        }
    }

    // EXACTLY ONE FUNCTION. Every occurrence of either constructor in this
    // file's CODE lies inside `pub fn run`'s body — so a third entry point
    // that constructed a client would fail here even if it were named neither
    // `run_with` nor `execute_with`. Line comments are stripped first because
    // this module's prose names both constructors on purpose (the module doc
    // and `run`'s own doc comment say `run` is the only place); a doc comment
    // sits OUTSIDE the body `fn_body` returns, so counting raw text would
    // compare a file total against a body total and never agree.
    let code = strip_line_comments(&mod_rs);
    let wrapper_code = strip_line_comments(&wrapper);
    for c in CONSTRUCTORS {
        let in_file = code.matches(c).count();
        let in_wrapper = wrapper_code.matches(c).count();
        assert!(in_wrapper > 0, "`pub fn run` does not name `{c}` in code");
        assert_eq!(
            in_file, in_wrapper,
            "`{c}` is constructed in more than one function: {in_file} occurrence(s) in \
             crates/logweir/src/backup/mod.rs's code, but only {in_wrapper} inside \
             `pub fn run`. GC18(c) rail 2 and Global Constraint 22 both rest on `run` being \
             the ONLY constructor (review F-4)."
        );
    }
}

/// Source text with every `//`-prefixed line removed. Crude for the same
/// reason `fn_body` is: the property being stated is "which function contains
/// this token", and a test that needed a lexer to say so would be a test with
/// a lexer bug in its future. `mod.rs` carries no block comments — asserted
/// here, so this helper cannot start lying if one is added.
fn strip_line_comments(src: &str) -> String {
    assert!(
        !src.contains("/*"),
        "strip_line_comments only handles line comments, and a block comment has appeared"
    );
    src.lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The text of one `fn`, from its signature to its closing brace, by brace
/// counting over the source. Crude on purpose: a test that needed a Rust
/// parser to state a one-line property would be a test with a parser bug in
/// its future.
fn fn_body(src: &str, signature: &str) -> String {
    let start = src
        .find(signature)
        .unwrap_or_else(|| panic!("`{signature}` not found"));
    let open = src[start..]
        .find('{')
        .unwrap_or_else(|| panic!("`{signature}` has no body"))
        + start;
    let mut depth = 0usize;
    for (i, ch) in src[open..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return src[start..open + i + 1].to_string();
                }
            }
            _ => {}
        }
    }
    panic!("`{signature}`'s body is unbalanced");
}

/// **Critique A F23.** `logweir` and the engine run on the HOST in the drill
/// path, so `http://minio:9000` — the compose SERVICE name — does not resolve
/// there. `examples/drill.yaml:17` and `:69-71` already use
/// `http://localhost:9000` for both of its storage blocks, and
/// `e2e/compose/config/backup-drill.yaml:6-13` writes the container-side
/// lesson out in full.
#[test]
fn the_shipped_backup_example_is_host_side() {
    let p = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/backup.yaml");
    let src = std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("{}: {e}", p.display()));
    assert!(
        src.contains("http://localhost:9000"),
        "examples/backup.yaml must name the HOST-side endpoint"
    );
    assert!(
        !src.contains("http://minio:9000"),
        "examples/backup.yaml names the compose service endpoint, which does not resolve on \
         the host the engine actually runs on"
    );
    // And it parses as the type it claims to be, with the brief's values.
    let spec: logweir_core::spec::BackupSpec = serde_yaml::from_str(&src).unwrap();
    assert_eq!(spec.backup_id, "mvp-demo");
    assert_eq!(spec.source.topics, vec!["orders", "payments"]);
    assert_eq!(spec.backup.compression, "zstd");
    assert_eq!(spec.backup.segment_max_records, 1000);
    assert_eq!(spec.backup.segment_max_bytes, 10_485_760);
    assert_eq!(spec.backup.max_concurrent_partitions, 3);
    // GC14's footer sentence.
    assert!(
        src.contains("Logweir is not affiliated with or endorsed by the ASF."),
        "examples/backup.yaml is missing the ASF footer sentence"
    );
}
