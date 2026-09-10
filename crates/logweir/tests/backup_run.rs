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
use logweir_core::engine::*;
use logweir_engine_oso::storage::Store;
use logweir_kafka::reader::{ClusterReader, ConsumedRecord, KafkaError, TopicMeta};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
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
    Fixture {
        _dir: dir,
        args: BackupRunArgs {
            spec: spec_path,
            allowed_clusters: allowed_path,
            // NEVER opened by this build (see `BackupRunArgs::signing_key`);
            // the path is threaded for Task 5b, so a fixture needs no key
            // material and this suite generates none.
            signing_key: PathBuf::from("signer.pem"),
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
/// subprocess-free — and it is also why a SCRAM spec reaches
/// `BackupOutcome.source_auth` here: rendering (and therefore Task 3's typed
/// `RenderError::UnsupportedAuthMode`) happens inside `OsoCliEngine::backup`,
/// not in this seam.
struct RecordingEngine {
    plans: Mutex<Vec<BackupPlan>>,
    /// `None` => the engine fails, as a real one exiting non-zero does.
    facts: Option<BackupFacts>,
    topics: Vec<TopicFacts>,
}

impl RecordingEngine {
    fn ok(topics: Vec<TopicFacts>) -> Self {
        Self {
            plans: Mutex::new(Vec::new()),
            facts: Some(BackupFacts {
                started_at: ts("2026-09-09T00:00:00Z"),
                finished_at: ts("2026-09-09T00:01:00Z"),
                exit_code: 0,
                unknown_key_warnings: vec![],
            }),
            topics,
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
            facts: None,
            topics: vec![],
        }
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
        EngineId {
            id: "double".into(),
            version: "v0.21.0".into(),
            digest: "sha256:0".into(),
        }
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
    let msg = guard_message(execute_with(&f.args, "run-1", &reader, &engine, &store).unwrap_err());
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
    let msg = guard_message(execute_with(&f.args, "run-1", &reader, &engine, &store).unwrap_err());
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
    let msg = guard_message(execute_with(&f.args, "run-1", &reader, &engine, &store).unwrap_err());
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
    let msg = guard_message(execute_with(&f.args, "run-1", &reader, &engine, &store).unwrap_err());
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

    let outcome = execute_with(&f.args, "run-1", &reader, &engine, &store).unwrap();
    assert_eq!(outcome.source_cluster_id, "CID-FROM-BROKER");
    assert_eq!(run_with(&f.args, &reader, &engine, &store), ExitCode::Ok);
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
    match execute_with(&f.args, "run-1", &reader, &engine, &store).unwrap_err() {
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
    let msg = guard_message(execute_with(&f.args, "run-1", &reader, &engine, &store).unwrap_err());
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

    let outcome = execute_with(&f.args, "run-1", &reader, &engine, &store).unwrap();
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
    let outcome2 = execute_with(&f2.args, "run-2", &reader, &engine2, &store2).unwrap();
    assert_eq!(outcome2.backup_id, "mvp-demo");
    let doc2 = logweir_engine_oso::render_backup::render(&engine2.recorded_plan()).unwrap();
    assert!(
        !doc2.contains("sched-7"),
        "the derived run must not carry the override's id:\n{doc2}"
    );
    assert!(doc2.contains("backup_id: \"mvp-demo\""), "{doc2}");
}

/// `BackupOutcome.source_auth` is filled by the explicit `match` over
/// `spec.source.auth` (Task 6 replaces it with `AuthSpec::to_render()` and
/// changes nothing else), so Task 5b has a populated field without depending
/// on Task 6.
///
/// A SCRAM spec is RECORDED faithfully here and never downgraded. It is
/// REFUSED at render time by Task 3's typed
/// `RenderError::UnsupportedAuthMode` — which lives inside
/// `OsoCliEngine::backup`, so it is reached with the real engine and not with
/// this seam's double. `e2e/tests/backup_argv.rs` proves that refusal at
/// process level.
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

    let outcome = execute_with(&f.args, "run-1", &reader, &engine, &store).unwrap();
    assert_eq!(
        outcome.source_auth,
        AuthRender::ScramSha512 {
            username: "logweir".into(),
            tls: false,
        }
    );
    // And the plan the engine was handed carries the same value — the field is
    // not a decoration on the outcome.
    assert_eq!(
        engine.recorded_plan().source_auth,
        AuthRender::ScramSha512 {
            username: "logweir".into(),
            tls: false,
        }
    );

    // The default, for contrast: no `auth` block at all is plaintext.
    let f2 = ok_fixture("mvp-demo");
    let engine2 = RecordingEngine::one_topic();
    let (store2, _k2, _b2) = archive_with_one_manifest("mvp-demo");
    let outcome2 = execute_with(&f2.args, "run-2", &reader, &engine2, &store2).unwrap();
    assert_eq!(outcome2.source_auth, AuthRender::Plaintext);
}

/// The read-back is what turns "the engine exited 0" into a measured result: a
/// backup of empty topics also exits 0. Every value here comes from the
/// ARCHIVE, and the manifest digest is over the exact bytes this run read —
/// not over the engine handle's own number, which a receipt quoting it would
/// be attesting without ever having seen the bytes.
#[test]
fn the_read_back_reports_the_archives_own_counts_window_and_manifest_digest() {
    let f = ok_fixture("mvp-demo");
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

    let outcome = execute_with(&f.args, "run-1", &reader, &engine, &store).unwrap();
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
    // The union of every segment's window, across every topic — not any one
    // topic's, and not the manifest's own declared range.
    assert_eq!(outcome.covered_from_ms, 1_755_999_000_000);
    assert_eq!(outcome.covered_to_ms, 1_756_000_060_000);
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

    assert_eq!(
        run_with(&f.args, &reader, &engine, &store),
        ExitCode::Operational
    );
    match execute_with(&f.args, "run-1", &reader, &engine, &store).unwrap_err() {
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

    assert_eq!(
        run_with(&f.args, &reader, &engine, &store),
        ExitCode::Operational
    );
    let err = execute_with(&f.args, "run-1", &reader, &engine, &store).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("holds no backup set `mvp-demo`"), "{msg}");
}

/// Interface **I6** is Task 5b's. An operator who asked for the receipt gets a
/// message naming the contract that owes it, exit 1 — never a silent exit 0
/// after a real backup with nothing to show for it, and never an archive
/// instead of the document they asked for.
#[test]
fn asking_for_a_receipt_exits_1_naming_task_5bs_contract() {
    for flag in ["receipt", "out"] {
        let mut f = ok_fixture("mvp-demo");
        let p = f._dir.path().join("receipt.json");
        if flag == "receipt" {
            f.args.receipt_out = Some(p);
        } else {
            f.args.out = Some(p);
        }
        let reader = StubReader::answering("SOURCE-CLUSTER-00000001");
        let engine = RecordingEngine::one_topic();
        let (store, _k, _b) = archive_with_one_manifest("mvp-demo");

        assert_eq!(
            run_with(&f.args, &reader, &engine, &store),
            ExitCode::Operational,
            "flag {flag}"
        );
        let msg = execute_with(&f.args, "run-1", &reader, &engine, &store)
            .unwrap_err()
            .to_string();
        assert!(msg.contains("interface I6"), "{msg}");
        assert!(msg.contains("Task 5b"), "{msg}");
        assert!(msg.contains("NO backup was taken"), "{msg}");
        assert!(
            engine.plans.lock().unwrap().is_empty(),
            "the engine must not run when the document cannot be written"
        );
    }
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
#[test]
fn only_the_wrapper_constructs_a_client() {
    const CONSTRUCTORS: [&str; 2] = ["RdKafkaReader::connect(", "Store::read_only_from_url("];
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
    // And `run_with` itself names neither — the seam every named test calls.
    let body = fn_body(&mod_rs, "pub fn run_with");
    for c in CONSTRUCTORS {
        assert!(!body.contains(c), "`pub fn run_with` names `{c}`:\n{body}");
    }
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
