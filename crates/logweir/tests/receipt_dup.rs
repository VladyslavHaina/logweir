//! Tracker defect **RECEIPT-DUP**: a second run of ONE execution must never
//! invalidate the first run's signed receipt.
//!
//! # The defect, as it was
//!
//! A Backup Job re-created from its frozen inputs (PLAT-06.1 case e — the Job
//! deleted or lost, and re-created by the controller under the SAME execution
//! id) ran the engine a second time over the same backup set. The engine
//! writes `<prefix>/<backup_id>/manifest.json` unconditionally, so the second
//! run REPLACED the manifest the first run's receipt attests, and — because
//! the topic had advanced between the two runs — the replacement hashes to a
//! different digest. Every verifier that reads the archive back (the drill's
//! point binding, `crates/logweir/src/drill/binding.rs`, and the `catalogSync`
//! deep check, `crates/logweir/src/check/kinds/catalog_sync.rs`) then reports
//! the FIRST receipt as not describing what the archive holds. Run identity was
//! idempotent; signed evidence was not.
//!
//! # The fix these rows pin
//!
//! Every run claims its execution with a create-only, execution-scoped object —
//! `logweir/backups/<backup_id>/execution.claim.json` — immediately BEFORE the
//! engine starts, and a run that cannot claim never starts the engine and never
//! signs anything. So at most one engine run writes a given backup set, and the
//! manifest a receipt attests is never rewritten by a later run of the same
//! execution.
//!
//! # Why these stores are filesystem stores
//!
//! The defect is an OVERWRITE by an external process, and `Store::in_memory`
//! offers only create-only puts: it cannot model the engine replacing its own
//! manifest. The engine double below therefore writes the manifest with
//! `std::fs::write` into a tempdir, exactly as the real engine writes it with
//! its own unconditional store client, and both of Logweir's handles are built
//! over that tempdir. No endpoint, no network (see this file's entry in
//! `no_network_in_unit_tests.rs`).
use logweir::backup::{execute_with, run_with, BackupError, BackupOutcome, BackupRunArgs};
use logweir::exit::ExitCode;
use logweir_core::backup_receipt::BackupReceipt;
use logweir_core::engine::*;
use logweir_engine_oso::storage::Store;
use logweir_evidence::keys::{SigningKey, VerifyingKey};
use logweir_kafka::reader::{ClusterReader, ConsumedRecord, KafkaError, TopicMeta};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// The execution id both runs share — what the controller passes as
/// `--backup-id-override` on the original Job and on its re-creation.
const EXECUTION_ID: &str = "3f1c9d2e-8a7b-4c6d-9e0f-1a2b3c4d5e6f-20260923-030000";

/// Where the fixture's first record sits, epoch ms.
const T0: i64 = 1_790_000_000_000;

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

struct Fixture {
    dir: tempfile::TempDir,
    args: BackupRunArgs,
    key: SigningKey,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let archive_root = dir.path().join("bucket");
        std::fs::create_dir_all(&archive_root).unwrap();
        let spec = dir.path().join("backup.yaml");
        std::fs::write(
            &spec,
            format!(
                "backup_id: spec-default-id\n\
                 source:\n\
                \x20 bootstrap_servers: [kafka-source:9092]\n\
                \x20 topics: [orders]\n\
                 storage:\n\
                \x20 backend: filesystem\n\
                \x20 path: {}\n\
                 backup:\n\
                \x20 compression: zstd\n\
                \x20 segment_max_records: 1000\n\
                \x20 segment_max_bytes: 10485760\n\
                \x20 max_concurrent_partitions: 3\n",
                archive_root.display()
            ),
        )
        .unwrap();
        let allowed = dir.path().join("allowed-clusters.json");
        std::fs::write(
            &allowed,
            "{\"allowed_cluster_ids\": [\"SCRATCH-CLUSTER-01\"]}",
        )
        .unwrap();
        let key = SigningKey::generate_p256();
        let key_path = dir.path().join("signer.pem");
        std::fs::write(&key_path, key.to_pkcs8_pem().unwrap()).unwrap();
        Self {
            args: BackupRunArgs {
                store_contract_version: None,
                spec,
                allowed_clusters: allowed,
                signing_key: key_path,
                triggered_by: Some("schedule".into()),
                out: None,
                receipt_out: None,
                backup_id_override: Some(EXECUTION_ID.into()),
            },
            key,
            dir,
        }
    }

    fn root(&self) -> PathBuf {
        self.dir.path().join("bucket")
    }

    fn url(&self) -> StorageUrl {
        StorageUrl::Filesystem { path: self.root() }
    }

    /// The archive handle: read-only, as production builds it (GC6).
    fn archive(&self) -> Store {
        Store::read_only_from_url(&self.url()).unwrap()
    }

    /// The evidence handle over the same root: the only writable one.
    fn evidence(&self) -> Store {
        Store::from_url(&self.url()).unwrap()
    }

    fn public_key(&self) -> VerifyingKey {
        self.key.verifying_key()
    }

    /// One `backup run` of THIS execution, as run `run_id`.
    fn run(&self, run_id: &str, engine: &AdvancingEngine) -> Result<BackupOutcome, BackupError> {
        execute_with(
            &self.args,
            run_id,
            &StubReader,
            engine,
            &self.archive(),
            &self.evidence(),
        )
    }

    /// Every receipt the evidence root holds for this execution.
    fn receipts(&self) -> Vec<String> {
        self.evidence()
            .list_keys(&format!("logweir/backups/{EXECUTION_ID}/"))
            .unwrap()
            .into_iter()
            .filter(|k| k.ends_with(".receipt.json"))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// The verdict: what an archive-reading verifier concludes about one receipt
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    Valid,
    Invalid(String),
}

/// The two facts every archive-reading verifier in the product checks, in the
/// order they check them: the receipt's DSSE signature under the trusted key,
/// then the manifest the receipt names, read back and hashed, against the
/// digest the receipt attests (`drill/binding.rs`'s final comparison and
/// `catalog_sync.rs`'s deep check are both this comparison).
fn verdict(evidence: &Store, archive: &Store, receipt_key: &str, key: &VerifyingKey) -> Verdict {
    let (bytes, _) = evidence.get(receipt_key).expect("the receipt is readable");
    let sidecar_key = receipt_key.replace(".receipt.json", ".receipt.sig");
    let (sidecar, _) = evidence.get(&sidecar_key).expect("the sidecar is readable");
    let sidecar: logweir_evidence::Sidecar = serde_json::from_slice(&sidecar).unwrap();
    if let Err(e) = logweir_evidence::verify::verify_detached(
        key,
        logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT,
        &bytes,
        &sidecar,
    ) {
        return Verdict::Invalid(format!("signature: {e}"));
    }
    let receipt: BackupReceipt = serde_json::from_slice(&bytes).unwrap();
    let manifest = match archive.get(&receipt.archive.manifest_key) {
        Ok((m, _)) => m,
        Err(e) => return Verdict::Invalid(format!("manifest unreadable: {e}")),
    };
    let actual = logweir_core::ids::sha256_prefixed(&manifest);
    if actual != receipt.archive.manifest_sha256 {
        return Verdict::Invalid(format!(
            "manifest {} hashes to {actual}, the receipt attests {}",
            receipt.archive.manifest_key, receipt.archive.manifest_sha256
        ));
    }
    Verdict::Valid
}

// ---------------------------------------------------------------------------
// Doubles
// ---------------------------------------------------------------------------

struct StubReader;

impl ClusterReader for StubReader {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        Ok("SOURCE-CLUSTER-01".into())
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

/// An engine over a topic that ADVANCES between runs: run `n` captures
/// `100 * n` records, the newest one minute later each time, and — as the real
/// engine does — writes `<backup_id>/manifest.json` with an unconditional
/// write, replacing whatever was there.
struct AdvancingEngine {
    root: PathBuf,
    runs: Cell<u32>,
    /// Whether the execution claim was ALREADY in the evidence root when the
    /// engine started — the ordering the fix depends on.
    claimed_before_start: Cell<Option<bool>>,
}

impl AdvancingEngine {
    fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
            runs: Cell::new(0),
            claimed_before_start: Cell::new(None),
        }
    }

    /// `(records, newest record's timestamp)` for the run the engine last made.
    fn generation_facts(&self) -> (i64, i64) {
        let n = i64::from(self.runs.get());
        (100 * n, T0 + 60_000 * n)
    }
}

fn ts(s: &str) -> chrono::DateTime<chrono::Utc> {
    s.parse().unwrap()
}

impl DataEngine for AdvancingEngine {
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
        let (records, newest) = self.generation_facts();
        Ok(BackupSetFacts {
            backup_id: set.backup_id.clone(),
            created_at: ts("2026-09-23T03:04:00Z"),
            source_cluster_id: None,
            manifest_sha256: "sha256:from-the-engines-own-handle".into(),
            manifest_version_id: None,
            consumer_group_snapshot_sha256: None,
            topics: vec![TopicFacts {
                name: "orders".into(),
                original_partition_count: Some(1),
                source_replication_factor: Some(1),
                configurations: BTreeMap::new(),
                partitions: vec![PartitionFacts {
                    partition_id: 0,
                    segments: vec![SegmentFacts {
                        key: "seg".into(),
                        start_offset: 0,
                        end_offset: records - 1,
                        start_timestamp: T0,
                        end_timestamp: newest,
                        record_count: records,
                        sha256: String::new(),
                        uploaded_at: 0,
                    }],
                    gaps: vec![],
                    pruned: vec![],
                }],
            }],
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
        self.runs.set(self.runs.get() + 1);
        self.claimed_before_start.set(Some(
            self.root
                .join("logweir/backups")
                .join(&plan.backup_id)
                .join("execution.claim.json")
                .is_file(),
        ));
        let (records, newest) = self.generation_facts();
        let set = self.root.join(&plan.backup_id);
        std::fs::create_dir_all(&set).unwrap();
        let manifest = serde_json::json!({
            "backup_id": plan.backup_id,
            "created_at": newest,
            "topics": [{
                "name": "orders",
                "partitions": [{
                    "partition_id": 0,
                    "segments": [{
                        "key": format!(
                            "{}/topics/orders/partition=0/segment-00000000000000000000.bin",
                            plan.backup_id
                        ),
                        "start_offset": 0,
                        "end_offset": records - 1,
                        "start_timestamp": T0,
                        "end_timestamp": newest,
                        "record_count": records,
                    }],
                }],
            }],
        });
        // THE ENGINE'S OWN, UNCONDITIONAL WRITE — the overwrite RECEIPT-DUP is
        // about. Logweir cannot make this write conditional: it is the
        // engine's, through the engine's own store client.
        std::fs::write(
            set.join("manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        Ok(BackupFacts {
            started_at: ts("2026-09-23T03:00:00Z"),
            finished_at: ts("2026-09-23T03:04:00Z"),
            exit_code: 0,
            unknown_key_warnings: vec![],
        })
    }
}

// ---------------------------------------------------------------------------
// The reproduction
// ---------------------------------------------------------------------------

/// **The RECEIPT-DUP reproduction.** Two runs of ONE execution over a topic
/// that advanced between them. Before the fix the second run exited 0, wrote a
/// second receipt, and left the first one reporting `Invalid` (the manifest it
/// attests had been replaced). The fix: the second run is refused before its
/// engine starts, writes no receipt, and the first receipt still verifies.
#[test]
fn a_second_run_of_one_execution_never_invalidates_the_first_receipt() {
    let f = Fixture::new();
    let engine = AdvancingEngine::new(&f.root());

    let first = f
        .run("01K5RUN0000000000000000001", &engine)
        .expect("the first run succeeds");
    assert_eq!(
        verdict(
            &f.evidence(),
            &f.archive(),
            &first.receipt_key,
            &f.public_key()
        ),
        Verdict::Valid,
        "the first receipt verifies when it is written"
    );

    // The Job is lost and re-created from its frozen inputs; the topic has
    // advanced meanwhile.
    let second = f.run("01K5RUN0000000000000000002", &engine);

    assert_eq!(
        verdict(
            &f.evidence(),
            &f.archive(),
            &first.receipt_key,
            &f.public_key()
        ),
        Verdict::Valid,
        "RECEIPT-DUP: a second run of the same execution invalidated the first signed receipt"
    );
    match second {
        Err(BackupError::Operational(message)) => {
            assert!(
                message.contains(logweir::backup::phase_run::EXECUTION_ALREADY_CLAIMED),
                "the refusal names itself: {message}"
            );
            assert!(
                message.contains(EXECUTION_ID),
                "and the execution: {message}"
            );
        }
        other => panic!(
            "the second run of an execution whose first run reached the engine must be \
             refused with exit 1 (a retry under a NEW execution id is the remedy), got {other:?}"
        ),
    }
    assert_eq!(
        engine.runs.get(),
        1,
        "the refused run never started the engine, so the manifest was never rewritten"
    );
    assert_eq!(
        f.receipts(),
        vec![first.receipt_key.clone()],
        "the refused run signed nothing: exactly one receipt for the execution"
    );
}

/// The claim is taken BEFORE the engine starts, names the run that holds it,
/// and lives beside the receipts as the one execution-scoped object there.
#[test]
fn the_first_run_claims_its_execution_before_the_engine_starts() {
    let f = Fixture::new();
    let engine = AdvancingEngine::new(&f.root());
    let run_id = "01K5RUN0000000000000000001";
    let outcome = f.run(run_id, &engine).expect("the first run succeeds");
    assert_eq!(
        engine.claimed_before_start.get(),
        Some(true),
        "the claim must exist when the engine starts: a claim taken after the engine \
         protects nothing"
    );
    let key = logweir::backup::phase_run::claim_key(EXECUTION_ID);
    assert_eq!(
        key,
        format!("logweir/backups/{EXECUTION_ID}/execution.claim.json")
    );
    let (bytes, _) = f
        .evidence()
        .get(&key)
        .expect("the claim is in the evidence root");
    let claim: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(claim["backup_id"], EXECUTION_ID);
    assert_eq!(
        claim["run_id"], run_id,
        "the claim names the run that holds it"
    );
    assert_eq!(claim["format_version"], "1.0.0");
    // The receipt and its sidecar are unchanged in key and shape.
    assert_eq!(
        outcome.receipt_key,
        format!("logweir/backups/{EXECUTION_ID}/{run_id}.receipt.json")
    );
}

/// A claim that already exists is an earlier run of this execution: exit 1 —
/// retryable, because a retry is a NEW execution — and the engine never runs.
#[test]
fn a_claimed_execution_is_exit_1_and_starts_no_engine() {
    let f = Fixture::new();
    let engine = AdvancingEngine::new(&f.root());
    let store = Store::in_memory("logweir/");
    store
        .put_create_only(&logweir::backup::phase_run::claim_key(EXECUTION_ID), b"{}")
        .unwrap();
    assert_eq!(
        run_with(&f.args, &StubReader, &engine, &store),
        ExitCode::Operational
    );
    assert_eq!(
        engine.runs.get(),
        0,
        "no engine run over a claimed execution"
    );
    assert!(
        store
            .list_keys(&format!("logweir/backups/{EXECUTION_ID}/"))
            .unwrap()
            .iter()
            .all(|k| !k.ends_with(".receipt.json")),
        "and nothing was signed"
    );
}

/// A backend that answers conditional put `NotImplemented` makes
/// `put_create_only` fall back to HEAD-then-PUT and say so. That claim is not
/// a lock: exit 4, no engine run, nothing signed.
#[test]
fn a_claim_the_store_cannot_enforce_fails_closed() {
    let f = Fixture::new();
    let engine = AdvancingEngine::new(&f.root());
    let store = Store::in_memory_without_conditional_put("logweir/");
    let err = execute_with(
        &f.args,
        "01K5RUN0000000000000000001",
        &StubReader,
        &engine,
        &store,
        &store,
    )
    .expect_err("an unenforceable claim must refuse the run");
    assert_eq!(err.exit_code(), ExitCode::SigningOrLock, "{err}");
    assert!(
        err.to_string()
            .contains(logweir::backup::phase_run::EXECUTION_CLAIM_UNPROVEN),
        "{err}"
    );
    assert!(err.to_string().contains("HEAD-then-PUT"), "{err}");
    assert_eq!(engine.runs.get(), 0, "no engine run on an unproven claim");
}

/// An S3-compatible store that ACCEPTS `If-None-Match: *` and overwrites anyway
/// looks, to a single put, exactly like one that honours it. The second
/// create of the same key is what tells them apart: exit 4, no engine run.
#[test]
fn a_store_that_ignores_if_none_match_fails_closed() {
    let f = Fixture::new();
    let engine = AdvancingEngine::new(&f.root());
    let store = Store::in_memory_ignoring_conditional_put("logweir/");
    let err = execute_with(
        &f.args,
        "01K5RUN0000000000000000001",
        &StubReader,
        &engine,
        &store,
        &store,
    )
    .expect_err("a store that ignores conditional create must refuse the run");
    assert_eq!(err.exit_code(), ExitCode::SigningOrLock, "{err}");
    let message = err.to_string();
    assert!(
        message.contains(logweir::backup::phase_run::EXECUTION_CLAIM_UNPROVEN),
        "{message}"
    );
    assert!(message.contains("ignores `If-None-Match: *`"), "{message}");
    assert_eq!(engine.runs.get(), 0, "no engine run on an unproven claim");
}

/// **Catalog row.** The claim is a new object under `logweir/backups/`, which
/// `logweir catalog sync` pages: it must be neither scanned as a receipt nor
/// counted as unreadable, and the one real point is still found.
#[test]
fn catalog_sync_sees_one_point_and_ignores_the_claim() {
    use logweir::catalog::cli::sync_with;
    let f = Fixture::new();
    let engine = AdvancingEngine::new(&f.root());
    let first = f.run("01K5RUN0000000000000000001", &engine).unwrap();
    let _refused = f.run("01K5RUN0000000000000000002", &engine).unwrap_err();

    let signer = logweir::backup::phase_run::load_signer(&f.args.signing_key).unwrap();
    let report = sync_with(
        &sync_args(),
        &f.evidence(),
        &signer,
        &[f.public_key()],
        ts("2026-09-23T04:00:00Z"),
        "s3://kafka-backups",
    )
    .unwrap();
    assert_eq!(
        report.scanned, 1,
        "one receipt, and the claim is not one: {report:?}"
    );
    assert_eq!(report.unreadable, 0, "{report:?}");
    assert_eq!(
        report.already_present, 1,
        "the run wrote its own point: {report:?}"
    );
    assert_eq!(report.points.len(), 1);
    assert_eq!(report.points[0].0, first.receipt_key);
}

/// **Old archives.** An execution directory written before the claim existed —
/// the committed, independently signed fixture receipt with its sidecar and NO
/// claim — still verifies, and `catalog sync` backfills it beside a new
/// execution that did take a claim: the layout change is additive, and nothing
/// reads a missing claim as a fault.
#[test]
fn an_old_archive_with_no_claim_still_verifies_and_syncs_beside_a_new_run() {
    use logweir::catalog::cli::sync_with;
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let fixtures = workspace.join("e2e/fixtures/signed");
    let old_bytes = std::fs::read(fixtures.join("backup-receipt.json")).unwrap();
    let old_sig = std::fs::read(fixtures.join("backup-receipt.sig")).unwrap();
    let old_key = VerifyingKey::from_pem_file(&fixtures.join("public.pem")).unwrap();
    let old: BackupReceipt = serde_json::from_slice(&old_bytes).unwrap();
    let old_receipt_key = format!(
        "logweir/backups/{}/{}.receipt.json",
        old.backup_id, old.run_id
    );

    let f = Fixture::new();
    let evidence = f.evidence();
    evidence
        .put_create_only(&old_receipt_key, &old_bytes)
        .unwrap();
    evidence
        .put_create_only(
            &old_receipt_key.replace(".receipt.json", ".receipt.sig"),
            &old_sig,
        )
        .unwrap();

    // A new execution beside it claims, runs and signs as usual.
    let engine = AdvancingEngine::new(&f.root());
    let new = f.run("01K5RUN0000000000000000001", &engine).unwrap();

    // The old receipt's signature still verifies under its own key, and its
    // execution directory carries no claim — which nothing treats as a fault.
    let sidecar: logweir_evidence::Sidecar = serde_json::from_slice(&old_sig).unwrap();
    logweir_evidence::verify::verify_detached(
        &old_key,
        logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT,
        &old_bytes,
        &sidecar,
    )
    .expect("the old receipt still verifies");
    assert!(evidence
        .get(&logweir::backup::phase_run::claim_key(&old.backup_id))
        .is_err());

    let signer = logweir::backup::phase_run::load_signer(&f.args.signing_key).unwrap();
    let report = sync_with(
        &sync_args(),
        &evidence,
        &signer,
        &[old_key, f.public_key()],
        ts("2026-09-23T04:00:00Z"),
        "s3://kafka-backups",
    )
    .unwrap();
    assert_eq!(
        report.scanned, 2,
        "two receipts, no claim scanned: {report:?}"
    );
    assert_eq!(
        report.written, 1,
        "the old receipt is backfilled: {report:?}"
    );
    assert_eq!(
        report.already_present, 1,
        "the new run wrote its own: {report:?}"
    );
    assert_eq!(report.unreadable, 0, "{report:?}");
    assert!(report.points.iter().any(|(k, _, _)| *k == old_receipt_key));
    assert!(report.points.iter().any(|(k, _, _)| *k == new.receipt_key));
}

/// `catalog sync`'s arguments for the in-process seam: the location string is
/// informational there, and the keys are passed to `sync_with` directly.
fn sync_args() -> logweir::catalog::cli::SyncArgs {
    logweir::catalog::cli::SyncArgs {
        location: logweir::catalog::cli::Location {
            url: "s3://kafka-backups".into(),
            region: None,
            endpoint: None,
            path_style: false,
            allow_http: false,
        },
        signing_key: PathBuf::from("unused-by-the-seam"),
        public_keys: Vec::new(),
        since: None,
        max: 100,
    }
}

/// **Python verifier agreement.** After the refused second run, the auditor's
/// independent reader (`docs/verify_scorecard.py`) still says VALID for the
/// first receipt, and the auditor's own `sha256` of the manifest bytes in the
/// archive equals the digest that receipt attests — the two facts an auditor
/// checks without a Rust toolchain.
#[test]
fn the_python_verifier_agrees_the_first_receipt_still_verifies() {
    let py = python();
    let f = Fixture::new();
    let engine = AdvancingEngine::new(&f.root());
    let first = f.run("01K5RUN0000000000000000001", &engine).unwrap();
    let _refused = f.run("01K5RUN0000000000000000002", &engine).unwrap_err();

    let receipt = f.root().join(&first.receipt_key);
    let sidecar = f.root().join(&first.sidecar_key);
    let public = f.dir.path().join("public.pem");
    std::fs::write(&public, f.public_key().to_public_key_pem().unwrap()).unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");

    let verify = run_bounded(
        std::process::Command::new(&py)
            .current_dir(&root)
            .arg("docs/verify_scorecard.py")
            .args(["--payload-type", "backup-receipt"])
            .arg(&receipt)
            .arg(&sidecar)
            .arg(&public),
    );
    assert!(
        verify.status.success() && String::from_utf8_lossy(&verify.stdout).starts_with("VALID"),
        "the Python reader must verify the first receipt\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&verify.stdout),
        String::from_utf8_lossy(&verify.stderr)
    );

    let parsed: BackupReceipt = serde_json::from_slice(&std::fs::read(&receipt).unwrap()).unwrap();
    let hashed = run_bounded(
        std::process::Command::new(&py)
            .arg("-c")
            .arg(
                "import hashlib,sys; \
                 print('sha256:' + hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())",
            )
            .arg(f.root().join(&parsed.archive.manifest_key)),
    );
    assert!(hashed.status.success(), "{hashed:?}");
    assert_eq!(
        String::from_utf8_lossy(&hashed.stdout).trim(),
        parsed.archive.manifest_sha256,
        "the auditor's own digest of the archived manifest is the one the receipt attests"
    );
}

/// The interpreter that can run the auditor's verifier — the same resolution
/// order every parity gate uses (`two_reader_parity.rs::python`).
fn python() -> PathBuf {
    for var in ["LOGWEIR_PYTHON", "LOGWEIR_E2E_PYTHON"] {
        if let Ok(p) = std::env::var(var) {
            if !p.is_empty() {
                return PathBuf::from(p);
            }
        }
    }
    let venv = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.e2e/venv/bin/python3");
    if venv.exists() {
        return venv;
    }
    PathBuf::from("python3")
}

/// A child process with a 30 s bound: a hung interpreter must fail this test,
/// never hang the suite.
fn run_bounded(cmd: &mut std::process::Command) -> std::process::Output {
    use std::io::Read as _;
    let mut child = cmd
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("the interpreter starts (set $LOGWEIR_PYTHON to one with `cryptography`)");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            panic!("the child process exceeded its 30 s bound");
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    };
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_end(&mut stdout)
        .unwrap();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_end(&mut stderr)
        .unwrap();
    std::process::Output {
        status,
        stdout,
        stderr,
    }
}
