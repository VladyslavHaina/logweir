mod fixtures; // crates/logweir/tests/fixtures/mod.rs — Task 14 step 5c
use logweir::drill::phase6_restore::assert_post_condition;
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Task 18 fix round 1 (2026-09-04), per task-18-review.md. Everything below
// this line is ADDITIVE: none of the four tests above are modified, per this
// round's explicit instruction. New imports needed only by the tests below.
use logweir::drill::DrillError;
use logweir_core::engine::{
    BackupSetFacts, BackupSetRef, DataEngine, EngineError, EngineId, PhaseObserver,
    PreflightReport, RecordFingerprint, RestoreFacts, RestorePlan, SampleSelection, StorageUrl,
};
use logweir_kafka::reader::{ClusterReader, ConsumedRecord, KafkaError, TopicMeta};
use std::sync::atomic::{AtomicBool, Ordering};

/// The backstop for a `dry_run` that arrives by any route the phase-0 guard did
/// not see. A no-op restore exits 0 and would otherwise score, producing a
/// signed scorecard whose RTO was measured around nothing.
#[test]
fn all_zero_end_offsets_fail_the_post_condition() {
    let mut m = BTreeMap::new();
    m.insert("drill-orders".to_string(), vec![(0, 0i64), (1, 0), (2, 0)]);
    let e = assert_post_condition(&m).unwrap_err();
    assert!(e.to_string().contains("no-op"), "{e}");
}

#[test]
fn one_non_zero_partition_satisfies_the_post_condition() {
    let mut m = BTreeMap::new();
    m.insert("drill-orders".to_string(), vec![(0, 0i64), (1, 41), (2, 0)]);
    assert!(assert_post_condition(&m).is_ok());
}

#[test]
fn an_empty_map_fails_rather_than_vacuously_passing() {
    assert!(assert_post_condition(&BTreeMap::new()).is_err());
}

#[test]
fn the_measured_window_brackets_the_subprocess_and_nothing_else() {
    let (r, engine) = fixtures::engine_that_sleeps_ms(300);
    let out = logweir::drill::phase6_restore::run(
        &engine,
        &fixtures::plan(),
        &r,
        &fixtures::mapping("orders", "drill-orders"),
        &mut fixtures::NullObserver,
    )
    .unwrap();
    let ms = (out.finished_at - out.started_at).num_milliseconds();
    assert!(
        (250..2_000).contains(&ms),
        "measured {ms}ms — it must bracket the subprocess only"
    );
}

// ---------------------------------------------------------------------------
// Fix round 1 additions. Doubles local to this file — no shared fixture is
// touched, no injection hook, no env var, no cfg flag; GC4/GR7 are unaffected.

/// Mutant M4 (review): `run()`'s call to `assert_post_condition` could be
/// deleted and every test above still passed, because `engine_that_sleeps_ms`'s
/// `FakeReader` target holds a non-zero end offset (500). This test uses a
/// target still at end offset 0 instead, so the backstop is load-bearing.
#[test]
fn run_refuses_a_restore_that_left_the_target_empty() {
    let engine = fixtures::SleepEngine { ms: 1 };
    let reader = fixtures::FakeReader {
        state: fixtures::target_with("MkU3OEVBNTcwNTJENDM2Qk", "drill-orders", 3, 0, &[]),
    };
    let r = logweir::drill::phase6_restore::run(
        &engine,
        &fixtures::plan(),
        &reader,
        &fixtures::mapping("orders", "drill-orders"),
        &mut fixtures::NullObserver,
    );
    let Err(e) = r else {
        panic!("run() returned Ok for a target still at end offset 0")
    };
    // The variant is what decides the exit code (Task 18 fix round 1, F1/F6):
    // it must be the not-pass finding, never Operational/Kafka/Engine.
    assert!(
        matches!(e, DrillError::RestoreNoOp(_)),
        "the no-op backstop must surface as RestoreNoOp (exit 2, a drill result), not {e:?}"
    );
    assert!(e.to_string().contains("no-op"), "{e}");
}

/// Review finding F5: an empty topic mapping and an all-zero restore are NOT
/// the same finding, and only one of them says anything about a restore that
/// ran. This pins the empty-map branch's classification and its wording.
#[test]
fn an_empty_topic_mapping_is_operational_not_a_restore_finding() {
    let e = assert_post_condition(&BTreeMap::new()).unwrap_err();
    assert!(
        matches!(e, DrillError::Operational(_)),
        "an empty selection says nothing about the archive; got {e:?}"
    );
    assert!(
        !e.to_string().contains("no-op"),
        "an empty selection must not claim a restore ran and did nothing: {e}"
    );
}

/// The other half of F5: an all-zero restore over a NON-EMPTY selection is a
/// positively established fact about the archive, so it must NOT share the
/// empty-map's `Operational` classification.
#[test]
fn all_zero_offsets_over_a_nonempty_selection_is_a_restore_finding_not_operational() {
    let mut m = BTreeMap::new();
    m.insert("drill-orders".to_string(), vec![(0, 0i64), (1, 0), (2, 0)]);
    let e = assert_post_condition(&m).unwrap_err();
    assert!(
        matches!(e, DrillError::RestoreNoOp(_)),
        "a non-empty selection that restored nothing is a drill result, not {e:?}"
    );
}

/// A minimal `DataEngine` double, local to this file: it never sleeps and
/// never reads a real clock — it returns FIXED sentinel timestamps and
/// records whether it was actually invoked. Review findings F3/M6/N6/N7/N18:
/// a duration-tolerance assertion cannot tell "measured the subprocess" apart
/// from "invented a plausible number" or "drifted by ~1s in either
/// direction". Comparing for exact identity against the engine's own
/// `RestoreFacts`, and asserting the engine was actually called, closes all
/// four at once with zero tolerance.
struct SentinelEngine {
    called: AtomicBool,
}
impl DataEngine for SentinelEngine {
    fn id(&self) -> EngineId {
        EngineId {
            id: "sentinel".into(),
            version: "v0.0.0".into(),
            digest: format!("sha256:{}", "0".repeat(64)),
        }
    }
    fn list_backup_sets(&self, _l: &StorageUrl) -> Result<Vec<BackupSetRef>, EngineError> {
        Ok(vec![])
    }
    fn describe(&self, _s: &BackupSetRef) -> Result<BackupSetFacts, EngineError> {
        Ok(fixtures::backup_facts_orders(3))
    }
    fn preflight(&self, _p: &RestorePlan) -> Result<PreflightReport, EngineError> {
        Err(EngineError::Operational("not used by this fixture".into()))
    }
    fn restore(
        &self,
        _p: &RestorePlan,
        _o: &mut dyn PhaseObserver,
    ) -> Result<RestoreFacts, EngineError> {
        self.called.store(true, Ordering::SeqCst);
        Ok(RestoreFacts {
            started_at: fixtures::ts("2026-01-01T00:00:00Z"),
            finished_at: fixtures::ts("2026-01-01T00:00:05Z"),
            exit_code: 0,
            unknown_key_warnings: vec!["restore.some_key".to_string()],
        })
    }
    fn fingerprints(&self, _s: &SampleSelection) -> Result<Vec<RecordFingerprint>, EngineError> {
        Ok(vec![])
    }
}

#[test]
fn the_measured_span_is_exactly_the_engines_own_facts_and_the_engine_is_actually_invoked() {
    let engine = SentinelEngine {
        called: AtomicBool::new(false),
    };
    let reader = fixtures::FakeReader {
        state: fixtures::target_with("MkU3OEVBNTcwNTJENDM2Qk", "drill-orders", 3, 500, &[]),
    };
    let out = logweir::drill::phase6_restore::run(
        &engine,
        &fixtures::plan(),
        &reader,
        &fixtures::mapping("orders", "drill-orders"),
        &mut fixtures::NullObserver,
    )
    .unwrap();
    assert!(
        engine.called.load(Ordering::SeqCst),
        "the engine must actually be invoked, not bypassed with a fabricated window"
    );
    assert_eq!(out.started_at, fixtures::ts("2026-01-01T00:00:00Z"));
    assert_eq!(out.finished_at, fixtures::ts("2026-01-01T00:00:05Z"));
    // F8: the engine's unknown-key warnings must survive phase 6, not be
    // silently dropped — nothing downstream can recover them otherwise.
    assert_eq!(
        out.unknown_key_warnings,
        vec!["restore.some_key".to_string()]
    );
}

/// A `ClusterReader` double whose `end_offsets` always fails, local to this
/// file. Review finding F7/N4: a broker read failure during the post-restore
/// check must surface as `DrillError::Kafka`, never be swallowed into a
/// false "no-op" finding about the archive.
struct FailingReader;
impl ClusterReader for FailingReader {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        Ok("MkU3OEVBNTcwNTJENDM2Qk".into())
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        Ok(vec![])
    }
    fn end_offsets(&self, _topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        Err(KafkaError::Client(
            "broker unreachable during post-condition read".into(),
        ))
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
        _t: &str,
        _p: i32,
        _from: i64,
        _max: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        Ok(vec![])
    }
}

#[test]
fn a_broker_failure_during_the_post_restore_read_surfaces_as_kafka_not_a_no_op() {
    let engine = fixtures::SleepEngine { ms: 1 };
    let reader = FailingReader;
    let r = logweir::drill::phase6_restore::run(
        &engine,
        &fixtures::plan(),
        &reader,
        &fixtures::mapping("orders", "drill-orders"),
        &mut fixtures::NullObserver,
    );
    let Err(e) = r else {
        panic!("run() returned Ok despite the reader failing on every call")
    };
    assert!(
        matches!(e, DrillError::Kafka(_)),
        "a broker failure reading the target is operational-by-way-of-Kafka, not {e:?}"
    );
}

// ---------------------------------------------------------------------------
// Task 6 (T0-14). ADDITIVE: nothing above this line is modified.

/// Ruling **R-E**. A phase-5 / phase-6 `restore.yaml` divergence is detected
/// inside `OsoCliEngine::restore`, i.e. AFTER phases 0-5 have run — the
/// admission guard passed, the plan was rendered, and the engine's own
/// `validate-restore` already executed. Global Constraint 11 reserves exit `3`
/// for "plan refused by a guard, **before anything runs**", so it does not
/// describe this at all. The refusal is `EngineError::Operational`, which the
/// existing route at `crates/logweir/src/drill/mod.rs:105` maps through
/// `DrillError::Engine` to exit `1` — operational error, no artifact. This
/// test is the machine check on that route, and it is the kill for a mutant
/// that re-points `DrillError::Engine(_)` at `ExitCode::GuardRefused`.
///
/// It lives here rather than beside the other three T0-14 tests in
/// `crates/logweir-engine-oso/tests/render_equality.rs` because
/// `logweir-engine-oso` does not depend on `logweir` and must not: adding the
/// dependency would invert the crate graph.
#[test]
fn render_mismatch_is_operational_not_guard() {
    let e = logweir::drill::DrillError::Engine(logweir_core::engine::EngineError::Operational(
        "rendered restore.yaml diverged between phase 5 and phase 6: phase 5 validated \
         sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, phase 6 \
         would restore sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
         — refusing"
            .into(),
    ));
    let code = logweir::exit::ExitCode::from(e);
    assert_eq!(code as u8, 1, "R-E: operational (1), no artifact");
    assert_ne!(
        code as u8, 3,
        "GC11's 3 is a guard refusal BEFORE anything runs; phases 0-5 have run"
    );
}

// ===========================================================================
// D3 §5.5 — the recovery point is re-verified BEFORE any restore phase runs
// ===========================================================================
//
// `crates/logweir/src/drill/binding.rs` owns the check's own rows (every
// refusal shape, over `Store::in_memory`). What belongs HERE is the ORDERING
// claim, because this file is the one about the restore phases: a plan whose
// bound point the archive does not hold must be refused before phase 0, with
// no broker contacted and no phase begun.
//
// The witness is D3 §2.4's own progress channel: `progress-phase=0:admit` is
// printed at the start of phase 0, so its ABSENCE on a refused run is direct
// evidence that no phase ran. The mutant it kills is moving the binding check
// after `context` — the run would then dial the bootstrap and announce phase 0
// before discovering that the point is not there.

mod binding_ordering {
    use chrono::Utc;
    use logweir_core::execution_contract as wire;
    use logweir_core::ids::sha256_prefixed;
    use logweir_core::spec::ApprovalDoc;
    use logweir_evidence::keys::SigningKey;
    use logweir_evidence::sign::sign_detached;
    use std::process::Command;

    /// A plan bound to one recovery point, over a filesystem archive.
    fn plan(archive: &std::path::Path, evidence: &std::path::Path, point: &str) -> String {
        format!(
            r#"
name: bound-restore
source:
  storage: {{backend: filesystem, path: {archive}}}
  backup: nightly-7
  topics: [orders]
  point:
{point}
target:
  bootstrap_servers: ["127.0.0.1:19098"]
  mode: scratch
  topic_mapping_prefix: "drill-"
  marker_topic: logweir.scratch
sample:
  window_start: 2026-01-01T00:00:00Z
  window_end: 2026-01-02T00:00:00Z
  records_per_partition: 25
objectives: {{rto_seconds: 1800, pass_rate: 1.0}}
evidence: {{backend: filesystem, path: {evidence}}}
"#,
            archive = archive.display(),
            evidence = evidence.display(),
        )
    }

    struct Run {
        code: i32,
        transcript: String,
    }

    /// Mount a bundle for `plan_text`, stamp a complete v2 contract over it and
    /// run the real binary.
    fn run_bound_restore(plan_text: &str) -> Run {
        let dir = tempfile::tempdir().expect("tempdir");
        let approver = SigningKey::generate_ed25519();
        let signing = SigningKey::generate_ed25519();
        let plan_bytes = plan_text.as_bytes().to_vec();
        let doc = ApprovalDoc {
            approver: "operator@example.com".into(),
            ticket: "CHG-D3-W5".into(),
            plan_hash: sha256_prefixed(&plan_bytes),
            approved_at: Utc::now(),
            subject_kind: "Restore".into(),
        };
        let approval = serde_json::to_vec(&doc).expect("approval serialises");
        let sidecar = serde_json::to_vec(
            &sign_detached(
                &approver,
                logweir::drill::phase1_approval::PAYLOAD_TYPE_APPROVAL,
                &approval,
            )
            .expect("sign"),
        )
        .expect("sidecar serialises");
        let approver_key = approver
            .verifying_key()
            .to_public_key_pem()
            .expect("pem")
            .into_bytes();
        let allowed = br#"{"allowed_cluster_ids":["TARGET00000000000000000"]}"#.to_vec();

        let plan_path = dir.path().join("restore.yaml");
        let approval_path = dir.path().join("approval.json");
        let approver_path = dir.path().join("approver.pub.pem");
        let allowed_path = dir.path().join("allowed-clusters.json");
        let signing_path = dir.path().join("signing.pem");
        std::fs::write(&plan_path, &plan_bytes).unwrap();
        std::fs::write(&approval_path, &approval).unwrap();
        std::fs::write(approval_path.with_extension("sig"), &sidecar).unwrap();
        std::fs::write(&approver_path, &approver_key).unwrap();
        std::fs::write(&allowed_path, &allowed).unwrap();
        std::fs::write(&signing_path, signing.to_pkcs8_pem().unwrap()).unwrap();

        let mut command = Command::new(env!("CARGO_BIN_EXE_logweir"));
        command
            .args(["restore", "run", wire::VERSION_ARG, wire::VERSION, "--spec"])
            .arg(&plan_path)
            .arg("--approval")
            .arg(&approval_path)
            .arg("--approver-key")
            .arg(&approver_path)
            .arg("--allowed-clusters")
            .arg(&allowed_path)
            .arg("--signing-key")
            .arg(&signing_path)
            .args(["--triggered-by", "approval/approval-a"]);
        for name in wire::ALL_ENV_ANY {
            command.env_remove(name);
        }
        for (name, value) in [
            (wire::VERSION_ENV, wire::VERSION.to_string()),
            (wire::SUBJECT_API_VERSION_ENV, "logweir.dev/v1alpha1".into()),
            (wire::SUBJECT_KIND_ENV, "Restore".into()),
            (wire::SUBJECT_NAME_ENV, "restore-a".into()),
            (wire::SUBJECT_NAMESPACE_ENV, "team-a".into()),
            (wire::SUBJECT_UID_ENV, "restore-uid-a".into()),
            (wire::APPROVAL_NAME_ENV, "approval-a".into()),
            (wire::APPROVAL_UID_ENV, "approval-uid-a".into()),
            (wire::PLAN_SHA256_ENV, sha256_prefixed(&plan_bytes)),
            (wire::APPROVAL_SHA256_ENV, sha256_prefixed(&approval)),
            (wire::APPROVAL_SIDECAR_SHA256_ENV, sha256_prefixed(&sidecar)),
            (
                wire::APPROVER_KEY_SHA256_ENV,
                sha256_prefixed(&approver_key),
            ),
            (wire::ALLOWED_CLUSTERS_SHA256_ENV, sha256_prefixed(&allowed)),
        ] {
            command.env(name, value);
        }
        let output = command.output().expect("run the binary");
        Run {
            code: output.status.code().unwrap_or(-1),
            transcript: format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ),
        }
    }

    /// An archive holding one receipt, and the binding that truthfully names it.
    fn archive() -> (tempfile::TempDir, String, String) {
        let dir = tempfile::tempdir().expect("tempdir");
        let manifest = br#"{"topics":[]}"#.to_vec();
        let manifest_sha256 = sha256_prefixed(&manifest);
        let manifest_key = "logweir/backups/nightly-7/run-1.manifest.json";
        let receipt = serde_json::json!({
            "format_version": "1.0.0",
            "run_id": "run-1",
            "backup_id": "nightly-7",
            "requested_at": "2026-01-01T00:00:00Z",
            "started_at": "2026-01-01T00:00:00Z",
            "finished_at": "2026-01-01T00:00:01Z",
            "exit_code": 0,
            "triggered_by": "manual",
            "source": {
                "cluster_id": "SOURCE00000000000000000",
                "bootstrap_servers": ["source:9092"],
                "auth": {"mode": "plaintext"},
                "topics": ["orders"]
            },
            "engine": {"id": "oso", "version": "1", "digest": "sha256:ee"},
            "archive": {
                "manifest_key": manifest_key,
                "manifest_sha256": manifest_sha256,
                "prefix": "logweir/backups/nightly-7/"
            },
            "records": {"orders": 3},
            "covered": {"from_ms": 1, "to_ms": 2}
        });
        let receipt_bytes = serde_json::to_vec(&receipt).expect("receipt serialises");
        let receipt_key = "logweir/backups/nightly-7/run-1.receipt.json";
        for (key, bytes) in [
            (receipt_key, receipt_bytes.clone()),
            (manifest_key, manifest),
        ] {
            let path = dir.path().join(key);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, bytes).unwrap();
        }
        let point_id = {
            // `lwp1-` + 32 hex of sha256(receipt bytes), D3 §5.1.
            let digest = sha256_prefixed(&receipt_bytes);
            format!("lwp1-{}", &digest["sha256:".len()..][..32])
        };
        let truthful = format!(
            "    point_id: {point_id}\n    receipt_key: {receipt_key}\n    \
             receipt_sha256: {}\n    manifest_sha256: {manifest_sha256}\n",
            sha256_prefixed(&receipt_bytes)
        );
        (dir, truthful, point_id)
    }

    /// **The ordering claim.** A bound point the archive does not hold is exit
    /// 3, and NO phase has begun when it is refused.
    #[test]
    fn a_point_the_archive_does_not_hold_is_refused_before_phase_zero() {
        let (archive_dir, truthful, _) = archive();
        let evidence = tempfile::tempdir().expect("tempdir");
        let tampered = truthful.replace(
            &truthful
                .lines()
                .find(|l| l.trim_start().starts_with("receipt_sha256:"))
                .expect("the digest line")
                .to_string(),
            &format!("    receipt_sha256: sha256:{}", "0".repeat(64)),
        );
        let run = run_bound_restore(&plan(archive_dir.path(), evidence.path(), &tampered));
        assert_eq!(run.code, 3, "{}", run.transcript);
        assert!(
            run.transcript.contains("PointBindingMismatch"),
            "{}",
            run.transcript
        );
        assert!(
            !run.transcript.contains("progress-phase=0:admit"),
            "no restore phase may begin: the refusal is BEFORE phase 0:\n{}",
            run.transcript
        );
        assert!(
            !run.transcript.contains("BrokerTransportFailure")
                && !run.transcript.contains("Connection refused")
                && !run.transcript.contains("19098"),
            "the plan's bootstrap must never be dialled:\n{}",
            run.transcript
        );
    }

    /// The control: the same plan with a truthful binding gets PAST the
    /// binding and fails later, for a reason that is not the binding. Without
    /// it, the row above would pass for a build that refused every bound plan.
    #[test]
    fn a_truthful_binding_gets_past_the_check_and_fails_later() {
        let (archive_dir, truthful, point_id) = archive();
        let evidence = tempfile::tempdir().expect("tempdir");
        // An evidence location that does not exist, so `drill::context` fails
        // FAST once the binding has been proven. Without it this row waits out
        // librdkafka's metadata timeout against a closed port to learn nothing
        // it does not already know: what is being asserted is that the run got
        // PAST the binding, not what it died of afterwards.
        let run = run_bound_restore(&plan(
            archive_dir.path(),
            &evidence.path().join("no-such-evidence-location"),
            &truthful,
        ));
        assert!(
            !run.transcript.contains("PointBindingMismatch"),
            "the binding is truthful; the run must fail for a LATER reason (exit {}):\n{}",
            run.code,
            run.transcript
        );
        assert!(
            run.transcript.contains(&point_id),
            "a verified binding names the point it proved:\n{}",
            run.transcript
        );
        assert_ne!(run.code, 0, "no broker is running, so it cannot succeed");
    }
}
