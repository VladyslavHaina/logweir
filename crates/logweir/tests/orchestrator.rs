//! Task 21a — the phase orchestrator, the `DrillError` -> exit-code contract
//! and the Prometheus textfile metrics.
mod fixtures;

use logweir::drill::{record, DrillError};
use logweir::exit::ExitCode;

/// The partial-record contract: the PhaseRecord is pushed BEFORE the phase's
/// result is returned, so a crash mid-drill still leaves a truthful document.
#[test]
fn a_failing_phase_still_leaves_its_record_and_updates_last_phase_completed() {
    let mut sc = fixtures::scorecard_pass();
    sc.phases.clear();
    sc.last_phase_completed = -1;

    let ok: Result<u8, DrillError> = record(&mut sc, 0, "admit", || Ok(1));
    assert!(ok.is_ok());
    assert_eq!(sc.phases.len(), 1);
    assert_eq!(sc.last_phase_completed, 0);

    let bad: Result<u8, DrillError> = record(&mut sc, 6, "restore", || {
        Err(DrillError::Operational("boom".into()))
    });
    assert!(bad.is_err());
    assert_eq!(sc.phases.len(), 2, "a failing phase still gets a record");
    assert_eq!(sc.phases[1].outcome, "failed: operational: boom");
    assert_eq!(
        sc.last_phase_completed, 0,
        "a phase that failed is not 'completed'"
    );
    assert!(sc.phases[1].duration_ms < 5_000);
}

#[test]
fn every_drill_error_maps_to_its_contracted_exit_code() {
    assert_eq!(
        ExitCode::from(DrillError::Guard(logweir_core::guard::GuardRefusal(
            "x".into()
        ))),
        ExitCode::GuardRefused
    );
    assert_eq!(
        ExitCode::from(DrillError::Operational("x".into())),
        ExitCode::Operational
    );
    assert_eq!(
        ExitCode::from(DrillError::SigningOrLock("x".into())),
        ExitCode::SigningOrLock
    );
    assert_eq!(
        ExitCode::from(DrillError::NotPass(
            Box::new(fixtures::scorecard_pass()),
            None
        )),
        ExitCode::DrillNotPass
    );
}

#[test]
fn the_textfile_metrics_carry_every_name_the_dashboard_reads() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("logweir.prom");
    logweir::metrics::write_textfile(&p, &fixtures::scorecard_pass()).unwrap();
    let t = std::fs::read_to_string(&p).unwrap();
    for m in [
        "logweir_drill_runs_total",
        "logweir_drill_rto_seconds",
        "logweir_drill_rpo_seconds",
        "logweir_drill_objective_met",
        "logweir_drill_fingerprint_mismatches",
        "logweir_drill_integrity_level",
        "logweir_drill_integrity_result",
        "logweir_evidence_lock_verified",
        // Task 22. Added by a later fix than the original eight and panelled in
        // `dashboards/logweir.json`, so it belongs in this list too.
        "logweir_drill_exit_code",
        // Task 4 fix round 1 (T0-3, display surface 2). Nothing here carried a
        // redaction signal, so a dashboard showed a clean drill over a document
        // that says a field was removed.
        "logweir_drill_redactions",
        // Task 11 (T0-7 rung 2). The metric `docs/kubernetes.md` rendered
        // inside a "Verified live" transcript and no code emitted.
        "logweir_drill_last_run_timestamp_seconds",
    ] {
        assert!(
            t.contains(m),
            "dashboards/logweir.json reads `{m}`, which nothing wrote:\n{t}"
        );
    }
    assert!(
        !t.contains("triggered_by"),
        "unbounded cardinality: it lives in the scorecard"
    );
}

/// Task 4 fix round 1 (T0-3, display surface 2). The Prometheus textfile
/// carried no redaction signal at all, so a dashboard rendered a clean drill
/// over a document announcing that a field had been removed.
///
/// The series is emitted UNCONDITIONALLY, `0` included. That is the property
/// this test pins hardest: a gauge that appears only when it is non-zero
/// cannot be alerted on with `logweir_drill_redactions > 0`, because to PromQL
/// an absent series and a whole document are the same thing.
#[test]
fn the_textfile_metrics_report_redactions_even_when_there_are_none() {
    let dir = tempfile::tempdir().unwrap();

    // The whole document every v0.1 run writes: the series must still be here.
    let whole = dir.path().join("whole.prom");
    let sc = fixtures::scorecard_pass();
    assert!(
        sc.redactions.is_empty(),
        "the fixture is the whole document"
    );
    logweir::metrics::write_textfile(&whole, &sc).unwrap();
    let t = std::fs::read_to_string(&whole).unwrap();
    assert!(
        t.contains("logweir_drill_redactions{cluster=\"MkU3OEVBNTcwNTJENDM2Qk\"} 0"),
        "an absent series and a whole document are indistinguishable to PromQL:\n{t}"
    );

    // A redacted document: the count is the alertable signal.
    let redacted = dir.path().join("redacted.prom");
    let mut sc = fixtures::scorecard_pass();
    sc.redactions = vec![
        logweir_core::scorecard::Redaction {
            path: "/measured/rpo_seconds".into(),
            reason: "customer policy".into(),
            present: false,
        },
        logweir_core::scorecard::Redaction {
            path: "/target/cluster_id".into(),
            reason: "customer policy".into(),
            present: false,
        },
    ];
    logweir::metrics::write_textfile(&redacted, &sc).unwrap();
    let t = std::fs::read_to_string(&redacted).unwrap();
    assert!(
        t.contains("logweir_drill_redactions{cluster=\"MkU3OEVBNTcwNTJENDM2Qk\"} 2"),
        "the redaction count must reach the dashboard:\n{t}"
    );
    // A count, never a path label: `path` is document-controlled and would be
    // unbounded cardinality, the same rule that keeps `triggered_by` out.
    assert!(
        !t.contains("/measured/rpo_seconds"),
        "a document-controlled path must never become a label:\n{t}"
    );
}

/// Task 22. The hand-maintained list above is a promise that someone remembers
/// to update it. This reads `dashboards/logweir.json` ITSELF, pulls every
/// `logweir_*` metric name out of its PromQL expressions, and requires each one
/// to appear in the textfile the CLI actually writes.
///
/// It is the mechanical half of the brief's rule "the dashboard must not outrun
/// the writer": adding a panel over a metric nothing emits turns this red
/// without anyone having to notice.
#[test]
fn every_metric_name_the_dashboard_queries_is_one_the_cli_writes() {
    let dashboard = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../dashboards/logweir.json"),
    )
    .expect("dashboards/logweir.json is checked in");

    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("logweir.prom");
    logweir::metrics::write_textfile(&p, &fixtures::scorecard_pass()).unwrap();
    let emitted = std::fs::read_to_string(&p).unwrap();

    // Every `logweir_`-prefixed identifier anywhere in the dashboard document.
    // Scanning the whole file rather than only `expr` fields is deliberate: a
    // metric named in a panel description a reader will act on is a claim too.
    let mut names: Vec<String> = Vec::new();
    let bytes = dashboard.as_bytes();
    let mut i = 0usize;
    while let Some(off) = dashboard[i..].find("logweir_") {
        let start = i + off;
        let mut end = start;
        while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
            end += 1;
        }
        names.push(dashboard[start..end].to_string());
        i = end;
    }
    names.sort();
    names.dedup();
    assert!(
        names.len() >= 8,
        "found only {} metric names in the dashboard; the scan is broken, not the dashboard",
        names.len()
    );
    for n in &names {
        assert!(
            emitted.contains(n.as_str()),
            "dashboards/logweir.json names `{n}`, which crates/logweir/src/metrics.rs \
             does not write. Either add the metric to the writer or drop the panel — \
             a dashboard over a metric nothing emits renders an empty graph that reads \
             as \"no drills failed\".\nwritten:\n{emitted}"
        );
    }
}

/// Phases 7 and 8 are wired: the scorecard is SCORED before it is signed.
/// `phase8_score::run` signs the document it is handed and never recomputes, so
/// if `compute_measured`/`decide` were dropped from `execute` the numbers would
/// be null and `outcome` would stay at its default — this test fails first.
#[test]
fn execute_scores_before_it_signs() {
    let f = fixtures::orchestrator_args_against_fixture_engine();
    let sc =
        logweir::drill::execute_with(&f.args, &f.run_id, &f.ctx).expect("fixture drill passes");
    assert!(
        sc.measured.rto_seconds.is_some(),
        "measured.rto_seconds is null"
    );
    assert!(
        sc.measured.rpo_seconds.is_some(),
        "measured.rpo_seconds is null"
    );
    assert_eq!(sc.outcome, logweir_core::outcome::Outcome::Pass);
    assert_eq!(sc.last_phase_completed, 9);
}

// ---------------------------------------------------------------------------
// Beyond the brief's four. Every test below exists to kill a specific mutant:
// a phase whose CALL SITE is deleted, a routing decision flipped, a value
// carried from the wrong place, or an obligation silently dropped.

use fixtures::Drill;
use logweir::drill::execute_with;
use logweir_core::outcome::{IntegrityLevel, IntegrityResult, Outcome};
use logweir_evidence::keys::{SigningKey, VerifyingKey};

struct NeverCalledEngine;

impl logweir_core::engine::DataEngine for NeverCalledEngine {
    fn id(&self) -> logweir_core::engine::EngineId {
        panic!("invalid signing material must stop before DataEngine::id")
    }

    fn list_backup_sets(
        &self,
        _: &logweir_core::engine::StorageUrl,
    ) -> Result<Vec<logweir_core::engine::BackupSetRef>, logweir_core::engine::EngineError> {
        panic!("invalid signing material must stop before DataEngine::list_backup_sets")
    }

    fn describe(
        &self,
        _: &logweir_core::engine::BackupSetRef,
    ) -> Result<logweir_core::engine::BackupSetFacts, logweir_core::engine::EngineError> {
        panic!("invalid signing material must stop before DataEngine::describe")
    }

    fn preflight(
        &self,
        _: &logweir_core::engine::RestorePlan,
    ) -> Result<logweir_core::engine::PreflightReport, logweir_core::engine::EngineError> {
        panic!("invalid signing material must stop before DataEngine::preflight")
    }

    fn restore(
        &self,
        _: &logweir_core::engine::RestorePlan,
        _: &mut dyn logweir_core::engine::PhaseObserver,
    ) -> Result<logweir_core::engine::RestoreFacts, logweir_core::engine::EngineError> {
        panic!("invalid signing material must stop before DataEngine::restore")
    }

    fn fingerprints(
        &self,
        _: &logweir_core::engine::SampleSelection,
    ) -> Result<Vec<logweir_core::engine::RecordFingerprint>, logweir_core::engine::EngineError>
    {
        panic!("invalid signing material must stop before DataEngine::fingerprints")
    }
}

fn signing_pub(f: &fixtures::OrchestratorFixture) -> VerifyingKey {
    SigningKey::from_pem_file(&f.args.signing_key)
        .unwrap()
        .verifying_key()
}

fn scorecard_from_store(f: &fixtures::OrchestratorFixture) -> Vec<u8> {
    f.ctx
        .store
        .get(&format!("logweir/drills/{}.json", f.run_id))
        .expect("phase 8 uploaded the scorecard")
        .0
}

#[test]
fn invalid_signing_material_stops_before_any_engine_call() {
    for case in ["missing", "malformed", "unreadable"] {
        let mut f = fixtures::orchestrator_args_against_fixture_engine();
        let path = f
            .args
            .signing_key
            .with_file_name(format!("{case}-signer.pem"));
        match case {
            "missing" => {}
            "malformed" => std::fs::write(
                &path,
                "-----BEGIN PRIVATE KEY-----\nDO-NOT-ECHO-KEY-MATERIAL\n-----END PRIVATE KEY-----\n",
            )
            .unwrap(),
            "unreadable" => std::fs::create_dir(&path).unwrap(),
            _ => unreachable!(),
        }
        f.args.signing_key = path.clone();
        f.ctx.engine = Box::new(NeverCalledEngine);

        let err = execute_with(&f.args, &f.run_id, &f.ctx)
            .expect_err("invalid signing material must refuse the run");
        assert!(
            matches!(err, DrillError::SigningPrerequisite(_)),
            "{case}: {err:?}"
        );
        let message = err.to_string();
        assert!(
            message.contains(&path.display().to_string()),
            "{case}: {message}"
        );
        assert!(
            message.contains("Mount a readable P-256 or Ed25519 PKCS#8 PEM private key"),
            "{case}: {message}"
        );
        assert!(
            message.contains("No engine data operation was started"),
            "{case}: {message}"
        );
        assert!(
            !message.contains("DO-NOT-ECHO-KEY-MATERIAL"),
            "{case}: key contents leaked: {message}"
        );
        assert!(
            f.ctx.store.list_keys("logweir/drills/").unwrap().is_empty(),
            "{case}: prerequisite failure must upload no evidence"
        );
    }
}

#[test]
fn rotating_the_file_does_not_change_any_execution_evidence_signer() {
    let f = fixtures::orchestrator_fixture(Drill::RotatesSigningKey);
    let original = signing_pub(&f);
    let replacement = f.replacement_signing_key.as_ref().unwrap();

    execute_with(&f.args, &f.run_id, &f.ctx).expect("the fixture drill passes");
    assert_eq!(
        SigningKey::from_pem_file(&f.args.signing_key)
            .unwrap()
            .verifying_key()
            .key_id(),
        replacement.key_id(),
        "the engine double must actually rotate the mounted file"
    );

    for (stem, payload_type) in [
        ("", logweir_evidence::PAYLOAD_TYPE_SCORECARD),
        (".receipt", logweir_evidence::PAYLOAD_TYPE_PUT_RECEIPT),
        (".teardown", logweir_evidence::PAYLOAD_TYPE_TEARDOWN),
    ] {
        let bytes = f
            .ctx
            .store
            .get(&format!("logweir/drills/{}{stem}.json", f.run_id))
            .unwrap()
            .0;
        let sidecar: logweir_evidence::Sidecar = serde_json::from_slice(
            &f.ctx
                .store
                .get(&format!("logweir/drills/{}{stem}.sig", f.run_id))
                .unwrap()
                .0,
        )
        .unwrap();
        logweir_evidence::verify::verify_detached(&original, payload_type, &bytes, &sidecar)
            .unwrap_or_else(|e| panic!("{stem} evidence must verify with the retained key: {e}"));
        assert!(
            logweir_evidence::verify::verify_detached(replacement, payload_type, &bytes, &sidecar)
                .is_err(),
            "{stem} evidence must not use the rotated file"
        );
    }
}

#[test]
fn an_external_ed25519_key_still_signs_independently_verifiable_evidence() {
    let f = fixtures::orchestrator_args_against_fixture_engine();
    let signer = SigningKey::generate_ed25519();
    std::fs::write(&f.args.signing_key, signer.to_pkcs8_pem().unwrap()).unwrap();
    let public = signer.verifying_key();

    execute_with(&f.args, &f.run_id, &f.ctx).expect("the Ed25519-backed drill passes");
    for (stem, payload_type) in [
        ("", logweir_evidence::PAYLOAD_TYPE_SCORECARD),
        (".receipt", logweir_evidence::PAYLOAD_TYPE_PUT_RECEIPT),
        (".teardown", logweir_evidence::PAYLOAD_TYPE_TEARDOWN),
    ] {
        let bytes = f
            .ctx
            .store
            .get(&format!("logweir/drills/{}{stem}.json", f.run_id))
            .unwrap()
            .0;
        let sidecar: logweir_evidence::Sidecar = serde_json::from_slice(
            &f.ctx
                .store
                .get(&format!("logweir/drills/{}{stem}.sig", f.run_id))
                .unwrap()
                .0,
        )
        .unwrap();
        logweir_evidence::verify::verify_detached(&public, payload_type, &bytes, &sidecar)
            .unwrap_or_else(|e| panic!("{stem} Ed25519 evidence must verify: {e}"));
    }
}

/// The phase-5 jump. `Verdict::Block` goes STRAIGHT to phase 8 — score, sign,
/// upload — and returns exit 2 with a signed artifact. Never phase 6, and
/// never exit 1 with nothing to show for it.
#[test]
fn a_blocked_preflight_exits_2_with_a_signed_scorecard_and_never_reaches_phase_6() {
    let f = fixtures::orchestrator_fixture(Drill::BlocksAtPreflight);
    let err = execute_with(&f.args, &f.run_id, &f.ctx).unwrap_err();
    let sc = match &err {
        logweir::drill::DrillError::NotPass(sc, _) => sc.clone(),
        other => panic!("a blocked preflight is a drill RESULT, got {other:?}"),
    };
    assert_eq!(
        ExitCode::from(err),
        ExitCode::DrillNotPass,
        "a preflight finding is the most valuable result a drill can produce; \
         exit 1 would discard it"
    );
    assert_eq!(sc.outcome, Outcome::PreflightFailed);
    assert_eq!(sc.integrity.level, IntegrityLevel::NotAttempted);
    assert_eq!(sc.integrity.result, IntegrityResult::Fail);
    assert!(sc.measured.rto_seconds.is_none());
    let phases: Vec<i8> = sc.phases.iter().map(|p| p.phase).collect();
    assert!(
        !phases.contains(&6) && !phases.contains(&7),
        "a blocked plan must never restore or verify: {phases:?}"
    );
    assert_eq!(
        phases,
        vec![0, 1, 2, 3, 4, 5],
        "the jump goes straight from 5 to 8, and 8's own record is pushed after \
         the document it signs is frozen"
    );
    // ...and it is genuinely SIGNED and uploaded, not merely returned.
    let bytes = scorecard_from_store(&f);
    let sidecar: logweir_evidence::Sidecar = serde_json::from_slice(
        &f.ctx
            .store
            .get(&format!("logweir/drills/{}.sig", f.run_id))
            .unwrap()
            .0,
    )
    .unwrap();
    logweir_evidence::verify::verify_detached(
        &signing_pub(&f),
        logweir_evidence::PAYLOAD_TYPE_SCORECARD,
        &bytes,
        &sidecar,
    )
    .expect("the uploaded preflight-failed scorecard must verify");
}

/// Carried obligation from Task 17. `PhaseRecord.notes` exists so a
/// `preflight-failed` scorecard states WHICH ground blocked the restore. This
/// task is its sole writer; without the copy the field is a sink nothing
/// reaches and the adjudication's reasoning is discarded.
#[test]
fn a_blocked_preflight_writes_every_finding_into_the_phase_5_notes() {
    let f = fixtures::orchestrator_fixture(Drill::BlocksAtPreflight);
    let sc = match execute_with(&f.args, &f.run_id, &f.ctx).unwrap_err() {
        logweir::drill::DrillError::NotPass(sc, _) => sc,
        other => panic!("expected NotPass, got {other:?}"),
    };
    let p5 = sc
        .phases
        .iter()
        .find(|p| p.phase == 5)
        .expect("phase 5 record");
    assert_eq!(p5.notes.len(), 1, "one finding, one note: {:?}", p5.notes);
    assert!(
        p5.notes[0].starts_with("orders/0 empty:"),
        "the note must name the topic, partition and ground: {:?}",
        p5.notes[0]
    );
    assert!(
        p5.notes[0].contains("no records in the selected window"),
        "the note must carry the finding's detail: {:?}",
        p5.notes[0]
    );
    // The note has to survive into the SIGNED bytes, not just the in-memory
    // document — that is the whole point of the field.
    let bytes = scorecard_from_store(&f);
    let signed: logweir_core::scorecard::Scorecard = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        signed
            .phases
            .iter()
            .find(|p| p.phase == 5)
            .map(|p| p.notes.clone())
            .unwrap_or_default(),
        p5.notes,
        "the findings must be inside the signed document"
    );
}

/// task-21a-addendum.md ruling A8. A restore that ran, exited 0, and left every
/// selected partition at end offset 0 is a positively established fact ABOUT
/// THE ARCHIVE. Routing it to exit 1 would throw away the most valuable
/// negative finding phase 6 can produce, and would leave no artifact.
#[test]
fn a_no_op_restore_is_a_drill_result_at_exit_2_never_an_operational_exit_1() {
    let f = fixtures::orchestrator_fixture(Drill::RestoresNothing);
    let err = execute_with(&f.args, &f.run_id, &f.ctx).unwrap_err();
    let sc = match &err {
        logweir::drill::DrillError::NotPass(sc, _) => sc.clone(),
        logweir::drill::DrillError::RestoreNoOp(m) => panic!(
            "RestoreNoOp escaped the phase-6 call site unintercepted; the next \
             `From<DrillError> for ExitCode` would panic: {m}"
        ),
        other => panic!("a no-op restore is a drill RESULT, got {other:?}"),
    };
    let code = ExitCode::from(err);
    assert_eq!(code, ExitCode::DrillNotPass);
    assert_ne!(
        code,
        ExitCode::Operational,
        "exit 1 says nothing about the archive; this finding says everything"
    );
    assert_eq!(sc.outcome, Outcome::FailIntegrity);
    assert_eq!(sc.integrity.level, IntegrityLevel::NotAttempted);
    assert_eq!(sc.integrity.result, IntegrityResult::Fail);
    assert!(sc.measured.rto_seconds.is_none());
    // Distinguishable from phase 5's block in the signed document itself.
    assert_ne!(sc.outcome, Outcome::PreflightFailed);
    let p6 = sc
        .phases
        .iter()
        .find(|p| p.phase == 6)
        .expect("phase 6 record");
    assert!(
        p6.outcome.starts_with("failed: drill-not-pass:"),
        "the phase record must name the classification: {}",
        p6.outcome
    );
    assert_eq!(p6.notes.len(), 1, "the reason belongs in the document");
    assert!(
        p6.notes[0].contains("end offset 0 after"),
        "{:?}",
        p6.notes[0]
    );
    assert!(
        !sc.phases.iter().any(|p| p.phase == 7),
        "phase 7 must never run over a restore that wrote nothing"
    );
    // Signed and uploaded, exactly as phase 5's block is.
    let bytes = scorecard_from_store(&f);
    assert!(!bytes.is_empty());
}

/// Every phase's CALL SITE, in order. Deleting any `record(...)` line, or
/// moving one, changes this list. Phase 8's own record is absent by design:
/// `phase8_score::run` is handed a frozen clone, so the document it signs
/// cannot contain the record of its own signing, and `execute_with` adopts
/// the signed document afterwards.
#[test]
fn every_phase_from_0_through_9_has_a_call_site_and_they_run_in_ascending_order() {
    let f = fixtures::orchestrator_args_against_fixture_engine();
    let sc = execute_with(&f.args, &f.run_id, &f.ctx).unwrap();
    let seen: Vec<(i8, &str)> = sc
        .phases
        .iter()
        .map(|p| (p.phase, p.name.as_str()))
        .collect();
    assert_eq!(
        seen,
        vec![
            (0, "admit"),
            (1, "approval"),
            (2, "target-ready"),
            (3, "target-diff"),
            (4, "sample-select"),
            (5, "preflight"),
            (6, "restore"),
            (7, "verify"),
            (9, "teardown"),
        ]
    );
    assert!(sc.phases.iter().all(|p| p.outcome == "ok"));
    // The signed document is the same sequence minus phase 9, which happens
    // after signing and is attested separately.
    let signed: logweir_core::scorecard::Scorecard =
        serde_json::from_slice(&scorecard_from_store(&f)).unwrap();
    assert_eq!(
        signed.phases.iter().map(|p| p.phase).collect::<Vec<_>>(),
        vec![0, 1, 2, 3, 4, 5, 6, 7]
    );
    assert_eq!(signed.last_phase_completed, 7);
}

/// THE CONSOLE AND THE ARTIFACT MUST AGREE. Nobody owned this question, and
/// they did not: `drill run` printed `last phase completed 9` for a run whose
/// signed scorecard says `7`. Both numbers were individually correct — phase 9
/// really did complete, and it really did happen after the bytes were frozen —
/// and an auditor comparing the two had no way to know that. Reading
/// `docs/formats/drill-scorecard.md` would have told them teardown never ran.
///
/// The assertion is against the SIGNED BYTES read back out of the store, never
/// against the in-memory document the line is derived from, so a mutant that
/// changes either side is caught. Deleting the fix — printing
/// `sc.last_phase_completed` again — makes this fail at assertion time with
/// `9` against `7`.
#[test]
fn the_stdout_line_quotes_the_signed_artifact_never_the_in_memory_copy() {
    let f = fixtures::orchestrator_args_against_fixture_engine();
    let sc = execute_with(&f.args, &f.run_id, &f.ctx).unwrap();
    let signed: logweir_core::scorecard::Scorecard =
        serde_json::from_slice(&scorecard_from_store(&f)).unwrap();

    assert_eq!(
        sc.last_phase_completed, 9,
        "the in-memory document legitimately outlives the artifact by two records"
    );
    assert_eq!(
        signed.last_phase_completed, 7,
        "and the artifact stops at 7"
    );

    let line = logweir::drill::summary_line(&sc);
    assert!(
        line.contains(&format!(
            "last phase completed {}",
            signed.last_phase_completed
        )),
        "the console must quote the signed artifact ({}), not the in-memory copy ({}): {line}",
        signed.last_phase_completed,
        sc.last_phase_completed
    );
    // The outcome is quoted in the artifact's own spelling too.
    assert!(
        line.contains("outcome pass"),
        "the outcome must read as the signed document spells it: {line}"
    );
}

/// `engine.matrix_verdict` DESCRIBES THE DRILL, and a failed drill must not
/// sign `"pass"`.
///
/// `docs/support-matrix.md` defines `pass` as "the full drill ran and passed".
/// Phase 5 raised the field the moment the `header_preflight` lever was
/// honoured and nothing lowered it again, so all four non-passing shapes below
/// signed `matrix_verdict: "pass"` with a null reason — verified on the real
/// artifacts read back out of the store here. A signed field saying "pass"
/// inside a failed drill is indefensible whatever it was meant to mean.
///
/// Read from the SIGNED BYTES, not from the returned document: the claim is
/// about what an auditor finds in the artifact.
#[test]
fn a_drill_that_did_not_pass_never_signs_a_matrix_pass() {
    for shape in [
        Drill::MissesTheRpoObjective,
        Drill::ReconcilesWithMismatches,
        Drill::BlocksAtPreflight,
        Drill::RestoresNothing,
    ] {
        let f = fixtures::orchestrator_fixture(shape);
        let _ = execute_with(&f.args, &f.run_id, &f.ctx);
        let signed: logweir_core::scorecard::Scorecard =
            serde_json::from_slice(&scorecard_from_store(&f)).unwrap();
        assert_ne!(
            signed.outcome,
            Outcome::Pass,
            "{shape:?} must not pass; the fixture is wrong"
        );
        assert_eq!(
            signed.engine.matrix_verdict,
            logweir_core::outcome::MatrixVerdict::Fail,
            "{shape:?} signed outcome {:?} beside matrix_verdict {:?}",
            signed.outcome,
            signed.engine.matrix_verdict
        );
        let reason = signed
            .engine
            .matrix_verdict_reason
            .as_deref()
            .unwrap_or_else(|| panic!("{shape:?}: a `fail` verdict must carry its reason"));
        assert!(
            reason.contains(signed.outcome.wire_name()),
            "{shape:?}: the reason must name the outcome that produced it: {reason:?}"
        );
    }
}

/// The other direction, so the fix cannot be "always fail": a drill that
/// passes at byte-fingerprint level signs `pass` with a null reason, exactly
/// as `docs/support-matrix.md`'s one green row records.
#[test]
fn a_drill_that_passed_at_byte_fingerprint_level_signs_a_matrix_pass() {
    let f = fixtures::orchestrator_args_against_fixture_engine();
    execute_with(&f.args, &f.run_id, &f.ctx).unwrap();
    let signed: logweir_core::scorecard::Scorecard =
        serde_json::from_slice(&scorecard_from_store(&f)).unwrap();
    assert_eq!(signed.outcome, Outcome::Pass);
    assert_eq!(signed.integrity.level, IntegrityLevel::ByteFingerprint);
    assert_eq!(
        signed.engine.matrix_verdict,
        logweir_core::outcome::MatrixVerdict::Pass
    );
    assert_eq!(signed.engine.matrix_verdict_reason, None);
}

/// The same property on the phase-5 jump, where the signed value is 5 and the
/// in-memory value is 5 as well — so this test's job is to prove the
/// derivation does not merely subtract two from whatever it is handed.
#[test]
fn the_stdout_line_quotes_the_artifact_on_the_blocked_preflight_path_too() {
    let f = fixtures::orchestrator_fixture(Drill::BlocksAtPreflight);
    let sc = match execute_with(&f.args, &f.run_id, &f.ctx).unwrap_err() {
        logweir::drill::DrillError::NotPass(sc, _) => sc,
        other => panic!("a blocked preflight is a drill RESULT: {other:?}"),
    };
    let signed: logweir_core::scorecard::Scorecard =
        serde_json::from_slice(&scorecard_from_store(&f)).unwrap();
    assert_eq!(signed.last_phase_completed, 5);
    assert!(
        logweir::drill::summary_line(&sc).contains("last phase completed 5"),
        "{}",
        logweir::drill::summary_line(&sc)
    );
}

/// task-21a-addendum.md ruling A5. The file at `--out` is the EXACT byte
/// string phase 8 signed — never a re-serialisation of a document phase 9
/// then mutated — so `logweir drill verify` on it recomputes the digest the
/// signature covers.
#[test]
fn the_out_artifact_is_the_exact_byte_string_phase_8_signed_and_still_verifies() {
    let f = fixtures::orchestrator_args_against_fixture_engine();
    let sc = execute_with(&f.args, &f.run_id, &f.ctx).unwrap();
    let on_disk = std::fs::read(&f.out).expect("--out was written");
    assert_eq!(
        on_disk,
        scorecard_from_store(&f),
        "the local artifact and the uploaded object must be the same bytes"
    );
    let sidecar: logweir_evidence::Sidecar =
        serde_json::from_slice(&std::fs::read(f.out.with_extension("sig")).unwrap()).unwrap();
    logweir_evidence::verify::verify_detached(
        &signing_pub(&f),
        logweir_evidence::PAYLOAD_TYPE_SCORECARD,
        &on_disk,
        &sidecar,
    )
    .expect("the --out artifact must verify against its own sidecar");
    // The in-memory document HAS grown phase 9 by now; re-serialising it would
    // have produced different bytes, which is the defect A5 names.
    assert_eq!(sc.last_phase_completed, 9);
    assert_ne!(
        logweir_core::det_json::to_deterministic_json(&sc).unwrap(),
        on_disk,
        "if these were equal the test could not distinguish the signed bytes \
         from a re-serialisation"
    );
}

/// Carried obligation from Task 20. Phase 8 signs before it puts, so the four
/// storage facts are unknowable at signing time and the scorecard neutralises
/// them. Until this receipt existed, Logweir published NO verifiable evidence
/// that its own upload was create-only.
#[test]
fn a_second_signed_receipt_publishes_the_post_put_readback() {
    let f = fixtures::orchestrator_args_against_fixture_engine();
    execute_with(&f.args, &f.run_id, &f.ctx).unwrap();

    let scorecard_bytes = scorecard_from_store(&f);
    let signed: logweir_core::scorecard::Scorecard =
        serde_json::from_slice(&scorecard_bytes).unwrap();
    assert!(
        !signed.evidence.create_only_enforced,
        "the SIGNED scorecard must keep under-claiming: the put had not happened"
    );

    let receipt_bytes = f
        .ctx
        .store
        .get(&format!("logweir/drills/{}.receipt.json", f.run_id))
        .expect("the post-put receipt must be uploaded beside the scorecard")
        .0;
    let sidecar: logweir_evidence::Sidecar = serde_json::from_slice(
        &f.ctx
            .store
            .get(&format!("logweir/drills/{}.receipt.sig", f.run_id))
            .expect("the receipt must be signed")
            .0,
    )
    .unwrap();
    logweir_evidence::verify::verify_detached(
        &signing_pub(&f),
        logweir_evidence::PAYLOAD_TYPE_PUT_RECEIPT,
        &receipt_bytes,
        &sidecar,
    )
    .expect("the receipt must verify with its own payload type");

    let r: logweir::drill::phase8_score::PutReceipt =
        serde_json::from_slice(&receipt_bytes).unwrap();
    assert_eq!(r.run_id, f.run_id);
    assert_eq!(
        r.scorecard_sha256,
        logweir_core::ids::sha256_prefixed(&scorecard_bytes),
        "the receipt must be bound to the exact signed bytes it describes"
    );
    assert_eq!(r.scorecard_key, format!("logweir/drills/{}.json", f.run_id));
    assert!(
        r.create_only_enforced,
        "the in-memory store DOES conditional puts; the receipt reports the \
         observed fact the scorecard could not"
    );
}

/// Carried obligation from Task 17, second half: nothing pinned the
/// skip-when-empty byte-stability that keeps the already-signed fixtures
/// valid. `notes` was added to `PhaseRecord` after those fixtures were minted,
/// so an omitted-when-empty field is the only thing keeping their signatures
/// good — and this asserts the CRYPTOGRAPHY, not merely that they parse.
#[test]
fn an_empty_notes_field_stays_absent_so_the_checked_in_signed_fixtures_still_verify() {
    for stem in ["scorecard", "scorecard-self-attested"] {
        check_signed_fixture_round_trips(stem);
    }
}

fn check_signed_fixture_round_trips(stem: &str) {
    let bytes = std::fs::read(format!("../../e2e/fixtures/signed/{stem}.json")).unwrap();
    let sc: logweir_core::scorecard::Scorecard = serde_json::from_slice(&bytes).unwrap();
    assert!(
        !sc.phases.is_empty() && sc.phases.iter().all(|p| p.notes.is_empty()),
        "this fixture predates `notes`; it must still carry phase records"
    );
    let round = logweir_core::det_json::to_deterministic_json(&sc).unwrap();
    assert_eq!(
        round, bytes,
        "re-serialising must reproduce the checked-in bytes exactly: an empty \
         `notes` that serialised as `[]` would break every signature minted \
         before the field existed"
    );
    let sidecar: logweir_evidence::Sidecar = serde_json::from_slice(
        &std::fs::read(format!("../../e2e/fixtures/signed/{stem}.sig")).unwrap(),
    )
    .unwrap();
    let key =
        VerifyingKey::from_pem_file(std::path::Path::new("../../e2e/fixtures/signed/public.pem"))
            .unwrap();
    logweir_evidence::verify::verify_detached(
        &key,
        logweir_evidence::PAYLOAD_TYPE_SCORECARD,
        &round,
        &sidecar,
    )
    .expect("the regenerated bytes must still verify, not merely parse");
    // And the skip is load-bearing rather than incidental: one note changes
    // the bytes, so a signature over the old document would no longer hold.
    let mut mutated = sc.clone();
    mutated.phases[0].notes.push("x".into());
    assert_ne!(
        logweir_core::det_json::to_deterministic_json(&mutated).unwrap(),
        bytes
    );
}

/// The `Selection` phase 4 emits carries an EMPTY `manifest_key`, and
/// `OsoCliEngine::fingerprints` refuses one. The orchestrator is the only
/// place that holds both the `Selection` and the real `BackupSetRef`.
#[test]
fn the_backup_set_ref_is_bound_into_every_selection_before_the_engine_fingerprints_it() {
    let f = fixtures::orchestrator_args_against_fixture_engine();
    execute_with(&f.args, &f.run_id, &f.ctx).unwrap();
    // The fixture engine records what it was asked about; a real engine would
    // simply have refused.
    let calls = fixtures::fingerprint_calls(&f);
    assert!(!calls.is_empty(), "phase 7 must reach the engine at all");
    for s in &calls {
        assert!(
            !s.set.manifest_key.is_empty(),
            "phase 4 emits an empty manifest_key; `bind_backup_set` was skipped for {}/{}",
            s.topic,
            s.partition
        );
        assert_eq!(s.set.manifest_key, "drills/fixture/manifest.json");
    }
}

/// `sample.records_expected` is the CANARY SIZE, not the manifest's window
/// total. Task 16 parked the distinction for this task; substituting one for
/// the other would make the signed document overstate what was verified.
#[test]
fn the_sample_block_publishes_the_canary_size_not_the_manifest_window_total() {
    let f = fixtures::orchestrator_args_against_fixture_engine();
    let sc = execute_with(&f.args, &f.run_id, &f.ctx).unwrap();
    assert_eq!(sc.sample.partitions, 1);
    assert_eq!(sc.sample.topics, 1);
    assert_eq!(
        sc.sample.records_expected,
        fixtures::FIXTURE_SAMPLE_RECORDS as u64,
        "records_per_partition x partitions selected"
    );
    assert_eq!(
        sc.sample.records_restored,
        fixtures::FIXTURE_SAMPLE_RECORDS as u64
    );
    assert_eq!(sc.sample.anchor, "head");
    assert_eq!(sc.integrity.records_sampled, sc.sample.records_expected);
}

/// `rto_excluding_preflight_seconds` is THE number compared against the RTO
/// objective, and phase 5's header sweep — which no incident responder
/// performs — must not be in it. The duration comes from the phase-5 record,
/// which is why phase 5's engine call sits inside that record.
#[test]
fn the_phase_5_duration_is_subtracted_from_the_scored_rto() {
    let f = fixtures::orchestrator_fixture(Drill::HasASlowPreflight);
    let sc = execute_with(&f.args, &f.run_id, &f.ctx).unwrap();
    let p5 = sc.phases.iter().find(|p| p.phase == 5).unwrap();
    assert!(
        p5.duration_ms >= 1_000,
        "the fixture's preflight sleeps; got {}ms",
        p5.duration_ms
    );
    let rto = sc.measured.rto_seconds.unwrap();
    let excl = sc.measured.rto_excluding_preflight_seconds.unwrap();
    assert!(rto >= 1, "the slow preflight must show up in the raw RTO");
    assert_eq!(
        excl,
        rto - (p5.duration_ms / 1000),
        "the phase-5 duration reaching `Timeline` is what makes these differ"
    );
    assert!(excl < rto);
    // ...and the restore-only figure brackets the SUBPROCESS, not the
    // preflight that ran before it: the fixture's restore is instantaneous, so
    // a `restore_started_at` taken from any earlier instant would show up here
    // as a second or more.
    assert_eq!(
        sc.measured.rto_restore_only_seconds,
        Some(0),
        "restore_started_at must come from phase 6's own Restored, not from an \
         earlier timestamp on the scorecard"
    );
}

/// An engine identity is what an auditor uses to say WHICH engine produced a
/// restore. An empty version or digest is not "unknown", it is a signed
/// document that names no engine at all.
#[test]
fn a_scorecard_is_never_signed_over_an_engine_that_names_itself_nothing() {
    let f = fixtures::orchestrator_fixture(Drill::NamesNoEngine);
    let err = execute_with(&f.args, &f.run_id, &f.ctx).unwrap_err();
    assert_eq!(ExitCode::from(err), ExitCode::Operational);
    assert!(
        f.ctx.store.list_keys("logweir/").unwrap().is_empty(),
        "nothing may be uploaded for a run that cannot name its engine"
    );
}

/// The facts each phase established have to reach the document. A phase whose
/// RESULT is dropped on the floor is the same defect as a phase that never
/// ran, and no ordering assertion catches it.
#[test]
fn each_phases_result_reaches_the_signed_document() {
    let f = fixtures::orchestrator_args_against_fixture_engine();
    execute_with(&f.args, &f.run_id, &f.ctx).unwrap();
    let sc: logweir_core::scorecard::Scorecard =
        serde_json::from_slice(&scorecard_from_store(&f)).unwrap();

    // 0 — admission
    assert_eq!(sc.target.cluster_id, fixtures::FIXTURE_CLUSTER_ID);
    // `Some`, because the fixture spec is a SCRATCH restore — the mode whose
    // phase 0 verifies the marker topic. A `newTopic` run carries `None` here
    // and `restore_mode.rs` owns that arm.
    assert_eq!(
        sc.target.marker_topic.as_deref(),
        Some(fixtures::FIXTURE_MARKER_TOPIC)
    );
    assert!(sc.target.mode.is_scratch());
    assert_eq!(sc.target.topic_mapping_entries, 1);
    assert_eq!(
        sc.target.topic_mapping_sha256,
        logweir_core::ids::sha256_prefixed(
            logweir_engine_oso::render_restore::render_topic_mapping_block(
                &[("orders".to_string(), "drill-orders".to_string())]
                    .into_iter()
                    .collect()
            )
            .expect("G-EXP: the fixture mapping holds no `${`")
            .as_bytes()
        ),
        "the hash must cover the block AS RENDERED into restore.yaml"
    );
    // 1 — approval
    assert_eq!(sc.approval.ticket, "CHG-40881");
    assert_eq!(sc.approval.approver, "sre-oncall@example.com");
    assert!(
        sc.approval.self_attested,
        "the fixture signs its own approval"
    );
    assert!(sc.approval_validated_at.is_some());
    // the archive read
    assert_eq!(sc.source.backup_id, "backup-2026-08-30T02:00:00Z");
    assert!(!sc.source.manifest_sha256.is_empty());
    assert!(!sc.source.captured_by_logweir, "phase -1 is Task 24's");
    // 3 — the diff.
    //
    // NO collision, and that is now a property rather than an accident: phase
    // 0 REFUSES a plan whose mapped target topics already exist (spec §6.1,
    // `phase0_admit::run`), so a drill that reaches phase 3 at all found the
    // scratch namespace empty. The fixture target lists the marker only for
    // exactly that reason. `phase3_diff`'s collision path keeps its own
    // coverage in `tests/phases_2_4.rs`, which drives the phase directly, and
    // the refusal has its own row in
    // `tests/topic_preflight.rs::a_mapped_target_topic_that_already_exists_is_a_guard_refusal_naming_it`.
    assert!(
        sc.target_diff.collisions.is_empty(),
        "a target topic that already existed would have been refused at phase 0: {:?}",
        sc.target_diff.collisions
    );
    // What the diff DID read off the target reaches the document as the
    // absent/would-create pair, at the manifest's partition count — the same
    // count the creation step then uses.
    assert_eq!(sc.target_diff.absent, vec!["drill-orders".to_string()]);
    assert_eq!(
        sc.target_diff.would_create,
        vec![("drill-orders".to_string(), 1)],
        "the diff must report what it actually READ off the target"
    );
    assert_eq!(sc.target_diff.level, "full");
    // FX-4: phase 3 ran and found no collision, so its 1.1.0 `not_assessed`
    // reaches the signed document as the claim `[]`, never absent ("not
    // recorded"), and the collision strings carry no qualifier of their own.
    assert_eq!(
        sc.target_diff.not_assessed,
        Some(vec![]),
        "phase 3's not_assessed must reach the signed document"
    );
    // 5 — the lever readback
    assert_eq!(
        sc.engine.levers.header_preflight,
        logweir_core::outcome::LeverState::Honoured
    );
    assert_eq!(
        sc.engine.matrix_verdict,
        logweir_core::outcome::MatrixVerdict::Pass
    );
    assert_eq!(sc.engine.version, "v0.21.0-fixture");
    // 7 — integrity and parity
    assert_eq!(sc.integrity.level, IntegrityLevel::ByteFingerprint);
    assert_eq!(sc.integrity.result, IntegrityResult::Pass);
    assert_eq!(sc.integrity.mismatches, 0);
    assert!(
        sc.topic_parity
            .intentionally_deviated
            .contains(&"drill-orders: cleanup.policy".to_string()),
        "a config the restore deliberately changed is INTENDED, and is named \
         against the target topic: {:?}",
        sc.topic_parity.intentionally_deviated
    );
    // FX-4: the fixture's plan binds no recovery point, so there is no signed
    // record of what was captured. No key diverged, and phase 7 says so in
    // `not_assessed` AND leaves its fail-safe marker in `unexpected_divergence`,
    // so a reader older than `not_assessed` cannot read parity here (M5).
    assert_eq!(
        sc.topic_parity.unexpected_divergence,
        vec!["drill-orders: configuration not assessed (unknown)".to_string()]
    );
    assert_eq!(
        sc.topic_parity.not_assessed,
        Some(vec!["drill-orders: configuration (unknown)".to_string()])
    );
    // FX-3: a SCRATCH drill's deviations are intended (above), and phase 7
    // makes the 1.2.0 claim that nothing was left unreconstructed.
    assert_eq!(sc.topic_parity.not_reconstructed, Some(vec![]));
    // FX-23: every SAMPLED document is 1.6.0, so the version marks a build
    // with FX-23's checks; a complete one stays 1.4.0 (below).
    assert_eq!(
        sc.format_version,
        logweir_core::scorecard::FORMAT_VERSION_WITH_UNSAMPLED_TOPICS
    );
    // FX-8: the fixture's plan states no point in time and its sample window
    // ends on the archive's newest timestamp, so nothing was selected by time:
    // the block is WRITTEN (1.3.0's claim) and both lists are empty.
    assert_eq!(
        sc.source.time_basis,
        Some(logweir_core::scorecard::TimeBasisLabel::default())
    );
    // 8 — the objectives, as REQUESTED plus the verdict
    assert_eq!(sc.objectives.rto_seconds, Some(900));
    assert_eq!(sc.objectives.met, Some(true));
    assert_eq!(sc.triggered_by.as_deref(), Some("fixture"));
}

/// **FX-3, end to end through the phase sequence.** The same fixture drill as
/// `each_phases_result_reaches_the_signed_document`, as a `newTopic` restore:
/// the spec's mode reaches phase 7 and decides the label in the SIGNED
/// document. The deviation the scratch run signs as intended (the source's
/// `cleanup.policy`) is signed here as NOT reconstructed, and also in
/// `unexpected_divergence`, where a reader older than format 1.2.0 sees it.
///
/// The approval covers the fixture's unchanged spec bytes; the mode and a
/// `topic_naming.prefix` that keeps the fixture's `drill-orders` name are set
/// on the parsed spec, which is what phases 0, 7 and 9 read.
///
/// Negative control: an orchestrator that hands phase 7 `TargetMode::Scratch`
/// instead of `c.spec.target.mode` signs `intentionally_deviated:
/// ["drill-orders: cleanup.policy"]` here and this test fails.
#[test]
fn a_new_topic_run_signs_its_lost_source_settings_as_not_reconstructed() {
    let mut f = fixtures::orchestrator_args_against_fixture_engine();
    f.ctx.spec.target.mode = logweir_core::spec::TargetMode::NewTopic;
    f.ctx.spec.target.topic_naming = Some(logweir_core::spec::TopicNaming {
        prefix: "drill-".into(),
    });
    execute_with(&f.args, &f.run_id, &f.ctx).unwrap();
    let sc: logweir_core::scorecard::Scorecard =
        serde_json::from_slice(&scorecard_from_store(&f)).unwrap();
    assert_eq!(sc.target.mode, logweir_core::spec::TargetMode::NewTopic);
    // FX-23: every SAMPLED document is 1.6.0, so the version marks a build
    // with FX-23's checks; a complete one stays 1.4.0 (below).
    assert_eq!(
        sc.format_version,
        logweir_core::scorecard::FORMAT_VERSION_WITH_UNSAMPLED_TOPICS
    );
    assert!(
        sc.topic_parity.intentionally_deviated.is_empty(),
        "a newTopic restore signs nothing as intended: {:?}",
        sc.topic_parity.intentionally_deviated
    );
    let not_reconstructed = sc
        .topic_parity
        .not_reconstructed
        .clone()
        .expect("phase 7 ran, so the 1.2.0 field is recorded");
    assert!(
        not_reconstructed.contains(&"drill-orders: cleanup.policy".to_string()),
        "{not_reconstructed:?}"
    );
    for entry in &not_reconstructed {
        assert!(
            sc.topic_parity.unexpected_divergence.contains(entry),
            "{entry} must also be an unexpected divergence: {:?}",
            sc.topic_parity.unexpected_divergence
        );
    }
}

/// **FX-3 review F2 (and FX-4's twin): a scorecard signed BEFORE phase 7
/// records neither parity claim.** `topic_parity.not_reconstructed` and
/// `topic_parity.not_assessed` are written by phase 7 alone; `[]` in either is
/// the claim "compared, and nothing left unreconstructed / unassessed". A run
/// that stops at phase 5 (a blocked preflight) or phase 6 (a restore that
/// wrote nothing) signs both ABSENT, in both modes, so no reader reads a
/// comparison that never ran.
///
/// Negative control: `new_scorecard` drafting `not_reconstructed:
/// Some(Vec::new())` (the review's surviving mutant R6), or `not_assessed:
/// Some(Vec::new())`, signs the key and this test fails.
#[test]
fn a_scorecard_signed_before_phase_7_records_neither_parity_claim() {
    for drill in [Drill::BlocksAtPreflight, Drill::RestoresNothing] {
        for mode in [
            logweir_core::spec::TargetMode::Scratch,
            logweir_core::spec::TargetMode::NewTopic,
        ] {
            let mut f = fixtures::orchestrator_fixture(drill);
            f.ctx.spec.target.mode = mode;
            if mode == logweir_core::spec::TargetMode::NewTopic {
                f.ctx.spec.target.topic_naming = Some(logweir_core::spec::TopicNaming {
                    prefix: "drill-".into(),
                });
            }
            let sc = match execute_with(&f.args, &f.run_id, &f.ctx).unwrap_err() {
                logweir::drill::DrillError::NotPass(sc, _) => sc,
                other => panic!("{drill:?} / {mode:?}: expected a signed NotPass, got {other:?}"),
            };
            assert!(
                !sc.phases.iter().any(|p| p.phase == 7),
                "{drill:?} / {mode:?}: phase 7 must not have run"
            );
            let signed: serde_json::Value =
                serde_json::from_slice(&scorecard_from_store(&f)).unwrap();
            let parity = signed["topic_parity"]
                .as_object()
                .expect("the signed document carries topic_parity");
            for claim in ["not_reconstructed", "not_assessed"] {
                assert!(
                    !parity.contains_key(claim),
                    "{drill:?} / {mode:?}: a scorecard signed before phase 7 must not record \
                     topic_parity.{claim}: {parity:?}"
                );
            }
        }
    }
}

/// Phase 9's call site, its policy and its binding. The attestation is bound
/// to the SIGNED BYTES, not to the run id a second time.
#[test]
fn teardown_deletes_the_mapped_scratch_topics_and_attests_them_against_the_signed_bytes() {
    let f = fixtures::orchestrator_args_against_fixture_engine();
    execute_with(&f.args, &f.run_id, &f.ctx).unwrap();
    let bytes = f
        .ctx
        .store
        .get(&format!("logweir/drills/{}.teardown.json", f.run_id))
        .expect("phase 9 must persist its attestation")
        .0;
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["teardown_policy"], "delete");
    assert_eq!(v["topics_deleted"], serde_json::json!(["drill-orders"]));
    assert_eq!(v["topics_failed"], serde_json::json!([]));
    assert_eq!(
        v["scorecard_sha256"],
        serde_json::json!(logweir_core::ids::sha256_prefixed(&scorecard_from_store(
            &f
        )))
    );
}

/// The metrics file is written for a drill RESULT, and it carries the one
/// thing Kubernetes hides: which exit code this run produced.
#[test]
fn the_metrics_file_distinguishes_a_pass_from_a_signed_non_pass() {
    let pass = fixtures::orchestrator_args_against_fixture_engine();
    let sc = execute_with(&pass.args, &pass.run_id, &pass.ctx).unwrap();
    logweir::metrics::write_textfile(&pass.metrics, &sc).unwrap();
    let t = std::fs::read_to_string(&pass.metrics).unwrap();
    assert!(t.contains("outcome=\"pass\""), "{t}");
    assert!(
        t.contains("logweir_drill_exit_code{cluster=\"MkU3OEVBNTcwNTJENDM2Qk\"} 0"),
        "{t}"
    );

    let blocked = fixtures::orchestrator_fixture(Drill::BlocksAtPreflight);
    let sc = match execute_with(&blocked.args, &blocked.run_id, &blocked.ctx).unwrap_err() {
        logweir::drill::DrillError::NotPass(sc, _) => *sc,
        other => panic!("{other:?}"),
    };
    logweir::metrics::write_textfile(&blocked.metrics, &sc).unwrap();
    let t = std::fs::read_to_string(&blocked.metrics).unwrap();
    assert!(t.contains("outcome=\"preflight-failed\""), "{t}");
    assert!(
        t.contains("logweir_drill_exit_code{cluster=\"MkU3OEVBNTcwNTJENDM2Qk\"} 2"),
        "a drill result must never be reported as exit 1: {t}"
    );
    assert!(t.contains("logweir_drill_integrity_level{cluster=\"MkU3OEVBNTcwNTJENDM2Qk\",level=\"not-attempted\"} 1"), "{t}");
}

/// Every name below has to be present as a SAMPLE, not merely inside the
/// `# HELP` / `# TYPE` comment lines that mention them. A metric whose value
/// line is deleted still leaves its name in the comments, so a bare
/// `contains(name)` check — which is what the brief's own test performs —
/// cannot tell the two apart, and a dashboard reading it would show no data.
#[test]
fn every_metric_name_is_emitted_as_a_sample_not_only_as_a_help_comment() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("logweir.prom");
    logweir::metrics::write_textfile(&p, &fixtures::scorecard_pass()).unwrap();
    let text = std::fs::read_to_string(&p).unwrap();
    let samples: Vec<&str> = text
        .lines()
        .filter(|l| !l.starts_with('#') && !l.trim().is_empty())
        .collect();
    for m in [
        "logweir_drill_runs_total",
        "logweir_drill_rto_seconds",
        "logweir_drill_rpo_seconds",
        "logweir_drill_objective_met",
        "logweir_drill_fingerprint_mismatches",
        "logweir_drill_integrity_level",
        "logweir_drill_integrity_result",
        "logweir_evidence_lock_verified",
        "logweir_drill_exit_code",
        // Task 11 (T0-7 rung 2). This entry is what kills the "emit the
        // HELP/TYPE block and no sample line" mutant.
        "logweir_drill_last_run_timestamp_seconds",
    ] {
        assert!(
            samples.iter().any(|l| l.starts_with(m)),
            "`{m}` appears only in a HELP/TYPE comment; no sample was emitted:\n{text}"
        );
    }
    // All three objectives are non-null in this fixture, so all three lines
    // must be there — a loop that emitted only the first would pass a bare
    // name check.
    for o in ["rto", "rpo", "pass_rate"] {
        assert!(
            samples
                .iter()
                .any(|l| l.contains(&format!("objective=\"{o}\""))),
            "the {o} objective produced no sample:\n{text}"
        );
    }
}

/// The lever readback is OBSERVED, never declared. A scorecard that never saw
/// the engine honour `header_preflight` must not carry a matrix pass — and
/// this is the only path on which the two differ, because a honoured lever
/// sets both to their positive values.
#[test]
fn an_ignored_engine_lever_is_reported_as_ignored_and_never_as_a_matrix_pass() {
    let f = fixtures::orchestrator_fixture(Drill::IgnoresTheHeaderLever);
    let sc = match execute_with(&f.args, &f.run_id, &f.ctx).unwrap_err() {
        logweir::drill::DrillError::NotPass(sc, _) => sc,
        other => panic!("an ignored lever blocks the plan at phase 5: {other:?}"),
    };
    assert_eq!(
        sc.engine.levers.header_preflight,
        logweir_core::outcome::LeverState::Ignored
    );
    assert_eq!(
        sc.engine.matrix_verdict,
        logweir_core::outcome::MatrixVerdict::FailLeverNotHonoured,
        "a run that did not observe the lever must not publish `pass`"
    );
    assert_eq!(sc.outcome, Outcome::PreflightFailed);
    let p5 = sc.phases.iter().find(|p| p.phase == 5).unwrap();
    assert!(
        p5.notes.iter().any(|n| n.contains("lever-ignored")),
        "{:?}",
        p5.notes
    );
}

/// Spec §7.2(a): a config key Logweir rendered that the engine dropped is a
/// per-run readback that must reach the signed document. There are TWO such
/// readbacks — phase 5's preflight and phase 6's restore — and phase 6's
/// arrives after phase 5's has already been written, so it has to be merged
/// rather than overwrite or be overwritten.
#[test]
fn a_key_the_engine_dropped_during_the_restore_reaches_the_signed_levers() {
    let f = fixtures::orchestrator_fixture(Drill::DropsARenderedKeyDuringRestore);
    execute_with(&f.args, &f.run_id, &f.ctx).unwrap();
    let sc: logweir_core::scorecard::Scorecard =
        serde_json::from_slice(&scorecard_from_store(&f)).unwrap();
    assert_eq!(
        sc.engine.levers.unknown_key_warnings,
        vec![
            // phase 5's, recorded first...
            "restore.header_preflight".to_string(),
            // ...and phase 6's, MERGED in rather than replacing it.
            "restore.checkpoint_interval_secs".to_string(),
        ],
        "both readbacks have to survive: phase 5's is written before phase 6 \
         runs, and phase 6's arrives afterwards"
    );
}

/// THE MAINLINE EXIT-2 PATH — a drill that ran every phase, was measured, was
/// SCORED, and did not pass.
///
/// This is the review's finding R3. Both exit-2 routes that were covered
/// before — a blocked preflight and a no-op restore — `return Err(NotPass)`
/// early from inside phases 5 and 6 and never reach `execute_with`'s final
/// `if sc.outcome != Outcome::Pass` gate. So that gate could be replaced with
/// `if false` and the entire workspace stayed green: a restore that missed its
/// RTO by 25 minutes would be signed as `fail-objective` in the bucket and
/// reported to the CronJob as **exit 0, passed**. That is the most valuable
/// result this product produces, announced as its opposite.
///
/// Both scoring routes are covered: `decide` reaches `FailObjective` through
/// the objectives and `FailIntegrity` through phase 7's verdict, and they are
/// different branches of the same function.
#[test]
fn a_scored_drill_that_does_not_pass_exits_2_after_running_every_phase() {
    for (shape, want) in [
        (Drill::MissesTheRpoObjective, Outcome::FailObjective),
        (Drill::ReconcilesWithMismatches, Outcome::FailIntegrity),
    ] {
        let f = fixtures::orchestrator_fixture(shape);
        let err = execute_with(&f.args, &f.run_id, &f.ctx)
            .err()
            .unwrap_or_else(|| panic!("{shape:?} must not report success"));
        let sc = match &err {
            logweir::drill::DrillError::NotPass(sc, _) => sc.clone(),
            other => panic!("{shape:?} is a drill RESULT, got {other:?}"),
        };

        // The exit code. `report` turns this same `NotPass` into the process
        // status (pinned by
        // `drill::tests::a_drill_result_reports_exit_2_and_an_operational_failure_reports_exit_1`),
        // so the two together cover the whole chain from score to exit status.
        let code = ExitCode::from(err);
        assert_eq!(code, ExitCode::DrillNotPass, "{shape:?}");
        assert_ne!(code, ExitCode::Ok, "{shape:?} must never report success");

        // It went THROUGH scoring rather than returning early: phases 6 and 7
        // both ran, and `measured` carries real numbers.
        let phases: Vec<i8> = sc.phases.iter().map(|p| p.phase).collect();
        assert!(
            phases.contains(&6) && phases.contains(&7),
            "{shape:?} must reach the final gate, not an early return: {phases:?}"
        );
        assert!(
            sc.measured.rto_seconds.is_some(),
            "{shape:?} was not scored"
        );
        assert!(
            sc.measured.rpo_seconds.is_some(),
            "{shape:?} was not scored"
        );
        assert_eq!(sc.outcome, want, "{shape:?}");

        // ...and a scorecard was EMITTED, which is what makes exit 2 different
        // from exit 1: locally at --out, and in the bucket.
        let on_disk = std::fs::read(&f.out).expect("--out was written");
        assert_eq!(on_disk, scorecard_from_store(&f));
        let signed: logweir_core::scorecard::Scorecard = serde_json::from_slice(&on_disk).unwrap();
        assert_eq!(
            signed.outcome, want,
            "{shape:?}: the SIGNED document must carry the failing outcome"
        );
    }

    // The positive direction of the same gate: a genuine pass must still be
    // exit 0. A mutant that always returns `NotPass` has to fail somewhere.
    let ok = fixtures::orchestrator_args_against_fixture_engine();
    let sc = execute_with(&ok.args, &ok.run_id, &ok.ctx)
        .expect("a passing drill must not be reported as a non-pass");
    assert_eq!(sc.outcome, Outcome::Pass);
}

/// FIX 2. The `RestoreNoOp` interception runs a restore that ACTUALLY
/// EXECUTED, so anything it created on the operator's cluster is still there.
/// Returning straight to exit 2 would leave scratch topics behind on the one
/// failure path where a drill wrote to their broker.
///
/// The phase-5 branch deliberately does NOT tear down: `restore` never ran, so
/// that drill created nothing, and deleting a mapped topic it did not create
/// would destroy someone else's data to tidy up after a run that touched
/// nothing. Both halves of that asymmetry are asserted here.
#[test]
fn a_no_op_restore_still_tears_down_its_scratch_topics_but_a_blocked_preflight_does_not() {
    let f = fixtures::orchestrator_fixture(Drill::RestoresNothing);
    let sc = match execute_with(&f.args, &f.run_id, &f.ctx).unwrap_err() {
        logweir::drill::DrillError::NotPass(sc, _) => sc,
        other => panic!("{other:?}"),
    };
    assert!(
        sc.phases.iter().any(|p| p.phase == 9),
        "the restore ran; its scratch topics must not be left behind: {:?}",
        sc.phases.iter().map(|p| p.phase).collect::<Vec<_>>()
    );
    let att = f
        .ctx
        .store
        .get(&format!("logweir/drills/{}.teardown.json", f.run_id))
        .expect("a teardown that happened must be attested")
        .0;
    let v: serde_json::Value = serde_json::from_slice(&att).unwrap();
    assert_eq!(v["topics_deleted"], serde_json::json!(["drill-orders"]));
    assert_eq!(
        v["scorecard_sha256"],
        serde_json::json!(logweir_core::ids::sha256_prefixed(&scorecard_from_store(
            &f
        ))),
        "the attestation binds to the signed bytes, not to the run id again"
    );

    // The other half of the asymmetry.
    let b = fixtures::orchestrator_fixture(Drill::BlocksAtPreflight);
    let sc = match execute_with(&b.args, &b.run_id, &b.ctx).unwrap_err() {
        logweir::drill::DrillError::NotPass(sc, _) => sc,
        other => panic!("{other:?}"),
    };
    assert!(
        !sc.phases.iter().any(|p| p.phase == 9),
        "a blocked preflight created nothing; it must not delete topics it did not make"
    );
    assert!(
        b.ctx
            .store
            .get(&format!("logweir/drills/{}.teardown.json", b.run_id))
            .is_err(),
        "no teardown ran, so no teardown may be attested"
    );
}

/// FIX 3. `PutReceipt.scorecard_key` must be the key `phase8_score::run`
/// actually put at, carried out on `Signed`, never a second reconstruction —
/// the same defect shape as Task 20's `retrieved_from` naming a prefix.
#[test]
fn the_receipt_names_the_key_the_scorecard_was_actually_put_at() {
    let f = fixtures::orchestrator_args_against_fixture_engine();
    execute_with(&f.args, &f.run_id, &f.ctx).unwrap();
    let r: logweir::drill::phase8_score::PutReceipt = serde_json::from_slice(
        &f.ctx
            .store
            .get(&format!("logweir/drills/{}.receipt.json", f.run_id))
            .unwrap()
            .0,
    )
    .unwrap();
    // The key must name an object that EXISTS and holds the bytes the receipt
    // is bound to — which is the whole property a reconstructed key cannot
    // guarantee.
    let at_key = f.ctx.store.get(&r.scorecard_key).unwrap_or_else(|e| {
        panic!(
            "the receipt names `{}`, which holds nothing: {e}",
            r.scorecard_key
        )
    });
    assert_eq!(
        r.scorecard_sha256,
        logweir_core::ids::sha256_prefixed(&at_key.0),
        "the key and the digest must name the same object"
    );
}

/// REGRESSION (Task 21c, found the first time the orchestrator was pointed at a
/// real engine). `build_plan` puts the restore checkpoint at
/// `temp_dir()/logweir-<run_id>/checkpoint.json`, and `drill::context` creates a
/// DIFFERENT directory (`logweir-<pid>`, for the rendered restore.yaml).
/// Nothing created the checkpoint's parent, and the engine does not create it
/// either, so `kafka-backup restore` exited 1 with a bare
/// `IO error: No such file or directory (os error 2)` on every run against the
/// real binary — on any host, CI included. Every existing test passed, because
/// every engine in this suite is a double that never opens the path.
///
/// This asserts the directory the plan names actually exists once the drill has
/// run, which is the only thing the real engine needs from it.
#[test]
fn the_restore_checkpoint_directory_exists_after_a_drill_runs() {
    let f = fixtures::orchestrator_args_against_fixture_engine();
    let dir = std::env::temp_dir().join(format!("logweir-{}", f.run_id));
    let _ = std::fs::remove_dir_all(&dir);
    assert!(!dir.exists(), "the fixture must start without it");
    execute_with(&f.args, &f.run_id, &f.ctx).expect("fixture drill passes");
    assert!(
        dir.is_dir(),
        "{} was never created, so the engine's `restore.checkpoint_state` has no \
         parent directory to write into",
        dir.display()
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// FIX 3 (Task 21c fix round 2). `logweir_core::scorecard::SampleInfo.anchor`
/// is a `String`, not `Anchor`: Global Constraint 12 freezes `format_version` at
/// 1.0.0 and retyping a published field is not an optional addition. Only
/// `Anchor::as_str()` writes it today, so it cannot drift in practice — but
/// "cannot drift in practice" is an argument, not a check. This is the check.
///
/// It reads the SIGNED bytes, not the in-memory struct, because the signed
/// document is the thing an auditor validates against the published schema.
#[test]
fn the_signed_scorecards_sample_anchor_is_always_one_of_the_closed_set() {
    let f = fixtures::orchestrator_args_against_fixture_engine();
    execute_with(&f.args, &f.run_id, &f.ctx).expect("fixture drill passes");
    let signed: serde_json::Value = serde_json::from_slice(&scorecard_from_store(&f)).unwrap();
    let anchor = signed["sample"]["anchor"]
        .as_str()
        .expect("sample.anchor is a JSON string");

    // Round-tripping THROUGH the enum is the assertion: a value outside the
    // closed set does not deserialize, whatever it is.
    let parsed: logweir_core::spec::Anchor = serde_json::from_value(anchor.into())
        .unwrap_or_else(|e| panic!("sample.anchor `{anchor}` is outside the closed set: {e}"));
    assert_eq!(
        parsed.as_str(),
        anchor,
        "the wire spelling must survive the round trip unchanged"
    );
    // Belt: the set itself, spelled out, so a variant added without thinking
    // about the signed format has to come through here.
    assert!(
        ["head", "tail", "random"].contains(&anchor),
        "sample.anchor `{anchor}` is not one of head|tail|random"
    );
}

// ===========================================================================
// D3 §2.4 — the teardown attestation key, on the scorecard's phase-9 record
// ===========================================================================

/// The `teardown-key=` line is driven by a phase-9 NOTE, and the note is the
/// only place a completed run remembers that the attestation was put.
///
/// It lives on `PhaseRecord.notes` for `failed_count`'s three reasons plus one
/// that matters here: the note is pushed AFTER phase 8 froze and signed the
/// scorecard bytes, so it can be read on the exit-2 path too — which is the
/// path that matters for D3 §4.4, where a rehearsal that did not pass is
/// exactly the run whose leftover topics block the next slot.
#[test]
fn the_teardown_key_is_read_off_the_phase_nine_record_and_only_when_it_is_there() {
    use logweir::drill::phase9_teardown::{attested_key, attested_note, teardown_key};

    let mut sc = fixtures::scorecard_pass();
    sc.phases.clear();
    assert_eq!(
        attested_key(&sc),
        None,
        "a run with no phase-9 record has no attestation to name"
    );

    let ok: Result<(), DrillError> = record(&mut sc, 9, "teardown", || Ok(()));
    ok.expect("phase 9 records");
    assert_eq!(
        attested_key(&sc),
        None,
        "phase 9 RAN and the put failed: there is still no key to name, which is the \
         `teardown-key=` mutant — a line naming an object nothing was written to"
    );

    let run_id = sc.run_id.clone();
    sc.phases
        .iter_mut()
        .find(|p| p.phase == 9)
        .expect("the phase-9 record")
        .notes = vec![attested_note(&run_id)];
    assert_eq!(
        attested_key(&sc).as_deref(),
        Some(teardown_key(&run_id).as_str()),
        "the announced key is the key `persist_with_signer` put at, from one function"
    );
    assert!(teardown_key(&run_id).ends_with(".teardown.json"));
}

/// The attested note must not disturb the failure notes beside it: both are
/// read by prefix out of the same `Vec<String>`, and `failed_count` feeds the
/// metric and the summary line.
#[test]
fn the_attested_note_does_not_disturb_the_teardown_failure_notes() {
    use logweir::drill::phase9_teardown::{
        attested_key, attested_note, failed_count, failed_topic_names_from_notes,
    };

    let mut sc = fixtures::scorecard_pass();
    sc.phases.clear();
    let ok: Result<(), DrillError> = record(&mut sc, 9, "teardown", || Ok(()));
    ok.expect("phase 9 records");
    let run_id = sc.run_id.clone();
    sc.phases
        .iter_mut()
        .find(|p| p.phase == 9)
        .expect("the phase-9 record")
        .notes = vec![
        "teardown-failed: drill-orders: BROKER SAID NO".to_string(),
        attested_note(&run_id),
    ];
    assert_eq!(failed_count(&sc), 1, "the failed count still counts one");
    assert_eq!(
        failed_topic_names_from_notes(&sc),
        vec!["drill-orders".to_string()]
    );
    assert!(attested_key(&sc).is_some());
}

/// **FX-4, end to end through the phase sequence.** The coverage a VERIFIED
/// point binding established reaches phase 7 through `Ctx` and becomes the
/// signed document's `topic_parity.not_assessed`: `captured` for the one
/// mapped topic yields `Some([])` — every topic assessed — and the SAME run
/// with the fixture's unknown coverage names the topic (the test above).
///
/// Negative control: a `run` that ignored `c.source_config_coverage` (passed
/// `SourceConfigCoverage::unknown()`) signs `configuration (unknown)` here and
/// this test fails.
#[test]
fn a_captured_source_configuration_reaches_the_signed_document_as_assessed() {
    let mut f = fixtures::orchestrator_args_against_fixture_engine();
    let mut receipt: logweir_core::backup_receipt::BackupReceipt = serde_json::from_slice(
        &std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../e2e/fixtures/signed/backup-receipt.json"),
        )
        .unwrap(),
    )
    .unwrap();
    receipt.format_version = "1.1.0".into();
    receipt.config_coverage = Some(std::collections::BTreeMap::from([(
        "orders".to_string(),
        logweir_core::backup_receipt::TopicConfigCoverage {
            coverage: "captured".into(),
            reason: None,
            timestamp_type: None,
        },
    )]));
    f.ctx.source_config_coverage =
        logweir_core::backup_receipt::SourceConfigCoverage::from_receipt(&receipt);
    execute_with(&f.args, &f.run_id, &f.ctx).unwrap();
    let sc: logweir_core::scorecard::Scorecard =
        serde_json::from_slice(&scorecard_from_store(&f)).unwrap();
    assert_eq!(sc.topic_parity.not_assessed, Some(vec![]));
}

// ------------------------------------------------------------------ FX-8
//
// The time basis, end to end through the phase sequence: the refusal of a
// selection by producer time over a `LogAppendTime` source, before any target
// topic exists, and the label the opt-in signs.

/// A receipt coverage recording `orders`' EFFECTIVE `message.timestamp.type`
/// as `value` from `source` — what FX-4's backup records and a verified point
/// binding hands the drill. The broker-default arm's only record.
fn coverage_recording(
    value: &str,
    source: &str,
) -> logweir_core::backup_receipt::SourceConfigCoverage {
    let mut receipt: logweir_core::backup_receipt::BackupReceipt = serde_json::from_slice(
        &std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../e2e/fixtures/signed/backup-receipt.json"),
        )
        .unwrap(),
    )
    .unwrap();
    receipt.format_version = "1.1.0".into();
    receipt.config_coverage = Some(std::collections::BTreeMap::from([(
        "orders".to_string(),
        logweir_core::backup_receipt::TopicConfigCoverage {
            coverage: "captured".into(),
            reason: None,
            timestamp_type: Some(logweir_core::backup_receipt::EffectiveConfigValue {
                value: value.into(),
                source: source.into(),
            }),
        },
    )]));
    logweir_core::backup_receipt::SourceConfigCoverage::from_receipt(&receipt)
}

/// The refusal a run returned, as the message the runner prints, with its exit
/// code and the `refusal-reason=` line a controller reads.
fn refusal(err: DrillError) -> (String, ExitCode, String) {
    let message = match &err {
        DrillError::Guard(g) => g.0.clone(),
        other => panic!("expected a guard refusal, got {other:?}"),
    };
    let line = logweir_core::guard::refusal_reason_line(&message);
    (message, ExitCode::from(err), line)
}

/// **FX-8, the topic-override arm — and WHERE it refuses.** The archive
/// manifest records `orders` as `LogAppendTime` and the plan states a point in
/// time with no `restore.time_basis`: exit 3, `refusal-reason=
/// PointInTimeByProducerTime`, naming the topic and the record — and NO target
/// topic was created, NO scorecard was written, the engine never fingerprinted.
///
/// KILLS: deleting the manifest arm (the run passes); moving the decision after
/// the target-creation step (`created_topics` is not empty); a refusal that
/// does not name its terminal state (`GuardRefused` on the line).
#[test]
fn the_time_basis_refusal_creates_no_target_topic() {
    let f = fixtures::orchestrator_fixture(Drill::SelectsALogAppendTimeTopicAtAPoint);
    let err = execute_with(&f.args, &f.run_id, &f.ctx).expect_err("refused");
    let (message, code, line) = refusal(err);
    assert_eq!(code, ExitCode::GuardRefused);
    assert_eq!(line, "refusal-reason=PointInTimeByProducerTime");
    assert!(
        message.contains(
            "`orders` (the archive manifest's topic override message.timestamp.type=LogAppendTime)"
        ),
        "{message}"
    );
    assert!(
        message.contains("restore.time_basis: producerTime"),
        "{message}"
    );
    assert!(
        fixtures::created_topics(&f).is_empty(),
        "a refused time basis created a target topic: {:?}",
        fixtures::created_topics(&f)
    );
    assert!(
        fixtures::fingerprint_calls(&f).is_empty(),
        "the refusal comes before phase 4"
    );
    assert!(
        f.ctx
            .store
            .get(&format!("logweir/drills/{}.json", f.run_id))
            .is_err(),
        "a refused run signs nothing"
    );
}

/// **FX-8, the opt-in.** The same archive and point with `restore.time_basis:
/// producerTime` in the approved plan: the run passes as `Passes` does, the
/// target topic IS created, and the signed scorecard — format 1.3.0 — lists
/// `orders` under `source.time_basis.producer_time` with the plan's value.
///
/// KILLS: a writer that drops the label (`source.time_basis` absent or its
/// list empty); an opt-in that is not read (the run is refused).
#[test]
fn the_producer_time_opt_in_runs_and_the_signed_scorecard_says_so() {
    let f = fixtures::orchestrator_fixture(Drill::SelectsALogAppendTimeTopicByProducerTime);
    execute_with(&f.args, &f.run_id, &f.ctx).expect("the opt-in runs");
    assert_eq!(fixtures::created_topics(&f).len(), 1);
    let sc: logweir_core::scorecard::Scorecard =
        serde_json::from_slice(&scorecard_from_store(&f)).unwrap();
    assert_eq!(sc.outcome, Outcome::Pass);
    // FX-23: every SAMPLED document is 1.6.0, so the version marks a build
    // with FX-23's checks; a complete one stays 1.4.0 (below).
    assert_eq!(
        sc.format_version,
        logweir_core::scorecard::FORMAT_VERSION_WITH_UNSAMPLED_TOPICS
    );
    assert_eq!(
        sc.source.time_basis,
        Some(logweir_core::scorecard::TimeBasisLabel {
            plan: Some("producerTime".into()),
            producer_time: vec!["orders".into()],
            not_recorded: vec![],
        })
    );
    assert!(sc.validate_invariants().is_ok());
}

/// **FX-8: the opt-in is inside the approved bytes.** An approval minted over
/// the plan WITHOUT `restore.time_basis` does not authorise the same plan WITH
/// it: phase 1 refuses before the archive is opened, and nothing is created.
///
/// KILLS: an opt-in read from anywhere but the hashed plan bytes, or a plan
/// hash computed over bytes with the field taken out.
#[test]
fn an_approval_without_the_opt_in_does_not_authorise_it() {
    let without = fixtures::orchestrator_fixture(Drill::SelectsALogAppendTimeTopicAtAPoint);
    let mut with = fixtures::orchestrator_fixture(Drill::SelectsALogAppendTimeTopicByProducerTime);
    // Same signer and approver key (both fixtures copy the one checked-in
    // key); only the plan the approval names differs.
    with.args.approval = without.args.approval.clone();
    let err = execute_with(&with.args, &with.run_id, &with.ctx)
        .expect_err("an approval over other bytes");
    let (message, code, _) = refusal(err);
    assert_eq!(code, ExitCode::GuardRefused);
    assert!(
        !message.starts_with("PointInTimeByProducerTime"),
        "refused at phase 1, not at the time basis: {message}"
    );
    assert!(fixtures::created_topics(&with).is_empty());
    // The control: the same approval over its own plan reaches the time basis.
    let err = execute_with(&without.args, &without.run_id, &without.ctx).expect_err("refused");
    assert_eq!(refusal(err).2, "refusal-reason=PointInTimeByProducerTime");
}

/// **FX-8, the broker-default arm.** No manifest override; the VERIFIED
/// receipt's coverage recorded `orders`' effective type `LogAppendTime` from
/// the broker's dynamic default. Refused the same way, naming that record,
/// with nothing created.
///
/// KILLS: reading only the manifest override (the run passes, labelled
/// `not_recorded`); ignoring `c.source_config_coverage`.
#[test]
fn a_broker_default_log_append_time_is_refused_from_the_receipts_record() {
    let mut f = fixtures::orchestrator_fixture(Drill::SelectsAtAPoint);
    f.ctx.source_config_coverage =
        coverage_recording("LogAppendTime", "dynamicDefaultBrokerConfig");
    let err = execute_with(&f.args, &f.run_id, &f.ctx).expect_err("refused");
    let (message, code, line) = refusal(err);
    assert_eq!(code, ExitCode::GuardRefused);
    assert_eq!(line, "refusal-reason=PointInTimeByProducerTime");
    assert!(
        message.contains(
            "`orders` (the bound backup receipt's effective message.timestamp.type LogAppendTime \
             from dynamicDefaultBrokerConfig)"
        ),
        "{message}"
    );
    assert!(fixtures::created_topics(&f).is_empty());
}

/// **FX-8, the unknown case and the `CreateTime` control**, over the same
/// point-in-time plan. Nothing records the type (an unbound plan, or a receipt
/// before FX-4): the run PASSES and signs `not_recorded: [orders]` — never
/// `CreateTime` by silence. The receipt recorded `CreateTime`: the run passes
/// and lists `orders` nowhere.
///
/// KILLS: refusing the unknown case; reading silence as `CreateTime` (the
/// unknown run signs empty lists); refusing a `CreateTime` topic.
#[test]
fn an_unrecorded_type_is_labelled_and_a_create_time_topic_is_not_refused() {
    let f = fixtures::orchestrator_fixture(Drill::SelectsAtAPoint);
    execute_with(&f.args, &f.run_id, &f.ctx).expect("the unknown case runs");
    let sc: logweir_core::scorecard::Scorecard =
        serde_json::from_slice(&scorecard_from_store(&f)).unwrap();
    assert_eq!(sc.outcome, Outcome::Pass);
    assert_eq!(
        sc.source.time_basis,
        Some(logweir_core::scorecard::TimeBasisLabel {
            plan: None,
            producer_time: vec![],
            not_recorded: vec!["orders".into()],
        })
    );

    let mut f = fixtures::orchestrator_fixture(Drill::SelectsAtAPoint);
    f.ctx.source_config_coverage = coverage_recording("CreateTime", "defaultConfig");
    execute_with(&f.args, &f.run_id, &f.ctx).expect("a CreateTime topic runs");
    let sc: logweir_core::scorecard::Scorecard =
        serde_json::from_slice(&scorecard_from_store(&f)).unwrap();
    assert_eq!(
        sc.source.time_basis,
        Some(logweir_core::scorecard::TimeBasisLabel::default())
    );
}

// ------------------------------------------------------------------ FX-16
//
// The restored set is the bound point's, or nothing is created: a plan that
// binds point A must not restore set B and then take A's recorded word —
// configuration coverage, timestamp types, the manifest pin — for B's records.

/// The set the fixture engine lists and describes, exactly as a VERIFIED point
/// binding records it (`binding::VerifiedPoint::set`) when its receipt
/// describes that set.
fn bound_as_restored(f: &fixtures::OrchestratorFixture) -> logweir::drill::binding::BoundSet {
    let sets = f
        .ctx
        .engine
        .list_backup_sets(&f.ctx.spec.source.storage)
        .expect("the fixture lists its set");
    let set = sets.last().expect("one set").clone();
    let facts = f
        .ctx
        .engine
        .describe(&set)
        .expect("the fixture describes it");
    logweir::drill::binding::BoundSet {
        point_id: "lwp1-0123456789abcdef0123456789abcdef".into(),
        backup_id: set.backup_id,
        manifest_key: set.manifest_key,
        manifest_sha256: facts.manifest_sha256,
        manifest_version_id: facts.manifest_version_id,
    }
}

/// Asserts the run was refused `PointBindingSetMismatch` (exit 3, the general
/// `GuardRefused` line, as every binding refusal) naming `needle`, and that it
/// was refused BEFORE phase 2: no target topic created, the engine never asked
/// for a fingerprint, no scorecard signed.
fn refused_set_mismatch(f: &fixtures::OrchestratorFixture, err: DrillError, needle: &str) {
    let (message, code, line) = refusal(err);
    assert_eq!(code, ExitCode::GuardRefused);
    assert_eq!(line, "refusal-reason=GuardRefused");
    assert!(
        message.starts_with("PointBindingSetMismatch. "),
        "{message}"
    );
    assert!(message.contains(needle), "{needle:?} not in: {message}");
    assert!(
        fixtures::created_topics(f).is_empty(),
        "a refused set created a target topic: {:?}",
        fixtures::created_topics(f)
    );
    assert!(fixtures::fingerprint_calls(f).is_empty(), "before phase 4");
    assert!(
        f.ctx
            .store
            .get(&format!("logweir/drills/{}.json", f.run_id))
            .is_err(),
        "a refused run signs nothing"
    );
}

/// **FX-16 — a plan binding point A while restoring set B is refused before
/// any target exists.** The fixture engine restores its one set; the verified
/// binding (as `Ctx::bound_set`) names that set's key, so it is the set
/// selected, and describes it with ONE difference per run: another set id
/// (what `latestCompleted` resolving to a newer set looks like), another
/// manifest digest (other bytes at that key), another version (the manifest
/// written again between the binding and `describe`). Each is exit 3 with
/// nothing created. Another manifest KEY never reaches this check since the
/// fix round: the set is selected by the point's key (the row below).
///
/// KILLS: removing the check (all three runs pass and create the topic);
/// moving it after the target-creation step (`created_topics` is not empty);
/// ignoring the set id, the digest or the version (that run passes).
#[test]
fn a_restored_set_that_is_not_the_bound_points_creates_no_target_topic() {
    type Edit = fn(&mut logweir::drill::binding::BoundSet);
    let rows: [(&str, Edit, &str); 3] = [
        (
            "another set id",
            |b| b.backup_id = "backup-2026-08-31T02:00:00Z".into(),
            "it is backup set `backup-2026-08-30T02:00:00Z`, not the receipt's \
             `backup-2026-08-31T02:00:00Z`",
        ),
        (
            "another manifest digest",
            |b| b.manifest_sha256 = format!("sha256:{}", "b".repeat(64)),
            "not the bound sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        ),
        (
            "another manifest version",
            |b| b.manifest_version_id = Some("3HL4kqtJlcpXroDTDmJ".into()),
            "its manifest read answered no version id, and the binding's read answered version \
             3HL4kqtJlcpXroDTDmJ",
        ),
    ];
    for (row, edit, needle) in rows {
        let mut f = fixtures::orchestrator_fixture(Drill::Passes);
        let mut bound = bound_as_restored(&f);
        edit(&mut bound);
        f.ctx.bound_set = Some(bound);
        let err = execute_with(&f.args, &f.run_id, &f.ctx)
            .expect_err(&format!("{row}: a set that is not the point's is refused"));
        refused_set_mismatch(&f, err, needle);
    }
}

/// **FX-16 fix round (review M-1): a bound plan's set is selected by the
/// point's key.** A point whose manifest key the archive's listing does not
/// show (the binding read it there moments earlier) is exit 1, the archive
/// answering inconsistently — never a substitution of another set by id. No
/// target topic, no fingerprint, no scorecard.
///
/// KILLS: selecting a bound plan's set by id again (the fixture's one set is
/// chosen, and the run is refused 3 on the key, or — without the key
/// comparison — restores it).
#[test]
fn a_bound_key_the_listing_does_not_show_is_operational_and_creates_nothing() {
    let mut f = fixtures::orchestrator_fixture(Drill::Passes);
    let mut bound = bound_as_restored(&f);
    bound.manifest_key = "copy/fixture/manifest.json".into();
    f.ctx.bound_set = Some(bound);
    let err = execute_with(&f.args, &f.run_id, &f.ctx).expect_err("not selected");
    let message = match &err {
        DrillError::Operational(m) => m.clone(),
        other => panic!("expected exit 1, got {other:?}"),
    };
    assert_eq!(ExitCode::from(err), ExitCode::Operational);
    assert!(
        message.contains("copy/fixture/manifest.json")
            && message.contains("refusing to choose another set by id"),
        "{message}"
    );
    assert!(fixtures::created_topics(&f).is_empty());
    assert!(fixtures::fingerprint_calls(&f).is_empty());
    assert!(f
        .ctx
        .store
        .get(&format!("logweir/drills/{}.json", f.run_id))
        .is_err());
}

/// The fixture engine, with a same-id COPY of its set listed FIRST (a key
/// that sorts before the set's own, as `0copy/…` does), recording every key
/// `describe` is asked about. Everything else is the fixture's.
struct ListsACopyFirst {
    inner: Box<dyn logweir_core::engine::DataEngine>,
    copy_key: String,
    described: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

/// A placeholder while the fixture's engine is moved into the wrapper.
struct NoEngine;
impl logweir_core::engine::DataEngine for NoEngine {
    fn id(&self) -> logweir_core::engine::EngineId {
        unreachable!()
    }
    fn list_backup_sets(
        &self,
        _: &logweir_core::engine::StorageUrl,
    ) -> Result<Vec<logweir_core::engine::BackupSetRef>, logweir_core::engine::EngineError> {
        unreachable!()
    }
    fn describe(
        &self,
        _: &logweir_core::engine::BackupSetRef,
    ) -> Result<logweir_core::engine::BackupSetFacts, logweir_core::engine::EngineError> {
        unreachable!()
    }
    fn preflight(
        &self,
        _: &logweir_core::engine::RestorePlan,
    ) -> Result<logweir_core::engine::PreflightReport, logweir_core::engine::EngineError> {
        unreachable!()
    }
    fn restore(
        &self,
        _: &logweir_core::engine::RestorePlan,
        _: &mut dyn logweir_core::engine::PhaseObserver,
    ) -> Result<logweir_core::engine::RestoreFacts, logweir_core::engine::EngineError> {
        unreachable!()
    }
    fn fingerprints(
        &self,
        _: &logweir_core::engine::SampleSelection,
    ) -> Result<Vec<logweir_core::engine::RecordFingerprint>, logweir_core::engine::EngineError>
    {
        unreachable!()
    }
}

impl logweir_core::engine::DataEngine for ListsACopyFirst {
    fn id(&self) -> logweir_core::engine::EngineId {
        self.inner.id()
    }
    fn list_backup_sets(
        &self,
        loc: &logweir_core::engine::StorageUrl,
    ) -> Result<Vec<logweir_core::engine::BackupSetRef>, logweir_core::engine::EngineError> {
        let mut sets = self.inner.list_backup_sets(loc)?;
        let copy = logweir_core::engine::BackupSetRef {
            backup_id: sets[0].backup_id.clone(),
            manifest_key: self.copy_key.clone(),
        };
        sets.insert(0, copy);
        Ok(sets)
    }
    fn describe(
        &self,
        set: &logweir_core::engine::BackupSetRef,
    ) -> Result<logweir_core::engine::BackupSetFacts, logweir_core::engine::EngineError> {
        self.described
            .lock()
            .unwrap()
            .push(set.manifest_key.clone());
        self.inner.describe(set)
    }
    fn describe_with_notices(
        &self,
        set: &logweir_core::engine::BackupSetRef,
    ) -> Result<
        (
            logweir_core::engine::BackupSetFacts,
            Vec<logweir_core::engine::ArchiveNotice>,
        ),
        logweir_core::engine::EngineError,
    > {
        self.described
            .lock()
            .unwrap()
            .push(set.manifest_key.clone());
        self.inner.describe_with_notices(set)
    }
    fn preflight(
        &self,
        plan: &logweir_core::engine::RestorePlan,
    ) -> Result<logweir_core::engine::PreflightReport, logweir_core::engine::EngineError> {
        self.inner.preflight(plan)
    }
    fn restore(
        &self,
        plan: &logweir_core::engine::RestorePlan,
        obs: &mut dyn logweir_core::engine::PhaseObserver,
    ) -> Result<logweir_core::engine::RestoreFacts, logweir_core::engine::EngineError> {
        self.inner.restore(plan, obs)
    }
    fn fingerprints(
        &self,
        sel: &logweir_core::engine::SampleSelection,
    ) -> Result<Vec<logweir_core::engine::RecordFingerprint>, logweir_core::engine::EngineError>
    {
        self.inner.fingerprints(sel)
    }
    fn validation_run(
        &self,
        plan: &logweir_core::engine::RestorePlan,
    ) -> Result<logweir_core::engine::EngineRun, logweir_core::engine::EngineError> {
        self.inner.validation_run(plan)
    }
}

/// **FX-16 fix round (review L-1): a same-id copy that sorts first does not
/// refuse a truthful plan.** The listing shows a copy of the point's set
/// (`0copy/…`) before the set itself; the plan is bound to the set itself.
/// The run selects and describes the set at the point's key, ONLY that key,
/// and passes as `Passes` does. Before the fix round the copy was selected by
/// id and the run refused `PointBindingSetMismatch` on its key.
///
/// KILLS: selecting a bound plan's set by id (first) again.
#[test]
fn a_same_id_copy_listed_first_does_not_refuse_a_truthful_plan() {
    let mut f = fixtures::orchestrator_fixture(Drill::Passes);
    let bound = bound_as_restored(&f);
    let described = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let inner = std::mem::replace(&mut f.ctx.engine, Box::new(NoEngine));
    f.ctx.engine = Box::new(ListsACopyFirst {
        inner,
        copy_key: "0copy/fixture/manifest.json".into(),
        described: described.clone(),
    });
    let listed = f
        .ctx
        .engine
        .list_backup_sets(&f.ctx.spec.source.storage)
        .unwrap();
    assert_eq!(listed[0].manifest_key, "0copy/fixture/manifest.json");
    assert_eq!(
        listed[0].backup_id, bound.backup_id,
        "the copy carries the id"
    );
    let bound_key = bound.manifest_key.clone();
    f.ctx.bound_set = Some(bound);
    execute_with(&f.args, &f.run_id, &f.ctx).expect("the truthful plan restores its set");
    assert_eq!(*described.lock().unwrap(), vec![bound_key]);
    assert_eq!(fixtures::created_topics(&f).len(), 1);
    let sc: logweir_core::scorecard::Scorecard =
        serde_json::from_slice(&scorecard_from_store(&f)).unwrap();
    assert_eq!(sc.outcome, Outcome::Pass);
}

/// **FX-16, the control.** The same run with the binding describing exactly
/// the set the engine restores passes as `Passes` does, and creates its one
/// target topic — so the row above is not a build that refuses every bound
/// run.
#[test]
fn a_restored_set_that_is_the_bound_points_runs() {
    let mut f = fixtures::orchestrator_fixture(Drill::Passes);
    f.ctx.bound_set = Some(bound_as_restored(&f));
    execute_with(&f.args, &f.run_id, &f.ctx).expect("the bound set restores");
    assert_eq!(fixtures::created_topics(&f).len(), 1);
    let sc: logweir_core::scorecard::Scorecard =
        serde_json::from_slice(&scorecard_from_store(&f)).unwrap();
    assert_eq!(sc.outcome, Outcome::Pass);
}

/// **FX-16 and FX-8: the receipt's word never reaches another set.** The
/// bound receipt records `orders` as `LogAppendTime` from the broker default
/// (FX-8's broker-default arm, decided from the RECEIPT alone). Bound to a
/// receipt of ANOTHER set, the run is refused `PointBindingSetMismatch`, not
/// `PointInTimeByProducerTime`: the set check comes first, so the time basis
/// is never decided from a record about other records. The control — the
/// same receipt describing the restored set — reaches the time basis and is
/// refused there, as FX-8's row is.
///
/// KILLS: moving the set check after `time_basis::decide` (the first run is
/// refused `PointInTimeByProducerTime`).
#[test]
fn a_restored_set_mismatch_is_refused_before_the_time_basis_reads_the_receipt() {
    let mut f = fixtures::orchestrator_fixture(Drill::SelectsAtAPoint);
    f.ctx.source_config_coverage =
        coverage_recording("LogAppendTime", "dynamicDefaultBrokerConfig");
    let mut other = bound_as_restored(&f);
    other.backup_id = "backup-2026-08-31T02:00:00Z".into();
    f.ctx.bound_set = Some(other);
    let err = execute_with(&f.args, &f.run_id, &f.ctx).expect_err("refused");
    refused_set_mismatch(&f, err, "not the receipt's `backup-2026-08-31T02:00:00Z`");

    let mut f = fixtures::orchestrator_fixture(Drill::SelectsAtAPoint);
    f.ctx.source_config_coverage =
        coverage_recording("LogAppendTime", "dynamicDefaultBrokerConfig");
    f.ctx.bound_set = Some(bound_as_restored(&f));
    let err = execute_with(&f.args, &f.run_id, &f.ctx).expect_err("refused");
    assert_eq!(
        refusal(err).2,
        "refusal-reason=PointInTimeByProducerTime",
        "the receipt's own set: FX-8 decides from its record"
    );
    assert!(fixtures::created_topics(&f).is_empty());
}

// ------------------------------------------------------------------ PROD-08.1

/// **PROD-08.1, the wiring.** A plan stating `sample.coverage: complete`
/// reaches phase 7's complete lane and the SIGNED document says so: coverage
/// complete, header order verified, the block covered and exact over all 500
/// records — the whole window, not the 25-record canary — `sample` holding
/// the complete canary (500 expected, one partition), and the archive's
/// fingerprints never asked for (the complete lane decodes the archive
/// itself). The signed bytes satisfy every invariant, IV-1..IV-7 included.
///
/// KILLS: the orchestrator not passing the plan's coverage to phase 7 (the
/// block says sampled); `complete_sample_info` not run (records_sampled 500
/// exceeds records_expected 25, refused at signing).
#[test]
fn a_complete_coverage_plan_signs_a_covered_exact_complete_block() {
    let f = fixtures::orchestrator_fixture(Drill::VerifiesCompletely);
    execute_with(&f.args, &f.run_id, &f.ctx)
        .expect("a complete verification of a correct restore passes");
    let sc: logweir_core::scorecard::Scorecard =
        serde_json::from_slice(&scorecard_from_store(&f)).unwrap();
    assert_eq!(sc.outcome, Outcome::Pass);
    // FX-23: a complete verification keeps the version it always had; only a
    // sampled one is 1.6.0.
    assert_eq!(sc.format_version, logweir_core::FORMAT_VERSION);
    let v = sc
        .integrity
        .verification
        .as_ref()
        .expect("the block is signed");
    assert_eq!(
        (v.coverage.as_str(), v.header_order.as_str()),
        ("complete", "verified")
    );
    let c = v.complete.as_ref().expect("the complete block");
    assert!(c.covered && c.replay.is_exact(), "{c:?}");
    assert_eq!(c.replay.expected, fixtures::FIXTURE_WINDOW_RECORDS as u64);
    assert_eq!(
        (
            c.archive.segments,
            c.archive.segments_verified,
            c.archive.records_decoded
        ),
        (1, 1, 500)
    );
    assert_eq!(
        (
            sc.sample.records_expected,
            sc.sample.partitions,
            sc.sample.topics
        ),
        (500, 1, 1)
    );
    assert_eq!(
        (
            sc.integrity.records_sampled,
            sc.integrity.records_sampled_matching
        ),
        (500, 500)
    );
    assert!(
        fixtures::fingerprint_calls(&f).is_empty(),
        "the complete lane reads the archive itself, never the engine's sampled fingerprints"
    );
    assert_eq!(sc.validate_invariants().map_err(|e| e.0), Ok(()));
}

/// **PROD-08.1.** The same plan, with one restored record past the canary
/// changed on the target: the complete lane finds it, the drill scores
/// `fail-integrity` (exit 2 with a signed document), and the signed block
/// counts exactly one different record. A sampled drill over the same target
/// reads the first 25 records only and cannot see offset 300.
///
/// KILLS: a complete lane that compares only the canary; an ordered-digest
/// comparison replaced by a count.
#[test]
fn a_complete_coverage_plan_finds_a_changed_record_past_the_canary() {
    let f = fixtures::orchestrator_fixture(Drill::VerifiesCompletelyAndFindsAChangedRecord);
    let err = execute_with(&f.args, &f.run_id, &f.ctx).expect_err("a changed record fails");
    assert!(matches!(err, DrillError::NotPass(..)), "{err:?}");
    let sc: logweir_core::scorecard::Scorecard =
        serde_json::from_slice(&scorecard_from_store(&f)).unwrap();
    assert_eq!(sc.outcome, Outcome::FailIntegrity);
    let c = sc
        .integrity
        .verification
        .as_ref()
        .and_then(|v| v.complete.as_ref())
        .expect("the complete block");
    assert_eq!((c.replay.mismatched, c.replay.matching), (1, 499));
    assert!(
        c.partitions[0]
            .findings
            .iter()
            .any(|l| l.contains("source offset 300 at target offset 300 differs from the archive")),
        "{:?}",
        c.partitions[0].findings
    );
    assert_eq!(sc.validate_invariants().map_err(|e| e.0), Ok(()));
}

/// **PROD-11.1, fail closed while the selection is not executable end to
/// end** (the orchestrator's binding note on the WIP, 2026-10-08). A plan that
/// states `restore.partitions` — one the archive satisfies, so the refusal is
/// not the archive's — is refused exit 3, naming `SelectionNotYetExecutable`,
/// before phase 2: no target topic created, no engine started, no sample
/// fingerprinted, nothing signed. The control is the same archive with no
/// selection (`Drill::Passes`), which runs and signs.
///
/// KILLS: deleting the refusal (the run would restore and sign a narrowed
/// selection judged over the whole archive), moving it after the target-topic
/// creation step or after phase 4.
#[test]
fn a_stated_selection_is_refused_before_phase_2_until_it_is_executable() {
    let f = fixtures::orchestrator_fixture(Drill::StatesAPartitionSelection);
    let err = execute_with(&f.args, &f.run_id, &f.ctx).expect_err("refused");
    let (message, code, line) = refusal(err);
    assert_eq!(code, ExitCode::GuardRefused);
    assert_eq!(line, "refusal-reason=GuardRefused");
    assert!(
        message.starts_with("SelectionNotYetExecutable: this plan states a replay selection"),
        "{message}"
    );
    assert!(
        fixtures::created_topics(&f).is_empty(),
        "a refused selection created a target topic: {:?}",
        fixtures::created_topics(&f)
    );
    assert!(fixtures::fingerprint_calls(&f).is_empty(), "before phase 4");
    assert!(
        f.ctx
            .store
            .get(&format!("logweir/drills/{}.json", f.run_id))
            .is_err(),
        "a refused run signs nothing"
    );

    let control = fixtures::orchestrator_fixture(Drill::Passes);
    execute_with(&control.args, &control.run_id, &control.ctx)
        .expect("the same archive without a selection runs");
    assert_eq!(fixtures::created_topics(&control).len(), 1);
}
