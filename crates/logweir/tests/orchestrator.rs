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
        ExitCode::from(DrillError::NotPass(Box::new(fixtures::scorecard_pass()))),
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

/// The phase-5 jump. `Verdict::Block` goes STRAIGHT to phase 8 — score, sign,
/// upload — and returns exit 2 with a signed artifact. Never phase 6, and
/// never exit 1 with nothing to show for it.
#[test]
fn a_blocked_preflight_exits_2_with_a_signed_scorecard_and_never_reaches_phase_6() {
    let f = fixtures::orchestrator_fixture(Drill::BlocksAtPreflight);
    let err = execute_with(&f.args, &f.run_id, &f.ctx).unwrap_err();
    let sc = match &err {
        logweir::drill::DrillError::NotPass(sc) => sc.clone(),
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
        logweir::drill::DrillError::NotPass(sc) => sc,
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
        logweir::drill::DrillError::NotPass(sc) => sc.clone(),
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
        logweir::drill::DrillError::NotPass(sc) => sc,
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
    assert_eq!(sc.target.marker_topic, fixtures::FIXTURE_MARKER_TOPIC);
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
    assert!(
        sc.topic_parity.unexpected_divergence.is_empty(),
        "{:?}",
        sc.topic_parity.unexpected_divergence
    );
    // 8 — the objectives, as REQUESTED plus the verdict
    assert_eq!(sc.objectives.rto_seconds, Some(900));
    assert_eq!(sc.objectives.met, Some(true));
    assert_eq!(sc.triggered_by.as_deref(), Some("fixture"));
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
        logweir::drill::DrillError::NotPass(sc) => *sc,
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
        logweir::drill::DrillError::NotPass(sc) => sc,
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
            logweir::drill::DrillError::NotPass(sc) => sc.clone(),
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
        logweir::drill::DrillError::NotPass(sc) => sc,
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
        logweir::drill::DrillError::NotPass(sc) => sc,
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
