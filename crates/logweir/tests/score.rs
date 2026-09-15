//! Phases 8 and 9 — score, sign, create-only upload (exit 4), teardown
//! attestation.
//!
//! Three properties carry the weight of this file, and each has at least one
//! test that fails at ASSERTION time (never merely at compile time) when the
//! property is inverted:
//!
//! 1. A signing failure is its own outcome — `ExitCode::SigningOrLock` (4),
//!    never `Operational` (1) — and nothing is uploaded.
//! 2. The upload is create-only: an existing key is REFUSED, not clobbered,
//!    and nothing is ever written outside `logweir/` or named `manifest.json`.
//! 3. The teardown attestation is honest: when teardown did not happen, the
//!    signed document says so instead of asserting a clean state.
mod fixtures;

use logweir::drill::phase8_score::{compute_measured, decide};
use logweir::drill::DrillError;
use logweir::exit::ExitCode;
use logweir_core::outcome::Outcome;
use logweir_kafka::reader::{KafkaError, TopicDeleter};

// ----------------------------------------------------------------- phase 8: measure

#[test]
fn the_four_rto_numbers_are_computed_from_scorecard_fields_only() {
    let t = fixtures::timeline(); // requested 09:00:00, approval_validated 09:00:30,
                                  // restore 09:02:00..09:05:34, phase5 duration 212s,
                                  // verified 09:09:02
    let m = compute_measured(
        &t,
        /*requested_pitr_ms*/ 1_756_519_200_000,
        /*newest_restored_ts_ms*/ 1_756_519_200_000,
    );
    assert_eq!(m.rto_seconds, Some(512)); // verified - approval_validated
    assert_eq!(m.rto_requested_to_verified_seconds, Some(542));
    assert_eq!(m.rto_restore_only_seconds, Some(214));
    assert_eq!(m.rto_excluding_preflight_seconds, Some(300)); // 512 - 212
    assert_eq!(m.rpo_seconds, Some(0));
    assert_eq!(m.rpo_source_relative_seconds, None);
    assert_eq!(
        m.rpo_source_relative_unmeasured_reason.as_deref(),
        Some("source cluster never contacted")
    );
}

/// The archive-coverage gap is `requested - newest_restored`, in that order.
/// Swapping the operands publishes a NEGATIVE gap in a signed document — a
/// headline auditor-facing number, inverted, which a reader would most likely
/// take to mean "no data loss".
#[test]
fn the_rpo_is_how_far_short_of_the_requested_point_the_newest_record_falls() {
    let pit = 1_756_519_200_000i64;
    let m = compute_measured(&fixtures::timeline(), pit, pit - 90_000);
    assert_eq!(
        m.rpo_seconds,
        Some(90),
        "the newest restored record is 90s BEFORE the requested recovery point, \
         so the archive-coverage gap is +90s"
    );
}

/// A gap of -90 seconds is not a smaller gap, it is a meaningless one. If the
/// newest restored record lands at or beyond the requested recovery point,
/// there is no coverage gap at that point.
#[test]
fn a_record_newer_than_the_requested_point_is_a_zero_gap_never_a_negative_one() {
    let pit = 1_756_519_200_000i64;
    let m = compute_measured(&fixtures::timeline(), pit, pit + 90_000);
    assert_eq!(m.rpo_seconds, Some(0));
    assert!(
        m.rpo_seconds.unwrap() >= 0,
        "rpo_seconds must never be negative in a signed document"
    );
}

/// The value compared against the objective is the one that excludes phase 5,
/// because header_preflight: full is a full read and decode of the window that
/// no incident responder performs.
#[test]
fn the_objective_is_compared_against_rto_excluding_preflight() {
    let m = fixtures::measured(/*rto*/ 950, /*excl*/ 300, /*rpo*/ 0);
    let (outcome, obj) = decide(
        &m,
        &fixtures::objectives(900, 300, 1.0),
        &fixtures::integrity_pass(),
    );
    assert_eq!(
        outcome,
        Outcome::Pass,
        "950 > 900 but 300 <= 900 — the excluding-preflight value wins"
    );
    assert_eq!(obj.met, Some(true));
}

#[test]
fn a_missed_rpo_is_fail_objective() {
    let m = fixtures::measured(400, 300, 900);
    let (outcome, obj) = decide(
        &m,
        &fixtures::objectives(900, 300, 1.0),
        &fixtures::integrity_pass(),
    );
    assert_eq!(outcome, Outcome::FailObjective);
    assert_eq!(obj.met, Some(false));
}

/// The pass-rate objective is ENFORCED, not decorative. `integrity_rate` keeps
/// `result: Pass` so `decide` reaches the objectives at all — with
/// `integrity_fail()` the run short-circuits to `FailIntegrity` and the rate
/// comparison is never consulted, which is exactly why no earlier test pinned
/// it.
#[test]
fn a_measured_pass_rate_below_the_objective_is_fail_objective() {
    let integ = fixtures::integrity_rate(8, 10);
    assert_eq!(
        integ.pass_rate_measured,
        Some(0.8),
        "the rate under test is PHASE 7's published field; `decide` reads it and \
         never recomputes one from the two counters"
    );
    let (outcome, obj) = decide(
        &fixtures::measured(1, 1, 0),
        &fixtures::objectives(900, 300, 1.0),
        &integ,
    );
    assert_eq!(
        obj.met,
        Some(false),
        "0.8 measured against a required 1.0 is a MISSED objective"
    );
    assert_eq!(outcome, Outcome::FailObjective);
}

/// The boundary: measured exactly equal to the requested rate is met.
#[test]
fn a_measured_pass_rate_exactly_at_the_objective_is_met() {
    let (outcome, obj) = decide(
        &fixtures::measured(1, 1, 0),
        &fixtures::objectives(900, 300, 0.8),
        &fixtures::integrity_rate(8, 10),
    );
    assert_eq!(obj.met, Some(true));
    assert_eq!(outcome, Outcome::Pass);
}

/// The `+ f64::EPSILON` tolerance in `decide` is load-bearing, not slop.
/// `3.0/10.0` is one ULP below the f64 nearest `0.1 + 0.2`, so a bare `>=`
/// would report a missed objective for a rate that is exactly what was asked
/// for. Deleting the tolerance turns this test red.
#[test]
fn a_pass_rate_one_ulp_low_from_float_representation_still_counts_as_met() {
    let want = 0.1f64 + 0.2f64; // 0.30000000000000004, one ULP above 0.3
    let got = 3.0f64 / 10.0f64; // 0.29999999999999999
    assert!(
        got < want,
        "the fixture must actually straddle the boundary"
    );
    let integ = fixtures::integrity_rate(3, 10);
    assert_eq!(integ.pass_rate_measured, Some(got));
    let (_, obj) = decide(
        &fixtures::measured(1, 1, 0),
        &fixtures::objectives(900, 300, want),
        &integ,
    );
    assert_eq!(
        obj.met,
        Some(true),
        "a one-ULP floating-point shortfall is a rounding artefact, not a missed objective"
    );
}

#[test]
fn a_mismatch_is_fail_integrity_regardless_of_the_objectives() {
    let (outcome, _) = decide(
        &fixtures::measured(1, 1, 0),
        &fixtures::objectives(900, 300, 1.0),
        &fixtures::integrity_fail(),
    );
    assert_eq!(outcome, Outcome::FailIntegrity);
}

#[test]
fn consume_only_keeps_the_requested_pass_rate_and_leaves_met_null() {
    let integ = fixtures::integrity_consume_only();
    assert_eq!(
        integ.pass_rate_measured, None,
        "the MEASURED rate is null — it lives in integrity.pass_rate_measured, set by \
         phase 7 and by nobody else"
    );
    let (outcome, obj) = decide(
        &fixtures::measured(1, 1, 0),
        &fixtures::objectives(900, 300, 1.0),
        &integ,
    );
    assert_eq!(outcome, Outcome::Pass);
    assert_eq!(
        obj.pass_rate,
        Some(1.0),
        "objectives.pass_rate is the REQUEST; discarding it would hide what was asked for"
    );
    assert_eq!(
        obj.met, None,
        "met is null, never true, when the pass rate is unmeasurable"
    );
}

/// The `objectives` block published in the signed document is the REQUEST in
/// all three fields — swapping the measured ratio into `objectives.pass_rate`
/// would silently discard what the adopter asked for.
#[test]
fn the_published_objectives_block_is_the_request_not_the_measurement() {
    let mut integ = fixtures::integrity_pass();
    integ.records_sampled = 100;
    integ.records_sampled_matching = 90;
    integ.mismatches = 10;
    integ.pass_rate_measured = Some(0.9);
    let (_, obj) = decide(
        &fixtures::measured(1, 1, 0),
        &fixtures::objectives(900, 300, 0.5),
        &integ,
    );
    assert_eq!(obj.rto_seconds, Some(900));
    assert_eq!(obj.rpo_seconds, Some(300));
    assert_eq!(obj.pass_rate, Some(0.5), "the ask, not the 0.9 measured");
    assert_eq!(
        integ.pass_rate_measured,
        Some(0.9),
        "the measurement is published apart, in the block phase 7 owns, and `decide` \
         leaves it exactly as it found it"
    );
}

/// The counters and the published rate are DIFFERENT FACTS, and `decide` reads
/// the published one. This fixture is the shape `phase7_verify::roll_up`
/// produces when one selection reconciles perfectly and another never
/// reconciled at all: `25/25` in the counters, and a deliberately withheld
/// rate. A `decide` that recomputes `matching / sampled` reports `met: true`
/// here and turns this test red at assertion time. The end-to-end form of the
/// same property, over a REAL phase-7 verdict and into the SIGNED BYTES, is
/// `crates/logweir/tests/verify_phase.rs`'s
/// `the_signed_document_carries_no_pass_rate_beside_a_partial_verdict`.
#[test]
fn decide_reads_the_published_rate_and_never_recomputes_one_from_the_counters() {
    let mut integ = fixtures::integrity_pass();
    integ.result = logweir_core::outcome::IntegrityResult::Partial;
    integ.partial_reason = Some("one selection returned zero archive fingerprints".into());
    integ.records_sampled = 25;
    integ.records_sampled_matching = 25;
    integ.mismatches = 0;
    integ.pass_rate_measured = None;
    let (outcome, obj) = decide(
        &fixtures::measured(1, 1, 0),
        &fixtures::objectives(900, 300, 1.0),
        &integ,
    );
    assert_eq!(outcome, Outcome::FailIntegrity);
    assert_eq!(
        obj.met, None,
        "the counters say 25/25 and the published rate says nothing; `met` follows the \
         published rate, so the aggregate verdict is unmeasurable"
    );
}

/// `all()` over an empty slice is vacuously TRUE. All three objective fields
/// are optional, so `objectives: {}` used to sign `met: true` under three `—`
/// rows — the identical defect `phase7_verify::roll_up` answers first and
/// explicitly for an empty ledger. Nothing was requested, so there is nothing
/// to report.
#[test]
fn no_objective_requested_leaves_met_null_never_vacuously_true() {
    let (outcome, obj) = decide(
        &fixtures::measured(1, 1, 0),
        &logweir_core::spec::ObjectivesSpec {
            rto_seconds: None,
            rpo_seconds: None,
            pass_rate: None,
        },
        &fixtures::integrity_pass(),
    );
    assert_eq!(
        obj.met, None,
        "no objective was requested, so `met: true` claims a verdict nobody asked for"
    );
    assert_eq!(
        outcome,
        Outcome::Pass,
        "an unrequested objective is not a MISSED one; integrity still decides the outcome"
    );
}

// ------------------------------------------------------- phase 8: the matrix verdict

/// `docs/support-matrix.md`'s five outcomes, decided from what the drill
/// actually did. The three-row table in `matrix_verdict_for`'s own doc comment,
/// asserted.
#[test]
fn the_matrix_verdict_describes_the_drill_that_ran() {
    use logweir::drill::phase8_score::matrix_verdict_for;
    use logweir_core::outcome::{IntegrityLevel as L, MatrixVerdict as V};

    // A pass at byte-fingerprint level is the one green row.
    assert_eq!(
        matrix_verdict_for(Outcome::Pass, L::ByteFingerprint, V::Pass),
        (V::Pass, None)
    );
    // A pass at a REDUCED integrity level is `pass-degraded`, which is what
    // that value is for — "the drill passed at a reduced integrity level".
    assert_eq!(
        matrix_verdict_for(Outcome::Pass, L::ConsumeOnly, V::Pass),
        (V::PassDegraded, None)
    );
    // Anything that is not a pass is a `fail` carrying its reason.
    for outcome in [
        Outcome::FailObjective,
        Outcome::FailIntegrity,
        Outcome::PreflightFailed,
    ] {
        let (verdict, reason) = matrix_verdict_for(outcome, L::ByteFingerprint, V::Pass);
        assert_eq!(verdict, V::Fail, "{outcome:?} must not carry a matrix pass");
        assert!(
            reason
                .as_deref()
                .is_some_and(|r| r.contains(outcome.wire_name())),
            "{outcome:?}: a `fail` verdict must name the outcome that produced it: {reason:?}"
        );
    }
}

/// The lever readback is NEVER RAISED here. Phase 5 lowered the field on
/// evidence — the engine accepted `header_preflight` and did not act on it —
/// and a drill-level verdict must not overwrite an engine-level finding with
/// a weaker one, in either direction.
#[test]
fn a_lever_finding_from_phase_5_survives_the_drill_level_verdict() {
    use logweir::drill::phase8_score::matrix_verdict_for;
    use logweir_core::outcome::{IntegrityLevel as L, MatrixVerdict as V};
    for outcome in [Outcome::Pass, Outcome::PreflightFailed] {
        assert_eq!(
            matrix_verdict_for(outcome, L::ByteFingerprint, V::FailLeverNotHonoured),
            (V::FailLeverNotHonoured, None),
            "{outcome:?} must not overwrite the lever finding"
        );
    }
}

// ----------------------------------------------------------------- phase 8: sign + put

#[test]
fn an_unreadable_signing_key_exits_4_and_uploads_nothing() {
    let store = fixtures::recording_store();
    let err = logweir::drill::phase8_score::run(
        &fixtures::scorecard_pass(),
        &fixtures::unreadable_signing_key().path,
        &store,
        None,
    )
    .unwrap_err();
    assert!(matches!(err, logweir::drill::DrillError::SigningOrLock(_)));
    // The exit-4 contract is asserted through the ONE mapping that owns it.
    assert_eq!(
        logweir::exit::ExitCode::from(err),
        logweir::exit::ExitCode::SigningOrLock
    );
    assert!(
        store.puts().is_empty(),
        "an unreadable signing input aborts before anything is uploaded"
    );
}

#[test]
fn a_backend_without_conditional_put_records_create_only_enforced_false() {
    let store = fixtures::store_without_conditional_put();
    let signed = logweir::drill::phase8_score::run(
        &fixtures::scorecard_pass(),
        &fixtures::good_signing_key().path,
        &store,
        None,
    )
    .unwrap();
    assert!(!signed.scorecard.evidence.create_only_enforced);
    assert!(
        !signed.scorecard.evidence.immutable,
        "immutable is false without a provider readback"
    );
}

/// Pins the CALL SITE of `put_create_only`, not just its behaviour: deleting
/// either put from `run` leaves this assertion failing at assertion time.
/// Also pins Global Constraint 6 — every key written is under `logweir/`, and
/// `manifest.json` is never among them.
#[test]
fn a_successful_run_writes_the_scorecard_and_its_sidecar_under_the_logweir_prefix() {
    let store = fixtures::recording_store();
    let sc = fixtures::scorecard_pass();
    let signed =
        logweir::drill::phase8_score::run(&sc, &fixtures::good_signing_key().path, &store, None)
            .unwrap();
    let run_id = &sc.run_id;
    assert_eq!(
        store.puts(),
        vec![
            format!("logweir/drills/{run_id}.json"),
            format!("logweir/drills/{run_id}.sig"),
        ]
    );
    for k in store.puts() {
        assert!(k.starts_with("logweir/"), "GC6: `{k}` escapes `logweir/`");
        assert!(
            !k.ends_with("manifest.json"),
            "GC6: logweir never writes manifest.json, got `{k}`"
        );
    }
    assert!(signed.scorecard.evidence.create_only_enforced);
}

/// The bytes signed ARE the bytes stored: the scorecard is never re-serialised
/// after signing, so the sidecar verifies against exactly what an auditor
/// downloads.
#[test]
fn the_stored_bytes_are_the_signed_bytes_and_verify_against_the_signing_key() {
    let store = fixtures::recording_store();
    let key = fixtures::good_signing_key();
    let sc = fixtures::scorecard_pass();
    let signed = logweir::drill::phase8_score::run(&sc, &key.path, &store, None).unwrap();

    let (stored, _vid) = store
        .get(&format!("logweir/drills/{}.json", sc.run_id))
        .unwrap();
    assert_eq!(
        stored, signed.bytes,
        "the stored object must be byte-identical to what was signed"
    );
    let vk = logweir_evidence::keys::SigningKey::from_pem_file(&key.path)
        .unwrap()
        .verifying_key();
    logweir_evidence::verify::verify_detached(
        &vk,
        logweir_evidence::PAYLOAD_TYPE_SCORECARD,
        &stored,
        &signed.sidecar,
    )
    .expect("the sidecar must cryptographically verify over the STORED bytes");

    // And the sidecar that was uploaded is the one returned.
    let (stored_sig, _) = store
        .get(&format!("logweir/drills/{}.sig", sc.run_id))
        .unwrap();
    assert_eq!(stored_sig, serde_json::to_vec(&signed.sidecar).unwrap());
}

/// Create-only means an existing object is never overwritten. A second drill
/// reusing a run id must be REFUSED, and the first drill's evidence must still
/// be byte-for-byte what it was.
#[test]
fn an_existing_key_is_refused_and_the_first_drills_bytes_are_untouched() {
    let store = fixtures::recording_store();
    let key = fixtures::good_signing_key();
    let sc = fixtures::scorecard_pass();
    let first = logweir::drill::phase8_score::run(&sc, &key.path, &store, None)
        .expect("the first put succeeds");

    // A SECOND, different document at the SAME key.
    let mut second_sc = sc.clone();
    second_sc.triggered_by = Some("a second drill that must not overwrite the first".into());
    let err = logweir::drill::phase8_score::run(&second_sc, &key.path, &store, None)
        .expect_err("an existing key must be refused, never overwritten");
    assert!(matches!(err, DrillError::SigningOrLock(_)));
    assert_eq!(ExitCode::from(err), ExitCode::SigningOrLock);

    let (stored, _) = store
        .get(&format!("logweir/drills/{}.json", sc.run_id))
        .unwrap();
    assert_eq!(
        stored, first.bytes,
        "the first drill's evidence was clobbered by the second"
    );
}

/// `immutable` is set ONLY from a provider readback. A caller's claim of `true`
/// reaches neither the in-memory scorecard (step 7 overwrites it with what the
/// store actually reported) nor the signed artifact (step 3 zeroes it before
/// the bytes exist). Both halves are asserted, because only asserting the
/// in-memory half is what let the divergence hide the first time.
#[test]
fn immutable_is_false_without_a_provider_readback_even_when_the_input_claimed_true() {
    let store = fixtures::recording_store();
    let mut sc = fixtures::scorecard_pass();
    sc.evidence.immutable = true;
    sc.evidence.retain_until = Some(fixtures::ts("2030-01-01T00:00:00Z"));
    let signed =
        logweir::drill::phase8_score::run(&sc, &fixtures::good_signing_key().path, &store, None)
            .unwrap();
    assert!(
        !signed.scorecard.evidence.immutable,
        "immutable must come from a readback, never from the caller's claim"
    );
    assert_eq!(
        signed.scorecard.evidence.retain_until, None,
        "a retention date nobody read back must not survive either"
    );
    let doc: serde_json::Value = serde_json::from_slice(&signed.bytes).unwrap();
    assert_eq!(
        doc["evidence"]["immutable"],
        serde_json::json!(false),
        "and the SIGNED bytes — the only thing an auditor sees — must not carry \
         the caller's WORM claim either"
    );
    assert_eq!(doc["evidence"]["retain_until"], serde_json::Value::Null);
}

/// The signed artifact carries NO claim about the upload, because at signing
/// time there is nothing to claim: the put has not happened.
///
/// Two properties are pinned together here, and they are the pair that makes
/// the signature meaningful:
///
/// 1. The four post-put fields (`create_only_enforced`, `immutable`,
///    `retain_until`, `version_id`) are zeroed BEFORE serialisation, whatever
///    the caller supplied — so a valid signature can never cover an
///    unsubstantiated WORM or create-only claim.
/// 2. The scorecard is never re-serialised AFTER signing. Step 7's readback
///    lands on `Signed.scorecard` only; re-serialising to "tidy up" the
///    divergence would produce bytes the sidecar no longer covers.
///
/// The caller here supplies the most optimistic possible evidence block and
/// the store genuinely enforces create-only, so BOTH a carried-through claim
/// and a re-serialised readback would show up as `true` in the bytes. Neither
/// does.
#[test]
fn the_signed_bytes_carry_no_unsubstantiated_claim_about_the_upload() {
    let store = fixtures::recording_store();
    let key = fixtures::good_signing_key();
    let mut sc = fixtures::scorecard_pass();
    sc.evidence.create_only_enforced = true;
    sc.evidence.immutable = true;
    sc.evidence.retain_until = Some(fixtures::ts("2030-01-01T00:00:00Z"));
    sc.evidence.version_id = Some("v-claimed-by-the-caller".into());

    let signed = logweir::drill::phase8_score::run(&sc, &key.path, &store, None).unwrap();

    let doc: serde_json::Value = serde_json::from_slice(&signed.bytes).unwrap();
    assert_eq!(
        doc["evidence"]["create_only_enforced"],
        serde_json::json!(false)
    );
    assert_eq!(doc["evidence"]["immutable"], serde_json::json!(false));
    assert_eq!(doc["evidence"]["retain_until"], serde_json::Value::Null);
    assert_eq!(doc["evidence"]["version_id"], serde_json::Value::Null);

    // The in-memory value holds the REAL readback: this store does enforce
    // create-only, so the two deliberately disagree — and they disagree in the
    // safe direction, the signed document under-claiming rather than over-
    // claiming. That is also what makes property 2 above testable at all.
    assert!(
        signed.scorecard.evidence.create_only_enforced,
        "the post-put truth is still captured in memory for a future receipt"
    );

    // Property 2: the stored bytes are still the signed bytes.
    let (stored, _) = store
        .get(&format!("logweir/drills/{}.json", sc.run_id))
        .unwrap();
    assert_eq!(signed.bytes, stored);
    let vk = logweir_evidence::keys::SigningKey::from_pem_file(&key.path)
        .unwrap()
        .verifying_key();
    logweir_evidence::verify::verify_detached(
        &vk,
        logweir_evidence::PAYLOAD_TYPE_SCORECARD,
        &stored,
        &signed.sidecar,
    )
    .expect("the stored bytes must still verify after the readback");
}

/// Step 2: a self-contradicting document is refused BEFORE it is signed, and
/// therefore before anything is uploaded.
#[test]
fn a_self_contradicting_scorecard_is_refused_before_signing_and_uploads_nothing() {
    let store = fixtures::recording_store();
    let mut sc = fixtures::scorecard_pass();
    // Violates the Global Constraint 18(a) false-branch invariant.
    sc.measured.rpo_source_relative_seconds = Some(0);
    let err =
        logweir::drill::phase8_score::run(&sc, &fixtures::good_signing_key().path, &store, None)
            .expect_err("a document violating its own invariants must never be signed");
    assert!(matches!(err, DrillError::SigningOrLock(_)));
    assert!(
        store.puts().is_empty(),
        "the invariant check must precede every put"
    );
}

#[test]
fn an_engine_subreport_under_the_per_run_prefix_is_carried_verbatim() {
    let store = fixtures::recording_store();
    let sc = fixtures::scorecard_pass();
    // Deliberately NOT canonical JSON: verbatim retention means these exact
    // bytes, whitespace and all, survive into the signed document.
    let raw = b"{ \"schema_version\":\"1.0.0\",  \"report_id\" : \"R\" }\n";
    store
        .put_create_only(
            &format!("logweir/{}/engine-validation/report.json", sc.run_id),
            raw,
        )
        .unwrap();

    let signed =
        logweir::drill::phase8_score::run(&sc, &fixtures::good_signing_key().path, &store, None)
            .unwrap();
    let sub = signed
        .scorecard
        .engine_subreport
        .expect("the sub-report under the per-run prefix must be retained");
    assert!(sub.retained_verbatim);
    assert_eq!(
        sub.retrieved_from,
        format!("logweir/{}/engine-validation/report.json", sc.run_id),
        "retrieved_from must name the exact key that was read, not the prefix it \
         sits under — a prefix does not identify what body_sha256 binds"
    );
    use base64::Engine as _;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(&sub.body_b64)
        .unwrap();
    assert_eq!(decoded, raw, "the engine's bytes must survive verbatim");
    assert_eq!(sub.body_sha256, logweir_core::ids::sha256_prefixed(raw));
    assert!(
        sub.caveat.contains("corroborates nothing Logweir claims"),
        "the spec §6.1 caveat must travel with the retained bytes"
    );
}

#[test]
fn an_absent_engine_subreport_is_a_note_on_phase_8_not_a_failure() {
    let store = fixtures::recording_store();
    let mut sc = fixtures::scorecard_pass();
    sc.phases.push(logweir_core::scorecard::PhaseRecord {
        phase: 8,
        name: "score".into(),
        at: fixtures::ts("2026-09-03T09:09:02Z"),
        outcome: "scored".into(),
        duration_ms: 3,
        notes: vec![],
    });
    let signed =
        logweir::drill::phase8_score::run(&sc, &fixtures::good_signing_key().path, &store, None)
            .unwrap();
    assert!(signed.scorecard.engine_subreport.is_none());
    let p8 = signed
        .scorecard
        .phases
        .iter()
        .find(|p| p.phase == 8)
        .unwrap();
    assert_eq!(
        p8.notes,
        vec!["no engine validation report under the per-run prefix".to_string()],
        "an empty prefix is a warning on the phase record, never a silent drop"
    );
}

/// MINOR-1: the brief assumes the per-run prefix holds exactly one object. When
/// it does not, the artifact must say which object was retained and that others
/// were dropped, rather than discarding them in silence.
#[test]
fn extra_objects_under_the_engine_validation_prefix_are_named_not_silently_dropped() {
    let store = fixtures::recording_store();
    let mut sc = fixtures::scorecard_pass();
    sc.phases.push(logweir_core::scorecard::PhaseRecord {
        phase: 8,
        name: "score".into(),
        at: fixtures::ts("2026-09-03T09:09:02Z"),
        outcome: "scored".into(),
        duration_ms: 3,
        notes: vec![],
    });
    let prefix = format!("logweir/{}/engine-validation", sc.run_id);
    // Written in REVERSE lexicographic order. Note this does NOT prove
    // `list_keys` sorts: the in-memory backend is a BTreeMap and returns keys
    // ordered whether or not `out.sort()` runs (mutant Z4 survives, and
    // `storage.rs` documents that honestly). What this DOES pin is the
    // phase-8 call site's choice of `keys[0]` over `keys[last]` (mutant Z3)
    // and that the retained key is named rather than assumed.
    store
        .put_create_only(&format!("{prefix}/b.json"), b"{\"second\":true}")
        .unwrap();
    store
        .put_create_only(&format!("{prefix}/a.json"), b"{\"first\":true}")
        .unwrap();

    let signed =
        logweir::drill::phase8_score::run(&sc, &fixtures::good_signing_key().path, &store, None)
            .unwrap();
    let sub = signed.scorecard.engine_subreport.clone().unwrap();
    assert_eq!(
        sub.retrieved_from,
        format!("{prefix}/a.json"),
        "the lexicographically first key is retained, deterministically"
    );
    assert_eq!(
        sub.body_sha256,
        logweir_core::ids::sha256_prefixed(b"{\"first\":true}")
    );

    let p8 = signed
        .scorecard
        .phases
        .iter()
        .find(|p| p.phase == 8)
        .unwrap();
    assert_eq!(
        p8.notes,
        vec![format!(
            "engine-validation prefix held 2 objects; retained {prefix}/a.json and dropped the rest"
        )],
        "dropping evidence silently is the failure mode this note exists to prevent"
    );
}

// ----------------------------------------------------------------- the exit contract

/// Global Constraint 11 lives in exactly one place. This test is what fails if
/// a signing failure is ever routed to exit 1 — or a guard refusal to exit 1,
/// or an operational failure to exit 4.
#[test]
fn each_drill_error_maps_to_its_contracted_exit_code() {
    assert_eq!(
        ExitCode::from(DrillError::SigningOrLock("no key".into())),
        ExitCode::SigningOrLock,
        "a drill that ran but could not be signed is exit 4, never exit 1"
    );
    assert_eq!(
        ExitCode::from(DrillError::Guard(logweir_core::guard::GuardRefusal(
            "refused".into()
        ))),
        ExitCode::GuardRefused
    );
    assert_eq!(
        ExitCode::from(DrillError::Operational("boom".into())),
        ExitCode::Operational
    );
    assert_eq!(
        ExitCode::from(DrillError::Kafka(KafkaError::Client("down".into()))),
        ExitCode::Operational
    );
    assert_eq!(
        ExitCode::from(DrillError::Engine(
            logweir_core::engine::EngineError::Operational("nope".into())
        )),
        ExitCode::Operational
    );
}

/// `RestoreNoOp` is a drill RESULT and must be intercepted by the orchestrator
/// and turned into a SIGNED scorecard before any exit code is derived. Letting
/// it fall through to a bare exit code would produce an exit-2 with no
/// artifact — the exact defect `task-21a-addendum.md` ruling A8 forbids.
#[test]
#[should_panic(expected = "RestoreNoOp must be intercepted")]
fn a_restore_no_op_may_never_be_converted_straight_to_an_exit_code() {
    let _ = ExitCode::from(DrillError::RestoreNoOp("every partition at 0".into()));
}

// ----------------------------------------------------------------- phase 9: teardown

/// A deleter that reports one topic as failed, per name.
struct PartiallyFailingDeleter;
impl TopicDeleter for PartiallyFailingDeleter {
    fn delete_topics(
        &self,
        names: &[String],
    ) -> Result<Vec<(String, Result<(), String>)>, KafkaError> {
        Ok(names
            .iter()
            .map(|n| {
                (
                    n.clone(),
                    Err("broker refused: TOPIC_AUTHORIZATION_FAILED".to_string()),
                )
            })
            .collect())
    }
}

/// A deleter whose whole call fails — no per-topic answers at all.
struct RefusingDeleter;
impl TopicDeleter for RefusingDeleter {
    fn delete_topics(
        &self,
        _names: &[String],
    ) -> Result<Vec<(String, Result<(), String>)>, KafkaError> {
        Err(KafkaError::Client("admin client unreachable".into()))
    }
}

#[test]
fn teardown_deletes_exactly_the_mapped_topics_and_nothing_else() {
    let d = fixtures::RecordingDeleter {
        deleted: Default::default(),
    };
    let a = logweir::drill::phase9_teardown::run(
        &d,
        &fixtures::mapping("orders", "drill-orders"),
        "delete",
        logweir_core::spec::TargetMode::Scratch,
        "RUN",
        "SC",
    );
    assert_eq!(a.topics_deleted, vec!["drill-orders"]);
    assert!(a.topics_failed.is_empty());
    assert_eq!(*d.deleted.lock().unwrap(), vec!["drill-orders"]);
}

#[test]
fn teardown_keep_deletes_nothing_but_still_attests() {
    let d = fixtures::RecordingDeleter {
        deleted: Default::default(),
    };
    let a = logweir::drill::phase9_teardown::run(
        &d,
        &fixtures::mapping("orders", "drill-orders"),
        "keep",
        logweir_core::spec::TargetMode::Scratch,
        "RUN",
        "SC",
    );
    assert!(a.topics_deleted.is_empty());
    assert!(d.deleted.lock().unwrap().is_empty());
    assert_eq!(a.teardown_policy, "keep");
}

/// The honesty property: a topic the broker refused to delete is NEVER listed
/// as deleted.
#[test]
fn a_topic_that_could_not_be_deleted_is_attested_as_failed_not_as_deleted() {
    let a = logweir::drill::phase9_teardown::run(
        &PartiallyFailingDeleter,
        &fixtures::mapping("orders", "drill-orders"),
        "delete",
        logweir_core::spec::TargetMode::Scratch,
        "RUN",
        "SC",
    );
    assert!(
        a.topics_deleted.is_empty(),
        "a topic the broker refused must never appear as deleted"
    );
    assert_eq!(a.topics_failed.len(), 1);
    assert_eq!(a.topics_failed[0].0, "drill-orders");
    assert!(a.topics_failed[0].1.contains("TOPIC_AUTHORIZATION_FAILED"));
}

/// A call-level failure means NOTHING was deleted — every mapped topic is
/// attested as failed, rather than the attestation falling silent.
#[test]
fn a_deleter_that_refuses_the_whole_call_fails_every_mapped_topic() {
    let mut mapping = fixtures::mapping("orders", "drill-orders");
    mapping.insert("payments".into(), "drill-payments".into());
    let a = logweir::drill::phase9_teardown::run(
        &RefusingDeleter,
        &mapping,
        "delete",
        logweir_core::spec::TargetMode::Scratch,
        "RUN",
        "SC",
    );
    assert!(a.topics_deleted.is_empty());
    let failed: Vec<&str> = a.topics_failed.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(failed, vec!["drill-orders", "drill-payments"]);
    assert!(a
        .topics_failed
        .iter()
        .all(|(_, e)| e.contains("admin client unreachable")));
}

/// The attestation names exactly ONE signed scorecard, by the sha256 of its
/// signed bytes — not the run id a second time.
#[test]
fn the_attestation_binds_the_sha256_of_the_signed_scorecard_bytes() {
    let store = fixtures::recording_store();
    let sc = fixtures::scorecard_pass();
    let signed =
        logweir::drill::phase8_score::run(&sc, &fixtures::good_signing_key().path, &store, None)
            .unwrap();
    let digest = logweir_core::ids::sha256_prefixed(&signed.bytes);
    let a = logweir::drill::phase9_teardown::run(
        &fixtures::RecordingDeleter {
            deleted: Default::default(),
        },
        &fixtures::mapping("orders", "drill-orders"),
        "delete",
        logweir_core::spec::TargetMode::Scratch,
        &sc.run_id,
        &digest,
    );
    assert_eq!(a.scorecard_sha256, digest);
    assert_ne!(
        a.scorecard_sha256, a.run_id,
        "the fifth parameter is the scorecard digest, never the run id again"
    );
}

/// The attestation is signed with PAYLOAD_TYPE_TEARDOWN, put create-only next
/// to the scorecard, and the failures survive into the stored bytes.
#[test]
fn the_persisted_attestation_is_signed_create_only_and_still_reports_the_failures() {
    let store = fixtures::recording_store();
    let key = fixtures::good_signing_key();
    let a = logweir::drill::phase9_teardown::run(
        &PartiallyFailingDeleter,
        &fixtures::mapping("orders", "drill-orders"),
        "delete",
        logweir_core::spec::TargetMode::Scratch,
        "RUN0",
        "sha256:deadbeef",
    );
    logweir::drill::phase9_teardown::persist(&a, &key.path, &store).unwrap();
    assert_eq!(
        store.puts(),
        vec![
            "logweir/drills/RUN0.teardown.json".to_string(),
            "logweir/drills/RUN0.teardown.sig".to_string(),
        ]
    );

    let (bytes, _) = store.get("logweir/drills/RUN0.teardown.json").unwrap();
    let doc: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(doc["topics_deleted"], serde_json::json!([]));
    assert_eq!(doc["topics_failed"][0][0], "drill-orders");
    assert_eq!(doc["scorecard_sha256"], "sha256:deadbeef");

    let (sig_bytes, _) = store.get("logweir/drills/RUN0.teardown.sig").unwrap();
    let sidecar: logweir_evidence::Sidecar = serde_json::from_slice(&sig_bytes).unwrap();
    let vk = logweir_evidence::keys::SigningKey::from_pem_file(&key.path)
        .unwrap()
        .verifying_key();
    logweir_evidence::verify::verify_detached(
        &vk,
        logweir_evidence::PAYLOAD_TYPE_TEARDOWN,
        &bytes,
        &sidecar,
    )
    .expect("the teardown attestation must verify over the stored bytes");

    // Create-only applies here too: a second persist for the same run id is
    // refused rather than silently replacing the first attestation.
    let err = logweir::drill::phase9_teardown::persist(&a, &key.path, &store)
        .expect_err("an existing teardown key must be refused");
    assert_eq!(ExitCode::from(err), ExitCode::SigningOrLock);
}

#[test]
fn an_unreadable_teardown_signing_key_is_exit_4() {
    let store = fixtures::recording_store();
    let a = logweir::drill::phase9_teardown::run(
        &fixtures::RecordingDeleter {
            deleted: Default::default(),
        },
        &fixtures::mapping("orders", "drill-orders"),
        "delete",
        logweir_core::spec::TargetMode::Scratch,
        "RUN1",
        "sha256:0",
    );
    let err = logweir::drill::phase9_teardown::persist(
        &a,
        &fixtures::unreadable_signing_key().path,
        &store,
    )
    .expect_err("an unusable signing key must fail");
    assert!(matches!(err, DrillError::SigningOrLock(_)));
    assert_eq!(ExitCode::from(err), ExitCode::SigningOrLock);
    assert!(
        store.puts().is_empty(),
        "signing precedes every put here too"
    );
}
