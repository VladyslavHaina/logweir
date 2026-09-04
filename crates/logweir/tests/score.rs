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

/// The value compared against the objective is the one that excludes phase 5,
/// because header_preflight: full is a full read and decode of the window that
/// no incident responder performs.
#[test]
fn the_objective_is_compared_against_rto_excluding_preflight() {
    let m = fixtures::measured(/*rto*/ 950, /*excl*/ 300, /*rpo*/ 0);
    let (outcome, obj, _) = decide(
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
    let (outcome, obj, _) = decide(
        &m,
        &fixtures::objectives(900, 300, 1.0),
        &fixtures::integrity_pass(),
    );
    assert_eq!(outcome, Outcome::FailObjective);
    assert_eq!(obj.met, Some(false));
}

#[test]
fn a_mismatch_is_fail_integrity_regardless_of_the_objectives() {
    let (outcome, _, _) = decide(
        &fixtures::measured(1, 1, 0),
        &fixtures::objectives(900, 300, 1.0),
        &fixtures::integrity_fail(),
    );
    assert_eq!(outcome, Outcome::FailIntegrity);
}

#[test]
fn consume_only_keeps_the_requested_pass_rate_and_leaves_met_null() {
    let (outcome, obj, measured_rate) = decide(
        &fixtures::measured(1, 1, 0),
        &fixtures::objectives(900, 300, 1.0),
        &fixtures::integrity_consume_only(),
    );
    assert_eq!(outcome, Outcome::Pass);
    assert_eq!(
        obj.pass_rate,
        Some(1.0),
        "objectives.pass_rate is the REQUEST; discarding it would hide what was asked for"
    );
    assert_eq!(
        measured_rate, None,
        "the MEASURED rate is null — it goes to integrity.pass_rate_measured"
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
    let (_, obj, measured_rate) = decide(
        &fixtures::measured(1, 1, 0),
        &fixtures::objectives(900, 300, 0.5),
        &integ,
    );
    assert_eq!(obj.rto_seconds, Some(900));
    assert_eq!(obj.rpo_seconds, Some(300));
    assert_eq!(obj.pass_rate, Some(0.5), "the ask, not the 0.9 measured");
    assert_eq!(measured_rate, Some(0.9), "the measurement, published apart");
}

// ----------------------------------------------------------------- phase 8: sign + put

#[test]
fn a_signing_failure_exits_4_and_uploads_nothing() {
    let store = fixtures::recording_store();
    let err = logweir::drill::phase8_score::run(
        &fixtures::scorecard_pass(),
        &fixtures::unreadable_signing_key().path,
        &store,
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
        "OSO's own rule, adopted verbatim: signing failures abort before anything is uploaded"
    );
}

#[test]
fn a_backend_without_conditional_put_records_create_only_enforced_false() {
    let store = fixtures::store_without_conditional_put();
    let signed = logweir::drill::phase8_score::run(
        &fixtures::scorecard_pass(),
        &fixtures::good_signing_key().path,
        &store,
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
        logweir::drill::phase8_score::run(&sc, &fixtures::good_signing_key().path, &store).unwrap();
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
    let signed = logweir::drill::phase8_score::run(&sc, &key.path, &store).unwrap();

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
    let first =
        logweir::drill::phase8_score::run(&sc, &key.path, &store).expect("the first put succeeds");

    // A SECOND, different document at the SAME key.
    let mut second_sc = sc.clone();
    second_sc.triggered_by = Some("a second drill that must not overwrite the first".into());
    let err = logweir::drill::phase8_score::run(&second_sc, &key.path, &store)
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

/// `immutable` is set ONLY from a provider readback. An input claiming `true`
/// is overwritten by what the store actually reported, not carried through.
#[test]
fn immutable_is_false_without_a_provider_readback_even_when_the_input_claimed_true() {
    let store = fixtures::recording_store();
    let mut sc = fixtures::scorecard_pass();
    sc.evidence.immutable = true;
    sc.evidence.retain_until = Some(fixtures::ts("2030-01-01T00:00:00Z"));
    let signed =
        logweir::drill::phase8_score::run(&sc, &fixtures::good_signing_key().path, &store).unwrap();
    assert!(
        !signed.scorecard.evidence.immutable,
        "immutable must come from a readback, never from the caller's claim"
    );
    assert_eq!(
        signed.scorecard.evidence.retain_until, None,
        "a retention date nobody read back must not survive either"
    );
}

/// The scorecard is NEVER re-serialised after signing.
///
/// Step 6's evidence readback mutates `Signed.scorecard` after the payload was
/// signed, so this is the one case where `Signed.scorecard` and `Signed.bytes`
/// deliberately disagree. Re-serialising at the end to "tidy that up" would
/// produce bytes the sidecar no longer verifies over — an auditor downloading
/// the stored object would get a document whose signature fails — so the
/// disagreement is pinned here on purpose, in both directions.
#[test]
fn the_evidence_readback_never_re_serialises_the_signed_payload() {
    let store = fixtures::recording_store();
    let key = fixtures::good_signing_key();
    let mut sc = fixtures::scorecard_pass();
    // Chosen so step 6 genuinely CHANGES `sc`: without a divergence between
    // the input evidence block and the readback, a re-serialisation would
    // produce identical bytes and this test could not tell the difference.
    sc.evidence.immutable = true;
    sc.evidence.retain_until = Some(fixtures::ts("2030-01-01T00:00:00Z"));

    let signed = logweir::drill::phase8_score::run(&sc, &key.path, &store).unwrap();
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

    let doc: serde_json::Value = serde_json::from_slice(&signed.bytes).unwrap();
    assert_eq!(
        doc["evidence"]["immutable"],
        serde_json::json!(true),
        "documented consequence of signing before putting: the readback lands on \
         Signed.scorecard ONLY. The SIGNED bytes still carry the evidence block the \
         caller supplied, because step 6 runs after signing and the payload is never \
         re-serialised. Whoever builds the scorecard (Task 21a) therefore owns getting \
         evidence.create_only_enforced right BEFORE phase 8 is called."
    );
    assert!(
        !signed.scorecard.evidence.immutable,
        "and the in-memory scorecard carries the honest readback"
    );
}

/// Step 2: a self-contradicting document is refused BEFORE it is signed, and
/// therefore before anything is uploaded.
#[test]
fn a_self_contradicting_scorecard_is_refused_before_signing_and_uploads_nothing() {
    let store = fixtures::recording_store();
    let mut sc = fixtures::scorecard_pass();
    // Violates the Global Constraint 18(a) false-branch invariant.
    sc.measured.rpo_source_relative_seconds = Some(0);
    let err = logweir::drill::phase8_score::run(&sc, &fixtures::good_signing_key().path, &store)
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
        logweir::drill::phase8_score::run(&sc, &fixtures::good_signing_key().path, &store).unwrap();
    let sub = signed
        .scorecard
        .engine_subreport
        .expect("the sub-report under the per-run prefix must be retained");
    assert!(sub.retained_verbatim);
    assert_eq!(
        sub.retrieved_from,
        format!("logweir/{}/engine-validation", sc.run_id)
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
        logweir::drill::phase8_score::run(&sc, &fixtures::good_signing_key().path, &store).unwrap();
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
    let a = logweir::drill::phase9_teardown::run(&RefusingDeleter, &mapping, "delete", "RUN", "SC");
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
        logweir::drill::phase8_score::run(&sc, &fixtures::good_signing_key().path, &store).unwrap();
    let digest = logweir_core::ids::sha256_prefixed(&signed.bytes);
    let a = logweir::drill::phase9_teardown::run(
        &fixtures::RecordingDeleter {
            deleted: Default::default(),
        },
        &fixtures::mapping("orders", "drill-orders"),
        "delete",
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
fn a_teardown_that_cannot_be_signed_is_exit_4() {
    let store = fixtures::recording_store();
    let a = logweir::drill::phase9_teardown::run(
        &fixtures::RecordingDeleter {
            deleted: Default::default(),
        },
        &fixtures::mapping("orders", "drill-orders"),
        "delete",
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
