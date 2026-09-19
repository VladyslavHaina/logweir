//! D3 §2.5's mapping table and §12's absent-field rule, over REAL objects.
//!
//! EVERY FIXTURE IN `tests/fixtures/` IS A COPY OF A LIVE OBJECT, taken from
//! the docker-desktop evidence under
//! `/tmp/logweir-roadmap-run/claude/artifacts/{d1,d3}-live/` and reduced to
//! name, namespace and status (annotations and `managedFields` removed). The
//! rows below then vary ONE fact at a time from that base, so a row that
//! passes is a row about a shape a controller really writes rather than one
//! this test invented to be convenient.

mod support;

use std::collections::BTreeSet;

use logweir_api::contract::{OperationState, VerificationState};
use logweir_api::status::{
    backup_view, restore_view, OperationStage, OperationView, TrustState, VerificationScopeLevel,
};
use serde_json::{json, Value};
use support::repo_root;
use weirkeeper::crds::backup::Backup;
use weirkeeper::crds::restore::Restore;

fn fixture(name: &str) -> Value {
    let path = repo_root()
        .join("crates/logweir-api/tests/fixtures")
        .join(name);
    serde_json::from_str(
        &std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display())),
    )
    .expect("a fixture is JSON")
}

fn now() -> chrono::DateTime<chrono::Utc> {
    "2026-09-19T01:30:00Z".parse().expect("a fixed instant")
}

fn backup_from(value: &Value) -> Backup {
    serde_json::from_value(value.clone()).expect("the fixture deserializes as a Backup")
}

fn restore_from(value: &Value) -> Restore {
    serde_json::from_value(value.clone()).expect("the fixture deserializes as a Restore")
}

fn view_of(value: &Value) -> OperationView {
    backup_view(&backup_from(value), now())
}

/// Set a JSON pointer, creating intermediate objects.
fn set(value: &mut Value, pointer: &str, new: Value) {
    let mut cursor = value;
    let parts: Vec<&str> = pointer.trim_start_matches('/').split('/').collect();
    for part in &parts[..parts.len() - 1] {
        cursor = cursor
            .as_object_mut()
            .expect("an object on the path")
            .entry((*part).to_string())
            .or_insert_with(|| json!({}));
    }
    cursor
        .as_object_mut()
        .expect("an object at the leaf")
        .insert(parts[parts.len() - 1].to_string(), new);
}

fn remove(value: &mut Value, pointer: &str) {
    let parts: Vec<&str> = pointer.trim_start_matches('/').split('/').collect();
    let mut cursor = value;
    for part in &parts[..parts.len() - 1] {
        let Some(next) = cursor.as_object_mut().and_then(|o| o.get_mut(*part)) else {
            return;
        };
        cursor = next;
    }
    if let Some(object) = cursor.as_object_mut() {
        object.remove(parts[parts.len() - 1]);
    }
}

// ======================================================================
// The stage table
// ======================================================================

/// **D3 §2.5's stage rows, each from the live object with one fact changed.**
#[test]
fn the_stage_a_controller_wrote_decides_the_state_and_a_terminal_phase_is_never_overridden() {
    let base = fixture("backup-succeeded-verified.json");

    // The live object as it stands: a finished, verified run.
    let finished = view_of(&base);
    assert_eq!(finished.stage, Some(OperationStage::Finished));
    assert_eq!(finished.operation.state, OperationState::Succeeded);
    assert!(finished.operation.terminal);

    for (stage, expected) in [
        ("Queued", OperationState::Queued),
        ("Preparing", OperationState::Preparing),
        ("Verifying", OperationState::Verifying),
        ("Running", OperationState::Running),
        ("Admission", OperationState::Running),
    ] {
        let mut object = base.clone();
        set(&mut object, "/status/phase", json!("Running"));
        set(&mut object, "/status/progress/stage", json!(stage));
        set(
            &mut object,
            "/status/progress/reason",
            json!("WaitingForPod"),
        );
        remove(&mut object, "/status/exitCode");
        remove(&mut object, "/status/exitReason");
        let view = view_of(&object);
        assert_eq!(
            view.operation.state, expected,
            "stage {stage} over phase Running"
        );
        assert_eq!(view.stage, OperationStage::parse(stage));
    }

    // A TERMINAL PHASE IS NOT OVERRIDDEN BY A STAGE. A controller that has
    // seen the Job end but has not written the terminal status yet would carry
    // `stage: Finished` beside `phase: Running`; the reverse — a `Queued`
    // stage on a `Succeeded` object — must not un-finish a recorded result.
    let mut stale_stage = base.clone();
    set(&mut stale_stage, "/status/progress/stage", json!("Queued"));
    let view = view_of(&stale_stage);
    assert_eq!(
        view.operation.state,
        OperationState::Succeeded,
        "a progress stage must never rewrite a recorded outcome"
    );
    assert_eq!(view.stage, Some(OperationStage::Queued));
}

/// **§12: `queued` and `preparing` are never INFERRED.**
///
/// REGRESSION REASON. The whole reason the progress channel exists is that
/// `phase: Pending` cannot tell "the Job has no pod" from "the pod's container
/// will not start". A projection that guessed either from the phase would
/// publish an observation the controller never made, and it would do it for
/// every object an older controller wrote — which is every object on a cluster
/// mid-upgrade.
#[test]
fn without_a_progress_block_queued_and_preparing_are_never_inferred() {
    let mut object = fixture("backup-succeeded-verified.json");
    remove(&mut object, "/status/progress");
    set(&mut object, "/status/phase", json!("Pending"));
    remove(&mut object, "/status/exitCode");
    remove(&mut object, "/status/exitReason");
    let view = view_of(&object);
    assert_eq!(view.operation.state, OperationState::Pending);
    assert_eq!(view.stage, Some(OperationStage::Admission));
    assert!(view.progress.is_none(), "no progress block is published");

    set(&mut object, "/status/phase", json!("Running"));
    let view = view_of(&object);
    assert_eq!(view.operation.state, OperationState::Running);

    // The legacy Restore in the fixtures was written before the progress
    // channel existed at all: the same rule, on the real object.
    let legacy = fixture("restore-legacy-no-progress.json");
    let view = restore_view(&restore_from(&legacy), now());
    assert!(view.progress.is_none());
    assert!(view.diagnostics.is_empty());
    assert_eq!(view.operation.state, OperationState::Succeeded);
    assert!(!matches!(
        view.operation.state,
        OperationState::Queued | OperationState::Preparing
    ));
}

/// **A Backup's dynamic-discovery phase is `preparing`, not `unknown`.**
#[test]
fn the_resolving_phase_is_preparing() {
    let mut object = fixture("backup-succeeded-verified.json");
    remove(&mut object, "/status/progress");
    remove(&mut object, "/status/exitCode");
    remove(&mut object, "/status/exitReason");
    set(&mut object, "/status/phase", json!("Resolving"));
    let view = view_of(&object);
    assert_eq!(view.stage, Some(OperationStage::Preparing));
    assert_eq!(view.operation.state, OperationState::Preparing);

    // A phase this build does not know is still `unknown`, with no stage.
    set(&mut object, "/status/phase", json!("Rehydrating"));
    let view = view_of(&object);
    assert_eq!(view.stage, None);
    assert_eq!(view.operation.state, OperationState::Unknown);
    assert_eq!(
        view.operation.state_reason.as_deref(),
        Some("UnrecognizedPhase")
    );
}

/// **D3 §2.5's last row: a status too old to believe is `unknown`.**
#[test]
fn a_stale_observation_and_a_never_reconciled_object_are_both_unknown() {
    let mut object = fixture("backup-succeeded-verified.json");
    set(&mut object, "/status/phase", json!("Running"));
    remove(&mut object, "/status/exitCode");
    remove(&mut object, "/status/exitReason");
    set(&mut object, "/status/progress/stage", json!("Running"));
    set(
        &mut object,
        "/status/progress/lastObservedTime",
        json!("2026-09-19T01:25:00Z"),
    );
    // Five minutes exactly is not stale; a second past it is.
    let view = view_of(&object);
    assert_eq!(view.operation.state, OperationState::Running);
    assert!(!view.stale);

    set(
        &mut object,
        "/status/progress/lastObservedTime",
        json!("2026-09-19T01:24:59Z"),
    );
    let view = view_of(&object);
    assert!(view.stale);
    assert_eq!(view.operation.state, OperationState::Unknown);
    assert_eq!(view.operation.state_reason.as_deref(), Some("StatusStale"));
    // The progress block it was computed from is still published: "the status
    // is stale" is a verdict ABOUT the facts, not a reason to withhold them.
    assert!(view.progress.is_some());

    // No status at all, 120 s after creation.
    let mut fresh = fixture("backup-succeeded-verified.json");
    fresh.as_object_mut().expect("object").remove("status");
    set(
        &mut fresh,
        "/metadata/creationTimestamp",
        json!("2026-09-19T01:29:00Z"),
    );
    let view = view_of(&fresh);
    assert_eq!(view.operation.state, OperationState::Pending);
    assert!(!view.stale, "two minutes is not yet stale");

    set(
        &mut fresh,
        "/metadata/creationTimestamp",
        json!("2026-09-19T01:27:00Z"),
    );
    let view = view_of(&fresh);
    assert!(view.stale);
    assert_eq!(view.operation.state, OperationState::Unknown);

    // A TERMINAL OBJECT IS NEVER STALE. Nothing is going to observe it again,
    // and reporting a finished run as `unknown` after five minutes would make
    // every completed backup in the console unknown by morning. The fixture
    // CARRIES a `lastObservedTime` for this row, so the terminal check is what
    // is being tested and not the absence of the field the check precedes.
    let mut terminal = fixture("backup-succeeded-verified.json");
    set(
        &mut terminal,
        "/status/progress/lastObservedTime",
        json!("2026-09-19T01:00:00Z"),
    );
    assert_eq!(
        terminal.pointer("/status/phase").and_then(Value::as_str),
        Some("Succeeded")
    );
    let long_after: chrono::DateTime<chrono::Utc> =
        "2027-01-01T00:00:00Z".parse().expect("an instant");
    let view = backup_view(&backup_from(&terminal), long_after);
    assert!(!view.stale);
    assert_eq!(view.operation.state, OperationState::Succeeded);
    // And at `now`, where the observation is already half an hour old.
    let view = backup_view(&backup_from(&terminal), now());
    assert!(!view.stale);
    assert_eq!(view.operation.state, OperationState::Succeeded);
}

// ======================================================================
// Trust, result and verification: three columns, never one
// ======================================================================

/// **§12: an absent `trust` block is `basis: none`, never a flattering
/// default.**
#[test]
fn the_trust_basis_of_an_unrecorded_block_is_none_and_never_current() {
    let base = fixture("backup-succeeded-verified.json");
    let view = view_of(&base);
    assert_eq!(view.trust.basis, "Current");
    assert_eq!(view.trust.state, TrustState::Verified);
    assert!(view.operation.verified_success);

    let mut without = base.clone();
    remove(&mut without, "/status/evidence/verification/trust");
    let view = view_of(&without);
    assert_eq!(
        view.trust.basis, "None",
        "an absent trust block is `None` — D3 §12's sentence word for word, in the \
         CRD's own spelling"
    );
    // The SIGNATURE still verified, so the result column is unchanged — the
    // two are different questions and the projection keeps them apart.
    assert_eq!(view.operation.verification.state, VerificationState::Valid);
    assert_eq!(view.trust.state, TrustState::Verified);

    // A retired key that signed while valid is a PASS with its own word.
    let mut historical = base.clone();
    set(
        &mut historical,
        "/status/evidence/verification/trust/basis",
        json!("Historical"),
    );
    set(
        &mut historical,
        "/status/evidence/verification/trust/keyState",
        json!("Retired"),
    );
    let view = view_of(&historical);
    assert_eq!(view.trust.state, TrustState::VerifiedHistorical);
    assert_eq!(view.trust.key_state.as_deref(), Some("Retired"));

    // TRUST-UPGRADE-SIGNEDAT's shape: a status written before `signedAt`
    // existed carries `basis: Unverified` beside `result: NotAttempted`, and
    // it is NOT green on any column.
    let mut unverified = base.clone();
    set(
        &mut unverified,
        "/status/evidence/verification/result",
        json!("NotAttempted"),
    );
    set(
        &mut unverified,
        "/status/evidence/verification/trust/basis",
        json!("Unverified"),
    );
    remove(
        &mut unverified,
        "/status/evidence/verification/trust/keyState",
    );
    let view = view_of(&unverified);
    assert_eq!(view.trust.basis, "Unverified");
    assert_eq!(view.trust.state, TrustState::NotAttempted);
    assert!(!view.operation.verified_success);

    // AND THE OTHER HALF OF THE SAME RULE, which the basis alone could get
    // wrong: a `Valid` result beside `basis: Unverified` is NOT verified.
    // `TrustBasis`'s contract is that `Unverified` is only ever written beside
    // `NotAttempted`, so this pairing is a controller defect — and the reading
    // of it has to fail closed rather than trust the half that says yes.
    let mut inconsistent = base.clone();
    set(
        &mut inconsistent,
        "/status/evidence/verification/trust/basis",
        json!("Unverified"),
    );
    let view = view_of(&inconsistent);
    assert_eq!(view.trust.basis, "Unverified");
    assert_eq!(
        view.trust.state,
        TrustState::NotAttempted,
        "a `Valid` result with an uncompared signing time is not a verified one"
    );

    // A revoked key is `untrusted`, which is neither `invalid` (the bytes are
    // fine) nor `notAttempted` (a verdict WAS reached).
    let mut revoked = base.clone();
    set(
        &mut revoked,
        "/status/evidence/verification/result",
        json!("NotAttempted"),
    );
    set(
        &mut revoked,
        "/status/evidence/verification/trust/basis",
        json!("None"),
    );
    set(
        &mut revoked,
        "/status/evidence/verification/trust/keyState",
        json!("Revoked"),
    );
    let view = view_of(&revoked);
    assert_eq!(view.trust.state, TrustState::Untrusted);
    assert!(!view.operation.verified_success);
}

/// **`Valid` can never be read UP from a basis.**
///
/// REGRESSION REASON. `TrustBasis`'s own header records the defect: a verdict
/// that meant "not verified" while leaving `Valid` on `result` put a green
/// badge on the console whatever it wrote beside it. The inverse is this test:
/// no basis string may turn a non-`Valid` result into a verified state.
#[test]
fn no_trust_basis_promotes_a_result_that_is_not_valid() {
    let base = fixture("backup-succeeded-verified.json");
    for result in ["Invalid", "NotAttempted", "Nonsense"] {
        for basis in [
            "Current",
            "Historical",
            "RecordedBeforeRevocation",
            "Unverified",
            "None",
            "SomethingNewer",
        ] {
            let mut object = base.clone();
            set(
                &mut object,
                "/status/evidence/verification/result",
                json!(result),
            );
            set(
                &mut object,
                "/status/evidence/verification/trust/basis",
                json!(basis),
            );
            let view = view_of(&object);
            assert!(
                !matches!(
                    view.trust.state,
                    TrustState::Verified | TrustState::VerifiedHistorical
                ),
                "result {result} with basis {basis} was read as {:?}",
                view.trust.state
            );
            assert!(!view.operation.verified_success);
        }
    }
}

/// **A Backup's verification scope is `none` with ABSENT counts.**
///
/// REGRESSION REASON. Zeroes here are worse than absence: `0 of 0 sampled
/// records matched` is a sentence a console renders as a failed comparison,
/// and a backup receipt attests counts and a window rather than a restore.
#[test]
fn a_backup_scope_is_none_with_no_counts_and_a_restore_maps_its_integrity_level() {
    let view = view_of(&fixture("backup-succeeded-verified.json"));
    assert_eq!(view.verification_scope.level, VerificationScopeLevel::None);
    assert!(view.verification_scope.records_sampled.is_none());
    assert!(view.verification_scope.records_sampled_matching.is_none());
    assert!(view.verification_scope.records_expected.is_none());
    assert!(view.completion.is_none());
    let capture = view
        .capture
        .expect("the live object carries a capture window");
    assert!(capture.started_at.is_some() && capture.finished_at.is_some());
    // The count the VERIFIED receipt attested, beside the window it covers.
    assert_eq!(capture.records, Some(10));

    // AND ITS ABSENCE IS A FACT ABOUT THE RUN. `status.records` was declared
    // and written by nothing until D3 W2 (`STATUS-RECORDS`), so every older
    // object has it absent — and a zero here would say the run captured
    // nothing, which is a different claim from "no receipt has been verified".
    let mut older = fixture("backup-succeeded-verified.json");
    remove(&mut older, "/status/records");
    let view = view_of(&older);
    assert_eq!(view.capture.expect("still a capture window").records, None);

    let base = fixture("restore-legacy-no-progress.json");
    for (level, expected) in [
        ("byte-fingerprint", VerificationScopeLevel::Sampled),
        ("consume-only", VerificationScopeLevel::Degraded),
        ("not-attempted", VerificationScopeLevel::None),
    ] {
        let mut object = base.clone();
        set(&mut object, "/status/integrity/level", json!(level));
        let view = restore_view(&restore_from(&object), now());
        assert_eq!(view.verification_scope.level, expected, "{level}");
    }

    // An absent integrity block is `none` — not observed, not "complete".
    let mut without = base.clone();
    remove(&mut without, "/status/integrity");
    let view = restore_view(&restore_from(&without), now());
    assert_eq!(view.verification_scope.level, VerificationScopeLevel::None);

    // `complete` does not exist in v1 and cannot be reached by any input.
    let mut invented = base.clone();
    set(&mut invented, "/status/integrity/level", json!("complete"));
    let view = restore_view(&restore_from(&invented), now());
    assert_eq!(view.verification_scope.level, VerificationScopeLevel::None);
}

/// **The completion panel is the signed scorecard's numbers, with the mode the
/// console keys its fixed guidance on.**
#[test]
fn the_completion_panel_carries_the_counts_the_scorecard_recorded() {
    let mut object = fixture("restore-legacy-no-progress.json");
    set(
        &mut object,
        "/status/completion",
        json!({
            "newTopics": [{"name": "scram-restored-orders", "partitions": 3}],
            "recordsExpected": 10,
            "recordsRestored": 10,
            "recordsSampled": 4,
            "recordsSampledMatching": 4,
            "integrityLevel": "byte-fingerprint",
            "sampleWindow": {"start": "2026-09-14T23:50:00Z", "end": "2026-09-14T23:57:00Z"}
        }),
    );
    set(
        &mut object,
        "/status/teardown",
        json!({
            "attestationKey": "logweir/drills/01M2H5MMSKY5JR6RW108EGN954.teardown.json",
            "deleted": ["scram-restored-orders"],
            "failed": [{"topic": "scram-restored-payments", "error": "UNKNOWN_TOPIC_OR_PARTITION"}]
        }),
    );
    let view = restore_view(&restore_from(&object), now());
    let completion = view.completion.clone().expect("the panel is published");
    assert_eq!(completion.records_sampled_matching, Some(4));
    assert_eq!(completion.records_restored, Some(10));
    assert_eq!(completion.new_topics.len(), 1);
    assert_eq!(completion.new_topics[0].partitions, Some(3));
    // THE MODE IS ON THE OPERATION, NOT ON THE PANEL. §3.5's two guidance
    // blocks are keyed on `spec.target.mode`, which is a fact about the run
    // from the moment it is created — a rehearsal is a rehearsal before its
    // scorecard exists — so a Restore that has not finished can still be
    // labelled.
    assert_eq!(view.target_mode.as_deref(), Some("newTopic"));
    assert_eq!(
        view.verification_scope.level,
        VerificationScopeLevel::Sampled
    );
    assert_eq!(view.verification_scope.records_sampled, Some(4));

    let teardown = view.teardown.expect("the teardown outcome is published");
    // THE NAMES, NOT A COUNT. "Which topics went" is the incident question and
    // a count cannot be reconciled against the names the run created.
    assert_eq!(teardown.deleted, vec!["scram-restored-orders".to_string()]);
    assert!(!teardown.deleted_truncated);
    assert_eq!(teardown.failed.len(), 1);
    assert_eq!(teardown.failed[0].topic, "scram-restored-payments");
}

/// **A Restore held for an approval says so.**
#[test]
fn a_restore_waiting_for_a_person_is_pending_and_awaiting_approval() {
    let mut object = fixture("restore-legacy-no-progress.json");
    set(&mut object, "/status/phase", json!("Pending"));
    set(&mut object, "/status/reason", json!("ApprovalNotVerified"));
    remove(&mut object, "/status/exitCode");
    remove(&mut object, "/status/exitReason");
    remove(&mut object, "/status/outcome");
    let view = restore_view(&restore_from(&object), now());
    assert_eq!(view.operation.state, OperationState::Pending);
    assert!(view.awaiting_approval);

    // Any other pending reason is pending and NOT awaiting a person.
    set(&mut object, "/status/reason", json!("AwaitingController"));
    let view = restore_view(&restore_from(&object), now());
    assert_eq!(view.operation.state, OperationState::Pending);
    assert!(!view.awaiting_approval);
}

// ======================================================================
// Diagnostics
// ======================================================================

/// **The diagnosis list is published, and the runner's own identifiers are
/// not.**
///
/// The live fixture carries a `PodCreateRejected` diagnostic AND a
/// `progress.runner` block naming the Job and the pod. The first is what
/// PLAT-14.1 asks for in as many words ("pod mount and scheduling failures as
/// useful resource-scoped errors"); the second is the infrastructure handle
/// `no_infrastructure_detail_is_frozen_into_the_operation_contract` keeps out
/// of the contract. This asserts the projection takes one and not the other.
#[test]
fn the_diagnosis_list_is_published_and_the_runner_block_is_not() {
    let object = fixture("backup-with-diagnostics.json");
    // The fixture really carries both, or this test proves nothing.
    assert!(object.pointer("/status/progress/runner/podName").is_some());
    assert!(object
        .pointer("/status/progress/diagnostics/0/code")
        .is_some());

    let view = view_of(&object);
    assert_eq!(view.diagnostics.len(), 1);
    let first = &view.diagnostics[0];
    assert_eq!(first.code, "PodCreateRejected");
    assert_eq!(first.severity, "Error");
    assert_eq!(first.count, Some(2));
    let object_ref = first
        .object
        .as_ref()
        .expect("the diagnostic names its object");
    assert_eq!(object_ref.kind, "Job");

    let body = serde_json::to_value(&view).expect("the view serialises");
    let progress = body.get("progress").expect("progress is published");
    let keys: BTreeSet<&str> = progress
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect();
    assert!(
        !keys.contains("runner"),
        "the progress block published the runner's Job and pod names: {keys:?}"
    );
    // And no value anywhere in the body IS the pod name.
    let pod = object
        .pointer("/status/progress/runner/podName")
        .and_then(Value::as_str)
        .expect("the fixture names a pod");
    let text = serde_json::to_string(&body).expect("serialises");
    assert!(
        !text.contains(pod),
        "the pod name reached the response body"
    );
}

/// **The diagnosis list is bounded.**
#[test]
fn the_diagnosis_list_is_capped_at_eight() {
    let mut object = fixture("backup-with-diagnostics.json");
    let one = object
        .pointer("/status/progress/diagnostics/0")
        .cloned()
        .expect("the fixture has one");
    let many: Vec<Value> = (0..40).map(|_| one.clone()).collect();
    set(
        &mut object,
        "/status/progress/diagnostics",
        Value::Array(many),
    );
    let view = view_of(&object);
    assert_eq!(view.diagnostics.len(), logweir_api::status::MAX_DIAGNOSTICS);
}
