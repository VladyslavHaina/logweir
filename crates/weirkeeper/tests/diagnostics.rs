//! D3 §2.3 / §2.4 (PLAT-14.1) — the one diagnosis derivation, its closed
//! vocabulary, its sanitizer, the fail-fast rule and the progress grammar.
//!
//! Pure rows only. The reconciler rows that put these through a route table
//! live in `backup_controller.rs` and `restore_controller.rs`.

use std::time::Duration;

use chrono::{DateTime, TimeZone as _, Utc};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use logweir_core::check_contract::CheckCode;
use serde_json::{json, Value};

use weirkeeper::check::waiting::EventFact;
use weirkeeper::conditions::{
    CONDITION_RUNNER_READY, REASON_RUNNER_STARTED, REASON_WAITING_FOR_POD, TERMINAL_STATES,
};
use weirkeeper::crds::{Condition, Diagnostic, DiagnosticObject, RunProgress, RunnerPhase};
use weirkeeper::diagnostics::{
    apply, apply_finished, bounded, derive, held_for, merge_diagnostics, parse_progress,
    recorded_terminal_state, sanitize, should_fail_fast, should_read_progress, terminal_state,
    Code, Diagnosis, Facts, Progress, Severity, Stage, Write, DIAGNOSTICS_MAX,
    FAIL_FAST_SECONDS_DEFAULT, FAIL_FAST_SECONDS_MIN, MESSAGE_MAX_BYTES, MOUNT_TRANSIENT_FOR,
    OBSERVED_HEARTBEAT, PROGRESS_LINE_MAX_BYTES, WAITING_FOR_POD_GRACE,
};

const NS: &str = "logweir-d3w2";
const JOB: &str = "logweir-backup-nightly";
const JOB_UID: &str = "job-uid-1111";
const POD: &str = "logweir-backup-nightly-abcde";

fn at(minute: u32, second: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 9, 3, minute, second)
        .single()
        .expect("a real instant")
}

/// A Job created at 03:00:00, unfinished.
fn job() -> Job {
    serde_json::from_value(json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": JOB, "namespace": NS, "uid": JOB_UID,
            "creationTimestamp": "2026-11-09T03:00:00Z",
        },
        "spec": { "template": { "spec": { "containers": [], "restartPolicy": "Never" } } },
        "status": { "active": 1 },
    }))
    .expect("the Job fixture parses")
}

/// A pod owned by [`job`], with `status` as given.
fn pod(status: Value) -> Pod {
    serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": POD, "namespace": NS,
            "creationTimestamp": "2026-11-09T03:00:05Z",
            "ownerReferences": [{
                "apiVersion": "batch/v1", "kind": "Job", "name": JOB,
                "uid": JOB_UID, "controller": true, "blockOwnerDeletion": true,
            }],
        },
        "spec": { "containers": [] },
        "status": status,
    }))
    .expect("the pod fixture parses")
}

/// The `runner` container waiting, with a SIDECAR AT INDEX 0 — the by-name
/// rule this crate reads container statuses under.
fn waiting_pod(phase: &str, scheduled: &str, reason: &str, message: &str) -> Pod {
    pod(json!({
        "phase": phase,
        "conditions": [{
            "type": "PodScheduled", "status": scheduled,
            "reason": if scheduled == "False" { "Unschedulable" } else { "" },
            "lastTransitionTime": "2026-11-09T03:00:05Z",
        }],
        "containerStatuses": [
            {"name": "log-shipper", "ready": true, "restartCount": 0, "image": "x",
             "imageID": "x", "state": {"running": {"startedAt": "2026-11-09T03:00:10Z"}}},
            {"name": "runner", "ready": false, "restartCount": 0, "image": "x",
             "imageID": "x", "state": {"waiting": {"reason": reason, "message": message}}},
        ],
    }))
}

fn facts<'a>(pod: Option<&'a Pod>, events: &'a [EventFact], now: DateTime<Utc>) -> Facts<'a> {
    Facts {
        job: Box::leak(Box::new(job())),
        pod,
        events,
        runner_phase: None,
        now,
    }
}

// ===========================================================================
// The closed vocabulary — D3 §2.3, exhaustive
// ===========================================================================

/// Every member of the closed enum has all three of D3 §2.3's columns, and the
/// spellings are `metav1` reasons.
#[test]
fn the_diagnosis_table_is_exhaustive_over_its_closed_enum() {
    assert_eq!(
        Code::ALL.len(),
        13,
        "D3 §2.3 names twelve D2 codes plus `WaitingForPod`; got {:?}",
        Code::ALL.iter().map(|c| c.as_str()).collect::<Vec<_>>()
    );
    for code in Code::ALL {
        let text = code.as_str();
        assert!(
            text.chars().next().is_some_and(|c| c.is_ascii_uppercase())
                && text.chars().all(|c| c.is_ascii_alphanumeric())
                && text.len() <= 316,
            "`{text}` has to be a valid `metav1.Condition.reason` — it reaches one through \
             `runner_ready_reason`"
        );
        // Severity is TOTAL, and the two values are not interchangeable.
        let severity = code.severity();
        assert!(
            matches!(severity, Severity::Warning | Severity::Error),
            "{text} has a severity"
        );
        // Transience is TOTAL at both ends of the one time-dependent row.
        let _ = code.transient(Duration::ZERO);
        let _ = code.transient(Duration::from_secs(86_400));
    }
    // The ONE code with no `RunnerReady` projection, and the reason.
    let unprojected: Vec<&str> = Code::ALL
        .iter()
        .filter(|c| c.runner_ready_reason().is_none())
        .map(|c| c.as_str())
        .collect();
    assert_eq!(
        unprojected,
        vec!["DisruptedMidRun"],
        "every code EXCEPT the one about a runner that had already started projects into a \
         `RunnerReady=False` reason. A node that went away mid-run is not a reason the runner \
         never started"
    );
}

/// The vocabulary is D2's, reached through the one door, and nothing else
/// crosses it.
///
/// MUTANT: adding a fourteenth code, or mapping a non-waiting `CheckCode`,
/// fails here.
#[test]
fn the_codes_are_exactly_d2s_waiting_table_plus_one() {
    let mapped: Vec<(&str, &str)> = CheckCode::ALL
        .iter()
        .filter_map(|c| Code::from_check_code(*c).map(|d| (c.as_str(), d.as_str())))
        .collect();
    assert_eq!(
        mapped,
        vec![
            ("CredentialSecretNotFound", "CredentialSecretNotFound"),
            ("CredentialSecretKeyMissing", "CredentialSecretKeyMissing"),
            ("TrustBundleNotFound", "TrustBundleNotFound"),
            ("RunnerImagePullFailed", "RunnerImagePullFailed"),
            ("RunnerImageNotPresent", "RunnerImageNotPresent"),
            ("RunnerImageInvalid", "RunnerImageInvalid"),
            ("PodUnschedulable", "PodUnschedulable"),
            ("SigningKeyMissing", "SigningKeyMissing"),
            ("VolumeMountFailed", "VolumeMountFailed"),
            ("RunnerServiceAccountMissing", "RunnerServiceAccountMissing"),
            ("PodCreateRejected", "PodCreateRejected"),
            // THE ONE RENAME, and it is D3 §2.3's own: a Backup is not a check.
            ("DisruptedMidCheck", "DisruptedMidRun"),
        ],
        "D-SEAMS S1: ONE classification table. Every diagnostic code but \
         `WaitingForPod` comes out of D2's `waiting.rs`, and the rendering is the identity \
         except for the one D3 renames"
    );
    assert!(
        Code::from_check_code(CheckCode::DeadlineExceeded).is_none(),
        "the Job's own clock running out is the CONSEQUENCE, not the cause. \
         `crash_terminal_state` already records it, and a diagnostic naming it would bury \
         whatever the pod had been waiting for"
    );
    assert!(
        Code::from_check_code(CheckCode::BrokerUnreachable).is_none()
            && Code::from_check_code(CheckCode::Succeeded).is_none(),
        "the check catalogue's own codes are not pod-waiting classifications and never become \
         a runner diagnostic"
    );
}

/// D3 §2.3's severity and transience columns, row by row.
///
/// MUTANT: making `PodUnschedulable` non-transient makes D3 §15's L2 false —
/// the scenario asserts NO fail-fast patch is issued for it.
#[test]
fn the_severity_and_transience_columns_are_d3s() {
    // "never starts without a change" — Error, not transient, ever.
    for code in [
        Code::CredentialSecretNotFound,
        Code::CredentialSecretKeyMissing,
        Code::TrustBundleNotFound,
        Code::SigningKeyMissing,
        Code::RunnerImageNotPresent,
        Code::RunnerImageInvalid,
        Code::RunnerServiceAccountMissing,
        Code::PodCreateRejected,
    ] {
        assert_eq!(code.severity(), Severity::Error, "{code} is an Error");
        assert!(
            !code.transient(Duration::ZERO) && !code.transient(Duration::from_secs(86_400)),
            "{code} names a reference that does not exist or an image this node will not get; \
             no amount of waiting fixes either"
        );
    }
    // "may resolve on its own" — Warning.
    for code in [
        Code::PodUnschedulable,
        Code::RunnerImagePullFailed,
        Code::VolumeMountFailed,
        Code::WaitingForPod,
    ] {
        assert_eq!(code.severity(), Severity::Warning, "{code} is a Warning");
        assert!(code.transient(Duration::ZERO), "{code} starts transient");
    }
    // "already fatal".
    assert_eq!(Code::DisruptedMidRun.severity(), Severity::Error);
    assert!(!Code::DisruptedMidRun.transient(Duration::ZERO));
    // A node can join a cluster; a cluster autoscaler exists. FOREVER.
    assert!(
        Code::PodUnschedulable.transient(Duration::from_secs(86_400)),
        "D3 §15's L2: an unschedulable pod is reported and left to the Job's own deadline, and \
         NO fail-fast patch is issued for it"
    );
}

/// The one row whose class changes with time — D3 §2.3's "first 180 s".
#[test]
fn a_volume_that_has_not_mounted_in_three_minutes_is_not_mounting() {
    assert!(
        Code::VolumeMountFailed.transient(MOUNT_TRANSIENT_FOR - Duration::from_secs(1)),
        "inside the window it may still resolve"
    );
    assert!(
        !Code::VolumeMountFailed.transient(MOUNT_TRANSIENT_FOR),
        "at the boundary it is not transient any more — the boundary is CLOSED on the \
         non-transient side, so a fail-fast configured at exactly 180 s fires"
    );
    assert_eq!(MOUNT_TRANSIENT_FOR, Duration::from_secs(180));
}

/// The projection lands on D3 §2.2's six reasons and nothing else, and four of
/// them are terminal states.
#[test]
fn the_runner_ready_projection_is_onto_the_six_closed_reasons() {
    let mut reasons: Vec<&str> = Code::ALL
        .iter()
        .filter_map(|c| c.runner_ready_reason())
        .collect();
    reasons.sort_unstable();
    reasons.dedup();
    assert_eq!(
        reasons,
        vec![
            "CredentialReferenceMissing",
            "PodCreationForbidden",
            "PodUnschedulable",
            "RunnerImageUnavailable",
            "VolumeMountFailed",
            "WaitingForPod",
        ],
        "D3 §2.2 gives `RunnerReady=False` six reasons. The PARAMETERS — which Secret, which \
         volume — travel in the diagnostic's object and message, never in a condition reason"
    );
    // The terminal states are the projection, minus the one that can never be
    // a verdict.
    for code in Code::ALL {
        match terminal_state(&Diagnosis {
            code: *code,
            message: String::new(),
            object_kind: "Pod",
            object_name: POD.to_string(),
        }) {
            Some(state) => assert!(
                TERMINAL_STATES.contains(&state),
                "`{state}` is written as a condition reason on a terminal status, so it has to \
                 be in TERMINAL_STATES or the regex test never sees it"
            ),
            None => assert!(
                matches!(code, Code::WaitingForPod | Code::DisruptedMidRun),
                "{code} has no terminal state and should"
            ),
        }
    }
    assert_eq!(
        Code::SigningKeyMissing.runner_ready_reason(),
        Some("VolumeMountFailed"),
        "the signing key IS a volume. The diagnostic keeps the specific code — which is what an \
         operator acts on — and the condition carries the class"
    );
}

// ===========================================================================
// Sanitization — D3 §2.3
// ===========================================================================

/// Every rule in D3 §2.3's sanitizer, and the mutant for each.
#[test]
fn sanitize_removes_what_a_status_must_not_carry_and_keeps_what_it_must() {
    // A newline is how a reader is fooled into thinking two facts are one, and
    // how a forged key line would be smuggled onto a status.
    assert_eq!(
        sanitize("secret \"kafka-src\" not found\nreceipt-key=logweir/evil.json"),
        "secret \"kafka-src\" not found receipt-key=logweir/evil.json",
        "whitespace collapses, so nothing below the first line can pretend to be its own line"
    );
    assert!(
        !sanitize("a\nb").contains('\n'),
        "MUTANT: a sanitizer that only truncated would leave the newline"
    );
    // URL userinfo, query and fragment are credentials.
    assert_eq!(
        sanitize("dial https://AKIA:supersecret@minio.example/bucket?X-Amz-Signature=deadbeef#f"),
        "dial https://minio.example/bucket",
        "userinfo, the query string and the fragment all go; the host and path stay, because \
         an operator has to know which endpoint"
    );
    assert!(
        !sanitize("x://u:p@h/").contains("u:p"),
        "MUTANT: keeping userinfo"
    );
    // PEM.
    assert_eq!(
        sanitize("the webhook said: -----BEGIN PRIVATE KEY-----\nMIIE..."),
        "the webhook said:",
        "everything from a PEM header on is key material and not an explanation"
    );
    // Object names are KEPT — a diagnostic nobody can act on is no diagnostic.
    assert!(
        sanitize("couldn't find key access-key-id in Secret logweir-d3w2/kafka-src")
            .contains("kafka-src"),
        "the Secret's name is already a reference in the spec every viewer can read, and it is \
         the one thing that makes the diagnostic actionable"
    );
    // The bound, on a char boundary.
    let wide = "é".repeat(400);
    let out = sanitize(&wide);
    assert!(out.len() <= MESSAGE_MAX_BYTES, "bounded at 512 bytes");
    assert!(
        out.chars().all(|c| c == 'é'),
        "MUTANT: truncating at a byte index would split a two-byte char and produce invalid \
         UTF-8 — or, in Rust, panic"
    );
    assert_eq!(MESSAGE_MAX_BYTES, 512, "D3 §2.2's `maxLength`");
}

// ===========================================================================
// The progress channel — D3 §2.4 as ratified
// ===========================================================================

#[test]
fn the_progress_grammar_is_read_by_key_name_and_the_last_line_wins() {
    let p = parse_progress(
        "progress-contract=2\n\
         progress-phase=0:admit\n\
         some ordinary log line\n\
         progress-phase=6:restore\n",
    );
    assert_eq!(p.contract.as_deref(), Some("2"));
    assert_eq!(p.phase.as_ref().and_then(|p| p.number), Some(6));
    assert_eq!(
        p.phase.as_ref().and_then(|p| p.name.as_deref()),
        Some("restore")
    );
    assert_eq!(p.ignored, 0);
}

/// D3 §2.4's five failure modes, each an ABSENCE and never an error.
#[test]
fn an_unknown_overlong_or_absent_progress_line_is_ignored_and_never_a_failure() {
    // 1. An old runner: no contract line at all.
    let old = parse_progress("progress-phase=6:restore\nbackup finished\n");
    assert!(
        old.phase.is_none() && old.contract.is_none(),
        "D3 §2.4: an old runner (no `progress-contract=`) yields NO progress and no error. The \
         announcement is the gate — a phase line without it is a line from something that is \
         not the ratified channel"
    );
    // 2. A phase name outside the closed vocabulary.
    let unknown = parse_progress("progress-contract=2\nprogress-phase=6:hunter2\n");
    assert!(
        unknown.phase.is_none(),
        "the vocabulary is CLOSED. It is not a charset rule, because `hunter2` passes every \
         charset rule anyone would write — and this text would land on an object every viewer \
         of the namespace can read"
    );
    assert_eq!(unknown.ignored, 1, "and the line is COUNTED, not silent");
    // 3. The right name at the wrong number.
    let misnumbered = parse_progress("progress-contract=2\nprogress-phase=3:restore\n");
    assert!(
        misnumbered.phase.is_none(),
        "`restore` is phase 6. A name accepted at any number would let a runner claim to be \
         further along than it is"
    );
    // 4. Malformed: no colon, a non-numeric phase, an out-of-range number.
    for line in [
        "progress-phase=restore",
        "progress-phase=x:restore",
        "progress-phase=99:restore",
        "progress-phase=-2:admit",
        "progress-phase=",
    ] {
        let p = parse_progress(&format!("progress-contract=2\n{line}\n"));
        assert!(p.phase.is_none(), "`{line}` is not the grammar");
        assert_eq!(p.ignored, 1, "`{line}` is counted");
    }
    // 5. Overlong — beyond the runner's own per-line bound.
    let long = format!(
        "progress-contract=2\nprogress-phase=6:restore{}\n",
        "x".repeat(PROGRESS_LINE_MAX_BYTES)
    );
    let p = parse_progress(&long);
    assert!(
        p.phase.is_none() && p.ignored == 1,
        "a line longer than {PROGRESS_LINE_MAX_BYTES} bytes cannot have come from the runner's \
         formatter, so it is not parsed"
    );
}

/// Out of order is not an error: the LAST line is where the runner says it is.
#[test]
fn progress_lines_out_of_order_take_the_last_one() {
    let p = parse_progress(
        "progress-contract=2\nprogress-phase=7:verify\nprogress-phase=3:target-diff\n",
    );
    assert_eq!(
        p.phase.as_ref().and_then(|p| p.number),
        Some(3),
        "a runner that printed 3 after 7 has told us it is in 3. Reordering on the controller's \
         say-so would be the controller overruling the process"
    );
}

/// Both vocabularies, at their own numbers — W5's landed grammar.
#[test]
fn both_runners_closed_vocabularies_parse_and_nothing_else_does() {
    for (n, name) in [
        (0, "admit"),
        (1, "approval"),
        (2, "target-ready"),
        (3, "target-diff"),
        (4, "sample-select"),
        (5, "preflight"),
        (6, "restore"),
        (7, "verify"),
        (8, "score-and-sign"),
        (9, "teardown"),
    ] {
        let p = parse_progress(&format!("progress-contract=2\nprogress-phase={n}:{name}\n"));
        assert_eq!(
            p.phase.as_ref().and_then(|p| p.name.as_deref()),
            Some(name),
            "restore phase {n} is `{name}`"
        );
    }
    for step in ["admit", "engine", "readback", "sign", "upload"] {
        let p = parse_progress(&format!("progress-contract=2\nprogress-phase=-1:{step}\n"));
        assert_eq!(
            p.phase.as_ref().and_then(|p| p.number),
            Some(-1),
            "the backup path has no numbered phases after admission, so every step is at -1"
        );
        assert_eq!(p.phase.as_ref().and_then(|p| p.name.as_deref()), Some(step));
    }
    assert!(
        parse_progress("progress-contract=2\nprogress-phase=-1:verify\n")
            .phase
            .is_none(),
        "a restore phase name at the backup number is not in either vocabulary"
    );
}

/// D3 §2.5's verifying row, off the runner's own phase.
#[test]
fn the_verifying_stage_comes_from_the_runners_phase() {
    let running = pod(json!({
        "phase": "Running",
        "conditions": [{"type": "PodScheduled", "status": "True",
                        "lastTransitionTime": "2026-11-09T03:00:05Z"}],
        "containerStatuses": [
            {"name": "log-shipper", "ready": true, "restartCount": 0, "image": "x",
             "imageID": "x", "state": {"running": {"startedAt": "2026-11-09T03:00:10Z"}}},
            {"name": "runner", "ready": true, "restartCount": 0, "image": "x",
             "imageID": "x", "state": {"running": {"startedAt": "2026-11-09T03:00:12Z"}}},
        ],
    }));
    let j = job();
    let stage_for = |phase: Option<RunnerPhase>| {
        derive(&Facts {
            job: &j,
            pod: Some(&running),
            events: &[],
            runner_phase: phase.as_ref(),
            now: at(5, 0),
        })
        .stage
    };
    assert_eq!(
        stage_for(None),
        Stage::Running,
        "no phase is still `Running`"
    );
    assert_eq!(
        stage_for(Some(RunnerPhase {
            number: Some(6),
            name: Some("restore".into())
        })),
        Stage::Running
    );
    assert_eq!(
        stage_for(Some(RunnerPhase {
            number: Some(7),
            name: Some("verify".into())
        })),
        Stage::Verifying,
        "D3 §2.5: restore phase 7 is `Verifying`"
    );
    for step in ["readback", "sign", "upload"] {
        assert_eq!(
            stage_for(Some(RunnerPhase {
                number: Some(-1),
                name: Some(step.into())
            })),
            Stage::Verifying,
            "D3 §2.5: the backup step `{step}` is `Verifying`"
        );
    }
    assert_eq!(
        stage_for(Some(RunnerPhase {
            number: Some(-1),
            name: Some("engine".into())
        })),
        Stage::Running,
        "…and `engine` is the work itself"
    );
}

// ===========================================================================
// The derivation — D3 §13's PLAT-14.1 unit rows
// ===========================================================================

/// **D3 §13's "Mount failure" unit row.** A `FailedMount` event plus
/// `ContainerCreating` is `VolumeMountFailed`.
#[test]
fn a_failed_mount_event_with_container_creating_is_a_volume_mount_failure() {
    let p = waiting_pod("Pending", "True", "ContainerCreating", "");
    let events = [EventFact {
        reason: "FailedMount".to_string(),
        message: "MountVolume.SetUp failed for volume \"plan\": configmap \"x-plan\" not found"
            .to_string(),
        involved_kind: "Pod".to_string(),
        involved_name: POD.to_string(),
    }];
    let d = derive(&facts(Some(&p), &events, at(2, 0)));
    let found = d.diagnosis.as_ref().expect("a diagnosis");
    assert_eq!(found.code, Code::VolumeMountFailed);
    assert_eq!(found.object_kind, "Pod");
    assert_eq!(found.object_name, POD);
    assert!(
        found.message.contains("plan"),
        "the message names the volume; got {}",
        found.message
    );
    assert_eq!(d.runner_ready.status, "False");
    assert_eq!(d.runner_ready.reason, "VolumeMountFailed");
    assert_eq!(d.stage, Stage::Preparing, "the pod is scheduled");
    assert!(!d.started);
    // The grace period IS the rule: before it, the event is not believed.
    let early = derive(&facts(Some(&p), &events, at(0, 30)));
    assert!(
        early.diagnosis.is_none(),
        "a mount that is still being attempted is not yet a finding — D2's MOUNT_GRACE, and \
         this controller does not have a second grace period of its own"
    );
}

/// **D3 §13's "Unschedulable pod" unit row.** Reported after the grace, and
/// never fail-fast.
#[test]
fn an_unschedulable_pod_is_reported_after_the_grace_and_never_failed_fast() {
    let p = waiting_pod("Pending", "False", "ContainerCreating", "");
    let d = derive(&facts(Some(&p), &[], at(5, 0)));
    let found = d.diagnosis.as_ref().expect("a diagnosis");
    assert_eq!(found.code, Code::PodUnschedulable);
    assert_eq!(d.runner_ready.reason, "PodUnschedulable");
    assert_eq!(
        d.stage,
        Stage::Preparing,
        "D3 §2.5: an unscheduled pod WITH a failure code is `Preparing`; `Queued` is the \
         no-code case"
    );
    assert!(
        !should_fail_fast(
            d.diagnosis.as_ref(),
            d.started,
            Some(Duration::from_secs(86_400)),
            Duration::from_secs(300)
        ),
        "a day of being unschedulable still does not cancel the Job — D3 §15's L2"
    );
    // Before the grace, nothing.
    assert!(
        derive(&facts(Some(&p), &[], at(0, 30))).diagnosis.is_none(),
        "60 s of grace is D2's UNSCHEDULABLE_GRACE, shared"
    );
}

/// D3 §2.3's one addition to D2's table.
#[test]
fn a_job_with_no_pod_and_no_event_is_waiting_for_pod_after_sixty_seconds() {
    assert!(
        derive(&facts(None, &[], at(0, 30))).diagnosis.is_none(),
        "half a minute with no pod is a Job starting, not a Job stuck"
    );
    let d = derive(&facts(None, &[], at(1, 1)));
    let found = d.diagnosis.as_ref().expect("a diagnosis");
    assert_eq!(found.code, Code::WaitingForPod);
    assert_eq!(
        found.object_kind, "Job",
        "there is no pod to name, so the diagnostic is about the Job"
    );
    assert_eq!(d.stage, Stage::Queued);
    assert_eq!(d.runner_ready.reason, REASON_WAITING_FOR_POD);
    assert_eq!(WAITING_FOR_POD_GRACE, Duration::from_secs(60));
    assert!(
        terminal_state(found).is_none(),
        "\"nothing has happened yet\" can never be a verdict about how a run finished"
    );
}

/// The kubelet's own three message forms reach three different codes, and the
/// condition reason collapses them — which is the point of having both.
#[test]
fn the_three_credential_forms_keep_their_codes_and_share_one_condition_reason() {
    let cases = [
        (
            "secret \"kafka-src\" not found",
            Code::CredentialSecretNotFound,
        ),
        (
            "couldn't find key access-key-id in Secret logweir-d3w2/kafka-src",
            Code::CredentialSecretKeyMissing,
        ),
        (
            "configmap \"logweir-trust\" not found",
            Code::TrustBundleNotFound,
        ),
    ];
    for (message, expected) in cases {
        let p = waiting_pod("Pending", "True", "CreateContainerConfigError", message);
        let d = derive(&facts(Some(&p), &[], at(1, 0)));
        let found = d.diagnosis.as_ref().expect("a diagnosis");
        assert_eq!(found.code, expected, "`{message}`");
        assert_eq!(
            d.runner_ready.reason, "CredentialReferenceMissing",
            "all three are the same CLASS to a condition reader, and three different repairs to \
             an operator. D3 §15's L1 asserts this reason live"
        );
    }
}

/// A runner that started is `RunnerReady=True`, whatever else is true.
#[test]
fn a_runner_that_has_started_is_ready_and_is_never_failed_fast() {
    let terminated = pod(json!({
        "phase": "Failed",
        "conditions": [{"type": "PodScheduled", "status": "True",
                        "lastTransitionTime": "2026-11-09T03:00:05Z"}],
        "containerStatuses": [
            {"name": "log-shipper", "ready": false, "restartCount": 0, "image": "x",
             "imageID": "x", "state": {"terminated": {"exitCode": 0,
                                                      "finishedAt": "2026-11-09T03:05:00Z"}}},
            {"name": "runner", "ready": false, "restartCount": 0, "image": "x",
             "imageID": "x", "state": {"terminated": {"exitCode": 1,
                                                      "finishedAt": "2026-11-09T03:05:00Z"}}},
        ],
    }));
    let d = derive(&facts(Some(&terminated), &[], at(6, 0)));
    assert!(d.started);
    assert_eq!(d.runner_ready.status, "True");
    assert_eq!(d.runner_ready.reason, REASON_RUNNER_STARTED);
    assert!(
        !should_fail_fast(
            Some(&Diagnosis {
                code: Code::CredentialSecretNotFound,
                message: String::new(),
                object_kind: "Pod",
                object_name: POD.to_string(),
            }),
            true,
            Some(Duration::from_secs(86_400)),
            Duration::from_secs(60),
        ),
        "a run whose process began has consumed its approval and its plan. Fail-fast exists \
         because NOTHING ran; cancelling one that did is a different decision with a different \
         cost"
    );
}

/// The disrupted-node row wins over everything, and carries no `RunnerReady`
/// opinion.
#[test]
fn a_disrupted_pod_is_disrupted_mid_run_and_is_not_a_runner_ready_reason() {
    let p = pod(json!({
        "phase": "Pending",
        "conditions": [
            {"type": "DisruptionTarget", "status": "True",
             "lastTransitionTime": "2026-11-09T03:02:00Z"},
            {"type": "PodScheduled", "status": "True",
             "lastTransitionTime": "2026-11-09T03:00:05Z"},
        ],
        "containerStatuses": [
            {"name": "log-shipper", "ready": false, "restartCount": 0, "image": "x",
             "imageID": "x", "state": {"waiting": {"reason": "ContainerCreating"}}},
            {"name": "runner", "ready": false, "restartCount": 0, "image": "x",
             "imageID": "x", "state": {"waiting": {"reason": "ImagePullBackOff",
                                                   "message": "back-off pulling"}}},
        ],
    }));
    let d = derive(&facts(Some(&p), &[], at(3, 0)));
    assert_eq!(
        d.diagnosis.as_ref().map(|x| x.code),
        Some(Code::DisruptedMidRun),
        "the node going away explains the `ImagePullBackOff` too, and D2's table orders it \
         first for exactly that reason"
    );
    assert_eq!(
        d.runner_ready.reason, REASON_WAITING_FOR_POD,
        "`DisruptedMidRun` projects to no reason, so the condition falls back to the honest \
         \"it has not started and nothing here says why it never will\""
    );
}

// ===========================================================================
// The diagnostics list — D3 §2.3's dedup, order and bound
// ===========================================================================

fn diagnosis(code: Code, name: &str) -> Diagnosis {
    Diagnosis {
        code,
        message: format!("{code} on {name}"),
        object_kind: "Pod",
        object_name: name.to_string(),
    }
}

#[test]
fn a_repeated_diagnosis_is_one_entry_with_a_count_and_not_a_second_row() {
    let first = merge_diagnostics(
        None,
        Some(&diagnosis(Code::PodUnschedulable, POD)),
        at(1, 0),
    )
    .expect("one entry");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].count, Some(1));
    assert_eq!(first[0].first_seen, Some(at(1, 0)));
    // The SAME key, at the same heartbeat instant: nothing moves.
    let same = merge_diagnostics(
        Some(&first),
        Some(&diagnosis(Code::PodUnschedulable, POD)),
        at(1, 0),
    )
    .expect("one entry");
    assert_eq!(
        same, first,
        "a pass inside the heartbeat window recomputes byte-identical bytes, which is what \
         makes `status_unchanged` send no patch at all (E11(d))"
    );
    // A later heartbeat moves `lastSeen` and `count` TOGETHER, and never
    // `firstSeen` — which is what `held_for` reads.
    let later = merge_diagnostics(
        Some(&first),
        Some(&diagnosis(Code::PodUnschedulable, POD)),
        at(2, 0),
    )
    .expect("one entry");
    assert_eq!(later.len(), 1, "one key, one entry");
    assert_eq!(later[0].count, Some(2));
    assert_eq!(later[0].last_seen, Some(at(2, 0)));
    assert_eq!(
        later[0].first_seen,
        Some(at(1, 0)),
        "`firstSeen` NEVER moves while the key matches — it is how \"continuously\" in D3 \
         §2.3's fail-fast rule is measured"
    );
    // A different object is a different key.
    let two = merge_diagnostics(
        Some(&later),
        Some(&diagnosis(Code::PodUnschedulable, "other-pod")),
        at(3, 0),
    )
    .expect("two entries");
    assert_eq!(two.len(), 2);
    assert_eq!(
        two[0].object.as_ref().map(|o| o.name.as_str()),
        Some("other-pod"),
        "newest `lastSeen` first — D3 §2.3's order"
    );
}

#[test]
fn the_diagnostics_list_is_bounded_at_eight() {
    let mut list: Option<Vec<Diagnostic>> = None;
    for i in 0..12 {
        list = merge_diagnostics(
            list.as_ref(),
            Some(&diagnosis(Code::PodUnschedulable, &format!("pod-{i}"))),
            at(1, i),
        );
    }
    let list = list.expect("entries");
    assert_eq!(
        list.len(),
        DIAGNOSTICS_MAX,
        "a status is not a log. D3 §2.2's `maxItems: 8`, and the API server would refuse the \
         whole patch past it — so the truncation is not cosmetic"
    );
    assert_eq!(
        list[0].object.as_ref().map(|o| o.name.as_str()),
        Some("pod-11"),
        "and the eight kept are the NEWEST eight"
    );
}

/// The terminal state a crashed pass reads back off the recorded list.
#[test]
fn the_recorded_diagnostic_names_the_terminal_state_and_a_warning_never_does() {
    let stored = |code: Code| RunProgress {
        stage: Stage::Preparing.as_str().to_string(),
        reason: None,
        message: None,
        last_transition_time: None,
        last_observed_time: None,
        runner: None,
        runner_phase: None,
        diagnostics: merge_diagnostics(None, Some(&diagnosis(code, POD)), at(1, 0)),
    };
    assert_eq!(
        recorded_terminal_state(Some(&stored(Code::CredentialSecretNotFound))),
        Some("CredentialReferenceMissing"),
        "D3 §15's L1: a Backup whose archive secretRef names a missing Secret reaches \
         `Failed/CredentialReferenceMissing` with `exitCode` ABSENT"
    );
    assert_eq!(
        recorded_terminal_state(Some(&stored(Code::PodUnschedulable))),
        None,
        "a WARNING never becomes a verdict: an unschedulable pod's terminal state comes from \
         `crash_terminal_state` reading the pod, which is a stronger observation"
    );
    assert_eq!(
        recorded_terminal_state(Some(&stored(Code::WaitingForPod))),
        None
    );
    assert_eq!(
        recorded_terminal_state(None),
        None,
        "no record, no override"
    );
}

// ===========================================================================
// Fail fast — D3 §2.3
// ===========================================================================

#[test]
fn fail_fast_needs_all_four_of_its_conditions() {
    let d = diagnosis(Code::CredentialSecretNotFound, POD);
    let window = Duration::from_secs(300);
    assert!(
        should_fail_fast(Some(&d), false, Some(window), window),
        "non-transient, never started, held for the whole window — the one shape that cancels"
    );
    assert!(
        !should_fail_fast(None, false, Some(window), window),
        "no diagnosis, no cancellation"
    );
    assert!(
        !should_fail_fast(Some(&d), true, Some(window), window),
        "the runner started"
    );
    assert!(
        !should_fail_fast(
            Some(&d),
            false,
            Some(window - Duration::from_secs(1)),
            window
        ),
        "one second short of the window is not the window"
    );
    assert!(
        !should_fail_fast(Some(&d), false, None, window),
        "MUTANT: an unrecorded `firstSeen` must not read as \"for ever\". A diagnosis this \
         controller has not yet written down has been held for an UNKNOWN time, and cancelling \
         a Job on an unknown is the failure mode the whole grace exists to prevent"
    );
    assert!(
        !should_fail_fast(
            Some(&diagnosis(Code::RunnerImagePullFailed, POD)),
            false,
            Some(Duration::from_secs(86_400)),
            window
        ),
        "a transient code is left to the Job's own deadline"
    );
}

#[test]
fn held_for_reads_the_recorded_first_seen_and_a_new_key_starts_again() {
    let d = diagnosis(Code::CredentialSecretNotFound, POD);
    let stored = RunProgress {
        stage: Stage::Preparing.as_str().to_string(),
        reason: None,
        message: None,
        last_transition_time: None,
        last_observed_time: None,
        runner: None,
        runner_phase: None,
        diagnostics: merge_diagnostics(None, Some(&d), at(1, 0)),
    };
    assert_eq!(
        held_for(Some(&stored), &d, at(6, 0)),
        Some(Duration::from_secs(300))
    );
    assert_eq!(
        held_for(
            Some(&stored),
            &diagnosis(Code::CredentialSecretNotFound, "other"),
            at(6, 0)
        ),
        None,
        "a different object is a different key, so it has been held for no time at all"
    );
    assert_eq!(held_for(None, &d, at(6, 0)), None);
}

/// The installation policy's three rules, without touching the environment.
#[test]
fn the_configured_bounds_clamp_rather_than_refuse() {
    assert_eq!(
        bounded(None, FAIL_FAST_SECONDS_DEFAULT, FAIL_FAST_SECONDS_MIN),
        FAIL_FAST_SECONDS_DEFAULT,
        "unset is the default"
    );
    assert_eq!(
        bounded(Some("5m"), FAIL_FAST_SECONDS_DEFAULT, FAIL_FAST_SECONDS_MIN),
        FAIL_FAST_SECONDS_DEFAULT,
        "a typo in a ConfigMap must not become an outage"
    );
    assert_eq!(
        bounded(
            Some(" 900 "),
            FAIL_FAST_SECONDS_DEFAULT,
            FAIL_FAST_SECONDS_MIN
        ),
        900,
        "whitespace is trimmed"
    );
    assert_eq!(
        bounded(Some("1"), FAIL_FAST_SECONDS_DEFAULT, FAIL_FAST_SECONDS_MIN),
        FAIL_FAST_SECONDS_MIN,
        "MUTANT: a one-second fail-fast would cancel every Job whose image is still being \
         pulled on a cold node, so the floor is enforced rather than the value honoured"
    );
    assert_eq!(FAIL_FAST_SECONDS_DEFAULT, 300);
    assert_eq!(FAIL_FAST_SECONDS_MIN, 60);
}

// ===========================================================================
// The TTL repair — D3 §2.7
// ===========================================================================

/// A finished Job with `ttl` and `owners` as given.
fn finished_job(ttl: Option<i32>, owners: Value) -> Job {
    let mut spec = json!({"template": {"spec": {"containers": [], "restartPolicy": "Never"}}});
    if let Some(ttl) = ttl {
        spec["ttlSecondsAfterFinished"] = json!(ttl);
    }
    serde_json::from_value(json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": JOB, "namespace": NS, "uid": JOB_UID,
                     "ownerReferences": owners},
        "spec": spec,
        "status": {"conditions": [{"type": "Complete", "status": "True",
                                   "lastTransitionTime": "2026-11-09T03:05:00Z"}]},
    }))
    .expect("the Job fixture parses")
}

const OWNER_UID: &str = "backup-uid-2222";

fn owned() -> Value {
    json!([{"apiVersion": "logweir.dev/v1alpha1", "kind": "Backup", "name": "b",
            "uid": OWNER_UID, "controller": true, "blockOwnerDeletion": true}])
}

/// D3 §2.7's three conditions, each one on its own.
///
/// MUTANT (M8): dropping the owner check. On the reconcile path the
/// compatibility guard refuses a foreign Job BEFORE the repair is reached, so
/// a route-table row alone cannot tell whether this check exists — the belt
/// hides the braces. This is the braces.
#[test]
fn the_ttl_repair_needs_all_three_of_its_conditions() {
    assert!(
        weirkeeper::diagnostics::needs_ttl_repair(&finished_job(None, owned()), OWNER_UID),
        "finished, no TTL, controlled by this object"
    );
    assert!(
        !weirkeeper::diagnostics::needs_ttl_repair(
            &finished_job(Some(604_800), owned()),
            OWNER_UID
        ),
        "a Job that already has one is not repaired — re-sending the same value every reconcile \
         is the write loop E11(d) is about"
    );
    assert!(
        !weirkeeper::diagnostics::needs_ttl_repair(&job(), OWNER_UID),
        "an UNFINISHED Job gets no TTL: that would be a deadline it did not ask for"
    );
    assert!(
        !weirkeeper::diagnostics::needs_ttl_repair(
            &finished_job(
                None,
                json!([{"apiVersion": "logweir.dev/v1alpha1", "kind": "Backup",
                                        "name": "b", "uid": "somebody-elses-uid",
                                        "controller": true, "blockOwnerDeletion": true}])
            ),
            OWNER_UID
        ),
        "MUTANT: a Job's NAME proves nothing — it is derived, not owned. Patching a stranger's \
         Job with a TTL is deleting somebody else's work on a timer"
    );
    assert!(
        !weirkeeper::diagnostics::needs_ttl_repair(
            &finished_job(
                None,
                json!([{"apiVersion": "logweir.dev/v1alpha1", "kind": "Backup",
                                        "name": "b", "uid": OWNER_UID,
                                        "controller": false, "blockOwnerDeletion": true}])
            ),
            OWNER_UID
        ),
        "a NON-CONTROLLER owner reference is a reference, not control"
    );
    assert!(
        !weirkeeper::diagnostics::needs_ttl_repair(&finished_job(None, json!([])), OWNER_UID),
        "an ownerless Job is nobody's to collect"
    );
    assert!(
        !weirkeeper::diagnostics::needs_ttl_repair(&finished_job(None, owned()), ""),
        "and an object with no UID of its own proves nothing about anything — an empty owner \
         must never match an owner reference that also carries an empty uid"
    );
}

// ===========================================================================
// The status write — D3 §2.2
// ===========================================================================

fn write_for<'a>(
    derived: &'a weirkeeper::diagnostics::Derived,
    progress: &'a Progress,
    stored: Option<&'a RunProgress>,
    now: DateTime<Utc>,
    scalar_reason: bool,
) -> Write<'a> {
    Write {
        derived,
        progress,
        stored,
        conditions: None,
        generation: Some(3),
        scalar_reason,
        now,
    }
}

#[test]
fn the_progress_block_folds_into_a_builders_patch_without_replacing_its_conditions() {
    let p = waiting_pod(
        "Pending",
        "True",
        "ImagePullBackOff",
        "back-off pulling image",
    );
    let d = derive(&facts(Some(&p), &[], at(2, 0)));
    let base = json!({"status": {
        "phase": "Running",
        "jobRef": {"name": JOB},
        "conditions": [{"type": "JobCreated", "status": "True", "reason": "JobCreated"}],
    }});
    let out = apply(
        base,
        &write_for(&d, &Progress::default(), None, at(2, 0), false),
    );
    let conditions = out["status"]["conditions"]
        .as_array()
        .expect("an array")
        .clone();
    assert_eq!(
        conditions.len(),
        2,
        "the base builder's condition is KEPT and `RunnerReady` is added beside it. A merge \
         patch REPLACES arrays, so a second builder emitting its own array would delete the \
         first one's: {conditions:?}"
    );
    assert_eq!(conditions[0]["type"].as_str(), Some("JobCreated"));
    assert_eq!(conditions[1]["type"].as_str(), Some(CONDITION_RUNNER_READY));
    assert_eq!(
        conditions[1]["reason"].as_str(),
        Some("RunnerImageUnavailable")
    );
    assert_eq!(conditions[1]["observedGeneration"].as_i64(), Some(3));
    assert_eq!(
        out["status"]["phase"].as_str(),
        Some("Running"),
        "untouched"
    );
    assert_eq!(
        out["status"]["progress"]["stage"].as_str(),
        Some("Preparing")
    );
    assert_eq!(
        out["status"]["progress"]["diagnostics"][0]["code"].as_str(),
        Some("RunnerImagePullFailed"),
        "the DIAGNOSTIC keeps the specific code while the condition carries the class"
    );
    assert_eq!(
        out["status"]["progress"]["runner"]["waitingReason"].as_str(),
        Some("ImagePullBackOff"),
        "the kubelet's own reason, VERBATIM — a translated one is a reason nobody can search for"
    );
    assert!(
        out["status"]["reason"].is_null(),
        "a `Backup` has no REASON printer column and no `status.reason` field"
    );
}

#[test]
fn a_restore_publishes_the_runner_ready_reason_as_its_scalar_reason() {
    let p = waiting_pod(
        "Pending",
        "True",
        "CreateContainerConfigError",
        "secret \"logweir-approval-bundle\" not found",
    );
    let d = derive(&facts(Some(&p), &[], at(2, 0)));
    let base = json!({"status": {
        "phase": "Running",
        "reason": "JobCreated",
        "conditions": [{"type": "JobCreated", "status": "True", "reason": "JobCreated"}],
    }});
    let out = apply(
        base,
        &write_for(&d, &Progress::default(), None, at(2, 0), true),
    );
    assert_eq!(
        out["status"]["reason"].as_str(),
        Some("CredentialReferenceMissing"),
        "D3 §2.2: `Restore.status.reason` is the `RunnerReady` reason WHILE IT IS FALSE. The \
         REASON column answering `JobCreated` for the four minutes a pod spends unable to mount \
         its approval bundle is exactly review finding M2's defect, one layer down"
    );
    // …and it goes back to the base builder's answer once the runner starts.
    let running = pod(json!({
        "phase": "Running",
        "conditions": [{"type": "PodScheduled", "status": "True",
                        "lastTransitionTime": "2026-11-09T03:00:05Z"}],
        "containerStatuses": [
            {"name": "runner", "ready": true, "restartCount": 0, "image": "x", "imageID": "x",
             "state": {"running": {"startedAt": "2026-11-09T03:00:12Z"}}},
        ],
    }));
    let started = derive(&facts(Some(&running), &[], at(2, 0)));
    let out = apply(
        json!({"status": {"reason": "JobCreated", "conditions": [
            {"type": "JobCreated", "status": "True", "reason": "JobCreated"}]}}),
        &write_for(&started, &Progress::default(), None, at(2, 0), true),
    );
    assert_eq!(out["status"]["reason"].as_str(), Some("JobCreated"));
}

/// E11(d)'s two timestamp rules.
#[test]
fn the_two_progress_timestamps_move_only_when_they_should() {
    let p = waiting_pod("Pending", "True", "ImagePullBackOff", "back-off");
    let d = derive(&facts(Some(&p), &[], at(2, 0)));
    let base = || json!({"status": {"conditions": []}});
    let first = apply(
        base(),
        &write_for(&d, &Progress::default(), None, at(2, 0), false),
    );
    let stored: RunProgress = serde_json::from_value(first["status"]["progress"].clone())
        .expect("the block round-trips through the CRD type");
    // Inside the heartbeat: both timestamps are the stored ones, so the whole
    // patch is byte-identical and nothing is sent.
    let inside = apply(
        base(),
        &write_for(&d, &Progress::default(), Some(&stored), at(2, 30), false),
    );
    assert_eq!(
        inside["status"]["progress"], first["status"]["progress"],
        "30 s later, nothing has changed, so the computed block is byte-identical — which is \
         what `status_unchanged` turns into no API write at all"
    );
    // After the heartbeat: `lastObservedTime` moves and NOTHING else does.
    let after = apply(
        base(),
        &write_for(
            &d,
            &Progress::default(),
            Some(&stored),
            at(2, 0) + chrono::Duration::seconds(61),
            false,
        ),
    );
    assert_ne!(
        after["status"]["progress"]["lastObservedTime"],
        first["status"]["progress"]["lastObservedTime"]
    );
    assert_eq!(
        after["status"]["progress"]["lastTransitionTime"],
        first["status"]["progress"]["lastTransitionTime"],
        "the stage and the reason did not change, so the transition time did not either"
    );
    assert_eq!(OBSERVED_HEARTBEAT, Duration::from_secs(60));
    // A stage change DOES move it.
    let running = pod(json!({
        "phase": "Running",
        "conditions": [{"type": "PodScheduled", "status": "True",
                        "lastTransitionTime": "2026-11-09T03:00:05Z"}],
        "containerStatuses": [
            {"name": "runner", "ready": true, "restartCount": 0, "image": "x", "imageID": "x",
             "state": {"running": {"startedAt": "2026-11-09T03:02:30Z"}}},
        ],
    }));
    let moved = derive(&facts(Some(&running), &[], at(2, 40)));
    let out = apply(
        base(),
        &write_for(
            &moved,
            &Progress::default(),
            Some(&stored),
            at(2, 40),
            false,
        ),
    );
    assert_eq!(out["status"]["progress"]["stage"].as_str(), Some("Running"));
    assert_ne!(
        out["status"]["progress"]["lastTransitionTime"],
        first["status"]["progress"]["lastTransitionTime"],
        "the stage moved, so the transition time did"
    );
}

/// A terminal patch says the run is over, and says it with an explicit `null`.
#[test]
fn a_terminal_patch_finishes_the_progress_block_and_clears_the_heartbeat() {
    let base = json!({"status": {
        "phase": "Failed",
        "conditions": [{"type": "Failed", "status": "True", "reason": "DrillNotPass",
                        "message": "the runner exited 2"}],
    }});
    let out = apply_finished(base, None, at(9, 0));
    assert_eq!(
        out["status"]["progress"]["stage"].as_str(),
        Some("Finished")
    );
    assert_eq!(
        out["status"]["progress"]["reason"].as_str(),
        Some("DrillNotPass"),
        "the progress block cannot disagree with the condition it is about: the reason is read \
         out of the patch's own terminal condition"
    );
    // AN EXPLICIT null, NOT AN OMISSION — and the assertion has to say so by
    // looking the key UP, because `value["absent"]` is itself `Null` in
    // `serde_json` and an `is_null()` check alone passes for a key that was
    // never written. A merge patch that omitted this key would leave `last
    // observed at 03:02` on a run that is over, and D3 §2.5's staleness row
    // would then call a finished run `unknown` five minutes later.
    let block = out["status"]["progress"]
        .as_object()
        .expect("the progress block is an object");
    assert!(
        block.contains_key("lastObservedTime"),
        "the key is PRESENT in the patch: {block:?}"
    );
    assert!(
        block["lastObservedTime"].is_null(),
        "…and its value is null, which is RFC 7386 for `delete this field`: {block:?}"
    );
    assert!(
        out["status"]["progress"].get("runner").is_none()
            && out["status"]["progress"].get("diagnostics").is_none(),
        "the runner facts and the diagnostics are OMITTED, which in RFC 7386 means \"leave them \
         alone\" — they are exactly what an operator looking at a failed run needs, and \
         re-deriving them from a pod that may already be collected is what step 2b's guard \
         exists to prevent"
    );
    assert_eq!(
        out["status"]["phase"].as_str(),
        Some("Failed"),
        "nothing else in the builder's patch is touched"
    );
}

/// The throttle, and what a throttled pass publishes.
#[test]
fn the_progress_read_is_throttled_and_a_throttled_pass_carries_the_stored_phase() {
    let stored = RunProgress {
        stage: Stage::Running.as_str().to_string(),
        reason: Some(REASON_RUNNER_STARTED.to_string()),
        message: None,
        last_transition_time: Some(at(1, 0)),
        last_observed_time: Some(at(1, 0)),
        runner: None,
        runner_phase: Some(RunnerPhase {
            number: Some(6),
            name: Some("restore".into()),
        }),
        diagnostics: None,
    };
    assert!(
        !should_read_progress(Some(&stored), true, at(1, 20)),
        "20 s after the last observation the log is not read again"
    );
    assert!(
        should_read_progress(Some(&stored), true, at(1, 40)),
        "…and 40 s after it is"
    );
    assert!(
        !should_read_progress(Some(&stored), false, at(9, 0)),
        "a runner that has not started has printed nothing, so there is nothing to read"
    );
    assert!(
        should_read_progress(None, true, at(1, 0)),
        "an object with no record yet is read once"
    );
    let carried = Progress::carried(Some(&stored));
    assert_eq!(
        carried.phase.as_ref().and_then(|p| p.number),
        Some(6),
        "a throttled pass republishes the phase the object already carries. `apply` writes the \
         whole progress object, so a carried phase is the difference between \"still in phase \
         6\" and a field that blinks out every other reconcile"
    );
}

/// `RunnerPhase` is published only when the ratified channel announced itself.
#[test]
fn no_contract_line_means_no_runner_phase_on_the_status() {
    let p = pod(json!({
        "phase": "Running",
        "conditions": [{"type": "PodScheduled", "status": "True",
                        "lastTransitionTime": "2026-11-09T03:00:05Z"}],
        "containerStatuses": [
            {"name": "runner", "ready": true, "restartCount": 0, "image": "x", "imageID": "x",
             "state": {"running": {"startedAt": "2026-11-09T03:00:12Z"}}},
        ],
    }));
    let d = derive(&facts(Some(&p), &[], at(2, 0)));
    let old_runner = parse_progress("progress-phase=6:restore\n");
    let out = apply(
        json!({"status": {"conditions": []}}),
        &write_for(&d, &old_runner, None, at(2, 0), false),
    );
    assert!(
        out["status"]["progress"]["runnerPhase"].is_null(),
        "an old runner yields no progress and no error — D3 §2.4"
    );
    let v2 = parse_progress("progress-contract=2\nprogress-phase=6:restore\n");
    let out = apply(
        json!({"status": {"conditions": []}}),
        &write_for(&d, &v2, None, at(2, 0), false),
    );
    assert_eq!(
        out["status"]["progress"]["runnerPhase"]["name"].as_str(),
        Some("restore")
    );
}

/// The whole block round-trips through the shipped CRD types, which is what
/// makes it a status the API server will accept rather than a JSON blob.
#[test]
fn the_written_progress_block_is_the_crd_type() {
    let p = waiting_pod("Pending", "True", "ImagePullBackOff", "back-off pulling");
    let d = derive(&facts(Some(&p), &[], at(2, 0)));
    let out = apply(
        json!({"status": {"conditions": []}}),
        &write_for(
            &d,
            &parse_progress("progress-contract=2\nprogress-phase=-1:engine\n"),
            None,
            at(2, 0),
            false,
        ),
    );
    let block: RunProgress = serde_json::from_value(out["status"]["progress"].clone())
        .expect("`status.progress` deserialises as the CRD's own `RunProgress`");
    assert_eq!(block.stage, "Preparing");
    assert_eq!(
        block.runner.as_ref().and_then(|r| r.pod_phase.as_deref()),
        Some("Pending")
    );
    assert_eq!(block.diagnostics.as_ref().map(Vec::len), Some(1));
    let conditions: Vec<Condition> =
        serde_json::from_value(out["status"]["conditions"].clone()).expect("conditions");
    assert!(conditions
        .iter()
        .any(|c| c.r#type == CONDITION_RUNNER_READY));
    // And a diagnostic the CRD would refuse never reaches it.
    let d0: &Diagnostic = &block.diagnostics.as_ref().expect("one")[0];
    assert!(d0.message.as_ref().is_none_or(|m| m.len() <= 512));
    assert_eq!(
        d0.object
            .as_ref()
            .map(|o: &DiagnosticObject| o.kind.as_str()),
        Some("Pod"),
        "`kind` is a closed enum of `Pod` and `Job` on the CRD"
    );
}
