//! Backup/Restore status → the product operation model, for every status the
//! current controller can write.
//!
//! THE STATUSES ARE THE CONTROLLER'S OWN. Rather than hand-written JSON, the
//! matrix below drives `weirkeeper`'s public status-patch builders —
//! `running_status_patch`, `finished_status_patch`, `crashed_status_patch`,
//! `refused_status_patch`, `admission_hold_patch`,
//! `approval_bundle_hold_patch` — and the verification second patch built the
//! way the reconcilers build it (`backup_badge`/`restore_badge`,
//! `verified_condition`, `second_patch`), applies them as merge patches, and
//! maps the result. The UI fixtures under `ui/tests/fixtures` are mapped too.
//!
//! THE INVARIANT CHECKED ON EVERY ROW: `verifiedSuccess` is true exactly when
//! the controller's own badge is green AND the state is `succeeded`, and never
//! with a verification state other than `valid`. `NotAttempted` is never
//! verified success.

mod support;

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use logweir_api::contract::{Operation, OperationState, ResultStatus, VerificationState};
use logweir_api::status::{backup_operation, restore_operation};
use serde_json::{json, Value};
use weirkeeper::conditions::apply_merge_patch;
use weirkeeper::controllers::backup::{self as backup_ctl, EvidenceKeys};
use weirkeeper::controllers::restore::{
    self as restore_ctl, RestoreAdmission, RestoreEvidenceKeys, ScorecardObservation,
};
use weirkeeper::crds::backup::Backup;
use weirkeeper::crds::restore::Restore;
use weirkeeper::verification::{
    backup_badge, conditions_in, restore_badge, second_patch, verified_condition,
    VerificationResult, VerificationVerdict,
};

fn now() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-09-15T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc)
}

fn base_backup() -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "metadata": {"name": "b1", "namespace": "team-a", "uid": "uid-b1", "resourceVersion": "7", "generation": 1},
        "spec": {"sourceRef": {"name": "source"}, "topics": ["orders"], "archive": {"url": "s3://b/p"}, "triggeredBy": "manual", "deadlineSeconds": 1800}
    })
}

fn base_restore() -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Restore",
        "metadata": {"name": "r1", "namespace": "team-a", "uid": "uid-r1", "resourceVersion": "9", "generation": 1},
        "spec": {
            "planBytes": "name: x\n", "approvalRef": {"name": "a1"}, "sourceArchive": {"url": "s3://b/p"},
            "backupSetRef": "set", "pointInTime": "2026-09-07T14:05:00Z",
            "target": {"clusterRef": {"name": "target"}, "mode": "newTopic", "topicNaming": {"prefix": "r-"}},
            "deadlineSeconds": 3600
        }
    })
}

fn patched<T: serde::de::DeserializeOwned>(mut object: Value, patch: &Value) -> (Value, T) {
    apply_merge_patch(&mut object, patch);
    let typed = serde_json::from_value(object.clone()).expect("the patched object deserialises");
    (object, typed)
}

fn backup_of(v: &Value) -> Backup {
    serde_json::from_value(v.clone()).unwrap()
}

fn restore_of(v: &Value) -> Restore {
    serde_json::from_value(v.clone()).unwrap()
}

fn verdict(result: VerificationVerdict, payload_type: &str) -> VerificationResult {
    VerificationResult {
        matched_key_id: matches!(result, VerificationVerdict::Valid).then(|| "key-1".to_string()),
        result,
        payload_type: payload_type.to_string(),
        verified_at: now(),
        detail: (!matches!(result, VerificationVerdict::Valid)).then(|| "a reason".to_string()),
        // PLAT-19.1: these fixtures model the three verdicts the API has
        // always normalised. The trust projection is the signer's lifecycle,
        // which this mapping does not read.
        trust: None,
    }
}

/// The Backup reconciler's second patch, built the way it builds it.
fn backup_second(object: &Value, terminal: &Value, result: VerificationVerdict) -> Value {
    let typed = backup_of(object);
    let block =
        verdict(result, "application/vnd.logweir.backup-receipt+json").to_status_value(None);
    let mut projected = terminal.pointer("/status").cloned().unwrap();
    projected["evidence"] = json!({"verification": block.clone()});
    let badge = backup_badge(&projected);
    let verified = verified_condition(&badge, None, typed.metadata.generation, now());
    second_patch(&conditions_in(terminal), verified, block)
}

fn restore_second(object: &Value, terminal: &Value, result: VerificationVerdict) -> Value {
    let typed = restore_of(object);
    let block = verdict(result, "application/vnd.logweir.scorecard+json").to_status_value(None);
    let mut projected = terminal.pointer("/status").cloned().unwrap();
    projected["evidence"] = json!({"verification": block.clone()});
    let badge = restore_badge(&projected);
    let verified = verified_condition(&badge, None, typed.metadata.generation, now());
    second_patch(&conditions_in(terminal), verified, block)
}

struct Expect {
    state: OperationState,
    reason: Option<&'static str>,
    result: ResultStatus,
    verification: VerificationState,
    verified_success: bool,
}

fn check(label: &str, op: &Operation, status: &Value, green: bool, expect: &Expect) {
    assert_eq!(op.state, expect.state, "{label}: state");
    if let Some(reason) = expect.reason {
        assert_eq!(
            op.state_reason.as_deref(),
            Some(reason),
            "{label}: stateReason"
        );
    }
    assert_eq!(op.result.status, expect.result, "{label}: result");
    assert_eq!(
        op.verification.state, expect.verification,
        "{label}: verification"
    );
    assert_eq!(
        op.verified_success, expect.verified_success,
        "{label}: verifiedSuccess"
    );
    assert_eq!(op.terminal, expect.state.is_terminal(), "{label}: terminal");
    // The invariant.
    assert_eq!(
        op.verified_success,
        green && op.state == OperationState::Succeeded,
        "{label}: verifiedSuccess must equal the controller's badge on a succeeded run; status {status}"
    );
    if op.verified_success {
        assert_eq!(op.verification.state, VerificationState::Valid, "{label}");
    }
    // JSON shape: result and verification are separate objects.
    let v = serde_json::to_value(op).unwrap();
    assert!(
        v["result"].is_object() && v["verification"].is_object(),
        "{label}"
    );
}

fn e(
    state: OperationState,
    reason: Option<&'static str>,
    result: ResultStatus,
    verification: VerificationState,
    verified_success: bool,
) -> Expect {
    Expect {
        state,
        reason,
        result,
        verification,
        verified_success,
    }
}

fn run_backup(label: &str, object: &Value, expect: Expect) {
    let typed = backup_of(object);
    let op = backup_operation(&typed);
    let status = object.get("status").cloned().unwrap_or(json!({}));
    let green = backup_badge(&status).green;
    check(label, &op, &status, green, &expect);
}

fn run_restore(label: &str, object: &Value, expect: Expect) {
    let typed = restore_of(object);
    let op = restore_operation(&typed);
    let status = object.get("status").cloned().unwrap_or(json!({}));
    let green = restore_badge(&status).green;
    check(label, &op, &status, green, &expect);
}

use OperationState as S;
use ResultStatus as R;
use VerificationState as V;

#[test]
fn every_backup_status_the_controller_writes() {
    let base = base_backup();
    let typed = backup_of(&base);
    run_backup(
        "no status",
        &base,
        e(
            S::Pending,
            Some("AwaitingController"),
            R::Pending,
            V::Pending,
            false,
        ),
    );

    let (running, _) = patched::<Backup>(
        base.clone(),
        &backup_ctl::running_status_patch(&typed, "b1", now()),
    );
    run_backup(
        "running",
        &running,
        e(
            S::Running,
            Some("JobCreated"),
            R::Pending,
            V::Pending,
            false,
        ),
    );

    let keys = EvidenceKeys {
        receipt: Some("logweir/backups/b1/r.receipt.json".into()),
        sidecar: Some("logweir/backups/b1/r.receipt.sig".into()),
    };
    let digest = Some("sha256:00ff");
    let no_keys = EvidenceKeys::default();

    // Exit 0 with complete evidence: verifying, then each verdict.
    let terminal = backup_ctl::finished_status_patch(
        &backup_of(&running),
        0,
        &keys,
        None,
        None,
        Some((1, 2)),
        digest,
        now(),
    );
    let (finished, _) = patched::<Backup>(running.clone(), &terminal);
    run_backup(
        "exit 0 unverified",
        &finished,
        e(
            S::Verifying,
            Some("EvidenceVerificationPending"),
            R::Pass,
            V::Pending,
            false,
        ),
    );
    for (verdict_value, v, success) in [
        (VerificationVerdict::Valid, V::Valid, true),
        (VerificationVerdict::Invalid, V::Invalid, false),
        (VerificationVerdict::NotAttempted, V::NotAttempted, false),
    ] {
        let (done, _) = patched::<Backup>(
            finished.clone(),
            &backup_second(&finished, &terminal, verdict_value),
        );
        run_backup(
            &format!("exit 0 {v:?}"),
            &done,
            e(S::Succeeded, Some("Ok"), R::Pass, v, success),
        );
    }

    // Exit 0 whose keys were unreadable: nothing to verify.
    let terminal = backup_ctl::finished_status_patch(
        &backup_of(&running),
        0,
        &no_keys,
        None,
        None,
        None,
        None,
        now(),
    );
    let (unreadable, _) = patched::<Backup>(running.clone(), &terminal);
    run_backup(
        "exit 0 no keys",
        &unreadable,
        e(S::Succeeded, Some("Ok"), R::Pass, V::NoEvidence, false),
    );

    // Exit 0 with keys but no digest: the controller does not verify.
    let terminal = backup_ctl::finished_status_patch(
        &backup_of(&running),
        0,
        &keys,
        None,
        None,
        None,
        None,
        now(),
    );
    let (no_digest, _) = patched::<Backup>(running.clone(), &terminal);
    run_backup(
        "exit 0 no digest",
        &no_digest,
        e(S::Succeeded, Some("Ok"), R::Pass, V::NotAttempted, false),
    );

    // Exit 2 (signed, not a pass).
    let terminal = backup_ctl::finished_status_patch(
        &backup_of(&running),
        2,
        &keys,
        None,
        None,
        None,
        digest,
        now(),
    );
    let (exit2, _) = patched::<Backup>(running.clone(), &terminal);
    run_backup(
        "exit 2 unverified",
        &exit2,
        e(S::Verifying, None, R::NotPass, V::Pending, false),
    );
    let (exit2_valid, _) = patched::<Backup>(
        exit2.clone(),
        &backup_second(&exit2, &terminal, VerificationVerdict::Valid),
    );
    run_backup(
        "exit 2 valid",
        &exit2_valid,
        e(S::Failed, Some("DrillNotPass"), R::NotPass, V::Valid, false),
    );

    // Exits 1, 3, 4 and an out-of-contract code: no artifact.
    // Exit code, the runner's refusal state, the orphan state, and what the
    // three of them together mean in the product model.
    type ExitCase = (
        i32,
        Option<&'static str>,
        Option<&'static str>,
        S,
        R,
        &'static str,
    );
    let cases: [ExitCase; 4] = [
        (1, None, None, S::Failed, R::Error, "Operational"),
        (
            3,
            Some("TargetTopicConfigRefused"),
            None,
            S::Refused,
            R::Refused,
            "GuardRefused",
        ),
        (
            4,
            None,
            Some("OrphanedScorecard"),
            S::Failed,
            R::Error,
            "SigningOrLock",
        ),
        (137, None, None, S::Failed, R::Error, "Operational"),
    ];
    for (code, refusal, orphan, state, result, reason) in cases {
        let terminal = backup_ctl::finished_status_patch(
            &backup_of(&running),
            code,
            &no_keys,
            refusal,
            orphan,
            None,
            None,
            now(),
        );
        let (object, _) = patched::<Backup>(running.clone(), &terminal);
        run_backup(
            &format!("exit {code}"),
            &object,
            e(state, Some(reason), result, V::NoEvidence, false),
        );
        let op = backup_operation(&backup_of(&object));
        assert_eq!(op.result.exit_code, Some(code));
        if let Some(r) = refusal.or(orphan) {
            assert_eq!(op.result.exit_reason.as_deref(), Some(r));
        }
    }

    // Crashed Jobs: no exit code, a failure, the Job named.
    for state in ["NoExitCode", "PodUnschedulable", "DisruptedMidDrill"] {
        let (object, _) = patched::<Backup>(
            running.clone(),
            &backup_ctl::crashed_status_patch(&backup_of(&running), state, "b1", now()),
        );
        run_backup(
            state,
            &object,
            e(S::Failed, Some(state), R::Error, V::NoEvidence, false),
        );
        assert_eq!(backup_operation(&backup_of(&object)).result.exit_code, None);
    }

    // Controller refusals before any Job.
    for state in [
        "NameTooLong",
        "ReferentNotFound",
        "ArchiveUrlUnreadable",
        "PlanConfigMapConflict",
        "CredentialNotRenderable",
        "GuardRefused",
        "JobNameConflict",
    ] {
        let (object, _) = patched::<Backup>(
            base.clone(),
            &backup_ctl::refused_status_patch(&typed, state, "refused", now()),
        );
        run_backup(
            state,
            &object,
            e(S::Refused, Some(state), R::Refused, V::NoEvidence, false),
        );
        let op = backup_operation(&backup_of(&object));
        assert_eq!(op.message.as_deref(), Some("refused"));
    }

    // A phase this build does not know.
    let mut unknown = base.clone();
    unknown["status"] = json!({"phase": "Exploded"});
    run_backup(
        "unknown phase",
        &unknown,
        e(
            S::Unknown,
            Some("UnrecognizedPhase"),
            R::Unknown,
            V::Pending,
            false,
        ),
    );
}

#[test]
fn every_restore_status_the_controller_writes() {
    let base = base_restore();
    let typed = restore_of(&base);
    run_restore(
        "no status",
        &base,
        e(
            S::Pending,
            Some("AwaitingController"),
            R::Pending,
            V::Pending,
            false,
        ),
    );

    let hold = restore_ctl::admission_hold_patch(
        &typed,
        &RestoreAdmission::ApprovalNotVerified {
            approval: "a1".into(),
        },
        now(),
    );
    let (held, _) = patched::<Restore>(base.clone(), &hold);
    run_restore(
        "approval hold",
        &held,
        e(
            S::Pending,
            Some("ApprovalNotVerified"),
            R::Pending,
            V::Pending,
            false,
        ),
    );

    let (preparing, _) = patched::<Restore>(
        base.clone(),
        &restore_ctl::approval_bundle_hold_patch(&typed, "retrying", now()),
    );
    run_restore(
        "bundle hold",
        &preparing,
        e(
            S::Preparing,
            Some("ApprovalBundleMaterializationFailed"),
            R::Pending,
            V::Pending,
            false,
        ),
    );

    for (admission, state) in [
        (
            RestoreAdmission::ApprovalNotReceived {
                approval: "a1".into(),
            },
            "ApprovalNotReceived",
        ),
        (
            RestoreAdmission::PlanHashMismatch {
                recomputed: "sha256:1".into(),
                approval_says: "sha256:2".into(),
            },
            "PlanHashMismatch",
        ),
        (
            RestoreAdmission::ClusterNotReachable {
                cluster: "target".into(),
            },
            "ClusterNotReachable",
        ),
        (
            RestoreAdmission::ApprovalSubjectMismatch {
                approval: "a1".into(),
                detail: "uid".into(),
            },
            "ApprovalSubjectMismatch",
        ),
    ] {
        let patch = restore_ctl::refused_status_patch(
            &typed,
            admission.reason(),
            &admission.to_string(),
            now(),
        );
        let (object, _) = patched::<Restore>(base.clone(), &patch);
        run_restore(
            state,
            &object,
            e(S::Refused, Some(state), R::Refused, V::NoEvidence, false),
        );
    }
    for state in [
        "NameTooLong",
        "JobNameConflict",
        "ApprovalBundleConflict",
        "PlanConfigMapConflict",
        "ReferentNotFound",
    ] {
        let (object, _) = patched::<Restore>(
            base.clone(),
            &restore_ctl::refused_status_patch(&typed, state, "refused", now()),
        );
        run_restore(
            state,
            &object,
            e(S::Refused, Some(state), R::Refused, V::NoEvidence, false),
        );
    }

    let (running, _) = patched::<Restore>(
        held.clone(),
        &restore_ctl::running_status_patch(&restore_of(&held), "r1", true, now()),
    );
    run_restore(
        "running",
        &running,
        e(
            S::Running,
            Some("JobCreated"),
            R::Pending,
            V::Pending,
            false,
        ),
    );

    let keys = RestoreEvidenceKeys {
        scorecard: Some("logweir/drills/x.json".into()),
        sidecar: Some("logweir/drills/x.json.sig".into()),
        offset_report: Some("logweir/drills/x.offsets.json".into()),
    };
    let observed = |outcome: &str| ScorecardObservation {
        outcome: Some(outcome.to_string()),
        last_phase_completed: Some(9),
        integrity_level: Some("byte-fingerprint".into()),
        integrity_result: Some(if outcome == "pass" { "pass" } else { "fail" }.into()),
        scorecard_sha256: Some("sha256:abcd".into()),
        offset_report_sha256: Some("sha256:ef01".into()),
        ..ScorecardObservation::default()
    };

    // Exit 0, outcome pass: verifying, then each verdict. The decision's own
    // example is here: pass + NotAttempted is NOT verified success.
    let pass = observed("pass");
    let terminal = restore_ctl::finished_status_patch(
        &restore_of(&running),
        0,
        &keys,
        None,
        Some(&pass),
        None,
        None,
        now(),
    );
    let (finished, _) = patched::<Restore>(running.clone(), &terminal);
    run_restore(
        "pass unverified",
        &finished,
        e(
            S::Verifying,
            Some("EvidenceVerificationPending"),
            R::Pass,
            V::Pending,
            false,
        ),
    );
    for (verdict_value, v, success) in [
        (VerificationVerdict::Valid, V::Valid, true),
        (VerificationVerdict::Invalid, V::Invalid, false),
        (VerificationVerdict::NotAttempted, V::NotAttempted, false),
    ] {
        let (done, _) = patched::<Restore>(
            finished.clone(),
            &restore_second(&finished, &terminal, verdict_value),
        );
        run_restore(
            &format!("pass {v:?}"),
            &done,
            e(S::Succeeded, Some("Ok"), R::Pass, v, success),
        );
        let op = restore_operation(&restore_of(&done));
        assert_eq!(op.result.outcome.as_deref(), Some("pass"));
        assert_eq!(op.result.last_phase_completed, Some(9));
        assert_eq!(
            op.evidence.offset_report_key.as_deref(),
            Some("logweir/drills/x.offsets.json")
        );
    }

    // Exit 2 with a failing outcome, verified Valid: a valid document saying
    // the restore did not reconcile is not green.
    for outcome in ["fail-integrity", "fail-objective"] {
        let o = observed(outcome);
        let terminal = restore_ctl::finished_status_patch(
            &restore_of(&running),
            2,
            &keys,
            None,
            Some(&o),
            None,
            None,
            now(),
        );
        let (object, _) = patched::<Restore>(running.clone(), &terminal);
        run_restore(
            outcome,
            &object,
            e(S::Verifying, None, R::NotPass, V::Pending, false),
        );
        let (done, _) = patched::<Restore>(
            object.clone(),
            &restore_second(&object, &terminal, VerificationVerdict::Valid),
        );
        run_restore(
            &format!("{outcome} valid"),
            &done,
            e(S::Failed, Some("DrillNotPass"), R::NotPass, V::Valid, false),
        );
    }

    // Exit 0 whose scorecard was not observed: keys without a digest.
    let terminal = restore_ctl::finished_status_patch(
        &restore_of(&running),
        0,
        &keys,
        None,
        None,
        None,
        None,
        now(),
    );
    let (unobserved, _) = patched::<Restore>(running.clone(), &terminal);
    run_restore(
        "pass unobserved",
        &unobserved,
        e(S::Succeeded, Some("Ok"), R::Pass, V::NotAttempted, false),
    );

    // Exits 1, 3, 4.
    for (code, refusal, state, result) in [
        (1, None, S::Failed, R::Error),
        (3, Some("WindowNotCovered"), S::Refused, R::Refused),
        (4, None, S::Failed, R::Error),
    ] {
        let terminal = restore_ctl::finished_status_patch(
            &restore_of(&running),
            code,
            &RestoreEvidenceKeys::default(),
            refusal,
            None,
            None,
            None,
            now(),
        );
        let (object, _) = patched::<Restore>(running.clone(), &terminal);
        run_restore(
            &format!("exit {code}"),
            &object,
            e(state, None, result, V::NoEvidence, false),
        );
    }

    for state in ["NoExitCode", "PodUnschedulable", "DisruptedMidDrill"] {
        let (object, _) = patched::<Restore>(
            running.clone(),
            &restore_ctl::crashed_status_patch(&restore_of(&running), state, "r1", now()),
        );
        run_restore(
            state,
            &object,
            e(S::Failed, Some(state), R::Error, V::NoEvidence, false),
        );
    }
}

#[test]
fn the_ui_fixtures_map_as_their_names_say() {
    for (file, state, verification, success) in [
        ("backup-valid-exit0.json", S::Succeeded, V::Valid, true),
        ("backup-invalid-exit0.json", S::Succeeded, V::Invalid, false),
        (
            "backup-notattempted-exit0.json",
            S::Succeeded,
            V::NotAttempted,
            false,
        ),
        ("backup-valid-exit2.json", S::Failed, V::Valid, false),
    ] {
        let object = support::fixture(file);
        let op = backup_operation(&backup_of(&object));
        assert_eq!(
            (op.state, op.verification.state, op.verified_success),
            (state, verification, success),
            "{file}"
        );
        assert_eq!(
            op.verified_success,
            backup_badge(&object["status"]).green,
            "{file}"
        );
    }
    for (file, state, verification, success) in [
        ("restore-valid-pass.json", S::Succeeded, V::Valid, true),
        ("restore-invalid-pass.json", S::Succeeded, V::Invalid, false),
        (
            "restore-notattempted-pass.json",
            S::Succeeded,
            V::NotAttempted,
            false,
        ),
        (
            "restore-valid-failintegrity.json",
            S::Failed,
            V::Valid,
            false,
        ),
    ] {
        let object = support::fixture(file);
        let op = restore_operation(&restore_of(&object));
        assert_eq!(
            (op.state, op.verification.state, op.verified_success),
            (state, verification, success),
            "{file}"
        );
        assert_eq!(
            op.verified_success,
            restore_badge(&object["status"]).green,
            "{file}"
        );
    }
}

#[test]
fn every_preview_fixture_object_satisfies_the_invariant() {
    let root = support::repo_root().join("ui/tests/fixtures/preview/namespaces/default");
    let backups: Value =
        serde_json::from_str(&std::fs::read_to_string(root.join("backups.json")).unwrap()).unwrap();
    let restores: Value =
        serde_json::from_str(&std::fs::read_to_string(root.join("restores.json")).unwrap())
            .unwrap();
    let mut states = std::collections::BTreeSet::new();
    for item in backups["items"].as_array().unwrap() {
        let op = backup_operation(&backup_of(item));
        let green = backup_badge(item.get("status").unwrap_or(&json!({}))).green;
        assert_eq!(
            op.verified_success,
            green && op.state == S::Succeeded,
            "{}",
            item["metadata"]["name"]
        );
        states.insert(format!("backup:{:?}", op.state));
    }
    for item in restores["items"].as_array().unwrap() {
        let op = restore_operation(&restore_of(item));
        let green = restore_badge(item.get("status").unwrap_or(&json!({}))).green;
        assert_eq!(
            op.verified_success,
            green && op.state == S::Succeeded,
            "{}",
            item["metadata"]["name"]
        );
        states.insert(format!("restore:{:?}", op.state));
    }
    assert!(
        states.len() >= 4,
        "the preview fixtures exercise several states: {states:?}"
    );
}

#[tokio::test]
async fn the_operations_route_serves_the_normalized_status() {
    let app = support::TestApp::new();
    let mut restore = support::fixture("restore-notattempted-pass.json");
    restore["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("namespace");
    app.fake.seed("restores", support::NS_A, restore);
    let mut backup = support::fixture("backup-valid-exit0.json");
    backup["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("namespace");
    app.fake.seed("backups", support::NS_A, backup);

    let r = app
        .get("/api/v1/namespaces/team-a/operations/restore/orders-drill-d")
        .await;
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    let item = &r.json()["item"];
    assert_eq!(item["kind"], "restore");
    assert_eq!(item["state"], "succeeded");
    assert_eq!(item["result"]["status"], "pass");
    assert_eq!(item["result"]["outcome"], "pass");
    assert_eq!(item["verification"]["state"], "notAttempted");
    assert_eq!(item["verifiedSuccess"], false);

    let b = app
        .get("/api/v1/namespaces/team-a/operations/backup/orders-hourly-20260911-124000")
        .await;
    let item = &b.json()["item"];
    assert_eq!(item["state"], "succeeded");
    assert_eq!(item["verification"]["state"], "valid");
    assert_eq!(
        item["verification"]["matchedKeyId"],
        "6ea0c47f9a1b4d2e8c5f0a3b7d6e9012"
    );
    assert_eq!(item["verifiedSuccess"], true);

    // The list rows carry the same summary.
    let list = app.get("/api/v1/namespaces/team-a/restores").await.json();
    assert_eq!(
        list["items"][0]["operation"]["verificationState"],
        "notAttempted"
    );
    assert_eq!(list["items"][0]["operation"]["verifiedSuccess"], false);

    for bad in [
        "discovery",
        "preflight",
        "Backup",
        "backups",
        "kafkacluster",
    ] {
        app.get(&format!("/api/v1/namespaces/team-a/operations/{bad}/x"))
            .await
            .assert_problem(404, "not_found");
    }
    app.get("/api/v1/namespaces/team-a/operations/backup/absent")
        .await
        .assert_problem(404, "not_found");
    app.fake.assert_strict();
}

/// **No infrastructure detail is frozen into the versioned operation contract.**
///
/// REGRESSION REASON (review finding R5). The projection used to copy the
/// controller's runner Job name into `Operation.jobName`. PLAT-17.1's own
/// problem statement is that direct CR manipulation "exposes infrastructure
/// details", and D0 hands PLAT-14.1 the final normalized mapping, listing what
/// stays visible: reason, message, exit code, last phase, timestamps and
/// evidence references. A Job name is on none of them.
///
/// The asymmetry is what decides it: adding a field to this document later is a
/// MINOR change, removing one is MAJOR. So the field waits for the task that
/// owns the ruling — and this test is what stops it, or any other Job/Pod/image
/// detail, from arriving by accident in the meantime.
///
/// The key set is asserted whole rather than one absent name at a time, so a
/// field added under a different spelling fails too.
#[test]
fn no_infrastructure_detail_is_frozen_into_the_operation_contract() {
    let base = base_backup();
    let typed = backup_of(&base);
    // A Job name that cannot be confused with the object's own name, so the
    // byte scan below means something.
    let job = "logweir-backup-b1-20260916t131321z-runner";
    let (running, _) = patched::<Backup>(
        base.clone(),
        &backup_ctl::running_status_patch(&typed, job, now()),
    );

    // The controller really did record it: the scan is not vacuous.
    assert_eq!(
        running
            .pointer("/status/jobRef/name")
            .and_then(Value::as_str),
        Some(job),
        "the fixture must carry a jobRef for this test to prove anything"
    );

    // The PUBLISHED field set, from the generated document rather than from one
    // serialized instance: an optional field is absent from an instance whether
    // it was removed or merely unset, and only one of those is a contract change.
    let document: Value =
        serde_json::from_str(&logweir_api::openapi::openapi_document()).expect("JSON");
    let declared: BTreeSet<&str> = document["components"]["schemas"]["Operation"]["properties"]
        .as_object()
        .expect("Operation is an object schema")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        declared,
        BTreeSet::from([
            "conditions",
            "createdAt",
            "evidence",
            "kind",
            "lastUpdatedAt",
            "message",
            "name",
            "namespace",
            "resourceVersion",
            "result",
            "state",
            "stateReason",
            "terminal",
            "uid",
            "verification",
            "verifiedSuccess",
        ]),
        "the operation contract's field set changed. Adding one is a MINOR change to \
         schemas/logweir-api-v1.openapi.json and needs `just schema`; adding an \
         infrastructure identifier — a Job, Pod, node or image name — is a contract \
         decision PLAT-14.1 owns, not a projection detail."
    );

    // NO FIELD *IS* THE JOB NAME — under `jobName` or any other spelling.
    //
    // The distinction this draws is deliberate. D0 keeps the controller's
    // `reason` and `message` visible, and the controller's running message says
    // "the runner Job <name> exists and has not finished". Prose that mentions
    // an object is not a machine-readable handle to it: a client cannot select
    // on it, and PLAT-14.1 owns that message's final wording anyway. A field
    // whose VALUE is exactly the Job name is the handle, and that is what stays
    // out until PLAT-14.1 rules.
    for (operation, label) in [
        (backup_operation(&backup_of(&running)), "Backup"),
        (
            {
                let restore_object = base_restore();
                let restore_typed = restore_of(&restore_object);
                let (restore_running, _) = patched::<Restore>(
                    restore_object.clone(),
                    &restore_ctl::running_status_patch(&restore_typed, job, true, now()),
                );
                restore_operation(&restore_of(&restore_running))
            },
            "Restore",
        ),
    ] {
        let value = serde_json::to_value(&operation).expect("the DTO serializes");
        let mut carriers = Vec::new();
        collect_fields_equal_to(&value, "", job, &mut carriers);
        assert!(
            carriers.is_empty(),
            "{label}: these fields carry the runner Job name as their value, which makes it a \
             handle rather than prose: {carriers:?}"
        );
        // Not vacuous: the name IS present in the controller's message.
        assert!(
            serde_json::to_string(&value).unwrap().contains(job),
            "{label}: the fixture's message no longer mentions the Job, so the check above \
             proves nothing — pick a status that does"
        );
    }
}

/// Every JSON path whose string value is exactly `needle`.
fn collect_fields_equal_to(value: &Value, path: &str, needle: &str, out: &mut Vec<String>) {
    match value {
        Value::String(text) if text == needle => out.push(path.to_string()),
        Value::Object(map) => {
            for (key, child) in map {
                collect_fields_equal_to(child, &format!("{path}/{key}"), needle, out);
            }
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                collect_fields_equal_to(child, &format!("{path}/{index}"), needle, out);
            }
        }
        _ => {}
    }
}
