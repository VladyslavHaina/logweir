//! Backup and Restore status, normalized into the product operation model.
//!
//! RESULT AND VERIFICATION ARE SEPARATE, AND "VERIFIED SUCCESS" IS NOT
//! INVENTED HERE. [`Operation::verified_success`] is computed by the
//! controller's own green-badge functions,
//! `weirkeeper::verification::{backup_badge, restore_badge}`, over the stored
//! status. This API and the controller therefore cannot disagree about what
//! "verified" means: a Restore with `outcome: pass` and verification
//! `NotAttempted` is `state: succeeded`, `verification.state: notAttempted`,
//! `verifiedSuccess: false`.
//!
//! # The state table
//!
//! | stored status | `state` | `stateReason` |
//! |---|---|---|
//! | no status / no phase | `pending` | `AwaitingController` |
//! | `Pending` + `ApprovalBundleMaterializationFailed` | `preparing` | that reason |
//! | `Pending` (e.g. `ApprovalNotVerified`) | `pending` | the reason |
//! | `Running` | `running` | the reason or `JobCreated` |
//! | terminal, evidence keys recorded, no verdict yet | `verifying` | `EvidenceVerificationPending` |
//! | `Succeeded` | `succeeded` | the terminal condition reason |
//! | `Failed`, exit 3 | `refused` | the terminal reason |
//! | `Failed`, no exit code, a controller refusal state | `refused` | that state |
//! | `Failed`, anything else | `failed` | the terminal reason |
//! | any other phase | `unknown` | `UnrecognizedPhase` |
//!
//! `queued` and `cancelled` are part of the model and are not produced by
//! current resources: there is no queue state on a Backup/Restore and no
//! cancel operation.

use chrono::{DateTime, Utc};
use kube::ResourceExt;
use serde_json::Value;
use weirkeeper::crds::backup::Backup;
use weirkeeper::crds::restore::Restore;
use weirkeeper::crds::{Condition, EvidenceVerification};

use crate::contract::{
    ConditionView, Operation, OperationEvidence, OperationKind, OperationResult, OperationState,
    OperationSummary, OperationVerification, ResultStatus, VerificationState,
};
use crate::validate::bounded;

/// The most conditions an operation response carries.
pub const MAX_CONDITIONS: usize = 16;

/// Terminal states a controller or guard writes when it refused a run before
/// anything executed. Everything else that is `Failed` without an exit code is
/// a failure (a crash, an unschedulable pod, a disrupted node).
pub const REFUSAL_STATES: &[&str] = &[
    "ApprovalNotReceived",
    "ApprovalSubjectMismatch",
    "ApprovalBundleConflict",
    "ArchiveUrlUnreadable",
    "ClusterNotReachable",
    "CredentialNotRenderable",
    "Expired",
    "GuardRefused",
    "GuardRefusedUnknownReason",
    "JobNameConflict",
    "NameTooLong",
    "PlanConfigMapConflict",
    "PlanHashMismatch",
    "ReferentNotFound",
    "TargetTopicConfigRefused",
];

/// A condition view with a bounded message.
#[must_use]
pub fn condition_view(c: &Condition) -> ConditionView {
    ConditionView {
        type_: c.r#type.clone(),
        status: c.status.clone(),
        reason: c.reason.clone(),
        message: c.message.as_deref().map(|m| bounded(m, 1024)),
        last_transition_time: c.last_transition_time,
    }
}

fn condition_views(conditions: Option<&Vec<Condition>>) -> Vec<ConditionView> {
    conditions
        .map(|cs| cs.iter().take(MAX_CONDITIONS).map(condition_view).collect())
        .unwrap_or_default()
}

/// The inputs the state table reads, common to both kinds.
struct Observed<'a> {
    phase: Option<&'a str>,
    exit_code: Option<i32>,
    /// `Restore.status.reason`; `None` on a Backup, which has no scalar.
    scalar_reason: Option<&'a str>,
    conditions: Option<&'a Vec<Condition>>,
    verification: Option<&'a EvidenceVerification>,
    /// Payload key, sidecar key and digest are all recorded: the controller
    /// attempts verification exactly in this case.
    evidence_recorded: bool,
    /// At least one evidence key is recorded.
    evidence_mentioned: bool,
}

fn find<'a>(conditions: Option<&'a Vec<Condition>>, type_: &str) -> Option<&'a Condition> {
    conditions.and_then(|cs| cs.iter().find(|c| c.r#type == type_))
}

/// The reason of the condition describing the current state.
fn current_reason<'a>(o: &Observed<'a>) -> Option<&'a str> {
    if let Some(reason) = o.scalar_reason {
        return Some(reason);
    }
    let by_type = |t: &str| find(o.conditions, t).and_then(|c| c.reason.as_deref());
    match o.phase {
        Some("Succeeded") => by_type("Complete"),
        Some("Failed") => by_type("Failed"),
        Some("Running") => by_type("JobCreated"),
        Some("Pending") => by_type("Admitted").or_else(|| by_type("ApprovalBundleReady")),
        _ => None,
    }
}

fn reason_message<'a>(o: &Observed<'a>, reason: Option<&str>) -> Option<String> {
    let reason = reason?;
    o.conditions?
        .iter()
        .find(|c| c.reason.as_deref() == Some(reason) && c.message.is_some())
        .and_then(|c| c.message.as_deref())
        .map(|m| bounded(m, 1024))
}

fn state_of(o: &Observed<'_>) -> (OperationState, Option<String>) {
    let reason = current_reason(o).map(str::to_string);
    match o.phase {
        None => (
            OperationState::Pending,
            Some("AwaitingController".to_string()),
        ),
        Some("Pending") => {
            if reason.as_deref() == Some("ApprovalBundleMaterializationFailed") {
                (OperationState::Preparing, reason)
            } else {
                (OperationState::Pending, reason)
            }
        }
        Some("Running") => (
            OperationState::Running,
            reason.or_else(|| Some("JobCreated".to_string())),
        ),
        Some(phase @ ("Succeeded" | "Failed")) => {
            if o.evidence_recorded && o.verification.is_none() {
                return (
                    OperationState::Verifying,
                    Some("EvidenceVerificationPending".to_string()),
                );
            }
            if phase == "Succeeded" {
                return (OperationState::Succeeded, reason);
            }
            let refused = o.exit_code == Some(3)
                || (o.exit_code.is_none()
                    && reason
                        .as_deref()
                        .is_some_and(|r| REFUSAL_STATES.contains(&r)));
            if refused {
                (OperationState::Refused, reason)
            } else {
                (OperationState::Failed, reason)
            }
        }
        Some(_) => (
            OperationState::Unknown,
            Some("UnrecognizedPhase".to_string()),
        ),
    }
}

fn result_of(o: &Observed<'_>, state: OperationState, outcome: Option<&str>) -> ResultStatus {
    if !matches!(
        state,
        OperationState::Succeeded
            | OperationState::Failed
            | OperationState::Refused
            | OperationState::Verifying
    ) {
        return if state == OperationState::Unknown {
            ResultStatus::Unknown
        } else {
            ResultStatus::Pending
        };
    }
    match o.exit_code {
        Some(0) => match outcome {
            None | Some("pass") => ResultStatus::Pass,
            Some(_) => ResultStatus::Unknown,
        },
        Some(2) => ResultStatus::NotPass,
        Some(3) => ResultStatus::Refused,
        Some(1 | 4) => ResultStatus::Error,
        Some(_) => ResultStatus::Error,
        None => {
            if state == OperationState::Refused {
                ResultStatus::Refused
            } else if o.phase == Some("Failed") {
                ResultStatus::Error
            } else {
                ResultStatus::Unknown
            }
        }
    }
}

fn verification_of(o: &Observed<'_>, terminal_run: bool) -> OperationVerification {
    let Some(v) = o.verification else {
        // No verdict on the object. Not finished, or finished with complete
        // evidence the controller has not verified yet: pending. Finished
        // with SOME evidence keys but no digest: the controller never attempts
        // verification for that shape, and its badge rule reads an absent
        // verdict as not attempted — so does this projection. Finished with no
        // evidence at all (exits 1, 3, 4, refusals, crashes): nothing to verify.
        let (state, detail) = if !terminal_run || o.evidence_recorded {
            (VerificationState::Pending, None)
        } else if o.evidence_mentioned {
            (
                VerificationState::NotAttempted,
                Some(
                    "evidence keys are recorded without a digest; the controller does not \
                     attempt verification for this run"
                        .to_string(),
                ),
            )
        } else {
            (VerificationState::NoEvidence, None)
        };
        return OperationVerification {
            state,
            matched_key_id: None,
            verified_at: None,
            payload_type: None,
            detail,
        };
    };
    let state = match v.result.as_deref() {
        // A `Valid` without its key or instant is not a verdict the controller
        // writes; the badge functions treat it as not attempted, and so does
        // this projection.
        Some("Valid") if v.matched_key_id.is_some() && v.verified_at.is_some() => {
            VerificationState::Valid
        }
        Some("Valid") | Some("NotAttempted") => VerificationState::NotAttempted,
        Some("Invalid") => VerificationState::Invalid,
        None => VerificationState::Pending,
        Some(_) => VerificationState::Unknown,
    };
    OperationVerification {
        state,
        matched_key_id: v.matched_key_id.clone(),
        verified_at: v.verified_at,
        payload_type: v.payload_type.clone(),
        detail: v.detail.as_deref().map(|d| bounded(d, 512)),
    }
}

fn last_updated(
    conditions: Option<&Vec<Condition>>,
    verification: Option<&EvidenceVerification>,
) -> Option<DateTime<Utc>> {
    let mut latest = conditions
        .into_iter()
        .flatten()
        .filter_map(|c| c.last_transition_time)
        .max();
    if let Some(at) = verification.and_then(|v| v.verified_at) {
        latest = Some(latest.map_or(at, |l| l.max(at)));
    }
    latest
}

/// The normalized operation for a Backup.
#[must_use]
pub fn backup_operation(backup: &Backup) -> Operation {
    let status = backup.status.as_ref();
    let evidence = status.and_then(|s| s.evidence.as_ref());
    let evidence_recorded = evidence.is_some_and(|e| {
        e.receipt_key.is_some() && e.sidecar_key.is_some() && e.receipt_sha256.is_some()
    });
    let evidence_mentioned = evidence.is_some_and(|e| {
        e.receipt_key.is_some() || e.sidecar_key.is_some() || e.receipt_sha256.is_some()
    });
    let observed = Observed {
        phase: status.and_then(|s| s.phase.as_deref()),
        exit_code: status.and_then(|s| s.exit_code),
        scalar_reason: None,
        conditions: status.and_then(|s| s.conditions.as_ref()),
        verification: evidence.and_then(|e| e.verification.as_ref()),
        evidence_recorded,
        evidence_mentioned,
    };
    let (state, state_reason) = state_of(&observed);
    let result_status = result_of(&observed, state, None);
    let terminal_run = matches!(observed.phase, Some("Succeeded" | "Failed"));
    let verification = verification_of(&observed, terminal_run);
    let badge = weirkeeper::verification::backup_badge(&status_json(status));
    Operation {
        kind: OperationKind::Backup,
        name: backup.name_any(),
        namespace: backup.namespace().unwrap_or_default(),
        uid: backup.uid().unwrap_or_default(),
        resource_version: backup.resource_version().unwrap_or_default(),
        created_at: backup.metadata.creation_timestamp.as_ref().map(|t| t.0),
        message: reason_message(&observed, state_reason.as_deref()),
        state,
        state_reason,
        terminal: state.is_terminal(),
        last_updated_at: last_updated(observed.conditions, observed.verification),
        result: OperationResult {
            status: result_status,
            exit_code: observed.exit_code,
            exit_reason: status.and_then(|s| s.exit_reason.clone()),
            outcome: None,
            last_phase_completed: None,
        },
        verified_success: badge.green
            && state == OperationState::Succeeded
            && verification.state == VerificationState::Valid,
        verification,
        evidence: OperationEvidence {
            payload_key: evidence.and_then(|e| e.receipt_key.clone()),
            payload_sha256: evidence.and_then(|e| e.receipt_sha256.clone()),
            sidecar_key: evidence.and_then(|e| e.sidecar_key.clone()),
            offset_report_key: None,
            offset_report_sha256: None,
        },
        conditions: condition_views(observed.conditions),
    }
}

/// The normalized operation for a Restore.
#[must_use]
pub fn restore_operation(restore: &Restore) -> Operation {
    let status = restore.status.as_ref();
    let evidence = status.and_then(|s| s.evidence.as_ref());
    let evidence_recorded = evidence.is_some_and(|e| {
        e.scorecard_key.is_some() && e.sidecar_key.is_some() && e.scorecard_sha256.is_some()
    });
    let evidence_mentioned = evidence.is_some_and(|e| {
        e.scorecard_key.is_some() || e.sidecar_key.is_some() || e.scorecard_sha256.is_some()
    });
    let observed = Observed {
        phase: status.and_then(|s| s.phase.as_deref()),
        exit_code: status.and_then(|s| s.exit_code),
        scalar_reason: status.and_then(|s| s.reason.as_deref()),
        conditions: status.and_then(|s| s.conditions.as_ref()),
        verification: evidence.and_then(|e| e.verification.as_ref()),
        evidence_recorded,
        evidence_mentioned,
    };
    let (state, state_reason) = state_of(&observed);
    let outcome = status.and_then(|s| s.outcome.as_deref());
    let result_status = result_of(&observed, state, outcome);
    let terminal_run = matches!(observed.phase, Some("Succeeded" | "Failed"));
    let verification = verification_of(&observed, terminal_run);
    let badge = weirkeeper::verification::restore_badge(&status_json(status));
    Operation {
        kind: OperationKind::Restore,
        name: restore.name_any(),
        namespace: restore.namespace().unwrap_or_default(),
        uid: restore.uid().unwrap_or_default(),
        resource_version: restore.resource_version().unwrap_or_default(),
        created_at: restore.metadata.creation_timestamp.as_ref().map(|t| t.0),
        message: reason_message(&observed, state_reason.as_deref()),
        state,
        state_reason,
        terminal: state.is_terminal(),
        last_updated_at: last_updated(observed.conditions, observed.verification),
        result: OperationResult {
            status: result_status,
            exit_code: observed.exit_code,
            exit_reason: status.and_then(|s| s.exit_reason.clone()),
            outcome: outcome.map(str::to_string),
            last_phase_completed: status.and_then(|s| s.last_phase_completed),
        },
        verified_success: badge.green
            && state == OperationState::Succeeded
            && verification.state == VerificationState::Valid,
        verification,
        evidence: OperationEvidence {
            payload_key: evidence.and_then(|e| e.scorecard_key.clone()),
            payload_sha256: evidence.and_then(|e| e.scorecard_sha256.clone()),
            sidecar_key: evidence.and_then(|e| e.sidecar_key.clone()),
            offset_report_key: evidence.and_then(|e| e.offset_report_key.clone()),
            offset_report_sha256: evidence.and_then(|e| e.offset_report_sha256.clone()),
        },
        conditions: condition_views(observed.conditions),
    }
}

/// The list-item summary of an operation.
#[must_use]
pub fn summary(operation: &Operation) -> OperationSummary {
    OperationSummary {
        state: operation.state,
        state_reason: operation.state_reason.clone(),
        terminal: operation.terminal,
        verification_state: operation.verification.state,
        verified_success: operation.verified_success,
    }
}

fn status_json<T: serde::Serialize>(status: Option<&T>) -> Value {
    status
        .and_then(|s| serde_json::to_value(s).ok())
        .unwrap_or_else(|| Value::Object(serde_json::Map::new()))
}
