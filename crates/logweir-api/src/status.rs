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

// ======================================================================
// D3 §2.5 — the normalized operation VIEW
// ======================================================================
//
// WHY A SECOND TYPE AND NOT MORE FIELDS ON `Operation`. `contract::Operation`
// is PLAT-17.1's frozen projection: `ui/contract.js` types it, the OpenAPI
// document publishes it, and D0's rule is that adding a field is MINOR and
// removing one is MAJOR. D3 §2.5 adds a stage, a progress block, a diagnosis
// list, a trust basis, a verification scope and two kind-specific panels, and
// every one of them is a superset. [`OperationView`] flattens the frozen shape
// and carries the additions beside it, so the wire body is the old body plus
// new keys — a console written against either reads the one it knows.
//
// WHAT IS STILL NOT HERE. No Job name, no pod name and no container state:
// D0's "remains visible in bounded form" list is reason, message, exit code,
// last phase, timestamps and evidence references, and
// `no_infrastructure_detail_is_frozen_into_the_operation_contract` keeps the
// first of those out. A DIAGNOSTIC's `object {kind, name}` IS published,
// because PLAT-14.1's acceptance asks in as many words for "pod mount and
// scheduling failures as useful resource-scoped errors" and a scheduling
// failure with no object named is not resource-scoped.
//
// AND NO PROSE. D3 §3.5 and §11 give `ui/render.js` the fixed sentences and
// say the server never authors them, so `verificationScope` carries the level
// and the counts and no `statement` string: a sentence in a versioned contract
// is a sentence that cannot be corrected without a contract change.

use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use weirkeeper::crds::{Diagnostic, RunProgress, TrustBasis};

/// D3 §2.5's stage vocabulary, exactly six values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum OperationStage {
    /// Accepted; nothing has been scheduled.
    Admission,
    /// A Job exists and no pod is running yet.
    Queued,
    /// The pod exists and the runner container has not started.
    Preparing,
    /// The runner is doing the work.
    Running,
    /// The runner is checking, or the controller is verifying the evidence.
    Verifying,
    /// Nothing more will happen.
    Finished,
}

impl OperationStage {
    /// The stage a controller wrote, if this build recognises the spelling.
    #[must_use]
    pub fn parse(stage: &str) -> Option<Self> {
        match stage {
            "Admission" => Some(Self::Admission),
            "Queued" => Some(Self::Queued),
            "Preparing" => Some(Self::Preparing),
            "Running" => Some(Self::Running),
            "Verifying" => Some(Self::Verifying),
            "Finished" => Some(Self::Finished),
            _ => None,
        }
    }
}

/// The runner's own phase, from its `progress-phase=` lines.
#[derive(Clone, Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RunnerPhaseView {
    /// `-1`..`9`. A backup reports `-1` with a step name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub number: Option<i32>,
    /// The step or phase name, at most 32 bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// The object a diagnostic is about.
#[derive(Clone, Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticObjectView {
    /// `Pod` or `Job`.
    pub kind: String,
    /// Its name.
    pub name: String,
}

/// One entry of the closed diagnosis list (D3 §2.3).
#[derive(Clone, Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticView {
    /// The CamelCase code, from D2's one classification table.
    pub code: String,
    /// `Warning` or `Error`.
    pub severity: String,
    /// The sanitized explanation, at most 512 bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// What it is about.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<DiagnosticObjectView>,
    /// When it was first seen.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_seen: Option<DateTime<Utc>>,
    /// When it was last seen.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen: Option<DateTime<Utc>>,
    /// How many times, capped by the controller.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<i64>,
}

/// The progress channel, absent on anything an older controller reconciled.
#[derive(Clone, Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProgressView {
    /// The stage the controller wrote, when this build recognises it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage: Option<OperationStage>,
    /// The CamelCase reason.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The sanitized message, at most 1024 bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// When the stage or reason last CHANGED.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<DateTime<Utc>>,
    /// When the run was last observed, in active stages only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_observed_time: Option<DateTime<Utc>>,
    /// The runner's own phase, from its `progress-phase=` lines.
    ///
    /// NAMED AS THE CRD NAMES IT. D3 §2.2 is "verbatim from the CRD" and the
    /// stored field is `status.progress.runnerPhase`; this view published it
    /// as `phase` in the first round and the console had already written
    /// `runnerPhase` against the decision. One spelling, and it is the one the
    /// object carries.
    #[serde(rename = "runnerPhase", skip_serializing_if = "Option::is_none")]
    pub runner_phase: Option<RunnerPhaseView>,
}

/// D3 §2.5's evidence verdict, which is the RESULT and the TRUST BASIS
/// together.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum TrustState {
    /// Not finished, or finished with evidence and no verdict yet.
    Pending,
    /// Verified under a key the policy accepts now.
    Verified,
    /// Verified under a key that was valid when it signed and has since been
    /// retired. **A pass, not a downgrade** — it is what rotation looks like.
    VerifiedHistorical,
    /// The signature verifies and the key is one this installation will not
    /// accept: unknown, revoked, or holding the wrong usage.
    Untrusted,
    /// The bytes do not match the signature.
    Invalid,
    /// No verdict was reached. NOT a verified result.
    NotAttempted,
    /// The run wrote no artifact to verify (exits 1, 3 and 4, refusals,
    /// crashes).
    NotApplicable,
}

/// Which trust policy answered, and the revision that answered.
#[derive(Clone, Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PolicyRefView {
    /// Its name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Its UID — a same-named replacement is a different policy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    /// Its `metadata.generation`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<i64>,
}

/// The trust half of the verdict, separate from the result and from the
/// signature check.
///
/// `basis` IS NEVER ABSENT HERE, AND THAT IS D3 §12. A status with no `trust`
/// block has not had its signing time compared with anything, so the honest
/// projection is `none` — "not observed" — and never a flattering default.
#[derive(Clone, Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OperationTrust {
    /// The combined verdict.
    pub state: TrustState,
    /// `Current`, `Historical`, `RecordedBeforeRevocation`, `Unverified` or
    /// `None`, **in the CRD's own spelling**. `None` is what an absent `trust`
    /// block projects to, which is D3 §12's sentence word for word: "`trust`
    /// absent → `basis: None`". The first round lowercased these and invented
    /// a vocabulary no other surface prints.
    pub basis: String,
    /// The signing key's state when the verdict was reached.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_state: Option<String>,
    /// Which policy answered.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicyRefView>,
    /// The signing time the verdict was reached AGAINST. Attacker-controlled
    /// for a compromised key, which is why it is recorded and not trusted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signed_at: Option<DateTime<Utc>>,
    /// `absent` once a bounded re-read established that the DOCUMENT carries
    /// no signing time. It is the answer, recorded in place of the question.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signing_time_read: Option<String>,
}

/// How thorough the record check was. `complete` does not exist in v1.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum VerificationScopeLevel {
    /// `byte-fingerprint`: a sample was compared byte for byte.
    Sampled,
    /// `consume-only`: records were read back and not compared.
    Degraded,
    /// No record check ran, or the artifact attests counts and a window rather
    /// than a restore.
    None,
}

/// What the verification actually covered, in numbers the console renders a
/// fixed sentence around.
#[derive(Clone, Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VerificationScopeView {
    /// The level.
    pub level: VerificationScopeLevel,
    /// How many records were sampled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub records_sampled: Option<i64>,
    /// How many of them matched byte for byte.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub records_sampled_matching: Option<i64>,
    /// The canary size the plan asked for.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub records_expected: Option<i64>,
}

/// One topic the run created.
#[derive(Clone, Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreatedTopicView {
    /// Its name.
    pub name: String,
    /// Its partition count.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitions: Option<i64>,
}

/// The window an integrity sample covered.
#[derive(Clone, Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SampleWindowView {
    /// Its start.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<DateTime<Utc>>,
    /// Its end.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<DateTime<Utc>>,
}

/// D3 §3.5's incident-facing completion panel — a Restore's, copied from the
/// signed scorecard and never recomputed.
#[derive(Clone, Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CompletionView {
    /// The topics the run created, with their partition counts.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(default)]
    pub new_topics: Vec<CreatedTopicView>,
    /// The canary size.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub records_expected: Option<i64>,
    /// How many records were restored in the sampled window.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub records_restored: Option<i64>,
    /// How many records were sampled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub records_sampled: Option<i64>,
    /// How many of them matched.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub records_sampled_matching: Option<i64>,
    /// `byte-fingerprint`, `consume-only` or `not-attempted`, verbatim.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integrity_level: Option<String>,
    /// The window the sample covered.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_window: Option<SampleWindowView>,
}

/// One topic teardown could not remove.
#[derive(Clone, Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TeardownFailureView {
    /// The topic.
    pub topic: String,
    /// The error, bounded.
    pub error: String,
}

/// What phase 9 removed, from the SIGNED teardown attestation.
#[derive(Clone, Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TeardownView {
    /// Where the attestation is.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attestation_key: Option<String>,
    /// The topics phase 9 removed, by name.
    ///
    /// A LIST AND NOT A COUNT. D3 §2.2 declares `deleted: [string]`, and the
    /// incident question is "which topics went", not "how many": a count
    /// cannot be reconciled against the names the run created. The first round
    /// published the length.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(default)]
    pub deleted: Vec<String>,
    /// Whether `deleted` was cut short by this projection's row bound.
    pub deleted_truncated: bool,
    /// The ones that could not be removed. A non-empty list is what makes the
    /// next rehearsal slot SKIP rather than adopt topics it did not create.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(default)]
    pub failed: Vec<TeardownFailureView>,
}

/// When the capture actually happened — a Backup's.
#[derive(Clone, Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CaptureView {
    /// Capture START, which is what protection freshness is measured from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    /// When it finished.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    /// How many records the VERIFIED receipt attested.
    ///
    /// Absent is a fact about the run, not a zero. `Backup.status.records` was
    /// declared with a printer column and written by nothing until D3 W2
    /// (defect `STATUS-RECORDS`), so every object older than that fix has it
    /// absent — and "no receipt has been verified for this run" is a different
    /// statement from "this run captured nothing".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub records: Option<i64>,
}

/// Configuration readiness (PLAT-03.1), which NEVER overwrites `state`.
#[derive(Clone, Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReadinessView {
    /// `unknown` until PLAT-03.1 lands.
    pub state: String,
    /// Why: `notImplemented`.
    pub basis: String,
}

impl ReadinessView {
    fn not_implemented() -> Self {
        Self {
            state: "unknown".to_string(),
            basis: "notImplemented".to_string(),
        }
    }
}

/// The normalized operation, D3 §2.5's shape: PLAT-17.1's frozen projection
/// plus everything D3 adds.
#[derive(Clone, Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OperationView {
    /// PLAT-17.1's `Operation`, flattened so the wire body is a superset.
    #[serde(flatten)]
    pub operation: Operation,
    /// The controller's stage. Absent means no stage was observed — an older
    /// controller, or a phase this build does not recognise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage: Option<OperationStage>,
    /// The progress channel. Absent on anything an older controller wrote, and
    /// its absence is why `queued` and `preparing` are never inferred.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress: Option<ProgressView>,
    /// The closed diagnosis list, newest `lastSeen` first, at most eight.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(default)]
    pub diagnostics: Vec<DiagnosticView>,
    /// The evidence verdict in trust terms, beside — never instead of — the
    /// signature result in `verification`.
    pub trust: OperationTrust,
    /// What the verification covered.
    pub verification_scope: VerificationScopeView,
    /// Whether the run is held for a human approval.
    pub awaiting_approval: bool,
    /// Whether the status itself is too old to believe (D3 §2.5's last row).
    pub stale: bool,
    /// Configuration readiness, which is a separate object.
    pub readiness: ReadinessView,
    /// A Backup's capture window.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capture: Option<CaptureView>,
    /// A Restore's completion panel.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion: Option<CompletionView>,
    /// A Restore's teardown outcome.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub teardown: Option<TeardownView>,
    /// `scratch` or `newTopic` — a Restore's target mode, absent on a Backup.
    ///
    /// AT THE TOP LEVEL, NOT ON THE COMPLETION PANEL. D3 §3.5 keys its two
    /// fixed guidance blocks on `spec.target.mode`, and that is a fact about
    /// the run from the moment it is created — a rehearsal is a rehearsal
    /// before its scorecard exists. The first round hid it inside
    /// `completion`, so a Restore that had not finished could not be labelled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_mode: Option<String>,
}

/// How long an active stage may go unobserved before the status is stale.
pub const STALE_OBSERVATION_SECONDS: i64 = 300;
/// How long an object may carry no status at all before it is stale.
pub const STALE_NO_STATUS_SECONDS: i64 = 120;
/// The most diagnostics a response carries.
pub const MAX_DIAGNOSTICS: usize = 8;
/// The most teardown rows of each kind a response carries. The CRD's own
/// `maxItems`.
pub const MAX_TEARDOWN_ROWS: usize = 256;

fn progress_view(progress: Option<&RunProgress>) -> Option<ProgressView> {
    let p = progress?;
    Some(ProgressView {
        stage: OperationStage::parse(&p.stage),
        reason: p.reason.clone(),
        message: p.message.as_deref().map(|m| bounded(m, 1024)),
        last_transition_time: p.last_transition_time,
        last_observed_time: p.last_observed_time,
        runner_phase: p.runner_phase.as_ref().map(|r| RunnerPhaseView {
            number: r.number,
            name: r.name.as_deref().map(|n| bounded(n, 32)),
        }),
    })
}

fn diagnostic_views(progress: Option<&RunProgress>) -> Vec<DiagnosticView> {
    let Some(list) = progress.and_then(|p| p.diagnostics.as_ref()) else {
        return Vec::new();
    };
    list.iter()
        .take(MAX_DIAGNOSTICS)
        .map(|d: &Diagnostic| DiagnosticView {
            code: bounded(&d.code, 64),
            severity: bounded(&d.severity, 16),
            message: d.message.as_deref().map(|m| bounded(m, 512)),
            object: d.object.as_ref().map(|o| DiagnosticObjectView {
                kind: bounded(&o.kind, 16),
                name: bounded(&o.name, 253),
            }),
            first_seen: d.first_seen,
            last_seen: d.last_seen,
            count: d.count,
        })
        .collect()
}

/// D3 §12's absent-field rule for the trust block, in one place.
///
/// An absent `trust` is `basis: none`; a `basis` this build does not recognise
/// is passed through verbatim rather than rounded to the nearest word it does.
fn trust_basis(trust: Option<&TrustBasis>) -> String {
    // PASSED THROUGH, NOT TRANSLATED. Every value here is a word the CRD
    // stores and `docs/kubernetes.md` §15.2c names; a projection that rounded
    // an unrecognised one to the nearest word it knew would report a basis
    // nobody wrote.
    trust
        .and_then(|t| t.basis.as_deref())
        .map_or_else(|| BASIS_NONE.to_string(), |basis| bounded(basis, 64))
}

/// What an absent `trust` block projects to (D3 §12).
pub const BASIS_NONE: &str = "None";

fn trust_of(
    verification: &OperationVerification,
    trust: Option<&TrustBasis>,
    signed_at: Option<DateTime<Utc>>,
) -> OperationTrust {
    let basis = trust_basis(trust);
    // THE RESULT DECIDES FIRST, THE BASIS ONLY REFINES IT. `TrustBasis`'s own
    // header says so: `Unverified` is always written beside
    // `result: NotAttempted` and never rescues a `Valid`. So a verdict that is
    // not `Valid` can never be read up to `verified` by anything written here.
    let state = match verification.state {
        VerificationState::Valid => match basis.as_str() {
            "Historical" => TrustState::VerifiedHistorical,
            "RecordedBeforeRevocation" => TrustState::Verified,
            "Unverified" => TrustState::NotAttempted,
            _ => TrustState::Verified,
        },
        VerificationState::Invalid => TrustState::Invalid,
        VerificationState::Pending => TrustState::Pending,
        VerificationState::NoEvidence => TrustState::NotApplicable,
        // `Untrusted` is not a `result` the CRD writes: an untrusted signer is
        // recorded as `NotAttempted` with a trust basis that says which key.
        // The basis is what tells the two apart, and the key state is what
        // says why.
        VerificationState::NotAttempted | VerificationState::Unknown => {
            match trust.and_then(|t| t.key_state.as_deref()) {
                Some("Revoked" | "Expired" | "Unknown") => TrustState::Untrusted,
                _ => TrustState::NotAttempted,
            }
        }
    };
    OperationTrust {
        state,
        basis,
        key_state: trust.and_then(|t| t.key_state.clone()),
        policy: trust
            .and_then(|t| t.policy.as_ref())
            .map(|p| PolicyRefView {
                name: p.name.clone(),
                uid: p.uid.clone(),
                generation: p.generation,
            }),
        signed_at,
        signing_time_read: trust.and_then(|t| t.signing_time_read.clone()),
    }
}

fn scope_level(integrity_level: Option<&str>) -> VerificationScopeLevel {
    match integrity_level {
        Some("byte-fingerprint") => VerificationScopeLevel::Sampled,
        Some("consume-only") => VerificationScopeLevel::Degraded,
        _ => VerificationScopeLevel::None,
    }
}

/// The stage a run is at, from the controller's own word when it wrote one and
/// from the phase alone when it did not.
///
/// `Queued` AND `Preparing` ARE NEVER INFERRED (D3 §12). They are distinctions
/// only the progress channel can draw — a Job with no pod, against a pod whose
/// container has not started — and a projection that guessed them from
/// `phase: Pending` would be inventing an observation.
fn stage_of(progress: Option<&RunProgress>, phase: Option<&str>) -> Option<OperationStage> {
    if let Some(stage) = progress.and_then(|p| OperationStage::parse(&p.stage)) {
        return Some(stage);
    }
    match phase {
        None | Some("Pending") => Some(OperationStage::Admission),
        Some("Resolving") => Some(OperationStage::Preparing),
        Some("Running") => Some(OperationStage::Running),
        Some("Succeeded" | "Failed" | "Refused") => Some(OperationStage::Finished),
        Some(_) => None,
    }
}

/// D3 §2.5's stage overrides, applied only to a run that has not finished.
///
/// A TERMINAL PHASE IS NEVER OVERRIDDEN. `phase`, `exitCode` and `outcome` own
/// the OUTCOME; the progress channel owns "what is happening and why is it
/// taking so long". A `Finished` stage beside a `Running` phase means the
/// controller has seen the Job end and not yet written the terminal status,
/// and reporting that as a result would publish an outcome nobody recorded.
fn apply_stage(
    state: OperationState,
    reason: Option<String>,
    stage: Option<OperationStage>,
    progress_reason: Option<&str>,
) -> (OperationState, Option<String>) {
    if state.is_terminal() || state == OperationState::Verifying {
        return (state, reason);
    }
    let promoted = match stage {
        Some(OperationStage::Queued) => Some(OperationState::Queued),
        Some(OperationStage::Preparing) => Some(OperationState::Preparing),
        Some(OperationStage::Verifying) => Some(OperationState::Verifying),
        _ => None,
    };
    match promoted {
        None => (state, reason),
        Some(next) => (next, progress_reason.map(str::to_string).or(reason)),
    }
}

/// D3 §2.5's last row: a status too old to believe is `unknown`, not the last
/// thing it said.
fn apply_staleness(
    state: OperationState,
    reason: Option<String>,
    progress: Option<&RunProgress>,
    created_at: Option<DateTime<Utc>>,
    has_status: bool,
    now: DateTime<Utc>,
) -> (OperationState, Option<String>, bool) {
    if state.is_terminal() {
        return (state, reason, false);
    }
    let unobserved = progress
        .and_then(|p| p.last_observed_time)
        .is_some_and(|at| (now - at).num_seconds() > STALE_OBSERVATION_SECONDS);
    let never_reconciled = !has_status
        && created_at.is_some_and(|at| (now - at).num_seconds() > STALE_NO_STATUS_SECONDS);
    if unobserved || never_reconciled {
        return (
            OperationState::Unknown,
            Some("StatusStale".to_string()),
            true,
        );
    }
    (state, reason, false)
}

/// The normalized view of a Backup, at `now`.
#[must_use]
pub fn backup_view(backup: &Backup, now: DateTime<Utc>) -> OperationView {
    let mut operation = backup_operation(backup);
    let status = backup.status.as_ref();
    let progress = status.and_then(|s| s.progress.as_ref());
    let stage = stage_of(progress, status.and_then(|s| s.phase.as_deref()));
    let (state, reason) = apply_stage(
        operation.state,
        operation.state_reason.clone(),
        stage,
        progress.and_then(|p| p.reason.as_deref()),
    );
    let (state, reason, stale) = apply_staleness(
        state,
        reason,
        progress,
        operation.created_at,
        status.is_some(),
        now,
    );
    operation.state = state;
    operation.state_reason = reason;
    operation.terminal = state.is_terminal();
    let recorded = status
        .and_then(|s| s.evidence.as_ref())
        .and_then(|e| e.verification.as_ref());
    let trust = trust_of(
        &operation.verification,
        recorded.and_then(|v| v.trust.as_ref()),
        recorded.and_then(|v| v.signed_at),
    );
    OperationView {
        stage,
        progress: progress_view(progress),
        diagnostics: diagnostic_views(progress),
        trust,
        // A BACKUP RECEIPT ATTESTS COUNTS AND A WINDOW, NOT A RESTORE. There is
        // no sampled record comparison on this path at all, so the level is
        // `none` and the three counts are absent rather than zero: zero
        // matching records out of zero sampled reads as a failed comparison.
        verification_scope: VerificationScopeView {
            level: VerificationScopeLevel::None,
            records_sampled: None,
            records_sampled_matching: None,
            records_expected: None,
        },
        awaiting_approval: false,
        stale,
        readiness: ReadinessView::not_implemented(),
        capture: status
            .and_then(|s| s.capture.as_ref())
            .map(|c| CaptureView {
                started_at: c.started_at,
                finished_at: c.finished_at,
                records: status.and_then(|s| s.records),
            }),
        completion: None,
        teardown: None,
        target_mode: None,
        operation,
    }
}

/// The normalized view of a Restore, at `now`.
#[must_use]
pub fn restore_view(restore: &Restore, now: DateTime<Utc>) -> OperationView {
    let mut operation = restore_operation(restore);
    let status = restore.status.as_ref();
    let progress = status.and_then(|s| s.progress.as_ref());
    let stage = stage_of(progress, status.and_then(|s| s.phase.as_deref()));
    let (state, reason) = apply_stage(
        operation.state,
        operation.state_reason.clone(),
        stage,
        progress.and_then(|p| p.reason.as_deref()),
    );
    let (state, reason, stale) = apply_staleness(
        state,
        reason,
        progress,
        operation.created_at,
        status.is_some(),
        now,
    );
    // D3 §2.5's first row: a Restore held for an approval is `pending` AND
    // says so, because "waiting for a person" and "waiting for a machine" are
    // the same word on a badge and different facts in an incident.
    let awaiting_approval =
        state == OperationState::Pending && reason.as_deref() == Some("ApprovalNotVerified");
    operation.state = state;
    operation.state_reason = reason;
    operation.terminal = state.is_terminal();
    let recorded = status
        .and_then(|s| s.evidence.as_ref())
        .and_then(|e| e.verification.as_ref());
    let trust = trust_of(
        &operation.verification,
        recorded.and_then(|v| v.trust.as_ref()),
        recorded.and_then(|v| v.signed_at),
    );
    let completion = status.and_then(|s| s.completion.as_ref());
    // The level comes from the SIGNED scorecard's own copy when the controller
    // wrote one, and from `status.integrity` otherwise; the two are the same
    // fact recorded by two tasks, and taking the completion block first means
    // the number beside the level came from the same document.
    let integrity_level = completion
        .and_then(|c| c.integrity_level.as_deref())
        .or_else(|| {
            status
                .and_then(|s| s.integrity.as_ref())
                .and_then(|i| i.level.as_deref())
        });
    OperationView {
        stage,
        progress: progress_view(progress),
        diagnostics: diagnostic_views(progress),
        trust,
        verification_scope: VerificationScopeView {
            level: scope_level(integrity_level),
            records_sampled: completion.and_then(|c| c.records_sampled),
            records_sampled_matching: completion.and_then(|c| c.records_sampled_matching),
            records_expected: completion.and_then(|c| c.records_expected),
        },
        awaiting_approval,
        stale,
        readiness: ReadinessView::not_implemented(),
        capture: None,
        completion: completion.map(|c| CompletionView {
            new_topics: c
                .new_topics
                .iter()
                .flatten()
                .take(256)
                .map(|t| CreatedTopicView {
                    name: bounded(&t.name, 253),
                    partitions: t.partitions,
                })
                .collect(),
            records_expected: c.records_expected,
            records_restored: c.records_restored,
            records_sampled: c.records_sampled,
            records_sampled_matching: c.records_sampled_matching,
            integrity_level: c.integrity_level.clone(),
            sample_window: c.sample_window.as_ref().map(|w| SampleWindowView {
                start: w.start,
                end: w.end,
            }),
        }),
        target_mode: Some(wire_name(&restore.spec.target.mode)),
        teardown: status
            .and_then(|s| s.teardown.as_ref())
            .map(|t| TeardownView {
                attestation_key: t.attestation_key.clone(),
                deleted: t
                    .deleted
                    .iter()
                    .flatten()
                    .take(MAX_TEARDOWN_ROWS)
                    .map(|name| bounded(name, 249))
                    .collect(),
                deleted_truncated: t
                    .deleted
                    .as_ref()
                    .is_some_and(|d| d.len() > MAX_TEARDOWN_ROWS),
                failed: t
                    .failed
                    .iter()
                    .flatten()
                    .take(MAX_TEARDOWN_ROWS)
                    .map(|f| TeardownFailureView {
                        topic: bounded(&f.topic, 253),
                        error: bounded(&f.error, 256),
                    })
                    .collect(),
            }),
        operation,
    }
}

// ======================================================================
// D3 §2.6 — the server-sent event stream
// ======================================================================
//
// POLLED, NOT WATCHED, AND THE CONTRACT SAYS SO. `KubeAdapter` has no `watch`
// method and this task does not add one: a watch is a long-lived connection
// per subscriber against the API server, it needs the `watch` verb in the
// console's RBAC, and it makes one browser tab's reconnect storm the API
// server's problem. The stream re-reads ONE named object it has already
// authorized, at [`StreamBounds::poll`], and emits only when the
// resourceVersion moves. The event id is that resourceVersion, so
// `Last-Event-ID` means exactly what it means on the read route.
//
// EVERY BOUND IS THE SERVER'S. The caller chooses nothing: no query parameter
// is accepted (so no token can be put in a URL, which is the one place a
// credential survives in a proxy log and a browser history), the connection is
// closed at [`StreamBounds::max_connection`], the heartbeat is fixed, and the
// number of concurrent streams one principal may hold in one namespace is
// capped.
//
// AND THE CEILING IS WALL CLOCK FROM SUBSCRIBE, NOT PRODUCER TIME. The first
// shape of this loop checked the deadline only BETWEEN sends, so a client that
// opened the stream and never read filled the eight-frame channel, left the
// producer parked in `tx.send(...).await`, and held its task and its slot for
// as long as it kept the socket open and silent — measured at 1.212 s against
// a 120 ms ceiling (review finding F3). Every send is now bounded by the
// REMAINING budget and the tick never overshoots it, so a silent socket costs
// one slot for `max_connection` and not for ever. Authorization is decided ONCE at subscribe, before the first read,
// and the stream carries no ambient authority afterwards: it re-reads the same
// object in the same namespace and can reach nothing else.

/// The bounds a stream runs inside.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamBounds {
    /// How often the object is re-read.
    pub poll: Duration,
    /// How long between heartbeats when nothing changes.
    pub heartbeat: Duration,
    /// The hard ceiling on one connection.
    pub max_connection: Duration,
}

impl Default for StreamBounds {
    fn default() -> Self {
        Self {
            poll: Duration::from_secs(2),
            heartbeat: Duration::from_secs(15),
            max_connection: Duration::from_secs(300),
        }
    }
}

static STREAM_BOUNDS: OnceLock<Mutex<StreamBounds>> = OnceLock::new();

fn bounds_cell() -> &'static Mutex<StreamBounds> {
    STREAM_BOUNDS.get_or_init(|| Mutex::new(StreamBounds::default()))
}

/// The bounds this process applies to every stream.
#[must_use]
pub fn stream_bounds() -> StreamBounds {
    *bounds_cell()
        .lock()
        .expect("the stream-bounds lock is never poisoned")
}

/// Shorten the bounds for a test.
///
/// A TEST HOOK, AND THE ONLY WAY TO SET THEM. No request parameter, no header
/// and no configuration key reaches these numbers: a caller that could ask for
/// a longer connection or a faster poll would be asking this service to spend
/// more of itself on one subscriber, which is the resource-exhaustion shape
/// this cap exists for. The same reasoning as
/// `crate::routes::reset_check_rate_limits`.
#[doc(hidden)]
pub fn set_stream_bounds_for_test(bounds: StreamBounds) {
    *bounds_cell()
        .lock()
        .expect("the stream-bounds lock is never poisoned") = bounds;
}

/// The event names this stream publishes. A closed set: a console that
/// switches on them cannot be surprised.
pub const STREAM_EVENTS: [&str; 4] = ["operation", "reset", "heartbeat", "end"];

/// Why a stream stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamEnd {
    /// The operation reached a terminal state and its verification settled.
    Settled,
    /// The connection hit [`StreamBounds::max_connection`]. Reconnect.
    MaxDuration,
    /// The object is gone. A new object under the same name is a different
    /// run, so the client re-reads rather than being handed one silently.
    Vanished,
}

impl StreamEnd {
    /// The `reason` the `end` event carries.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            StreamEnd::Settled => "settled",
            StreamEnd::MaxDuration => "maxDuration",
            StreamEnd::Vanished => "vanished",
        }
    }
}

/// One `text/event-stream` frame.
///
/// `data` IS ONE LINE BECAUSE THE PAYLOAD IS COMPACT JSON. A serialized DTO
/// carries no raw newline — `serde_json` escapes every one inside a string —
/// so a single `data:` line is correct and no framing bug can split a record.
/// The assertion is not decorative: a multi-line `data` would be read by the
/// browser as one value with embedded newlines, and a `\n\n` inside it would
/// end the event early.
#[must_use]
pub fn frame(event: &str, id: Option<&str>, data: &str) -> String {
    debug_assert!(
        !data.contains('\n'),
        "an event payload is compact JSON and never carries a newline"
    );
    let mut out = String::with_capacity(data.len() + 64);
    out.push_str("event: ");
    out.push_str(event);
    out.push('\n');
    if let Some(id) = id {
        out.push_str("id: ");
        out.push_str(id);
        out.push('\n');
    }
    out.push_str("data: ");
    out.push_str(data);
    out.push_str("\n\n");
    out
}

/// A `Last-Event-ID`, validated.
///
/// A KUBERNETES resourceVersion IS AN OPAQUE STRING THE API SERVER CHOOSES,
/// and on every implementation in the wild it is a bounded decimal. Refusing
/// anything else keeps a caller from putting arbitrary bytes into an equality
/// comparison that decides whether a snapshot is sent — and keeps a header
/// nobody validated out of a log line.
#[must_use]
pub fn valid_event_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 64 && value.bytes().all(|b| b.is_ascii_digit())
}

/// Whether the stream has nothing left to report.
///
/// TERMINAL IS NOT ENOUGH. A run that exited 0 with its evidence unverified is
/// terminal and its verdict is still coming; closing there would leave a
/// console showing "succeeded, verification pending" for ever, which is the
/// exact conflation PLAT-14.1 exists to remove.
#[must_use]
pub fn is_settled(view: &OperationView) -> bool {
    view.operation.terminal && view.trust.state != TrustState::Pending
}

/// The two durable kinds a stream can follow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamKind {
    /// A `Backup`.
    Backup,
    /// A `Restore`.
    Restore,
}

impl StreamKind {
    async fn read(
        self,
        state: &crate::app::AppState,
        namespace: &str,
        name: &str,
        now: DateTime<Utc>,
    ) -> Result<OperationView, crate::kube::KubeFailure> {
        match self {
            StreamKind::Backup => state
                .kube()
                .get::<Backup>(namespace, name)
                .await
                .map(|o| backup_view(&o, now)),
            StreamKind::Restore => state
                .kube()
                .get::<Restore>(namespace, name)
                .await
                .map(|o| restore_view(&o, now)),
        }
    }
}

/// A response body fed by one bounded task.
///
/// NO NEW PACKAGE, AND NO HAND-ROLLED STATE MACHINE. `hyper::body::Body` is
/// the `http-body` trait this crate already links through axum, and a bounded
/// `tokio::sync::mpsc` channel turns "poll a body" into "poll a receiver". The
/// producer is an ordinary `async fn`, so the loop that decides what to emit is
/// readable as a loop rather than as a `poll_frame` match.
pub struct EventStreamBody {
    rx: tokio::sync::mpsc::Receiver<bytes::Bytes>,
}

impl hyper::body::Body for EventStreamBody {
    type Data = bytes::Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<bytes::Bytes>, Self::Error>>> {
        self.rx
            .poll_recv(cx)
            .map(|frame| frame.map(|bytes| Ok(hyper::body::Frame::data(bytes))))
    }
}

/// The most frames buffered for a slow reader before the producer waits.
///
/// A CHANNEL WITH A CEILING IS THE BACKPRESSURE. An unbounded channel would
/// let a client that never reads turn a 300-second stream into 300 seconds of
/// retained snapshots; at eight the producer blocks on `send`, which is what
/// stops one subscriber from spending this process's memory.
pub const STREAM_BUFFER: usize = 8;

/// Open the stream, after the caller has authorized it and taken a slot.
///
/// THE FIRST READ IS SYNCHRONOUS AND ITS FAILURE IS A PROBLEM, NOT A STREAM.
/// A `text/event-stream` that opens and immediately emits nothing is how a
/// console comes to render "connecting…" for a run that does not exist. So the
/// object is read once here: a 404 is `not_found`, an unreachable API server is
/// `kubernetes_unavailable`, and only a readable object opens a stream at all.
///
/// # Errors
///
/// The adapter's failure, mapped by [`crate::kube::KubeFailure::into_api_error`].
pub async fn open_stream(
    state: crate::app::AppState,
    kind: StreamKind,
    namespace: String,
    name: String,
    slot: crate::auth::ratelimit::StreamSlot,
    last_event_id: Option<String>,
) -> Result<axum::response::Response, crate::problem::ApiError> {
    let bounds = stream_bounds();
    let first = kind
        .read(&state, &namespace, &name, state.now())
        .await
        .map_err(crate::kube::KubeFailure::into_api_error)?;

    let (tx, rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(STREAM_BUFFER);
    tokio::spawn(async move {
        // The slot lives exactly as long as this task; a dropped connection
        // ends the task at the next tick and releases it.
        let _slot = slot;
        let started = std::time::Instant::now();
        let deadline = started + bounds.max_connection;
        let mut last_emit = started;
        let mut version = first.operation.resource_version.clone();

        // RESUME IS A FULL SNAPSHOT OR NOTHING. The event id is the
        // resourceVersion, and this service keeps no history of them, so it
        // cannot replay what happened between two versions. A client that
        // resumes at the version we are already at is exactly caught up and is
        // sent nothing; a client that resumes at any other version — older,
        // newer, or from a different object under the same name — is sent a
        // `reset` carrying the whole current state, which is the only honest
        // answer and is what D3 §2.6's "one reset snapshot" asks for.
        let resumed_here = last_event_id.as_deref() == Some(version.as_str());
        let opening = if resumed_here {
            None
        } else if last_event_id.is_some() {
            Some("reset")
        } else {
            Some("operation")
        };
        if let Some(event) = opening {
            if !send_view(&tx, event, &first, deadline).await {
                return;
            }
            last_emit = std::time::Instant::now();
        }
        if is_settled(&first) {
            let _ = send_end(&tx, StreamEnd::Settled, deadline).await;
            return;
        }

        loop {
            // THE TICK NEVER OVERSHOOTS THE DEADLINE. Sleeping a whole poll
            // interval past it would make the ceiling "300 s plus up to one
            // poll", which is not what `docs/api.md` says.
            let Some(left) = remaining(deadline) else {
                let _ = send_end(&tx, StreamEnd::MaxDuration, deadline).await;
                return;
            };
            // A closed receiver is a client that went away. `timeout` resolves
            // either way, so this is the tick AND the disconnect check.
            if tokio::time::timeout(bounds.poll.min(left), tx.closed())
                .await
                .is_ok()
            {
                return;
            }
            if remaining(deadline).is_none() {
                let _ = send_end(&tx, StreamEnd::MaxDuration, deadline).await;
                return;
            }
            let view = match kind.read(&state, &namespace, &name, state.now()).await {
                Ok(view) => view,
                Err(crate::kube::KubeFailure::NotFound) => {
                    let _ = send_end(&tx, StreamEnd::Vanished, deadline).await;
                    return;
                }
                // A TRANSIENT FAILURE IS NOT AN EVENT. The client is already
                // being told the truth by the absence of a new snapshot, and
                // a stream that published every Kubernetes hiccup would be a
                // stream that publishes the API server's state rather than the
                // operation's. The heartbeat below keeps the connection alive
                // and the next tick tries again.
                Err(_) => continue,
            };
            if view.operation.resource_version != version {
                version = view.operation.resource_version.clone();
                if !send_view(&tx, "operation", &view, deadline).await {
                    return;
                }
                last_emit = std::time::Instant::now();
                if is_settled(&view) {
                    let _ = send_end(&tx, StreamEnd::Settled, deadline).await;
                    return;
                }
            } else if last_emit.elapsed() >= bounds.heartbeat {
                if !send_heartbeat(&tx, state.now(), deadline).await {
                    return;
                }
                last_emit = std::time::Instant::now();
            }
        }
    });

    let mut response = axum::response::Response::new(axum::body::Body::new(EventStreamBody { rx }));
    let headers = response.headers_mut();
    headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("text/event-stream"),
    );
    headers.insert(
        http::header::CACHE_CONTROL,
        http::HeaderValue::from_static("no-store"),
    );
    // Proxies that buffer a response body turn a live stream into a 300-second
    // silence followed by everything at once. This is the one hint the common
    // ones honour; nothing depends on it being obeyed.
    headers.insert(
        http::HeaderName::from_static("x-accel-buffering"),
        http::HeaderValue::from_static("no"),
    );
    Ok(response)
}

/// How much of the connection budget is left, or `None` past it.
fn remaining(deadline: std::time::Instant) -> Option<Duration> {
    deadline.checked_duration_since(std::time::Instant::now())
}

/// Send one frame, or give up when the connection budget runs out.
///
/// THE TIMEOUT IS THE WHOLE POINT. A bounded channel is the memory bound and a
/// blocking `send` is the backpressure, but a `send` that can block for ever
/// is also a task and a stream slot that live for ever. Past the deadline this
/// returns `false` and the caller drops the stream, which closes the body — a
/// client that has not read for five minutes is not waiting for an `end`
/// frame.
async fn send(
    tx: &tokio::sync::mpsc::Sender<bytes::Bytes>,
    text: String,
    deadline: std::time::Instant,
) -> bool {
    let Some(left) = remaining(deadline) else {
        return false;
    };
    matches!(
        tokio::time::timeout(left, tx.send(bytes::Bytes::from(text))).await,
        Ok(Ok(()))
    )
}

async fn send_view(
    tx: &tokio::sync::mpsc::Sender<bytes::Bytes>,
    event: &str,
    view: &OperationView,
    deadline: std::time::Instant,
) -> bool {
    let Ok(data) = serde_json::to_string(view) else {
        return false;
    };
    let id = view.operation.resource_version.clone();
    let id = if valid_event_id(&id) { Some(id) } else { None };
    send(tx, frame(event, id.as_deref(), &data), deadline).await
}

async fn send_heartbeat(
    tx: &tokio::sync::mpsc::Sender<bytes::Bytes>,
    now: DateTime<Utc>,
    deadline: std::time::Instant,
) -> bool {
    let data = serde_json::json!({ "at": now }).to_string();
    send(tx, frame("heartbeat", None, &data), deadline).await
}

/// The last frame, which must not be lost to its own deadline.
///
/// `end` IS SENT WITH WHAT IS LEFT, AND OTHERWISE WITHOUT WAITING. A client
/// that is reading has room in the channel and receives the reason its stream
/// closed; a client that is not reading has a full channel and gets nothing,
/// which is correct — it has not read a frame for the length of the whole
/// connection and is not waiting for one more. Using the bounded `send` alone
/// would have dropped the `end` frame for EVERY stream that hit the ceiling,
/// because the budget is exhausted at exactly the moment the frame is written.
async fn send_end(
    tx: &tokio::sync::mpsc::Sender<bytes::Bytes>,
    end: StreamEnd,
    deadline: std::time::Instant,
) -> bool {
    let data = serde_json::json!({ "reason": end.as_str() }).to_string();
    let text = frame("end", None, &data);
    if remaining(deadline).is_some() {
        return send(tx, text, deadline).await;
    }
    tx.try_send(bytes::Bytes::from(text)).is_ok()
}

/// A CRD enum's WIRE spelling, not its Rust one.
///
/// `format!("{:?}")` was the obvious way to project one of these and it is
/// wrong for every enum that carries a `rename_all`: `KeyAlgorithm::P256`
/// serialises as `p256` in the object the API server stores, and a projection
/// that published `P256` would publish a value no other surface — the CRD
/// schema, `kubectl get -o json`, the controller's own logs — ever prints. It
/// returns the empty string only for a type that does not serialise to a
/// string at all, which no unit-variant enum does.
#[must_use]
pub fn wire_name<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default()
}
