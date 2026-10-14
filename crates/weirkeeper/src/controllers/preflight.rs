//! The `Preflight` reconciler: run one isolated check Job and say, in codes an
//! operator can act on, what would stop this operation from succeeding.
//!
//! # A `ready` verdict authorizes nothing
//!
//! Everything this controller publishes is advisory. Every execution-time
//! check (the backup controller's glob rail and destination admission, the
//! runner's source rails, the restore admission, phase 0's collision check,
//! the signer validation) still runs, and none of them consults a
//! `Preflight`; `no_execution_path_reads_preflight_or_discovery` in
//! `tests/preflight_controller.rs` is a source scan that holds it. A preview is
//! a statement about a moment: a colliding topic created afterwards is caught
//! at execution and nowhere else.
//!
//! # It proves what only a pod can prove
//!
//! The controller holds no verb on `secrets`
//! (`linkage::the_controller_never_reads_a_secret`), and "the bytes exist" is
//! not "the credential works". So a preflight is one short-lived Job in the
//! same shape an execution pod has (image, ServiceAccount, Secret projections,
//! CA and signing mounts, `automountServiceAccountToken: false`), and it
//! reports what that pod observed: an image pull, a projected key, a broker
//! that accepted the SASL exchange, a bucket that answered a LIST, a signer
//! that loaded.
//!
//! The cost is one pod per preflight, within the namespace's quota. A missing
//! Secret blocks the whole pod, and every Job-sourced row is then reported
//! `unknown` with [`CheckCode::BlockedByPrerequisite`] and the named cause,
//! rather than silently passing.
//!
//! # What this controller writes, and what it never writes
//!
//! It patches `preflights/status`, creates its own plan and result
//! `ConfigMap`s and its own Job, and patches that Job's
//! `ttlSecondsAfterFinished` after the status commit. It patches no target: a
//! restore preflight performs no write of any kind, and the validate-only
//! `CreateTopics` belongs to the runner, inside the pod, with
//! `AdminOptions::validate_only(true)`. `a_restore_preflight_writes_to_no_target`
//! is the structural test.
//!
//! # A verdict that cannot say what it was about is worthless
//!
//! `status.binding` is recorded before the Job is created, from the objects
//! this pass resolved: the recomputed `planHash`, every referent with its UID
//! and generation, the CA digests, the roster, the approval's resource version
//! and the policy digest, reduced to one
//! [`inputs_digest`](logweir_core::check_contract::inputs_digest). The API
//! recomputes it from live objects on every GET and answers
//! `applicable: false` with named `staleReasons` when it differs.
//!
//! This controller keeps the same promise: a terminal `Preflight` whose
//! `result.expiresAt` has passed, or whose referents have moved, is downgraded
//! to `unknown` on the next pass.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Event;
use kube::api::{ListParams, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::runtime::{watcher, Controller};
use kube::{Api, Resource as _, ResourceExt as _};
use serde_json::json;
use tracing::{debug, info, warn};

use logweir_core::check_contract::{
    aggregate, aggregate_expires_at, inputs_digest, redact, ApprovalRef, Authority, BindingInputs,
    CaBundleRef, CheckCode, CheckId, CheckOperation, CheckOutcome, CheckPlan, CheckPlanKind,
    CheckRelay, CheckRequest, CheckScope, CheckState, ConnectionPlan, CredentialMode,
    DestinationPlan, FrameExpectations, Gating, GrantBindingRef, GrantCredentials, GrantRef,
    OperationReadinessRequest, OverallState, Referent, RestorePreflightRequest, RosterRef, Stream,
    CHECK_CONTRACT_VERSION, CHECK_PLAN_CONTRACT,
};
use logweir_core::destination::DestinationRole;

use super::approval::ReconcileError;
use super::Context;
use crate::check::{self, job as check_job, limits, plan as check_plan, policy as check_policy};
use crate::conditions::{current_condition, merge_condition, status_unchanged};
use crate::connection::{self, ConnectionUse, ResolvedConnection};
use crate::crds::approval::{Approval, ApproverKeyWindow};
use crate::crds::backup::Backup;
use crate::crds::kafka_cluster::KafkaCluster;
use crate::crds::preflight::{
    CheckEntry, CheckScope as StatusScope, DetailsRef, Preflight, PreflightBinding,
    PreflightOperation, PreflightResult, PreflightStatus, Referent as StatusReferent,
};
use crate::crds::restore::Restore;
use crate::crds::Condition;
use crate::destination::{self, ResolvedDestination};

/// Nothing has been created yet.
pub const PHASE_PENDING: &str = "Pending";
/// A concurrency ceiling is holding this check back.
pub const PHASE_QUEUED: &str = "Queued";
/// The Job exists and has not finished.
pub const PHASE_RUNNING: &str = "Running";
/// A result was produced. **`Completed` is not `ready`**: the verdict is in
/// `status.result.state`.
pub const PHASE_COMPLETED: &str = "Completed";
/// No result could be produced at all.
pub const PHASE_FAILED: &str = "Failed";
/// The requester asked for it to stop and it did.
pub const PHASE_CANCELLED: &str = "Cancelled";

/// Every phase, so a test can assert the closed set.
pub const PHASES: [&str; 6] = [
    PHASE_PENDING,
    PHASE_QUEUED,
    PHASE_RUNNING,
    PHASE_COMPLETED,
    PHASE_FAILED,
    PHASE_CANCELLED,
];

/// `Complete` — whether this check produced a result at all.
pub const CONDITION_COMPLETE: &str = "Complete";
/// `Ready` — the verdict, restated as a condition. ADVISORY.
pub const CONDITION_READY: &str = "Ready";

/// How long before a running check is looked at again.
pub const REQUEUE_RUNNING_SECS: u64 = 10;
/// How long before a queued check is looked at again.
pub const REQUEUE_QUEUED_SECS: u64 = limits::QUEUED_REQUEUE_SECS;
/// How long before a reconcile that ERRORED is retried.
pub const ERROR_REQUEUE_SECONDS: u64 = 30;
/// The floor on the revalidation requeue of a terminal, still-`ready` check.
///
/// A check whose `expiresAt` is seconds away would otherwise be requeued at a
/// sub-second interval and spin the loop; a check whose expiry has already
/// passed is downgraded on this very pass and never requeued at all.
pub const MIN_REVALIDATE_SECS: u64 = 30;
/// How long a terminal check waits before the collector looks at it again.
///
/// The backstop, not the mechanism: a check whose verdict can still go stale
/// is requeued sooner by [`revalidation_action`], and this wakes the one whose
/// verdict cannot (a `notReady` or `unknown` result that would otherwise wait
/// for ever on `Action::await_change()`).
pub const REQUEUE_TERMINAL_SECS: u64 = 3600;
/// The most objects one garbage-collection pass may delete: a bound on the
/// blast radius and on the API server, not on the total.
pub const GC_MAX_DELETES_PER_PASS: usize = 20;
/// The page size one garbage-collection pass lists.
pub const GC_LIST_LIMIT: u32 = 500;

/// Fifteen minutes, the default expiry.
pub const EXPIRY_DEFAULT: Duration = Duration::from_secs(15 * 60);
/// Ten minutes — the two `approval.*` rows, capped again by the approver key's
/// own `notAfter`.
pub const EXPIRY_APPROVAL: Duration = Duration::from_secs(10 * 60);

/// One row of the catalogue this controller owns.
///
/// The runner owns the **J** rows and stamps their gating and expiry from its
/// own table (`logweir::check::catalogue`). This is the other half: the **C**
/// rows the controller answers from Kubernetes objects, the **P** rows it
/// answers from pod status, and the one **E** row no process observes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Row {
    /// The check id.
    pub id: CheckId,
    /// Whether it gates.
    pub gating: Gating,
    /// Who answered.
    pub authority: Authority,
    /// How long its answer is good for. `None` is "until bytes change": such a
    /// row contributes no `expiresAt`.
    pub expiry: Option<Duration>,
}

const fn row(id: CheckId, gating: Gating, authority: Authority, expiry: Option<Duration>) -> Row {
    Row {
        id,
        gating,
        authority,
        expiry,
    }
}

/// The **C** and **E** rows of a `Backup` readiness check.
pub const BACKUP_CONTROLLER_ROWS: &[Row] = &[
    row(
        CheckId::ConnectionResolved,
        Gating::Blocking,
        Authority::Controller,
        Some(EXPIRY_DEFAULT),
    ),
    // "J or C" — see `DESTINATION_ACCESS_CONTROLLER_ROWS`.
    row(
        CheckId::DestinationEvidenceReadable,
        Gating::Advisory,
        Authority::Controller,
        Some(EXPIRY_DEFAULT),
    ),
    row(
        CheckId::ConnectionClusterIdentity,
        Gating::Blocking,
        Authority::Controller,
        Some(EXPIRY_DEFAULT),
    ),
    row(
        CheckId::DestinationResolved,
        Gating::Blocking,
        Authority::Controller,
        Some(EXPIRY_DEFAULT),
    ),
    row(
        CheckId::SignerRostered,
        Gating::Blocking,
        Authority::Controller,
        Some(EXPIRY_DEFAULT),
    ),
    row(
        CheckId::ConfigurationPolicy,
        Gating::Advisory,
        Authority::Controller,
        Some(EXPIRY_DEFAULT),
    ),
    row(
        CheckId::ConfigurationEgress,
        Gating::ExecutionOnly,
        Authority::Controller,
        None,
    ),
];

/// The **C** and **E** rows of a `DestinationAccess` check.
pub const DESTINATION_ACCESS_CONTROLLER_ROWS: &[Row] = &[
    row(
        CheckId::DestinationResolved,
        Gating::Blocking,
        Authority::Controller,
        Some(EXPIRY_DEFAULT),
    ),
    // "J or C": the controller answers it only when no check pod can
    // (`EvidenceReadPlan::ControllerAnswers`); the Job answers it otherwise.
    row(
        CheckId::DestinationEvidenceReadable,
        Gating::Advisory,
        Authority::Controller,
        Some(EXPIRY_DEFAULT),
    ),
    row(
        CheckId::ConfigurationPolicy,
        Gating::Advisory,
        Authority::Controller,
        Some(EXPIRY_DEFAULT),
    ),
    row(
        CheckId::ConfigurationEgress,
        Gating::ExecutionOnly,
        Authority::Controller,
        None,
    ),
];

/// The **C** and **E** rows of a `Restore` preflight.
pub const RESTORE_CONTROLLER_ROWS: &[Row] = &[
    row(
        CheckId::TargetResolved,
        Gating::Blocking,
        Authority::Controller,
        Some(EXPIRY_DEFAULT),
    ),
    // "J or C", and on a Restore only ever C: reported for an inline-archive
    // point whose scorecard the controller could not read
    // (`legacy_evidence_row`), and advisory because the restore itself is
    // unaffected.
    row(
        CheckId::DestinationEvidenceReadable,
        Gating::Advisory,
        Authority::Controller,
        Some(EXPIRY_DEFAULT),
    ),
    row(
        CheckId::TargetClusterIdentity,
        Gating::Blocking,
        Authority::Controller,
        Some(EXPIRY_DEFAULT),
    ),
    row(
        CheckId::DestinationResolved,
        Gating::Blocking,
        Authority::Controller,
        Some(EXPIRY_DEFAULT),
    ),
    // "Until bytes change": a plan that parses does not stop parsing, and a
    // mapped name that is legal does not become illegal. Both are functions of
    // `planBytes`, which `binding.planHash` already pins.
    row(
        CheckId::PlanParse,
        Gating::Blocking,
        Authority::Controller,
        None,
    ),
    row(
        CheckId::PlanBindings,
        Gating::Blocking,
        Authority::Controller,
        Some(EXPIRY_DEFAULT),
    ),
    row(
        CheckId::PlanNames,
        Gating::Blocking,
        Authority::Controller,
        None,
    ),
    row(
        CheckId::RecoveryPointState,
        Gating::Blocking,
        Authority::Controller,
        Some(EXPIRY_DEFAULT),
    ),
    // Execution-only for a restore, blocking for a backup: the verdict needs
    // the runner's public `signerKeyId` fact, and a `RestorePreflightRequest`
    // carries no `signer_path`, so a restore preflight pod is never given a
    // key. A blocking row nobody can answer would pin every restore preflight
    // at `unknown` for ever. The row is still reported, with
    // `SignerKeyIdNotObserved` and a message naming why.
    row(
        CheckId::SignerRostered,
        Gating::ExecutionOnly,
        Authority::Controller,
        None,
    ),
    row(
        CheckId::ApprovalState,
        Gating::Blocking,
        Authority::Controller,
        Some(EXPIRY_APPROVAL),
    ),
    row(
        CheckId::ApprovalKeyValidity,
        Gating::Advisory,
        Authority::Controller,
        Some(EXPIRY_APPROVAL),
    ),
    row(
        CheckId::ConfigurationPolicy,
        Gating::Advisory,
        Authority::Controller,
        Some(EXPIRY_DEFAULT),
    ),
    row(
        CheckId::ConfigurationEgress,
        Gating::ExecutionOnly,
        Authority::Controller,
        None,
    ),
];

/// The **C** and **E** rows of a `SourceConnection` check.
///
/// The Backup catalogue minus everything about what the connection is used
/// for: no `destination.*` (the request names none), no `signer.*` (projecting
/// a signing key into a connectivity test would widen the pod's blast radius),
/// no `connection.topicsReadable` (no topic was selected). What is left is the
/// connection itself and `configuration.*`.
pub const SOURCE_CONNECTION_CONTROLLER_ROWS: &[Row] = &[
    row(
        CheckId::ConnectionResolved,
        Gating::Blocking,
        Authority::Controller,
        Some(EXPIRY_DEFAULT),
    ),
    row(
        CheckId::ConnectionClusterIdentity,
        Gating::Blocking,
        Authority::Controller,
        Some(EXPIRY_DEFAULT),
    ),
    row(
        CheckId::ConfigurationPolicy,
        Gating::Advisory,
        Authority::Controller,
        Some(EXPIRY_DEFAULT),
    ),
    row(
        CheckId::ConfigurationEgress,
        Gating::ExecutionOnly,
        Authority::Controller,
        None,
    ),
];

/// The **C** and **E** rows of a `Switchover` review (PROD-04.2 §3.8): the
/// `Restore` it is about, the restore's target connection, its evidence
/// destination, and `configuration.*`. No signer (a review signs nothing) and
/// no cluster-identity row: the Job's `switchover.target` holds the target to
/// the cluster id the restore's signed scorecard names, which is the stricter
/// question.
pub const SWITCHOVER_CONTROLLER_ROWS: &[Row] = &[
    row(
        CheckId::SwitchoverRestore,
        Gating::Blocking,
        Authority::Controller,
        Some(EXPIRY_DEFAULT),
    ),
    row(
        CheckId::TargetResolved,
        Gating::Blocking,
        Authority::Controller,
        Some(EXPIRY_DEFAULT),
    ),
    row(
        CheckId::DestinationResolved,
        Gating::Blocking,
        Authority::Controller,
        Some(EXPIRY_DEFAULT),
    ),
    row(
        CheckId::ConfigurationPolicy,
        Gating::Advisory,
        Authority::Controller,
        Some(EXPIRY_DEFAULT),
    ),
    row(
        CheckId::ConfigurationEgress,
        Gating::ExecutionOnly,
        Authority::Controller,
        None,
    ),
];

/// The **P** rows — answered from pod status by `check::waiting`, never by a
/// process inside the pod.
#[must_use]
pub fn pod_rows(operation: PreflightOperation) -> Vec<Row> {
    let mut out = vec![
        row(
            CheckId::RunnerImage,
            Gating::Blocking,
            Authority::PodStatus,
            Some(EXPIRY_DEFAULT),
        ),
        row(
            CheckId::RunnerPod,
            Gating::Blocking,
            Authority::PodStatus,
            Some(EXPIRY_DEFAULT),
        ),
    ];
    // A destination credential row needs a destination: a `SourceConnection`
    // check names none, and a blocking `unknown/PodNotStarted` row about it
    // would pin every connection test at `unknown`.
    if operation != PreflightOperation::SourceConnection {
        out.push(row(
            CheckId::DestinationCredentialProjected,
            Gating::Blocking,
            Authority::PodStatus,
            Some(EXPIRY_DEFAULT),
        ));
    }
    match operation {
        PreflightOperation::Backup | PreflightOperation::SourceConnection => out.push(row(
            CheckId::ConnectionCredentialProjected,
            Gating::Blocking,
            Authority::PodStatus,
            Some(EXPIRY_DEFAULT),
        )),
        PreflightOperation::Restore | PreflightOperation::Switchover => out.push(row(
            CheckId::TargetCredentialProjected,
            Gating::Blocking,
            Authority::PodStatus,
            Some(EXPIRY_DEFAULT),
        )),
        // A destination-access check dials no broker, so there is no
        // connection credential to project and no row to report about one.
        PreflightOperation::DestinationAccess => {}
    }
    out
}

/// The controller's own rows for an operation — **C**, **P** and **E**.
#[must_use]
pub fn controller_rows(operation: PreflightOperation) -> Vec<Row> {
    let base: &[Row] = match operation {
        PreflightOperation::Backup => BACKUP_CONTROLLER_ROWS,
        PreflightOperation::Restore => RESTORE_CONTROLLER_ROWS,
        PreflightOperation::DestinationAccess => DESTINATION_ACCESS_CONTROLLER_ROWS,
        PreflightOperation::SourceConnection => SOURCE_CONNECTION_CONTROLLER_ROWS,
        PreflightOperation::Switchover => SWITCHOVER_CONTROLLER_ROWS,
    };
    let mut out = base.to_vec();
    out.extend(pod_rows(operation));
    out
}

/// The rows the check Job will answer for this rendered plan: pure, and the
/// mirror of the runner's own emission rules.
///
/// A function of the request, not the operation: which `roles` a destination
/// check exercises, whether a `signer_path` was projected, whether an evidence
/// destination was named, whether the restore target is `scratch`. `assemble`
/// renders a row nobody answered as a blocking `unknown`, so any difference
/// from what the runner emits would pin the verdict at `unknown` for ever.
/// `the_expected_rows_are_the_rows_the_runner_emits` holds this against the
/// runner's own fixtures in `crates/logweir/tests/check_cli.rs`, read out of
/// its source because the two crates share no dependency edge.
///
/// `restore_target_is_scratch` comes from the plan bytes a `restorePreflight`
/// mounts: `target.scratchMarker` is the one row that depends on the document
/// rather than the request ([`PlanFacts::scratch`]).
#[must_use]
pub fn job_rows(request: &CheckRequest, restore_target_is_scratch: bool) -> BTreeSet<CheckId> {
    let mut out = BTreeSet::new();
    match request {
        CheckRequest::OperationReadiness(r) => {
            let skip: BTreeSet<CheckId> = r.skip_checks.iter().copied().collect();
            let mut push = |id: CheckId| {
                if !skip.contains(&id) {
                    out.insert(id);
                }
            };
            // `readiness.rs` applies `skip` to `runner.contract` too.
            push(CheckId::RunnerContract);
            match r.operation {
                CheckOperation::Restore | CheckOperation::Switchover => {
                    push(CheckId::TargetAuthenticated);
                }
                // `readiness::connection_ids`' own table, mirrored: a
                // source-connection operation has no topic row.
                CheckOperation::SourceConnection => push(CheckId::ConnectionAuthenticated),
                CheckOperation::Backup | CheckOperation::DestinationAccess => {
                    push(CheckId::ConnectionAuthenticated);
                    push(CheckId::ConnectionTopicsDescribable);
                }
            }
            if r.operation == CheckOperation::Backup {
                push(CheckId::ConnectionTopicsReadable);
            }
            if r.destination.is_some() {
                for role in &r.roles {
                    push(destination_row_for(*role));
                }
            }
            // `readiness.rs` pushes the binding row through the same skip
            // filter, when the plan lists a grant.
            if request.compares_grant_bindings() {
                push(CheckId::DestinationCredentialBound);
            }
            if r.signer_path.is_some() {
                push(CheckId::SignerPrivateKeyUsable);
            }
            // The capability rows the plan lists, and no other: `readiness.rs`
            // emits exactly `capabilityChecks`, through the same skip filter.
            for id in request.capability_checks() {
                push(*id);
            }
        }
        CheckRequest::DestinationAccess(r) => {
            out.insert(CheckId::RunnerContract);
            for role in &r.roles {
                out.insert(destination_row_for(*role));
            }
            if request.compares_grant_bindings() {
                out.insert(CheckId::DestinationCredentialBound);
            }
        }
        // Two rows, without `connection.topicsDescribable`:
        // `kinds::source_connection::run` emits exactly these, and argues the
        // omission there; and (FX-46) every capability row the plan lists,
        // on every path (blocked when the connection did not authenticate).
        CheckRequest::SourceConnection(_) => {
            out.insert(CheckId::RunnerContract);
            out.insert(CheckId::ConnectionAuthenticated);
            for id in request.capability_checks() {
                out.insert(*id);
            }
        }
        // `kinds::switchover::run` emits exactly these four, on every path
        // (a row it could not ask is `unknown`, blocked, never absent).
        CheckRequest::SwitchoverReadiness(_) => {
            out.extend(SWITCHOVER_JOB_ROWS);
        }
        CheckRequest::RestorePreflight(r) => {
            let wanted: BTreeSet<CheckId> = r.checks.iter().copied().collect();
            let skipped: BTreeSet<CheckId> = r.skip_checks.iter().copied().collect();
            let want =
                |id: CheckId| (wanted.is_empty() || wanted.contains(&id)) && !skipped.contains(&id);
            // `restore.rs` pushes `runner.contract` unconditionally, unlike
            // `readiness.rs`, which filters it; this mirrors the runner.
            out.insert(CheckId::RunnerContract);
            for id in [
                CheckId::PlanParse,
                CheckId::ArchiveBackupSet,
                CheckId::ArchiveCoverage,
                CheckId::ArchiveSegments,
                CheckId::TargetAuthenticated,
                CheckId::TargetMappedTopics,
                CheckId::TargetTopicCreate,
                CheckId::TargetTimestampBound,
                CheckId::TargetLogAppendTime,
            ] {
                if want(id) {
                    out.insert(id);
                }
            }
            if restore_target_is_scratch && want(CheckId::TargetScratchMarker) {
                out.insert(CheckId::TargetScratchMarker);
            }
            if r.evidence_destination.is_some() && want(CheckId::DestinationEvidenceWritable) {
                out.insert(CheckId::DestinationEvidenceWritable);
            }
            // `restore.rs` reports it first, through `want`.
            if request.compares_grant_bindings() && want(CheckId::DestinationCredentialBound) {
                out.insert(CheckId::DestinationCredentialBound);
            }
            // `restore.rs` emits every capability row the plan lists, whatever
            // `checks` says: an empty `checks` never includes one, and the plan
            // validation refuses one that is also skipped.
            for id in request.capability_checks() {
                out.insert(*id);
            }
        }
        // This controller never renders these: a topic inventory belongs to
        // `TopicDiscovery`, an evidence fetch to verification, and a
        // `catalogSync` (a relayed body, not a row set) to `RecoveryCatalog`.
        CheckRequest::TopicInventory(_)
        | CheckRequest::EvidenceFetch(_)
        | CheckRequest::CatalogSync(_) => {}
    }
    out
}

/// The capability rows a plan for `operation` asks its runner for: every one
/// the pure layer defines for it
/// (`logweir_core::check_contract::capability_checks_for`), minus the ones the
/// request skips.
///
/// A capability row is emitted only when the plan lists it, so this list is
/// what makes a `Preflight` name a missing capability at all. A skipped one is
/// reported `skipped` by [`assemble`]; the plan validation refuses an id that
/// is both listed and skipped.
#[must_use]
pub fn capability_checks(operation: CheckOperation, skip: &[CheckId]) -> Vec<CheckId> {
    logweir_core::check_contract::capability_checks_for(operation)
        .iter()
        .copied()
        .filter(|id| !skip.contains(id))
        .collect()
}

/// The one row a destination role produces — `access.rs`'s `match`, mirrored.
#[must_use]
pub fn destination_row_for(role: DestinationRole) -> CheckId {
    match role {
        DestinationRole::ArchiveRead => CheckId::DestinationArchiveListable,
        DestinationRole::ArchiveWrite => CheckId::DestinationArchivePrefixWritable,
        DestinationRole::EvidenceWrite => CheckId::DestinationEvidenceWritable,
        DestinationRole::EvidenceRead => CheckId::DestinationEvidenceReadable,
    }
}

/// The rows to report `unknown` for when NO plan could be rendered at all.
///
/// The rows a `switchoverReadiness` Job answers.
pub const SWITCHOVER_JOB_ROWS: [CheckId; 4] = [
    CheckId::RunnerContract,
    CheckId::SwitchoverEvidence,
    CheckId::SwitchoverTarget,
    CheckId::SwitchoverGroups,
];

/// Used only when a blocking controller row refused before a `CheckRequest`
/// existed. Every row is `unknown` either way; listing them makes the verdict
/// name the unanswered questions instead of looking short and green.
#[must_use]
pub fn unrendered_job_rows(operation: PreflightOperation) -> BTreeSet<CheckId> {
    match operation {
        PreflightOperation::Backup => [
            CheckId::RunnerContract,
            CheckId::ConnectionAuthenticated,
            CheckId::ConnectionTopicsDescribable,
            CheckId::DestinationArchiveListable,
            CheckId::DestinationEvidenceWritable,
            CheckId::SignerPrivateKeyUsable,
        ]
        .into_iter()
        .collect(),
        PreflightOperation::DestinationAccess => [
            CheckId::RunnerContract,
            CheckId::DestinationArchiveListable,
            CheckId::DestinationEvidenceWritable,
        ]
        .into_iter()
        .collect(),
        PreflightOperation::SourceConnection => {
            [CheckId::RunnerContract, CheckId::ConnectionAuthenticated]
                .into_iter()
                .collect()
        }
        PreflightOperation::Switchover => SWITCHOVER_JOB_ROWS.into_iter().collect(),
        PreflightOperation::Restore => [
            CheckId::RunnerContract,
            // `plan.parse` is absent: the controller answers it (it holds the
            // claimed hash and the bytes).
            CheckId::TargetAuthenticated,
            CheckId::TargetMappedTopics,
            CheckId::TargetTopicCreate,
            CheckId::TargetTimestampBound,
            CheckId::ArchiveBackupSet,
            CheckId::ArchiveCoverage,
            CheckId::ArchiveSegments,
        ]
        .into_iter()
        .collect(),
    }
}

/// The plan kind an operation runs.
#[must_use]
pub fn plan_kind(operation: PreflightOperation) -> CheckPlanKind {
    match operation {
        PreflightOperation::Backup => CheckPlanKind::OperationReadiness,
        PreflightOperation::Restore => CheckPlanKind::RestorePreflight,
        PreflightOperation::DestinationAccess => CheckPlanKind::DestinationAccess,
        PreflightOperation::SourceConnection => CheckPlanKind::SourceConnection,
        PreflightOperation::Switchover => CheckPlanKind::SwitchoverReadiness,
    }
}

/// The pure-layer spelling of an operation, for the plan and the binding.
#[must_use]
pub fn check_operation(operation: PreflightOperation) -> CheckOperation {
    match operation {
        PreflightOperation::Backup => CheckOperation::Backup,
        PreflightOperation::Restore => CheckOperation::Restore,
        PreflightOperation::DestinationAccess => CheckOperation::DestinationAccess,
        PreflightOperation::SourceConnection => CheckOperation::SourceConnection,
        PreflightOperation::Switchover => CheckOperation::Switchover,
    }
}

/// The one construction site for a controller-owned outcome, so gating and
/// expiry are properties of the row rather than the call site, and
/// `every_controller_row_has_a_catalogue_entry` is a real guard.
///
/// # Panics
/// Never in practice: only for a [`CheckId`] absent from every table above,
/// which is a programming error this crate's own tests catch.
#[must_use]
pub fn outcome(
    operation: PreflightOperation,
    id: CheckId,
    state: CheckState,
    code: CheckCode,
    now: DateTime<Utc>,
) -> CheckOutcome {
    let r = controller_rows(operation)
        .into_iter()
        .find(|r| r.id == id)
        .unwrap_or_else(|| {
            panic!("{id} is not a row the Preflight controller owns for operation {operation:?}")
        });
    let mut out = CheckOutcome::new(id, state, r.gating, r.authority, code);
    out.observed_at = Some(now);
    out.expires_at = r.expiry.and_then(|d| add(now, d));
    out
}

fn add(now: DateTime<Utc>, d: Duration) -> Option<DateTime<Utc>> {
    chrono::Duration::from_std(d)
        .ok()
        .and_then(|d| now.checked_add_signed(d))
}

/// Pull an outcome's expiry forward to `limit` when `limit` is sooner.
///
/// A verdict about a key must not outlive the key: two rows' expiry is
/// `min(15 m, key notAfter)` and `min(10 m, notAfter)`.
#[must_use]
pub fn cap_expiry(mut out: CheckOutcome, limit: Option<DateTime<Utc>>) -> CheckOutcome {
    if let Some(limit) = limit {
        out.expires_at = Some(match out.expires_at {
            Some(e) if e <= limit => e,
            _ => limit,
        });
    }
    out
}

/// `connection.resolved` / `target.resolved`.
///
/// The mapping from the connection contract's terminal states to `CheckCode`s
/// lives here, not in `connection::resolve`: a `CheckCode` is preflight
/// vocabulary and a terminal state is execution vocabulary.
#[must_use]
pub fn connection_row(
    operation: PreflightOperation,
    resolved: Option<&Result<ResolvedConnection, connection::ConnectionRefusal>>,
    cluster_name: &str,
    now: DateTime<Utc>,
) -> CheckOutcome {
    let id = match operation {
        PreflightOperation::Restore | PreflightOperation::Switchover => CheckId::TargetResolved,
        _ => CheckId::ConnectionResolved,
    };
    let scope = CheckScope {
        kind: "KafkaCluster".to_string(),
        name: cluster_name.to_string(),
        uid: None,
    };
    match resolved {
        None => outcome(
            operation,
            id,
            CheckState::NotReady,
            CheckCode::ConnectionNotFound,
            now,
        )
        .with_scope(scope)
        .with_message(&format!(
            "no KafkaCluster named `{cluster_name}` exists in this namespace"
        ))
        .with_remedy(
            "Create the KafkaCluster this check names, or point the check at one that exists. \
             A connection reference is resolved only in its own namespace.",
        ),
        Some(Ok(c)) => outcome(operation, id, CheckState::Ready, CheckCode::Resolved, now)
            .with_scope(CheckScope {
                uid: c.uid.clone(),
                ..scope
            })
            .with_message(&format!(
                "the connection resolves to {} broker address(es) as {}",
                c.bootstrap_servers.len(),
                c.principal
            )),
        Some(Err(refusal)) => {
            let code = match refusal.reason {
                crate::conditions::TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE => {
                    CheckCode::CredentialReferenceMissing
                }
                _ => CheckCode::ConnectionInvalid,
            };
            outcome(operation, id, CheckState::NotReady, code, now)
                .with_scope(scope)
                .with_message(&refusal.message)
                .with_remedy(&format!(
                    "Fix `{}` on the KafkaCluster and create a new Preflight; spec.request is \
                     immutable.",
                    refusal.field
                ))
        }
    }
}

/// `destination.resolved`.
///
/// `DestinationRefusal` already carries a [`CheckCode`] (validation and the
/// catalogue share one closed set), so there is no second mapping here.
#[must_use]
pub fn destination_row(
    operation: PreflightOperation,
    resolved: &Result<ResolvedDestination, destination::DestinationRefusal>,
    name: &str,
    now: DateTime<Utc>,
) -> CheckOutcome {
    let scope = CheckScope {
        kind: "BackupDestination".to_string(),
        name: name.to_string(),
        uid: None,
    };
    match resolved {
        Ok(d) => outcome(
            operation,
            CheckId::DestinationResolved,
            CheckState::Ready,
            CheckCode::DestinationValid,
            now,
        )
        .with_scope(CheckScope {
            uid: Some(d.uid.clone()),
            ..scope
        })
        .with_message(&format!(
            "the destination is Valid at generation {} for the {} grant",
            d.generation,
            d.role.as_str()
        )),
        Err(refusal) => outcome(
            operation,
            CheckId::DestinationResolved,
            CheckState::NotReady,
            refusal.code,
            now,
        )
        .with_scope(scope)
        .with_message(&refusal.message)
        .with_remedy(&format!(
            "Fix `{}` on the BackupDestination, wait for its Valid condition to catch up with \
             the new generation, and create a new Preflight.",
            refusal.field
        )),
    }
}

/// `configuration.policy`, advisory.
#[must_use]
pub fn policy_row(
    operation: PreflightOperation,
    load: &check_policy::PolicyLoad,
    now: DateTime<Utc>,
) -> CheckOutcome {
    let code = load.code();
    let state = if load.is_readable() {
        CheckState::Ready
    } else {
        CheckState::NotReady
    };
    let out = outcome(operation, CheckId::ConfigurationPolicy, state, code, now);
    match load {
        check_policy::PolicyLoad::Unreadable { reason, .. } => {
            out.with_message(reason).with_remedy(
                "Fix the installation policy ConfigMap. Until it parses the controller uses a \
                 fail-closed policy: no visibility attestation applies and no ControllerIdentity \
                 location is allowlisted.",
            )
        }
        check_policy::PolicyLoad::Defaulted(_) => {
            out.with_message("no installation policy is configured; the documented defaults apply")
        }
        check_policy::PolicyLoad::Loaded(_) => {
            out.with_message("the installation policy parsed and validated")
        }
    }
}

/// `configuration.egress`, an execution-only row.
///
/// `unknown` for ever, by construction: Docker Desktop does not enforce
/// NetworkPolicy, an enforcing cluster does it at the CNI, and neither the
/// controller nor a pod that got through can tell whether the deny path is
/// closed. `CheckOutcome::new` forces an execution-only row to `unknown`, so
/// the remedy is the whole content.
#[must_use]
pub fn egress_row(
    operation: PreflightOperation,
    broker_ports: &[String],
    store_ports: &[String],
    now: DateTime<Utc>,
) -> CheckOutcome {
    let ports = |v: &[String]| {
        if v.is_empty() {
            "<none observed>".to_string()
        } else {
            v.join(", ")
        }
    };
    outcome(
        operation,
        CheckId::ConfigurationEgress,
        CheckState::Unknown,
        CheckCode::NetworkPolicyEnforcementNotObservable,
        now,
    )
    .with_message(
        "whether egress is permitted is decided by a NetworkPolicy this check cannot observe",
    )
    .with_remedy(&format!(
        "If this namespace has a default-deny NetworkPolicy, allow egress to the broker \
         port(s) {} and the object-store port(s) {}. A non-standard endpoint port is the case \
         this row exists for.",
        ports(broker_ports),
        ports(store_ports)
    ))
}

/// What the controller knows about the namespace's trust: the reduced fact
/// behind `signer.rostered` and the restore allowlist.
///
/// The fields below are `TrustRoster/default`'s, and they are the trust when
/// [`Self::governing`] is `None` (the namespace resolves to the synthesised
/// `legacy-roster-v1`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RosterFacts {
    /// Whether a `TrustRoster` object was found at all.
    pub found: bool,
    /// Its UID, for the binding.
    pub uid: String,
    /// Its generation, for the binding.
    pub generation: i64,
    /// `(keyId, notAfter)` for every signing key.
    pub signing_keys: Vec<(String, Option<DateTime<Utc>>)>,
    /// `(keyId, notAfter)` for every approver key.
    pub approver_keys: Vec<(String, Option<DateTime<Utc>>)>,
    /// The cluster ids a restore may target.
    pub allowed_cluster_ids: Vec<String>,
    /// The namespace's trust when a `TrustPolicy` governs it, or when two
    /// contest it: [`crate::trust::resolve`]'s answer, which the `Approval`
    /// controller, the catalog and the runner's bundles use. `None` is the
    /// roster fallback above.
    pub governing: Option<GoverningTrust>,
    /// On the roster fallback, every roster key a `TrustPolicy` revoked for
    /// `KeyCompromise`, with the policies recording it. The roster fields carry
    /// no lifecycle, so without this a namespace on the roster would answer
    /// `ready/SignerRostered` for a compromised signer that every verification
    /// in the same namespace refuses.
    pub compromised_keys: Vec<(String, Vec<String>)>,
}

/// A namespace's trust when the roster is NOT the answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GoverningTrust {
    /// One `TrustPolicy` governs it (an explicit `spec.namespaces` match, or
    /// the one `default: true` policy).
    Policy(Box<crate::trust::ResolvedTrust>),
    /// Two or more claim it, so it resolves to no trust at all
    /// ([`crate::trust::REASON_TRUST_POLICY_CONFLICT`]). Sorted names.
    Conflict(Vec<String>),
    /// The `TrustPolicy` list could not be read, so which trust governs the
    /// namespace has no answer. The signer row is `unknown`, never the
    /// roster's answer; the preflight still runs (a scoped install without
    /// the cluster-scoped trust grant must not lose every other row).
    Unreadable(String),
}

impl RosterFacts {
    /// Fold [`crate::trust::resolve`]'s answer into the roster's facts: a
    /// governing policy or a conflict is recorded; the legacy roster and an
    /// unconfigured namespace leave [`Self::governing`] `None`, because the
    /// roster fields already say exactly that.
    #[must_use]
    pub fn with_resolution(
        mut self,
        resolution: Result<&crate::trust::Resolution, &kube::Error>,
    ) -> Self {
        let resolution = match resolution {
            Ok(resolution) => resolution,
            Err(e) => {
                self.governing = Some(GoverningTrust::Unreadable(format!(
                    "the namespace's TrustPolicy resolution could not be read: {e}"
                )));
                return self;
            }
        };
        self.governing = match resolution {
            crate::trust::Resolution::Trust(trust) if !trust.source.is_legacy() => {
                Some(GoverningTrust::Policy(trust.clone()))
            }
            crate::trust::Resolution::Conflict { policies, .. } => {
                Some(GoverningTrust::Conflict(policies.clone()))
            }
            _ => None,
        };
        if let crate::trust::Resolution::Trust(trust) = resolution {
            if trust.source.is_legacy() {
                self.compromised_keys = trust
                    .keys
                    .iter()
                    .filter(|k| !k.compromise_inherited_from.is_empty())
                    .map(|k| (k.trust.key_id.clone(), k.compromise_inherited_from.clone()))
                    .collect();
            }
        }
        self
    }

    /// The cluster ids a scratch restore may target, the list the runner's
    /// `allowed-clusters.json` is rendered from: the governing policy's
    /// `allowedTargetClusterIds`, the roster's `allowedClusterIds` when none
    /// governs, nothing in a contested namespace. `None` when the policy list
    /// could not be read: the roster's list is not the answer then, and
    /// [`cluster_identity_under_trust`] turns it into `unknown`, `TrustUnknown`.
    #[must_use]
    pub fn restore_allowlist(&self) -> Option<&[String]> {
        match &self.governing {
            None => Some(&self.allowed_cluster_ids),
            Some(GoverningTrust::Policy(trust)) => Some(&trust.allowed_target_cluster_ids),
            Some(GoverningTrust::Conflict(_)) => Some(&[]),
            Some(GoverningTrust::Unreadable(_)) => None,
        }
    }

    /// The object a trust row's remedy is about.
    fn scope(&self) -> CheckScope {
        match &self.governing {
            None => CheckScope {
                kind: "TrustRoster".to_string(),
                name: crate::ROSTER_NAME.to_string(),
                uid: (self.found && !self.uid.is_empty()).then(|| self.uid.clone()),
            },
            Some(GoverningTrust::Policy(trust)) => {
                let uid = match &trust.source {
                    crate::trust::TrustSource::Policy { uid, .. } => uid.clone(),
                    crate::trust::TrustSource::LegacyRoster => None,
                };
                CheckScope {
                    kind: "TrustPolicy".to_string(),
                    name: trust.source.name().to_string(),
                    uid,
                }
            }
            Some(GoverningTrust::Conflict(policies)) => CheckScope {
                kind: "TrustPolicy".to_string(),
                name: policies.join(","),
                uid: None,
            },
            Some(GoverningTrust::Unreadable(_)) => CheckScope {
                kind: "TrustPolicy".to_string(),
                name: String::new(),
                uid: None,
            },
        }
    }

    /// The roster's contribution to the binding digest.
    #[must_use]
    pub fn binding(&self) -> RosterRef {
        RosterRef {
            uid: self.uid.clone(),
            generation: self.generation,
        }
    }
}

/// Whether a relayed string is a signing key id at all.
///
/// The sha256 of a SubjectPublicKeyInfo DER, lowercase hex: the form of
/// `logweir_core::trust::TrustedKey::key_id` and of the roster's key ids.
/// Nothing else is comparable against the roster.
#[must_use]
pub fn is_signer_key_id(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// `signer.rostered`.
///
/// The public key id is a fact the check Job reports (`signerKeyId` on
/// `signer.privateKeyUsable`), derived from a private key the controller may
/// not read. Before the pod relays, the row can only say
/// `SignerKeyIdNotObserved`; `SignerNotRostered` would be a claim about a key
/// nobody has seen.
///
/// The scope is the trust the namespace resolved to: every answer is about
/// one trust object, and most remedies are "edit that object". With no
/// `TrustPolicy` governing, that is the cluster-scoped `TrustRoster`; when a
/// policy governs, the key is judged against that policy (the trust the
/// `Approval` controller, the catalog and the runner's bundles use) by
/// [`signer_policy_row`]. The row carries the object's UID once read, so a
/// verdict against a replaced object is visibly about a different one.
#[must_use]
pub fn signer_rostered_row(
    operation: PreflightOperation,
    roster: &RosterFacts,
    observed_key_id: Option<&str>,
    now: DateTime<Utc>,
) -> CheckOutcome {
    let id = CheckId::SignerRostered;
    let roster_scope = roster.scope();
    if operation == PreflightOperation::Restore {
        // See `RESTORE_CONTROLLER_ROWS`: no restore pod reports a key id.
        // `CheckOutcome::new` forces an execution-only row to `unknown`.
        return outcome(
            operation,
            id,
            CheckState::Unknown,
            CheckCode::SignerKeyIdNotObserved,
            now,
        )
        .with_message(
            "a restore preflight's check plan projects no signing key, so which key this restore \
             would sign its evidence with is established by the run",
        )
        .with_remedy(
            "The runner still validates its signer before it writes anything, and \
             `TrustRoster.spec.signingKeys` is what makes that evidence verifiable.",
        )
        .with_scope(roster_scope);
    }
    if let Some(governing) = roster.governing.as_ref() {
        return signer_policy_row(operation, governing, observed_key_id, now)
            .with_scope(roster_scope);
    }
    if !roster.found {
        return outcome(
            operation,
            id,
            CheckState::NotReady,
            CheckCode::TrustRosterNotFound,
            now,
        )
        .with_message("no cluster-scoped TrustRoster named `default` exists")
        .with_remedy(
            "Create the TrustRoster and add the runner's PUBLIC signing key to \
             `spec.signingKeys`; evidence signed by an unrostered key verifies nowhere.",
        )
        .with_scope(roster_scope);
    }
    if roster.signing_keys.is_empty() {
        return outcome(
            operation,
            id,
            CheckState::NotReady,
            CheckCode::TrustRosterNotLoaded,
            now,
        )
        .with_message("the TrustRoster carries no signing key")
        .with_remedy("Add the runner's public signing key to `spec.signingKeys`.")
        .with_scope(roster_scope);
    }
    // A relayed value that is not a key id is not a key id. The roster is
    // written in sha256 of a SubjectPublicKeyInfo DER, so anything that is not
    // 64 lowercase hex characters (for example a value the redactor replaced
    // with `[redacted]`) did not come from `ValidatedSigner`. The honest answer
    // is `unknown`, as for a pod that has not reported, not a blocking refusal
    // about a key nobody has seen. The redactor also keeps key ids
    // ([`logweir_core::check_contract::redact`]'s public-identifier exemption).
    let observed_key_id = observed_key_id.filter(|k| is_signer_key_id(k));
    let Some(key_id) = observed_key_id else {
        return outcome(
            operation,
            id,
            CheckState::Unknown,
            CheckCode::SignerKeyIdNotObserved,
            now,
        )
        .with_message(
            "the check pod has not reported which signing key it holds, so it cannot be \
             matched against the roster",
        )
        .with_scope(roster_scope);
    };
    let Some((_, not_after)) = roster.signing_keys.iter().find(|(k, _)| k == key_id) else {
        return outcome(
            operation,
            id,
            CheckState::NotReady,
            CheckCode::SignerNotRostered,
            now,
        )
        .with_message(&format!(
            "the runner holds signing key `{key_id}`, which the TrustRoster does not list"
        ))
        .with_remedy(
            "Add this key id to `TrustRoster.spec.signingKeys`, or project the signing key \
             the roster already trusts.",
        )
        .with_scope(roster_scope);
    };
    // A `KeyCompromise` revocation on any `TrustPolicy` reaches the roster: the
    // key may not sign anything new, as `signer_policy_row` answers
    // `KeyRevoked`, and the row says where the revocation is recorded.
    if let Some((_, recorded_by)) = roster.compromised_keys.iter().find(|(k, _)| k == key_id) {
        return outcome(
            operation,
            id,
            CheckState::NotReady,
            CheckCode::SignerKeyExpired,
            now,
        )
        .with_message(&format!(
            "the TrustRoster lists signing key `{key_id}`, but TrustPolicy/{} revoked it for \
             KeyCompromise, which applies to every namespace: it may not sign new evidence \
             (KeyRevoked)",
            recorded_by.join(", TrustPolicy/")
        ))
        .with_remedy(
            "Rotate the runner's signing key to a new key trusted by this namespace's trust, and \
             re-create the TrustRoster without the compromised key.",
        )
        .with_scope(roster_scope);
    }
    if not_after.is_some_and(|t| t <= now) {
        return cap_expiry(
            outcome(
                operation,
                id,
                CheckState::NotReady,
                CheckCode::SignerKeyExpired,
                now,
            )
            .with_message(&format!(
                "the roster entry for signing key `{key_id}` expired at {}",
                not_after.map(|t| t.to_rfc3339()).unwrap_or_default()
            ))
            .with_remedy("Rotate the runner's signing key and roster the new public half.")
            .with_scope(roster_scope),
            *not_after,
        );
    }
    cap_expiry(
        outcome(
            operation,
            id,
            CheckState::Ready,
            CheckCode::SignerRostered,
            now,
        )
        .with_message(&format!("signing key `{key_id}` is on the TrustRoster"))
        .with_fact("signerKeyId", key_id)
        .with_scope(roster_scope),
        *not_after,
    )
}

/// `signer.rostered` in a namespace a `TrustPolicy` governs (or two contest).
///
/// Judged by the new-signature rule: the key is the one the run is about to
/// sign evidence with, so the question is
/// [`logweir_core::trust::may_sign_new`]'s (`Active`,
/// `notBefore ≤ now < notAfter`, `EvidenceSigning`), resolved by key id through
/// [`crate::trust::ResolvedTrust::may_sign_new_for`]. A retired key still
/// verifies what it signed, but evidence it wrote today would be
/// `SignedOutsideValidity` when read. The key must also be usable (its PEM
/// parses and hashes to its id), because only usable keys reach a verifier.
///
/// The codes are the roster row's, so a console needs no second vocabulary: a
/// conflict or a policy with no usable signing key is `TrustRosterNotLoaded`,
/// a key the policy does not carry for `EvidenceSigning` is
/// `SignerNotRostered`, a listed key that may not sign now is
/// `SignerKeyExpired` naming the refusal, and an unreadable policy list is
/// `unknown`, `TrustUnknown`.
fn signer_policy_row(
    operation: PreflightOperation,
    governing: &GoverningTrust,
    observed_key_id: Option<&str>,
    now: DateTime<Utc>,
) -> CheckOutcome {
    use logweir_core::trust::{KeyUsage, SigningRefusal};
    let id = CheckId::SignerRostered;
    let trust = match governing {
        GoverningTrust::Conflict(policies) => {
            return outcome(
                operation,
                id,
                CheckState::NotReady,
                CheckCode::TrustRosterNotLoaded,
                now,
            )
            .with_message(&format!(
                "this namespace is claimed by more than one TrustPolicy ({}), so it resolves to no \
                 trust and evidence signed here verifies nowhere",
                policies.join(", ")
            ))
            .with_remedy("Remove the namespace from all but one TrustPolicy's spec.namespaces.");
        }
        GoverningTrust::Unreadable(detail) => {
            return outcome(
                operation,
                id,
                CheckState::Unknown,
                CheckCode::TrustUnknown,
                now,
            )
            .with_message(detail)
            .with_remedy(
                "Grant the controller list/watch on trustpolicies (the chart's ClusterRole does), \
                 then run this check again.",
            );
        }
        GoverningTrust::Policy(trust) => trust,
    };
    let policy = trust.source.name();
    if trust.keys_for(KeyUsage::EvidenceSigning).next().is_none() {
        return outcome(
            operation,
            id,
            CheckState::NotReady,
            CheckCode::TrustRosterNotLoaded,
            now,
        )
        .with_message(&format!(
            "TrustPolicy `{policy}` governs this namespace and carries no usable EvidenceSigning key"
        ))
        .with_remedy(&format!(
            "Add the runner's public signing key to TrustPolicy `{policy}` with usage \
             EvidenceSigning."
        ));
    }
    let Some(key_id) = observed_key_id.filter(|k| is_signer_key_id(k)) else {
        return outcome(
            operation,
            id,
            CheckState::Unknown,
            CheckCode::SignerKeyIdNotObserved,
            now,
        )
        .with_message(&format!(
            "the check pod has not reported which signing key it holds, so it cannot be \
             matched against TrustPolicy `{policy}`"
        ));
    };
    let listed = trust
        .key(key_id)
        .filter(|k| k.trust.has_usage(KeyUsage::EvidenceSigning) && k.is_usable());
    let Some(key) = listed else {
        return outcome(
            operation,
            id,
            CheckState::NotReady,
            CheckCode::SignerNotRostered,
            now,
        )
        .with_message(&format!(
            "the runner holds signing key `{key_id}`, which TrustPolicy `{policy}` does not \
             carry as a usable EvidenceSigning key"
        ))
        .with_remedy(&format!(
            "Add this key id to TrustPolicy `{policy}` with usage EvidenceSigning, or project \
             the signing key the policy already trusts."
        ));
    };
    match trust.may_sign_new_for(key_id, KeyUsage::EvidenceSigning, now) {
        Ok(()) => cap_expiry(
            outcome(
                operation,
                id,
                CheckState::Ready,
                CheckCode::SignerRostered,
                now,
            )
            .with_message(&format!(
                "signing key `{key_id}` may sign new evidence under TrustPolicy `{policy}`"
            ))
            .with_fact("signerKeyId", key_id),
            Some(key.trust.not_after),
        ),
        Err(refusal) => {
            let row = outcome(
                operation,
                id,
                CheckState::NotReady,
                CheckCode::SignerKeyExpired,
                now,
            )
            .with_message(&format!(
                "TrustPolicy `{policy}` carries signing key `{key_id}` but it may not sign new \
                 evidence now: {}",
                refusal.as_str()
            ))
            .with_remedy(
                "Rotate the runner's signing key to one the policy carries as Active \
                 EvidenceSigning, inside its notBefore/notAfter window.",
            );
            // A key whose window has not opened becomes usable at `notBefore`,
            // so the verdict is re-asked then; every other refusal is final.
            if refusal == SigningRefusal::KeyNotYetValid {
                cap_expiry(row, Some(key.trust.not_before))
            } else {
                row
            }
        }
    }
}

/// [`cluster_identity_row`] against the namespace's trust, or `unknown` when
/// that trust could not be read and the row's answer would depend on the
/// allowlist.
///
/// The allowlist decides a scratch restore's `TargetNotAllowlisted` and a
/// backup's `SourceIsAllowlistedTarget`. With the `TrustPolicy` list
/// unreadable neither can be answered, so a `ready` (or an allowlist-only
/// refusal) becomes a blocking `unknown`, `TrustUnknown`: never `ready` on a
/// guess. A row decided before the allowlist (identity not observed or
/// changed, target is the source) keeps its answer: those are facts about the
/// broker.
#[must_use]
pub fn cluster_identity_under_trust(
    operation: PreflightOperation,
    observed: Option<&str>,
    recorded: Option<&str>,
    trust: &RosterFacts,
    source_cluster_id: Option<&str>,
    scratch: bool,
    now: DateTime<Utc>,
) -> CheckOutcome {
    if let Some(allowlist) = trust.restore_allowlist() {
        return cluster_identity_row(
            operation,
            observed,
            recorded,
            allowlist,
            source_cluster_id,
            scratch,
            now,
        );
    }
    let row = cluster_identity_row(
        operation,
        observed,
        recorded,
        &[],
        source_cluster_id,
        scratch,
        now,
    );
    let consults_the_allowlist = match operation {
        PreflightOperation::Restore => scratch,
        PreflightOperation::Backup => true,
        _ => false,
    };
    let answered_by_the_allowlist =
        row.state == CheckState::Ready || row.code == CheckCode::TargetNotAllowlisted;
    if !(consults_the_allowlist && answered_by_the_allowlist) {
        return row;
    }
    let detail = match &trust.governing {
        Some(GoverningTrust::Unreadable(detail)) => detail.clone(),
        _ => "the namespace's trust could not be read".to_string(),
    };
    let mut unknown = outcome(
        operation,
        row.id,
        CheckState::Unknown,
        CheckCode::TrustUnknown,
        now,
    )
    .with_message(&format!(
        "{detail}, so whether this cluster is on the namespace's restore allowlist has no answer"
    ))
    .with_remedy(
        "Grant the controller list/watch on trustpolicies (the chart's ClusterRole does), then \
         run this check again.",
    )
    .with_scope(trust.scope());
    if let Some(observed) = observed {
        unknown = unknown.with_fact("clusterId", observed);
    }
    unknown
}

/// `connection.clusterIdentity` / `target.clusterIdentity`, the **J+C** row:
/// the check Job reports the observed cluster id, and the verdict needs
/// `KafkaCluster.status.clusterId` and the allowlist, which a
/// credential-holding Job has no Kubernetes read for.
#[must_use]
pub fn cluster_identity_row(
    operation: PreflightOperation,
    observed: Option<&str>,
    recorded: Option<&str>,
    allowed_cluster_ids: &[String],
    source_cluster_id: Option<&str>,
    scratch: bool,
    now: DateTime<Utc>,
) -> CheckOutcome {
    let id = match operation {
        PreflightOperation::Restore => CheckId::TargetClusterIdentity,
        _ => CheckId::ConnectionClusterIdentity,
    };
    let Some(observed) = observed else {
        return outcome(
            operation,
            id,
            CheckState::Unknown,
            CheckCode::ClusterIdentityNotObserved,
            now,
        )
        .with_message("the check pod has not reported the broker's cluster id");
    };
    if let Some(recorded) = recorded.filter(|r| !r.is_empty()) {
        if recorded != observed {
            return outcome(
                operation,
                id,
                CheckState::NotReady,
                CheckCode::ClusterIdentityChanged,
                now,
            )
            .with_message(&format!(
                "the broker reports cluster id `{observed}` and the KafkaCluster's status \
                 records `{recorded}`"
            ))
            .with_remedy(
                "The bootstrap address now points at a different cluster. Confirm which \
                 cluster is meant before running anything against it.",
            )
            .with_fact("clusterId", observed);
        }
    }
    match operation {
        PreflightOperation::Restore => {
            if source_cluster_id.is_some_and(|s| s == observed) {
                return outcome(
                    operation,
                    id,
                    CheckState::NotReady,
                    CheckCode::TargetEqualsSource,
                    now,
                )
                .with_message(
                    "the restore target is the cluster the archive was taken from; a restore \
                     into the source cluster is refused",
                )
                .with_fact("clusterId", observed);
            }
            // The allowlist is a scratch-mode rule: a `newTopic` restore writes
            // into a named cluster on purpose.
            if scratch && !allowed_cluster_ids.iter().any(|a| a == observed) {
                return outcome(
                    operation,
                    id,
                    CheckState::NotReady,
                    CheckCode::TargetNotAllowlisted,
                    now,
                )
                .with_message(&format!(
                    "cluster id `{observed}` is not in this namespace's restore allowlist (the \
                     governing TrustPolicy's spec.allowedTargetClusterIds, or \
                     TrustRoster.spec.allowedClusterIds when no policy governs)"
                ))
                .with_remedy(
                    "Add the scratch cluster's id to the governing TrustPolicy's \
                     `spec.allowedTargetClusterIds` (or `TrustRoster.spec.allowedClusterIds` when \
                     no policy governs the namespace). The allowlist is read from the trust and \
                     never from a plan.",
                )
                .with_fact("clusterId", observed);
            }
            outcome(
                operation,
                id,
                CheckState::Ready,
                CheckCode::TargetAllowed,
                now,
            )
            .with_message("the target cluster is allowed for this restore")
            .with_fact("clusterId", observed)
        }
        _ => {
            // A source whose id is on the restore allowlist is a scratch
            // cluster, which the runner refuses to back up; this reports it
            // before the run. A `Backup` verdict only: a `SourceConnection`
            // check asks whether the connection answers, not whether the
            // cluster may be backed up, so a dial to a legitimate restore
            // target must not read `not ready`. `ClusterIdentityChanged` above
            // stays blocking for both: dialling a different cluster than the
            // object records is a fact about the connection itself.
            if operation == PreflightOperation::Backup
                && allowed_cluster_ids.iter().any(|a| a == observed)
            {
                return outcome(
                    operation,
                    id,
                    CheckState::NotReady,
                    CheckCode::SourceIsAllowlistedTarget,
                    now,
                )
                .with_message(&format!(
                    "cluster id `{observed}` is on this namespace's restore allowlist, which \
                     marks it a restore TARGET; the runner's phase -1 rail refuses to back one up"
                ))
                .with_remedy(
                    "Back up the production cluster, not the scratch target. If this really is \
                     the source, remove its id from the restore allowlist.",
                )
                .with_fact("clusterId", observed);
            }
            outcome(
                operation,
                id,
                CheckState::Ready,
                CheckCode::ClusterIdentityMatches,
                now,
            )
            .with_message("the broker's cluster id matches the one on record")
            .with_fact("clusterId", observed)
        }
    }
}

/// The restore plan, reduced to the facts the three `plan.*` rows read.
///
/// No `PartialEq`: `logweir_core::spec::DrillSpec` deliberately defines none.
/// What identifies a plan is [`PlanFacts::recomputed_hash`], which
/// `binding.planHash` carries.
#[derive(Clone, Debug)]
pub struct PlanFacts {
    /// The verbatim bytes. Never re-serialised: `planHash` binds these.
    pub bytes: Vec<u8>,
    /// `sha256:<hex>` recomputed by the controller from `bytes`.
    pub recomputed_hash: String,
    /// The hash the requester claimed, when it claimed one.
    pub claimed_hash: Option<String>,
    /// The parsed plan, or the parse error's own text.
    pub parsed: Result<Box<logweir_core::spec::DrillSpec>, String>,
}

impl PlanFacts {
    /// Reduce the bytes a requester submitted.
    #[must_use]
    pub fn of(bytes: &[u8], claimed_hash: Option<&str>) -> Self {
        Self {
            bytes: bytes.to_vec(),
            recomputed_hash: logweir_core::ids::sha256_prefixed(bytes),
            claimed_hash: claimed_hash.map(str::to_string),
            parsed: serde_yaml::from_slice::<logweir_core::spec::DrillSpec>(bytes)
                .map(Box::new)
                .map_err(|e| e.to_string()),
        }
    }

    /// Whether the target mode is `scratch`, the mode the allowlist and the
    /// marker row apply to.
    #[must_use]
    pub fn scratch(&self) -> bool {
        self.parsed
            .as_ref()
            .is_ok_and(|s| matches!(s.target.mode, logweir_core::spec::TargetMode::Scratch))
    }
}

/// A digest, shortened so the redaction chokepoint does not eat it.
///
/// `check_contract::redact` replaces any hex run of 40 characters or more, so
/// a whole `sha256:<64 hex>` in a message reaches the status as `[redacted]`.
/// The message carries the first twelve hex characters, enough to tell two
/// plans apart; the whole digest is in `status.binding.planHash`, a field
/// nothing redacts.
#[must_use]
pub fn short_digest(digest: &str) -> String {
    match digest.split_once(':') {
        Some((algo, hex)) if hex.len() > 12 => format!("{algo}:{}…", &hex[..12]),
        _ => digest.to_string(),
    }
}

/// `plan.parse`, the controller's half.
///
/// The hash is recomputed here and the claim is never trusted:
/// `spec.request.restore.planHash` is the requester's assertion about bytes it
/// also supplied, and the API's applicability test compares exactly that
/// field.
#[must_use]
pub fn plan_parse_row(facts: &PlanFacts, now: DateTime<Utc>) -> CheckOutcome {
    let op = PreflightOperation::Restore;
    if let Some(claimed) = facts.claimed_hash.as_deref() {
        if claimed != facts.recomputed_hash {
            return outcome(
                op,
                CheckId::PlanParse,
                CheckState::NotReady,
                CheckCode::PlanHashMismatch,
                now,
            )
            .with_message(&format!(
                "the submitted planHash is {} and these bytes hash to {}",
                short_digest(claimed),
                short_digest(&facts.recomputed_hash)
            ))
            .with_remedy(
                "Re-submit the preflight with the hash of the exact bytes an approver would \
                 sign. The controller recomputes it and never trusts the claim.",
            );
        }
    }
    match facts.parsed.as_ref() {
        Err(e) => outcome(
            op,
            CheckId::PlanParse,
            CheckState::NotReady,
            CheckCode::PlanUnparseable,
            now,
        )
        .with_message(e)
        .with_remedy("Fix the restore plan document and re-submit."),
        Ok(_) => outcome(
            op,
            CheckId::PlanParse,
            CheckState::Ready,
            CheckCode::PlanParsed,
            now,
        )
        .with_message(&format!(
            "the plan parses and hashes to {}",
            short_digest(&facts.recomputed_hash)
        )),
    }
}

/// Kafka's own topic-name grammar, as the broker enforces it.
const TOPIC_NAME_MAX_CHARS: usize = 249;

fn topic_name_is_legal(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && name.chars().count() <= TOPIC_NAME_MAX_CHARS
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
}

/// A scope naming the TOPIC a row is about.
///
/// `redact` replaces any run of 40+ characters from `[A-Za-z0-9+/=_-]`, and a
/// topic name's only run-breaking character is `.`, so the too-long name a
/// `MappedTopicNameIllegal` row is about is the one `with_message` eats.
/// `entry_of` copies `scope` verbatim, and a topic name is not
/// credential-shaped, so the name goes there.
#[must_use]
pub fn topic_scope(name: &str) -> CheckScope {
    CheckScope {
        kind: "Topic".to_string(),
        name: name.to_string(),
        uid: None,
    }
}

/// `plan.names`: the four `notReady` codes, over the mapped names. Phase 0's
/// rules, applied before the pod exists; phase 0 still runs.
#[must_use]
pub fn plan_names_row(facts: &PlanFacts, now: DateTime<Utc>) -> CheckOutcome {
    let op = PreflightOperation::Restore;
    let mk = |state: CheckState, code: CheckCode| outcome(op, CheckId::PlanNames, state, code, now);
    let Ok(spec) = facts.parsed.as_ref() else {
        return mk(CheckState::NotReady, CheckCode::PlanUnparseable)
            .with_message("the plan does not parse, so its mapped names cannot be checked");
    };
    let topics = &spec.source.topics;
    if let Err(entry) = logweir_core::guard::reject_glob_metacharacters(topics) {
        return mk(CheckState::NotReady, CheckCode::GlobInTopic)
            .with_scope(topic_scope(&entry))
            .with_message(&format!(
                "the plan selects `{entry}`, which carries a glob metacharacter; Logweir never \
                 expands patterns into a run"
            ))
            .with_remedy("Name each topic explicitly (guard G-GLOB).");
    }
    let prefix = logweir_core::spec::target_topic_prefix(spec);
    // `$` is refused in both halves: the engine's config loader expands
    // `${NAME}` in pre-parse text, so a name that can name an environment
    // variable can read one.
    if let Some(entry) = topics
        .iter()
        .find(|t| t.contains('$'))
        .or_else(|| prefix.contains('$').then_some(&prefix))
    {
        return mk(CheckState::NotReady, CheckCode::ExpansionInTopic)
            .with_scope(topic_scope(entry))
            .with_message(&format!(
                "`{entry}` carries a `$`, which the engine's configuration loader expands \
                 before the document is parsed"
            ))
            .with_remedy("Remove the `$` from the topic name or the mapping prefix.");
    }
    // A plan that restores under the original topic names maps every topic
    // onto itself on purpose, behind its own approval subject, so the identity
    // map is refused only when the plan did not opt in, or opted in in a shape
    // phase 0 refuses (`refuse_shape`, the runner's own words).
    if let Some(why) = logweir_core::original_name::refuse_shape(spec) {
        return mk(CheckState::NotReady, CheckCode::TopicMappingIdentity)
            .with_message(&why)
            .with_remedy(
                "A restore under the original topic names is `newTopic` with `prefix: \"\"`; \
                 every other restore sets a target topic prefix nothing has used.",
            );
    }
    if prefix.is_empty() && !logweir_core::original_name::is_original_name_restore(spec) {
        return mk(CheckState::NotReady, CheckCode::TopicMappingIdentity)
            .with_message(
                "the plan's target prefix is empty, so every source topic maps onto itself and \
                 the restore would write over the topics the archive was taken from",
            )
            .with_remedy("Set a target topic prefix nothing has used.");
    }
    if let Some(bad) = topics
        .iter()
        .map(|t| format!("{prefix}{t}"))
        .find(|n| !topic_name_is_legal(n))
    {
        return mk(CheckState::NotReady, CheckCode::MappedTopicNameIllegal)
            .with_scope(topic_scope(&bad))
            .with_message(&format!(
                "the mapped name `{bad}` is not a legal Kafka topic name ([a-zA-Z0-9._-], at \
                 most {TOPIC_NAME_MAX_CHARS} characters, not `.` or `..`)"
            ))
            .with_remedy("Shorten the prefix, or rename the source topic.");
    }
    mk(CheckState::Ready, CheckCode::MappedNamesLegal).with_message(&format!(
        "all {} mapped names are legal under prefix `{prefix}`",
        topics.len()
    ))
}

/// What the referents say, reduced to what `plan.bindings` compares.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BindingFacts {
    /// The target `KafkaCluster`'s bootstrap addresses.
    pub target_bootstrap: Vec<String>,
    /// The source destination's archive `StorageUrl`, when one is named.
    pub source_storage: Option<logweir_core::engine::StorageUrl>,
    /// The evidence destination's evidence `StorageUrl`, when one is named.
    pub evidence_storage: Option<logweir_core::engine::StorageUrl>,
    /// The topics the recovery point covers, when it is known.
    pub recovery_point_topics: Option<Vec<String>>,
    /// A legacy inline archive the plan must read — `None` for a
    /// destination-backed check, whose location `source_storage` pins.
    pub legacy_source: Option<LegacyBinding>,
}

/// `plan.bindings`.
///
/// Four codes, because every mismatch is a different remedy: the wrong bucket
/// is a plan edit, the wrong broker is a target edit, and a topic the recovery
/// point never captured means choosing a different recovery point.
#[must_use]
pub fn plan_bindings_row(
    facts: &PlanFacts,
    bindings: &BindingFacts,
    now: DateTime<Utc>,
) -> CheckOutcome {
    let op = PreflightOperation::Restore;
    let mk =
        |state: CheckState, code: CheckCode| outcome(op, CheckId::PlanBindings, state, code, now);
    let Ok(spec) = facts.parsed.as_ref() else {
        return mk(CheckState::NotReady, CheckCode::PlanUnparseable)
            .with_message("the plan does not parse, so its references cannot be compared");
    };
    if !bindings.target_bootstrap.is_empty()
        && spec.target.bootstrap_servers != bindings.target_bootstrap
    {
        return mk(CheckState::NotReady, CheckCode::PlanTargetMismatch)
            .with_message(
                "the plan's target bootstrap addresses are not the ones the target KafkaCluster \
                 declares",
            )
            .with_remedy(
                "Re-render the plan from the target this preflight names, or point the \
                 preflight at the KafkaCluster the plan was built for.",
            );
    }
    if let Some(expected) = bindings.source_storage.as_ref() {
        if &spec.source.storage != expected {
            return mk(CheckState::NotReady, CheckCode::PlanDestinationMismatch)
                .with_message(
                    "the plan reads the archive from a location that is not the source \
                     BackupDestination's",
                )
                .with_remedy(
                    "Re-render the plan from the saved destination, or name the destination \
                     the plan already points at.",
                );
        }
    }
    if let Some(legacy) = bindings.legacy_source.as_ref() {
        if let Some(row) = legacy_binding_mismatch(spec, legacy, now) {
            return row;
        }
    }
    if let Some(expected) = bindings.evidence_storage.as_ref() {
        if &spec.evidence != expected {
            return mk(
                CheckState::NotReady,
                CheckCode::PlanEvidenceDestinationMismatch,
            )
            .with_message(
                "the plan writes evidence to a location that is not the evidence \
                 BackupDestination's",
            )
            .with_remedy("Re-render the plan from the saved evidence destination.");
        }
    }
    if let Some(covered) = bindings.recovery_point_topics.as_ref() {
        let missing: Vec<&String> = spec
            .source
            .topics
            .iter()
            .filter(|t| !covered.contains(t))
            .collect();
        if let Some(first) = missing.first() {
            return mk(
                CheckState::NotReady,
                CheckCode::PlanTopicsNotInRecoveryPoint,
            )
            .with_scope(topic_scope(first))
            .with_message(&format!(
                "{} selected topic(s) are not in the recovery point, the first being `{first}`",
                missing.len()
            ))
            .with_remedy(
                "Choose a recovery point that covers these topics, or drop them from the \
                     plan.",
            )
            .with_detail(json!({
                "count": missing.len(),
                "sample": missing.iter().take(10).map(|t| t.as_str()).collect::<Vec<_>>(),
            }));
        }
    }
    mk(CheckState::Ready, CheckCode::PlanMatchesReferences).with_message(
        "the plan names the target, the destinations and the recovery point this check resolved",
    )
}

/// `plan.bindings`'s legacy half: the plan reads the inline archive the
/// request names, and that is the archive the recovery point was written to.
///
/// Both are compared on scheme, bucket and prefix, everything a legacy
/// `archive.url` carries; [`legacy_source_destination`] reads the other four
/// storage fields from the plan.
fn legacy_binding_mismatch(
    spec: &logweir_core::spec::DrillSpec,
    legacy: &LegacyBinding,
    now: DateTime<Utc>,
) -> Option<CheckOutcome> {
    let mk = |message: String| {
        outcome(
            PreflightOperation::Restore,
            CheckId::PlanBindings,
            CheckState::NotReady,
            CheckCode::PlanDestinationMismatch,
            now,
        )
        .with_scope(CheckScope {
            kind: LEGACY_ARCHIVE_SCOPE_KIND.to_string(),
            name: legacy.url.clone(),
            uid: None,
        })
        .with_message(&message)
        .with_remedy(
            "Re-render the plan from the recovery point's own archive, or name the archive the \
             plan already reads.",
        )
    };
    let named = url_location(&legacy.url);
    let reads = match &spec.source.storage {
        logweir_core::engine::StorageUrl::S3 { bucket, prefix, .. } => Some((
            "s3".to_string(),
            bucket.clone(),
            prefix.trim_matches('/').to_string(),
        )),
        _ => None,
    };
    if reads.as_ref() != Some(&named) {
        return Some(mk(format!(
            "the plan reads the archive from {} and the inline archive this check names is `{}`",
            describe_plan_source(&spec.source.storage),
            legacy.url
        )));
    }
    if let Some(point_url) = legacy.point_url.as_deref() {
        if url_location(point_url) != named {
            return Some(mk(format!(
                "the recovery point `{}` was written to `{point_url}` and this check names `{}`",
                legacy.point.as_deref().unwrap_or_default(),
                legacy.url
            )));
        }
    }
    None
}

/// A plan's `source.storage`, as the bucket and prefix an operator compares.
fn describe_plan_source(storage: &logweir_core::engine::StorageUrl) -> String {
    match storage {
        logweir_core::engine::StorageUrl::S3 { bucket, prefix, .. } => {
            if prefix.trim_matches('/').is_empty() {
                format!("`s3://{bucket}`")
            } else {
                format!("`s3://{bucket}/{}`", prefix.trim_matches('/'))
            }
        }
        logweir_core::engine::StorageUrl::Gcs { .. } => "a GCS location".to_string(),
        logweir_core::engine::StorageUrl::Azure { .. } => "an Azure location".to_string(),
        logweir_core::engine::StorageUrl::Filesystem { .. } => "a filesystem path".to_string(),
    }
}

/// The recovery point, reduced; its identity is a `Backup` UID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveryPointFacts {
    /// No `recoveryPointRef` was given. The row is not reported.
    NotRequested,
    /// The named `Backup` does not exist.
    NotFound { name: String },
    /// It exists.
    Found {
        /// Its name.
        name: String,
        /// Its UID, the recovery point's identity.
        uid: String,
        /// The UID the requester pinned, when it pinned one.
        expected_uid: Option<String>,
        /// Its phase.
        phase: Option<String>,
        /// Its frozen destination `locationDigest`, when it has one.
        location_digest: Option<String>,
        /// The source destination's `locationDigest` this check resolved.
        expected_location_digest: Option<String>,
    },
}

/// `recoveryPoint.state`, bound to a `Backup` UID.
///
/// A `Backup` deleted and recreated under the same name is a different run
/// over a different window, so `RecoveryPointUidChanged` exists, and the UID in
/// the binding digest makes every earlier preflight stale.
///
/// A recovery point is only restorable from the destination it was written
/// to. Its frozen `locationDigest` (`Backup.status.destination`) is compared
/// with the digest this check resolved for the source destination:
///
/// * both present and equal → `RecoveryPointSucceeded`;
/// * both present and different → `RecoveryPointLocationMismatch`, with both
///   digests in the message;
/// * the point has none and the check resolved one → `unknown` with
///   `RecoveryPointLocationUnknown`: a point archived before saved destinations
///   publishes no location, and `ready` would report a comparison nobody made.
///
/// Neither digest is recomputed from a live object: a destination edited after
/// a run froze must not make a moved recovery point look settled.
#[must_use]
pub fn recovery_point_row(facts: &RecoveryPointFacts, now: DateTime<Utc>) -> Option<CheckOutcome> {
    let op = PreflightOperation::Restore;
    let mk = |state: CheckState, code: CheckCode| {
        outcome(op, CheckId::RecoveryPointState, state, code, now)
    };
    match facts {
        RecoveryPointFacts::NotRequested => None,
        RecoveryPointFacts::NotFound { name } => Some(
            mk(CheckState::NotReady, CheckCode::RecoveryPointNotFound)
                .with_scope(CheckScope {
                    kind: "Backup".to_string(),
                    name: name.clone(),
                    uid: None,
                })
                .with_message(&format!(
                    "no Backup named `{name}` exists in this namespace"
                ))
                .with_remedy("Choose a recovery point that exists."),
        ),
        RecoveryPointFacts::Found {
            name,
            uid,
            expected_uid,
            phase,
            location_digest,
            expected_location_digest,
        } => {
            let scope = CheckScope {
                kind: "Backup".to_string(),
                name: name.clone(),
                uid: Some(uid.clone()),
            };
            if let Some(expected) = expected_uid.as_deref().filter(|u| !u.is_empty()) {
                if expected != uid {
                    return Some(
                        mk(CheckState::NotReady, CheckCode::RecoveryPointUidChanged)
                            .with_scope(scope)
                            .with_message(&format!(
                                "the Backup named `{name}` now has UID {uid} and this request \
                                 pinned {expected}; it was deleted and recreated"
                            ))
                            .with_remedy(
                                "Re-pick the recovery point. A same-named replacement is a \
                                 different run over a different window.",
                            ),
                    );
                }
            }
            if phase.as_deref() != Some(crate::conditions::PHASE_SUCCEEDED) {
                return Some(
                    mk(CheckState::NotReady, CheckCode::RecoveryPointNotSucceeded)
                        .with_scope(scope)
                        .with_message(&format!(
                            "the recovery point's phase is {}",
                            phase.as_deref().unwrap_or("<unset>")
                        ))
                        .with_remedy("Restore from a Backup that Succeeded."),
                );
            }
            match (
                location_digest.as_deref(),
                expected_location_digest.as_deref(),
            ) {
                // Two different locations. Both digests are in the message, so
                // an operator with two destinations learns which one holds the
                // point.
                (Some(frozen), Some(expected)) if frozen != expected => {
                    return Some(
                        mk(
                            CheckState::NotReady,
                            CheckCode::RecoveryPointLocationMismatch,
                        )
                        .with_scope(scope)
                        .with_message(&format!(
                            "the recovery point was written to {frozen} and the source \
                             destination this check resolved is {expected}"
                        ))
                        .with_remedy(
                            "Read this recovery point from the destination it was written \
                             to.",
                        ),
                    );
                }
                // A legacy point under a destination-backed check: the Backup
                // publishes no frozen destination, so the row cannot say the
                // location agrees, and as a blocking `unknown` it holds the
                // verdict until someone confirms the location by hand. The
                // mirror case (a point with a digest under a check that
                // resolved no source destination) stays `ready`: that check
                // makes no location claim, and `plan.bindings` holds the
                // legacy plan's location to account.
                (None, Some(expected)) => {
                    return Some(
                        mk(CheckState::Unknown, CheckCode::RecoveryPointLocationUnknown)
                            .with_scope(scope)
                            .with_message(&format!(
                                "this recovery point records no frozen destination, so it cannot \
                             be compared with the source destination this check resolved \
                             ({expected}); it was archived by an inline-archive run or by a \
                             controller that predates status.destination"
                            ))
                            .with_remedy(
                                "Confirm by hand that this Backup was written to this \
                             destination, or restore from a recovery point archived through \
                             it.",
                            ),
                    );
                }
                _ => {}
            }
            Some(
                mk(CheckState::Ready, CheckCode::RecoveryPointSucceeded)
                    .with_scope(scope)
                    .with_message("the recovery point succeeded and is the one this check pinned"),
            )
        }
    }
}

/// What a `Switchover` review read (PROD-04.2 §3.8): the `Restore` it names,
/// the namespace's evidence keys, and the groups it asks about.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SwitchoverFacts {
    /// `restoreRef.name`.
    pub requested: String,
    /// `restoreRef.uid`, when the request named one.
    pub requested_uid: Option<String>,
    /// The `Restore` by that name, when one exists.
    pub restore: Option<SwitchoverRestoreFacts>,
    /// The evidence-signing keyring of the namespace's trust, or why there is
    /// none.
    pub keyring: Result<logweir_core::execution_contract::EvidenceKeyring, String>,
    /// The groups to read (empty: every group the mapping maps).
    pub groups: Vec<logweir_core::check_contract::SwitchoverReadinessGroup>,
}

/// The `Restore` a review names, in the facts the review is admitted on —
/// the `Switchover` controller's own admission (`switchover::admit`, step 2).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SwitchoverRestoreFacts {
    pub name: String,
    pub uid: String,
    /// `sha256:` of its plan bytes.
    pub plan_hash: String,
    pub phase: Option<String>,
    /// `status.evidence.verification.result`.
    pub verdict: Option<String>,
    pub scorecard_key: Option<String>,
    pub scratch: bool,
    /// Whether it names a saved evidence destination (an inline-archive
    /// restore does not).
    pub evidence_destination: bool,
}

impl SwitchoverFacts {
    /// The facts of one review, from the request, the object and the trust.
    #[must_use]
    pub fn of(
        request: &crate::crds::preflight::SwitchoverPreflightRequest,
        restore: Option<&Restore>,
        trust: Result<&crate::trust::Resolution, &kube::Error>,
    ) -> Self {
        let keyring = match trust {
            Ok(crate::trust::Resolution::Trust(t)) => super::restore::evidence_keyring_bytes(t)
                .map_err(|e| e.to_string())
                .and_then(|json| serde_json::from_str(&json).map_err(|e| e.to_string())),
            Ok(_) => Err(
                "the namespace resolves to no single trust (no TrustPolicy and no \
                          roster, or two policies claim it)"
                    .to_string(),
            ),
            Err(e) => Err(format!("the namespace's trust could not be read: {e}")),
        };
        Self {
            requested: request.restore_ref.name.clone(),
            requested_uid: request.restore_ref.uid.clone(),
            restore: restore.map(|r| {
                let st = r.status.as_ref();
                let evidence = st.and_then(|s| s.evidence.as_ref());
                SwitchoverRestoreFacts {
                    name: r.name_any(),
                    uid: r.uid().unwrap_or_default(),
                    plan_hash: logweir_core::ids::sha256_prefixed(r.spec.plan_bytes.as_bytes()),
                    phase: st.and_then(|s| s.phase.clone()),
                    verdict: evidence
                        .and_then(|e| e.verification.as_ref())
                        .and_then(|v| v.result.clone()),
                    scorecard_key: evidence.and_then(|e| e.scorecard_key.clone()),
                    scratch: r.spec.target.mode == crate::crds::restore::TargetMode::Scratch,
                    evidence_destination: r.spec.evidence_destination_ref.is_some(),
                }
            }),
            keyring,
            groups: request
                .groups
                .iter()
                .flatten()
                .map(|g| logweir_core::check_contract::SwitchoverReadinessGroup {
                    source: g.source.clone(),
                    target: g.target.clone(),
                })
                .collect(),
        }
    }
}

/// `switchover.restore` (PROD-04.2 §3.8): the `Restore` a review names
/// exists with the requested UID, is not a scratch drill, `Succeeded` with a
/// `Valid` evidence verdict and a recorded scorecard key, writes its evidence
/// to a saved destination, and the namespace has evidence keys to verify it
/// under — the `Switchover` controller's admission, read-only. Anything else
/// is `notReady` and no Job is rendered.
#[must_use]
pub fn switchover_restore_row(facts: Option<&SwitchoverFacts>, now: DateTime<Utc>) -> CheckOutcome {
    let op = PreflightOperation::Switchover;
    let not_ready = |code: CheckCode, message: String, remedy: &str| {
        outcome(
            op,
            CheckId::SwitchoverRestore,
            CheckState::NotReady,
            code,
            now,
        )
        .with_message(&message)
        .with_remedy(remedy)
    };
    let Some(f) = facts else {
        return not_ready(
            CheckCode::RestoreNotFound,
            "the review names no Restore".to_string(),
            "Name the verified Restore whose consumer positions a switchover would apply.",
        );
    };
    let scope = CheckScope {
        kind: "Restore".to_string(),
        name: f.requested.clone(),
        uid: f.restore.as_ref().map(|r| r.uid.clone()),
    };
    let Some(r) = f.restore.as_ref() else {
        return not_ready(
            CheckCode::RestoreNotFound,
            format!(
                "no Restore named `{}` exists in this namespace",
                f.requested
            ),
            "Name a Restore of this namespace; a switchover applies only a restore that \
             succeeded here.",
        )
        .with_scope(scope);
    };
    if f.requested_uid.as_deref().is_some_and(|u| u != r.uid) {
        return not_ready(
            CheckCode::RestoreNotFound,
            format!(
                "Restore `{}` is UID {}, not the UID {} this review names: it was replaced",
                r.name,
                r.uid,
                f.requested_uid.as_deref().unwrap_or_default()
            ),
            "Review the current Restore by its current UID.",
        )
        .with_scope(scope);
    }
    if r.scratch {
        return not_ready(
            CheckCode::RestoreNotSucceeded,
            format!(
                "{}: Restore `{}` is a scratch drill; its topics are torn down, so no group is \
                 moved onto them",
                logweir_core::switchover::RESTORE_IS_SCRATCH,
                r.name
            ),
            "Switch groups over onto a restore into new topics, not a scratch drill.",
        )
        .with_scope(scope);
    }
    let phase = r.phase.as_deref().unwrap_or("not started");
    if phase != "Succeeded" || r.verdict.as_deref() != Some("Valid") {
        return not_ready(
            CheckCode::RestoreNotSucceeded,
            format!(
                "{}: Restore `{}` is {phase} and its evidence verdict is {}; a switchover \
                 applies only a succeeded restore whose signed scorecard verified",
                logweir_core::switchover::RESTORE_NOT_SUCCEEDED,
                r.name,
                r.verdict.as_deref().unwrap_or("not recorded")
            ),
            "Wait for the restore to finish and its evidence to verify, or restore again.",
        )
        .with_scope(scope);
    }
    if r.scorecard_key.is_none() {
        return not_ready(
            CheckCode::RestoreNotSucceeded,
            format!("Restore `{}` records no scorecard key", r.name),
            "Wait for the restore's evidence to be recorded.",
        )
        .with_scope(scope);
    }
    if !r.evidence_destination {
        return not_ready(
            CheckCode::ArchiveUrlUnreadable,
            format!(
                "Restore `{}` writes its evidence through an inline archive; this build reviews \
                 restores whose evidence is a saved BackupDestination, and the Switchover \
                 itself still checks this one when it runs",
                r.name
            ),
            "Create the Switchover directly (it repeats every check), or restore with \
             evidenceDestinationRef.",
        )
        .with_scope(scope);
    }
    if let Err(why) = &f.keyring {
        return not_ready(
            CheckCode::TrustRosterNotLoaded,
            format!("no evidence key to verify the restore's scorecard under: {why}"),
            "Give the namespace one TrustPolicy (or the default roster) that lists its \
             evidence-signing key.",
        )
        .with_scope(scope);
    }
    outcome(
        op,
        CheckId::SwitchoverRestore,
        CheckState::Ready,
        CheckCode::RestoreSucceeded,
        now,
    )
    .with_message(&format!(
        "Restore `{}` succeeded and its evidence verdict is Valid; the Job verifies its signed \
         scorecard and positions mapping",
        r.name
    ))
    .with_scope(scope)
}

/// How many `Backup`s one list page reads for the catalog point's verdict join.
pub const CATALOG_POINT_BACKUP_PAGE: u32 = 500;
/// How many such pages one check reads. A namespace holding more `Backup`s than
/// `4 × 500` cannot be shown to hold no refusal of this receipt, and the row
/// says so (`unknown`) rather than guessing.
pub const CATALOG_POINT_BACKUP_PAGES: usize = 4;
/// How many view pages one check reads: the view's own ceiling.
pub const CATALOG_POINT_VIEW_PAGES: usize = 8;

/// The catalog point a restore check is about, reduced.
///
/// The console read the row when the operator chose the point; this re-reads
/// it so a green preview cannot survive a point that became `Missing`. The row
/// is the controller's own materialised judgement (availability ×
/// verification, trust applied), read from the immutable page `ConfigMap`s the
/// catalog's status names and checked against their recorded digests, and
/// joined with the namespace's `Backup` verdicts on the receipt digest: the
/// catalog decides only where the controller could not look, so a `Backup` of
/// the same receipt whose own verdict is a reached refusal refuses the point
/// whatever the row says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogPointFacts {
    /// No `catalogPointRef` was given. The row is not reported by this path.
    NotRequested,
    /// The catalog does not exist in this namespace.
    CatalogNotFound {
        /// The catalog asked for.
        catalog: String,
    },
    /// The view could not be read in full, so no answer is possible.
    ViewUnavailable {
        /// The catalog.
        catalog: String,
        /// Why, in words.
        detail: String,
    },
    /// The view was read in full and does not list the point.
    NotInView {
        /// The catalog.
        catalog: String,
        /// The point asked for.
        point_id: String,
    },
    /// The view lists it.
    Found(Box<CatalogPointFound>),
}

/// What the view says about a point it lists, and what the check compares.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogPointFound {
    /// The catalog.
    pub catalog: String,
    /// Its UID.
    pub catalog_uid: String,
    /// The row, as the controller materialised it.
    pub entry: crate::catalog_view::ViewEntry,
    /// A reached verdict on a `Backup` of this receipt, when one exists:
    /// `catalog_view::ControllerRefusals`' answer, the join the product API's
    /// `/points` route and retention make.
    pub refused_by: Option<String>,
    /// Whether every `Backup` in the namespace was read and every refusal
    /// could be tied to a point. `false` makes the row `unknown`: an unread or
    /// unattributed refusal could be this receipt's.
    pub backups_complete: bool,
    /// The plan's `source.point`, when the plan parsed and carries one.
    pub plan_point: Option<logweir_core::execution_contract::PointBinding>,
    /// The plan's `source.backup`, when the plan parsed.
    pub plan_backup: Option<String>,
    /// Whether the check has a parsed plan at all.
    pub plan_parsed: bool,
    /// The row's signer, judged again against the namespace's current trust.
    pub signer: SignerRecheck,
}

/// The row's signer against the namespace's current trust.
///
/// The row's `verification` is the catalog's sync-time verdict; a key revoked
/// or retired since would otherwise keep a `Verified` row green until the next
/// sync. This is the `logweir_core::trust::decide` the runner applies before
/// it restores, over the trust the controller resolves for the namespace's
/// `Approval`s (`crate::trust::resolve`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SignerRecheck {
    /// Current trust accepts the row's signer for `EvidenceSigning`.
    Trusted,
    /// Current trust refuses it.
    Untrusted {
        /// The signer key id the row names.
        key_id: String,
        /// `UntrustReason::as_str`, or why the key material is unusable.
        reason: String,
        /// The key's lifecycle state as the verdict saw it.
        key_state: String,
    },
    /// No answer: the trust could not be resolved, or the row names no key.
    Unjudgeable {
        /// Why, in words.
        detail: String,
    },
}

/// Judge `entry`'s signer against `trust`. Pure.
///
/// The claimed signing time is the row's recovery point: the view carries no
/// receipt `finished_at`, and the recovery point is the earliest instant the
/// receipt can have been signed. A key retired between the two is accepted
/// here and refused by the runner, which reads the real `finished_at` before
/// moving any data.
#[must_use]
pub fn signer_recheck(
    entry: &crate::catalog_view::ViewEntry,
    trust: Result<&crate::trust::ResolvedTrust, &str>,
    now: DateTime<Utc>,
) -> SignerRecheck {
    use logweir_core::trust::{EvidenceClaim, IndependentObservation, KeyUsage, TrustResult};
    let trust = match trust {
        Ok(trust) => trust,
        Err(detail) => {
            return SignerRecheck::Unjudgeable {
                detail: detail.to_string(),
            }
        }
    };
    let Some(key_id) = entry
        .signer_key_id
        .as_deref()
        .filter(|k| !k.trim().is_empty())
    else {
        return SignerRecheck::Unjudgeable {
            detail: format!(
                "the catalog lists `{}` with no signer key id, so its signer cannot be judged \
                 against the current trust",
                entry.point_id
            ),
        };
    };
    let Some(signed_at) = DateTime::<Utc>::from_timestamp_millis(entry.recovery_point_at_ms) else {
        return SignerRecheck::Unjudgeable {
            detail: format!(
                "the catalog row for `{}` carries no usable recovery point instant",
                entry.point_id
            ),
        };
    };
    let verdict = trust.decide_for(
        key_id,
        KeyUsage::EvidenceSigning,
        &EvidenceClaim::at(signed_at),
        &IndependentObservation::none(),
        now,
    );
    // A pass is `Valid` on a basis the badge admits, not `Valid` alone.
    // `decide` pairs `Valid` with `Current`/`Historical` only today; the
    // allow-list keeps it so.
    let pass = verdict.result == TrustResult::Valid
        && crate::verification::ValidBasis::of_core(verdict.basis).is_pass();
    if !pass {
        return SignerRecheck::Untrusted {
            key_id: key_id.to_string(),
            reason: verdict
                .reason
                .map_or("NotAccepted", logweir_core::trust::UntrustReason::as_str)
                .to_string(),
            key_state: verdict.key_state.as_str().to_string(),
        };
    }
    // The runner is handed only keys whose public half parses and whose id is
    // its own (`restore::evidence_keyring_bytes`); a key it would not be
    // handed cannot verify the receipt there.
    if !trust
        .key(key_id)
        .is_some_and(crate::trust::ResolvedKey::is_usable)
    {
        return SignerRecheck::Untrusted {
            key_id: key_id.to_string(),
            reason: "KeyMaterialUnusable".to_string(),
            key_state: verdict.key_state.as_str().to_string(),
        };
    }
    SignerRecheck::Trusted
}

/// Everything [`catalog_point_facts`] reads, gathered by the reconciler.
pub struct CatalogPointRead<'a> {
    /// The catalog asked for.
    pub catalog_name: &'a str,
    /// The point asked for.
    pub point_id: &'a str,
    /// The catalog, when it exists.
    pub catalog: Option<&'a crate::crds::recovery_catalog::RecoveryCatalog>,
    /// Each page the status names, in order, with the object read for it
    /// (`None` when it is gone).
    pub pages: &'a [(String, Option<k8s_openapi::api::core::v1::ConfigMap>)],
    /// The reached refusals among the namespace's `Backup`s
    /// (`catalog_view::ControllerRefusals`, read leniently).
    pub refusals: &'a crate::catalog_view::ControllerRefusals,
    /// Whether that list is the whole namespace.
    pub backups_complete: bool,
    /// The plan, when the check has one.
    pub plan: Option<&'a PlanFacts>,
    /// The namespace's current trust (`crate::trust::resolve`), or why it
    /// could not be resolved.
    pub trust: Result<&'a crate::trust::ResolvedTrust, &'a str>,
    /// The check's clock.
    pub now: DateTime<Utc>,
}

/// Reduce what the reconciler read to [`CatalogPointFacts`]. Pure.
///
/// A page is trusted only when it is `immutable: true` and its entry lines
/// hash to the digest the catalog's status recorded for it (the API's
/// `/points` rule, and the only integrity a Job-owned page has). A page that
/// is gone, mutable, or disagrees with its digest makes the view unreadable.
#[must_use]
pub fn catalog_point_facts(read: &CatalogPointRead<'_>) -> CatalogPointFacts {
    let catalog = read.catalog_name.to_string();
    let Some(object) = read.catalog else {
        return CatalogPointFacts::CatalogNotFound { catalog };
    };
    let unavailable = |detail: String| CatalogPointFacts::ViewUnavailable {
        catalog: catalog.clone(),
        detail,
    };
    let status = object.status.as_ref();
    match status.and_then(|s| s.view_expires_at) {
        None => return unavailable("the catalog has published no view yet".to_string()),
        Some(at) if at <= read.now => {
            return unavailable(format!(
                "the catalog's view expired at {}; sync the catalog again",
                at.to_rfc3339()
            ))
        }
        Some(_) => {}
    }
    let recorded: Vec<&crate::crds::recovery_catalog::CatalogPage> = status
        .and_then(|s| s.pages.as_ref())
        .map(|p| p.iter().collect())
        .unwrap_or_default();
    if recorded.len() > CATALOG_POINT_VIEW_PAGES {
        return unavailable(format!(
            "the view names {} pages and a check reads at most {CATALOG_POINT_VIEW_PAGES}",
            recorded.len()
        ));
    }
    let mut found: Option<crate::catalog_view::ViewEntry> = None;
    for page in &recorded {
        let map = read
            .pages
            .iter()
            .find(|(name, _)| name == &page.config_map_name)
            .and_then(|(_, map)| map.as_ref());
        let Some(map) = map else {
            return unavailable(format!(
                "view page {} is gone; the view aged out or is being replaced",
                page.config_map_name
            ));
        };
        if map.immutable != Some(true) {
            return unavailable(format!(
                "view page {} is not immutable, so its rows cannot be trusted",
                page.config_map_name
            ));
        }
        let body = map
            .data
            .as_ref()
            .and_then(|d| d.get(crate::catalog_view::PAGE_DATA_KEY))
            .map(String::as_str)
            .unwrap_or_default();
        let lines: Vec<&str> = body.lines().filter(|l| !l.trim().is_empty()).collect();
        let computed = crate::catalog_view::page_digest(&lines);
        let bare = |d: &str| d.strip_prefix("sha256:").unwrap_or(d).to_string();
        match page.sha256.as_deref() {
            Some(want) if bare(want) == bare(&computed) => {}
            _ => {
                return unavailable(format!(
                    "view page {} does not match the digest the catalog recorded for it",
                    page.config_map_name
                ))
            }
        }
        if found.is_none() {
            found = lines
                .iter()
                .filter_map(|l| serde_json::from_str::<crate::catalog_view::ViewEntry>(l).ok())
                .find(|e| e.point_id == read.point_id);
        }
    }
    let Some(entry) = found else {
        return CatalogPointFacts::NotInView {
            catalog,
            point_id: read.point_id.to_string(),
        };
    };
    let parsed = read.plan.and_then(|p| p.parsed.as_ref().ok());
    CatalogPointFacts::Found(Box::new(CatalogPointFound {
        catalog,
        catalog_uid: object.uid().unwrap_or_default(),
        refused_by: read.refusals.refusal_for(&entry).map(str::to_string),
        backups_complete: read.backups_complete
            && read.refusals.is_complete()
            && read.refusals.unattributed() == 0,
        plan_point: parsed.and_then(|p| p.source.point.clone()),
        plan_backup: parsed.map(|p| p.source.backup.clone()),
        plan_parsed: parsed.is_some(),
        signer: signer_recheck(&entry, read.trust, read.now),
        entry,
    }))
}

/// `recoveryPoint.state` for a catalog point.
///
/// In the order an operator repairs in:
///
/// 1. the catalog or its view is not there → `notReady`/`unknown`;
/// 2. the view does not list the point → `notReady RecoveryPointNotFound`;
/// 3. a `Backup` of this receipt carries a reached refusal →
///    `notReady CatalogPointRefusedByController`, whatever the row says;
/// 4. the row is not selectable → `notReady CatalogPointNotSelectable`, with
///    both axes in the message;
/// 5. the row's signer is refused by the namespace's current trust →
///    `notReady CatalogPointSignerUntrusted`; that trust could not be resolved
///    or the row names no signer → `unknown CatalogPointSignerUnknown`;
/// 6. not every `Backup` could be read → `unknown CatalogPointViewUnavailable`,
///    asked after the refusal it could not rule out, because a refusal that
///    was found is an answer;
/// 7. the plan is not bound to this row (no `source.point`, a different
///    binding, or a `source.backup` that is not the row's set) →
///    `notReady CatalogPointBindingMismatch`;
/// 8. otherwise `ready CatalogPointSelectable`.
#[must_use]
pub fn catalog_point_row(facts: &CatalogPointFacts, now: DateTime<Utc>) -> Option<CheckOutcome> {
    let op = PreflightOperation::Restore;
    let mk = |state: CheckState, code: CheckCode| {
        outcome(op, CheckId::RecoveryPointState, state, code, now)
    };
    let scope = |name: &str, uid: Option<String>| CheckScope {
        kind: "RecoveryCatalog".to_string(),
        name: name.to_string(),
        uid,
    };
    match facts {
        CatalogPointFacts::NotRequested => None,
        CatalogPointFacts::CatalogNotFound { catalog } => Some(
            mk(CheckState::NotReady, CheckCode::RecoveryPointNotFound)
                .with_scope(scope(catalog, None))
                .with_message(&format!(
                    "no RecoveryCatalog named `{catalog}` exists in this namespace"
                ))
                .with_remedy("Connect the archive (create the catalog), or choose a point from a catalog that exists."),
        ),
        CatalogPointFacts::ViewUnavailable { catalog, detail } => Some(
            mk(CheckState::Unknown, CheckCode::CatalogPointViewUnavailable)
                .with_scope(scope(catalog, None))
                .with_message(detail)
                .with_remedy("Request a catalog sync and run this check again once the view is published."),
        ),
        CatalogPointFacts::NotInView { catalog, point_id } => Some(
            mk(CheckState::NotReady, CheckCode::RecoveryPointNotFound)
                .with_scope(scope(catalog, None))
                .with_message(&format!(
                    "the catalog's current view does not list recovery point `{point_id}`"
                ))
                .with_remedy("Choose a point the catalog lists; a point outside the view is reachable with `logweir catalog list`."),
        ),
        CatalogPointFacts::Found(found) => {
            let f = found.as_ref();
            let e = &f.entry;
            let at = scope(&f.catalog, Some(f.catalog_uid.clone()));
            if let Some(verdict) = &f.refused_by {
                return Some(
                    mk(CheckState::NotReady, CheckCode::CatalogPointRefusedByController)
                        .with_scope(at)
                        .with_message(&format!(
                            "a Backup of this receipt carries the verdict `{verdict}`; the \
                             controller reached it, and a catalog row never outranks it"
                        ))
                        .with_remedy("Choose another recovery point; this receipt's own evidence was refused."),
                );
            }
            if !e.selectable {
                return Some(
                    mk(CheckState::NotReady, CheckCode::CatalogPointNotSelectable)
                        .with_scope(at)
                        .with_message(&format!(
                            "the catalog lists `{}` with availability {} and verification {}",
                            e.point_id,
                            e.availability.as_str(),
                            e.verification.as_str()
                        ))
                        .with_remedy(
                            e.remedy
                                .as_deref()
                                .unwrap_or("Choose a point the catalog marks restorable."),
                        ),
                );
            }
            match &f.signer {
                SignerRecheck::Trusted => {}
                SignerRecheck::Untrusted {
                    key_id,
                    reason,
                    key_state,
                } => {
                    return Some(
                        mk(CheckState::NotReady, CheckCode::CatalogPointSignerUntrusted)
                            .with_scope(at)
                            .with_message(&format!(
                                "the catalog lists `{}` as {}, but its receipt's signer `{key_id}` \
                                 is refused by this namespace's current trust: {reason} (key \
                                 state {key_state}); the row predates that change",
                                e.point_id,
                                e.verification.as_str()
                            ))
                            .with_remedy("Choose a point signed by a key the current trust accepts; the runner refuses this receipt before it moves any data."),
                    );
                }
                SignerRecheck::Unjudgeable { detail } => {
                    return Some(
                        mk(CheckState::Unknown, CheckCode::CatalogPointSignerUnknown)
                            .with_scope(at)
                            .with_message(detail)
                            .with_remedy("Resolve the namespace's TrustPolicy (exactly one policy, or the legacy TrustRoster), then run this check again."),
                    );
                }
            }
            if !f.backups_complete {
                return Some(
                    mk(CheckState::Unknown, CheckCode::CatalogPointViewUnavailable)
                        .with_scope(at)
                        .with_message(&format!(
                            "the Backup verdicts in this namespace could not all be read or tied \
                             to a point (a list failure, more than {} x {} Backups, or a refused \
                             verdict naming no receipt digest and no set id), so no reached \
                             refusal of this receipt can be ruled out",
                            CATALOG_POINT_BACKUP_PAGES, CATALOG_POINT_BACKUP_PAGE
                        ))
                        .with_remedy("Prune the namespace's Backup history, or verify the receipt with `logweir catalog list`."),
                );
            }
            let mismatch = |why: String| {
                mk(CheckState::NotReady, CheckCode::CatalogPointBindingMismatch)
                    .with_scope(scope(&f.catalog, Some(f.catalog_uid.clone())))
                    .with_message(&why)
                    .with_remedy("Re-render the plan from the catalog point; the runner refuses a plan whose binding is not the archive's.")
            };
            if !f.plan_parsed {
                return Some(mismatch(
                    "the plan did not parse, so it cannot be shown to be bound to this point"
                        .to_string(),
                ));
            }
            let Some(point) = f.plan_point.as_ref() else {
                return Some(mismatch(
                    "the plan carries no source.point, so the runner would restore without \
                     re-verifying this receipt"
                        .to_string(),
                ));
            };
            let manifest = e.manifest_sha256.as_deref().unwrap_or_default();
            let mut faults = Vec::new();
            if point.point_id != e.point_id {
                faults.push(format!("point_id {} is not {}", point.point_id, e.point_id));
            }
            if point.receipt_key != e.receipt_key {
                faults.push(format!(
                    "receipt_key {} is not {}",
                    point.receipt_key, e.receipt_key
                ));
            }
            if point.receipt_sha256 != e.receipt_sha256 {
                faults.push("receipt_sha256 is not the row's".to_string());
            }
            if manifest.is_empty() || point.manifest_sha256 != manifest {
                faults.push("manifest_sha256 is not the row's".to_string());
            }
            if f.plan_backup.as_deref() != Some(e.backup_id.as_str()) {
                faults.push(format!(
                    "source.backup {} is not the row's set {}",
                    f.plan_backup.as_deref().unwrap_or("(none)"),
                    e.backup_id
                ));
            }
            if !faults.is_empty() {
                return Some(mismatch(format!(
                    "the plan's binding is not this catalog row's: {}",
                    faults.join("; ")
                )));
            }
            Some(
                mk(CheckState::Ready, CheckCode::CatalogPointSelectable)
                    .with_scope(at)
                    .with_message(&format!(
                        "the catalog lists `{}` {} and {}, no Backup verdict refuses its \
                         receipt, and the plan is bound to it",
                        e.point_id,
                        e.availability.as_str(),
                        e.verification.as_str()
                    )),
            )
        }
    }
}

/// The `Approval`, reduced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApprovalFacts {
    /// A draft: `approval.state` is `skipped` with
    /// [`CheckCode::SubjectNotCreated`], because there is no subject to
    /// approve yet.
    Draft,
    /// The `Restore` names no approval at all.
    NotNamed,
    /// It is named and absent.
    NotFound { name: String },
    /// It exists.
    Found {
        /// Its name.
        name: String,
        /// Its UID, for the binding.
        uid: String,
        /// Its `metadata.resourceVersion`, for the binding. It moves when
        /// verification status moves, so a preflight taken while an approval
        /// was pending goes stale the moment it is verified.
        resource_version: String,
        /// `status.verified`.
        verified: Option<bool>,
        /// The `Verified` condition's reason, an `ApprovalRefusal::reason()`
        /// token such as `KeyIdExpired`: what this row routes on.
        reason: Option<String>,
        /// The `Verified` condition's MESSAGE, which is what an operator
        /// reads. Never routed on.
        message: Option<String>,
        /// The roster key id that verified it.
        matched_key_id: Option<String>,
        /// `status.approverKeyWindow`: the matched approver key's validity
        /// window, as the Approval controller published it from the trust that
        /// governs the namespace, the authority its verdict came from. The only
        /// window this file may read. `None` (no key matched, or an older
        /// controller) is unknown, never valid. Boxed to keep `Found` small
        /// (`clippy::large_enum_variant`).
        approver_key_window: Option<Box<ApproverKeyWindow>>,
        /// The `plan_hash` inside the approval's own signed bytes.
        approved_plan_hash: Option<String>,
        /// The `Restore` the approval names.
        subject_name: Option<String>,
        /// The Restore UID the verdict was recorded for
        /// (`status.verifiedSubjectRef.uid`) beside the UID of the Restore this
        /// check is about. A consumed `Approval` is never re-read by its
        /// controller, so it does not flip to `ReferentUidChanged` when its
        /// Restore is re-created under the same name; this row must see the
        /// UID itself. Boxed, likewise. `None` when either is unknown.
        uid_binding: Option<Box<UidBinding>>,
    },
}

/// The two UIDs [`ApprovalFacts::Found`] compares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UidBinding {
    /// `Approval.status.verifiedSubjectRef.uid`.
    pub verified_uid: String,
    /// The UID of the `Restore` this preflight is about.
    pub restore_uid: String,
}

/// The `Verified` condition reason that means the approver key had passed its
/// `notAfter` when the Approval controller last looked.
///
/// One spelling, the controller's:
/// `the_expiry_reason_is_the_one_the_approval_controller_writes` pins it
/// against `ApprovalRefusal::KeyIdExpired`'s own `reason()`.
pub const APPROVAL_REASON_KEY_ID_EXPIRED: &str = "KeyIdExpired";

/// `approval.state` and `approval.keyValidity`.
///
/// This row reads the Approval's verdict and never re-derives it. The
/// Approval controller resolves the approver key through the trust policy that
/// governs the namespace; a second derivation from `TrustRoster/default`
/// disagreed with it (a policy can expire a key the roster still shows open),
/// and a preflight must never authorise what the controller has refused. So
/// the verdict comes from the Approval: its `Verified` condition, that
/// condition's reason, and `status.matchedKeyId`. `approval_rows` is not given
/// a [`RosterFacts`] at all (`the_approval_rows_are_not_given_the_roster`).
///
/// The key's window comes from the Approval too
/// (`status.approverKeyWindow`, as the resolved trust policy declares it), for
/// `ApproverKeyExpiresBeforeDeadline` and the `min(10 m, notAfter)` cap.
///
/// `deadline` is the instant the restore would still need the approver key to
/// be valid at. `ApproverKeyExpiresBeforeDeadline` is advisory, a warning
/// ahead of time: a key that expires mid-run does not invalidate an approval
/// that verified while it was valid, and the blocking `approval.state` row is
/// where a withdrawn verdict refuses.
#[must_use]
pub fn approval_rows(
    facts: &ApprovalFacts,
    recomputed_plan_hash: &str,
    restore_name: Option<&str>,
    deadline: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Vec<CheckOutcome> {
    let op = PreflightOperation::Restore;
    let state_row =
        |state: CheckState, code: CheckCode| outcome(op, CheckId::ApprovalState, state, code, now);
    match facts {
        ApprovalFacts::Draft => vec![state_row(CheckState::Skipped, CheckCode::SubjectNotCreated)
            .with_message(
                "this preflight is about a draft plan, which no approver has been asked to sign \
                 yet; a skipped blocking check keeps the overall verdict `unknown`",
            )],
        ApprovalFacts::NotNamed => {
            vec![
                state_row(CheckState::NotReady, CheckCode::ApprovalNotVerified)
                    .with_message("the Restore names no Approval")
                    .with_remedy("Create the Approval this restore's plan hash was signed into."),
            ]
        }
        ApprovalFacts::NotFound { name } => {
            vec![
                state_row(CheckState::NotReady, CheckCode::ApprovalNotVerified)
                    .with_scope(CheckScope {
                        kind: "Approval".to_string(),
                        name: name.clone(),
                        uid: None,
                    })
                    .with_message(&format!(
                        "no Approval named `{name}` exists in this namespace"
                    ))
                    .with_remedy("Create the Approval, or wait for the approver to submit it."),
            ]
        }
        ApprovalFacts::Found {
            name,
            uid,
            verified,
            reason,
            message,
            matched_key_id,
            approver_key_window,
            approved_plan_hash,
            subject_name,
            uid_binding,
            ..
        } => {
            let scope = CheckScope {
                kind: "Approval".to_string(),
                name: name.clone(),
                uid: Some(uid.clone()),
            };
            // No key window is derived here: it is read back from the
            // `Approval` (see the function's note). The window must name the
            // key the verdict names; two fields can disagree (a half-landed
            // merge patch, one key's window beside another's verdict), and
            // disagreement is `None`, which is unknown, never valid.
            let window = matched_window(matched_key_id.as_deref(), approver_key_window.as_deref());
            let not_after = window.map(|w| w.not_after);
            let mut out = Vec::new();
            let foreign = uid_binding
                .as_deref()
                .filter(|b| b.verified_uid != b.restore_uid);
            let state = if let Some(binding) = foreign {
                // A verdict about another object of this name. The Restore
                // controller refuses it terminally (`ApprovalSubjectMismatch`),
                // so `ready` here would be the only green on the way there.
                state_row(CheckState::NotReady, CheckCode::ApprovalSubjectMismatch)
                    .with_scope(scope.clone())
                    .with_message(&format!(
                        "the Approval was verified for Restore UID {} and this Restore is UID {}: \
                         an object of the same name re-created is another subject, and an \
                         Approval never rebinds",
                        binding.verified_uid, binding.restore_uid
                    ))
                    .with_remedy("Create a new Approval for this Restore.")
            } else if let Some(subject) = subject_name.as_deref() {
                if restore_name.is_some_and(|r| r != subject) {
                    state_row(CheckState::NotReady, CheckCode::ApprovalSubjectMismatch)
                        .with_scope(scope.clone())
                        .with_message(&format!(
                            "the Approval authorises `{subject}` and this check is about `{}`",
                            restore_name.unwrap_or_default()
                        ))
                        .with_remedy("Use the Approval that names this Restore.")
                } else {
                    approval_verdict(
                        &scope,
                        *verified,
                        reason.as_deref(),
                        message.as_deref(),
                        approved_plan_hash.as_deref(),
                        recomputed_plan_hash,
                        now,
                    )
                }
            } else {
                approval_verdict(
                    &scope,
                    *verified,
                    reason.as_deref(),
                    message.as_deref(),
                    approved_plan_hash.as_deref(),
                    recomputed_plan_hash,
                    now,
                )
            };
            // `min(10 m, notAfter)` on both rows: a verdict about a key must
            // not outlive the key, or a restore could be admitted on a window
            // that closed while the record still said `ready`.
            out.push(cap_expiry(state, not_after));
            out.push(cap_expiry(
                key_validity_row(matched_key_id.as_deref(), window, deadline, now),
                not_after,
            ));
            out
        }
    }
}

/// One `approval.state` verdict, from the Approval's own status and nothing
/// else, in the order plan hash, expiry, verified.
fn approval_verdict(
    scope: &CheckScope,
    verified: Option<bool>,
    reason: Option<&str>,
    message: Option<&str>,
    approved_plan_hash: Option<&str>,
    recomputed: &str,
    now: DateTime<Utc>,
) -> CheckOutcome {
    let op = PreflightOperation::Restore;
    let mk =
        |state: CheckState, code: CheckCode| outcome(op, CheckId::ApprovalState, state, code, now);
    // The hash first: an approval verified against a different plan is a
    // stronger, more actionable finding than "not verified yet", which would
    // send an operator to wait for something that has already happened.
    if let Some(approved) = approved_plan_hash {
        if approved != recomputed {
            return mk(CheckState::NotReady, CheckCode::ApprovalPlanMismatch)
                .with_scope(scope.clone())
                .with_message(&format!(
                    "the Approval authorises plan {} and this plan hashes to {}",
                    short_digest(approved),
                    short_digest(recomputed)
                ))
                .with_remedy(
                    "Any edit to the plan is a new plan and needs a new approval. Re-submit the \
                     plan for signature.",
                );
        }
    }
    // The expiry is the controller's, verbatim: `KeyIdExpired` is the reason
    // `controllers::approval` writes when the verifying key is past the
    // `notAfter` the resolved trust policy carries for it.
    if reason.is_some_and(|r| r == APPROVAL_REASON_KEY_ID_EXPIRED) {
        return mk(CheckState::NotReady, CheckCode::ApprovalExpired)
            .with_scope(scope.clone())
            // The controller's own sentence when it wrote one: it names the key
            // and the `notAfter`, which this row must not invent.
            .with_message(
                message.unwrap_or("the approver key that verified this approval has expired"),
            )
            .with_remedy("Have the restore re-approved with a current approver key.");
    }
    match verified {
        Some(true) => mk(CheckState::Ready, CheckCode::ApprovalVerified)
            .with_scope(scope.clone())
            .with_message("the Approval's Verified condition is True"),
        // Every other refusal is `ApprovalNotVerified` carrying the
        // controller's own words, including `KeyRetired`, `KeyRevoked` and
        // `KeyNotYetValid`, which are not folded into `ApprovalExpired`: a
        // revocation reported as an expiry sends an operator to extend a
        // window when the remedy is an investigation. The reason and message
        // say which.
        Some(false) => mk(CheckState::NotReady, CheckCode::ApprovalNotVerified)
            .with_scope(scope.clone())
            .with_message(message.or(reason).unwrap_or("the Approval did not verify"))
            .with_remedy("Read the Approval's own Verified condition for the exact refusal."),
        // No status yet is not a refusal: the Approval controller has not
        // reconciled it, which is a wait, `ApprovalPending`.
        None => mk(CheckState::Unknown, CheckCode::ApprovalPending)
            .with_scope(scope.clone())
            .with_message("the Approval has no verification status yet"),
    }
}

/// The published window, but only when it is about the key the verdict names.
///
/// `controllers::approval` writes the `keyId` inside the window for this
/// comparison. A window about another key would ground a green advisory row
/// in someone else's rotation schedule, which is worse than none.
fn matched_window<'a>(
    matched_key_id: Option<&str>,
    window: Option<&'a ApproverKeyWindow>,
) -> Option<&'a ApproverKeyWindow> {
    let window = window?;
    match matched_key_id {
        Some(id) if id == window.key_id => Some(window),
        _ => None,
    }
}

/// `approval.keyValidity`, the advisory row, reading the window the `Approval`
/// publishes:
///
/// * the window's `notAfter` falls before the restore's deadline →
///   `notReady`/`ApproverKeyExpiresBeforeDeadline`. Advisory, so it warns
///   ahead of time and never refuses; if the key does expire, the blocking
///   `approval.state` row relays `KeyIdExpired`, which is a fact and not a
///   forecast.
/// * the deadline falls inside the window → `ready`/`ApproverKeyValid`.
/// * no window is published, or it is about another key →
///   `unknown`/`ApproverKeyWindowUnknown`, never `ready`: unevaluated or stale
///   expiry information is unknown, not valid. `unknown` on an advisory row
///   does not make the aggregate `unknown`.
///
/// Not derived from `Verified=True` alone: that says the key was usable when
/// the controller last looked, not that it will be at the end of a restore
/// that has not started.
fn key_validity_row(
    key_id: Option<&str>,
    window: Option<&ApproverKeyWindow>,
    deadline: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> CheckOutcome {
    let op = PreflightOperation::Restore;
    let mk = |state: CheckState, code: CheckCode| {
        outcome(op, CheckId::ApprovalKeyValidity, state, code, now)
    };
    let Some(window) = window else {
        return mk(CheckState::Unknown, CheckCode::ApproverKeyWindowUnknown).with_message(
            &format!(
                "the Approval publishes no validity window for approver key `{}`, so this check \
             cannot say whether that key outlasts this restore; an unpublished window is \
             unknown and never valid",
                key_id.unwrap_or("<unknown>")
            ),
        );
    };
    match deadline {
        // `<`: the key expires before the deadline. A key whose `notAfter` is
        // exactly the deadline is not early, and `>` would warn about every
        // key that outlasts the restore.
        Some(deadline) if window.not_after < deadline => mk(
            CheckState::NotReady,
            CheckCode::ApproverKeyExpiresBeforeDeadline,
        )
        .with_message(&format!(
            "approver key `{}` expires at {}, before this restore's deadline at {}",
            window.key_id,
            window.not_after.to_rfc3339(),
            deadline.to_rfc3339()
        ))
        .with_remedy("Rotate the approver key, or start the restore sooner."),
        Some(_) => mk(CheckState::Ready, CheckCode::ApproverKeyValid).with_message(&format!(
            "the Approval controller verified this approval under approver key `{}`, whose \
             published validity window runs to {}; this restore's deadline falls inside it",
            window.key_id,
            window.not_after.to_rfc3339()
        )),
        // No deadline, so no comparison, and the sentence says so. Reachable:
        // `deadline` is `now + spec.deadlineSeconds`, an unbounded `int64` in
        // the CRD, so an absurd value overflows to `None`. The verdict is
        // unchanged (an advisory row cannot refuse), but a green row must not
        // describe work it did not do.
        None => mk(CheckState::Ready, CheckCode::ApproverKeyValid).with_message(&format!(
            "the Approval controller verified this approval under approver key `{}`, whose \
             published validity window runs to {}; this restore names no deadline to compare \
             it against",
            window.key_id,
            window.not_after.to_rfc3339()
        )),
    }
}

// ---------------------------------------------------------------------------
// The pod-status rows (tests/preflight_controller.rs finds this banner)
// ---------------------------------------------------------------------------

/// Which check a waiting code belongs to, adjusted for the operation.
/// [`check::attribute`] answers in the neutral `connection.*` vocabulary; a
/// restore's catalogue has the `target.*` equivalents instead.
#[must_use]
pub fn attribute_for(
    operation: PreflightOperation,
    waiting: &check::Waiting,
    projections: &check::Projections,
) -> Option<CheckId> {
    let id = check::attribute(waiting, projections)?;
    Some(match (operation, id) {
        (
            PreflightOperation::Restore | PreflightOperation::Switchover,
            CheckId::ConnectionCredentialProjected,
        ) => CheckId::TargetCredentialProjected,
        (_, other) => other,
    })
}

/// The **P** rows, plus whether every Job-sourced row is blocked.
///
/// "An unmatched code blocks the whole pod: every Job-sourced check becomes
/// `unknown` with `BlockedByPrerequisite`, and the named cause is reported."
/// That is a statement about the other rows, so it cannot live on any one of
/// them, and a caller that aggregated the pod rows alone would report `ready`
/// for a pod that never started.
#[must_use]
pub fn pod_outcomes(
    operation: PreflightOperation,
    waiting: Option<&check::Waiting>,
    projections: &check::Projections,
    relayed: bool,
    image_id: Option<&str>,
    now: DateTime<Utc>,
) -> (Vec<CheckOutcome>, bool) {
    let rows = pod_rows(operation);
    let ready_code = |id: CheckId| match id {
        CheckId::RunnerImage => CheckCode::ImageAvailable,
        CheckId::RunnerPod => CheckCode::PodStarted,
        _ => CheckCode::Projected,
    };
    let Some(w) = waiting else {
        if !relayed {
            // No waiting state and no relay: the pod has not produced anything
            // yet. Every P row is `unknown`, and so is every J row.
            return (
                rows.iter()
                    .map(|r| {
                        outcome(
                            operation,
                            r.id,
                            CheckState::Unknown,
                            CheckCode::PodNotStarted,
                            now,
                        )
                        .with_message("the check pod has not started")
                    })
                    .collect(),
                true,
            );
        }
        // A verified relay is the proof: the runner wrote framed output from
        // inside the pod, so the image pulled, the pod started and every
        // projection succeeded. Those are preconditions of the bytes, not
        // inferences.
        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            let mut row = outcome(operation, r.id, CheckState::Ready, ready_code(r.id), now)
                .with_message("the check pod ran with this projection in place");
            if r.id == CheckId::RunnerImage {
                row = row
                    .with_message("the runner image was present and the container started")
                    .with_fact("imageID", image_id.unwrap_or("<not reported>"));
            }
            out.push(row);
        }
        return (out, false);
    };

    let attributed = attribute_for(operation, w, projections);
    let carrier = attributed.filter(|id| rows.iter().any(|r| r.id == *id));
    // The waiting state is still reported below (a pod disrupted after it
    // wrote its frames is a fact an operator wants), but a verified relay is
    // never discarded: replacing a result that exists with
    // `BlockedByPrerequisite` placeholders is strictly worse than reporting
    // both, and `blocked` means "drop every relayed row".
    let blocks_the_job_rows = !relayed;
    let mut out = Vec::with_capacity(rows.len());
    for r in &rows {
        if Some(r.id) == carrier {
            out.push(
                outcome(operation, r.id, CheckState::NotReady, w.code, now)
                    .with_message(&w.message)
                    .with_remedy(&remedy_for(w.code)),
            );
        } else {
            out.push(
                outcome(
                    operation,
                    r.id,
                    CheckState::Unknown,
                    CheckCode::PodNotStarted,
                    now,
                )
                .with_message("the check pod did not get far enough to answer this"),
            );
        }
    }
    if carrier.is_none() {
        // The cause is named on `runner.pod` rather than attributed to a
        // guess: a code naming a Secret this plan did not project is
        // attributed to nothing (`check::attribute`), because sending an
        // operator to rotate the wrong credential is worse.
        if let Some(slot) = out.iter_mut().find(|o| o.id == CheckId::RunnerPod) {
            *slot = outcome(
                operation,
                CheckId::RunnerPod,
                CheckState::NotReady,
                w.code,
                now,
            )
            .with_message(&w.message)
            .with_remedy(&remedy_for(w.code));
        }
    }
    (out, blocks_the_job_rows)
}

/// One sentence an operator can act on, per pod-level code.
#[must_use]
pub fn remedy_for(code: CheckCode) -> String {
    match code {
        CheckCode::CredentialSecretNotFound => {
            "Create the Secret the connection or the destination names, in this namespace. The \
             controller holds no verb on secrets, so this is the kubelet's report and not a \
             guess."
        }
        CheckCode::CredentialSecretKeyMissing => {
            "Add the named key to that Secret, or point the reference at the key that is there."
        }
        CheckCode::TrustBundleNotFound => {
            "Create the ConfigMap the destination's caBundle names, with the declared key."
        }
        CheckCode::RunnerImagePullFailed => {
            "Check the image reference and the pull credentials for this namespace."
        }
        CheckCode::RunnerImageNotPresent => {
            "The pull policy is `Never` and the node has no such image. Load the image for this \
             node's architecture, or set a pull policy that fetches it."
        }
        CheckCode::RunnerImageInvalid => "Correct the runner image reference.",
        CheckCode::PodUnschedulable => {
            "No node can take the check pod. Check taints, node selectors and resource requests."
        }
        CheckCode::SigningKeyMissing => {
            "Create the `logweir-signing-key` Secret in this namespace; the runner opens it as a \
             file and cannot sign evidence without it."
        }
        CheckCode::VolumeMountFailed => "The named volume did not mount; check its source object.",
        CheckCode::RunnerServiceAccountMissing => {
            "Create the ServiceAccount the check runs as, or point the destination's grant at one \
             that exists."
        }
        CheckCode::PodCreateRejected => {
            "The pod was refused before it started — a ResourceQuota, a PodSecurity level or an \
             admission webhook. The refusal is quoted above."
        }
        CheckCode::DisruptedMidCheck => {
            "The node went away mid-check. Nothing about the subject was observed; run the check \
             again."
        }
        CheckCode::DeadlineExceeded => {
            "The check Job reached its deadline without producing a result. Raise \
             `spec.request.timeoutSeconds`, or fix whatever the pod was waiting for."
        }
        CheckCode::ResultUnreadable => {
            "The runner's output did not verify. Re-run the check; if it persists the runner \
             image and this controller disagree about the check contract."
        }
        CheckCode::RunnerContractUnsupported => {
            "This runner image does not implement `logweir check run`. Upgrade the runner image \
             to one that ships the check contract."
        }
        // The runner parsed the plan and refused a field or a bound this
        // controller wrote: an older runner image than the controller.
        CheckCode::CheckContractMismatch => {
            "The runner image is older than this controller and refused the check plan it \
             rendered (the message names the field when the runner said which). Upgrade the \
             runner image to this controller's release."
        }
        CheckCode::CheckPlanConflict => {
            "A plan ConfigMap with this check's name already exists and is not this check's. \
             Delete the stale object, or create a new Preflight."
        }
        CheckCode::ResultStorageConflict => {
            "A result ConfigMap with this check's name already exists and holds different bytes."
        }
        CheckCode::ConcurrencyLimited => {
            "The installation's check concurrency ceiling is reached. This check is queued and \
             starts when a slot frees."
        }
        CheckCode::CancelRequested => "The check was cancelled on request.",
        CheckCode::Stalled => {
            "The check produced no Job and no result within its own budget. Create a new \
             Preflight."
        }
        _ => "See the per-check findings.",
    }
    .to_string()
}

/// Put one pass's findings together: pure, and the one place the verdict's row
/// set is decided.
///
/// 1. The controller's own rows first: a **C** row is the controller's answer,
///    and a relayed duplicate would be a second answer with one id.
/// 2. The relayed rows, for every id the controller did not answer.
/// 3. Every row `expected` names that nobody answered, as `unknown` with
///    [`CheckCode::BlockedByPrerequisite`]: never absent, because an absent
///    blocking row cannot hold the verdict back.
/// 4. The rows the request asked to skip, as `skipped`, which keeps the
///    overall verdict `unknown`: skipping a question is not answering it.
///
/// `expected` is [`job_rows`] over the rendered plan, or
/// [`unrendered_job_rows`] when no plan could be rendered; a parameter, so the
/// rule that must match the runner's emission lives in one guarded function.
#[must_use]
pub fn assemble(
    controller: Vec<CheckOutcome>,
    relayed: Vec<CheckOutcome>,
    blocked: bool,
    expected: &BTreeSet<CheckId>,
    skipped: &BTreeSet<CheckId>,
    now: DateTime<Utc>,
) -> Vec<CheckOutcome> {
    let mut seen: BTreeSet<CheckId> = BTreeSet::new();
    let mut out: Vec<CheckOutcome> = Vec::new();
    for c in controller {
        if seen.insert(c.id) {
            out.push(c);
        }
    }
    if !blocked {
        for c in relayed {
            if seen.insert(c.id) {
                out.push(c);
            }
        }
    }
    for id in expected.iter().copied() {
        if seen.contains(&id) || skipped.contains(&id) {
            continue;
        }
        seen.insert(id);
        out.push(
            CheckOutcome::new(
                id,
                CheckState::Unknown,
                // A row nobody answered is blocking, whatever the runner's
                // catalogue says: the verdict must not be `ready` here.
                Gating::Blocking,
                Authority::CheckJob,
                CheckCode::BlockedByPrerequisite,
            )
            .with_message("the check Job did not report this row")
            .with_times(now, now),
        );
    }
    for id in skipped {
        if seen.insert(*id) {
            out.push(
                CheckOutcome::new(
                    *id,
                    CheckState::Skipped,
                    Gating::Blocking,
                    Authority::Controller,
                    // The closed vocabulary has no "the requester asked me not
                    // to" code; `BlockedByPrerequisite` with the `skipped` state
                    // means the request's own `skipChecks` blocked it.
                    CheckCode::BlockedByPrerequisite,
                )
                .with_message("this check was skipped at the request's own asking")
                .with_remedy(
                    "A skipped blocking check keeps the overall verdict `unknown`. Create a \
                     Preflight without the skip to get an answer.",
                )
                .with_times(now, now),
            );
        }
    }
    out.sort_by_key(|c| c.id);
    out
}

/// The objects a row's `scope` names when the row itself did not name one.
///
/// One per authority, because the authority says who observed the thing:
/// `podStatus` is the kubelet reporting on a Pod, `checkJob` the runner inside
/// a Job, `controller` this process about the `Preflight`. A row that knows a
/// better referent (the `KafkaCluster` a connection dialled, the
/// `BackupDestination` a grant belongs to, the `TrustRoster` a key is on) sets
/// its own and keeps it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScopeReferents {
    /// The `Preflight` being reconciled. Always known.
    pub subject: Option<CheckScope>,
    /// The check Job, once one exists.
    pub job: Option<CheckScope>,
    /// The check pod, once the Job has one.
    pub pod: Option<CheckScope>,
}

impl ScopeReferents {
    /// The referent for one authority, falling back outward: a pod row with no
    /// pod is still about the Job that would have made one, and everything
    /// falls back to the subject.
    #[must_use]
    pub fn for_authority(&self, authority: Authority) -> Option<&CheckScope> {
        let chain: [&Option<CheckScope>; 3] = match authority {
            Authority::PodStatus => [&self.pod, &self.job, &self.subject],
            Authority::CheckJob => [&self.job, &self.subject, &self.subject],
            Authority::Controller => [&self.subject, &self.subject, &self.subject],
        };
        chain.into_iter().flatten().next()
    }
}

/// Give every row that carries no `scope` the referent its authority implies.
///
/// The UI names each failed prerequisite and its remedy, with check time and
/// scope; a row with `scope: null` leaves "which object is this about" blank on
/// exactly the rows an operator reads.
///
/// It runs after [`assemble`], which decides which rows exist and builds the
/// placeholders (rows nobody answered, rows the request skipped) that need a
/// referent too.
pub fn fill_scopes(checks: &mut [CheckOutcome], referents: &ScopeReferents) {
    for c in checks.iter_mut() {
        if c.scope.is_some() {
            continue;
        }
        c.scope = referents.for_authority(c.authority).cloned();
    }
}

/// The `Preflight` itself, as a scope.
#[must_use]
pub fn subject_scope(pf: &Preflight) -> CheckScope {
    CheckScope {
        kind: Preflight::kind(&()).to_string(),
        name: pf.name_any(),
        uid: pf.uid(),
    }
}

/// The overall state's wire spelling.
#[must_use]
pub fn state_str(state: OverallState) -> &'static str {
    match state {
        OverallState::Ready => "ready",
        OverallState::NotReady => "notReady",
        OverallState::Unknown => "unknown",
    }
}

/// One outcome, as the shipped CRD's `status.result.checks[]` entry.
///
/// [`CheckOutcome`] carries `facts` (`clusterId`, `brokerCount`, `imageID`,
/// `signerKeyId`) and a bounded `detail`; the shipped `Preflight` CRD carries
/// neither, because free-form JSON in a structural schema needs
/// `x-kubernetes-preserve-unknown-fields` (see `crds/preflight.rs`). So the
/// facts are rendered into the message, deterministically ordered, then
/// redacted and capped again; the long form lives in the details `ConfigMap`
/// that `result.detailsRef` names.
#[must_use]
pub fn entry_of(outcome: &CheckOutcome) -> CheckEntry {
    let mut message = outcome.message.clone();
    if !outcome.facts.is_empty() {
        let facts = outcome
            .facts
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("; ");
        message = format!("{message} [{facts}]");
    }
    if let Some(detail) = outcome.detail.as_ref() {
        message = format!("{message} [detail: {detail}]");
    }
    CheckEntry {
        id: outcome.id.as_str().to_string(),
        category: Some(outcome.category.clone()),
        scope: outcome.scope.as_ref().map(|s| StatusScope {
            kind: Some(s.kind.clone()),
            name: Some(s.name.clone()),
            uid: s.uid.clone(),
        }),
        state: match outcome.state {
            CheckState::Ready => "ready",
            CheckState::NotReady => "notReady",
            CheckState::Unknown => "unknown",
            CheckState::Skipped => "skipped",
        }
        .to_string(),
        gating: Some(
            match outcome.gating {
                Gating::Blocking => "blocking",
                Gating::Advisory => "advisory",
                Gating::ExecutionOnly => "executionOnly",
            }
            .to_string(),
        ),
        authority: Some(
            match outcome.authority {
                Authority::Controller => "controller",
                Authority::CheckJob => "checkJob",
                Authority::PodStatus => "podStatus",
            }
            .to_string(),
        ),
        code: Some(outcome.code.as_str().to_string()),
        message: Some(redact(&message)).filter(|m| !m.is_empty()),
        remedy: Some(outcome.remedy.clone()).filter(|m| !m.is_empty()),
        observed_at: outcome.observed_at,
        expires_at: outcome.expires_at,
    }
}

/// How many rows `status.result.checks` may hold: the shipped CRD's own
/// `maxItems`, which the API server enforces with a 422.
pub const MAX_PUBLISHED_CHECKS: usize = logweir_core::check_contract::MAX_CHECK_ENTRIES;

/// The aggregated verdict, decided by the pure layer.
///
/// Returns the result and how many rows did not fit, which the caller puts in
/// `status.message`.
///
/// The relay is capped at 64 rows and `assemble` adds controller and pod rows
/// on top, but the CRD declares `maxItems: 64`; an oversized merge patch is a
/// 422 and the verdict would never be published. So the list is capped here,
/// after `aggregate` / `aggregate_expires_at` ran over every row: truncation
/// must never change the verdict (or more rows could publish `ready`). The cap
/// decides only which rows an operator reads, most useful first: blocking rows
/// that are not `ready`, because they explain the verdict.
#[must_use]
pub fn result_for(
    checks: &[CheckOutcome],
    details: Option<DetailsRef>,
) -> (PreflightResult, usize) {
    let state = state_str(aggregate(checks)).to_string();
    let expires_at = aggregate_expires_at(checks);

    let rank = |c: &CheckOutcome| match (c.gating, c.state) {
        (Gating::Blocking, CheckState::NotReady) => 0u8,
        (Gating::Blocking, CheckState::Unknown | CheckState::Skipped) => 1,
        (Gating::Advisory, CheckState::NotReady) => 2,
        (Gating::Blocking, CheckState::Ready) => 3,
        _ => 4,
    };
    let mut ordered: Vec<&CheckOutcome> = checks.iter().collect();
    // Stable, so the id order `assemble` established survives inside a rank.
    ordered.sort_by_key(|c| rank(c));
    let dropped = ordered.len().saturating_sub(MAX_PUBLISHED_CHECKS);
    ordered.truncate(MAX_PUBLISHED_CHECKS);
    // Published in id order, whatever the rank order was: a reader scanning a
    // status wants the catalogue's order, not the controller's triage.
    ordered.sort_by_key(|c| c.id);

    (
        PreflightResult {
            state,
            expires_at,
            checks: Some(ordered.into_iter().map(entry_of).collect()),
            details_ref: details,
        },
        dropped,
    )
}

/// One recorded binding, as `status.binding`.
#[must_use]
pub fn binding_status(inputs: &BindingInputs) -> PreflightBinding {
    PreflightBinding {
        operation: Some(
            match inputs.operation {
                CheckOperation::Backup => "Backup",
                CheckOperation::Restore => "Restore",
                CheckOperation::DestinationAccess => "DestinationAccess",
                CheckOperation::SourceConnection => "SourceConnection",
                CheckOperation::Switchover => "Switchover",
            }
            .to_string(),
        ),
        plan_hash: inputs.plan_hash.clone(),
        inputs_digest: Some(inputs_digest(inputs)),
        referents: Some(
            inputs
                .referents
                .iter()
                .map(|r| StatusReferent {
                    kind: r.kind.clone(),
                    name: r.name.clone(),
                    uid: Some(r.uid.clone()),
                    generation: r.generation,
                })
                .collect(),
        ),
        policy_digest: Some(inputs.policy_digest.clone()),
    }
}

/// Why a recorded verdict no longer applies.
///
/// `logweir_core::check_contract::stale_reasons` compares two whole
/// [`BindingInputs`], as the API does with live objects. The controller holds
/// the recorded side only as `status.binding`, which is narrower (the digest,
/// the plan hash, the referents and the policy digest, not the CA bundle list
/// or the approval's resource version). So this compares what the status
/// carries and leans on the digest for the rest: `inputsDigest` covers all of
/// it, so a change the named comparisons cannot see still surfaces as
/// [`logweir_core::check_contract::StaleReason::InputsDigestChanged`].
///
/// The empty vector means the verdict still applies.
#[must_use]
pub fn stale_against_status(
    recorded: &PreflightBinding,
    recorded_expires_at: Option<DateTime<Utc>>,
    current: &BindingInputs,
    now: DateTime<Utc>,
) -> Vec<logweir_core::check_contract::StaleReason> {
    use logweir_core::check_contract::StaleReason;
    let mut out = Vec::new();
    if recorded_expires_at.is_none_or(|e| now >= e) {
        out.push(StaleReason::Expired);
    }
    if recorded.plan_hash != current.plan_hash {
        out.push(StaleReason::PlanHashChanged);
    }
    let recorded_referents = recorded.referents.clone().unwrap_or_default();
    let key = |kind: &str, name: &str| format!("{kind}/{name}");
    let mut names: BTreeSet<String> = BTreeSet::new();
    let mut before: BTreeMap<String, (Option<String>, Option<i64>)> = BTreeMap::new();
    for r in &recorded_referents {
        names.insert(key(&r.kind, &r.name));
        before.insert(key(&r.kind, &r.name), (r.uid.clone(), r.generation));
    }
    let mut after: BTreeMap<String, (Option<String>, Option<i64>)> = BTreeMap::new();
    for r in &current.referents {
        names.insert(key(&r.kind, &r.name));
        after.insert(key(&r.kind, &r.name), (Some(r.uid.clone()), r.generation));
    }
    for name in names {
        if before.get(&name) != after.get(&name) {
            out.push(StaleReason::ReferentChanged(name));
        }
    }
    if recorded.policy_digest.as_deref() != Some(current.policy_digest.as_str()) {
        out.push(StaleReason::PolicyChanged);
    }
    // The digest is the authority on "did anything change", checked last so a
    // difference the named comparisons cannot see is still reported.
    let digests_differ = recorded.inputs_digest.as_deref() != Some(inputs_digest(current).as_str());
    let only_expiry = out.iter().all(|r| *r == StaleReason::Expired);
    if digests_differ && only_expiry {
        out.push(StaleReason::InputsDigestChanged);
    }
    out
}

/// Downgrade a stored verdict that no longer applies. Pure.
///
/// `ready` becomes `unknown`, not `notReady`: nothing was found wrong; the
/// answer stopped being about the current objects.
///
/// Returns `None` when the verdict still applies and nothing needs writing.
#[must_use]
pub fn downgrade(
    result: &PreflightResult,
    stale: &[logweir_core::check_contract::StaleReason],
) -> Option<PreflightResult> {
    if stale.is_empty() || result.state != state_str(OverallState::Ready) {
        return None;
    }
    Some(PreflightResult {
        state: state_str(OverallState::Unknown).to_string(),
        ..result.clone()
    })
}

/// The stale reasons as the one comma-separated spelling every surface uses.
#[must_use]
pub fn stale_text(stale: &[logweir_core::check_contract::StaleReason]) -> String {
    stale
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Everything one status write carries.
#[derive(Clone, Debug, Default)]
pub struct StatusInput {
    /// One of [`PHASES`].
    pub phase: String,
    /// The scalar reason, always a closed code.
    pub reason: Option<CheckCode>,
    /// A redacted, bounded explanation.
    pub message: String,
    /// What this verdict is about. Recorded before the Job is created.
    pub binding: Option<PreflightBinding>,
    /// The check Job's name.
    pub job_ref: Option<String>,
    /// The instant the check was taken.
    pub observed_at: Option<DateTime<Utc>>,
    /// The verdict.
    pub result: Option<PreflightResult>,
}

/// The `/status` body one pass produces.
///
/// `Complete` says whether a result exists at all, and `Ready` restates the
/// verdict. `Ready` is `Unknown`, never `False`, for an `unknown` verdict: "I
/// could not tell" is not a refusal.
#[must_use]
pub fn status_for(pf: &Preflight, input: &StatusInput, now: DateTime<Utc>) -> PreflightStatus {
    let previous = pf.status.as_ref();
    let generation = pf.metadata.generation;
    let reason = input.reason.map_or_else(
        || CheckCode::NotReady.as_str().to_string(),
        |c| c.as_str().to_string(),
    );
    let complete = input.phase == PHASE_COMPLETED;
    let ready_status = match input.result.as_ref().map(|r| r.state.as_str()) {
        Some("ready") => "True",
        Some("notReady") => "False",
        _ => "Unknown",
    };
    let message = redact(&input.message);
    let condition = |type_: &str, status: &str, reason: &str, message: &str| {
        merge_condition(
            current_condition(previous.and_then(|s| s.conditions.as_ref()), type_),
            Condition {
                r#type: type_.to_string(),
                status: status.to_string(),
                observed_generation: generation,
                last_transition_time: Some(now),
                reason: Some(reason.to_string()),
                message: Some(message.to_string()),
            },
        )
    };
    PreflightStatus {
        phase: Some(input.phase.clone()),
        reason: Some(reason.clone()),
        message: Some(message.clone()).filter(|m| !m.is_empty()),
        // A binding is never unwritten: a later pass that could not resolve
        // the referents keeps the one the Job was created against, because a
        // vanished binding cannot be told apart from "never bound".
        binding: input
            .binding
            .clone()
            .or_else(|| previous.and_then(|s| s.binding.clone())),
        job_ref: input
            .job_ref
            .clone()
            .map(|name| crate::crds::LocalRef { name })
            .or_else(|| previous.and_then(|s| s.job_ref.clone())),
        observed_at: input
            .observed_at
            .or_else(|| previous.and_then(|s| s.observed_at)),
        result: input
            .result
            .clone()
            .or_else(|| previous.and_then(|s| s.result.clone())),
        conditions: Some(vec![
            condition(
                CONDITION_COMPLETE,
                if complete { "True" } else { "False" },
                &reason,
                &message,
            ),
            condition(CONDITION_READY, ready_status, &reason, &message),
        ]),
    }
}

/// The name a legacy inline archive carries in the check plan, where a saved
/// destination's name would be. It is never an object name, and
/// [`rescope_legacy`] replaces it on every relayed row before a verdict is
/// published.
pub const LEGACY_ARCHIVE_PLAN_NAME: &str = "inline-archive";

/// The scope kind of a row about a legacy inline archive: the
/// `legacySourceArchive` a `Backup` recorded in `spec.archive`, named by its
/// URL.
pub const LEGACY_ARCHIVE_SCOPE_KIND: &str = "InlineArchive";

/// A restore readiness check over a recovery point with no saved destination
/// (`spec.request.restore.legacySourceArchive`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacySource {
    /// The inline archive URL the request names.
    pub url: String,
    /// The Secret it names, when it names one: the restore Job's archive
    /// credential.
    pub secret: Option<String>,
}

impl LegacySource {
    /// The scope every row about this archive carries.
    #[must_use]
    pub fn scope(&self) -> CheckScope {
        CheckScope {
            kind: LEGACY_ARCHIVE_SCOPE_KIND.to_string(),
            name: self.url.clone(),
            uid: None,
        }
    }
}

/// What `plan.bindings` holds a legacy plan's location to.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LegacyBinding {
    /// The inline archive URL the request names.
    pub url: String,
    /// The named recovery point's `Backup.spec.archive.url`, when a point is
    /// named and was found: what the point was written to.
    pub point_url: Option<String>,
    /// That point's name, for the message.
    pub point: Option<String>,
}

/// `(scheme, bucket, prefix)` of an inline archive URL, normalised the way
/// `retention::bucket_and_prefix` normalises it, so `s3://b/p/` and `s3://b/p`
/// are one location.
fn url_location(url: &str) -> (String, String, String) {
    let scheme = url.split_once("://").map_or("", |(s, _)| s).to_string();
    let (bucket, prefix) = crate::retention::bucket_and_prefix(url);
    (scheme, bucket, prefix.trim_matches('/').to_string())
}

/// The legacy inline archive, resolved into the shape a check plan carries:
/// the principal and the location the restore Job itself will use. Pure.
///
/// An inline-archive `Restore`'s runner (`restore::runner_job_spec_with_policy`)
/// projects `spec.sourceArchive.secretRef`'s `access-key-id` and
/// `secret-access-key` as `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` and
/// reads the location the approved plan names (`source.storage`), filling an
/// omitted region or endpoint from the controller's forwarded addressing (the
/// values the installation policy publishes as `legacyArchiveAddressing`).
/// This builds exactly that: a `SecretKeys` grant on the same Secret and keys,
/// and the plan's location with the same two fallbacks. `path_style` and
/// `allow_http` always come from the plan, because the runner's store applies
/// them after the environment. So a green `archive.*` row is a real read, as
/// the restore will make it.
///
/// A custom endpoint is path-style whatever the plan says, because the engine
/// forces it. Transport is `InsecureHTTP` only for `allow_http: true` with an
/// explicit `http://` endpoint, and is never derived from anything else.
///
/// Fail closed: everything this cannot establish is a `destination.resolved`
/// refusal, which holds the Job back and the verdict at `notReady`: a URL that
/// is not `s3://`, a request that names no Secret (the restore Job would use
/// whatever credential chain its pod finds, which no check pod can
/// reproduce), a plan that does not parse or does not read S3, and a location
/// the destination rules refuse (an `http://` endpoint with
/// `allow_http: false`, which the runner cannot reach either).
///
/// # Errors
///
/// A [`destination::DestinationRefusal`] naming the field and the reason.
pub fn legacy_source_destination(
    namespace: &str,
    legacy: &LegacySource,
    plan_source: Option<&logweir_core::engine::StorageUrl>,
    addressing: &check_policy::LegacyArchiveAddressing,
) -> Result<ResolvedDestination, destination::DestinationRefusal> {
    use logweir_core::destination::{
        engine_compatible, validate, Addressing, DestinationLocation, StorageProvider,
        TransportSecurity,
    };
    use logweir_core::engine::StorageUrl;
    let refuse = |code: CheckCode, field: &str, message: String| destination::DestinationRefusal {
        code,
        field: field.to_string(),
        message,
    };
    let url = &legacy.url;
    let (scheme, bucket, _) = url_location(url);
    if scheme != "s3" || bucket.is_empty() {
        return Err(refuse(
            CheckCode::DestinationNotValid,
            "spec.request.restore.legacySourceArchive.url",
            format!(
                "the inline archive `{url}` is not an s3:// bucket; a restore readiness check \
                 reads S3 only, so this recovery point's archive cannot be read before the run"
            ),
        ));
    }
    let Some(secret) = legacy.secret.as_deref().filter(|s| !s.is_empty()) else {
        return Err(refuse(
            CheckCode::DestinationRoleNotConfigured,
            "spec.request.restore.legacySourceArchive.secretRef",
            format!(
                "the inline archive `{url}` names no secretRef, so the restore Job would read it \
                 with whatever credential chain its pod finds, and no check pod can prove that \
                 chain; name the Secret the Backup recorded in spec.archive.secretRef"
            ),
        ));
    };
    let Some(source) = plan_source else {
        return Err(refuse(
            CheckCode::PlanUnparseable,
            "spec.request.restore.planBytes",
            "the plan does not parse, so the location the restore would read is not known"
                .to_string(),
        ));
    };
    let StorageUrl::S3 {
        bucket,
        prefix,
        region,
        endpoint,
        path_style,
        allow_http,
    } = source
    else {
        return Err(refuse(
            CheckCode::DestinationNotValid,
            "spec.request.restore.planBytes",
            "the approved plan's source.storage is not S3, so no check pod can read it".to_string(),
        ));
    };
    let present = |v: &Option<String>| v.clone().filter(|s| !s.is_empty());
    let or_installation = |v: &str| Some(v.to_string()).filter(|s| !s.is_empty());
    let endpoint = present(endpoint).or_else(|| or_installation(&addressing.endpoint));
    let region = present(region).or_else(|| or_installation(&addressing.region));
    let location = DestinationLocation {
        provider: StorageProvider::S3,
        bucket: bucket.clone(),
        prefix: prefix.trim_matches('/').to_string(),
        region,
        addressing: if *path_style || endpoint.is_some() {
            Addressing::PathStyle
        } else {
            Addressing::VirtualHosted
        },
        transport: if *allow_http
            && endpoint
                .as_deref()
                .is_some_and(|e| e.starts_with("http://"))
        {
            TransportSecurity::InsecureHttp
        } else {
            TransportSecurity::Tls
        },
        endpoint,
    };
    if let Err(errors) = validate(&location) {
        return Err(refuse(
            CheckCode::DestinationNotValid,
            "spec.request.restore.planBytes",
            format!(
                "the approved plan's source.storage is not a location the restore Job can reach: \
                 {}",
                errors
                    .iter()
                    .map(|e| e.message.clone())
                    .collect::<Vec<_>>()
                    .join("; ")
            ),
        ));
    }
    if let Err(why) = engine_compatible(&location) {
        return Err(refuse(
            CheckCode::DestinationNotValid,
            "spec.request.restore.planBytes",
            why.to_string(),
        ));
    }
    Ok(ResolvedDestination {
        name: LEGACY_ARCHIVE_PLAN_NAME.to_string(),
        namespace: namespace.to_string(),
        uid: String::new(),
        generation: 0,
        role: DestinationRole::ArchiveRead,
        location_digest: location.location_digest(),
        canonical_url: location.canonical_url(),
        location,
        ca_bundle: None,
        ca_sha256: None,
        ca_pem: None,
        grant: destination::ResolvedGrant::SecretKeys {
            secret: secret.to_string(),
            access_key_id_key: super::backup::ARCHIVE_ACCESS_KEY.to_string(),
            secret_access_key_key: super::backup::ARCHIVE_SECRET_KEY.to_string(),
            session_token_key: None,
        },
        // An inline archive: its Secret is bound to the location.
        inline_archive: true,
    })
}

/// The inline archive an existing `Restore`'s Job reads: its own
/// `spec.sourceArchive`, URL and Secret.
#[must_use]
pub fn restore_legacy_source(restore: &Restore) -> LegacySource {
    LegacySource {
        url: restore.spec.source_archive.url.clone(),
        secret: restore
            .spec
            .source_archive
            .secret_ref
            .as_ref()
            .map(|s| s.name.clone()),
    }
}

/// A `restoreRef` check whose `legacySourceArchive` is not the `Restore`'s
/// own. `None` when they agree.
///
/// The request's CEL rule requires `legacySourceArchive` beside a `restoreRef`
/// for a point with no destination, but nothing ties the two together, so a
/// check could read with Secret A while the `Restore` projects Secret B. A
/// readiness check reads as the restore will, so another archive or Secret is
/// `notReady`, both named, and no Job is created.
#[must_use]
pub fn legacy_request_conflict(
    requested: &LegacySource,
    own: &LegacySource,
    restore_name: &str,
) -> Option<destination::DestinationRefusal> {
    if requested == own {
        return None;
    }
    let secret = |s: &LegacySource| {
        s.secret
            .as_deref()
            .map_or_else(|| "no Secret".to_string(), |n| format!("Secret `{n}`"))
    };
    Some(destination::DestinationRefusal {
        code: CheckCode::PlanDestinationMismatch,
        field: "spec.request.restore.legacySourceArchive".to_string(),
        message: format!(
            "this check names inline archive `{}` read with {}, and Restore `{restore_name}` \
             reads `{}` with {}; a readiness check reads as the restore Job will, so it must \
             name the Restore's own spec.sourceArchive",
            requested.url,
            secret(requested),
            own.url,
            secret(own)
        ),
    })
}

/// `destination.resolved` for a legacy inline archive: what the check reads,
/// as whom, and that there is no `BackupDestination` behind it.
#[must_use]
pub fn legacy_source_row(
    legacy: &LegacySource,
    resolved: Option<&DestinationResolution>,
    now: DateTime<Utc>,
) -> CheckOutcome {
    let op = PreflightOperation::Restore;
    match resolved {
        Some(Ok(d)) => outcome(
            op,
            CheckId::DestinationResolved,
            CheckState::Ready,
            CheckCode::DestinationValid,
            now,
        )
        .with_scope(legacy.scope())
        .with_message(&format!(
            "no BackupDestination: this recovery point's inline archive is read at {} ({}, {}) \
             with Secret `{}` (keys access-key-id and secret-access-key) — the location the \
             approved plan names and the credential the restore Job projects",
            d.canonical_url,
            d.location.endpoint.as_deref().unwrap_or("AWS S3"),
            d.location.transport.as_str(),
            legacy.secret.as_deref().unwrap_or_default(),
        )),
        Some(Err(refusal)) => outcome(
            op,
            CheckId::DestinationResolved,
            CheckState::NotReady,
            refusal.code,
            now,
        )
        .with_scope(legacy.scope())
        .with_message(&refusal.message)
        .with_remedy(&format!(
            "Fix `{}` and create a new Preflight, or save the archive as a BackupDestination \
             and restore from it by destinationRef.",
            refusal.field
        )),
        // Unreachable by construction (`resolve` always resolves a legacy
        // source), and answered as the refusal it would be, never as ready.
        None => outcome(
            op,
            CheckId::DestinationResolved,
            CheckState::NotReady,
            CheckCode::DestinationNotValid,
            now,
        )
        .with_scope(legacy.scope())
        .with_message("the inline archive was not resolved, so nothing can be checked"),
    }
}

/// `destination.evidenceReadable` for a legacy restore: advisory, answered by
/// the controller, and present only when the run's evidence would not be
/// verified.
///
/// An inline-archive run's scorecard is read only through the controller's
/// archive handle, and only in that handle's bucket
/// (`destination::legacy_evidence_scope`, `restore::legacy_evidence_source`).
/// A plan writing its evidence elsewhere restores the data and then publishes
/// `NotAttempted` with no completion. This says so before the approver signs,
/// while the plan can still change, and never blocks.
///
/// Never `ready`: comparing two bucket names is not a read, so the matching
/// case publishes no row.
#[must_use]
pub fn legacy_evidence_row(
    legacy: &LegacySource,
    plan_evidence: &logweir_core::engine::StorageUrl,
    handle_archive_url: Option<&str>,
    now: DateTime<Utc>,
) -> Option<CheckOutcome> {
    use crate::destination::LegacyEvidenceScope;
    let scope = destination::legacy_evidence_scope(plan_evidence, handle_archive_url);
    let message = match &scope {
        LegacyEvidenceScope::GlobalHandleApplies => return None,
        LegacyEvidenceScope::HandleLocationUnstated => {
            "this controller holds no archive handle (LOGWEIR_ARCHIVE_URL is not set), and an \
             inline-archive restore's scorecard is read only through it: this run would publish \
             verification NotAttempted and no completion"
                .to_string()
        }
        // The handle by role, never by value: this message is in a status
        // every operator of the namespace reads.
        LegacyEvidenceScope::Elsewhere { evidence, .. } => format!(
            "the plan writes its scorecard to {evidence}, which is not the bucket of {}; an \
             inline-archive restore's evidence is read only through that handle, so this run \
             would publish verification NotAttempted and no completion",
            destination::ARCHIVE_HANDLE_LABEL
        ),
    };
    Some(
        outcome(
            PreflightOperation::Restore,
            CheckId::DestinationEvidenceReadable,
            CheckState::Unknown,
            CheckCode::EvidenceReadNotConfigured,
            now,
        )
        .with_scope(legacy.scope())
        .with_message(&message)
        .with_remedy(
            "Write the evidence to the bucket of the controller's archive handle — on the \
             chart's default install the recovery point's own archive bucket, which the console \
             starts the field on — and check again; the restore itself is not affected, only its \
             verification.",
        )
        .with_fact("grant", "evidenceRead"),
    )
}

/// Every relayed row the runner scoped to the legacy archive's placeholder
/// name, rescoped to the archive itself: there is no
/// `BackupDestination/<plan name>` for a legacy source, and a scope naming one
/// would send an operator looking for it. The same placeholder phrase in a
/// message is rewritten to name the inline archive; nothing else is touched.
#[must_use]
pub fn rescope_legacy(mut checks: Vec<CheckOutcome>, legacy: &LegacySource) -> Vec<CheckOutcome> {
    let placeholder = format!("destination `{LEGACY_ARCHIVE_PLAN_NAME}`");
    let named = format!("inline archive `{}`", legacy.url);
    for c in &mut checks {
        if c.scope
            .as_ref()
            .is_some_and(|s| s.kind == "BackupDestination" && s.name == LEGACY_ARCHIVE_PLAN_NAME)
        {
            c.scope = Some(legacy.scope());
        }
        if c.message.contains(&placeholder) {
            // Through the chokepoint again: the URL came from a request, and a
            // relayed message is only ever published redacted and capped.
            c.message = redact(&c.message.replace(&placeholder, &named));
        }
    }
    checks
}

/// The subject's own facts plus every referent, reduced to what the rows and
/// the binding read.
///
/// One struct and one resolution pass, because the binding must be computed
/// from exactly the objects the checks were computed from.
#[derive(Debug)]
pub struct Inputs {
    /// Which operation.
    pub operation: PreflightOperation,
    /// The subject's namespace.
    pub namespace: String,
    /// The rows the request asked to skip.
    pub skip: BTreeSet<CheckId>,
    /// The in-Job budget.
    pub timeout_seconds: i64,
    /// The installation policy in force.
    pub policy_digest: String,
    /// Whether the policy read.
    pub policy: Option<check_policy::PolicyLoad>,
    /// The roster.
    pub roster: RosterFacts,
    /// The `KafkaCluster` this check dials, when it dials one.
    pub cluster_name: Option<String>,
    /// Its UID.
    pub cluster_uid: Option<String>,
    /// Its generation.
    pub cluster_generation: Option<i64>,
    /// `KafkaCluster.status.clusterId`, the recorded identity.
    pub cluster_recorded_id: Option<String>,
    /// The resolution, or `None` when no such object exists.
    pub connection: Option<Result<ResolvedConnection, connection::ConnectionRefusal>>,
    /// The archive-side destination's name.
    pub archive_name: Option<String>,
    /// Its resolution.
    pub archive: Option<Result<ResolvedDestination, destination::DestinationRefusal>>,
    /// The evidence destination's name, when there is a second one.
    pub evidence_name: Option<String>,
    /// Its resolution.
    pub evidence: Option<Result<ResolvedDestination, destination::DestinationRefusal>>,
    /// The grants this check exercises.
    pub roles: Vec<DestinationRole>,
    /// Whether the archive-side destination opted in to the create-only
    /// readiness marker (`spec.readiness.writeProbe: CreateOnlyMarker`). Read
    /// from the object, never assumed ([`destination_spec_facts`]).
    pub write_probe: bool,
    /// The archive-side destination's own `evidenceWrite` grant, resolved from
    /// the same object: the principal `destination.evidenceWritable` is about.
    /// Resolved for a Backup or `DestinationAccess` check that requests
    /// `EvidenceWrite`; `None` otherwise. See
    /// [`Inputs::evidence_write_principal`].
    pub evidence_write_grant:
        Option<Result<destination::ResolvedGrant, destination::DestinationRefusal>>,
    /// The same object's own `evidenceRead` grant: the principal
    /// `destination.evidenceReadable` is about. Resolved for a Backup or
    /// `DestinationAccess` check that requests `EvidenceRead`; `None`
    /// otherwise. See [`Inputs::evidence_read_plan`].
    pub evidence_read_grant:
        Option<Result<destination::ResolvedGrant, destination::DestinationRefusal>>,
    /// The archive destination's `SecretKeys` grants whose binding the check
    /// compares ([`grant_bindings_of`]); empty when it has none, or when no
    /// destination object was read.
    pub grant_bindings: Vec<GrantBindingRef>,
    /// A backup check's topic set.
    pub topics: Vec<String>,
    /// A restore check's plan.
    pub plan: Option<PlanFacts>,
    /// What `plan.bindings` compares against.
    pub bindings: BindingFacts,
    /// The recovery point.
    pub recovery_point: RecoveryPointFacts,
    /// Or a catalog point. When requested, it is what `recoveryPoint.state`
    /// reports.
    pub catalog_point: CatalogPointFacts,
    /// The approval.
    pub approval: ApprovalFacts,
    /// The approval's contribution to the binding.
    pub approval_binding: Option<ApprovalRef>,
    /// The `Restore` this check is about, when it is about one.
    pub restore_name: Option<String>,
    /// The instant the approver key must still be valid at.
    pub deadline: Option<DateTime<Utc>>,
    /// The backup set the restore reads.
    pub backup_id: String,
    /// The recovery point's own source cluster id, for `TargetEqualsSource`.
    pub source_cluster_id: Option<String>,
    /// Every referent, for the binding.
    pub referents: Vec<Referent>,
    /// Every CA bundle digest, for the binding.
    pub ca_bundles: Vec<CaBundleRef>,
    /// A backup check's inline archive, which this build cannot render into a
    /// check plan (`Failed`/`ArchiveUrlUnreadable`).
    pub legacy_archive: Option<String>,
    /// A restore check's inline archive: a recovery point with no saved
    /// destination, read through [`legacy_source_destination`] with the
    /// restore Job's own principal.
    pub legacy_source: Option<LegacySource>,
    /// The controller's archive handle location, `LOGWEIR_ARCHIVE_URL`, which
    /// [`legacy_evidence_row`] compares a legacy plan's evidence with.
    pub legacy_evidence_handle: Option<String>,
    /// A `Switchover` review's `Restore` and keys (PROD-04.2 §3.8).
    pub switchover: Option<SwitchoverFacts>,
}

impl Default for Inputs {
    fn default() -> Self {
        Self {
            // `PreflightOperation` has no `Default` (the operation is required
            // on the wire); this one exists for tests and `..Inputs::default()`,
            // and every real construction sets it from the request.
            operation: PreflightOperation::Backup,
            namespace: String::new(),
            skip: BTreeSet::new(),
            timeout_seconds: 0,
            policy_digest: String::new(),
            policy: None,
            roster: RosterFacts::default(),
            cluster_name: None,
            cluster_uid: None,
            cluster_generation: None,
            cluster_recorded_id: None,
            connection: None,
            archive_name: None,
            archive: None,
            evidence_name: None,
            evidence: None,
            roles: Vec::new(),
            write_probe: false,
            evidence_write_grant: None,
            evidence_read_grant: None,
            grant_bindings: Vec::new(),
            topics: Vec::new(),
            plan: None,
            bindings: BindingFacts::default(),
            recovery_point: RecoveryPointFacts::NotRequested,
            catalog_point: CatalogPointFacts::NotRequested,
            approval: ApprovalFacts::Draft,
            approval_binding: None,
            restore_name: None,
            deadline: None,
            backup_id: String::new(),
            source_cluster_id: None,
            referents: Vec::new(),
            ca_bundles: Vec::new(),
            legacy_archive: None,
            legacy_source: None,
            legacy_evidence_handle: None,
            switchover: None,
        }
    }
}

/// What the relay told the controller about facts only the pod could see.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RelayFacts {
    /// The broker's cluster id, from the `clusterId` fact.
    pub cluster_id: Option<String>,
    /// The runner's public signing key id, from the `signerKeyId` fact.
    pub signer_key_id: Option<String>,
}

impl RelayFacts {
    /// Read the two facts out of a verified relay's outcomes.
    #[must_use]
    pub fn of(checks: &[CheckOutcome]) -> Self {
        let fact = |id: CheckId, key: &str| {
            checks
                .iter()
                .find(|c| c.id == id)
                .and_then(|c| c.facts.get(key))
                .cloned()
        };
        Self {
            cluster_id: fact(CheckId::ConnectionAuthenticated, "clusterId")
                .or_else(|| fact(CheckId::TargetAuthenticated, "clusterId")),
            signer_key_id: fact(CheckId::SignerPrivateKeyUsable, "signerKeyId"),
        }
    }
}

impl Inputs {
    /// The binding this verdict is about.
    #[must_use]
    pub fn binding(&self) -> BindingInputs {
        BindingInputs {
            operation: check_operation(self.operation),
            plan_hash: self.plan.as_ref().map(|p| p.recomputed_hash.clone()),
            topics: (self.operation == PreflightOperation::Backup).then(|| self.topics.clone()),
            referents: self.referents.clone(),
            ca_bundles: self.ca_bundles.clone(),
            roster: self.roster.binding(),
            approval: self.approval_binding.clone(),
            policy_digest: self.policy_digest.clone(),
        }
    }

    /// What the check pod was asked to project, so a waiting code can be
    /// attributed to a row rather than to the pod.
    #[must_use]
    pub fn projections(&self) -> check::Projections {
        let secret_of =
            |d: Option<&Result<ResolvedDestination, destination::DestinationRefusal>>| match d {
                Some(Ok(d)) => match &d.grant {
                    destination::ResolvedGrant::SecretKeys { secret, .. } => Some(secret.clone()),
                    _ => None,
                },
                _ => None,
            };
        check::Projections {
            connection_secret: match self.connection.as_ref() {
                Some(Ok(c)) => c.password.as_ref().map(|p| p.name.clone()),
                _ => None,
            },
            destination_secret: secret_of(self.archive.as_ref())
                .or_else(|| secret_of(self.evidence.as_ref())),
            evidence_write_secret: match self.evidence_write_principal() {
                Ok(Some((grant, _))) => grant.secret_name,
                _ => None,
            },
            evidence_read_secret: match self.evidence_read_plan() {
                Ok(EvidenceReadPlan::Separate(grant, _)) => grant.secret_name,
                _ => None,
            },
            signer_secret: self
                .signs()
                .then(|| super::backup::SIGNING_KEY_SECRET.to_string()),
            trust_config_maps: match self.archive.as_ref() {
                Some(Ok(d)) => d
                    .ca_bundle
                    .as_ref()
                    .map(|c| vec![c.config_map_name.clone()])
                    .unwrap_or_default(),
                _ => Vec::new(),
            },
        }
    }

    /// Whether this operation's pod needs the signing key: only a backup
    /// (its receipt) and a restore (its evidence) sign anything. A
    /// destination-access or source-connection check does not, and projecting
    /// a key into either would widen the pod's blast radius past the question.
    #[must_use]
    pub fn signs(&self) -> bool {
        matches!(
            self.operation,
            PreflightOperation::Backup | PreflightOperation::Restore
        )
    }

    /// Whether this check writes the create-only readiness marker: the
    /// destination opted in AND the check requests the `EvidenceWrite` role.
    #[must_use]
    pub fn probes_evidence_write(&self) -> bool {
        self.write_probe && self.roles.contains(&DestinationRole::EvidenceWrite)
    }

    /// The separate principal the create-only marker must be written as, and
    /// the environment that projects it.
    ///
    /// * `Ok(None)`: no marker is written (no probe, no `EvidenceWrite` role, a
    ///   Restore or source-connection check, or an unresolved destination
    ///   reported as such), or the `evidenceWrite` grant is the grant the plan's
    ///   destination credential came from. The plan carries no separate grant.
    /// * `Ok(Some(..))`: the grants differ; the plan names the evidence-write
    ///   grant by reference and the pod is projected its credential beside the
    ///   destination's.
    /// * `Err`: a marker would be written and this pod cannot exercise the
    ///   evidence-write grant (two workload identities, or a grant never
    ///   resolved). `destination.resolved` reports it and no Job is rendered: a
    ///   probe that cannot run as the right principal must not run as another.
    ///
    /// # Errors
    ///
    /// [`destination::DestinationRefusal`], as above.
    pub fn evidence_write_principal(
        &self,
    ) -> Result<Option<(GrantRef, destination::DestinationEnv)>, destination::DestinationRefusal>
    {
        if !matches!(
            self.operation,
            PreflightOperation::Backup | PreflightOperation::DestinationAccess
        ) || !self.probes_evidence_write()
        {
            return Ok(None);
        }
        let Some(Ok(primary)) = self.archive.as_ref() else {
            return Ok(None);
        };
        let grant = match self.evidence_write_grant.as_ref() {
            Some(Ok(g)) => g,
            Some(Err(refusal)) => return Err(refusal.clone()),
            // Fail closed: a marker is about to be written and nobody resolved
            // whose it is.
            None => {
                return Err(destination::DestinationRefusal {
                    code: CheckCode::DestinationRoleNotConfigured,
                    field: "spec.access.evidenceWrite".to_string(),
                    message: format!(
                        "the evidence-write grant of BackupDestination {} was not resolved, so \
                         the create-only marker has no principal to be written as",
                        primary.name
                    ),
                })
            }
        };
        let Some(env) = primary.check_evidence_write_env(grant)? else {
            return Ok(None);
        };
        let reference = match grant {
            destination::ResolvedGrant::SecretKeys { secret, .. } => {
                GrantRef::static_secret(secret.clone())
            }
            destination::ResolvedGrant::WorkloadIdentity {
                service_account_name,
            } => GrantRef::workload_identity(service_account_name.clone()),
            // `check_evidence_write_env` refused both already.
            destination::ResolvedGrant::ControllerIdentity
            | destination::ResolvedGrant::NotConfigured => return Ok(None),
        };
        Ok(Some((reference, env)))
    }

    /// Which principal `destination.evidenceReadable` is answered as, and what
    /// the pod needs for it.
    ///
    /// See [`EvidenceReadPlan`]. [`EvidenceReadPlan::ControllerAnswers`] has the
    /// controller answer the advisory row `unknown` with the reason while every
    /// other row still runs. It covers:
    ///
    /// * a grant the resolver refused (e.g. `ControllerIdentityNotAllowlisted`
    ///   before the policy lists the location): an advisory row's refusal must
    ///   not stop the archive and marker rows of the same check;
    /// * a Backup check's workload identity that cannot share the check pod's
    ///   ServiceAccount: a Backup run never reads evidence in its own pod.
    ///
    /// A `DestinationAccess` check whose evidence-read workload identity cannot
    /// share the pod is still refused: it asked for that principal, and no pod
    /// can be it.
    ///
    /// # Errors
    ///
    /// [`destination::DestinationRefusal`]: `ExecutionContextConflict` (a
    /// `DestinationAccess` check whose evidence-read workload identity cannot
    /// share the pod) or `DestinationRoleNotConfigured` (the role is requested
    /// and its grant was never resolved; fail closed).
    pub fn evidence_read_plan(&self) -> Result<EvidenceReadPlan, destination::DestinationRefusal> {
        if !matches!(
            self.operation,
            PreflightOperation::Backup | PreflightOperation::DestinationAccess
        ) || !self.roles.contains(&DestinationRole::EvidenceRead)
        {
            return Ok(EvidenceReadPlan::NotRequested);
        }
        let Some(Ok(primary)) = self.archive.as_ref() else {
            return Ok(EvidenceReadPlan::NotRequested);
        };
        let grant = match self.evidence_read_grant.as_ref() {
            Some(Ok(g)) => g,
            // The advisory row says why, and nothing else is held back.
            Some(Err(refusal)) => {
                return Ok(EvidenceReadPlan::ControllerAnswers(format!(
                    "the evidence-read grant of BackupDestination {} was refused ({}: {}), so \
                     no principal read the evidence root; the other rows of this check ran",
                    primary.name,
                    refusal.code.as_str(),
                    refusal.message
                )))
            }
            // Fail closed, as for the marker.
            None => {
                return Err(destination::DestinationRefusal {
                    code: CheckCode::DestinationRoleNotConfigured,
                    field: "spec.access.evidenceRead".to_string(),
                    message: format!(
                        "the evidence-read grant of BackupDestination {} was not resolved, so \
                         destination.evidenceReadable has no principal to be answered as",
                        primary.name
                    ),
                })
            }
        };
        let service_account = match grant {
            // No pod holds these, even when it is the destination grant (a
            // `DestinationAccess` check whose only role is `EvidenceRead`):
            // the plan's credential is then the ambient chain, which is
            // reading as nobody the destination names.
            destination::ResolvedGrant::ControllerIdentity => {
                return Ok(EvidenceReadPlan::NotInPod(
                    GrantRef::without_pod_credential(GrantCredentials::ControllerIdentity),
                ))
            }
            destination::ResolvedGrant::NotConfigured => {
                return Ok(EvidenceReadPlan::NotInPod(
                    GrantRef::without_pod_credential(GrantCredentials::NotConfigured),
                ))
            }
            _ if grant == &primary.grant => return Ok(EvidenceReadPlan::SameGrant),
            destination::ResolvedGrant::SecretKeys {
                secret,
                access_key_id_key,
                secret_access_key_key,
                session_token_key,
            } => {
                let vars = logweir_core::check_contract::EVIDENCE_READ_ENV;
                let mut from_secret = vec![
                    crate::job::EnvFromSecret {
                        name: vars.access_key_id.to_string(),
                        secret_name: secret.clone(),
                        optional: false,
                        key: access_key_id_key.clone(),
                    },
                    crate::job::EnvFromSecret {
                        name: vars.secret_access_key.to_string(),
                        secret_name: secret.clone(),
                        optional: false,
                        key: secret_access_key_key.clone(),
                    },
                ];
                if let Some(token) = session_token_key {
                    from_secret.push(crate::job::EnvFromSecret {
                        name: vars.session_token.to_string(),
                        secret_name: secret.clone(),
                        optional: false,
                        key: token.clone(),
                    });
                }
                // The evidenceRead Secret's optional binding, beside it, against
                // this destination's binding.
                from_secret.push(crate::job::EnvFromSecret {
                    name: logweir_core::credential_binding::EVIDENCE_READ_CREDENTIAL_BINDING_ENV
                        .to_string(),
                    secret_name: secret.clone(),
                    optional: true,
                    key: logweir_core::credential_binding::CREDENTIAL_BINDING_KEY.to_string(),
                });
                return Ok(EvidenceReadPlan::Separate(
                    GrantRef::static_secret(secret.clone()),
                    destination::DestinationEnv {
                        literals: vec![(
                            logweir_core::credential_binding::EVIDENCE_READ_CREDENTIAL_BINDING_EXPECTED_ENV
                                .to_string(),
                            primary.credential_binding(),
                        )],
                        from_secret,
                        service_account_name: None,
                    },
                ));
            }
            destination::ResolvedGrant::WorkloadIdentity {
                service_account_name,
            } => service_account_name,
        };
        // One pod, one ServiceAccount: every other principal this pod must
        // run as.
        let others: Vec<String> = [
            match self.connection.as_ref() {
                Some(Ok(c)) => Some(c.execution.service_account_name.clone()),
                _ => None,
            },
            primary.grant.service_account_name().map(str::to_string),
            match self.evidence_write_principal() {
                Ok(Some((_, env))) => env.service_account_name,
                _ => None,
            },
        ]
        .into_iter()
        .flatten()
        .filter(|sa| !sa.is_empty())
        .collect();
        if let Some(other) = others.iter().find(|sa| *sa != service_account) {
            if self.operation == PreflightOperation::Backup {
                return Ok(EvidenceReadPlan::ControllerAnswers(format!(
                    "the evidence-read grant of BackupDestination {} runs as ServiceAccount \
                     {service_account} and this check pod runs as {other}; one pod has one \
                     ServiceAccount, so this check did not read the evidence root as any \
                     principal (a Backup run never reads evidence in its own pod, so the \
                     verdict is not held back)",
                    primary.name
                )));
            }
            return Err(destination::DestinationRefusal {
                code: CheckCode::ExecutionContextConflict,
                field: "spec.access.evidenceRead".to_string(),
                message: format!(
                    "BackupDestination {} reads evidence as ServiceAccount {service_account} \
                     and this check pod must run as {other}; one pod has one ServiceAccount, so \
                     one check cannot exercise both",
                    primary.name
                ),
            });
        }
        Ok(EvidenceReadPlan::Separate(
            GrantRef::workload_identity(service_account.clone()),
            destination::DestinationEnv {
                literals: Vec::new(),
                from_secret: Vec::new(),
                service_account_name: Some(service_account.clone()),
            },
        ))
    }

    /// The roles the rendered plan carries: [`Inputs::roles`], minus an
    /// `EvidenceRead` the controller answers itself
    /// ([`EvidenceReadPlan::ControllerAnswers`]).
    #[must_use]
    pub fn plan_roles(&self) -> Vec<DestinationRole> {
        let dropped = matches!(
            self.evidence_read_plan(),
            Ok(EvidenceReadPlan::ControllerAnswers(_))
        );
        self.roles
            .iter()
            .copied()
            .filter(|r| !(dropped && *r == DestinationRole::EvidenceRead))
            .collect()
    }

    /// `destination.resolved`, over both destinations (see the call site).
    ///
    /// `None` when this operation names no destination at all.
    #[must_use]
    pub fn destination_verdict_row(&self, now: DateTime<Utc>) -> Option<CheckOutcome> {
        if let Some(legacy) = self.legacy_source.as_ref() {
            return Some(legacy_source_row(legacy, self.archive.as_ref(), now));
        }
        let pairs: [(Option<&String>, Option<&DestinationResolution>); 2] = [
            (self.archive_name.as_ref(), self.archive.as_ref()),
            (self.evidence_name.as_ref(), self.evidence.as_ref()),
        ];
        let mut first_ok: Option<CheckOutcome> = None;
        for (name, resolved) in pairs {
            let (Some(name), Some(resolved)) = (name, resolved) else {
                continue;
            };
            let row = destination_row(self.operation, resolved, name, now);
            if resolved.is_err() {
                return Some(row);
            }
            first_ok.get_or_insert(row);
        }
        // The third principal on the same row: a destination whose
        // evidence-write grant this pod cannot exercise is not ready to be
        // checked, and `DestinationValid` would hide why the marker row never
        // ran.
        if let (Some(name), Err(refusal)) =
            (self.archive_name.as_ref(), self.evidence_write_principal())
        {
            return Some(destination_row(self.operation, &Err(refusal), name, now));
        }
        if let (Some(name), Err(refusal)) = (self.archive_name.as_ref(), self.evidence_read_plan())
        {
            return Some(destination_row(self.operation, &Err(refusal), name, now));
        }
        first_ok
    }

    /// The controller-authority and pod-authority rows for this pass.
    #[must_use]
    pub fn controller_outcomes(
        &self,
        facts: &RelayFacts,
        waiting: Option<&check::Waiting>,
        relayed: bool,
        image_id: Option<&str>,
        now: DateTime<Utc>,
    ) -> (Vec<CheckOutcome>, bool) {
        let op = self.operation;
        let mut out: Vec<CheckOutcome> = Vec::new();
        if let Some(load) = self.policy.as_ref() {
            out.push(policy_row(op, load, now));
        }
        out.push(egress_row(
            op,
            &self.broker_ports(),
            &self.store_ports(),
            now,
        ));
        // The connection rows belong to the operations that dial, the signer
        // row to the ones that sign: `SourceConnection` dials and does not
        // sign, and a blocking `signer.rostered` row about a key its pod was
        // never given would be false.
        if op != PreflightOperation::DestinationAccess {
            out.push(connection_row(
                op,
                self.connection.as_ref(),
                self.cluster_name.as_deref().unwrap_or_default(),
                now,
            ));
            // A switchover review holds the target to the cluster id the
            // restore's signed scorecard names (`switchover.target`, the
            // Job's), the stricter question; it has no row of its own here.
            if op != PreflightOperation::Switchover {
                out.push(cluster_identity_under_trust(
                    op,
                    facts.cluster_id.as_deref(),
                    self.cluster_recorded_id.as_deref(),
                    &self.roster,
                    self.source_cluster_id.as_deref(),
                    self.plan.as_ref().is_some_and(PlanFacts::scratch),
                    now,
                ));
            }
        }
        if op == PreflightOperation::Switchover {
            out.push(switchover_restore_row(self.switchover.as_ref(), now));
        }
        if self.signs() {
            out.push(signer_rostered_row(
                op,
                &self.roster,
                facts.signer_key_id.as_deref(),
                now,
            ));
        }
        // One `destination.resolved` id, two objects: a restore names a source
        // and an evidence destination. The row carries the first refusal,
        // scoped to the object that produced it, and falls back to the archive
        // when both resolve, so the broken one is never hidden behind a green
        // row about the other.
        if let Some(row) = self.destination_verdict_row(now) {
            out.push(row);
        }
        // An advisory evidence-read row the pod cannot answer is the
        // controller's, with its reason: never absent, never blocking.
        if let (Ok(EvidenceReadPlan::ControllerAnswers(reason)), Some(Ok(d))) =
            (self.evidence_read_plan(), self.archive.as_ref())
        {
            out.push(evidence_read_row(op, &d.name, Some(&d.uid), &reason, now));
        }
        if op == PreflightOperation::Restore {
            if let Some(plan) = self.plan.as_ref() {
                out.push(plan_parse_row(plan, now));
                out.push(plan_names_row(plan, now));
                out.push(plan_bindings_row(plan, &self.bindings, now));
                if let (Some(legacy), Ok(spec)) =
                    (self.legacy_source.as_ref(), plan.parsed.as_ref())
                {
                    out.extend(legacy_evidence_row(
                        legacy,
                        &spec.evidence,
                        self.legacy_evidence_handle.as_deref(),
                        now,
                    ));
                }
            }
            // One `recoveryPoint.state` row: the catalog point's when the
            // request names one (it cannot name both), the Backup's otherwise.
            if let Some(row) = catalog_point_row(&self.catalog_point, now)
                .or_else(|| recovery_point_row(&self.recovery_point, now))
            {
                out.push(row);
            }
            out.extend(approval_rows(
                &self.approval,
                self.plan
                    .as_ref()
                    .map_or("", |p| p.recomputed_hash.as_str()),
                self.restore_name.as_deref(),
                self.deadline,
                now,
            ));
        }
        let (pod, blocked) = pod_outcomes(op, waiting, &self.projections(), relayed, image_id, now);
        out.extend(pod);
        (out, blocked)
    }

    fn broker_ports(&self) -> Vec<String> {
        match self.connection.as_ref() {
            Some(Ok(c)) => {
                let mut ports: Vec<String> = c
                    .bootstrap_servers
                    .iter()
                    .filter_map(|b| b.rsplit_once(':').map(|(_, p)| p.to_string()))
                    .collect();
                ports.sort();
                ports.dedup();
                ports
            }
            _ => Vec::new(),
        }
    }

    fn store_ports(&self) -> Vec<String> {
        let mut ports: Vec<String> = [self.archive.as_ref(), self.evidence.as_ref()]
            .into_iter()
            .flatten()
            .filter_map(|d| d.as_ref().ok())
            .filter_map(|d| d.location.endpoint.as_deref())
            .filter_map(|e| {
                let host = e.rsplit("://").next().unwrap_or(e);
                host.rsplit_once(':').map(|(_, p)| p.to_string())
            })
            .collect();
        ports.sort();
        ports.dedup();
        ports
    }

    /// The rows whose refusal makes a check plan unrenderable.
    ///
    /// Every other refusal still runs the Job: a preflight reports every
    /// problem at once, and a restore awaiting its approver is exactly when a
    /// check of the archive is most wanted.
    #[must_use]
    pub fn plan_blockers<'a>(&self, outcomes: &'a [CheckOutcome]) -> Vec<&'a CheckOutcome> {
        outcomes
            .iter()
            .filter(|c| {
                c.state == CheckState::NotReady
                    && matches!(
                        c.id,
                        CheckId::ConnectionResolved
                            | CheckId::TargetResolved
                            | CheckId::DestinationResolved
                            | CheckId::PlanParse
                            | CheckId::SwitchoverRestore
                    )
            })
            .collect()
    }
}

/// Where the archive destination's CA bundle is mounted inside the check pod.
pub const ARCHIVE_CA_PATH: &str = "/check/archive-ca.pem";
/// Where the evidence destination's CA bundle is mounted.
pub const EVIDENCE_CA_PATH: &str = "/check/evidence-ca.pem";
/// Where the verbatim restore plan is mounted.
pub const RESTORE_PLAN_PATH: &str = "/check/plan.yaml";

/// What resolving one `BackupDestination` for one role produced: the resolved
/// form, or the refusal that is itself a finding.
pub type DestinationResolution = Result<ResolvedDestination, destination::DestinationRefusal>;

/// The plan documents and the Job this check needs.
#[derive(Clone, Debug)]
pub struct JobShape {
    /// What goes into the immutable plan `ConfigMap`.
    pub documents: check_plan::PlanDocuments,
    /// The Job.
    pub spec: check_job::CheckJobSpec,
    /// The plan document, as a value, so the caller derives the rows the runner
    /// will emit ([`job_rows`]) from the request it actually rendered.
    pub plan: CheckPlan,
}

fn connection_plan(c: &ResolvedConnection) -> ConnectionPlan {
    let side = c.execution.side;
    ConnectionPlan {
        bootstrap_servers: c.bootstrap_servers.clone(),
        auth_mode: c.auth.mode_str().to_string(),
        username: c.auth.username().map(str::to_string),
        // The name, never the value: the password reaches the pod as a
        // `secretKeyRef`, and the controller never holds the bytes.
        password_env: c.password.as_ref().map(|_| side.password_env().to_string()),
        tls: Some(c.tls()),
        // An opaque in-pod path, never rendered bytes: a `KafkaCluster` may
        // name its CA in a Secret, which this controller cannot read. The
        // check pod gets the mount an execution pod gets, and the runner passes
        // the path to librdkafka without opening it.
        ca_file: c.tls_ca.as_ref().map(|_| side.ca_file_path()),
        // The `mtls` pair's in-pod paths, where the resolver's own projection
        // mounts them; never key material.
        client_cert_file: c
            .client_certificate
            .as_ref()
            .map(|_| side.client_cert_file_path()),
        client_key_file: c
            .client_certificate
            .as_ref()
            .map(|_| side.client_key_file_path()),
        principal: c.principal.clone(),
    }
}

fn destination_plan(
    d: &ResolvedDestination,
    ca_file: Option<&str>,
    grant_bindings: Vec<GrantBindingRef>,
) -> DestinationPlan {
    DestinationPlan {
        name: d.name.clone(),
        uid: d.uid.clone(),
        location: d.location.clone(),
        location_digest: d.location_digest.clone(),
        // The destination side is the one place a CA is rendered as bytes:
        // `StoreOptions::with_root_certificate` takes PEM, not a path.
        ca_file: d.ca_pem.as_ref().and(ca_file).map(str::to_string),
        credentials: match &d.grant {
            destination::ResolvedGrant::SecretKeys { .. } => CredentialMode::Static,
            destination::ResolvedGrant::WorkloadIdentity { .. } => CredentialMode::WorkloadIdentity,
            destination::ResolvedGrant::ControllerIdentity
            | destination::ResolvedGrant::NotConfigured => CredentialMode::Ambient,
        },
        grant_bindings,
    }
}

/// A Restore's evidence destination lists its `evidenceWrite` grant (resolved
/// for that role, fall-back included) when it is a Secret.
fn evidence_grant_bindings(e: &ResolvedDestination) -> Vec<GrantBindingRef> {
    grant_binding_ref(DestinationRole::EvidenceWrite, &e.grant)
        .into_iter()
        .collect()
}

/// The binding-only pair each listed grant contributes to the check pod: the
/// grant's Secret's `logweir-binding` as an optional `secretKeyRef` (an
/// unbound or missing Secret still lets the pod start, and the runner names
/// it) and `expected`, the destination's binding, as a literal. No credential
/// variable rides with it: a grant this check does not exercise is compared
/// and never used.
fn push_grant_binding_env(
    bindings: &[GrantBindingRef],
    expected: &str,
    env_literal: &mut Vec<(String, String)>,
    env_from_secret: &mut Vec<crate::job::EnvFromSecret>,
) {
    for grant in bindings {
        let (projected, expected_env) =
            logweir_core::credential_binding::grant_binding_env(grant.role);
        env_literal.push((expected_env.to_string(), expected.to_string()));
        env_from_secret.push(crate::job::EnvFromSecret {
            name: projected.to_string(),
            secret_name: grant.secret_name.clone(),
            optional: true,
            key: logweir_core::credential_binding::CREDENTIAL_BINDING_KEY.to_string(),
        });
    }
}

/// Render one check's plan documents and Job. Pure.
///
/// # Errors
///
/// A sentence naming what could not be rendered: a programming error or a
/// refusal the caller already reported as a row. The caller turns it into
/// `phase: Failed`, "the check could not produce a result".
pub fn build_job_shape(
    inputs: &Inputs,
    owner: &crate::job::RunnerOwner,
    image: &crate::job::RunnerImage,
) -> Result<JobShape, String> {
    let operation = inputs.operation;
    let kind = plan_kind(operation);
    let skip: Vec<CheckId> = inputs.skip.iter().copied().collect();

    let connection = match inputs.connection.as_ref() {
        Some(Ok(c)) => Some(c),
        _ => None,
    };
    let archive = match inputs.archive.as_ref() {
        Some(Ok(d)) => Some(d),
        _ => None,
    };
    let evidence = match inputs.evidence.as_ref() {
        Some(Ok(d)) => Some(d),
        _ => None,
    };
    // The marker's principal. A refusal here was already published as
    // `destination.resolved`, which `plan_blockers` holds the Job back on; a
    // caller that skipped that must not render a plan that writes as someone
    // else.
    let evidence_write = inputs
        .evidence_write_principal()
        .map_err(|refusal| refusal.message)?;
    let evidence_write_grant = evidence_write.as_ref().map(|(g, _)| g.clone());
    // The evidence read's principal, by the same rule.
    let evidence_read = inputs
        .evidence_read_plan()
        .map_err(|refusal| refusal.message)?;
    let evidence_read_grant = match &evidence_read {
        EvidenceReadPlan::Separate(g, _) | EvidenceReadPlan::NotInPod(g) => Some(g.clone()),
        EvidenceReadPlan::NotRequested
        | EvidenceReadPlan::SameGrant
        | EvidenceReadPlan::ControllerAnswers(_) => None,
    };
    let plan_roles = inputs.plan_roles();

    let request = match operation {
        PreflightOperation::Backup => {
            let c = connection.ok_or("the source connection did not resolve")?;
            let d = archive.ok_or("the destination did not resolve")?;
            CheckRequest::OperationReadiness(Box::new(OperationReadinessRequest {
                operation: CheckOperation::Backup,
                connection: connection_plan(c),
                destination: Some(destination_plan(
                    d,
                    Some(ARCHIVE_CA_PATH),
                    inputs.grant_bindings.clone(),
                )),
                roles: plan_roles.clone(),
                topics: inputs.topics.clone(),
                signer_path: Some(crate::backup_execution::SIGNING_KEY_PATH.to_string()),
                // The create-only marker is the only key a check may ever
                // write, and it is opt-in on the destination
                // (`spec.readiness.writeProbe`), never assumed.
                write_probe: inputs.write_probe,
                evidence_write: evidence_write_grant.clone(),
                evidence_read: evidence_read_grant.clone(),
                skip_checks: skip.clone(),
                capability_checks: capability_checks(CheckOperation::Backup, &skip),
            }))
        }
        PreflightOperation::SourceConnection => {
            let c = connection.ok_or("the source connection did not resolve")?;
            CheckRequest::SourceConnection(logweir_core::check_contract::SourceConnectionRequest {
                connection: connection_plan(c),
                // FX-46: what the endpoint could be used for, so the test
                // can say "usable as a source, not as a target". The request
                // carries no skip list.
                capability_checks: capability_checks(CheckOperation::SourceConnection, &[]),
            })
        }
        PreflightOperation::DestinationAccess => {
            let d = archive.ok_or("the destination did not resolve")?;
            CheckRequest::DestinationAccess(
                logweir_core::check_contract::DestinationAccessRequest {
                    destination: destination_plan(
                        d,
                        Some(ARCHIVE_CA_PATH),
                        inputs.grant_bindings.clone(),
                    ),
                    roles: plan_roles.clone(),
                    write_probe: inputs.write_probe,
                    evidence_write: evidence_write_grant.clone(),
                    evidence_read: evidence_read_grant.clone(),
                },
            )
        }
        PreflightOperation::Switchover => {
            let c = connection.ok_or("the target connection did not resolve")?;
            let d = archive.ok_or("the evidence destination did not resolve")?;
            let f = inputs
                .switchover
                .as_ref()
                .ok_or("there is no switchover request")?;
            let r = f.restore.as_ref().ok_or("the Restore does not exist")?;
            let keyring = f.keyring.clone()?;
            CheckRequest::SwitchoverReadiness(Box::new(
                logweir_core::check_contract::SwitchoverReadinessRequest {
                    restore_name: r.name.clone(),
                    restore_uid: r.uid.clone(),
                    restore_plan_sha256: r.plan_hash.clone(),
                    scorecard_key: r
                        .scorecard_key
                        .clone()
                        .ok_or("the Restore records no scorecard key")?,
                    target: connection_plan(c),
                    evidence: destination_plan(d, Some(ARCHIVE_CA_PATH), Vec::new()),
                    keyring,
                    groups: f.groups.clone(),
                },
            ))
        }
        PreflightOperation::Restore => {
            let c = connection.ok_or("the target connection did not resolve")?;
            let d = archive.ok_or("the source destination did not resolve")?;
            let plan = inputs.plan.as_ref().ok_or("there is no restore plan")?;
            CheckRequest::RestorePreflight(Box::new(RestorePreflightRequest {
                plan_file: RESTORE_PLAN_PATH.to_string(),
                plan_sha256: plan.recomputed_hash.clone(),
                target: connection_plan(c),
                source_destination: destination_plan(
                    d,
                    Some(ARCHIVE_CA_PATH),
                    inputs.grant_bindings.clone(),
                ),
                evidence_destination: evidence.map(|e| {
                    destination_plan(e, Some(EVIDENCE_CA_PATH), evidence_grant_bindings(e))
                }),
                backup_id: inputs.backup_id.clone(),
                // The manifest key is the archive's own convention; the runner
                // joins it to the destination's prefix.
                manifest_key: format!("{}/manifest.json", inputs.backup_id),
                // Empty means "every row this kind owns"; naming them would be a
                // second catalogue to drift from the runner's.
                checks: Vec::new(),
                skip_checks: skip.clone(),
                capability_checks: capability_checks(CheckOperation::Restore, &skip),
            }))
        }
    };

    let plan = CheckPlan {
        contract: CHECK_PLAN_CONTRACT.to_string(),
        contract_version: CHECK_CONTRACT_VERSION,
        subject_uid: owner.uid.clone(),
        timeout_seconds: u32::try_from(inputs.timeout_seconds)
            .map_err(|_| "the timeout does not fit in the plan's budget".to_string())?,
        policy_digest: Some(inputs.policy_digest.clone()),
        request,
    };
    plan.validate().map_err(|e| e.to_string())?;
    let check_plan_bytes = serde_json::to_vec(&plan).map_err(|e| e.to_string())?;

    let documents = check_plan::PlanDocuments {
        check_plan: check_plan_bytes,
        source_ca: None,
        target_ca: None,
        archive_ca: archive.and_then(|d| d.ca_pem.clone()),
        evidence_ca: evidence.and_then(|d| d.ca_pem.clone()),
        restore_plan: inputs.plan.as_ref().map(|p| p.bytes.clone()),
    };

    // The pod's shape: the execution pod's, with the check's plan.
    let projection = connection.map(ResolvedConnection::project);
    let mut secret_mounts = Vec::new();
    if inputs.signs() {
        secret_mounts.push(crate::job::SecretMount {
            volume: super::backup::SIGNING_VOLUME.to_string(),
            secret_name: super::backup::SIGNING_KEY_SECRET.to_string(),
            mount_path: super::backup::SIGNING_MOUNT_PATH.to_string(),
            items: vec![(
                super::backup::SIGNING_KEY_SECRET_KEY.to_string(),
                super::backup::SIGNING_KEY_FILE.to_string(),
            )],
        });
    }
    let mut config_map_mounts = Vec::new();
    let mut env_from_secret = Vec::new();
    let mut env_literal = Vec::new();
    if let Some(p) = projection {
        secret_mounts.extend(p.secret_mounts);
        config_map_mounts.extend(p.config_map_mounts);
        env_from_secret.extend(p.env_from_secret);
        env_literal.extend(p.env_literal);
    }
    let mut service_account: Option<String> = connection
        .map(|c| c.execution.service_account_name.clone())
        .filter(|s| !s.is_empty());
    // One binding-only pair per grant the plan lists, and only those: the
    // runner compares the plan's list, and an unlisted pair would project a
    // Secret key for nothing.
    for (_, d) in plan.request.bound_destinations() {
        let expected = [archive, evidence]
            .into_iter()
            .flatten()
            .find(|r| r.uid == d.uid)
            .map(ResolvedDestination::credential_binding)
            .ok_or("a listed grant names a destination this check did not resolve")?;
        push_grant_binding_env(
            &d.grant_bindings,
            &expected,
            &mut env_literal,
            &mut env_from_secret,
        );
    }
    if let Some(d) = archive {
        let env = d.job_env();
        env_literal.extend(env.literals);
        env_from_secret.extend(env.from_secret);
        if let Some(sa) = env.service_account_name {
            // A destination demanding its own ServiceAccount and a connection
            // demanding another is a conflict, not a choice: either would run
            // the check as a principal the other half was not validated
            // against. The row reports `ExecutionContextConflict`.
            if service_account.as_deref().is_some_and(|s| s != sa) {
                return Err(format!(
                    "the connection runs as ServiceAccount `{}` and the destination's grant \
                     demands `{sa}`; a check cannot run as both",
                    service_account.unwrap_or_default()
                ));
            }
            service_account = Some(sa);
        }
        if let Some(e) = evidence {
            let env = e
                .evidence_env(d)
                .map_err(|refusal| refusal.message.clone())?;
            env_literal.extend(env.literals);
            env_from_secret.extend(env.from_secret);
        }
        // The evidence-write grant, when it is a second principal: its keys
        // under `LOGWEIR_EVIDENCE_AWS_*` (never `AWS_*`, which are the
        // destination grant's), or its ServiceAccount.
        if let Some((_, env)) = evidence_write {
            env_literal.extend(env.literals);
            env_from_secret.extend(env.from_secret);
            if let Some(sa) = env.service_account_name {
                if service_account.as_deref().is_some_and(|s| s != sa) {
                    return Err(format!(
                        "the check runs as ServiceAccount `{}` and the destination's \
                         evidence-write grant demands `{sa}`; a check cannot run as both",
                        service_account.unwrap_or_default()
                    ));
                }
                service_account = Some(sa);
            }
        }
        // The evidence-read grant, when it is a second (or third) principal:
        // its keys under `LOGWEIR_EVIDENCE_READ_AWS_*`, or its ServiceAccount.
        if let EvidenceReadPlan::Separate(_, env) = evidence_read {
            env_literal.extend(env.literals);
            env_from_secret.extend(env.from_secret);
            if let Some(sa) = env.service_account_name {
                if service_account.as_deref().is_some_and(|s| s != sa) {
                    return Err(format!(
                        "the check runs as ServiceAccount `{}` and the destination's \
                         evidence-read grant demands `{sa}`; a check cannot run as both",
                        service_account.unwrap_or_default()
                    ));
                }
                service_account = Some(sa);
            }
        }
    }
    env_literal.sort();
    env_literal.dedup();
    secret_mounts.sort_by(|a, b| a.volume.cmp(&b.volume));
    config_map_mounts.sort_by(|a, b| a.volume.cmp(&b.volume));
    env_from_secret.sort_by(|a, b| a.name.cmp(&b.name));

    let job_name = check_job::check_job_name(kind, &owner.uid);
    let spec = check_job::CheckJobSpec {
        kind,
        namespace: inputs.namespace.clone(),
        owner: owner.clone(),
        connection_uid: inputs.cluster_uid.clone(),
        plan_config_map: check_plan::plan_config_map_name(&job_name),
        plan_sha256: documents.check_plan_sha256(),
        subject_uid: owner.uid.clone(),
        timeout_seconds: inputs.timeout_seconds,
        service_account_name: service_account
            .unwrap_or_else(|| super::backup::RUNNER_SERVICE_ACCOUNT.to_string()),
        secret_mounts,
        config_map_mounts,
        env_from_secret,
        env_literal,
        image: image.image.clone(),
        image_pull_policy: image.image_pull_policy.clone(),
        attempt: None,
    };
    Ok(JobShape {
        documents,
        spec,
        plan,
    })
}

fn roster_facts(load: &super::approval::RosterLoad) -> RosterFacts {
    match load {
        super::approval::RosterLoad::NotFound => RosterFacts::default(),
        super::approval::RosterLoad::Found(r) => RosterFacts {
            found: true,
            uid: r.uid().unwrap_or_default(),
            generation: r.metadata.generation.unwrap_or(0),
            signing_keys: r
                .spec
                .signing_keys
                .iter()
                .map(|k| (k.key_id.clone(), k.not_after))
                .collect(),
            approver_keys: r
                .spec
                .approver_keys
                .iter()
                .map(|k| (k.key_id.clone(), k.not_after))
                .collect(),
            allowed_cluster_ids: r.spec.allowed_cluster_ids.clone(),
            governing: None,
            compromised_keys: Vec::new(),
        },
    }
}

/// Read what [`catalog_point_facts`] needs: the catalog, the page `ConfigMap`s
/// its status names (by those names and no others), and the namespace's
/// `Backup`s in bounded pages.
async fn read_catalog_point(
    client: &kube::Client,
    namespace: &str,
    reference: &crate::crds::preflight::CatalogPointRef,
    plan: Option<&PlanFacts>,
    now: DateTime<Utc>,
) -> Result<CatalogPointFacts, ReconcileError> {
    use crate::crds::recovery_catalog::RecoveryCatalog;
    use k8s_openapi::api::core::v1::ConfigMap;
    let catalogs: Api<RecoveryCatalog> = Api::namespaced(client.clone(), namespace);
    let catalog = catalogs
        .get_opt(&reference.catalog_ref.name)
        .await
        .map_err(ReconcileError::Api)?;
    let mut pages: Vec<(String, Option<ConfigMap>)> = Vec::new();
    if let Some(c) = catalog.as_ref() {
        let maps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
        for page in c
            .status
            .as_ref()
            .and_then(|s| s.pages.as_ref())
            .into_iter()
            .flatten()
            .take(CATALOG_POINT_VIEW_PAGES)
        {
            let map = maps
                .get_opt(&page.config_map_name)
                .await
                .map_err(ReconcileError::Api)?;
            pages.push((page.config_map_name.clone(), map));
        }
    }
    // The Backup verdicts, read leniently, as `/points` and retention join
    // them (`catalog_view::BackupVerdictFacts`): a `Backup` this build cannot
    // type refuses at most its own point, and a failed list makes the row
    // `unknown` rather than failing the check.
    let resource = kube::api::ApiResource::erase::<Backup>(&());
    let api: Api<kube::api::DynamicObject> =
        Api::namespaced_with(client.clone(), namespace, &resource);
    let mut facts: Vec<crate::catalog_view::BackupVerdictFacts> = Vec::new();
    let mut complete = false;
    let mut token: Option<String> = None;
    for _ in 0..CATALOG_POINT_BACKUP_PAGES {
        let mut params = ListParams::default().limit(CATALOG_POINT_BACKUP_PAGE);
        if let Some(t) = token.take() {
            params = params.continue_token(&t);
        }
        let list = match api.list(&params).await {
            Ok(list) => list,
            Err(error) => {
                warn!(namespace = %namespace, error = %error,
                    "the Backup verdicts could not be listed for a catalog point check");
                break;
            }
        };
        token = list.metadata.continue_.clone().filter(|t| !t.is_empty());
        facts.extend(
            list.items
                .iter()
                .map(|o| crate::catalog_view::BackupVerdictFacts::from_json(&o.data)),
        );
        if token.is_none() {
            complete = true;
            break;
        }
    }
    let refusals = crate::catalog_view::ControllerRefusals::from_facts(facts);
    // The current trust, as the Approval controller resolves it for this
    // namespace. A failed read, a contested namespace and an unconfigured one
    // are each an unknown row, never a pass.
    let resolved = crate::trust::resolve(client, namespace).await;
    let unresolved: String;
    let trust = match &resolved {
        Ok(crate::trust::Resolution::Trust(trust)) => Ok(trust.as_ref()),
        Ok(crate::trust::Resolution::Conflict { policies, .. }) => {
            unresolved = format!(
                "the namespace's trust is contested by {} TrustPolicies ({}), so the catalog \
                 row's signer cannot be judged against it",
                policies.len(),
                policies.join(", ")
            );
            Err(unresolved.as_str())
        }
        Ok(crate::trust::Resolution::Unconfigured) => Err(
            "no TrustPolicy governs this namespace and there is no TrustRoster, so the catalog \
             row's signer cannot be judged",
        ),
        Err(error) => {
            warn!(namespace = %namespace, error = %error,
                "the trust could not be resolved for a catalog point check");
            unresolved = format!("the namespace's trust could not be read: {error}");
            Err(unresolved.as_str())
        }
    };
    Ok(catalog_point_facts(&CatalogPointRead {
        catalog_name: &reference.catalog_ref.name,
        point_id: &reference.point_id,
        catalog: catalog.as_ref(),
        pages: &pages,
        refusals: &refusals,
        backups_complete: complete,
        plan,
        trust,
        now,
    }))
}

fn referent(
    kind: &str,
    namespace: &str,
    name: &str,
    uid: &str,
    generation: Option<i64>,
) -> Referent {
    Referent {
        kind: kind.to_string(),
        namespace: namespace.to_string(),
        name: name.to_string(),
        uid: uid.to_string(),
        generation,
    }
}

/// The grant a `DestinationAccess` check's destination credential is resolved
/// for: the one credential its plan's `DestinationPlan` names.
///
/// `ArchiveRead` first whenever it is requested: `archiveWrite` is never
/// probed by a check, and resolving the credential for it would answer
/// `destination.archiveListable` as the archive-write principal. Then the
/// declaration order (`ArchiveWrite`, `EvidenceWrite`, `EvidenceRead`);
/// `evidenceWrite` never needs to be primary, because the marker is written as
/// its own principal ([`Inputs::evidence_write_principal`]). Empty roles cannot
/// reach here (CEL `minItems: 1`).
#[must_use]
pub fn access_primary_role(roles: &[DestinationRole]) -> DestinationRole {
    const ORDER: [DestinationRole; 4] = [
        DestinationRole::ArchiveRead,
        DestinationRole::ArchiveWrite,
        DestinationRole::EvidenceWrite,
        DestinationRole::EvidenceRead,
    ];
    ORDER
        .into_iter()
        .find(|r| roles.contains(r))
        .unwrap_or(DestinationRole::ArchiveRead)
}

/// How `destination.evidenceReadable` is answered — [`Inputs::evidence_read_plan`].
#[derive(Clone, Debug)]
pub enum EvidenceReadPlan {
    /// The check does not carry the `EvidenceRead` role.
    NotRequested,
    /// The `evidenceRead` grant is the plan's destination grant: nothing second
    /// is projected and the plan names nothing.
    SameGrant,
    /// A different Secret or ServiceAccount: named in the plan, projected into
    /// the pod.
    Separate(GrantRef, destination::DestinationEnv),
    /// `ControllerIdentity` or not configured: named in the plan so the runner
    /// answers `unknown` and reads as nobody.
    NotInPod(GrantRef),
    /// The grant was refused by the resolver, or (Backup only) cannot share
    /// the pod's ServiceAccount: the role is left out of the rendered plan and
    /// the controller answers the advisory row `unknown` /
    /// `EvidenceReadNotConfigured` with this reason ([`evidence_read_row`]).
    ControllerAnswers(String),
}

/// `destination.evidenceReadable`, answered by the controller for
/// [`EvidenceReadPlan::ControllerAnswers`]. Advisory `unknown`, so the
/// aggregate is untouched.
#[must_use]
pub fn evidence_read_row(
    operation: PreflightOperation,
    destination_name: &str,
    destination_uid: Option<&str>,
    reason: &str,
    now: DateTime<Utc>,
) -> CheckOutcome {
    outcome(
        operation,
        CheckId::DestinationEvidenceReadable,
        CheckState::Unknown,
        CheckCode::EvidenceReadNotConfigured,
        now,
    )
    .with_scope(CheckScope {
        kind: "BackupDestination".to_string(),
        name: destination_name.to_string(),
        uid: destination_uid.map(str::to_string),
    })
    .with_message(reason)
    .with_remedy(
        "Fix the evidence-read grant the message names (the installation policy's \
         ControllerIdentity allowlist, or a ServiceAccount the check pod can run as); the other \
         rows of this check are not affected by it.",
    )
    .with_fact("grant", "evidenceRead")
}

/// What a `BackupDestination`'s spec says that its resolved form does not
/// carry, for one check.
#[derive(Clone, Debug, Default)]
pub struct DestinationSpecFacts {
    /// `spec.readiness.writeProbe: CreateOnlyMarker`.
    pub write_probe: bool,
    /// Whether a Backup readiness check should add the `EvidenceRead` role.
    pub request_evidence_read: bool,
    /// The object's own `evidenceWrite` grant, when the check requests the
    /// `EvidenceWrite` role — see [`Inputs::evidence_write_grant`].
    pub evidence_write_grant:
        Option<Result<destination::ResolvedGrant, destination::DestinationRefusal>>,
    /// The object's own `evidenceRead` grant, when the check requests (or a
    /// Backup check adds) the `EvidenceRead` role — see
    /// [`Inputs::evidence_read_grant`].
    pub evidence_read_grant:
        Option<Result<destination::ResolvedGrant, destination::DestinationRefusal>>,
    /// The destination's `SecretKeys` grants whose binding the check compares
    /// ([`grant_bindings_of`]).
    pub grant_bindings: Vec<GrantBindingRef>,
}

/// The `SecretKeys` grants of `object` whose `logweir-binding` a check for
/// `operation` compares (`destination.credentialBound`). Pure.
///
/// * **`DestinationAccess`: every grant the destination declares**, whichever
///   roles the check exercises: "test access" is a question about the
///   destination, and `ready` for a destination whose `archiveWrite` Secret
///   was written for another one would precede a refused backup. An
///   undeclared role falls back to `archiveWrite`, which is listed already.
/// * **`Backup`: `archiveWrite` and `evidenceWrite`** (fall-back included),
///   each compared the way the backup Job's runner compares it
///   (`STORE_BINDING_PAIRS`).
/// * **`Restore`: `archiveRead`** of the source destination; the evidence
///   destination's `evidenceWrite` is listed by [`build_job_shape`] from its
///   own resolution.
/// * **`SourceConnection`: none**; it names no destination.
///
/// A grant that is not `SecretKeys` carries no binding and is not listed, nor
/// is a grant the resolver refuses (`destination.resolved` reports it).
#[must_use]
pub fn grant_bindings_of(
    operation: PreflightOperation,
    object: &crate::crds::backup_destination::BackupDestination,
    policy: &check_policy::Policy,
) -> Vec<GrantBindingRef> {
    let roles: Vec<DestinationRole> = match operation {
        PreflightOperation::DestinationAccess => DestinationRole::ALL
            .into_iter()
            .filter(|role| destination::declares(object, *role))
            .collect(),
        PreflightOperation::Backup => {
            vec![
                DestinationRole::ArchiveWrite,
                DestinationRole::EvidenceWrite,
            ]
        }
        PreflightOperation::Restore => vec![DestinationRole::ArchiveRead],
        // A switchover review compares no binding: its plan carries none
        // (`validate_grant_bindings`), and the evidence grant it reads with
        // refuses a mis-bound Secret before any read.
        PreflightOperation::SourceConnection | PreflightOperation::Switchover => Vec::new(),
    };
    roles
        .into_iter()
        .filter_map(|role| {
            let grant = destination::resolve(object, role, policy).ok()?.grant;
            grant_binding_ref(role, &grant)
        })
        .collect()
}

/// One grant as a [`GrantBindingRef`], when it is a Secret.
#[must_use]
pub fn grant_binding_ref(
    role: DestinationRole,
    grant: &destination::ResolvedGrant,
) -> Option<GrantBindingRef> {
    match grant {
        destination::ResolvedGrant::SecretKeys { secret, .. } => Some(GrantBindingRef {
            role,
            secret_name: secret.clone(),
        }),
        destination::ResolvedGrant::WorkloadIdentity { .. }
        | destination::ResolvedGrant::ControllerIdentity
        | destination::ResolvedGrant::NotConfigured => None,
    }
}

/// [`DestinationSpecFacts`] for one check. Pure.
///
/// * **`write_probe` is read for every operation**, `DestinationAccess`
///   included, so a check requesting `EvidenceWrite` on a destination that
///   opted in to the marker actually probes it. The opt-in stays the
///   destination owner's: `Disabled` (the default) writes nothing, and the row
///   is then execution-only.
/// * **`EvidenceRead` is added only to a Backup check, and only when the
///   destination configures that grant.** A `DestinationAccess` request names
///   its own roles.
/// * **The `evidenceWrite` grant is resolved for a Backup or
///   `DestinationAccess` check that requests `EvidenceWrite`**, so the marker
///   is written as that principal. A Restore preflight's evidence destination
///   is a second object, resolved for `EvidenceWrite` already, and its row is
///   execution-only.
/// * **The `evidenceRead` grant is resolved whenever the check will carry the
///   `EvidenceRead` role**, so `destination.evidenceReadable` is answered as
///   that principal or not at all.
#[must_use]
pub fn destination_spec_facts(
    operation: PreflightOperation,
    object: &crate::crds::backup_destination::BackupDestination,
    roles: &[DestinationRole],
    policy: &check_policy::Policy,
) -> DestinationSpecFacts {
    // `EvidenceRead` is requested only when the destination configures that
    // grant: probing an unconfigured role would report a refusal about a grant
    // nobody asked for. Absent, verification is `NotAttempted` and the
    // advisory row is not requested. Which principal answers it is
    // `Inputs::evidence_read_plan`'s business.
    let evidence_read =
        destination::resolve(object, DestinationRole::EvidenceRead, policy).map(|d| d.grant);
    // A grant that is configured and refused (not allowlisted, an
    // `ArchiveReadGrant` with no `archiveRead`) is still requested: the
    // controller answers the advisory row with the refusal rather than leave
    // it absent, which would read as "no grant configured".
    let evidence_read_configured =
        !matches!(evidence_read, Ok(destination::ResolvedGrant::NotConfigured));
    let request_evidence_read = evidence_read_configured && operation == PreflightOperation::Backup;
    let reads_evidence = match operation {
        PreflightOperation::Backup => request_evidence_read,
        PreflightOperation::DestinationAccess => roles.contains(&DestinationRole::EvidenceRead),
        PreflightOperation::Restore
        | PreflightOperation::SourceConnection
        | PreflightOperation::Switchover => false,
    };
    let writes_evidence = matches!(
        operation,
        PreflightOperation::Backup | PreflightOperation::DestinationAccess
    ) && roles.contains(&DestinationRole::EvidenceWrite);
    DestinationSpecFacts {
        write_probe: destination::write_probe_enabled(object),
        request_evidence_read,
        evidence_write_grant: writes_evidence.then(|| {
            destination::resolve(object, DestinationRole::EvidenceWrite, policy).map(|d| d.grant)
        }),
        evidence_read_grant: reads_evidence.then_some(evidence_read),
        grant_bindings: grant_bindings_of(operation, object, policy),
    }
}

async fn resolve_destination(
    client: &kube::Client,
    namespace: &str,
    name: &str,
    role: DestinationRole,
    policy: &check_policy::Policy,
) -> Result<Result<ResolvedDestination, destination::DestinationRefusal>, kube::Error> {
    match destination::resolve_ref(client, namespace, name, role, policy).await {
        Ok(d) => Ok(Ok(d)),
        Err(destination::ResolveError::Refused(r)) => Ok(Err(*r)),
        Err(destination::ResolveError::Api(e)) => Err(e),
    }
}

/// Everything one pass reads, in one walk; see [`Inputs`].
///
/// # Errors
///
/// [`ReconcileError`] for anything that is not a finding: a missing namespace,
/// or an API server that would not answer. A referent that does not exist is a
/// finding and comes back inside [`Inputs`].
#[allow(clippy::too_many_lines)]
pub async fn resolve(
    client: &kube::Client,
    pf: &Preflight,
    namespace: &str,
    cache: &check_policy::PolicyCache,
    now: DateTime<Utc>,
) -> Result<Inputs, ReconcileError> {
    let request = &pf.spec.request;
    let policy_ref = check_policy::configured_ref(
        std::env::var(check_policy::POLICY_CONFIGMAP_ENV)
            .ok()
            .as_deref(),
        std::env::var(check_policy::INSTALLATION_NAMESPACE_ENV)
            .ok()
            .as_deref(),
    );
    let policy_load = check_policy::load(client, policy_ref.as_ref(), cache, now)
        .await
        .map_err(ReconcileError::Api)?;
    let policy = policy_load.policy().clone();
    let roster_load = super::approval::load_roster(client)
        .await
        .map_err(ReconcileError::Api)?;
    // The namespace's resolved trust: what the `Approval` controller verifies
    // against and the runner's bundles are rendered from. The roster is still
    // read above: it is the fallback, and its UID is in the binding.
    let resolution = crate::trust::resolve(client, namespace).await;
    if let Err(e) = &resolution {
        warn!(namespace = %namespace, error = %e,
            "the namespace's trust could not be resolved for a preflight; signer.rostered is unknown");
    }
    let roster = roster_facts(&roster_load).with_resolution(resolution.as_ref());

    let mut inputs = Inputs {
        operation: request.operation,
        namespace: namespace.to_string(),
        skip: request
            .skip_checks
            .clone()
            .unwrap_or_default()
            .iter()
            .filter_map(|s| CheckId::parse(s))
            .collect(),
        timeout_seconds: i64::from(request.timeout_seconds),
        policy_digest: policy.digest(),
        policy: Some(policy_load.clone()),
        roster,
        ..Inputs::default()
    };
    if let Some(GoverningTrust::Policy(trust)) = inputs.roster.governing.as_ref() {
        if let crate::trust::TrustSource::Policy {
            name,
            uid,
            generation,
        } = &trust.source
        {
            // A policy edit makes the verdict stale, as a roster edit does.
            inputs.referents.push(referent(
                "TrustPolicy",
                "",
                name,
                uid.as_deref().unwrap_or_default(),
                *generation,
            ));
        }
    }
    if inputs.roster.found {
        inputs.referents.push(referent(
            "TrustRoster",
            "",
            crate::ROSTER_NAME,
            &inputs.roster.uid,
            Some(inputs.roster.generation),
        ));
    }

    // The KafkaCluster, when this operation dials one.
    let cluster_name = match request.operation {
        PreflightOperation::Backup => request.backup.as_ref().map(|b| b.source_ref.name.clone()),
        PreflightOperation::Restore => {
            let r = request.restore.as_ref();
            // `None` here is not "no target": a `restoreRef` request derives
            // it from the `Restore` below.
            r.and_then(|r| r.target_ref.as_ref())
                .map(|t| t.name.clone())
        }
        PreflightOperation::SourceConnection => request
            .source_connection
            .as_ref()
            .map(|c| c.connection_ref.name.clone()),
        // Derived from the `Restore` below, as a `restoreRef` restore check's.
        PreflightOperation::DestinationAccess | PreflightOperation::Switchover => None,
    };

    // The Restore, for a restore bound to an existing subject.
    let mut restore_object: Option<Restore> = None;
    if let Some(r) = request.restore.as_ref() {
        if let Some(ref_) = r.restore_ref.as_ref() {
            let api: Api<Restore> = Api::namespaced(client.clone(), namespace);
            restore_object = api.get_opt(&ref_.name).await.map_err(ReconcileError::Api)?;
        }
    }
    // A switchover review's `Restore` (PROD-04.2 §3.8): its target and its
    // evidence destination are the review's.
    if let Some(sw) = request.switchover.as_ref() {
        let api: Api<Restore> = Api::namespaced(client.clone(), namespace);
        restore_object = api
            .get_opt(&sw.restore_ref.name)
            .await
            .map_err(ReconcileError::Api)?;
    }

    let cluster_name = cluster_name.or_else(|| {
        restore_object
            .as_ref()
            .map(|r| r.spec.target.cluster_ref.name.clone())
    });
    if let Some(name) = cluster_name.as_deref() {
        let api: Api<KafkaCluster> = Api::namespaced(client.clone(), namespace);
        let cluster = api.get_opt(name).await.map_err(ReconcileError::Api)?;
        inputs.cluster_name = Some(name.to_string());
        match cluster {
            None => inputs.connection = None,
            Some(c) => {
                inputs.cluster_uid = c.uid();
                inputs.cluster_generation = c.metadata.generation;
                inputs.cluster_recorded_id = c.status.as_ref().and_then(|s| s.cluster_id.clone());
                inputs.bindings.target_bootstrap = c.spec.bootstrap_servers.clone();
                let use_ = match request.operation {
                    PreflightOperation::Restore => ConnectionUse::PreflightTarget,
                    _ => ConnectionUse::PreflightSource,
                };
                inputs.connection = Some(connection::resolve(&c, use_));
                inputs.referents.push(referent(
                    "KafkaCluster",
                    namespace,
                    name,
                    &inputs.cluster_uid.clone().unwrap_or_default(),
                    inputs.cluster_generation,
                ));
            }
        }
    }

    // The destinations.
    let (archive_ref, evidence_ref, roles, legacy) = match request.operation {
        PreflightOperation::Backup => {
            let b = request.backup.as_ref();
            // The roles decide the rows: the runner emits one destination row
            // per requested role (`ArchiveRead` → `archiveListable`,
            // `EvidenceWrite` → `evidenceWritable`, `EvidenceRead` →
            // `evidenceReadable`), and a blocking row never emitted would hold
            // the verdict at `unknown` for ever. `EvidenceRead` is added below,
            // only when the destination configures that grant.
            (
                b.and_then(|b| b.destination_ref.as_ref().map(|d| d.name.clone())),
                None,
                vec![DestinationRole::ArchiveRead, DestinationRole::EvidenceWrite],
                b.and_then(|b| b.legacy_archive.as_ref().map(|a| a.url.clone())),
            )
        }
        PreflightOperation::Restore => {
            let r = request.restore.as_ref();
            (
                r.and_then(|r| r.source_destination_ref.as_ref().map(|d| d.name.clone())),
                r.and_then(|r| r.evidence_destination_ref.as_ref().map(|d| d.name.clone())),
                vec![DestinationRole::ArchiveRead, DestinationRole::EvidenceWrite],
                // A restore's inline archive is checked, not refused (see the
                // `legacy_source` resolution below); only a backup's inline
                // archive still ends `ArchiveUrlUnreadable`.
                None,
            )
        }
        PreflightOperation::DestinationAccess => {
            let d = request.destination_access.as_ref();
            (
                d.map(|d| d.destination_ref.name.clone()),
                None,
                d.map(|d| d.roles.clone()).unwrap_or_default(),
                None,
            )
        }
        // No destination and no role: nothing is resolved, nothing is
        // projected and no `destination.*` row is expected of the Job.
        PreflightOperation::SourceConnection => (None, None, Vec::new(), None),
        // The restore's evidence destination, read with its `evidenceRead`
        // grant (as an `evidenceFetch` reads evidence) and nothing else.
        PreflightOperation::Switchover => (
            restore_object
                .as_ref()
                .and_then(|r| r.spec.evidence_destination_ref.as_ref())
                .map(|d| d.name.clone()),
            None,
            vec![DestinationRole::EvidenceRead],
            None,
        ),
    };
    inputs.roles = roles;
    inputs.legacy_archive = legacy;
    let archive_role = match request.operation {
        PreflightOperation::Restore => DestinationRole::ArchiveRead,
        PreflightOperation::DestinationAccess => access_primary_role(&inputs.roles),
        PreflightOperation::Backup => DestinationRole::ArchiveWrite,
        // Unreachable (`archive_ref` is `None` here); spelled rather than
        // wildcarded so an operation added later cannot inherit a grant by
        // falling through.
        PreflightOperation::SourceConnection => DestinationRole::ArchiveRead,
        PreflightOperation::Switchover => DestinationRole::EvidenceRead,
    };
    if let Some(name) = archive_ref {
        let resolved = resolve_destination(client, namespace, &name, archive_role, &policy)
            .await
            .map_err(ReconcileError::Api)?;
        if let Ok(d) = resolved.as_ref() {
            inputs.referents.push(referent(
                "BackupDestination",
                namespace,
                &d.name,
                &d.uid,
                Some(d.generation),
            ));
            if let Some(sha) = d.ca_sha256.as_ref() {
                inputs.ca_bundles.push(CaBundleRef {
                    destination_uid: d.uid.clone(),
                    sha256: sha.clone(),
                });
            }
            inputs.bindings.source_storage = Some(d.plan_storage());
        }
        // The spec facts the resolved form does not carry, read once from the
        // object (see `destination_spec_facts`, pure so the derivation is a
        // unit row).
        if resolved.is_ok() {
            let api: Api<crate::crds::backup_destination::BackupDestination> =
                Api::namespaced(client.clone(), namespace);
            if let Some(object) = api.get_opt(&name).await.map_err(ReconcileError::Api)? {
                let facts =
                    destination_spec_facts(request.operation, &object, &inputs.roles, &policy);
                inputs.write_probe = facts.write_probe;
                inputs.evidence_write_grant = facts.evidence_write_grant;
                inputs.evidence_read_grant = facts.evidence_read_grant;
                inputs.grant_bindings = facts.grant_bindings;
                if facts.request_evidence_read {
                    inputs.roles.push(DestinationRole::EvidenceRead);
                }
            }
        }
        inputs.archive_name = Some(name);
        inputs.archive = Some(resolved);
    }
    if let Some(name) = evidence_ref {
        let resolved = resolve_destination(
            client,
            namespace,
            &name,
            DestinationRole::EvidenceWrite,
            &policy,
        )
        .await
        .map_err(ReconcileError::Api)?;
        if let Ok(d) = resolved.as_ref() {
            inputs.referents.push(referent(
                "BackupDestination",
                namespace,
                &d.name,
                &d.uid,
                Some(d.generation),
            ));
            if let Some(sha) = d.ca_sha256.as_ref() {
                inputs.ca_bundles.push(CaBundleRef {
                    destination_uid: d.uid.clone(),
                    sha256: sha.clone(),
                });
            }
            inputs.bindings.evidence_storage = Some(d.evidence_storage());
        }
        inputs.evidence_name = Some(name);
        inputs.evidence = Some(resolved);
    }

    match request.operation {
        PreflightOperation::Backup => {
            if let Some(b) = request.backup.as_ref() {
                inputs.topics = b.topics.clone();
            }
        }
        PreflightOperation::Restore => {
            let r = request.restore.as_ref();
            // The bytes, verbatim, from whichever side supplied them: a draft
            // carries its own, a `restoreRef` takes the `Restore`'s. Never
            // re-serialised: `planHash` binds these exact bytes.
            let (bytes, claimed) = match (
                r.and_then(|r| r.plan_bytes.as_deref()),
                restore_object.as_ref(),
            ) {
                (Some(b), _) => (
                    Some(b.as_bytes().to_vec()),
                    r.and_then(|r| r.plan_hash.clone()),
                ),
                (None, Some(obj)) => (Some(obj.spec.plan_bytes.as_bytes().to_vec()), None),
                (None, None) => (None, None),
            };
            if let Some(bytes) = bytes {
                inputs.plan = Some(PlanFacts::of(&bytes, claimed.as_deref()));
            }

            // A recovery point with no saved destination, read through the
            // principal and location the restore Job will use
            // (`legacy_source_destination`). No referent is added: the URL and
            // Secret name are in the immutable request, the location in the
            // plan bytes `planHash` binds, and the legacy addressing in the
            // policy digest.
            if let Some(archive) = r.and_then(|r| r.legacy_source_archive.as_ref()) {
                let requested = LegacySource {
                    url: archive.url.clone(),
                    secret: archive.secret_ref.as_ref().map(|s| s.name.clone()),
                };
                // A check of an existing Restore reads as that Restore will:
                // its Job projects `spec.sourceArchive` (URL and Secret), so
                // both come from there, and a request naming anything else is
                // refused rather than answered about a credential the restore
                // will not use.
                let (legacy, conflict) = match restore_object.as_ref() {
                    None => (requested, None),
                    Some(obj) => {
                        let own = restore_legacy_source(obj);
                        let conflict = legacy_request_conflict(&requested, &own, &obj.name_any());
                        (own, conflict)
                    }
                };
                inputs.archive = Some(match conflict {
                    Some(refusal) => Err(refusal),
                    None => legacy_source_destination(
                        namespace,
                        &legacy,
                        inputs
                            .plan
                            .as_ref()
                            .and_then(|p| p.parsed.as_ref().ok())
                            .map(|s| &s.source.storage),
                        &policy.legacy_archive_addressing,
                    ),
                });
                inputs.bindings.legacy_source = Some(LegacyBinding {
                    url: legacy.url.clone(),
                    ..LegacyBinding::default()
                });
                inputs.legacy_source = Some(legacy);
                inputs.legacy_evidence_handle = crate::retention::configured_archive_url(
                    std::env::var(crate::retention::ARCHIVE_URL_ENV),
                );
            }
            inputs.restore_name = restore_object.as_ref().map(kube::ResourceExt::name_any);
            inputs.deadline = restore_object.as_ref().and_then(|r| {
                now.checked_add_signed(chrono::Duration::seconds(r.spec.deadline_seconds))
            });
            inputs.backup_id = restore_object
                .as_ref()
                .map(|r| r.spec.backup_set_ref.clone())
                .or_else(|| {
                    inputs
                        .plan
                        .as_ref()
                        .and_then(|p| p.parsed.as_ref().ok().map(|s| s.source.backup.clone()))
                })
                .unwrap_or_default();
            if let Some(obj) = restore_object.as_ref() {
                inputs.referents.push(referent(
                    "Restore",
                    namespace,
                    &obj.name_any(),
                    &obj.uid().unwrap_or_default(),
                    obj.metadata.generation,
                ));
            }

            // The recovery point (a Backup UID).
            let mut recovery_point_source: Option<String> = None;
            inputs.recovery_point = match r.and_then(|r| r.recovery_point_ref.as_ref()) {
                None => RecoveryPointFacts::NotRequested,
                Some(rp) => {
                    let api: Api<Backup> = Api::namespaced(client.clone(), namespace);
                    match api.get_opt(&rp.name).await.map_err(ReconcileError::Api)? {
                        None => RecoveryPointFacts::NotFound {
                            name: rp.name.clone(),
                        },
                        Some(backup) => {
                            let uid = backup.uid().unwrap_or_default();
                            inputs.referents.push(referent(
                                "Backup",
                                namespace,
                                &backup.name_any(),
                                &uid,
                                None,
                            ));
                            inputs.bindings.recovery_point_topics =
                                Some(backup.spec.topics.clone()).filter(|t| !t.is_empty());
                            // What a legacy point was written to: its own
                            // immutable `spec.archive.url`, which `plan.bindings`
                            // holds the legacy plan's location to.
                            if let Some(legacy) = inputs.bindings.legacy_source.as_mut() {
                                legacy.point_url = Some(backup.spec.archive.url.clone());
                                legacy.point = Some(backup.name_any());
                            }
                            recovery_point_source = Some(backup.spec.source_ref.name.clone());
                            RecoveryPointFacts::Found {
                                name: backup.name_any(),
                                uid,
                                expected_uid: rp.uid.clone(),
                                phase: backup.status.as_ref().and_then(|s| s.phase.clone()),
                                // The two digests the location comparison
                                // needs, neither recomputed: what the run froze
                                // (`Backup.status.destination`, absent on a
                                // legacy point), and what this check resolved
                                // for the source destination. Deriving either
                                // from the live `BackupDestination` would let a
                                // later edit hide a moved recovery point.
                                location_digest: backup
                                    .status
                                    .as_ref()
                                    .and_then(|s| s.destination.as_ref())
                                    .map(|d| d.location_digest.clone()),
                                // A saved destination's digest only: a legacy
                                // source's location comes from the plan, so the
                                // comparison would be the plan with itself;
                                // `plan.bindings` holds it instead.
                                expected_location_digest: inputs
                                    .archive
                                    .as_ref()
                                    .filter(|_| inputs.legacy_source.is_none())
                                    .and_then(|r| r.as_ref().ok())
                                    .map(|d| d.location_digest.clone()),
                            }
                        }
                    }
                }
            };

            // A catalog point.
            if let Some(cp) = r.and_then(|r| r.catalog_point_ref.as_ref()) {
                inputs.catalog_point =
                    read_catalog_point(client, namespace, cp, inputs.plan.as_ref(), now).await?;
                if let CatalogPointFacts::Found(found) = &inputs.catalog_point {
                    inputs.referents.push(referent(
                        "RecoveryCatalog",
                        namespace,
                        &found.catalog,
                        &found.catalog_uid,
                        None,
                    ));
                }
            }

            // The approval.
            inputs.approval = match restore_object.as_ref() {
                None => ApprovalFacts::Draft,
                Some(obj) => {
                    let named = obj.spec.approval_ref_name();
                    if named.is_empty() {
                        ApprovalFacts::NotNamed
                    } else {
                        let api: Api<Approval> = Api::namespaced(client.clone(), namespace);
                        match api.get_opt(named).await.map_err(ReconcileError::Api)? {
                            None => ApprovalFacts::NotFound {
                                name: named.to_string(),
                            },
                            Some(a) => {
                                let uid = a.uid().unwrap_or_default();
                                let resource_version = a.resource_version().unwrap_or_default();
                                inputs.approval_binding = Some(ApprovalRef {
                                    uid: uid.clone(),
                                    resource_version: resource_version.clone(),
                                });
                                inputs.referents.push(referent(
                                    "Approval",
                                    namespace,
                                    &a.name_any(),
                                    &uid,
                                    None,
                                ));
                                let verified_condition = a
                                    .status
                                    .as_ref()
                                    .and_then(|s| s.conditions.as_ref())
                                    .and_then(|c| {
                                        c.iter().find(|c| {
                                            c.r#type == crate::conditions::CONDITION_VERIFIED
                                        })
                                    });
                                ApprovalFacts::Found {
                                    name: a.name_any(),
                                    uid,
                                    resource_version,
                                    verified: a.status.as_ref().and_then(|s| s.verified),
                                    reason: verified_condition.and_then(|c| c.reason.clone()),
                                    message: verified_condition.and_then(|c| c.message.clone()),
                                    matched_key_id: a
                                        .status
                                        .as_ref()
                                        .and_then(|s| s.matched_key_id.clone()),
                                    approver_key_window: a
                                        .status
                                        .as_ref()
                                        .and_then(|s| s.approver_key_window.clone())
                                        .map(Box::new),
                                    approved_plan_hash: super::restore::approval_plan_hash(&a),
                                    subject_name: Some(a.spec.subject_ref.name.clone()),
                                    uid_binding: a
                                        .status
                                        .as_ref()
                                        .and_then(|s| s.verified_subject_ref.as_ref())
                                        .zip(obj.uid())
                                        .map(|(bound, restore_uid)| {
                                            Box::new(UidBinding {
                                                verified_uid: bound.uid.clone(),
                                                restore_uid,
                                            })
                                        }),
                                }
                            }
                        }
                    }
                }
            };
            // The source cluster's recorded id, for `TargetEqualsSource`, read
            // from the recovery point's own source `KafkaCluster`, never the
            // plan: a plan could name anything, and the question is whether the
            // target is the cluster the archive was taken from.
            if let Some(source_name) = recovery_point_source.as_deref() {
                let clusters: Api<KafkaCluster> = Api::namespaced(client.clone(), namespace);
                if let Some(source) = clusters
                    .get_opt(source_name)
                    .await
                    .map_err(ReconcileError::Api)?
                {
                    inputs.source_cluster_id =
                        source.status.as_ref().and_then(|s| s.cluster_id.clone());
                }
            }
        }
        // Neither names a plan or a topic set: a destination-access check is
        // about grants and a source-connection check is about one dial.
        PreflightOperation::DestinationAccess | PreflightOperation::SourceConnection => {}
        PreflightOperation::Switchover => {
            if let Some(sw) = request.switchover.as_ref() {
                inputs.switchover = Some(SwitchoverFacts::of(
                    sw,
                    restore_object.as_ref(),
                    resolution.as_ref(),
                ));
            }
            if let Some(obj) = restore_object.as_ref() {
                inputs.restore_name = Some(obj.name_any());
                // The review is stale once the Restore changes (its status
                // included: a new verdict is a new object generation only for
                // spec, so the UID and generation pin the object, and the
                // Job re-reads its evidence).
                inputs.referents.push(referent(
                    "Restore",
                    namespace,
                    &obj.name_any(),
                    &obj.uid().unwrap_or_default(),
                    obj.metadata.generation,
                ));
            }
        }
    }
    Ok(inputs)
}

/// The owner reference every object this check creates carries.
fn owner_of(pf: &Preflight) -> Result<crate::job::RunnerOwner, ReconcileError> {
    Ok(crate::job::RunnerOwner {
        api_version: Preflight::api_version(&()).to_string(),
        kind: Preflight::kind(&()).to_string(),
        name: pf.name_any(),
        uid: pf
            .uid()
            .ok_or_else(|| ReconcileError::NoUid(pf.name_any()))?,
    })
}

/// The events for one Job and its pod, by `involvedObject.uid`.
///
/// By UID, never by name: `FailedCreate` is common in a namespace with a
/// `ResourceQuota`, and someone else's event would cancel a healthy check Job.
async fn events_for(
    client: &kube::Client,
    namespace: &str,
    uids: &[String],
) -> Result<Vec<check::EventFact>, kube::Error> {
    let api: Api<Event> = Api::namespaced(client.clone(), namespace);
    let mut out = Vec::new();
    for uid in uids.iter().filter(|u| !u.is_empty()) {
        let list = api
            .list(&ListParams::default().fields(&format!("involvedObject.uid={uid}")))
            .await?;
        out.extend(list.items.iter().filter_map(check::EventFact::from_event));
    }
    Ok(out)
}

fn image_id_of(pod: Option<&k8s_openapi::api::core::v1::Pod>) -> Option<String> {
    pod?.status
        .as_ref()?
        .container_statuses
        .as_ref()?
        .iter()
        .find(|c| c.name == crate::job::CONTAINER_NAME)
        .map(|c| c.image_id.clone())
        .filter(|i| !i.is_empty())
}

/// Write `/status`, with seam **S7** in both halves.
///
/// Answers the object as the API server stored it when a patch was sent and
/// accepted, and `None` when nothing was sent or the precondition failed. A
/// pass that writes again must precondition on that answer ([`written_or`]):
/// the first write moved the `resourceVersion`, and a second write with the
/// version this pass was handed would be refused 409 and lost.
async fn patch_status(
    client: &kube::Client,
    pf: &Preflight,
    namespace: &str,
    status: &PreflightStatus,
) -> Result<Option<Preflight>, ReconcileError> {
    let name = pf.name_any();
    let mut patch = json!({ "status": status });
    if status_unchanged(
        pf.status
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok())
            .as_ref(),
        &patch,
    ) {
        debug!(
            preflight = %name,
            namespace = %namespace,
            "the computed status equals the one on the object; no patch is sent"
        );
        return Ok(None);
    }
    let resource_version = pf
        .metadata
        .resource_version
        .clone()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            ReconcileError::Api(kube::Error::Discovery(
                kube::error::DiscoveryError::MissingResource(format!(
                    "Preflight {name} carries no metadata.resourceVersion, which a /status \
                     compare-and-set needs (D-SEAMS S7)"
                )),
            ))
        })?;
    patch
        .as_object_mut()
        .expect("a status patch is always a JSON object")
        .insert(
            "metadata".to_string(),
            json!({ "name": name, "resourceVersion": resource_version }),
        );
    let api: Api<Preflight> = Api::namespaced(client.clone(), namespace);
    // A merge patch, never `replace_status`: the role grants `patch` on
    // `preflights/status` and `update` on nothing, and the resourceVersion in
    // the body gives a merge patch its compare-and-set.
    match api
        .patch_status(&name, &PatchParams::default(), &Patch::Merge(patch))
        .await
    {
        Ok(stored) => Ok(Some(stored)),
        Err(kube::Error::Api(e)) if e.code == 409 => {
            debug!(
                preflight = %name,
                namespace = %namespace,
                "the status changed under this reconcile (409); the next pass reads it"
            );
            Ok(None)
        }
        Err(e) => Err(ReconcileError::Api(e)),
    }
}

/// The object a later write of this pass preconditions on: what the earlier
/// write stored, or, when it sent nothing or was refused, the object in hand.
fn written_or<'a>(written: Option<&'a Preflight>, pf: &'a Preflight) -> &'a Preflight {
    written.unwrap_or(pf)
}

/// Reconcile one `Preflight`.
///
/// # Errors
///
/// [`ReconcileError`] for anything that is not a finding.
#[allow(clippy::too_many_lines)]
pub async fn reconcile_preflight(
    pf: &Preflight,
    ctx: &Context,
    cache: &check_policy::PolicyCache,
    now: DateTime<Utc>,
) -> Result<Action, ReconcileError> {
    let name = pf.name_any();
    let namespace = pf
        .namespace()
        .filter(|n| !n.is_empty())
        .ok_or_else(|| ReconcileError::NoNamespace(name.clone()))?;
    let owner = owner_of(pf)?;
    let operation = pf.spec.request.operation;
    let kind = plan_kind(operation);
    let job_name = check_job::check_job_name(kind, &owner.uid);
    let client = &ctx.client;

    let phase = pf.status.as_ref().and_then(|s| s.phase.clone());
    let terminal = matches!(
        phase.as_deref(),
        Some(PHASE_COMPLETED | PHASE_FAILED | PHASE_CANCELLED)
    );

    let jobs: Api<Job> = Api::namespaced(client.clone(), &namespace);

    // 1. Cancellation.
    if pf.spec.cancel_requested && !terminal {
        if let Some(job) = jobs.get_opt(&job_name).await.map_err(ReconcileError::Api)? {
            // Ownership is verified inside `check::cancel`: a Job's name is a
            // pure function of this subject's UID, and a foreign Job that
            // happens to carry it is never patched.
            check::cancel(client, &namespace, &job, &owner.uid)
                .await
                .map_err(ReconcileError::Api)?;
        }
        let status = status_for(
            pf,
            &StatusInput {
                phase: PHASE_CANCELLED.to_string(),
                reason: Some(CheckCode::CancelRequested),
                message: "the check was cancelled on request".to_string(),
                ..StatusInput::default()
            },
            now,
        );
        patch_status(client, pf, &namespace, &status).await?;
        return Ok(Action::await_change());
    }

    // 2. A terminal check is revalidated, never re-run, and collected. The
    // collector runs first, on the same pass, as the only other work a
    // terminal check has; an absent policy means the one-hour default. Its
    // answer never changes the revalidation verdict: a check it deleted gets
    // a 404 on the status write, handled like any other API answer.
    if terminal {
        // The same reference `resolve` reads, from the same pure decision, so a
        // terminal pass and a running one cannot disagree about which document
        // is in force.
        let policy_ref = check_policy::configured_ref(
            std::env::var(check_policy::POLICY_CONFIGMAP_ENV)
                .ok()
                .as_deref(),
            std::env::var(check_policy::INSTALLATION_NAMESPACE_ENV)
                .ok()
                .as_deref(),
        );
        let policy = check_policy::load(client, policy_ref.as_ref(), cache, now)
            .await
            .map_err(ReconcileError::Api)?;
        let retention = policy.policy().preflight.retention_seconds;
        let api: Api<Preflight> = Api::namespaced(client.clone(), &namespace);
        collect_expired(&api, &namespace, retention, now).await?;
        return revalidate(pf, ctx, cache, &namespace, now).await;
    }

    // 3. Everything this pass reads.
    let inputs = resolve(client, pf, &namespace, cache, now).await?;
    let binding = inputs.binding();
    let recorded_binding = binding_status(&binding);

    if let Some(url) = inputs.legacy_archive.as_deref() {
        // Named, not guessed, and a backup's only: a backup check over an
        // inline archive would need the archive-write principal a legacy
        // Backup Job holds, which no check exercises. `Failed` means "the
        // check could not produce a result", which is exactly true and is not
        // a verdict about the operation. A restore's inline archive is read
        // with the restore Job's own principal and never reaches this arm.
        let status = status_for(
            pf,
            &StatusInput {
                phase: PHASE_FAILED.to_string(),
                reason: Some(CheckCode::ArchiveUrlUnreadable),
                message: format!(
                    "this build checks saved destinations only; the inline archive `{url}` \
                     cannot be turned into a check plan. Create a BackupDestination for it and \
                     name it with destinationRef."
                ),
                binding: Some(recorded_binding),
                ..StatusInput::default()
            },
            now,
        );
        patch_status(client, pf, &namespace, &status).await?;
        return Ok(Action::await_change());
    }

    // The rows that decide whether a plan can be rendered at all do not read
    // anything the pod reports, so they can be computed before the Job exists.
    let (probe, _) = inputs.controller_outcomes(&RelayFacts::default(), None, false, None, now);
    let blockers = inputs.plan_blockers(&probe);
    if !blockers.is_empty() {
        let reason = blockers[0].code;
        let message = blockers
            .iter()
            .map(|b| format!("{}: {}", b.id, b.message))
            .collect::<Vec<_>>()
            .join("; ");
        // No plan was rendered, so there is no request to derive the expected
        // rows from.
        let mut checks = assemble(
            probe,
            Vec::new(),
            true,
            &unrendered_job_rows(operation),
            &inputs.skip,
            now,
        );
        // No Job and no pod exist on this path, so every row that named no
        // object of its own is about the `Preflight` itself.
        fill_scopes(
            &mut checks,
            &ScopeReferents {
                subject: Some(subject_scope(pf)),
                job: None,
                pod: None,
            },
        );
        let status = status_for(
            pf,
            &StatusInput {
                phase: PHASE_COMPLETED.to_string(),
                reason: Some(reason),
                message,
                binding: Some(recorded_binding),
                observed_at: Some(now),
                result: Some(result_for(&checks, None).0),
                ..StatusInput::default()
            },
            now,
        );
        patch_status(client, pf, &namespace, &status).await?;
        return Ok(Action::await_change());
    }

    let shape = match build_job_shape(&inputs, &owner, &ctx.runner_image) {
        Ok(shape) => shape,
        Err(why) => {
            let status = status_for(
                pf,
                &StatusInput {
                    phase: PHASE_FAILED.to_string(),
                    reason: Some(CheckCode::ExecutionContextConflict),
                    message: why,
                    binding: Some(recorded_binding),
                    ..StatusInput::default()
                },
                now,
            );
            patch_status(client, pf, &namespace, &status).await?;
            return Ok(Action::await_change());
        }
    };

    // 4. The Job.
    let existing = jobs.get_opt(&job_name).await.map_err(ReconcileError::Api)?;
    let Some(job) = existing else {
        // The binding is recorded before the Job exists: a binding written
        // afterwards could not be told apart from one about whatever the
        // objects became in between.
        let counts = limits::count(
            &limits::check_jobs(client)
                .await
                .map_err(ReconcileError::Api)?,
            &namespace,
            inputs.cluster_uid.as_deref(),
        );
        if let limits::Admission::Queued(code) = limits::admit(
            &counts,
            &inputs.policy.as_ref().map_or_else(
                || check_policy::Policy::defaults().checks,
                |p| p.policy().checks,
            ),
            kind,
        ) {
            let status = status_for(
                pf,
                &StatusInput {
                    phase: PHASE_QUEUED.to_string(),
                    reason: Some(code),
                    message: format!(
                        "{} active check Job(s) in this namespace and {} across the \
                         installation; this check starts when a slot frees",
                        counts.namespace, counts.total
                    ),
                    binding: Some(recorded_binding),
                    ..StatusInput::default()
                },
                now,
            );
            patch_status(client, pf, &namespace, &status).await?;
            return Ok(Action::requeue(Duration::from_secs(REQUEUE_QUEUED_SECS)));
        }

        let status = status_for(
            pf,
            &StatusInput {
                phase: PHASE_PENDING.to_string(),
                reason: Some(CheckCode::PodNotStarted),
                message: "the check plan is rendered and the Job is being created".to_string(),
                binding: Some(recorded_binding),
                job_ref: Some(job_name.clone()),
                ..StatusInput::default()
            },
            now,
        );
        // The first of two writes this pass may make: a plan refusal below
        // writes again, preconditioned on where this write left the object.
        let pending_written = patch_status(client, pf, &namespace, &status).await?;
        let pf_now = written_or(pending_written.as_ref(), pf);

        let config_map = check_plan::build(&job_name, &namespace, &owner, &shape.documents)
            .map_err(|e| plan_conflict(e.to_string()))?;
        let digest = shape.documents.check_plan_sha256();
        match check_plan::ensure(client, &namespace, &config_map, &owner.uid, &digest).await {
            Ok(_) => {}
            Err(check_plan::EnsureError::Api(e)) => return Err(ReconcileError::Api(e)),
            Err(check_plan::EnsureError::Plan(e)) => {
                let status = status_for(
                    pf_now,
                    &StatusInput {
                        phase: PHASE_FAILED.to_string(),
                        reason: Some(e.code()),
                        message: e.to_string(),
                        ..StatusInput::default()
                    },
                    now,
                );
                patch_status(client, pf_now, &namespace, &status).await?;
                return Ok(Action::await_change());
            }
        }
        match check::create_job(client, &shape.spec).await {
            Ok(_) => {}
            // A 409 is the duplicate reconcile working: the name is a pure
            // function of the subject's UID, so the Job already exists and the
            // next pass observes it.
            Err(kube::Error::Api(e)) if e.code == 409 => {}
            Err(e) => return Err(ReconcileError::Api(e)),
        }
        info!(
            preflight = %name,
            namespace = %namespace,
            job = %job_name,
            operation = ?operation,
            "preflight check Job created"
        );
        return Ok(Action::requeue(Duration::from_secs(REQUEUE_RUNNING_SECS)));
    };

    // 5. Observe it.
    let job_uid = job.uid().unwrap_or_default();
    let pod = check::pod::find_owned_pod(client, &namespace, &job)
        .await
        .map_err(ReconcileError::Api)?;
    let events = events_for(
        client,
        &namespace,
        &[
            job_uid.clone(),
            pod.as_ref()
                .and_then(kube::ResourceExt::uid)
                .unwrap_or_default(),
        ],
    )
    .await
    .map_err(ReconcileError::Api)?;
    // The digest the Job was actually created with, read off the plan
    // `ConfigMap`'s annotation rather than recomputed: a destination whose
    // generation moved after the Job started would otherwise verify the relay
    // against a plan the pod never mounted, and report `ResultUnreadable`.
    let expect = FrameExpectations {
        plan_sha256: mounted_plan_digest(client, &namespace, &job_name)
            .await
            .map_err(ReconcileError::Api)?
            .unwrap_or_else(|| shape.documents.check_plan_sha256()),
        subject_uid: owner.uid.clone(),
    };
    let observation = check::observe(client, &namespace, &job, &events, &expect, now)
        .await
        .map_err(ReconcileError::Api)?;

    if observation.cancel_now {
        check::cancel(client, &namespace, &job, &owner.uid)
            .await
            .map_err(ReconcileError::Api)?;
    }

    let relay_result = observation
        .relay
        .as_ref()
        .and_then(CheckRelay::result)
        .and_then(Result::ok);
    let relayed_checks = relay_result
        .as_ref()
        .map(|r| r.checks.clone())
        .unwrap_or_default();
    let facts = RelayFacts::of(&relayed_checks);
    let relayed = relay_result.is_some();
    let image_id = image_id_of(pod.as_ref());

    let (controller, blocked_by_pod) = inputs.controller_outcomes(
        &facts,
        observation.waiting.as_ref(),
        relayed,
        image_id.as_deref(),
        now,
    );

    if observation.phase == check::CheckPhase::Running {
        let status = status_for(
            pf,
            &StatusInput {
                phase: PHASE_RUNNING.to_string(),
                reason: Some(observation.reason),
                message: observation.message.clone(),
                binding: Some(recorded_binding),
                job_ref: Some(job_name.clone()),
                ..StatusInput::default()
            },
            now,
        );
        patch_status(client, pf, &namespace, &status).await?;
        return Ok(Action::requeue(Duration::from_secs(REQUEUE_RUNNING_SECS)));
    }

    // 6. Terminal: the details ConfigMap, then the commit.
    let details = match observation
        .relay
        .as_ref()
        .and_then(|r| r.stream(Stream::Details))
    {
        Some(bytes) if !bytes.is_empty() => {
            // Redacted by rule, not by `redact`, whose 512-character cap is
            // right for a message and destroys a stream: the details document
            // is JSON lines, and a cut line is unparseable.
            let text = logweir_core::check_contract::apply_rules(
                &String::from_utf8_lossy(bytes),
                logweir_core::check_contract::redaction_rules(),
            );
            let object = check::chunks::build_details(&job_name, &namespace, &owner, &text);
            match check::chunks::write_all(
                client,
                &namespace,
                &owner.uid,
                std::slice::from_ref(&object),
            )
            .await
            {
                Ok(_) => Some(DetailsRef {
                    name: check::chunks::details_name(&job_name),
                    sha256: Some(logweir_core::ids::sha256_prefixed(text.as_bytes())),
                }),
                Err(check::chunks::WriteError::Api(e)) => return Err(ReconcileError::Api(e)),
                Err(check::chunks::WriteError::Conflict(c)) => {
                    warn!(
                        preflight = %name,
                        namespace = %namespace,
                        error = %c,
                        code = CheckCode::ResultStorageConflict.as_str(),
                        "the details ConfigMap could not be written; the verdict is published \
                         without it"
                    );
                    None
                }
            }
        }
        _ => None,
    };

    // `Failed` means no result could be produced. A pod that never started
    // because a Secret is missing did produce one
    // (`connection.credentialProjected notReady CredentialSecretNotFound`,
    // with a remedy), and `Failed` would hide that finding. A waiting code
    // that belongs to no row (the Job's deadline, a disrupted node, an
    // unreadable relay) really is `Failed`.
    let attributed = observation
        .waiting
        .as_ref()
        .and_then(|w| attribute_for(operation, w, &inputs.projections()))
        .is_some();
    let failed = observation.phase == check::CheckPhase::Failed && !relayed && !attributed;
    // The rows this plan's runner will have emitted, derived from the request
    // that was rendered, not the operation.
    let expected = job_rows(
        &shape.plan.request,
        inputs.plan.as_ref().is_some_and(PlanFacts::scratch),
    );
    // A legacy source has no `BackupDestination`: the runner's placeholder
    // scope is replaced by the inline archive itself.
    let relayed_checks = match inputs.legacy_source.as_ref() {
        Some(legacy) => rescope_legacy(relayed_checks, legacy),
        None => relayed_checks,
    };
    let mut checks = assemble(
        controller,
        relayed_checks,
        blocked_by_pod,
        &expected,
        &inputs.skip,
        now,
    );
    fill_scopes(
        &mut checks,
        &ScopeReferents {
            subject: Some(subject_scope(pf)),
            job: Some(CheckScope {
                kind: "Job".to_string(),
                name: job_name.clone(),
                uid: (!job_uid.is_empty()).then(|| job_uid.clone()),
            }),
            pod: pod.as_ref().map(|p| CheckScope {
                kind: "Pod".to_string(),
                name: p.name_any(),
                uid: p.uid(),
            }),
        },
    );
    let (result, dropped) = result_for(&checks, details);
    let expires_at = result.expires_at;
    let message = if dropped == 0 {
        observation.message.clone()
    } else {
        format!(
            "{} ({dropped} further check row(s) did not fit the {MAX_PUBLISHED_CHECKS}-row cap \
             on status.result.checks; the verdict is computed over all of them)",
            observation.message
        )
    };
    let status = status_for(
        pf,
        &StatusInput {
            // A check that produced rows is `Completed`, whatever they say: the
            // row set always carries the controller's own findings, so it is
            // `Failed` only when the pod's failure left nothing to relay.
            phase: if failed {
                PHASE_FAILED.to_string()
            } else {
                PHASE_COMPLETED.to_string()
            },
            reason: Some(if failed {
                observation.reason
            } else {
                verdict_reason(&result)
            }),
            message,
            binding: Some(recorded_binding),
            job_ref: Some(job_name.clone()),
            observed_at: Some(now),
            result: Some(result),
        },
        now,
    );
    patch_status(client, pf, &namespace, &status).await?;

    // The TTL is patched only after the commit: the relay lives on the pod,
    // and the TTL controller deletes a Job and its pods together.
    if crate::controllers::backup::job_finished(&job) {
        check::set_ttl(client, &namespace, &job_name)
            .await
            .map_err(ReconcileError::Api)?;
    }

    Ok(revalidation_action(expires_at, now))
}

fn plan_conflict(why: String) -> ReconcileError {
    ReconcileError::Api(kube::Error::Discovery(
        kube::error::DiscoveryError::MissingResource(why),
    ))
}

/// The scalar reason a finished verdict carries.
#[must_use]
pub fn verdict_reason(result: &PreflightResult) -> CheckCode {
    match result.state.as_str() {
        "ready" => CheckCode::Valid,
        "notReady" => CheckCode::NotReady,
        _ => CheckCode::BlockedByPrerequisite,
    }
}

/// When to look at a terminal, still-`ready` check again.
#[must_use]
pub fn revalidation_action(expires_at: Option<DateTime<Utc>>, now: DateTime<Utc>) -> Action {
    match expires_at {
        // A verdict with no expiry cannot go stale by the clock, and one whose
        // expiry has passed is downgraded on the pass that notices.
        None => Action::await_change(),
        Some(e) => {
            let secs = (e - now).num_seconds().max(0);
            Action::requeue(Duration::from_secs(
                u64::try_from(secs).unwrap_or(0).max(MIN_REVALIDATE_SECS),
            ))
        }
    }
}

/// The digest the plan `ConfigMap` this Job mounts actually carries.
async fn mounted_plan_digest(
    client: &kube::Client,
    namespace: &str,
    job_name: &str,
) -> Result<Option<String>, kube::Error> {
    let maps: Api<k8s_openapi::api::core::v1::ConfigMap> =
        Api::namespaced(client.clone(), namespace);
    Ok(maps
        .get_opt(&check_plan::plan_config_map_name(job_name))
        .await?
        .and_then(|cm| {
            cm.metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(check_plan::DIGEST_ANNOTATION))
                .cloned()
        }))
}

/// One terminal preflight, as [`expired_terminal`] reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalPreflight {
    /// `metadata.name`.
    pub name: String,
    /// `metadata.uid`: the DELETE precondition, not the name.
    pub uid: String,
    /// The instant retention counts from: `status.result.expiresAt` when the
    /// check produced a verdict with an expiry, else when it was observed.
    pub basis: DateTime<Utc>,
}

/// Which terminal preflights are collectable. Pure.
///
/// `now > basis + retentionSeconds`, where the basis is `result.expiresAt` for
/// a check that produced a verdict and the observation time for one that never
/// did. No keep-last-N cohort, deliberately: a `Preflight` is about one plan of
/// one operation, and a cohort would delete the only readiness verdict a
/// restore has. Ordered newest-first by basis with the UID as tie-break, so two
/// passes over the same input agree.
#[must_use]
pub fn expired_terminal(
    preflights: &[TerminalPreflight],
    retention_seconds: u32,
    now: DateTime<Utc>,
) -> Vec<String> {
    let mut sorted: Vec<&TerminalPreflight> = preflights.iter().collect();
    sorted.sort_by(|a, b| b.basis.cmp(&a.basis).then_with(|| a.uid.cmp(&b.uid)));
    let retention = chrono::Duration::seconds(i64::from(retention_seconds));
    sorted
        .into_iter()
        .filter(|p| now > p.basis + retention)
        .map(|p| p.uid.clone())
        .collect()
}

/// One listed object as [`expired_terminal`] reads it, or `None` when it is not
/// a collectable terminal preflight.
fn gc_row(pf: &Preflight) -> Option<TerminalPreflight> {
    let phase = pf.status.as_ref().and_then(|s| s.phase.as_deref())?;
    // The guard that matters: a non-terminal check is still running, and its
    // pod holds the only copy of a relay nobody has read.
    if !matches!(phase, PHASE_COMPLETED | PHASE_FAILED | PHASE_CANCELLED) {
        return None;
    }
    if pf.metadata.deletion_timestamp.is_some() {
        return None;
    }
    let status = pf.status.as_ref()?;
    let basis = status
        .result
        .as_ref()
        .and_then(|r| r.expires_at)
        .or(status.observed_at)
        .or_else(|| {
            status
                .conditions
                .as_ref()
                .and_then(|c| c.iter().find(|c| c.r#type == CONDITION_COMPLETE))
                .and_then(|c| c.last_transition_time)
        })
        .or(pf.metadata.creation_timestamp.as_ref().map(|t| t.0))?;
    Some(TerminalPreflight {
        name: pf.name_any(),
        uid: pf.uid().filter(|u| !u.is_empty())?,
        basis,
    })
}

/// Collect the namespace's expired terminal checks.
///
/// The same four bounds as `controllers::topic_discovery::collect_expired`:
/// one kind, terminal only, UID preconditions, and at most
/// [`GC_MAX_DELETES_PER_PASS`] per pass. The details `ConfigMap` and the check
/// Job go by ownerReference cascade.
///
/// # Errors
///
/// [`ReconcileError::Api`] only when the LIST fails; a failed DELETE is logged
/// and the pass continues.
async fn collect_expired(
    api: &Api<Preflight>,
    namespace: &str,
    retention_seconds: u32,
    now: DateTime<Utc>,
) -> Result<usize, ReconcileError> {
    let page = api
        .list(&kube::api::ListParams::default().limit(GC_LIST_LIMIT))
        .await
        .map_err(ReconcileError::Api)?;
    let rows: Vec<TerminalPreflight> = page.items.iter().filter_map(gc_row).collect();
    let doomed = expired_terminal(&rows, retention_seconds, now);
    let mut collected = 0usize;
    for uid in doomed.iter().take(GC_MAX_DELETES_PER_PASS) {
        let Some(row) = rows.iter().find(|r| &r.uid == uid) else {
            continue;
        };
        let params = kube::api::DeleteParams {
            preconditions: Some(kube::api::Preconditions {
                uid: Some(uid.clone()),
                resource_version: None,
            }),
            ..kube::api::DeleteParams::default()
        };
        // `api.delete(` — see `controllers::topic_discovery::collect_expired`
        // for why the receiver is spelled this way and not aliased.
        match api.delete(&row.name, &params).await {
            Ok(_) => {
                collected += 1;
                info!(
                    namespace = %namespace,
                    preflight = %row.name,
                    "a terminal Preflight past its retention window was collected"
                );
            }
            Err(e) => warn!(
                namespace = %namespace,
                preflight = %row.name,
                error = %e,
                "a terminal Preflight could not be collected; the next pass tries again"
            ),
        }
    }
    Ok(collected)
}

/// Every exit requeues at [`REQUEUE_TERMINAL_SECS`] rather than
/// `await_change()`, which keeps the collector alive: `reconcile_preflight`'s
/// terminal branch runs the GC on the same pass, and an object parked on
/// `await_change()` never has another pass. The cost is one list per terminal
/// check per hour per namespace.
async fn revalidate(
    pf: &Preflight,
    ctx: &Context,
    cache: &check_policy::PolicyCache,
    namespace: &str,
    now: DateTime<Utc>,
) -> Result<Action, ReconcileError> {
    let Some(status) = pf.status.as_ref() else {
        return Ok(Action::requeue(Duration::from_secs(REQUEUE_TERMINAL_SECS)));
    };
    let (Some(result), Some(recorded)) = (status.result.as_ref(), status.binding.as_ref()) else {
        return Ok(Action::requeue(Duration::from_secs(REQUEUE_TERMINAL_SECS)));
    };
    if result.state != state_str(OverallState::Ready) {
        // Nothing to downgrade: `notReady` and `unknown` do not get better by
        // going stale.
        return Ok(Action::requeue(Duration::from_secs(REQUEUE_TERMINAL_SECS)));
    }
    // The clock first, at no API cost: an expired verdict is stale whatever
    // the referents say.
    let expired = result.expires_at.is_none_or(|e| now >= e);
    let stale = if expired {
        vec![logweir_core::check_contract::StaleReason::Expired]
    } else {
        let inputs = resolve(&ctx.client, pf, namespace, cache, now).await?;
        stale_against_status(recorded, result.expires_at, &inputs.binding(), now)
    };
    let Some(downgraded) = downgrade(result, &stale) else {
        return Ok(revalidation_action(result.expires_at, now));
    };
    let reasons = stale_text(&stale);
    info!(
        preflight = %pf.name_any(),
        namespace = %namespace,
        stale_reasons = %reasons,
        "a ready preflight stopped applying; its verdict is downgraded to unknown"
    );
    let next = status_for(
        pf,
        &StatusInput {
            phase: PHASE_COMPLETED.to_string(),
            reason: Some(CheckCode::BlockedByPrerequisite),
            message: format!(
                "this result no longer applies ({reasons}); create a new Preflight for the \
                 current objects"
            ),
            result: Some(downgraded),
            ..StatusInput::default()
        },
        now,
    );
    patch_status(&ctx.client, pf, namespace, &next).await?;
    Ok(Action::requeue(Duration::from_secs(REQUEUE_TERMINAL_SECS)))
}

/// The `kube::runtime` reconcile entry point.
async fn reconcile(
    pf: Arc<Preflight>,
    ctx: Arc<ReconcilerContext>,
) -> Result<Action, ReconcileError> {
    // One clock read per pass: two reads could put a verdict and the expiry
    // that bounds it on either side of the same instant.
    reconcile_preflight(&pf, &ctx.context, &ctx.policy, Utc::now()).await
}

fn error_policy(pf: Arc<Preflight>, err: &ReconcileError, _ctx: Arc<ReconcilerContext>) -> Action {
    warn!(
        preflight = %pf.name_any(),
        error = %err,
        "preflight reconcile failed; requeueing"
    );
    Action::requeue(Duration::from_secs(ERROR_REQUEUE_SECONDS))
}

/// The context this reconciler holds: the shared one, plus the policy cache.
///
/// A second type rather than a field on [`Context`], which every reconciler
/// shares and none of the others would read.
pub struct ReconcilerContext {
    /// What every reconciler in this directory needs.
    pub context: Context,
    /// The 30-second policy cache, so a reconcile does not `get` the policy
    /// `ConfigMap` on every pass.
    pub policy: check_policy::PolicyCache,
}

/// Run the `Preflight` controller until the process ends.
pub fn controller(
    client: kube::Client,
    runner_image: crate::job::RunnerImage,
) -> impl std::future::Future<Output = ()> + Send {
    // One watch per watched namespace: `crate::scope` is the whole cluster
    // unless `LOGWEIR_WATCH_NAMESPACES` names the execution namespaces, and
    // then this reconciler runs once per namespace with an `Api::namespaced`
    // watch, the only shape the scoped chart's RoleBindings permit.
    crate::scope::run_everywhere(move |namespace| {
        controller_in(client.clone(), runner_image.clone(), namespace)
    })
}

/// One watch of [`controller`], over `namespace` (`None` is the whole
/// cluster).
fn controller_in(
    client: kube::Client,
    runner_image: crate::job::RunnerImage,
    namespace: Option<String>,
) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<Preflight> = crate::scope::api(&client, namespace.as_deref());
    let jobs: Api<Job> = crate::scope::api(&client, namespace.as_deref());
    let ctx = Arc::new(ReconcilerContext {
        context: Context {
            client,
            // No archive handle: the archive checks are the check Job's, with
            // the destination's own credential, inside a pod that ends.
            archive: None,
            runner_image,
        },
        policy: check_policy::PolicyCache::new(),
    });
    async move {
        Controller::new(api, watcher::Config::default())
            .owns(jobs, watcher::Config::default())
            .run(reconcile, error_policy, ctx)
            .for_each(|_| std::future::ready(()))
            .await;
    }
}
