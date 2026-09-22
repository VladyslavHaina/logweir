//! The `Preflight` reconciler: run one isolated check Job and say, in codes an
//! operator can act on, what would stop this operation from succeeding.
//!
//! # A `ready` verdict authorizes nothing (D2 §6.8)
//!
//! Everything this controller publishes is ADVISORY. The backup controller's
//! glob rail and destination admission, the runner's phase −1 source rails, the
//! restore admission checks 0–9, phase 0's collision check and LogAppendTime
//! probe, G-WIN in phase 5 and PLAT-02.2's signer validation all still run, and
//! none of them consults a `Preflight`. That is not a convention: it is
//! `no_execution_path_reads_preflight_or_discovery` in
//! `tests/preflight_controller.rs`, a source scan over the four execution
//! reconcilers and both runner paths, whose planted mutant is an `import` of
//! this kind into `controllers/restore.rs`.
//!
//! The reason is the defect UI-FAKEPREFLIGHT names. A preview is a statement
//! about a moment; a run happens later. A colliding topic created in between is
//! caught at execution and nowhere else, and a reconciler that trusted a green
//! badge would have removed the only check that could see it.
//!
//! # It proves what only a pod can prove (D2 §6.1)
//!
//! The controller holds no verb on `secrets` — `linkage::the_controller_never_reads_a_secret`
//! — and would not want one if it did: "the bytes exist" is not "the credential
//! works". So a preflight is one short-lived Job in the SAME shape an execution
//! pod has (same image, same ServiceAccount, same Secret projections, same CA
//! and signing mounts, `automountServiceAccountToken: false`), and what it
//! reports is what that pod observed: an image pull on a schedulable node, a
//! kubelet that could project the key, a broker that accepted the SASL
//! exchange, a bucket that answered a LIST, a signer that loaded.
//!
//! The costs are accepted and named in D2 §6.1: one pod per preflight, the
//! namespace's quota, and a missing Secret blocking the whole pod — in which
//! case every Job-sourced row is reported `unknown` with
//! [`CheckCode::BlockedByPrerequisite`] and the named cause beside it, rather
//! than silently passing.
//!
//! # What this controller writes, and what it never writes
//!
//! It patches `preflights/status`, creates its own plan and result
//! `ConfigMap`s and its own Job, and patches that Job's
//! `ttlSecondsAfterFinished` after the status commit. It patches **no target**:
//! a restore preflight performs no write of any kind, and the validate-only
//! `CreateTopics` D2 §6.7 calls for belongs to the runner, inside the pod,
//! with `AdminOptions::validate_only(true)`. `a_restore_preflight_writes_to_no_target`
//! is the structural test.
//!
//! # A verdict that cannot say what it was about is worthless
//!
//! `status.binding` is recorded BEFORE the Job is created, from the objects
//! this pass actually resolved: the recomputed `planHash`, every referent with
//! its UID and generation, the CA digests, the roster, the approval's resource
//! version and the policy digest, reduced to one
//! [`inputs_digest`](logweir_core::check_contract::inputs_digest). W12 recomputes
//! that from live objects on every GET and answers `applicable: false` with
//! named `staleReasons` when it differs — editing the target, the recovery
//! point, the point in time, the topic subset or the mapping prefix all change
//! the plan bytes, and choosing a recreated destination changes a UID.
//!
//! This controller keeps the same promise from its own side: a terminal
//! `Preflight` whose `result.expiresAt` has passed, or whose referents have
//! moved under it, is DOWNGRADED to `unknown` on the next pass. A stale green
//! badge that nobody is looking at the API through is still a stale green
//! badge.

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
    DestinationPlan, FrameExpectations, Gating, OperationReadinessRequest, OverallState, Referent,
    RestorePreflightRequest, RosterRef, Stream, CHECK_CONTRACT_VERSION, CHECK_PLAN_CONTRACT,
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

// ---------------------------------------------------------------------------
// Phases, conditions and requeues
// ---------------------------------------------------------------------------

/// Nothing has been created yet.
pub const PHASE_PENDING: &str = "Pending";
/// A concurrency ceiling is holding this check back — D2 §4.4.
pub const PHASE_QUEUED: &str = "Queued";
/// The Job exists and has not finished.
pub const PHASE_RUNNING: &str = "Running";
/// A result was produced. **`Completed` is not `ready`**: the verdict is in
/// `status.result.state`.
pub const PHASE_COMPLETED: &str = "Completed";
/// No result could be produced at all — D2 §6.2's failure vocabulary.
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
/// How long before a queued check is looked at again — D2 §4.3's `limits.rs`.
pub const REQUEUE_QUEUED_SECS: u64 = limits::QUEUED_REQUEUE_SECS;
/// How long before a reconcile that ERRORED is retried.
pub const ERROR_REQUEUE_SECONDS: u64 = 30;
/// The floor on the revalidation requeue of a terminal, still-`ready` check.
///
/// A check whose `expiresAt` is seconds away would otherwise be requeued at a
/// sub-second interval and spin the loop; a check whose expiry has already
/// passed is downgraded on this very pass and never requeued at all.
pub const MIN_REVALIDATE_SECS: u64 = 30;
/// How long a terminal check waits before the collector looks at it again —
/// D2 §4.3's `gc.rs`.
///
/// ONE HOUR, the same pace `controllers::topic_discovery` uses, and it is the
/// backstop rather than the mechanism: a check whose verdict can still go
/// stale is requeued much sooner by [`revalidation_action`], and this is what
/// wakes the one whose verdict cannot — a `notReady` or `unknown` result that
/// would otherwise sit forever with `Action::await_change()`.
pub const REQUEUE_TERMINAL_SECS: u64 = 3600;
/// The most objects ONE garbage-collection pass may delete — D2 §4.3.
///
/// See `controllers::topic_discovery::GC_MAX_DELETES_PER_PASS`: a bound on the
/// blast radius and on the API server, not on the total.
pub const GC_MAX_DELETES_PER_PASS: usize = 20;
/// The page size one garbage-collection pass lists.
pub const GC_LIST_LIMIT: u32 = 500;

// ---------------------------------------------------------------------------
// The controller's half of D2 §6.3's catalogue
// ---------------------------------------------------------------------------

/// Fifteen minutes — D2 §6.3's default expiry.
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
    /// How long its answer is good for. `None` is D2 §6.3's "until bytes
    /// change" — such a row contributes no `expiresAt` and cannot pull the
    /// aggregate's expiry forward.
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
    // EXECUTION-ONLY FOR A RESTORE, AND BLOCKING FOR A BACKUP — not a
    // weakening, a statement about what is answerable. The verdict needs the
    // runner's PUBLIC `signerKeyId` fact, which rides on
    // `signer.privateKeyUsable`; the landed `RestorePreflightRequest` (D2 §4.2,
    // W1's contract) carries no `signer_path`, so a `restorePreflight` pod is
    // never given a key to report. A BLOCKING row nobody can answer pins every
    // restore preflight at `unknown` for ever, which is reviewer finding F1's
    // own shape. The row is still reported, with `SignerKeyIdNotObserved` and a
    // message naming the reason. Closing it is a W1 amendment: add
    // `signer_path` to `RestorePreflightRequest`.
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

/// The **C** and **E** rows of a `SourceConnection` check (D2-SOURCECHECK).
///
/// D2 §6.3's Backup catalogue MINUS everything that is about something the
/// connection is used FOR. No `destination.*`, because the request names none;
/// no `signer.*`, because nothing would be signed and projecting a signing key
/// into a connectivity test would widen the pod's blast radius past the
/// question; no `connection.topicsReadable`, because no topic was selected for
/// anything to be readable from. What is left is the connection itself, and
/// `configuration.*`, which is about the installation and not about the
/// operation.
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
    // A DESTINATION CREDENTIAL ROW NEEDS A DESTINATION. It used to be
    // unconditional, which was harmless while every operation named one; a
    // `SourceConnection` check names none, and an unconditional row would have
    // reported `destination.credentialProjected unknown/PodNotStarted` —
    // BLOCKING — about a destination nobody asked for, pinning every
    // connection test at `unknown`.
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
        PreflightOperation::Restore => out.push(row(
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
    };
    let mut out = base.to_vec();
    out.extend(pod_rows(operation));
    out
}

/// The rows the check JOB will answer for THIS RENDERED PLAN — **pure**, and
/// the mirror of the runner's own emission rules.
///
/// # Why this is a function of the plan and not of the operation
///
/// It used to be a static list per operation, and that was reviewer finding
/// **F1**: a `Preflight` could never report `ready`. The rows the runner emits
/// are decided by the REQUEST it is handed — which `roles` a destination check
/// exercises, whether a `signer_path` was projected, whether an evidence
/// destination was named, whether the restore plan's target mode is `scratch` —
/// and every static list differed from the real one. `assemble` renders a row
/// nobody answered as a BLOCKING `unknown`, so each difference pinned the
/// verdict at `unknown` for ever, with a row reading "the check Job did not
/// report this row" and no remedy.
///
/// So the rule lives here, once, and
/// `the_expected_rows_are_the_rows_the_runner_emits` holds it against the
/// runner's own pinned fixtures in `crates/logweir/tests/check_cli.rs` — the
/// two crates share no dependency edge, so that test reads the runner's
/// `want` literals out of its source rather than copying them.
///
/// `restore_target_is_scratch` comes from the plan bytes a `restorePreflight`
/// mounts: `target.scratchMarker` is the one row whose presence depends on the
/// document rather than on the request, and the controller parses the same
/// bytes ([`PlanFacts::scratch`]).
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
                CheckOperation::Restore => push(CheckId::TargetAuthenticated),
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
            if r.signer_path.is_some() {
                push(CheckId::SignerPrivateKeyUsable);
            }
        }
        CheckRequest::DestinationAccess(r) => {
            out.insert(CheckId::RunnerContract);
            for role in &r.roles {
                out.insert(destination_row_for(*role));
            }
        }
        // TWO ROWS, AND `connection.topicsDescribable` IS NOT ONE OF THEM.
        // `kinds::source_connection::run` emits exactly these; the row it
        // deliberately omits is argued there, and this mirror is what
        // `the_expected_rows_are_the_rows_the_runner_emits` holds against the
        // runner's own fixtures.
        CheckRequest::SourceConnection(_) => {
            out.insert(CheckId::RunnerContract);
            out.insert(CheckId::ConnectionAuthenticated);
        }
        CheckRequest::RestorePreflight(r) => {
            let wanted: BTreeSet<CheckId> = r.checks.iter().copied().collect();
            let skipped: BTreeSet<CheckId> = r.skip_checks.iter().copied().collect();
            let want =
                |id: CheckId| (wanted.is_empty() || wanted.contains(&id)) && !skipped.contains(&id);
            // `restore.rs` pushes `runner.contract` UNCONDITIONALLY — unlike
            // `readiness.rs`, which filters it. The asymmetry is the runner's;
            // mirroring it is the whole point of this function.
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
        }
        // None of these three is ever rendered by this controller: a topic
        // inventory belongs to `TopicDiscovery`, an evidence fetch to the
        // verification path, and a `catalogSync` to `RecoveryCatalog` — whose
        // result is a relayed BODY and not a row set, so it has no expected
        // rows for a `Preflight` to be missing.
        CheckRequest::TopicInventory(_)
        | CheckRequest::EvidenceFetch(_)
        | CheckRequest::CatalogSync(_) => {}
    }
    out
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
/// Used only on the path where a blocking controller row refused before a
/// `CheckRequest` existed (an unresolvable connection or destination, an
/// unparseable plan). Every row here is `unknown` in that case whatever the set
/// is; what it buys is that the verdict LISTS the questions that went
/// unanswered instead of publishing a short green-looking record.
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
        PreflightOperation::Restore => [
            CheckId::RunnerContract,
            // `plan.parse` is deliberately absent: the CONTROLLER answers it
            // (it holds the claimed hash and the bytes), so listing it here
            // would claim one id for two authorities.
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

/// The plan kind an operation runs — D2 §4.2.
#[must_use]
pub fn plan_kind(operation: PreflightOperation) -> CheckPlanKind {
    match operation {
        PreflightOperation::Backup => CheckPlanKind::OperationReadiness,
        PreflightOperation::Restore => CheckPlanKind::RestorePreflight,
        PreflightOperation::DestinationAccess => CheckPlanKind::DestinationAccess,
        PreflightOperation::SourceConnection => CheckPlanKind::SourceConnection,
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
    }
}

/// The ONE construction site for a controller-owned outcome.
///
/// Going through it is what makes gating and expiry properties of the ROW
/// rather than of the call site — the same rule the runner's catalogue keeps on
/// its side, and the reason `every_controller_row_has_a_catalogue_entry` can be
/// a real guard.
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
/// D2 §6.3 spells two rows' expiry as `min(15 m, key notAfter)` and
/// `min(10 m, notAfter)`: a verdict about a key must not outlive the key.
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

// ---------------------------------------------------------------------------
// The controller-authority rows, each a pure function of one reduced fact
// ---------------------------------------------------------------------------

/// `connection.resolved` / `target.resolved` — D2 §6.3.
///
/// The three `notReady` codes are the catalogue's, and the mapping from the
/// saved-connection contract's own terminal states is here rather than in
/// `connection::resolve` because a `CheckCode` is a preflight vocabulary and a
/// terminal state is an execution one.
#[must_use]
pub fn connection_row(
    operation: PreflightOperation,
    resolved: Option<&Result<ResolvedConnection, connection::ConnectionRefusal>>,
    cluster_name: &str,
    now: DateTime<Utc>,
) -> CheckOutcome {
    let id = match operation {
        PreflightOperation::Restore => CheckId::TargetResolved,
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

/// `destination.resolved` — D2 §6.3.
///
/// `DestinationRefusal` already carries a [`CheckCode`], because D2 §3.3's
/// validation vocabulary and §6.3's catalogue are the same closed set; a second
/// mapping here would be a second spelling.
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

/// `configuration.policy` — D2 §6.3, advisory.
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

/// `configuration.egress` — D2 §6.3's execution-only row.
///
/// It is `unknown` FOREVER and by construction: Docker Desktop does not enforce
/// NetworkPolicy, a cluster that does enforces it at the CNI, and neither the
/// controller nor a pod that got through can tell an operator whether the deny
/// path is actually closed. `CheckOutcome::new` forces an execution-only row to
/// `unknown` whatever a caller passes, so the remedy is the whole content.
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

/// What the controller knows about the signing roster — the reduced fact
/// `signer.rostered` reads.
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
}

impl RosterFacts {
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
/// The sha256 of a SubjectPublicKeyInfo DER, lowercase hex —
/// `logweir_core::trust::TrustedKey::key_id`'s form, and what
/// `TrustRoster.spec.signingKeys[*].keyId` is written in. Nothing else is
/// comparable against the roster.
#[must_use]
pub fn is_signer_key_id(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// `signer.rostered` — D2 §6.3.
///
/// # Why the key id is `unknown` and not `notReady` before the pod runs
///
/// The PUBLIC key id is a fact the check Job reports (`signer.privateKeyUsable`
/// carries it as the `signerKeyId` fact), because it is derived from the
/// private key the kubelet projected and the controller may not read that
/// Secret. So the first pass — and every pass where the pod has not relayed —
/// can only say `SignerKeyIdNotObserved`, and saying `SignerNotRostered`
/// instead would be a claim about a key nobody has seen.
///
/// # The scope is the `TrustRoster`
///
/// Every answer this row can give — not found, empty, not listed, expired,
/// rostered — is a statement about the one cluster-scoped `TrustRoster`, and
/// the remedy for four of the five is "edit that object". It carries the
/// roster's UID once one was read, so a verdict taken against a roster that has
/// since been replaced is visibly about a different object.
#[must_use]
pub fn signer_rostered_row(
    operation: PreflightOperation,
    roster: &RosterFacts,
    observed_key_id: Option<&str>,
    now: DateTime<Utc>,
) -> CheckOutcome {
    let id = CheckId::SignerRostered;
    let roster_scope = CheckScope {
        kind: "TrustRoster".to_string(),
        name: crate::ROSTER_NAME.to_string(),
        uid: (roster.found && !roster.uid.is_empty()).then(|| roster.uid.clone()),
    };
    if operation == PreflightOperation::Restore {
        // See `RESTORE_CONTROLLER_ROWS`: the restore check plan carries no
        // signer path, so no pod reports a key id and there is nothing to match
        // against the roster. `CheckOutcome::new` forces an execution-only row
        // to `unknown` whatever is passed here.
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
            "The runner still validates its signer before it writes anything (PLAT-02.2), and \
             `TrustRoster.spec.signingKeys` is what makes that evidence verifiable.",
        )
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
    // A RELAYED VALUE THAT IS NOT A KEY ID IS NOT A KEY ID. D2-SIGNERID-
    // REDACTED: the runner's `signerKeyId` fact went through the redactor's
    // long-run rule, which replaced it with the literal `[redacted]`, and this
    // comparison then asked the roster whether it listed a key called
    // `[redacted]`. It never does, so `signer.rostered` answered
    // `notReady/SignerNotRostered` for a key whose SPKI sha256 IS on the roster
    // — a permanent, blocking false negative on every Backup preflight, while
    // `signer.privateKeyUsable` on the same Job said `ready`.
    //
    // The redactor now keeps a key id ([`logweir_core::check_contract::
    // redact`]'s public-identifier exemption), and this is the belt: the roster
    // is written in sha256 of a SubjectPublicKeyInfo DER, so anything that is
    // not 64 lowercase hex characters did not come out of `ValidatedSigner`,
    // whatever it came out of. The honest answer for it is `unknown` — the same
    // one a pod that has not reported gets — and NOT a blocking refusal about a
    // key nobody has actually seen.
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

/// `connection.clusterIdentity` / `target.clusterIdentity` — the **J+C** row.
///
/// The check Job reports the OBSERVED cluster id as a fact; the verdict needs
/// `KafkaCluster.status.clusterId` and `TrustRoster.spec.allowedClusterIds`,
/// neither of which a credential-holding Job is given Kubernetes read for. So
/// the two halves meet here, and this is the one row whose authority is
/// genuinely both.
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
            // The allowlist is a SCRATCH-mode rule (D2 §6.3's
            // `TargetNotAllowlisted` is spelled "(scratch)"): a `newTopic`
            // restore writes into a named cluster on purpose.
            if scratch && !allowed_cluster_ids.iter().any(|a| a == observed) {
                return outcome(
                    operation,
                    id,
                    CheckState::NotReady,
                    CheckCode::TargetNotAllowlisted,
                    now,
                )
                .with_message(&format!(
                    "cluster id `{observed}` is not in TrustRoster.spec.allowedClusterIds"
                ))
                .with_remedy(
                    "Add the scratch cluster's id to `TrustRoster.spec.allowedClusterIds`. The \
                     allowlist is read from the roster and never from a plan.",
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
            // D2 §6.3: a SOURCE whose id is on the restore allowlist is a
            // scratch cluster, and the runner's phase −1 rail refuses to back
            // one up. Reporting it here is how an operator learns before the
            // run rather than from an exit code.
            //
            // IT IS A `Backup` VERDICT AND ONLY A `Backup` VERDICT (review
            // F4). A `SourceConnection` check asks whether the connection
            // answers; whether the cluster may be backed up is a different
            // question, asked by a different operation, and the phase −1 rail
            // it cites guards a run this operation does not describe. Reported
            // here it made a successful dial to a legitimate restore target
            // read `not ready`, under a remedy — "Back up the production
            // cluster, not the scratch target" — advising about a backup
            // nobody had asked for. `ClusterIdentityChanged` above stays
            // blocking for both: dialling a DIFFERENT cluster than the object
            // records is a fact about the connection itself.
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
                    "cluster id `{observed}` is listed in TrustRoster.spec.allowedClusterIds, \
                     which marks it a restore TARGET; the runner's phase -1 rail refuses to \
                     back one up"
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

// ---------------------------------------------------------------------------
// The restore-only controller rows
// ---------------------------------------------------------------------------

/// The restore plan, reduced to the facts the three `plan.*` rows read.
///
/// NO `PartialEq`: `logweir_core::spec::DrillSpec` has none, and deriving one
/// here would mean comparing plans by a structural equality the pure crate
/// deliberately does not define. What identifies a plan is
/// [`PlanFacts::recomputed_hash`], which is what `binding.planHash` carries.
#[derive(Clone, Debug)]
pub struct PlanFacts {
    /// The verbatim bytes. Never re-serialised: `planHash` binds THESE.
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

    /// Whether the target mode is `scratch` — the mode the allowlist and the
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
/// `check_contract::redact`'s `long-base64-or-hex-run` rule replaces any hex
/// run of 40 characters or more — which a `sha256:<64 hex>` is — so a message
/// quoting a whole digest reaches a status as `[redacted]`. That rule is right
/// and is not weakened here; the message carries the first twelve hex
/// characters, which is enough for a human to tell two plans apart, and the
/// WHOLE digest is in `status.binding.planHash`, where nothing redacts it
/// because it is a field and not prose.
#[must_use]
pub fn short_digest(digest: &str) -> String {
    match digest.split_once(':') {
        Some((algo, hex)) if hex.len() > 12 => format!("{algo}:{}…", &hex[..12]),
        _ => digest.to_string(),
    }
}

/// `plan.parse` — D2 §6.3, and the controller's half of it.
///
/// THE HASH IS RECOMPUTED HERE AND THE CLAIM IS NEVER TRUSTED. `spec.request
/// .restore.planHash` is the requester's assertion about bytes it also
/// supplied; a preflight that took it on faith would bind its verdict to a
/// hash that names some other document, and W12's applicability test compares
/// exactly that field.
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
/// Reviewer finding **F7**: `redact`'s `long-base64-or-hex-run` rule replaces
/// any run of 40+ characters from `[A-Za-z0-9+/=_-]`, and a Kafka topic name's
/// only run-breaking character is `.`. So the exact case a
/// `MappedTopicNameIllegal` row is about — a name that is too long — is the
/// case whose name `with_message` eats. `entry_of` copies `scope` VERBATIM, and
/// a topic name is not credential-shaped, so the name goes there and the prose
/// keeps its (possibly redacted) copy.
#[must_use]
pub fn topic_scope(name: &str) -> CheckScope {
    CheckScope {
        kind: "Topic".to_string(),
        name: name.to_string(),
        uid: None,
    }
}

/// `plan.names` — D2 §6.3's four `notReady` codes, over the MAPPED names.
///
/// The rules are phase 0's, applied to the plan a restore would submit rather
/// than to the document a running pod already holds. Phase 0 still runs; this
/// is the same arithmetic reported before the pod exists, which is the whole
/// point of a preflight.
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
    // `$` is refused in BOTH halves: the engine's config loader expands
    // `${NAME}` in pre-parse text, so a name that can name an environment
    // variable is a name that can read one.
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
    if prefix.is_empty() {
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
}

/// `plan.bindings` — D2 §6.3.
///
/// EVERY MISMATCH IS A DIFFERENT REMEDY, which is why the four codes are four
/// codes: a plan pointed at the wrong bucket is an edit to the plan, a plan
/// pointed at the wrong broker is an edit to the target, and a plan naming a
/// topic the recovery point never captured is a choice of a different recovery
/// point.
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

/// The recovery point, reduced — PLAT-11.1's identity is a `Backup` UID.
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
        /// Its UID — PLAT-11.1's identity.
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

/// `recoveryPoint.state` — D2 §6.3, bound to a `Backup` UID (PLAT-11.1).
///
/// # A same-named replacement is not the same recovery point
///
/// `RecoveryPointUidChanged` exists because a `Backup` deleted and recreated
/// under the same name is a different run over a different window. The UID is
/// what PLAT-11.1 fixes as the identity, and it is in the binding digest, so a
/// recreated recovery point also makes every earlier preflight stale.
///
/// # The location comparison, and its three answers
///
/// A recovery point is only restorable from the destination it was WRITTEN to
/// (D2 §3.12). The point's frozen `locationDigest` — `Backup.status.destination`
/// — is compared with the digest this check resolved for the source
/// destination:
///
/// * both present and equal → `RecoveryPointSucceeded`;
/// * both present and different → `RecoveryPointLocationMismatch`, with both
///   digests in the message;
/// * the point has none and the check resolved one → `unknown` with
///   `RecoveryPointLocationUnknown`. A point archived before saved destinations
///   existed publishes no location, and a blocking row that answered `ready`
///   there would be reporting a comparison nobody made.
///
/// Neither digest is recomputed from a live object: a destination edited after
/// a run froze must not be able to make a moved recovery point look settled.
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
                // TWO LOCATIONS, AND THEY ARE NOT THE SAME ONE. Both digests
                // are in the message: an operator holding two destinations
                // needs to know WHICH of them this point is in, and a sentence
                // that only says "different" sends them to compare two objects
                // by hand.
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
                // A LEGACY POINT UNDER A DESTINATION-BACKED CHECK. The Backup
                // publishes no frozen destination, so this row cannot say the
                // location agrees — and saying `ready` would be the check
                // claiming a comparison it never made. `unknown` is the honest
                // answer and, as a blocking row, it holds the verdict at
                // `unknown` until somebody confirms the location by hand.
                //
                // The mirror case — a point WITH a digest under a check that
                // resolved no source destination — stays `ready` here: that
                // preflight makes no location claim to compare against, and
                // `plan.bindings` is the row that holds the legacy plan's
                // location to account (D2 §3.12).
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

/// The `Approval`, reduced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApprovalFacts {
    /// A DRAFT. D2 §6.3: `approval.state` is `skipped` with
    /// [`CheckCode::SubjectNotCreated`] — there is no subject to approve yet,
    /// and that is a real answer rather than a failure to decide.
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
        /// Its `metadata.resourceVersion`, for the binding — it MOVES when
        /// verification status moves, which is what makes a preflight taken
        /// while an approval was pending go stale the moment it is verified.
        resource_version: String,
        /// `status.verified`.
        verified: Option<bool>,
        /// The `Verified` condition's REASON — a `ApprovalRefusal::reason()`
        /// token such as `KeyIdExpired`, and the thing this row ROUTES on.
        ///
        /// IT USED TO BE THE MESSAGE OR THE REASON, WHICHEVER EXISTED, and
        /// that conflation is half of PREFLIGHT-APPROVAL-ROSTER: a router
        /// cannot branch on a sentence, so this row had nothing to branch on
        /// and re-derived the verdict from the roster instead.
        reason: Option<String>,
        /// The `Verified` condition's MESSAGE, which is what an operator
        /// reads. Never routed on.
        message: Option<String>,
        /// The roster key id that verified it.
        matched_key_id: Option<String>,
        /// `status.approverKeyWindow` — the matched approver key's declared
        /// validity window, as the **Approval controller** published it.
        ///
        /// THE ONLY WINDOW THIS FILE MAY READ. It is not resolved here and it
        /// is not the roster's: `controllers::approval` resolved the key
        /// through the `TrustPolicy` that governs the namespace and wrote what
        /// it found, which is the same authority the verdict beside it came
        /// from. `None` is the honest answer of an `Approval` that matched no
        /// key, or of a controller that predates the field — and `None` is
        /// UNKNOWN, never valid.
        ///
        /// BOXED because `Found` is already the large variant of this enum and
        /// a fourth inline field tipped `clippy::large_enum_variant`: every
        /// `ApprovalFacts` in a reduced-inputs struct would otherwise carry the
        /// window's bytes whether or not there is one.
        approver_key_window: Option<Box<ApproverKeyWindow>>,
        /// The `plan_hash` inside the approval's own signed bytes.
        approved_plan_hash: Option<String>,
        /// The `Restore` the approval names.
        subject_name: Option<String>,
    },
}

/// The `Verified` condition reason that means the approver key had passed its
/// `notAfter` when the Approval controller last looked.
///
/// ONE SPELLING, AND IT IS THE CONTROLLER'S.
/// `the_expiry_reason_is_the_one_the_approval_controller_writes` pins it
/// against `ApprovalRefusal::KeyIdExpired`'s own `reason()`, so a rename there
/// is a red test here rather than a preflight that silently stops recognising
/// an expiry.
pub const APPROVAL_REASON_KEY_ID_EXPIRED: &str = "KeyIdExpired";

/// `approval.state` and `approval.keyValidity` — D2 §6.3's two rows.
///
/// # This row reads the Approval's verdict and never re-derives it
///
/// Defect **PREFLIGHT-APPROVAL-ROSTER**. It used to resolve the approver key
/// and its `notAfter` out of `TrustRoster/default` and decide expiry here,
/// while `controllers::approval` resolves the same key through the **trust
/// policy** that governs the namespace (D3 W10). Those are two different
/// authorities and they disagree: on `lab-refresh-4` an isolated `TrustPolicy`
/// carrying an expiring approver key moved the Approval to
/// `Verified=False, KeyIdExpired` while this row, reading the roster, still
/// said `ready`. A preflight that authorises what the controller has already
/// refused is the one direction this kind may never fail in — and the roster
/// being immutable is also why PLAT-03.2's `expired approval` test could not
/// be built without recreating a shared trust anchor.
///
/// So the verdict comes from the Approval: its `Verified` condition, that
/// condition's REASON, and `status.matchedKeyId`. **`approval_rows` is not
/// given a [`RosterFacts`] at all**, which is what makes "does not read the
/// roster" a property of the signature rather than of a comment;
/// `the_approval_rows_are_not_given_the_roster` holds the shape.
///
/// # The key's WINDOW comes from the Approval too — and only from there
///
/// Defect **APPROVAL-KEY-WINDOW-UNPUBLISHED**, the deliberate regression the
/// fix above created and recorded. Two D2 §6.3 behaviours are about the KEY and
/// not about the verdict — `ApproverKeyExpiresBeforeDeadline` and the
/// `min(10 m, notAfter)` re-check cap on both rows — and both need a window
/// this file may no longer resolve. `controllers::approval` now publishes it
/// (`status.approverKeyWindow`: the matched key's `keyId`, `notBefore` and
/// `notAfter`, as the RESOLVED TRUST POLICY declares them), so the two return
/// reading the same authority the verdict does.
///
/// `deadline` is the instant the restore would still need the approver key to
/// be valid at. `ApproverKeyExpiresBeforeDeadline` is ADVISORY, which is D2
/// §6.3's own `Gate` column for `approval.keyValidity` (`A`) and what §6.4
/// spells out — *"Advisory `notReady` appears as warnings"* — so this is a
/// warning ahead of time and not a refusal: a key that expires mid-run does not
/// invalidate an approval that verified while it was valid, and the blocking
/// `approval.state` row is where a withdrawn verdict refuses.
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
            ..
        } => {
            let scope = CheckScope {
                kind: "Approval".to_string(),
                name: name.clone(),
                uid: Some(uid.clone()),
            };
            // NO KEY WINDOW IS DERIVED HERE. It is READ BACK from the
            // `Approval`, which resolved it through the trust policy that
            // governs the namespace — the roster this file used to walk is not
            // that authority (see the type-level note).
            //
            // AND THE WINDOW MUST NAME THE KEY THE VERDICT NAMES. A window
            // beside a key id is two fields, and two fields can disagree: a
            // merge patch that lands half an update, or a controller that
            // published one key's window beside another key's verdict, would
            // otherwise have this row compare a deadline against a window that
            // is about nothing it read. Disagreement is `None`, which is
            // unknown — never valid.
            let window = matched_window(matched_key_id.as_deref(), approver_key_window.as_deref());
            let not_after = window.map(|w| w.not_after);
            let mut out = Vec::new();
            let state = if let Some(subject) = subject_name.as_deref() {
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
            // `min(10 m, notAfter)` ON BOTH ROWS — D2 §6.3's `Expiry` column
            // for `approval.state` and for `approval.keyValidity` alike. A
            // verdict about a key must not outlive the key: a preflight that
            // stayed fresh for ten minutes past a `notAfter` four minutes away
            // would let a restore be admitted on a window that closed while the
            // record still said `ready`.
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
/// else.
///
/// THE ORDER IS PLAN HASH, THEN EXPIRY, THEN VERIFIED, and it is unchanged by
/// PREFLIGHT-APPROVAL-ROSTER — only the SOURCE of the expiry answer moved.
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
    // THE HASH FIRST. An approval that verified against a DIFFERENT plan is a
    // stronger and more actionable finding than "not verified yet", and
    // reporting the plan mismatch as a pending approval sends an operator to
    // wait for something that has already happened.
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
    // THE EXPIRY IS THE CONTROLLER'S, VERBATIM. `KeyIdExpired` is the reason
    // `controllers::approval` writes when the key that verified an approval is
    // past the `notAfter` of the entry the RESOLVED TRUST POLICY carries for
    // it. Deciding it here from `TrustRoster/default` was the defect: a policy
    // can expire a key the roster still shows as open, and this row then
    // authorised what the controller had already refused.
    if reason.is_some_and(|r| r == APPROVAL_REASON_KEY_ID_EXPIRED) {
        return mk(CheckState::NotReady, CheckCode::ApprovalExpired)
            .with_scope(scope.clone())
            // THE CONTROLLER'S OWN SENTENCE when it wrote one: it names the key
            // id and the `notAfter` that passed, which this row no longer
            // resolves and must not invent.
            .with_message(
                message.unwrap_or("the approver key that verified this approval has expired"),
            )
            .with_remedy("Have the restore re-approved with a current approver key.");
    }
    match verified {
        Some(true) => mk(CheckState::Ready, CheckCode::ApprovalVerified)
            .with_scope(scope.clone())
            .with_message("the Approval's Verified condition is True"),
        // EVERY OTHER REFUSAL IS `ApprovalNotVerified` CARRYING THE
        // CONTROLLER'S OWN WORDS — including `KeyRetired`, `KeyRevoked` and
        // `KeyNotYetValid`, which are deliberately NOT folded into
        // `ApprovalExpired`. `controllers::approval` is explicit that a
        // revocation reported as an expiry sends an operator to extend a
        // window when the remedy is an investigation, and a retirement has no
        // `notAfter` to extend at all. The closed check vocabulary has one
        // code for "did not verify"; the reason and the message carry which.
        Some(false) => mk(CheckState::NotReady, CheckCode::ApprovalNotVerified)
            .with_scope(scope.clone())
            .with_message(message.or(reason).unwrap_or("the Approval did not verify"))
            .with_remedy("Read the Approval's own Verified condition for the exact refusal."),
        // NO STATUS YET IS NOT A REFUSAL. The approval controller has not
        // reconciled it, which is a wait and not a verdict — D2 §6.3's
        // `ApprovalPending`.
        None => mk(CheckState::Unknown, CheckCode::ApprovalPending)
            .with_scope(scope.clone())
            .with_message("the Approval has no verification status yet"),
    }
}

/// The published window, but only when it is about the key the verdict names.
///
/// `controllers::approval` writes the `keyId` INSIDE the window for this
/// comparison. A window that names another key is not a window about this
/// approval, and comparing a deadline against it would be worse than having
/// none: it would be a green advisory row grounded in someone else's rotation
/// schedule.
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

/// `approval.keyValidity` — D2 §6.3's advisory row, reading the window the
/// `Approval` publishes.
///
/// # Three answers, and the third is the one that matters
///
/// * the window is published and its `notAfter` falls **before** the restore's
///   deadline — `notReady`/`ApproverKeyExpiresBeforeDeadline`, D2 §6.3's own
///   condition (`notAfter` < now + `deadlineSeconds`). ADVISORY: §6.3 gates
///   this row `A` and §6.4 says *"Advisory `notReady` appears as warnings"*, so
///   it warns ahead of time and never refuses. The refusal, if the key does
///   expire, is the blocking `approval.state` row relaying `KeyIdExpired` —
///   which is a fact and not a forecast.
/// * the window is published and the deadline falls inside it —
///   `ready`/`ApproverKeyValid`.
/// * **no window is published, or the one published is about another key —
///   `unknown`/`ApproverKeyWindowUnknown`, NEVER `ready`.** PLAT-19.1's
///   acceptance: unevaluated or stale expiry information is unknown, not valid.
///   An `Approval` that matched no key has no window to publish and its verdict
///   is already `notReady` on the blocking row; a controller image that
///   predates `status.approverKeyWindow` publishes none either, and during that
///   upgrade window this row says so rather than inventing a pass. `unknown` on
///   an ADVISORY row does not make the aggregate `unknown` (D2 §6.4 aggregates
///   over blocking checks), so honesty here costs a reader nothing but a green
///   badge it was not entitled to.
///
/// # Why this is not derived from `Verified=True` alone
///
/// A verified approval says the key was usable when the controller last looked.
/// This row asks a different question — will it still be usable at the end of a
/// restore that has not started — and "it verified" is not an answer to it.
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
        // THE COMPARISON IS `<`, AND THE DIRECTION IS D2 §6.3's: the key
        // expires BEFORE the deadline. A key whose `notAfter` is exactly the
        // deadline is not early, and a `>` here would warn about every key that
        // outlasts the restore — the one row an operator would learn to ignore.
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
        // NO DEADLINE, SO NO COMPARISON — AND THE SENTENCE SAYS SO. This arm
        // used to share the one above and claim "this restore's deadline falls
        // inside it" about a comparison that never happened. It is reachable:
        // `deadline` is `now + spec.deadlineSeconds` and `deadlineSeconds` is
        // an unbounded `int64` in the CRD, so an absurd value overflows the
        // addition to `None` and silences the warning for a key that expires in
        // a minute. The verdict is unchanged — an advisory row cannot refuse,
        // and the blocking `approval.state` row already reports a WITHDRAWN
        // verdict — but a green row must not describe work it did not do.
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
// The pod-status rows, and what an unattributable waiting code does
// ---------------------------------------------------------------------------

/// Which check a waiting code belongs to, ADJUSTED for the operation.
///
/// [`check::attribute`] answers in the `connection.*` vocabulary because that
/// is the framework's neutral spelling. D2 §6.3 gives a restore the TARGET
/// equivalents, and a restore that reported `connection.credentialProjected`
/// would name a row its own catalogue does not contain.
#[must_use]
pub fn attribute_for(
    operation: PreflightOperation,
    waiting: &check::Waiting,
    projections: &check::Projections,
) -> Option<CheckId> {
    let id = check::attribute(waiting, projections)?;
    Some(match (operation, id) {
        (PreflightOperation::Restore, CheckId::ConnectionCredentialProjected) => {
            CheckId::TargetCredentialProjected
        }
        (_, other) => other,
    })
}

/// The **P** rows, plus whether every Job-sourced row is blocked.
///
/// # Why "blocked" is a second return value and not a state on a row
///
/// D2 §4.3: "An unmatched code blocks the whole pod: every Job-sourced check
/// becomes `unknown` with `BlockedByPrerequisite`, and the named cause is
/// reported." That is a statement about the OTHER rows, so it cannot live on
/// any one of them — and a caller that aggregated the pod rows alone would
/// report `ready` for a pod that never started.
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
        // A VERIFIED RELAY IS THE PROOF. The runner wrote framed output from
        // inside the pod, so the image pulled, the pod started and every
        // projection the kubelet was asked for succeeded — those are not
        // inferences, they are preconditions of the bytes being there.
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
    // REVIEWER QUESTION Q1. The waiting state is still REPORTED below — a pod
    // that was disrupted after it wrote its frames is a fact an operator wants
    // — but a VERIFIED RELAY IS NEVER DISCARDED. `check::observe` is not
    // believed to produce both in one pass; this is the safe direction anyway,
    // because throwing away a result that exists and replacing it with
    // `BlockedByPrerequisite` placeholders is strictly worse than reporting
    // both, and `blocked` is exactly "drop every relayed row".
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
        // THE CAUSE IS NAMED ON `runner.pod` RATHER THAN ATTRIBUTED TO A GUESS.
        // A code that names a Secret this plan did not project is deliberately
        // attributed to nothing (`check::attribute`), because sending an
        // operator to rotate the wrong credential is worse than telling them
        // the pod did not start and why.
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

// ---------------------------------------------------------------------------
// Assembly and the verdict
// ---------------------------------------------------------------------------

/// Put one pass's findings together — **pure**, and the ONE place the verdict's
/// row set is decided.
///
/// 1. The controller's own rows, first, because a **C** row is the controller's
///    answer by D2 §6.3's legend and a relayed duplicate would be a second
///    answer with one id.
/// 2. The relayed rows, for every id the controller did not answer.
/// 3. Every row `expected` names that nobody answered, as `unknown` with
///    [`CheckCode::BlockedByPrerequisite`] — never absent, because an absent
///    blocking row is a row that cannot hold the verdict back.
/// 4. The rows the REQUEST asked to skip, as `skipped`, which D2 §6.2 keeps the
///    overall verdict `unknown` for: skipping a question is not answering it.
///
/// `expected` is [`job_rows`] over the RENDERED plan, or
/// [`unrendered_job_rows`] when no plan could be rendered. It is a parameter
/// and not a lookup here so that the one rule that has to match the runner's
/// emission lives in one function with one guard (reviewer finding **F1**).
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
                // A row nobody answered is BLOCKING, whatever the runner's own
                // catalogue would have made it: this is the case where the
                // verdict must not be `ready`, and an advisory placeholder
                // would let it be.
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
                    // THE CLOSED VOCABULARY HAS NO "THE REQUESTER ASKED ME NOT
                    // TO" SPELLING, and `logweir-core` is not this task's file
                    // to add one to. `BlockedByPrerequisite` is the vocabulary's
                    // "this row has no answer, and here is what blocked it";
                    // the prerequisite here is the request's own `skipChecks`,
                    // and the `skipped` STATE is what tells the two apart.
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
/// One per AUTHORITY, because the authority IS the statement about who
/// observed the thing: `podStatus` is the kubelet reporting on a Pod,
/// `checkJob` is the runner reporting from inside a Job, and `controller` is
/// this process reporting about the `Preflight` it is reconciling. A row that
/// knows a better referent — the `KafkaCluster` a connection dialled, the
/// `BackupDestination` a grant belongs to, the `TrustRoster` a key is or is not
/// on — sets its own and keeps it.
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
/// D2 §6.3's acceptance sentence is "the UI names each failed prerequisite and
/// its remedy, with check time **and scope**", and the live run found six
/// `notReady` rows with `scope: null` — the `podStatus` credential rows, the
/// `checkJob` connection rows and the `controller` `signer.rostered` rows
/// (`objects/s14/notready-rows.json`). Those are exactly the rows an operator
/// reads, and "which object is this about" was blank on all of them.
///
/// It runs AFTER [`assemble`] rather than inside it because `assemble` decides
/// which rows exist and this decides what a row that exists is about; and
/// because [`assemble`]'s own placeholders — the rows nobody answered and the
/// rows the request skipped — need a referent too, and they are built there.
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
/// # Facts and bounded detail are folded into `message`
///
/// [`CheckOutcome`] carries `facts` (`clusterId`, `brokerCount`, `imageID`,
/// `signerKeyId`) and a bounded `detail`; the shipped `Preflight` CRD carries
/// NEITHER, deliberately — `detail` is free-form JSON, which in a structural
/// schema needs `x-kubernetes-preserve-unknown-fields` and turns a status into
/// a place arbitrary bytes can be parked (`crds/preflight.rs` records the
/// reason). Dropping them silently would lose the one non-secret fact an
/// operator most often needs — WHICH cluster, WHICH key — so they are rendered
/// into the message, deterministically ordered, and the whole thing is redacted
/// and capped again on the way out. The long form lives in the details
/// `ConfigMap` that `result.detailsRef` names.
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

/// How many rows `status.result.checks` may hold — the shipped CRD's own
/// `maxItems`, restated here because the API server enforces it with a 422.
pub const MAX_PUBLISHED_CHECKS: usize = logweir_core::check_contract::MAX_CHECK_ENTRIES;

/// The aggregated verdict — D2 §6.4, and nothing this module decides itself.
///
/// Returns the result and **how many rows did not fit**, which the caller puts
/// in `status.message`.
///
/// # The verdict is computed over EVERY row, and only the list is capped
///
/// Reviewer finding **F8**: `CheckResult::validate` caps the RELAY at 64 rows,
/// and `assemble` then adds up to sixteen controller and pod rows on top. The
/// shipped CRD declares `maxItems: 64` on `status.result.checks`, so the merge
/// patch was a 422 the reconciler could only requeue on — the verdict was never
/// published at all.
///
/// Capping is therefore done here, and `aggregate` / `aggregate_expires_at` run
/// over the WHOLE set first: a truncation that could change the verdict would
/// be a way to publish `ready` by producing more rows. What the cap decides is
/// which rows an operator gets to READ, so the order is the order of
/// usefulness — blocking rows that are not `ready` first, because those are the
/// ones that explain the verdict.
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
    // STABLE, so the id order `assemble` established survives inside a rank.
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

// ---------------------------------------------------------------------------
// The binding, and what makes a stored verdict stop applying
// ---------------------------------------------------------------------------

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

/// Why a RECORDED verdict no longer applies, in D2 §6.6's own spellings.
///
/// # This is the controller's half of an API-side test, and it is not a copy
///
/// `logweir_core::check_contract::stale_reasons` compares two whole
/// [`BindingInputs`], which is what W12 has: it recomputes one from live
/// objects and holds the other from the status. A CONTROLLER holds the recorded
/// side only as `status.binding`, which is deliberately narrower — the CRD
/// stores the digest, the plan hash, the referents and the policy digest, and
/// not the CA bundle list or the approval's resource version. So this compares
/// what the status actually carries and leans on the DIGEST for everything
/// else: `inputsDigest` is computed over all of it, so a change the four named
/// comparisons cannot see still surfaces as
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
    // THE DIGEST IS THE AUTHORITY ON "did anything change", and it is checked
    // last so that a difference the named comparisons cannot see is still
    // reported rather than swallowed.
    let digests_differ = recorded.inputs_digest.as_deref() != Some(inputs_digest(current).as_str());
    let only_expiry = out.iter().all(|r| *r == StaleReason::Expired);
    if digests_differ && only_expiry {
        out.push(StaleReason::InputsDigestChanged);
    }
    out
}

/// Downgrade a stored verdict that no longer applies — **pure**.
///
/// `ready` becomes `unknown`. Not `notReady`: nothing was found wrong, the
/// answer simply stopped being about the current objects, and "this operation
/// will fail" is a different and stronger claim than "I no longer know".
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

// ---------------------------------------------------------------------------
// The status body
// ---------------------------------------------------------------------------

/// Everything one status write carries.
#[derive(Clone, Debug, Default)]
pub struct StatusInput {
    /// One of [`PHASES`].
    pub phase: String,
    /// The scalar reason, always a closed code.
    pub reason: Option<CheckCode>,
    /// A redacted, bounded explanation.
    pub message: String,
    /// What this verdict is about. Recorded BEFORE the Job is created.
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
/// The two conditions are D2 §6.2's: `Complete` says whether a result exists at
/// all, and `Ready` restates the verdict. **`Ready` is `Unknown` and never
/// `False` for an `unknown` verdict** — a UI that rendered "not ready" for "I
/// could not tell" would turn every skipped or blocked check into a refusal.
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
        // A BINDING IS NEVER UNWRITTEN. A later pass that could not resolve the
        // referents keeps the one the Job was created against: the alternative
        // is a verdict whose binding vanished, which is the one state W12
        // cannot tell apart from "never bound".
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

// ---------------------------------------------------------------------------
// Resolution: everything one pass reads from the API server, reduced
// ---------------------------------------------------------------------------

/// The subject's own facts plus every referent, reduced to what the rows and
/// the binding read.
///
/// ONE STRUCT AND ONE RESOLUTION PASS, because the binding must be computed
/// from exactly the objects the checks were computed from. Two walks would be
/// two readings of a cluster that moves.
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
    /// readiness marker (`spec.readiness.writeProbe: CreateOnlyMarker`).
    ///
    /// READ FROM THE OBJECT, never assumed. It used to be hard-`false`, which
    /// gave an operator who had opted in a `WriteNotProbed` row whose message
    /// was false about their own spec (reviewer finding **F3**).
    pub write_probe: bool,
    /// A backup check's topic set.
    pub topics: Vec<String>,
    /// A restore check's plan.
    pub plan: Option<PlanFacts>,
    /// What `plan.bindings` compares against.
    pub bindings: BindingFacts,
    /// The recovery point.
    pub recovery_point: RecoveryPointFacts,
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
    /// An inline archive, which this build cannot render into a check plan.
    pub legacy_archive: Option<String>,
}

impl Default for Inputs {
    fn default() -> Self {
        Self {
            // A DEFAULT HAS TO NAME ONE, AND NAMING `Backup` IS NOT A CLAIM.
            // `PreflightOperation` has no `Default` on purpose — the operation
            // is required on the wire — so this exists for tests and for
            // `..Inputs::default()`, and every real construction sets it from
            // `spec.request.operation` on the line above.
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
            topics: Vec::new(),
            plan: None,
            bindings: BindingFacts::default(),
            recovery_point: RecoveryPointFacts::NotRequested,
            approval: ApprovalFacts::Draft,
            approval_binding: None,
            restore_name: None,
            deadline: None,
            backup_id: String::new(),
            source_cluster_id: None,
            referents: Vec::new(),
            ca_bundles: Vec::new(),
            legacy_archive: None,
        }
    }
}

/// What the relay told the controller about facts only the pod could see.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RelayFacts {
    /// The broker's cluster id, from the `clusterId` fact.
    pub cluster_id: Option<String>,
    /// The runner's PUBLIC signing key id, from the `signerKeyId` fact.
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
    /// The binding this verdict is about — D2 §6.6.
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

    /// Whether this operation's pod needs the signing key.
    ///
    /// A DESTINATION-ACCESS CHECK DOES NOT, and neither does a
    /// SOURCE-CONNECTION CHECK. That is the point of the distinction: one
    /// exercises grants and the other dials a broker, so projecting a signing
    /// key into either would widen the pod's blast radius past the question
    /// being asked. Only the two operations that would really sign something —
    /// a backup's receipt, a restore's evidence — get the key.
    #[must_use]
    pub fn signs(&self) -> bool {
        matches!(
            self.operation,
            PreflightOperation::Backup | PreflightOperation::Restore
        )
    }

    /// `destination.resolved`, over BOTH destinations — see the call site.
    ///
    /// `None` when this operation names no destination at all.
    #[must_use]
    pub fn destination_verdict_row(&self, now: DateTime<Utc>) -> Option<CheckOutcome> {
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
        // THE CONNECTION ROWS BELONG TO THE OPERATIONS THAT DIAL, and the
        // SIGNER ROW to the ones that sign. They used to be one `if`, because
        // `DestinationAccess` was the only operation that did neither;
        // `SourceConnection` dials and does not sign, which is what splits
        // them. Pushing `signer.rostered` for it would have published a
        // BLOCKING controller row about a key the pod was never given.
        if op != PreflightOperation::DestinationAccess {
            out.push(connection_row(
                op,
                self.connection.as_ref(),
                self.cluster_name.as_deref().unwrap_or_default(),
                now,
            ));
            out.push(cluster_identity_row(
                op,
                facts.cluster_id.as_deref(),
                self.cluster_recorded_id.as_deref(),
                &self.roster.allowed_cluster_ids,
                self.source_cluster_id.as_deref(),
                self.plan.as_ref().is_some_and(PlanFacts::scratch),
                now,
            ));
        }
        if self.signs() {
            out.push(signer_rostered_row(
                op,
                &self.roster,
                facts.signer_key_id.as_deref(),
                now,
            ));
        }
        // F9: ONE `destination.resolved` ID, TWO OBJECTS. A restore names a
        // source destination and an evidence destination, and the catalogue
        // has a single row id for both. Reporting only the archive's verdict
        // gave an operator a GREEN row about the destination that was fine and
        // no cause at all for the one that was not. The row therefore carries
        // the first REFUSAL, scoped to the object that produced it, and falls
        // back to the archive when both resolve.
        if let Some(row) = self.destination_verdict_row(now) {
            out.push(row);
        }
        if op == PreflightOperation::Restore {
            if let Some(plan) = self.plan.as_ref() {
                out.push(plan_parse_row(plan, now));
                out.push(plan_names_row(plan, now));
                out.push(plan_bindings_row(plan, &self.bindings, now));
            }
            if let Some(row) = recovery_point_row(&self.recovery_point, now) {
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
    /// EVERY OTHER REFUSAL STILL RUNS THE JOB. A preflight exists to report
    /// every problem at once: a restore whose approval has not been signed yet
    /// is exactly the case D2 §6.2 names (`restoreRef` pointing at a `Restore`
    /// awaiting an approver), and refusing to check the archive because of it
    /// would make the preflight useless precisely when it is most wanted.
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
                    )
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Rendering the check plan and the Job
// ---------------------------------------------------------------------------

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
    /// The plan document, as a value — so the caller can derive the rows the
    /// runner will emit ([`job_rows`]) from the REQUEST it actually rendered
    /// rather than from the operation (reviewer finding **F1**).
    pub plan: CheckPlan,
}

fn connection_plan(c: &ResolvedConnection) -> ConnectionPlan {
    let side = c.execution.side;
    ConnectionPlan {
        bootstrap_servers: c.bootstrap_servers.clone(),
        auth_mode: c.auth.mode_str().to_string(),
        username: c.auth.username().map(str::to_string),
        // THE NAME, NEVER THE VALUE. The password reaches the pod as a
        // `secretKeyRef` the kubelet projects; the plan says which variable to
        // read and the controller never holds the bytes.
        password_env: c.password.as_ref().map(|_| side.password_env().to_string()),
        tls: Some(c.tls()),
        // AN OPAQUE IN-POD PATH, and deliberately not rendered bytes: a
        // `KafkaCluster` may name its CA in a Secret, and this controller holds
        // no verb on `secrets`. The check pod gets the SAME mount an execution
        // pod gets, and the runner passes the path to librdkafka without
        // opening it.
        ca_file: c.tls_ca.as_ref().map(|_| side.ca_file_path()),
        principal: c.principal.clone(),
    }
}

fn destination_plan(d: &ResolvedDestination, ca_file: Option<&str>) -> DestinationPlan {
    DestinationPlan {
        name: d.name.clone(),
        uid: d.uid.clone(),
        location: d.location.clone(),
        location_digest: d.location_digest.clone(),
        // The DESTINATION side is the one place a CA is rendered as BYTES:
        // `StoreOptions::with_root_certificate` takes PEM, not a path.
        ca_file: d.ca_pem.as_ref().and(ca_file).map(str::to_string),
        credentials: match &d.grant {
            destination::ResolvedGrant::SecretKeys { .. } => CredentialMode::Static,
            destination::ResolvedGrant::WorkloadIdentity { .. } => CredentialMode::WorkloadIdentity,
            destination::ResolvedGrant::ControllerIdentity
            | destination::ResolvedGrant::NotConfigured => CredentialMode::Ambient,
        },
    }
}

/// Render one check's plan documents and Job — **pure**.
///
/// # Errors
///
/// A sentence naming what could not be rendered. Every one of them is a
/// programming error or a refusal the caller already reported as a row; it is a
/// `String` rather than a code because the caller turns it into
/// `phase: Failed`, which D2 §6.2 defines as "the check could not produce a
/// result".
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

    let request = match operation {
        PreflightOperation::Backup => {
            let c = connection.ok_or("the source connection did not resolve")?;
            let d = archive.ok_or("the destination did not resolve")?;
            CheckRequest::OperationReadiness(Box::new(OperationReadinessRequest {
                operation: CheckOperation::Backup,
                connection: connection_plan(c),
                destination: Some(destination_plan(d, Some(ARCHIVE_CA_PATH))),
                roles: inputs.roles.clone(),
                topics: inputs.topics.clone(),
                signer_path: Some(crate::backup_execution::SIGNING_KEY_PATH.to_string()),
                // The create-only marker is the ONLY key a check may ever
                // write, and it is opt-in ON THE DESTINATION — read from
                // `spec.readiness.writeProbe`, never assumed (reviewer finding
                // F3).
                write_probe: inputs.write_probe,
                skip_checks: skip.clone(),
            }))
        }
        PreflightOperation::SourceConnection => {
            let c = connection.ok_or("the source connection did not resolve")?;
            CheckRequest::SourceConnection(logweir_core::check_contract::SourceConnectionRequest {
                connection: connection_plan(c),
            })
        }
        PreflightOperation::DestinationAccess => {
            let d = archive.ok_or("the destination did not resolve")?;
            CheckRequest::DestinationAccess(
                logweir_core::check_contract::DestinationAccessRequest {
                    destination: destination_plan(d, Some(ARCHIVE_CA_PATH)),
                    roles: inputs.roles.clone(),
                    write_probe: inputs.write_probe,
                },
            )
        }
        PreflightOperation::Restore => {
            let c = connection.ok_or("the target connection did not resolve")?;
            let d = archive.ok_or("the source destination did not resolve")?;
            let plan = inputs.plan.as_ref().ok_or("there is no restore plan")?;
            CheckRequest::RestorePreflight(Box::new(RestorePreflightRequest {
                plan_file: RESTORE_PLAN_PATH.to_string(),
                plan_sha256: plan.recomputed_hash.clone(),
                target: connection_plan(c),
                source_destination: destination_plan(d, Some(ARCHIVE_CA_PATH)),
                evidence_destination: evidence.map(|e| destination_plan(e, Some(EVIDENCE_CA_PATH))),
                backup_id: inputs.backup_id.clone(),
                // The manifest key is the archive's own convention; the runner
                // joins it to the destination's prefix.
                manifest_key: format!("{}/manifest.json", inputs.backup_id),
                // EMPTY MEANS "every row this kind owns". Naming them here
                // would be a second catalogue for the runner's own table to
                // drift from.
                checks: Vec::new(),
                skip_checks: skip.clone(),
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

    // --- the pod's shape: the execution pod's, with the check's plan ---
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
    if let Some(d) = archive {
        let env = d.job_env();
        env_literal.extend(env.literals);
        env_from_secret.extend(env.from_secret);
        if let Some(sa) = env.service_account_name {
            // A DESTINATION THAT DEMANDS ITS OWN ServiceAccount AND A
            // CONNECTION THAT DEMANDS ANOTHER IS A CONFLICT, NOT A CHOICE.
            // Picking one would run the check as a principal neither half was
            // validated against; the row reports it as
            // `ExecutionContextConflict`.
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
    };
    Ok(JobShape {
        documents,
        spec,
        plan,
    })
}

// ---------------------------------------------------------------------------
// Resolution against the API server
// ---------------------------------------------------------------------------

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
        },
    }
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

/// Everything one pass reads, in ONE walk — see [`Inputs`].
///
/// # Errors
///
/// [`ReconcileError`] for anything that is not a finding: a missing namespace,
/// or an API server that would not answer. A referent that does not exist is a
/// FINDING and comes back inside [`Inputs`].
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
    let roster = roster_facts(&roster_load);

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
    if inputs.roster.found {
        inputs.referents.push(referent(
            "TrustRoster",
            "",
            crate::ROSTER_NAME,
            &inputs.roster.uid,
            Some(inputs.roster.generation),
        ));
    }

    // --- the KafkaCluster, when this operation dials one --------------------
    let cluster_name = match request.operation {
        PreflightOperation::Backup => request.backup.as_ref().map(|b| b.source_ref.name.clone()),
        PreflightOperation::Restore => {
            let r = request.restore.as_ref();
            // `None` here is not "no target": it is DERIVED from the
            // `Restore` below, which a `restoreRef` request names instead.
            r.and_then(|r| r.target_ref.as_ref())
                .map(|t| t.name.clone())
        }
        PreflightOperation::SourceConnection => request
            .source_connection
            .as_ref()
            .map(|c| c.connection_ref.name.clone()),
        PreflightOperation::DestinationAccess => None,
    };

    // --- the Restore, for a restore bound to an existing subject ------------
    let mut restore_object: Option<Restore> = None;
    if let Some(r) = request.restore.as_ref() {
        if let Some(ref_) = r.restore_ref.as_ref() {
            let api: Api<Restore> = Api::namespaced(client.clone(), namespace);
            restore_object = api.get_opt(&ref_.name).await.map_err(ReconcileError::Api)?;
        }
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

    // --- the destinations ---------------------------------------------------
    let (archive_ref, evidence_ref, roles, legacy) = match request.operation {
        PreflightOperation::Backup => {
            let b = request.backup.as_ref();
            // THE ROLES DECIDE THE ROWS (reviewer finding **F1**). The runner
            // emits one destination row PER REQUESTED ROLE: `ArchiveRead` is
            // `destination.archiveListable`, `EvidenceWrite` is
            // `destination.evidenceWritable`, `EvidenceRead` is
            // `destination.evidenceReadable`. `ArchiveWrite` was requested here
            // and `ArchiveRead` was not, so D2 §6.3's BLOCKING
            // `destination.archiveListable` row was never emitted and never
            // could be — and `assemble` then held the verdict at `unknown` for
            // ever. `EvidenceRead` is added below, but only when the
            // destination configures that grant.
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
                r.and_then(|r| r.legacy_source_archive.as_ref().map(|a| a.url.clone())),
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
        // NO DESTINATION AND NO ROLE. Every `None` here is a question this
        // operation does not ask, so nothing is resolved, nothing is projected
        // and no `destination.*` row is expected of the Job.
        PreflightOperation::SourceConnection => (None, None, Vec::new(), None),
    };
    inputs.roles = roles;
    inputs.legacy_archive = legacy;
    let archive_role = match request.operation {
        PreflightOperation::Restore => DestinationRole::ArchiveRead,
        PreflightOperation::DestinationAccess => inputs
            .roles
            .iter()
            .copied()
            .min_by_key(|r| {
                DestinationRole::ALL
                    .iter()
                    .position(|a| a == r)
                    .unwrap_or(usize::MAX)
            })
            .unwrap_or(DestinationRole::ArchiveRead),
        PreflightOperation::Backup => DestinationRole::ArchiveWrite,
        // Unreachable in practice: the arm above left `archive_ref` `None`, so
        // nothing is resolved with this role. It is spelled rather than
        // wildcarded so that an operation added later cannot inherit a grant
        // by falling through.
        PreflightOperation::SourceConnection => DestinationRole::ArchiveRead,
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
        // THE SPEC FACTS THE RESOLVED FORM DOES NOT CARRY, read once from the
        // object itself: whether the destination opted in to the create-only
        // marker (reviewer finding **F3**) and whether it configures an
        // `evidenceRead` grant at all. A `DestinationAccess` request names its
        // own roles, so neither is derived for it.
        if resolved.is_ok() && request.operation != PreflightOperation::DestinationAccess {
            let api: Api<crate::crds::backup_destination::BackupDestination> =
                Api::namespaced(client.clone(), namespace);
            if let Some(object) = api.get_opt(&name).await.map_err(ReconcileError::Api)? {
                inputs.write_probe = destination::write_probe_enabled(&object);
                // `EvidenceRead` is requested ONLY when the destination
                // configures that grant. The check plan carries ONE credential
                // for its destination, so probing a role the object leaves
                // unconfigured would exercise the wrong credential and report a
                // refusal about a grant nobody asked for. Absent means
                // verification is `NotAttempted` (D2 §3.6), and the advisory
                // row is then simply not requested rather than answered wrongly.
                let configured = !matches!(
                    destination::resolve(&object, DestinationRole::EvidenceRead, &policy)
                        .map(|d| d.grant),
                    Ok(destination::ResolvedGrant::NotConfigured) | Err(_)
                );
                if configured && request.operation == PreflightOperation::Backup {
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
            // THE BYTES, VERBATIM, FROM WHICHEVER SIDE SUPPLIED THEM. A draft
            // carries its own; a `restoreRef` takes the `Restore`'s. Neither is
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

            // --- the recovery point (PLAT-11.1: a Backup UID) ---------------
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
                            recovery_point_source = Some(backup.spec.source_ref.name.clone());
                            RecoveryPointFacts::Found {
                                name: backup.name_any(),
                                uid,
                                expected_uid: rp.uid.clone(),
                                phase: backup.status.as_ref().and_then(|s| s.phase.clone()),
                                // THE TWO DIGESTS THE LOCATION COMPARISON
                                // NEEDS, AND NEITHER IS RECOMPUTED HERE. The
                                // left one is what the run FROZE (D2 §3.7,
                                // `Backup.status.destination`, absent on a
                                // legacy inline-archive point); the right one
                                // is what THIS check resolved for the source
                                // destination a moment ago. Deriving either
                                // from the live `BackupDestination` would let
                                // an edit after the freeze make a moved
                                // recovery point look like it never moved.
                                location_digest: backup
                                    .status
                                    .as_ref()
                                    .and_then(|s| s.destination.as_ref())
                                    .map(|d| d.location_digest.clone()),
                                expected_location_digest: inputs
                                    .archive
                                    .as_ref()
                                    .and_then(|r| r.as_ref().ok())
                                    .map(|d| d.location_digest.clone()),
                            }
                        }
                    }
                }
            };

            // --- the approval ----------------------------------------------
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
                                }
                            }
                        }
                    }
                }
            };
            // THE SOURCE CLUSTER'S RECORDED ID, for `TargetEqualsSource`.
            // It is read from the recovery point's OWN source `KafkaCluster`
            // and never from the plan: a plan that named its own source could
            // name anything, and the whole question is whether the target is
            // the cluster the archive was taken from.
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
    }
    Ok(inputs)
}

// ---------------------------------------------------------------------------
// The reconcile
// ---------------------------------------------------------------------------

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
/// **By UID and never by name.** `FailedCreate` is a common event in a
/// namespace with a `ResourceQuota`, and a classifier handed somebody else's
/// event would cancel a healthy check Job because of an unrelated workload
/// (`waiting::from_failed_create`'s own note).
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

/// Write `/status`, with D-SEAMS **S7** in both halves.
async fn patch_status(
    client: &kube::Client,
    pf: &Preflight,
    namespace: &str,
    status: &PreflightStatus,
) -> Result<(), ReconcileError> {
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
        return Ok(());
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
    // A MERGE PATCH AND NEVER `replace_status`: the role grants `patch` on
    // `preflights/status` and `update` on nothing, and the resourceVersion in
    // the BODY is what gives a merge patch its compare-and-set.
    match api
        .patch_status(&name, &PatchParams::default(), &Patch::Merge(patch))
        .await
    {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 409 => {
            debug!(
                preflight = %name,
                namespace = %namespace,
                "the status changed under this reconcile (409); the next pass reads it"
            );
            Ok(())
        }
        Err(e) => Err(ReconcileError::Api(e)),
    }
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

    // --- 1. Cancellation ----------------------------------------------------
    if pf.spec.cancel_requested && !terminal {
        if let Some(job) = jobs.get_opt(&job_name).await.map_err(ReconcileError::Api)? {
            // OWNERSHIP IS VERIFIED INSIDE `check::cancel`. A Job's name is a
            // pure function of this subject's UID, and a name is not an
            // identity: a foreign Job that happens to carry it is never
            // patched.
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

    // --- 2. A terminal check is REVALIDATED, never re-run, and COLLECTED -----
    //
    // D2 §4.3's `gc.rs` runs FIRST and on the same pass, because it is the only
    // other work a terminal check has. An absent policy is the documented
    // default (one hour) and not an error. The collector's answer never
    // changes the revalidation verdict: a check this pass deleted is simply
    // gone, and a `revalidate` that then writes its status gets a 404 the
    // reconciler already handles as any other API answer.
    if terminal {
        // THE SAME REFERENCE `resolve` READS, and from the same pure decision
        // (`check_policy::configured_ref`), so a terminal pass and a running
        // one cannot disagree about which document is in force.
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

    // --- 3. Everything this pass reads --------------------------------------
    let inputs = resolve(client, pf, &namespace, cache, now).await?;
    let binding = inputs.binding();
    let recorded_binding = binding_status(&binding);

    if let Some(url) = inputs.legacy_archive.as_deref() {
        // NAMED, NOT GUESSED. Turning an inline `s3://…` URL into the
        // `DestinationLocation` a check plan needs is the legacy-addressing
        // block of D2 §4.4, which belongs to the destination resolver and not
        // to this controller. `Failed` is D2 §6.2's "the check could not
        // produce a result", which is exactly true and is NOT a verdict about
        // the operation.
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
        // NO PLAN WAS RENDERED, so there is no request to derive the expected
        // rows from.
        let mut checks = assemble(
            probe,
            Vec::new(),
            true,
            &unrendered_job_rows(operation),
            &inputs.skip,
            now,
        );
        // NO JOB AND NO POD EXIST on this path, so every row that named no
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

    // --- 4. The Job -----------------------------------------------------------
    let existing = jobs.get_opt(&job_name).await.map_err(ReconcileError::Api)?;
    let Some(job) = existing else {
        // THE BINDING IS RECORDED BEFORE THE JOB EXISTS (D2 §6.6). A verdict
        // whose binding was written afterwards could not be told apart from a
        // verdict about whatever the objects became in between.
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
        patch_status(client, pf, &namespace, &status).await?;

        let config_map = check_plan::build(&job_name, &namespace, &owner, &shape.documents)
            .map_err(|e| plan_conflict(e.to_string()))?;
        let digest = shape.documents.check_plan_sha256();
        match check_plan::ensure(client, &namespace, &config_map, &owner.uid, &digest).await {
            Ok(_) => {}
            Err(check_plan::EnsureError::Api(e)) => return Err(ReconcileError::Api(e)),
            Err(check_plan::EnsureError::Plan(e)) => {
                let status = status_for(
                    pf,
                    &StatusInput {
                        phase: PHASE_FAILED.to_string(),
                        reason: Some(e.code()),
                        message: e.to_string(),
                        ..StatusInput::default()
                    },
                    now,
                );
                patch_status(client, pf, &namespace, &status).await?;
                return Ok(Action::await_change());
            }
        }
        match check::create_job(client, &shape.spec).await {
            Ok(_) => {}
            // A 409 IS THE DUPLICATE RECONCILE WORKING. The name is a pure
            // function of the subject's UID, so the Job this pass wanted
            // already exists and the next pass observes it.
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

    // --- 5. Observe it ------------------------------------------------------
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
    // THE DIGEST THE JOB WAS ACTUALLY CREATED WITH, read off the plan
    // `ConfigMap`'s annotation rather than recomputed here: a destination whose
    // generation moved after the Job started would otherwise make this pass
    // verify the relay against a plan the pod never mounted, and report
    // `ResultUnreadable` for a check that ran correctly.
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

    // --- 6. Terminal: the details ConfigMap, then the commit ----------------
    let details = match observation
        .relay
        .as_ref()
        .and_then(|r| r.stream(Stream::Details))
    {
        Some(bytes) if !bytes.is_empty() => {
            // REDACTED BY RULE AND NOT BY `redact`. `redact` ends with the
            // 512-CHARACTER cap, which is right for a message and destroys a
            // stream: the details document is JSON lines and a cut line is an
            // unparseable line (the same distinction `check_contract`'s own
            // `redact_path` had to learn). The rules are the chokepoint; the
            // cap is a message bound.
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

    // D2 §6.2's failure vocabulary, exactly: `Failed` means NO RESULT COULD BE
    // PRODUCED. A pod that never started because a Secret is missing DID
    // produce a result — `connection.credentialProjected notReady
    // CredentialSecretNotFound`, with a remedy — and reporting that as `Failed`
    // would hide the one finding an operator can act on behind a phase that
    // says "ask again later". A waiting code that belongs to NO row (the Job's
    // own deadline, a disrupted node, an unreadable relay) is the other case,
    // and that one really is `Failed`.
    let attributed = observation
        .waiting
        .as_ref()
        .and_then(|w| attribute_for(operation, w, &inputs.projections()))
        .is_some();
    let failed = observation.phase == check::CheckPhase::Failed && !relayed && !attributed;
    // THE ROWS THIS PLAN'S RUNNER WILL HAVE EMITTED, derived from the request
    // that was rendered and not from the operation (reviewer finding F1).
    let expected = job_rows(
        &shape.plan.request,
        inputs.plan.as_ref().is_some_and(PlanFacts::scratch),
    );
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
            // A CHECK THAT PRODUCED ROWS IS `Completed`, WHATEVER THEY SAY.
            // `Failed` is reserved for "no result could be produced" — D2 §6.2
            // — and the row set here always carries the controller's own
            // findings, so it is `Failed` only when the pod's own failure left
            // nothing to relay.
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

    // THE TTL IS PATCHED ONLY AFTER THE COMMIT. The relay lives on the pod and
    // the TTL controller deletes a Job and its pods together; a TTL set before
    // the status write lets garbage collection race the read.
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

/// A terminal check is never re-run — it is REVALIDATED.
///
/// D2 §6.6's applicability test is W12's, over the API. This is the same test
/// from the controller's side, and it exists because a stale green badge is
/// stale whether or not anybody is reading the API through a client that
/// recomputes: `status.result.state` is a printer column
/// (`kubectl get preflights`) and a UI field, and leaving it `ready` after the
/// referents moved is defect UI-FAKEPREFLIGHT with a new spelling.
/// One terminal preflight, as [`expired_terminal`] reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalPreflight {
    /// `metadata.name`.
    pub name: String,
    /// `metadata.uid` — the DELETE precondition, not the name.
    pub uid: String,
    /// The instant retention counts from: `status.result.expiresAt` when the
    /// check produced a verdict with an expiry, else when it was observed.
    pub basis: DateTime<Utc>,
}

/// Which terminal preflights are collectable — **pure**, D2 §4.3's `gc.rs`.
///
/// ONE RULE, AND IT IS NOT THE DISCOVERY'S. `now > basis + retentionSeconds`,
/// where the basis is `result.expiresAt` for a check that produced a verdict
/// and the observation time for one that never did (D2 §4.3: "or
/// `observedAt + …` if never ready"). There is no keep-last-N cohort here,
/// deliberately: a `Preflight` is about ONE plan of ONE operation, so there is
/// no "the newest five for this connection" to be outside of, and inventing a
/// cohort would delete the only readiness verdict a restore has.
///
/// The order is newest-first by basis with the UID as the tie-break, so two
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
    // THE GUARD THAT MATTERS. A non-terminal check is still running, and its
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

/// D2 §4.3's `gc.rs` for this kind: collect the namespace's expired terminal
/// checks.
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

/// EVERY EXIT REQUEUES AT [`REQUEUE_TERMINAL_SECS`] RATHER THAN
/// `await_change()`, and that is what keeps the collector alive. A `notReady`
/// or `unknown` verdict cannot go stale, so this function has nothing more to
/// say about it — but `reconcile_preflight`'s terminal branch runs the GC on
/// the same pass, and an object parked on `await_change()` never has another
/// pass. The cost is one list per terminal check per hour per namespace.
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
    // THE CLOCK FIRST, AND IT COSTS NO API CALL. An expired verdict is stale
    // whatever the referents say, so a check that has simply run out of time is
    // downgraded without re-reading a cluster.
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
    // ONE CLOCK READ PER PASS, for the reason every other reconciler in this
    // directory takes one: two reads could put a verdict and the expiry that
    // bounds it on either side of the same instant.
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
/// A SECOND TYPE RATHER THAN A FIELD ON [`Context`]. `controllers::Context` is
/// shared by every reconciler in this directory and all but this one read no
/// installation policy; a
/// field there would be carried and never read, which is the shape
/// `Context::runner_image`'s own note warns about.
pub struct ReconcilerContext {
    /// What every reconciler in this directory needs.
    pub context: Context,
    /// D2 §4.4's 30-second policy cache, so a reconcile does not `get` the
    /// policy `ConfigMap` on every pass.
    pub policy: check_policy::PolicyCache,
}

/// Run the `Preflight` controller until the process ends.
pub fn controller(
    client: kube::Client,
    runner_image: crate::job::RunnerImage,
) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<Preflight> = Api::all(client.clone());
    let jobs: Api<Job> = Api::all(client.clone());
    let ctx = Arc::new(ReconcilerContext {
        context: Context {
            client,
            // NO ARCHIVE HANDLE. A preflight reads no archive from the
            // controller: the archive checks are the check JOB's, with the
            // destination's own credential, inside a pod that ends.
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
