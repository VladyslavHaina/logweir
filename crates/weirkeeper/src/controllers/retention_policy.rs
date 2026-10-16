//! The `RetentionPolicy` reconciler: what would be removed, and (only when an
//! administrator has said so twice) what is.
//!
//! # A reconciler whose answer is irreversible
//!
//! This reconciler's output can destroy data. It still patches only its own
//! `/status` and creates only a Job of a different kind; it holds no delete
//! verb, no `secrets` verb, and it does not link the code that deletes:
//! `logweir-reaper` is reachable from `logweir-retention` and nothing else
//! (`scripts/check-no-archive-write.sh` check 3). Its authority is exactly
//! "create a Job from an approved plan", which an operator can see in
//! `kubectl get jobs`.
//!
//! # The four gates between a rule and a deleted object
//!
//! 1. `spec.mode` must be `Enforce`. `Report` (the default) evaluates and
//!    publishes and creates nothing.
//! 2. `spec.enforcement.approvedPlanSha256` must equal
//!    `status.lastEvaluation.planSha256`, and the plan must be younger than
//!    `planMaxAgeSeconds`, enforced here. The digest is a function of what
//!    would be deleted and of nothing that moves on its own (no instant,
//!    generation or counter): a rules edit, a `holds` edit or a new backup
//!    changes the content, and that is what invalidates an approval.
//! 3. A lease is written with a resourceVersion-preconditioned patch and it
//!    must land (a 409 aborts the pass before any Job exists), and then a
//!    consistent, non-cached, cluster-wide list of `Restore`s is made. A
//!    restore that arrived after the lease cannot slip past the list, because
//!    the list happens second, and the list fails closed on the whole
//!    destination: any nonterminal restore reading it stops the run.
//! 4. Each point's intent tombstone is written, create-only under `logweir/`,
//!    before that point's first delete, and a point whose intent cannot be
//!    written is not deleted. The run's own record carries the outcome and is
//!    written after it. The worker also re-validates every key and refuses the
//!    whole plan on the first one outside `<scope>/<backupId>/`.
//!
//! # A run whose pod is refused at creation fails fast
//!
//! A `ResourceQuota`, a `LimitRange`, an admission webhook or a missing
//! [`SERVICE_ACCOUNT`] can refuse the enforcement Job's pod, leaving only a
//! `FailedCreate` Event on the Job. Once the Job has had no pod for 30 seconds
//! and that Event says why ([`crate::check::refused_pod_creation`]), the Job is
//! cancelled and `Enforced=False` / `PodCreationForbidden` names the
//! admission's own words. The run is harvested when the cancelled Job has
//! finished (the lease is held until then) and counts toward
//! `EnforcementDegraded` like any failed run. Nothing was deleted.
//!
//! # The evaluation reads the right destination
//!
//! Its input is the `RecoveryCatalog`'s bounded view of this destination
//! (`crate::catalog_view`), read from the page `ConfigMap`s the catalog
//! published: never a bucket walk with the controller's global handle, and
//! never a schedule's archive URL.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::{Api, ListParams, Patch, PatchParams, PostParams};
use kube::runtime::controller::Action;
use kube::runtime::{watcher, Controller};
use kube::{Resource as _, ResourceExt as _};
use serde_json::{json, Value};
use tracing::{debug, info, warn};

use logweir_core::destination::DestinationRole;

use crate::catalog_view::{self as view, ViewEntry};
use crate::check;
use crate::conditions::{keep_instant_unless_changed, merge_condition, status_unchanged};
use crate::crds::backup::Backup;
use crate::crds::recovery_catalog::RecoveryCatalog;
use crate::crds::restore::Restore;
use crate::crds::retention_policy::{RetentionMode, RetentionPolicy, EVALUATION_LIST_BOUND};
use crate::crds::{Condition, Time};
use crate::destination::{self, ResolveError, ResolvedDestination};
use crate::job::RunnerOwner;
use crate::retention_plan::{self as plan, PointFacts};
use crate::runner_log;

use super::Context;

/// `Ready`: this policy is usable.
pub const CONDITION_READY: &str = "Ready";
/// `Evaluated`: the last evaluation succeeded.
pub const CONDITION_EVALUATED: &str = "Evaluated";
/// `Enforced`: what enforcement is doing, or why it is not.
pub const CONDITION_ENFORCED: &str = "Enforced";
/// `ExternalLifecycleConflict`: a declared bucket rule contradicts the rules.
pub const CONDITION_EXTERNAL_CONFLICT: &str = "ExternalLifecycleConflict";
/// `EnforcementDegraded`: three consecutive failed runs.
pub const CONDITION_DEGRADED: &str = "EnforcementDegraded";

/// The five condition types, in the order they are written.
pub const CONDITION_TYPES: &[&str] = &[
    CONDITION_READY,
    CONDITION_EVALUATED,
    CONDITION_ENFORCED,
    CONDITION_EXTERNAL_CONFLICT,
    CONDITION_DEGRADED,
];

/// `Ready=True`: the policy resolves and its catalog view is readable.
pub const REASON_POLICY_READY: &str = "PolicyReady";
/// `Ready=False`: two policies cover one destination.
pub const REASON_CONFLICT: &str = "Conflict";
/// `Ready=False`: the destination does not resolve.
pub const REASON_DESTINATION_UNUSABLE: &str = "DestinationUnusable";
/// `Ready=False`: the named catalog does not exist, or covers another
/// destination.
pub const REASON_CATALOG_UNUSABLE: &str = "CatalogUnusable";
/// `Ready=False`: the namespace holds more `Backup`s than one evaluation reads,
/// so the controller's own verdicts cannot all be joined and nothing is planned.
pub const REASON_BACKUP_HISTORY_TOO_LARGE: &str = "BackupHistoryTooLarge";
/// `Ready=False`: the `Backup` list failed, or a refused `Backup` names no
/// receipt digest and no set id and so could be tied to no point.
pub const REASON_BACKUP_VERDICTS_UNREADABLE: &str = "BackupVerdictsUnreadable";
/// `Ready=False`: `ExternalLifecycle` combined with guarantees no provider rule
/// can make.
pub const REASON_UNSUPPORTED_COMBINATION: &str = "UnsupportedCombination";

/// `Evaluated=True`.
pub const REASON_EVALUATION_COMPLETE: &str = "EvaluationComplete";
/// `Evaluated=False`: the catalog view is not readable right now.
pub const REASON_VIEW_UNREADABLE: &str = "ViewUnreadable";
/// `Evaluated=Unknown`: nothing has been evaluated yet.
pub const REASON_NEVER_EVALUATED: &str = "NeverEvaluated";
/// `Evaluated=False`: the controller's own `Backup` verdicts could not all be
/// read — [`REASON_BACKUP_HISTORY_TOO_LARGE`] or
/// [`REASON_BACKUP_VERDICTS_UNREADABLE`] says why — so nothing is planned.
pub const REASON_BACKUP_VERDICTS_INCOMPLETE: &str = "BackupVerdictsIncomplete";
/// `Evaluated=False`: the plan could not be rendered — a key escaped the scope.
pub const REASON_PLAN_REFUSED: &str = "PlanRefused";

/// `Enforced=False`: `mode` is not `Enforce`.
pub const REASON_RECOMMENDATION_ONLY: &str = "RecommendationOnly";
/// `Enforced=False`: waiting for an administrator to approve the current plan.
pub const REASON_AWAITING_APPROVAL: &str = "AwaitingApproval";
/// `Enforced=False`: the approved digest is not the current plan's.
pub const REASON_PLAN_SUPERSEDED: &str = "PlanSuperseded";
/// `Enforced=False`: the approved plan is older than `planMaxAgeSeconds`.
pub const REASON_PLAN_EXPIRED: &str = "PlanExpired";
/// `Enforced=False`: a nonterminal restore references a leased point.
pub const REASON_ACTIVE_RESTORE: &str = "ActiveRestore";
/// `Enforced=False`: `spec.enforcement.schedule` is not a UTC cron expression.
///
/// A refusal and not a fall-back to "every reconcile": a retention cadence
/// this build cannot read is a cadence nobody configured.
pub const REASON_UNSUPPORTED_SCHEDULE: &str = "UnsupportedSchedule";
/// `Enforced=False`: the lease PATCH did not land, so nothing holds these
/// points and no Job may start.
pub const REASON_LEASE_NOT_HELD: &str = "LeaseNotHeld";
/// `Enforced=False`: an object already holds the plan `ConfigMap`'s name and is
/// not this plan.
pub const REASON_PLAN_CONFIG_MAP_CONFLICT: &str = "PlanConfigMapConflict";
/// `Enforced=False`: the run record PATCH did not land, so no Job was created.
pub const REASON_RUN_NOT_RECORDED: &str = "RunNotRecorded";
/// `Enforced=True`: a run is in flight.
pub const REASON_RUN_IN_PROGRESS: &str = "RunInProgress";
/// `Enforced=True`: the last run completed.
pub const REASON_RUN_COMPLETE: &str = "RunComplete";
/// `Enforced=False`: the last run did not complete.
pub const REASON_RUN_FAILED: &str = "RunFailed";
/// `Enforced=False`: the worker refused a credential whose Secret is not bound
/// to this policy, its destination's route and its scope
/// (`retention-refusal=CredentialBindingMismatch`). Nothing was deleted.
pub const REASON_CREDENTIAL_BINDING_MISMATCH: &str =
    logweir_core::credential_binding::CREDENTIAL_BINDING_MISMATCH;

/// Every `retention-refusal=` code this build publishes: a closed table, so no
/// byte of a pod log reaches the status except one of these. It is the
/// worker's `logweir_retention::REFUSAL_CODES`, which
/// `tests/retention_policy_controller.rs` reads to hold the two equal. Only
/// [`REASON_CREDENTIAL_BINDING_MISMATCH`] is also a condition reason; the
/// others are named in `Enforced`'s and `Degraded`'s messages.
pub const RETENTION_REFUSALS: [&str; 22] = [
    "NotRun",
    "UnknownArgument",
    "ContractVersion",
    "NoPlanPath",
    "PlanUnreadable",
    "BindingIncomplete",
    "GenerationNotANumber",
    "CapUnreadable",
    "LocationUnreadable",
    "NoRecordCredential",
    REASON_CREDENTIAL_BINDING_MISMATCH,
    "PortUnavailable",
    "PlanUnparseable",
    "PlanWrongFormat",
    "PlanDigestMismatch",
    "PlanPolicyMismatch",
    "RetentionScopeViolation",
    "PlanManifestNotFirst",
    "PlanOverCap",
    "PlanEmptyLine",
    "PlanDuplicatePoint",
    "PlanDuplicateKey",
];
/// `Enforced=False`: the last run never started, because the namespace refused
/// its pod at creation (a `ResourceQuota`, a `LimitRange`, an admission webhook
/// or a missing ServiceAccount). The terminal state a `Backup`'s or `Restore`'s
/// runner reaches for the same refusal; the message carries the admission's
/// words. Counted like [`REASON_RUN_FAILED`]: three in a row are
/// `EnforcementDegraded`.
pub const REASON_POD_CREATION_FORBIDDEN: &str =
    crate::conditions::TERMINAL_STATE_POD_CREATION_FORBIDDEN;
/// `Enforced=True` with `requireApprovedPlan: false`: the choice is visible on
/// the object.
pub const REASON_UNATTENDED: &str = "UnattendedDeletionEnabled";
/// `Enforced=False`: there is nothing to delete.
pub const REASON_NOTHING_TO_DO: &str = "NothingToDo";
/// `Enforced=False`: points are due and the plan is empty, because none fits
/// the per-run ceiling: each is in a backup set that more due points name than
/// `spec.enforcement.maxDeletionsPerRun`, and a set is planned whole or not at
/// all. Not [`REASON_NOTHING_TO_DO`], which beside a held-back count above
/// zero would misstate why nothing is deleted.
pub const REASON_NOTHING_FITS_CEILING: &str = "NothingFitsCeiling";
/// `Enforced=False`: the catalog view does not hold every point of the archive
/// (a window, an unfinished walk, or the catalog did not say), so no run
/// starts from it. The evaluation is still published.
pub const REASON_VIEW_INCOMPLETE: &str = "ViewIncomplete";
/// `Enforced=False`: the catalog view is past its `viewExpiresAt`, or states
/// none, so no run starts from it.
pub const REASON_VIEW_EXPIRED: &str = "ViewExpired";
/// `Enforced=False`: a Job of this run's name exists and is not ours.
pub const REASON_JOB_NAME_CONFLICT: &str = "JobNameConflict";
/// `Enforced=False`: the destination has no `spec.access.evidenceWrite` grant
/// an enforcement Job could write its record with, so no Job is created.
pub const REASON_EVIDENCE_GRANT_UNUSABLE: &str = "EvidenceGrantUnusable";

/// `ExternalLifecycleConflict=True`: the declared expiry would expire kept
/// points.
pub const REASON_DECLARED_EXPIRY_CONFLICTS: &str = "DeclaredExpiryConflicts";
/// `ExternalLifecycleConflict=False`.
pub const REASON_NO_CONFLICT: &str = "NoConflict";

/// `EnforcementDegraded=True`.
pub const REASON_CONSECUTIVE_FAILURES: &str = "ConsecutiveRunFailures";
/// `EnforcementDegraded=False`.
pub const REASON_HEALTHY: &str = "Healthy";

/// Every reason this reconciler writes: the closed set a test asserts over.
pub const CONDITION_REASONS: &[&str] = &[
    REASON_POLICY_READY,
    REASON_CONFLICT,
    REASON_DESTINATION_UNUSABLE,
    REASON_CATALOG_UNUSABLE,
    REASON_UNSUPPORTED_COMBINATION,
    REASON_EVALUATION_COMPLETE,
    REASON_VIEW_UNREADABLE,
    REASON_NEVER_EVALUATED,
    REASON_PLAN_REFUSED,
    REASON_RECOMMENDATION_ONLY,
    REASON_AWAITING_APPROVAL,
    REASON_PLAN_SUPERSEDED,
    REASON_PLAN_EXPIRED,
    REASON_ACTIVE_RESTORE,
    REASON_UNSUPPORTED_SCHEDULE,
    REASON_LEASE_NOT_HELD,
    REASON_PLAN_CONFIG_MAP_CONFLICT,
    REASON_RUN_NOT_RECORDED,
    REASON_RUN_IN_PROGRESS,
    REASON_RUN_COMPLETE,
    REASON_RUN_FAILED,
    REASON_POD_CREATION_FORBIDDEN,
    REASON_UNATTENDED,
    REASON_NOTHING_TO_DO,
    REASON_NOTHING_FITS_CEILING,
    REASON_VIEW_INCOMPLETE,
    REASON_VIEW_EXPIRED,
    REASON_JOB_NAME_CONFLICT,
    REASON_EVIDENCE_GRANT_UNUSABLE,
    REASON_DECLARED_EXPIRY_CONFLICTS,
    REASON_NO_CONFLICT,
    REASON_CONSECUTIVE_FAILURES,
    REASON_HEALTHY,
];

/// `status.enforcement` when nothing deletes.
pub const ENFORCEMENT_RECOMMENDATION_ONLY: &str = "RecommendationOnly";
/// `status.enforcement` when the Logweir worker does.
pub const ENFORCEMENT_LOGWEIR_WORKER: &str = "LogweirWorker";
/// `status.enforcement` when a bucket lifecycle rule is declared.
pub const ENFORCEMENT_EXTERNAL: &str = "ExternalLifecycleDeclared";

/// A guarantee Logweir itself makes and tests.
pub const GUARANTEE_LOGWEIR: &str = "LogweirEnforced";
/// A guarantee somebody else may or may not be making. Logweir did not read the
/// rule, did not verify it and will not claim it works.
pub const GUARANTEE_PROVIDER_UNVERIFIED: &str = "ProviderEnforcedUnverified";
/// A guarantee nobody is making.
pub const GUARANTEE_NOT_ENFORCED: &str = "NotEnforced";

/// The three requeues.
pub const RUNNING_REQUEUE_SECONDS: u64 = 15;
/// The idle requeue.
pub const IDLE_REQUEUE_SECONDS: u64 = 60;
/// The error requeue.
pub const ERROR_REQUEUE_SECONDS: u64 = 30;

/// How many consecutive failed runs stop the scheduling.
pub const DEGRADED_AFTER_FAILURES: i64 = 3;

/// The per-point codes that are about the bucket or the credential rather than
/// the plan: the only ones a degraded policy re-probes for on its own
/// (`reprobe_due`).
pub const BUCKET_LEVEL_CODES: &[&str] = &["VersionedBucket", "VersionProbeRefused"];

/// How long after a bucket-level refusal a degraded policy starts one re-probe
/// run.
pub const REPROBE_AFTER: chrono::TimeDelta = chrono::TimeDelta::hours(24);

/// How long a lease lives beyond the Job's deadline, so a lease never outlives
/// the run it protects by much and never dies before it.
pub const LEASE_MARGIN_SECONDS: i64 = 300;

/// The label an operator finds a retention Job by.
pub const LABEL_POLICY_UID: &str = "logweir.dev/retention-policy-uid";
/// The component label.
pub const COMPONENT_RETENTION: &str = "retention";

/// The Job and `ConfigMap` name prefix.
pub const NAME_PREFIX: &str = "lwr-";

/// The retention Job's ServiceAccount, never the runner's: the retention pod
/// is the one pod in the installation that mounts a delete-capable credential.
pub const SERVICE_ACCOUNT: &str = "logweir-retention";

/// The binary the Job runs: `logweir-retention`, not `logweir`.
pub const RETENTION_BINARY: &str = "logweir-retention";
/// The retention contract version this controller pins into every Job.
pub const RETENTION_CONTRACT_VERSION: &str = "1";

/// Where the plan `ConfigMap` is mounted.
pub const PLAN_MOUNT_PATH: &str = "/retention";
/// The plan volume's name.
pub const PLAN_VOLUME: &str = "retention-plan";

/// The environment the Job carries. Public so a controller-double test names
/// the same strings the Job builder does.
pub mod env {
    /// The approved plan digest.
    pub const PLAN_SHA256: &str = "LOGWEIR_RETENTION_PLAN_SHA256";
    /// The policy UID.
    pub const POLICY_UID: &str = "LOGWEIR_RETENTION_POLICY_UID";
    /// The generation.
    pub const POLICY_GENERATION: &str = "LOGWEIR_RETENTION_POLICY_GENERATION";
    /// `spec.scope.prefix`, checked by the worker against the plan's own.
    pub const SCOPE_PREFIX: &str = "LOGWEIR_RETENTION_SCOPE_PREFIX";
    /// The run id.
    pub const RUN_ID: &str = "LOGWEIR_RETENTION_RUN_ID";
    /// The approver reference, or `unattended`.
    pub const APPROVER: &str = "LOGWEIR_RETENTION_APPROVER";
    /// The per-run point ceiling.
    pub const MAX_DELETIONS: &str = "LOGWEIR_RETENTION_MAX_DELETIONS";
    /// The per-run object ceiling.
    pub const MAX_OBJECTS: &str = "LOGWEIR_RETENTION_MAX_OBJECTS";
    /// The destination's `DestinationLocation`, as JSON.
    pub const LOCATION: &str = "LOGWEIR_RETENTION_LOCATION";

    /// All nine: the only variable names a refusal line may put on a status.
    pub const BINDING: [&str; 9] = [
        PLAN_SHA256,
        POLICY_UID,
        POLICY_GENERATION,
        SCOPE_PREFIX,
        RUN_ID,
        APPROVER,
        MAX_DELETIONS,
        MAX_OBJECTS,
        LOCATION,
    ];
}

/// What the write guard says when a status PATCH cannot be preconditioned.
///
/// Named because two places depend on it: the operator reading the controller
/// log, and `tests/retention_policy_controller.rs`, which pins it so a write
/// that loses its precondition is always reported. A /status compare-and-set
/// needs `metadata.resourceVersion` (seam S7); without one, a blind write
/// would overwrite a status this pass never read.
pub const NO_RESOURCE_VERSION: &str =
    "RetentionPolicy carries no metadata.resourceVersion, which a /status compare-and-set \
     needs (D-SEAMS S7); no patch is sent";

/// The annotation an administrator's `kubectl patch` records itself in, so the
/// enforcement record can name who approved.
pub const APPROVER_ANNOTATION: &str = "logweir.dev/retention-plan-approver";

/// The only thing that is not a status write.
#[derive(Debug)]
pub enum ReconcileError {
    /// The API server could not be talked to. **Requeue.**
    Api(kube::Error),
}

impl std::fmt::Display for ReconcileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Api(e) => write!(f, "the Kubernetes API returned an error: {e}"),
        }
    }
}

impl std::error::Error for ReconcileError {}

impl From<kube::Error> for ReconcileError {
    fn from(e: kube::Error) -> Self {
        Self::Api(e)
    }
}

/// Where one pass left the policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetentionPhase {
    /// Nothing is due.
    Idle,
    /// An evaluation was published; nothing was deleted.
    Evaluated,
    /// A run was started this pass.
    Started,
    /// A run is in flight.
    Running,
    /// A run finished and its result was published.
    Harvested,
    /// A terminal refusal.
    Refused,
    /// A declaration was recorded; Logweir enforces nothing.
    Declared,
}

/// What one pass did: the value every controller-double test asserts over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    /// Where the pass left the policy.
    pub phase: RetentionPhase,
    /// `Ready`'s status.
    pub ready: &'static str,
    /// `Ready`'s reason.
    pub ready_reason: &'static str,
    /// `Enforced`'s reason.
    pub enforced_reason: &'static str,
    /// `status.enforcement`.
    pub enforcement: &'static str,
    /// How many points the evaluation considered.
    pub points_evaluated: i64,
    /// How many candidates it found.
    pub candidates: usize,
    /// How many points something protected.
    pub protected: usize,
    /// How many points it could not classify.
    pub skipped: usize,
    /// The current plan's digest, when one was computed.
    pub plan_sha256: Option<String>,
    /// The Job this pass acted on, if any.
    pub job_name: Option<String>,
    /// How many object-store deletes this controller performed. Always zero:
    /// on the outcome so a test asserts it, and constant because this crate
    /// cannot delete (`scripts/check-no-archive-write.sh`).
    pub deletes_performed: u32,
}

/// A digest the catalog published, reduced to the bare lowercase hex the
/// recomputation produces.
///
/// `RecoveryCatalog.status.pages[].sha256` is `sha256:<lowercase hex>` (the
/// CRD, `charts/logweir/crds/recoverycatalogs.yaml`, and `catalog_view::seal`),
/// while `catalog_view::page_digest`, which this controller recomputes the
/// page with, returns bare hex. Compared literally they never match, and every
/// policy would read `Evaluated=False/ViewUnreadable`.
///
/// Normalised at the reader, not the writer: `page_digest` has a second caller
/// (the runner header check) that is already consistent. Both spellings are
/// accepted defensively; the hex must still match the page's bytes. Only the
/// prefix is stripped: a truncated, upper-case or other-algorithm value fails
/// the full-length comparison at the call site.
fn bare_hex(published: &str) -> &str {
    published.strip_prefix("sha256:").unwrap_or(published)
}

fn refused(reason: &'static str) -> Outcome {
    Outcome {
        phase: RetentionPhase::Refused,
        ready: "False",
        ready_reason: reason,
        enforced_reason: REASON_RECOMMENDATION_ONLY,
        enforcement: ENFORCEMENT_RECOMMENDATION_ONLY,
        points_evaluated: 0,
        candidates: 0,
        protected: 0,
        skipped: 0,
        plan_sha256: None,
        job_name: None,
        deletes_performed: 0,
    }
}

/// Everything one pass reads that is not the object itself.
pub struct PolicyContext<'a> {
    /// The client every `Api` is built from.
    pub client: &'a kube::Client,
    /// The installation policy, for destination resolution.
    pub policy: &'a check::policy::Policy,
    /// The image and pull policy the retention Job will name.
    pub runner_image: &'a crate::job::RunnerImage,
    /// This pass's instant.
    pub now: DateTime<Utc>,
}

/// Reconcile one `RetentionPolicy`.
///
/// # Errors
///
/// [`ReconcileError::Api`] only — every other refusal is a status write.
pub async fn reconcile_policy(
    policy: &RetentionPolicy,
    ctx: &PolicyContext<'_>,
) -> Result<Outcome, ReconcileError> {
    let name = policy.name_any();
    let Some(namespace) = policy.namespace().filter(|n| !n.is_empty()) else {
        warn!(policy = %name, "RetentionPolicy carries no metadata.namespace; nothing is done");
        return Ok(refused(REASON_DESTINATION_UNUSABLE));
    };
    let Some(uid) = policy.uid().filter(|u| !u.is_empty()) else {
        warn!(policy = %name, namespace = %namespace, "RetentionPolicy carries no metadata.uid");
        return Ok(refused(REASON_DESTINATION_UNUSABLE));
    };
    let mut pass = Pass {
        resource_version: std::sync::Mutex::new(
            policy
                .metadata
                .resource_version
                .clone()
                .filter(|v| !v.is_empty()),
        ),
        observed_status: std::sync::Mutex::new(
            policy
                .status
                .as_ref()
                .and_then(|s| serde_json::to_value(s).ok()),
        ),
        policy,
        name,
        namespace,
        uid,
        ctx,
    };
    pass.run().await
}

struct Pass<'a> {
    policy: &'a RetentionPolicy,
    name: String,
    namespace: String,
    uid: String,
    ctx: &'a PolicyContext<'a>,
    /// The `metadata.resourceVersion` the next status PATCH will use as its
    /// precondition.
    ///
    /// An enforcing pass patches three times (the evaluation, the lease,
    /// `lastEnforcement`), and each success advances the version; reusing the
    /// watcher's version would make the lease patch 409, and an unwritten lease
    /// with a Job created anyway is an untracked deletion run that the next
    /// pass would start again. This reconciler holds no `get` on its own kind,
    /// so the version each successful PATCH returns is the only fresh one.
    ///
    /// A `Mutex`, not a `RefCell`: the reconcile future must be `Send`. It is
    /// uncontended (one pass, one thread), and no guard is ever held across an
    /// `await`.
    resource_version: std::sync::Mutex<Option<String>>,
    /// The status as this pass believes it now stands, for the "nothing
    /// changed, send nothing" comparison; after the evaluation patch the
    /// object in hand is stale.
    observed_status: std::sync::Mutex<Option<Value>>,
}

/// What one status PATCH did.
///
/// Three answers, not a `Result<(), _>`: "nothing needed writing" and
/// "somebody else wrote first" are different facts, and only one of them may
/// be swallowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatchOutcome {
    /// The API server accepted it and the version cursor moved.
    Applied,
    /// The computed status equalled the stored one; nothing was sent.
    Unchanged,
    /// The precondition failed, and the caller decides: for an evaluation, the
    /// next pass reads it; for the lease, abort before creating a Job, because
    /// nothing would be holding the points.
    Conflict,
}

impl Pass<'_> {
    fn generation(&self) -> i64 {
        self.policy.metadata.generation.unwrap_or(0)
    }

    /// The spec has changed since the controller last acted on it.
    ///
    /// `metadata.generation != status.observedGeneration` is the only thing on
    /// the object that says so, and it is the one release for a degraded
    /// policy: not time, a restart, or the count ageing out. A policy never
    /// reconciled has no `observedGeneration` and counts as changed.
    fn spec_changed(&self) -> bool {
        self.policy
            .status
            .as_ref()
            .and_then(|s| s.observed_generation)
            .unwrap_or(-1)
            != self.generation()
    }

    /// The consecutive-failure budget this pass starts from.
    ///
    /// A spec change zeroes it: a release that left the counter at its ceiling
    /// would give the operator one run before the next failure re-degraded the
    /// policy. Every reader of the count (the enforcement decision, the
    /// harvest, the published evaluation) goes through here, so the release
    /// rule is stated once.
    fn budget_before(&self) -> i64 {
        if self.spec_changed() {
            return 0;
        }
        self.policy
            .status
            .as_ref()
            .and_then(|s| s.consecutive_run_failures)
            .unwrap_or(0)
    }

    /// The retry budget is spent: scheduling stops until the spec changes.
    fn budget_spent(&self) -> bool {
        self.budget_before() >= DEGRADED_AFTER_FAILURES
    }

    /// A degraded policy may start one re-probe run now.
    ///
    /// A spec change releases a degraded policy, but two refusals are fixed
    /// outside the object: `VersionProbeRefused` (grant the retention
    /// credential `s3:GetObject`) and `VersionedBucket` (stop versioning the
    /// bucket). So when every per-point code of the last run is in
    /// [`BUCKET_LEVEL_CODES`] and it finished at least [`REPROBE_AFTER`] ago, one
    /// run is scheduled again. Safe by construction: the worker refuses before
    /// deleting anything while the reason holds, so a re-probe deletes nothing
    /// until it has cleared, and costs one Job and one intent tombstone per
    /// planned point per day. A failed re-probe re-stamps `finishedAt`, which is
    /// the backoff.
    fn reprobe_due(&self) -> bool {
        let Some(last) = self
            .policy
            .status
            .as_ref()
            .and_then(|s| s.last_enforcement.as_ref())
        else {
            return false;
        };
        let codes: Vec<String> = last
            .failed
            .iter()
            .flatten()
            .map(|f| f.code.clone())
            .collect();
        only_bucket_level(&codes)
            && last
                .finished_at
                .as_ref()
                .is_some_and(|f| self.ctx.now - *f >= REPROBE_AFTER)
    }

    /// The last run's outcome in words, for the `EnforcementDegraded` message:
    /// the refusal it named, or else the exit code and the closed per-point
    /// codes, which is what the object knows.
    fn last_failure_detail(&self) -> String {
        if let Some((code, variable)) = self.last_refusal() {
            return refused_detail(code, variable);
        }
        let record = self
            .policy
            .status
            .as_ref()
            .and_then(|s| s.last_enforcement.as_ref());
        failure_detail(record.and_then(|r| r.exit_code), &self.last_failure_codes())
    }

    /// The refusal `status.lastEnforcement` records for the last run, when it
    /// finished with exit 3 and no run has started since (`start_run` clears
    /// the two fields with the rest of a finished run's). Read back through the
    /// harvest's closed tables, so only a known code and a binding variable's
    /// name reach a message.
    fn last_refusal(&self) -> Option<(&'static str, Option<&'static str>)> {
        let run = self.policy.status.as_ref()?.last_enforcement.as_ref()?;
        if run.finished_at.is_none() || run.exit_code != Some(3) {
            return None;
        }
        let recorded = run.refusal_code.as_deref()?;
        // The worker's closed table, or this controller's own name for the
        // mixed pair (FX-43), which no worker prints.
        let older = logweir_core::runner_line::RUNNER_OLDER_THAN_CONTROLLER;
        let code = RETENTION_REFUSALS
            .iter()
            .copied()
            .find(|known| *known == recorded)
            .or_else(|| (recorded == older).then_some(older))?;
        let variable = run
            .refusal_variable
            .as_deref()
            .and_then(|named| env::BINDING.iter().copied().find(|known| *known == named));
        Some((code, variable))
    }

    /// The last run's refusal as `Enforced` publishes it, `(reason, message)`:
    /// what the harvest wrote, re-published from the record until the next run
    /// starts (D-R1).
    fn held_refusal(&self) -> Option<(&'static str, String)> {
        let (code, variable) = self.last_refusal()?;
        let run_id = self
            .policy
            .status
            .as_ref()?
            .last_enforcement
            .as_ref()?
            .run_id
            .as_deref()?;
        Some(refused_run_enforced(run_id, code, variable))
    }

    /// The closed per-point codes the last run left on the object.
    fn last_failure_codes(&self) -> Vec<String> {
        self.policy
            .status
            .as_ref()
            .and_then(|s| s.last_enforcement.as_ref())
            .and_then(|r| r.failed.as_ref())
            .map(|f| f.iter().map(|d| d.code.clone()).collect())
            .unwrap_or_default()
    }

    fn owner(&self) -> RunnerOwner {
        RunnerOwner {
            api_version: RetentionPolicy::api_version(&()).to_string(),
            kind: RetentionPolicy::kind(&()).to_string(),
            name: self.name.clone(),
            uid: self.uid.clone(),
        }
    }

    fn stem(&self) -> String {
        plan::config_map_stem(&self.uid)
    }

    /// The enforcement slot `spec.enforcement.schedule` names, at `now`.
    ///
    /// The slot is the cron's own latest due firing, and it is both the cadence
    /// gate and the run id's slot, so "one run per slot" is a property of the
    /// name rather than the clock. `Err` is an unparseable expression: a
    /// refusal, not a silently hourly cadence.
    fn enforcement_slot(&self) -> Result<Option<DateTime<Utc>>, String> {
        let Some(enforcement) = self.policy.spec.enforcement.as_ref() else {
            return Ok(None);
        };
        // UTC, no time zone: a retention cadence that moved with DST would run
        // twice or not at all on two nights a year.
        let cadence = crate::cadence::Cadence::parse(&enforcement.schedule, None)
            .map_err(|e| format!("spec.enforcement.schedule is not a UTC cron expression: {e}"))?;
        Ok(cadence.latest_due_slot(self.ctx.now))
    }

    fn rules(&self) -> plan::Rules {
        let r = &self.policy.spec.rules;
        plan::Rules {
            keep_last: r.keep_last.map(i64::from),
            keep_days: r.keep_days.map(i64::from),
            min_usable_points: i64::from(r.min_usable_points),
        }
    }

    async fn run(&mut self) -> Result<Outcome, ReconcileError> {
        // 1. Conflict first: two policies for one destination are both
        //    `Ready=False/Conflict`, and neither may so much as compute a plan
        //    an administrator could approve.
        if let Some(other) = self.contesting_policy().await? {
            return self
                .publish_refusal(
                    REASON_CONFLICT,
                    &format!(
                        "RetentionPolicy {}/{other} covers the same destination as this one; \
                         neither evaluates and neither enforces until one is removed. Two \
                         policies for one destination can approve two different plans for the \
                         same objects.",
                        self.namespace
                    ),
                )
                .await;
        }

        // 2. `ExternalLifecycle` is a DECLARATION and produces no evaluation.
        if self.policy.spec.mode == RetentionMode::ExternalLifecycle {
            return self.declare_external().await;
        }

        // 3. The tracked run, before any decision to start one.
        if let Some(outcome) = self.tracked_run().await? {
            return Ok(outcome);
        }

        // 4. Evaluate. This is the whole of `Report` mode, and the input to
        //    `Enforce`.
        self.evaluate().await
    }

    /// Another `RetentionPolicy` in this namespace covering the same
    /// destination.
    ///
    /// Listed in this namespace only, compared on `destinationRef.name`. The
    /// pair is symmetric, so both objects see the conflict and both go
    /// `Ready=False`.
    async fn contesting_policy(&self) -> Result<Option<String>, ReconcileError> {
        let api: Api<RetentionPolicy> = Api::namespaced(self.ctx.client.clone(), &self.namespace);
        let list = api.list(&ListParams::default()).await?;
        let mine = &self.policy.spec.destination_ref.name;
        let mut others: Vec<String> = list
            .items
            .iter()
            .filter(|p| p.name_any() != self.name && &p.spec.destination_ref.name == mine)
            .map(|p| p.name_any())
            .collect();
        others.sort();
        Ok(others.into_iter().next())
    }

    /// Record the declaration, mark what is and is not enforced, and raise
    /// `ExternalLifecycleConflict` when the declared expiry would expire points
    /// the rules keep.
    ///
    /// Logweir reads no lifecycle configuration (`object_store` exposes no
    /// such API), so `ageExpiry` and `legalHold` are
    /// `ProviderEnforcedUnverified`, not a claim that the rule is in force, and
    /// the three guarantees a bucket rule cannot express are `NotEnforced`.
    async fn declare_external(&self) -> Result<Outcome, ReconcileError> {
        let declared = self.policy.spec.external_lifecycle.as_ref();
        let expiration_days = declared.map(|d| i64::from(d.expiration_days));
        let rules = self.rules();
        // The conflict: the bucket would expire what the rules keep. Compared
        // on `keepDays` only: `keepLast` and `minUsablePoints` are counts, and a
        // lifecycle rule cannot count.
        let conflict = match (expiration_days, rules.keep_days) {
            (Some(expiry), Some(keep)) => expiry < keep,
            _ => false,
        };
        let guarantees = json!({
            "ageExpiry": GUARANTEE_PROVIDER_UNVERIFIED,
            "legalHold": GUARANTEE_PROVIDER_UNVERIFIED,
            "minUsablePoints": GUARANTEE_NOT_ENFORCED,
            "activeRestoreProtection": GUARANTEE_NOT_ENFORCED,
            "sharedSegments": GUARANTEE_NOT_ENFORCED,
        });
        let conflict_message = match (expiration_days, rules.keep_days) {
            (Some(expiry), Some(keep)) if conflict => format!(
                "the declared lifecycle rule expires objects after {expiry} days and this \
                 policy's rules keep points for {keep} days; the bucket wins, and Logweir cannot \
                 protect an individual point here"
            ),
            _ => "no declared expiry contradicts the configured rules".to_string(),
        };
        let conditions = self.conditions(&[
            (
                CONDITION_READY,
                "True",
                REASON_POLICY_READY,
                format!(
                    "deletion at this destination is performed by the bucket lifecycle rule `{}`; \
                     Logweir reports and cannot protect individual points here",
                    declared.map_or("<unnamed>", |d| d.rule_id.as_str())
                ),
            ),
            (
                CONDITION_EVALUATED,
                "Unknown",
                REASON_NEVER_EVALUATED,
                "an ExternalLifecycle policy produces no Logweir evaluation: nothing here reads \
                 the provider's rule, so there is nothing to report about what it would remove"
                    .to_string(),
            ),
            (
                CONDITION_ENFORCED,
                "False",
                REASON_RECOMMENDATION_ONLY,
                "Logweir deletes nothing under this policy".to_string(),
            ),
            (
                CONDITION_EXTERNAL_CONFLICT,
                if conflict { "True" } else { "False" },
                if conflict {
                    REASON_DECLARED_EXPIRY_CONFLICTS
                } else {
                    REASON_NO_CONFLICT
                },
                conflict_message,
            ),
            (
                CONDITION_DEGRADED,
                "False",
                REASON_HEALTHY,
                "no enforcement run has been attempted".to_string(),
            ),
        ]);
        let mut status = json!({
            "enforcement": ENFORCEMENT_EXTERNAL,
            "guarantees": guarantees,
            "conditions": conditions,
            // Cleared, beside `Evaluated=Unknown/NeverEvaluated`: a merge patch
            // omitting it would keep a previous mode's evaluation, showing a
            // plan preview this mode never makes, deriving an approved-plan
            // state from it, and reusing its `planExpiresAt` on a switch back
            // to `Enforce`.
            "lastEvaluation": Value::Null,
        });
        self.adopt_generation(&mut status);
        self.patch_status(status).await?;
        Ok(Outcome {
            phase: RetentionPhase::Declared,
            ready: "True",
            ready_reason: REASON_POLICY_READY,
            enforced_reason: REASON_RECOMMENDATION_ONLY,
            enforcement: ENFORCEMENT_EXTERNAL,
            points_evaluated: 0,
            candidates: 0,
            protected: 0,
            skipped: 0,
            plan_sha256: None,
            job_name: None,
            deletes_performed: 0,
        })
    }

    /// `Some` when a run this policy started is still in flight or has not been
    /// harvested.
    async fn tracked_run(&self) -> Result<Option<Outcome>, ReconcileError> {
        let Some(run_id) = self
            .policy
            .status
            .as_ref()
            .and_then(|s| s.last_enforcement.as_ref())
            .and_then(|r| r.run_id.clone())
        else {
            return Ok(None);
        };
        let harvested = self
            .policy
            .status
            .as_ref()
            .and_then(|s| s.last_enforcement.as_ref())
            .is_some_and(|r| r.finished_at.is_some());
        if harvested {
            return Ok(None);
        }
        let job_name = self.job_name(&run_id);
        let jobs: Api<Job> = Api::namespaced(self.ctx.client.clone(), &self.namespace);
        let Some(job) = jobs.get_opt(&job_name).await? else {
            // The Job is gone and the run was never harvested — its TTL fired
            // between passes, or it was removed. The RECORD in object storage
            // is the durable answer; the status says the run did not report.
            return Ok(Some(
                self.harvest(&run_id, None, &RunReport::default()).await?,
            ));
        };
        // A Job wearing this run's name is not thereby this policy's Job.
        if !super::recovery_catalog::owned_by(&job, &self.uid) {
            return Ok(Some(
                self.publish_refusal(
                    REASON_JOB_NAME_CONFLICT,
                    &format!(
                        "Job {}/{job_name} carries this run's name and is controlled by \
                         something else; nothing is read from it and no second run is created",
                        self.namespace
                    ),
                )
                .await?,
            ));
        }
        if !super::backup::job_finished(&job) {
            // A pod the namespace refused is failed fast: no request while the
            // Job counts a pod or is younger than the 30-second grace,
            // otherwise one pod list and, when it finds none, one events list
            // by the Job's UID. The Job is cancelled and the status names the
            // refusal now.
            //
            // The run is not harvested in this pass, and the lease stays: a pod
            // admitted between the pod list and the deadline patch could still
            // run until the Job controller kills it. The Job finishes within
            // about a second, its watch wakes the next pass, and `harvest_run`
            // proves "no pod" at the finish (or reads a started pod's exit
            // code) before it releases the lease, counts the run and patches
            // the TTL.
            if let Some(refusal) =
                check::refused_pod_creation(self.ctx.client, &self.namespace, &job, self.ctx.now)
                    .await?
            {
                check::cancel(self.ctx.client, &self.namespace, &job, &self.uid).await?;
                warn!(
                    policy = %self.name, namespace = %self.namespace, run = %run_id,
                    job = %job_name, code = refusal.code.as_str(),
                    "the retention run's pod was refused at creation; the Job is cancelled, the \
                     status names the refusal, and the run is recorded once the Job has \
                     finished. Nothing was deleted"
                );
                return Ok(Some(
                    self.report_refused(&run_id, &job_name, &refusal.message)
                        .await?,
                ));
            }
            return Ok(Some(self.report_running(&run_id, &job_name).await?));
        }
        let report = self.harvest_run(&job).await?;
        Ok(Some(self.harvest(&run_id, Some(&job), &report).await?))
    }

    /// The exit code and the run's own key lines, through the pod's owner UID,
    /// never a label (seam **S6**). All five `status.lastEnforcement` fields
    /// come from here; `previously_refused()` reads `failed[]`, which is what
    /// keeps a provider-refused point out of the next plan until the reason
    /// clears.
    async fn harvest_run(&self, job: &Job) -> Result<RunReport, ReconcileError> {
        let Some(pod) = check::pod::find_owned_pod(self.ctx.client, &self.namespace, job).await?
        else {
            // A Job that finished without ever having a pod, whose
            // `FailedCreate` Event says why, is that refusal, not "produced no
            // exit code".
            return Ok(RunReport {
                refused: check::finished_without_pod(
                    self.ctx.client,
                    &self.namespace,
                    job,
                    self.ctx.now,
                )
                .await
                .map(|w| w.message),
                ..RunReport::default()
            });
        };
        let exit_code = super::backup::terminated_exit_code(&pod);
        let pods: Api<k8s_openapi::api::core::v1::Pod> =
            Api::namespaced(self.ctx.client.clone(), &self.namespace);
        let log = match runner_log::read_text(&pods, &pod.name_any(), &runner_log::RELAY).await {
            Ok(log) => log,
            // A pod whose log is gone is a run whose durable answer is the
            // record in object storage; it is not a reconcile failure.
            Err(e) if check::is_log_absent(&e) => String::new(),
            Err(e) => return Err(ReconcileError::Api(e)),
        };
        Ok(run_report(&crate::job::job_token(job), &log, exit_code))
    }

    async fn report_running(
        &self,
        run_id: &str,
        job_name: &str,
    ) -> Result<Outcome, ReconcileError> {
        let conditions = self.conditions(&[
            (
                CONDITION_READY,
                "True",
                REASON_POLICY_READY,
                "the policy resolves".to_string(),
            ),
            (
                CONDITION_ENFORCED,
                "True",
                REASON_RUN_IN_PROGRESS,
                format!("retention run {run_id} is executing as Job {job_name}"),
            ),
        ]);
        let mut status = json!({
            "enforcement": ENFORCEMENT_LOGWEIR_WORKER,
            "conditions": conditions,
        });
        self.adopt_generation(&mut status);
        self.patch_status(status).await?;
        Ok(Outcome {
            phase: RetentionPhase::Running,
            ready: "True",
            ready_reason: REASON_POLICY_READY,
            enforced_reason: REASON_RUN_IN_PROGRESS,
            enforcement: ENFORCEMENT_LOGWEIR_WORKER,
            points_evaluated: 0,
            candidates: 0,
            protected: 0,
            skipped: 0,
            plan_sha256: None,
            job_name: Some(job_name.to_string()),
            deletes_performed: 0,
        })
    }

    /// The pass that cancelled a run whose pod was refused at creation.
    ///
    /// `Enforced=False` / `PodCreationForbidden` with the admission's words, so
    /// the refusal is on the object the moment it is known, and nothing else:
    /// `lastEnforcement`, the lease and `consecutiveRunFailures` are left for
    /// the harvest of the finished Job (see `tracked_run`).
    async fn report_refused(
        &self,
        run_id: &str,
        job_name: &str,
        admission: &str,
    ) -> Result<Outcome, ReconcileError> {
        let conditions = self.conditions(&[
            (
                CONDITION_READY,
                "True",
                REASON_POLICY_READY,
                "the policy resolves".to_string(),
            ),
            (
                CONDITION_ENFORCED,
                "False",
                REASON_POD_CREATION_FORBIDDEN,
                format!(
                    "retention run {run_id} never started ({REASON_POD_CREATION_FORBIDDEN}), so \
                     nothing was deleted: {admission}. Job {job_name} is cancelled; the run is \
                     recorded, and its lease released, once the Job has finished"
                ),
            ),
        ]);
        let mut status = json!({
            "enforcement": ENFORCEMENT_LOGWEIR_WORKER,
            "conditions": conditions,
        });
        self.adopt_generation(&mut status);
        self.patch_status(status).await?;
        Ok(Outcome {
            phase: RetentionPhase::Running,
            ready: "True",
            ready_reason: REASON_POLICY_READY,
            enforced_reason: REASON_POD_CREATION_FORBIDDEN,
            enforcement: ENFORCEMENT_LOGWEIR_WORKER,
            points_evaluated: 0,
            candidates: 0,
            protected: 0,
            skipped: 0,
            plan_sha256: None,
            job_name: Some(job_name.to_string()),
            deletes_performed: 0,
        })
    }

    /// Publish what a finished run did, clear the lease, and count the failure
    /// if it was one.
    ///
    /// The status is written before the Job's TTL is patched (S7's ordering,
    /// as in `controllers/backup.rs`): pod garbage collection must never race
    /// the exit-code read.
    async fn harvest(
        &self,
        run_id: &str,
        job: Option<&Job>,
        report: &RunReport,
    ) -> Result<Outcome, ReconcileError> {
        let exit_code = report.exit_code;
        let failed = exit_code != Some(0);
        // Through `budget_before`, so a spec change zeroes the count here too:
        // a harvest adopts the new generation, and the release must not depend
        // on which arm ran first.
        let previous = self.budget_before();
        // A run that stopped on its own ceiling is not a failed run: it exits 1
        // because work remains, and counting it would degrade retention on a
        // large archive after three ordinary bounded runs.
        let bounded = report.bounded_only();
        let failures = if failed && !bounded {
            previous + 1
        } else if failed {
            previous
        } else {
            0
        };
        let degraded = failures >= DEGRADED_AFTER_FAILURES;
        // The refusal the run named, read only off an exit 3: it is published
        // on the conditions and recorded in `lastEnforcement`.
        let refusal = report
            .refusal_code
            .filter(|_| exit_code == Some(3))
            .map(|code| (code, report.refusal_variable));
        let binding_refused =
            refusal.is_some_and(|(code, _)| code == REASON_CREDENTIAL_BINDING_MISMATCH);
        let reason = if report.refused.is_some() {
            REASON_POD_CREATION_FORBIDDEN
        } else if binding_refused {
            REASON_CREDENTIAL_BINDING_MISMATCH
        } else if failed {
            REASON_RUN_FAILED
        } else {
            REASON_RUN_COMPLETE
        };
        let message = match exit_code {
            None if report.refused.is_some() => format!(
                "retention run {run_id} never started ({REASON_POD_CREATION_FORBIDDEN}), so \
                 nothing was deleted: {}",
                report.refused.as_deref().unwrap_or_default()
            ),
            Some(0) => format!("retention run {run_id} completed"),
            Some(3) => match refusal {
                Some((code, variable)) => refused_run_enforced(run_id, code, variable).1,
                None => format!(
                    "retention run {run_id} REFUSED its plan before deleting anything (exit 3); \
                     the archive is untouched"
                ),
            },
            Some(code) if bounded => format!(
                "retention run {run_id} exited {code} having reached its own \
                 maxObjectsPerRun; the leftovers are named and the next run completes them. \
                 This is not counted as a failure."
            ),
            Some(code) => format!(
                "retention run {run_id} exited {code}: {} point(s) did not complete. The \
                 create-only, unsigned record under logweir/retention/ names which.",
                report.failed.len()
            ),
            None => format!(
                "retention run {run_id} produced no exit code: its pod is gone or was never \
                 readable. The create-only, unsigned record under logweir/retention/ is the \
                 durable answer."
            ),
        };
        let codes: Vec<String> = report.failed.iter().map(|(_, code)| code.clone()).collect();
        let detail = match report.refused.as_deref() {
            Some(admission) => format!(
                "the last run never started, because its pod was refused at creation \
                 ({REASON_POD_CREATION_FORBIDDEN}): {admission}"
            ),
            None => match refusal {
                Some((code, variable)) => refused_detail(code, variable),
                None => failure_detail(exit_code, &codes),
            },
        };
        let stop = scheduling_stop(&codes);
        let conditions = self.conditions(&[
            (
                CONDITION_READY,
                "True",
                REASON_POLICY_READY,
                "the policy resolves".to_string(),
            ),
            (
                CONDITION_ENFORCED,
                if failed { "False" } else { "True" },
                reason,
                message,
            ),
            (
                CONDITION_DEGRADED,
                if degraded { "True" } else { "False" },
                if degraded {
                    REASON_CONSECUTIVE_FAILURES
                } else {
                    REASON_HEALTHY
                },
                if degraded {
                    format!(
                        "{failures} consecutive retention runs have failed and the retry budget \
                         is spent: {detail}. {stop} (edit spec.enforcement, or spec.rules, and \
                         the count clears with it). status.lastEnforcement.recordKey names the \
                         durable record of the last run."
                    )
                } else {
                    format!("{failures} consecutive run failures; {detail}")
                },
            ),
        ]);
        let mut harvest_status = json!({
                // A run refused on its credential's binding leaves a policy that
                // deletes nothing until a human rebinds the Secret: a standing
                // state, published as `publish_standing_refusal` publishes one,
                // so the console does not read "enforced by Logweir" over it.
                "enforcement": if binding_refused {
                    ENFORCEMENT_RECOMMENDATION_ONLY
                } else {
                    ENFORCEMENT_LOGWEIR_WORKER
                },
                // The terminal fields the CRD declares; `failed[]` is what
                // `previously_refused()` reads to keep a provider-refused point
                // out of the next plan, and the refusal is what `Enforced` and
                // `EnforcementDegraded` keep naming until the next run.
                "lastEnforcement": {
                    "runId": run_id,
                    "finishedAt": self.ctx.now,
                    "exitCode": exit_code,
                    "deleted": report.deleted,
                    "failed": report
                        .failed
                        .iter()
                        .map(|(point_id, code)| json!({"pointId": point_id, "code": code}))
                        .collect::<Vec<Value>>(),
                    "objectsDeleted": report.objects_deleted,
                    "recordKey": report.record_key,
                    "recordSha256": report.record_sha256,
                    "refusalCode": refusal.map(|(code, _)| code),
                    "refusalVariable": refusal.and_then(|(_, variable)| variable),
                },
                // RFC 7386: `null` deletes the key. The lease exists only while
                // a run holds it; one left behind would hold restore admission
                // for nothing.
                "lease": Value::Null,
                "consecutiveRunFailures": failures,
                "conditions": conditions,
        });
        if binding_refused {
            // Object-wise, one key: the other guarantees are untouched.
            harvest_status["guarantees"] = json!({ "ageExpiry": GUARANTEE_NOT_ENFORCED });
        }
        // Through the helper like every other writer: it adopts the generation
        // and adds nothing here, because this patch already carries the count
        // (released by `budget_before()`) and its own `EnforcementDegraded`.
        self.adopt_generation(&mut harvest_status);
        let outcome = self.patch_status(harvest_status).await?;

        // The TTL after the status, and only if the status actually landed.
        // `patch_status` answers `Conflict` both for a 409 and for a missing
        // precondition cursor ([`NO_RESOURCE_VERSION`]), and either way the
        // exit code was not published. The TTL is the irreversible half: the
        // Job and pod would be collected and the next pass could only record
        // "produced no exit code". So the TTL waits and the next pass reads the
        // run again.
        match outcome {
            PatchOutcome::Applied | PatchOutcome::Unchanged => {
                if let Some(job) = job {
                    self.patch_job_ttl(job).await?;
                }
            }
            PatchOutcome::Conflict => {
                warn!(
                    policy = %self.name, namespace = %self.namespace, run = %run_id,
                    "the harvest status did not land; this run stays untracked and its Job keeps \
                     its pod so the next pass can read the exit code again"
                );
            }
        }
        Ok(Outcome {
            phase: RetentionPhase::Harvested,
            ready: "True",
            ready_reason: REASON_POLICY_READY,
            enforced_reason: reason,
            enforcement: ENFORCEMENT_LOGWEIR_WORKER,
            points_evaluated: 0,
            candidates: 0,
            protected: 0,
            skipped: 0,
            plan_sha256: None,
            job_name: job.map(kube::ResourceExt::name_any),
            deletes_performed: 0,
        })
    }

    async fn patch_job_ttl(&self, job: &Job) -> Result<(), ReconcileError> {
        if job
            .spec
            .as_ref()
            .and_then(|s| s.ttl_seconds_after_finished)
            .is_some()
        {
            return Ok(());
        }
        let jobs: Api<Job> = Api::namespaced(self.ctx.client.clone(), &self.namespace);
        match runner_log::set_ttl(&jobs, &job.name_any(), check::job::TTL_SECONDS, None).await {
            Ok(()) => Ok(()),
            Err(kube::Error::Api(e)) if e.code == 404 || e.code == 422 => Ok(()),
            Err(e) => Err(ReconcileError::Api(e)),
        }
    }

    async fn evaluate(&self) -> Result<Outcome, ReconcileError> {
        // The destination, resolved for two roles from one read. `ArchiveRead`
        // is what the location is resolved under: this controller reads
        // nothing from the archive, and that is the narrowest role the
        // location is available from. `EvidenceWrite` is the record
        // credential, a separate principal: the enforcement Job's
        // `LOGWEIR_EVIDENCE_AWS_*` must not carry the `archiveRead` grant,
        // which on a destination that separates principals cannot write the
        // intent tombstones (and must not write under `logweir/` anyway).
        let roles = match destination::resolve_ref_roles(
            self.ctx.client,
            &self.namespace,
            &self.policy.spec.destination_ref.name,
            DestinationRole::ArchiveRead,
            DestinationRole::EvidenceWrite,
            self.ctx.policy,
        )
        .await
        {
            Ok(r) => r,
            Err(ResolveError::Api(e)) => return Err(ReconcileError::Api(e)),
            Err(ResolveError::Refused(refusal)) => {
                return self
                    .publish_refusal(REASON_DESTINATION_UNUSABLE, &refusal.to_string())
                    .await
            }
        };
        let resolved = roles.primary;

        // The scope must be the destination's own prefix: a scope the
        // destination does not root would give a plan a bound the
        // destination's credential could still reach.
        let dest_prefix = resolved.location.prefix.trim_end_matches('/');
        let scope = self.policy.spec.scope.prefix.trim_end_matches('/');
        // On a segment boundary: a bare `starts_with` would let `…/team-ab`, a
        // sibling's prefix, pass a guard whose purpose is that a scope may only
        // narrow the destination. An empty destination prefix covers every
        // scope.
        let narrows = dest_prefix.is_empty()
            || scope == dest_prefix
            || scope.starts_with(&format!("{dest_prefix}/"));
        if !narrows {
            return self
                .publish_refusal(
                    REASON_DESTINATION_UNUSABLE,
                    &format!(
                        "spec.scope.prefix `{scope}` is not under BackupDestination {}'s own \
                         prefix `{dest_prefix}`; a retention scope may only ever narrow the \
                         destination it covers",
                        resolved.name
                    ),
                )
                .await;
        }

        // The catalog view: the pages the `RecoveryCatalog` published.
        let view = match self.view_entries().await? {
            Ok(view) => view,
            Err(message) => return self.publish_view_failure(&message).await,
        };
        let entries = &view.entries;

        let location_id = resolved.canonical_url.clone();
        let dest = plan::Destination {
            location_id: location_id.clone(),
            scope_prefix: self.policy.spec.scope.prefix.clone(),
        };
        // The controller's own verdicts, before any row is counted: a point
        // whose `Backup` the controller refused is not usable, whatever the
        // view row says. A listing that could not be completed is an
        // evaluation that could not be completed: an unseen refusal would let a
        // stale row take a keep rank and push an older good point into the
        // plan, and "could not tell" never authorises a deletion.
        let refusals = match self.controller_refusals().await? {
            Ok(refusals) => refusals,
            Err((reason, message)) => return self.publish_verdicts_failure(reason, &message).await,
        };
        let points: Vec<PointFacts> = entries.iter().map(|e| point_facts(e, &refusals)).collect();
        let protection = self.protection_set(&location_id, &points).await?;
        let holds: Vec<plan::Hold> = self
            .policy
            .spec
            .holds
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|h| plan::Hold {
                point_id: h.point_id.clone(),
                reason: h.reason.clone(),
                until: h.until,
            })
            .collect();
        let rules = self.rules();
        let max_deletions = self
            .policy
            .spec
            .enforcement
            .as_ref()
            .map_or(50, |e| i64::from(e.max_deletions_per_run));
        let evaluation = plan::evaluate(&plan::Input {
            destination: &dest,
            points: &points,
            rules,
            holds: &holds,
            protection: &protection,
            now: self.ctx.now,
            max_deletions_per_run: max_deletions,
        });

        let document = match plan::plan_document(&self.identity(), &dest, rules, &evaluation) {
            Ok(d) => d,
            Err(e) => return self.publish_plan_refusal(&e.to_string()).await,
        };
        let (plan_bytes, plan_sha256) = match plan::plan_bytes(&document) {
            Ok(pair) => pair,
            Err(e) => return self.publish_plan_refusal(&e.to_string()).await,
        };
        let plan_max_age = self
            .policy
            .spec
            .enforcement
            .as_ref()
            .map_or(3600, |e| i64::from(e.plan_max_age_seconds));
        let window = self.plan_window(&plan_sha256, plan_max_age);

        // Enforce, or not.
        let view_refusal = view.run_refusal(&self.policy.spec.catalog_ref.name, self.ctx.now);
        let decision = self.enforcement_decision(&plan_sha256, &evaluation, &window, view_refusal);
        let mut outcome = Outcome {
            phase: RetentionPhase::Evaluated,
            ready: "True",
            ready_reason: REASON_POLICY_READY,
            enforced_reason: decision.reason,
            enforcement: decision.enforcement,
            points_evaluated: evaluation.points_evaluated,
            candidates: evaluation.candidates.len(),
            protected: evaluation.protected.len(),
            skipped: evaluation.skipped.len(),
            plan_sha256: Some(plan_sha256.clone()),
            job_name: None,
            deletes_performed: 0,
        };

        // The real object count is not known here: the catalog view carries no
        // segment keys, so every candidate's `objects` is `None` ("not
        // observed"), and the `Evaluated` condition says so.
        self.publish_evaluation(
            &evaluation,
            &EvaluationBounds {
                max_deletions_per_run: max_deletions,
                view_truncated: view.truncated,
                walk_complete: view.walk_complete,
                catalog_total: view.total,
                view_entries: view.entries.len(),
                segments_visible: points.iter().any(|p| !p.segment_keys.is_empty()),
            },
            &plan_sha256,
            &window,
            &decision,
            self.policy
                .spec
                .enforcement
                .as_ref()
                .map(|_| self.retention_credential_binding(&resolved)),
        )
        .await?;

        if decision.start {
            // The slot the decision already validated; `start_run` names the
            // run after it, so the two cannot disagree.
            let slot = self
                .enforcement_slot()
                .ok()
                .flatten()
                .unwrap_or(self.ctx.now);
            match self
                .start_run(
                    &resolved,
                    roles
                        .secondary
                        .as_ref()
                        .map_err(std::convert::AsRef::as_ref),
                    roles.secondary_declared,
                    &plan_bytes,
                    &plan_sha256,
                    &evaluation.candidates,
                    slot,
                )
                .await?
            {
                StartOutcome::Started(job_name) => {
                    outcome.phase = RetentionPhase::Started;
                    outcome.job_name = Some(job_name);
                    outcome.enforced_reason = REASON_RUN_IN_PROGRESS;
                }
                StartOutcome::ActiveRestore => {
                    outcome.enforced_reason = REASON_ACTIVE_RESTORE;
                    self.publish_enforcement_refusal(
                        REASON_ACTIVE_RESTORE,
                        "a nonterminal Restore is reading this destination; no retention Job is \
                         created while one is. Retention fails closed on the whole destination \
                         until the restore-side admission hold lands (D3 §6.5).",
                    )
                    .await?;
                }
                StartOutcome::AlreadyHarvested => {
                    outcome.enforced_reason = REASON_NOTHING_TO_DO;
                }
                StartOutcome::LeaseNotHeld => {
                    outcome.enforced_reason = REASON_LEASE_NOT_HELD;
                }
                StartOutcome::RunNotRecorded => {
                    outcome.enforced_reason = REASON_RUN_NOT_RECORDED;
                }
                StartOutcome::PlanConfigMapConflict(message) => {
                    outcome.enforced_reason = REASON_PLAN_CONFIG_MAP_CONFLICT;
                    self.publish_enforcement_refusal(REASON_PLAN_CONFIG_MAP_CONFLICT, &message)
                        .await?;
                }
                StartOutcome::EvidenceGrantUnusable(message) => {
                    outcome.enforced_reason = REASON_EVIDENCE_GRANT_UNUSABLE;
                    // And the object stops claiming deletion: nothing runs here
                    // until a human edits the destination.
                    outcome.enforcement = ENFORCEMENT_RECOMMENDATION_ONLY;
                    self.publish_standing_refusal(REASON_EVIDENCE_GRANT_UNUSABLE, &message)
                        .await?;
                }
            }
        }
        Ok(outcome)
    }

    /// When this plan's approval window opened and when it closes.
    ///
    /// Anchored to the first pass that saw this digest: recomputed from `now`
    /// it would never expire, and an instant inside the digested bytes would
    /// change the digest. While `status.lastEvaluation.planSha256` equals what
    /// this pass computed, the stored `planExpiresAt` stands.
    ///
    /// A lapsed window re-anchors once, refusing that pass: an unchanged
    /// digest means the archive has not moved, but the plan's age is a gate in
    /// its own right, and an approval left lying for a week must not silently
    /// authorise today's run. The pass refuses with [`REASON_PLAN_EXPIRED`] and
    /// re-anchors, so the administrator sees a fresh preview of the same plan
    /// rather than a policy wedged for ever.
    fn plan_window(&self, plan_sha256: &str, max_age_seconds: i64) -> PlanWindow {
        let stored = self
            .policy
            .status
            .as_ref()
            .and_then(|s| s.last_evaluation.as_ref());
        let same_plan = stored
            .and_then(|e| e.plan_sha256.as_deref())
            .is_some_and(|d| d == plan_sha256);
        let stored_expiry = stored.and_then(|e| e.plan_expires_at);
        match (same_plan, stored_expiry) {
            (true, Some(expires_at)) if self.ctx.now <= expires_at => PlanWindow {
                expires_at,
                expired: false,
            },
            (true, Some(_)) => PlanWindow {
                // Re-anchored by this pass, which refuses.
                expires_at: self.ctx.now + chrono::Duration::seconds(max_age_seconds),
                expired: true,
            },
            _ => PlanWindow {
                expires_at: self.ctx.now + chrono::Duration::seconds(max_age_seconds),
                expired: false,
            },
        }
    }

    fn identity(&self) -> plan::PlanIdentity {
        plan::PlanIdentity {
            namespace: self.namespace.clone(),
            name: self.name.clone(),
            uid: self.uid.clone(),
            generation: self.generation(),
        }
    }

    /// The reached refusals among this namespace's `Backup`s
    /// ([`view::ControllerRefusals`]).
    ///
    /// Read through the lenient [`view::BackupVerdictFacts`] projection of an
    /// untyped list: one `Backup` this build cannot type refuses at most its
    /// own point and never wedges the evaluation. Bounded by
    /// [`MAX_BACKUP_PAGES`] pages of [`BACKUP_PAGE_LIMIT`].
    ///
    /// `Ok(Err((ready_reason, message)))` is a join that could not be
    /// completed (the bound, a list failure, or a refusal tied to no point),
    /// which the caller publishes as `Evaluated=False` and plans nothing on:
    /// the refusal retention did not see is exactly the one that would have
    /// enlarged the plan.
    #[allow(clippy::type_complexity)]
    async fn controller_refusals(
        &self,
    ) -> Result<Result<view::ControllerRefusals, (&'static str, String)>, ReconcileError> {
        let resource = kube::api::ApiResource::erase::<Backup>(&());
        let api: Api<kube::api::DynamicObject> =
            Api::namespaced_with(self.ctx.client.clone(), &self.namespace, &resource);
        let mut facts: Vec<view::BackupVerdictFacts> = Vec::new();
        let mut token: Option<String> = None;
        for _ in 0..MAX_BACKUP_PAGES {
            let mut params = ListParams::default().limit(BACKUP_PAGE_LIMIT);
            if let Some(cursor) = token.as_deref() {
                params = params.continue_token(cursor);
            }
            let page = match api.list(&params).await {
                Ok(page) => page,
                Err(e) => {
                    warn!(
                        policy = %self.name, namespace = %self.namespace, error = %e,
                        "the Backup verdicts could not be listed; nothing is planned"
                    );
                    return Ok(Err((
                        REASON_BACKUP_VERDICTS_UNREADABLE,
                        format!(
                            "the Backup objects in namespace {} could not be listed ({e}); \
                             retention cannot establish that the controller refused none of \
                             the points it would count as usable, so it plans nothing",
                            self.namespace
                        ),
                    )));
                }
            };
            token = page.metadata.continue_.clone().filter(|t| !t.is_empty());
            facts.extend(
                page.items
                    .iter()
                    .map(|o| view::BackupVerdictFacts::from_json(&o.data)),
            );
            if token.is_none() {
                let refusals = view::ControllerRefusals::from_facts(facts);
                if refusals.unattributed() > 0 {
                    return Ok(Err((
                        REASON_BACKUP_VERDICTS_UNREADABLE,
                        format!(
                            "{} Backup object(s) in namespace {} carry a refused or unreadable \
                             evidence verdict and name neither status.evidence.receiptSha256 \
                             nor status.backupId, so the refusal cannot be tied to a point; \
                             retention plans nothing until they are repaired or removed",
                            refusals.unattributed(),
                            self.namespace
                        ),
                    )));
                }
                return Ok(Ok(refusals));
            }
        }
        Ok(Err((
            REASON_BACKUP_HISTORY_TOO_LARGE,
            format!(
                "namespace {} holds more than {} Backup objects, the most one evaluation reads; \
                 retention cannot establish that the controller refused none of the points it \
                 would count as usable, so it plans nothing. The catalog view is fine: prune \
                 the Backup history and retention evaluates again.",
                self.namespace,
                MAX_BACKUP_PAGES * BACKUP_PAGE_LIMIT as usize
            ),
        )))
    }

    /// Read the catalog's published pages.
    ///
    /// `Ok(Err(message))` is "the view is not readable right now": an
    /// `Evaluated=False` status, not a reconcile error, because a retention
    /// evaluation failure must never block anything, least of all a backup.
    /// The entries come back with what the catalog said about the view's own
    /// bound ([`CatalogView`]).
    async fn view_entries(&self) -> Result<Result<CatalogView, String>, ReconcileError> {
        let catalogs: Api<RecoveryCatalog> =
            Api::namespaced(self.ctx.client.clone(), &self.namespace);
        let name = &self.policy.spec.catalog_ref.name;
        let Some(catalog) = catalogs.get_opt(name).await? else {
            return Ok(Err(format!(
                "namespace {} has no RecoveryCatalog named {name}; retention evaluates the \
                 catalog's bounded view and never a bucket walk of its own",
                self.namespace
            )));
        };
        // The catalog must cover the same destination: a policy for A whose
        // catalog syncs B would evaluate B's points and name A's bucket. The
        // evaluation would refuse every point anyway (their `locations[]`
        // would not match), but this names the field.
        let catalog_dest = catalog
            .spec
            .destination_ref
            .as_ref()
            .map(|d| d.name.clone());
        if catalog_dest.as_deref() != Some(self.policy.spec.destination_ref.name.as_str()) {
            return Ok(Err(format!(
                "RecoveryCatalog {name} covers destination {}, and this policy covers {}; a \
                 retention evaluation reads the view of its OWN destination and of no other",
                catalog_dest.as_deref().unwrap_or("<legacyArchive>"),
                self.policy.spec.destination_ref.name
            )));
        }
        let Some(pages) = catalog.status.as_ref().and_then(|s| s.pages.as_ref()) else {
            return Ok(Err(format!(
                "RecoveryCatalog {name} has published no view: its window expired or it has \
                 never synced. The archive is untouched; sync the catalog and retention \
                 evaluates again."
            )));
        };
        let maps: Api<ConfigMap> = Api::namespaced(self.ctx.client.clone(), &self.namespace);
        let mut entries: Vec<ViewEntry> = Vec::new();
        for page in pages {
            let Some(cm) = maps.get_opt(&page.config_map_name).await? else {
                return Ok(Err(format!(
                    "catalog page {} is gone; the view is incomplete and an incomplete view \
                     never authorises a deletion",
                    page.config_map_name
                )));
            };
            let Some(body) = cm
                .data
                .as_ref()
                .and_then(|d| d.get(view::PAGE_DATA_KEY))
                .cloned()
            else {
                return Ok(Err(format!(
                    "catalog page {} carries no `{}` key",
                    page.config_map_name,
                    view::PAGE_DATA_KEY
                )));
            };
            // The page digest, checked, and its absence is a failure:
            // `status.pages[].sha256` is over exactly these bytes, so a page
            // that changed under the reader is caught here rather than turned
            // into a plan, and a page with no digest is one nothing can check.
            let Some(expected) = page.sha256.as_deref() else {
                return Ok(Err(format!(
                    "catalog page {} carries no `status.pages[].sha256`, so its bytes cannot be \
                     checked against what the catalog published; an unverified view never \
                     authorises a deletion",
                    page.config_map_name
                )));
            };
            let lines: Vec<&str> = body.lines().filter(|l| !l.trim().is_empty()).collect();
            let found = view::page_digest(&lines);
            if found != bare_hex(expected) {
                return Ok(Err(format!(
                    "catalog page {} digests to {found} and the catalog published \
                     {expected}; the view is not what the catalog says it is",
                    page.config_map_name
                )));
            }
            for line in body.lines().filter(|l| !l.trim().is_empty()) {
                match serde_json::from_str::<ViewEntry>(line) {
                    Ok(entry) => entries.push(entry),
                    Err(e) => {
                        return Ok(Err(format!(
                            "catalog page {} carries an entry this build cannot read ({e}); a \
                             view it cannot fully read never authorises a deletion",
                            page.config_map_name
                        )))
                    }
                }
            }
        }
        let status = catalog.status.as_ref();
        Ok(Ok(CatalogView {
            entries,
            truncated: status.and_then(|s| s.truncated),
            walk_complete: status
                .and_then(|s| s.cursor.as_ref())
                .and_then(|c| c.complete),
            total: status.and_then(|s| s.counts).and_then(|c| c.total),
            expires_at: status.and_then(|s| s.view_expires_at),
        }))
    }

    /// The controller-supplied protection set: every nonterminal `Restore` that
    /// names a set at this destination.
    ///
    /// Matched on the archive location and the backup set, never the set
    /// alone: two destinations can hold sets of the same `backupId`.
    async fn active_restore_sets(
        &self,
        location_id: &str,
    ) -> Result<BTreeSet<String>, ReconcileError> {
        // A consistent list: `ListParams` with no `resource_version` is a
        // quorum read from etcd, not the watch cache, over every namespace this
        // controller acts in (`crate::scope`). A restore in a namespace the
        // controller cannot see cannot be running a Job this controller
        // created.
        let restores: Vec<Restore> =
            crate::scope::list_everywhere::<Restore>(self.ctx.client, &ListParams::default())
                .await?;
        let mut active_sets: BTreeSet<String> = BTreeSet::new();
        for restore in &restores {
            if is_terminal_restore(restore) {
                continue;
            }
            if !restore_touches(
                restore,
                &self.policy.spec.destination_ref.name,
                location_id,
                &self.namespace,
            ) {
                continue;
            }
            active_sets.insert(restore.spec.backup_set_ref.clone());
        }
        Ok(active_sets)
    }

    /// The protection set, in the vocabulary the pure evaluation uses.
    ///
    /// A `Restore` names a `backupSetRef` and the evaluation keys on point ids,
    /// so every point whose set a nonterminal restore names is protected (a
    /// restore reads the whole set).
    async fn protection_set(
        &self,
        location_id: &str,
        points: &[PointFacts],
    ) -> Result<plan::Protection, ReconcileError> {
        let active_sets = self.active_restore_sets(location_id).await?;
        let active_restore: BTreeSet<String> = points
            .iter()
            .filter(|p| active_sets.contains(&p.backup_id))
            .map(|p| p.point_id.clone())
            .collect();
        Ok(plan::Protection {
            active_restore,
            refused: self.previously_refused(),
        })
    }

    /// Points a previous run was refused on, kept until the reason clears: the
    /// bounded retry.
    fn previously_refused(&self) -> BTreeMap<String, String> {
        self.policy
            .status
            .as_ref()
            .and_then(|s| s.last_enforcement.as_ref())
            .and_then(|r| r.failed.as_ref())
            .map(|failed| {
                failed
                    .iter()
                    .filter(|f| f.code == "Locked" || f.code == "AccessDenied")
                    .map(|f| (f.point_id.clone(), f.code.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_run(
        &self,
        resolved: &ResolvedDestination,
        evidence: Result<&ResolvedDestination, &destination::DestinationRefusal>,
        evidence_declared: bool,
        plan_bytes: &[u8],
        plan_sha256: &str,
        candidates: &[plan::Candidate],
        slot: DateTime<Utc>,
    ) -> Result<StartOutcome, ReconcileError> {
        // The record credential is settled before anything is spent: a
        // destination whose `evidenceWrite` grant no enforcement Job can use is
        // a configuration problem, not a failed run, and must cost neither a
        // lease, an immutable `ConfigMap`, nor a slot of the retry budget.
        let delete_secret = self
            .policy
            .spec
            .enforcement
            .as_ref()
            .map(|e| e.credential_secret_ref.name.as_str());
        let evidence_env = match evidence_credential(evidence, evidence_declared, delete_secret) {
            Ok(env) => env,
            Err(message) => {
                warn!(
                    policy = %self.name, namespace = %self.namespace,
                    destination = %self.policy.spec.destination_ref.name,
                    "no usable evidenceWrite grant; no retention Job is created"
                );
                return Ok(StartOutcome::EvidenceGrantUnusable(message));
            }
        };
        let leased: Vec<String> = candidates.iter().map(|c| c.point_id.clone()).collect();
        // The slot is the cron's, so the run id (a pure function of it) makes
        // "one run per slot" a property of the name: a duplicate reconcile
        // inside a slot gets 409 `AlreadyExists`.
        let run_id = plan::run_id(&self.uid, plan_sha256, slot.timestamp());

        // A run this policy has already harvested is not started again.
        //
        // Every pass inside one slot that computes the same plan recomputes the
        // same run id, so `jobs.create` answers 409. Falling through would
        // record `lastEnforcement` as a new run (clearing `finishedAt`),
        // `tracked_run` would harvest the same Job again, and
        // `consecutiveRunFailures` would gain failures no Job produced,
        // degrading retention early.
        //
        // Here and not in `enforcement_decision`: the unattended branch
        // (`requireApprovedPlan: false`) returns `start: true` before the slot
        // is computed. This is the one place both branches pass through.
        //
        // Finished is the condition: a record with no `finishedAt` is a run in
        // flight, which `tracked_run` looks after.
        let recorded = self
            .policy
            .status
            .as_ref()
            .and_then(|s| s.last_enforcement.as_ref());
        if recorded.and_then(|r| r.run_id.as_deref()) == Some(run_id.as_str())
            && recorded.is_some_and(|r| r.finished_at.is_some())
        {
            debug!(
                policy = %self.name, namespace = %self.namespace, run = %run_id,
                "this plan has already run and been harvested in this slot; no Job is created \
                 and the record is left alone"
            );
            return Ok(StartOutcome::AlreadyHarvested);
        }

        // (a) The lease, with a resourceVersion-preconditioned patch, and it
        //     must land: a run nothing is holding points for must not start.
        let expires_at = self.ctx.now
            + chrono::Duration::seconds(
                self.policy
                    .spec
                    .enforcement
                    .as_ref()
                    .map_or(1800, |e| i64::from(e.deadline_seconds))
                    + LEASE_MARGIN_SECONDS,
            );
        let lease = self
            .patch_status(json!({
                "lease": {
                    "runId": run_id,
                    "pointIds": leased.clone(),
                    "acquiredAt": self.ctx.now,
                    "expiresAt": expires_at,
                },
            }))
            .await?;
        if lease != PatchOutcome::Applied {
            warn!(
                policy = %self.name, namespace = %self.namespace, run = %run_id,
                "the retention lease did not land ({lease:?}); this pass creates no Job and the \
                 next one re-evaluates from the status that did land"
            );
            return Ok(StartOutcome::LeaseNotHeld);
        }

        // (b) Then the consistent re-list. The order is the whole property: a
        //     restore that arrives after (a) is seen by (b).
        //
        //     It fails closed on the whole destination, not only the leased
        //     points: a `Restore` reading any set at this destination stops
        //     the run. A run refused for a restore it would not have touched
        //     costs one cadence slot; the other way costs the set.
        let active_sets = self.active_restore_sets(&resolved.canonical_url).await?;
        if !active_sets.is_empty() {
            warn!(
                policy = %self.name, namespace = %self.namespace,
                sets = active_sets.len(),
                "a nonterminal Restore reads this destination; no retention Job is created"
            );
            return Ok(StartOutcome::ActiveRestore);
        }

        // The plan `ConfigMap`, immutable and owned by the policy.
        let plan_name = plan::plan_config_map_name(&self.uid, plan_sha256);
        if let Err(message) = self
            .ensure_plan_config_map(&plan_name, plan_bytes, plan_sha256)
            .await?
        {
            warn!(
                policy = %self.name, namespace = %self.namespace,
                config_map = %plan_name,
                "the plan ConfigMap is not this plan's; no Job is created"
            );
            return Ok(StartOutcome::PlanConfigMapConflict(message));
        }

        let job_name = self.job_name(&run_id);
        // FX-43: the worker seals every line it prints for this controller
        // with the Job's token, and only those lines are read.
        let job = crate::job::tokened(self.build_job(
            &job_name,
            &run_id,
            &plan_name,
            plan_sha256,
            resolved,
            &evidence_env,
        ));
        let jobs: Api<Job> = Api::namespaced(self.ctx.client.clone(), &self.namespace);

        // (c) A Job already at this name ends the pass, before the record.
        //
        // This read lets the record be written before the Job exists (below)
        // without restarting a harvested run: `previously_refused()` can make
        // the plan digest oscillate back to an earlier run's, which inside one
        // slot is the same run id, and the guard at the top only compares
        // against the run last recorded.
        //
        // If the Job appears between this read and the create, it is a new Job
        // for this run, so the record this pass wrote describes it correctly;
        // the create's 409 arm keeps it.
        if jobs
            .get_opt(&job_name)
            .await
            .map_err(ReconcileError::Api)?
            .is_some()
        {
            debug!(
                policy = %self.name, namespace = %self.namespace,
                job = %job_name, run = %run_id,
                "a Job already stands at this run's deterministic name; this pass creates \
                 none, records nothing and leaves the run record alone"
            );
            return Ok(StartOutcome::AlreadyHarvested);
        }

        // (d) The run record, and then the Job.
        //
        // A record write refused after the Job was created would leave a
        // deletion Job that the status never tracks: no harvest, no reported
        // outcome, no retry budget. A 409 here is ordinary, because this
        // object's version moves under the pass constantly. Deleting the Job
        // on a refused write is not available (this controller has no `delete`
        // on `jobs`, by design), and writing first fails better: a record with
        // no Job is a run that never ran, which the next pass supersedes.
        //
        // `runId`, `startedAt` and `planSha256` are this run's own; the nine
        // terminal fields below describe a finished run and are deleted by
        // explicit RFC 7386 `null`, so the previous run's outcome cannot be
        // read as this one's (`previously_refused()` reads `failed[]`).
        //
        // The clearing is unconditional because the run reaching here is
        // always a different one: `start_run` follows `tracked_run` reporting
        // nothing in flight, and the guard at the top has already returned for
        // a harvested run of this id. A "clear only a different run" branch
        // would be unreachable.
        let record = json!({
            "runId": run_id,
            "startedAt": self.ctx.now,
            "planSha256": plan_sha256,
            "finishedAt": Value::Null,
            "exitCode": Value::Null,
            "deleted": Value::Null,
            "failed": Value::Null,
            "objectsDeleted": Value::Null,
            "recordKey": Value::Null,
            "recordSha256": Value::Null,
            "refusalCode": Value::Null,
            "refusalVariable": Value::Null,
        });
        let recorded = self
            .patch_status(json!({ "lastEnforcement": record }))
            .await?;
        if recorded != PatchOutcome::Applied {
            warn!(
                policy = %self.name, namespace = %self.namespace, run = %run_id,
                "the retention run record did not land ({recorded:?}); this pass creates no \
                 Job, so there is no deletion run the status does not track. The next pass \
                 re-evaluates from the status that did land"
            );
            return Ok(StartOutcome::RunNotRecorded);
        }

        match jobs.create(&PostParams::default(), &job).await {
            Ok(_) => {
                info!(
                    policy = %self.name, namespace = %self.namespace,
                    job = %job_name, run = %run_id,
                    "created the retention Job for an approved plan"
                );
            }
            // A deterministic name makes a duplicate reconcile a 409, not a
            // second deletion run. Reaching this arm now means the Job appeared
            // after the read above, started by a concurrent pass for this run:
            // the record this pass wrote describes it, and is left as written.
            Err(kube::Error::Api(e)) if e.code == 409 => {
                debug!(
                    job = %job_name, run = %run_id,
                    "a Job appeared at this run's deterministic name between the read and the \
                     create; this pass creates none and the record it wrote stands"
                );
                return Ok(StartOutcome::Started(job_name));
            }
            // The record is withdrawn, and the budget is not spent. A `403`
            // from a quota or webhook, a `422`, a `500`: the record landed and
            // no Job exists, so the next pass would harvest it with no exit
            // code as a failure, and three transient create failures would
            // wedge retention at `EnforcementDegraded=True` for runs that never
            // ran. So the record is withdrawn with an explicit RFC 7386 `null`
            // before the error is returned.
            //
            // Not closed: a controller that dies between the record write and
            // the create leaves the record, and the next pass counts that run
            // as a failure. Closing it needs a CRD field naming the awaited
            // Job. The window is one API call wide and fails safe: retention
            // stops sooner, never later.
            Err(e) => {
                let withdrawn = self
                    .patch_status(json!({ "lastEnforcement": Value::Null }))
                    .await?;
                warn!(
                    policy = %self.name, namespace = %self.namespace,
                    job = %job_name, run = %run_id, error = %e,
                    withdrawn = ?withdrawn,
                    "the retention Job was not created; the run record this pass wrote is \
                     withdrawn so a run that never ran is not harvested as a failed one and \
                     does not spend a retry-budget slot"
                );
                return Err(ReconcileError::Api(e));
            }
        }
        Ok(StartOutcome::Started(job_name))
    }

    #[allow(clippy::type_complexity)]
    async fn ensure_plan_config_map(
        &self,
        name: &str,
        bytes: &[u8],
        digest: &str,
    ) -> Result<Result<(), String>, ReconcileError> {
        let maps: Api<ConfigMap> = Api::namespaced(self.ctx.client.clone(), &self.namespace);
        if let Some(existing) = maps.get_opt(name).await? {
            // An object at this name is not thereby this plan. The name is
            // derived from the policy UID, so anyone who can read the status
            // can predict it, and a tenant with `create configmaps` can squat
            // it with different bytes and `immutable: true`. The worker's
            // digest check would turn that into a permanent exit 3 the
            // controller cannot repair (it holds no `delete` verb); refusing
            // here says which object and why.
            let found = existing
                .metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(plan::PLAN_DIGEST_ANNOTATION))
                .map(String::as_str);
            let body = existing
                .data
                .as_ref()
                .and_then(|d| d.get(plan::PLAN_DATA_KEY))
                .map(String::as_str)
                .unwrap_or_default();
            let actual = logweir_core::ids::sha256_prefixed(body.as_bytes());
            if found != Some(digest) || actual != digest {
                return Ok(Err(format!(
                    "ConfigMap {}/{name} exists carrying digest {} (its bytes digest to \
                     {actual}) and this pass rendered {digest}; the plan an administrator \
                     approved is not the plan at that name, so no Job is created. Remove the \
                     object to unblock — this controller holds no delete verb.",
                    self.namespace,
                    found.unwrap_or("<none>")
                )));
            }
            // Same name, same annotation, same bytes: there is nothing to do.
            return Ok(Ok(()));
        }
        let cm = ConfigMap {
            metadata: kube::api::ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(self.namespace.clone()),
                labels: Some(BTreeMap::from([
                    (
                        check::job::LABEL_MANAGED_BY.to_string(),
                        check::job::MANAGED_BY.to_string(),
                    ),
                    (
                        view::LABEL_COMPONENT.to_string(),
                        COMPONENT_RETENTION.to_string(),
                    ),
                    (LABEL_POLICY_UID.to_string(), self.uid.clone()),
                ])),
                annotations: Some(BTreeMap::from([(
                    plan::PLAN_DIGEST_ANNOTATION.to_string(),
                    digest.to_string(),
                )])),
                owner_references: Some(vec![
                    k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
                        api_version: RetentionPolicy::api_version(&()).to_string(),
                        kind: RetentionPolicy::kind(&()).to_string(),
                        name: self.name.clone(),
                        uid: self.uid.clone(),
                        controller: Some(true),
                        // `false`: `true` asks for `update` on the owner's
                        // `finalizers` subresource under
                        // OwnerReferencesPermissionEnforcement, which this
                        // ClusterRole grants on nothing.
                        block_owner_deletion: Some(false),
                    },
                ]),
                ..kube::api::ObjectMeta::default()
            },
            immutable: Some(true),
            data: Some(BTreeMap::from([(
                plan::PLAN_DATA_KEY.to_string(),
                String::from_utf8_lossy(bytes).into_owned(),
            )])),
            binary_data: None,
        };
        match maps.create(&PostParams::default(), &cm).await {
            Ok(_) | Err(kube::Error::Api(kube::core::ErrorResponse { code: 409, .. })) => {
                Ok(Ok(()))
            }
            Err(e) => Err(ReconcileError::Api(e)),
        }
    }

    fn job_name(&self, run_id: &str) -> String {
        format!("{}-{run_id}", self.stem())
    }

    /// The retention Job.
    ///
    /// It runs `logweir-retention`, not `logweir`, under its own
    /// ServiceAccount, with two separately named credentials: the
    /// delete-capable one from `spec.enforcement.credentialSecretRef`, and the
    /// destination's `evidenceWrite` grant for the record. Neither alone can
    /// both remove a point and write the document that attributes its removal.
    fn build_job(
        &self,
        job_name: &str,
        run_id: &str,
        plan_config_map: &str,
        plan_sha256: &str,
        resolved: &ResolvedDestination,
        evidence_env: &[crate::job::EnvFromSecret],
    ) -> Job {
        let enforcement = self.policy.spec.enforcement.as_ref();
        let deadline = enforcement.map_or(1800, |e| i64::from(e.deadline_seconds));
        let mut env_literal = vec![
            (env::PLAN_SHA256.to_string(), plan_sha256.to_string()),
            (env::POLICY_UID.to_string(), self.uid.clone()),
            (
                env::POLICY_GENERATION.to_string(),
                self.generation().to_string(),
            ),
            (
                env::SCOPE_PREFIX.to_string(),
                self.policy.spec.scope.prefix.clone(),
            ),
            (env::RUN_ID.to_string(), run_id.to_string()),
            (
                env::APPROVER.to_string(),
                self.approver().unwrap_or_else(|| "unattended".to_string()),
            ),
            (
                env::MAX_DELETIONS.to_string(),
                enforcement
                    .map_or(50, |e| e.max_deletions_per_run)
                    .to_string(),
            ),
            (
                env::MAX_OBJECTS.to_string(),
                enforcement
                    .map_or(20_000, |e| e.max_objects_per_run)
                    .to_string(),
            ),
            (
                env::LOCATION.to_string(),
                serde_json::to_string(&resolved.location).unwrap_or_default(),
            ),
            ("RUST_LOG".to_string(), "warn".to_string()),
        ];
        // The destination's addressing, complete and explicit, without the
        // destination grant's binding expectation: the `AWS_*` credential in
        // this pod is the delete-capable one, whose expectation is the
        // policy's own (below).
        let dest_env = resolved.job_env();
        env_literal.extend(
            dest_env
                .literals
                .iter()
                .filter(|(name, _)| {
                    name
                        != logweir_core::credential_binding::ARCHIVE_CREDENTIAL_BINDING_EXPECTED_ENV
                })
                .cloned(),
        );
        if enforcement.is_some() {
            env_literal.push((
                logweir_core::credential_binding::ARCHIVE_CREDENTIAL_BINDING_EXPECTED_ENV
                    .to_string(),
                self.retention_credential_binding(resolved),
            ));
        }
        // The record credential is the SAME destination's `evidenceWrite`
        // grant, so its expectation is that destination's binding.
        if evidence_env
            .iter()
            .any(|e| e.name == logweir_core::credential_binding::EVIDENCE_CREDENTIAL_BINDING_ENV)
        {
            env_literal.push((
                logweir_core::credential_binding::EVIDENCE_CREDENTIAL_BINDING_EXPECTED_ENV
                    .to_string(),
                resolved.credential_binding(),
            ));
        }
        env_literal.sort_by(|a, b| a.0.cmp(&b.0));
        env_literal.dedup_by(|a, b| a.0 == b.0);

        // The delete-capable credential, by `secretKeyRef` only: it never
        // appears in the Job spec, a ConfigMap, the status or a log.
        let mut env_from_secret: Vec<crate::job::EnvFromSecret> = Vec::new();
        if let Some(secret) = enforcement.map(|e| e.credential_secret_ref.name.clone()) {
            env_from_secret.push(crate::job::EnvFromSecret {
                name: destination::AWS_ACCESS_KEY_ID_ENV.to_string(),
                secret_name: secret.clone(),
                optional: false,
                key: crate::crds::backup_destination::DEFAULT_ACCESS_KEY_ID_KEY.to_string(),
            });
            env_from_secret.push(crate::job::EnvFromSecret {
                name: destination::AWS_SECRET_ACCESS_KEY_ENV.to_string(),
                secret_name: secret.clone(),
                optional: false,
                key: crate::crds::backup_destination::DEFAULT_SECRET_ACCESS_KEY_KEY.to_string(),
            });
            // And its optional binding beside it: the worker refuses a delete
            // key whose Secret is not bound to this policy, its destination's
            // route and its scope before it builds a handle.
            env_from_secret.push(crate::job::EnvFromSecret {
                name: logweir_core::credential_binding::ARCHIVE_CREDENTIAL_BINDING_ENV.to_string(),
                secret_name: secret,
                optional: true,
                key: logweir_core::credential_binding::CREDENTIAL_BINDING_KEY.to_string(),
            });
        }
        env_from_secret.extend(
            dest_env
                .from_secret
                .iter()
                .filter(|e| {
                    // The destination's archive credential is dropped here, all
                    // three variables of it: `AWS_ACCESS_KEY_ID` in a retention
                    // pod is the delete-capable grant and nothing else. A
                    // reader's `AWS_SESSION_TOKEN` beside the deleter's id and
                    // secret would be a mismatched triple, failing every call
                    // with no hint where the third part came from.
                    e.name != destination::AWS_ACCESS_KEY_ID_ENV
                        && e.name != destination::AWS_SECRET_ACCESS_KEY_ENV
                        && e.name != destination::AWS_SESSION_TOKEN_ENV
                        // And the destination grant's binding with them: its
                        // Secret is not the one in this pod.
                        && e.name
                            != logweir_core::credential_binding::ARCHIVE_CREDENTIAL_BINDING_ENV
                })
                .cloned(),
        );
        // The `evidenceWrite` grant, for the record under `logweir/`: the
        // destination's own, checked by `evidence_credential` before this pass
        // spent anything, and never one of the other two principals reused.
        env_from_secret.extend(evidence_env.iter().cloned());

        let mut job = crate::job::build(&crate::job::RunnerJobSpec {
            name: job_name.to_string(),
            namespace: self.namespace.clone(),
            owner: self.owner(),
            args: vec![
                "run".to_string(),
                "--plan".to_string(),
                format!("{PLAN_MOUNT_PATH}/{}", plan::PLAN_DATA_KEY),
                "--retention-contract-version".to_string(),
                RETENTION_CONTRACT_VERSION.to_string(),
            ],
            deadline_seconds: deadline,
            service_account_name: SERVICE_ACCOUNT.to_string(),
            secret_mounts: Vec::new(),
            config_map_mounts: vec![crate::job::ConfigMapMount {
                volume: PLAN_VOLUME.to_string(),
                config_map_name: plan_config_map.to_string(),
                mount_path: PLAN_MOUNT_PATH.to_string(),
                items: Vec::new(),
            }],
            env_from_secret,
            env_literal,
            plan_config_map: None,
            image: self.ctx.runner_image.image.clone(),
            image_pull_policy: self.ctx.runner_image.image_pull_policy.clone(),
            // `RetentionPolicy` states no resources for its Job.
            resources: None,
        });
        // The binary: `job::build` renders the runner's argv against the
        // image's default entrypoint, and retention runs a different
        // executable in the same image.
        if let Some(spec) = job.spec.as_mut() {
            if let Some(pod) = spec.template.spec.as_mut() {
                for container in &mut pod.containers {
                    if container.name == crate::job::CONTAINER_NAME {
                        container.command = Some(vec![RETENTION_BINARY.to_string()]);
                    }
                }
            }
        }
        let labels = BTreeMap::from([
            (
                check::job::LABEL_MANAGED_BY.to_string(),
                check::job::MANAGED_BY.to_string(),
            ),
            (
                view::LABEL_COMPONENT.to_string(),
                COMPONENT_RETENTION.to_string(),
            ),
            (LABEL_POLICY_UID.to_string(), self.uid.clone()),
        ]);
        job.metadata.labels = Some(labels.clone());
        if let Some(spec) = job.spec.as_mut() {
            let mut meta = spec.template.metadata.take().unwrap_or_default();
            meta.labels = Some(labels);
            spec.template.metadata = Some(meta);
        }
        job
    }

    /// Who approved the current plan, from the annotation an administrator's
    /// patch carries.
    fn approver(&self) -> Option<String> {
        self.policy
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(APPROVER_ANNOTATION))
            .cloned()
    }

    /// Whether this pass may create a Job, and what the `Enforced` condition
    /// says either way.
    fn enforcement_decision(
        &self,
        plan_sha256: &str,
        evaluation: &plan::Evaluation,
        window: &PlanWindow,
        view_refusal: Option<(&'static str, String)>,
    ) -> EnforcementDecision {
        if self.policy.spec.mode != RetentionMode::Enforce {
            return EnforcementDecision {
                start: false,
                enforcement: ENFORCEMENT_RECOMMENDATION_ONLY,
                reason: REASON_RECOMMENDATION_ONLY,
                message: "this policy reports what would be removed and removes nothing; set \
                          spec.mode: Enforce and approve a plan digest to change that"
                    .to_string(),
            };
        }
        let Some(enforcement) = self.policy.spec.enforcement.as_ref() else {
            return EnforcementDecision {
                start: false,
                enforcement: ENFORCEMENT_RECOMMENDATION_ONLY,
                reason: REASON_RECOMMENDATION_ONLY,
                message: "spec.mode is Enforce and spec.enforcement is absent".to_string(),
            };
        };
        // No run from a partial or expired view: points outside it are never
        // evaluated, so a set shared across its edge, or a point a stale view
        // still lists, cannot be proven safe to delete.
        if let Some((reason, message)) = view_refusal {
            return EnforcementDecision {
                start: false,
                enforcement: ENFORCEMENT_LOGWEIR_WORKER,
                reason,
                message,
            };
        }
        // Three consecutive failures stop scheduling until the spec changes.
        // `budget_before` returns 0 when the generation moved; one rule, read
        // here, in `harvest` and in `publish_evaluation`, so the condition and
        // the decision cannot disagree.
        let failures = self.budget_before();
        if self.budget_spent() && !self.reprobe_due() {
            return EnforcementDecision {
                start: false,
                enforcement: ENFORCEMENT_LOGWEIR_WORKER,
                reason: REASON_RUN_FAILED,
                message: format!(
                    "{failures} consecutive runs failed; {}",
                    scheduling_stop(&self.last_failure_codes()).to_lowercase()
                ),
            };
        }
        if evaluation.candidates.is_empty() {
            // An empty plan beside held-back points is not "nothing to remove":
            // every point the rules would remove is in a group (a backup set,
            // or sets linked by a shared object) larger than the ceiling, so
            // nothing is deleted until someone raises it, and `Enforced` is
            // where an operator looks to learn why.
            if evaluation.truncated_by_cap > 0 {
                return EnforcementDecision {
                    start: false,
                    enforcement: ENFORCEMENT_LOGWEIR_WORKER,
                    reason: REASON_NOTHING_FITS_CEILING,
                    message: nothing_fits_ceiling_message(
                        evaluation.truncated_by_cap,
                        i64::from(enforcement.max_deletions_per_run),
                    ),
                };
            }
            return EnforcementDecision {
                start: false,
                enforcement: ENFORCEMENT_LOGWEIR_WORKER,
                reason: REASON_NOTHING_TO_DO,
                message: "the evaluation found nothing to remove".to_string(),
            };
        }
        if !enforcement.require_approved_plan {
            return EnforcementDecision {
                start: true,
                enforcement: ENFORCEMENT_LOGWEIR_WORKER,
                reason: REASON_UNATTENDED,
                message: "spec.enforcement.requireApprovedPlan is false: this policy deletes \
                          without an administrator reviewing each plan. The choice is recorded \
                          here so it is visible on the object."
                    .to_string(),
            };
        }
        // The cadence, which is also the run id's slot. An unparseable
        // expression is a refusal, never "every reconcile".
        let slot = match self.enforcement_slot() {
            // A slot older than the plan window is not acted on:
            // `latest_due_slot` is almost always `Some`, so a controller started
            // nine months after a yearly firing would otherwise treat it as
            // due. The bound is `planMaxAgeSeconds`, the one that bounds an
            // approval.
            Ok(Some(slot))
                if self.ctx.now - slot
                    <= chrono::Duration::seconds(i64::from(enforcement.plan_max_age_seconds)) =>
            {
                slot
            }
            Ok(Some(slot)) => {
                return EnforcementDecision {
                    start: false,
                    enforcement: ENFORCEMENT_LOGWEIR_WORKER,
                    reason: REASON_NOTHING_TO_DO,
                    message: format!(
                        "the last firing of spec.enforcement.schedule was {}, longer ago than \
                         planMaxAgeSeconds; the next run is its next firing",
                        slot.to_rfc3339()
                    ),
                }
            }
            Ok(None) => {
                return EnforcementDecision {
                    start: false,
                    enforcement: ENFORCEMENT_LOGWEIR_WORKER,
                    reason: REASON_NOTHING_TO_DO,
                    message: "spec.enforcement.schedule has not come due yet".to_string(),
                }
            }
            Err(message) => {
                return EnforcementDecision {
                    start: false,
                    enforcement: ENFORCEMENT_RECOMMENDATION_ONLY,
                    reason: REASON_UNSUPPORTED_SCHEDULE,
                    message,
                }
            }
        };
        // One run per slot: a second pass inside a slot would get 409 anyway,
        // but a pass that already recorded a run for this slot should not POST
        // a Job it knows is there.
        let last_run = self
            .policy
            .status
            .as_ref()
            .and_then(|s| s.last_enforcement.as_ref())
            .and_then(|r| r.run_id.clone());
        if last_run.as_deref()
            == Some(plan::run_id(&self.uid, plan_sha256, slot.timestamp()).as_str())
        {
            return EnforcementDecision {
                start: false,
                enforcement: ENFORCEMENT_LOGWEIR_WORKER,
                reason: REASON_NOTHING_TO_DO,
                message: format!(
                    "this plan has already run in the slot beginning {}; the next run is the \
                     next firing of spec.enforcement.schedule",
                    slot.to_rfc3339()
                ),
            };
        }
        let Some(approved) = enforcement.approved_plan_sha256.as_deref() else {
            return EnforcementDecision {
                start: false,
                enforcement: ENFORCEMENT_LOGWEIR_WORKER,
                reason: REASON_AWAITING_APPROVAL,
                message: format!(
                    "the current plan digests to {plan_sha256}; set \
                     spec.enforcement.approvedPlanSha256 to exactly that to authorise the run"
                ),
            };
        };
        if approved != plan_sha256 {
            return EnforcementDecision {
                start: false,
                enforcement: ENFORCEMENT_LOGWEIR_WORKER,
                reason: REASON_PLAN_SUPERSEDED,
                message: format!(
                    "the approved digest is {approved} and the current plan digests to \
                     {plan_sha256}; the archive or the rules moved, so the approval no longer \
                     describes what would be deleted"
                ),
            };
        }
        // The age gate: `planMaxAgeSeconds` (300..86400 in the CRD) is one of
        // the four gates.
        if window.expired {
            return EnforcementDecision {
                start: false,
                enforcement: ENFORCEMENT_LOGWEIR_WORKER,
                reason: REASON_PLAN_EXPIRED,
                message: format!(
                    "the approved plan {plan_sha256} was previewed more than \
                     spec.enforcement.planMaxAgeSeconds ago; it is re-previewed rather than \
                     executed, and the window reopens at {}",
                    window.expires_at.to_rfc3339()
                ),
            };
        }
        EnforcementDecision {
            start: true,
            enforcement: ENFORCEMENT_LOGWEIR_WORKER,
            reason: REASON_RUN_IN_PROGRESS,
            message: format!("plan {plan_sha256} is approved and current"),
        }
    }

    async fn publish_evaluation(
        &self,
        evaluation: &plan::Evaluation,
        bounds: &EvaluationBounds,
        plan_sha256: &str,
        window: &PlanWindow,
        decision: &EnforcementDecision,
        credential_binding: Option<String>,
    ) -> Result<PatchOutcome, ReconcileError> {
        let candidates: Vec<Value> = evaluation
            .candidates
            .iter()
            .map(|c| {
                let mut candidate = json!({
                    "pointId": c.point_id,
                    "reason": c.reason.as_str(),
                    "recoveryPointAt": Time::from(
                        DateTime::from_timestamp_millis(c.recovery_point_at_ms)
                            .unwrap_or(self.ctx.now),
                    ),
                    // Absent when this build cannot say: the view carries no
                    // segment keys, and absent is "not observed".
                    "objects": c.objects(),
                    "bytes": c.bytes,
                });
                // And absent means no key at all, not `null`: inside a replaced
                // array a `null` member is stored (`nullable: true`), but the
                // controller reads its status back typed, where `null` and
                // absent are both `None`. A patch carrying `null`s would never
                // compare equal to the stored status, and would be sent on every
                // pass.
                if let Some(fields) = candidate.as_object_mut() {
                    fields.retain(|_, value| !value.is_null());
                }
                candidate
            })
            .collect();
        let protected: Vec<Value> = evaluation
            .protected
            .iter()
            .map(|p| json!({"pointId": p.point_id, "reason": p.reason.as_str()}))
            .collect();
        let skipped: Vec<Value> = evaluation
            .skipped
            .iter()
            .map(|s| json!({"pointId": s.point_id, "reason": s.reason.as_str()}))
            .collect();
        // `sharedSegments` reports what is true. The guarantee is "a segment
        // two points share is not removed with one of them", and `evaluate`
        // can only make it when a point carries its segment keys, which no
        // catalog view entry does; so it is `NotEnforced`. Derived from the
        // points rather than a constant, so it changes by itself the day a
        // view carries segment keys.
        //
        // Points naming one backup set are protected together (the engine's
        // own layout puts every key under `{backup_id}/`); what cannot be seen
        // is a manifest naming a segment in another set's directory. The
        // Evaluated message says exactly which half is in force.
        let segments_visible = bounds.segments_visible;
        // The refusal the newest run named stands on `Enforced` until the next
        // run starts (D-R1): over a decision to run again, and over one that
        // only waits for the cadence or a spec edit (`NothingToDo`, the spent
        // budget's `RunFailed`), which would otherwise un-say it within seconds
        // of the harvest. A decision naming something to change first (the
        // mode, the schedule, the view, the ceiling, the approval) takes
        // `Enforced`; `lastEnforcement` and `EnforcementDegraded` still name
        // the refusal. A binding refusal also holds the two fields the console
        // reads: only a human rebinding the Secret clears it
        // ([`Self::publish_standing_refusal`]).
        let held = if decision.start
            || decision.reason == REASON_NOTHING_TO_DO
            || decision.reason == REASON_RUN_FAILED
        {
            self.held_refusal()
        } else {
            None
        };
        let enforcement = if held
            .as_ref()
            .is_some_and(|(reason, _)| *reason == REASON_CREDENTIAL_BINDING_MISMATCH)
        {
            ENFORCEMENT_RECOMMENDATION_ONLY
        } else {
            decision.enforcement
        };
        let guarantees = json!({
            // Not while the retry budget is spent: a degraded policy schedules
            // no run, so "age expiry is enforced by Logweir" would contradict
            // `EnforcementDegraded`, whatever `spec.mode` asks for.
            "ageExpiry": if enforcement == ENFORCEMENT_LOGWEIR_WORKER
                && !self.budget_spent()
            {
                GUARANTEE_LOGWEIR
            } else {
                GUARANTEE_NOT_ENFORCED
            },
            "minUsablePoints": GUARANTEE_LOGWEIR,
            "activeRestoreProtection": GUARANTEE_LOGWEIR,
            "sharedSegments": if segments_visible {
                GUARANTEE_LOGWEIR
            } else {
                GUARANTEE_NOT_ENFORCED
            },
            // Not `LogweirEnforced`: `object_store` exposes no WORM readback,
            // so "legal hold respected" means "a provider refusal is
            // authoritative and recorded", never "Logweir knows the hold exists".
            "legalHold": GUARANTEE_PROVIDER_UNVERIFIED,
        });
        let conditions = self.conditions(&[
            (
                CONDITION_READY,
                "True",
                REASON_POLICY_READY,
                "the destination and the catalog resolve".to_string(),
            ),
            (
                CONDITION_EVALUATED,
                "True",
                REASON_EVALUATION_COMPLETE,
                format!(
                    "{} point(s) evaluated at this destination: {} kept, {} candidate(s), {} \
                     protected, {} skipped.{}{}{}{}",
                    evaluation.points_evaluated,
                    evaluation.kept.len(),
                    evaluation.candidates.len(),
                    evaluation.protected.len(),
                    evaluation.skipped.len(),
                    // What the two bounds left out, said on the object: the
                    // per-run ceiling's held-back points and a windowed view.
                    bounds.held_back_sentence(evaluation),
                    bounds.view_sentence(&self.policy.spec.catalog_ref.name),
                    if segments_visible {
                        ""
                    } else {
                        // Said on the object, not only in a doc comment.
                        " This catalog view carries no segment keys. Points that name one \
                         backup set are protected together (SharedSegment, matched on \
                         backupId), but a segment one set's manifest names under another \
                         set's directory cannot be seen, so sharedSegments is NotEnforced, \
                         and the plan names each set's key prefix rather than its objects:"
                    },
                    if segments_visible {
                        String::new()
                    } else {
                        format!(
                            " candidates[].objects is omitted rather than guessed, and \
                             `logweir-retention --dry-run` enumerates the real count. \
                             {} point(s) would be removed{}.",
                            evaluation.candidates.len(),
                            // Not "N would be removed" alone when the ceiling
                            // cut the plan: N is this plan, not the rules.
                            if evaluation.truncated_by_cap > 0 {
                                " by this plan"
                            } else {
                                ""
                            }
                        )
                    }
                ),
            ),
            // A refusal is not un-said by the next decision: only a later run
            // can show that its cause is gone (for a binding refusal, the
            // controller reads no Secret at all), so until one starts the
            // refusal stands rather than `NothingToDo` or `Enforced=True`
            // over a run that was refused.
            match held {
                Some((reason, message)) => (CONDITION_ENFORCED, "False", reason, message),
                None => (
                    CONDITION_ENFORCED,
                    if decision.start { "True" } else { "False" },
                    decision.reason,
                    decision.message.clone(),
                ),
            },
            (
                CONDITION_EXTERNAL_CONFLICT,
                "False",
                REASON_NO_CONFLICT,
                "no bucket lifecycle rule is declared for this destination".to_string(),
            ),
            // `EnforcementDegraded` is published by the pass that acts on it:
            // the evaluation is where scheduling stops, so it is where the
            // object must say so, from the same `budget_spent()` the decision
            // used.
            (
                CONDITION_DEGRADED,
                if self.budget_spent() { "True" } else { "False" },
                if self.budget_spent() {
                    REASON_CONSECUTIVE_FAILURES
                } else {
                    REASON_HEALTHY
                },
                if self.budget_spent() {
                    format!(
                        "{} consecutive retention runs have failed and the retry budget is \
                         spent: {}. {}. status.lastEnforcement.recordKey names the durable \
                         record of the last run.",
                        self.budget_before(),
                        self.last_failure_detail(),
                        scheduling_stop(&self.last_failure_codes())
                    )
                } else if self.spec_changed() {
                    "the spec changed; the consecutive-failure budget is released and \
                     status.consecutiveRunFailures is reset to 0"
                        .to_string()
                } else {
                    match self.last_refusal() {
                        Some((code, variable)) => format!(
                            "{} consecutive run failures; {}",
                            self.budget_before(),
                            refused_detail(code, variable)
                        ),
                        None => format!("{} consecutive run failures", self.budget_before()),
                    }
                },
            ),
        ]);
        // `lastEvaluation.at` is the evaluation that reached these findings:
        // kept while the rest of the block comes out the same, and moved by
        // the first evaluation that finds something different
        // (`keep_instant_unless_changed`, compared on what the API server will
        // store). A fresh `now` on every pass would make every pass a write
        // that wakes the next pass.
        //
        // The four counts close: `pointsEvaluated` = `keptCount` +
        // `candidateCount` + `truncatedByCap` + the points in `skipped`. A
        // point the per-run ceiling held back is in `truncatedByCap` and in no
        // list. `0` means "nothing was held back"; an absent member means an
        // older controller did not say; `viewIncomplete` is `null` (a merge
        // patch's "remove") when the catalog did not say.
        //
        // Each list is cut at the CRD's `maxItems` with its count beside it: a
        // list past the bound makes the API server refuse the whole status
        // write, conditions included.
        let count = |n: usize| i64::try_from(n).unwrap_or(i64::MAX);
        let cut = |list: &[Value]| list[..list.len().min(EVALUATION_LIST_BOUND)].to_vec();
        let kept = &evaluation.kept[..evaluation.kept.len().min(EVALUATION_LIST_BOUND)];
        let mut last_evaluation = json!({
            "at": self.ctx.now,
            "pointsEvaluated": evaluation.points_evaluated,
            "keptCount": count(evaluation.kept.len()),
            "candidateCount": count(evaluation.candidates.len()),
            "protectedCount": count(evaluation.protected.len()),
            "skippedCount": count(evaluation.skipped.len()),
            "truncatedByCap": evaluation.truncated_by_cap,
            "maxDeletionsPerRun": bounds.max_deletions_per_run,
            "viewIncomplete": bounds.view_incomplete(),
            "kept": kept,
            "candidates": cut(&candidates),
            "protected": cut(&protected),
            "skipped": cut(&skipped),
            "planSha256": plan_sha256,
            // The plan this evaluation rendered, named beside its digest. The
            // name is a pure function of the policy UID and the digest
            // (`plan::plan_config_map_name`), so it is as true as the digest,
            // even before any run materializes the object; a ref written only
            // by `start_run` would name the previous run's plan.
            "planRef": { "name": plan::plan_config_map_name(&self.uid, plan_sha256) },
            "planExpiresAt": window.expires_at,
        });
        // A member the stored block does not carry never moves the instant.
        // Over a CRD that predates `ADDITIVE_EVALUATION_FIELDS` the API server
        // prunes them, so the stored block never has them and "the block
        // changed" would be true on every pass: a hot loop for as long as the
        // controller is ahead of its CRD. They are compared as stored; the
        // patch still carries them, so a CRD that knows them stores them once.
        let stored = stored_for_instant(
            self.observed()
                .as_ref()
                .and_then(|status| status.get("lastEvaluation")),
            &last_evaluation,
        );
        keep_instant_unless_changed(stored.as_ref(), &mut last_evaluation, "at");
        let mut status = json!({
            "enforcement": enforcement,
            "guarantees": guarantees,
            "lastEvaluation": last_evaluation,
            // What `spec.enforcement.credentialSecretRef` must carry under
            // `logweir-binding`; `null` clears it when enforcement is removed.
            "credentialBinding": credential_binding,
            "conditions": conditions,
        });
        self.adopt_generation(&mut status);
        self.patch_status(status).await
    }

    /// The binding the delete-capable credential's Secret must carry: this
    /// policy's UID, the archive route of the destination it resolves to now, and
    /// its scope prefix ([`logweir_core::credential_binding::retention_binding`]).
    /// A destination re-created under the same name at another endpoint changes
    /// it, so the delete key never follows the name to a new route.
    fn retention_credential_binding(&self, resolved: &ResolvedDestination) -> String {
        logweir_core::credential_binding::retention_binding(
            &self.uid,
            &resolved.plan_storage(),
            &self.policy.spec.scope.prefix,
        )
    }

    /// Rewrite `Enforced` alone, after the evaluation has already been
    /// published, when the run was refused between the two.
    async fn publish_enforcement_refusal(
        &self,
        reason: &'static str,
        message: &str,
    ) -> Result<(), ReconcileError> {
        let conditions =
            self.conditions(&[(CONDITION_ENFORCED, "False", reason, message.to_string())]);
        self.patch_status(json!({ "conditions": conditions }))
            .await?;
        Ok(())
    }

    /// [`Self::publish_enforcement_refusal`] for a refusal that is a standing
    /// configuration state, not a per-pass race.
    ///
    /// `publish_evaluation` has already written `status.enforcement` as
    /// `LogweirWorker` and `status.guarantees.ageExpiry` as `LogweirEnforced`, and
    /// the console reads those two fields ("an isolated Logweir retention worker
    /// deletes archive objects", "enforced by Logweir"), not the condition reason.
    /// On a policy that will create no Job until a human edits the destination,
    /// both would be false. `ActiveRestore`, `LeaseNotHeld`, `RunNotRecorded` and
    /// `PlanConfigMapConflict` clear on the next reconcile by themselves and keep
    /// the plain helper; this case follows [`Self::publish_refusal`], which writes
    /// `ENFORCEMENT_RECOMMENDATION_ONLY`.
    ///
    /// `guarantees` is patched object-wise, naming one key (RFC 7386 merges
    /// object members), so the other guarantees survive. `conditions`, an array,
    /// goes through [`Self::conditions`]'s full upsert.
    async fn publish_standing_refusal(
        &self,
        reason: &'static str,
        message: &str,
    ) -> Result<(), ReconcileError> {
        let conditions =
            self.conditions(&[(CONDITION_ENFORCED, "False", reason, message.to_string())]);
        self.patch_status(json!({
            "enforcement": ENFORCEMENT_RECOMMENDATION_ONLY,
            "guarantees": { "ageExpiry": GUARANTEE_NOT_ENFORCED },
            "conditions": conditions,
        }))
        .await?;
        Ok(())
    }

    async fn publish_view_failure(&self, message: &str) -> Result<Outcome, ReconcileError> {
        // A retention evaluation failure never blocks a backup: a different
        // controller, object and condition. `Evaluated=False` is the whole of
        // it.
        let conditions = self.conditions(&[
            (
                CONDITION_READY,
                "False",
                REASON_CATALOG_UNUSABLE,
                message.to_string(),
            ),
            (
                CONDITION_EVALUATED,
                "False",
                REASON_VIEW_UNREADABLE,
                message.to_string(),
            ),
            (
                CONDITION_ENFORCED,
                "False",
                REASON_RECOMMENDATION_ONLY,
                "nothing is removed while the view is unreadable".to_string(),
            ),
        ]);
        let mut status = json!({
            "enforcement": ENFORCEMENT_RECOMMENDATION_ONLY,
            "conditions": conditions,
        });
        self.adopt_generation(&mut status);
        self.patch_status(status).await?;
        Ok(Outcome {
            ready: "False",
            ready_reason: REASON_CATALOG_UNUSABLE,
            enforced_reason: REASON_RECOMMENDATION_ONLY,
            ..refused(REASON_CATALOG_UNUSABLE)
        })
    }

    /// The controller's own `Backup` verdicts could not all be read. Not a
    /// catalog failure, so it does not say one: `Ready` names the Backup side,
    /// `Evaluated` has its own reason, and nothing is planned.
    async fn publish_verdicts_failure(
        &self,
        ready_reason: &'static str,
        message: &str,
    ) -> Result<Outcome, ReconcileError> {
        let conditions = self.conditions(&[
            (CONDITION_READY, "False", ready_reason, message.to_string()),
            (
                CONDITION_EVALUATED,
                "False",
                REASON_BACKUP_VERDICTS_INCOMPLETE,
                message.to_string(),
            ),
            (
                CONDITION_ENFORCED,
                "False",
                REASON_RECOMMENDATION_ONLY,
                "nothing is removed while the controller's Backup verdicts cannot all be read"
                    .to_string(),
            ),
        ]);
        let mut status = json!({
            "enforcement": ENFORCEMENT_RECOMMENDATION_ONLY,
            "conditions": conditions,
        });
        self.adopt_generation(&mut status);
        self.patch_status(status).await?;
        Ok(refused(ready_reason))
    }

    async fn publish_plan_refusal(&self, message: &str) -> Result<Outcome, ReconcileError> {
        let conditions = self.conditions(&[
            (
                CONDITION_READY,
                "False",
                REASON_UNSUPPORTED_COMBINATION,
                message.to_string(),
            ),
            (
                CONDITION_EVALUATED,
                "False",
                REASON_PLAN_REFUSED,
                message.to_string(),
            ),
            (
                CONDITION_ENFORCED,
                "False",
                REASON_RECOMMENDATION_ONLY,
                "no plan was written, so nothing can be approved".to_string(),
            ),
        ]);
        let mut status = json!({
            "enforcement": ENFORCEMENT_RECOMMENDATION_ONLY,
            "conditions": conditions,
        });
        self.adopt_generation(&mut status);
        self.patch_status(status).await?;
        Ok(refused(REASON_UNSUPPORTED_COMBINATION))
    }

    async fn publish_refusal(
        &self,
        reason: &'static str,
        message: &str,
    ) -> Result<Outcome, ReconcileError> {
        let conditions = self.conditions(&[
            (CONDITION_READY, "False", reason, message.to_string()),
            (
                CONDITION_ENFORCED,
                "False",
                REASON_RECOMMENDATION_ONLY,
                "nothing is removed while this policy is not ready".to_string(),
            ),
        ]);
        let mut status = json!({
            "enforcement": ENFORCEMENT_RECOMMENDATION_ONLY,
            "conditions": conditions,
        });
        self.adopt_generation(&mut status);
        self.patch_status(status).await?;
        Ok(refused(reason))
    }

    /// The full condition array, with `rows` upserted into it, never `rows`
    /// alone.
    ///
    /// A merge patch replaces an array whole, so returning only the rows a call
    /// site named would delete every other condition: `EnforcementDegraded`
    /// would vanish on the pass after `harvest` published it, leaving nothing
    /// to say the policy had stopped, and a one-row refusal would wipe
    /// `Ready`, `Evaluated` and `ExternalLifecycleConflict` mid-pass.
    ///
    /// The base is the status as this pass believes it now stands
    /// (`observed()`): a pass patches more than once and the second patch must
    /// keep what the first wrote. Order is stable (existing conditions keep
    /// their positions, new types are appended), so a diff shows what changed.
    fn conditions(&self, rows: &[(&str, &str, &str, String)]) -> Vec<Value> {
        let existing = self.existing_conditions();
        let mut out = existing.clone();
        for (r#type, status, reason, message) in rows {
            let merged = self.one_condition(&existing, r#type, status, reason, message.clone());
            match out.iter().position(|c| &c.r#type == r#type) {
                Some(at) => out[at] = merged,
                None => out.push(merged),
            }
        }
        out.into_iter()
            .map(|c| serde_json::to_value(c).unwrap_or(Value::Null))
            .collect()
    }

    /// The conditions as this pass believes they now stand.
    fn existing_conditions(&self) -> Vec<Condition> {
        self.observed()
            .and_then(|status| status.get("conditions").cloned())
            .and_then(|c| serde_json::from_value::<Vec<Condition>>(c).ok())
            .or_else(|| {
                self.policy
                    .status
                    .as_ref()
                    .and_then(|s| s.conditions.clone())
            })
            .unwrap_or_default()
    }

    /// One condition, with the `metav1.Condition` `lastTransitionTime`
    /// contract: `merge_condition` keeps the existing one when neither
    /// `status` nor `reason` moved.
    fn one_condition(
        &self,
        existing: &[Condition],
        r#type: &str,
        status: &str,
        reason: &str,
        message: String,
    ) -> Condition {
        let next = Condition {
            r#type: r#type.to_string(),
            status: status.to_string(),
            observed_generation: Some(self.generation()),
            last_transition_time: Some(self.ctx.now),
            reason: Some(reason.to_string()),
            message: Some(message),
        };
        merge_condition(existing.iter().find(|c| c.r#type == r#type), next)
    }

    /// Adopt `metadata.generation` onto the status and, when it advances,
    /// release the consecutive-failure budget in the same patch.
    ///
    /// Every writer that sets `observedGeneration` goes through here. Many
    /// passes return through a refusal writer before the evaluation is reached
    /// (an unreadable view, a refused plan, an unusable destination, a declared
    /// external lifecycle), and the runs were failing for a reason, so a
    /// refusal co-occurring with the operator's spec edit is the likely case.
    /// If those writers adopted the generation without releasing the budget,
    /// the edit would be consumed and the budget spent again. Adopting and
    /// releasing are the same event and the same patch, so a 409 cannot split
    /// them.
    ///
    /// The `EnforcementDegraded` condition is cleared here too, so an operator
    /// never sees `EnforcementDegraded=True` beside `consecutiveRunFailures: 0`.
    fn adopt_generation(&self, status: &mut Value) {
        let object = status
            .as_object_mut()
            .expect("a status patch is always a JSON object");
        object.insert("observedGeneration".to_string(), json!(self.generation()));
        if !self.spec_changed() {
            return;
        }
        // The helper fills in what the caller did not say and never overrides
        // it: `harvest` writes the count this run produced (already released by
        // `budget_before()`, so a failure under the new spec is one failure),
        // and a writer that published its own `EnforcementDegraded` has more to
        // say than "released".
        //
        // An explicit `0`, never a deletion: "no consecutive failures" is an
        // answer. Inserted only on the pass that adopts a new generation;
        // carried on every pass, it would overwrite the harvests' count.
        object
            .entry("consecutiveRunFailures".to_string())
            .or_insert_with(|| json!(0));
        // "The caller said it" means this pass: `conditions()` preserves every
        // condition the object carries, so presence alone proves nothing. A row
        // written this pass carries this pass's `observedGeneration`; a row
        // carried forward has the old one. So `harvest`'s fresh verdict wins,
        // and a stale `True` from before the edit is replaced.
        let written_this_pass = object
            .get("conditions")
            .and_then(Value::as_array)
            .is_some_and(|conditions| {
                conditions.iter().any(|c| {
                    c["type"] == CONDITION_DEGRADED
                        && c["observedGeneration"] == json!(self.generation())
                })
            });
        if written_this_pass {
            return;
        }
        let released = serde_json::to_value(
            self.one_condition(
                &self.existing_conditions(),
                CONDITION_DEGRADED,
                "False",
                REASON_HEALTHY,
                "the spec changed; the consecutive-failure budget is released and \
             status.consecutiveRunFailures is reset to 0"
                    .to_string(),
            ),
        )
        .unwrap_or(Value::Null);
        match object.get_mut("conditions").and_then(Value::as_array_mut) {
            Some(conditions) => match conditions
                .iter()
                .position(|c| c["type"] == CONDITION_DEGRADED)
            {
                Some(at) => conditions[at] = released,
                None => conditions.push(released),
            },
            // A writer that carries no conditions still gets the release one,
            // built over the full existing array so nothing is dropped.
            None => {
                object.insert(
                    "conditions".to_string(),
                    json!(self.conditions(&[(
                        CONDITION_DEGRADED,
                        "False",
                        REASON_HEALTHY,
                        "the spec changed; the consecutive-failure budget is released and \
                         status.consecutiveRunFailures is reset to 0"
                            .to_string(),
                    )])),
                );
            }
        }
    }

    /// The version the next patch will precondition on (seam **S7**: every
    /// status write is a resourceVersion-preconditioned merge PATCH).
    fn version(&self) -> Option<String> {
        self.resource_version
            .lock()
            .expect("the version cursor is uncontended")
            .clone()
    }

    fn set_version(&self, next: Option<String>) {
        *self
            .resource_version
            .lock()
            .expect("the version cursor is uncontended") = next;
    }

    /// The status as this pass believes it now stands.
    fn observed(&self) -> Option<Value> {
        self.observed_status
            .lock()
            .expect("the status cursor is uncontended")
            .clone()
    }

    async fn patch_status(&self, status: Value) -> Result<PatchOutcome, ReconcileError> {
        let patch = json!({ "status": status });
        // Cloned out before the await: a guard alive across an `.await` makes
        // the reconcile future non-`Send`.
        let observed = self.observed().clone();
        if status_unchanged(observed.as_ref(), &patch) {
            debug!(
                policy = %self.name, namespace = %self.namespace,
                "the computed status equals the one this pass has written; no patch is sent"
            );
            return Ok(PatchOutcome::Unchanged);
        }
        let Some(resource_version) = self.version() else {
            warn!(
                policy = %self.name, namespace = %self.namespace,
                "{NO_RESOURCE_VERSION}"
            );
            return Ok(PatchOutcome::Conflict);
        };
        let mut body = patch.clone();
        body.as_object_mut()
            .expect("a status patch is always a JSON object")
            .insert(
                "metadata".to_string(),
                json!({ "name": self.name, "resourceVersion": resource_version }),
            );
        let api: Api<RetentionPolicy> = Api::namespaced(self.ctx.client.clone(), &self.namespace);
        match api
            .patch_status(&self.name, &PatchParams::default(), &Patch::Merge(body))
            .await
        {
            Ok(applied) => {
                // The cursor moves: the only fresh version available to a
                // reconciler with no `get` on its own kind.
                self.set_version(
                    applied
                        .metadata
                        .resource_version
                        .clone()
                        .filter(|v| !v.is_empty()),
                );
                let mut merged = observed.unwrap_or(Value::Null);
                if let Some(next) = patch.get("status") {
                    crate::conditions::apply_merge_patch(&mut merged, next);
                }
                *self
                    .observed_status
                    .lock()
                    .expect("the status cursor is uncontended") = Some(merged);
                Ok(PatchOutcome::Applied)
            }
            // A 409 is the precondition working: something wrote this status
            // since the read, so the object in hand is stale. Returned, not
            // swallowed (see `PatchOutcome::Conflict`).
            Err(kube::Error::Api(e)) if e.code == 409 => {
                debug!(
                    policy = %self.name, namespace = %self.namespace,
                    "the status changed under this reconcile (409); the next pass reads it"
                );
                // The cursor is now unknowable: clearing it makes every later
                // patch in this pass a no-op rather than a blind write.
                self.set_version(None);
                Ok(PatchOutcome::Conflict)
            }
            Err(e) => Err(ReconcileError::Api(e)),
        }
    }
}

/// The catalog view one evaluation read, with what the catalog said about the
/// view's own bounds.
struct CatalogView {
    /// Every entry of every published page.
    entries: Vec<ViewEntry>,
    /// `RecoveryCatalog.status.truncated`: whether the view is a window over
    /// an archive that holds more points. `None` when the catalog did not say.
    truncated: Option<bool>,
    /// `RecoveryCatalog.status.cursor.complete`: whether the walk that built
    /// the view finished, or stopped on its object budget
    /// (`Synced=False/ScanIncomplete`) with the pages published anyway.
    walk_complete: Option<bool>,
    /// `RecoveryCatalog.status.counts.total`: every point the walk saw.
    total: Option<i64>,
    /// `RecoveryCatalog.status.viewExpiresAt`.
    expires_at: Option<DateTime<Utc>>,
}

impl CatalogView {
    /// Why an `Enforce` run may not start from this view, or `None` when the
    /// catalog said the view is current and holds every point. A run deletes,
    /// so "not said" refuses like "no".
    fn run_refusal(&self, catalog: &str, now: DateTime<Utc>) -> Option<(&'static str, String)> {
        match self.expires_at {
            Some(at) if at > now => {}
            expired => {
                let when = expired.map_or_else(
                    || "states no status.viewExpiresAt".to_string(),
                    |at| format!("expired at {} (status.viewExpiresAt)", at.to_rfc3339()),
                );
                return Some((
                    REASON_VIEW_EXPIRED,
                    format!(
                        "RecoveryCatalog {catalog}'s view {when}, so it no longer describes the \
                         archive: no run starts and nothing is deleted. Let the catalog sync \
                         (its Synced condition says why it has not)."
                    ),
                ));
            }
        }
        let mut causes: Vec<&str> = Vec::new();
        // No remedy the catalog cannot take: `spec.sync` is immutable, and
        // `viewLimit` cures only a window the limit cut.
        if self.truncated == Some(true) {
            causes.push(
                "its view is a window (status.truncated). When spec.sync.viewLimit cut it, \
                 re-create the RecoveryCatalog with a larger viewLimit (spec.sync is \
                 immutable); an archive of more than 5000 points, the largest viewLimit, is \
                 never enforced and reads ViewIncomplete. A larger viewLimit does not help a \
                 view cut for page space, for an entry too large for one page or by merged \
                 duplicate rows, nor a mode: Full catalog resuming a walk that does not finish \
                 in one sync (re-create it with a larger spec.sync.maxObjectsPerRun, or use \
                 mode: Index)",
            );
        }
        if self.walk_complete == Some(false) {
            causes.push(
                "its walk stopped before the end (status.cursor.complete is false); the \
                 catalog's Synced condition says why: its object budget \
                 (spec.sync.maxObjectsPerRun), its clock, or a shard it could not read",
            );
        }
        if causes.is_empty() && (self.truncated.is_none() || self.walk_complete.is_none()) {
            causes.push(
                "it did not say whether its walk finished and its view is whole; let the \
                 catalog sync",
            );
        }
        if causes.is_empty() {
            return None;
        }
        Some((
            REASON_VIEW_INCOMPLETE,
            format!(
                "RecoveryCatalog {catalog}'s view may not hold every point of the archive: {}. \
                 A run deletes only what the whole archive proves safe, so no run starts from \
                 this view and nothing is deleted.",
                causes.join("; ")
            ),
        ))
    }
}

/// What bounded one evaluation beside the rules: the facts that make its
/// counts a complete account, or say why they are not. A bound that cut a
/// computation is published with the result it cut: the per-run ceiling, and
/// a catalog view that is a window or the pages of an unfinished walk.
struct EvaluationBounds {
    /// The per-run point ceiling `plan::evaluate` applied.
    max_deletions_per_run: i64,
    /// [`CatalogView::truncated`].
    view_truncated: Option<bool>,
    /// [`CatalogView::walk_complete`].
    walk_complete: Option<bool>,
    /// [`CatalogView::total`].
    catalog_total: Option<i64>,
    /// How many entries the view held.
    view_entries: usize,
    /// Whether any point carried its segment keys — see `sharedSegments` in
    /// [`Pass::publish_evaluation`].
    segments_visible: bool,
}

impl EvaluationBounds {
    /// The `Evaluated` message's sentence about the per-run ceiling. Empty
    /// when the ceiling held nothing back.
    ///
    /// When the plan is empty the sentence promises no later plan: every due
    /// point is in a backup set larger than the ceiling, and a set is planned
    /// whole or not at all, so the next evaluation finds the same. The remedy
    /// is the ceiling, and the sentence names it.
    fn held_back_sentence(&self, evaluation: &plan::Evaluation) -> String {
        if evaluation.truncated_by_cap <= 0 {
            return String::new();
        }
        if evaluation.candidates.is_empty() {
            return format!(
                " {} point(s) are due under the rules and held back by the per-run ceiling \
                 (maxDeletionsPerRun {}): they are not kept, and this plan is empty because not \
                 one of them fits. Each is in a backup set that more due points name than the \
                 ceiling (sets that share objects count as one), a set is planned whole or not \
                 at all, and no plan names them until maxDeletionsPerRun is raised.",
                evaluation.truncated_by_cap, self.max_deletions_per_run
            );
        }
        format!(
            " {} more point(s) are due under the rules and held back by the per-run ceiling \
             (maxDeletionsPerRun {}): they are not kept and not in this plan, and they stay due \
             until a later plan names them.",
            evaluation.truncated_by_cap, self.max_deletions_per_run
        )
    }

    /// `status.lastEvaluation.viewIncomplete`: `Some(true)` when the catalog
    /// said its view does not hold every point (a window, or an unfinished
    /// walk); `Some(false)` only when it said both that the walk finished and
    /// that the view is whole; `None` when it did not say. An unknown half
    /// never reads as "whole".
    fn view_incomplete(&self) -> Option<bool> {
        if self.view_truncated == Some(true) || self.walk_complete == Some(false) {
            return Some(true);
        }
        (self.view_truncated == Some(false) && self.walk_complete == Some(true)).then_some(false)
    }

    /// The `Evaluated` message's sentences about a catalog view that does not
    /// hold every point. Empty unless the catalog said so.
    fn view_sentence(&self, catalog: &str) -> String {
        let mut out = String::new();
        if self.view_truncated == Some(true) {
            let counted = match self.catalog_total {
                Some(total) => format!(
                    "RecoveryCatalog {catalog} counted {total} point(s) and its view holds {}",
                    self.view_entries
                ),
                None => format!(
                    "RecoveryCatalog {catalog} reports status.truncated and its view holds {}",
                    self.view_entries
                ),
            };
            // `status.truncated` has more than one cause: more points than
            // `spec.sync.viewLimit`, entries left out for page space, an entry
            // too large for a page, or rows merged as duplicates
            // (`catalog_view::materialise`). Raising `viewLimit` helps only the
            // first, so the remedy is named with its condition.
            out.push_str(&format!(
                " The catalog view is a window: {counted}. The points outside it were not \
                 evaluated, are in none of these counts, and are never candidates while they \
                 stay outside the view. Raising spec.sync.viewLimit brings them in only when \
                 the limit is what cut the view: the catalog also reports status.truncated \
                 when it left entries out for page space or as too large for one page, and \
                 when it merged duplicate rows."
            ));
        }
        if self.walk_complete == Some(false) {
            out.push_str(&format!(
                " The catalog view is incomplete: RecoveryCatalog {catalog}'s walk stopped on \
                 its object budget before it finished (status.cursor.complete is false) and its \
                 view holds {} point(s). The points the walk has not reached were not \
                 evaluated, are in none of these counts, and are never candidates until a sync \
                 reaches them.",
                self.view_entries
            ));
        }
        out
    }
}

/// The `lastEvaluation` members added after the first release. Each is
/// additive: an older status does not carry them, and an older CRD prunes
/// them.
pub const ADDITIVE_EVALUATION_FIELDS: [&str; 6] = [
    "keptCount",
    "truncatedByCap",
    "maxDeletionsPerRun",
    "viewIncomplete",
    "protectedCount",
    "skippedCount",
];

/// `stored` as the "did the findings change?" comparison should see it: with
/// every [`ADDITIVE_EVALUATION_FIELDS`] member it lacks taken from `next`.
///
/// So a block that differs from the stored one only by members the stored one
/// lacks keeps its instant (see the call site: one write versus a hot loop). A
/// member the stored block carries is compared as usual, and a `null` in
/// `next` (a merge patch's "remove") is not copied.
///
/// The cost: `viewIncomplete` first appearing does not move `at` on its own.
/// A readable view always comes with the catalog's `status.truncated` and
/// `status.cursor`, so that transition does not arise from a view this
/// controller evaluates.
#[must_use]
pub fn stored_for_instant(stored: Option<&Value>, next: &Value) -> Option<Value> {
    let mut stored = stored?.clone();
    let fields = stored.as_object_mut()?;
    for key in ADDITIVE_EVALUATION_FIELDS {
        if fields.contains_key(key) {
            continue;
        }
        if let Some(value) = next.get(key).filter(|v| !v.is_null()) {
            fields.insert(key.to_string(), value.clone());
        }
    }
    Some(stored)
}

/// What a finished retention run reported about itself.
///
/// Parsed from the worker's `retention-point=` / `retention-record=` /
/// `retention-result=` key lines, by name and never by position: a pod log is
/// stdout and stderr merged in nondeterministic order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunReport {
    /// The process exit code, from the pod's terminated state.
    pub exit_code: Option<i32>,
    /// The points it removed entirely.
    pub deleted: Vec<String>,
    /// The points it could not, with their closed codes.
    pub failed: Vec<(String, String)>,
    /// How many object keys went.
    pub objects_deleted: i64,
    /// Where the attributable record landed.
    pub record_key: Option<String>,
    /// That record's digest.
    pub record_sha256: Option<String>,
    /// Why the run's pod was never created, when the Job controller said so:
    /// the `FailedCreate` row of [`crate::check::waiting`], redacted. `Some`
    /// only for a run with no pod at all, so `exit_code` is `None`.
    pub refused: Option<String>,
    /// The worker's `retention-refusal=` code, from the closed
    /// [`RETENTION_REFUSALS`] table: `Some` only for a run that exited 3 on a
    /// refusal the status names.
    pub refusal_code: Option<&'static str>,
    /// The binding variable that refusal names (`variable=`), one of the nine
    /// [`env`] names this controller projects, or `None`.
    pub refusal_variable: Option<&'static str>,
}

/// A refusal as `Enforced` and `Degraded` name it: the code, and the variable
/// in parentheses.
fn refusal_words(code: &str, variable: Option<&str>) -> String {
    match variable {
        Some(variable) => format!("{code} ({variable})"),
        None => code.to_string(),
    }
}

/// `EnforcementDegraded`'s detail for a run that exited 3 on a named refusal.
fn refused_detail(code: &str, variable: Option<&str>) -> String {
    format!(
        "the last run exited 3, refused {}",
        refusal_words(code, variable)
    )
}

/// `Enforced`'s reason and message for run `run_id`, which exited 3 on the
/// named refusal `code`. Only `CredentialBindingMismatch` is a reason of its
/// own; every other refusal is `RunFailed`, named in the message.
fn refused_run_enforced(
    run_id: &str,
    code: &str,
    variable: Option<&str>,
) -> (&'static str, String) {
    if code == logweir_core::runner_line::RUNNER_OLDER_THAN_CONTROLLER {
        return (
            REASON_RUN_FAILED,
            format!(
                "retention run {run_id} was REFUSED before deleting anything (exit 3, {code}): \
                 the retention worker printed no line sealed with this Job's line token, which \
                 every worker at least as new as this controller does, so the runner image is \
                 older than the controller (an older worker refuses `{}`). Nothing of its log \
                 was read and the archive is untouched. Upgrade the runner image to the \
                 controller's version (the chart's `runnerImage`)",
                logweir_core::runner_line::RUNNER_LINE_TOKEN_ARG
            ),
        );
    }
    if code == REASON_CREDENTIAL_BINDING_MISMATCH {
        return (
            REASON_CREDENTIAL_BINDING_MISMATCH,
            format!(
                "retention run {run_id} REFUSED a credential before building any handle \
                 ({REASON_CREDENTIAL_BINDING_MISMATCH}): the Secret \
                 spec.enforcement.credentialSecretRef names, or the destination's \
                 evidenceWrite Secret, carries no `logweir-binding` or one written for another \
                 policy, destination route or scope. Nothing was deleted. {}",
                logweir_core::credential_binding::BINDING_REMEDY
            ),
        );
    }
    (
        REASON_RUN_FAILED,
        format!(
            "retention run {run_id} was REFUSED before deleting anything (exit 3, {}); the \
             archive is untouched",
            refusal_words(code, variable)
        ),
    )
}

impl RunReport {
    /// Whether the only thing that stopped this run was its own object
    /// ceiling. Such a run exits 1 (work remains) and must not count toward
    /// `consecutiveRunFailures`, or three bounded runs on a large archive would
    /// stop retention for good.
    #[must_use]
    pub fn bounded_only(&self) -> bool {
        !self.failed.is_empty()
            && self
                .failed
                .iter()
                .all(|(_, code)| code == BUDGET_EXHAUSTED_CODE)
    }
}

/// The worker's code for "I stopped on my own ceiling".
pub const BUDGET_EXHAUSTED_CODE: &str = "BudgetExhausted";

/// A retention Job's report, from the lines its worker sealed with the Job's
/// token (FX-43, [`runner_log::own_lines`]): a `retention-point=`,
/// `retention-refusal=` or `retention-record=` line a provider's error body or
/// a plan got into the log is not read. A Job given the token whose worker
/// vouched for no exit and refused the flag (an older worker, exit 3) is the
/// mixed pair, named: `refusal_code` is `RunnerOlderThanController` and
/// nothing else of the log is read. A Job an older controller built is read
/// as it always was. Pure.
#[must_use]
pub fn run_report(token: &crate::job::JobToken, log: &str, exit_code: Option<i32>) -> RunReport {
    let lines = runner_log::own_lines(token, log);
    if !log.is_empty()
        && exit_code.is_some_and(runner_log::vouches_for_exit)
        && lines.runner_is_older()
    {
        return RunReport {
            exit_code,
            refusal_code: Some(logweir_core::runner_line::RUNNER_OLDER_THAN_CONTROLLER),
            ..RunReport::default()
        };
    }
    parse_run_lines(lines.text(), exit_code)
}

/// Read a retention Job's key lines. Pure, and bounded: only lines carrying
/// one of the three prefixes are read, and an unparseable value is skipped,
/// not guessed. [`run_report`] hands it the lines the worker sealed.
#[must_use]
pub fn parse_run_lines(log: &str, exit_code: Option<i32>) -> RunReport {
    let mut report = RunReport {
        exit_code,
        ..RunReport::default()
    };
    for line in log.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("retention-point=") {
            let mut fields = rest.split_whitespace();
            let Some(point_id) = fields.next() else {
                continue;
            };
            let mut state = None;
            let mut code = None;
            for field in fields {
                if let Some(v) = field.strip_prefix("state=") {
                    state = Some(v.to_string());
                } else if let Some(v) = field.strip_prefix("code=") {
                    code = Some(v.to_string());
                }
            }
            match state.as_deref() {
                Some("Deleted") => report.deleted.push(point_id.to_string()),
                Some(_) => report.failed.push((
                    point_id.to_string(),
                    code.unwrap_or_else(|| "Unknown".to_string()),
                )),
                None => {}
            }
        } else if let Some(rest) = line.strip_prefix("retention-refusal=") {
            // A closed table: the code is one this build knows, or nothing is
            // read from the line, and the variable likewise from the nine names
            // this controller projects.
            let mut fields = rest.split_whitespace();
            let code = fields.next().unwrap_or_default();
            report.refusal_code = RETENTION_REFUSALS
                .iter()
                .copied()
                .find(|known| *known == code);
            report.refusal_variable = report.refusal_code.and_then(|_| {
                let named = fields.find_map(|f| f.strip_prefix("variable="))?;
                env::BINDING.iter().copied().find(|known| *known == named)
            });
        } else if let Some(rest) = line.strip_prefix("retention-record=") {
            let mut fields = rest.split_whitespace();
            report.record_key = fields.next().map(str::to_string);
            for field in fields {
                if let Some(v) = field.strip_prefix("sha256=") {
                    report.record_sha256 = Some(v.to_string());
                }
            }
        } else if let Some(rest) = line.strip_prefix("retention-result=") {
            for field in rest.split_whitespace() {
                if let Some(v) = field.strip_prefix("objects=") {
                    if let Ok(n) = v.parse::<i64>() {
                        report.objects_deleted = n;
                    }
                }
            }
        }
    }
    report
}

/// What `start_run` did, and why it did not do more.
///
/// A named enum: "the lease did not land", "a restore is reading this
/// destination" and "something else owns the plan ConfigMap's name" are
/// different findings an operator fixes in different places.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartOutcome {
    /// The Job was created (or already existed at its deterministic name).
    Started(String),
    /// The run this plan and slot name has already been harvested, or a Job
    /// already stands at its deterministic name; there is nothing to start and
    /// nothing to record.
    AlreadyHarvested,
    /// The lease PATCH did not land, so nothing is holding these points.
    LeaseNotHeld,
    /// The run record PATCH did not land, so no Job was created: a run the
    /// status does not track is one nothing harvests, counts or accounts for.
    RunNotRecorded,
    /// A nonterminal `Restore` reads this destination.
    ActiveRestore,
    /// An object already holds the plan `ConfigMap`'s name and is not this
    /// plan.
    PlanConfigMapConflict(String),
    /// The destination has no `spec.access.evidenceWrite` grant an enforcement
    /// Job could write its record with. Returned before the lease, so the pass
    /// spends no lease, `ConfigMap`, run record or retry-budget slot on a
    /// configuration problem.
    EvidenceGrantUnusable(String),
}

/// How long the current plan stays approvable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanWindow {
    /// When the window closes. Published as
    /// `status.lastEvaluation.planExpiresAt`.
    pub expires_at: DateTime<Utc>,
    /// Whether this pass observed the previous window lapse. Such a pass
    /// starts no run and re-anchors the window, so the administrator's next
    /// look sees a fresh preview of the same plan.
    pub expired: bool,
}

/// Whether the policy may start a Job, and what to say about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnforcementDecision {
    /// Whether a Job is created this pass.
    pub start: bool,
    /// `status.enforcement`.
    pub enforcement: &'static str,
    /// `Enforced`'s reason.
    pub reason: &'static str,
    /// `Enforced`'s message.
    pub message: String,
}

/// `Enforced=False/NothingFitsCeiling`'s message: how many points are due, the
/// ceiling none of them fits, and what to change. A ceiling of `due` fits all
/// of them in one plan; the field's maximum is 500, and a larger backlog goes
/// set by set.
#[must_use]
pub fn nothing_fits_ceiling_message(due: i64, ceiling: i64) -> String {
    let remedy = if due <= i64::from(MAX_DELETIONS_PER_RUN_LIMIT) {
        format!(
            "Raise spec.enforcement.maxDeletionsPerRun to at least the number of points that \
             name the smallest of those sets; {due} fits all of them"
        )
    } else {
        format!(
            "Raise spec.enforcement.maxDeletionsPerRun (at most {MAX_DELETIONS_PER_RUN_LIMIT}) \
             to at least the number of points that name the smallest of those sets"
        )
    };
    format!(
        "{due} point(s) are due under the rules and the plan is empty: not one of them fits \
         the per-run ceiling (spec.enforcement.maxDeletionsPerRun {ceiling}). Each is in a \
         backup set that more due points name than the ceiling (sets that share objects count \
         as one), and a set is planned whole or not at all, so no run removes them at this \
         ceiling. {remedy}."
    )
}

/// The largest `spec.enforcement.maxDeletionsPerRun` the CRD admits.
pub const MAX_DELETIONS_PER_RUN_LIMIT: i32 = 500;

/// The three `LOGWEIR_EVIDENCE_AWS_*` references an enforcement Job writes its
/// intent tombstone and run record with, or the message naming the field the
/// destination cannot supply one from.
///
/// Two principals: the delete grant (`spec.enforcement.credentialSecretRef`),
/// which may remove a point and may not write under `logweir/`, and the record
/// grant (the destination's `spec.access.evidenceWrite`), which may create
/// under `logweir/` and may not delete. Neither alone is enough, which makes a
/// deletion attributable, and this is where the one deleting Job's variables
/// are decided (`docs/kubernetes.md`, "Enforcing").
///
/// `evidenceWrite` absent still means `archiveWrite`, as
/// [`destination::resolve`] defaults it: absent grants do not widen, and an
/// installation that never separated principals does not move on upgrade. The
/// role is resolved separately from `archiveRead`, the read-only principal,
/// which is never a fall-back here.
///
/// Refused, each because it could only be a Job that refuses itself:
///
/// 1. **The role's own resolver refusal**, a malformed grant.
/// 2. **A grant with no static keys.** `logweir-retention`'s
///    `EvidenceSink::open` uses `StoreOptions::static_keys` and has no
///    workload-identity path, so a `WorkloadIdentity` grant would exit 3 after
///    spending a lease, a `ConfigMap`, a run record and a retry-budget slot.
/// 3. **The record grant is the delete Secret**: one principal that both
///    removes a point and attributes the removal. This catches the name, not
///    the material: two Secrets may hold identical keys, and keeping the
///    principals distinct remains the operator's obligation.
///
/// `declared` only decides which field the message names (the operator's own
/// `spec.access.evidenceWrite`, or the `spec.access.archiveWrite` it defaulted
/// to), so the sentence points at the line they wrote.
pub fn evidence_credential(
    evidence: Result<&ResolvedDestination, &destination::DestinationRefusal>,
    declared: bool,
    delete_secret: Option<&str>,
) -> Result<Vec<crate::job::EnvFromSecret>, String> {
    let evidence = match evidence {
        Ok(resolved) => resolved,
        Err(refusal) => {
            return Err(format!(
                "{refusal}; no Job was created and nothing was deleted"
            ))
        }
    };
    let where_ = format!("BackupDestination {}/{}", evidence.namespace, evidence.name);
    // The field the operator actually wrote: an object that declares no
    // `evidenceWrite` got the grant from `archiveWrite`, and the message says
    // so.
    let field = if declared {
        "spec.access.evidenceWrite"
    } else {
        "spec.access.archiveWrite (spec.access.evidenceWrite is absent and defaults to it)"
    };
    match &evidence.grant {
        destination::ResolvedGrant::SecretKeys {
            secret,
            access_key_id_key,
            secret_access_key_key,
            session_token_key,
        } => {
            if delete_secret == Some(secret.as_str()) {
                return Err(format!(
                    "{where_} resolves the retention record credential to Secret {secret} from \
                     {field}, which is the same Secret spec.enforcement.credentialSecretRef \
                     names as the DELETE credential. One principal that both removes a point \
                     and writes the record attributing its removal can forge that record, and \
                     the two-credential rule in docs/kubernetes.md §7f is that neither alone is \
                     enough. Name a different Secret for one of them. This compares NAMES only: \
                     two differently named Secrets holding the same keys are the same principal \
                     and no controller can see that. No Job was created and nothing was deleted."
                ));
            }
            let mut out = vec![
                crate::job::EnvFromSecret {
                    name: destination::EVIDENCE_ACCESS_KEY_ID_ENV.to_string(),
                    secret_name: secret.clone(),
                    optional: false,
                    key: access_key_id_key.clone(),
                },
                crate::job::EnvFromSecret {
                    name: destination::EVIDENCE_SECRET_ACCESS_KEY_ENV.to_string(),
                    secret_name: secret.clone(),
                    optional: false,
                    key: secret_access_key_key.clone(),
                },
            ];
            if let Some(token) = session_token_key {
                out.push(crate::job::EnvFromSecret {
                    name: destination::EVIDENCE_SESSION_TOKEN_ENV.to_string(),
                    secret_name: secret.clone(),
                    optional: false,
                    key: token.clone(),
                });
            }
            // The record Secret's optional binding, beside it; the Job builder
            // adds the destination's expectation.
            out.push(crate::job::EnvFromSecret {
                name: logweir_core::credential_binding::EVIDENCE_CREDENTIAL_BINDING_ENV.to_string(),
                secret_name: secret.clone(),
                optional: true,
                key: logweir_core::credential_binding::CREDENTIAL_BINDING_KEY.to_string(),
            });
            Ok(out)
        }
        other => Err(format!(
            "{where_} resolves the retention record credential from {field} to a {} grant, and \
             the retention worker builds its record store from the static keys \
             LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID and LOGWEIR_EVIDENCE_AWS_SECRET_ACCESS_KEY only \
             — it has no workload-identity path, so a Job carrying that grant could do nothing \
             but refuse itself. Give the destination a SecretKeys spec.access.evidenceWrite \
             grant: create-only puts under logweir/, and no delete. No Job was created and \
             nothing was deleted.",
            grant_mode(other)
        )),
    }
}

/// A grant's `mode` as `spec.access.<role>.mode` spells it, never the grant's
/// `Debug`, which would carry Secret and ServiceAccount names into a status
/// field.
fn grant_mode(grant: &destination::ResolvedGrant) -> &'static str {
    match grant {
        destination::ResolvedGrant::SecretKeys { .. } => "SecretKeys",
        destination::ResolvedGrant::WorkloadIdentity { .. } => "WorkloadIdentity",
        destination::ResolvedGrant::ControllerIdentity => "ControllerIdentity",
        destination::ResolvedGrant::NotConfigured => "absent",
    }
}

/// "the last run exited 1 with AccessDenied on 2 point(s)", and the honest
/// shapes when there is less to say.
///
/// The codes are the closed per-point vocabulary, never a raw provider error
/// body. Duplicates are counted, not listed.
fn failure_detail(exit_code: Option<i32>, codes: &[String]) -> String {
    let exit = match exit_code {
        Some(code) => format!("the last run exited {code}"),
        None => {
            "the last run produced no exit code (its pod is gone or was never readable)".to_string()
        }
    };
    let mut counted: BTreeMap<&str, usize> = BTreeMap::new();
    for code in codes {
        *counted.entry(code.as_str()).or_default() += 1;
    }
    if counted.is_empty() {
        return format!("{exit} and named no per-point code");
    }
    let versioned = counted.contains_key("VersionedBucket");
    let unprobed = counted.contains_key("VersionProbeRefused");
    let named: Vec<String> = counted
        .into_iter()
        .map(|(code, n)| format!("{code} on {n} point(s)"))
        .collect();
    // The two refusals about the bucket, not the point, said with their
    // remedy: nothing about them clears on a retry, so the retry budget is
    // what stops the policy, and the condition must say why.
    let mut remedy = String::new();
    if versioned {
        remedy.push_str(
            ". VersionedBucket: the bucket is versioned (every S3 Object Lock bucket is), so \
             a delete by key would only write a delete marker over the data and never consult \
             a legal hold; the worker deletes nothing there. Enforce on an unversioned bucket, \
             or declare the provider's own lifecycle with mode: ExternalLifecycle",
        );
    }
    if unprobed {
        remedy.push_str(
            ". VersionProbeRefused: the retention credential could not HEAD the object, so \
             the worker could not establish that a delete would remove it; grant it \
             s3:GetObject on <prefix>/*",
        );
    }
    if only_bucket_level(codes) {
        remedy.push_str(
            ". This refusal is about the bucket or the credential, not the plan: once the \
             retry budget is spent, one re-probe run is started 24 h after the last run, and it \
             deletes nothing unless the refusal has cleared, so no spec edit is needed to resume",
        );
    }
    format!("{exit} with {}{remedy}", named.join(", "))
}

/// Whether every per-point code of a run is about the bucket or the
/// credential ([`BUCKET_LEVEL_CODES`]): the one condition under which a
/// degraded policy re-probes on its own.
fn only_bucket_level(codes: &[String]) -> bool {
    !codes.is_empty()
        && codes
            .iter()
            .all(|c| BUCKET_LEVEL_CODES.contains(&c.as_str()))
}

/// What a spent retry budget means for scheduling, in the words the re-probe
/// rule uses: the plain stop, or the stop with its one exception when every
/// code was bucket-level.
fn scheduling_stop(codes: &[String]) -> &'static str {
    if only_bucket_level(codes) {
        "Scheduling is stopped except for one re-probe run 24 h after the last run; a spec \
         edit resumes it at once"
    } else {
        "No further run is scheduled until spec changes"
    }
}

/// One view entry, as the evaluation sees it.
///
/// `segment_keys` is empty because the view's page entry carries `manifestKey`
/// and no segment list: the plan names the manifest and the set's key bound,
/// and the worker enumerates within it
/// (`retention_plan::PlanLine::enumerate_set`).
///
/// `refusals` is the namespace's `Backup`s' reached refusals: a row whose own
/// `Backup` the controller recorded `Invalid`/`Untrusted` is marked
/// [`PointFacts::refused_by_controller`], whatever the row says.
#[must_use]
pub fn point_facts(entry: &ViewEntry, refusals: &view::ControllerRefusals) -> PointFacts {
    PointFacts {
        point_id: entry.point_id.clone(),
        backup_id: entry.backup_id.clone(),
        recovery_point_at_ms: entry.recovery_point_at_ms,
        locations: entry
            .locations
            .iter()
            .map(|l| l.location_id.clone())
            .collect(),
        availability: entry.availability,
        verification: entry.verification,
        manifest_key: entry.manifest_key.clone(),
        segment_keys: Vec::new(),
        bytes: None,
        refused_by_controller: refusals.refusal_for(entry).is_some(),
    }
}

/// The page size of the `Backup` listing a retention evaluation joins against
/// ([`view::ControllerRefusals`]).
pub const BACKUP_PAGE_LIMIT: u32 = 500;

/// How many `Backup` pages one evaluation follows before it refuses to plan:
/// 10 000 objects, far above `protection_policy`'s 1 000, because here an
/// incomplete listing stops retention rather than narrowing a health verdict.
pub const MAX_BACKUP_PAGES: usize = 20;

/// The terminal `Restore` phases. Any other phase, including none (a `Restore`
/// the reconciler has not seen yet) and `Interrupted` (PROD-07.2), is
/// nonterminal and protects its point.
pub const TERMINAL_RESTORE_PHASES: &[&str] = &["Succeeded", "Failed", "Refused"];

/// Whether this `Restore` has finished, so it no longer holds its point. An
/// `Interrupted` restore holds it until its partial topics were cleaned up
/// (PROD-07.2), or until it is deleted.
#[must_use]
pub fn is_terminal_restore(restore: &Restore) -> bool {
    let phase = restore.status.as_ref().and_then(|s| s.phase.as_deref());
    if phase == Some(super::restore::restore_interruption::PHASE_INTERRUPTED) {
        return !super::restore::restore_interruption::holds_point(restore);
    }
    phase.is_some_and(|p| TERMINAL_RESTORE_PHASES.contains(&p))
}

/// Whether this `Restore` reads from the destination under evaluation.
///
/// By identity, never by substring. `location_id` is the destination's
/// canonical URL, which does not contain the `BackupDestination`'s name, and a
/// `Restore` with `sourceDestinationRef` carries the sentinel
/// `logweir-destination://<name>` as its archive URL; a substring match would
/// miss every restore that names its destination by reference, and leave the
/// reaper free to delete a set a live restore reads.
///
/// So when the restore names a destination, the two names are compared within
/// the same namespace (a `destinationRef` is namespace-local), as
/// `controllers/protection_policy.rs` does. URL equality remains only for a
/// legacy restore that names no destination.
///
/// `destination_name` is the policy's own `spec.destinationRef.name`.
#[must_use]
pub fn restore_touches(
    restore: &Restore,
    destination_name: &str,
    location_id: &str,
    namespace: &str,
) -> bool {
    if let Some(reference) = restore.spec.source_destination_ref.as_ref() {
        return restore.namespace().as_deref() == Some(namespace)
            && reference.name == destination_name;
    }
    let url = restore.spec.source_archive.url.trim_end_matches('/');
    url == location_id.trim_end_matches('/')
}

async fn installation_policy(client: &kube::Client) -> check::policy::Policy {
    use std::sync::OnceLock;
    static CACHE: OnceLock<check::policy::PolicyCache> = OnceLock::new();
    static REFERENCE: OnceLock<Option<(String, String)>> = OnceLock::new();
    let reference = REFERENCE.get_or_init(|| {
        check::policy::configured_ref(
            std::env::var(check::policy::POLICY_CONFIGMAP_ENV)
                .ok()
                .as_deref(),
            std::env::var(check::policy::INSTALLATION_NAMESPACE_ENV)
                .ok()
                .as_deref(),
        )
    });
    let cache = CACHE.get_or_init(check::policy::PolicyCache::new);
    match check::policy::load(client, reference.as_ref(), cache, Utc::now()).await {
        Ok(load) => load.policy().clone(),
        Err(e) => {
            warn!(error = %e, "the installation policy could not be read; failing closed");
            check::policy::Policy::fail_closed()
        }
    }
}

async fn reconcile(
    policy: Arc<RetentionPolicy>,
    ctx: Arc<Context>,
) -> Result<Action, ReconcileError> {
    let installation = installation_policy(&ctx.client).await;
    let outcome = reconcile_policy(
        &policy,
        &PolicyContext {
            client: &ctx.client,
            policy: &installation,
            runner_image: &ctx.runner_image,
            now: Utc::now(),
        },
    )
    .await?;
    Ok(policy_action(&outcome))
}

/// The [`Action`] a pass returns: a timed requeue ([`RUNNING_REQUEUE_SECONDS`]
/// while a run is started or running, [`IDLE_REQUEUE_SECONDS`] otherwise),
/// never `await_change()`: an enforcement slot comes due with no event on any
/// object, so this requeue is what starts the run
/// (`retention_policy_controller::an_enforcement_slot_starts_from_the_timed_requeue`).
#[must_use]
pub fn policy_action(outcome: &Outcome) -> Action {
    let seconds = match outcome.phase {
        RetentionPhase::Running | RetentionPhase::Started => RUNNING_REQUEUE_SECONDS,
        _ => IDLE_REQUEUE_SECONDS,
    };
    Action::requeue(std::time::Duration::from_secs(seconds))
}

/// The [`Action`] a failed pass returns: a timed requeue of
/// [`ERROR_REQUEUE_SECONDS`].
#[must_use]
pub fn policy_error_action() -> Action {
    Action::requeue(std::time::Duration::from_secs(ERROR_REQUEUE_SECONDS))
}

fn error_policy(policy: Arc<RetentionPolicy>, err: &ReconcileError, _ctx: Arc<Context>) -> Action {
    warn!(
        policy = %policy.name_any(),
        namespace = %policy.namespace().unwrap_or_default(),
        error = %err,
        "the RetentionPolicy reconcile could not complete; nothing was deleted"
    );
    policy_error_action()
}

/// Run the `RetentionPolicy` controller until the process ends.
///
/// It takes no archive handle: deletion happens inside the retention Job with
/// its own credential, and the evaluation reads the catalog's view out of
/// `ConfigMap`s.
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
    let api: Api<RetentionPolicy> = crate::scope::api(&client, namespace.as_deref());
    let jobs: Api<Job> = crate::scope::api(&client, namespace.as_deref());
    let ctx = Arc::new(Context {
        client,
        archive: None,
        runner_image,
    });
    async move {
        Controller::new(api, watcher::Config::default())
            .owns(jobs, watcher::Config::default())
            .run(reconcile, error_policy, ctx)
            .for_each(|_| std::future::ready(()))
            .await;
    }
}
