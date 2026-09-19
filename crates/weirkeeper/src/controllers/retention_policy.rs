//! The `RetentionPolicy` reconciler — what would be removed, and (only when an
//! administrator has said so twice) what is.
//!
//! # WHAT A RECONCILER IN THIS DIRECTORY MAY DO WHEN THE ANSWER IS
//! IRREVERSIBLE (D3 W9)
//!
//! This is the first reconciler whose output can destroy data, and every arm of
//! it is shaped by that. It still patches only its own `/status` and creates
//! only a Job of a different kind; it holds no delete verb, no `secrets` verb,
//! and — the load-bearing part — **it does not link the code that deletes.**
//! `logweir-reaper` is reachable from `logweir-retention` and from nothing else
//! (`scripts/check-no-archive-write.sh` check 3), so the authority this
//! controller has is exactly "create a Job from an approved plan", which is a
//! thing an operator can see in `kubectl get jobs`.
//!
//! # The four gates between a rule and a deleted object
//!
//! 1. `spec.mode` must be `Enforce`. `Report` (the default) evaluates and
//!    publishes and creates nothing.
//! 2. `spec.enforcement.approvedPlanSha256` must equal
//!    `status.lastEvaluation.planSha256`, and the plan must be younger than
//!    `planMaxAgeSeconds` — both halves, and the second one is enforced here
//!    rather than only declared (review `d3w9` H4). **The digest is a function
//!    of what would be deleted and of nothing that moves on its own**: no
//!    instant, no generation, no counter (review `d3w9` C1). A rules edit, a
//!    `holds` edit or a new backup changes the CONTENT, and that is what
//!    invalidates an approval.
//! 3. A lease is written with a resourceVersion-preconditioned patch **and it
//!    must land** — a 409 aborts the pass before any Job exists (review `d3w9`
//!    C3) — and THEN a consistent, non-cached, cluster-wide list of `Restore`s
//!    is made. A restore that arrived after the lease cannot slip past the
//!    list, because the list happens second; and because the restore-side
//!    admission hold is still a hand-off to whoever owns `controllers/restore.rs`,
//!    this list **fails closed on the whole destination**: any nonterminal
//!    restore reading it stops the run.
//! 4. Each point's **intent tombstone** is written, create-only under
//!    `logweir/`, before that point's first delete, and a point whose intent
//!    cannot be written is not deleted (review `d3w9` L7). The run's own record
//!    carries the outcome and so is necessarily written after it. The worker
//!    also re-validates every key and refuses the whole plan on the first one
//!    outside `<scope>/<backupId>/`.
//!
//! # This controller does not function live until W13 lands (review `d3w9` L5)
//!
//! Every Job it builds requests the [`SERVICE_ACCOUNT`] ServiceAccount, and
//! nothing on this branch creates it — W13 owns it together with the
//! `retention.enabled` chart value. An `Enforce` policy here produces a Job
//! whose pods the API server will not admit, which is correctly fail-closed and
//! is a **merge-ordering constraint**, not a runtime surprise to discover at
//! the live acceptance.
//!
//! # And the evaluation names the right destination
//!
//! Its input is the `RecoveryCatalog`'s bounded VIEW of this destination
//! (`crate::catalog_view`), read out of the page `ConfigMap`s the catalog
//! published — never a bucket walk with the controller's one global handle, and
//! never the schedule's current archive URL. That is where RET-WRONGBUCKET
//! closes for destination-backed retention; the legacy `BackupSchedule` report
//! gets the honesty note in `backup_schedule.rs` instead (D3 §6.3).

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
use crate::conditions::{merge_condition, status_unchanged};
use crate::crds::recovery_catalog::RecoveryCatalog;
use crate::crds::restore::Restore;
use crate::crds::retention_policy::{RetentionMode, RetentionPolicy};
use crate::crds::{Condition, Time};
use crate::destination::{self, ResolveError, ResolvedDestination};
use crate::job::RunnerOwner;
use crate::retention_plan::{self as plan, PointFacts};

use super::Context;

// ---------------------------------------------------------------------------
// Conditions — D3 §6.2's five names, and their closed reasons
// ---------------------------------------------------------------------------

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
/// `Ready=False`: `ExternalLifecycle` combined with guarantees no provider rule
/// can make.
pub const REASON_UNSUPPORTED_COMBINATION: &str = "UnsupportedCombination";

/// `Evaluated=True`.
pub const REASON_EVALUATION_COMPLETE: &str = "EvaluationComplete";
/// `Evaluated=False`: the catalog view is not readable right now.
pub const REASON_VIEW_UNREADABLE: &str = "ViewUnreadable";
/// `Evaluated=Unknown`: nothing has been evaluated yet.
pub const REASON_NEVER_EVALUATED: &str = "NeverEvaluated";
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
/// A refusal and not a fall-back to "every reconcile" (review `d3w9` M5): a
/// retention cadence this build cannot read is a cadence nobody configured.
pub const REASON_UNSUPPORTED_SCHEDULE: &str = "UnsupportedSchedule";
/// `Enforced=False`: the lease PATCH did not land, so nothing holds these
/// points and no Job may start (review `d3w9` C3).
pub const REASON_LEASE_NOT_HELD: &str = "LeaseNotHeld";
/// `Enforced=False`: an object already holds the plan `ConfigMap`'s name and is
/// not this plan (review `d3w9` M6).
pub const REASON_PLAN_CONFIG_MAP_CONFLICT: &str = "PlanConfigMapConflict";
/// `Enforced=True`: a run is in flight.
pub const REASON_RUN_IN_PROGRESS: &str = "RunInProgress";
/// `Enforced=True`: the last run completed.
pub const REASON_RUN_COMPLETE: &str = "RunComplete";
/// `Enforced=False`: the last run did not complete.
pub const REASON_RUN_FAILED: &str = "RunFailed";
/// `Enforced=True` with `requireApprovedPlan: false` — D3 §6.5 requires the
/// choice to be visible ON the object.
pub const REASON_UNATTENDED: &str = "UnattendedDeletionEnabled";
/// `Enforced=False`: there is nothing to delete.
pub const REASON_NOTHING_TO_DO: &str = "NothingToDo";
/// `Enforced=False`: a Job of this run's name exists and is not ours.
pub const REASON_JOB_NAME_CONFLICT: &str = "JobNameConflict";

/// `ExternalLifecycleConflict=True`: the declared expiry would expire kept
/// points.
pub const REASON_DECLARED_EXPIRY_CONFLICTS: &str = "DeclaredExpiryConflicts";
/// `ExternalLifecycleConflict=False`.
pub const REASON_NO_CONFLICT: &str = "NoConflict";

/// `EnforcementDegraded=True`.
pub const REASON_CONSECUTIVE_FAILURES: &str = "ConsecutiveRunFailures";
/// `EnforcementDegraded=False`.
pub const REASON_HEALTHY: &str = "Healthy";

/// Every reason this reconciler writes — the closed set a test asserts over.
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
    REASON_RUN_IN_PROGRESS,
    REASON_RUN_COMPLETE,
    REASON_RUN_FAILED,
    REASON_UNATTENDED,
    REASON_NOTHING_TO_DO,
    REASON_JOB_NAME_CONFLICT,
    REASON_DECLARED_EXPIRY_CONFLICTS,
    REASON_NO_CONFLICT,
    REASON_CONSECUTIVE_FAILURES,
    REASON_HEALTHY,
];

// ---------------------------------------------------------------------------
// `status.enforcement` and `status.guarantees`
// ---------------------------------------------------------------------------

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

/// How many consecutive failed runs stop the scheduling — D3 §6.5.
pub const DEGRADED_AFTER_FAILURES: i64 = 3;

/// How long a lease lives beyond the Job's deadline, so a lease never outlives
/// the run it protects by much and never dies before it.
pub const LEASE_MARGIN_SECONDS: i64 = 300;

/// The label an operator finds a retention Job by.
pub const LABEL_POLICY_UID: &str = "logweir.dev/retention-policy-uid";
/// The component label.
pub const COMPONENT_RETENTION: &str = "retention";

/// The Job and `ConfigMap` name prefix.
pub const NAME_PREFIX: &str = "lwr-";

/// The retention Job's ServiceAccount. Its own, never the runner's: the
/// retention pod is the one pod in the installation that mounts a delete-capable
/// credential.
pub const SERVICE_ACCOUNT: &str = "logweir-retention";

/// The binary the Job runs — NOT `logweir` (D-SEAMS S1's third exception).
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
}

/// What the write guard says when a status PATCH cannot be preconditioned.
///
/// **Named, not inlined, because it is a contract two places depend on**: the
/// operator reading the controller log, and
/// `tests/retention_policy_controller.rs`, which pins it so that a write
/// silently losing its precondition cannot stop being reported. A /status
/// compare-and-set needs `metadata.resourceVersion` (D-SEAMS S7); the cursor
/// is `None` only before the first patch of a pass on an object that carries
/// none, or after a 409 cleared it, and in both cases a blind write would
/// overwrite a status this pass never read.
pub const NO_RESOURCE_VERSION: &str =
    "RetentionPolicy carries no metadata.resourceVersion, which a /status compare-and-set \
     needs (D-SEAMS S7); no patch is sent";

/// The annotation an administrator's `kubectl patch` records itself in, so the
/// enforcement record can name who approved (D3 §6.5).
pub const APPROVER_ANNOTATION: &str = "logweir.dev/retention-plan-approver";

// ---------------------------------------------------------------------------
// Errors and outcomes
// ---------------------------------------------------------------------------

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

/// What one pass did — the value every controller-double test asserts over.
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
    /// **How many object-store deletes this controller performed. Always
    /// zero.** It is on the outcome so a test can assert it rather than assume
    /// it, and it is a constant because this crate cannot delete: see
    /// `scripts/check-no-archive-write.sh`.
    pub deletes_performed: u32,
}

/// A digest the catalog published, reduced to the bare lowercase hex the
/// recomputation produces.
///
/// **THE TWO SPELLINGS ARE THE SAME DIGEST AND THIS IS THE ONLY PLACE THAT
/// KNOWS IT.** `RecoveryCatalog.status.pages[].sha256` is documented in the
/// CRD (`config/crd/recoverycatalogs.yaml`: "`sha256:<lowercase hex>` over its
/// entry lines") and written that way by `catalog_view::seal`, while
/// `catalog_view::page_digest` — the function this controller recomputes the
/// page with, and the one the runner's own `catalog-page=… sha256=<hex>`
/// header is checked against — returns BARE hex. Comparing the two literally
/// is never equal, so every `RetentionPolicy` in a real cluster answered
/// `Evaluated=False/ViewUnreadable` and no retention report was rendered for
/// any destination (defect RET-DIGEST-PREFIX, reproduced live 2026-09-18; the
/// controller's own message printed both values, equal apart from the prefix).
///
/// NORMALISING HERE AND NOT AT THE WRITER is deliberate: `page_digest` has a
/// second caller (`catalog_view.rs`'s check of the runner's header, documented
/// bare) that is already consistent, so prefixing the function would move the
/// defect rather than remove it. The reader is the side that has to accept
/// what the contract says is published.
///
/// BOTH SPELLINGS ARE ACCEPTED, DEFENSIVELY AND NOT FOR COMPATIBILITY — and
/// the difference is worth stating, because the weaker claim is the one that
/// gets quoted. **No released build has ever published the bare spelling**:
/// `catalog_view::seal` has written `sha256_prefixed` since the catalog
/// controller first landed (review finding L3). Accepting the bare form costs
/// nothing — the hex must still match the page's bytes — and means a writer
/// that ever emitted it would be read rather than refused. What follows from
/// this function is only that upgrade and rollback need no catalog resync,
/// which is true because the accepted set only grew.
///
/// The prefix is the ONLY thing stripped. A value that is neither spelling — a
/// truncated digest, an upper-case one, a different algorithm — is returned
/// unchanged and fails the full-length comparison at the call site, which is
/// the behaviour that matters.
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

// ---------------------------------------------------------------------------
// The context
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// The pass
// ---------------------------------------------------------------------------

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
    /// The `metadata.resourceVersion` the NEXT status PATCH will use as its
    /// precondition — review `d3w9` **C3**.
    ///
    /// # Why this is threaded rather than read from the object each time
    ///
    /// The first landing took the version from `self.policy`, the object the
    /// watcher delivered, on every patch. An enforcing pass patches three
    /// times: the evaluation, then the lease, then `lastEnforcement`. The first
    /// succeeds and advances the object's version; the second and third then
    /// carry a version the API server has already superseded, 409, and — worse
    /// — the 409 was mapped to `Ok(())`. The lease was therefore **never
    /// written**, the Job was created anyway, the run was never tracked or
    /// harvested, `consecutiveRunFailures` never moved, and sixty seconds later
    /// the next pass computed a new run id and created a **second deletion
    /// Job**.
    ///
    /// This reconciler holds no `get` on its own kind, so it cannot re-read the
    /// object; the version each successful PATCH returns is the only fresh one
    /// available, and it is what the next patch uses.
    ///
    /// A `Mutex` and not a `RefCell`: the arms take `&self`, `kube`'s
    /// `Controller` requires the reconcile future to be `Send`, and `RefCell`
    /// is not `Sync`. It is uncontended by construction — one pass, one thread
    /// — and **no guard is ever held across an `await`**, which is the other
    /// way a lock makes a future non-`Send`.
    resource_version: std::sync::Mutex<Option<String>>,
    /// The status as this pass believes it now stands, for the
    /// "nothing changed, send nothing" comparison.
    ///
    /// Threaded for the same reason as the version: after the evaluation patch
    /// the object in hand is stale, and comparing the lease against the stale
    /// copy would send a patch that is already there — or skip one that is not.
    observed_status: std::sync::Mutex<Option<Value>>,
}

/// What one status PATCH did.
///
/// A three-way answer and not a `Result<(), _>`, because "nothing needed
/// writing" and "somebody else wrote first" are different facts and exactly one
/// of them may be swallowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatchOutcome {
    /// The API server accepted it and the version cursor moved.
    Applied,
    /// The computed status equalled the stored one; nothing was sent.
    Unchanged,
    /// The precondition failed. **The caller decides.** For an evaluation this
    /// is "the next pass reads it"; for the LEASE it is "abort before creating
    /// a Job", because a run whose lease did not land is a run nothing is
    /// holding points for.
    Conflict,
}

impl Pass<'_> {
    fn generation(&self) -> i64 {
        self.policy.metadata.generation.unwrap_or(0)
    }

    /// The spec has changed since the controller last acted on it.
    ///
    /// `metadata.generation != status.observedGeneration` is the ONLY thing on
    /// the object that says so, and D3 §6.5 makes it the one release for a
    /// degraded policy: not time, not a restart, not the count ageing out. A
    /// policy that has never been reconciled has no `observedGeneration`, and
    /// "never acted on" counts as changed — it has no failure history either.
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
    /// **A SPEC CHANGE ZEROES IT, AND THAT IS WHY THIS IS A FUNCTION AND NOT A
    /// FIELD READ.** D3 §6.5 says a spec change releases the stop; a release
    /// that left the counter at its ceiling would give the operator exactly one
    /// run before the next failure re-degraded the policy, which is not a
    /// retry budget. Live at `d387f87` that is what happened:
    /// `consecutiveRunFailures` stayed at 3 across the generation bump that
    /// correctly resumed scheduling.
    ///
    /// Every reader of the count goes through here — the enforcement decision,
    /// the harvest that adds to it, and the evaluation that publishes it — so
    /// the release rule is stated once.
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

    /// The last run's outcome IN WORDS, for the `EnforcementDegraded` message.
    ///
    /// A condition that says only "3 consecutive runs have failed" tells an
    /// operator that something is wrong and nothing about what; D3 §6.5's
    /// clause is "it says why in words". The exit code and the closed per-point
    /// codes are what the object already knows, so they are what it says.
    fn last_failure_detail(&self) -> String {
        let record = self
            .policy
            .status
            .as_ref()
            .and_then(|s| s.last_enforcement.as_ref());
        let codes: Vec<String> = record
            .and_then(|r| r.failed.as_ref())
            .map(|f| f.iter().map(|d| d.code.clone()).collect())
            .unwrap_or_default();
        failure_detail(record.and_then(|r| r.exit_code), &codes)
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
    /// Review `d3w9` **M5** and **Q1**. The first landing read the cron field
    /// nowhere and used `now / 60` as the run id's slot, so an operator who
    /// wrote `"17 4 * * *"` expecting one nightly run had nothing bounding how
    /// often an approved plan could start one, and the deterministic-name
    /// protection against a duplicate reconcile held for exactly one minute.
    ///
    /// The slot is now the cron's own latest due firing, and it is BOTH the
    /// cadence gate and the run id's slot — which is what makes "one run per
    /// slot" a property of the name rather than of the clock.
    ///
    /// `Err` is an unparseable expression, which is a refusal and not a
    /// silently-hourly cadence.
    fn enforcement_slot(&self) -> Result<Option<DateTime<Utc>>, String> {
        let Some(enforcement) = self.policy.spec.enforcement.as_ref() else {
            return Ok(None);
        };
        // UTC and no time zone: D3 §6.2 says "UTC cron", and a retention
        // cadence that moved with a zone's DST would run twice or not at all on
        // two nights a year.
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
        // 1. CONFLICT FIRST. Two policies for one destination put BOTH in
        //    `Ready=False/Conflict` and NEITHER evaluates or enforces (D3 §6.3).
        //    It is first because a conflicted policy must not so much as
        //    compute a plan: a plan an administrator could approve is the thing
        //    that makes two contesting policies dangerous.
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
    /// LISTED IN THIS NAMESPACE ONLY, and compared on `destinationRef.name`,
    /// which is namespace-local by construction. The tie-break is deterministic
    /// — the pair is symmetric, so both objects see the conflict and both go
    /// `Ready=False`, which is what D3 §6.3 asks for ("put both in
    /// `Ready=False/Conflict`").
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

    // -----------------------------------------------------------------------
    // `ExternalLifecycle`
    // -----------------------------------------------------------------------

    /// D3 §6.6: record the declaration, mark what is and is not enforced, and
    /// raise `ExternalLifecycleConflict` when the declared expiry would expire
    /// points the rules consider kept.
    ///
    /// **Logweir reads no lifecycle configuration.** `object_store` 0.14
    /// exposes no such API, so `ageExpiry` and `legalHold` are
    /// `ProviderEnforcedUnverified` — not `LogweirEnforced` and not a claim
    /// that the rule is in force — and the three guarantees a bucket rule
    /// cannot express are `NotEnforced` outright.
    async fn declare_external(&self) -> Result<Outcome, ReconcileError> {
        let declared = self.policy.spec.external_lifecycle.as_ref();
        let expiration_days = declared.map(|d| i64::from(d.expiration_days));
        let rules = self.rules();
        // THE CONFLICT: the bucket would expire what the rules keep. Compared
        // on `keepDays` because that is the only rule a lifecycle rule can be
        // compared against at all — `keepLast` and `minUsablePoints` are
        // COUNTS, and a lifecycle rule cannot count.
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

    // -----------------------------------------------------------------------
    // The tracked run
    // -----------------------------------------------------------------------

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
        // D-SEAMS S6, applied to a Job: a Job wearing this run's name is not
        // thereby this policy's Job.
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
            return Ok(Some(self.report_running(&run_id, &job_name).await?));
        }
        let report = self.harvest_run(&job).await?;
        Ok(Some(self.harvest(&run_id, Some(&job), &report).await?))
    }

    /// The exit code AND the run's own key lines, through the pod's OWNER UID
    /// and never through a label (D-SEAMS **S6**, defect SEC-PODLOG).
    ///
    /// Review `d3w9` **H2**. The first landing read the exit code and stopped,
    /// so `status.lastEnforcement.{deleted, failed, objectsDeleted, recordKey,
    /// recordSha256}` — all five declared in the CRD — were never written. The
    /// consequence was not cosmetic: `previously_refused()` reads `failed[]`,
    /// so D3 §6.5's "a provider refusal is recorded, kept, and **excluded from
    /// the next plan** until the reason clears" never fired, and every run
    /// re-attempted the locked point.
    async fn harvest_run(&self, job: &Job) -> Result<RunReport, ReconcileError> {
        let Some(pod) = check::pod::find_owned_pod(self.ctx.client, &self.namespace, job).await?
        else {
            return Ok(RunReport::default());
        };
        let exit_code = super::backup::terminated_exit_code(&pod);
        let pods: Api<k8s_openapi::api::core::v1::Pod> =
            Api::namespaced(self.ctx.client.clone(), &self.namespace);
        let log = match pods
            .logs(&pod.name_any(), &check::relay::log_params())
            .await
        {
            Ok(log) => log,
            // A pod whose log is gone is a run whose durable answer is the
            // record in object storage; it is not a reconcile failure.
            Err(e) if check::is_log_absent(&e) => String::new(),
            Err(e) => return Err(ReconcileError::Api(e)),
        };
        Ok(parse_run_lines(&log, exit_code))
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

    /// Publish what a finished run did, clear the lease, and count the failure
    /// if it was one.
    ///
    /// **The status is written BEFORE the Job's TTL is patched** (S7's ordering,
    /// the same rule `controllers/backup.rs` follows): pod garbage collection
    /// must never race the exit-code read.
    async fn harvest(
        &self,
        run_id: &str,
        job: Option<&Job>,
        report: &RunReport,
    ) -> Result<Outcome, ReconcileError> {
        let exit_code = report.exit_code;
        let failed = exit_code != Some(0);
        // THROUGH `budget_before`, so a spec change zeroes the count here too.
        // A harvest also writes `observedGeneration`, i.e. it adopts the new
        // generation; adopting it without releasing the budget would leave the
        // release depending on which arm happened to run first.
        let previous = self.budget_before();
        // A RUN THAT STOPPED ON ITS OWN CEILING IS NOT A FAILED RUN (review
        // `d3w9` M2). It exits 1 because work remains, and counting it would
        // make three ordinary bounded runs on a large archive set
        // `EnforcementDegraded` and stop retention for good.
        let bounded = report.bounded_only();
        let failures = if failed && !bounded {
            previous + 1
        } else if failed {
            previous
        } else {
            0
        };
        let degraded = failures >= DEGRADED_AFTER_FAILURES;
        let reason = if failed {
            REASON_RUN_FAILED
        } else {
            REASON_RUN_COMPLETE
        };
        let message = match exit_code {
            Some(0) => format!("retention run {run_id} completed"),
            Some(3) => format!(
                "retention run {run_id} REFUSED its plan before deleting anything (exit 3); the \
                 archive is untouched"
            ),
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
        let detail = failure_detail(
            exit_code,
            &report
                .failed
                .iter()
                .map(|(_, code)| code.clone())
                .collect::<Vec<String>>(),
        );
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
                         is spent: {detail}. No further run is scheduled until spec changes \
                         (edit spec.enforcement, or spec.rules, and the count clears with it). \
                         status.lastEnforcement.recordKey names the durable record of the last \
                         run."
                    )
                } else {
                    format!("{failures} consecutive run failures; {detail}")
                },
            ),
        ]);
        let mut harvest_status = json!({
                "enforcement": ENFORCEMENT_LOGWEIR_WORKER,
                // THE FIVE FIELDS THE CRD DECLARES AND THE FIRST LANDING NEVER
                // WROTE (review `d3w9` H2). `failed[]` is the input
                // `previously_refused()` reads, so without it D3 §6.5's "a provider
                // refusal is recorded, kept and excluded from the next plan" could
                // not fire at all.
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
                },
                // RFC 7386: `null` DELETES the key. The lease exists only while a
                // run holds it, and a lease left behind would hold restore
                // admission for nothing.
                "lease": Value::Null,
                "consecutiveRunFailures": failures,
                "conditions": conditions,
        });
        // Through the helper like every other writer. It adopts the generation
        // and adds nothing here: this patch already carries the count (already
        // released by `budget_before()`) and its own `EnforcementDegraded`.
        self.adopt_generation(&mut harvest_status);
        let outcome = self.patch_status(harvest_status).await?;

        // THE TTL AFTER THE STATUS, and only then (S7). The Job's pod carries
        // the exit code this pass just published.
        //
        // AND ONLY IF THE STATUS ACTUALLY LANDED. `patch_status` answers
        // `Conflict` both for a 409 and for a pass whose precondition cursor is
        // gone ([`NO_RESOURCE_VERSION`]) — in either case this run's exit code
        // was NOT published. Setting the Job's TTL then is the one irreversible
        // half of the pair: the Job and its pod are collected, the exit code
        // becomes unreadable, and the next pass harvests a run it can only
        // record as "produced no exit code". S7's ordering rule exists to stop
        // garbage collection racing the exit-code read; a write that did not
        // land has not won that race, it has lost it. So the TTL waits, the run
        // stays untracked, and the next pass reads it again.
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
        let patch = json!({"spec": {"ttlSecondsAfterFinished": check::job::TTL_SECONDS}});
        match jobs
            .patch(
                &job.name_any(),
                &PatchParams::default(),
                &Patch::Merge(patch),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(e)) if e.code == 404 || e.code == 422 => Ok(()),
            Err(e) => Err(ReconcileError::Api(e)),
        }
    }

    // -----------------------------------------------------------------------
    // The evaluation
    // -----------------------------------------------------------------------

    async fn evaluate(&self) -> Result<Outcome, ReconcileError> {
        // The destination, for its location id and (in `Enforce`) its Job
        // environment. `ArchiveRead`: this controller reads nothing from the
        // archive at all, and the role is what the LOCATION is resolved under.
        let resolved = match destination::resolve_ref(
            self.ctx.client,
            &self.namespace,
            &self.policy.spec.destination_ref.name,
            DestinationRole::ArchiveRead,
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

        // THE SCOPE MUST BE THE DESTINATION'S OWN PREFIX (D3 §6.2). A policy
        // whose scope names a prefix the destination does not root would give a
        // plan a bound the destination's credential could still reach.
        let dest_prefix = resolved.location.prefix.trim_end_matches('/');
        let scope = self.policy.spec.scope.prefix.trim_end_matches('/');
        // ON A SEGMENT BOUNDARY (review `d3w9` M3). A bare `starts_with` lets
        // `…/team-ab` — a sibling tenant's prefix — pass a guard whose stated
        // purpose is that a scope may only ever NARROW the destination it
        // covers. The destination's own prefix may legitimately be empty, in
        // which case every scope is under it.
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
        let entries = match self.view_entries().await? {
            Ok(entries) => entries,
            Err(message) => return self.publish_view_failure(&message).await,
        };

        let location_id = resolved.canonical_url.clone();
        let dest = plan::Destination {
            location_id: location_id.clone(),
            scope_prefix: self.policy.spec.scope.prefix.clone(),
        };
        let points: Vec<PointFacts> = entries.iter().map(point_facts).collect();
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

        // ENFORCE, OR NOT.
        let decision = self.enforcement_decision(&plan_sha256, &evaluation, &window);
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

        // THE REAL OBJECT COUNT IS NOT KNOWN HERE (review `d3w9` M1). The
        // catalog view carries no segment keys, so every candidate's `objects`
        // is `None` — "not observed" — rather than the misleading `1` the first
        // landing published, and the `Evaluated` condition says so.
        self.publish_evaluation(&evaluation, &plan_sha256, &window, &decision, &points)
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
                StartOutcome::PlanConfigMapConflict(message) => {
                    outcome.enforced_reason = REASON_PLAN_CONFIG_MAP_CONFLICT;
                    self.publish_enforcement_refusal(REASON_PLAN_CONFIG_MAP_CONFLICT, &message)
                        .await?;
                }
            }
        }
        Ok(outcome)
    }

    /// When this plan's approval window opened and when it closes — review
    /// `d3w9` **H4**, and the other half of **C1**.
    ///
    /// # The anchor is the DIGEST, not the pass
    ///
    /// `planExpiresAt` recomputed from `now` on every pass would never expire;
    /// anchored to an instant inside the digested bytes it would change the
    /// digest, which is C1. So it is anchored to the first pass that SAW this
    /// digest: while `status.lastEvaluation.planSha256` still equals what this
    /// pass computed, the stored `planExpiresAt` stands.
    ///
    /// # And a lapsed window re-anchors, once, refusing that pass
    ///
    /// A digest that never changes is an archive that has not moved, so the
    /// plan still describes exactly what it described — but D3 §6.5 makes the
    /// age a gate in its own terms ("the plan is younger than
    /// `planMaxAgeSeconds`"), and an approval left lying for a week must not
    /// silently authorise today's run. The pass that observes the lapse
    /// therefore **refuses** with [`REASON_PLAN_EXPIRED`] and re-anchors the
    /// window, so the administrator's next look sees a fresh preview of the
    /// same plan rather than a policy wedged forever.
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

    /// Read the catalog's published pages.
    ///
    /// `Ok(Err(message))` is "the view is not readable right now", which is an
    /// `Evaluated=False` status and NOT a reconcile error: a retention
    /// evaluation failure must never block anything, least of all a backup.
    #[allow(clippy::type_complexity)]
    async fn view_entries(&self) -> Result<Result<Vec<ViewEntry>, String>, ReconcileError> {
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
        // THE CATALOG MUST COVER THE SAME DESTINATION. A policy pointed at
        // destination A whose catalog syncs destination B would evaluate B's
        // points and name A's bucket — the exact shape of RET-WRONGBUCKET, one
        // level up. The pure evaluation would still refuse every point (their
        // `locations[]` would not match), but saying so here gives the operator
        // the field name.
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
            // THE PAGE DIGEST, CHECKED, AND ITS ABSENCE IS A FAILURE (review
            // `d3w9` M4). `status.pages[].sha256` is over exactly these bytes,
            // so a page that changed under the reader is caught here rather
            // than turned into a plan — and a page the catalog published no
            // digest for is a page nothing can check. The same function already
            // refuses an INCOMPLETE view; accepting an UNVERIFIED one would be
            // the same defect through the other door.
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
        Ok(Ok(entries))
    }

    /// The controller-supplied protection set: every nonterminal `Restore` that
    /// names a set at this destination.
    ///
    /// **Matched on the archive location AND the backup set**, never on the set
    /// alone: two destinations legitimately hold sets of the same `backupId`,
    /// and protecting one because the other is being restored would be the
    /// wrong-destination defect wearing a helpful face.
    async fn active_restore_sets(
        &self,
        location_id: &str,
    ) -> Result<BTreeSet<String>, ReconcileError> {
        let restores: Api<Restore> = Api::all(self.ctx.client.clone());
        // A CONSISTENT LIST: `ListParams` with no `resource_version` is a
        // quorum read from etcd, not a read of the API server's watch cache.
        // D3 §6.5 requires the re-listing before a Job is created to be
        // non-cached, and this is that read.
        let list = restores.list(&ListParams::default()).await?;
        let mut active_sets: BTreeSet<String> = BTreeSet::new();
        for restore in &list.items {
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
    /// A `Restore` names a `backupSetRef`, and the evaluation keys on POINT
    /// ids, so the mapping happens here over the view's own entries: every
    /// point whose set a nonterminal restore names is protected. Two points in
    /// one set are both protected, which is correct — a restore reads the set.
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

    /// Points a previous run was refused on, kept until the reason clears —
    /// D3 §6.5's bounded retry.
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

    // -----------------------------------------------------------------------
    // Enforcement
    // -----------------------------------------------------------------------

    async fn start_run(
        &self,
        resolved: &ResolvedDestination,
        plan_bytes: &[u8],
        plan_sha256: &str,
        candidates: &[plan::Candidate],
        slot: DateTime<Utc>,
    ) -> Result<StartOutcome, ReconcileError> {
        let leased: Vec<String> = candidates.iter().map(|c| c.point_id.clone()).collect();
        // THE SLOT IS THE CRON'S, NOT THE MINUTE'S (review `d3w9` M5/Q1). The
        // run id is a pure function of it, so "one run per slot" is a property
        // of the NAME — a duplicate reconcile inside a slot gets 409
        // `AlreadyExists` from the API server — rather than of the clock.
        let run_id = plan::run_id(&self.uid, plan_sha256, slot.timestamp());

        // A RUN THIS POLICY HAS ALREADY HARVESTED IS NOT STARTED AGAIN —
        // defect RET-COUNT-EARLY.
        //
        // The run id is a pure function of the policy, the plan digest and the
        // slot, so every pass inside one slot that computes the same plan
        // recomputes the SAME id. `jobs.create` below then answers 409
        // `AlreadyExists` — deliberately, that is what makes a duplicate
        // reconcile idempotent — and the pass falls through to record
        // `lastEnforcement` as if a new run had begun, clearing `finishedAt`
        // with the explicit nulls a genuinely new run needs. The object then
        // claims the finished run is in flight, `tracked_run` harvests the SAME
        // Job a second time, and `consecutiveRunFailures` gains a failure no
        // Job produced. Every further reconcile in the slot adds another: live
        // on `af64073`, TWO enforcement Jobs and a count of 3, with
        // `EnforcementDegraded` firing a full run early.
        //
        // WHY HERE AND NOT IN `enforcement_decision`. The "one run per slot"
        // check there guards the approved-plan branch only; the unattended
        // branch (`requireApprovedPlan: false`) returns `start: true` before the
        // slot is computed at all, which is the configuration the live defect
        // ran under. This is the one place both branches pass through, and the
        // first place the id exists.
        //
        // FINISHED IS THE CONDITION, NOT MERELY RECORDED. A record with no
        // `finishedAt` is a run still in flight, and `tracked_run` — not this
        // arm — is what looks after it.
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

        // (a) THE LEASE, with a resourceVersion-preconditioned patch, AND IT
        //     MUST LAND (review `d3w9` C3). The first landing swallowed the
        //     409 this patch got every enforcing pass — the evaluation patch
        //     had already advanced the object's version — so the Job was
        //     created with no lease recorded, the run was never tracked or
        //     harvested, and sixty seconds later a second deletion Job
        //     followed. A run nothing is holding points for is a run that must
        //     not start.
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

        // (b) AND THEN the consistent re-list. The ORDER is the whole property:
        //     a restore that arrives after (a) is seen by (b).
        //
        //     **FAIL CLOSED ON THE WHOLE DESTINATION**, not only on the leased
        //     points. The restore-side admission hold D3 §6.5 calls for lives
        //     in `controllers/restore.rs`, which is another worker's file and a
        //     recorded hand-off; until it lands, the only guard that exists is
        //     this one, and a `Restore` reading ANY set at this destination is
        //     enough to stop the run. A run refused for a restore it would not
        //     have touched costs one cadence slot; the other way costs the set.
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
        let job = self.build_job(&job_name, &run_id, &plan_name, plan_sha256, resolved);
        let jobs: Api<Job> = Api::namespaced(self.ctx.client.clone(), &self.namespace);
        match jobs.create(&PostParams::default(), &job).await {
            Ok(_) => {
                info!(
                    policy = %self.name, namespace = %self.namespace,
                    job = %job_name, run = %run_id,
                    "created the retention Job for an approved plan"
                );
            }
            // A DETERMINISTIC NAME MAKES A DUPLICATE RECONCILE A 409, not a
            // second deletion run. That is the whole reason the run id is a
            // pure function of the policy, the plan digest and the minute.
            //
            // AND IT RETURNS HERE RATHER THAN FALLING THROUGH TO THE RECORD —
            // defect RET-COUNT-EARLY's second and decisive arm. This arm used
            // to log and continue, so the patch below wrote `runId`/`startedAt`
            // and nulled all seven terminal fields for a Job that ALREADY
            // EXISTS. When that Job's run had already been harvested, the nulls
            // resurrected it: `tracked_run` read `finishedAt: None`, found the
            // Job present and finished, re-read its pod and counted the same
            // failure again. Nothing bounded the repetition.
            //
            // It is reachable even when the guard at the top of this function
            // is not, because that guard compares against the run the object
            // LAST recorded and the digest can return to an EARLIER run's.
            // `previously_refused()` reads the last run's `failed[]`, so the
            // exclusion set oscillates — run 1's plan, minus run 1's refusals
            // gives run 2's, minus run 2's gives run 1's again — and inside one
            // `* * * * *` slot that is the same run id. Live on `af64073`:
            // `status.lastEnforcement` named the FIRST Job with a `startedAt`
            // 17.5 s after that Job was created and a `finishedAt` 100 ms
            // later, carrying its real `exitCode 1` and its own `recordKey`.
            //
            // WHAT IS GIVEN UP, STATED. The fall-through also recovered a run
            // whose Job was created by a pass that died before recording it.
            // That recovery is now not performed: the Job runs, its own
            // create-only record under `logweir/retention/` is written by the
            // worker and is the durable evidence, and the controller
            // under-reports that run. Under-reporting a run is fail-safe —
            // retention stops sooner, never later — and re-recording it is the
            // defect above. The lease this pass wrote still holds the points.
            Err(kube::Error::Api(e)) if e.code == 409 => {
                debug!(
                    job = %job_name, run = %run_id,
                    "a Job already stands at this run's deterministic name; this pass creates \
                     none and leaves the run record alone"
                );
                return Ok(StartOutcome::AlreadyHarvested);
            }
            Err(e) => return Err(ReconcileError::Api(e)),
        }
        // THE RUN RECORD. `runId`, `startedAt` and `planSha256` are this run's
        // own; the seven terminal fields below describe a run that has FINISHED
        // and are deleted, by explicit RFC 7386 `null`, so the previous run's
        // outcome cannot be read as this one's (defect
        // RET-DEGRADED-UNREACHABLE). `previously_refused()` reads `failed[]` to
        // exclude a point from the next plan, so a stale one is not cosmetic.
        //
        // **THE RUN REACHING HERE IS ALWAYS A DIFFERENT ONE**, and that is why
        // the clearing is unconditional. `start_run` is reached only after
        // `tracked_run` returned "nothing in flight", which it does only when
        // no run is recorded or the recorded one carries `finishedAt`. So a
        // recorded run seen here has finished — and the guard at the top of
        // this function has already returned for it. A second, write-side
        // "clear only a different run" test was written and removed: it is
        // provably dead behind that gate, and a branch no row can reach is not
        // a guard (defect RET-COUNT-EARLY, and the argument is recorded so the
        // next reader does not re-add it).
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
        });
        self.patch_status(json!({
            "lastEnforcement": record,
            "lastEvaluation": { "planRef": { "name": plan_name } },
        }))
        .await?;
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
            // AN OBJECT AT THIS NAME IS NOT THEREBY THIS PLAN (review `d3w9`
            // M6). The name is `<stem>-plan-<12 hex of digest>` and the stem is
            // derived from the policy UID, so it is predictable to anyone who
            // can read the status — and a namespace tenant with `create
            // configmaps` can squat it with different bytes and
            // `immutable: true`. The worker's own digest check would turn that
            // into a permanent exit 3, which is safe and is still a denial of
            // service the controller cannot repair, holding no `delete` verb.
            // Refusing here says which object and why.
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
    /// **It runs `logweir-retention` and not `logweir`** (D-SEAMS S1's third
    /// exception), under its OWN ServiceAccount, with two separately named
    /// credentials: the delete-capable one from
    /// `spec.enforcement.credentialSecretRef`, and the destination's
    /// `evidenceWrite` grant for the record. Neither alone can both remove a
    /// point and write the document that attributes its removal.
    fn build_job(
        &self,
        job_name: &str,
        run_id: &str,
        plan_config_map: &str,
        plan_sha256: &str,
        resolved: &ResolvedDestination,
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
        // The destination's addressing, complete and explicit (D-SEAMS S5).
        let dest_env = resolved.job_env();
        env_literal.extend(dest_env.literals.iter().cloned());
        env_literal.sort_by(|a, b| a.0.cmp(&b.0));
        env_literal.dedup_by(|a, b| a.0 == b.0);

        // THE DELETE-CAPABLE CREDENTIAL, by `secretKeyRef` ONLY. It never
        // appears in the Job spec, in a ConfigMap, in the status or in a log.
        let mut env_from_secret: Vec<crate::job::EnvFromSecret> = Vec::new();
        if let Some(secret) = enforcement.map(|e| e.credential_secret_ref.name.clone()) {
            env_from_secret.push(crate::job::EnvFromSecret {
                name: destination::AWS_ACCESS_KEY_ID_ENV.to_string(),
                secret_name: secret.clone(),
                key: crate::crds::backup_destination::DEFAULT_ACCESS_KEY_ID_KEY.to_string(),
            });
            env_from_secret.push(crate::job::EnvFromSecret {
                name: destination::AWS_SECRET_ACCESS_KEY_ENV.to_string(),
                secret_name: secret,
                key: crate::crds::backup_destination::DEFAULT_SECRET_ACCESS_KEY_KEY.to_string(),
            });
        }
        // The `evidenceWrite` grant, for the record under `logweir/`. The
        // destination's own, resolved separately, and NEVER the delete
        // credential reused.
        env_from_secret.extend(
            dest_env
                .from_secret
                .iter()
                .filter(|e| {
                    // THE DESTINATION'S ARCHIVE CREDENTIAL IS DROPPED HERE ON
                    // PURPOSE. `AWS_ACCESS_KEY_ID` in a retention pod is the
                    // DELETE-capable grant and nothing else; letting the
                    // destination's read grant land on the same variable would
                    // silently decide which of the two the worker deletes with.
                    e.name != destination::AWS_ACCESS_KEY_ID_ENV
                        && e.name != destination::AWS_SECRET_ACCESS_KEY_ENV
                })
                .cloned(),
        );
        if let Some(evidence) = self.evidence_env_from_secret(resolved) {
            env_from_secret.extend(evidence);
        }

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
        });
        // THE BINARY. `job::build` renders the runner's argv against the image's
        // default entrypoint; retention runs a DIFFERENT executable in the same
        // image, so the command is set here and nowhere else.
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

    /// The `evidenceWrite` grant's projected variables, when the destination
    /// declares one distinct from the archive grant.
    fn evidence_env_from_secret(
        &self,
        resolved: &ResolvedDestination,
    ) -> Option<Vec<crate::job::EnvFromSecret>> {
        match &resolved.grant {
            destination::ResolvedGrant::SecretKeys {
                secret,
                access_key_id_key,
                secret_access_key_key,
                session_token_key,
            } => {
                let mut out = vec![
                    crate::job::EnvFromSecret {
                        name: destination::EVIDENCE_ACCESS_KEY_ID_ENV.to_string(),
                        secret_name: secret.clone(),
                        key: access_key_id_key.clone(),
                    },
                    crate::job::EnvFromSecret {
                        name: destination::EVIDENCE_SECRET_ACCESS_KEY_ENV.to_string(),
                        secret_name: secret.clone(),
                        key: secret_access_key_key.clone(),
                    },
                ];
                if let Some(token) = session_token_key {
                    out.push(crate::job::EnvFromSecret {
                        name: destination::EVIDENCE_SESSION_TOKEN_ENV.to_string(),
                        secret_name: secret.clone(),
                        key: token.clone(),
                    });
                }
                Some(out)
            }
            _ => None,
        }
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
        // THREE CONSECUTIVE FAILURES STOP SCHEDULING UNTIL THE SPEC CHANGES.
        // Both halves live in `budget_before`: it returns 0 when
        // `metadata.generation != status.observedGeneration`, which is the only
        // thing on the object that says the spec changed. One rule, read here,
        // in `harvest` and in `publish_evaluation`, so the condition a console
        // reads and the decision this pass makes cannot disagree.
        let failures = self.budget_before();
        if self.budget_spent() {
            return EnforcementDecision {
                start: false,
                enforcement: ENFORCEMENT_LOGWEIR_WORKER,
                reason: REASON_RUN_FAILED,
                message: format!(
                    "{failures} consecutive runs failed; no further run is scheduled until the \
                     spec changes"
                ),
            };
        }
        if evaluation.candidates.is_empty() {
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
        // THE CADENCE, WHICH IS ALSO THE RUN ID'S SLOT (review `d3w9` M5/Q1).
        // An unparseable expression is a refusal: a retention policy whose cron
        // this build cannot read must not fall back to "every reconcile".
        let slot = match self.enforcement_slot() {
            // A SLOT OLDER THAN THE PLAN WINDOW IS NOT ACTED ON. `latest_due_slot`
            // answers "the most recent firing at or before now", which for any
            // cron is almost always Some — a controller that started nine months
            // after a yearly schedule's firing would otherwise treat it as due.
            // The bound is `planMaxAgeSeconds`, deliberately the same one that
            // bounds an approval: a slot no approval could still be valid for is
            // a slot to wait past, not to catch up on.
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
        // ONE RUN PER SLOT. The run id is a pure function of the slot, so a
        // second pass inside one slot recomputes the same name and gets 409
        // `AlreadyExists` — but a pass that has already RECORDED a run for this
        // slot should not even try, or every reconcile would POST a Job it
        // knows is there.
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
        // THE AGE GATE, WHICH THE FIRST LANDING DECLARED AND NEVER ENFORCED
        // (review `d3w9` H4). `planMaxAgeSeconds` is one of D3 §6.5's four
        // gates and the CRD bounds it 300..86400 as if it meant something;
        // `REASON_PLAN_EXPIRED` sat in the closed reason set with nothing
        // emitting it.
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

    // -----------------------------------------------------------------------
    // Status writes
    // -----------------------------------------------------------------------

    async fn publish_evaluation(
        &self,
        evaluation: &plan::Evaluation,
        plan_sha256: &str,
        window: &PlanWindow,
        decision: &EnforcementDecision,
        points: &[PointFacts],
    ) -> Result<PatchOutcome, ReconcileError> {
        let candidates: Vec<Value> = evaluation
            .candidates
            .iter()
            .map(|c| {
                json!({
                    "pointId": c.point_id,
                    "reason": c.reason.as_str(),
                    "recoveryPointAt": Time::from(
                        DateTime::from_timestamp_millis(c.recovery_point_at_ms)
                            .unwrap_or(self.ctx.now),
                    ),
                    // ABSENT WHEN THIS BUILD CANNOT SAY (review `d3w9` M1).
                    // `skip_serializing_if` on the CRD field makes `null` and
                    // absent the same thing on the wire; absent is D3 §12's
                    // "not observed", and it is the honest answer while the
                    // view carries no segment keys.
                    "objects": c.objects(),
                    "bytes": c.bytes,
                })
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
        // `sharedSegments` REPORTS WHAT IS TRUE (review `d3w9` H1). The
        // guarantee is "a segment two points share is not removed with one of
        // them", and `evaluate` can only make it when a point carries its
        // segment keys. `point_facts` cannot supply them — the catalog view
        // entry has no segment field at all — so on every view this build reads
        // the honest answer is `NotEnforced`, and the first landing wrote
        // `LogweirEnforced` unconditionally. That is the withdrawn-guarantee
        // defect class on a status field.
        //
        // Derived from the POINTS rather than from a constant, so the day a
        // view entry carries its segment keys the value changes with no other
        // edit, and a test that goes through `point_facts` is what observes it.
        let segments_visible = points.iter().any(|p| !p.segment_keys.is_empty());
        let guarantees = json!({
            "ageExpiry": if decision.enforcement == ENFORCEMENT_LOGWEIR_WORKER {
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
            // NOT `LogweirEnforced`: `object_store` exposes no WORM readback, so
            // "legal hold respected" means "a provider refusal is authoritative
            // and recorded", never "Logweir knows the hold exists" (D3 §16).
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
                     protected, {} skipped.{}{}",
                    evaluation.points_evaluated,
                    evaluation.kept.len(),
                    evaluation.candidates.len(),
                    evaluation.protected.len(),
                    evaluation.skipped.len(),
                    if segments_visible {
                        ""
                    } else {
                        // SAID ON THE OBJECT, not only in a Rust doc comment
                        // (review `d3w9` H1 and M1).
                        " This catalog view carries no segment keys, so shared-segment                          protection is NotEnforced and the plan names each set's key prefix                          rather than its objects:"
                    },
                    if segments_visible {
                        String::new()
                    } else {
                        format!(
                            " candidates[].objects is omitted rather than guessed, and \
                             `logweir-retention --dry-run` enumerates the real count. \
                             {} point(s) would be removed.",
                            evaluation.candidates.len()
                        )
                    }
                ),
            ),
            (
                CONDITION_ENFORCED,
                if decision.start { "True" } else { "False" },
                decision.reason,
                decision.message.clone(),
            ),
            (
                CONDITION_EXTERNAL_CONFLICT,
                "False",
                REASON_NO_CONFLICT,
                "no bucket lifecycle rule is declared for this destination".to_string(),
            ),
            // `EnforcementDegraded` IS PUBLISHED BY THE PASS THAT ACTS ON IT,
            // not only by the harvest that last incremented the count. The
            // evaluation is where the budget is read and where scheduling is
            // stopped, so it is where the object has to say so — otherwise the
            // one thing an operator can see (a policy that quietly creates no
            // Jobs) has no explanation anywhere, which is what `d387f87`
            // shipped. `budget_spent()` is the same function the decision above
            // used, so the words and the behaviour are one thing.
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
                         spent: {}. No further run is scheduled until spec changes. \
                         status.lastEnforcement.recordKey names the durable record of the last \
                         run.",
                        self.budget_before(),
                        self.last_failure_detail()
                    )
                } else if self.spec_changed() {
                    "the spec changed; the consecutive-failure budget is released and \
                     status.consecutiveRunFailures is reset to 0"
                        .to_string()
                } else {
                    format!("{} consecutive run failures", self.budget_before())
                },
            ),
        ]);
        let mut status = json!({
            "enforcement": decision.enforcement,
            "guarantees": guarantees,
            "lastEvaluation": {
                "at": self.ctx.now,
                "pointsEvaluated": evaluation.points_evaluated,
                "candidateCount": i64::try_from(evaluation.candidates.len()).unwrap_or(i64::MAX),
                "kept": evaluation.kept,
                "candidates": candidates,
                "protected": protected,
                "skipped": skipped,
                "planSha256": plan_sha256,
                "planExpiresAt": window.expires_at,
            },
            "conditions": conditions,
        });
        self.adopt_generation(&mut status);
        self.patch_status(status).await
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

    async fn publish_view_failure(&self, message: &str) -> Result<Outcome, ReconcileError> {
        // A RETENTION EVALUATION FAILURE NEVER BLOCKS A BACKUP. It is a
        // different controller, a different object and a different condition;
        // `Evaluated=False` is the whole of it, and no `Backup` is touched.
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

    /// The FULL condition array, with `rows` upserted into it — never `rows`
    /// alone.
    ///
    /// **`conditions` IS AN ARRAY, AND A MERGE PATCH REPLACES AN ARRAY WHOLE**
    /// (RFC 7386). Returning only the rows a call site happened to name
    /// therefore DELETED every condition it did not name, and that is how
    /// `EnforcementDegraded` came to be missing from a policy that had spent
    /// its retry budget: `harvest` published it, the very next pass published
    /// `publish_evaluation`'s four conditions, and the array replace took it
    /// off the object. Live at `d387f87`: `consecutiveRunFailures 3`,
    /// scheduling correctly stopped, and `EnforcementDegraded` **absent** — not
    /// `False`, not present at all — so nothing a console could read said the
    /// policy had stopped or why.
    ///
    /// It was never only that condition. A pass that evaluates and is then
    /// refused calls `publish_enforcement_refusal`, which names ONE condition;
    /// before this change that single-element array replaced `Ready`,
    /// `Evaluated` and `ExternalLifecycleConflict` too, mid-pass.
    ///
    /// **The base is the status as THIS PASS believes it now stands**, not the
    /// object the watcher delivered: a pass patches more than once, and the
    /// second patch has to preserve what the first one wrote. `observed()` is
    /// exactly that cursor, and the watcher's copy is only the fallback for the
    /// first write of a pass.
    ///
    /// Order is stable — existing conditions keep their positions, new types
    /// are appended — so a diff of two status writes shows what changed rather
    /// than a reshuffle.
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

    /// One condition, carrying the `lastTransitionTime` the `metav1.Condition`
    /// contract says it should — `merge_condition` keeps the existing one when
    /// neither `status` nor `reason` moved, so a condition that has not
    /// transitioned does not look like it has on every pass.
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

    /// Adopt `metadata.generation` onto the status — **and, when it advances,
    /// release the consecutive-failure budget in the SAME patch.**
    ///
    /// EVERY WRITER THAT SETS `observedGeneration` GOES THROUGH HERE, and the
    /// reason is a hole this branch's first landing left (review finding G1).
    /// Seven writers adopt the generation; only the evaluation released the
    /// budget. `evaluate()` returns through the refusal writers **before** the
    /// evaluation is ever reached — an unreadable catalog view, a refused plan,
    /// an unusable destination, a declared external lifecycle — so a policy
    /// degraded at the ceiling, whose operator edits the spec, and whose view
    /// is still unreadable, had its edit CONSUMED: `observedGeneration` moved,
    /// `consecutiveRunFailures` stayed at 3, `spec_changed()` went false, and
    /// the budget was spent again. The runs were failing for a reason, so a
    /// refusal co-occurring with the edit is the likely case, not an exotic
    /// one. Every further edit would be eaten the same way.
    ///
    /// ONE RULE, STATED ONCE — the argument `budget_before()` already makes,
    /// applied to the write side. Adopting the generation and releasing the
    /// budget are the same event and are therefore the same patch: splitting
    /// them would let a 409 between the two leave the generation new and the
    /// count at its ceiling, which is the defect with an extra step.
    ///
    /// The `EnforcementDegraded` condition is cleared here too, upserted into
    /// whatever conditions the caller has already built, so an operator never
    /// sees `EnforcementDegraded=True` beside `consecutiveRunFailures: 0`.
    fn adopt_generation(&self, status: &mut Value) {
        let object = status
            .as_object_mut()
            .expect("a status patch is always a JSON object");
        object.insert("observedGeneration".to_string(), json!(self.generation()));
        if !self.spec_changed() {
            return;
        }
        // THE HELPER FILLS IN WHAT THE CALLER DID NOT SAY, and never overrides
        // it. `harvest` writes the count this run produced — and `budget_before()`
        // has already zeroed the history for it, so a run that failed under the
        // NEW spec is one failure and not none; overwriting that with the
        // release's `0` would lose the run. Same for the condition: a writer
        // that has published its own `EnforcementDegraded` has more to say
        // about it than "released" does.
        //
        // AN EXPLICIT `0`, NEVER A DELETION: "no consecutive failures" is an
        // answer, and a console showing an empty field would be showing an
        // absence where there is one. And inserted only on the pass that adopts
        // a NEW generation — in an RFC 7386 merge a key carried on every pass
        // would overwrite a count the harvests are keeping.
        object
            .entry("consecutiveRunFailures".to_string())
            .or_insert_with(|| json!(0));
        // "THE CALLER SAID IT" MEANS THIS PASS, NOT THE OBJECT'S HISTORY.
        // `conditions()` preserves every condition the object already carries,
        // so the array always holds an `EnforcementDegraded` once one has ever
        // been published — checking for its mere presence would make this arm
        // dead. A row a caller WROTE this pass carries this pass's
        // `observedGeneration`; a row merely carried forward still carries the
        // old one. That is the difference, and it is the difference that makes
        // `harvest`'s freshly computed verdict win while a stale `True` from
        // before the edit is replaced.
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
            // A writer that carries no conditions at all still gets the release
            // one, built over the full existing array so nothing is dropped.
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

    /// Every status write is a resourceVersion-preconditioned merge PATCH —
    /// D-SEAMS **S7**, in both halves.
    /// The version the next patch will precondition on.
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
        // CLONED OUT BEFORE THE AWAIT. A guard alive across an `.await` makes
        // the reconcile future non-`Send`, which `kube`'s `Controller` refuses.
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
                // THE CURSOR MOVES. This is the only fresh version available to
                // a reconciler that holds no `get` on its own kind, and the
                // next patch of this pass needs it (review `d3w9` C3).
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
            // A 409 IS THE PRECONDITION WORKING: something wrote this status
            // between the read and the write, so the object in hand is stale.
            // **It is returned, not swallowed** — see `PatchOutcome::Conflict`.
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

/// What a finished retention run reported about itself.
///
/// Parsed from the worker's `retention-point=` / `retention-record=` /
/// `retention-result=` key lines — **by name and never by position**, the rule
/// `notify-result=` and `refusal-reason=` already follow, because a pod log is
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
}

impl RunReport {
    /// Whether the only thing that stopped this run was its own object ceiling
    /// — review `d3w9` **M2**.
    ///
    /// Such a run exits 1 (work remains) and must **not** count toward
    /// `consecutiveRunFailures`: three bounded runs on a large archive would
    /// otherwise set `EnforcementDegraded` and stop retention for good.
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

/// Read a retention Job's key lines — **pure**.
///
/// Bounded by construction: it reads only lines carrying one of the three
/// prefixes, and a value it cannot parse is skipped rather than guessed.
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
/// A named enum rather than `Option<String>`: "the lease did not land", "a
/// restore is reading this destination" and "something else owns the plan
/// ConfigMap's name" are three different findings that an operator fixes in
/// three different places, and the first landing collapsed them into `None`.
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
    /// A nonterminal `Restore` reads this destination.
    ActiveRestore,
    /// An object already holds the plan `ConfigMap`'s name and is not this
    /// plan.
    PlanConfigMapConflict(String),
}

/// How long the current plan stays approvable — review `d3w9` H4.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanWindow {
    /// When the window closes. Published as
    /// `status.lastEvaluation.planExpiresAt`.
    pub expires_at: DateTime<Utc>,
    /// Whether THIS pass observed the previous window lapse. A pass that did
    /// starts no run and re-anchors the window, so the administrator's next
    /// look sees a fresh preview of the same plan rather than a wedged policy.
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

// ---------------------------------------------------------------------------
// Small pure helpers
// ---------------------------------------------------------------------------

/// "the last run exited 1 with AccessDenied on 2 point(s)", and the honest
/// shapes when there is less to say.
///
/// **The codes are the closed per-point vocabulary and never a raw provider
/// error body** — the same rule the record follows. Duplicates are counted, not
/// listed: three points denied for one reason is one reason.
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
    let named: Vec<String> = counted
        .into_iter()
        .map(|(code, n)| format!("{code} on {n} point(s)"))
        .collect();
    format!("{exit} with {}", named.join(", "))
}

/// One view entry, as the evaluation sees it.
///
/// **`segment_keys` is empty and that is the view's shape, not an omission
/// here**: D3 W8's page entry carries `manifestKey` and no segment list. The
/// plan therefore names the manifest and the set's key bound, and the worker
/// enumerates within that bound — see `retention_plan::PlanLine::enumerate_set`.
#[must_use]
pub fn point_facts(entry: &ViewEntry) -> PointFacts {
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
    }
}

/// The terminal `Restore` phases. A restore in any other phase — including one
/// with no phase at all, which is a `Restore` the reconciler has not seen yet —
/// is nonterminal and protects its point.
pub const TERMINAL_RESTORE_PHASES: &[&str] = &["Succeeded", "Failed", "Refused"];

/// Whether this `Restore` has finished.
#[must_use]
pub fn is_terminal_restore(restore: &Restore) -> bool {
    restore
        .status
        .as_ref()
        .and_then(|s| s.phase.as_deref())
        .is_some_and(|p| TERMINAL_RESTORE_PHASES.contains(&p))
}

/// Whether this `Restore` reads from the destination under evaluation.
///
/// # BY IDENTITY, NEVER BY SUBSTRING (review `d3w9` **C2**)
///
/// The first landing asked `location_id.contains(reference.name)`.
/// `location_id` is the destination's canonical URL — `s3://<bucket>/<prefix>`
/// — which does not contain the `BackupDestination` OBJECT's name, and
/// `crds/restore.rs`'s CEL rule forces `sourceArchive.url` to the sentinel
/// `logweir-destination://<name>` whenever `sourceDestinationRef` is set, so
/// the URL fallback could not rescue it either. A nonterminal `Restore` that
/// named its destination by reference — the path the whole D2/D3 design steers
/// toward — was therefore **not seen at all**: no `ActiveRestore` protection,
/// nothing in the consistent re-list, and the reaper free to delete the
/// manifest and segments of a set a live restore was reading. Whether it
/// misfired was a coincidence of spelling: a destination named `kafka` *would*
/// have matched `s3://lw/kafka-backups/…`.
///
/// So: when the restore names a destination, compare the two NAMES and require
/// the same namespace — a `destinationRef` is namespace-local, so the same name
/// elsewhere is a different object. This is the comparison
/// `controllers/protection_policy.rs` already makes. The URL equality stays,
/// and only for the case it was written for: a legacy restore that names no
/// destination at all.
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

// ---------------------------------------------------------------------------
// The registration point
// ---------------------------------------------------------------------------

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
    let seconds = match outcome.phase {
        RetentionPhase::Running | RetentionPhase::Started => RUNNING_REQUEUE_SECONDS,
        _ => IDLE_REQUEUE_SECONDS,
    };
    Ok(Action::requeue(std::time::Duration::from_secs(seconds)))
}

fn error_policy(policy: Arc<RetentionPolicy>, err: &ReconcileError, _ctx: Arc<Context>) -> Action {
    warn!(
        policy = %policy.name_any(),
        namespace = %policy.namespace().unwrap_or_default(),
        error = %err,
        "the RetentionPolicy reconcile could not complete; nothing was deleted"
    );
    Action::requeue(std::time::Duration::from_secs(ERROR_REQUEUE_SECONDS))
}

/// Run the `RetentionPolicy` controller until the process ends.
///
/// **It takes no archive handle.** Deletion happens inside the retention Job
/// with its own credential; the evaluation reads the catalog's view out of
/// `ConfigMap`s. This process never holds a handle that could reach either.
pub fn controller(
    client: kube::Client,
    runner_image: crate::job::RunnerImage,
) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<RetentionPolicy> = Api::all(client.clone());
    let jobs: Api<Job> = Api::all(client.clone());
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
