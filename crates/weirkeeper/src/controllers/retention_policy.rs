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
//!    `planMaxAgeSeconds`. A rules edit bumps `metadata.generation`, which is
//!    inside the plan bytes, which changes the digest — so a policy change
//!    invalidates an approval without anything having to remember to.
//! 3. A lease is written with a resourceVersion-preconditioned patch and THEN a
//!    consistent, non-cached, cluster-wide list of `Restore`s is made. A
//!    restore that arrived after the lease cannot slip past the list, because
//!    the list happens second.
//! 4. The worker itself re-validates every key, refuses the whole plan on the
//!    first one outside `<scope>/<backupId>/`, and writes the attributable
//!    record before it deletes.
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
use crate::conditions::{current_condition, merge_condition, status_unchanged};
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
}

impl Pass<'_> {
    fn generation(&self) -> i64 {
        self.policy.metadata.generation.unwrap_or(0)
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
        format!(
            "{NAME_PREFIX}{}",
            &logweir_core::ids::sha256_hex(self.uid.as_bytes())[..20]
        )
    }

    fn status_value(&self) -> Option<Value> {
        self.policy
            .status
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok())
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
        self.patch_status(json!({
            "observedGeneration": self.generation(),
            "enforcement": ENFORCEMENT_EXTERNAL,
            "guarantees": guarantees,
            "conditions": conditions,
        }))
        .await?;
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
            return Ok(Some(self.harvest(&run_id, None, None).await?));
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
        let exit_code = self.exit_code_of(&job).await?;
        Ok(Some(self.harvest(&run_id, Some(&job), exit_code).await?))
    }

    /// The exit code, through the pod's OWNER UID and never through a label
    /// (D-SEAMS **S6**, defect SEC-PODLOG).
    async fn exit_code_of(&self, job: &Job) -> Result<Option<i32>, ReconcileError> {
        let pod = check::pod::find_owned_pod(self.ctx.client, &self.namespace, job).await?;
        Ok(pod.and_then(|p| super::backup::terminated_exit_code(&p)))
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
        self.patch_status(json!({
            "observedGeneration": self.generation(),
            "enforcement": ENFORCEMENT_LOGWEIR_WORKER,
            "conditions": conditions,
        }))
        .await?;
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
        exit_code: Option<i32>,
    ) -> Result<Outcome, ReconcileError> {
        let failed = exit_code != Some(0);
        let previous = self
            .policy
            .status
            .as_ref()
            .and_then(|s| s.consecutive_run_failures)
            .unwrap_or(0);
        let failures = if failed { previous + 1 } else { 0 };
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
            Some(code) => format!(
                "retention run {run_id} exited {code}: at least one point did not complete. The \
                 signed-key record under logweir/retention/ names which."
            ),
            None => format!(
                "retention run {run_id} produced no exit code: its pod is gone or was never \
                 readable. The record under logweir/retention/ is the durable answer."
            ),
        };
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
                        "{failures} consecutive runs have failed; scheduling stops until \
                         spec changes"
                    )
                } else {
                    format!("{failures} consecutive run failures")
                },
            ),
        ]);
        self.patch_status(json!({
            "observedGeneration": self.generation(),
            "enforcement": ENFORCEMENT_LOGWEIR_WORKER,
            "lastEnforcement": {
                "runId": run_id,
                "finishedAt": self.ctx.now,
                "exitCode": exit_code,
            },
            // RFC 7386: `null` DELETES the key. The lease exists only while a
            // run holds it, and a lease left behind would hold restore
            // admission for nothing.
            "lease": Value::Null,
            "consecutiveRunFailures": failures,
            "conditions": conditions,
        }))
        .await?;

        // THE TTL AFTER THE STATUS, and only then (S7). The Job's pod carries
        // the exit code this pass just published.
        if let Some(job) = job {
            self.patch_job_ttl(job).await?;
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
        if !scope.starts_with(dest_prefix) {
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

        let document =
            match plan::plan_document(&self.identity(), &dest, rules, &evaluation, self.ctx.now) {
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
        let expires_at = self.ctx.now + chrono::Duration::seconds(plan_max_age);

        // ENFORCE, OR NOT.
        let decision = self.enforcement_decision(&plan_sha256, &evaluation);
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

        self.publish_evaluation(&evaluation, &plan_sha256, expires_at, &decision)
            .await?;

        if decision.start {
            match self
                .start_run(&resolved, &plan_bytes, &plan_sha256, &evaluation.candidates)
                .await?
            {
                Some(job_name) => {
                    outcome.phase = RetentionPhase::Started;
                    outcome.job_name = Some(job_name);
                    outcome.enforced_reason = REASON_RUN_IN_PROGRESS;
                }
                None => {
                    outcome.enforced_reason = REASON_ACTIVE_RESTORE;
                }
            }
        }
        Ok(outcome)
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
            // THE PAGE DIGEST, CHECKED. `status.pages[].sha256` is over exactly
            // these bytes, so a page that changed under the reader is caught
            // here rather than turned into a plan.
            if let Some(expected) = page.sha256.as_deref() {
                let lines: Vec<&str> = body.lines().filter(|l| !l.trim().is_empty()).collect();
                let found = view::page_digest(&lines);
                if found != expected {
                    return Ok(Err(format!(
                        "catalog page {} digests to {found} and the catalog published \
                         {expected}; the view is not what the catalog says it is",
                        page.config_map_name
                    )));
                }
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
            if !restore_touches(restore, location_id, &self.namespace) {
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
    ) -> Result<Option<String>, ReconcileError> {
        let leased: Vec<String> = candidates.iter().map(|c| c.point_id.clone()).collect();
        let slot = self.ctx.now.timestamp() / 60;
        let run_id = plan::run_id(&self.uid, plan_sha256, slot);

        // (a) THE LEASE, with a resourceVersion-preconditioned patch.
        let expires_at = self.ctx.now
            + chrono::Duration::seconds(
                self.policy
                    .spec
                    .enforcement
                    .as_ref()
                    .map_or(1800, |e| i64::from(e.deadline_seconds))
                    + LEASE_MARGIN_SECONDS,
            );
        self.patch_status(json!({
            "lease": {
                "runId": run_id,
                "pointIds": leased.clone(),
                "acquiredAt": self.ctx.now,
                "expiresAt": expires_at,
            },
        }))
        .await?;

        // (b) AND THEN the consistent re-list. The ORDER is the whole property:
        //     a restore that arrives after (a) is seen by (b); a restore that
        //     arrives after (b) is held by restore admission, which refuses
        //     while a matching lease exists.
        let active_sets = self.active_restore_sets(&resolved.canonical_url).await?;
        let contested: Vec<&str> = candidates
            .iter()
            .filter(|c| active_sets.contains(&c.backup_id))
            .map(|c| c.point_id.as_str())
            .collect();
        if !contested.is_empty() {
            warn!(
                policy = %self.name, namespace = %self.namespace,
                points = contested.len(),
                "a nonterminal Restore references a leased point; no retention Job is created"
            );
            return Ok(None);
        }

        // The plan `ConfigMap`, immutable and owned by the policy.
        let plan_name = format!(
            "{}-plan-{}",
            self.stem(),
            &plan_sha256.trim_start_matches("sha256:")[..12]
        );
        self.ensure_plan_config_map(&plan_name, plan_bytes, plan_sha256)
            .await?;

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
            Err(kube::Error::Api(e)) if e.code == 409 => {
                debug!(job = %job_name, "the retention Job already exists; this pass creates none");
            }
            Err(e) => return Err(ReconcileError::Api(e)),
        }
        self.patch_status(json!({
            "lastEnforcement": {
                "runId": run_id,
                "startedAt": self.ctx.now,
                "planSha256": plan_sha256,
            },
        }))
        .await?;
        Ok(Some(job_name))
    }

    async fn ensure_plan_config_map(
        &self,
        name: &str,
        bytes: &[u8],
        digest: &str,
    ) -> Result<(), ReconcileError> {
        let maps: Api<ConfigMap> = Api::namespaced(self.ctx.client.clone(), &self.namespace);
        if maps.get_opt(name).await?.is_some() {
            // IMMUTABLE AND CONTENT-NAMED, so an existing object at this name
            // holds exactly these bytes; there is nothing to reconcile.
            return Ok(());
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
            Ok(_) | Err(kube::Error::Api(kube::core::ErrorResponse { code: 409, .. })) => Ok(()),
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
        let failures = self
            .policy
            .status
            .as_ref()
            .and_then(|s| s.consecutive_run_failures)
            .unwrap_or(0);
        let observed = self
            .policy
            .status
            .as_ref()
            .and_then(|s| s.observed_generation)
            .unwrap_or(-1);
        // THREE CONSECUTIVE FAILURES STOP SCHEDULING UNTIL THE SPEC CHANGES.
        // "The spec changed" is `metadata.generation != observedGeneration`,
        // which is the only thing on the object that says so.
        if failures >= DEGRADED_AFTER_FAILURES && observed == self.generation() {
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
        expires_at: DateTime<Utc>,
        decision: &EnforcementDecision,
    ) -> Result<(), ReconcileError> {
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
        let guarantees = json!({
            "ageExpiry": if decision.enforcement == ENFORCEMENT_LOGWEIR_WORKER {
                GUARANTEE_LOGWEIR
            } else {
                GUARANTEE_NOT_ENFORCED
            },
            "minUsablePoints": GUARANTEE_LOGWEIR,
            "activeRestoreProtection": GUARANTEE_LOGWEIR,
            "sharedSegments": GUARANTEE_LOGWEIR,
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
                     protected, {} skipped",
                    evaluation.points_evaluated,
                    evaluation.kept.len(),
                    evaluation.candidates.len(),
                    evaluation.protected.len(),
                    evaluation.skipped.len()
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
        ]);
        self.patch_status(json!({
            "observedGeneration": self.generation(),
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
                "planExpiresAt": expires_at,
            },
            "conditions": conditions,
        }))
        .await
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
        self.patch_status(json!({
            "observedGeneration": self.generation(),
            "enforcement": ENFORCEMENT_RECOMMENDATION_ONLY,
            "conditions": conditions,
        }))
        .await?;
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
        self.patch_status(json!({
            "observedGeneration": self.generation(),
            "enforcement": ENFORCEMENT_RECOMMENDATION_ONLY,
            "conditions": conditions,
        }))
        .await?;
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
        self.patch_status(json!({
            "observedGeneration": self.generation(),
            "enforcement": ENFORCEMENT_RECOMMENDATION_ONLY,
            "conditions": conditions,
        }))
        .await?;
        Ok(refused(reason))
    }

    fn conditions(&self, rows: &[(&str, &str, &str, String)]) -> Vec<Value> {
        let existing = self
            .policy
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref());
        rows.iter()
            .map(|(r#type, status, reason, message)| {
                let next = Condition {
                    r#type: (*r#type).to_string(),
                    status: (*status).to_string(),
                    observed_generation: Some(self.generation()),
                    last_transition_time: Some(self.ctx.now),
                    reason: Some((*reason).to_string()),
                    message: Some(message.clone()),
                };
                let merged = merge_condition(current_condition(existing, r#type), next);
                serde_json::to_value(merged).unwrap_or(Value::Null)
            })
            .collect()
    }

    /// Every status write is a resourceVersion-preconditioned merge PATCH —
    /// D-SEAMS **S7**, in both halves.
    async fn patch_status(&self, status: Value) -> Result<(), ReconcileError> {
        let patch = json!({ "status": status });
        if status_unchanged(self.status_value().as_ref(), &patch) {
            debug!(
                policy = %self.name, namespace = %self.namespace,
                "the computed status equals the one on the object; no patch is sent"
            );
            return Ok(());
        }
        let Some(resource_version) = self
            .policy
            .metadata
            .resource_version
            .clone()
            .filter(|v| !v.is_empty())
        else {
            warn!(
                policy = %self.name, namespace = %self.namespace,
                "RetentionPolicy carries no metadata.resourceVersion, which a /status \
                 compare-and-set needs (D-SEAMS S7); no patch is sent"
            );
            return Ok(());
        };
        let mut body = patch;
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
            Ok(_) => Ok(()),
            // A 409 IS THE PRECONDITION WORKING: something wrote this status
            // between the read and the write, so the object in hand is stale.
            Err(kube::Error::Api(e)) if e.code == 409 => {
                debug!(
                    policy = %self.name, namespace = %self.namespace,
                    "the status changed under this reconcile (409); the next pass reads it"
                );
                Ok(())
            }
            Err(e) => Err(ReconcileError::Api(e)),
        }
    }
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
/// Matched on the destination REFERENCE when the restore names one, and on the
/// archive URL otherwise. A restore in another namespace naming a destination of
/// the same name is NOT this destination: a `destinationRef` is namespace-local.
#[must_use]
pub fn restore_touches(restore: &Restore, location_id: &str, namespace: &str) -> bool {
    if let Some(reference) = restore.spec.source_destination_ref.as_ref() {
        return restore.namespace().as_deref() == Some(namespace)
            && location_id.contains(reference.name.as_str());
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
