//! The `RecoveryCatalog` reconciler: run a bounded sync, and publish a bounded
//! view of what can actually be recovered — D3 §5.3, PLAT-15.1's controller
//! half.
//!
//! # What it does, in one paragraph
//!
//! It resolves the catalog's destination, renders an immutable `catalogSync`
//! check plan, runs it as ONE short-lived Job through the shared check
//! framework, finds that Job's pod **by controller-owner UID** (D-SEAMS **S6**),
//! reads the framed stdout, re-evaluates every point's signer against this
//! installation's trust source, materialises the newest ≤ 5 000 points into
//! immutable `ConfigMap` pages **owned by the sync Job**, and writes the counts,
//! the histogram, the signer summary and the fence pointer onto `/status` with a
//! conditional merge PATCH (D-SEAMS **S7**).
//!
//! # It deletes nothing, and that is a design and not an omission
//!
//! There is no `delete` verb anywhere in the `weirkeeper` `ClusterRole` and
//! `Api::delete` is called nowhere under `crates/weirkeeper/src/`. The view is
//! collected by the sync Job's `ttlSecondsAfterFinished`
//! ([`crate::catalog_view::ttl_seconds`]): when the TTL controller removes the
//! Job, Kubernetes garbage collection removes its pages. A catalog whose syncing
//! stopped therefore reports `Stale` and then `ViewExpired` rather than an empty
//! archive — honest, and the durable truth in object storage is untouched.
//!
//! # It is never a second source of truth
//!
//! D3 §17: the view is a derived, bounded projection. The signed receipt is the
//! verification root, the counts describe the whole walk while the pages carry
//! only the window, and every restore re-verifies its point at execution
//! (PLAT-15.2 §5.5.6). Nothing here is evidence; it is an index onto evidence.
//!
//! # The one thing the Job decides and the one thing this file decides
//!
//! The Job reads the archive, so it decides availability and whether a DSSE
//! signature verified. **Trust is decided here**, once, in
//! [`crate::catalog_view::classify_verification`], against
//! [`crate::catalog_view::TrustView`] — today synthesised from the
//! `TrustRoster`, which is the trust source in use. D3 W1/W10 replace that one
//! constructor with the bound `TrustPolicy`, and nothing else in this file
//! moves.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::{Api, Patch, PatchParams, PostParams};
use kube::runtime::controller::Action;
use kube::runtime::{watcher, Controller};
use kube::{Resource as _, ResourceExt as _};
use serde_json::{json, Value};
use tracing::{debug, info, warn};

use logweir_core::check_contract::{CheckCode, FrameExpectations, Stream};
use logweir_core::destination::DestinationRole;

use crate::catalog_view::{self as view, PageConflict, SyncTrigger, TrustView, ViewLimits};
use crate::check::{self, CheckPhase};
use crate::conditions::{current_condition, merge_condition, status_unchanged};
use crate::crds::recovery_catalog::{LastSyncJob, RecoveryCatalog, SyncCursor};
use crate::crds::{Condition, Time};
use crate::destination::{self, ResolveError};
use crate::job::RunnerOwner;

use super::Context;

// ---------------------------------------------------------------------------
// Conditions — the four D3 §5.3 names, and their closed reasons
// ---------------------------------------------------------------------------

/// `Ready`: a usable view exists right now.
pub const CONDITION_READY: &str = "Ready";
/// `Synced`: what the last sync did.
pub const CONDITION_SYNCED: &str = "Synced";
/// `Stale`: the view is older than two sync intervals.
pub const CONDITION_STALE: &str = "Stale";
/// `TrustAvailable`: this installation holds key material to verify with.
pub const CONDITION_TRUST_AVAILABLE: &str = "TrustAvailable";

/// The four condition types, in the order they are written.
pub const CONDITION_TYPES: &[&str] = &[
    CONDITION_READY,
    CONDITION_SYNCED,
    CONDITION_STALE,
    CONDITION_TRUST_AVAILABLE,
];

/// `Ready=True`: pages are published and have not aged out.
pub const REASON_VIEW_READY: &str = "ViewReady";
/// `Ready=Unknown`: no sync has produced a view yet.
pub const REASON_NEVER_SYNCED: &str = "NeverSynced";
/// `Ready=Unknown`: a sync is running and no view has been published yet.
pub const REASON_SYNC_IN_PROGRESS: &str = "SyncInProgress";
/// `Ready=False`: the pages aged out with their Job's TTL. **The archive is
/// untouched** — this says the Kubernetes window is gone, not that the backups
/// are.
pub const REASON_VIEW_EXPIRED: &str = "ViewExpired";
/// `Ready=False`: the destination this catalog names is not usable.
pub const REASON_DESTINATION_UNUSABLE: &str = "DestinationUnusable";
/// `Ready=False`: `spec.legacyArchive` — see [`LEGACY_ARCHIVE_MESSAGE`].
pub const REASON_LEGACY_ARCHIVE_UNSUPPORTED: &str = "LegacyArchiveUnsupported";
/// `Ready=False`: a page name is taken by an object this sync did not write.
pub const REASON_PAGE_CONFLICT: &str = "PageConflict";
/// `Ready=False`: a Job carrying this sync's name is controlled by something
/// else. D-SEAMS **S6**: a name is not an identity.
pub const REASON_JOB_NAME_CONFLICT: &str = "JobNameConflict";

/// `Synced=True`: the walk completed and its result read.
pub const REASON_SUCCEEDED: &str = "Succeeded";
/// `Synced=False`: some of the archive could not be read. The entries say
/// `Unreadable` and NEVER `Missing` — "could not tell" is not "is not there".
pub const REASON_PARTIAL_SCAN: &str = "PartialScan";
/// `Synced=False`: the budget ran out before the walk finished. The cursor is
/// recorded and the next sync continues; this is not a failure.
pub const REASON_SCAN_INCOMPLETE: &str = "ScanIncomplete";

/// `Stale=True`.
pub const REASON_VIEW_STALE: &str = "ViewStale";
/// `Stale=False`.
pub const REASON_VIEW_FRESH: &str = "ViewFresh";

/// `TrustAvailable=True`.
pub const REASON_TRUST_MATERIAL_PRESENT: &str = "TrustMaterialPresent";
/// `TrustAvailable=False`: nothing here can verify anything, and every point is
/// `NotAttempted` rather than `Invalid`.
pub const REASON_NO_TRUST_MATERIAL: &str = "NoTrustMaterial";

/// Every reason this reconciler can write. One list, so the
/// `metav1.Condition.reason` regex test cannot miss one.
pub const CONDITION_REASONS: &[&str] = &[
    REASON_VIEW_READY,
    REASON_NEVER_SYNCED,
    REASON_SYNC_IN_PROGRESS,
    REASON_VIEW_EXPIRED,
    REASON_DESTINATION_UNUSABLE,
    REASON_LEGACY_ARCHIVE_UNSUPPORTED,
    REASON_PAGE_CONFLICT,
    REASON_JOB_NAME_CONFLICT,
    REASON_SUCCEEDED,
    REASON_PARTIAL_SCAN,
    REASON_SCAN_INCOMPLETE,
    REASON_VIEW_STALE,
    REASON_VIEW_FRESH,
    REASON_TRUST_MATERIAL_PRESENT,
    REASON_NO_TRUST_MATERIAL,
];

/// What `Ready=False/LegacyArchiveUnsupported` says.
///
/// AN ABSENT CAPABILITY, NAMED (D3 §5.3's own rule for `catalogWindowQuery`).
/// `spec.legacyArchive` is admitted by the CRD because PLAT-15.2 will use it;
/// this controller resolves `spec.destinationRef` and nothing else, and says so
/// rather than creating a Job it cannot address.
pub const LEGACY_ARCHIVE_MESSAGE: &str =
    "this build syncs a catalog through spec.destinationRef only; spec.legacyArchive is \
     PLAT-15.2's connect-an-existing-archive path and no sync Job is created for it. Create a \
     BackupDestination with a read-only archiveRead grant and a RecoveryCatalog that names it.";

// ---------------------------------------------------------------------------
// Timing
// ---------------------------------------------------------------------------

/// The requeue while a sync Job is running.
pub const RUNNING_REQUEUE_SECONDS: u64 = 15;
/// The requeue when nothing is running. Short enough that `Stale` and
/// `ViewExpired` appear when they become true, and cheap because an unchanged
/// status sends no PATCH at all.
pub const IDLE_REQUEUE_SECONDS: u64 = 60;
/// The requeue after an error.
pub const ERROR_REQUEUE_SECONDS: u64 = 30;

/// The sync plan's own budget, in seconds.
///
/// FIFTEEN MINUTES, against `maxObjectsPerRun`'s ceiling of a million keys. The
/// Job's `activeDeadlineSeconds` is this plus the framework's 90-second margin
/// for image pull and scheduling, so a deadline that fires means the POD never
/// got going — a different fact from a walk that ran out of budget, which
/// records its cursor and continues.
pub const SYNC_TIMEOUT_SECONDS: i64 = 900;

// ---------------------------------------------------------------------------
// Errors and outcomes
// ---------------------------------------------------------------------------

/// Why a reconcile could not finish.
///
/// ONE VARIANT, AND THAT IS THE DESIGN: every refusal this reconciler can make
/// is a VERDICT and is written to the status, so the only thing left to fail on
/// is the API server. A reconciler that returned `Err` for "the destination is
/// not valid" would requeue forever and publish nothing an operator could read.
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

/// Where one pass left the catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogPhase {
    /// Nothing is due; the published view (if any) stands.
    Idle,
    /// A sync Job was created this pass.
    Started,
    /// A sync Job is running.
    Running,
    /// A sync finished and its view was published.
    Published,
    /// A sync finished and did not produce a readable view.
    Failed,
    /// A terminal refusal that is not about a sync at all.
    Refused,
}

/// What one pass did — the value every controller-double test asserts over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    /// Where the pass left the catalog.
    pub phase: CatalogPhase,
    /// `Ready`'s status: `True`, `False` or `Unknown`.
    pub ready: &'static str,
    /// `Ready`'s reason.
    pub ready_reason: &'static str,
    /// `Synced`'s reason — a closed [`CheckCode`] spelling on a failure.
    pub synced_reason: String,
    /// How many page `ConfigMap`s this pass wrote or adopted.
    pub pages: usize,
    /// How many entries the published view carries.
    pub entries: usize,
    /// Whether the archive holds more points than the view.
    pub truncated: bool,
    /// The sync Job this pass acted on, if any.
    pub job_name: Option<String>,
}

// ---------------------------------------------------------------------------
// The context a pass needs
// ---------------------------------------------------------------------------

/// Everything one pass reads that is not the object itself.
///
/// `now` and `policy` are ARGUMENTS and not reads, which is what lets a whole
/// state machine — never synced, running, published, expired, stale — be driven
/// over a route table with no clock and no `ConfigMap` fetch.
pub struct SyncContext<'a> {
    /// The client every `Api` is built from.
    pub client: &'a kube::Client,
    /// The installation policy, for destination resolution.
    pub policy: &'a check::policy::Policy,
    /// The image and pull policy the sync Job will name.
    pub runner_image: &'a crate::job::RunnerImage,
    /// This pass's instant.
    pub now: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// The pass
// ---------------------------------------------------------------------------

/// Reconcile one `RecoveryCatalog`.
///
/// # The order, and why each step is where it is
///
/// 1. **Identity.** No namespace or no UID means no Job can be named and no
///    reference can be resolved; it is reported and nothing is created.
/// 2. **The tracked sync Job, before any decision to start one.** A catalog
///    that started a sync owns exactly one at a time, and a pass that computed
///    a new trigger first could create a second one for the same archive.
/// 3. **Harvest before trigger.** A finished Job that has not been read yet is
///    always more useful than a new one.
/// 4. **The trigger.** `spec.syncRequest` wins over the interval, and the token
///    it produces is what makes a repeated request idempotent: the Job's name is
///    a pure function of it, so a duplicate reconcile gets 409 `AlreadyExists`
///    from the API server instead of running a second walk.
///
/// # Errors
///
/// [`ReconcileError::Api`] only — every other refusal is a status write.
pub async fn reconcile_catalog(
    catalog: &RecoveryCatalog,
    ctx: &SyncContext<'_>,
) -> Result<Outcome, ReconcileError> {
    let name = catalog.name_any();
    let Some(namespace) = catalog.namespace().filter(|n| !n.is_empty()) else {
        // Unreachable for a namespaced object that came from a watch, and named
        // rather than unwrapped.
        warn!(catalog = %name, "RecoveryCatalog carries no metadata.namespace; nothing is done");
        return Ok(refused_outcome(REASON_DESTINATION_UNUSABLE));
    };
    let Some(uid) = catalog.uid().filter(|u| !u.is_empty()) else {
        warn!(catalog = %name, namespace = %namespace, "RecoveryCatalog carries no metadata.uid");
        return Ok(refused_outcome(REASON_DESTINATION_UNUSABLE));
    };

    let trust = trust_view(ctx.client).await?;
    let mut pass = Pass {
        catalog,
        name,
        namespace,
        uid,
        trust,
        ctx,
        // Corrected by `run` as its first act; `false` is the conservative
        // starting point, and an arm that ran before the lookup would report a
        // view as gone rather than report a view that is gone as present.
        job_present: false,
    };
    pass.run().await
}

fn refused_outcome(reason: &'static str) -> Outcome {
    Outcome {
        phase: CatalogPhase::Refused,
        ready: "False",
        ready_reason: reason,
        synced_reason: String::new(),
        pages: 0,
        entries: 0,
        truncated: false,
        job_name: None,
    }
}

/// The trust source, projected — **the seam D3 W1/W10 replaces**.
///
/// `TrustRoster` is the trust source in use today, so this is where it is read.
/// When `TrustPolicy` resolution lands, this function resolves the policy bound
/// to the catalog's namespace instead and everything downstream is unchanged:
/// [`view::TrustView`] already carries `Retired` and `Revoked`, and
/// [`view::classify_verification`] already produces `VerifiedHistorical` and
/// `Revoked` from them.
async fn trust_view(client: &kube::Client) -> Result<TrustView, ReconcileError> {
    let spec = super::roster_spec(client).await?;
    Ok(TrustView::from_roster(&spec))
}

struct Pass<'a> {
    catalog: &'a RecoveryCatalog,
    name: String,
    namespace: String,
    uid: String,
    trust: TrustView,
    ctx: &'a SyncContext<'a>,
    /// Whether the Job named in `status.lastSyncJob` still exists.
    ///
    /// SET ONCE PER PASS, BEFORE ANY ARM RUNS (review finding F2). The pages
    /// are owned by that Job, so "the Job is gone" IS "the pages are gone", and
    /// every arm that writes a status has to know it — not only the idle one.
    job_present: bool,
}

impl Pass<'_> {
    fn owner(&self) -> RunnerOwner {
        RunnerOwner {
            api_version: RecoveryCatalog::api_version(&()).to_string(),
            kind: RecoveryCatalog::kind(&()).to_string(),
            name: self.name.clone(),
            uid: self.uid.clone(),
        }
    }

    fn generation(&self) -> i64 {
        self.catalog.metadata.generation.unwrap_or(0)
    }

    fn interval(&self) -> i32 {
        self.catalog.spec.sync.interval_seconds
    }

    fn status_value(&self) -> Option<Value> {
        self.catalog
            .status
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok())
    }

    async fn run(&mut self) -> Result<Outcome, ReconcileError> {
        // 1. The tracked Job, BEFORE every other decision — including the
        //    refusal arms, which must also know whether the published view's
        //    Job is still there (review finding F2).
        let tracked = self
            .catalog
            .status
            .as_ref()
            .and_then(|s| s.last_sync_job.as_ref())
            .and_then(|j| j.name.clone());
        let tracked_job = match tracked.as_ref() {
            Some(job_name) => {
                let jobs: Api<Job> = Api::namespaced(self.ctx.client.clone(), &self.namespace);
                jobs.get_opt(job_name).await?
            }
            None => None,
        };
        self.job_present = tracked_job.is_some();

        // 2. `spec.legacyArchive` is a named absence, not a Job.
        if self.catalog.spec.destination_ref.is_none() {
            return self
                .publish_refusal(REASON_LEGACY_ARCHIVE_UNSUPPORTED, LEGACY_ARCHIVE_MESSAGE)
                .await;
        }

        if let Some(job) = tracked_job.as_ref() {
            if !owned_by(job, &self.uid) {
                return self
                    .publish_refusal(
                        REASON_JOB_NAME_CONFLICT,
                        &format!(
                            "Job {}/{} carries this catalog's sync name and is controlled by \
                             something else; a name is not an identity and this pass reads \
                             nothing from it",
                            self.namespace,
                            job.name_any()
                        ),
                    )
                    .await;
            }
            if !crate::controllers::backup::job_finished(job) {
                return self.report_running(job).await;
            }
            let harvested = self
                .catalog
                .status
                .as_ref()
                .and_then(|s| s.last_sync_job.as_ref())
                .is_some_and(|record| harvested_record(record, job));
            if !harvested {
                return self.harvest(job).await;
            }
        }

        // 3. The trigger.
        let Some(trigger) = self.trigger() else {
            return self.report_idle().await;
        };
        let stem = view::sync_stem(&self.uid, &trigger.token());
        if Some(&stem) == tracked.as_ref() {
            // The trigger this pass computed is the one already carried out.
            return self.report_idle().await;
        }
        self.start(&trigger, &stem).await
    }

    /// What, if anything, should run now.
    ///
    /// `syncRequest` FIRST, because an operator asking for a sync now should not
    /// wait for the interval; the interval second; and neither when
    /// `intervalSeconds` is 0 and the request has already been acted on, which
    /// is what "manual only" means.
    fn trigger(&self) -> Option<SyncTrigger> {
        let observed = self
            .catalog
            .status
            .as_ref()
            .and_then(|s| s.observed_sync_request.as_ref());
        if let Some(token) = self
            .catalog
            .spec
            .sync_request
            .as_ref()
            .filter(|t| !t.trim().is_empty())
        {
            if Some(token) != observed {
                return Some(SyncTrigger::Requested(token.clone()));
            }
        }
        let slot = view::periodic_slot(self.ctx.now, self.interval())?;
        if self.slot_already_served(slot) {
            return None;
        }
        Some(SyncTrigger::Periodic(slot))
    }

    /// Whether a sync has ALREADY FINISHED inside this interval slot — in which
    /// case the slot is served and nothing is due.
    ///
    /// THE SECOND HALF OF CATALOG-RESYNC-NOT-HARVESTED. Without this, a
    /// `syncRequest` that completed at 11:55 was followed one reconcile later
    /// by the 12:00 slot's own sync, because the slot's Job name is not the
    /// request's Job name and the tracked-name comparison therefore matched
    /// nothing. That second Job wrote `Synced=Unknown/PodNotStarted` over the
    /// `Synced=True/Succeeded` the publish had just written — which is why the
    /// live harness could not use `Synced` as a completion signal and watched
    /// `status.syncedAt` instead. `intervalSeconds` is a CADENCE, not an
    /// alarm clock: a walk that finished inside the slot is the walk the slot
    /// asked for, whatever started it.
    ///
    /// `lastSyncJob.finishedAt` and not `syncedAt`, deliberately: a sync that
    /// ran and FAILED has also spent the slot's budget against the same
    /// archive, and re-running it every reconcile until the slot rolls over is
    /// the retry storm this controller's requeue is designed to avoid. The
    /// failure is on `Synced` for an operator to act on.
    fn slot_already_served(&self, slot: i64) -> bool {
        self.catalog
            .status
            .as_ref()
            .and_then(|s| s.last_sync_job.as_ref())
            .and_then(|j| j.finished_at)
            .and_then(|at| view::periodic_slot(at, self.interval()))
            .is_some_and(|served| served >= slot)
    }

    // -----------------------------------------------------------------------
    // Starting a sync
    // -----------------------------------------------------------------------

    async fn start(
        &mut self,
        trigger: &SyncTrigger,
        stem: &str,
    ) -> Result<Outcome, ReconcileError> {
        let reference = self
            .catalog
            .spec
            .destination_ref
            .as_ref()
            .expect("destination_ref is present: the legacyArchive arm returned above");
        let resolved = match destination::resolve_ref(
            self.ctx.client,
            &self.namespace,
            &reference.name,
            // ArchiveRead: this sync LISTS and READS and writes nothing. A
            // destination that declares no `archiveRead` grant falls back to
            // `archiveWrite` (D2 §3.4's defaulting), so an operator who wants a
            // genuinely read-only credential configures `spec.access.archiveRead`
            // — which is what D3 §5.3's "a read-only credential" asks for and
            // what this controller cannot enforce from here.
            DestinationRole::ArchiveRead,
            self.ctx.policy,
        )
        .await
        {
            Ok(resolved) => resolved,
            Err(ResolveError::Api(e)) => return Err(ReconcileError::Api(e)),
            Err(ResolveError::Refused(refusal)) => {
                return self
                    .publish_refusal(REASON_DESTINATION_UNUSABLE, &refusal.to_string())
                    .await;
            }
        };

        // The Job runs where the destination's references resolve, and nowhere
        // else (D2 §3.4). A catalog in another namespace would project whatever
        // objects happen to carry those names there.
        if let Err(refusal) = resolved.check_job_namespace(&self.namespace) {
            return self
                .publish_refusal(REASON_DESTINATION_UNUSABLE, &refusal.to_string())
                .await;
        }

        // The trust bundle, when this installation holds key material.
        let trust_config_map = self.ensure_trust_bundle().await?;

        let cursor = self.catalog.status.as_ref().and_then(|s| s.cursor.as_ref());
        let request = view::CatalogSyncRequest {
            destination: logweir_core::check_contract::DestinationPlan {
                name: resolved.name.clone(),
                uid: resolved.uid.clone(),
                location: resolved.location.clone(),
                location_digest: resolved.location_digest.clone(),
                ca_file: resolved.ca_bundle.as_ref().map(|_| {
                    format!(
                        "{}/{}",
                        check::job::CHECK_MOUNT_PATH,
                        check::plan::ARCHIVE_CA_KEY
                    )
                }),
                credentials: credential_mode(&resolved.grant),
            },
            mode: self.catalog.spec.sync.mode.into(),
            deep_check: self.catalog.spec.sync.deep_check.into(),
            max_objects_per_run: i64::from(self.catalog.spec.sync.max_objects_per_run),
            view_limit: i64::from(self.catalog.spec.sync.view_limit),
            index_shard: cursor.and_then(|c| c.index_shard.clone()),
            rescan_start_after: cursor.and_then(|c| c.rescan_start_after.clone()),
            trust_bundle_file: trust_config_map
                .as_ref()
                .map(|_| format!("{}/{}", view::TRUST_MOUNT_PATH, view::TRUST_BUNDLE_KEY)),
        };
        let plan_bytes = match view::plan_document(
            &self.uid,
            u32::try_from(SYNC_TIMEOUT_SECONDS).unwrap_or(u32::MAX),
            Some(&self.ctx.policy.digest()),
            &request,
        ) {
            Ok(bytes) => bytes,
            Err(e) => {
                warn!(catalog = %self.name, namespace = %self.namespace, error = %e,
                      "the catalog sync plan did not serialise; nothing is created");
                return self
                    .publish_refusal(
                        REASON_DESTINATION_UNUSABLE,
                        "the catalog sync plan could not be rendered from the resolved \
                         destination",
                    )
                    .await;
            }
        };
        let plan_digest = logweir_core::ids::sha256_prefixed(&plan_bytes);

        let plan_name = check::plan::plan_config_map_name(stem);
        let documents = check::plan::PlanDocuments {
            check_plan: plan_bytes,
            archive_ca: resolved.ca_pem.clone(),
            ..check::plan::PlanDocuments::default()
        };

        // ==================================================================
        // THE JOB IS CREATED FIRST, AND THE PLAN SECOND — review finding F1
        // ==================================================================
        //
        // The plan `ConfigMap` must be owned by the **sync Job**, because the
        // Job's `ttlSecondsAfterFinished` is the only thing in this design that
        // ever removes anything. Owned by the CATALOG — which is long-lived and
        // whose plan name changes every slot — each plan would be an orphan
        // nothing collects: one per sync, forever, with no `delete` verb
        // anywhere and no remedy short of deleting the `RecoveryCatalog`.
        //
        // An owner reference needs the owner's UID, and a Job's UID exists only
        // after the API server has created it. So the order inverts, and the
        // consequence is stated rather than hidden: **there is a window in
        // which the Job exists and its plan does not**, and a pod scheduled
        // inside it sits `ContainerCreating` on the missing volume. The kubelet
        // retries that mount indefinitely, so the ordinary case resolves in
        // milliseconds; a controller that crashed between the two calls leaves
        // a pod pending until the Job's `activeDeadlineSeconds` fires.
        //
        // WHICH IS WHY NOTHING IS RECORDED UNTIL BOTH EXIST. `lastSyncJob.name`
        // is written only after the plan is in place, so a pass that died in
        // the window is reached again by the SAME trigger token, computes the
        // SAME name, gets 409 `AlreadyExists` on the Job, reads its UID back
        // and creates the plan the pod is waiting for.
        let env = resolved.job_env();
        let spec = view::SyncJobSpec {
            name: stem.to_string(),
            namespace: self.namespace.clone(),
            owner: self.owner(),
            plan_config_map: plan_name,
            plan_sha256: plan_digest.clone(),
            subject_uid: self.uid.clone(),
            timeout_seconds: SYNC_TIMEOUT_SECONDS,
            ttl_seconds: view::ttl_seconds(self.interval()),
            service_account_name: env
                .service_account_name
                .clone()
                .unwrap_or_else(|| crate::controllers::backup::RUNNER_SERVICE_ACCOUNT.to_string()),
            trust_config_map,
            env_literal: env.literals.clone(),
            env_from_secret: env.from_secret.clone(),
            image: self.ctx.runner_image.image.clone(),
            image_pull_policy: self.ctx.runner_image.image_pull_policy.clone(),
        };
        let jobs: Api<Job> = Api::namespaced(self.ctx.client.clone(), &self.namespace);
        let job = match jobs
            .create(&PostParams::default(), &view::build_sync_job(&spec))
            .await
        {
            Ok(job) => job,
            // A 409 IS THE DUPLICATE RECONCILE WORKING. The name is a pure
            // function of the catalog UID and the trigger token, so a second
            // pass computing the same name finds the Job it wanted — and reads
            // it back, because the plan below needs its UID.
            Err(kube::Error::Api(e)) if e.code == 409 => {
                debug!(catalog = %self.name, namespace = %self.namespace, job = %stem,
                       "the sync Job this pass wanted already exists");
                let Some(existing) = jobs.get_opt(stem).await? else {
                    // Created and gone between the two calls; the next pass
                    // creates it again.
                    return Err(ReconcileError::Api(kube::Error::Api(e)));
                };
                existing
            }
            Err(e) => return Err(ReconcileError::Api(e)),
        };
        if !owned_by(&job, &self.uid) {
            return self
                .publish_refusal(
                    REASON_JOB_NAME_CONFLICT,
                    &format!(
                        "Job {}/{stem} carries this sync's name and is controlled by something \
                         else; no plan is written for it and nothing is read from it",
                        self.namespace
                    ),
                )
                .await;
        }
        let job_owner = RunnerOwner {
            api_version: "batch/v1".to_string(),
            kind: "Job".to_string(),
            name: stem.to_string(),
            uid: job.uid().unwrap_or_default(),
        };

        // `blockOwnerDeletion: false`, never `true`: blocking deletion asks the
        // API server for `update` on `jobs/finalizers` under the
        // `OwnerReferencesPermissionEnforcement` admission plugin, and this
        // ClusterRole grants that on nothing.
        let config_map =
            match check::plan::build_owned(stem, &self.namespace, &job_owner, &documents, false) {
                Ok(cm) => cm,
                Err(e) => {
                    return self
                        .publish_refusal(REASON_PAGE_CONFLICT, &e.to_string())
                        .await;
                }
            };
        match check::plan::ensure(
            self.ctx.client,
            &self.namespace,
            &config_map,
            &job_owner.uid,
            &plan_digest,
        )
        .await
        {
            Ok(_) => {}
            Err(check::plan::EnsureError::Api(e)) => return Err(ReconcileError::Api(e)),
            Err(check::plan::EnsureError::Plan(e)) => {
                return self
                    .publish_refusal(REASON_PAGE_CONFLICT, &e.to_string())
                    .await;
            }
        }

        // ==================================================================
        // `lastSyncJob` IS REPLACED, NEVER MERGED INTO — CATALOG-RESYNC-NOT-HARVESTED
        // ==================================================================
        //
        // The status write is an RFC 7386 MERGE patch, and a merge patch
        // recurses into an object: `{"lastSyncJob": {"name": …}}` over a
        // harvested `{"name": …, "startedAt": …, "finishedAt": …,
        // "exitCode": …}` left the PREVIOUS Job's timestamps on the record of
        // the NEW one. `run` decides whether a finished Job has been read from
        // exactly one fact — `lastSyncJob.finishedAt` is set — so every sync
        // after the first was born already looking harvested and was never
        // read: its Job ran to `Complete`, the view was never republished, and
        // the only way left to refresh a catalog was to delete and re-create
        // it (D3 W14, 19 of 19 sync Jobs `Complete` and none harvested).
        //
        // `null` DELETES A KEY in a merge patch, so naming every other field
        // of `LastSyncJob` here is what makes this ONE record of ONE Job
        // rather than a union of two. A field added to `LastSyncJob` without a
        // line here would resurrect this bug, which is why
        // `the_started_record_names_every_field_of_last_sync_job` reads the
        // struct's own schema and fails until it is added.
        let mut status = json!({
            "observedGeneration": self.generation(),
            "lastSyncJob": {
                "name": stem,
                "startedAt": Value::Null,
                "finishedAt": Value::Null,
                "exitCode": Value::Null,
                "refusalReason": Value::Null,
            },
        });
        // `observedSyncRequest` IS RECORDED AT CREATION, not at harvest. The
        // token records what this controller has ACTED on; recording it only
        // after a result would make a crash mid-sync look like a request that
        // was never served and start a second walk for it.
        if let SyncTrigger::Requested(token) = trigger {
            status["observedSyncRequest"] = Value::String(token.clone());
        }
        // `Ready` IS ABOUT THE VIEW, NOT ABOUT THE SYNC (review finding F6).
        // A catalog with live pages that starts its hourly sync still HAS a
        // usable view, which is D3 §5.3's own definition of `Ready`; hardcoding
        // `Unknown` here made it flap `True -> Unknown -> True` every hour
        // inside the fifteen-second requeue gap, and any alerting that reads
        // `Ready != True` as "no view" blinked with it. `SyncInProgress` lives
        // on `Synced`, where it belongs.
        let ready = self.published_ready();
        let conditions = self.conditions(
            ready,
            (
                "Unknown",
                REASON_SYNC_IN_PROGRESS.to_string(),
                format!("sync Job {stem} was created"),
            ),
        );
        status["conditions"] = Value::Array(conditions);
        self.clear_expired_view(&mut status);
        self.patch_status(status).await?;
        info!(
            catalog = %self.name, namespace = %self.namespace, job = %stem,
            mode = ?self.catalog.spec.sync.mode, "catalog sync started"
        );
        Ok(Outcome {
            phase: CatalogPhase::Started,
            ready: ready.0,
            ready_reason: ready.1,
            synced_reason: REASON_SYNC_IN_PROGRESS.to_string(),
            pages: 0,
            entries: 0,
            truncated: false,
            job_name: Some(stem.to_string()),
        })
    }

    /// The immutable `ConfigMap` carrying this installation's PUBLIC signing
    /// keys, so the sync Job can verify a DSSE signature at all.
    ///
    /// `None` when there is no trust material: the Job then reports
    /// `notAttempted` for every point and `TrustAvailable=False` says why.
    /// Mounting an EMPTY bundle instead would make the runner configure a trust
    /// store with nothing in it, which fails differently and less legibly.
    async fn ensure_trust_bundle(&self) -> Result<Option<String>, ReconcileError> {
        if self.trust.is_empty() {
            return Ok(None);
        }
        let jobs_generation = self.trust_generation();
        let name = view::trust_config_map_name(&self.uid, jobs_generation);
        let cm = view::trust_config_map(&name, &self.namespace, &self.owner(), &self.trust);
        let maps: Api<ConfigMap> = Api::namespaced(self.ctx.client.clone(), &self.namespace);
        match maps.create(&PostParams::default(), &cm).await {
            Ok(_) => Ok(Some(name)),
            // Already rendered for this trust generation. Immutable and owned
            // by this catalog, so it is the same bytes by construction.
            Err(kube::Error::Api(e)) if e.code == 409 => Ok(Some(name)),
            Err(e) => Err(ReconcileError::Api(e)),
        }
    }

    /// The trust source's generation — what makes the bundle's name change when
    /// the keys do.
    ///
    /// The digest of the key ids and nothing else: the roster carries a
    /// `metadata.generation`, but this controller reads its SPEC through
    /// [`super::roster_spec`] and a digest over what was actually projected is
    /// the number that cannot disagree with the bytes.
    fn trust_generation(&self) -> i64 {
        let joined = self
            .trust
            .keys
            .iter()
            .map(|k| k.key_id.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        let digest = logweir_core::ids::sha256_hex(joined.as_bytes());
        i64::from_str_radix(&digest[..12], 16).unwrap_or(0)
    }

    // -----------------------------------------------------------------------
    // Watching a running sync
    // -----------------------------------------------------------------------

    async fn report_running(&mut self, job: &Job) -> Result<Outcome, ReconcileError> {
        let expect = frame_expectations(job, &self.uid);
        let observation = check::observe(
            self.ctx.client,
            &self.namespace,
            job,
            &[],
            &expect,
            self.ctx.now,
        )
        .await?;
        // A TERMINAL WAITING STATE IS CANCELLED EARLY, exactly as every other
        // check is: a pod whose credential Secret does not exist cannot succeed,
        // and its `activeDeadlineSeconds` is fifteen minutes away.
        if observation.cancel_now
            && check::cancel(self.ctx.client, &self.namespace, job, &self.uid).await?
        {
            debug!(catalog = %self.name, namespace = %self.namespace, job = %job.name_any(),
                   reason = %observation.reason, "the sync Job cannot succeed; its deadline was collapsed");
        }
        let ready = self.published_ready();
        let mut status = json!({
            "observedGeneration": self.generation(),
            "conditions": self.conditions(
                ready,
                (
                    "Unknown",
                    observation.reason.as_str().to_string(),
                    observation.message.clone(),
                ),
            ),
        });
        self.clear_expired_view(&mut status);
        self.patch_status(status).await?;
        Ok(Outcome {
            phase: CatalogPhase::Running,
            ready: ready.0,
            ready_reason: ready.1,
            synced_reason: observation.reason.as_str().to_string(),
            pages: 0,
            entries: 0,
            truncated: self.published_truncated(),
            job_name: Some(job.name_any()),
        })
    }

    // -----------------------------------------------------------------------
    // Harvesting a finished sync
    // -----------------------------------------------------------------------

    async fn harvest(&mut self, job: &Job) -> Result<Outcome, ReconcileError> {
        let job_name = job.name_any();
        let expect = frame_expectations(job, &self.uid);
        let observation = check::observe(
            self.ctx.client,
            &self.namespace,
            job,
            &[],
            &expect,
            self.ctx.now,
        )
        .await?;
        let started_at = job
            .status
            .as_ref()
            .and_then(|s| s.start_time.as_ref())
            .map(|t| t.0);
        let finished_at = job
            .status
            .as_ref()
            .and_then(|s| s.completion_time.as_ref())
            .map(|t| t.0)
            .unwrap_or(self.ctx.now);

        // THE HARVEST WRITE REPLACES THE RECORD TOO — review finding L1.
        //
        // Seeded with the same explicit `null`s `start` writes, for the same
        // RFC 7386 reason, and then overwritten by the arms below where a
        // value exists. The path that needs it is the UPGRADE one: a harvest
        // is normally preceded by this build's `start`, which already cleared
        // the record, but a catalog stuck by
        // `CATALOG-RESYNC-NOT-HARVESTED` is harvested with NO such `start` in
        // front of it — so a stale `refusalReason` from the one sync that did
        // refuse would be published beside `exitCode: 0` and
        // `Synced=True/Succeeded`, describing two different Jobs as one.
        let last_job = |reason: Option<&CheckCode>| {
            let mut value = json!({
                "name": job_name,
                "finishedAt": Time::from(finished_at),
                "startedAt": Value::Null,
                "exitCode": Value::Null,
                "refusalReason": Value::Null,
            });
            if let Some(at) = started_at {
                value["startedAt"] = json!(Time::from(at));
            }
            if let Some(code) = observation.exit_code {
                value["exitCode"] = json!(code);
            }
            if let Some(code) = reason {
                value["refusalReason"] = json!(code.as_str());
            }
            value
        };

        if observation.phase != CheckPhase::Succeeded {
            // THE PREVIOUS VIEW IS RETAINED. A merge patch that omits `pages`
            // leaves them, and they are still owned by the Job that wrote them,
            // so they age out on their own TTL. A failed sync must not blank a
            // view that is still true.
            let reason = observation.reason;
            let mut status = json!({
                "observedGeneration": self.generation(),
                "lastSyncJob": last_job(Some(&reason)),
                "conditions": self.conditions(
                    self.published_ready(),
                    ("False", reason.as_str().to_string(), observation.message.clone()),
                ),
            });
            self.clear_expired_view(&mut status);
            self.patch_status(status).await?;
            warn!(
                catalog = %self.name, namespace = %self.namespace, job = %job_name,
                reason = %reason, "catalog sync did not produce a readable result"
            );
            let ready = self.published_ready();
            return Ok(Outcome {
                phase: CatalogPhase::Failed,
                ready: ready.0,
                ready_reason: ready.1,
                synced_reason: reason.as_str().to_string(),
                pages: 0,
                entries: 0,
                truncated: self.published_truncated(),
                job_name: Some(job_name),
            });
        }

        let relay = observation
            .relay
            .as_ref()
            .expect("a Succeeded observation carries a verified relay");
        let body_bytes = relay.stream(Stream::Details).unwrap_or_default();
        let body = match std::str::from_utf8(body_bytes) {
            Ok(text) => text,
            Err(_) => {
                return self
                    .publish_sync_failure(
                        &job_name,
                        last_job(None),
                        view::BodyError::NotUtf8.code(),
                        &view::BodyError::NotUtf8.to_string(),
                    )
                    .await;
            }
        };
        let limits = ViewLimits::from_settings(&self.catalog.spec.sync);
        // The entry cap is this catalog's OWN `viewLimit`, not a constant: the
        // runner is told to relay the newest `viewLimit` points and nothing
        // more, so a body that carries more is a body that did not honour the
        // plan it was given (review finding F7).
        let parsed = match view::parse_body(body, limits.view_limit) {
            Ok(parsed) => parsed,
            Err(e) => {
                return self
                    .publish_sync_failure(&job_name, last_job(None), e.code(), &e.to_string())
                    .await;
            }
        };

        let counts = parsed.counts.clone().unwrap_or_default();
        // THE FULL SIGNER LIST FEEDS THE COUNTERS, the bounded one feeds the
        // status (review finding F10): `untrustedSigner` covers the whole walk,
        // and summing it over the sixteen rows the status can display would
        // under-report an archive written by more installations than that.
        let all_signers = view::signer_summaries(&parsed.signers, &self.trust);
        let signers = view::bounded(all_signers.clone());
        let materialised = view::materialise(
            parsed.entries(),
            counts.total,
            &self.trust,
            &limits,
            self.ctx.now,
        );
        let tally = view::tally(&counts, &all_signers, materialised.window);

        // The pages, owned by the Job so the TTL collects them.
        let job_owner = RunnerOwner {
            api_version: "batch/v1".to_string(),
            kind: "Job".to_string(),
            name: job_name.clone(),
            uid: job.uid().unwrap_or_default(),
        };
        let mut page_names = Vec::with_capacity(materialised.pages.len());
        for (index, draft) in materialised.pages.iter().enumerate() {
            let page_name = view::page_config_map_name(&job_name, index);
            let cm = view::page_config_map(
                &page_name,
                &self.namespace,
                &job_owner,
                &self.uid,
                draft,
                index,
            );
            if let Err(conflict) = self
                .put_immutable(&cm, &job_owner.uid, &draft.sha256)
                .await?
            {
                return self
                    .publish_refusal(REASON_PAGE_CONFLICT, &conflict.to_string())
                    .await;
            }
            page_names.push(page_name);
        }

        let document = view::index_document(&job_name, &limits, &materialised, &page_names);
        let index_name = view::index_config_map_name(&job_name);
        let index_cm = match view::index_config_map(
            &index_name,
            &self.namespace,
            &job_owner,
            &self.uid,
            &document,
        ) {
            Ok(cm) => cm,
            Err(e) => {
                return self
                    .publish_sync_failure(
                        &job_name,
                        last_job(None),
                        CheckCode::ResultUnreadable,
                        &format!("the fence-pointer document did not serialise: {e}"),
                    )
                    .await;
            }
        };
        let index_digest = index_cm
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(view::PAGE_DIGEST_ANNOTATION))
            .cloned()
            .unwrap_or_default();
        if let Err(conflict) = self
            .put_immutable(&index_cm, &job_owner.uid, &index_digest)
            .await?
        {
            return self
                .publish_refusal(REASON_PAGE_CONFLICT, &conflict.to_string())
                .await;
        }

        let cursor = parsed.cursor.clone().unwrap_or_default();
        let complete = cursor.complete;
        let unreadable = counts.unreadable > 0;
        let (synced_status, synced_reason, synced_message) = if unreadable {
            (
                "False",
                REASON_PARTIAL_SCAN.to_string(),
                format!(
                    "{} of {} points could not be read — a permission or transport failure, which \
                     is NOT the same as absent; those entries say Unreadable and never Missing",
                    counts.unreadable, counts.total
                ),
            )
        } else if !complete {
            (
                "False",
                REASON_SCAN_INCOMPLETE.to_string(),
                "the object budget ran out before the walk finished; the cursor is recorded and \
                 the next sync continues from it"
                    .to_string(),
            )
        } else {
            (
                "True",
                REASON_SUCCEEDED.to_string(),
                sync_message(&materialised, &parsed, &tally),
            )
        };

        let expires_at = view::view_expires_at(finished_at, self.interval());
        let mut status = json!({
            "observedGeneration": self.generation(),
            "syncedAt": Time::from(finished_at),
            "viewExpiresAt": Time::from(expires_at),
            "cursor": SyncCursor {
                index_shard: cursor.index_shard.clone(),
                rescan_start_after: cursor.rescan_start_after.clone(),
                complete: Some(complete),
            },
            "counts": tally.counts,
            "truncated": materialised.truncated,
            "histogram": view::histogram(&counts),
            "signers": signers,
            "pages": materialised
                .pages
                .iter()
                .enumerate()
                .map(|(i, p)| {
                    let mut numbered = p.clone();
                    numbered.index = i;
                    numbered.status_row(&page_names[i])
                })
                .collect::<Vec<_>>(),
            "indexConfigMap": index_name,
            "lastSyncJob": last_job(None),
        });
        status["conditions"] = Value::Array(self.conditions(
            (
                "True",
                REASON_VIEW_READY,
                "the bounded view is published and has not aged out",
            ),
            (synced_status, synced_reason.clone(), synced_message),
        ));
        self.patch_status(status).await?;
        info!(
            catalog = %self.name, namespace = %self.namespace, job = %job_name,
            pages = materialised.pages.len(), entries = materialised.entries,
            total = counts.total, truncated = materialised.truncated,
            "catalog view published"
        );
        Ok(Outcome {
            phase: CatalogPhase::Published,
            ready: "True",
            ready_reason: REASON_VIEW_READY,
            synced_reason,
            pages: materialised.pages.len(),
            entries: materialised.entries,
            truncated: materialised.truncated,
            job_name: Some(job_name),
        })
    }

    /// Create one immutable `ConfigMap`, accepting an identical one this sync
    /// already wrote and **never adopting a foreign-owned object**.
    async fn put_immutable(
        &self,
        cm: &ConfigMap,
        owner_uid: &str,
        digest: &str,
    ) -> Result<Result<(), PageConflict>, ReconcileError> {
        let maps: Api<ConfigMap> = Api::namespaced(self.ctx.client.clone(), &self.namespace);
        match maps.create(&PostParams::default(), cm).await {
            Ok(_) => Ok(Ok(())),
            Err(kube::Error::Api(response)) if response.code == 409 => {
                let Some(existing) = maps.get_opt(&cm.name_any()).await? else {
                    // Created and removed between the two calls; the next pass
                    // creates it.
                    return Err(ReconcileError::Api(kube::Error::Api(response)));
                };
                Ok(view::accepts_existing_page(&existing, owner_uid, digest))
            }
            Err(e) => Err(ReconcileError::Api(e)),
        }
    }

    async fn publish_sync_failure(
        &mut self,
        job_name: &str,
        last_job: Value,
        code: CheckCode,
        message: &str,
    ) -> Result<Outcome, ReconcileError> {
        let ready = self.published_ready();
        let mut status = json!({
            "observedGeneration": self.generation(),
            "lastSyncJob": last_job,
            "conditions": self.conditions(
                ready,
                ("False", code.as_str().to_string(), message.to_string()),
            ),
        });
        self.clear_expired_view(&mut status);
        self.patch_status(status).await?;
        warn!(
            catalog = %self.name, namespace = %self.namespace, job = %job_name,
            reason = %code, "catalog sync result not readable"
        );
        Ok(Outcome {
            phase: CatalogPhase::Failed,
            ready: ready.0,
            ready_reason: ready.1,
            synced_reason: code.as_str().to_string(),
            pages: 0,
            entries: 0,
            truncated: self.published_truncated(),
            job_name: Some(job_name.to_string()),
        })
    }

    // -----------------------------------------------------------------------
    // Idle, expiry and refusals
    // -----------------------------------------------------------------------

    async fn report_idle(&mut self) -> Result<Outcome, ReconcileError> {
        let expired = self.view_gone();
        let ready = self.published_ready();
        let mut status = json!({
            "observedGeneration": self.generation(),
            "conditions": self.conditions(ready, self.published_synced()),
        });
        self.clear_expired_view(&mut status);
        self.patch_status(status).await?;
        Ok(Outcome {
            phase: CatalogPhase::Idle,
            ready: ready.0,
            ready_reason: ready.1,
            synced_reason: self.published_synced().1,
            pages: if expired {
                0
            } else {
                self.catalog
                    .status
                    .as_ref()
                    .and_then(|s| s.pages.as_ref())
                    .map_or(0, Vec::len)
            },
            entries: 0,
            truncated: !expired && self.published_truncated(),
            job_name: None,
        })
    }

    async fn publish_refusal(
        &mut self,
        reason: &'static str,
        message: &str,
    ) -> Result<Outcome, ReconcileError> {
        let mut status = json!({
            "observedGeneration": self.generation(),
            "conditions": self.conditions(
                ("False", reason, message),
                self.published_synced(),
            ),
        });
        // THE REFUSAL PATH CLEARS AN EXPIRED VIEW TOO (review finding F2). A
        // catalog whose destination went invalid after its sync Job aged out
        // kept naming garbage-collected page ConfigMaps forever, with `Ready`'s
        // reason pointing at the destination and nothing at all saying the
        // window was gone.
        self.clear_expired_view(&mut status);
        self.patch_status(status).await?;
        warn!(
            catalog = %self.name, namespace = %self.namespace, reason = reason,
            message = message, "recovery catalog not ready"
        );
        Ok(Outcome {
            phase: CatalogPhase::Refused,
            ready: "False",
            ready_reason: reason,
            synced_reason: self.published_synced().1,
            pages: 0,
            entries: 0,
            truncated: false,
            job_name: None,
        })
    }

    /// Whether this catalog has any published pages at all.
    fn has_pages(&self) -> bool {
        self.catalog
            .status
            .as_ref()
            .and_then(|s| s.pages.as_ref())
            .is_some_and(|p| !p.is_empty())
    }

    /// Whether the published view is GONE — either past its recorded expiry, or
    /// owned by a Job the API server no longer has.
    ///
    /// The second half is the one that matters in practice: the TTL controller
    /// removes the Job and garbage collection takes its pages with it, and that
    /// can happen before `viewExpiresAt` if the clock or the TTL moved.
    fn view_gone(&self) -> bool {
        self.view_expired() || (self.has_pages() && !self.job_present)
    }

    /// Stop listing pages that are gone — **on every path that writes a
    /// status**, review finding F2.
    ///
    /// `null` DELETES THE KEY in an RFC 7386 merge patch. A `status.pages[]`
    /// naming a `ConfigMap` the API server no longer has is a link to a 404,
    /// and a reader cannot tell it from a page it simply has not fetched — the
    /// published contract tells W11 and W12 to read these names and never
    /// compute one, so leaving them is leading them straight into it. It used
    /// to happen only on the idle path; a catalog whose destination went
    /// invalid after its Job aged out kept naming garbage-collected objects
    /// forever.
    fn clear_expired_view(&self, status: &mut Value) {
        if !(self.view_gone() && self.has_pages()) {
            return;
        }
        let Some(map) = status.as_object_mut() else {
            return;
        };
        map.insert("pages".to_string(), Value::Null);
        map.insert("indexConfigMap".to_string(), Value::Null);
        map.insert("truncated".to_string(), Value::Null);
    }

    fn view_expired(&self) -> bool {
        self.catalog
            .status
            .as_ref()
            .and_then(|s| s.view_expires_at)
            .is_some_and(|at| self.ctx.now > at)
    }

    fn published_truncated(&self) -> bool {
        self.catalog
            .status
            .as_ref()
            .and_then(|s| s.truncated)
            .unwrap_or(false)
    }

    /// `Ready` for a pass that publishes no new view: whatever the published
    /// one justifies.
    fn published_ready(&self) -> (&'static str, &'static str, &'static str) {
        let has_pages = self.has_pages();
        if !has_pages {
            return (
                "Unknown",
                REASON_NEVER_SYNCED,
                "no sync has published a view yet; the durable catalog in object storage is \
                 unaffected",
            );
        }
        if self.view_gone() {
            return (
                "False",
                REASON_VIEW_EXPIRED,
                "the view's pages aged out with their sync Job's TTL. The archive is untouched: \
                 the durable catalog is still in object storage and the next sync republishes \
                 the window.",
            );
        }
        (
            "True",
            REASON_VIEW_READY,
            "the bounded view is published and has not aged out",
        )
    }

    /// `Synced` for a pass that runs no sync: whatever the last one said.
    fn published_synced(&self) -> (&'static str, String, String) {
        let existing = current_condition(
            self.catalog
                .status
                .as_ref()
                .and_then(|s| s.conditions.as_ref()),
            CONDITION_SYNCED,
        );
        match existing {
            Some(c) => (
                match c.status.as_str() {
                    "True" => "True",
                    "False" => "False",
                    _ => "Unknown",
                },
                c.reason
                    .clone()
                    .unwrap_or_else(|| REASON_NEVER_SYNCED.to_string()),
                c.message.clone().unwrap_or_default(),
            ),
            None => (
                "Unknown",
                REASON_NEVER_SYNCED.to_string(),
                "no sync has run".to_string(),
            ),
        }
    }

    /// The four conditions, in one place so no arm can forget `Stale` or
    /// `TrustAvailable`.
    fn conditions(&self, ready: (&str, &str, &str), synced: (&str, String, String)) -> Vec<Value> {
        let existing = self
            .catalog
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref());
        let generation = self.generation();
        let synced_at = self.catalog.status.as_ref().and_then(|s| s.synced_at);
        let stale = view::is_stale(self.ctx.now, synced_at, self.interval());
        let rows = [
            Condition {
                r#type: CONDITION_READY.to_string(),
                status: ready.0.to_string(),
                observed_generation: Some(generation),
                last_transition_time: Some(self.ctx.now),
                reason: Some(ready.1.to_string()),
                message: Some(ready.2.to_string()),
            },
            Condition {
                r#type: CONDITION_SYNCED.to_string(),
                status: synced.0.to_string(),
                observed_generation: Some(generation),
                last_transition_time: Some(self.ctx.now),
                reason: Some(synced.1),
                message: Some(synced.2),
            },
            Condition {
                r#type: CONDITION_STALE.to_string(),
                status: if stale { "True" } else { "False" }.to_string(),
                observed_generation: Some(generation),
                last_transition_time: Some(self.ctx.now),
                reason: Some(
                    if stale {
                        REASON_VIEW_STALE
                    } else {
                        REASON_VIEW_FRESH
                    }
                    .to_string(),
                ),
                message: Some(if stale {
                    format!(
                        "the view is older than two sync intervals ({}s); what it lists may no \
                         longer be what the archive holds",
                        self.interval()
                    )
                } else {
                    "the view is within two sync intervals of its last refresh".to_string()
                }),
            },
            Condition {
                r#type: CONDITION_TRUST_AVAILABLE.to_string(),
                status: if self.trust.is_empty() {
                    "False"
                } else {
                    "True"
                }
                .to_string(),
                observed_generation: Some(generation),
                last_transition_time: Some(self.ctx.now),
                reason: Some(
                    if self.trust.is_empty() {
                        REASON_NO_TRUST_MATERIAL
                    } else {
                        REASON_TRUST_MATERIAL_PRESENT
                    }
                    .to_string(),
                ),
                message: Some(if self.trust.is_empty() {
                    "this installation holds no signing key material, so every point is \
                     NotAttempted and nothing here is presented as verified evidence"
                        .to_string()
                } else {
                    format!(
                        "{} signing key(s) from {} are available to verify with",
                        self.trust.keys.len(),
                        self.trust.source
                    )
                }),
            },
        ];
        rows.into_iter()
            .map(|next| {
                let merged = merge_condition(current_condition(existing, &next.r#type), next);
                serde_json::to_value(merged).unwrap_or(Value::Null)
            })
            .collect()
    }

    // -----------------------------------------------------------------------
    // The status write — D-SEAMS S7 in both halves
    // -----------------------------------------------------------------------

    async fn patch_status(&self, status: Value) -> Result<(), ReconcileError> {
        let patch = json!({ "status": status });
        if status_unchanged(self.status_value().as_ref(), &patch) {
            debug!(
                catalog = %self.name, namespace = %self.namespace,
                "the computed status equals the one on the object; no patch is sent"
            );
            return Ok(());
        }
        let Some(resource_version) = self
            .catalog
            .metadata
            .resource_version
            .clone()
            .filter(|v| !v.is_empty())
        else {
            warn!(
                catalog = %self.name, namespace = %self.namespace,
                "RecoveryCatalog carries no metadata.resourceVersion, which a /status \
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
        let api: Api<RecoveryCatalog> = Api::namespaced(self.ctx.client.clone(), &self.namespace);
        match api
            .patch_status(&self.name, &PatchParams::default(), &Patch::Merge(body))
            .await
        {
            Ok(_) => Ok(()),
            // A 409 IS THE PRECONDITION WORKING, not a failure: something wrote
            // this status between the read and the write, so the object in hand
            // is stale and the next reconcile reads the newer one.
            Err(kube::Error::Api(e)) if e.code == 409 => {
                debug!(
                    catalog = %self.name, namespace = %self.namespace,
                    "the status changed under this reconcile (409); the next pass reads it"
                );
                Ok(())
            }
            Err(e) => Err(ReconcileError::Api(e)),
        }
    }
}

// ---------------------------------------------------------------------------
// Small pure helpers
// ---------------------------------------------------------------------------

/// Whether `record` is the record of **this Job's** completion, and therefore
/// whether this Job's result has already been read.
///
/// `finishedAt` is the fact that says "read"; the comparison with the Job's own
/// `completionTime` is what makes it a fact about THIS Job. Two reasons it is
/// not simply `finished_at.is_some()`:
///
/// 1. **Upgrade.** Every catalog stuck by `CATALOG-RESYNC-NOT-HARVESTED` is
///    carrying, right now, a new Job's name beside an older Job's timestamp.
///    The write above stops that being created; this reads past one that
///    already exists, so a stuck catalog harvests its completed Job on the
///    first reconcile after the upgrade instead of waiting for its next slot —
///    and a `intervalSeconds: 0` catalog, which has no next slot, recovers at
///    all.
/// 2. **It is the honest question.** "Has something been written here" is a
///    weaker claim than "this Job's result is what was written", and the
///    difference is exactly the bug.
///
/// A Job that finished with NO `completionTime` — a `Failed` condition carries
/// none — is recorded with the harvesting pass's own clock, so there is nothing
/// to compare against and the tracked name is the identity. Answering `false`
/// there would re-harvest a failed sync on every pass forever.
#[must_use]
pub fn harvested_record(record: &LastSyncJob, job: &Job) -> bool {
    let Some(recorded) = record.finished_at else {
        return false;
    };
    match job.status.as_ref().and_then(|s| s.completion_time.as_ref()) {
        Some(completed) => recorded == completed.0,
        None => true,
    }
}

/// Whether this Job's CONTROLLER owner reference is the catalog — D-SEAMS
/// **S6**, applied to a Job rather than a pod. A Job carrying the right name is
/// not thereby this catalog's Job.
#[must_use]
pub fn owned_by(job: &Job, catalog_uid: &str) -> bool {
    job.meta()
        .owner_references
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|o| o.uid == catalog_uid && o.controller == Some(true))
}

/// The frame expectations for a sync Job, read off the Job's OWN environment.
///
/// `LOGWEIR_CHECK_PLAN_SHA256` is the digest this controller pinned when it
/// created the Job, so the relay is verified against the plan that actually ran
/// rather than against one this pass re-rendered — which would differ the moment
/// the cursor moved.
#[must_use]
pub fn frame_expectations(job: &Job, subject_uid: &str) -> FrameExpectations {
    let plan_sha256 = job
        .spec
        .as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .map(|p| p.containers.as_slice())
        .unwrap_or_default()
        .iter()
        .find(|c| c.name == crate::job::CONTAINER_NAME)
        .and_then(|c| c.env.as_ref())
        .and_then(|env| {
            env.iter()
                .find(|e| e.name == check::job::PLAN_SHA256_ENV)
                .and_then(|e| e.value.clone())
        })
        .unwrap_or_default();
    FrameExpectations {
        plan_sha256,
        subject_uid: subject_uid.to_string(),
    }
}

/// D2's credential mode for a resolved grant.
fn credential_mode(
    grant: &destination::ResolvedGrant,
) -> logweir_core::check_contract::CredentialMode {
    use logweir_core::check_contract::CredentialMode;
    match grant {
        destination::ResolvedGrant::SecretKeys { .. } => CredentialMode::Static,
        destination::ResolvedGrant::WorkloadIdentity { .. } => CredentialMode::WorkloadIdentity,
        // Neither reaches a sync Job: `ControllerIdentity` is the controller's
        // own allowlisted read and `NotConfigured` is read by nobody. The
        // ambient chain is what a Job with no projected credential would use,
        // and naming it here is honest about what such a pod could reach.
        destination::ResolvedGrant::ControllerIdentity
        | destination::ResolvedGrant::NotConfigured => CredentialMode::Ambient,
    }
}

/// The `Synced=True` message: what was published, and what the ten counters
/// could not carry.
fn sync_message(materialised: &view::View, parsed: &view::SyncBody, tally: &view::Tally) -> String {
    let mut message = format!(
        "the walk completed; the newest {} of {} points are materialised across {} page(s)",
        materialised.entries,
        tally.counts.total.unwrap_or(0),
        materialised.pages.len()
    );
    if materialised.truncated {
        message.push_str(
            ". The view is a WINDOW: points beyond it are counted and histogrammed here and \
             listed by `logweir catalog list` against the archive",
        );
    }
    if parsed.skipped_entries > 0 {
        message.push_str(&format!(
            ". {} entr(y/ies) did not parse and were skipped, never counted as available",
            parsed.skipped_entries
        ));
    }
    if materialised.dropped_oversized > 0 {
        message.push_str(&format!(
            ". {} entr(y/ies) do not fit one page and were refused, never counted as available",
            materialised.dropped_oversized
        ));
    }
    for (state, count, scope) in &tally.unrepresented {
        message.push_str(&format!(
            ". {count} point(s) in {scope} are {state}, which this status has no counter for"
        ));
    }
    message
}

// ---------------------------------------------------------------------------
// The kube::runtime entry points
// ---------------------------------------------------------------------------

/// The installation policy, read through one process-wide cache.
///
/// The DECISION is `check::policy::configured_ref`, which is pure and tested;
/// this is only the read, and it is cached for 30 seconds so a reconcile does
/// not `get` the `ConfigMap` on every pass. A test drives
/// [`reconcile_catalog`] with a `Policy` of its own and never reaches this.
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
    catalog: Arc<RecoveryCatalog>,
    ctx: Arc<Context>,
) -> Result<Action, ReconcileError> {
    let policy = installation_policy(&ctx.client).await;
    let outcome = reconcile_catalog(
        &catalog,
        &SyncContext {
            client: &ctx.client,
            policy: &policy,
            runner_image: &ctx.runner_image,
            now: Utc::now(),
        },
    )
    .await?;
    Ok(Action::requeue(std::time::Duration::from_secs(
        match outcome.phase {
            CatalogPhase::Started | CatalogPhase::Running => RUNNING_REQUEUE_SECONDS,
            _ => IDLE_REQUEUE_SECONDS,
        },
    )))
}

fn error_policy(catalog: Arc<RecoveryCatalog>, err: &ReconcileError, _ctx: Arc<Context>) -> Action {
    warn!(
        catalog = %catalog.name_any(),
        error = %err,
        "recovery catalog reconcile failed; requeueing"
    );
    Action::requeue(std::time::Duration::from_secs(ERROR_REQUEUE_SECONDS))
}

/// Run the `RecoveryCatalog` controller until the process ends.
///
/// `.owns(jobs, …)` so a sync Job finishing wakes the catalog that owns it
/// rather than waiting out the requeue — the same arrangement the `Backup`,
/// `Restore` and `KafkaCluster` controllers have, and it needs no verb this
/// role does not already grant.
pub fn controller(
    client: kube::Client,
    runner_image: crate::job::RunnerImage,
) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<RecoveryCatalog> = Api::all(client.clone());
    let jobs: Api<Job> = Api::all(client.clone());
    let ctx = Arc::new(Context {
        client,
        // NO ARCHIVE HANDLE. The controller reads no archive: the sync Job
        // does, with the destination's own credential, and the controller reads
        // the Job's stdout. That is what keeps every tenant's object-store
        // credential out of this process (D2 §3.8 option B, rejected).
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

/// The `BTreeMap` type alias `serde_json` needs for a `ConfigMap`'s `data` in a
/// test fixture, re-exported so a test does not import `std` twice.
pub type ConfigMapData = BTreeMap<String, String>;
