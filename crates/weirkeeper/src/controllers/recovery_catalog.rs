//! The `RecoveryCatalog` reconciler: run a bounded sync, and publish a bounded
//! view of what can actually be recovered (D3 §5.3).
//!
//! It resolves the catalog's destination, renders an immutable `catalogSync`
//! check plan, runs it as one short-lived Job through the shared check
//! framework, finds that Job's pod by controller-owner UID (seam **S6**), reads
//! the framed stdout, re-evaluates every point's signer against this
//! installation's trust, materialises the newest ≤ 5 000 points into immutable
//! `ConfigMap` pages owned by the sync Job, and writes the counts, the
//! histogram, the signer summary and the fence pointer onto `/status` with a
//! conditional merge PATCH (seam **S7**).
//!
//! # It deletes nothing
//!
//! This reconciler calls no delete. The view is collected by the sync Job's
//! `ttlSecondsAfterFinished` ([`crate::catalog_view::ttl_seconds`]): when the
//! TTL controller removes the Job, garbage collection removes its pages. A
//! catalog whose syncing stopped reports `Stale` and then `ViewExpired` rather
//! than an empty archive, and the durable truth in object storage is untouched.
//!
//! # It is never a second source of truth
//!
//! The view is a derived, bounded projection (D3 §17): the signed receipt is the
//! verification root, the counts describe the whole walk while the pages carry
//! only the window, and every restore re-verifies its point at execution. It is
//! an index onto evidence, not evidence.
//!
//! # The Job decides availability; this file decides trust
//!
//! The Job reads the archive, so it decides availability and whether a DSSE
//! signature verified. Trust is decided here, once, in
//! [`crate::catalog_view::classify_verification`], against the namespace's
//! [`crate::catalog_view::TrustView`] resolved by [`crate::trust::resolve`] as
//! the `Approval` controller and the restore preflight resolve it. A
//! `TrustPolicy` event wakes every catalog in the namespaces it could govern, so
//! a new, retired or revoked key reaches `TrustAvailable` and the next sync
//! without waiting for the requeue.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::{Api, ListParams, Patch, PatchParams, PostParams};
use kube::runtime::controller::Action;
use kube::runtime::reflector::{self, ObjectRef};
use kube::runtime::{watcher, Controller};
use kube::{Resource as _, ResourceExt as _};
use serde_json::{json, Value};
use tracing::{debug, info, warn};

use logweir_core::check_contract::{
    CatalogSyncMode, CheckCode, CheckPlan, CheckRequest, FrameExpectations, Stream,
};
use logweir_core::destination::DestinationRole;

use crate::catalog_view::{self as view, PageConflict, SyncTrigger, TrustView, ViewLimits};
use crate::check::{self, CheckPhase};
use crate::conditions::{current_condition, merge_condition, status_unchanged};
use crate::crds::recovery_catalog::{LastSyncJob, RecoveryCatalog, SyncCursor};
use crate::crds::{Condition, Time};
use crate::destination::{self, ResolveError};
use crate::job::RunnerOwner;

use super::Context;

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
/// `Ready=False`: an older `RecoveryCatalog` in this namespace already catalogs
/// the same `BackupDestination` (D3 §5.5 item 1). The newer one runs no sync and
/// publishes no view; see [`duplicate_of`] for which one is newer.
pub const REASON_DUPLICATE_CATALOG: &str = "DuplicateCatalog";

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
/// `TrustAvailable=False`: two `TrustPolicy` objects claim this namespace, so
/// it resolves to no trust ([`crate::trust::REASON_TRUST_POLICY_CONFLICT`],
/// the reason an `Approval` there is refused with). Every point is
/// `NotAttempted`.
pub const REASON_TRUST_POLICY_CONFLICT: &str = crate::trust::REASON_TRUST_POLICY_CONFLICT;

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
    REASON_DUPLICATE_CATALOG,
    REASON_SUCCEEDED,
    REASON_PARTIAL_SCAN,
    REASON_SCAN_INCOMPLETE,
    REASON_VIEW_STALE,
    REASON_VIEW_FRESH,
    REASON_TRUST_MATERIAL_PRESENT,
    REASON_NO_TRUST_MATERIAL,
    REASON_TRUST_POLICY_CONFLICT,
];

/// What `Ready=False/LegacyArchiveUnsupported` says: an absent capability,
/// named (D3 §5.3's rule). This controller syncs through `spec.destinationRef`
/// only, and says so rather than creating a Job it cannot address.
pub const LEGACY_ARCHIVE_MESSAGE: &str =
    "this build syncs a catalog through spec.destinationRef only; spec.legacyArchive is \
     the connect-an-existing-archive path and no sync Job is created for it. Create a \
     BackupDestination with a read-only archiveRead grant and a RecoveryCatalog that names it.";

/// The requeue while a sync Job is running.
pub const RUNNING_REQUEUE_SECONDS: u64 = 15;
/// The requeue when nothing is running. Short enough that `Stale` and
/// `ViewExpired` appear when they become true, and cheap because an unchanged
/// status sends no PATCH at all.
pub const IDLE_REQUEUE_SECONDS: u64 = 60;
/// The requeue after an error.
pub const ERROR_REQUEUE_SECONDS: u64 = 30;

/// The sync plan's own budget, in seconds: fifteen minutes, against
/// `maxObjectsPerRun`'s ceiling of a million keys. The Job's deadline adds the
/// framework's 90-second start-up margin, so a deadline that fires means the
/// pod never got going, unlike a walk that ran out of budget, which records its
/// cursor and continues.
pub const SYNC_TIMEOUT_SECONDS: i64 = 900;

/// Why a reconcile could not finish. One variant: every refusal is a verdict
/// written to the status, so only the API server can fail a pass; an `Err` for
/// "the destination is not valid" would requeue forever and publish nothing.
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
    /// Every `TrustPolicy`, from the process-wide store once it has synced
    /// (`crate::trust::spawn_policy_reflector`), so the namespace's trust is
    /// resolved with no API call. `None` (not synced, or a caller holds none)
    /// resolves with one cluster-wide `LIST`, because an unsynced store looks
    /// like a cluster with no policy and would hand every namespace to the
    /// roster.
    pub trust_policies: Option<&'a [crate::crds::trust_policy::TrustPolicy]>,
    /// The `RecoveryCatalog`s compared for D3 §5.5's one-catalog-per-destination
    /// rule ([`duplicate_of`]): this controller's own store once its first list
    /// is done, and `None` otherwise, which lists the namespace once. An unsynced
    /// store looks like a namespace with no other catalog and would let a
    /// duplicate sync. Other namespaces and the catalog itself may be included:
    /// [`duplicate_of`] filters them.
    pub peers: Option<&'a [RecoveryCatalog]>,
}

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

    let trust = trust_view(ctx.client, &namespace, ctx.trust_policies).await?;
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

/// The trust bound to the catalog's namespace, projected.
///
/// [`crate::trust::resolve`] is the one resolution: the `Approval` controller
/// verifies against it, the restore preflight re-judges a catalog point's
/// signer against it, and the runner's evidence keyring is rendered from it. A
/// namespace a `TrustPolicy` governs is judged by that policy's
/// `EvidenceSigning` keys and their `Retired`/`Revoked` states; otherwise by the
/// synthesised `legacy-roster-v1`. A failed read is a reconcile error, retried
/// under [`error_policy`]'s fixed delay.
async fn trust_view(
    client: &kube::Client,
    namespace: &str,
    policies: Option<&[crate::crds::trust_policy::TrustPolicy]>,
) -> Result<TrustView, ReconcileError> {
    let resolution = match policies {
        Some(policies) => crate::trust::resolve_with(policies, client, namespace).await?,
        None => crate::trust::resolve(client, namespace).await?,
    };
    Ok(TrustView::from_resolution(&resolution))
}

struct Pass<'a> {
    catalog: &'a RecoveryCatalog,
    name: String,
    namespace: String,
    uid: String,
    trust: TrustView,
    ctx: &'a SyncContext<'a>,
    /// Whether the Job named in `status.lastSyncJob` still exists, set once per
    /// pass before any arm runs: the pages are owned by that Job, so "the Job is
    /// gone" is "the pages are gone", and every arm that writes a status needs
    /// it.
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
        // 1. The tracked Job, before every other decision, including the
        //    refusal arms, which also need to know whether the view's Job is
        //    still there.
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

        // 2b. One catalog per destination per namespace (D3 §5.5 item 1),
        //     before the tracked Job is harvested or a new one started: a
        //     duplicate that was syncing reads nothing from its Job and starts
        //     no other; the Job ages out on its own TTL.
        let listed;
        let peers = match self.ctx.peers {
            Some(peers) => peers,
            None => {
                let api: Api<RecoveryCatalog> =
                    Api::namespaced(self.ctx.client.clone(), &self.namespace);
                listed = api.list(&ListParams::default()).await?.items;
                &listed[..]
            }
        };
        if let Some(elder) = duplicate_of(self.catalog, peers) {
            let message = duplicate_message(&self.namespace, elder);
            return self.publish_duplicate(&message).await;
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

    /// Whether a sync has already finished inside this interval slot, in which
    /// case the slot is served and nothing is due.
    ///
    /// Without it, a `syncRequest` that completed at 11:55 would be followed by
    /// the 12:00 slot's own sync (a different Job name), which would overwrite
    /// `Synced=True/Succeeded` with `Synced=Unknown/PodNotStarted`.
    /// `intervalSeconds` is a cadence, not an alarm clock: a walk that finished
    /// inside the slot is the walk the slot asked for. `lastSyncJob.finishedAt`,
    /// not `syncedAt`: a sync that failed has also spent the slot's budget, and
    /// re-running it every reconcile would be a retry storm; the failure is on
    /// `Synced` for an operator.
    fn slot_already_served(&self, slot: i64) -> bool {
        // A slot served by a sync whose view was then withdrawn is not served:
        // a duplicate that takes over after its elder is deleted has no view,
        // and counting its last sync would leave it viewless until the next slot
        // (or forever at `intervalSeconds: 0`).
        if current_condition(
            self.catalog
                .status
                .as_ref()
                .and_then(|s| s.conditions.as_ref()),
            CONDITION_SYNCED,
        )
        .and_then(|c| c.reason.as_deref())
            == Some(REASON_DUPLICATE_CATALOG)
        {
            return false;
        }
        self.catalog
            .status
            .as_ref()
            .and_then(|s| s.last_sync_job.as_ref())
            .and_then(|j| j.finished_at)
            .and_then(|at| view::periodic_slot(at, self.interval()))
            .is_some_and(|served| served >= slot)
    }

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
            // ArchiveRead: this sync lists and reads and writes nothing. With no
            // `archiveRead` grant the destination falls back to `archiveWrite`
            // (D2 §3.4), so a read-only credential is the operator's to
            // configure in `spec.access.archiveRead`.
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
                grant_bindings: Vec::new(),
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

        // The Job is created first and the plan second. The plan `ConfigMap`
        // must be owned by the sync Job, whose TTL is the only thing that
        // removes anything; owned by the long-lived catalog, every plan would be
        // an orphan. An owner reference needs the Job's UID, which exists only
        // once the Job does, so for a moment the Job exists without its plan and
        // a pod scheduled then waits in `ContainerCreating` (the kubelet retries
        // the mount). `lastSyncJob.name` is written only after both exist, so a
        // pass that died between them computes the same name from the same
        // trigger token, gets 409 on the Job, reads its UID back and creates the
        // plan.
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
            // A 409 is the duplicate reconcile: the name is a pure function of
            // the catalog UID and the trigger token. Read the Job back, because
            // the plan below needs its UID.
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

        // `lastSyncJob` is replaced, never merged into. In an RFC 7386 merge
        // patch `{"lastSyncJob": {"name": …}}` would keep the previous Job's
        // timestamps, and `run` decides whether a finished Job was read from
        // `lastSyncJob.finishedAt` alone, so every later sync would be born
        // looking harvested and never read. `null` deletes a key, so naming
        // every other field of `LastSyncJob` here makes this one record of one
        // Job; `the_started_record_names_every_field_of_last_sync_job` fails
        // until a new field is added here.
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
        // `observedSyncRequest` is recorded at creation, not at harvest: it
        // records what this controller has acted on, so a crash mid-sync does
        // not start a second walk for the same request.
        if let SyncTrigger::Requested(token) = trigger {
            status["observedSyncRequest"] = Value::String(token.clone());
        }
        // `Ready` is about the view, not the sync (D3 §5.3): a catalog with
        // live pages that starts a sync still has a usable view, and
        // `SyncInProgress` lives on `Synced`.
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

    /// The trust source's generation, which renames the bundle when the keys
    /// change: a digest of the key ids, because `metadata.generation` also moves
    /// on edits that do not change the bundle (a retirement, a revocation). A
    /// key's state is not in the bundle; it is applied at classification.
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

    async fn report_running(&mut self, job: &Job) -> Result<Outcome, ReconcileError> {
        let expect = frame_expectations(job, &self.uid);
        // The Job's Events are read while its pod has not started, so a pod
        // refused at creation is `PodCreateRejected` with the admission's words
        // 30 seconds after the Job, and cancelled below.
        let observation = check::observe_reading_events(
            self.ctx.client,
            &self.namespace,
            job,
            &expect,
            self.ctx.now,
        )
        .await?;
        // A terminal waiting state is cancelled early, as for every check: it
        // cannot succeed and its deadline is fifteen minutes away.
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

    async fn harvest(&mut self, job: &Job) -> Result<Outcome, ReconcileError> {
        let job_name = job.name_any();
        let expect = frame_expectations(job, &self.uid);
        // The same read as `report_running`: the Job that pass cancelled
        // finishes `DeadlineExceeded`, and without the Events a harvest would
        // publish that consequence instead of the refusal that caused it.
        let observation = check::observe_reading_events(
            self.ctx.client,
            &self.namespace,
            job,
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

        // The harvest write replaces the record too, seeded with the same
        // explicit `null`s `start` writes and overwritten below where a value
        // exists, so a stale `refusalReason` never sits beside `exitCode: 0` and
        // `Synced=True/Succeeded`, describing two Jobs as one. It matters for a
        // catalog harvested with no preceding `start` of this build.
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
        // The entry cap is this catalog's own `viewLimit`: the runner relays the
        // newest `viewLimit` points, so a longer body did not honour its plan.
        let parsed = match view::parse_body(body, limits.view_limit) {
            Ok(parsed) => parsed,
            Err(e) => {
                return self
                    .publish_sync_failure(&job_name, last_job(None), e.code(), &e.to_string())
                    .await;
            }
        };

        let counts = parsed.counts.clone().unwrap_or_default();
        // The full signer list feeds the counters and the bounded one the
        // status: `untrustedSigner` covers the whole walk, and summing over the
        // sixteen displayed rows would under-report.
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
        // A walk that did not start at the beginning of the archive is a
        // window: a resumed Full rescan lists only the tail.
        let from_start = self.walked_from_start(job).await?;
        let unreadable = counts.unreadable > 0;
        let (synced_status, synced_reason, mut synced_message) = if unreadable {
            (
                "False",
                REASON_PARTIAL_SCAN.to_string(),
                // By cause: a document over its read bound is a size, never a
                // permission or transport failure.
                view::unreadable_message(&counts),
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
        if let Some(note) = partial_view_note(counts.total, complete, from_start) {
            synced_message = format!("{synced_message}. {note}");
        }

        let truncated = materialised.truncated || !from_start;
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
            "truncated": truncated,
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
            truncated,
            job_name: Some(job_name),
        })
    }

    /// Whether the harvested Job's walk began at the start of the archive, read
    /// from the plan it ran under: the plan `ConfigMap` the Job owns, whose
    /// bytes must digest to the `LOGWEIR_CHECK_PLAN_SHA256` the relay was
    /// verified against, and never from the status in hand, which may have
    /// moved since the Job was created. A Full rescan given `rescanStartAfter`
    /// walked only the tail; an Index walk always starts today. Anything this
    /// cannot establish is "no", so the view is published as a window.
    async fn walked_from_start(&self, job: &Job) -> Result<bool, ReconcileError> {
        let pinned = frame_expectations(job, &self.uid).plan_sha256;
        let maps: Api<ConfigMap> = Api::namespaced(self.ctx.client.clone(), &self.namespace);
        let Some(text) = maps
            .get_opt(&check::plan::plan_config_map_name(&job.name_any()))
            .await?
            .and_then(|cm| cm.data)
            .and_then(|mut data| data.remove(check::job::CHECK_PLAN_KEY))
        else {
            return Ok(false);
        };
        if pinned.is_empty() || logweir_core::ids::sha256_prefixed(text.as_bytes()) != pinned {
            return Ok(false);
        }
        Ok(
            match serde_json::from_str::<CheckPlan>(&text).map(|plan| plan.request) {
                Ok(CheckRequest::CatalogSync(request)) => {
                    request.mode == CatalogSyncMode::Index || request.rescan_start_after.is_none()
                }
                _ => false,
            },
        )
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
        // The refusal path clears an expired view too, or a catalog whose
        // destination went invalid after its Job aged out would name collected
        // page ConfigMaps forever.
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

    /// `Ready=False/DuplicateCatalog` (D3 §5.5 item 1) and no view.
    ///
    /// [`Self::publish_refusal`], plus withdrawing a published view even while
    /// its pages exist: the console, the API's point listing and a catalog-point
    /// restore's preflight read `status.pages[]` without reading `Ready`, so a
    /// refused catalog that kept its pages would still be a second, competing
    /// index onto one archive. The pages age out with their sync Job's TTL.
    async fn publish_duplicate(&mut self, message: &str) -> Result<Outcome, ReconcileError> {
        // `Synced` says so too, rather than carry `SyncInProgress` or
        // `Succeeded` beside a withdrawn view.
        let synced = (
            "False",
            REASON_DUPLICATE_CATALOG.to_string(),
            "a duplicate catalog runs no sync; the elder catalog over the same destination \
             syncs it"
                .to_string(),
        );
        let mut status = json!({
            "observedGeneration": self.generation(),
            "conditions": self.conditions(
                ("False", REASON_DUPLICATE_CATALOG, message),
                synced,
            ),
        });
        // `viewExpiresAt` goes with the pages: a `ProtectionPolicy` reads it
        // first, and left behind it would make a withdrawn view read as fresh
        // and empty ("no available point") instead of stale.
        if self.has_pages()
            || self
                .catalog
                .status
                .as_ref()
                .is_some_and(|s| s.view_expires_at.is_some())
        {
            if let Some(map) = status.as_object_mut() {
                map.insert("pages".to_string(), Value::Null);
                map.insert("indexConfigMap".to_string(), Value::Null);
                map.insert("truncated".to_string(), Value::Null);
                map.insert("viewExpiresAt".to_string(), Value::Null);
            }
        }
        self.patch_status(status).await?;
        warn!(
            catalog = %self.name, namespace = %self.namespace,
            reason = REASON_DUPLICATE_CATALOG, message = message,
            "recovery catalog refused: an older catalog indexes the same destination"
        );
        Ok(Outcome {
            phase: CatalogPhase::Refused,
            ready: "False",
            ready_reason: REASON_DUPLICATE_CATALOG,
            synced_reason: REASON_DUPLICATE_CATALOG.to_string(),
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

    /// Stop listing pages that are gone, on every path that writes a status.
    /// `null` deletes the key in an RFC 7386 merge patch; a `status.pages[]`
    /// naming a deleted `ConfigMap` is a link to a 404 that readers, told to
    /// read these names and never compute one, would follow.
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
            trust_available(&self.trust, generation, self.ctx.now),
        ];
        rows.into_iter()
            .map(|next| {
                let merged = merge_condition(current_condition(existing, &next.r#type), next);
                serde_json::to_value(merged).unwrap_or(Value::Null)
            })
            .collect()
    }

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
            // A 409 is the precondition working: the object in hand is stale
            // and the next reconcile reads the newer one.
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

/// The catalog that takes precedence over `catalog` for its destination when
/// `catalog` is a duplicate; `None` when it is the one that syncs.
///
/// D3 §5.5 item 1 refuses the newer of two catalogs for one destination in one
/// namespace. "Newer" is `(creationTimestamp, name)`: the API server's creation
/// time, then the namespace-unique name for the one-second tie, so every pair
/// has exactly one elder and every reconcile reaches the same answer. Existing
/// duplicates keep exactly one syncing catalog, the first created.
///
/// Only a peer in the same namespace with the same `spec.destinationRef.name`
/// is compared (`destinationRef` is immutable). A peer being deleted never
/// holds the destination, so the next in line takes over as the elder goes.
/// The catalog itself is skipped by UID (by name without one). A catalog with
/// no `creationTimestamp` sorts after every one that has one.
#[must_use]
pub fn duplicate_of<'a>(
    catalog: &RecoveryCatalog,
    peers: &'a [RecoveryCatalog],
) -> Option<&'a RecoveryCatalog> {
    let destination = catalog.spec.destination_ref.as_ref()?.name.as_str();
    let namespace = catalog.namespace();
    let own_uid = catalog.uid();
    let own_name = catalog.name_any();
    let key = |c: &RecoveryCatalog| {
        (
            c.metadata.creation_timestamp.as_ref().map(|t| t.0),
            c.name_any(),
        )
    };
    // `None` sorts BEFORE `Some` in `Option`'s `Ord`, which is the opposite
    // of what a missing timestamp should mean here.
    let older = |a: &RecoveryCatalog, b: &RecoveryCatalog| {
        let (at_a, name_a) = key(a);
        let (at_b, name_b) = key(b);
        match (at_a, at_b) {
            (Some(x), Some(y)) if x != y => x < y,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            _ => name_a < name_b,
        }
    };
    peers
        .iter()
        .filter(|p| p.namespace() == namespace)
        .filter(|p| match (own_uid.as_deref(), p.uid()) {
            (Some(own), Some(theirs)) => own != theirs,
            _ => p.name_any() != own_name,
        })
        .filter(|p| p.metadata.deletion_timestamp.is_none())
        .filter(|p| {
            p.spec
                .destination_ref
                .as_ref()
                .is_some_and(|r| r.name == destination)
        })
        .filter(|p| older(p, catalog))
        .reduce(|eldest, p| if older(p, eldest) { p } else { eldest })
}

/// `Ready=False/DuplicateCatalog`'s message: which catalog holds the
/// destination, and the two ways out.
#[must_use]
pub fn duplicate_message(namespace: &str, elder: &RecoveryCatalog) -> String {
    let destination = elder
        .spec
        .destination_ref
        .as_ref()
        .map(|r| r.name.as_str())
        .unwrap_or_default();
    let created = elder
        .metadata
        .creation_timestamp
        .as_ref()
        .map(|t| {
            format!(
                " (created {})",
                t.0.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
            )
        })
        .unwrap_or_default();
    format!(
        "RecoveryCatalog {namespace}/{}{created} already catalogs BackupDestination \
         {namespace}/{destination}, and one destination has one catalog per namespace (D3 §5.5): \
         this newer catalog runs no sync and publishes no view. Use {}, or delete it and this \
         catalog takes over within a minute.",
        elder.name_any(),
        elder.name_any(),
    )
}

/// The `TrustAvailable` condition for this pass's resolved trust. Three answers:
/// keys to verify with (`True`), a namespace two `TrustPolicy` objects contest
/// (`False/TrustPolicyConflict`, naming them), and no key material
/// (`False/NoTrustMaterial`). A conflict is not "no key material": trust is
/// configured, it just does not resolve.
#[must_use]
pub fn trust_available(trust: &TrustView, generation: i64, now: DateTime<Utc>) -> Condition {
    let (status, reason, message) = match (&trust.unresolved, trust.is_empty()) {
        (Some(conflict), _) => ("False", REASON_TRUST_POLICY_CONFLICT, conflict.clone()),
        (None, true) => (
            "False",
            REASON_NO_TRUST_MATERIAL,
            "this installation holds no signing key material, so every point is NotAttempted \
             and nothing here is presented as verified evidence"
                .to_string(),
        ),
        (None, false) => (
            "True",
            REASON_TRUST_MATERIAL_PRESENT,
            format!(
                "{} signing key(s) from {} are available to verify with",
                trust.keys.len(),
                trust.source
            ),
        ),
    };
    Condition {
        r#type: CONDITION_TRUST_AVAILABLE.to_string(),
        status: status.to_string(),
        observed_generation: Some(generation),
        last_transition_time: Some(now),
        reason: Some(reason.to_string()),
        message: Some(message),
    }
}

/// Whether `record` is the record of this Job's completion, and so whether this
/// Job's result has already been read.
///
/// `finishedAt` says "read"; comparing it with the Job's own `completionTime`
/// makes it a fact about this Job rather than "something was written here", so
/// a catalog carrying a new Job's name beside an older Job's timestamp harvests
/// its completed Job. A Job that finished with no `completionTime` (a `Failed`
/// condition carries none) is recorded with the harvesting pass's clock, so the
/// tracked name is the identity; `false` there would re-harvest a failed sync
/// forever.
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

/// What a `Synced` message adds when the published view is not the whole
/// archive's: a `Full` rescan resumed after the previous cursor lists only the
/// tail, and an unfinished walk lists only what it reached. `None` for a whole
/// walk. `status.truncated` and `status.cursor.complete` say it too; this says
/// it in words, so "the newest 136 of 136 points" does not read as the whole
/// archive.
fn partial_view_note(walked: i64, complete: bool, from_start: bool) -> Option<String> {
    let (source, rest) = if !from_start {
        ("a resumed rescan", "the archive holds more")
    } else if !complete {
        ("a walk that did not finish", "the archive may hold more")
    } else {
        return None;
    };
    Some(format!(
        "It is a partial view: {walked} point(s) from {source}; {rest}"
    ))
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
    shared: crate::trust::SharedPolicies,
    objects: reflector::Store<RecoveryCatalog>,
) -> Result<Action, ReconcileError> {
    let policy = installation_policy(&ctx.client).await;
    // THE SHARED STORE, ONCE IT HAS SYNCED — no `LIST trustpolicies` per pass.
    let snapshot: Option<Vec<crate::crds::trust_policy::TrustPolicy>> = shared
        .synced
        .load(std::sync::atomic::Ordering::Relaxed)
        .then(|| shared.store.state().iter().map(|p| (**p).clone()).collect());
    // THIS CONTROLLER'S OWN STORE, once its first list is complete, for the
    // one-catalog-per-destination rule — the same "is it ready yet" the
    // manual-run pool asks (`run_pool::store_snapshot`). Only this namespace's
    // catalogs are cloned; `None` makes the pass `LIST` the namespace instead.
    let namespace = catalog.namespace();
    let peers: Option<Vec<RecoveryCatalog>> =
        crate::run_pool::store_snapshot(&objects).map(|all| {
            all.iter()
                .filter(|c| c.namespace() == namespace)
                .map(|c| (**c).clone())
                .collect()
        });
    let outcome = reconcile_catalog(
        &catalog,
        &SyncContext {
            client: &ctx.client,
            policy: &policy,
            runner_image: &ctx.runner_image,
            now: Utc::now(),
            trust_policies: snapshot.as_deref(),
            peers: peers.as_deref(),
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

/// The `RecoveryCatalog`s a `TrustPolicy` event should enqueue — the re-trust
/// trigger the `Approval`, `Backup` and `Restore` controllers carry.
///
/// A catalog's `TrustAvailable`, its trust bundle and every point its next sync
/// classifies are a function of the namespace's RESOLVED trust, so a policy
/// edit — a new key, a retirement, a revocation — must reach it now, not at the
/// idle requeue. `objects` is this controller's OWN store (zero API calls),
/// and the scope is the UNION of what the policy bound before and after the
/// event, so an edit that NARROWS still wakes the namespace it stopped
/// governing (see [`crate::trust::PolicyScopeMemory`]).
fn policy_targets(
    objects: &reflector::Store<RecoveryCatalog>,
    scopes: &crate::trust::PolicyScopeMemory,
    policy: &crate::crds::trust_policy::TrustPolicy,
) -> Vec<ObjectRef<RecoveryCatalog>> {
    let scope = scopes.observe(policy);
    crate::verification::targets_in_scope(objects.state(), &scope)
}

/// Run the `RecoveryCatalog` controller until the process ends.
///
/// It `owns` its sync Jobs, so a finishing Job wakes its catalog rather than
/// waiting out the requeue. `.watches(TrustPolicy)` is the re-trust trigger
/// ([`policy_targets`]), backed off like every trigger watch
/// (`watcher::default_backoff`), so a failing `TrustPolicy` LIST is retried
/// with bounded delay. Trust resolves from the process-wide `TrustPolicy` store
/// ([`crate::trust::spawn_policy_reflector`]) once it has synced, and with one
/// `LIST` until then.
pub async fn controller(client: kube::Client, runner_image: crate::job::RunnerImage) {
    // One watch per watched namespace: the whole cluster unless
    // `LOGWEIR_WATCH_NAMESPACES` names namespaces, and then an `Api::namespaced`
    // watch each, the only shape the scoped chart's RoleBindings permit. The
    // cluster-scoped `TrustPolicy` store is built once here and shared, not one
    // identical cluster-wide watch per namespace; this is an `async fn` so the
    // reflector is spawned inside the runtime (`tokio::spawn`).
    let policies = crate::trust::spawn_policy_reflector(&client);
    crate::scope::run_everywhere(move |namespace| {
        controller_in(
            client.clone(),
            runner_image.clone(),
            policies.clone(),
            namespace,
        )
    })
    .await;
}

/// One watch of [`controller`], over `namespace` (`None` is the whole
/// cluster).
fn controller_in(
    client: kube::Client,
    runner_image: crate::job::RunnerImage,
    shared: crate::trust::SharedPolicies,
    namespace: Option<String>,
) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<RecoveryCatalog> = crate::scope::api(&client, namespace.as_deref());
    let jobs: Api<Job> = crate::scope::api(&client, namespace.as_deref());
    let ctx = Arc::new(Context {
        client,
        // NO ARCHIVE HANDLE. The controller reads no archive: the sync Job
        // does, with the destination's own credential, and the controller reads
        // the Job's stdout. That is what keeps every tenant's object-store
        // credential out of this process (D2 §3.8 option B, rejected).
        archive: None,
        runner_image,
    });
    let policy_api: Api<crate::crds::trust_policy::TrustPolicy> = Api::all(ctx.client.clone());
    let scopes = Arc::new(crate::trust::PolicyScopeMemory::default());
    async move {
        let controller = Controller::new(api, watcher::Config::default());
        let objects = controller.store();
        // The same store, for the one-catalog-per-destination rule.
        let peers = controller.store();
        controller
            .owns(jobs, watcher::Config::default())
            .watches(policy_api, watcher::Config::default(), move |policy| {
                policy_targets(&objects, &scopes, &policy)
            })
            .run(
                move |object, context| reconcile(object, context, shared.clone(), peers.clone()),
                error_policy,
                ctx,
            )
            .for_each(|_| std::future::ready(()))
            .await;
    }
}

/// The `BTreeMap` type alias `serde_json` needs for a `ConfigMap`'s `data` in a
/// test fixture, re-exported so a test does not import `std` twice.
pub type ConfigMapData = BTreeMap<String, String>;
