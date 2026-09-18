//! The `ProtectionPolicy` reconciler — PLAT-14.2, decision D3 §§3.1–3.5.
//!
//! # The thin half
//!
//! Every verdict this file publishes is computed by [`crate::protection`],
//! which holds no client. This module reads what the API server says, calls
//! [`crate::protection::evaluate`] and
//! [`crate::protection::reconcile_alerts`], creates at most
//! [`crate::protection::MAX_DELIVERY_JOBS_PER_PASS`] delivery Jobs, and
//! patches `protectionpolicies/status`. That is the whole of it.
//!
//! # WHAT IT WRITES TO, AND THE ONE SENTENCE THAT MATTERS
//!
//! **A notification failure never rewrites a backup result.** This reconciler
//! patches `protectionpolicies/status` and nothing else — there is no
//! `Api<Backup>` or `Api<Restore>` status write anywhere in this file, and
//! `notification_failure_never_patches_a_backup` asserts it over a route table
//! that would panic on the attempt. `Api<Backup>` appears here exactly once,
//! for a bounded `list`, because a protection verdict is a projection of runs
//! that already happened and this controller is not the execution authority
//! for any of them.
//!
//! # THE THREE ORDERING RULES
//!
//! 1. **The event `ConfigMap` before its Job** (D3 §3.4 point 3). A Job whose
//!    mounted `ConfigMap` does not exist sits in `ContainerCreating` until its
//!    deadline and reports nothing at all.
//! 2. **The status before the TTL** (seam **S7**, the `Backup` path's own
//!    rule). `ttlSecondsAfterFinished` is patched onto a finished delivery Job
//!    only after the `/status` patch carrying that delivery's verdict returned
//!    200. The exit code lives on the POD, and the TTL controller removes the
//!    Job and its pod together: a TTL set first lets garbage collection race
//!    the read, and the delivery would be recorded as "no exit code" forever.
//! 3. **The pod is proved before it is read** (seam **S6**, defect
//!    `SEC-PODLOG`). [`crate::check::pod::find_owned_pod`] narrows by the
//!    Job's name label and then checks the Job's own `metadata.uid` on the
//!    pod's controller `ownerReference`; a second claimant is refused rather
//!    than ranked. A `notify-result=` line is a document this controller
//!    writes into a custom resource's status, so reading a stranger's is the
//!    whole of that defect.
//!
//! # No credential value is held, read or written
//!
//! `config/rbac/role.yaml` grants this controller no verb on `secrets`, and
//! this file adds none. A sink's routing key or webhook URL reaches the
//! delivery pod as a `valueFrom.secretKeyRef` the kubelet projects from a
//! reference written into a PodSpec — writing a reference is not reading a
//! value. Nothing from a pod log reaches the status except a `'static` string
//! from [`crate::protection::NOTIFY_RESULTS`]'s closed table.

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::StreamExt as _;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ConfigMap, Pod};
use kube::api::{ListParams, ObjectMeta, Patch, PatchParams, PostParams};
use kube::runtime::controller::Action;
use kube::runtime::{watcher, Controller};
use kube::{Api, Resource as _, ResourceExt as _};
use serde_json::{json, Value};
use tracing::{debug, info, warn};

use crate::conditions::{current_condition, merge_condition, status_unchanged};
use crate::crds::backup::Backup;
use crate::crds::backup_destination::BackupDestination;
use crate::crds::backup_schedule::BackupSchedule;
use crate::crds::kafka_cluster::KafkaCluster;
use crate::crds::protection_policy::{
    AlertDelivery, AlertEntry, AlertKind, NotificationRoute, ProtectionPolicy,
    ProtectionPolicyStatus, SecretKeyRef,
};
use crate::crds::recovery_catalog::RecoveryCatalog;
use crate::crds::restore::Restore;
use crate::crds::{Condition, LocalRef, Time};
use crate::identity;
use crate::job::{self, ConfigMapMount, EnvFromSecret, RunnerImage, RunnerJobSpec, RunnerOwner};
use crate::protection as p;

use super::approval::ReconcileError;

// ===========================================================================
// Constants
// ===========================================================================

/// `Ready` — this controller looked at the object and could publish a verdict.
pub const CONDITION_READY: &str = "Ready";
/// `Protected` — D3 §3.2: `True` only for `Healthy`, `Unknown` for `Unknown`.
pub const CONDITION_PROTECTED: &str = "Protected";
/// `NotificationsDelivered`.
pub const CONDITION_NOTIFICATIONS_DELIVERED: &str = "NotificationsDelivered";

/// `Ready`'s reason when a verdict was published.
pub const REASON_EVALUATED: &str = "Evaluated";
/// `Ready`'s reason for an object with no namespace or UID.
pub const REASON_NOT_EVALUATED: &str = "NotEvaluated";
/// `NotificationsDelivered`'s reason when every due alert was delivered.
pub const REASON_DELIVERED: &str = "Delivered";
/// …when a delivery Job exists and has not finished.
pub const REASON_DELIVERY_PENDING: &str = "DeliveryPending";
/// …after [`crate::protection::MAX_DELIVERY_ATTEMPTS`] failed attempts.
///
/// **AND NOTHING ELSE HAPPENS.** D3 §3.4 point 4: exhaustion sets this
/// condition and does not touch a `Backup`, a `Restore`, a phase or a piece of
/// evidence.
pub const REASON_DELIVERY_FAILED: &str = "DeliveryFailed";
/// …when the policy configures no route, or none for this kind. Reported as
/// `True`: nothing failed, and an operator who configured no sink chose that.
pub const REASON_NOTHING_TO_DELIVER: &str = "NothingToDeliver";

/// The ServiceAccount the delivery Job runs as.
///
/// # `logweir-runner` AND NOT A NEW `logweir-notifier` (deviation from D3 §3.4)
///
/// D3 §3.4 names `logweir-notifier`. That account does not exist, and creating
/// it means a new chart template, a new object in `logweir.yaml`, a new
/// `docs/install.md` step-4 fragment for every namespace that runs Jobs and a
/// new `manifest_lint` exemption row — all of them files D3 §14 gives to the
/// wave-4 RBAC/chart/docs worker, not to this one. `logweir-runner` is the
/// account every other runner Job in this product names, it is granted **no
/// verb on anything**, its token is not mounted
/// (`automountServiceAccountToken: false`, twice over — on the account and on
/// the PodSpec [`crate::job::build`] writes), and it carries the
/// `imagePullSecrets` a private registry needs. The security delta is nil and
/// the delta in files touched is four. Recorded so W13 can rename it in one
/// line if the separate identity is wanted for its own sake.
pub const SERVICE_ACCOUNT: &str = "logweir-runner";

/// `spec.activeDeadlineSeconds` on a delivery Job — D3 §3.4 point 3.
///
/// Three sinks at `NOTIFY_TIMEOUT` (10 s) plus connect is thirty seconds in
/// the worst case; 120 leaves room for image pull and scheduling and still
/// bounds a wedged sink.
pub const DELIVERY_DEADLINE_SECONDS: i64 = 120;

/// `ttlSecondsAfterFinished`, patched on only after the status landed.
pub const DELIVERY_TTL_SECONDS: i32 = 3600;

/// The argv the delivery Job runs — D3 §3.4 point 2, verbatim.
///
/// A FUNCTION AND NOT A LITERAL AT THE CALL SITE, so the subcommand's contract
/// and this controller's spelling of it are one thing.
#[must_use]
pub fn delivery_argv() -> Vec<String> {
    vec![
        "notify".to_string(),
        "deliver".to_string(),
        "--event".to_string(),
        format!("{}/{}", p::EVENT_MOUNT_PATH, p::EVENT_DATA_KEY),
    ]
}

/// The volume the event `ConfigMap` is projected as.
pub const EVENT_VOLUME: &str = "event";

/// `PAGERDUTY_ROUTING_KEY` — D3 W4's sink variable.
pub const ROUTING_KEY_ENV: &str = "PAGERDUTY_ROUTING_KEY";
/// `NOTIFY_WEBHOOK_URL`.
pub const WEBHOOK_URL_ENV: &str = "NOTIFY_WEBHOOK_URL";
/// `NOTIFY_SLACK_WEBHOOK_URL`.
pub const SLACK_WEBHOOK_URL_ENV: &str = "NOTIFY_SLACK_WEBHOOK_URL";
/// `PAGERDUTY_ENDPOINT` — a service REGION, not a credential, so it is a
/// literal. Without it an EU-service-region adopter enqueues into a region
/// that does not hold their account and never sees a page.
pub const PAGERDUTY_ENDPOINT_ENV: &str = "PAGERDUTY_ENDPOINT";

/// `NOTIFY_ALLOW_INSECURE_SINKS` — the variable a delivery Job carries when
/// THIS INSTALLATION has turned the escape hatch on, spelt exactly as
/// `logweir::notify`'s `ALLOW_INSECURE_SINKS_ENV` reads it inside that Job.
///
/// It is a literal `1` and it is not a credential: the sink URL stays in the
/// `Secret` the kubelet resolves, so neither the Job's command line nor its
/// env names a URL. Without it `logweir notify deliver` refuses a
/// non-`https://` webhook or Slack URL BEFORE it dials, which is why an
/// in-cluster echo sink on a laptop cluster received zero POSTs
/// (`NOTIFY-INSECURE-SINK-UNEXPOSED`): the hatch was documented and set by
/// nothing.
pub const ALLOW_INSECURE_SINKS_ENV: &str = "NOTIFY_ALLOW_INSECURE_SINKS";

/// The variable THIS PROCESS reads to decide whether the Jobs it creates
/// carry [`ALLOW_INSECURE_SINKS_ENV`]. `charts/logweir` renders it from
/// `notify.allowInsecureSinks`, and nothing else sets it.
///
/// # Why it is an INSTALLATION setting and never a policy field
///
/// A `ProtectionPolicy` is a namespaced object any namespace operator may
/// write. If the hatch lived in its spec, whoever could create a policy could
/// downgrade their own alerts' transport to cleartext — carrying the event,
/// the policy's name, its health and (on Slack) a bearer credential in the
/// URL — and the administrator who installed Logweir would have nowhere to
/// say no. Here the only way to set it is an edit to the controller
/// Deployment, which is the installation's own boundary: the same one
/// [`crate::job::RUNNER_IMAGE_ENV`] and the archive addressing already sit on.
/// The answer is therefore the same for every policy in the cluster, and
/// `a_policy_cannot_turn_the_insecure_sink_hatch_on` is the row that holds it.
///
/// # Why the two names differ
///
/// Every variable this controller reads FOR ITSELF is `LOGWEIR_`-prefixed and
/// every variable it writes into a runner Job is not. They are also different
/// decisions — whether an installation allows the hatch at all, versus what
/// one Job was told — and one name for both would make a `grep` in a cluster
/// unable to tell the controller's setting from a Job's.
pub const CONTROLLER_ALLOW_INSECURE_SINKS_ENV: &str = "LOGWEIR_NOTIFY_ALLOW_INSECURE_SINKS";

/// [`CONTROLLER_ALLOW_INSECURE_SINKS_ENV`]'s value as a decision.
///
/// **AN EXPLICIT AFFIRMATIVE ONLY** — `1`, `true` or `yes`, trimmed and
/// case-insensitive. Everything else is "no": `0`, `false`, an absent
/// variable, and the empty string a Kubernetes `env:` entry with an empty
/// `value:` actually produces (plan erratum **E19(e)**). That is the same
/// reading `logweir::notify::SinkRoutes::from_lookup` gives the Job-side
/// variable and the same one `weirkeeper::retention`'s flag parser gives its
/// own, so an operator who switches the value off does not discover it was
/// still on — and a hatch that defaults open is not a hatch.
///
/// # Why it takes the `Result`
///
/// So a test can hand it `Ok(String::new())` — the exact value the E19(e)
/// defect is about — without mutating process-global state the rest of the
/// binary shares. The predicate is the whole of the decision; [`controller`]
/// supplies the read, exactly once per process.
#[must_use]
pub fn configured_allow_insecure_sinks(raw: Result<String, std::env::VarError>) -> bool {
    raw.is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes"
        )
    })
}

/// How long before an errored reconcile is retried.
pub const ERROR_REQUEUE_SECONDS: u64 = 30;
/// How long before a pass that has an in-flight or deferred delivery looks
/// again. Short, because a page nobody sent is the defect.
pub const REQUEUE_DELIVERY_SECONDS: u64 = 15;

/// One page of the bounded `Backup` listing.
pub const BACKUP_PAGE_LIMIT: u32 = 200;
/// How many pages of that listing one evaluation walks.
///
/// `200 × 5` objects, at most once per `evaluationIntervalSeconds` (60 s
/// floor). The cap is what makes the read bounded; `status.lastAvailablePoint`
/// is a projection of the newest run, not a census.
pub const MAX_BACKUP_PAGES: usize = 5;
/// How many catalog page `ConfigMap`s one evaluation reads — the CRD's
/// `status.pages` `maxItems`.
pub const MAX_CATALOG_PAGES: usize = 8;
/// The `ConfigMap` key a catalog page's entries live under, one compact JSON
/// object per line.
pub const CATALOG_PAGE_DATA_KEY: &str = "entries.jsonl";

// ===========================================================================
// Errors, context and outcome
// ===========================================================================

/// What this reconciler needs.
#[derive(Clone)]
pub struct ProtectionContext {
    /// The client every `Api` here is built from.
    pub client: kube::Client,
    /// The image and pull policy the delivery Jobs name.
    pub runner_image: RunnerImage,
    /// Whether THIS INSTALLATION allows a delivery Job to POST to a
    /// non-`https://` sink — [`CONTROLLER_ALLOW_INSECURE_SINKS_ENV`], read
    /// once by [`controller`] and never from a policy's spec.
    pub allow_insecure_sinks: bool,
}

/// What one pass did — the shape the tests assert over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    /// The health it published.
    pub health: p::Health,
    /// The freshness it published.
    pub freshness: p::Freshness,
    /// Why.
    pub reason: p::FreshnessReason,
    /// How many delivery Jobs it created.
    pub created_jobs: usize,
    /// How many due deliveries it deferred to the next pass.
    pub deferred: usize,
    /// How many finished delivery Jobs it had its TTL patched on.
    pub ttl_patched: usize,
    /// Whether the status patch landed (or was already what this pass
    /// computed).
    pub committed: bool,
    /// Seconds until the next pass.
    pub requeue_seconds: u64,
}

// ===========================================================================
// The reconcile
// ===========================================================================

/// Reconcile one `ProtectionPolicy`.
///
/// # Errors
///
/// [`ReconcileError::Api`] for anything that is not a verdict.
#[allow(clippy::too_many_lines)]
pub async fn reconcile_policy(
    policy: &ProtectionPolicy,
    ctx: &ProtectionContext,
    now: Time,
) -> Result<Outcome, ReconcileError> {
    let name = policy.name_any();
    let Some(namespace) = policy.namespace() else {
        warn!(policy = %name, "ProtectionPolicy carries no metadata.namespace; nothing is done");
        return Ok(idle());
    };
    let Some(uid) = policy.uid() else {
        warn!(
            policy = %name,
            namespace = %namespace,
            "ProtectionPolicy carries no metadata.uid; an alert key built from an empty UID \
             would be one incident per kind across every policy in the cluster, so nothing is \
             done"
        );
        return Ok(idle());
    };
    let api: Api<ProtectionPolicy> = Api::namespaced(ctx.client.clone(), &namespace);
    let spec = &policy.spec;
    let interval = spec.evaluation_interval_seconds;

    // ------------------------------------------------------------------
    // Read.
    // ------------------------------------------------------------------
    let clusters: Api<KafkaCluster> = Api::namespaced(ctx.client.clone(), &namespace);
    let source_exists = clusters
        .get_opt(&spec.protects.source_ref.name)
        .await
        .map_err(ReconcileError::Api)?
        .is_some();

    let destination_exists = match spec.protects.destination_ref.as_ref() {
        None => true,
        Some(reference) => {
            let destinations: Api<BackupDestination> =
                Api::namespaced(ctx.client.clone(), &namespace);
            destinations
                .get_opt(&reference.name)
                .await
                .map_err(ReconcileError::Api)?
                .is_some()
        }
    };

    let schedules_api: Api<BackupSchedule> = Api::namespaced(ctx.client.clone(), &namespace);
    let mut schedule_objects: Vec<(String, Option<BackupSchedule>)> = Vec::new();
    for reference in spec.protects.schedule_refs.as_deref().unwrap_or_default() {
        let found = schedules_api
            .get_opt(&reference.name)
            .await
            .map_err(ReconcileError::Api)?;
        schedule_objects.push((reference.name.clone(), found));
    }
    let schedule_facts: Vec<p::ScheduleFacts> =
        schedule_objects.iter().map(schedule_facts).collect();

    let backups = list_backups(&ctx.client, &namespace).await?;
    let members: Vec<&Backup> = backups
        .iter()
        .filter(|b| is_member(b, spec, &schedule_objects))
        .collect();

    let catalog = read_catalog(&ctx.client, &namespace, spec, now).await?;

    // ------------------------------------------------------------------
    // Project, then evaluate. Both pure.
    // ------------------------------------------------------------------
    let mut candidates: Vec<p::PointCandidate> = members
        .iter()
        .map(|b| candidate_from_backup(b, spec))
        .collect();
    candidates.sort_by_key(|c| std::cmp::Reverse(c.recovery_point_at));
    candidates.truncate(p::MAX_BACKUPS_SCANNED);

    let mut slots: Vec<p::SlotRun> = members.iter().map(|b| slot_run(b)).collect();
    // Newest first. `creationTimestamp` is set by the API server on every
    // object, so it orders a retry chain (attempt n+1 is created after n) and
    // a manual run alike; the name breaks a second-granular tie so the order
    // does not vary between two reconciles over one cluster state.
    slots.sort_by(|a, b| {
        b.at.cmp(&a.at)
            .then_with(|| b.attempt.cmp(&a.attempt))
            .then_with(|| b.backup_name.cmp(&a.backup_name))
    });
    slots.truncate(p::MAX_BACKUPS_SCANNED);

    let rehearsal = p::RehearsalFacts::default();
    let verdict = p::evaluate(&p::Inputs {
        spec,
        source_exists,
        destination_exists,
        candidates: &candidates,
        catalog: &catalog,
        schedules: &schedule_facts,
        slots: &slots,
        rehearsal: &rehearsal,
        now,
    });

    // ------------------------------------------------------------------
    // Observe the deliveries already in flight, THEN move the ledger.
    // ------------------------------------------------------------------
    let stored_alerts: Vec<AlertEntry> = policy
        .status
        .as_ref()
        .and_then(|s| s.alerts.clone())
        .unwrap_or_default();
    let mut observed: Vec<AlertEntry> = Vec::new();
    let mut finished_jobs: Vec<String> = Vec::new();
    for entry in &stored_alerts {
        let (entry, finished) = observe_delivery(&ctx.client, &namespace, entry, now).await?;
        if let Some(job_name) = finished {
            finished_jobs.push(job_name);
        }
        observed.push(entry);
    }

    // D3 §3.3's fifth kind. Read AFTER the deliveries in flight, so a
    // recovery recorded this pass cannot displace a delivery verdict this pass
    // was about to write.
    let backup_ids: Vec<String> = members.iter().filter_map(|b| backup_set_id(b)).collect();
    let completions = read_recoveries(&ctx.client, &namespace, spec, &backup_ids).await?;
    let observed = p::fold_recoveries(&observed, &completions, now);

    let ledger = p::reconcile_alerts(
        &observed,
        &verdict.open_kinds,
        verdict.health,
        &uid,
        spec.notifications.as_ref(),
        now,
    );
    let mut alerts = ledger.alerts;

    // ------------------------------------------------------------------
    // Deliver. The Job first, then the event `ConfigMap` it mounts and OWNS
    // it, then the ledger note that a Job exists for this transition.
    // ------------------------------------------------------------------
    let owner = owner_of(&name, &uid);
    let mut created = 0usize;
    let mut deferred = 0usize;
    for due in &ledger.due {
        let Some(index) = alerts.iter().position(|a| a.key == due.key) else {
            continue;
        };
        let state = p::AlertState::parse(&alerts[index].state).unwrap_or(p::AlertState::Open);
        let transition = alerts[index].transition.unwrap_or(0);

        if p::is_suppressed(spec.notifications.as_ref(), alerts[index].kind, state) {
            alerts[index].notified_transition = Some(transition);
            alerts[index].delivery = Some(AlertDelivery {
                state: Some(p::DeliveryState::Suppressed.as_str().to_string()),
                attempts: Some(0),
                last_attempt_at: None,
                job_ref: None,
                last_error: Some(p::cap_error(
                    "no configured route carries this alert kind; nothing was sent and nothing \
                     is retried",
                )),
            });
            continue;
        }

        if created >= p::MAX_DELIVERY_JOBS_PER_PASS {
            deferred += 1;
            continue;
        }

        // The backoff. A previous attempt for THIS transition that failed sets
        // `attempts`; a new transition starts at attempt 1 with no wait.
        let previous = alerts[index]
            .delivery
            .as_ref()
            .filter(|_| alerts[index].notified_transition == Some(transition));
        let attempts = previous.and_then(|d| d.attempts).unwrap_or(0);
        if let Some(d) = previous {
            if p::DeliveryState::parse(d.state.as_deref().unwrap_or_default())
                == Some(p::DeliveryState::Failed)
            {
                match p::next_attempt_at(d) {
                    None => continue,
                    Some(at) if at > now => {
                        deferred += 1;
                        continue;
                    }
                    Some(_) => {}
                }
            }
        }
        let attempt = attempts + 1;

        let facts = p::EventFacts {
            namespace: &namespace,
            name: &name,
            uid: &uid,
            alert_key: &alerts[index].key,
            kind: alerts[index].kind,
            // A `RecoveryCompleted` is recorded as `Resolved` because it
            // auto-resolves the instant it opens — but the MESSAGE about it
            // is news, not the clearing of a page, so its action is
            // `trigger`. A `resolve` here would read on a webhook as "the
            // recovery-completed condition has cleared", which is nonsense.
            open: state == p::AlertState::Open
                || alerts[index].kind == AlertKind::RecoveryCompleted,
            transition,
            health: verdict.health,
            summary: &verdict.summary,
            point: verdict.point.as_ref(),
            consecutive_failed_runs: verdict.consecutive_failed_runs,
            missed_slots: missed_slot_count(&schedule_facts, &verdict),
            // ALWAYS `sampled`, `degraded` or `none`, and this controller
            // publishes `sampled` for a policy whose newest point carries
            // verified evidence and `none` otherwise. There is no fourth
            // value and there must never be one: `logweir notify deliver`
            // refuses `complete` at parse time.
            scope: verification_scope(&verdict),
            generated_at: now,
        };
        let document = p::event_document(&facts);
        let config_map_name = p::event_config_map_name(&name, &alerts[index].key, transition);
        let job_name = p::delivery_job_name(&name, &uid, &alerts[index].key, transition, attempt);
        let spec_for_job = delivery_job_spec(
            &job_name,
            &namespace,
            &owner,
            &config_map_name,
            spec.notifications
                .as_ref()
                .and_then(|n| n.routes.as_deref()),
            &ctx.runner_image,
            ctx.allow_insecure_sinks,
        );
        // THE JOB FIRST, AND THE ConfigMap IT MOUNTS SECOND — review F7, and
        // the shape `controllers/recovery_catalog.rs` already uses for its
        // pages. The event ConfigMap is `immutable: true` and this role holds
        // no `delete`; owned by the POLICY, nothing ever removed it, so one
        // object per `(alertKey, transition)` accumulated for the life of the
        // policy. Owned by the FIRST attempt's Job, the API server's TTL
        // controller collects it with that Job.
        //
        // The cost is stated rather than buried: a pod scheduled in the window
        // between the two creates sits `ContainerCreating` on a mount the
        // kubelet retries, and a crash inside that window leaves a Job whose
        // ConfigMap never arrives — which the next pass repairs, because the
        // ledger records the delivery only after BOTH objects exist.
        create_delivery_job(&ctx.client, &namespace, &spec_for_job).await?;
        let first_job = p::delivery_job_name(&name, &uid, &alerts[index].key, transition, 1);
        let cm_owner = event_config_map_owner(&ctx.client, &namespace, &first_job).await?;
        create_event_config_map(
            &ctx.client,
            &namespace,
            &config_map_name,
            &cm_owner,
            &document,
        )
        .await?;
        created += 1;

        alerts[index].notified_transition = Some(transition);
        alerts[index].delivery = Some(AlertDelivery {
            state: Some(p::DeliveryState::Pending.as_str().to_string()),
            attempts: Some(attempt),
            last_attempt_at: Some(now),
            job_ref: Some(LocalRef {
                name: job_name.clone(),
            }),
            last_error: None,
        });
        info!(
            policy = %name,
            namespace = %namespace,
            alert = %alerts[index].key,
            transition,
            attempt,
            job = %job_name,
            "created one delivery Job for one alert transition; a duplicate reconcile computes \
             the same name and gets 409 rather than paging a human twice"
        );
    }

    // ------------------------------------------------------------------
    // Status. One merge PATCH, `metadata.resourceVersion` as the
    // precondition (seam S7).
    // ------------------------------------------------------------------
    let status = build_status(policy, &verdict, alerts, now, interval);
    let commit = write_status(&api, policy, &status).await?;

    // ------------------------------------------------------------------
    // The TTL, LAST, and only on a committed status.
    // ------------------------------------------------------------------
    let mut ttl_patched = 0usize;
    if commit.is_committed() {
        for job_name in &finished_jobs {
            set_delivery_ttl(&ctx.client, &namespace, job_name).await?;
            ttl_patched += 1;
        }
    } else if !finished_jobs.is_empty() {
        debug!(
            policy = %name,
            "the status patch did not land (409); the finished delivery Jobs keep their pods \
             until the pass that reads the newer object records them"
        );
    }

    let pending = status
        .alerts
        .as_ref()
        .is_some_and(|a| a.iter().any(is_delivery_pending));
    let requeue_seconds = if deferred > 0 || pending {
        REQUEUE_DELIVERY_SECONDS
    } else {
        u64::try_from(interval).unwrap_or(300)
    };

    Ok(Outcome {
        health: verdict.health,
        freshness: verdict.freshness,
        reason: verdict.reason,
        created_jobs: created,
        deferred,
        ttl_patched,
        committed: commit.is_committed(),
        requeue_seconds,
    })
}

/// The outcome for an object this controller cannot identify.
///
/// Nothing was read, nothing was written and no verdict was computed —
/// [`REASON_NOT_EVALUATED`] is the reason a `Ready` condition would carry, and
/// the freshness reason says the same thing rather than borrowing one that
/// would describe a check that never ran.
fn idle() -> Outcome {
    Outcome {
        health: p::Health::Unknown,
        freshness: p::Freshness::Unknown,
        reason: p::FreshnessReason::NotEvaluated,
        created_jobs: 0,
        deferred: 0,
        ttl_patched: 0,
        committed: false,
        requeue_seconds: ERROR_REQUEUE_SECONDS,
    }
}

fn is_delivery_pending(entry: &AlertEntry) -> bool {
    entry
        .delivery
        .as_ref()
        .and_then(|d| d.state.as_deref())
        .and_then(p::DeliveryState::parse)
        == Some(p::DeliveryState::Pending)
}

/// How many slots the schedules report as missed.
///
/// D1 W2's `status.missedSlots` when the schedule carries it; otherwise `1`
/// for a schedule that recorded a `lastMissedSlot` and `0` for one that did
/// not. The fallback is a LOWER bound and is documented as one: the field that
/// would make it exact is D1's, in review, and a protection verdict does not
/// wait on a neighbouring worker's rebase.
fn missed_slot_count(schedules: &[p::ScheduleFacts], verdict: &p::Verdict) -> i64 {
    let declared: i64 = schedules.iter().filter_map(|s| s.missed_slots).sum();
    if declared > 0 {
        return declared;
    }
    // The LIVE signal, not the sticky audit field (review F5): a slot missed in
    // January and fired over every night since is not a miss this event should
    // report to the person it wakes.
    i64::from(verdict.missed_since_last_fire)
}

/// `sampled` when the newest available point carries verified evidence, `none`
/// otherwise.
///
/// **NEVER `complete`.** Logweir compares a SAMPLE of records; the enum has
/// three variants and `logweir notify deliver` refuses a fourth at parse time.
/// `degraded` belongs to a rehearsal's `integrityLevel: consume-only` and is
/// reachable from PLAT-14.3's inputs, not from a backup's.
fn verification_scope(verdict: &p::Verdict) -> p::VerificationScope {
    match verdict.point.as_ref().map(|point| point.evidence) {
        Some(evidence) if evidence.is_verified() => p::VerificationScope::Sampled,
        _ => p::VerificationScope::None,
    }
}

// ===========================================================================
// Reading
// ===========================================================================

/// Every `Backup` in the namespace, bounded by [`MAX_BACKUP_PAGES`] pages of
/// [`BACKUP_PAGE_LIMIT`].
///
/// # Why this is not narrowed by `logweir.dev/schedule-uid`
///
/// D3 §3.2 names that label as the INDEX and `spec.scheduleRef.uid` as the
/// AUTHORITY. Nothing on `main` writes the label yet (it is D1 W2's), so a
/// label-selected list answers empty and every installation would read
/// `Unprotected` — and a **manual run of a schedule is part of its history**
/// (`identity::is_run_of_schedule`), which a label written only by the
/// scheduler would miss even once it exists. Membership is therefore decided
/// by the authority, over a bounded listing. Narrowing by the label when D1's
/// writer lands is an optimisation, not a change to the rule.
async fn list_backups(
    client: &kube::Client,
    namespace: &str,
) -> Result<Vec<Backup>, ReconcileError> {
    let api: Api<Backup> = Api::namespaced(client.clone(), namespace);
    let mut out: Vec<Backup> = Vec::new();
    let mut token: Option<String> = None;
    for _ in 0..MAX_BACKUP_PAGES {
        let mut params = ListParams::default().limit(BACKUP_PAGE_LIMIT);
        if let Some(cursor) = token.as_deref() {
            params = params.continue_token(cursor);
        }
        let page = api.list(&params).await.map_err(ReconcileError::Api)?;
        token = page.metadata.continue_.clone().filter(|t| !t.is_empty());
        out.extend(page.items);
        if token.is_none() {
            break;
        }
    }
    if token.is_some() {
        debug!(
            namespace = %namespace,
            pages = MAX_BACKUP_PAGES,
            "the Backup listing hit its page bound; the protection verdict is a projection of \
             the runs it saw and says so"
        );
    }
    Ok(out)
}

/// Whether this `Backup` counts towards this policy's objective.
///
/// With `scheduleRefs`: membership is `identity::is_run_of_schedule` against
/// each named schedule's UID — which accepts `spec.scheduleRef.{name,uid}`,
/// the legacy controller `ownerReference` and PLAT-05.2's retention
/// annotation, and therefore **counts a manual run of the schedule as
/// history**. Without `scheduleRefs`: every run of the named source that
/// writes to the named destination.
fn is_member(
    backup: &Backup,
    spec: &crate::crds::protection_policy::ProtectionPolicySpec,
    schedules: &[(String, Option<BackupSchedule>)],
) -> bool {
    if backup.spec.source_ref.name != spec.protects.source_ref.name {
        return false;
    }
    if !destination_matches(backup, spec) {
        return false;
    }
    let refs = spec.protects.schedule_refs.as_deref().unwrap_or_default();
    if refs.is_empty() {
        return true;
    }
    schedules.iter().any(|(name, found)| {
        found
            .as_ref()
            .and_then(kube::ResourceExt::uid)
            .is_some_and(|uid| identity::is_run_of_schedule(backup, name, &uid))
    })
}

/// Whether the run wrote where this policy protects.
///
/// A saved destination matches by NAME; an inline archive matches by URL. The
/// two are never compared across: a `destinationRef` run carries the sentinel
/// `logweir-destination://<name>` in `spec.archive.url`, so comparing URLs
/// would make a destination-backed run match a `legacyArchive` policy whose
/// URL happened to be the sentinel.
fn destination_matches(
    backup: &Backup,
    spec: &crate::crds::protection_policy::ProtectionPolicySpec,
) -> bool {
    match (
        spec.protects.destination_ref.as_ref(),
        spec.protects.legacy_archive.as_ref(),
    ) {
        (Some(wanted), _) => {
            backup
                .spec
                .destination_ref
                .as_ref()
                .map(|d| d.name.as_str())
                == Some(&wanted.name)
        }
        (None, Some(archive)) => {
            backup.spec.destination_ref.is_none() && backup.spec.archive.url == archive.url
        }
        // The CEL rule H1 makes this unreachable; answered rather than
        // asserted, because a panic here would be a controller crash on an
        // object the API server accepted.
        (None, None) => false,
    }
}

fn schedule_facts((name, found): &(String, Option<BackupSchedule>)) -> p::ScheduleFacts {
    let Some(schedule) = found else {
        return p::ScheduleFacts {
            name: name.clone(),
            exists: false,
            ..p::ScheduleFacts::default()
        };
    };
    let status = schedule.status.as_ref();
    let ready = status
        .and_then(|s| s.conditions.as_ref())
        .and_then(|c| c.iter().find(|c| c.r#type == "Ready"))
        .map(|c| c.status.clone());
    p::ScheduleFacts {
        name: name.clone(),
        suspended: schedule.spec.suspend,
        ready,
        next_fire_time: status.and_then(|s| s.next_fire_time),
        last_missed_slot: status.and_then(|s| s.last_missed_slot.clone()),
        last_fire_time: status.and_then(|s| s.last_fire_time),
        missed_slots: declared_missed_slots(schedule),
        exists: true,
    }
}

/// D1 W2's `status.missedSlots`, read **defensively** off the serialized
/// status rather than off a typed field.
///
/// The field is a CONTRACT this worker consumes and not one it can require:
/// D1 W2 is in review, not on `main`. Reading it through `serde_json` means
/// the day it lands the count becomes exact with no edit here, and until then
/// the absence is an absence rather than a compile error or a pinned
/// dependency on another worker's branch.
fn declared_missed_slots(schedule: &BackupSchedule) -> Option<i64> {
    let status = serde_json::to_value(schedule.status.as_ref()?).ok()?;
    match status.get("missedSlots")? {
        // D1 W2's landed shape: `{count, countCapped, recent, …}`. `count` is a
        // FLOOR when `countCapped` is set — the enumeration stopped at its
        // 1000-slot cap — and a floor is the honest number to alert on.
        Value::Object(block) => block.get("count").and_then(Value::as_i64),
        // The two shapes the field was drafted as, kept because this read is
        // deliberately defensive: a protection verdict does not wait on a
        // neighbouring worker's rebase, and it does not break on one either.
        Value::Number(n) => n.as_i64(),
        Value::Array(items) => i64::try_from(items.len()).ok(),
        _ => None,
    }
}

/// One `Backup` as a candidate recovery point.
fn candidate_from_backup(
    backup: &Backup,
    spec: &crate::crds::protection_policy::ProtectionPolicySpec,
) -> p::PointCandidate {
    let status = backup.status.as_ref();
    let verification = status
        .and_then(|s| s.evidence.as_ref())
        .and_then(|e| e.verification.as_ref());
    let evidence = p::Evidence::from_verification(
        verification.and_then(|v| v.result.as_deref()),
        verification
            .and_then(|v| v.trust.as_ref())
            .and_then(|t| t.basis.as_deref()),
    );
    let point_id = status
        .and_then(|s| s.evidence.as_ref())
        .and_then(|e| e.receipt_sha256.as_deref())
        .and_then(p::point_id_from_receipt_digest);
    // `windowCovered.toMs` is EXCLUSIVE — the newest archived record is one
    // millisecond before it — and it is published under its own name so
    // nothing confuses it with the capture start the objective is measured
    // from.
    let newest_record_at = status
        .and_then(|s| s.window_covered.as_ref())
        .and_then(|w| chrono::DateTime::from_timestamp_millis(w.to_ms.saturating_sub(1)));
    p::PointCandidate {
        backup_name: Some(backup.name_any()),
        point_id,
        recovery_point_at: status
            .and_then(|s| s.capture.as_ref())
            .and_then(|c| c.started_at),
        newest_record_at,
        phase: status.and_then(|s| s.phase.clone()),
        exit_code: status.and_then(|s| s.exit_code),
        evidence,
        topics: backup.spec.topics.clone(),
        covers_all_topics: backup.spec.all_user_topics.is_some(),
        destination_matches: destination_matches(backup, spec),
        source_matches: backup.spec.source_ref.name == spec.protects.source_ref.name,
    }
}

/// One `Backup` as a slot of history.
fn slot_run(backup: &Backup) -> p::SlotRun {
    let (_, attempt, _) = identity::declared_trigger(backup);
    let status = backup.status.as_ref();
    let phase = status.and_then(|s| s.phase.as_deref());
    let outcome = match phase {
        Some("Succeeded") => p::SlotOutcome::Succeeded,
        Some("Failed") => p::SlotOutcome::Failed,
        _ => p::SlotOutcome::Running,
    };
    p::SlotRun {
        // A manual run is its own slot: it belongs to the schedule's history
        // and has no cron slot of its own, so keying it on the object name
        // keeps it from collapsing into a scheduled slot's retry chain.
        slot: backup
            .spec
            .slot
            .clone()
            .unwrap_or_else(|| backup.name_any()),
        attempt,
        outcome,
        at: status
            .and_then(|s| s.capture.as_ref())
            .and_then(|c| c.finished_at.or(c.started_at))
            .or_else(|| backup.creation_timestamp().map(|t| t.0)),
        backup_name: backup.name_any(),
        reason: status.and_then(|s| s.exit_reason.clone()),
    }
}

/// The archive-set identity of one run — the value a `Restore` names in
/// `spec.backupSetRef`.
///
/// `status.execution.id` is the server-derived identity PLAT-06.1 froze;
/// `status.backupId` is what an older controller wrote. Both are the same
/// string for every object either of them produced, and reading the newer one
/// first is what makes a `Restore` of a legacy run still match.
fn backup_set_id(backup: &Backup) -> Option<String> {
    let status = backup.status.as_ref()?;
    status
        .execution
        .as_ref()
        .map(|e| e.id.clone())
        .or_else(|| status.backup_id.clone())
}

/// Terminal `Restore`s that recovered a point this policy protects.
///
/// # What "matching this policy's points" means, exactly
///
/// `spec.backupSetRef` is a `backupId` — the ARCHIVE SET, D3 §5.1's
/// `backup_id` — and it has to be one of the sets this policy's own member
/// runs produced. The destination has to match too: two policies over two
/// destinations can name the same schedule's `backupId` only if the same run
/// wrote to both, which it does not.
///
/// A restore of somebody else's archive is not this policy's recovery, and
/// recording it here would tell one team that another team's incident is over.
async fn read_recoveries(
    client: &kube::Client,
    namespace: &str,
    spec: &crate::crds::protection_policy::ProtectionPolicySpec,
    backup_ids: &[String],
) -> Result<Vec<p::RecoveryCompletion>, ReconcileError> {
    if backup_ids.is_empty() {
        return Ok(Vec::new());
    }
    let api: Api<Restore> = Api::namespaced(client.clone(), namespace);
    let restores = api
        .list(&ListParams::default().limit(BACKUP_PAGE_LIMIT))
        .await
        .map_err(ReconcileError::Api)?;
    let mut out: Vec<p::RecoveryCompletion> = Vec::new();
    for restore in restores.items {
        let Some(uid) = restore.uid() else { continue };
        if !backup_ids.contains(&restore.spec.backup_set_ref) {
            continue;
        }
        if !restore_destination_matches(&restore, spec) {
            continue;
        }
        let status = restore.status.as_ref();
        let phase = status.and_then(|s| s.phase.clone()).unwrap_or_default();
        if !matches!(phase.as_str(), "Succeeded" | "Failed") {
            continue;
        }
        out.push(p::RecoveryCompletion {
            restore_name: restore.name_any(),
            restore_uid: uid,
            phase,
            outcome: status.and_then(|s| s.outcome.clone()),
            at: restore_finished_at(&restore),
        });
        if out.len() >= p::MAX_RECOVERY_ALERTS {
            break;
        }
    }
    Ok(out)
}

/// When this `Restore` actually FINISHED — review **F10**.
///
/// `RestoreStatus` carries no `finishedAt`, so the terminal instant is the
/// `lastTransitionTime` of its terminal condition (`Complete` or `Failed`),
/// which the restore reconciler writes when the run ends.
/// `metadata.creationTimestamp` is the fallback and only the fallback: for a
/// long recovery it is HOURS before completion, and it is that instant that
/// lands in the ledger's `openedAt`/`resolvedAt` and in the event a responder
/// reads on D3 §3.5's incident-facing surface.
fn restore_finished_at(restore: &Restore) -> Option<Time> {
    restore
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|conditions| {
            conditions
                .iter()
                .filter(|c| {
                    matches!(c.r#type.as_str(), "Complete" | "Failed") && c.status == "True"
                })
                .filter_map(|c| c.last_transition_time)
                .max()
        })
        .or_else(|| restore.creation_timestamp().map(|t| t.0))
}

/// The `Restore` read the archive this policy protects.
fn restore_destination_matches(
    restore: &Restore,
    spec: &crate::crds::protection_policy::ProtectionPolicySpec,
) -> bool {
    match (
        spec.protects.destination_ref.as_ref(),
        spec.protects.legacy_archive.as_ref(),
    ) {
        (Some(wanted), _) => {
            restore
                .spec
                .source_destination_ref
                .as_ref()
                .map(|d| d.name.as_str())
                == Some(&wanted.name)
        }
        (None, Some(archive)) => {
            restore.spec.source_destination_ref.is_none()
                && restore.spec.source_archive.url == archive.url
        }
        (None, None) => false,
    }
}

/// What the catalog can say about this policy's points.
async fn read_catalog(
    client: &kube::Client,
    namespace: &str,
    spec: &crate::crds::protection_policy::ProtectionPolicySpec,
    now: Time,
) -> Result<p::CatalogAnswer, ReconcileError> {
    if !spec.objectives.require_catalog_availability {
        return Ok(p::CatalogAnswer::NotConsulted);
    }
    let Some(reference) = spec.protects.catalog_ref.as_ref() else {
        return Ok(p::CatalogAnswer::NotConsulted);
    };
    let catalogs: Api<RecoveryCatalog> = Api::namespaced(client.clone(), namespace);
    let Some(catalog) = catalogs
        .get_opt(&reference.name)
        .await
        .map_err(ReconcileError::Api)?
    else {
        return Ok(p::CatalogAnswer::Stale(
            p::FreshnessReason::CatalogUnreadable,
        ));
    };
    let status = catalog.status.as_ref();
    // AN EXPIRED VIEW IS NOT A VIEW. `viewExpiresAt` is the catalog
    // controller's own statement of how long its pages describe the archive;
    // reading them past it would answer "the point is there" from a listing
    // taken before a retention run.
    let fresh = status
        .and_then(|s| s.view_expires_at)
        .is_some_and(|at| at > now);
    if !fresh {
        return Ok(p::CatalogAnswer::Stale(p::FreshnessReason::CatalogStale));
    }
    let pages: Vec<String> = status
        .and_then(|s| s.pages.as_ref())
        .map(|pages| {
            pages
                .iter()
                .take(MAX_CATALOG_PAGES)
                .map(|page| page.config_map_name.clone())
                .collect()
        })
        .unwrap_or_default();
    let maps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    let mut entries: Vec<p::CatalogEntry> = Vec::new();
    for page in &pages {
        let Some(map) = maps.get_opt(page).await.map_err(ReconcileError::Api)? else {
            // A page the status names and the API server does not have is an
            // UNREADABLE view, not an empty one: answering "no entry" would
            // read as "the point is gone".
            return Ok(p::CatalogAnswer::Stale(
                p::FreshnessReason::CatalogUnreadable,
            ));
        };
        let Some(body) = map.data.as_ref().and_then(|d| d.get(CATALOG_PAGE_DATA_KEY)) else {
            return Ok(p::CatalogAnswer::Stale(
                p::FreshnessReason::CatalogUnreadable,
            ));
        };
        for line in body.lines().filter(|l| !l.trim().is_empty()) {
            match serde_json::from_str::<p::CatalogEntry>(line) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    debug!(
                        namespace = %namespace,
                        catalog = %reference.name,
                        page = %page,
                        error = %e,
                        "a catalog page line did not parse as a view entry; it is skipped"
                    );
                }
            }
        }
    }
    Ok(p::CatalogAnswer::Fresh(entries))
}

// ===========================================================================
// Delivery
// ===========================================================================

/// The owner reference every object this reconciler creates carries.
///
/// From `kube::Resource`'s own `api_version`/`kind` and never two string
/// literals: a hand-written `apiVersion` that drifts from the CRD makes the
/// garbage collector refuse to resolve the owner, and an unresolvable owner is
/// a cascade that silently does not happen.
#[must_use]
pub fn owner_of(name: &str, uid: &str) -> RunnerOwner {
    RunnerOwner {
        api_version: ProtectionPolicy::api_version(&()).to_string(),
        kind: ProtectionPolicy::kind(&()).to_string(),
        name: name.to_string(),
        uid: uid.to_string(),
    }
}

/// The owner reference, with `blockOwnerDeletion: false` — D3 §3.4 point 3.
///
/// # Why `false`, and why it is written out rather than defaulted
///
/// `blockOwnerDeletion: true` makes the garbage collector hold the OWNER in
/// `Terminating` until this dependent is gone, and it requires the deleter to
/// hold `update` on the owner's `finalizers` subresource. A delivery Job is a
/// side effect of a verdict; an operator deleting a `ProtectionPolicy` during
/// an incident must not find the delete hanging on a notification Job whose
/// pod is waiting out a 120-second deadline against a sink that is down.
/// [`crate::job::build`] writes `true` — correct for a run whose evidence must
/// outlive nothing — so this is overridden here rather than parameterised in a
/// file this worker does not own.
fn deletion_safe_owner(
    owner: &RunnerOwner,
) -> k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
    k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
        api_version: owner.api_version.clone(),
        kind: owner.kind.clone(),
        name: owner.name.clone(),
        uid: owner.uid.clone(),
        controller: Some(true),
        block_owner_deletion: Some(false),
    }
}

/// The owner every event `ConfigMap` carries: the FIRST delivery Job for its
/// transition.
///
/// # Why the Job and not the policy (review F7)
///
/// The ConfigMap is `immutable: true` and this role holds `delete` on nothing,
/// so whoever owns it decides when it goes away. Owned by the
/// `ProtectionPolicy` it never went away: one object per
/// `(alertKey, transition)`, for the life of the policy — with a daily
/// re-notify and two long-open alerts, thousands a year that every
/// `kubectl get cm` and every controller LIST pays for. Owned by the Job, the
/// API server's TTL controller collects it with that Job.
///
/// **The FIRST attempt's Job**, not this attempt's: retries share the one
/// ConfigMap, and hanging it off attempt 3 would leave attempts 1 and 2 to
/// create an owner reference to a Job that does not exist yet. Attempt 1's TTL
/// is patched only once its own verdict is recorded, and the whole retry
/// schedule runs inside six minutes against a 3600 s TTL, so the mount is alive
/// for every attempt that can still be made.
///
/// The Job's `metadata.uid` is read back from the create — or, on the
/// duplicate-reconcile 409, from a `get`. A UID that cannot be read at all
/// falls back to the policy: an unowned immutable object is worse than one
/// collected late, and the fallback is logged rather than silent.
async fn event_config_map_owner(
    client: &kube::Client,
    namespace: &str,
    job_name: &str,
) -> Result<RunnerOwner, ReconcileError> {
    let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
    let uid = jobs
        .get_opt(job_name)
        .await
        .map_err(ReconcileError::Api)?
        .and_then(|job| job.uid());
    match uid {
        Some(uid) => Ok(RunnerOwner {
            api_version: "batch/v1".to_string(),
            kind: "Job".to_string(),
            name: job_name.to_string(),
            uid,
        }),
        None => {
            warn!(
                namespace = %namespace,
                job = %job_name,
                "the first delivery Job for this transition carries no readable metadata.uid; \
                 the event ConfigMap is owned by the ProtectionPolicy instead and is collected \
                 when the policy is deleted rather than by the Job's TTL"
            );
            Err(ReconcileError::Api(kube::Error::Discovery(
                kube::error::DiscoveryError::MissingResource(format!(
                    "delivery Job {job_name} has no metadata.uid to own its event ConfigMap"
                )),
            )))
        }
    }
}

/// Create the immutable event `ConfigMap`, tolerating the duplicate-reconcile
/// 409.
///
/// `immutable: true` because a view a second pass could rewrite is not a
/// record: the document names a `(policyUID, alertKey, transition)` triple and
/// the Job that mounts it may already be running.
async fn create_event_config_map(
    client: &kube::Client,
    namespace: &str,
    name: &str,
    owner: &RunnerOwner,
    document: &Value,
) -> Result<(), ReconcileError> {
    let maps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    let body = serde_json::to_string(document).unwrap_or_else(|_| "{}".to_string());
    let map = ConfigMap {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            labels: Some(BTreeMap::from([
                (
                    crate::check::job::LABEL_MANAGED_BY.to_string(),
                    crate::check::job::MANAGED_BY.to_string(),
                ),
                (
                    crate::check::job::LABEL_COMPONENT.to_string(),
                    COMPONENT_NOTIFICATION.to_string(),
                ),
            ])),
            owner_references: Some(vec![deletion_safe_owner(owner)]),
            ..ObjectMeta::default()
        },
        immutable: Some(true),
        data: Some(BTreeMap::from([(p::EVENT_DATA_KEY.to_string(), body)])),
        binary_data: None,
    };
    match maps.create(&PostParams::default(), &map).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 409 => {
            debug!(
                namespace = %namespace,
                config_map = %name,
                "the event ConfigMap already exists; this pass adopts it (the name is a pure \
                 function of the alert key and the transition, so it holds the same document)"
            );
            Ok(())
        }
        Err(e) => Err(ReconcileError::Api(e)),
    }
}

/// `app.kubernetes.io/component` on the objects a delivery creates.
pub const COMPONENT_NOTIFICATION: &str = "notification";

/// The [`RunnerJobSpec`] for one delivery.
///
/// # One Job, one sink of each kind
///
/// `spec.notifications.routes` is `maxItems: 4`, and `logweir notify deliver`
/// reads ONE variable per sink kind. So a Job addresses at most one PagerDuty,
/// one webhook and one Slack, and this function projects the FIRST of each in
/// route order. That is a real bound on D3 §3.1's shape and it is recorded
/// rather than papered over: two routes that both name a webhook deliver to
/// the first one only. The alternative — a Job per route — contradicts D3
/// §3.4's "ONE Job per event" and would multiply the pages a single transition
/// produces.
///
/// # `allow_insecure_sinks`
///
/// The INSTALLATION's answer, threaded from [`ProtectionContext`] and never
/// from `routes`: `true` adds [`ALLOW_INSECURE_SINKS_ENV`]`=1` to the literal
/// env and `false` adds nothing at all, so a default install renders exactly
/// the Job it rendered before this parameter existed. See
/// [`CONTROLLER_ALLOW_INSECURE_SINKS_ENV`] for why a policy cannot set it.
#[must_use]
pub fn delivery_job_spec(
    job_name: &str,
    namespace: &str,
    owner: &RunnerOwner,
    config_map_name: &str,
    routes: Option<&[NotificationRoute]>,
    image: &RunnerImage,
    allow_insecure_sinks: bool,
) -> RunnerJobSpec {
    let routes = routes.unwrap_or_default();
    let mut env_from_secret: Vec<EnvFromSecret> = Vec::new();
    let mut env_literal: Vec<(String, String)> = vec![
        // The deliverer's stdout is the machine contract; anything chattier
        // competes with the `notify-result=` lines for the log's byte budget.
        ("RUST_LOG".to_string(), "warn".to_string()),
    ];

    // THE INSTALLATION'S ESCAPE HATCH, AND ONLY WHEN IT IS ON. The argument
    // comes from the CONTROLLER's environment
    // ([`CONTROLLER_ALLOW_INSECURE_SINKS_ENV`]) and `routes` cannot reach this
    // line: a policy is a namespaced object and this is the cluster
    // administrator's switch.
    //
    // WHEN IT IS OFF, NOTHING IS PUSHED — not the variable with a falsy value.
    // A rendered Job is then byte-identical to the one this function built
    // before the value existed, which is what makes the default install's
    // goldens unmoved and what keeps `logweir notify deliver`'s own reading
    // ("an explicit affirmative only") the single place the answer is decided.
    if allow_insecure_sinks {
        env_literal.push((ALLOW_INSECURE_SINKS_ENV.to_string(), "1".to_string()));
    }

    if let Some((route, channel)) = routes
        .iter()
        .find_map(|r| r.pager_duty.as_ref().map(|c| (r, c)))
    {
        env_from_secret.push(secret_env(ROUTING_KEY_ENV, &channel.routing_key_secret_ref));
        if let Some(endpoint) = channel.endpoint.as_deref() {
            env_literal.push((PAGERDUTY_ENDPOINT_ENV.to_string(), endpoint.to_string()));
        }
        debug!(route = %route.name, "the PagerDuty route this delivery Job addresses");
    }
    if let Some(channel) = routes.iter().find_map(|r| r.webhook.as_ref()) {
        env_from_secret.push(secret_env(WEBHOOK_URL_ENV, &channel.url_secret_ref));
    }
    if let Some(channel) = routes.iter().find_map(|r| r.slack.as_ref()) {
        env_from_secret.push(secret_env(
            SLACK_WEBHOOK_URL_ENV,
            &channel.webhook_url_secret_ref,
        ));
    }

    RunnerJobSpec {
        name: job_name.to_string(),
        namespace: namespace.to_string(),
        owner: owner.clone(),
        args: delivery_argv(),
        deadline_seconds: DELIVERY_DEADLINE_SECONDS,
        service_account_name: SERVICE_ACCOUNT.to_string(),
        // NO SECRET VOLUME. The signing key, the approval bundle and the
        // archive credential are all absent from a delivery: it signs nothing,
        // it reads no archive, and it holds no token.
        secret_mounts: Vec::new(),
        config_map_mounts: vec![ConfigMapMount {
            volume: EVENT_VOLUME.to_string(),
            config_map_name: config_map_name.to_string(),
            mount_path: p::EVENT_MOUNT_PATH.to_string(),
            items: Vec::new(),
        }],
        env_from_secret,
        env_literal,
        plan_config_map: None,
        image: image.image.clone(),
        image_pull_policy: image.image_pull_policy.clone(),
    }
}

/// `valueFrom.secretKeyRef` AND NEVER A LITERAL.
///
/// The controller holds no verb on `secrets`, so it could not inline the value
/// if it wanted to; this function is where that fact becomes the Job's shape.
/// Every reader of `kubectl get job -o yaml` sees a Secret NAME and a KEY.
fn secret_env(name: &str, reference: &SecretKeyRef) -> EnvFromSecret {
    EnvFromSecret {
        name: name.to_string(),
        secret_name: reference.name.clone(),
        key: reference.key.clone(),
    }
}

/// Create the delivery Job, tolerating the duplicate-reconcile 409, with
/// `blockOwnerDeletion: false` on its owner reference.
async fn create_delivery_job(
    client: &kube::Client,
    namespace: &str,
    spec: &RunnerJobSpec,
) -> Result<(), ReconcileError> {
    let mut built = job::build(spec);
    built.metadata.owner_references = Some(vec![deletion_safe_owner(&spec.owner)]);
    let labels = BTreeMap::from([
        (
            crate::check::job::LABEL_MANAGED_BY.to_string(),
            crate::check::job::MANAGED_BY.to_string(),
        ),
        (
            crate::check::job::LABEL_COMPONENT.to_string(),
            COMPONENT_NOTIFICATION.to_string(),
        ),
    ]);
    built.metadata.labels = Some(labels.clone());
    if let Some(job_spec) = built.spec.as_mut() {
        let mut meta = job_spec.template.metadata.take().unwrap_or_default();
        meta.labels = Some(labels);
        job_spec.template.metadata = Some(meta);
    }
    let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
    match jobs.create(&PostParams::default(), &built).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 409 => {
            debug!(
                namespace = %namespace,
                job = %spec.name,
                "the delivery Job already exists; this pass adopts it rather than paging a \
                 human a second time"
            );
            Ok(())
        }
        Err(e) => Err(ReconcileError::Api(e)),
    }
}

/// Look at the delivery Job for one ledger entry and record what it did.
///
/// Returns the entry (possibly unchanged) and the name of a FINISHED Job whose
/// verdict this pass just recorded — the caller patches its TTL after the
/// status write, never before.
async fn observe_delivery(
    client: &kube::Client,
    namespace: &str,
    entry: &AlertEntry,
    now: Time,
) -> Result<(AlertEntry, Option<String>), ReconcileError> {
    let Some(delivery) = entry.delivery.as_ref() else {
        return Ok((entry.clone(), None));
    };
    if p::DeliveryState::parse(delivery.state.as_deref().unwrap_or_default())
        != Some(p::DeliveryState::Pending)
    {
        return Ok((entry.clone(), None));
    }
    let Some(job_ref) = delivery.job_ref.as_ref() else {
        return Ok((entry.clone(), None));
    };
    let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
    let Some(job) = jobs
        .get_opt(&job_ref.name)
        .await
        .map_err(ReconcileError::Api)?
    else {
        // The Job is gone — its TTL fired, or an operator removed it. There is
        // nothing left to read and nothing to claim; the delivery is recorded
        // as failed rather than left Pending forever, which would keep the
        // requeue at fifteen seconds for the life of the object.
        let mut next = entry.clone();
        next.delivery = Some(AlertDelivery {
            state: Some(p::DeliveryState::Failed.as_str().to_string()),
            last_attempt_at: Some(now),
            last_error: Some(p::cap_error(
                "the delivery Job no longer exists, so its result cannot be read",
            )),
            ..delivery.clone()
        });
        return Ok((next, None));
    };
    if !crate::controllers::backup::job_finished(&job) {
        return Ok((entry.clone(), None));
    }

    // SEAM S6. The pod is proved by the Job's own `metadata.uid` on its
    // controller `ownerReference` before one byte of its log is read; a second
    // claimant is refused rather than ranked.
    let found = crate::check::pod::find_owned_pod(client, namespace, &job)
        .await
        .map_err(ReconcileError::Api)?;
    let (exit_code, tail) = match found.as_ref() {
        Some(pod) => {
            let exit = crate::controllers::backup::terminated_exit_code(pod);
            let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
            let log = match pods
                .logs(&pod.name_any(), &crate::check::relay::log_params())
                .await
            {
                Ok(log) => log,
                Err(e) if crate::check::is_log_absent(&e) => String::new(),
                Err(e) => return Err(ReconcileError::Api(e)),
            };
            (exit, log)
        }
        None => (None, String::new()),
    };
    let lines = crate::controllers::backup::tail_lines(&tail);
    let (state, message) = p::classify_delivery(exit_code, &lines);

    let mut next = entry.clone();
    next.delivery = Some(AlertDelivery {
        state: Some(state.as_str().to_string()),
        last_attempt_at: Some(now),
        last_error: (state != p::DeliveryState::Delivered).then(|| p::cap_error(&message)),
        ..delivery.clone()
    });
    if state == p::DeliveryState::Failed {
        warn!(
            namespace = %namespace,
            alert = %entry.key,
            job = %job_ref.name,
            attempts = delivery.attempts.unwrap_or(0),
            "a protection alert was not delivered; the failure is recorded on this policy's \
             status and NOTHING else is written — no Backup, no Restore, no evidence"
        );
    }
    Ok((next, Some(job_ref.name.clone())))
}

/// Patch a finished delivery Job's `ttlSecondsAfterFinished`.
///
/// ONLY AFTER THE STATUS WRITE RETURNED 200 — see the module header's rule 2.
async fn set_delivery_ttl(
    client: &kube::Client,
    namespace: &str,
    job_name: &str,
) -> Result<(), ReconcileError> {
    let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
    jobs.patch(
        job_name,
        &PatchParams::default(),
        &Patch::Merge(json!({"spec": {"ttlSecondsAfterFinished": DELIVERY_TTL_SECONDS}})),
    )
    .await
    .map_err(ReconcileError::Api)?;
    Ok(())
}

// ===========================================================================
// Status
// ===========================================================================

/// What [`write_status`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Commit {
    /// The API server accepted the patch.
    Written,
    /// The computed status equals the one on the object; nothing was sent.
    Unchanged,
    /// Something wrote this status between the read and the write.
    Conflicted,
}

impl Commit {
    /// Whether the server's status now says what this pass computed — which is
    /// what the TTL patch's ordering depends on.
    #[must_use]
    pub fn is_committed(self) -> bool {
        matches!(self, Self::Written | Self::Unchanged)
    }
}

/// Assemble the whole status block.
#[must_use]
pub fn build_status(
    policy: &ProtectionPolicy,
    verdict: &p::Verdict,
    alerts: Vec<AlertEntry>,
    now: Time,
    interval: i32,
) -> ProtectionPolicyStatus {
    let stored = policy.status.as_ref();
    let existing = stored.and_then(|s| s.conditions.as_ref());
    let generation = policy.metadata.generation;

    let ready = merge_condition(
        current_condition(existing, CONDITION_READY),
        Condition {
            r#type: CONDITION_READY.to_string(),
            status: "True".to_string(),
            observed_generation: generation,
            last_transition_time: Some(now),
            reason: Some(REASON_EVALUATED.to_string()),
            // `condition_summary` AND NOT `summary` — review F4. A condition
            // `message` is part of the object; the event's sentence carries the
            // concrete age and would make the status differ every time a minute
            // rolled over (erratum E11(d)).
            message: Some(verdict.condition_summary.clone()),
        },
    );
    let protected = merge_condition(
        current_condition(existing, CONDITION_PROTECTED),
        Condition {
            r#type: CONDITION_PROTECTED.to_string(),
            status: verdict.health.condition_status().to_string(),
            observed_generation: generation,
            last_transition_time: Some(now),
            reason: Some(verdict.reason.as_str().to_string()),
            message: Some(verdict.condition_summary.clone()),
        },
    );
    let (delivery_status, delivery_reason, delivery_message) = delivery_condition(&alerts);
    let delivered = merge_condition(
        current_condition(existing, CONDITION_NOTIFICATIONS_DELIVERED),
        Condition {
            r#type: CONDITION_NOTIFICATIONS_DELIVERED.to_string(),
            status: delivery_status.to_string(),
            observed_generation: generation,
            last_transition_time: Some(now),
            reason: Some(delivery_reason.to_string()),
            message: Some(delivery_message),
        },
    );

    let status = ProtectionPolicyStatus {
        observed_generation: generation,
        evaluated_at: None,
        health: Some(verdict.health.as_str().to_string()),
        availability_basis: Some(verdict.basis.as_str().to_string()),
        last_available_point: verdict.point.as_ref().map(p::available_point_status),
        last_attempt: verdict.last_attempt.clone(),
        consecutive_failed_runs: Some(verdict.consecutive_failed_runs),
        missed: (verdict.missed.last_missed_slot.is_some()
            || verdict.missed.since_last_fire.is_some())
        .then(|| verdict.missed.clone()),
        schedules: (!verdict.schedules.is_empty()).then(|| verdict.schedules.clone()),
        rehearsal: verdict.rehearsal.clone(),
        stale_since: stale_since(stored, verdict.health, now),
        alerts: (!alerts.is_empty()).then_some(alerts),
        conditions: Some(vec![ready, protected, delivered]),
    };
    settle_clock_fields(stored, status, interval, now)
}

/// Decide `evaluatedAt` and the two fields measured FROM it, together.
///
/// # D3 §3.1's rule, and the two fields that were outside it (review F4)
///
/// `evaluatedAt` is "rewritten only on change or when older than interval/2".
/// `lastAvailablePoint.ageSeconds` and `missed.sinceLastFire` are both
/// documented as "at `evaluatedAt`" — they are *derived from that instant*, not
/// from `now` — but they were computed from a live clock read, so a pass over
/// unchanged cluster state produced a different status every second: a PATCH,
/// a `resourceVersion` bump, the reconciler's own write waking it again. That
/// is the measured `KafkaCluster` defect (erratum **E11(d)**) on every policy,
/// for ever, and the test that claimed otherwise passed only because it froze
/// the clock.
///
/// All three move together or none of them does. The comparison is over the
/// computed status with all three stripped, so "did the VERDICT change?" is
/// asked about the cluster and not about the clock. When nothing changed and
/// the stored instant is younger than half the interval, the stored values for
/// all three are restored and [`crate::conditions::status_unchanged`] then
/// sends nothing at all.
fn settle_clock_fields(
    stored: Option<&ProtectionPolicyStatus>,
    mut next: ProtectionPolicyStatus,
    interval: i32,
    now: Time,
) -> ProtectionPolicyStatus {
    let fresh = |stored_at: Time| {
        let half = i64::from(interval).max(2) / 2;
        (now - stored_at).num_seconds() < half
    };
    let keep = stored
        .filter(|s| s.evaluated_at.is_some_and(fresh) && verdict_bytes(s) == verdict_bytes(&next));
    match keep {
        Some(stored) => {
            next.evaluated_at = stored.evaluated_at;
            if let (Some(point), Some(stored_point)) = (
                next.last_available_point.as_mut(),
                stored.last_available_point.as_ref(),
            ) {
                point.age_seconds = stored_point.age_seconds;
            }
            if let (Some(missed), Some(stored_missed)) =
                (next.missed.as_mut(), stored.missed.as_ref())
            {
                missed.since_last_fire = stored_missed.since_last_fire;
            }
        }
        None => next.evaluated_at = Some(now),
    }
    next
}

/// `staleSince` — set the first time health becomes `Stale` or `Unprotected`
/// and KEPT while it stays there; cleared when it does not.
///
/// It is a "since when" and not a heartbeat: rewriting it on every pass would
/// make every pass a write, and a reconciler's own status patch is what wakes
/// it (erratum **E11(d)**).
fn stale_since(
    stored: Option<&ProtectionPolicyStatus>,
    health: p::Health,
    now: Time,
) -> Option<Time> {
    if !matches!(health, p::Health::Stale | p::Health::Unprotected) {
        return None;
    }
    stored.and_then(|s| s.stale_since).or(Some(now))
}

/// A status serialized with every CLOCK-DERIVED field removed, for the
/// equality [`settle_clock_fields`] asks.
///
/// The three: `evaluatedAt`, `lastAvailablePoint.ageSeconds` and
/// `missed.sinceLastFire`. Each is a measurement of an instant against a stored
/// fact, so each moves on its own with no change to the cluster; comparing them
/// would make "did the verdict change?" answer "yes" for ever.
fn verdict_bytes(status: &ProtectionPolicyStatus) -> Value {
    let mut value = serde_json::to_value(status).unwrap_or(Value::Null);
    if let Some(point) = value
        .get_mut("lastAvailablePoint")
        .and_then(Value::as_object_mut)
    {
        point.remove("ageSeconds");
    }
    if let Some(missed) = value.get_mut("missed").and_then(Value::as_object_mut) {
        missed.remove("sinceLastFire");
    }
    if let Some(map) = value.as_object_mut() {
        map.remove("evaluatedAt");
    }
    value
}

/// `NotificationsDelivered`, from the ledger alone.
fn delivery_condition(alerts: &[AlertEntry]) -> (&'static str, &'static str, String) {
    let mut pending = 0usize;
    let mut failed = 0usize;
    let mut delivered = 0usize;
    let mut suppressed = 0usize;
    for entry in alerts {
        match entry
            .delivery
            .as_ref()
            .and_then(|d| d.state.as_deref())
            .and_then(p::DeliveryState::parse)
        {
            Some(p::DeliveryState::Pending) => pending += 1,
            Some(p::DeliveryState::Failed) => failed += 1,
            Some(p::DeliveryState::Delivered) => delivered += 1,
            Some(p::DeliveryState::Suppressed) => suppressed += 1,
            None => {}
        }
    }
    if failed > 0 {
        return (
            "False",
            REASON_DELIVERY_FAILED,
            format!(
                "{failed} alert transition(s) were not delivered after {} attempts; this policy's \
                 status is the only thing that records it — no backup result was rewritten",
                p::MAX_DELIVERY_ATTEMPTS
            ),
        );
    }
    if pending > 0 {
        return (
            "Unknown",
            REASON_DELIVERY_PENDING,
            format!("{pending} alert transition(s) have a delivery Job that has not finished"),
        );
    }
    if delivered > 0 {
        return (
            "True",
            REASON_DELIVERED,
            format!("{delivered} alert transition(s) were delivered to every configured sink"),
        );
    }
    (
        "True",
        REASON_NOTHING_TO_DELIVER,
        if suppressed > 0 {
            format!(
                "{suppressed} alert transition(s) matched no configured route; nothing was sent, \
                 which is a configuration choice and not a failure"
            )
        } else {
            "no alert transition needed delivering".to_string()
        },
    )
}

/// Add the optimistic-concurrency precondition to a `/status` merge patch —
/// D-SEAMS **S7**.
///
/// The API server applies a `metadata.resourceVersion` carried in a patch BODY
/// as an update precondition and answers **409** on a mismatch, which is how a
/// merge PATCH gets a compare-and-set without the `update` verb this role
/// grants on nothing.
fn status_patch_with_preconditions(
    policy: &ProtectionPolicy,
    mut patch: Value,
) -> Result<Value, Box<kube::Error>> {
    let name = policy.name_any();
    let resource_version = policy
        .metadata
        .resource_version
        .clone()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            Box::new(kube::Error::Discovery(
                kube::error::DiscoveryError::MissingResource(format!(
                    "ProtectionPolicy {name} carries no metadata.resourceVersion, which a \
                     /status compare-and-set needs (D-SEAMS S7)"
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
    Ok(patch)
}

/// Every `status` key that this pass may need to REMOVE, and therefore must
/// spell as an explicit `null` when it computed `None`.
///
/// # An omitted key is "leave it alone", not "clear it" (review F3)
///
/// Every field of `ProtectionPolicyStatus` is `skip_serializing_if =
/// "Option::is_none"`, and RFC 7386 removes a key only for an explicit `null`.
/// So a status serialized straight to a merge patch could SET each of these and
/// never clear one — and the consequences are the two claims this controller
/// exists to avoid making:
///
/// * `lastAvailablePoint` survived a verdict that just decided no point is
///   available, so the API and the console kept naming a recovery point while
///   `health` read `Unknown` — the exact conflation D3 §3.2 is written against,
///   on the surface an incident responder reads;
/// * `staleSince` from a long-resolved outage sat on a `Healthy` policy,
///   contradicting its own field documentation.
///
/// `conditions` is deliberately NOT here: this controller always writes all
/// three, so a `null` would be a removal that never applies, and an array in a
/// merge patch is replaced wholesale anyway.
///
/// `metadata` is not a status key and never appears in this list;
/// [`status_patch_with_preconditions`] adds it to the body afterwards.
pub const CLEARABLE_STATUS_FIELDS: [&str; 7] = [
    "lastAvailablePoint",
    "lastAttempt",
    "missed",
    "schedules",
    "rehearsal",
    "staleSince",
    "alerts",
];

/// The `{"status": …}` merge-patch body, with an explicit `null` for every
/// clearable field this pass computed as `None`.
///
/// [`crate::conditions::status_unchanged`] applies the body exactly as the API
/// server would, so the no-op skip keeps working: a `null` for a key the object
/// does not have is itself a no-op.
#[must_use]
pub fn status_patch_body(status: &ProtectionPolicyStatus) -> Value {
    let mut body = serde_json::to_value(status).unwrap_or(Value::Null);
    if let Some(map) = body.as_object_mut() {
        for field in CLEARABLE_STATUS_FIELDS {
            map.entry(field.to_string()).or_insert(Value::Null);
        }
    }
    json!({ "status": body })
}

/// Patch `/status`, with the S7 precondition and the no-op skip.
async fn write_status(
    api: &Api<ProtectionPolicy>,
    policy: &ProtectionPolicy,
    status: &ProtectionPolicyStatus,
) -> Result<Commit, ReconcileError> {
    let name = policy.name_any();
    let patch = status_patch_body(status);
    if status_unchanged(
        policy
            .status
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok())
            .as_ref(),
        &patch,
    ) {
        debug!(
            policy = %name,
            "the computed protection status equals the one on the object; no patch is sent"
        );
        return Ok(Commit::Unchanged);
    }
    let body =
        status_patch_with_preconditions(policy, patch).map_err(|e| ReconcileError::Api(*e))?;
    match api
        .patch_status(&name, &PatchParams::default(), &Patch::Merge(body))
        .await
    {
        Ok(_) => Ok(Commit::Written),
        Err(kube::Error::Api(e)) if e.code == 409 => {
            debug!(
                policy = %name,
                "the status changed under this reconcile (409); the next pass reads it"
            );
            Ok(Commit::Conflicted)
        }
        Err(e) => Err(ReconcileError::Api(e)),
    }
}

// ===========================================================================
// Registration
// ===========================================================================

async fn reconcile(
    policy: Arc<ProtectionPolicy>,
    ctx: Arc<ProtectionContext>,
) -> Result<Action, ReconcileError> {
    let outcome = reconcile_policy(&policy, &ctx, chrono::Utc::now()).await?;
    Ok(Action::requeue(std::time::Duration::from_secs(
        outcome.requeue_seconds,
    )))
}

fn error_policy(
    policy: Arc<ProtectionPolicy>,
    err: &ReconcileError,
    _ctx: Arc<ProtectionContext>,
) -> Action {
    warn!(
        policy = %policy.name_any(),
        error = %err,
        "protection policy reconcile failed; requeueing"
    );
    Action::requeue(std::time::Duration::from_secs(ERROR_REQUEUE_SECONDS))
}

/// Run the `ProtectionPolicy` controller until the process ends.
///
/// ALL NAMESPACES (`Api::all`), like every other controller in this directory,
/// and `.owns(jobs, …)` so a finished delivery Job wakes its own policy rather
/// than waiting out the evaluation interval — a page whose result is recorded
/// five minutes late is a page whose retry is five minutes late.
pub fn controller(
    client: kube::Client,
    runner_image: RunnerImage,
) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<ProtectionPolicy> = Api::all(client.clone());
    let jobs: Api<Job> = Api::all(client.clone());
    // THE READ, ONCE PER PROCESS, and the only read of this variable in the
    // crate. It is here rather than in `fn main` because it belongs to this
    // reconciler alone — no other controller creates a delivery Job — and the
    // DECISION is `configured_allow_insecure_sinks`, a pure predicate a test
    // can drive without mutating process-global state.
    let allow_insecure_sinks =
        configured_allow_insecure_sinks(std::env::var(CONTROLLER_ALLOW_INSECURE_SINKS_ENV));
    if allow_insecure_sinks {
        // ONE LINE, AND IT IS A WARNING. An installation that allows cleartext
        // alert delivery should say so in its own logs: the POST carries the
        // protection event, the policy's name and — on Slack, where the URL is
        // the credential — a bearer token, and the operator who set this on a
        // laptop is not always the one reading the controller a month later.
        warn!(
            env = CONTROLLER_ALLOW_INSECURE_SINKS_ENV,
            "this installation ALLOWS INSECURE notification sinks: every delivery Job created \
             here carries NOTIFY_ALLOW_INSECURE_SINKS=1 and may POST a protection event over \
             cleartext http. It is a local-development setting; production leaves it unset"
        );
    }
    let ctx = Arc::new(ProtectionContext {
        client,
        runner_image,
        allow_insecure_sinks,
    });
    async move {
        Controller::new(api, watcher::Config::default())
            .owns(jobs, watcher::Config::default())
            .run(reconcile, error_policy, ctx)
            .for_each(|_| std::future::ready(()))
            .await;
    }
}
