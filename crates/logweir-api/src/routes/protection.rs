//! `GET /api/v1/namespaces/{ns}/protection-policies[/{name}]` — PLAT-14.2's
//! read half: is this data protected, by when, and who was told when it was
//! not.
//!
//! THE THREE THINGS THIS PAGE EXISTS TO KEEP APART. "A schedule is enabled",
//! "a run succeeded" and "there is a recovery point I could actually restore
//! from" are three different claims, and collapsing them is the defect
//! PLAT-14.2 names. So the projection publishes `health` beside
//! `availabilityBasis` (how availability was decided), the schedules' OWN
//! readiness beside the objective, and `lastAvailablePoint` — the newest point
//! that is available, which is not the same object as `lastAttempt`.
//!
//! NO SINK, NO ROUTING KEY, NO URL. `spec.notifications.routes[]` carries
//! `secretKeyRef`s and nothing else; this projection carries each route's NAME
//! and which CHANNELS it has configured, as booleans. A webhook URL is a
//! bearer token with a hostname on the front (the CRD says so), and a Secret
//! NAME beside it would tell a reader which Secret to go and read. The alert
//! ledger's `delivery.lastError` is already redacted and bounded by the
//! controller and is bounded again here.

use axum::extract::State;
use axum::response::Response;
use chrono::{DateTime, Utc};
use http::{StatusCode, Uri};
use kube::ResourceExt;
use schemars::JsonSchema;
use serde::Serialize;
use weirkeeper::crds::protection_policy::{
    AlertEntry, AvailablePoint, LastAttempt, ProtectionPolicy, ScheduleSummary,
};

use super::{authorize, get_object, json, list_page, list_query, ApiPath};
use crate::app::AppState;
use crate::auth::Actor;
use crate::authz::Action;
use crate::contract::{ConditionView, NameRef, Page};
use crate::http::RequestId;
use crate::problem::ApiError;
use crate::status::condition_view;
use crate::validate::bounded;

/// The cursor scope's route identifier.
pub const ROUTE_LIST: &str = "GET /api/v1/namespaces/{ns}/protection-policies";

/// The most alerts, schedules and topics a response carries.
pub const MAX_ROWS: usize = 16;
/// The most topics one point's list carries.
pub const MAX_TOPICS: usize = 64;

/// What the policy protects, as references.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProtectedSubjectView {
    /// The source connection.
    pub source_ref: NameRef,
    /// The topics the objective is about. Absent means "the topics of the
    /// newest matching point", which is the honest reading of a schedule that
    /// selects dynamically — never "all of them".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topics: Option<Vec<String>>,
    /// The schedules whose runs count.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub schedule_refs: Vec<NameRef>,
    /// The saved destination, when there is one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination_ref: Option<NameRef>,
    /// The catalog that answers "is the point still there?".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog_ref: Option<NameRef>,
}

/// What "protected" means here.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ObjectivesView {
    /// How old the newest recovery point may be, measured from CAPTURE START.
    pub max_recovery_point_age_seconds: i32,
    /// How many consecutive failed runs are tolerated.
    pub max_consecutive_failed_runs: i32,
    /// Whether unverified evidence counts as protection.
    pub require_verified_evidence: bool,
    /// Whether the catalog must say the point is still in storage.
    pub require_catalog_availability: bool,
    /// How old the newest successful rehearsal may be.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_rehearsal_age_seconds: Option<i32>,
}

/// One configured alert destination — its name and which channels it has.
///
/// THE CHANNELS ARE BOOLEANS ON PURPOSE. A PagerDuty routing key, a webhook
/// URL and a Slack webhook URL are each a credential; so is the name of the
/// Secret that holds one, to a reader deciding what to try to read next.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NotificationRouteView {
    /// What status and logs call it.
    pub name: String,
    /// Whether a PagerDuty channel is configured.
    pub pager_duty: bool,
    /// Whether a webhook channel is configured.
    pub webhook: bool,
    /// Whether a Slack channel is configured.
    pub slack: bool,
}

/// Where alerts go, and which ones.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NotificationsView {
    /// The routes, at most four.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub routes: Vec<NotificationRouteView>,
    /// Which alert kinds are sent. Absent means all of them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kinds: Option<Vec<String>>,
    /// Whether a resolution is delivered too.
    pub send_resolved: bool,
    /// How long before an open alert is re-sent; `0` disables re-notify.
    pub renotify_after_seconds: i32,
}

/// The newest point this policy can actually recover from.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AvailablePointView {
    /// The durable point identity, when the catalog supplied one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub point_id: Option<String>,
    /// The `Backup` object, while one still exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backup_ref: Option<NameRef>,
    /// **Capture start** — what the objective is measured from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery_point_at: Option<DateTime<Utc>>,
    /// The newest record the point contains. A DIFFERENT NUMBER, published
    /// beside the one above so nobody reads an idle topic as stale protection.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub newest_record_at: Option<DateTime<Utc>>,
    /// How old the point was when the objective was evaluated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age_seconds: Option<i64>,
    /// `Valid`, `ValidHistorical`, `Untrusted` or `NotAttempted`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evidence: Option<String>,
    /// The topics it covers, bounded.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub topics: Vec<String>,
    /// Whether `topics` was cut short.
    pub topics_truncated: bool,
}

/// The most recent run, whatever became of it.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LastAttemptView {
    /// The `Backup`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backup_ref: Option<NameRef>,
    /// Its phase.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// Its reason.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// When.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub at: Option<DateTime<Utc>>,
}

/// A schedule as this policy sees it — rendered BESIDE protection health and
/// never folded into it.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleHealthView {
    /// The schedule's name.
    pub name: String,
    /// Whether it is suspended — the commonest cause of staleness.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suspended: Option<bool>,
    /// Its own `Ready` status.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready: Option<String>,
    /// When it fires next.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_fire_time: Option<DateTime<Utc>>,
    /// The last slot it missed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_missed_slot: Option<String>,
}

/// How a transition was delivered, or why it was not.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AlertDeliveryView {
    /// `Pending`, `Delivered`, `Failed` or `Suppressed`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    /// How many attempts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempts: Option<i64>,
    /// When the last one was.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_attempt_at: Option<DateTime<Utc>>,
    /// The last error, redacted by the controller and bounded again here.
    /// Never a routing key and never a URL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// One entry of the deduplication ledger.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AlertView {
    /// The stable dedup key.
    pub key: String,
    /// What it is about.
    pub kind: String,
    /// `Open` or `Resolved`.
    pub state: String,
    /// When it opened.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub opened_at: Option<DateTime<Utc>>,
    /// When it resolved.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_at: Option<DateTime<Utc>>,
    /// The transition counter: it increments on open, on resolve and on each
    /// re-notify.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transition: Option<i64>,
    /// The last transition a delivery was created for. `transition` ahead of
    /// this one is an alert whose latest state has not been delivered.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notified_transition: Option<i64>,
    /// How that delivery went.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery: Option<AlertDeliveryView>,
}

/// A `ProtectionPolicy`, projected.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProtectionPolicyView {
    /// The object name.
    pub name: String,
    /// The namespace.
    pub namespace: String,
    /// The Kubernetes UID.
    pub uid: String,
    /// The resourceVersion this projection was read at.
    pub resource_version: String,
    /// `metadata.generation`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<i64>,
    /// When the object was created.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    /// What it protects.
    pub protects: ProtectedSubjectView,
    /// What "protected" means.
    pub objectives: ObjectivesView,
    /// Where alerts go. Absent means the verdict is reported here and
    /// delivered nowhere — a real choice, not a broken configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notifications: Option<NotificationsView>,
    /// How often it is evaluated.
    pub evaluation_interval_seconds: i32,
    /// The generation the verdict was computed from. Different from
    /// `generation` means the verdict predates the current objective.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// When the verdict was reached.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evaluated_at: Option<DateTime<Utc>>,
    /// `Healthy`, `AtRisk`, `Stale`, `Unprotected` or `Unknown`. Absent means
    /// nothing has been evaluated yet, which is not `Healthy`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health: Option<String>,
    /// `KubernetesStatus`, `Catalog` or `CatalogStale` — how availability was
    /// decided. "The point exists" and "a Backup object says it succeeded" are
    /// different claims.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub availability_basis: Option<String>,
    /// The newest point that can actually be recovered from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_available_point: Option<AvailablePointView>,
    /// The most recent run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_attempt: Option<LastAttemptView>,
    /// How many runs have failed in a row.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consecutive_failed_runs: Option<i64>,
    /// The last slot that did not run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_missed_slot: Option<String>,
    /// Seconds since the last successful fire.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub since_last_fire: Option<i64>,
    /// Since when the objective has been missed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stale_since: Option<DateTime<Utc>>,
    /// The schedules, with their own readiness.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub schedules: Vec<ScheduleHealthView>,
    /// When a rehearsal last passed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rehearsal_last_succeeded_at: Option<DateTime<Utc>>,
    /// When one last failed, and why.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rehearsal_last_failed_at: Option<DateTime<Utc>>,
    /// The reason the last rehearsal failed or was skipped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rehearsal_last_reason: Option<String>,
    /// The deduplication ledger.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub alerts: Vec<AlertView>,
    /// `Ready`, `Protected` and `NotificationsDelivered`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<ConditionView>,
}

fn name_ref(reference: &weirkeeper::crds::LocalRef) -> NameRef {
    NameRef {
        name: reference.name.clone(),
    }
}

fn point_view(point: &AvailablePoint) -> AvailablePointView {
    let topics: Vec<String> = point
        .topics
        .iter()
        .flatten()
        .take(MAX_TOPICS)
        .map(|t| bounded(t, 249))
        .collect();
    AvailablePointView {
        point_id: point.point_id.clone(),
        backup_ref: point.backup_ref.as_ref().map(name_ref),
        recovery_point_at: point.recovery_point_at,
        newest_record_at: point.newest_record_at,
        age_seconds: point.age_seconds,
        evidence: point.evidence.clone(),
        // The controller's own flag OR our own truncation; either way the
        // reader is told the list is not the whole list.
        topics_truncated: point.topics_truncated.unwrap_or(false)
            || point.topics.as_ref().is_some_and(|t| t.len() > MAX_TOPICS),
        topics,
    }
}

fn attempt_view(attempt: &LastAttempt) -> LastAttemptView {
    LastAttemptView {
        backup_ref: attempt.backup_ref.as_ref().map(name_ref),
        phase: attempt.phase.clone(),
        reason: attempt.reason.clone(),
        at: attempt.at,
    }
}

fn schedule_view(summary: &ScheduleSummary) -> ScheduleHealthView {
    ScheduleHealthView {
        name: summary.name.clone(),
        suspended: summary.suspended,
        ready: summary.ready.clone(),
        next_fire_time: summary.next_fire_time,
        last_missed_slot: summary.last_missed_slot.clone(),
    }
}

fn alert_view(alert: &AlertEntry) -> AlertView {
    AlertView {
        key: bounded(&alert.key, 253),
        kind: crate::status::wire_name(&alert.kind),
        state: bounded(&alert.state, 32),
        opened_at: alert.opened_at,
        resolved_at: alert.resolved_at,
        transition: alert.transition,
        notified_transition: alert.notified_transition,
        delivery: alert.delivery.as_ref().map(|d| AlertDeliveryView {
            state: d.state.clone(),
            attempts: d.attempts,
            last_attempt_at: d.last_attempt_at,
            last_error: d.last_error.as_deref().map(|e| bounded(e, 512)),
        }),
    }
}

/// Project one policy.
#[must_use]
pub fn view(policy: &ProtectionPolicy) -> ProtectionPolicyView {
    let spec = &policy.spec;
    let status = policy.status.as_ref();
    ProtectionPolicyView {
        name: policy.name_any(),
        namespace: policy.namespace().unwrap_or_default(),
        uid: policy.uid().unwrap_or_default(),
        resource_version: policy.resource_version().unwrap_or_default(),
        generation: policy.metadata.generation,
        created_at: policy.metadata.creation_timestamp.as_ref().map(|t| t.0),
        protects: ProtectedSubjectView {
            source_ref: name_ref(&spec.protects.source_ref),
            topics: spec
                .protects
                .topics
                .as_ref()
                .map(|t| t.iter().take(256).map(|n| bounded(n, 249)).collect()),
            schedule_refs: spec
                .protects
                .schedule_refs
                .iter()
                .flatten()
                .take(MAX_ROWS)
                .map(name_ref)
                .collect(),
            destination_ref: spec.protects.destination_ref.as_ref().map(name_ref),
            catalog_ref: spec.protects.catalog_ref.as_ref().map(name_ref),
        },
        objectives: ObjectivesView {
            max_recovery_point_age_seconds: spec.objectives.max_recovery_point_age_seconds,
            max_consecutive_failed_runs: spec.objectives.max_consecutive_failed_runs,
            require_verified_evidence: spec.objectives.require_verified_evidence,
            require_catalog_availability: spec.objectives.require_catalog_availability,
            max_rehearsal_age_seconds: spec.objectives.max_rehearsal_age_seconds,
        },
        notifications: spec.notifications.as_ref().map(|n| NotificationsView {
            routes: n
                .routes
                .iter()
                .flatten()
                .take(4)
                .map(|r| NotificationRouteView {
                    name: bounded(&r.name, 63),
                    pager_duty: r.pager_duty.is_some(),
                    webhook: r.webhook.is_some(),
                    slack: r.slack.is_some(),
                })
                .collect(),
            kinds: n
                .kinds
                .as_ref()
                .map(|k| k.iter().map(crate::status::wire_name).collect()),
            send_resolved: n.send_resolved,
            renotify_after_seconds: n.renotify_after_seconds,
        }),
        evaluation_interval_seconds: spec.evaluation_interval_seconds,
        observed_generation: status.and_then(|s| s.observed_generation),
        evaluated_at: status.and_then(|s| s.evaluated_at),
        health: status.and_then(|s| s.health.clone()),
        availability_basis: status.and_then(|s| s.availability_basis.clone()),
        last_available_point: status
            .and_then(|s| s.last_available_point.as_ref())
            .map(point_view),
        last_attempt: status
            .and_then(|s| s.last_attempt.as_ref())
            .map(attempt_view),
        consecutive_failed_runs: status.and_then(|s| s.consecutive_failed_runs),
        last_missed_slot: status
            .and_then(|s| s.missed.as_ref())
            .and_then(|m| m.last_missed_slot.clone()),
        since_last_fire: status
            .and_then(|s| s.missed.as_ref())
            .and_then(|m| m.since_last_fire),
        stale_since: status.and_then(|s| s.stale_since),
        schedules: status
            .and_then(|s| s.schedules.as_ref())
            .into_iter()
            .flatten()
            .take(MAX_ROWS)
            .map(schedule_view)
            .collect(),
        rehearsal_last_succeeded_at: status
            .and_then(|s| s.rehearsal.as_ref())
            .and_then(|r| r.last_succeeded_at),
        rehearsal_last_failed_at: status
            .and_then(|s| s.rehearsal.as_ref())
            .and_then(|r| r.last_failed_at),
        rehearsal_last_reason: status
            .and_then(|s| s.rehearsal.as_ref())
            .and_then(|r| r.last_reason.clone()),
        alerts: status
            .and_then(|s| s.alerts.as_ref())
            .into_iter()
            .flatten()
            .take(MAX_ROWS)
            .map(alert_view)
            .collect(),
        conditions: status
            .and_then(|s| s.conditions.as_ref())
            .into_iter()
            .flatten()
            .take(crate::status::MAX_CONDITIONS)
            .map(condition_view)
            .collect(),
    }
}

/// A page of protection policies.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProtectionPolicyList {
    /// The request ID.
    pub request_id: String,
    /// The items on this page.
    pub items: Vec<ProtectionPolicyView>,
    /// Paging.
    pub page: Page,
}

/// One protection policy.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProtectionPolicyResponse {
    /// The request ID.
    pub request_id: String,
    /// The item.
    pub item: ProtectionPolicyView,
}

/// `GET .../protection-policies`.
pub async fn list(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(ns): ApiPath<String>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadProtectionPolicies)?;
    let query = list_query(uri.query())?;
    let (items, page) =
        list_page::<ProtectionPolicy>(&state, &actor, &ns, ROUTE_LIST, &query).await?;
    Ok(json(
        StatusCode::OK,
        &ProtectionPolicyList {
            request_id,
            items: items.iter().map(view).collect(),
            page,
        },
    ))
}

/// `GET .../protection-policies/{name}`.
pub async fn get_one(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, name)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    crate::http::parse_query(uri.query(), &[])?;
    authorize(&state, &actor, &ns, Action::ReadProtectionPolicies)?;
    let object = get_object::<ProtectionPolicy>(&state, &actor, &ns, &name).await?;
    Ok(json(
        StatusCode::OK,
        &ProtectionPolicyResponse {
            request_id,
            item: view(&object),
        },
    ))
}
