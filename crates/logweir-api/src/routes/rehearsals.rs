//! `GET /api/v1/namespaces/{ns}/rehearsal-schedules[/{name}]` — PLAT-14.3's
//! read half: does the recovery path still work, when was it last proved, and
//! what stopped the last slot.
//!
//! A SKIP IS A RESULT AND IS PUBLISHED AS ONE. `lastSkipped` carries the slot
//! and the reason — `NoQualifyingPoint`, `AuthorizationExpired`,
//! `LeftoverTopics` and the rest — because a rehearsal that quietly stops
//! running looks exactly like a rehearsal that keeps passing if the surface
//! only shows the last success. `cleanup.pendingTopics` is published for the
//! same reason: while it is non-empty every further slot is skipped, and an
//! operator who cannot see it cannot clear it.
//!
//! THE AUTHORIZATION IS NAMED, NEVER CARRIED. `spec.authorization` references
//! an `Approval` by name; the signed standing document, its signatures and the
//! trusted public keys live in that object and reach a caller only through the
//! explicit approval-packet route.

use axum::extract::State;
use axum::response::Response;
use chrono::{DateTime, Utc};
use http::{StatusCode, Uri};
use kube::ResourceExt;
use schemars::JsonSchema;
use serde::Serialize;
use weirkeeper::crds::rehearsal_schedule::RehearsalSchedule;

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
pub const ROUTE_LIST: &str = "GET /api/v1/namespaces/{ns}/rehearsal-schedules";

/// The most pending topics a response carries.
pub const MAX_PENDING_TOPICS: usize = 64;

/// Which point a rehearsal takes.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PointSelectorView {
    /// Candidate schedules.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(default)]
    pub schedule_refs: Vec<NameRef>,
    /// The catalog candidates come from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog_ref: Option<NameRef>,
    /// `NewestAvailable` — the only strategy in v1.
    pub selection: String,
    /// How old a point must be before it qualifies.
    pub min_age_seconds: i32,
    /// The topics the rehearsal restores.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topics: Option<Vec<String>>,
    /// Whether unverified evidence disqualifies a point. CEL pins it `true` in
    /// v1: a rehearsal over unverified evidence proves the archive is readable
    /// and nothing about whether it is trustworthy.
    pub require_verified_evidence: bool,
}

/// Where a rehearsal writes.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RehearsalTargetView {
    /// The target connection.
    pub cluster_ref: NameRef,
    /// The prefix as WRITTEN in the spec. The controller renders
    /// `<prefix><uid-first-8>-` per schedule, which is what teardown's
    /// deletion guard matches.
    pub topic_prefix: String,
    /// The marker topic phase 0 checks.
    pub marker_topic: String,
    /// The replication factor for created topics.
    pub replication_factor: i32,
}

/// The bounds a rehearsal runs inside.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RehearsalBoundsView {
    /// `Forbid` — the only policy in v1.
    pub concurrency_policy: String,
    /// The run deadline.
    pub deadline_seconds: i32,
    /// The missed-slot horizon.
    pub starting_deadline_seconds: i32,
    /// How many records per partition the canary writes.
    pub records_per_partition: i32,
    /// The partition ceiling, enforced when the catalog knows the count.
    pub max_partitions: i32,
}

/// The last rehearsal that passed.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RehearsalSuccessView {
    /// The `Restore` it ran as.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restore_ref: Option<NameRef>,
    /// When.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub at: Option<DateTime<Utc>>,
    /// Which point.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub point_id: Option<String>,
    /// The verification verdict the run recorded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evidence: Option<String>,
    /// The measured recovery time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rto_seconds: Option<i64>,
}

/// The last rehearsal that failed.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RehearsalFailureView {
    /// The `Restore`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restore_ref: Option<NameRef>,
    /// When.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub at: Option<DateTime<Utc>>,
    /// Why.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// A slot that did not run, and why.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SkippedSlotView {
    /// The DUE slot that was refused — never the instant the controller
    /// looked. A skipped slot is consumed and is not fired late.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slot: Option<String>,
    /// The reason, from the closed list.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Topics a previous teardown could not remove. While this is non-empty the
/// next slot is SKIPPED rather than allowed to adopt topics it did not create.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CleanupStateView {
    /// The topics still there.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(default)]
    pub pending_topics: Vec<String>,
    /// Whether the list was cut short.
    pub pending_topics_truncated: bool,
    /// Since when.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub since: Option<DateTime<Utc>>,
}

/// A `RehearsalSchedule`, projected.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RehearsalScheduleView {
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
    /// The five-field UTC cron expression.
    pub schedule: String,
    /// Whether it is suspended — the only mutable field of a sealed spec.
    pub suspend: bool,
    /// The protection policy the results surface on.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protection_policy_ref: Option<NameRef>,
    /// Which point.
    pub point: PointSelectorView,
    /// Where it writes.
    pub target: RehearsalTargetView,
    /// The bounds.
    pub bounds: RehearsalBoundsView,
    /// The requested recovery time objective.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rto_seconds: Option<i64>,
    /// The `Approval` carrying the signed standing authorization. A NAME: the
    /// document and its signatures are reachable only through the approval
    /// packet route.
    pub standing_approval_ref: NameRef,
    /// The approval policy, when one is named.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_policy_ref: Option<NameRef>,
    /// The generation the status was computed from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// The last slot the controller decided: fired, or skipped with
    /// `lastSkipped.slot` naming the same slot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_scheduled_slot: Option<String>,
    /// When it fires next.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_fire_time: Option<DateTime<Utc>>,
    /// The rehearsal running now.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_restore_ref: Option<NameRef>,
    /// The reservation a slot took before it created its child.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_restore_ref: Option<NameRef>,
    /// The last pass.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_succeeded: Option<RehearsalSuccessView>,
    /// The last failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_failed: Option<RehearsalFailureView>,
    /// The last skip. **A skip is not a pass and is never rendered as one.**
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_skipped: Option<SkippedSlotView>,
    /// Leftover topics, which block the next slot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleanup: Option<CleanupStateView>,
    /// `Ready`, `Authorized` and `RehearsalHealthy`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(default)]
    pub conditions: Vec<ConditionView>,
}

fn name_ref(reference: &weirkeeper::crds::LocalRef) -> NameRef {
    NameRef {
        name: reference.name.clone(),
    }
}

/// Project one rehearsal schedule.
#[must_use]
pub fn view(schedule: &RehearsalSchedule) -> RehearsalScheduleView {
    let spec = &schedule.spec;
    let status = schedule.status.as_ref();
    RehearsalScheduleView {
        name: schedule.name_any(),
        namespace: schedule.namespace().unwrap_or_default(),
        uid: schedule.uid().unwrap_or_default(),
        resource_version: schedule.resource_version().unwrap_or_default(),
        generation: schedule.metadata.generation,
        created_at: schedule.metadata.creation_timestamp.as_ref().map(|t| t.0),
        schedule: bounded(&spec.schedule, 128),
        suspend: spec.suspend,
        protection_policy_ref: spec.protection_policy_ref.as_ref().map(name_ref),
        point: PointSelectorView {
            schedule_refs: spec
                .point
                .schedule_refs
                .iter()
                .flatten()
                .take(16)
                .map(name_ref)
                .collect(),
            catalog_ref: spec.point.catalog_ref.as_ref().map(name_ref),
            selection: crate::status::wire_name(&spec.point.selection),
            min_age_seconds: spec.point.min_age_seconds,
            topics: spec
                .point
                .topics
                .as_ref()
                .map(|t| t.iter().take(32).map(|n| bounded(n, 249)).collect()),
            require_verified_evidence: spec.point.require_verified_evidence,
        },
        target: RehearsalTargetView {
            cluster_ref: name_ref(&spec.target.cluster_ref),
            topic_prefix: bounded(&spec.target.topic_prefix, 128),
            marker_topic: bounded(&spec.target.marker_topic, 249),
            replication_factor: spec.target.replication_factor,
        },
        bounds: RehearsalBoundsView {
            concurrency_policy: crate::status::wire_name(&spec.bounds.concurrency_policy),
            deadline_seconds: spec.bounds.deadline_seconds,
            starting_deadline_seconds: spec.bounds.starting_deadline_seconds,
            records_per_partition: spec.bounds.records_per_partition,
            max_partitions: spec.bounds.max_partitions,
        },
        rto_seconds: spec.objectives.as_ref().and_then(|o| o.rto_seconds),
        standing_approval_ref: name_ref(&spec.authorization.standing_approval_ref),
        approval_policy_ref: spec
            .authorization
            .approval_policy_ref
            .as_ref()
            .map(name_ref),
        observed_generation: status.and_then(|s| s.observed_generation),
        last_scheduled_slot: status.and_then(|s| s.last_scheduled_slot.clone()),
        next_fire_time: status.and_then(|s| s.next_fire_time),
        active_restore_ref: status
            .and_then(|s| s.active_restore_ref.as_ref())
            .map(name_ref),
        pending_restore_ref: status
            .and_then(|s| s.pending_restore_ref.as_ref())
            .map(name_ref),
        last_succeeded: status.and_then(|s| s.last_succeeded.as_ref()).map(|v| {
            RehearsalSuccessView {
                restore_ref: v.restore_ref.as_ref().map(name_ref),
                at: v.at,
                point_id: v.point_id.clone(),
                evidence: v.evidence.clone(),
                rto_seconds: v.rto_seconds,
            }
        }),
        last_failed: status
            .and_then(|s| s.last_failed.as_ref())
            .map(|v| RehearsalFailureView {
                restore_ref: v.restore_ref.as_ref().map(name_ref),
                at: v.at,
                reason: v.reason.as_deref().map(|r| bounded(r, 256)),
            }),
        last_skipped: status
            .and_then(|s| s.last_skipped.as_ref())
            .map(|v| SkippedSlotView {
                slot: v.slot.clone(),
                reason: v.reason.as_deref().map(|r| bounded(r, 256)),
            }),
        cleanup: status.and_then(|s| s.cleanup.as_ref()).map(|c| {
            let all = c.pending_topics.as_ref();
            CleanupStateView {
                pending_topics: all
                    .into_iter()
                    .flatten()
                    .take(MAX_PENDING_TOPICS)
                    .map(|t| bounded(t, 249))
                    .collect(),
                pending_topics_truncated: all.is_some_and(|t| t.len() > MAX_PENDING_TOPICS),
                since: c.since,
            }
        }),
        conditions: status
            .and_then(|s| s.conditions.as_ref())
            .into_iter()
            .flatten()
            .take(crate::status::MAX_CONDITIONS)
            .map(condition_view)
            .collect(),
    }
}

/// A page of rehearsal schedules.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RehearsalScheduleList {
    /// The request ID.
    pub request_id: String,
    /// The items on this page.
    pub items: Vec<RehearsalScheduleView>,
    /// Paging.
    pub page: Page,
}

/// One rehearsal schedule.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RehearsalScheduleResponse {
    /// The request ID.
    pub request_id: String,
    /// The item.
    pub item: RehearsalScheduleView,
}

/// `GET .../rehearsal-schedules`.
pub async fn list(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(ns): ApiPath<String>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadRehearsalSchedules)?;
    let query = list_query(uri.query())?;
    let (items, page) =
        list_page::<RehearsalSchedule>(&state, &actor, &ns, ROUTE_LIST, &query).await?;
    Ok(json(
        StatusCode::OK,
        &RehearsalScheduleList {
            request_id,
            items: items.iter().map(view).collect(),
            page,
        },
    ))
}

/// `GET .../rehearsal-schedules/{name}`.
pub async fn get_one(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, name)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    crate::http::parse_query(uri.query(), &[])?;
    authorize(&state, &actor, &ns, Action::ReadRehearsalSchedules)?;
    let object = get_object::<RehearsalSchedule>(&state, &actor, &ns, &name).await?;
    Ok(json(
        StatusCode::OK,
        &RehearsalScheduleResponse {
            request_id,
            item: view(&object),
        },
    ))
}
