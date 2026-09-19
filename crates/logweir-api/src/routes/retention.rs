//! `GET /api/v1/namespaces/{ns}/retention-policies[/{name}]` — PLAT-16.1 and
//! PLAT-16.2's read half: what would be removed, what is actually enforcing
//! it, and whether the plan an administrator approved is still the plan.
//!
//! WHAT IS ENFORCING IS NOT WHAT IS CONFIGURED. `status.enforcement` is
//! `RecommendationOnly`, `LogweirWorker` or `ExternalLifecycleDeclared`, and
//! `status.guarantees` says, per guarantee, whether Logweir enforces it, a
//! provider is *declared* to (unverified), or nothing does. A console that
//! printed the spec's `mode` alone would tell an operator their points are
//! protected by a bucket lifecycle rule Logweir has never read.
//!
//! THE APPROVED-PLAN STATE IS DERIVED HERE, ONCE. "Is there a plan", "does the
//! approved digest match it" and "is it still young enough" are three facts on
//! two objects, and a surface that recombined them itself would be a second
//! implementation of the rule that decides whether a deletion Job may be
//! created. [`ApprovedPlanState`] is that rule, and it is `Unknown` — never
//! `Approved` — whenever anything it needs is absent (D3 §12).
//!
//! NO CREDENTIAL, NOT EVEN ITS NAME. `spec.enforcement.credentialSecretRef`
//! names the one delete-capable credential in the installation; this
//! projection publishes `credentialConfigured: true` and not the Secret's
//! name, because a name is what a reader needs to decide what to try next.

use axum::extract::State;
use axum::response::Response;
use chrono::{DateTime, Utc};
use http::{StatusCode, Uri};
use kube::ResourceExt;
use schemars::JsonSchema;
use serde::Serialize;
use weirkeeper::crds::retention_policy::{RetentionMode, RetentionPolicy};

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
pub const ROUTE_LIST: &str = "GET /api/v1/namespaces/{ns}/retention-policies";

/// The most rows of each evaluation list a response carries.
pub const MAX_ROWS: usize = 200;

/// Where the approved-plan gate stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ApprovedPlanState {
    /// The policy does not enforce, so there is nothing to approve.
    NotApplicable,
    /// `requireApprovedPlan: false`. Deletion runs unattended, and the
    /// controller records `Enforced/UnattendedDeletionEnabled` so the choice
    /// is visible on the object rather than only in a values file.
    NotRequired,
    /// Enforcement is configured and no evaluation has produced a plan yet.
    NoPlan,
    /// A plan exists and no approved digest matches it.
    AwaitingApproval,
    /// The approved digest matches the current plan and the plan is young
    /// enough.
    Approved,
    /// The approved digest matches and the plan has aged out; the next
    /// evaluation replaces it and it must be approved again.
    Expired,
    /// Something the rule needs is absent. **Never `Approved`.**
    Unknown,
}

/// One point the evaluation would remove.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CandidateView {
    /// The point.
    pub point_id: String,
    /// Which rule made it a candidate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Its capture start.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery_point_at: Option<DateTime<Utc>>,
    /// How many objects it holds, when the view knows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub objects: Option<i64>,
    /// How many bytes, when the view knows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<i64>,
}

/// One point the rules selected and something else protected.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProtectedPointView {
    /// The point.
    pub point_id: String,
    /// `ActiveRestore`, `MinUsablePoints`, `LegalHold`, `SharedSegment`,
    /// `Hold` or `Unknown`.
    pub reason: String,
}

/// One point or key the evaluation could not classify. **Unknown is
/// retained**: nothing here is ever a deletion candidate.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SkippedEntryView {
    /// The point, when one was identified.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub point_id: Option<String>,
    /// The object key, when the point was not.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// `Unreadable`, `UnsupportedFormat` or `Conflict`.
    pub reason: String,
}

/// What the last evaluation found. **Nothing here was deleted.**
///
/// THE NAME CARRIES ITS DOMAIN BECAUSE THE SCHEMA NAME IS GLOBAL. `schemars`
/// keys `components/schemas` by the type's SHORT name, so two `EvaluationView`
/// types in two route modules silently become one schema and one of the two
/// `$ref`s points at the wrong shape — with no error anywhere. `trust.rs` has
/// its own evaluation view, and
/// `no_two_published_types_share_a_schema_name` in `tests/contract.rs` is what
/// keeps the pair honest.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RetentionEvaluationView {
    /// When.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub at: Option<DateTime<Utc>>,
    /// How many points were considered.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub points_evaluated: Option<i64>,
    /// How many candidates there are in total, which may exceed the rows
    /// below.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub candidate_count: Option<i64>,
    /// The points the rules keep.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub kept: Vec<String>,
    /// The points the rules would remove.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub candidates: Vec<CandidateView>,
    /// The points something else protected.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub protected: Vec<ProtectedPointView>,
    /// The points the evaluation could not classify.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub skipped: Vec<SkippedEntryView>,
    /// Whether any of the four lists above was cut short by this route's own
    /// row bound.
    pub truncated: bool,
    /// The plan `ConfigMap`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_ref: Option<NameRef>,
    /// The digest an administrator approves. A digest, not a credential.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_sha256: Option<String>,
    /// When the plan ages out.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_expires_at: Option<DateTime<Utc>>,
}

/// One point a run could not delete.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FailedDeletionView {
    /// The point.
    pub point_id: String,
    /// The refusal code — `AccessDenied`, `Locked`, `PreconditionFailed` and
    /// the rest are NOT retried and keep the point.
    pub code: String,
}

/// What the last enforcement run actually did.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EnforcementRunView {
    /// The run id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// When it started.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    /// When it finished. Absent while a run is in flight.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    /// The plan digest it executed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_sha256: Option<String>,
    /// How many points it deleted.
    pub deleted: i64,
    /// The ones it could not.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub failed: Vec<FailedDeletionView>,
    /// How many objects went.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub objects_deleted: Option<i64>,
    /// Where the attributable record is. **Create-only and UNSIGNED**, verified
    /// by the digest beside it; no surface calls it signed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record_key: Option<String>,
    /// That record's digest.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record_sha256: Option<String>,
    /// The worker's exit code.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
}

/// Which guarantees are in force, and by whom.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GuaranteesView {
    /// Age-based expiry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age_expiry: Option<String>,
    /// The floor on usable points.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_usable_points: Option<String>,
    /// Protection of a point an active restore needs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_restore_protection: Option<String>,
    /// Protection of a segment two manifests share.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shared_segments: Option<String>,
    /// Legal hold.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub legal_hold: Option<String>,
}

/// The declared, unverified provider lifecycle rule.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ExternalLifecycleView {
    /// `s3`, `gcs` or `azure`.
    pub provider: String,
    /// The rule id, as the operator declared it. **Logweir has not read it.**
    pub rule_id: String,
    /// The declared expiry.
    pub expiration_days: i32,
    /// The declared prefix.
    pub prefix: String,
}

/// The settings that make `Enforce` bounded and attributable — without the
/// credential.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EnforcementSettingsView {
    /// Whether a delete-capable credential is configured. A boolean, never a
    /// name.
    pub credential_configured: bool,
    /// The evaluation/enforcement cadence.
    pub schedule: String,
    /// Whether a plan digest must be approved before any Job is created.
    pub require_approved_plan: bool,
    /// The digest an administrator approved, when there is one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approved_plan_sha256: Option<String>,
    /// How long an approved plan stays fresh.
    pub plan_max_age_seconds: i32,
    /// The per-run point ceiling.
    pub max_deletions_per_run: i32,
    /// The per-run object ceiling.
    pub max_objects_per_run: i32,
    /// The run deadline.
    pub deadline_seconds: i32,
}

/// A `RetentionPolicy`, projected.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RetentionPolicyView {
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
    /// The destination it governs.
    pub destination_ref: NameRef,
    /// The catalog it evaluates.
    pub catalog_ref: NameRef,
    /// The only prefix deletion may ever touch.
    pub scope_prefix: String,
    /// `Report`, `Enforce` or `ExternalLifecycle`, as configured.
    pub mode: String,
    /// How many points the rules keep by rank.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keep_last: Option<i32>,
    /// How many days they keep by age.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keep_days: Option<i32>,
    /// The floor, which the rules can never go under.
    pub min_usable_points: i32,
    /// How many holds are in force.
    pub holds: i64,
    /// The declared bucket lifecycle rule, when the mode declares one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_lifecycle: Option<ExternalLifecycleView>,
    /// The enforcement settings, when the mode has them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enforcement_settings: Option<EnforcementSettingsView>,
    /// The generation the status was computed from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// **What is actually enforcing**: `RecommendationOnly`, `LogweirWorker`
    /// or `ExternalLifecycleDeclared`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enforcement: Option<String>,
    /// Which guarantees are in force, and by whom.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guarantees: Option<GuaranteesView>,
    /// Where the approved-plan gate stands.
    pub approved_plan_state: ApprovedPlanState,
    /// The last evaluation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_evaluation: Option<RetentionEvaluationView>,
    /// The last enforcement run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_enforcement: Option<EnforcementRunView>,
    /// How many points a run has claimed right now. A non-zero lease is why a
    /// restore of one of them holds with `PointRetentionInProgress`.
    pub leased_points: i64,
    /// How many runs have failed in a row.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consecutive_run_failures: Option<i64>,
    /// Whether `EnforcementDegraded` is `True`: three consecutive failed runs
    /// stop scheduling until the spec changes.
    pub enforcement_degraded: bool,
    /// `Ready`, `Evaluated`, `Enforced`, `ExternalLifecycleConflict` and
    /// `EnforcementDegraded`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<ConditionView>,
}

/// D3 §6.5's two-step approval gate, in one place.
///
/// EVERY ABSENT INPUT IS `Unknown`, AND `Unknown` IS NOT `Approved`. The gate
/// decides whether a deletion Job may exist; a projection that answered
/// "approved" because it could not find the plan digest would be the most
/// expensive possible reading of D3 §12's absent-field rule.
#[must_use]
pub fn approved_plan_state(policy: &RetentionPolicy, now: DateTime<Utc>) -> ApprovedPlanState {
    if policy.spec.mode != RetentionMode::Enforce {
        return ApprovedPlanState::NotApplicable;
    }
    let Some(enforcement) = policy.spec.enforcement.as_ref() else {
        // `Enforce` without an `enforcement` block is refused by CEL K2, so
        // this is an object written before that rule or by a client talking to
        // a different CRD. It is not approved, and it is not a plan either.
        return ApprovedPlanState::Unknown;
    };
    if !enforcement.require_approved_plan {
        return ApprovedPlanState::NotRequired;
    }
    let evaluation = policy
        .status
        .as_ref()
        .and_then(|s| s.last_evaluation.as_ref());
    let Some(plan) = evaluation.and_then(|e| e.plan_sha256.as_deref()) else {
        return ApprovedPlanState::NoPlan;
    };
    let Some(approved) = enforcement.approved_plan_sha256.as_deref() else {
        return ApprovedPlanState::AwaitingApproval;
    };
    if approved != plan {
        return ApprovedPlanState::AwaitingApproval;
    }
    match evaluation.and_then(|e| e.plan_expires_at) {
        None => ApprovedPlanState::Unknown,
        Some(expires) if now >= expires => ApprovedPlanState::Expired,
        Some(_) => ApprovedPlanState::Approved,
    }
}

fn condition_is_true(policy: &RetentionPolicy, type_: &str) -> bool {
    policy
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .into_iter()
        .flatten()
        .any(|c| c.r#type == type_ && c.status == "True")
}

/// Project one retention policy at `now`.
#[must_use]
pub fn view(policy: &RetentionPolicy, now: DateTime<Utc>) -> RetentionPolicyView {
    let spec = &policy.spec;
    let status = policy.status.as_ref();
    let evaluation = status.and_then(|s| s.last_evaluation.as_ref());
    let truncated = evaluation.is_some_and(|e| {
        e.kept.as_ref().is_some_and(|v| v.len() > MAX_ROWS)
            || e.candidates.as_ref().is_some_and(|v| v.len() > MAX_ROWS)
            || e.protected.as_ref().is_some_and(|v| v.len() > MAX_ROWS)
            || e.skipped.as_ref().is_some_and(|v| v.len() > MAX_ROWS)
    });
    RetentionPolicyView {
        name: policy.name_any(),
        namespace: policy.namespace().unwrap_or_default(),
        uid: policy.uid().unwrap_or_default(),
        resource_version: policy.resource_version().unwrap_or_default(),
        generation: policy.metadata.generation,
        created_at: policy.metadata.creation_timestamp.as_ref().map(|t| t.0),
        destination_ref: NameRef {
            name: spec.destination_ref.name.clone(),
        },
        catalog_ref: NameRef {
            name: spec.catalog_ref.name.clone(),
        },
        scope_prefix: bounded(&spec.scope.prefix, 512),
        mode: crate::status::wire_name(&spec.mode),
        keep_last: spec.rules.keep_last,
        keep_days: spec.rules.keep_days,
        min_usable_points: spec.rules.min_usable_points,
        holds: spec.holds.as_ref().map_or(0, Vec::len) as i64,
        external_lifecycle: spec
            .external_lifecycle
            .as_ref()
            .map(|e| ExternalLifecycleView {
                provider: crate::status::wire_name(&e.provider),
                rule_id: bounded(&e.rule_id, 256),
                expiration_days: e.expiration_days,
                prefix: bounded(&e.prefix, 512),
            }),
        enforcement_settings: spec.enforcement.as_ref().map(|e| EnforcementSettingsView {
            credential_configured: true,
            schedule: bounded(&e.schedule, 128),
            require_approved_plan: e.require_approved_plan,
            approved_plan_sha256: e.approved_plan_sha256.clone(),
            plan_max_age_seconds: e.plan_max_age_seconds,
            max_deletions_per_run: e.max_deletions_per_run,
            max_objects_per_run: e.max_objects_per_run,
            deadline_seconds: e.deadline_seconds,
        }),
        observed_generation: status.and_then(|s| s.observed_generation),
        enforcement: status.and_then(|s| s.enforcement.clone()),
        guarantees: status
            .and_then(|s| s.guarantees.as_ref())
            .map(|g| GuaranteesView {
                age_expiry: g.age_expiry.clone(),
                min_usable_points: g.min_usable_points.clone(),
                active_restore_protection: g.active_restore_protection.clone(),
                shared_segments: g.shared_segments.clone(),
                legal_hold: g.legal_hold.clone(),
            }),
        approved_plan_state: approved_plan_state(policy, now),
        last_evaluation: evaluation.map(|e| RetentionEvaluationView {
            at: e.at,
            points_evaluated: e.points_evaluated,
            candidate_count: e.candidate_count,
            kept: e
                .kept
                .iter()
                .flatten()
                .take(MAX_ROWS)
                .map(|p| bounded(p, 128))
                .collect(),
            candidates: e
                .candidates
                .iter()
                .flatten()
                .take(MAX_ROWS)
                .map(|c| CandidateView {
                    point_id: bounded(&c.point_id, 128),
                    reason: c.reason.clone(),
                    recovery_point_at: c.recovery_point_at,
                    objects: c.objects,
                    bytes: c.bytes,
                })
                .collect(),
            protected: e
                .protected
                .iter()
                .flatten()
                .take(MAX_ROWS)
                .map(|p| ProtectedPointView {
                    point_id: bounded(&p.point_id, 128),
                    reason: bounded(&p.reason, 64),
                })
                .collect(),
            skipped: e
                .skipped
                .iter()
                .flatten()
                .take(MAX_ROWS)
                .map(|s| SkippedEntryView {
                    point_id: s.point_id.as_deref().map(|p| bounded(p, 128)),
                    key: s.key.as_deref().map(|k| bounded(k, 512)),
                    reason: bounded(&s.reason, 64),
                })
                .collect(),
            truncated,
            plan_ref: e.plan_ref.as_ref().map(|r| NameRef {
                name: r.name.clone(),
            }),
            plan_sha256: e.plan_sha256.clone(),
            plan_expires_at: e.plan_expires_at,
        }),
        last_enforcement: status.and_then(|s| s.last_enforcement.as_ref()).map(|r| {
            EnforcementRunView {
                run_id: r.run_id.clone(),
                started_at: r.started_at,
                finished_at: r.finished_at,
                plan_sha256: r.plan_sha256.clone(),
                deleted: r.deleted.as_ref().map_or(0, Vec::len) as i64,
                failed: r
                    .failed
                    .iter()
                    .flatten()
                    .take(MAX_ROWS)
                    .map(|f| FailedDeletionView {
                        point_id: bounded(&f.point_id, 128),
                        code: bounded(&f.code, 64),
                    })
                    .collect(),
                objects_deleted: r.objects_deleted,
                record_key: r.record_key.clone(),
                record_sha256: r.record_sha256.clone(),
                exit_code: r.exit_code,
            }
        }),
        leased_points: status
            .and_then(|s| s.lease.as_ref())
            .and_then(|l| l.point_ids.as_ref())
            .map_or(0, Vec::len) as i64,
        consecutive_run_failures: status.and_then(|s| s.consecutive_run_failures),
        enforcement_degraded: condition_is_true(policy, "EnforcementDegraded"),
        conditions: status
            .and_then(|s| s.conditions.as_ref())
            .into_iter()
            .flatten()
            .take(crate::status::MAX_CONDITIONS)
            .map(condition_view)
            .collect(),
    }
}

/// A page of retention policies.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RetentionPolicyList {
    /// The request ID.
    pub request_id: String,
    /// The items on this page.
    pub items: Vec<RetentionPolicyView>,
    /// Paging.
    pub page: Page,
}

/// One retention policy.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RetentionPolicyResponse {
    /// The request ID.
    pub request_id: String,
    /// The item.
    pub item: RetentionPolicyView,
}

/// `GET .../retention-policies`.
pub async fn list(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(ns): ApiPath<String>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadRetentionPolicies)?;
    let query = list_query(uri.query())?;
    let (items, page) =
        list_page::<RetentionPolicy>(&state, &actor, &ns, ROUTE_LIST, &query).await?;
    let now = state.now();
    Ok(json(
        StatusCode::OK,
        &RetentionPolicyList {
            request_id,
            items: items.iter().map(|p| view(p, now)).collect(),
            page,
        },
    ))
}

/// `GET .../retention-policies/{name}`.
pub async fn get_one(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, name)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    crate::http::parse_query(uri.query(), &[])?;
    authorize(&state, &actor, &ns, Action::ReadRetentionPolicies)?;
    let object = get_object::<RetentionPolicy>(&state, &actor, &ns, &name).await?;
    Ok(json(
        StatusCode::OK,
        &RetentionPolicyResponse {
            request_id,
            item: view(&object, state.now()),
        },
    ))
}
