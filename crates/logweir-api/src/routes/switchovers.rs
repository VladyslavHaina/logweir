//! Switchovers (PROD-04.2): `Switchover` projections, a create whose
//! `planBytes` are opaque here, and the approval routing a `Restore` gets,
//! for subject kind `Switchover`.
//!
//! THE PLAN IS PARSED ONLY TO REFUSE EARLY. The create checks that `planHash`
//! is the SHA-256 of exactly `planBytes`, that the plan is a bounded switchover
//! plan and that its `restore` block is `restoreRef`, then stores the string
//! unchanged; the controller and the runner decide again from the same bytes.
//!
//! Approval routing, by the namespace's binding (as a `Restore`'s):
//! unbound — nothing is signed, the approver records `logweir drill approve
//! --subject-kind Switchover`'s files through `POST .../approval`; `confirm` —
//! the console signs the authorization document (subject kind `Switchover`)
//! and it IS the Approval; `two-person`/`strict` — the console's signed
//! request is stored as the confirmation an approver countersigns
//! (`logweir drill countersign`) and submits through `POST .../approval`.

use std::collections::BTreeMap;

use axum::body::Body;
use axum::extract::State;
use axum::response::Response;
use http::{HeaderMap, StatusCode, Uri};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::ResourceExt as _;
use logweir_core::approval_policy::{
    ApprovalRoute, EffectivePolicy, Requester, RestoreAuthorization,
};
use weirkeeper::crds::approval::{Approval, ApprovalSpec, SubjectKind, SubjectRef};
use weirkeeper::crds::switchover::{Switchover, SwitchoverRestoreRef, SwitchoverSpec};
use weirkeeper::crds::LocalRef;

use super::{authorize, create_idempotent, get_object, json, list_page, list_query, ApiPath};
use crate::app::AppState;
use crate::approval;
use crate::auth::Actor;
use crate::authz::Action;
use crate::contract::{
    self as c, ApprovalResponse, AuthorizationState, CreateSwitchoverRequest, RestoreRoutingView,
    SubmitApprovalRequest, SwitchoverGroupView, SwitchoverList, SwitchoverResponse, SwitchoverView,
};
use crate::http::{read_json, RequestId, MAX_JSON_BODY};
use crate::idempotency::IdempotencyKey;
use crate::kube::KubeFailure;
use crate::problem::{ApiError, FieldError, ProblemCode};
use crate::projection;

/// The list route identifier.
pub const ROUTE_LIST: &str = "GET /api/v1/namespaces/{ns}/switchovers";
/// The create route identifier.
pub const ROUTE_CREATE: &str = "POST /api/v1/namespaces/{ns}/switchovers";
/// The deterministic name prefix.
pub const NAME_PREFIX: &str = "swo-";
/// The v1 approval document's payload type.
const PAYLOAD_TYPE_APPROVAL_V1: &str = "application/vnd.logweir.drill-approval+json;version=1.0.0";

/// The projection; `planBytes` only on a single object's read.
#[must_use]
pub fn view(s: &Switchover, with_plan: bool) -> SwitchoverView {
    let st = s.status.as_ref();
    SwitchoverView {
        name: s.name_any(),
        namespace: s.namespace().unwrap_or_default(),
        uid: s.uid().unwrap_or_default(),
        restore: c::SwitchoverRestoreRef {
            name: s.spec.restore_ref.name.clone(),
            uid: s.spec.restore_ref.uid.clone(),
        },
        approval_name: s.spec.approval_ref.name.clone(),
        plan_hash: logweir_core::ids::sha256_prefixed(s.spec.plan_bytes.as_bytes()),
        plan_bytes: with_plan.then(|| s.spec.plan_bytes.clone()),
        phase: st.and_then(|s| s.phase.clone()),
        reason: st.and_then(|s| s.reason.clone()),
        exit_code: st.and_then(|s| s.exit_code),
        groups: st
            .map(|s| {
                s.groups
                    .iter()
                    .map(|g| SwitchoverGroupView {
                        source: g.source.clone(),
                        target: g.target.clone(),
                        outcome: g.outcome.clone(),
                        reason: g.reason.clone(),
                        created: g.created,
                        partitions: g.partitions,
                    })
                    .collect()
            })
            .unwrap_or_default(),
        record_key: st
            .and_then(|s| s.evidence.as_ref())
            .and_then(|e| e.record_key.clone()),
    }
}

/// The create's own refusals, before anything exists.
///
/// # Errors
///
/// `validation_failed`, naming each field.
pub fn validate_create(r: &CreateSwitchoverRequest) -> Result<(), ApiError> {
    let mut errors = Vec::new();
    if r.plan_bytes.len() > logweir_core::switchover::MAX_PLAN_BYTES {
        errors.push(FieldError::new(
            "planBytes",
            "too_long",
            format!("at most {} bytes", logweir_core::switchover::MAX_PLAN_BYTES),
        ));
    }
    if r.plan_hash != logweir_core::ids::sha256_prefixed(r.plan_bytes.as_bytes()) {
        errors.push(FieldError::new(
            "planHash",
            "plan_hash_mismatch",
            "is not the sha256 of exactly planBytes",
        ));
    }
    match logweir_core::switchover::SwitchoverPlan::parse(r.plan_bytes.as_bytes()) {
        Ok(plan) => {
            if plan.restore.name != r.restore_ref.name || plan.restore.uid != r.restore_ref.uid {
                errors.push(FieldError::new(
                    "restoreRef",
                    "plan_mismatch",
                    "is not the plan's own restore block",
                ));
            }
        }
        Err(why) => errors.push(FieldError::new("planBytes", "invalid", why)),
    }
    if !(60..=86_400).contains(&r.deadline_seconds) {
        errors.push(FieldError::new(
            "deadlineSeconds",
            "out_of_range",
            "60 to 86400",
        ));
    }
    if !crate::validate::is_dns_subdomain(&r.approval_ref.name) {
        errors.push(FieldError::new(
            "approvalRef.name",
            "invalid",
            "an object name",
        ));
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ApiError::validation(errors))
    }
}

/// The stored object.
#[must_use]
pub fn build(
    namespace: &str,
    name: String,
    annotations: BTreeMap<String, String>,
    r: &CreateSwitchoverRequest,
) -> Switchover {
    Switchover {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: Some(namespace.to_string()),
            annotations: Some(annotations),
            ..ObjectMeta::default()
        },
        spec: SwitchoverSpec {
            plan_bytes: r.plan_bytes.clone(),
            approval_ref: LocalRef {
                name: r.approval_ref.name.clone(),
            },
            restore_ref: SwitchoverRestoreRef {
                name: r.restore_ref.name.clone(),
                uid: r.restore_ref.uid.clone(),
            },
            deadline_seconds: r.deadline_seconds,
        },
        status: None,
    }
}

/// `GET .../switchovers`.
pub async fn list(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(ns): ApiPath<String>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadRestores)?;
    let query = list_query(uri.query())?;
    let (items, page) = list_page::<Switchover>(&state, &actor, &ns, ROUTE_LIST, &query).await?;
    Ok(json(
        StatusCode::OK,
        &SwitchoverList {
            request_id,
            items: items.iter().map(|s| view(s, false)).collect(),
            page,
        },
    ))
}

/// `GET .../switchovers/{name}`.
pub async fn get_one(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, name)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadRestores)?;
    crate::http::parse_query(uri.query(), &[])?;
    let object = get_object::<Switchover>(&state, &actor, &ns, &name).await?;
    Ok(json(
        StatusCode::OK,
        &SwitchoverResponse {
            request_id,
            replayed: None,
            item: view(&object, true),
            authorization: None,
        },
    ))
}

/// `POST .../switchovers`.
pub async fn create(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(ns): ApiPath<String>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::CreateRestore)?;
    crate::http::parse_query(uri.query(), &[])?;
    let key = IdempotencyKey::from_headers(&headers)?;
    let request: CreateSwitchoverRequest = read_json(body, MAX_JSON_BODY).await?;
    validate_create(&request)?;
    let policies = approval::effective_policies(state.approval(), state.kube())
        .await
        .map_err(KubeFailure::into_api_error)?;
    let effective = policies.resolve(&ns);
    if let Some(policy) = effective.bound() {
        let route = approval::route_of(policy)?;
        if !route.console_may_request(state.console_kind()) {
            return Err(ApiError::new(
                ProblemCode::PolicyMismatch,
                format!(
                    "Namespace {ns} is bound to approval policy {} ({route}); submit through the \
                     shared console. Nothing was created.",
                    policy.name
                ),
            ));
        }
    }
    super::run_create_rate(&state, &actor, &ns, super::ManualRun::Restore)?;
    let created = create_idempotent(
        &state,
        &actor,
        &ns,
        ROUTE_CREATE,
        NAME_PREFIX,
        &key,
        &request_id,
        &request,
        |name, annotations| build(&ns, name, annotations, &request),
    )
    .await?;
    let authorization = route_submission(
        &state,
        &actor,
        &ns,
        &created.object,
        &effective,
        request.ticket.as_deref(),
    )
    .await?;
    Ok(json(
        created.status(),
        &SwitchoverResponse {
            request_id,
            replayed: Some(created.replayed),
            item: view(&created.object, true),
            authorization: Some(authorization),
        },
    ))
}

/// Whether an existing Approval is this switchover's console document under
/// `policy`.
fn ours(
    existing: &Approval,
    sw: &Switchover,
    policy: &logweir_core::approval_policy::ApprovalPolicy,
) -> Option<RestoreAuthorization> {
    let doc = RestoreAuthorization::from_bytes(existing.spec.approval_bytes.as_bytes()).ok()?;
    (existing.spec.subject_ref.kind == SubjectKind::Switchover
        && existing.spec.subject_ref.name == sw.name_any()
        && doc.subject.kind == logweir_core::spec::SUBJECT_KIND_SWITCHOVER
        && doc.subject.uid == sw.uid().unwrap_or_default()
        && doc.plan_hash == logweir_core::ids::sha256_prefixed(sw.spec.plan_bytes.as_bytes())
        && doc.policy.digest == policy.digest())
    .then_some(doc)
}

/// The approval routing of a created switchover (the `Restore`'s
/// `authorize_submission`, for subject kind `Switchover`).
async fn route_submission(
    state: &AppState,
    actor: &Actor,
    ns: &str,
    sw: &Switchover,
    effective: &EffectivePolicy,
    ticket: Option<&str>,
) -> Result<RestoreRoutingView, ApiError> {
    let approval_name = sw.spec.approval_ref.name.clone();
    let Some(policy) = effective.bound() else {
        return Ok(RestoreRoutingView {
            mode: effective.mode().into(),
            operator_mode: ApprovalRoute::of(effective),
            policy: effective.name().to_string(),
            policy_digest: None,
            legacy: true,
            state: AuthorizationState::AwaitingApproval,
            approval_name,
            confirmation_name: None,
            requester: None,
            expires_at: None,
        });
    };
    let key = match state.approval().confirmation_key() {
        Ok(Some(key)) => key,
        Ok(None) => {
            return Err(ApiError::new(
                ProblemCode::InternalError,
                "This console holds no confirmation key for a namespace bound to an approval \
                 policy.",
            ))
        }
        Err(reason) => return Err(ApiError::new(ProblemCode::InternalError, reason)),
    };
    let governed = approval::route_of(policy)?.awaits_an_approver();
    let target = if governed {
        approval::confirmation_name(&approval_name)
    } else {
        approval_name.clone()
    };
    let view = |doc: &RestoreAuthorization| RestoreRoutingView {
        mode: policy.mode.into(),
        operator_mode: ApprovalRoute::of(effective),
        policy: policy.name.clone(),
        policy_digest: Some(policy.digest()),
        legacy: false,
        state: if governed {
            AuthorizationState::AwaitingApproval
        } else {
            AuthorizationState::Confirmed
        },
        approval_name: approval_name.clone(),
        confirmation_name: governed.then(|| target.clone()),
        requester: Some(doc.requester.principal_id()),
        expires_at: Some(doc.expires_at),
    };
    match state.kube().get::<Approval>(ns, &target).await {
        Ok(existing) => {
            return ours(&existing, sw, policy)
                .map(|d| view(&d))
                .ok_or_else(|| {
                    ApiError::new(
                        ProblemCode::StateConflict,
                        format!(
                            "An Approval named {target} already exists in {ns} and is not this \
                         Switchover's; it is never adopted. Submit with another approval name."
                        ),
                    )
                })
        }
        Err(KubeFailure::NotFound) => {}
        Err(other) => return Err(other.into_api_error()),
    }
    let mut doc = approval::document(
        policy,
        ns,
        &sw.name_any(),
        &sw.uid().unwrap_or_default(),
        &logweir_core::ids::sha256_prefixed(sw.spec.plan_bytes.as_bytes()),
        Requester {
            issuer: actor.issuer.clone(),
            subject: actor.subject.clone(),
        },
        state.now(),
        ticket.map(str::to_string),
        logweir_core::original_name::ApprovalSubject::Ordinary,
        None,
    );
    // The one difference from a Restore's: the subject kind the document
    // binds, which every verifier compares.
    doc.subject.kind = logweir_core::spec::SUBJECT_KIND_SWITCHOVER.to_string();
    let bytes = doc.to_bytes();
    let sidecar = key
        .sign(&bytes)
        .map_err(|reason| ApiError::new(ProblemCode::InternalError, reason))?;
    let object = Approval {
        metadata: ObjectMeta {
            name: Some(target.clone()),
            namespace: Some(ns.to_string()),
            annotations: Some(
                [
                    (
                        "logweir.dev/approval-mode".to_string(),
                        policy.mode.as_str().to_string(),
                    ),
                    (
                        "logweir.dev/approval-policy".to_string(),
                        policy.name.clone(),
                    ),
                    ("logweir.dev/requester".to_string(), actor.id()),
                ]
                .into_iter()
                .collect(),
            ),
            ..Default::default()
        },
        spec: ApprovalSpec {
            subject_ref: SubjectRef {
                kind: SubjectKind::Switchover,
                name: sw.name_any(),
            },
            plan_hash: doc.plan_hash.clone(),
            approval_bytes: String::from_utf8(bytes).map_err(|_| {
                ApiError::new(
                    ProblemCode::InternalError,
                    "The authorization document is not UTF-8.",
                )
            })?,
            sidecar_bytes: serde_json::to_string(&sidecar).map_err(|_| {
                ApiError::new(
                    ProblemCode::InternalError,
                    "The sidecar could not be rendered.",
                )
            })?,
        },
        status: None,
    };
    match state.kube().create(ns, &object).await {
        Ok(_) | Err(KubeFailure::AlreadyExists) => {
            actor.audit.note("requester", &doc.requester.principal_id());
            Ok(view(&doc))
        }
        Err(other) => Err(other.into_api_error()),
    }
}

/// `POST .../switchovers/{name}/approval`: in an unbound namespace, records
/// the files `logweir drill approve --subject-kind Switchover` wrote; under a
/// two-person or strict policy, an approver's countersignature over the
/// console's stored request (the controller re-verifies every signature).
///
/// # Errors
///
/// `forbidden` for the requester approving their own request,
/// `policy_mismatch`, `not_found`, `state_conflict`, `validation_failed`.
pub async fn submit_approval(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, name)): ApiPath<(String, String)>,
    uri: Uri,
    body: Body,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::SubmitApproval)?;
    crate::http::parse_query(uri.query(), &[])?;
    let request: SubmitApprovalRequest = read_json(body, MAX_JSON_BODY).await?;
    let sw = get_object::<Switchover>(&state, &actor, &ns, &name).await?;
    let policies = approval::effective_policies(state.approval(), state.kube())
        .await
        .map_err(KubeFailure::into_api_error)?;
    let effective = policies.resolve(&ns);
    let plan_hash = logweir_core::ids::sha256_prefixed(sw.spec.plan_bytes.as_bytes());
    let approval_bytes = match effective.bound() {
        None => {
            let Some(bytes) = request.approval_bytes else {
                return Err(ApiError::validation(vec![FieldError::new(
                    "approvalBytes",
                    "required",
                    "an unbound namespace records the approval.json `logweir drill approve \
                     --subject-kind Switchover` wrote, beside its sidecar",
                )]));
            };
            let doc: Option<serde_json::Value> = serde_json::from_str(&bytes).ok();
            let field = |k: &str| {
                doc.as_ref()
                    .and_then(|d| d.get(k))
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
            };
            let payload = serde_json::from_str::<serde_json::Value>(&request.sidecar_bytes)
                .ok()
                .and_then(|v| {
                    v.get("payloadType")
                        .and_then(|t| t.as_str())
                        .map(str::to_string)
                });
            if payload.as_deref() != Some(PAYLOAD_TYPE_APPROVAL_V1)
                || field("plan_hash").as_deref() != Some(plan_hash.as_str())
                || field("subject_kind").as_deref()
                    != Some(logweir_core::spec::SUBJECT_KIND_SWITCHOVER)
            {
                return Err(ApiError::validation(vec![FieldError::new(
                    "approvalBytes",
                    "plan_mismatch",
                    "an approval document of subject kind Switchover over this switchover's \
                     plan hash, signed under the v1 approval payload type",
                )]));
            }
            bytes
        }
        Some(policy) => {
            if !approval::route_of(policy)?.awaits_an_approver() {
                return Err(ApiError::new(
                    ProblemCode::PolicyMismatch,
                    format!(
                        "Namespace {ns} is under policy {}, where the console's confirmation is \
                         the whole authorization; there is nothing to approve.",
                        policy.name
                    ),
                ));
            }
            let confirmation = approval::confirmation_name(&sw.spec.approval_ref.name);
            let stored = state
                .kube()
                .get::<Approval>(&ns, &confirmation)
                .await
                .map_err(KubeFailure::into_api_error)?;
            let Some(doc) = ours(&stored, &sw, policy) else {
                return Err(ApiError::new(
                    ProblemCode::PolicyMismatch,
                    format!(
                        "{confirmation} is not this Switchover's request under {}",
                        policy.name
                    ),
                ));
            };
            if doc.requester.issuer == actor.issuer && doc.requester.subject == actor.subject {
                actor.audit.set_failure("self_approval");
                return Err(ApiError::new(
                    ProblemCode::Forbidden,
                    "You requested this switchover; a second person approves it.",
                ));
            }
            stored.spec.approval_bytes
        }
    };
    let approval_name = sw.spec.approval_ref.name.clone();
    let object = Approval {
        metadata: ObjectMeta {
            name: Some(approval_name.clone()),
            namespace: Some(ns.clone()),
            annotations: Some(
                [
                    (
                        "logweir.dev/approval-policy".to_string(),
                        effective.name().to_string(),
                    ),
                    ("logweir.dev/approver".to_string(), actor.id()),
                ]
                .into_iter()
                .collect(),
            ),
            ..Default::default()
        },
        spec: ApprovalSpec {
            subject_ref: SubjectRef {
                kind: SubjectKind::Switchover,
                name: sw.name_any(),
            },
            plan_hash,
            approval_bytes,
            sidecar_bytes: request.sidecar_bytes,
        },
        status: None,
    };
    let (stored, replayed) = match state.kube().create(&ns, &object).await {
        Ok(created) => (created, false),
        Err(KubeFailure::AlreadyExists) => {
            let existing = state
                .kube()
                .get::<Approval>(&ns, &approval_name)
                .await
                .map_err(KubeFailure::into_api_error)?;
            if existing.spec.approval_bytes != object.spec.approval_bytes
                || existing.spec.sidecar_bytes != object.spec.sidecar_bytes
            {
                return Err(ApiError::new(
                    ProblemCode::StateConflict,
                    format!(
                        "An Approval named {approval_name} already exists with other contents; \
                         it is immutable and never replaced."
                    ),
                ));
            }
            (existing, true)
        }
        Err(other) => return Err(other.into_api_error()),
    };
    Ok(json(
        if replayed {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        &ApprovalResponse {
            request_id,
            replayed: Some(replayed),
            item: projection::approval(&stored),
        },
    ))
}
