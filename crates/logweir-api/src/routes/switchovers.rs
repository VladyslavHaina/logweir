//! Switchovers (PROD-04.2): `Switchover` projections, a create whose
//! `planBytes` are opaque here, and the approval routing a `Restore` gets,
//! for subject kind `Switchover`.
//!
//! THE PLAN IS PARSED ONLY TO REFUSE EARLY. The create checks that `planHash`
//! is the SHA-256 of exactly `planBytes`, that the plan is a bounded switchover
//! plan and that its `restore` block is `restoreRef`, then stores the string
//! unchanged; the controller and the runner decide again from the same bytes.
//!
//! EVERY APPROVAL GATE IS THE RESTORE'S, BY THE SAME FUNCTION
//! (`super::subject`): the create's refusals before anything exists
//! (`restores::refuse_before_create`: the modes `localAdmin` serves, the
//! console key, a comparable requester, the ticket, a showable two-person
//! scope, the confirmation name), the console's signing
//! (`restores::authorize_submission`), an approver's submission
//! (`restores::submitted_files`, `restores::submit_for`) and the second
//! person's click (`console_approval::switchover_request`,
//! `console_approval::switchover_approve`). Routing, by the namespace's
//! binding: unbound — the approver records `logweir drill approve
//! --subject-kind Switchover`'s files through `POST .../approval`; `confirm`
//! — the console signs the authorization document (subject kind
//! `Switchover`) and it IS the Approval; `two-person` — a second person
//! approves in the console; `strict` — an approver countersigns
//! (`logweir drill countersign`) and submits through `POST .../approval`.

use std::collections::BTreeMap;

use axum::body::Body;
use axum::extract::State;
use axum::response::Response;
use http::{HeaderMap, StatusCode, Uri};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::ResourceExt as _;
use weirkeeper::crds::switchover::{Switchover, SwitchoverRestoreRef, SwitchoverSpec};
use weirkeeper::crds::LocalRef;

use super::restores::{self, CreateGate, SignedBesideThePlan};
use super::subject::{GatedPlan, Subject};
use super::{authorize, create_idempotent, get_object, json, list_page, list_query, ApiPath};
use crate::app::AppState;
use crate::approval;
use crate::auth::Actor;
use crate::authz::Action;
use crate::contract::{
    self as c, CreateSwitchoverRequest, SubmitApprovalRequest, SwitchoverGroupView, SwitchoverList,
    SwitchoverResponse, SwitchoverView,
};
use crate::http::{read_json, RequestId, MAX_JSON_BODY};
use crate::idempotency::IdempotencyKey;
use crate::kube::KubeFailure;
use crate::problem::{ApiError, FieldError};

/// The list route identifier.
pub const ROUTE_LIST: &str = "GET /api/v1/workspaces/{ws}/switchovers";
/// The create route identifier.
pub const ROUTE_CREATE: &str = "POST /api/v1/workspaces/{ws}/switchovers";
/// The deterministic name prefix.
pub const NAME_PREFIX: &str = "swo-";

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
        message: st
            .and_then(|s| s.message.as_deref())
            .map(|m| crate::validate::bounded(m, 2048)),
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

/// The create's own refusals, before anything exists: the restore create's
/// plan, hash and approval-name checks (the same function), then the
/// switchover plan's grammar and its `restore` block.
///
/// # Errors
///
/// `validation_failed`, naming each field.
pub fn validate_create(r: &CreateSwitchoverRequest) -> Result<(), ApiError> {
    let mut errors =
        restores::plan_and_approval_errors(&r.plan_bytes, &r.plan_hash, &r.approval_ref.name);
    if errors.iter().all(|e| e.field != "planBytes") {
        match logweir_core::switchover::SwitchoverPlan::parse(r.plan_bytes.as_bytes()) {
            Ok(plan) => {
                if plan.restore.name != r.restore_ref.name || plan.restore.uid != r.restore_ref.uid
                {
                    errors.push(FieldError::new(
                        "restoreRef",
                        "plan_mismatch",
                        "is not the plan's own restore block",
                    ));
                }
            }
            Err(why) => errors.push(FieldError::new("planBytes", "invalid", why)),
        }
    }
    if !(60..=86_400).contains(&r.deadline_seconds) {
        errors.push(FieldError::new(
            "deadlineSeconds",
            "out_of_range",
            "60 to 86400",
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
    let policies = approval::effective_policies(state.approval(), state.kube())
        .await
        .map_err(KubeFailure::into_api_error)?;
    let effective = policies.resolve(&ns);
    // THE RESTORE CREATE'S GATES, BY THE SAME FUNCTION, before the plan's own
    // grammar: in a two-person namespace a plan too large to show in full is
    // refused `scope_incomplete` by name (it would wait for an approval
    // nobody is ever offered).
    restores::refuse_before_create(
        &state,
        &actor,
        &ns,
        &effective,
        &CreateGate {
            ticket: request.ticket.as_deref(),
            approval_name: &request.approval_ref.name,
            plan: GatedPlan::Switchover(&request.plan_bytes),
        },
    )?;
    validate_create(&request)?;
    restores::refuse_taken_approval::<Switchover>(
        &state,
        &ns,
        &effective,
        &request.approval_ref.name,
        &super::deterministic_name(&actor, &ns, ROUTE_CREATE, NAME_PREFIX, &key),
        "Switchover",
    )
    .await?;
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
    let authorization = restores::authorize_submission(
        &state,
        &actor,
        &ns,
        &Subject::of_switchover(&ns, &created.object),
        &effective,
        SignedBesideThePlan {
            ticket: request.ticket.as_deref(),
            confirmation: None,
        },
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

/// `POST .../switchovers/{name}/approval`: the restore route's submission,
/// by the same functions — in an unbound namespace the files `logweir drill
/// approve --subject-kind Switchover` wrote; under a strict policy an
/// approver's countersignature over the console's stored request; refused
/// under `confirm` and `two-person` (a second person approves in the console).
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
    let submitted = restores::submitted_files(&actor, &request)?;
    let switchover = get_object::<Switchover>(&state, &actor, &ns, &name).await?;
    restores::submit_for(
        &state,
        &actor,
        request_id,
        &ns,
        &Subject::of_switchover(&ns, &switchover),
        request,
        submitted,
    )
    .await
}
