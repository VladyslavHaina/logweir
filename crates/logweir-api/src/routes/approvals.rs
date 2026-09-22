//! Approvals: public metadata in lists and single reads; the raw approval
//! and sidecar documents ONLY through the explicit packet route, which is a
//! separate action (`approvalPacketRead`) so PLAT-17.2 can bind it to
//! operator/approver roles alone. A governed approval is SUBMITTED through
//! `POST .../restores/{name}/approval` (`routes::restores::submit_approval`,
//! PLAT-19.2); this module adds the namespace's effective approval policy.

use axum::extract::State;
use axum::response::Response;
use http::{StatusCode, Uri};
use weirkeeper::crds::approval::Approval;

use super::{authorize, get_object, json, list_page, list_query, ApiPath};
use crate::app::AppState;
use crate::auth::Actor;
use crate::authz::Action;
use crate::contract::{
    ApprovalList, ApprovalPacketResponse, ApprovalPolicyResponse, ApprovalPolicyView,
    ApprovalResponse,
};
use crate::http::RequestId;
use crate::problem::ApiError;
use crate::projection;

/// The list route identifier.
pub const ROUTE_LIST: &str = "GET /api/v1/namespaces/{ns}/approvals";

/// `GET .../approvals`.
pub async fn list(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(ns): ApiPath<String>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadApprovals)?;
    let query = list_query(uri.query())?;
    let (items, page) = list_page::<Approval>(&state, &actor, &ns, ROUTE_LIST, &query).await?;
    Ok(json(
        StatusCode::OK,
        &ApprovalList {
            request_id,
            items: items.iter().map(projection::approval).collect(),
            page,
        },
    ))
}

/// `GET .../approvals/{name}`.
pub async fn get_one(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, name)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadApprovals)?;
    crate::http::parse_query(uri.query(), &[])?;
    let object = get_object::<Approval>(&state, &actor, &ns, &name).await?;
    Ok(json(
        StatusCode::OK,
        &ApprovalResponse {
            request_id,
            replayed: None,
            item: projection::approval(&object),
        },
    ))
}

/// `GET .../approvals/{name}/packet`.
pub async fn packet(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, name)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadApprovalPacket)?;
    crate::http::parse_query(uri.query(), &[])?;
    let object = get_object::<Approval>(&state, &actor, &ns, &name).await?;
    tracing::info!(namespace = %ns, name = %name, actor = %actor.id(), "approval packet read");
    Ok(json(
        StatusCode::OK,
        &ApprovalPacketResponse {
            request_id,
            item: projection::approval_packet(&object),
        },
    ))
}

/// The policy route identifier.
pub const ROUTE_POLICY: &str = "GET /api/v1/namespaces/{ns}/approval-policy";

/// `GET .../approval-policy` — the namespace's effective approval policy
/// (PLAT-19.2; D0: "Approver: read effective policy").
///
/// Readable by every role that may read approvals, because the operator's
/// wizard needs to say BEFORE submitting whether the Restore will run on the
/// console's confirmation or wait for an approver.
pub async fn policy(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(ns): ApiPath<String>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadApprovals)?;
    crate::http::parse_query(uri.query(), &[])?;
    let settings = state.approval();
    let effective = settings.policies.resolve(&ns);
    let bound = effective.bound();
    Ok(json(
        StatusCode::OK,
        &ApprovalPolicyResponse {
            request_id,
            item: ApprovalPolicyView {
                namespace: ns.clone(),
                name: effective.name().to_string(),
                mode: effective.mode().into(),
                legacy: effective.is_legacy(),
                max_age_seconds: bound.map(|p| p.max_age_seconds),
                require_distinct_principal: bound.is_some_and(|p| p.require_distinct_principal),
                digest: effective.digest(),
                installation_digest: settings.policies.digest(),
                confirmation_key_id: settings
                    .confirmation
                    .as_ref()
                    .map(|k| k.key_id().to_string()),
            },
        },
    ))
}
