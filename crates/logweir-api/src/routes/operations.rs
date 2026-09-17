//! `GET /api/v1/namespaces/{ns}/operations/{kind}/{name}`: the normalized
//! status of one Backup, Restore, TopicDiscovery or Preflight. `kind` is D0's
//! closed set `backup|restore|discovery|preflight`; any other value is 404.
//!
//! TWO SHAPES, BECAUSE THEY ARE TWO THINGS. A backup and a restore answer
//! `OperationResponse`, which carries a result, evidence references and a
//! verification verdict. A transient check answers `CheckOperationResponse`,
//! which carries none of those: a topic list has no signed evidence, and a
//! contract that published `verification: pending` for one would be inviting a
//! console to render a verdict that can never arrive. Each kind's
//! authorization is its own domain's (`operation.read` for the two durable
//! runs, `topicDiscovery.read` / `preflight.read` for the checks), so the
//! check routes here can never be a way around the narrowing an approver gets
//! on the preflight route itself.

use axum::extract::State;
use axum::response::Response;
use http::{StatusCode, Uri};
use weirkeeper::crds::backup::Backup;
use weirkeeper::crds::restore::Restore;

use super::{authorize, get_object, json, ApiPath};
use crate::app::AppState;
use crate::auth::Actor;
use crate::authz::Action;
use crate::contract::OperationResponse;
use crate::http::RequestId;
use crate::problem::ApiError;
use crate::status;

/// `GET .../operations/{kind}/{name}`.
pub async fn get_one(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, kind, name)): ApiPath<(String, String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    crate::http::parse_query(uri.query(), &[])?;
    match kind.as_str() {
        "discovery" => {
            return super::topic_discoveries::operation(&state, request_id, &actor, &ns, &name)
                .await
        }
        "preflight" => {
            return super::preflights::operation(&state, request_id, &actor, &ns, &name).await
        }
        _ => {}
    }
    authorize(&state, &actor, &ns, Action::ReadOperations)?;
    let item = match kind.as_str() {
        "backup" => {
            status::backup_operation(&get_object::<Backup>(&state, &actor, &ns, &name).await?)
        }
        "restore" => {
            status::restore_operation(&get_object::<Restore>(&state, &actor, &ns, &name).await?)
        }
        _ => return Err(ApiError::not_found()),
    };
    Ok(json(
        StatusCode::OK,
        &OperationResponse { request_id, item },
    ))
}
