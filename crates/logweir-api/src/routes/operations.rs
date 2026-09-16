//! `GET /api/v1/namespaces/{ns}/operations/{kind}/{name}`: the normalized
//! status of one Backup or Restore. `kind` is the closed set
//! `backup|restore`; `discovery` and `preflight` have no producer yet and are
//! 404, as is any other value.

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
    authorize(&state, &actor, &ns, Action::ReadOperations)?;
    crate::http::parse_query(uri.query(), &[])?;
    let item = match kind.as_str() {
        "backup" => status::backup_operation(&get_object::<Backup>(&state, &ns, &name).await?),
        "restore" => status::restore_operation(&get_object::<Restore>(&state, &ns, &name).await?),
        _ => return Err(ApiError::not_found()),
    };
    Ok(json(
        StatusCode::OK,
        &OperationResponse { request_id, item },
    ))
}
