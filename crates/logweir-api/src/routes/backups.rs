//! Backups: `Backup` projections. Manual backup creation is PLAT-06.1/06.2
//! and has no route here; the session advertises `manualBackupCreate: false`.

use axum::extract::State;
use axum::response::Response;
use http::{StatusCode, Uri};
use weirkeeper::crds::backup::Backup;

use super::{authorize, get_object, json, list_page, list_query, ApiPath};
use crate::app::AppState;
use crate::auth::Actor;
use crate::authz::Action;
use crate::contract::{BackupList, BackupResponse};
use crate::http::RequestId;
use crate::problem::ApiError;
use crate::projection;

/// The list route identifier.
pub const ROUTE_LIST: &str = "GET /api/v1/namespaces/{ns}/backups";

/// `GET .../backups`. `labelSelector=logweir.dev/schedule=<name>` lists one
/// schedule's runs.
pub async fn list(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(ns): ApiPath<String>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadBackups)?;
    let query = list_query(uri.query())?;
    let (items, page) = list_page::<Backup>(&state, &actor, &ns, ROUTE_LIST, &query).await?;
    Ok(json(
        StatusCode::OK,
        &BackupList {
            request_id,
            items: items.iter().map(projection::backup).collect(),
            page,
        },
    ))
}

/// `GET .../backups/{name}`.
pub async fn get_one(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, name)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadBackups)?;
    crate::http::parse_query(uri.query(), &[])?;
    let object = get_object::<Backup>(&state, &actor, &ns, &name).await?;
    Ok(json(
        StatusCode::OK,
        &BackupResponse {
            request_id,
            replayed: None,
            item: projection::backup(&object),
        },
    ))
}
