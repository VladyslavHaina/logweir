//! `GET /api/v1/workspaces`: the workspaces the actor holds a role in.

use axum::extract::State;
use axum::response::Response;
use http::StatusCode;

use crate::app::AppState;
use crate::auth::Actor;
use crate::contract::NamespaceListResponse;
use crate::http::RequestId;

/// The actor's workspace grants.
pub async fn list(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
) -> Response {
    super::json(
        StatusCode::OK,
        &NamespaceListResponse {
            request_id,
            items: super::session::grants(&state, &actor),
        },
    )
}
