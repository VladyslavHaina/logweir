//! `GET /api/v1/namespaces`: the configured grants, never core Namespaces.

use axum::extract::State;
use axum::response::Response;
use http::StatusCode;

use crate::app::AppState;
use crate::auth::Actor;
use crate::contract::NamespaceListResponse;
use crate::http::RequestId;

/// The explicitly granted namespaces.
pub async fn list_namespaces(
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
