//! `GET /api/v1/session`.

use axum::extract::{Request, State};
use axum::response::Response;
use http::StatusCode;

use crate::app::AppState;
use crate::auth::Actor;
use crate::authz::Capabilities;
use crate::contract::{ActorView, NamespaceGrant, SessionResponse};
use crate::http::RequestId;

/// The grants of an actor, each with its capabilities.
pub fn grants(state: &AppState, actor: &Actor) -> Vec<NamespaceGrant> {
    state
        .authorizer()
        .namespaces(actor)
        .into_iter()
        .map(|name| NamespaceGrant {
            capabilities: Capabilities::for_namespace(state.authorizer(), actor, &name),
            name,
        })
        .collect()
}

/// The session: actor, explicit grants and capability flags.
pub async fn get_session(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    request: Request,
) -> Response {
    let (parts, _) = request.into_parts();
    let authenticator = state.authenticator();
    let namespaces = grants(&state, &actor);
    let capabilities = Capabilities::union(namespaces.iter().map(|g| &g.capabilities));
    super::json(
        StatusCode::OK,
        &SessionResponse {
            request_id,
            authentication_mode: authenticator.mode(),
            actor: ActorView {
                id: actor.id(),
                issuer: actor.issuer.clone(),
                subject: actor.subject.clone(),
                display_name: actor.display_name.clone(),
            },
            expires_at: authenticator.session_expiry(&parts),
            csrf_token: authenticator.csrf_token(&parts, &actor),
            namespaces,
            capabilities,
        },
    )
}
