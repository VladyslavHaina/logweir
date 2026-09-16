//! `/healthz`, `/readyz` and the two redirects to `/ui/`.

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use http::StatusCode;

use crate::app::AppState;
use crate::contract::HealthStatus;
use crate::problem::{ApiError, ProblemCode};

/// Liveness: the process answers. No dependency is consulted.
pub async fn healthz() -> Response {
    super::json(
        StatusCode::OK,
        &HealthStatus {
            status: "ok".to_string(),
        },
    )
}

/// Readiness: the configuration was valid at startup (the process would not
/// be running otherwise) and Kubernetes answers for this service's identity.
/// The body carries no detail about why.
pub async fn readyz(State(state): State<AppState>) -> Response {
    if state.ready().await {
        super::json(
            StatusCode::OK,
            &HealthStatus {
                status: "ready".to_string(),
            },
        )
    } else {
        ApiError::new(
            ProblemCode::KubernetesUnavailable,
            "The service is not ready.",
        )
        .into_response()
    }
}

/// `/` and `/ui` redirect to `/ui/`, relative to this origin.
pub async fn redirect_to_ui() -> Response {
    (
        StatusCode::PERMANENT_REDIRECT,
        [(http::header::LOCATION, "/ui/")],
    )
        .into_response()
}
