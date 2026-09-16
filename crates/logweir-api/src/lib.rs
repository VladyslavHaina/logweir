#![forbid(unsafe_code)]
//! `logweir-api` — the bounded Logweir product API (PLAT-17.1).
//!
//! WHAT THIS IS. One small HTTP service that serves the existing static UI
//! at `/ui/` and a typed JSON API at `/api/v1` on the same origin, and that
//! reads and creates the existing `logweir.dev/v1alpha1` resources through one
//! Kubernetes adapter ([`kube`]). `weirkeeper` stays the only execution
//! controller; this service creates custom resources and never a Job.
//!
//! WHAT THIS IS NOT. It is not a Kubernetes proxy. There is no generic
//! group/version/resource route, no request-supplied Kubernetes path, no
//! Secret, Pod, log, exec or Job access, no delete, and no arbitrary patch —
//! the one update is `BackupSchedule.spec.suspend` under a resourceVersion
//! precondition. Inbound `Impersonate-*` headers are refused and nothing is
//! ever impersonated.
//!
//! THIS STAGE'S IDENTITY MODE. Only `localAdmin`: a loopback-only listener,
//! the configured administrator as the actor, and namespaces from
//! configuration alone ([`config`]). OIDC, sessions, CSRF tokens, roles and
//! audit are PLAT-17.2 and plug into [`auth::Authenticator`] and
//! [`authz::Authorizer`] without changing a route.
//!
//! MODULE MAP.
//!
//! * [`app`] — state and the route table (the boundary).
//! * [`http`] — request IDs, security headers, problem rendering, the Host,
//!   `Impersonate-*`, Origin and Content-Type guards, strict JSON and query
//!   parsing.
//! * [`auth`], [`authz`] — the identity and authorization seam.
//! * [`kube`] — the only Kubernetes adapter, 10-second deadlines.
//! * [`routes`] — one module per product resource.
//! * [`idempotency`] — deterministic names and replay comparison.
//! * [`cursor`] — authenticated list cursors.
//! * [`status`] — Backup/Restore status to the product operation model.
//! * [`projection`] — stored objects to response DTOs.
//! * [`contract`], [`problem`], [`openapi`] — the HTTP contract and its
//!   generated, drift-tested OpenAPI document.
//! * [`assets`] — the static UI allowlist.

pub mod app;
pub mod assets;
pub mod auth;
pub mod authz;
pub mod config;
pub mod contract;
pub mod cursor;
pub mod http;
pub mod idempotency;
pub mod kube;
pub mod openapi;
pub mod problem;
pub mod projection;
pub mod routes;
pub mod status;
pub mod validate;

use std::sync::Arc;

/// What the configuration alone decides, loaded before any Kubernetes client
/// exists so that a refusal here is a configuration refusal (exit 2) rather
/// than a startup failure.
pub struct Preflight {
    /// The cursor MAC key.
    pub cursor_key: Vec<u8>,
    /// The static UI allowlist.
    pub assets: assets::StaticAssets,
}

/// Read the cursor key and the static assets.
///
/// # Errors
///
/// A reason naming the file or directory.
pub fn preflight(config: &config::Config) -> Result<Preflight, String> {
    Ok(Preflight {
        cursor_key: config::read_cursor_key(&config.cursor_key_file).map_err(|e| e.to_string())?,
        assets: assets::StaticAssets::load(&config.ui_directory)?,
    })
}

/// Build the application state from a validated configuration, its preflight
/// and a built Kubernetes client.
#[must_use]
pub fn state_from_parts(
    config: &config::Config,
    preflight: Preflight,
    client: ::kube::Client,
) -> app::AppState {
    app::AppState::new(app::Settings {
        authenticator: Arc::new(auth::LocalAdminAuthenticator::new(
            &config.local_admin.subject,
            &config.local_admin.display_name,
        )),
        authorizer: Arc::new(authz::LocalAdminAuthorizer::new(config.namespaces.clone())),
        kube: kube::KubeAdapter::new(client),
        cursor_key: cursor::CursorKey::new(preflight.cursor_key),
        clock: Arc::new(app::SystemClock),
        public_origin: config.public_origin.clone(),
        allowed_hosts: config.allowed_hosts.clone(),
        assets: preflight.assets,
        readiness_namespace: config.namespaces[0].clone(),
    })
}
