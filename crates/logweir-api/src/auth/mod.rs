//! Authentication: who is making this request.
//!
//! THE SEAM PLAT-17.2 PLUGS INTO. Route code never asks HOW an actor was
//! authenticated: it takes an [`Actor`] (an axum extractor backed by the
//! configured [`Authenticator`]) and asks [`crate::authz::Authorizer`] whether
//! that actor may perform an action in a namespace. This stage ships exactly
//! one authenticator, [`LocalAdminAuthenticator`]; the OIDC session
//! authenticator, its CSRF token check ([`Authenticator::verify_unsafe`]) and
//! role bindings replace the two trait objects in `AppState` without changing
//! a route.
//!
//! WHAT IS NEVER AN IDENTITY INPUT. `X-Remote-User`, `X-Forwarded-User`,
//! `X-Auth-Request-*`, `Host`, `Forwarded` and `X-Forwarded-*` have no effect
//! on who the actor is, in any authenticator. `Impersonate-*` headers are
//! refused outright by `crate::http::boundary_guard` before routing.

use axum::extract::FromRequestParts;
use http::request::Parts;
use schemars::JsonSchema;
use serde::Serialize;

use crate::app::AppState;
use crate::problem::ApiError;

/// The issuer string of the local administrator actor.
pub const LOCAL_ADMIN_ISSUER: &str = "urn:logweir:local-admin";

/// How the current actor was authenticated.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum AuthenticationMode {
    /// The explicit loopback administrator mode of this stage.
    LocalAdmin,
}

/// The authenticated principal. Authorization uses `issuer` and `subject`
/// only — never the display name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Actor {
    /// The identity issuer (an OIDC issuer URL in PLAT-17.2).
    pub issuer: String,
    /// The subject within the issuer.
    pub subject: String,
    /// A display name, for presentation only.
    pub display_name: String,
}

impl Actor {
    /// The stable actor ID: `<issuer>#<subject>`.
    #[must_use]
    pub fn id(&self) -> String {
        format!("{}#{}", self.issuer, self.subject)
    }
}

/// Resolves the actor of a request.
pub trait Authenticator: Send + Sync + 'static {
    /// The mode this authenticator implements.
    fn mode(&self) -> AuthenticationMode;

    /// The actor for this request, or `unauthenticated`/`session_expired`.
    ///
    /// # Errors
    ///
    /// An [`ApiError`] when the request carries no valid identity.
    fn authenticate(&self, parts: &Parts) -> Result<Actor, ApiError>;

    /// Extra checks for an unsafe method, run AFTER the route-independent
    /// Origin and Content-Type guard. PLAT-17.2's session authenticator
    /// verifies its synchronizer CSRF token here. The local administrator has
    /// no session and therefore no token to verify.
    ///
    /// # Errors
    ///
    /// An [`ApiError`] (`forbidden`) when the check fails.
    fn verify_unsafe(&self, _parts: &Parts, _actor: &Actor) -> Result<(), ApiError> {
        Ok(())
    }

    /// The session expiry to report, if sessions expire.
    fn session_expiry(&self, _parts: &Parts) -> Option<chrono::DateTime<chrono::Utc>> {
        None
    }

    /// The CSRF token to hand the browser, if this mode uses one.
    fn csrf_token(&self, _parts: &Parts, _actor: &Actor) -> Option<String> {
        None
    }
}

/// The configured local administrator, for every request.
///
/// Safe only because `crate::config` refuses a non-loopback listener in this
/// mode and `crate::http::boundary_guard` refuses a `Host` this listener does
/// not serve: reaching the socket at all is the authentication, exactly as it
/// is for `kubectl proxy` on loopback.
#[derive(Clone, Debug)]
pub struct LocalAdminAuthenticator {
    actor: Actor,
}

impl LocalAdminAuthenticator {
    /// The authenticator for one configured subject.
    #[must_use]
    pub fn new(subject: &str, display_name: &str) -> Self {
        Self {
            actor: Actor {
                issuer: LOCAL_ADMIN_ISSUER.to_string(),
                subject: subject.to_string(),
                display_name: display_name.to_string(),
            },
        }
    }
}

impl Authenticator for LocalAdminAuthenticator {
    fn mode(&self) -> AuthenticationMode {
        AuthenticationMode::LocalAdmin
    }

    fn authenticate(&self, _parts: &Parts) -> Result<Actor, ApiError> {
        Ok(self.actor.clone())
    }
}

impl FromRequestParts<AppState> for Actor {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let authenticator = state.authenticator();
        let actor = authenticator.authenticate(parts)?;
        if !crate::http::is_safe_method(&parts.method) {
            authenticator.verify_unsafe(parts, &actor)?;
        }
        Ok(actor)
    }
}
