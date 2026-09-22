//! Authentication: who is making this request.
//!
//! TWO AUTHENTICATORS, ONE SEAM. Route code never asks HOW an actor was
//! authenticated: it takes an [`Actor`] (an axum extractor backed by the
//! configured [`Authenticator`]) and asks [`crate::authz::Authorizer`] whether
//! that actor may perform an action in a namespace. [`LocalAdminAuthenticator`]
//! is the explicit loopback administrator mode; [`shared::SessionAuthenticator`]
//! is the OIDC session of shared mode, and it plugs in by replacing two trait
//! objects in `AppState` — no route changed to add it.
//!
//! WHAT IS NEVER AN IDENTITY INPUT. `X-Remote-User`, `X-Forwarded-User`,
//! `X-Auth-Request-*`, `X-Remote-Extra-*`, `Host`, `Forwarded` and
//! `X-Forwarded-*` have no effect on who the actor is, in any authenticator.
//! `crate::http::boundary_guard` STRIPS the identity-claiming family from the
//! request before routing — so no later code can read one even by mistake —
//! and records the names it removed in the audit record.
//! `Impersonate-*` headers are refused outright with 400, because a client
//! sending one is asking for something this service will never do.
//!
//! THE CALLBACK URL IS CONFIGURATION, NOT A HEADER. `publicBaseUrl` is an exact
//! administrator value; the redirect URI is derived from it once, at startup.
//! Nothing in [`login`] reads `Host`, `Forwarded` or `X-Forwarded-Host`, so
//! Host poisoning cannot move the redirect.

pub mod keys;
pub mod login;
pub mod oidc;
pub mod ratelimit;
pub mod session;
pub mod shared;

use std::sync::Arc;

use axum::extract::FromRequestParts;
use http::request::Parts;
use schemars::JsonSchema;
use serde::Serialize;

use crate::app::AppState;
use crate::audit::AuditContext;
use crate::problem::ApiError;

/// The issuer string of the local administrator actor.
pub const LOCAL_ADMIN_ISSUER: &str = "urn:logweir:local-admin";

/// How the current actor was authenticated.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum AuthenticationMode {
    /// The explicit loopback administrator mode.
    LocalAdmin,
    /// A validated OpenID Connect session.
    Oidc,
}

impl AuthenticationMode {
    /// The wire spelling, also used in the audit record.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            AuthenticationMode::LocalAdmin => "localAdmin",
            AuthenticationMode::Oidc => "oidc",
        }
    }
}

/// The authenticated principal.
///
/// AUTHORIZATION USES `issuer` AND `subject`, PLUS `groups`. It never uses
/// `display_name`, which exists only so a page can greet someone, and which the
/// audit record keeps in its own field for exactly that reason.
#[derive(Clone, Debug)]
pub struct Actor {
    /// The identity issuer (an OIDC issuer URL in shared mode).
    pub issuer: String,
    /// The subject within the issuer.
    pub subject: String,
    /// A display name, for presentation only.
    pub display_name: String,
    /// The exact group claim strings this session carried. Role bindings match
    /// these by exact string; nothing here is a pattern.
    pub groups: Vec<String>,
    /// The session id, when the mode has sessions. Only its SHA-256 is ever
    /// logged.
    pub session_id: Option<String>,
    /// The audit record under construction for this request.
    ///
    /// It rides on the actor because every place that makes a decision already
    /// holds one, which is how a handler cannot forget to attribute a decision
    /// it just made.
    pub audit: Arc<AuditContext>,
}

impl PartialEq for Actor {
    /// Two actors are equal when their IDENTITY is equal. The audit context is
    /// per-request bookkeeping and is deliberately not part of it.
    fn eq(&self, other: &Self) -> bool {
        self.issuer == other.issuer
            && self.subject == other.subject
            && self.display_name == other.display_name
            && self.groups == other.groups
            && self.session_id == other.session_id
    }
}

impl Eq for Actor {}

impl Actor {
    /// An actor with no groups and no session, for the modes that have none.
    #[must_use]
    pub fn new(
        issuer: impl Into<String>,
        subject: impl Into<String>,
        display: impl Into<String>,
    ) -> Self {
        Self {
            issuer: issuer.into(),
            subject: subject.into(),
            display_name: display.into(),
            groups: Vec::new(),
            session_id: None,
            audit: Arc::new(AuditContext::default()),
        }
    }

    /// The same actor with exact group claims.
    #[must_use]
    pub fn with_groups(mut self, groups: Vec<String>) -> Self {
        self.groups = groups;
        self
    }

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
    /// Origin and Content-Type guard. The session authenticator verifies its
    /// synchronizer CSRF token here. The local administrator has no session
    /// and therefore no token to verify.
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

    /// Where an unauthenticated browser should be sent to sign in, if this
    /// mode has a login route.
    fn login_path(&self) -> Option<&'static str> {
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
            actor: Actor::new(LOCAL_ADMIN_ISSUER, subject, display_name),
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
        // THE ACCESS LAYER ALREADY DID THIS. `crate::access::enforce` runs on
        // every route, authenticates the actor (CSRF included) before any
        // handler, and hands it over; decoding the session a second time would
        // only cost a second AEAD open and a second audit write of the same
        // values. Only that layer can insert the value — request extensions
        // are not reachable from the wire.
        if let Some(crate::access::AuthenticatedActor(actor)) =
            parts.extensions.get::<crate::access::AuthenticatedActor>()
        {
            return Ok(actor.clone());
        }
        let authenticator = state.authenticator();
        let audit = crate::audit::context_of(parts);
        let mut actor = match authenticator.authenticate(parts) {
            Ok(actor) => actor,
            Err(error) => {
                audit.set_failure(error.code.as_str());
                return Err(error);
            }
        };
        actor.audit = Arc::clone(&audit);
        audit.set_actor(
            authenticator.mode().as_str(),
            &actor.id(),
            &actor.display_name,
            actor.session_id.as_deref(),
        );
        if !crate::http::is_safe_method(&parts.method) {
            if let Err(error) = authenticator.verify_unsafe(parts, &actor) {
                audit.set_failure(error.code.as_str());
                return Err(error);
            }
        }
        Ok(actor)
    }
}
