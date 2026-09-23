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
//! TWO IDENTITY MODES, NAMED IN THE CONFIGURATION. `localAdmin` is a
//! loopback-only listener with the configured administrator as the actor and
//! namespaces from configuration alone. `shared` is the SSO console: OpenID
//! Connect Authorization Code + PKCE, a short-lived authenticated and encrypted
//! stateless session cookie, a synchronizer CSRF token on every unsafe method,
//! exact group-to-role and namespace bindings, and one structured audit record
//! per request ([`auth`], [`authz`], [`audit`]). Both plug into the same two
//! trait objects, so no route knows which mode it is serving.
//!
//! MODULE MAP.
//!
//! * [`app`] — state and the route table (the boundary).
//! * [`access`] — the per-route access declarations and the layer that
//!   enforces them before any handler runs.
//! * [`http`] — request IDs, security headers, problem rendering, the Host,
//!   `Impersonate-*`, Origin and Content-Type guards, strict JSON and query
//!   parsing.
//! * [`auth`], [`authz`] — the identity and authorization seam.
//! * [`audit`] — the structured attribution record and its redaction.
//! * [`kube`] — the only Kubernetes adapter, 10-second deadlines.
//! * [`routes`] — one module per product resource.
//! * [`idempotency`] — deterministic names and replay comparison.
//! * [`cursor`] — authenticated list cursors.
//! * [`status`] — Backup/Restore status to the product operation model.
//! * [`projection`] — stored objects to response DTOs.
//! * [`contract`], [`problem`], [`openapi`] — the HTTP contract and its
//!   generated, drift-tested OpenAPI document.
//! * [`assets`] — the static UI allowlist.

pub mod access;
pub mod app;
pub mod approval;
pub mod assets;
pub mod audit;
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
pub mod trusted_proxy;
pub mod validate;

use std::sync::Arc;

use crate::auth::keys::CookieKeys;

/// What the configuration alone decides, loaded before any Kubernetes client
/// exists so that a refusal here is a configuration refusal (exit 2) rather
/// than a startup failure.
///
/// EVERY KEY AND SECRET IS READ HERE. A missing, malformed, short or
/// unexpectedly rotated key file, and an empty client-secret file, all refuse
/// the process before a socket exists — which is the only point at which
/// refusing is free.
pub struct Preflight {
    /// The cursor MAC key.
    pub cursor_key: Vec<u8>,
    /// The static UI allowlist.
    pub assets: assets::StaticAssets,
    /// Shared mode's key material.
    pub shared: Option<SharedPreflight>,
    /// PLAT-19.2: the approval-policy document and the console confirmation
    /// key. A served namespace bound to an explicit policy without a key is a
    /// refusal here.
    pub approval: approval::ApprovalSettings,
}

/// The session key and client secret of shared mode.
pub struct SharedPreflight {
    /// The session/CSRF keys, carrying the version that goes in the cookie.
    pub session_keys: Arc<CookieKeys>,
    /// The OIDC client secret.
    pub client_secret: auth::oidc::Secret,
    /// The trust anchors for the provider's TLS certificate: the system roots
    /// and/or `oidc.caBundleFile`, read and parsed here so an unreadable or
    /// empty bundle refuses the process before a socket exists (chart gap G1).
    pub tls_trust: auth::oidc::TlsTrust,
}

/// Read the keys, the client secret and the static assets.
///
/// # Errors
///
/// A reason naming the file or directory.
pub fn preflight(config: &config::Config) -> Result<Preflight, String> {
    let cursor_key = match &config.cursor_key {
        config::CursorKeySource::RawFile(path) => {
            config::read_cursor_key(path).map_err(|e| e.to_string())?
        }
        config::CursorKeySource::Versioned(key) => {
            auth::keys::read_versioned_key(&key.file, key.expected_version)
                .map_err(|e| e.to_string())?
                .bytes()
                .to_vec()
        }
    };
    let shared = match config.shared() {
        None => None,
        Some(shared) => {
            let session_key = auth::keys::read_versioned_key(
                &shared.session_key.file,
                shared.session_key.expected_version,
            )
            .map_err(|e| e.to_string())?;
            let secret = std::fs::read_to_string(&shared.oidc.client_secret_file).map_err(|e| {
                format!(
                    "cannot read the OIDC client secret file {}: {e}",
                    shared.oidc.client_secret_file.display()
                )
            })?;
            let secret = secret.trim().to_string();
            if secret.is_empty() {
                return Err(format!(
                    "the OIDC client secret file {} is empty",
                    shared.oidc.client_secret_file.display()
                ));
            }
            let tls_trust = auth::oidc::TlsTrust::load(
                shared.oidc.ca_bundle_file.as_deref(),
                shared.oidc.system_roots,
            )?;
            Some(SharedPreflight {
                session_keys: Arc::new(CookieKeys::new(&session_key)),
                client_secret: auth::oidc::Secret::new(secret),
                tls_trust,
            })
        }
    };
    let approval = approval::ApprovalSettings::load(
        config.approval_policy_file.as_deref(),
        config.confirmation_key_file.as_deref(),
        &config.namespaces,
    )?;
    Ok(Preflight {
        cursor_key,
        assets: assets::StaticAssets::load(&config.ui_directory)?,
        shared,
        approval,
    })
}

/// What a mode contributes to the application state: an authenticator, an
/// authorizer, and shared mode's extras.
type ModeParts = (
    Arc<dyn auth::Authenticator>,
    Arc<dyn authz::Authorizer>,
    Option<Arc<app::SharedMode>>,
);

/// Build the application state from a validated configuration, its preflight
/// and a built Kubernetes client.
///
/// # Errors
///
/// A reason, when shared mode's HTTPS client for the identity provider cannot
/// be initialised.
pub fn state_from_parts(
    config: &config::Config,
    preflight: Preflight,
    client: ::kube::Client,
) -> Result<app::AppState, String> {
    let clock: Arc<dyn app::Clock> = Arc::new(app::SystemClock);
    let (authenticator, authorizer, shared): ModeParts = match (&config.mode, preflight.shared) {
        (config::Mode::LocalAdmin(admin), _) => (
            Arc::new(auth::LocalAdminAuthenticator::new(
                &admin.subject,
                &admin.display_name,
            )),
            Arc::new(authz::LocalAdminAuthorizer::new(config.namespaces.clone())),
            None,
        ),
        (config::Mode::Shared(settings), Some(material)) => {
            let http = auth::oidc::HyperHttpClient::new(
                settings.oidc.insecure_loopback_issuer,
                &material.tls_trust,
            )?;
            let provider = auth::oidc::Provider::new(
                auth::oidc::OidcSettings {
                    issuer: settings.oidc.issuer.clone(),
                    client_id: settings.oidc.client_id.clone(),
                    client_secret: material.client_secret,
                    redirect_uri: settings.redirect_uri.clone(),
                    allowed_algorithms: settings.oidc.allowed_algorithms.clone(),
                    scopes: settings.oidc.scopes.clone(),
                    groups_claim: settings.oidc.groups_claim.clone(),
                    display_name_claim: settings.oidc.display_name_claim.clone(),
                    token_auth_method: settings.oidc.token_auth_method,
                },
                Box::new(http),
            );
            (
                Arc::new(auth::shared::SessionAuthenticator::new(
                    Arc::clone(&material.session_keys),
                    Arc::clone(&clock),
                )),
                Arc::new(authz::SharedAuthorizer::new(authz::RoleBindings {
                    revision: settings.roles.revision.clone(),
                    bindings: settings.roles.bindings.clone(),
                })),
                Some(Arc::new(app::SharedMode {
                    provider,
                    keys: material.session_keys,
                    login_limiter: auth::ratelimit::RateLimiter::for_login(),
                    streams: auth::ratelimit::StreamSlots::new(),
                    session_max_age_seconds: settings.session_max_age_seconds,
                    trusted_proxies: Arc::new(trusted_proxy::TrustedProxies::new(
                        settings.trusted_proxy_cidrs.clone(),
                        settings.trusted_proxy_service.clone(),
                    )),
                    require_trusted_proxy: settings.require_trusted_proxy,
                })),
            )
        }
        (config::Mode::Shared(_), None) => {
            return Err(
                "shared mode needs its session key and client secret, which the preflight did \
                 not produce"
                    .to_string(),
            )
        }
    };

    Ok(app::AppState::new(app::Settings {
        authenticator,
        authorizer,
        kube: kube::KubeAdapter::new(client),
        cursor_key: cursor::CursorKey::new(preflight.cursor_key),
        clock,
        public_origin: config.public_origin.clone(),
        allowed_hosts: config.allowed_hosts.clone(),
        assets: preflight.assets,
        readiness_namespace: config.namespaces[0].clone(),
        shared,
        kubernetes_principal: config.kubernetes_principal.clone(),
        approval: Arc::new(preflight.approval),
    }))
}
