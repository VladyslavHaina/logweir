//! Application state and the router.
//!
//! THE ROUTE TABLE IS THE BOUNDARY. Every path this service answers is listed
//! in [`router`]; there is no wildcard under `/api`, no `/apis`, no raw or
//! proxy route, and the fallback for everything else is `not_found`. Every
//! route group carries `crate::access::enforce` as a route layer, and every
//! route must have an entry in `crate::access::ROUTES` — `Public` included —
//! or it fails closed. The
//! route-boundary tests request `/apis/...`, `/api`, `/api/v1/raw`, core
//! paths, Secrets, Pods and logs and expect 404 with no Kubernetes call.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::routing::get;
use axum::Router;
use chrono::{DateTime, Utc};
use tokio::sync::Mutex;

use crate::assets::StaticAssets;
use crate::auth::keys::CookieKeys;
use crate::auth::oidc::Provider;
use crate::auth::ratelimit::{RateLimiter, StreamSlots};
use crate::auth::Authenticator;
use crate::authz::Authorizer;
use crate::config::Cidr;
use crate::cursor::CursorKey;
use crate::kube::KubeAdapter;

/// A source of the current time. Injected so cursor expiry is testable
/// without sleeping.
pub trait Clock: Send + Sync + 'static {
    /// Now.
    fn now(&self) -> DateTime<Utc>;
}

/// The system clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Everything shared mode adds to the state.
///
/// Its presence is what makes `/auth/login` and `/auth/callback` exist at all:
/// [`router`] adds them only when it is `Some`, so in localAdmin mode they are
/// not in the route table and answer 404 like any other unserved path.
pub struct SharedMode {
    /// The OIDC provider, its caches and its HTTP client.
    pub provider: Provider,
    /// The session/CSRF key and its version.
    pub keys: Arc<CookieKeys>,
    /// The per-peer limit on the unauthenticated login surface.
    pub login_limiter: RateLimiter,
    /// Per-actor, per-namespace concurrent stream slots. No route uses one
    /// yet; see `crate::auth::ratelimit`.
    pub streams: Arc<StreamSlots>,
    /// The session lifetime in seconds.
    pub session_max_age_seconds: i64,
    /// Proxy ranges whose forwarded headers may be RECORDED. Never an
    /// identity input.
    pub trusted_proxy_cidrs: Vec<Cidr>,
    /// Whether the entry point refuses any request that did not arrive
    /// through one of those proxies over HTTPS
    /// (`crate::http::boundary_guard`, `requireTrustedProxy`).
    pub require_trusted_proxy: bool,
}

/// Everything a request handler needs.
pub struct Settings {
    /// The authenticator.
    pub authenticator: Arc<dyn Authenticator>,
    /// The authorizer.
    pub authorizer: Arc<dyn Authorizer>,
    /// The Kubernetes adapter.
    pub kube: KubeAdapter,
    /// The cursor MAC key.
    pub cursor_key: CursorKey,
    /// The clock.
    pub clock: Arc<dyn Clock>,
    /// The exact origin unsafe requests must carry.
    pub public_origin: String,
    /// `Host` values this listener serves.
    pub allowed_hosts: Vec<String>,
    /// The static assets.
    pub assets: StaticAssets,
    /// The namespace the readiness probe lists in.
    pub readiness_namespace: String,
    /// Shared mode's extras, absent in localAdmin mode.
    pub shared: Option<Arc<SharedMode>>,
    /// The Kubernetes identity this process writes as, recorded on every
    /// created object and in every audit line (`kubernetes.principal`).
    pub kubernetes_principal: String,
}

struct Inner {
    settings: Settings,
    readiness: Mutex<Option<(Instant, bool)>>,
}

/// Shared, cheaply cloned state.
#[derive(Clone)]
pub struct AppState {
    inner: Arc<Inner>,
}

/// How long a readiness verdict is reused.
pub const READINESS_CACHE: Duration = Duration::from_secs(5);

impl AppState {
    /// State over settings.
    #[must_use]
    pub fn new(settings: Settings) -> Self {
        Self {
            inner: Arc::new(Inner {
                settings,
                readiness: Mutex::new(None),
            }),
        }
    }

    /// The authenticator.
    #[must_use]
    pub fn authenticator(&self) -> &dyn Authenticator {
        self.inner.settings.authenticator.as_ref()
    }

    /// The authorizer.
    #[must_use]
    pub fn authorizer(&self) -> &dyn Authorizer {
        self.inner.settings.authorizer.as_ref()
    }

    /// The Kubernetes adapter.
    #[must_use]
    pub fn kube(&self) -> &KubeAdapter {
        &self.inner.settings.kube
    }

    /// The cursor key.
    #[must_use]
    pub fn cursor_key(&self) -> &CursorKey {
        &self.inner.settings.cursor_key
    }

    /// The current time.
    #[must_use]
    pub fn now(&self) -> DateTime<Utc> {
        self.inner.settings.clock.now()
    }

    /// The configured public origin.
    #[must_use]
    pub fn public_origin(&self) -> &str {
        &self.inner.settings.public_origin
    }

    /// The served `Host` values.
    #[must_use]
    pub fn allowed_hosts(&self) -> &[String] {
        &self.inner.settings.allowed_hosts
    }

    /// The static assets.
    #[must_use]
    pub fn assets(&self) -> &StaticAssets {
        &self.inner.settings.assets
    }

    /// Shared mode's extras, when this process is in shared mode.
    #[must_use]
    pub fn shared(&self) -> Option<&Arc<SharedMode>> {
        self.inner.settings.shared.as_ref()
    }

    /// The Kubernetes identity this process writes as.
    #[must_use]
    pub fn kubernetes_principal(&self) -> &str {
        &self.inner.settings.kubernetes_principal
    }

    /// Readiness, cached for [`READINESS_CACHE`]: Kubernetes answers for this
    /// service's identity and, in shared mode, the OIDC provider's discovery
    /// document and keys are available.
    pub async fn ready(&self) -> bool {
        let mut cached = self.inner.readiness.lock().await;
        if let Some((at, verdict)) = *cached {
            if at.elapsed() < READINESS_CACHE {
                return verdict;
            }
        }
        let kubernetes = self
            .kube()
            .probe(&self.inner.settings.readiness_namespace)
            .await
            .is_ok();
        // SHARED MODE IS ALSO NOT READY WITHOUT ITS PROVIDER (D0: discovery and
        // JWKS must initialise). The key material was checked before the
        // socket existed; this is the part that can change while running.
        let provider = match self.shared() {
            None => true,
            Some(shared) => shared.provider.ready().await,
        };
        let verdict = kubernetes && provider;
        *cached = Some((Instant::now(), verdict));
        verdict
    }
}

/// The complete router, middleware included.
pub fn router(state: AppState) -> Router {
    use crate::routes::{
        approvals, backups, cadence_previews, catalogs, connections, destinations, health,
        namespaces, operations, preflights, protection, rehearsals, restores, retention, schedules,
        session, topic_discoveries, trust,
    };

    let api = Router::new()
        .route("/api/v1/session", get(session::get_session))
        // D1 §4.4's exact path. It has no `{ns}` because it reads nothing: a
        // draft cadence is not an object, and this route makes no Kubernetes
        // call at all.
        .route("/api/v1/cadence-previews", get(cadence_previews::preview))
        .route("/api/v1/namespaces", get(namespaces::list_namespaces))
        .route(
            "/api/v1/namespaces/{ns}/connections",
            get(connections::list).post(connections::create),
        )
        .route(
            "/api/v1/namespaces/{ns}/connections/{name}",
            get(connections::get_one),
        )
        .route(
            "/api/v1/namespaces/{ns}/connections/{name}/topic-discoveries",
            get(topic_discoveries::by_connection).post(topic_discoveries::create),
        )
        .route(
            "/api/v1/namespaces/{ns}/destinations",
            get(destinations::list).post(destinations::create),
        )
        .route(
            "/api/v1/namespaces/{ns}/destinations:from-legacy",
            axum::routing::post(destinations::from_legacy),
        )
        .route(
            "/api/v1/namespaces/{ns}/destinations/{name}",
            get(destinations::get_one).post(destinations::command),
        )
        .route(
            "/api/v1/namespaces/{ns}/destinations/{name}/usage",
            get(destinations::usage),
        )
        .route(
            "/api/v1/namespaces/{ns}/topic-discoveries/{id}",
            get(topic_discoveries::get_one).post(topic_discoveries::cancel),
        )
        .route(
            "/api/v1/namespaces/{ns}/topic-discoveries/{id}/topics",
            get(topic_discoveries::topics),
        )
        .route(
            "/api/v1/namespaces/{ns}/preflights",
            axum::routing::post(preflights::create),
        )
        .route(
            "/api/v1/namespaces/{ns}/preflights/{id}",
            get(preflights::get_one).post(preflights::cancel),
        )
        .route(
            "/api/v1/namespaces/{ns}/preflights/{id}/details",
            get(preflights::details),
        )
        .route(
            "/api/v1/namespaces/{ns}/schedules",
            get(schedules::list).post(schedules::create),
        )
        .route(
            "/api/v1/namespaces/{ns}/schedules/{name}",
            get(schedules::get_one)
                .put(schedules::update)
                .post(schedules::command),
        )
        .route(
            "/api/v1/namespaces/{ns}/backups",
            get(backups::list).post(backups::create),
        )
        .route(
            "/api/v1/namespaces/{ns}/backups/{name}",
            get(backups::get_one),
        )
        .route(
            "/api/v1/namespaces/{ns}/restores",
            get(restores::list).post(restores::create),
        )
        .route(
            "/api/v1/namespaces/{ns}/restores/{name}",
            get(restores::get_one),
        )
        .route("/api/v1/namespaces/{ns}/approvals", get(approvals::list))
        .route(
            "/api/v1/namespaces/{ns}/approvals/{name}",
            get(approvals::get_one),
        )
        .route(
            "/api/v1/namespaces/{ns}/approvals/{name}/packet",
            get(approvals::packet),
        )
        .route(
            "/api/v1/namespaces/{ns}/operations/{kind}/{name}",
            get(operations::get_one),
        )
        // D3 §2.6's stream. It is a SEPARATE path rather than a content
        // negotiation on the read route: a route table that is the boundary
        // cannot have a route whose resource cost depends on an `Accept`
        // header, and the capability flag a console reads
        // (`operationEvents`) has to name something.
        .route(
            "/api/v1/namespaces/{ns}/operations/{kind}/{name}/events",
            get(operations::events),
        )
        // ---------------------------------------------------------------
        // D3 §10's read families. Every one is a bounded GET with the same
        // cursor, and the only write among them is the catalog create that
        // PLAT-15.2's "connect an existing archive" needs.
        // ---------------------------------------------------------------
        .route(
            "/api/v1/namespaces/{ns}/protection-policies",
            get(protection::list),
        )
        .route(
            "/api/v1/namespaces/{ns}/protection-policies/{name}",
            get(protection::get_one),
        )
        .route(
            "/api/v1/namespaces/{ns}/rehearsal-schedules",
            get(rehearsals::list),
        )
        .route(
            "/api/v1/namespaces/{ns}/rehearsal-schedules/{name}",
            get(rehearsals::get_one),
        )
        .route(
            "/api/v1/namespaces/{ns}/catalogs",
            get(catalogs::list).post(catalogs::create),
        )
        .route(
            "/api/v1/namespaces/{ns}/catalogs/{name}",
            get(catalogs::get_one),
        )
        .route(
            "/api/v1/namespaces/{ns}/catalogs/{name}/points",
            get(catalogs::points),
        )
        .route(
            "/api/v1/namespaces/{ns}/catalogs/{name}/signers",
            get(catalogs::signers),
        )
        .route(
            "/api/v1/namespaces/{ns}/retention-policies",
            get(retention::list),
        )
        .route(
            "/api/v1/namespaces/{ns}/retention-policies/{name}",
            get(retention::get_one),
        )
        // CLUSTER-SCOPED, AND THE ONLY ONE. A `TrustPolicy` names the
        // namespaces it governs, so it has no `{ns}` and its own
        // administrator-only rule; `routes::trust::authorize_cluster` is that
        // rule and records the decision under the `*` pseudo-namespace.
        .route("/api/v1/trust-policies", get(trust::list))
        .route("/api/v1/trust-policies/{name}", get(trust::get_one))
        // THE ACCESS LAYER, INSIDE THE ORIGIN GUARD. `crate::access::enforce`
        // is the floor no route can fall below: authentication and the
        // declared action, namespace first, before any handler runs. It sits
        // inside `unsafe_request_guard` so a cross-origin write is still
        // refused as one before anyone asks who sent it.
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::access::enforce,
        ))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::http::unsafe_request_guard,
        ));

    // SHARED MODE'S THREE EXTRA ROUTES. `/auth/login` and `/auth/callback` are
    // the only paths an unauthenticated caller reaches that do work, so they
    // carry their own rate limit; the logout command is an UNSAFE method under
    // the same Origin/JSON/CSRF guard as every other mutation.
    let api = match state.shared() {
        None => api,
        Some(_) => api.merge(
            Router::new()
                .route(
                    "/api/v1/session/logout",
                    axum::routing::post(crate::auth::login::logout),
                )
                .route_layer(axum::middleware::from_fn_with_state(
                    state.clone(),
                    crate::access::enforce,
                ))
                .route_layer(axum::middleware::from_fn_with_state(
                    state.clone(),
                    crate::http::unsafe_request_guard,
                )),
        ),
    };
    let auth = match state.shared() {
        None => Router::new(),
        Some(_) => Router::new()
            .route(
                crate::auth::login::LOGIN_PATH,
                get(crate::auth::login::login),
            )
            .route(
                crate::auth::login::CALLBACK_PATH,
                get(crate::auth::login::callback),
            )
            .route_layer(axum::middleware::from_fn_with_state(
                state.clone(),
                crate::access::enforce,
            )),
    };

    Router::new()
        .route("/healthz", get(health::healthz))
        .route("/readyz", get(health::readyz))
        .route("/", get(health::redirect_to_ui))
        .route("/ui", get(health::redirect_to_ui))
        .route("/ui/", get(crate::assets::serve))
        .route("/ui/{*path}", get(crate::assets::serve))
        // Public by DECLARATION: `crate::access::ROUTES` names each of these
        // `Public`, and a route added here without an entry fails closed.
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::access::enforce,
        ))
        .merge(api)
        .merge(auth)
        .fallback(fallback)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::http::boundary_guard,
        ))
        .layer(axum::middleware::from_fn(crate::http::request_context))
        .with_state(state)
}

async fn fallback() -> crate::problem::ApiError {
    crate::problem::ApiError::not_found()
}
