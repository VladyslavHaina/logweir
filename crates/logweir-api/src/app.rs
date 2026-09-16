//! Application state and the router.
//!
//! THE ROUTE TABLE IS THE BOUNDARY. Every path this service answers is listed
//! in [`router`]; there is no wildcard under `/api`, no `/apis`, no raw or
//! proxy route, and the fallback for everything else is `not_found`. The
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
    /// Proxy ranges whose forwarded headers may be RECORDED. Never read by a
    /// decision.
    pub trusted_proxy_cidrs: Vec<Cidr>,
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

    /// Readiness, cached for [`READINESS_CACHE`].
    pub async fn ready(&self) -> bool {
        let mut cached = self.inner.readiness.lock().await;
        if let Some((at, verdict)) = *cached {
            if at.elapsed() < READINESS_CACHE {
                return verdict;
            }
        }
        let verdict = self
            .kube()
            .probe(&self.inner.settings.readiness_namespace)
            .await
            .is_ok();
        *cached = Some((Instant::now(), verdict));
        verdict
    }
}

/// The complete router, middleware included.
pub fn router(state: AppState) -> Router {
    use crate::routes::{
        approvals, backups, connections, health, namespaces, operations, restores, schedules,
        session,
    };

    let api = Router::new()
        .route("/api/v1/session", get(session::get_session))
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
            "/api/v1/namespaces/{ns}/schedules",
            get(schedules::list).post(schedules::create),
        )
        .route(
            "/api/v1/namespaces/{ns}/schedules/{name}",
            get(schedules::get_one).post(schedules::command),
        )
        .route("/api/v1/namespaces/{ns}/backups", get(backups::list))
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
            ),
    };

    Router::new()
        .route("/healthz", get(health::healthz))
        .route("/readyz", get(health::readyz))
        .route("/", get(health::redirect_to_ui))
        .route("/ui", get(health::redirect_to_ui))
        .route("/ui/", get(crate::assets::serve))
        .route("/ui/{*path}", get(crate::assets::serve))
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
