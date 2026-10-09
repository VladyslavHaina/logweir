//! Transport middleware, strict body and query parsing.
//!
//! THE LAYERS, OUTSIDE IN:
//!
//! 1. [`request_context`] — a fresh request ID (client-supplied IDs are
//!    ignored), `X-Request-ID`, the security headers on EVERY response,
//!    `Cache-Control: no-store` unless a static asset set its own, the
//!    problem+json rendering of every error, one log line per request (method,
//!    path, status, latency — never a query string, header value or body), and
//!    the per-request [`crate::audit::AuditContext`], emitted once at the end.
//! 2. [`boundary_guard`] — refuses any `Impersonate-*` header, STRIPS the
//!    identity-claiming header family (`X-Remote-User`, `X-Forwarded-User`,
//!    `X-Auth-Request-*` and friends) so that nothing downstream can read one
//!    even by mistake, and refuses any `Host` this listener does not serve,
//!    before routing.
//! 3. The router. On every matched `/api/` route, [`unsafe_request_guard`]
//!    (applied over the whole router) requires that an unsafe method carry `Origin`
//!    exactly equal to the configured public origin and
//!    `Content-Type: application/json`, before any handler or extractor runs.
//!    The session's synchronizer CSRF token is checked after that, by
//!    `Authenticator::verify_unsafe` inside the `Actor` extractor.
//!
//! THERE IS NO CORS LAYER, AND THAT IS THE POINT. No response carries
//! `Access-Control-Allow-Origin` or `Access-Control-Allow-Credentials`, so a
//! browser will not let another origin read one; and because the exact-`Origin`
//! check refuses unsafe methods outright, a cross-origin write is refused even
//! when the attacker does not care about reading the answer.

use std::collections::BTreeMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::extract::{FromRequestParts, Request, State};
use axum::middleware::Next;
use axum::response::Response;
use http::header::{self, HeaderName, HeaderValue};
use http::request::Parts;
use http::Method;
use http_body_util::BodyExt as _;
use serde::de::DeserializeOwned;

use crate::app::AppState;
use crate::audit::AuditContext;
use crate::problem::{
    self, ApiError, FieldError, PendingProblem, ProblemCode, PROBLEM_CONTENT_TYPE,
};

/// The largest JSON mutation body, 1 MiB.
pub const MAX_JSON_BODY: usize = 1024 * 1024;

/// How long [`read_json`] waits for a whole body, from its first read to the
/// last byte (FX-24b).
///
/// THE TRANSPORT'S PROGRESS WINDOW IS NOT ENOUGH ON ITS OWN. `main.rs` fails a
/// body read that receives less than its floor (32 KiB, FX-24c) in thirty
/// seconds, but a client that sends just over the floor in every window
/// never meets it, and a 1 MiB body at that pace would hold its connection
/// permit for a quarter of an hour. This is the total, and it is twice the
/// window so each bound keeps its own job: a body that stops, or trickles
/// below the floor, is ended by the window at thirty seconds, one that keeps
/// above the floor by this at sixty. Sixty seconds for at most 1 MiB asks for
/// 17 KiB/s, which any client that means to send the body clears; the
/// console's own bodies are a few KiB.
pub const JSON_BODY_DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);

/// The Content-Security-Policy on every response, verbatim from the contract
/// decision.
pub const CONTENT_SECURITY_POLICY: &str =
    "default-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'";

/// The Permissions-Policy on every response.
pub const PERMISSIONS_POLICY: &str =
    "accelerometer=(), camera=(), geolocation=(), gyroscope=(), magnetometer=(), microphone=(), payment=(), usb=()";

/// The request ID of the current request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestId(pub String);

impl<S: Send + Sync> FromRequestParts<S> for RequestId {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<RequestId>()
            .cloned()
            .ok_or_else(|| ApiError::new(ProblemCode::InternalError, "The request has no ID."))
    }
}

/// GET, HEAD and OPTIONS. Everything else is unsafe.
#[must_use]
pub fn is_safe_method(method: &Method) -> bool {
    matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS)
}

/// The immediate peer's address, inserted per connection by the server loop.
///
/// It is the SOCKET peer, which behind an ingress is the ingress. Nothing here
/// ever trusts `X-Forwarded-For` for it: see [`forwarded_client`] and
/// [`login_rate_key`], the only two readers of that header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PeerAddr(pub IpAddr);

/// Every `X-Forwarded-For` hop, across repeated header lines, in order. A line
/// that is not visible ASCII is skipped, and so are empty hops.
fn forwarded_for_hops(headers: &http::HeaderMap) -> Vec<&str> {
    headers
        .get_all(HeaderName::from_static("x-forwarded-for"))
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .filter(|hop| !hop.is_empty())
        .collect()
}

/// One `X-Forwarded-For` hop as an address: a bare IP address, or a socket
/// address (`203.0.113.5:4711`, `[2001:db8::5]:443`) with its port dropped.
/// Some proxies append the second form (Azure Application Gateway does); a
/// hop is written by the proxy that received it, so its form is that proxy's
/// and choosing it gives the client nothing (FX-13 review L2). Anything else
/// — `unknown`, a bracketed address with no port, a name — is not an address.
fn hop_address(hop: &str) -> Option<IpAddr> {
    hop.parse::<IpAddr>()
        .ok()
        .or_else(|| hop.parse::<std::net::SocketAddr>().ok().map(|s| s.ip()))
}

/// THE RIGHTMOST HOP THE TRUSTED PROXIES DID NOT ADD. A proxy APPENDS the
/// address it received from, so everything left of its entry was sent by the
/// client and may be invented; walking from the right past our own proxies'
/// addresses finds the first hop none of them vouch for — the client as the
/// outermost trusted proxy saw it (review L4). A hop that is not an address
/// ([`hop_address`]) cannot be a trusted proxy, so the walk stops there and
/// returns it. `None` when there are no hops, or when every hop is a trusted
/// proxy.
fn rightmost_untrusted_hop<'a>(
    trusted: &crate::trusted_proxy::TrustedProxies,
    hops: &[&'a str],
) -> Option<&'a str> {
    hops.iter()
        .rev()
        .copied()
        .find(|hop| hop_address(hop).is_none_or(|ip| !trusted.contains(ip)))
}

/// The forwarded client address, but ONLY when the immediate peer is inside a
/// configured trusted-proxy range.
///
/// It is used for the audit line's `forwardedFor` field. The sign-in limiter
/// chooses its bucket from the same hop, through [`login_rate_key`], under the
/// same trust check; nothing else reads it. No authentication, authorization,
/// redirect or callback reads it, whatever the peer is — a forged
/// `X-Forwarded-For` from a client can therefore change one log field's
/// presence and nothing about a decision.
#[must_use]
pub fn forwarded_client(
    state: &AppState,
    parts_headers: &http::HeaderMap,
    peer: Option<IpAddr>,
) -> Option<String> {
    let shared = state.shared()?;
    let peer = peer?;
    if !shared.trusted_proxies.contains(peer) {
        return None;
    }
    let hops = forwarded_for_hops(parts_headers);
    let client = rightmost_untrusted_hop(&shared.trusted_proxies, &hops)
        .or_else(|| hops.first().copied())?;
    Some(crate::validate::bounded(client, 64))
}

/// Which address a sign-in request is counted against, and why.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoginRateKey {
    /// The client as the trusted proxy saw it: the rightmost `X-Forwarded-For`
    /// hop that is not itself a trusted proxy.
    ForwardedClient(IpAddr),
    /// The socket peer: it is not a trusted proxy, or it is and its
    /// `X-Forwarded-For` is absent, names only trusted proxies, or ends in a
    /// hop that is not an address ([`hop_address`]).
    Peer(IpAddr),
}

impl LoginRateKey {
    /// The address the limiter counts against. The limiter folds it into its
    /// bucket ([`crate::auth::ratelimit::bucket_of`]): an IPv4-mapped address
    /// to IPv4, any other IPv6 address to its `/64`.
    #[must_use]
    pub fn address(self) -> IpAddr {
        match self {
            Self::ForwardedClient(ip) | Self::Peer(ip) => ip,
        }
    }

    /// The audit note's value: which of the two the key is. Never the address.
    #[must_use]
    pub fn basis(self) -> &'static str {
        match self {
            Self::ForwardedClient(_) => "forwardedClient",
            Self::Peer(_) => "peer",
        }
    }
}

/// The sign-in limiter's key for one request (FX-13).
///
/// THE FORWARDED CLIENT, ONLY FROM A TRUSTED PEER. When `peer` is a trusted
/// proxy — [`crate::trusted_proxy::TrustedProxies::contains`], the very check
/// [`entry_point`] makes, so a Service source that is not read yet or is older
/// than its `MAX_AGE` trusts nobody here either — the key is the rightmost
/// `X-Forwarded-For` hop that is not itself a trusted proxy. Behind one
/// ingress that gives every client its own budget, where the peer alone made
/// one global budget that a single client could spend for everyone.
///
/// EVERYTHING ELSE IS THE PEER. An untrusted peer's header is never read, so a
/// forged one cannot move a request out of its own bucket. A trusted peer
/// whose header is absent, names only trusted proxies (the request began
/// inside the proxy tier) or ends in a hop that is not an address is
/// counted against the peer, as every request was before.
///
/// ONLY `X-Forwarded-For`. Traefik, the supported ingress, deletes a client's
/// `X-Forwarded-*` and appends the client's socket address, while it copies an
/// RFC 7239 `Forwarded` header through as the client wrote it; reading
/// `Forwarded` would let the client choose its bucket.
///
/// A BUCKET, NOT AN IDENTITY (D0, amended 2026-10-07). The address chooses
/// which counter is charged, and a counter can only refuse. It is not an
/// identity, a grant, or an audit actor.
#[must_use]
pub fn login_rate_key(
    trusted: &crate::trusted_proxy::TrustedProxies,
    headers: &http::HeaderMap,
    peer: IpAddr,
) -> LoginRateKey {
    if !trusted.contains(peer) {
        return LoginRateKey::Peer(peer);
    }
    let hops = forwarded_for_hops(headers);
    match rightmost_untrusted_hop(trusted, &hops).and_then(hop_address) {
        Some(client) => LoginRateKey::ForwardedClient(client),
        None => LoginRateKey::Peer(peer),
    }
}

/// The limit on `/auth/login` and `/auth/callback`, per client behind a
/// trusted proxy and otherwise per peer ([`login_rate_key`]).
///
/// # Errors
///
/// `rate_limited` with a `Retry-After`.
pub fn check_login_rate(state: &AppState, parts: &Parts) -> Result<(), ApiError> {
    let Some(shared) = state.shared() else {
        return Ok(());
    };
    let Some(PeerAddr(peer)) = parts.extensions.get::<PeerAddr>().copied() else {
        // No peer address means no connection-level information — an
        // in-process test router. There is nothing to key a limit on, and
        // inventing one would make the limit a coin toss.
        return Ok(());
    };
    let key = login_rate_key(&shared.trusted_proxies, &parts.headers, peer);
    let audit = parts.extensions.get::<Arc<AuditContext>>();
    if let Some(audit) = audit {
        audit.note("loginRateKey", key.basis());
    }
    match shared.login_limiter.check(key.address()) {
        crate::auth::ratelimit::Decision::Allowed => Ok(()),
        crate::auth::ratelimit::Decision::AllowedUntracked { announce } => {
            // The table of keys is full of live windows: this new key is
            // served without one rather than locked out. Many addresses are
            // signing in at once, which an operator wants to see: on every
            // such request's audit line, and in ONE warning a window — never
            // a second log line per request.
            if let Some(audit) = audit {
                audit.note("loginRateUntracked", "tableFull");
            }
            if announce {
                tracing::warn!(
                    tracked = crate::auth::ratelimit::MAX_TRACKED_PEERS,
                    "the sign-in limiter's table is full of live windows: new clients are \
                     served without a window until it drains (audit note loginRateUntracked); \
                     this is logged once a window"
                );
            }
            Ok(())
        }
        crate::auth::ratelimit::Decision::Limited {
            retry_after_seconds,
        } => {
            let mut error = ApiError::new(
                ProblemCode::RateLimited,
                "Too many sign-in attempts from this address. Try again shortly.",
            );
            error.retry_after_seconds = Some(retry_after_seconds);
            Err(error)
        }
    }
}

/// Layer 1. See the module documentation.
pub async fn request_context(mut req: Request, next: Next) -> Response {
    let started = Instant::now();
    let request_id = ulid::Ulid::new().to_string();
    let method = req.method().clone();
    let path = crate::validate::bounded(req.uri().path(), 256);
    req.extensions_mut().insert(RequestId(request_id.clone()));
    let audit = Arc::new(AuditContext::new(&request_id, method.as_str(), &path));
    req.extensions_mut().insert(Arc::clone(&audit));

    // THE AUDIT RECORD IS THE REQUEST'S AMBIENT CONTEXT. Everything the
    // handler awaits — including the adapter call that creates an object —
    // runs inside this scope, which is how `crate::kube` stamps a created
    // object with its author without any route passing one.
    let mut response = crate::audit::scope(Arc::clone(&audit), next.run(req)).await;

    if let Some(PendingProblem(error)) = response.extensions_mut().remove::<PendingProblem>() {
        response = rerender(&response, &error, &request_id);
    } else if response.status().is_client_error() || response.status().is_server_error() {
        let is_problem = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with(PROBLEM_CONTENT_TYPE));
        if !is_problem {
            let code = ProblemCode::for_status(response.status());
            let error = ApiError::new(code, default_detail(code));
            response = rerender(&response, &error, &request_id);
        }
    }

    let headers = response.headers_mut();
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        headers.insert(HeaderName::from_static("x-request-id"), value);
    }
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(CONTENT_SECURITY_POLICY),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        HeaderName::from_static("permissions-policy"),
        HeaderValue::from_static(PERMISSIONS_POLICY),
    );
    headers.insert(
        HeaderName::from_static("cross-origin-opener-policy"),
        HeaderValue::from_static("same-origin"),
    );
    headers.insert(
        HeaderName::from_static("cross-origin-resource-policy"),
        HeaderValue::from_static("same-origin"),
    );
    if !headers.contains_key(header::CACHE_CONTROL) {
        headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    }

    let status = response.status().as_u16();
    let latency = started.elapsed();
    let latency_ms = latency.as_millis();
    if status >= 500 {
        tracing::warn!(request_id = %request_id, method = %method, path = %path, status, latency_ms, "request");
    } else {
        tracing::info!(request_id = %request_id, method = %method, path = %path, status, latency_ms, "request");
    }
    // ONE AUDIT RECORD PER REQUEST, EMITTED HERE, so no handler can forget.
    crate::audit::emit(
        &audit.finish(status, latency.as_millis().min(u128::from(u64::MAX)) as u64),
        &audit.notes(),
    );
    response
}

/// Replace a response with the rendered problem, keeping `Allow`.
fn rerender(original: &Response, error: &ApiError, request_id: &str) -> Response {
    let mut rendered = problem::render(error, request_id);
    if let Some(allow) = original.headers().get(header::ALLOW) {
        rendered.headers_mut().insert(header::ALLOW, allow.clone());
    }
    rendered
}

fn default_detail(code: ProblemCode) -> &'static str {
    match code {
        ProblemCode::NotFound => "No such resource.",
        ProblemCode::MethodNotAllowed => "The route does not accept this method.",
        ProblemCode::PayloadTooLarge => "The request body is too large.",
        ProblemCode::UnsupportedMediaType => "The request body must be application/json.",
        ProblemCode::MalformedRequest => "The request is malformed.",
        ProblemCode::ValidationFailed => "The request failed validation.",
        _ => "The request could not be completed.",
    }
}

/// The two paths the `Host` allowlist does not cover. See [`boundary_guard`].
pub const HOST_EXEMPT_PATHS: [&str; 2] = ["/healthz", "/readyz"];

/// Layer 2. See the module documentation.
///
/// THE `Host` ALLOWLIST DOES NOT COVER THE TWO PROBES, and that is deliberate
/// rather than an oversight. [`crate::config`] derives the allowed authorities
/// from `publicOrigin` and the listen port, which is right for every route that
/// acts with an actor's authority: it is what stops a DNS-rebinding page from
/// reaching a loopback listener through a name that resolves to 127.0.0.1.
///
/// A kubelet HTTP probe cannot satisfy it. It addresses the Pod directly and
/// sends `Host: <podIP>:<port>`, an address no administrator configures and no
/// origin names, so with the guard applied to `/healthz` and `/readyz` a
/// deployed console would answer both probes 421 and never become ready. That
/// is a real trap for the stage that packages this service, not a hypothetical.
///
/// Exempting them leaks nothing. Neither reads a header, a cookie, a query or a
/// body; neither consults the authenticator, the authorizer or any actor;
/// neither takes a namespace. `/healthz` answers a fixed `{"status":"ok"}`
/// without touching a dependency, and `/readyz` answers `{"status":"ready"}` or
/// a problem that names no endpoint, no cluster and no reason — the
/// `the_readiness_probe_never_says_why` test holds that line. A rebinding page
/// that reaches them learns that a Logweir API is listening, which is the same
/// thing a refused connection tells it.
///
/// The `Impersonate-*` refusal below is NOT exempted. It costs one header scan
/// and there is no reason a probe would carry one.
pub async fn boundary_guard(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Response {
    // THE IDENTITY-CLAIMING HEADERS ARE REMOVED, NOT MERELY IGNORED. Ignoring
    // them is a property of every reader; removing them is a property of the
    // request, and it is the one a future route cannot undo by accident. Their
    // NAMES go into the audit record; their values are never read.
    //
    // This runs BEFORE the impersonation refusal so that the refused request
    // gets a complete audit line too: a caller who sends both is exactly the
    // caller an operator most wants the record for.
    let stripped = strip_identity_headers(req.headers_mut());
    let peer = req.extensions().get::<PeerAddr>().copied();
    if let Some(audit) = req.extensions().get::<Arc<AuditContext>>().cloned() {
        audit.set_kubernetes_principal(state.kubernetes_principal());
        let forwarded = forwarded_client(&state, req.headers(), peer.map(|p| p.0));
        audit.set_transport(
            &peer.map(|p| p.0.to_string()).unwrap_or_default(),
            forwarded.as_deref(),
            &stripped,
        );
    }

    if let Some(name) = req
        .headers()
        .keys()
        .find(|name| name.as_str().starts_with("impersonate-"))
    {
        let mut error = ApiError::new(
            ProblemCode::HeaderNotAllowed,
            "Impersonation headers are refused; this service never impersonates.",
        );
        error.errors.push(FieldError::new(
            name.as_str().to_string(),
            "not_allowed",
            "Impersonate-* headers are not accepted",
        ));
        return refuse(&req, error);
    }
    // An EXACT path match, against the router's own paths. No prefix and no
    // normalisation, so `/healthz/../api/v1/session` is not exempt — it is not
    // a route either, and the fallback answers it 404.
    let exempt = HOST_EXEMPT_PATHS.contains(&req.uri().path());
    let host_ok = exempt || {
        let mut hosts = req.headers().get_all(header::HOST).iter();
        match (hosts.next(), hosts.next()) {
            (Some(host), None) => host.to_str().is_ok_and(|h| {
                state
                    .allowed_hosts()
                    .iter()
                    .any(|a| a.eq_ignore_ascii_case(h))
            }),
            _ => false,
        }
    };
    if !host_ok {
        return refuse(
            &req,
            ApiError::new(
                ProblemCode::MisdirectedRequest,
                "The Host header does not name an authority this listener serves.",
            ),
        );
    }
    if !exempt {
        if let Err(reason) = entry_point(&state, req.headers(), peer.map(|p| p.0)) {
            if let Some(audit) = req.extensions().get::<Arc<AuditContext>>() {
                audit.set_failure("untrusted_entry_point");
                audit.note("entryPoint", reason);
            }
            return axum::response::IntoResponse::into_response(ApiError::new(
                ProblemCode::MisdirectedRequest,
                "This console accepts requests only through its trusted HTTPS entry point.",
            ));
        }
    }
    next.run(req).await
}

/// The trusted-entry-point contract (`requireTrustedProxy`, shared mode).
///
/// WHAT IT IS. With the flag on, a request is served only when BOTH hold:
/// the immediate socket peer is inside a `trustedProxyCidrs` range — the
/// ingress, not a pod that dialled the ClusterIP directly — and that proxy
/// asserted, in exactly one `X-Forwarded-Proto` header, that the browser's
/// hop was `https`. A request from anywhere else, or one the proxy did not
/// vouch for, is `421 misdirected_request` (audit code
/// `untrusted_entry_point`, with `peerNotTrusted` or
/// `forwardedProtoNotHttps` as the note). The two probes are exempt, because a
/// kubelet dials the Pod IP.
///
/// WHAT IT IS NOT, AND WHY IT DOES NOT BREAK D0. D0 forbids deriving an
/// IDENTITY, a callback URL or an authorization decision from forwarded
/// headers, and nothing here does: the header is read only from a peer the
/// administrator named, and it can only ever REFUSE. A forged
/// `X-Forwarded-Proto` from an untrusted peer is never read — the peer check
/// refuses first — and a correct one grants nothing that a session and a role
/// binding did not already grant. It is the application's own copy of what an
/// enforcing NetworkPolicy gives the ingress path, for the clusters (Docker
/// Desktop among them) where a NetworkPolicy is accepted and not enforced.
///
/// # Errors
///
/// The reason, for the audit note.
pub fn entry_point(
    state: &AppState,
    headers: &http::HeaderMap,
    peer: Option<IpAddr>,
) -> Result<(), &'static str> {
    let Some(shared) = state.shared() else {
        return Ok(());
    };
    if !shared.require_trusted_proxy {
        return Ok(());
    }
    let trusted = peer.is_some_and(|peer| shared.trusted_proxies.contains(peer));
    if !trusted {
        return Err("peerNotTrusted");
    }
    let mut protos = headers
        .get_all(HeaderName::from_static("x-forwarded-proto"))
        .iter();
    let https = match (protos.next(), protos.next()) {
        (Some(value), None) => value
            .to_str()
            .is_ok_and(|v| v.trim().eq_ignore_ascii_case("https")),
        _ => false,
    };
    if !https {
        return Err("forwardedProtoNotHttps");
    }
    Ok(())
}

/// Render a transport-boundary refusal AND attribute it.
///
/// THE AUDIT RECORD MUST NAME THE CHECK THAT REFUSED, NOT THE STATUS.
/// Without this the four refusals below reached `AuditContext::finish` with no
/// failure code, which substitutes `http_<status>` — so an operator reading the
/// log saw `http_400` for an impersonation attempt and `http_403` for both a
/// forged `Origin` and a missing one, and could not tell a cross-origin write
/// from a bad CSRF token. These are precisely the attack-shaped refusals, and
/// D0's "Audit attribution" asks the record to keep the real reason. Review
/// finding F-1.
///
/// The response is unchanged; only the record gains the code the body already
/// carried.
fn refuse(req: &Request, error: ApiError) -> Response {
    if let Some(audit) = req.extensions().get::<Arc<AuditContext>>() {
        audit.set_failure(error.code.as_str());
    }
    axum::response::IntoResponse::into_response(error)
}

/// Remove every identity-claiming header, returning the names removed.
///
/// `Impersonate-*` is NOT in this set: [`boundary_guard`] answers those 400
/// before reaching here, because a client sending one is asking this service to
/// do something it will never do, and a silent strip would hide that.
#[must_use]
pub fn strip_identity_headers(headers: &mut http::HeaderMap) -> Vec<String> {
    let doomed: Vec<HeaderName> = headers
        .keys()
        .filter(|name| crate::audit::is_ignored_identity_header(name.as_str()))
        .cloned()
        .collect();
    let mut removed = Vec::with_capacity(doomed.len());
    for name in doomed {
        headers.remove(&name);
        removed.push(name.as_str().to_string());
    }
    removed.sort();
    removed
}

/// Layer 3, on every matched `/api/v1` route. See the module documentation.
pub async fn unsafe_request_guard(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    // THE `/api/` ROUTES ONLY. This layer wraps the whole router so that no
    // route is outside it by position; the static page, the probes and the
    // sign-in steps take no unsafe method (the router answers 405), and an
    // unrouted path is the fallback's 404.
    let api_route = req
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .is_some_and(|m| m.as_str().starts_with("/api/"));
    if api_route && !is_safe_method(req.method()) {
        let mut origins = req.headers().get_all(header::ORIGIN).iter();
        let origin_ok = match (origins.next(), origins.next()) {
            (Some(origin), None) => origin.as_bytes() == state.public_origin().as_bytes(),
            _ => false,
        };
        if !origin_ok {
            return refuse(
                &req,
                ApiError::new(
                    ProblemCode::OriginMismatch,
                    "Unsafe requests must carry an Origin header equal to the configured public \
                     origin.",
                ),
            );
        }
        if !is_json_content_type(req.headers()) {
            return refuse(
                &req,
                ApiError::new(
                    ProblemCode::UnsupportedMediaType,
                    "Unsafe requests must send Content-Type: application/json.",
                ),
            );
        }
    }
    next.run(req).await
}

/// `application/json`, optionally with `charset=utf-8`, sent once.
#[must_use]
pub fn is_json_content_type(headers: &http::HeaderMap) -> bool {
    let mut values = headers.get_all(header::CONTENT_TYPE).iter();
    let (Some(value), None) = (values.next(), values.next()) else {
        return false;
    };
    let Ok(text) = value.to_str() else {
        return false;
    };
    let mut parts = text.split(';').map(str::trim);
    if !parts
        .next()
        .is_some_and(|essence| essence.eq_ignore_ascii_case("application/json"))
    {
        return false;
    }
    parts.all(|param| {
        param.split_once('=').is_some_and(|(k, v)| {
            k.trim().eq_ignore_ascii_case("charset")
                && v.trim().trim_matches('"').eq_ignore_ascii_case("utf-8")
        })
    })
}

/// Read a JSON body of at most `limit` bytes, within [`JSON_BODY_DEADLINE`],
/// into a strict DTO.
///
/// A body that is not whole by the deadline, or that stops arriving for the
/// transport's stall deadline, is `malformed_request`: what arrived is not a
/// request. Dropping the unfinished body is also what makes hyper close the
/// connection after the answer rather than wait for the rest.
///
/// # Errors
///
/// `payload_too_large`, `malformed_request` (not JSON, or not received in
/// time) or `validation_failed` (well-formed JSON that does not match the
/// DTO, unknown fields included).
pub async fn read_json<T: DeserializeOwned>(body: Body, limit: usize) -> Result<T, ApiError> {
    let collected = tokio::time::timeout(
        JSON_BODY_DEADLINE,
        http_body_util::Limited::new(body, limit).collect(),
    )
    .await;
    let bytes = match collected {
        Ok(Ok(collected)) => collected.to_bytes(),
        Err(_) => {
            return Err(ApiError::new(
                ProblemCode::MalformedRequest,
                format!(
                    "The request body was not received within {} seconds.",
                    JSON_BODY_DEADLINE.as_secs()
                ),
            ));
        }
        Ok(Err(error)) => {
            if error.is::<http_body_util::LengthLimitError>() {
                return Err(ApiError::new(
                    ProblemCode::PayloadTooLarge,
                    format!("The request body exceeds {limit} bytes."),
                ));
            }
            if crate::transport::is_timeout(error.as_ref()) {
                // The body's progress window (FX-24b; its floor, FX-24c): no
                // window's worth of bytes arrived in time.
                return Err(ApiError::new(
                    ProblemCode::MalformedRequest,
                    "The request body stopped arriving, or arrived too slowly, before it was \
                     complete.",
                ));
            }
            return Err(ApiError::new(
                ProblemCode::MalformedRequest,
                "The request body could not be read.",
            ));
        }
    };
    serde_json::from_slice::<T>(&bytes).map_err(|error| match error.classify() {
        serde_json::error::Category::Data => ApiError::validation(vec![data_error(&error)]),
        _ => ApiError::new(
            ProblemCode::MalformedRequest,
            "The request body is not valid JSON.",
        ),
    })
}

fn backticked(message: &str) -> Option<String> {
    let start = message.find('`')? + 1;
    let end = start + message[start..].find('`')?;
    Some(message[start..end].to_string())
}

fn data_error(error: &serde_json::Error) -> FieldError {
    let message = error.to_string();
    if message.starts_with("unknown field") {
        FieldError::new(
            backticked(&message).unwrap_or_else(|| "body".into()),
            "unknown_field",
            "this field is not part of the request contract",
        )
    } else if message.starts_with("missing field") {
        FieldError::new(
            backticked(&message).unwrap_or_else(|| "body".into()),
            "required",
            "this field is required",
        )
    } else if message.starts_with("unknown variant") {
        FieldError::new(
            "body",
            "invalid_value",
            "a value is not one of the allowed values",
        )
    } else if message.starts_with("invalid type") {
        FieldError::new("body", "invalid_type", "a value has the wrong JSON type")
    } else {
        FieldError::new(
            "body",
            "invalid",
            "the body does not match the request contract",
        )
    }
}

/// Parse a query string, refusing unknown and repeated parameters.
///
/// # Errors
///
/// `malformed_request` naming the parameter.
pub fn parse_query(
    raw: Option<&str>,
    allowed: &[&str],
) -> Result<BTreeMap<String, String>, ApiError> {
    let mut out = BTreeMap::new();
    let Some(raw) = raw.filter(|r| !r.is_empty()) else {
        return Ok(out);
    };
    let pairs: Vec<(String, String)> = serde_urlencoded::from_str(raw).map_err(|_| {
        ApiError::new(
            ProblemCode::MalformedRequest,
            "The query string is malformed.",
        )
    })?;
    for (key, value) in pairs {
        if !allowed.contains(&key.as_str()) {
            let mut e = ApiError::new(
                ProblemCode::MalformedRequest,
                "The query string names a parameter this route does not accept.",
            );
            e.errors.push(FieldError::new(
                crate::validate::bounded(&key, 64),
                "unknown_parameter",
                "not accepted by this route",
            ));
            return Err(e);
        }
        if out.insert(key.clone(), value).is_some() {
            let mut e = ApiError::new(
                ProblemCode::MalformedRequest,
                "A query parameter is repeated.",
            );
            e.errors.push(FieldError::new(
                key,
                "repeated_parameter",
                "send each parameter once",
            ));
            return Err(e);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **The identity-claiming headers are REMOVED from the map, not merely
    /// left for everyone to ignore.**
    ///
    /// REGRESSION REASON. A planted mutant that kept reporting the names while
    /// skipping `headers.remove` SURVIVED the whole suite: every other test
    /// asserts a consequence (the actor is unchanged, the names are audited),
    /// and both consequences hold whether or not the header is still on the
    /// request. The difference only matters for the code that has not been
    /// written yet — which is exactly the code the stripping exists to protect
    /// — so it needs a test that looks at the map itself.
    #[test]
    fn identity_headers_are_removed_from_the_request_not_just_ignored() {
        let mut headers = http::HeaderMap::new();
        for (name, value) in [
            ("x-remote-user", "impostor"),
            ("x-remote-groups", "cluster-admins"),
            ("x-remote-extra-scopes", "everything"),
            ("x-forwarded-user", "impostor"),
            ("x-forwarded-email", "impostor@example.test"),
            ("x-auth-request-user", "impostor"),
            ("x-auth-request-anything", "impostor"),
            // Kept: these are not identity claims.
            ("host", "console.test"),
            ("cookie", "__Host-logweir_session=x"),
            ("origin", "https://console.test"),
            ("x-forwarded-for", "203.0.113.9"),
            ("x-forwarded-proto", "https"),
        ] {
            headers.insert(
                HeaderName::from_static(name),
                HeaderValue::from_static(value),
            );
        }

        let removed = strip_identity_headers(&mut headers);
        assert_eq!(
            removed,
            vec![
                "x-auth-request-anything",
                "x-auth-request-user",
                "x-forwarded-email",
                "x-forwarded-user",
                "x-remote-extra-scopes",
                "x-remote-groups",
                "x-remote-user",
            ]
        );
        for name in &removed {
            assert!(
                !headers.contains_key(name.as_str()),
                "{name} is still on the request after being reported as stripped"
            );
        }
        // The value is gone, not merely the first of several.
        assert!(headers.get_all("x-remote-user").iter().next().is_none());
        // And nothing else was taken.
        assert_eq!(headers.len(), 5, "{headers:?}");
        for kept in [
            "host",
            "cookie",
            "origin",
            "x-forwarded-for",
            "x-forwarded-proto",
        ] {
            assert!(headers.contains_key(kept), "{kept} was removed");
        }
        // A second pass finds nothing left to do.
        assert!(strip_identity_headers(&mut headers).is_empty());
    }

    // ------------------------------------------------ the sign-in limiter's key

    use crate::config::{Cidr, ServiceRef};
    use crate::trusted_proxy::TrustedProxies;

    fn addr(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    /// The ingress range of these tests: `10.42.0.0/16`.
    fn ingress() -> TrustedProxies {
        TrustedProxies::from_cidrs(vec![Cidr::parse("10.42.0.0/16").unwrap()])
    }

    fn xff(values: &[&str]) -> http::HeaderMap {
        let mut headers = http::HeaderMap::new();
        for value in values {
            headers.append(
                HeaderName::from_static("x-forwarded-for"),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        headers
    }

    fn key(trusted: &TrustedProxies, values: &[&str], peer: &str) -> LoginRateKey {
        login_rate_key(trusted, &xff(values), addr(peer))
    }

    /// **Two clients behind one trusted proxy are two keys; one client through
    /// two trusted proxies is one.** NEGATIVE CONTROL: the peer is the same
    /// for both clients, so a key that ignored the header (the defect) makes
    /// the first assertion fail, and one keyed on the peer makes the second's
    /// two proxies two keys.
    #[test]
    fn the_key_is_the_client_behind_a_trusted_proxy() {
        let t = ingress();
        let a = key(&t, &["203.0.113.50"], "10.42.0.17");
        let b = key(&t, &["203.0.113.51"], "10.42.0.17");
        assert_eq!(a, LoginRateKey::ForwardedClient(addr("203.0.113.50")));
        assert_eq!(b, LoginRateKey::ForwardedClient(addr("203.0.113.51")));
        assert_ne!(a.address(), b.address());

        // The same client through two ingress pods, and through a chain of
        // two: one key.
        let via_other = key(&t, &["203.0.113.50"], "10.42.0.18");
        let via_chain = key(&t, &["203.0.113.50, 10.42.0.18"], "10.42.0.17");
        assert_eq!(via_other.address(), a.address());
        assert_eq!(via_chain.address(), a.address());
        // An IPv4-mapped spelling of the same client is the same bucket.
        assert_eq!(
            crate::auth::ratelimit::bucket_of(
                key(&t, &["::ffff:203.0.113.50"], "10.42.0.17").address()
            ),
            crate::auth::ratelimit::bucket_of(a.address())
        );
        assert_eq!(a.basis(), "forwardedClient");
    }

    /// **The rightmost untrusted hop, never the leftmost.** Everything left of
    /// the hop the trusted proxy appended was written by the client; keying on
    /// it would let a client choose a fresh bucket per request.
    #[test]
    fn the_key_is_the_rightmost_hop_the_proxies_did_not_add() {
        let t = ingress();
        for values in [
            &["198.51.100.66, 203.0.113.50"][..],
            &["198.51.100.66, 203.0.113.50, 10.42.0.9"][..],
            // Repeated header lines are one chain, in order.
            &["198.51.100.66", "203.0.113.50"][..],
            // A malformed hop the CLIENT sent, left of the appended one, is
            // never read.
            &["not-an-address, 203.0.113.50"][..],
            // A proxy that appends `ip:port` (review L2): the port is dropped,
            // and a trusted hop with a port is still skipped.
            &["198.51.100.66, 203.0.113.50:4711"][..],
            &["198.51.100.66, 203.0.113.50:4711, 10.42.0.9:80"][..],
        ] {
            assert_eq!(
                key(&t, values, "10.42.0.17"),
                LoginRateKey::ForwardedClient(addr("203.0.113.50")),
                "{values:?}"
            );
        }
    }

    /// **A bracketed IPv6 socket address is its address.** NEGATIVE CONTROL:
    /// the same address bracketed without a port is not an address, and
    /// falls back to the peer.
    #[test]
    fn a_socket_address_hop_is_keyed_on_its_address() {
        let t = ingress();
        assert_eq!(
            key(&t, &["[2001:db8::5]:443"], "10.42.0.17"),
            LoginRateKey::ForwardedClient(addr("2001:db8::5"))
        );
        assert_eq!(
            key(&t, &["[2001:db8::5]"], "10.42.0.17"),
            LoginRateKey::Peer(addr("10.42.0.17"))
        );
    }

    /// **An untrusted peer's header is never read.** NEGATIVE CONTROL: the
    /// same header from the trusted peer DOES choose the key, so the
    /// assertion below fails for a limiter that reads it from anyone.
    #[test]
    fn an_untrusted_peers_header_is_ignored() {
        let t = ingress();
        assert_eq!(
            key(&t, &["203.0.113.50"], "10.99.3.4"),
            LoginRateKey::Peer(addr("10.99.3.4"))
        );
        assert_eq!(
            key(&t, &["203.0.113.50"], "10.42.0.17"),
            LoginRateKey::ForwardedClient(addr("203.0.113.50"))
        );
        // An RFC 7239 `Forwarded` header is never read, from anyone: Traefik
        // copies it through as the client wrote it.
        let mut forwarded = http::HeaderMap::new();
        forwarded.insert(
            HeaderName::from_static("forwarded"),
            HeaderValue::from_static("for=203.0.113.50"),
        );
        assert_eq!(
            login_rate_key(&t, &forwarded, addr("10.42.0.17")),
            LoginRateKey::Peer(addr("10.42.0.17"))
        );
        assert_eq!(LoginRateKey::Peer(addr("10.42.0.17")).basis(), "peer");
    }

    /// **A Service source that is not ready, or older than its `MAX_AGE`,
    /// trusts nobody here either**, exactly as the entry point's gate.
    /// NEGATIVE CONTROL: the same source, freshly read, does choose the
    /// client's key.
    #[test]
    fn a_trusted_set_not_ready_or_too_old_trusts_nobody() {
        let service = || {
            Some(ServiceRef {
                namespace: "traefik".into(),
                name: "traefik".into(),
            })
        };
        let unread = TrustedProxies::new(Vec::new(), service());
        assert!(!unread.ready());
        assert_eq!(
            key(&unread, &["203.0.113.50"], "10.1.0.7"),
            LoginRateKey::Peer(addr("10.1.0.7"))
        );

        let stale = TrustedProxies::new(Vec::new(), service());
        let old = std::time::Instant::now()
            .checked_sub(crate::trusted_proxy::MAX_AGE + std::time::Duration::from_secs(1))
            .expect("the clock is past the window");
        stale.replace_at([addr("10.1.0.7")], old);
        assert_eq!(
            key(&stale, &["203.0.113.50"], "10.1.0.7"),
            LoginRateKey::Peer(addr("10.1.0.7"))
        );

        let fresh = TrustedProxies::new(Vec::new(), service());
        fresh.replace_at([addr("10.1.0.7")], std::time::Instant::now());
        assert_eq!(
            key(&fresh, &["203.0.113.50"], "10.1.0.7"),
            LoginRateKey::ForwardedClient(addr("203.0.113.50"))
        );
    }

    /// **Absent, malformed, or made only of trusted proxies: the peer.** A
    /// chain of only proxies began inside the proxy tier, so it has no client
    /// hop to count. NEGATIVE CONTROL: the well-formed chain from the same
    /// peer keys on the client, so each fallback below is a real choice.
    #[test]
    fn a_header_without_a_client_hop_falls_back_to_the_peer() {
        let t = ingress();
        let peer = LoginRateKey::Peer(addr("10.42.0.17"));
        for values in [
            &[][..],
            &[""][..],
            &[" , "][..],
            &["10.42.0.9"][..],
            &["10.42.0.9, 10.42.0.17"][..],
            &["203.0.113.50, not-an-address"][..],
            &["unknown"][..],
            &["[2001:db8::1]"][..],
            &["203.0.113.50:not-a-port"][..],
        ] {
            assert_eq!(key(&t, values, "10.42.0.17"), peer, "{values:?}");
        }
        assert_eq!(
            key(&t, &["203.0.113.50"], "10.42.0.17"),
            LoginRateKey::ForwardedClient(addr("203.0.113.50"))
        );
    }

    #[test]
    fn json_content_types() {
        let mut h = http::HeaderMap::new();
        assert!(!is_json_content_type(&h));
        for (value, ok) in [
            ("application/json", true),
            ("Application/JSON; charset=UTF-8", true),
            ("application/json; charset=latin1", false),
            ("application/json; boundary=x", false),
            ("text/plain", false),
            ("application/merge-patch+json", false),
            ("application/jsonx", false),
        ] {
            h.insert(header::CONTENT_TYPE, value.parse().unwrap());
            assert_eq!(is_json_content_type(&h), ok, "{value}");
        }
    }

    #[test]
    fn queries_refuse_unknown_and_repeated_parameters() {
        assert!(parse_query(Some("limit=5&cursor=x"), &["limit", "cursor"]).is_ok());
        assert_eq!(
            parse_query(Some("limit=5&watch=true"), &["limit"])
                .unwrap_err()
                .code,
            ProblemCode::MalformedRequest
        );
        assert_eq!(
            parse_query(Some("limit=5&limit=6"), &["limit"])
                .unwrap_err()
                .code,
            ProblemCode::MalformedRequest
        );
    }
}
