//! Transport middleware, strict body and query parsing.
//!
//! THE LAYERS, OUTSIDE IN:
//!
//! 1. [`request_context`] — a fresh request ID (client-supplied IDs are
//!    ignored), `X-Request-ID`, the security headers on EVERY response,
//!    `Cache-Control: no-store` unless a static asset set its own, the
//!    problem+json rendering of every error, and one log line per request
//!    (method, path, status, latency — never a query string, header value or
//!    body).
//! 2. [`boundary_guard`] — refuses any `Impersonate-*` header and any `Host`
//!    this listener does not serve, before routing.
//! 3. The router. Under `/api/v1`, [`unsafe_request_guard`] runs as a route
//!    layer on every matched route: an unsafe method must carry `Origin`
//!    exactly equal to the configured public origin and
//!    `Content-Type: application/json`, before any handler or extractor runs.
//!    PLAT-17.2 adds its CSRF token check through
//!    `Authenticator::verify_unsafe`, not by editing a route.

use std::collections::BTreeMap;
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
use crate::problem::{
    self, ApiError, FieldError, PendingProblem, ProblemCode, PROBLEM_CONTENT_TYPE,
};

/// The largest JSON mutation body, 1 MiB.
pub const MAX_JSON_BODY: usize = 1024 * 1024;

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

/// Layer 1. See the module documentation.
pub async fn request_context(mut req: Request, next: Next) -> Response {
    let started = Instant::now();
    let request_id = ulid::Ulid::new().to_string();
    let method = req.method().clone();
    let path = crate::validate::bounded(req.uri().path(), 256);
    req.extensions_mut().insert(RequestId(request_id.clone()));

    let mut response = next.run(req).await;

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
    let latency_ms = started.elapsed().as_millis();
    if status >= 500 {
        tracing::warn!(request_id = %request_id, method = %method, path = %path, status, latency_ms, "request");
    } else {
        tracing::info!(request_id = %request_id, method = %method, path = %path, status, latency_ms, "request");
    }
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
pub async fn boundary_guard(State(state): State<AppState>, req: Request, next: Next) -> Response {
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
        return axum::response::IntoResponse::into_response(error);
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
        return axum::response::IntoResponse::into_response(ApiError::new(
            ProblemCode::MisdirectedRequest,
            "The Host header does not name an authority this listener serves.",
        ));
    }
    next.run(req).await
}

/// Layer 3, on every matched `/api/v1` route. See the module documentation.
pub async fn unsafe_request_guard(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    if !is_safe_method(req.method()) {
        let mut origins = req.headers().get_all(header::ORIGIN).iter();
        let origin_ok = match (origins.next(), origins.next()) {
            (Some(origin), None) => origin.as_bytes() == state.public_origin().as_bytes(),
            _ => false,
        };
        if !origin_ok {
            return axum::response::IntoResponse::into_response(ApiError::new(
                ProblemCode::OriginMismatch,
                "Unsafe requests must carry an Origin header equal to the configured public \
                 origin.",
            ));
        }
        if !is_json_content_type(req.headers()) {
            return axum::response::IntoResponse::into_response(ApiError::new(
                ProblemCode::UnsupportedMediaType,
                "Unsafe requests must send Content-Type: application/json.",
            ));
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

/// Read a JSON body of at most `limit` bytes into a strict DTO.
///
/// # Errors
///
/// `payload_too_large`, `malformed_request` (not JSON) or `validation_failed`
/// (well-formed JSON that does not match the DTO, unknown fields included).
pub async fn read_json<T: DeserializeOwned>(body: Body, limit: usize) -> Result<T, ApiError> {
    let bytes = match http_body_util::Limited::new(body, limit).collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(error) => {
            if error.is::<http_body_util::LengthLimitError>() {
                return Err(ApiError::new(
                    ProblemCode::PayloadTooLarge,
                    format!("The request body exceeds {limit} bytes."),
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
