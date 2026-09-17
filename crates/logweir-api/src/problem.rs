//! `application/problem+json` errors with a closed set of stable codes.
//!
//! EVERY ERROR THIS SERVICE RETURNS IS ONE OF [`ProblemCode`]. A handler
//! returns [`ApiError`]; its `IntoResponse` implementation stores the error in
//! the response extensions with an empty body, and the request-context
//! middleware (`crate::http::request_context`) renders the JSON body once it
//! knows the request ID. The same middleware converts any error response that
//! is NOT already a problem — an axum default 405, a path rejection — into the
//! problem for its status, so no plain-text error can leave the process.
//!
//! KUBERNETES MESSAGES ARE NEVER ECHOED. `detail` is always a sentence this
//! crate wrote; a Kubernetes `Status.message` is logged after redaction by the
//! adapter and never copied into a response.

use axum::response::{IntoResponse, Response};
use http::StatusCode;
use schemars::JsonSchema;
use serde::Serialize;

/// The stable problem codes. Clients branch on `code`, never on `detail`.
///
/// The decision's baseline set is here verbatim (`unauthenticated`,
/// `session_expired`, `forbidden`, `namespace_forbidden`, `not_found`,
/// `idempotency_conflict`, `state_conflict`, `approval_required`,
/// `policy_mismatch`, `precondition_failed`, `validation_failed`,
/// `rate_limited`, `kubernetes_unavailable`, `upstream_timeout`, plus the
/// cursor pair). The remainder name transport-level refusals the baseline did
/// not enumerate; each is documented in the OpenAPI document.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProblemCode {
    /// 400 — the body is not JSON, a query string is malformed or carries an
    /// unknown or repeated parameter.
    MalformedRequest,
    /// 400 — a header this service refuses outright (`Impersonate-*`).
    HeaderNotAllowed,
    /// 400 — a durable POST arrived without `Idempotency-Key`.
    IdempotencyKeyRequired,
    /// 400 — `Idempotency-Key` is not 8–128 visible ASCII characters, or is
    /// present on a route that does not accept it.
    IdempotencyKeyInvalid,
    /// 400 — a list cursor failed authentication or names another scope.
    CursorInvalid,
    /// 401 — no authenticated actor (PLAT-17.2 authenticators).
    Unauthenticated,
    /// 401 — the session is past its signed expiry (PLAT-17.2).
    SessionExpired,
    /// 403 — the actor is not permitted this action.
    Forbidden,
    /// 403 — the namespace is not explicitly granted to the actor.
    NamespaceForbidden,
    /// 403 — an unsafe method without an `Origin` exactly equal to the
    /// configured public origin.
    OriginMismatch,
    /// 404 — no such route or no such object.
    NotFound,
    /// 404 — a legacy object carries no frozen execution and the installation
    /// publishes no legacy addressing, so no destination can be derived from
    /// facts (D2 §3.12 step 2c).
    LegacyLocationUnknown,
    /// 405 — the route exists and does not accept this method.
    MethodNotAllowed,
    /// 409 — the idempotency key was already used with a different request.
    IdempotencyConflict,
    /// 409 — the deterministic object exists but was not created by this
    /// idempotency scope; it is never adopted.
    StateConflict,
    /// 409 — an approval is required first (PLAT-19.2).
    ApprovalRequired,
    /// 409 — `:update-access` tried to move a destination's location;
    /// `spec.storage` is immutable (D2 R1).
    DestinationLocationImmutable,
    /// 409 — `:update-access` tried to change a destination's transport;
    /// `spec.transport.security` is immutable in BOTH directions (D2 R2).
    TransportDowngradeForbidden,
    /// 409 — a legacy adoption named a destination whose `locationDigest`
    /// differs from the one derived from the legacy object (D2 §3.12).
    LegacyLocationMismatch,
    /// 409 — a stored result chunk failed its owner, immutability or digest
    /// check (D2 §5.6). The page is refused rather than served from bytes
    /// whose provenance did not hold.
    ResultIntegrityFailed,
    /// 409 — the bound approval policy does not match (PLAT-19.2).
    PolicyMismatch,
    /// 409 — a manual run named `expectedGeneration` and the schedule's
    /// policy has moved on since (D1 §8.2). The body carries the current
    /// generation and run-policy digest so the console can show what changed;
    /// confirming starts a NEW idempotency intent, because running a policy
    /// the person did not see is not what the button promised.
    PolicyChanged,
    /// 410 — the list cursor expired or Kubernetes compacted its continue
    /// token; restart the list without a cursor.
    CursorExpired,
    /// 412 — `expectedResourceVersion` is not the object's current version.
    PreconditionFailed,
    /// 413 — the request body exceeds its limit.
    PayloadTooLarge,
    /// 415 — an unsafe method without `Content-Type: application/json`.
    UnsupportedMediaType,
    /// 421 — the `Host` header names an authority this listener does not
    /// serve (DNS-rebinding defence).
    MisdirectedRequest,
    /// 422 — the request is well-formed JSON and fails validation.
    ValidationFailed,
    /// 422 — a destination request breaks one of D2 §3.2's rules. It carries
    /// the same field paths and rule ids the CRD's CEL and the controller use,
    /// so all three enforcement points name one mistake the same way.
    DestinationInvalid,
    /// 429 — Kubernetes throttled the request; see `Retry-After`.
    RateLimited,
    /// 500 — an invariant this service holds did not hold.
    InternalError,
    /// 503 — Kubernetes could not be reached or refused this service's own
    /// identity.
    KubernetesUnavailable,
    /// 504 — the Kubernetes call exceeded its 10-second deadline.
    UpstreamTimeout,
}

impl ProblemCode {
    /// Every code, in declaration order. The OpenAPI document and the tests
    /// iterate this list, so a new code cannot be undocumented or untested.
    pub const ALL: [ProblemCode; 33] = [
        ProblemCode::MalformedRequest,
        ProblemCode::HeaderNotAllowed,
        ProblemCode::IdempotencyKeyRequired,
        ProblemCode::IdempotencyKeyInvalid,
        ProblemCode::CursorInvalid,
        ProblemCode::Unauthenticated,
        ProblemCode::SessionExpired,
        ProblemCode::Forbidden,
        ProblemCode::NamespaceForbidden,
        ProblemCode::OriginMismatch,
        ProblemCode::NotFound,
        ProblemCode::LegacyLocationUnknown,
        ProblemCode::MethodNotAllowed,
        ProblemCode::IdempotencyConflict,
        ProblemCode::StateConflict,
        ProblemCode::ApprovalRequired,
        ProblemCode::PolicyMismatch,
        ProblemCode::PolicyChanged,
        ProblemCode::DestinationLocationImmutable,
        ProblemCode::TransportDowngradeForbidden,
        ProblemCode::LegacyLocationMismatch,
        ProblemCode::ResultIntegrityFailed,
        ProblemCode::CursorExpired,
        ProblemCode::PreconditionFailed,
        ProblemCode::PayloadTooLarge,
        ProblemCode::UnsupportedMediaType,
        ProblemCode::MisdirectedRequest,
        ProblemCode::ValidationFailed,
        ProblemCode::DestinationInvalid,
        ProblemCode::RateLimited,
        ProblemCode::InternalError,
        ProblemCode::KubernetesUnavailable,
        ProblemCode::UpstreamTimeout,
    ];

    /// The wire spelling, e.g. `namespace_forbidden`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            ProblemCode::MalformedRequest => "malformed_request",
            ProblemCode::HeaderNotAllowed => "header_not_allowed",
            ProblemCode::IdempotencyKeyRequired => "idempotency_key_required",
            ProblemCode::IdempotencyKeyInvalid => "idempotency_key_invalid",
            ProblemCode::CursorInvalid => "cursor_invalid",
            ProblemCode::Unauthenticated => "unauthenticated",
            ProblemCode::SessionExpired => "session_expired",
            ProblemCode::Forbidden => "forbidden",
            ProblemCode::NamespaceForbidden => "namespace_forbidden",
            ProblemCode::OriginMismatch => "origin_mismatch",
            ProblemCode::NotFound => "not_found",
            ProblemCode::LegacyLocationUnknown => "legacy_location_unknown",
            ProblemCode::MethodNotAllowed => "method_not_allowed",
            ProblemCode::IdempotencyConflict => "idempotency_conflict",
            ProblemCode::StateConflict => "state_conflict",
            ProblemCode::ApprovalRequired => "approval_required",
            ProblemCode::PolicyMismatch => "policy_mismatch",
            ProblemCode::PolicyChanged => "policy_changed",
            ProblemCode::DestinationLocationImmutable => "destination_location_immutable",
            ProblemCode::TransportDowngradeForbidden => "transport_downgrade_forbidden",
            ProblemCode::LegacyLocationMismatch => "legacy_location_mismatch",
            ProblemCode::ResultIntegrityFailed => "result_integrity_failed",
            ProblemCode::CursorExpired => "cursor_expired",
            ProblemCode::PreconditionFailed => "precondition_failed",
            ProblemCode::PayloadTooLarge => "payload_too_large",
            ProblemCode::UnsupportedMediaType => "unsupported_media_type",
            ProblemCode::MisdirectedRequest => "misdirected_request",
            ProblemCode::ValidationFailed => "validation_failed",
            ProblemCode::DestinationInvalid => "destination_invalid",
            ProblemCode::RateLimited => "rate_limited",
            ProblemCode::InternalError => "internal_error",
            ProblemCode::KubernetesUnavailable => "kubernetes_unavailable",
            ProblemCode::UpstreamTimeout => "upstream_timeout",
        }
    }

    /// The HTTP status this code is always returned with.
    #[must_use]
    pub const fn status(self) -> StatusCode {
        match self {
            ProblemCode::MalformedRequest
            | ProblemCode::HeaderNotAllowed
            | ProblemCode::IdempotencyKeyRequired
            | ProblemCode::IdempotencyKeyInvalid
            | ProblemCode::CursorInvalid => StatusCode::BAD_REQUEST,
            ProblemCode::Unauthenticated | ProblemCode::SessionExpired => StatusCode::UNAUTHORIZED,
            ProblemCode::Forbidden
            | ProblemCode::NamespaceForbidden
            | ProblemCode::OriginMismatch => StatusCode::FORBIDDEN,
            ProblemCode::NotFound | ProblemCode::LegacyLocationUnknown => StatusCode::NOT_FOUND,
            ProblemCode::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            ProblemCode::IdempotencyConflict
            | ProblemCode::StateConflict
            | ProblemCode::ApprovalRequired
            | ProblemCode::PolicyMismatch
            | ProblemCode::PolicyChanged
            | ProblemCode::DestinationLocationImmutable
            | ProblemCode::TransportDowngradeForbidden
            | ProblemCode::LegacyLocationMismatch
            | ProblemCode::ResultIntegrityFailed => StatusCode::CONFLICT,
            ProblemCode::CursorExpired => StatusCode::GONE,
            ProblemCode::PreconditionFailed => StatusCode::PRECONDITION_FAILED,
            ProblemCode::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            ProblemCode::UnsupportedMediaType => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ProblemCode::MisdirectedRequest => StatusCode::MISDIRECTED_REQUEST,
            ProblemCode::ValidationFailed | ProblemCode::DestinationInvalid => {
                StatusCode::UNPROCESSABLE_ENTITY
            }
            ProblemCode::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            ProblemCode::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
            ProblemCode::KubernetesUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            ProblemCode::UpstreamTimeout => StatusCode::GATEWAY_TIMEOUT,
        }
    }

    /// A short, fixed title for the code.
    #[must_use]
    pub const fn title(self) -> &'static str {
        match self {
            ProblemCode::MalformedRequest => "Malformed request",
            ProblemCode::HeaderNotAllowed => "Header not allowed",
            ProblemCode::IdempotencyKeyRequired => "Idempotency key required",
            ProblemCode::IdempotencyKeyInvalid => "Idempotency key invalid",
            ProblemCode::CursorInvalid => "Cursor invalid",
            ProblemCode::Unauthenticated => "Authentication required",
            ProblemCode::SessionExpired => "Session expired",
            ProblemCode::Forbidden => "Forbidden",
            ProblemCode::NamespaceForbidden => "Namespace not granted",
            ProblemCode::OriginMismatch => "Origin not allowed",
            ProblemCode::NotFound => "Not found",
            ProblemCode::LegacyLocationUnknown => "Legacy location unknown",
            ProblemCode::MethodNotAllowed => "Method not allowed",
            ProblemCode::IdempotencyConflict => "Idempotency key conflict",
            ProblemCode::StateConflict => "State conflict",
            ProblemCode::ApprovalRequired => "Approval required",
            ProblemCode::PolicyMismatch => "Policy mismatch",
            ProblemCode::PolicyChanged => "Schedule policy changed",
            ProblemCode::DestinationLocationImmutable => "Destination location immutable",
            ProblemCode::TransportDowngradeForbidden => "Transport change forbidden",
            ProblemCode::LegacyLocationMismatch => "Legacy location mismatch",
            ProblemCode::ResultIntegrityFailed => "Result integrity failed",
            ProblemCode::CursorExpired => "Cursor expired",
            ProblemCode::PreconditionFailed => "Precondition failed",
            ProblemCode::PayloadTooLarge => "Payload too large",
            ProblemCode::UnsupportedMediaType => "Unsupported media type",
            ProblemCode::MisdirectedRequest => "Misdirected request",
            ProblemCode::ValidationFailed => "Request validation failed",
            ProblemCode::DestinationInvalid => "Destination invalid",
            ProblemCode::RateLimited => "Rate limited",
            ProblemCode::InternalError => "Internal error",
            ProblemCode::KubernetesUnavailable => "Kubernetes unavailable",
            ProblemCode::UpstreamTimeout => "Upstream timeout",
        }
    }

    /// Whether repeating the identical request later can succeed.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(
            self,
            ProblemCode::RateLimited
                | ProblemCode::KubernetesUnavailable
                | ProblemCode::UpstreamTimeout
                | ProblemCode::InternalError
        )
    }

    /// The `type` URI: `https://logweir.dev/problems/<code with hyphens>`.
    #[must_use]
    pub fn type_uri(self) -> String {
        format!(
            "https://logweir.dev/problems/{}",
            self.as_str().replace('_', "-")
        )
    }

    /// The code a bare HTTP error status maps to when a response was produced
    /// outside [`ApiError`] (axum's own 404/405, an extractor rejection).
    #[must_use]
    pub fn for_status(status: StatusCode) -> ProblemCode {
        match status.as_u16() {
            400 => ProblemCode::MalformedRequest,
            401 => ProblemCode::Unauthenticated,
            403 => ProblemCode::Forbidden,
            404 => ProblemCode::NotFound,
            405 => ProblemCode::MethodNotAllowed,
            409 => ProblemCode::StateConflict,
            410 => ProblemCode::CursorExpired,
            412 => ProblemCode::PreconditionFailed,
            413 => ProblemCode::PayloadTooLarge,
            415 => ProblemCode::UnsupportedMediaType,
            421 => ProblemCode::MisdirectedRequest,
            422 => ProblemCode::ValidationFailed,
            429 => ProblemCode::RateLimited,
            503 => ProblemCode::KubernetesUnavailable,
            504 => ProblemCode::UpstreamTimeout,
            _ => ProblemCode::InternalError,
        }
    }
}

/// One field-level validation failure.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FieldError {
    /// The JSON path of the field, e.g. `topics[2]`, or a header or query
    /// parameter name.
    pub field: String,
    /// A stable machine-readable code for this field, e.g. `required`.
    pub code: String,
    /// A human-readable sentence. Never echoes a credential.
    pub message: String,
}

impl FieldError {
    /// A field error from its three parts.
    pub fn new(
        field: impl Into<String>,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            field: field.into(),
            code: code.into(),
            message: message.into(),
        }
    }
}

/// What a schedule's policy is NOW, on `policy_changed` (D1 §8.2).
///
/// A PROBLEM EXTENSION, NOT A FIELD ERROR. `expectedGeneration` was not
/// malformed — it named a revision that has been superseded — so the answer a
/// console needs is the current revision, not a sentence about a field. RFC
/// 9457 permits extension members; this is the only one this API defines.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PolicyChangedDetail {
    /// The schedule's current `metadata.generation`.
    pub current_generation: i64,
    /// Its current run-policy digest, when the controller has recorded one.
    /// ABSENT means the controller has not evaluated this revision yet, never
    /// that the policy is empty.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_run_policy_sha256: Option<String>,
}

/// The rendered problem document.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Problem {
    /// `https://logweir.dev/problems/<code>`.
    #[serde(rename = "type")]
    pub type_uri: String,
    /// A fixed title for the code.
    pub title: String,
    /// The HTTP status, repeated.
    pub status: u16,
    /// The stable code.
    pub code: ProblemCode,
    /// A sentence this service wrote. Never a Kubernetes message.
    pub detail: String,
    /// The request ID, also returned as `X-Request-ID`.
    pub request_id: String,
    /// Whether repeating the identical request later can succeed.
    pub retryable: bool,
    /// Field-level failures, when the code is `validation_failed`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<FieldError>,
    /// The schedule's current revision, when the code is `policy_changed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicyChangedDetail>,
}

/// An error a handler returns.
#[derive(Clone, Debug)]
pub struct ApiError {
    /// The stable code.
    pub code: ProblemCode,
    /// A sentence this service wrote.
    pub detail: String,
    /// Field-level failures.
    pub errors: Vec<FieldError>,
    /// `Retry-After` seconds, for `rate_limited`.
    pub retry_after_seconds: Option<u64>,
    /// The schedule's current revision, for `policy_changed`.
    pub policy: Option<PolicyChangedDetail>,
}

impl ApiError {
    /// An error with a detail sentence.
    pub fn new(code: ProblemCode, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
            errors: Vec::new(),
            retry_after_seconds: None,
            policy: None,
        }
    }

    /// `validation_failed` carrying field errors.
    pub fn validation(errors: Vec<FieldError>) -> Self {
        Self {
            code: ProblemCode::ValidationFailed,
            detail: "One or more fields are invalid.".to_string(),
            errors,
            retry_after_seconds: None,
            policy: None,
        }
    }

    /// `destination_invalid` carrying the field errors D2 §3.2 names.
    ///
    /// SEPARATE FROM `validation_failed` ON PURPOSE. A destination's rules are
    /// the CRD's own CEL rules, evaluated here so the operator sees them
    /// before the object is POSTed; the distinct code lets a client tell "you
    /// typed the wrong shape" from "this location and this transport cannot
    /// both be true".
    #[must_use]
    pub fn destination_invalid(errors: Vec<FieldError>) -> Self {
        Self {
            code: ProblemCode::DestinationInvalid,
            detail: "The destination is not valid. Each field below names the rule it breaks."
                .to_string(),
            errors,
            retry_after_seconds: None,
            policy: None,
        }
    }

    /// `policy_changed` naming the revision the schedule is at NOW.
    #[must_use]
    pub fn policy_changed(current_generation: i64, current_digest: Option<String>) -> Self {
        Self {
            code: ProblemCode::PolicyChanged,
            detail: "The schedule's policy changed after the revision this request named. Read \
                     the schedule again; confirming runs the new revision under a new \
                     Idempotency-Key."
                .to_string(),
            errors: Vec::new(),
            retry_after_seconds: None,
            policy: Some(PolicyChangedDetail {
                current_generation,
                current_run_policy_sha256: current_digest,
            }),
        }
    }

    /// `not_found` with the fixed sentence.
    #[must_use]
    pub fn not_found() -> Self {
        Self::new(ProblemCode::NotFound, "No such resource.")
    }

    /// Render the problem document for a request ID.
    #[must_use]
    pub fn to_problem(&self, request_id: &str) -> Problem {
        Problem {
            type_uri: self.code.type_uri(),
            title: self.code.title().to_string(),
            status: self.code.status().as_u16(),
            code: self.code,
            detail: self.detail.clone(),
            request_id: request_id.to_string(),
            retryable: self.code.retryable(),
            errors: self.errors.clone(),
            policy: self.policy.clone(),
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code.as_str(), self.detail)
    }
}

impl std::error::Error for ApiError {}

/// The marker the request-context middleware renders. Public so tests of the
/// middleware can construct one; handlers use `ApiError` directly.
#[derive(Clone, Debug)]
pub struct PendingProblem(pub ApiError);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = self.code.status().into_response();
        response.extensions_mut().insert(PendingProblem(self));
        response
    }
}

/// The media type of every error body.
pub const PROBLEM_CONTENT_TYPE: &str = "application/problem+json";

/// Render a problem document into a complete response body and headers.
///
/// Used by the request-context middleware, which owns the request ID.
#[must_use]
pub fn render(error: &ApiError, request_id: &str) -> Response {
    let problem = error.to_problem(request_id);
    let body = serde_json::to_vec(&problem).unwrap_or_else(|_| {
        // A `Problem` is strings, integers and booleans; serialisation cannot
        // fail. The fallback exists so this function has no panic path.
        br#"{"type":"https://logweir.dev/problems/internal-error","title":"Internal error","status":500,"code":"internal_error","detail":"The error could not be rendered.","requestId":"","retryable":true}"#.to_vec()
    });
    let mut response = (error.code.status(), body).into_response();
    let headers = response.headers_mut();
    headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static(PROBLEM_CONTENT_TYPE),
    );
    if let Some(seconds) = error.retry_after_seconds {
        if let Ok(value) = http::HeaderValue::from_str(&seconds.to_string()) {
            headers.insert(http::header::RETRY_AFTER, value);
        }
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_code_has_a_distinct_wire_spelling_and_a_hyphenated_type() {
        let mut seen = std::collections::BTreeSet::new();
        for code in ProblemCode::ALL {
            assert!(seen.insert(code.as_str()), "duplicate {}", code.as_str());
            let json = serde_json::to_value(code).unwrap();
            assert_eq!(json, serde_json::Value::String(code.as_str().to_string()));
            assert!(!code.type_uri().contains('_'));
        }
        assert_eq!(seen.len(), ProblemCode::ALL.len());
    }
}
