//! One module per bounded product resource, plus the shared route mechanics:
//! path extraction, name checks, paging and idempotent creation.

pub mod approvals;
pub mod backups;
pub mod connections;
pub mod health;
pub mod namespaces;
pub mod operations;
pub mod restores;
pub mod schedules;
pub mod session;

use axum::extract::FromRequestParts;
use axum::response::{IntoResponse, Response};
use http::request::Parts;
use http::StatusCode;
use kube::ResourceExt;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::app::AppState;
use crate::auth::Actor;
use crate::authz::{self, Action};
use crate::contract::Page;
use crate::cursor::{self, CursorError, CursorScope};
use crate::idempotency::{self, IdempotencyKey, ReplayVerdict};
use crate::kube::{KubeFailure, PageRequest, ProductResource};
use crate::problem::{ApiError, FieldError, ProblemCode};
use crate::validate;

/// The default page size.
pub const DEFAULT_LIMIT: u32 = 50;
/// The largest page size.
pub const MAX_LIMIT: u32 = 200;

/// Path parameters whose decode failure is a plain `not_found`.
pub struct ApiPath<T>(pub T);

impl<S, T> FromRequestParts<S> for ApiPath<T>
where
    T: DeserializeOwned + Send,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        axum::extract::Path::<T>::from_request_parts(parts, state)
            .await
            .map(|p| ApiPath(p.0))
            .map_err(|_| ApiError::not_found())
    }
}

/// Serialize a DTO as `application/json` with a status.
pub fn json<T: Serialize>(status: StatusCode, body: &T) -> Response {
    match serde_json::to_vec(body) {
        Ok(bytes) => {
            let mut response = (status, bytes).into_response();
            response.headers_mut().insert(
                http::header::CONTENT_TYPE,
                http::HeaderValue::from_static("application/json"),
            );
            response
        }
        Err(_) => ApiError::new(
            ProblemCode::InternalError,
            "The response could not be rendered.",
        )
        .into_response(),
    }
}

/// Authorize, namespace first. A malformed namespace can never be granted, so
/// it is `namespace_forbidden` by the same rule.
///
/// # Errors
///
/// `namespace_forbidden` or `forbidden`.
pub fn authorize(
    state: &AppState,
    actor: &Actor,
    namespace: &str,
    action: Action,
) -> Result<(), ApiError> {
    authz::authorize(state.authorizer(), actor, namespace, action)
}

/// A resource name that cannot exist is `not_found`, without a Kubernetes
/// call.
///
/// # Errors
///
/// `not_found`.
pub fn check_name(name: &str) -> Result<(), ApiError> {
    if validate::is_dns_subdomain(name) {
        Ok(())
    } else {
        Err(ApiError::not_found())
    }
}

/// A validated list query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListQuery {
    /// The page size.
    pub limit: u32,
    /// The opaque cursor.
    pub cursor: Option<String>,
    /// An equality label selector.
    pub label_selector: Option<String>,
}

/// Parse `limit`, `cursor` and `labelSelector`.
///
/// # Errors
///
/// `malformed_request` for unknown or repeated parameters, `validation_failed`
/// for an out-of-range limit or an unsupported selector.
pub fn list_query(raw: Option<&str>) -> Result<ListQuery, ApiError> {
    let params = crate::http::parse_query(raw, &["limit", "cursor", "labelSelector"])?;
    let limit = match params.get("limit") {
        None => DEFAULT_LIMIT,
        Some(text) => match text.parse::<u32>() {
            Ok(n) if (1..=MAX_LIMIT).contains(&n) => n,
            _ => {
                return Err(ApiError::validation(vec![FieldError::new(
                    "limit",
                    "out_of_range",
                    format!("limit must be an integer from 1 to {MAX_LIMIT}"),
                )]))
            }
        },
    };
    let label_selector = match params.get("labelSelector") {
        None => None,
        Some(selector) if selector.is_empty() => None,
        Some(selector) => {
            if !is_equality_selector(selector) {
                return Err(ApiError::validation(vec![FieldError::new(
                    "labelSelector",
                    "unsupported_selector",
                    "only comma-separated key=value equality terms are supported",
                )]));
            }
            Some(selector.clone())
        }
    };
    Ok(ListQuery {
        limit,
        cursor: params.get("cursor").cloned(),
        label_selector,
    })
}

fn is_label_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        && bytes[0].is_ascii_alphanumeric()
        && bytes[bytes.len() - 1].is_ascii_alphanumeric()
}

/// `key=value[,key=value]`, Kubernetes label syntax, at most 8 terms.
#[must_use]
pub fn is_equality_selector(selector: &str) -> bool {
    let terms: Vec<&str> = selector.split(',').collect();
    terms.len() <= 8
        && terms.iter().all(|term| {
            let Some((key, value)) = term.split_once('=') else {
                return false;
            };
            if value.contains('=') || (!value.is_empty() && !is_label_name(value)) {
                return false;
            }
            match key.split_once('/') {
                Some((prefix, name)) => validate::is_dns_subdomain(prefix) && is_label_name(name),
                None => is_label_name(key),
            }
        })
}

/// One authorized page of a native list, with cursor sealing.
///
/// # Errors
///
/// `cursor_invalid`, `cursor_expired` (the cursor's own expiry or a
/// Kubernetes 410), or the adapter's failure.
pub async fn list_page<K: ProductResource>(
    state: &AppState,
    actor: &Actor,
    namespace: &str,
    route: &str,
    query: &ListQuery,
) -> Result<(Vec<K>, Page), ApiError> {
    let scope = CursorScope {
        actor_id: actor.id(),
        route: route.to_string(),
        namespace: namespace.to_string(),
        filters: query
            .label_selector
            .as_ref()
            .map(|s| format!("labelSelector={s}"))
            .unwrap_or_default(),
    };
    let continue_token = match &query.cursor {
        None => None,
        Some(cursor) => match cursor::open(state.cursor_key(), &scope, cursor, state.now()) {
            Ok(token) => Some(token),
            Err(CursorError::Invalid) => {
                return Err(ApiError::new(
                    ProblemCode::CursorInvalid,
                    "The cursor is not valid for this list; restart the list without a cursor.",
                ))
            }
            Err(CursorError::Expired) => {
                return Err(ApiError::new(
                    ProblemCode::CursorExpired,
                    "The cursor expired; restart the list without a cursor.",
                ))
            }
        },
    };
    let page = PageRequest {
        limit: query.limit,
        continue_token,
        label_selector: query.label_selector.clone(),
    };
    let list = state
        .kube()
        .list::<K>(namespace, &page)
        .await
        .map_err(KubeFailure::into_api_error)?;
    let next_cursor = list
        .metadata
        .continue_
        .as_deref()
        .filter(|token| !token.is_empty())
        .map(|token| cursor::seal(state.cursor_key(), &scope, token, state.now()));
    Ok((
        list.items,
        Page {
            limit: query.limit,
            next_cursor,
            snapshot: list.metadata.resource_version.clone(),
        },
    ))
}

/// The outcome of an idempotent create.
pub struct Created<K> {
    /// The stored object.
    pub object: K,
    /// Whether this replayed an earlier identical request.
    pub replayed: bool,
}

impl<K> Created<K> {
    /// 201 for a create, 200 for a replay.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        if self.replayed {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        }
    }
}

/// Create an object under a deterministic name, or replay/refuse per
/// `crate::idempotency`.
///
/// `build` receives the deterministic name and the annotations and returns
/// the object to create.
///
/// # Errors
///
/// `idempotency_conflict`, `state_conflict`, or the adapter's failure.
#[allow(clippy::too_many_arguments)]
pub async fn create_idempotent<K, Req>(
    state: &AppState,
    actor: &Actor,
    namespace: &str,
    route: &str,
    prefix: &str,
    key: &IdempotencyKey,
    request_id: &str,
    validated: &Req,
    build: impl Fn(String, std::collections::BTreeMap<String, String>) -> K,
) -> Result<Created<K>, ApiError>
where
    K: ProductResource,
    Req: Serialize,
{
    let canonical = serde_json::to_vec(validated).map_err(|_| {
        ApiError::new(
            ProblemCode::InternalError,
            "The request could not be hashed.",
        )
    })?;
    let identity = idempotency::identity(actor, namespace, route, prefix, key, &canonical);
    let object = build(
        identity.name.clone(),
        idempotency::annotations(&identity, request_id, actor),
    );

    // Two attempts: the second covers an object deleted between our
    // AlreadyExists and our read of it. A third outcome is a conflict.
    for _ in 0..2 {
        match state.kube().create::<K>(namespace, &object).await {
            Ok(created) => {
                tracing::info!(
                    namespace,
                    route,
                    name = %identity.name,
                    uid = %created.uid().unwrap_or_default(),
                    request_sha256 = %identity.request_hash,
                    idempotency_scope_sha256 = %identity.scope_hash,
                    actor = %actor.id(),
                    "created"
                );
                return Ok(Created {
                    object: created,
                    replayed: false,
                });
            }
            Err(KubeFailure::AlreadyExists) => {}
            Err(other) => return Err(other.into_api_error()),
        }
        let existing = match state.kube().get::<K>(namespace, &identity.name).await {
            Ok(existing) => existing,
            Err(KubeFailure::NotFound) => continue,
            Err(other) => return Err(other.into_api_error()),
        };
        return match idempotency::compare(Some(existing.annotations()), &identity) {
            ReplayVerdict::Replay => Ok(Created {
                object: existing,
                replayed: true,
            }),
            ReplayVerdict::DifferentRequest => Err(ApiError::new(
                ProblemCode::IdempotencyConflict,
                "This Idempotency-Key was already used with a different request. Use a new key \
                 for a new request.",
            )),
            ReplayVerdict::Foreign => Err(ApiError::new(
                ProblemCode::StateConflict,
                "An object with the deterministic name exists and was not created by this \
                 request scope; it is not adopted.",
            )),
        };
    }
    Err(ApiError::new(
        ProblemCode::StateConflict,
        "The object changed concurrently while the create was replayed; retry the request.",
    ))
}

/// Read an object after its name check.
///
/// # Errors
///
/// `not_found` or the adapter's failure.
pub async fn get_object<K: ProductResource>(
    state: &AppState,
    namespace: &str,
    name: &str,
) -> Result<K, ApiError> {
    check_name(name)?;
    state
        .kube()
        .get::<K>(namespace, name)
        .await
        .map_err(KubeFailure::into_api_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selectors() {
        assert!(is_equality_selector("logweir.dev/schedule=nightly"));
        assert!(is_equality_selector("a=b,c="));
        for bad in ["a!=b", "a in (b)", "a", "a=b=c", "=b", "a=b c", "!a"] {
            assert!(!is_equality_selector(bad), "{bad}");
        }
    }

    #[test]
    fn limits() {
        assert_eq!(list_query(None).unwrap().limit, DEFAULT_LIMIT);
        assert_eq!(list_query(Some("limit=200")).unwrap().limit, 200);
        for bad in ["limit=0", "limit=201", "limit=-1", "limit=x"] {
            assert_eq!(
                list_query(Some(bad)).unwrap_err().code,
                ProblemCode::ValidationFailed,
                "{bad}"
            );
        }
    }
}
