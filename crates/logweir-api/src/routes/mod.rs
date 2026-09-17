//! One module per bounded product resource, plus the shared route mechanics:
//! path extraction, name checks, paging and idempotent creation.

pub mod approvals;
pub mod backups;
pub mod connections;
pub mod destinations;
pub mod health;
pub mod namespaces;
pub mod operations;
pub mod preflights;
pub mod restores;
pub mod schedules;
pub mod session;
pub mod topic_discoveries;

use std::collections::BTreeMap;

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
    let authorizer = state.authorizer();
    let outcome = authz::decide(authorizer, actor, namespace, action);
    // THE DECISION IS ATTRIBUTED WHERE IT IS MADE. The audit record keeps the
    // REAL reason even when the response hides it: an ungranted namespace
    // answers 404 in shared mode so a caller cannot enumerate, and this line is
    // where `namespace_forbidden` is still written down.
    let roles: Vec<String> = authorizer
        .roles(actor, namespace)
        .iter()
        .map(|role| role.as_str().to_string())
        .collect();
    actor.audit.set_decision(
        namespace,
        action.name(),
        &roles,
        &authorizer.binding_revision(),
        if outcome.is_ok() {
            crate::audit::Decision::Allow
        } else {
            crate::audit::Decision::Deny
        },
    );
    match outcome {
        Ok(()) => Ok(()),
        Err(denial) => {
            actor.audit.set_failure(denial.audit_code());
            Err(denial.response(authorizer.hides_unbound_namespaces()))
        }
    }
}

/// A SECOND action the same request also needs, without replacing the audit
/// record's primary action.
///
/// WHY NOT JUST CALL [`authorize`] TWICE. The audit record names one product
/// action, and the LAST `authorize` wins; a route that checks
/// `credential.write` after `destination.manage` would file the request under
/// the wrong name and make "who created a destination last week" unanswerable.
/// This decides the extra action with the same table, records it as a note,
/// and leaves the route's own action in place.
///
/// # Errors
///
/// `namespace_forbidden` or `forbidden`, with the denial recorded.
pub fn authorize_also(
    state: &AppState,
    actor: &Actor,
    namespace: &str,
    action: Action,
) -> Result<(), ApiError> {
    let authorizer = state.authorizer();
    actor.audit.note("alsoRequired", action.name());
    match authz::decide(authorizer, actor, namespace, action) {
        Ok(()) => Ok(()),
        Err(denial) => {
            actor.audit.set_decision(
                namespace,
                action.name(),
                &authorizer
                    .roles(actor, namespace)
                    .iter()
                    .map(|role| role.as_str().to_string())
                    .collect::<Vec<String>>(),
                &authorizer.binding_revision(),
                crate::audit::Decision::Deny,
            );
            actor.audit.set_failure(denial.audit_code());
            Err(denial.response(authorizer.hides_unbound_namespaces()))
        }
    }
}

/// Record the object a route touched, for the audit line.
fn note_object<K: ProductResource>(actor: &Actor, object: &K) {
    let meta = object.meta();
    actor.audit.set_object(
        &K::kind(&()),
        meta.name.as_deref().unwrap_or_default(),
        meta.uid.as_deref().unwrap_or_default(),
        meta.resource_version.as_deref().unwrap_or_default(),
    );
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
    create_idempotent_inner(
        state, actor, namespace, route, prefix, None, key, request_id, validated, build,
    )
    .await
}

/// Whether an object under `name` is already THIS request's own — a replay —
/// decided only by this service's idempotency record.
///
/// NEVER BY THE EXISTENCE OF ANYTHING ELSE. A route that needs to know
/// "did I already run?" before it writes cannot infer it from a side object
/// being present: a Secret sitting under a deterministic name is evidence that
/// SOMETHING wrote it, not that this request did. The annotations
/// [`create_named_idempotent`] records — the scope hash and the canonical
/// request hash, both taken over the client's own `Idempotency-Key` — are the
/// only thing that identifies a request as its own earlier attempt.
///
/// An absent object is not a replay. Any other Kubernetes failure is
/// propagated rather than read as one, so a transient error never turns into
/// "assume it was mine".
///
/// # Errors
///
/// The adapter's failure, or `internal_error` if the request cannot be hashed.
pub async fn is_own_replay<K, Req>(
    state: &AppState,
    actor: &Actor,
    namespace: &str,
    route: &str,
    name: &str,
    key: &IdempotencyKey,
    validated: &Req,
) -> Result<bool, ApiError>
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
    let mut identity = idempotency::identity(actor, namespace, route, "", key, &canonical);
    identity.name = name.to_string();
    match state.kube().get::<K>(namespace, name).await {
        Ok(existing) => Ok(matches!(
            idempotency::compare(Some(existing.annotations()), &identity),
            ReplayVerdict::Replay
        )),
        Err(KubeFailure::NotFound) => Ok(false),
        Err(other) => Err(other.into_api_error()),
    }
}

/// Create an object under a name the CALLER chose, with the same replay rules.
///
/// FOR THE KINDS WHOSE NAME IS PART OF THE CONTRACT. A `BackupDestination` is
/// referenced by name by every schedule, backup and restore that uses it, and
/// an operator picks that name; a hashed name would make the reference
/// unreadable and the object un-namable from `kubectl`. The idempotency scope
/// is unchanged — it still binds issuer, subject, namespace, route and key —
/// so a replay still returns the same object and a different request under the
/// same key is still `idempotency_conflict`. What changes is the failure mode
/// of a name COLLISION between two actors: the second gets `state_conflict`,
/// because an object this scope did not create is never adopted.
#[allow(clippy::too_many_arguments)]
pub async fn create_named_idempotent<K, Req>(
    state: &AppState,
    actor: &Actor,
    namespace: &str,
    route: &str,
    name: &str,
    key: &IdempotencyKey,
    request_id: &str,
    validated: &Req,
    build: impl Fn(String, std::collections::BTreeMap<String, String>) -> K,
) -> Result<Created<K>, ApiError>
where
    K: ProductResource,
    Req: Serialize,
{
    create_idempotent_inner(
        state,
        actor,
        namespace,
        route,
        "",
        Some(name),
        key,
        request_id,
        validated,
        build,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn create_idempotent_inner<K, Req>(
    state: &AppState,
    actor: &Actor,
    namespace: &str,
    route: &str,
    prefix: &str,
    fixed_name: Option<&str>,
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
    let mut identity = idempotency::identity(actor, namespace, route, prefix, key, &canonical);
    if let Some(name) = fixed_name {
        identity.name = name.to_string();
    }
    actor
        .audit
        .set_create_hashes(&identity.scope_hash, &identity.request_hash);
    // THE PLAN HASH, NEVER THE PLAN BYTES. D0 asks the audit record to carry
    // the canonical plan hash; it is read from the validated request rather
    // than passed in by each route, so a future route that submits a plan
    // cannot forget to attribute it.
    if let Some(plan_hash) = serde_json::from_slice::<serde_json::Value>(&canonical)
        .ok()
        .and_then(|value| {
            value
                .get("planHash")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
    {
        actor.audit.set_plan_hash(&plan_hash);
    }
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
                note_object(actor, &created);
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
            ReplayVerdict::Replay => {
                note_object(actor, &existing);
                actor.audit.note("replayed", "true");
                Ok(Created {
                    object: existing,
                    replayed: true,
                })
            }
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
    actor: &Actor,
    namespace: &str,
    name: &str,
) -> Result<K, ApiError> {
    check_name(name)?;
    let object = state
        .kube()
        .get::<K>(namespace, name)
        .await
        .map_err(KubeFailure::into_api_error)?;
    // The read is attributed to the exact object, with its UID and
    // resourceVersion, so an audit reader can tell WHICH object was seen and
    // not merely which name was asked for.
    note_object(actor, &object);
    Ok(object)
}

// ======================================================================
// Transient checks: rate limit, cancellation, object-level authorization
// ======================================================================

/// D2 §8.2: at most this many discovery creates per actor, per namespace, per
/// minute.
pub const DISCOVERY_CREATES_PER_MINUTE: u32 = 6;
/// D2 §8.3: at most this many preflight creates per actor, per namespace, per
/// minute.
pub const PREFLIGHT_CREATES_PER_MINUTE: u32 = 20;
/// The window both limits use.
pub const CHECK_RATE_WINDOW_SECONDS: i64 = 60;

struct CheckWindow {
    started: chrono::DateTime<chrono::Utc>,
    count: u32,
}

/// The check-create windows, keyed by `(actor, namespace, route)`.
///
/// WHY A PROCESS GLOBAL AND NOT A FIELD. `AppState`'s `Settings` is built by
/// `main.rs` and by the test harness, both outside this task's ownership, and
/// widening that constructor to carry a limiter would change a signature two
/// other stages depend on. The trade is stated rather than hidden: this map is
/// per PROCESS, so two console replicas each permit the configured rate, and
/// the limit is a politeness bound on how fast one operator can queue check
/// Jobs — not a security control. The real ceiling on concurrent checks is the
/// controller's `checks.maxActivePerNamespace`, which no API can talk past.
/// The clock is [`AppState::now`], so a test advances it instead of sleeping.
static CHECK_WINDOWS: std::sync::OnceLock<std::sync::Mutex<BTreeMap<String, CheckWindow>>> =
    std::sync::OnceLock::new();

/// The most keys tracked at once. Past this the map is cleared rather than
/// grown without bound: a flood of actor/namespace pairs must not become a
/// memory leak, and losing a window only forgives requests.
pub const MAX_TRACKED_CHECK_KEYS: usize = 4096;

/// Count one check create and decide.
///
/// # Errors
///
/// `rate_limited` with `Retry-After` when the window is full.
pub fn check_create_rate(
    state: &AppState,
    actor: &Actor,
    namespace: &str,
    route: &str,
    per_window: u32,
) -> Result<(), ApiError> {
    let now = state.now();
    let key = format!("{}\n{namespace}\n{route}", actor.id());
    let mut windows = CHECK_WINDOWS
        .get_or_init(|| std::sync::Mutex::new(BTreeMap::new()))
        .lock()
        .expect("the check-window lock is never poisoned");
    if windows.len() > MAX_TRACKED_CHECK_KEYS {
        windows.clear();
    }
    let window = windows.entry(key).or_insert(CheckWindow {
        started: now,
        count: 0,
    });
    let elapsed = (now - window.started).num_seconds();
    if !(0..CHECK_RATE_WINDOW_SECONDS).contains(&elapsed) {
        window.started = now;
        window.count = 0;
    }
    if window.count >= per_window {
        let remaining = CHECK_RATE_WINDOW_SECONDS - (now - window.started).num_seconds();
        let mut error = ApiError::new(
            ProblemCode::RateLimited,
            "Too many checks were started in this namespace; wait for the window to reset.",
        );
        error.retry_after_seconds = Some(remaining.clamp(1, CHECK_RATE_WINDOW_SECONDS) as u64);
        return Err(error);
    }
    window.count += 1;
    Ok(())
}

/// Forget every counted window. A test hook: the map is a process global, so
/// one test's creates would otherwise be another's rate limit.
#[doc(hidden)]
pub fn reset_check_rate_limits() {
    if let Some(lock) = CHECK_WINDOWS.get() {
        lock.lock()
            .expect("the check-window lock is never poisoned")
            .clear();
    }
}

/// Whether `actor` created the object, by the annotation the create recorded.
///
/// EXACT ACTOR, NOT A ROLE. D0 gives an operator "start/read/cancel OWN
/// checks": two operators in one namespace are both operators, and the one who
/// did not start a check may not stop it. The comparison is the stable
/// `issuer#subject` id, never a display name.
#[must_use]
pub fn created_by<K: ProductResource>(object: &K, actor: &Actor) -> bool {
    object
        .meta()
        .annotations
        .as_ref()
        .and_then(|a| a.get(idempotency::ANNOTATION_ACTOR))
        .is_some_and(|recorded| recorded == &actor.id())
}

/// Whether `actor` administers `namespace`.
///
/// THE ONE EXCEPTION TO OWNERSHIP, AND ONLY FOR CANCEL. D0's "cancel own
/// checks" is written in the OPERATOR row; an administrator who cannot stop a
/// twenty-thousand-topic discovery an operator started before going home has
/// to wait out `timeoutSeconds` or reach for `kubectl`, which is the outcome
/// the console exists to avoid. It stays narrow: cancelling deletes no archive
/// byte, no Kafka topic, no durable run and no signed evidence, the audit line
/// records who actually did it, and nothing else in this crate consults the
/// role for an ownership decision.
#[must_use]
pub fn administers(state: &AppState, actor: &Actor, namespace: &str) -> bool {
    state
        .authorizer()
        .roles(actor, namespace)
        .contains(&crate::authz::Role::Administrator)
}

/// Ask one transient check to stop, after the caller has authorized it.
///
/// IDEMPOTENT IN BOTH DIRECTIONS. A check that has already finished is
/// answered 200 `alreadyTerminal` with nothing written; a check whose
/// `cancelRequested` is already true is answered 200 with nothing written; and
/// a concurrent change between the read and the patch is retried once before
/// it becomes a conflict.
///
/// # Errors
///
/// `forbidden` when the actor did not start the check, `state_conflict` when
/// the object changed twice under the request, or the adapter's failure.
pub async fn cancel_check<K>(
    state: &AppState,
    actor: &Actor,
    namespace: &str,
    name: &str,
    terminal: impl Fn(&K) -> bool,
    already_requested: impl Fn(&K) -> bool,
) -> Result<(K, bool), ApiError>
where
    K: crate::kube::CancellableCheck,
{
    for _ in 0..2 {
        let object = get_object::<K>(state, actor, namespace, name).await?;
        if !created_by(&object, actor) {
            if administers(state, actor, namespace) {
                // WHO ACTUALLY DID IT IS RECORDED. An administrator stopping
                // someone else's check is a different event from an operator
                // stopping its own, and the audit reader should not have to
                // infer which one happened.
                actor.audit.note("cancelledAnotherActorsCheck", "true");
            } else {
                // The AUDIT line already names the object and the actor; the
                // response says what the rule is without saying who owns it.
                actor.audit.set_failure("forbidden");
                return Err(ApiError::new(
                    ProblemCode::Forbidden,
                    "This check was started by another actor. An operator may cancel only the \
                     checks it started; an administrator of this namespace may cancel any.",
                ));
            }
        }
        if terminal(&object) {
            return Ok((object, true));
        }
        if already_requested(&object) {
            return Ok((object, false));
        }
        let version = object.meta().resource_version.clone().unwrap_or_default();
        match state
            .kube()
            .request_check_cancel::<K>(namespace, name, &version)
            .await
        {
            Ok(updated) => {
                tracing::info!(
                    namespace,
                    name,
                    actor = %actor.id(),
                    "cancel requested"
                );
                note_object(actor, &updated);
                actor.audit.note("cancelRequested", "true");
                return Ok((updated, false));
            }
            Err(KubeFailure::Conflict) => continue,
            Err(other) => return Err(other.into_api_error()),
        }
    }
    Err(ApiError::new(
        ProblemCode::StateConflict,
        "The check changed concurrently while the cancel was applied; read it again and retry.",
    ))
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
