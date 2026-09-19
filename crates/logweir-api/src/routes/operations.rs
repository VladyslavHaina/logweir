//! `GET /api/v1/namespaces/{ns}/operations/{kind}/{name}`: the normalized
//! status of one Backup, Restore, TopicDiscovery or Preflight, and
//! `.../events`: the same thing as a bounded server-sent stream. `kind` is
//! D0's closed set `backup|restore|discovery|preflight`; any other value is
//! 404.
//!
//! TWO SHAPES, BECAUSE THEY ARE TWO THINGS. A backup and a restore answer
//! `OperationViewResponse`, which carries a result, evidence references, a
//! verification verdict and D3 §2.5's stage, progress, diagnosis list, trust
//! basis and completion panel. A transient check answers
//! `CheckOperationResponse`, which carries none of those: a topic list has no
//! signed evidence, and a contract that published `verification: pending` for
//! one would be inviting a console to render a verdict that can never arrive.
//! Each kind's authorization is its own domain's (`operation.read` for the two
//! durable runs, `topicDiscovery.read` / `preflight.read` for the checks), so
//! the check routes here can never be a way around the narrowing an approver
//! gets on the preflight route itself.
//!
//! THE STREAM IS ONLY FOR THE TWO DURABLE KINDS. A transient check finishes in
//! seconds and already has a cancel route; giving it a long-lived connection
//! would add a resource a bounded check does not need.

use axum::extract::State;
use axum::response::Response;
use http::{StatusCode, Uri};
use schemars::JsonSchema;
use serde::Serialize;
use weirkeeper::crds::backup::Backup;
use weirkeeper::crds::restore::Restore;

use super::{authorize, authorize_also, get_object, json, ApiPath};
use crate::app::AppState;
use crate::auth::ratelimit::{StreamSlot, StreamSlots};
use crate::auth::Actor;
use crate::authz::Action;
use crate::http::RequestId;
use crate::problem::{ApiError, ProblemCode};
use crate::status::{self, OperationView, StreamKind};

/// `GET .../operations/{kind}/{name}` for a backup or a restore.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OperationViewResponse {
    /// The request ID.
    pub request_id: String,
    /// The normalized operation: PLAT-17.1's projection plus D3 §2.5's
    /// additions.
    pub item: OperationView,
}

/// The header a reconnecting `EventSource` sends.
pub const LAST_EVENT_ID: &str = "last-event-id";

/// `GET .../operations/{kind}/{name}`.
pub async fn get_one(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, kind, name)): ApiPath<(String, String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    crate::http::parse_query(uri.query(), &[])?;
    match kind.as_str() {
        "discovery" => {
            return super::topic_discoveries::operation(&state, request_id, &actor, &ns, &name)
                .await
        }
        "preflight" => {
            return super::preflights::operation(&state, request_id, &actor, &ns, &name).await
        }
        _ => {}
    }
    authorize(&state, &actor, &ns, Action::ReadOperations)?;
    let now = state.now();
    let item = match kind.as_str() {
        "backup" => status::backup_view(
            &get_object::<Backup>(&state, &actor, &ns, &name).await?,
            now,
        ),
        "restore" => status::restore_view(
            &get_object::<Restore>(&state, &actor, &ns, &name).await?,
            now,
        ),
        _ => return Err(ApiError::not_found()),
    };
    Ok(json(
        StatusCode::OK,
        &OperationViewResponse { request_id, item },
    ))
}

/// The concurrent-stream table this process uses in localAdmin mode.
///
/// WHY A PROCESS GLOBAL FOR ONE MODE. `SharedMode` carries its own
/// [`StreamSlots`] and shared mode uses it; localAdmin mode has no
/// `SharedMode` at all, and a ceiling that disappears in one of the two modes
/// is a ceiling with a hole in it. `Settings` is built by `src/main.rs` and by
/// the test harness, so widening that constructor would change a signature
/// outside this change; the same trade `routes::check_create_rate` states is
/// taken here, for the same reason and with the same consequence — the count
/// is per PROCESS, and one console replica does not know what another is
/// serving.
static LOCAL_STREAM_SLOTS: std::sync::OnceLock<std::sync::Arc<StreamSlots>> =
    std::sync::OnceLock::new();

fn slots(state: &AppState) -> std::sync::Arc<StreamSlots> {
    match state.shared() {
        Some(shared) => std::sync::Arc::clone(&shared.streams),
        None => std::sync::Arc::clone(LOCAL_STREAM_SLOTS.get_or_init(StreamSlots::new)),
    }
}

/// How long a refused subscriber is told to wait. It is the poll interval, not
/// the connection ceiling: a slot is released the moment another stream ends,
/// and telling a browser to wait five minutes for a slot that may free in two
/// seconds would be a worse answer than the truth.
pub const STREAM_RETRY_AFTER_SECONDS: u64 = 5;

/// `GET .../operations/{kind}/{name}/events` — the bounded SSE stream.
///
/// AUTHORIZED ONCE, AT SUBSCRIBE, BEFORE ANY READ. The namespace grant and
/// both actions (`operation.read` and `operation.stream`) are decided before
/// the first Kubernetes call, so an ungranted namespace answers exactly what
/// the read route answers and no stream is ever opened to find out whether an
/// object exists. The audit record is emitted when this handler returns —
/// which is when the decision was made — and the body that follows carries no
/// further authority: it re-reads the one object it was authorized for.
///
/// NO QUERY PARAMETER IS ACCEPTED, AND THAT IS THE POINT. `EventSource` cannot
/// set headers, so the temptation is to let a caller put a token in the URL;
/// a URL is the one place a credential survives in a proxy log, a browser
/// history and a `Referer`. This stream is authenticated by the same session
/// cookie every other route uses, and `?access_token=` is `malformed_request`
/// like any other unknown parameter.
pub async fn events(
    State(state): State<AppState>,
    RequestId(_request_id): RequestId,
    actor: Actor,
    ApiPath((ns, kind, name)): ApiPath<(String, String, String)>,
    uri: Uri,
    headers: http::HeaderMap,
) -> Result<Response, ApiError> {
    crate::http::parse_query(uri.query(), &[])?;
    let kind = match kind.as_str() {
        "backup" => StreamKind::Backup,
        "restore" => StreamKind::Restore,
        // A discovery and a preflight have a read route and no stream; so does
        // any other word. Both are 404 for the same reason: the route table is
        // the boundary.
        _ => return Err(ApiError::not_found()),
    };
    authorize(&state, &actor, &ns, Action::ReadOperations)?;
    authorize_also(&state, &actor, &ns, Action::StreamOperationEvents)?;
    super::check_name(&name)?;

    let last_event_id = match headers.get(LAST_EVENT_ID) {
        None => None,
        Some(value) => {
            let text = value.to_str().unwrap_or_default();
            if !status::valid_event_id(text) {
                return Err(ApiError::new(
                    ProblemCode::MalformedRequest,
                    "Last-Event-ID must be the resourceVersion of an earlier event.",
                ));
            }
            Some(text.to_string())
        }
    };

    let Some(slot) = take_slot(&state, &actor, &ns) else {
        let mut error = ApiError::new(
            ProblemCode::RateLimited,
            "Too many open event streams for this principal in this namespace. Close one, or \
             fall back to polling the read route.",
        );
        error.retry_after_seconds = Some(STREAM_RETRY_AFTER_SECONDS);
        actor.audit.set_failure("rate_limited");
        return Err(error);
    };
    actor.audit.note("stream", "open");
    status::open_stream(state.clone(), kind, ns, name, slot, last_event_id).await
}

fn take_slot(state: &AppState, actor: &Actor, namespace: &str) -> Option<StreamSlot> {
    slots(state).acquire(&actor.id(), namespace)
}

/// Forget every held slot. A test hook: the localAdmin table is a process
/// global, so one test's stream would otherwise be another's ceiling.
#[doc(hidden)]
pub fn stream_slots_for_test(state: &AppState) -> std::sync::Arc<StreamSlots> {
    slots(state)
}
