//! Topic discoveries: bounded, honest topic inventory (PLAT-09.1).
//!
//! # The API reads chunks; it never reads Kafka
//!
//! A discovery is a `TopicDiscovery` object whose controller runs an isolated
//! Job. This service creates the object, reads its status and pages through
//! the immutable `ConfigMap` chunks the controller committed. It opens no
//! broker connection, and there is no code path here that could.
//!
//! # A page is verified before it is served
//!
//! Every chunk read is checked against D2 §5.6: owned by THIS discovery,
//! `immutable: true`, and its annotated digest equal both to the digest the
//! status indexes and to the SHA-256 of the bytes. Any mismatch is
//! `result_integrity_failed` — the page is refused rather than served from
//! bytes whose provenance did not hold.
//!
//! # Nothing here lists ConfigMaps
//!
//! Chunk names come from `status.result.chunks[i].name` of an object the
//! caller already authorized. The adapter has no ConfigMap list verb, so "never
//! list all ConfigMaps" is a property of the code and not a promise about it.
//!
//! # A successful list is not a complete list
//!
//! `visibility` is never better than `unknown` without an explicit
//! administrator-governed attestation. An ACL can hide a topic from `DESCRIBE`
//! with no error anywhere, so "the list succeeded" says nothing about whether
//! it was whole.

use std::collections::BTreeMap;

use axum::body::Body;
use axum::extract::State;
use axum::response::Response;
use chrono::{DateTime, Utc};
use http::{HeaderMap, StatusCode, Uri};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::Resource as _;
use weirkeeper::crds::kafka_cluster::KafkaCluster;
use weirkeeper::crds::topic_discovery::{
    TopicDiscovery as TopicDiscoveryCr, TopicDiscoveryRequest, TopicDiscoverySpec,
    TopicInventoryResult,
};
use weirkeeper::crds::LocalRef;

use super::{
    authorize, cancel_check, check_create_rate, check_name, create_idempotent, get_object, json,
    list_query, ApiPath, DEFAULT_LIMIT, DISCOVERY_CREATES_PER_MINUTE, MAX_LIMIT,
};
use crate::app::AppState;
use crate::auth::Actor;
use crate::authz::Action;
use crate::contract::{
    CancelResponse, CheckErrorView, CheckLifecycle, CheckOperation, CheckOperationKind,
    CheckOperationResponse, CreateTopicDiscoveryRequest, DiscoveryConnectionView,
    DiscoveryCountsView, DiscoveryLatestResponse, ExpectedTopicsView, Page, ScanView,
    TopicDiscovery, TopicDiscoveryList, TopicDiscoveryResponse, TopicEntryView, TopicPageResponse,
    VisibilityState, VisibilityView,
};
use crate::cursor::{self, CursorError, CursorScope};
use crate::http::{read_json, RequestId, MAX_JSON_BODY};
use crate::idempotency::IdempotencyKey;
use crate::kube::{KubeFailure, PageRequest};
use crate::problem::{ApiError, FieldError, ProblemCode};
use crate::status::{condition_view, MAX_CONDITIONS};
use crate::validate;

/// The create route identifier (idempotency scope).
pub const ROUTE_CREATE: &str = "POST /api/v1/namespaces/{ns}/connections/{name}/topic-discoveries";
/// The per-connection list route identifier (cursor scope).
pub const ROUTE_BY_CONNECTION: &str =
    "GET /api/v1/namespaces/{ns}/connections/{name}/topic-discoveries";
/// The topics route identifier (cursor scope).
pub const ROUTE_TOPICS: &str = "GET /api/v1/namespaces/{ns}/topic-discoveries/{id}/topics";
/// The cancel route identifier.
pub const ROUTE_CANCEL: &str = "POST /api/v1/namespaces/{ns}/topic-discoveries/{id}:cancel";
/// The deterministic name prefix (D2 §5.1).
pub const NAME_PREFIX: &str = "td-";
/// The `:cancel` command suffix.
pub const CANCEL: &str = ":cancel";

/// The label that binds a discovery to its connection, so the per-connection
/// list is a label read and never an unbounded scan.
pub const CONNECTION_LABEL: &str = "logweir.dev/connection";
/// The label carrying the request parameter hash, so `reuseFresh` finds an
/// identical earlier discovery without reading every object's spec.
pub const PARAMETERS_LABEL: &str = "logweir.dev/discovery-parameters";

/// At most this many expected topics may be named (D2 §5.1).
pub const MAX_EXPECTED_TOPICS: usize = 500;
/// The lowest `maxTopics`.
pub const MIN_MAX_TOPICS: i32 = 1;
/// The highest `maxTopics` (the contract's hard ceiling).
pub const MAX_MAX_TOPICS: i32 = 50_000;
/// The default `maxTopics`.
pub const DEFAULT_MAX_TOPICS: i32 = 20_000;
/// The lowest in-Job Kafka budget, in seconds.
pub const MIN_TIMEOUT_SECONDS: i32 = 10;
/// The highest in-Job Kafka budget, in seconds.
pub const MAX_TIMEOUT_SECONDS: i32 = 300;
/// The default in-Job Kafka budget, in seconds.
pub const DEFAULT_TIMEOUT_SECONDS: i32 = 60;

/// D2 §5.6's scan budget: at most this many chunks per topics request.
pub const MAX_CHUNKS_PER_REQUEST: u32 = 8;
/// The one data key inside a topic chunk.
pub const CHUNK_KEY: &str = "topics.tsv";
/// The annotation carrying a chunk's own digest.
pub const SHA256_ANNOTATION: &str = "logweir.dev/result-sha256";
/// The most discoveries the per-connection list scans for the two slots.
pub const LATEST_SCAN: u32 = 50;

/// The `basis` entry recorded when a claim of completeness arrives without the
/// attestation that would justify it, and is therefore published as `unknown`.
pub const ATTESTATION_MISSING_BASIS: &str = "attestationMissing";

// ======================================================================
// Projection
// ======================================================================

fn lifecycle_of(object: &TopicDiscoveryCr) -> CheckLifecycle {
    match object
        .status
        .as_ref()
        .and_then(|s| s.phase.as_deref())
        .unwrap_or("Pending")
    {
        "Pending" => CheckLifecycle::Pending,
        "Queued" => CheckLifecycle::Queued,
        "Running" => CheckLifecycle::Running,
        "Succeeded" => CheckLifecycle::Succeeded,
        "Failed" => CheckLifecycle::Failed,
        "Cancelled" => CheckLifecycle::Cancelled,
        // A phase this build does not recognise is never read as success.
        _ => CheckLifecycle::Unknown,
    }
}

/// The visibility the API publishes, which is not always the one the status
/// carries.
///
/// THE API IS THE LAST HONEST BOUNDARY BEFORE THE CONSOLE. D2 §5.4 and
/// D-SEAMS S3 say a claim of completeness needs an explicit,
/// administrator-governed attestation — so `attestedComplete` WITHOUT one is
/// downgraded to `unknown` here, with the downgrade written into `basis` so
/// nobody has to guess why the console stopped saying "all topics". A
/// controller that writes the state and forgets the attestation is a bug this
/// refuses to render rather than a bug the operator finds out about during a
/// restore.
///
/// `limited` is passed through as the controller wrote it. It is a claim about
/// an authorization failure OBSERVED inside the check Job — a fact the API
/// never sees — so there is nothing here to verify it against, and downgrading
/// it would replace the controller's honest "I saw an omission" with a weaker
/// answer that is also less true.
fn visibility_view(result: &TopicInventoryResult) -> VisibilityView {
    let declared = result.visibility.state.as_str();
    let mut basis = result.visibility.basis.clone().unwrap_or_default();
    let attestation = result.visibility.attestation.clone();
    let state = match declared {
        "limited" => VisibilityState::Limited,
        "attestedComplete" if attestation.is_some() => VisibilityState::AttestedComplete,
        "attestedComplete" => {
            basis.push(ATTESTATION_MISSING_BASIS.to_string());
            VisibilityState::Unknown
        }
        // ANYTHING ELSE IS `unknown`. A spelling this build does not recognise
        // is not a claim of completeness.
        _ => VisibilityState::Unknown,
    };
    VisibilityView {
        state,
        basis,
        attestation,
    }
}

/// D2 §5.7's staleness, recomputed per read against the connection as it is
/// NOW.
fn staleness(
    object: &TopicDiscoveryCr,
    connection: Option<&KafkaCluster>,
    now: DateTime<Utc>,
) -> (bool, Vec<String>) {
    let mut reasons = Vec::new();
    let status = object.status.as_ref();
    if let Some(fresh_until) = status.and_then(|s| s.fresh_until.as_ref()) {
        if now >= *fresh_until {
            reasons.push("expired".to_string());
        }
    }
    if let (Some(binding), Some(connection)) = (status.and_then(|s| s.binding.as_ref()), connection)
    {
        let meta = connection.meta();
        if binding.connection_uid.is_some() && binding.connection_uid != meta.uid {
            reasons.push("connectionReplaced".to_string());
        } else if binding.connection_generation.is_some()
            && binding.connection_generation != meta.generation
        {
            reasons.push("connectionChanged".to_string());
        }
        if let Some(principal) = &binding.principal {
            let current = connection
                .spec
                .auth
                .username
                .as_ref()
                .map_or_else(|| "User:ANONYMOUS".to_string(), |u| format!("User:{u}"));
            if principal != &current {
                reasons.push("principalChanged".to_string());
            }
        }
    }
    (!reasons.is_empty(), reasons)
}

/// A `TopicDiscovery` as the product DTO.
#[must_use]
pub fn project(
    object: &TopicDiscoveryCr,
    connection: Option<&KafkaCluster>,
    now: DateTime<Utc>,
) -> TopicDiscovery {
    let meta = object.meta();
    let status = object.status.as_ref();
    let result = status.and_then(|s| s.result.as_ref());
    let binding = status.and_then(|s| s.binding.as_ref());
    let state = lifecycle_of(object);
    let (stale, stale_reasons) = staleness(object, connection, now);
    let failure = matches!(state, CheckLifecycle::Failed | CheckLifecycle::Cancelled);
    TopicDiscovery {
        id: meta.name.clone().unwrap_or_default(),
        namespace: meta.namespace.clone().unwrap_or_default(),
        uid: meta.uid.clone().unwrap_or_default(),
        resource_version: meta.resource_version.clone().unwrap_or_default(),
        created_at: meta.creation_timestamp.as_ref().map(|t| t.0),
        connection: DiscoveryConnectionView {
            name: object.spec.request.connection_ref.name.clone(),
            uid: binding.and_then(|b| b.connection_uid.clone()),
            generation: binding.and_then(|b| b.connection_generation),
            principal: binding.and_then(|b| b.principal.clone()),
            auth_mode: binding.and_then(|b| b.auth_mode.clone()),
        },
        state,
        reason: status.and_then(|s| s.reason.clone()),
        terminal: state.is_terminal(),
        observed_at: status.and_then(|s| s.observed_at.as_ref().copied()),
        fresh_until: status.and_then(|s| s.fresh_until.as_ref().copied()),
        stale,
        stale_reasons,
        cluster_id: result.and_then(|r| r.cluster_id.clone()),
        counts: result.map(|r| DiscoveryCountsView {
            listed: r.counts.listed,
            returned: r.counts.returned,
            internal_excluded: r.counts.internal_excluded,
            errored: r.counts.errored,
        }),
        truncated: result.is_some_and(|r| r.truncated),
        truncation_reason: result.and_then(|r| r.truncation_reason.clone()),
        visibility: result.map(visibility_view),
        expected: result
            .and_then(|r| r.expected.as_ref())
            .map(|e| ExpectedTopicsView {
                requested: e.requested,
                visible: e.visible,
                not_authorized: e.not_authorized,
                not_found: e.not_found,
                unknown: e.unknown,
            }),
        topics_sha256: result.and_then(|r| r.topics_sha256.clone()),
        chunk_count: result
            .and_then(|r| r.chunks.as_ref())
            .map_or(0, std::vec::Vec::len),
        error: failure.then(|| CheckErrorView {
            code: status
                .and_then(|s| s.reason.clone())
                .unwrap_or_else(|| "Unknown".to_string()),
            message: status
                .and_then(|s| s.message.clone())
                .map(|m| validate::bounded(&m, 1024)),
        }),
        conditions: status
            .and_then(|s| s.conditions.as_ref())
            .map(|c| c.iter().take(MAX_CONDITIONS).map(condition_view).collect())
            .unwrap_or_default(),
    }
}

// ======================================================================
// Validation
// ======================================================================

/// Validate a create request.
///
/// # Errors
///
/// `validation_failed` naming every invalid field.
pub fn validate_create(request: &CreateTopicDiscoveryRequest) -> Result<(), ApiError> {
    let mut errors = Vec::new();
    if let Some(expected) = &request.expected_topics {
        if expected.len() > MAX_EXPECTED_TOPICS {
            errors.push(FieldError::new(
                "expectedTopics",
                "count_out_of_range",
                format!("at most {MAX_EXPECTED_TOPICS} names may be named"),
            ));
        }
        for (i, topic) in expected.iter().enumerate() {
            if !validate::is_topic_name(topic) {
                errors.push(FieldError::new(
                    format!("expectedTopics[{i}]"),
                    "invalid_topic",
                    "must be a Kafka topic name; patterns are refused",
                ));
            }
        }
    }
    if let Some(max) = request.max_topics {
        if !(MIN_MAX_TOPICS..=MAX_MAX_TOPICS).contains(&max) {
            errors.push(FieldError::new(
                "maxTopics",
                "out_of_range",
                format!("maxTopics is {MIN_MAX_TOPICS} to {MAX_MAX_TOPICS}"),
            ));
        }
    }
    if let Some(timeout) = request.timeout_seconds {
        if !(MIN_TIMEOUT_SECONDS..=MAX_TIMEOUT_SECONDS).contains(&timeout) {
            errors.push(FieldError::new(
                "timeoutSeconds",
                "out_of_range",
                format!(
                    "the Kafka budget is {MIN_TIMEOUT_SECONDS} to {MAX_TIMEOUT_SECONDS} seconds"
                ),
            ));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ApiError::validation(errors))
    }
}

// ======================================================================
// Building the stored object
// ======================================================================

/// The hash of the request PARAMETERS plus the connection, as a label value.
/// Two requests with the same parameters against the same connection share it,
/// which is what `reuseFresh` matches on.
#[must_use]
pub fn parameters_hash(connection: &str, request: &CreateTopicDiscoveryRequest) -> String {
    let mut expected = request.expected_topics.clone().unwrap_or_default();
    expected.sort();
    expected.dedup();
    let canonical = serde_json::json!({
        "connection": connection,
        "includeInternal": request.include_internal.unwrap_or(false),
        "expectedTopics": expected,
        "maxTopics": request.max_topics.unwrap_or(DEFAULT_MAX_TOPICS),
        "timeoutSeconds": request.timeout_seconds.unwrap_or(DEFAULT_TIMEOUT_SECONDS),
    });
    let digest = logweir_core::ids::sha256_prefixed(canonical.to_string().as_bytes());
    // A label value is at most 63 characters and may not carry a colon.
    digest
        .strip_prefix("sha256:")
        .unwrap_or(&digest)
        .chars()
        .take(40)
        .collect()
}

/// Build the stored object for a validated request.
#[must_use]
pub fn build(
    namespace: &str,
    name: String,
    annotations: BTreeMap<String, String>,
    connection: &str,
    request: &CreateTopicDiscoveryRequest,
) -> TopicDiscoveryCr {
    TopicDiscoveryCr {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: Some(namespace.to_string()),
            annotations: Some(annotations),
            labels: Some(BTreeMap::from([
                (CONNECTION_LABEL.to_string(), connection.to_string()),
                (
                    PARAMETERS_LABEL.to_string(),
                    parameters_hash(connection, request),
                ),
            ])),
            ..ObjectMeta::default()
        },
        spec: TopicDiscoverySpec {
            request: TopicDiscoveryRequest {
                connection_ref: LocalRef {
                    name: connection.to_string(),
                },
                include_internal: request.include_internal.unwrap_or(false),
                expected_topics: request.expected_topics.clone().map(|mut names| {
                    names.sort();
                    names.dedup();
                    names
                }),
                max_topics: request.max_topics.unwrap_or(DEFAULT_MAX_TOPICS),
                timeout_seconds: request.timeout_seconds.unwrap_or(DEFAULT_TIMEOUT_SECONDS),
            },
            cancel_requested: false,
        },
        status: None,
    }
}

// ======================================================================
// Routes
// ======================================================================

async fn connection_or_404(
    state: &AppState,
    actor: &Actor,
    namespace: &str,
    name: &str,
) -> Result<KafkaCluster, ApiError> {
    get_object::<KafkaCluster>(state, actor, namespace, name).await
}

/// `POST .../connections/{name}/topic-discoveries`.
#[allow(clippy::too_many_arguments)]
pub async fn create(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, connection)): ApiPath<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::DiscoverTopics)?;
    check_name(&connection)?;
    crate::http::parse_query(uri.query(), &[])?;
    let key = IdempotencyKey::from_headers(&headers)?;
    let mut request: CreateTopicDiscoveryRequest = read_json(body, MAX_JSON_BODY).await?;
    validate_create(&request)?;
    // THE CONNECTION IS READ FIRST. A discovery against a name that does not
    // exist would otherwise be a durable object nobody can explain, and the
    // controller would have to refuse it after the fact.
    let cluster = connection_or_404(&state, &actor, &ns, &connection).await?;
    check_create_rate(
        &state,
        &actor,
        &ns,
        ROUTE_CREATE,
        DISCOVERY_CREATES_PER_MINUTE,
    )?;
    // Canonical form: every omitted field IS its default, so both spellings
    // hash identically and replay as one request.
    let reuse_fresh = request.reuse_fresh.unwrap_or(true);
    request.include_internal = Some(request.include_internal.unwrap_or(false));
    request.max_topics = Some(request.max_topics.unwrap_or(DEFAULT_MAX_TOPICS));
    request.timeout_seconds = Some(request.timeout_seconds.unwrap_or(DEFAULT_TIMEOUT_SECONDS));
    request.reuse_fresh = Some(reuse_fresh);
    if let Some(mut names) = request.expected_topics.take() {
        names.sort();
        names.dedup();
        request.expected_topics = Some(names);
    }
    if reuse_fresh {
        if let Some(fresh) = fresh_identical(&state, &actor, &ns, &connection, &request).await? {
            actor.audit.note("reused", "true");
            return Ok(json(
                StatusCode::OK,
                &TopicDiscoveryResponse {
                    request_id,
                    replayed: None,
                    reused: Some(true),
                    item: project(&fresh, Some(&cluster), state.now()),
                },
            ));
        }
    }
    let created = create_idempotent(
        &state,
        &actor,
        &ns,
        ROUTE_CREATE,
        NAME_PREFIX,
        &key,
        &request_id,
        &request,
        |name, annotations| build(&ns, name, annotations, &connection, &request),
    )
    .await?;
    Ok(json(
        if created.replayed {
            StatusCode::OK
        } else {
            StatusCode::ACCEPTED
        },
        &TopicDiscoveryResponse {
            request_id,
            replayed: Some(created.replayed),
            reused: Some(false),
            item: project(&created.object, Some(&cluster), state.now()),
        },
    ))
}

/// A fresh, succeeded discovery with the same parameters and connection.
async fn fresh_identical(
    state: &AppState,
    actor: &Actor,
    namespace: &str,
    connection: &str,
    request: &CreateTopicDiscoveryRequest,
) -> Result<Option<TopicDiscoveryCr>, ApiError> {
    let cluster = get_object::<KafkaCluster>(state, actor, namespace, connection).await?;
    let page = PageRequest {
        limit: LATEST_SCAN,
        continue_token: None,
        label_selector: Some(format!(
            "{CONNECTION_LABEL}={connection},{PARAMETERS_LABEL}={}",
            parameters_hash(connection, request)
        )),
    };
    let list = state
        .kube()
        .list::<TopicDiscoveryCr>(namespace, &page)
        .await
        .map_err(KubeFailure::into_api_error)?;
    let now = state.now();
    Ok(list
        .items
        .into_iter()
        .filter(|d| lifecycle_of(d) == CheckLifecycle::Succeeded)
        .filter(|d| !staleness(d, Some(&cluster), now).0)
        .max_by_key(|d| {
            d.status
                .as_ref()
                .and_then(|s| s.observed_at.as_ref().copied())
        }))
}

/// `GET .../topic-discoveries/{id}`.
pub async fn get_one(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, id)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadTopicDiscoveries)?;
    crate::http::parse_query(uri.query(), &[])?;
    let object = get_object::<TopicDiscoveryCr>(&state, &actor, &ns, &id).await?;
    let connection = read_connection(&state, &ns, &object).await;
    Ok(json(
        StatusCode::OK,
        &TopicDiscoveryResponse {
            request_id,
            replayed: None,
            reused: None,
            item: project(&object, connection.as_ref(), state.now()),
        },
    ))
}

/// The connection a discovery names, when it still exists. A deleted
/// connection is not an error: the RESULT is still readable, and `stale` is
/// what says the binding can no longer be compared.
async fn read_connection(
    state: &AppState,
    namespace: &str,
    object: &TopicDiscoveryCr,
) -> Option<KafkaCluster> {
    state
        .kube()
        .get::<KafkaCluster>(namespace, &object.spec.request.connection_ref.name)
        .await
        .ok()
}

/// `GET .../connections/{name}/topic-discoveries[?latest=true]`.
pub async fn by_connection(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, connection)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadTopicDiscoveries)?;
    check_name(&connection)?;
    let params = crate::http::parse_query(uri.query(), &["latest", "limit", "cursor"])?;
    let latest = params.get("latest").is_some_and(|v| v == "true");
    let cluster = connection_or_404(&state, &actor, &ns, &connection).await?;
    let now = state.now();
    if latest {
        let page = PageRequest {
            limit: LATEST_SCAN,
            continue_token: None,
            label_selector: Some(format!("{CONNECTION_LABEL}={connection}")),
        };
        let list = state
            .kube()
            .list::<TopicDiscoveryCr>(&ns, &page)
            .await
            .map_err(KubeFailure::into_api_error)?;
        let newest = |items: &[TopicDiscoveryCr]| -> Option<TopicDiscoveryCr> {
            items
                .iter()
                .max_by_key(|d| d.meta().creation_timestamp.as_ref().map(|t| t.0))
                .cloned()
        };
        let succeeded: Vec<TopicDiscoveryCr> = list
            .items
            .iter()
            .filter(|d| lifecycle_of(d) == CheckLifecycle::Succeeded)
            .cloned()
            .collect();
        return Ok(json(
            StatusCode::OK,
            &DiscoveryLatestResponse {
                request_id,
                latest_attempt: newest(&list.items).map(|d| project(&d, Some(&cluster), now)),
                // A FAILED ATTEMPT NEVER HIDES THE LAST GOOD INVENTORY, and a
                // good inventory never hides that the newest attempt failed.
                last_successful: newest(&succeeded).map(|d| project(&d, Some(&cluster), now)),
            },
        ));
    }
    let mut query = list_query(uri.query().map(strip_latest).as_deref())?;
    query.label_selector = Some(format!("{CONNECTION_LABEL}={connection}"));
    let (items, page) =
        super::list_page::<TopicDiscoveryCr>(&state, &actor, &ns, ROUTE_BY_CONNECTION, &query)
            .await?;
    Ok(json(
        StatusCode::OK,
        &TopicDiscoveryList {
            request_id,
            items: items
                .iter()
                .map(|d| project(d, Some(&cluster), now))
                .collect(),
            page,
        },
    ))
}

/// `latest` is this route's own parameter and never part of the cursor scope.
fn strip_latest(raw: &str) -> String {
    raw.split('&')
        .filter(|p| !p.starts_with("latest="))
        .collect::<Vec<_>>()
        .join("&")
}

// ======================================================================
// The topics page
// ======================================================================

#[derive(Clone, Copy, PartialEq, Eq)]
enum InternalFilter {
    Include,
    Exclude,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ErroredFilter {
    Include,
    Exclude,
    Only,
}

struct TopicFilters {
    q: String,
    prefix: String,
    internal: InternalFilter,
    errored: ErroredFilter,
}

impl TopicFilters {
    /// The canonical filter string a cursor is bound to. Change any filter and
    /// the cursor stops opening: a page cannot straddle two filter sets.
    fn canonical(&self, uid: &str, topics_sha256: &str) -> String {
        format!(
            "uid={uid}&topics={topics_sha256}&q={}&prefix={}&internal={}&errored={}",
            self.q,
            self.prefix,
            match self.internal {
                InternalFilter::Include => "include",
                InternalFilter::Exclude => "exclude",
            },
            match self.errored {
                ErroredFilter::Include => "include",
                ErroredFilter::Exclude => "exclude",
                ErroredFilter::Only => "only",
            }
        )
    }

    fn keeps(&self, entry: &TopicEntryView) -> bool {
        if !self.prefix.is_empty() && !entry.name.starts_with(&self.prefix) {
            return false;
        }
        if !self.q.is_empty() && !entry.name.to_lowercase().contains(&self.q) {
            return false;
        }
        match self.internal {
            InternalFilter::Exclude if entry.internal => return false,
            _ => {}
        }
        match self.errored {
            ErroredFilter::Exclude if entry.error_code.is_some() => false,
            ErroredFilter::Only if entry.error_code.is_none() => false,
            _ => true,
        }
    }
}

fn parse_filters(params: &BTreeMap<String, String>) -> Result<TopicFilters, ApiError> {
    let mut errors = Vec::new();
    let bounded = |value: Option<&String>, field: &str, errors: &mut Vec<FieldError>| -> String {
        let Some(value) = value else {
            return String::new();
        };
        if value.len() > 249 || value.chars().any(char::is_control) {
            errors.push(FieldError::new(
                field,
                "invalid_value",
                "at most 249 bytes on one line",
            ));
            return String::new();
        }
        value.clone()
    };
    let q = bounded(params.get("q"), "q", &mut errors).to_lowercase();
    let prefix = bounded(params.get("prefix"), "prefix", &mut errors);
    let internal = match params.get("internal").map(String::as_str) {
        None | Some("exclude") => InternalFilter::Exclude,
        Some("include") => InternalFilter::Include,
        Some(_) => {
            errors.push(FieldError::new(
                "internal",
                "invalid_value",
                "internal is include or exclude",
            ));
            InternalFilter::Exclude
        }
    };
    let errored = match params.get("errored").map(String::as_str) {
        None | Some("include") => ErroredFilter::Include,
        Some("exclude") => ErroredFilter::Exclude,
        Some("only") => ErroredFilter::Only,
        Some(_) => {
            errors.push(FieldError::new(
                "errored",
                "invalid_value",
                "errored is include, exclude or only",
            ));
            ErroredFilter::Include
        }
    };
    if !errors.is_empty() {
        return Err(ApiError::validation(errors));
    }
    Ok(TopicFilters {
        q,
        prefix,
        internal,
        errored,
    })
}

/// One TSV line as a row. A malformed line is `None`: the stored bytes are
/// verified by digest, so a line that does not parse is a producer bug, and
/// inventing a partial row would hide it.
fn parse_line(line: &str) -> Option<TopicEntryView> {
    let mut fields = line.split('\t');
    let name = fields.next()?;
    let partitions = fields.next()?.parse::<u32>().ok()?;
    let flags = fields.next().unwrap_or("-");
    if name.is_empty() {
        return None;
    }
    let mut internal = false;
    let mut expected = false;
    let mut error_code = None;
    if flags != "-" {
        for flag in flags.split(',') {
            match flag {
                "internal" => internal = true,
                "expected" => expected = true,
                other => {
                    if let Some(code) = other.strip_prefix("error:") {
                        error_code = Some(code.to_string());
                    }
                }
            }
        }
    }
    Some(TopicEntryView {
        name: name.to_string(),
        partitions,
        internal,
        expected,
        error_code,
    })
}

/// `GET .../topic-discoveries/{id}/topics`.
pub async fn topics(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, id)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadTopicDiscoveries)?;
    let params = crate::http::parse_query(
        uri.query(),
        &["limit", "cursor", "q", "prefix", "internal", "errored"],
    )?;
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
    let filters = parse_filters(&params)?;
    let object = get_object::<TopicDiscoveryCr>(&state, &actor, &ns, &id).await?;
    let uid = object.meta().uid.clone().unwrap_or_default();
    let Some(result) = object.status.as_ref().and_then(|s| s.result.as_ref()) else {
        return Err(ApiError::new(
            ProblemCode::NotFound,
            "This discovery has produced no inventory to page through.",
        ));
    };
    let topics_sha256 = result.topics_sha256.clone().unwrap_or_default();
    let chunks = result.chunks.clone().unwrap_or_default();
    let scope = CursorScope {
        actor_id: actor.id(),
        route: ROUTE_TOPICS.to_string(),
        namespace: ns.clone(),
        filters: filters.canonical(&uid, &topics_sha256),
    };
    let (mut chunk_index, mut offset) = match params.get("cursor") {
        None => (0usize, 0usize),
        Some(cursor) => open_position(&state, &scope, cursor)?,
    };
    let mut items = Vec::new();
    let mut chunks_scanned = 0u32;
    let mut complete = true;
    while chunk_index < chunks.len() {
        if chunks_scanned >= MAX_CHUNKS_PER_REQUEST {
            // THE SCAN BUDGET IS A BOUND ON WORK, NOT ON TRUTH. A sparse `q`
            // returns a short page with `complete: false` and a cursor rather
            // than reading the whole result in one request.
            complete = false;
            break;
        }
        let chunk = &chunks[chunk_index];
        // `prefix` can skip a whole chunk without reading it: the status
        // records each chunk's first and last stored name.
        if !filters.prefix.is_empty() {
            let after_last = chunk
                .last_name
                .as_deref()
                .is_some_and(|last| last < filters.prefix.as_str());
            let before_first = chunk.first_name.as_deref().is_some_and(|first| {
                first > filters.prefix.as_str() && !first.starts_with(&filters.prefix)
            });
            if after_last || before_first {
                chunk_index += 1;
                offset = 0;
                continue;
            }
        }
        let document = state
            .kube()
            .get_result_document(&ns, &chunk.name)
            .await
            .map_err(|failure| match failure {
                KubeFailure::NotFound => ApiError::new(
                    ProblemCode::CursorExpired,
                    "The stored inventory was collected with its discovery; run a new one.",
                ),
                other => other.into_api_error(),
            })?;
        super::preflights::verify_document(&document, &uid, Some(&chunk.sha256))?;
        chunks_scanned += 1;
        let body = document.data.get(CHUNK_KEY).cloned().unwrap_or_default();
        let mut consumed = offset;
        let mut filled = false;
        for line in body.lines().skip(offset) {
            consumed += 1;
            let Some(entry) = parse_line(line) else {
                continue;
            };
            if !filters.keeps(&entry) {
                continue;
            }
            items.push(entry);
            if items.len() as u32 >= limit {
                filled = true;
                break;
            }
        }
        if filled {
            offset = consumed;
            break;
        }
        chunk_index += 1;
        offset = 0;
    }
    let exhausted = chunk_index >= chunks.len();
    let next_cursor = if exhausted {
        None
    } else {
        Some(cursor::seal(
            state.cursor_key(),
            &scope,
            &format!("{chunk_index}:{offset}"),
            state.now(),
        ))
    };
    Ok(json(
        StatusCode::OK,
        &TopicPageResponse {
            request_id,
            items,
            page: Page {
                limit,
                next_cursor,
                // THE SNAPSHOT NAMES THE EXACT RESULT. A discovery re-run
                // produces a new object and a new digest, so a client can tell
                // a continued page from a page of something else.
                snapshot: Some(format!("{uid}@{topics_sha256}")),
            },
            scan: ScanView {
                complete: complete && exhausted,
                chunks_scanned,
            },
        },
    ))
}

fn open_position(
    state: &AppState,
    scope: &CursorScope,
    cursor: &str,
) -> Result<(usize, usize), ApiError> {
    let invalid = || {
        ApiError::new(
            ProblemCode::CursorInvalid,
            "The cursor is not valid for this list; restart the list without a cursor.",
        )
    };
    match cursor::open(state.cursor_key(), scope, cursor, state.now()) {
        Ok(token) => {
            let (chunk, offset) = token.split_once(':').ok_or_else(invalid)?;
            Ok((
                chunk.parse::<usize>().map_err(|_| invalid())?,
                offset.parse::<usize>().map_err(|_| invalid())?,
            ))
        }
        Err(CursorError::Invalid) => Err(invalid()),
        Err(CursorError::Expired) => Err(ApiError::new(
            ProblemCode::CursorExpired,
            "The cursor expired; restart the list without a cursor.",
        )),
    }
}

/// `POST .../topic-discoveries/{id}:cancel`.
pub async fn cancel(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, target)): ApiPath<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::CancelTopicDiscovery)?;
    let Some(id) = target.strip_suffix(CANCEL) else {
        return Err(ApiError::new(ProblemCode::NotFound, "No such command."));
    };
    check_name(id)?;
    crate::http::parse_query(uri.query(), &[])?;
    IdempotencyKey::refuse_on(&headers, ROUTE_CANCEL, None)?;
    let (object, already_terminal) = cancel_check::<TopicDiscoveryCr>(
        &state,
        &actor,
        &ns,
        id,
        |o| lifecycle_of(o).is_terminal(),
        |o| o.spec.cancel_requested,
    )
    .await?;
    let projected = project(&object, None, state.now());
    Ok(json(
        StatusCode::OK,
        &CancelResponse {
            request_id,
            id: projected.id,
            state: serde_json::to_value(projected.state)
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_default(),
            already_terminal,
        },
    ))
}

/// `GET .../operations/discovery/{name}`.
pub async fn operation(
    state: &AppState,
    request_id: String,
    actor: &Actor,
    namespace: &str,
    name: &str,
) -> Result<Response, ApiError> {
    authorize(state, actor, namespace, Action::ReadTopicDiscoveries)?;
    let object = get_object::<TopicDiscoveryCr>(state, actor, namespace, name).await?;
    let projected = project(&object, None, state.now());
    Ok(json(
        StatusCode::OK,
        &CheckOperationResponse {
            request_id,
            item: CheckOperation {
                kind: CheckOperationKind::Discovery,
                name: projected.id,
                namespace: projected.namespace,
                uid: projected.uid,
                resource_version: projected.resource_version,
                created_at: projected.created_at,
                state: projected.state,
                state_reason: projected.reason,
                message: object
                    .status
                    .as_ref()
                    .and_then(|s| s.message.clone())
                    .map(|m| validate::bounded(&m, 1024)),
                terminal: projected.terminal,
                cancellable: !projected.terminal && !object.spec.cancel_requested,
                observed_at: projected.observed_at,
                conditions: projected.conditions,
            },
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tsv_line_round_trips_the_flag_vocabulary() {
        let entry = parse_line("orders\t3\t-").unwrap();
        assert_eq!((entry.name.as_str(), entry.partitions), ("orders", 3));
        assert!(!entry.internal && !entry.expected && entry.error_code.is_none());
        let entry = parse_line("__consumer_offsets\t50\tinternal,expected,error:Denied").unwrap();
        assert!(entry.internal && entry.expected);
        assert_eq!(entry.error_code.as_deref(), Some("Denied"));
        for bad in ["", "orders", "orders\tx\t-", "\t3\t-"] {
            assert!(parse_line(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn the_parameter_hash_is_a_legal_label_value_and_ignores_ordering() {
        let a = CreateTopicDiscoveryRequest {
            include_internal: Some(false),
            expected_topics: Some(vec!["b".into(), "a".into()]),
            max_topics: None,
            timeout_seconds: None,
            reuse_fresh: None,
        };
        let b = CreateTopicDiscoveryRequest {
            expected_topics: Some(vec!["a".into(), "b".into()]),
            ..a.clone()
        };
        let hash = parameters_hash("source", &a);
        assert_eq!(hash, parameters_hash("source", &b));
        assert_ne!(hash, parameters_hash("other", &a));
        assert!(hash.len() <= 63 && hash.chars().all(|c| c.is_ascii_alphanumeric()));
    }
}
