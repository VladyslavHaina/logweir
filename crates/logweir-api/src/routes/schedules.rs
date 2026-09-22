//! Schedules: `BackupSchedule` projections, a typed create, and the one
//! permitted mutation — `:set-suspension` under a resourceVersion
//! precondition.

use std::collections::BTreeMap;

use axum::body::Body;
use axum::extract::State;
use axum::response::Response;
use http::{HeaderMap, StatusCode, Uri};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use weirkeeper::cadence::{self, Cadence, CadenceError};
use weirkeeper::crds::backup_destination::DESTINATION_URL_SCHEME;
use weirkeeper::crds::backup_schedule::{
    BackupSchedule, BackupScheduleSpec, CatchUpPolicy as CrdCatchUp,
    ConcurrencyPolicy as CrdConcurrency, Retention, RetrySpec,
};
use weirkeeper::crds::selection::{
    AllUserTopics as CrdAllUserTopics, IncompleteDiscovery as CrdIncompleteDiscovery,
    TopicExclusions as CrdTopicExclusions,
};
use weirkeeper::crds::{ArchiveRef, LocalRef};

use super::{
    authorize, check_name, create_idempotent, get_object, json, list_page, list_query, ApiPath,
};
use crate::app::AppState;
use crate::auth::Actor;
use crate::authz::Action;
use crate::contract::{
    ArchiveRequest, CatchUpPolicy, ConcurrencyPolicy, CreateScheduleRequest,
    IncompleteDiscoveryPolicy, NameRef, RetentionRequest, RetryPolicy, ScheduleList,
    ScheduleResponse, SetSuspensionRequest, TopicSelectionRequest, UpdateSchedulePolicyRequest,
};
use crate::http::{read_json, RequestId, MAX_JSON_BODY};
use crate::idempotency::IdempotencyKey;
use crate::kube::{KubeFailure, ScheduleSpecEdit};
use crate::problem::{ApiError, FieldError, ProblemCode};
use crate::projection;
use crate::validate;

/// The list route identifier.
pub const ROUTE_LIST: &str = "GET /api/v1/namespaces/{ns}/schedules";
/// The create route identifier.
pub const ROUTE_CREATE: &str = "POST /api/v1/namespaces/{ns}/schedules";
/// The command route identifier.
pub const ROUTE_SET_SUSPENSION: &str =
    "POST /api/v1/namespaces/{ns}/schedules/{name}:set-suspension";
/// The command suffix.
pub const SET_SUSPENSION: &str = ":set-suspension";
/// The field-error code for an expression the cadence engine cannot read.
///
/// D1 §0.2 fixes this spelling, and `create`, `update` and
/// `routes::cadence_previews` all use THIS constant so that one condition
/// cannot acquire two codes again.
pub const SCHEDULE_INVALID: &str = "schedule_invalid";

/// The field-error code for a zone the compiled-in database does not have.
pub const TIMEZONE_UNKNOWN: &str = "timezone_unknown";

/// The deterministic name prefix. `sch-` plus 26 characters is 30, inside
/// the 32-character schedule-name budget the scheduled Backup name leaves
/// (`logweir-backup-<schedule>-<yyyymmdd-hhmmss>` within 63).
pub const NAME_PREFIX: &str = "sch-";

/// `GET .../schedules`.
pub async fn list(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(ns): ApiPath<String>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadSchedules)?;
    let query = list_query(uri.query())?;
    let (items, page) =
        list_page::<BackupSchedule>(&state, &actor, &ns, ROUTE_LIST, &query).await?;
    Ok(json(
        StatusCode::OK,
        &ScheduleList {
            request_id,
            items: items.iter().map(projection::schedule).collect(),
            page,
        },
    ))
}

/// `GET .../schedules/{name}`.
pub async fn get_one(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, name)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadSchedules)?;
    crate::http::parse_query(uri.query(), &[])?;
    let object = get_object::<BackupSchedule>(&state, &actor, &ns, &name).await?;
    Ok(json(
        StatusCode::OK,
        &ScheduleResponse {
            request_id,
            replayed: None,
            item: projection::schedule(&object),
        },
    ))
}

pub(crate) fn validate_archive(
    field: &str,
    archive: &ArchiveRequest,
    errors: &mut Vec<FieldError>,
) {
    if let Err(code) = validate::check_archive_url(&archive.url) {
        errors.push(FieldError::new(
            format!("{field}.url"),
            code,
            "must be an s3://, gs://, az:// or file:/// URL without credentials",
        ));
    }
    if let Some(r) = &archive.credential_ref {
        if !validate::is_dns_subdomain(&r.name) {
            errors.push(FieldError::new(
                format!("{field}.credentialRef.name"),
                "invalid_name",
                "must be a Kubernetes object name",
            ));
        }
    }
}

/// Validate a create request with the controller's own cron parser.
///
/// # Errors
///
/// `validation_failed` naming every invalid field.
pub fn validate_create(request: &CreateScheduleRequest) -> Result<(), ApiError> {
    let mut errors = Vec::new();
    // THE ZONE IS PART OF THE SAME PARSE (PLAT-10.1): the create DTO carries
    // `timeZone`, so this route resolves the expression in the zone it will be
    // stored with -- an unknown zone is `timeZone: timezone_unknown` here
    // rather than `Ready=False`/`UnknownTimeZone` on an object the console
    // said was fine. `schedules::the_three_cadence_routes_answer_one_code`
    // pins the one `schedule_invalid` code across the three cadence routes.
    validate_cadence_fields(&request.schedule, request.time_zone.as_deref(), &mut errors);
    if !validate::is_dns_subdomain(&request.source_ref.name) {
        errors.push(FieldError::new(
            "sourceRef.name",
            "invalid_name",
            "must be a Kubernetes object name",
        ));
    }
    validate_policy_fields(
        &PolicyFields {
            selection_prefix: "",
            selection: &create_selection(request),
            archive: request.archive.as_ref(),
            destination_ref: request.destination_ref.as_ref(),
            starting_deadline_seconds: request.starting_deadline_seconds,
            active_deadline_seconds: request.active_deadline_seconds,
            retry: request.retry.as_ref(),
            retention: request.retention.as_ref(),
        },
        &mut errors,
    );
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ApiError::validation(errors))
    }
}

/// The create route's two selection fields, read as the one selection both
/// routes validate and build from.
fn create_selection(request: &CreateScheduleRequest) -> TopicSelectionRequest {
    TopicSelectionRequest {
        topics: request.topics.clone(),
        all_user_topics: request.all_user_topics.clone(),
    }
}

/// Build the stored object.
///
/// # Panics
///
/// Never: `validate_create` runs first and is what refuses a request with
/// neither or both of `archive` and `destinationRef`. The debug assertion
/// below is there so that a caller who skips it fails loudly in tests rather
/// than writing an empty `archive.url`.
#[must_use]
pub fn build(
    namespace: &str,
    name: String,
    annotations: BTreeMap<String, String>,
    request: &CreateScheduleRequest,
) -> BackupSchedule {
    let mut errors = Vec::new();
    // THE SENTINEL IS BUILT, NEVER ACCEPTED — the same helper the edit route
    // uses, so `logweir-destination://<name>` has exactly one producer in this
    // binary and `destinationRef` cannot arrive with an inline URL beside it.
    let (archive, destination_ref) = destination_or_archive(
        request.archive.as_ref(),
        request.destination_ref.as_ref(),
        &mut errors,
    );
    debug_assert!(errors.is_empty(), "validate_create ran first");
    let (topics, all_user_topics) = selection_fields(&create_selection(request));
    BackupSchedule {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: Some(namespace.to_string()),
            annotations: Some(annotations),
            ..ObjectMeta::default()
        },
        spec: BackupScheduleSpec {
            schedule: request.schedule.clone(),
            source_ref: LocalRef {
                name: request.source_ref.name.clone(),
            },
            topics,
            archive,
            destination_ref,
            // D1 W2 added the cadence and selection policy fields to
            // `BackupScheduleSpec` and D1 W6 gave the edit route all of them.
            // PLAT-10.1 gave them to this route as well, because a form that
            // can only create a policy it cannot express has to follow every
            // create with an edit against an object that is already
            // scheduling. An ABSENT field is still written absent, which is
            // still the documented default: UTC, a one-hour starting deadline,
            // no catch-up, no retries and a 3600 s run deadline.
            all_user_topics,
            time_zone: request.time_zone.clone(),
            starting_deadline_seconds: request.starting_deadline_seconds,
            catch_up_policy: request.catch_up_policy.map(|c| match c {
                CatchUpPolicy::None => CrdCatchUp::None,
                CatchUpPolicy::Latest => CrdCatchUp::Latest,
            }),
            retry: request.retry.as_ref().map(|r| RetrySpec {
                max_retries: r.max_retries,
                delay_seconds: r.delay_seconds,
            }),
            active_deadline_seconds: request.active_deadline_seconds,
            concurrency_policy: match request.concurrency_policy {
                None | Some(ConcurrencyPolicy::Forbid) => CrdConcurrency::Forbid,
                Some(ConcurrencyPolicy::Allow) => CrdConcurrency::Allow,
            },
            retention: request.retention.as_ref().map(|r| Retention {
                keep_last: r.keep_last,
                keep_days: r.keep_days,
            }),
            suspend: request.suspended,
        },
        status: None,
    }
}

/// `POST .../schedules`.
pub async fn create(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(ns): ApiPath<String>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::CreateSchedule)?;
    crate::http::parse_query(uri.query(), &[])?;
    let key = IdempotencyKey::from_headers(&headers)?;
    let mut request: CreateScheduleRequest = read_json(body, MAX_JSON_BODY).await?;
    validate_create(&request)?;
    // Canonical form: an omitted policy IS `Forbid`, so both spellings hash
    // identically and replay as one request.
    request.concurrency_policy = Some(
        request
            .concurrency_policy
            .unwrap_or(ConcurrencyPolicy::Forbid),
    );
    let created = create_idempotent(
        &state,
        &actor,
        &ns,
        ROUTE_CREATE,
        NAME_PREFIX,
        &key,
        &request_id,
        &request,
        |name, annotations| build(&ns, name, annotations, &request),
    )
    .await?;
    Ok(json(
        created.status(),
        &ScheduleResponse {
            request_id,
            replayed: Some(created.replayed),
            item: projection::schedule(&created.object),
        },
    ))
}

/// `POST .../schedules/{name}:set-suspension`.
#[allow(clippy::too_many_arguments)]
pub async fn command(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, target)): ApiPath<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::SetScheduleSuspension)?;
    let Some(name) = target.strip_suffix(SET_SUSPENSION) else {
        return Err(ApiError::new(ProblemCode::NotFound, "No such command."));
    };
    check_name(name)?;
    crate::http::parse_query(uri.query(), &[])?;
    IdempotencyKey::refuse_on(
        &headers,
        ROUTE_SET_SUSPENSION,
        Some("expectedResourceVersion"),
    )?;
    let request: SetSuspensionRequest = read_json(body, MAX_JSON_BODY).await?;
    if validate::check_single_line(&request.expected_resource_version, 128).is_err() {
        return Err(ApiError::validation(vec![FieldError::new(
            "expectedResourceVersion",
            "required",
            "the resourceVersion last read is required",
        )]));
    }
    let updated = state
        .kube()
        .set_schedule_suspension(
            &ns,
            name,
            request.suspended,
            &request.expected_resource_version,
        )
        .await
        .map_err(|failure| match failure {
            KubeFailure::Conflict => ApiError::new(
                ProblemCode::PreconditionFailed,
                "expectedResourceVersion is not the schedule's current resourceVersion; read it \
                 again.",
            ),
            other => other.into_api_error(),
        })?;
    tracing::info!(
        namespace = %ns,
        name,
        suspended = request.suspended,
        actor = %actor.id(),
        "schedule suspension set"
    );
    Ok(json(
        StatusCode::OK,
        &ScheduleResponse {
            request_id,
            replayed: None,
            item: projection::schedule(&updated),
        },
    ))
}

// ======================================================================
// PLAT-05.1: the editable future policy
// ======================================================================

/// The edit route identifier.
pub const ROUTE_UPDATE: &str = "PUT /api/v1/namespaces/{ns}/schedules/{name}";

/// The most named topics one schedule may carry, as on create.
pub const MAX_TOPICS: usize = 256;

/// Validate a topic selection and turn it into the CRD's two fields.
///
/// WHAT IS REFUSED HERE, AND WHAT IS DELIBERATELY NOT. A glob, a malformed
/// name, a duplicate and an EMPTY selection are refused before any write: the
/// CRD cannot express them (a stored `topics: []` object would be stranded by
/// such a rule on the 1.29 floor, D1 §5.2) and a run with nothing to do is not
/// a run. The THIRD SHAPE — a named allowlist together with `allUserTopics` —
/// is NOT refused here: it is the CRD's own R2, and D1 §5.2 says the API must
/// let the API server refuse it rather than keep a copy that can drift.
///
/// WHERE THE FIELD PATHS COME FROM. `field` is the name the selection has in
/// the request that carries it — `topicSelection` on the edit route, and the
/// EMPTY STRING on the create route, whose `topics` and `allUserTopics` are
/// top-level fields (PLAT-10.1). With an empty `field` the paths lose their
/// prefix (`topics[0]`, not `.topics[0]`) and the empty-selection refusal is
/// reported on `topics`, which is the input a console has to highlight; the
/// edit route's own paths are byte-identical to what they were.
pub(crate) fn validate_selection(
    field: &str,
    selection: &TopicSelectionRequest,
    errors: &mut Vec<FieldError>,
) {
    let path = |suffix: &str| {
        if field.is_empty() {
            suffix.to_string()
        } else {
            format!("{field}.{suffix}")
        }
    };
    if selection.topics.is_empty() && selection.all_user_topics.is_none() {
        errors.push(FieldError::new(
            if field.is_empty() { "topics" } else { field },
            "selection_invalid",
            "a run needs either named topics or allUserTopics; an empty selection is not a run",
        ));
    }
    if selection.topics.len() > MAX_TOPICS {
        errors.push(FieldError::new(
            path("topics"),
            "count_out_of_range",
            format!("at most {MAX_TOPICS} named topics; patterns are refused"),
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    for (i, topic) in selection.topics.iter().enumerate() {
        if !validate::is_topic_name(topic) {
            errors.push(FieldError::new(
                path(&format!("topics[{i}]")),
                "invalid_topic",
                "must be a Kafka topic name; patterns are refused",
            ));
        } else if !seen.insert(topic.as_str()) {
            errors.push(FieldError::new(
                path(&format!("topics[{i}]")),
                "duplicate",
                "each topic may appear once",
            ));
        }
    }
    let Some(dynamic) = &selection.all_user_topics else {
        return;
    };
    let Some(exclude) = &dynamic.exclude else {
        return;
    };
    for (i, topic) in exclude.topics.iter().flatten().enumerate() {
        if !validate::is_topic_name(topic) {
            errors.push(FieldError::new(
                path(&format!("allUserTopics.exclude.topics[{i}]")),
                "invalid_topic",
                "an exclusion is an exact Kafka topic name, never a pattern",
            ));
        }
    }
    for (i, prefix) in exclude.prefixes.iter().flatten().enumerate() {
        if prefix.is_empty() || prefix.len() > 249 || !is_topic_prefix(prefix) {
            errors.push(FieldError::new(
                path(&format!("allUserTopics.exclude.prefixes[{i}]")),
                "invalid_prefix",
                "an exclusion prefix is 1 to 249 characters of [a-zA-Z0-9._-]; `orders-` \
                 excludes `orders-eu` because it is a literal prefix, and `orders*` is not a \
                 prefix at all",
            ));
        }
    }
}

/// The CRD's `TOPIC_PREFIX_PATTERN`, spelled as a scan so no regex engine is
/// linked for it.
fn is_topic_prefix(value: &str) -> bool {
    value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// The CRD selection fields for a validated request.
pub(crate) fn selection_fields(
    selection: &TopicSelectionRequest,
) -> (Vec<String>, Option<CrdAllUserTopics>) {
    let dynamic = selection
        .all_user_topics
        .as_ref()
        .map(|d| CrdAllUserTopics {
            exclude: d.exclude.as_ref().map(|e| CrdTopicExclusions {
                topics: e.topics.clone(),
                prefixes: e.prefixes.clone(),
            }),
            incomplete_discovery: match d.incomplete_discovery {
                IncompleteDiscoveryPolicy::Refuse => CrdIncompleteDiscovery::Refuse,
                IncompleteDiscoveryPolicy::BackUpVisibleTopics => {
                    CrdIncompleteDiscovery::BackUpVisibleTopics
                }
            },
        });
    (selection.topics.clone(), dynamic)
}

/// Where a run writes: an inline archive, or the sentinel a saved destination
/// requires.
///
/// THE SENTINEL IS BUILT HERE AND NEVER ACCEPTED FROM A BODY. The CRD's rule
/// is `archive.url == "logweir-destination://" + destinationRef.name` with no
/// `secretRef`; a client that could send that URL itself could also send it
/// WITHOUT a `destinationRef`, which is the reserved-scheme case the rule
/// exists to refuse. `validate::check_archive_url` refuses the scheme outright,
/// so the only way to reach it is this function.
pub(crate) fn destination_or_archive(
    archive: Option<&ArchiveRequest>,
    destination_ref: Option<&NameRef>,
    errors: &mut Vec<FieldError>,
) -> (ArchiveRef, Option<LocalRef>) {
    match (archive, destination_ref) {
        (Some(a), None) => {
            validate_archive("archive", a, errors);
            (
                ArchiveRef {
                    url: a.url.clone(),
                    secret_ref: a.credential_ref.as_ref().map(|r| LocalRef {
                        name: r.name.clone(),
                    }),
                },
                None,
            )
        }
        (None, Some(d)) => {
            if !validate::is_dns_subdomain(&d.name) {
                errors.push(FieldError::new(
                    "destinationRef.name",
                    "invalid_name",
                    "must be a Kubernetes object name",
                ));
            }
            (
                ArchiveRef {
                    url: format!("{DESTINATION_URL_SCHEME}{}", d.name),
                    secret_ref: None,
                },
                Some(LocalRef {
                    name: d.name.clone(),
                }),
            )
        }
        (Some(_), Some(_)) | (None, None) => {
            errors.push(FieldError::new(
                "archive",
                "required",
                "send exactly one of archive or destinationRef",
            ));
            (
                ArchiveRef {
                    url: String::new(),
                    secret_ref: None,
                },
                None,
            )
        }
    }
}

fn check_range(field: &str, value: Option<i64>, min: i64, max: i64, errors: &mut Vec<FieldError>) {
    if let Some(v) = value {
        if !(min..=max).contains(&v) {
            errors.push(FieldError::new(
                field,
                "out_of_range",
                format!("must be from {min} to {max}"),
            ));
        }
    }
}

/// Validate an edit, field by field, before anything is read or written.
///
/// # Errors
///
/// `validation_failed` naming every invalid field.
pub fn validate_update(request: &UpdateSchedulePolicyRequest) -> Result<(), ApiError> {
    let mut errors = Vec::new();
    validate_cadence_fields(&request.schedule, request.time_zone.as_deref(), &mut errors);
    if request.source_ref.is_some() {
        // THE ONE IMMUTABLE FIELD, REFUSED BEFORE ANY READ OR WRITE. The CRD's
        // R1 would refuse it too, and says the same sentence — this is the
        // same rule answered a round trip earlier, with the CRD's own words.
        errors.push(FieldError::new(
            "sourceRef",
            "field_immutable",
            weirkeeper::crds::backup_schedule::SOURCE_REF_IMMUTABLE_MESSAGE,
        ));
    }
    validate_policy_fields(
        &PolicyFields {
            selection_prefix: "topicSelection",
            selection: &request.topic_selection,
            archive: request.archive.as_ref(),
            destination_ref: request.destination_ref.as_ref(),
            starting_deadline_seconds: request.starting_deadline_seconds,
            active_deadline_seconds: request.active_deadline_seconds,
            retry: request.retry.as_ref(),
            retention: request.retention.as_ref(),
        },
        &mut errors,
    );
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ApiError::validation(errors))
    }
}

/// The cadence half both routes validate identically: a single-line
/// expression, parsed by THE CONTROLLER'S OWN PARSER in the zone it will be
/// stored with, against the controller's own zone table, and a bounded zone
/// name. An expression this API accepts and the scheduler refuses would leave
/// `Ready=False` on an object the console said was fine.
fn validate_cadence_fields(schedule: &str, time_zone: Option<&str>, errors: &mut Vec<FieldError>) {
    match validate::check_single_line(schedule, 128) {
        Err(code) => errors.push(FieldError::new(
            "schedule",
            code,
            "a cron expression is required",
        )),
        Ok(()) => {
            if let Err(e) = Cadence::parse(schedule, time_zone) {
                let (field, code) = match e {
                    CadenceError::UnknownTimeZone { .. } => ("timeZone", TIMEZONE_UNKNOWN),
                    CadenceError::Schedule(_) => ("schedule", SCHEDULE_INVALID),
                };
                errors.push(FieldError::new(
                    field,
                    code,
                    validate::bounded(&e.to_string(), 256),
                ));
            }
        }
    }
    if let Some(zone) = time_zone {
        if validate::check_single_line(zone, 64).is_err() {
            errors.push(FieldError::new(
                "timeZone",
                "too_long",
                "timeZone must be an IANA zone name of at most 64 characters",
            ));
        }
    }
}

/// The future-policy members the create and the replace routes share, read
/// from either DTO. ONE VALIDATOR FOR BOTH (review LOW-3): the create route
/// used to carry a copy of the edit route's rules, and the next edit to one
/// copy would have drifted from the other.
struct PolicyFields<'a> {
    /// Where the selection's field errors are reported: `""` on create (flat
    /// `topics`/`allUserTopics`), `"topicSelection"` on the replace.
    selection_prefix: &'a str,
    selection: &'a TopicSelectionRequest,
    archive: Option<&'a ArchiveRequest>,
    destination_ref: Option<&'a NameRef>,
    starting_deadline_seconds: Option<i64>,
    active_deadline_seconds: Option<i64>,
    retry: Option<&'a RetryPolicy>,
    retention: Option<&'a RetentionRequest>,
}

fn validate_policy_fields(fields: &PolicyFields<'_>, errors: &mut Vec<FieldError>) {
    validate_selection(fields.selection_prefix, fields.selection, errors);
    let _ = destination_or_archive(fields.archive, fields.destination_ref, errors);
    check_range(
        "startingDeadlineSeconds",
        fields.starting_deadline_seconds,
        cadence::MIN_STARTING_DEADLINE_SECONDS,
        cadence::MAX_STARTING_DEADLINE_SECONDS,
        errors,
    );
    check_range(
        "activeDeadlineSeconds",
        fields.active_deadline_seconds,
        cadence::MIN_ACTIVE_DEADLINE_SECONDS,
        cadence::MAX_ACTIVE_DEADLINE_SECONDS,
        errors,
    );
    if let Some(retry) = fields.retry {
        if !(0..=3).contains(&retry.max_retries) {
            errors.push(FieldError::new(
                "retry.maxRetries",
                "out_of_range",
                "must be from 0 to 3",
            ));
        }
        check_range(
            "retry.delaySeconds",
            retry.delay_seconds,
            cadence::MIN_RETRY_DELAY_SECONDS,
            cadence::MAX_RETRY_DELAY_SECONDS,
            errors,
        );
    }
    if let Some(retention) = fields.retention {
        for (field, value) in [
            ("retention.keepLast", retention.keep_last),
            ("retention.keepDays", retention.keep_days),
        ] {
            check_range(field, value, 0, 100_000, errors);
        }
    }
}

/// `PUT .../schedules/{name}` — replace the future policy (D1 §5.6).
pub async fn update(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, name)): ApiPath<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::EditSchedulePolicy)?;
    check_name(&name)?;
    crate::http::parse_query(uri.query(), &[])?;
    // AN EDIT IS NOT A DURABLE CREATE. It has a precondition instead: the same
    // body sent twice under the same `expectedGeneration` is a 412 the second
    // time, because the first one moved the generation. An `Idempotency-Key`
    // would promise a replay this route cannot give.
    IdempotencyKey::refuse_on(&headers, ROUTE_UPDATE, Some("expectedGeneration"))?;
    let request: UpdateSchedulePolicyRequest = read_json(body, MAX_JSON_BODY).await?;
    validate_update(&request)?;

    let current = get_object::<BackupSchedule>(&state, &actor, &ns, &name).await?;
    let generation = current.metadata.generation.unwrap_or_default();
    if generation != request.expected_generation {
        actor.audit.set_failure("precondition_failed");
        return Err(ApiError::new(
            ProblemCode::PreconditionFailed,
            format!(
                "expectedGeneration {} is not this schedule's current generation ({generation}); \
                 read it again.",
                request.expected_generation
            ),
        ));
    }
    let version = current
        .metadata
        .resource_version
        .clone()
        .unwrap_or_default();

    let mut errors = Vec::new();
    let (archive, destination_ref) = destination_or_archive(
        request.archive.as_ref(),
        request.destination_ref.as_ref(),
        &mut errors,
    );
    debug_assert!(errors.is_empty(), "validate_update ran first");
    let (topics, all_user_topics) = selection_fields(&request.topic_selection);
    let edit = ScheduleSpecEdit {
        schedule: request.schedule.clone(),
        time_zone: request.time_zone.clone(),
        topics,
        all_user_topics,
        archive,
        destination_ref,
        concurrency_policy: match request.concurrency_policy {
            None | Some(ConcurrencyPolicy::Forbid) => CrdConcurrency::Forbid,
            Some(ConcurrencyPolicy::Allow) => CrdConcurrency::Allow,
        },
        starting_deadline_seconds: request.starting_deadline_seconds,
        catch_up_policy: request.catch_up_policy.map(|c| match c {
            CatchUpPolicy::None => CrdCatchUp::None,
            CatchUpPolicy::Latest => CrdCatchUp::Latest,
        }),
        retry: request.retry.as_ref().map(|r| RetrySpec {
            max_retries: r.max_retries,
            delay_seconds: r.delay_seconds,
        }),
        active_deadline_seconds: request.active_deadline_seconds,
        retention: request.retention.as_ref().map(|r| Retention {
            keep_last: r.keep_last,
            keep_days: r.keep_days,
        }),
        suspend: request.suspended,
    };

    let updated = state
        .kube()
        .set_schedule_policy(&ns, &name, &edit, &version)
        .await
        .map_err(|failure| match failure {
            // THE OBJECT MOVED BETWEEN THE READ AND THE WRITE. The generation
            // matched a moment ago; something landed in between. That is the
            // same answer as a stale generation, and for the same reason.
            KubeFailure::Conflict => ApiError::new(
                ProblemCode::PreconditionFailed,
                "The schedule changed between the read and the edit; read it again and retry.",
            ),
            other => other.into_api_error(),
        })?;
    tracing::info!(
        namespace = %ns,
        name = %name,
        from_generation = generation,
        to_generation = updated.metadata.generation.unwrap_or_default(),
        actor = %actor.id(),
        "schedule policy edited"
    );
    // The REVISION the edit produced, so a console can show "g8" without a
    // second read. `runPolicySha256` is the CONTROLLER's, and it is absent
    // until the controller observes this generation — this projection never
    // invents one.
    Ok(json(
        StatusCode::OK,
        &ScheduleResponse {
            request_id,
            replayed: None,
            item: projection::schedule(&updated),
        },
    ))
}
