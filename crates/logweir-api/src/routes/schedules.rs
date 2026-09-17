//! Schedules: `BackupSchedule` projections, a typed create, and the one
//! permitted mutation — `:set-suspension` under a resourceVersion
//! precondition.

use std::collections::BTreeMap;

use axum::body::Body;
use axum::extract::State;
use axum::response::Response;
use http::{HeaderMap, StatusCode, Uri};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use weirkeeper::crds::backup_schedule::{
    BackupSchedule, BackupScheduleSpec, ConcurrencyPolicy as CrdConcurrency, Retention,
};
use weirkeeper::crds::{ArchiveRef, LocalRef};

use super::{
    authorize, check_name, create_idempotent, get_object, json, list_page, list_query, ApiPath,
};
use crate::app::AppState;
use crate::auth::Actor;
use crate::authz::Action;
use crate::contract::{
    ArchiveRequest, ConcurrencyPolicy, CreateScheduleRequest, ScheduleList, ScheduleResponse,
    SetSuspensionRequest,
};
use crate::http::{read_json, RequestId, MAX_JSON_BODY};
use crate::idempotency::IdempotencyKey;
use crate::kube::KubeFailure;
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
    match validate::check_single_line(&request.schedule, 128) {
        Err(code) => errors.push(FieldError::new(
            "schedule",
            code,
            "a cron expression is required",
        )),
        Ok(()) => {
            if let Err(e) = weirkeeper::slot::Cron::parse(&request.schedule) {
                errors.push(FieldError::new(
                    "schedule",
                    "invalid_cron",
                    validate::bounded(&e.to_string(), 256),
                ));
            }
        }
    }
    if !validate::is_dns_subdomain(&request.source_ref.name) {
        errors.push(FieldError::new(
            "sourceRef.name",
            "invalid_name",
            "must be a Kubernetes object name",
        ));
    }
    if request.topics.is_empty() || request.topics.len() > 256 {
        errors.push(FieldError::new(
            "topics",
            "count_out_of_range",
            "between 1 and 256 named topics are required; patterns are refused",
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    for (i, topic) in request.topics.iter().enumerate() {
        if !validate::is_topic_name(topic) {
            errors.push(FieldError::new(
                format!("topics[{i}]"),
                "invalid_topic",
                "must be a Kafka topic name; patterns are refused",
            ));
        } else if !seen.insert(topic.as_str()) {
            errors.push(FieldError::new(
                format!("topics[{i}]"),
                "duplicate",
                "each topic may appear once",
            ));
        }
    }
    validate_archive("archive", &request.archive, &mut errors);
    if let Some(retention) = &request.retention {
        for (field, value) in [
            ("retention.keepLast", retention.keep_last),
            ("retention.keepDays", retention.keep_days),
        ] {
            if let Some(v) = value {
                if !(0..=100_000).contains(&v) {
                    errors.push(FieldError::new(
                        field,
                        "out_of_range",
                        "must be from 0 to 100000",
                    ));
                }
            }
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ApiError::validation(errors))
    }
}

/// Build the stored object.
#[must_use]
pub fn build(
    namespace: &str,
    name: String,
    annotations: BTreeMap<String, String>,
    request: &CreateScheduleRequest,
) -> BackupSchedule {
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
            topics: request.topics.clone(),
            archive: ArchiveRef {
                url: request.archive.url.clone(),
                secret_ref: request.archive.credential_ref.as_ref().map(|r| LocalRef {
                    name: r.name.clone(),
                }),
            },
            // As in the restore route: this route takes an inline archive, so
            // the saved-destination reference is absent and the sentinel rule
            // has nothing to bind.
            destination_ref: None,
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
