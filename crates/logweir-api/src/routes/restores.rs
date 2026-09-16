//! Restores: `Restore` projections and a typed create whose `planBytes` are
//! OPAQUE.
//!
//! THE PLAN IS NEVER REPARSED. The create checks that `planHash` is the
//! SHA-256 of exactly the submitted `planBytes` string and copies that string
//! into `spec.planBytes` unchanged; nothing in this crate parses the plan
//! document, and the JSON string the Kubernetes client sends decodes to the
//! identical bytes. The controller recomputes the same hash before any Job.

use std::collections::BTreeMap;

use axum::body::Body;
use axum::extract::State;
use axum::response::Response;
use chrono::{DateTime, SecondsFormat, Utc};
use http::{HeaderMap, StatusCode, Uri};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use weirkeeper::crds::restore::{Restore, RestoreSpec, RestoreTarget, TargetMode, TopicNaming};
use weirkeeper::crds::{ArchiveRef, LocalRef};

use super::{authorize, create_idempotent, get_object, json, list_page, list_query, ApiPath};
use crate::app::AppState;
use crate::auth::Actor;
use crate::authz::Action;
use crate::contract::{CreateRestoreRequest, RestoreList, RestoreMode, RestoreResponse};
use crate::http::{read_json, RequestId, MAX_JSON_BODY};
use crate::idempotency::IdempotencyKey;
use crate::problem::{ApiError, FieldError};
use crate::projection;
use crate::validate;

/// The list route identifier.
pub const ROUTE_LIST: &str = "GET /api/v1/namespaces/{ns}/restores";
/// The create route identifier.
pub const ROUTE_CREATE: &str = "POST /api/v1/namespaces/{ns}/restores";
/// The deterministic name prefix (30 characters in all).
pub const NAME_PREFIX: &str = "rst-";
/// The largest plan document accepted, 256 KiB.
pub const MAX_PLAN_BYTES: usize = 256 * 1024;

fn is_backup_set_ref(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
}

/// Validate a create request and return the parsed point in time.
///
/// # Errors
///
/// `validation_failed` naming every invalid field.
pub fn validate_create(request: &CreateRestoreRequest) -> Result<DateTime<Utc>, ApiError> {
    let mut errors = Vec::new();
    if request.plan_bytes.is_empty() {
        errors.push(FieldError::new(
            "planBytes",
            "required",
            "the plan document is required",
        ));
    } else if request.plan_bytes.len() > MAX_PLAN_BYTES {
        errors.push(FieldError::new(
            "planBytes",
            "too_long",
            format!("the plan document is at most {MAX_PLAN_BYTES} bytes"),
        ));
    }
    let well_formed_hash = request
        .plan_hash
        .strip_prefix("sha256:")
        .is_some_and(|hex| {
            hex.len() == 64
                && hex
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        });
    if !well_formed_hash {
        errors.push(FieldError::new(
            "planHash",
            "invalid_format",
            "must be sha256: followed by 64 lowercase hexadecimal characters",
        ));
    } else if logweir_core::ids::sha256_prefixed(request.plan_bytes.as_bytes()) != request.plan_hash
    {
        errors.push(FieldError::new(
            "planHash",
            "plan_hash_mismatch",
            "is not the SHA-256 of planBytes exactly as submitted",
        ));
    }
    if !validate::is_dns_subdomain(&request.approval_ref.name) {
        errors.push(FieldError::new(
            "approvalRef.name",
            "invalid_name",
            "must be a Kubernetes object name",
        ));
    }
    super::schedules::validate_archive("sourceArchive", &request.source_archive, &mut errors);
    if !is_backup_set_ref(&request.backup_set_ref) {
        errors.push(FieldError::new(
            "backupSetRef",
            "invalid_backup_set",
            "must be a backup set ID of letters, digits, '.', '_' or '-'",
        ));
    }
    let point_in_time = match DateTime::parse_from_rfc3339(&request.point_in_time) {
        Ok(t) => Some(t.with_timezone(&Utc)),
        Err(_) => {
            errors.push(FieldError::new(
                "pointInTime",
                "invalid_timestamp",
                "must be an RFC 3339 timestamp",
            ));
            None
        }
    };
    if !validate::is_dns_subdomain(&request.target.cluster_ref.name) {
        errors.push(FieldError::new(
            "target.clusterRef.name",
            "invalid_name",
            "must be a Kubernetes object name",
        ));
    }
    let prefix = &request.target.topic_naming.prefix;
    if prefix.is_empty() || prefix.len() > 128 || !validate::is_topic_name(prefix) {
        errors.push(FieldError::new(
            "target.topicNaming.prefix",
            "invalid_prefix",
            "a non-empty topic-name prefix is required: a restore only writes new topics",
        ));
    }
    if !(60..=86_400).contains(&request.deadline_seconds) {
        errors.push(FieldError::new(
            "deadlineSeconds",
            "out_of_range",
            "must be from 60 to 86400",
        ));
    }
    match (errors.is_empty(), point_in_time) {
        (true, Some(t)) => Ok(t),
        _ => Err(ApiError::validation(errors)),
    }
}

/// Build the stored object. `plan_bytes` is moved in unchanged.
#[must_use]
pub fn build(
    namespace: &str,
    name: String,
    annotations: BTreeMap<String, String>,
    request: &CreateRestoreRequest,
    point_in_time: DateTime<Utc>,
) -> Restore {
    Restore {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: Some(namespace.to_string()),
            annotations: Some(annotations),
            ..ObjectMeta::default()
        },
        spec: RestoreSpec {
            plan_bytes: request.plan_bytes.clone(),
            approval_ref: LocalRef {
                name: request.approval_ref.name.clone(),
            },
            source_archive: ArchiveRef {
                url: request.source_archive.url.clone(),
                secret_ref: request
                    .source_archive
                    .credential_ref
                    .as_ref()
                    .map(|r| LocalRef {
                        name: r.name.clone(),
                    }),
            },
            // D2 W6b's saved destinations are not an API route yet: this
            // route takes an inline archive, so both refs are absent and the
            // object behaves exactly as it did before the fields existed.
            source_destination_ref: None,
            evidence_destination_ref: None,
            backup_set_ref: request.backup_set_ref.clone(),
            point_in_time,
            target: RestoreTarget {
                cluster_ref: LocalRef {
                    name: request.target.cluster_ref.name.clone(),
                },
                mode: match request.target.mode {
                    RestoreMode::Scratch => TargetMode::Scratch,
                    RestoreMode::NewTopic => TargetMode::NewTopic,
                },
                topic_naming: TopicNaming {
                    prefix: request.target.topic_naming.prefix.clone(),
                },
            },
            deadline_seconds: request.deadline_seconds,
        },
        status: None,
    }
}

/// `GET .../restores`.
pub async fn list(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(ns): ApiPath<String>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadRestores)?;
    let query = list_query(uri.query())?;
    let (items, page) = list_page::<Restore>(&state, &actor, &ns, ROUTE_LIST, &query).await?;
    Ok(json(
        StatusCode::OK,
        &RestoreList {
            request_id,
            items: items
                .iter()
                .map(|r| projection::restore(r, false))
                .collect(),
            page,
        },
    ))
}

/// `GET .../restores/{name}`.
pub async fn get_one(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, name)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadRestores)?;
    crate::http::parse_query(uri.query(), &[])?;
    let object = get_object::<Restore>(&state, &actor, &ns, &name).await?;
    Ok(json(
        StatusCode::OK,
        &RestoreResponse {
            request_id,
            replayed: None,
            item: projection::restore(&object, true),
        },
    ))
}

/// `POST .../restores`.
pub async fn create(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(ns): ApiPath<String>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::CreateRestore)?;
    crate::http::parse_query(uri.query(), &[])?;
    let key = IdempotencyKey::from_headers(&headers)?;
    let mut request: CreateRestoreRequest = read_json(body, MAX_JSON_BODY).await?;
    let point_in_time = validate_create(&request)?;
    // Canonical form for the request hash: equivalent RFC 3339 spellings of
    // one instant are one request. `planBytes` is NOT canonicalised.
    request.point_in_time = point_in_time.to_rfc3339_opts(SecondsFormat::AutoSi, true);
    let created = create_idempotent(
        &state,
        &actor,
        &ns,
        ROUTE_CREATE,
        NAME_PREFIX,
        &key,
        &request_id,
        &request,
        |name, annotations| build(&ns, name, annotations, &request, point_in_time),
    )
    .await?;
    Ok(json(
        created.status(),
        &RestoreResponse {
            request_id,
            replayed: Some(created.replayed),
            item: projection::restore(&created.object, true),
        },
    ))
}
