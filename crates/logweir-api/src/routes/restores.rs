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
use crate::contract::{
    CreateRestoreRequest, RestoreList, RestoreMode, RestoreResponse, TopicMappingRow,
};
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
/// The most mapping rows a declaration may carry — the same bound
/// `CreateScheduleRequest` puts on a named topic list.
pub const MAX_TOPIC_MAPPING_ROWS: usize = 1000;

/// Check a declared `topicMapping` against the prefix the same request stores.
///
/// # What this can check, and why it is not "parsing the plan"
///
/// It never reads `planBytes`. The mapping rule is prefix concatenation and
/// nothing else in this version (`logweir_core::spec::target_topic_prefix`:
/// `newTopic` takes `target.topic_naming.prefix`, `scratch` takes
/// `topic_mapping_prefix`, and neither admits a per-topic rename), and the
/// prefix is a STORED field of `Restore.spec`. So `prefix + source` is a pure
/// function of the object this route is about to create, and every row that is
/// not that is a request whose preview and whose submission disagree.
///
/// The three refusals, each naming the value the operator has to go and fix:
///
/// * `duplicate_mapping` — two sources produce one target name. With an
///   injective prefix map this can only be a REPEATED source, which is why the
///   message names BOTH rows rather than one.
/// * `mapping_mismatch` — a row whose target is not `prefix + source`.
/// * `mapped_name_illegal` — a row whose target is not a name a broker would
///   accept, `logweir_core::guard::topic_name_is_kafka_legal`; and
///   `mapping_identity` for a target equal to its source, which is a restore
///   writing over the topic it came from
///   (`logweir_core::guard::check_topic_mapping_coverage`).
fn validate_topic_mapping(rows: &[TopicMappingRow], prefix: &str, errors: &mut Vec<FieldError>) {
    if rows.is_empty() {
        errors.push(FieldError::new(
            "topicMapping",
            "empty",
            "a declared mapping names at least one topic; omit the field to declare none",
        ));
        return;
    }
    if rows.len() > MAX_TOPIC_MAPPING_ROWS {
        errors.push(FieldError::new(
            "topicMapping",
            "too_many",
            format!("at most {MAX_TOPIC_MAPPING_ROWS} mapping rows"),
        ));
        return;
    }
    // FIRST SEEN WINS, so the message names the row an operator would keep and
    // the row they would delete, in the order they sent them.
    let mut first_for_target: BTreeMap<&str, &str> = BTreeMap::new();
    for (index, row) in rows.iter().enumerate() {
        if !logweir_core::guard::topic_name_is_kafka_legal(&row.source) {
            errors.push(FieldError::new(
                format!("topicMapping[{index}].source"),
                "invalid_topic",
                "must be a topic name a broker accepts: letters, digits, '.', '_' or '-'",
            ));
            continue;
        }
        if !logweir_core::guard::topic_name_is_kafka_legal(&row.target) {
            errors.push(FieldError::new(
                format!("topicMapping[{index}].target"),
                "mapped_name_illegal",
                format!(
                    "the mapped name for `{}` is not a name a broker accepts; shorten the \
                     prefix or rename the topic",
                    row.source
                ),
            ));
            continue;
        }
        if row.target == row.source {
            errors.push(FieldError::new(
                format!("topicMapping[{index}].target"),
                "mapping_identity",
                format!(
                    "maps `{}` onto itself; a restore writes to a NEW topic and the target \
                     must differ from the source",
                    row.source
                ),
            ));
            continue;
        }
        let expected = format!("{prefix}{}", row.source);
        if row.target != expected {
            errors.push(FieldError::new(
                format!("topicMapping[{index}].target"),
                "mapping_mismatch",
                format!(
                    "`{}` maps to `{expected}` under the prefix this request stores; the \
                     preview and the submission disagree",
                    row.source
                ),
            ));
            continue;
        }
        if let Some(first) = first_for_target.insert(row.target.as_str(), row.source.as_str()) {
            errors.push(FieldError::new(
                format!("topicMapping[{index}].target"),
                "duplicate_mapping",
                format!(
                    "`{}` and `{}` both map to the target topic `{}`; one restore cannot \
                     write two source topics into one target",
                    first, row.source, row.target
                ),
            ));
        }
    }
}

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
    match (
        request.source_destination_ref.as_ref(),
        request.evidence_destination_ref.as_ref(),
    ) {
        (Some(source), Some(evidence)) => {
            for (field, reference) in [
                ("sourceDestinationRef.name", source),
                ("evidenceDestinationRef.name", evidence),
            ] {
                if !validate::is_dns_subdomain(&reference.name) {
                    errors.push(FieldError::new(
                        field,
                        "invalid_name",
                        "must be a Kubernetes object name",
                    ));
                }
            }
            let expected = format!("logweir-destination://{}", source.name);
            if request.source_archive.url != expected {
                errors.push(FieldError::new(
                    "sourceArchive.url",
                    "destination_sentinel_mismatch",
                    format!(
                        "must be `{expected}` when sourceDestinationRef names `{}`",
                        source.name
                    ),
                ));
            }
            if request.source_archive.credential_ref.is_some() {
                errors.push(FieldError::new(
                    "sourceArchive.credentialRef",
                    "destination_owns_credential",
                    "must be absent when sourceDestinationRef is set",
                ));
            }
        }
        (None, None) => super::schedules::validate_archive(
            "sourceArchive",
            &request.source_archive,
            &mut errors,
        ),
        (Some(_), None) => errors.push(FieldError::new(
            "evidenceDestinationRef",
            "required_together",
            "is required when sourceDestinationRef is set",
        )),
        (None, Some(_)) => errors.push(FieldError::new(
            "sourceDestinationRef",
            "required_together",
            "is required when evidenceDestinationRef is set",
        )),
    }
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
    // THE MAPPING IS CHECKED AGAINST THE PREFIX ABOVE, and only when that
    // prefix is itself legal: a mismatch computed from a refused prefix would
    // name an expected target nobody could ever produce, so the operator would
    // be sent to fix the wrong field.
    //
    // AND ONLY FOR `newTopic`, WHICH IS A FAIL-CLOSED REFUSAL AND NOT A GAP.
    // `logweir_core::spec::target_topic_prefix` reads
    // `target.topic_naming.prefix` for `newTopic` and the PLAN's own
    // `topic_mapping_prefix` for `scratch` -- and this route never parses the
    // plan, so in `scratch` mode it does not hold the string the run will
    // actually map through. A check against `topicNaming.prefix` would then be
    // a verdict about a value the runner does not read: it would pass a
    // declaration that disagrees with the run and fail one that agrees with
    // it. This service does not perform a check on a value it does not have,
    // so the declaration is defined for `newTopic` and is refused by name for
    // `scratch`. The independent review found the previous code claiming both
    // modes while checking one.
    if let Some(rows) = request.topic_mapping.as_deref() {
        if matches!(request.target.mode, RestoreMode::Scratch) {
            errors.push(FieldError::new(
                "topicMapping",
                "unsupported_for_mode",
                "a declared mapping is defined for target.mode `newTopic` only: in `scratch` \
                 the runner maps through the plan's own `topic_mapping_prefix`, which this \
                 route never parses, so nothing here could check the declaration against the \
                 prefix the run would use",
            ));
        } else if errors
            .iter()
            .all(|e| e.field != "target.topicNaming.prefix")
        {
            validate_topic_mapping(rows, prefix, &mut errors);
        }
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
            approval_ref: Some(LocalRef {
                name: request.approval_ref.name.clone(),
            }),
            // This route creates a per-run, human-approved Restore. A standing
            // authorization is minted by the rehearsal controller and never by
            // an HTTP request, so `authorization` is absent here by
            // construction and the CEL rule that refuses both is satisfied.
            authorization: None,
            runner_resources: None,
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
            source_destination_ref: request.source_destination_ref.as_ref().map(|r| LocalRef {
                name: r.name.clone(),
            }),
            evidence_destination_ref: request.evidence_destination_ref.as_ref().map(|r| LocalRef {
                name: r.name.clone(),
            }),
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
