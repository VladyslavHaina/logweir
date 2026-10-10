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
use kube::ResourceExt as _;
use logweir_core::approval_policy::{
    ApprovalMode, EffectivePolicy, OperatorMode, Requester, RestoreAuthorization,
};
use weirkeeper::crds::approval::{Approval, ApprovalSpec, SubjectKind, SubjectRef};
use weirkeeper::crds::restore::{
    Restore, RestoreSpec, RestoreTarget, TargetMode, TopicNaming, VerificationCoverage,
};
use weirkeeper::crds::{ArchiveRef, LocalRef};

use super::{authorize, create_idempotent, get_object, json, list_page, list_query, ApiPath};
use crate::app::AppState;
use crate::approval;
use crate::auth::Actor;
use crate::authz::Action;
use crate::contract::{
    ApprovalResponse, AuthorizationState, CreateRestoreRequest, RestoreCoverage, RestoreList,
    RestoreMode, RestoreResponse, RestoreRoutingView, SubmitApprovalRequest, TopicMappingRow,
};
use crate::http::{read_json, RequestId, MAX_JSON_BODY};
use crate::idempotency::IdempotencyKey;
use crate::kube::KubeFailure;
use crate::problem::ProblemCode;
use crate::problem::{ApiError, FieldError};
use crate::projection;
use crate::validate;

/// The list route identifier.
pub const ROUTE_LIST: &str = "GET /api/v1/namespaces/{ns}/restores";
/// The create route identifier.
pub const ROUTE_CREATE: &str = "POST /api/v1/namespaces/{ns}/restores";
/// The governed approval submission route identifier (PLAT-19.2).
pub const ROUTE_SUBMIT_APPROVAL: &str = "POST /api/v1/namespaces/{ns}/restores/{name}/approval";
/// The largest submitted sidecar, 64 KiB.
pub const MAX_SIDECAR_BYTES: usize = 64 * 1024;
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
fn validate_topic_mapping(
    rows: &[TopicMappingRow],
    prefix: &str,
    original_name: bool,
    errors: &mut Vec<FieldError>,
) {
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
        // PROD-15.1: the identity map is the one mapping an original-name
        // restore has, and it is refused everywhere else.
        if row.target == row.source && !original_name {
            errors.push(FieldError::new(
                format!("topicMapping[{index}].target"),
                "mapping_identity",
                format!(
                    "maps `{}` onto itself; a restore writes to a NEW topic and the target \
                     must differ from the source (a restore under the original names sets \
                     target.topicNaming.originalName)",
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
    // PROD-15.1: `originalName: true` is the one request whose prefix is
    // empty — a restore under the original topic names, into topics that do
    // not exist (OD-2) — and only in `newTopic` mode. Every other request
    // keeps the non-empty prefix it always needed.
    let original_name = request.target.topic_naming.original_name == Some(true);
    if original_name {
        if !matches!(request.target.mode, RestoreMode::NewTopic) {
            errors.push(FieldError::new(
                "target.topicNaming.originalName",
                "requires_new_topic",
                "a restore under the original topic names is a `newTopic` restore: a scratch \
                 drill never restores under the original names",
            ));
        }
        if !prefix.is_empty() {
            errors.push(FieldError::new(
                "target.topicNaming.prefix",
                "prefix_with_original_name",
                "must be empty with originalName: a restore under the original topic names \
                 maps every topic onto its own name",
            ));
        }
        // A restore under the original topic names REQUIRES complete
        // verification: a sampled check can pass a record another producer
        // wrote into the restored name. The runner refuses the sampled plan
        // at phase 0 and the controller at admission; refusing the request
        // here names the field before anything is stored or signed.
        if request.coverage != Some(RestoreCoverage::Complete) {
            errors.push(FieldError::new(
                "coverage",
                "original_name_requires_complete",
                "must be `complete` with originalName: a restore under the original topic \
                 names is verified completely, every restored record compared with the \
                 archive, never by sample",
            ));
        }
    } else if prefix.is_empty() || prefix.len() > 128 || !validate::is_topic_name(prefix) {
        errors.push(FieldError::new(
            "target.topicNaming.prefix",
            "invalid_prefix",
            "a non-empty topic-name prefix is required: a restore only writes new topics (a \
             restore under the original topic names sets originalName instead)",
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
        } else if errors.iter().all(|e| {
            e.field != "target.topicNaming.prefix" && e.field != "target.topicNaming.originalName"
        }) {
            validate_topic_mapping(rows, prefix, original_name, &mut errors);
        }
    }
    if !(60..=86_400).contains(&request.deadline_seconds) {
        errors.push(FieldError::new(
            "deadlineSeconds",
            "out_of_range",
            "must be from 60 to 86400",
        ));
    }
    // PROD-08.1a: the record bound bounds a complete verification and nothing
    // else — the CEL rule on `Restore.spec` and the runner's phase 0 refuse the
    // same pair; refusing it here names the field before anything is stored.
    if let Some(bound) = request.complete_max_records {
        if bound < 1 {
            errors.push(FieldError::new(
                "completeMaxRecords",
                "out_of_range",
                "must be at least 1: a complete verification that may decode no record \
                 compares nothing",
            ));
        } else if request.coverage != Some(RestoreCoverage::Complete) {
            errors.push(FieldError::new(
                "completeMaxRecords",
                "requires_complete_coverage",
                "bounds a complete verification and is set only with coverage `complete`",
            ));
        }
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
                    // PROD-15.1: stored only when true, so every other
                    // request stores the object it always did.
                    original_name: (request.target.topic_naming.original_name == Some(true))
                        .then_some(true),
                },
            },
            deadline_seconds: request.deadline_seconds,
            // PROD-08.1a: the declaration, verbatim. The controller holds it
            // to the plan's own `sample.coverage` before anything runs.
            coverage: request.coverage.map(|c| match c {
                RestoreCoverage::Sampled => VerificationCoverage::Sampled,
                RestoreCoverage::Complete => VerificationCoverage::Complete,
            }),
            complete_max_records: request.complete_max_records,
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
            authorization: None,
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
    // PLAT-19.2: a governed request's confirmation lives beside the Approval
    // under `<approvalRef>-confirmation`, and that name must still be a name.
    // PROD-16.1: the policies as this request sees them, the fresh-install
    // marker applied — read BEFORE anything is created.
    let policies = approval::effective_policies(state.approval(), state.kube())
        .await
        .map_err(KubeFailure::into_api_error)?;
    let effective = policies.resolve(&ns);
    refuse_typed_confirmation(&effective, &request)?;
    refuse_before_create(&state, &actor, &ns, &effective, &request)?;
    if effective.mode() == ApprovalMode::Governed
        && !effective.is_legacy()
        && request.approval_ref.name.len() > approval::MAX_GOVERNED_APPROVAL_NAME
    {
        return Err(ApiError::validation(vec![FieldError::new(
            "approvalRef.name",
            "too_long",
            format!(
                "at most {} characters in a namespace bound to a Governed policy, so that its \
                 confirmation `<name>{}` is still an object name",
                approval::MAX_GOVERNED_APPROVAL_NAME,
                approval::CONFIRMATION_SUFFIX
            ),
        )]));
    }
    // Canonical form for the request hash: equivalent RFC 3339 spellings of
    // one instant are one request. `planBytes` is NOT canonicalised.
    request.point_in_time = point_in_time.to_rfc3339_opts(SecondsFormat::AutoSi, true);
    // P10: THE PER-ACTOR CREATE LIMIT, after every refusal that is about the
    // request itself (a malformed restore does not spend the window) and
    // before the object exists. The ceiling on how many restores RUN at once
    // is the controller's manual-restore pool.
    super::run_create_rate(&state, &actor, &ns, super::ManualRun::Restore)?;
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
    // PLAT-19.2 — ROUTE BY THE FROZEN POLICY. The Restore exists (and has a
    // UID) before anything is signed; a replay of an interrupted request
    // completes the sequence by reading what is already there.
    let authorization = authorize_submission(
        &state,
        &actor,
        &ns,
        &created.object,
        &request.approval_ref.name,
        &effective,
        SignedBesideThePlan {
            ticket: request.ticket.as_deref(),
            confirmation: typed_confirmation(&request),
        },
    )
    .await?;
    Ok(json(
        created.status(),
        &RestoreResponse {
            request_id,
            replayed: Some(created.replayed),
            item: projection::restore(&created.object, true),
            authorization: Some(authorization),
        },
    ))
}

/// **OD-10 (PROD-15.1 review M1), decided by the owner on 2026-10-09.** On a
/// one-person-confirmation namespace (`confirm`, internal `Ordinary`) the
/// requester may confirm a restore under the ORIGINAL topic names alone, but
/// only by RE-TYPING every original topic name, exactly; the console signs
/// what was typed into the authorization document. Refused BEFORE the
/// Restore exists, each by name:
///
/// * `originalNameConfirmation.typedTopics` / `typed_topics_required` — an
///   original-name request in such a namespace without the typed names;
/// * `originalNameConfirmation.typedTopics` / `typed_topics_mismatch` — names
///   that are not exactly the plan's `source.topics` (the message names what
///   is missing, extra or repeated);
/// * `originalNameConfirmation` / `not_accepted` — typed names on any other
///   request: an ordinary restore, or a namespace where a second person
///   approves (`strict`) or nothing is signed (unbound), which typed names
///   never replace.
///
/// # Errors
///
/// `validation_failed`.
fn refuse_typed_confirmation(
    effective: &EffectivePolicy,
    request: &CreateRestoreRequest,
) -> Result<(), ApiError> {
    use logweir_core::original_name::{typed_topics_mismatch, MAX_TYPED_TOPICS};
    let original = request.target.topic_naming.original_name == Some(true);
    let one_person = effective
        .bound()
        .is_some_and(|p| p.mode == ApprovalMode::Ordinary);
    match (
        original && one_person,
        request.original_name_confirmation.as_ref(),
    ) {
        (true, None) => Err(ApiError::validation(vec![FieldError::new(
            "originalNameConfirmation.typedTopics",
            "typed_topics_required",
            "this namespace is confirmed by one person (confirm): a restore under the ORIGINAL \
             topic names is confirmed only with every original topic name re-typed, exactly \
             (the owner's decision OD-10); nothing was created",
        )])),
        (true, Some(confirmation)) => {
            let plan = serde_yaml::from_str::<logweir_core::spec::DrillSpec>(&request.plan_bytes)
                .map_err(|e| {
                ApiError::validation(vec![FieldError::new(
                    "planBytes",
                    "invalid",
                    format!(
                        "the plan does not parse, so the typed topic names cannot be \
                             compared with the topics it restores: {e}"
                    ),
                )])
            })?;
            match typed_topics_mismatch(&plan.source.topics, &confirmation.typed_topics) {
                None => Ok(()),
                Some(why) => Err(ApiError::validation(vec![FieldError::new(
                    "originalNameConfirmation.typedTopics",
                    "typed_topics_mismatch",
                    format!(
                        "the typed topic names are not exactly the ones this plan restores \
                         ({why}); re-type every original topic name, exactly (at most \
                         {MAX_TYPED_TOPICS}); nothing was created"
                    ),
                )])),
            }
        }
        (false, Some(_)) => Err(ApiError::validation(vec![FieldError::new(
            "originalNameConfirmation",
            "not_accepted",
            "typed topic names confirm only a restore under the original topic names in a \
             namespace confirmed by one person; here they would replace nothing (an ordinary \
             restore, or a namespace where a second person approves or nothing is signed)",
        )])),
        (false, None) => Ok(()),
    }
}

/// What a create request asks the console to sign beside the plan hash: the
/// change ticket (PLAT-19.2) and, on a one-person confirmation of an
/// original-name restore, the typed topic names (OD-10).
struct SignedBesideThePlan<'a> {
    ticket: Option<&'a str>,
    confirmation: Option<logweir_core::original_name::OriginalNameConfirmation>,
}

/// The typed confirmation a request carries, as the console signs it.
fn typed_confirmation(
    request: &CreateRestoreRequest,
) -> Option<logweir_core::original_name::OriginalNameConfirmation> {
    request.original_name_confirmation.as_ref().map(|c| {
        logweir_core::original_name::OriginalNameConfirmation {
            typed_topics: c.typed_topics.clone(),
        }
    })
}

/// PLAT-19.2's refusals that must come BEFORE the Restore exists, so a refused
/// submission leaves nothing behind.
///
/// * **Only the modes `localAdmin` may serve** (PROD-16.1, amending review
///   H1 and D0's "does not expose Ordinary"): `confirm` and `strict` are
///   served there — the confirming principal is
///   `urn:logweir:local-admin#admin`, and whoever can reach this console can
///   confirm, a residual SECURITY.md states — and `two-person` (PROD-16.2)
///   never is ([`OperatorMode::allowed_in_local_admin`]).
/// * **The console key exists** (PROD-16.1): under a bound policy the
///   console signs, and the managed key is written by the identity hook after
///   this pod starts. Until it is there, nothing is created.
/// * **The ticket** (D0: "required in Governed, optional in Ordinary"), and
///   none in an unbound namespace, which signs nothing.
/// * **No `approvalRef.name` ending in `-confirmation` under Governed**
///   (review L4): it would collide with another Restore's confirmation
///   object.
/// * **A requester a two-person approval can compare** (PROD-16.2): under a
///   policy whose approval the console signs, the requester is one of the two
///   people every reader tells apart
///   (`logweir_core::approval_policy::console_principal`).
///
/// # Errors
///
/// `policy_mismatch` or `validation_failed`.
fn refuse_before_create(
    state: &AppState,
    actor: &Actor,
    ns: &str,
    effective: &EffectivePolicy,
    request: &CreateRestoreRequest,
) -> Result<(), ApiError> {
    let Some(policy) = effective.bound() else {
        if request.ticket.is_some() {
            return Err(ApiError::validation(vec![FieldError::new(
                "ticket",
                "not_accepted",
                format!(
                    "namespace {ns} is bound to no approval policy ({}), so the console signs \
                     nothing here; the approver records the ticket with `logweir drill approve \
                     --ticket`",
                    logweir_core::approval_policy::LEGACY_GOVERNED_POLICY_NAME
                ),
            )]));
        }
        return Ok(());
    };
    let operator_mode = OperatorMode::of(effective);
    if state.shared().is_none() && !operator_mode.allowed_in_local_admin() {
        return Err(ApiError::new(
            ProblemCode::PolicyMismatch,
            format!(
                "Namespace {ns} is bound to approval policy {} ({operator_mode}), and this console \
                 runs in localAdmin mode, whose one identity cannot be two people. Submit through \
                 the shared console. Nothing was created.",
                policy.name
            ),
        ));
    }
    match state.approval().confirmation_key() {
        Ok(Some(_)) => {}
        Ok(None) => {
            return Err(ApiError::new(
                ProblemCode::PolicyMismatch,
                format!(
                    "Namespace {ns} is under approval policy {} ({operator_mode}), which the \
                     console signs, and this console's confirmation key is not there yet: the \
                     installation's identity hook writes it once, at install. Try again in a \
                     minute; if it persists, the hook did not finish (`helm status`). Nothing was \
                     created.",
                    policy.name
                ),
            ))
        }
        Err(reason) => return Err(ApiError::new(ProblemCode::InternalError, reason)),
    }
    // PROD-16.2: under a two-person policy the requester is one of the two
    // people every reader compares. An identity that cannot be compared (or
    // is a machine's) could never be told apart from an approver's, so no
    // request is made for it: nothing would ever be able to approve it.
    if operator_mode == OperatorMode::TwoPerson {
        if let Err(reason) = logweir_core::approval_policy::console_principal(
            "requester",
            &actor.issuer,
            &actor.subject,
        ) {
            actor.audit.set_failure("requester_not_comparable");
            return Err(ApiError::new(
                ProblemCode::PolicyMismatch,
                format!(
                    "Namespace {ns} is bound to approval policy {} (two-person), and {reason}. \
                     Nothing was created.",
                    policy.name
                ),
            ));
        }
    }
    if let Err(reason) =
        logweir_core::approval_policy::check_ticket(policy.mode, request.ticket.as_deref())
    {
        return Err(ApiError::validation(vec![FieldError::new(
            "ticket",
            if request.ticket.is_none() {
                "required"
            } else {
                "invalid"
            },
            reason,
        )]));
    }
    if policy.mode == ApprovalMode::Governed
        && request
            .approval_ref
            .name
            .ends_with(approval::CONFIRMATION_SUFFIX)
    {
        return Err(ApiError::validation(vec![FieldError::new(
            "approvalRef.name",
            "reserved_suffix",
            format!(
                "in a namespace bound to a Governed policy an Approval name may not end in `{}`: \
                 that is where another Restore's console confirmation lives",
                approval::CONFIRMATION_SUFFIX
            ),
        )]));
    }
    Ok(())
}

/// The v2 document an existing object carries, when it is THIS Restore's
/// under THIS policy — the replay test for a create sequence that was
/// interrupted after the console signed.
fn existing_is_ours(
    existing: &Approval,
    restore: &Restore,
    policy: &logweir_core::approval_policy::ApprovalPolicy,
    ticket: Option<Option<&str>>,
    confirmation: Option<Option<&logweir_core::original_name::OriginalNameConfirmation>>,
) -> Option<RestoreAuthorization> {
    let doc = RestoreAuthorization::from_bytes(existing.spec.approval_bytes.as_bytes()).ok()?;
    let ours = existing.spec.subject_ref.kind == SubjectKind::Restore
        && existing.spec.subject_ref.name == restore.name_any()
        && doc.subject.uid == restore.uid().unwrap_or_default()
        && doc.subject.namespace == restore.namespace().unwrap_or_default()
        && doc.plan_hash == logweir_core::ids::sha256_prefixed(restore.spec.plan_bytes.as_bytes())
        && doc.policy.name == policy.name
        && doc.policy.digest == policy.digest()
        && doc.authorization_mode == policy.mode
        // PROD-15.1: and the approval subject this Restore declares.
        && doc.approval_subject.as_deref() == restore_approval_subject(restore).wire()
        // A replay must also carry the ticket it signed; `None` is "any"
        // (the approval route, which reads the confirmation as it is).
        && ticket.is_none_or(|t| doc.ticket.as_deref() == t)
        // OD-10: and the typed names it signed; `None` is "any", as above.
        && confirmation.is_none_or(|c| doc.original_name_confirmation.as_ref() == c);
    ours.then_some(doc)
}

/// **PROD-15.1.** The approval subject a stored Restore needs, from its own
/// declaration (`spec.target.topicNaming.originalName`) — what the console
/// signs into the authorization document and nothing else. The controller
/// holds the declaration to the plan before any Job exists, and refuses an
/// approval whose signed subject is not the plan's.
#[must_use]
pub fn restore_approval_subject(restore: &Restore) -> logweir_core::original_name::ApprovalSubject {
    if restore.spec.target.topic_naming.is_original_name() {
        logweir_core::original_name::ApprovalSubject::OriginalName
    } else {
        logweir_core::original_name::ApprovalSubject::Ordinary
    }
}

/// The audit record's policy identity: `<name>@<snapshot digest>`, or the
/// legacy synthesis's name.
fn policy_identity(effective: &EffectivePolicy) -> String {
    match effective.digest() {
        Some(digest) => format!("{}@{digest}", effective.name()),
        None => effective.name().to_string(),
    }
}

/// **PLAT-19.2's console half**: sign what the namespace's frozen policy
/// requires, store it, and say where the submission goes next.
///
/// * unbound — nothing is signed; today's governed flow (`awaitingApproval`);
/// * Ordinary — the console-signed document IS the Approval the Restore
///   references (`confirmed`);
/// * Governed — the console-signed document is stored as the CONFIRMATION
///   object an approver countersigns (`awaitingApproval`).
///
/// # Errors
///
/// `state_conflict` when an object this Restore did not produce holds the
/// name, or the adapter's failure.
async fn authorize_submission(
    state: &AppState,
    actor: &Actor,
    namespace: &str,
    restore: &Restore,
    approval_name: &str,
    effective: &EffectivePolicy,
    signed: SignedBesideThePlan<'_>,
) -> Result<RestoreRoutingView, ApiError> {
    let SignedBesideThePlan {
        ticket,
        confirmation,
    } = signed;
    actor.audit.note("approvalPolicy", effective.name());
    actor.audit.note("approvalMode", effective.mode().as_str());
    actor.audit.set_policy_digest(&policy_identity(effective));
    let Some(policy) = effective.bound() else {
        return Ok(RestoreRoutingView {
            mode: effective.mode().into(),
            operator_mode: OperatorMode::of(effective).into(),
            policy: effective.name().to_string(),
            policy_digest: None,
            legacy: true,
            state: AuthorizationState::AwaitingApproval,
            approval_name: approval_name.to_string(),
            confirmation_name: None,
            requester: None,
            expires_at: None,
        });
    };
    let key = match state.approval().confirmation_key() {
        Ok(Some(key)) => key,
        // Unreachable: `refuse_before_create` refused before the Restore
        // existed, and a key once loaded is never unloaded.
        Ok(None) => {
            return Err(ApiError::new(
                ProblemCode::InternalError,
                "This console holds no confirmation key for a namespace bound to an approval \
                 policy.",
            ))
        }
        Err(reason) => return Err(ApiError::new(ProblemCode::InternalError, reason)),
    };
    let governed = approval::awaits_approver(policy.mode);
    let target = if governed {
        approval::confirmation_name(approval_name)
    } else {
        approval_name.to_string()
    };
    let view = |doc: &RestoreAuthorization| RestoreRoutingView {
        mode: policy.mode.into(),
        operator_mode: OperatorMode::of(effective).into(),
        policy: policy.name.clone(),
        policy_digest: Some(policy.digest()),
        legacy: false,
        state: if governed {
            AuthorizationState::AwaitingApproval
        } else {
            AuthorizationState::Confirmed
        },
        approval_name: approval_name.to_string(),
        confirmation_name: governed.then(|| target.clone()),
        requester: Some(doc.requester.principal_id()),
        expires_at: Some(doc.expires_at),
    };
    let conflict = || {
        ApiError::new(
            ProblemCode::StateConflict,
            format!(
                "An Approval named {target} already exists in {namespace} and is not this \
                 Restore's console confirmation under policy {}; it is never adopted. Submit \
                 with another approval name.",
                policy.name
            ),
        )
    };
    match state.kube().get::<Approval>(namespace, &target).await {
        Ok(existing) => {
            let doc = existing_is_ours(
                &existing,
                restore,
                policy,
                Some(ticket),
                Some(confirmation.as_ref()),
            )
            .ok_or_else(conflict)?;
            actor.audit.note("requester", &doc.requester.principal_id());
            return Ok(view(&doc));
        }
        Err(KubeFailure::NotFound) => {}
        Err(other) => return Err(other.into_api_error()),
    }

    let doc = approval::document(
        policy,
        namespace,
        &restore.name_any(),
        &restore.uid().unwrap_or_default(),
        &logweir_core::ids::sha256_prefixed(restore.spec.plan_bytes.as_bytes()),
        Requester {
            issuer: actor.issuer.clone(),
            subject: actor.subject.clone(),
        },
        state.now(),
        ticket.map(str::to_string),
        restore_approval_subject(restore),
        confirmation.clone(),
    );
    let bytes = doc.to_bytes();
    let sidecar = key
        .sign(&bytes)
        .map_err(|reason| ApiError::new(ProblemCode::InternalError, reason))?;
    let object = Approval {
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some(target.clone()),
            namespace: Some(namespace.to_string()),
            annotations: Some(
                [
                    (
                        "logweir.dev/approval-mode".to_string(),
                        policy.mode.as_str().to_string(),
                    ),
                    (
                        "logweir.dev/approval-policy".to_string(),
                        policy.name.clone(),
                    ),
                    ("logweir.dev/requester".to_string(), actor.id()),
                ]
                .into_iter()
                .collect(),
            ),
            ..Default::default()
        },
        spec: ApprovalSpec {
            subject_ref: SubjectRef {
                kind: SubjectKind::Restore,
                name: restore.name_any(),
            },
            plan_hash: doc.plan_hash.clone(),
            approval_bytes: String::from_utf8(bytes).map_err(|_| {
                ApiError::new(
                    ProblemCode::InternalError,
                    "The authorization document is not UTF-8.",
                )
            })?,
            sidecar_bytes: serde_json::to_string(&sidecar).map_err(|_| {
                ApiError::new(
                    ProblemCode::InternalError,
                    "The sidecar could not be rendered.",
                )
            })?,
        },
        status: None,
    };
    match state.kube().create(namespace, &object).await {
        Ok(created) => {
            actor.audit.note("requester", &doc.requester.principal_id());
            actor.audit.note(
                "confirmation",
                &format!("{}/{}", namespace, created.name_any()),
            );
            Ok(view(&doc))
        }
        // A CONCURRENT REPLAY won the create: adopt it only if it is ours.
        Err(KubeFailure::AlreadyExists) => {
            let existing = state
                .kube()
                .get::<Approval>(namespace, &target)
                .await
                .map_err(KubeFailure::into_api_error)?;
            let doc = existing_is_ours(
                &existing,
                restore,
                policy,
                Some(ticket),
                Some(confirmation.as_ref()),
            )
            .ok_or_else(conflict)?;
            Ok(view(&doc))
        }
        Err(other) => Err(other.into_api_error()),
    }
}

/// `POST .../restores/{name}/approval` — **a governed approver submits the
/// countersignature** (PLAT-19.2, D0 "Governed approval submission"), or, in
/// an UNBOUND namespace, records today's v1 approval files
/// ([`record_legacy_approval`]). An Ordinary namespace has nothing to approve
/// and answers `policy_mismatch`.
///
/// # What it checks, and what it leaves to the controller
///
/// The route binds the submission to the ONE governed request it is about:
/// the namespace must be bound to a Governed policy, the console's
/// confirmation for this Restore must exist and still name this Restore's
/// UID, plan hash and the CURRENT policy digest, and it must not have
/// expired. Then separation of duties AT THE API (D0: an approver "cannot
/// approve own request when policy requires independence"; an administrator
/// role changes nothing): the submitting actor must not be the requester the
/// console attested. The Approval it creates carries the confirmation's EXACT
/// document bytes and the console's signatures plus the approver's; the
/// controller re-verifies every signature, the approver key's usage and its
/// principal against the requester before any Job exists.
///
/// # Errors
///
/// `forbidden` for self-approval, `policy_mismatch` outside a Governed
/// binding or for a confirmation issued under another policy, `not_found`,
/// `state_conflict`, `validation_failed`.
#[allow(clippy::too_many_lines)]
pub async fn submit_approval(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, name)): ApiPath<(String, String)>,
    uri: Uri,
    body: Body,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::SubmitApproval)?;
    crate::http::parse_query(uri.query(), &[])?;
    let request: SubmitApprovalRequest = read_json(body, MAX_JSON_BODY).await?;
    // NO PRIVATE KEY IS EVER STORED (review L3; D0). The page refuses it
    // first; this is the same rule where the bytes are written. The text is
    // never echoed.
    let keyed: Vec<FieldError> = [
        ("sidecarBytes", Some(request.sidecar_bytes.as_str())),
        ("approvalBytes", request.approval_bytes.as_deref()),
    ]
    .into_iter()
    .filter(|(_, text)| text.is_some_and(approval::carries_private_key))
    .map(|(field, _)| {
        FieldError::new(
            field,
            "private_key",
            "this carries private-key text; paste the signed files `logweir drill` wrote, never \
             a key. Nothing was stored.",
        )
    })
    .collect();
    if !keyed.is_empty() {
        actor.audit.set_failure("private_key_refused");
        return Err(ApiError::validation(keyed));
    }
    if request.sidecar_bytes.len() > MAX_SIDECAR_BYTES {
        return Err(ApiError::validation(vec![FieldError::new(
            "sidecarBytes",
            "too_long",
            format!("the sidecar is at most {MAX_SIDECAR_BYTES} bytes"),
        )]));
    }
    let submitted: logweir_evidence::Sidecar = serde_json::from_str(&request.sidecar_bytes)
        .map_err(|_| {
            ApiError::validation(vec![FieldError::new(
                "sidecarBytes",
                "not_a_sidecar",
                "must be the DSSE sidecar `logweir drill countersign` wrote",
            )])
        })?;
    if request
        .approval_bytes
        .as_ref()
        .is_some_and(|b| b.len() > MAX_SIDECAR_BYTES)
    {
        return Err(ApiError::validation(vec![FieldError::new(
            "approvalBytes",
            "too_long",
            format!("the approval document is at most {MAX_SIDECAR_BYTES} bytes"),
        )]));
    }
    let restore = get_object::<Restore>(&state, &actor, &ns, &name).await?;
    let effective = approval::effective_policies(state.approval(), state.kube())
        .await
        .map_err(KubeFailure::into_api_error)?
        .resolve(&ns);
    actor.audit.note("approvalPolicy", effective.name());
    actor.audit.note("approvalMode", effective.mode().as_str());
    actor.audit.set_policy_digest(&policy_identity(&effective));
    if matches!(effective, EffectivePolicy::Legacy) {
        return record_legacy_approval(&state, &actor, request_id, &ns, &restore, request).await;
    }
    if request.approval_bytes.is_some() {
        return Err(ApiError::validation(vec![FieldError::new(
            "approvalBytes",
            "not_accepted",
            "under an explicit approval policy the console's confirmation document is the one \
             signed; submit only the countersigned sidecar",
        )]));
    }
    let Some(policy) = effective
        .bound()
        .filter(|p| p.mode == ApprovalMode::Governed)
    else {
        return Err(ApiError::new(
            ProblemCode::PolicyMismatch,
            format!(
                "Namespace {ns} is bound to {} ({}), not an explicit Governed policy, so there is \
                 no governed approval to submit here.",
                effective.name(),
                effective.mode()
            ),
        ));
    };
    // PROD-16.2: A TWO-PERSON NAMESPACE TAKES NO PERSONAL-KEY COUNTERSIGNATURE.
    // Its policy snapshot says the console signs the approval, and a request
    // made under one setting is not approved under the other: accepting a
    // countersignature here would keep the personal-key roster as a second,
    // standing way in. The controller and the runner refuse it too.
    if policy.approver_signature == logweir_core::approval_policy::ApproverSignature::Console {
        actor.audit.set_failure("countersignature_not_accepted");
        return Err(ApiError::new(
            ProblemCode::PolicyMismatch,
            format!(
                "Namespace {ns} is bound to {} (two-person): a second person approves in the \
                 console, and a personal-key countersignature is not accepted here. Nothing was \
                 stored.",
                policy.name
            ),
        ));
    }
    let approval_name = restore.spec.approval_ref_name().to_string();
    let confirmation_name = approval::confirmation_name(&approval_name);
    let confirmation = match state.kube().get::<Approval>(&ns, &confirmation_name).await {
        Ok(object) => object,
        Err(KubeFailure::NotFound) => {
            return Err(ApiError::new(
                ProblemCode::NotFound,
                format!(
                    "Restore {name} has no console confirmation {confirmation_name}; only a \
                     request the console confirmed can be approved."
                ),
            ))
        }
        Err(other) => return Err(other.into_api_error()),
    };
    let Some(doc) = existing_is_ours(&confirmation, &restore, policy, None, None) else {
        return Err(ApiError::new(
            ProblemCode::PolicyMismatch,
            format!(
                "The confirmation {confirmation_name} does not name this Restore's UID, plan hash \
                 and the current policy {} ({}); submit the Restore again.",
                policy.name,
                policy.digest()
            ),
        ));
    };
    let requester = doc.requester.principal_id();
    actor.audit.note("requester", &requester);
    actor.audit.note("approverPrincipal", &actor.id());
    if doc.expires_at <= state.now() {
        return Err(ApiError::new(
            ProblemCode::StateConflict,
            format!(
                "The request expired at {}; an expired request authorises nothing. Submit the \
                 Restore again.",
                doc.expires_at.to_rfc3339()
            ),
        ));
    }
    // SEPARATION OF DUTIES AT THE API. The controller makes the same
    // comparison over the approver KEY's principal; this one is over the
    // authenticated actor, and neither replaces the other.
    if policy.require_distinct_principal && actor.id() == requester {
        actor.audit.note("separation", "refused");
        actor.audit.set_failure("self_approval_forbidden");
        return Err(ApiError::new(
            ProblemCode::Forbidden,
            format!(
                "You requested this restore ({requester}); policy {} requires the approver to be \
                 a different principal from the requester, and no role changes that.",
                policy.name
            ),
        ));
    }
    actor.audit.note("separation", "distinct");
    actor.audit.note("expiresAt", &doc.expires_at.to_rfc3339());
    let console: logweir_evidence::Sidecar = serde_json::from_str(&confirmation.spec.sidecar_bytes)
        .map_err(|_| {
            ApiError::new(
                ProblemCode::StateConflict,
                "The console confirmation's sidecar does not parse.",
            )
        })?;
    let merged = approval::merge_countersignature(&console, &submitted).map_err(|reason| {
        ApiError::validation(vec![FieldError::new(
            "sidecarBytes",
            "no_countersignature",
            reason,
        )])
    })?;
    let object = Approval {
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some(approval_name.clone()),
            namespace: Some(ns.clone()),
            annotations: Some(
                [
                    (
                        "logweir.dev/approval-mode".to_string(),
                        policy.mode.as_str().to_string(),
                    ),
                    (
                        "logweir.dev/approval-policy".to_string(),
                        policy.name.clone(),
                    ),
                    ("logweir.dev/requester".to_string(), requester.clone()),
                    ("logweir.dev/approver".to_string(), actor.id()),
                ]
                .into_iter()
                .collect(),
            ),
            ..Default::default()
        },
        spec: ApprovalSpec {
            subject_ref: SubjectRef {
                kind: SubjectKind::Restore,
                name: restore.name_any(),
            },
            plan_hash: doc.plan_hash.clone(),
            // THE CONFIRMATION'S EXACT BYTES — never re-serialised.
            approval_bytes: confirmation.spec.approval_bytes.clone(),
            sidecar_bytes: serde_json::to_string(&merged).map_err(|_| {
                ApiError::new(
                    ProblemCode::InternalError,
                    "The sidecar could not be rendered.",
                )
            })?,
        },
        status: None,
    };
    let (stored, replayed) = match state.kube().create(&ns, &object).await {
        Ok(created) => (created, false),
        Err(KubeFailure::AlreadyExists) => {
            let existing = state
                .kube()
                .get::<Approval>(&ns, &approval_name)
                .await
                .map_err(KubeFailure::into_api_error)?;
            if existing.spec.approval_bytes != object.spec.approval_bytes
                || existing.spec.sidecar_bytes != object.spec.sidecar_bytes
            {
                return Err(ApiError::new(
                    ProblemCode::StateConflict,
                    format!(
                        "An Approval named {approval_name} already exists with other contents; it \
                         is immutable and never replaced."
                    ),
                ));
            }
            (existing, true)
        }
        Err(other) => return Err(other.into_api_error()),
    };
    Ok(json(
        if replayed {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        &ApprovalResponse {
            request_id,
            replayed: Some(replayed),
            item: projection::approval(&stored),
        },
    ))
}

/// The v1 payload type `logweir drill approve` signs (`weirkeeper`'s
/// `controllers::approval::PAYLOAD_TYPE_APPROVAL`).
const PAYLOAD_TYPE_APPROVAL_V1: &str = "application/vnd.logweir.drill-approval+json;version=1.0.0";

/// **Recording today's approval through the console** — an UNBOUND namespace
/// (`legacy-governed-v1`). Found by the live journeys: the approvals page
/// offered the v1 form in console mode and the product API had no route to
/// record it, so an approver had to use kubectl.
///
/// The route records the two files `logweir drill approve` wrote, EXACTLY as
/// they arrived, as the Approval the Restore's `spec.approvalRef` names. It
/// verifies no signature — the Approval controller does, against the
/// namespace's `GovernedApproval` keys, and the runner again — but it refuses
/// early what could never verify for THIS Restore: a sidecar that is not the
/// v1 payload type, and a document whose `plan_hash` is not this Restore's.
/// A v1 document names no requester, so there is no principal to separate
/// from: the authority is the Approver role on the route and the approver
/// key's custody, exactly as it is for `kubectl create`.
async fn record_legacy_approval(
    state: &AppState,
    actor: &Actor,
    request_id: String,
    ns: &str,
    restore: &Restore,
    request: SubmitApprovalRequest,
) -> Result<Response, ApiError> {
    let Some(approval_bytes) = request.approval_bytes else {
        return Err(ApiError::validation(vec![FieldError::new(
            "approvalBytes",
            "required",
            "an unbound namespace records the approval.json `logweir drill approve` wrote, \
             beside its sidecar",
        )]));
    };
    let payload_type = serde_json::from_str::<serde_json::Value>(&request.sidecar_bytes)
        .ok()
        .and_then(|v| {
            v.get("payloadType")
                .and_then(|t| t.as_str())
                .map(str::to_string)
        });
    if payload_type.as_deref() != Some(PAYLOAD_TYPE_APPROVAL_V1) {
        return Err(ApiError::new(
            ProblemCode::PolicyMismatch,
            format!(
                "Namespace {ns} is bound to no approval policy ({}), which records the approval \
                 `logweir drill approve` signs; this sidecar's payload type is {}.",
                logweir_core::approval_policy::LEGACY_GOVERNED_POLICY_NAME,
                payload_type.as_deref().unwrap_or("absent")
            ),
        ));
    }
    let plan_hash = logweir_core::ids::sha256_prefixed(restore.spec.plan_bytes.as_bytes());
    let signed_hash = serde_json::from_str::<serde_json::Value>(&approval_bytes)
        .ok()
        .and_then(|v| {
            v.get("plan_hash")
                .and_then(|h| h.as_str())
                .map(str::to_string)
        });
    if signed_hash.as_deref() != Some(plan_hash.as_str()) {
        return Err(ApiError::validation(vec![FieldError::new(
            "approvalBytes",
            "plan_mismatch",
            format!(
                "the approval document names plan hash {} and Restore {} hashes to {plan_hash}; \
                 sign this Restore's plan",
                signed_hash.as_deref().unwrap_or("none"),
                restore.name_any()
            ),
        )]));
    }
    // PROD-15.1: the signed approval subject must be the one this Restore
    // needs, refused here as the plan hash is — before an Approval exists
    // that the controller would refuse (`ApprovalSubjectMismatch`).
    let signed_subject = serde_json::from_str::<serde_json::Value>(&approval_bytes)
        .ok()
        .and_then(|v| {
            v.get("approval_subject")
                .and_then(|h| h.as_str())
                .map(str::to_string)
        });
    let subject_refusal =
        logweir_core::original_name::ApprovalSubject::from_wire(signed_subject.as_deref())
            .and_then(|signed| {
                logweir_core::original_name::check_approval_subject(
                    restore_approval_subject(restore),
                    signed,
                )
            });
    if let Err(reason) = subject_refusal {
        return Err(ApiError::validation(vec![FieldError::new(
            "approvalBytes",
            "approval_subject_mismatch",
            reason,
        )]));
    }
    let approval_name = restore.spec.approval_ref_name().to_string();
    actor.audit.note("approverPrincipal", &actor.id());
    actor
        .audit
        .note("approval", &format!("{ns}/{approval_name}"));
    let object = Approval {
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some(approval_name.clone()),
            namespace: Some(ns.to_string()),
            annotations: Some(
                [
                    (
                        "logweir.dev/approval-policy".to_string(),
                        logweir_core::approval_policy::LEGACY_GOVERNED_POLICY_NAME.to_string(),
                    ),
                    ("logweir.dev/approver".to_string(), actor.id()),
                ]
                .into_iter()
                .collect(),
            ),
            ..Default::default()
        },
        spec: ApprovalSpec {
            subject_ref: SubjectRef {
                kind: SubjectKind::Restore,
                name: restore.name_any(),
            },
            plan_hash,
            // EXACTLY AS THEY ARRIVED: the controller hashes these bytes.
            approval_bytes,
            sidecar_bytes: request.sidecar_bytes,
        },
        status: None,
    };
    let (stored, replayed) = match state.kube().create(ns, &object).await {
        Ok(created) => (created, false),
        Err(KubeFailure::AlreadyExists) => {
            let existing = state
                .kube()
                .get::<Approval>(ns, &approval_name)
                .await
                .map_err(KubeFailure::into_api_error)?;
            if existing.spec.approval_bytes != object.spec.approval_bytes
                || existing.spec.sidecar_bytes != object.spec.sidecar_bytes
                || existing.spec.subject_ref.name != object.spec.subject_ref.name
            {
                return Err(ApiError::new(
                    ProblemCode::StateConflict,
                    format!(
                        "An Approval named {approval_name} already exists with other contents; it \
                         is immutable and never replaced."
                    ),
                ));
            }
            (existing, true)
        }
        Err(other) => return Err(other.into_api_error()),
    };
    Ok(json(
        if replayed {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        &ApprovalResponse {
            request_id,
            replayed: Some(replayed),
            item: projection::approval(&stored),
        },
    ))
}
