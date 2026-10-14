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
    ApprovalMode, ApprovalRoute, EffectivePolicy, Requester, RestoreAuthorization,
};
use weirkeeper::crds::approval::{Approval, ApprovalSpec, SubjectRef};
use weirkeeper::crds::restore::{
    Restore, RestoreSpec, RestoreTarget, TargetMode, TopicNaming, VerificationCoverage,
};
use weirkeeper::crds::{ArchiveRef, LocalRef};

use super::subject::{GatedPlan, Subject};
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
/// The governed approval submission route identifier.
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

/// **The opaque plan and its approval name**, as every create that carries
/// them checks them (a `Restore`'s and a `Switchover`'s, PROD-04.2): the plan
/// present and at most [`MAX_PLAN_BYTES`], `planHash` well formed and the
/// SHA-256 of exactly those bytes, `approvalRef.name` an object name.
#[must_use]
pub(crate) fn plan_and_approval_errors(
    plan_bytes: &str,
    plan_hash: &str,
    approval_name: &str,
) -> Vec<FieldError> {
    let mut errors = Vec::new();
    if plan_bytes.is_empty() {
        errors.push(FieldError::new(
            "planBytes",
            "required",
            "the plan document is required",
        ));
    } else if plan_bytes.len() > MAX_PLAN_BYTES {
        errors.push(FieldError::new(
            "planBytes",
            "too_long",
            format!("the plan document is at most {MAX_PLAN_BYTES} bytes"),
        ));
    }
    let well_formed_hash = plan_hash.strip_prefix("sha256:").is_some_and(|hex| {
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
    } else if logweir_core::ids::sha256_prefixed(plan_bytes.as_bytes()) != plan_hash {
        errors.push(FieldError::new(
            "planHash",
            "plan_hash_mismatch",
            "is not the SHA-256 of planBytes exactly as submitted",
        ));
    }
    if !validate::is_dns_subdomain(approval_name) {
        errors.push(FieldError::new(
            "approvalRef.name",
            "invalid_name",
            "must be a Kubernetes object name",
        ));
    }
    errors
}

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
        // The identity map is the one mapping an original-name restore has, and it
        // is refused everywhere else.
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
    let mut errors = plan_and_approval_errors(
        &request.plan_bytes,
        &request.plan_hash,
        &request.approval_ref.name,
    );
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
    // `originalName: true` is the one request whose prefix is empty -- a restore
    // under the original topic names, into topics that do not exist -- and only
    // in `newTopic` mode. Every other request keeps its non-empty prefix.
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
    // `logweir_core::spec::target_topic_prefix` reads `target.topic_naming.prefix`
    // for `newTopic` and the PLAN's own `topic_mapping_prefix` for `scratch`,
    // and this route never parses the plan, so in `scratch` mode it does not
    // hold the string the run will map through. A check against
    // `topicNaming.prefix` would be a verdict about a value the runner does not
    // read, so the declaration is defined for `newTopic` and refused by name for
    // `scratch`.
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
    // The record bound bounds a complete verification and nothing
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
            // This route creates a per-run, human-approved Restore. Standing
            // authorizations are withdrawn in v1, so `authorization` is absent
            // here by construction and the CEL rule that refuses both is
            // satisfied.
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
                    // Stored only when true, so every other request stores the object it always
                    // did.
                    original_name: (request.target.topic_naming.original_name == Some(true))
                        .then_some(true),
                },
            },
            deadline_seconds: request.deadline_seconds,
            // The declaration, verbatim. The controller holds it
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
    // A governed request's confirmation lives beside the Approval under
    // `<approvalRef>-confirmation`, and that name must still be a name. The
    // policies as this request sees them, the fresh-install marker applied, are
    // read BEFORE anything is created.
    let policies = approval::effective_policies(state.approval(), state.kube())
        .await
        .map_err(KubeFailure::into_api_error)?;
    let effective = policies.resolve(&ns);
    refuse_typed_confirmation(&effective, &request)?;
    refuse_before_create(
        &state,
        &actor,
        &ns,
        &effective,
        &CreateGate {
            ticket: request.ticket.as_deref(),
            approval_name: &request.approval_ref.name,
            plan: GatedPlan::Restore(&request.plan_bytes),
        },
    )?;
    refuse_taken_approval::<Restore>(
        &state,
        &ns,
        &effective,
        &request.approval_ref.name,
        &super::deterministic_name(&actor, &ns, ROUTE_CREATE, NAME_PREFIX, &key),
        "Restore",
    )
    .await?;
    // Canonical form for the request hash: equivalent RFC 3339 spellings of
    // one instant are one request. `planBytes` is NOT canonicalised.
    request.point_in_time = point_in_time.to_rfc3339_opts(SecondsFormat::AutoSi, true);
    // THE PER-ACTOR CREATE LIMIT, after every refusal that is about the
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
    // ROUTE BY THE FROZEN POLICY. The Restore exists (and has a
    // UID) before anything is signed; a replay of an interrupted request
    // completes the sequence by reading what is already there.
    let authorization = authorize_submission(
        &state,
        &actor,
        &ns,
        &Subject::of_restore(&ns, &created.object),
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

/// **Typed confirmation of an original-name restore.** On a
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
/// change ticket and, on a one-person confirmation of an original-name
/// restore, the typed topic names.
pub(crate) struct SignedBesideThePlan<'a> {
    /// The change ticket.
    pub ticket: Option<&'a str>,
    /// The typed topic names (a `Restore` under the original names only).
    pub confirmation: Option<logweir_core::original_name::OriginalNameConfirmation>,
}

/// What the create gates read of a request, for either kind
/// ([`refuse_before_create`]).
pub(crate) struct CreateGate<'a> {
    /// The change ticket.
    pub ticket: Option<&'a str>,
    /// `approvalRef.name`.
    pub approval_name: &'a str,
    /// The plan, for the two-person scope gate.
    pub plan: GatedPlan<'a>,
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

/// The refusals that must come BEFORE the Restore exists, so a refused
/// submission leaves nothing behind.
///
/// * **Only the modes `localAdmin` may serve**: `confirm` and `strict` are
///   served there -- the confirming principal is
///   `urn:logweir:local-admin#admin`, and whoever can reach this console can
///   confirm, a residual SECURITY.md states -- and `two-person` never is
///   ([`ApprovalRoute::console_may_request`]).
/// * **The console key exists**: under a bound policy the console signs, and
///   the managed key is written by the identity hook after this pod starts.
///   Until it is there, nothing is created.
/// * **The ticket** (D0: "required in Governed, optional in Ordinary"), and
///   none in an unbound namespace, which signs nothing.
/// * **No `approvalRef.name` ending in `-confirmation` under Governed**: it
///   would collide with another Restore's confirmation object.
/// * **A requester a two-person approval can compare**: under a policy whose
///   approval the console signs, the requester is one of the two people every
///   reader tells apart (`logweir_core::approval_policy::console_principal`).
/// * **An `approvalRef.name` whose confirmation is still a name** under a
///   Governed policy (`approval::MAX_GOVERNED_APPROVAL_NAME`).
///
/// THE SAME FUNCTION GATES A `Switchover`'s create (PROD-04.2): only the
/// plan's scope rule differs, by [`GatedPlan`].
///
/// # Errors
///
/// `policy_mismatch` or `validation_failed`.
pub(crate) fn refuse_before_create(
    state: &AppState,
    actor: &Actor,
    ns: &str,
    effective: &EffectivePolicy,
    gate: &CreateGate<'_>,
) -> Result<(), ApiError> {
    let Some(policy) = effective.bound() else {
        if gate.ticket.is_some() {
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
    // THE ROW OF THE APPROVAL TABLE, and what THIS console may do
    // under it. Nothing below reads the policy's mode or its approver
    // signature on its own.
    let route = approval::route_of(policy)?;
    if !route.console_may_request(state.console_kind()) {
        return Err(ApiError::new(
            ProblemCode::PolicyMismatch,
            format!(
                "Namespace {ns} is bound to approval policy {} ({route}), and this console \
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
                    "Namespace {ns} is under approval policy {} ({route}), which the \
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
    // Under a two-person policy the requester is one of the two
    // people every reader compares. An identity that cannot be compared (or
    // is a machine's) could never be told apart from an approver's, so no
    // request is made for it: nothing would ever be able to approve it.
    if route == logweir_core::approval_policy::ApprovalRoute::SecondPersonInConsole {
        if let Err(reason) = logweir_core::approval_policy::console_principal(
            logweir_core::approval_policy::Party::Requester,
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
        // NOTHING IS REQUESTED HERE THAT A SECOND PERSON COULD NOT BE SHOWN IN
        // FULL. The approver sees every topic and the name it is restored under,
        // the source, the target cluster, the window and the coverage, from this
        // plan; a plan too large for that, unreadable, or carrying a value a page
        // cannot show faithfully would wait for an approval nobody is ever offered.
        // So the largest restore this mode accepts is the largest one it can show
        // (`logweir_core::approval_scope::MAX_SCOPE_TOPICS`); a larger one is split,
        // or approved under a strict policy. The ticket is shown to the approver by
        // the same rule, so a ticket the scope would refuse is refused here, by
        // name, without echoing it.
        if let Some(ticket) = gate.ticket {
            if !logweir_core::approval_scope::showable(ticket) {
                actor.audit.set_failure("ticket_not_showable");
                return Err(ApiError::validation(vec![FieldError::new(
                    "ticket",
                    "not_showable",
                    format!(
                        "Namespace {ns} is bound to approval policy {} (two-person), and the \
                         approver is shown the change ticket: it must be printable ASCII (no \
                         character outside ASCII, no control character) of at most {} \
                         characters. Nothing was created.",
                        policy.name,
                        logweir_core::approval_scope::MAX_SCOPE_TEXT_CHARS
                    ),
                )]));
            }
        }
        if let Err(incomplete) = gate.plan.scope() {
            actor.audit.set_failure("scope_incomplete");
            actor
                .audit
                .note("scope", &format!("incomplete:{}", incomplete.code()));
            return Err(ApiError::validation(vec![FieldError::new(
                "planBytes",
                "scope_incomplete",
                format!(
                    "Namespace {ns} is bound to approval policy {} (two-person). {}. Nothing was \
                     created.",
                    policy.name,
                    incomplete.sentence_for(gate.plan.scope_of())
                ),
            )]));
        }
    }
    if let Err(reason) = logweir_core::approval_policy::check_ticket(policy.mode, gate.ticket) {
        return Err(ApiError::validation(vec![FieldError::new(
            "ticket",
            if gate.ticket.is_none() {
                "required"
            } else {
                "invalid"
            },
            reason,
        )]));
    }
    if policy.mode == ApprovalMode::Governed
        && gate.approval_name.ends_with(approval::CONFIRMATION_SUFFIX)
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
    if policy.mode == ApprovalMode::Governed
        && gate.approval_name.len() > approval::MAX_GOVERNED_APPROVAL_NAME
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
    Ok(())
}

/// Where the console stores its signed document for a request naming
/// `approval_name` under `policy`: the Approval itself under `confirm`, its
/// `-confirmation` object under a policy an approver countersigns.
fn approval_target(
    policy: &logweir_core::approval_policy::ApprovalPolicy,
    approval_name: &str,
) -> Result<String, ApiError> {
    Ok(if approval::route_of(policy)?.awaits_an_approver() {
        approval::confirmation_name(approval_name)
    } else {
        approval_name.to_string()
    })
}

/// **A create refused `state_conflict` leaves nothing behind** (theme A's
/// PoC defect D3). Under a bound policy the console stores its signed
/// document at [`approval_target`], and an Approval already there that this
/// request did not produce is never adopted ([`authorize_submission`]). That
/// was decided only AFTER the subject existed, so a refused request left a
/// `Restore` (or a `Switchover`) behind, which then failed
/// `ApprovalSubjectMismatch`. It is decided here, before anything is
/// created: an Approval under the name can be this request's own only when
/// the subject it was made for already exists under this request's own name
/// `subject_name` (an interrupted earlier attempt, which the create replays
/// and [`authorize_submission`] then judges, creating nothing). With no such
/// subject the Approval binds another object's UID, never the UID a create
/// would mint, so the request is refused and nothing is created.
///
/// The residual, stated: an Approval created under the name between this
/// read and [`authorize_submission`]'s is still refused after the subject
/// exists; the answer then says so.
///
/// # Errors
///
/// `state_conflict`, `policy_mismatch`, or the adapter's failure.
pub(crate) async fn refuse_taken_approval<K: crate::kube::ProductResource>(
    state: &AppState,
    ns: &str,
    effective: &EffectivePolicy,
    approval_name: &str,
    subject_name: &str,
    title: &str,
) -> Result<(), ApiError> {
    let Some(policy) = effective.bound() else {
        return Ok(());
    };
    let target = approval_target(policy, approval_name)?;
    match state.kube().get::<Approval>(ns, &target).await {
        Err(KubeFailure::NotFound) => return Ok(()),
        Err(other) => return Err(other.into_api_error()),
        Ok(_) => {}
    }
    match state.kube().get::<K>(ns, subject_name).await {
        Ok(_) => Ok(()),
        Err(KubeFailure::NotFound) => Err(ApiError::new(
            ProblemCode::StateConflict,
            format!(
                "An Approval named {target} already exists in {ns} and was not made for this \
                 request: it binds another object, and it is never adopted. Nothing was \
                 created. Submit this {title} with another approval name."
            ),
        )),
        Err(other) => Err(other.into_api_error()),
    }
}

/// The v2 document an existing object carries, when it is THIS subject's
/// under THIS policy — the replay test for a create sequence that was
/// interrupted after the console signed.
fn existing_is_ours(
    existing: &Approval,
    subject: &Subject,
    policy: &logweir_core::approval_policy::ApprovalPolicy,
    ticket: Option<Option<&str>>,
    confirmation: Option<Option<&logweir_core::original_name::OriginalNameConfirmation>>,
) -> Option<RestoreAuthorization> {
    let doc = RestoreAuthorization::from_bytes(existing.spec.approval_bytes.as_bytes()).ok()?;
    let ours = existing.spec.subject_ref.kind == subject.crd_kind()
        && existing.spec.subject_ref.name == subject.name
        && doc.subject.kind == subject.wire()
        && doc.subject.uid == subject.uid
        && doc.subject.namespace == subject.namespace
        && doc.plan_hash == subject.plan_hash()
        && doc.policy.name == policy.name
        && doc.policy.digest == policy.digest()
        && doc.authorization_mode == policy.mode
        // And the approval subject this object declares.
        && doc.approval_subject.as_deref() == subject.approval_subject.wire()
        // A replay must also carry the ticket it signed; `None` is "any"
        // (the approval route, which reads the confirmation as it is).
        && ticket.is_none_or(|t| doc.ticket.as_deref() == t)
        // And the typed names it signed; `None` is "any", as above.
        && confirmation.is_none_or(|c| doc.original_name_confirmation.as_ref() == c);
    ours.then_some(doc)
}

/// The approval subject a stored Restore needs, from its own
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

/// **The console's half of policy routing**: sign what the namespace's
/// frozen policy requires, store it, and say where the submission goes next.
///
/// * unbound — nothing is signed; today's governed flow (`awaitingApproval`);
/// * Ordinary — the console-signed document IS the Approval the Restore
///   references (`confirmed`);
/// * Governed — the console-signed document is stored as the CONFIRMATION
///   object an approver countersigns (`awaitingApproval`).
///
/// The same function routes a `Switchover`'s submission (PROD-04.2); the
/// document binds the subject's kind, which every verifier compares.
///
/// # Errors
///
/// `state_conflict` when an object this subject did not produce holds the
/// name, or the adapter's failure.
pub(crate) async fn authorize_submission(
    state: &AppState,
    actor: &Actor,
    namespace: &str,
    subject: &Subject,
    effective: &EffectivePolicy,
    signed: SignedBesideThePlan<'_>,
) -> Result<RestoreRoutingView, ApiError> {
    let SignedBesideThePlan {
        ticket,
        confirmation,
    } = signed;
    let approval_name = subject.approval_name.as_str();
    actor.audit.note("approvalPolicy", effective.name());
    actor.audit.note("approvalMode", effective.mode().as_str());
    actor.audit.set_policy_digest(&policy_identity(effective));
    let Some(policy) = effective.bound() else {
        return Ok(RestoreRoutingView {
            mode: effective.mode().into(),
            operator_mode: ApprovalRoute::of(effective),
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
    // By the table: `confirm` is authorised by the console's signature
    // alone; `two-person` and `strict` leave a REQUEST and wait.
    let governed = approval::route_of(policy)?.awaits_an_approver();
    let target = approval_target(policy, approval_name)?;
    let view = |doc: &RestoreAuthorization| RestoreRoutingView {
        mode: policy.mode.into(),
        operator_mode: ApprovalRoute::of(effective),
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
            // `refuse_taken_approval` refused a taken name before the subject
            // existed; reaching this means the subject is an earlier attempt's,
            // or the name was taken between the two reads.
            format!(
                "An Approval named {target} already exists in {namespace} and is not this \
                 {}'s console confirmation under policy {}; it is never adopted. {} {} existed \
                 before this answer (an earlier attempt of this request, or the name was taken \
                 concurrently) and will wait for an approval: submit again with another approval \
                 name.",
                subject.title(),
                policy.name,
                subject.title(),
                subject.name
            ),
        )
    };
    match state.kube().get::<Approval>(namespace, &target).await {
        Ok(existing) => {
            let doc = existing_is_ours(
                &existing,
                subject,
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

    let mut doc = approval::document(
        policy,
        namespace,
        &subject.name,
        &subject.uid,
        &subject.plan_hash(),
        Requester {
            issuer: actor.issuer.clone(),
            subject: actor.subject.clone(),
        },
        state.now(),
        ticket.map(str::to_string),
        subject.approval_subject,
        confirmation.clone(),
    );
    // THE KIND THE DOCUMENT BINDS, which every verifier compares: a
    // Restore's authorization never authorises a Switchover and back.
    doc.subject.kind = subject.wire().to_string();
    let bytes = doc.to_bytes();
    let sidecar = key
        .sign(&bytes)
        .map_err(|reason| ApiError::new(ProblemCode::InternalError, reason))?;
    // DUI-21: who asked, in a document THIS console signs and binds to this
    // request (approval name, requester, plan hash): the provider-verified
    // email, and a name only when the sign-in took the display name from the
    // configured name claim (never a cut email or subject, DUI review L3).
    // Beside the signed request, never in it; served only if the signature
    // verifies.
    let configured_name = actor.display_name_claimed;
    let display = approval::requester_display(
        key,
        approval_name,
        &doc.requester.principal_id(),
        &doc.plan_hash,
        actor.verified_email.as_deref(),
        configured_name.then_some(actor.display_name.as_str()),
    )
    .map_err(|reason| ApiError::new(ProblemCode::InternalError, reason))?
    .map(|(document, signature)| {
        [
            (approval::REQUESTER_DISPLAY_ANNOTATION.to_string(), document),
            (
                approval::REQUESTER_DISPLAY_SIGNATURE_ANNOTATION.to_string(),
                signature,
            ),
        ]
    })
    .into_iter()
    .flatten();
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
                    (approval::REQUESTER_ANNOTATION.to_string(), actor.id()),
                ]
                .into_iter()
                .chain(display)
                .collect(),
            ),
            ..Default::default()
        },
        spec: ApprovalSpec {
            subject_ref: SubjectRef {
                kind: subject.crd_kind(),
                name: subject.name.clone(),
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
                subject,
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
/// countersignature** (D0 "Governed approval submission"), or, in
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
    let submitted = submitted_files(&actor, &request)?;
    let restore = get_object::<Restore>(&state, &actor, &ns, &name).await?;
    submit_for(
        &state,
        &actor,
        request_id,
        &ns,
        &Subject::of_restore(&ns, &restore),
        request,
        submitted,
    )
    .await
}

/// **What an approval submission's files must be before anything is read**,
/// for either kind: no private-key text, a sidecar of at most
/// [`MAX_SIDECAR_BYTES`] that parses as a DSSE sidecar, an approval document
/// of at most the same. Returns the parsed sidecar.
///
/// # Errors
///
/// `validation_failed`, naming the field; the text is never echoed.
pub(crate) fn submitted_files(
    actor: &Actor,
    request: &SubmitApprovalRequest,
) -> Result<logweir_evidence::Sidecar, ApiError> {
    // NO PRIVATE KEY IS EVER STORED (D0). The page refuses it
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
    Ok(submitted)
}

/// The submission, once its files passed [`submitted_files`] and the subject
/// was read: by the namespace's policy, a v1 record (unbound) or a strict
/// countersignature over the console's request. A `Switchover`'s submission
/// is this same function (PROD-04.2).
///
/// # Errors
///
/// See [`submit_approval`].
#[allow(clippy::too_many_lines)]
pub(crate) async fn submit_for(
    state: &AppState,
    actor: &Actor,
    request_id: String,
    ns: &str,
    subject: &Subject,
    request: SubmitApprovalRequest,
    submitted: logweir_evidence::Sidecar,
) -> Result<Response, ApiError> {
    let name = subject.name.as_str();
    let effective = approval::effective_policies(state.approval(), state.kube())
        .await
        .map_err(KubeFailure::into_api_error)?
        .resolve(ns);
    actor.audit.note("approvalPolicy", effective.name());
    actor.audit.note("approvalMode", effective.mode().as_str());
    actor.audit.set_policy_digest(&policy_identity(&effective));
    if matches!(effective, EffectivePolicy::Legacy) {
        return record_legacy_approval(state, actor, request_id, ns, subject, request, &submitted)
            .await;
    }
    if request.approval_bytes.is_some() {
        return Err(ApiError::validation(vec![FieldError::new(
            "approvalBytes",
            "not_accepted",
            "under an explicit approval policy the console's confirmation document is the one \
             signed; submit only the countersigned sidecar",
        )]));
    }
    let not_governed = || {
        ApiError::new(
            ProblemCode::PolicyMismatch,
            format!(
                "Namespace {ns} is bound to {} ({}), not an explicit Governed policy, so there is \
                 no governed approval to submit here.",
                effective.name(),
                effective.mode()
            ),
        )
    };
    let Some(policy) = effective.bound() else {
        return Err(not_governed());
    };
    // BY THE TABLE: only the personal-key row takes a
    // countersignature.
    match approval::route_of(policy)? {
        logweir_core::approval_policy::ApprovalRoute::PersonalKey => {}
        logweir_core::approval_policy::ApprovalRoute::RequesterConfirms => {
            return Err(not_governed())
        }
        // A TWO-PERSON NAMESPACE TAKES NO PERSONAL-KEY COUNTERSIGNATURE. Its
        // policy snapshot says the console signs the approval, and a request
        // made under one setting is not approved under the other: accepting a
        // countersignature here would keep the personal-key roster as a
        // second, standing way in. The controller and the runner refuse it
        // too.
        logweir_core::approval_policy::ApprovalRoute::SecondPersonInConsole => {
            actor.audit.set_failure("countersignature_not_accepted");
            return Err(ApiError::new(
                ProblemCode::PolicyMismatch,
                format!(
                    "Namespace {ns} is bound to {} (two-person): a second person approves in \
                     the console, and a personal-key countersignature is not accepted here. \
                     Nothing was stored.",
                    policy.name
                ),
            ));
        }
    }
    let approval_name = subject.approval_name.clone();
    let confirmation_name = approval::confirmation_name(&approval_name);
    let confirmation = match state.kube().get::<Approval>(ns, &confirmation_name).await {
        Ok(object) => object,
        Err(KubeFailure::NotFound) => {
            return Err(ApiError::new(
                ProblemCode::NotFound,
                format!(
                    "{} {name} has no console confirmation {confirmation_name}; only a request \
                     the console confirmed can be approved.",
                    subject.title()
                ),
            ))
        }
        Err(other) => return Err(other.into_api_error()),
    };
    let Some(doc) = existing_is_ours(&confirmation, subject, policy, None, None) else {
        return Err(ApiError::new(
            ProblemCode::PolicyMismatch,
            format!(
                "The confirmation {confirmation_name} does not name this {}'s UID, plan hash and \
                 the current policy {} ({}); submit the {} again.",
                subject.title(),
                policy.name,
                policy.digest(),
                subject.title()
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
                 {} again.",
                doc.expires_at.to_rfc3339(),
                subject.title()
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
                "You requested this {} ({requester}); policy {} requires the approver to be a \
                 different principal from the requester, and no role changes that.",
                subject.noun(),
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
            namespace: Some(ns.to_string()),
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
                kind: subject.crd_kind(),
                name: subject.name.clone(),
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

/// **Recording today's approval through the console** -- an UNBOUND
/// namespace (`legacy-governed-v1`), so an approver need not use kubectl.
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
    subject: &Subject,
    request: SubmitApprovalRequest,
    submitted: &logweir_evidence::Sidecar,
) -> Result<Response, ApiError> {
    let Some(approval_bytes) = request.approval_bytes else {
        return Err(ApiError::validation(vec![FieldError::new(
            "approvalBytes",
            "required",
            "an unbound namespace records the approval.json `logweir drill approve` wrote, \
             beside its sidecar",
        )]));
    };
    // The sidecar parsed in `submitted_files`; its payload type is the v1's.
    if submitted.payload_type != PAYLOAD_TYPE_APPROVAL_V1 {
        return Err(ApiError::new(
            ProblemCode::PolicyMismatch,
            format!(
                "Namespace {ns} is bound to no approval policy ({}), which records the approval \
                 `logweir drill approve` signs; this sidecar's payload type is {}.",
                logweir_core::approval_policy::LEGACY_GOVERNED_POLICY_NAME,
                submitted.payload_type
            ),
        ));
    }
    // THE DOCUMENT, PARSED ONCE: one that does not parse is refused by name,
    // never read as "no plan hash".
    let Ok(signed) =
        serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&approval_bytes)
    else {
        return Err(ApiError::validation(vec![FieldError::new(
            "approvalBytes",
            "not_an_approval",
            "must be the approval.json `logweir drill approve` wrote (a JSON object)",
        )]));
    };
    let text = |key: &str| signed.get(key).and_then(serde_json::Value::as_str);
    let plan_hash = subject.plan_hash();
    if text("plan_hash") != Some(plan_hash.as_str()) {
        return Err(ApiError::validation(vec![FieldError::new(
            "approvalBytes",
            "plan_mismatch",
            format!(
                "the approval document names plan hash {} and {} {} hashes to {plan_hash}; sign \
                 this {}'s plan",
                text("plan_hash").unwrap_or("none"),
                subject.title(),
                subject.name,
                subject.title()
            ),
        )]));
    }
    // THE KIND IT BINDS, refused here as the plan hash is: a v1 document
    // without the field is a Restore's (`ApprovalDoc`'s compatibility
    // default); the controller refuses any other mismatch
    // (`SubjectKindMismatch`), and nothing is stored that it would.
    let kind = match signed.get("subject_kind") {
        None => Some(logweir_core::approval_policy::SUBJECT_KIND_RESTORE),
        Some(value) => value.as_str(),
    };
    if kind != Some(subject.wire()) {
        return Err(ApiError::validation(vec![FieldError::new(
            "approvalBytes",
            "subject_kind_mismatch",
            format!(
                "the approval document does not authorise a {}; sign it with `logweir drill \
                 approve --subject-kind {}`",
                subject.title(),
                subject.title()
            ),
        )]));
    }
    // The signed approval subject must be the one this object
    // needs, refused here as the plan hash is — before an Approval exists
    // that the controller would refuse (`ApprovalSubjectMismatch`).
    let subject_refusal = logweir_core::original_name::ApprovalSubject::from_wire(text(
        "approval_subject",
    ))
    .and_then(|signed| {
        logweir_core::original_name::check_approval_subject(subject.approval_subject, signed)
    });
    if let Err(reason) = subject_refusal {
        return Err(ApiError::validation(vec![FieldError::new(
            "approvalBytes",
            "approval_subject_mismatch",
            reason,
        )]));
    }
    let approval_name = subject.approval_name.clone();
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
                kind: subject.crd_kind(),
                name: subject.name.clone(),
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
