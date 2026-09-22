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
    ApprovalMode, EffectivePolicy, Requester, RestoreAuthorization,
};
use weirkeeper::crds::approval::{Approval, ApprovalSpec, SubjectKind, SubjectRef};
use weirkeeper::crds::restore::{Restore, RestoreSpec, RestoreTarget, TargetMode, TopicNaming};
use weirkeeper::crds::{ArchiveRef, LocalRef};

use super::{authorize, create_idempotent, get_object, json, list_page, list_query, ApiPath};
use crate::app::AppState;
use crate::approval;
use crate::auth::Actor;
use crate::authz::Action;
use crate::contract::{
    ApprovalResponse, AuthorizationState, CreateRestoreRequest, RestoreList, RestoreMode,
    RestoreResponse, RestoreRoutingView, SubmitApprovalRequest, TopicMappingRow,
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
    let effective = state.approval().policies.resolve(&ns);
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

/// The v2 document an existing object carries, when it is THIS Restore's
/// under THIS policy — the replay test for a create sequence that was
/// interrupted after the console signed.
fn existing_is_ours(
    existing: &Approval,
    restore: &Restore,
    policy: &logweir_core::approval_policy::ApprovalPolicy,
) -> Option<RestoreAuthorization> {
    let doc = RestoreAuthorization::from_bytes(existing.spec.approval_bytes.as_bytes()).ok()?;
    let ours = existing.spec.subject_ref.kind == SubjectKind::Restore
        && existing.spec.subject_ref.name == restore.name_any()
        && doc.subject.uid == restore.uid().unwrap_or_default()
        && doc.subject.namespace == restore.namespace().unwrap_or_default()
        && doc.plan_hash == logweir_core::ids::sha256_prefixed(restore.spec.plan_bytes.as_bytes())
        && doc.policy.name == policy.name
        && doc.policy.digest == policy.digest()
        && doc.authorization_mode == policy.mode;
    ours.then_some(doc)
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
) -> Result<RestoreRoutingView, ApiError> {
    actor.audit.note("approvalPolicy", effective.name());
    actor.audit.note("approvalMode", effective.mode().as_str());
    let Some(policy) = effective.bound() else {
        return Ok(RestoreRoutingView {
            mode: effective.mode().into(),
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
    let Some(key) = state.approval().confirmation.as_ref() else {
        // Unreachable: startup refuses a bound served namespace without a key.
        return Err(ApiError::new(
            ProblemCode::InternalError,
            "This console holds no confirmation key for a namespace bound to an approval policy.",
        ));
    };
    let governed = approval::awaits_approver(policy.mode);
    let target = if governed {
        approval::confirmation_name(approval_name)
    } else {
        approval_name.to_string()
    };
    let view = |doc: &RestoreAuthorization| RestoreRoutingView {
        mode: policy.mode.into(),
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
            let doc = existing_is_ours(&existing, restore, policy).ok_or_else(conflict)?;
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
        None,
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
            let doc = existing_is_ours(&existing, restore, policy).ok_or_else(conflict)?;
            Ok(view(&doc))
        }
        Err(other) => Err(other.into_api_error()),
    }
}

/// `POST .../restores/{name}/approval` — **a governed approver submits the
/// countersignature** (PLAT-19.2, D0 "Governed approval submission").
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
    let restore = get_object::<Restore>(&state, &actor, &ns, &name).await?;
    let effective = state.approval().policies.resolve(&ns);
    actor.audit.note("approvalPolicy", effective.name());
    actor.audit.note("approvalMode", effective.mode().as_str());
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
    let Some(doc) = existing_is_ours(&confirmation, &restore, policy) else {
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
