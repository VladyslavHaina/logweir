//! Preflights: operation readiness and restore preflight (PLAT-03).
//!
//! # A result is a result ABOUT something
//!
//! Every preflight carries a `binding` — the plan hash and the digest over the
//! objects it resolved — and [`project`] recomputes `applicable` and `stale`
//! on EVERY read against the caller's current draft. Edit the plan and the
//! hash changes; choose another target, destination or recovery point and a
//! referent UID changes; let it sit and it expires. A cached verdict rendered
//! as current readiness is the defect PLAT-03 exists to remove, so this module
//! never serves one without saying so.
//!
//! # Ready authorizes nothing
//!
//! The aggregate is advisory. Execution-time guards remain the authority, and
//! `executionOnly` names in the response the checks that cannot be answered
//! before the run, so "ready" is never read as "every permission is verified".
//!
//! # Plan bytes are forwarded verbatim
//!
//! `planBytes` is opaque: this service checks `planHash` against the SHA-256 of
//! exactly those bytes and stores them unchanged. It never parses, reformats
//! or re-emits a plan document.

use axum::body::Body;
use axum::extract::State;
use axum::response::Response;
use chrono::{DateTime, Utc};
use http::{HeaderMap, StatusCode, Uri};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::Resource as _;
use logweir_core::destination::DestinationRole;
use std::collections::BTreeMap;
use weirkeeper::crds::backup_destination::BackupDestination;
use weirkeeper::crds::preflight::{
    BackupPreflightRequest as CrdBackupRequest, DestinationAccessRequest, Preflight as PreflightCr,
    PreflightOperation, PreflightRequest, PreflightSpec,
    RestorePreflightRequest as CrdRestoreRequest, UidRef,
};
use weirkeeper::crds::{ArchiveRef, LocalRef};

use super::{
    authorize, cancel_check, check_create_rate, check_name, create_idempotent, get_object, json,
    ApiPath, Created, DEFAULT_LIMIT, MAX_LIMIT, PREFLIGHT_CREATES_PER_MINUTE,
};
use crate::app::AppState;
use crate::auth::Actor;
use crate::authz::{Action, Role};
use crate::contract::{
    CancelResponse, CheckEntryView, CheckGating, CheckOperation, CheckOperationKind,
    CheckOperationResponse, CheckScopeView, CheckVerdict, CreatePreflightRequest,
    DestinationRoleDto, DetailEntryView, DetailPageResponse, ExecutionOnlyView, Page, Preflight,
    PreflightBindingView, PreflightOperationDto, PreflightResponse, PreflightState, ReferentView,
};
use crate::cursor::{self, CursorError, CursorScope};
use crate::http::{read_json, RequestId, MAX_JSON_BODY};
use crate::idempotency::IdempotencyKey;
use crate::problem::{ApiError, FieldError, ProblemCode};
use crate::status::{condition_view, MAX_CONDITIONS};
use crate::validate;

/// The create route identifier (idempotency scope).
pub const ROUTE_CREATE: &str = "POST /api/v1/namespaces/{ns}/preflights";
/// The details route identifier (cursor scope).
pub const ROUTE_DETAILS: &str = "GET /api/v1/namespaces/{ns}/preflights/{id}/details";
/// The cancel route identifier.
pub const ROUTE_CANCEL: &str = "POST /api/v1/namespaces/{ns}/preflights/{id}:cancel";
/// The destination-test route's idempotency scope.
pub const ROUTE_DESTINATION_TEST: &str = "POST /api/v1/namespaces/{ns}/destinations/{name}:test";
/// The deterministic name prefix (D2 §6.2).
pub const NAME_PREFIX: &str = "pf-";
/// The `:cancel` command suffix.
pub const CANCEL: &str = ":cancel";

/// The largest plan document accepted, in bytes (D2 §6.2).
pub const MAX_PLAN_BYTES: usize = 256 * 1024;
/// The most detail entries one page carries.
pub const MAX_DETAIL_PAGE: u32 = MAX_LIMIT;
/// The one data key inside a details document.
pub const DETAILS_KEY: &str = "details.jsonl";
/// The annotation carrying a result document's own digest.
pub const SHA256_ANNOTATION: &str = "logweir.dev/result-sha256";

// ======================================================================
// Projection
// ======================================================================

fn operation_dto(operation: PreflightOperation) -> PreflightOperationDto {
    match operation {
        PreflightOperation::Backup => PreflightOperationDto::Backup,
        PreflightOperation::Restore => PreflightOperationDto::Restore,
        PreflightOperation::DestinationAccess => PreflightOperationDto::DestinationAccess,
    }
}

fn verdict(state: &str) -> CheckVerdict {
    match state {
        "ready" => CheckVerdict::Ready,
        "notReady" => CheckVerdict::NotReady,
        "skipped" => CheckVerdict::Skipped,
        _ => CheckVerdict::Unknown,
    }
}

fn gating(value: Option<&String>) -> Option<CheckGating> {
    match value.map(String::as_str) {
        Some("blocking") => Some(CheckGating::Blocking),
        Some("advisory") => Some(CheckGating::Advisory),
        Some("executionOnly") => Some(CheckGating::ExecutionOnly),
        _ => None,
    }
}

fn entry_view(entry: &weirkeeper::crds::preflight::CheckEntry) -> CheckEntryView {
    CheckEntryView {
        id: entry.id.clone(),
        category: entry.category.clone(),
        scope: entry.scope.as_ref().map(|s| CheckScopeView {
            kind: s.kind.clone(),
            name: s.name.clone(),
            uid: s.uid.clone(),
        }),
        state: verdict(&entry.state),
        gating: gating(entry.gating.as_ref()),
        authority: entry.authority.clone(),
        code: entry.code.clone(),
        message: entry.message.as_ref().map(|m| validate::bounded(m, 512)),
        remedy: entry.remedy.as_ref().map(|m| validate::bounded(m, 512)),
        observed_at: entry.observed_at.as_ref().copied(),
        expires_at: entry.expires_at.as_ref().copied(),
    }
}

/// The aggregate: the controller's phase, and — once complete — the result
/// state it recorded.
fn state_of(object: &PreflightCr) -> PreflightState {
    let status = object.status.as_ref();
    let phase = status.and_then(|s| s.phase.as_deref()).unwrap_or("Pending");
    match phase {
        "Pending" => PreflightState::Pending,
        "Queued" => PreflightState::Queued,
        "Running" => PreflightState::Running,
        "Cancelled" => PreflightState::Cancelled,
        "Failed" => PreflightState::Failed,
        "Completed" => match status
            .and_then(|s| s.result.as_ref())
            .map(|r| r.state.as_str())
        {
            Some("ready") => PreflightState::Ready,
            Some("notReady") => PreflightState::NotReady,
            // COMPLETED WITH NO RECORDED AGGREGATE IS `unknown`, NOT `ready`.
            _ => PreflightState::Unknown,
        },
        // A phase this build does not recognise is never optimistic.
        _ => PreflightState::Unknown,
    }
}

const fn is_terminal(state: PreflightState) -> bool {
    matches!(
        state,
        PreflightState::Ready
            | PreflightState::NotReady
            | PreflightState::Unknown
            | PreflightState::Failed
            | PreflightState::Cancelled
    )
}

/// D2 §6.6's applicability, recomputed per read.
///
/// `draft_plan_hash` is the `?planHash=` the caller sent: the hash of the plan
/// they are looking at RIGHT NOW. A result bound to a different hash is a
/// result about a plan that no longer exists.
fn staleness(
    object: &PreflightCr,
    state: PreflightState,
    now: DateTime<Utc>,
    draft_plan_hash: Option<&str>,
) -> (bool, Vec<String>) {
    let mut reasons = Vec::new();
    let status = object.status.as_ref();
    let result = status.and_then(|s| s.result.as_ref());
    if let Some(expires) = result.and_then(|r| r.expires_at.as_ref()) {
        if now >= *expires {
            reasons.push("expired".to_string());
        }
    }
    let bound = status
        .and_then(|s| s.binding.as_ref())
        .and_then(|b| b.plan_hash.as_deref());
    if let (Some(draft), Some(bound)) = (draft_plan_hash, bound) {
        if draft != bound {
            reasons.push("planHashChanged".to_string());
        }
    }
    if draft_plan_hash.is_some() && bound.is_none() {
        reasons.push("planHashChanged".to_string());
    }
    if object.spec.cancel_requested && !is_terminal(state) {
        reasons.push("cancelRequested".to_string());
    }
    // A RESULT THAT DOES NOT EXIST IS NOT FRESH. Anything before `Completed`
    // has no verdict to be applicable, and saying "stale" there would be
    // wrong; `applicable` below is what carries that.
    let stale = !reasons.is_empty();
    (stale, reasons)
}

/// A `Preflight` as the product DTO.
#[must_use]
pub fn project(
    object: &PreflightCr,
    now: DateTime<Utc>,
    draft_plan_hash: Option<&str>,
) -> Preflight {
    let meta = object.meta();
    let status = object.status.as_ref();
    let result = status.and_then(|s| s.result.as_ref());
    let state = state_of(object);
    let (stale, stale_reasons) = staleness(object, state, now, draft_plan_hash);
    let entries: Vec<CheckEntryView> = result
        .and_then(|r| r.checks.as_ref())
        .map(|c| c.iter().map(entry_view).collect())
        .unwrap_or_default();
    let (warnings, checks): (Vec<CheckEntryView>, Vec<CheckEntryView>) =
        entries.iter().cloned().partition(|e| {
            e.gating == Some(CheckGating::Advisory) && e.state == CheckVerdict::NotReady
        });
    let execution_only: Vec<ExecutionOnlyView> = entries
        .iter()
        .filter(|e| e.gating == Some(CheckGating::ExecutionOnly))
        .map(|e| ExecutionOnlyView {
            id: e.id.clone(),
            note: e.message.clone().unwrap_or_else(|| {
                "This permission is verified only when the run executes.".to_string()
            }),
        })
        .collect();
    let completed = matches!(
        state,
        PreflightState::Ready | PreflightState::NotReady | PreflightState::Unknown
    );
    Preflight {
        id: meta.name.clone().unwrap_or_default(),
        namespace: meta.namespace.clone().unwrap_or_default(),
        uid: meta.uid.clone().unwrap_or_default(),
        resource_version: meta.resource_version.clone().unwrap_or_default(),
        created_at: meta.creation_timestamp.as_ref().map(|t| t.0),
        operation: operation_dto(object.spec.request.operation),
        state,
        reason: status.and_then(|s| s.reason.clone()),
        terminal: is_terminal(state),
        binding: PreflightBindingView {
            plan_hash: status
                .and_then(|s| s.binding.as_ref())
                .and_then(|b| b.plan_hash.clone()),
            inputs_digest: status
                .and_then(|s| s.binding.as_ref())
                .and_then(|b| b.inputs_digest.clone()),
            referents: status
                .and_then(|s| s.binding.as_ref())
                .and_then(|b| b.referents.as_ref())
                .map(|r| {
                    r.iter()
                        .take(16)
                        .map(|r| ReferentView {
                            kind: r.kind.clone(),
                            name: r.name.clone(),
                            uid: r.uid.clone(),
                            generation: r.generation,
                        })
                        .collect()
                })
                .unwrap_or_default(),
        },
        applicable: completed && !stale,
        stale,
        stale_reasons,
        observed_at: status.and_then(|s| s.observed_at.as_ref().copied()),
        expires_at: result.and_then(|r| r.expires_at.as_ref().copied()),
        checks,
        warnings,
        execution_only,
        details_available: result.and_then(|r| r.details_ref.as_ref()).is_some(),
        conditions: status
            .and_then(|s| s.conditions.as_ref())
            .map(|c| c.iter().take(MAX_CONDITIONS).map(condition_view).collect())
            .unwrap_or_default(),
    }
}

// ======================================================================
// Approver narrowing
// ======================================================================

/// D0: an approver reads readiness "only when needed for the approval packet".
///
/// An actor whose ONLY binding in this namespace is Approver may read a
/// Restore preflight — the readiness of the thing it is being asked to
/// authorize — and nothing else. It answers `not_found` rather than
/// `forbidden`, for the same enumeration reason an ungranted namespace does:
/// an approver must not be able to map which backup checks exist.
fn narrow_for_approver(
    state: &AppState,
    actor: &Actor,
    namespace: &str,
    object: &PreflightCr,
) -> Result<(), ApiError> {
    let roles = state.authorizer().roles(actor, namespace);
    let approver_only = roles.contains(&Role::Approver)
        && !roles
            .iter()
            .any(|r| matches!(r, Role::Viewer | Role::Operator | Role::Administrator));
    if approver_only && object.spec.request.operation != PreflightOperation::Restore {
        actor.audit.set_failure("forbidden");
        return Err(ApiError::not_found());
    }
    Ok(())
}

// ======================================================================
// Validation
// ======================================================================

fn check_reference(field: &str, name: &str, errors: &mut Vec<FieldError>) {
    if !validate::is_dns_subdomain(name) {
        errors.push(FieldError::new(
            field,
            "invalid_name",
            "must be a Kubernetes object name",
        ));
    }
}

/// Validate a create request.
///
/// # Errors
///
/// `validation_failed` naming every invalid field.
pub fn validate_create(request: &CreatePreflightRequest) -> Result<(), ApiError> {
    let mut errors = Vec::new();
    let blocks = [
        ("backup", request.backup.is_some()),
        ("restore", request.restore.is_some()),
        ("destinationAccess", request.destination_access.is_some()),
    ];
    let expected = match request.operation {
        PreflightOperationDto::Backup => "backup",
        PreflightOperationDto::Restore => "restore",
        PreflightOperationDto::DestinationAccess => "destinationAccess",
    };
    for (name, present) in blocks {
        if present && name != expected {
            errors.push(FieldError::new(
                name,
                "block_mismatch",
                format!("operation `{expected}` takes the `{expected}` block and no other"),
            ));
        }
    }
    if !blocks
        .iter()
        .any(|(name, present)| *name == expected && *present)
    {
        errors.push(FieldError::new(
            expected,
            "required",
            format!("operation `{expected}` needs the `{expected}` block"),
        ));
    }
    if let Some(backup) = &request.backup {
        check_reference(
            "backup.sourceConnection",
            &backup.source_connection,
            &mut errors,
        );
        match (&backup.destination, &backup.legacy_archive) {
            (Some(destination), None) => {
                check_reference("backup.destination", destination, &mut errors);
            }
            (None, Some(archive)) => {
                super::schedules::validate_archive("backup.legacyArchive", archive, &mut errors);
            }
            _ => errors.push(FieldError::new(
                "backup.destination",
                "exactly_one",
                "set exactly one of destination or legacyArchive",
            )),
        }
        if backup.topics.is_empty() || backup.topics.len() > 1000 {
            errors.push(FieldError::new(
                "backup.topics",
                "count_out_of_range",
                "between 1 and 1000 named topics are required",
            ));
        }
        for (i, topic) in backup.topics.iter().enumerate() {
            if !validate::is_topic_name(topic) {
                errors.push(FieldError::new(
                    format!("backup.topics[{i}]"),
                    "invalid_topic",
                    "must be a Kafka topic name; patterns are refused",
                ));
            }
        }
        if let Some(schedule) = &backup.schedule {
            check_reference("backup.schedule", schedule, &mut errors);
        }
    }
    if let Some(restore) = &request.restore {
        match (&restore.plan_bytes, &restore.restore_name) {
            (Some(bytes), None) => {
                if bytes.is_empty() || bytes.len() > MAX_PLAN_BYTES {
                    errors.push(FieldError::new(
                        "restore.planBytes",
                        "out_of_range",
                        format!("the plan is 1 to {MAX_PLAN_BYTES} bytes"),
                    ));
                }
                match &restore.plan_hash {
                    None => errors.push(FieldError::new(
                        "restore.planHash",
                        "required",
                        "a draft needs the sha256 of exactly these bytes",
                    )),
                    Some(hash) => {
                        let computed = logweir_core::ids::sha256_prefixed(bytes.as_bytes());
                        if hash != &computed {
                            // THE HASH IS COMPARED, NOT TRUSTED, and the
                            // MESSAGE DOES NOT ECHO EITHER VALUE: a plan hash
                            // in an error body is a fingerprint of a document
                            // the reader may not be authorized to see.
                            errors.push(FieldError::new(
                                "restore.planHash",
                                "hash_mismatch",
                                "planHash is not the sha256 of planBytes",
                            ));
                        }
                    }
                }
                match &restore.target {
                    None => errors.push(FieldError::new(
                        "restore.target",
                        "required",
                        "a draft needs the target connection",
                    )),
                    Some(target) => check_reference("restore.target", target, &mut errors),
                }
            }
            (None, Some(name)) => check_reference("restore.restoreName", name, &mut errors),
            _ => errors.push(FieldError::new(
                "restore.planBytes",
                "exactly_one",
                "set exactly one of planBytes (a draft) or restoreName (an existing Restore)",
            )),
        }
        match (
            &restore.source_destination,
            &restore.evidence_destination,
            &restore.legacy_source_archive,
        ) {
            (Some(source), Some(evidence), None) => {
                check_reference("restore.sourceDestination", source, &mut errors);
                check_reference("restore.evidenceDestination", evidence, &mut errors);
            }
            (None, None, Some(archive)) => {
                super::schedules::validate_archive(
                    "restore.legacySourceArchive",
                    archive,
                    &mut errors,
                );
            }
            (None, None, None) => {}
            _ => errors.push(FieldError::new(
                "restore.sourceDestination",
                "exactly_one",
                "source and evidence destinations are set together, and never beside a \
                 legacySourceArchive",
            )),
        }
        if let Some(point) = &restore.recovery_point {
            check_reference(
                "restore.recoveryPoint.backupName",
                &point.backup_name,
                &mut errors,
            );
        }
    }
    if let Some(access) = &request.destination_access {
        check_reference(
            "destinationAccess.destination",
            &access.destination,
            &mut errors,
        );
        if access.roles.is_empty() || access.roles.len() > 4 {
            errors.push(FieldError::new(
                "destinationAccess.roles",
                "count_out_of_range",
                "between 1 and 4 roles are required",
            ));
        }
    }
    if let Some(skip) = &request.skip_checks {
        if skip.len() > 32 {
            errors.push(FieldError::new(
                "skipChecks",
                "count_out_of_range",
                "at most 32 checks may be skipped",
            ));
        }
    }
    if let Some(timeout) = request.timeout_seconds {
        if !(30..=600).contains(&timeout) {
            errors.push(FieldError::new(
                "timeoutSeconds",
                "out_of_range",
                "the check budget is 30 to 600 seconds",
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

fn local(name: &str) -> LocalRef {
    LocalRef {
        name: name.to_string(),
    }
}

fn crd_role(role: DestinationRoleDto) -> DestinationRole {
    match role {
        DestinationRoleDto::ArchiveWrite => DestinationRole::ArchiveWrite,
        DestinationRoleDto::ArchiveRead => DestinationRole::ArchiveRead,
        DestinationRoleDto::EvidenceWrite => DestinationRole::EvidenceWrite,
        DestinationRoleDto::EvidenceRead => DestinationRole::EvidenceRead,
    }
}

fn archive_ref(archive: &crate::contract::ArchiveRequest) -> ArchiveRef {
    ArchiveRef {
        url: archive.url.clone(),
        secret_ref: archive.credential_ref.as_ref().map(|r| local(&r.name)),
    }
}

/// Build the stored object for a validated request.
#[must_use]
pub fn build(
    namespace: &str,
    name: String,
    annotations: BTreeMap<String, String>,
    labels: BTreeMap<String, String>,
    request: &CreatePreflightRequest,
) -> PreflightCr {
    PreflightCr {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: Some(namespace.to_string()),
            annotations: Some(annotations),
            labels: Some(labels),
            ..ObjectMeta::default()
        },
        spec: PreflightSpec {
            request: PreflightRequest {
                operation: match request.operation {
                    PreflightOperationDto::Backup => PreflightOperation::Backup,
                    PreflightOperationDto::Restore => PreflightOperation::Restore,
                    PreflightOperationDto::DestinationAccess => {
                        PreflightOperation::DestinationAccess
                    }
                },
                backup: request.backup.as_ref().map(|b| CrdBackupRequest {
                    source_ref: local(&b.source_connection),
                    destination_ref: b.destination.as_deref().map(local),
                    legacy_archive: b.legacy_archive.as_ref().map(archive_ref),
                    topics: b.topics.clone(),
                    schedule_ref: b.schedule.as_deref().map(local),
                }),
                restore: request.restore.as_ref().map(|r| CrdRestoreRequest {
                    // VERBATIM. The bytes go in as they arrived; nothing here
                    // parses or re-serializes a plan document.
                    plan_bytes: r.plan_bytes.clone(),
                    plan_hash: r.plan_hash.clone(),
                    restore_ref: r.restore_name.as_ref().map(|name| UidRef {
                        name: name.clone(),
                        uid: None,
                    }),
                    target_ref: r.target.as_deref().map(local),
                    source_destination_ref: r.source_destination.as_deref().map(local),
                    evidence_destination_ref: r.evidence_destination.as_deref().map(local),
                    legacy_source_archive: r.legacy_source_archive.as_ref().map(archive_ref),
                    recovery_point_ref: r.recovery_point.as_ref().map(|p| UidRef {
                        name: p.backup_name.clone(),
                        uid: p.backup_uid.clone(),
                    }),
                }),
                destination_access: request.destination_access.as_ref().map(|d| {
                    DestinationAccessRequest {
                        destination_ref: local(&d.destination),
                        roles: d.roles.iter().copied().map(crd_role).collect(),
                    }
                }),
                skip_checks: request.skip_checks.clone(),
                timeout_seconds: request
                    .timeout_seconds
                    .unwrap_or(weirkeeper::crds::preflight::DEFAULT_TIMEOUT_SECONDS),
            },
            cancel_requested: false,
        },
        status: None,
    }
}

fn labels_for(request: &CreatePreflightRequest) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    if let Some(access) = &request.destination_access {
        labels.insert(
            super::destinations::DESTINATION_LABEL.to_string(),
            access.destination.clone(),
        );
    }
    if let Some(backup) = &request.backup {
        if let Some(destination) = &backup.destination {
            labels.insert(
                super::destinations::DESTINATION_LABEL.to_string(),
                destination.clone(),
            );
        }
    }
    labels
}

// ======================================================================
// Routes
// ======================================================================

/// `POST .../preflights`.
pub async fn create(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(ns): ApiPath<String>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::RunPreflight)?;
    crate::http::parse_query(uri.query(), &[])?;
    let key = IdempotencyKey::from_headers(&headers)?;
    let mut request: CreatePreflightRequest = read_json(body, MAX_JSON_BODY).await?;
    validate_create(&request)?;
    check_create_rate(
        &state,
        &actor,
        &ns,
        ROUTE_CREATE,
        PREFLIGHT_CREATES_PER_MINUTE,
    )?;
    // Canonical form: an omitted budget IS the default, so both spellings hash
    // identically and replay as one request.
    request.timeout_seconds = Some(
        request
            .timeout_seconds
            .unwrap_or(weirkeeper::crds::preflight::DEFAULT_TIMEOUT_SECONDS),
    );
    let labels = labels_for(&request);
    let created = create_idempotent(
        &state,
        &actor,
        &ns,
        ROUTE_CREATE,
        NAME_PREFIX,
        &key,
        &request_id,
        &request,
        |name, annotations| build(&ns, name, annotations, labels.clone(), &request),
    )
    .await?;
    Ok(json(
        if created.replayed {
            StatusCode::OK
        } else {
            StatusCode::ACCEPTED
        },
        &PreflightResponse {
            request_id,
            replayed: Some(created.replayed),
            item: project(&created.object, state.now(), None),
        },
    ))
}

/// Start a `DestinationAccess` preflight for `POST .../destinations/{name}:test`.
///
/// # Errors
///
/// The create's own failures.
pub async fn start_destination_access(
    state: &AppState,
    actor: &Actor,
    namespace: &str,
    destination: &str,
    roles: &[DestinationRoleDto],
    key: &IdempotencyKey,
    request_id: &str,
) -> Result<Created<PreflightCr>, ApiError> {
    check_create_rate(
        state,
        actor,
        namespace,
        ROUTE_DESTINATION_TEST,
        PREFLIGHT_CREATES_PER_MINUTE,
    )?;
    let request = CreatePreflightRequest {
        operation: PreflightOperationDto::DestinationAccess,
        backup: None,
        restore: None,
        destination_access: Some(crate::contract::DestinationAccessPreflightRequest {
            destination: destination.to_string(),
            roles: roles.to_vec(),
        }),
        skip_checks: None,
        timeout_seconds: Some(weirkeeper::crds::preflight::DEFAULT_TIMEOUT_SECONDS),
    };
    validate_create(&request)?;
    let labels = labels_for(&request);
    create_idempotent(
        state,
        actor,
        namespace,
        ROUTE_DESTINATION_TEST,
        NAME_PREFIX,
        key,
        request_id,
        &request,
        |name, annotations| build(namespace, name, annotations, labels.clone(), &request),
    )
    .await
}

/// `GET .../preflights/{id}`.
pub async fn get_one(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, id)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadPreflights)?;
    let query = crate::http::parse_query(uri.query(), &["planHash"])?;
    if let Some(hash) = query.get("planHash") {
        if !is_sha256(hash) {
            return Err(ApiError::validation(vec![FieldError::new(
                "planHash",
                "invalid_hash",
                "planHash is sha256:<64 lowercase hex>",
            )]));
        }
    }
    let object = get_object::<PreflightCr>(&state, &actor, &ns, &id).await?;
    narrow_for_approver(&state, &actor, &ns, &object)?;
    Ok(json(
        StatusCode::OK,
        &PreflightResponse {
            request_id,
            replayed: None,
            item: project(
                &object,
                state.now(),
                query.get("planHash").map(String::as_str),
            ),
        },
    ))
}

fn is_sha256(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    })
}

/// `GET .../preflights/{id}/details`.
pub async fn details(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, id)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadPreflights)?;
    let query = crate::http::parse_query(uri.query(), &["check", "limit", "cursor"])?;
    let limit = match query.get("limit") {
        None => DEFAULT_LIMIT,
        Some(text) => match text.parse::<u32>() {
            Ok(n) if (1..=MAX_DETAIL_PAGE).contains(&n) => n,
            _ => {
                return Err(ApiError::validation(vec![FieldError::new(
                    "limit",
                    "out_of_range",
                    format!("limit must be an integer from 1 to {MAX_DETAIL_PAGE}"),
                )]))
            }
        },
    };
    let check = query.get("check").cloned().unwrap_or_default();
    if check.len() > 128 || check.chars().any(char::is_control) {
        return Err(ApiError::validation(vec![FieldError::new(
            "check",
            "invalid_value",
            "a check id is one bounded printable line",
        )]));
    }
    let object = get_object::<PreflightCr>(&state, &actor, &ns, &id).await?;
    narrow_for_approver(&state, &actor, &ns, &object)?;
    let Some(reference) = object
        .status
        .as_ref()
        .and_then(|s| s.result.as_ref())
        .and_then(|r| r.details_ref.as_ref())
    else {
        return Ok(json(
            StatusCode::OK,
            &DetailPageResponse {
                request_id,
                items: Vec::new(),
                page: Page {
                    limit,
                    next_cursor: None,
                    snapshot: None,
                },
            },
        ));
    };
    let scope = CursorScope {
        actor_id: actor.id(),
        route: ROUTE_DETAILS.to_string(),
        namespace: ns.clone(),
        filters: format!("id={id}&check={check}"),
    };
    let offset = match query.get("cursor") {
        None => 0usize,
        Some(cursor) => open_offset(&state, &scope, cursor)?,
    };
    let document = state
        .kube()
        .get_result_document(&ns, &reference.name)
        .await
        .map_err(KubeFailureExt::details_failure)?;
    verify_document(
        &document,
        object.meta().uid.as_deref().unwrap_or_default(),
        reference.sha256.as_deref(),
    )?;
    let body = document.data.get(DETAILS_KEY).cloned().unwrap_or_default();
    let mut items = Vec::new();
    let mut scanned;
    let mut next = None;
    for (index, line) in body.lines().enumerate() {
        if index < offset {
            continue;
        }
        scanned = index + 1;
        let Ok(entry) = serde_json::from_str::<serde_json::Value>(line) else {
            // A LINE THAT IS NOT JSON IS SKIPPED, NOT GUESSED AT. The producer
            // writes JSON lines; anything else is a truncated write, and a
            // half-line rendered as prose would be a detail nobody wrote.
            continue;
        };
        let owner = entry
            .get("check")
            .or_else(|| entry.get("id"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        if !check.is_empty() && owner.as_deref() != Some(check.as_str()) {
            continue;
        }
        items.push(DetailEntryView {
            check: owner,
            entry,
        });
        if items.len() as u32 >= limit {
            next = Some(cursor::seal(
                state.cursor_key(),
                &scope,
                &scanned.to_string(),
                state.now(),
            ));
            break;
        }
    }
    Ok(json(
        StatusCode::OK,
        &DetailPageResponse {
            request_id,
            items,
            page: Page {
                limit,
                next_cursor: next,
                snapshot: reference.sha256.clone(),
            },
        },
    ))
}

fn open_offset(state: &AppState, scope: &CursorScope, cursor: &str) -> Result<usize, ApiError> {
    match cursor::open(state.cursor_key(), scope, cursor, state.now()) {
        Ok(token) => token.parse::<usize>().map_err(|_| {
            ApiError::new(
                ProblemCode::CursorInvalid,
                "The cursor is not valid for this list; restart the list without a cursor.",
            )
        }),
        Err(CursorError::Invalid) => Err(ApiError::new(
            ProblemCode::CursorInvalid,
            "The cursor is not valid for this list; restart the list without a cursor.",
        )),
        Err(CursorError::Expired) => Err(ApiError::new(
            ProblemCode::CursorExpired,
            "The cursor expired; restart the list without a cursor.",
        )),
    }
}

/// D2 §5.6's integrity rule, applied to every stored result this API serves.
pub(crate) fn verify_document(
    document: &crate::kube::ResultDocument,
    owner_uid: &str,
    expected_sha256: Option<&str>,
) -> Result<(), ApiError> {
    let integrity = |why: &str| {
        ApiError::new(
            ProblemCode::ResultIntegrityFailed,
            format!(
                "A stored result document failed its integrity check ({why}). The page is \
                 refused rather than served from bytes whose provenance did not hold."
            ),
        )
    };
    if document.controller_owner_uid() != Some(owner_uid) {
        return Err(integrity("it is not owned by this check"));
    }
    if document.immutable != Some(true) {
        return Err(integrity("it is not immutable"));
    }
    let Some(expected) = expected_sha256 else {
        return Err(integrity("the status records no digest for it"));
    };
    if document.annotation(SHA256_ANNOTATION) != Some(expected) {
        return Err(integrity(
            "its recorded digest is not the one the status indexes",
        ));
    }
    let body: String = document.data.values().cloned().collect();
    if logweir_core::ids::sha256_prefixed(body.as_bytes()) != expected {
        return Err(integrity("its bytes do not hash to its recorded digest"));
    }
    Ok(())
}

trait KubeFailureExt {
    fn details_failure(self) -> ApiError;
}

impl KubeFailureExt for crate::kube::KubeFailure {
    fn details_failure(self) -> ApiError {
        match self {
            crate::kube::KubeFailure::NotFound => ApiError::new(
                ProblemCode::NotFound,
                "The stored result was collected with its check.",
            ),
            other => other.into_api_error(),
        }
    }
}

/// `POST .../preflights/{id}:cancel`.
pub async fn cancel(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, target)): ApiPath<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::CancelPreflight)?;
    let Some(id) = target.strip_suffix(CANCEL) else {
        return Err(ApiError::new(ProblemCode::NotFound, "No such command."));
    };
    check_name(id)?;
    crate::http::parse_query(uri.query(), &[])?;
    IdempotencyKey::refuse_on(&headers, ROUTE_CANCEL)?;
    let (object, already_terminal) = cancel_check::<PreflightCr>(
        &state,
        &actor,
        &ns,
        id,
        |o| is_terminal(state_of(o)),
        |o| o.spec.cancel_requested,
    )
    .await?;
    let projected = project(&object, state.now(), None);
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

/// `GET .../operations/preflight/{name}`.
pub async fn operation(
    state: &AppState,
    request_id: String,
    actor: &Actor,
    namespace: &str,
    name: &str,
) -> Result<Response, ApiError> {
    authorize(state, actor, namespace, Action::ReadPreflights)?;
    let object = get_object::<PreflightCr>(state, actor, namespace, name).await?;
    narrow_for_approver(state, actor, namespace, &object)?;
    let projected = project(&object, state.now(), None);
    Ok(json(
        StatusCode::OK,
        &CheckOperationResponse {
            request_id,
            item: CheckOperation {
                kind: CheckOperationKind::Preflight,
                name: projected.id,
                namespace: projected.namespace,
                uid: projected.uid,
                resource_version: projected.resource_version,
                created_at: projected.created_at,
                state: lifecycle(projected.state),
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

/// A preflight's aggregate, as the lifecycle a normalized operation reports.
///
/// `ready`, `notReady` and `unknown` are all "the check produced a result" —
/// the VERDICT is `Preflight.state`, and folding it into a lifecycle here
/// would say "failed" about a check that worked perfectly and found a problem.
const fn lifecycle(state: PreflightState) -> crate::contract::CheckLifecycle {
    use crate::contract::CheckLifecycle as L;
    match state {
        PreflightState::Pending => L::Pending,
        PreflightState::Queued => L::Queued,
        PreflightState::Running => L::Running,
        PreflightState::Ready | PreflightState::NotReady | PreflightState::Unknown => L::Succeeded,
        PreflightState::Failed => L::Failed,
        PreflightState::Cancelled => L::Cancelled,
    }
}

/// A destination exists before it is tested: the test route reads it first, so
/// a test against a name that does not exist is 404 rather than a Preflight
/// nobody can explain.
#[allow(dead_code)]
fn destination_exists(_: &BackupDestination) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plan_hash_query_is_the_repository_s_own_spelling() {
        assert!(is_sha256(&format!("sha256:{}", "a".repeat(64))));
        assert!(!is_sha256(&format!("sha256:{}", "A".repeat(64))));
        assert!(!is_sha256(&format!("sha256:{}", "a".repeat(63))));
        assert!(!is_sha256("deadbeef"));
    }

    #[test]
    fn an_unrecognised_phase_is_never_optimistic() {
        let mut object = PreflightCr::new(
            "pf-x",
            PreflightSpec {
                request: PreflightRequest {
                    operation: PreflightOperation::Backup,
                    backup: None,
                    restore: None,
                    destination_access: None,
                    skip_checks: None,
                    timeout_seconds: 120,
                },
                cancel_requested: false,
            },
        );
        object.status = Some(weirkeeper::crds::preflight::PreflightStatus {
            phase: Some("Ascendant".to_string()),
            ..Default::default()
        });
        assert_eq!(state_of(&object), PreflightState::Unknown);
    }
}
