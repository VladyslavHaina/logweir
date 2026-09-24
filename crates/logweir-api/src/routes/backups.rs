//! Backups: `Backup` projections, and the canonical manual run (PLAT-06.2).
//!
//! ONE CR PATH (D1 §8.1). "Back up now" and "Run first backup now" both create
//! a `Backup` with `spec.trigger.kind: Manual` and `triggeredBy: manual` — the
//! same object `kubectl create -f config/samples/backup-manual.yaml` creates.
//! There is no second mechanism, no annotation the controller executes and no
//! run this route can make that a person could not make by hand.
//!
//! A MANUAL RUN IS NEVER BLOCKED BY THE SCHEDULE'S STATE (D1 §8.3). A suspended
//! schedule, a schedule whose cadence is invalid, a schedule with a run already
//! going under `Forbid`, and a schedule DELETED a second after the click all
//! still produce a run: the policy was copied when the request was accepted,
//! `concurrencyPolicy` is about slots and not about people, and the controller
//! never reads a `BackupSchedule` for a manual run. The response reports the
//! schedule's state so the console can say so instead of hiding it.
//!
//! READINESS IS RECORDED, NEVER OBEYED (D1 §8.4). This API does not call a
//! preflight, does not wait for one and does not refuse on one. A
//! `readinessAcknowledgement` becomes an annotation and nothing more —
//! execution-time guards stay the authority, and a console that wants a
//! "Run anyway" confirmation implements it in the console.

use std::collections::BTreeMap;

use axum::body::Body;
use axum::extract::State;
use axum::response::Response;
use http::{HeaderMap, StatusCode, Uri};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::ResourceExt;
use weirkeeper::controllers::backup_schedule;
use weirkeeper::crds::backup::{Backup, BackupSpec, ScheduleRef, Trigger, TriggerKind};
use weirkeeper::crds::backup_schedule::BackupSchedule;
use weirkeeper::crds::LocalRef;
use weirkeeper::{identity, policy};

use super::{
    authorize, check_name, create_idempotent, get_object, json, list_page, list_query, schedules,
    ApiPath,
};
use crate::app::AppState;
use crate::auth::Actor;
use crate::authz::Action;
use crate::contract::{
    AcknowledgedReadiness, BackupList, BackupResponse, BackupScheduleRefRequest,
    CreateBackupRequest, ManualBackupResponse, ScheduleContextView,
};
use crate::http::{read_json, RequestId, MAX_JSON_BODY};
use crate::idempotency::IdempotencyKey;
use crate::problem::{ApiError, FieldError};
use crate::projection;
use crate::validate;

/// The list route identifier.
pub const ROUTE_LIST: &str = "GET /api/v1/namespaces/{ns}/backups";

/// `GET .../backups`. `labelSelector=logweir.dev/schedule=<name>` lists one
/// schedule's runs.
pub async fn list(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(ns): ApiPath<String>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadBackups)?;
    let query = list_query(uri.query())?;
    let (items, page) = list_page::<Backup>(&state, &actor, &ns, ROUTE_LIST, &query).await?;
    Ok(json(
        StatusCode::OK,
        &BackupList {
            request_id,
            items: items.iter().map(projection::backup).collect(),
            page,
        },
    ))
}

/// `GET .../backups/{name}`.
pub async fn get_one(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, name)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadBackups)?;
    crate::http::parse_query(uri.query(), &[])?;
    let object = get_object::<Backup>(&state, &actor, &ns, &name).await?;
    Ok(json(
        StatusCode::OK,
        &BackupResponse {
            request_id,
            replayed: None,
            item: projection::backup(&object),
        },
    ))
}

// ======================================================================
// PLAT-06.2: "Back up now" and "Run first backup now"
// ======================================================================

/// The create route identifier. It is part of the idempotency scope, so it is
/// also part of the object's name.
pub const ROUTE_CREATE: &str = "POST /api/v1/namespaces/{ns}/backups";

/// D1 §3.1/§8.1: the deterministic name of a manual run. `logweir-manual-`
/// plus the 26 base32 characters of the idempotency scope digest is 41
/// characters, well inside DNS-1123's 63.
pub const NAME_PREFIX: &str = "logweir-manual-";

/// The annotation that records "a person was shown a not-ready verdict and
/// went ahead" (D1 §8.4). NOT AUTHORITATIVE: nothing reads it to decide
/// anything, and the run's own execution-time guards still run.
pub const READINESS_ACK_ANNOTATION: &str = "logweir.dev/readiness-ack";

/// The default run deadline, as D1 §8.1 writes it.
pub const DEFAULT_DEADLINE_SECONDS: i64 = weirkeeper::cadence::DEFAULT_ACTIVE_DEADLINE_SECONDS;

/// Which of D1 §8.2's two bodies this request is.
enum Shape<'a> {
    /// Body A: copy a schedule's current revision.
    FromSchedule(&'a BackupScheduleRefRequest),
    /// Body B: the body IS the policy.
    AdHoc,
}

/// Decide the body's shape and refuse a mixture.
///
/// A POLICY FIELD BESIDE `scheduleRef` IS `422`, NOT A MERGE. "Back up now on
/// this schedule" promises the run the schedule describes; a body that also
/// named topics would produce a run whose receipt says `scheduleRef: nightly`
/// and whose contents are something else.
fn shape(request: &CreateBackupRequest) -> Result<Shape<'_>, ApiError> {
    let policy_fields: [(&str, bool); 5] = [
        ("sourceRef", request.source_ref.is_some()),
        ("topicSelection", request.topic_selection.is_some()),
        ("legacyArchive", request.legacy_archive.is_some()),
        ("destinationRef", request.destination_ref.is_some()),
        ("deadlineSeconds", request.deadline_seconds.is_some()),
    ];
    match &request.schedule_ref {
        Some(reference) => {
            let named: Vec<FieldError> = policy_fields
                .iter()
                .filter(|(_, present)| *present)
                .map(|(field, _)| {
                    FieldError::new(
                        *field,
                        "not_allowed_with_schedule_ref",
                        "a run taken from a schedule copies that schedule's policy; send this \
                         field only in an ad-hoc request",
                    )
                })
                .collect();
            if named.is_empty() {
                Ok(Shape::FromSchedule(reference))
            } else {
                Err(ApiError::validation(named))
            }
        }
        None => {
            if request.source_ref.is_none() {
                return Err(ApiError::validation(vec![FieldError::new(
                    "sourceRef",
                    "required",
                    "send scheduleRef to run a schedule's policy, or sourceRef with a selection \
                     and a destination to run an ad-hoc backup",
                )]));
            }
            Ok(Shape::AdHoc)
        }
    }
}

/// Validate an ad-hoc body (D1 §8.2 body B).
fn validate_ad_hoc(request: &CreateBackupRequest) -> Result<(), ApiError> {
    let mut errors = Vec::new();
    match &request.source_ref {
        Some(r) if validate::is_dns_subdomain(&r.name) => {}
        Some(_) => errors.push(FieldError::new(
            "sourceRef.name",
            "invalid_name",
            "must be a Kubernetes object name",
        )),
        None => errors.push(FieldError::new(
            "sourceRef",
            "required",
            "an ad-hoc run needs a source connection",
        )),
    }
    match &request.topic_selection {
        Some(selection) => {
            schedules::validate_selection("topicSelection", selection, &mut errors);
        }
        None => errors.push(FieldError::new(
            "topicSelection",
            "required",
            "an ad-hoc run needs a topic selection",
        )),
    }
    let _ = schedules::destination_or_archive(
        request.legacy_archive.as_ref(),
        request.destination_ref.as_ref(),
        &mut errors,
    );
    if let Some(seconds) = request.deadline_seconds {
        if !(weirkeeper::cadence::MIN_ACTIVE_DEADLINE_SECONDS
            ..=weirkeeper::cadence::MAX_ACTIVE_DEADLINE_SECONDS)
            .contains(&seconds)
        {
            errors.push(FieldError::new(
                "deadlineSeconds",
                "out_of_range",
                "must be from 60 to 86400",
            ));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ApiError::validation(errors))
    }
}

/// Check the acknowledgement's shape. It is recorded, never obeyed.
fn validate_acknowledgement(request: &CreateBackupRequest) -> Result<(), ApiError> {
    let Some(ack) = &request.readiness_acknowledgement else {
        return Ok(());
    };
    if validate::is_dns_subdomain(&ack.preflight) {
        Ok(())
    } else {
        Err(ApiError::validation(vec![FieldError::new(
            "readinessAcknowledgement.preflight",
            "invalid_name",
            "must be a Kubernetes object name",
        )]))
    }
}

/// The `Backup` a validated request becomes.
///
/// ONE CR PATH (D1 §8.1). This is the same object `kubectl create -f
/// config/samples/backup-manual.yaml` produces, down to the labels and the
/// trigger block; the only differences are the deterministic name and the
/// idempotency annotations the console mode adds.
fn build(
    namespace: &str,
    name: String,
    mut annotations: BTreeMap<String, String>,
    request: &CreateBackupRequest,
    from: Option<&BackupSchedule>,
) -> Backup {
    let mut labels = BTreeMap::from([
        (identity::TRIGGER_LABEL.to_string(), "manual".to_string()),
        (identity::ATTEMPT_LABEL.to_string(), "0".to_string()),
    ]);
    if let Some(ack) = &request.readiness_acknowledgement {
        let state = match ack.state {
            AcknowledgedReadiness::NotReady => "notReady",
            AcknowledgedReadiness::Unknown => "unknown",
        };
        annotations.insert(
            READINESS_ACK_ANNOTATION.to_string(),
            format!("{}={}", ack.preflight, state),
        );
    }
    // THE POLICY HALF, FROM ONE BUILDER. Body A calls the SCHEDULER'S OWN
    // `run_policy_spec`, the function `decide` validates and `scheduled_backup`
    // POSTs — so "Back up now" and the next slot copy the same fields, resolve
    // the absent deadline the same way and therefore digest the same. A second
    // construction here is exactly how a manual run comes to record a digest
    // the controller then refuses (D1 §3.1 rule 5).
    let mut spec = match from {
        Some(schedule) => {
            let schedule_name = schedule.name_any();
            let uid = schedule.uid().unwrap_or_default();
            labels.insert(identity::SCHEDULE_LABEL.to_string(), schedule_name.clone());
            if !uid.is_empty() {
                labels.insert(identity::SCHEDULE_UID_LABEL.to_string(), uid.clone());
            }
            let mut spec = backup_schedule::run_policy_spec(&schedule.spec);
            spec.schedule_ref = Some(ScheduleRef {
                name: schedule_name,
                uid: Some(uid),
                generation: schedule.metadata.generation,
                // Filled in below, from the COPY rather than from
                // `status.policy`: the status is what the controller last
                // OBSERVED and may be an older generation, while the run must
                // record the digest of what it is actually going to do.
                run_policy_sha256: None,
            });
            spec
        }
        None => {
            let mut errors = Vec::new();
            let (archive, destination_ref) = schedules::destination_or_archive(
                request.legacy_archive.as_ref(),
                request.destination_ref.as_ref(),
                &mut errors,
            );
            debug_assert!(errors.is_empty(), "validate_ad_hoc ran first");
            let selection = request
                .topic_selection
                .as_ref()
                .expect("validate_ad_hoc ran first");
            let (topics, all_user_topics) = schedules::selection_fields(selection);
            BackupSpec {
                source_ref: LocalRef {
                    name: request
                        .source_ref
                        .as_ref()
                        .expect("validate_ad_hoc ran first")
                        .name
                        .clone(),
                },
                topics,
                all_user_topics,
                archive,
                destination_ref,
                schedule_ref: None,
                slot: None,
                triggered_by: String::new(),
                trigger: None,
                deadline_seconds: request.deadline_seconds.unwrap_or(DEFAULT_DEADLINE_SECONDS),
            }
        }
    };
    // THE IDENTITY, WRITTEN LAST AND ALWAYS THE SAME. `run_policy_spec` fills
    // `triggeredBy: schedule` because it builds the SCHEDULER'S run; a manual
    // run overwrites it, has no slot and has no attempt above zero.
    spec.triggered_by = identity::TRIGGERED_BY_MANUAL.to_string();
    spec.slot = None;
    spec.trigger = Some(Trigger {
        kind: TriggerKind::Manual,
        attempt: 0,
        retry_of: None,
        time_zone: None,
    });
    // The digest covers the policy and not the identity, so it is taken over
    // the finished spec; `run_policy_sha256` reads neither `scheduleRef` nor
    // `trigger`.
    let digest = policy::run_policy_sha256(&spec);
    if let Some(reference) = spec.schedule_ref.as_mut() {
        reference.run_policy_sha256 = Some(digest);
    }
    Backup {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: Some(namespace.to_string()),
            labels: Some(labels),
            annotations: Some(annotations),
            ..ObjectMeta::default()
        },
        spec,
        status: None,
    }
}

/// `POST .../backups`.
#[allow(clippy::too_many_lines)]
pub async fn create(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath(ns): ApiPath<String>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::CreateManualBackup)?;
    crate::http::parse_query(uri.query(), &[])?;
    let key = IdempotencyKey::from_headers(&headers)?;
    let request: CreateBackupRequest = read_json(body, MAX_JSON_BODY).await?;
    validate_acknowledgement(&request)?;

    let from = match shape(&request)? {
        Shape::AdHoc => {
            validate_ad_hoc(&request)?;
            None
        }
        Shape::FromSchedule(reference) => {
            check_name(&reference.name).map_err(|_| {
                ApiError::validation(vec![FieldError::new(
                    "scheduleRef.name",
                    "invalid_name",
                    "must be a Kubernetes object name",
                )])
            })?;
            let schedule =
                get_object::<BackupSchedule>(&state, &actor, &ns, &reference.name).await?;
            // D1 §8.2: the REVISION the caller believes it is running.
            if let Some(expected) = reference.expected_generation {
                let current = schedule.metadata.generation.unwrap_or_default();
                if current != expected {
                    actor.audit.set_failure("policy_changed");
                    return Err(ApiError::policy_changed(
                        current,
                        schedule
                            .status
                            .as_ref()
                            .and_then(|s| s.policy.as_ref())
                            .map(|p| p.run_policy_sha256.clone()),
                    ));
                }
            }
            Some(schedule)
        }
    };

    // THE RUN POLICY IS CHECKED AFTER THE COPY, AND ON THE COPY. A schedule
    // may be stored with a shape this build refuses (an older CRD, a hand
    // edit); copying it and only then validating is what makes the answer a
    // 422 about the RUN rather than a run that fails at the guard rail.
    {
        let probe = build(
            &ns,
            "probe".to_string(),
            BTreeMap::new(),
            &request,
            from.as_ref(),
        );
        if let Err(errors) = policy::validate_run_policy(&probe.spec) {
            return Err(ApiError::validation(
                errors
                    .into_iter()
                    .map(|e| {
                        FieldError::new(
                            field_for(&e.field, from.is_some()),
                            "selection_invalid",
                            validate::bounded(
                                &message_for(&e.field, &e.message, from.is_some()),
                                384,
                            ),
                        )
                    })
                    .collect(),
            ));
        }
    }

    // P10: THE PER-ACTOR CREATE LIMIT — `429 rate_limited` with
    // `Retry-After` past `rateLimits.manualBackupsPerMinute` per actor, per
    // namespace, per minute. After every refusal that is about the request
    // itself (a malformed run does not spend the window), before the object
    // exists. It bounds how fast one person can QUEUE runs; how many RUN at
    // once is the controller's manual-run pool, which holds whatever this
    // lets through.
    super::run_create_rate(&state, &actor, &ns, super::ManualRun::Backup)?;
    let created = create_idempotent(
        &state,
        &actor,
        &ns,
        ROUTE_CREATE,
        NAME_PREFIX,
        &key,
        &request_id,
        &request,
        |name, annotations| build(&ns, name, annotations, &request, from.as_ref()),
    )
    .await?;

    let schedule = from.as_ref().map(|schedule| ScheduleContextView {
        name: schedule.name_any(),
        uid: schedule.uid().unwrap_or_default(),
        generation: schedule.metadata.generation.unwrap_or_default(),
        run_policy_sha256: created
            .object
            .spec
            .schedule_ref
            .as_ref()
            .and_then(|r| r.run_policy_sha256.clone()),
        // A SUSPENDED SCHEDULE DOES NOT BLOCK A MANUAL RUN, and an active one
        // does not either (D1 §8.3). Both are REPORTED so the console can say
        // so rather than leaving the person to wonder.
        suspended: schedule.spec.suspend,
        active_runs: schedule
            .status
            .as_ref()
            .and_then(|s| s.active_runs.as_ref())
            .map(|runs| runs.iter().map(projection::active_run).collect())
            .unwrap_or_default(),
    });
    tracing::info!(
        namespace = %ns,
        name = %created.object.name_any(),
        schedule = %schedule.as_ref().map(|s| s.name.clone()).unwrap_or_default(),
        replayed = created.replayed,
        actor = %actor.id(),
        "manual backup created"
    );
    Ok(json(
        created.status(),
        &ManualBackupResponse {
            request_id,
            replayed: created.replayed,
            item: projection::backup(&created.object),
            schedule,
        },
    ))
}

/// Map a `weirkeeper::policy` field path onto the REQUEST's own field names,
/// so a console can highlight the input the person typed.
///
/// `FieldError.field` IS A PATH AND NOTHING ELSE. It used to return
/// `"scheduleRef.name (topics)"` for a from-schedule run, which is a path with
/// a parenthetical glued on: `ui/contract.js` types the field as a plain
/// string with no parser, so a console doing
/// `errors.find(e => e.field === "scheduleRef.name")` to highlight the
/// schedule picker found nothing and fell back to an unhighlighted generic
/// 422. The offending schedule field belongs in the MESSAGE, which is already
/// a sentence.
fn field_for(field: &str, from_schedule: bool) -> String {
    if from_schedule {
        // The person typed a schedule name; the input to highlight is the
        // picker, not `topicSelection.topics`, which is not on that form.
        return "scheduleRef.name".to_string();
    }
    match field {
        "topics" => "topicSelection.topics".to_string(),
        "allUserTopics" => "topicSelection.allUserTopics".to_string(),
        other => other.to_string(),
    }
}

/// The sentence a from-schedule refusal carries: which of the SCHEDULE's own
/// fields is the problem, said in words rather than smuggled into the path.
fn message_for(field: &str, message: &str, from_schedule: bool) -> String {
    if from_schedule {
        format!(
            "the schedule's policy cannot run as it stands (spec.{field}): {message}. Edit the \
             schedule, or send an ad-hoc request with its own selection."
        )
    } else {
        message.to_string()
    }
}
