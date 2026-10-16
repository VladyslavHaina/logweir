//! **PROD-07.2: an honest interrupted restore** (design
//! `docs/to-do/decisions/OD-23-core.md` §3).
//!
//! - **Cancel** (`logweir.dev/cancel-requested-by`): before the run Job exists
//!   the `Restore` is `Failed`, `Cancelled`, and nothing ran. While it runs the
//!   controller sets `spec.suspend: true` on the run Job; the Job controller
//!   deletes its pod, the runner forwards SIGTERM to its engine and SIGKILLs it
//!   after ten seconds, and signs nothing. The `Restore` stays `Running`,
//!   reason `Stopping`, until no pod of the run Job is left. A run that had
//!   already ended keeps its outcome: its scorecard decides.
//! - **Interrupted**: a run that ended without a signed scorecard after it
//!   created its target topics. Known from the runner's `targets-record-key=`
//!   line when its log is read, and assumed when no pod or exit code is left
//!   (a cancel, a deleted or evicted pod): the reconcile then decides.
//! - **Reconcile**: a copy of the run Job's pod running `logweir restore
//!   reconcile` (same flags, `--run-id` the interrupted run). Its end is read
//!   like a run's ([`super::finish_job`]): a signed `pass` is `Succeeded`,
//!   `ReconciledComplete`; a signed `fail-integrity` stays `Interrupted` with
//!   its counts on `status.integrity`; the run's own scorecard (it signed after
//!   all) decides as it would have; `reconcile-none=` (no targets record, or
//!   none of its topics is still the run's) is `Failed`. Any other end is
//!   tried again, at most three times, five minutes apart, then
//!   `verification: Failed`.
//! - **Clean up** (`logweir.dev/cleanup-requested-by`, only after a signed
//!   reconcile): `logweir restore cleanup --run-id <the reconcile's run>`,
//!   wired as a clone's cleanup is, retried the same way. What it removed and
//!   left reaches `status.teardown`; `Removed` releases the recovery point.
//!
//! THE CONTROLLER DELETES NO TOPIC and starts no retry of the restore.

use super::restore_clone;
use super::{
    condition, finish_job, patch_status_if_changed, Requeue, Reread, RestoreError,
    RestoreEvidenceKeys, RestoreOutcome,
};
use crate::crds::restore::Restore;
use crate::job;
use chrono::{DateTime, Utc};
use k8s_openapi::api::batch::v1::Job;
use kube::api::{Api, Patch, PatchParams, PostParams};
use kube::{Resource as _, ResourceExt as _};
use serde_json::{json, Value};

/// Who asked to cancel the run (set by the API).
pub const CANCEL_ANNOTATION: &str = "logweir.dev/cancel-requested-by";
/// Who asked to clean up an interrupted run's topics (set by the API).
pub const CLEANUP_ANNOTATION: &str = "logweir.dev/cleanup-requested-by";
/// The phase.
pub const PHASE_INTERRUPTED: &str = "Interrupted";
/// The condition type every write of this module carries.
pub const CONDITION_INTERRUPTED: &str = "Interrupted";
/// Reasons.
pub const REASON_CANCELLED: &str = "Cancelled";
/// The run Job is suspended and a pod of it is still there.
pub const REASON_STOPPING: &str = "Stopping";
/// The reconcile found every kept partition exact.
pub const REASON_RECONCILED_COMPLETE: &str = "ReconciledComplete";
/// Causes.
pub const CAUSE_CANCELLED: &str = "Cancelled";
/// The runner was stopped or killed: a pod deleted or evicted, a deadline.
pub const CAUSE_RUNNER_STOPPED: &str = "RunnerStopped";
/// The run failed after it created its targets.
pub const CAUSE_RUN_FAILED: &str = "RunFailed";
/// `status.interruption.verification` values.
pub const VERIFICATION_RUNNING: &str = "Running";
/// The reconcile signed.
pub const VERIFICATION_DONE: &str = "Done";
/// The reconcile failed three times, or refused.
pub const VERIFICATION_FAILED: &str = "Failed";
/// The cleanup's phases (as a clone's).
pub const CLEANUP_REMOVING: &str = restore_clone::PHASE_REMOVING;
/// The cleanup deleted what the record named.
pub const CLEANUP_REMOVED: &str = restore_clone::PHASE_REMOVED;
/// The cleanup refused, or failed three times.
pub const CLEANUP_FAILED: &str = restore_clone::PHASE_REMOVAL_FAILED;
/// The runner's line for an exit without a scorecard after it created its
/// targets (`logweir::drill::reconcile::TARGETS_RECORD_KEY_PREFIX`).
pub const TARGETS_RECORD_KEY_PREFIX: &str = "targets-record-key=";
/// The reconcile's line when there was nothing to verify
/// (`logweir::drill::reconcile::NONE_PREFIX`).
pub const RECONCILE_NONE_PREFIX: &str = "reconcile-none=";
/// The runner's `failure-reason=` value when a signal stopped it.
pub const RUNNER_CANCELLED_REASON: &str = "Cancelled";
/// The flag the run and the reconcile name the interrupted run with.
pub const RUN_ID_ARG: &str = "--run-id";
/// At most this many reconcile (or cleanup) Jobs.
pub const MAX_ATTEMPTS: i64 = restore_clone::MAX_ATTEMPTS;
/// How long after a failed attempt the next one starts.
pub const RETRY_AFTER_SECONDS: i64 = restore_clone::RETRY_AFTER_SECONDS;
/// How often a stopping run, a reconcile or a cleanup is looked at.
pub const POLL_SECONDS: u64 = 10;
/// The pod grace a run Job carries, so a stop has time to stop the engine
/// (ten seconds) and remove what a scratch run created.
pub const RUN_GRACE_SECONDS: i64 = 30;

fn annotation<'a>(restore: &'a Restore, key: &str) -> Option<&'a str> {
    restore
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(key))
        .map(String::as_str)
}

fn phase_of(restore: &Restore) -> Option<&str> {
    restore.status.as_ref().and_then(|s| s.phase.as_deref())
}

fn interruption(restore: &Restore) -> Option<&crate::crds::restore::RestoreInterruption> {
    restore
        .status
        .as_ref()
        .and_then(|s| s.interruption.as_ref())
}

fn bounded(s: &str) -> String {
    s.chars().take(1024).collect()
}

fn handled(restore: &Restore, requeue: Requeue) -> RestoreOutcome {
    RestoreOutcome {
        job_name: restore.name_any(),
        created: false,
        admission: None,
        exit_code: None,
        terminal_state: None,
        keys: RestoreEvidenceKeys::default(),
        ttl_patched: false,
        requeue,
    }
}

/// **A fresh run id for a new run Job**, so the controller can name its
/// targets record after its pod is gone: `--run-id <id>` on the runner's argv.
/// `None` when the random source failed; the run then mints its own and a
/// reconcile after a lost pod cannot find its record (it reads as nothing to
/// verify).
#[must_use]
pub fn new_run_id(now: DateTime<Utc>) -> Option<String> {
    let mut bytes = [0u8; 16];
    rustls::crypto::ring::default_provider()
        .secure_random
        .fill(&mut bytes)
        .ok()?;
    let ms = u64::try_from(now.timestamp_millis()).unwrap_or(0);
    Some(logweir_core::ids::format_run_id(
        ms,
        u128::from_be_bytes(bytes),
    ))
}

/// Name the run (`--run-id`) and give its pod [`RUN_GRACE_SECONDS`]. Pure
/// over the Job.
pub fn prepare_run_job(job: &mut Job, run_id: Option<&str>) {
    let Some(pod) = job.spec.as_mut().and_then(|s| s.template.spec.as_mut()) else {
        return;
    };
    pod.termination_grace_period_seconds = Some(RUN_GRACE_SECONDS);
    if let (Some(id), Some(c)) = (
        run_id,
        pod.containers
            .iter_mut()
            .find(|c| c.name == job::CONTAINER_NAME),
    ) {
        c.args
            .get_or_insert_with(Vec::new)
            .extend([RUN_ID_ARG.to_string(), id.to_string()]);
    }
}

/// The run id a Job's runner was given, if any.
#[must_use]
pub fn run_id_of(job: &Job) -> Option<String> {
    let args = job
        .spec
        .as_ref()?
        .template
        .spec
        .as_ref()?
        .containers
        .iter()
        .find(|c| c.name == job::CONTAINER_NAME)?
        .args
        .as_ref()?;
    let i = args.iter().position(|a| a == RUN_ID_ARG)?;
    args.get(i + 1).cloned()
}

/// **The cause** of an interruption: a cancel the API recorded first, then
/// the runner's own word that a signal stopped it, else a run that failed (or
/// a pod that is gone).
#[must_use]
pub fn cause(restore: &Restore, runner_stopped: bool, run_failed: bool) -> &'static str {
    if annotation(restore, CANCEL_ANNOTATION).is_some() {
        CAUSE_CANCELLED
    } else if run_failed && !runner_stopped {
        CAUSE_RUN_FAILED
    } else {
        CAUSE_RUNNER_STOPPED
    }
}

/// **`patch` turned into the `Interrupted` write**: the phase, the reason (the
/// cause), the interruption block, and the `Interrupted` condition. Pure.
#[must_use]
pub fn interrupted_patch(
    mut patch: Value,
    restore: &Restore,
    cause: &str,
    run_id: Option<&str>,
    now: DateTime<Utc>,
) -> Value {
    let message = format!(
        "the run ended without a signed scorecard after it created its target topics ({cause}); \
         nothing was resumed. A reconcile Job verifies what it left"
    );
    let block = json!({
        "cause": cause,
        "requestedBy": annotation(restore, CANCEL_ANNOTATION).map(bounded),
        "writerGoneAt": now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "runId": run_id,
        "verification": VERIFICATION_RUNNING,
        "attempts": 0,
    });
    let status = patch
        .as_object_mut()
        .and_then(|p| p.get_mut("status"))
        .and_then(Value::as_object_mut);
    if let Some(status) = status {
        status.insert("phase".into(), json!(PHASE_INTERRUPTED));
        status.insert("reason".into(), json!(cause));
        status.insert("interruption".into(), block);
        let new = condition(restore, CONDITION_INTERRUPTED, "True", cause, &message, now);
        let mut conditions: Vec<Value> = status
            .get("conditions")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        conditions.retain(|c| c.get("type") != Some(&json!(CONDITION_INTERRUPTED)));
        conditions.insert(0, new);
        status.insert("conditions".into(), json!(conditions));
    }
    patch
}

/// **The decision after a run Job's exit was read** (the run's own path):
/// an exit that signed nothing (not 0, 2 or 3) from a run that printed its
/// targets record is `Interrupted`. `None` keeps the ordinary terminal write.
#[must_use]
pub fn after_exit(
    restore: &Restore,
    job: &Job,
    exit_code: i32,
    log: &str,
    finished: Value,
    now: DateTime<Utc>,
) -> Option<Value> {
    if matches!(exit_code, 0 | 2 | 3) {
        return None;
    }
    if crate::runner_log::key_values(log, TARGETS_RECORD_KEY_PREFIX).is_empty() {
        return None;
    }
    let stopped = crate::runner_log::tail_lines(log)
        .last()
        .and_then(|l| l.strip_prefix(logweir_core::guard::FAILURE_REASON_PREFIX))
        == Some(RUNNER_CANCELLED_REASON);
    let cause = cause(restore, stopped, true);
    Some(interrupted_patch(
        finished,
        restore,
        cause,
        run_id_of(job).as_deref(),
        now,
    ))
}

/// **The decision when no exit code is left** (no pod, a deleted pod, a
/// deadline): a run that may have started is `Interrupted` and the reconcile
/// decides; one that never started (unschedulable, an image it could not
/// pull) is `Failed` as before. `None` keeps the ordinary write.
#[must_use]
pub fn after_crash(
    restore: &Restore,
    job: &Job,
    terminal_state: &str,
    crashed: Value,
    now: DateTime<Utc>,
) -> Option<Value> {
    let may_have_run = terminal_state == crate::conditions::TERMINAL_STATE_NO_EXIT_CODE
        || terminal_state == crate::conditions::TERMINAL_STATE_DISRUPTED_MID_DRILL;
    if !may_have_run {
        return None;
    }
    let run_id = run_id_of(job)?;
    let cause = cause(restore, true, false);
    Some(interrupted_patch(
        crashed,
        restore,
        cause,
        Some(&run_id),
        now,
    ))
}

/// **What this module does before the reconcile's own steps**: an
/// `Interrupted` restore is advanced here, and a cancel of one that is not
/// terminal is applied. `None` hands the pass to the reconcile.
///
/// # Errors
///
/// [`RestoreError`]; a cancel before any Job is [`RestoreError::Refused`]
/// `Cancelled`, which the reconcile writes as `Failed`.
pub(super) async fn before(
    restore: &Restore,
    client: &kube::Client,
    namespace: &str,
    now: DateTime<Utc>,
    runner: &job::RunnerImage,
    reads: &Reread<'_>,
) -> Result<Option<RestoreOutcome>, RestoreError> {
    if phase_of(restore) == Some(PHASE_INTERRUPTED) {
        return advance(restore, client, namespace, now, runner, reads)
            .await
            .map(Some);
    }
    if super::status_is_terminal(restore) {
        return Ok(None);
    }
    let Some(by) = annotation(restore, CANCEL_ANNOTATION) else {
        return Ok(None);
    };
    let name = restore.name_any();
    let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
    let Some(run) = jobs.get_opt(&name).await.map_err(RestoreError::Api)? else {
        return Err(RestoreError::Refused(
            REASON_CANCELLED,
            format!("cancelled by {by} before its Job was created; nothing ran"),
        ));
    };
    if crate::controllers::backup::job_finished(&run) {
        // The run's own end decides, even after a cancel.
        return Ok(None);
    }
    if !run.spec.as_ref().and_then(|s| s.suspend).unwrap_or(false) {
        jobs.patch(
            &name,
            &PatchParams::default(),
            &Patch::Merge(json!({"spec": {"suspend": true}})),
        )
        .await
        .map_err(RestoreError::Api)?;
        tracing::info!(restore = %name, namespace, by, "cancel: the run Job is suspended");
    }
    let restores: Api<Restore> = Api::namespaced(client.clone(), namespace);
    let found = super::find_pod(client, namespace, &name, run.uid().as_deref()).await?;
    let left: Vec<String> = found
        .pod
        .iter()
        .map(|p| p.name_any())
        .chain(found.contested.iter().cloned())
        .collect();
    if !left.is_empty() {
        let message = format!(
            "cancel requested by {by}: the run Job is suspended and its pod {} is still \
             stopping (SIGTERM to the engine, SIGKILL after ten seconds); a pod on a lost node \
             stays until it is force-deleted",
            left.join(", ")
        );
        let conditions = crate::conditions::carry_remaining(
            restore.status.as_ref().and_then(|s| s.conditions.as_ref()),
            vec![condition(
                restore,
                CONDITION_INTERRUPTED,
                "Unknown",
                REASON_STOPPING,
                &message,
                now,
            )],
        );
        patch_status_if_changed(
            &restores,
            restore,
            &name,
            json!({"status": {"phase": "Running", "reason": REASON_STOPPING,
                              "conditions": conditions}}),
        )
        .await?;
        return Ok(Some(handled(restore, Requeue::After(POLL_SECONDS))));
    }
    // No pod is left: the writer is gone. The reconcile decides whether the
    // run created anything.
    let base = json!({"status": {"jobRef": {"name": name}}});
    let Some(run_id) = run_id_of(&run) else {
        patch_status_if_changed(
            &restores,
            restore,
            &name,
            failed_patch(
                restore,
                REASON_CANCELLED,
                "cancelled; the run Job names no run id, so nothing it may have created can be \
                 found; check the target for its topics",
                now,
            ),
        )
        .await?;
        return Ok(Some(handled(restore, Requeue::AwaitChange)));
    };
    patch_status_if_changed(
        &restores,
        restore,
        &name,
        interrupted_patch(base, restore, CAUSE_CANCELLED, Some(&run_id), now),
    )
    .await?;
    Ok(Some(handled(restore, Requeue::After(1))))
}

/// A terminal `Failed` write with `reason`.
fn failed_patch(restore: &Restore, reason: &str, message: &str, now: DateTime<Utc>) -> Value {
    let conditions = crate::conditions::carry_remaining(
        restore.status.as_ref().and_then(|s| s.conditions.as_ref()),
        vec![
            condition(
                restore,
                crate::conditions::CONDITION_FAILED,
                "True",
                reason,
                message,
                now,
            ),
            condition(
                restore,
                CONDITION_INTERRUPTED,
                "False",
                reason,
                message,
                now,
            ),
        ],
    );
    json!({"status": {"phase": "Failed", "reason": reason, "conditions": conditions}})
}

/// The status patch of `interruption` fields alone.
fn interruption_patch(fields: Value) -> Value {
    json!({"status": {"interruption": fields}})
}

async fn patch_interruption(
    restore: &Restore,
    client: &kube::Client,
    namespace: &str,
    fields: Value,
) -> Result<(), RestoreError> {
    let restores: Api<Restore> = Api::namespaced(client.clone(), namespace);
    patch_status_if_changed(
        &restores,
        restore,
        &restore.name_any(),
        interruption_patch(fields),
    )
    .await
    .map(|_| ())
}

/// The reconcile Job's name for one attempt (at most 56 characters).
#[must_use]
pub fn reconcile_job_name(uid: &str, attempt: i64) -> String {
    format!("logweir-reconcile-{uid}-{attempt}")
}

/// The interrupted run's cleanup Job's name for one attempt.
#[must_use]
pub fn cleanup_job_name(uid: &str, attempt: i64) -> String {
    format!("logweir-cleanup-{uid}-{attempt}")
}

/// **The reconcile's argv**: the run's own, with `reconcile` for `run` and
/// without the run's line token (the Job gets its own). The run's `--run-id`
/// names the interrupted run. `None` when the argv is not a `restore run`.
#[must_use]
pub fn reconcile_argv(run_args: &[String]) -> Option<Vec<String>> {
    if run_args.first().map(String::as_str) != Some("restore")
        || run_args.get(1).map(String::as_str) != Some("run")
        || !run_args.iter().any(|a| a == RUN_ID_ARG)
    {
        return None;
    }
    let mut argv = Vec::with_capacity(run_args.len());
    let mut skip = false;
    for (i, a) in run_args.iter().enumerate() {
        if skip {
            skip = false;
            continue;
        }
        if a == logweir_core::runner_line::RUNNER_LINE_TOKEN_ARG
            || a == logweir_core::refusal_detail::LINE_TOKEN_ARG
        {
            skip = true;
            continue;
        }
        argv.push(if i == 1 {
            "reconcile".to_string()
        } else {
            a.clone()
        });
    }
    Some(argv)
}

fn runner_args(job: &Job) -> Vec<String> {
    job.spec
        .as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .and_then(|p| p.containers.iter().find(|c| c.name == job::CONTAINER_NAME))
        .and_then(|c| c.args.clone())
        .unwrap_or_default()
}

fn owner(restore: &Restore) -> crate::job::RunnerOwner {
    crate::job::RunnerOwner {
        api_version: Restore::api_version(&()).to_string(),
        kind: Restore::kind(&()).to_string(),
        name: restore.name_any(),
        uid: restore.uid().unwrap_or_default(),
    }
}

/// Create `job` (or find this `Restore`'s Job of that name). The inner `Err`
/// is a reason to record.
async fn create_owned(
    jobs: &Api<Job>,
    restore: &Restore,
    mut job: Job,
) -> Result<Result<String, String>, RestoreError> {
    let name = job.name_any();
    if let Some(existing) = jobs.get_opt(&name).await.map_err(RestoreError::Api)? {
        return Ok(
            if crate::evidence_fetch::is_owned(&existing, &owner(restore)) {
                Ok(name)
            } else {
                Err(format!("Job {name} exists and is not this Restore's"))
            },
        );
    }
    // A fresh line token for this Job's runner (FX-43).
    let _ = job::add_fresh_line_token(&mut job);
    match jobs.create(&PostParams::default(), &job).await {
        Ok(_) => Ok(Ok(name)),
        Err(kube::Error::Api(e)) if e.code == 409 => Ok(Ok(name)),
        Err(e) => Err(RestoreError::Api(e)),
    }
}

/// Build and create reconcile attempt `attempt`, a copy of the run Job.
async fn start_reconcile(
    restore: &Restore,
    jobs: &Api<Job>,
    attempt: i64,
) -> Result<Result<String, String>, RestoreError> {
    let name = restore.name_any();
    let Some(run) = jobs.get_opt(&name).await.map_err(RestoreError::Api)? else {
        return Ok(Err(format!(
            "the run Job {name}, which the reconcile Job is copied from, is gone"
        )));
    };
    let Some(argv) = reconcile_argv(&runner_args(&run)) else {
        return Ok(Err(
            "the run Job's argv is not a `restore run` naming its run id".to_string(),
        ));
    };
    let job_name = reconcile_job_name(&restore.uid().unwrap_or_default(), attempt);
    let Some(mut copy) = restore_clone::cleanup_job(&run, &job_name, argv, false, None) else {
        return Ok(Err(
            "the run Job has no runner container to copy".to_string()
        ));
    };
    // The reconcile reads the whole restored target: the run's own deadline.
    if let Some(spec) = copy.spec.as_mut() {
        spec.active_deadline_seconds = run.spec.as_ref().and_then(|s| s.active_deadline_seconds);
    }
    create_owned(jobs, restore, copy).await
}

#[allow(clippy::too_many_arguments)]
/// Build and create cleanup attempt `attempt`: a clone cleanup's wiring
/// (`restore_clone::rewire`), its argv naming the reconcile's run.
async fn start_cleanup(
    restore: &Restore,
    client: &kube::Client,
    jobs: &Api<Job>,
    namespace: &str,
    reconcile_run_id: &str,
    attempt: i64,
    image: Option<&str>,
    now: DateTime<Utc>,
) -> Result<Result<String, String>, RestoreError> {
    let name = restore.name_any();
    let Some(run) = jobs.get_opt(&name).await.map_err(RestoreError::Api)? else {
        return Ok(Err(format!(
            "the run Job {name}, which the cleanup Job is copied from, is gone"
        )));
    };
    let Some(argv) = restore_clone::cleanup_argv(&runner_args(&run), reconcile_run_id, attempt)
    else {
        return Ok(Err(
            "the run Job's argv names no plan or signing key to copy".to_string(),
        ));
    };
    let job_name = cleanup_job_name(&restore.uid().unwrap_or_default(), attempt);
    let Some(copy) = restore_clone::cleanup_job(&run, &job_name, argv, false, None) else {
        return Ok(Err(
            "the run Job has no runner container to copy".to_string()
        ));
    };
    let wiring = match restore_clone::evidence_access(restore, client, namespace, now)
        .await
        .map_err(RestoreError::Api)?
    {
        Ok(None) => restore_clone::Wiring::inline(),
        Ok(Some(env)) => restore_clone::Wiring {
            read_grant: env.read_grant.clone(),
            env: Some(env),
        },
        Err(refusal) => {
            return Ok(Err(format!(
                "the cleanup Job could not be given the evidence destination's grants \
                 ({refusal}); nothing was deleted"
            )))
        }
    };
    let image = image.unwrap_or(job::RUNNER_IMAGE);
    create_owned(jobs, restore, restore_clone::rewire(copy, &wiring, image)).await
}

/// One ended Job: its exit code, its pod, and its own lines.
struct Ended {
    exit_code: Option<i32>,
    pod: Option<k8s_openapi::api::core::v1::Pod>,
    log: Option<String>,
}

async fn read_ended(
    client: &kube::Client,
    namespace: &str,
    job: &Job,
) -> Result<Ended, RestoreError> {
    let job_name = job.name_any();
    let found = crate::check::pod::find_owned_pod_by_selectors(
        client,
        namespace,
        &job_name,
        job.uid().as_deref(),
        &crate::controllers::backup::pod_selectors(&job_name),
    )
    .await
    .map_err(RestoreError::Api)?;
    let exit_code = found
        .pod
        .as_ref()
        .and_then(crate::controllers::backup::terminated_exit_code);
    let log = match found.pod.as_ref() {
        Some(pod) => {
            let pods: Api<k8s_openapi::api::core::v1::Pod> =
                Api::namespaced(client.clone(), namespace);
            match crate::runner_log::read_own_terminal(&pods, &pod.name_any(), job, exit_code)
                .await
                .map_err(RestoreError::Api)?
            {
                crate::runner_log::Terminal::Lines(lines) => Some(lines.text().to_string()),
                _ => None,
            }
        }
        None => None,
    };
    Ok(Ended {
        exit_code,
        pod: found.pod,
        log,
    })
}

/// One step of an `Interrupted` restore.
async fn advance(
    restore: &Restore,
    client: &kube::Client,
    namespace: &str,
    now: DateTime<Utc>,
    runner: &job::RunnerImage,
    reads: &Reread<'_>,
) -> Result<RestoreOutcome, RestoreError> {
    let i = interruption(restore);
    match i.and_then(|i| i.verification.as_deref()) {
        Some(VERIFICATION_DONE) => {
            let cleanup = cleanup_step(restore, client, namespace, now, runner).await?;
            if !matches!(cleanup, Requeue::AwaitChange) {
                return Ok(handled(restore, cleanup));
            }
            // The reconcile's own evidence verdict, when a fetch Job reads it.
            let requeue =
                super::continue_evidence_fetch(restore, client, namespace, now, runner, reads)
                    .await?;
            Ok(handled(restore, requeue))
        }
        Some(VERIFICATION_FAILED) => Ok(handled(restore, Requeue::AwaitChange)),
        _ => reconcile_step(restore, client, namespace, now, runner, reads).await,
    }
}

async fn reconcile_step(
    restore: &Restore,
    client: &kube::Client,
    namespace: &str,
    now: DateTime<Utc>,
    runner: &job::RunnerImage,
    reads: &Reread<'_>,
) -> Result<RestoreOutcome, RestoreError> {
    let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
    let i = interruption(restore);
    let attempts = i.and_then(|i| i.attempts).unwrap_or(0);
    let current = i.and_then(|i| i.job_ref.as_ref()).map(|r| r.name.clone());
    let Some(job_name) = current.filter(|_| attempts > 0) else {
        return start_attempt(restore, client, namespace, &jobs, 1, None).await;
    };
    let Some(job) = jobs.get_opt(&job_name).await.map_err(RestoreError::Api)? else {
        return failed_attempt(
            restore,
            client,
            namespace,
            &jobs,
            attempts,
            format!("reconcile Job {job_name} is gone before it was read"),
            now,
        )
        .await;
    };
    if !crate::controllers::backup::job_finished(&job) {
        return Ok(handled(restore, Requeue::After(POLL_SECONDS)));
    }
    let ended = read_ended(client, namespace, &job).await?;
    let log = ended.log.as_deref().unwrap_or_default();
    match ended.exit_code {
        Some(0) if !crate::runner_log::key_values(log, RECONCILE_NONE_PREFIX).is_empty() => {
            let why = crate::runner_log::key_values(log, RECONCILE_NONE_PREFIX)
                .pop()
                .map(logweir_core::refusal_detail::clean_message)
                .unwrap_or_default();
            let reason = match i.and_then(|i| i.cause.as_deref()) {
                Some(CAUSE_CANCELLED) => REASON_CANCELLED,
                _ => crate::conditions::CONDITION_REASON_OPERATIONAL,
            };
            let mut patch = failed_patch(
                restore,
                reason,
                &format!("the run ended without a signed scorecard and left nothing: {why}"),
                now,
            );
            patch["status"]["interruption"] = json!({
                "verification": VERIFICATION_DONE,
                "verificationReason": bounded(&why),
            });
            let restores: Api<Restore> = Api::namespaced(client.clone(), namespace);
            patch_status_if_changed(&restores, restore, &restore.name_any(), patch).await?;
            Ok(handled(restore, Requeue::AwaitChange))
        }
        Some(code @ (0 | 2)) if ended.pod.is_some() => {
            let pod = ended.pod.as_ref().expect("checked");
            let interrupted_run = i.and_then(|i| i.run_id.clone()).unwrap_or_default();
            let view = ReconcileEnd {
                interrupted_run,
                exit_code: code,
                now,
            };
            finish_job(
                restore,
                client,
                namespace,
                &job,
                pod,
                code,
                reads,
                now,
                runner,
                super::Finish::Reconcile(&view),
            )
            .await
        }
        Some(3) => {
            let why = log
                .lines()
                .rev()
                .find_map(|l| l.strip_prefix("refusal-detail="))
                .map(logweir_core::refusal_detail::clean_message)
                .unwrap_or_else(|| "the reconcile refused (exit 3)".to_string());
            patch_interruption(
                restore,
                client,
                namespace,
                json!({"verification": VERIFICATION_FAILED,
                       "verificationReason": bounded(&why)}),
            )
            .await?;
            Ok(handled(restore, Requeue::AwaitChange))
        }
        code => {
            let how = code.map_or_else(
                || "the reconcile Job ended with no exit code".to_string(),
                |c| format!("the reconcile Job exited {c}"),
            );
            let detail = log
                .lines()
                .rev()
                .find_map(|l| l.strip_prefix("failure-detail="))
                .map(logweir_core::refusal_detail::clean_message);
            let why = match detail {
                Some(d) => format!("{how}: {d}"),
                None => how,
            };
            let ended_at = crate::diagnostics::job_ended_at(&job).unwrap_or(now);
            if attempts < MAX_ATTEMPTS
                && now < ended_at + chrono::Duration::seconds(RETRY_AFTER_SECONDS)
            {
                patch_interruption(
                    restore,
                    client,
                    namespace,
                    json!({"verificationReason": bounded(&why)}),
                )
                .await?;
                let wait = (ended_at + chrono::Duration::seconds(RETRY_AFTER_SECONDS) - now)
                    .num_seconds()
                    .max(1);
                return Ok(handled(
                    restore,
                    Requeue::After(u64::try_from(wait).unwrap_or(1)),
                ));
            }
            failed_attempt(restore, client, namespace, &jobs, attempts, why, now).await
        }
    }
}

/// What a reconcile Job's end means to [`super::finish_job`].
pub(super) struct ReconcileEnd {
    pub(super) interrupted_run: String,
    pub(super) exit_code: i32,
    pub(super) now: DateTime<Utc>,
}

impl ReconcileEnd {
    /// **The terminal write of a run's finish, as the reconcile's**: the
    /// run's own scorecard (relayed) decides as it would have; a reconcile
    /// scorecard that passed is `Succeeded`, `ReconciledComplete`; one that
    /// did not keeps the `Interrupted` phase, its verification `Done`.
    pub(super) fn adjust(
        &self,
        restore: &Restore,
        mut patch: Value,
        keys: &RestoreEvidenceKeys,
    ) -> Value {
        let run_id = keys
            .scorecard
            .as_deref()
            .and_then(restore_clone::run_id_of)
            .unwrap_or_default();
        let relayed = run_id == self.interrupted_run;
        let Some(status) = patch.get_mut("status").and_then(Value::as_object_mut) else {
            return patch;
        };
        if relayed {
            status.insert(
                "interruption".into(),
                json!({"verification": VERIFICATION_DONE,
                       "verificationReason": "the run signed its scorecard before it stopped; \
                                              the scorecard decides"}),
            );
            return patch;
        }
        status.insert(
            "interruption".into(),
            json!({"verification": VERIFICATION_DONE, "verificationReason": null,
                   "reconcileRunId": run_id}),
        );
        let message = if self.exit_code == 0 {
            "the reconcile verified every partition the interrupted run left as exact; nothing \
             was restored again"
        } else {
            "the reconcile signed what the interrupted run left (per-partition counts on \
             status.integrity); nothing was resumed. Retry into new topics, or clean them up"
        };
        let (phase, reason, cond_status) = if self.exit_code == 0 {
            ("Succeeded", REASON_RECONCILED_COMPLETE, "False")
        } else {
            (PHASE_INTERRUPTED, CAUSE_RUN_FAILED, "True")
        };
        let reason = if self.exit_code == 0 {
            reason.to_string()
        } else {
            interruption(restore)
                .and_then(|i| i.cause.clone())
                .unwrap_or_else(|| reason.to_string())
        };
        status.insert("phase".into(), json!(phase));
        status.insert("reason".into(), json!(reason));
        let mut conditions: Vec<Value> = status
            .get("conditions")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        conditions.retain(|c| c.get("type") != Some(&json!(CONDITION_INTERRUPTED)));
        conditions.insert(
            0,
            condition(
                restore,
                CONDITION_INTERRUPTED,
                cond_status,
                &reason,
                message,
                self.now,
            ),
        );
        status.insert("conditions".into(), json!(conditions));
        patch
    }
}

async fn start_attempt(
    restore: &Restore,
    client: &kube::Client,
    namespace: &str,
    jobs: &Api<Job>,
    attempt: i64,
    reason: Option<String>,
) -> Result<RestoreOutcome, RestoreError> {
    match start_reconcile(restore, jobs, attempt).await? {
        Ok(job_name) => {
            let mut fields = json!({"attempts": attempt, "jobRef": {"name": job_name},
                                    "verification": VERIFICATION_RUNNING});
            if let Some(why) = reason {
                fields["verificationReason"] = json!(bounded(&why));
            }
            patch_interruption(restore, client, namespace, fields).await?;
            Ok(handled(restore, Requeue::After(POLL_SECONDS)))
        }
        Err(why) => {
            patch_interruption(
                restore,
                client,
                namespace,
                json!({"attempts": attempt, "verification": VERIFICATION_FAILED,
                       "verificationReason": bounded(&why)}),
            )
            .await?;
            Ok(handled(restore, Requeue::AwaitChange))
        }
    }
}

async fn failed_attempt(
    restore: &Restore,
    client: &kube::Client,
    namespace: &str,
    jobs: &Api<Job>,
    attempts: i64,
    why: String,
    _now: DateTime<Utc>,
) -> Result<RestoreOutcome, RestoreError> {
    if attempts >= MAX_ATTEMPTS {
        patch_interruption(
            restore,
            client,
            namespace,
            json!({"verification": VERIFICATION_FAILED,
                   "verificationReason": bounded(&format!(
                       "{why}; {attempts} attempts failed, so the partial targets are not \
                        verified and the recovery point stays held. Fix the cause and create a \
                        new Restore into new topics; the topics the run created are left"))}),
        )
        .await?;
        return Ok(handled(restore, Requeue::AwaitChange));
    }
    start_attempt(restore, client, namespace, jobs, attempts + 1, Some(why)).await
}

/// The cleanup, once asked for after a signed reconcile. `AwaitChange` when
/// there is nothing to do.
async fn cleanup_step(
    restore: &Restore,
    client: &kube::Client,
    namespace: &str,
    now: DateTime<Utc>,
    runner: &job::RunnerImage,
) -> Result<Requeue, RestoreError> {
    let Some(by) = annotation(restore, CLEANUP_ANNOTATION) else {
        return Ok(Requeue::AwaitChange);
    };
    let i = interruption(restore);
    let Some(reconcile_run) = i.and_then(|i| i.reconcile_run_id.clone()) else {
        return Ok(Requeue::AwaitChange);
    };
    let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
    let cleanup = i.and_then(|i| i.cleanup.as_ref());
    let attempts = cleanup.and_then(|c| c.attempts).unwrap_or(0);
    let image = runner.image.as_deref();
    match cleanup.and_then(|c| c.phase.as_deref()) {
        Some(CLEANUP_REMOVED | CLEANUP_FAILED) => Ok(Requeue::AwaitChange),
        Some(CLEANUP_REMOVING) => {
            let Some(job_name) = cleanup
                .and_then(|c| c.job_ref.as_ref())
                .map(|r| r.name.clone())
            else {
                return Ok(Requeue::AwaitChange);
            };
            let Some(job) = jobs.get_opt(&job_name).await.map_err(RestoreError::Api)? else {
                patch_interruption(
                    restore,
                    client,
                    namespace,
                    json!({"cleanup": {"phase": CLEANUP_FAILED,
                        "reason": format!("cleanup Job {job_name} is gone before it was read")}}),
                )
                .await?;
                return Ok(Requeue::AwaitChange);
            };
            if !crate::controllers::backup::job_finished(&job) {
                return Ok(Requeue::After(POLL_SECONDS));
            }
            let ended = read_ended(client, namespace, &job).await?;
            let report = ended.log.as_deref().and_then(|log| {
                crate::runner_log::key_values(
                    log,
                    logweir_core::target_creation::CLONE_CLEANUP_PREFIX,
                )
                .pop()
                .and_then(logweir_core::target_creation::CloneCleanupReport::parse)
            });
            let earlier: Vec<String> = restore
                .status
                .as_ref()
                .and_then(|s| s.teardown.as_ref())
                .and_then(|t| t.deleted.clone())
                .unwrap_or_default();
            let earlier_count = restore
                .status
                .as_ref()
                .and_then(|s| s.teardown.as_ref())
                .and_then(|t| t.deleted_count);
            let restores: Api<Restore> = Api::namespaced(client.clone(), namespace);
            let write = |cleanup: Value, teardown: Option<Value>| {
                let mut status = json!({"interruption": {"cleanup": cleanup}});
                if let Some(t) = teardown {
                    status["teardown"] = t;
                }
                json!({ "status": status })
            };
            match restore_clone::ended(ended.exit_code, report) {
                restore_clone::Ended::Removed(report) => {
                    patch_status_if_changed(
                        &restores,
                        restore,
                        &restore.name_any(),
                        write(
                            json!({"phase": CLEANUP_REMOVED, "reason": null}),
                            Some(restore_clone::teardown_value(
                                &report,
                                &earlier,
                                earlier_count,
                            )),
                        ),
                    )
                    .await?;
                    Ok(Requeue::AwaitChange)
                }
                restore_clone::Ended::Refused(why) => {
                    patch_status_if_changed(
                        &restores,
                        restore,
                        &restore.name_any(),
                        write(
                            json!({"phase": CLEANUP_FAILED, "reason": bounded(&why)}),
                            None,
                        ),
                    )
                    .await?;
                    Ok(Requeue::AwaitChange)
                }
                restore_clone::Ended::Failed(why, report) => {
                    let teardown = report
                        .as_ref()
                        .map(|r| restore_clone::teardown_value(r, &earlier, earlier_count));
                    if attempts >= MAX_ATTEMPTS {
                        patch_status_if_changed(
                            &restores,
                            restore,
                            &restore.name_any(),
                            write(
                                json!({"phase": CLEANUP_FAILED, "reason": bounded(&format!(
                                    "{why}; {attempts} attempts failed; delete the topics the \
                                     reconcile's scorecard lists under target.creation yourself"))}),
                                teardown,
                            ),
                        )
                        .await?;
                        return Ok(Requeue::AwaitChange);
                    }
                    let ended_at = crate::diagnostics::job_ended_at(&job).unwrap_or(now);
                    let next = ended_at + chrono::Duration::seconds(RETRY_AFTER_SECONDS);
                    if now < next {
                        return Ok(Requeue::After(
                            u64::try_from((next - now).num_seconds())
                                .unwrap_or(1)
                                .max(1),
                        ));
                    }
                    let started = start_cleanup(
                        restore,
                        client,
                        &jobs,
                        namespace,
                        &reconcile_run,
                        attempts + 1,
                        image,
                        now,
                    )
                    .await?;
                    let cleanup = match started {
                        Ok(job_name) => json!({"phase": CLEANUP_REMOVING, "attempts": attempts + 1,
                                               "jobRef": {"name": job_name},
                                               "reason": bounded(&why)}),
                        Err(why) => json!({"phase": CLEANUP_FAILED, "attempts": attempts + 1,
                                           "reason": bounded(&why)}),
                    };
                    patch_status_if_changed(
                        &restores,
                        restore,
                        &restore.name_any(),
                        write(cleanup, teardown),
                    )
                    .await?;
                    Ok(Requeue::After(POLL_SECONDS))
                }
            }
        }
        _ => {
            let started = start_cleanup(
                restore,
                client,
                &jobs,
                namespace,
                &reconcile_run,
                1,
                image,
                now,
            )
            .await?;
            let cleanup = match started {
                Ok(job_name) => json!({"phase": CLEANUP_REMOVING, "attempts": 1,
                                       "requestedBy": bounded(by),
                                       "jobRef": {"name": job_name}}),
                Err(why) => json!({"phase": CLEANUP_FAILED, "attempts": 1,
                                   "requestedBy": bounded(by), "reason": bounded(&why)}),
            };
            patch_interruption(restore, client, namespace, json!({ "cleanup": cleanup })).await?;
            Ok(Requeue::After(POLL_SECONDS))
        }
    }
}

/// **Whether an interrupted restore still holds its recovery point**: until
/// its cleanup is `Removed`.
#[must_use]
pub fn holds_point(restore: &Restore) -> bool {
    phase_of(restore) == Some(PHASE_INTERRUPTED)
        && interruption(restore)
            .and_then(|i| i.cleanup.as_ref())
            .and_then(|c| c.phase.as_deref())
            != Some(CLEANUP_REMOVED)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_args() -> Vec<String> {
        [
            "restore",
            "run",
            "--spec",
            "/plan/restore.yaml",
            "--signing-key",
            "/signing/key.pem",
            "--run-id",
            "01RUN",
            "--runner-line-token",
            "abc",
        ]
        .iter()
        .map(|s| (*s).to_string())
        .collect()
    }

    /// The reconcile runs the run's own flags as `restore reconcile`, keeps
    /// the run id, and drops the run's line token (its Job gets its own).
    #[test]
    fn the_reconcile_argv_is_the_runs_own_as_reconcile() {
        let argv = reconcile_argv(&run_args()).expect("a run argv");
        assert_eq!(argv[..2], ["restore".to_string(), "reconcile".to_string()]);
        assert!(argv.windows(2).any(|w| w == ["--run-id", "01RUN"]));
        assert!(!argv
            .iter()
            .any(|a| a == "--runner-line-token" || a == "abc"));
        let mut no_id = run_args();
        no_id.drain(6..8);
        assert!(reconcile_argv(&no_id).is_none(), "no run id, no reconcile");
    }

    /// A fresh run id is a ULID, distinct per call.
    #[test]
    fn run_ids_are_fresh() {
        let now = chrono::Utc::now();
        let a = new_run_id(now).expect("random");
        let b = new_run_id(now).expect("random");
        assert_eq!(a.len(), 26);
        assert_ne!(a, b);
        assert!(logweir_core::ids::is_run_id_segment(&a));
    }
}
