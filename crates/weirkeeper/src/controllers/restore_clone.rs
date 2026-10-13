//! **PROD-11.1a: a TTL clone's lifetime, its cleanup Job and its finalizer**
//! (design `docs/to-do/decisions/PROD-04.2-cutover.md` §5).
//!
//! A `Restore` whose approved plan is a scratch restore with `target.teardown:
//! {ttl_seconds: N}` is a CLONE. This module is the controller's whole part in
//! it, reached through ONE call, [`around`], which wraps every reconcile of a
//! `Restore`:
//!
//! 1. **Admission**: the plan's clone shape is refused by name
//!    (`CloneTtlOutOfRange`, `CloneTtlNeedsScratch`), and a clone gets the
//!    finalizer `logweir.dev/clone-cleanup` before its run Job exists, so a
//!    deletion during the run still cleans up.
//! 2. **After the run**: a passing clone is `Active` until `expiresAt` (the
//!    success the controller recorded, plus N). Its cleanup Job is built then,
//!    SUSPENDED, as a copy of the run Job's pod (the same plan, signing key,
//!    target credential and evidence store), with the argv of `logweir restore
//!    cleanup`; a clone that did not pass was torn down at once by its own run
//!    and is `NotKept`.
//! 3. **At `expiresAt`, or when the `Restore` is deleted first**: the Job is
//!    resumed (`Removing`). Exit 0 is `Removed`, its `clone-cleanup=` line
//!    copied to `status.teardown`, and the finalizer released; exit 3 (a
//!    refusal, nothing deleted) is `RemovalFailed` at once; any other end is
//!    retried, at most three attempts five minutes apart, then
//!    `RemovalFailed`. The finalizer stays on a `RemovalFailed` clone until an
//!    operator removes it.
//!
//! **The finalizer fails closed.** Once on, it is released ONLY on a settled
//! clone phase (`Removed`; `NotKept`, which needs the runner's own exit code,
//! 1 to 4, or no run Job ever recorded) or on the recorded proof that no run Job was
//! created: neither `status.jobRef` (written in the pass that creates the run
//! Job) nor `status.exitCode`. The absence of a Job is never that proof. A
//! plan this controller cannot read, a clone shape it refuses, a recorded run
//! Job that is gone before its outcome, or a run that ended with no exit code
//! is `RemovalFailed` with the reason and the manual step, and keeps the
//! finalizer. A finalizer on a plan that parses and is not a clone is
//! released on deletion.
//!
//! THE CONTROLLER DELETES NO TOPIC. It starts a runner Job; the runner
//! deletes only what the run's signed scorecard records, on the cluster it
//! records, while each topic's ID is the recorded one.

use super::{parse_plan, status_is_terminal, Requeue, RestoreError, RestoreOutcome};
use crate::crds::restore::Restore;
use chrono::{DateTime, Utc};
use k8s_openapi::api::batch::v1::Job;
use kube::api::{Api, Patch, PatchParams, PostParams};
use kube::{Resource as _, ResourceExt as _};
use logweir_core::spec::TargetMode;
use logweir_core::target_creation::{CloneCleanupReport, CLONE_CLEANUP_PREFIX};
use serde_json::{json, Value};

/// The finalizer a TTL clone's `Restore` carries until its cleanup is done.
pub const FINALIZER: &str = "logweir.dev/clone-cleanup";
/// At most this many cleanup Jobs per clone.
pub const MAX_ATTEMPTS: i64 = 3;
/// How long after a failed attempt the next one starts.
pub const RETRY_AFTER_SECONDS: i64 = 300;
/// A cleanup Job's `activeDeadlineSeconds`, counted from its resumption.
pub const CLEANUP_DEADLINE_SECONDS: i64 = 600;
/// How often a running cleanup is looked at again.
pub const POLL_SECONDS: u64 = 30;

/// The run passed and its topics are kept until `expiresAt`.
pub const PHASE_ACTIVE: &str = "Active";
/// A cleanup Job is running.
pub const PHASE_REMOVING: &str = "Removing";
/// The cleanup deleted what the record named.
pub const PHASE_REMOVED: &str = "Removed";
/// The cleanup refused, or failed three times; the finalizer stays.
pub const PHASE_REMOVAL_FAILED: &str = "RemovalFailed";
/// The run did not pass: its own run tore the topics down at once.
pub const PHASE_NOT_KEPT: &str = "NotKept";

/// What the approved plan says this `Restore` is, as far as clones go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    /// The plan parses and is not a clone (`target.teardown` is a policy).
    NotClone,
    /// A TTL clone: a scratch plan with `target.teardown: {ttl_seconds: N}`
    /// within its bounds.
    Clone(u64),
    /// The plan declares the clone form in a shape the runner refuses
    /// (`CloneTtlOutOfRange`, `CloneTtlNeedsScratch`), in the runner's words.
    Refused(String),
    /// The plan does not parse in this controller (version skew, a decode
    /// change): whether it is a clone is NOT known.
    Unreadable(String),
}

/// Classify the approved plan. Pure. `Unreadable` and `Refused` are never
/// read as "not a clone": a finalizer put there because the object WAS a
/// clone is never released on them.
#[must_use]
pub fn classify(restore: &Restore) -> Plan {
    let plan = match parse_plan(restore) {
        Ok(plan) => plan,
        Err(e) => {
            return Plan::Unreadable(format!(
                "spec.planBytes does not parse in this controller ({e})"
            ))
        }
    };
    let Some(ttl) = plan.target.teardown.ttl_seconds() else {
        return Plan::NotClone;
    };
    match logweir_core::target_creation::refuse_teardown_shape(&plan) {
        Some(why) => Plan::Refused(why),
        None if plan.target.mode == TargetMode::Scratch => Plan::Clone(ttl),
        None => Plan::Refused("the clone form outside scratch mode".to_string()),
    }
}

/// The clone shape the runner refuses at phase 0, refused here before any
/// Job exists, in the runner's words.
fn shape_refusal(why: &str) -> RestoreError {
    RestoreError::Refused(
        crate::conditions::TERMINAL_STATE_EXECUTION_SPEC_INVALID,
        format!(
            "{why}; no Job was created. spec is immutable and the approval binds the plan \
             bytes — create a new Restore with a plan the runner accepts"
        ),
    )
}

/// **Whether the controller ever recorded a run Job for this `Restore`.**
/// `status.jobRef` is written in the pass that creates the run Job (and by
/// every later pass that observes it), and `status.exitCode` only from a
/// Job's pod. Neither recorded is the positive proof that no run Job was
/// created; the ABSENCE OF A JOB is never that proof (it may have been
/// deleted, or collected by a foreground deletion of this object).
#[must_use]
pub fn run_recorded(restore: &Restore) -> bool {
    restore
        .status
        .as_ref()
        .is_some_and(|s| s.job_ref.is_some() || s.exit_code.is_some())
}

/// What a clone being deleted before its run's outcome is recorded may do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OnDelete {
    /// No run Job was ever recorded and none exists: nothing was created, so
    /// the finalizer is released.
    Release,
    /// The run Job exists: the reconcile observes it end, then the clone's
    /// lifetime decides.
    Observe,
    /// A run Job was recorded and is gone before its outcome was: whether it
    /// created topics is unknown. `RemovalFailed`, finalizer kept.
    Hold(String),
}

/// **The release decision for a clone deleted before its run ended.** Pure;
/// `run_job_exists` is the API server's answer for the run Job (a read that
/// failed is an error, never "absent").
#[must_use]
pub fn on_delete(restore: &Restore, run_job_exists: bool) -> OnDelete {
    if run_job_exists {
        OnDelete::Observe
    } else if !run_recorded(restore) {
        OnDelete::Release
    } else {
        OnDelete::Hold(format!(
            "the Restore was deleted before its run's outcome was recorded, and its run Job \
             {} is gone, so whether the run created topics is unknown; nothing was deleted. \
             Look for topics with the clone's prefix on the target, delete any this run \
             created, then remove the finalizer {FINALIZER}",
            restore
                .status
                .as_ref()
                .and_then(|s| s.job_ref.as_ref())
                .map_or_else(|| restore.name_any(), |r| r.name.clone())
        ))
    }
}

/// The cleanup Job's name for one attempt: `logweir-clone-cleanup-<uid>-<n>`,
/// at most 60 characters, so its pods' `job-name` label fits.
#[must_use]
pub fn cleanup_job_name(uid: &str, attempt: i64) -> String {
    format!("logweir-clone-cleanup-{uid}-{attempt}")
}

/// The run id a scorecard key names (`logweir/drills/<run_id>.json`).
#[must_use]
pub fn run_id_of(scorecard_key: &str) -> Option<String> {
    let id = scorecard_key.rsplit('/').next()?.strip_suffix(".json")?;
    (!id.is_empty()
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'))
    .then(|| id.to_string())
}

/// `logweir restore cleanup`'s argv, from the run Job's own: the same
/// `--spec`, `--signing-key`, and, when the run had them,
/// `--evidence-keys` and `--store-contract-version`. `None` when the run's
/// argv names no plan or no signing key.
#[must_use]
pub fn cleanup_argv(run_args: &[String], run_id: &str, attempt: i64) -> Option<Vec<String>> {
    let value = |flag: &str| -> Option<&String> {
        run_args
            .iter()
            .position(|a| a == flag)
            .and_then(|i| run_args.get(i + 1))
    };
    let mut argv = vec!["restore".to_string(), "cleanup".to_string()];
    for (flag, required) in [
        ("--spec", true),
        ("--signing-key", true),
        ("--evidence-keys", false),
        ("--store-contract-version", false),
    ] {
        match value(flag) {
            Some(v) => argv.extend([flag.to_string(), v.clone()]),
            None if required => return None,
            None => {}
        }
    }
    argv.extend([
        "--run-id".to_string(),
        run_id.to_string(),
        "--attempt".to_string(),
        attempt.to_string(),
    ]);
    Some(argv)
}

/// The labels the Job controller writes on a Job's pod template and that a
/// copy must not carry: the API server refuses a selector it did not make.
const JOB_CONTROLLER_LABELS: [&str; 4] = [
    "controller-uid",
    "batch.kubernetes.io/controller-uid",
    "job-name",
    "batch.kubernetes.io/job-name",
];

/// **The cleanup Job for one attempt**, as a copy of the run Job: the same
/// owner (the `Restore`), ServiceAccount, volumes and environment — the plan,
/// the signing key, the target credential and the evidence store the run
/// used — with `logweir restore cleanup`'s argv, the current runner image
/// when one is configured, no selector or controller labels, its own
/// deadline, and `suspend` as asked. Pure.
#[must_use]
pub fn cleanup_job(
    run_job: &Job,
    name: &str,
    argv: Vec<String>,
    suspend: bool,
    image: Option<&str>,
) -> Option<Job> {
    let mut spec = run_job.spec.clone()?;
    spec.selector = None;
    spec.manual_selector = None;
    spec.suspend = Some(suspend);
    spec.ttl_seconds_after_finished = None;
    spec.active_deadline_seconds = Some(CLEANUP_DEADLINE_SECONDS);
    if let Some(meta) = spec.template.metadata.as_mut() {
        if let Some(labels) = meta.labels.as_mut() {
            for key in JOB_CONTROLLER_LABELS {
                labels.remove(key);
            }
        }
        meta.annotations = None;
    }
    let pod = spec.template.spec.as_mut()?;
    let container = pod
        .containers
        .iter_mut()
        .find(|c| c.name == crate::job::CONTAINER_NAME)?;
    container.args = Some(argv);
    if let Some(image) = image {
        container.image = Some(image.to_string());
    }
    let mut labels = run_job.metadata.labels.clone().unwrap_or_default();
    for key in JOB_CONTROLLER_LABELS {
        labels.remove(key);
    }
    Some(Job {
        metadata: kube::api::ObjectMeta {
            name: Some(name.to_string()),
            namespace: run_job.metadata.namespace.clone(),
            labels: Some(labels),
            owner_references: run_job.metadata.owner_references.clone(),
            ..kube::api::ObjectMeta::default()
        },
        spec: Some(spec),
        status: None,
    })
}

/// What a clone needs next, from its stored status alone. Pure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// The run has not ended: nothing yet.
    Wait,
    /// The run did not pass and its runner settled what it created (it
    /// exited, tearing down and naming what it left), or no run Job was ever
    /// recorded: record `NotKept` and release the finalizer.
    NotKept,
    /// The clone cannot be settled: record `RemovalFailed` with this reason;
    /// the finalizer stays.
    Fail(String),
    /// The run passed and nothing is recorded: keep it until this instant.
    Keep(DateTime<Utc>),
    /// Kept, and not due before this instant.
    Until(DateTime<Utc>),
    /// Due: start attempt `n`.
    Start(i64),
    /// A cleanup Job is running or has ended: look at it.
    Watch,
    /// Removed: release the finalizer.
    Release,
    /// `RemovalFailed`: nothing; the finalizer stays.
    Hold,
}

/// **The clone's state machine over its stored status.** `deleting`: the
/// `Restore` is being deleted, which makes a kept clone due at once.
#[must_use]
pub fn step(restore: &Restore, ttl: u64, deleting: bool, now: DateTime<Utc>) -> Step {
    let status = restore.status.as_ref();
    if !status_is_terminal(restore) {
        return Step::Wait;
    }
    let clone = status.and_then(|s| s.clone.as_ref());
    let phase = clone.and_then(|c| c.phase.as_deref());
    match phase {
        Some(PHASE_REMOVED) | Some(PHASE_NOT_KEPT) => return Step::Release,
        Some(PHASE_REMOVAL_FAILED) => return Step::Hold,
        Some(PHASE_REMOVING) => return Step::Watch,
        _ => {}
    }
    let passed =
        status.is_some_and(|s| s.phase.as_deref() == Some("Succeeded") && s.exit_code == Some(0));
    if !passed {
        // Settled only by the runner's own exit, one of its defined codes 1
        // to 4 (its teardown ran and named what it left), or by no run Job
        // ever recorded. A run Job that ended with no exit code, or with a
        // code the runner never returns (137 for an OOM kill or SIGKILL, 143
        // for SIGTERM on a drain or an eviction), never ran its teardown.
        let exited = status.is_some_and(|s| s.exit_code.is_some_and(|c| (1..=4).contains(&c)));
        return if exited || !run_recorded(restore) {
            Step::NotKept
        } else {
            let how = status.and_then(|s| s.exit_code).map_or_else(
                || "without an exit code".to_string(),
                |c| format!("with exit code {c}, which the runner never returns (a signal)"),
            );
            Step::Fail(format!(
                "the run ended {how}, so its own teardown did not run and whatever it created is \
                 still on the target; nothing was deleted. Look for topics with the clone's \
                 prefix, delete any this run created, then remove the finalizer {FINALIZER}"
            ))
        };
    }
    let Some(expires_at) = clone.and_then(|c| c.expires_at) else {
        let ttl = i64::try_from(ttl).unwrap_or(i64::MAX);
        return Step::Keep(now + chrono::Duration::seconds(ttl));
    };
    if deleting || now >= expires_at {
        Step::Start(clone.and_then(|c| c.attempts).unwrap_or(0) + 1)
    } else {
        Step::Until(expires_at)
    }
}

/// What one ended cleanup Job means.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ended {
    /// Exit 0: done.
    Removed(CloneCleanupReport),
    /// Exit 3: refused, nothing deleted; no retry can change it.
    Refused(String),
    /// Any other end, with what is known; retried while attempts remain.
    Failed(String, Option<CloneCleanupReport>),
}

/// Read one ended attempt: its exit code and its `clone-cleanup=` line. Pure.
#[must_use]
pub fn ended(exit_code: Option<i32>, report: Option<CloneCleanupReport>) -> Ended {
    match (exit_code, report) {
        (Some(0), Some(r)) => Ended::Removed(r),
        (Some(3), r) => Ended::Refused(
            r.and_then(|r| r.refused)
                .unwrap_or_else(|| "the cleanup refused (exit 3) and printed no reason".into()),
        ),
        (Some(code), r) => Ended::Failed(format!("the cleanup Job exited {code}"), r),
        (None, r) => Ended::Failed(
            "the cleanup Job ended with no exit code (deadline, eviction or a crash)".into(),
            r,
        ),
    }
}

/// `status.teardown` from a cleanup's report: what it deleted, what it left
/// and why, and the attestation's key. Bounded by the CRD (256 entries, 256
/// characters per reason).
/// `earlier` is what earlier attempts already deleted (the stored
/// `status.teardown.deleted`), kept so a later attempt's list does not
/// replace it.
#[must_use]
pub fn teardown_value(report: &CloneCleanupReport, earlier: &[String]) -> Value {
    let bounded = |s: &str| -> String { s.chars().take(256).collect() };
    let mut t = serde_json::Map::new();
    if let Some(key) = &report.attestation_key {
        t.insert("attestationKey".into(), json!(key));
    }
    let mut deleted: Vec<&String> = earlier.iter().collect();
    for name in &report.deleted {
        if !deleted.contains(&name) {
            deleted.push(name);
        }
    }
    t.insert(
        "deleted".into(),
        json!(deleted.into_iter().take(256).collect::<Vec<_>>()),
    );
    t.insert(
        "failed".into(),
        json!(report
            .left
            .iter()
            .take(256)
            .map(|l| json!({"topic": l.topic, "error": bounded(&l.reason)}))
            .collect::<Vec<_>>()),
    );
    Value::Object(t)
}

fn bounded_reason(reason: &str) -> String {
    reason.chars().take(1024).collect()
}

/// The sooner of two requeues.
fn sooner(a: Requeue, b: Requeue) -> Requeue {
    match (a, b) {
        (Requeue::After(x), Requeue::After(y)) => Requeue::After(x.min(y)),
        (Requeue::After(x), Requeue::AwaitChange) | (Requeue::AwaitChange, Requeue::After(x)) => {
            Requeue::After(x)
        }
        (Requeue::AwaitChange, Requeue::AwaitChange) => Requeue::AwaitChange,
    }
}

fn seconds_until(at: DateTime<Utc>, now: DateTime<Utc>) -> u64 {
    u64::try_from((at - now).num_seconds()).unwrap_or(0).max(1)
}

fn has_finalizer(restore: &Restore) -> bool {
    restore.finalizers().iter().any(|f| f == FINALIZER)
}

/// **The one call** `controllers::restore` makes: the clone's admission
/// before the reconcile, `inner` (the reconcile itself), and the clone's
/// lifetime after it. Every other `Restore` is `inner`, unchanged.
///
/// A `Restore` being deleted is never handed to `inner` while it has no run
/// Job, so deleting a clone never starts its run.
///
/// # Errors
///
/// `inner`'s errors, a refused clone shape, and the API's.
pub async fn around<F>(
    restore: &Restore,
    client: &kube::Client,
    now: DateTime<Utc>,
    image: Option<&str>,
    inner: F,
) -> Result<RestoreOutcome, RestoreError>
where
    F: std::future::Future<Output = Result<RestoreOutcome, RestoreError>>,
{
    let name = restore.name_any();
    let Some(namespace) = restore.namespace() else {
        return inner.await;
    };
    let restores: Api<Restore> = Api::namespaced(client.clone(), &namespace);
    let deleting = restore.meta().deletion_timestamp.is_some();
    let terminal = status_is_terminal(restore);
    let ttl = match classify(restore) {
        Plan::Clone(ttl) => ttl,
        // Not a clone, and the plan says so: a finalizer left from anywhere
        // never blocks its deletion.
        Plan::NotClone => {
            if deleting && has_finalizer(restore) {
                set_finalizer(&restores, restore, false).await?;
            }
            return inner.await;
        }
        // A shape the runner refuses, or a plan this controller cannot read.
        // Without the finalizer it is the ordinary refusal (the reconcile
        // refuses an unreadable plan itself). WITH it, the object was
        // admitted as a clone: fail closed in both directions — nothing is
        // deleted, the finalizer stays, and the status says why.
        Plan::Refused(why) | Plan::Unreadable(why) => {
            if has_finalizer(restore) {
                let recorded = restore
                    .status
                    .as_ref()
                    .and_then(|s| s.clone.as_ref())
                    .and_then(|c| c.phase.as_deref())
                    == Some(PHASE_REMOVAL_FAILED);
                if recorded {
                    return if deleting {
                        Ok(handled(&name, Requeue::AwaitChange))
                    } else {
                        inner.await
                    };
                }
                patch_clone(
                    &restores,
                    &name,
                    json!({"phase": PHASE_REMOVAL_FAILED, "reason": bounded_reason(&format!(
                        "{why}; this controller cannot tell what the clone owes, so nothing \
                         was deleted. Look for topics with the clone's prefix on the target, \
                         delete any its run created, then remove the finalizer {FINALIZER}"))}),
                    None,
                )
                .await?;
                if deleting {
                    return Ok(handled(&name, Requeue::AwaitChange));
                }
                return inner.await;
            }
            if deleting {
                return Ok(handled(&name, Requeue::AwaitChange));
            }
            return match classify(restore) {
                Plan::Refused(why) if !terminal => Err(shape_refusal(&why)),
                _ => inner.await,
            };
        }
    };
    if deleting && !terminal {
        let jobs: Api<Job> = Api::namespaced(client.clone(), &namespace);
        let exists = jobs
            .get_opt(&name)
            .await
            .map_err(RestoreError::Api)?
            .is_some();
        match on_delete(restore, exists) {
            OnDelete::Observe => {}
            OnDelete::Release => {
                if has_finalizer(restore) {
                    set_finalizer(&restores, restore, false).await?;
                }
                return Ok(handled(&name, Requeue::AwaitChange));
            }
            OnDelete::Hold(why) => {
                patch_clone(
                    &restores,
                    &name,
                    json!({"ttlSeconds": i64::try_from(ttl).unwrap_or(i64::MAX),
                           "phase": PHASE_REMOVAL_FAILED, "reason": bounded_reason(&why)}),
                    None,
                )
                .await?;
                return Ok(handled(&name, Requeue::AwaitChange));
            }
        }
    } else if !deleting && !has_finalizer(restore) && !terminal {
        // At admission, before the run Job exists. The patch moves the
        // object's version, which the reconcile's status writes are
        // preconditioned on, so this pass ends here and the watch event of
        // the patch starts the next.
        set_finalizer(&restores, restore, true).await?;
        return Ok(handled(&name, Requeue::After(1)));
    }
    let mut outcome = inner.await?;
    // What the reconcile just wrote is read back: the clone acts on the
    // run's terminal status in the same pass that recorded it.
    let Some(latest) = restores.get_opt(&name).await.map_err(RestoreError::Api)? else {
        return Ok(outcome);
    };
    let deleting = latest.meta().deletion_timestamp.is_some();
    let requeue = advance(&latest, client, &namespace, ttl, deleting, now, image).await?;
    outcome.requeue = sooner(outcome.requeue, requeue);
    Ok(outcome)
}

fn handled(name: &str, requeue: Requeue) -> RestoreOutcome {
    RestoreOutcome {
        job_name: name.to_string(),
        created: false,
        admission: None,
        exit_code: None,
        terminal_state: None,
        keys: super::RestoreEvidenceKeys::default(),
        ttl_patched: false,
        requeue,
    }
}

/// One step of the clone's lifetime.
async fn advance(
    restore: &Restore,
    client: &kube::Client,
    namespace: &str,
    ttl: u64,
    deleting: bool,
    now: DateTime<Utc>,
    image: Option<&str>,
) -> Result<Requeue, RestoreError> {
    let restores: Api<Restore> = Api::namespaced(client.clone(), namespace);
    let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
    let name = restore.name_any();
    let uid = restore.uid().unwrap_or_default();
    let ttl_i64 = i64::try_from(ttl).unwrap_or(i64::MAX);
    match step(restore, ttl, deleting, now) {
        Step::Wait | Step::Hold => Ok(Requeue::AwaitChange),
        Step::Release => {
            if has_finalizer(restore) {
                set_finalizer(&restores, restore, false).await?;
            }
            Ok(Requeue::AwaitChange)
        }
        Step::NotKept => {
            let stored = patch_clone(
                &restores,
                &name,
                json!({"ttlSeconds": ttl_i64, "phase": PHASE_NOT_KEPT}),
                None,
            )
            .await?;
            if has_finalizer(&stored) {
                set_finalizer(&restores, &stored, false).await?;
            }
            Ok(Requeue::AwaitChange)
        }
        Step::Keep(expires_at) => {
            // The suspended attempt-1 Job, built now while the run Job is
            // certainly still there.
            let built = build_attempt(restore, &jobs, &uid, 1, true, image).await?;
            let mut clone = json!({
                "ttlSeconds": ttl_i64,
                "expiresAt": expires_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                "phase": PHASE_ACTIVE,
                "attempts": 0,
            });
            match built {
                Ok(job_name) => clone["jobRef"] = json!({"name": job_name}),
                Err(why) => {
                    clone["phase"] = json!(PHASE_REMOVAL_FAILED);
                    clone["reason"] = json!(bounded_reason(&why));
                }
            }
            patch_clone(&restores, &name, clone, None).await?;
            Ok(Requeue::After(seconds_until(expires_at, now)))
        }
        Step::Fail(why) => {
            patch_clone(
                &restores,
                &name,
                json!({"ttlSeconds": ttl_i64, "phase": PHASE_REMOVAL_FAILED,
                       "reason": bounded_reason(&why)}),
                None,
            )
            .await?;
            Ok(Requeue::AwaitChange)
        }
        Step::Until(at) => Ok(Requeue::After(seconds_until(at, now))),
        Step::Start(attempt) => {
            let started = if attempt == 1 {
                resume(&jobs, &cleanup_job_name(&uid, 1)).await?
            } else {
                build_attempt(restore, &jobs, &uid, attempt, false, image).await?
            };
            match started {
                Ok(job_name) => {
                    patch_clone(
                        &restores,
                        &name,
                        json!({"phase": PHASE_REMOVING, "attempts": attempt,
                               "jobRef": {"name": job_name}}),
                        None,
                    )
                    .await?;
                    Ok(Requeue::After(POLL_SECONDS))
                }
                Err(why) => {
                    patch_clone(
                        &restores,
                        &name,
                        json!({"phase": PHASE_REMOVAL_FAILED, "attempts": attempt,
                               "reason": bounded_reason(&why)}),
                        None,
                    )
                    .await?;
                    Ok(Requeue::AwaitChange)
                }
            }
        }
        Step::Watch => watch(restore, client, namespace, now).await,
    }
}

/// A `Removing` clone: look at its current cleanup Job.
async fn watch(
    restore: &Restore,
    client: &kube::Client,
    namespace: &str,
    now: DateTime<Utc>,
) -> Result<Requeue, RestoreError> {
    let restores: Api<Restore> = Api::namespaced(client.clone(), namespace);
    let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
    let name = restore.name_any();
    let clone = restore.status.as_ref().and_then(|s| s.clone.as_ref());
    // A `Removing` clone always records its attempt and its Job; one that
    // does not is not retried on a guess.
    let (Some(attempts), Some(job_name)) = (
        clone.and_then(|c| c.attempts),
        clone
            .and_then(|c| c.job_ref.as_ref())
            .map(|r| r.name.clone()),
    ) else {
        patch_clone(
            &restores,
            &name,
            json!({"phase": PHASE_REMOVAL_FAILED, "reason": bounded_reason(&format!(
                "the clone is Removing with no recorded attempt or cleanup Job, so the cleanup \
                 is not retried; look for topics with the clone's prefix on the target, delete \
                 any its run created, then remove the finalizer {FINALIZER}"))}),
            None,
        )
        .await?;
        return Ok(Requeue::AwaitChange);
    };
    let Some(job) = jobs.get_opt(&job_name).await.map_err(RestoreError::Api)? else {
        patch_clone(
            &restores,
            &name,
            json!({"phase": PHASE_REMOVAL_FAILED,
                   "reason": format!("cleanup Job {job_name} is gone before it was read")}),
            None,
        )
        .await?;
        return Ok(Requeue::AwaitChange);
    };
    if !crate::controllers::backup::job_finished(&job) {
        return Ok(Requeue::After(POLL_SECONDS));
    }
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
    // FX-43: the cleanup's lines are read only through its Job's token, in
    // one call (`runner_log::read_own_terminal`); a runner older than the
    // controller is named and nothing of its log is read.
    let mut older = false;
    let report = match found.pod.as_ref() {
        Some(pod) => {
            let pods: Api<k8s_openapi::api::core::v1::Pod> =
                Api::namespaced(client.clone(), namespace);
            // A log the API did not hand over is an error this pass retries,
            // never "the cleanup printed nothing", which would spend an
            // attempt.
            match crate::runner_log::read_own_terminal(&pods, &pod.name_any(), &job, exit_code)
                .await
                .map_err(RestoreError::Api)?
            {
                crate::runner_log::Terminal::Lines(lines) => {
                    crate::runner_log::key_values(lines.text(), CLONE_CLEANUP_PREFIX)
                        .pop()
                        .and_then(CloneCleanupReport::parse)
                }
                crate::runner_log::Terminal::RunnerOlder => {
                    older = true;
                    None
                }
                crate::runner_log::Terminal::OverBound => None,
            }
        }
        None => None,
    };
    let earlier: Vec<String> = restore
        .status
        .as_ref()
        .and_then(|s| s.teardown.as_ref())
        .and_then(|t| t.deleted.clone())
        .unwrap_or_default();
    let outcome = if older {
        // Refused, not retried: the next attempt runs the same image.
        Ended::Refused(format!(
            "{}: {}",
            logweir_core::runner_line::RUNNER_OLDER_THAN_CONTROLLER,
            crate::runner_log::RUNNER_OLDER_SENTENCE
        ))
    } else {
        ended(exit_code, report)
    };
    match outcome {
        Ended::Removed(report) => {
            // `reason: null` clears an earlier attempt's reason.
            let stored = patch_clone(
                &restores,
                &name,
                json!({"phase": PHASE_REMOVED, "reason": null}),
                Some(teardown_value(&report, &earlier)),
            )
            .await?;
            if has_finalizer(&stored) {
                set_finalizer(&restores, &stored, false).await?;
            }
            Ok(Requeue::AwaitChange)
        }
        Ended::Refused(why) => {
            patch_clone(
                &restores,
                &name,
                json!({"phase": PHASE_REMOVAL_FAILED, "reason": bounded_reason(&why)}),
                None,
            )
            .await?;
            Ok(Requeue::AwaitChange)
        }
        Ended::Failed(why, report) => {
            let teardown = report.as_ref().map(|r| teardown_value(r, &earlier));
            if attempts >= MAX_ATTEMPTS {
                patch_clone(
                    &restores,
                    &name,
                    json!({"phase": PHASE_REMOVAL_FAILED,
                           "reason": bounded_reason(&format!(
                               "{why}; {attempts} attempts failed, so the topics named in \
                                status.teardown.failed are left: delete them yourself, then \
                                remove the finalizer {FINALIZER}"))}),
                    teardown,
                )
                .await?;
                return Ok(Requeue::AwaitChange);
            }
            let ended_at = crate::diagnostics::job_ended_at(&job).unwrap_or(now);
            let next = ended_at + chrono::Duration::seconds(RETRY_AFTER_SECONDS);
            if now < next {
                return Ok(Requeue::After(seconds_until(next, now)));
            }
            let uid = restore.uid().unwrap_or_default();
            let image = job
                .spec
                .as_ref()
                .and_then(|s| s.template.spec.as_ref())
                .and_then(|p| p.containers.first())
                .and_then(|c| c.image.clone());
            let started =
                build_attempt(restore, &jobs, &uid, attempts + 1, false, image.as_deref()).await?;
            let mut clone = json!({"attempts": attempts + 1, "reason": bounded_reason(&why)});
            match started {
                Ok(job_name) => clone["jobRef"] = json!({"name": job_name}),
                Err(why) => {
                    clone["phase"] = json!(PHASE_REMOVAL_FAILED);
                    clone["reason"] = json!(bounded_reason(&why));
                }
            }
            patch_clone(&restores, &name, clone, teardown).await?;
            Ok(Requeue::After(POLL_SECONDS))
        }
    }
}

/// Create the cleanup Job for `attempt` from the run Job (attempt 1) or the
/// first cleanup Job (later attempts, once the run Job may be gone). The
/// inner `Err` is a reason the clone records: nothing to copy, or no run id.
async fn build_attempt(
    restore: &Restore,
    jobs: &Api<Job>,
    uid: &str,
    attempt: i64,
    suspend: bool,
    image: Option<&str>,
) -> Result<Result<String, String>, RestoreError> {
    let name = cleanup_job_name(uid, attempt);
    if let Some(existing) = jobs.get_opt(&name).await.map_err(RestoreError::Api)? {
        return Ok(
            if crate::evidence_fetch::is_owned(&existing, &owner(restore)) {
                Ok(name)
            } else {
                Err(format!(
                    "Job {name} exists and is not this Restore's; the cleanup was not started"
                ))
            },
        );
    }
    let Some(run_id) = restore
        .status
        .as_ref()
        .and_then(|s| s.evidence.as_ref())
        .and_then(|e| e.scorecard_key.as_deref())
        .and_then(run_id_of)
    else {
        return Ok(Err(
            "the run recorded no scorecard key, so no cleanup can name it; delete the clone's \
             topics yourself, then remove the finalizer"
                .to_string(),
        ));
    };
    let template_name = if attempt == 1 {
        restore.name_any()
    } else {
        cleanup_job_name(uid, 1)
    };
    let Some(template) = jobs
        .get_opt(&template_name)
        .await
        .map_err(RestoreError::Api)?
    else {
        return Ok(Err(format!(
            "Job {template_name}, which the cleanup Job is copied from, is gone; delete the \
             clone's topics yourself, then remove the finalizer"
        )));
    };
    if !crate::evidence_fetch::is_owned(&template, &owner(restore)) {
        return Ok(Err(format!(
            "Job {template_name} is not this Restore's; the cleanup was not started"
        )));
    }
    let run_args = template
        .spec
        .as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .and_then(|p| {
            p.containers
                .iter()
                .find(|c| c.name == crate::job::CONTAINER_NAME)
        })
        .and_then(|c| c.args.clone())
        .unwrap_or_default();
    let Some(argv) = cleanup_argv(&run_args, &run_id, attempt) else {
        return Ok(Err(
            "the run Job's argv names no plan or signing key to copy".to_string(),
        ));
    };
    let Some(job) = cleanup_job(&template, &name, argv, suspend, image) else {
        return Ok(Err(
            "the run Job has no runner container to copy".to_string()
        ));
    };
    // FX-43: a fresh line token for this attempt's Job (the argv was built
    // fresh, so the run Job's token is not copied).
    jobs.create(&PostParams::default(), &crate::job::tokened(job))
        .await
        .map_err(RestoreError::Api)?;
    Ok(Ok(name))
}

/// Resume the suspended attempt-1 Job. A Job that is gone is a reason the
/// clone records (`RemovalFailed`); any other error is the API's, and the
/// next pass tries again.
async fn resume(jobs: &Api<Job>, name: &str) -> Result<Result<String, String>, RestoreError> {
    match jobs
        .patch(
            name,
            &PatchParams::default(),
            &Patch::Merge(json!({"spec": {"suspend": false}})),
        )
        .await
    {
        Ok(_) => Ok(Ok(name.to_string())),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(Err(format!(
            "the suspended cleanup Job {name} is gone, so the cleanup cannot start; look for \
             topics with the clone's prefix on the target, delete any its run created, then \
             remove the finalizer {FINALIZER}"
        ))),
        Err(e) => Err(RestoreError::Api(e)),
    }
}

fn owner(restore: &Restore) -> crate::job::RunnerOwner {
    crate::job::RunnerOwner {
        api_version: Restore::api_version(&()).to_string(),
        kind: Restore::kind(&()).to_string(),
        name: restore.name_any(),
        uid: restore.uid().unwrap_or_default(),
    }
}

/// Merge-patch `status.clone` (and `status.teardown` when given); the object
/// as stored, which a finalizer change in the same pass preconditions on.
async fn patch_clone(
    restores: &Api<Restore>,
    name: &str,
    clone: Value,
    teardown: Option<Value>,
) -> Result<Restore, RestoreError> {
    let mut status = json!({ "clone": clone });
    if let Some(t) = teardown {
        status["teardown"] = t;
    }
    restores
        .patch_status(
            name,
            &PatchParams::default(),
            &Patch::Merge(json!({ "status": status })),
        )
        .await
        .map_err(RestoreError::Api)
}

/// Add or remove [`FINALIZER`], preconditioned on the object's
/// `resourceVersion` so a concurrent change to the list is never overwritten.
async fn set_finalizer(
    restores: &Api<Restore>,
    restore: &Restore,
    present: bool,
) -> Result<(), RestoreError> {
    let mut list: Vec<String> = restore
        .finalizers()
        .iter()
        .filter(|f| *f != FINALIZER)
        .cloned()
        .collect();
    if present {
        list.push(FINALIZER.to_string());
    }
    restores
        .patch(
            &restore.name_any(),
            &PatchParams::default(),
            &Patch::Merge(json!({"metadata": {
                "finalizers": list,
                "resourceVersion": restore.resource_version(),
            }})),
        )
        .await
        .map_err(RestoreError::Api)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crds::restore::{CloneStatus, RestoreEvidence, RestoreStatus};

    fn t(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn restore_with(status: Option<RestoreStatus>) -> Restore {
        let mut r: Restore = serde_json::from_value(json!({
            "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
            "metadata": {"name": "c1", "namespace": "ns", "uid": "u-1"},
            "spec": {
                "planBytes": "x", "approvalRef": {"name": "a"},
                "sourceArchive": {"url": "s3://b/p"}, "backupSetRef": "s",
                "pointInTime": "2026-10-13T00:00:00Z",
                "target": {"clusterRef": {"name": "k"}, "mode": "scratch",
                           "topicNaming": {"prefix": "c-"}},
                "deadlineSeconds": 600
            }
        }))
        .unwrap();
        r.status = status;
        r
    }

    fn terminal(phase: &str, exit: i32, clone: Option<CloneStatus>) -> Restore {
        restore_with(Some(RestoreStatus {
            phase: Some(phase.into()),
            exit_code: Some(exit),
            clone,
            evidence: Some(
                serde_json::from_value::<RestoreEvidence>(
                    json!({"scorecardKey": "logweir/drills/01J9.json"}),
                )
                .unwrap(),
            ),
            ..RestoreStatus::default()
        }))
    }

    fn clone(phase: &str, expires: &str, attempts: i64) -> Option<CloneStatus> {
        Some(CloneStatus {
            ttl_seconds: Some(300),
            expires_at: Some(t(expires)),
            phase: Some(phase.into()),
            attempts: Some(attempts),
            job_ref: None,
            reason: None,
        })
    }

    /// **The clone's state machine.** KILLS: keeping a run that did not
    /// pass; a cleanup before `expiresAt`; a deletion that waits for
    /// `expiresAt`; a retry past the third attempt's record; releasing the
    /// finalizer of a `RemovalFailed` clone.
    #[test]
    fn the_clone_moves_through_its_states_by_its_status() {
        let now = t("2026-10-13T12:00:00Z");
        assert_eq!(step(&restore_with(None), 300, false, now), Step::Wait);
        assert_eq!(
            step(&terminal("Failed", 2, None), 300, false, now),
            Step::NotKept
        );
        assert_eq!(
            step(&terminal("Failed", 1, None), 300, false, now),
            Step::NotKept
        );
        assert_eq!(
            step(&terminal("Succeeded", 0, None), 300, false, now),
            Step::Keep(t("2026-10-13T12:05:00Z"))
        );
        let active = terminal(
            "Succeeded",
            0,
            clone(PHASE_ACTIVE, "2026-10-13T12:05:00Z", 0),
        );
        assert_eq!(
            step(&active, 300, false, now),
            Step::Until(t("2026-10-13T12:05:00Z"))
        );
        assert_eq!(
            step(&active, 300, true, now),
            Step::Start(1),
            "deletion is due"
        );
        assert_eq!(
            step(&active, 300, false, t("2026-10-13T12:05:00Z")),
            Step::Start(1),
            "due at expiresAt"
        );
        for (phase, want) in [
            (PHASE_REMOVING, Step::Watch),
            (PHASE_REMOVED, Step::Release),
            (PHASE_NOT_KEPT, Step::Release),
            (PHASE_REMOVAL_FAILED, Step::Hold),
        ] {
            let r = terminal("Succeeded", 0, clone(phase, "2026-10-13T12:05:00Z", 1));
            assert_eq!(step(&r, 300, true, now), want, "{phase}");
        }
    }

    fn ended_without_exit(job_ref: bool) -> Restore {
        let mut r = terminal("Failed", 0, None);
        let status = r.status.as_mut().unwrap();
        status.exit_code = None;
        status.job_ref = job_ref.then(|| crate::crds::LocalRef { name: "c1".into() });
        r
    }

    /// **The release decision fails closed** (the commit review's three
    /// findings). A run that did not pass is settled only by its runner's own
    /// exit or by no run Job ever recorded; a run Job that ended with no exit
    /// code keeps the finalizer. KILLS: `NotKept` for every run that did not
    /// pass (the review's state drift); a settlement that ignores `jobRef`.
    #[test]
    fn a_run_that_ended_without_an_exit_code_keeps_the_finalizer() {
        let now = t("2026-10-13T12:00:00Z");
        assert!(matches!(
            step(&ended_without_exit(true), 300, true, now),
            Step::Fail(why) if why.contains("without an exit code")
        ));
        // CONTROL: refused before any run Job was recorded.
        assert_eq!(
            step(&ended_without_exit(false), 300, true, now),
            Step::NotKept
        );
    }

    /// **Review MEDIUM-1: a runner killed by a signal did not exit.** Exit 137
    /// (an OOM kill, SIGKILL) and 143 (SIGTERM on a drain or an eviction) are
    /// not runner codes, so its teardown never ran: `Fail`, the finalizer
    /// kept. KILLS `exit_code.is_some()` as "exited". CONTROL: the runner's own
    /// codes 1 to 4 settle the clone (`NotKept`).
    #[test]
    fn a_runner_killed_by_a_signal_keeps_the_finalizer() {
        let now = t("2026-10-13T12:00:00Z");
        for code in [137, 143, 9, 255] {
            let mut r = ended_without_exit(true);
            r.status.as_mut().unwrap().exit_code = Some(code);
            assert!(
                matches!(step(&r, 300, true, now), Step::Fail(why)
                    if why.contains(&format!("exit code {code}"))),
                "{code}"
            );
        }
        for code in 1..=4 {
            let mut r = ended_without_exit(true);
            r.status.as_mut().unwrap().exit_code = Some(code);
            assert_eq!(step(&r, 300, true, now), Step::NotKept, "{code}");
        }
    }

    /// **Deleting a clone before its run ended releases it only on proof that
    /// no run Job was ever recorded.** KILLS: releasing because the Job is
    /// absent (it may have been deleted after creating topics); releasing with
    /// a recorded `jobRef` or exit code; holding a Job that exists.
    #[test]
    fn a_deletion_before_the_outcome_releases_only_on_recorded_proof() {
        let pending = restore_with(Some(RestoreStatus {
            phase: Some("Pending".into()),
            ..RestoreStatus::default()
        }));
        assert_eq!(on_delete(&pending, false), OnDelete::Release);
        assert_eq!(on_delete(&pending, true), OnDelete::Observe);
        let mut ran = pending.clone();
        ran.status.as_mut().unwrap().job_ref = Some(crate::crds::LocalRef { name: "c1".into() });
        assert!(matches!(on_delete(&ran, false), OnDelete::Hold(why) if why.contains("c1")));
        let mut exited = pending.clone();
        exited.status.as_mut().unwrap().exit_code = Some(1);
        assert!(matches!(on_delete(&exited, false), OnDelete::Hold(_)));
        assert!(!run_recorded(&pending) && run_recorded(&ran) && run_recorded(&exited));
    }

    fn plan_bytes(teardown: &str, mode: &str) -> String {
        format!(
            "source:\n  storage: {{backend: s3, bucket: b, prefix: p, region: us-east-1}}\n  \
             backup: latestCompleted\n  topics: [orders]\n\
             target:\n  bootstrap_servers: [\"k:9092\"]\n  mode: {mode}\n  \
             topic_mapping_prefix: \"c-\"\n  teardown: {teardown}\n\
             sample:\n  window_start: \"2026-10-13T00:00:00Z\"\n  \
             window_end: \"2026-10-13T01:00:00Z\"\n\
             objectives: {{}}\n\
             evidence: {{backend: s3, bucket: e, prefix: logweir/, region: us-east-1}}\n"
        )
    }

    /// **A plan this controller cannot read, or a clone shape it refuses, is
    /// never "not a clone".** KILLS: `parse_plan(..).ok()?` read as not a
    /// clone (the review's first finding).
    #[test]
    fn an_unreadable_or_refused_plan_is_never_not_a_clone() {
        let mut r = restore_with(None);
        assert!(matches!(classify(&r), Plan::Unreadable(_)), "planBytes `x`");
        r.spec.plan_bytes = plan_bytes("{ttl_seconds: 300}", "scratch");
        assert_eq!(classify(&r), Plan::Clone(300));
        r.spec.plan_bytes = plan_bytes("{ttl_seconds: 10}", "scratch");
        assert!(matches!(classify(&r), Plan::Refused(w) if w.starts_with("CloneTtlOutOfRange")));
        r.spec.plan_bytes = plan_bytes("{ttl_seconds: 300}", "newTopic");
        assert!(matches!(classify(&r), Plan::Refused(w) if w.starts_with("CloneTtlNeedsScratch")));
        r.spec.plan_bytes = plan_bytes("delete", "scratch");
        assert_eq!(classify(&r), Plan::NotClone);
    }

    /// The cleanup argv: the run's plan, key, keyring and store contract, its
    /// run id and attempt — and nothing of `restore run`'s.
    #[test]
    fn the_cleanup_argv_carries_only_what_the_cleanup_reads() {
        let run: Vec<String> = [
            "restore",
            "run",
            "--execution-contract-version",
            "2",
            "--spec",
            "/plan/p.yaml",
            "--approval",
            "/a/a.json",
            "--evidence-keys",
            "/a/keys.json",
            "--signing-key",
            "/s/key.pem",
            "--store-contract-version",
            "1",
        ]
        .iter()
        .map(|s| (*s).to_string())
        .collect();
        assert_eq!(
            cleanup_argv(&run, "01J9", 2).unwrap(),
            [
                "restore",
                "cleanup",
                "--spec",
                "/plan/p.yaml",
                "--signing-key",
                "/s/key.pem",
                "--evidence-keys",
                "/a/keys.json",
                "--store-contract-version",
                "1",
                "--run-id",
                "01J9",
                "--attempt",
                "2"
            ]
        );
        assert!(
            cleanup_argv(&run[..4], "01J9", 1).is_none(),
            "no plan, no cleanup"
        );
    }

    /// The copied Job: owner and pod kept, the Job controller's selector and
    /// labels dropped, suspended as asked, with its own deadline and no TTL.
    #[test]
    fn the_cleanup_job_is_the_run_jobs_pod_with_the_cleanup_argv() {
        let run: Job = serde_json::from_value(json!({
            "metadata": {"name": "c1", "namespace": "ns", "uid": "j-1",
                "labels": {"app.kubernetes.io/name": "logweir", "job-name": "c1"},
                "ownerReferences": [{"apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
                    "name": "c1", "uid": "u-1", "controller": true}]},
            "spec": {
                "selector": {"matchLabels": {"controller-uid": "j-1"}},
                "ttlSecondsAfterFinished": 604800, "backoffLimit": 0,
                "template": {
                    "metadata": {"labels": {"controller-uid": "j-1", "job-name": "c1",
                        "app.kubernetes.io/name": "logweir"},
                        "annotations": {"logweir.dev/line-token": "x"}},
                    "spec": {"restartPolicy": "Never", "containers": [
                        {"name": "runner", "image": "old", "args": ["restore", "run"]}]}
                }
            }
        }))
        .unwrap();
        let job = cleanup_job(
            &run,
            &cleanup_job_name("u-1", 1),
            vec!["restore".into(), "cleanup".into()],
            true,
            Some("new"),
        )
        .unwrap();
        let spec = job.spec.as_ref().unwrap();
        assert_eq!(spec.suspend, Some(true));
        assert_eq!(spec.selector, None);
        assert_eq!(spec.ttl_seconds_after_finished, None);
        assert_eq!(spec.active_deadline_seconds, Some(CLEANUP_DEADLINE_SECONDS));
        let meta = spec.template.metadata.as_ref().unwrap();
        assert_eq!(
            meta.labels.as_ref().unwrap().keys().collect::<Vec<_>>(),
            ["app.kubernetes.io/name"]
        );
        assert_eq!(meta.annotations, None);
        let c = &spec.template.spec.as_ref().unwrap().containers[0];
        assert_eq!(c.args.as_deref().unwrap(), ["restore", "cleanup"]);
        assert_eq!(c.image.as_deref(), Some("new"));
        assert_eq!(
            job.metadata.name.as_deref(),
            Some("logweir-clone-cleanup-u-1-1")
        );
        assert_eq!(job.metadata.owner_references, run.metadata.owner_references);
        assert!(!job.metadata.labels.unwrap().contains_key("job-name"));
    }

    /// Exit 0 with its line is `Removed`; exit 3 is a refusal, never retried;
    /// anything else is a failure. KILLS: a retry of a refusal; `Removed`
    /// from an exit 0 that printed no line.
    #[test]
    fn an_attempt_is_read_by_its_exit_code_and_its_line() {
        let report = CloneCleanupReport {
            run_id: "r".into(),
            deleted: vec!["c-a".into()],
            ..CloneCleanupReport::default()
        };
        assert_eq!(ended(Some(0), Some(report.clone())), Ended::Removed(report));
        let refused = CloneCleanupReport {
            refused: Some("CloneTargetClusterChanged: x".into()),
            ..CloneCleanupReport::default()
        };
        assert_eq!(
            ended(Some(3), Some(refused)),
            Ended::Refused("CloneTargetClusterChanged: x".into())
        );
        assert!(matches!(ended(Some(0), None), Ended::Failed(..)));
        assert!(matches!(ended(Some(1), None), Ended::Failed(..)));
        assert!(matches!(ended(None, None), Ended::Failed(..)));
    }

    #[test]
    fn the_run_id_is_read_off_the_scorecard_key() {
        assert_eq!(
            run_id_of("logweir/drills/01J9X2QK7C4V0R8YB3ZP6MTS5A.json").as_deref(),
            Some("01J9X2QK7C4V0R8YB3ZP6MTS5A")
        );
        assert_eq!(run_id_of("logweir/drills/.json"), None);
        assert_eq!(run_id_of("logweir/drills/a b.json"), None);
        assert_eq!(run_id_of("logweir/drills/x.sig"), None);
    }

    #[test]
    fn the_teardown_status_is_the_reports() {
        let report = CloneCleanupReport {
            run_id: "r".into(),
            deleted: vec!["c-a".into()],
            already_absent: vec!["c-c".into()],
            left: vec![logweir_core::target_creation::CloneCleanupLeft {
                topic: "c-b".into(),
                reason: "CloneTargetReplaced: x".into(),
            }],
            attestation_key: Some("logweir/drills/r.cleanup.json".into()),
            refused: None,
        };
        assert_eq!(
            teardown_value(&report, &[]),
            json!({"attestationKey": "logweir/drills/r.cleanup.json", "deleted": ["c-a"],
                   "failed": [{"topic": "c-b", "error": "CloneTargetReplaced: x"}]})
        );
        // An earlier attempt's deletions stay listed (review LOW-4); KILLS a
        // later attempt's list replacing them.
        assert_eq!(
            teardown_value(&report, &["c-z".into(), "c-a".into()])["deleted"],
            json!(["c-z", "c-a"])
        );
    }
}
