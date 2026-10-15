//! The shared check framework (D2 §4.3).
//!
//! Every check controller needs the same thing: run one short-lived, isolated
//! Job that mirrors an execution pod, watch it without trusting a label, read
//! its framed stdout, and turn what happened into the closed [`CheckCode`]
//! vocabulary. Written per controller, that would give slightly different
//! answers to "did the Secret exist?" in different status fields, so the moving
//! parts are here and the reconcilers are thin:
//!
//! | module | what it decides |
//! |---|---|
//! | [`job`] | the Job's name, labels, deadline, TTL and projections |
//! | [`plan`] | the immutable, owned, digest-pinned plan `ConfigMap` and its 409 rule |
//! | [`chunks`] | the immutable result `ConfigMap`s, their size bounds and the commit point |
//! | [`limits`] | D2 §4.4's four concurrency ceilings, counted from Jobs |
//! | [`pod`] | which pod belongs to that Job: by controller-owner UID, never by label (seam **S6**) |
//! | [`waiting`] | what a pod that has not run yet is waiting for, as a code |
//! | [`relay`] | the verified frames, and the runner's contract refusal |
//! | [`policy`] | the installation policy `ConfigMap`, its defaults and its fail-closed refusal |
//!
//! # The seam a check controller calls
//!
//! [`classify`] is a pure function from what the API server said to an
//! [`Observation`]; everything that reads a clock or a socket is its caller's,
//! so a controller test drives a whole state machine over fixtures.
//! [`attribute`] is the other half: a waiting code belongs to a check id when
//! it names a Secret or volume the plan projected, and blocks the whole pod
//! when it does not (D2 §4.3, "Mapping to check IDs"). The collection of
//! terminal check objects (`collect_expired`) lives in the two check
//! controllers, the only places a `delete` is granted.
//!
use chrono::{DateTime, Utc};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use serde_json::{json, Value};
use tracing::debug;

use logweir_core::check_contract::{
    CheckCode, CheckId, CheckOutcome, CheckRelay, FrameExpectations, OverallState,
};

pub mod chunks;
pub mod job;
pub mod limits;
pub mod plan;
pub mod pod;
pub mod policy;
pub mod relay;
pub mod waiting;

pub use waiting::{EventFact, Waiting};

/// D2 §6.4's aggregation, re-exported so a controller names one path.
///
/// Re-exported, not reimplemented: the API, the UI and both controllers must
/// agree on what a set of outcomes adds up to (blocking `notReady` wins, then
/// blocking `unknown`, and an empty set is `Unknown`, never `Ready`).
pub use logweir_core::check_contract::aggregate;

/// Where a check has got to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckPhase {
    /// The Job exists and has not finished.
    Running,
    /// The Job finished and its relay verified.
    Succeeded,
    /// The Job finished without a verifiable relay, or a waiting state is
    /// terminal.
    Failed,
}

/// What one pass observed about a check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Observation {
    /// Where it has got to.
    pub phase: CheckPhase,
    /// The scalar reason a status carries. Always a closed code.
    pub reason: CheckCode,
    /// A redacted explanation. Never log content.
    pub message: String,
    /// The verified relay, when there is one.
    pub relay: Option<CheckRelay>,
    /// What the pod is waiting for, when it is waiting.
    pub waiting: Option<Waiting>,
    /// The `runner` container's exit code, when it terminated.
    pub exit_code: Option<i32>,
    /// Whether this state can only be left by cancelling the Job — D2 §4.3's
    /// "early cancel".
    pub cancel_now: bool,
}

/// Everything [`classify`] is allowed to look at.
///
/// `log` is `Option` on purpose: a pass that has not read the pod log yet (the
/// Job is still running, or there is no owned pod) passes `None`, and
/// [`classify`] then never invents a relay. A controller that read a log it had
/// no pod for would be reading somebody else's.
pub struct Input<'a> {
    /// The check Job.
    pub job: &'a Job,
    /// Its owned pod — [`pod::find_owned_pod`]'s answer, never a label match.
    pub pod: Option<&'a Pod>,
    /// Events for the Job and the pod.
    pub events: &'a [EventFact],
    /// The pod log, when one was read.
    pub log: Option<&'a str>,
    /// The plan digest and subject UID the relay must carry.
    pub expect: &'a FrameExpectations,
    /// This pass's instant. An argument, never a clock read.
    pub now: DateTime<Utc>,
}

/// Turn one pass's observations into an [`Observation`] — **pure**.
///
/// # The order
///
/// 1. **A waiting state that is terminal wins over everything**, including a
///    finished Job: a pod whose Secret does not exist tells you why the Job is
///    about to fail, and `DeadlineExceeded` would bury it.
/// 2. **An unfinished Job is `Running`**, carrying whatever non-terminal
///    waiting state there is so an operator sees `PodUnschedulable` while it is
///    happening rather than ninety seconds later.
/// 3. **A finished Job with no pod, or no log, has no relay** — that is
///    [`CheckCode::ResultUnreadable`], not a guess from the exit code.
/// 4. **The contract refusal is read before the frames.** Exit 3 prints
///    `refusal-reason=` and NO frames, so decoding first would report
///    `ResultUnreadable` for a runner that refused correctly and said so.
/// 5. **The frames decide the rest.** A verified relay is `Succeeded` whatever
///    the per-check states inside it say — D2 §4.2's exit 0 is "an end line was
///    printed", and a check that found a problem still ran.
#[must_use]
pub fn classify(input: &Input<'_>) -> Observation {
    let waiting = waiting::classify(&waiting::Observed {
        job: input.job,
        pod: input.pod,
        events: input.events,
        now: input.now,
    });

    // 1. Terminal waiting.
    if let Some(w) = waiting.as_ref().filter(|w| w.is_terminal()) {
        return Observation {
            phase: CheckPhase::Failed,
            reason: w.code,
            message: w.message.clone(),
            relay: None,
            waiting: Some(w.clone()),
            exit_code: None,
            // Early cancel: the Job cannot succeed and its deadline is minutes
            // away.
            cancel_now: true,
        };
    }

    let finished = crate::controllers::backup::job_finished(input.job);
    if !finished {
        return Observation {
            phase: CheckPhase::Running,
            reason: waiting
                .as_ref()
                .map_or(CheckCode::PodNotStarted, |w| w.code),
            message: waiting.as_ref().map_or_else(
                || "the check Job is running".to_string(),
                |w| w.message.clone(),
            ),
            relay: None,
            waiting,
            exit_code: None,
            cancel_now: false,
        };
    }

    let exit_code = input
        .pod
        .and_then(crate::controllers::backup::terminated_exit_code);

    // A finished Job with any waiting state says what it was waiting for: a pod
    // in `ImagePullBackOff` (not terminal, so not cancelled early) reaches its
    // deadline without ever starting its `runner`, and `RunnerImagePullFailed`
    // is what the operator acts on, where `ResultUnreadable` or
    // `DeadlineExceeded` would bury it. The classification is already ordered
    // most-specific-first.
    if let Some(w) = waiting.as_ref() {
        return Observation {
            phase: CheckPhase::Failed,
            reason: w.code,
            message: w.message.clone(),
            relay: None,
            waiting: Some(w.clone()),
            exit_code,
            cancel_now: false,
        };
    }

    // 3. No log to read.
    let Some(log) = input.log else {
        return Observation {
            phase: CheckPhase::Failed,
            reason: CheckCode::ResultUnreadable,
            message: format!(
                "the check Job finished and no `{}` container log was read (exit {}), so the \
                 relay could not be verified; nothing is inferred from the exit code, which \
                 covers every operational failure alike",
                crate::job::CONTAINER_NAME,
                exit_code.map_or_else(|| "<none>".to_string(), |c| c.to_string())
            ),
            relay: None,
            waiting,
            exit_code,
            cancel_now: false,
        };
    };

    // 3b. FX-43: whose lines are read. Under the Job's every-line token, the
    // sealed end frame and `refusal-reason=` and the frames the end frame
    // binds by digest; nothing else. A runner that vouched for no exit and
    // refused the token flag is older than this controller, and says nothing
    // this pass reads.
    let raw = log;
    let lines = crate::runner_log::own_lines_binding(
        &crate::job::job_token(input.job),
        raw,
        &[
            logweir_core::check_contract::TOPIC_FRAME_PREFIX,
            logweir_core::check_contract::PART_FRAME_PREFIX,
        ],
    );
    if !raw.is_empty()
        && exit_code.is_some_and(crate::runner_log::vouches_for_exit)
        && lines.runner_is_older()
    {
        return Observation {
            phase: CheckPhase::Failed,
            reason: CheckCode::CheckContractMismatch,
            message: format!(
                "{}: the check runner refused the argument `{}` and printed no line sealed \
                 with this Job's line token (exit {}), so the runner image is older than this \
                 controller; nothing of its log was read — upgrade the runner image to this \
                 controller's release",
                logweir_core::runner_line::RUNNER_OLDER_THAN_CONTROLLER,
                logweir_core::runner_line::RUNNER_LINE_TOKEN_ARG,
                exit_code.map_or_else(|| "<none>".to_string(), |c| c.to_string()),
            ),
            relay: None,
            waiting,
            exit_code,
            cancel_now: false,
        };
    }
    let log = lines.text();

    // 4. The contract refusal, BY KEY NAME from a bounded tail.
    if let Some(code) = relay::refusal_reason(log) {
        // A runner refusing a plan this controller rendered is in practice an
        // older runner image: say so, naming the field it did not know when its
        // own stderr line says which.
        let hint = if code == CheckCode::CheckContractMismatch {
            // A hint from the runner's own stderr (never sealed): an
            // identifier of at most 64 letters and digits, shown, never acted
            // on.
            let field = refused_plan_field(raw)
                .map(|f| format!(" (it did not know the plan field `{f}`)"))
                .unwrap_or_default();
            format!(
                "; the runner image is older than this controller{field} — upgrade the runner \
                 image to this controller's release"
            )
        } else {
            String::new()
        };
        return Observation {
            phase: CheckPhase::Failed,
            reason: code,
            message: format!(
                "the runner refused the check plan and printed `{}{code}` (exit {}); no frames \
                 were expected and none are required{hint}",
                crate::controllers::backup::REFUSAL_REASON_PREFIX,
                exit_code.map_or_else(|| "<none>".to_string(), |c| c.to_string())
            ),
            relay: None,
            waiting,
            exit_code,
            cancel_now: false,
        };
    }

    // 5. The frames.
    match relay::decode(log, input.expect) {
        Ok(relay) => Observation {
            phase: CheckPhase::Succeeded,
            reason: CheckCode::Succeeded,
            message: format!(
                "the check relayed a verified result (exit {})",
                exit_code.map_or_else(|| "<none>".to_string(), |c| c.to_string())
            ),
            relay: Some(relay),
            waiting,
            exit_code,
            cancel_now: false,
        },
        Err(refusal) => Observation {
            phase: CheckPhase::Failed,
            reason: refusal.code,
            // The reason and not the log: a relay that does not verify is
            // reported with the exit code and without log content (D2 §4.3).
            message: format!(
                "the check Job's output did not verify: {} (exit {})",
                refusal.reason,
                exit_code.map_or_else(|| "<none>".to_string(), |c| c.to_string())
            ),
            relay: None,
            waiting,
            exit_code,
            cancel_now: false,
        },
    }
}

/// What a check plan projected into its pod, by the name the kubelet would use.
///
/// This is how a waiting code becomes an ANSWER ABOUT A CHECK rather than an
/// answer about a pod: `CredentialSecretNotFound{secret: "kafka-src"}` is
/// `connection.credentialProjected` when `kafka-src` is the connection's
/// Secret, and `destination.credentialProjected` when it is the destination's.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Projections {
    /// The Secret the Kafka connection's credential comes from.
    pub connection_secret: Option<String>,
    /// The Secret the destination's credential comes from.
    pub destination_secret: Option<String>,
    /// The Secret a separate `evidenceWrite` grant's credential comes from, when
    /// the check writes the create-only marker as that principal. Still the
    /// destination's credential, so a missing one is
    /// `destination.credentialProjected`.
    pub evidence_write_secret: Option<String>,
    /// The Secret a separate `evidenceRead` grant's credential comes from, for
    /// the same reason.
    pub evidence_read_secret: Option<String>,
    /// The Secret the signing key comes from.
    pub signer_secret: Option<String>,
    /// Every `ConfigMap` projected as trust material.
    pub trust_config_maps: Vec<String>,
}

/// Which check a waiting code belongs to, or `None` when it belongs to the
/// whole pod.
///
/// `None` is an answer, not a failure to decide (D2 §4.3: "An unmatched code
/// blocks the whole pod"). A code attributed to the wrong check would send an
/// operator to rotate the wrong credential, so a code naming a Secret this plan
/// did not project is attributed to nothing.
#[must_use]
pub fn attribute(waiting: &Waiting, projections: &Projections) -> Option<CheckId> {
    match waiting.code {
        CheckCode::RunnerImagePullFailed
        | CheckCode::RunnerImageNotPresent
        | CheckCode::RunnerImageInvalid => Some(CheckId::RunnerImage),
        CheckCode::RunnerServiceAccountMissing
        | CheckCode::PodCreateRejected
        | CheckCode::PodUnschedulable
        | CheckCode::VolumeMountFailed => Some(CheckId::RunnerPod),
        CheckCode::SigningKeyMissing => Some(CheckId::SignerPrivateKeyUsable),
        CheckCode::CredentialSecretNotFound | CheckCode::CredentialSecretKeyMissing => {
            let named = waiting.secret.as_deref()?;
            if projections.connection_secret.as_deref() == Some(named) {
                Some(CheckId::ConnectionCredentialProjected)
            } else if projections.destination_secret.as_deref() == Some(named)
                || projections.evidence_write_secret.as_deref() == Some(named)
                || projections.evidence_read_secret.as_deref() == Some(named)
            {
                Some(CheckId::DestinationCredentialProjected)
            } else if projections.signer_secret.as_deref() == Some(named) {
                Some(CheckId::SignerPrivateKeyUsable)
            } else {
                None
            }
        }
        CheckCode::TrustBundleNotFound => {
            let named = waiting.config_map.as_deref()?;
            projections
                .trust_config_maps
                .iter()
                .any(|c| c == named)
                .then_some(CheckId::DestinationResolved)
        }
        _ => None,
    }
}

/// The plan field an older runner refused, read from its own stderr line
/// (`… unknown field `evidenceRead`, expected one of …`).
///
/// A HINT, not a contract: the text is serde's, and a runner that words it
/// differently simply gets the hint without a field name. The value is kept
/// only when it is a plain identifier of at most 64 ASCII letters and digits,
/// so nothing else from a pod log can ride into a status message.
#[must_use]
pub fn refused_plan_field(log: &str) -> Option<String> {
    const MARKER: &str = "unknown field `";
    let at = log.rfind(MARKER)? + MARKER.len();
    let rest = &log[at..];
    let field = &rest[..rest.find('`')?];
    (!field.is_empty() && field.len() <= 64 && field.bytes().all(|b| b.is_ascii_alphanumeric()))
        .then(|| field.to_string())
}

/// The merge patch that cancels an unfinished check Job (D2 §4.3's `cancel.rs`).
///
/// `activeDeadlineSeconds: 1`, not `delete`: the controller holds no `delete`
/// on Jobs, and a collapsed deadline is the better mechanism anyway. The Job
/// fails with `DeadlineExceeded`, becomes finished, gets its TTL and stays long
/// enough to say what happened; a delete would take the reason with it. The
/// caller verifies the owner UID first ([`is_cancellable`]): a name derived
/// from the subject's UID is not an identity.
#[must_use]
pub fn cancel_patch() -> Value {
    json!({ "spec": { "activeDeadlineSeconds": 1 } })
}

/// Whether this Job may be cancelled by the controller of `owner_uid`.
///
/// Both halves: it must not already be finished — cancelling a finished Job
/// would rewrite the reason it finished for — and its controller
/// `ownerReference` must be the subject. A foreign Job that happens to carry
/// the same name is never touched.
#[must_use]
pub fn is_cancellable(job: &Job, owner_uid: &str) -> bool {
    use kube::Resource as _;
    if crate::controllers::backup::job_finished(job) {
        return false;
    }
    job.meta()
        .owner_references
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|o| o.uid == owner_uid && o.controller == Some(true))
}

/// The overall state of a set of outcomes, with the waiting cause folded in.
///
/// A thin wrapper over [`aggregate`] that exists so a controller cannot forget
/// the second half: an unattributable waiting code makes every Job-sourced
/// check `unknown`, so aggregating the outcomes alone would report `Ready` for
/// a pod that never started.
#[must_use]
pub fn overall(outcomes: &[CheckOutcome], blocked_by: Option<&Waiting>) -> OverallState {
    match blocked_by {
        Some(_) => OverallState::Unknown,
        None => aggregate(outcomes),
    }
}

/// Create one check Job.
///
/// **No TTL is set here** — [`crate::job::build`]'s note 4 and [`set_ttl`].
///
/// # Errors
/// [`kube::Error`], including the **409** a duplicate reconcile gets. A 409 is
/// not handled here: the Job's name is a pure function of the subject's UID and
/// the check kind ([`job::check_job_name`]), so a 409 means "the check this
/// pass wanted already exists", which is what the caller's next `get_opt`
/// finds.
pub async fn create_job(
    client: &kube::Client,
    spec: &job::CheckJobSpec,
) -> Result<Job, kube::Error> {
    let jobs: kube::Api<Job> = kube::Api::namespaced(client.clone(), &spec.namespace);
    // FX-43: the check runner seals its end frame, its refusal and its exit
    // with the Job's token, and only those (and the frames the end frame
    // binds) are read.
    jobs.create(
        &kube::api::PostParams::default(),
        &crate::job::tokened(job::build(spec)),
    )
    .await
}

/// Patch a finished check Job's `ttlSecondsAfterFinished`.
///
/// The caller sends this only after its status write returned 200: the relay
/// lives on the pod, the TTL controller deletes the Job and its pods together,
/// and a TTL patched before the commit lets garbage collection race the read.
/// This function cannot enforce that.
///
/// # Errors
/// [`kube::Error`].
pub async fn set_ttl(
    client: &kube::Client,
    namespace: &str,
    job_name: &str,
) -> Result<(), kube::Error> {
    let jobs: kube::Api<Job> = kube::Api::namespaced(client.clone(), namespace);
    crate::runner_log::set_ttl(&jobs, job_name, job::TTL_SECONDS, None).await
}

/// Cancel an owned, unfinished check Job by collapsing its deadline.
///
/// Returns `false` and sends NOTHING when [`is_cancellable`] says no — a
/// finished Job, or one this subject does not control. A foreign Job that
/// happens to carry the same name is never patched.
///
/// # Errors
/// [`kube::Error`].
pub async fn cancel(
    client: &kube::Client,
    namespace: &str,
    job: &Job,
    owner_uid: &str,
) -> Result<bool, kube::Error> {
    use kube::ResourceExt as _;
    if !is_cancellable(job, owner_uid) {
        return Ok(false);
    }
    let jobs: kube::Api<Job> = kube::Api::namespaced(client.clone(), namespace);
    jobs.patch(
        &job.name_any(),
        &kube::api::PatchParams::default(),
        &kube::api::Patch::Merge(cancel_patch()),
    )
    .await?;
    Ok(true)
}

/// Whether a `pods/log` failure means "there is no log", rather than "the API
/// server could not be reached".
///
/// **400 and 404 only.** 400 is what the API server answers for a container
/// that is waiting to start or was never created; 404 is the pod having gone.
/// Both are facts about the pod that [`classify`] already has a branch for.
/// Every other status — 401, 403, 429, 5xx — and every transport error is a
/// reason to requeue, because a verdict published from one of those would be a
/// claim about somebody's cluster derived from a control-plane failure.
#[must_use]
pub fn is_log_absent(error: &kube::Error) -> bool {
    matches!(error, kube::Error::Api(response) if response.code == 400 || response.code == 404)
}

/// One whole observation pass: find the owned pod (never a label match), read
/// the relay only when the Job has finished and an owned pod was found, and hand
/// everything to the pure [`classify`]. A running check's stdout has no end
/// frame yet, and a `pods/log` read of a pod this Job does not own is a read of
/// somebody else's output; both rules are enforced here rather than per caller.
///
/// # Errors
/// [`kube::Error`] from the pod `list` or the `pods/log` read.
pub async fn observe(
    client: &kube::Client,
    namespace: &str,
    job: &Job,
    events: &[EventFact],
    expect: &FrameExpectations,
    now: DateTime<Utc>,
) -> Result<Observation, kube::Error> {
    let owned = pod::find_owned_pod(client, namespace, job).await?;
    observe_found(client, namespace, job, owned.as_ref(), events, expect, now).await
}

/// [`observe`], reading the Job's Events itself through [`job_events`], only
/// when the classifier could use one: for a check controller with no Events
/// read of its own, so a pod refused at creation is named rather than ending
/// `DeadlineExceeded`.
///
/// # Errors
/// [`kube::Error`] from the pod `list` or the `pods/log` read. The events list
/// is best effort ([`crate::diagnostics::events_for`]).
pub async fn observe_reading_events(
    client: &kube::Client,
    namespace: &str,
    job: &Job,
    expect: &FrameExpectations,
    now: DateTime<Utc>,
) -> Result<Observation, kube::Error> {
    let owned = pod::find_owned_pod(client, namespace, job).await?;
    let events = job_events(client, namespace, job, owned.as_ref(), now).await;
    observe_found(client, namespace, job, owned.as_ref(), &events, expect, now).await
}

/// The Events that explain why one Job's pod has not started, for every
/// Job-owning controller without its own read.
///
/// * Only when [`waiting::reads_events`] says the classifier could use one (no
///   pod after [`waiting::POD_CREATE_GRACE`], or a container still creating
///   after [`waiting::MOUNT_GRACE`]), so every healthy run costs nothing.
/// * One list, by `involvedObject.uid` (the Job's while there is no pod, the
///   pod's once there is), with `limit` [`crate::diagnostics::EVENT_LIMIT`],
///   through [`crate::diagnostics::events_for`].
/// * By UID, never by name: `FailedCreate` is common in a namespace with a
///   `ResourceQuota`, and a classifier handed somebody else's would cancel a
///   healthy Job.
///
/// Best effort: a failed list yields an empty slice, so the classification
/// falls back to `PodNotStarted` and then the Job's own deadline, and a role
/// without `events: list` behaves exactly as before.
pub async fn job_events(
    client: &kube::Client,
    namespace: &str,
    job: &Job,
    pod: Option<&Pod>,
    now: DateTime<Utc>,
) -> Vec<EventFact> {
    use kube::ResourceExt as _;
    if !waiting::reads_events(job, pod, now) {
        return Vec::new();
    }
    let subject = match pod {
        Some(p) => p.uid(),
        None => job.uid(),
    };
    crate::diagnostics::events_for(client, namespace, &[subject]).await
}

/// Why an unfinished runner Job has no pod, when the Job controller said why:
/// the fail-fast read for the runner Jobs that keep no pod read of their own
/// while they run (the `KafkaCluster` probe, a dynamic backup's discovery Job,
/// a retention run).
///
/// `None` costs no request at all while the Job's own status counts a pod or
/// the Job is younger than [`waiting::POD_CREATE_GRACE`]
/// ([`waiting::no_pod_counted`]); otherwise one pod list (the owner-UID proof)
/// and, when that finds none, one [`job_events`] list.
///
/// # Errors
/// [`kube::Error`] from the pod list. The events list is best effort.
pub async fn refused_pod_creation(
    client: &kube::Client,
    namespace: &str,
    job: &Job,
    now: DateTime<Utc>,
) -> Result<Option<Waiting>, kube::Error> {
    if !waiting::no_pod_counted(job, now) {
        return Ok(None);
    }
    if pod::find_owned_pod(client, namespace, job).await?.is_some() {
        return Ok(None);
    }
    Ok(finished_without_pod(client, namespace, job, now).await)
}

/// Why a Job that has NO owned pod was never given one, from its
/// `FailedCreate` Event — the read a runner kind makes once it has already
/// listed pods and found none (a finished probe, delivery or retention Job).
pub async fn finished_without_pod(
    client: &kube::Client,
    namespace: &str,
    job: &Job,
    now: DateTime<Utc>,
) -> Option<Waiting> {
    let events = job_events(client, namespace, job, None, now).await;
    waiting::pod_create_refusal(job, None, &events, now)
}

/// [`observe`]'s body, once the owned pod is known.
async fn observe_found(
    client: &kube::Client,
    namespace: &str,
    job: &Job,
    owned: Option<&Pod>,
    events: &[EventFact],
    expect: &FrameExpectations,
    now: DateTime<Utc>,
) -> Result<Observation, kube::Error> {
    use kube::ResourceExt as _;
    let finished = crate::controllers::backup::job_finished(job);
    let log = match (finished, owned) {
        (true, Some(p)) => {
            let pods: kube::Api<Pod> = kube::Api::namespaced(client.clone(), namespace);
            match crate::runner_log::read_text(&pods, &p.name_any(), &crate::runner_log::RELAY)
                .await
            {
                Ok(log) => Some(log),
                // No log is an answer: a finished Job whose `runner` never
                // started has no stdout, and the API server says so with 400
                // ("container \"runner\" … is waiting to start") or 404.
                // Propagating it would make `classify`'s DeadlineExceeded branch
                // unreachable and leave the check requeueing with no status.
                Err(e) if is_log_absent(&e) => {
                    debug!(
                        namespace = %namespace,
                        pod = %p.name_any(),
                        error = %e,
                        "the check pod has no readable `runner` log; classifying from the Job \
                         and the pod status instead of failing the pass"
                    );
                    None
                }
                // Everything else is transport-class and really is a reason to
                // requeue rather than to publish a verdict.
                Err(e) => return Err(e),
            }
        }
        _ => None,
    };
    Ok(classify(&Input {
        job,
        pod: owned,
        events,
        log: log.as_deref(),
        expect,
        now,
    }))
}
