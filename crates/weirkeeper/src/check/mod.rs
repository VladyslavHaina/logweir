//! The shared check framework — D2 §4.3.
//!
//! # What this module is for
//!
//! Four different controllers will want the same thing: run one short-lived,
//! isolated Job that mirrors an execution pod, watch it without ever trusting a
//! label, read its framed stdout, and turn what happened into the closed
//! [`CheckCode`] vocabulary. `TopicDiscovery` (W8) and `Preflight` (W9) are the
//! first two; the `KafkaCluster` probe joins them in D2 §4.5's phase 1.
//!
//! Writing that four times is how four slightly different answers to "did the
//! Secret exist?" end up in four status fields. So the moving parts are here
//! and the reconcilers are thin:
//!
//! | module | what it decides |
//! |---|---|
//! | [`job`] | the Job's name, labels, deadline, TTL and projections |
//! | [`pod`] | which pod belongs to that Job — by controller-owner UID, never by label (D-SEAMS **S6**) |
//! | [`waiting`] | what a pod that has not run yet is waiting for, as a code |
//! | [`relay`] | the verified frames, and the runner's contract refusal |
//! | [`policy`] | the installation policy `ConfigMap`, its defaults and its fail-closed refusal |
//!
//! # The seam W8 and W9 call
//!
//! [`classify`] is a PURE function from what the API server said to an
//! [`Observation`]. Everything that reads a clock or a socket is its caller's.
//! That is what lets a controller test drive a whole state machine over
//! fixtures, and it is why a reconciler in this crate can be asserted to have
//! "called nothing its test did not record".
//!
//! [`attribute`] is the other half: a waiting code belongs to a specific check
//! id when it names a Secret or volume the plan projected, and BLOCKS THE WHOLE
//! POD when it does not (D2 §4.3, "Mapping to check IDs").
//!
//! # What is deliberately not here yet
//!
//! D2 §4.3 also lists `plan.rs`, `chunks.rs`, `cancel.rs`, `limits.rs` and
//! `gc.rs`. Each of those writes or deletes an object owned by one of the three
//! kinds D2 §13.2 gives to **W6a**, which has not landed: a plan `ConfigMap`'s
//! 409 rule is about the owner UID of a `TopicDiscovery` or a `Preflight`, and
//! `gc.rs` needs `DeleteParams` preconditions over the same. They are W8/W9's
//! to land with the controllers that own those objects, on top of what is here.
//! [`ttl_patch`] and [`cancel_patch`] — the two Job patches the ordering rules
//! are about — ARE here, because they are properties of the check Job and not
//! of any CR.

use chrono::{DateTime, Utc};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use serde_json::{json, Value};

use logweir_core::check_contract::{
    CheckCode, CheckId, CheckOutcome, CheckRelay, FrameExpectations, OverallState,
};

pub mod job;
pub mod pod;
pub mod policy;
pub mod relay;
pub mod waiting;

pub use waiting::{EventFact, Waiting};

/// D2 §6.4's aggregation, re-exported so a controller names one path.
///
/// RE-EXPORTED AND NOT REIMPLEMENTED: the API, the UI and both controllers must
/// agree on what a set of outcomes adds up to, and the rule (blocking notReady
/// wins, then blocking unknown, and an EMPTY set is `Unknown` and never
/// `Ready`) lives in the pure crate where the API can reach it too.
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
/// # The order, and why each step is where it is
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
            // EARLY CANCEL. The Job cannot succeed and its
            // `activeDeadlineSeconds` is minutes away; a user watching a
            // spinner for a Secret that does not exist is the experience this
            // whole framework is meant to replace.
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

    // A finished Job whose deadline fired, or that was disrupted, says what
    // happened rather than reporting an unreadable relay for a pod that never
    // wrote one.
    if let Some(w) = waiting.as_ref().filter(|w| {
        matches!(
            w.code,
            CheckCode::DeadlineExceeded | CheckCode::DisruptedMidCheck
        )
    }) {
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

    // 4. The contract refusal, BY KEY NAME from a bounded tail.
    if let Some(code) = relay::refusal_reason(log) {
        return Observation {
            phase: CheckPhase::Failed,
            reason: code,
            message: format!(
                "the runner refused the check plan and printed `{}{code}` (exit {}); no frames \
                 were expected and none are required",
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
            // THE REASON, AND NOT THE LOG. D2 §4.3: a relay that does not
            // verify is reported with the exit code and without log content.
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
    /// The Secret the signing key comes from.
    pub signer_secret: Option<String>,
    /// Every `ConfigMap` projected as trust material.
    pub trust_config_maps: Vec<String>,
}

/// Which check a waiting code belongs to, or `None` when it belongs to the
/// whole pod.
///
/// `None` IS A REAL ANSWER AND NOT A FAILURE TO DECIDE. D2 §4.3: "An unmatched
/// code blocks the whole pod: every Job-sourced check becomes `unknown` with
/// `BlockedByPrerequisite`, and the named cause is reported." A code attributed
/// to the wrong check would send an operator to rotate the wrong credential, so
/// a code that names a Secret this plan did not project is deliberately
/// attributed to nothing.
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
            } else if projections.destination_secret.as_deref() == Some(named) {
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

/// The merge patch that sets a finished check Job's TTL — D2 §4.3.
///
/// # This is only ever sent AFTER the status commit
///
/// The TTL controller deletes a Job and its pods together, and the relay lives
/// only on the pod. The `Backup` path learned this the same way
/// ([`crate::controllers::backup::TTL_SECONDS_AFTER_FINISHED`]): the ordering
/// is a guarantee because the status patch is `?`-propagated, so a status write
/// that did not return 200 leaves the reconcile before any TTL exists.
#[must_use]
pub fn ttl_patch() -> Value {
    json!({ "spec": { "ttlSecondsAfterFinished": job::TTL_SECONDS } })
}

/// The merge patch that cancels an unfinished check Job — D2 §4.3's
/// `cancel.rs`.
///
/// `activeDeadlineSeconds: 1` and not `delete`: the weirkeeper `ClusterRole`
/// grants `delete` on nothing (`config/rbac/role.yaml`'s own header), and it is
/// the better mechanism anyway — the Job FAILS with `DeadlineExceeded`, its
/// pods terminate, and it becomes finished, so the TTL applies and the object
/// stays around long enough to say what happened.
///
/// **The caller verifies the owner UID before sending it.** A Job's name is
/// derived from the subject's UID ([`job::check_job_name`]), but a name is not
/// an identity; [`is_cancellable`] is the check.
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

// ---------------------------------------------------------------------------
// The three calls the framework makes against the API server
// ---------------------------------------------------------------------------

/// Create one check Job.
///
/// **No TTL is set here** — [`crate::job::build`]'s note 4 and [`ttl_patch`].
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
    jobs.create(&kube::api::PostParams::default(), &job::build(spec))
        .await
}

/// Patch a finished check Job's `ttlSecondsAfterFinished`.
///
/// # THE CALLER SENDS THIS ONLY AFTER ITS STATUS WRITE RETURNED 200
///
/// That is not a convention this function can enforce, and saying so here is
/// the next best thing: the relay lives on the pod, the TTL controller deletes
/// the Job and its pods together, and a TTL patched before the commit lets
/// garbage collection race the read. The `Backup` and `KafkaCluster` paths
/// carry the same note above their own TTL patches.
///
/// # Errors
/// [`kube::Error`].
pub async fn set_ttl(
    client: &kube::Client,
    namespace: &str,
    job_name: &str,
) -> Result<(), kube::Error> {
    let jobs: kube::Api<Job> = kube::Api::namespaced(client.clone(), namespace);
    jobs.patch(
        job_name,
        &kube::api::PatchParams::default(),
        &kube::api::Patch::Merge(ttl_patch()),
    )
    .await?;
    Ok(())
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

/// One whole observation pass — **the seam W8 and W9 call**.
///
/// It finds the owned pod (never a label match), reads the relay **only** when
/// the Job has finished and an owned pod was found, and hands everything to the
/// pure [`classify`].
///
/// # Why the log is read only for a finished Job with an owned pod
///
/// A running check's stdout has no end frame yet, so decoding it would always
/// be `ResultUnreadable`; and a `pods/log` read for a pod this Job does not own
/// is a read of somebody else's output, which is the whole of defect
/// `SEC-PODLOG`. Both conditions are enforced here rather than left to each
/// caller.
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
    use kube::ResourceExt as _;
    let owned = pod::find_owned_pod(client, namespace, job).await?;
    let finished = crate::controllers::backup::job_finished(job);
    let log = match (finished, owned.as_ref()) {
        (true, Some(p)) => {
            let pods: kube::Api<Pod> = kube::Api::namespaced(client.clone(), namespace);
            Some(pods.logs(&p.name_any(), &relay::log_params()).await?)
        }
        _ => None,
    };
    Ok(classify(&Input {
        job,
        pod: owned.as_ref(),
        events,
        log: log.as_deref(),
        expect,
        now,
    }))
}
