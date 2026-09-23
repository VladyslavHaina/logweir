//! **One** derivation of "why is this run taking so long" for every Job-backed
//! run — D3 §2.3.
//!
//! # There is no second classification table here
//!
//! D-SEAMS **S1** fixes one classification table for pod, Job and Event state,
//! and that table is [`crate::check::waiting`] — D2 §4.3's, written for check
//! Jobs and already exercised row by row. This module does not restate it. It
//! *generalises* it over a `Backup`'s and a `Restore`'s runner Job by adding
//! exactly the three columns D3 §2.3 adds: the **severity**, the
//! **transience**, and the **`metav1` condition reason** a
//! [`crate::conditions::CONDITION_RUNNER_READY`] carries while the runner
//! cannot start. [`Code::from_check_code`] is the only door between the two,
//! so a row added to D2's classifier reaches this one and a row invented here
//! would have no producer.
//!
//! # Two vocabularies, and the difference is deliberate
//!
//! * [`Code`] is the DIAGNOSTIC code — thirteen values, D3 §2.3's list
//!   verbatim. It is the most specific thing known, and it is what
//!   `status.progress.diagnostics[].code` carries: `CredentialSecretKeyMissing`
//!   tells an operator to add a key, and `CredentialSecretNotFound` tells them
//!   to create a Secret.
//! * [`Code::runner_ready_reason`] projects it into the SIX closed reasons D3
//!   §2.2 gives the `RunnerReady` condition. A `metav1.Condition.reason` is a
//!   rendered, stable label that other software matches on; the parameters
//!   (`{secret}`, `{volume}`) travel in the diagnostic's `object` and
//!   `message`, never in a condition reason.
//!
//! The projection is many-to-one on purpose. Four of the six reasons are also
//! [`crate::conditions::TERMINAL_STATES`] members, which is what makes
//! "the terminal reason is the recorded diagnostic" (§2.3) expressible: the
//! terminal state a fail-fast writes is the diagnostic's PROJECTION, which is
//! the value D3 §15's L1 asserts live (`Failed/CredentialReferenceMissing` for
//! a missing Secret).
//!
//! # Nothing here reads a clock, opens a socket or trusts a pod it did not own
//!
//! `now` is an argument on every function that needs one, as everywhere else
//! in this crate. The pod reaches [`derive`] only through
//! [`crate::check::pod::find_owned_pod_by_selectors`], which has already
//! verified the pod's controller owner UID against the Job's (seam **S6**,
//! defect SEC-PODLOG); this module re-states that requirement in
//! [`Facts::pod`]'s contract and never lists pods itself.

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ContainerStatus, Pod};
use kube::{Resource as _, ResourceExt as _};
use logweir_core::check_contract::CheckCode;
use serde_json::{json, Value};
use tracing::{debug, warn};

use crate::check::waiting::{self, EventFact, Waiting};
use crate::conditions::{
    current_condition, merge_condition, CONDITION_RUNNER_READY, REASON_RUNNER_STARTED,
    REASON_WAITING_FOR_POD, TERMINAL_STATE_CREDENTIAL_REFERENCE_MISSING,
    TERMINAL_STATE_POD_CREATION_FORBIDDEN, TERMINAL_STATE_POD_UNSCHEDULABLE,
    TERMINAL_STATE_RUNNER_IMAGE_UNAVAILABLE, TERMINAL_STATE_VOLUME_MOUNT_FAILED,
};
use crate::crds::{Condition, Diagnostic, DiagnosticObject, RunProgress, RunnerFacts, RunnerPhase};

// ===========================================================================
// Installation policy
// ===========================================================================

/// How long a non-transient diagnostic may hold before the Job is cancelled —
/// D3 §2.3's `failFastSeconds`, chart value `controller.failFastSeconds`.
pub const FAIL_FAST_SECONDS_ENV: &str = "LOGWEIR_FAIL_FAST_SECONDS";
/// [`FAIL_FAST_SECONDS_ENV`]'s default, five minutes.
pub const FAIL_FAST_SECONDS_DEFAULT: u64 = 300;
/// [`FAIL_FAST_SECONDS_ENV`]'s floor. A one-second fail-fast would cancel
/// every Job whose image is still being pulled on a cold node.
pub const FAIL_FAST_SECONDS_MIN: u64 = 60;

/// How long a finished Job is kept — D3 §2.7, chart value
/// `controller.jobTtlSeconds`.
pub const JOB_TTL_SECONDS_ENV: &str = "LOGWEIR_JOB_TTL_SECONDS";
/// [`JOB_TTL_SECONDS_ENV`]'s floor. Below an hour an operator cannot fetch the
/// pod log of a run that failed overnight.
pub const JOB_TTL_SECONDS_MIN: i32 = 3_600;

/// Read a bounded `u64` out of the environment, clamped at `min`.
///
/// **An unreadable value is the default and never a panic.** This is read on a
/// reconcile path, and a controller that refused to reconcile because somebody
/// typed `5m` into a ConfigMap would convert a typo into an outage.
fn bounded_env(var: &str, default: u64, min: u64) -> u64 {
    let raw = std::env::var(var).ok();
    let value = bounded(raw.as_deref(), default, min);
    if raw.is_some() && Some(value.to_string()) != raw.as_ref().map(|r| r.trim().to_string()) {
        warn!(
            var,
            configured = raw.as_deref().unwrap_or_default(),
            used = value,
            min,
            "the configured value is not a whole number of seconds at or above this build's \
             floor; the floor or the default is used"
        );
    }
    value
}

/// [`bounded_env`]'s decision, as a PURE function of the configured text.
///
/// Separate from the environment read so the three rules can be asserted
/// without `set_var`, which is process-global and therefore unusable in a
/// suite that runs tests in parallel.
///
/// 1. absent, blank or unparseable → `default` — a controller that refused to
///    reconcile because somebody typed `5m` into a ConfigMap would convert a
///    typo into an outage;
/// 2. below `min` → `min`, because the floors exist to stop a configuration
///    from cancelling every Job whose image is still being pulled;
/// 3. otherwise the configured value.
#[must_use]
pub fn bounded(raw: Option<&str>, default: u64, min: u64) -> u64 {
    match raw.map(str::trim).map(str::parse::<u64>) {
        Some(Ok(v)) => v.max(min),
        _ => default,
    }
}

/// The one configured value that means **never fail fast** — review round 1,
/// finding F8's third step.
///
/// Zero, and not a sentinel word, because the value is a number of seconds
/// everywhere else and "0 seconds of patience" is the one reading of zero that
/// would be actively dangerous: cancel on sight. Giving it the OPPOSITE
/// meaning is a decision, which is why it is a named constant carrying this
/// note rather than a bare `0` in a comparison.
///
/// **The chart value does not exist yet.** `controller.failFastSeconds` is
/// W13's (report gap G3). This is the controller half, ready for it, so that
/// when the value lands its semantics are already written down and tested
/// rather than invented at the point of use.
pub const FAIL_FAST_NEVER: u64 = 0;

/// [`FAIL_FAST_SECONDS_ENV`]'s effective value, or `None` for **never**.
///
/// `None` is [`FAIL_FAST_NEVER`]: fail-fast is disabled and every Job runs to
/// its own `activeDeadlineSeconds`. That is the lever for the case the review
/// identified — a cluster where an external controller (external-secrets, a
/// vault injector) materialises a Secret a few minutes behind the Job, where a
/// healthy run would otherwise be cancelled at the 300 s default.
#[must_use]
pub fn fail_fast_seconds() -> Option<Duration> {
    fail_fast_window(std::env::var(FAIL_FAST_SECONDS_ENV).ok().as_deref())
}

/// [`fail_fast_seconds`]'s decision, as a PURE function of the configured
/// text — the same split [`bounded`] has, and for the same reason: `set_var`
/// is process-global and unusable in a suite that runs tests in parallel, so a
/// rule that lives only behind an environment read is a rule with no mutant.
///
/// Three answers, and the order matters:
///
/// 1. exactly [`FAIL_FAST_NEVER`] → `None`, **read before the floor is
///    applied**. [`bounded`] clamps 1..59 UP to the minimum, which is right
///    for a configured patience and wrong for a request to switch the
///    behaviour off — `0` would otherwise come back as 60, which is the
///    opposite of what was asked for;
/// 2. anything else parseable → [`bounded`]'s answer, floor included;
/// 3. absent or unparseable → the default.
#[must_use]
pub fn fail_fast_window(raw: Option<&str>) -> Option<Duration> {
    if raw.map(str::trim).and_then(|v| v.parse::<u64>().ok()) == Some(FAIL_FAST_NEVER) {
        return None;
    }
    Some(Duration::from_secs(bounded(
        raw,
        FAIL_FAST_SECONDS_DEFAULT,
        FAIL_FAST_SECONDS_MIN,
    )))
}

/// [`JOB_TTL_SECONDS_ENV`]'s effective value, as the Job field's own type.
///
/// The default is [`crate::controllers::backup::TTL_SECONDS_AFTER_FINISHED`],
/// so an installation that configures nothing gets exactly the behaviour it
/// had before this was configurable.
#[must_use]
pub fn job_ttl_seconds() -> i32 {
    let default = u64::try_from(crate::controllers::backup::TTL_SECONDS_AFTER_FINISHED)
        .expect("the compiled-in TTL is positive");
    let min = u64::try_from(JOB_TTL_SECONDS_MIN).expect("the floor is positive");
    let secs = bounded_env(JOB_TTL_SECONDS_ENV, default, min);
    i32::try_from(secs).unwrap_or(i32::MAX)
}

// ===========================================================================
// The closed diagnosis vocabulary — D3 §2.3
// ===========================================================================

/// How bad a [`Code`] is — D3 §2.3's severity column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Severity {
    /// It may still resolve, or it has not stopped the run.
    Warning,
    /// Nothing about this run will get better on its own.
    Error,
}

impl Severity {
    /// The wire spelling on `status.progress.diagnostics[].severity`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Warning => "Warning",
            Self::Error => "Error",
        }
    }
}

/// How long `VolumeMountFailed` is believed to be transient — D3 §2.3's
/// "first 180 s". A volume that has not mounted in three minutes is not
/// mounting.
pub const MOUNT_TRANSIENT_FOR: Duration = Duration::from_secs(180);

/// How long a Job may exist with no pod and no event before the absence is
/// itself reported — D3 §2.3's `WaitingForPod` "after 60 s".
pub const WAITING_FOR_POD_GRACE: Duration = Duration::from_secs(60);

/// How often an "it is still happening" timestamp may be rewritten — plan
/// erratum **E11(d)**, D3 §2.2's `lastObservedTime`.
///
/// A reconciler's own status patch is what wakes it, so a field carrying a
/// fresh clock read on every pass makes every pass a write. Sixty seconds is
/// D3 §2.2's number and it governs `progress.lastObservedTime` and every
/// `diagnostics[].lastSeen` alike.
pub const OBSERVED_HEARTBEAT: Duration = Duration::from_secs(60);

/// The CLOSED diagnosis vocabulary — D3 §2.3, exactly.
///
/// Twelve of the thirteen are [`CheckCode`]s, reached only through
/// [`Code::from_check_code`]; [`Code::DisruptedMidRun`] is D2's
/// `DisruptedMidCheck` under the name a Backup or a Restore renders it with,
/// and [`Code::WaitingForPod`] is D3 §2.3's one addition — "no pod and no
/// event yet", which D2's classifier reports as `None` because a check that
/// has not started is not yet a finding.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Code {
    /// The Secret a credential names does not exist.
    CredentialSecretNotFound,
    /// The Secret exists and the key inside it does not.
    CredentialSecretKeyMissing,
    /// The trust bundle ConfigMap does not exist.
    TrustBundleNotFound,
    /// The runner image could not be pulled.
    RunnerImagePullFailed,
    /// The runner image is not on the node and the pull policy is `Never`.
    RunnerImageNotPresent,
    /// The runner image reference is not a valid name.
    RunnerImageInvalid,
    /// No node will take the pod.
    PodUnschedulable,
    /// The signing key volume did not mount.
    SigningKeyMissing,
    /// Some other volume did not mount.
    VolumeMountFailed,
    /// The ServiceAccount the pod names does not exist.
    RunnerServiceAccountMissing,
    /// The Job controller could not create the pod at all.
    PodCreateRejected,
    /// The node running this pod went away mid-run.
    DisruptedMidRun,
    /// The Job exists, has no pod, and no event explains why.
    WaitingForPod,
}

impl Code {
    /// Every member, in declaration order — what an exhaustive test iterates.
    pub const ALL: &'static [Self] = &[
        Self::CredentialSecretNotFound,
        Self::CredentialSecretKeyMissing,
        Self::TrustBundleNotFound,
        Self::RunnerImagePullFailed,
        Self::RunnerImageNotPresent,
        Self::RunnerImageInvalid,
        Self::PodUnschedulable,
        Self::SigningKeyMissing,
        Self::VolumeMountFailed,
        Self::RunnerServiceAccountMissing,
        Self::PodCreateRejected,
        Self::DisruptedMidRun,
        Self::WaitingForPod,
    ];

    /// The wire spelling on `status.progress.diagnostics[].code`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CredentialSecretNotFound => "CredentialSecretNotFound",
            Self::CredentialSecretKeyMissing => "CredentialSecretKeyMissing",
            Self::TrustBundleNotFound => "TrustBundleNotFound",
            Self::RunnerImagePullFailed => "RunnerImagePullFailed",
            Self::RunnerImageNotPresent => "RunnerImageNotPresent",
            Self::RunnerImageInvalid => "RunnerImageInvalid",
            Self::PodUnschedulable => "PodUnschedulable",
            Self::SigningKeyMissing => "SigningKeyMissing",
            Self::VolumeMountFailed => "VolumeMountFailed",
            Self::RunnerServiceAccountMissing => "RunnerServiceAccountMissing",
            Self::PodCreateRejected => "PodCreateRejected",
            // THE ONE RENAME, AND IT IS D3 §2.3's OWN. D2 spells this
            // `DisruptedMidCheck` because its subject is a check; a Backup and
            // a Restore are not checks and an operator reading
            // `DisruptedMidCheck` on a restore would go looking for a check.
            Self::DisruptedMidRun => "DisruptedMidRun",
            Self::WaitingForPod => "WaitingForPod",
        }
    }

    /// D2's classification, rendered for a Backup or a Restore runner Job.
    ///
    /// **THE ONLY DOOR BETWEEN THE TWO VOCABULARIES.** Every member of
    /// [`Code`] except [`Code::WaitingForPod`] is reachable only from here, so
    /// there is no second classifier and no invented code.
    ///
    /// `None` for every [`CheckCode`] that is not a waiting classification.
    /// The one D2's classifier can actually return that way is
    /// [`CheckCode::DeadlineExceeded`]: the Job's own clock running out is not
    /// a CAUSE, it is the consequence, and `crash_terminal_state` already
    /// records it. Reporting it as a diagnostic would bury whatever the pod
    /// had been waiting for.
    #[must_use]
    pub fn from_check_code(code: CheckCode) -> Option<Self> {
        Some(match code {
            CheckCode::CredentialSecretNotFound => Self::CredentialSecretNotFound,
            CheckCode::CredentialSecretKeyMissing => Self::CredentialSecretKeyMissing,
            CheckCode::TrustBundleNotFound => Self::TrustBundleNotFound,
            CheckCode::RunnerImagePullFailed => Self::RunnerImagePullFailed,
            CheckCode::RunnerImageNotPresent => Self::RunnerImageNotPresent,
            CheckCode::RunnerImageInvalid => Self::RunnerImageInvalid,
            CheckCode::PodUnschedulable => Self::PodUnschedulable,
            CheckCode::SigningKeyMissing => Self::SigningKeyMissing,
            CheckCode::VolumeMountFailed => Self::VolumeMountFailed,
            CheckCode::RunnerServiceAccountMissing => Self::RunnerServiceAccountMissing,
            CheckCode::PodCreateRejected => Self::PodCreateRejected,
            CheckCode::DisruptedMidCheck => Self::DisruptedMidRun,
            _ => return None,
        })
    }

    /// D3 §2.3's severity column.
    #[must_use]
    pub fn severity(self) -> Severity {
        match self {
            Self::PodUnschedulable
            | Self::RunnerImagePullFailed
            | Self::VolumeMountFailed
            | Self::WaitingForPod => Severity::Warning,
            _ => Severity::Error,
        }
    }

    /// D3 §2.3's transience column: whether waiting could still fix it.
    ///
    /// `observed_for` is how long this code has held CONTINUOUSLY, which is
    /// what makes the one time-dependent row expressible: a volume that has
    /// not mounted in [`MOUNT_TRANSIENT_FOR`] is not going to.
    ///
    /// [`Code::PodUnschedulable`] is transient FOREVER and that is a decision,
    /// not an oversight (D2's `Waiting::is_terminal` takes the same one): a
    /// node can join, a pod can be preempted and a cluster autoscaler exists.
    /// It is reported and left to the Job's own deadline — D3 §15's L2 asserts
    /// exactly that no fail-fast patch is issued for it.
    #[must_use]
    pub fn transient(self, observed_for: Duration) -> bool {
        match self {
            Self::PodUnschedulable | Self::RunnerImagePullFailed | Self::WaitingForPod => true,
            Self::VolumeMountFailed => observed_for < MOUNT_TRANSIENT_FOR,
            _ => false,
        }
    }

    /// The `RunnerReady=False` reason this code projects into — one of D3
    /// §2.2's six, or `None` for a code that is not about a runner that has
    /// not started.
    ///
    /// [`Code::DisruptedMidRun`] is the one `None`: the node went away, which
    /// means the runner HAD started, so `RunnerReady` is not the condition
    /// with an opinion about it.
    #[must_use]
    pub fn runner_ready_reason(self) -> Option<&'static str> {
        Some(match self {
            Self::CredentialSecretNotFound
            | Self::CredentialSecretKeyMissing
            | Self::TrustBundleNotFound => TERMINAL_STATE_CREDENTIAL_REFERENCE_MISSING,
            Self::RunnerImagePullFailed
            | Self::RunnerImageNotPresent
            | Self::RunnerImageInvalid => TERMINAL_STATE_RUNNER_IMAGE_UNAVAILABLE,
            Self::PodUnschedulable => TERMINAL_STATE_POD_UNSCHEDULABLE,
            // THE SIGNING KEY IS A VOLUME, AND THE CONDITION SAYS SO. The
            // diagnostic keeps `SigningKeyMissing` — which is the fact an
            // operator acts on — and the condition reason is the closed label
            // for its class. D3 §2.2 gives the condition six values and
            // `SigningKeyMissing` is not one of them.
            Self::SigningKeyMissing | Self::VolumeMountFailed => TERMINAL_STATE_VOLUME_MOUNT_FAILED,
            Self::RunnerServiceAccountMissing | Self::PodCreateRejected => {
                TERMINAL_STATE_POD_CREATION_FORBIDDEN
            }
            Self::WaitingForPod => REASON_WAITING_FOR_POD,
            Self::DisruptedMidRun => return None,
        })
    }
}

impl std::fmt::Display for Code {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One classified cause, with the object it is about.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Diagnosis {
    /// The closed code.
    pub code: Code,
    /// A sanitized, bounded explanation — [`sanitize`].
    pub message: String,
    /// `Pod` or `Job`.
    pub object_kind: &'static str,
    /// Its name.
    pub object_name: String,
}

impl Diagnosis {
    /// The dedup key D3 §2.3 fixes: `(code, object.kind, object.name)`.
    #[must_use]
    pub fn key(&self) -> (Code, &'static str, &str) {
        (self.code, self.object_kind, self.object_name.as_str())
    }
}

// ===========================================================================
// Sanitization — D3 §2.3
// ===========================================================================

/// The byte bound on a diagnostic message — D3 §2.2's `maxLength: 512`.
pub const MESSAGE_MAX_BYTES: usize = 512;

/// Make a kubelet's, a scheduler's or an admission webhook's prose safe to put
/// on a status — D3 §2.3's `diagnostics::sanitize`.
///
/// In order: collapse every run of whitespace (a newline on a status is how a
/// reader is fooled into thinking two facts are one), drop everything from a
/// `-----BEGIN` marker onwards (a webhook that echoed a mounted PEM back at us
/// must not have it copied onto an object every viewer can read), remove URL
/// userinfo, query strings and fragments (an endpoint with a presigned
/// signature in its query is a credential), and truncate to
/// [`MESSAGE_MAX_BYTES`] **on a char boundary**.
///
/// Object NAMES are kept. A Secret's or a ConfigMap's name is already a
/// reference in the spec every viewer of this object can read, and a
/// diagnostic that would not say which Secret is missing is a diagnostic
/// nobody can act on.
#[must_use]
pub fn sanitize(message: &str) -> String {
    // 1. Anything after a PEM header is key material or a certificate, and in
    //    either case it is not an explanation.
    let cut = match message.find("-----BEGIN") {
        Some(at) => &message[..at],
        None => message,
    };
    // 2. Whitespace, including the newline a forged second line would need.
    let collapsed = cut.split_whitespace().collect::<Vec<_>>().join(" ");
    // 3. URL userinfo, query and fragment, token by token.
    let scrubbed = collapsed
        .split(' ')
        .map(scrub_token)
        .collect::<Vec<_>>()
        .join(" ");
    truncate_on_char_boundary(scrubbed.trim(), MESSAGE_MAX_BYTES)
}

/// One whitespace-delimited token with its URL secrets removed.
fn scrub_token(token: &str) -> String {
    let Some(scheme_at) = token.find("://") else {
        return token.to_string();
    };
    let (scheme, rest) = token.split_at(scheme_at + 3);
    // `?` and `#` first: a query string can carry a presigned signature, and a
    // fragment can carry anything at all.
    let rest = rest.split(['?', '#']).next().unwrap_or_default();
    // `user:password@host` — everything before the LAST `@` in the authority.
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let (authority, path) = rest.split_at(authority_end);
    let authority = match authority.rfind('@') {
        Some(at) => &authority[at + 1..],
        None => authority,
    };
    format!("{scheme}{authority}{path}")
}

/// Truncate to at most `max` BYTES without splitting a character.
fn truncate_on_char_boundary(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}

// ===========================================================================
// The derivation — D3 §2.3
// ===========================================================================

/// What a run has got to, in user-facing terms — D3 §2.2's `progress.stage`.
///
/// **THE CONTROLLER'S HALF ONLY.** D3 §2.5's third column — the normalized API
/// `state` — is `logweir-api`'s (W11) and is not computed anywhere in this
/// crate: a controller that also published the normalized value would be the
/// second source of truth §17 forbids.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    /// Nothing has been created yet, or admission has not passed.
    Admission,
    /// A Job exists and no pod is running it yet.
    Queued,
    /// A pod exists and the runner container has not started.
    Preparing,
    /// The runner is doing the work.
    Running,
    /// The runner is checking its own work, or the controller is verifying it.
    Verifying,
    /// Terminal.
    Finished,
}

impl Stage {
    /// The wire spelling on `status.progress.stage`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Admission => "Admission",
            Self::Queued => "Queued",
            Self::Preparing => "Preparing",
            Self::Running => "Running",
            Self::Verifying => "Verifying",
            Self::Finished => "Finished",
        }
    }

    /// Whether `progress.lastObservedTime` is written at all — D3 §2.2's
    /// "active stages only".
    #[must_use]
    pub fn is_active(self) -> bool {
        !matches!(self, Self::Finished)
    }
}

/// Everything [`derive`] is allowed to look at.
#[derive(Clone, Copy, Debug)]
pub struct Facts<'a> {
    /// The runner Job. Always present: this is only called for a Job that
    /// exists.
    pub job: &'a Job,
    /// Its owned pod.
    ///
    /// **ALREADY OWNER-UID VERIFIED BY THE CALLER** —
    /// [`crate::check::pod::find_owned_pod_by_selectors`] is the one producer
    /// and it compares the pod's controller owner reference against the Job's
    /// UID (seam **S6**, defect SEC-PODLOG). Handing this function a pod found
    /// by label alone would be handing it a stranger's pod.
    pub pod: Option<&'a Pod>,
    /// Events for the Job and the pod, in any order. An EMPTY slice is a legal
    /// input and means "none were read", never "none exist".
    pub events: &'a [EventFact],
    /// How many pods claimed this Job when more than one did — review finding
    /// **F11**.
    ///
    /// `find_owned_pod_by_selectors` answers a CONTESTED Job with `pod: None`,
    /// which is the right ACCESS decision (nothing is read) and the wrong
    /// thing to say out loud: "no pod has appeared" is the opposite of what
    /// happened. This is what lets the message tell the truth on the one path
    /// defect SEC-PODLOG exists for.
    pub contested: usize,
    /// The runner's own phase, from the progress channel — [`parse_progress`].
    pub runner_phase: Option<&'a RunnerPhase>,
    /// This pass's instant. An argument, never a clock read.
    pub now: DateTime<Utc>,
}

/// The `RunnerReady` condition one pass computes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunnerReady {
    /// `True` or `False`. Never `Unknown`: "the runner has been seen running
    /// or terminated" is a question a pod either answers or does not.
    pub status: &'static str,
    /// One of D3 §2.2's six `False` reasons, or
    /// [`REASON_RUNNER_STARTED`].
    pub reason: &'static str,
    /// A sanitized explanation.
    pub message: String,
}

/// What one pass derived about a Job-backed run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Derived {
    /// Where the run has got to.
    pub stage: Stage,
    /// The one classified cause, when there is one.
    pub diagnosis: Option<Diagnosis>,
    /// D3 §2.2's `progress.runner` block.
    pub runner: RunnerFacts,
    /// D3 §2.2's `RunnerReady` condition.
    pub runner_ready: RunnerReady,
    /// Whether the runner container has EVER started — `state.running`,
    /// `state.terminated`, `lastState.terminated` or a non-zero
    /// `restartCount`. The fail-fast precondition (§2.3): a run whose process
    /// began has consumed its approval and its plan, and cancelling it is not
    /// free.
    pub started: bool,
}

/// One pure function from (Job, owned pod, container states, events, runner
/// phase) to a closed diagnosis — D3 §2.3, and the only one.
///
/// # The order is the classification, and it is D2's
///
/// [`crate::check::waiting::classify`] decides which row fires; this function
/// renders its answer, adds the one row D2 has no reason to have
/// ([`Code::WaitingForPod`]) and computes the stage, the runner block and the
/// condition around it. A second `match` over pod conditions here would be the
/// second table seam S1 forbids.
#[must_use]
pub fn derive(facts: &Facts<'_>) -> Derived {
    let runner_status = facts.pod.and_then(runner_container);
    let started = runner_status.is_some_and(container_has_started);
    let runner = runner_facts(facts, runner_status);

    let classified = waiting::classify(&waiting::Observed {
        job: facts.job,
        pod: facts.pod,
        events: facts.events,
        now: facts.now,
    });
    let diagnosis = classified
        .as_ref()
        .and_then(|w| render(w, facts))
        // D3 §2.3's one addition. D2's classifier answers `None` for a Job
        // with no pod and no `FailedCreate` event, because for a check that
        // is simply "not started yet"; for a run an operator is watching,
        // sixty seconds of nothing is itself the answer.
        .or_else(|| waiting_for_pod(facts));

    let runner_ready = runner_ready(&diagnosis, started, facts);
    let stage = stage_of(&diagnosis, started, facts);
    Derived {
        stage,
        diagnosis,
        runner,
        runner_ready,
        started,
    }
}

/// D2's [`Waiting`] as a [`Diagnosis`], or `None` for a row that is not a
/// cause ([`Code::from_check_code`]'s contract).
fn render(waiting: &Waiting, facts: &Facts<'_>) -> Option<Diagnosis> {
    let code = Code::from_check_code(waiting.code)?;
    // THE OBJECT IS THE ONE THE ROW IS ABOUT, and it is decided by the code
    // rather than by "whatever pod we have": `PodCreateRejected` and
    // `RunnerServiceAccountMissing` are the Job controller failing to make a
    // pod, so naming a pod would name one that does not exist.
    let (object_kind, object_name) = match code {
        Code::PodCreateRejected | Code::RunnerServiceAccountMissing | Code::WaitingForPod => {
            ("Job", facts.job.name_any())
        }
        _ => match facts.pod {
            Some(pod) => ("Pod", pod.name_any()),
            None => ("Job", facts.job.name_any()),
        },
    };
    Some(Diagnosis {
        code,
        message: sanitize(&waiting.message),
        object_kind,
        object_name,
    })
}

/// D3 §2.3's `WaitingForPod`: a Job that has existed for
/// [`WAITING_FOR_POD_GRACE`] with no pod and nothing said about why.
fn waiting_for_pod(facts: &Facts<'_>) -> Option<Diagnosis> {
    if facts.pod.is_some() {
        return None;
    }
    let created = facts.job.meta().creation_timestamp.as_ref()?.0;
    let age = (facts.now - created).to_std().ok()?;
    if age < WAITING_FOR_POD_GRACE {
        return None;
    }
    let secs = WAITING_FOR_POD_GRACE.as_secs();
    let message = if facts.contested > 0 {
        // PODS EXIST. What does not exist is one this Job can be PROVED to
        // own, and the terminal answer for that is `PodOwnershipContested`,
        // which `crash_terminal_state` writes when the Job ends. Saying "no
        // pod has appeared" here would contradict the namespace the operator
        // is looking at.
        format!(
            "{} pods claim this runner Job as their controller owner and none of them could be \
             proved to be its own, so none was read and nothing they printed is trusted",
            facts.contested
        )
    } else {
        format!(
            "the runner Job has existed for more than {secs} seconds and no pod it controls has \
             appeared; no Event explains why"
        )
    };
    Some(Diagnosis {
        code: Code::WaitingForPod,
        message: sanitize(&message),
        object_kind: "Job",
        object_name: facts.job.name_any(),
    })
}

/// The `runner` container's status, by NAME and never by index — the rule
/// [`crate::controllers::backup::terminated_exit_code`] is written from.
fn runner_container(pod: &Pod) -> Option<&ContainerStatus> {
    pod.status
        .as_ref()?
        .container_statuses
        .as_ref()?
        .iter()
        .find(|c| c.name == crate::job::CONTAINER_NAME)
}

/// Whether this container has EVER been more than `waiting`.
fn container_has_started(status: &ContainerStatus) -> bool {
    let state = status.state.as_ref();
    state.is_some_and(|s| s.running.is_some() || s.terminated.is_some())
        || status
            .last_state
            .as_ref()
            .is_some_and(|s| s.terminated.is_some() || s.running.is_some())
        || status.restart_count > 0
}

/// D3 §2.2's `progress.runner` block. Every field is optional because every
/// one of them can be legitimately unavailable while the pod starts.
fn runner_facts(facts: &Facts<'_>, runner: Option<&ContainerStatus>) -> RunnerFacts {
    let container_state = runner.and_then(|c| {
        let s = c.state.as_ref()?;
        Some(if s.running.is_some() {
            "Running"
        } else if s.terminated.is_some() {
            "Terminated"
        } else {
            "Waiting"
        })
    });
    RunnerFacts {
        job_name: Some(facts.job.name_any()),
        pod_name: facts.pod.map(kube::ResourceExt::name_any),
        pod_phase: facts
            .pod
            .and_then(|p| p.status.as_ref()?.phase.clone())
            .filter(|p| POD_PHASES.contains(&p.as_str())),
        // `false` WITH NO DIAGNOSTIC IS THE "it is just queued" CASE, which is
        // why this is written even when it is false and why it is `None` when
        // there is no pod to have an opinion about.
        scheduled: facts
            .pod
            .map(|p| waiting::pod_condition(p, "PodScheduled") == Some("True")),
        container_state: container_state.map(str::to_string),
        // VERBATIM AND BOUNDED. A translated kubelet reason is a reason nobody
        // can search for; a 64-byte bound is D3 §2.2's.
        waiting_reason: runner
            .and_then(|c| c.state.as_ref()?.waiting.as_ref()?.reason.clone())
            .map(|r| truncate_on_char_boundary(&r, 64)),
        started_at: runner
            .and_then(|c| Some(c.state.as_ref()?.running.as_ref()?.started_at.as_ref()?.0)),
    }
}

/// The five `pod.status.phase` values the CRD enumerates. A value outside them
/// is dropped rather than written: the field is a closed enum on the CRD and a
/// sixth value would be refused by the API server, failing the whole patch.
const POD_PHASES: [&str; 5] = ["Pending", "Running", "Succeeded", "Failed", "Unknown"];

/// D3 §2.2's `RunnerReady`.
fn runner_ready(diagnosis: &Option<Diagnosis>, started: bool, facts: &Facts<'_>) -> RunnerReady {
    if started {
        return RunnerReady {
            status: "True",
            reason: REASON_RUNNER_STARTED,
            message: format!(
                "the `{}` container has been seen running or terminated",
                crate::job::CONTAINER_NAME
            ),
        };
    }
    // A diagnosis with no `RunnerReady` projection is one about a runner that
    // HAD started, so it cannot be the reason this one has not.
    if let Some(reason) = diagnosis
        .as_ref()
        .and_then(|d| d.code.runner_ready_reason())
    {
        return RunnerReady {
            status: "False",
            reason,
            message: diagnosis
                .as_ref()
                .map(|d| d.message.clone())
                .unwrap_or_default(),
        };
    }
    RunnerReady {
        status: "False",
        reason: REASON_WAITING_FOR_POD,
        message: sanitize(&format!(
            "the runner Job {} exists and its `{}` container has not started",
            facts.job.name_any(),
            crate::job::CONTAINER_NAME
        )),
    }
}

/// D3 §2.5's first column, for the states a Job-backed pass can see.
///
/// `Admission` and the Backup-only `Resolving` row are NOT here: both are
/// decided before a Job exists, so the reconciler writes them itself.
fn stage_of(diagnosis: &Option<Diagnosis>, started: bool, facts: &Facts<'_>) -> Stage {
    if started {
        // D3 §2.5: the runner is verifying when the restore is in phase 7 or
        // the backup is in one of its three self-checking steps.
        if facts.runner_phase.is_some_and(is_verification_phase) {
            return Stage::Verifying;
        }
        return Stage::Running;
    }
    match facts.pod {
        // A pod that a node has taken is PREPARING even with no diagnostic:
        // the image is being pulled, the volumes are being mounted.
        Some(pod) if waiting::pod_condition(pod, "PodScheduled") == Some("True") => {
            Stage::Preparing
        }
        // An unscheduled pod with a failure code is `Preparing` too — D3 §2.5
        // says `Queued` is "no pod, or pod unscheduled WITHOUT a failure code".
        Some(_) if diagnosis.is_some() => Stage::Preparing,
        _ => Stage::Queued,
    }
}

/// D3 §2.5's verification row, over the runner's own phase vocabulary.
fn is_verification_phase(phase: &RunnerPhase) -> bool {
    match (phase.number, phase.name.as_deref()) {
        (Some(7), _) => true,
        (Some(-1), Some(name)) => BACKUP_VERIFYING_STEPS.contains(&name),
        _ => false,
    }
}

// ===========================================================================
// The runner progress channel — D3 §2.4, as ratified by W5's review
// ===========================================================================

/// The once-per-run version announcement — D3 §2.4 as amended 2026-09-17.
///
/// **IT MEANS TWO DIFFERENT NUMBERS AND A READER MUST KNOW WHICH.** On the
/// restore path it is the EXECUTION CONTRACT version the controller stamped;
/// on the backup path no controller stamps one, so it is the version the
/// runner binary implements. The amendment is explicit that a consumer reads
/// it against the path it is on and never treats the two as one number space —
/// which is why this controller RECORDS it verbatim on
/// `progress.runnerPhase`'s sibling field and compares it to nothing.
pub const PROGRESS_CONTRACT_PREFIX: &str = "progress-contract=";

/// `progress-phase=<n>:<name>` — D3 §2.4's grammar, byte-exact.
pub const PROGRESS_PHASE_PREFIX: &str = "progress-phase=";

/// The runner's own per-line bound (`logweir_core::execution_contract`'s
/// `PROGRESS_LINE_MAX_BYTES`), MIRRORED here.
///
/// A line longer than this cannot have come from the runner's formatter, so
/// it is ignored rather than parsed. Mirrored and not imported because the
/// bound is the RUNNER's promise about what it emits, and a controller that
/// imported it would silently follow a runner that widened it.
pub const PROGRESS_LINE_MAX_BYTES: usize = 96;

/// How many trailing log lines the progress read looks at — D3 §2.4's
/// `LogParams{tail_lines: 50}`.
pub const PROGRESS_TAIL_LINES: i64 = 50;

/// The byte ceiling on the progress read — D3 §2.4's `limit_bytes: 65536`.
pub const PROGRESS_LIMIT_BYTES: i64 = 65_536;

/// How often the progress read is made — D3 §2.4's "at most once per 30 s".
pub const PROGRESS_READ_INTERVAL: Duration = Duration::from_secs(30);

/// The restore runner's CLOSED phase vocabulary, at its own numbers — D3 §2.4
/// and W5's landed grammar.
pub const RESTORE_PHASES: [&str; 10] = [
    "admit",
    "approval",
    "target-ready",
    "target-diff",
    "sample-select",
    "preflight",
    "restore",
    "verify",
    "score-and-sign",
    "teardown",
];

/// The backup runner's CLOSED step vocabulary. Every one is reported at phase
/// `-1`, because the backup path has no numbered phases after admission.
pub const BACKUP_STEPS: [&str; 5] = ["admit", "engine", "readback", "sign", "upload"];

/// The backup steps D3 §2.5 calls verification.
pub const BACKUP_VERIFYING_STEPS: [&str; 3] = ["readback", "sign", "upload"];

/// What the progress channel said, if anything.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Progress {
    /// `progress-contract=`'s value, verbatim and bounded. `None` for an OLD
    /// RUNNER, which is not an error and never will be.
    pub contract: Option<String>,
    /// The last well-formed, in-vocabulary phase line.
    pub phase: Option<RunnerPhase>,
    /// How many `progress-phase=` lines were IGNORED — malformed, out of the
    /// closed vocabulary, or overlong.
    ///
    /// D3 §2.4: an unknown phase or a malformed line is ignored, never a
    /// failure. The count is what the reconciler logs; see the module header
    /// of [`crate::controllers::backup`] for why it is not a diagnostic.
    pub ignored: usize,
}

/// Read the ratified progress grammar out of a bounded log tail — D3 §2.4.
///
/// **BY KEY NAME, NEVER BY POSITION** (erratum E4), and the LAST well-formed
/// line wins: a runner prints one line per phase and the newest is the phase
/// it is in.
///
/// # Every failure mode is an absence
///
/// * No `progress-contract=` and no `progress-phase=` — an old runner. No
///   phase, no error, no diagnostic.
/// * A phase number outside `-1..=9`, a name not in the closed vocabulary, a
///   missing `:`, a line over [`PROGRESS_LINE_MAX_BYTES`] — counted in
///   [`Progress::ignored`] and otherwise ignored. A name that is not in the
///   vocabulary is the important one: the vocabulary is what stops a runner
///   (or anything that can write to its stdout) from putting arbitrary text on
///   an object every viewer of the namespace can read.
/// * Lines out of order — the last one wins, because that is what "the phase
///   it is in now" means, and a runner that printed 3 after 7 has told us it
///   is in 3.
#[must_use]
pub fn parse_progress(log: &str) -> Progress {
    parse_progress_after(log, false)
}

/// [`parse_progress`], told whether the channel has **already** announced
/// itself on an earlier pass — review finding **F2**.
///
/// # Why the gate needs a memory
///
/// `progress-contract=` is printed ONCE, at the top of the run
/// (`docs/stability.md`: "printed once, before the first `progress-phase=`
/// line"). This controller reads the last [`PROGRESS_TAIL_LINES`] lines. So
/// the moment a run's merged stdout and stderr pass fifty lines — a matter of
/// how chatty the run is, not whether — the announcement is outside every
/// window this controller will ever read, and a gate with no memory would then
/// decide there is no channel, write `runnerPhase: null` over a phase it had
/// already recorded, and make D3 §2.5's `Verifying` row unreachable for the
/// rest of the run.
///
/// **A stored phase is itself proof the channel announced itself**, because a
/// phase can only have been published through this gate. That is the memory,
/// and it needs no new status field: `announced` is
/// `stored.runner_phase.is_some()`. Publishing the version (gap G1) would also
/// fix it and is the better long-term answer; this is what makes that field
/// optional rather than urgent.
#[must_use]
pub fn parse_progress_after(log: &str, announced: bool) -> Progress {
    let mut out = Progress::default();
    for line in log.lines().map(|l| l.trim_end_matches('\r')) {
        if line.len() > PROGRESS_LINE_MAX_BYTES {
            if line.starts_with(PROGRESS_PHASE_PREFIX) {
                out.ignored += 1;
            }
            continue;
        }
        if let Some(v) = line.strip_prefix(PROGRESS_CONTRACT_PREFIX) {
            // Recorded verbatim and compared to nothing — see
            // [`PROGRESS_CONTRACT_PREFIX`]. Bounded by the line bound above.
            if !v.is_empty() {
                out.contract = Some(v.to_string());
            }
            continue;
        }
        if let Some(v) = line.strip_prefix(PROGRESS_PHASE_PREFIX) {
            match parse_phase(v) {
                Some(phase) => out.phase = Some(phase),
                None => out.ignored += 1,
            }
        }
    }
    // THE VERSION ANNOUNCEMENT IS THE GATE — D3 §2.4's "an old runner (no
    // `progress-contract=`) yields no progress and no error", ratified by W5's
    // review as "one line, once, before the first phase line".
    //
    // The version itself is NOT PUBLISHED, and that is a recorded gap rather
    // than a choice: `crds::RunnerPhase` is `{number, name}` and the CRD
    // declares no field for a contract version. This worker owns no CRD file,
    // so the line is used for the one thing it can be used for without a
    // schema change — deciding whether there IS a progress channel — and the
    // field is reported to the shapes owner.
    if out.contract.is_none() && !announced {
        out.phase = None;
    }
    out
}

impl Progress {
    /// What a pass that did NOT read the log publishes: whatever the object
    /// already says.
    ///
    /// A merge patch that omitted `runnerPhase` would leave the stored value
    /// alone — but [`apply`] writes the whole `progress` object, so the phase
    /// has to be carried explicitly or a throttled pass would blank it.
    #[must_use]
    pub fn carried(stored: Option<&RunProgress>) -> Self {
        let phase = stored.and_then(|p| p.runner_phase.clone());
        Self {
            // Carried, not announced: this pass read no log. Set so the phase
            // survives the `contract.is_none()` gate above, which is about a
            // log that WAS read and said nothing.
            contract: phase.as_ref().map(|_| CARRIED.to_string()),
            phase,
            ignored: 0,
        }
    }
}

/// [`Progress::carried`]'s marker. Never written to a status — there is no
/// field for it — and never compared against a version.
const CARRIED: &str = "<carried>";

/// Whether this pass reads the pod log for progress — D3 §2.4's "at most once
/// per 30 s, while `RunnerReady=True` and not finished".
///
/// The throttle is keyed on `progress.lastObservedTime`, the only per-object
/// clock this controller stores, and it is compared against
/// [`OBSERVED_HEARTBEAT`] — the interval at which that field actually MOVES.
///
/// # Why not against [`PROGRESS_READ_INTERVAL`] — review finding F3
///
/// Because the clock is not the reconcile's; it is the stored field's, and
/// that field is rewritten at most once a minute (E11(d)). Comparing a
/// once-a-minute timestamp against a thirty-second bound makes the predicate
/// true at +30 s, +45 s **and** +60 s of every heartbeat cycle — with
/// `REQUEUE_SECS = 15`, three 64 KiB `pods/log` GETs a minute where §2.4
/// allows at most two. The first version of this function did exactly that
/// while its own doc comment claimed the opposite.
///
/// So the effective rate is one read per minute. That satisfies "at most once
/// per 30 s" strictly, and it is the honest consequence of refusing to add a
/// second timestamp field to a status for the sake of a log read: a reconciler
/// that kept the interval in memory would lose it on every restart and then
/// read on every pass until the next heartbeat.
#[must_use]
pub fn should_read_progress(
    stored: Option<&RunProgress>,
    started: bool,
    now: DateTime<Utc>,
) -> bool {
    if !started {
        return false;
    }
    let Some(last) = stored.and_then(|p| p.last_observed_time) else {
        return true;
    };
    (now - last).to_std().is_ok_and(|d| d >= OBSERVED_HEARTBEAT)
}

/// The progress read itself — D3 §2.4's bounded `LogParams`.
///
/// # The read looks for NEW lines, and an absence is not a retraction
///
/// Review finding **F2**. Three things about this window are true at once: the
/// contract announcement is printed once at the very top of the run and
/// scrolls out of it; a phase line is printed once per phase and scrolls out
/// of it too; and [`apply`] writes the WHOLE `progress` object, so whatever
/// this function does not return is erased rather than left alone.
///
/// So `stored` is consulted twice. It supplies the gate's memory (a stored
/// phase proves the channel announced itself — [`parse_progress_after`]), and
/// it supplies the answer whenever this window carries no phase line of its
/// own: a run that has been in phase 6 for two minutes prints nothing new, and
/// "nothing new" means the phase has not changed, never that there is no
/// phase.
///
/// # Errors
///
/// Never. A pod log that cannot be read is NOT an error on this path: the
/// channel is optional by contract, the pod may have just been garbage
/// collected, and a reconcile that failed because an optional log read 404ed
/// would turn a cosmetic field into an outage. The failure is logged and the
/// answer is what the object already said.
pub async fn read_progress(
    client: &kube::Client,
    namespace: &str,
    pod_name: &str,
    stored: Option<&RunProgress>,
) -> Progress {
    let pods: kube::Api<Pod> = kube::Api::namespaced(client.clone(), namespace);
    let params = kube::api::LogParams {
        tail_lines: Some(PROGRESS_TAIL_LINES),
        limit_bytes: Some(PROGRESS_LIMIT_BYTES),
        ..Default::default()
    };
    match pods.logs(pod_name, &params).await {
        Ok(log) => {
            let mut progress =
                parse_progress_after(&log, stored.is_some_and(|p| p.runner_phase.is_some()));
            if progress.phase.is_none() {
                // NO NEW PHASE LINE IN THE WINDOW is not "no phase" — see this
                // function's header. Carrying is what keeps `runnerPhase` from
                // blinking out on every pass of a long phase.
                progress.phase = stored.and_then(|p| p.runner_phase.clone());
            }
            if progress.ignored > 0 {
                // D3 §2.4: ignored, never a failure. It is a LOG LINE and not
                // a `status.progress.diagnostics` entry, and the reason is
                // D3 §2.3: that vocabulary is CLOSED and has no member for
                // "the runner printed something I could not read". Inventing
                // one would be exactly the invention §2.3 forbids.
                warn!(
                    namespace,
                    pod = pod_name,
                    ignored = progress.ignored,
                    "the runner printed `progress-phase=` lines this controller could not \
                     parse, or whose phase name is outside the closed vocabulary; they are \
                     ignored and the last well-formed phase stands"
                );
            }
            progress
        }
        Err(error) => {
            debug!(
                namespace,
                pod = pod_name,
                %error,
                "the optional progress read did not return a log; the phase the object already \
                 carries stands and nothing about the run changes"
            );
            Progress::carried(stored)
        }
    }
}

/// The events for one Job and its pod, by `involvedObject.uid`.
///
/// **BY UID AND NEVER BY NAME** (seam **S6**). D3 §2.3 specifies
/// `involvedObject.kind=Pod,involvedObject.name=<pod>`; a name is not an
/// identity, `FailedCreate` is a common event in a namespace with a
/// `ResourceQuota`, and `waiting::from_failed_create`'s own note records that
/// a classifier handed somebody else's event would cancel a healthy Job. The
/// UID form is strictly narrower, is what `controllers/preflight.rs` already
/// uses, and is recorded as a deliberate deviation.
///
/// # Errors
///
/// Never. An events list that fails yields an EMPTY slice, which D2's
/// classifier reads as "none were read" — the weaker code (`WaitingForPod`),
/// never an invented cause (D3 §16). A reconcile that failed because a
/// best-effort, rotated resource could not be listed would convert a
/// diagnostic aid into an outage.
pub async fn events_for(
    client: &kube::Client,
    namespace: &str,
    uids: &[Option<String>],
) -> Vec<EventFact> {
    let api: kube::Api<k8s_openapi::api::core::v1::Event> =
        kube::Api::namespaced(client.clone(), namespace);
    let mut out = Vec::new();
    for uid in uids.iter().flatten().filter(|u| !u.is_empty()) {
        let params = kube::api::ListParams::default()
            .fields(&format!("involvedObject.uid={uid}"))
            .limit(EVENT_LIMIT);
        match api.list(&params).await {
            Ok(list) => out.extend(list.items.iter().filter_map(EventFact::from_event)),
            Err(error) => debug!(
                namespace,
                uid = uid.as_str(),
                %error,
                "the events for this object could not be listed; the classification proceeds \
                 with the facts it has"
            ),
        }
    }
    out
}

/// D3 §2.3's `limit=20` on each events list.
pub const EVENT_LIMIT: u32 = 20;

/// How recently a WARNING-class diagnostic must have been observed, measured
/// back from the instant the Job ended, to be read as what ended it.
///
/// `lastSeen` is rewritten at most once per [`OBSERVED_HEARTBEAT`] while the
/// cause persists and a running Job is reconciled every 15 s, so a cause that
/// was still true when the Job ended carries a `lastSeen` within about 75 s of
/// the end. Three heartbeats is that with room for one missed pass, and still
/// short enough that a warning from a run's preparing minutes — an image pull
/// that backed off and then succeeded — cannot be read as the reason a run
/// that later ran for a quarter of an hour ended.
pub const WARNING_CAUSE_WINDOW: Duration = Duration::from_secs(3 * 60);

/// The terminal state a run reached WITHOUT an exit code, when a recorded
/// diagnostic explains it — D3 §2.2.
///
/// `None` when the stored status carries no diagnostic with a terminal
/// projection, in which case `crash_terminal_state`'s existing table is
/// unchanged and the answer is still `NoExitCode`. The NEWEST qualifying
/// diagnostic wins, which is the list's own order; `WaitingForPod` is never a
/// verdict ("nothing has happened yet" cannot be a cause).
///
/// # A WARNING THAT ENDED THE RUN IS ITS TERMINAL REASON
///
/// Defect WARNING-DIAGNOSTICS-NOEXITCODE (PLAT-14.1). The first landing kept
/// only `Error`-severity codes, on the theory that a warning's pod would
/// still be there for `crash_terminal_state` to read. It is not: the Job
/// controller deletes the pod of a Job that hit its deadline, and fail-fast
/// ends a run BY hitting its deadline. So a projected ConfigMap that never
/// mounted (fail-fast fired, deadline 900 → 1) and a pod no node would take
/// (the Job's own deadline) both ended `NoExitCode` — live, on harness-rows-11's
/// `operation-states` rows — while their diagnostics named the cause. D3 §2.2
/// says the new states replace `NoExitCode` "when the matching diagnostic was
/// recorded before the Job ended", and names no severity.
///
/// A warning is read as the cause only when BOTH hold, because "may resolve on
/// its own" (D3 §2.3's column) means an old one may have:
///
/// * **the runner never started**, as far as the stored status says — no
///   `startedAt`, no `Running`/`Terminated` container state, no runner phase.
///   A warning about a runner that then ran explains nothing about how it
///   ended; and
/// * **it was still being observed when the Job ended**: its `lastSeen` is
///   within [`WARNING_CAUSE_WINDOW`] of `ended_at`. A warning that stopped
///   being seen had resolved.
///
/// Anything else falls back to `NoExitCode`, the weaker answer — never an
/// invented cause (D3 §16). `Error`-severity codes are read exactly as
/// before: they do not resolve on their own.
#[must_use]
pub fn recorded_terminal_state(
    stored: Option<&RunProgress>,
    ended_at: DateTime<Utc>,
) -> Option<&'static str> {
    let stored = stored?;
    let started = runner_recorded_as_started(stored);
    stored.diagnostics.as_ref()?.iter().find_map(|d| {
        let code = Code::ALL.iter().find(|c| c.as_str() == d.code)?;
        let reason = code
            .runner_ready_reason()
            .filter(|r| *r != REASON_WAITING_FOR_POD)?;
        match code.severity() {
            Severity::Error => Some(reason),
            Severity::Warning => (!started && seen_at_the_end(d, ended_at)).then_some(reason),
        }
    })
}

/// Whether the stored status records the runner as ever having started.
fn runner_recorded_as_started(stored: &RunProgress) -> bool {
    let runner = stored.runner.as_ref();
    runner.is_some_and(|r| {
        r.started_at.is_some()
            || matches!(r.container_state.as_deref(), Some("Running" | "Terminated"))
    }) || stored.runner_phase.is_some()
}

/// Whether this diagnostic was still being observed when the Job ended.
fn seen_at_the_end(d: &Diagnostic, ended_at: DateTime<Utc>) -> bool {
    let Some(last_seen) = d.last_seen else {
        return false;
    };
    let window = chrono::Duration::from_std(WARNING_CAUSE_WINDOW).unwrap_or_default();
    last_seen >= ended_at - window
}

/// When a finished Job ended: the `lastTransitionTime` of its `Failed` or
/// `Complete` condition, else `completionTime`. `None` for a Job that says
/// neither, and the caller then uses its own instant.
#[must_use]
pub fn job_ended_at(job: &Job) -> Option<DateTime<Utc>> {
    let status = job.status.as_ref()?;
    status
        .conditions
        .as_ref()
        .and_then(|cs| {
            cs.iter()
                .filter(|c| (c.type_ == "Failed" || c.type_ == "Complete") && c.status == "True")
                .filter_map(|c| c.last_transition_time.as_ref().map(|t| t.0))
                .min()
        })
        .or_else(|| status.completion_time.as_ref().map(|t| t.0))
}

/// `<n>:<name>` against the closed vocabulary.
fn parse_phase(value: &str) -> Option<RunnerPhase> {
    let (number, name) = value.split_once(':')?;
    let number: i32 = number.parse().ok()?;
    let legal = match number {
        -1 => BACKUP_STEPS.contains(&name),
        0..=9 => {
            let at = usize::try_from(number).ok()?;
            RESTORE_PHASES.get(at) == Some(&name)
        }
        _ => false,
    };
    legal.then(|| RunnerPhase {
        number: Some(number),
        name: Some(name.to_string()),
    })
}

// ===========================================================================
// The status write — D3 §2.2
// ===========================================================================

/// Everything [`apply`] needs to turn a [`Derived`] into status bytes.
#[derive(Clone, Copy, Debug)]
pub struct Write<'a> {
    /// This pass's derivation.
    pub derived: &'a Derived,
    /// What the progress channel said.
    pub progress: &'a Progress,
    /// The object's STORED `status.progress`, for the two timestamp rules.
    pub stored: Option<&'a RunProgress>,
    /// The object's STORED `status.conditions`, for [`merge_condition`].
    pub conditions: Option<&'a Vec<Condition>>,
    /// `metadata.generation`, for `observedGeneration`.
    pub generation: Option<i64>,
    /// Whether this kind carries the scalar `status.reason` the REASON printer
    /// column reads — `true` for `Restore`, `false` for `Backup`, which has no
    /// such column and no such field.
    pub scalar_reason: bool,
    /// This pass's instant.
    pub now: DateTime<Utc>,
}

/// Fold D3 §2.2's progress block, its `RunnerReady` condition and (for a
/// `Restore`) the scalar `reason` into a `/status` merge patch another builder
/// already produced.
///
/// # Why this composes rather than replaces
///
/// `running_status_patch` and the terminal builders are each called from
/// sixteen places, four of them in crates this worker does not own. Folding
/// the progress block in here keeps every one of those call sites, and — the
/// part that matters — keeps **one** producer of `status.conditions` per
/// patch: a merge patch REPLACES arrays, so a second builder emitting its own
/// array would delete whatever the first one wrote. `base`'s array is read
/// back out and `RunnerReady` is merged into it.
///
/// # The two timestamp rules, and why a steady object sends nothing
///
/// Plan erratum **E11(d)**: a reconciler's own status patch is what wakes it.
///
/// * `lastTransitionTime` moves only when `stage` or `reason` moves — the
///   `metav1.Condition` rule, applied to the progress block.
/// * `lastObservedTime` is rewritten at most once per [`OBSERVED_HEARTBEAT`],
///   and only in an ACTIVE stage; a `Finished` stage clears it with an
///   explicit `null`, because "last observed" does not hold for a run that is
///   over.
///
/// Every other field is a pure function of the Job, the pod and the log, so an
/// unchanged run recomputes byte-identical bytes and
/// [`crate::conditions::status_unchanged`] sends no patch at all.
#[must_use]
pub fn apply(base: Value, write: &Write<'_>) -> Value {
    let Value::Object(mut root) = base else {
        return base;
    };
    let Some(Value::Object(status)) = root.get_mut("status") else {
        return Value::Object(root);
    };

    let d = write.derived;
    let stage = d.stage;
    let reason = d.runner_ready.reason;

    // ---- the condition -------------------------------------------------
    let next = merge_condition(
        current_condition(write.conditions, CONDITION_RUNNER_READY),
        Condition {
            r#type: CONDITION_RUNNER_READY.to_string(),
            status: d.runner_ready.status.to_string(),
            observed_generation: write.generation,
            last_transition_time: Some(write.now),
            reason: Some(reason.to_string()),
            message: Some(d.runner_ready.message.clone()),
        },
    );
    let mut owned: Vec<Value> = status
        .get("conditions")
        .and_then(|c| c.as_array())
        .cloned()
        .unwrap_or_default();
    owned.retain(|c| c.get("type") != Some(&json!(CONDITION_RUNNER_READY)));
    owned.push(json!(next));
    // AND EVERY STORED CONDITION THIS PATCH IS NOT ABOUT — defect
    // RESTORE-ADMITTED-DROPPED. The base builder's array is the array a merge
    // PATCH will REPLACE, so a type neither the builder nor this fold names is
    // deleted from the object. `Admitted` was exactly such a type:
    // `running_status_patch` writes it on the CREATING pass only, and the
    // second reconcile of an unchanged running `Restore` took it straight back
    // off — the one statement on the object saying the run had been approved,
    // gone one pass after it appeared, on an object nothing else re-asserts it
    // for. It is not a list of types to remember: `upsert_conditions` carries
    // whatever the object holds, in the order it holds it, so this stays fixed
    // for the next condition somebody adds.
    status.insert(
        "conditions".to_string(),
        Value::Array(crate::conditions::upsert_conditions(
            write.conditions,
            owned,
        )),
    );

    // ---- the scalar reason ---------------------------------------------
    //
    // D3 §2.2: `Restore.status.reason` is "the reason of the condition this
    // patch writes about current state" — the `RunnerReady` reason WHILE IT IS
    // FALSE, and otherwise whatever the base builder already wrote
    // (`JobCreated`, or the terminal reason). `every_status_write_sets_the_
    // scalar_reason` is extended by this, not weakened: the field is still
    // always set, still CamelCase, still verbatim a condition's own reason.
    if write.scalar_reason && d.runner_ready.status == "False" {
        status.insert("reason".to_string(), json!(reason));
    }

    // ---- the progress block --------------------------------------------
    let changed = write
        .stored
        .is_none_or(|p| p.stage != stage.as_str() || p.reason.as_deref() != Some(reason));
    let mut progress = serde_json::Map::new();
    progress.insert("stage".to_string(), json!(stage.as_str()));
    progress.insert("reason".to_string(), json!(reason));
    progress.insert(
        "message".to_string(),
        json!(truncate_on_char_boundary(&d.runner_ready.message, 1024)),
    );
    progress.insert(
        "lastTransitionTime".to_string(),
        json!(if changed {
            write.now
        } else {
            write
                .stored
                .and_then(|p| p.last_transition_time)
                .unwrap_or(write.now)
        }),
    );
    let observed = heartbeat(
        write.stored.and_then(|p| p.last_observed_time),
        write.now,
        stage.is_active(),
    );
    progress.insert(
        "lastObservedTime".to_string(),
        // EXPLICIT `null` AND NOT AN OMISSION. A merge patch that omits a key
        // leaves the stored value alone, and "this run was last observed at
        // 14:02" is false the moment the run is over — D3 §2.2's "a status
        // field cleared with explicit null when it no longer holds".
        observed.map_or(Value::Null, |t| json!(t)),
    );
    progress.insert(
        "runner".to_string(),
        serde_json::to_value(&d.runner).unwrap_or(Value::Null),
    );
    progress.insert(
        "runnerPhase".to_string(),
        write
            .progress
            .phase
            .as_ref()
            .and_then(|p| serde_json::to_value(p).ok())
            .unwrap_or(Value::Null),
    );
    progress.insert(
        "diagnostics".to_string(),
        merge_diagnostics(
            write.stored.and_then(|p| p.diagnostics.as_ref()),
            d.diagnosis.as_ref(),
            observed.unwrap_or(write.now),
        )
        .map_or(Value::Null, |v| json!(v)),
    );
    status.insert("progress".to_string(), Value::Object(progress));
    Value::Object(root)
}

/// The `lastObservedTime` rule: keep the stored value until
/// [`OBSERVED_HEARTBEAT`] has passed, and drop it entirely once the stage is
/// not active.
#[must_use]
pub fn heartbeat(
    stored: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    active: bool,
) -> Option<DateTime<Utc>> {
    if !active {
        return None;
    }
    match stored {
        Some(t) if (now - t).to_std().is_ok_and(|d| d < OBSERVED_HEARTBEAT) => Some(t),
        _ => Some(now),
    }
}

/// The diagnostics list D3 §2.3 describes: deduplicated on
/// `(code, object.kind, object.name)`, sorted by `lastSeen` newest first, and
/// truncated to eight.
///
/// # `count` counts OBSERVATIONS, not Events, and the difference is recorded
///
/// D3 §2.3 says `count` increments by the Event `count`/series delta. The fact
/// slice this controller is handed is [`EventFact`], D2's four-field value
/// type, which carries no `count` — and widening it is an edit to D2's
/// classifier, not to this task's files. So `count` increments once per
/// HEARTBEAT: it moves exactly when `lastSeen` moves, which is at most once
/// per [`OBSERVED_HEARTBEAT`]. That keeps the number meaningful ("this has
/// been true for `count` minutes"), keeps it bounded, and — the reason it is
/// not simply incremented per reconcile — keeps a steady object's status
/// byte-identical between heartbeats, which is the whole of E11(d).
#[must_use]
pub fn merge_diagnostics(
    stored: Option<&Vec<Diagnostic>>,
    next: Option<&Diagnosis>,
    last_seen: DateTime<Utc>,
) -> Option<Vec<Diagnostic>> {
    let mut by_key: BTreeMap<(String, String, String), Diagnostic> = BTreeMap::new();
    let mut order: Vec<(String, String, String)> = Vec::new();
    for d in stored.into_iter().flatten() {
        let key = diagnostic_key(d);
        if by_key.insert(key.clone(), d.clone()).is_none() {
            order.push(key);
        }
    }
    if let Some(n) = next {
        let key = (
            n.code.as_str().to_string(),
            n.object_kind.to_string(),
            n.object_name.clone(),
        );
        match by_key.get_mut(&key) {
            Some(existing) => {
                // THE HEARTBEAT, AND NOTHING ELSE. `firstSeen` never moves;
                // `lastSeen` and `count` move together or not at all.
                if existing.last_seen != Some(last_seen) {
                    existing.last_seen = Some(last_seen);
                    existing.count = Some(
                        existing
                            .count
                            .unwrap_or(1)
                            .saturating_add(1)
                            .min(DIAGNOSTIC_COUNT_CAP),
                    );
                }
                existing.message = Some(n.message.clone());
                existing.severity = n.code.severity().as_str().to_string();
            }
            None => {
                by_key.insert(
                    key.clone(),
                    Diagnostic {
                        code: n.code.as_str().to_string(),
                        severity: n.code.severity().as_str().to_string(),
                        message: Some(n.message.clone()),
                        object: Some(DiagnosticObject {
                            kind: n.object_kind.to_string(),
                            name: n.object_name.clone(),
                        }),
                        first_seen: Some(last_seen),
                        last_seen: Some(last_seen),
                        count: Some(1),
                    },
                );
                order.push(key);
            }
        }
    }
    let mut out: Vec<Diagnostic> = order
        .into_iter()
        .filter_map(|k| by_key.remove(&k))
        .collect();
    if out.is_empty() {
        return None;
    }
    // NEWEST `lastSeen` FIRST. `sort_by` is STABLE, so two diagnostics seen in
    // the same heartbeat keep the order they were first recorded in — a total
    // order, so a steady object computes the same array on every pass.
    out.sort_by(|a, b| b.last_seen.cmp(&a.last_seen));
    out.truncate(DIAGNOSTICS_MAX);
    Some(out)
}

/// D3 §2.2's `maxItems: 8`. A status is not a log.
pub const DIAGNOSTICS_MAX: usize = 8;

/// D3 §2.2's `count` ceiling. A status is not a counter store, and an
/// unbounded integer here is an unbounded write rate.
pub const DIAGNOSTIC_COUNT_CAP: i64 = 1_000_000;

/// A stored diagnostic's dedup key.
fn diagnostic_key(d: &Diagnostic) -> (String, String, String) {
    let (kind, name) = d
        .object
        .as_ref()
        .map_or((String::new(), String::new()), |o| {
            (o.kind.clone(), o.name.clone())
        });
    (d.code.clone(), kind, name)
}

// ===========================================================================
// Fail fast — D3 §2.3
// ===========================================================================

/// Whether this pass should cancel the Job before it does any work — D3
/// §2.3's "fail fast before any work".
///
/// Four conditions, and all four are required:
///
/// 1. there IS a diagnosis;
/// 2. it is **not transient** at the duration it has held;
/// 3. the runner container has **never started** — a run whose process began
///    has consumed its approval and its plan, and the cancellation D2's
///    `cancel.rs` performs is an `activeDeadlineSeconds` patch, not a rollback;
/// 4. it has held continuously for at least `fail_fast`.
///
/// `held_for` is computed from the STORED diagnostic's `firstSeen`, which is
/// what makes "continuously" true rather than "at some point": a diagnosis
/// that cleared and came back gets a new `firstSeen`, because
/// [`merge_diagnostics`] only keeps the old one while the key keeps matching.
#[must_use]
pub fn should_fail_fast(
    diagnosis: Option<&Diagnosis>,
    started: bool,
    held_for: Option<Duration>,
    fail_fast: Duration,
) -> bool {
    let Some(d) = diagnosis else { return false };
    if started {
        return false;
    }
    let Some(held) = held_for else { return false };
    if d.code.transient(held) {
        return false;
    }
    held >= fail_fast
}

/// How long the stored status says this diagnosis has held.
#[must_use]
pub fn held_for(
    stored: Option<&RunProgress>,
    diagnosis: &Diagnosis,
    now: DateTime<Utc>,
) -> Option<Duration> {
    let first = stored?
        .diagnostics
        .as_ref()?
        .iter()
        // THE WHOLE DEDUP KEY — `(code, object.kind, object.name)`, D3 §2.3's,
        // and review finding F10. Matching on the name alone would let a Job
        // and a Pod of the same name share one `firstSeen`, and "continuously
        // observed for `failFastSeconds`" would then be measured from another
        // object's clock. Unreachable today (a Job's pod always carries the
        // generated suffix) and it is the fail-fast PRECONDITION, so it is
        // written as the whole key rather than as most of it.
        .find(|d| {
            d.code == diagnosis.code.as_str()
                && d.object
                    .as_ref()
                    .map(|o| (o.kind.as_str(), o.name.as_str()))
                    == Some((diagnosis.object_kind, diagnosis.object_name.as_str()))
        })?
        .first_seen?;
    (now - first).to_std().ok()
}

/// The terminal state a fail-fast cancellation writes — D3 §2.2's four new
/// members of [`crate::conditions::TERMINAL_STATES`], plus the two that were
/// already there.
///
/// It is the diagnosis's `RunnerReady` PROJECTION and not its raw code,
/// because that is the value D3 §15's L1 and L2 assert live
/// (`Failed/CredentialReferenceMissing`, `Failed/PodUnschedulable`) and
/// because the raw code vocabulary is not a `TERMINAL_STATES` vocabulary.
#[must_use]
pub fn terminal_state(diagnosis: &Diagnosis) -> Option<&'static str> {
    let reason = diagnosis.code.runner_ready_reason()?;
    // `WaitingForPod` is NOT a terminal state and never becomes one: "nothing
    // has happened yet" is the one answer that cannot be a verdict.
    (reason != REASON_WAITING_FOR_POD).then_some(reason)
}

// ===========================================================================
// The observation seam both reconcilers call
// ===========================================================================

/// One pass's whole observation of a running Job-backed run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Run {
    /// The derivation.
    pub derived: Derived,
    /// What the progress channel said, or what the object already said when
    /// this pass was throttled out of reading it.
    pub progress: Progress,
    /// The owned pod's name, when there was one — so a caller can log it.
    pub pod_name: Option<String>,
}

/// **The seam.** Observe one unfinished runner Job: find its owned pod, list
/// the events that explain it when it is not running, read the progress
/// channel when it is, and derive.
///
/// ONE IMPLEMENTATION FOR BOTH RECONCILERS — D3 §2.3's "one implementation for
/// every Job-backed run". `controllers/backup.rs` and `controllers/restore.rs`
/// call exactly this; neither classifies anything itself.
///
/// # What is read, and what is not
///
/// * The pod, through [`crate::check::pod::find_owned_pod_by_selectors`],
///   which verifies the controller owner UID (seam **S6**).
/// * Core `v1/events`, **only when the pod is not Running** — D3 §2.3. A
///   healthy run costs one pod list per reconcile and nothing else.
/// * The pod log, **only when the runner has started and the throttle allows
///   it** ([`should_read_progress`]).
///
/// # Errors
///
/// [`kube::Error`] from the pod list alone. The events list and the log read
/// are best effort by design; see [`events_for`] and [`read_progress`].
pub async fn observe(
    client: &kube::Client,
    namespace: &str,
    job: &Job,
    selectors: &[String],
    stored: Option<&RunProgress>,
    now: DateTime<Utc>,
) -> Result<Run, kube::Error> {
    let job_uid = job.uid();
    let found = crate::check::pod::find_owned_pod_by_selectors(
        client,
        namespace,
        &job.name_any(),
        job_uid.as_deref(),
        selectors,
    )
    .await?;
    let pod = found.pod.as_ref();
    let pod_name = pod.map(kube::ResourceExt::name_any);
    let running = pod.and_then(runner_container).is_some_and(|c| {
        c.state
            .as_ref()
            .is_some_and(|s| s.running.is_some() || s.terminated.is_some())
    });
    // D3 §2.3: the events list is made ONLY while the pod is not running. A
    // healthy run must not pay a namespace-wide list every fifteen seconds
    // for a diagnostic it will never need.
    let events = if running {
        Vec::new()
    } else {
        events_for(
            client,
            namespace,
            &[pod.and_then(kube::ResourceExt::uid), job_uid],
        )
        .await
    };
    // DERIVED TWICE, AND DELIBERATELY. The first derivation answers "has the
    // runner started", which is what decides whether the progress read happens
    // at all; the second carries the phase that read produced, because D3
    // §2.5's `Verifying` stage is a function of it.
    let first = derive(&Facts {
        job,
        pod,
        events: &events,
        contested: found.contested.len(),
        runner_phase: None,
        now,
    });
    let progress = match (&pod_name, should_read_progress(stored, first.started, now)) {
        (Some(name), true) => read_progress(client, namespace, name, stored).await,
        _ => Progress::carried(stored),
    };
    let derived = derive(&Facts {
        job,
        pod,
        events: &events,
        contested: found.contested.len(),
        runner_phase: progress.phase.as_ref(),
        now,
    });
    Ok(Run {
        derived,
        progress,
        pod_name,
    })
}

/// Cancel a runner Job that cannot start — D3 §2.3's "fail fast before any
/// work", through **D2's `cancel.rs`** and not a second cancellation path.
///
/// Returns the terminal state the cancellation is FOR, or `None` when nothing
/// was sent. The Job then fails with `DeadlineExceeded`, the existing
/// crashed-Job path runs on the next pass, and
/// [`recorded_terminal_state`] turns the diagnostic this pass recorded into
/// the terminal reason — which is why the status patch that records it must
/// have landed BEFORE this is called.
///
/// # Errors
///
/// [`kube::Error`] from the deadline patch.
pub async fn fail_fast(
    client: &kube::Client,
    namespace: &str,
    job: &Job,
    owner_uid: &str,
    run: &Run,
    stored: Option<&RunProgress>,
    now: DateTime<Utc>,
) -> Result<Option<&'static str>, kube::Error> {
    let Some(diagnosis) = run.derived.diagnosis.as_ref() else {
        return Ok(None);
    };
    // OFF IS OFF, AND IT IS CHECKED FIRST. `None` means the installation asked
    // for no fail-fast at all, so no Job is cancelled and every run reaches
    // its own `activeDeadlineSeconds` exactly as it did before this behaviour
    // existed.
    let Some(window) = fail_fast_seconds() else {
        return Ok(None);
    };
    let held = held_for(stored, diagnosis, now);
    if !should_fail_fast(Some(diagnosis), run.derived.started, held, window) {
        return Ok(None);
    }
    let Some(state) = terminal_state(diagnosis) else {
        return Ok(None);
    };
    if !crate::check::cancel(client, namespace, job, owner_uid).await? {
        return Ok(None);
    }
    warn!(
        namespace,
        job = %job.name_any(),
        code = %diagnosis.code,
        terminal_state = state,
        held_seconds = held.map_or(0, |d| d.as_secs()),
        "this run cannot start and waiting cannot fix it, so its Job's deadline is collapsed \
         now rather than at its own activeDeadlineSeconds. No data-plane process ever ran, so \
         no approval, plan or archive state was consumed; a new attempt is a new object"
    );
    Ok(Some(state))
}

/// Whether this Job is one whose missing TTL may be repaired — D3 §2.7's three
/// conditions, as a PURE predicate.
///
/// Separate from [`repair_ttl`] because it is the whole of the rule, and a
/// rule inside an `async fn` that needs a `kube::Client` is a rule whose only
/// test is a route table. A route table can prove the patch was not sent; it
/// cannot easily prove WHICH of the three conditions stopped it, and on the
/// reconcile path the compatibility guard refuses a foreign Job before this is
/// ever reached — so the belt would hide whether the braces exist at all.
///
/// 1. **Finished.** An unfinished Job with a TTL is a Job with a deadline it
///    did not ask for.
/// 2. **No TTL already.** Re-sending the same value every reconcile is the
///    write loop E11(d) is about.
/// 3. **Controlled by exactly this object.** A Job named after the CR proves
///    nothing — the name is derived, not owned. Patching a stranger's Job with
///    a TTL is deleting somebody else's work on a timer.
#[must_use]
pub fn needs_ttl_repair(job: &Job, owner_uid: &str) -> bool {
    if !crate::controllers::backup::job_finished(job) {
        return false;
    }
    if job
        .spec
        .as_ref()
        .and_then(|s| s.ttl_seconds_after_finished)
        .is_some()
    {
        return false;
    }
    !owner_uid.is_empty()
        && job
            .meta()
            .owner_references
            .as_deref()
            .unwrap_or_default()
            .iter()
            .any(|o| o.uid == owner_uid && o.controller == Some(true))
}

/// Patch a finished Job's missing `ttlSecondsAfterFinished` — D3 §2.7's
/// repair.
///
/// Four conditions, and the caller has already established the first: the
/// object's status is TERMINAL. Here: the Job is finished, it carries exactly
/// this object's controller owner, and it has no TTL. **"Status first, TTL
/// second" is preserved by construction** — this runs only on a pass over an
/// object whose terminal status is already stored.
///
/// Returns whether a patch was sent.
///
/// # Errors
///
/// [`kube::Error`] from the patch.
pub async fn repair_ttl(
    client: &kube::Client,
    namespace: &str,
    job: &Job,
    owner_uid: &str,
) -> Result<bool, kube::Error> {
    if !needs_ttl_repair(job, owner_uid) {
        return Ok(false);
    }
    let jobs: kube::Api<Job> = kube::Api::namespaced(client.clone(), namespace);
    jobs.patch(
        &job.name_any(),
        &kube::api::PatchParams::default(),
        &kube::api::Patch::Merge(
            json!({ "spec": { "ttlSecondsAfterFinished": job_ttl_seconds() } }),
        ),
    )
    .await?;
    warn!(
        namespace,
        job = %job.name_any(),
        ttl_seconds = job_ttl_seconds(),
        "this run is terminal and its finished Job carried no ttlSecondsAfterFinished; the TTL \
         is repaired so the Job and its pod are collected. The status was already written, so \
         nothing the DTO reads depends on either of them"
    );
    Ok(true)
}

/// Move `status.progress` to [`Stage::Finished`] on a terminal `/status`
/// merge patch — D3 §2.2 and §2.5.
///
/// # Why a terminal patch has to say this at all
///
/// `progress.stage` is what the console renders and what D3 §2.5 normalizes
/// into the API `state`. A terminal patch that left the block alone would
/// leave a finished run reading `stage: Running` with a `lastObservedTime`
/// that stops moving — which §2.5's staleness row turns into `unknown`
/// (`StatusStale`) three hundred seconds later. A run that succeeded would
/// render as a run nobody can account for.
///
/// # A PARTIAL merge, and that is the difference from [`apply`]
///
/// This writes four keys. `runner`, `runnerPhase` and `diagnostics` are
/// **omitted**, which in RFC 7386 means "leave them alone": the last runner
/// facts, the last phase and the diagnostics that explain the outcome are
/// exactly what an operator looking at a failed run needs, and re-deriving
/// them from a pod that may already be collected is the defect step 2b's guard
/// exists to prevent. `lastObservedTime` is cleared with an **explicit null**
/// because "last observed" does not hold for a run that is over.
///
/// The reason is read out of `base`'s own first condition, which every
/// terminal builder in this crate puts its terminal condition at — so the
/// progress block cannot disagree with the condition it is about.
#[must_use]
pub fn apply_finished(base: Value, stored: Option<&RunProgress>, now: DateTime<Utc>) -> Value {
    let Value::Object(mut root) = base else {
        return base;
    };
    let reason = root
        .get("status")
        .and_then(|s| s.pointer("/conditions/0/reason"))
        .and_then(|r| r.as_str())
        .map(str::to_string);
    let message = root
        .get("status")
        .and_then(|s| s.pointer("/conditions/0/message"))
        .and_then(|m| m.as_str())
        .map(|m| truncate_on_char_boundary(&sanitize(m), 1024));
    let Some(Value::Object(status)) = root.get_mut("status") else {
        return Value::Object(root);
    };
    let changed = stored.is_none_or(|p| {
        p.stage != Stage::Finished.as_str() || p.reason.as_deref() != reason.as_deref()
    });
    let mut progress = serde_json::Map::new();
    progress.insert("stage".to_string(), json!(Stage::Finished.as_str()));
    if let Some(reason) = reason {
        progress.insert("reason".to_string(), json!(reason));
    }
    if let Some(message) = message {
        progress.insert("message".to_string(), json!(message));
    }
    progress.insert(
        "lastTransitionTime".to_string(),
        json!(if changed {
            now
        } else {
            stored.and_then(|p| p.last_transition_time).unwrap_or(now)
        }),
    );
    progress.insert("lastObservedTime".to_string(), Value::Null);
    status.insert("progress".to_string(), Value::Object(progress));
    Value::Object(root)
}
