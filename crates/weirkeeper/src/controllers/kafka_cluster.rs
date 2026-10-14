//! The `KafkaCluster` probe reconciler: the loop that makes `status.reachable`
//! mean something.
//!
//! # The controller never dials a broker, so a probe is a Job
//!
//! The control plane holds no `get` on Secrets (spec §9), so a SASL password
//! never reaches this process and it cannot authenticate to a cluster that
//! requires one. The dial happens where the credential can be projected: a
//! short-lived runner Job (Global Constraint 33), whose `valueFrom.secretKeyRef`
//! names the Secret this controller cannot read.
//!
//! The subcommand is `logweir cluster-probe`, not `logweir doctor` (see
//! `crates/logweir/src/probe.rs`). Interface **I14** is the contract this module
//! reads: exactly two stdout lines, `cluster-id=<id>` then
//! `reachable=true|false`, read by key name from a bounded tail through
//! [`runner_log::key_values`], because a pod log merges stdout and stderr in
//! nondeterministic order.
//!
//! # Nothing is guessed
//!
//! A log carrying neither line yields `reachable: None`, no `clusterId`, and
//! [`REASON_PROBE_OUTPUT_UNREADABLE`]. The exit code alone never writes a
//! `reachable` value: exit 1 covers an unreachable broker, a missing runner
//! binary, a bad flag and an unprojected credential alike, so inferring
//! `reachable: false` from it would report a control-plane mistake as a fact
//! about somebody's cluster.
//!
//! # A probe whose pod is refused at creation says so
//!
//! A `ResourceQuota`, a `LimitRange` or an admission webhook can refuse the
//! probe pod, leaving only a `FailedCreate` Event on the Job. Once the Job has
//! had no pod for [`check::waiting::POD_CREATE_GRACE`] and that Event says why,
//! the Job is cancelled and the status reads `Reachable=Unknown` /
//! `PodCreationForbidden` with the admission's own words. `reachable` is
//! cleared; `clusterId` and `observedAt` keep their last values and age exactly
//! as they would with no probe. The Job then gets the usual TTL, so the next
//! probe runs on the ordinary cadence.
//!
//! # It creates, it patches, and it deletes nothing
//!
//! The re-probe cadence is the Job's own [`PROBE_TTL_SECONDS`]: the API server
//! garbage-collects the finished Job, the next reconcile finds none, and a fresh
//! probe runs. This reconciler deletes no Job, no pod and no object ([`super`]'s
//! rule).
//!
//! # A Job Kubernetes is collecting is not a probe
//!
//! The TTL controller deletes a finished Job with foreground propagation: the
//! Job gains a `deletionTimestamp`, its pod goes first, and the Job lingers
//! until the pod is gone. Read naively, that pod-less finished Job is a crashed
//! probe. So, before any verdict is formed:
//!
//! * **a Job with a `deletionTimestamp` is not judged** (no pod read, no status
//!   write, no TTL patch: [`Deferred::JobBeingDeleted`]);
//! * **a finished Job whose verdict is already recorded is not judged again**.
//!   The marker is [`VERDICT_RECORDED_ANNOTATION`], which this reconciler sets
//!   beside the TTL only after the status write that recorded the verdict
//!   returned ([`Deferred::VerdictRecorded`]);
//! * **`NotFound` and `Conflict` while a Job is collected are expected**: a pod
//!   gone before its log read ([`Deferred::PodGone`]), a Job gone before its TTL
//!   patch, and a status write that lost its `resourceVersion` precondition
//!   ([`Deferred::StatusSuperseded`]) are debug lines, never WARN and never a
//!   reconcile error.
//!
//! The last recorded verdict therefore stands until the next probe answers, and
//! a crash, an unreadable log or a refused pod is logged at WARN once per Job:
//! on the pass whose status write changed the object.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, PostParams};
use kube::runtime::controller::Action;
use kube::runtime::{watcher, Controller};
use kube::{Resource, ResourceExt as _};
use serde_json::{json, Value};
use std::fmt;
use tracing::{debug, info, warn};

use super::backup;
use super::Context;
use crate::check;
use crate::conditions::{
    current_condition, merge_condition, StatusVersion, TERMINAL_STATE_NAME_TOO_LONG,
    TERMINAL_STATE_POD_OWNERSHIP_CONTESTED,
};
use crate::connection::{self, ConnectionUse, ResolvedConnection};
use crate::crds::kafka_cluster::{AuthMode, KafkaCluster};
use crate::job::{self, RunnerJobSpec, RunnerOwner};
use crate::runner_log;

/// WARN on the pass whose status write first recorded a crash or a refusal,
/// debug on every later pass that finds it already recorded. `$first` is
/// whether that pass's write changed the object ([`StatusWrite::Changed`]).
macro_rules! warn_when_first_recorded {
    ($first:expr, $($event:tt)+) => {
        if $first {
            warn!($($event)+);
        } else {
            debug!($($event)+);
        }
    };
}

/// The `Job` name a `KafkaCluster` gets: `logweir-probe-<cr name>`.
///
/// Prefixed, unlike the `Backup` and `Restore` Jobs: a `KafkaCluster` is a
/// long-lived object other kinds reference by name, and a Job sharing its name
/// could collide with anything else created for the same cluster. The prefix
/// costs thirteen characters of the 63-character `job-name` label budget, hence
/// [`name_limit_for_cluster`].
pub const PROBE_JOB_PREFIX: &str = "logweir-probe-";

/// The environment variable the probe reads its SASL password from.
///
/// `LOGWEIR_SOURCE_PASSWORD` for a cluster in either role: a probe reads one
/// cluster, so it reads the "source" variable whatever `spec.role` says.
/// `the_probe_password_variable_matches_the_subcommand` pins it against
/// `logweir cluster-probe`'s help text.
pub const SOURCE_PASSWORD_ENV: &str = "LOGWEIR_SOURCE_PASSWORD";

/// The key inside a `KafkaCluster`'s `auth.secretRef` Secret that holds the
/// password: the key the `Restore` path projects
/// ([`super::restore::TARGET_PASSWORD_SECRET_KEY`]), so one Secret serves both.
pub const SOURCE_PASSWORD_SECRET_KEY: &str = super::restore::TARGET_PASSWORD_SECRET_KEY;

/// Interface **I14**'s first line, as a prefix. Matched by name.
pub const CLUSTER_ID_PREFIX: &str = "cluster-id=";

/// Interface **I14**'s second line, as a prefix. Matched by name.
pub const REACHABLE_PREFIX: &str = "reachable=";

/// FX-46's third line, `brokers-answered=<answered>/<listed>`: how many of
/// the brokers the cluster lists answered the probe on their advertised
/// addresses. Matched by name; absent from an older runner's log.
pub const BROKERS_ANSWERED_PREFIX: &str = "brokers-answered=";

/// The one condition type a `KafkaCluster` carries. Declared here rather than in
/// [`crate::conditions`], which holds the exit-code vocabulary and the terminal
/// states an execution can reach; a probe's condition is neither.
pub const CONDITION_REACHABLE: &str = "Reachable";

/// `Reachable=True`. A broker answered and named its cluster id.
pub const REASON_REACHABLE: &str = "Reachable";

/// `Reachable=False`: the probe ran and printed `reachable=false` itself. Never
/// derived from the exit code, since exit 1 alone could mean a wrong image.
pub const REASON_PROBE_REPORTED_UNREACHABLE: &str = "ProbeReportedUnreachable";

/// `Reachable=Unknown`. The pod log carried neither contract line, so nothing
/// about the cluster is known either way.
pub const REASON_PROBE_OUTPUT_UNREADABLE: &str = "ProbeOutputUnreadable";

/// `Reachable=False` (FX-46): the bootstrap address answered and named the
/// cluster, and a broker the cluster advertises did not answer the probe on
/// its advertised address, so no run through this connection can reach every
/// partition. The probe's `brokers-answered=` line says how many of how many
/// did; its log names each silent broker.
pub const REASON_ADVERTISED_BROKERS_UNREACHABLE: &str = "AdvertisedBrokersUnreachable";

/// `Reachable=Unknown`. A probe Job exists and has not finished.
///
/// Written on the creating pass: an empty status is indistinguishable from an
/// object this controller has never seen. The merge patch touches neither
/// `reachable` nor `clusterId`, so the last known values stand while a fresh
/// probe is in flight.
pub const REASON_PROBE_RUNNING: &str = "ProbeRunning";

/// Every condition `reason` this module writes, in one list that
/// `every_probe_condition_reason_is_a_valid_metav1_reason` checks against the
/// `metav1.Condition` reason pattern, which forbids `-`.
pub const PROBE_CONDITION_REASONS: &[&str] = &[
    REASON_REACHABLE,
    REASON_PROBE_REPORTED_UNREACHABLE,
    REASON_PROBE_OUTPUT_UNREADABLE,
    REASON_ADVERTISED_BROKERS_UNREACHABLE,
    REASON_PROBE_RUNNING,
    // The last reading is too old to vouch for. A probe reason, not a terminal
    // state: the next probe that answers replaces it.
    REASON_PROBE_STALE,
    crate::conditions::TERMINAL_STATE_CONNECTION_CONFIG_INVALID,
    crate::conditions::TERMINAL_STATE_CONNECTION_REFERENCE_INVALID,
    crate::conditions::TERMINAL_STATE_CONNECTION_FIELD_UNSUPPORTED,
    crate::conditions::TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
    // The resolver's PLAIN-without-TLS refusal, and the probe's refusal of a
    // credential whose binding names another connection.
    crate::conditions::TERMINAL_STATE_PLAIN_WITHOUT_TLS,
    crate::conditions::TERMINAL_STATE_CREDENTIAL_BINDING_MISMATCH,
    // The probe pod was refused at creation: the terminal state a `Backup`'s or
    // a `Restore`'s runner reaches for the same refusal.
    crate::conditions::TERMINAL_STATE_POD_CREATION_FORBIDDEN,
];

/// `spec.activeDeadlineSeconds` on a probe Job. Two minutes: the subcommand
/// bounds the dial at ten seconds and the rest is image start, so a deadline
/// that fires means the pod never got going.
pub const PROBE_DEADLINE_SECONDS: i64 = 120;

/// `ttlSecondsAfterFinished` on a finished probe Job, and therefore the
/// re-probe interval.
///
/// A TTL rather than a timer because a reconciler here deletes nothing: the API
/// server collects the finished Job, the next reconcile finds none, and a fresh
/// probe runs. Patched only after the status write carrying the verdict
/// returned, so pod garbage collection never races the log read.
pub const PROBE_TTL_SECONDS: i32 = 300;

/// How long before a probe Job that has not finished is looked at again.
pub const REQUEUE_SECS: u64 = 15;

/// How long before a cluster whose probe has been read is probed again.
///
/// [`PROBE_TTL_SECONDS`] plus a margin: a requeue at exactly the TTL would find
/// the Job still present about half the time and wait another full interval.
/// The Job's deletion is also a watch event (`controller().owns(jobs)`), so
/// this timer is the backstop.
pub const RE_PROBE_SECS: u64 = PROBE_TTL_SECONDS as u64 + 15;

/// The longest `metadata.name` a `KafkaCluster` may have and still be probed.
///
/// The Job's pods carry [`PROBE_JOB_PREFIX`] + the cluster's name in the
/// `batch.kubernetes.io/job-name` label, whose values stop at
/// [`crate::slot::NAME_LIMIT`] characters; the API server refuses a longer one
/// outright, which would leave the object with `status: null` forever.
#[must_use]
pub fn name_limit_for_cluster() -> usize {
    crate::slot::NAME_LIMIT.saturating_sub(PROBE_JOB_PREFIX.len())
}

/// The probe Job's name for a cluster: `logweir-probe-<cr name>`.
#[must_use]
pub fn probe_job_name(cluster_name: &str) -> String {
    format!("{PROBE_JOB_PREFIX}{cluster_name}")
}

/// Interface **I14**'s two lines, as read off a pod log.
///
/// Two `Option`s, INDEPENDENTLY, and no key is ever derived from the other: a
/// log carrying only `cluster-id=` says nothing about reachability, and a log
/// carrying only `reachable=true` names no cluster.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProbeReport {
    /// `cluster-id=<id>`'s value, when it was **non-empty**. The unreachable
    /// arm of interface I14 prints the key with an EMPTY value, which is an
    /// absence and is recorded as one.
    pub cluster_id: Option<String>,
    /// `reachable=`'s value, when it was exactly `true` or exactly `false`.
    /// Anything else — an absent line, a truncated one, a third spelling — is
    /// `None`, and `None` is never turned into `false`.
    pub reachable: Option<bool>,
    /// `true` when the tail carries the probe's stderr line
    /// `refusal-reason=CredentialBindingMismatch`: the probe refused the
    /// projected credential before dialling, because its binding does not name
    /// this `KafkaCluster`. Read only beside `reachable=false`; it can never
    /// make a probe read as reachable.
    pub credential_binding_refused: bool,
    /// FX-46's `brokers-answered=<answered>/<listed>`, when the line is there
    /// and is two numbers with `answered <= listed`, or `<answered>/?` (the
    /// broker list could not be read: `listed` is `None`). Read only to NAME
    /// a `reachable=false`; it never makes a probe read as reachable.
    pub brokers_answered: Option<(u32, Option<u32>)>,
}

/// Read the two contract lines out of a pod log, by key name, from a bounded
/// tail ([`runner_log::key_values`], the one key scan in this crate). The last
/// occurrence of each key wins, as in [`backup::evidence_keys`].
///
/// An empty `cluster-id=` (interface I14's unreachable arm) is an absence:
/// `Some("")` in `status.clusterId` would later be compared against a target's
/// id by Global Constraint 18's fourth rail, and a rail that compares two
/// absences passes.
#[must_use]
pub fn probe_report(log: &str) -> ProbeReport {
    let refused = format!(
        "refusal-reason={}",
        crate::conditions::TERMINAL_STATE_CREDENTIAL_BINDING_MISMATCH
    );
    ProbeReport {
        cluster_id: runner_log::key_values(log, CLUSTER_ID_PREFIX)
            .pop()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_string),
        reachable: runner_log::key_values(log, REACHABLE_PREFIX)
            .pop()
            .and_then(|v| match v.trim() {
                "true" => Some(true),
                "false" => Some(false),
                _ => None,
            }),
        credential_binding_refused: runner_log::tail_lines(log)
            .iter()
            .any(|line| line.trim() == refused),
        brokers_answered: runner_log::key_values(log, BROKERS_ANSWERED_PREFIX)
            .pop()
            .and_then(|v| {
                let (a, l) = v.trim().split_once('/')?;
                let a = a.parse::<u32>().ok()?;
                if l == "?" {
                    return Some((a, None));
                }
                let l = l.parse::<u32>().ok()?;
                (a <= l).then_some((a, Some(l)))
            }),
    }
}

/// What one reading of a probe's log decided.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Verdict {
    /// The `Reachable` condition's `status` — `True`, `False` or `Unknown`.
    pub status: &'static str,
    /// The condition's `reason`, and verbatim the scalar `status.reason`.
    pub reason: String,
    /// The value for `status.reachable`. `None` clears the field
    /// ([`observed_status_patch`] writes `null`): an unreadable probe vouches
    /// for no earlier reading, and admission reads `reachable == true` alone.
    pub reachable: Option<bool>,
    /// The value for `status.clusterId`, written only alongside
    /// `reachable: Some(true)`.
    pub cluster_id: Option<String>,
    /// FX-46: how many of the advertised brokers answered (of how many, when
    /// their list was read), for the message of an
    /// [`REASON_ADVERTISED_BROKERS_UNREACHABLE`] verdict.
    pub brokers_answered: Option<(u32, Option<u32>)>,
}

/// The verdict for one finished probe, from its log alone.
///
/// The signature is the property: this function cannot see the exit code.
/// Exit 1 is what a wrong image, a bad flag, an unprojected credential and an
/// unreachable broker all produce, so the code reaches only the condition
/// message ([`verdict_message`]), and a run with no code at all is
/// [`backup::crash_terminal_state`]'s to classify.
#[must_use]
pub fn verdict(report: &ProbeReport) -> Verdict {
    match report.reachable {
        Some(true) => Verdict {
            status: "True",
            reason: REASON_REACHABLE.to_string(),
            reachable: Some(true),
            cluster_id: report.cluster_id.clone(),
            brokers_answered: report.brokers_answered,
        },
        // The probe refused the credential's binding and dialled nothing: the
        // named reason, not "unreachable".
        Some(false) if report.credential_binding_refused => Verdict {
            status: "False",
            reason: crate::conditions::TERMINAL_STATE_CREDENTIAL_BINDING_MISMATCH.to_string(),
            reachable: Some(false),
            cluster_id: None,
            brokers_answered: None,
        },
        // FX-46: the bootstrap address answered and an advertised broker did
        // not. Named, so an operator is sent to `advertised.listeners` rather
        // than to the bootstrap address, the one thing that works.
        Some(false)
            if report
                .brokers_answered
                .is_some_and(|(answered, listed)| listed.is_none_or(|l| answered < l)) =>
        {
            Verdict {
                status: "False",
                reason: REASON_ADVERTISED_BROKERS_UNREACHABLE.to_string(),
                reachable: Some(false),
                // Not recorded: `status.clusterId` is written beside a
                // reachable cluster only (Global Constraint 18's fourth rail
                // compares it).
                cluster_id: None,
                brokers_answered: report.brokers_answered,
            }
        }
        Some(false) => Verdict {
            status: "False",
            reason: REASON_PROBE_REPORTED_UNREACHABLE.to_string(),
            reachable: Some(false),
            // Never carried over: an unreachable probe observed no cluster id.
            cluster_id: None,
            brokers_answered: None,
        },
        None => Verdict {
            status: "Unknown",
            reason: REASON_PROBE_OUTPUT_UNREADABLE.to_string(),
            reachable: None,
            cluster_id: None,
            brokers_answered: None,
        },
    }
}

/// The verdict for a probe Job given FX-43's token whose runner vouched for
/// no exit: the runner image is older than the controller. Nothing from its
/// log is read, `reachable` is cleared (an older probe's lines are not this
/// controller's to trust), and the reason names the pair.
#[must_use]
pub fn runner_older_verdict() -> Verdict {
    Verdict {
        status: "Unknown",
        reason: crate::conditions::TERMINAL_STATE_RUNNER_OLDER_THAN_CONTROLLER.to_string(),
        reachable: None,
        cluster_id: None,
        brokers_answered: None,
    }
}

/// The condition MESSAGE for a verdict, naming the exit code the probe pod
/// reported.
#[must_use]
pub fn verdict_message(v: &Verdict, exit_code: i32) -> String {
    if v.reason == crate::conditions::TERMINAL_STATE_RUNNER_OLDER_THAN_CONTROLLER {
        return format!(
            "{}: the probe printed no line sealed with its Job's line token (pod exit \
             {exit_code}), which every runner at least as new as this controller does, so the \
             runner image is older than the controller (an older one stops at the unknown \
             argument `{}`). Nothing from its log was read and `reachable` is cleared. Upgrade \
             the runner image to the controller's version (the chart's `runnerImage`)",
            v.reason,
            logweir_core::runner_line::RUNNER_LINE_TOKEN_ARG
        );
    }
    if let (REASON_ADVERTISED_BROKERS_UNREACHABLE, Some((answered, listed))) =
        (v.reason.as_str(), v.brokers_answered)
    {
        let how_many = match listed {
            Some(listed) => format!(
                "only {answered} of the {listed} broker(s) the cluster advertises answered the \
                 probe on their advertised addresses"
            ),
            None => "no broker the cluster advertises answered a metadata request, so their \
                     list could not even be read"
                .to_string(),
        };
        return format!(
            "the bootstrap address answered and named the cluster, and {how_many} (pod exit \
             {exit_code}), so no backup or restore through this connection can reach every \
             partition. Every broker must advertise a host and port the runner pods in this \
             namespace can reach: check advertised.listeners for the listener the bootstrap \
             address belongs to, or use a bootstrap address of a listener meant for this \
             network. The probe Job's log names each broker that did not answer"
        );
    }
    match v.reachable {
        Some(true) => match v.brokers_answered {
            Some((answered, Some(listed))) => format!(
                "the probe reached a broker, read cluster id {}, and {answered} of the {listed} \
                 broker(s) the cluster advertises answered on their advertised addresses (pod \
                 exit {exit_code})",
                v.cluster_id.as_deref().unwrap_or("<unnamed>")
            ),
            _ => format!(
                "the probe reached a broker and read cluster id {} (pod exit {exit_code})",
                v.cluster_id.as_deref().unwrap_or("<unnamed>")
            ),
        },
        Some(false) => format!(
            "the probe ran and printed `{REACHABLE_PREFIX}false` (pod exit {exit_code}); the \
             broker did not answer, or it refused the credential this cluster names"
        ),
        None => format!(
            "the pod log carried no parseable `{CLUSTER_ID_PREFIX}` / `{REACHABLE_PREFIX}` line \
             in its last {} non-empty lines (pod exit {exit_code}); `reachable` is cleared \
             rather than guessed from the exit code, which Global Constraint 11 uses for every \
             operational failure alike",
            backup::KEY_SCAN_TAIL_LINES
        ),
    }
}

/// The probe's argv (interface **I14**'s flag list), built from the resolved
/// connection plus this object's marker topic.
///
/// The connection flags are [`ResolvedConnection::probe_args`], the resolution
/// the backup and restore Jobs are built from, so a probe never dials settings
/// those runs do not use. `--marker-topic` is passed whenever the field is set,
/// and the probe never asserts it. `--tls` is emitted only when the transport is
/// TLS: a boolean flag has no false form.
#[must_use]
pub fn runner_argv(cluster: &KafkaCluster, connection: &ResolvedConnection) -> Vec<String> {
    let mut argv = vec!["cluster-probe".to_string()];
    argv.extend(connection.probe_args());
    if let Some(t) = cluster.spec.marker_topic.as_ref() {
        argv.push("--marker-topic".to_string());
        argv.push(t.clone());
    }
    argv
}

/// `--auth-mode`'s value for a CRD auth mode.
///
/// The spellings are byte-identical to the CRD enum and to
/// `logweir_core::spec::AuthSpec` (interface I33), so this renames a value and
/// never translates one. No wildcard arm: a new mode fails to compile here
/// rather than reaching an argv as `plaintext`.
#[must_use]
pub fn auth_mode_flag(mode: AuthMode) -> &'static str {
    match mode {
        AuthMode::Plaintext => "plaintext",
        AuthMode::ScramSha512 => "scramSha512",
        AuthMode::ScramSha256 => "scramSha256",
        AuthMode::Plain => "plain",
        AuthMode::Mtls => "mtls",
    }
}

/// The probe Job for a cluster.
///
/// # Errors
///
/// [`KafkaClusterError::NoNamespace`] / [`KafkaClusterError::NoUid`], and
/// [`KafkaClusterError::Refused`] carrying the connection's own refusal when
/// [`connection::resolve`] refuses it.
pub fn runner_job_spec(cluster: &KafkaCluster) -> Result<RunnerJobSpec, KafkaClusterError> {
    let name = cluster.name_any();
    let namespace = cluster
        .namespace()
        .ok_or_else(|| KafkaClusterError::NoNamespace(name.clone()))?;
    let uid = cluster
        .uid()
        .ok_or_else(|| KafkaClusterError::NoUid(name.clone()))?;

    // The password and the CA are projected and never read: the controller
    // holds no `get` on Secrets (spec §9). A cluster whose auth needs a
    // `secretRef` it lacks is refused by the resolver before any Job exists.
    let connection = connection::resolve(cluster, ConnectionUse::Probe)?;
    connection.check_job_namespace(&namespace)?;
    let projection = connection.project();

    Ok(RunnerJobSpec {
        name: probe_job_name(&name),
        namespace,
        owner: RunnerOwner {
            api_version: KafkaCluster::api_version(&()).to_string(),
            kind: KafkaCluster::kind(&()).to_string(),
            name,
            uid,
        },
        args: runner_argv(cluster, &connection),
        deadline_seconds: PROBE_DEADLINE_SECONDS,
        service_account_name: connection.execution.service_account_name.clone(),
        // No signing key: a probe signs nothing, reads no approval and writes
        // no artifact, so its only volume besides `/work` is the connection's
        // CA.
        secret_mounts: projection.secret_mounts,
        config_map_mounts: projection.config_map_mounts,
        env_from_secret: projection.env_from_secret,
        env_literal: {
            let mut env = vec![("RUST_LOG".to_string(), "info".to_string())];
            env.extend(projection.env_literal);
            env
        },
        // No plan: `logweir cluster-probe` reads no `--spec`, and a Job with an
        // empty `/plan` mount would stall in `ContainerCreating` until its
        // deadline fired.
        plan_config_map: None,
        // The shipped pin; the reconciler overwrites it when this process was
        // handed another image (`job::RUNNER_IMAGE_ENV`, read once in `main`),
        // so this stays a pure function of the custom resource.
        image: None,
        // Likewise `job::IMAGE_PULL_POLICY` (`job::RUNNER_PULL_POLICY_ENV`).
        image_pull_policy: None,
        // `KafkaCluster` states no resources for its probe Job.
        resources: None,
    })
}

/// When the probe this pass is reading actually finished.
///
/// Not `now`: with `controller().owns(jobs)`, a status patch carrying a fresh
/// `now` bumps the `resourceVersion`, the watch fires, and the reconcile
/// re-reads the same finished Job and writes another `now`, a hot loop. An
/// instant taken from the probe makes every re-read of the same Job produce a
/// byte-identical patch, which the API server treats as a no-op, and it is the
/// truer value: the CRD field says when the probe was performed.
/// `the_observed_status_patch_is_stable_across_passes` pins it.
///
/// Four sources, closest to the probe first: the runner container's
/// `finishedAt`, the Job's `completionTime`, the Job's terminal condition's
/// `lastTransitionTime`, and, only when none is offered, `now`.
#[must_use]
pub fn observed_at(job: &Job, pod: Option<&Pod>, now: DateTime<Utc>) -> DateTime<Utc> {
    pod.and_then(|p| p.status.as_ref())
        .and_then(|s| s.container_statuses.as_ref())
        .and_then(|cs| cs.iter().find(|c| c.name == crate::job::CONTAINER_NAME))
        .and_then(|c| c.state.as_ref())
        .and_then(|s| s.terminated.as_ref())
        .and_then(|t| t.finished_at.as_ref())
        .map(|t| t.0)
        .or_else(|| {
            job.status
                .as_ref()
                .and_then(|s| s.completion_time.as_ref())
                .map(|t| t.0)
        })
        .or_else(|| {
            job.status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .and_then(|cs| {
                    cs.iter()
                        .filter(|c| {
                            c.status == "True" && (c.type_ == "Complete" || c.type_ == "Failed")
                        })
                        .filter_map(|c| c.last_transition_time.as_ref())
                        .map(|t| t.0)
                        .next()
                })
        })
        .unwrap_or(now)
}

/// One condition, as a merge-patch fragment. Exactly one is ever written: a
/// condition array is a map keyed by `type`. `lastTransitionTime` moves only on
/// a real transition, decided by [`crate::conditions::merge_condition`].
fn condition(
    cluster: &KafkaCluster,
    status: &str,
    reason: &str,
    message: &str,
    now: DateTime<Utc>,
) -> Value {
    json!(merge_condition(
        current_condition(
            cluster.status.as_ref().and_then(|s| s.conditions.as_ref()),
            CONDITION_REACHABLE,
        ),
        crate::crds::Condition {
            r#type: CONDITION_REACHABLE.to_string(),
            status: status.to_string(),
            observed_generation: cluster.meta().generation,
            last_transition_time: Some(now),
            reason: Some(reason.to_string()),
            message: Some(message.to_string()),
        },
    ))
}

/// Patch `/status` under seam **S7**'s `metadata.resourceVersion`
/// precondition, and not at all when the patch would change nothing (both
/// [`crate::conditions::patch_status_preconditioned`]'s decisions), so a
/// verdict computed from a stale watch-cache copy cannot overwrite a newer
/// write. No caller writes twice in a pass, so there is no `patch_status_at`.
///
/// A `409` is an answer here, not an error: it becomes
/// [`StatusWrite::Superseded`], a debug line, because the watch cache handed
/// this pass an older copy than the API server holds. Every caller returns at
/// once on it, so a verdict that did not land is never followed by its TTL; the
/// newer copy's watch event reconciles again.
async fn patch_status_if_changed(
    api: &Api<KafkaCluster>,
    cluster: &KafkaCluster,
    name: &str,
    patch: Value,
) -> Result<StatusWrite, KafkaClusterError> {
    let observed = StatusVersion::observed(cluster.meta());
    match crate::conditions::patch_status_preconditioned(
        api,
        "KafkaCluster",
        name,
        &observed,
        cluster
            .status
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok())
            .as_ref(),
        patch,
    )
    .await
    {
        // A skipped write returns the version it was handed; a landed one,
        // the version the API server moved the object to.
        Ok(after) if after == observed => Ok(StatusWrite::Unchanged),
        Ok(_) => Ok(StatusWrite::Changed),
        Err(kube::Error::Api(e)) if e.code == 409 => {
            debug!(
                cluster = %name,
                observed_resource_version = observed.get().unwrap_or_default(),
                "the status write lost its resourceVersion precondition to a newer copy of this \
                 KafkaCluster; nothing was written, and the newer copy's watch event reconciles \
                 again"
            );
            Ok(StatusWrite::Superseded)
        }
        Err(e) => Err(KafkaClusterError::Api(e)),
    }
}

/// What one `/status` write did — [`patch_status_if_changed`]'s answer.
#[must_use = "a superseded status write must end the pass before any TTL is set"]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StatusWrite {
    /// The patch landed and moved the object: the first record of what it
    /// says, which is when a crash or a refusal is logged at WARN.
    Changed,
    /// The computed status equals the stored one, and nothing was sent.
    Unchanged,
    /// `409`: the object moved on since this pass read it. Nothing was
    /// written ([`Deferred::StatusSuperseded`]).
    Superseded,
}

/// The annotation this reconciler puts on a probe Job, in the same merge patch
/// as its TTL, once the status write that recorded the Job's verdict returned.
/// Its value is the Job's own `metadata.uid`.
pub const VERDICT_RECORDED_ANNOTATION: &str = "logweir.dev/probe-verdict-recorded";

/// Whether a finished probe Job's verdict is already on the status.
///
/// [`set_probe_ttl`] writes [`VERDICT_RECORDED_ANNOTATION`] = the Job's UID
/// beside the TTL, in one merge patch, only after the status write that
/// recorded the verdict (a reading, a crash or a refused pod) returned. A
/// finished Job that carries it has been judged; a pod it no longer has was
/// collected after the reading.
///
/// The marker is never `ttlSecondsAfterFinished` itself: a mutating admission
/// policy can give every new Job a TTL at creation, and then every probe Job
/// would read as judged from birth and a real crash would never be recorded.
/// Nothing but this reconciler writes the annotation, and the UID makes a value
/// copied onto another Job worthless.
#[must_use]
pub fn verdict_recorded(job: &Job) -> bool {
    let Some(uid) = job.metadata.uid.as_deref().filter(|u| !u.is_empty()) else {
        return false;
    };
    job.metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(VERDICT_RECORDED_ANNOTATION))
        .is_some_and(|v| v == uid)
}

/// Whether Kubernetes is deleting this probe Job.
///
/// The TTL controller deletes a finished Job with foreground propagation, so
/// the Job is still readable, pod-less, with a `deletionTimestamp`, for as long
/// as its pod takes to go. Nothing about the cluster is read from it.
#[must_use]
pub fn being_deleted(job: &Job) -> bool {
    job.metadata.deletion_timestamp.is_some()
}

/// How old the last probe reading (`status.observedAt`) may grow, with no newer
/// probe answering, before `reachable` stops vouching for it.
///
/// Twice [`RE_PROBE_SECS`] (630 s), the console's own freshness budget. A
/// healthy reading is replaced within [`RE_PROBE_SECS`] plus at most
/// [`PROBE_DEADLINE_SECONDS`], so it never reaches this. A probe Job whose
/// deletion stalls (a pod `Terminating` on a lost node, a foreign finalizer)
/// does: the Job's name is fixed, so no newer probe runs while it exists. A
/// `Restore` and a rehearsal admit a target on `reachable == true` alone, so
/// this bound keeps them from admitting on a reading nobody has repeated.
pub const STALE_AFTER_SECS: i64 = 2 * RE_PROBE_SECS as i64;

/// `Reachable=Unknown`: the last reading is older than [`STALE_AFTER_SECS`]
/// and no newer probe has answered, so `reachable` is cleared.
pub const REASON_PROBE_STALE: &str = "ProbeStale";

/// Whether `reachable` asserts a reading older than [`STALE_AFTER_SECS`] at
/// `now`. Only a set `reachable` can be stale; one with no `observedAt` beside
/// it names no instant to vouch for, and is stale too.
#[must_use]
pub fn reachable_is_stale(cluster: &KafkaCluster, now: DateTime<Utc>) -> bool {
    let Some(status) = cluster.status.as_ref() else {
        return false;
    };
    if status.reachable.is_none() {
        return false;
    }
    status
        .observed_at
        .as_ref()
        .is_none_or(|at| reading_is_stale(*at, now))
}

/// Whether a reading taken at `observed` is older than [`STALE_AFTER_SECS`].
#[must_use]
pub fn reading_is_stale(observed: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    (now - observed).num_seconds() > STALE_AFTER_SECS
}

/// `default`, or sooner: the pass that would find the reading taken at
/// `observed` stale, so a deferral or a re-read never sleeps past the bound.
fn requeue_before_stale(
    default: u64,
    observed: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Requeue {
    let until_stale = observed.map(|at| {
        let left = STALE_AFTER_SECS + 1 - (now - at).num_seconds();
        u64::try_from(left.max(1)).unwrap_or(1)
    });
    Requeue::After(until_stale.map_or(default, |u| u.min(default)))
}

/// The `/status` merge patch for a reading too old to vouch for.
///
/// `reachable` cleared, `ProbeStale` in the scalar and the condition, and
/// `observedAt` and `clusterId` left alone as the record of the last real look.
/// `detail` and the stored `observedAt` are the whole message, so a later pass
/// over the same state computes the same bytes and writes nothing.
#[must_use]
pub fn stale_status_patch(cluster: &KafkaCluster, detail: &str, now: DateTime<Utc>) -> Value {
    let last = cluster
        .status
        .as_ref()
        .and_then(|s| s.observed_at.as_ref())
        .map_or_else(
            || "no recorded instant".to_string(),
            |t| t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        );
    json!({
        "status": {
            "reachable": null,
            "reason": REASON_PROBE_STALE,
            "conditions": [condition(
                cluster,
                "Unknown",
                REASON_PROBE_STALE,
                &format!(
                    "the last probe reading ({last}) is more than {STALE_AFTER_SECS} s old and \
                     no newer probe has answered: {detail}; `reachable` is cleared rather than \
                     left vouching for it, and the next probe that answers sets it again"
                ),
                now,
            )],
        }
    })
}

/// The `/status` merge patch for the pass that created a probe Job.
///
/// It omits `reachable`, `clusterId` and `observedAt`, so a re-probe in flight
/// keeps the last observation, and writes that somebody is looking: the
/// difference between "not probed yet" and "never seen".
#[must_use]
pub fn probe_started_patch(cluster: &KafkaCluster, job_name: &str, now: DateTime<Utc>) -> Value {
    // A probe in flight does not re-vouch for a stale reading: past
    // `STALE_AFTER_SECS` the last observation is cleared here too.
    let stale = reachable_is_stale(cluster, now);
    let message = if stale {
        format!(
            "probe Job {job_name} is running; the last observation is more than              {STALE_AFTER_SECS} s old, so `reachable` is cleared until this probe answers"
        )
    } else {
        format!("probe Job {job_name} is running; the last observation, if any, stands")
    };
    let mut patch = json!({
        "status": {
            "reason": REASON_PROBE_RUNNING,
            "conditions": [condition(
                cluster,
                "Unknown",
                REASON_PROBE_RUNNING,
                &message,
                now,
            )],
        }
    });
    if stale {
        patch["status"]["reachable"] = Value::Null;
    }
    // Publish the binding the credential Secret must carry, so an operator
    // creating it by hand can copy it. Public (a UID and an endpoint digest);
    // written only for a connection that has a credential and resolves.
    if let Some(binding) = credential_binding_of(cluster) {
        patch["status"]["credentialBinding"] = json!(binding);
    }
    patch
}

/// The binding the cluster's credential Secret must carry, when it has a
/// credential and resolves — [`ResolvedConnection::credential_binding`].
#[must_use]
pub fn credential_binding_of(cluster: &KafkaCluster) -> Option<String> {
    let resolved = connection::resolve(cluster, ConnectionUse::Probe).ok()?;
    resolved.credential_secret()?;
    resolved.credential_binding()
}

/// The `/status` merge patch for a probe whose log has been read.
///
/// `reachable` is written on every verdict: `true` or `false` from the log's
/// own line, and `null` (removed under a merge patch) when the log carried
/// neither, because a `Restore` and a rehearsal admit a target on
/// `reachable == true` alone. `clusterId` is written only beside
/// `reachable: true` and otherwise kept as the identity last observed.
/// `observedAt` is written on every verdict: "we looked and could not tell" is
/// an observation with a time.
#[must_use]
pub fn observed_status_patch(
    cluster: &KafkaCluster,
    v: &Verdict,
    exit_code: i32,
    observed_at: DateTime<Utc>,
) -> Value {
    let mut status = serde_json::Map::new();
    // `null` FOR AN UNREADABLE PROBE: clear what this verdict cannot vouch for.
    status.insert("reachable".to_string(), json!(v.reachable));
    if let Some(id) = v.cluster_id.as_ref() {
        status.insert("clusterId".to_string(), json!(id));
    }
    // The probe's own instant, not the controller's: see `observed_at`.
    status.insert("observedAt".to_string(), json!(observed_at));
    // The scalar the condition's reason is promoted to: `reachable` alone
    // cannot tell `ProbeOutputUnreadable` from `NoExitCode` from a probe in
    // flight.
    status.insert("reason".to_string(), json!(v.reason));
    status.insert(
        "conditions".to_string(),
        json!([condition(
            cluster,
            v.status,
            &v.reason,
            &verdict_message(v, exit_code),
            observed_at,
        )]),
    );
    json!({ "status": Value::Object(status) })
}

/// The `/status` merge patch for a probe Job that finished with no exit code
/// at all: the crashed-Job case.
///
/// The sub-case is [`backup::crash_terminal_state`]'s and the condition is
/// `Unknown`: a pod that never ran wrote no log. `reachable` is cleared, so a
/// `Restore` cannot admit a target on a stale `true`; `clusterId` stays as the
/// identity last observed, and `observedAt` is the crashed attempt's instant.
#[must_use]
pub fn crashed_status_patch(
    cluster: &KafkaCluster,
    terminal_state: &str,
    job_name: &str,
    observed_at: DateTime<Utc>,
) -> Value {
    json!({
        "status": {
            "reachable": null,
            "observedAt": observed_at,
            "reason": terminal_state,
            "conditions": [condition(
                cluster,
                "Unknown",
                terminal_state,
                &format!(
                    "probe Job {job_name} finished with no terminated state for the `{}` \
                     container, so no exit code and no log could be read; `reachable` is \
                     cleared rather than invented, and the next probe runs on the ordinary \
                     cadence",
                    crate::job::CONTAINER_NAME
                ),
                observed_at,
            )],
        }
    })
}

/// The `/status` merge patch for a probe Job whose pod the namespace refused at
/// creation.
///
/// `PodCreationForbidden` and the admission's own words (`refusal` is
/// [`check::waiting::pod_create_refusal`]'s, already redacted), with the
/// condition `Unknown`. `reachable` is cleared and `clusterId` kept, as in
/// [`crashed_status_patch`]; the operator reads the quota, not "no exit code".
///
/// No `observedAt`: a refused probe was never performed, and the refusal loop
/// re-probes every [`RE_PROBE_SECS`], so writing its instant would keep the
/// last real observation looking fresh for as long as the quota stands.
/// `seen_at` is the condition's `lastTransitionTime` and nothing else.
#[must_use]
pub fn pod_refused_status_patch(
    cluster: &KafkaCluster,
    job_name: &str,
    refusal: &check::Waiting,
    seen_at: DateTime<Utc>,
) -> Value {
    let reason = crate::conditions::TERMINAL_STATE_POD_CREATION_FORBIDDEN;
    json!({
        "status": {
            "reachable": null,
            "reason": reason,
            "conditions": [condition(
                cluster,
                "Unknown",
                reason,
                &format!(
                    "probe Job {job_name} never ran, so `reachable` is cleared rather than \
                     invented: {} ({})",
                    refusal.message,
                    refusal.code.as_str()
                ),
                seen_at,
            )],
        }
    })
}

/// The `/status` merge patch for a refusal this controller made itself, before
/// any `POST`. `reachable` is cleared, as in
/// [`connection_refused_status_patch`].
#[must_use]
pub fn refused_status_patch(
    cluster: &KafkaCluster,
    reason: &str,
    message: &str,
    now: DateTime<Utc>,
) -> Value {
    json!({
        "status": {
            "reachable": null,
            "reason": reason,
            "conditions": [condition(cluster, "Unknown", reason, message, now)],
        }
    })
}

/// The `/status` merge patch for a connection this controller refused to
/// resolve, before any Job exists.
///
/// `reachable` is cleared and nothing else about the last probe is: a `Restore`
/// admits a target only on `reachable == true`, and a stale `true` from a probe
/// run with different settings is an observation this controller does not
/// stand behind. `clusterId` and `observedAt` stay as the record of that look.
#[must_use]
pub fn connection_refused_status_patch(
    cluster: &KafkaCluster,
    reason: &str,
    message: &str,
    now: DateTime<Utc>,
) -> Value {
    json!({
        "status": {
            "reachable": null,
            "reason": reason,
            "conditions": [condition(cluster, "Unknown", reason, message, now)],
        }
    })
}

/// Whether this object already carries a terminal refusal. Only
/// [`TERMINAL_STATE_NAME_TOO_LONG`] is terminal here, because `metadata.name`
/// is the one input to a probe that cannot change.
#[must_use]
pub fn status_is_terminal(cluster: &KafkaCluster) -> bool {
    cluster
        .status
        .as_ref()
        .and_then(|s| s.reason.as_deref())
        .is_some_and(|r| r == TERMINAL_STATE_NAME_TOO_LONG)
}

/// When to look at a cluster again.
///
/// A local enum rather than `super::restore::Requeue`, which has the same
/// shape: the two modules share no object. `Action` cannot be compared, so the
/// interval is a value a test asserts, and [`action_for`] is the one place it
/// becomes an `Action`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Requeue {
    /// Nothing will change until something edits this object: `NameTooLong` is
    /// the only state that reaches it, because `metadata.name` is immutable.
    AwaitChange,
    /// Look again after this many seconds.
    After(u64),
}

/// Why a pass formed no verdict on purpose.
///
/// Each is a state Kubernetes passes through while it collects a probe Job, or
/// a race with a newer copy of the object, and none says anything about the
/// cluster: the last recorded verdict stands, nothing is logged above debug,
/// and the reconcile is an outcome, not an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Deferred {
    /// The probe Job carries `metadata.deletionTimestamp`: its TTL (or anyone
    /// else) is deleting it, and under foreground propagation its pod goes
    /// first. Nothing is read from it; the next probe is created once it is
    /// gone.
    JobBeingDeleted,
    /// The probe Job finished, has no terminated `runner` to read, and carries
    /// [`VERDICT_RECORDED_ANNOTATION`] ([`verdict_recorded`]): its pod was
    /// collected after the reading, so it is not re-judged as a crash.
    VerdictRecorded,
    /// The probe pod was listed but was gone by the `pods/log` read (`404`).
    /// The next pass sees the Job as it now stands.
    PodGone,
    /// The `/status` write lost its `resourceVersion` precondition (`409`):
    /// the watch cache handed this pass an older copy of the object than the
    /// API server holds. The newer copy's own watch event reconciles again,
    /// and no TTL is set after a verdict write that did not land.
    StatusSuperseded,
}

/// What one reconcile did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProbeOutcome {
    /// The probe Job's name — `logweir-probe-<cr name>`.
    pub job_name: String,
    /// Whether this pass created it.
    pub created: bool,
    /// The value written to `status.reachable`, when one was written.
    pub reachable: Option<bool>,
    /// The value written to `status.clusterId`, when one was written.
    pub cluster_id: Option<String>,
    /// The scalar `status.reason` this pass wrote, when it wrote a status.
    pub reason: Option<String>,
    /// Whether the Job was patched with `ttlSecondsAfterFinished`.
    pub ttl_patched: bool,
    pub requeue: Requeue,
    /// Why this pass formed no verdict on purpose, when it did not.
    pub deferred: Option<Deferred>,
}

impl ProbeOutcome {
    /// A pass that formed no verdict on purpose: nothing written from it.
    ///
    /// The requeue is a backstop — the event that ends each state (the Job's
    /// deletion, the newer object's watch event) usually arrives first. A
    /// recorded verdict waits for its TTL like any read probe; the other three
    /// look again on the short clock.
    fn deferred(job_name: String, created: bool, why: Deferred) -> Self {
        Self {
            job_name,
            created,
            reachable: None,
            cluster_id: None,
            reason: None,
            ttl_patched: false,
            requeue: match why {
                Deferred::VerdictRecorded => Requeue::After(RE_PROBE_SECS),
                Deferred::JobBeingDeleted | Deferred::PodGone | Deferred::StatusSuperseded => {
                    Requeue::After(REQUEUE_SECS)
                }
            },
            deferred: Some(why),
        }
    }
}

/// The `kube::runtime::Action` one outcome asks for — the ONE mapping, so the
/// interval a test asserts and the interval the runtime gets cannot drift.
#[must_use]
pub fn action_for(outcome: &ProbeOutcome) -> Action {
    match outcome.requeue {
        Requeue::AwaitChange => Action::await_change(),
        Requeue::After(secs) => Action::requeue(std::time::Duration::from_secs(secs)),
    }
}

/// Anything that is not an outcome. Requeues; writes nothing.
#[derive(Debug)]
pub enum KafkaClusterError {
    /// No `metadata.namespace`. Unreachable from the API server; named rather
    /// than unwrapped.
    NoNamespace(String),
    /// No `metadata.uid`, so no owner reference can be built.
    NoUid(String),
    /// The API server could not be talked to. **Requeue** — a transport error
    /// says nothing about the cluster, and writing a status from one would
    /// publish a guess.
    Api(kube::Error),
    /// A TERMINAL refusal this controller decided by itself, carrying the
    /// terminal state and the message its condition names. Raised with `?` so
    /// the status write cannot be forgotten at one of the refusal sites.
    Refused(&'static str, String),
}

impl fmt::Display for KafkaClusterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoNamespace(n) => write!(f, "KafkaCluster {n} has no metadata.namespace"),
            Self::NoUid(n) => write!(f, "KafkaCluster {n} has no metadata.uid"),
            Self::Api(e) => write!(f, "the Kubernetes API returned an error: {e}"),
            Self::Refused(state, message) => write!(f, "refused ({state}): {message}"),
        }
    }
}

impl std::error::Error for KafkaClusterError {}

impl From<kube::Error> for KafkaClusterError {
    fn from(e: kube::Error) -> Self {
        Self::Api(e)
    }
}

/// Patch a finished probe Job's `ttlSecondsAfterFinished`, which is also the
/// re-probe timer ([`PROBE_TTL_SECONDS`]), together with
/// [`VERDICT_RECORDED_ANNOTATION`] = the Job's UID, in one patch: the Job reads
/// "verdict recorded" exactly when this reconciler gave it its TTL, and a TTL
/// an admission policy set at creation is overwritten rather than trusted.
///
/// Every caller sends this only after its status patch returned (a status that
/// did not land leaves through `?` or [`StatusWrite::Superseded`]), so pod
/// garbage collection never races the read the status records. Returns whether
/// the patch was applied; a Job already gone (`404`) is the state the TTL
/// exists to reach, not an error.
async fn set_probe_ttl(jobs: &Api<Job>, job: &Job) -> Result<bool, KafkaClusterError> {
    let job_name = job.name_any();
    let uid = job.metadata.uid.clone().unwrap_or_default();
    let marker = Some((VERDICT_RECORDED_ANNOTATION, uid.as_str()));
    match runner_log::set_ttl(jobs, &job_name, PROBE_TTL_SECONDS, marker).await {
        Ok(()) => Ok(true),
        Err(kube::Error::Api(e)) if e.code == 404 => {
            debug!(
                job = %job_name,
                "the probe Job was already collected before its TTL patch; nothing is left to \
                 collect, and its deletion starts the next probe"
            );
            Ok(false)
        }
        Err(e) => Err(KafkaClusterError::Api(e)),
    }
}

/// The probe pod for a Job (seam **S6**).
///
/// Found by the two labels in [`backup::pod_selectors`]'s order, then by the
/// selection rule the execution controllers and the check framework share
/// ([`check::pod::find_owned_pod_by_selectors`]): a candidate is read only when
/// its controller `ownerReference` is a `Job` carrying this probe Job's
/// `metadata.uid`. The probe's lines become `status.reachable`, which `Restore`
/// admission reads, so a pod planted with the Job's label must not be able to
/// assert that an unreachable cluster is reachable. Zero owned pods is "no pod
/// yet", the crashed-probe branch.
async fn find_pod(
    client: &kube::Client,
    namespace: &str,
    job_name: &str,
    job_uid: Option<&str>,
) -> Result<check::pod::FoundPod, KafkaClusterError> {
    check::pod::find_owned_pod_by_selectors(
        client,
        namespace,
        job_name,
        job_uid,
        &backup::pod_selectors(job_name),
    )
    .await
    .map_err(KafkaClusterError::Api)
}

/// Reconcile one `KafkaCluster` at the instant `now`.
///
/// # The state machine, one pass per event
///
/// 0. **The object's own name is longer than [`name_limit_for_cluster`]** → a
///    terminal status (`Reachable=Unknown`, `NameTooLong`) and nothing is
///    created: the API server would refuse the Job, and a requeue on a refusal
///    that can never succeed leaves the object with no status at all.
/// 1. **No Job** → `POST` the probe Job; status says a probe is running, and
///    the last observation stands.
/// 2. **Job exists, not finished** → the same running status. A Job with no
///    pod after the grace and a `FailedCreate` Event naming it is cancelled,
///    and `PodCreationForbidden` is written.
/// 3. **Job finished, `runner` terminated** → read the two contract lines from
///    the `pods/log` subresource, patch `/status` with `reachable`, `clusterId`,
///    `observedAt`, the scalar `reason` and one condition, and only after that
///    patch returns, `PATCH` the Job with its TTL, the re-probe timer.
/// 4. **Job finished, no terminated state for `runner`** → the crashed-Job
///    case, classified by [`backup::crash_terminal_state`], with `reachable`
///    cleared and the usual TTL, or, when the Job never had a pod and its
///    `FailedCreate` Event says why, `PodCreationForbidden`.
///
/// `now` is an argument: the one clock read is in the `kube::runtime` wrapper.
///
/// # Errors
///
/// [`KafkaClusterError`] for anything that is not an outcome.
pub async fn reconcile_cluster(
    cluster: &KafkaCluster,
    client: &kube::Client,
    now: DateTime<Utc>,
) -> Result<ProbeOutcome, KafkaClusterError> {
    reconcile_cluster_with_runner_image(cluster, client, now, &job::RunnerImage::default()).await
}

/// [`reconcile_cluster`], with the runner image and pull policy this controller
/// process was handed.
///
/// The probe runs the same image as the backup and restore runners (one
/// binary), so both overrides reach it too; an unset field is the compiled-in
/// constant (`job::RUNNER_IMAGE`, `job::IMAGE_PULL_POLICY`).
///
/// # Errors
///
/// [`KafkaClusterError`] for anything that is not an outcome.
pub async fn reconcile_cluster_with_runner_image(
    cluster: &KafkaCluster,
    client: &kube::Client,
    now: DateTime<Utc>,
    runner: &job::RunnerImage,
) -> Result<ProbeOutcome, KafkaClusterError> {
    match reconcile_cluster_inner(cluster, client, now, runner).await {
        Err(KafkaClusterError::Refused(state, message)) => {
            // The one place a self-decided refusal is written, so it cannot be
            // forgotten at one of the refusal sites.
            let name = cluster.name_any();
            let namespace = cluster
                .namespace()
                .ok_or_else(|| KafkaClusterError::NoNamespace(name.clone()))?;
            warn!(
                cluster = %name,
                namespace = %namespace,
                terminal_state = state,
                reason = %message,
                "refusing to probe this KafkaCluster terminally: nothing was created, and a \
                 requeue over an immutable metadata.name would never succeed"
            );
            if !status_is_terminal(cluster) {
                let clusters: Api<KafkaCluster> = Api::namespaced(client.clone(), &namespace);
                if patch_status_if_changed(
                    &clusters,
                    cluster,
                    &name,
                    refused_status_patch(cluster, state, &message, now),
                )
                .await?
                    == StatusWrite::Superseded
                {
                    return Ok(ProbeOutcome::deferred(
                        probe_job_name(&name),
                        false,
                        Deferred::StatusSuperseded,
                    ));
                }
            }
            Ok(ProbeOutcome {
                job_name: probe_job_name(&name),
                created: false,
                reachable: None,
                cluster_id: None,
                reason: Some(state.to_string()),
                ttl_patched: false,
                requeue: Requeue::AwaitChange,
                deferred: None,
            })
        }
        other => other,
    }
}

/// [`reconcile_cluster`]'s body. See that function for the state machine.
async fn reconcile_cluster_inner(
    cluster: &KafkaCluster,
    client: &kube::Client,
    now: DateTime<Utc>,
    runner: &job::RunnerImage,
) -> Result<ProbeOutcome, KafkaClusterError> {
    let name = cluster.name_any();
    let namespace = cluster
        .namespace()
        .ok_or_else(|| KafkaClusterError::NoNamespace(name.clone()))?;
    let job_name = probe_job_name(&name);
    let jobs: Api<Job> = Api::namespaced(client.clone(), &namespace);
    let clusters: Api<KafkaCluster> = Api::namespaced(client.clone(), &namespace);

    // Step 0: the object's own name, before any `POST`.
    if name.len() > name_limit_for_cluster() {
        return Err(KafkaClusterError::Refused(
            TERMINAL_STATE_NAME_TOO_LONG,
            format!(
                "the object name is {} characters and its probe Job would be named \
                 `{PROBE_JOB_PREFIX}` + that name, which the pod label `{}` may carry at most {} \
                 characters of; shorten the KafkaCluster's name by {} characters",
                name.len(),
                backup::JOB_NAME_LABEL,
                crate::slot::NAME_LIMIT,
                name.len().saturating_sub(name_limit_for_cluster())
            ),
        ));
    }

    // Step 0b: the connection, resolved before any `GET` or `POST`. Not
    // terminal: it is re-evaluated on every reconcile, so a controller that
    // understands the object clears the refusal with no edit. A refusal creates
    // no Job, but it does collect one it did not create.
    if let Err(refusal) = connection::resolve(cluster, ConnectionUse::Probe) {
        warn!(
            cluster = %name,
            namespace = %namespace,
            reason = refusal.reason,
            field = %refusal.field,
            "refusing to probe this KafkaCluster: its saved connection does not resolve, so no \
             probe Job is created"
        );
        if patch_status_if_changed(
            &clusters,
            cluster,
            &name,
            connection_refused_status_patch(cluster, refusal.reason, &refusal.message, now),
        )
        .await?
            == StatusWrite::Superseded
        {
            return Ok(ProbeOutcome::deferred(
                job_name,
                false,
                Deferred::StatusSuperseded,
            ));
        }
        // A refusal does not make an existing probe Job disappear: one left by
        // an earlier build would otherwise never get a TTL, and `spec` is
        // immutable, so nothing would look again. The Job is adopted for
        // collection only: a finished one gets the step-3 TTL, an in-flight one
        // holds the reconciler on the requeue clock. Its log is never read and
        // `reachable` never written from it: it dialled settings this build
        // refuses, so its verdict is not one the controller stands behind.
        let existing = jobs
            .get_opt(&job_name)
            .await
            .map_err(KafkaClusterError::Api)?;
        let (ttl_patched, requeue) = match existing {
            None => (false, Requeue::AwaitChange),
            // Already being collected; its deletion is a watch event.
            Some(job) if being_deleted(&job) => (false, Requeue::AwaitChange),
            // Already given its TTL by an earlier pass: the same.
            Some(job) if backup::job_finished(&job) && verdict_recorded(&job) => {
                (false, Requeue::AwaitChange)
            }
            Some(job) if backup::job_finished(&job) => {
                let patched = set_probe_ttl(&jobs, &job).await?;
                info!(
                    cluster = %name,
                    namespace = %namespace,
                    job = %job_name,
                    "the refused connection left a finished probe Job behind; it is given the \
                     usual TTL so it is collected, and its log is not read"
                );
                (patched, Requeue::AwaitChange)
            }
            Some(_) => (false, Requeue::After(REQUEUE_SECS)),
        };
        return Ok(ProbeOutcome {
            job_name,
            created: false,
            reachable: None,
            cluster_id: None,
            reason: Some(refusal.reason.to_string()),
            ttl_patched,
            requeue,
            deferred: None,
        });
    }

    let existing = jobs
        .get_opt(&job_name)
        .await
        .map_err(KafkaClusterError::Api)?;

    // STEP 1. No Job: probe.
    let Some(job) = existing else {
        if status_is_terminal(cluster) {
            debug!(
                cluster = %name,
                namespace = %namespace,
                "this KafkaCluster carries a terminal refusal; no probe is created"
            );
            return Ok(ProbeOutcome {
                job_name,
                created: false,
                reachable: None,
                cluster_id: None,
                reason: None,
                ttl_patched: false,
                requeue: Requeue::AwaitChange,
                deferred: None,
            });
        }
        let mut spec = runner_job_spec(cluster)?;
        // The image and pull-policy overrides; `None` leaves the compiled-in
        // constant.
        spec.image = runner.image.clone();
        spec.image_pull_policy = runner.image_pull_policy.clone();
        let mut probe_job = job::build(&spec);
        // FX-43: the probe seals its two contract lines and its end line with
        // a fresh per-Job token, so a line the bootstrap list or a broker
        // got into the log is never read as the probe's.
        if !job::add_fresh_line_token(&mut probe_job) {
            warn!(
                cluster = %name,
                namespace = %namespace,
                job = %job_name,
                "the random source gave no line token; the probe Job is created without one \
                 and its log is read as a Job an older controller built"
            );
        }
        jobs.create(&PostParams::default(), &probe_job)
            .await
            .map_err(KafkaClusterError::Api)?;
        info!(
            cluster = %name,
            namespace = %namespace,
            job = %job_name,
            "created the probe Job; this controller never dials a broker itself and never reads \
             a Secret, which is why a probe is a Job"
        );
        if patch_status_if_changed(
            &clusters,
            cluster,
            &name,
            probe_started_patch(cluster, &job_name, now),
        )
        .await?
            == StatusWrite::Superseded
        {
            return Ok(ProbeOutcome::deferred(
                job_name,
                true,
                Deferred::StatusSuperseded,
            ));
        }
        return Ok(ProbeOutcome {
            job_name,
            created: true,
            reachable: None,
            cluster_id: None,
            reason: Some(REASON_PROBE_RUNNING.to_string()),
            ttl_patched: false,
            requeue: Requeue::After(REQUEUE_SECS),
            deferred: None,
        });
    };

    // A Job Kubernetes is deleting is not a probe: under foreground
    // propagation its pod goes first, so it would read as a crash and clear a
    // `reachable` its own reading recorded. No pod read, no status write, no
    // TTL patch; the Job's deletion event creates the next probe. Bounded: a
    // stalled deletion keeps the fixed-name Job and so blocks the next probe,
    // and once the last reading is older than `STALE_AFTER_SECS`, `reachable`
    // is cleared (`ProbeStale`).
    if being_deleted(&job) {
        debug!(
            cluster = %name,
            namespace = %namespace,
            job = %job_name,
            finished = backup::job_finished(&job),
            verdict_recorded = verdict_recorded(&job),
            "the probe Job is being deleted; nothing is read from it, the last recorded verdict \
             stands, and the next probe is created once it is gone"
        );
        let since = job
            .metadata
            .deletion_timestamp
            .as_ref()
            .map_or_else(String::new, |t| {
                t.0.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
            });
        return defer_or_mark_stale(
            &clusters,
            cluster,
            &namespace,
            job_name,
            Deferred::JobBeingDeleted,
            || format!("its probe Job has been in deletion since {since}"),
            now,
        )
        .await;
    }

    // Step 2: running.
    if !backup::job_finished(&job) {
        // A pod the namespace refused is failed fast. No request while the
        // Job's status counts a pod or the Job is within the grace; otherwise
        // one pod list and, when it finds none, one events list by the Job's UID.
        if let Some(refusal) = check::refused_pod_creation(client, &namespace, &job, now)
            .await
            .map_err(KafkaClusterError::Api)?
        {
            let cancelled =
                check::cancel(client, &namespace, &job, &cluster.uid().unwrap_or_default())
                    .await
                    .map_err(KafkaClusterError::Api)?;
            let wrote = patch_status_if_changed(
                &clusters,
                cluster,
                &name,
                pod_refused_status_patch(cluster, &job_name, &refusal, now),
            )
            .await?;
            if wrote == StatusWrite::Superseded {
                return Ok(ProbeOutcome::deferred(
                    job_name,
                    false,
                    Deferred::StatusSuperseded,
                ));
            }
            warn_when_first_recorded!(
                wrote == StatusWrite::Changed,
                cluster = %name,
                namespace = %namespace,
                job = %job_name,
                code = refusal.code.as_str(),
                cancelled,
                reason = %refusal.message,
                "the probe Job's pod was refused at creation; the Job is cancelled and the \
                 status names the refusal and `reachable` is cleared"
            );
            return Ok(ProbeOutcome {
                job_name,
                created: false,
                reachable: None,
                cluster_id: None,
                reason: Some(crate::conditions::TERMINAL_STATE_POD_CREATION_FORBIDDEN.to_string()),
                ttl_patched: false,
                // The cancelled Job finishes within a second; the pass that
                // sees it finished gives it the TTL (step 4).
                requeue: Requeue::After(REQUEUE_SECS),
                deferred: None,
            });
        }
        if patch_status_if_changed(
            &clusters,
            cluster,
            &name,
            probe_started_patch(cluster, &job_name, now),
        )
        .await?
            == StatusWrite::Superseded
        {
            return Ok(ProbeOutcome::deferred(
                job_name,
                false,
                Deferred::StatusSuperseded,
            ));
        }
        return Ok(ProbeOutcome {
            job_name,
            created: false,
            reachable: None,
            cluster_id: None,
            reason: Some(REASON_PROBE_RUNNING.to_string()),
            ttl_patched: false,
            requeue: Requeue::After(REQUEUE_SECS),
            deferred: None,
        });
    }

    let found = find_pod(client, &namespace, &job_name, job.uid().as_deref()).await?;
    // The pod and its code travel together, so the `pods/log` call below holds
    // a `&Pod` and not an `Option` (as on the `Backup` path).
    let terminated = found
        .pod
        .as_ref()
        .and_then(|p| backup::terminated_exit_code(p).map(|code| (p, code)));

    // Step 4: the crashed-Job case, before the happy path, which needs a pod
    // whose container terminated.
    let Some((pod, exit_code)) = terminated else {
        // A Job whose verdict is already recorded is not re-judged: a pod it no
        // longer has was collected after that verdict, and reading the absence
        // as a crash would clear the `reachable` the reading wrote.
        if verdict_recorded(&job) {
            debug!(
                cluster = %name,
                namespace = %namespace,
                job = %job_name,
                pod_present = found.pod.is_some(),
                "the probe Job's verdict is already on the status (it carries this reconciler's \
                 marker), so the absence of a terminated runner now is not a crash; nothing is \
                 re-judged"
            );
            // Bounded the same way: a judged Job its TTL never collects keeps
            // the next probe from running.
            return defer_or_mark_stale(
                &clusters,
                cluster,
                &namespace,
                job_name,
                Deferred::VerdictRecorded,
                || "its judged probe Job has not been collected by its TTL".to_string(),
                now,
            )
            .await;
        }
        // A Job that never had a pod and was told why. Read before the crash
        // classification, which can only say `NoExitCode` for "no pod".
        let refusal = if found.pod.is_none() && found.contested.is_empty() {
            check::finished_without_pod(client, &namespace, &job, now).await
        } else {
            None
        };
        if let Some(refusal) = refusal {
            // The refusal's instant, for the condition only: it is not an
            // observation of the cluster, so it never reaches `observedAt`.
            let seen = observed_at(&job, None, now);
            let wrote = patch_status_if_changed(
                &clusters,
                cluster,
                &name,
                pod_refused_status_patch(cluster, &job_name, &refusal, seen),
            )
            .await?;
            if wrote == StatusWrite::Superseded {
                return Ok(ProbeOutcome::deferred(
                    job_name,
                    false,
                    Deferred::StatusSuperseded,
                ));
            }
            // Once per Job: the fail-fast pass in step 2 usually recorded this
            // refusal already, and then this pass changes nothing.
            warn_when_first_recorded!(
                wrote == StatusWrite::Changed,
                cluster = %name,
                namespace = %namespace,
                job = %job_name,
                code = refusal.code.as_str(),
                reason = %refusal.message,
                "the probe Job finished without ever having a pod, because its pod was refused \
                 at creation; `reachable` is cleared and the Job gets the usual TTL, so the \
                 next probe runs on the ordinary cadence"
            );
            // Only now, as on the happy path: the status naming the refusal is
            // on the server before the Job (and the Event's subject) can be
            // collected. The TTL is the re-probe timer, so a lifted quota is
            // noticed within one cadence and a standing one costs one refused
            // Job per cadence.
            let ttl_patched = set_probe_ttl(&jobs, &job).await?;
            return Ok(ProbeOutcome {
                job_name,
                created: false,
                reachable: None,
                cluster_id: None,
                reason: Some(crate::conditions::TERMINAL_STATE_POD_CREATION_FORBIDDEN.to_string()),
                ttl_patched,
                requeue: Requeue::After(RE_PROBE_SECS),
                deferred: None,
            });
        }
        let terminal_state = if found.contested.is_empty() {
            backup::crash_terminal_state(found.pod.as_ref())
        } else {
            TERMINAL_STATE_POD_OWNERSHIP_CONTESTED
        };
        let observed = observed_at(&job, found.pod.as_ref(), now);
        let wrote = patch_status_if_changed(
            &clusters,
            cluster,
            &name,
            crashed_status_patch(cluster, terminal_state, &job_name, observed),
        )
        .await?;
        if wrote == StatusWrite::Superseded {
            return Ok(ProbeOutcome::deferred(
                job_name,
                false,
                Deferred::StatusSuperseded,
            ));
        }
        // A real crash is a WARN once per Job, on the pass whose write recorded
        // it; the TTL below marks the verdict recorded.
        warn_when_first_recorded!(
            wrote == StatusWrite::Changed,
            cluster = %name,
            namespace = %namespace,
            job = %job_name,
            terminal_state,
            contested = %found.contested.join(","),
            "no probe output could be read: either the Job finished with no terminated state for \
             the runner container, or more than one pod claimed the Job and none was read. \
             Nothing about this cluster is known either way: `reachable` is cleared, and the Job \
             gets the usual TTL so the next probe runs on the ordinary cadence"
        );
        // The TTL replaces the Job on the re-probe cadence; without it the
        // pod-less Job would be re-read on every requeue and the connection
        // never probed again. Patched only after the status above landed.
        let ttl_patched = set_probe_ttl(&jobs, &job).await?;
        return Ok(ProbeOutcome {
            job_name,
            created: false,
            reachable: None,
            cluster_id: None,
            reason: Some(terminal_state.to_string()),
            ttl_patched,
            requeue: Requeue::After(RE_PROBE_SECS),
            deferred: None,
        });
    };

    // Step 3: read the two lines through the `pods/log` subresource, the only
    // route to a runner's stdout.
    let pod_name = pod.name_any();
    let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace);
    let raw = match runner_log::read_terminal(&pods, &pod_name).await {
        Ok(runner_log::TerminalRead::Read(raw)) => raw,
        // Cut before its end: nothing is taken from it.
        Ok(runner_log::TerminalRead::OverBound) => String::new(),
        // The pod went between the list and the read. Nothing was read, so
        // nothing is written: the next pass sees the Job as it now stands.
        Err(kube::Error::Api(e)) if e.code == 404 => {
            debug!(
                cluster = %name,
                namespace = %namespace,
                pod = %pod_name,
                "the probe pod was gone by the log read; nothing was read, so nothing is written"
            );
            return Ok(ProbeOutcome::deferred(job_name, false, Deferred::PodGone));
        }
        Err(e) => return Err(KafkaClusterError::Api(e)),
    };
    // THE one answer to "whose line is it" (FX-43): only the lines the probe
    // sealed with this Job's token.
    let lines = runner_log::own_lines(&job::job_token(&job), &raw);
    let v = if !raw.is_empty() && runner_log::vouches_for_exit(exit_code) && lines.runner_is_older()
    {
        runner_older_verdict()
    } else {
        verdict(&probe_report(lines.text()))
    };
    let observed = observed_at(&job, Some(pod), now);

    // A reading too old to vouch for is not written as a verdict: no newer
    // probe has run, so `reachable` is cleared (`ProbeStale`) and the Job gets
    // its TTL so the next probe runs.
    if reading_is_stale(observed, now) {
        let taken = observed.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let detail = format!(
            "probe Job {job_name}'s own reading was taken at {taken} and the Job has not been \
             replaced"
        );
        let wrote = patch_status_if_changed(
            &clusters,
            cluster,
            &name,
            stale_status_patch(cluster, &detail, now),
        )
        .await?;
        if wrote == StatusWrite::Superseded {
            return Ok(ProbeOutcome::deferred(
                job_name,
                false,
                Deferred::StatusSuperseded,
            ));
        }
        warn_when_first_recorded!(
            wrote == StatusWrite::Changed,
            cluster = %name,
            namespace = %namespace,
            job = %job_name,
            reason = %detail,
            "the probe reading is too old to vouch for; `reachable` is cleared (ProbeStale) and \
             the Job gets the usual TTL, so the next probe runs"
        );
        let ttl_patched = if verdict_recorded(&job) {
            false
        } else {
            set_probe_ttl(&jobs, &job).await?
        };
        return Ok(ProbeOutcome {
            job_name,
            created: false,
            reachable: None,
            cluster_id: None,
            reason: Some(REASON_PROBE_STALE.to_string()),
            ttl_patched,
            requeue: Requeue::After(RE_PROBE_SECS),
            deferred: None,
        });
    }

    let wrote = patch_status_if_changed(
        &clusters,
        cluster,
        &name,
        observed_status_patch(cluster, &v, exit_code, observed),
    )
    .await?;
    // Only once the verdict landed: a superseded write ends the pass here, so
    // no TTL exists for a reading the status does not hold.
    if wrote == StatusWrite::Superseded {
        return Ok(ProbeOutcome::deferred(
            job_name,
            false,
            Deferred::StatusSuperseded,
        ));
    }

    if v.reachable.is_none() {
        warn_when_first_recorded!(
            wrote == StatusWrite::Changed,
            cluster = %name,
            namespace = %namespace,
            pod = %pod_name,
            exit_code,
            tail_lines = backup::KEY_SCAN_TAIL_LINES,
            "the probe pod log carried no parseable contract line; `reachable` is cleared and \
             nothing is guessed from the exit code"
        );
    }

    // Only now: a status patch that did not land has left this function, so pod
    // garbage collection cannot start on a probe whose log was never read. A
    // Job an earlier pass already marked is not patched again.
    let ttl_patched = if verdict_recorded(&job) {
        false
    } else {
        set_probe_ttl(&jobs, &job).await?
    };

    if wrote == StatusWrite::Changed || ttl_patched {
        info!(
            cluster = %name,
            namespace = %namespace,
            job = %job_name,
            exit_code,
            reachable = ?v.reachable,
            cluster_id = v.cluster_id.as_deref().unwrap_or("<unread>"),
            reason = %v.reason,
            "the probe finished; its verdict is on the status and the Job now has a TTL, which is \
             also when the next probe runs"
        );
    }

    Ok(ProbeOutcome {
        job_name,
        created: false,
        reachable: v.reachable,
        cluster_id: v.cluster_id,
        reason: Some(v.reason),
        ttl_patched,
        // Never past the staleness bound: a Job its TTL does not collect is
        // looked at again before this reading turns stale.
        requeue: requeue_before_stale(RE_PROBE_SECS, Some(observed), now),
        deferred: None,
    })
}

/// A pass that forms no verdict on purpose, unless the last reading has
/// outlived [`STALE_AFTER_SECS`]; then it is cleared ([`stale_status_patch`])
/// and logged at WARN once, on the write that recorded it. `detail` names why
/// no newer probe can answer.
#[allow(clippy::too_many_arguments)]
async fn defer_or_mark_stale(
    clusters: &Api<KafkaCluster>,
    cluster: &KafkaCluster,
    namespace: &str,
    job_name: String,
    why: Deferred,
    detail: impl FnOnce() -> String,
    now: DateTime<Utc>,
) -> Result<ProbeOutcome, KafkaClusterError> {
    let name = cluster.name_any();
    let mut outcome = ProbeOutcome::deferred(job_name, false, why);
    let default = match outcome.requeue {
        Requeue::After(secs) => secs,
        Requeue::AwaitChange => REQUEUE_SECS,
    };
    if !reachable_is_stale(cluster, now) {
        outcome.requeue = requeue_before_stale(
            default,
            cluster.status.as_ref().and_then(|s| s.observed_at),
            now,
        );
        return Ok(outcome);
    }
    let detail = detail();
    let wrote = patch_status_if_changed(
        clusters,
        cluster,
        &name,
        stale_status_patch(cluster, &detail, now),
    )
    .await?;
    if wrote == StatusWrite::Superseded {
        return Ok(ProbeOutcome::deferred(
            outcome.job_name,
            false,
            Deferred::StatusSuperseded,
        ));
    }
    warn_when_first_recorded!(
        wrote == StatusWrite::Changed,
        cluster = %name,
        namespace = %namespace,
        job = %outcome.job_name,
        reason = %detail,
        "the last probe reading is too old to vouch for and no newer probe can answer; \
         `reachable` is cleared (ProbeStale)"
    );
    outcome.reason = Some(REASON_PROBE_STALE.to_string());
    Ok(outcome)
}

/// The `kube::runtime` reconcile entry point, and the one clock read in this
/// file.
async fn reconcile(
    cluster: Arc<KafkaCluster>,
    ctx: Arc<Context>,
) -> Result<Action, KafkaClusterError> {
    let outcome =
        reconcile_cluster_with_runner_image(&cluster, &ctx.client, Utc::now(), &ctx.runner_image)
            .await?;
    Ok(action_for(&outcome))
}

/// Requeue on an error, naming it. Never a panic and never a drop.
///
/// `reachable: false` is an outcome, not an error, and never reaches here.
/// Every error that does is a WARN: the races of a Job being collected are
/// answered in place as outcomes, so anything else, including a `404` or a
/// `409` from any other call, is not a known race and stays visible.
fn error_policy(cluster: Arc<KafkaCluster>, err: &KafkaClusterError, _ctx: Arc<Context>) -> Action {
    log_reconcile_error(&cluster.name_any(), err);
    Action::requeue(std::time::Duration::from_secs(REQUEUE_SECS))
}

/// [`error_policy`]'s log line, at WARN for every error. Its own function so
/// a row can read the level.
pub fn log_reconcile_error(cluster: &str, err: &KafkaClusterError) {
    warn!(
        cluster = %cluster,
        error = %err,
        "KafkaCluster probe reconcile failed; requeueing"
    );
}

/// Run the `KafkaCluster` probe controller until the process ends.
///
/// It `owns` the probe Jobs it creates, so a probe pod terminating and the
/// Job's TTL collection (when the next probe is due) wake this reconciler, not
/// only the requeue timer. It takes no archive handle: a probe reads no archive,
/// so this file is not one of interface **I13**'s.
pub fn controller(
    client: kube::Client,
    runner_image: job::RunnerImage,
) -> impl std::future::Future<Output = ()> + Send {
    // One watch per watched namespace: the whole cluster unless
    // `LOGWEIR_WATCH_NAMESPACES` names namespaces, and then an `Api::namespaced`
    // watch each, the only shape the scoped chart's RoleBindings permit.
    crate::scope::run_everywhere(move |namespace| {
        controller_in(client.clone(), runner_image.clone(), namespace)
    })
}

/// One watch of [`controller`], over `namespace` (`None` is the whole
/// cluster).
fn controller_in(
    client: kube::Client,
    runner_image: job::RunnerImage,
    namespace: Option<String>,
) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<KafkaCluster> = crate::scope::api(&client, namespace.as_deref());
    let jobs: Api<Job> = crate::scope::api(&client, namespace.as_deref());
    let ctx = Arc::new(Context {
        client,
        archive: None,
        // No backup Job here, but the probe Job runs the runner image under the
        // runner pull policy.
        runner_image,
    });
    async move {
        Controller::new(api, watcher::Config::default())
            .owns(jobs, watcher::Config::default())
            .run(reconcile, error_policy, ctx)
            .for_each(|_| std::future::ready(()))
            .await;
    }
}
