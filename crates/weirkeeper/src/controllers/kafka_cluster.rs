//! The `KafkaCluster` probe reconciler — the loop that makes
//! `status.reachable` mean something.
//!
//! # THE CONTROLLER NEVER DIALS A BROKER, AND THAT IS WHY A PROBE IS A JOB
//!
//! Spec §9: there is **no `get` on Secrets anywhere** in the control plane. A
//! SASL password therefore never reaches this process, so this process cannot
//! authenticate to a cluster that requires one — and a probe that worked only
//! for `plaintext` clusters would report `reachable: false` for every
//! authenticated cluster in the fleet, which is worse than no probe. So the
//! dial happens where the credential can be projected and nowhere else: in a
//! short-lived runner Job, exactly like every other execution in this product
//! (Global Constraint 33). The Secret is named in the Job's
//! `valueFrom.secretKeyRef` — a reference this controller writes and a value it
//! cannot see.
//!
//! # The subcommand is `logweir cluster-probe`, not `logweir doctor`
//!
//! `doctor` takes two more MANDATORY paths, cannot be told which auth to use,
//! refuses unless the cluster is in a restore-target allowlist AND holds a
//! healthy marker topic — which no `role: source` cluster does — and prints the
//! observed id only inside prose. `crates/logweir/src/probe.rs`'s module header
//! states all four with their call sites. Interface **I14** is the contract this
//! module reads: **exactly two stdout lines**, `cluster-id=<id>` then
//! `reachable=true|false`.
//!
//! # The two lines are read BY KEY NAME from a bounded tail
//!
//! Plan erratum **E4**: a pod log is stdout and stderr merged in
//! **nondeterministic order**, measured twice on the same refusal with the
//! discriminator arriving last in one run and second-to-last in the other. No
//! reader here may take "the last line of the pod log". [`probe_report`] scans
//! [`backup::tail_lines`] — the ONE tail implementation, shared rather than
//! copied, so a change to [`backup::KEY_SCAN_TAIL_LINES`] reaches every scanner
//! — and matches on the key name.
//!
//! # Nothing is guessed
//!
//! A log body carrying neither line yields `reachable: None`, no `clusterId`,
//! and the condition reason [`REASON_PROBE_OUTPUT_UNREADABLE`]. **The exit code
//! alone never writes a `reachable` value**: exit 1 is Global Constraint 11's
//! "operational error" and covers an unreachable broker, a missing runner
//! binary, a bad flag and an unprojected credential alike, so inferring
//! `reachable: false` from it would report a control-plane mistake as a fact
//! about somebody's cluster.
//!
//! # It creates, it patches, and it deletes nothing
//!
//! The re-probe cadence is the Job's own [`PROBE_TTL_SECONDS`]: the API server
//! garbage-collects the finished Job, the next reconcile finds none, and a fresh
//! probe runs. This reconciler deletes no Job, no pod and no object — the rule
//! [`super`]'s header states, kept by a controller whose whole purpose is a
//! repeating observation.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, ListParams, LogParams, Patch, PatchParams, PostParams};
use kube::runtime::controller::Action;
use kube::runtime::{watcher, Controller};
use kube::{Resource, ResourceExt as _};
use serde_json::{json, Value};
use std::fmt;
use tracing::{debug, info, warn};

use super::backup;
use super::Context;
use crate::conditions::{
    current_condition, merge_condition, status_unchanged, TERMINAL_STATE_NAME_TOO_LONG,
};
use crate::connection::{self, ConnectionUse, ResolvedConnection};
use crate::crds::kafka_cluster::{AuthMode, KafkaCluster};
use crate::job::{self, RunnerJobSpec, RunnerOwner};

/// The `Job` name a `KafkaCluster` gets: `logweir-probe-<cr name>`.
///
/// **PREFIXED, UNLIKE THE `Backup` AND `Restore` JOBS**, which are named after
/// their CR verbatim. Those objects exist to be run once; a `KafkaCluster` is a
/// long-lived connection object that other kinds reference by name, and a Job
/// sharing its name would collide with anything else this control plane might
/// one day create for the same cluster. The cost is thirteen characters of the
/// 63-character `batch.kubernetes.io/job-name` label budget, which is exactly
/// what [`name_limit_for_cluster`] is about and why a long cluster name is
/// refused BEFORE any `POST`.
pub const PROBE_JOB_PREFIX: &str = "logweir-probe-";

/// The environment variable the probe reads its SASL password from.
///
/// **`LOGWEIR_SOURCE_PASSWORD`, FOR A CLUSTER IN EITHER ROLE.** The runner has
/// two variables — one per side of a drill — and a probe has no sides: it is
/// reading one cluster, so it reads the "source" one whatever the object's
/// `spec.role` says. `logweir cluster-probe`'s own help text names the same
/// variable, and the pair is asserted by
/// `the_probe_password_variable_matches_the_subcommand`.
pub const SOURCE_PASSWORD_ENV: &str = "LOGWEIR_SOURCE_PASSWORD";

/// The key inside a `KafkaCluster`'s `auth.secretRef` Secret that holds the
/// password.
///
/// The same key the `Restore` path projects
/// ([`super::restore::TARGET_PASSWORD_SECRET_KEY`]): one convention for one
/// kind of Secret, so an adopter who wrote a credential for a restore does not
/// have to write a second one for the probe.
pub const SOURCE_PASSWORD_SECRET_KEY: &str = super::restore::TARGET_PASSWORD_SECRET_KEY;

/// Interface **I14**'s first line, as a prefix. Matched BY NAME (erratum E4).
pub const CLUSTER_ID_PREFIX: &str = "cluster-id=";

/// Interface **I14**'s second line, as a prefix. Matched BY NAME (erratum E4).
pub const REACHABLE_PREFIX: &str = "reachable=";

/// The one condition type a `KafkaCluster` carries.
///
/// The CRD's own condition-type description names it. Declared HERE rather than
/// in [`crate::conditions`] for the reason `super::restore`'s
/// `REFERENT_NOT_FOUND_REASON` is: that module holds Global Constraint 11's
/// exit-code vocabulary and the terminal states an EXECUTION can reach, and a
/// probe's condition is neither.
pub const CONDITION_REACHABLE: &str = "Reachable";

/// `Reachable=True`. A broker answered and named its cluster id.
pub const REASON_REACHABLE: &str = "Reachable";

/// `Reachable=False`. **The probe ran and said `reachable=false` itself.**
///
/// NOT DERIVED FROM THE EXIT CODE. The runner prints this line; exit 1 alone
/// could equally mean the image was wrong, so the LINE is what writes `false`
/// and the code never does.
pub const REASON_PROBE_REPORTED_UNREACHABLE: &str = "ProbeReportedUnreachable";

/// `Reachable=Unknown`. The pod log carried neither contract line, so nothing
/// about the cluster is known either way.
pub const REASON_PROBE_OUTPUT_UNREADABLE: &str = "ProbeOutputUnreadable";

/// `Reachable=Unknown`. A probe Job exists and has not finished.
///
/// WRITTEN ON THE CREATING PASS, AND NOT NOTHING. A `KafkaCluster` with an
/// empty status is indistinguishable from one this controller has never seen —
/// the shape review finding **MEDIUM-1** was about — and a merge patch that
/// touches neither `reachable` nor `clusterId` leaves the LAST KNOWN values
/// alone while a fresh probe is in flight, which is what an operator wants from
/// a re-probe.
pub const REASON_PROBE_RUNNING: &str = "ProbeRunning";

/// Every condition `reason` this module writes, in one list.
///
/// ONE LIST SO A REGEX TEST CANNOT MISS ONE:
/// `every_probe_condition_reason_is_a_valid_metav1_reason` iterates it against
/// `^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$`, the pattern a real
/// `metav1.Condition` validates `reason` against and which **forbids `-`**
/// (errata **E5b**).
pub const PROBE_CONDITION_REASONS: &[&str] = &[
    REASON_REACHABLE,
    REASON_PROBE_REPORTED_UNREACHABLE,
    REASON_PROBE_OUTPUT_UNREADABLE,
    REASON_PROBE_RUNNING,
    crate::conditions::TERMINAL_STATE_CONNECTION_CONFIG_INVALID,
    crate::conditions::TERMINAL_STATE_CONNECTION_REFERENCE_INVALID,
    crate::conditions::TERMINAL_STATE_CONNECTION_FIELD_UNSUPPORTED,
    crate::conditions::TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
];

/// `spec.activeDeadlineSeconds` on a probe Job.
///
/// `KafkaCluster.spec` carries no `deadlineSeconds` — it is a connection
/// object, not a run — so the value is this module's. **TWO MINUTES**: the dial
/// itself is bounded at ten seconds by the subcommand, and the rest of the
/// budget is image start, so a deadline that fires means the pod never got
/// going rather than that the broker was slow.
pub const PROBE_DEADLINE_SECONDS: i64 = 120;

/// `ttlSecondsAfterFinished` on a finished probe Job — **and therefore the
/// re-probe interval**.
///
/// # This is the schedule, and the mechanism is the point
///
/// The brief names no re-probe cadence, so five minutes is this task's recorded
/// choice. It is spelled as a TTL rather than as a timer because a reconciler in
/// this directory **deletes nothing**: the API server collects the finished Job,
/// the next reconcile finds no Job, and a fresh probe runs. A `KafkaCluster` is
/// not evidence, which is why this is not the `Backup` path's seven days
/// (`backup::TTL_SECONDS_AFTER_FINISHED`) — a probe Job whose log has been read
/// onto the status has nothing left to say.
///
/// Patched ONLY AFTER the status write that carries the verdict has returned
/// 200, exactly as on the `Backup` path: pod garbage collection must never race
/// the log read.
pub const PROBE_TTL_SECONDS: i32 = 300;

/// How long before a probe Job that has not finished is looked at again.
pub const REQUEUE_SECS: u64 = 15;

/// How long before a cluster whose probe has been read is probed again.
///
/// [`PROBE_TTL_SECONDS`] **plus a margin**, deliberately: a requeue landing at
/// exactly the TTL would find the Job still present about half the time, write
/// the same verdict again and wait another full interval, turning a five-minute
/// cadence into a ten-minute one. The Job's deletion is also a watch event
/// (`controller().owns(jobs)`), so this timer is the backstop and not the
/// primary path.
pub const RE_PROBE_SECS: u64 = PROBE_TTL_SECONDS as u64 + 15;

/// The longest `metadata.name` a `KafkaCluster` may have and still be probed.
///
/// The Job's name is [`PROBE_JOB_PREFIX`] + the cluster's name, and the Job's
/// pods carry that name in the `batch.kubernetes.io/job-name` LABEL, whose
/// values stop at [`crate::slot::NAME_LIMIT`] characters. Measured on the
/// `Backup` path (review finding **MEDIUM-1**, errata **E5d**): the API server
/// refuses the Job outright — `spec.template.labels: Invalid value: … must be
/// no more than 63 characters` — which a reconciler turns into a requeue, and
/// the object then sits with `status: null` forever.
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
}

/// Read the two contract lines out of a pod log, **by key name, from a bounded
/// tail**.
///
/// [`backup::tail_lines`] is the ONE tail implementation in this crate
/// (erratum **E4**), shared and not forked: the prefix sets differ between the
/// three reconcilers, the definition of "the tail" must not.
///
/// The LAST occurrence of each key wins within that tail, matching
/// [`backup::evidence_keys`]: a runner that printed a draft line would have the
/// final one be the true one.
///
/// # Why an empty `cluster-id=` is an absence
///
/// Interface I14 prints `cluster-id=` with no value on the unreachable arm,
/// deliberately, so nothing can be mistaken for an observation. Recording
/// `Some("")` would put an empty string in `status.clusterId`, where Global
/// Constraint 18's fourth rail later compares it against a target's id —
/// `"" != ""` is false, and a rail that compares two absences is a rail that
/// passes.
#[must_use]
pub fn probe_report(log: &str) -> ProbeReport {
    let mut report = ProbeReport::default();
    for line in backup::tail_lines(log) {
        if let Some(v) = line.strip_prefix(CLUSTER_ID_PREFIX) {
            report.cluster_id = if v.trim().is_empty() {
                None
            } else {
                Some(v.trim().to_string())
            };
        }
        if let Some(v) = line.strip_prefix(REACHABLE_PREFIX) {
            report.reachable = match v.trim() {
                "true" => Some(true),
                "false" => Some(false),
                _ => None,
            };
        }
    }
    report
}

/// What one reading of a probe's log decided.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Verdict {
    /// The `Reachable` condition's `status` — `True`, `False` or `Unknown`.
    pub status: &'static str,
    /// The condition's `reason`, and verbatim the scalar `status.reason`.
    pub reason: String,
    /// The value for `status.reachable`. `None` leaves the field alone: a merge
    /// patch that omits a key does not clear it, so an unreadable probe does
    /// not erase the last thing that WAS known.
    pub reachable: Option<bool>,
    /// The value for `status.clusterId`, written only alongside
    /// `reachable: Some(true)`.
    pub cluster_id: Option<String>,
}

/// The verdict for one finished probe, **from its log alone**.
///
/// # THE TWO LINES DECIDE, AND THIS FUNCTION CANNOT SEE THE EXIT CODE
///
/// The signature is the property. Interface **I14** makes the two lines the
/// machine contract and the code its summary, and exit **1** is Global
/// Constraint 11's *operational error* — what a wrong image, a bad flag, an
/// unprojected credential and a genuinely unreachable broker all produce alike.
/// A controller that read `reachable: false` off the code would publish a
/// control-plane mistake as a fact about somebody's cluster, so the code is not
/// an argument here: it reaches the condition MESSAGE
/// ([`verdict_message`]) — where a human reading `kubectl describe` wants it —
/// and the classification of a run with NO code at all is
/// [`backup::crash_terminal_state`]'s, a different function for a different
/// observation.
#[must_use]
pub fn verdict(report: &ProbeReport) -> Verdict {
    match report.reachable {
        Some(true) => Verdict {
            status: "True",
            reason: REASON_REACHABLE.to_string(),
            reachable: Some(true),
            cluster_id: report.cluster_id.clone(),
        },
        Some(false) => Verdict {
            status: "False",
            reason: REASON_PROBE_REPORTED_UNREACHABLE.to_string(),
            reachable: Some(false),
            // NEVER CARRIED OVER. An unreachable probe observed no cluster id,
            // and the previous pass's value is not an observation this pass
            // made.
            cluster_id: None,
        },
        None => Verdict {
            status: "Unknown",
            reason: REASON_PROBE_OUTPUT_UNREADABLE.to_string(),
            reachable: None,
            cluster_id: None,
        },
    }
}

/// The condition MESSAGE for a verdict, naming the exit code the probe pod
/// reported.
#[must_use]
pub fn verdict_message(v: &Verdict, exit_code: i32) -> String {
    match v.reachable {
        Some(true) => format!(
            "the probe reached a broker and read cluster id {} (pod exit {exit_code})",
            v.cluster_id.as_deref().unwrap_or("<unnamed>")
        ),
        Some(false) => format!(
            "the probe ran and printed `{REACHABLE_PREFIX}false` (pod exit {exit_code}); the \
             broker did not answer, or it refused the credential this cluster names"
        ),
        None => format!(
            "the pod log carried no parseable `{CLUSTER_ID_PREFIX}` / `{REACHABLE_PREFIX}` line \
             in its last {} non-empty lines (pod exit {exit_code}); `reachable` is left unset \
             rather than guessed from the exit code, which Global Constraint 11 uses for every \
             operational failure alike",
            backup::KEY_SCAN_TAIL_LINES
        ),
    }
}

/// The probe's argv — interface **I14**'s flag list, built from the RESOLVED
/// connection (PLAT-07.1) plus this object's marker topic.
///
/// The connection flags are [`ResolvedConnection::probe_args`] — the same
/// resolution the backup and restore Jobs are built from, so a probe can never
/// dial settings those runs do not use. A connection that does not resolve has
/// no argv at all: [`runner_job_spec`] refuses it first.
///
/// `--marker-topic` IS PASSED UNCONDITIONALLY WHEN THE FIELD IS SET, and the
/// probe never asserts it: this controller does not branch on a field whose only
/// consumer is a drill-time guard, and the subcommand refuses nothing because of
/// it. `--tls` is emitted only when the transport really is TLS, the way
/// `super::restore::runner_argv` emits `--approver-key-ids` only for a non-empty
/// roster: a boolean flag has no false form.
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
/// **THE SPELLINGS ARE BYTE-IDENTICAL TO THE CRD ENUM AND TO
/// `logweir_core::spec::AuthSpec`** (interface I33), so this is a rename of a
/// value and never a translation of one. Written as a `match` with no wildcard:
/// a third mode fails to compile here rather than reaching an argv as
/// `plaintext`.
#[must_use]
pub fn auth_mode_flag(mode: AuthMode) -> &'static str {
    match mode {
        AuthMode::Plaintext => "plaintext",
        AuthMode::ScramSha512 => "scramSha512",
    }
}

/// The probe Job for a cluster.
///
/// # Errors
///
/// [`KafkaClusterError::NoNamespace`] / [`KafkaClusterError::NoUid`] — neither
/// reachable from the API server, named rather than unwrapped — and
/// [`KafkaClusterError::Refused`] carrying the connection's own refusal when
/// [`connection::resolve`] refuses it (PLAT-07.1).
pub fn runner_job_spec(cluster: &KafkaCluster) -> Result<RunnerJobSpec, KafkaClusterError> {
    let name = cluster.name_any();
    let namespace = cluster
        .namespace()
        .ok_or_else(|| KafkaClusterError::NoNamespace(name.clone()))?;
    let uid = cluster
        .uid()
        .ok_or_else(|| KafkaClusterError::NoUid(name.clone()))?;

    // THE ONE RESOLUTION (PLAT-07.1). The password and the CA, PROJECTED AND
    // NEVER READ: `valueFrom.secretKeyRef` and a projected volume only — this
    // controller holds no `get` on Secrets (spec §9), so these are references
    // it writes into a pod spec and values it cannot see. A `scramSha512`
    // cluster with no `secretRef` used to get a probe with no variable, which
    // then printed `reachable=false` about a configuration mistake; the
    // resolver refuses it before any Job exists instead.
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
        // NO SIGNING KEY, AND NOTHING BUT THE CONNECTION'S OWN CA. A probe
        // signs nothing, reads no approval and writes no artifact, so the only
        // volume it may carry besides `/work` is the private CA the connection
        // names — projected exactly as the backup and restore Jobs project it.
        secret_mounts: projection.secret_mounts,
        config_map_mounts: projection.config_map_mounts,
        env_from_secret: projection.env_from_secret,
        env_literal: {
            let mut env = vec![("RUST_LOG".to_string(), "info".to_string())];
            env.extend(projection.env_literal);
            env
        },
        // NO PLAN. `logweir cluster-probe` reads no `--spec`, which is why
        // `job::RunnerJobSpec::plan_config_map` is an `Option` at all — and a
        // Job with an empty `/plan` mount would stall in `ContainerCreating`
        // until its deadline fired (measured live on the `Backup` path).
        plan_config_map: None,
        // THE SHIPPED PIN, AND THE RECONCILER OVERWRITES IT IF THIS PROCESS
        // WAS HANDED ANOTHER IMAGE (Task 33, `job::RUNNER_IMAGE_ENV`). This
        // function is a pure function of the custom resource and stays one:
        // the override is a property of the PROCESS, read once in `main`.
        image: None,
        // AND THE COMPILED-IN `job::IMAGE_PULL_POLICY`, OVERWRITTEN THE SAME
        // WAY IF THIS PROCESS WAS HANDED ANOTHER POLICY (Task 37,
        // `job::RUNNER_PULL_POLICY_ENV`). Same argument, same one line.
        image_pull_policy: None,
    })
}

/// When the probe this pass is reading **actually finished**.
///
/// # THIS IS NOT `now`, AND THE LIVE RUN IS WHY
///
/// Measured on docker-desktop with `observedAt: now`: **3,388 reconciles in
/// ninety seconds**, a hot loop. The mechanism is `controller().owns(jobs)`
/// plus a status field that changes on every read — a status patch carrying a
/// fresh `now` bumps the object's `resourceVersion`, the `KafkaCluster` watch
/// fires, the reconcile re-reads the same finished Job, writes another fresh
/// `now`, and so on at ~2,000 passes a minute. (The TTL patch is not the
/// culprit: a merge patch whose content is unchanged is a no-op and bumps
/// nothing. The clock was.)
///
/// Taking the instant from the PROBE instead of from the controller fixes it at
/// the root and is also the more truthful value — the CRD field says "when the
/// probe above was performed", not "when a reconcile last looked". It is a
/// property of the Job, so every re-read of the same Job produces a
/// BYTE-IDENTICAL patch, the API server treats it as a no-op, and the loop has
/// nothing to feed on. `the_observed_status_patch_is_stable_across_passes` is
/// the regression test.
///
/// Four sources, in order of how close each is to the probe itself: the runner
/// container's own `finishedAt`, the Job's `completionTime`, the Job's terminal
/// condition's `lastTransitionTime`, and — only when the API server offered
/// none of the three — `now`.
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

// ---------------------------------------------------------------------------
// Status patches
// ---------------------------------------------------------------------------

/// One condition, as a merge-patch fragment. **Exactly one is ever written**
/// (errata **E5c**): a condition array is a map keyed by `type`, so two
/// `Reachable` entries would be a malformed status whatever their statuses said.
///
/// `lastTransitionTime` moves only when the condition actually transitions,
/// and the comparison that decides it is
/// [`crate::conditions::merge_condition`] — ONE implementation for all six
/// reconcilers (plan erratum E11(d)); this file used to carry a private copy.
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

/// Patch `/status` — unless the patch would change nothing.
///
/// The decision is [`crate::conditions::status_unchanged`]'s; this exists so
/// this reconciler's five patch sites read as one line each.
async fn patch_status_if_changed(
    api: &Api<KafkaCluster>,
    cluster: &KafkaCluster,
    name: &str,
    patch: Value,
) -> Result<(), KafkaClusterError> {
    if status_unchanged(
        cluster
            .status
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok())
            .as_ref(),
        &patch,
    ) {
        debug!(
            cluster = %name,
            "the computed status equals the one on the object; no patch is sent"
        );
        return Ok(());
    }
    api.patch_status(name, &PatchParams::default(), &Patch::Merge(patch))
        .await
        .map_err(KafkaClusterError::Api)?;
    Ok(())
}

/// The `/status` merge patch for the pass that CREATED a probe Job.
///
/// It touches neither `reachable` nor `clusterId` nor `observedAt`: a merge
/// patch that omits a key leaves it alone, so a re-probe in flight does not
/// erase the last thing that was actually observed. What it does write is that
/// somebody is looking — the difference between "not probed yet" and "this
/// controller has never seen the object".
#[must_use]
pub fn probe_started_patch(cluster: &KafkaCluster, job_name: &str, now: DateTime<Utc>) -> Value {
    json!({
        "status": {
            "reason": REASON_PROBE_RUNNING,
            "conditions": [condition(
                cluster,
                "Unknown",
                REASON_PROBE_RUNNING,
                &format!("probe Job {job_name} is running; the last observation, if any, stands"),
                now,
            )],
        }
    })
}

/// The `/status` merge patch for a probe whose log has been read.
///
/// `reachable` and `clusterId` appear only when the verdict HAS them, so an
/// unreadable probe leaves both alone rather than clearing them — and
/// `observedAt` is written on every verdict, because "we looked and could not
/// tell" is itself an observation with a time.
#[must_use]
pub fn observed_status_patch(
    cluster: &KafkaCluster,
    v: &Verdict,
    exit_code: i32,
    observed_at: DateTime<Utc>,
) -> Value {
    let mut status = serde_json::Map::new();
    if let Some(r) = v.reachable {
        status.insert("reachable".to_string(), json!(r));
    }
    if let Some(id) = v.cluster_id.as_ref() {
        status.insert("clusterId".to_string(), json!(id));
    }
    // THE PROBE'S OWN INSTANT, NOT THE CONTROLLER'S — see `observed_at` for the
    // 3,388-reconciles-in-ninety-seconds measurement that made this the value.
    status.insert("observedAt".to_string(), json!(observed_at));
    // The scalar the condition's reason is promoted to (review finding M2):
    // `reachable` alone cannot distinguish `ProbeOutputUnreadable` from
    // `NoExitCode` from a probe still in flight.
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

/// The `/status` merge patch for a probe Job that finished with **no exit code
/// at all** — the crashed-Job case.
///
/// The sub-case is [`backup::crash_terminal_state`]'s, shared rather than
/// re-derived, and the condition is `Unknown`: a pod that never ran wrote no
/// log, so nothing about the cluster is known either way. `reachable` and
/// `clusterId` are left alone.
#[must_use]
pub fn crashed_status_patch(
    cluster: &KafkaCluster,
    terminal_state: &str,
    job_name: &str,
    observed_at: DateTime<Utc>,
) -> Value {
    json!({
        "status": {
            "observedAt": observed_at,
            "reason": terminal_state,
            "conditions": [condition(
                cluster,
                "Unknown",
                terminal_state,
                &format!(
                    "probe Job {job_name} finished with no terminated state for the `{}` \
                     container, so no exit code and no log could be read; `reachable` is left \
                     unset rather than invented",
                    crate::job::CONTAINER_NAME
                ),
                observed_at,
            )],
        }
    })
}

/// The `/status` merge patch for a refusal this controller made ITSELF, before
/// any `POST`.
#[must_use]
pub fn refused_status_patch(
    cluster: &KafkaCluster,
    reason: &str,
    message: &str,
    now: DateTime<Utc>,
) -> Value {
    json!({
        "status": {
            "reason": reason,
            "conditions": [condition(cluster, "Unknown", reason, message, now)],
        }
    })
}

/// The `/status` merge patch for a connection this controller refused to
/// resolve (PLAT-07.1) — before any Job exists.
///
/// `reachable` IS CLEARED (`null` removes it under a merge patch) and nothing
/// else about the last probe is. A connection that does not resolve is not one
/// any run may use, and a `Restore` admits a target only on
/// `reachable == true`: a stale `true` from a probe an earlier controller ran
/// with different settings would let a restore reach Job construction on the
/// strength of an observation this controller does not stand behind.
/// `clusterId` and `observedAt` stay as the record of that last look.
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

/// Whether this object already carries a TERMINAL refusal.
///
/// Only one refusal on this path is terminal — [`TERMINAL_STATE_NAME_TOO_LONG`]
/// — because `metadata.name` is the one input to a probe that cannot change.
/// Everything else about a cluster's reachability is a repeating observation,
/// which is why this is not `status_is_terminal`-shaped on the `Backup` path's
/// model of "a run that has finished".
#[must_use]
pub fn status_is_terminal(cluster: &KafkaCluster) -> bool {
    cluster
        .status
        .as_ref()
        .and_then(|s| s.reason.as_deref())
        .is_some_and(|r| r == TERMINAL_STATE_NAME_TOO_LONG)
}

// ---------------------------------------------------------------------------
// Outcome, requeue, error
// ---------------------------------------------------------------------------

/// When to look at a cluster again.
///
/// A LOCAL ENUM AND NOT `super::restore::Requeue`. The shape is deliberately
/// identical — `Action` implements neither `PartialEq` nor a `Debug` a test can
/// read an interval off, so the interval has to be assertable as a value — but a
/// probe reconciler that imported the restore reconciler's type would couple two
/// modules that share no object, and [`action_for`] is the ONE place either of
/// them becomes an `Action`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Requeue {
    /// Nothing will change until something edits this object: `NameTooLong` is
    /// the only state that reaches it, because `metadata.name` is immutable.
    AwaitChange,
    /// Look again after this many seconds.
    After(u64),
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
    /// When to look again.
    pub requeue: Requeue,
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

// ---------------------------------------------------------------------------
// The reconcile
// ---------------------------------------------------------------------------

/// The probe pod for a Job, found by the two labels in
/// [`backup::pod_selectors`]'s order.
async fn find_pod(
    client: &kube::Client,
    namespace: &str,
    job_name: &str,
) -> Result<Option<Pod>, KafkaClusterError> {
    let api: Api<Pod> = Api::namespaced(client.clone(), namespace);
    for selector in backup::pod_selectors(job_name) {
        let list = api
            .list(&ListParams::default().labels(&selector))
            .await
            .map_err(KafkaClusterError::Api)?;
        if let Some(pod) = list.items.into_iter().next() {
            return Ok(Some(pod));
        }
        debug!(
            job = %job_name,
            selector = %selector,
            "no probe pod matched this selector; trying the next"
        );
    }
    Ok(None)
}

/// Reconcile one `KafkaCluster` at the instant `now`.
///
/// # The state machine, one pass per event
///
/// 0. **The object's own name is longer than [`name_limit_for_cluster`]** → a
///    TERMINAL status (condition `Reachable=Unknown`, reason `NameTooLong`) and
///    **nothing is created**, before any `POST`. The Job the API server would
///    refuse is a Job whose pods could never be labelled, and a requeue on a
///    refusal that can never succeed leaves the object with no status at all
///    (review **MEDIUM-1**, errata **E5d**).
/// 1. **No Job** → `POST` the probe Job; status says a probe is running, and
///    the last observation stands.
/// 2. **Job exists, not finished** → the same running status. Nothing else.
/// 3. **Job finished, `runner` terminated** → `get` the pod's log through the
///    `pods/log` subresource, read the two contract lines BY NAME from a bounded
///    tail, patch `/status` with `reachable`, `clusterId`, `observedAt`, the
///    scalar `reason` and ONE condition — **and only after that patch returns
///    200**, `PATCH` the Job with `ttlSecondsAfterFinished`, which is also the
///    re-probe timer.
/// 4. **Job finished, no terminated state for `runner`** → the crashed-Job
///    case, classified by [`backup::crash_terminal_state`], with `reachable`
///    left alone.
///
/// # No clock read in this function
///
/// `now` is an argument, as on every other path in this directory: the one clock
/// read is in the `kube::runtime` wrapper, so every assertion below is over a
/// value.
///
/// # Interface I28 is a declared late binding
///
/// Reading the two lines needs `get` on the `pods/log` subresource, and the
/// ClusterRole that grants it is Task 21's. Every test of this function runs
/// against [`crate::testing::mock_client`] and needs no RBAC; the first
/// execution that needs the real rule is Task 24's `just k8s-demo`.
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
/// process was handed — Task 33, and Task 37's policy beside it.
///
/// The probe Job runs the SAME image the backup and restore runners do
/// (`logweir cluster-probe` is a subcommand of the one binary), so both
/// overrides reach it too: on a cluster that did not build the pins, a probe
/// Job naming the compile-time digest is `ErrImageNeverPull` exactly as a
/// backup Job would be, and a probe Job naming a `latest` tag under a policy
/// that never pulls is the same failure again. An unset field is the
/// compiled-in constant — `job::RUNNER_IMAGE`, `job::IMAGE_PULL_POLICY`.
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
            // THE ONE PLACE A SELF-DECIDED REFUSAL IS WRITTEN, so it cannot be
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
                patch_status_if_changed(
                    &clusters,
                    cluster,
                    &name,
                    refused_status_patch(cluster, state, &message, now),
                )
                .await?;
            }
            Ok(ProbeOutcome {
                job_name: probe_job_name(&name),
                created: false,
                reachable: None,
                cluster_id: None,
                reason: Some(state.to_string()),
                ttl_patched: false,
                requeue: Requeue::AwaitChange,
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

    // STEP 0. THE OBJECT'S OWN NAME, BEFORE ANY `POST` (errata E5d).
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

    // STEP 0b. THE CONNECTION, BEFORE ANY `GET` OR `POST` (PLAT-07.1). Pure,
    // like the name check above, and NOT terminal: it is re-evaluated on every
    // reconcile, so a controller upgrade that understands the object — or a
    // rollback fixed by rolling forward — clears the refusal with no edit.
    if let Err(refusal) = connection::resolve(cluster, ConnectionUse::Probe) {
        warn!(
            cluster = %name,
            namespace = %namespace,
            reason = refusal.reason,
            field = %refusal.field,
            "refusing to probe this KafkaCluster: its saved connection does not resolve, so no \
             probe Job is created"
        );
        patch_status_if_changed(
            &clusters,
            cluster,
            &name,
            connection_refused_status_patch(cluster, refusal.reason, &refusal.message, now),
        )
        .await?;
        return Ok(ProbeOutcome {
            job_name,
            created: false,
            reachable: None,
            cluster_id: None,
            reason: Some(refusal.reason.to_string()),
            ttl_patched: false,
            requeue: Requeue::AwaitChange,
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
            });
        }
        let mut spec = runner_job_spec(cluster)?;
        // THE TWO LINES THE OVERRIDES ARE (Task 33's image, Task 37's pull
        // policy). `None` in either leaves the compiled-in constant in place,
        // which is what every test that does not pass one sees.
        spec.image = runner.image.clone();
        spec.image_pull_policy = runner.image_pull_policy.clone();
        jobs.create(&PostParams::default(), &job::build(&spec))
            .await
            .map_err(KafkaClusterError::Api)?;
        info!(
            cluster = %name,
            namespace = %namespace,
            job = %job_name,
            "created the probe Job; this controller never dials a broker itself and never reads \
             a Secret, which is why a probe is a Job"
        );
        patch_status_if_changed(
            &clusters,
            cluster,
            &name,
            probe_started_patch(cluster, &job_name, now),
        )
        .await?;
        return Ok(ProbeOutcome {
            job_name,
            created: true,
            reachable: None,
            cluster_id: None,
            reason: Some(REASON_PROBE_RUNNING.to_string()),
            ttl_patched: false,
            requeue: Requeue::After(REQUEUE_SECS),
        });
    };

    // STEP 2. Running.
    if !backup::job_finished(&job) {
        patch_status_if_changed(
            &clusters,
            cluster,
            &name,
            probe_started_patch(cluster, &job_name, now),
        )
        .await?;
        return Ok(ProbeOutcome {
            job_name,
            created: false,
            reachable: None,
            cluster_id: None,
            reason: Some(REASON_PROBE_RUNNING.to_string()),
            ttl_patched: false,
            requeue: Requeue::After(REQUEUE_SECS),
        });
    }

    let pod = find_pod(client, &namespace, &job_name).await?;
    let exit_code = pod.as_ref().and_then(backup::terminated_exit_code);

    // STEP 4. The crashed-Job case, before the happy path, because the happy
    // path needs a pod whose container terminated and this branch is "there is
    // none".
    let Some(exit_code) = exit_code else {
        let terminal_state = backup::crash_terminal_state(pod.as_ref());
        let observed = observed_at(&job, pod.as_ref(), now);
        warn!(
            cluster = %name,
            namespace = %namespace,
            job = %job_name,
            terminal_state,
            "the probe Job finished with no terminated state for the runner container; nothing \
             about this cluster is known either way, and `reachable` is left unset"
        );
        patch_status_if_changed(
            &clusters,
            cluster,
            &name,
            crashed_status_patch(cluster, terminal_state, &job_name, observed),
        )
        .await?;
        return Ok(ProbeOutcome {
            job_name,
            created: false,
            reachable: None,
            cluster_id: None,
            reason: Some(terminal_state.to_string()),
            ttl_patched: false,
            requeue: Requeue::After(RE_PROBE_SECS),
        });
    };

    // STEP 3. Read the two lines through the `pods/log` subresource — the only
    // route to a runner's stdout, and the RBAC rule that grants it is Task 21's
    // (interface I28, a declared late binding).
    let pod_name = pod
        .as_ref()
        .map(kube::ResourceExt::name_any)
        .unwrap_or_default();
    let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace);
    let log = pods
        .logs(&pod_name, &LogParams::default())
        .await
        .map_err(KafkaClusterError::Api)?;
    let report = probe_report(&log);
    let v = verdict(&report);
    let observed = observed_at(&job, pod.as_ref(), now);

    if v.reachable.is_none() {
        warn!(
            cluster = %name,
            namespace = %namespace,
            pod = %pod_name,
            exit_code,
            tail_lines = backup::KEY_SCAN_TAIL_LINES,
            "the probe pod log carried no parseable contract line; `reachable` is left unset and \
             nothing is guessed from the exit code"
        );
    }

    patch_status_if_changed(
        &clusters,
        cluster,
        &name,
        observed_status_patch(cluster, &v, exit_code, observed),
    )
    .await?;

    // ONLY NOW. The `?` above is what makes this ordering a guarantee rather
    // than a comment: a status patch that did not return 200 leaves this
    // function before any TTL exists, so pod garbage collection cannot start on
    // a probe whose log was never read. The TTL is also the re-probe timer —
    // see `PROBE_TTL_SECONDS`.
    jobs.patch(
        &job_name,
        &PatchParams::default(),
        &Patch::Merge(json!({
            "spec": { "ttlSecondsAfterFinished": PROBE_TTL_SECONDS }
        })),
    )
    .await
    .map_err(KafkaClusterError::Api)?;

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

    Ok(ProbeOutcome {
        job_name,
        created: false,
        reachable: v.reachable,
        cluster_id: v.cluster_id,
        reason: Some(v.reason),
        ttl_patched: true,
        requeue: Requeue::After(RE_PROBE_SECS),
    })
}

/// The `kube::runtime` reconcile entry point. **THE ONE CLOCK READ IN THIS FILE
/// IS HERE.**
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
/// **A TRANSPORT ERROR REQUEUES; A FALSE PROBE DOES NOT REACH HERE.** `reachable:
/// false` is an OUTCOME with a status written for it and a re-probe scheduled by
/// the Job's TTL, not an error — the distinction interface I14 exists to make.
fn error_policy(cluster: Arc<KafkaCluster>, err: &KafkaClusterError, _ctx: Arc<Context>) -> Action {
    warn!(
        cluster = %cluster.name_any(),
        error = %err,
        "KafkaCluster probe reconcile failed; requeueing"
    );
    Action::requeue(std::time::Duration::from_secs(REQUEUE_SECS))
}

/// Run the `KafkaCluster` probe controller until the process ends.
///
/// `Api::all`, and it `owns` the probe Jobs it creates, so a probe pod
/// terminating — and the Job's own TTL collection, which is when the next probe
/// is due — wakes this reconciler through the Job rather than only on the
/// requeue timer.
///
/// It takes NO archive handle. A probe reads no archive, so this is the one
/// controller in the crate whose `Context` needs nothing but a client, and
/// `crates/weirkeeper/src/controllers/kafka_cluster.rs` is therefore not one of
/// interface **I13**'s files.
pub fn controller(
    client: kube::Client,
    runner_image: job::RunnerImage,
) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<KafkaCluster> = Api::all(client.clone());
    let jobs: Api<Job> = Api::all(client.clone());
    let ctx = Arc::new(Context {
        client,
        archive: None,
        // Task 33 and Task 37: this reconciler creates no BACKUP Job, but it
        // does create the probe Job, and that Job runs the runner image under
        // the runner pull policy.
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
