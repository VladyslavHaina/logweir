//! The `Backup` reconciler — one Job, and the exit-code contract made
//! visible.
//!
//! # The whole problem in one paragraph
//!
//! Kubernetes hides the one number Logweir exists to produce. `kubectl get
//! pods` renders every non-zero exit as a generic `Error`, the Job's own
//! `.status` carries only `BackoffLimitExceeded`, and the code lives at
//! exactly one path —
//! `pod.status.containerStatuses[].state.terminated.exitCode`. Worse,
//! `restartPolicy: OnFailure` **deletes the pod** (measured live,
//! `docs/kubernetes.md` §1), so the code is not buried in `lastState` — it is
//! gone. Global Constraint 11 has five codes and the corpus has a whole
//! exit-code-to-condition table with nothing implementing it. This module is
//! the implementation: [`job::build`]'s Job shape keeps the code readable,
//! and [`reconcile_backup`] lifts it onto `Backup.status.exitCode`, which is
//! the field spec §8's green badge reads.
//!
//! # The green rule reads the exit code, and there is no `outcome`
//!
//! A `Backup` is green ⟺ `status.evidence.verification.result == Valid`
//! **and** `status.exitCode == 0` (interface **I21**). `Restore` has an
//! `outcome`; `Backup` deliberately does not (spec §3.2, C95), and
//! `the_backup_green_rule_reads_exit_code_and_not_an_outcome` asserts the
//! absence rather than trusting it. Adding one would give the badge two
//! sources of truth that a partial status could disagree about.
//!
//! # The ordering that is load-bearing
//!
//! The status patch carrying the exit code happens **before** the Job is
//! patched with `ttlSecondsAfterFinished`. Pod garbage collection must never
//! race the exit-code read (`design-operator.md:264-266`): the TTL controller
//! deletes the Job **and its pods**, and the code lives on the pod. So
//! [`job::build`] sets no TTL at creation time, and this reconciler patches
//! one on only after the status write has returned 200.
//! `ttl_is_patched_only_after_status` asserts both halves — the index order,
//! and that a status patch answered 500 produces zero Job patches.
//!
//! # The case nothing in the corpus handled
//!
//! A Job that finishes with **no terminated state for `runner` at all** — the
//! node was lost, the pod never scheduled, the pod was garbage-collected.
//! There is no exit code to read and there never will be, so the reconciler
//! writes a TERMINAL status naming what it observed and stops. It never leaves
//! the CR in `Running` forever, and it never invents a code: a fabricated `1`
//! would be indistinguishable from a real operational failure, and a
//! fabricated `0` would turn a lost run into a green badge.
//!
//! # What this module deletes: nothing
//!
//! Not the Job, not the pod, and not an orphaned scorecard. Global Constraint
//! 6 gives no Logweir component a delete capability over the archive in tag 1,
//! and a failed pod is the only place its exit code exists.
//!
//! # What this module does NOT do
//!
//! **It performs no signature verification.** Task 24 does, through
//! `logweir-verify` (Global Constraint 27, as narrowed: this crate links the
//! verifying half and never the signer). This reconciler records the two
//! evidence KEYS and leaves `status.evidence.verification` untouched.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use futures::StreamExt as _;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ConfigMap, Pod};
use kube::api::{Api, LogParams, Patch, PatchParams, PostParams};
use kube::runtime::controller::Action;
use kube::runtime::reflector::{self, ObjectRef};
use kube::runtime::{watcher, Controller};
use kube::{Resource, ResourceExt as _};
use serde_json::{json, Value};
use tracing::{debug, info, warn};

use super::Context;
use crate::backup_execution::{
    self, annotate_runner_job, compatible_backup_job, derived_runner_argv, execution_identity,
    has_exact_backup_owner, inputs_config_map, job_provenance, resolve_inputs,
    runner_argv_annotation, verify_frozen_config_map, ExecutionRefusal, FrozenInputs,
    JobProvenance, ResolvedSelection, RunnerArgvAnnotation, INPUTS_SHA256_ANNOTATION,
    RUNNER_ARGV_ANNOTATION,
};
use crate::check;
use crate::conditions::{
    current_condition, merge_condition, reason_for_exit, wire_reason_for_exit, StatusVersion,
    CONDITION_COMPLETE, CONDITION_EVIDENCE_RECORDED, CONDITION_EXECUTION_INPUTS_UNVERIFIED,
    CONDITION_FAILED, CONDITION_JOB_CREATED, CONDITION_REASON_GUARD_REFUSED,
    CONDITION_RUNNER_ARGV_ANNOTATION_IGNORED, CONDITION_RUNNER_READY, CONDITION_TOPICS_RESOLVED,
    CONDITION_VERIFIED, PHASE_FAILED, PHASE_RUNNING, PHASE_SUCCEEDED,
    REASON_EVIDENCE_KEYS_RECORDED, REASON_EVIDENCE_KEYS_UNREADABLE, REASON_JOB_INPUTS_MISMATCH,
    REASON_LEGACY_EXECUTION, REASON_OPERATIONAL, REASON_RUNNER_ARGV_ANNOTATION_IGNORED,
    REASON_RUNNER_ARGV_ANNOTATION_MALFORMED, TERMINAL_STATE_DISRUPTED_MID_DRILL,
    TERMINAL_STATE_GUARD_REFUSED_UNKNOWN_REASON, TERMINAL_STATE_INVALID_TOPIC_SELECTION,
    TERMINAL_STATE_JOB_NAME_CONFLICT, TERMINAL_STATE_NAME_TOO_LONG, TERMINAL_STATE_NO_EXIT_CODE,
    TERMINAL_STATE_ORPHANED_SCORECARD, TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT,
    TERMINAL_STATE_POD_OWNERSHIP_CONTESTED, TERMINAL_STATE_POD_UNSCHEDULABLE,
    TERMINAL_STATE_REFERENT_NOT_FOUND, TERMINAL_STATE_SCHEDULE_NOT_FOUND,
};
use crate::controllers::backup_selection;
use crate::crds::backup::{Backup, FrozenDestination};
use crate::crds::backup_schedule::BackupSchedule;
use crate::crds::kafka_cluster::KafkaCluster;
use crate::crds::Condition;
use crate::destination::{
    self, DestinationRefusal, DestinationRole, ResolveError, ResolvedDestination,
    ResolvedDestinationSnapshot,
};
use crate::diagnostics;
use crate::job::{self, EnvFromSecret, RunnerJobSpec, RunnerOwner, SecretMount, CONTAINER_NAME};
use crate::policy::SelectionShape;
use crate::verification::{
    backup_badge, conditions_in, second_patch, stored_verification, verified_condition,
    EvidenceRef, VerificationVerdict, VerifyOracle,
};
use logweir_core::check_contract::CheckCode;
use logweir_core::ids::sha256_prefixed;
use logweir_store::Store;

/// `ttlSecondsAfterFinished`, **patched on after the status write** and never
/// set at creation time.
///
/// SEVEN DAYS. The TTL controller deletes the Job and its pods together, and
/// the exit code lives only on the pod, so this value is how long an operator
/// has to go and look at the thing the status already recorded. A shorter
/// value buys nothing — the status is already written by the time this is
/// patched — and a longer one accumulates finished Jobs in a namespace whose
/// quota is somebody else's.
pub const TTL_SECONDS_AFTER_FINISHED: i32 = 604_800;

/// The current label the job controller puts on every pod it creates.
///
/// `batch.kubernetes.io/job-name` is the 1.27+ spelling and the one to prefer.
pub const JOB_NAME_LABEL: &str = "batch.kubernetes.io/job-name";

/// The legacy, unprefixed label — still set on 1.29, and the fallback.
///
/// BOTH ARE SET ON 1.29 AND ONLY THE PREFIXED ONE IS CURRENT, so the
/// reconciler tries the prefixed selector first and falls back to this one
/// when it returns nothing. A controller that used only the legacy label would
/// break on the release that finally drops it; one that used only the current
/// label would break on a cluster at the floor that has not backfilled it.
/// Two selectors, tried in that order, is the only shape that works across
/// both — and critique B M10 asked for a stated rule rather than an
/// improvisation at the call site.
pub const JOB_NAME_LABEL_LEGACY: &str = "job-name";

/// The ServiceAccount the runner pod names.
///
/// NAMED BUT NOT TOKEN-MOUNTED. `job::build` sets
/// `automountServiceAccountToken: false`, because a runner pod makes ZERO
/// Kubernetes API calls and the key-holder is deliberately the component with
/// no cluster credential. The NAME still matters: PodSecurity admission,
/// image-pull secrets and Task 21's RBAC all attach to it, and a pod with no
/// ServiceAccount named silently gets `default`, which is the one account an
/// operator is most likely to have granted something to.
pub const RUNNER_SERVICE_ACCOUNT: &str = "logweir-runner";

/// The Secret holding the receipt signing key.
pub const SIGNING_KEY_SECRET: &str = "logweir-signing-key";
/// The key within [`SIGNING_KEY_SECRET`].
pub const SIGNING_KEY_SECRET_KEY: &str = "signing.pem";
/// The volume name the signing key is projected as.
pub const SIGNING_VOLUME: &str = "signing";
/// Where [`SIGNING_VOLUME`] is mounted — the directory half of
/// `controllers::backup_schedule::SIGNING_KEY_PATH`.
pub const SIGNING_MOUNT_PATH: &str = "/signing";
/// The file name the signing key is projected under, so the mounted path is
/// exactly `controllers::backup_schedule::SIGNING_KEY_PATH`.
pub const SIGNING_KEY_FILE: &str = "key.pem";

/// The environment variable names the object-store credential reaches the
/// runner under.
///
/// `object_store`'s OWN credential chain, not the AWS SDK's: `~/.aws`,
/// `AWS_PROFILE` and SSO are unsupported (`examples/cronjob-drill.yaml:120`).
pub const ARCHIVE_ACCESS_KEY_ENV: &str = "AWS_ACCESS_KEY_ID";
/// See [`ARCHIVE_ACCESS_KEY_ENV`].
pub const ARCHIVE_SECRET_KEY_ENV: &str = "AWS_SECRET_ACCESS_KEY";
/// The key within `Backup.spec.archive.secretRef` holding the access key id.
pub const ARCHIVE_ACCESS_KEY: &str = "access-key-id";
/// The key within `Backup.spec.archive.secretRef` holding the secret key.
pub const ARCHIVE_SECRET_KEY: &str = "secret-access-key";

/// The object-store ADDRESSING variables the controller forwards to every
/// runner Job it creates — Task 24.
///
/// # Why the controller forwards its own environment
///
/// Critique B **H20**: *nothing in the design says where the object store is.*
/// A `Backup` names its archive as a URL (`spec.archive.url`) and a credential
/// (`spec.archive.secretRef`), and neither can carry an ENDPOINT — which is
/// everything an adopter on MinIO, Ceph, or any S3-compatible store needs,
/// and exactly what the Phase B demo needs to reach the compose stack's MinIO
/// at `http://host.docker.internal:9000`.
///
/// These four are `object_store`'s OWN variable names, not Logweir knobs, and
/// two of them ALREADY reach the runner by another route:
/// [`crate::retention::storage_url_for`] reads `AWS_ALLOW_HTTP` and
/// `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` out of the CONTROLLER's environment when
/// it renders `plan_backup_spec`'s `storage` block. Forwarding all four makes
/// one configuration point instead of two halves that can disagree — an
/// adopter points the controller at their store and the runners follow.
///
/// **A VARIABLE THAT IS NOT SET IS NOT FORWARDED.** The default install sets
/// none of them, so a default runner Job's env is exactly what it was before
/// this task: `RUST_LOG` plus `job::build`'s own three. Nothing about the
/// shipped `logweir.yaml` changes, which is what keeps it applyable by a
/// stranger (Global Constraint 37); the demo's own kustomize patch is what
/// sets them.
pub const ARCHIVE_ADDRESSING_ENV: [&str; 4] = [
    "AWS_ENDPOINT_URL",
    "AWS_REGION",
    "AWS_ALLOW_HTTP",
    "AWS_VIRTUAL_HOSTED_STYLE_REQUEST",
];

/// [`ARCHIVE_ADDRESSING_ENV`]'s variables that are actually set on this
/// process, in declaration order, as `env_literal` pairs.
///
/// NEVER A CREDENTIAL. `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are
/// deliberately not in the list: the runner's archive credential comes from
/// the `Backup`'s own `spec.archive.secretRef` through `env_from_secret`, and
/// the controller's is a DIFFERENT PRINCIPAL (`logweir-evidence-ro`, read-only,
/// spec §9) which must never be handed to a pod that writes.
#[must_use]
pub fn archive_addressing_env() -> Vec<(String, String)> {
    ARCHIVE_ADDRESSING_ENV
        .iter()
        .filter_map(|name| {
            std::env::var(name)
                .ok()
                .filter(|v| !v.is_empty())
                .map(|v| ((*name).to_string(), v))
        })
        .collect()
}

/// `receipt-key=` — interface **I7**'s first line.
pub const RECEIPT_KEY_PREFIX: &str = "receipt-key=";
/// `sidecar-key=` — interface **I7**'s second line.
pub const SIDECAR_KEY_PREFIX: &str = "sidecar-key=";
/// `refusal-reason=` — Global Constraint 11's final line for exit 3.
pub const REFUSAL_REASON_PREFIX: &str = "refusal-reason=";

/// How many trailing lines of the pod log the key scan looks at.
///
/// THE CONTRACT SAYS "THE FINAL TWO STDOUT LINES", AND THIS IS SIXTEEN. The
/// extra reads cost nothing and buy tolerance for a trailing blank line, a
/// `\r\n`, or a shutdown line a future runner appends — none of which changes
/// which line carries which key, because the scan matches on the KEY NAME and
/// not on a position. It is bounded rather than unbounded so a 200 MB log
/// cannot make the scan the expensive part of a reconcile.
///
/// # Why it was eight and is now sixteen — D3 W5's review finding F5
///
/// Execution contract v2 added a conditional `teardown-key=` line to the
/// restore runner's tail. Measured on a PASSING restore
/// (`crates/logweir/tests/progress_channel.rs`, child row
/// `the_progress_child_runs_one_restore_and_exits`): summary,
/// `topic-preflight=`, `teardown-key=`, `scorecard-key=`, `sidecar-key=`,
/// `offset-report-key=` — six lines, **seven** in production where `tracing`
/// also emits `drill finished`. That is seven of eight, and the next worker to
/// append one trailing line would push `topic-preflight=` — erratum E10(c)'s
/// only producer for `Restore.status.topicPreflight` — out of the window. The
/// scan matches by key NAME, so the failure mode is a silently absent status
/// field and not an error, which is exactly the kind of defect a budget
/// exists to prevent.
///
/// So the constant is RAISED rather than the runner trimmed, and the budget is
/// written down: **7 used, 9 reserved**. A larger window is safe by
/// construction — every scanner here takes the LAST occurrence of each key
/// prefix, so widening it can only find a key it would otherwise have missed,
/// never a different one. [`BUDGETED_TRAILING_LINES`] is the measured seven,
/// and `the_key_scan_window_has_room_for_the_runners_trailing_block` in
/// `tests/backup_controller.rs` is what fails when the two disagree — mirrored
/// from the runner-side row, which is where a runner-side change is caught.
pub const KEY_SCAN_TAIL_LINES: usize = 16;

/// How many trailing lines a passing restore actually prints in production —
/// the number [`KEY_SCAN_TAIL_LINES`] is a budget over.
///
/// MIRRORED from `crates/logweir/tests/progress_channel.rs`'s
/// `the_trailing_lines_a_passing_restore_prints_fit_the_controllers_scan_window`,
/// which measures it by running the real runner. A `weirkeeper` dependency in
/// that crate would invert the layering, so the number is stated on both sides
/// and each side's test names the other. Raise this when a runner appends a
/// trailing line; the controller-side row then says whether the window still
/// has room.
pub const BUDGETED_TRAILING_LINES: usize = 7;

/// How long before an unfinished Job is looked at again.
///
/// FIFTEEN SECONDS. A Job's own events wake this controller, so the requeue
/// exists for the one transition no watch delivers: a pod that terminated
/// while the controller was down. Half `backup_schedule`'s interval, because a
/// run in flight is the state an operator is actually watching.
pub const REQUEUE_SECS: u64 = 15;

/// The two label selectors used to find the pod, in the order they are tried.
///
/// Pure, and returned as an array rather than built at the call site, so
/// `the_exit_code_and_the_keys_come_from_the_logs_subresource` can assert the
/// order and the spelling without a client.
#[must_use]
pub fn pod_selectors(job_name: &str) -> [String; 2] {
    [
        format!("{JOB_NAME_LABEL}={job_name}"),
        format!("{JOB_NAME_LABEL_LEGACY}={job_name}"),
    ]
}

/// The plan ConfigMap a `Backup`'s runner reads its rendered `backup.yaml`
/// and cluster allowlist from.
///
/// **THIS TASK RENDERS IT** — errata **E5a**, review finding HIGH-1. Task 18's
/// runner argv points at `/plan/backup.yaml` and
/// `/plan/allowed-clusters.json`, so the mount has to exist; `Backup.spec`
/// carries no `planBytes` (unlike `Restore`, whose controller writes the bytes
/// verbatim into a ConfigMap of its own) and no task's Files block owned the
/// renderer. Measured live before the fix: the pod stalled in
/// `ContainerCreating` on `MountVolume.SetUp failed for volume "plan":
/// configmap "<name>-plan" not found`, the `activeDeadlineSeconds` fired, the
/// job controller deleted the pod, and the reconciler wrote a terminal
/// `NoExitCode` — every scheduled backup in tag 1 failing as an unexplained
/// "NoExitCode".
///
/// See [`plan_config_map`] for the object and [`plan_backup_spec`] for the
/// document.
#[must_use]
pub fn plan_config_map_name(backup_name: &str) -> String {
    format!("{backup_name}-plan")
}

/// The key in the plan ConfigMap carrying the rendered `BackupSpec`, and the
/// file name Task 18's argv points `--spec` at.
pub const PLAN_SPEC_KEY: &str = "backup.yaml";
/// The key carrying the cluster allowlist, and the file name Task 18's argv
/// points `--allowed-clusters` at.
pub const PLAN_ALLOWED_CLUSTERS_KEY: &str = "allowed-clusters.json";

/// The `backup_id` a run of this `Backup` reports.
///
/// `status.execution.id` once the controller has frozen this object's inputs —
/// the identity the runner was actually handed. Before that, and for a Job an
/// older controller created, it is
/// [`backup_execution::legacy_backup_id`]: the first controller owner's UID
/// plus `spec.slot` (a scheduled `Backup`'s owner IS its schedule, so this is
/// `slot::backup_id_for(<schedule uid>, <slot>)`), else this object's own UID.
/// For every `Backup` [`execution_identity`] accepts, the two values are equal.
///
/// NOT FROM `metadata.name`, which the archive prefix is not, and never from an
/// annotation.
#[must_use]
pub fn plan_backup_id(backup: &Backup) -> String {
    backup
        .status
        .as_ref()
        .and_then(|status| status.execution.as_ref())
        .map_or_else(
            || backup_execution::legacy_backup_id(backup),
            |execution| execution.id.clone(),
        )
}

/// A terminal refusal from the execution contract, as this reconciler's error.
fn refused(refusal: ExecutionRefusal) -> BackupError {
    BackupError::Refused(refusal.state, refusal.message)
}

/// The inputs a run of `backup` against `cluster` would freeze NOW: the derived
/// identity, resolved from the typed spec, the referent and this controller's
/// forwarded archive addressing, in canonical form.
///
/// PURE apart from the environment read of the addressing variables, which is
/// the same read [`archive_addressing_env`] and
/// [`crate::retention::storage_url_for`] have always made.
///
/// # Errors
///
/// [`BackupError::Refused`] with the terminal state
/// [`backup_execution::resolve_inputs`] or [`execution_identity`] names.
pub fn desired_execution_inputs(
    backup: &Backup,
    cluster: &KafkaCluster,
) -> Result<FrozenInputs, BackupError> {
    desired_execution_inputs_for(backup, cluster, &resolved_selection(backup)?)
}

/// [`desired_execution_inputs`] for a selection that has ALREADY been resolved.
///
/// # THE ENTRY POINT D1 W5 (PLAT-09.2) CALLS
///
/// The dynamic path resolves its topic list from a discovery Job — an async,
/// multi-pass affair that cannot live inside a pure resolver — and then freezes
/// through exactly this function, so a dynamic run and a named one share one
/// canonical encoding, one `status.execution` write and one
/// [`verify_frozen_config_map`] comparison. See
/// [`backup_execution::ResolvedSelection`].
///
/// # Errors
///
/// [`BackupError::Refused`] with the terminal state
/// [`backup_execution::resolve_inputs`] or [`execution_identity`] names.
pub fn desired_execution_inputs_for(
    backup: &Backup,
    cluster: &KafkaCluster,
    selection: &ResolvedSelection,
) -> Result<FrozenInputs, BackupError> {
    desired_execution_inputs_for_destination(backup, cluster, selection, None)
}

/// [`desired_execution_inputs_for`] for a run whose location comes from a
/// saved `BackupDestination` — D2 §3.5, §3.7, seam **S4**.
///
/// # THE TWO PATHS DIFFER IN WHAT THE CONTROLLER'S OWN PROCESS CONTRIBUTES
///
/// `None` is the legacy inline-`archive` run, byte for byte as it has always
/// been: `storage` from [`crate::retention::storage_url_for`] (which reads
/// `AWS_ALLOW_HTTP` and `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` out of this
/// process) and [`archive_addressing_env`]'s four forwarded variables frozen
/// into the snapshot. That is defect **SEC-ENVHTTP**, and it stays on the
/// legacy path deliberately: those four variables are how every existing
/// install points its runners at a non-AWS store, and removing them would
/// break every upgrade at the moment of the upgrade. What closes the defect
/// there is moving OFF the legacy path — `destinations:from-legacy` (D2 §3.12)
/// — and until an operator does, `docs/kubernetes.md` §20 names the
/// forwarding as the reason a controller-level `AWS_ALLOW_HTTP=true` is a
/// cluster-wide setting.
///
/// `Some(destination)` is the closed path: `storage` from the destination's own
/// location, NO forwarded addressing in the snapshot at all, and the complete
/// explicit `AWS_*` set rendered into the Job from
/// [`ResolvedDestination::job_env`] — which reads no environment variable of
/// any kind. A planted `AWS_ENDPOINT_URL` in the controller's process cannot
/// reach such a Job, and `no_controller_environment_reaches_a_destination_backed_job`
/// is the guard.
///
/// # Errors
///
/// [`BackupError::Refused`] with the terminal state
/// [`backup_execution::resolve_inputs`] or [`execution_identity`] names, or
/// [`TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT`] when the CA bytes are not the
/// ones the resolved destination's digest names.
pub fn desired_execution_inputs_for_destination(
    backup: &Backup,
    cluster: &KafkaCluster,
    selection: &ResolvedSelection,
    destination: Option<&BackupDestinations>,
) -> Result<FrozenInputs, BackupError> {
    // THE BYTES TRAVEL WITH THE SNAPSHOT, NOT INSIDE IT (D2 §3.7). `ca_pem` is
    // read here, from the resolution the reconciler already made, so the plan
    // `ConfigMap` and the digest in the frozen document are written in one act
    // and cannot disagree.
    //
    // NOT `from_utf8_lossy`. A `ConfigMap`'s `data` values are strings, so
    // bytes that are not UTF-8 would be replacement-charactered on the way in
    // and the plan would then digest to something the snapshot does not name —
    // a `PlanConfigMapConflict` on every later pass, with a message about a
    // digest rather than about the bundle. `check_ca_bundle` has already
    // refused anything that is not a PEM bundle; this is the second rail, and
    // it names the real problem.
    let ca_pem = match destination.and_then(|d| d.archive.ca_pem.as_ref()) {
        None => None,
        Some(bytes) => Some(String::from_utf8(bytes.clone()).map_err(|_| {
            BackupError::Refused(
                TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT,
                format!(
                    "the CA bundle of the BackupDestination this Backup names is not UTF-8, so \
                     it cannot be written into the immutable plan ConfigMap a runner mounts \
                     ({} bytes)",
                    bytes.len()
                ),
            )
        })?),
    };
    desired_execution_inputs_frozen(
        backup,
        cluster,
        selection,
        destination.map(BackupDestinations::snapshot).as_ref(),
        ca_pem,
    )
}

/// [`desired_execution_inputs_for_destination`] for a run whose destination has
/// ALREADY been frozen — the readback path.
///
/// # Why a frozen run never re-resolves its destination
///
/// Every edit to a `BackupDestination` bumps `metadata.generation`, and the
/// frozen block records the generation it resolved, so `verify_frozen_config_map`
/// compares it. A pass that re-creates a garbage-collected Job by RESOLVING the
/// live object would therefore terminate a running backup because somebody
/// rotated a Secret name while it ran — fail-closed, and still exactly the
/// thing D2 §3.7 says cannot happen ("later destination edits cannot affect a
/// created run"). `backup_execution::stored_destination` reads the snapshot
/// back out of the immutable plan, and this is what renders from it.
///
/// `ca_pem` is the `archive-ca.pem` key of that same plan, so the digest bind
/// in `FrozenInputs::freeze_with_ca` is evaluated against the bytes a kubelet
/// would actually mount.
///
/// # Errors
///
/// Whatever [`desired_execution_inputs_for_destination`] refuses.
pub fn desired_execution_inputs_frozen(
    backup: &Backup,
    cluster: &KafkaCluster,
    selection: &ResolvedSelection,
    destination: Option<&crate::destination::ResolvedDestinationSnapshot>,
    ca_pem: Option<String>,
) -> Result<FrozenInputs, BackupError> {
    let identity = execution_identity(backup).map_err(refused)?;
    let inputs = resolve_inputs(
        backup,
        identity,
        cluster,
        &archive_addressing_env(),
        selection,
        destination,
    )
    .map_err(refused)?;
    FrozenInputs::freeze_with_ca(inputs, ca_pem).map_err(refused)
}

/// Which of D1 §7.1's two selection shapes this `Backup` declares — the check
/// that runs before ANY read and before any `POST`.
///
/// # Errors
///
/// [`TERMINAL_STATE_INVALID_TOPIC_SELECTION`], naming every field-level reason,
/// for a spec that declares neither shape. TERMINAL, because `Backup.spec` is
/// CEL-immutable and a requeue over a shape that cannot be edited would never
/// succeed.
pub fn declared_selection_shape(backup: &Backup) -> Result<SelectionShape, BackupError> {
    crate::policy::validate_topic_selection(&backup.spec).map_err(|errors| {
        let detail = errors
            .iter()
            .map(|e| format!("{}: {}", e.field, e.message))
            .collect::<Vec<_>>()
            .join("; ");
        BackupError::Refused(
            TERMINAL_STATE_INVALID_TOPIC_SELECTION,
            format!(
                "spec.topics and spec.allUserTopics do not form one of the two selection shapes \
                 D1 §7.1 admits (a non-empty named allowlist with no allUserTopics, or \
                 `topics: []` with one): {detail}"
            ),
        )
    })
}

/// The topic list this run freezes, when it can be decided WITHOUT reading the
/// cluster.
///
/// # The SYNCHRONOUS half of the selection
///
/// The `SelectedTopics` arm is complete and final: the named allowlist,
/// verbatim, coverage `NamedTopics`. The `AllUserTopics` arm cannot be answered
/// here at all — it is a topic discovery Job, several reconcile passes and an
/// installation policy (D1 §7.2, [`backup_selection::resolve`]) — so this pure
/// entry point says so rather than guessing. What it must never do is fall
/// through to the freeze with the empty `spec.topics` a dynamic object carries:
/// an empty allowlist rendered into `backup.yaml` is the "no allowlist means
/// everything" shape guard **G-GLOB** exists to prevent.
///
/// [`desired_execution_inputs`] is the only caller, and it exists for tests and
/// for callers that hold a `Backup` and a `KafkaCluster` and no client.
///
/// [`backup_selection::resolve`]: crate::controllers::backup_selection::resolve
///
/// # Errors
///
/// [`TERMINAL_STATE_INVALID_TOPIC_SELECTION`] for a spec that declares neither
/// shape, and for a dynamic selection, which only the asynchronous resolver can
/// answer.
pub fn resolved_selection(backup: &Backup) -> Result<ResolvedSelection, BackupError> {
    match declared_selection_shape(backup)? {
        SelectionShape::SelectedTopics => Ok(ResolvedSelection::named(&backup.spec)),
        SelectionShape::AllUserTopics => Err(BackupError::Refused(
            TERMINAL_STATE_INVALID_TOPIC_SELECTION,
            "spec.allUserTopics asks this run to cover every user topic its principal can see, \
             which is resolved by a per-run topic discovery Job (PLAT-09.2) and not by a pure \
             function of the spec. This entry point resolves a named allowlist only; the \
             reconciler resolves the dynamic shape through \
             `controllers::backup_selection::resolve`"
                .to_string(),
        )),
    }
}

/// The typed `BackupSpec` the runner's `--spec` file carries, rendered from
/// the resolved execution inputs.
///
/// **TYPED, AND THAT IS THE POINT.** The document is built as
/// `logweir_core::spec::BackupSpec` and serialised, never assembled as text:
/// `storage` is an internally tagged enum whose variants have incompatible
/// REQUIRED fields, so a stringly-typed renderer produces `backend:
/// filesystem` beside a `bucket:` key and the engine's config load fails with
/// a hard "missing field" the unknown-key readback cannot catch (the argument
/// `logweir_core::engine::StorageUrl`'s own doc comment makes). Rendering
/// through the type the CLI parses is also what lets
/// `the_rendered_plan_parses_back_as_a_backup_spec` read the ConfigMap body
/// back with `serde_yaml::from_str::<BackupSpec>` and compare field by field.
///
/// # What each field comes from
///
/// * `source.bootstrap_servers`, `source.auth` — the `KafkaCluster`
///   `spec.sourceRef` names. **Never from `Backup.spec`**, which carries
///   neither.
/// * `source.topics` — `Backup.spec.topics` **verbatim**, behind
///   [`logweir_core::guard::reject_glob_metacharacters`] in
///   [`reconcile_backup`] step 0.
/// * `storage` — `Backup.spec.archive.url`, through
///   [`crate::retention::storage_url_for`], the same parser the controller's
///   own read-only handle is built with.
/// * `backup_id` — the server-derived run identity
///   ([`execution_identity`]).
/// * `backup` — `BackupSettings::default()`. `Backup.spec` has no tunables.
///
/// The document is a projection of [`FrozenInputs`]; see
/// [`FrozenInputs::backup_spec`].
///
/// # Errors
///
/// [`BackupError::Refused`] for a spec that states no runnable identity, an
/// unreadable `archive.url`, or a source connection
/// [`crate::connection::resolve`] refuses (PLAT-07.1) — a `scramSha512`
/// cluster with no username or `secretRef` among them.
pub fn plan_backup_spec(
    backup: &Backup,
    cluster: &KafkaCluster,
) -> Result<logweir_core::spec::BackupSpec, BackupError> {
    Ok(desired_execution_inputs(backup, cluster)?.backup_spec())
}

/// The cluster allowlist the runner's `--allowed-clusters` file carries.
///
/// **THE SHAPE IS `logweir_core::spec::AllowedClusters`, WHICH IS WHAT THE
/// READER PARSES** — `crates/logweir/src/backup/mod.rs:263-267` reads the file
/// and `serde_json::from_str::<AllowedClusters>`s it, so the format is the type
/// and not a convention.
///
/// # `allowed_cluster_ids` IS EMPTY, AND THE CLUSTER ID GOES IN
/// `source_cluster_id`
///
/// READ `crates/logweir/src/backup/phase_minus1_admit.rs:158-190` BEFORE
/// CHANGING THIS. On the BACKUP path `allowed_cluster_ids` is the restore-
/// TARGET allowlist — the set of scratch clusters a drill may restore INTO —
/// and Global Constraint 18(c)'s fourth rail **REFUSES** a run whose observed
/// source cluster id appears in it, because a cluster cannot be both the
/// source of an archive and a cluster whose topics a drill deletes. So putting
/// the source cluster's id in that list would make every backup exit 3.
/// `source_cluster_id` is the field that carries it; the backup path does not
/// read that field at all (phase −1's comment says so in as many words —
/// it is the drill's, for refusing a TARGET equal to the source).
///
/// # And that is why the allowlist can stay a ConfigMap key here
///
/// Errata **E5a**: on this path the allowlist is a **consistency rail, not a
/// boundary**. The bootstrap address the run dials comes from the
/// CEL-immutable `sourceRef`, not from this file, so a subject with `patch
/// configmaps` who replaces it can only ADD ids to `allowed_cluster_ids` — and
/// every id they could add makes the backup REFUSE, never widen. Task 22 moved
/// the drill path's allowlist into the `logweir-approval-bundle` Secret
/// because there it authorises a restore TARGET, which is the opposite
/// direction. `docs/kubernetes.md` §10 carries this reason in prose.
///
/// [`FrozenInputs::allowed_clusters`] renders the same document from frozen
/// inputs; for inputs resolved from `cluster` the two are equal.
#[must_use]
pub fn plan_allowed_clusters(cluster: &KafkaCluster) -> logweir_core::spec::AllowedClusters {
    logweir_core::spec::AllowedClusters {
        allowed_cluster_ids: Vec::new(),
        source_cluster_id: cluster
            .status
            .as_ref()
            .and_then(|s| s.cluster_id.clone())
            .filter(|id| !id.is_empty()),
    }
}

/// The plan ConfigMap object: create-only, `immutable: true`, owned by exactly
/// this `Backup`, carrying the canonical input snapshot
/// ([`backup_execution::INPUTS_KEY`]) and the two runner documents rendered
/// from it, and annotated with the run identity and the snapshot digest.
///
/// PURE, so the object a test builds is byte-identical to the one the
/// reconciler `POST`s.
///
/// `ownerReferences` WITH `controller: true` AND `blockOwnerDeletion: true`:
/// deleting the `Backup` garbage-collects its plan, and a half-deleted
/// `Backup` cannot orphan a ConfigMap naming its source cluster. It is also
/// what makes the 409 case decidable — see
/// [`TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT`].
///
/// NO CREDENTIAL IN ANY KEY, AND NO SECRET NAME. `AuthSpec` carries a username
/// and never a password at any variant; the SCRAM password and the
/// object-store credential reach the runner as `secretKeyRef` env built from
/// the pinned `KafkaCluster` and the immutable `Backup.spec`, so a ConfigMap —
/// an object with no encryption at rest and a much wider read surface than a
/// Secret — carries nothing but the snapshot and the two documents.
///
/// # Errors
///
/// Whatever [`desired_execution_inputs`] refuses.
pub fn plan_config_map(backup: &Backup, cluster: &KafkaCluster) -> Result<ConfigMap, BackupError> {
    inputs_config_map(backup, &desired_execution_inputs(backup, cluster)?).map_err(refused)
}

// ---------------------------------------------------------------------------
// Destination admission — D2 §3.6
// ---------------------------------------------------------------------------

/// Whether the pinned engine has been PROVED to honour a destination's custom
/// CA bundle — D2 §3.5's `[UNVERIFIED — U1: whether the engine honours a custom CA file]`.
///
/// # `false`, AND IT STAYS `false` UNTIL SOMEBODY MEASURES IT
///
/// The claim is that the engine honours `SSL_CERT_FILE` through
/// rustls-platform-verifier on Linux. It is plausible, it is undemonstrated,
/// and the failure mode if it is wrong is a Backup that dials a private-CA
/// endpoint, fails its TLS handshake inside the engine child, and reports an
/// opaque operational error with no mention of certificates. So a destination
/// declaring `spec.transport.caBundle` is REFUSED for Backup and Restore
/// admission with [`CheckCode::CaBundleUnsupportedByEngine`] — while
/// Logweir-only paths (checks, verification, the evidence handle) support the
/// CA today, because those build their own rustls client and no engine child
/// is involved.
///
/// It is a COMPILED CONSTANT and not a field: flipping it is a claim about a
/// recorded engine digest, which is a code change somebody reviews, not a
/// cluster setting somebody flips. The administrator-governed escape hatch is
/// the policy key `engine.allowUnverifiedCustomCa`, default `false`, which
/// D2 §14 sets to run scenario S2b; the constant flips only after S2b passes,
/// and the key returns to `false`.
pub const ENGINE_CUSTOM_CA_VERIFIED: bool = false;

/// Whether a destination's CA bundle may be handed to an ENGINE-driven run —
/// [`ENGINE_CUSTOM_CA_VERIFIED`] or the administrator's opt-in.
///
/// The two are OR-ed and not AND-ed: the constant is "we proved it", the policy
/// key is "an administrator accepts the risk for this installation". Either is
/// sufficient; neither is implied by the other.
#[must_use]
pub fn engine_custom_ca_allowed(policy: &crate::check::policy::Policy) -> bool {
    ENGINE_CUSTOM_CA_VERIFIED || policy.engine.allow_unverified_custom_ca
}

/// The refusal message an engine-driven run gets for an unverified custom CA.
#[must_use]
pub fn engine_custom_ca_refusal(namespace: &str, name: &str) -> String {
    format!(
        "BackupDestination {namespace}/{name} declares spec.transport.caBundle, and whether the \
         pinned engine honours a custom CA has not been measured on a recorded engine digest \
         (D2 §3.5 U1). A run whose TLS handshake fails inside the engine child reports an opaque \
         operational error with no mention of certificates, so it is refused here instead. \
         Checks, verification and the controller's own evidence reads DO support this CA. To \
         accept the risk for this installation, an administrator sets \
         engine.allowUnverifiedCustomCa in the installation policy ConfigMap"
    )
}

/// The longest a `Backup` waits for its `BackupDestination` to exist and be
/// `Valid` before the wait becomes the answer — D2 §3.6.
///
/// # Why a HOLD at all, and why it ends
///
/// A `destinationRef` and the `BackupDestination` it names are created by two
/// different `kubectl apply`s, often in one directory, in whatever order the
/// server happens to take them. Refusing terminally on the first pass would
/// make a correct manifest set fail on a race the operator cannot influence —
/// and a `Backup.spec` is CEL-immutable, so that refusal could never be
/// repaired in place.
///
/// It ends because a HOLD that never ends is a `Backup` that sits at `Pending`
/// for the rest of the cluster's life with nothing to observe it. Ten minutes
/// is long enough for a second `apply` and short enough that a scheduled run's
/// next slot has not yet overtaken it.
pub const DESTINATION_HOLD_MAX_SECONDS: i64 = 600;

/// How long this `Backup` may hold: `min(spec.deadlineSeconds, 600)`.
///
/// **THE RUN'S OWN DEADLINE CAPS IT.** A `Backup` whose runner is allowed sixty
/// seconds should not spend ten minutes waiting to start one; the operator
/// already said how long this run is worth.
#[must_use]
pub fn destination_hold_budget(backup: &Backup) -> i64 {
    backup
        .spec
        .deadline_seconds
        .clamp(0, DESTINATION_HOLD_MAX_SECONDS)
}

/// Whether this `Backup` has held for its whole budget — D2 §3.6 step 1.
///
/// Measured from `metadata.creationTimestamp`, which is the API server's own
/// clock reading and the one timestamp a `Backup` carries that no controller
/// wrote. A `Backup` with no creation timestamp (impossible from an API server,
/// reachable from a hand-built fixture) has not expired: inventing an expiry
/// from a missing timestamp would fail runs for want of a field.
#[must_use]
pub fn destination_hold_expired(backup: &Backup, now: DateTime<Utc>) -> bool {
    let Some(created) = backup.meta().creation_timestamp.as_ref() else {
        return false;
    };
    (now - created.0).num_seconds() >= destination_hold_budget(backup)
}

/// The ONE `BackupDestination` a `Backup` names, resolved for BOTH roles a
/// backup run needs.
///
/// # A BACKUP WRITES TWO THINGS, AND THEY MAY BE TWO PRINCIPALS
///
/// The archive, through `archiveWrite`; and its signed receipt, through
/// `evidenceWrite` over Global Constraint 6's `logweir/` root of the same
/// bucket. `evidenceWrite` falls back to `archiveWrite` (D2 §3.4), so on the
/// common destination these are the same grant and the Job carries one
/// credential plus `LOGWEIR_EVIDENCE_CREDENTIALS=archive`. An operator who
/// separates the principals gets two.
///
/// **THE RUNNER REFUSES A RUN THAT DOES NOT SAY WHICH.** Before this struct
/// existed the Backup path resolved only `archiveWrite` and rendered no
/// evidence variable at all, and every destination-backed run exited 3 at the
/// store builder with `LOGWEIR_EVIDENCE_CREDENTIALS is \`\``, after the archive
/// handles were built and before a byte was archived — the erratum **E20**
/// failure class, one layer further in.
#[derive(Debug)]
pub struct BackupDestinations {
    /// Resolved for [`DestinationRole::ArchiveWrite`], with its CA bundle read.
    pub archive: ResolvedDestination,
    /// The same object resolved for [`DestinationRole::EvidenceWrite`]. Only
    /// its `grant` differs from [`Self::archive`], and often not even that.
    pub evidence: ResolvedDestination,
}

impl BackupDestinations {
    /// The block this run freezes — the archive resolution, carrying the
    /// evidence grant when it differs.
    #[must_use]
    pub fn snapshot(&self) -> crate::destination::ResolvedDestinationSnapshot {
        self.archive.snapshot_with_evidence(&self.evidence.grant)
    }
}

/// What destination admission decided for one `Backup`.
#[derive(Debug)]
pub enum DestinationAdmission {
    /// The `Backup` names no `destinationRef`: a legacy inline-`archive` run,
    /// unchanged in every respect.
    NotRequested,
    /// One `BackupDestination`, resolved for both roles with its CA bundle read.
    Resolved(Box<BackupDestinations>),
    /// The destination is absent or not yet `Valid`, and the hold budget has
    /// not run out. NOTHING IS CREATED while this is the answer.
    Holding {
        /// The [`logweir_core::check_contract::CheckCode`] as a condition
        /// `reason`.
        reason: &'static str,
        /// What is wrong and what to do. Names objects and fields, never a
        /// credential — the resolver has none to name.
        message: String,
    },
}

/// Resolve the `BackupDestination` a `Backup` names, for the role a backup run
/// needs — D2 §3.6's "Backup reconcile additions", before the freeze.
///
/// # THE ROLE IS `ArchiveWrite`, AND IT DOES NOT DEFAULT TO ANYTHING
///
/// A backup run WRITES the archive. `archiveRead` and `evidenceWrite` fall back
/// to this grant (D2 §3.4) and never the other way round, so a destination that
/// configures only a read grant is `DestinationRoleNotConfigured` here rather
/// than a run that writes with whatever principal was available.
///
/// # The three outcomes
///
/// * [`DestinationAdmission::NotRequested`] — no `destinationRef`.
/// * [`DestinationAdmission::Holding`] — `DestinationNotFound` or
///   `DestinationNotValid` within the budget ([`destination_hold_budget`]).
/// * `Err(BackupError::Refused)` — every other refusal, and an expired hold.
///   Terminal: a role nobody configured, two ServiceAccounts in one pod or an
///   addressing the pinned engine cannot honour are DECISIONS, and a decision
///   retried forever is a decision nobody sees.
///
/// # Errors
///
/// [`BackupError::Api`] for a failed read, or [`BackupError::Refused`] as
/// above.
pub async fn admit_destination(
    backup: &Backup,
    client: &kube::Client,
    namespace: &str,
    now: DateTime<Utc>,
) -> Result<DestinationAdmission, BackupError> {
    let Some(reference) = backup.spec.destination_ref.as_ref() else {
        return Ok(DestinationAdmission::NotRequested);
    };
    // THE POLICY IS READ ONLY ON THIS PATH, THROUGH ONE PROCESS-WIDE CACHE. A
    // legacy `Backup` performs no extra `get` at all, which is what keeps every
    // route-table double in `tests/backup_controller.rs` unchanged — and what
    // keeps an installation that has never created a `BackupDestination` from
    // depending on a `ConfigMap` in the release namespace to run a backup.
    let load = crate::check::policy::load(
        client,
        super::topic_discovery::configured_policy_ref().as_ref(),
        installation_policy_cache(),
        now,
    )
    .await
    .map_err(BackupError::Api)?;
    // ONE `get` OF THE OBJECT, TWO RESOLUTIONS OF IT, ONE `get` OF ITS CA.
    // `resolve_ref` would do the archive half in one call, and then the
    // evidence half would need a second read of the same object on every pass
    // — so the read is opened out here and `resolve` (pure) is called twice
    // over the bytes it returned. Both resolutions therefore describe the SAME
    // revision of the same object, which a second `get` could not promise.
    let object = match resolve_destination_object(client, namespace, &reference.name).await? {
        Ok(object) => object,
        Err(refusal) => {
            if refusal.is_hold() && !destination_hold_expired(backup, now) {
                return Ok(DestinationAdmission::Holding {
                    reason: refusal.reason(),
                    message: refusal.message,
                });
            }
            return Err(BackupError::Refused(refusal.reason(), refusal.message));
        }
    };
    let archive = match destination::resolve(&object, DestinationRole::ArchiveWrite, load.policy())
    {
        Ok(archive) => archive,
        Err(refusal) => {
            if refusal.is_hold() && !destination_hold_expired(backup, now) {
                return Ok(DestinationAdmission::Holding {
                    reason: refusal.reason(),
                    message: refusal.message,
                });
            }
            return Err(BackupError::Refused(refusal.reason(), refusal.message));
        }
    };
    let observation = destination::read_ca_bundle(client, &archive)
        .await
        .map_err(BackupError::Api)?;
    let archive = archive
        .with_ca(&observation)
        .map_err(|refusal| BackupError::Refused(refusal.reason(), refusal.message))?;
    // THE JOB RUNS WHERE THE REFERENCES RESOLVE, AND NOWHERE ELSE. Every
    // reference the resolution carries is a bare object name, and the kubelet
    // resolves a bare name in the POD's namespace; a Job placed elsewhere would
    // project whichever Secret happens to carry that name THERE. `resolve_ref`
    // is namespace-local, so this can only fail on a hand-built resolution —
    // and it is checked anyway, because the consequence is a silent credential
    // substitution.
    if let Err(refusal) = archive.check_job_namespace(namespace) {
        return Err(BackupError::Refused(refusal.reason(), refusal.message));
    }
    // D2 §3.5's U1 gate. TERMINAL: `spec.transport.caBundle` is immutable on
    // the destination, so a requeue would never succeed — the operator either
    // drops the bundle, uses a public root, or an administrator opts the
    // installation in.
    if archive.ca_bundle.is_some() && !engine_custom_ca_allowed(load.policy()) {
        return Err(BackupError::Refused(
            CheckCode::CaBundleUnsupportedByEngine.as_str(),
            format!(
                "{}{}",
                engine_custom_ca_refusal(&archive.namespace, &archive.name),
                policy_reachability_note(&load)
            ),
        ));
    }
    // THE SECOND ROLE, FROM THE SAME OBJECT AND THE SAME READ. A backup writes
    // its receipt through `evidenceWrite`, and the runner refuses a run that
    // does not say which credential that store uses. `resolve` is pure over the
    // object `resolve_ref` already fetched, so this costs no API call — but it
    // CAN refuse (an `evidenceWrite` naming a Secret with a blank name, say),
    // and that refusal is terminal for the same reason the archive one is.
    let evidence = destination::resolve(&object, DestinationRole::EvidenceWrite, load.policy())
        .map_err(|refusal| BackupError::Refused(refusal.reason(), refusal.message))?
        .with_ca(&observation)
        .map_err(|refusal| BackupError::Refused(refusal.reason(), refusal.message))?;
    // AND THE TWO MUST BE SATISFIABLE BY ONE POD. `evidence_env` owns that
    // question — one pod has one ServiceAccount — and refusing here means the
    // conflict is a terminal `ExecutionContextConflict` before anything is
    // frozen, rather than a Job the API server accepts and the runner cannot
    // authenticate.
    if let Err(refusal) = evidence.evidence_env(&archive) {
        return Err(BackupError::Refused(refusal.reason(), refusal.message));
    }
    Ok(DestinationAdmission::Resolved(Box::new(
        BackupDestinations { archive, evidence },
    )))
}

/// The `BackupDestination` a `destinationRef` names, or the refusal that says
/// why there is none.
///
/// `Ok(Err(refusal))` rather than `Err` for a `DestinationNotFound`: whether an
/// absent object is a hold or a terminal refusal is the CALLER's decision (it
/// depends on the hold budget), and this function does not know the budget.
///
/// # Errors
///
/// [`BackupError::Api`] for a read that failed for any reason other than 404.
async fn resolve_destination_object(
    client: &kube::Client,
    namespace: &str,
    name: &str,
) -> Result<
    Result<crate::crds::backup_destination::BackupDestination, DestinationRefusal>,
    BackupError,
> {
    let api: Api<crate::crds::backup_destination::BackupDestination> =
        Api::namespaced(client.clone(), namespace);
    match api.get_opt(name).await.map_err(BackupError::Api)? {
        Some(object) => Ok(Ok(object)),
        None => Ok(Err(DestinationRefusal {
            code: CheckCode::DestinationNotFound,
            field: "spec.destinationRef.name".to_string(),
            message: format!(
                "namespace {namespace} has no BackupDestination named {name}; a destinationRef \
                 is namespace-local and is never resolved in another namespace"
            ),
        })),
    }
}

/// The ONE installation-policy cache this controller holds.
///
/// # Why a process-wide cache and not a fresh one per call
///
/// `PolicyCache::get` answers from an entry younger than its TTL; a cache
/// constructed at the call site always misses, so the policy `ConfigMap` was
/// re-read on every reconcile pass of every destination-backed object — twice
/// per `Backup` pass, once for admission and once for the evidence source.
/// `controllers::preflight` and `controllers::topic_discovery` hold theirs in
/// their context; this controller's `Context` is shared with every route-table
/// double in `tests/backup_controller.rs`, so a `OnceLock` beside the store
/// cache is the same fix without reshaping a struct three test files build.
fn installation_policy_cache() -> &'static crate::check::policy::PolicyCache {
    static CACHE: std::sync::OnceLock<crate::check::policy::PolicyCache> =
        std::sync::OnceLock::new();
    CACHE.get_or_init(crate::check::policy::PolicyCache::new)
}

/// A sentence naming the missing administrator setting, when this
/// installation's policy document is not the one refusing — D2 W11's
/// `weirkeeper-policy` `ConfigMap`.
///
/// # FAIL CLOSED, AND SAY WHICH CLOSED DOOR IT IS
///
/// Both features this task ships are administrator settings with closed
/// defaults: `evidence.controllerIdentityLocations` is empty and
/// `engine.allowUnverifiedCustomCa` is `false`. When an administrator HAS
/// written a policy and simply did not list this location, the refusal's own
/// message is the whole story — they edit the `ConfigMap`.
///
/// When there is no policy document at all the refusal reads as "your
/// administrator did not list this location" and the truth is "no policy
/// exists, so nobody has listed anything anywhere" — and an operator who cannot
/// tell those apart goes looking for an object that is not there. Two ways to
/// be in that state, both covered:
///
/// * **No reference configured.** Neither
///   [`crate::check::policy::POLICY_CONFIGMAP_ENV`] nor
///   `LOGWEIR_INSTALLATION_NAMESPACE` on the controller Deployment, so
///   `configured_ref` is `None` and nothing is ever read. A hand-wired or
///   embedded controller; the chart and `logweir.yaml` both set the pair.
/// * **Referenced and absent.** The variables are set and the `ConfigMap` is
///   not there — `get_opt` answered `None` and the load defaulted.
///
/// Empty when a policy was actually read, loaded or refused: then the document
/// exists and the administrator's own message applies.
#[must_use]
fn policy_reachability_note(load: &crate::check::policy::PolicyLoad) -> String {
    if !matches!(load, crate::check::policy::PolicyLoad::Defaulted(_)) {
        return String::new();
    }
    let reference = crate::controllers::topic_discovery::configured_policy_ref();
    let where_ = match reference.as_ref() {
        None => format!(
            "this installation names no policy ConfigMap at all — neither {} nor {} is set on \
             the controller Deployment",
            crate::check::policy::POLICY_CONFIGMAP_ENV,
            crate::check::policy::INSTALLATION_NAMESPACE_ENV
        ),
        Some((namespace, name)) => format!(
            "the policy ConfigMap this installation names, {namespace}/{name}, does not exist"
        ),
    };
    format!(
        ". NOTE: {where_}, so every installation-policy key reads its closed default and no \
         administrator setting can change one. The chart renders this ConfigMap from its \
         `engine.*` and `evidence.*` values (D2 W11)"
    )
}

/// The `/status` patch a holding `Backup` carries — `phase: Pending` and one
/// `Admitted=False` condition naming the destination refusal.
///
/// # It is not a terminal state and it does not pretend to be one
///
/// `phase: Pending` is the phase a `Backup` already has before its Job exists,
/// and `status_is_terminal` does not match it, so the next pass re-reads the
/// destination and the hold resolves itself the moment the object appears.
/// `Admitted=False` is what a console renders, and its `reason` is the
/// resolver's own `CheckCode` — the same string the `BackupDestination`'s own
/// `Valid` condition carries, so the two objects agree about the fault.
#[must_use]
pub fn destination_hold_patch(
    backup: &Backup,
    reason: &str,
    message: &str,
    now: DateTime<Utc>,
) -> Value {
    let existing = backup.status.as_ref().and_then(|s| s.conditions.as_ref());
    let admitted = condition(
        backup,
        crate::conditions::CONDITION_ADMITTED,
        "False",
        reason,
        message,
        now,
    );
    json!({
        "status": {
            "phase": crate::conditions::PHASE_PENDING,
            // THE OTHER CONDITIONS ARE CARRIED, because a JSON merge patch
            // REPLACES arrays: a hold that emitted only `Admitted` would delete
            // whatever `TopicsResolved` a dynamic selection had already
            // written on an earlier pass.
            "conditions": carry_conditions(existing, vec![admitted]),
        }
    })
}

// ---------------------------------------------------------------------------
// Evidence observation for destination-backed runs — D2 §3.9, §3.10
// ---------------------------------------------------------------------------

/// The `detail` a `NotAttempted` carries when the destination declares no
/// `evidenceRead` — D2 §3.9 step 2, verbatim.
///
/// # A DEFINED ANSWER, NOT A SILENCE
///
/// `evidenceRead` absent means nobody configured a reader, which is a truthful
/// fact about the destination and NOT a green badge and NOT an error. The
/// detail names the field to add and the command that verifies the receipt
/// without a controller-held credential, so an operator who reads it knows
/// both ways out.
pub const EVIDENCE_READ_NOT_CONFIGURED_PREFIX: &str = "BackupDestination ";

/// The rest of [`EVIDENCE_READ_NOT_CONFIGURED_PREFIX`]'s sentence.
pub const EVIDENCE_READ_NOT_CONFIGURED_SUFFIX: &str =
    " has no spec.access.evidenceRead; run the printed logweir drill verify command instead";

/// Where this run's evidence is read from, for ONE reconcile pass.
///
/// `Debug` is HAND-WRITTEN, because [`Self::Destination`] holds a `Store` and
/// `Store` is deliberately not `Debug` — a derived one would put a handle's
/// configuration, and one day its credential provider, into whatever log line
/// formatted it. What a reader needs is WHICH ARM, which is what the routing
/// property is about; `evidence_is_routed_by_grant_and_never_falls_back_to_the_global_handle`
/// asserts on the arm and never on the handle.
pub enum EvidenceSource {
    /// A legacy inline-`archive` run: the controller's ONE global handle, as
    /// it has always been. D2 §3.10 confines that handle to exactly this case.
    GlobalHandle,
    /// A destination-backed run whose `evidenceRead` is
    /// [`crate::destination::ResolvedGrant::ControllerIdentity`] and whose
    /// location the installation policy allowlists — D2 §3.8 option **E**. The
    /// handle comes from [`crate::evidence_store::StoreCache`]: bucket, prefix,
    /// region, endpoint, addressing and CA from the destination, and only the
    /// CREDENTIAL from the controller's own environment.
    Destination(Arc<Store>),
    /// Nothing may read this run's evidence from here, and the reason is a
    /// fact about the destination or about this build. `NotAttempted` with
    /// this detail, never `Invalid`: a missing reader is not a bad document.
    NotAttempted {
        /// What is missing, and what to do about it. Never a credential.
        detail: String,
    },
}

impl std::fmt::Debug for EvidenceSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GlobalHandle => f.write_str("GlobalHandle"),
            Self::Destination(_) => f.write_str("Destination(<read-only store>)"),
            Self::NotAttempted { detail } => f
                .debug_struct("NotAttempted")
                .field("detail", detail)
                .finish(),
        }
    }
}

/// Which handle this `Backup`'s evidence is read through — D2 §3.9.
///
/// # THE GLOBAL HANDLE IS FOR LEGACY OBJECTS AND NOTHING ELSE (D2 §3.10)
///
/// Grounding **G2**: the controller's one store takes its bucket, region,
/// endpoint and credential from the controller's own process. On an
/// installation with two destinations, reading the second one's evidence
/// through it means the wrong bucket or the wrong principal — and the
/// resulting `NotAttempted` reads as "no evidence" rather than "wrong bucket".
/// So a `Backup` that names a `destinationRef` never reaches it.
///
/// # The three grants that are not `ControllerIdentity`
///
/// * **Absent** (`NotConfigured`): `NotAttempted` naming the field to add.
/// * **`SecretKeys` / `WorkloadIdentity`**: D2 §3.9 reads these through an
///   evidence-fetch JOB in the object's own namespace, because the controller
///   holds no verb on `secrets` and must not — option **B** was rejected for
///   exactly that (D2 §3.8). **THIS BUILD DOES NOT CREATE THAT JOB.** The
///   answer is `NotAttempted` with a detail that says so, which is the honest
///   report of a capability that is not here; what it must never be is a
///   silent fall-back to the global handle, because that handle holds a
///   different principal over a different bucket.
/// * **Not allowlisted**: the resolver's own
///   `ControllerIdentityNotAllowlisted`, so an operator cannot point the
///   controller's principal at a location a cluster administrator did not list.
///
/// # Errors
///
/// [`kube::Error`] for a failed read. A destination that does not resolve at
/// all is NOT an error here: this runs AFTER a Job has finished, and a
/// destination edited or deleted in the meantime must not turn an observed run
/// into a refusal. It is `NotAttempted` with the refusal's own message.
pub async fn evidence_source(
    backup: &Backup,
    client: &kube::Client,
    namespace: &str,
    now: DateTime<Utc>,
) -> Result<EvidenceSource, kube::Error> {
    evidence_source_for(backup.spec.destination_ref.as_ref(), client, namespace, now).await
}

/// [`evidence_source`] for any object that names a destination whose
/// `evidenceRead` grant is the one to use.
///
/// # WHICH REF THE CALLER PASSES IS THE WHOLE QUESTION ON THE RESTORE PATH
///
/// A `Restore` names TWO destinations, and its scorecard was written under the
/// EVIDENCE destination's `evidenceWrite`. Reading it back through the SOURCE
/// destination's bucket is the two-destination form of grounding **G2** — a
/// read against the wrong store reported as "no document". The parameter is a
/// ref rather than an object so the call site has to say which one it means.
///
/// # Errors
///
/// [`kube::Error`] for a failed read; see [`evidence_source`].
pub async fn evidence_source_for(
    reference: Option<&crate::crds::LocalRef>,
    client: &kube::Client,
    namespace: &str,
    now: DateTime<Utc>,
) -> Result<EvidenceSource, kube::Error> {
    let Some(reference) = reference else {
        return Ok(EvidenceSource::GlobalHandle);
    };
    let load = crate::check::policy::load(
        client,
        super::topic_discovery::configured_policy_ref().as_ref(),
        installation_policy_cache(),
        now,
    )
    .await?;
    let resolved = match destination::resolve_ref(
        client,
        namespace,
        &reference.name,
        DestinationRole::EvidenceRead,
        load.policy(),
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(ResolveError::Api(e)) => return Err(e),
        Err(ResolveError::Refused(refusal)) => {
            // FAIL CLOSED, AND SAY WHICH CLOSED DOOR IT IS. A
            // `ControllerIdentityNotAllowlisted` on an installation that
            // renders no policy `ConfigMap` at all reads as "your
            // administrator did not list this location" when the truth is that
            // nobody CAN list one — see `policy_reachability_note`.
            return Ok(EvidenceSource::NotAttempted {
                detail: format!("{refusal}{}", policy_reachability_note(&load)),
            });
        }
    };
    match &resolved.grant {
        crate::destination::ResolvedGrant::NotConfigured => Ok(EvidenceSource::NotAttempted {
            detail: format!(
                "{EVIDENCE_READ_NOT_CONFIGURED_PREFIX}{}{EVIDENCE_READ_NOT_CONFIGURED_SUFFIX}",
                resolved.name
            ),
        }),
        crate::destination::ResolvedGrant::SecretKeys { .. }
        | crate::destination::ResolvedGrant::WorkloadIdentity { .. } => {
            Ok(EvidenceSource::NotAttempted {
                detail: format!(
                    "BackupDestination {}/{} reads evidence with a grant only a pod may hold (D2 \
                     §3.9's evidence-fetch Job), and this build does not create that Job. The \
                     controller holds no verb on secrets and does not read this credential \
                     itself; run the printed logweir drill verify command, or set \
                     spec.access.evidenceRead.mode: ControllerIdentity for an allowlisted \
                     location",
                    resolved.namespace, resolved.name
                ),
            })
        }
        crate::destination::ResolvedGrant::ControllerIdentity => {
            // ONE CACHE PER PROCESS, BOUNDED AT 32. Each handle owns a
            // connection pool and a tokio runtime; the cache is keyed by
            // destination UID, generation and CA digest, so an edit or a
            // rotated root is a new handle by construction. `get_or_build`
            // constructs inside `spawn_blocking` — interface I13's one
            // sanctioned second construction site.
            static CACHE: std::sync::OnceLock<crate::evidence_store::StoreCache> =
                std::sync::OnceLock::new();
            let cache = CACHE.get_or_init(crate::evidence_store::StoreCache::new);
            match cache.get_or_build(&resolved, load.policy()).await {
                Ok(store) => Ok(EvidenceSource::Destination(store)),
                Err(refusal) => Ok(EvidenceSource::NotAttempted {
                    detail: format!("{refusal}{}", policy_reachability_note(&load)),
                }),
            }
        }
    }
}

/// The two evidence keys, as read off the log.
///
/// Both `Option`, independently: a log carrying only one of the two lines
/// yields one key and one absence, and neither is derived from the other.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EvidenceKeys {
    /// `receipt-key=<key>`'s value.
    pub receipt: Option<String>,
    /// `sidecar-key=<key>`'s value.
    pub sidecar: Option<String>,
}

impl EvidenceKeys {
    /// Both lines were present.
    #[must_use]
    pub fn complete(&self) -> bool {
        self.receipt.is_some() && self.sidecar.is_some()
    }
}

/// Whether the two evidence objects exist in the archive.
///
/// Two independent booleans, because the whole point of the exit-4 check is
/// the ASYMMETRIC case: a payload that exists without its sidecar.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EvidencePresence {
    /// The receipt object exists.
    pub payload: bool,
    /// The detached sidecar exists.
    pub sidecar: bool,
}

/// What the archive says about a finished run's evidence.
///
/// TWO FACTS, ONE READ. Both of them need the same object-store handle — the
/// presence of the two keys (step 5's orphan check) and the receipt's own
/// `covered` block (interface **I22**'s window) — so they are one observation
/// rather than two round trips against the same bucket.
/// **NOT `Copy` SINCE TASK 24**, and the reason is a `String`. `receipt_sha256`
/// is the digest of the bytes this observation fetched, which the status has
/// to record so that a LATER pass can re-fetch and compare — a verification
/// that only checked the signature would accept a genuinely-signed OLDER
/// receipt put in this one's place. Every call site that took the value by
/// copy now takes it by reference.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArchiveObservation {
    /// Whether each of the two evidence objects exists.
    pub presence: EvidencePresence,
    /// The receipt's `covered{from_ms, to_ms}`, in **epoch milliseconds**,
    /// when the receipt was read and named its window.
    pub covered: Option<(i64, i64)>,
    /// `sha256_prefixed` of the receipt bytes that were fetched, when they
    /// were. **Computed here and never copied out of the document**: a
    /// document cannot carry its own digest. `None` when the receipt could not
    /// be read at all, which is NOT OBSERVED and not "the receipt is empty".
    pub receipt_sha256: Option<String>,
    /// The sum of `BackupReceipt.records` — how many records this run
    /// archived, across every topic it names. **Defect STATUS-RECORDS**, and
    /// see [`observe_archive`] for why it is summed here and written only
    /// after the receipt VERIFIES.
    pub records: Option<i64>,
    /// `BackupReceipt.{started_at, finished_at}` — D3 §2.2's `status.capture`,
    /// copied verbatim from the same verified receipt.
    pub capture: Option<(DateTime<Utc>, DateTime<Utc>)>,
}

/// What the archive-facing half of this reconciler is handed, and the reason
/// it is a parameter.
///
/// # Why an injected observation and not a `Store` handle
///
/// Reading the receipt and reading the two evidence keys both need a read-only
/// archive handle, and that handle is **interface I13** — the shared
/// `Option<Arc<Store>>` on [`super::Context`], built once in `main` before the
/// tokio runtime exists. Global Constraint 6 fixes what it may be, too:
/// read-only, with no delete capability anywhere in the tree. So both reads
/// are written here as pure decisions over an INJECTED observation, which is
/// what lets `exit_four_with_a_payload_and_no_sidecar_is_orphaned_scorecard`
/// and `window_covered_is_epoch_milliseconds_on_the_status` assert the status
/// this reconciler patches without a socket, while this module's `reconcile`
/// entry point builds the
/// real one from [`super::Context::archive`].
///
/// # WHY IT IS ASYNC, AND WHY THAT IS NOT DECORATION
///
/// `Store::get` is a **blocking** method that drives its own current-thread
/// runtime, and `kube` drives every reconciler ON a runtime: a direct call
/// COMPILES CLEANLY and panics with *Cannot start a runtime from within a
/// runtime* at the first reconcile. So every `Store` call in this crate goes
/// through `tokio::task::spawn_blocking(move || …).await` — interface **I13**,
/// enforced over this file's source text by
/// `tests/retention.rs::no_store_call_is_made_outside_spawn_blocking`, whose
/// `I13_FILES` names it. A synchronous `Fn` cannot `.await` anything, so the
/// oracle's TYPE has to be the async one: the review of this task proved by
/// execution that the sync shape compiles and then fails that guard.
///
/// `EvidenceKeys` BY VALUE, not by reference: the future outlives the call, so
/// the keys have to be owned by the closure that reads them inside
/// `spawn_blocking`.
///
/// # `None` means NOT OBSERVED, never "absent"
///
/// A controller that treated an unavailable archive handle as "the sidecar is
/// missing" would put `OrphanedScorecard` on every exit-4 run in a cluster
/// with no archive credential, and one that treated it as "the window is
/// empty" would write a `windowCovered` of nothing over a real one. So a
/// `None` archive on [`super::Context`] yields a `None` observation, which
/// omits `windowCovered` from the merge patch and never records an orphan.
///
/// # `BoxFuture<'static, …>` AND NOT `BoxFuture<'a, …>`, AND IT IS MEASURED
///
/// The review handed this task the second shape. It does not compile at the
/// call site: an `Fn`'s `Output` is an associated TYPE matched exactly, so a
/// future borrowed for `'a` makes `'a` the lifetime of the returned future AND
/// of the `&'a dyn` — and `reconcile`'s oracle is a local, so the borrow
/// checker asks for a `'static` local
/// (`error[E0597]: 'oracle' does not live long enough … assignment requires
/// that 'oracle' is borrowed for 'static`). The future owns everything it
/// reads — an `Arc<Store>` clone and the keys, both moved in — so `'static` is
/// what it actually is, and `'a` stays what it should be: how long the
/// reconcile holds the oracle.
pub type ArchiveOracle<'a> =
    &'a (dyn Fn(EvidenceKeys) -> BoxFuture<'static, Option<ArchiveObservation>> + Send + Sync + 'a);

/// The oracle for a controller that holds no archive handle: it observes
/// nothing.
///
/// See [`ArchiveOracle`]. This is what this module's `reconcile` entry point
/// uses when
/// [`super::Context::archive`] is `None` — a controller with no
/// `LOGWEIR_ARCHIVE_URL` configured — and it is what every unit test that is
/// not about the archive passes.
#[must_use]
pub fn unobserved_archive(_keys: EvidenceKeys) -> BoxFuture<'static, Option<ArchiveObservation>> {
    Box::pin(async { None })
}

/// The two `get`s the real oracle makes, as ONE blocking function.
///
/// **THE ONLY PLACE IN THIS FILE THAT TOUCHES A `Store`, AND IT IS NOT
/// `async`.** Both reads happen on one `spawn_blocking` thread (see
/// [`ArchiveOracle`]): two round trips against the same bucket for two facts
/// that are decided together, rather than two hops on and off the runtime.
///
/// `Store` EXPOSES NO `head`, so presence is "a `get` that returned bytes".
/// That is strictly more work than a `HEAD` for the sidecar, whose bytes are
/// discarded — and it is the only capability the read-only handle has. The
/// receipt's bytes are needed anyway, for `covered`.
///
/// The two booleans are INDEPENDENT, because the whole point of the exit-4
/// check is the asymmetric case: a payload that exists without its sidecar.
/// An unreadable receipt is `payload: false`, not an error — a run whose
/// receipt cannot be fetched has an archive that says nothing about it, and
/// `orphan_state` is then reading a fact rather than a failure.
///
/// Returns `None` when there is no key to look up at all, which is NOT
/// OBSERVED and not "absent".
#[must_use]
pub fn observe_archive(store: &Store, keys: &EvidenceKeys) -> Option<ArchiveObservation> {
    if keys.receipt.is_none() && keys.sidecar.is_none() {
        return None;
    }
    let receipt = keys
        .receipt
        .as_deref()
        .and_then(|key| store.get(key).ok())
        .map(|(bytes, _version)| bytes);
    let sidecar = keys
        .sidecar
        .as_deref()
        .is_some_and(|key| store.get(key).is_ok());
    let document = receipt
        .as_deref()
        .and_then(|bytes| serde_json::from_slice::<Value>(bytes).ok());
    let covered = document.as_ref().and_then(covered_from_receipt);
    let records = document.as_ref().and_then(records_from_receipt);
    let capture = document.as_ref().and_then(capture_from_receipt);
    // THE DIGEST OF WHAT WAS ACTUALLY FETCHED, in the one spelling this corpus
    // uses (`sha256:<lowercase hex>`), so a value read off the status and a
    // value read out of a signed document compare as strings. Task 24's
    // `verify_evidence` re-fetches this object on a later pass and compares.
    let receipt_sha256 = receipt.as_deref().map(sha256_prefixed);
    Some(ArchiveObservation {
        presence: EvidencePresence {
            payload: receipt.is_some(),
            sidecar,
        },
        covered,
        receipt_sha256,
        records,
        capture,
    })
}

/// The total record count out of a receipt — **defect STATUS-RECORDS**.
///
/// `Backup.status.records` has been declared on the CRD with a `RECORDS`
/// printer column since the kind existed and nothing ever wrote it: the column
/// was blank on every `Backup` the PLAT-06.1 and PLAT-07.1 live runs produced,
/// while the counts sat in the signed receipt all along. Found by
/// `plat07-live`; D3 assigns it to PLAT-14.1.
///
/// **A SUM, AND THE FIELD IS SCALAR.** `BackupReceipt.records` is per topic
/// and `status.records` is one integer, so the answer is their sum — which is
/// exactly what the column means ("how many records the run archived") and
/// what the CRD's field description already says. The per-topic breakdown
/// stays where it is attested, in the signed document; a status is not a
/// second copy of a receipt.
///
/// `None` rather than `0` when the block is absent or a value does not fit:
/// a blank column is honest and a zero is a claim.
#[must_use]
pub fn records_from_receipt(receipt: &Value) -> Option<i64> {
    let records = receipt.get("records")?.as_object()?;
    let mut total: i64 = 0;
    for value in records.values() {
        total = total.checked_add(i64::try_from(value.as_u64()?).ok()?)?;
    }
    Some(total)
}

/// `status.capture`, from the same receipt — D3 §2.2.
///
/// BOTH INSTANTS OR NEITHER, for [`covered_from_receipt`]'s reason: a half-read
/// window cannot be told apart from a window that is genuinely open-ended.
#[must_use]
pub fn capture_from_receipt(receipt: &Value) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let at = |key: &str| -> Option<DateTime<Utc>> {
        receipt.get(key)?.as_str()?.parse::<DateTime<Utc>>().ok()
    };
    Some((at("started_at")?, at("finished_at")?))
}

/// Read the two evidence keys off a pod log — **by key name, never by
/// position**.
///
/// # Why by name
///
/// Interface **I7** fixes the ORDER the runner prints them in, and a
/// controller that trusted the order would be a controller whose evidence
/// pointer silently swaps if a future runner emits them the other way round —
/// writing the `.sig` key into `receiptKey` and the `.json` key into
/// `sidecarKey`, both of which "look like" object keys and neither of which a
/// verifier could then fetch. The second arm of
/// `the_runner_keys_are_read_from_the_final_two_stdout_lines` feeds the lines
/// reversed and asserts each key still lands in its own field.
///
/// # Why no key is ever guessed
///
/// A receipt key is derivable — `logweir/backups/<backup_id>/<run_id>.receipt.json` —
/// and a controller that derived one on an unreadable log would point
/// `status.evidence.receiptKey` at an object that may not exist. A verifier
/// (Task 24) would then report `Invalid` for a run whose evidence was merely
/// unread. Absent is the truthful value, and the condition reason
/// [`REASON_EVIDENCE_KEYS_UNREADABLE`] is how the absence is explained.
///
/// The LAST occurrence of each prefix wins, within the final
/// [`KEY_SCAN_TAIL_LINES`] lines: a runner that logged an earlier draft of the
/// key would have the final one be the one that was written.
#[must_use]
pub fn evidence_keys(log: &str) -> EvidenceKeys {
    let mut keys = EvidenceKeys::default();
    for line in tail_lines(log) {
        if let Some(v) = line.strip_prefix(RECEIPT_KEY_PREFIX) {
            keys.receipt = Some(v.to_string());
        }
        if let Some(v) = line.strip_prefix(SIDECAR_KEY_PREFIX) {
            keys.sidecar = Some(v.to_string());
        }
    }
    keys
}

/// The terminal state a guard refusal named, from the log's final
/// `refusal-reason=` line.
///
/// Global Constraint 11: **the pod log API has no stream selector**, so
/// nothing a controller reads is distinguishable by stream and the refusal
/// discriminator has to be on stdout. An exit 3 whose log carries no such line
/// is [`TERMINAL_STATE_GUARD_REFUSED_UNKNOWN_REASON`] — a named observation,
/// not a shrug and not a guess at which guard fired.
#[must_use]
pub fn refusal_state(log: &str) -> Option<String> {
    let mut found = None;
    for line in tail_lines(log) {
        if let Some(v) = line.strip_prefix(REFUSAL_REASON_PREFIX) {
            found = Some(v.to_string());
        }
    }
    found
}

/// The final [`KEY_SCAN_TAIL_LINES`] non-empty lines, trimmed of `\r`.
///
/// **PUBLIC, AND SHARED WITH [`super::restore`] (Task 20).** Interface I7's
/// two keys and interface I8's three come off the same bounded tail under the
/// same rule — plan erratum **E4**'s "scan a bounded tail and match by key
/// name" — and the two reconcilers must not disagree about what "the tail" is.
/// Sharing the ONE implementation rather than copying three lines is what
/// makes a change to [`KEY_SCAN_TAIL_LINES`] reach both scanners; the key
/// scans themselves stay separate, because the prefix sets differ and one of
/// interface I8's three lines is conditional.
#[must_use]
pub fn tail_lines(log: &str) -> Vec<&str> {
    let all: Vec<&str> = log
        .lines()
        .map(|l| l.trim_end_matches('\r'))
        .filter(|l| !l.trim().is_empty())
        .collect();
    let start = all.len().saturating_sub(KEY_SCAN_TAIL_LINES);
    all[start..].to_vec()
}

/// The exit code of the container named `runner`, if it terminated.
///
/// **BY NAME, NEVER BY INDEX.** `status.containerStatuses` is not ordered by
/// anything a controller may rely on, and an init container or a logging
/// sidecar would put an unrelated `exitCode: 0` at index 0 — turning a failed
/// run into a green badge. `the_container_is_selected_by_name` is the test,
/// over a two-container fixture whose index 0 exited 0 and whose `runner`
/// exited 2.
///
/// `state.terminated`, not `lastState.terminated`: under
/// `restartPolicy: Never` there is no restart, so `state` is where the code
/// is, and reading `lastState` would silently accept a shape this Job cannot
/// produce.
#[must_use]
pub fn terminated_exit_code(pod: &Pod) -> Option<i32> {
    pod.status
        .as_ref()?
        .container_statuses
        .as_ref()?
        .iter()
        .find(|c| c.name == CONTAINER_NAME)?
        .state
        .as_ref()?
        .terminated
        .as_ref()
        .map(|t| t.exit_code)
}

/// The terminal state for a finished Job that reported no exit code.
///
/// Four sub-cases, in this order, and the order is the classification:
/// 1. no pod at all — the selector returned an empty list, so the pod was
///    garbage-collected or never created: [`TERMINAL_STATE_NO_EXIT_CODE`];
/// 2. the pod carries `DisruptionTarget=True` — the node went away mid-run:
///    [`TERMINAL_STATE_DISRUPTED_MID_DRILL`], checked BEFORE the scheduling
///    case because a disrupted pod can also be `Pending`;
/// 3. the pod is `Pending` with `PodScheduled=False` and reason
///    `Unschedulable`: [`TERMINAL_STATE_POD_UNSCHEDULABLE`], which wrote no
///    log at all;
/// 4. anything else: [`TERMINAL_STATE_NO_EXIT_CODE`].
#[must_use]
pub fn crash_terminal_state(pod: Option<&Pod>) -> &'static str {
    let Some(pod) = pod else {
        return TERMINAL_STATE_NO_EXIT_CODE;
    };
    let status = pod.status.as_ref();
    let conditions = status.and_then(|s| s.conditions.as_ref());
    if let Some(conditions) = conditions {
        if conditions
            .iter()
            .any(|c| c.type_ == "DisruptionTarget" && c.status == "True")
        {
            return TERMINAL_STATE_DISRUPTED_MID_DRILL;
        }
        let pending = status.and_then(|s| s.phase.as_deref()) == Some("Pending");
        if pending
            && conditions.iter().any(|c| {
                c.type_ == "PodScheduled"
                    && c.status == "False"
                    && c.reason.as_deref() == Some("Unschedulable")
            })
        {
            return TERMINAL_STATE_POD_UNSCHEDULABLE;
        }
    }
    TERMINAL_STATE_NO_EXIT_CODE
}

/// Whether an exit-4 run left a payload without its sidecar.
///
/// Returns the terminal state, or `None` when there is nothing to record —
/// which includes **every case where the archive was not observed**. It does
/// not delete, does not repair, and is not rendered as a result
/// (`design-operator.md:527-535`): an orphaned scorecard is a fact about the
/// archive that an operator has to decide about, and a controller that "fixed"
/// it would be a controller that wrote to an archive Global Constraint 6 gives
/// it read-only.
#[must_use]
pub fn orphan_state(exit_code: i32, presence: Option<EvidencePresence>) -> Option<&'static str> {
    if exit_code != 4 {
        return None;
    }
    match presence {
        Some(p) if p.payload && !p.sidecar => Some(TERMINAL_STATE_ORPHANED_SCORECARD),
        _ => None,
    }
}

/// The [`RunnerJobSpec`] one `Backup` and its referenced source cluster produce,
/// from the inputs that would be frozen now.
///
/// PURE, so the Job a test builds is byte-identical to the one the reconciler
/// `POST`s and an assertion over this function is an assertion over the
/// request. The argv is [`backup_execution::runner_argv`] of the derived run
/// identity; no annotation is read.
///
/// # Errors
///
/// Whatever [`desired_execution_inputs`] refuses, or
/// [`runner_job_spec_from_inputs`].
pub fn runner_job_spec(
    backup: &Backup,
    cluster: &KafkaCluster,
) -> Result<RunnerJobSpec, BackupError> {
    runner_job_spec_from_inputs(backup, cluster, &desired_execution_inputs(backup, cluster)?)
}

/// The [`RunnerJobSpec`] for FROZEN inputs.
///
/// Everything the container executes — argv, deadline, archive addressing
/// environment, the plan mount — comes from `frozen`. The two Secret key
/// references come from `cluster` and `Backup.spec.archive.secretRef`, which
/// the snapshot pins by the cluster's UID and the CEL-immutable specs; the
/// snapshot deliberately names no Secret. A `cluster` whose UID is not the
/// frozen one is refused rather than combined.
///
/// # Errors
///
/// [`BackupError`] when the object carries no namespace or UID (both
/// unreachable from the API server), when `cluster` is not the frozen
/// referent, or when a SCRAM source has no usable Secret reference.
pub fn runner_job_spec_from_inputs(
    backup: &Backup,
    cluster: &KafkaCluster,
    frozen: &FrozenInputs,
) -> Result<RunnerJobSpec, BackupError> {
    let name = backup.name_any();
    let namespace = backup
        .namespace()
        .ok_or_else(|| BackupError::NoNamespace(name.clone()))?;
    let uid = backup
        .uid()
        .ok_or_else(|| BackupError::NoUid(name.clone()))?;
    let inputs = &frozen.inputs;
    if cluster.uid().as_deref() != Some(inputs.source.cluster.uid.as_str()) {
        return Err(BackupError::Refused(
            TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT,
            format!(
                "the frozen execution inputs pin KafkaCluster {} with UID {}, and the referent \
                 read now has UID {}; its credential reference cannot be combined with those \
                 inputs",
                inputs.source.cluster.name,
                inputs.source.cluster.uid,
                cluster.uid().unwrap_or_default()
            ),
        ));
    }

    // THE SOURCE CONNECTION, FROM THE ONE RESOLVER THE PROBE USES (PLAT-07.1):
    // the password as `secretKeyRef` and a private CA as a projected file,
    // references only. A connection that does not resolve is refused here,
    // before the Job exists.
    let connection =
        crate::connection::resolve(cluster, crate::connection::ConnectionUse::BackupSource)?;
    connection.check_job_namespace(&namespace)?;
    // AND IT MUST BE THE CONNECTION THE SNAPSHOT FROZE. The UID check above
    // says the referent is the same OBJECT; this says its connection still
    // resolves to what the frozen plan document was rendered from and approved
    // against. Addresses, auth and the CA reference are exactly the three
    // things the snapshot carries and the Job or the plan acts on, so an edit
    // to any of them between the freeze and a Job re-creation is refused here
    // as well as by `verify_frozen_config_map` — this function is public and
    // pure, and a caller holding frozen inputs may reach it without the
    // reconciler's verify pass.
    let frozen_connection = (
        &inputs.source.bootstrap_servers,
        &inputs.source.auth,
        &inputs.source.tls_ca,
    );
    let resolved_connection = (
        &connection.bootstrap_servers,
        &connection.auth,
        &connection.tls_ca,
    );
    if frozen_connection != resolved_connection {
        return Err(BackupError::Refused(
            TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT,
            format!(
                "the frozen execution inputs of {name} resolved KafkaCluster {} to a different \
                 connection than it resolves to now (bootstrap servers, auth or the auth.tlsCa \
                 reference), so the frozen plan bytes would be executed against a changed \
                 connection",
                inputs.source.cluster.name
            ),
        ));
    }
    let projection = connection.project();

    let mut secret_mounts = vec![SecretMount {
        volume: SIGNING_VOLUME.to_string(),
        secret_name: SIGNING_KEY_SECRET.to_string(),
        mount_path: SIGNING_MOUNT_PATH.to_string(),
        items: vec![(
            SIGNING_KEY_SECRET_KEY.to_string(),
            SIGNING_KEY_FILE.to_string(),
        )],
    }];
    secret_mounts.extend(projection.secret_mounts);
    // Kept out of `env_from_secret` for the same reason the drill manifest
    // keeps the signing key out of it: a key is a FILE the runner opens by
    // path, and an env var holding PEM text would appear in
    // `kubectl describe pod` output for anyone with pod read.
    secret_mounts.sort_by(|a, b| a.volume.cmp(&b.volume));

    let mut env_from_secret = projection.env_from_secret;
    if let Some(secret) = backup.spec.archive.secret_ref.as_ref() {
        env_from_secret.push(EnvFromSecret {
            name: ARCHIVE_ACCESS_KEY_ENV.to_string(),
            secret_name: secret.name.clone(),
            key: ARCHIVE_ACCESS_KEY.to_string(),
        });
        env_from_secret.push(EnvFromSecret {
            name: ARCHIVE_SECRET_KEY_ENV.to_string(),
            secret_name: secret.name.clone(),
            key: ARCHIVE_SECRET_KEY.to_string(),
        });
    }

    // === THE DESTINATION'S CONTRIBUTION, FROM THE FROZEN BLOCK (D2 §3.5) ===
    //
    // FROM `frozen` AND NOT FROM A FRESH READ. A Job is created once and may be
    // RE-created — garbage collected, a node lost, a controller restarted
    // mid-run. Rendering the second Job from the destination as it reads NOW
    // would let a CA or access rotation between the freeze and the re-creation
    // change what an approved, half-written run addresses and trusts. D2 §3.7:
    // "the Job is rendered only from the snapshot".
    //
    // LEGACY RUNS REACH NONE OF THIS. `inputs.destination` is `None` for every
    // inline-`archive` run, `job_env` is never called, and the Job this
    // function returns is byte-identical to the one it returned before
    // destinations existed — which is what the untouched goldens assert.
    //
    // TWO HALVES, BECAUSE A BACKUP WRITES TWO THINGS. `job_env` is the ARCHIVE
    // store the engine writes segments through; `evidence_job_env` is the
    // receipt store, over Global Constraint 6's `logweir/` root of the same
    // bucket. The runner builds BOTH and refuses a run that does not name the
    // second one's credential — before this line existed, every
    // destination-backed run exited 3 there with the archive already opened and
    // nothing archived.
    let destination_env = inputs.destination.as_ref().map(|d| d.job_env());
    let evidence_env = inputs.destination.as_ref().map(|d| d.evidence_job_env());
    for env in [destination_env.as_ref(), evidence_env.as_ref()]
        .into_iter()
        .flatten()
    {
        env_from_secret.extend(env.from_secret.iter().cloned());
    }
    // ONE POD, ONE ServiceAccount. A workload-identity grant REPLACES the
    // connection's runner ServiceAccount, because the pod's identity is what
    // the object store authenticates and a pod has exactly one. The runner
    // reaches no API server either way (`job::build` sets
    // `automountServiceAccountToken: false`), so this is an object-store
    // identity and not a Kubernetes privilege change.
    let service_account_name = destination_env
        .as_ref()
        .and_then(|env| env.service_account_name.clone())
        .or_else(|| {
            evidence_env
                .as_ref()
                .and_then(|env| env.service_account_name.clone())
        })
        .unwrap_or_else(|| connection.execution.service_account_name.clone());

    Ok(RunnerJobSpec {
        // THE JOB IS NAMED AFTER THE CR, VERBATIM. See
        // `RunnerJobSpec::name` for the 63-character argument.
        name: name.clone(),
        namespace,
        owner: RunnerOwner {
            api_version: Backup::api_version(&()).to_string(),
            kind: Backup::kind(&()).to_string(),
            name,
            uid,
        },
        args: inputs.runner.args.clone(),
        deadline_seconds: inputs.runner.deadline_seconds,
        service_account_name,
        secret_mounts,
        config_map_mounts: projection.config_map_mounts,
        env_from_secret,
        // `RUST_LOG` is pinned rather than inherited: below `info` the run id
        // and the exit-code meaning line are lost, and those two are how a
        // pod is correlated with the archive it read
        // (`docs/kubernetes.md` §1b). The addressing variables are the FROZEN
        // ones, so a Job recreated after a controller restart addresses the
        // store its plan document names.
        env_literal: {
            let mut env = vec![("RUST_LOG".to_string(), "info".to_string())];
            // EMPTY FOR A DESTINATION-BACKED RUN — `resolve_inputs` froze no
            // addressing at all for one — so these two `extend`s are exclusive
            // in practice and the complete explicit set below is the only
            // answer to "where is the bucket".
            env.extend(
                inputs
                    .archive
                    .addressing_env
                    .iter()
                    .map(|v| (v.name.clone(), v.value.clone())),
            );
            for block in [destination_env.as_ref(), evidence_env.as_ref()]
                .into_iter()
                .flatten()
            {
                env.extend(block.literals.iter().cloned());
            }
            env.extend(projection.env_literal);
            env
        },
        plan_config_map: Some(plan_config_map_name(&backup.name_any())),
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

/// The runner Job for frozen inputs, exactly as the reconciler `POST`s it: the
/// [`runner_job_spec_from_inputs`] shape with this process's image overrides,
/// and the Job and its pod template annotated with the run identity and the
/// inputs digest ([`annotate_runner_job`]).
///
/// # Errors
///
/// Whatever [`runner_job_spec_from_inputs`] refuses.
pub fn runner_job(
    backup: &Backup,
    cluster: &KafkaCluster,
    frozen: &FrozenInputs,
    runner: &job::RunnerImage,
) -> Result<Job, BackupError> {
    let mut spec = runner_job_spec_from_inputs(backup, cluster, frozen)?;
    // THE TWO LINES THE OVERRIDES ARE (Task 33's image, Task 37's pull
    // policy). `None` in either leaves the compiled-in constant in place,
    // which is what every test that does not pass one sees.
    spec.image = runner.image.clone();
    spec.image_pull_policy = runner.image_pull_policy.clone();
    let mut built = job::build(&spec);
    annotate_runner_job(&mut built, frozen);
    Ok(built)
}

/// Whether a Job has reached a terminal condition.
///
/// `Complete` or `Failed` with `status: "True"`. Not `status.succeeded` /
/// `status.failed`, whose counters are incremented for reasons a
/// single-pod Job cannot distinguish, and not `completionTime`, which a failed
/// Job never gets.
#[must_use]
pub fn job_finished(job: &Job) -> bool {
    job.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|cs| {
            cs.iter()
                .any(|c| (c.type_ == "Complete" || c.type_ == "Failed") && c.status == "True")
        })
}

/// Whether `status` is already terminal, so a finished run is not re-run.
///
/// Reading `phase` and not `exitCode`: the crashed-Job case writes a terminal
/// phase with NO exit code on purpose, and a check that keyed on the code
/// would re-create the Job for exactly the run whose code is unrecoverable.
#[must_use]
pub fn status_is_terminal(backup: &Backup) -> bool {
    matches!(
        backup.status.as_ref().and_then(|s| s.phase.as_deref()),
        Some(PHASE_SUCCEEDED | PHASE_FAILED)
    )
}

/// One condition, as a merge-patch fragment.
///
/// `lastTransitionTime` moves only when the condition actually transitions,
/// and the comparison that decides it is
/// [`crate::conditions::merge_condition`] — ONE implementation for all six
/// reconcilers (plan erratum E11(d)). This file used to carry a private copy
/// of it, as did `restore.rs`, `kafka_cluster.rs` and `backup_schedule.rs`;
/// four copies of a rule is how two other reconcilers came to ship without it
/// at all.
///
/// BUILT BY SERIALISING [`crate::crds::Condition`] AND NOT BY HAND. A merge
/// patch REPLACES an array rather than merging it, so the element this writes
/// is compared whole against the stored one by
/// [`crate::conditions::status_unchanged`]; a hand-built element that spelled
/// one optional field differently from the stored one — `observedGeneration:
/// null` where the stored object simply has no such key — would compare
/// unequal on every pass and re-open the very loop this closes.
fn condition(
    backup: &Backup,
    r#type: &str,
    status: &str,
    reason: &str,
    message: &str,
    now: DateTime<Utc>,
) -> Value {
    json!(merge_condition(
        current_condition(
            backup.status.as_ref().and_then(|s| s.conditions.as_ref()),
            r#type,
        ),
        crate::crds::Condition {
            r#type: r#type.to_string(),
            status: status.to_string(),
            observed_generation: backup.meta().generation,
            last_transition_time: Some(now),
            reason: Some(reason.to_string()),
            message: Some(message.to_string()),
        },
    ))
}

/// `backup` with a `/status` merge patch applied in memory, exactly as the API
/// server applies it ([`crate::conditions::apply_merge_patch`]).
///
/// Used after a write succeeds within one pass, so a later builder in the same
/// pass carries what that write stored and a later change check compares
/// against it. An unreadable result keeps the object as it was.
#[must_use]
pub fn with_status_patch(backup: &Backup, patch: &Value) -> Backup {
    let Some(fragment) = patch.get("status") else {
        return backup.clone();
    };
    let mut status = backup
        .status
        .as_ref()
        .and_then(|s| serde_json::to_value(s).ok())
        .unwrap_or(Value::Null);
    crate::conditions::apply_merge_patch(&mut status, fragment);
    let mut projected = backup.clone();
    if let Ok(next) = serde_json::from_value(status) {
        projected.status = Some(next);
    }
    projected
}

/// Patch `/status` — under seam **S7**'s `metadata.resourceVersion`
/// precondition, and not at all when the patch would change nothing.
///
/// THE THIRD AND FOURTH RULES OF THE STATUS-WRITE CONTRACT, at this kind's
/// eight patch sites. Neither is re-implemented here — both are
/// [`crate::conditions::patch_status_preconditioned`]'s — so neither can be
/// applied at seven call sites and forgotten at the eighth, which is what
/// defect STATUS-PATCH-NO-RV was: every write from this reconciler was
/// unconditional. `Backup` has FOUR status writers (this one,
/// `backup_selection`, `backup_schedule`'s reservation and `schedule_history`),
/// so "a concurrent writer" here is the ordinary case and not a race to
/// imagine.
///
/// THE PRECONDITION IS THE OBJECT THIS CALLER OBSERVED, which for the second
/// and third write of one pass is not the object the watch delivered — see
/// [`with_status_written`] and [`patch_status_at`].
async fn patch_status_if_changed(
    api: &Api<Backup>,
    backup: &Backup,
    name: &str,
    patch: Value,
) -> Result<StatusVersion, BackupError> {
    patch_status_at(
        api,
        backup,
        name,
        &StatusVersion::observed(backup.meta()),
        patch,
    )
    .await
}

/// [`patch_status_if_changed`] for a write that is NOT the first of its pass.
///
/// `at` is where the previous write of this pass left the object. The freeze
/// pass writes the execution record, then `TopicsResolved`, then the running
/// patch; the terminal pass writes the outcome and then the evidence verdict.
/// Preconditioned on the watch's version instead, every one of those later
/// writes would be refused with a `409` — and on the terminal pass the verdict
/// would be lost, because a terminal `Backup` is never read again.
async fn patch_status_at(
    api: &Api<Backup>,
    backup: &Backup,
    name: &str,
    at: &StatusVersion,
    patch: Value,
) -> Result<StatusVersion, BackupError> {
    crate::conditions::patch_status_preconditioned(
        api,
        "Backup",
        name,
        at,
        backup
            .status
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok())
            .as_ref(),
        patch,
    )
    .await
    .map_err(BackupError::Api)
}

/// [`with_status_patch`], ALSO carrying the `resourceVersion` the write that
/// stored `patch` left behind.
///
/// The projection exists so a later builder in the same pass sees what that
/// write stored; seam **S7** adds the other half — a later WRITE in the same
/// pass must also precondition on where that write left the object. Both
/// halves travel on one value, so a call site cannot advance the status and
/// forget the version.
#[must_use]
fn with_status_written(backup: &Backup, patch: &Value, at: &StatusVersion) -> Backup {
    let mut next = with_status_patch(backup, patch);
    if let Some(version) = at.get() {
        next.metadata.resource_version = Some(version.to_string());
    }
    next
}

/// The `/status` merge patch for a Job that exists and has not finished.
///
/// BUILT AS JSON AND NOT BY SERIALISING `BackupStatus`, for merge-patch
/// semantics: every optional field on that struct skips serialising when
/// `None`, so an absent key means "leave it alone" — which is exactly what a
/// running reconcile wants for the fields a finished one will write, and which
/// a serialised struct could not express.
///
/// The condition array CARRIES the execution-contract conditions and
/// `Verified` ([`carry_conditions`]): a merge patch replaces arrays, and this
/// builder owns only `JobCreated`.
#[must_use]
pub fn running_status_patch(backup: &Backup, job_name: &str, now: DateTime<Utc>) -> Value {
    let conditions = carry_conditions(
        backup.status.as_ref().and_then(|s| s.conditions.as_ref()),
        vec![condition(
            backup,
            CONDITION_JOB_CREATED,
            "True",
            CONDITION_JOB_CREATED,
            &format!("the runner Job {job_name} exists and has not finished"),
            now,
        )],
    );
    json!({
        "status": {
            "phase": PHASE_RUNNING,
            "jobRef": { "name": job_name },
            "conditions": conditions,
        }
    })
}

/// The condition types a `Backup` patch builder carries without owning them:
/// the execution-contract observations
/// ([`CONDITION_EXECUTION_INPUTS_UNVERIFIED`],
/// [`CONDITION_RUNNER_ARGV_ANNOTATION_IGNORED`]), D1's
/// [`CONDITION_TOPICS_RESOLVED`], and last
/// [`crate::conditions::CONDITION_VERIFIED`] and
/// [`crate::conditions::CONDITION_RUNNER_READY`].
///
/// A MERGE PATCH REPLACES ARRAYS, so a builder that writes `conditions` owes
/// the parts it does not own — the hot loop `verification::carry_verified`
/// documents is the same one a dropped observation would start. The order is
/// fixed (the builder's own, then the three observations, then `Verified` and
/// `RunnerReady`) so a steady object computes the same array on every pass and
/// sends nothing.
///
/// # `RunnerReady` is carried, and review finding F1 is why
///
/// D3 §2.2 says every terminal builder carries `RunnerReady` and `Verified`
/// forward. It did not. The condition is written by the progress path and by
/// nothing else, so the terminal patch that RECORDS a failure was deleting the
/// one condition that says what the failure was — and a terminal object is
/// never reconciled again, so no later pass rewrote it. A `Backup` whose
/// Secret is missing showed `RunnerReady=False/CredentialReferenceMissing` for
/// five minutes and then, at the fail-fast cancellation, showed nothing at
/// all. D3 §15's L1 asserts that condition live, AFTER the terminal patch.
#[must_use]
pub fn carry_conditions(
    existing: Option<&Vec<Condition>>,
    mut conditions: Vec<Value>,
) -> Vec<Value> {
    for r#type in [
        CONDITION_EXECUTION_INPUTS_UNVERIFIED,
        CONDITION_RUNNER_ARGV_ANNOTATION_IGNORED,
        // D1 §7.2 R9. `TopicsResolved` is written by
        // `controllers::backup_selection` and by nothing else, and every other
        // builder owes it the same debt it owes the two observations above: a
        // merge patch replaces `status.conditions`, so the `Running` patch that
        // follows a freeze would otherwise erase the answer to "where did this
        // run's topic list come from" one line after writing it.
        CONDITION_TOPICS_RESOLVED,
    ] {
        if conditions
            .iter()
            .any(|c| c.get("type") == Some(&json!(r#type)))
        {
            continue;
        }
        if let Some(c) = current_condition(existing, r#type) {
            conditions.push(json!(c));
        }
    }
    crate::verification::carry_conditions(
        existing,
        conditions,
        &[CONDITION_VERIFIED, CONDITION_RUNNER_READY],
    )
}

/// The `status.destination` projection of a frozen destination snapshot —
/// D2 §3.7.
///
/// ONE PROJECTION, SO THE STATUS AND THE FROZEN DOCUMENT CANNOT DISAGREE, for
/// the same reason `SelectionInputs::status` is one: a reader comparing
/// `status.destination.locationDigest` against the plan is comparing two
/// renderings of one value, not two digests.
///
/// It copies four fields and derives nothing. The snapshot's storage blocks,
/// transport, addressing, CA digest and grant stay where they are: they are
/// what the JOB is rendered from, and a second spelling of them on the object
/// would be a second thing to keep true.
#[must_use]
pub fn frozen_destination_status(snapshot: &ResolvedDestinationSnapshot) -> FrozenDestination {
    FrozenDestination {
        name: snapshot.name.clone(),
        uid: snapshot.uid.clone(),
        generation: snapshot.generation,
        location_digest: snapshot.location_digest.clone(),
    }
}

/// The `/status` merge patch that records frozen inputs: `status.execution`,
/// `status.selection`, `status.destination`, and nothing else.
///
/// SENT BEFORE THE JOB IS CREATED, and a merge of object keys: it replaces no
/// array and so cannot drop a condition another writer owns.
///
/// **`status.selection` IS WRITTEN AT THE FREEZE** (D1 §7.6), in this same
/// patch and not a later one. It says what the run may claim to have covered,
/// and a coverage label that appeared only after the Job finished would be
/// absent for exactly the window in which somebody is watching the run. It is
/// absent for a `Backup` frozen under grammar `v1`, which is the documented
/// absent-field behaviour and not a degraded state.
///
/// **`status.destination` IS WRITTEN AT THE FREEZE TOO** (D2 §3.7), from the
/// same snapshot the plan is rendered from, and it is the only place this
/// controller writes it. A later pass re-reads the STORED snapshot, renders
/// this same patch and `patch_status_if_changed` sends nothing, so the block a
/// recovery point publishes is the block its run was admitted with — a
/// destination edited afterwards moves neither.
///
/// **An omitted key, never an explicit `null`.** A legacy inline-`archive` run
/// has no destination and no `selection`, and a merge patch carrying
/// `"destination": null` would be this controller asserting the absence on
/// every pass of every legacy object. The absence is the object's, not a value
/// this patch owns; `Backup.spec` is immutable, so neither key can ever need
/// clearing.
#[must_use]
pub fn execution_status_patch(frozen: &FrozenInputs) -> Value {
    let mut status = serde_json::Map::new();
    status.insert("execution".to_string(), json!(frozen.status()));
    if let Some(selection) = frozen.inputs.selection.as_ref() {
        status.insert("selection".to_string(), json!(selection.status()));
    }
    if let Some(destination) = frozen.inputs.destination.as_ref() {
        status.insert(
            "destination".to_string(),
            json!(frozen_destination_status(destination)),
        );
    }
    json!({ "status": Value::Object(status) })
}

/// Whether, and how, this pass observed the runner Job.
#[derive(Clone, Copy, Debug)]
pub enum JobObservation<'a> {
    /// The Job is absent: a Job from frozen inputs is about to be created, so
    /// no earlier provenance finding still describes anything.
    Absent,
    /// The Job exists and is controlled by this `Backup`.
    Present(&'a Job),
    /// The pass ended before the Job was classified (a refusal): earlier
    /// provenance findings are kept as they are.
    NotObserved,
}

/// The legacy runner-argv annotation condition for `backup`, when it carries
/// the annotation.
///
/// The message names the annotation's size and digest and whether it equals
/// the argv this controller derives — never its content, which is
/// client-controlled text.
#[must_use]
pub fn runner_argv_annotation_condition(backup: &Backup, now: DateTime<Utc>) -> Option<Condition> {
    let observed = runner_argv_annotation(backup)?;
    let derived = derived_runner_argv(backup);
    let (reason, message) = match observed {
        RunnerArgvAnnotation::Malformed { bytes, sha256 } => (
            REASON_RUNNER_ARGV_ANNOTATION_MALFORMED,
            format!(
                "the legacy {RUNNER_ARGV_ANNOTATION} annotation ({bytes} bytes, {sha256}) is not a \
                 JSON array of strings and is not executed; the runner argv is derived from \
                 spec.triggeredBy and the server-generated execution identity"
            ),
        ),
        RunnerArgvAnnotation::Parsed {
            bytes,
            sha256,
            argv,
        } => {
            let comparison = match derived {
                Some(derived) if derived == argv => {
                    "it equals the argv derived from spec.triggeredBy and the server-generated \
                     execution identity, which is the argv that runs"
                }
                Some(_) => {
                    "it DIFFERS from the argv derived from spec.triggeredBy and the \
                     server-generated execution identity, and only the derived argv runs"
                }
                None => "this Backup states no runnable execution identity, so nothing runs",
            };
            (
                REASON_RUNNER_ARGV_ANNOTATION_IGNORED,
                format!(
                    "the legacy {RUNNER_ARGV_ANNOTATION} annotation ({bytes} bytes, {sha256}) is \
                     not executed; {comparison}"
                ),
            )
        }
    };
    Some(crate::crds::Condition {
        r#type: CONDITION_RUNNER_ARGV_ANNOTATION_IGNORED.to_string(),
        status: "True".to_string(),
        observed_generation: backup.meta().generation,
        last_transition_time: Some(now),
        reason: Some(reason.to_string()),
        message: Some(message),
    })
}

/// The [`CONDITION_EXECUTION_INPUTS_UNVERIFIED`] condition for an existing Job,
/// when that Job is not known to run this `Backup`'s frozen inputs.
#[must_use]
pub fn execution_inputs_unverified_condition(
    backup: &Backup,
    job: &Job,
    now: DateTime<Utc>,
) -> Option<Condition> {
    let recorded = backup.status.as_ref().and_then(|s| s.execution.as_ref());
    let job_name = job.name_any();
    let (reason, message) = match job_provenance(job, recorded) {
        JobProvenance::Frozen => return None,
        JobProvenance::Legacy => (
            REASON_LEGACY_EXECUTION,
            format!(
                "runner Job {job_name} carries no {INPUTS_SHA256_ANNOTATION} annotation and this \
                 Backup records no status.execution: a controller that predates frozen execution \
                 inputs created it, possibly from the legacy {RUNNER_ARGV_ANNOTATION} annotation. \
                 It is observed to completion and never changed, re-derived or re-executed; create \
                 a new Backup to run with frozen inputs"
            ),
        ),
        JobProvenance::Mismatch { job, recorded } => (
            REASON_JOB_INPUTS_MISMATCH,
            format!(
                "runner Job {job_name} carries execution-inputs digest {} and status.execution \
                 records {}: the Job was not created from this Backup's frozen inputs (for example \
                 by an older controller after a rollback). It is observed to completion and never \
                 changed; create a new Backup to run with verified inputs",
                job.as_deref().unwrap_or("<none>"),
                recorded.as_deref().unwrap_or("<none>")
            ),
        ),
    };
    Some(crate::crds::Condition {
        r#type: CONDITION_EXECUTION_INPUTS_UNVERIFIED.to_string(),
        status: "True".to_string(),
        observed_generation: backup.meta().generation,
        last_transition_time: Some(now),
        reason: Some(reason.to_string()),
        message: Some(message),
    })
}

/// `backup` as this pass observes it: its stored status with the two
/// execution-contract conditions replaced by what this pass computed.
///
/// IN MEMORY ONLY. Patch builders read it so the observation reaches the API
/// server inside the patch the pass sends anyway; whether a patch is sent at
/// all is still decided against the STORED status. `lastTransitionTime` moves
/// only on a transition ([`merge_condition`]), so a steady observation adds no
/// write.
///
/// An existing Job that an older controller created (or that does not match
/// `status.execution`) is described by `ExecutionInputsUnverified` alone: this
/// controller did not derive or ignore anything for that Job, so the
/// annotation condition is not asserted beside it.
#[must_use]
pub fn observed_view(backup: &Backup, job: JobObservation<'_>, now: DateTime<Utc>) -> Backup {
    let unverified = match job {
        JobObservation::NotObserved => None,
        JobObservation::Absent => Some(None),
        JobObservation::Present(job) => {
            Some(execution_inputs_unverified_condition(backup, job, now))
        }
    };
    let foreign_provenance = matches!(unverified, Some(Some(_)));
    let annotation = if foreign_provenance {
        None
    } else {
        runner_argv_annotation_condition(backup, now)
    };

    let stored = backup.status.as_ref().and_then(|s| s.conditions.as_ref());
    let mut conditions: Vec<Condition> = stored.cloned().unwrap_or_default();
    let mut upsert = |r#type: &str, next: Option<Condition>| {
        let merged = next.map(|next| merge_condition(current_condition(stored, r#type), next));
        match (conditions.iter().position(|c| c.r#type == r#type), merged) {
            (Some(at), Some(merged)) => conditions[at] = merged,
            (Some(at), None) => {
                conditions.remove(at);
            }
            (None, Some(merged)) => conditions.push(merged),
            (None, None) => {}
        }
    };
    if let Some(next) = unverified {
        upsert(CONDITION_EXECUTION_INPUTS_UNVERIFIED, next);
    }
    upsert(CONDITION_RUNNER_ARGV_ANNOTATION_IGNORED, annotation);

    if stored.map_or(conditions.is_empty(), |stored| *stored == conditions) {
        return backup.clone();
    }
    let mut view = backup.clone();
    view.status.get_or_insert_with(Default::default).conditions = Some(conditions);
    view
}

/// The `/status` merge patch for a finished Job whose `runner` container
/// terminated.
///
/// `windowCovered` is written **only when the receipt was read** — see
/// [`ArchiveOracle`]. A `None` `covered` omits the key from the merge patch,
/// which means "leave it alone": a run whose receipt could not be fetched must
/// not overwrite a window a previous pass recorded, and must certainly not
/// write a zero one.
/// `#[allow(clippy::too_many_arguments)]`, AND THE REASON IS THE FUNCTION'S
/// WHOLE POINT. This is a PURE patch builder: every parameter is one
/// independent OBSERVATION the reconcile made, and the value of the function
/// is that a test can construct any combination of them and assert the exact
/// bytes that reach `/status`. Bundling them into a struct to satisfy the
/// seven-argument lint would move the combinations into a constructor and
/// change nothing about how many facts the status carries — while making every
/// existing assertion in `tests/{backup,restore}_controller.rs` read one level
/// further from the patch it is about. Task 24 took the eighth argument (the
/// receipt digest on the `Backup` path, the topic-preflight observation on the
/// `Restore` path) and this allow with it.
#[allow(clippy::too_many_arguments)]
#[must_use]
pub fn finished_status_patch(
    backup: &Backup,
    exit_code: i32,
    keys: &EvidenceKeys,
    refusal: Option<&str>,
    orphan: Option<&str>,
    covered: Option<(i64, i64)>,
    receipt_sha256: Option<&str>,
    now: DateTime<Utc>,
) -> Value {
    // TWO VOCABULARIES, TWO FIELDS (errata E5b, review LOW-2). The CONDITION's
    // `reason` is CamelCase, because that is what a `metav1.Condition`'s own
    // validation pattern permits; `exitReason` keeps GC11's wire string, which
    // is what the CRD's field description, `docs/kubernetes.md`'s table and
    // `logweir drill`'s outcome strings all already say. The message carries
    // the wire string too, so an operator reading `kubectl describe` sees both
    // spellings of the one fact in one place.
    let wire_reason = wire_reason_for_exit(exit_code);
    let cond_reason = reason_for_exit(exit_code);
    let (phase, cond_type) = if exit_code == 0 {
        (PHASE_SUCCEEDED, CONDITION_COMPLETE)
    } else {
        (PHASE_FAILED, CONDITION_FAILED)
    };

    let mut conditions = vec![condition(
        backup,
        cond_type,
        "True",
        cond_reason,
        &format!(
            "the runner exited {exit_code} ({wire_reason}); the code was read from \
             status.containerStatuses[name={CONTAINER_NAME}].state.terminated.exitCode"
        ),
        now,
    )];
    // THE EVIDENCE CONDITION EXISTS ONLY AT EXIT 0, AND IS ITS OWN TYPE.
    // Review finding HIGH-2 / errata E5c: this used to append a SECOND
    // `Failed` condition, `status: "False"`, whenever the key lines were
    // absent — which is always for exits 1, 3 and 4, because GC11 says those
    // runs write no artifact. Two conditions sharing a `type` is a malformed
    // status (the array is a map keyed by `type`), and the message was untrue
    // for a refusal besides. So: raised only where the contract promised an
    // artifact, under its own type, in both directions.
    // `a_failed_run_carries_exactly_one_failed_condition` asserts every arm.
    if exit_code == 0 {
        let (status, reason, message) = if keys.complete() {
            (
                "True",
                REASON_EVIDENCE_KEYS_RECORDED,
                "both `receipt-key=` and `sidecar-key=` were read off the pod log and are on \
                 status.evidence",
            )
        } else {
            (
                "False",
                REASON_EVIDENCE_KEYS_UNREADABLE,
                "the pod log did not carry both `receipt-key=` and `sidecar-key=`; neither \
                 evidence key is set, and none was guessed from the backup id",
            )
        };
        conditions.push(condition(
            backup,
            CONDITION_EVIDENCE_RECORDED,
            status,
            reason,
            message,
            now,
        ));
    }

    // The terminal state, when there is one, is the most specific thing known
    // about the run: an orphaned scorecard on exit 4, or the guard's own state
    // on exit 3. Otherwise the GC11 wire reason.
    let exit_reason = orphan.or(refusal).unwrap_or(wire_reason);

    let mut evidence = serde_json::Map::new();
    if let Some(k) = keys.receipt.as_ref() {
        evidence.insert("receiptKey".to_string(), json!(k));
    }
    if let Some(k) = keys.sidecar.as_ref() {
        evidence.insert("sidecarKey".to_string(), json!(k));
    }
    // OMITTED, NEVER NULLED, when the receipt was not read: a merge patch with
    // no key means "leave it alone", and a run whose receipt could not be
    // fetched must not erase a digest a previous pass recorded.
    if let Some(d) = receipt_sha256 {
        evidence.insert("receiptSha256".to_string(), json!(d));
    }

    let mut status = serde_json::Map::new();
    status.insert("phase".to_string(), json!(phase));
    status.insert("exitCode".to_string(), json!(exit_code));
    status.insert("exitReason".to_string(), json!(exit_reason));
    // THE BACKUP ID, AND IT IS THE SAME VALUE THE PLAN ALREADY CARRIES.
    // `BackupStatus.backup_id` was declared on the CRD from the start and
    // nothing wrote it; the one producer, [`plan_backup_id`], put the id into
    // the runner's plan ConfigMap alone (`plan_backup_spec`,
    // `BackupSpec.backup_id`). So the archive prefix a run wrote under was
    // readable from the runner's input and from nowhere on the object — and
    // `ui/pages/restore-wizard.js::initialState` reads `status.backupId` for
    // `fields.backupSetRef`, which the plan grammar requires, so the restore
    // wizard threw before its first step rendered on any real cluster. Task 28
    // found it by walking the page; this is the field it was owed.
    //
    // ONE FUNCTION, TWO CONSUMERS, SO THEY CANNOT DISAGREE. `plan_backup_id`
    // is pure and derives the id from the controller owner's UID and
    // `spec.slot` — NOT from `metadata.name`, which the archive prefix is not.
    //
    // ON THIS PATCH AND NO OTHER. The id names a set in the archive, so it is
    // written by the patch that speaks for a run that REACHED the archive:
    // `running_status_patch` speaks before there is one, and
    // `crashed_status_patch` and `refused_status_patch` speak for runs that
    // never wrote one at all — a run that produced no archive names no set.
    // This is the terminal patch for a run that ran, and it is the one the
    // restore wizard's `Succeeded` rows come from.
    status.insert("backupId".to_string(), json!(plan_backup_id(backup)));
    // THE EXISTING `Verified` CONDITION IS CARRIED FORWARD, and without this
    // line the controller hot-loops: a merge patch REPLACES arrays, so this
    // one would delete the condition the SECOND patch adds, which would re-add
    // it, which would wake this reconciler again. Measured at 20 reconciles
    // per second on the Phase B run. See `verification::carry_verified`.
    let conditions = carry_conditions(
        backup.status.as_ref().and_then(|s| s.conditions.as_ref()),
        conditions,
    );
    status.insert("conditions".to_string(), json!(conditions));
    if !evidence.is_empty() {
        status.insert("evidence".to_string(), Value::Object(evidence));
    }
    if let Some((from_ms, to_ms)) = covered {
        status.insert("windowCovered".to_string(), window_covered(from_ms, to_ms));
    }
    json!({ "status": Value::Object(status) })
}

/// The `/status` merge patch for the crashed-Job case — a Job that finished
/// with **no terminated state for `runner`**.
///
/// `exitCode` IS ABSENT, AND ITS ABSENCE IS THE ASSERTION. A merge patch with
/// no `exitCode` key leaves the field alone, and the field was never set — so
/// the status says "this run has no exit code" rather than fabricating a `1`
/// (indistinguishable from a real operational failure) or a `0` (a green
/// badge for a run that never reported). `exitReason` is
/// [`REASON_OPERATIONAL`]: a run whose code is unrecoverable produced no
/// artifact either, which is exactly what GC11's code 1 means, and the
/// SUB-CASE is the condition's `reason`.
///
/// THE CONDITION ARRAY GOES THROUGH [`crate::verification::carry_verified`],
/// as [`finished_status_patch`]'s does — belt and braces beside the
/// already-terminal guard in `reconcile_backup`. A merge patch REPLACES
/// arrays, so a builder that owns the array owes the parts of it that are not
/// its own; `Verified` is the controller's fact about the run and not this
/// branch's to delete. With the guard in place this branch can no longer be
/// reached by an object that carries one, and a builder whose correctness
/// depends on a caller's guard is one refactor from being wrong again.
#[must_use]
pub fn crashed_status_patch(
    backup: &Backup,
    terminal_state: &str,
    job_name: &str,
    now: DateTime<Utc>,
) -> Value {
    let conditions = carry_conditions(
        backup.status.as_ref().and_then(|s| s.conditions.as_ref()),
        vec![condition(
            backup,
            CONDITION_FAILED,
            "True",
            terminal_state,
            "the Job finished but no container named runner reported a terminated state; \
             the exit code is unrecoverable",
            now,
        )],
    );
    json!({
        "status": {
            "phase": PHASE_FAILED,
            "exitReason": REASON_OPERATIONAL,
            "jobRef": { "name": job_name },
            "conditions": conditions,
        }
    })
}

/// The `/status` merge patch for a run this CONTROLLER refused, before any
/// `POST`.
///
/// TERMINAL, WITH NO `exitCode`, AND NEVER A REQUEUE. Nothing ran, so there is
/// no code to lift and none is invented; `exitReason` is
/// [`REASON_OPERATIONAL`], which is what Global Constraint 11's code 1 means —
/// the run could not be attempted and no artifact was written — and the
/// SUB-CASE is the condition's `reason`, exactly as in
/// [`crashed_status_patch`].
///
/// WHY A STATUS AND NOT AN ERROR. A `BackupError` reaches `error_policy` and
/// becomes a 15-second requeue, which for a refusal that can never succeed is
/// an infinite loop with an EMPTY status: no phase, no condition, an empty
/// `PHASE` column, and nothing for `kubectl describe backup` to say. That is
/// review finding MEDIUM-1, measured on a 64-character `Backup`. A refusal the
/// controller can decide by itself is a terminal answer, and an answer belongs
/// on the object.
///
/// Its condition array goes through [`crate::verification::carry_verified`]
/// for [`crashed_status_patch`]'s reason.
#[must_use]
pub fn refused_status_patch(
    backup: &Backup,
    reason: &str,
    message: &str,
    now: DateTime<Utc>,
) -> Value {
    let conditions = carry_conditions(
        backup.status.as_ref().and_then(|s| s.conditions.as_ref()),
        vec![condition(
            backup,
            CONDITION_FAILED,
            "True",
            reason,
            message,
            now,
        )],
    );
    json!({
        "status": {
            "phase": PHASE_FAILED,
            "exitReason": REASON_OPERATIONAL,
            "conditions": conditions,
        }
    })
}

/// `status.windowCovered`, from a receipt's `covered` block.
///
/// **EPOCH MILLISECONDS, TWO INTEGERS** (interface **I22**), the same shape as
/// `BackupReceipt.covered{from_ms, to_ms}` with `to_ms` EXCLUSIVE — never two
/// RFC 3339 strings. The receipt these two fields mirror carries integers, and
/// a controller that converted between the two representations is a controller
/// that can round a window boundary.
#[must_use]
pub fn window_covered(from_ms: i64, to_ms: i64) -> Value {
    json!({ "fromMs": from_ms, "toMs": to_ms })
}

/// Read `covered{from_ms, to_ms}` out of a receipt document.
///
/// Both keys or nothing: a half-read window is worse than an unread one,
/// because a consumer cannot tell the difference between "the archive covers
/// from here to unknown" and "the archive covers nothing".
#[must_use]
pub fn covered_from_receipt(receipt: &Value) -> Option<(i64, i64)> {
    let covered = receipt.get("covered")?;
    Some((
        covered.get("from_ms")?.as_i64()?,
        covered.get("to_ms")?.as_i64()?,
    ))
}

/// What one reconcile did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackupOutcome {
    /// The Job's name — always the `Backup`'s own name.
    pub job_name: String,
    /// Whether this reconcile created the Job.
    pub created: bool,
    /// The exit code that reached `status.exitCode`, when there was one.
    pub exit_code: Option<i32>,
    /// The terminal state that reached the condition's `reason`, when the
    /// crashed-Job path was taken.
    pub terminal_state: Option<String>,
    /// The two evidence keys, as read.
    pub keys: EvidenceKeys,
    /// Whether the Job was patched with `ttlSecondsAfterFinished`.
    pub ttl_patched: bool,
}

/// Anything that is not an outcome. Requeues; writes nothing.
#[derive(Debug)]
pub enum BackupError {
    /// No `metadata.namespace`. Unreachable from the API server; named rather
    /// than unwrapped.
    NoNamespace(String),
    /// No `metadata.uid`, so no owner reference can be built.
    NoUid(String),
    /// The API server could not be talked to. Requeue.
    Api(kube::Error),
    /// A race the next pass resolves — an object answered `409` to a create
    /// and `404` to the read that followed. Requeue; writes nothing.
    Transient(String),
    /// A TERMINAL REFUSAL THIS CONTROLLER DECIDED BY ITSELF, carrying the
    /// terminal state and the message its condition names.
    ///
    /// NOT A REQUEUE, and that is the whole reason the variant exists. A
    /// refusal over a CEL-immutable spec can never succeed on the next pass,
    /// so requeueing it leaves the CR with an empty status forever (review
    /// finding MEDIUM-1). `reconcile_backup` converts this into a
    /// [`refused_status_patch`] and returns an OUTCOME; it never reaches
    /// `error_policy`.
    Refused(&'static str, String),
}

impl fmt::Display for BackupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoNamespace(name) => {
                write!(f, "the object {name} carries no metadata.namespace")
            }
            Self::NoUid(name) => write!(f, "the object {name} carries no metadata.uid"),
            Self::Api(e) => write!(f, "kubernetes API error: {e}"),
            Self::Transient(message) => write!(f, "transient: {message}"),
            Self::Refused(state, message) => write!(f, "{state}: {message}"),
        }
    }
}

impl std::error::Error for BackupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NoNamespace(_) | Self::NoUid(_) | Self::Transient(_) | Self::Refused(..) => None,
            Self::Api(e) => Some(e),
        }
    }
}

impl From<kube::Error> for BackupError {
    fn from(e: kube::Error) -> Self {
        Self::Api(e)
    }
}

/// `GET` the `KafkaCluster` `spec.sourceRef` names.
///
/// A MISSING REFERENT IS TERMINAL AND NOT A REQUEUE (errata **E5a**).
/// `Backup.spec` is CEL-immutable, so a `sourceRef` that resolves to nothing
/// resolves to nothing on every later pass too; a requeue would leave the CR
/// with an empty status and no explanation, which is exactly the shape review
/// finding MEDIUM-1 was about.
///
/// # Errors
///
/// [`BackupError::Refused`] with [`TERMINAL_STATE_REFERENT_NOT_FOUND`] when
/// the object is absent; [`BackupError::Api`] when the API server could not be
/// asked, which IS transient and IS a requeue.
async fn plan_source_cluster(
    backup: &Backup,
    client: &kube::Client,
    namespace: &str,
) -> Result<KafkaCluster, BackupError> {
    let referent = backup.spec.source_ref.name.clone();
    let clusters: Api<KafkaCluster> = Api::namespaced(client.clone(), namespace);
    clusters
        .get_opt(&referent)
        .await
        .map_err(BackupError::Api)?
        .ok_or_else(|| {
            BackupError::Refused(
                TERMINAL_STATE_REFERENT_NOT_FOUND,
                format!(
                    "spec.sourceRef names the KafkaCluster `{referent}`, which does not exist in \
                     namespace {namespace}; spec is immutable, so this cannot resolve later for \
                     this object"
                ),
            )
        })
}

/// D1 §3.1 rule 2: a scheduled-kind run needs its `BackupSchedule` to exist,
/// with the UID its identity was derived from, **before the freeze**.
///
/// # Why a run has to say this itself now
///
/// Until PLAT-05.2 a scheduled `Backup` carried a controller ownerReference to
/// its schedule, so deleting the schedule garbage-collected every run of it and
/// the question never arose. D1 §5.2 removes that reference to KEEP the
/// history, which means an unfrozen run of a deleted schedule would otherwise
/// go on and start a Job under a policy nobody can look up any more. "Deleting
/// a schedule stops future work" is the rule, and this is where it is enforced.
///
/// **A recreated schedule is a different schedule.** A UID that does not match
/// is `ScheduleNotFound` and not an adoption: two same-named schedules' runs
/// sharing an archive prefix is exactly what the UID is in the reference for.
///
/// **Manual runs never reach here**, even when they copied a schedule's policy
/// (D1 §8.3): a manual run is "run this policy now", and it stays runnable
/// after the schedule it was copied from is gone.
///
/// # Errors
///
/// [`crate::conditions::TERMINAL_STATE_SCHEDULE_NOT_FOUND`] when the schedule
/// is absent or carries another UID; [`BackupError::Api`] when the API server
/// could not be asked, which IS transient and IS a requeue.
async fn require_schedule_for_scheduled_run(
    identity: &backup_execution::ExecutionIdentity,
    client: &kube::Client,
    namespace: &str,
) -> Result<(), BackupError> {
    let Some(schedule) = identity.schedule.as_ref() else {
        return Ok(());
    };
    let schedules: Api<BackupSchedule> = Api::namespaced(client.clone(), namespace);
    let found = schedules
        .get_opt(&schedule.name)
        .await
        .map_err(BackupError::Api)?;
    match found.as_ref().and_then(kube::ResourceExt::uid) {
        Some(uid) if uid == schedule.uid => Ok(()),
        Some(uid) => Err(BackupError::Refused(
            TERMINAL_STATE_SCHEDULE_NOT_FOUND,
            format!(
                "this run's identity is derived from BackupSchedule `{}` UID {}, and the \
                 BackupSchedule of that name in namespace {namespace} has UID {uid}; a schedule \
                 deleted and recreated under the same name is a different schedule and does not \
                 adopt this run",
                schedule.name, schedule.uid
            ),
        )),
        None => Err(BackupError::Refused(
            TERMINAL_STATE_SCHEDULE_NOT_FOUND,
            format!(
                "spec.scheduleRef names the BackupSchedule `{}`, which does not exist in \
                 namespace {namespace}; deleting a schedule stops future work, and this run's \
                 inputs were never frozen. A run whose inputs ARE frozen is not re-checked",
                schedule.name
            ),
        )),
    }
}

/// The selection a previous pass froze for this `Backup`, or `None` when this
/// run has not been frozen yet (and so has a selection to resolve).
///
/// The `status.execution` gate is the same one D1 §3.1 rule 2's referent check
/// uses, and it comes first: a run with no recorded execution has no plan to
/// read, and one extra `GET` per unfrozen pass would be a request made for
/// nothing. See [`backup_execution::stored_selection`] for why a frozen run
/// re-reads rather than re-resolves, and why that does not weaken the
/// stored-plan comparison.
///
/// # Errors
///
/// [`BackupError::Api`] when the API server could not be asked. A `ConfigMap`
/// that is absent, unreadable or `v1` is `Ok(None)`, not an error:
/// [`freeze_execution_inputs`] owns the decision about a missing or
/// unacceptable plan, and owns it with the recorded digest in hand.
async fn frozen_plan(
    backup: &Backup,
    client: &kube::Client,
    namespace: &str,
) -> Result<Option<ConfigMap>, BackupError> {
    if backup
        .status
        .as_ref()
        .and_then(|s| s.execution.as_ref())
        .is_none()
    {
        return Ok(None);
    }
    let maps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    maps.get_opt(&plan_config_map_name(&backup.name_any()))
        .await
        .map_err(BackupError::Api)
}

/// Freeze this run's inputs in the plan ConfigMap, or admit the frozen inputs
/// already there — and return the inputs the Job must be built from.
///
/// CREATE-ONLY. The desired object is `POST`ed; a `409` is decided by reading
/// the existing object and admitting it only through
/// [`verify_frozen_config_map`], which compares it against `desired` — a
/// resolution made NOW from the spec, the referent and this controller's
/// addressing — and against `status.execution` when that is recorded. When
/// `status.execution` is recorded the existing object is read FIRST: the inputs
/// were frozen by an earlier pass, and a ConfigMap that has since disappeared
/// is re-created only when the fresh resolution digests to exactly the recorded
/// value.
///
/// Nothing here patches, replaces or deletes a ConfigMap: a plan another pass
/// may already have mounted is never rewritten.
///
/// THE CONFIGMAP WRITE IS A KUBERNETES API WRITE, NOT AN ARCHIVE WRITE.
/// `scripts/check-no-archive-write.sh` is unaffected: its control-plane token
/// list forbids the writable `Store` constructor and the put family, and
/// anchors `.delete(` to a store-shaped receiver precisely so that an
/// ordinary `api.create(…)` on a ConfigMap is not a hit.
///
/// # Errors
///
/// [`TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT`] for any existing object that is
/// not this run's frozen inputs, or a missing one whose inputs changed;
/// [`BackupError::Transient`] for a `409` followed by a `404`;
/// [`BackupError::Api`] for anything else transient.
async fn freeze_execution_inputs(
    backup: &Backup,
    client: &kube::Client,
    namespace: &str,
    desired: &FrozenInputs,
) -> Result<FrozenInputs, BackupError> {
    let name = backup.name_any();
    let cm_name = plan_config_map_name(&name);
    let uid = backup
        .uid()
        .ok_or_else(|| BackupError::NoUid(name.clone()))?;
    let maps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    let recorded = backup.status.as_ref().and_then(|s| s.execution.as_ref());

    if let Some(recorded) = recorded {
        if let Some(existing) = maps.get_opt(&cm_name).await.map_err(BackupError::Api)? {
            let frozen = verify_frozen_config_map(&existing, backup, desired, Some(recorded))
                .map_err(refused)?;
            debug!(
                backup = %name,
                namespace = %namespace,
                config_map = %cm_name,
                inputs_sha256 = %frozen.sha256,
                "the recorded frozen execution inputs verified against a fresh resolution"
            );
            return Ok(frozen);
        }
        if recorded.inputs_sha256 != desired.sha256 {
            return Err(BackupError::Refused(
                TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT,
                format!(
                    "status.execution records inputs {} in ConfigMap {cm_name}, which no longer \
                     exists, and the inputs resolved now digest to {}; the frozen inputs are not \
                     re-created from a different resolution. Delete this Backup and create a new \
                     one",
                    recorded.inputs_sha256, desired.sha256
                ),
            ));
        }
    }

    let body = inputs_config_map(backup, desired).map_err(refused)?;
    match maps.create(&PostParams::default(), &body).await {
        Ok(created) if has_exact_backup_owner(&created.metadata, &name, &uid) => {
            info!(
                backup = %name,
                namespace = %namespace,
                config_map = %cm_name,
                execution_id = %desired.inputs.execution.id,
                inputs_sha256 = %desired.sha256,
                "froze the execution inputs in the immutable plan ConfigMap the runner Job mounts \
                 at /plan"
            );
            Ok(desired.clone())
        }
        Ok(_) => Err(BackupError::Refused(
            TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT,
            format!(
                "the API create response for ConfigMap {cm_name} did not retain the exact single \
                 owner reference for Backup {name} UID {uid}"
            ),
        )),
        Err(kube::Error::Api(e)) if e.code == 409 => {
            let existing = maps
                .get_opt(&cm_name)
                .await
                .map_err(BackupError::Api)?
                .ok_or_else(|| {
                    BackupError::Transient(format!(
                        "the ConfigMap {cm_name} answered 409 to a create and 404 to the read \
                         that followed; it was deleted concurrently and will be retried"
                    ))
                })?;
            verify_frozen_config_map(&existing, backup, desired, recorded).map_err(refused)
        }
        Err(e) => Err(BackupError::Api(e)),
    }
}

/// `POST` the runner Job, admitting a concurrent winner only when it is
/// controlled by exactly this `Backup`. Returns whether this call created it.
///
/// # Errors
///
/// [`TERMINAL_STATE_JOB_NAME_CONFLICT`] for a create response or a `409` winner
/// without the exact single owner reference; [`BackupError::Transient`] for a
/// `409` followed by a `404`; [`BackupError::Api`] otherwise.
async fn create_runner_job(
    jobs: &Api<Job>,
    backup: &Backup,
    desired: &Job,
) -> Result<bool, BackupError> {
    let name = backup.name_any();
    let namespace = backup.namespace().unwrap_or_default();
    match jobs.create(&PostParams::default(), desired).await {
        Ok(created) if compatible_backup_job(&created, backup) => Ok(true),
        Ok(_) => Err(BackupError::Refused(
            TERMINAL_STATE_JOB_NAME_CONFLICT,
            format!(
                "the API create response for Job {namespace}/{name} did not retain the exact \
                 single owner reference for Backup {namespace}/{name} UID {}",
                backup.uid().unwrap_or_default()
            ),
        )),
        Err(kube::Error::Api(e)) if e.code == 409 => {
            let raced = jobs
                .get_opt(&name)
                .await
                .map_err(BackupError::Api)?
                .ok_or_else(|| {
                    BackupError::Transient(format!(
                        "Job {namespace}/{name} answered 409 to a create and 404 to the read that \
                         followed; it was deleted concurrently and will be retried"
                    ))
                })?;
            if compatible_backup_job(&raced, backup) {
                Ok(false)
            } else {
                Err(BackupError::Refused(
                    TERMINAL_STATE_JOB_NAME_CONFLICT,
                    format!(
                        "Job {namespace}/{name} won a concurrent create but is not controlled by \
                         this Backup's UID {}; it was not adopted",
                        backup.uid().unwrap_or_default()
                    ),
                ))
            }
        }
        Err(e) => Err(BackupError::Api(e)),
    }
}

/// Find the pod the exit code is read from — D-SEAMS **S6**, `SEC-PODLOG`.
///
/// The prefixed selector first, the legacy one as a fallback — see
/// [`JOB_NAME_LABEL_LEGACY`] — and of the pods a selector returns, ONLY one
/// whose **controller** `ownerReference` is a `Job` carrying THIS Job's
/// `metadata.uid` ([`check::pod::is_owned_by_job`]).
///
/// # What this used to do, and why it is gone
///
/// It matched the label, then preferred a pod naming this Job's UID among its
/// owner references — of any kind, controller or not — and otherwise **fell
/// back to the first pod with no Job owner at all**. Both halves are readable
/// by a namespace tenant: `batch.kubernetes.io/job-name` is a plain label, and
/// a pod created by hand has no owner references, so planting a labelled,
/// ownerless pod put its `exitCode`, its `refusal-reason=` and its two
/// evidence keys onto somebody else's `Backup` as soon as the real pod was
/// garbage-collected — which the Job outlives by
/// [`TTL_SECONDS_AFTER_FINISHED`], seven days. There is no fallback now: zero
/// owned pods is "no pod yet", the same state a Job whose pod has not been
/// scheduled is in, and it is handled by the crashed-Job branch that already
/// exists for it.
///
/// A `Backup`'s Job is named after the `Backup`, so a Job deleted and
/// re-created from the same frozen inputs also leaves the label selector
/// matching BOTH the new Job's pod and, until garbage collection finishes, the
/// deleted Job's — the UID check is what separates them.
async fn find_pod(
    client: &kube::Client,
    namespace: &str,
    job_name: &str,
    job_uid: Option<&str>,
) -> Result<check::pod::FoundPod, BackupError> {
    check::pod::find_owned_pod_by_selectors(
        client,
        namespace,
        job_name,
        job_uid,
        &pod_selectors(job_name),
    )
    .await
    .map_err(BackupError::Api)
}

/// Reconcile one `Backup` at the instant `now`.
///
/// # The state machine, one pass per event
///
/// 0. **The object's own name is longer than [`crate::slot::NAME_LIMIT`]** →
///    a TERMINAL status (`phase: Failed`, `exitReason: operational`, condition
///    reason [`TERMINAL_STATE_NAME_TOO_LONG`]) and nothing is created. Before
///    any `POST`, because the Job the API server would refuse is a Job whose
///    pods could never be labelled, and a requeue on a refusal that can never
///    succeed leaves the CR with NO STATUS AT ALL (review MEDIUM-1).
/// 1. **No Job and no terminal status** → derive the run identity
///    ([`execution_identity`]), resolve the inputs against the referenced
///    `KafkaCluster`, FREEZE them in the immutable plan ConfigMap (or verify the
///    frozen inputs already there), record `status.execution`, and only then
///    build the Job from the frozen inputs and `POST` it. Status
///    `phase: Running`, condition `JobCreated`. The same path re-creates a Job
///    that disappeared from a nonterminal `Backup`, from the same frozen inputs.
///    No annotation is read: a legacy `logweir.dev/runner-argv` annotation
///    only raises `RunnerArgvAnnotationIgnored`.
/// 2. **Job exists, not finished** → the Job must be controlled by exactly
///    this `Backup` (else [`TERMINAL_STATE_JOB_NAME_CONFLICT`], never adopted);
///    `phase: Running`, `jobRef` set, and `ExecutionInputsUnverified` when the
///    Job was not created from this object's frozen inputs (an older
///    controller's Job is observed unchanged).
/// 3. **Job finished, `runner` terminated** → read `exitCode` from that
///    container's `state.terminated.exitCode`; set `status.exitCode`,
///    `status.phase`, and a `Complete`/`Failed` condition whose reason is the
///    GC11 wire string. **Then** `get` the pod's log through the `pods/log`
///    subresource, read the FINAL TWO STDOUT LINES by key name, set the two
///    evidence keys — and only after the status patch returns 200, `PATCH` the
///    Job with `ttlSecondsAfterFinished`.
/// 4. **Job finished, no terminated state for `runner`** → the crashed-Job
///    case: a TERMINAL status naming the sub-case, with `exitCode` absent.
/// 5. **Exit 4** additionally consults the presence oracle and may record
///    `OrphanedScorecard`. It does not delete, repair, or render the orphan as
///    a result.
///
/// # No clock read in this function
///
/// `now` is an argument, as in `reconcile_schedule`: the one clock read is in
/// the `kube::runtime` wrapper, before anything is decided, so every
/// assertion below is over a value.
///
/// # Errors
///
/// [`BackupError`] for anything that is not an outcome.
pub async fn reconcile_backup(
    backup: &Backup,
    client: &kube::Client,
    archive: ArchiveOracle<'_>,
    verify: VerifyOracle<'_>,
    now: DateTime<Utc>,
) -> Result<BackupOutcome, BackupError> {
    reconcile_backup_with_runner_image(
        backup,
        client,
        archive,
        verify,
        now,
        &job::RunnerImage::default(),
    )
    .await
}

/// [`reconcile_backup`], with the runner image and pull policy this controller
/// process was handed — Task 33, and Task 37's policy beside it.
///
/// `runner.image` is `None` for the shipped pin `job::RUNNER_IMAGE` and
/// `Some(reference)` for the value `main` read out of `job::RUNNER_IMAGE_ENV`;
/// `runner.image_pull_policy` is `None` for the compiled-in
/// `job::IMAGE_PULL_POLICY` and `Some(policy)` for the value `main` read out of
/// `job::RUNNER_PULL_POLICY_ENV`. The pair becomes
/// `job::RunnerJobSpec::{image, image_pull_policy}` and therefore the image and
/// the pull policy of every Job this reconciler creates, and it changes nothing
/// else about the Job.
///
/// WHY THIS IS A SECOND FUNCTION AND NOT A SIXTH PARAMETER ON
/// [`reconcile_backup`]. Forty-three rows in `tests/backup_controller.rs` and
/// `tests/verification.rs` call that function with route-table doubles, and
/// none of them is about the image: a sixth argument would have written
/// `None` forty-three times and buried the one call site where the answer is
/// not `None` (`reconcile`, below).
pub async fn reconcile_backup_with_runner_image(
    backup: &Backup,
    client: &kube::Client,
    archive: ArchiveOracle<'_>,
    verify: VerifyOracle<'_>,
    now: DateTime<Utc>,
    runner: &job::RunnerImage,
) -> Result<BackupOutcome, BackupError> {
    match reconcile_backup_inner(backup, client, archive, verify, now, runner).await {
        Err(BackupError::Refused(state, message)) => {
            // THE ONE PLACE A SELF-DECIDED REFUSAL IS WRITTEN. Every refusal
            // inside the reconcile is a `?` on `BackupError::Refused`, so the
            // status write cannot be forgotten at one of them — which is how
            // the 64-character `Backup` came to requeue forever with
            // `status: null` (review MEDIUM-1).
            let name = backup.name_any();
            let namespace = backup
                .namespace()
                .ok_or_else(|| BackupError::NoNamespace(name.clone()))?;
            warn!(
                backup = %name,
                namespace = %namespace,
                terminal_state = state,
                reason = %message,
                "refusing this Backup terminally: no runner Job runs for it, and a requeue over an \
                 immutable spec would never succeed"
            );
            if !status_is_terminal(backup) {
                let backups: Api<Backup> = Api::namespaced(client.clone(), &namespace);
                // The annotation observation rides on the refusal too: a
                // hostile annotation on a refused Backup is surfaced, and it
                // is not what refused it.
                let view = observed_view(backup, JobObservation::NotObserved, now);
                patch_status_if_changed(
                    &backups,
                    backup,
                    &name,
                    diagnostics::apply_finished(
                        refused_status_patch(&view, state, &message, now),
                        backup.status.as_ref().and_then(|s| s.progress.as_ref()),
                        now,
                    ),
                )
                .await?;
            }
            Ok(BackupOutcome {
                job_name: name,
                created: false,
                exit_code: None,
                terminal_state: Some(state.to_string()),
                keys: EvidenceKeys::default(),
                ttl_patched: false,
            })
        }
        other => other,
    }
}

/// [`reconcile_backup`]'s body. See that function for the state machine; the
/// split exists so a `BackupError::Refused` raised anywhere below reaches
/// exactly one status write.
async fn reconcile_backup_inner(
    backup: &Backup,
    client: &kube::Client,
    archive: ArchiveOracle<'_>,
    verify: VerifyOracle<'_>,
    now: DateTime<Utc>,
    runner: &job::RunnerImage,
) -> Result<BackupOutcome, BackupError> {
    let name = backup.name_any();
    let namespace = backup
        .namespace()
        .ok_or_else(|| BackupError::NoNamespace(name.clone()))?;
    // The Job is named after the CR, VERBATIM.
    let job_name = name.clone();
    let jobs: Api<Job> = Api::namespaced(client.clone(), &namespace);
    let backups: Api<Backup> = Api::namespaced(client.clone(), &namespace);

    // STEP 0. THE OBJECT'S OWN NAME, BEFORE ANY `POST`. A `Backup` whose name
    // exceeds `slot::NAME_LIMIT` produces a Job the API server refuses
    // (`spec.template.labels: … must be no more than 63 characters`), and the
    // refusal used to become a `BackupError::Api` -> `error_policy` -> a
    // 15-second requeue with `status: null`, FOREVER. Checked here so nothing
    // is created and the answer is on the object — review finding MEDIUM-1,
    // errata E5d. Task 18 guards the scheduled path at name-minting time; this
    // is the same refusal for a `Backup` that reached the reconciler by any
    // other route.
    if name.len() > crate::slot::NAME_LIMIT {
        return Err(BackupError::Refused(
            TERMINAL_STATE_NAME_TOO_LONG,
            format!(
                "the object name is {} characters and the pod label `{JOB_NAME_LABEL}` may carry \
                 at most {}; the Job's pods could not be labelled, so their exit code could \
                 never be read",
                name.len(),
                crate::slot::NAME_LIMIT
            ),
        ));
    }

    // STEP 0b. GLOBAL CONSTRAINT 18(c) RAIL 1, BEFORE ANY `POST`. `topics` is
    // a mandatory NAMED allowlist and a pattern in it is a refusal, not a
    // selector: `orders*` handed to the engine's own selector is "every topic
    // whose name starts with orders", which is the one shape a mandatory
    // allowlist exists to make impossible. The rail is
    // `logweir_core::guard`'s, SHARED and not reimplemented, so the controller
    // and the runner refuse the same six characters — and refusing HERE means
    // the pattern never reaches a rendered document, a ConfigMap or a Job.
    if let Err(entry) = logweir_core::guard::reject_glob_metacharacters(&backup.spec.topics) {
        return Err(BackupError::Refused(
            CONDITION_REASON_GUARD_REFUSED,
            format!(
                "spec.topics names `{entry}`, which carries a glob metacharacter; topics are a \
                 mandatory NAMED allowlist (Global Constraint 18(c) rail 1, guard G-GLOB) and a \
                 pattern is refused rather than expanded"
            ),
        ));
    }

    // STEP 0b'. THE SELECTION SHAPE, BEFORE ANY `POST`, AND FOR THE SAME
    // REASON THE GLOB RAIL IS HERE. D1 §7.1 fixes exactly two shapes; the
    // third — a named allowlist beside a dynamic block — is the one this
    // build would answer WRONG rather than refuse, because nothing in it
    // resolves `allUserTopics` yet, so an operator who asked for whole-cluster
    // coverage would get a two-topic run and no signal.
    //
    // ADMISSION REFUSES IT TOO (`crds::backup::SELECTION_SHAPE_RULE`), and
    // this rail is not redundant with it: an object admitted by an OLDER CRD
    // revision reaches this controller unchecked, and `validate_topic_selection`
    // is the one implementation the scheduler, this reconciler and the API's
    // 422 all share, so the three cannot disagree about what a valid policy is.
    //
    // THE SELECTION HALF ONLY. `deadlineSeconds` is part of the run policy
    // digest but its refusal already belongs to PLAT-06.1's
    // `ExecutionSpecInvalid`, which names the field; moving it here would
    // change the terminal state of a run that already has one.
    //
    // TERMINAL, because `spec` is CEL-immutable: a requeue over a shape that
    // cannot be edited would never succeed.
    //
    // THE SHAPE ONLY, HERE. Which topics a dynamic selection RESOLVES to is a
    // question with a cluster read in it, so it is asked at the freeze
    // boundary below — `resolved_selection` — and not in front of the Job
    // read, where a run whose plan is already frozen would be asked it again.
    let shape = declared_selection_shape(backup)?;

    // STEP 0b''. THE RUN POLICY DIGEST THE OBJECT CARRIES MUST BE THE ONE ITS
    // OWN FIELDS PRODUCE — D1 §3.1 rule 5. AN INTEGRITY CHECK AGAINST BUGS AND
    // NOT A SECURITY BOUNDARY (D1 §8.7): `Backup.spec` is CEL-immutable and
    // this recomputes the digest from the same object, so a mismatch means the
    // control plane copied a schedule's policy and then wrote different
    // fields. Terminal, because nothing can fix it in place.
    if let Err(error) = crate::identity::check_run_policy_digest(backup) {
        return Err(BackupError::Refused(
            error.terminal_state(),
            error.to_string(),
        ));
    }

    let existing = jobs.get_opt(&job_name).await.map_err(BackupError::Api)?;

    // STEP 0c. A JOB THIS BACKUP DOES NOT CONTROL IS NEVER OBSERVED. The Job is
    // named after the `Backup`, so a name alone proves nothing: a Job with no
    // owner, a different `Backup` UID or a second owner could otherwise lift a
    // stranger's exit code and evidence keys onto this object.
    if let Some(job) = existing.as_ref() {
        if !compatible_backup_job(job, backup) {
            if status_is_terminal(backup) {
                return Ok(BackupOutcome {
                    job_name,
                    created: false,
                    exit_code: backup.status.as_ref().and_then(|s| s.exit_code),
                    terminal_state: None,
                    keys: EvidenceKeys::default(),
                    ttl_patched: false,
                });
            }
            return Err(BackupError::Refused(
                TERMINAL_STATE_JOB_NAME_CONFLICT,
                format!(
                    "Job {namespace}/{job_name} already exists but is not controlled by exactly \
                     Backup {namespace}/{name} with UID {}; it was neither observed nor adopted. \
                     Remove the foreign Job and create a new Backup",
                    backup.uid().unwrap_or_default()
                ),
            ));
        }
    }

    // STEP 1. Nothing running and nothing terminal: freeze, record, create.
    let Some(job) = existing else {
        if status_is_terminal(backup) {
            // A finished run whose Job has been garbage-collected. Re-creating
            // it would re-run an archive capture whose receipt is already
            // signed and already in the bucket.
            debug!(
                backup = %name,
                namespace = %namespace,
                "the Job is gone and the status is terminal; nothing to do"
            );
            return Ok(BackupOutcome {
                job_name,
                created: false,
                exit_code: backup.status.as_ref().and_then(|s| s.exit_code),
                terminal_state: None,
                keys: EvidenceKeys::default(),
                ttl_patched: false,
            });
        }
        let view = observed_view(backup, JobObservation::Absent, now);

        // THE RUN IDENTITY, FROM THE TYPED SPEC AND SERVER METADATA ONLY — no
        // annotation, before any referent is read. D1 §3.1's one derivation,
        // which knows all four trigger kinds.
        let identity = execution_identity(backup).map_err(refused)?;

        // D1 §3.1 RULE 2, AND ONLY BEFORE THE FREEZE. A scheduled-kind run
        // needs the `BackupSchedule` its `scheduleRef` names to exist in this
        // namespace with that UID: PLAT-05.2 stops a deleted schedule from
        // garbage-collecting its runs, so "deleting a schedule stops future
        // work" has to be said by the run itself. AFTER the freeze nothing is
        // re-checked — a frozen run executes the policy it copied, and a
        // schedule deleted while its Job runs does not change what that Job is
        // doing. A MANUAL run never requires the schedule, even when it copied
        // one (D1 §8.3): that is the difference between "run this policy now"
        // and "this is the schedule's run".
        if backup
            .status
            .as_ref()
            .and_then(|s| s.execution.as_ref())
            .is_none()
        {
            require_schedule_for_scheduled_run(&identity, client, &namespace).await?;
        }

        // === THE D1 W5 (PLAT-09.2) CALL SITE ===
        //
        // GATED ON THE FREEZE, LIKE THE REFERENT CHECK ABOVE, AND FOR THE SAME
        // REASON. This branch is re-entered AFTER the freeze whenever the Job
        // has gone (garbage collection, a deleted Job, a node that lost it) —
        // `a_nonterminal_backup_whose_job_is_gone_recreates_it_from_the_frozen_inputs`
        // is that pass. A run's topic set is decided ONCE; resolving it again
        // on such a pass asks the cluster a question whose answer has moved on,
        // and `verify_frozen_config_map` would then refuse the run as a
        // `PlanConfigMapConflict` on an archive that may be half written. So
        // when `status.execution` is recorded, the selection is READ BACK from
        // the plan the run was admitted with.
        //
        // **DISCOVERY IS NOT IN FRONT OF THIS GATE.** Only the `AllUserTopics`
        // arm of the inner match — the unfrozen path — resolves anything, and a
        // frozen dynamic run therefore never runs a second discovery Job (D1
        // §12, `a_frozen_dynamic_backup_never_reruns_discovery`);
        // `stored_selection` is what makes that true for free.
        //
        // `discovery_job` is `Some` ONLY on the pass that resolved the names
        // from a runner. It is what D1 §7.2 R9's last line is patched on, after
        // the freeze's status write — never on a pass that read the plan back.
        // THE PLAN THIS RUN WAS ADMITTED WITH, READ ONCE. Both readbacks below
        // come out of it: the topic selection (D1 W5) and the resolved
        // destination (D2 §3.7). One `get` rather than two, and — more to the
        // point — one revision of one object, so the two answers cannot come
        // from different passes.
        let stored_plan = frozen_plan(backup, client, &namespace).await?;
        let stored_destination = stored_plan
            .as_ref()
            .and_then(backup_execution::stored_destination);

        // === THE DESTINATION, BEFORE ANYTHING IS CREATED (D2 §3.6) ===
        //
        // **IN FRONT OF THE SELECTION BLOCK, AND THAT POSITION IS THE CLAIM.**
        // `backup_selection::resolve` CREATES a discovery Job against the
        // production cluster. With the admission below it, a `Backup` naming a
        // destination that does not exist yet ran that Job first and only then
        // held — so "nothing is created while this holds" was false on the
        // dynamic path, and the hold budget (measured from
        // `creationTimestamp`) had already been spent on discovery. Here,
        // nothing at all is created until the destination resolves.
        //
        // **AND IT IS SKIPPED ENTIRELY ONCE THE RUN IS FROZEN.** A pass that
        // re-creates a garbage-collected Job renders from `stored_destination`,
        // never from a fresh resolution: every edit to a `BackupDestination`
        // bumps its generation, the frozen block records the generation, and
        // `verify_frozen_config_map` compares the block whole — so re-resolving
        // would terminate a running backup because somebody rotated a Secret
        // name while it ran. D2 §3.7 says a later edit "cannot affect a created
        // run"; this is what makes that true rather than fail-closed.
        let destination = match &stored_destination {
            Some(_) => None,
            None => match admit_destination(backup, client, &namespace, now).await? {
                DestinationAdmission::NotRequested => None,
                DestinationAdmission::Resolved(resolved) => Some(resolved),
                DestinationAdmission::Holding { reason, message } => {
                    info!(
                        backup = %name,
                        namespace = %namespace,
                        reason,
                        detail = %message,
                        "holding: this Backup's BackupDestination is not usable yet, and no \
                         discovery Job, no plan, no ConfigMap and no runner Job are created \
                         while it is not"
                    );
                    patch_status_if_changed(
                        &backups,
                        backup,
                        &name,
                        destination_hold_patch(&view, reason, &message, now),
                    )
                    .await?;
                    return Ok(BackupOutcome {
                        job_name,
                        created: false,
                        exit_code: None,
                        terminal_state: None,
                        keys: EvidenceKeys::default(),
                        ttl_patched: false,
                    });
                }
            },
        };

        let mut discovery_job: Option<String> = None;
        let selection = match stored_plan
            .as_ref()
            .and_then(backup_execution::stored_selection)
        {
            Some(frozen) => frozen,
            None => match shape {
                SelectionShape::SelectedTopics => ResolvedSelection::named(&backup.spec),
                SelectionShape::AllUserTopics => {
                    // `runner` IS PASSED HERE FOR THE SAME REASON IT IS PASSED
                    // TO `create_runner_job` BELOW: the discovery Job is a Job
                    // this controller creates, so it takes the image and pull
                    // policy this PROCESS was configured with and never the
                    // compile-time pin (defect D1-DISCOVERY-IMAGE).
                    match backup_selection::resolve(backup, client, &namespace, now, runner).await?
                    {
                        // The discovery Job exists and has not produced a
                        // readable result. Nothing else happens this pass; the
                        // reconciler's own 15 s requeue is D1 §7.2 R2's.
                        backup_selection::Resolution::Pending => {
                            return Ok(BackupOutcome {
                                job_name,
                                created: false,
                                exit_code: None,
                                terminal_state: None,
                                keys: EvidenceKeys::default(),
                                ttl_patched: false,
                            })
                        }
                        // TERMINAL, AND THE STATUS IS ALREADY ON THE OBJECT.
                        // `backup_selection` writes it itself so that `Failed`
                        // and `TopicsResolved` land in one array; raising a
                        // `BackupError::Refused` here would have this pass's
                        // conclusion written a second time, from a view that
                        // predates the condition.
                        backup_selection::Resolution::Refused { state } => {
                            return Ok(BackupOutcome {
                                job_name,
                                created: false,
                                exit_code: None,
                                terminal_state: Some(state.to_string()),
                                keys: EvidenceKeys::default(),
                                ttl_patched: false,
                            })
                        }
                        backup_selection::Resolution::Resolved(resolved) => {
                            discovery_job = resolved
                                .selection
                                .discovery
                                .as_ref()
                                .map(|d| d.discovery_job.clone());
                            *resolved
                        }
                    }
                }
            },
        };

        // Resolve once: the plan and credential must describe the same
        // KafkaCluster used by the connection probe, including its Secret.
        let cluster = plan_source_cluster(backup, client, &namespace).await?;
        let desired = match stored_destination {
            // THE READBACK PATH. The bytes beside the snapshot are read back
            // too, so the digest bind in `freeze_with_ca` is evaluated against
            // what a kubelet would mount rather than against what the
            // controller wishes were there.
            Some((snapshot, ca_pem)) => desired_execution_inputs_frozen(
                backup,
                &cluster,
                &selection,
                Some(&snapshot),
                ca_pem,
            )?,
            None => desired_execution_inputs_for_destination(
                backup,
                &cluster,
                &selection,
                destination.as_deref(),
            )?,
        };

        // THE PLAN CONFIGMAP, IN THIS SAME PASS AND BEFORE THE JOB `POST`
        // (errata E5a) — and now the freeze boundary. The Job mounts
        // `<name>-plan` at `/plan`, so a Job created first is a pod that stalls
        // in `ContainerCreating` until its deadline fires — measured live.
        // `the_plan_config_map_is_posted_before_the_job` asserts the order.
        let frozen = freeze_execution_inputs(backup, client, &namespace, &desired).await?;

        // RECORDED BEFORE ANY JOB EXISTS. A pass that stops after this write
        // and before the Job resumes from the same record: the next pass reads
        // the ConfigMap first and verifies it against this digest.
        let recorded = execution_status_patch(&frozen);
        // THE VERSION TRAVELS WITH THE PROJECTION. Two more `/status` writes
        // follow in this same pass and seam S7 preconditions each of them on
        // where the previous one left the object, not on the version the watch
        // delivered — which this write has already superseded.
        let at = patch_status_if_changed(&backups, backup, &name, recorded.clone()).await?;
        let mut stored = with_status_written(backup, &recorded, &at);
        let mut view = with_status_patch(&view, &recorded);

        // D1 §7.2 R9's LAST TWO STEPS, IN THIS ORDER AND ONLY ON THE PASS THAT
        // RESOLVED THE NAMES. `TopicsResolved=True` is written after
        // `status.selection` exists, so the condition is never on an object
        // whose names are not frozen — and the discovery Job's TTL is patched
        // only after THAT write returned 200, because the TTL controller
        // deletes a Job and its pods together and the relay lives on the pod.
        // Both are `?`-propagated, so a failed write leaves the reconcile
        // before the next line.
        if let Some(discovery) = discovery_job.as_deref() {
            let (resolved_patch, resolved_at) =
                backup_selection::record_resolved(&stored, client, &namespace, discovery, now)
                    .await?;
            stored = with_status_written(&stored, &resolved_patch, &resolved_at);
            view = with_status_patch(&view, &resolved_patch);
        }

        if let Some(annotation) = runner_argv_annotation(backup) {
            let (bytes, sha256) = match &annotation {
                RunnerArgvAnnotation::Malformed { bytes, sha256 }
                | RunnerArgvAnnotation::Parsed { bytes, sha256, .. } => (*bytes, sha256.clone()),
            };
            warn!(
                backup = %name,
                namespace = %namespace,
                annotation = RUNNER_ARGV_ANNOTATION,
                annotation_bytes = bytes,
                annotation_sha256 = %sha256,
                "the legacy runner-argv annotation is ignored; the Job runs the argv derived from \
                 the typed spec and the server-generated execution identity"
            );
        }

        let desired_job = runner_job(backup, &cluster, &frozen, runner)?;
        let created = create_runner_job(&jobs, backup, &desired_job).await?;
        info!(
            backup = %name,
            namespace = %namespace,
            job = %job_name,
            created,
            execution_id = %frozen.inputs.execution.id,
            trigger = frozen.inputs.execution.trigger.as_str(),
            inputs_sha256 = %frozen.sha256,
            "the runner Job runs the frozen execution inputs"
        );
        patch_status_if_changed(
            &backups,
            &stored,
            &name,
            running_status_patch(&view, &job_name, now),
        )
        .await?;
        return Ok(BackupOutcome {
            job_name,
            created,
            exit_code: None,
            terminal_state: None,
            keys: EvidenceKeys::default(),
            ttl_patched: false,
        });
    };

    let view = observed_view(backup, JobObservation::Present(&job), now);

    // STEP 2. Running.
    if !job_finished(&job) {
        // D3 §2.3 / §2.4 — PLAT-14.1. The ONE derivation
        // (`weirkeeper::diagnostics`), which is also the only thing on this
        // path that lists events or reads the running pod's log. It answers
        // the question `phase: Running` never could: a `Backup` whose pod sits
        // in `ImagePullBackOff` is `Running` by every field that existed
        // before this block.
        let stored = backup.status.as_ref().and_then(|s| s.progress.as_ref());
        let run = diagnostics::observe(
            client,
            &namespace,
            &job,
            &pod_selectors(&job_name),
            stored,
            now,
        )
        .await
        .map_err(BackupError::Api)?;
        patch_status_if_changed(
            &backups,
            backup,
            &name,
            diagnostics::apply(
                running_status_patch(&view, &job_name, now),
                &diagnostics::Write {
                    derived: &run.derived,
                    progress: &run.progress,
                    stored,
                    conditions: backup.status.as_ref().and_then(|s| s.conditions.as_ref()),
                    generation: backup.meta().generation,
                    // `Backup` HAS NO SCALAR `reason`. Its CRD carries no
                    // REASON printer column and `BackupStatus` no such field;
                    // `Restore`'s does, and review finding M2 is why.
                    scalar_reason: false,
                    now,
                },
            ),
        )
        .await?;
        // FAIL FAST — D3 §2.3, and AFTER the status write above, which is
        // what makes `recorded_terminal_state` able to name the cause on the
        // pass that observes the cancelled Job.
        let failed_fast = diagnostics::fail_fast(
            client,
            &namespace,
            &job,
            &backup.uid().unwrap_or_default(),
            &run,
            stored,
            now,
        )
        .await
        .map_err(BackupError::Api)?;
        return Ok(BackupOutcome {
            job_name,
            created: false,
            exit_code: None,
            terminal_state: failed_fast.map(str::to_string),
            keys: EvidenceKeys::default(),
            ttl_patched: false,
        });
    }

    // STEP 2b. ALREADY TERMINAL: READ NOTHING AND WRITE NOTHING.
    //
    // **PROVEN LIVE** by the Task 24 review, on both kinds, on ordinary paths.
    // A finished `Backup` at `phase: Succeeded`, `exitCode: 0`, a signed
    // receipt in the bucket and `verification.result: Valid` was relabelled
    // `phase: Failed`, `exitReason: operational`, its `[Complete,
    // EvidenceRecorded, Verified]` conditions replaced by a single
    // `Failed/NoExitCode` — by nothing more than one `kubectl delete pod` on
    // its finished runner. The Job outlives its pod by
    // [`TTL_SECONDS_AFTER_FINISHED`] (seven days), and in that window pod GC,
    // an eviction, a node restart or a plain delete is enough; on the
    // `Restore` side a controller RESTART is enough on its own. The exit code
    // was never re-read — it was RE-DERIVED from a pod that no longer exists,
    // and the honest answer to "what did that pod exit with" is the one
    // already on the object.
    //
    // THE SAME RULE THE JOB-ABSENT BRANCH ABOVE ALREADY APPLIES, one branch
    // later: a run that recorded a terminal exit code is finished, and nothing
    // a later pass can observe about its pod is news. `Restore` carries the
    // twin.
    //
    // WHAT THIS GIVES UP, SAID PLAINLY: a pass that wrote the terminal status
    // and then failed on the TTL patch or on the verification patch is not
    // retried by a later pass over the same object — the object is terminal,
    // so every later pass stops here. Both are recoverable by an edit (which
    // is what `Action::await_change` waits for on the `Restore` side) and
    // neither is a falsehood on the object; re-deriving a terminal status from
    // a vanished pod IS one, forever, with no edit able to fix it.
    if status_is_terminal(backup) {
        debug!(
            backup = %name,
            namespace = %namespace,
            job = %job_name,
            "the status is already terminal; the runner's pod is not read again and no patch is \
             sent"
        );
        // D3 §2.7's REPAIR, and the one thing this branch does. The guard
        // above gives up retrying the pass that made the object terminal
        // (see its note), and the TTL is the half of that pass whose loss is
        // silent: a finished Job with no TTL is never collected, so it and
        // its pod sit in somebody's namespace quota forever. No pod is read
        // and no status is written — the Job's own spec is the whole input.
        let ttl_patched =
            diagnostics::repair_ttl(client, &namespace, &job, &backup.uid().unwrap_or_default())
                .await
                .map_err(BackupError::Api)?;
        return Ok(BackupOutcome {
            job_name,
            created: false,
            exit_code: backup.status.as_ref().and_then(|s| s.exit_code),
            terminal_state: None,
            keys: EvidenceKeys::default(),
            ttl_patched,
        });
    }

    let job_uid = job.uid();
    let found = find_pod(client, &namespace, &job_name, job_uid.as_deref()).await?;
    // THE POD AND ITS CODE TRAVEL TOGETHER. Binding the pair is what lets the
    // happy path below hold a `&Pod` instead of an `Option` it has to unwrap
    // at the `pods/log` call — an `unwrap_or_default()` there would have read
    // `GET …/pods//log`, the pod COLLECTION, on any future path that reached
    // it with `None` (review finding R6).
    let terminated = found
        .pod
        .as_ref()
        .and_then(|p| terminated_exit_code(p).map(|code| (p, code)));

    // STEP 4. The crashed-Job case, before the happy path, because the happy
    // path needs a code and this branch is "there is none".
    let Some((pod, exit_code)) = terminated else {
        let terminal_state = if found.contested.is_empty() {
            // D3 §2.2: the four new states REPLACE `NoExitCode` only when the
            // matching diagnostic was recorded BEFORE the Job ended.
            // `crash_terminal_state`'s existing table is otherwise unchanged —
            // a disrupted node is still `DisruptedMidDrill` and an
            // unschedulable pod is still `PodUnschedulable`, both of which are
            // more specific than anything a diagnostic could add.
            let from_pod = crash_terminal_state(found.pod.as_ref());
            if from_pod == TERMINAL_STATE_NO_EXIT_CODE {
                diagnostics::recorded_terminal_state(
                    backup.status.as_ref().and_then(|s| s.progress.as_ref()),
                )
                .unwrap_or(from_pod)
            } else {
                from_pod
            }
        } else {
            TERMINAL_STATE_POD_OWNERSHIP_CONTESTED
        };
        warn!(
            backup = %name,
            namespace = %namespace,
            job = %job_name,
            terminal_state,
            contested = %found.contested.join(","),
            "no exit code could be read for this run: either the Job finished with no terminated \
             state for the runner container, or more than one pod claimed the Job and none was \
             read. A terminal status rather than watching forever, and no invented exit code"
        );
        patch_status_if_changed(
            &backups,
            backup,
            &name,
            diagnostics::apply_finished(
                crashed_status_patch(&view, terminal_state, &job_name, now),
                backup.status.as_ref().and_then(|s| s.progress.as_ref()),
                now,
            ),
        )
        .await?;
        return Ok(BackupOutcome {
            job_name,
            created: false,
            exit_code: None,
            terminal_state: Some(terminal_state.to_string()),
            keys: EvidenceKeys::default(),
            ttl_patched: false,
        });
    };

    // STEP 3. The code is known. Read the log through the `pods/log`
    // subresource — the only route to a runner's stdout, and the RBAC rule
    // that grants it is Task 21's (interface I28, a declared late binding).
    let pod_name = pod.name_any();
    let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace);
    let log = pods
        .logs(&pod_name, &LogParams::default())
        .await
        .map_err(BackupError::Api)?;
    let keys = evidence_keys(&log);
    let refusal = if exit_code == 3 {
        Some(
            refusal_state(&log)
                .unwrap_or_else(|| TERMINAL_STATE_GUARD_REFUSED_UNKNOWN_REASON.to_string()),
        )
    } else {
        None
    };
    // WHICH HANDLE THIS RUN'S EVIDENCE IS READ THROUGH — D2 §3.9, §3.10. A
    // legacy object takes the oracle it has always taken; a destination-backed
    // one takes its OWN destination's read-only handle, or a `NotAttempted`
    // naming why there is none. The global handle is never used for a
    // destination-backed run: it holds a different principal over a different
    // bucket, and an answer from it would read as "no evidence" rather than
    // "wrong bucket" (grounding G2).
    let evidence_from = evidence_source(backup, client, &namespace, now)
        .await
        .map_err(BackupError::Api)?;

    // STEP 5, and interface I22's window, off ONE observation. AWAITED: the
    // real oracle's two `Store` reads happen inside one `spawn_blocking`
    // (interface I13, see `ArchiveOracle`), so this is the one point in the
    // reconcile that yields to the runtime for the archive.
    let observed = match &evidence_from {
        EvidenceSource::GlobalHandle => archive(keys.clone()).await,
        EvidenceSource::Destination(store) => {
            // THE SAME `observe_archive`, ON A DIFFERENT HANDLE. One reader of
            // a receipt, one presence vocabulary, one window extraction — a
            // second implementation for destination-backed runs would be a
            // second answer to "what does this receipt say".
            let handle = Arc::clone(store);
            let keys = keys.clone();
            tokio::task::spawn_blocking(move || observe_archive(&handle, &keys))
                .await
                .ok()
                .flatten()
        }
        // NOTHING WAS READ, AND NOTHING IS GUESSED. No presence, so
        // `orphan_state` gets `None` and no run is called an
        // `OrphanedScorecard` for want of a reader; no `windowCovered`, because
        // the window is the RECEIPT's and no receipt was fetched.
        EvidenceSource::NotAttempted { .. } => None,
    };
    let orphan = orphan_state(exit_code, observed.as_ref().map(|o| o.presence));
    let covered = observed.as_ref().and_then(|o| o.covered);
    let receipt_sha256 = observed.as_ref().and_then(|o| o.receipt_sha256.clone());

    // WARNED ONLY WHERE IT IS NEWS. A refusal (exit 3), an operational failure
    // (1) or a signing failure (4) wrote no artifact BY CONTRACT (GC11), so
    // "no evidence key lines" is the expected shape and a warning about it is
    // a warning about nothing — the same defect as the condition this branch
    // used to raise (errata E5c).
    if !keys.complete() {
        if exit_code == 0 {
            warn!(
                backup = %name,
                namespace = %namespace,
                pod = %pod_name,
                "the runner exited 0 and the pod log did not carry both evidence key lines; \
                 neither key is set and none is guessed"
            );
        } else {
            debug!(
                backup = %name,
                namespace = %namespace,
                pod = %pod_name,
                exit_code,
                "no evidence key lines, which is what Global Constraint 11 says a non-zero exit \
                 writes; no evidence condition is raised"
            );
        }
    }

    // BUILT ONCE AND HELD, because the SECOND patch needs the condition array
    // this one carries: a JSON merge patch REPLACES arrays, and after this
    // PATCH returns, the in-memory `backup` is stale and no longer says what
    // the object says. See `verification::second_patch`.
    let terminal = diagnostics::apply_finished(
        finished_status_patch(
            &view,
            exit_code,
            &keys,
            refusal.as_deref(),
            orphan,
            covered,
            receipt_sha256.as_deref(),
            now,
        ),
        backup.status.as_ref().and_then(|s| s.progress.as_ref()),
        now,
    );
    // WHERE THE OBJECT NOW STANDS: the evidence verdict below is the SECOND
    // write of this pass, and a terminal `Backup` is never reconciled again, so
    // a `409` there would lose the verdict rather than defer it.
    let at = patch_status_if_changed(&backups, backup, &name, terminal.clone()).await?;

    // ONLY NOW. The `?` above is what makes this ordering a guarantee rather
    // than a comment: a status patch that did not return 200 leaves this
    // function before any TTL exists, so pod GC cannot start on a run whose
    // code was never recorded.
    jobs.patch(
        &job_name,
        &PatchParams::default(),
        &Patch::Merge(json!({
            // D3 §2.7, chart value `controller.jobTtlSeconds`. The default IS
            // `TTL_SECONDS_AFTER_FINISHED`, so an installation that configures
            // nothing keeps exactly the behaviour it had.
            "spec": { "ttlSecondsAfterFinished": diagnostics::job_ttl_seconds() }
        })),
    )
    .await
    .map_err(BackupError::Api)?;

    // ===================================================================
    // THE SECOND PATCH — Task 24, interface I21's `Backup` half.
    // ===================================================================
    //
    // AFTER the terminal status write and after the TTL, in a patch of its
    // own, so a verification that fails — or a 500 on this very PATCH — can
    // never prevent the exit code from being recorded. The `?` on the terminal
    // patch above is what makes that an ordering guarantee rather than a
    // comment.
    //
    // ATTEMPTED ONLY WHEN AN ARTIFACT WAS WRITTEN, AND **BOTH KEYS ARE WHAT
    // SAYS SO** — D2 §3.9 step 2, "if both keys are present, choose by
    // `evidenceRead` mode". At exits 1, 3 and 4 the contract says no artifact
    // was written (GC11), the runner prints no key lines, and there is no
    // document to have an opinion about: no verification block is written at
    // all. That absence is the GC11 distinction and it is DELIBERATELY kept.
    //
    // THE DIGEST IS REQUIRED BY THE PATHS THAT VERIFY BYTES, AND BY THEM ONLY.
    // A `NotAttempted` source fetched nothing, so `receipt_sha256` is `None`
    // BY CONSTRUCTION (`observed` is `None` for it, above) — and while the
    // digest was demanded of every source this whole block was skipped for it,
    // so an operator whose destination reads evidence with a grant only a pod
    // may hold (`SecretKeys`, `WorkloadIdentity`) saw NO
    // `status.evidence.verification` at all rather than the honest
    // `NotAttempted` and the sentence naming why. That is defect
    // D2-EVIDENCE-NOTATTEMPTED-UNWRITTEN: the sentence existed and reached
    // nobody. NOTHING IS INVENTED HERE — the only verdict writable without
    // bytes is `NotAttempted`, and `Valid`/`Invalid`/`Untrusted` still come
    // only from a verifier that read the document.
    let reference = match (
        keys.receipt.as_deref(),
        keys.sidecar.as_deref(),
        receipt_sha256.as_deref(),
    ) {
        (Some(payload_key), Some(sidecar_key), Some(digest)) => Some(EvidenceRef {
            // PLAT-19.1: trust is resolved PER NAMESPACE, and this is
            // `metadata.namespace` read off the object being reconciled —
            // never a name the subject supplied.
            namespace: namespace.to_string(),
            payload_key: payload_key.to_string(),
            payload_sha256: digest.to_string(),
            sidecar_key: sidecar_key.to_string(),
            payload_type: logweir_verify::PAYLOAD_TYPE_BACKUP_RECEIPT,
        }),
        _ => None,
    };
    let verdict = match (&evidence_from, reference) {
        // THE DECISION IS ALREADY MADE AND IT IS RECORDED. `evidence_source`
        // answered with the reason there is no reader for this run's evidence;
        // the guard is `keys.complete()` and not the digest, because the
        // question this arm answers is "was a receipt written", which the keys
        // say and the digest — which only a fetch produces — cannot.
        (EvidenceSource::NotAttempted { detail }, _) if keys.complete() => {
            Some(crate::verification::VerificationResult::not_attempted(
                logweir_verify::PAYLOAD_TYPE_BACKUP_RECEIPT,
                detail.clone(),
            ))
        }
        (EvidenceSource::GlobalHandle, Some(reference)) => Some(verify(reference).await),
        // THE SAME VERIFIER, ON THE DESTINATION'S OWN HANDLE — D2 §3.9's
        // "no second verification path". `verify_oracle` already takes the
        // store it reads through, resolves this namespace's trust and runs
        // `verify_resolved` inside one `spawn_blocking`; handing it another
        // handle is the whole change. Digest, DSSE and the trust
        // projection are D3 W10's and are not re-decided here.
        (EvidenceSource::Destination(store), Some(reference)) => Some(
            crate::verification::verify_oracle(Some(Arc::clone(store)), client.clone())(reference)
                .await,
        ),
        // GC11's no-artifact case, and a fetch that returned no digest: no
        // document, no opinion, no block.
        _ => None,
    };
    if let Some(result) = verdict {
        let current = backup
            .status
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok());
        let block = result.to_status_value(stored_verification(current.as_ref()));
        // THE BADGE IS COMPUTED OVER THE STATUS THAT WILL EXIST, not the one
        // that did: the terminal patch has landed, so `exitCode` is the value
        // it wrote. Interface I21's `Backup` rule reads `exitCode` and there
        // is no `outcome` on this path to read instead.
        let mut projected = terminal
            .pointer("/status")
            .cloned()
            .unwrap_or_else(|| json!({}));
        projected["evidence"] = json!({ "verification": block.clone() });
        let badge = backup_badge(&projected);
        let verified = verified_condition(
            &badge,
            current_condition(
                backup.status.as_ref().and_then(|s| s.conditions.as_ref()),
                crate::conditions::CONDITION_VERIFIED,
            ),
            backup.meta().generation,
            now,
        );
        info!(
            backup = %name,
            namespace = %namespace,
            verification = %result.result,
            matched_key_id = result.matched_key_id.as_deref().unwrap_or("<none>"),
            green = badge.green,
            badge = %badge.label,
            // R2: THIS SENTENCE IS WHAT THE `NotAttempted` ARM CAN SAY TOO.
            // It used to read "weirkeeper verified this Backup's signed
            // receipt with its read-only evidence credential", which was true
            // while the digest fence stood in front of this line and became
            // false the moment a verdict reached without a fetch could get
            // here: on `NotAttempted` nothing was fetched, no credential was
            // used and no signature was checked. An operator greps this
            // sentence to find the runs whose receipt WAS checked, so the
            // verb has to be the one every arm earns. `verification` carries
            // which verdict it was.
            "weirkeeper recorded this Backup's evidence verdict"
        );
        // DEFECT STATUS-RECORDS AND D3 §2.2's `capture`, ON THIS PATCH AND
        // NO OTHER, AND ONLY ON `Valid`. Both are copied out of the receipt
        // bytes this reconcile fetched — the same bytes whose digest is on
        // `status.evidence.receiptSha256` and whose signature `verify` has
        // just checked against this namespace's trust. "From the verified
        // receipt" is therefore literal: an `Invalid`, `Untrusted` or
        // `NotAttempted` verdict writes NEITHER field, so a count on a
        // `Backup` is a count some key this installation accepts attested to.
        //
        // The asymmetry with `windowCovered` — written on the terminal patch,
        // before verification — is deliberate and is not widened here.
        // `windowCovered` predates the trust work and changing when it is
        // written would change the meaning of a field other code already
        // reads (`orphan_state`'s siblings, the protection evaluation).
        let mut evidence_patch = second_patch(
            &conditions_in(&terminal),
            verified,
            crate::verification::verification_patch_value(block),
        );
        if result.result == VerificationVerdict::Valid {
            if let Some(status) = evidence_patch
                .get_mut("status")
                .and_then(Value::as_object_mut)
            {
                if let Some(records) = observed.as_ref().and_then(|o| o.records) {
                    status.insert("records".to_string(), json!(records));
                }
                if let Some((started, finished)) = observed.as_ref().and_then(|o| o.capture) {
                    status.insert(
                        "capture".to_string(),
                        json!({ "startedAt": started, "finishedAt": finished }),
                    );
                }
            }
        }
        patch_status_at(&backups, backup, &name, &at, evidence_patch).await?;
    }

    info!(
        backup = %name,
        namespace = %namespace,
        job = %job_name,
        exit_code,
        exit_reason = wire_reason_for_exit(exit_code),
        receipt_key = keys.receipt.as_deref().unwrap_or("<unread>"),
        sidecar_key = keys.sidecar.as_deref().unwrap_or("<unread>"),
        "the runner finished; the exit code and the evidence keys are on the status and the Job \
         now has a TTL"
    );

    Ok(BackupOutcome {
        job_name,
        created: false,
        exit_code: Some(exit_code),
        terminal_state: orphan.map(str::to_string).or(refusal),
        keys,
        ttl_patched: true,
    })
}

/// The `kube::runtime` reconcile entry point.
///
/// THE ONE CLOCK READ IN THIS FILE IS HERE.
///
/// # The archive oracle, built per reconcile over a handle built once
///
/// [`super::Context::archive`] is the controller's ONE read-only `Arc<Store>`,
/// constructed in `main` before the tokio runtime exists — interface **I13**.
/// The closure below is cheap (an `Arc` clone) and the HANDLE is not rebuilt:
/// a `Store` constructor drives its own runtime, so building one here would
/// both panic and discard the connection pool on every reconcile.
///
/// `None` — a controller with no `LOGWEIR_ARCHIVE_URL` — takes
/// [`unobserved_archive`]'s answer through the same shape: the `?` inside the
/// future is what turns "no handle" into NOT OBSERVED, so `windowCovered` is
/// omitted and no run is ever called an `OrphanedScorecard` for want of a
/// credential. See [`ArchiveOracle`] for why `None` is the only honest value
/// and why the oracle is async.
async fn reconcile(backup: Arc<Backup>, ctx: Arc<Context>) -> Result<Action, BackupError> {
    let archive = ctx.archive.clone();
    let oracle = move |keys: EvidenceKeys| -> BoxFuture<'static, Option<ArchiveObservation>> {
        let handle = archive.clone();
        Box::pin(async move {
            let handle = handle?;
            // ONE `spawn_blocking`, TWO `get`s — interface I13. `Store` drives
            // its own current-thread runtime and this task is already on one.
            tokio::task::spawn_blocking(move || observe_archive(&handle, &keys))
                .await
                .ok()
                .flatten()
        })
    };
    let verify = crate::verification::verify_oracle(ctx.archive.clone(), ctx.client.clone());
    reconcile_backup_with_runner_image(
        &backup,
        &ctx.client,
        &oracle,
        &verify,
        Utc::now(),
        &ctx.runner_image,
    )
    .await?;
    Ok(Action::requeue(std::time::Duration::from_secs(
        REQUEUE_SECS,
    )))
}

/// Requeue on an error, naming it. Never a panic and never a drop.
fn error_policy(backup: Arc<Backup>, err: &BackupError, _ctx: Arc<Context>) -> Action {
    warn!(
        backup = %backup.name_any(),
        error = %err,
        "backup reconcile failed; requeueing"
    );
    Action::requeue(std::time::Duration::from_secs(REQUEUE_SECS))
}

// ---------------------------------------------------------------------------
// The re-trust trigger — PLAT-19.1, D3 §7.4 "re-evaluation without re-fetching"
// ---------------------------------------------------------------------------

/// Every Backup a `TrustPolicy` event could change the verdict of.
///
/// # No LIST, and that is the whole design
///
/// The obvious shape is "one paginated LIST per bound namespace on every
/// policy event". This controller already RUNS a watch over every Backup in
/// the cluster — that is what `Controller::new` is — so `Controller::store()`
/// is the same index, already paid for, already warm, and cannot be staler
/// than the event being mapped. The trigger therefore costs **zero** API
/// calls and is bounded by the objects this controller holds rather than by
/// the cluster.
///
/// # It over-approximates on purpose
///
/// [`crate::trust::may_govern`] treats a `default: true` policy as claiming
/// every namespace, although resolution would hand an explicitly-named
/// namespace to its own policy instead. Enqueuing an object the policy does
/// not govern costs one re-derivation that writes nothing
/// (`verification::retrust` returns `None` for an unchanged block, erratum
/// **E11(d)**); failing to enqueue one leaves a revoked key green until
/// something else happens to reconcile it. The two errors are not symmetric.
fn policy_targets(
    objects: &reflector::Store<Backup>,
    scopes: &crate::trust::PolicyScopeMemory,
    policy: &crate::crds::trust_policy::TrustPolicy,
) -> Vec<ObjectRef<Backup>> {
    // THE UNION OF BEFORE AND AFTER. A `watches` mapper is handed only the
    // NEW object, so an edit that NARROWS — a namespace removed, `default`
    // cleared — would otherwise enqueue nothing in the namespace it just
    // stopped governing, which is the one edit that certainly changed that
    // namespace's resolution. See `trust::PolicyScopeMemory`.
    let scope = scopes.observe(policy);
    crate::verification::targets_in_scope(objects.state(), &scope)
}

/// [`reconcile`], with the re-trust pass in front of it.
///
/// # Why this is in FRONT and not inside
///
/// Because a terminal object short-circuits: [`reconcile_backup_inner`]
/// returns long before the verification block is written, which is what keeps
/// a finished run quiet (D3 §1) and is also why "force a reconcile" was never
/// a way to re-apply a revocation. The re-trust pass is the other half of that
/// rule — it re-derives the verdict from what is already ON the status, with
/// no storage read and no signature check — so it belongs where a terminal
/// object still reaches it.
///
/// It runs only when the object already carries a `matchedKeyId`
/// ([`crate::verification::has_trust_verdict`]) and the policy reflector has
/// synced. Both guards are about cost and correctness at once: an object with
/// no verdict has nothing to re-decide, and an unsynced store looks like a
/// cluster with no `TrustPolicy` at all, which would resolve every namespace
/// to the legacy roster and write a verdict nobody asked for.
async fn reconcile_with_trust(
    backup: Arc<Backup>,
    ctx: Arc<Context>,
    policies: reflector::Store<crate::crds::trust_policy::TrustPolicy>,
    synced: Arc<AtomicBool>,
) -> Result<Action, BackupError> {
    if synced.load(Ordering::Relaxed) {
        let value = serde_json::to_value(&*backup).ok();
        let status = value.as_ref().and_then(|v| v.get("status"));
        if crate::verification::has_trust_verdict(status) {
            if let Some(namespace) = backup.namespace() {
                let snapshot: Vec<crate::crds::trust_policy::TrustPolicy> =
                    policies.state().into_iter().map(|p| (*p).clone()).collect();
                let resolution =
                    crate::trust::resolve_with(&snapshot, &ctx.client, &namespace).await?;
                let api: Api<Backup> = Api::namespaced(ctx.client.clone(), &namespace);
                // ONE BOUNDED RE-READ FOR A STATUS THAT PREDATES `signedAt`
                // (TRUST-UPGRADE-SIGNEDAT). It happens only for a block that
                // carries a matched key, no signing time and no `trust` at all
                // — a pre-PLAT-19.1 write — and it goes through THE SAME
                // evidence path the original verdict came from, so a
                // destination-backed run reads its own bucket and one whose
                // grant only a pod may hold reads nothing and says so.
                let signing_time = match crate::verification::signing_time_need(status, Utc::now())
                {
                    crate::verification::ReadPlan::None => {
                        crate::verification::SigningTime::NotNeeded
                    }
                    // THE BACKOFF SHORT-CIRCUITS BEFORE `evidence_source`, so a
                    // deferred pass costs neither a `Store::get` NOR the
                    // destination read that resolving the handle would need.
                    crate::verification::ReadPlan::Deferred => {
                        crate::verification::SigningTime::Deferred
                    }
                    crate::verification::ReadPlan::Read(need) => {
                        // A FAILED READ IS NOT A FAILED RECONCILE — review
                        // finding **F6**. `?` here aborted the pass before
                        // `apply_retrust` ran, so a kube API blip delayed a
                        // revocation on exactly the objects this hook is about.
                        // The error becomes the reason no read was attempted,
                        // and the verdict is still re-derived.
                        let (handle, unread) =
                            match evidence_source(&backup, &ctx.client, &namespace, Utc::now())
                                .await
                            {
                                Ok(EvidenceSource::GlobalHandle) => (ctx.archive.clone(), None),
                                Ok(EvidenceSource::Destination(store)) => (Some(store), None),
                                Ok(EvidenceSource::NotAttempted { detail }) => (None, Some(detail)),
                                Err(e) => (
                                    None,
                                    Some(crate::verification::evidence_path_unreadable(&e)),
                                ),
                            };
                        crate::verification::recover_signing_time(handle, unread, need).await
                    }
                };
                crate::verification::apply_retrust(
                    &api,
                    &*backup,
                    &resolution,
                    crate::verification::backup_badge,
                    Utc::now(),
                    &signing_time,
                )
                .await?;
            }
        }
    }
    reconcile(backup, ctx).await
}

/// Run the `Backup` controller until the process ends.
///
/// `Api::all`, and it `owns` the Jobs it creates so a pod terminating wakes
/// this reconciler through its Job rather than only on the requeue timer —
/// which is what keeps the gap between "the runner exited" and "the exit code
/// is on the status" short enough that a TTL is never the thing that closes
/// it.
///
/// `archive` is the controller's ONE read-only archive handle, built once in
/// `main` before the tokio runtime exists and shared as `Arc<Store>` —
/// interface **I13**, the same argument `backup_schedule::controller` takes.
/// `None` is a controller with no archive configured: it writes no
/// `windowCovered` and records no `OrphanedScorecard`, which is the truthful
/// answer and not a degraded one (see [`ArchiveOracle`]).
///
/// `runner_image` is Task 33's runtime override and Task 37's beside it, read
/// once each in `main` out of `job::RUNNER_IMAGE_ENV` and
/// `job::RUNNER_PULL_POLICY_ENV`: an unset field is the compiled-in constant,
/// and a set one is what the operator's install asked for — the image this
/// cluster's nodes actually hold, and the policy that makes that reference
/// resolvable (a LOADED tag must not be pulled; a `latest` tag must be).
pub fn controller(
    client: kube::Client,
    archive: Option<Arc<Store>>,
    runner_image: job::RunnerImage,
) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<Backup> = Api::all(client.clone());
    let jobs: Api<Job> = Api::all(client.clone());
    let client_for_watch = client.clone();
    let ctx = Arc::new(Context {
        client,
        archive,
        runner_image,
    });
    // ONE reflector over `TrustPolicy`, shared by the trigger and by every
    // resolution this controller performs (review finding F9). A watch is one
    // long-lived connection; a LIST per reconcile is one round trip per object
    // per requeue, and this controller holds every object in the cluster.
    let (policies, policy_writer) = reflector::store::<crate::crds::trust_policy::TrustPolicy>();
    let policy_api: Api<crate::crds::trust_policy::TrustPolicy> = Api::all(client_for_watch);
    let synced = Arc::new(AtomicBool::new(false));
    // ONE memory of what each policy bound last, owned by the mapper.
    let scopes = Arc::new(crate::trust::PolicyScopeMemory::default());
    async move {
        // NOT `wait_until_ready().await` BEFORE STARTING. A cluster whose
        // `trustpolicies` CRD is not installed never syncs, and awaiting here
        // would mean this controller never reconciles anything at all — a
        // startup regression for every install that has not migrated. The flag
        // is the same answer without the hostage: until it is set, the
        // re-trust pass is skipped and everything else runs exactly as before.
        let ready_probe = policies.clone();
        let ready_flag = Arc::clone(&synced);
        tokio::spawn(async move {
            if ready_probe.wait_until_ready().await.is_ok() {
                ready_flag.store(true, Ordering::Relaxed);
            }
        });
        tokio::spawn(
            reflector::reflector(
                policy_writer,
                watcher(policy_api.clone(), watcher::Config::default()),
            )
            .for_each(|_| std::future::ready(())),
        );

        let controller = Controller::new(api, watcher::Config::default());
        let objects = controller.store();
        controller
            .owns(jobs, watcher::Config::default())
            // THE RE-TRUST TRIGGER. A `TrustPolicy` event maps to the objects
            // this controller already holds in the namespaces that policy could
            // govern — see `policy_targets`.
            .watches(policy_api, watcher::Config::default(), move |policy| {
                policy_targets(&objects, &scopes, &policy)
            })
            .run(
                move |object, context| {
                    reconcile_with_trust(object, context, policies.clone(), Arc::clone(&synced))
                },
                error_policy,
                ctx,
            )
            .for_each(|_| std::future::ready(()))
            .await;
    }
}
