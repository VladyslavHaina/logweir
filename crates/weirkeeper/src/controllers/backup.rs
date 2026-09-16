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
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use futures::StreamExt as _;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ConfigMap, Pod};
use kube::api::{Api, ListParams, LogParams, Patch, PatchParams, PostParams};
use kube::runtime::controller::Action;
use kube::runtime::{watcher, Controller};
use kube::{Resource, ResourceExt as _};
use serde_json::{json, Value};
use tracing::{debug, info, warn};

use super::Context;
use crate::backup_execution::{
    self, annotate_runner_job, compatible_backup_job, derived_runner_argv, execution_identity,
    has_exact_backup_owner, inputs_config_map, job_provenance, resolve_inputs,
    runner_argv_annotation, verify_frozen_config_map, ExecutionRefusal, FrozenInputs,
    JobProvenance, RunnerArgvAnnotation, INPUTS_SHA256_ANNOTATION, RUNNER_ARGV_ANNOTATION,
};
use crate::conditions::{
    current_condition, merge_condition, reason_for_exit, status_unchanged, wire_reason_for_exit,
    CONDITION_COMPLETE, CONDITION_EVIDENCE_RECORDED, CONDITION_EXECUTION_INPUTS_UNVERIFIED,
    CONDITION_FAILED, CONDITION_JOB_CREATED, CONDITION_REASON_GUARD_REFUSED,
    CONDITION_RUNNER_ARGV_ANNOTATION_IGNORED, PHASE_FAILED, PHASE_RUNNING, PHASE_SUCCEEDED,
    REASON_EVIDENCE_KEYS_RECORDED, REASON_EVIDENCE_KEYS_UNREADABLE, REASON_JOB_INPUTS_MISMATCH,
    REASON_LEGACY_EXECUTION, REASON_OPERATIONAL, REASON_RUNNER_ARGV_ANNOTATION_IGNORED,
    REASON_RUNNER_ARGV_ANNOTATION_MALFORMED, TERMINAL_STATE_DISRUPTED_MID_DRILL,
    TERMINAL_STATE_GUARD_REFUSED_UNKNOWN_REASON, TERMINAL_STATE_JOB_NAME_CONFLICT,
    TERMINAL_STATE_NAME_TOO_LONG, TERMINAL_STATE_NO_EXIT_CODE, TERMINAL_STATE_ORPHANED_SCORECARD,
    TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT, TERMINAL_STATE_POD_UNSCHEDULABLE,
    TERMINAL_STATE_REFERENT_NOT_FOUND,
};
use crate::crds::backup::Backup;
use crate::crds::kafka_cluster::KafkaCluster;
use crate::crds::Condition;
use crate::job::{self, EnvFromSecret, RunnerJobSpec, RunnerOwner, SecretMount, CONTAINER_NAME};
use crate::verification::{
    backup_badge, conditions_in, second_patch, stored_verification, verified_condition,
    EvidenceRef, VerifyOracle,
};
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
/// THE CONTRACT SAYS "THE FINAL TWO STDOUT LINES", AND THIS IS EIGHT. The two
/// extra reads cost nothing and buy tolerance for a trailing blank line, a
/// `\r\n`, or a shutdown line a future runner appends — none of which changes
/// which line carries which key, because the scan matches on the KEY NAME and
/// not on a position. It is bounded rather than unbounded so a 200 MB log
/// cannot make the scan the expensive part of a reconcile.
pub const KEY_SCAN_TAIL_LINES: usize = 8;

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
    let identity = execution_identity(backup).map_err(refused)?;
    let inputs =
        resolve_inputs(backup, identity, cluster, &archive_addressing_env()).map_err(refused)?;
    FrozenInputs::freeze(inputs).map_err(refused)
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
    let covered = receipt
        .as_deref()
        .and_then(|bytes| serde_json::from_slice::<Value>(bytes).ok())
        .and_then(|doc| covered_from_receipt(&doc));
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
    })
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
        service_account_name: connection.execution.service_account_name.clone(),
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
            env.extend(
                inputs
                    .archive
                    .addressing_env
                    .iter()
                    .map(|v| (v.name.clone(), v.value.clone())),
            );
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

/// Patch `/status` — unless the patch would change nothing.
///
/// THE THIRD RULE OF THE STATUS-WRITE CONTRACT, at this kind's five patch
/// sites. The decision is [`crate::conditions::status_unchanged`]'s and is not
/// re-implemented here; this exists so the five call sites read as one line
/// each and so the skip is impossible to apply at four of them and forget at
/// the fifth.
async fn patch_status_if_changed(
    api: &Api<Backup>,
    backup: &Backup,
    name: &str,
    patch: Value,
) -> Result<(), BackupError> {
    if status_unchanged(
        backup
            .status
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok())
            .as_ref(),
        &patch,
    ) {
        debug!(
            backup = %name,
            "the computed status equals the one on the object; no patch is sent"
        );
        return Ok(());
    }
    api.patch_status(name, &PatchParams::default(), &Patch::Merge(patch))
        .await
        .map_err(BackupError::Api)?;
    Ok(())
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
/// [`CONDITION_RUNNER_ARGV_ANNOTATION_IGNORED`]) and, last,
/// [`crate::conditions::CONDITION_VERIFIED`].
///
/// A MERGE PATCH REPLACES ARRAYS, so a builder that writes `conditions` owes
/// the parts it does not own — the hot loop `verification::carry_verified`
/// documents is the same one a dropped observation would start. The order is
/// fixed (the builder's own, then the two observations, then `Verified`) so a
/// steady object computes the same array on every pass and sends nothing.
#[must_use]
pub fn carry_conditions(
    existing: Option<&Vec<Condition>>,
    mut conditions: Vec<Value>,
) -> Vec<Value> {
    for r#type in [
        CONDITION_EXECUTION_INPUTS_UNVERIFIED,
        CONDITION_RUNNER_ARGV_ANNOTATION_IGNORED,
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
    crate::verification::carry_verified(existing, conditions)
}

/// The `/status` merge patch that records frozen inputs: `status.execution`,
/// and nothing else.
///
/// SENT BEFORE THE JOB IS CREATED, and a merge of one object key: it replaces
/// no array and so cannot drop a condition another writer owns.
#[must_use]
pub fn execution_status_patch(frozen: &FrozenInputs) -> Value {
    json!({ "status": { "execution": frozen.status() } })
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

/// The pod a Job with UID `job_uid` produced, out of a label-selected list.
///
/// A `Backup`'s Job is named after the `Backup`, so a Job deleted and
/// re-created from the same frozen inputs leaves the job-name label selector
/// matching BOTH the new Job's pod and, until garbage collection finishes, the
/// deleted Job's. A pod owned by some OTHER Job is therefore never read: its
/// exit code belongs to a run this Job is not. A pod naming no Job owner at
/// all (a shape the job controller does not produce) is the fallback.
#[must_use]
pub fn select_job_pod(pods: Vec<Pod>, job_uid: Option<&str>) -> Option<Pod> {
    let job_owners = |pod: &Pod| -> Vec<String> {
        pod.metadata
            .owner_references
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter(|owner| owner.kind == "Job")
            .map(|owner| owner.uid.clone())
            .collect()
    };
    if let Some(uid) = job_uid {
        if let Some(at) = pods
            .iter()
            .position(|pod| job_owners(pod).iter().any(|owner| owner == uid))
        {
            return pods.into_iter().nth(at);
        }
    }
    pods.into_iter().find(|pod| job_owners(pod).is_empty())
}

/// Find the pod the exit code is read from.
///
/// The prefixed selector first, the legacy one as a fallback — see
/// [`JOB_NAME_LABEL_LEGACY`]. Of the pods a selector returns, the one
/// [`select_job_pod`] attributes to THIS Job's UID: `backoffLimit: 0` plus
/// `restartPolicy: Never` yields exactly one per Job (verified live), and a Job
/// re-created under the same name must not read its predecessor's pod.
async fn find_pod(
    client: &kube::Client,
    namespace: &str,
    job_name: &str,
    job_uid: Option<&str>,
) -> Result<Option<Pod>, BackupError> {
    let api: Api<Pod> = Api::namespaced(client.clone(), namespace);
    for selector in pod_selectors(job_name) {
        let list = api
            .list(&ListParams::default().labels(&selector))
            .await
            .map_err(BackupError::Api)?;
        if let Some(pod) = select_job_pod(list.items, job_uid) {
            return Ok(Some(pod));
        }
        debug!(
            job = %job_name,
            selector = %selector,
            "no pod of this Job matched this selector; trying the next"
        );
    }
    Ok(None)
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
                    refused_status_patch(&view, state, &message, now),
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
        // annotation, before any referent is read.
        let identity = execution_identity(backup).map_err(refused)?;

        // Resolve once: the plan and credential must describe the same
        // KafkaCluster used by the connection probe, including its Secret.
        let cluster = plan_source_cluster(backup, client, &namespace).await?;
        let desired = FrozenInputs::freeze(
            resolve_inputs(backup, identity, &cluster, &archive_addressing_env())
                .map_err(refused)?,
        )
        .map_err(refused)?;

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
        patch_status_if_changed(&backups, backup, &name, recorded.clone()).await?;
        let stored = with_status_patch(backup, &recorded);
        let view = with_status_patch(&view, &recorded);

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
        patch_status_if_changed(
            &backups,
            backup,
            &name,
            running_status_patch(&view, &job_name, now),
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
        return Ok(BackupOutcome {
            job_name,
            created: false,
            exit_code: backup.status.as_ref().and_then(|s| s.exit_code),
            terminal_state: None,
            keys: EvidenceKeys::default(),
            ttl_patched: false,
        });
    }

    let job_uid = job.uid();
    let pod = find_pod(client, &namespace, &job_name, job_uid.as_deref()).await?;
    let exit_code = pod.as_ref().and_then(terminated_exit_code);

    // STEP 4. The crashed-Job case, before the happy path, because the happy
    // path needs a code and this branch is "there is none".
    let Some(exit_code) = exit_code else {
        let terminal_state = crash_terminal_state(pod.as_ref());
        warn!(
            backup = %name,
            namespace = %namespace,
            job = %job_name,
            terminal_state,
            "the Job finished with no terminated state for the runner container; writing a \
             terminal status rather than watching forever, and inventing no exit code"
        );
        patch_status_if_changed(
            &backups,
            backup,
            &name,
            crashed_status_patch(&view, terminal_state, &job_name, now),
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
    let pod_name = pod
        .as_ref()
        .map(kube::ResourceExt::name_any)
        .unwrap_or_default();
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
    // STEP 5, and interface I22's window, off ONE observation. AWAITED: the
    // real oracle's two `Store` reads happen inside one `spawn_blocking`
    // (interface I13, see `ArchiveOracle`), so this is the one point in the
    // reconcile that yields to the runtime for the archive.
    let observed = archive(keys.clone()).await;
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
    let terminal = finished_status_patch(
        &view,
        exit_code,
        &keys,
        refusal.as_deref(),
        orphan,
        covered,
        receipt_sha256.as_deref(),
        now,
    );
    patch_status_if_changed(&backups, backup, &name, terminal.clone()).await?;

    // ONLY NOW. The `?` above is what makes this ordering a guarantee rather
    // than a comment: a status patch that did not return 200 leaves this
    // function before any TTL exists, so pod GC cannot start on a run whose
    // code was never recorded.
    jobs.patch(
        &job_name,
        &PatchParams::default(),
        &Patch::Merge(json!({
            "spec": { "ttlSecondsAfterFinished": TTL_SECONDS_AFTER_FINISHED }
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
    // ATTEMPTED ONLY WHEN THERE IS SOMETHING TO VERIFY. Both keys and the
    // digest have to be present: at exits 1, 3 and 4 the contract says no
    // artifact was written (GC11), so there is no document to have an opinion
    // about and no verification block is written at all.
    if let (Some(payload_key), Some(sidecar_key), Some(digest)) = (
        keys.receipt.as_deref(),
        keys.sidecar.as_deref(),
        receipt_sha256.as_deref(),
    ) {
        let result = verify(EvidenceRef {
            payload_key: payload_key.to_string(),
            payload_sha256: digest.to_string(),
            sidecar_key: sidecar_key.to_string(),
            payload_type: logweir_verify::PAYLOAD_TYPE_BACKUP_RECEIPT,
        })
        .await;
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
            "weirkeeper verified this Backup's signed receipt with its read-only evidence \
             credential"
        );
        patch_status_if_changed(
            &backups,
            backup,
            &name,
            second_patch(&conditions_in(&terminal), verified, block),
        )
        .await?;
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
    let ctx = Arc::new(Context {
        client,
        archive,
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
