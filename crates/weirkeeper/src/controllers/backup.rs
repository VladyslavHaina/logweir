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
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::api::{Api, ListParams, LogParams, Patch, PatchParams, PostParams};
use kube::runtime::controller::Action;
use kube::runtime::{watcher, Controller};
use kube::{Resource, ResourceExt as _};
use serde_json::{json, Value};
use tracing::{debug, info, warn};

use super::Context;
use crate::conditions::{
    reason_for_exit, wire_reason_for_exit, CONDITION_COMPLETE, CONDITION_EVIDENCE_RECORDED,
    CONDITION_FAILED, CONDITION_JOB_CREATED, CONDITION_REASON_GUARD_REFUSED, PHASE_FAILED,
    PHASE_RUNNING, PHASE_SUCCEEDED, REASON_EVIDENCE_KEYS_RECORDED, REASON_EVIDENCE_KEYS_UNREADABLE,
    REASON_OPERATIONAL, TERMINAL_STATE_ARCHIVE_URL_UNREADABLE,
    TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE, TERMINAL_STATE_DISRUPTED_MID_DRILL,
    TERMINAL_STATE_GUARD_REFUSED_UNKNOWN_REASON, TERMINAL_STATE_NAME_TOO_LONG,
    TERMINAL_STATE_NO_EXIT_CODE, TERMINAL_STATE_ORPHANED_SCORECARD,
    TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT, TERMINAL_STATE_POD_UNSCHEDULABLE,
    TERMINAL_STATE_REFERENT_NOT_FOUND,
};
use crate::crds::backup::Backup;
use crate::crds::kafka_cluster::{AuthMode, KafkaCluster};
use crate::job::{self, EnvFromSecret, RunnerJobSpec, RunnerOwner, SecretMount, CONTAINER_NAME};
use logweir_core::spec::AuthSpec;
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

/// The `backup_id` the rendered document names.
///
/// `<owner uid>-<slot>` for a scheduled `Backup`, which is byte-identical to
/// the value Task 18 wrote into the runner argv's override flag
/// (`slot::backup_id_for(<schedule uid>, <slot>)`, and the controller owner
/// reference on a scheduled `Backup` IS the schedule) — asserted by
/// `the_rendered_plan_parses_back_as_a_backup_spec`, which reads the flag's
/// value out of the argv. The flag is not named in this file: this crate's own
/// guard (`tests/schedule_controller.rs::the_backup_id_override_is_passed_not_defined`)
/// permits the token on code lines in `backup_schedule.rs` alone, and a test
/// is not a code line under `src/`.
///
/// THE VALUE IS OVERRIDDEN AT RUN TIME ANYWAY. `logweir backup run` takes
/// `args.backup_id_override` in preference to `spec.backup_id` (interface
/// **I10**, `crates/logweir/src/backup/mod.rs:333-337`), so the field this
/// renders is load-bearing only for a `Backup` whose argv carries no override
/// — a hand-written one. Deriving it rather than defaulting it keeps the two
/// halves from disagreeing about which archive prefix a run writes under,
/// which is the colliding-`backup_id` case that leaves a partial archive
/// behind.
#[must_use]
pub fn plan_backup_id(backup: &Backup) -> String {
    let owner = backup
        .owner_references()
        .iter()
        .find(|o| o.controller == Some(true))
        .map(|o| o.uid.clone());
    match (owner, backup.spec.slot.as_deref()) {
        (Some(uid), Some(slot)) => crate::slot::backup_id_for(&uid, slot),
        // A `Backup` with no controller owner or no slot is a hand-written
        // one. Its own UID is unique per object per cluster, which is the
        // whole argument `slot::backup_id_for` makes for using a UID.
        _ => backup.uid().unwrap_or_else(|| backup.name_any()),
    }
}

/// The typed `BackupSpec` the runner's `--spec` file carries, rendered from
/// `Backup.spec` and the source `KafkaCluster`.
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
/// `weirkeeper` may link `logweir-core`: it is the PURE layer, it declares no
/// `logweir-evidence` edge, and `check-one-signer.sh` / `check-pure-core.sh`
/// are unchanged by the edge (which Task 16 already took).
///
/// # What each field comes from
///
/// * `source.bootstrap_servers`, `source.auth` — the `KafkaCluster`
///   `spec.sourceRef` names. **Never from `Backup.spec`**, which carries
///   neither: the address is the referent's, and `sourceRef` is CEL-immutable,
///   so the pair cannot drift after an approval binds them.
/// * `source.topics` — `Backup.spec.topics` **verbatim**, behind
///   [`logweir_core::guard::reject_glob_metacharacters`]. See
///   [`reconcile_backup`] step 0 for why the rail refuses before any `POST`
///   rather than here.
/// * `storage` — `Backup.spec.archive.url`, through
///   [`crate::retention::storage_url_for`], the same parser the controller's
///   own read-only handle is built with, so the runner and the controller
///   cannot disagree about where the archive is.
/// * `backup_id` — [`plan_backup_id`].
/// * `backup` — `BackupSettings::default()`. `Backup.spec` has no tunables and
///   inventing CRD fields for them is Task 15b's decision, not this one's; the
///   defaults are `logweir-core`'s single source for them.
///
/// # Errors
///
/// [`BackupError::Refused`] with a terminal state, for the two things that can
/// be wrong with a spec that will never change: an unreadable `archive.url`
/// and a `scramSha512` cluster with no username.
pub fn plan_backup_spec(
    backup: &Backup,
    cluster: &KafkaCluster,
) -> Result<logweir_core::spec::BackupSpec, BackupError> {
    let name = backup.name_any();
    let auth = match cluster.spec.auth.mode {
        AuthMode::Plaintext => AuthSpec::Plaintext,
        AuthMode::ScramSha512 => {
            let username = cluster
                .spec
                .auth
                .username
                .clone()
                .filter(|u| !u.is_empty())
                .ok_or_else(|| {
                    BackupError::Refused(
                        TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
                        format!(
                            "the KafkaCluster {} names auth mode scramSha512 and no auth.username, so the plan document cannot name the identity the run will present",
                            cluster.name_any()
                        ),
                    )
                })?;
            AuthSpec::ScramSha512 {
                username,
                tls: cluster.spec.auth.tls,
            }
        }
    };
    let storage = crate::retention::storage_url_for(&backup.spec.archive.url).map_err(|e| {
        BackupError::Refused(
            TERMINAL_STATE_ARCHIVE_URL_UNREADABLE,
            format!("the archive URL on {name} is not an object-store location: {e}"),
        )
    })?;
    Ok(logweir_core::spec::BackupSpec {
        source: logweir_core::spec::BackupSourceSpec {
            bootstrap_servers: cluster.spec.bootstrap_servers.clone(),
            auth,
            topics: backup.spec.topics.clone(),
        },
        storage,
        backup_id: plan_backup_id(backup),
        backup: logweir_core::spec::BackupSettings::default(),
    })
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

/// The plan ConfigMap object, with exactly two keys.
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
/// NO CREDENTIAL IN EITHER KEY. `AuthSpec` carries a username and never a
/// password at any variant, and the object-store credential reaches the runner
/// as `secretKeyRef` env, so a ConfigMap — an object with no encryption at
/// rest and a much wider read surface than a Secret — carries nothing but the
/// two documents.
///
/// # Errors
///
/// Whatever [`plan_backup_spec`] refuses, plus a `BackupError` for an object
/// with no namespace or UID (both unreachable from the API server, both named
/// rather than unwrapped).
pub fn plan_config_map(backup: &Backup, cluster: &KafkaCluster) -> Result<ConfigMap, BackupError> {
    let name = backup.name_any();
    let namespace = backup
        .namespace()
        .ok_or_else(|| BackupError::NoNamespace(name.clone()))?;
    let uid = backup
        .uid()
        .ok_or_else(|| BackupError::NoUid(name.clone()))?;

    let spec_yaml = serde_yaml::to_string(&plan_backup_spec(backup, cluster)?).map_err(|e| {
        BackupError::Refused(
            TERMINAL_STATE_ARCHIVE_URL_UNREADABLE,
            format!("the rendered backup spec for {name} does not serialise: {e}"),
        )
    })?;
    let allowed_json =
        serde_json::to_string_pretty(&plan_allowed_clusters(cluster)).map_err(|e| {
            BackupError::Refused(
                TERMINAL_STATE_ARCHIVE_URL_UNREADABLE,
                format!("the rendered cluster allowlist for {name} does not serialise: {e}"),
            )
        })?;

    Ok(ConfigMap {
        metadata: ObjectMeta {
            name: Some(plan_config_map_name(&name)),
            namespace: Some(namespace),
            owner_references: Some(vec![OwnerReference {
                api_version: Backup::api_version(&()).to_string(),
                kind: Backup::kind(&()).to_string(),
                name,
                uid,
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]),
            ..ObjectMeta::default()
        },
        data: Some(
            [
                (PLAN_SPEC_KEY.to_string(), spec_yaml),
                (PLAN_ALLOWED_CLUSTERS_KEY.to_string(), allowed_json),
            ]
            .into_iter()
            .collect(),
        ),
        ..ConfigMap::default()
    })
}

/// Whether `existing` is owned by the `Backup` with this UID.
///
/// The 409 discriminator. `controller: true` and the UID, not the name: a
/// `Backup` deleted and recreated under the same name is a DIFFERENT object,
/// and its plan is rendered from a spec that may name a different source
/// cluster.
#[must_use]
pub fn is_owned_by(existing: &ConfigMap, backup_uid: &str) -> bool {
    existing
        .metadata
        .owner_references
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|o| o.controller == Some(true) && o.uid == backup_uid)
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ArchiveObservation {
    /// Whether each of the two evidence objects exists.
    pub presence: EvidencePresence,
    /// The receipt's `covered{from_ms, to_ms}`, in **epoch milliseconds**,
    /// when the receipt was read and named its window.
    pub covered: Option<(i64, i64)>,
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
    Some(ArchiveObservation {
        presence: EvidencePresence {
            payload: receipt.is_some(),
            sidecar,
        },
        covered,
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

/// The runner argv for `backup`, **read off the annotation and passed through
/// unchanged**.
///
/// # It is not rebuilt here, and that is the point
///
/// `Backup.spec` carries no argv — the field set is Task 15b's and the spec is
/// sealed by a CEL rule — so the argv travels on
/// `controllers::backup_schedule::RUNNER_ARGV_ANNOTATION` as a JSON array.
/// Building one here instead would silently drop `--backup-id-override`
/// (interface **I10**), which is the flag that makes a re-run for a slot reuse
/// the slot's own backup id rather than mint a second one — so a dropped flag
/// is a second, partial archive rather than a visible error. Task 18's review
/// made this ruling; `the_argv_is_the_annotation_verbatim` asserts it.
///
/// `None` for an absent or unparseable annotation. A `Backup` created by hand
/// with no annotation is a spec error to report, never an argv to invent.
#[must_use]
pub fn runner_argv(backup: &Backup) -> Option<Vec<String>> {
    let raw = backup
        .annotations()
        .get(super::backup_schedule::RUNNER_ARGV_ANNOTATION)?;
    serde_json::from_str::<Vec<String>>(raw).ok()
}

/// The [`RunnerJobSpec`] one `Backup` produces.
///
/// PURE, so the Job a test builds is byte-identical to the one the reconciler
/// `POST`s and an assertion over this function is an assertion over the
/// request.
///
/// # Errors
///
/// [`BackupError`] when the object carries no namespace or UID (both
/// unreachable from the API server, both named rather than unwrapped) or no
/// usable runner argv.
pub fn runner_job_spec(backup: &Backup) -> Result<RunnerJobSpec, BackupError> {
    let name = backup.name_any();
    let namespace = backup
        .namespace()
        .ok_or_else(|| BackupError::NoNamespace(name.clone()))?;
    let uid = backup
        .uid()
        .ok_or_else(|| BackupError::NoUid(name.clone()))?;
    let args = runner_argv(backup).ok_or_else(|| BackupError::NoRunnerArgv(name.clone()))?;

    let mut secret_mounts = vec![SecretMount {
        volume: SIGNING_VOLUME.to_string(),
        secret_name: SIGNING_KEY_SECRET.to_string(),
        mount_path: SIGNING_MOUNT_PATH.to_string(),
        items: vec![(
            SIGNING_KEY_SECRET_KEY.to_string(),
            SIGNING_KEY_FILE.to_string(),
        )],
    }];
    // Kept out of `env_from_secret` for the same reason the drill manifest
    // keeps the signing key out of it: a key is a FILE the runner opens by
    // path, and an env var holding PEM text would appear in
    // `kubectl describe pod` output for anyone with pod read.
    secret_mounts.sort_by(|a, b| a.volume.cmp(&b.volume));

    let mut env_from_secret = Vec::new();
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
        args,
        deadline_seconds: backup.spec.deadline_seconds,
        service_account_name: RUNNER_SERVICE_ACCOUNT.to_string(),
        secret_mounts,
        env_from_secret,
        // `RUST_LOG` is pinned rather than inherited: below `info` the run id
        // and the exit-code meaning line are lost, and those two are how a
        // pod is correlated with the archive it read
        // (`docs/kubernetes.md` §1b).
        env_literal: vec![("RUST_LOG".to_string(), "info".to_string())],
        plan_config_map: Some(plan_config_map_name(&backup.name_any())),
    })
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

/// `lastTransitionTime` moves only when the condition actually transitions.
///
/// This controller requeues, so a bump on every write would put a fresh
/// transition timestamp on a `Backup` that never changed state — both a lie
/// about the condition and a `resourceVersion` bump every watcher in the
/// cluster receives.
fn last_transition_time(
    backup: &Backup,
    r#type: &str,
    status: &str,
    reason: &str,
    now: DateTime<Utc>,
) -> DateTime<Utc> {
    backup
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|cs| cs.iter().find(|c| c.r#type == r#type))
        .filter(|c| c.status == status && c.reason.as_deref() == Some(reason))
        .and_then(|c| c.last_transition_time)
        .unwrap_or(now)
}

/// One condition, as a merge-patch fragment.
fn condition(
    backup: &Backup,
    r#type: &str,
    status: &str,
    reason: &str,
    message: &str,
    now: DateTime<Utc>,
) -> Value {
    json!({
        "type": r#type,
        "status": status,
        "reason": reason,
        "message": message,
        "observedGeneration": backup.meta().generation,
        "lastTransitionTime": last_transition_time(backup, r#type, status, reason, now),
    })
}

/// The `/status` merge patch for a Job that exists and has not finished.
///
/// BUILT AS JSON AND NOT BY SERIALISING `BackupStatus`, for merge-patch
/// semantics: every optional field on that struct skips serialising when
/// `None`, so an absent key means "leave it alone" — which is exactly what a
/// running reconcile wants for the fields a finished one will write, and which
/// a serialised struct could not express.
#[must_use]
pub fn running_status_patch(backup: &Backup, job_name: &str, now: DateTime<Utc>) -> Value {
    json!({
        "status": {
            "phase": PHASE_RUNNING,
            "jobRef": { "name": job_name },
            "conditions": [condition(
                backup,
                CONDITION_JOB_CREATED,
                "True",
                CONDITION_JOB_CREATED,
                &format!("the runner Job {job_name} exists and has not finished"),
                now,
            )],
        }
    })
}

/// The `/status` merge patch for a finished Job whose `runner` container
/// terminated.
///
/// `windowCovered` is written **only when the receipt was read** — see
/// [`ArchiveOracle`]. A `None` `covered` omits the key from the merge patch,
/// which means "leave it alone": a run whose receipt could not be fetched must
/// not overwrite a window a previous pass recorded, and must certainly not
/// write a zero one.
#[must_use]
pub fn finished_status_patch(
    backup: &Backup,
    exit_code: i32,
    keys: &EvidenceKeys,
    refusal: Option<&str>,
    orphan: Option<&str>,
    covered: Option<(i64, i64)>,
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

    let mut status = serde_json::Map::new();
    status.insert("phase".to_string(), json!(phase));
    status.insert("exitCode".to_string(), json!(exit_code));
    status.insert("exitReason".to_string(), json!(exit_reason));
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
#[must_use]
pub fn crashed_status_patch(
    backup: &Backup,
    terminal_state: &str,
    job_name: &str,
    now: DateTime<Utc>,
) -> Value {
    json!({
        "status": {
            "phase": PHASE_FAILED,
            "exitReason": REASON_OPERATIONAL,
            "jobRef": { "name": job_name },
            "conditions": [condition(
                backup,
                CONDITION_FAILED,
                "True",
                terminal_state,
                "the Job finished but no container named runner reported a terminated state; \
                 the exit code is unrecoverable",
                now,
            )],
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
#[must_use]
pub fn refused_status_patch(
    backup: &Backup,
    reason: &str,
    message: &str,
    now: DateTime<Utc>,
) -> Value {
    json!({
        "status": {
            "phase": PHASE_FAILED,
            "exitReason": REASON_OPERATIONAL,
            "conditions": [condition(backup, CONDITION_FAILED, "True", reason, message, now)],
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
    /// No usable `logweir.dev/runner-argv` annotation. **Never an invented
    /// argv** — see [`runner_argv`].
    NoRunnerArgv(String),
    /// The API server could not be talked to. Requeue.
    Api(kube::Error),
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
            Self::NoRunnerArgv(name) => write!(
                f,
                // The flag is named in PROSE and not as its literal token:
                // `tests/schedule_controller.rs::the_backup_id_override_is_passed_not_defined`
                // counts the token on CODE lines and permits exactly one
                // occurrence in this crate, in `backup_schedule::runner_argv`.
                // A second literal here would make that guard's count wrong
                // for a message string, which is the worst kind of guard
                // failure — true, and about nothing.
                "the object {name} carries no parseable `{}` annotation, so there is no runner \
                 argv; one is never invented here, because a rebuilt argv silently drops the \
                 backup id override flag (interface I10)",
                super::backup_schedule::RUNNER_ARGV_ANNOTATION
            ),
            Self::Api(e) => write!(f, "kubernetes API error: {e}"),
            Self::Refused(state, message) => write!(f, "{state}: {message}"),
        }
    }
}

impl std::error::Error for BackupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NoNamespace(_) | Self::NoUid(_) | Self::NoRunnerArgv(_) | Self::Refused(..) => {
                None
            }
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

/// `POST` the plan ConfigMap, and decide the 409.
///
/// A 409 IS SUCCESS ONLY IF THE EXISTING OBJECT IS OURS. The ordinary 409 is
/// this same reconcile's previous pass: the object is byte-identical, because
/// it is rendered from an immutable spec by a pure function. A 409 on an
/// object owned by something else is a plan document a stranger wrote, at the
/// mount path of a pod that holds this `Backup`'s signing key, and it is
/// terminal — see [`TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT`].
///
/// THE CONFIGMAP WRITE IS A KUBERNETES API WRITE, NOT AN ARCHIVE WRITE.
/// `scripts/check-no-archive-write.sh` is unaffected: its control-plane token
/// list forbids the writable `Store` constructor and the put family, and
/// anchors `.delete(` to a store-shaped receiver precisely so that an
/// ordinary `api.create(…)` on a ConfigMap is not a hit.
///
/// # Errors
///
/// Whatever [`plan_config_map`] refuses;
/// [`TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT`] for a foreign existing object;
/// [`BackupError::Api`] for anything transient.
async fn write_plan_config_map(
    backup: &Backup,
    client: &kube::Client,
    namespace: &str,
    cluster: &KafkaCluster,
) -> Result<(), BackupError> {
    let name = backup.name_any();
    let cm_name = plan_config_map_name(&name);
    let uid = backup
        .uid()
        .ok_or_else(|| BackupError::NoUid(name.clone()))?;
    let desired = plan_config_map(backup, cluster)?;
    let maps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);

    match maps.create(&PostParams::default(), &desired).await {
        Ok(_) => {
            info!(
                backup = %name,
                namespace = %namespace,
                config_map = %cm_name,
                source_cluster = %cluster.name_any(),
                "rendered the plan ConfigMap the runner Job mounts at /plan"
            );
            Ok(())
        }
        Err(kube::Error::Api(e)) if e.code == 409 => {
            let existing = maps
                .get_opt(&cm_name)
                .await
                .map_err(BackupError::Api)?
                .ok_or_else(|| {
                    // A 409 followed by a 404 is a race with a deletion, and a
                    // race IS transient: requeue rather than refuse.
                    BackupError::Refused(
                        TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT,
                        format!(
                            "the ConfigMap {cm_name} answered 409 to a create and 404 to the \
                             read that followed it"
                        ),
                    )
                })?;
            if is_owned_by(&existing, &uid) {
                debug!(
                    backup = %name,
                    namespace = %namespace,
                    config_map = %cm_name,
                    "the plan ConfigMap already exists and is owned by this Backup; a rendered \
                     plan is a pure function of an immutable spec, so it is the same bytes"
                );
                Ok(())
            } else {
                Err(BackupError::Refused(
                    TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT,
                    format!(
                        "the ConfigMap {cm_name} already exists and carries no controller owner \
                         reference with this Backup's UID; the runner Job would mount a plan \
                         document this object did not render, in the pod that holds the signing \
                         key"
                    ),
                ))
            }
        }
        Err(e) => Err(BackupError::Api(e)),
    }
}

/// Find the pod the exit code is read from.
///
/// The prefixed selector first, the legacy one as a fallback — see
/// [`JOB_NAME_LABEL_LEGACY`]. Returns the FIRST pod: `backoffLimit: 0` plus
/// `restartPolicy: Never` yields exactly one (verified live), so a second pod
/// would mean the Job shape had changed under the controller, and taking the
/// first is then no worse than any other arbitrary choice — while looping over
/// several and merging their codes into one field would be.
async fn find_pod(
    client: &kube::Client,
    namespace: &str,
    job_name: &str,
) -> Result<Option<Pod>, BackupError> {
    let api: Api<Pod> = Api::namespaced(client.clone(), namespace);
    for selector in pod_selectors(job_name) {
        let list = api
            .list(&ListParams::default().labels(&selector))
            .await
            .map_err(BackupError::Api)?;
        if let Some(pod) = list.items.into_iter().next() {
            return Ok(Some(pod));
        }
        debug!(
            job = %job_name,
            selector = %selector,
            "no pod matched this selector; trying the next"
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
/// 1. **No Job and no terminal status** → build and `POST` the Job. Status
///    `phase: Running`, condition `JobCreated`.
/// 2. **Job exists, not finished** → `phase: Running`, `jobRef` set. Nothing
///    else.
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
    now: DateTime<Utc>,
) -> Result<BackupOutcome, BackupError> {
    match reconcile_backup_inner(backup, client, archive, now).await {
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
                "refusing this Backup terminally: nothing was created, and a requeue over an \
                 immutable spec would never succeed"
            );
            if !status_is_terminal(backup) {
                let backups: Api<Backup> = Api::namespaced(client.clone(), &namespace);
                backups
                    .patch_status(
                        &name,
                        &PatchParams::default(),
                        &Patch::Merge(refused_status_patch(backup, state, &message, now)),
                    )
                    .await
                    .map_err(BackupError::Api)?;
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
    now: DateTime<Utc>,
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

    // STEP 1. Nothing running and nothing terminal: create.
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
        let spec = runner_job_spec(backup)?;

        // THE PLAN CONFIGMAP, IN THIS SAME PASS AND BEFORE THE JOB `POST`
        // (errata E5a). The Job mounts `<name>-plan` at `/plan`, so a Job
        // created first is a pod that stalls in `ContainerCreating` until its
        // deadline fires — measured live, and the reason this task's review
        // found every scheduled backup failing as an unexplained `NoExitCode`.
        // `the_plan_config_map_is_posted_before_the_job` asserts the order
        // over the recorded route table.
        let cluster = plan_source_cluster(backup, client, &namespace).await?;
        write_plan_config_map(backup, client, &namespace, &cluster).await?;

        jobs.create(&PostParams::default(), &job::build(&spec))
            .await
            .map_err(BackupError::Api)?;
        info!(
            backup = %name,
            namespace = %namespace,
            job = %job_name,
            "created the runner Job"
        );
        backups
            .patch_status(
                &name,
                &PatchParams::default(),
                &Patch::Merge(running_status_patch(backup, &job_name, now)),
            )
            .await
            .map_err(BackupError::Api)?;
        return Ok(BackupOutcome {
            job_name,
            created: true,
            exit_code: None,
            terminal_state: None,
            keys: EvidenceKeys::default(),
            ttl_patched: false,
        });
    };

    // STEP 2. Running.
    if !job_finished(&job) {
        backups
            .patch_status(
                &name,
                &PatchParams::default(),
                &Patch::Merge(running_status_patch(backup, &job_name, now)),
            )
            .await
            .map_err(BackupError::Api)?;
        return Ok(BackupOutcome {
            job_name,
            created: false,
            exit_code: None,
            terminal_state: None,
            keys: EvidenceKeys::default(),
            ttl_patched: false,
        });
    }

    let pod = find_pod(client, &namespace, &job_name).await?;
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
        backups
            .patch_status(
                &name,
                &PatchParams::default(),
                &Patch::Merge(crashed_status_patch(backup, terminal_state, &job_name, now)),
            )
            .await
            .map_err(BackupError::Api)?;
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
    let orphan = orphan_state(exit_code, observed.map(|o| o.presence));
    let covered = observed.and_then(|o| o.covered);

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

    backups
        .patch_status(
            &name,
            &PatchParams::default(),
            &Patch::Merge(finished_status_patch(
                backup,
                exit_code,
                &keys,
                refusal.as_deref(),
                orphan,
                covered,
                now,
            )),
        )
        .await
        .map_err(BackupError::Api)?;

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
    reconcile_backup(&backup, &ctx.client, &oracle, Utc::now()).await?;
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
pub fn controller(
    client: kube::Client,
    archive: Option<Arc<Store>>,
) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<Backup> = Api::all(client.clone());
    let jobs: Api<Job> = Api::all(client.clone());
    let ctx = Arc::new(Context { client, archive });
    async move {
        Controller::new(api, watcher::Config::default())
            .owns(jobs, watcher::Config::default())
            .run(reconcile, error_policy, ctx)
            .for_each(|_| std::future::ready(()))
            .await;
    }
}
