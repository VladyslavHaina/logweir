//! The `Backup` execution contract — PLAT-06.1.
//!
//! # What executes, and where it comes from
//!
//! A runner Job executes exactly three things, and every one of them is
//! derived here from typed, server-side facts:
//!
//! 1. **The run identity** ([`execution_identity`]). A scheduled `Backup` —
//!    `spec.triggeredBy: schedule`, a `spec.scheduleRef`, a `spec.slot`, a
//!    controller owner reference to that `BackupSchedule` and the deterministic
//!    object name for that slot — runs as `<schedule uid>-<slot>`
//!    ([`crate::slot::backup_id_for`], guard **G-SLOT**). A manual `Backup` —
//!    `spec.triggeredBy: manual` — runs as its own API-server UID. Creating a
//!    `Backup` under a new name is a new run; re-creating the same name while
//!    the object exists is `AlreadyExists`; deleting and re-creating it mints a
//!    new UID and therefore a new manual run.
//! 2. **The resolved inputs** ([`BackupExecutionInputs`]): the identity, the
//!    source connection resolved from the `KafkaCluster` (its UID, addresses,
//!    SCRAM username and TLS flag), the topic allowlist, the archive location
//!    with its resolved storage block and the object-store addressing
//!    environment, and the runner argv and deadline. No Secret value and no
//!    Secret name is part of it: the password and archive credential references
//!    are fixed by the pinned `KafkaCluster` UID and the CEL-immutable
//!    `KafkaCluster.spec` and `Backup.spec`, so the Job builder derives them
//!    from those objects and the ConfigMap, which has no encryption at rest,
//!    names no credential.
//! 3. **The runner argv** ([`runner_argv`]), a pure function of the trigger and
//!    the identity.
//!
//! # No annotation contributes
//!
//! Controllers before PLAT-06.1 executed the JSON array on the
//! `logweir.dev/runner-argv` annotation ([`RUNNER_ARGV_ANNOTATION`]), so a
//! manual `Backup` without it could not run and whoever could annotate a
//! `Backup` could change the subcommand, the spec path, the signing key path or
//! the backup id. That annotation is now only OBSERVED ([`runner_argv_annotation`]),
//! reported by size and digest, and never executed or echoed.
//!
//! # Frozen before any Job exists
//!
//! [`FrozenInputs`] is the canonical encoding of the inputs
//! (`logweir_core::det_json`) plus its `sha256:` digest. The controller writes
//! it into the create-only, `immutable: true` plan ConfigMap
//! ([`inputs_config_map`]) together with the two documents the runner reads,
//! both rendered FROM the snapshot, and records `status.execution` before it
//! creates the Job. On every later pass that needs the ConfigMap, the controller
//! re-resolves the inputs and admits the existing object only through
//! [`verify_frozen_config_map`]: exact single owner, immutable, canonical,
//! digest-annotated, documents re-rendered byte for byte, bound to this
//! `Backup`, equal to the recorded status and equal to the fresh resolution in
//! everything executable. Anything else is a terminal conflict; nothing is
//! overwritten or adopted.
//!
//! This module performs no I/O. It reads the process environment only through
//! [`crate::retention::storage_url_for`], the one archive URL parser, exactly as
//! the plan renderer always has.

use std::collections::BTreeMap;
use std::fmt;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::{Resource, ResourceExt as _};
use serde::{Deserialize, Serialize};

use crate::conditions::{
    TERMINAL_STATE_ARCHIVE_URL_UNREADABLE, TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
    TERMINAL_STATE_EXECUTION_SPEC_INVALID, TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT,
    TERMINAL_STATE_REFERENT_NOT_FOUND,
};
use crate::controllers::backup::{plan_config_map_name, PLAN_ALLOWED_CLUSTERS_KEY, PLAN_SPEC_KEY};
use crate::crds::backup::{Backup, BackupExecution};
use crate::crds::backup_schedule::BackupSchedule;
use crate::crds::kafka_cluster::{AuthMode, KafkaCluster};
use crate::crds::LocalRef;
use logweir_core::engine::StorageUrl;
use logweir_core::ids::sha256_prefixed;
use logweir_core::spec::{AllowedClusters, AuthSpec, BackupSettings, BackupSourceSpec, BackupSpec};

/// `spec.triggeredBy` for a `Backup` a person or a client created.
pub const TRIGGER_MANUAL: &str = "manual";

/// `spec.triggeredBy` for a `Backup` the `BackupSchedule` reconciler created.
pub const TRIGGER_SCHEDULE: &str = "schedule";

/// Where the rendered `backup.yaml` is mounted in the runner pod.
pub const SPEC_PATH: &str = "/plan/backup.yaml";
/// Where the restore-target allowlist is mounted in the runner pod.
pub const ALLOWED_CLUSTERS_PATH: &str = "/plan/allowed-clusters.json";
/// Where the receipt signing key is projected in the runner pod.
pub const SIGNING_KEY_PATH: &str = "/signing/key.pem";
/// Where the runner writes its backup document.
///
/// **NOT IN THE RUNNER ARGV, AND THE REASON IS A MEASUREMENT.** `logweir
/// backup run` writes exactly one document and `--receipt-out` takes
/// precedence over `--out`, so passing both at DIFFERENT paths is refused with
/// exit 1 before the engine is spawned, by `refuse_two_receipt_paths` in the
/// CLI's own `backup/phase_minus1_admit.rs`. The scheduled argv passed both
/// until Task 24 measured it on a live cluster, which means every scheduled
/// `Backup` in the shipped tree failed that way and archived nothing. The
/// constant is kept because `job::WORK_VOLUME`'s doc comment and
/// `docs/kubernetes.md` name the path a runner's scratch volume has to make
/// writable, and that is still true; nothing passes it as a flag.
pub const OUT_PATH: &str = "/work/backup.json";
/// Where the runner writes the signed receipt.
pub const RECEIPT_OUT_PATH: &str = "/work/receipt.json";

/// The annotation older controllers executed as the runner argv.
///
/// OBSERVED AND NEVER EXECUTED. New scheduled `Backup`s do not carry it; a
/// `Backup` that does gets a `RunnerArgvAnnotationIgnored` condition naming
/// the annotation's size and digest. The name stays public so tests, docs and
/// upgrade tooling can identify legacy objects precisely.
pub const RUNNER_ARGV_ANNOTATION: &str = "logweir.dev/runner-argv";

/// The plan ConfigMap key carrying the canonical typed input snapshot.
pub const INPUTS_KEY: &str = "execution-inputs.json";

/// The snapshot grammar this controller writes and reads. A snapshot naming
/// any other version is a conflict, never a best-effort parse.
pub const INPUTS_VERSION: &str = "logweir.dev/backup-execution-inputs/v1";

/// The annotation, on the plan ConfigMap and on the runner Job and its pod
/// template, carrying [`FrozenInputs::sha256`].
pub const INPUTS_SHA256_ANNOTATION: &str = "logweir.dev/execution-inputs-sha256";

/// The annotation, beside [`INPUTS_SHA256_ANNOTATION`], carrying the run
/// identity.
pub const EXECUTION_ID_ANNOTATION: &str = "logweir.dev/execution-id";

/// A terminal refusal this module decided, with the terminal state the
/// controller writes and a message that names what was wrong and never a
/// credential.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionRefusal {
    /// One of [`crate::conditions::TERMINAL_STATES`].
    pub state: &'static str,
    /// The condition message.
    pub message: String,
}

impl ExecutionRefusal {
    fn new(state: &'static str, message: impl Into<String>) -> Self {
        Self {
            state,
            message: message.into(),
        }
    }

    fn spec(message: impl Into<String>) -> Self {
        Self::new(TERMINAL_STATE_EXECUTION_SPEC_INVALID, message)
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self::new(TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT, message)
    }
}

impl fmt::Display for ExecutionRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.state, self.message)
    }
}

impl std::error::Error for ExecutionRefusal {}

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

/// What caused a run, as the typed spec states it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExecutionTrigger {
    /// [`TRIGGER_MANUAL`].
    Manual,
    /// [`TRIGGER_SCHEDULE`].
    Schedule,
}

impl ExecutionTrigger {
    /// The wire spelling, which is also the runner's `--triggered-by` value
    /// and therefore the signed receipt's `triggered_by`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Manual => TRIGGER_MANUAL,
            Self::Schedule => TRIGGER_SCHEDULE,
        }
    }
}

/// One Kubernetes object, by namespace, name and API-server UID.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ObjectIdentity {
    /// `metadata.namespace`.
    pub namespace: String,
    /// `metadata.name`.
    pub name: String,
    /// `metadata.uid`.
    pub uid: String,
}

/// The schedule half of a scheduled identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ScheduleTrigger {
    /// The `BackupSchedule`'s name.
    pub name: String,
    /// The `BackupSchedule`'s UID, from the controller owner reference.
    pub uid: String,
    /// The slot, `yyyymmdd-hhmmss` in UTC.
    pub slot: String,
}

/// A run identity, and the object it was derived for.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecutionIdentity {
    /// The archive `backup_id`: `<schedule uid>-<slot>` or the `Backup` UID.
    pub id: String,
    /// `manual` or `schedule`.
    pub trigger: ExecutionTrigger,
    /// The `Backup` this identity binds to. A snapshot copied onto another
    /// object — another name, namespace or UID — does not verify.
    pub backup: ObjectIdentity,
    /// Present exactly for `schedule`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<ScheduleTrigger>,
}

/// A `triggeredBy` value, bounded for a condition message. The field is spec
/// text and not a credential, but it is unbounded.
fn shown(value: &str) -> String {
    const LIMIT: usize = 64;
    if value.chars().count() <= LIMIT {
        value.to_string()
    } else {
        format!("{}…", value.chars().take(LIMIT).collect::<String>())
    }
}

/// Whether `slot` is a canonical `yyyymmdd-hhmmss` instant.
fn valid_slot(slot: &str) -> bool {
    const FORMAT: &str = "%Y%m%d-%H%M%S";
    slot.len() == 15
        && chrono::NaiveDateTime::parse_from_str(slot, FORMAT)
            .is_ok_and(|t| t.format(FORMAT).to_string() == slot)
}

/// Derive the run identity from the typed spec and server-generated metadata.
///
/// # Errors
///
/// [`TERMINAL_STATE_EXECUTION_SPEC_INVALID`] naming the field when the spec
/// states no runnable trigger, or a `schedule` trigger that is not the complete
/// `BackupSchedule` controller identity. A hand-written `Backup` that claims to
/// be a scheduled run is refused rather than silently re-labelled: its signed
/// receipt would otherwise say `schedule` about a run no schedule created.
pub fn execution_identity(backup: &Backup) -> Result<ExecutionIdentity, ExecutionRefusal> {
    let name = backup.name_any();
    let namespace = backup
        .namespace()
        .filter(|ns| !ns.is_empty())
        .ok_or_else(|| {
            ExecutionRefusal::spec(format!("the Backup {name} carries no metadata.namespace"))
        })?;
    let uid = backup.uid().filter(|uid| !uid.is_empty()).ok_or_else(|| {
        ExecutionRefusal::spec(format!("the Backup {name} carries no metadata.uid"))
    })?;
    let object = ObjectIdentity {
        namespace,
        name: name.clone(),
        uid: uid.clone(),
    };
    match backup.spec.triggered_by.as_str() {
        TRIGGER_MANUAL => {
            if let Some(slot) = backup.spec.slot.as_deref() {
                return Err(ExecutionRefusal::spec(format!(
                    "spec.triggeredBy is `manual` but spec.slot is `{}`; a slot names one \
                     BackupSchedule run, while a manual Backup runs under its own \
                     server-generated UID. Create the manual Backup without spec.slot",
                    shown(slot)
                )));
            }
            Ok(ExecutionIdentity {
                id: uid,
                trigger: ExecutionTrigger::Manual,
                backup: object,
                schedule: None,
            })
        }
        TRIGGER_SCHEDULE => {
            let schedule = backup
                .spec
                .schedule_ref
                .as_ref()
                .filter(|reference| !reference.name.is_empty())
                .ok_or_else(|| {
                    ExecutionRefusal::spec(
                        "spec.triggeredBy is `schedule` but spec.scheduleRef is absent; only the \
                         BackupSchedule controller creates scheduled runs, and a Backup created \
                         by hand or by a client is `manual`",
                    )
                })?;
            let slot = backup.spec.slot.as_deref().ok_or_else(|| {
                ExecutionRefusal::spec(
                    "spec.triggeredBy is `schedule` but spec.slot is absent, so no scheduled run \
                     identity exists",
                )
            })?;
            if !valid_slot(slot) {
                return Err(ExecutionRefusal::spec(format!(
                    "spec.slot `{}` is not a UTC slot in yyyymmdd-hhmmss form",
                    shown(slot)
                )));
            }
            let owner = backup
                .owner_references()
                .iter()
                .find(|owner| owner.controller == Some(true))
                .filter(|owner| {
                    owner.api_version == BackupSchedule::api_version(&())
                        && owner.kind == BackupSchedule::kind(&())
                        && owner.name == schedule.name
                        && !owner.uid.is_empty()
                })
                .ok_or_else(|| {
                    ExecutionRefusal::spec(format!(
                        "spec.triggeredBy is `schedule` but the Backup has no controller owner \
                         reference to BackupSchedule `{}`; the scheduled run identity is \
                         derived from that owner's UID and is never supplied by a client",
                        shown(&schedule.name)
                    ))
                })?;
            match crate::slot::scheduled_backup_name(&schedule.name, slot) {
                Ok(expected) if expected == name => {}
                Ok(expected) => {
                    return Err(ExecutionRefusal::spec(format!(
                        "spec.triggeredBy is `schedule` but the object is named `{name}`; the \
                         BackupSchedule controller names slot {slot}'s run `{expected}`, and one \
                         deterministic name is what makes a duplicate slot an AlreadyExists"
                    )))
                }
                Err(error) => {
                    return Err(ExecutionRefusal::spec(format!(
                        "spec.scheduleRef and spec.slot cannot name a scheduled Backup: {error}"
                    )))
                }
            }
            Ok(ExecutionIdentity {
                id: crate::slot::backup_id_for(&owner.uid, slot),
                trigger: ExecutionTrigger::Schedule,
                backup: object,
                schedule: Some(ScheduleTrigger {
                    name: schedule.name.clone(),
                    uid: owner.uid.clone(),
                    slot: slot.to_string(),
                }),
            })
        }
        other => Err(ExecutionRefusal::spec(format!(
            "spec.triggeredBy is `{}`; this controller runs `manual` and `schedule` Backups \
             only, and the value becomes the signed receipt's triggered_by",
            shown(other)
        ))),
    }
}

/// The backup id controllers before PLAT-06.1 reported: the first controller
/// owner's UID plus `spec.slot`, else this object's UID.
///
/// USED ONLY TO REPORT A RUN THIS CONTROLLER DID NOT DERIVE — a Job an older
/// controller created, whose status must say what that controller would have
/// said. For every `Backup` [`execution_identity`] accepts, the two agree.
#[must_use]
pub fn legacy_backup_id(backup: &Backup) -> String {
    let owner = backup
        .owner_references()
        .iter()
        .find(|o| o.controller == Some(true))
        .map(|o| o.uid.clone());
    match (owner, backup.spec.slot.as_deref()) {
        (Some(uid), Some(slot)) => crate::slot::backup_id_for(&uid, slot),
        _ => backup.uid().unwrap_or_else(|| backup.name_any()),
    }
}

/// The runner argv for one run: a pure function of the trigger and the run
/// identity, with every path fixed by this controller.
///
/// INTERFACE **I10** IS PASSED HERE, NOT DEFINED HERE. The backup id override
/// flag is Task 4's, on Task 4's `logweir backup run`; this function writes it
/// so the CLI does not have to know whether a schedule or a person asked for
/// the run. It is always present, including for a manual run whose plan
/// document already names the same id, so the Job spec states the executed
/// identity. `tests/schedule_controller.rs::the_backup_id_override_is_passed_not_defined`
/// asserts that the flag token appears on exactly one code line in this crate,
/// in this function.
///
/// The leading `"backup"` token names the **`logweir`** subcommand, not the
/// engine's; `--receipt-out` and never `--out` (see [`OUT_PATH`]).
#[must_use]
pub fn runner_argv(trigger: ExecutionTrigger, execution_id: &str) -> Vec<String> {
    [
        "backup",
        "run",
        "--spec",
        SPEC_PATH,
        "--allowed-clusters",
        ALLOWED_CLUSTERS_PATH,
        "--signing-key",
        SIGNING_KEY_PATH,
        "--receipt-out",
        RECEIPT_OUT_PATH,
        "--triggered-by",
        trigger.as_str(),
        "--backup-id-override",
        execution_id,
    ]
    .iter()
    .map(|token| (*token).to_string())
    .collect()
}

// ---------------------------------------------------------------------------
// The snapshot
// ---------------------------------------------------------------------------

/// A literal environment variable forwarded to the runner.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EnvValue {
    /// The variable's name.
    pub name: String,
    /// Its value. Only object-store ADDRESSING variables are forwarded, never
    /// a credential.
    pub value: String,
}

/// The source connection, resolved from the `KafkaCluster`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceInputs {
    /// The `KafkaCluster` the settings were resolved from, UID included, so a
    /// deleted-and-recreated cluster of the same name is a different input.
    pub cluster: ObjectIdentity,
    /// The bootstrap addresses the run dials.
    pub bootstrap_servers: Vec<String>,
    /// Mode, SCRAM username and TLS flag. No password at any variant.
    pub auth: AuthSpec,
    /// `KafkaCluster.status.clusterId` when the inputs were resolved.
    /// INFORMATIONAL on the backup path: the runner observes the id from the
    /// broker and never reads this, so it is excluded from the executable
    /// comparison and a later probe result never invalidates frozen inputs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_cluster_id: Option<String>,
}

/// Where the archive is written.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ArchiveInputs {
    /// `Backup.spec.archive.url`.
    pub url: String,
    /// The typed storage block the runner's `backup.yaml` carries, resolved by
    /// [`crate::retention::storage_url_for`].
    pub storage: StorageUrl,
    /// The object-store addressing variables the controller forwards
    /// ([`crate::controllers::backup::ARCHIVE_ADDRESSING_ENV`]), frozen so a
    /// recreated Job cannot address a different store than its plan document.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub addressing_env: Vec<EnvValue>,
}

/// The engine tunables the runner's `backup.yaml` carries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunnerSettings {
    /// `backup.compression`.
    pub compression: String,
    /// `backup.segment_max_records`.
    pub segment_max_records: u64,
    /// `backup.segment_max_bytes`.
    pub segment_max_bytes: u64,
    /// `backup.max_concurrent_partitions`.
    pub max_concurrent_partitions: u32,
}

impl From<&BackupSettings> for RunnerSettings {
    fn from(settings: &BackupSettings) -> Self {
        Self {
            compression: settings.compression.clone(),
            segment_max_records: settings.segment_max_records,
            segment_max_bytes: settings.segment_max_bytes,
            max_concurrent_partitions: settings.max_concurrent_partitions,
        }
    }
}

impl From<&RunnerSettings> for BackupSettings {
    fn from(settings: &RunnerSettings) -> Self {
        Self {
            compression: settings.compression.clone(),
            segment_max_records: settings.segment_max_records,
            segment_max_bytes: settings.segment_max_bytes,
            max_concurrent_partitions: settings.max_concurrent_partitions,
        }
    }
}

/// What the runner container executes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunnerInputs {
    /// The container argv, [`runner_argv`] of the identity.
    pub args: Vec<String>,
    /// The Job's `activeDeadlineSeconds`.
    pub deadline_seconds: i64,
    /// The engine tunables.
    pub settings: RunnerSettings,
}

/// The canonical typed snapshot of everything one run executes — the
/// `execution-inputs.json` document.
///
/// VERSIONED AND CLOSED. Every struct denies unknown fields and the controller
/// re-encodes a stored snapshot and requires the same bytes, so a snapshot
/// written by a later grammar is a conflict and never a partial read. Later
/// tasks that freeze more (a schedule revision, a resolved dynamic topic set)
/// extend this type under a new [`INPUTS_VERSION`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BackupExecutionInputs {
    /// [`INPUTS_VERSION`].
    pub version: String,
    /// The run identity.
    pub execution: ExecutionIdentity,
    /// The source connection.
    pub source: SourceInputs,
    /// `Backup.spec.topics`, verbatim.
    pub topics: Vec<String>,
    /// The archive.
    pub archive: ArchiveInputs,
    /// The container argv, deadline and tunables.
    pub runner: RunnerInputs,
}

impl BackupExecutionInputs {
    /// The snapshot with its informational fields cleared: the value two
    /// resolutions are compared by.
    #[must_use]
    pub fn executable(&self) -> Self {
        let mut view = self.clone();
        view.source.observed_cluster_id = None;
        view
    }
}

/// Resolve the inputs of one run from the typed `Backup`, its derived identity,
/// the referenced `KafkaCluster` and the controller's forwarded addressing
/// environment.
///
/// # Errors
///
/// A terminal [`ExecutionRefusal`]: `ReferentNotFound` for a cluster with no
/// UID, `CredentialNotRenderable` for a SCRAM cluster without a username or a
/// usable Secret reference, or an archive Secret reference with a blank name,
/// `ArchiveUrlUnreadable` for an archive URL no storage block can be rendered
/// from, and `ExecutionSpecInvalid` for a non-positive deadline.
pub fn resolve_inputs(
    backup: &Backup,
    identity: ExecutionIdentity,
    cluster: &KafkaCluster,
    addressing_env: &[(String, String)],
) -> Result<BackupExecutionInputs, ExecutionRefusal> {
    let name = backup.name_any();
    let cluster_name = cluster.name_any();
    if backup.spec.deadline_seconds <= 0 {
        return Err(ExecutionRefusal::spec(format!(
            "spec.deadlineSeconds is {}; the runner Job's activeDeadlineSeconds must be positive",
            backup.spec.deadline_seconds
        )));
    }
    let cluster_uid = cluster.uid().filter(|uid| !uid.is_empty()).ok_or_else(|| {
        ExecutionRefusal::new(
            TERMINAL_STATE_REFERENT_NOT_FOUND,
            format!(
                "the KafkaCluster {cluster_name} carries no metadata.uid, so the connection this \
                 run resolves cannot be pinned"
            ),
        )
    })?;
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
                    ExecutionRefusal::new(
                        TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
                        format!(
                            "the KafkaCluster {cluster_name} names auth mode scramSha512 and no \
                             auth.username, so the plan document cannot name the identity the run \
                             will present"
                        ),
                    )
                })?;
            // The reference is VALIDATED here and not copied: the Job builder
            // projects it from this same KafkaCluster, whose UID the snapshot
            // pins and whose spec is CEL-immutable.
            if crate::controllers::kafka_cluster::source_password_env(cluster).is_none() {
                return Err(ExecutionRefusal::new(
                    TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
                    format!(
                        "the KafkaCluster {cluster_name} uses scramSha512 but has no non-empty \
                         auth.secretRef.name; reference a Secret in namespace {} with a password \
                         key",
                        identity.backup.namespace
                    ),
                ));
            }
            AuthSpec::ScramSha512 {
                username,
                tls: cluster.spec.auth.tls,
            }
        }
    };
    let storage = crate::retention::storage_url_for(&backup.spec.archive.url).map_err(|e| {
        ExecutionRefusal::new(
            TERMINAL_STATE_ARCHIVE_URL_UNREADABLE,
            format!("the archive URL on {name} is not an object-store location: {e}"),
        )
    })?;
    if backup
        .spec
        .archive
        .secret_ref
        .as_ref()
        .is_some_and(|secret| secret.name.trim().is_empty())
    {
        return Err(ExecutionRefusal::new(
            TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
            format!("spec.archive.secretRef on {name} names no Secret"),
        ));
    }
    let args = runner_argv(identity.trigger, &identity.id);
    Ok(BackupExecutionInputs {
        version: INPUTS_VERSION.to_string(),
        source: SourceInputs {
            cluster: ObjectIdentity {
                namespace: cluster
                    .namespace()
                    .unwrap_or_else(|| identity.backup.namespace.clone()),
                name: cluster_name,
                uid: cluster_uid,
            },
            bootstrap_servers: cluster.spec.bootstrap_servers.clone(),
            auth,
            observed_cluster_id: cluster
                .status
                .as_ref()
                .and_then(|status| status.cluster_id.clone())
                .filter(|id| !id.is_empty()),
        },
        topics: backup.spec.topics.clone(),
        archive: ArchiveInputs {
            url: backup.spec.archive.url.clone(),
            storage,
            addressing_env: addressing_env
                .iter()
                .map(|(name, value)| EnvValue {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect(),
        },
        runner: RunnerInputs {
            args,
            deadline_seconds: backup.spec.deadline_seconds,
            settings: RunnerSettings::from(&BackupSettings::default()),
        },
        execution: identity,
    })
}

/// Inputs in their one canonical encoding, with the digest that names them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrozenInputs {
    /// The typed snapshot.
    pub inputs: BackupExecutionInputs,
    /// `logweir_core::det_json` of [`FrozenInputs::inputs`], the exact
    /// `execution-inputs.json` bytes.
    pub canonical: String,
    /// `sha256_prefixed` of [`FrozenInputs::canonical`].
    pub sha256: String,
}

/// The one canonical encoding of a snapshot.
///
/// # Errors
///
/// A conflict for a snapshot that does not serialise, which a typed value
/// built by this module cannot produce.
pub fn canonical_inputs(inputs: &BackupExecutionInputs) -> Result<String, ExecutionRefusal> {
    let bytes = logweir_core::det_json::to_deterministic_json(inputs).map_err(|e| {
        ExecutionRefusal::conflict(format!("the execution inputs do not serialise: {e}"))
    })?;
    String::from_utf8(bytes)
        .map_err(|e| ExecutionRefusal::conflict(format!("the execution inputs are not UTF-8: {e}")))
}

impl FrozenInputs {
    /// Encode `inputs` canonically and digest the bytes.
    ///
    /// # Errors
    ///
    /// See [`canonical_inputs`].
    pub fn freeze(inputs: BackupExecutionInputs) -> Result<Self, ExecutionRefusal> {
        let canonical = canonical_inputs(&inputs)?;
        let sha256 = sha256_prefixed(canonical.as_bytes());
        Ok(Self {
            inputs,
            canonical,
            sha256,
        })
    }

    /// The `backup.yaml` document, rendered from the snapshot.
    #[must_use]
    pub fn backup_spec(&self) -> BackupSpec {
        let inputs = &self.inputs;
        BackupSpec {
            source: BackupSourceSpec {
                bootstrap_servers: inputs.source.bootstrap_servers.clone(),
                auth: inputs.source.auth.clone(),
                topics: inputs.topics.clone(),
            },
            storage: inputs.archive.storage.clone(),
            backup_id: inputs.execution.id.clone(),
            backup: BackupSettings::from(&inputs.runner.settings),
        }
    }

    /// The `allowed-clusters.json` document, rendered from the snapshot.
    ///
    /// `allowed_cluster_ids` is EMPTY on the backup path: it is the restore
    /// TARGET allowlist, and the runner refuses a source whose broker-observed
    /// id appears in it. See `controllers::backup::plan_allowed_clusters`.
    #[must_use]
    pub fn allowed_clusters(&self) -> AllowedClusters {
        AllowedClusters {
            allowed_cluster_ids: Vec::new(),
            source_cluster_id: self.inputs.source.observed_cluster_id.clone(),
        }
    }

    /// The three ConfigMap keys: the two runner documents and the snapshot.
    ///
    /// # Errors
    ///
    /// A conflict for a document that does not serialise.
    pub fn documents(&self) -> Result<BTreeMap<String, String>, ExecutionRefusal> {
        let spec = serde_yaml::to_string(&self.backup_spec()).map_err(|e| {
            ExecutionRefusal::new(
                TERMINAL_STATE_ARCHIVE_URL_UNREADABLE,
                format!("the rendered backup spec does not serialise: {e}"),
            )
        })?;
        let allowed = serde_json::to_string_pretty(&self.allowed_clusters()).map_err(|e| {
            ExecutionRefusal::new(
                TERMINAL_STATE_ARCHIVE_URL_UNREADABLE,
                format!("the rendered cluster allowlist does not serialise: {e}"),
            )
        })?;
        Ok([
            (PLAN_SPEC_KEY.to_string(), spec),
            (PLAN_ALLOWED_CLUSTERS_KEY.to_string(), allowed),
            (INPUTS_KEY.to_string(), self.canonical.clone()),
        ]
        .into_iter()
        .collect())
    }

    /// The `status.execution` projection for these inputs.
    #[must_use]
    pub fn status(&self) -> BackupExecution {
        BackupExecution {
            id: self.inputs.execution.id.clone(),
            inputs_ref: LocalRef {
                name: plan_config_map_name(&self.inputs.execution.backup.name),
            },
            inputs_sha256: self.sha256.clone(),
        }
    }
}

/// The exact single owner reference a `Backup`-owned object carries.
#[must_use]
pub fn backup_owner_reference(backup_name: &str, backup_uid: &str) -> OwnerReference {
    OwnerReference {
        api_version: Backup::api_version(&()).to_string(),
        kind: Backup::kind(&()).to_string(),
        name: backup_name.to_string(),
        uid: backup_uid.to_string(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

/// Whether `metadata` is owned by exactly this `Backup` incarnation and by
/// nothing else.
///
/// A SECOND OWNER IS REFUSED EVEN BESIDE A COMPLETE FIRST ONE: it can keep the
/// object alive after this `Backup` is deleted, so a later `Backup` of the same
/// name could find inputs this one froze.
#[must_use]
pub fn has_exact_backup_owner(metadata: &ObjectMeta, backup_name: &str, backup_uid: &str) -> bool {
    let owners = metadata.owner_references.as_deref().unwrap_or_default();
    owners.len() == 1
        && owners
            .first()
            .is_some_and(|owner| *owner == backup_owner_reference(backup_name, backup_uid))
}

/// The create-only, immutable plan ConfigMap for frozen inputs.
///
/// # Errors
///
/// A refusal when the `Backup` has no namespace or UID, or a document does not
/// serialise.
pub fn inputs_config_map(
    backup: &Backup,
    frozen: &FrozenInputs,
) -> Result<ConfigMap, ExecutionRefusal> {
    let name = backup.name_any();
    let namespace = backup.namespace().ok_or_else(|| {
        ExecutionRefusal::spec(format!("the Backup {name} carries no metadata.namespace"))
    })?;
    let uid = backup.uid().ok_or_else(|| {
        ExecutionRefusal::spec(format!("the Backup {name} carries no metadata.uid"))
    })?;
    Ok(ConfigMap {
        metadata: ObjectMeta {
            name: Some(plan_config_map_name(&name)),
            namespace: Some(namespace),
            annotations: Some(
                [
                    (
                        EXECUTION_ID_ANNOTATION.to_string(),
                        frozen.inputs.execution.id.clone(),
                    ),
                    (INPUTS_SHA256_ANNOTATION.to_string(), frozen.sha256.clone()),
                ]
                .into_iter()
                .collect(),
            ),
            owner_references: Some(vec![backup_owner_reference(&name, &uid)]),
            ..ObjectMeta::default()
        },
        data: Some(frozen.documents()?),
        immutable: Some(true),
        ..ConfigMap::default()
    })
}

/// Admit an existing plan ConfigMap as this run's frozen inputs, or refuse.
///
/// `desired` is a FRESH resolution from the current spec, referents and
/// controller environment; `recorded` is `status.execution` when the controller
/// already wrote it. Returns the STORED snapshot, which the Job is built from.
///
/// # Errors
///
/// [`TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT`] naming the first property that
/// does not hold: exact single owner, `immutable: true`, the three keys and no
/// binary data, a supported and canonical snapshot, the digest and id
/// annotations, documents re-rendered byte for byte, binding to this `Backup`
/// and its identity, the recorded status, and executable equality with the
/// fresh resolution. A mutable plan written by a controller that predates
/// frozen inputs is refused here too: it cannot carry a snapshot, and nothing
/// is overwritten to make it carry one.
pub fn verify_frozen_config_map(
    existing: &ConfigMap,
    backup: &Backup,
    desired: &FrozenInputs,
    recorded: Option<&BackupExecution>,
) -> Result<FrozenInputs, ExecutionRefusal> {
    let backup_name = backup.name_any();
    let cm_name = plan_config_map_name(&backup_name);
    let backup_uid = backup.uid().unwrap_or_default();
    let refuse = |detail: String| {
        Err(ExecutionRefusal::conflict(format!(
            "the ConfigMap {cm_name} is not this Backup's frozen execution inputs: {detail}; it \
             is neither overwritten nor adopted. Delete this Backup and create a new one"
        )))
    };

    if existing.metadata.name.as_deref() != Some(cm_name.as_str())
        || existing.metadata.namespace != backup.namespace()
    {
        return refuse("its name or namespace is not the Backup's plan ConfigMap".to_string());
    }
    let owners = existing
        .metadata
        .owner_references
        .as_deref()
        .unwrap_or_default();
    if !owners.iter().any(|owner| owner.uid == backup_uid) {
        return refuse(format!(
            "it carries no owner reference with this Backup's UID {backup_uid} (a foreign or \
             ownerless object)"
        ));
    }
    if owners.len() != 1 {
        return refuse(format!(
            "it carries {} owner references; exactly one, this Backup, is required",
            owners.len()
        ));
    }
    if !has_exact_backup_owner(&existing.metadata, &backup_name, &backup_uid) {
        return refuse(
            "its owner reference is not the complete controller reference for this Backup \
             (apiVersion, kind, name, controller and blockOwnerDeletion)"
                .to_string(),
        );
    }
    let data = existing.data.clone().unwrap_or_default();
    let Some(stored) = data.get(INPUTS_KEY) else {
        return refuse(if existing.immutable == Some(true) {
            format!("it has no {INPUTS_KEY}")
        } else {
            format!(
                "it is a mutable plan without {INPUTS_KEY}, written by a controller that \
                 predates frozen execution inputs"
            )
        });
    };
    if existing.immutable != Some(true) {
        return refuse("it is not immutable".to_string());
    }
    if existing
        .binary_data
        .as_ref()
        .is_some_and(|binary| !binary.is_empty())
    {
        return refuse("it carries binaryData".to_string());
    }
    let mut keys: Vec<&str> = data.keys().map(String::as_str).collect();
    keys.sort_unstable();
    let mut expected_keys = vec![PLAN_ALLOWED_CLUSTERS_KEY, PLAN_SPEC_KEY, INPUTS_KEY];
    expected_keys.sort_unstable();
    if keys != expected_keys {
        return refuse(format!("its keys are {keys:?}, not {expected_keys:?}"));
    }
    let version = serde_json::from_str::<serde_json::Value>(stored)
        .ok()
        .and_then(|value| {
            value
                .get("version")
                .and_then(|v| v.as_str())
                .map(str::to_string)
        });
    if version.as_deref() != Some(INPUTS_VERSION) {
        return refuse(format!(
            "{INPUTS_KEY} names grammar {:?}, and this controller reads {INPUTS_VERSION}",
            version.as_deref().unwrap_or("<none>")
        ));
    }
    let parsed = match serde_json::from_str::<BackupExecutionInputs>(stored) {
        Ok(parsed) => parsed,
        Err(e) => return refuse(format!("{INPUTS_KEY} is not a typed snapshot: {e}")),
    };
    let frozen = FrozenInputs::freeze(parsed)?;
    if frozen.canonical != *stored {
        return refuse(format!("{INPUTS_KEY} is not in its canonical encoding"));
    }
    let annotations = existing.metadata.annotations.clone().unwrap_or_default();
    if annotations.get(INPUTS_SHA256_ANNOTATION) != Some(&frozen.sha256) {
        return refuse(format!(
            "its {INPUTS_SHA256_ANNOTATION} annotation is not the digest of {INPUTS_KEY} ({})",
            frozen.sha256
        ));
    }
    if annotations.get(EXECUTION_ID_ANNOTATION) != Some(&frozen.inputs.execution.id) {
        return refuse(format!(
            "its {EXECUTION_ID_ANNOTATION} annotation is not the snapshot's execution id"
        ));
    }
    if frozen.documents()? != data {
        return refuse(
            "its runner documents are not the documents rendered from its snapshot".to_string(),
        );
    }
    if let Some(recorded) = recorded {
        if recorded.inputs_sha256 != frozen.sha256
            || recorded.id != frozen.inputs.execution.id
            || recorded.inputs_ref.name != cm_name
        {
            return refuse(format!(
                "status.execution records id {} and digest {} in {}, and the ConfigMap holds id \
                 {} and digest {}",
                recorded.id,
                recorded.inputs_sha256,
                recorded.inputs_ref.name,
                frozen.inputs.execution.id,
                frozen.sha256
            ));
        }
    }
    if frozen.inputs.execution != desired.inputs.execution {
        return refuse(format!(
            "its snapshot binds execution {} of {}/{} (uid {}), and this Backup derives execution \
             {}",
            frozen.inputs.execution.id,
            frozen.inputs.execution.backup.namespace,
            frozen.inputs.execution.backup.name,
            frozen.inputs.execution.backup.uid,
            desired.inputs.execution.id
        ));
    }
    if frozen.inputs.executable() != desired.inputs.executable() {
        return refuse(
            "its snapshot differs from the inputs resolved now from spec, the referenced \
             KafkaCluster and the controller's archive addressing, so frozen plan bytes would be \
             combined with a changed connection"
                .to_string(),
        );
    }
    Ok(frozen)
}

// ---------------------------------------------------------------------------
// The Job, and what is observed about it
// ---------------------------------------------------------------------------

/// Whether a Job occupying the `Backup`'s name is controlled by exactly this
/// `Backup` incarnation. Its inputs may still be legacy; its identity may never
/// be inferred from the name alone.
#[must_use]
pub fn compatible_backup_job(job: &Job, backup: &Backup) -> bool {
    let Some(uid) = backup.uid() else {
        return false;
    };
    job.metadata.name.as_deref() == Some(backup.name_any().as_str())
        && job.metadata.namespace == backup.namespace()
        && has_exact_backup_owner(&job.metadata, &backup.name_any(), &uid)
}

/// Whether an existing Job runs this `Backup`'s frozen inputs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JobProvenance {
    /// The Job's inputs digest equals `status.execution.inputsSha256`.
    Frozen,
    /// Neither the Job nor the `Backup` names an inputs digest: an older
    /// controller created the Job.
    Legacy,
    /// Exactly one side names a digest, or the two differ.
    Mismatch {
        /// The Job's annotation.
        job: Option<String>,
        /// `status.execution.inputsSha256`.
        recorded: Option<String>,
    },
}

/// Classify an existing Job against the recorded execution.
#[must_use]
pub fn job_provenance(job: &Job, recorded: Option<&BackupExecution>) -> JobProvenance {
    let on_job = job
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(INPUTS_SHA256_ANNOTATION))
        .cloned();
    let recorded = recorded.map(|execution| execution.inputs_sha256.clone());
    match (on_job, recorded) {
        (None, None) => JobProvenance::Legacy,
        (Some(job), Some(recorded)) if job == recorded => JobProvenance::Frozen,
        (job, recorded) => JobProvenance::Mismatch { job, recorded },
    }
}

/// Stamp a runner Job, and its pod template, with the run identity and inputs
/// digest it was built from.
pub fn annotate_runner_job(job: &mut Job, frozen: &FrozenInputs) {
    let pairs = [
        (
            EXECUTION_ID_ANNOTATION.to_string(),
            frozen.inputs.execution.id.clone(),
        ),
        (INPUTS_SHA256_ANNOTATION.to_string(), frozen.sha256.clone()),
    ];
    job.metadata
        .annotations
        .get_or_insert_with(BTreeMap::new)
        .extend(pairs.clone());
    if let Some(spec) = job.spec.as_mut() {
        spec.template
            .metadata
            .get_or_insert_with(ObjectMeta::default)
            .annotations
            .get_or_insert_with(BTreeMap::new)
            .extend(pairs);
    }
}

/// The legacy runner-argv annotation, as observed. Never its content.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunnerArgvAnnotation {
    /// Not a JSON array of strings.
    Malformed {
        /// The value's length in bytes.
        bytes: usize,
        /// `sha256_prefixed` of the value.
        sha256: String,
    },
    /// A JSON array of strings, which is still not executed.
    Parsed {
        /// The value's length in bytes.
        bytes: usize,
        /// `sha256_prefixed` of the value.
        sha256: String,
        /// The parsed array, for comparison only.
        argv: Vec<String>,
    },
}

/// Observe [`RUNNER_ARGV_ANNOTATION`] on `backup`, if present.
#[must_use]
pub fn runner_argv_annotation(backup: &Backup) -> Option<RunnerArgvAnnotation> {
    let raw = backup.annotations().get(RUNNER_ARGV_ANNOTATION)?;
    let bytes = raw.len();
    let sha256 = sha256_prefixed(raw.as_bytes());
    Some(match serde_json::from_str::<Vec<String>>(raw) {
        Ok(argv) => RunnerArgvAnnotation::Parsed {
            bytes,
            sha256,
            argv,
        },
        Err(_) => RunnerArgvAnnotation::Malformed { bytes, sha256 },
    })
}

/// The argv this controller runs, or would run, for `backup`: from
/// `status.execution` once inputs are frozen, else from the derivable identity.
#[must_use]
pub fn derived_runner_argv(backup: &Backup) -> Option<Vec<String>> {
    let trigger = match backup.spec.triggered_by.as_str() {
        TRIGGER_MANUAL => ExecutionTrigger::Manual,
        TRIGGER_SCHEDULE => ExecutionTrigger::Schedule,
        _ => return None,
    };
    if let Some(execution) = backup.status.as_ref().and_then(|s| s.execution.as_ref()) {
        return Some(runner_argv(trigger, &execution.id));
    }
    execution_identity(backup)
        .ok()
        .map(|identity| runner_argv(identity.trigger, &identity.id))
}
