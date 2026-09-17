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
    TERMINAL_STATE_REFERENT_NOT_FOUND, TERMINAL_STATE_SCHEDULED_IDENTITY_MISMATCH,
};
use crate::connection::ConnectionUse;
use crate::controllers::backup::{plan_config_map_name, PLAN_ALLOWED_CLUSTERS_KEY, PLAN_SPEC_KEY};
use crate::crds::backup::{Backup, BackupExecution, BackupSpec as BackupCrdSpec, TriggerKind};
use crate::crds::backup_schedule::BackupSchedule;
use crate::crds::kafka_cluster::KafkaCluster;
use crate::crds::selection::{Coverage, IncompleteDiscovery, SelectionMode, SelectionStatus};
use crate::crds::LocalRef;
use crate::destination::ResolvedDestinationSnapshot;
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

/// PLAT-06.1's snapshot grammar: identity, source, topics, archive, runner.
///
/// STILL READ, STILL EXECUTED, NEVER REWRITTEN. Every `Backup` frozen by a
/// controller that predates D1 carries this version, and a run in flight at
/// upgrade time must keep executing the bytes it was admitted with — so this
/// constant is not "the old one", it is one of the two grammars
/// [`verify_frozen_config_map`] admits. See [`INPUTS_VERSIONS_READ`].
pub const INPUTS_VERSION_V1: &str = "logweir.dev/backup-execution-inputs/v1";

/// D1 §3.3's grammar: [`INPUTS_VERSION_V1`] **plus** the trigger, the schedule
/// revision, the run policy digest, the frozen selection and the reserved
/// destination block.
///
/// ADDITIVE, AND THE ADDITIONS ARE ALL OPTIONAL. A `v1` document deserialises
/// into the same type with every `v2` field absent and re-encodes to the exact
/// bytes it was stored as, which is what keeps a frozen `v1` plan verifiable
/// after the upgrade (D-SEAMS **S4**).
pub const INPUTS_VERSION_V2: &str = "logweir.dev/backup-execution-inputs/v2";

/// The snapshot grammar this controller WRITES.
pub const INPUTS_VERSION: &str = INPUTS_VERSION_V2;

/// Every snapshot grammar this controller READS, newest first. A snapshot
/// naming any other version is a conflict, never a best-effort parse.
pub const INPUTS_VERSIONS_READ: [&str; 2] = [INPUTS_VERSION_V2, INPUTS_VERSION_V1];

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

impl From<crate::connection::ConnectionRefusal> for ExecutionRefusal {
    /// A connection the ONE resolver refuses is a refusal of the run, with the
    /// resolver's own terminal state and message (PLAT-07.1). The state is
    /// carried rather than flattened to one of this module's: a caller that
    /// sees `ConnectionConfigInvalid` is being told the saved connection is
    /// wrong, not that the snapshot is.
    fn from(refusal: crate::connection::ConnectionRefusal) -> Self {
        Self::new(refusal.reason, refusal.message)
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

/// Whether `slot` is a canonical `yyyymmdd-hhmmss` instant that EXISTS on the
/// calendar — `20261309-031700` is fifteen characters of digits and a hyphen,
/// and is not a date.
fn valid_slot(slot: &str) -> bool {
    const FORMAT: &str = "%Y%m%d-%H%M%S";
    slot.len() == 15
        && chrono::NaiveDateTime::parse_from_str(slot, FORMAT)
            .is_ok_and(|t| t.format(FORMAT).to_string() == slot)
}

/// Derive the run identity from the typed spec and server-generated metadata.
///
/// # ONE DERIVATION, IN [`crate::identity`], AND THIS IS ITS EXECUTION-SIDE
/// PROJECTION
///
/// PLAT-06.1 derived the identity here, from `spec.triggeredBy` plus the
/// `BackupSchedule` controller owner reference. D1 §3.1 moves the derivation
/// into [`crate::identity::run_identity`], which reads the object and nothing
/// else and knows all four trigger kinds — so a retry named `-r1` cannot be
/// minted by the scheduler and adopted as attempt 0 here. This function calls
/// that one derivation and projects it into the `execution` block of the
/// frozen grammar.
///
/// **EVERY PLAT-06.1 OBJECT IS STILL ADMITTED, BYTE FOR BYTE.** A scheduled
/// `Backup` created before `spec.trigger` existed carries `triggeredBy:
/// schedule`, a `spec.scheduleRef` without a `uid` and the controller owner
/// reference; D1 §3.1 rule 4 reads it as `Scheduled`/attempt 0 and rule 3's
/// last clause takes the UID from that owner reference, so its execution id is
/// `<owner uid>-<slot>` — exactly what PLAT-06.1 computed. Nothing is
/// converted and no write happens on upgrade.
///
/// # What this adds on top of the one derivation
///
/// Two checks that belong to the EXECUTION contract and not to identity:
///
/// 1. **`spec.triggeredBy` is still a two-value vocabulary.** The value becomes
///    the signed receipt's `triggered_by`, so an unknown one is
///    [`TERMINAL_STATE_EXECUTION_SPEC_INVALID`] exactly as it was — and it must
///    AGREE with `spec.trigger.kind`, or a `Manual` run would sign a receipt
///    saying `schedule`.
/// 2. **A legacy owner reference must name the schedule `spec.scheduleRef`
///    names.** `run_identity` takes the UID from the first complete
///    `BackupSchedule` controller owner; PLAT-06.1 additionally required that
///    owner's NAME to equal `scheduleRef.name`, and dropping that would let a
///    `Backup` naming schedule `a` execute under schedule `b`'s archive prefix.
///
/// # Errors
///
/// [`TERMINAL_STATE_EXECUTION_SPEC_INVALID`] for a missing namespace or UID and
/// for an unknown `spec.triggeredBy`;
/// [`crate::conditions::TERMINAL_STATE_SCHEDULED_IDENTITY_MISMATCH`] for a
/// `triggeredBy` that contradicts `spec.trigger.kind` and for every identity
/// [`crate::identity::run_identity`] refuses;
/// [`crate::conditions::TERMINAL_STATE_NAME_TOO_LONG`] for a composed name that
/// does not fit. A hand-written `Backup` that claims to be a scheduled run is
/// refused rather than silently re-labelled: its signed receipt would otherwise
/// say `schedule` about a run no schedule created.
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
        uid,
    };

    // (1) THE RECEIPT'S OWN VOCABULARY, UNCHANGED.
    let declared = backup.spec.triggered_by.as_str();
    if declared != TRIGGER_MANUAL && declared != TRIGGER_SCHEDULE {
        return Err(ExecutionRefusal::spec(format!(
            "spec.triggeredBy is `{}`; this controller runs `manual` and `schedule` Backups \
             only, and the value becomes the signed receipt's triggered_by",
            shown(declared)
        )));
    }

    let run = crate::identity::run_identity(backup)
        .map_err(|error| ExecutionRefusal::new(error.terminal_state(), error.to_string()))?;

    let trigger = match run.kind {
        TriggerKind::Manual => ExecutionTrigger::Manual,
        TriggerKind::Scheduled | TriggerKind::CatchUp | TriggerKind::Retry => {
            ExecutionTrigger::Schedule
        }
    };
    if trigger.as_str() != declared {
        return Err(ExecutionRefusal::new(
            TERMINAL_STATE_SCHEDULED_IDENTITY_MISMATCH,
            format!(
                "spec.trigger.kind is {:?} but spec.triggeredBy is `{}`; the two-value field is \
                 what the signed receipt carries, so a {} run may not sign a receipt saying \
                 `{}`",
                run.kind,
                shown(declared),
                trigger.as_str(),
                shown(declared)
            ),
        ));
    }

    // PLAT-06.1's CALENDAR check on the slot, kept. `identity::run_identity`
    // checks the SHAPE — fifteen characters, digits and one hyphen — which
    // admits `20261309-031700`, a thirteenth month. A slot is a UTC instant and
    // an object named after one that does not exist would take an archive
    // prefix no schedule can ever produce.
    if let Some(slot) = run.slot.as_deref() {
        if !valid_slot(slot) {
            return Err(ExecutionRefusal::new(
                TERMINAL_STATE_SCHEDULED_IDENTITY_MISMATCH,
                format!(
                    "spec.slot `{}` is not a UTC instant in yyyymmdd-hhmmss form",
                    shown(slot)
                ),
            ));
        }
    }

    let schedule = match run.schedule {
        None => None,
        Some(schedule) => {
            // (2) PLAT-06.1's owner-NAME check, kept.
            if schedule.from_owner_reference {
                let named = backup.owner_references().iter().any(|owner| {
                    owner.controller == Some(true)
                        && owner.api_version == BackupSchedule::api_version(&())
                        && owner.kind == BackupSchedule::kind(&())
                        && owner.name == schedule.name
                        && owner.uid == schedule.uid
                });
                if !named {
                    return Err(ExecutionRefusal::new(
                        TERMINAL_STATE_SCHEDULED_IDENTITY_MISMATCH,
                        format!(
                            "spec.scheduleRef names `{}` and carries no uid, and this Backup's \
                             BackupSchedule controller owner reference names a different \
                             schedule; the legacy identity is that owner's UID, so the run would \
                             execute under another schedule's archive prefix",
                            shown(&schedule.name)
                        ),
                    ));
                }
            }
            Some(ScheduleTrigger {
                name: schedule.name,
                uid: schedule.uid,
                slot: run.slot.clone().unwrap_or_default(),
            })
        }
    };

    Ok(ExecutionIdentity {
        id: run.execution_id,
        trigger,
        backup: object,
        schedule,
    })
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
    /// The private CA the connection verifies the broker with, as the
    /// reference [`crate::connection::resolve`] decided — Secret or ConfigMap,
    /// object name and data key (PLAT-07.1).
    ///
    /// EXECUTABLE, so a `tlsCa` edited after the freeze is a
    /// [`TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT`] and never a Job that
    /// verifies the broker against a different root than the plan was approved
    /// for. A CA CERTIFICATE IS PUBLIC MATERIAL, which is why its reference may
    /// be named here while the SASL password's may not: the password reference
    /// is projected into the Job from the pinned `KafkaCluster` and resolved by
    /// the kubelet, and appears in no snapshot, no document and no status.
    ///
    /// Absent for every connection that names no CA, so a snapshot frozen
    /// before this field existed re-encodes to the same bytes and is still
    /// admitted under grammar [`INPUTS_VERSION`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_ca: Option<crate::connection::CaReference>,
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

// ---------------------------------------------------------------------------
// Grammar v2 (D1 §3.3): the trigger, the revision, the selection, the
// reserved destination block
// ---------------------------------------------------------------------------

/// `false` as a serde skip predicate, for a `bool` that is absent when unset.
fn is_false(value: &bool) -> bool {
    !*value
}

/// The `trigger` block: WHICH KIND of run this is, and where in its slot's
/// attempt chain it sits (D1 §3.1).
///
/// # Why this is frozen beside `execution`, and is not `execution`
///
/// [`ExecutionIdentity`] is `v1`'s and its bytes are load-bearing: it is
/// compared field for field against a fresh derivation on every later pass, and
/// every `Backup` frozen before D1 carries it. Adding `kind` and `attempt` to
/// it would have changed the bytes of every stored snapshot. This block carries
/// the finer trigger additively, and `execution.id` — which already encodes the
/// attempt through `slot::backup_id_for_attempt`'s `-r<k>` suffix — stays the
/// one identity the runner is handed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunInputs {
    /// `Scheduled`, `CatchUp`, `Retry` or `Manual`.
    pub kind: TriggerKind,
    /// `0` for everything but a retry.
    pub attempt: u32,
    /// `metadata.name` of the attempt this one retries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_of: Option<String>,
    /// The IANA zone the slot was computed in. INFORMATIONAL: the slot itself
    /// is a UTC instant, and this records the zone so a history row keeps its
    /// local time after somebody edits the schedule's `timeZone`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_zone: Option<String>,
}

/// The `scheduleRef` block: which `BackupSchedule` revision this run copied
/// (D1 §5.3).
///
/// **IT SURVIVES THE SCHEDULE.** PLAT-05.2 stops deleting a schedule from
/// deleting its history, so the revision a run executed has to be recorded
/// somewhere that outlives the object — and the frozen inputs are the only
/// immutable place a run has.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ScheduleRefInputs {
    /// The `BackupSchedule` name, in this namespace.
    pub name: String,
    /// Its UID. Absent only for an ad-hoc `Backup` that names a schedule
    /// without one — a hand-written object, never one the API or the scheduler
    /// creates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    /// Its `metadata.generation` when this run was admitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<i64>,
    /// The run policy digest COPIED from the schedule at that generation.
    ///
    /// Beside [`BackupExecutionInputs::run_policy_sha256`], which is the digest
    /// this run's OWN fields produce. D1 §3.1 rule 5 requires the two to be
    /// equal, and [`crate::identity::check_run_policy_digest`] is what refuses
    /// the run when they are not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_policy_sha256: Option<String>,
}

/// The exclusions a dynamic selection applied, canonicalised.
///
/// SORTED AND DEDUPLICATED, like the run policy digest's copy, so two runs that
/// applied the same exclusions freeze the same bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExclusionsInputs {
    /// Exact names.
    pub topics: Vec<String>,
    /// Literal prefixes, never patterns.
    pub prefixes: Vec<String>,
}

/// A bounded set of names discovery removed, with the count it was cut from.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DiscoveryNames {
    /// How many there were.
    pub count: i64,
    /// Up to the first `count` of them, byte-sorted. BOUNDED: a plan
    /// `ConfigMap` is one MiB and a cluster may hold thousands of topics.
    pub names: Vec<String>,
    /// Whether `names` was cut short.
    #[serde(default, skip_serializing_if = "is_false")]
    pub truncated: bool,
}

/// What the per-run discovery observed — D1 §3.3's `selection.discovery`.
///
/// **RESERVED FOR D1 W5 (PLAT-09.2), AND NOTHING WRITES IT YET.** The grammar
/// declares it here, inside PLAT-06.1's one frozen document (D-SEAMS **S4**),
/// so the worker that resolves dynamic selection extends this validation rather
/// than adding a parallel freeze.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DiscoveryInputs {
    /// When the runner observed the cluster, RFC 3339.
    pub observed_at: String,
    /// The broker-reported cluster id the listing came from.
    pub cluster_id: String,
    /// `unknown`, `limited` or `attestedComplete` — D-SEAMS **S3**. A
    /// successful listing ALONE is `unknown`.
    pub visibility: String,
    /// How the listing was obtained. `metadata-list` today.
    pub basis: String,
    /// `sha256:<hex>` over the canonical discovery result the run parsed.
    pub result_sha256: String,
    /// How many topics the principal could see.
    pub visible_topic_count: i64,
    /// The internal topics excluded.
    pub internal_excluded: DiscoveryNames,
    /// The topics the exclusion rules removed.
    pub excluded_by_rule: DiscoveryNames,
    /// How many the broker refused to describe.
    pub limited_topic_count: i64,
    /// The discovery Job this result was read from.
    pub discovery_job: String,
}

/// The `selection` block: HOW this run chose its topics, and what it may
/// honestly claim to have covered (D1 §3.3, §7.4).
///
/// # The names are NOT repeated here
///
/// D1 §3.3 sketches a `selection.topics`. This grammar does not carry one:
/// `v1`'s top-level [`BackupExecutionInputs::topics`] IS the exact frozen list
/// — the bytes `backup.yaml` is rendered from and the bytes a later pass
/// compares — and a second copy inside this block would be a third copy of a
/// list D1 §7.2 R8 bounds at 256 KiB of names, in a `ConfigMap` bounded at one
/// MiB, with two places for one answer to drift. The count and the byte size
/// are recorded instead, and [`BackupExecutionInputs::selection`] is the
/// PROVENANCE of `topics`, never a second statement of it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SelectionInputs {
    /// `SelectedTopics` or `AllUserTopics`.
    pub mode: SelectionMode,
    /// What this run may claim to have covered. Only
    /// [`Coverage::AllUserTopicsAttested`] ever means "everything".
    pub coverage: Coverage,
    /// How many topics [`BackupExecutionInputs::topics`] holds.
    pub resolved_topic_count: i64,
    /// How many bytes those names take.
    pub resolved_topic_bytes: i64,
    /// The exclusions, in dynamic mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude: Option<ExclusionsInputs>,
    /// The policy's answer to incomplete visibility, in dynamic mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incomplete_discovery: Option<IncompleteDiscovery>,
    /// What discovery observed. Reserved for D1 W5.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery: Option<DiscoveryInputs>,
}

impl SelectionInputs {
    /// The `status.selection` projection of this block.
    ///
    /// ONE PROJECTION, SO THE STATUS AND THE FROZEN DOCUMENT CANNOT DISAGREE.
    /// A reader comparing `status.selection.resolvedTopicCount` against the
    /// plan is comparing two renderings of the same value, not two counts.
    #[must_use]
    pub fn status(&self) -> SelectionStatus {
        let discovery = self.discovery.as_ref();
        SelectionStatus {
            mode: self.mode,
            coverage: self.coverage,
            visibility: discovery.map(|d| d.visibility.clone()),
            resolved_topic_count: self.resolved_topic_count,
            resolved_topic_bytes: Some(self.resolved_topic_bytes),
            internal_excluded_count: discovery.map(|d| d.internal_excluded.count),
            excluded_by_rule_count: discovery.map(|d| d.excluded_by_rule.count),
            limited_topic_count: discovery.map(|d| d.limited_topic_count),
            discovery_observed_at: discovery.and_then(|d| {
                chrono::DateTime::parse_from_rfc3339(&d.observed_at)
                    .ok()
                    .map(|t| t.with_timezone(&chrono::Utc))
            }),
            discovery_sha256: discovery.map(|d| d.result_sha256.clone()),
        }
    }
}

/// What a run's topic resolution decided: the exact frozen list and the
/// provenance block that explains it.
///
/// # THE SEAM D1 W5 PLUGS INTO
///
/// [`resolve_inputs`] takes one of these and never computes a selection of its
/// own. Today the only producer is [`ResolvedSelection::named`], the named
/// allowlist. W5 (PLAT-09.2) adds a second producer — the discovery result —
/// and passes it to the same function, so the dynamic path freezes through the
/// same code, the same canonical encoding and the same
/// [`verify_frozen_config_map`] comparison as the named one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedSelection {
    /// The exact names handed to `backup.yaml`, in the order they are handed.
    pub topics: Vec<String>,
    /// Where those names came from.
    pub selection: SelectionInputs,
}

impl ResolvedSelection {
    /// The named allowlist: `spec.topics` VERBATIM, coverage
    /// [`Coverage::NamedTopics`].
    ///
    /// VERBATIM AND NOT SORTED. `Backup.spec.topics` keeps the user's order all
    /// the way to the engine — only [`crate::policy::run_policy_sha256`]
    /// canonicalises — and reordering here would change the frozen bytes of
    /// every run whose list is not already sorted.
    #[must_use]
    pub fn named(spec: &BackupCrdSpec) -> Self {
        let topics = spec.topics.clone();
        Self {
            selection: SelectionInputs {
                mode: SelectionMode::SelectedTopics,
                coverage: Coverage::NamedTopics,
                resolved_topic_count: i64::try_from(topics.len()).unwrap_or(i64::MAX),
                resolved_topic_bytes: i64::try_from(topics.iter().map(String::len).sum::<usize>())
                    .unwrap_or(i64::MAX),
                exclude: None,
                incomplete_discovery: None,
                discovery: None,
            },
            topics,
        }
    }
}

/// The canonical typed snapshot of everything one run executes — the
/// `execution-inputs.json` document.
///
/// VERSIONED AND CLOSED. Every struct denies unknown fields and the controller
/// re-encodes a stored snapshot and requires the same bytes, so a snapshot
/// written by a later grammar is a conflict and never a partial read.
///
/// # `v1` and `v2` are ONE TYPE, and that is what makes the upgrade free
///
/// D-SEAMS **S4**: this document is the single frozen grammar and every worker
/// that freezes more adds a BLOCK to it. So `v2` is `v1` plus five optional
/// fields, and a `v1` document deserialises with all five absent. Because each
/// one is `skip_serializing_if = "Option::is_none"` and
/// `logweir_core::det_json` emits struct fields in DECLARATION order, that
/// document re-encodes to the exact bytes it was stored as — which is the
/// property [`verify_frozen_config_map`] rests on, and the reason a `Backup`
/// frozen by a PLAT-06.1 controller keeps running across the upgrade instead of
/// turning into a `PlanConfigMapConflict`.
///
/// **THE ORDER OF THESE FIELDS IS THE WIRE FORMAT.** Reordering them re-encodes
/// every stored snapshot differently and turns every running `Backup` terminal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BackupExecutionInputs {
    /// [`INPUTS_VERSION_V1`] or [`INPUTS_VERSION_V2`]; newly frozen documents
    /// carry [`INPUTS_VERSION`].
    pub version: String,
    /// The run identity. `v1`.
    pub execution: ExecutionIdentity,
    /// Which kind of run, and where in its attempt chain. `v2`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger: Option<RunInputs>,
    /// The `BackupSchedule` revision this run copied. `v2`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_ref: Option<ScheduleRefInputs>,
    /// The digest of THIS run's own policy fields (D1 §3.2). `v2`.
    ///
    /// Recorded for every run, including an ad-hoc manual one that copied no
    /// schedule — so a console can compare what a run was asked to do against
    /// what a schedule asks for now, without a schedule having been involved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_policy_sha256: Option<String>,
    /// The source connection. `v1`.
    pub source: SourceInputs,
    /// **The exact topic list this run executes**, in the order
    /// `backup.yaml` carries it. `v1`.
    ///
    /// `Backup.spec.topics` verbatim in `SelectedTopics` mode; the byte-sorted
    /// resolved names in `AllUserTopics` mode. Never empty and never a pattern
    /// — see [`ResolvedSelection`].
    pub topics: Vec<String>,
    /// Where [`BackupExecutionInputs::topics`] came from, and what this run may
    /// claim to have covered. `v2`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection: Option<SelectionInputs>,
    /// The resolved `BackupDestination` this run writes to — D2 §3.7, seam
    /// **S4**. `v2`.
    ///
    /// **RESERVED. NOTHING WRITES IT YET.** D2 W10 wires destination-backed
    /// execution; the block is declared here so that when it does, it lands in
    /// PLAT-06.1's one document under a grammar that already loads, verifies
    /// and compares it — not in a second freeze.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination: Option<ResolvedDestinationSnapshot>,
    /// The archive. `v1`.
    pub archive: ArchiveInputs,
    /// The container argv, deadline and tunables. `v1`.
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

    /// This snapshot as grammar [`INPUTS_VERSION_V1`] states it: every `v2`
    /// block dropped.
    ///
    /// # Why a downgraded VIEW rather than a version-blind comparison
    ///
    /// A `Backup` frozen before D1 holds a `v1` plan, and the fresh resolution
    /// made for it now is a `v2` one. Comparing them whole would find five
    /// added blocks and refuse the run as a `PlanConfigMapConflict` — every
    /// in-flight `Backup` in the cluster, at the moment the new controller
    /// starts. Comparing the stored `v1` document against this view of the
    /// fresh resolution asks the only question a `v1` plan can answer: are the
    /// fields it actually froze still the fields this run resolves?
    ///
    /// It is deliberately NOT used the other way round. A `v2` plan is compared
    /// whole, so a changed trigger, revision, policy digest, selection or
    /// destination is a conflict.
    #[must_use]
    pub fn as_version_v1(&self) -> Self {
        let mut view = self.clone();
        view.version = INPUTS_VERSION_V1.to_string();
        view.trigger = None;
        view.schedule_ref = None;
        view.run_policy_sha256 = None;
        view.selection = None;
        view.destination = None;
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
/// UID, whatever [`crate::connection::resolve`] refuses the source connection
/// for (`CredentialNotRenderable` for a SCRAM cluster without a username or a
/// usable Secret reference, `ConnectionConfigInvalid`,
/// `ConnectionReferenceInvalid` or `ConnectionFieldUnsupported` — PLAT-07.1),
/// `CredentialNotRenderable` for an archive Secret reference with a blank name,
/// `ArchiveUrlUnreadable` for an archive URL no storage block can be rendered
/// from, and `ExecutionSpecInvalid` for a non-positive deadline.
pub fn resolve_inputs(
    backup: &Backup,
    identity: ExecutionIdentity,
    cluster: &KafkaCluster,
    addressing_env: &[(String, String)],
    selection: &ResolvedSelection,
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
    // THE ONE RESOLUTION (PLAT-07.1). The source connection's settings are not
    // read off `cluster.spec` here: `connection::resolve` is the single answer
    // to "what does a runner Job need to dial this KafkaCluster", so the plan
    // document, the probe and the Job cannot disagree, and a connection it
    // refuses is refused BEFORE anything is frozen or created.
    //
    // WHAT IS COPIED AND WHAT IS ONLY VALIDATED. The bootstrap addresses, the
    // auth block and the CA reference are copied into the snapshot, because
    // each of them changes what the run dials or trusts and a later pass must
    // compare them. The PASSWORD reference is validated and NOT copied: the Job
    // builder projects it from this same KafkaCluster, whose UID the snapshot
    // pins and whose spec is CEL-immutable, so no Secret name and no credential
    // key reaches a ConfigMap.
    let connection = crate::connection::resolve(cluster, ConnectionUse::BackupSource)
        .map_err(ExecutionRefusal::from)?;
    connection
        .check_job_namespace(&identity.backup.namespace)
        .map_err(ExecutionRefusal::from)?;
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
    // THE LIST THAT REACHES THE ENGINE, RE-CHECKED AT THE FREEZE BOUNDARY.
    // `reconcile_backup`'s step 0b already refuses a glob in `spec.topics` and
    // its step 0b' refuses a selection that is neither D1 §7.1 shape — but
    // W5's dynamic path produces this list from a runner's stdout rather than
    // from the spec, and the last place to refuse an empty or patterned
    // allowlist is the one every producer goes through. An empty list handed
    // to `backup.yaml` is the "no allowlist means everything" shape guard
    // G-GLOB exists for.
    if selection.topics.is_empty() {
        return Err(ExecutionRefusal::new(
            crate::conditions::TERMINAL_STATE_SELECTION_EMPTY,
            format!(
                "the resolved topic selection for {name} is empty; a mandatory allowlist whose \
                 absence means `all topics` is not an allowlist (guard G-GLOB), so no runner Job \
                 is created"
            ),
        ));
    }
    if let Err(entry) = logweir_core::guard::reject_glob_metacharacters(&selection.topics) {
        return Err(ExecutionRefusal::new(
            crate::conditions::TERMINAL_STATE_INVALID_TOPIC_SELECTION,
            format!(
                "the resolved topic selection for {name} names `{}`, which carries a glob \
                 metacharacter; topics are a mandatory NAMED allowlist and a pattern is refused \
                 rather than expanded",
                shown(&entry)
            ),
        ));
    }

    let args = runner_argv(identity.trigger, &identity.id);
    Ok(BackupExecutionInputs {
        version: INPUTS_VERSION.to_string(),
        trigger: Some(run_inputs(backup)),
        schedule_ref: schedule_ref_inputs(backup),
        run_policy_sha256: Some(crate::policy::run_policy_sha256(&backup.spec)),
        selection: Some(selection.selection.clone()),
        destination: None,
        source: SourceInputs {
            cluster: ObjectIdentity {
                namespace: cluster
                    .namespace()
                    .unwrap_or_else(|| identity.backup.namespace.clone()),
                name: cluster_name,
                uid: cluster_uid,
            },
            bootstrap_servers: connection.bootstrap_servers,
            auth: connection.auth,
            tls_ca: connection.tls_ca,
            observed_cluster_id: cluster
                .status
                .as_ref()
                .and_then(|status| status.cluster_id.clone())
                .filter(|id| !id.is_empty()),
        },
        topics: selection.topics.clone(),
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

/// The `trigger` block for this `Backup`, read the way D1 §3.1 reads it.
///
/// PURE, AND THE SAME READ [`crate::identity::run_identity`] MAKES. A `Backup`
/// created before `spec.trigger` existed has none, and rule 4 says what it is:
/// `Scheduled`/0 when `triggeredBy` is `schedule`, `Manual` otherwise. That is
/// exactly what the controller that created it did, so freezing the derived
/// value converts nothing and loses nothing.
fn run_inputs(backup: &Backup) -> RunInputs {
    let (kind, attempt, retry_of) = crate::identity::declared_trigger(backup);
    RunInputs {
        kind,
        attempt,
        retry_of: retry_of.map(|r| r.name.clone()),
        time_zone: backup
            .spec
            .trigger
            .as_ref()
            .and_then(|t| t.time_zone.clone())
            .filter(|zone| !zone.is_empty()),
    }
}

/// The `scheduleRef` block for this `Backup`, or `None` for an ad-hoc run.
///
/// COPIED, NOT RESOLVED. Whatever `spec.scheduleRef` states is what the run
/// executed under; the schedule is never re-read to fill this in, because a
/// schedule edited between the admission and the freeze must not change what a
/// created run records (D1 §5.4).
fn schedule_ref_inputs(backup: &Backup) -> Option<ScheduleRefInputs> {
    let reference = backup.spec.schedule_ref.as_ref()?;
    if reference.name.is_empty() {
        return None;
    }
    Some(ScheduleRefInputs {
        name: reference.name.clone(),
        uid: reference.uid.clone().filter(|uid| !uid.is_empty()),
        generation: reference.generation,
        run_policy_sha256: reference.run_policy_sha256.clone(),
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
    // BOTH GRAMMARS ARE READ, AND ONLY ONE IS WRITTEN (D-SEAMS S4). A `v1`
    // plan is a run a PLAT-06.1 controller admitted and a runner may already
    // have mounted; refusing it on the version string alone would make the
    // upgrade terminate every `Backup` in flight.
    if !version
        .as_deref()
        .is_some_and(|v| INPUTS_VERSIONS_READ.contains(&v))
    {
        return refuse(format!(
            "{INPUTS_KEY} names grammar {:?}, and this controller reads {INPUTS_VERSIONS_READ:?}",
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
    // THE COMPARISON IS MADE AT THE STORED DOCUMENT'S OWN GRAMMAR. A `v1` plan
    // is asked only what a `v1` plan states; a `v2` plan is compared whole, so
    // every block D1 added is a conflict when it differs.
    let stored_is_v1 = frozen.inputs.version == INPUTS_VERSION_V1;
    let expected = if stored_is_v1 {
        desired.inputs.as_version_v1()
    } else {
        desired.inputs.clone()
    };

    if frozen.inputs.execution != expected.execution {
        return refuse(format!(
            "its snapshot binds execution {} of {}/{} (uid {}), and this Backup derives execution \
             {}",
            frozen.inputs.execution.id,
            frozen.inputs.execution.backup.namespace,
            frozen.inputs.execution.backup.name,
            frozen.inputs.execution.backup.uid,
            expected.execution.id
        ));
    }
    // THE `v2` BLOCKS, EACH NAMED SEPARATELY. The whole-snapshot comparison
    // below would catch all of these, with a message that says only "the
    // inputs differ" — and "which of the nine blocks" is the first thing an
    // operator reading a terminal `PlanConfigMapConflict` needs.
    if frozen.inputs.trigger != expected.trigger {
        return refuse(format!(
            "its snapshot froze trigger {:?} and this Backup declares {:?}; a run's kind and \
             attempt decide its archive prefix and are never re-decided after the freeze",
            frozen.inputs.trigger, expected.trigger
        ));
    }
    if frozen.inputs.schedule_ref != expected.schedule_ref {
        return refuse(format!(
            "its snapshot froze the BackupSchedule revision {:?} and this Backup records {:?}; \
             an edited schedule changes the NEXT admission and never a created run",
            frozen.inputs.schedule_ref, expected.schedule_ref
        ));
    }
    if frozen.inputs.run_policy_sha256 != expected.run_policy_sha256 {
        return refuse(format!(
            "its snapshot froze run policy {} and this Backup's fields digest to {}",
            frozen
                .inputs
                .run_policy_sha256
                .as_deref()
                .unwrap_or("<none>"),
            expected.run_policy_sha256.as_deref().unwrap_or("<none>")
        ));
    }
    if frozen.inputs.selection != expected.selection {
        return refuse(
            "its snapshot froze a different topic selection than this Backup resolves now; a \
             frozen run covers the set it was admitted with and a coverage claim is never \
             re-decided"
                .to_string(),
        );
    }
    if frozen.inputs.destination != expected.destination {
        return refuse(
            "its snapshot froze a different resolved BackupDestination than this Backup resolves \
             now"
            .to_string(),
        );
    }
    if frozen.inputs.executable() != expected.executable() {
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
