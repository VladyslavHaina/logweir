//! The `Backup` execution contract.
//!
//! # What executes, and where it comes from
//!
//! A runner Job executes exactly three things, each derived here from typed,
//! server-side facts:
//!
//! 1. **The run identity** ([`execution_identity`]). A scheduled `Backup` runs
//!    as `<schedule uid>-<slot>` ([`crate::slot::backup_id_for`], guard
//!    **G-SLOT**); a manual one runs as its own API-server UID, so deleting and
//!    re-creating it is a new run.
//! 2. **The resolved inputs** ([`BackupExecutionInputs`]): the identity, the
//!    source connection resolved from the `KafkaCluster` (UID, addresses, SCRAM
//!    username, TLS flag), the topic allowlist, the archive location with its
//!    storage block and addressing environment, and the runner argv and
//!    deadline. No Secret value or name is part of it: the credential references
//!    follow from the pinned `KafkaCluster` UID and the CEL-immutable specs, so
//!    the plan ConfigMap, which has no encryption at rest, names no credential.
//! 3. **The runner argv** ([`runner_argv`]), a pure function of the trigger and
//!    the identity.
//!
//! The `logweir.dev/runner-argv` annotation ([`RUNNER_ARGV_ANNOTATION`]) older
//! controllers executed is only observed ([`runner_argv_annotation`]), reported
//! by size and digest, never executed or echoed: whoever could annotate a
//! `Backup` could otherwise change the subcommand, the paths or the backup id.
//!
//! # Frozen before any Job exists
//!
//! [`FrozenInputs`] is the canonical encoding of the inputs
//! (`logweir_core::det_json`) plus its `sha256:` digest. The controller writes
//! it into the create-only, `immutable: true` plan ConfigMap
//! ([`inputs_config_map`]) with the two documents the runner reads, both
//! rendered from the snapshot, and records `status.execution` before it creates
//! the Job. Every later pass admits the existing object only through
//! [`verify_frozen_config_map`]: exact single owner, immutable, canonical,
//! digest-annotated, documents re-rendered byte for byte, bound to this
//! `Backup`, equal to the recorded status and to a fresh resolution in
//! everything executable. Anything else is a terminal conflict; nothing is
//! overwritten or adopted.
//!
//! This module performs no I/O. It reads the process environment only through
//! [`crate::retention::storage_url_for`], the one archive URL parser.

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
use crate::crds::kafka_cluster::KafkaCluster;
use crate::crds::selection::{Coverage, IncompleteDiscovery, SelectionMode, SelectionStatus};
use crate::crds::LocalRef;
use crate::destination::{ResolvedDestinationSnapshot, ARCHIVE_CA_PLAN_KEY};
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
/// Not in the runner argv: `logweir backup run` writes one document,
/// `--receipt-out` takes precedence over `--out`, and passing both at different
/// paths is refused with exit 1 before the engine starts
/// (`refuse_two_receipt_paths`). The constant names the path a runner's scratch
/// volume must make writable (`job::WORK_VOLUME`).
pub const OUT_PATH: &str = "/work/backup.json";
/// Where the runner writes the signed receipt.
pub const RECEIPT_OUT_PATH: &str = "/work/receipt.json";

/// The annotation older controllers executed as the runner argv. Observed and
/// never executed: a `Backup` that carries it gets a
/// `RunnerArgvAnnotationIgnored` condition naming its size and digest. Public
/// so tests, docs and upgrade tooling can identify legacy objects.
pub const RUNNER_ARGV_ANNOTATION: &str = "logweir.dev/runner-argv";

/// The plan ConfigMap key carrying the canonical typed input snapshot.
pub const INPUTS_KEY: &str = "execution-inputs.json";

/// The first snapshot grammar: identity, source, topics, archive, runner.
///
/// Still read and executed, never rewritten: a `Backup` frozen before D1
/// carries it, and a run in flight at upgrade must keep executing the bytes it
/// was admitted with. One of the two grammars [`verify_frozen_config_map`]
/// admits ([`INPUTS_VERSIONS_READ`]).
pub const INPUTS_VERSION_V1: &str = "logweir.dev/backup-execution-inputs/v1";

/// D1 §3.3's grammar: [`INPUTS_VERSION_V1`] plus the trigger, the schedule
/// revision, the run policy digest, the frozen selection and the destination
/// block. All additions are optional, so a `v1` document deserialises into the
/// same type and re-encodes to its stored bytes, which keeps a frozen `v1` plan
/// verifiable after the upgrade (seam **S4**).
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
    /// A connection the shared resolver refuses is a refusal of the run, with
    /// the resolver's own terminal state and message: `ConnectionConfigInvalid`
    /// says the saved connection is wrong, not the snapshot.
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
    /// The `BackupSchedule`'s UID.
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

/// A `triggeredBy` value, bounded for a condition message (spec text, not a
/// credential, but unbounded). `pub(crate)` because a relayed topic name in
/// `controllers::backup_selection`'s refusal messages takes the same bound.
pub(crate) fn shown(value: &str) -> String {
    const LIMIT: usize = 64;
    if value.chars().count() <= LIMIT {
        value.to_string()
    } else {
        format!("{}…", value.chars().take(LIMIT).collect::<String>())
    }
}

/// Derive the run identity from the typed spec and server-generated metadata.
///
/// The derivation is [`crate::identity::run_identity`] (D1 §3.1), which reads
/// the object alone and knows all four trigger kinds; this projects it into the
/// `execution` block of the frozen grammar. A scheduled `Backup` created before
/// `spec.trigger` existed (`triggeredBy: schedule`, a `scheduleRef` without a
/// `uid`, the controller owner reference) still reads as `Scheduled`/attempt 0
/// with the UID from that owner reference, so its execution id is unchanged and
/// nothing is converted on upgrade.
///
/// The one check added here belongs to the execution contract:
/// `spec.triggeredBy` becomes the signed receipt's `triggered_by`, so an unknown
/// value is [`TERMINAL_STATE_EXECUTION_SPEC_INVALID`], and it must agree with
/// `spec.trigger.kind` or a `Manual` run would sign a receipt saying
/// `schedule`.
///
/// # Errors
///
/// [`TERMINAL_STATE_EXECUTION_SPEC_INVALID`] for a missing namespace or UID and
/// for an unknown `spec.triggeredBy`;
/// [`crate::conditions::TERMINAL_STATE_SCHEDULED_IDENTITY_MISMATCH`] for a
/// `triggeredBy` that contradicts `spec.trigger.kind` and for every identity
/// [`crate::identity::run_identity`] refuses (a name its trigger does not
/// compose, a slot that is not a date, a legacy ownerReference naming another
/// schedule, an illegal trigger shape);
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

    // (1) The receipt's own vocabulary.
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

    let schedule = run.schedule.map(|schedule| ScheduleTrigger {
        name: schedule.name,
        uid: schedule.uid,
        slot: run.slot.clone().unwrap_or_default(),
    });

    Ok(ExecutionIdentity {
        id: run.execution_id,
        trigger,
        backup: object,
        schedule,
    })
}

/// The backup id older controllers reported: the first controller owner's UID
/// plus `spec.slot`, else this object's UID. Used only to report a Job an older
/// controller created; for every `Backup` [`execution_identity`] accepts, the
/// two agree.
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
/// Interface **I10**'s backup id override flag is always passed, even when the
/// plan already names the same id, so the Job spec states the executed
/// identity. `tests/schedule_controller.rs::the_backup_id_override_is_passed_not_defined`
/// asserts that the flag token appears on exactly one code line in this crate,
/// in this function. The leading `"backup"` names the `logweir` subcommand;
/// `--receipt-out` and never `--out` (see [`OUT_PATH`]).
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

/// [`runner_argv`] with D2 §3.5's version-skew handshake appended, for a
/// destination-backed run.
///
/// An unknown flag is a clap parse error, so an old runner image handed a
/// destination-backed Job exits before it could build its store from whatever
/// `AWS_*` was in the pod (seam S5). A legacy inline-`archive` run does not
/// carry it: the flag promises that every store setting arrives explicitly, and
/// on that path it does not.
#[must_use]
pub fn runner_argv_with_store_contract(
    trigger: ExecutionTrigger,
    execution_id: &str,
) -> Vec<String> {
    let mut argv = runner_argv(trigger, execution_id);
    argv.push(crate::destination::STORE_CONTRACT_VERSION_ARG.to_string());
    argv.push(crate::destination::STORE_CONTRACT_VERSION.to_string());
    argv
}

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
    /// The private CA the connection verifies the broker with, as the reference
    /// [`crate::connection::resolve`] decided (Secret or ConfigMap, object name
    /// and data key).
    ///
    /// Executable, so a `tlsCa` edited after the freeze is a
    /// [`TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT`], never a Job that verifies the
    /// broker against another root. A CA certificate is public, which is why its
    /// reference may be named here while the SASL password's may not. Absent
    /// when no CA is named, so older snapshots re-encode to the same bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_ca: Option<crate::connection::CaReference>,
    /// `KafkaCluster.status.clusterId` when the inputs were resolved.
    /// Informational: the runner observes the id from the broker, so it is
    /// excluded from the executable comparison and a later probe never
    /// invalidates frozen inputs.
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

/// `false` as a serde skip predicate, for a `bool` that is absent when unset.
fn is_false(value: &bool) -> bool {
    !*value
}

/// The `trigger` block: which kind of run this is, and where in its slot's
/// attempt chain it sits (D1 §3.1).
///
/// Beside `execution`, not inside it: [`ExecutionIdentity`]'s bytes are
/// compared against a fresh derivation on every pass and every `v1` snapshot
/// carries them, so adding fields there would change every stored snapshot.
/// `execution.id` already encodes the attempt (`-r<k>`) and stays the one
/// identity the runner is handed.
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
/// (D1 §5.3). Deleting a schedule keeps its history, and the frozen inputs are
/// the only immutable place a run has to record the revision it executed.
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
    /// The run policy digest copied from the schedule at that generation. D1
    /// §3.1 rule 5 requires it to equal
    /// [`BackupExecutionInputs::run_policy_sha256`] (this run's own fields), and
    /// [`crate::identity::check_run_policy_digest`] refuses the run otherwise.
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

/// What the per-run discovery observed (D1 §3.3's `selection.discovery`),
/// frozen inside the one execution document (seam **S4**).
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

/// The `selection` block: how this run chose its topics, and what it may
/// honestly claim to have covered (D1 §3.3, §7.4).
///
/// The names are not repeated here: [`BackupExecutionInputs::topics`] is the
/// exact frozen list, and a second copy would be a third copy of up to 256 KiB
/// of names in a one-MiB `ConfigMap`, with two places for one answer to drift.
/// The count and byte size are recorded instead; this block is the provenance
/// of `topics`, never a second statement of it.
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
    /// What discovery observed, in dynamic mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery: Option<DiscoveryInputs>,
}

impl SelectionInputs {
    /// The `status.selection` projection of this block: one projection, so the
    /// status and the frozen document cannot disagree.
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
/// [`resolve_inputs`] takes one of these and computes no selection of its own:
/// [`ResolvedSelection::named`] (the named allowlist) and the dynamic discovery
/// result both freeze through the same code, encoding and
/// [`verify_frozen_config_map`] comparison.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedSelection {
    /// The exact names handed to `backup.yaml`, in the order they are handed.
    pub topics: Vec<String>,
    /// Where those names came from.
    pub selection: SelectionInputs,
}

/// The resolved `BackupDestination` a frozen plan carries, and the CA bytes
/// frozen beside it, so a destination edited after the freeze changes nothing
/// for the running run.
///
/// Every edit bumps `metadata.generation`, which the frozen block records, so a
/// pass that re-created a garbage-collected Job from the live object would be
/// refused as a `PlanConfigMapConflict` on an archive that may be half written.
/// `None` for a legacy plan, an unparseable one, or a `Backup` whose plan
/// `ConfigMap` is gone; the caller then re-resolves and re-freezes.
pub fn stored_destination(
    existing: &ConfigMap,
) -> Option<(ResolvedDestinationSnapshot, Option<String>)> {
    let data = existing.data.as_ref()?;
    let stored = data.get(INPUTS_KEY)?;
    let inputs: BackupExecutionInputs = serde_json::from_str(stored).ok()?;
    let destination = inputs.destination?;
    Some((destination, data.get(ARCHIVE_CA_PLAN_KEY).cloned()))
}

/// The topic selection a frozen plan carries.
///
/// A run's topic set is decided once, at the freeze. Resolving it again on a
/// pass that only re-creates a collected Job would see topics created or
/// deleted since, and [`verify_frozen_config_map`] would refuse a run whose
/// archive may be half written (D1 §12,
/// `a_frozen_dynamic_backup_never_reruns_discovery`). The comparison is not
/// weakened: [`verify_frozen_config_map`] still requires the stored bytes to
/// digest to `status.execution.inputsSha256`, which this controller wrote
/// before any Job existed. `None` for a `ConfigMap` with no snapshot, an
/// unparseable one, or a `v1` snapshot (a named allowlist).
pub fn stored_selection(existing: &ConfigMap) -> Option<ResolvedSelection> {
    let stored = existing.data.as_ref()?.get(INPUTS_KEY)?;
    let inputs: BackupExecutionInputs = serde_json::from_str(stored).ok()?;
    let selection = inputs.selection?;
    Some(ResolvedSelection {
        topics: inputs.topics,
        selection,
    })
}

impl ResolvedSelection {
    /// The named allowlist: `spec.topics` verbatim, coverage
    /// [`Coverage::NamedTopics`]. Not sorted: the user's order reaches the
    /// engine (only [`crate::policy::run_policy_sha256`] canonicalises), and
    /// sorting would change the frozen bytes of every unsorted list.
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

/// The canonical typed snapshot of everything one run executes: the
/// `execution-inputs.json` document.
///
/// Versioned and closed: every struct denies unknown fields and a stored
/// snapshot must re-encode to the same bytes, so a later grammar is a conflict,
/// never a partial read. `v1` and `v2` are one type (seam **S4**): `v2` adds
/// optional blocks, each `skip_serializing_if = "Option::is_none"`, and
/// `logweir_core::det_json` emits fields in declaration order, so a `v1`
/// document re-encodes to the bytes it was stored as and keeps running across
/// the upgrade ([`verify_frozen_config_map`] rests on this).
///
/// **The order of these fields is the wire format.** Reordering them re-encodes
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
    /// The resolved `BackupDestination` this run writes to (D2 §3.7, seam
    /// **S4**). `v2`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination: Option<ResolvedDestinationSnapshot>,
    /// The consumer groups this run records positions for, copied from
    /// `Backup.spec.consumerGroups` in the spec's order. `v2`, optional and last
    /// among the optional blocks: absent when the run selects none, so every
    /// older snapshot, and every one that selects no group, re-encodes to its
    /// stored bytes. A controller older than this field refuses a snapshot that
    /// carries it (`deny_unknown_fields`: `PlanConfigMapConflict`), the rollback
    /// cost, only for runs that select groups.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_groups: Option<Vec<String>>,
    /// **PROD-05.1a.** The Strimzi `KafkaTopic` resources the controller
    /// listed for this run's topics, when the `KafkaCluster` names its
    /// Strimzi cluster (`spec.strimzi`). Rendered into
    /// [`PLAN_KAFKA_TOPIC_RESOURCES_KEY`] and passed to the runner as
    /// `--kafka-topic-resources`, so the receipt records
    /// `owner_detection: [kafkaTopicResources]`. `v2`, optional, after
    /// `consumerGroups`: absent for every other run, which keeps its bytes. A
    /// controller older than this field refuses a snapshot that carries it
    /// (`PlanConfigMapConflict`), the rollback cost, only for such runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kafka_topic_resources: Option<KafkaTopicResourcesInputs>,
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
    /// A `Backup` frozen before D1 holds a `v1` plan while its fresh resolution
    /// is `v2`; comparing them whole would refuse every in-flight `Backup` the
    /// moment the new controller starts. Comparing the stored `v1` document with
    /// this view asks whether the fields it froze still resolve the same. Never
    /// used the other way: a `v2` plan is compared whole.
    #[must_use]
    pub fn as_version_v1(&self) -> Self {
        let mut view = self.clone();
        view.version = INPUTS_VERSION_V1.to_string();
        view.trigger = None;
        view.schedule_ref = None;
        view.run_policy_sha256 = None;
        view.selection = None;
        view.destination = None;
        view.consumer_groups = None;
        view.kafka_topic_resources = None;
        view
    }
}

/// The plan `ConfigMap` key the listed `KafkaTopic` resources are rendered
/// into (PROD-05.1a).
pub const PLAN_KAFKA_TOPIC_RESOURCES_KEY: &str = "kafka-topic-resources.yaml";

/// Where the runner reads them.
pub const KAFKA_TOPIC_RESOURCES_PATH: &str = "/plan/kafka-topic-resources.yaml";

/// **PROD-05.1a.** What the controller listed: the Strimzi cluster, its
/// namespace, and every `KafkaTopic` there labelled for that cluster, not
/// unmanaged, that names one of the run's topics. Possibly empty: the list
/// succeeded and found none, which is a look ("no owner"), never "not
/// checked".
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KafkaTopicResourcesInputs {
    /// The `strimzi.io/cluster` label value.
    pub cluster: String,
    /// The namespace listed.
    pub namespace: String,
    /// The resources, by name.
    pub resources: Vec<KafkaTopicResourceInputs>,
}

/// One listed `KafkaTopic`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KafkaTopicResourceInputs {
    /// `metadata.name`.
    pub name: String,
    /// `spec.topicName`, when it sets one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_name: Option<String>,
}

impl KafkaTopicResourcesInputs {
    /// The resources as the runner's `--kafka-topic-resources` reads them
    /// ([`crate::strimzi_topics::document`]).
    ///
    /// # Errors
    ///
    /// A conflict for a document that does not serialise.
    pub fn document(&self) -> Result<String, ExecutionRefusal> {
        crate::strimzi_topics::document(self).map_err(|e| {
            ExecutionRefusal::conflict(format!(
                "the listed KafkaTopic resources do not serialise: {e}"
            ))
        })
    }
}

/// The inputs with the listed `KafkaTopic` resources frozen in, and the
/// runner told where they are (`--kafka-topic-resources`,
/// `--strimzi-cluster`). `None` leaves the inputs as they are, byte for byte.
#[must_use]
pub fn with_kafka_topic_resources(
    mut inputs: BackupExecutionInputs,
    resources: Option<KafkaTopicResourcesInputs>,
) -> BackupExecutionInputs {
    if let Some(block) = resources {
        inputs.runner.args.extend([
            "--kafka-topic-resources".to_string(),
            KAFKA_TOPIC_RESOURCES_PATH.to_string(),
            "--strimzi-cluster".to_string(),
            block.cluster.clone(),
        ]);
        inputs.kafka_topic_resources = Some(block);
    }
    inputs
}

/// The `KafkaTopic` resources a frozen plan carries: `Some(block)` as it was
/// frozen (`Some(None)` for a frozen run without one), `None` when the plan
/// holds no readable snapshot. A frozen run never lists again.
#[must_use]
pub fn stored_kafka_topic_resources(plan: &ConfigMap) -> Option<Option<KafkaTopicResourcesInputs>> {
    let stored = plan.data.as_ref()?.get(INPUTS_KEY)?;
    let inputs: BackupExecutionInputs = serde_json::from_str(stored).ok()?;
    Some(inputs.kafka_topic_resources)
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
/// `ConnectionReferenceInvalid` or `ConnectionFieldUnsupported`),
/// `CredentialNotRenderable` for an archive Secret reference with a blank name,
/// `ArchiveUrlUnreadable` for an archive URL no storage block can be rendered
/// from, and `ExecutionSpecInvalid` for a non-positive deadline.
pub fn resolve_inputs(
    backup: &Backup,
    identity: ExecutionIdentity,
    cluster: &KafkaCluster,
    addressing_env: &[(String, String)],
    selection: &ResolvedSelection,
    destination: Option<&ResolvedDestinationSnapshot>,
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
    // The one resolution: `connection::resolve` answers "what does a runner Job
    // need to dial this KafkaCluster" for the plan, the probe and the Job, and
    // refuses before anything is frozen. The bootstrap addresses, the auth block
    // and the CA reference are copied, because each changes what the run dials
    // or trusts. The password reference is only validated: the Job builder
    // projects it from this same pinned, CEL-immutable KafkaCluster, so no
    // Secret name reaches a ConfigMap.
    let connection = crate::connection::resolve(cluster, ConnectionUse::BackupSource)
        .map_err(ExecutionRefusal::from)?;
    connection
        .check_job_namespace(&identity.backup.namespace)
        .map_err(ExecutionRefusal::from)?;
    // The archive comes from the resolved destination or from the URL, never
    // both. A destination-backed `Backup`'s `spec.archive.url` is the sentinel
    // `logweir-destination://<name>` (`DESTINATION_SENTINEL_RULE`), which
    // `storage_url_for` refuses as an unknown scheme, so an older controller
    // writes `ArchiveUrlUnreadable` before it POSTs anything (D2 §3.6's rollback
    // behaviour).
    let storage = match destination {
        // The snapshot, not a live resolution, so a pass that re-creates a
        // collected Job renders the storage block the run was admitted with.
        Some(frozen) => frozen.archive_storage.clone(),
        None => crate::retention::storage_url_for(&backup.spec.archive.url).map_err(|e| {
            ExecutionRefusal::new(
                TERMINAL_STATE_ARCHIVE_URL_UNREADABLE,
                format!("the archive URL on {name} is not an object-store location: {e}"),
            )
        })?,
    };
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
    // The list that reaches the engine, re-checked at the freeze boundary every
    // producer goes through: the dynamic path builds it from a runner's stdout,
    // not the spec, and an empty list in `backup.yaml` is the "no allowlist
    // means everything" shape guard G-GLOB exists for.
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
    // D1 §3.3: "Kafka-legal names `^[a-zA-Z0-9._-]{249}$` are re-validated
    // before freeze". Not the glob rail again: a space, a slash, a control
    // character or a 250-character name has no glob metacharacter. A
    // runner-relayed name the broker cannot hold would otherwise be frozen into
    // an immutable plan and fail opaquely inside the engine.
    if let Err(entry) = logweir_core::guard::reject_non_kafka_topic_names(&selection.topics) {
        return Err(ExecutionRefusal::new(
            crate::conditions::TERMINAL_STATE_INVALID_TOPIC_SELECTION,
            format!(
                "the resolved topic selection for {name} names `{}`, which is not a name a Kafka \
                 broker accepts (`^[a-zA-Z0-9._-]{{1,249}}$`); it is refused rather than frozen \
                 into a plan the engine would fail on",
                shown(&logweir_core::check_contract::redact(&entry))
            ),
        ));
    }

    // The maximum one backup may name, at the boundary every producer goes
    // through. A larger backup would sign a receipt the recovery catalog could
    // not list and this controller could not verify, so it is refused by name
    // before anything is frozen, and nothing is dropped to make it fit. The
    // runner refuses the same count at phase −1
    // (`logweir_core::topic_budget::refuse_topic_count`).
    if let Some(why) = logweir_core::topic_budget::refuse_topic_count(selection.topics.len()) {
        return Err(ExecutionRefusal::new(
            crate::conditions::TERMINAL_STATE_SELECTION_TOO_LARGE,
            format!(
                "the topic selection for {name} is refused: {why}. No runner Job is created \
                 and no topic is left out to make the selection fit"
            ),
        ));
    }

    // A consumer group selection the runner would refuse at phase −1 (exit 3)
    // is refused here, before anything is frozen or created.
    if let Some(why) = crate::policy::selected_groups(&backup.spec)
        .and_then(logweir_core::consumer_positions::refuse_selection)
    {
        return Err(ExecutionRefusal::spec(format!(
            "spec.consumerGroups on {name}: {why}; every selected group gets exactly one \
             outcome in the receipt, so a selection the runner cannot record is refused \
             before any Job exists"
        )));
    }

    let args = match destination {
        Some(_) => runner_argv_with_store_contract(identity.trigger, &identity.id),
        None => runner_argv(identity.trigger, &identity.id),
    };
    Ok(BackupExecutionInputs {
        version: INPUTS_VERSION.to_string(),
        trigger: Some(run_inputs(backup)),
        schedule_ref: schedule_ref_inputs(backup),
        run_policy_sha256: Some(crate::policy::run_policy_sha256(&backup.spec)),
        selection: Some(selection.selection.clone()),
        destination: destination.cloned(),
        consumer_groups: crate::policy::selected_groups(&backup.spec).map(<[String]>::to_vec),
        // Added by `with_kafka_topic_resources`, after the listing.
        kafka_topic_resources: None,
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
            // Empty for a destination-backed run: the forwarded set is the
            // controller's own `AWS_*` addressing, which the engine's
            // `from_env()` would honour over the approved plan, and a
            // destination renders its complete set from the object alone
            // (`ResolvedDestination::job_env`).
            addressing_env: match destination {
                Some(_) => Vec::new(),
                None => addressing_env
                    .iter()
                    .map(|(name, value)| EnvValue {
                        name: name.clone(),
                        value: value.clone(),
                    })
                    .collect(),
            },
        },
        runner: RunnerInputs {
            args,
            deadline_seconds: backup.spec.deadline_seconds,
            settings: RunnerSettings::from(&BackupSettings::default()),
        },
        execution: identity,
    })
}

/// The `trigger` block for this `Backup`, read as
/// [`crate::identity::run_identity`] reads it (D1 §3.1). A `Backup` created
/// before `spec.trigger` existed is `Scheduled`/0 when `triggeredBy` is
/// `schedule` and `Manual` otherwise, which is what its creator did, so freezing
/// the derived value converts nothing.
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
/// Copied, not resolved: a schedule edited between admission and the freeze
/// must not change what a created run records (D1 §5.4).
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
    /// The destination's CA bundle, copied into the same immutable plan
    /// `ConfigMap` as [`ARCHIVE_CA_PLAN_KEY`] (D2 §3.7).
    ///
    /// The bytes sit beside the snapshot and the snapshot carries `caSha256`,
    /// because a 64 KiB bundle inside it would go through `det_json` on every
    /// reconcile; the digest binds them, so a CA swapped into an existing run
    /// makes the plan disagree with its own snapshot
    /// ([`verify_frozen_config_map`]). `None` for every legacy run and every
    /// destination with no `spec.transport.caBundle`.
    pub ca_pem: Option<String>,
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
        Self::freeze_with_ca(inputs, None)
    }

    /// [`FrozenInputs::freeze`], with the destination CA bundle that travels
    /// beside the snapshot in the same immutable plan `ConfigMap`.
    ///
    /// # Errors
    ///
    /// See [`canonical_inputs`], plus
    /// [`TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT`] when the bytes are not the
    /// ones the snapshot's `caSha256` names, or one is present without the
    /// other: a CA the plan cannot prove is a CA the run does not get.
    pub fn freeze_with_ca(
        inputs: BackupExecutionInputs,
        ca_pem: Option<String>,
    ) -> Result<Self, ExecutionRefusal> {
        let declared = inputs
            .destination
            .as_ref()
            .and_then(|d| d.ca_sha256.as_deref());
        match (declared, ca_pem.as_deref()) {
            (None, None) => {}
            (Some(digest), Some(bytes)) => {
                let computed = sha256_prefixed(bytes.as_bytes());
                if computed != digest {
                    return Err(ExecutionRefusal::conflict(format!(
                        "the frozen destination names CA digest {digest} and the bundle beside it \
                         digests to {computed}; the bytes a run trusts are the bytes its snapshot \
                         commits to"
                    )));
                }
            }
            (Some(digest), None) => {
                return Err(ExecutionRefusal::conflict(format!(
                    "the frozen destination names CA digest {digest} and no {ARCHIVE_CA_PLAN_KEY} \
                     travels with it; a Job would mount no CA while the snapshot says it has one"
                )));
            }
            (None, Some(_)) => {
                return Err(ExecutionRefusal::conflict(format!(
                    "a {ARCHIVE_CA_PLAN_KEY} was handed to the freeze and the snapshot names no \
                     destination CA; nothing in the frozen document would commit to those bytes"
                )));
            }
        }
        let canonical = canonical_inputs(&inputs)?;
        let sha256 = sha256_prefixed(canonical.as_bytes());
        Ok(Self {
            inputs,
            canonical,
            sha256,
            ca_pem,
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
                topic_owners: None,
                // From the frozen snapshot, so a re-created Job records the
                // groups the run was admitted with.
                consumer_groups: inputs.consumer_groups.clone(),
                schema_registry: None,
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
        let mut documents: BTreeMap<String, String> = [
            (PLAN_SPEC_KEY.to_string(), spec),
            (PLAN_ALLOWED_CLUSTERS_KEY.to_string(), allowed),
            (INPUTS_KEY.to_string(), self.canonical.clone()),
        ]
        .into_iter()
        .collect();
        // The fourth key, only for a destination that declares a CA;
        // `freeze_with_ca` has already bound these bytes to `caSha256`.
        if let Some(ca) = self.ca_pem.as_ref() {
            documents.insert(ARCHIVE_CA_PLAN_KEY.to_string(), ca.clone());
        }
        // PROD-05.1a: the listed `KafkaTopic`s, rendered from the snapshot.
        if let Some(resources) = self.inputs.kafka_topic_resources.as_ref() {
            documents.insert(
                PLAN_KAFKA_TOPIC_RESOURCES_KEY.to_string(),
                resources.document()?,
            );
        }
        Ok(documents)
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
/// A second owner is refused even beside a complete first one: it can keep the
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
    // The fourth key is admitted here and proved below: a destination with a CA
    // bundle freezes its bytes beside the snapshot (D2 §3.7). `freeze_with_ca`
    // refuses bytes whose digest the snapshot does not name, and `documents()`
    // is compared against `data` whole.
    if data.contains_key(ARCHIVE_CA_PLAN_KEY) {
        expected_keys.push(ARCHIVE_CA_PLAN_KEY);
    }
    // PROD-05.1a's key, proved the same way: `documents()` renders it from
    // the snapshot and is compared against `data` whole.
    if data.contains_key(PLAN_KAFKA_TOPIC_RESOURCES_KEY) {
        expected_keys.push(PLAN_KAFKA_TOPIC_RESOURCES_KEY);
    }
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
    // Both grammars are read, one is written (seam S4): a `v1` plan may already
    // be mounted by a runner, and refusing it on the version alone would
    // terminate every `Backup` in flight at upgrade.
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
    // Refrozen with the bytes the object carries, so the digest bind is
    // evaluated against what a kubelet would mount.
    let frozen = FrozenInputs::freeze_with_ca(parsed, data.get(ARCHIVE_CA_PLAN_KEY).cloned())?;
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
    // The `v2` blocks, each named: the whole-snapshot comparison below would
    // catch them too, but only as "the inputs differ".
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
