//! `Preflight` — one bounded readiness observation for an operation that has
//! not run yet.
//!
//! # A check Job, not a controller that reads Secrets (ADR 0008 Amendment F)
//!
//! The controller holds no verb on `secrets` and that invariant is a test
//! (G8). More importantly, only the real execution pod shape proves the real
//! prerequisites: an image pull on a schedulable node, a ServiceAccount that
//! exists, a Secret and key the kubelet can project, CA trust, egress, SASL, an
//! S3 signature and a loadable signer. A controller reading a Secret would
//! prove that bytes exist and nothing else.
//!
//! The cost is accepted and named: one pod per preflight, namespace quota, and
//! a missing Secret blocking the whole pod — in which case dependent checks are
//! reported `BlockedByPrerequisite` with the exact cause rather than silently
//! passing.
//!
//! # `Completed/notReady` is not `Failed`
//!
//! `phase: Failed` means the check could not produce a result at all
//! (`ResultUnreadable`, `RunnerContractUnsupported`, `Stalled`). A finished
//! check that found the operation not ready is `phase: Completed` with
//! `result.state: notReady` — a real answer, and the one an operator acts on.
//!
//! # Advisory, always
//!
//! A `ready` verdict authorizes nothing. Every execution-time guard still runs
//! (D2 §6.8); this kind exists so an operator learns about a missing grant
//! before a restore burns an hour, not so a reconciler can skip a rail.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{ArchiveRef, Condition, LocalRef, Time};

/// `sha256:<64 lowercase hex>` — the one digest spelling this corpus uses.
pub const SHA256_PATTERN: &str = "^sha256:[0-9a-f]{64}$";

/// The Kafka topic-name grammar, as the broker enforces it.
pub const TOPIC_NAME_PATTERN: &str = super::topic_discovery::TOPIC_NAME_PATTERN;

/// P1 — the request is immutable; a new check is a new object.
pub const P1_REQUEST_IMMUTABLE_RULE: &str = super::SPEC_IMMUTABLE_RULE;
/// P1's message.
pub const P1_REQUEST_IMMUTABLE_MESSAGE: &str = "spec.request is immutable; create a new Preflight";

/// P2 — `cancelRequested` moves only from `false` to `true`.
pub const P2_CANCEL_MONOTONIC_RULE: &str = super::topic_discovery::CANCEL_MONOTONIC_RULE;
/// P2's message.
pub const P2_CANCEL_MONOTONIC_MESSAGE: &str = super::topic_discovery::CANCEL_MONOTONIC_MESSAGE;

/// P3 — exactly the block matching `operation` is set.
pub const P3_OPERATION_BLOCK_RULE: &str = "self.operation == 'Backup' ? (has(self.backup) && !has(self.restore) && !has(self.destinationAccess) && !has(self.sourceConnection)) : self.operation == 'Restore' ? (has(self.restore) && !has(self.backup) && !has(self.destinationAccess) && !has(self.sourceConnection)) : self.operation == 'DestinationAccess' ? (has(self.destinationAccess) && !has(self.backup) && !has(self.restore) && !has(self.sourceConnection)) : (has(self.sourceConnection) && !has(self.backup) && !has(self.restore) && !has(self.destinationAccess))";
/// P3's message.
pub const P3_OPERATION_BLOCK_MESSAGE: &str =
    "exactly the block matching spec.request.operation may be set";

/// P4 — a backup check names a destination or a legacy archive, never both.
pub const P4_BACKUP_TARGET_RULE: &str = "has(self.destinationRef) != has(self.legacyArchive)";
/// P4's message.
pub const P4_BACKUP_TARGET_MESSAGE: &str = "set exactly one of destinationRef or legacyArchive";

/// P5 — a restore check names a draft plan or an existing `Restore`.
pub const P5_RESTORE_SUBJECT_RULE: &str = "has(self.planBytes) != has(self.restoreRef)";
/// P5's message.
pub const P5_RESTORE_SUBJECT_MESSAGE: &str =
    "set exactly one of planBytes (a draft) or restoreRef (an existing Restore)";

/// P6 — a draft plan carries its own hash and target.
pub const P6_DRAFT_FIELDS_RULE: &str =
    "!has(self.planBytes) || (has(self.planHash) && has(self.targetRef))";
/// P6's message.
pub const P6_DRAFT_FIELDS_MESSAGE: &str =
    "a draft needs planHash and targetRef; the controller recomputes the hash";

/// P7 — the two restore destinations are set together.
pub const P7_RESTORE_DESTINATIONS_RULE: &str =
    "has(self.sourceDestinationRef) == has(self.evidenceDestinationRef)";
/// P7's message.
pub const P7_RESTORE_DESTINATIONS_MESSAGE: &str =
    "source and evidence destinations are set together";

/// P8 — destinations or a legacy archive, never both.
pub const P8_RESTORE_SOURCE_RULE: &str =
    "has(self.sourceDestinationRef) != has(self.legacySourceArchive)";
/// P8's message.
pub const P8_RESTORE_SOURCE_MESSAGE: &str =
    "set exactly one of the destination refs or legacySourceArchive";

/// P9 — `planHash` is the one digest spelling.
pub const P9_PLAN_HASH_RULE: &str =
    "!has(self.planHash) || self.planHash.matches('^sha256:[0-9a-f]{64}$')";
/// P9's message.
pub const P9_PLAN_HASH_MESSAGE: &str =
    "planHash is sha256:<64 lowercase hex>, the form logweir_core::ids::sha256_prefixed produces";

/// P10 — a restore check is about ONE recovery point: a `Backup` or a catalog
/// point (PLAT-15.2), never both. Two answers to "which point" in one check
/// would leave `recoveryPoint.state` choosing between them.
pub const P10_ONE_RECOVERY_POINT_RULE: &str =
    "!(has(self.recoveryPointRef) && has(self.catalogPointRef))";
/// P10's message.
pub const P10_ONE_RECOVERY_POINT_MESSAGE: &str =
    "set at most one of recoveryPointRef (a Backup) or catalogPointRef (a catalog point)";

/// A catalog point's id: `lwp1-` plus 32 lowercase hex (D3 §5.1).
pub const POINT_ID_PATTERN: &str = "^lwp1-[0-9a-f]{32}$";

/// The rules on `.spec` itself.
pub const SPEC_RULES: [super::SpecRule; 1] = [super::SpecRule::new(
    P2_CANCEL_MONOTONIC_RULE,
    P2_CANCEL_MONOTONIC_MESSAGE,
)];

/// The transition rule attached to `.spec.request`.
pub const REQUEST_RULE: (&[&str], &str, &str) = (
    &["request"],
    P1_REQUEST_IMMUTABLE_RULE,
    P1_REQUEST_IMMUTABLE_MESSAGE,
);

/// The validation rules attached below `.spec`, each at the node whose fields
/// it reads. None of them names `oldSelf`.
pub const NESTED_RULES: [(&[&str], &str, &str); 8] = [
    (
        &["request"],
        P3_OPERATION_BLOCK_RULE,
        P3_OPERATION_BLOCK_MESSAGE,
    ),
    (
        &["request", "backup"],
        P4_BACKUP_TARGET_RULE,
        P4_BACKUP_TARGET_MESSAGE,
    ),
    (
        &["request", "restore"],
        P5_RESTORE_SUBJECT_RULE,
        P5_RESTORE_SUBJECT_MESSAGE,
    ),
    (
        &["request", "restore"],
        P6_DRAFT_FIELDS_RULE,
        P6_DRAFT_FIELDS_MESSAGE,
    ),
    (
        &["request", "restore"],
        P7_RESTORE_DESTINATIONS_RULE,
        P7_RESTORE_DESTINATIONS_MESSAGE,
    ),
    (
        &["request", "restore"],
        P8_RESTORE_SOURCE_RULE,
        P8_RESTORE_SOURCE_MESSAGE,
    ),
    (
        &["request", "restore"],
        P9_PLAN_HASH_RULE,
        P9_PLAN_HASH_MESSAGE,
    ),
    (
        &["request", "restore"],
        P10_ONE_RECOVERY_POINT_RULE,
        P10_ONE_RECOVERY_POINT_MESSAGE,
    ),
];

/// The default in-Job budget, in seconds, when the installation policy names
/// none.
pub const DEFAULT_TIMEOUT_SECONDS: i32 = 120;

fn default_timeout_seconds() -> i32 {
    DEFAULT_TIMEOUT_SECONDS
}

/// Which operation is being checked.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
pub enum PreflightOperation {
    /// A backup that has not been created yet.
    Backup,
    /// A restore — a draft plan, or one waiting for an approver.
    Restore,
    /// A destination's grants, on their own.
    DestinationAccess,
    /// A source connection, on its own: does this `KafkaCluster` answer, as
    /// this principal, right now (D2-SOURCECHECK).
    ///
    /// IT IS NOT A NARROWER `Backup`. A Backup readiness check needs a
    /// destination and one to a thousand named topics before it can be
    /// rendered at all, and a console control that asked an operator for those
    /// in order to test a connection would be a different question wearing the
    /// "Test connection" label.
    SourceConnection,
}

/// A reference to an object with its UID, so a same-named replacement is not
/// silently accepted as the referent.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UidRef {
    /// The referenced object's name, in this namespace.
    pub name: String,
    /// Its UID, when the requester knew it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
}

/// What a backup readiness check needs to know.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BackupPreflightRequest {
    /// The `KafkaCluster` the backup would read.
    pub source_ref: LocalRef,
    /// The saved destination it would write to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_ref: Option<LocalRef>,
    /// An inline archive location, for an installation that has not adopted
    /// saved destinations. Exactly one of this and `destinationRef` (P4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_archive: Option<ArchiveRef>,
    /// NAMED topics, never patterns — guard **G-GLOB**, as everywhere else.
    #[schemars(length(min = 1, max = 1000), inner(regex(path = "TOPIC_NAME_PATTERN")))]
    pub topics: Vec<String>,
    /// The schedule this check is about, when there is one. Context only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_ref: Option<LocalRef>,
}

/// What a restore readiness check needs to know.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RestorePreflightRequest {
    /// A draft plan, as the exact bytes an approver would sign. Exactly one of
    /// this and `restoreRef` (P5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 262144))]
    pub plan_bytes: Option<String>,
    /// The draft's sha256. **The controller recomputes it**; this field is the
    /// requester's claim, and a mismatch is the check's own finding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(regex(path = "SHA256_PATTERN"))]
    pub plan_hash: Option<String>,
    /// An existing `Restore` — typically one waiting for an approver.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restore_ref: Option<UidRef>,
    /// The target `KafkaCluster`. Required with a draft (P6); derived from the
    /// `Restore` otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_ref: Option<LocalRef>,
    /// The destination the archive is read from. Set with
    /// `evidenceDestinationRef` (P7).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_destination_ref: Option<LocalRef>,
    /// The destination evidence is written to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_destination_ref: Option<LocalRef>,
    /// An inline source archive, for an installation that has not adopted
    /// saved destinations. Exactly one of this and the destination refs (P8).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_source_archive: Option<ArchiveRef>,
    /// The recovery point being restored, by the identity PLAT-11.1 fixes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_point_ref: Option<UidRef>,
    /// Or a recovery point read from a `RecoveryCatalog`'s view (PLAT-15.2,
    /// D3 §5.5 step 5): the controller re-reads that catalog row when the
    /// check runs and reports `recoveryPoint.state` from it — the row's
    /// availability and verification, any reached `Backup` verdict on the same
    /// receipt, and whether the plan's `source.point` is this row's binding.
    /// At most one of this and `recoveryPointRef` (P10). ABSENT on every
    /// object written before PLAT-15.2, which therefore behaves exactly as it
    /// did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_point_ref: Option<CatalogPointRef>,
}

/// A recovery point in a `RecoveryCatalog`'s view, by its content-derived id.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CatalogPointRef {
    /// The catalog, in this namespace.
    pub catalog_ref: LocalRef,
    /// `lwp1-` plus 32 lowercase hex characters (D3 §5.1).
    #[schemars(regex(path = "POINT_ID_PATTERN"))]
    pub point_id: String,
}

/// What a destination-access check needs to know.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DestinationAccessRequest {
    /// The destination to exercise.
    pub destination_ref: LocalRef,
    /// Which grants to exercise. `ArchiveWrite`, `ArchiveRead`,
    /// `EvidenceWrite`, `EvidenceRead`.
    #[schemars(length(min = 1, max = 4))]
    pub roles: Vec<logweir_core::destination::DestinationRole>,
}

/// What a source-connectivity check needs to know.
///
/// ONE REFERENCE, AND THE ABSENCES ARE THE POINT: no destination, no plan, no
/// topic list and no signer. The check dials the connection this names with
/// that connection's own credential and reports whether the broker answered.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SourceConnectionPreflightRequest {
    /// The `KafkaCluster` to dial.
    pub connection_ref: LocalRef,
}

/// What to check. Sealed by P1 once created.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PreflightRequest {
    /// Which operation this is about. P3 ties the block below to it.
    pub operation: PreflightOperation,
    /// The backup block, for `operation: Backup`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup: Option<BackupPreflightRequest>,
    /// The restore block, for `operation: Restore`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restore: Option<RestorePreflightRequest>,
    /// The destination block, for `operation: DestinationAccess`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_access: Option<DestinationAccessRequest>,
    /// The connection block, for `operation: SourceConnection`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_connection: Option<SourceConnectionPreflightRequest>,
    /// Checks to leave out. **A skipped blocking check keeps the overall state
    /// `unknown`**: skipping a question is not answering it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 32))]
    pub skip_checks: Option<Vec<String>>,
    /// The in-Job budget.
    #[serde(default = "default_timeout_seconds")]
    #[schemars(range(min = 30, max = 600))]
    pub timeout_seconds: i32,
}

/// `Preflight.spec`.
#[derive(kube::CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "logweir.dev",
    version = "v1alpha1",
    kind = "Preflight",
    doc = "One bounded readiness observation for a Backup, a Restore, a BackupDestination's grants or a source connection on its own, executed as an isolated Job with no Kubernetes token. `spec.request` is immutable and `spec.cancelRequested` may only move from false to true. The verdict is ADVISORY: a `ready` result authorizes nothing and every execution-time guard still runs.",
    plural = "preflights",
    singular = "preflight",
    namespaced,
    status = "PreflightStatus",
    printcolumn = r#"{"name":"OPERATION","type":"string","jsonPath":".spec.request.operation"}"#,
    printcolumn = r#"{"name":"PHASE","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"RESULT","type":"string","jsonPath":".status.result.state","description":"ready, notReady or unknown — advisory, never authorization"}"#,
    printcolumn = r#"{"name":"EXPIRES","type":"date","jsonPath":".status.result.expiresAt"}"#,
    printcolumn = r#"{"name":"AGE","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct PreflightSpec {
    /// What to check. Immutable (P1).
    pub request: PreflightRequest,
    /// Ask the controller to stop. `false` → `true` only (P2).
    #[serde(default)]
    pub cancel_requested: bool,
}

/// One object this check was bound to, with the revision it observed.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Referent {
    /// The referent's kind.
    pub kind: String,
    /// Its name, in this namespace.
    pub name: String,
    /// Its UID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    /// Its `metadata.generation` at binding time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<i64>,
}

/// What this verdict is about, exactly.
///
/// THE POINT OF THIS BLOCK IS INVALIDATION. A verdict that does not say which
/// revision of which objects it was computed from cannot be told apart from a
/// verdict about something else, which is how a stale green badge is born.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PreflightBinding {
    /// The operation checked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
    /// The plan hash the controller RECOMPUTED, for a restore.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_hash: Option<String>,
    /// `sha256:<lowercase hex>` over the canonical binding inputs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inputs_digest: Option<String>,
    /// Every object this check read, with its revision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 16))]
    pub referents: Option<Vec<Referent>>,
    /// `sha256:<lowercase hex>` over the installation policy in force.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_digest: Option<String>,
}

/// Where a check's finding is about.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CheckScope {
    /// The object's kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Its name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Its UID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
}

/// One check's finding.
// The `detail` field of D2 §6.4 is deliberately ABSENT from this schema. A
// free-form JSON object in a structural schema needs
// `x-kubernetes-preserve-unknown-fields`, which turns off pruning for that
// subtree and makes the status a place arbitrary bytes can be parked. The
// bounded facts a UI needs are in `code`, `message` and `remedy`; anything
// larger belongs in the owned details `ConfigMap` `result.detailsRef` names.
//
// Kept out of the doc comment because `schemars` publishes doc comments as
// `description` in the shipped CRD.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CheckEntry {
    /// The check id, e.g. `target.mappedTopics`.
    pub id: String,
    /// Its category — `source`, `target`, `archive`, `evidence`, `runtime`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    /// What the finding is about.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<CheckScope>,
    /// `ready`, `notReady`, `unknown` or `skipped`.
    pub state: String,
    /// `blocking` or `advisory`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gating: Option<String>,
    /// Who answered — `controller`, `checkJob` or `podStatus`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority: Option<String>,
    /// The closed-vocabulary code. **Codes, not raw errors**: an S3 or SASL
    /// error body is exactly what redaction exists to keep out of a status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// A redacted, bounded explanation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 512))]
    pub message: Option<String>,
    /// What to do about it, in one sentence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 512))]
    pub remedy: Option<String>,
    /// When this check was answered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<Time>,
    /// When this check's answer stops being usable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<Time>,
}

/// The owned, immutable `ConfigMap` carrying the long form of the findings.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DetailsRef {
    /// The `ConfigMap` name, in this namespace.
    pub name: String,
    /// `sha256:<lowercase hex>` over its bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

/// The aggregated verdict.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PreflightResult {
    /// `ready`, `notReady` or `unknown`. `notReady` if any blocking check is
    /// `notReady`; otherwise `unknown` if any blocking check is `unknown` or
    /// `skipped`; otherwise `ready`.
    pub state: String,
    /// The minimum `expiresAt` over the non-skipped checks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<Time>,
    /// The findings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 64))]
    pub checks: Option<Vec<CheckEntry>>,
    /// Where the long form lives.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details_ref: Option<DetailsRef>,
}

/// `Preflight.status`.
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PreflightStatus {
    /// `Pending`, `Queued`, `Running`, `Completed`, `Failed` or `Cancelled`.
    /// `Failed` means no result could be produced — it is NOT `Completed` with
    /// `state: notReady`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// The reason of the condition this patch writes, promoted to a scalar.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// A redacted, bounded explanation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 1024))]
    pub message: Option<String>,
    /// What this verdict is about, exactly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding: Option<PreflightBinding>,
    /// The Job that ran, or is running, the check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_ref: Option<LocalRef>,
    /// When the check was taken — the runner container's `finishedAt`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<Time>,
    /// The verdict.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<PreflightResult>,
    /// The condition set. Two types, `Complete` and `Ready`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}
