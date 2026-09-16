//! The HTTP contract: request and response DTOs.
//!
//! SEPARATE FROM THE CRDs ON PURPOSE. Responses are product projections of
//! the stored objects, never the objects themselves: no `managedFields`, no
//! annotations, no owner references, no Secret data (only Secret NAMES), no
//! approval bytes outside the packet route, and archive URLs with any userinfo
//! redacted. Requests carry `#[serde(deny_unknown_fields)]` at every level, so
//! a field this contract does not name is refused rather than dropped.
//!
//! The OpenAPI document (`crate::openapi`) is generated from these types and
//! checked in as `schemas/logweir-api-v1.openapi.json`; the drift test fails
//! when the two disagree.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::auth::AuthenticationMode;
use crate::authz::{Capabilities, Role};

// ======================================================================
// Shared pieces
// ======================================================================

/// A same-namespace reference by name.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct NameRef {
    /// The referenced object's name, in the same namespace.
    pub name: String,
}

/// One page of a list.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Page {
    /// The page size that was applied.
    pub limit: u32,
    /// An opaque cursor for the next page, valid for fifteen minutes for the
    /// same actor, route, namespace and filters; absent on the last page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// The Kubernetes list resourceVersion this page was read at.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<String>,
}

/// A status condition, with a bounded message.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConditionView {
    /// The condition type.
    #[serde(rename = "type")]
    pub type_: String,
    /// `True`, `False` or `Unknown`.
    pub status: String,
    /// The CamelCase reason.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The message, at most 1024 bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// When the condition last changed status or reason.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<DateTime<Utc>>,
}

/// An archive location and the name of the Secret that reaches it.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ArchiveView {
    /// The object-store URL, with any userinfo redacted.
    pub url: String,
    /// The name of the Secret carrying the object-store credential. A name
    /// only; this service never reads Secrets.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential_ref: Option<NameRef>,
}

/// Health and readiness bodies.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HealthStatus {
    /// `ok` (liveness), `ready` or `notReady`.
    pub status: String,
}

// ======================================================================
// Session and namespaces
// ======================================================================

/// The authenticated actor.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActorView {
    /// The stable actor ID, `<issuer>#<subject>`.
    pub id: String,
    /// The identity issuer.
    pub issuer: String,
    /// The subject.
    pub subject: String,
    /// A display name. Never used for authorization.
    pub display_name: String,
}

/// One explicitly granted namespace.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceGrant {
    /// The namespace name.
    pub name: String,
    /// The product roles the actor holds here, sorted and unioned across every
    /// binding that matched. Empty in localAdmin mode, which has no roles.
    pub roles: Vec<Role>,
    /// What the actor may do there. Domains without a route are `false`.
    pub capabilities: Capabilities,
}

/// `GET /api/v1/session`.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionResponse {
    /// The request ID.
    pub request_id: String,
    /// How the actor was authenticated.
    pub authentication_mode: AuthenticationMode,
    /// The actor.
    pub actor: ActorView,
    /// When the session expires; `null` in localAdmin mode, which has no
    /// session.
    pub expires_at: Option<DateTime<Utc>>,
    /// The CSRF token for unsafe requests; `null` in localAdmin mode, which
    /// relies on the loopback listener and the exact Origin check.
    pub csrf_token: Option<String>,
    /// The revision of the administrator's role-binding table that produced
    /// these grants. It is recorded in every audit line, so a decision can be
    /// tied to the configuration that made it. Empty in localAdmin mode.
    pub binding_revision: String,
    /// The explicitly granted namespaces.
    pub namespaces: Vec<NamespaceGrant>,
    /// The union of the grants' capabilities.
    pub capabilities: Capabilities,
}

/// `GET /api/v1/namespaces`.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceListResponse {
    /// The request ID.
    pub request_id: String,
    /// The configured grants. Core `Namespace` objects are never listed.
    pub items: Vec<NamespaceGrant>,
}

// ======================================================================
// Connections (KafkaCluster)
// ======================================================================

/// What a connection is to Logweir.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ConnectionRole {
    /// A cluster backed up from.
    Source,
    /// A cluster restored into.
    Target,
}

/// The SASL mechanism, or `plaintext` for none.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ConnectionAuthMode {
    /// No SASL.
    Plaintext,
    /// SASL/SCRAM-SHA-512.
    ScramSha512,
}

/// Authentication for a new connection. Existing credentials only: this
/// request names a Secret; it never carries a password.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ConnectionAuthRequest {
    /// `plaintext` or `scramSha512`.
    pub mode: ConnectionAuthMode,
    /// The SASL principal; required for `scramSha512`, refused for
    /// `plaintext`.
    #[serde(default)]
    pub username: Option<String>,
    /// An existing Secret in this namespace holding the SASL password;
    /// required for `scramSha512`, refused for `plaintext`.
    #[serde(default)]
    pub credential_ref: Option<NameRef>,
    /// Whether the transport is TLS.
    pub tls: bool,
}

/// `POST /api/v1/namespaces/{ns}/connections`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CreateConnectionRequest {
    /// `source` or `target`.
    pub role: ConnectionRole,
    /// Broker bootstrap addresses, `host:port`, 1 to 16 entries.
    pub bootstrap_servers: Vec<String>,
    /// How Logweir authenticates.
    pub auth: ConnectionAuthRequest,
    /// The marker topic that proves a scratch target.
    #[serde(default)]
    pub marker_topic: Option<String>,
}

/// Authentication settings of a stored connection.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionAuthView {
    /// `plaintext` or `scramSha512`.
    pub mode: ConnectionAuthMode,
    /// The SASL principal.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// The credential Secret's NAME. Never its data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential_ref: Option<NameRef>,
    /// Whether the transport is TLS.
    pub tls: bool,
}

/// The controller's last reachability observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ReachabilityState {
    /// The last probe reached a broker.
    Reachable,
    /// The last probe did not.
    Unreachable,
    /// No completed probe.
    Unknown,
}

/// Reachability, as the controller recorded it.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReachabilityView {
    /// The state.
    pub state: ReachabilityState,
    /// The controller's CamelCase reason.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The cluster ID read from the broker.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_id: Option<String>,
    /// When the probe ran.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<DateTime<Utc>>,
}

/// A saved connection.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Connection {
    /// The object name.
    pub name: String,
    /// The namespace.
    pub namespace: String,
    /// The Kubernetes UID.
    pub uid: String,
    /// The resourceVersion this projection was read at.
    pub resource_version: String,
    /// When the object was created.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    /// The role label.
    pub role: String,
    /// Bootstrap addresses.
    pub bootstrap_servers: Vec<String>,
    /// Authentication settings.
    pub auth: ConnectionAuthView,
    /// The marker topic.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub marker_topic: Option<String>,
    /// The controller's reachability observation.
    pub reachability: ReachabilityView,
}

// ======================================================================
// Schedules (BackupSchedule)
// ======================================================================

/// An archive location in a request.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ArchiveRequest {
    /// `s3://`, `gs://`, `az://` or `file:///`; no userinfo.
    pub url: String,
    /// An existing Secret in this namespace carrying the object-store
    /// credential.
    #[serde(default)]
    pub credential_ref: Option<NameRef>,
}

/// Whether a slot may start while an earlier run is unfinished.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub enum ConcurrencyPolicy {
    /// Never overlap (the default).
    Forbid,
    /// Permit overlapping slots.
    Allow,
}

/// Retention reporting settings. Logweir deletes nothing.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RetentionRequest {
    /// Keep the newest N sets.
    #[serde(default)]
    pub keep_last: Option<i64>,
    /// Keep sets newer than N days.
    #[serde(default)]
    pub keep_days: Option<i64>,
}

/// `POST /api/v1/namespaces/{ns}/schedules`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CreateScheduleRequest {
    /// A five-field UTC cron expression or `@hourly`/`@daily`/`@weekly`,
    /// parsed by the controller's own parser.
    pub schedule: String,
    /// The source connection, in this namespace.
    pub source_ref: NameRef,
    /// Named topics, 1 to 256; patterns are refused.
    pub topics: Vec<String>,
    /// Where backups are written.
    pub archive: ArchiveRequest,
    /// `Forbid` (default) or `Allow`.
    #[serde(default)]
    pub concurrency_policy: Option<ConcurrencyPolicy>,
    /// Retention reporting.
    #[serde(default)]
    pub retention: Option<RetentionRequest>,
    /// Whether the schedule starts suspended.
    pub suspended: bool,
}

/// `POST /api/v1/namespaces/{ns}/schedules/{name}:set-suspension`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SetSuspensionRequest {
    /// The desired value of the schedule's one mutable field.
    pub suspended: bool,
    /// The resourceVersion the client last read. A stale value is
    /// `precondition_failed`.
    pub expected_resource_version: String,
}

/// Retention settings of a stored schedule.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RetentionView {
    /// Keep the newest N sets.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keep_last: Option<i64>,
    /// Keep sets newer than N days.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keep_days: Option<i64>,
}

/// One set retention would remove. It is still in the archive.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RemovableSetView {
    /// The backup set ID.
    pub backup_id: String,
    /// The newest record instant in the set.
    pub newest_record_at: DateTime<Utc>,
    /// `OlderThanKeepDays` or `BeyondKeepLast`.
    pub reason: String,
    /// The configured keepDays, for `OlderThanKeepDays`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub days: Option<i64>,
    /// The newest-first rank, for `BeyondKeepLast`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rank: Option<i64>,
}

/// The controller's retention report, bounded to 100 entries per list.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RetentionReportView {
    /// When retention was last evaluated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evaluated_at: Option<DateTime<Utc>>,
    /// keepLast as applied.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keep_last: Option<i64>,
    /// keepDays as applied.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keep_days: Option<i64>,
    /// Kept set IDs, newest first.
    pub sets_kept: Vec<String>,
    /// Sets that would be removed, newest first. Nothing was deleted.
    pub sets_that_would_be_removed: Vec<RemovableSetView>,
    /// Removal commands in the archive scheme's own CLI. Reported, never run.
    pub removal_commands: Vec<String>,
    /// The same commands in `mc` spelling.
    pub mc_removal_commands: Vec<String>,
    /// How many manifests could not be read.
    pub skipped_manifests: usize,
    /// Why nothing would be removed, when that is not "nothing to remove".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// Whether any list above was cut at 100 entries.
    pub truncated: bool,
}

/// What the schedule controller last recorded.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleStatusView {
    /// When the schedule last created a Backup.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_fire_time: Option<DateTime<Utc>>,
    /// When it will next create one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_fire_time: Option<DateTime<Utc>>,
    /// The running Backup's name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_backup: Option<String>,
    /// A reserved Backup name whose creation is not yet confirmed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_backup: Option<String>,
    /// The most recent skipped slot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_missed_slot: Option<String>,
    /// The `Ready` condition.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready: Option<ConditionView>,
    /// The retention report.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_report: Option<RetentionReportView>,
}

/// A saved schedule.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Schedule {
    /// The object name.
    pub name: String,
    /// The namespace.
    pub namespace: String,
    /// The Kubernetes UID.
    pub uid: String,
    /// The resourceVersion; send it back as `expectedResourceVersion`.
    pub resource_version: String,
    /// When the object was created.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    /// The cron expression.
    pub schedule: String,
    /// The source connection.
    pub source_ref: NameRef,
    /// Named topics.
    pub topics: Vec<String>,
    /// Where backups are written.
    pub archive: ArchiveView,
    /// `Forbid` or `Allow`.
    pub concurrency_policy: ConcurrencyPolicy,
    /// Retention reporting settings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention: Option<RetentionView>,
    /// Whether the schedule is suspended.
    pub suspended: bool,
    /// What the controller recorded.
    pub status: ScheduleStatusView,
}

// ======================================================================
// Operations: the normalized status model (PLAT-14.1 states)
// ======================================================================

/// The two operation kinds with a status route in this build.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum OperationKind {
    /// A `Backup`.
    Backup,
    /// A `Restore`.
    Restore,
}

/// The product lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum OperationState {
    /// Accepted and waiting: not yet observed by the controller, or held for
    /// an approval.
    Pending,
    /// Waiting for execution capacity. Not produced by current resources.
    Queued,
    /// The controller is materializing execution inputs.
    Preparing,
    /// The runner Job exists and has not finished.
    Running,
    /// The run finished and its signed evidence has not been verified yet.
    Verifying,
    /// The run exited 0.
    Succeeded,
    /// The run finished without a pass, crashed, or failed operationally.
    Failed,
    /// A guard or the controller refused the run before anything executed.
    Refused,
    /// Cancelled. Not produced by current resources (no cancel exists).
    Cancelled,
    /// A phase this build does not recognise.
    Unknown,
}

impl OperationState {
    /// Whether the state can no longer change on its own.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            OperationState::Succeeded
                | OperationState::Failed
                | OperationState::Refused
                | OperationState::Cancelled
        )
    }
}

/// The operation's result, separate from evidence verification.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ResultStatus {
    /// No result yet.
    Pending,
    /// Exit 0 (and, for a restore, outcome `pass` when recorded).
    Pass,
    /// Exit 2: a signed result that is not a pass.
    NotPass,
    /// Exit 3, or a controller refusal before execution.
    Refused,
    /// Exit 1 or 4, a crash, or a terminal failure without a code.
    Error,
    /// A combination this build does not recognise.
    Unknown,
}

/// Whether the controller verified the signed evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum VerificationState {
    /// Not finished, or evidence exists and no verdict is recorded yet.
    Pending,
    /// The controller verified the signature and digest.
    Valid,
    /// The controller verified and the answer was no.
    Invalid,
    /// The controller could not attempt verification. NOT a verified result.
    NotAttempted,
    /// The finished run recorded no signed evidence to verify.
    NoEvidence,
    /// A verdict string this build does not recognise.
    Unknown,
}

/// The result half of an operation.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OperationResult {
    /// The result class.
    pub status: ResultStatus,
    /// The runner's exit code, when one was recovered.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// The wire exit reason or terminal state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_reason: Option<String>,
    /// The restore scorecard outcome (`pass`, `fail-objective`, ...).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    /// The last restore phase slot completed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_phase_completed: Option<i32>,
}

/// The verification half of an operation.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OperationVerification {
    /// The verification state.
    pub state: VerificationState,
    /// The TrustRoster signing key that verified the evidence.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_key_id: Option<String>,
    /// When the verdict was reached.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verified_at: Option<DateTime<Utc>>,
    /// The DSSE payload type verified.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_type: Option<String>,
    /// Why the verdict is what it is, at most 512 bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Evidence object references. Keys and digests only.
#[derive(Clone, Debug, Default, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OperationEvidence {
    /// The signed receipt (backup) or scorecard (restore) key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_key: Option<String>,
    /// Its recorded SHA-256.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_sha256: Option<String>,
    /// The detached DSSE sidecar key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sidecar_key: Option<String>,
    /// The restore offset report key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset_report_key: Option<String>,
    /// The restore offset report SHA-256.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset_report_sha256: Option<String>,
}

/// The normalized status of one Backup or Restore.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Operation {
    /// `backup` or `restore`.
    pub kind: OperationKind,
    /// The object name.
    pub name: String,
    /// The namespace.
    pub namespace: String,
    /// The Kubernetes UID.
    pub uid: String,
    /// The resourceVersion this status was read at.
    pub resource_version: String,
    /// When the object was created.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    /// The lifecycle state.
    pub state: OperationState,
    /// The CamelCase reason for the state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_reason: Option<String>,
    /// The message of the condition that carries the reason, bounded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Whether the state is final.
    pub terminal: bool,
    /// The most recent condition transition or verification instant.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated_at: Option<DateTime<Utc>>,
    /// The result, separate from verification.
    pub result: OperationResult,
    /// Evidence verification, separate from the result.
    pub verification: OperationVerification,
    /// True only when the result passed AND the evidence verified `Valid`
    /// (the controller's green-badge rule). `NotAttempted` is never verified
    /// success.
    pub verified_success: bool,
    /// Evidence object references.
    pub evidence: OperationEvidence,
    /// The status conditions, at most 16.
    pub conditions: Vec<ConditionView>,
    //
    // THERE IS NO `jobName` HERE, DELIBERATELY. An earlier draft carried the
    // runner Job's name. PLAT-17.1 exists because "direct CR manipulation
    // exposes infrastructure details", and D0 gives PLAT-14.1 the final
    // normalized operation mapping: what "remains visible in bounded form" is
    // reason, message, exit code, last phase, timestamps and evidence
    // references. A Job name is on none of those lists.
    //
    // Removing it now is the cheap direction. Adding a field later is a MINOR
    // change to this document; removing one is MAJOR. So the field waits for
    // the task that owns the ruling instead of being frozen into a versioned
    // contract by whoever wrote the projection first.
    // `no_infrastructure_detail_is_frozen_into_the_operation_contract` in
    // `tests/status_mapping.rs` is what keeps it out.
}

/// The status summary carried by list items.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OperationSummary {
    /// The lifecycle state.
    pub state: OperationState,
    /// The CamelCase reason.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_reason: Option<String>,
    /// Whether the state is final.
    pub terminal: bool,
    /// The verification state.
    pub verification_state: VerificationState,
    /// Result pass AND verification `Valid`.
    pub verified_success: bool,
}

// ======================================================================
// Backups
// ======================================================================

/// A backup run.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Backup {
    /// The object name.
    pub name: String,
    /// The namespace.
    pub namespace: String,
    /// The Kubernetes UID.
    pub uid: String,
    /// The resourceVersion.
    pub resource_version: String,
    /// When the object was created.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    /// The source connection.
    pub source_ref: NameRef,
    /// Named topics.
    pub topics: Vec<String>,
    /// Where the archive is written.
    pub archive: ArchiveView,
    /// The schedule that created the run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule: Option<String>,
    /// The schedule slot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slot: Option<String>,
    /// `schedule` or `manual`.
    pub triggered_by: String,
    /// The Job deadline.
    pub deadline_seconds: i64,
    /// The archive backup ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backup_id: Option<String>,
    /// Records archived.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub records: Option<i64>,
    /// The covered window, epoch milliseconds, end exclusive.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_covered: Option<WindowCoveredView>,
    /// The manifest key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest_key: Option<String>,
    /// The identity the run presented: mode and username, never a password.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_auth: Option<ObservedAuthView>,
    /// The normalized status summary.
    pub operation: OperationSummary,
}

/// The covered window.
#[derive(Clone, Copy, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WindowCoveredView {
    /// Inclusive start, epoch milliseconds.
    pub from_ms: i64,
    /// Exclusive end, epoch milliseconds.
    pub to_ms: i64,
}

/// The identity a run presented.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ObservedAuthView {
    /// `plaintext` or `scramSha512`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// The SASL principal presented.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
}

// ======================================================================
// Restores
// ======================================================================

/// `scratch` (a drill) or `newTopic`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum RestoreMode {
    /// A drill into a proven scratch cluster, torn down afterwards.
    Scratch,
    /// A real restore into new topics.
    NewTopic,
}

/// How restored topics are named.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TopicNamingRequest {
    /// Prepended to each source topic name.
    pub prefix: String,
}

/// Where a restore writes.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RestoreTargetRequest {
    /// The target connection, in this namespace.
    pub cluster_ref: NameRef,
    /// `scratch` or `newTopic`.
    pub mode: RestoreMode,
    /// Topic naming.
    pub topic_naming: TopicNamingRequest,
}

/// `POST /api/v1/namespaces/{ns}/restores`.
///
/// `planBytes` is OPAQUE: the service checks `planHash` against the SHA-256 of
/// exactly these bytes and stores them unchanged. It never parses or
/// re-emits the plan document.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CreateRestoreRequest {
    /// The exact plan document bytes the approval will bind, at most 256 KiB.
    pub plan_bytes: String,
    /// `sha256:<lowercase hex>` of `planBytes`, as the client computed it.
    pub plan_hash: String,
    /// The Approval that will authorise this restore, in this namespace. It
    /// need not exist yet: the controller holds the restore until it
    /// verifies.
    pub approval_ref: NameRef,
    /// The archive to restore from.
    pub source_archive: ArchiveRequest,
    /// The backup set ID inside the archive.
    pub backup_set_ref: String,
    /// The RFC 3339 point in time to restore to.
    pub point_in_time: String,
    /// Where the restore writes.
    pub target: RestoreTargetRequest,
    /// The Job deadline, 60 to 86400 seconds.
    pub deadline_seconds: i64,
}

/// A restore target, as stored.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RestoreTargetView {
    /// The target connection.
    pub cluster_ref: NameRef,
    /// `scratch` or `newTopic`.
    pub mode: RestoreMode,
    /// The topic prefix.
    pub topic_prefix: String,
}

/// A restore run.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Restore {
    /// The object name.
    pub name: String,
    /// The namespace.
    pub namespace: String,
    /// The Kubernetes UID.
    pub uid: String,
    /// The resourceVersion.
    pub resource_version: String,
    /// When the object was created.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    /// `sha256:<hex>` of the stored plan bytes, computed by this service.
    pub plan_hash: String,
    /// The stored plan bytes' length in bytes.
    pub plan_bytes_length: usize,
    /// The stored plan bytes, unchanged. Present on the single-object read
    /// only; lists omit it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_bytes: Option<String>,
    /// The Approval name this restore waits for.
    pub approval_ref: NameRef,
    /// The archive restored from.
    pub source_archive: ArchiveView,
    /// The backup set ID.
    pub backup_set_ref: String,
    /// The point in time.
    pub point_in_time: DateTime<Utc>,
    /// Where it writes.
    pub target: RestoreTargetView,
    /// The Job deadline.
    pub deadline_seconds: i64,
    /// The topics this run created.
    pub new_topics: Vec<String>,
    /// The normalized status summary.
    pub operation: OperationSummary,
}

// ======================================================================
// Approvals
// ======================================================================

/// What an approval is about.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SubjectRefView {
    /// `Restore` or `Backup`.
    pub kind: String,
    /// The subject name.
    pub name: String,
}

/// The exact object whose bytes the controller verified.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VerifiedSubjectView {
    /// The subject kind.
    pub kind: String,
    /// The subject name.
    pub name: String,
    /// The subject namespace.
    pub namespace: String,
    /// The subject UID.
    pub uid: String,
}

/// Public approval metadata. The documents themselves are only on the packet
/// route.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Approval {
    /// The object name.
    pub name: String,
    /// The namespace.
    pub namespace: String,
    /// The Kubernetes UID.
    pub uid: String,
    /// The resourceVersion.
    pub resource_version: String,
    /// When the object was created.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    /// The subject.
    pub subject_ref: SubjectRefView,
    /// The plan hash the create form supplied.
    pub plan_hash: String,
    /// The approval document's length in bytes.
    pub approval_bytes_length: usize,
    /// The sidecar's length in bytes.
    pub sidecar_bytes_length: usize,
    /// Whether the controller verified the approval.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verified: Option<bool>,
    /// The approver key that verified it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_key_id: Option<String>,
    /// The approver subject from the roster.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approver: Option<String>,
    /// The change ticket the document names.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ticket: Option<String>,
    /// Whether approver and requester may be the same principal.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub self_attested_risk: Option<bool>,
    /// The exact verified subject.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verified_subject: Option<VerifiedSubjectView>,
    /// The status conditions, at most 16.
    pub conditions: Vec<ConditionView>,
}

/// `GET .../approvals/{name}/packet`: the raw documents, verbatim.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalPacket {
    /// The object name.
    pub name: String,
    /// The namespace.
    pub namespace: String,
    /// The Kubernetes UID.
    pub uid: String,
    /// The subject.
    pub subject_ref: SubjectRefView,
    /// The plan hash the create form supplied.
    pub plan_hash: String,
    /// The approval document text, verbatim.
    pub approval_bytes: String,
    /// The DSSE sidecar text, verbatim.
    pub sidecar_bytes: String,
}

// ======================================================================
// Envelopes
// ======================================================================

macro_rules! envelopes {
    ($($item:ident => $list:ident, $single:ident;)*) => {$(
        #[doc = concat!("A page of `", stringify!($item), "` items.")]
        #[derive(Clone, Debug, Serialize, JsonSchema)]
        #[serde(rename_all = "camelCase")]
        pub struct $list {
            /// The request ID.
            pub request_id: String,
            /// The items on this page.
            pub items: Vec<$item>,
            /// Paging.
            pub page: Page,
        }

        #[doc = concat!("One `", stringify!($item), "`.")]
        #[derive(Clone, Debug, Serialize, JsonSchema)]
        #[serde(rename_all = "camelCase")]
        pub struct $single {
            /// The request ID.
            pub request_id: String,
            /// On a durable create: whether this response replays an earlier
            /// identical request (HTTP 200) rather than creating (HTTP 201).
            #[serde(skip_serializing_if = "Option::is_none")]
            pub replayed: Option<bool>,
            /// The item.
            pub item: $item,
        }
    )*};
}

envelopes! {
    Connection => ConnectionList, ConnectionResponse;
    Schedule => ScheduleList, ScheduleResponse;
    Backup => BackupList, BackupResponse;
    Restore => RestoreList, RestoreResponse;
    Approval => ApprovalList, ApprovalResponse;
}

/// `GET .../operations/{kind}/{name}`.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OperationResponse {
    /// The request ID.
    pub request_id: String,
    /// The normalized operation.
    pub item: Operation,
}

/// `GET .../approvals/{name}/packet`.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalPacketResponse {
    /// The request ID.
    pub request_id: String,
    /// The packet.
    pub item: ApprovalPacket,
}
