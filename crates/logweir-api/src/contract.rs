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
    /// The newest `SourceConnection` preflight for this connection, when the
    /// detail route found one.
    ///
    /// A DIFFERENT FACT FROM `reachability`, and the console renders them
    /// apart. `reachability` is the controller's own probe on its own cadence;
    /// this is a check somebody asked for, with its own instant and its own
    /// staleness. Absent on a list and on a create: one detail read is one
    /// bounded label scan, and a page of connections would be one per row.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_test: Option<LastTestView>,
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
    /// The `metadata.generation` the controller has evaluated. A `generation`
    /// ahead of this one is an edit the controller has not seen yet; ABSENT
    /// means it has not been computed, never that it is zero.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// The revision the controller last evaluated, with the effective zone,
    /// the tz database that resolved it and the run-policy digest.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<SchedulePolicyView>,
    /// Up to five upcoming firings for the current generation; empty when the
    /// schedule is suspended or invalid. **This is the staleness signal**: a
    /// live controller rewrites these once `nextRuns[0].at` has passed, so a
    /// first entry in the past means nobody is evaluating this schedule.
    /// ABSENT means "not yet computed", never "it never fires".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_runs: Option<Vec<NextRunView>>,
    /// The schedule-created runs that are not terminal. ABSENT means "not yet
    /// computed", never "none are running".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_runs: Option<Vec<ActiveRunView>>,
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
    /// The resourceVersion; send it back as `expectedResourceVersion` on
    /// `:set-suspension`.
    pub resource_version: String,
    /// The `metadata.generation`; send it back as `expectedGeneration` on
    /// `PUT .../schedules/{name}` and on a manual run taken from this
    /// schedule. It increments on EVERY spec change, suspension included.
    ///
    /// OPTIONAL IN THE SCHEMA, ALWAYS EMITTED BY THIS BUILD. The API server
    /// sets `metadata.generation` on every object, so this projection always
    /// carries it; it is declared optional ONLY because `ui/contract.js`'s
    /// decoder drift test compares the schema's `required` set with a frozen
    /// list on the console side, and moving a field into that set is a
    /// contract change that must land in the same commit as `ui/contract.js`
    /// and the console fixtures (D1 W7's ownership). A console that somehow
    /// sees it absent must ask for a reload rather than guess a value for
    /// `expectedGeneration`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<i64>,
    /// When the object was created.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    /// The cron expression.
    pub schedule: String,
    /// The preset this expression IS, or absent for "Advanced cron".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preset: Option<CadencePreset>,
    /// The IANA zone the expression is evaluated in. ABSENT means UTC, which
    /// is what every pre-PLAT-04.2 schedule does.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_zone: Option<String>,
    /// The source connection. Immutable: a different cluster is a different
    /// schedule.
    pub source_ref: NameRef,
    /// Named topics. Empty with `allUserTopics`.
    pub topics: Vec<String>,
    /// Dynamic selection, when the schedule uses it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub all_user_topics: Option<AllUserTopics>,
    /// Where backups are written. With `destinationRef` this is the CRD's
    /// sentinel URL and the console should render the destination instead.
    pub archive: ArchiveView,
    /// The saved destination, when the schedule names one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination_ref: Option<NameRef>,
    /// `Forbid` or `Allow`.
    pub concurrency_policy: ConcurrencyPolicy,
    /// How long after its instant a slot may still start. ABSENT means 3600.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub starting_deadline_seconds: Option<i64>,
    /// ABSENT means `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catch_up_policy: Option<CatchUpPolicy>,
    /// ABSENT means no retries.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryPolicy>,
    /// The run's `activeDeadlineSeconds`. ABSENT means 3600.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_deadline_seconds: Option<i64>,
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
    /// Where the archive is written. With `destinationRef` this is the CRD's
    /// sentinel URL `logweir-destination://<name>` and carries no credential;
    /// a console renders the destination instead of the URL.
    pub archive: ArchiveView,
    /// The saved `BackupDestination` this run was written to, in this
    /// namespace.
    ///
    /// ABSENT means the run carries its archive location INLINE in `archive`,
    /// exactly as every run did before saved destinations existed. It is a
    /// legacy inline-archive run, not a degraded one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination_ref: Option<BackupDestinationRefView>,
    /// `sha256:<lowercase hex>` over the canonical location this run was
    /// FROZEN against — **where this recovery point's archive actually is**,
    /// copied verbatim from `Backup.status.destination.locationDigest`.
    ///
    /// ABSENT for a legacy inline-archive run, and for any run frozen by a
    /// controller that predates the frozen-destination block. Absent means
    /// "this recovery point publishes no frozen location", never "its location
    /// is unknown to be wrong" — and NOTHING recomputes one from a live
    /// `BackupDestination`: a destination edited after a run froze must not be
    /// able to make a moved recovery point look settled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location_digest: Option<String>,
    /// The schedule that created the run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule: Option<String>,
    /// The schedule slot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slot: Option<String>,
    /// `schedule` or `manual`.
    pub triggered_by: String,
    /// What caused this run. ABSENT on a run created before PLAT-04.2: read
    /// `triggeredBy` then, and say "trigger not recorded" rather than
    /// inventing one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger: Option<TriggerView>,
    /// The schedule revision this run copied. ABSENT — or present with no
    /// `uid`/`generation` — on a run created before PLAT-05.1: the console
    /// shows "revision not recorded", never a guess.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule_ref: Option<ScheduleRefView>,
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

/// Which kind of run a `Backup` is (D1 §3.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
pub enum TriggerKind {
    /// A slot that fired at its own instant.
    Scheduled,
    /// The same slot, started late because the controller was not running.
    CatchUp,
    /// Attempt k of a slot, 1 to 3, with a NEW execution id.
    Retry,
    /// Created by a person, this API or the console. No slot; the execution id
    /// is the object's own UID.
    Manual,
}

/// What caused a run.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TriggerView {
    /// The kind.
    pub kind: TriggerKind,
    /// 0 for `Scheduled`, `CatchUp` and `Manual`; 1 to 3 for `Retry`.
    pub attempt: i32,
    /// The attempt this one retries.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_of: Option<NameRef>,
    /// The zone the slot was computed in. INFORMATIONAL: the slot itself is
    /// UTC, and this is what keeps a history row's local time readable after
    /// somebody edits the schedule's `timeZone`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_zone: Option<String>,
}

/// The schedule revision a run recorded.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleRefView {
    /// The schedule's name, in this namespace.
    pub name: String,
    /// Its UID. A schedule deleted and recreated under the same name is a
    /// different schedule; a run of the old one is not a run of the new one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    /// The revision this run copied.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<i64>,
    /// The digest of the policy it copied.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_policy_sha256: Option<String>,
}

/// The saved destination a run was written to.
///
/// TWO HALVES FROM TWO PLACES, AND THE DIFFERENCE IS THE POINT. `name` is what
/// the run ASKED for — `Backup.spec.destinationRef`, immutable with the rest of
/// the spec. `uid` is what the controller RESOLVED at the freeze —
/// `Backup.status.destination.uid` — and it is ABSENT until the freeze wrote
/// one, and on every run frozen by a controller that predates the block. A
/// destination deleted and recreated under the same name is a different object,
/// and the uid is what says so; nothing here reads a live `BackupDestination`
/// to fill it in.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackupDestinationRefView {
    /// The destination's name, in this namespace.
    pub name: String,
    /// Its `metadata.uid` as the freeze recorded it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
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

/// One row of the topic mapping the caller previewed: a source topic in the
/// recovery point, and the target topic the restore would create for it.
///
/// THIS IS A DECLARATION, NOT A STORED FIELD. `Restore.spec` has no topic
/// list — the subset lives in the opaque plan bytes
/// (`logweir_core::spec::SourceSpec::topics`) — so nothing here is persisted.
/// What it buys is a rail this service CAN check without parsing the plan:
/// `target.topicNaming.prefix` IS stored, the mapping rule is prefix
/// concatenation and nothing else (`logweir_core::spec::target_topic_prefix`),
/// so every row must be exactly `prefix + source`. A console that previewed
/// one mapping and submitted another is refused here by name instead of
/// discovering it in phase 0 after an approver has signed.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TopicMappingRow {
    /// The source topic, as the recovery point froze it.
    pub source: String,
    /// The target topic name this restore would create for it.
    pub target: String,
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
    /// The saved destination the archive is read from. Set together with
    /// `evidenceDestinationRef`; when set, `sourceArchive.url` is the
    /// `logweir-destination://<name>` sentinel and carries no credential.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_destination_ref: Option<NameRef>,
    /// The saved destination evidence is written to. Set together with
    /// `sourceDestinationRef`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_destination_ref: Option<NameRef>,
    /// The backup set ID inside the archive.
    pub backup_set_ref: String,
    /// The RFC 3339 point in time to restore to.
    pub point_in_time: String,
    /// Where the restore writes.
    pub target: RestoreTargetRequest,
    /// The Job deadline, 60 to 86400 seconds.
    pub deadline_seconds: i64,
    /// The exact source→target mapping the caller previewed, checked against
    /// `target.topicNaming.prefix` and refused row by row.
    ///
    /// ABSENT IS EXACTLY THE BEHAVIOUR THIS ROUTE HAD BEFORE THE FIELD
    /// EXISTED, and `skip_serializing_if` keeps an absent one out of the
    /// idempotency request hash, so a client that predates it replays onto the
    /// same object it always did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_mapping: Option<Vec<TopicMappingRow>>,
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
// Destinations (BackupDestination, PLAT-08)
// ======================================================================

/// The object-store provider a destination names.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum StorageProviderDto {
    /// S3 and S3-compatible endpoints.
    S3,
}

/// How a request names the bucket.
///
/// NEVER A TRANSPORT CHOICE, in either direction. Addressing says how the
/// bucket appears in the URL; `transport.security` alone decides whether the
/// connection is encrypted. This is defect G5 written into the contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum AddressingDto {
    /// `https://endpoint/bucket/key`.
    PathStyle,
    /// `https://bucket.endpoint/key`.
    VirtualHosted,
}

/// Transport security. Immutable once the destination exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum TransportSecurityDto {
    /// TLS, with an `https://` endpoint or none.
    Tls,
    /// Plaintext HTTP, and only with an explicit `http://` endpoint.
    InsecureHttp,
}

/// How one role's credential is obtained.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum AccessModeDto {
    /// Keys read from a Secret in this namespace, projected into the Job.
    SecretKeys,
    /// The pod's own ServiceAccount identity; no Secret is projected.
    WorkloadIdentity,
    /// The controller's own read-only object-store handle. `evidenceRead`
    /// only.
    ControllerIdentity,
    /// The destination's explicit read-only `archiveRead` grant.
    /// `evidenceRead` only.
    ArchiveReadGrant,
    /// RESPONSES ONLY: the grant is absent and `archiveWrite` is used. It is
    /// never accepted in a request, because "absent" and "explicitly say the
    /// thing absence means" would then be two spellings of one state.
    InheritsArchiveWrite,
    /// RESPONSES ONLY: `evidenceRead` is absent, so verification is
    /// `NotAttempted` and this says so rather than leaving a blank field.
    NotConfigured,
}

/// Which credential a destination test exercises.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "camelCase")]
pub enum DestinationRoleDto {
    /// The grant execution Jobs write the archive with.
    ArchiveWrite,
    /// The read-only archive grant.
    ArchiveRead,
    /// The grant that writes evidence under `logweir/`.
    EvidenceWrite,
    /// The grant that reads evidence back for verification.
    EvidenceRead,
}

/// Whether an explicit readiness test may write a marker object.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum WriteProbeDto {
    /// No object is ever written by a readiness test.
    Disabled,
    /// A destination test may create ONE marker object under the
    /// destination's own prefix. It is never deleted.
    CreateOnlyMarker,
}

/// Where the archive root is. Immutable once the destination exists.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct StorageRequest {
    /// The object-store provider.
    pub provider: StorageProviderDto,
    /// The bucket.
    pub bucket: String,
    /// The key prefix inside the bucket, relative; absent is the bucket root.
    /// Never `logweir` or anything under it.
    #[serde(default)]
    pub prefix: Option<String>,
    /// The region, when the provider needs one.
    #[serde(default)]
    pub region: Option<String>,
    /// An http(s) origin — scheme, host and optional port. Absent means AWS
    /// S3.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// How a request names the bucket.
    pub addressing: AddressingDto,
}

/// A CA bundle in a `ConfigMap` in this namespace. A `ConfigMap` and never a
/// Secret: a CA certificate is public material.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CaBundleRequest {
    /// The `ConfigMap` name, in this namespace.
    pub config_map_name: String,
    /// The data key holding the PEM bundle. Absent means `ca.crt`.
    #[serde(default)]
    pub key: Option<String>,
}

/// Transport security and its trust material.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TransportRequest {
    /// `tls` or `insecureHttp`. It must agree with the endpoint scheme, and
    /// `addressing` never changes it.
    pub security: TransportSecurityDto,
    /// A private CA, for `tls` only.
    #[serde(default)]
    pub ca_bundle: Option<CaBundleRequest>,
}

/// A credential value typed once.
///
/// WRITE-ONLY. These bytes are turned into a Secret and are never echoed, in
/// any response, log line, status, annotation or audit record. The Secret is
/// created and never read back; the only thing that comes out of this field is
/// a Secret NAME.
#[derive(Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct NewCredentialRequest {
    /// The access key id.
    pub access_key_id: String,
    /// The secret access key.
    pub secret_access_key: String,
    /// A session token, when the credential is temporary.
    #[serde(default)]
    pub session_token: Option<String>,
}

// `Debug` is HAND-WRITTEN so that a `dbg!`, a `tracing` field or a panic
// message can never print a credential. The type has no `Display`.
impl std::fmt::Debug for NewCredentialRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NewCredentialRequest(<redacted>)")
    }
}

/// An existing Secret in this namespace, and the keys inside it.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ExistingSecretRequest {
    /// The Secret name.
    pub name: String,
    /// The data key holding the access key id. Absent means `access-key-id`.
    #[serde(default)]
    pub access_key_id_key: Option<String>,
    /// The data key holding the secret access key. Absent means
    /// `secret-access-key`.
    #[serde(default)]
    pub secret_access_key_key: Option<String>,
    /// The data key holding a session token. No default.
    #[serde(default)]
    pub session_token_key: Option<String>,
}

/// Exactly one of an existing Secret reference or a new write-only value.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SecretSourceRequest {
    /// Name an existing Secret.
    #[serde(default)]
    pub existing: Option<ExistingSecretRequest>,
    /// Or type the credential once, here.
    #[serde(default)]
    pub new: Option<NewCredentialRequest>,
}

/// The ServiceAccount a workload-identity grant runs as.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct WorkloadIdentityRequest {
    /// The ServiceAccount name. Absent means `logweir-runner`.
    #[serde(default)]
    pub service_account_name: Option<String>,
}

/// One role's grant.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AccessGrantRequest {
    /// How the credential is obtained.
    pub mode: AccessModeDto,
    /// The Secret, for `secretKeys`.
    #[serde(default)]
    pub secret: Option<SecretSourceRequest>,
    /// The ServiceAccount, for `workloadIdentity`.
    #[serde(default)]
    pub workload_identity: Option<WorkloadIdentityRequest>,
}

/// The four grants. Absent is a DEFINED answer and never a wider one:
/// `archiveRead` and `evidenceWrite` absent mean "use `archiveWrite`", and
/// `evidenceRead` absent means verification is not attempted and says so.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AccessRequest {
    /// The grant execution Jobs write the archive with. Required.
    pub archive_write: AccessGrantRequest,
    /// The read-only archive grant.
    #[serde(default)]
    pub archive_read: Option<AccessGrantRequest>,
    /// The grant that writes evidence under `logweir/`.
    #[serde(default)]
    pub evidence_write: Option<AccessGrantRequest>,
    /// The grant that reads evidence back for verification.
    #[serde(default)]
    pub evidence_read: Option<AccessGrantRequest>,
}

/// Explicit readiness settings.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReadinessRequest {
    /// Whether a destination test may write a marker. Absent means
    /// `createOnlyMarker` HERE, while the CRD's own default is `disabled`:
    /// a form that discloses the choice may default to the useful answer, and
    /// an object created by `kubectl` with no opinion may not.
    #[serde(default)]
    pub write_probe: Option<WriteProbeDto>,
}

/// `POST /api/v1/namespaces/{ns}/destinations`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CreateDestinationRequest {
    /// The destination's name, which is also its Kubernetes object name: a
    /// schedule, a backup and a restore all reference it by this name.
    pub name: String,
    /// What this destination is, for a human reading a list.
    #[serde(default)]
    pub description: Option<String>,
    /// The location. Immutable once created.
    pub storage: StorageRequest,
    /// Transport security and its trust material.
    pub transport: TransportRequest,
    /// The four credential grants.
    pub access: AccessRequest,
    /// Readiness settings.
    #[serde(default)]
    pub readiness: Option<ReadinessRequest>,
    /// Whether this becomes the namespace's default destination. At most one
    /// destination per namespace may hold it, and a second request naming
    /// another default is a conflict rather than a silent takeover.
    #[serde(default)]
    pub default: Option<bool>,
}

/// The transport half of an access rotation: the CA reference, and nothing
/// else. `security` is absent from this type ON PURPOSE — it is immutable.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct UpdateTransportRequest {
    /// The new CA reference, or `null` to clear it.
    #[serde(default)]
    pub ca_bundle: Option<CaBundleRequest>,
}

/// `POST .../destinations/{name}:update-access`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct UpdateDestinationAccessRequest {
    /// The `metadata.generation` the client last read. A stale value is
    /// `precondition_failed`: a rotation must not land on a destination that
    /// changed under the operator's feet.
    pub expected_generation: i64,
    /// The complete four-grant object. A grant omitted here is REMOVED.
    pub access: AccessRequest,
    /// The CA reference. Absent leaves it alone; present with a `null`
    /// `caBundle` clears it.
    #[serde(default)]
    pub transport: Option<UpdateTransportRequest>,
}

/// `POST .../destinations:from-legacy`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DestinationFromLegacyRequest {
    /// The new destination's name.
    pub name: String,
    /// The legacy `BackupSchedule` to derive the location from.
    #[serde(default)]
    pub source_schedule: Option<String>,
    /// Or the legacy `Backup`.
    #[serde(default)]
    pub source_backup: Option<String>,
    /// The four credential grants for the derived location.
    pub access: AccessRequest,
    /// What this destination is.
    #[serde(default)]
    pub description: Option<String>,
}

/// `POST .../destinations/{name}:test`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TestDestinationRequest {
    /// The roles to exercise, 1 to 4. Absent means every configured role.
    #[serde(default)]
    pub roles: Option<Vec<DestinationRoleDto>>,
}

/// A destination's location, as stored.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StorageView {
    /// The provider.
    pub provider: StorageProviderDto,
    /// The bucket.
    pub bucket: String,
    /// The prefix, `""` for the bucket root.
    pub prefix: String,
    /// The region.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// The endpoint origin; absent means AWS S3.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// How a request names the bucket.
    pub addressing: AddressingDto,
}

/// A CA bundle reference, with the digest the controller observed.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CaBundleView {
    /// The `ConfigMap` name.
    pub config_map_name: String,
    /// The data key.
    pub key: String,
    /// `sha256:<hex>` over the CA bytes the controller read, so a rotation is
    /// visible without reading the bundle again.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

/// Transport security, as stored.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TransportView {
    /// `tls` or `insecureHttp`.
    pub security: TransportSecurityDto,
    /// The CA reference.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca_bundle: Option<CaBundleView>,
}

/// One role's grant, as stored. NEVER A CREDENTIAL VALUE: a Secret name and
/// the key names inside it, both of which are public references.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AccessGrantView {
    /// How the credential is obtained.
    pub mode: AccessModeDto,
    /// The Secret's NAME, for `secretKeys`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_name: Option<String>,
    /// The data KEY NAMES inside it, sorted.
    pub keys: Vec<String>,
    /// The ServiceAccount, for `workloadIdentity`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_account_name: Option<String>,
}

/// The four grants, as stored.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AccessView {
    /// The archive write grant.
    pub archive_write: AccessGrantView,
    /// The archive read grant, or `inheritsArchiveWrite`.
    pub archive_read: AccessGrantView,
    /// The evidence write grant, or `inheritsArchiveWrite`.
    pub evidence_write: AccessGrantView,
    /// The evidence read grant, or `notConfigured`.
    pub evidence_read: AccessGrantView,
}

/// The controller's verdict on a destination.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DestinationStatusView {
    /// Whether the `Valid` condition is `True`. `null` means the controller
    /// has not reached a verdict yet — which is NOT the same as invalid.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub valid: Option<bool>,
    /// The CamelCase reason.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The bounded message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// The generation the verdict was computed from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// When the controller last reached it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<DateTime<Utc>>,
}

/// The most recent explicit access test.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LastTestView {
    /// The `Preflight` that ran it.
    pub preflight_id: String,
    /// Its aggregate state.
    pub state: PreflightState,
    /// When the check observed the destination.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<DateTime<Utc>>,
    /// Whether the result has expired or its inputs changed. A stale test is
    /// never rendered as health.
    pub stale: bool,
    /// Whether the search for the newest test hit its page bound, so this may
    /// not be the newest one. A "last test" that might not be last is worse
    /// than none, so the caller is told rather than reassured.
    pub truncated: bool,
}

/// A saved destination.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Destination {
    /// The object name, which is how everything else references it.
    pub name: String,
    /// The namespace.
    pub namespace: String,
    /// The Kubernetes UID.
    pub uid: String,
    /// The resourceVersion this projection was read at.
    pub resource_version: String,
    /// The `metadata.generation`; send it back as `expectedGeneration`.
    pub generation: i64,
    /// When the object was created.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    /// What this destination is.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The location.
    pub storage: StorageView,
    /// Transport security.
    pub transport: TransportView,
    /// The four grants, references only.
    pub access: AccessView,
    /// Whether a test may write a marker.
    pub write_probe: WriteProbeDto,
    /// The archive root as one URL, so an operator and a plan never spell one
    /// location two ways.
    pub canonical_url: String,
    /// `sha256:<hex>` over the canonical location, as the controller computed
    /// it. Absent until the controller has observed the object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location_digest: Option<String>,
    /// The controller's verdict.
    pub status: DestinationStatusView,
    /// Whether this is the namespace's default destination.
    pub default: bool,
    /// The most recent explicit access test.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_test: Option<LastTestView>,
}

/// A destination, as a list row.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DestinationSummary {
    /// The object name.
    pub name: String,
    /// The Kubernetes UID.
    pub uid: String,
    /// The `metadata.generation`.
    pub generation: i64,
    /// What this destination is.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The archive root as one URL.
    pub canonical_url: String,
    /// The endpoint origin; absent means AWS S3.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// `tls` or `insecureHttp`.
    pub transport: TransportSecurityDto,
    /// How a request names the bucket.
    pub addressing: AddressingDto,
    /// The controller's verdict.
    pub status: DestinationStatusView,
    /// Whether this is the namespace's default destination.
    pub default: bool,
}

/// One object a destination is referenced by.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DestinationUseView {
    /// `BackupSchedule` or `Backup`.
    pub kind: String,
    /// The object name.
    pub name: String,
    /// When it was created.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
}

/// `GET .../destinations/{name}/usage`.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DestinationUsageResponse {
    /// The request ID.
    pub request_id: String,
    /// The destination.
    pub name: String,
    /// Schedules that name it, at most 100.
    pub schedules: Vec<DestinationUseView>,
    /// Recent backups that name it, at most 100, newest name last.
    pub backups: Vec<DestinationUseView>,
    /// Whether either list was cut.
    pub truncated: bool,
    /// How the lists were built, stated so an empty answer is not read as
    /// "nothing uses this".
    pub basis: String,
}

/// `GET .../destinations` — one page of rows.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DestinationList {
    /// The request ID.
    pub request_id: String,
    /// The rows on this page.
    pub items: Vec<DestinationSummary>,
    /// Paging.
    pub page: Page,
}

/// One `Destination`.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DestinationResponse {
    /// The request ID.
    pub request_id: String,
    /// On a durable create: whether this replays an earlier identical request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replayed: Option<bool>,
    //
    // THERE IS NO `addressingSource` AND NO `notes` HERE, DELIBERATELY. D2
    // §3.12 defines both for the legacy adoption: `addressingSource` names
    // WHICH source the storage block was derived from (`frozenExecution` or
    // `installationConfig`), and `notes` carries the confirmations that
    // derivation owes the operator. Neither source is readable in this build,
    // so `:from-legacy` refuses instead of deriving (see
    // `routes::destinations`), and a field nothing can fill is a field a
    // console would render as "derived from nothing".
    //
    // Adding a field later is a MINOR change to this document; removing one is
    // MAJOR. So they wait for W11, which is the task that can populate them.
    /// The item.
    pub item: Destination,
}

// ======================================================================
// Topic discoveries (TopicDiscovery, PLAT-09.1)
// ======================================================================

/// The lifecycle of a transient check.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum CheckLifecycle {
    /// Accepted; the controller has not observed it yet.
    Pending,
    /// Waiting for a check slot.
    Queued,
    /// The check Job exists and has not finished.
    Running,
    /// A result was produced.
    Succeeded,
    /// No result could be produced. Distinct from a result that says "no".
    Failed,
    /// Cancelled before a result.
    Cancelled,
    /// A phase this build does not recognise.
    Unknown,
}

impl CheckLifecycle {
    /// Whether the state can no longer change on its own.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            CheckLifecycle::Succeeded | CheckLifecycle::Failed | CheckLifecycle::Cancelled
        )
    }
}

/// How complete a topic inventory's own author believes it is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum VisibilityState {
    /// A successful Kafka list ALONE. It is never called complete: an ACL can
    /// hide a topic from `DESCRIBE` with no error anywhere.
    Unknown,
    /// An authorization omission was observed.
    Limited,
    /// An administrator-governed attestation says the principal sees
    /// everything.
    AttestedComplete,
}

/// What a discovery could see, and why.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VisibilityView {
    /// The state.
    pub state: VisibilityState,
    /// The observations behind it.
    pub basis: Vec<String>,
    /// The attestation that justified `attestedComplete`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attestation: Option<String>,
}

/// The counts a discovery recorded.
#[derive(Clone, Copy, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveryCountsView {
    /// Names the broker listed.
    pub listed: i64,
    /// Entries stored.
    pub returned: i64,
    /// Internal topics excluded.
    pub internal_excluded: i64,
    /// Entries whose metadata could not be read.
    pub errored: i64,
}

/// What became of the expected topics.
#[derive(Clone, Copy, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ExpectedTopicsView {
    /// How many were asked about.
    pub requested: i64,
    /// Visible in the listing.
    pub visible: i64,
    /// Refused by authorization.
    pub not_authorized: i64,
    /// Absent from the cluster.
    pub not_found: i64,
    /// Neither confirmed nor refuted.
    pub unknown: i64,
}

/// The connection a discovery was bound to when it ran.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveryConnectionView {
    /// The connection name.
    pub name: String,
    /// Its UID when the check ran.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    /// Its generation when the check ran.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<i64>,
    /// The principal the check presented, e.g. `User:backup`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
    /// The SASL mechanism, or `plaintext`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_mode: Option<String>,
}

/// A bounded, redacted failure.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CheckErrorView {
    /// The CamelCase code.
    pub code: String,
    /// The redacted message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// A bounded topic inventory.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicDiscovery {
    /// The object name, which is the id every other route uses.
    pub id: String,
    /// The namespace.
    pub namespace: String,
    /// The Kubernetes UID.
    pub uid: String,
    /// The resourceVersion this projection was read at.
    pub resource_version: String,
    /// When the object was created.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    /// The connection it was bound to.
    pub connection: DiscoveryConnectionView,
    /// The lifecycle state.
    pub state: CheckLifecycle,
    /// The CamelCase reason.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Whether the state is final.
    pub terminal: bool,
    /// When the runner container finished, which is when the facts were true.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<DateTime<Utc>>,
    /// Until when the result counts as fresh.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fresh_until: Option<DateTime<Utc>>,
    /// Whether the result is past its freshness or its binding changed.
    pub stale: bool,
    /// Why, when it is: `expired`, `connectionReplaced`, `connectionChanged`
    /// or `principalChanged`.
    ///
    /// A DIFFERENT VOCABULARY FROM A DIFFERENT PRODUCER, and deliberately
    /// still strings. D2 §5.7's four reasons are computed by THIS service, by
    /// comparing the recorded binding with the connection as it is now; no
    /// controller emits them, so there is no second list for a closed enum to
    /// be pinned against the way [`StaleReasonKind`] is pinned against
    /// `check_contract::StaleReason`. Closing it would be a contract change
    /// with nothing on the other side asking for it.
    pub stale_reasons: Vec<String>,
    /// The cluster id the check read.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_id: Option<String>,
    /// The counts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub counts: Option<DiscoveryCountsView>,
    /// Whether the inventory was cut.
    pub truncated: bool,
    /// Why it was cut.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncation_reason: Option<String>,
    /// Completeness, never better than `unknown` without an attestation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub visibility: Option<VisibilityView>,
    /// What became of the expected topics.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected: Option<ExpectedTopicsView>,
    /// `sha256:<hex>` over the canonical TSV of the stored entries. The topics
    /// page binds its cursor to it, so a page cannot straddle two results.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topics_sha256: Option<String>,
    /// How many stored chunks the result has.
    pub chunk_count: usize,
    /// The failure, when there is one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<CheckErrorView>,
    /// The status conditions, at most 16.
    pub conditions: Vec<ConditionView>,
}

/// `POST .../connections/{name}/topic-discoveries`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CreateTopicDiscoveryRequest {
    /// Whether `__`-prefixed topics are kept. Absent means false.
    #[serde(default)]
    pub include_internal: Option<bool>,
    /// Names to ask about explicitly, at most 500.
    #[serde(default)]
    pub expected_topics: Option<Vec<String>>,
    /// The ceiling on stored entries.
    #[serde(default)]
    pub max_topics: Option<i32>,
    /// The in-Job Kafka budget in seconds.
    #[serde(default)]
    pub timeout_seconds: Option<i32>,
    /// Whether a fresh, identical, succeeded discovery may be returned instead
    /// of starting another. Absent means true.
    #[serde(default)]
    pub reuse_fresh: Option<bool>,
}

/// One row of a topic page.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicEntryView {
    /// The topic name.
    pub name: String,
    /// Its partition count.
    pub partitions: u32,
    /// Whether it is a Kafka internal topic (`__`-prefixed, and only that).
    pub internal: bool,
    /// Whether it was one of the expected names.
    pub expected: bool,
    /// The CamelCase code, when its metadata could not be read.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
}

/// How much of the stored result one page actually looked at.
#[derive(Clone, Copy, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScanView {
    /// Whether the scan reached the end of the result. A sparse `q` returns a
    /// short page with `complete: false` and a cursor rather than reading
    /// every chunk in one request.
    pub complete: bool,
    /// How many chunks this request read, at most 8.
    pub chunks_scanned: u32,
}

/// `GET .../topic-discoveries/{id}/topics`.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicPageResponse {
    /// The request ID.
    pub request_id: String,
    /// The rows on this page, in stored (bytewise name) order.
    pub items: Vec<TopicEntryView>,
    /// Paging. `snapshot` is `<uid>@<topicsSha256>`.
    pub page: Page,
    /// How much of the result was read.
    pub scan: ScanView,
}

/// `GET .../connections/{name}/topic-discoveries?latest=true`.
///
/// TWO SLOTS, NOT ONE. A failed attempt never hides the last successful
/// inventory, and a successful inventory never hides that the newest attempt
/// failed.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveryLatestResponse {
    /// The request ID.
    pub request_id: String,
    /// The newest discovery of any state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_attempt: Option<TopicDiscovery>,
    /// The newest one that produced a result.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_successful: Option<TopicDiscovery>,
}

/// `GET .../connections/{name}/topic-discoveries` — one page.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicDiscoveryList {
    /// The request ID.
    pub request_id: String,
    /// The discoveries on this page.
    pub items: Vec<TopicDiscovery>,
    /// Paging.
    pub page: Page,
}

/// One `TopicDiscovery`.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicDiscoveryResponse {
    /// The request ID.
    pub request_id: String,
    /// On a create: whether this replays an earlier identical request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replayed: Option<bool>,
    /// On a create: whether a fresh identical result was returned instead of
    /// starting another check.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reused: Option<bool>,
    /// The item.
    pub item: TopicDiscovery,
}

/// `POST .../topic-discoveries/{id}:cancel` and
/// `POST .../preflights/{id}:cancel`.
///
/// CANCELLATION IS A WISH, RECORDED. It never deletes archive data, Kafka
/// topics, durable runs or signed evidence; the controller verifies the exact
/// owned Job and UID before stopping anything.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CancelResponse {
    /// The request ID.
    pub request_id: String,
    /// The check's id.
    pub id: String,
    /// Its state after the request.
    pub state: String,
    /// True when the check had already finished, in which case nothing was
    /// written. Repeating a cancel is 200 either way.
    pub already_terminal: bool,
}

// ======================================================================
// Preflights (Preflight, PLAT-03)
// ======================================================================

/// What a preflight is about.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum PreflightOperationDto {
    /// "Back up now" and schedule readiness.
    Backup,
    /// Restore readiness, bound to an exact plan hash.
    Restore,
    /// An explicit destination access test.
    DestinationAccess,
    /// A source connection on its own: does this `KafkaCluster` answer, as
    /// this principal, right now (D2-SOURCECHECK). It names no destination, no
    /// plan and no topic, which is what lets the console's "Test connection"
    /// start one with nothing but the cluster the reader is looking at.
    SourceConnection,
}

/// The aggregate a preflight reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum PreflightState {
    /// Accepted; not observed yet.
    Pending,
    /// Waiting for a check slot.
    Queued,
    /// The check Job exists and has not finished.
    Running,
    /// Every blocking check passed.
    Ready,
    /// A blocking check said no.
    NotReady,
    /// A blocking check could not be decided, or was skipped.
    Unknown,
    /// No result could be produced at all.
    Failed,
    /// Cancelled before a result.
    Cancelled,
}

/// One check's verdict.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum CheckVerdict {
    /// The check passed.
    Ready,
    /// The check failed.
    NotReady,
    /// The check could not be decided.
    Unknown,
    /// The caller asked for it to be skipped. A skipped blocking check keeps
    /// the aggregate `unknown`; it never counts as a pass.
    Skipped,
}

/// Whether a check's verdict gates the operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum CheckGating {
    /// A `notReady` here makes the whole operation `notReady`.
    Blocking,
    /// Reported as a warning; never changes the aggregate.
    Advisory,
    /// Cannot be checked before execution; always `unknown` and always
    /// excluded from the aggregate.
    ExecutionOnly,
}

/// What a check looked at.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CheckScopeView {
    /// The kind.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// The name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The UID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
}

/// One check entry.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CheckEntryView {
    /// The stable check id, e.g. `target.mappedTopics`.
    pub id: String,
    /// Its category.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    /// What it looked at.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<CheckScopeView>,
    /// The verdict.
    pub state: CheckVerdict,
    /// Whether it gates.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gating: Option<CheckGating>,
    /// Who observed the fact.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authority: Option<String>,
    /// The CamelCase code.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// The redacted message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// What to do about it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remedy: Option<String>,
    /// When the fact was observed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<DateTime<Utc>>,
    /// When it stops counting.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
}

/// One object a preflight's binding names.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReferentView {
    /// The kind.
    pub kind: String,
    /// The name.
    pub name: String,
    /// The UID at the time of the check.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    /// The generation at the time of the check.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<i64>,
}

/// Exactly what a result is a result ABOUT.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PreflightBindingView {
    /// The plan hash the check was bound to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_hash: Option<String>,
    /// The digest over every input the verdict depended on.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inputs_digest: Option<String>,
    /// The objects it resolved, at most 16.
    pub referents: Vec<ReferentView>,
}

/// Why a stored readiness verdict no longer describes the caller's inputs.
///
/// CLOSED OVER CORE'S VOCABULARY, PLUS ONE. Six of these are the spellings
/// `logweir_core::check_contract::StaleReason` renders, and
/// `crates/logweir-api/tests/preflights.rs` pins the two lists against each
/// other so neither side can grow a reason the other cannot carry.
/// [`StaleReasonKind::Unverifiable`] is the seventh and is this service's own:
/// core has no spelling for "I could not compare this", and without one the
/// only honest answers left were to invent a reason or to stay silent — and
/// staying silent is `applicable: true` for a verdict nobody checked.
///
/// THESE ARE COMPARISONS, NOT MESSAGES. The API recomputes staleness on every
/// read from `status.binding` against the objects as they are now — which is
/// the division of labour `weirkeeper`'s preflight reconciler documents and
/// `check_contract::inputs_digest` restates. An earlier cut of this module
/// recovered the reasons from the controller's prose instead; that prose is
/// redacted and capped at 512 characters, so a long referent list lost its
/// closing bracket and the parse returned NOTHING — reporting a downgraded
/// verdict as applicable. It failed open, which is the one direction a
/// staleness check may never fail.
///
/// `cancelRequested` is DELIBERATELY ABSENT, and was in the first cut of this
/// enum. The controller never emits it: a cancelled check is `state:
/// cancelled` and carries no result at all, so its verdict is not stale — it
/// is ABSENT, which is a different thing and one `state` and `terminal`
/// already say. Reporting it here invited a console to render "your readiness
/// result is out of date" for a check that never produced one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum StaleReasonKind {
    /// The verdict is past `expiresAt`, or recorded no expiry at all.
    Expired,
    /// The plan the caller is looking at is not the plan the check was bound
    /// to.
    PlanHashChanged,
    /// A named object's UID or generation moved, or it appeared or vanished,
    /// since the verdict was computed — a recreated destination, an edited
    /// access block, a re-created recovery point, a `TrustRoster` edit, or an
    /// `Approval` whose resourceVersion moved when verification landed.
    /// [`StaleReasonView::kind`] and [`StaleReasonView::name`] say which.
    ReferentChanged,
    /// A destination's CA bundle `ConfigMap` now digests differently, so the
    /// trust material the check exercised is not the trust material a run
    /// would use.
    ///
    /// **RESERVED: nothing emits this yet.** It is part of core's vocabulary,
    /// so it is published here, but the recorded `status.binding` does not
    /// carry the CA bundle list and neither this service nor the controller
    /// can therefore compare it. CA-bundle drift surfaces, if at all, through
    /// the destination's own `referentChanged` (its generation moves when its
    /// `caBundle` reference is edited). It becomes reachable when
    /// `status.binding` records `caBundles[]`; until then a console that
    /// branches on it is branching on a value it will never receive, and
    /// `docs/api.md` says so.
    CaBundleChanged,
    /// The installation policy `ConfigMap` digests differently, which can
    /// change the concurrency ceilings, the engine CA rule, the
    /// `ControllerIdentity` allowlist and the visibility attestations the
    /// verdict was computed under.
    PolicyChanged,
    /// The recomputed inputs digest differs and none of the named reasons
    /// explains it. THE CATCH-ALL EXISTS SO "stale" IS NEVER REPORTED WITHOUT
    /// A REASON: a field the five named comparisons do not cover (a backup
    /// readiness request's topic set, for instance) still surfaces.
    ///
    /// **RESERVED on the same grounds as [`StaleReasonKind::CaBundleChanged`]:**
    /// the recorded digest was taken over a wider document than this service
    /// can rebuild, so comparing the two would report a difference that is an
    /// artefact of the narrower recomputation rather than a change in the
    /// world. A gap this service cannot close is
    /// [`StaleReasonKind::Unverifiable`], not a false positive.
    InputsDigestChanged,
    /// **This service could not compare something, so it refuses to call the
    /// verdict applicable.** [`StaleReasonView::basis`] says what.
    ///
    /// THE ONE VARIANT THAT IS NOT CORE'S. Every other reason answers "this
    /// changed"; this one answers "I do not know whether it changed", which is
    /// a different claim and the only honest one when a referent cannot be
    /// read, when the recorded binding is missing, or when an input lives
    /// somewhere this service has no verb for. Silence in those cases is
    /// `applicable: true` for a verdict nobody checked — and a green readiness
    /// badge that was never re-evaluated is exactly the defect PLAT-03.2 is
    /// about.
    Unverifiable,
}

/// One reason, with its subject when it has one.
///
/// STRUCTURED, NOT A PARSED STRING. The controller renders `referentChanged`
/// as `referentChanged:<Kind>/<name>`; splitting that once, here, is the
/// difference between a console that can say "the destination `primary` was
/// replaced" and one that shows a colon-separated token to an operator.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StaleReasonView {
    /// Which reason this is.
    pub reason: StaleReasonKind,
    /// The kind of the object that moved. `referentChanged` and
    /// `unverifiable` only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// That object's name — or, for the `TrustRoster` and `Approval` a binding
    /// names, its UID, because those two are identified in the binding by UID
    /// and a name would add nothing. `referentChanged` and `unverifiable`
    /// only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Why this service could not compare something. `unverifiable` only, and
    /// always present there: a refusal to answer that does not say what it
    /// could not check is not an answer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub basis: Option<String>,
}

impl StaleReasonView {
    /// A reason with no subject.
    #[must_use]
    pub const fn plain(reason: StaleReasonKind) -> Self {
        Self {
            reason,
            kind: None,
            name: None,
            basis: None,
        }
    }

    /// `unverifiable`, with the cause and the object it is about.
    #[must_use]
    pub fn unverifiable(kind: Option<String>, name: Option<String>, basis: &str) -> Self {
        Self {
            reason: StaleReasonKind::Unverifiable,
            kind,
            name,
            basis: Some(basis.to_string()),
        }
    }
}

/// A check that cannot be answered before the run.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionOnlyView {
    /// The check id.
    pub id: String,
    /// Why it is only knowable at execution time.
    pub note: String,
}

/// The backup half of a preflight request.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct BackupPreflightRequest {
    /// The source connection, in this namespace.
    pub source_connection: String,
    /// The destination, in this namespace.
    #[serde(default)]
    pub destination: Option<String>,
    /// Or a legacy inline archive.
    #[serde(default)]
    pub legacy_archive: Option<ArchiveRequest>,
    /// Named topics, 1 to 1000.
    pub topics: Vec<String>,
    /// The schedule this readiness is about, for context.
    #[serde(default)]
    pub schedule: Option<String>,
}

/// The recovery point a restore preflight is about.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RecoveryPointRequest {
    /// The `Backup` that produced it.
    pub backup_name: String,
    /// Its UID. A deleted and recreated point is a different point.
    #[serde(default)]
    pub backup_uid: Option<String>,
}

/// The restore half of a preflight request.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RestorePreflightRequest {
    /// The exact draft plan bytes, at most 256 KiB. FORWARDED VERBATIM: the
    /// service checks `planHash` against the SHA-256 of exactly these bytes
    /// and stores them unchanged. It never parses or re-emits the plan.
    #[serde(default)]
    pub plan_bytes: Option<String>,
    /// `sha256:<lowercase hex>` of `planBytes`.
    #[serde(default)]
    pub plan_hash: Option<String>,
    /// Or an existing `Restore` to check.
    #[serde(default)]
    pub restore_name: Option<String>,
    /// The target connection; required with `planBytes`.
    #[serde(default)]
    pub target: Option<String>,
    /// The destination the archive is read from.
    #[serde(default)]
    pub source_destination: Option<String>,
    /// The destination evidence is read from. Set together with
    /// `sourceDestination`.
    #[serde(default)]
    pub evidence_destination: Option<String>,
    /// Or a legacy inline source archive.
    #[serde(default)]
    pub legacy_source_archive: Option<ArchiveRequest>,
    /// The recovery point.
    #[serde(default)]
    pub recovery_point: Option<RecoveryPointRequest>,
}

/// The destination-access half of a preflight request.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DestinationAccessPreflightRequest {
    /// The destination to exercise.
    pub destination: String,
    /// The roles to exercise, 1 to 4.
    pub roles: Vec<DestinationRoleDto>,
}

/// The source-connection half of a preflight request.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SourceConnectionPreflightRequest {
    /// The `KafkaCluster` to dial, in this namespace. REQUIRED: a connectivity
    /// check with no connection is not a smaller check, it is no check, and
    /// the absence is refused as `422 connectionRef required` rather than
    /// defaulted to anything.
    pub connection_ref: String,
}

/// `POST .../preflights`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CreatePreflightRequest {
    /// What this is about. Exactly the matching block below may be set.
    pub operation: PreflightOperationDto,
    /// The backup block.
    #[serde(default)]
    pub backup: Option<BackupPreflightRequest>,
    /// The restore block.
    #[serde(default)]
    pub restore: Option<RestorePreflightRequest>,
    /// The destination-access block.
    #[serde(default)]
    pub destination_access: Option<DestinationAccessPreflightRequest>,
    /// The source-connection block.
    #[serde(default)]
    pub source_connection: Option<SourceConnectionPreflightRequest>,
    /// Blocking checks to skip, at most 32. A skipped blocking check keeps the
    /// aggregate `unknown`.
    #[serde(default)]
    pub skip_checks: Option<Vec<String>>,
    /// The check budget in seconds, 30 to 600.
    #[serde(default)]
    pub timeout_seconds: Option<i32>,
}

/// An operation-readiness result.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Preflight {
    /// The object name, which is the id every other route uses.
    pub id: String,
    /// The namespace.
    pub namespace: String,
    /// The Kubernetes UID.
    pub uid: String,
    /// The resourceVersion this projection was read at.
    pub resource_version: String,
    /// When the object was created.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    /// What it is about.
    pub operation: PreflightOperationDto,
    /// The lifecycle, and — once complete — the aggregate.
    pub state: PreflightState,
    /// The CamelCase reason.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Whether the state is final.
    pub terminal: bool,
    /// Exactly what the result is about.
    pub binding: PreflightBindingView,
    /// Whether the result still describes the caller's current inputs.
    ///
    /// RECOMPUTED PER READ. A result is applicable only when it completed, has
    /// not expired, and its binding still matches: edit the plan, the target,
    /// the destination or the recovery point and this goes false.
    pub applicable: bool,
    /// Whether it is out of date.
    pub stale: bool,
    /// Why, when it is. Closed over [`StaleReasonKind`].
    pub stale_reasons: Vec<StaleReasonView>,
    /// What the staleness comparison actually covered, in the order it ran.
    ///
    /// AN EMPTY `staleReasons` MEANS "I COMPARED THESE AND THEY MATCH", and
    /// this is the list of "these". Without it a console cannot tell a verdict
    /// that was re-checked against live objects from one where the check was
    /// skipped, and those two look identical in every other field.
    pub stale_basis: Vec<String>,
    /// When the facts were observed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<DateTime<Utc>>,
    /// The minimum expiry over the non-skipped checks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
    /// The blocking checks.
    pub checks: Vec<CheckEntryView>,
    /// The advisory checks that said no.
    pub warnings: Vec<CheckEntryView>,
    /// The checks that can only be answered while the run executes. They never
    /// affect the aggregate, and "ready" never means they passed.
    pub execution_only: Vec<ExecutionOnlyView>,
    /// Whether a details document exists for the details route.
    pub details_available: bool,
    /// The status conditions, at most 16.
    pub conditions: Vec<ConditionView>,
}

/// One `Preflight`.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PreflightResponse {
    /// The request ID.
    pub request_id: String,
    /// On a create: whether this replays an earlier identical request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replayed: Option<bool>,
    /// The item.
    pub item: Preflight,
}

/// One entry of a preflight's detail document.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DetailEntryView {
    /// The check id the entry belongs to, when the producer named one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub check: Option<String>,
    /// The entry, verbatim. The producer's own bounded, redacted JSON object.
    pub entry: serde_json::Value,
}

/// `GET .../preflights/{id}/details`.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DetailPageResponse {
    /// The request ID.
    pub request_id: String,
    /// The entries on this page, in stored order.
    pub items: Vec<DetailEntryView>,
    /// Paging. `snapshot` is the details document's own digest.
    pub page: Page,
}

// ======================================================================
// Check operations (the normalized status of a transient check)
// ======================================================================

/// The two transient check kinds with a status route.
///
/// SEPARATE FROM [`OperationKind`] ON PURPOSE. A check has no archive result,
/// no signed evidence and no verification verdict, so folding it into
/// [`Operation`] would mean publishing three fields that are meaningless for
/// it and inviting a console to render "verification: pending" for a topic
/// list.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum CheckOperationKind {
    /// A `TopicDiscovery`.
    Discovery,
    /// A `Preflight`.
    Preflight,
}

/// The normalized status of one transient check.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CheckOperation {
    /// `discovery` or `preflight`.
    pub kind: CheckOperationKind,
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
    pub state: CheckLifecycle,
    /// The CamelCase reason.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_reason: Option<String>,
    /// The bounded message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Whether the state is final.
    pub terminal: bool,
    /// Whether this check may still be cancelled.
    pub cancellable: bool,
    /// When the facts were observed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<DateTime<Utc>>,
    /// The status conditions, at most 16.
    pub conditions: Vec<ConditionView>,
}

/// `GET .../operations/discovery/{name}` and `.../operations/preflight/{name}`.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CheckOperationResponse {
    /// The request ID.
    pub request_id: String,
    /// The normalized check.
    pub item: CheckOperation,
}

// ======================================================================
// Cadence: presets, previews and the editable schedule policy (D1 §4, §5)
// ======================================================================

/// The DST marker on one firing (D1 §4.3/§4.4).
///
/// THE SPELLING IS THE CONTROLLER'S, NOT THIS CRATE'S. `BackupSchedule.status.
/// nextRuns[].adjustment` carries these exact PascalCase strings, and a console
/// renders the preview and the saved status with one branch. A marker this
/// build does not recognise is OMITTED from a projection rather than echoed:
/// an unknown word is not a fact about a time zone.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum CadenceAdjustment {
    /// The matched local time does not exist (spring forward); this instant is
    /// the end of the gap.
    NonexistentLocalTimeShifted,
    /// The matched local time happens twice; this is the FIRST occurrence.
    RepeatedLocalTimeFirst,
    /// The matched local time happens twice; this is the SECOND occurrence.
    RepeatedLocalTimeSecond,
}

impl CadenceAdjustment {
    /// The stored spelling, or `None` for a word this build does not know.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "NonexistentLocalTimeShifted" => Some(Self::NonexistentLocalTimeShifted),
            "RepeatedLocalTimeFirst" => Some(Self::RepeatedLocalTimeFirst),
            "RepeatedLocalTimeSecond" => Some(Self::RepeatedLocalTimeSecond),
            _ => None,
        }
    }
}

/// One upcoming firing: the UTC instant, the local wall time with its offset,
/// and what the local clock did to reach it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NextRunView {
    /// The UTC instant. This, and only this, is the slot's identity.
    pub at: DateTime<Utc>,
    /// The same instant in the schedule's zone, offset included —
    /// `2026-10-25T02:30:00+02:00`. The offset is what distinguishes the two
    /// occurrences of a repeated hour.
    pub local_time: String,
    /// The marker, omitted when the instant needed no adjustment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adjustment: Option<CadenceAdjustment>,
}

/// One of the five preset cadences (D1 §4.2), with its parameters.
///
/// A PRESET IS NEVER STORED. `spec.schedule` is the single source of truth;
/// this is the catalogue entry an expression IS, so a form can round-trip it.
/// `ui/tests/fixtures/cadence-presets.json` is the same catalogue as data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum CadencePreset {
    /// Every hour at `minute`.
    #[serde(rename_all = "camelCase")]
    Hourly {
        /// 0–59.
        minute: u32,
    },
    /// At local wall-clock hours divisible by `n`, at `minute`.
    #[serde(rename_all = "camelCase")]
    EveryNHours {
        /// 2, 3, 4, 6, 8 or 12.
        n: u32,
        /// 0–59.
        minute: u32,
    },
    /// Every day at `hour`:`minute`.
    #[serde(rename_all = "camelCase")]
    Daily {
        /// 0–23.
        hour: u32,
        /// 0–59.
        minute: u32,
    },
    /// Every week on `dayOfWeek` at `hour`:`minute`.
    #[serde(rename_all = "camelCase")]
    Weekly {
        /// 0–6, 0 = Sunday.
        day_of_week: u32,
        /// 0–23.
        hour: u32,
        /// 0–59.
        minute: u32,
    },
    /// Every month on `dayOfMonth` at `hour`:`minute`.
    #[serde(rename_all = "camelCase")]
    Monthly {
        /// 1–28.
        day_of_month: u32,
        /// 0–23.
        hour: u32,
        /// 0–59.
        minute: u32,
    },
}

/// `GET /api/v1/cadence-previews`.
///
/// A DRAFT PREVIEW, COMPUTED IN RUST AND NOWHERE ELSE (D1 §4.4). The browser
/// renders what this returns and never evaluates cron; the saved schedule's own
/// previews are `status.nextRuns`, written by the controller from the same
/// module.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CadencePreviewResponse {
    /// The request ID.
    pub request_id: String,
    /// The canonical five-field expression that was evaluated. With `preset=`
    /// it is what the preset compiled to, so the form can save exactly this.
    pub schedule: String,
    /// The preset this expression IS, or absent for "Advanced cron".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preset: Option<CadencePreset>,
    /// The EFFECTIVE zone: `UTC` when `timeZone` was not sent, so a reader
    /// never has to know the default.
    pub time_zone: String,
    /// Which time-zone database resolved it, e.g. `chrono-tz 0.10.4`. Rule
    /// updates ship with a release, so two releases can disagree about a slot
    /// and this says which one answered.
    pub tzdb: String,
    /// The instant the walk started strictly after — the server's now unless
    /// `after` was sent.
    pub after: DateTime<Utc>,
    /// Up to `count` firings, ascending. SHORTER IS A REAL ANSWER: an
    /// expression such as `0 0 29 2 *` runs out of firings, and an empty list
    /// means "it does not fire again", not "the server gave up".
    pub runs: Vec<NextRunView>,
}

/// Topics to leave out of a dynamic selection. Literal names and literal
/// prefixes; never a pattern language.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TopicExclusions {
    /// Exact names, at most 1000.
    #[serde(default)]
    pub topics: Option<Vec<String>>,
    /// Literal prefixes, at most 32. `orders-` excludes `orders-eu`;
    /// `orders*` is not writable at all.
    #[serde(default)]
    pub prefixes: Option<Vec<String>>,
}

/// What a dynamic run does when discovery cannot prove it saw everything.
/// REQUIRED, WITH NO DEFAULT: both possible defaults are wrong in a way the
/// operator would not notice (D1 §0.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub enum IncompleteDiscoveryPolicy {
    /// Fail the run.
    Refuse,
    /// Back up what was visible and label the run `VisibleUserTopicsOnly`.
    BackUpVisibleTopics,
}

/// Dynamic selection: every user topic the run's principal can see, minus the
/// exclusions.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AllUserTopics {
    /// What to leave out.
    #[serde(default)]
    pub exclude: Option<TopicExclusions>,
    /// Required.
    pub incomplete_discovery: IncompleteDiscoveryPolicy,
}

/// How a run picks its topics: a named allowlist, or dynamic resolution.
///
/// THE THIRD SHAPE IS NOT REFUSED HERE, AND THAT IS DELIBERATE. Sending a
/// non-empty `topics` together with `allUserTopics` is refused by the CRD's own
/// CEL (D1 §5.2 R2 / the `Backup` rule), and this API lets the API server say
/// so rather than keeping a second copy of the rule that can drift from it. An
/// EMPTY selection — neither a name nor a block — is refused here, before any
/// write, because no rule catches it and a run with nothing to do is not a run.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TopicSelectionRequest {
    /// Named topics; patterns are refused. Empty for dynamic selection.
    #[serde(default)]
    pub topics: Vec<String>,
    /// Dynamic selection.
    #[serde(default)]
    pub all_user_topics: Option<AllUserTopics>,
}

/// Whether a slot past its starting deadline may still run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub enum CatchUpPolicy {
    /// Count it in `status.missedSlots` and move on — today's behaviour, and
    /// what an absent field means.
    None,
    /// The LATEST due slot, and only that one, may still run. At most one
    /// catch-up run, ever.
    Latest,
}

/// Retries for a failed slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RetryPolicy {
    /// 0 to 3. Each retry is a NEW `Backup` with a new execution id: a failed
    /// attempt's partial archive is never appended to.
    pub max_retries: i32,
    /// Seconds between a failed attempt finishing and its retry becoming
    /// admissible, 60 to 21600. Absent means 300.
    #[serde(default)]
    pub delay_seconds: Option<i64>,
}

/// `PUT /api/v1/namespaces/{ns}/schedules/{name}` — the editable future policy
/// (D1 §5.1, §5.6).
///
/// THE WHOLE POLICY, NOT A DIFF. Every mutable field is sent; a field omitted
/// is REMOVED, which is what makes "absent means the documented default"
/// reachable from a form. `spec.sourceRef` is not on this DTO at all: the
/// identity of the protected cluster is immutable, and a request naming it is
/// `422 validation_failed` with `sourceRef: field_immutable` before anything is
/// read or written.
///
/// IT CHANGES THE FUTURE AND NOTHING ELSE. A `Backup` already created keeps its
/// copied policy and its frozen inputs; an edit during a run does not reach the
/// run (D1 §5.5).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct UpdateSchedulePolicyRequest {
    /// The `metadata.generation` the client last read. A different current
    /// generation is `412 precondition_failed`: somebody else edited the
    /// policy between the read and this write.
    pub expected_generation: i64,
    /// A five-field cron expression or `@hourly`/`@daily`/`@weekly`, parsed by
    /// the controller's own parser.
    pub schedule: String,
    /// An IANA zone name. Absent means UTC and reproduces today's slots
    /// exactly.
    #[serde(default)]
    pub time_zone: Option<String>,
    /// PRESENT ONLY TO BE REFUSED, and present so that the refusal is a
    /// FIELD ERROR rather than "unknown field". A console that reads a
    /// schedule and writes it back sends every field it read; `sourceRef` is
    /// one of them, and `sourceRef: field_immutable` tells the person what to
    /// do (create a new schedule) where `malformed_request` would not.
    #[serde(default)]
    pub source_ref: Option<NameRef>,
    /// The topic selection.
    pub topic_selection: TopicSelectionRequest,
    /// Where backups are written. Exactly one of `archive` or
    /// `destinationRef`.
    #[serde(default)]
    pub archive: Option<ArchiveRequest>,
    /// A saved `BackupDestination` in this namespace. Exactly one of `archive`
    /// or `destinationRef`; the sentinel `archive.url` the CRD requires is
    /// built here and is never accepted from a body.
    #[serde(default)]
    pub destination_ref: Option<NameRef>,
    /// `Forbid` (default) or `Allow`.
    #[serde(default)]
    pub concurrency_policy: Option<ConcurrencyPolicy>,
    /// How long after its instant a slot may still start, 60 to 604800.
    /// Absent means 3600.
    #[serde(default)]
    pub starting_deadline_seconds: Option<i64>,
    /// Absent means `None`.
    #[serde(default)]
    pub catch_up_policy: Option<CatchUpPolicy>,
    /// Absent means no retries.
    #[serde(default)]
    pub retry: Option<RetryPolicy>,
    /// The run's `activeDeadlineSeconds`, 60 to 86400. Absent means 3600.
    #[serde(default)]
    pub active_deadline_seconds: Option<i64>,
    /// Retention reporting. Logweir deletes nothing.
    #[serde(default)]
    pub retention: Option<RetentionRequest>,
    /// Whether the schedule is suspended. Suspension is part of the policy
    /// here as well as its own command route; flipping it changes
    /// `metadata.generation` and leaves `runPolicySha256` alone.
    pub suspended: bool,
}

/// The revision a schedule's controller last evaluated (D1 §4.8).
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchedulePolicyView {
    /// The `metadata.generation` this block was computed from.
    pub generation: i64,
    /// `sha256:<lowercase hex>` over the run policy of that generation.
    pub run_policy_sha256: String,
    /// The EFFECTIVE zone, `UTC` when `spec.timeZone` is absent.
    pub time_zone: String,
    /// Which time-zone database resolved it.
    pub tzdb: String,
    /// When this revision was first observed.
    pub effective_since: DateTime<Utc>,
    /// When this status last MOVED — **not** a liveness probe.
    ///
    /// The controller re-examines every schedule every 30 s and writes nothing
    /// when the computed status equals the stored one, so this instant
    /// standing still means "nothing has changed", not "nobody is watching". A
    /// console must not compare it with the requeue interval. The staleness
    /// signal is `nextRuns[0].at` in the past (D1 §4.9 as amended).
    pub evaluated_at: DateTime<Utc>,
}

/// One schedule-created run that is not terminal (D1 §4.8).
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActiveRunView {
    /// The `Backup`'s name.
    pub name: String,
    /// `Scheduled`, `CatchUp` or `Retry`. A manual run is never here: it is
    /// not counted by, and never blocked by, `concurrencyPolicy`.
    pub kind: String,
    /// 0 for a scheduled or catch-up run, 1 to 3 for a retry.
    pub attempt: i32,
}

// ======================================================================
// Manual backups (D1 §8.2)
// ======================================================================

/// The schedule a manual run copies its policy from.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct BackupScheduleRefRequest {
    /// The `BackupSchedule` name, in this namespace.
    pub name: String,
    /// The revision the caller believes it is running. A different current
    /// generation is `409 policy_changed`, with the current generation and
    /// digest in the body.
    #[serde(default)]
    pub expected_generation: Option<i64>,
}

/// The readiness verdict a person clicked past.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum AcknowledgedReadiness {
    /// A preflight said a prerequisite was not met and the person ran anyway.
    NotReady,
    /// Readiness could not be established.
    Unknown,
}

/// "Run anyway", recorded and never authoritative (D1 §8.4).
///
/// THE API AND THE CONTROLLER NEVER GATE ON READINESS. Execution-time guards
/// stay authoritative and the direct `kubectl create -f` path exists
/// regardless; this records that a person was shown a `notReady` or `unknown`
/// verdict and went ahead. It is an annotation on the run, not a permission.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReadinessAcknowledgementRequest {
    /// The `Preflight` whose verdict was shown, in this namespace.
    pub preflight: String,
    /// What it said.
    pub state: AcknowledgedReadiness,
}

/// `POST /api/v1/namespaces/{ns}/backups` — "Back up now" and "Run first
/// backup now" (D1 §8.2).
///
/// TWO BODIES, ONE CR PATH. With `scheduleRef` the API copies the schedule's
/// current revision — selection, archive, deadline — and records
/// `{uid, generation, runPolicySha256}`; a policy field in the body beside it
/// is `422`, because a "manual run of this schedule" that quietly ran a
/// different policy would be the wrong thing recorded in the receipt. Without
/// it the body IS the policy, for a cluster with no schedule yet.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CreateBackupRequest {
    /// Body A: copy this schedule's current revision.
    #[serde(default)]
    pub schedule_ref: Option<BackupScheduleRefRequest>,
    /// Body B: the source connection, in this namespace.
    #[serde(default)]
    pub source_ref: Option<NameRef>,
    /// Body B: the topic selection.
    #[serde(default)]
    pub topic_selection: Option<TopicSelectionRequest>,
    /// Body B: an inline archive location.
    #[serde(default)]
    pub legacy_archive: Option<ArchiveRequest>,
    /// Body B: a saved `BackupDestination` instead of an inline archive.
    #[serde(default)]
    pub destination_ref: Option<NameRef>,
    /// Body B: the run's `activeDeadlineSeconds`, 60 to 86400. Absent means
    /// 3600.
    #[serde(default)]
    pub deadline_seconds: Option<i64>,
    /// Either body: "Run anyway" after a `notReady` or `unknown` verdict.
    #[serde(default)]
    pub readiness_acknowledgement: Option<ReadinessAcknowledgementRequest>,
}

/// The schedule a manual run was taken from, as it stood when the run was
/// created.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleContextView {
    /// The schedule's name.
    pub name: String,
    /// Its UID.
    pub uid: String,
    /// The generation whose policy this run copied.
    pub generation: i64,
    /// The digest of that policy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_policy_sha256: Option<String>,
    /// Whether the schedule is suspended. A manual run is ALLOWED while it is,
    /// and does not resume it; the console says so rather than hiding it.
    pub suspended: bool,
    /// The schedule-created runs that were not terminal. A manual run is
    /// neither counted by nor blocked by `concurrencyPolicy`; this is a
    /// non-blocking notice.
    pub active_runs: Vec<ActiveRunView>,
}

/// `POST /api/v1/namespaces/{ns}/backups`: the created run, and the schedule
/// it came from.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ManualBackupResponse {
    /// The request ID.
    pub request_id: String,
    /// Whether this response replays an earlier identical request (HTTP 200)
    /// rather than creating (HTTP 201).
    pub replayed: bool,
    /// The created run.
    pub item: Backup,
    /// The schedule, when the run was taken from one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule: Option<ScheduleContextView>,
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
