//! `ProtectionPolicy` — the objective a set of schedules is meant to meet, and
//! who hears about it when they do not.
//!
//! # Why it is a kind, and why its spec is the one that is NOT sealed
//!
//! An objective spans several schedules, outlives any one of them — schedules
//! are immutable and are replaced by drain-and-retain (`docs/kubernetes.md`
//! §9) — and is about a source and a topic set rather than about storage, so
//! it belongs on neither a `BackupSchedule` nor a `BackupDestination` (ADR 0008
//! Amendment G).
//!
//! It is **evaluation policy, never an execution input**. Nothing here is
//! frozen into a run, no run reads it, and editing it cannot change a recorded
//! result — it can only change what Logweir SAYS about results that already
//! exist. That is the whole reason this is the one kind in the group with no
//! CEL seal on `.spec`: there is nothing here whose mutation could rewrite
//! history, and an objective an operator cannot tighten is an objective nobody
//! adopts.
//!
//! # No credential value, only references
//!
//! Every notification channel names a `secretKeyRef`. The routing key, the
//! webhook URL and the Slack URL are all credentials — a Slack webhook URL is
//! a bearer token with a hostname on the front — so none of them appears as a
//! value here. They are projected into the delivery Job and nowhere else. This
//! closes decision O17 for this path; the drill spec's inline `notifications`
//! block is unchanged and stays documented as plaintext.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{ArchiveRef, Condition, LocalRef, SpecRule, Time};

/// A DNS-1123 subdomain — a Secret name.
pub const OBJECT_NAME_PATTERN: &str = super::kafka_cluster::OBJECT_NAME_PATTERN;
/// The API server's own rule for a `data` key.
pub const DATA_KEY_PATTERN: &str = super::kafka_cluster::DATA_KEY_PATTERN;
/// The Kafka topic-name grammar.
pub const TOPIC_NAME_PATTERN: &str = super::topic_discovery::TOPIC_NAME_PATTERN;

/// H1 — protection names a saved destination or an inline archive, not both
/// and not neither.
pub const H1_DESTINATION_XOR_RULE: &str = "has(self.destinationRef) != has(self.legacyArchive)";
/// H1's message.
pub const H1_DESTINATION_XOR_MESSAGE: &str =
    "spec.protects sets exactly one of destinationRef or legacyArchive";

/// H2 — a notification route that names no channel delivers nothing, and an
/// alert nobody receives is worse than no alert configured: it looks
/// configured.
pub const H2_ROUTE_HAS_A_CHANNEL_RULE: &str =
    "has(self.pagerDuty) || has(self.webhook) || has(self.slack)";
/// H2's message.
pub const H2_ROUTE_HAS_A_CHANNEL_MESSAGE: &str =
    "a notification route names at least one of pagerDuty, webhook or slack";

/// `ProtectionPolicy` carries **no rule on `.spec`**, and that is the
/// decision.
///
/// See the module header: this spec is evaluation policy, so there is no
/// recorded result an edit could rewrite and nothing to seal.
/// `exactly_one_kind_declares_no_spec_rule` asserts that this is the only such
/// kind, so a second one is a red test rather than an omission.
pub const SPEC_RULES: [SpecRule; 0] = [];

/// The rules attached below `.spec`.
pub const NESTED_RULES: [(&[&str], &str, &str); 2] = [
    (
        &["protects"],
        H1_DESTINATION_XOR_RULE,
        H1_DESTINATION_XOR_MESSAGE,
    ),
    (
        &["notifications", "routes", "[]"],
        H2_ROUTE_HAS_A_CHANNEL_RULE,
        H2_ROUTE_HAS_A_CHANNEL_MESSAGE,
    ),
];

/// The default evaluation cadence, in seconds.
pub const DEFAULT_EVALUATION_INTERVAL_SECONDS: i32 = 300;
/// The default consecutive-failure tolerance.
pub const DEFAULT_MAX_CONSECUTIVE_FAILED_RUNS: i32 = 1;
/// The default re-notify interval, in seconds.
pub const DEFAULT_RENOTIFY_AFTER_SECONDS: i32 = 86_400;

fn default_evaluation_interval_seconds() -> i32 {
    DEFAULT_EVALUATION_INTERVAL_SECONDS
}
fn default_max_consecutive_failed_runs() -> i32 {
    DEFAULT_MAX_CONSECUTIVE_FAILED_RUNS
}
fn default_true() -> bool {
    true
}
fn default_renotify_after_seconds() -> i32 {
    DEFAULT_RENOTIFY_AFTER_SECONDS
}

/// A key inside a Secret in this namespace. **A reference, never a value.**
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SecretKeyRef {
    /// The Secret name, in this namespace.
    #[schemars(regex(path = "OBJECT_NAME_PATTERN"), length(min = 1, max = 253))]
    pub name: String,
    /// The data key.
    #[schemars(regex(path = "DATA_KEY_PATTERN"), length(min = 1, max = 253))]
    pub key: String,
}

/// What this policy protects.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProtectedSubject {
    /// The `KafkaCluster` whose data this is about.
    pub source_ref: LocalRef,
    /// The topics. Absent means "the topics of the newest matching point",
    /// which is the honest reading when a schedule selects dynamically.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 256), inner(regex(path = "TOPIC_NAME_PATTERN")))]
    pub topics: Option<Vec<String>>,
    /// The schedules whose runs count towards this objective.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 16))]
    pub schedule_refs: Option<Vec<LocalRef>>,
    /// The saved destination the points live in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_ref: Option<LocalRef>,
    /// An inline archive, for an installation with no saved destinations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_archive: Option<ArchiveRef>,
    /// The `RecoveryCatalog` that answers "is the point still there?".
    /// Absent means availability is judged from Kubernetes status alone, and
    /// `status.availabilityBasis` says so.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_ref: Option<LocalRef>,
}

/// What "protected" means for this subject.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Objectives {
    /// How old the newest recovery point may be. Measured from CAPTURE START,
    /// not from when a run finished: a four-hour backup that started at 02:00
    /// protects you to 02:00, and dating it 06:00 would overstate the
    /// protection by the length of the run.
    #[schemars(range(min = 300, max = 31536000))]
    pub max_recovery_point_age_seconds: i32,
    /// How many consecutive failed runs are tolerated before the objective is
    /// at risk.
    #[serde(default = "default_max_consecutive_failed_runs")]
    #[schemars(range(min = 0, max = 100))]
    pub max_consecutive_failed_runs: i32,
    /// Whether a point with unverified evidence counts as protection. Default
    /// `true`, and the default is the safe direction.
    #[serde(default = "default_true")]
    pub require_verified_evidence: bool,
    /// Whether the catalog must say the point is still in storage.
    #[serde(default = "default_true")]
    pub require_catalog_availability: bool,
    /// How old the newest successful rehearsal may be (PLAT-14.3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 3600, max = 31536000))]
    pub max_rehearsal_age_seconds: Option<i32>,
}

/// PagerDuty Events v2.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PagerDutyChannel {
    /// The routing key, by reference.
    pub routing_key_secret_ref: SecretKeyRef,
    /// The Events endpoint, when it is not the global one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 2048))]
    pub endpoint: Option<String>,
}

/// A generic webhook. The URL is a credential, so it is a reference.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WebhookChannel {
    /// The URL, by reference.
    pub url_secret_ref: SecretKeyRef,
}

/// A Slack incoming webhook. The URL is a bearer token with a hostname on the
/// front, so it is a reference too.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SlackChannel {
    /// The webhook URL, by reference.
    pub webhook_url_secret_ref: SecretKeyRef,
}

/// One destination for alerts.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NotificationRoute {
    /// What to call it in status and in logs.
    #[schemars(length(min = 1, max = 63))]
    pub name: String,
    /// PagerDuty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pager_duty: Option<PagerDutyChannel>,
    /// A webhook.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook: Option<WebhookChannel>,
    /// Slack.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slack: Option<SlackChannel>,
}

/// What an alert is about.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
pub enum AlertKind {
    /// A run failed.
    BackupFailure,
    /// The newest point is older than the objective allows.
    Staleness,
    /// The catalog cannot find the point in storage.
    ArchiveUnavailable,
    /// A rehearsal failed or was skipped for too long.
    RehearsalFailure,
    /// A recovery finished — the incident-facing one, and the only alert that
    /// is good news.
    RecoveryCompleted,
}

/// Where alerts go.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Notifications {
    /// The routes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 4))]
    pub routes: Option<Vec<NotificationRoute>>,
    /// Which alerts to send. Absent means all of them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 5))]
    pub kinds: Option<Vec<AlertKind>>,
    /// Whether a resolution is delivered too.
    #[serde(default = "default_true")]
    pub send_resolved: bool,
    /// How long before an open alert is re-sent. `0` disables re-notify.
    #[serde(default = "default_renotify_after_seconds")]
    #[schemars(range(min = 0, max = 604800))]
    pub renotify_after_seconds: i32,
}

/// `ProtectionPolicy.spec`.
#[derive(kube::CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "logweir.dev",
    version = "v1alpha1",
    kind = "ProtectionPolicy",
    doc = "The recovery objective a set of schedules is meant to meet, and who hears about it when they do not (ADR 0008 Amendment G). The spec is MUTABLE and carries no CEL seal: it is evaluation policy, never an execution input, so editing it changes what Logweir says about results and never the results themselves. Notification channels are secretKeyRef references; no credential value appears here.",
    plural = "protectionpolicies",
    singular = "protectionpolicy",
    namespaced,
    status = "ProtectionPolicyStatus",
    printcolumn = r#"{"name":"SOURCE","type":"string","jsonPath":".spec.protects.sourceRef.name"}"#,
    printcolumn = r#"{"name":"HEALTH","type":"string","jsonPath":".status.health"}"#,
    printcolumn = r#"{"name":"POINT-AGE","type":"integer","jsonPath":".status.lastAvailablePoint.ageSeconds"}"#,
    printcolumn = r#"{"name":"BASIS","type":"string","jsonPath":".status.availabilityBasis","description":"KubernetesStatus, Catalog or CatalogStale"}"#,
    printcolumn = r#"{"name":"AGE","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct ProtectionPolicySpec {
    /// What this policy is about.
    pub protects: ProtectedSubject,
    /// What "protected" means.
    pub objectives: Objectives,
    /// Where alerts go. Absent means the verdict is reported in status and in
    /// the console and delivered nowhere — which is a real choice, not a
    /// broken configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notifications: Option<Notifications>,
    /// How often to evaluate.
    #[serde(default = "default_evaluation_interval_seconds")]
    #[schemars(range(min = 60, max = 3600))]
    pub evaluation_interval_seconds: i32,
}

/// The newest point this policy can actually recover from.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AvailablePoint {
    /// The durable point identity, when the catalog supplied one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub point_id: Option<String>,
    /// The `Backup` object, while one still exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_ref: Option<LocalRef>,
    /// **Capture start** — what the objective is measured from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_point_at: Option<Time>,
    /// The newest record the point contains, labelled separately so nobody
    /// confuses the two.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub newest_record_at: Option<Time>,
    /// How old the point was at `evaluatedAt`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub age_seconds: Option<i64>,
    /// `Valid`, `ValidHistorical`, `Invalid`, `Untrusted` or `NotAttempted`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence: Option<String>,
    /// The topics it covers, bounded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 64))]
    pub topics: Option<Vec<String>>,
    /// Whether `topics` was cut short.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topics_truncated: Option<bool>,
}

/// The most recent run, whatever became of it.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LastAttempt {
    /// The `Backup`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_ref: Option<LocalRef>,
    /// Its phase.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// Its reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// When.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<Time>,
}

/// What a schedule looks like from here.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleSummary {
    /// The schedule's name.
    pub name: String,
    /// Whether it is suspended — the commonest cause of staleness, and the one
    /// nobody thinks of.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suspended: Option<bool>,
    /// Its `Ready` status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready: Option<String>,
    /// When it fires next.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_fire_time: Option<Time>,
    /// The last slot it missed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_missed_slot: Option<String>,
}

/// Missed-slot facts.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MissedSummary {
    /// The last slot that did not run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_missed_slot: Option<String>,
    /// Seconds since the last successful fire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since_last_fire: Option<i64>,
}

/// The rehearsal side of protection.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RehearsalSummary {
    /// When a rehearsal last passed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_succeeded_at: Option<Time>,
    /// The `Restore` it ran as.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_restore_ref: Option<LocalRef>,
    /// When one last failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failed_at: Option<Time>,
    /// Why.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reason: Option<String>,
}

/// How an alert was delivered, or why it was not.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AlertDelivery {
    /// `Pending`, `Delivered`, `Failed` or `Suppressed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    /// How many times delivery was attempted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempts: Option<i64>,
    /// When it was last attempted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_attempt_at: Option<Time>,
    /// The delivery Job.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_ref: Option<LocalRef>,
    /// The last error, redacted and bounded. Never a routing key, never a URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 512))]
    pub last_error: Option<String>,
}

/// One entry of the deduplication ledger.
///
/// A LEDGER AND NOT A LOG. `transition` increments on open, on resolve and on
/// each re-notify; `notifiedTransition` records the last one a delivery Job was
/// created for. The pair is what makes delivery exactly-once-per-transition
/// across controller restarts, instead of "once per reconcile", which is how an
/// alerting integration gets muted by its own operator.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AlertEntry {
    /// The dedup key — `logweir-protection-<policyUID>-<kind>`.
    pub key: String,
    /// What it is about.
    pub kind: AlertKind,
    /// `Open` or `Resolved`.
    pub state: String,
    /// When it opened.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opened_at: Option<Time>,
    /// When it resolved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_at: Option<Time>,
    /// The transition counter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transition: Option<i64>,
    /// The last transition a delivery Job was created for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notified_transition: Option<i64>,
    /// How that delivery went.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<AlertDelivery>,
}

/// `ProtectionPolicy.status`.
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProtectionPolicyStatus {
    /// The `metadata.generation` this verdict was computed from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// When. Rewritten only on change, or when older than half the interval.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evaluated_at: Option<Time>,
    /// `Healthy`, `AtRisk`, `Stale`, `Unprotected` or `Unknown`. **`Unknown`
    /// is a real answer** and never rendered as healthy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health: Option<String>,
    /// How availability was decided — `KubernetesStatus`, `Catalog` or
    /// `CatalogStale`. Printed, because "the point exists" and "a `Backup`
    /// object says it succeeded" are different claims.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub availability_basis: Option<String>,
    /// The newest point that can actually be recovered from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_available_point: Option<AvailablePoint>,
    /// The most recent run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_attempt: Option<LastAttempt>,
    /// How many runs have failed in a row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consecutive_failed_runs: Option<i64>,
    /// Missed-slot facts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub missed: Option<MissedSummary>,
    /// The schedules, as this policy sees them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 16))]
    pub schedules: Option<Vec<ScheduleSummary>>,
    /// The rehearsal side.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rehearsal: Option<RehearsalSummary>,
    /// Since when the objective has been missed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stale_since: Option<Time>,
    /// The deduplication ledger.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 16))]
    pub alerts: Option<Vec<AlertEntry>>,
    /// The condition set: `Ready`, `Protected`, `NotificationsDelivered`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}
