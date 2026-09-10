//! `BackupSchedule` — the one kind with a mutable field, and therefore the one
//! kind whose CEL seal is object-level.
//!
//! # The seal, and why it is not five per-field rules
//!
//! Five of the six kinds carry `self == oldSelf` on `.spec`. This one cannot:
//! `suspend` must be flippable without recreating the schedule, which is the
//! only write the controller performs on any `.spec` in tag 1 (spec §7).
//!
//! The obvious shape — a `self == oldSelf` transition rule on each of the
//! other five fields — DOES NOT HOLD. A per-field transition rule is evaluated
//! only when `oldSelf` exists for that field, so an OPTIONAL field could be
//! **added** after creation (absent → present) and the rule would never fire.
//! `retention` is optional, and `retention.keepDays` inside it is optional
//! too, so the hole is not theoretical: an adopter could add a retention block
//! to an approved, running schedule. `optionalOldSelf` closes exactly this and
//! is Kubernetes 1.30+, above the 1.29 floor Global Constraint 25 fixes.
//!
//! [`SUSPEND_ONLY_RULE`] is therefore ONE object-level rule on `.spec`, which
//! is evaluated on every update, and its `has(self.x) == has(oldSelf.x)`
//! halves are what refuse the absent → present transition.
//!
//! # One disclosed divergence, with its reason
//!
//! Critique B M3 supplies this rule text:
//!
//! ```text
//! self.filter(k, k != 'suspend').all(k, has(oldSelf[k]) == has(self[k]) && (!has(self[k]) || self[k] == oldSelf[k]))
//! ```
//!
//! It cannot ship, and this is MEASURED rather than argued. Kubernetes CEL
//! exposes a structural-schema **object** as a message type, not as a map, so
//! `self.filter(…)` and `oldSelf[k]` do not typecheck against it and the CRD is
//! REJECTED at `kubectl apply` time by the API server's rule compilation.
//! Applied to a live `docker-desktop` API server (Task 15b, 2026-09-09) on a
//! throwaway probe CRD, `kubectl apply` exited 1 with:
//!
//! ```text
//! spec.validation.openAPIV3Schema.properties[spec].x-kubernetes-validations[0].rule:
//!   Invalid value: …: compilation failed:
//!   ERROR: <input>:1:50: invalid argument to has() macro
//! ```
//!
//! — three times over, once for each `has()` on a subscript. The enumerated
//! form in [`SUSPEND_ONLY_RULE`] holds M3's property exactly — object-level
//! evaluation, the absent → present transition closed, `suspend` alone mutable
//! — in CEL the API server compiles, and the same live run confirmed the
//! behaviour end to end: changing `spec.schedule` was refused with
//! [`SUSPEND_ONLY_MESSAGE`], flipping `spec.suspend` was accepted, and ADDING
//! the absent optional `spec.retention` was refused.
//!
//! This note exists so the next reader does not "restore" the map form. The
//! rejected string is above; it is quoted here and nowhere else.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{ArchiveRef, Condition, LocalRef, Time};

/// The object-level CEL rule that makes `spec.suspend` the only mutable field.
///
/// Read the module header for why this is one rule on `.spec` and not five
/// rules on five fields, and for the rejected map-based form. The clause order
/// is the field order of [`BackupScheduleSpec`], minus `suspend`.
pub const SUSPEND_ONLY_RULE: &str = "has(self.schedule) == has(oldSelf.schedule) && (!has(self.schedule) || self.schedule == oldSelf.schedule) && has(self.sourceRef) == has(oldSelf.sourceRef) && (!has(self.sourceRef) || self.sourceRef == oldSelf.sourceRef) && has(self.topics) == has(oldSelf.topics) && (!has(self.topics) || self.topics == oldSelf.topics) && has(self.archive) == has(oldSelf.archive) && (!has(self.archive) || self.archive == oldSelf.archive) && has(self.retention) == has(oldSelf.retention) && (!has(self.retention) || self.retention == oldSelf.retention)";

/// The message the API server returns when [`SUSPEND_ONLY_RULE`] refuses an
/// update.
pub const SUSPEND_ONLY_MESSAGE: &str =
    "only spec.suspend is mutable; create a new BackupSchedule instead";

/// `{keepLast, keepDays}` — **reporting only**, in every tag.
///
/// Evaluated by the controller against the manifests it lists; the result —
/// which sets *would* be removed, plus the exact removal command — goes into
/// [`BackupScheduleStatus::retention_report`] and the UI. **The adopter's own
/// bucket lifecycle policy does the deleting.** Guard **G-RET**: the retention
/// path's archive handle is read-only and performs zero object-store writes,
/// and retention never touches a Kafka topic.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Retention {
    /// Keep the most recent N backup sets; older sets are REPORTED as
    /// removable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_last: Option<i64>,
    /// Keep backup sets newer than N days; older sets are REPORTED as
    /// removable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_days: Option<i64>,
}

/// What a retention evaluation found. Nothing here was deleted.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RetentionReport {
    /// When the controller last evaluated retention.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evaluated_at: Option<Time>,
    /// The `backupId`s the policy would keep.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kept: Option<Vec<String>>,
    /// The `backupId`s the policy WOULD remove. They are still in the
    /// archive; Logweir deleted nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub removable: Option<Vec<String>>,
    /// The exact command an operator can run to remove the sets above, for
    /// their own object store. Reported, never executed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    /// Why the evaluation is incomplete, when it is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// `BackupSchedule.spec`. Only `suspend` is mutable — see the module header.
#[derive(kube::CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "logweir.dev",
    version = "v1alpha1",
    kind = "BackupSchedule",
    doc = "A recurring backup of a named topic set into an archive. `spec.suspend` is the only mutable field; every other field is sealed by an object-level CEL rule. Retention REPORTS what it would remove and deletes nothing.",
    plural = "backupschedules",
    singular = "backupschedule",
    namespaced,
    status = "BackupScheduleStatus",
    printcolumn = r#"{"name":"SCHEDULE","type":"string","jsonPath":".spec.schedule"}"#,
    printcolumn = r#"{"name":"SUSPEND","type":"string","jsonPath":".spec.suspend","description":"the one mutable spec field"}"#,
    printcolumn = r#"{"name":"LAST","type":"date","jsonPath":".status.lastFireTime"}"#,
    printcolumn = r#"{"name":"NEXT","type":"date","jsonPath":".status.nextFireTime"}"#,
    printcolumn = r#"{"name":"READY","type":"string","jsonPath":".status.conditions[?(@.type==\"Ready\")].status"}"#,
    printcolumn = r#"{"name":"AGE","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct BackupScheduleSpec {
    /// A five-field cron expression, in UTC.
    pub schedule: String,
    /// The `KafkaCluster` to back up, in this namespace.
    pub source_ref: LocalRef,
    /// NAMED topics, never patterns. Global Constraint 18's first rail and
    /// guard **G-GLOB**: a glob metacharacter is refused, and an omitted list
    /// is not permitted at all — the one shape that would mean "everything" to
    /// the engine. A mandatory allowlist whose absence means "all topics" is
    /// not an allowlist.
    pub topics: Vec<String>,
    /// Where the backup is written.
    pub archive: ArchiveRef,
    /// `{keepLast, keepDays}` — **reporting only**. Logweir deletes nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<Retention>,
    /// Stop firing new `Backup`s. **The only mutable field on this spec**, and
    /// the only `.spec` write the controller performs on any kind in tag 1.
    #[serde(default)]
    pub suspend: bool,
}

/// `BackupSchedule.status`.
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackupScheduleStatus {
    /// When this schedule last created a `Backup`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_fire_time: Option<Time>,
    /// When it will next create one, given `schedule` and `suspend`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_fire_time: Option<Time>,
    /// The `Backup` currently running for this schedule, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_backup_ref: Option<LocalRef>,
    /// The most recent slot that came due and was NOT fired — the controller
    /// was down, or the previous run was still active. Recorded because
    /// object identity is a pure function of the trigger (guard **G-SLOT**):
    /// a missed slot is never silently re-fired under a different name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_missed_slot: Option<String>,
    /// What retention WOULD remove. Nothing was deleted — see
    /// [`RetentionReport`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_report: Option<RetentionReport>,
    /// `Ready`, and whatever else the controller reports.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}
