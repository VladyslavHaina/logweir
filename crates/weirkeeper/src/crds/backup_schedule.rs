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
//! other six fields — DOES NOT HOLD. A per-field transition rule is evaluated
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
/// Read the module header for why this is one rule on `.spec` and not six
/// rules on six fields, and for the rejected map-based form. The clause order
/// is the field order of [`BackupScheduleSpec`], minus `suspend`.
pub const SUSPEND_ONLY_RULE: &str = "has(self.schedule) == has(oldSelf.schedule) && (!has(self.schedule) || self.schedule == oldSelf.schedule) && has(self.sourceRef) == has(oldSelf.sourceRef) && (!has(self.sourceRef) || self.sourceRef == oldSelf.sourceRef) && has(self.topics) == has(oldSelf.topics) && (!has(self.topics) || self.topics == oldSelf.topics) && has(self.archive) == has(oldSelf.archive) && (!has(self.archive) || self.archive == oldSelf.archive) && has(self.concurrencyPolicy) == has(oldSelf.concurrencyPolicy) && (!has(self.concurrencyPolicy) || self.concurrencyPolicy == oldSelf.concurrencyPolicy) && has(self.retention) == has(oldSelf.retention) && (!has(self.retention) || self.retention == oldSelf.retention)";

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

/// Whether a new scheduled slot may start while an earlier one is unfinished.
///
/// `Forbid` is both the wire-schema default and the deserialization default,
/// so schedules created before this field existed acquire the safe behavior
/// without a migration write. `Allow` must be selected explicitly.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, JsonSchema, PartialEq, Eq)]
pub enum ConcurrencyPolicy {
    /// Do not start a slot while any Backup owned by this schedule is
    /// nonterminal or has an unknown state.
    #[default]
    Forbid,
    /// Permit different scheduled slots to run at the same time.
    Allow,
}

/// What a retention evaluation found. **Nothing here was deleted.**
// TASK 19 SHAPED THIS FIELD SET AGAINST THE STRUCT THAT PRODUCES IT.
// `crate::retention::RetentionReport` is the whole evaluation, and this is the
// typed status block that carries it, field for field, so
// `kubectl get backupschedule -o yaml` shows the report and the UI reads it
// with no extra call. The one divergence is `RemovableSetReport` — see the
// note above it for why a Rust enum cannot be a structural-schema field.
// A `//` comment for the reason `crds::Condition`'s is: doc comments become
// the shipped CRD's `description`.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RetentionReport {
    /// When the controller last evaluated retention.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evaluated_at: Option<Time>,
    /// `spec.retention.keepLast`, **as it was applied**. Absent when no rule
    /// was configured, or when the configured value was not a non-negative
    /// count and was therefore not applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_last: Option<i64>,
    /// `spec.retention.keepDays`, as it was applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_days: Option<i64>,
    /// The `backupId`s the policy keeps, newest first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sets_kept: Option<Vec<String>>,
    /// The sets the policy WOULD remove, newest first. They are still in the
    /// archive; Logweir deleted nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sets_that_would_be_removed: Option<Vec<RemovableSetReport>>,
    /// The exact removal command for each set above, one per set, in the same
    /// order, **in the CLI of that archive's own scheme** — `aws s3 rm` for
    /// `s3://`, `gsutil -m rm -r` for `gs://`, `az storage blob delete-batch`
    /// for `az://` and `rm -rf` for `file://`. **Reported, never executed.**
    ///
    /// THE FIELD NAME SAYS `aws` AND THREE OF THE FOUR SCHEMES DO NOT, and the
    /// name is deliberately unchanged (Task 24a, plan erratum **E18(b)**):
    /// renaming a status field an adopter may already read, to fix a
    /// description, would cost more than the description was worth. What was
    /// actually broken is what the field CONTAINED — it printed
    /// `aws s3 rm 'file:///…'` for a filesystem archive, a string an operator
    /// runs and which does nothing — and that is fixed. This sentence is the
    /// description catching up with the value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws_cli: Option<Vec<String>>,
    /// The same commands in `mc`'s spelling. Reported, never executed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mc_cli: Option<Vec<String>>,
    /// The manifest keys under the prefix that could not be read, and why.
    /// Each was SKIPPED: it is in neither `setsKept` nor
    /// `setsThatWouldBeRemoved`, and the rest of the archive was still
    /// evaluated. An empty list means every manifest under the prefix was
    /// read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skipped: Option<Vec<SkippedManifestReport>>,
    /// Why the evaluation removed nothing, when the reason is not "there is
    /// nothing to remove" — set when no retention rule is configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl RetentionReport {
    /// Whether this report FOUND what `other` found — every field compared
    /// except [`RetentionReport::evaluated_at`].
    ///
    /// # Why `evaluatedAt` is the one field left out
    ///
    /// It is a "when computed" field, not a finding: it timestamps the
    /// evaluation, so comparing it would make every evaluation differ from
    /// every other by construction. Plan erratum **E11(d)**, review finding
    /// M-1 — `retentionReport.evaluatedAt = now` on every pass is what made a
    /// `BackupSchedule` with an archive configured bump its own
    /// `resourceVersion` on every reconcile and spin, exactly as the two
    /// unconditional `lastTransitionTime` writes did. The rule is the same one
    /// the `metav1.Condition` contract states for a transition time: the
    /// timestamp moves when the thing it timestamps moves. The caller
    /// (`controllers::backup_schedule::status_patch_with_retention`) keeps the
    /// stored instant when this returns `true`.
    #[must_use]
    pub fn same_findings_as(&self, other: &Self) -> bool {
        let ignoring_when = |r: &Self| Self {
            evaluated_at: None,
            ..r.clone()
        };
        ignoring_when(self) == ignoring_when(other)
    }
}

/// One manifest key the evaluation could not read. **Task 19 review, F-5.**
// WHY A SKIP IS A REPORTED FACT AND NOT AN ERROR.
// `retention::evaluate` used to return `Err` on the first manifest that did
// not parse, so an archive of fifty good backup sets plus one stray
// `x/manifest.json` yielded NO `retentionReport` at all — the controller
// warned and omitted the whole block, which is indistinguishable in the status
// from "no evaluation has happened". Skipping the one key and naming it here
// keeps the other forty-nine reportable and makes the gap visible.
//
// Kept out of the doc comment because `schemars` publishes doc comments as
// `description` in the shipped CRD — the same reason `crds::Condition`'s own
// provenance note is a `//` comment.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SkippedManifestReport {
    /// The manifest key, exactly as the archive listed it.
    pub key: String,
    /// Why it could not be read — `is not a backup manifest` for a sibling
    /// JSON object the `/manifest.json` filter picked up, or the storage
    /// error's own message.
    pub reason: String,
}

/// One set retention WOULD remove. **It is still in the archive.**
// WHY `reason` IS A STRING AND NOT AN ENUM WITH DATA.
// `crate::retention::RemovalReason` is a Rust enum carrying `days` or `rank`,
// and `schemars` renders any data-carrying enum as `oneOf` with a `type`
// inside each branch. A Kubernetes STRUCTURAL SCHEMA forbids `type` inside
// `oneOf`/`anyOf`/`not`, so a faithfully typed `RemovalReason` in this status
// block would be rejected by the API server's schema validation at
// `kubectl apply` time — the CRD would not install at all. This is therefore
// a FLAT projection: `reason` is the variant name (a closed set, enumerated by
// `RemovalReason::name`, which is a `match` with no wildcard) and the
// parameter travels in `days` or `rank` beside it.
//
// Kept out of the doc comment because `schemars` publishes doc comments as
// `description` in the shipped CRD — the same reason `crds::Condition`'s own
// provenance note is a `//` comment.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RemovableSetReport {
    /// The backup set's id — the manifest key's parent directory.
    pub backup_id: String,
    /// The set's newest record instant, read from the manifest BODY and never
    /// from the key string, which carries no timestamp.
    pub newest_record_at: Time,
    /// `OlderThanKeepDays` or `BeyondKeepLast`. `OlderThanKeepDays` when both
    /// rules select the set, because age is the reason an operator acts on.
    pub reason: String,
    /// The configured `keepDays`, when `reason` is `OlderThanKeepDays`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub days: Option<i64>,
    /// The set's 1-based rank in newest-first order, when `reason` is
    /// `BeyondKeepLast`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rank: Option<i64>,
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
    /// A five-field cron expression, in UTC: minute hour day-of-month month
    /// day-of-week. `*`, a literal, a comma list, an `a-b` range and a `*/n`
    /// step are accepted, as are `@hourly`, `@daily` and `@weekly`; anything
    /// else is refused with a `Ready` condition naming the field, never read as
    /// a silent match-all. A slot that comes due more than the ONE-HOUR
    /// missed-slot horizon before the controller looks is skipped and recorded
    /// in `status.lastMissedSlot`.
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
    /// Whether a due slot may start while an earlier Backup owned by this
    /// schedule is unfinished. Omitted means `Forbid`, including on schedules
    /// stored before this field was introduced. `Allow` is explicit.
    #[serde(default)]
    pub concurrency_policy: ConcurrencyPolicy,
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
    /// A `Forbid` slot atomically admitted in status but whose `Backup`
    /// creation has not yet been confirmed. This short-lived reservation
    /// closes the controller crash window and is cleared after the child is
    /// observed or created.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_backup_ref: Option<LocalRef>,
    /// The most recent slot that came due and was NOT fired — the controller
    /// was down, or `Forbid` found a previous owned run still active. Recorded because
    /// object identity is a pure function of the trigger (guard **G-SLOT**):
    /// a missed slot is never silently re-fired under a different name.
    ///
    /// THE MISSED-SLOT HORIZON IS ONE HOUR. A slot that came due more than one
    /// hour before the controller looked is skipped and recorded here, with a
    /// `Ready` condition whose reason is `SlotMissed`; a controller restarted
    /// after a week therefore fires at most the current slot and never six
    /// days of backlog. A concurrency skip uses reason `ConcurrencyBlocked`.
    /// The field is written when a slot is skipped and is
    /// never cleared afterwards — it is the audit trail of the skip, so a
    /// correct implementation is distinguishable from a broken schedule.
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
