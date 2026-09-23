//! `BackupSchedule` — the kind whose policy an operator edits, and the one
//! field editing may never reach.
//!
//! # What PLAT-05.1 changed, and the one thing it did not
//!
//! Until D1 this spec carried an object-level seal that made `suspend` the one
//! mutable field, so changing a cron expression meant creating a second
//! schedule and draining the first. D1 §5.1 inverts that: **every field is
//! mutable except `sourceRef`**, because a schedule's identity is the cluster
//! it protects and nothing else. One schedule's history must not mix two
//! clusters, so `sourceRef` keeps its seal ([`SOURCE_REF_IMMUTABLE_RULE`], D1's
//! rule R1) and everything else — cadence, zone, deadlines, catch-up, retry,
//! topics, archive, destination, retention, concurrency, suspend — is future
//! policy the next admission reads.
//!
//! That is safe because a **run** is still immutable. Each created `Backup`
//! copies the policy and records `spec.scheduleRef {uid, generation,
//! runPolicySha256}`, and PLAT-06.1 freezes the resolved settings into an
//! immutable ConfigMap before any Job exists. An edit therefore changes the
//! next admission and can never reach a run that already exists.
//!
//! `destinationRef` WAS SEALED BY THE OLD RULE AND IS NOW MUTABLE, and that is
//! a decision rather than an oversight: D1 §5.1 makes `archive` mutable, and
//! `archive` and `destinationRef` are two spellings of one location (the
//! sentinel rule below binds them). Sealing one while the other moves would be
//! an immutability claim an operator could route around by editing the
//! spelling the seal did not name.
//!
//! # Why R1 is still an object-level rule
//!
//! The obvious shape — `self.sourceRef == oldSelf.sourceRef` on
//! `.spec.sourceRef` — DOES NOT HOLD for an optional field, because a
//! per-field transition rule is evaluated only when `oldSelf` exists at that
//! path, so an absent → present transition never fires it. `sourceRef` is
//! required today, but a rule whose soundness depends on a `required:` list
//! somebody may edit is a rule that stops working silently. R1 is therefore on
//! `.spec`, where it is evaluated on every update, and it carries its own
//! `has(self.x) == has(oldSelf.x)` half.
//!
//! # One disclosed divergence, with its reason
//!
//! Critique B M3 supplied a map-shaped rule text:
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
//! This note exists so the next reader does not "restore" the map form. The
//! rejected string is above; it is quoted here and nowhere else.
//!
//! # What is deliberately NOT in CEL
//!
//! "Exactly one of a non-empty `topics` or `allUserTopics`" is not expressible
//! without breaking stored objects: a schedule stored with `topics: []` would
//! fail the rule on every update, including a `suspend` flip, and the 1.29
//! floor has no validation ratcheting. Cron validity and time-zone validity are
//! not expressible at all. All three are controller fail-closed checks (D1 §4.5
//! step 0) and API 422s; the CRD carries only what it can carry without
//! stranding an object somebody already created.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::selection::AllUserTopics;
use super::{ArchiveRef, Condition, LocalRef, SpecRule, Time};

/// The IANA zone-name grammar D1 §4.1 states.
///
/// A SYNTAX GATE AND NOT A ZONE LIST. The schema cannot hold the tz database,
/// and an enum of 600 zone names would go stale with every tzdata release, so
/// the CRD refuses what is not shaped like a zone name and the controller
/// resolves the rest against the compiled-in database — `Ready=False` with
/// reason `UnknownTimeZone` for a well-shaped name nobody has (D1 §4.3).
pub const TIME_ZONE_PATTERN: &str = r"^[A-Za-z][A-Za-z0-9_+\-]*(/[A-Za-z0-9_+\-]+){0,2}$";

/// **R1** (D1 §5.2). The object-level CEL rule that makes `spec.sourceRef` the
/// one field an edit may not reach.
///
/// Read the module header for why this is on `.spec` and not on
/// `.spec.sourceRef`, and for the rejected map-based form. It compares a
/// REQUIRED field to itself, so no stored object can fail it — which is what
/// makes it applicable on the 1.29 floor, where there is no validation
/// ratcheting to rescue an object an added rule strands.
pub const SOURCE_REF_IMMUTABLE_RULE: &str = "has(self.sourceRef) == has(oldSelf.sourceRef) && (!has(self.sourceRef) || self.sourceRef == oldSelf.sourceRef)";

/// The message the API server returns when [`SOURCE_REF_IMMUTABLE_RULE`]
/// refuses an update.
pub const SOURCE_REF_IMMUTABLE_MESSAGE: &str =
    "spec.sourceRef is immutable; create a new BackupSchedule to protect a different cluster";

/// **R2** (D1 §5.2). Dynamic selection and a named allowlist are two answers to
/// one question.
///
/// A VALIDATION RULE AND NOT A TRANSITION RULE, so it runs on create as well.
/// It names a field no stored object has (`allUserTopics` is new), so it cannot
/// strand anything: the left disjunct is true for every object that exists
/// today.
///
/// The converse — "`topics: []` requires `allUserTopics`" — is deliberately NOT
/// here; see the module header.
pub const SELECTION_SHAPE_RULE: &str = "!has(self.allUserTopics) || size(self.topics) == 0";

/// The message [`SELECTION_SHAPE_RULE`] travels with.
pub const SELECTION_SHAPE_MESSAGE: &str = "spec.allUserTopics requires spec.topics to be empty";

/// **R3** (D1 §5.2). The retry name budget, on the schema ROOT because that is
/// the one node a rule may read `self.metadata.name` from.
///
/// A retry `Backup` is named `logweir-backup-<schedule>-<slot>-r<N>`, three
/// characters longer than attempt 0, so a schedule that wants retries has a
/// 29-character name budget instead of 32
/// ([`crate::slot::max_schedule_name_len`]). Refusing the EDIT is the only
/// place this can be refused usefully: refusing at admission time would leave a
/// schedule that looks configured and silently never retries.
///
/// `maxRetries == 0` is exempt, because a schedule that configured zero retries
/// never composes a `-r<N>` name. No stored object has `spec.retry`, so the
/// first disjunct is true for every object that exists today.
pub const RETRY_NAME_BUDGET_RULE: &str =
    "!has(self.spec.retry) || self.spec.retry.maxRetries == 0 || size(self.metadata.name) <= 29";

/// The message [`RETRY_NAME_BUDGET_RULE`] travels with.
pub const RETRY_NAME_BUDGET_MESSAGE: &str =
    "a BackupSchedule with retries must be named in 29 characters or fewer; retry Backups are named logweir-backup-<schedule>-<slot>-r<N>";

/// The CEL rule that ties `spec.destinationRef` to the sentinel in
/// `spec.archive.url`.
///
/// Identical in text and in reason to [`super::backup::DESTINATION_SENTINEL_RULE`]
/// — the same two fields, the same rollback behaviour — and stated once per
/// kind because each kind carries its own `.spec` rule list and a shared
/// constant read from the other module would hide which kinds actually have
/// it.
pub const DESTINATION_SENTINEL_RULE: &str = super::backup::DESTINATION_SENTINEL_RULE;

/// The message [`DESTINATION_SENTINEL_RULE`] travels with.
pub const DESTINATION_SENTINEL_MESSAGE: &str = super::backup::DESTINATION_SENTINEL_MESSAGE;

/// The rules on `BackupSchedule`'s `.spec`.
///
/// The seal (R1) is first: it is a transition rule and is skipped on create.
/// R2 and the destination sentinel are validation rules that must run on create
/// as well. R3 is not here — a name-length budget may only read
/// `self.metadata.name`, which is readable only at the schema ROOT, so it is
/// attached by `crds::mod`'s emitter through
/// [`super::attach_root_rule`].
pub const SPEC_RULES: [SpecRule; 3] = [
    SpecRule::new(SOURCE_REF_IMMUTABLE_RULE, SOURCE_REF_IMMUTABLE_MESSAGE),
    SpecRule::new(SELECTION_SHAPE_RULE, SELECTION_SHAPE_MESSAGE),
    SpecRule::new(DESTINATION_SENTINEL_RULE, DESTINATION_SENTINEL_MESSAGE),
];

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

/// What to do with a slot that came due while nothing was watching (D1 §4.1).
///
/// A CRD ENUM BESIDE [`crate::cadence::CatchUpPolicy`], not that type itself,
/// and the reason is ownership: `cadence` is a pure module with no `schemars`
/// dependency and no Kubernetes types in it, and making a wire schema out of it
/// would put the CRD's compatibility contract inside a module whose job is
/// arithmetic. [`CatchUpPolicy::policy`] is the one conversion, so the two
/// cannot drift.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, JsonSchema, PartialEq, Eq)]
pub enum CatchUpPolicy {
    /// Count the slot in `status.missedSlots` and move on — **today's
    /// behaviour, and what an absent field means**.
    #[default]
    None,
    /// The LATEST due slot, and only that one, may still run after its
    /// starting deadline. At most one catch-up run, ever. Bounded(N) was
    /// rejected (D1 §4.10): `logweir backup run` captures what the broker
    /// retains WHEN IT RUNS, so N catch-up runs back to back produce
    /// near-identical archives at N times the load.
    Latest,
}

impl CatchUpPolicy {
    /// This policy as the cadence engine spells it.
    #[must_use]
    pub const fn policy(self) -> crate::cadence::CatchUpPolicy {
        match self {
            Self::None => crate::cadence::CatchUpPolicy::None,
            Self::Latest => crate::cadence::CatchUpPolicy::Latest,
        }
    }
}

/// `spec.retry` (D1 §4.1). Absent means **no retries**.
///
/// `maxRetries` IS REQUIRED AND HAS NO SCHEMA DEFAULT, so writing the block at
/// all is a decision about how many attempts a slot gets; `0` is a legal value
/// that records "retries were considered and declined". `delaySeconds` is
/// optional and defaults to [`crate::cadence::DEFAULT_RETRY_DELAY_SECONDS`].
///
/// Retries are only ever attempted for a **retryable** failure (D1 §4.6), only
/// for the latest due slot, and only until the next slot comes due.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RetrySpec {
    /// How many retries one slot may have, `0..=3`. Each retry is a new
    /// `Backup` with a new execution id: a failed attempt's partial archive is
    /// never appended to.
    #[schemars(range(min = 0, max = 3))]
    pub max_retries: i32,
    /// Seconds between a failed attempt finishing and its retry becoming
    /// admissible. Absent means 300.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 60, max = 21600))]
    pub delay_seconds: Option<i64>,
}

impl RetrySpec {
    /// This block as the cadence engine spells it, with the documented default
    /// filled in.
    #[must_use]
    pub fn policy(&self) -> crate::cadence::RetryPolicy {
        crate::cadence::RetryPolicy {
            max_retries: u32::try_from(self.max_retries).unwrap_or(u32::MAX),
            delay_seconds: self
                .delay_seconds
                .unwrap_or(crate::cadence::DEFAULT_RETRY_DELAY_SECONDS),
        }
    }
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

/// `BackupSchedule.spec`. Every field is editable except `sourceRef` — see the
/// module header.
#[derive(kube::CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "logweir.dev",
    version = "v1alpha1",
    kind = "BackupSchedule",
    doc = "A recurring backup of a topic set into an archive. Every spec field is EDITABLE except `sourceRef`, which is sealed by an object-level CEL rule; each created Backup copies the policy and records the revision it ran, so an edit reaches the next run and never a run that exists. Retention REPORTS what it would remove and deletes nothing.",
    plural = "backupschedules",
    singular = "backupschedule",
    namespaced,
    status = "BackupScheduleStatus",
    printcolumn = r#"{"name":"SCHEDULE","type":"string","jsonPath":".spec.schedule"}"#,
    printcolumn = r#"{"name":"SUSPEND","type":"string","jsonPath":".spec.suspend"}"#,
    printcolumn = r#"{"name":"LAST","type":"date","jsonPath":".status.lastFireTime"}"#,
    printcolumn = r#"{"name":"NEXT","type":"date","jsonPath":".status.nextFireTime"}"#,
    printcolumn = r#"{"name":"READY","type":"string","jsonPath":".status.conditions[?(@.type==\"Ready\")].status"}"#,
    printcolumn = r#"{"name":"AGE","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct BackupScheduleSpec {
    /// A five-field cron expression: minute hour day-of-month month
    /// day-of-week. `*`, a literal, a comma list, an `a-b` range and a `*/n`
    /// step are accepted, as are `@hourly`, `@daily` and `@weekly`; anything
    /// else is refused with a `Ready` condition naming the field, never read as
    /// a silent match-all. The fields are read in `timeZone`, which is UTC when
    /// absent; the slot identity is always the UTC instant. A slot that comes
    /// due more than `startingDeadlineSeconds` before the controller looks is
    /// skipped and counted in `status.missedSlots`, unless `catchUpPolicy` is
    /// `Latest`. A slot that came due before this object's
    /// `metadata.creationTimestamp` is never fired and is not counted as
    /// missed, as with a Kubernetes CronJob: the first run is the first slot at
    /// or after creation.
    pub schedule: String,
    /// The `KafkaCluster` to back up, in this namespace.
    ///
    /// **The one immutable field on this spec.** A schedule's identity is the
    /// cluster it protects; one schedule's history must not mix two clusters,
    /// so protecting a different cluster is a different schedule.
    pub source_ref: LocalRef,
    /// NAMED topics, never patterns. Global Constraint 18's first rail and
    /// guard **G-GLOB**: a glob metacharacter is refused, and an omitted list
    /// is not permitted at all — the one shape that would mean "everything" to
    /// the engine. A mandatory allowlist whose absence means "all topics" is
    /// not an allowlist.
    ///
    /// `[]` WITH `allUserTopics` SET is the one other legal shape: dynamic
    /// selection, resolved per run. `[]` on its own is refused by the
    /// controller before it admits anything.
    pub topics: Vec<String>,
    /// Dynamic selection: every user topic the run's principal can see, minus
    /// the exclusions. Requires `topics: []` (CEL).
    ///
    /// Wire-compatible with an older controller by construction: it reads
    /// `topics: []`, renders an empty list, and the runner's own empty-list
    /// rail exits 3 without contacting the engine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub all_user_topics: Option<AllUserTopics>,
    /// Where the backup is written.
    ///
    /// With `destinationRef` set this is the sentinel
    /// `logweir-destination://<name>` and carries no `secretRef`.
    pub archive: ArchiveRef,
    /// The saved `BackupDestination` every run of this schedule writes to, in
    /// this namespace.
    ///
    /// Optional, additive and sealed like every other field but `suspend`:
    /// re-pointing a schedule at a different destination is a different
    /// schedule, for the same reason a different location is a different
    /// destination.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_ref: Option<LocalRef>,
    /// Whether a due slot may start while an earlier Backup owned by this
    /// schedule is unfinished. Omitted means `Forbid`, including on schedules
    /// stored before this field was introduced. `Allow` is explicit.
    #[serde(default)]
    pub concurrency_policy: ConcurrencyPolicy,
    /// The IANA zone `schedule`'s fields are read in. **Absent means UTC and
    /// reproduces the slots an older controller computed, instant for
    /// instant.**
    ///
    /// The slot identity stays the UTC instant, so names remain unique and
    /// monotonic whatever the zone. Every real instant whose local wall time
    /// matches fires once; a matching local time that does not exist (a DST
    /// gap) fires once at the end of the gap; a fixed local time inside a
    /// repeated hour therefore fires at BOTH occurrences, and
    /// `status.nextRuns` marks which is which. A name this build's tz database
    /// does not have is `Ready=False` with reason `UnknownTimeZone` and admits
    /// nothing — never a silent fall back to UTC.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 64), regex(path = "TIME_ZONE_PATTERN"))]
    pub time_zone: Option<String>,
    /// How long after a slot came due it may still start. **Absent means
    /// 3600** — the one-hour horizon an older controller hard-coded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 60, max = 604800))]
    pub starting_deadline_seconds: Option<i64>,
    /// What to do with a slot that is past its starting deadline. **Absent
    /// means `None`** — count it and move on, which is today's behaviour.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catch_up_policy: Option<CatchUpPolicy>,
    /// How many times a slot may be retried, and how long after the failure.
    /// **Absent means no retries**, which is today's behaviour. Only a
    /// retryable failure is retried, only for the latest due slot, and only
    /// until the next slot comes due.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetrySpec>,
    /// The Job deadline copied into every created `Backup`'s
    /// `spec.deadlineSeconds`. **Absent means 3600**, the constant an older
    /// controller used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 60, max = 86400))]
    pub active_deadline_seconds: Option<i64>,
    /// `{keepLast, keepDays}` — **reporting only**. Logweir deletes nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<Retention>,
    /// Stop admitting new slots, catch-ups and retries. Runs that already
    /// exist are unaffected, and a reservation that was already accepted is
    /// still resumed — suspending stops future work, it does not cancel
    /// accepted work.
    #[serde(default)]
    pub suspend: bool,
}

/// The revision the controller last evaluated, and the digest of what a run
/// under it does (D1 §4.8).
///
/// `runPolicySha256` COVERS WHAT A RUN DOES AND NOT WHEN IT RUNS. Cadence, zone,
/// deadlines, catch-up, retry, concurrency, retention and `suspend` are
/// excluded, so an operator can see at a glance that suspending and resuming a
/// schedule left the policy digest alone and that a topic-list edit did not.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PolicyStatus {
    /// The `metadata.generation` this block was computed from.
    pub generation: i64,
    /// `sha256:<lowercase hex>` over the run policy of that generation.
    pub run_policy_sha256: String,
    /// The EFFECTIVE zone — `UTC` when `spec.timeZone` is absent, so a reader
    /// never has to know the default to read the status.
    pub time_zone: String,
    /// Which time-zone database resolved it, so a slot computed by one release
    /// can be explained after a tzdata update.
    pub tzdb: String,
    /// When this revision was first observed. A catch-up never runs a slot
    /// older than this: a schedule edited at noon does not retroactively back
    /// up the morning under the new policy.
    pub effective_since: Time,
    /// When this status last MOVED — not when the controller last looked.
    ///
    /// The controller re-examines every schedule every 30 s and deliberately
    /// writes nothing when the computed status equals the stored one, so this
    /// instant standing still means "nothing has changed", not "nobody is
    /// watching". The staleness signal is `status.nextRuns[0].at`: a live
    /// controller rewrites the previews once their first entry has passed.
    pub evaluated_at: Time,
}

/// One upcoming firing (D1 §4.4).
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NextRun {
    /// The UTC instant.
    pub at: Time,
    /// The same instant rendered in the schedule's zone, offset included —
    /// `2026-10-25T02:30:00+02:00`.
    pub local_time: String,
    /// `NonexistentLocalTimeShifted`, `RepeatedLocalTimeFirst` or
    /// `RepeatedLocalTimeSecond`. Omitted when the instant needed no
    /// adjustment, which is every instant outside a DST transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adjustment: Option<String>,
}

/// What happened to the most recent slot the controller decided about
/// (D1 §4.8).
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LastSlot {
    /// The slot, `yyyymmdd-hhmmss` of its UTC instant.
    pub slot: String,
    /// The instant that name spells.
    pub due_at: Time,
    /// Which attempt of it this disposition is about.
    pub attempt: i32,
    /// One of `Admitted`, `CaughtUp`, `Retried`, `Missed`, `Superseded`,
    /// `Blocked`, `NameUnavailable`, `Released`, `Failed`, `Exhausted`.
    pub disposition: String,
    /// The `Backup` the disposition is about, when there is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_ref: Option<LocalRef>,
    /// The `Ready` reason that accompanied the decision.
    pub reason: String,
    /// When it was decided.
    pub decided_at: Time,
}

/// One slot that came due and was not run (D1 §4.8).
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MissedSlot {
    /// The slot.
    pub slot: String,
    /// `ControllerUnavailable`, `ConcurrencyBlocked`, `Superseded` or
    /// `BeforeRevision`.
    pub reason: String,
    /// When the controller noticed.
    pub recorded_at: Time,
}

/// The running total of slots that came due and were not run (D1 §4.8).
///
/// A COUNT PLUS A BOUNDED SAMPLE. A controller down for a week under a
/// one-minute schedule skipped ten thousand slots; naming them all would put
/// half a megabyte in a status. The count is the fact an operator acts on and
/// the ten most recent are the evidence.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MissedSlots {
    /// How many slots have been skipped since the schedule was created.
    pub count: i64,
    /// Whether any single evaluation stopped at its 1000-slot enumeration cap,
    /// so `count` is a floor rather than a total.
    pub count_capped: bool,
    /// The newest slot the accounting has already considered. It advances only
    /// when a slot receives a FINAL disposition, so a slot that waits and is
    /// then superseded is counted exactly once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_evaluated_slot: Option<String>,
    /// The ten most recent skips, newest first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recent: Option<Vec<MissedSlot>>,
}

/// A slot admitted in status but whose `Backup` creation has not been confirmed
/// (D1 §4.8, §5.4).
///
/// THE ATOMIC BOUNDARY. The reservation is a resourceVersion-conditional merge
/// PATCH; a 409 means an edit landed and the reconcile starts again under the
/// new generation. Once it is accepted, the `Backup` is created from the same
/// in-memory object, so the run's copied policy is always the policy of the
/// generation recorded here.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PendingRun {
    /// The `Backup`'s deterministic name.
    pub name: String,
    /// The slot it is for.
    pub slot: String,
    /// `0` for a scheduled or catch-up run, `1..=3` for a retry.
    pub attempt: i32,
    /// `Scheduled`, `CatchUp` or `Retry`.
    pub kind: String,
    /// The `metadata.generation` the reservation was made under, and therefore
    /// the generation the created run records.
    pub generation: i64,
}

/// One schedule-created run that is not terminal (D1 §4.8).
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ActiveRun {
    /// The `Backup`'s name.
    pub name: String,
    /// `Scheduled`, `CatchUp` or `Retry`.
    pub kind: String,
    /// `0` for a scheduled or catch-up run, `1..=3` for a retry.
    pub attempt: i32,
}

/// One `Backup` the §6.2 migration could not detach from its schedule, and why
/// (D1 §6.2 step 4).
///
/// NAMED IN THE STATUS AND NOT ONLY IN A CONDITION MESSAGE, because the
/// `HistoryRetained` condition is recomputed on EVERY reconcile and only an
/// inventory pass observes a blocked patch. A condition whose message were the
/// only record would lose its names on the next steady pass and then re-acquire
/// them an hour later; a reader would see a schedule flap between "blocked, and
/// here is what" and "blocked, and no idea what".
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MigrationBlocked {
    /// The `Backup`'s name.
    pub name: String,
    /// Why the patch could not be applied. ONE CLOSED VOCABULARY, not a mix of
    /// HTTP digits and CamelCase — a `jq` or console reader has to be able to
    /// branch on it:
    ///
    /// * `ApiForbidden` — the API server answered `403`. Transient: fix the
    ///   RBAC and the next inventory retries.
    /// * `ApiInvalid` — the API server answered `422`. Transient: a legacy
    ///   object that fails a newer schema; fix it and the next inventory
    ///   retries.
    /// * `NoScheduleReference` — **terminal**, and the one that is not the API
    ///   server's doing. The run's `spec.scheduleRef` does not name this
    ///   schedule, so detaching it would leave it a member of nothing
    ///   ([`crate::identity::is_run_of_schedule`] rule 3), and `Backup.spec` is
    ///   CEL-sealed so no operator can add the reference. It never clears. The
    ///   remedies are `--cascade=orphan` or deleting the run.
    ///
    /// Every entry names a `Backup`. An inventory that did not finish is not
    /// about one object and is therefore NOT in this list — it is
    /// [`ScheduleHistory::ownership_scan_complete`], and the condition says so.
    pub reason: String,
}

/// What the §6.7 inventory found (D1 §4.8 `history`, §6.2, §6.6).
///
/// # This block is the whole input to `HistoryRetained`
///
/// The condition is a pure function of what is written here, recomputed on
/// every reconcile rather than copied forward, so a status a human edited and a
/// status the controller wrote produce the same condition. That is why the
/// blocked runs and the two legacy counters are FIELDS and not only prose in a
/// message.
///
/// # Three fields D1 §4.8 did not name, each load-bearing
///
/// * `runCountCapped` — the inventory follows a bounded number of pages, and a
///   pass that is migrating stops as soon as it has spent its PATCH budget, so
///   `runCount`, `estimatedBytes` and `legacyOwnedRuns` are FLOORS whenever it
///   is set. The same honesty [`MissedSlots::count_capped`] applies to the slot
///   enumeration.
/// * `ownershipScanComplete` — see its own note. `runCountCapped` says the
///   COUNTS are floors; this says whether the OWNERSHIP claim can be made at
///   all, which is a different question and the one deletion turns on.
/// * `legacyMigratableRuns` — how many of `legacyOwnedRuns` are terminal and
///   could therefore be patched now. It is what separates "there is migration
///   work left, come back on the next pass" from "the only owned runs left are
///   still running, come back at the ordinary interval", which is D1 §6.2's
///   "never retried faster than the inventory interval".
/// * `migrationBlocked` — see [`MigrationBlocked`].
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleHistory {
    /// How many `Backup`s of this schedule UID the last inventory saw,
    /// terminal and active alike. A floor when `runCountCapped` is set.
    pub run_count: i64,
    /// Whether the inventory stopped at its page cap, so `run_count` and
    /// `estimated_bytes` are floors rather than totals.
    pub run_count_capped: bool,
    /// D1 §6.6's estimate of the etcd bytes this history occupies:
    /// Σ(serialized `Backup` length + 2048 + 2 × topic name bytes).
    pub estimated_bytes: i64,
    /// How many of those runs still carry this schedule's controller
    /// ownerReference, so deleting the schedule would still collect them.
    pub legacy_owned_runs: i64,
    /// How many of `legacy_owned_runs` are terminal, and therefore migratable
    /// on the next pass.
    pub legacy_migratable_runs: i64,
    /// The runs whose migration PATCH was refused: **an arbitrary bounded
    /// sample of at most ten, sorted by name** so that two passes over the same
    /// namespace report the same ten. It is a sample and not a list — the total
    /// is inside `legacy_owned_runs`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration_blocked: Option<Vec<MigrationBlocked>>,
    /// Whether the last walk that could have found a legacy ownerReference ran
    /// to exhaustion (D1 §6.7, and the hole this field closes).
    ///
    /// # THE CLAIM `HistoryRetained=True` RESTS ON, AND WHY IT NEEDED A FIELD
    ///
    /// "No run is owned by this schedule" is a statement about runs the
    /// controller did not see as much as about the ones it did. A **capped**
    /// namespace-wide walk saw at most
    /// `MAX_INVENTORY_PAGES × INVENTORY_PAGE_SIZE` objects and says so in
    /// `run_count_capped`; what it cannot say is that the objects past the
    /// bound carry no ownerReference. Before this field, such a walk reported
    /// `runCountCapped: true`, which the `HistoryLarge` arm turned into
    /// `HistoryRetained=True`, which unlocked the
    /// `logweir.dev/schedule-uid` selector — a label no unmigrated legacy
    /// object carries. Every later walk then looked only where the answer could
    /// not be, the condition said `Retained` forever, and D1 §6.9's upgrade
    /// gate passed on a schedule whose history a default-propagation delete
    /// would still collect. Reached by D1 §6.6's own published worst row.
    ///
    /// `false` therefore forces `HistoryRetained=False` and keeps the next walk
    /// namespace-wide. It is `true` when the walk ended on its own, and when
    /// the walk was label-selected — a selected walk is only ever run *after* a
    /// complete namespace-wide one proved there was nothing owned, and that
    /// earlier proof is what the claim rests on.
    pub ownership_scan_complete: bool,
    /// When the inventory last ran.
    pub inventoried_at: Time,
}

/// `BackupSchedule.status`.
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackupScheduleStatus {
    /// The `metadata.generation` the controller has evaluated. A generation
    /// ahead of this one is an edit the controller has not seen yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// The revision that is in force, and the digest of what a run under it
    /// does.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicyStatus>,
    /// The next five firings, computed by the controller. **The browser never
    /// evaluates cron**; it renders what this says. Empty while the schedule is
    /// suspended or its policy is invalid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_runs: Option<Vec<NextRun>>,
    /// What happened to the most recent slot that was decided about.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_slot: Option<LastSlot>,
    /// How many slots came due and were not run, with the ten most recent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub missed_slots: Option<MissedSlots>,
    /// The reservation, if one is outstanding. `pendingBackupRef` mirrors its
    /// name for readers written before this block existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_run: Option<PendingRun>,
    /// Every nonterminal schedule-created run, at most ten. `activeBackupRef`
    /// mirrors the first entry.
    ///
    /// An ABSENT list and an EMPTY list are different facts: absent means this
    /// controller has not yet taken an inventory of the schedule's runs, and
    /// empty means it has and there are none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_runs: Option<Vec<ActiveRun>>,
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
    /// What the PLAT-05.2 inventory last found: how much history this schedule
    /// has, and how much of it is still owned by the schedule object (D1 §6).
    ///
    /// An ABSENT block means the inventory has never run, which is what makes
    /// the first reconcile after an upgrade take one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history: Option<ScheduleHistory>,
    /// `Ready`, and whatever else the controller reports.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}
