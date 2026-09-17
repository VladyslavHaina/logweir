//! The `BackupSchedule` cron reconciler — guard **G-SLOT**.
//!
//! # The whole design in one sentence
//!
//! The object name is a pure function of the trigger, while `Forbid` first
//! reserves that name through a resource-version-checked `/status` replace.
//! The reservation serializes different slots across replicas; a duplicate
//! child `create` still gets **409 `AlreadyExists`**, which is the same-slot
//! idempotence key.
//!
//! # Why that is the design and not merely a tidy one
//!
//! A scheduled backup that fires twice for one slot is not a wasted run. A
//! second `backup` into a colliding `backup_id` does **not** accumulate:
//! measured on this project's own stack, a re-run left the manifest describing
//! 2048 records while the broker held 6000 (`scripts/e2e-seed.sh:81-91`) — a
//! partial archive a later drill restores from and calls a success. Any design
//! that derives the object name from a reconcile-time clock, or from
//! `status.lastFireTime`, produces two objects when the controller crashes
//! between the `create` and the status write. So [`decide`] is a pure function
//! of `(name, spec, now)` that **returns the object name**, and
//! [`reconcile_schedule`] — the half that talks to the API server — contains no
//! `Utc::now()` at all: the one clock read in this file is in the
//! `kube::runtime` wrapper, before anything is decided.
//!
//! # The final status write happens AFTER the create, deliberately
//!
//! `Forbid` has a short-lived `pendingBackupRef` before the create, but it does
//! not claim the Backup fired. A restart resumes that accepted deterministic
//! child. After the child is observed or created, the final patch records
//! `lastFireTime`, moves the reference to `activeBackupRef`, and clears the
//! reservation. This distinguishes accepted work from a stale active ref.
//!
//! # A skipped slot is a fact, not a silence
//!
//! A controller restarted after a week must not fire six days of backlog, so a
//! slot older than the one-hour [`MISSED_SLOT_HORIZON`] is skipped. It is also
//! **recorded** — `status.lastMissedSlot` plus a `Ready` condition with reason
//! [`REASON_SLOT_MISSED`] — because without the record a correct implementation
//! is indistinguishable from a broken schedule (critique B M20). The horizon is
//! stated in the CRD field description, where an adopter running
//! `kubectl explain` will find it.
//!
//! # A slot is MISSED only if it was NEVER FIRED
//!
//! The horizon alone does not say that. [`decide`] reads no `status` — that is
//! what makes the object name a pure function of the trigger — so on its own it
//! cannot tell "this slot came due and I fired it hours ago" from "this slot
//! came due and nobody fired it". Left there, the remedy above INVERTS: a
//! healthy `0 0 * * *` reconciled at 10:00 finds its own 00:00 slot outside the
//! one-hour horizon and writes that slot into `status.lastMissedSlot` with the
//! message "was not fired", which is false. Measured before the fix (fix round
//! 1, review finding HIGH-1): a daily schedule sat in `SlotMissed` for **1379
//! of 1440 minutes a day**, a weekly one for 166 of 168 hours — so the one
//! field an adopter reads to tell a correct implementation from a broken
//! schedule was written by NORMAL OPERATION on every schedule whose period
//! exceeds an hour.
//!
//! So the REASON is refined against `status.lastFireTime` by
//! [`refine_against_last_fire`], and the NAME is not. The split is the guard:
//! `decide` keeps its three pure arguments and mints the name; the refinement
//! runs afterwards, takes only `Option<DateTime<Utc>>`, and can turn
//! [`SlotDecision::Missed`] into [`SlotDecision::AlreadyFired`] and nothing
//! else. It cannot reach a name, cannot reach the `Due` arm, and therefore
//! cannot reintroduce the mutant G-SLOT kills (a name derived from
//! `status.lastFireTime` produces two objects when the controller crashes
//! between the `create` and the status write).
//!
//! # `suspend` is the one mutable field
//!
//! Task 15b's object-level CEL rule
//! ([`crate::crds::backup_schedule::SUSPEND_ONLY_RULE`]) makes it so. This
//! reconciler reads it and never writes it: like every reconciler in this
//! directory it patches `/status` and nothing else, and it never DELETEs.

use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use futures::StreamExt as _;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::{ListParams, ObjectMeta, Patch, PatchParams, PostParams};
use kube::runtime::controller::Action;
use kube::runtime::{watcher, Controller};
use kube::{Api, Resource, ResourceExt};
use serde_json::json;
use tracing::{debug, info, warn};

use logweir_store::Store;

use super::Context;
use crate::cadence::{
    Cadence, CadenceError, CatchUpPolicy as CadenceCatchUp, MissedReason, RetryAdmission,
    RetryPolicy, SlotAdmission,
};
use crate::conditions::{current_condition, merge_condition, status_unchanged};
use crate::crds::backup::{Backup, BackupSpec, ScheduleRef, Trigger, TriggerKind};
use crate::crds::backup_schedule::{
    ActiveRun, BackupSchedule, BackupScheduleSpec, ConcurrencyPolicy, LastSlot, MissedSlot,
    MissedSlots, NextRun, PendingRun, PolicyStatus, Retention,
};
use crate::crds::{Condition, LocalRef};
use crate::identity::is_run_of_schedule;
use crate::retention::RetentionReport;
use crate::slot::{scheduled_backup_name, slot_name, CronError, SlotError};
use logweir_core::destination::FieldError;

/// A value rendered into a condition message, bounded and with control
/// characters stripped.
///
/// A CONDITION MESSAGE IS READ BY A PERSON AND WRITTEN FROM ADOPTER-SUPPLIED
/// TEXT. The same treatment `backup_execution::shown` gives a spec value, for
/// the same reason: a 4 KiB zone name full of escape sequences would be a
/// `kubectl describe` nobody can read.
fn shown(value: &str) -> String {
    let cleaned: String = value
        .chars()
        .filter(|c| !c.is_control())
        .take(120)
        .collect();
    if value.chars().count() > 120 {
        format!("{cleaned}…")
    } else {
        cleaned
    }
}

/// The condition type this reconciler owns.
pub const CONDITION_READY: &str = "Ready";

/// `reason` when a due slot was fired (or found already fired).
pub const REASON_SCHEDULED: &str = "Scheduled";

/// `reason` when `spec.suspend` is true.
pub const REASON_SUSPENDED: &str = "Suspended";

/// `reason` when the due slot was older than [`MISSED_SLOT_HORIZON`].
pub const REASON_SLOT_MISSED: &str = "SlotMissed";

/// `reason` when `Forbid` declines a due slot because an earlier owned Backup
/// has not reached a known terminal phase.
pub const REASON_CONCURRENCY_BLOCKED: &str = "ConcurrencyBlocked";

/// `reason` when the expression parses but has no fire inside the parser's
/// lookback.
pub const REASON_NO_DUE_SLOT: &str = "NoDueSlot";

/// `reason` when `spec.schedule` did not parse.
pub const REASON_UNPARSEABLE_SCHEDULE: &str = "UnparseableSchedule";

/// `reason` when the composed object name would exceed the 63-character label
/// cap.
pub const REASON_NAME_TOO_LONG: &str = "NameTooLong";

/// `reason` when `spec.topics`/`spec.allUserTopics` are not one of the two
/// legal selection shapes, or a name in either is not Kafka-legal.
///
/// FAIL CLOSED, AND THAT IS D1 §5.5. The API server accepts these edits (the
/// converse of R2 is not expressible on the 1.29 floor without stranding stored
/// objects), so the controller is where they stop: `Ready=False`, no
/// admissions, running Backups untouched, and one reconcile after the spec is
/// fixed the schedule resumes.
pub const REASON_INVALID_TOPIC_SELECTION: &str = crate::conditions::REASON_INVALID_TOPIC_SELECTION;

/// `reason` when some other run-policy field is unusable — a non-positive
/// deadline, a retry block outside its range.
pub const REASON_INVALID_RUN_POLICY: &str = crate::conditions::REASON_INVALID_RUN_POLICY;

/// `reason` when `spec.timeZone` names a zone this build's database does not
/// have. **Fail closed**: never silently UTC.
pub const REASON_UNKNOWN_TIME_ZONE: &str = crate::conditions::REASON_UNKNOWN_TIME_ZONE;

/// `reason` when a past-deadline slot ran as a catch-up.
pub const REASON_CAUGHT_UP: &str = crate::conditions::REASON_CAUGHT_UP;

/// `reason` when a catch-up is admissible but concurrency blocks it.
pub const REASON_CATCH_UP_BLOCKED: &str = crate::conditions::REASON_CATCH_UP_BLOCKED;

/// `reason` when a retry was admitted.
pub const REASON_RETRY_SCHEDULED: &str = crate::conditions::REASON_RETRY_SCHEDULED;

/// `reason` when a retry is waiting out its delay.
pub const REASON_RETRY_PENDING: &str = crate::conditions::REASON_RETRY_PENDING;

/// `reason` when a retry is due but concurrency blocks it.
pub const REASON_RETRY_BLOCKED: &str = crate::conditions::REASON_RETRY_BLOCKED;

/// `reason` when the attempt chain reached `maxRetries` without succeeding.
pub const REASON_RETRY_EXHAUSTED: &str = crate::conditions::REASON_RETRY_EXHAUSTED;

/// `reason` when the latest attempt failed and the policy does not retry it.
pub const REASON_RUN_FAILED: &str = crate::conditions::REASON_RUN_FAILED;

/// `reason` when a slot's deterministic name is held by a foreign object.
pub const REASON_SLOT_NAME_UNAVAILABLE: &str = crate::conditions::REASON_SLOT_NAME_UNAVAILABLE;

/// `reason` when `Allow` has reached its concurrent-run ceiling.
pub const REASON_ACTIVE_RUN_LIMIT: &str = crate::conditions::REASON_ACTIVE_RUN_LIMIT;

/// `reason` when the installed CRD is older than this controller.
pub const REASON_CRD_OUTDATED: &str = crate::conditions::REASON_CRD_OUTDATED;

/// The most schedule-created runs of one schedule that may be unfinished at
/// once under `concurrencyPolicy: Allow` (D1 §4.7).
///
/// TEN, AND IT IS A BACKSTOP RATHER THAN A POLICY. `Allow` means "different
/// slots may overlap", not "there is no bound": a schedule whose runs take
/// longer than its period would otherwise accumulate one new Job per slot for
/// as long as the condition lasts, and the broker read each of them costs is
/// the same whether anybody is waiting for it. At the ceiling the schedule
/// reports `ActiveRunLimit` and admits nothing further — visibly, so the
/// operator can lengthen the period or shorten the run.
pub const MAX_ACTIVE_RUNS: usize = 10;

/// How many slots one reconcile will enumerate when accounting for downtime.
pub const MAX_SKIPPED_SLOT_ENUMERATION: usize = crate::cadence::MAX_SKIPPED_SLOT_ENUMERATION;

/// How many recent missed slots `status.missedSlots.recent` keeps.
pub const RECENT_MISSED_SLOTS: usize = 10;

/// `spec.triggeredBy` on every `Backup` this reconciler creates.
///
/// RECORDED RATHER THAN INFERRED from the presence of `scheduleRef`, because
/// the signed receipt carries it and an auditor reads the receipt. It is the
/// execution contract's own spelling ([`crate::backup_execution::TRIGGER_SCHEDULE`]),
/// because the `Backup` reconciler derives the scheduled run identity from it.
pub const TRIGGERED_BY_SCHEDULE: &str = crate::backup_execution::TRIGGER_SCHEDULE;

/// The label carrying the `BackupSchedule` that created a `Backup`.
///
/// Duplicated information — `spec.scheduleRef` says the same thing — and it is
/// duplicated on purpose: `spec` is not selectable, and `kubectl get backups -l
/// logweir.dev/schedule=nightly` is the question an operator actually asks. The
/// value is safe as a label because [`scheduled_backup_name`] has already
/// refused any schedule name that would not fit inside 63 characters.
pub const SCHEDULE_LABEL: &str = "logweir.dev/schedule";

/// The label carrying the slot a `Backup` is for.
pub const SLOT_LABEL: &str = "logweir.dev/slot";

/// The label carrying the schedule's UID, which — unlike its name — a recreated
/// schedule does not share.
pub const SCHEDULE_UID_LABEL: &str = crate::identity::SCHEDULE_UID_LABEL;

/// The label carrying the trigger kind, lowercase-hyphenated.
pub const TRIGGER_LABEL: &str = crate::identity::TRIGGER_LABEL;

/// The label carrying the attempt number.
pub const ATTEMPT_LABEL: &str = crate::identity::ATTEMPT_LABEL;

// NO RUNNER ARGV, AND NO ANNOTATION CARRYING ONE (PLAT-06.1). This reconciler
// used to write the runner-argv annotation onto every Backup it created, and
// the Backup reconciler executed it. The Backup reconciler now derives the argv
// from the typed spec and the server-generated run identity
// (`crate::backup_execution`), so the object this reconciler creates carries
// only its spec, its two labels and its controller owner reference.

/// `spec.deadlineSeconds` on every `Backup` this reconciler creates.
///
/// A CONSTANT BECAUSE `BackupSchedule` HAS NO SUCH FIELD, and adding one would
/// be a change to a sealed spec that is not this task's (Task 15b owns the
/// field set). One hour is the value; a schedule whose backup needs longer is a
/// request to the controller for a `deadlineSeconds` field on
/// `BackupSchedule`, not a reason to guess here.
pub const SCHEDULED_DEADLINE_SECONDS: i64 = 3600;

/// How long after a slot came due this reconciler will still fire it.
///
/// ONE HOUR. A controller restarted after a week must not fire six days of
/// backlog — a `Backup` for a window nobody is waiting for costs the same
/// broker read as one somebody is. The horizon is stated in
/// `crds/backup_schedule.rs`'s `lastMissedSlot` field description so
/// `kubectl explain backupschedule.status.lastMissedSlot` says it too.
#[must_use]
pub fn missed_slot_horizon() -> Duration {
    Duration::seconds(MISSED_SLOT_HORIZON)
}

/// [`missed_slot_horizon`] in seconds, as a plain integer.
pub const MISSED_SLOT_HORIZON: i64 = 3600;

/// How often this reconciler wakes with no event to wake it.
///
/// THIRTY SECONDS, AND IT IS THE WHOLE CLOCK. There is no timer, no leader
/// lease and no cron daemon: a cron reconciler whose name is a pure function of
/// the trigger does not need one, because waking late is a missed slot (which
/// is recorded) and waking twice is a 409 (which is success). Thirty seconds
/// bounds the lateness at half the shortest slot this grammar can express.
pub const REQUEUE_SECS: u64 = 30;

/// What one reconcile decided, and — for a due slot — the name it decided.
///
/// THE NAME IS IN THE DECISION, not computed later beside the `POST`. That
/// placement is the guard: a name minted inside the API-server-facing half
/// could read a clock or a status without the pure function noticing, and
/// `the_name_never_reads_a_reconcile_clock_or_a_status` would have nothing to
/// read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SlotDecision {
    /// `spec.suspend` is true. Create nothing; clear `nextFireTime`.
    Suspended,
    /// `spec.schedule` did not parse. Create nothing.
    Unparseable(CronError),
    /// `spec.timeZone` is a well-shaped name this build's tz database does not
    /// have. **Fail closed**: a zone nobody can resolve is not silently UTC,
    /// because a schedule that fires eight hours from where its operator
    /// expects is worse than one that visibly does not fire.
    UnknownTimeZone {
        /// The name as it was written.
        got: String,
    },
    /// `spec.retry` asks for retries this schedule's name cannot be extended
    /// to hold. Create nothing.
    ///
    /// REFUSED AS A POLICY, NOT AT THE RETRY. The CRD's root rule R3 refuses
    /// the EDIT, which is where it is useful; this arm is the defence for an
    /// object admitted by an older CRD that did not carry R3. Admitting the
    /// slot and silently never retrying it would leave a schedule that looks
    /// configured and is not.
    RetryNamesTooLong {
        /// The refusal, naming the limit.
        error: SlotError,
    },
    /// `spec.topics`/`spec.allUserTopics` are not one of the two legal
    /// selection shapes, or a name in either is not Kafka-legal. Create
    /// nothing; running Backups are untouched.
    InvalidTopicSelection {
        /// Every problem, each naming a dotted path rooted at `spec`.
        errors: Vec<FieldError>,
    },
    /// Some other run-policy field is unusable. Create nothing.
    InvalidRunPolicy {
        /// Every problem, each naming a dotted path rooted at `spec`.
        errors: Vec<FieldError>,
    },
    /// The expression parses but no slot has come due inside the parser's
    /// lookback.
    NoDueSlot {
        /// When it will next fire, if ever.
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// A slot came due, is older than [`MISSED_SLOT_HORIZON`], and
    /// `status.lastFireTime` says this schedule already fired it.
    ///
    /// THE HEALTHY STEADY STATE OF EVERY SCHEDULE SLOWER THAN HOURLY, and the
    /// variant that keeps [`Self::Missed`] meaning what it says. Produced only
    /// by [`refine_against_last_fire`], only from [`Self::Missed`], and never
    /// by [`decide`] — which reads no status at all.
    AlreadyFired {
        /// The slot that came due.
        due: DateTime<Utc>,
        /// That slot as [`slot_name`] spells it.
        slot: String,
        /// What `status.lastFireTime` said, which is at or after `due`.
        last_fire_time: DateTime<Utc>,
        /// When it will next fire.
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// A slot came due, is past its starting deadline, and was never fired.
    Missed {
        /// The slot that came due.
        due: DateTime<Utc>,
        /// That slot as [`slot_name`] spells it.
        slot: String,
        /// Why it will not run: past the starting deadline with no catch-up,
        /// or older than the revision in force.
        reason: MissedReason,
        /// When it will next fire.
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// A slot is past its starting deadline, `catchUpPolicy` is `Latest`, and
    /// it may still run. One catch-up, ever, and only for the latest due slot.
    CatchUpDue {
        /// The slot that came due.
        due: DateTime<Utc>,
        /// That slot as [`slot_name`] spells it.
        slot: String,
        /// The `Backup`'s `metadata.name` — the SAME name attempt 0 would have
        /// had, because a catch-up IS that slot, started late.
        name: String,
        /// When it will next fire.
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// A catch-up was admitted and its `Backup` created.
    CaughtUp {
        /// The slot that came due.
        due: DateTime<Utc>,
        /// That slot as [`slot_name`] spells it.
        slot: String,
        /// The `Backup`'s name.
        name: String,
        /// When it will next fire.
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// A catch-up is admissible but concurrency blocks it. It stays admissible
    /// while this slot is the latest due one.
    CatchUpBlocked {
        /// The slot that is waiting.
        slot: String,
        /// The runs that block it.
        active_backups: Vec<String>,
        /// When it will next fire.
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// The highest observed attempt of the latest due slot is still running,
    /// or has no terminal record yet.
    InProgress {
        /// The slot.
        slot: String,
        /// Which attempt.
        attempt: u32,
        /// The run's name.
        name: String,
        /// When it will next fire.
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// A retry was admitted and its `Backup` created.
    Retried {
        /// The slot being retried.
        due: DateTime<Utc>,
        /// That slot as [`slot_name`] spells it.
        slot: String,
        /// The retry's name, `…-r<k>`.
        name: String,
        /// Which attempt this is, `1..=3`.
        attempt: u32,
        /// When it will next fire.
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// A retry is admissible but its delay has not elapsed.
    RetryPending {
        /// The slot.
        slot: String,
        /// The attempt that failed.
        attempt: u32,
        /// When the retry becomes admissible.
        due_at: DateTime<Utc>,
        /// When it will next fire.
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// A retry is due but concurrency blocks it.
    RetryBlocked {
        /// The slot.
        slot: String,
        /// The attempt that failed.
        attempt: u32,
        /// The runs that block it.
        active_backups: Vec<String>,
        /// When it will next fire.
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// The attempt chain reached `maxRetries` without succeeding.
    RetryExhausted {
        /// The slot.
        slot: String,
        /// The last attempt.
        attempt: u32,
        /// The configured ceiling.
        max_retries: u32,
        /// Whether `spec.retry` was configured at all; when it was not, the
        /// reason reported is `RunFailed` rather than `RetryExhausted`, because
        /// nothing was exhausted.
        retry_configured: bool,
        /// When it will next fire.
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// The latest attempt failed in a way that is never retried — a guard
    /// refusal, a drill that did not pass, a signing failure.
    RunFailed {
        /// The slot.
        slot: String,
        /// The attempt that failed.
        attempt: u32,
        /// Its name.
        name: String,
        /// When it will next fire.
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// The deterministic name this slot needs is held by an object this
    /// schedule does not own.
    SlotNameUnavailable {
        /// The slot.
        slot: String,
        /// The name that is taken.
        name: String,
        /// When it will next fire.
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// `Allow` has reached its ceiling of concurrent schedule-created runs.
    ActiveRunLimit {
        /// The slot that was not admitted.
        slot: String,
        /// The runs that are already going.
        active_backups: Vec<String>,
        /// When it will next fire.
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// A write response came back without a field this controller wrote, which
    /// means the installed CRD is older than this controller (D1 §4.9).
    CrdOutdated {
        /// Which field the API server pruned.
        detail: String,
        /// When it will next fire.
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// A slot was due but `Forbid` found an earlier owned Backup whose state is
    /// nonterminal or unknown. The slot is not retried under a new identity.
    ConcurrencyBlocked {
        /// The slot that was not fired.
        slot: String,
        /// Names of the owned Backups that prevented admission, sorted for a
        /// stable condition message.
        active_backups: Vec<String>,
        /// When the cron expression next fires.
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// A slot is due, and the object name it needs does not fit.
    NameTooLong {
        /// That slot as [`slot_name`] spells it.
        slot: String,
        /// The refusal, naming the limit.
        error: SlotError,
        /// When it will next fire.
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// A slot is due. Create `name`.
    Due {
        /// The slot that came due.
        due: DateTime<Utc>,
        /// That slot as [`slot_name`] spells it.
        slot: String,
        /// The `Backup`'s `metadata.name` — a pure function of `due`.
        name: String,
        /// When it will next fire.
        next_fire_time: Option<DateTime<Utc>>,
    },
}

impl SlotDecision {
    /// The condition `reason` for this decision.
    ///
    /// A `match` WITH NO WILDCARD, so a ninth variant fails to compile until
    /// someone names its reason — the same discipline
    /// [`super::approval::ApprovalRefusal::reason`] holds.
    ///
    /// [`Self::AlreadyFired`] REPORTS `Scheduled`, NOT A SEVENTH REASON. An
    /// adopter reading `kubectl get backupschedules` on a healthy nightly
    /// backup is looking at a schedule that is being honoured; a distinct
    /// reason for "the slot I fired at midnight is now more than an hour old"
    /// would be a printer column that changes every day for no operational
    /// reason. `SlotMissed` stays scarce so that it stays informative.
    #[must_use]
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Suspended => REASON_SUSPENDED,
            Self::Unparseable(_) => REASON_UNPARSEABLE_SCHEDULE,
            Self::UnknownTimeZone { .. } => REASON_UNKNOWN_TIME_ZONE,
            Self::InvalidTopicSelection { .. } => REASON_INVALID_TOPIC_SELECTION,
            Self::InvalidRunPolicy { .. } => REASON_INVALID_RUN_POLICY,
            Self::NoDueSlot { .. } => REASON_NO_DUE_SLOT,
            Self::Missed { .. } => REASON_SLOT_MISSED,
            Self::ConcurrencyBlocked { .. } => REASON_CONCURRENCY_BLOCKED,
            Self::CatchUpBlocked { .. } => REASON_CATCH_UP_BLOCKED,
            Self::ActiveRunLimit { .. } => REASON_ACTIVE_RUN_LIMIT,
            Self::NameTooLong { .. } | Self::RetryNamesTooLong { .. } => REASON_NAME_TOO_LONG,
            Self::SlotNameUnavailable { .. } => REASON_SLOT_NAME_UNAVAILABLE,
            Self::CaughtUp { .. } | Self::CatchUpDue { .. } => REASON_CAUGHT_UP,
            Self::Retried { .. } => REASON_RETRY_SCHEDULED,
            Self::RetryPending { .. } => REASON_RETRY_PENDING,
            Self::RetryBlocked { .. } => REASON_RETRY_BLOCKED,
            // A CHAIN THAT WAS NEVER CONFIGURED TO RETRY EXHAUSTED NOTHING.
            // D1 §4.7 row 10 says so explicitly: with `spec.retry` absent the
            // reason is `RunFailed`, because `RetryExhausted` on a schedule
            // that has no retry policy reads as "your retries ran out" to an
            // operator who never asked for any.
            Self::RetryExhausted {
                retry_configured, ..
            } => {
                if *retry_configured {
                    REASON_RETRY_EXHAUSTED
                } else {
                    REASON_RUN_FAILED
                }
            }
            Self::RunFailed { .. } => REASON_RUN_FAILED,
            Self::CrdOutdated { .. } => REASON_CRD_OUTDATED,
            Self::Due { .. } | Self::AlreadyFired { .. } | Self::InProgress { .. } => {
                REASON_SCHEDULED
            }
        }
    }

    /// The `Ready` condition's `status` for this decision.
    ///
    /// `True` MEANS "THE SCHEDULE IS UNDERSTOOD AND BEING HONOURED", which
    /// includes [`Self::Missed`]: skipping a stale slot IS the honoured
    /// behaviour. `False` is for the three states in which this schedule will
    /// not fire as written — suspended, unparseable and unnameable.
    ///
    /// THE ARGUMENT FOR `Self::Missed` USED TO BE "a daily schedule spends 23
    /// of 24 hours with its latest slot outside the horizon", and that
    /// sentence was a symptom of review finding HIGH-1 rather than a reason:
    /// a healthy daily schedule now reports [`Self::AlreadyFired`] there, so
    /// `Self::Missed` is once again the rare event it names. `True` remains
    /// its status — a skip that is recorded, reasoned and messaged is a
    /// schedule doing what it was told, and `Ready=False` is reserved for the
    /// states an operator has to act on.
    #[must_use]
    pub fn ready(&self) -> bool {
        match self {
            Self::Due { .. }
            | Self::AlreadyFired { .. }
            | Self::InProgress { .. }
            | Self::Missed { .. }
            | Self::ConcurrencyBlocked { .. }
            | Self::CatchUpDue { .. }
            | Self::CaughtUp { .. }
            | Self::CatchUpBlocked { .. }
            | Self::Retried { .. }
            | Self::RetryPending { .. }
            | Self::RetryBlocked { .. }
            | Self::RetryExhausted { .. }
            | Self::RunFailed { .. }
            | Self::SlotNameUnavailable { .. }
            | Self::ActiveRunLimit { .. } => true,
            Self::Suspended
            | Self::Unparseable(_)
            | Self::UnknownTimeZone { .. }
            | Self::InvalidTopicSelection { .. }
            | Self::InvalidRunPolicy { .. }
            | Self::NoDueSlot { .. }
            | Self::CrdOutdated { .. } => false,
            Self::NameTooLong { .. } | Self::RetryNamesTooLong { .. } => false,
        }
    }

    /// When this schedule next fires, as this decision computed it.
    #[must_use]
    pub fn next_fire_time(&self) -> Option<DateTime<Utc>> {
        match self {
            // A SUSPENDED SCHEDULE HAS NO NEXT FIRING, and reporting the
            // instant the cron expression would have chosen would put a future
            // timestamp in `kubectl get`'s NEXT column for a schedule that is
            // not going to fire.
            Self::Suspended
            | Self::Unparseable(_)
            | Self::UnknownTimeZone { .. }
            | Self::InvalidTopicSelection { .. }
            | Self::InvalidRunPolicy { .. }
            | Self::RetryNamesTooLong { .. } => None,
            Self::NoDueSlot { next_fire_time }
            | Self::AlreadyFired { next_fire_time, .. }
            | Self::InProgress { next_fire_time, .. }
            | Self::Missed { next_fire_time, .. }
            | Self::ConcurrencyBlocked { next_fire_time, .. }
            | Self::CatchUpDue { next_fire_time, .. }
            | Self::CaughtUp { next_fire_time, .. }
            | Self::CatchUpBlocked { next_fire_time, .. }
            | Self::Retried { next_fire_time, .. }
            | Self::RetryPending { next_fire_time, .. }
            | Self::RetryBlocked { next_fire_time, .. }
            | Self::RetryExhausted { next_fire_time, .. }
            | Self::RunFailed { next_fire_time, .. }
            | Self::SlotNameUnavailable { next_fire_time, .. }
            | Self::ActiveRunLimit { next_fire_time, .. }
            | Self::CrdOutdated { next_fire_time, .. }
            | Self::NameTooLong { next_fire_time, .. }
            | Self::Due { next_fire_time, .. } => *next_fire_time,
        }
    }

    /// How long until this schedule should be looked at again (D1 §4.5 step
    /// 8).
    ///
    /// `min(REQUEUE_SECS, whatever this decision is waiting for)`. The only
    /// decision that waits for something sooner than the next poll is a retry
    /// serving out its delay; everything else is either idle or waiting for a
    /// clock tick the poll already covers. The value is never longer than the
    /// poll, so a bug here can cost a wasted wake-up and never a missed slot.
    #[must_use]
    pub fn requeue_after(&self, now: DateTime<Utc>) -> std::time::Duration {
        let poll = std::time::Duration::from_secs(REQUEUE_SECS);
        match self {
            Self::RetryPending { due_at, .. } => due_at
                .signed_duration_since(now)
                .to_std()
                .map_or(poll, |wait| wait.min(poll)),
            _ => poll,
        }
    }

    /// The condition `message`: what happened, and what happens next.
    #[must_use]
    pub fn message(&self) -> String {
        let next = render_next(self.next_fire_time());
        match self {
            Self::Suspended => {
                "spec.suspend is true, so no Backup is created and there is no next firing; \
                 suspend is the one mutable field on this spec"
                    .to_string()
            }
            Self::Unparseable(e) => format!("spec.schedule does not parse: {e}"),
            // NAMES EVERY PROBLEM, NOT THE FIRST. An operator who fixes one of
            // three mistakes and waits thirty seconds to learn about the next
            // has been told three times to submit three times.
            Self::InvalidTopicSelection { errors } => format!(
                "the topic selection is not usable and no slot will be admitted until it is \
                 fixed: {}",
                render_field_errors(errors)
            ),
            Self::InvalidRunPolicy { errors } => format!(
                "the run policy is not usable and no slot will be admitted until it is fixed: \
                 {}",
                render_field_errors(errors)
            ),
            Self::NoDueSlot { .. } => format!(
                "spec.schedule has no firing at or before now, so nothing is due; the next \
                 firing is {next}"
            ),
            // WHAT THE HEALTHY STEADY STATE SAYS, and it says the fired slot
            // and the instant it was fired rather than repeating "nothing is
            // due": an adopter looking at a nightly backup at 10:00 wants to
            // read that midnight's slot ran.
            Self::AlreadyFired {
                slot,
                last_fire_time,
                ..
            } => format!(
                "slot {slot} was fired at {}; nothing further is due, and the next firing is \
                 {next}",
                last_fire_time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
            ),
            // VERBATIM FROM THE BRIEF. This sentence is the whole of critique B
            // M20's remedy: it names the slot, names the horizon, says the slot
            // was NOT fired, and says when the schedule resumes.
            Self::Missed { slot, reason, .. } => match reason {
                MissedReason::PastStartingDeadline => format!(
                    "slot {slot} is past its starting deadline and was not fired; \
                     catchUpPolicy is None, so it is counted in status.missedSlots and the next \
                     firing is {next}"
                ),
                // A CATCH-UP THAT WOULD PREDATE THE POLICY IT RAN UNDER is not
                // a catch-up an operator asked for: editing a schedule at noon
                // must not retroactively back up the morning under the new
                // policy.
                MissedReason::BeforeRevision => format!(
                    "slot {slot} came due before the revision now in force, so it is not caught \
                     up; the next firing is {next}"
                ),
            },
            Self::ConcurrencyBlocked {
                slot,
                active_backups,
                ..
            } => format!(
                "slot {slot} was not fired because concurrencyPolicy Forbid found unfinished or \
                 unknown owned Backup(s) {}; the next firing is {next}",
                active_backups.join(", ")
            ),
            Self::NameTooLong { slot, error, .. } => format!(
                "slot {slot} is due but no Backup could be named for it: {error}; the next \
                 firing is {next}"
            ),
            Self::Due { slot, name, .. } => {
                format!("slot {slot} is due and its Backup is {name}; the next firing is {next}")
            }
            Self::UnknownTimeZone { got } => format!(
                "spec.timeZone `{}` is not a zone this build's database ({}) has, so no slot can \
                 be computed and nothing is admitted; a zone nobody can resolve is never read as \
                 UTC",
                shown(got),
                crate::cadence::TZDB_SOURCE
            ),
            Self::RetryNamesTooLong { error } => format!(
                "spec.retry asks for retries this schedule cannot name: {error}; no slot is \
                 admitted, because a schedule that looks configured and silently never retries \
                 is worse than one that says so"
            ),
            Self::CatchUpDue { slot, name, .. } | Self::CaughtUp { slot, name, .. } => format!(
                "slot {slot} is past its starting deadline and catchUpPolicy is Latest, so it \
                 runs once as {name}; the next firing is {next}"
            ),
            Self::CatchUpBlocked {
                slot,
                active_backups,
                ..
            } => format!(
                "slot {slot} is waiting to be caught up: {} is still unfinished; the next firing \
                 is {next}",
                active_backups.join(", ")
            ),
            Self::InProgress {
                slot,
                attempt,
                name,
                ..
            } => format!(
                "slot {slot} attempt {attempt} is running as {name}; the next firing is {next}"
            ),
            Self::Retried {
                slot,
                name,
                attempt,
                ..
            } => format!(
                "slot {slot} failed and is being retried as attempt {attempt}, {name}; the next \
                 firing is {next}"
            ),
            Self::RetryPending {
                slot,
                attempt,
                due_at,
                ..
            } => format!(
                "slot {slot} attempt {attempt} failed; its retry becomes admissible at {}, and \
                 the next firing is {next}",
                due_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
            ),
            Self::RetryBlocked {
                slot,
                attempt,
                active_backups,
                ..
            } => format!(
                "slot {slot} attempt {attempt} is ready to retry but {} is still unfinished; the \
                 next firing is {next}",
                active_backups.join(", ")
            ),
            Self::RetryExhausted {
                slot,
                attempt,
                max_retries,
                retry_configured,
                ..
            } => {
                if *retry_configured {
                    format!(
                        "slot {slot} failed after {attempt} of {max_retries} retries and will \
                         not be retried again; the next firing is {next}"
                    )
                } else {
                    format!(
                        "slot {slot} failed and spec.retry is not configured, so it is not \
                         retried; the next firing is {next}"
                    )
                }
            }
            Self::RunFailed {
                slot,
                attempt,
                name,
                ..
            } => format!(
                "slot {slot} attempt {attempt} ({name}) failed for a reason that is never \
                 retried — a refusal is a decision, not a blip; the next firing is {next}"
            ),
            Self::SlotNameUnavailable { slot, name, .. } => format!(
                "slot {slot} cannot run because {name} is held by an object this schedule does \
                 not own; the next firing is {next}"
            ),
            Self::ActiveRunLimit {
                slot,
                active_backups,
                ..
            } => format!(
                "slot {slot} was not admitted because {} schedule-created runs of this schedule \
                 are already unfinished under concurrencyPolicy Allow; the next firing is {next}",
                active_backups.len()
            ),
            Self::CrdOutdated { detail, .. } => format!(
                "the installed CustomResourceDefinition is older than this controller: {detail}. \
                 Nothing further is admitted, because a run created under a pruned identity \
                 would execute without one. Apply the CRDs, then roll the controller."
            ),
        }
    }
}

/// Every field error in one sentence, bounded so a condition message stays a
/// message.
///
/// `metav1.Condition.message` IS 32 KiB AND AN OPERATOR IS NOT. Ten problems is
/// already more than anyone reads in a `kubectl describe`; the eleventh and
/// beyond are counted rather than printed, and the API's 422 carries the full
/// list.
fn render_field_errors(errors: &[FieldError]) -> String {
    const SHOWN: usize = 10;
    let mut parts: Vec<String> = errors
        .iter()
        .take(SHOWN)
        .map(|e| format!("{}: {}", e.field, e.message))
        .collect();
    if errors.len() > SHOWN {
        parts.push(format!("and {} more", errors.len() - SHOWN));
    }
    parts.join("; ")
}

/// An optional next-firing instant, as a condition message spells it.
fn render_next(next: Option<DateTime<Utc>>) -> String {
    match next {
        Some(t) => t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        // NAMED, NOT BLANK. An empty string in the middle of a sentence reads
        // as a formatting bug; "none" reads as the fact it is.
        None => "none".to_string(),
    }
}

/// Decide what one `BackupSchedule` needs, including the object name.
///
/// A PURE FUNCTION OF ITS THREE ARGUMENTS. It reads no clock — `now` is handed
/// in — no `status`, and no environment, which is what makes the returned
/// [`SlotDecision::Due::name`] a pure function of the trigger. Two reconciles
/// at two different instants inside one slot compute the same `due`, the same
/// `slot` and therefore the same `name`; the second `POST` collides and the
/// 409 is the idempotence key.
///
/// The steps are D1 §4.5's, in order: `suspend` first (so a suspended
/// schedule's expression is never even parsed into a firing), then the whole of
/// step 0's policy validation, then the due slot, then the name, then the
/// starting deadline and the catch-up policy.
///
/// # What it deliberately cannot answer
///
/// Two of D1 §4.7's rows need the status: whether a past-deadline catch-up
/// predates the revision in force (row 19), and whether a slot with no object
/// was nonetheless already fired (row 14). Both are [`refine_against_status`]'s,
/// which takes instants rather than an object and mints no name — see its own
/// note for why that boundary is a second function and not a fourth argument.
#[must_use]
pub fn decide(name: &str, spec: &BackupScheduleSpec, now: DateTime<Utc>) -> SlotDecision {
    if spec.suspend {
        return SlotDecision::Suspended;
    }
    // D1 §4.5 STEP 0, AND IT IS BEFORE THE CRON PARSE ON PURPOSE. A schedule
    // whose topic selection cannot produce a run has nothing to admit whatever
    // its cadence says, and reporting the cadence problem first would send an
    // operator to fix the field that is not broken.
    if let Err(errors) = crate::policy::validate_topic_selection(&run_policy_spec(spec)) {
        return SlotDecision::InvalidTopicSelection { errors };
    }
    if let Err(errors) = crate::policy::validate_run_policy(&run_policy_spec(spec)) {
        return SlotDecision::InvalidRunPolicy { errors };
    }
    let cadence = match Cadence::parse(&spec.schedule, spec.time_zone.as_deref()) {
        Ok(cadence) => cadence,
        Err(CadenceError::Schedule(e)) => return SlotDecision::Unparseable(e),
        Err(CadenceError::UnknownTimeZone { got }) => return SlotDecision::UnknownTimeZone { got },
    };
    // THE CRD'S RANGES ARE THE FIRST GATE AND NOT THE ONLY ONE. An object
    // created against an older CRD, or read after the API server pruned a
    // field it does not declare, reaches here without having passed them.
    if let Err(problem) = crate::cadence::validate_deadlines(
        spec.starting_deadline_seconds,
        spec.active_deadline_seconds,
    ) {
        return SlotDecision::InvalidRunPolicy {
            errors: vec![deadline_error(&problem)],
        };
    }
    let retry = spec.retry.map(|r| r.policy());
    if let Some(policy) = retry {
        if let Err(problem) = policy.validate() {
            return SlotDecision::InvalidRunPolicy {
                errors: vec![retry_error(&problem)],
            };
        }
        // D1 §3.1 RULE 6. The CRD's root rule R3 refuses the EDIT; this is the
        // defence for an object admitted by a CRD that did not carry it.
        if let Err(error) = crate::slot::retry_names_fit(name, policy.max_retries) {
            return SlotDecision::RetryNamesTooLong { error };
        }
    }

    let next_fire_time = cadence.next_fire_after(now);
    let Some(due) = cadence.latest_due_slot(now) else {
        return SlotDecision::NoDueSlot { next_fire_time };
    };
    let slot = slot_name(due);
    let object_name = match scheduled_backup_name(name, &slot) {
        Ok(object_name) => object_name,
        Err(error) => {
            return SlotDecision::NameTooLong {
                slot,
                error,
                next_fire_time,
            }
        }
    };
    // `effective_since` IS `None` HERE AND THAT IS THE POINT: it is a status
    // field, so the `BeforeRevision` arm belongs to the refinement below.
    match crate::cadence::admit_slot(
        due,
        now,
        starting_deadline_seconds(spec),
        catch_up_policy(spec),
        None,
    ) {
        SlotAdmission::Scheduled => SlotDecision::Due {
            due,
            slot,
            name: object_name,
            next_fire_time,
        },
        SlotAdmission::CatchUp => SlotDecision::CatchUpDue {
            due,
            slot,
            name: object_name,
            next_fire_time,
        },
        SlotAdmission::Missed { reason } => SlotDecision::Missed {
            due,
            slot,
            reason,
            next_fire_time,
        },
    }
}

/// `spec.startingDeadlineSeconds`, with D1 §4.1's documented default.
#[must_use]
pub fn starting_deadline_seconds(spec: &BackupScheduleSpec) -> i64 {
    spec.starting_deadline_seconds
        .unwrap_or(crate::cadence::DEFAULT_STARTING_DEADLINE_SECONDS)
}

/// `spec.catchUpPolicy`, with D1 §4.1's documented default.
#[must_use]
pub fn catch_up_policy(spec: &BackupScheduleSpec) -> CadenceCatchUp {
    spec.catch_up_policy.unwrap_or_default().policy()
}

fn deadline_error(problem: &crate::cadence::DeadlineProblem) -> FieldError {
    let field = match problem {
        crate::cadence::DeadlineProblem::StartingOutOfRange { .. } => {
            "spec.startingDeadlineSeconds"
        }
        crate::cadence::DeadlineProblem::ActiveOutOfRange { .. } => "spec.activeDeadlineSeconds",
    };
    FieldError {
        field: field.to_string(),
        rule: "cadence",
        message: problem.to_string(),
    }
}

fn retry_error(problem: &crate::cadence::RetryPolicyProblem) -> FieldError {
    let field = match problem {
        crate::cadence::RetryPolicyProblem::MaxRetriesTooHigh { .. } => "spec.retry.maxRetries",
        crate::cadence::RetryPolicyProblem::DelayOutOfRange { .. } => "spec.retry.delaySeconds",
    };
    FieldError {
        field: field.to_string(),
        rule: "cadence",
        message: problem.to_string(),
    }
}

/// Refine a decision's **reporting** against the two status instants D1 §4.7
/// rows 14 and 19 need.
///
/// A SLOT IS MISSED ONLY IF IT WAS NEVER FIRED. [`decide`] cannot know that —
/// it reads no status, which is exactly what makes the object name a pure
/// function of the trigger — so the questions that need the status are asked
/// here, after the name has already been minted:
///
/// 1. A past-deadline slot ([`SlotDecision::Missed`] or
///    [`SlotDecision::CatchUpDue`]) at or before `last_fire_time` becomes
///    [`SlotDecision::AlreadyFired`]. This is the healthy steady state of every
///    schedule slower than its starting deadline.
/// 2. A [`SlotDecision::CatchUpDue`] older than `effective_since` becomes
///    `Missed` with reason `BeforeRevision` (row 19): a schedule edited at noon
///    must not retroactively back up the morning under the new policy.
///
/// Every other decision is returned untouched.
///
/// # This function cannot endanger G-SLOT, and here is why
///
/// It takes instants and not a `BackupSchedule`, so there is no status field it
/// could reach beyond the two; it never constructs [`SlotDecision::Due`] or
/// [`SlotDecision::CatchUpDue`], so it cannot mint or change a name; and the
/// name it carries forward was minted by [`decide`] from `(name, spec, now)`.
/// The mutant "derive the name from `status.lastFireTime`" is still killed at
/// assertion time by
/// `a_crash_between_create_and_status_write_yields_exactly_one_backup`, and
/// `the_name_never_reads_a_reconcile_clock_or_a_status` reads this function's
/// own body to assert it calls no name function.
///
/// # Why not two more arguments to `decide`
///
/// Because then the pure function that mints the name would hold the status,
/// and the source-reading guard that says it must not would have to be
/// weakened to a guard about how the status is USED — a property no test in
/// this file can read. A second function is a boundary a test can see.
#[must_use]
pub fn refine_against_status(
    decision: SlotDecision,
    last_fire_time: Option<DateTime<Utc>>,
    effective_since: Option<DateTime<Utc>>,
) -> SlotDecision {
    let already_fired = |due: DateTime<Utc>| last_fire_time.filter(|last| due <= *last);
    match decision {
        SlotDecision::Missed {
            due,
            slot,
            reason,
            next_fire_time,
        } => match already_fired(due) {
            Some(last) => SlotDecision::AlreadyFired {
                due,
                slot,
                last_fire_time: last,
                next_fire_time,
            },
            None => SlotDecision::Missed {
                due,
                slot,
                reason,
                next_fire_time,
            },
        },
        SlotDecision::CatchUpDue {
            due,
            slot,
            name,
            next_fire_time,
        } => {
            if let Some(last) = already_fired(due) {
                return SlotDecision::AlreadyFired {
                    due,
                    slot,
                    last_fire_time: last,
                    next_fire_time,
                };
            }
            match effective_since {
                Some(since) if due < since => SlotDecision::Missed {
                    due,
                    slot,
                    reason: MissedReason::BeforeRevision,
                    next_fire_time,
                },
                _ => SlotDecision::CatchUpDue {
                    due,
                    slot,
                    name,
                    next_fire_time,
                },
            }
        }
        other => other,
    }
}

/// One observed attempt of a slot, reduced to what the retry decision reads.
///
/// OBSERVED DETERMINISTIC OBJECTS, NOT A STATUS FIELD OR A CLOCK — D1 §3.1
/// rule 7. The attempt chain is discovered by `GET`ting the names
/// `name(S, 0)`, `name(S, 1)`, … and stopping at the first 404, never by
/// listing and never by reading a counter the controller wrote itself. A
/// counter can disagree with the objects; the objects cannot disagree with
/// themselves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservedAttempt {
    /// The `Backup`'s name.
    pub name: String,
    /// Which attempt it is, `0..=3`.
    pub attempt: u32,
    /// Its terminal record, or `None` while it is still going.
    pub outcome: Option<AttemptOutcome>,
}

/// What an attempt's terminal record says.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AttemptOutcome {
    /// Whether the run reached `Succeeded`.
    pub succeeded: bool,
    /// Whether the failure is one D1 §4.6 retries.
    pub retryable: bool,
    /// When the terminal record was written — the `Failed` condition's
    /// `lastTransitionTime`, which is written once by the terminal patch.
    pub finished_at: DateTime<Utc>,
}

/// What the attempt chain of the latest due slot says should happen next.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttemptDecision {
    /// No attempt of this slot exists. The caller falls through to D1 §4.5
    /// step 7.
    NoAttempt,
    /// Some attempt succeeded. Nothing further is due for this slot (6a).
    Succeeded {
        /// The attempt that succeeded.
        name: String,
        /// Which attempt it was.
        attempt: u32,
    },
    /// The highest attempt has no terminal record yet (6b).
    InProgress {
        /// Its name.
        name: String,
        /// Which attempt it is.
        attempt: u32,
    },
    /// The highest attempt failed in a way that is never retried (6c).
    NotRetryable {
        /// Its name.
        name: String,
        /// Which attempt it was.
        attempt: u32,
    },
    /// The chain reached the configured ceiling (6d).
    Exhausted {
        /// The last attempt.
        attempt: u32,
        /// The ceiling.
        max_retries: u32,
        /// Whether `spec.retry` was configured at all.
        retry_configured: bool,
    },
    /// A retry is admissible but its delay has not elapsed (6e).
    Pending {
        /// The attempt that failed.
        attempt: u32,
        /// When the retry becomes admissible.
        due_at: DateTime<Utc>,
    },
    /// A retry is due; create it (6g). Concurrency is the caller's question.
    Retry {
        /// The plan for the new attempt.
        plan: RunPlan,
        /// The attempt it retries.
        retry_of: String,
    },
    /// The retry's name does not fit.
    NameTooLong {
        /// The refusal.
        error: SlotError,
    },
}

/// D1 §4.5 step 6: what the observed attempt chain of the latest due slot says.
///
/// A SECOND PURE FUNCTION, AND THAT IS D1 §3.1 RULE 7's WHOLE POINT. [`decide`]
/// mints only the attempt-0 name from `(name, spec, now)`; this one mints retry
/// names from OBSERVED OBJECTS — the chain the caller `GET`s — plus the retry
/// policy and the instant. Neither reads a status counter, so a controller that
/// crashed between creating `-r1` and recording it still sees exactly one `-r1`
/// on restart, and a `maxRetries` lowered by an edit still sees the attempts
/// that already exist.
///
/// `observed` must be the chain in attempt order with no gaps; the caller
/// builds it by `GET`ting deterministic names and stopping at the first 404.
#[must_use]
pub fn decide_attempt(
    schedule_name: &str,
    slot: DateTime<Utc>,
    observed: &[ObservedAttempt],
    retry: Option<RetryPolicy>,
    now: DateTime<Utc>,
) -> AttemptDecision {
    let Some(highest) = observed.last() else {
        return AttemptDecision::NoAttempt;
    };
    // 6a FIRST. A succeeded attempt ends the slot whatever a later object says
    // — and a later object can only exist because an earlier attempt failed, so
    // "any succeeded" and "the last succeeded" agree in every chain this
    // controller can create. Asking the whole chain is the shape that stays
    // right if a human creates one by hand.
    if let Some(done) = observed
        .iter()
        .find(|a| a.outcome.is_some_and(|o| o.succeeded))
    {
        return AttemptDecision::Succeeded {
            name: done.name.clone(),
            attempt: done.attempt,
        };
    }
    let Some(outcome) = highest.outcome else {
        return AttemptDecision::InProgress {
            name: highest.name.clone(),
            attempt: highest.attempt,
        };
    };
    // `Some(slot)` AS THE LATEST DUE SLOT, because the caller only ever hands
    // this function the latest due slot; supersession (row 22) is the skipped
    // -slot accounting's, not this function's.
    match crate::cadence::admit_retry(
        slot,
        Some(slot),
        highest.attempt,
        outcome.retryable,
        outcome.finished_at,
        retry,
        now,
    ) {
        RetryAdmission::NotRetryable => AttemptDecision::NotRetryable {
            name: highest.name.clone(),
            attempt: highest.attempt,
        },
        RetryAdmission::Exhausted {
            attempt,
            max_retries,
            retry_configured,
        } => AttemptDecision::Exhausted {
            attempt,
            max_retries,
            retry_configured,
        },
        RetryAdmission::Pending { due_at } => AttemptDecision::Pending {
            attempt: highest.attempt,
            due_at,
        },
        RetryAdmission::Due { attempt } => {
            match crate::slot::scheduled_backup_name_for_attempt(
                schedule_name,
                &slot_name(slot),
                attempt,
            ) {
                Ok(name) => AttemptDecision::Retry {
                    plan: RunPlan {
                        slot: slot_name(slot),
                        name,
                        kind: TriggerKind::Retry,
                        attempt,
                        retry_of: Some(highest.name.clone()),
                    },
                    retry_of: highest.name.clone(),
                },
                Err(error) => AttemptDecision::NameTooLong { error },
            }
        }
        // UNREACHABLE BY CONSTRUCTION, AND NAMED RATHER THAN WILDCARDED: the
        // caller passes the slot as its own latest due slot, so `latest > slot`
        // is false. A wildcard here would silently absorb a future caller that
        // passes a different slot.
        RetryAdmission::Superseded { by } => AttemptDecision::Pending {
            attempt: highest.attempt,
            due_at: by,
        },
    }
}

/// What one attempt's terminal record says, or `None` while it is still going.
///
/// D1 §4.6's classification, read off the object. `finished_at` is the `Failed`
/// condition's `lastTransitionTime`, which the terminal patch writes once; a
/// terminal object with no such condition falls back to nothing retryable,
/// because a retry delay measured from an instant nobody recorded is a retry
/// that fires at an arbitrary time.
#[must_use]
pub fn attempt_outcome(backup: &Backup) -> Option<AttemptOutcome> {
    let status = backup.status.as_ref()?;
    let phase = status.phase.as_deref()?;
    if !matches!(phase, "Succeeded" | "Failed" | "Refused") {
        return None;
    }
    if phase == "Succeeded" {
        return Some(AttemptOutcome {
            succeeded: true,
            retryable: false,
            finished_at: terminal_instant(backup).unwrap_or_default(),
        });
    }
    // NO TERMINAL CONDITION MEANS NOT RETRYABLE, AND IT IS A DECISION RATHER
    // THAN A GAP. The retry delay is measured from the instant the attempt
    // finished, and that instant is the `Failed` condition's
    // `lastTransitionTime`, written once by the terminal patch. A terminal
    // object without one — written by an older controller, or by hand — gives
    // no instant to measure from, so a retry would fire at an arbitrary time.
    // The run is still TERMINAL: reporting it as in progress instead would
    // block every later slot of a `Forbid` schedule forever.
    let finished_at = terminal_instant(backup);
    let retryable = finished_at.is_some()
        && crate::cadence::is_retryable(crate::cadence::TerminalOutcome {
            exit_code: status.exit_code,
            terminal_state: status.exit_reason.as_deref(),
        });
    Some(AttemptOutcome {
        succeeded: false,
        retryable,
        finished_at: finished_at.unwrap_or_default(),
    })
}

/// The `lastTransitionTime` of a `Backup`'s terminal condition.
fn terminal_instant(backup: &Backup) -> Option<DateTime<Utc>> {
    let conditions = backup.status.as_ref()?.conditions.as_ref()?;
    conditions
        .iter()
        .find(|c| {
            (c.r#type == crate::conditions::CONDITION_FAILED
                || c.r#type == crate::conditions::CONDITION_COMPLETE)
                && c.status == "True"
        })
        .and_then(|c| c.last_transition_time)
}

/// The policy half of the `Backup` this schedule would create — everything
/// that decides WHAT a run does, and none of the identity that decides which
/// run it is.
///
/// ONE BUILDER, THREE READERS. [`decide`] validates this (D1 §4.5 step 0),
/// [`run_policy_digest`] digests it, and [`scheduled_backup`] fills in the
/// identity and POSTs it. A second construction anywhere is how a schedule
/// comes to admit a policy the run then refuses, or to record a digest over
/// fields the run does not carry.
///
/// `deadlineSeconds` RESOLVES THE ABSENT DEFAULT HERE, because D1 §3.2 puts
/// `activeDeadlineSeconds` inside the digested document: a schedule that says
/// nothing and a schedule that says 3600 ask for the same run and must digest
/// the same.
#[must_use]
pub fn run_policy_spec(spec: &BackupScheduleSpec) -> BackupSpec {
    BackupSpec {
        source_ref: spec.source_ref.clone(),
        topics: spec.topics.clone(),
        archive: spec.archive.clone(),
        destination_ref: spec.destination_ref.clone(),
        all_user_topics: spec.all_user_topics.clone(),
        schedule_ref: None,
        slot: None,
        triggered_by: TRIGGERED_BY_SCHEDULE.to_string(),
        trigger: None,
        deadline_seconds: spec
            .active_deadline_seconds
            .unwrap_or(SCHEDULED_DEADLINE_SECONDS),
    }
}

/// `sha256:<lowercase hex>` over this schedule's run policy (D1 §3.2).
///
/// It covers the source, the topic selection, the archive and the run deadline
/// — what a run DOES. Cadence, zone, deadlines, catch-up, retry, concurrency,
/// retention and `suspend` are excluded, so flipping `suspend` moves
/// `metadata.generation` and visibly leaves this alone.
#[must_use]
pub fn run_policy_digest(spec: &BackupScheduleSpec) -> String {
    crate::policy::run_policy_sha256(&run_policy_spec(spec))
}

/// The `Backup` object one due slot produces.
///
/// PURE, AND THAT IS WHY THE OWNER UID IS AN ARGUMENT. Everything here is a
/// function of `(schedule, schedule_uid, slot, name)`: the object a test builds
/// is byte-identical to the one the reconciler `POST`s, so an assertion over
/// this function is an assertion over the request.
///
/// # The revision, and why it comes from the object rather than an argument
///
/// D1 §5.4's atomic boundary says a `Backup` always runs exactly the generation
/// it records. The way to guarantee that is not to pass a generation in — a
/// caller could pass one it read somewhere else — but to take BOTH the policy
/// and the generation from the SAME in-memory `BackupSchedule` the reservation
/// was made against. `spec.scheduleRef.generation` is then
/// `schedule.metadata.generation` by construction and
/// `spec.scheduleRef.runPolicySha256` is the digest of the fields this very
/// object copied, which is what `identity::check_run_policy_digest` recomputes
/// and what `run_identity` refuses on a mismatch.
///
/// NO ANNOTATION. The run identity the `Backup` reconciler derives from this
/// object is `backup_id_for(schedule_uid, slot)`: `spec.triggeredBy: schedule`,
/// `spec.scheduleRef`, `spec.slot`, the controller owner reference below and
/// the deterministic `name` are exactly the facts
/// [`crate::backup_execution::execution_identity`] requires.
///
/// `ownerReferences` WITH `controller: true` AND `blockOwnerDeletion: true`, so
/// deleting the schedule garbage-collects its backups and a half-deleted
/// schedule cannot orphan them. `api_version` and `kind` come from the derive's
/// own [`Resource`] impl rather than from two string literals, so they cannot
/// drift from the CRD. **PLAT-05.2 (W4) is the task that removes this
/// reference**; until it lands, history is still owned.
#[must_use]
pub fn scheduled_backup(
    schedule: &BackupSchedule,
    schedule_uid: &str,
    slot: &str,
    name: &str,
) -> Backup {
    scheduled_run(schedule, schedule_uid, &RunPlan::scheduled(slot, name))
}

/// What one admission decided to create: which slot, under which trigger kind,
/// at which attempt, under which object name.
///
/// A VALUE AND NOT FOUR ARGUMENTS, because the four are not independent: a
/// retry's name, attempt and `retryOf` are one derivation
/// ([`crate::slot::scheduled_backup_name_for_attempt`]), and splitting them
/// across a call signature is how a `-r1` object comes to claim attempt 0.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunPlan {
    /// The slot, `yyyymmdd-hhmmss`.
    pub slot: String,
    /// The `Backup`'s deterministic name.
    pub name: String,
    /// Which kind of run this admission is.
    pub kind: TriggerKind,
    /// `0` for `Scheduled` and `CatchUp`, `1..=3` for `Retry`.
    pub attempt: u32,
    /// The attempt this one retries, for `Retry` only.
    pub retry_of: Option<String>,
}

impl RunPlan {
    /// Attempt 0 of a slot that fired at its own instant.
    #[must_use]
    pub fn scheduled(slot: &str, name: &str) -> Self {
        Self {
            slot: slot.to_string(),
            name: name.to_string(),
            kind: TriggerKind::Scheduled,
            attempt: 0,
            retry_of: None,
        }
    }

    /// `Scheduled`, `CatchUp` or `Retry`, as the status spells it.
    #[must_use]
    pub fn kind_name(&self) -> &'static str {
        match self.kind {
            TriggerKind::Scheduled => "Scheduled",
            TriggerKind::CatchUp => "CatchUp",
            TriggerKind::Retry => "Retry",
            TriggerKind::Manual => "Manual",
        }
    }

    /// The label value for [`crate::identity::TRIGGER_LABEL`].
    #[must_use]
    pub fn trigger_label(&self) -> &'static str {
        match self.kind {
            TriggerKind::Scheduled => "scheduled",
            TriggerKind::CatchUp => "catch-up",
            TriggerKind::Retry => "retry",
            TriggerKind::Manual => "manual",
        }
    }
}

/// The `Backup` object one admission produces, for any of the three
/// schedule-created trigger kinds.
///
/// See [`scheduled_backup`] for why the revision is read off the object.
#[must_use]
pub fn scheduled_run(schedule: &BackupSchedule, schedule_uid: &str, plan: &RunPlan) -> Backup {
    let schedule_name = schedule.name_any();
    let policy = run_policy_spec(&schedule.spec);
    let digest = crate::policy::run_policy_sha256(&policy);
    Backup {
        metadata: ObjectMeta {
            name: Some(plan.name.clone()),
            namespace: schedule.namespace(),
            labels: Some(
                [
                    (SCHEDULE_LABEL.to_string(), schedule_name.clone()),
                    (SCHEDULE_UID_LABEL.to_string(), schedule_uid.to_string()),
                    (SLOT_LABEL.to_string(), plan.slot.clone()),
                    (TRIGGER_LABEL.to_string(), plan.trigger_label().to_string()),
                    (ATTEMPT_LABEL.to_string(), plan.attempt.to_string()),
                ]
                .into_iter()
                .collect(),
            ),
            owner_references: Some(vec![OwnerReference {
                api_version: BackupSchedule::api_version(&()).to_string(),
                kind: BackupSchedule::kind(&()).to_string(),
                name: schedule_name.clone(),
                uid: schedule_uid.to_string(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]),
            ..ObjectMeta::default()
        },
        spec: BackupSpec {
            // `name` AND `uid`, because a schedule deleted and recreated under
            // the same name is a different schedule and must not adopt this
            // run; `generation` and `runPolicySha256` because PLAT-05.1 makes
            // the policy editable, so "which schedule" stopped answering
            // "under which policy".
            schedule_ref: Some(ScheduleRef {
                name: schedule_name,
                uid: Some(schedule_uid.to_string()),
                generation: schedule.metadata.generation,
                run_policy_sha256: Some(digest),
            }),
            slot: Some(plan.slot.clone()),
            trigger: Some(Trigger {
                kind: plan.kind,
                attempt: i32::try_from(plan.attempt).unwrap_or(i32::MAX),
                retry_of: plan
                    .retry_of
                    .as_ref()
                    .map(|name| LocalRef { name: name.clone() }),
                // INFORMATIONAL. The slot is a UTC instant whatever the zone;
                // this records the zone it was COMPUTED in, so a history row
                // keeps its local time after somebody edits `spec.timeZone`.
                time_zone: schedule.spec.time_zone.clone(),
            }),
            ..policy
        },
        status: None,
    }
}

/// Whether `backup` has this complete `BackupSchedule` identity as controller.
///
/// Labels and `spec.scheduleRef` are hints. The controller reference must match
/// API version, kind, name, UID, and `controller: true`; a schedule deleted and
/// recreated under the same name must not adopt the previous object's work.
#[must_use]
pub fn is_owned_by_schedule(backup: &Backup, schedule_name: &str, schedule_uid: &str) -> bool {
    backup
        .metadata
        .owner_references
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|owner| {
            owner.controller == Some(true)
                && owner.name == schedule_name
                && owner.uid == schedule_uid
                && owner.kind == BackupSchedule::kind(&())
                && owner.api_version == BackupSchedule::api_version(&())
        })
}

/// Whether `backup` is a run of this schedule that **participates in
/// `concurrencyPolicy`** (D1 §2, §8.3).
///
/// # Membership and accounting are two questions, and this is the second one
///
/// [`crate::identity::is_run_of_schedule`] answers "is this run part of this
/// schedule's history", and a MANUAL run created from a schedule is: it carries
/// `spec.scheduleRef {name, uid}`, it appears in the schedule's history view,
/// and PLAT-05.2's inventory and migration must see it. But D1 §2 defines a
/// *schedule-created run* as one whose trigger kind is `Scheduled`, `CatchUp`
/// or `Retry`, and **only those participate in `concurrencyPolicy`** — "Back up
/// now" follows the CronJob precedent and is neither blocked by a running slot
/// nor blocks the next one (D1 §0.1 item 7, §8.3).
///
/// Until now the distinction cost nothing, because accounting used the complete
/// controller ownerReference and a manual `Backup` has none. Moving accounting
/// onto `scheduleRef` — which PLAT-05.2 forces, since it removes that
/// ownerReference — would have made every manual run occupy a `Forbid` slot.
/// This function is where the two questions part.
///
/// # Why `declared_trigger` and not `run_identity`
///
/// [`crate::identity::declared_trigger`] is infallible and already applies D1
/// §3.1 rule 4 to legacy objects: a `Backup` with no `spec.trigger` and
/// `triggeredBy: schedule` reads as `Scheduled`/0, exactly as the controller
/// that created it did. `run_identity` can return `Err`, and a run whose
/// identity does not compose must still COUNT as active — dropping it would
/// weaken `Forbid` for precisely the malformed objects that most deserve it.
#[must_use]
pub fn participates_in_concurrency(
    backup: &Backup,
    schedule_name: &str,
    schedule_uid: &str,
) -> bool {
    is_run_of_schedule(backup, schedule_name, schedule_uid)
        && matches!(
            crate::identity::declared_trigger(backup).0,
            TriggerKind::Scheduled | TriggerKind::CatchUp | TriggerKind::Retry
        )
}

/// Whether the Backup CR has reached a phase that cannot run again.
///
/// Everything else, including an absent or future phase, is deliberately
/// nonterminal: `Forbid` treats unknown state conservatively. A missing Job is
/// not terminal because the Backup controller may recreate it.
#[must_use]
pub fn backup_is_terminal(backup: &Backup) -> bool {
    matches!(
        backup
            .status
            .as_ref()
            .and_then(|status| status.phase.as_deref()),
        Some("Succeeded" | "Failed" | "Refused")
    )
}

/// The `/status` **merge patch** one slot reservation is.
///
/// A PATCH AND NOT A PUT, AND THAT IS AN RBAC CONTRACT AS MUCH AS A
/// CONCURRENCY ONE. `Api::replace_status` issues `PUT`, which the API server
/// authorises as the verb `update` on `backupschedules/status`; the shipped
/// ClusterRole grants `patch` on the status subresources and deliberately
/// grants no `update` anywhere (`config/rbac/role.yaml`, and the chart and
/// install-file copies of it). A reservation sent as a replace is therefore
/// **403 on every shipped install** — with the default `concurrencyPolicy:
/// Forbid` that is every due slot of every schedule, so nothing is ever
/// created. The compare-and-set is not lost by moving to a patch: Kubernetes
/// applies a `metadata.resourceVersion` carried in a patch BODY as an update
/// precondition and answers a mismatch `409 Conflict`, which is the same
/// mechanism the final status write has always used — hence
/// [`status_patch_with_preconditions`] and not a second one here.
///
/// EXPLICIT `null`s, because merge-patch semantics make an absent key mean
/// "leave it alone" while the replaced object this used to build cleared
/// `activeBackupRef` by simply not carrying it. The two fields a reservation
/// decides — a cleared active reference and the accepted pending one — are
/// therefore both written, always.
fn reservation_patch(
    schedule: &BackupSchedule,
    decision: &SlotDecision,
    plan: &RunPlan,
    now: DateTime<Utc>,
) -> serde_json::Value {
    let backup_name = plan.name.as_str();
    let mut status = serde_json::Map::new();
    status.insert("activeBackupRef".to_string(), serde_json::Value::Null);
    status.insert(
        "pendingBackupRef".to_string(),
        json!(LocalRef {
            name: backup_name.to_string(),
        }),
    );
    // THE GENERATION TRAVELS WITH THE RESERVATION, AND THAT IS D1 §5.4. The
    // patch carries `metadata.resourceVersion`, so an edit that landed between
    // the read and this write makes the API server answer 409 and the reconcile
    // starts again under the new generation. When it is accepted, this number
    // is the generation the `Backup` created from the same in-memory object
    // records — so a run's copied policy always equals the schedule spec at the
    // generation written inside it.
    status.insert(
        "pendingRun".to_string(),
        json!(PendingRun {
            name: backup_name.to_string(),
            slot: plan.slot.clone(),
            attempt: i32::try_from(plan.attempt).unwrap_or(i32::MAX),
            kind: plan.kind_name().to_string(),
            generation: schedule.metadata.generation.unwrap_or_default(),
        }),
    );
    status.insert(
        "nextFireTime".to_string(),
        match decision.next_fire_time() {
            Some(t) => json!(t),
            None => serde_json::Value::Null,
        },
    );
    status.insert(
        "conditions".to_string(),
        json!([merge_condition(
            current_condition(
                schedule.status.as_ref().and_then(|s| s.conditions.as_ref()),
                CONDITION_READY,
            ),
            Condition {
                r#type: CONDITION_READY.to_string(),
                status: "True".to_string(),
                observed_generation: schedule.metadata.generation,
                last_transition_time: Some(now),
                reason: Some(REASON_SCHEDULED.to_string()),
                message: Some(format!(
                    "{}; concurrencyPolicy Forbid atomically admitted this slot and is creating the Backup",
                    decision.message()
                )),
            },
        )]),
    );
    json!({ "status": serde_json::Value::Object(status) })
}

/// The slot, instant and attempt a deterministic run name spells, or `None`
/// when the name is not one this schedule could have minted.
///
/// EXTENDED TO ACCEPT `-r<k>` (D1 §4.9). A reservation written as a bare
/// `pendingBackupRef` by an older controller carries no attempt, so the attempt
/// has to be recovered from the name — and a name with a retry suffix that this
/// parser did not understand would be read as "stale status", clearing a
/// reservation that had been accepted. The composed name is compared back
/// against the parse, so a name that is not `name(schedule, slot, attempt)` is
/// rejected rather than half-understood.
fn reserved_slot(schedule_name: &str, backup_name: &str) -> Option<(String, DateTime<Utc>, u32)> {
    let prefix = format!("{}{}-", crate::slot::SCHEDULED_BACKUP_PREFIX, schedule_name);
    let tail = backup_name.strip_prefix(&prefix)?;
    let (slot, attempt) = match tail.len() {
        crate::slot::SLOT_NAME_LEN => (tail, 0u32),
        _ => {
            let (slot, suffix) = tail.split_at_checked(crate::slot::SLOT_NAME_LEN)?;
            let attempt: u32 = suffix.strip_prefix("-r")?.parse().ok()?;
            (slot, attempt)
        }
    };
    if attempt > crate::slot::MAX_RETRIES
        || crate::slot::scheduled_backup_name_for_attempt(schedule_name, slot, attempt)
            .ok()
            .as_deref()
            != Some(backup_name)
    {
        return None;
    }
    let due = slot_instant(slot)?;
    Some((slot.to_string(), due, attempt))
}

/// Add optimistic-concurrency preconditions to a `/status` merge patch.
///
/// Kubernetes applies `metadata.resourceVersion` as an update precondition.
/// Including the object name as well makes the body self-identifying and keeps
/// the precondition tied to the same object named by the request path. A stale
/// finalizer therefore receives `409 Conflict` instead of merging scheduling
/// fields computed before a newer reservation.
fn status_patch_with_preconditions(
    schedule: &BackupSchedule,
    mut patch: serde_json::Value,
) -> Result<serde_json::Value, ScheduleError> {
    let name = schedule.name_any();
    let resource_version = schedule
        .metadata
        .resource_version
        .clone()
        .ok_or_else(|| ScheduleError::MissingResourceVersion(name.clone()))?;
    patch
        .as_object_mut()
        .expect("a status patch is always a JSON object")
        .insert(
            "metadata".to_string(),
            json!({
                "name": name,
                "resourceVersion": resource_version,
            }),
        );
    Ok(patch)
}

/// The `/status` merge patch one decision produces.
///
/// BUILT AS JSON RATHER THAN BY SERIALISING `BackupScheduleStatus`, and the
/// reason is merge-patch semantics. Every optional field on that struct carries
/// `skip_serializing_if = "Option::is_none"`, so a serialised `None` is an
/// ABSENT key — and an absent key in a merge patch means "leave it alone".
/// Clearing `nextFireTime` when a schedule is suspended therefore needs an
/// explicit JSON `null`, which only a hand-built body can carry.
///
/// WHAT IS DELIBERATELY NOT WRITTEN. `lastFireTime` is omitted unless a child
/// was created or observed for the decision. `activeBackupRef` and
/// `pendingBackupRef` are updated only when the API-facing reconciliation has
/// actual owned-run information; the public pure helper keeps them otherwise.
/// `lastMissedSlot` is written by [`SlotDecision::Missed`] and
/// [`SlotDecision::ConcurrencyBlocked`] and never cleared: it is the audit
/// trail of a skip. [`SlotDecision::AlreadyFired`] does not rewrite the fire
/// time, so a steady reconcile cannot move the timestamp an operator uses to
/// tell when the backup actually ran.
///
/// `lastTransitionTime` MOVES ONLY WHEN THE CONDITION TRANSITIONS — see
/// [`crate::conditions::merge_condition`], which is now the ONE
/// implementation of that comparison for all six reconcilers (plan erratum
/// E11(d); this file's private copy was one of four). This reconciler requeues
/// every [`REQUEUE_SECS`] seconds, so a bump-on-every-write would put 2,880
/// fresh transition timestamps a day on a schedule that never changed state
/// (fix round 1, review finding MED-1), which is both a lie about the
/// condition and 2,880 `resourceVersion` bumps every watcher in the cluster
/// has to receive. `retentionReport.evaluatedAt` obeys the same rule through
/// [`crate::crds::backup_schedule::RetentionReport::same_findings_as`].
#[must_use]
pub fn status_patch(
    schedule: &BackupSchedule,
    decision: &SlotDecision,
    created: Option<&str>,
    now: DateTime<Utc>,
) -> serde_json::Value {
    status_patch_with_retention(schedule, decision, created, None, now)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PendingRefUpdate {
    Keep,
    Clear,
}

/// [`status_patch`], plus the retention report (Task 19).
///
/// `None` OMITS THE KEY ENTIRELY, and that is deliberate. A `Merge` patch
/// carrying `retentionReport: null` would DELETE a report the controller had
/// already written, so a controller that lost its archive handle — an
/// unmounted credential, a removed environment variable — would erase the last
/// good report rather than leave it standing. An absent key says "this
/// reconcile has nothing to say about retention"; an absent BLOCK on the
/// object says "no evaluation has ever happened".
#[must_use]
pub fn status_patch_with_retention(
    schedule: &BackupSchedule,
    decision: &SlotDecision,
    created: Option<&str>,
    retention: Option<&RetentionReport>,
    now: DateTime<Utc>,
) -> serde_json::Value {
    let work = Reconcile {
        created: created.map(str::to_string),
        already_existed: false,
        fired_due: None,
        active: created
            .map(|name| {
                vec![ActiveRun {
                    name: name.to_string(),
                    kind: "Scheduled".to_string(),
                    attempt: 0,
                }]
            })
            .unwrap_or_default(),
        pending: PendingRefUpdate::Keep,
        last_slot: None,
        missed: schedule
            .status
            .as_ref()
            .and_then(|s| s.missed_slots.clone()),
        status_write_base: schedule.clone(),
    };
    status_patch_with_refs(schedule, decision, &work, retention, now)
}

/// The `status.policy` block this reconcile should carry, and whether
/// `observedGeneration` has to move (D1 §4.5 step 0).
///
/// # The three instants, and why only one of them is `now`
///
/// `generation`, `runPolicySha256` and `timeZone` are facts about the object.
/// `effectiveSince` is the instant THIS REVISION was first observed, so it must
/// be kept while the generation is unchanged — a catch-up decides against it
/// (D1 §4.7 row 19), and refreshing it every pass would make every catch-up
/// look "before the revision" forever.
///
/// `evaluatedAt` IS THE INSTANT THE STATUS LAST MOVED, NOT THE INSTANT THE
/// CONTROLLER LAST LOOKED, and that is a deliberate refinement of D1 §4.9. This
/// reconciler requeues every thirty seconds; an instant rewritten on every pass
/// would put 2,880 `resourceVersion` bumps a day on a schedule that never
/// changed state, wake this reconciler's own watch and spin it — the identical
/// defect that two unconditional `lastTransitionTime` writes and one
/// unconditional `retentionReport.evaluatedAt` already caused in this file
/// (plan erratum E11(d)). It is therefore built from the stored value, and the
/// caller rewrites it to `now` only when the status is being written anyway.
///
/// The consequence for readers: **`evaluatedAt` is not a liveness probe.** The
/// staleness signal D1 §4.9 wanted is `status.nextRuns[0].at` in the past by
/// more than a couple of requeue intervals — a live controller rewrites
/// `nextRuns` when its first entry passes, so a first entry that has gone stale
/// is a controller that stopped.
fn policy_status(schedule: &BackupSchedule, now: DateTime<Utc>) -> PolicyStatus {
    let generation = schedule.metadata.generation.unwrap_or_default();
    let stored = schedule.status.as_ref().and_then(|s| s.policy.as_ref());
    let unchanged = stored.filter(|p| p.generation == generation);
    let effective_since = unchanged.map_or(now, |p| p.effective_since);
    let evaluated_at = unchanged.map_or(now, |p| p.evaluated_at);
    PolicyStatus {
        generation,
        run_policy_sha256: run_policy_digest(&schedule.spec),
        time_zone: schedule
            .spec
            .time_zone
            .clone()
            .unwrap_or_else(|| crate::cadence::Zone::Utc.name().to_string()),
        tzdb: crate::cadence::TZDB_SOURCE.to_string(),
        effective_since,
        evaluated_at,
    }
}

/// D1 §4.4: the next five firings, or `[]` when this schedule will not fire.
///
/// COMPUTED IN RUST AND NOWHERE ELSE. The browser never evaluates cron — a
/// second implementation of a DST rule is a second answer to a question that
/// has to have one — so the previews are a status field, rendered rather than
/// derived by whoever reads them.
fn next_runs(schedule: &BackupSchedule, decision: &SlotDecision) -> Vec<NextRun> {
    let base = match decision.next_fire_time() {
        // A schedule that will not fire advertises no firings. An empty list is
        // the honest answer for `Suspended`, `UnknownTimeZone`, an invalid
        // policy and an unparseable expression alike.
        None => return Vec::new(),
        Some(base) => base,
    };
    let Ok(cadence) = Cadence::parse(&schedule.spec.schedule, schedule.spec.time_zone.as_deref())
    else {
        return Vec::new();
    };
    // `base - 1s` SO THE FIRST ENTRY IS `nextFireTime` ITSELF. `next_runs`
    // walks strictly after its argument, and a preview whose first row differs
    // from the NEXT column of `kubectl get` is a preview an operator distrusts.
    cadence
        .next_runs(
            base - Duration::seconds(1),
            crate::cadence::STATUS_NEXT_RUNS,
        )
        .into_iter()
        .map(|run| NextRun {
            at: run.at,
            local_time: run.local_time,
            adjustment: run.adjustment.map(|a| format!("{a:?}")),
        })
        .collect()
}

fn status_patch_with_refs(
    schedule: &BackupSchedule,
    decision: &SlotDecision,
    work: &Reconcile,
    retention: Option<&RetentionReport>,
    now: DateTime<Utc>,
) -> serde_json::Value {
    let mut status = serde_json::Map::new();
    // D1 §4.5 STEP 0 AND §5.3. Written on every final patch, so an operator can
    // always read which revision the controller acted on and what the run
    // policy of that revision digests to — a `suspend` flip moves `generation`
    // and visibly leaves `runPolicySha256` alone.
    status.insert(
        "observedGeneration".to_string(),
        json!(schedule.metadata.generation.unwrap_or_default()),
    );
    status.insert("policy".to_string(), json!(policy_status(schedule, now)));
    status.insert("nextRuns".to_string(), json!(next_runs(schedule, decision)));
    if let Some(report) = retention {
        // `evaluatedAt` IS KEPT WHEN THE FINDINGS ARE THE SAME — plan erratum
        // E11(d), review finding M-1. `evaluatedAt` is a "when computed" field,
        // so writing `now` into it on every pass made the whole status differ
        // on every pass, bumped `resourceVersion`, woke this reconciler's own
        // watch and spun it.
        let mut next = report.to_status();
        if let Some(previous) = schedule
            .status
            .as_ref()
            .and_then(|s| s.retention_report.as_ref())
        {
            if next.same_findings_as(previous) {
                next.evaluated_at = previous.evaluated_at;
            }
        }
        status.insert("retentionReport".to_string(), json!(next));
    }
    status.insert(
        "nextFireTime".to_string(),
        match decision.next_fire_time() {
            Some(t) => json!(t),
            // EXPLICIT NULL, NOT AN OMITTED KEY — see the note above.
            None => serde_json::Value::Null,
        },
    );
    // `lastMissedSlot` IS KEPT AS THE AUDIT FIELD IT ALWAYS WAS. It is written
    // when a slot is skipped or blocked and never cleared; `status.missedSlots`
    // is the richer block beside it, and the two agree because both are written
    // from the same decision.
    if matches!(
        decision,
        SlotDecision::Missed { .. }
            | SlotDecision::ConcurrencyBlocked { .. }
            | SlotDecision::CatchUpBlocked { .. }
            | SlotDecision::ActiveRunLimit { .. }
            | SlotDecision::SlotNameUnavailable { .. }
    ) {
        if let Some((_, slot)) = decision_slot(decision) {
            status.insert("lastMissedSlot".to_string(), json!(slot));
        }
    }
    if let Some(last) = work.last_slot.as_ref() {
        // `decidedAt` IS A "WHEN COMPUTED" FIELD, AND THEY ALL HAVE TO OBEY THE
        // SAME RULE — plan erratum E11(d), a third time. A steady schedule
        // reaches the same disposition for the same slot on every pass, so a
        // timestamp rewritten each time would make the whole status differ each
        // time and put 2,880 `resourceVersion` bumps a day on an object that
        // never changed. The instant moves when the DECISION moves.
        let mut next = last.clone();
        if let Some(previous) = schedule.status.as_ref().and_then(|s| s.last_slot.as_ref()) {
            let same = LastSlot {
                decided_at: next.decided_at,
                ..previous.clone()
            };
            if same == next {
                next.decided_at = previous.decided_at;
            }
        }
        status.insert("lastSlot".to_string(), json!(next));
    }
    if let Some(missed) = work.missed.as_ref() {
        status.insert("missedSlots".to_string(), json!(missed));
    }
    // `lastFireTime` IS THE DUE INSTANT, NEVER THE RECONCILE CLOCK. The
    // fallback keeps [`status_patch`]'s pure-function contract: a caller that
    // hands in an admitting decision and no observation still records the fire
    // that decision names. `AlreadyFired` is deliberately NOT in the list — a
    // steady reconcile must not move the timestamp an operator uses to tell
    // when the backup actually ran.
    if let Some(due) = work.fired_due.or(match decision {
        SlotDecision::Due { due, .. }
        | SlotDecision::CaughtUp { due, .. }
        | SlotDecision::Retried { due, .. } => Some(*due),
        _ => None,
    }) {
        status.insert("lastFireTime".to_string(), json!(due));
    }
    // `activeRuns` IS ALWAYS WRITTEN, `[]` INCLUDED, and that is what turns the
    // next reconcile into the O(active) branch instead of a namespace LIST. An
    // absent block means "never inventoried"; an empty one means "inventoried,
    // and there are none".
    status.insert("activeRuns".to_string(), json!(work.active));
    status.insert(
        "activeBackupRef".to_string(),
        match work.active.first() {
            Some(run) => json!({ "name": run.name }),
            None => serde_json::Value::Null,
        },
    );
    if work.pending == PendingRefUpdate::Clear {
        // BOTH KEYS, ALWAYS. `pendingRun` is the typed reservation and
        // `pendingBackupRef` is the mirror older readers use; clearing one and
        // leaving the other would present a reservation that no longer exists
        // to whichever reader looked at the wrong field.
        status.insert("pendingBackupRef".to_string(), serde_json::Value::Null);
        status.insert("pendingRun".to_string(), serde_json::Value::Null);
    }
    let ready = if decision.ready() { "True" } else { "False" };
    let reason = decision.reason();
    let mut conditions = vec![merge_condition(
        current_condition(
            schedule.status.as_ref().and_then(|s| s.conditions.as_ref()),
            CONDITION_READY,
        ),
        Condition {
            r#type: CONDITION_READY.to_string(),
            status: ready.to_string(),
            observed_generation: schedule.metadata.generation,
            last_transition_time: Some(now),
            reason: Some(reason.to_string()),
            message: Some(decision.message()),
        },
    )];
    // THE `HistoryRetained` SLOT, AND IT IS A DOCUMENTED NO-OP HERE.
    //
    // A JSON merge patch REPLACES `status.conditions`, so a builder that emits
    // only the conditions it owns DELETES every condition somebody else wrote —
    // the same class of defect `verification::carry_verified` fixes on Backups.
    // PLAT-05.2 (W4) owns `HistoryRetained` and the `schedule_history::observe`
    // call that computes it; until that lands this reconciler must not invent
    // one (claiming `Retained` while it still writes an ownerReference would be
    // false), and must not drop one either. So it carries forward whatever is
    // stored, unchanged, and W4 fills the slot by computing the condition here
    // instead of copying it.
    if let Some(retained) = current_condition(
        schedule.status.as_ref().and_then(|s| s.conditions.as_ref()),
        crate::conditions::CONDITION_HISTORY_RETAINED,
    ) {
        conditions.push(retained.clone());
    }
    status.insert("conditions".to_string(), json!(conditions));
    json!({ "status": serde_json::Value::Object(status) })
}

/// The slot a decision names, for the audit fields.
fn decision_slot(decision: &SlotDecision) -> Option<(Option<DateTime<Utc>>, String)> {
    match decision {
        SlotDecision::Missed { due, slot, .. } => Some((Some(*due), slot.clone())),
        SlotDecision::ConcurrencyBlocked { slot, .. }
        | SlotDecision::CatchUpBlocked { slot, .. }
        | SlotDecision::ActiveRunLimit { slot, .. }
        | SlotDecision::SlotNameUnavailable { slot, .. } => Some((None, slot.clone())),
        _ => None,
    }
}

/// What one reconcile did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScheduleOutcome {
    /// What was decided, including the object name for a due slot.
    pub decision: SlotDecision,
    /// The `Backup` name this reconcile created, or found already created.
    pub created: Option<String>,
    /// Whether the `POST` was answered **409 `AlreadyExists`** — the mechanism
    /// working, not a failure.
    pub already_existed: bool,
}

/// Anything that is not a decision. Requeues; writes nothing.
#[derive(Debug)]
pub enum ScheduleError {
    /// The object carries no `metadata.namespace`. Unreachable for an object
    /// that came from the API server; named rather than unwrapped.
    NoNamespace(String),
    /// The object carries no `metadata.uid`, so no `backup_id` and no owner
    /// reference can be built. Also unreachable from the API server, and also
    /// named: a `backup_id` built from a name instead would reintroduce the
    /// cross-namespace collision [`crate::slot::backup_id_for`] exists to prevent.
    NoUid(String),
    /// A status admission or finalization needs the API server's
    /// optimistic-concurrency token.
    MissingResourceVersion(String),
    /// The deterministic name for a due slot is already held by an object this
    /// schedule UID does not own.
    ForeignBackup(String),
    /// A write response came back without a field this controller wrote, which
    /// means the installed CRD is older than this controller (D1 §4.9).
    CrdOutdated(String),
    /// A status reservation could not be serialized.
    Serialization(serde_json::Error),
    /// The API server could not be talked to. Requeue.
    Api(kube::Error),
}

impl fmt::Display for ScheduleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoNamespace(name) => {
                write!(f, "the object {name} carries no metadata.namespace")
            }
            Self::NoUid(name) => write!(f, "the object {name} carries no metadata.uid"),
            Self::MissingResourceVersion(name) => write!(
                f,
                "the object {name} carries no metadata.resourceVersion required for status compare-and-swap"
            ),
            Self::ForeignBackup(name) => write!(
                f,
                "the deterministic Backup name {name} is held by an object that does not carry the complete current BackupSchedule controller identity"
            ),
            Self::CrdOutdated(detail) => write!(
                f,
                "the installed CustomResourceDefinition is older than this controller: {detail}. Apply the CRDs, then roll the controller."
            ),
            Self::Serialization(e) => write!(f, "could not serialize schedule status: {e}"),
            Self::Api(e) => write!(f, "kubernetes API error: {e}"),
        }
    }
}

impl std::error::Error for ScheduleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NoNamespace(_)
            | Self::NoUid(_)
            | Self::MissingResourceVersion(_)
            | Self::ForeignBackup(_)
            | Self::CrdOutdated(_) => None,
            Self::Serialization(e) => Some(e),
            Self::Api(e) => Some(e),
        }
    }
}

impl From<kube::Error> for ScheduleError {
    fn from(e: kube::Error) -> Self {
        Self::Api(e)
    }
}

impl From<serde_json::Error> for ScheduleError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serialization(e)
    }
}

/// Reconcile one `BackupSchedule` at the instant `now`.
///
/// NO `Utc::now()` IN THIS FUNCTION, AND THAT IS THE GUARD. The clock is read
/// once, by the `kube::runtime` wrapper below, before [`decide`] runs; from
/// there the instant is a value. A second clock read between the decision and
/// the `POST` is exactly the mutant
/// `a_crash_between_create_and_status_write_yields_exactly_one_backup` kills.
///
/// THE STATUS IS READ ONCE, AND ONLY FOR THE REASON. `status.lastFireTime`
/// reaches [`refine_against_last_fire`] and nothing else: it decides whether a
/// stale slot is `SlotMissed` or `AlreadyFired`, never what anything is called.
/// See that function's note for why the boundary is a second function.
///
/// THE ORDER IS `create` THEN `status`, AND A 409 IS SUCCESS. A duplicate
/// reconcile computes the same name, so the API server answers 409
/// `AlreadyExists`; that is logged at `debug` and never at `warn`, because it
/// is the mechanism working rather than a problem. The status write then
/// happens either way, which is what makes a crash in the window between the
/// two harmless.
///
/// # Errors
///
/// [`ScheduleError`] for anything that is not a decision.
pub async fn reconcile_schedule(
    schedule: &BackupSchedule,
    client: &kube::Client,
    now: DateTime<Utc>,
) -> Result<ScheduleOutcome, ScheduleError> {
    reconcile_schedule_with_archive(schedule, client, None, now).await
}

/// [`reconcile_schedule`], plus the controller's ONE read-only archive handle.
///
/// TASK 19. The retention **report** is refreshed on every reconcile, from the
/// same status write the slot decision produces, so
/// `kubectl get backupschedule -o yaml` shows both and the UI reads them with
/// no extra call. `archive` is `None` on a controller with no archive
/// configured, and then no `retentionReport` key is written at all — an absent
/// block says "no evaluation happened", which is a different claim from an
/// empty `setsThatWouldBeRemoved`.
///
/// TWO ENTRY POINTS AND NOT A FOURTH PARAMETER ON ONE, because
/// [`reconcile_schedule`]'s three-argument shape is Task 18's tested contract
/// and eight of its tests name it. This is the same function with the handle
/// threaded through; the three-argument form is it with `None`.
///
/// EVERY STORE CALL IS INSIDE `spawn_blocking` — interface **I13**, and the
/// property `no_store_call_is_made_outside_spawn_blocking` reads out of this
/// file's source. `Store` drives its own current-thread runtime, and
/// `Runtime::block_on` from a thread already driving one panics with *Cannot
/// start a runtime from within a runtime*; `kube`'s `Controller` drives this
/// function ON a runtime. Without the closure the code compiles cleanly and
/// dies at the first retention reconcile.
///
/// A RETENTION FAILURE IS NOT A RECONCILE FAILURE. An unreadable archive — a
/// lapsed credential, a prefix pointing at objects that are not manifests —
/// must not stop a schedule from firing its next slot, so the report is
/// logged at `warn` and omitted, and the slot decision is written regardless.
/// Retention is a REPORT; a schedule is a backup.
///
/// # Errors
///
/// [`ScheduleError`] for anything that is not a decision.
pub async fn reconcile_schedule_with_archive(
    schedule: &BackupSchedule,
    client: &kube::Client,
    archive: Option<&Arc<Store>>,
    now: DateTime<Utc>,
) -> Result<ScheduleOutcome, ScheduleError> {
    let name = schedule.name_any();
    let namespace = schedule
        .namespace()
        .ok_or_else(|| ScheduleError::NoNamespace(name.clone()))?;
    let uid = schedule
        .uid()
        .ok_or_else(|| ScheduleError::NoUid(name.clone()))?;
    let generation = schedule.metadata.generation.unwrap_or_default();
    let stored = schedule.status.as_ref();

    // ---- step 0 ---------------------------------------------------------
    //
    // THE NAME IS DECIDED WITHOUT THE STATUS; THE REPORTING IS DECIDED WITH
    // IT. Two statements and not one nested call, so the pure decision is a
    // value this function holds before anything refines how it is reported —
    // and so the refinement cannot be mistaken for part of the name.
    //
    // `effectiveSince` IS READ ONLY FOR THE REVISION IN FORCE. A block left by
    // an older generation says when THAT revision started, which is not a fact
    // about this one; taking it would make a catch-up decide row 19 against
    // the wrong instant.
    let decision = decide(&name, &schedule.spec, now);
    let effective_since = stored
        .and_then(|s| s.policy.as_ref())
        .filter(|p| p.generation == generation)
        .map(|p| p.effective_since);
    let mut decision = refine_against_status(
        decision,
        stored.and_then(|s| s.last_fire_time),
        effective_since,
    );

    let backups_api: Api<Backup> = Api::namespaced(client.clone(), &namespace);
    let mut work = Reconcile {
        created: None,
        already_existed: false,
        fired_due: None,
        active: Vec::new(),
        pending: PendingRefUpdate::Keep,
        last_slot: None,
        missed: stored.and_then(|s| s.missed_slots.clone()),
        status_write_base: schedule.clone(),
    };

    // ---- step 1: the active set -----------------------------------------
    let reservation = stored.and_then(|status| reserved_run(&name, status));
    let observed = refresh_active_runs(
        &backups_api,
        stored,
        &name,
        &uid,
        reservation.as_ref().map(|r| r.name.as_str()),
    )
    .await?;
    work.active = observed.active;

    // ---- step 2: an accepted reservation whose child does not exist ------
    if let Some(reserved) = reservation.as_ref() {
        match observed.pending_child {
            // The child exists: the reservation did its job and is cleared.
            // A terminal child also records the fire, because the slot DID
            // run — the controller simply did not see it finish in time.
            Some(child) => {
                work.pending = PendingRefUpdate::Clear;
                work.already_existed = true;
                work.created = Some(reserved.name.clone());
                if backup_is_terminal(&child) {
                    work.fired_due = Some(reserved.due);
                }
            }
            None => {
                // ROW 2: an invalid run policy RELEASES the reservation rather
                // than turning it into a run the Backup controller refuses
                // terminally. Cadence and time-zone invalidity do NOT block
                // this — the reserved slot is already a UTC instant, and the
                // policy the child would copy is still usable.
                if matches!(
                    decision,
                    SlotDecision::InvalidTopicSelection { .. }
                        | SlotDecision::InvalidRunPolicy { .. }
                ) {
                    work.pending = PendingRefUpdate::Clear;
                    work.last_slot = Some(LastSlot {
                        slot: reserved.plan.slot.clone(),
                        due_at: reserved.due,
                        attempt: i32::try_from(reserved.plan.attempt).unwrap_or(i32::MAX),
                        disposition: DISPOSITION_RELEASED.to_string(),
                        backup_ref: None,
                        reason: decision.reason().to_string(),
                        decided_at: now,
                    });
                } else {
                    // Suspension and deadlines do not cancel an accepted
                    // reservation: it is work this schedule already admitted.
                    create_run(
                        &backups_api,
                        schedule,
                        &uid,
                        &reserved.plan,
                        &mut work,
                        &namespace,
                        &name,
                    )
                    .await?;
                    work.fired_due = Some(reserved.due);
                    decision = admitted_decision(&decision, &reserved.plan, reserved.due);
                    work.last_slot =
                        Some(last_slot_for(&decision, &reserved.plan, reserved.due, now));
                }
                return finish(
                    schedule, client, archive, &namespace, &name, decision, work, now,
                )
                .await;
            }
        }
    }

    // ---- steps 3 and 4: suspended, or a policy that cannot run ------------
    if !matches!(
        decision,
        SlotDecision::Due { .. }
            | SlotDecision::CatchUpDue { .. }
            | SlotDecision::Missed { .. }
            | SlotDecision::AlreadyFired { .. }
    ) {
        return finish(
            schedule, client, archive, &namespace, &name, decision, work, now,
        )
        .await;
    }

    // ---- step 5: the latest due slot, and what was skipped to reach it ----
    let Some((due, slot)) = due_slot(&decision) else {
        return finish(
            schedule, client, archive, &namespace, &name, decision, work, now,
        )
        .await;
    };

    // ---- step 6: the attempt chain of S ----------------------------------
    let retry = schedule.spec.retry.map(|r| r.policy());
    let chain = observe_attempt_chain(
        &backups_api,
        &name,
        &uid,
        &slot,
        walk_retries(schedule, stored, &work.active),
    )
    .await;
    let chain = match chain {
        Ok(chain) => chain,
        Err(ChainError::Foreign(held)) => {
            // ROW 6: the deterministic name is held by an object this schedule
            // does not own. The slot is skipped and RECORDED, never re-run
            // under a different name.
            decision = SlotDecision::SlotNameUnavailable {
                slot: slot.clone(),
                name: held,
                next_fire_time: decision.next_fire_time(),
            };
            work.record_slot(&decision, &slot, due, 0, None, now);
            work.account_skipped(schedule, &decision, &slot, due, now, true);
            return finish(
                schedule, client, archive, &namespace, &name, decision, work, now,
            )
            .await;
        }
        Err(ChainError::Api(e)) => return Err(e),
    };

    let attempt = decide_attempt(&name, due, &chain, retry, now);
    let blocked = blocking_runs(schedule, &work.active);

    match attempt {
        AttemptDecision::NoAttempt => {}
        AttemptDecision::Succeeded {
            name: run, attempt, ..
        } => {
            decision = SlotDecision::AlreadyFired {
                due,
                slot: slot.clone(),
                last_fire_time: stored.and_then(|s| s.last_fire_time).unwrap_or(due),
                next_fire_time: decision.next_fire_time(),
            };
            work.fired_due = Some(due);
            // `created` MEANS "THE RUN THIS SLOT HAS", not "the object this
            // reconcile POSTed". A reconcile that finds the slot's run already
            // there has the same outcome as the one that created it — which is
            // what makes a crash between the create and the status write
            // harmless, and what the G-SLOT guard asserts.
            work.created = Some(run.clone());
            work.already_existed = true;
            work.record_slot(&decision, &slot, due, attempt, Some(run), now);
            work.account_skipped(schedule, &decision, &slot, due, now, true);
            return finish(
                schedule, client, archive, &namespace, &name, decision, work, now,
            )
            .await;
        }
        AttemptDecision::InProgress {
            name: run, attempt, ..
        } => {
            decision = SlotDecision::InProgress {
                slot: slot.clone(),
                attempt,
                name: run.clone(),
                next_fire_time: decision.next_fire_time(),
            };
            work.created = Some(run);
            work.already_existed = true;
            work.fired_due = Some(due);
            work.account_skipped(schedule, &decision, &slot, due, now, false);
            return finish(
                schedule, client, archive, &namespace, &name, decision, work, now,
            )
            .await;
        }
        AttemptDecision::NotRetryable {
            name: run, attempt, ..
        } => {
            decision = SlotDecision::RunFailed {
                slot: slot.clone(),
                attempt,
                name: run.clone(),
                next_fire_time: decision.next_fire_time(),
            };
            work.fired_due = Some(due);
            work.created = Some(run.clone());
            work.already_existed = true;
            work.record_slot(&decision, &slot, due, attempt, Some(run), now);
            work.account_skipped(schedule, &decision, &slot, due, now, true);
            return finish(
                schedule, client, archive, &namespace, &name, decision, work, now,
            )
            .await;
        }
        AttemptDecision::Exhausted {
            attempt,
            max_retries,
            retry_configured,
        } => {
            decision = SlotDecision::RetryExhausted {
                slot: slot.clone(),
                attempt,
                max_retries,
                retry_configured,
                next_fire_time: decision.next_fire_time(),
            };
            work.fired_due = Some(due);
            let last = chain.last().map(|a| a.name.clone());
            work.created = last.clone();
            work.already_existed = last.is_some();
            work.record_slot(&decision, &slot, due, attempt, last, now);
            work.account_skipped(schedule, &decision, &slot, due, now, true);
            return finish(
                schedule, client, archive, &namespace, &name, decision, work, now,
            )
            .await;
        }
        AttemptDecision::Pending { attempt, due_at } => {
            decision = SlotDecision::RetryPending {
                slot: slot.clone(),
                attempt,
                due_at,
                next_fire_time: decision.next_fire_time(),
            };
            work.fired_due = Some(due);
            work.created = chain.last().map(|a| a.name.clone());
            work.already_existed = work.created.is_some();
            work.account_skipped(schedule, &decision, &slot, due, now, false);
            return finish(
                schedule, client, archive, &namespace, &name, decision, work, now,
            )
            .await;
        }
        AttemptDecision::NameTooLong { error } => {
            decision = SlotDecision::RetryNamesTooLong { error };
            work.account_skipped(schedule, &decision, &slot, due, now, false);
            return finish(
                schedule, client, archive, &namespace, &name, decision, work, now,
            )
            .await;
        }
        AttemptDecision::Retry { plan, .. } => {
            if !blocked.is_empty() {
                decision = SlotDecision::RetryBlocked {
                    slot: slot.clone(),
                    attempt: plan.attempt,
                    active_backups: blocked,
                    next_fire_time: decision.next_fire_time(),
                };
                work.fired_due = Some(due);
                work.account_skipped(schedule, &decision, &slot, due, now, false);
                return finish(
                    schedule, client, archive, &namespace, &name, decision, work, now,
                )
                .await;
            }
            admit(
                &backups_api,
                client,
                schedule,
                &uid,
                &plan,
                due,
                &mut decision,
                &mut work,
                &namespace,
                &name,
                now,
            )
            .await?;
            return finish(
                schedule, client, archive, &namespace, &name, decision, work, now,
            )
            .await;
        }
    }

    // ---- step 7: no attempt of S exists ----------------------------------
    //
    // ROW 14: a slot at or before `lastFireTime` with no object was fired and
    // its history has been pruned. A fired slot is never re-run: the archive it
    // wrote is the recovery point, and a second run under the same execution id
    // does not accumulate — it leaves a partial archive a later drill restores
    // from and calls a success.
    if stored
        .and_then(|s| s.last_fire_time)
        .is_some_and(|last| due <= last)
    {
        decision = SlotDecision::AlreadyFired {
            due,
            slot: slot.clone(),
            last_fire_time: stored.and_then(|s| s.last_fire_time).unwrap_or(due),
            next_fire_time: decision.next_fire_time(),
        };
        work.account_skipped(schedule, &decision, &slot, due, now, true);
        return finish(
            schedule, client, archive, &namespace, &name, decision, work, now,
        )
        .await;
    }

    match &decision {
        SlotDecision::Missed { .. } => {
            work.record_slot(&decision, &slot, due, 0, None, now);
            work.account_skipped(schedule, &decision, &slot, due, now, true);
        }
        SlotDecision::Due {
            name: object_name, ..
        }
        | SlotDecision::CatchUpDue {
            name: object_name, ..
        } => {
            let catch_up = matches!(decision, SlotDecision::CatchUpDue { .. });
            if !blocked.is_empty() {
                decision = if catch_up {
                    SlotDecision::CatchUpBlocked {
                        slot: slot.clone(),
                        active_backups: blocked,
                        next_fire_time: decision.next_fire_time(),
                    }
                } else if schedule.spec.concurrency_policy == ConcurrencyPolicy::Allow {
                    SlotDecision::ActiveRunLimit {
                        slot: slot.clone(),
                        active_backups: blocked,
                        next_fire_time: decision.next_fire_time(),
                    }
                } else {
                    SlotDecision::ConcurrencyBlocked {
                        slot: slot.clone(),
                        active_backups: blocked,
                        next_fire_time: decision.next_fire_time(),
                    }
                };
                work.record_slot(&decision, &slot, due, 0, None, now);
                work.account_skipped(schedule, &decision, &slot, due, now, false);
            } else {
                let plan = RunPlan {
                    slot: slot.clone(),
                    name: object_name.clone(),
                    kind: if catch_up {
                        TriggerKind::CatchUp
                    } else {
                        TriggerKind::Scheduled
                    },
                    attempt: 0,
                    retry_of: None,
                };
                admit(
                    &backups_api,
                    client,
                    schedule,
                    &uid,
                    &plan,
                    due,
                    &mut decision,
                    &mut work,
                    &namespace,
                    &name,
                    now,
                )
                .await?;
            }
        }
        // `AlreadyFired` reached here only through the refinement, which means
        // the slot was fired and its object has been pruned. Nothing to do.
        _ => {}
    }

    finish(
        schedule, client, archive, &namespace, &name, decision, work, now,
    )
    .await
}

/// `status.lastSlot.disposition` values (D1 §4.8). A closed set, spelled once.
const DISPOSITION_ADMITTED: &str = "Admitted";
const DISPOSITION_CAUGHT_UP: &str = "CaughtUp";
const DISPOSITION_RETRIED: &str = "Retried";
const DISPOSITION_MISSED: &str = "Missed";
const DISPOSITION_BLOCKED: &str = "Blocked";
const DISPOSITION_NAME_UNAVAILABLE: &str = "NameUnavailable";
const DISPOSITION_RELEASED: &str = "Released";
const DISPOSITION_FAILED: &str = "Failed";
const DISPOSITION_EXHAUSTED: &str = "Exhausted";
const DISPOSITION_DONE: &str = "Admitted";

/// `status.missedSlots.recent[].reason` values (D1 §4.5 step 5).
const MISSED_CONTROLLER_UNAVAILABLE: &str = "ControllerUnavailable";
const MISSED_CONCURRENCY_BLOCKED: &str = "ConcurrencyBlocked";
const MISSED_PAST_DEADLINE: &str = "PastStartingDeadline";
const MISSED_BEFORE_REVISION: &str = "BeforeRevision";
const MISSED_NAME_UNAVAILABLE: &str = "NameUnavailable";

/// The mutable half of one reconcile: what it created, what it observed and
/// what it will write.
///
/// A STRUCT AND NOT NINE LOCALS, because the §4.5 algorithm has ten exits and
/// every one of them writes the same status. Threading nine `let mut`s through
/// ten early returns is how one of them comes to forget `activeRuns`.
struct Reconcile {
    created: Option<String>,
    already_existed: bool,
    fired_due: Option<DateTime<Utc>>,
    active: Vec<ActiveRun>,
    pending: PendingRefUpdate,
    last_slot: Option<LastSlot>,
    missed: Option<MissedSlots>,
    status_write_base: BackupSchedule,
}

impl Reconcile {
    /// Record what happened to the slot this reconcile decided about.
    fn record_slot(
        &mut self,
        decision: &SlotDecision,
        slot: &str,
        due: DateTime<Utc>,
        attempt: u32,
        backup: Option<String>,
        now: DateTime<Utc>,
    ) {
        self.last_slot = Some(LastSlot {
            slot: slot.to_string(),
            due_at: due,
            attempt: i32::try_from(attempt).unwrap_or(i32::MAX),
            disposition: disposition_of(decision).to_string(),
            backup_ref: backup.map(|name| LocalRef { name }),
            reason: decision.reason().to_string(),
            decided_at: now,
        });
    }

    /// D1 §4.5 step 5: count the slots that came due between the last one this
    /// schedule finished deciding about and this one.
    ///
    /// `final_disposition` IS THE WHOLE CORRECTNESS ARGUMENT. `lastEvaluatedSlot`
    /// advances only when the current slot has received a disposition it cannot
    /// come back from, so a slot that waits for concurrency and is then
    /// superseded by a newer one is inside the interval exactly once, and a
    /// reconcile that runs thirty seconds later does not count the same gap
    /// again.
    fn account_skipped(
        &mut self,
        schedule: &BackupSchedule,
        decision: &SlotDecision,
        slot: &str,
        due: DateTime<Utc>,
        now: DateTime<Utc>,
        final_disposition: bool,
    ) {
        if !final_disposition {
            return;
        }
        let mut missed = self.missed.clone().unwrap_or(MissedSlots {
            count: 0,
            count_capped: false,
            last_evaluated_slot: None,
            recent: None,
        });
        let previous = missed
            .last_evaluated_slot
            .as_deref()
            .and_then(slot_instant)
            .filter(|previous| *previous < due);
        let mut recent: Vec<MissedSlot> = Vec::new();
        if let Some(previous) = previous {
            if let Ok(cadence) =
                Cadence::parse(&schedule.spec.schedule, schedule.spec.time_zone.as_deref())
            {
                // BOTH ENDS EXCLUSIVE: `previous` already has a disposition and
                // `due` is being given one now.
                let skipped = cadence.skipped_slots(previous, due, MAX_SKIPPED_SLOT_ENUMERATION);
                missed.count = missed
                    .count
                    .saturating_add(i64::try_from(skipped.count).unwrap_or(i64::MAX));
                missed.count_capped =
                    missed.count_capped || skipped.capped || skipped.horizon_reached;
                // The NAMES of the gap are not enumerated into the status: a
                // week of one-minute slots is ten thousand of them, and the
                // count is the fact an operator acts on. The sample below is
                // the slot this reconcile actually decided about.
            }
        }
        if let Some(reason) = missed_reason(decision) {
            missed.count = missed.count.saturating_add(1);
            recent.push(MissedSlot {
                slot: slot.to_string(),
                reason: reason.to_string(),
                recorded_at: now,
            });
        } else if previous.is_some() {
            recent.push(MissedSlot {
                slot: slot.to_string(),
                reason: MISSED_CONTROLLER_UNAVAILABLE.to_string(),
                recorded_at: now,
            });
            // The marker entry above is dropped again below when the slot was
            // not itself skipped; it exists only so a caller reading `recent`
            // sees the boundary the count moved across.
            recent.pop();
        }
        if !recent.is_empty() {
            let mut all = recent;
            all.extend(missed.recent.take().into_iter().flatten());
            all.truncate(RECENT_MISSED_SLOTS);
            missed.recent = Some(all);
        }
        missed.last_evaluated_slot = Some(slot.to_string());
        self.missed = Some(missed);
    }
}

/// The `status.lastSlot.disposition` one decision produces.
fn disposition_of(decision: &SlotDecision) -> &'static str {
    match decision {
        SlotDecision::Due { .. } | SlotDecision::AlreadyFired { .. } => DISPOSITION_ADMITTED,
        SlotDecision::CaughtUp { .. } | SlotDecision::CatchUpDue { .. } => DISPOSITION_CAUGHT_UP,
        SlotDecision::Retried { .. } => DISPOSITION_RETRIED,
        SlotDecision::Missed { .. } => DISPOSITION_MISSED,
        SlotDecision::ConcurrencyBlocked { .. }
        | SlotDecision::CatchUpBlocked { .. }
        | SlotDecision::RetryBlocked { .. }
        | SlotDecision::ActiveRunLimit { .. } => DISPOSITION_BLOCKED,
        SlotDecision::SlotNameUnavailable { .. } => DISPOSITION_NAME_UNAVAILABLE,
        SlotDecision::RunFailed { .. } => DISPOSITION_FAILED,
        SlotDecision::RetryExhausted { .. } => DISPOSITION_EXHAUSTED,
        SlotDecision::Suspended
        | SlotDecision::Unparseable(_)
        | SlotDecision::UnknownTimeZone { .. }
        | SlotDecision::InvalidTopicSelection { .. }
        | SlotDecision::InvalidRunPolicy { .. }
        | SlotDecision::RetryNamesTooLong { .. }
        | SlotDecision::NoDueSlot { .. }
        | SlotDecision::NameTooLong { .. }
        | SlotDecision::CrdOutdated { .. }
        | SlotDecision::InProgress { .. }
        | SlotDecision::RetryPending { .. } => DISPOSITION_DONE,
    }
}

/// Why THIS slot was skipped, or `None` when it was not skipped at all.
fn missed_reason(decision: &SlotDecision) -> Option<&'static str> {
    match decision {
        SlotDecision::Missed { reason, .. } => Some(match reason {
            MissedReason::PastStartingDeadline => MISSED_PAST_DEADLINE,
            MissedReason::BeforeRevision => MISSED_BEFORE_REVISION,
        }),
        SlotDecision::SlotNameUnavailable { .. } => Some(MISSED_NAME_UNAVAILABLE),
        SlotDecision::ConcurrencyBlocked { .. }
        | SlotDecision::CatchUpBlocked { .. }
        | SlotDecision::ActiveRunLimit { .. } => Some(MISSED_CONCURRENCY_BLOCKED),
        _ => None,
    }
}

/// The instant a `yyyymmdd-hhmmss` slot name spells.
fn slot_instant(slot: &str) -> Option<DateTime<Utc>> {
    chrono::NaiveDateTime::parse_from_str(slot, "%Y%m%d-%H%M%S")
        .ok()
        .map(|naive| naive.and_utc())
}

/// The due slot a decision is about, when it is about one.
fn due_slot(decision: &SlotDecision) -> Option<(DateTime<Utc>, String)> {
    match decision {
        SlotDecision::Due { due, slot, .. }
        | SlotDecision::CatchUpDue { due, slot, .. }
        | SlotDecision::Missed { due, slot, .. }
        | SlotDecision::AlreadyFired { due, slot, .. } => Some((*due, slot.clone())),
        _ => None,
    }
}

/// An accepted reservation, read from either spelling of it.
struct Reservation {
    name: String,
    plan: RunPlan,
    due: DateTime<Utc>,
}

/// The reservation this status holds, if any.
///
/// THE TYPED BLOCK FIRST, THE MIRROR AS A FALLBACK. `status.pendingRun` carries
/// the slot, the attempt and the kind; `status.pendingBackupRef` is a name an
/// older controller wrote, and the name is enough to recover all three because
/// it is a pure function of them. Reading the mirror is what makes a
/// roll-forward resume an accepted slot instead of abandoning it — and a
/// reservation neither spelling can be parsed into is stale status rather than
/// work to preserve, so it is cleared.
fn reserved_run(
    schedule_name: &str,
    status: &crate::crds::backup_schedule::BackupScheduleStatus,
) -> Option<Reservation> {
    let (name, slot, attempt, kind) = match status.pending_run.as_ref() {
        Some(pending) => (
            pending.name.clone(),
            pending.slot.clone(),
            u32::try_from(pending.attempt).unwrap_or(0),
            match pending.kind.as_str() {
                "CatchUp" => TriggerKind::CatchUp,
                "Retry" => TriggerKind::Retry,
                _ => TriggerKind::Scheduled,
            },
        ),
        None => {
            let name = status.pending_backup_ref.as_ref()?.name.clone();
            let (slot, _due, attempt) = reserved_slot(schedule_name, &name)?;
            let kind = if attempt == 0 {
                TriggerKind::Scheduled
            } else {
                TriggerKind::Retry
            };
            (name, slot, attempt, kind)
        }
    };
    // THE NAME IS RE-DERIVED AND COMPARED, NOT TRUSTED. A `pendingRun` whose
    // name is not `name(schedule, slot, attempt)` is not a reservation this
    // controller made, and resuming it would create an object whose identity
    // `identity::run_identity` refuses terminally.
    let composed =
        crate::slot::scheduled_backup_name_for_attempt(schedule_name, &slot, attempt).ok()?;
    if composed != name {
        return None;
    }
    let due = slot_instant(&slot)?;
    Some(Reservation {
        name: name.clone(),
        plan: RunPlan {
            slot: slot.clone(),
            name,
            kind,
            attempt,
            retry_of: (attempt > 0)
                .then(|| {
                    crate::slot::scheduled_backup_name_for_attempt(
                        schedule_name,
                        &slot,
                        attempt - 1,
                    )
                    .ok()
                })
                .flatten(),
        },
        due,
    })
}

/// Everything step 1 observed.
struct ObservedRuns {
    active: Vec<ActiveRun>,
    pending_child: Option<Backup>,
}

/// D1 §4.5 step 1: refresh the active set.
///
/// # Two branches, and why the LIST one has to stay
///
/// The steady-state branch `GET`s the ≤ 10 names `status.activeRuns` records
/// plus the reservation, which is O(active) and independent of how much history
/// the schedule has. That is only correct once the block EXISTS, and a schedule
/// reconciled by a controller that predates it has none — so an ABSENT
/// `activeRuns` falls back to one namespace-wide LIST, exactly what this
/// reconciler always did, and the block it then writes turns the next reconcile
/// into the cheap branch. An absent list and an empty list are therefore
/// different facts, which is why `activeRuns` is `Option<Vec<_>>` and why the
/// final status always writes it, `[]` included.
///
/// **This is PLAT-05.2 (W4)'s seam.** `schedule_history::observe(…)` replaces
/// the bootstrap LIST with a paginated, label-selected inventory that also
/// repairs `activeRuns` on a schedule whose status was lost, and re-runs it
/// every 60 minutes; until it lands the bootstrap below is that inventory's
/// one-shot form. Nothing about admission correctness depends on either: every
/// schedule-created run is recorded by a resourceVersion-conditional status
/// write BEFORE it is created.
async fn refresh_active_runs(
    api: &Api<Backup>,
    status: Option<&crate::crds::backup_schedule::BackupScheduleStatus>,
    name: &str,
    uid: &str,
    pending: Option<&str>,
) -> Result<ObservedRuns, ScheduleError> {
    let mut active: Vec<ActiveRun> = Vec::new();
    let mut pending_child: Option<Backup> = None;

    match status.and_then(|s| s.active_runs.as_ref()) {
        Some(recorded) => {
            for entry in recorded.iter().take(MAX_ACTIVE_RUNS) {
                if let Some(backup) = api.get_opt(&entry.name).await? {
                    if participates_in_concurrency(&backup, name, uid)
                        && !backup_is_terminal(&backup)
                    {
                        active.push(active_entry(&backup));
                    }
                    if Some(entry.name.as_str()) == pending {
                        pending_child = Some(backup);
                    }
                }
            }
            if let Some(pending) = pending {
                if pending_child.is_none() && !recorded.iter().any(|e| e.name == pending) {
                    if let Some(backup) = api.get_opt(pending).await? {
                        if !participates_in_concurrency(&backup, name, uid) {
                            return Err(ScheduleError::ForeignBackup(pending.to_string()));
                        }
                        if !backup_is_terminal(&backup) {
                            active.push(active_entry(&backup));
                        }
                        pending_child = Some(backup);
                    }
                }
            }
        }
        None => {
            let listed = api.list(&ListParams::default()).await?;
            for backup in &listed.items {
                let this = backup.name_any();
                let member = participates_in_concurrency(backup, name, uid);
                if Some(this.as_str()) == pending {
                    if !member {
                        return Err(ScheduleError::ForeignBackup(this));
                    }
                    pending_child = Some(backup.clone());
                }
                if member && !backup_is_terminal(backup) {
                    active.push(active_entry(backup));
                }
            }
        }
    }
    active.sort_by(|a, b| a.name.cmp(&b.name));
    active.dedup_by(|a, b| a.name == b.name);
    active.truncate(MAX_ACTIVE_RUNS);
    Ok(ObservedRuns {
        active,
        pending_child,
    })
}

/// One active run, as the status records it.
fn active_entry(backup: &Backup) -> ActiveRun {
    let (kind, attempt, _) = crate::identity::declared_trigger(backup);
    ActiveRun {
        name: backup.name_any(),
        kind: format!("{kind:?}"),
        attempt: i32::try_from(attempt).unwrap_or(0),
    }
}

/// Why the attempt chain could not be observed.
enum ChainError {
    /// The deterministic name is held by an object this schedule does not own.
    Foreign(String),
    /// The API server could not be talked to.
    Api(ScheduleError),
}

/// D1 §4.5 step 6: the attempt chain of one slot, by GET of deterministic
/// names, stopping at the first 404.
///
/// NEVER BY LISTING. A list is O(history) and answers a different question:
/// "which objects carry this label" rather than "does the object this slot's
/// attempt k would be called exist". The second question is the one admission
/// turns on, and its answer is a name the controller can compute.
///
/// `walk_retries` BOUNDS THE WALK WITHOUT LOSING D1's PROPERTY. The decision
/// says to walk to the hard maximum independently of the current `maxRetries`,
/// so that lowering it still sees the attempts that exist. Walking past attempt
/// 0 on a schedule that has NEVER had a retry policy and records no attempt
/// above 0 cannot find anything — this controller is the only writer of those
/// names — so the caller skips it and the steady-state reconcile costs one GET.
async fn observe_attempt_chain(
    api: &Api<Backup>,
    schedule_name: &str,
    uid: &str,
    slot: &str,
    walk_retries: bool,
) -> Result<Vec<ObservedAttempt>, ChainError> {
    let mut chain: Vec<ObservedAttempt> = Vec::new();
    let highest = if walk_retries {
        crate::slot::MAX_RETRIES
    } else {
        0
    };
    for attempt in 0..=highest {
        let Ok(name) = crate::slot::scheduled_backup_name_for_attempt(schedule_name, slot, attempt)
        else {
            break;
        };
        let found = api
            .get_opt(&name)
            .await
            .map_err(|e| ChainError::Api(ScheduleError::Api(e)))?;
        let Some(backup) = found else { break };
        // D1 §3.1 RULE 1 CONSTRAINS SCHEDULED NAMES ONLY, SO THIS HAS TO
        // DECIDE WHAT ELSE MAY SIT ON ONE. Nothing refuses a `Manual` Backup
        // named `logweir-backup-<schedule>-<slot>`; the repository's own
        // `pre-connection-contract-inputs.json` fixture is that shape. Such an
        // object is a **foreign occupant**, not an attempt of this slot: it
        // executes under its own UID and its own execution id, so reading it as
        // attempt k would make the scheduler believe a window was covered by a
        // run that wrote a different archive. The slot is reported
        // `SlotNameUnavailable` and never re-run under a different name — the
        // same answer another schedule's object gets, for the same reason.
        if !participates_in_concurrency(&backup, schedule_name, uid) {
            return Err(ChainError::Foreign(name));
        }
        chain.push(ObservedAttempt {
            name,
            attempt,
            outcome: attempt_outcome(&backup),
        });
    }
    Ok(chain)
}

/// Whether the attempt chain walk should look past attempt 0.
fn walk_retries(
    schedule: &BackupSchedule,
    status: Option<&crate::crds::backup_schedule::BackupScheduleStatus>,
    active: &[ActiveRun],
) -> bool {
    schedule.spec.retry.is_some()
        || active.iter().any(|run| run.attempt > 0)
        || status
            .and_then(|s| s.last_slot.as_ref())
            .is_some_and(|last| last.attempt > 0)
        || status
            .and_then(|s| s.pending_run.as_ref())
            .is_some_and(|pending| pending.attempt > 0)
}

/// The runs that block a new admission (D1 §4.7's "blocked").
///
/// `Forbid`: ANY nonterminal schedule-created run of this schedule. `Allow`: the
/// ceiling [`MAX_ACTIVE_RUNS`], and nothing below it. Manual runs are neither
/// counted nor blocked — they carry no `scheduleRef.uid` membership as a
/// schedule-created run and never appear in `activeRuns`.
fn blocking_runs(schedule: &BackupSchedule, active: &[ActiveRun]) -> Vec<String> {
    match schedule.spec.concurrency_policy {
        ConcurrencyPolicy::Forbid => active.iter().map(|run| run.name.clone()).collect(),
        ConcurrencyPolicy::Allow => {
            if active.len() >= MAX_ACTIVE_RUNS {
                active.iter().map(|run| run.name.clone()).collect()
            } else {
                Vec::new()
            }
        }
    }
}

/// The decision an accepted admission becomes.
fn admitted_decision(decision: &SlotDecision, plan: &RunPlan, due: DateTime<Utc>) -> SlotDecision {
    let next_fire_time = decision.next_fire_time();
    match plan.kind {
        TriggerKind::CatchUp => SlotDecision::CaughtUp {
            due,
            slot: plan.slot.clone(),
            name: plan.name.clone(),
            next_fire_time,
        },
        TriggerKind::Retry => SlotDecision::Retried {
            due,
            slot: plan.slot.clone(),
            name: plan.name.clone(),
            attempt: plan.attempt,
            next_fire_time,
        },
        TriggerKind::Scheduled | TriggerKind::Manual => SlotDecision::Due {
            due,
            slot: plan.slot.clone(),
            name: plan.name.clone(),
            next_fire_time,
        },
    }
}

/// The `status.lastSlot` an admission records.
fn last_slot_for(
    decision: &SlotDecision,
    plan: &RunPlan,
    due: DateTime<Utc>,
    now: DateTime<Utc>,
) -> LastSlot {
    LastSlot {
        slot: plan.slot.clone(),
        due_at: due,
        attempt: i32::try_from(plan.attempt).unwrap_or(i32::MAX),
        disposition: disposition_of(decision).to_string(),
        backup_ref: Some(LocalRef {
            name: plan.name.clone(),
        }),
        reason: decision.reason().to_string(),
        decided_at: now,
    }
}

/// Create one admitted run, adopting an `AlreadyExists` winner only when it is
/// this schedule's own.
///
/// A 409 PROVES ONLY THAT SOMETHING HOLDS THE NAME. The winner is fetched and
/// has to carry this schedule's membership before it is treated as same-slot
/// idempotence; a transient 404 or GET failure is returned conservatively and
/// no success status is written.
async fn create_run(
    api: &Api<Backup>,
    schedule: &BackupSchedule,
    uid: &str,
    plan: &RunPlan,
    work: &mut Reconcile,
    namespace: &str,
    schedule_name: &str,
) -> Result<(), ScheduleError> {
    let backup = scheduled_run(schedule, uid, plan);
    let mut terminal = false;
    match api.create(&PostParams::default(), &backup).await {
        Ok(created) => {
            info!(
                schedule = %schedule_name,
                namespace = %namespace,
                backup = %plan.name,
                slot = %plan.slot,
                attempt = plan.attempt,
                kind = plan.kind_name(),
                "created a scheduled Backup"
            );
            // D1 §4.9's CRD-BEFORE-CONTROLLER GUARD. The API server silently
            // PRUNES a field an older CRD does not declare, so the response is
            // read back: a created object with no `spec.trigger` or no
            // `spec.scheduleRef.uid` has no identity, and the Backup controller
            // refuses it before any POST rather than executing it ambiguously.
            if created.spec.trigger.is_none()
                || created
                    .spec
                    .schedule_ref
                    .as_ref()
                    .and_then(|r| r.uid.as_deref())
                    .is_none()
            {
                return Err(ScheduleError::CrdOutdated(
                    "the created Backup came back without spec.trigger or spec.scheduleRef.uid, \
                     so the installed CRD is older than this controller"
                        .to_string(),
                ));
            }
        }
        Err(kube::Error::Api(e)) if e.code == 409 => {
            let existing = api.get(&plan.name).await?;
            if existing.name_any() != plan.name
                || !participates_in_concurrency(&existing, schedule_name, uid)
            {
                return Err(ScheduleError::ForeignBackup(plan.name.clone()));
            }
            terminal = backup_is_terminal(&existing);
            work.already_existed = true;
            debug!(
                schedule = %schedule_name,
                namespace = %namespace,
                backup = %plan.name,
                slot = %plan.slot,
                "the Backup for this attempt already exists; AlreadyExists IS the idempotence key"
            );
        }
        Err(e) => return Err(e.into()),
    }
    work.created = Some(plan.name.clone());
    work.pending = PendingRefUpdate::Clear;
    if !terminal {
        work.active.retain(|run| run.name != plan.name);
        work.active.push(ActiveRun {
            name: plan.name.clone(),
            kind: plan.kind_name().to_string(),
            attempt: i32::try_from(plan.attempt).unwrap_or(0),
        });
        work.active.sort_by(|a, b| a.name.cmp(&b.name));
    }
    Ok(())
}

/// D1 §4.5's ADMIT: reserve, then create from the same in-memory object.
///
/// THE RESERVATION IS UNIFORM FOR `Forbid` AND `Allow`, which is new. It makes
/// `status.activeRuns` complete by construction — every schedule-created run is
/// recorded before it exists — and gives every run the same crash semantics: a
/// controller that dies between the two finds the reservation on restart and
/// resumes exactly that deterministic child.
#[allow(clippy::too_many_arguments)]
async fn admit(
    api: &Api<Backup>,
    client: &kube::Client,
    schedule: &BackupSchedule,
    uid: &str,
    plan: &RunPlan,
    due: DateTime<Utc>,
    decision: &mut SlotDecision,
    work: &mut Reconcile,
    namespace: &str,
    schedule_name: &str,
    now: DateTime<Utc>,
) -> Result<(), ScheduleError> {
    let schedules_api: Api<BackupSchedule> = Api::namespaced(client.clone(), namespace);
    let body = status_patch_with_preconditions(
        schedule,
        reservation_patch(schedule, decision, plan, now),
    )?;
    let reserved = schedules_api
        .patch_status(schedule_name, &PatchParams::default(), &Patch::Merge(body))
        .await?;
    // THE SECOND HALF OF D1 §4.9's GUARD, and the cheaper half: the reservation
    // response must contain what was written. An older CRD prunes `pendingRun`,
    // so the crash-safety the reservation exists for would silently not exist.
    if reserved
        .status
        .as_ref()
        .and_then(|s| s.pending_run.as_ref())
        .is_none()
    {
        return Err(ScheduleError::CrdOutdated(
            "the reservation response came back without status.pendingRun, so the installed CRD \
             is older than this controller"
                .to_string(),
        ));
    }
    work.status_write_base = reserved;
    create_run(api, schedule, uid, plan, work, namespace, schedule_name).await?;
    work.fired_due = Some(due);
    *decision = admitted_decision(decision, plan, due);
    work.last_slot = Some(last_slot_for(decision, plan, due, now));
    work.account_skipped(schedule, decision, &plan.slot, due, now, true);
    Ok(())
}

/// D1 §4.5 step 8: the retention report, the final status write and the
/// outcome.
#[allow(clippy::too_many_arguments)]
async fn finish(
    schedule: &BackupSchedule,
    client: &kube::Client,
    archive: Option<&Arc<Store>>,
    namespace: &str,
    name: &str,
    decision: SlotDecision,
    work: Reconcile,
    now: DateTime<Utc>,
) -> Result<ScheduleOutcome, ScheduleError> {
    // THE RETENTION REPORT. Between the create and the status write, so the
    // report travels in the same patch as the slot decision — one write, one
    // resourceVersion bump.
    //
    // INSIDE `spawn_blocking`, AND THAT IS INTERFACE I13. Every `Store` method
    // drives its own current-thread runtime, and this function is driven ON a
    // runtime by `kube`'s `Controller`; a direct call panics with *Cannot start
    // a runtime from within a runtime*.
    let retention_report = match archive {
        None => None,
        Some(store) => {
            let store = Arc::clone(store);
            let archive_url = schedule.spec.archive.url.clone();
            let prefix = crate::retention::bucket_and_prefix(&archive_url).1;
            let retention = schedule.spec.retention.clone().unwrap_or(Retention {
                keep_last: None,
                keep_days: None,
            });
            let joined = tokio::task::spawn_blocking(move || {
                crate::retention::evaluate(&store, &archive_url, &prefix, &retention, now)
            })
            .await;
            match joined {
                Ok(Ok(report)) => Some(report),
                // A REPORT IS NOT A BACKUP. An unreadable archive must not stop
                // a schedule from firing, so this is a `warn` and an omitted
                // block, never an error the reconcile returns.
                Ok(Err(e)) => {
                    warn!(
                        schedule = %name,
                        namespace = %namespace,
                        error = %e,
                        "could not evaluate retention against the archive; the report is omitted \
                         and the slot decision is written regardless"
                    );
                    None
                }
                Err(e) => {
                    warn!(
                        schedule = %name,
                        namespace = %namespace,
                        error = %e,
                        "the retention evaluation task did not complete; the report is omitted"
                    );
                    None
                }
            }
        }
    };

    let api: Api<BackupSchedule> = Api::namespaced(client.clone(), namespace);
    let patch = status_patch_with_refs(
        &work.status_write_base,
        &decision,
        &work,
        retention_report.as_ref(),
        now,
    );
    // NO WRITE WHEN NOTHING CHANGED — plan erratum E11(d), review finding M-1.
    // On a 30 s requeue a bump-on-every-write is 2,880 API writes a day per
    // schedule that never changed state.
    if status_unchanged(
        work.status_write_base
            .status
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok())
            .as_ref(),
        &patch,
    ) {
        debug!(
            schedule = %name,
            namespace = %namespace,
            "the computed status equals the one on the object; no patch is sent"
        );
    } else {
        // THE ONE PLACE `evaluatedAt` MOVES. The computed block carries the
        // stored instant so that a steady schedule compares equal and is not
        // written; once this branch is taken the status is moving anyway.
        let mut patch = patch;
        if let Some(evaluated) = patch
            .get_mut("status")
            .and_then(|s| s.get_mut("policy"))
            .and_then(|p| p.get_mut("evaluatedAt"))
        {
            *evaluated = json!(now);
        }
        let patch = status_patch_with_preconditions(&work.status_write_base, patch)?;
        api.patch_status(name, &PatchParams::default(), &Patch::Merge(patch))
            .await?;
    }

    if !decision.ready() {
        // WARN AND NOT ERROR. A suspended schedule is an operator's own
        // decision and an unparseable one is a spec they can fix; neither is
        // something to page for.
        warn!(
            schedule = %name,
            namespace = %namespace,
            reason = decision.reason(),
            "backup schedule is not ready"
        );
    }

    Ok(ScheduleOutcome {
        decision,
        created: work.created,
        already_existed: work.already_existed,
    })
}

/// The `kube::runtime` reconcile entry point.
///
/// THE ONE CLOCK READ IN THIS FILE IS HERE, and it is read once per reconcile,
/// before anything is decided. Everything downstream takes the instant as a
/// value.
async fn reconcile(
    schedule: Arc<BackupSchedule>,
    ctx: Arc<Context>,
) -> Result<Action, ScheduleError> {
    let now = Utc::now();
    let outcome =
        reconcile_schedule_with_archive(&schedule, &ctx.client, ctx.archive.as_ref(), now).await?;
    // NOT `Action::await_change()`. A cron schedule's next event is a clock
    // tick, and no Kubernetes watch delivers one; without a requeue a schedule
    // created at 09:00 would never fire again until somebody edited it.
    //
    // D1 §4.5 step 8 asks for `min(30 s, next retry/slot due)`. The floor keeps
    // a retry whose delay expires in two seconds from waiting twenty-eight more
    // for a wake-up that was going to happen anyway; it never LENGTHENS the
    // interval, so nothing drifts.
    Ok(Action::requeue(outcome.decision.requeue_after(now)))
}

/// Requeue on an error, naming it. Never a panic and never a drop.
fn error_policy(schedule: Arc<BackupSchedule>, err: &ScheduleError, _ctx: Arc<Context>) -> Action {
    warn!(
        schedule = %schedule.name_any(),
        error = %err,
        "backup schedule reconcile failed; requeueing"
    );
    Action::requeue(std::time::Duration::from_secs(REQUEUE_SECS))
}

/// Run the `BackupSchedule` controller until the process ends.
///
/// `Api::all`: this controller reconciles schedules in every namespace, which
/// is what a cluster-scoped install means (Global Constraint 30 — one
/// controller per cluster, no fleet).
///
/// `archive` is the controller's ONE read-only archive handle, built once in
/// `main` before the tokio runtime exists and shared as `Arc<Store>` —
/// interface **I13**. `None` is a controller with no archive configured; it
/// writes no retention report and the schedule half is unaffected.
pub fn controller(
    client: kube::Client,
    archive: Option<Arc<Store>>,
) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<BackupSchedule> = Api::all(client.clone());
    let ctx = Arc::new(Context {
        client,
        archive,
        // Task 33 (and Task 37's pull policy beside it): this reconciler
        // creates no runner Job, so the image and policy this process was
        // handed would be carried and never read. The default here is
        // "unused", never "no override is configured" — see
        // `super::Context::runner_image`.
        runner_image: crate::job::RunnerImage::default(),
    });
    async move {
        Controller::new(api, watcher::Config::default())
            .run(reconcile, error_policy, ctx)
            // Every item is already logged by `reconcile_schedule` or by
            // `error_policy`; the stream exists to be DRIVEN.
            .for_each(|_| std::future::ready(()))
            .await;
    }
}
