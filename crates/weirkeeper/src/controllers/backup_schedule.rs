//! The `BackupSchedule` cron reconciler: guard **G-SLOT**.
//!
//! # The design
//!
//! The object name is a pure function of the trigger, while `Forbid` first
//! reserves that name through a resource-version-checked `/status` replace.
//! The reservation serializes different slots across replicas; a duplicate
//! child `create` still gets **409 `AlreadyExists`**, which is the same-slot
//! idempotence key.
//!
//! A scheduled backup that fires twice for one slot is not just a wasted run:
//! a second `backup` into a colliding `backup_id` does not accumulate, and
//! leaves a manifest that disagrees with the broker, a partial archive a later
//! drill restores from and calls a success. A name derived from a
//! reconcile-time clock or from `status.lastFireTime` produces two objects when
//! the controller crashes between the `create` and the status write. So
//! [`decide`] is a pure function of `(name, spec, now)` that returns the object
//! name, and [`reconcile_schedule`] contains no `Utc::now()`: the one clock
//! read in this file is in the `kube::runtime` wrapper, before anything is
//! decided.
//!
//! # The final status write happens after the create
//!
//! `Forbid` has a short-lived `pendingBackupRef` before the create, but it does
//! not claim the Backup fired. A restart resumes that accepted deterministic
//! child. After the child is observed or created, the final patch records
//! `lastFireTime`, moves the reference to `activeBackupRef`, and clears the
//! reservation. This distinguishes accepted work from a stale active ref.
//!
//! # A skipped slot is recorded
//!
//! A controller restarted after a week must not fire six days of backlog, so a
//! slot older than the one-hour [`MISSED_SLOT_HORIZON`] is skipped and
//! recorded (`status.lastMissedSlot` plus a `Ready` condition with reason
//! [`REASON_SLOT_MISSED`]); without the record a correct implementation is
//! indistinguishable from a broken schedule. The CRD field description states
//! the horizon for `kubectl explain`.
//!
//! # A slot is missed only if it was never fired
//!
//! [`decide`] reads no `status`, so on its own it cannot tell "fired hours
//! ago" from "never fired", and a healthy daily schedule reconciled at 10:00
//! would report its own 00:00 slot as missed. So the reason is refined
//! against `status.lastFireTime` by [`refine_against_last_fire`], and the name
//! is not: the refinement takes only `Option<DateTime<Utc>>` and can turn
//! [`SlotDecision::Missed`] into [`SlotDecision::AlreadyFired`] and nothing
//! else. It cannot reach a name or the `Due` arm, so it cannot reintroduce a
//! name derived from `status.lastFireTime`.
//!
//! # A slot that predates the schedule is not its slot
//!
//! A schedule never fires a slot due before its own
//! `metadata.creationTimestamp`, the Kubernetes `CronJob` rule; the starting
//! deadline alone would admit one. [`bound_by_creation`] runs on [`decide`]'s
//! output before any status refinement and turns such a slot into
//! [`SlotDecision::BeforeCreation`]: not fired, not missed, `Ready=True`.
//!
//! # Only `sourceRef` is sealed
//!
//! The object-level CEL rule
//! ([`crate::crds::backup_schedule::SOURCE_REF_IMMUTABLE_RULE`]) seals it;
//! every other field is policy the next admission reads. Like every
//! reconciler here this one patches `/status` and nothing else, and it never
//! deletes.

use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use futures::StreamExt as _;
use kube::api::{ObjectMeta, Patch, PatchParams, PostParams};
use kube::runtime::controller::Action;
use kube::runtime::{reflector, watcher, Controller, WatchStreamExt as _};
use kube::{Api, ResourceExt};
use serde_json::json;
use tracing::{debug, info, warn};

use logweir_store::Store;

use super::Context;
use crate::cadence::{
    Cadence, CadenceError, CatchUpPolicy as CadenceCatchUp, MissedReason, RetryAdmission,
    RetryPolicy, SlotAdmission,
};
use crate::conditions::{
    current_condition, keep_instant_unless_changed, merge_condition, replacing, status_unchanged,
};
use crate::crds::backup::{Backup, BackupSpec, ScheduleRef, Trigger, TriggerKind};
use crate::crds::backup_schedule::{
    ActiveRun, BackupSchedule, BackupScheduleSpec, ConcurrencyPolicy, LastSlot, MissedSlot,
    MissedSlots, NextRun, PendingRun, PolicyStatus,
};
use crate::crds::{Condition, LocalRef};
use crate::identity::is_run_of_schedule;
use crate::slot::{scheduled_backup_name, slot_name, CronError, SlotError};
use logweir_core::destination::FieldError;

/// A value rendered into a condition message, bounded and with control
/// characters stripped.
///
/// A condition message is read by a person and written from adopter-supplied
/// text, so it gets the treatment `backup_execution::shown` gives a spec
/// value: a 4 KiB zone name full of escape sequences is unreadable.
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
/// Fail closed: the API server accepts these edits, so the controller is where
/// they stop: `Ready=False`, no admissions, running Backups untouched, and the
/// schedule resumes one reconcile after the spec is fixed.
pub const REASON_INVALID_TOPIC_SELECTION: &str = crate::conditions::REASON_INVALID_TOPIC_SELECTION;

/// `reason` when some other run-policy field is unusable: a non-positive
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
/// once under `concurrencyPolicy: Allow`.
///
/// A backstop, not a policy: `Allow` means "different slots may overlap", not
/// "unbounded", and a schedule whose runs outlast its period would otherwise
/// add one Job per slot for as long as that lasts. At the ceiling the schedule
/// reports `ActiveRunLimit` and admits nothing further, visibly.
pub const MAX_ACTIVE_RUNS: usize = 10;

/// How many slots one reconcile will enumerate when accounting for downtime.
pub const MAX_SKIPPED_SLOT_ENUMERATION: usize = crate::cadence::MAX_SKIPPED_SLOT_ENUMERATION;

/// How many recent missed slots `status.missedSlots.recent` keeps.
pub const RECENT_MISSED_SLOTS: usize = 10;

/// `spec.triggeredBy` on every `Backup` this reconciler creates.
///
/// Recorded, not inferred from `scheduleRef`, because the signed receipt
/// carries it and an auditor reads the receipt. The execution contract's own
/// spelling ([`crate::backup_execution::TRIGGER_SCHEDULE`]), from which the
/// `Backup` reconciler derives the scheduled run identity.
pub const TRIGGERED_BY_SCHEDULE: &str = crate::backup_execution::TRIGGER_SCHEDULE;

/// The label carrying the `BackupSchedule` that created a `Backup`.
///
/// Duplicates `spec.scheduleRef` on purpose: `spec` is not selectable, and
/// `kubectl get backups -l logweir.dev/schedule=nightly` is what an operator
/// asks. Safe as a label because [`scheduled_backup_name`] has already refused
/// any schedule name that would not fit in 63 characters.
pub const SCHEDULE_LABEL: &str = "logweir.dev/schedule";

/// The label carrying the slot a `Backup` is for.
pub const SLOT_LABEL: &str = "logweir.dev/slot";

/// The label carrying the schedule's UID, which a recreated schedule does not
/// share.
pub const SCHEDULE_UID_LABEL: &str = crate::identity::SCHEDULE_UID_LABEL;

/// The label carrying the trigger kind, lowercase-hyphenated.
pub const TRIGGER_LABEL: &str = crate::identity::TRIGGER_LABEL;

/// The label carrying the attempt number.
pub const ATTEMPT_LABEL: &str = crate::identity::ATTEMPT_LABEL;

// No runner argv and no annotation carrying one: the Backup reconciler derives
// the argv from the typed spec and the server-generated run identity
// (`crate::backup_execution`).

/// `spec.deadlineSeconds` on every `Backup` this reconciler creates.
///
/// A constant, because `BackupSchedule` has no such field.
pub const SCHEDULED_DEADLINE_SECONDS: i64 = 3600;

/// How long after a slot came due this reconciler will still fire it.
///
/// One hour: a controller restarted after a week must not fire six days of
/// backlog. `crds/backup_schedule.rs`'s `lastMissedSlot` field description
/// states the horizon too.
#[must_use]
pub fn missed_slot_horizon() -> Duration {
    Duration::seconds(MISSED_SLOT_HORIZON)
}

/// [`missed_slot_horizon`] in seconds, as a plain integer.
pub const MISSED_SLOT_HORIZON: i64 = 3600;

/// How often this reconciler wakes with no event to wake it.
///
/// Thirty seconds, and it is the whole clock: no timer, lease or cron daemon.
/// Waking late is a recorded missed slot and waking twice is a 409, so this
/// only bounds the lateness, at half the shortest slot the grammar can express.
pub const REQUEUE_SECS: u64 = 30;

/// What one reconcile decided, and for a due slot the name it decided.
///
/// The name is in the decision, not computed beside the `POST`: a name minted
/// in the API-server-facing half could read a clock or a status unnoticed
/// (`the_name_never_reads_a_reconcile_clock_or_a_status`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SlotDecision {
    /// `spec.suspend` is true. Create nothing; clear `nextFireTime`.
    Suspended,
    /// `spec.schedule` did not parse. Create nothing.
    Unparseable(CronError),
    /// `spec.timeZone` is a well-shaped name this build's tz database does not
    /// have. Fail closed, never silently UTC: a schedule firing hours from
    /// where its operator expects is worse than one that visibly does not.
    UnknownTimeZone {
        /// The name as it was written.
        got: String,
    },
    /// `spec.retry` asks for retries this schedule's name cannot be extended
    /// to hold. Create nothing. The CRD refuses the edit; this arm covers an
    /// object admitted by an older CRD, rather than a schedule that looks
    /// configured and silently never retries.
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
    /// The latest due slot came due before this schedule's own
    /// `metadata.creationTimestamp`, so it is not this schedule's slot (the
    /// Kubernetes `CronJob` rule).
    ///
    /// Not a miss: nothing is recorded in `status.missedSlots` or
    /// `status.lastMissedSlot` and no `lastSlot` is written. `Ready` is `True`:
    /// a schedule waiting for its first slot is healthy. Produced only by
    /// [`bound_by_creation`], only from [`Self::Due`], [`Self::CatchUpDue`]
    /// and [`Self::Missed`], and it carries no name.
    BeforeCreation {
        /// The slot that came due before the schedule existed.
        due: DateTime<Utc>,
        /// That slot as [`slot_name`] spells it.
        slot: String,
        /// The schedule's `metadata.creationTimestamp`.
        created_at: DateTime<Utc>,
        /// `status.lastFireTime`, when it is at or after `due`: an older
        /// controller, with no creation bound, already fired this slot. Filled
        /// only by [`refine_against_status`]; it changes the message, never the
        /// decision.
        fired_at: Option<DateTime<Utc>>,
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// A slot came due, is older than [`MISSED_SLOT_HORIZON`], and
    /// `status.lastFireTime` says this schedule already fired it.
    ///
    /// The healthy steady state of every schedule slower than hourly, which
    /// keeps [`Self::Missed`] meaning what it says. Produced only by
    /// [`refine_against_last_fire`], only from [`Self::Missed`], never by
    /// [`decide`].
    AlreadyFired {
        /// The slot that came due.
        due: DateTime<Utc>,
        /// That slot as [`slot_name`] spells it.
        slot: String,
        /// What `status.lastFireTime` said, which is at or after `due`.
        last_fire_time: DateTime<Utc>,
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
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// A slot is past its starting deadline, `catchUpPolicy` is `Latest`, and
    /// it may still run. One catch-up, ever, and only for the latest due slot.
    CatchUpDue {
        /// The slot that came due.
        due: DateTime<Utc>,
        /// That slot as [`slot_name`] spells it.
        slot: String,
        /// The `Backup`'s `metadata.name`: the same name attempt 0 would have
        /// had, because a catch-up is that slot, started late.
        name: String,
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
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// A catch-up is admissible but concurrency blocks it. It stays admissible
    /// while this slot is the latest due one.
    CatchUpBlocked {
        /// The slot that is waiting.
        slot: String,
        /// The runs that block it.
        active_backups: Vec<String>,
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// The highest observed attempt of the latest due slot is still running,
    /// or has no terminal record yet.
    InProgress {
        slot: String,
        /// Which attempt.
        attempt: u32,
        /// The run's name.
        name: String,
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
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// A retry is admissible but its delay has not elapsed.
    RetryPending {
        slot: String,
        /// The attempt that failed.
        attempt: u32,
        /// When the retry becomes admissible.
        due_at: DateTime<Utc>,
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// A retry is due but concurrency blocks it.
    RetryBlocked {
        slot: String,
        /// The attempt that failed.
        attempt: u32,
        /// The runs that block it.
        active_backups: Vec<String>,
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// The attempt chain reached `maxRetries` without succeeding.
    RetryExhausted {
        slot: String,
        /// The last attempt.
        attempt: u32,
        /// The configured ceiling.
        max_retries: u32,
        /// Whether `spec.retry` was configured at all; when it was not, the
        /// reason reported is `RunFailed` rather than `RetryExhausted`, because
        /// nothing was exhausted.
        retry_configured: bool,
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// The latest attempt failed in a way that is never retried — a guard
    /// refusal, a drill that did not pass, a signing failure.
    RunFailed {
        slot: String,
        /// The attempt that failed.
        attempt: u32,
        name: String,
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// The deterministic name this slot needs is held by an object this
    /// schedule does not own.
    SlotNameUnavailable {
        slot: String,
        /// The name that is taken.
        name: String,
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// `Allow` has reached its ceiling of concurrent schedule-created runs.
    ActiveRunLimit {
        /// The slot that was not admitted.
        slot: String,
        /// The runs that are already going.
        active_backups: Vec<String>,
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// A write response came back without a field this controller wrote, which
    /// means the installed CRD is older than this controller (D1 §4.9).
    CrdOutdated {
        /// Which field the API server pruned.
        detail: String,
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
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// A slot is due, and the object name it needs does not fit.
    NameTooLong {
        /// That slot as [`slot_name`] spells it.
        slot: String,
        /// The refusal, naming the limit.
        error: SlotError,
        next_fire_time: Option<DateTime<Utc>>,
    },
    /// A slot is due. Create `name`.
    Due {
        /// The slot that came due.
        due: DateTime<Utc>,
        /// That slot as [`slot_name`] spells it.
        slot: String,
        /// The `Backup`'s `metadata.name`: a pure function of `due`.
        name: String,
        next_fire_time: Option<DateTime<Utc>>,
    },
}

impl SlotDecision {
    /// The condition `reason` for this decision.
    ///
    /// A `match` with no wildcard, so a new variant fails to compile until
    /// someone names its reason.
    ///
    /// [`Self::AlreadyFired`] reports `Scheduled`: a healthy nightly backup is
    /// a schedule being honoured, and a reason that changed every day for no
    /// operational reason would make the printer column noise. `SlotMissed`
    /// stays scarce so that it stays informative.
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
            // A chain never configured to retry exhausted nothing: with
            // `spec.retry` absent the reason is `RunFailed`, not "your retries
            // ran out" to an operator who asked for none.
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
            // A schedule waiting for its first slot is being honoured; the
            // message says which slot predated the schedule.
            Self::Due { .. }
            | Self::AlreadyFired { .. }
            | Self::InProgress { .. }
            | Self::BeforeCreation { .. } => REASON_SCHEDULED,
        }
    }

    /// The `Ready` condition's `status` for this decision.
    ///
    /// `True` means "the schedule is understood and being honoured", which
    /// includes [`Self::Missed`]: a recorded, reasoned skip of a stale slot is
    /// the honoured behaviour. `False` is reserved for the states an operator
    /// must act on, where this schedule will not fire as written: suspended,
    /// unparseable and unnameable.
    #[must_use]
    pub fn ready(&self) -> bool {
        match self {
            Self::Due { .. }
            | Self::AlreadyFired { .. }
            | Self::BeforeCreation { .. }
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
            // A suspended schedule has no next firing; the cron expression's
            // next instant would put a false future time in the NEXT column.
            Self::Suspended
            | Self::Unparseable(_)
            | Self::UnknownTimeZone { .. }
            | Self::InvalidTopicSelection { .. }
            | Self::InvalidRunPolicy { .. }
            | Self::RetryNamesTooLong { .. } => None,
            Self::NoDueSlot { next_fire_time }
            | Self::BeforeCreation { next_fire_time, .. }
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

    /// How long until this schedule should be looked at again:
    /// `min(REQUEUE_SECS, whatever this decision is waiting for)`. Only a retry
    /// serving out its delay waits for something sooner than the poll, so a
    /// bug here can cost a wasted wake-up and never a missed slot.
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
            // Names every problem, not the first, so one submission fixes all.
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
            Self::BeforeCreation {
                slot,
                created_at,
                fired_at: None,
                ..
            } => format!(
                "slot {slot} came due before this schedule was created at {}; a slot due \
                 before its schedule existed is never fired and is not counted as missed, and \
                 the first firing is {next}",
                created_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
            ),
            // The slot was fired by an older controller without the creation
            // bound; "never fired" beside its `lastFireTime` would be false.
            Self::BeforeCreation {
                slot,
                created_at,
                fired_at: Some(fired),
                ..
            } => format!(
                "slot {slot} came due before this schedule was created at {}; an earlier \
                 controller fired it (status.lastFireTime {}), and this one neither fires nor \
                 retries a slot due before its schedule existed; the next firing is {next}",
                created_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                fired.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
            ),
            // The healthy steady state names the fired slot and when it fired:
            // at 10:00 an adopter wants to read that midnight's slot ran.
            Self::AlreadyFired {
                slot,
                last_fire_time,
                ..
            } => format!(
                "slot {slot} was fired at {}; nothing further is due, and the next firing is \
                 {next}",
                last_fire_time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
            ),
            // This sentence names the slot, the horizon, that the slot was not
            // fired, and when the schedule resumes.
            Self::Missed { slot, reason, .. } => match reason {
                MissedReason::PastStartingDeadline => format!(
                    "slot {slot} is past its starting deadline and was not fired; \
                     catchUpPolicy is None, so it is counted in status.missedSlots and the next \
                     firing is {next}"
                ),
                // A catch-up that would predate the policy it ran under: editing
                // a schedule at noon must not retroactively back up the morning
                // under the new policy.
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
/// `metav1.Condition.message` allows 32 KiB, but ten problems is already more
/// than anyone reads in `kubectl describe`; the rest are counted, and the
/// API's 422 carries the full list.
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
        // Named, not blank: "none" reads as the fact it is.
        None => "none".to_string(),
    }
}

/// Decide what one `BackupSchedule` needs, including the object name.
///
/// A pure function of its three arguments: no clock (`now` is handed in), no
/// `status`, no environment, so [`SlotDecision::Due::name`] is a pure function
/// of the trigger. Two reconciles inside one slot compute the same `due`,
/// `slot` and `name`; the second `POST` collides and the 409 is the
/// idempotence key.
///
/// In order: `suspend` first (a suspended schedule's expression is never
/// parsed into a firing), then the policy validation, then the due slot, the
/// name, the starting deadline and the catch-up policy.
///
/// Whether a past-deadline catch-up predates the revision in force, and
/// whether a slot with no object was already fired, need the status:
/// [`refine_against_status`] answers them without minting a name.
#[must_use]
pub fn decide(name: &str, spec: &BackupScheduleSpec, now: DateTime<Utc>) -> SlotDecision {
    if spec.suspend {
        return SlotDecision::Suspended;
    }
    // Step 0, before the cron parse: a schedule whose topic selection cannot
    // produce a run has nothing to admit, and reporting the cadence first
    // would send the operator to the field that is not broken.
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
    // The CRD's ranges are not the only gate: an object created against an
    // older CRD, or read after the API server pruned a field, reaches here
    // without having passed them.
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
        // The CRD refuses the edit; this covers an object admitted by a CRD
        // that did not carry the rule.
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
    // `effective_since` is `None` here: it is a status field, so the
    // `BeforeRevision` arm belongs to the refinement.
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

/// `spec.startingDeadlineSeconds`, with its documented default.
#[must_use]
pub fn starting_deadline_seconds(spec: &BackupScheduleSpec) -> i64 {
    spec.starting_deadline_seconds
        .unwrap_or(crate::cadence::DEFAULT_STARTING_DEADLINE_SECONDS)
}

/// `spec.catchUpPolicy`, with its documented default.
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

/// Refine a decision's reporting against the two status instants it needs.
///
/// [`decide`] reads no status, which is what makes the object name a pure
/// function of the trigger, so the questions that need the status are asked
/// here, after the name was minted:
///
/// 1. A past-deadline slot ([`SlotDecision::Missed`] or
///    [`SlotDecision::CatchUpDue`]) at or before `last_fire_time` becomes
///    [`SlotDecision::AlreadyFired`]: the healthy steady state of every
///    schedule slower than its starting deadline.
/// 2. A [`SlotDecision::CatchUpDue`] older than `effective_since` becomes
///    `Missed` with reason `BeforeRevision`: a schedule edited at noon must not
///    retroactively back up the morning under the new policy.
/// 3. A [`SlotDecision::BeforeCreation`] whose slot is at or before
///    `last_fire_time` records that an older controller fired it (`fired_at`),
///    so its message does not say "never fired". It stays `BeforeCreation`.
///
/// Every other decision is returned untouched.
///
/// It cannot endanger G-SLOT: it takes instants, not a `BackupSchedule`; it
/// never constructs [`SlotDecision::Due`] or [`SlotDecision::CatchUpDue`], so
/// it cannot mint or change a name.
/// `a_crash_between_create_and_status_write_yields_exactly_one_backup` and
/// `the_name_never_reads_a_reconcile_clock_or_a_status` (which reads this
/// function's body) hold it. A separate function rather than more arguments to
/// `decide`, so the boundary is one a test can see.
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
        // The creation bound stays; only its wording learns the fire: the slot
        // is still not this controller's to fire or retry, but its message must
        // not say "never fired" beside the `lastFireTime` that records it.
        SlotDecision::BeforeCreation {
            due,
            slot,
            created_at,
            next_fire_time,
            ..
        } => SlotDecision::BeforeCreation {
            due,
            slot,
            created_at,
            fired_at: already_fired(due),
            next_fire_time,
        },
        other => other,
    }
}

/// Whether a slot due at `due` came due before a schedule created at
/// `created_at` existed.
///
/// Strictly before: `metadata.creationTimestamp` has one-second resolution and
/// every slot falls on a whole minute, so a slot at exactly the creation
/// second is the schedule's own. An object with no `creationTimestamp` (a test
/// fixture; the API server always sets it) is not bounded.
#[must_use]
pub fn slot_predates_creation(due: DateTime<Utc>, created_at: Option<DateTime<Utc>>) -> bool {
    created_at.is_some_and(|created| due < created)
}

/// Bound a decision by the schedule's own creation time.
///
/// A [`SlotDecision::Due`], [`SlotDecision::CatchUpDue`] or
/// [`SlotDecision::Missed`] whose slot came due before `created_at` becomes
/// [`SlotDecision::BeforeCreation`]; everything else is returned untouched, so
/// a slot missed after creation is still missed, caught up or retried.
///
/// A separate step for [`refine_against_status`]'s reason; it can only take a
/// name away, never construct `Due` or `CatchUpDue`, so it cannot endanger
/// G-SLOT. Only the latest due slot is bounded, which is enough: when it
/// predates creation every earlier slot does too, and the first slot at or
/// after creation is the next firing.
#[must_use]
pub fn bound_by_creation(
    decision: SlotDecision,
    created_at: Option<DateTime<Utc>>,
) -> SlotDecision {
    let (due, slot, next_fire_time) = match &decision {
        SlotDecision::Due {
            due,
            slot,
            next_fire_time,
            ..
        }
        | SlotDecision::CatchUpDue {
            due,
            slot,
            next_fire_time,
            ..
        }
        | SlotDecision::Missed {
            due,
            slot,
            next_fire_time,
            ..
        } => (*due, slot.clone(), *next_fire_time),
        _ => return decision,
    };
    match created_at {
        Some(created) if slot_predates_creation(due, created_at) => SlotDecision::BeforeCreation {
            due,
            slot,
            created_at: created,
            fired_at: None,
            next_fire_time,
        },
        _ => decision,
    }
}

/// One observed attempt of a slot, reduced to what the retry decision reads.
///
/// Observed deterministic objects, not a status field or a clock: the attempt
/// chain is discovered by `GET`ting `name(S, 0)`, `name(S, 1)`, … up to the
/// first 404, never by listing or by reading a counter the controller wrote.
/// A counter can disagree with the objects; the objects cannot disagree with
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
    /// Whether the failure is one that is retried.
    pub retryable: bool,
    /// When the terminal record was written: the `Failed` condition's
    /// `lastTransitionTime`, which the terminal patch writes once.
    pub finished_at: DateTime<Utc>,
}

/// What the attempt chain of the latest due slot says should happen next.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttemptDecision {
    /// No attempt of this slot exists. The caller falls through to creating
    /// attempt 0.
    NoAttempt,
    /// Some attempt succeeded. Nothing further is due for this slot.
    Succeeded {
        /// The attempt that succeeded.
        name: String,
        /// Which attempt it was.
        attempt: u32,
    },
    /// The highest attempt has no terminal record yet.
    InProgress {
        name: String,
        /// Which attempt it is.
        attempt: u32,
    },
    /// The highest attempt failed in a way that is never retried.
    NotRetryable {
        name: String,
        /// Which attempt it was.
        attempt: u32,
    },
    /// The chain reached the configured ceiling.
    Exhausted {
        /// The last attempt.
        attempt: u32,
        /// The ceiling.
        max_retries: u32,
        /// Whether `spec.retry` was configured at all.
        retry_configured: bool,
    },
    /// A retry is admissible but its delay has not elapsed.
    Pending {
        /// The attempt that failed.
        attempt: u32,
        /// When the retry becomes admissible.
        due_at: DateTime<Utc>,
    },
    /// A retry is due; create it. Concurrency is the caller's question.
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

/// What the observed attempt chain of the latest due slot says.
///
/// A second pure function: [`decide`] mints only the attempt-0 name from
/// `(name, spec, now)`; this one mints retry names from observed objects (the
/// chain the caller `GET`s), the retry policy and the instant. Neither reads a
/// status counter, so a controller that crashed between creating `-r1` and
/// recording it still sees exactly one `-r1`, and a lowered `maxRetries` still
/// sees the attempts that exist.
///
/// `observed` must be the chain in attempt order with no gaps; the caller
/// builds it by `GET`ting deterministic names up to the first 404.
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
    // A succeeded attempt ends the slot whatever a later object says. A later
    // object only exists because an earlier attempt failed, so "any" and "the
    // last" agree for every chain this controller creates; asking the whole
    // chain stays right if a human creates one by hand.
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
    // `Some(slot)` as the latest due slot: the caller only hands this
    // function the latest due slot; supersession is the skipped-slot
    // accounting's.
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
        // Unreachable (the caller passes its own latest due slot), and named
        // rather than wildcarded so a future caller passing another slot is
        // not silently absorbed.
        RetryAdmission::Superseded { by } => AttemptDecision::Pending {
            attempt: highest.attempt,
            due_at: by,
        },
    }
}

/// What one attempt's terminal record says, or `None` while it is still going.
///
/// `finished_at` is the `Failed` condition's `lastTransitionTime`, which the
/// terminal patch writes once; a terminal object with no such condition falls
/// back to nothing retryable.
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
    // No terminal condition means not retryable, by decision: the retry delay
    // is measured from the `Failed` condition's `lastTransitionTime`, and an
    // object without one (an older controller, or by hand) gives no instant,
    // so a retry would fire at an arbitrary time. The run is still terminal:
    // reporting it in progress would block every later `Forbid` slot for ever.
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

/// The policy half of the `Backup` this schedule would create: everything that
/// decides what a run does, none of the identity that decides which run it is.
///
/// One builder, three readers: [`decide`] validates it, [`run_policy_digest`]
/// digests it, and [`scheduled_backup`] fills in the identity and POSTs it. A
/// second construction is how a schedule comes to admit a policy the run then
/// refuses, or to record a digest over fields the run does not carry.
///
/// `deadlineSeconds` resolves the absent default here, because it is inside
/// the digested document: a schedule that says nothing and one that says 3600
/// ask for the same run and must digest the same.
#[must_use]
pub fn run_policy_spec(spec: &BackupScheduleSpec) -> BackupSpec {
    BackupSpec {
        source_ref: spec.source_ref.clone(),
        topics: spec.topics.clone(),
        archive: spec.archive.clone(),
        destination_ref: spec.destination_ref.clone(),
        all_user_topics: spec.all_user_topics.clone(),
        consumer_groups: spec.consumer_groups.clone(),
        schedule_ref: None,
        slot: None,
        triggered_by: TRIGGERED_BY_SCHEDULE.to_string(),
        trigger: None,
        deadline_seconds: spec
            .active_deadline_seconds
            .unwrap_or(SCHEDULED_DEADLINE_SECONDS),
    }
}

/// `sha256:<lowercase hex>` over this schedule's run policy.
///
/// It covers the source, the topic selection, the archive and the run
/// deadline: what a run does. Cadence, zone, deadlines, catch-up, retry,
/// concurrency, retention and `suspend` are excluded, so flipping `suspend`
/// moves `metadata.generation` and leaves this alone.
#[must_use]
pub fn run_policy_digest(spec: &BackupScheduleSpec) -> String {
    crate::policy::run_policy_sha256(&run_policy_spec(spec))
}

/// The `Backup` object one due slot produces.
///
/// Pure, so the owner UID is an argument: the object a test builds is
/// byte-identical to the one the reconciler `POST`s.
///
/// A `Backup` always runs exactly the generation it records, so both the
/// policy and the generation come from the same in-memory `BackupSchedule`
/// the reservation was made against, never a generation passed in:
/// `spec.scheduleRef.generation` is `schedule.metadata.generation` by
/// construction, and `spec.scheduleRef.runPolicySha256` is the digest of the
/// fields this object copied, which `identity::check_run_policy_digest`
/// recomputes.
///
/// No annotation: `spec.triggeredBy: schedule`, `spec.scheduleRef`,
/// `spec.slot` and the deterministic `name` are exactly the facts
/// [`crate::backup_execution::execution_identity`] requires, and the run
/// identity is `backup_id_for(schedule_uid, slot)`. No owner reference to the
/// schedule, so deleting the schedule keeps its history.
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
/// A value, not four arguments: a retry's name, attempt and `retryOf` are one
/// derivation ([`crate::slot::scheduled_backup_name_for_attempt`]), and
/// splitting them is how a `-r1` object comes to claim attempt 0.
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
            // No ownerReference to the schedule: deleting a schedule must not
            // hand its runs, their plans and Jobs to the garbage collector.
            // Membership is `spec.scheduleRef {name, uid}` plus the labels below
            // ([`crate::identity::is_run_of_schedule`]), which survives the
            // owner being deleted. A run still owns its own plan ConfigMap and
            // Job, so deleting a run still collects what it made.
            ..ObjectMeta::default()
        },
        spec: BackupSpec {
            // `name` and `uid`, because a schedule recreated under the same
            // name is a different schedule and must not adopt this run;
            // `generation` and `runPolicySha256` because the policy is
            // editable, so "which schedule" does not answer "under which
            // policy".
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
                // Informational: the slot is a UTC instant whatever the zone;
                // this records the zone it was computed in, so a history row
                // keeps its local time after `spec.timeZone` is edited.
                time_zone: schedule.spec.time_zone.clone(),
            }),
            ..policy
        },
        status: None,
    }
}

/// Whether `backup` is a run of this schedule that participates in
/// `concurrencyPolicy`.
///
/// Membership and accounting are two questions.
/// [`crate::identity::is_run_of_schedule`] answers membership, and a manual run
/// created from a schedule is a member (it carries `spec.scheduleRef`, appears
/// in the history view, and the inventory must see it). Only schedule-created
/// runs (`Scheduled`, `CatchUp`, `Retry`) participate in `concurrencyPolicy`:
/// "Back up now" follows the CronJob precedent and neither is blocked by a
/// running slot nor blocks the next one.
///
/// [`crate::identity::declared_trigger`], not `run_identity`: it is infallible
/// and reads a legacy object with no `spec.trigger` and `triggeredBy:
/// schedule` as `Scheduled`/0. A run whose identity does not compose must
/// still count as active, or `Forbid` would weaken for exactly the malformed
/// objects that most deserve it.
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

/// The `/status` merge patch one slot reservation is.
///
/// A patch, not a PUT: `Api::replace_status` is the verb `update` on
/// `backupschedules/status`, which the shipped ClusterRole deliberately grants
/// nowhere (`charts/logweir/templates/clusterrole.yaml`), so a replace would be
/// 403 on every install and, with the default `Forbid`, nothing would ever be
/// created. The compare-and-set survives: a `metadata.resourceVersion` in a
/// patch body is an update precondition, answered `409 Conflict` on mismatch
/// ([`status_patch_with_preconditions`]).
///
/// Explicit `null`s, because an absent key in a merge patch means "leave it
/// alone": the cleared active reference and the accepted pending one are both
/// always written.
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
    // The generation travels with the reservation: the patch carries
    // `metadata.resourceVersion`, so an edit landing between the read and this
    // write gets a 409 and the reconcile restarts under the new generation.
    // When accepted, it is the generation the `Backup` created from the same
    // in-memory object records, so a run's copied policy always equals the
    // schedule spec at the generation written inside it.
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
                // The actual policy, not a literal: `admit` is the path for both
                // policies. The final write replaces it moments later, but a
                // watcher or the console can read it.
                message: Some(format!(
                    "{}; concurrencyPolicy {:?} atomically admitted this slot and is creating \
                     the Backup",
                    decision.message(),
                    schedule.spec.concurrency_policy
                )),
            },
        )]),
    );
    json!({ "status": serde_json::Value::Object(status) })
}

/// The slot, instant and attempt a deterministic run name spells, or `None`
/// when the name is not one this schedule could have minted.
///
/// It accepts `-r<k>`: a reservation written as a bare `pendingBackupRef` by
/// an older controller carries no attempt, which has to be recovered from the
/// name, and a misread retry suffix would clear an accepted reservation as
/// stale. The composed name is compared back against the parse, so a name that
/// is not `name(schedule, slot, attempt)` is rejected rather than
/// half-understood.
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
/// Built as JSON, not by serialising `BackupScheduleStatus`, whose `None`
/// fields serialise as absent keys, which a merge patch reads as "leave it
/// alone"; clearing `nextFireTime` for a suspended schedule needs an explicit
/// `null`.
///
/// `lastFireTime` is omitted unless a child was created or observed for the
/// decision. `activeBackupRef` and `pendingBackupRef` change only when the
/// API-facing reconciliation has actual owned-run information.
/// `lastMissedSlot` is written by [`SlotDecision::Missed`] and
/// [`SlotDecision::ConcurrencyBlocked`] and never cleared: it is the audit
/// trail of a skip. [`SlotDecision::AlreadyFired`] does not rewrite the fire
/// time, so a steady reconcile cannot move the timestamp an operator uses to
/// tell when the backup actually ran.
///
/// `lastTransitionTime` moves only when the condition transitions
/// ([`crate::conditions::merge_condition`]), and `lastSlot.decidedAt` likewise
/// ([`crate::conditions::keep_instant_unless_changed`]): this reconciler
/// requeues every [`REQUEUE_SECS`], and a fresh instant on every write would
/// be 2,880 `resourceVersion` bumps a day on an unchanged schedule. Every
/// struct block goes through [`crate::conditions::replacing`], so a field that
/// became `None` is cleared rather than left behind.
#[must_use]
pub fn status_patch(
    schedule: &BackupSchedule,
    decision: &SlotDecision,
    created: Option<&str>,
    now: DateTime<Utc>,
) -> serde_json::Value {
    let work = Reconcile {
        history: None,
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
    status_patch_with_refs(schedule, decision, &work, now)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PendingRefUpdate {
    Keep,
    Clear,
}

/// The `status.policy` block this reconcile should carry, and whether
/// `observedGeneration` has to move.
///
/// `generation`, `runPolicySha256` and `timeZone` are facts about the object.
/// `effectiveSince` is the instant this revision was first observed, kept
/// while the generation is unchanged: a catch-up decides against it, and
/// refreshing it every pass would make every catch-up look "before the
/// revision".
///
/// `evaluatedAt` is the instant the status last moved, not the instant the
/// controller last looked: an instant rewritten on every 30-second pass would
/// bump `resourceVersion` 2,880 times a day and wake this reconciler's own
/// watch. It is built from the stored value and rewritten to `now` only when
/// the status is written anyway. So `evaluatedAt` is not a liveness probe; the
/// staleness signal is `status.nextRuns[0].at` in the past by more than a
/// couple of requeues, because a live controller rewrites `nextRuns` when its
/// first entry passes.
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

/// The next five firings, or `[]` when this schedule will not fire.
///
/// Computed in Rust and nowhere else: the browser never evaluates cron, since
/// a second DST implementation is a second answer to a question that must have
/// one.
fn next_runs(schedule: &BackupSchedule, decision: &SlotDecision) -> Vec<NextRun> {
    let base = match decision.next_fire_time() {
        // A schedule that will not fire advertises no firings.
        None => return Vec::new(),
        Some(base) => base,
    };
    let Ok(cadence) = Cadence::parse(&schedule.spec.schedule, schedule.spec.time_zone.as_deref())
    else {
        return Vec::new();
    };
    // `base - 1s` so the first entry is `nextFireTime` itself: `next_runs`
    // walks strictly after its argument, and the preview's first row must
    // match the NEXT column of `kubectl get`.
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
    now: DateTime<Utc>,
) -> serde_json::Value {
    let mut status = serde_json::Map::new();
    // The stored status as the API server holds it. Every struct block goes
    // through `replacing(…, stored)`, so a field that became `None` is sent as
    // `null` and cleared, and every "when computed" instant is kept by
    // `keep_instant_unless_changed`, comparing the block as it will be stored
    // with the instant left out. A typed comparison read a stale
    // `lastSlot.backupRef` as a new decision on every pass and spun the
    // schedules at ~120 writes a second.
    let stored = schedule
        .status
        .as_ref()
        .and_then(|s| serde_json::to_value(s).ok());
    let stored_block = |key: &str| {
        stored
            .as_ref()
            .and_then(|s| s.get(key))
            .filter(|v| !v.is_null())
    };
    // Written on every final patch, so an operator can read which revision
    // the controller acted on and what its run policy digests to; a `suspend`
    // flip moves `generation` and leaves `runPolicySha256` alone.
    status.insert(
        "observedGeneration".to_string(),
        json!(schedule.metadata.generation.unwrap_or_default()),
    );
    status.insert(
        "policy".to_string(),
        replacing(json!(policy_status(schedule, now)), stored_block("policy")),
    );
    status.insert("nextRuns".to_string(), json!(next_runs(schedule, decision)));
    status.insert(
        "nextFireTime".to_string(),
        match decision.next_fire_time() {
            Some(t) => json!(t),
            // Explicit null, not an omitted key (see above).
            None => serde_json::Value::Null,
        },
    );
    // `lastMissedSlot` is the audit field: written when a slot is skipped or
    // blocked and never cleared. `status.missedSlots` is the richer block
    // beside it, written from the same decision.
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
        // `decidedAt` is a "when computed" field and moves only when the
        // decision moves; rewritten each pass it would make every status
        // differ. A decision that names no `Backup` after one that did has
        // `backupRef: None`, an absent key that could never clear the stored
        // one, so the reference is cleared (`replacing`) and the comparison is
        // made on what the API server will store
        // (`keep_instant_unless_changed`).
        let mut next = replacing(json!(last), stored_block("lastSlot"));
        keep_instant_unless_changed(stored_block("lastSlot"), &mut next, "decidedAt");
        status.insert("lastSlot".to_string(), next);
    }
    if let Some(missed) = work.missed.as_ref() {
        status.insert(
            "missedSlots".to_string(),
            replacing(json!(missed), stored_block("missedSlots")),
        );
    }
    // `lastFireTime` is the due instant, never the reconcile clock. The
    // fallback keeps [`status_patch`] a pure function: a caller handing in an
    // admitting decision and no observation still records the fire it names.
    // `AlreadyFired` is not in the list: a steady reconcile must not move the
    // timestamp an operator uses to tell when the backup ran.
    if let Some(due) = work.fired_due.or(match decision {
        SlotDecision::Due { due, .. }
        | SlotDecision::CaughtUp { due, .. }
        | SlotDecision::Retried { due, .. } => Some(*due),
        _ => None,
    }) {
        status.insert("lastFireTime".to_string(), json!(due));
    }
    // `activeRuns` is always written, `[]` included: that turns the next
    // reconcile into the O(active) branch instead of a namespace LIST. Absent
    // means "never inventoried"; empty means "there are none".
    status.insert("activeRuns".to_string(), json!(work.active));
    status.insert(
        "activeBackupRef".to_string(),
        match work.active.first() {
            Some(run) => json!({ "name": run.name }),
            None => serde_json::Value::Null,
        },
    );
    if work.pending == PendingRefUpdate::Clear {
        // Both keys, always: `pendingRun` is the typed reservation and
        // `pendingBackupRef` the mirror older readers use, and clearing one
        // would show a reservation that no longer exists to the other.
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
    // The `HistoryRetained` condition. A merge patch replaces
    // `status.conditions`, so a builder that emits only its own conditions
    // deletes the others; and a copied condition outlives the facts it came
    // from. So it is computed from `status.history` (this pass's block when it
    // took an inventory, the stored one otherwise) by
    // [`crate::controllers::schedule_history::history_condition`].
    //
    // With no block at all, the stored condition is carried: a schedule never
    // inventoried has no facts, and inventing `Retained` would be a claim
    // nothing observed. The reconciler always inventories when `history` is
    // absent, but the public pure `status_patch` helpers can reach this.
    let history = work
        .history
        .clone()
        .or_else(|| schedule.status.as_ref().and_then(|s| s.history.clone()));
    let stored_retained = current_condition(
        schedule.status.as_ref().and_then(|s| s.conditions.as_ref()),
        crate::conditions::CONDITION_HISTORY_RETAINED,
    );
    match history {
        Some(history) => {
            conditions.push(crate::controllers::schedule_history::history_condition(
                &history,
                stored_retained,
                schedule.metadata.generation,
                now,
            ));
            // `replacing`, as for `lastSlot`: a migration no longer blocked has
            // `migrationBlocked: None`, and an absent key would leave the old
            // sample for the condition above to keep reading.
            status.insert(
                "history".to_string(),
                replacing(json!(history), stored_block("history")),
            );
        }
        None => {
            if let Some(retained) = stored_retained {
                conditions.push(retained.clone());
            }
        }
    }
    // `spec.retention` no longer reports. A schedule that still sets it backs
    // up as before and says so here, by name.
    if schedule.spec.retention.is_some() {
        conditions.push(merge_condition(
            current_condition(
                schedule.status.as_ref().and_then(|s| s.conditions.as_ref()),
                crate::conditions::CONDITION_RETENTION_REPORT_WITHDRAWN,
            ),
            Condition {
                r#type: crate::conditions::CONDITION_RETENTION_REPORT_WITHDRAWN.to_string(),
                status: "True".to_string(),
                observed_generation: schedule.metadata.generation,
                last_transition_time: Some(now),
                reason: Some(crate::conditions::REASON_USE_RETENTION_POLICY.to_string()),
                message: Some(crate::conditions::RETENTION_REPORT_WITHDRAWN_MESSAGE.to_string()),
            },
        ));
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
    /// Whether the `POST` was answered **409 `AlreadyExists`**: the mechanism
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
    /// reference can be built. Unreachable from the API server, and named: a
    /// `backup_id` built from a name instead would reintroduce the
    /// cross-namespace collision [`crate::slot::backup_id_for`] prevents.
    NoUid(String),
    /// A status admission or finalization needs the API server's
    /// optimistic-concurrency token.
    MissingResourceVersion(String),
    /// The deterministic name for a due slot is already held by an object this
    /// schedule UID does not own.
    ForeignBackup(String),
    /// A write response came back without a field this controller wrote: the
    /// installed CRD is older than this controller.
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

/// The history step's two failures, rejoining the scheduler's. `ForeignBackup`
/// keeps its meaning (the reservation names an object this schedule does not
/// own) and maps to the variant that already exists for it.
impl From<crate::controllers::schedule_history::HistoryError> for ScheduleError {
    fn from(e: crate::controllers::schedule_history::HistoryError) -> Self {
        match e {
            crate::controllers::schedule_history::HistoryError::Api(e) => Self::Api(e),
            crate::controllers::schedule_history::HistoryError::ForeignBackup(name) => {
                Self::ForeignBackup(name)
            }
        }
    }
}

impl From<serde_json::Error> for ScheduleError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serialization(e)
    }
}

/// Reconcile one `BackupSchedule` at the instant `now`.
///
/// No `Utc::now()` in this function: the clock is read once, by the
/// `kube::runtime` wrapper, before [`decide`] runs. A second clock read
/// between the decision and the `POST` is what
/// `a_crash_between_create_and_status_write_yields_exactly_one_backup` kills.
///
/// The status is read only for the reason: `status.lastFireTime` reaches
/// [`refine_against_last_fire`] and decides `SlotMissed` versus
/// `AlreadyFired`, never what anything is called.
///
/// `create`, then `status`, and a 409 is success: a duplicate reconcile
/// computes the same name, the 409 is logged at `debug` as the mechanism
/// working, and the status write happens either way, which makes a crash
/// between the two harmless.
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

/// [`reconcile_schedule`], with the controller's archive handle.
///
/// The schedule controller reads no archive: the handle was for the withdrawn
/// per-schedule retention report, and the parameter stays only so the
/// reconcile entry points keep the shape their tests pin.
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
    reconcile_schedule_with_archive_at(schedule, client, archive, None, now).await
}

/// [`reconcile_schedule_with_archive`] with the controller's archive location.
/// Neither the handle nor the location is read; see above.
///
/// # Errors
///
/// [`ScheduleError`] for anything that is not a decision.
pub async fn reconcile_schedule_with_archive_at(
    schedule: &BackupSchedule,
    client: &kube::Client,
    _archive: Option<&Arc<Store>>,
    _controller_archive_url: Option<&str>,
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

    // Step 0. The name is decided without the status, the reporting with it:
    // two statements, so the pure decision is a value held before anything
    // refines how it is reported.
    //
    // `effectiveSince` is read only for the revision in force: an older
    // generation's block says when that revision started, which would make a
    // catch-up decide against the wrong instant.
    //
    // The creation bound comes first: a slot that predates the object is not
    // this schedule's, so no refinement may report it fired or missed.
    let decision = bound_by_creation(
        decide(&name, &schedule.spec, now),
        schedule.metadata.creation_timestamp.as_ref().map(|t| t.0),
    );
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
        history: None,
        created: None,
        already_existed: false,
        fired_due: None,
        active: Vec::new(),
        pending: PendingRefUpdate::Keep,
        last_slot: None,
        missed: stored.and_then(|s| s.missed_slots.clone()),
        status_write_base: schedule.clone(),
    };

    // Step 1: the active set.
    let reservation = stored.and_then(|status| reserved_run(&name, status));
    // A reservation neither spelling parses is stale status, and must be
    // cleared: only a deterministic scheduled name over a real instant can be
    // a reservation this controller could resume. Left in place, a suspended
    // schedule would show accepted work that does not exist, for ever, in
    // `kubectl get`, the console and the staleness alert.
    if reservation.is_none()
        && stored.is_some_and(|status| {
            status.pending_run.is_some() || status.pending_backup_ref.is_some()
        })
    {
        work.pending = PendingRefUpdate::Clear;
    }
    // Step 1 is `schedule_history::observe`: it GETs the ≤ 10 recorded names in
    // steady state, and takes one paginated, `limit`ed, label-selected
    // inventory when one is due, which also migrates legacy ownerReferences
    // and produces the `status.history` block the `HistoryRetained` condition
    // is computed from. Never a namespace-wide unbounded list.
    let observed = crate::controllers::schedule_history::observe(
        &backups_api,
        stored,
        &name,
        &uid,
        reservation.as_ref().map(|r| r.name.as_str()),
        now,
    )
    .await?;
    work.active = observed.active;
    work.history = Some(observed.history);

    // Step 2: an accepted reservation whose child does not exist.
    if let Some(reserved) = reservation.as_ref() {
        match observed.pending_child {
            // The child exists: the reservation did its job and is cleared. A
            // terminal child also records the fire, because the slot did run.
            Some(child) => {
                work.pending = PendingRefUpdate::Clear;
                work.already_existed = true;
                work.created = Some(reserved.name.clone());
                if backup_is_terminal(&child) {
                    work.fired_due = Some(reserved.due);
                }
            }
            None => {
                // An invalid run policy releases the reservation rather than
                // turning it into a run the Backup controller refuses
                // terminally. Cadence and time-zone invalidity do not: the
                // reserved slot is already a UTC instant, and the policy the
                // child would copy is still usable.
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
                return finish(client, &namespace, &name, decision, work, now).await;
            }
        }
    }

    // Steps 3 and 4: suspended, or a policy that cannot run.
    if !matches!(
        decision,
        SlotDecision::Due { .. }
            | SlotDecision::CatchUpDue { .. }
            | SlotDecision::Missed { .. }
            | SlotDecision::AlreadyFired { .. }
    ) {
        return finish(client, &namespace, &name, decision, work, now).await;
    }

    // Step 5: the latest due slot, and what was skipped to reach it.
    let Some((due, slot)) = due_slot(&decision) else {
        return finish(client, &namespace, &name, decision, work, now).await;
    };

    // Step 6: the attempt chain of S.
    let retry = schedule.spec.retry.map(|r| r.policy());
    let chain = observe_attempt_chain(
        &backups_api,
        &name,
        &uid,
        &slot,
        walk_retries(schedule, stored, &work.active),
    )
    .await?;

    let attempt = decide_attempt(&name, due, &chain.attempts, retry, now);
    // Asked after the chain has been read: the name the next admission needs
    // is held by an object this schedule does not own, so the slot is skipped
    // and recorded, never re-run under another name. Here rather than in the
    // walk, so a foreign object at a later attempt cannot erase what earlier
    // attempts proved.
    if slot_name_is_unavailable(&chain, &attempt) {
        let held = chain.foreign_next.clone().unwrap_or_default();
        decision = SlotDecision::SlotNameUnavailable {
            slot: slot.clone(),
            name: held,
            next_fire_time: decision.next_fire_time(),
        };
        let attempt_index = u32::try_from(chain.attempts.len()).unwrap_or(0);
        work.record_slot(&decision, &slot, due, attempt_index, None, now);
        work.account_skipped(schedule, &decision, &slot, due, now, true);
        return finish(client, &namespace, &name, decision, work, now).await;
    }
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
            // `created` means "the run this slot has", not "the object this
            // reconcile POSTed": finding the run already there is the same
            // outcome, which makes a crash between create and status write
            // harmless (the G-SLOT guard asserts it).
            work.created = Some(run.clone());
            work.already_existed = true;
            work.record_slot(&decision, &slot, due, attempt, Some(run), now);
            work.account_skipped(schedule, &decision, &slot, due, now, true);
            return finish(client, &namespace, &name, decision, work, now).await;
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
            return finish(client, &namespace, &name, decision, work, now).await;
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
            return finish(client, &namespace, &name, decision, work, now).await;
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
            let last = chain.attempts.last().map(|a| a.name.clone());
            work.created = last.clone();
            work.already_existed = last.is_some();
            work.record_slot(&decision, &slot, due, attempt, last, now);
            work.account_skipped(schedule, &decision, &slot, due, now, true);
            return finish(client, &namespace, &name, decision, work, now).await;
        }
        AttemptDecision::Pending { attempt, due_at } => {
            decision = SlotDecision::RetryPending {
                slot: slot.clone(),
                attempt,
                due_at,
                next_fire_time: decision.next_fire_time(),
            };
            work.fired_due = Some(due);
            work.created = chain.attempts.last().map(|a| a.name.clone());
            work.already_existed = work.created.is_some();
            work.account_skipped(schedule, &decision, &slot, due, now, false);
            return finish(client, &namespace, &name, decision, work, now).await;
        }
        AttemptDecision::NameTooLong { error } => {
            decision = SlotDecision::RetryNamesTooLong { error };
            work.account_skipped(schedule, &decision, &slot, due, now, false);
            return finish(client, &namespace, &name, decision, work, now).await;
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
                return finish(client, &namespace, &name, decision, work, now).await;
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
            return finish(client, &namespace, &name, decision, work, now).await;
        }
    }

    // Step 7: no attempt of S exists.
    //
    // A slot at or before `lastFireTime` with no object was fired and its
    // history pruned. A fired slot is never re-run: a second run under the
    // same execution id leaves a partial archive a later drill restores from
    // and calls a success.
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
        return finish(client, &namespace, &name, decision, work, now).await;
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

    finish(client, &namespace, &name, decision, work, now).await
}

/// `status.lastSlot.disposition` values. A closed set, spelled once.
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

/// `status.missedSlots.recent[].reason` values.
const MISSED_CONTROLLER_UNAVAILABLE: &str = "ControllerUnavailable";
const MISSED_CONCURRENCY_BLOCKED: &str = "ConcurrencyBlocked";
const MISSED_PAST_DEADLINE: &str = "PastStartingDeadline";
const MISSED_BEFORE_REVISION: &str = "BeforeRevision";
const MISSED_NAME_UNAVAILABLE: &str = "NameUnavailable";

/// The mutable half of one reconcile: what it created, what it observed and
/// what it will write.
///
/// A struct, not nine locals: the algorithm has ten exits and every one writes
/// the same status, and nine `let mut`s through ten early returns is how one
/// forgets `activeRuns`.
struct Reconcile {
    /// What the history step recorded about this schedule's retained runs;
    /// `None` when reached through the pure `status_patch` helpers.
    history: Option<crate::crds::backup_schedule::ScheduleHistory>,
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

    /// Count the slots that came due between the last one this schedule
    /// finished deciding about and this one.
    ///
    /// `final_disposition` is the correctness argument: `lastEvaluatedSlot`
    /// advances only when the current slot has a disposition it cannot come
    /// back from, so a slot that waits for concurrency and is then superseded
    /// is inside the interval exactly once, and a reconcile thirty seconds
    /// later does not count the same gap again.
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
        // The guard that makes "exactly once" true. The early return above
        // suppresses a non-final disposition and the `previous < due` filter
        // the gap enumeration; neither stops the same slot reaching the same
        // final disposition on the next pass, which is a missed slot's steady
        // state. On a 30-second requeue a daily schedule would inflate `count`
        // by ~2,880 for one slot, fill `recent` with copies of it and bump
        // `resourceVersion` every time. `SlotNameUnavailable` takes the same
        // path.
        if missed.last_evaluated_slot.as_deref() == Some(slot) {
            return;
        }
        let previous = missed
            .last_evaluated_slot
            .as_deref()
            .and_then(slot_instant)
            .filter(|previous| *previous < due);
        let mut recent: Vec<MissedSlot> = Vec::new();
        let mut gap = 0usize;
        if let Some(previous) = previous {
            if let Ok(cadence) =
                Cadence::parse(&schedule.spec.schedule, schedule.spec.time_zone.as_deref())
            {
                // Both ends exclusive: `previous` already has a disposition and
                // `due` is being given one now.
                let skipped = cadence.skipped_slots(previous, due, MAX_SKIPPED_SLOT_ENUMERATION);
                gap = skipped.count;
                missed.count = missed
                    .count
                    .saturating_add(i64::try_from(skipped.count).unwrap_or(i64::MAX));
                missed.count_capped =
                    missed.count_capped || skipped.capped || skipped.horizon_reached;
                // The gap's names are not enumerated into the status: a week of
                // one-minute slots is ten thousand, and the count is the fact an
                // operator acts on.
            }
        }
        if let Some(reason) = missed_reason(decision) {
            missed.count = missed.count.saturating_add(1);
            recent.push(MissedSlot {
                slot: slot.to_string(),
                reason: reason.to_string(),
                recorded_at: now,
            });
        } else if previous.is_some() && gap > 0 {
            // The boundary entry: the gap moved the count but this slot got a
            // disposition of its own, so `recent` would otherwise hold nothing
            // for a downtime that counted dozens of slots. One entry naming the
            // slot the accounting caught up at, with the reason the gap was
            // skipped, makes the count legible.
            recent.push(MissedSlot {
                slot: slot.to_string(),
                reason: MISSED_CONTROLLER_UNAVAILABLE.to_string(),
                recorded_at: now,
            });
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
        | SlotDecision::BeforeCreation { .. }
        | SlotDecision::NameTooLong { .. }
        | SlotDecision::CrdOutdated { .. }
        | SlotDecision::InProgress { .. }
        | SlotDecision::RetryPending { .. } => DISPOSITION_DONE,
    }
}

/// Why this slot was skipped, or `None` when it was not skipped at all.
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

/// The instant a `yyyymmdd-hhmmss` slot name spells, or `None` when the
/// calendar has no such instant.
///
/// No `format(…) == slot` round trip: for `%Y%m%d-%H%M%S` the parser refuses
/// every out-of-range field (measured on `20261309-031700`, `20260230-000000`,
/// `20260915-256100`, `00000000-000000`, `20260915-236000`, `20261232-000000`,
/// `20260931-000000` and `99999999-999999`); the only odd string it accepts,
/// the leap second `20260915-235960`, round-trips to itself. A round trip
/// would be a guard no mutant can kill. The parse refusing is what matters: a
/// `status.pendingBackupRef` naming a non-calendar slot is stale status, which
/// the composed-name check cannot detect.
/// `a_reservation_naming_an_impossible_slot_is_cleared_rather_than_resumed`
/// drives four such names through a reconcile.
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
/// The typed block first, the mirror as a fallback: `status.pendingRun`
/// carries the slot, attempt and kind; `status.pendingBackupRef` is a name an
/// older controller wrote, from which all three can be recovered, so a
/// roll-forward resumes an accepted slot instead of abandoning it. A
/// reservation neither spelling parses into is stale status, and is cleared.
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
    // The name is re-derived and compared, not trusted: a `pendingRun` whose
    // name is not `name(schedule, slot, attempt)` is not a reservation this
    // controller made, and resuming it would create an object
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

/// The attempt chain of one slot, by GET of deterministic names, stopping at
/// the first 404.
///
/// Never by listing: a list is O(history) and answers "which objects carry
/// this label" rather than "does attempt k's object exist", which is the
/// question admission turns on.
///
/// The walk goes to the hard maximum regardless of the current `maxRetries`,
/// so lowering it still sees existing attempts. `walk_retries` skips past
/// attempt 0 only on a schedule that has never had a retry policy and records
/// no attempt above 0, where nothing can be found (this controller is the only
/// writer of those names), so the steady-state reconcile costs one GET.
async fn observe_attempt_chain(
    api: &Api<Backup>,
    schedule_name: &str,
    uid: &str,
    slot: &str,
    walk_retries: bool,
) -> Result<ObservedChain, ScheduleError> {
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
        let found = api.get_opt(&name).await.map_err(ScheduleError::Api)?;
        let Some(backup) = found else { break };
        // A `Manual` Backup can carry a scheduled-looking name. Such an object
        // is a foreign occupant, not an attempt of this slot: it runs under its
        // own UID and execution id, so reading it as attempt k would claim a
        // window was covered by a run that wrote a different archive. The slot
        // is reported `SlotNameUnavailable` and never re-run under another
        // name.
        //
        // The walk stops here and the attempts already observed still count,
        // or a foreign `-r1` would erase a succeeded attempt 0 beside it. The
        // caller decides which fact wins ([`slot_name_is_unavailable`]), on
        // what the next admission would need.
        if !participates_in_concurrency(&backup, schedule_name, uid) {
            return Ok(ObservedChain {
                attempts: chain,
                foreign_next: Some(name),
            });
        }
        chain.push(ObservedAttempt {
            name,
            attempt,
            outcome: attempt_outcome(&backup),
        });
    }
    Ok(ObservedChain {
        attempts: chain,
        foreign_next: None,
    })
}

/// The attempt chain of one slot, and the name the walk stopped at.
///
/// `foreign_next` is the deterministic name of the attempt after the last one
/// in `attempts`, when something this schedule does not own holds it: the
/// name the next admission for this slot would need.
struct ObservedChain {
    attempts: Vec<ObservedAttempt>,
    foreign_next: Option<String>,
}

/// Whether the foreign occupant blocks what this reconcile would actually do.
///
/// A held name matters only when something would be created under it: step 7's
/// admission of `name(S, 0)` and a retry of `name(S, k+1)`, where the name the
/// walk stopped at is the one that would be POSTed. Every other decision
/// creates nothing, so the attempts speak: a succeeded attempt 0 beside a
/// squatted `-r1` is `AlreadyFired`, because the window was covered.
fn slot_name_is_unavailable(chain: &ObservedChain, attempt: &AttemptDecision) -> bool {
    chain.foreign_next.is_some()
        && matches!(
            attempt,
            AttemptDecision::NoAttempt | AttemptDecision::Retry { .. }
        )
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

/// The runs that block a new admission.
///
/// `Forbid`: any nonterminal schedule-created run of this schedule. `Allow`:
/// the ceiling [`MAX_ACTIVE_RUNS`], and nothing below it. Manual runs are
/// neither counted nor blocked: they never appear in `activeRuns`.
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
/// A 409 proves only that something holds the name: the winner is fetched and
/// must carry this schedule's membership before it counts as same-slot
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
            // The API server silently prunes a field an older CRD does not
            // declare, so the response is read back: a created object with no
            // `spec.trigger` or `spec.scheduleRef.uid` has no identity, and the
            // Backup controller refuses it before any POST.
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

/// Reserve, then create from the same in-memory object.
///
/// The reservation is uniform for `Forbid` and `Allow`: it makes
/// `status.activeRuns` complete by construction (every schedule-created run is
/// recorded before it exists), and a controller that dies between the two
/// finds the reservation on restart and resumes exactly that child.
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
    // The reservation response must contain what was written: an older CRD
    // prunes `pendingRun`, and the crash safety it exists for would silently
    // not exist.
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

/// The final status write and the outcome.
async fn finish(
    client: &kube::Client,
    namespace: &str,
    name: &str,
    decision: SlotDecision,
    work: Reconcile,
    now: DateTime<Utc>,
) -> Result<ScheduleOutcome, ScheduleError> {
    let api: Api<BackupSchedule> = Api::namespaced(client.clone(), namespace);
    let patch = status_patch_with_refs(&work.status_write_base, &decision, &work, now);
    // No write when nothing changed: on a 30 s requeue a write per pass is
    // 2,880 API writes a day per unchanged schedule.
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
        // The one place `evaluatedAt` moves: the computed block carries the
        // stored instant so a steady schedule compares equal; here the status
        // is moving anyway.
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
        // Warn, not error: a suspended schedule is the operator's decision and
        // an unparseable one a spec they can fix.
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
/// The one clock read in this file is here, once per reconcile, before
/// anything is decided.
async fn reconcile(
    schedule: Arc<BackupSchedule>,
    ctx: Arc<Context>,
) -> Result<Action, ScheduleError> {
    reconcile_pass(&schedule, &ctx.client, ctx.archive.as_ref(), Utc::now())
        .await
        .map(|(_, action)| action)
}

/// One pass of the `kube::runtime` reconciler at `now`: the outcome, and the
/// [`Action`] the pass hands back, which is [`schedule_action`] of the
/// decision on every path that decided anything.
///
/// Public, and the whole of [`reconcile`] apart from its clock read, so a test
/// can drive the controller as kube does, reconcile after requeue, with no
/// watch event in between (`tests/schedule_status_churn.rs`).
///
/// # Errors
///
/// [`ScheduleError`], which `error_policy` turns into [`error_action`].
pub async fn reconcile_pass(
    schedule: &BackupSchedule,
    client: &kube::Client,
    archive: Option<&Arc<Store>>,
    now: DateTime<Utc>,
) -> Result<(ScheduleOutcome, Action), ScheduleError> {
    let outcome = reconcile_schedule_with_archive(schedule, client, archive, now).await?;
    let action = schedule_action(&outcome.decision, now);
    Ok((outcome, action))
}

/// The [`Action`] a pass that decided `decision` returns: always a timed
/// requeue of [`SlotDecision::requeue_after`], at most [`REQUEUE_SECS`].
///
/// This requeue is the schedule's clock. A cron schedule's next event is a
/// clock tick, which no watch delivers, and the watch does not deliver this
/// controller's own status writes either ([`schedule_triggers`]), so
/// `await_change()` would leave every steady schedule asleep, and a poll longer
/// than the starting deadline would miss every slot.
/// `every_reconcile_exit_requeues_within_the_poll` and
/// `a_due_slot_fires_from_the_requeue_alone` pin it. The `min(30 s, next due)`
/// floor only ever shortens the wait, so nothing drifts.
#[must_use]
pub fn schedule_action(decision: &SlotDecision, now: DateTime<Utc>) -> Action {
    Action::requeue(decision.requeue_after(now))
}

/// The [`Action`] a pass that failed returns: a timed requeue of
/// [`REQUEUE_SECS`], because an error is not a reason to stop the clock.
#[must_use]
pub fn error_action() -> Action {
    Action::requeue(std::time::Duration::from_secs(REQUEUE_SECS))
}

/// Requeue on an error, naming it. Never a panic and never a drop.
fn error_policy(schedule: Arc<BackupSchedule>, err: &ScheduleError, _ctx: Arc<Context>) -> Action {
    warn!(
        schedule = %schedule.name_any(),
        error = %err,
        "backup schedule reconcile failed; requeueing"
    );
    error_action()
}

/// What, about a `BackupSchedule` watch event, may wake this reconciler: the
/// object's identity and spec revision (`metadata.uid` and
/// `metadata.generation`) and nothing else.
///
/// This reconciler is the only writer of the status, so a status change on
/// the watch is its own last write coming back; waking on it re-runs a pass
/// that just ran, and a pass that changes anything would spin (~120 writes a
/// second on a live install). `generation` moves on every spec change and
/// never on a `/status` write, so spec edits take effect at once; everything
/// else waits for the requeue.
///
/// The UID too, because kube's predicate cache is keyed by namespace and name:
/// a schedule re-created under the same name starts at `generation: 1`, and a
/// generation-only hash would match the deleted object's and never reconcile
/// the new one.
#[must_use]
pub fn reconcile_trigger(schedule: &BackupSchedule) -> Option<u64> {
    use std::hash::{Hash as _, Hasher as _};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    schedule.metadata.uid.hash(&mut hasher);
    schedule.metadata.generation.hash(&mut hasher);
    Some(hasher.finish())
}

/// The watch events that trigger a reconcile: `applied`, with every event
/// whose [`reconcile_trigger`] equals the previous one for the same object
/// dropped. [`controller_in`] feeds it the reflector's applied objects, so the
/// store still holds the newest object while the trigger ignores status-only
/// events. Public so the filter is a property a test can drive with a plain
/// stream, without a cluster.
pub fn schedule_triggers<S>(
    applied: S,
) -> impl futures::Stream<Item = Result<BackupSchedule, watcher::Error>> + Send
where
    S: futures::Stream<Item = Result<BackupSchedule, watcher::Error>> + Send,
{
    applied.predicate_filter(reconcile_trigger)
}

/// The one trigger stream [`controller_in`] hands to `Controller::for_stream`:
/// a watch over `api`, reflected into `writer`'s store before the filter (so
/// the store holds the newest object), then filtered by
/// [`schedule_triggers`].
///
/// One expression, so a test can pin the wiring:
/// `the_schedule_controller_is_built_on_the_filtered_trigger` asserts the call
/// shape `Controller::for_stream(schedule_trigger_stream(` and that this body
/// is exactly `schedule_triggers(watcher(api, …)…)`.
pub fn schedule_trigger_stream(
    api: Api<BackupSchedule>,
    writer: reflector::store::Writer<BackupSchedule>,
) -> impl futures::Stream<Item = Result<BackupSchedule, watcher::Error>> + Send {
    schedule_triggers(
        watcher(api, watcher::Config::default())
            .default_backoff()
            .reflect(writer)
            .applied_objects(),
    )
}

/// Run the `BackupSchedule` controller until the process ends.
///
/// `Api::all`: this controller reconciles schedules in every namespace, one
/// controller per cluster.
///
/// `archive` is the controller's one read-only archive handle (interface
/// **I13**), which the schedule controller does not read.
pub fn controller(
    client: kube::Client,
    archive: Option<Arc<Store>>,
) -> impl std::future::Future<Output = ()> + Send {
    // One watch per watched namespace: `crate::scope` is the whole cluster
    // unless `LOGWEIR_WATCH_NAMESPACES` names the execution namespaces, and
    // then this reconciler runs once per namespace with an `Api::namespaced`
    // watch, the only shape the scoped chart's RoleBindings permit.
    crate::scope::run_everywhere(move |namespace| {
        controller_in(client.clone(), archive.clone(), namespace)
    })
}

/// One watch of [`controller`], over `namespace` (`None` is the whole
/// cluster).
fn controller_in(
    client: kube::Client,
    archive: Option<Arc<Store>>,
    namespace: Option<String>,
) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<BackupSchedule> = crate::scope::api(&client, namespace.as_deref());
    let ctx = Arc::new(Context {
        client,
        archive,
        // This reconciler creates no runner Job, so the image is "unused",
        // never "no override is configured" (see `super::Context::runner_image`).
        runner_image: crate::job::RunnerImage::default(),
    });
    // `for_stream`, not `Controller::new`, which wakes the reconciler on
    // every event including its own `/status` PATCH, so a pass that always
    // changed something would run ~120 times a second. The reflector sees
    // every event; only the trigger is filtered (`schedule_triggers`). The
    // schedule's clock is the requeue every pass returns.
    let (reader, writer) = reflector::store();
    async move {
        Controller::for_stream(schedule_trigger_stream(api, writer), reader)
            .run(reconcile, error_policy, ctx)
            // Every item is already logged by `reconcile_schedule` or
            // `error_policy`; the stream exists to be driven.
            .for_each(|_| std::future::ready(()))
            .await;
    }
}
