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
use crate::conditions::{current_condition, merge_condition, status_unchanged};
use crate::crds::backup::{Backup, BackupSpec, ScheduleRef, Trigger, TriggerKind};
use crate::crds::backup_schedule::{
    BackupSchedule, BackupScheduleSpec, ConcurrencyPolicy, PendingRun, PolicyStatus, Retention,
};
use crate::crds::{Condition, LocalRef};
use crate::retention::RetentionReport;
use crate::slot::{scheduled_backup_name, slot_name, Cron, CronError, SlotError};
use logweir_core::destination::FieldError;

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
    /// A slot came due, is older than [`MISSED_SLOT_HORIZON`], and was never
    /// fired.
    Missed {
        /// The slot that came due.
        due: DateTime<Utc>,
        /// That slot as [`slot_name`] spells it.
        slot: String,
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
            Self::InvalidTopicSelection { .. } => REASON_INVALID_TOPIC_SELECTION,
            Self::InvalidRunPolicy { .. } => REASON_INVALID_RUN_POLICY,
            Self::NoDueSlot { .. } => REASON_NO_DUE_SLOT,
            Self::Missed { .. } => REASON_SLOT_MISSED,
            Self::ConcurrencyBlocked { .. } => REASON_CONCURRENCY_BLOCKED,
            Self::NameTooLong { .. } => REASON_NAME_TOO_LONG,
            Self::Due { .. } | Self::AlreadyFired { .. } => REASON_SCHEDULED,
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
            | Self::Missed { .. }
            | Self::ConcurrencyBlocked { .. } => true,
            Self::Suspended
            | Self::Unparseable(_)
            | Self::InvalidTopicSelection { .. }
            | Self::InvalidRunPolicy { .. }
            | Self::NoDueSlot { .. } => false,
            Self::NameTooLong { .. } => false,
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
            | Self::InvalidTopicSelection { .. }
            | Self::InvalidRunPolicy { .. } => None,
            Self::NoDueSlot { next_fire_time }
            | Self::AlreadyFired { next_fire_time, .. }
            | Self::Missed { next_fire_time, .. }
            | Self::ConcurrencyBlocked { next_fire_time, .. }
            | Self::NameTooLong { next_fire_time, .. }
            | Self::Due { next_fire_time, .. } => *next_fire_time,
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
            Self::Missed { slot, .. } => format!(
                "slot {slot} is older than the one-hour missed-slot horizon and was not fired; \
                 the next firing is {next}"
            ),
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
/// The steps are the brief's, in order: `suspend` first (so a suspended
/// schedule's expression is never even parsed into a firing), then the
/// expression, then the due slot, then the horizon, then the name.
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
    let cron = match Cron::parse(&spec.schedule) {
        Ok(cron) => cron,
        Err(e) => return SlotDecision::Unparseable(e),
    };
    let next_fire_time = cron.next_fire_after(now);
    let Some(due) = cron.last_fire_at_or_before(now) else {
        return SlotDecision::NoDueSlot { next_fire_time };
    };
    let slot = slot_name(due);
    if now - due > missed_slot_horizon() {
        return SlotDecision::Missed {
            due,
            slot,
            next_fire_time,
        };
    }
    match scheduled_backup_name(name, &slot) {
        Ok(name) => SlotDecision::Due {
            due,
            slot,
            name,
            next_fire_time,
        },
        Err(error) => SlotDecision::NameTooLong {
            slot,
            error,
            next_fire_time,
        },
    }
}

/// Refine a decision's **reporting** against `status.lastFireTime`.
///
/// A SLOT IS MISSED ONLY IF IT WAS NEVER FIRED. [`decide`] cannot know that —
/// it reads no status, which is exactly what makes the object name a pure
/// function of the trigger — so the one question that needs the status is asked
/// here, after the name has already been minted, and it is asked of one arm
/// only: a [`SlotDecision::Missed`] whose `due` instant is at or before
/// `last_fire_time` becomes [`SlotDecision::AlreadyFired`]. Every other
/// decision is returned untouched.
///
/// # This function cannot endanger G-SLOT, and here is why
///
/// It takes `Option<DateTime<Utc>>` and not a `BackupSchedule`, so there is no
/// status field it could reach beyond the one instant; it never constructs
/// [`SlotDecision::Due`], so it cannot change a name; and the only variant it
/// produces mints no name at all. The mutant "derive the name from
/// `status.lastFireTime`" is still killed at assertion time by
/// `a_crash_between_create_and_status_write_yields_exactly_one_backup`, and
/// `the_name_never_reads_a_reconcile_clock_or_a_status` reads this function's
/// own body to assert it names neither `scheduled_backup_name` nor
/// `SlotDecision::Due`.
///
/// # Why not a fourth argument to `decide`
///
/// Because then the pure function that mints the name would hold the status,
/// and the source-reading guard that says it must not would have to be
/// weakened to a guard about how the status is USED — a property no test in
/// this file can read. A second function is a boundary a test can see.
#[must_use]
pub fn refine_against_last_fire(
    decision: SlotDecision,
    last_fire_time: Option<DateTime<Utc>>,
) -> SlotDecision {
    match (decision, last_fire_time) {
        (
            SlotDecision::Missed {
                due,
                slot,
                next_fire_time,
            },
            Some(last),
        ) if due <= last => SlotDecision::AlreadyFired {
            due,
            slot,
            last_fire_time: last,
            next_fire_time,
        },
        (other, _) => other,
    }
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

fn reserved_slot(schedule_name: &str, backup_name: &str) -> Option<(String, DateTime<Utc>)> {
    let prefix = format!("{}{}-", crate::slot::SCHEDULED_BACKUP_PREFIX, schedule_name);
    let slot = backup_name.strip_prefix(&prefix)?;
    if slot.len() != 15
        || scheduled_backup_name(schedule_name, slot).ok().as_deref() != Some(backup_name)
    {
        return None;
    }
    let due = chrono::NaiveDateTime::parse_from_str(slot, "%Y%m%d-%H%M%S")
        .ok()?
        .and_utc();
    Some((slot.to_string(), due))
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

#[derive(Clone, Debug)]
enum ActiveRefUpdate {
    Keep,
    Set(String),
    Clear,
}

#[derive(Clone, Debug)]
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
    let active = created.map_or(ActiveRefUpdate::Keep, |name| {
        ActiveRefUpdate::Set(name.to_string())
    });
    status_patch_with_refs(
        schedule,
        decision,
        active,
        PendingRefUpdate::Keep,
        None,
        retention,
        now,
    )
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

fn status_patch_with_refs(
    schedule: &BackupSchedule,
    decision: &SlotDecision,
    active: ActiveRefUpdate,
    pending: PendingRefUpdate,
    fired_due: Option<DateTime<Utc>>,
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
    if let Some(report) = retention {
        // `evaluatedAt` IS KEPT WHEN THE FINDINGS ARE THE SAME — plan erratum
        // E11(d), review finding M-1. `evaluatedAt` is a "when computed" field,
        // so writing `now` into it on every pass made the whole status differ
        // on every pass, bumped `resourceVersion`, woke this reconciler's own
        // watch and spun it — the identical defect the two unconditional
        // `lastTransitionTime` writes had, reached through a different field.
        // The comparison is the report's own
        // (`crds::backup_schedule::RetentionReport::same_findings_as`), which
        // compares every field EXCEPT the instant.
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
    if let SlotDecision::Missed { slot, .. } | SlotDecision::ConcurrencyBlocked { slot, .. } =
        decision
    {
        status.insert("lastMissedSlot".to_string(), json!(slot));
    }
    if let Some(due) = fired_due.or(match decision {
        SlotDecision::Due { due, .. } => Some(*due),
        _ => None,
    }) {
        status.insert("lastFireTime".to_string(), json!(due));
    }
    match active {
        ActiveRefUpdate::Keep => {}
        ActiveRefUpdate::Set(name) => {
            status.insert("activeBackupRef".to_string(), json!({ "name": name }));
        }
        ActiveRefUpdate::Clear => {
            status.insert("activeBackupRef".to_string(), serde_json::Value::Null);
        }
    }
    match pending {
        PendingRefUpdate::Keep => {}
        // BOTH KEYS, ALWAYS. `pendingRun` is the typed reservation and
        // `pendingBackupRef` is the mirror older readers use; clearing one and
        // leaving the other would present a reservation that no longer exists
        // to whichever reader looked at the wrong field.
        PendingRefUpdate::Clear => {
            status.insert("pendingBackupRef".to_string(), serde_json::Value::Null);
            status.insert("pendingRun".to_string(), serde_json::Value::Null);
        }
    }
    let ready = if decision.ready() { "True" } else { "False" };
    let reason = decision.reason();
    status.insert(
        "conditions".to_string(),
        json!([merge_condition(
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
        )]),
    );
    json!({ "status": serde_json::Value::Object(status) })
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
            | Self::ForeignBackup(_) => None,
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

    // THE NAME IS DECIDED WITHOUT THE STATUS; THE REASON IS DECIDED WITH IT.
    // Two statements and not one nested call, so the pure decision is a value
    // this function holds before anything refines how it is reported — and so
    // the refinement cannot be mistaken for part of the name.
    let decision = decide(&name, &schedule.spec, now);
    let mut decision = refine_against_last_fire(
        decision,
        schedule.status.as_ref().and_then(|s| s.last_fire_time),
    );

    let mut created = None;
    let mut already_existed = false;
    let mut fired_due_override = None;
    let mut active_update = ActiveRefUpdate::Keep;
    let mut pending_update = PendingRefUpdate::Keep;
    // The final status CAS is based on the watched object unless this reconcile
    // first wins a reservation. In that case the API server's reservation
    // response supplies the newer resourceVersion that only this finalizer may
    // consume. A later reservation necessarily advances it again.
    let mut status_write_base = schedule.clone();
    let backups_api: Api<Backup> = Api::namespaced(client.clone(), &namespace);

    if schedule.spec.concurrency_policy == ConcurrencyPolicy::Allow {
        // `Allow` may have several active children, so the singular status ref
        // is only the most recently reported one. Between due slots, still
        // clear or replace it from actual owned Backup state so a completed or
        // deleted child is not displayed as running forever.
        if !matches!(decision, SlotDecision::Due { .. }) {
            if let Some(stored_active) = schedule
                .status
                .as_ref()
                .and_then(|status| status.active_backup_ref.as_ref())
            {
                let listed = backups_api.list(&ListParams::default()).await?;
                let mut active_names: Vec<String> = listed
                    .items
                    .iter()
                    .filter(|backup| {
                        is_owned_by_schedule(backup, &name, &uid) && !backup_is_terminal(backup)
                    })
                    .map(|backup| backup.name_any())
                    .collect();
                active_names.sort();
                active_update = active_names
                    .iter()
                    .find(|name| name.as_str() == stored_active.name)
                    .or_else(|| active_names.first())
                    .map_or(ActiveRefUpdate::Clear, |name| {
                        ActiveRefUpdate::Set(name.clone())
                    });
            }
        }
        if let SlotDecision::Due {
            slot,
            name: object_name,
            ..
        } = &decision
        {
            let backup = scheduled_backup(schedule, &uid, slot, object_name);
            let mut create_was_terminal = false;
            match backups_api.create(&PostParams::default(), &backup).await {
                Ok(_) => info!(
                    schedule = %name,
                    namespace = %namespace,
                    backup = %object_name,
                    slot = %slot,
                    "created a scheduled Backup"
                ),
                Err(kube::Error::Api(e)) if e.code == 409 => {
                    // A 409 proves only that *something* owns the deterministic
                    // name. Fetch the winner and require the complete current
                    // schedule controller identity before treating it as
                    // same-slot idempotence. A transient 404 or GET failure is
                    // returned conservatively and no success status is written.
                    let existing = backups_api.get(object_name).await?;
                    if existing.name_any() != *object_name
                        || !is_owned_by_schedule(&existing, &name, &uid)
                    {
                        return Err(ScheduleError::ForeignBackup(object_name.clone()));
                    }
                    create_was_terminal = backup_is_terminal(&existing);
                    already_existed = true;
                    debug!(
                        schedule = %name,
                        namespace = %namespace,
                        backup = %object_name,
                        slot = %slot,
                        "the Backup for this slot already exists; AlreadyExists IS the idempotence key"
                    );
                }
                Err(e) => return Err(e.into()),
            }
            created = Some(object_name.clone());
            active_update = if create_was_terminal {
                ActiveRefUpdate::Clear
            } else {
                ActiveRefUpdate::Set(object_name.clone())
            };
        }
    } else {
        // LIST, THEN FILTER BY CONTROLLER UID. Labels and names are not an
        // ownership boundary, and an omitted/unknown phase is active.
        let listed = backups_api.list(&ListParams::default()).await?;
        let mut owned: Vec<&Backup> = listed
            .items
            .iter()
            .filter(|backup| is_owned_by_schedule(backup, &name, &uid))
            .collect();
        owned.sort_by_key(|backup| backup.name_any());
        let mut active_names: Vec<String> = owned
            .iter()
            .filter(|backup| !backup_is_terminal(backup))
            .map(|backup| backup.name_any())
            .collect();
        active_names.sort();

        let stored_active = schedule
            .status
            .as_ref()
            .and_then(|status| status.active_backup_ref.as_ref())
            .map(|reference| reference.name.as_str());
        let preferred_active = stored_active
            .filter(|stored| active_names.iter().any(|name| name == *stored))
            .or_else(|| active_names.first().map(String::as_str));
        active_update = preferred_active.map_or_else(
            || {
                if stored_active.is_some() {
                    ActiveRefUpdate::Clear
                } else {
                    ActiveRefUpdate::Keep
                }
            },
            |name| ActiveRefUpdate::Set(name.to_string()),
        );

        let pending_name = schedule
            .status
            .as_ref()
            .and_then(|status| status.pending_backup_ref.as_ref())
            .map(|reference| reference.name.clone());
        if let Some(pending) = pending_name.as_deref() {
            if let Some(existing) = listed
                .items
                .iter()
                .find(|backup| backup.name_any() == pending)
            {
                if !is_owned_by_schedule(existing, &name, &uid) {
                    return Err(ScheduleError::ForeignBackup(pending.to_string()));
                }
                pending_update = PendingRefUpdate::Clear;
            } else if reserved_slot(&name, pending).is_none() {
                // Only a deterministic scheduled name can be an admission
                // reservation. Anything else is stale status, not work to
                // preserve.
                pending_update = PendingRefUpdate::Clear;
            }
        }

        // D1 §4.5 STEP 2 AND §5.5, THE RELEASE ARM. A reservation is accepted
        // in-flight work even when the controller was down long enough that
        // the cron decision is now Missed, or the operator suspended future
        // slots — but NOT when the run policy the child would copy is invalid.
        // Creating it then would POST a `Backup` the Backup controller refuses
        // terminally (`InvalidTopicSelection`), so the reservation is released
        // instead and one reconcile after the spec is fixed the schedule
        // resumes. Backups that already exist are untouched either way: this
        // arm reads and writes status, and never a child's spec.
        if matches!(
            decision,
            SlotDecision::InvalidTopicSelection { .. } | SlotDecision::InvalidRunPolicy { .. }
        ) {
            if pending_name.is_some() {
                pending_update = PendingRefUpdate::Clear;
            }
        } else if !matches!(decision, SlotDecision::Due { .. }) && active_names.is_empty() {
            if let Some(pending) = pending_name.as_deref() {
                if let Some(existing) = listed
                    .items
                    .iter()
                    .find(|backup| backup.name_any() == pending)
                {
                    if !is_owned_by_schedule(existing, &name, &uid) {
                        return Err(ScheduleError::ForeignBackup(pending.to_string()));
                    }
                    if backup_is_terminal(existing) {
                        already_existed = true;
                        created = Some(pending.to_string());
                        fired_due_override = reserved_slot(&name, pending).map(|(_, due)| due);
                    }
                } else if let Some((pending_slot, pending_due)) = reserved_slot(&name, pending) {
                    let backup = scheduled_backup(schedule, &uid, &pending_slot, pending);
                    let mut pending_was_terminal = false;
                    match backups_api.create(&PostParams::default(), &backup).await {
                        Ok(_) => {}
                        Err(kube::Error::Api(e)) if e.code == 409 => {
                            let existing = backups_api.get(pending).await?;
                            if !is_owned_by_schedule(&existing, &name, &uid) {
                                return Err(ScheduleError::ForeignBackup(pending.to_string()));
                            }
                            pending_was_terminal = backup_is_terminal(&existing);
                            already_existed = true;
                        }
                        Err(e) => return Err(e.into()),
                    }
                    created = Some(pending.to_string());
                    active_update = if pending_was_terminal {
                        ActiveRefUpdate::Clear
                    } else {
                        ActiveRefUpdate::Set(pending.to_string())
                    };
                    pending_update = PendingRefUpdate::Clear;
                    fired_due_override = Some(pending_due);
                }
            }
        }

        if let SlotDecision::Due {
            due,
            slot,
            name: due_name,
            next_fire_time,
        } = &decision
        {
            let due_value = *due;
            let slot_value = slot.clone();
            let due_name_value = due_name.clone();
            let next_value = *next_fire_time;

            if let Some(existing) = listed
                .items
                .iter()
                .find(|backup| backup.name_any() == due_name_value)
            {
                if !is_owned_by_schedule(existing, &name, &uid) {
                    return Err(ScheduleError::ForeignBackup(due_name_value));
                }
                created = Some(due_name_value.clone());
                already_existed = true;
            } else if !active_names.is_empty() {
                decision = SlotDecision::ConcurrencyBlocked {
                    slot: slot_value,
                    active_backups: active_names,
                    next_fire_time: next_value,
                };
            } else {
                let mut create_name = due_name_value.clone();
                let mut create_slot = slot_value.clone();
                let mut create_due = due_value;
                let mut needs_reservation = true;

                if let Some(pending) = pending_name.as_deref() {
                    if let Some(existing) = listed
                        .items
                        .iter()
                        .find(|backup| backup.name_any() == pending)
                    {
                        if !is_owned_by_schedule(existing, &name, &uid) {
                            return Err(ScheduleError::ForeignBackup(pending.to_string()));
                        }
                    } else if let Some((reserved_slot, reserved_due)) =
                        reserved_slot(&name, pending)
                    {
                        create_name = pending.to_string();
                        create_slot = reserved_slot;
                        create_due = reserved_due;
                        needs_reservation = false;
                    }
                }

                if needs_reservation {
                    // THE ADMISSION CAS, AND THE ONE VERB IT NEEDS. The
                    // precondition helper refuses an object with no
                    // `resourceVersion` before anything is sent, so the
                    // reservation is either compare-and-set or not made.
                    let schedules_api: Api<BackupSchedule> =
                        Api::namespaced(client.clone(), &namespace);
                    let body = status_patch_with_preconditions(
                        schedule,
                        reservation_patch(
                            schedule,
                            &decision,
                            &RunPlan::scheduled(&create_slot, &create_name),
                            now,
                        ),
                    )?;
                    status_write_base = schedules_api
                        .patch_status(&name, &PatchParams::default(), &Patch::Merge(body))
                        .await?;
                }

                let backup = scheduled_backup(schedule, &uid, &create_slot, &create_name);
                let mut create_was_terminal = false;
                match backups_api.create(&PostParams::default(), &backup).await {
                    Ok(_) => info!(
                        schedule = %name,
                        namespace = %namespace,
                        backup = %create_name,
                        slot = %create_slot,
                        "created a scheduled Backup admitted by concurrencyPolicy Forbid"
                    ),
                    Err(kube::Error::Api(e)) if e.code == 409 => {
                        let existing = backups_api.get(&create_name).await?;
                        if !is_owned_by_schedule(&existing, &name, &uid) {
                            return Err(ScheduleError::ForeignBackup(create_name));
                        }
                        create_was_terminal = backup_is_terminal(&existing);
                        already_existed = true;
                    }
                    Err(e) => return Err(e.into()),
                }
                created = Some(create_name.clone());
                active_update = if create_was_terminal {
                    ActiveRefUpdate::Clear
                } else {
                    ActiveRefUpdate::Set(create_name.clone())
                };
                pending_update = PendingRefUpdate::Clear;
                if create_name != due_name_value {
                    fired_due_override = Some(create_due);
                    decision = SlotDecision::ConcurrencyBlocked {
                        slot: slot_value,
                        active_backups: vec![create_name],
                        next_fire_time: next_value,
                    };
                } else {
                    decision = SlotDecision::Due {
                        due: create_due,
                        slot: create_slot,
                        name: due_name_value,
                        next_fire_time: next_value,
                    };
                }
            }
        }
    }

    // THE RETENTION REPORT. Between the create and the status write, so the
    // report travels in the same patch as the slot decision — one write, one
    // resourceVersion bump.
    //
    // INSIDE `spawn_blocking`, AND THAT IS INTERFACE I13. Every `Store`
    // method drives its own current-thread runtime, and this function is
    // driven ON a runtime by `kube`'s `Controller`; a direct call panics with
    // *Cannot start a runtime from within a runtime*.
    let retention_report = match archive {
        None => None,
        Some(store) => {
            let store = Arc::clone(store);
            let archive_url = schedule.spec.archive.url.clone();
            // The handle's own key space is what it was built over, so the
            // prefix to list under is the archive URL's own prefix. The URL
            // itself is what the rendered commands name.
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
                // A REPORT IS NOT A BACKUP. An unreadable archive must not
                // stop a schedule from firing, so this is a `warn` and an
                // omitted block, never an error the reconcile returns.
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

    // AFTER THE CREATE, ALWAYS. See the function note: the crash window is
    // harmless only in this order.
    let api: Api<BackupSchedule> = Api::namespaced(client.clone(), &namespace);
    let patch = status_patch_with_refs(
        &status_write_base,
        &decision,
        active_update,
        pending_update,
        fired_due_override,
        retention_report.as_ref(),
        now,
    );
    // NO WRITE WHEN NOTHING CHANGED — plan erratum E11(d), review finding M-1.
    // The two rules above (the merged condition, the kept `evaluatedAt`) make
    // a steady schedule's computed status equal to the stored one; this is
    // what turns that equality into NO API CALL, which is the property the
    // route-table test can see. The decision is still returned and still
    // logged.
    if status_unchanged(
        status_write_base
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
        // written; once this branch is taken the status is moving anyway, so
        // the instant catches up in the same patch and costs no extra write.
        let mut patch = patch;
        if let Some(evaluated) = patch
            .get_mut("status")
            .and_then(|s| s.get_mut("policy"))
            .and_then(|p| p.get_mut("evaluatedAt"))
        {
            *evaluated = json!(now);
        }
        // Every finalization and stale-reference clear is a real CAS. In
        // particular, `pendingBackupRef: null` can be applied only to the
        // resourceVersion that still held the reservation this reconcile
        // observed or created; a newer reservation and its scheduling
        // condition survive an older writer intact.
        let patch = status_patch_with_preconditions(&status_write_base, patch)?;
        api.patch_status(&name, &PatchParams::default(), &Patch::Merge(patch))
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
        created,
        already_existed,
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
    reconcile_schedule_with_archive(&schedule, &ctx.client, ctx.archive.as_ref(), Utc::now())
        .await?;
    // NOT `Action::await_change()`. A cron schedule's next event is a clock
    // tick, and no Kubernetes watch delivers one; without a requeue a schedule
    // created at 09:00 would never fire again until somebody edited it.
    Ok(Action::requeue(std::time::Duration::from_secs(
        REQUEUE_SECS,
    )))
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
