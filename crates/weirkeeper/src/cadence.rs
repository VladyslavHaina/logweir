//! Cadence: the time zone a cron expression is read in, the policy inputs that
//! decide whether a due slot may still run, and the preview the console shows
//! (decision D1 §4).
//!
//! Everything here is a pure function of its arguments, as in [`crate::slot`]:
//! no clock is read, so a duplicate reconcile after a crash computes the same
//! slot, name and preview (guard **G-SLOT**).
//!
//! # Three properties
//!
//! **1. An absent time zone is the UTC evaluator itself.** [`Zone::Utc`]
//! delegates straight to [`Cron::last_fire_at_or_before`] and
//! [`Cron::next_fire_after`], so there is no second evaluator to drift.
//! `absent_timezone_is_byte_identical_to_legacy_utc` measures it over 10 000
//! random `(expression, instant)` pairs.
//!
//! **2. A local wall time that is not a single instant still fires** (D1 §4.3):
//!
//! > every real instant whose local wall time matches fires once; a matching
//! > local time that does not exist fires once at the end of the gap
//! > (deduplicated).
//!
//! So a fixed local time in a repeated (fall-back) hour fires at both
//! occurrences, and one in a spring-forward gap fires once at the transition.
//! An interval schedule such as `*/15 * * * *` keeps its UTC cadence through
//! both without a burst or a hole. Skipping the gap would silently lose a
//! nightly backup once a year, and firing once in the repeated hour would make
//! the run's local time disagree with the preview. The cost is at most one
//! extra run per fall-back for a fixed-time schedule, which `status.nextRuns`
//! shows in advance ([`Adjustment::RepeatedLocalTimeFirst`] /
//! [`Adjustment::RepeatedLocalTimeSecond`]); `concurrencyPolicy: Forbid` still
//! prevents overlap.
//!
//! **3. Slot identity stays the UTC instant** (`yyyymmdd-hhmmss`, as
//! [`crate::slot::slot_name`] produces it). A slot named in local time would
//! collide across a fall-back and would not be monotonic. The zone is recorded
//! beside the slot, never inside it.
//!
//! # The time-zone database
//!
//! [`TZDB_SOURCE`]: `chrono-tz`, compiled in and pinned by `Cargo.lock`. The
//! host's `/usr/share/zoneinfo` was rejected (D1 §4.3): the controller, the API
//! and the test runner could otherwise compute different UTC instants for the
//! same `spec.timeZone`. A tz rule amendment therefore needs a release, and
//! `status.policy.tzdb` shows which rules were used.
//!
//! # One evaluator
//!
//! The controller writes `status.nextRuns` from [`Cadence::next_runs`] and the
//! API answers `GET /api/v1/cadence-previews` from the same function; the
//! browser evaluates no cron at all (D1 §4.4).

use std::collections::BTreeMap;
use std::fmt;

use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};

use crate::slot::{slot_name, Cron, CronError, WALK_DAYS};

/// The time-zone database this binary evaluates `spec.timeZone` against,
/// written into `status.policy.tzdb` (D1 §4.8) and pinned to `Cargo.lock` by
/// `tests/cadence.rs::the_tzdb_source_is_the_version_cargo_lock_resolves`: a tz
/// amendment changes when backups run, so a wrong version string is worse than
/// none.
pub const TZDB_SOURCE: &str = "chrono-tz 0.10.4";

/// `spec.startingDeadlineSeconds` when the field is absent (D1 §4.1): the
/// one-hour missed-slot horizon schedules always had, so absence keeps meaning
/// exactly that.
pub const DEFAULT_STARTING_DEADLINE_SECONDS: i64 = 3600;

/// The narrowest `spec.startingDeadlineSeconds` the CRD accepts (D1 §4.1).
pub const MIN_STARTING_DEADLINE_SECONDS: i64 = 60;

/// The widest `spec.startingDeadlineSeconds` the CRD accepts — one week.
pub const MAX_STARTING_DEADLINE_SECONDS: i64 = 604_800;

/// `spec.activeDeadlineSeconds` when the field is absent (D1 §4.1): the
/// constant the scheduler has always copied into a scheduled run.
pub const DEFAULT_ACTIVE_DEADLINE_SECONDS: i64 = 3600;

/// The narrowest `spec.activeDeadlineSeconds` the CRD accepts (D1 §4.1).
pub const MIN_ACTIVE_DEADLINE_SECONDS: i64 = 60;

/// The widest `spec.activeDeadlineSeconds` the CRD accepts — one day.
pub const MAX_ACTIVE_DEADLINE_SECONDS: i64 = 86_400;

/// `spec.retry.delaySeconds` when `retry` is present without it (D1 §4.1).
pub const DEFAULT_RETRY_DELAY_SECONDS: i64 = 300;

/// The narrowest `spec.retry.delaySeconds` the CRD accepts.
pub const MIN_RETRY_DELAY_SECONDS: i64 = 60;

/// The widest `spec.retry.delaySeconds` the CRD accepts — six hours.
pub const MAX_RETRY_DELAY_SECONDS: i64 = 21_600;

/// How many entries the controller writes to `status.nextRuns` (D1 §4.4).
pub const STATUS_NEXT_RUNS: usize = 5;

/// The API's default `count` for `GET /api/v1/cadence-previews` (D1 §4.4).
pub const DEFAULT_PREVIEW_COUNT: usize = 10;

/// The API's maximum `count` for `GET /api/v1/cadence-previews` (D1 §4.4).
pub const MAX_PREVIEW_COUNT: usize = 20;

/// The cap on the skipped-slot enumeration of D1 §4.5 step 5. A controller down
/// for a year under `*/1 * * * *` has half a million skipped slots; counting
/// them costs a reconcile, so [`SkippedSlots::capped`] lets
/// `status.missedSlots.countCapped` say "at least".
pub const MAX_SKIPPED_SLOT_ENUMERATION: usize = 1000;

/// The widest UTC offset this walk allows any zone to have, in hours.
///
/// Wider than any zone on purpose (live offsets run from −12:00 to +14:00, and
/// historical LMT entries a little past both): it is the slack
/// [`Cadence::fire_after`] and [`Cadence::last_fire_at_or_before`] use to know
/// when they may stop walking local dates, and being tight would cost a missed
/// slot. Narrowed to 12, `0 0 * * *` in `Pacific/Apia` reports a false
/// [`Adjustment::NonexistentLocalTimeShifted`] against 2011-12-30T10:00Z;
/// `tests/cadence.rs::apia_skipped_day_maps_all_matches_to_one_instant` holds
/// it.
const MAX_OFFSET_HOURS: i64 = 26;

/// How far past a nonexistent local time the end of its gap is searched, in
/// minutes (D1 §4.3: "search forward ≤ 48 h"). A gap is not always an hour:
/// `Pacific/Apia` skipped the whole of 2011-12-30 crossing the date line.
const GAP_SEARCH_MINUTES: i64 = 48 * 60;

/// How many local dates either side of the walk are probed before the answer
/// can be trusted to be the extreme one.
///
/// [`MAX_OFFSET_HOURS`] plus [`GAP_SEARCH_MINUTES`] rounded up to whole days: a
/// local date's instants land up to 26 h either side of it and a gap shift can
/// push one 48 h further, 74 h in all. Starting the walk that far out and
/// stopping that far past the first hit makes "the first hit is the earliest"
/// true.
///
/// Zero would lose a firing: `America/Sitka` lived 1867-10-18 twice (a 24-hour
/// repeated day, the Alaska purchase, +14:59 to −09:01). For `0 0 * * *`,
/// [`Cadence::fire_at_or_before`]`(1867-10-19T01:00Z)` is 1867-10-18T09:01:13Z,
/// which comes from local date 1867-10-19, one date after the argument's own;
/// a walk starting at the argument's date answers a whole slot too early.
/// `tests/cadence.rs::a_twenty_four_hour_repeated_day_needs_the_edge_slack`
/// holds the case.
const EDGE_DAYS: i64 = 4;

/// The bound on how many local dates either direction of a zoned walk visits:
/// [`WALK_DAYS`] (ten years, because `0 0 29 2 *` can go eight without firing)
/// plus [`EDGE_DAYS`] at each end, so a zoned cadence answers for exactly the
/// expressions a UTC one answers for.
const MAX_DATE_STEPS: u64 = WALK_DAYS as u64 + 2 * EDGE_DAYS.unsigned_abs();

/// The zone a cadence's cron fields are read in.
///
/// Two variants rather than `Option<Tz>`, because they are different claims:
/// [`Zone::Utc`] is the absent field and takes the UTC evaluator's own path;
/// [`Zone::Named`] is an explicit choice and takes the local-date walk. They
/// agree for `Tz::UTC` (`explicit_utc_agrees_with_the_absent_field`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Zone {
    /// `spec.timeZone` absent: UTC, evaluated by [`crate::slot`] itself.
    Utc,
    /// A named IANA zone resolved against the compiled-in database.
    Named(Tz),
}

impl Zone {
    /// Resolve `spec.timeZone`, `None` for the absent field.
    ///
    /// # Errors
    ///
    /// [`CadenceError::UnknownTimeZone`], naming the string, when the database
    /// has no such zone: `Ready=False` with no admissions and running work
    /// untouched (D1 §4.3), so a typo neither fires a backup at the wrong hour
    /// nor stops one that is running. An empty string is refused rather than
    /// read as UTC: it is a field the user set.
    pub fn resolve(name: Option<&str>) -> Result<Zone, CadenceError> {
        match name {
            None => Ok(Zone::Utc),
            Some(raw) => {
                raw.parse::<Tz>()
                    .map(Zone::Named)
                    .map_err(|_| CadenceError::UnknownTimeZone {
                        got: raw.to_string(),
                    })
            }
        }
    }

    /// The zone's name — `"UTC"` for the absent field, which is what D1 §4.8
    /// writes into `status.policy.timeZone` ("effective, `UTC` when absent").
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Zone::Utc => "UTC",
            Zone::Named(tz) => tz.name(),
        }
    }

    /// `t` rendered as a local wall time with its offset, e.g.
    /// `2026-10-25T02:30:00+02:00`. The offset is what tells the two
    /// occurrences of a repeated hour apart.
    #[must_use]
    pub fn render_local(&self, t: DateTime<Utc>) -> String {
        match self {
            Zone::Utc => t.format("%Y-%m-%dT%H:%M:%S+00:00").to_string(),
            Zone::Named(tz) => t
                .with_timezone(tz)
                .format("%Y-%m-%dT%H:%M:%S%:z")
                .to_string(),
        }
    }
}

/// Why a cadence could not be built.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CadenceError {
    /// The cron expression did not parse; the inner error names the field.
    Schedule(CronError),
    /// The compiled-in database has no zone by this name.
    UnknownTimeZone {
        /// The string that was written, verbatim.
        got: String,
    },
}

impl fmt::Display for CadenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Schedule(e) => write!(f, "{e}"),
            Self::UnknownTimeZone { got } => write!(
                f,
                "\"{got}\" is not a time zone in {TZDB_SOURCE}; spec.timeZone takes an IANA name \
                 such as Europe/Berlin, and an absent field means UTC"
            ),
        }
    }
}

impl std::error::Error for CadenceError {}

impl From<CronError> for CadenceError {
    fn from(e: CronError) -> Self {
        Self::Schedule(e)
    }
}

/// What the local clock did to a slot, when it did anything (D1 §4.4). Absent
/// for every slot of every UTC schedule and nearly every zoned one; a marker
/// flags a row whose local wall time and UTC instant tell different stories.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Adjustment {
    /// The matched local time does not exist (spring forward); this instant is
    /// the end of the gap.
    NonexistentLocalTimeShifted,
    /// The matched local time happens twice (fall back); this is the FIRST
    /// occurrence, still on the pre-transition offset.
    RepeatedLocalTimeFirst,
    /// The matched local time happens twice; this is the SECOND occurrence.
    RepeatedLocalTimeSecond,
}

impl Adjustment {
    /// How truthful this marker is about its instant; lower wins when several
    /// matched local times map to one instant. In `Europe/Berlin` on 2027-03-28
    /// a `*/15` schedule matches 02:00 to 02:45 (all nonexistent, shifted to
    /// 01:00 UTC) and also 03:00 local, which is 01:00 UTC and exists, so `None`
    /// outranks the shift.
    fn rank(this: Option<Adjustment>) -> u8 {
        match this {
            None => 0,
            Some(Adjustment::RepeatedLocalTimeFirst | Adjustment::RepeatedLocalTimeSecond) => 1,
            Some(Adjustment::NonexistentLocalTimeShifted) => 2,
        }
    }
}

/// Whether a firing at the same instant replaces the one already held.
///
/// The one tie-break for [`Cadence::fire_at_or_before`], [`Cadence::fire_after`]
/// and [`record`], which reach the same instant backwards, forwards and through
/// a map; with separate rules a `BackupSchedule.status` could contradict itself
/// about one slot (`Pacific/Apia`, `0 0 * * *`, 2011-12-30T10:00Z).
///
/// Strictly less: equal ranks are equally truthful, and keeping the one held
/// makes the answer independent of the walk's visiting order.
fn marker_supersedes(candidate: Option<Adjustment>, held: Option<Adjustment>) -> bool {
    Adjustment::rank(candidate) < Adjustment::rank(held)
}

/// One firing: the UTC instant, and what the local clock did to reach it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fire {
    /// The UTC instant. This, and only this, is the slot's identity.
    pub at: DateTime<Utc>,
    /// The marker, when there is one.
    pub adjustment: Option<Adjustment>,
}

impl Fire {
    /// The slot string this firing is named by — `yyyymmdd-hhmmss` of [`Fire::at`].
    #[must_use]
    pub fn slot(&self) -> String {
        slot_name(self.at)
    }
}

/// One entry of `status.nextRuns` and of the API's cadence preview (D1 §4.4).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NextRun {
    /// The UTC instant, which is the slot.
    pub at: DateTime<Utc>,
    /// The same instant rendered as a local wall time with its offset.
    pub local_time: String,
    /// The DST marker, omitted when the instant needed no adjustment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adjustment: Option<Adjustment>,
}

/// `spec.catchUpPolicy` (D1 §4.1).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum CatchUpPolicy {
    /// A slot past its starting deadline is counted and skipped — today's
    /// behaviour, and what an absent field means.
    #[default]
    None,
    /// The latest due slot, and only that one, may still run after its
    /// deadline: at most one catch-up run. Bounded(N) was rejected (D1 §4.10):
    /// a backup captures what the broker retains when it runs, so N runs back
    /// to back produce near-identical archives at N times the load.
    Latest,
}

/// `spec.retry` (D1 §4.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetryPolicy {
    /// `0..=`[`crate::slot::MAX_RETRIES`]. Zero is a legal, meaningful value:
    /// the schedule records that it considered retries and chose none.
    pub max_retries: u32,
    /// Seconds between a failed attempt finishing and its retry becoming
    /// admissible.
    pub delay_seconds: i64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 0,
            delay_seconds: DEFAULT_RETRY_DELAY_SECONDS,
        }
    }
}

/// Why a retry policy is not usable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryPolicyProblem {
    /// `maxRetries` above [`crate::slot::MAX_RETRIES`].
    MaxRetriesTooHigh {
        /// The value that was written.
        got: u32,
        /// The cap.
        limit: u32,
    },
    /// `delaySeconds` outside [`MIN_RETRY_DELAY_SECONDS`]`..=`[`MAX_RETRY_DELAY_SECONDS`].
    DelayOutOfRange {
        /// The value that was written.
        got: i64,
        /// The lowest accepted value.
        min: i64,
        /// The highest accepted value.
        max: i64,
    },
}

impl fmt::Display for RetryPolicyProblem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MaxRetriesTooHigh { got, limit } => write!(
                f,
                "spec.retry.maxRetries is {got}; the highest accepted value is {limit}"
            ),
            Self::DelayOutOfRange { got, min, max } => write!(
                f,
                "spec.retry.delaySeconds is {got}; accepted values are {min}..={max}"
            ),
        }
    }
}

impl RetryPolicy {
    /// Whether this policy is inside the ranges D1 §4.1 states. The CRD's
    /// OpenAPI ranges are not the only gate: an object created against an older
    /// CRD reaches the scheduler without them, so D1 §4.5 step 0 validates again
    /// and fails closed.
    ///
    /// # Errors
    ///
    /// [`RetryPolicyProblem`], naming the field and the value.
    pub fn validate(&self) -> Result<(), RetryPolicyProblem> {
        if self.max_retries > crate::slot::MAX_RETRIES {
            return Err(RetryPolicyProblem::MaxRetriesTooHigh {
                got: self.max_retries,
                limit: crate::slot::MAX_RETRIES,
            });
        }
        if self.delay_seconds < MIN_RETRY_DELAY_SECONDS
            || self.delay_seconds > MAX_RETRY_DELAY_SECONDS
        {
            return Err(RetryPolicyProblem::DelayOutOfRange {
                got: self.delay_seconds,
                min: MIN_RETRY_DELAY_SECONDS,
                max: MAX_RETRY_DELAY_SECONDS,
            });
        }
        Ok(())
    }
}

/// Why a deadline field is not usable (D1 §4.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeadlineProblem {
    /// `startingDeadlineSeconds` outside
    /// [`MIN_STARTING_DEADLINE_SECONDS`]`..=`[`MAX_STARTING_DEADLINE_SECONDS`].
    StartingOutOfRange {
        /// The value that was written.
        got: i64,
        /// The lowest accepted value.
        min: i64,
        /// The highest accepted value.
        max: i64,
    },
    /// `activeDeadlineSeconds` outside
    /// [`MIN_ACTIVE_DEADLINE_SECONDS`]`..=`[`MAX_ACTIVE_DEADLINE_SECONDS`].
    ActiveOutOfRange {
        /// The value that was written.
        got: i64,
        /// The lowest accepted value.
        min: i64,
        /// The highest accepted value.
        max: i64,
    },
}

impl fmt::Display for DeadlineProblem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StartingOutOfRange { got, min, max } => write!(
                f,
                "spec.startingDeadlineSeconds is {got}; accepted values are {min}..={max}"
            ),
            Self::ActiveOutOfRange { got, min, max } => write!(
                f,
                "spec.activeDeadlineSeconds is {got}; accepted values are {min}..={max}"
            ),
        }
    }
}

impl std::error::Error for DeadlineProblem {}

/// Whether the two deadline fields are inside the ranges D1 §4.1 states: the
/// same second gate as [`RetryPolicy::validate`], so the CRD and the scheduler
/// cannot disagree about a range. `None` is always valid; it is the documented
/// default ([`DEFAULT_STARTING_DEADLINE_SECONDS`],
/// [`DEFAULT_ACTIVE_DEADLINE_SECONDS`]).
///
/// # Errors
///
/// [`DeadlineProblem`], naming the field, the value and the accepted range.
pub fn validate_deadlines(
    starting_deadline_seconds: Option<i64>,
    active_deadline_seconds: Option<i64>,
) -> Result<(), DeadlineProblem> {
    if let Some(got) = starting_deadline_seconds {
        if !(MIN_STARTING_DEADLINE_SECONDS..=MAX_STARTING_DEADLINE_SECONDS).contains(&got) {
            return Err(DeadlineProblem::StartingOutOfRange {
                got,
                min: MIN_STARTING_DEADLINE_SECONDS,
                max: MAX_STARTING_DEADLINE_SECONDS,
            });
        }
    }
    if let Some(got) = active_deadline_seconds {
        if !(MIN_ACTIVE_DEADLINE_SECONDS..=MAX_ACTIVE_DEADLINE_SECONDS).contains(&got) {
            return Err(DeadlineProblem::ActiveOutOfRange {
                got,
                min: MIN_ACTIVE_DEADLINE_SECONDS,
                max: MAX_ACTIVE_DEADLINE_SECONDS,
            });
        }
    }
    Ok(())
}

/// What the deadline and catch-up policy say about one due slot (D1 §4.5 step
/// 7, truth-table rows 17–21).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotAdmission {
    /// Inside `startingDeadlineSeconds`: a normal scheduled run.
    Scheduled,
    /// Past the deadline, and `catchUpPolicy: Latest` admits it — once.
    CatchUp,
    /// Past the deadline and nothing admits it.
    Missed {
        /// Which rule refused it.
        reason: MissedReason,
    },
}

/// Why a due slot was not admitted (D1 §4.5 step 7).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MissedReason {
    /// `now - slot` exceeded `startingDeadlineSeconds` and `catchUpPolicy` is
    /// `None` — today's behaviour.
    PastStartingDeadline,
    /// `catchUpPolicy: Latest`, but the slot came due before this revision was
    /// first observed. D1 §4.7 row 19: catch-up recovers downtime, it does not
    /// back-fill a policy that did not exist yet.
    BeforeRevision,
}

/// Whether the latest due slot may still be admitted, and as what.
///
/// The order of the arms is the truth table's. The deadline is tested first, so
/// a slot inside the horizon is `Scheduled` whatever `catchUpPolicy` and
/// `effective_since` say (D1 §4.7 keeps that for a slot that came due before an
/// edit). A slot that came due before the schedule was created is never
/// admitted: the callers bound the latest due slot by
/// `metadata.creationTimestamp` first (`backup_schedule::bound_by_creation`).
///
/// The boundary is inclusive: `now - slot == starting_deadline` is inside the
/// horizon (`the_starting_deadline_boundary_is_inclusive`), so the answer does
/// not depend on which side of a millisecond the reconcile woke.
/// `starting_deadline_seconds` is the resolved value.
///
/// # Precondition
///
/// `slot <= now`. Its only correct argument is [`Cadence::latest_due_slot`],
/// which never returns a future instant; a future `slot` would come back
/// [`SlotAdmission::Scheduled`]. It is stated rather than guarded because D1
/// §4.7's table has no row for a fifth outcome;
/// `admit_slot_is_documented_for_a_future_slot` pins the answer.
#[must_use]
pub fn admit_slot(
    slot: DateTime<Utc>,
    now: DateTime<Utc>,
    starting_deadline_seconds: i64,
    catch_up: CatchUpPolicy,
    effective_since: Option<DateTime<Utc>>,
) -> SlotAdmission {
    let age = now.signed_duration_since(slot);
    if age <= Duration::seconds(starting_deadline_seconds) {
        return SlotAdmission::Scheduled;
    }
    match catch_up {
        CatchUpPolicy::None => SlotAdmission::Missed {
            reason: MissedReason::PastStartingDeadline,
        },
        CatchUpPolicy::Latest => match effective_since {
            Some(since) if slot < since => SlotAdmission::Missed {
                reason: MissedReason::BeforeRevision,
            },
            _ => SlotAdmission::CatchUp,
        },
    }
}

/// One attempt's terminal record, reduced to the two observations D1 §4.6
/// classifies, so the API and a test can answer without a Kubernetes object and
/// this module stays off the CRD types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TerminalOutcome<'a> {
    /// `status.exitCode`, absent when no container ever reported one — a
    /// pod that was never scheduled, a Job deleted mid-flight, a controller
    /// refusal made before any `POST`.
    pub exit_code: Option<i32>,
    /// `status.exitReason` in the terminal-state vocabulary
    /// ([`crate::conditions::TERMINAL_STATES`] and D1 §3.4's additions), or
    /// `None` when the record carries no state either.
    pub terminal_state: Option<&'a str>,
}

/// The terminal states that are retried when there is no exit code (D1 §4.6).
///
/// An allowlist, which is the "unknown ⇒ not retryable" rule itself: a terminal
/// state added later, or written by a newer controller and read by an older
/// one, falls to `false` rather than into an automatic re-run of an
/// unclassified failure. What unites the four: the work never started, or was
/// interrupted by something outside the run's control. A guard refusal, a
/// not-a-pass drill and a signing failure are decisions, and D1 §4.6 never
/// retries a decision.
/// `retry_classification_is_a_closed_match_and_unknown_is_not_retryable` names
/// the exact list.
pub const RETRYABLE_TERMINAL_STATES: &[&str] = &[
    crate::conditions::TERMINAL_STATE_DISRUPTED_MID_DRILL,
    crate::conditions::TERMINAL_STATE_POD_UNSCHEDULABLE,
    crate::conditions::TERMINAL_STATE_NO_EXIT_CODE,
    crate::conditions::TERMINAL_STATE_DISCOVERY_FAILED,
];

/// Whether D1 §4.6 retries this terminal record.
///
/// | Record | Retried | Because |
/// |---|---|---|
/// | exit **1** (`Operational`) | yes | the run failed and wrote nothing; a broker or a network can be back in five minutes |
/// | exit outside `0..=4` (137, 143, OOM) | yes | the container was killed — by `activeDeadlineSeconds`, by the OOM killer, by a node going away — and none of those is a statement about the backup |
/// | no exit code, state in [`RETRYABLE_TERMINAL_STATES`] | yes | nothing ran, or what ran was interrupted |
/// | exit **0** | no | it succeeded; this arm exists so a caller that passes a success gets `false` rather than a panic |
/// | exit **2** (`DrillNotPass`) | no | **a signed document WAS written.** Re-running would spend a broker read to produce a second copy of a result the product already has |
/// | exit **3** (`GuardRefused`), **4** (`SigningOrLock`) | no | a decision, and a decision repeated is the same decision |
/// | no exit code, any other state | no | the controller refused before any `POST` (a missing referent, an unrenderable credential, a name that does not fit) — nothing about waiting changes it — or the state is one this build has never heard of |
///
/// The unrecognised arm matters most: a retry on a state nobody classified
/// would re-run a full backup every `delaySeconds` until `maxRetries` for a
/// refusal that will not change; `false` costs one missed retry and says so in
/// `status.lastSlot.reason`.
#[must_use]
pub fn is_retryable(outcome: TerminalOutcome<'_>) -> bool {
    match outcome.exit_code {
        // Exhaustive over the four contract codes that are DECISIONS (0, 2, 3
        // and 4, as `conditions.rs` partitions them) …
        Some(0 | 2 | 3 | 4) => false,
        // … so what is left is exit 1 and every code outside the contract, and
        // both of those are the run failing rather than deciding.
        Some(_) => true,
        None => outcome
            .terminal_state
            .is_some_and(|state| RETRYABLE_TERMINAL_STATES.contains(&state)),
    }
}

/// What the retry policy says about the attempt chain of the slot currently
/// being decided (D1 §4.5 step 6, truth-table rows 10–13 and 22).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryAdmission {
    /// A newer slot is due; this chain is no longer the one being decided, and
    /// its slot is counted in `missedSlots` as superseded (row 22).
    Superseded {
        /// The slot that took over.
        by: DateTime<Utc>,
    },
    /// The failure is not in D1 §4.6's retryable set. Nothing further happens
    /// for this slot.
    NotRetryable,
    /// `attempt >= max_retries` — including the `max_retries: 0` case, and
    /// including a `max_retries` an edit LOWERED below the attempts already
    /// made (row 10).
    Exhausted {
        /// The highest attempt that exists.
        attempt: u32,
        /// The policy's cap. Zero both when `spec.retry` is absent and when it
        /// is present with `maxRetries: 0`, which is why the field below
        /// exists.
        max_retries: u32,
        /// Whether `spec.retry` was set at all.
        /// Whether `spec.retry` was set at all: the decision is the same, but a
        /// schedule that used up configured retries reports `RetryExhausted`
        /// and one that never asked for retries reports `RunFailed` (D1 §4.7
        /// row 10), and `max_retries: 0` alone cannot tell them apart.
        retry_configured: bool,
    },
    /// Retryable and under the cap, but the delay has not elapsed (row 11).
    Pending {
        /// When the retry becomes admissible — the instant to requeue at.
        due_at: DateTime<Utc>,
    },
    /// Retryable, under the cap, delay elapsed: admit attempt `attempt`
    /// (row 13). Concurrency is the caller's to check (row 12).
    Due {
        /// The attempt number to create — one past the highest that exists.
        attempt: u32,
    },
}

/// Whether the failed attempt chain of `slot` may be retried at `now`.
///
/// A retry never outlives its slot: the moment a newer slot comes due, the
/// older chain is superseded. D1 §4.5 gets that from its step order (latest due
/// slot first, then its attempt chain); it is the first arm here so a test can
/// hold it (`a_retry_is_superseded_by_the_next_due_slot`). Dropping a long
/// delayed retry for a fresh run is the right trade: a backup captures what the
/// broker retains when it runs, so the newer slot's run is more useful.
///
/// `retryable` is the caller's classification of the terminal record
/// ([`is_retryable`]), passed in because this module does not know the `Backup`
/// status type.
#[must_use]
pub fn admit_retry(
    slot: DateTime<Utc>,
    latest_due_slot: Option<DateTime<Utc>>,
    attempt: u32,
    retryable: bool,
    finished_at: DateTime<Utc>,
    policy: Option<RetryPolicy>,
    now: DateTime<Utc>,
) -> RetryAdmission {
    if let Some(latest) = latest_due_slot {
        if latest > slot {
            return RetryAdmission::Superseded { by: latest };
        }
    }
    if !retryable {
        return RetryAdmission::NotRetryable;
    }
    let max_retries = policy.map_or(0, |p| p.max_retries);
    if attempt >= max_retries {
        return RetryAdmission::Exhausted {
            attempt,
            max_retries,
            retry_configured: policy.is_some(),
        };
    }
    let delay = policy.map_or(DEFAULT_RETRY_DELAY_SECONDS, |p| p.delay_seconds);
    let due_at = finished_at + Duration::seconds(delay);
    if now < due_at {
        return RetryAdmission::Pending { due_at };
    }
    RetryAdmission::Due {
        attempt: attempt + 1,
    }
}

/// The result of the skipped-slot enumeration of D1 §4.5 step 5.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SkippedSlots {
    /// How many slots were counted — at most the cap the caller passed.
    pub count: usize,
    /// Whether the walk stopped at the cap, so `count` is a floor and not the
    /// total.
    pub capped: bool,
    /// Whether the walk stopped at the ten-year [`horizon_date`] instead of
    /// reaching `before`, so `count` is a floor for a SECOND reason.
    ///
    /// A downtime longer than [`WALK_DAYS`] under a sparse expression
    /// (`0 0 1 1 *` and twelve years) runs out of window before it reaches the
    /// cap, and without this flag `capped: false` would present a floor as an
    /// exact total. Separate from [`SkippedSlots::capped`] because an operator
    /// acts on them differently: "there are thousands" versus "this product
    /// cannot name a slot that far out".
    pub horizon_reached: bool,
}

/// A cron expression and the zone its fields are read in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cadence {
    cron: Cron,
    zone: Zone,
}

impl Cadence {
    /// A cadence from an already-parsed expression and an already-resolved
    /// zone.
    #[must_use]
    pub fn new(cron: Cron, zone: Zone) -> Self {
        Self { cron, zone }
    }

    /// Parse `spec.schedule` and resolve `spec.timeZone` in one call.
    ///
    /// # Errors
    ///
    /// [`CadenceError::Schedule`] naming the cron field, or
    /// [`CadenceError::UnknownTimeZone`] naming the zone. The expression is
    /// parsed FIRST so a schedule with two mistakes reports the one the user is
    /// more likely to have made.
    pub fn parse(schedule: &str, time_zone: Option<&str>) -> Result<Self, CadenceError> {
        let cron = Cron::parse(schedule)?;
        let zone = Zone::resolve(time_zone)?;
        Ok(Self::new(cron, zone))
    }

    /// The parsed expression.
    #[must_use]
    pub fn cron(&self) -> Cron {
        self.cron
    }

    /// The resolved zone.
    #[must_use]
    pub fn zone(&self) -> Zone {
        self.zone
    }

    /// The most recent instant at or before `t` at which this cadence fires.
    /// For [`Zone::Utc`] this is [`Cron::last_fire_at_or_before`] itself.
    #[must_use]
    pub fn last_fire_at_or_before(&self, t: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.fire_at_or_before(t).map(|f| f.at)
    }

    /// The earliest instant strictly after `t` at which this cadence fires.
    ///
    /// For [`Zone::Utc`] this IS [`Cron::next_fire_after`].
    #[must_use]
    pub fn next_fire_after(&self, t: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.fire_after(t).map(|f| f.at)
    }

    /// The one slot a reconcile at `now` may act on (D1 §4.5 step 5). Only the
    /// latest due slot is ever eligible, which bounds the backlog: after a week
    /// of downtime `catchUpPolicy: Latest` can create at most one catch-up run.
    #[must_use]
    pub fn latest_due_slot(&self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.last_fire_at_or_before(now)
    }

    /// The most recent firing at or before `t`, with the DST marker
    /// [`Cadence::next_runs`] gives the same instant, whichever walk a caller
    /// uses ([`marker_supersedes`];
    /// `tests/cadence.rs::every_walk_agrees_on_the_marker_for_an_instant`).
    #[must_use]
    pub fn fire_at_or_before(&self, t: DateTime<Utc>) -> Option<Fire> {
        let Zone::Named(tz) = self.zone else {
            return self.cron.last_fire_at_or_before(t).map(|at| Fire {
                at,
                adjustment: None,
            });
        };
        let mut best: Option<Fire> = None;
        let mut date = t.with_timezone(&tz).date_naive() + Duration::days(EDGE_DAYS);
        for _ in 0..MAX_DATE_STEPS {
            if let Some(b) = best {
                if latest_possible_utc(date) < b.at {
                    break;
                }
            }
            for fire in self.fires_on_local_date(tz, date) {
                if fire.at <= t && best.is_none_or(|b| later_or_better(fire, b)) {
                    best = Some(fire);
                }
            }
            date = date.pred_opt()?;
        }
        best
    }

    /// The earliest firing strictly after `t`, with its DST marker (as in
    /// [`Cadence::fire_at_or_before`]). Strictly after, so
    /// [`Cadence::next_runs`] terminates and the sequence strictly increases
    /// even where matched local times collapsed onto one instant.
    #[must_use]
    pub fn fire_after(&self, t: DateTime<Utc>) -> Option<Fire> {
        let Zone::Named(tz) = self.zone else {
            return self.cron.next_fire_after(t).map(|at| Fire {
                at,
                adjustment: None,
            });
        };
        let mut best: Option<Fire> = None;
        let mut date = t.with_timezone(&tz).date_naive() - Duration::days(EDGE_DAYS);
        for _ in 0..MAX_DATE_STEPS {
            if let Some(b) = best {
                if earliest_possible_utc(date) > b.at {
                    break;
                }
            }
            for fire in self.fires_on_local_date(tz, date) {
                if fire.at > t && best.is_none_or(|b| earlier_or_better(fire, b)) {
                    best = Some(fire);
                }
            }
            date = date.succ_opt()?;
        }
        best
    }

    /// The next `count` runs after `after`, in ascending order (D1 §4.4).
    ///
    /// Fewer than `count` is a real answer: `0 0 29 2 *` can run out of firings
    /// inside [`WALK_DAYS`]. `count` is not clamped here: the API clamps to
    /// [`MAX_PREVIEW_COUNT`] and the controller asks for [`STATUS_NEXT_RUNS`].
    #[must_use]
    pub fn next_runs(&self, after: DateTime<Utc>, count: usize) -> Vec<NextRun> {
        self.fires_after(after, count)
            .into_iter()
            .map(|fire| NextRun {
                at: fire.at,
                local_time: self.zone.render_local(fire.at),
                adjustment: fire.adjustment,
            })
            .collect()
    }

    /// The next `count` firings strictly after `after`, ascending, in one walk of
    /// the local dates: calling [`Cadence::fire_after`] in a loop re-scans the
    /// same [`EDGE_DAYS`]-wide window each time, quadratic for a minutely
    /// expression.
    fn fires_after(&self, after: DateTime<Utc>, count: usize) -> Vec<Fire> {
        self.fires_after_bounded(after, count, None).fires
    }

    /// [`Cadence::fires_after`], additionally stopping once the walk has passed
    /// `until`. [`Cadence::skipped_slots`] asks for
    /// [`MAX_SKIPPED_SLOT_ENUMERATION`]` + 1` firings, and an ordinary reconcile
    /// has no skipped slots, so without the bound every healthy reconcile would
    /// walk a thousand slots ahead.
    fn fires_after_bounded(
        &self,
        after: DateTime<Utc>,
        count: usize,
        until: Option<DateTime<Utc>>,
    ) -> Walk {
        if count == 0 {
            return Walk::default();
        }
        let Zone::Named(tz) = self.zone else {
            // The UTC path stays the legacy call, iterated: each step is a
            // bitset walk over UTC dates with no zone lookup at all, so there
            // is nothing here for a single pass to save.
            let horizon = horizon_date(after.date_naive());
            let mut walk = Walk {
                fires: Vec::with_capacity(count.min(MAX_PREVIEW_COUNT)),
                horizon_reached: false,
            };
            let mut cursor = after;
            for _ in 0..count {
                let Some(at) = self.cron.next_fire_after(cursor) else {
                    break;
                };
                if at.date_naive() > horizon {
                    walk.horizon_reached = true;
                    break;
                }
                walk.fires.push(Fire {
                    at,
                    adjustment: None,
                });
                cursor = at;
                if until.is_some_and(|u| at >= u) {
                    break;
                }
            }
            return walk;
        };
        let horizon = horizon_date(after.with_timezone(&tz).date_naive());
        let mut found: BTreeMap<DateTime<Utc>, Option<Adjustment>> = BTreeMap::new();
        let mut date = after.with_timezone(&tz).date_naive() - Duration::days(EDGE_DAYS);
        let mut horizon_reached = false;
        for _ in 0..MAX_DATE_STEPS {
            // The walk answers for exactly the window `slot.rs` answers for,
            // and the UTC branch above applies the SAME bound — see
            // [`horizon_date`].
            if date > horizon {
                horizon_reached = true;
                break;
            }
            // Stop once `count` firings are held AND no later local date could
            // produce one earlier than the last of them …
            if let Some((&nth, _)) = found.iter().nth(count - 1) {
                if earliest_possible_utc(date) > nth {
                    break;
                }
            }
            // … or once every firing this date and its successors could produce
            // is already past the caller's window.
            if until.is_some_and(|u| earliest_possible_utc(date) >= u) {
                break;
            }
            for fire in self.fires_on_local_date(tz, date) {
                if fire.at > after {
                    record(&mut found, fire.at, fire.adjustment);
                }
            }
            let Some(next) = date.succ_opt() else { break };
            date = next;
        }
        Walk {
            fires: found
                .into_iter()
                .take(count)
                .map(|(at, adjustment)| Fire { at, adjustment })
                .collect(),
            horizon_reached,
        }
    }

    /// How many slots fall strictly between `after` and `before`, up to `cap`
    /// (D1 §4.5 step 5). Both ends are exclusive, because the interval is
    /// `(lastEvaluatedSlot, S)`, so a slot is counted exactly once across
    /// reconciles.
    #[must_use]
    pub fn skipped_slots(
        &self,
        after: DateTime<Utc>,
        before: DateTime<Utc>,
        cap: usize,
    ) -> SkippedSlots {
        // One more than the cap, so `capped` can tell "ran out of slots" from
        // "ran out of budget" (`status.missedSlots.countCapped`).
        let walk = self.fires_after_bounded(after, cap.saturating_add(1), Some(before));
        let mut count = 0usize;
        for fire in walk.fires {
            if fire.at >= before {
                // A slot at or past `before` was reached, so the interval was
                // walked end to end: neither bound truncated the answer.
                return SkippedSlots {
                    count,
                    capped: false,
                    horizon_reached: false,
                };
            }
            if count == cap {
                return SkippedSlots {
                    count,
                    capped: true,
                    horizon_reached: false,
                };
            }
            count += 1;
        }
        SkippedSlots {
            count,
            capped: false,
            horizon_reached: walk.horizon_reached,
        }
    }

    /// Every instant this cadence fires at for the matched local minutes of one
    /// local calendar date, ascending and deduplicated.
    ///
    /// The whole DST rule is the `match` on `from_local_datetime`'s three
    /// answers: `Single` fires once; `Ambiguous` (the local time happens twice)
    /// fires both instants, as distinct slots; `None` (the local time does not
    /// exist) moves to the end of the gap, where several matched minutes
    /// collapse onto one instant through the `BTreeMap`, so an interval schedule
    /// does not burst.
    fn fires_on_local_date(&self, tz: Tz, date: NaiveDate) -> Vec<Fire> {
        if !self.cron.matches_date(date) {
            return Vec::new();
        }
        let mut found: BTreeMap<DateTime<Utc>, Option<Adjustment>> = BTreeMap::new();
        for minutes in 0..24 * 60 {
            if !self.cron.matches_minute_of_day(minutes) {
                continue;
            }
            let Some(local) = date.and_hms_opt(minutes / 60, minutes % 60, 0) else {
                continue;
            };
            match tz.from_local_datetime(&local) {
                chrono::LocalResult::Single(dt) => {
                    record(&mut found, dt.with_timezone(&Utc), None);
                }
                chrono::LocalResult::Ambiguous(first, second) => {
                    record(
                        &mut found,
                        first.with_timezone(&Utc),
                        Some(Adjustment::RepeatedLocalTimeFirst),
                    );
                    record(
                        &mut found,
                        second.with_timezone(&Utc),
                        Some(Adjustment::RepeatedLocalTimeSecond),
                    );
                }
                chrono::LocalResult::None => {
                    if let Some(at) = gap_end(tz, local) {
                        record(
                            &mut found,
                            at,
                            Some(Adjustment::NonexistentLocalTimeShifted),
                        );
                    }
                }
            }
        }
        found
            .into_iter()
            .map(|(at, adjustment)| Fire { at, adjustment })
            .collect()
    }
}

/// One pass of [`Cadence::fires_after_bounded`]: the firings it found, and
/// whether [`horizon_date`] stopped it. A preview may ignore the difference;
/// [`Cadence::skipped_slots`] reports a count, and a count cut by the window is
/// a floor, not a total.
#[derive(Clone, Debug, Default)]
struct Walk {
    /// The firings, ascending, at most the `count` asked for.
    fires: Vec<Fire>,
    /// Whether the walk stopped because it reached [`horizon_date`].
    horizon_reached: bool,
}

/// Keep the most truthful marker when two matched local times land on one
/// instant — see [`Adjustment::rank`].
fn record(
    found: &mut BTreeMap<DateTime<Utc>, Option<Adjustment>>,
    at: DateTime<Utc>,
    adjustment: Option<Adjustment>,
) {
    if found
        .get(&at)
        .is_none_or(|held| marker_supersedes(adjustment, *held))
    {
        found.insert(at, adjustment);
    }
}

/// Whether `candidate` beats `held` for [`Cadence::fire_at_or_before`]: a later
/// instant, or the same instant with a better marker ([`marker_supersedes`]).
fn later_or_better(candidate: Fire, held: Fire) -> bool {
    candidate.at > held.at
        || (candidate.at == held.at && marker_supersedes(candidate.adjustment, held.adjustment))
}

/// Whether `candidate` beats `held` for [`Cadence::fire_after`]: an earlier
/// instant, or the same instant with a better marker ([`marker_supersedes`]).
fn earlier_or_better(candidate: Fire, held: Fire) -> bool {
    candidate.at < held.at
        || (candidate.at == held.at && marker_supersedes(candidate.adjustment, held.adjustment))
}

/// The UTC instant of the first real local minute at or after `local`, when
/// `local` itself does not exist: the transition instant, found minute by
/// minute so a 30-minute gap (`Australia/Lord_Howe`), a 60-minute one and a
/// 24-hour one (`Pacific/Apia`, 2011-12-30) need no cases. `Ambiguous` answers
/// with its earliest instant for the same reason. `None` only when the whole
/// [`GAP_SEARCH_MINUTES`] window is nonexistent, so a corrupt database cannot
/// hang a reconcile.
fn gap_end(tz: Tz, local: NaiveDateTime) -> Option<DateTime<Utc>> {
    for step in 1..=GAP_SEARCH_MINUTES {
        let candidate = local + Duration::minutes(step);
        match tz.from_local_datetime(&candidate) {
            chrono::LocalResult::Single(dt) => return Some(dt.with_timezone(&Utc)),
            chrono::LocalResult::Ambiguous(first, _) => return Some(first.with_timezone(&Utc)),
            chrono::LocalResult::None => {}
        }
    }
    None
}

/// The last local date a multi-entry walk visits: [`WALK_DAYS`] past `from`.
///
/// One horizon for both branches of [`Cadence::fires_after_bounded`], the bound
/// `slot.rs` already answers within. Iterating [`Cron::next_fire_after`]
/// restarts its walk from each firing, so without it the UTC branch would show
/// `0 0 29 2 *` firings out to 2168 while the zoned branch stopped at ten
/// years: a preview that depended on whether `spec.timeZone` was written
/// (`explicit_utc_agrees_with_the_absent_field` compares whole previews).
///
/// Ten years is therefore also the limit of [`Cadence::skipped_slots`], the
/// same blindness [`Cron`] has. [`NaiveDate::MAX`] rather than a panic at the
/// end of the calendar: the walk stops at [`MAX_DATE_STEPS`] regardless.
fn horizon_date(from: NaiveDate) -> NaiveDate {
    from.checked_add_days(chrono::Days::new(u64::from(WALK_DAYS)))
        .unwrap_or(NaiveDate::MAX)
}

/// The earliest UTC instant any firing of local date `date` could have.
fn earliest_possible_utc(date: NaiveDate) -> DateTime<Utc> {
    date.and_hms_opt(0, 0, 0)
        .map_or(DateTime::<Utc>::MIN_UTC, |d| d.and_utc())
        - Duration::hours(MAX_OFFSET_HOURS)
}

/// The latest UTC instant any firing of local date `date` could have —
/// including one a gap shifted [`GAP_SEARCH_MINUTES`] into the future.
fn latest_possible_utc(date: NaiveDate) -> DateTime<Utc> {
    date.and_hms_opt(23, 59, 0)
        .map_or(DateTime::<Utc>::MAX_UTC, |d| d.and_utc())
        + Duration::hours(MAX_OFFSET_HOURS)
        + Duration::minutes(GAP_SEARCH_MINUTES)
}

/// The preset catalogue (D1 §4.2): the five cadences the console offers as a
/// form, and the canonical cron string each one compiles to.
///
/// No preset is stored in any spec: `spec.schedule` stays the single source of
/// truth, since two representations of one cadence can disagree with nothing to
/// say which one the controller obeyed. [`match_preset`] is the other half of
/// [`compile`]: the form shows a preset when the saved expression is one, and
/// "Advanced cron" when it is not. `every_preset_round_trips_through_its_cron`
/// asserts `match_preset(compile(p)) == Some(p)` for every preset in range.
pub mod presets {
    use serde::{Deserialize, Serialize};

    use crate::slot::{expand_alias, Cron, CronError};

    /// The `n` values `everyNHours` accepts (D1 §4.2): divisors of 24. `*/n` in
    /// the hour field matches local hours divisible by `n`, so `*/5` would leave
    /// a four-hour gap once a day.
    pub const EVERY_N_HOURS_VALUES: [u32; 6] = [2, 3, 4, 6, 8, 12];

    /// One of the five preset cadences, with its parameters.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(tag = "kind", rename_all = "camelCase")]
    pub enum Preset {
        /// Every hour, at `minute`.
        #[serde(rename_all = "camelCase")]
        Hourly {
            /// 0–59.
            minute: u32,
        },
        /// Every `n` hours — at local wall-clock hours divisible by `n` — at
        /// `minute`.
        #[serde(rename_all = "camelCase")]
        EveryNHours {
            /// One of [`EVERY_N_HOURS_VALUES`].
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
        /// Every week on `day_of_week` at `hour`:`minute`.
        #[serde(rename_all = "camelCase")]
        Weekly {
            /// 0–6, 0 = Sunday.
            day_of_week: u32,
            /// 0–23.
            hour: u32,
            /// 0–59.
            minute: u32,
        },
        /// Every month on `day_of_month` at `hour`:`minute`.
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

    /// Why a preset could not be compiled.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum PresetError {
        /// A parameter outside the range the catalogue states.
        OutOfRange {
            /// The preset kind, as the catalogue spells it.
            kind: &'static str,
            /// The parameter name, as the catalogue spells it.
            parameter: &'static str,
            /// The value that was written.
            value: u32,
            /// What the catalogue accepts, rendered for a human.
            accepted: String,
        },
    }

    impl std::fmt::Display for PresetError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::OutOfRange {
                    kind,
                    parameter,
                    value,
                    accepted,
                } => write!(
                    f,
                    "preset {kind}: {parameter} is {value}; accepted values are {accepted}"
                ),
            }
        }
    }

    impl std::error::Error for PresetError {}

    /// One parameter of one preset, as the API and the console read it.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct PresetParameter {
        /// The parameter's name, matching the DTO field.
        pub name: &'static str,
        /// The lowest accepted value.
        pub min: u32,
        /// The highest accepted value.
        pub max: u32,
        /// The exact accepted set, when it is not the whole `min..=max` range.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub values: Option<&'static [u32]>,
    }

    /// One preset kind, as data: the form the API serves and the console
    /// renders, so the browser needs no evaluator (D1 §4.2). `cron_template` is
    /// a string template with `{parameter}` holes.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct PresetSpec {
        /// The DTO's `kind` discriminator.
        pub kind: &'static str,
        /// The five-field expression with `{parameter}` holes.
        pub cron_template: &'static str,
        /// The parameters, in the order a form should present them.
        pub parameters: &'static [PresetParameter],
    }

    /// The whole catalogue, in the order a form should offer it.
    pub const CATALOGUE: &[PresetSpec] = &[
        PresetSpec {
            kind: "hourly",
            cron_template: "{minute} * * * *",
            parameters: &[MINUTE],
        },
        PresetSpec {
            kind: "everyNHours",
            cron_template: "{minute} */{n} * * *",
            parameters: &[EVERY_N_HOURS_N, MINUTE],
        },
        PresetSpec {
            kind: "daily",
            cron_template: "{minute} {hour} * * *",
            parameters: &[HOUR, MINUTE],
        },
        PresetSpec {
            kind: "weekly",
            cron_template: "{minute} {hour} * * {dayOfWeek}",
            parameters: &[DAY_OF_WEEK, HOUR, MINUTE],
        },
        PresetSpec {
            kind: "monthly",
            cron_template: "{minute} {hour} {dayOfMonth} * *",
            parameters: &[DAY_OF_MONTH, HOUR, MINUTE],
        },
    ];

    /// The `n` parameter of `everyNHours`.
    const EVERY_N_HOURS_N: PresetParameter = PresetParameter {
        name: "n",
        min: 2,
        max: 12,
        values: Some(&EVERY_N_HOURS_VALUES),
    };

    /// The `dayOfWeek` parameter of `weekly`; 0 is Sunday.
    const DAY_OF_WEEK: PresetParameter = PresetParameter {
        name: "dayOfWeek",
        min: 0,
        max: 6,
        values: None,
    };

    /// The `dayOfMonth` parameter of `monthly`: 28, not 31, because
    /// `0 3 31 * *` silently skips five months a year. Advanced cron still
    /// accepts 29 to 31.
    const DAY_OF_MONTH: PresetParameter = PresetParameter {
        name: "dayOfMonth",
        min: 1,
        max: 28,
        values: None,
    };

    /// The `hour` parameter, shared by three presets.
    const HOUR: PresetParameter = PresetParameter {
        name: "hour",
        min: 0,
        max: 23,
        values: None,
    };

    /// The `minute` parameter, shared by all five.
    const MINUTE: PresetParameter = PresetParameter {
        name: "minute",
        min: 0,
        max: 59,
        values: None,
    };

    /// The canonical five-field expression this preset compiles to.
    ///
    /// # Errors
    ///
    /// [`PresetError::OutOfRange`], naming the preset, the parameter and what
    /// the catalogue accepts. A preset is a DTO from a browser, so its ranges
    /// are checked here, where the error names the form field rather than the
    /// cron field. Each check names its parameter constant, never
    /// `CATALOGUE[i]`, so reordering the catalogue cannot validate one preset
    /// against another's range.
    pub fn compile(preset: &Preset) -> Result<String, PresetError> {
        match *preset {
            Preset::Hourly { minute } => {
                check("hourly", MINUTE, minute)?;
                Ok(format!("{minute} * * * *"))
            }
            Preset::EveryNHours { n, minute } => {
                check("everyNHours", EVERY_N_HOURS_N, n)?;
                check("everyNHours", MINUTE, minute)?;
                Ok(format!("{minute} */{n} * * *"))
            }
            Preset::Daily { hour, minute } => {
                check("daily", HOUR, hour)?;
                check("daily", MINUTE, minute)?;
                Ok(format!("{minute} {hour} * * *"))
            }
            Preset::Weekly {
                day_of_week,
                hour,
                minute,
            } => {
                check("weekly", DAY_OF_WEEK, day_of_week)?;
                check("weekly", HOUR, hour)?;
                check("weekly", MINUTE, minute)?;
                Ok(format!("{minute} {hour} * * {day_of_week}"))
            }
            Preset::Monthly {
                day_of_month,
                hour,
                minute,
            } => {
                check("monthly", DAY_OF_MONTH, day_of_month)?;
                check("monthly", HOUR, hour)?;
                check("monthly", MINUTE, minute)?;
                Ok(format!("{minute} {hour} {day_of_month} * *"))
            }
        }
    }

    /// One parameter against its catalogue entry.
    fn check(kind: &'static str, p: PresetParameter, value: u32) -> Result<(), PresetError> {
        let ok = match p.values {
            Some(set) => set.contains(&value),
            None => value >= p.min && value <= p.max,
        };
        if ok {
            return Ok(());
        }
        let accepted = match p.values {
            Some(set) => set
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(", "),
            None => format!("{}..={}", p.min, p.max),
        };
        Err(PresetError::OutOfRange {
            kind,
            parameter: p.name,
            value,
            accepted,
        })
    }

    /// The preset a saved `spec.schedule` is, or `None` for "Advanced cron".
    ///
    /// Exact match of the canonical integer fields: a comma list, a weekday
    /// range or a day-of-month past 28 are legal cadences but not presets, and a
    /// form that cannot express what is saved is worse than the expression.
    /// `@hourly`, `@daily` and `@weekly` are presets: [`expand_alias`] expands
    /// them to `hourly{0}`, `daily{0,0}` and `weekly{0,0,0}`.
    #[must_use]
    pub fn match_preset(schedule: &str) -> Option<Preset> {
        // The expression must be one this product would actually run. A string
        // that does not parse has no preset, and saying so here keeps the form
        // from offering to "edit" a cadence the controller refuses.
        Cron::parse(schedule).ok()?;
        let trimmed = schedule.trim();
        let expanded = expand_alias(trimmed).unwrap_or(trimmed);
        let f: Vec<&str> = expanded.split_whitespace().collect();
        let [minute, hour, dom, month, dow] = f.as_slice() else {
            return None;
        };
        if *month != "*" {
            return None;
        }
        let minute = literal(minute)?;
        match (hour, dom, dow) {
            (&"*", &"*", &"*") => Some(Preset::Hourly { minute }),
            (h, &"*", &"*") => {
                if let Some(step) = h.strip_prefix("*/") {
                    let n = literal(step)?;
                    if !EVERY_N_HOURS_VALUES.contains(&n) {
                        return None;
                    }
                    return Some(Preset::EveryNHours { n, minute });
                }
                Some(Preset::Daily {
                    hour: literal(h)?,
                    minute,
                })
            }
            (h, &"*", d) => Some(Preset::Weekly {
                day_of_week: literal(d)?,
                hour: literal(h)?,
                minute,
            }),
            (h, d, &"*") => {
                let day_of_month = literal(d)?;
                if !(1..=28).contains(&day_of_month) {
                    return None;
                }
                Some(Preset::Monthly {
                    day_of_month,
                    hour: literal(h)?,
                    minute,
                })
            }
            _ => None,
        }
    }

    /// A field that is exactly one decimal integer with no sign, no leading
    /// zero beyond `0` itself, and nothing else.
    ///
    /// The leading-zero refusal makes the round trip an equality: `Cron::parse`
    /// reads `0 03 * * *` as hour 3, but [`compile`] emits `0 3 * * *`, and the
    /// form would silently rewrite the saved expression on the next save.
    fn literal(term: &str) -> Option<u32> {
        if term.len() > 1 && term.starts_with('0') {
            return None;
        }
        if !term.bytes().all(|b| b.is_ascii_digit()) || term.is_empty() {
            return None;
        }
        term.parse().ok()
    }

    /// The catalogue as the API serves it.
    #[must_use]
    pub fn catalogue() -> &'static [PresetSpec] {
        CATALOGUE
    }

    /// Re-exported so a caller that only took `presets` can still name the
    /// parse error [`match_preset`] swallows.
    pub type ScheduleError = CronError;
}
