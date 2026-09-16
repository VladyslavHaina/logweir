//! Cadence: the time zone a cron expression is read in, the policy inputs that
//! decide whether a due slot may still run, and the preview the console shows.
//!
//! Decision **D1 §4** (PLAT-04.2). Everything here is a pure function of its
//! arguments, for the same reason [`crate::slot`] is: `Utc::now()` appears
//! nowhere in this file, so a duplicate reconcile after a crash computes the
//! same slot, the same name and the same preview. Guard **G-SLOT** covers this
//! module too.
//!
//! # The three properties this module holds
//!
//! **1. An absent time zone is today's behaviour, byte for byte.** Not
//! "equivalent", not "the same modulo rounding" — the SAME CODE. [`Zone::Utc`]
//! delegates straight to [`Cron::last_fire_at_or_before`] and
//! [`Cron::next_fire_after`], so there is no second evaluator that could drift
//! from the one every existing `BackupSchedule` has been scheduled by.
//! `absent_timezone_is_byte_identical_to_legacy_utc` measures it over 10 000
//! random `(expression, instant)` pairs, and the zoned path is compared against
//! it separately for an explicit `timeZone: UTC`.
//!
//! **2. A local wall time that is not a single instant still fires.** The rule
//! is D1 §4.3, and it is stated as a rule about INSTANTS rather than about
//! clocks:
//!
//! > every real instant whose local wall time matches fires once; a matching
//! > local time that does not exist fires once at the end of the gap
//! > (deduplicated).
//!
//! Two consequences follow and both are intended. A fixed local time inside a
//! repeated (fall-back) hour fires at BOTH occurrences, because both are real
//! instants whose local wall time matches. A fixed local time inside a
//! spring-forward gap fires ONCE, at the transition instant, because that is
//! the first real instant at or after the time the user asked for. An interval
//! schedule such as `*/15 * * * *` keeps its UTC cadence through both
//! transitions without a burst and without a hole: in the gap every matching
//! local minute maps to the same transition instant and the duplicates
//! collapse, and in the repeated hour the two occurrences interleave into the
//! same 15-minute grid.
//!
//! The rejected readings, named so they are not re-derived. *Skip the gap*
//! loses a whole day for a fixed-time schedule — a nightly backup that does not
//! run on the night the clocks go forward, silently, once a year. *Fire once in
//! the repeated hour* has to choose which occurrence, and either choice makes
//! the run's local time disagree with the one the preview showed. The cost of
//! the rule as written is at most one extra run per fall-back for a fixed-time
//! schedule; `status.nextRuns` shows both instants with
//! [`Adjustment::RepeatedLocalTimeFirst`] / [`Adjustment::RepeatedLocalTimeSecond`]
//! beside them, so it is a predicted outcome rather than a surprise, and
//! `concurrencyPolicy: Forbid` still prevents overlap.
//!
//! **3. Slot identity stays the UTC instant.** `yyyymmdd-hhmmss` of the UTC
//! instant, exactly as [`crate::slot::slot_name`] has always produced it. A
//! slot named in local time would not be unique across a fall-back (two
//! instants, one name, one object — the colliding-`backup_id` false pass
//! [`crate::slot`]'s header is about) and would not be monotonic. The zone is
//! recorded BESIDE the slot, never inside it.
//!
//! # Where the time-zone database comes from
//!
//! [`TZDB_SOURCE`] names it: `chrono-tz`, compiled into the binary, pinned by
//! `Cargo.lock` and asserted against this constant by a test. The host's
//! `/usr/share/zoneinfo` was rejected in D1 §4.3 — the controller image, the
//! API image and the test runner would each carry whatever tzdata their base
//! shipped and could compute different UTC instants for the same
//! `spec.timeZone`, which is a scheduled backup that fires at two different
//! times depending on which process evaluated it. The price is that a tz rule
//! amendment needs a release, and that price is paid where it can be seen:
//! `status.policy.tzdb` carries this string.
//!
//! # Why the preview is computed HERE and nowhere else
//!
//! D1 §4.4 puts one cadence evaluator in the product. The controller writes
//! `status.nextRuns` from [`Cadence::next_runs`]; the API answers
//! `GET /api/v1/cadence-previews` from the same function; the browser renders
//! what one of those two returns and evaluates no cron at all. A second
//! implementation in JavaScript is a second answer to "when does my backup
//! run", and the one the user reads would be the one that is not authoritative.

use std::collections::BTreeMap;
use std::fmt;

use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};

use crate::slot::{slot_name, Cron, CronError, WALK_DAYS};

/// The time-zone database this binary evaluates `spec.timeZone` against.
///
/// WRITTEN INTO `status.policy.tzdb` (D1 §4.8) so an operator reading a
/// schedule can tell which rule set produced its slots, and PINNED TO
/// `Cargo.lock` by `tests/cadence.rs::the_tzdb_source_is_the_version_cargo_lock_resolves`.
/// The pin is the point: a tz amendment that moves a transition changes when
/// backups run, and a string that says `chrono-tz 0.10.4` while the binary
/// links 0.11 is worse than no string at all.
pub const TZDB_SOURCE: &str = "chrono-tz 0.10.4";

/// `spec.startingDeadlineSeconds` when the field is absent (D1 §4.1).
///
/// 3600, WHICH IS TODAY'S HORIZON AND NOT A NEW DEFAULT. Every schedule that
/// exists before this task was scheduled with a one-hour missed-slot horizon
/// (`docs/kubernetes.md` §9), so absence has to keep meaning exactly that.
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

/// The cap on the skipped-slot enumeration of D1 §4.5 step 5.
///
/// A CAP AND NOT A LIMIT ON THE ANSWER. A controller that was down for a year
/// with `*/1 * * * *` has half a million skipped slots; counting them exactly
/// buys nothing and walking them costs a reconcile. [`SkippedSlots::capped`]
/// says the count stopped here, so `status.missedSlots.countCapped` can say
/// "at least" rather than a number that is quietly wrong.
pub const MAX_SKIPPED_SLOT_ENUMERATION: usize = 1000;

/// The widest UTC offset this walk allows any zone to have, in hours.
///
/// TWENTY-SIX, WHICH IS WIDER THAN ANY ZONE AND DELIBERATELY SO. Live offsets
/// run from −12:00 to +14:00; historical LMT entries in the database reach a
/// little past both. This number is not a claim about tzdb — it is the slack
/// [`Cadence::fire_after`] and [`Cadence::last_fire_at_or_before`] use to know
/// when they may STOP walking local dates, and being generous costs a handful
/// of extra date probes while being tight would cost a missed slot.
///
/// AND IT IS LOAD-BEARING, MEASURED RATHER THAN ASSUMED. Narrowed to 12, the
/// walk stops one local date early for `0 0 * * *` in `Pacific/Apia` and
/// reports [`Adjustment::NonexistentLocalTimeShifted`] against
/// 2011-12-30T10:00Z — an instant whose local time, 2011-12-31T00:00+14:00,
/// exists and is matched. The INSTANT is unchanged, which is why only a marker
/// assertion sees it;
/// `tests/cadence.rs::apia_skipped_day_maps_all_matches_to_one_instant` makes
/// that assertion at every preview length from 1 to 4, because the walk may
/// stop as soon as it holds `count` firings.
const MAX_OFFSET_HOURS: i64 = 26;

/// How far past a nonexistent local time the end of its gap is searched, in
/// minutes (D1 §4.3: "search forward ≤ 48 h").
///
/// FORTY-EIGHT HOURS BECAUSE A GAP IS NOT ALWAYS AN HOUR. `Pacific/Apia`
/// skipped the whole of 2011-12-30 when it crossed the date line, so every
/// local time on that date is nonexistent and the first real instant after it
/// is a day and a bit later. An hour-shaped search would have returned `None`
/// for a schedule that must still fire.
const GAP_SEARCH_MINUTES: i64 = 48 * 60;

/// How many local dates either side of the walk are probed before the answer
/// can be trusted to be the extreme one.
///
/// FOUR, AND IT IS [`MAX_OFFSET_HOURS`] PLUS [`GAP_SEARCH_MINUTES`] ROUNDED UP
/// TO WHOLE DAYS. A local date's instants can land up to 26 h either side of
/// it, and a gap shift can push one a further 48 h into the future; 26 + 48 is
/// 74 h, under four days. Starting the walk that far out and stopping that far
/// past the first hit is what makes "the first hit is the earliest hit" true
/// rather than merely usual.
///
/// IT IS LOAD-BEARING, AND SETTING IT TO ZERO LOSES A WHOLE FIRING. The case is
/// `America/Sitka` on 1867-10-18 — the Alaska purchase, when the territory
/// moved from the Russian calendar at +14:59 to the American one at −09:01 and
/// lived the same local day TWICE, a 24-hour repeated day. For `0 0 * * *`,
/// [`Cadence::fire_at_or_before`]`(1867-10-19T01:00Z)` is 1867-10-18T09:01:13Z,
/// and that instant comes from local date 1867-10-**19** — one local date
/// AFTER the local date of the argument, because at +14:59 a local midnight
/// lands most of a day earlier in UTC. A walk that started at the argument's
/// own local date would never visit it and would answer 1867-10-17T09:01:13Z,
/// a whole slot too early. `America/Juneau`, `America/Metlakatla` and
/// `America/Anchorage` carry the same day.
///
/// An earlier round of this module recorded the opposite — "no measured effect"
/// — on the strength of a sweep that covered only the eastward, gap-shaped
/// direction (`Pacific/Apia` 2011, `Pacific/Kiritimati`) and no 24-hour fall
/// back, which is precisely where the walk's START date decides the answer.
/// The claim was false and the review that found it is why this paragraph
/// exists: `tests/cadence.rs::a_twenty_four_hour_repeated_day_needs_the_edge_slack`
/// now holds the case, so the constant cannot be inlined away as dead slack.
const EDGE_DAYS: i64 = 4;

/// The bound on how many local dates either direction of a zoned walk visits.
///
/// [`WALK_DAYS`] IS THE FIGURE `slot.rs` ALREADY ARGUED — ten years, because
/// `0 0 29 2 *` can go eight of them without firing — and the edges are
/// [`EDGE_DAYS`] at each end. Reproducing the bound rather than inventing one
/// is what makes "a zoned cadence answers for exactly the expressions a UTC one
/// answers for" true.
const MAX_DATE_STEPS: u64 = WALK_DAYS as u64 + 2 * EDGE_DAYS.unsigned_abs();

// ---------------------------------------------------------------------------
// The zone
// ---------------------------------------------------------------------------

/// The zone a cadence's cron fields are read in.
///
/// TWO VARIANTS AND NOT `Option<Tz>`, because the two are not the same claim.
/// [`Zone::Utc`] is *the absent field*, and it takes a code path that IS the
/// pre-D1 evaluator; [`Zone::Named`] is a user's explicit choice and takes the
/// local-date walk. They agree for `Tz::UTC` — `explicit_utc_agrees_with_the_absent_field`
/// asserts it — and that agreement is a test rather than an assumption.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Zone {
    /// `spec.timeZone` absent: UTC, evaluated by [`crate::slot`] itself.
    Utc,
    /// A named IANA zone resolved against the compiled-in database.
    Named(Tz),
}

impl Zone {
    /// Resolve `spec.timeZone` — `None` for the absent field.
    ///
    /// # Errors
    ///
    /// [`CadenceError::UnknownTimeZone`], naming the string, when the database
    /// has no such zone. D1 §4.3 makes this `Ready=False`
    /// `reason=UnknownTimeZone` with NO admissions and running work
    /// untouched: a typo in a zone name must not fire a backup at the wrong
    /// hour, and must not stop one that is already running.
    ///
    /// AN EMPTY STRING IS REFUSED RATHER THAN READ AS UTC. `timeZone: ""` is a
    /// field the user set, and reading a set field as "absent" is how a
    /// schedule ends up running in a zone nobody chose.
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
    /// `2026-10-25T02:30:00+02:00`.
    ///
    /// THE OFFSET IS PART OF THE RENDERING AND NOT DECORATION. It is the only
    /// thing that distinguishes the two occurrences of a repeated hour when a
    /// user reads the preview: `02:30:00+02:00` and `02:30:00+01:00` are the
    /// same clock face and two different instants.
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

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Fires and previews
// ---------------------------------------------------------------------------

/// What the local clock did to a slot, when it did anything (D1 §4.4).
///
/// ABSENT MEANS "NOTHING HAPPENED", which is every slot of every UTC schedule
/// and all but a handful of any zoned one. A marker is written only where the
/// local wall time and the UTC instant tell different stories, so a preview row
/// carrying one is a row the user should read twice.
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
    /// How truthful this marker is about the instant it is attached to; lower
    /// wins when two matched local times map to ONE instant.
    ///
    /// THE COLLISION IS REAL AND HAS A RIGHT ANSWER. In `Europe/Berlin` on
    /// 2027-03-28 a `*/15` schedule matches 02:00, 02:15, 02:30 and 02:45 —
    /// all nonexistent, all shifted to 01:00 UTC — and ALSO matches 03:00
    /// local, which IS 01:00 UTC and exists. One instant, five reasons. The
    /// instant fires once (the rule says "deduplicated") and the marker it
    /// carries should be the one a reader can verify: there is a real local
    /// 03:00 here, so `None` outranks the shift.
    fn rank(this: Option<Adjustment>) -> u8 {
        match this {
            None => 0,
            Some(Adjustment::RepeatedLocalTimeFirst | Adjustment::RepeatedLocalTimeSecond) => 1,
            Some(Adjustment::NonexistentLocalTimeShifted) => 2,
        }
    }
}

/// Whether a firing at the SAME instant replaces the one already held.
///
/// ONE TIE-BREAK, THREE CALLERS, AND THAT IS THE WHOLE POINT OF THIS FUNCTION.
/// [`Cadence::fire_at_or_before`], [`Cadence::fire_after`] and [`record`] all
/// reduce several matched local times to one firing, and they reach the same
/// instant from three different directions — backwards through local dates,
/// forwards through them, and collected into a map. Each of them once had its
/// own spelling of "which of these is the answer", and only the map's applied
/// [`Adjustment::rank`]. The consequence was measurable and reached the wire:
/// `Pacific/Apia`, `0 0 * * *`, `fire_after(2011-12-29T10:00Z)` reported
/// 2011-12-30T10:00Z as [`Adjustment::NonexistentLocalTimeShifted`] while
/// `next_runs` reported the SAME instant as unadjusted — so a
/// `BackupSchedule.status` written from both could contradict itself about one
/// slot, in the one module D1 §4.4 exists to make single-valued.
///
/// STRICTLY LESS, NOT LESS-OR-EQUAL: an equal rank means the two markers are
/// equally truthful, and keeping the one already held makes the answer
/// independent of the order the walk happened to visit the local dates in.
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

// ---------------------------------------------------------------------------
// Policy inputs
// ---------------------------------------------------------------------------

/// `spec.catchUpPolicy` (D1 §4.1).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum CatchUpPolicy {
    /// A slot past its starting deadline is counted and skipped — today's
    /// behaviour, and what an absent field means.
    #[default]
    None,
    /// The LATEST due slot, and only that one, may still run after its
    /// deadline. At most one catch-up run, ever.
    ///
    /// BOUNDED(N) WAS REJECTED (D1 §4.10): `logweir backup run` captures what
    /// the broker retains WHEN IT RUNS, so N catch-up runs executed
    /// back-to-back produce near-identical archives at N times the broker and
    /// storage load, with no recovery-point benefit.
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
    /// Whether this policy is inside the ranges D1 §4.1 states.
    ///
    /// THE CRD'S OpenAPI RANGES ARE THE FIRST GATE AND NOT THE ONLY ONE. An
    /// object created against an older CRD, or by a controller reading a
    /// pruned field, reaches the scheduler without having passed them; D1 §4.5
    /// step 0 validates the policy again and fails closed. Two gates, one
    /// table — this function is the table.
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

/// Whether the two deadline fields are inside the ranges D1 §4.1 states.
///
/// THE SAME "TWO GATES, ONE TABLE" ARGUMENT [`RetryPolicy::validate`] MAKES,
/// AND IT HAS TO HOLD FOR THESE TWO AS WELL. D1 §4.5 step 0 re-validates the
/// whole policy fail-closed precisely because an object created against an
/// older CRD, or read after the API server pruned a field it does not declare,
/// reaches the scheduler without having passed the OpenAPI ranges. Exporting
/// [`MIN_STARTING_DEADLINE_SECONDS`] and friends but leaving the comparison to
/// the controller is how the CRD and the scheduler come to disagree about what
/// "one week" means.
///
/// `None` IS ALWAYS VALID: an absent field is the documented default
/// ([`DEFAULT_STARTING_DEADLINE_SECONDS`], [`DEFAULT_ACTIVE_DEADLINE_SECONDS`]),
/// and a default this module chose cannot be out of a range this module states.
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
/// THE ORDER OF THE ARMS IS THE TRUTH TABLE'S ORDER AND IS LOAD-BEARING. The
/// deadline is tested FIRST, so a slot inside the horizon is `Scheduled`
/// whatever `catchUpPolicy` says and whatever `effective_since` is — that is
/// today's behaviour and D1 §4.7 keeps it deliberately ("a slot that came due
/// before the schedule was created but is still inside
/// `startingDeadlineSeconds` is admitted by row 17").
///
/// THE BOUNDARY IS INCLUSIVE. `now - slot == starting_deadline` is INSIDE the
/// horizon, which is what `<=` means and what
/// `the_starting_deadline_boundary_is_inclusive` pins to the second. An
/// exclusive boundary would make a slot that arrives exactly on the horizon
/// depend on which side of a millisecond the reconcile woke up.
///
/// `starting_deadline_seconds` is the resolved value — the caller supplies
/// [`DEFAULT_STARTING_DEADLINE_SECONDS`] for an absent field.
///
/// # Precondition
///
/// **`slot <= now`.** This function answers "may the slot that came due still
/// run", and D1 §4.5 step 5 produces its only correct argument —
/// [`Cadence::latest_due_slot`], which is `last_fire_at_or_before(now)` and so
/// never returns a future instant. A `slot` AFTER `now` has a negative age,
/// falls into the first arm and comes back [`SlotAdmission::Scheduled`]: this
/// function would tell a caller to run a backup before its slot. It is stated
/// here rather than guarded because a guard would need a fifth outcome that D1
/// §4.7's table has no row for, and because the value belongs to the caller's
/// step 5 — `admit_slot_is_documented_for_a_future_slot` pins the answer so it
/// is a contract and not an accident.
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
/// classifies.
///
/// A SHAPE OF ITS OWN AND NOT `&Backup`. The classification is the input the
/// schedule controller feeds [`admit_retry`], and it has to be answerable by
/// the API and by a test without a Kubernetes object in hand; taking the two
/// fields it actually reads is also what keeps this module off the CRD types
/// D1 §11.1 gives another worker.
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

/// The terminal states that ARE retried when there is no exit code (D1 §4.6).
///
/// AN ALLOWLIST, AND THAT IS THE "UNKNOWN ⇒ NOT RETRYABLE" RULE ITSELF. D1
/// §4.6 calls for a closed `match`; a list of what to retry closes it in the
/// safe direction, because a terminal state added by a later task — or written
/// by a newer controller and read by an older one — falls to `false` rather
/// than into an automatic re-run of work whose failure nobody has classified.
/// The opposite spelling, a list of what NOT to retry, fails open on exactly
/// the same input.
///
/// WHAT UNITES THE FOUR: the work never started, or started and was
/// interrupted by something outside the run's own control — so running it
/// again is a fresh attempt at the same job rather than a repeat of a decision
/// the product already made. A guard refusal, a not-a-pass drill and a signing
/// failure are all decisions, and D1 §4.6 never retries a decision.
///
/// `"DiscoveryFailed"` IS SPELLED OUT because D1 §3.4 assigns the constant to
/// `conditions.rs` and D1 §11.1 assigns that file to another worker; the
/// spelling is pinned by `retry_classification_is_a_closed_match_and_unknown_is_not_retryable`,
/// and the constant replaces the literal when it lands.
pub const RETRYABLE_TERMINAL_STATES: &[&str] = &[
    crate::conditions::TERMINAL_STATE_DISRUPTED_MID_DRILL,
    crate::conditions::TERMINAL_STATE_POD_UNSCHEDULABLE,
    crate::conditions::TERMINAL_STATE_NO_EXIT_CODE,
    "DiscoveryFailed",
];

/// Whether D1 §4.6 retries this terminal record.
///
/// # The table, and why each row is the way round it is
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
/// THE UNRECOGNISED ARM IS THE ONE THAT MATTERS. A retry loop that fires on a
/// state nobody classified is a run that repeats every `delaySeconds` until
/// `maxRetries` — three extra full backups of a cluster for a refusal that was
/// never going to change. `false` costs one missed retry and says so in
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
        ///
        /// THE DECISION IS THE SAME AND THE REASON IS NOT (D1 §4.7 row 10).
        /// A schedule that configured retries and used them up reports
        /// `RetryExhausted`; a schedule that never asked for retries reports
        /// `RunFailed`, because "exhausted" would name a budget it never had.
        /// `max_retries: 0` alone cannot tell the two apart, so a caller
        /// reading only this variant would have to go back to the spec — which
        /// is how the two reasons come to drift.
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
/// # The "never past the next due slot" rule, and where it lives
///
/// D1 §4.5 chooses the latest due slot FIRST (step 5) and only then observes
/// that slot's attempt chain (step 6), so a retry can never outlive the slot it
/// belongs to: the moment a newer slot comes due, the older one is superseded
/// and its pending retry is simply not what this reconcile is deciding. That
/// ordering is reproduced here as the FIRST arm rather than left implicit,
/// because "the scheduler happens to call this in the right order" is not a
/// property a test can hold — `a_retry_is_superseded_by_the_next_due_slot`
/// holds this one.
///
/// A retry that has waited out a long delay across a slot boundary is
/// therefore dropped in favour of a fresh run of the newer slot, which is the
/// right trade: a backup captures what the broker retains when it runs, so the
/// newer slot's run is strictly more useful than a re-run of the older one.
///
/// `retryable` is the caller's classification of the terminal record (D1 §4.6:
/// a closed `match` whose unknown arm is NOT retryable). It is an argument
/// rather than a computation here because the terminal record's shape is the
/// `Backup` status type, which this module deliberately does not know.
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
    /// THE UNDERCOUNT THIS FIELD EXISTS TO STOP BEING SILENT. A downtime longer
    /// than [`WALK_DAYS`] under an expression too sparse to reach `cap` first —
    /// `0 0 1 1 *` and twelve years, say — runs out of window before it runs
    /// out of slots, and without this flag `capped: false` would present that
    /// floor as an exact total. `status.missedSlots.count` would then read as
    /// "twelve slots were skipped" for a schedule that skipped more.
    ///
    /// It is a separate flag from [`SkippedSlots::capped`] because the two are
    /// different facts about the answer and an operator acts on them
    /// differently: `capped` means "stop counting, there are thousands", and
    /// this one means "this product cannot name a slot that far out at all" —
    /// the same ten-year blindness [`Cron`] has had since PLAT-04.1.
    pub horizon_reached: bool,
}

// ---------------------------------------------------------------------------
// The cadence
// ---------------------------------------------------------------------------

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
    ///
    /// For [`Zone::Utc`] this IS [`Cron::last_fire_at_or_before`] — the same
    /// call, not an equivalent one, which is what makes the absent field
    /// byte-for-byte today's behaviour.
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

    /// The one slot a reconcile at `now` may act on (D1 §4.5 step 5).
    ///
    /// ONLY THE LATEST DUE SLOT IS EVER ELIGIBLE, which is the whole backlog
    /// bound: a controller that was down for a week finds one slot here, not
    /// a queue of them, so `catchUpPolicy: Latest` can create at most one
    /// catch-up run however long the downtime was.
    #[must_use]
    pub fn latest_due_slot(&self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.last_fire_at_or_before(now)
    }

    /// The most recent firing at or before `t`, with its DST marker.
    ///
    /// THE MARKER IS THE ONE [`Cadence::next_runs`] GIVES THE SAME INSTANT.
    /// Several matched local times can collapse onto one instant, and the
    /// answer must not depend on which of this module's three walks a caller
    /// happened to use — see [`marker_supersedes`], and
    /// `tests/cadence.rs::every_walk_agrees_on_the_marker_for_an_instant`,
    /// which measures it over a corpus rather than asserting it.
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

    /// The earliest firing strictly after `t`, with its DST marker.
    ///
    /// STRICTLY AFTER, which is what makes [`Cadence::next_runs`] terminate and
    /// produce no duplicates: each entry is the argument for the next call, and
    /// the sequence is strictly increasing even where several matched local
    /// times collapsed onto one instant.
    ///
    /// The marker is the one [`Cadence::next_runs`] gives the same instant; see
    /// [`Cadence::fire_at_or_before`].
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
    /// SHORTER THAN `count` IS A REAL ANSWER, not a failure: an expression such
    /// as `0 0 29 2 *` runs out of firings inside [`WALK_DAYS`], and an empty
    /// vector is what a schedule that never fires again should show.
    ///
    /// `count` is NOT clamped here. The API clamps to [`MAX_PREVIEW_COUNT`] and
    /// the controller asks for [`STATUS_NEXT_RUNS`]; a clamp in this function
    /// would silently return fewer rows than a caller that had already
    /// validated its own bound.
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

    /// The next `count` firings strictly after `after`, ascending.
    ///
    /// ONE WALK OF THE LOCAL DATES, NOT `count` OF THEM. Calling
    /// [`Cadence::fire_after`] in a loop is the obvious spelling and is
    /// quadratic in the wrong variable: each call re-scans the same
    /// [`EDGE_DAYS`]-wide window of local dates, and a 20-entry preview of a
    /// minutely expression re-converted the same 1440 local minutes twenty
    /// times. Measured on the release build before this was one pass:
    /// 3.15 ms per `fire_after` for `* * * * *`, so 63 ms for one preview
    /// request and 3.1 s for a capped [`Cadence::skipped_slots`] walk.
    fn fires_after(&self, after: DateTime<Utc>, count: usize) -> Vec<Fire> {
        self.fires_after_bounded(after, count, None).fires
    }

    /// [`Cadence::fires_after`], additionally stopping once the walk has passed
    /// `until`.
    ///
    /// THE TIME BOUND IS NOT AN OPTIMISATION, IT IS THE DIFFERENCE BETWEEN A
    /// 1-SLOT WALK AND A 1000-SLOT ONE. [`Cadence::skipped_slots`] asks for
    /// [`MAX_SKIPPED_SLOT_ENUMERATION`]` + 1` firings, and the ordinary
    /// reconcile it runs in has ZERO skipped slots; without the bound every
    /// healthy reconcile would walk a thousand slots into the future to
    /// discover that none of them is inside its window.
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
    /// (D1 §4.5 step 5).
    ///
    /// BOTH ENDS EXCLUSIVE, because the interval it counts is
    /// `(lastEvaluatedSlot, S)`: the slot already accounted for and the slot
    /// being decided are both outside it, so a slot is counted exactly once
    /// across reconciles.
    #[must_use]
    pub fn skipped_slots(
        &self,
        after: DateTime<Utc>,
        before: DateTime<Utc>,
        cap: usize,
    ) -> SkippedSlots {
        // ONE MORE THAN THE CAP IS ASKED FOR, AND THAT EXTRA ONE IS THE WHOLE
        // POINT of `capped`. `count == cap` alone cannot say whether the walk
        // ran out of slots or ran out of budget, and
        // `status.missedSlots.countCapped` is exactly the field an operator
        // reads to tell "1000 slots were skipped" from "at least 1000 were".
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
    /// LOCAL calendar date, ascending and deduplicated.
    ///
    /// THE WHOLE DST RULE IS THE `match` IN THIS FUNCTION, and it is three
    /// arms because `from_local_datetime` has three answers:
    ///
    /// * `Single` — one real instant. The ordinary case, and every case in a
    ///   zone with no DST.
    /// * `Ambiguous` — the local time happens twice, so BOTH instants fire.
    ///   They are distinct slots with distinct names, and `Forbid` still keeps
    ///   them from overlapping.
    /// * `None` — the local time does not exist, so the firing moves to the end
    ///   of the gap. Several matched local minutes inside one gap collapse onto
    ///   that single instant, which is what the `BTreeMap` deduplicates and
    ///   what keeps an interval schedule from bursting.
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
/// whether [`horizon_date`] is what stopped it.
///
/// A STRUCT RATHER THAN A `Vec` BECAUSE "I RAN OUT OF SLOTS" AND "I RAN OUT OF
/// WINDOW" ARE DIFFERENT ANSWERS and only the caller can tell which one matters
/// to it. [`Cadence::next_runs`] does not care — a preview that stops at ten
/// years is a preview that stops — but [`Cadence::skipped_slots`] reports a
/// COUNT, and a count truncated by the window is a floor presented as a total.
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
/// `local` itself does not exist.
///
/// THE END OF THE GAP IS THE TRANSITION INSTANT, and searching minute by minute
/// is what makes that true for a 30-minute gap (`Australia/Lord_Howe`), a
/// 60-minute one (most zones) and a 24-hour one (`Pacific/Apia`, 2011-12-30)
/// without three cases. `Ambiguous` is answered with its EARLIEST instant for
/// the same reason the gap is answered with its end: the firing moves forward
/// to the first real instant and no further.
///
/// `None` only when the whole [`GAP_SEARCH_MINUTES`] window is nonexistent,
/// which no entry in the database is; returning it rather than looping is what
/// keeps a corrupt or future database from hanging a reconcile.
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
/// ONE HORIZON FOR BOTH BRANCHES OF [`Cadence::fires_after_bounded`], AND IT IS
/// THE BOUND `slot.rs` ALREADY ANSWERS WITHIN. Without it the two paths gave
/// different answers to the same question: the UTC branch iterates
/// [`Cron::next_fire_after`], which restarts its own [`WALK_DAYS`] walk from
/// each firing, so `0 0 29 2 *` yielded twenty February 29ths stretching to
/// 2168, while the zoned branch makes ONE pass over local dates and yielded the
/// one inside the next ten years. A preview that depends on whether
/// `spec.timeZone` was written is the one thing D1 §4.4's "one cadence
/// evaluator" rules out, and `explicit_utc_agrees_with_the_absent_field` now
/// compares whole previews rather than single firings.
///
/// TEN YEARS IS THEREFORE ALSO THE LIMIT OF [`Cadence::skipped_slots`]: a
/// downtime longer than [`WALK_DAYS`] under an expression sparse enough not to
/// reach [`MAX_SKIPPED_SLOT_ENUMERATION`] first would under-count. That is the
/// blindness [`Cron`] itself has had since PLAT-04.1 — it cannot name a slot
/// further out than this either — and inventing a wider bound here would be a
/// count of slots this product could not have scheduled.
///
/// [`NaiveDate::MAX`] rather than a panic at the end of the calendar: the walk
/// stops at [`MAX_DATE_STEPS`] regardless, and a date arithmetic overflow is
/// not a reason to answer a preview differently.
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

// ---------------------------------------------------------------------------
// Presets
// ---------------------------------------------------------------------------

/// The preset catalogue (D1 §4.2): the five cadences the console offers as a
/// form, and the canonical cron string each one compiles to.
///
/// **NO PRESET IS STORED IN ANY SPEC.** `spec.schedule` stays the single source
/// of truth and a preset is a rendering of it. D1 §4.10 rejects a stored preset
/// field for the reason a reader can check on any object: two representations
/// of one cadence have a state in which they disagree, and nothing can say
/// which of them the controller obeyed.
///
/// [`match_preset`] is therefore the other half of [`compile`], not an
/// afterthought: the form shows a preset when the saved expression IS one, and
/// "Advanced cron" when it is not. `every_preset_round_trips_through_its_cron`
/// asserts `match_preset(compile(p)) == Some(p)` for every preset in range —
/// 60 + 6×60 + 24×60 + 7×24×60 + 28×24×60 of them.
pub mod presets {
    use serde::{Deserialize, Serialize};

    use crate::slot::{expand_alias, Cron, CronError};

    /// The `n` values `everyNHours` accepts (D1 §4.2).
    ///
    /// DIVISORS OF 24, AND THAT IS THE POINT. `*/n` in the hour field matches
    /// local wall-clock hours divisible by `n`, so `*/5` would fire at 00:00,
    /// 05:00, 10:00, 15:00, 20:00 and then again at 00:00 — a four-hour gap
    /// once a day that nobody chooses on purpose. The five that divide 24
    /// evenly, plus 12, are the set that means what the form says it means.
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

    /// One preset kind, as DATA — which is the form the API serves and the
    /// console renders.
    ///
    /// D1 §4.2 requires the catalogue to be exposed as data precisely so the
    /// browser needs no evaluator: `cron_template` is a STRING TEMPLATE with
    /// `{parameter}` holes, so a page can show "Every day at 02:00" and the
    /// expression it will save without parsing anything.
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

    /// The `dayOfMonth` parameter of `monthly`.
    ///
    /// 28 AND NOT 31: a `0 3 31 * *` schedule does not run in February, April,
    /// June, September or November, which is a monthly backup that silently
    /// happens seven times a year. The ADVANCED CRON field still accepts 29, 30
    /// and 31 — the form just does not hand an adopter that hole by default.
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
    /// the catalogue accepts. A preset is a DTO an API accepts from a browser,
    /// so its ranges are checked here rather than trusted: `daily{hour: 25}`
    /// would otherwise compile to `0 25 * * *`, which [`Cron::parse`] refuses
    /// at a point where the error names the cron field instead of the form
    /// field the user filled in.
    ///
    /// EACH CHECK NAMES ITS PARAMETER CONSTANT, NEVER `CATALOGUE[i]`. An
    /// earlier spelling reached in by index — `CATALOGUE[1].parameters[0]` for
    /// `n`, `[3]` for `dayOfWeek`, `[4]` for `dayOfMonth` — so reordering the
    /// catalogue would have validated `everyNHours` against `weekly`'s range,
    /// and the generated fixture, regenerated in the same commit as the
    /// documented workflow says, would have agreed with the mistake.
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

    /// The preset a saved `spec.schedule` IS, or `None` for "Advanced cron".
    ///
    /// EXACT MATCH OF THE CANONICAL INTEGER FIELDS, and nothing looser. A
    /// comma list in the minute field, a weekday RANGE in the day-of-week
    /// field and a day-of-month past 28 are all legal cadences, and none of
    /// them is a preset; showing a form that cannot express what is saved is
    /// worse than showing the expression.
    ///
    /// THE THREE `@` ALIASES ARE PRESETS (D1 §4.2). `@hourly`, `@daily` and
    /// `@weekly` expand — through [`expand_alias`], the one table — to `0 * * *
    /// *`, `0 0 * * *` and `0 0 * * 0`, which this matcher then reads as
    /// `hourly{0}`, `daily{0,0}` and `weekly{0,0,0}`.
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
    /// THE LEADING-ZERO REFUSAL IS WHAT MAKES THE ROUND TRIP AN EQUALITY.
    /// `Cron::parse` accepts `03` as hour 3, so `0 03 * * *` is a legal
    /// expression — but [`compile`] emits `0 3 * * *`, and a matcher that read
    /// both as `daily{3,0}` would claim a preset whose own compilation is a
    /// DIFFERENT string from the one saved. The form would then silently
    /// rewrite the user's expression on the next save.
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
