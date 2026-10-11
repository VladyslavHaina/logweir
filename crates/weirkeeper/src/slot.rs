//! The trigger, and the names it is a pure function of — guard **G-SLOT**.
//!
//! # The property this module holds
//!
//! A second `backup` into a colliding `backup_id` does not accumulate: measured
//! on this project's own stack, a re-run left the manifest describing 2048
//! records while the broker held 6000, a partial archive a later drill
//! restores from and calls a success. A name derived from a reconcile-time
//! clock or from `status.lastFireTime` produces two objects when the
//! controller crashes between the `create` and the status write.
//!
//! So the name is a pure function of the trigger: everything here takes its
//! inputs as arguments and reads no clock, no status and no environment. A
//! duplicate reconcile after a crash computes the same name, the API server
//! answers its `POST` with 409 `AlreadyExists`, and that 409 is the idempotence
//! key: no lease, no lock, no `status` round trip.
//!
//! # Why the slot is `yyyymmdd-hhmmss` and not `YYYYmmddTHHMMSSZ`
//!
//! A Kubernetes object name is a DNS-1123 subdomain (lowercase alphanumerics,
//! `-` and `.`), so the `T` and `Z` of the ISO-8601 basic form are refused. The
//! `YYYYmmddTHHMMSSZ` form stays for Kafka topic names, which permit it.
//!
//! # Why the cron parser is hand-written
//!
//! Global Constraint 38 closes the workspace graph, and five integer fields,
//! five syntactic forms and three `@` aliases do not justify a dependency. The
//! parser's obligation in return is to refuse what it does not understand,
//! naming the field: a cron parser whose unknown token quietly becomes `*`
//! turns a typo into "every minute of every day".

use std::fmt;

use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};

/// The maximum number of days [`Cron::last_fire_at_or_before`] and
/// [`Cron::next_fire_after`] will walk before giving up and returning `None`.
///
/// Ten years: the sparsest expression this grammar can write, `0 0 29 2 *`, has
/// an eight-year gap across a non-leap century (2096 → 2104), so anything
/// shorter reports `None` for a legal expression, and an expression with no
/// match inside ten years has none a scheduler should act on (the reconciler
/// writes a condition for `None`). Public because [`crate::cadence`] walks
/// local dates and must reproduce the same bound.
pub const WALK_DAYS: u32 = 3700;

/// A five-field cron expression: minute hour day-of-month month day-of-week.
///
/// Supports `*`, a literal, a comma list, an `a-b` range and a `*/n` step.
/// Anything else is a parse error naming the field — never a silent match-all.
///
/// Each field is a bitset (the widest has 60 values), so matching is a shift
/// and a mask.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cron {
    /// Minutes 0..=59, bit *n* set when minute *n* matches.
    minute: u64,
    /// Hours 0..=23.
    hour: u64,
    /// Days of month 1..=31.
    day_of_month: u64,
    /// Months 1..=12.
    month: u64,
    /// Days of week 0..=6, Sunday = 0.
    day_of_week: u64,
    /// Whether the day-of-month field's first character was anything other
    /// than `*`. Kept apart from the bitset because the cron day rule depends on
    /// whether the field was restricted, not on its bits: `*` and `1-31` produce
    /// identical bitsets and behave differently ([`Cron::day_matches`],
    /// [`parse_field`]).
    dom_restricted: bool,
    /// Whether the day-of-week field's FIRST CHARACTER was anything other than
    /// `*`.
    dow_restricted: bool,
}

/// One field's position and name, for an error message that points at the
/// offending field rather than at the whole expression.
///
/// The index is **1-based**, because that is how a human counts the fields of
/// `17 3 * * 1` when reading an error about the third one.
const FIELDS: [(usize, &str, u32, u32); 5] = [
    (1, "minute", 0, 59),
    (2, "hour", 0, 23),
    (3, "day-of-month", 1, 31),
    (4, "month", 1, 12),
    (5, "day-of-week", 0, 6),
];

/// The three `@` aliases this parser accepts, and what each expands to.
///
/// An adopter will type them, and a `@daily` that parsed as nothing would
/// never fire. Every other `@` form (`@yearly`, `@monthly`, `@reboot`, …) is
/// refused naming these three: `@monthly` read as `@daily` would be thirty
/// unwanted backups a month, and `@reboot` means nothing to a controller.
const AT_FORMS: [(&str, &str); 3] = [
    ("@hourly", "0 * * * *"),
    ("@daily", "0 0 * * *"),
    ("@weekly", "0 0 * * 0"),
];

/// What was wrong with one field.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FieldProblem {
    /// The term matched none of the five accepted forms.
    Unrecognised,
    /// A value outside the field's own range.
    OutOfRange {
        /// The value that was written.
        value: u64,
        /// The field's lowest legal value.
        min: u32,
        /// The field's highest legal value.
        max: u32,
    },
    /// `*/0` — a step of zero, which would either divide by zero or, worse,
    /// be quietly read as `*`.
    ZeroStep,
    /// `a-b` with `a > b`, which matches nothing and is far more likely a
    /// transposition than an intent.
    DescendingRange {
        /// The range's start.
        from: u64,
        /// The range's end.
        to: u64,
    },
}

impl fmt::Display for FieldProblem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unrecognised => write!(f, "is not a literal, list, range or step"),
            Self::OutOfRange { value, min, max } => {
                write!(f, "names {value}, which is outside {min}..={max}")
            }
            Self::ZeroStep => write!(
                f,
                "has a step of 0; a step must be at least 1, and a 0 step is never `*`"
            ),
            Self::DescendingRange { from, to } => {
                write!(f, "is a range whose start {from} is after its end {to}")
            }
        }
    }
}

/// Why an expression did not parse. Every variant names what it refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CronError {
    /// The expression did not have exactly five whitespace-separated fields.
    FieldCount {
        /// How many fields were actually present.
        got: usize,
    },
    /// One field did not parse.
    Field {
        /// The 1-based field position.
        index: usize,
        /// The field's name, as [`FIELDS`] spells it.
        name: &'static str,
        /// The term that failed, verbatim.
        term: String,
        /// What was wrong with it.
        problem: FieldProblem,
    },
    /// An `@` form other than the three [`AT_FORMS`] accepts.
    UnknownAtForm {
        /// The form that was written, verbatim.
        got: String,
    },
}

impl fmt::Display for CronError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FieldCount { got } => write!(
                f,
                "a cron expression has five fields (minute hour day-of-month month \
                 day-of-week); got {got}"
            ),
            Self::Field {
                index,
                name,
                term,
                problem,
            } => write!(f, "cron field {index} ({name}): \"{term}\" {problem}"),
            Self::UnknownAtForm { got } => write!(
                f,
                "unrecognised cron form \"{got}\"; only @hourly, @daily and @weekly are accepted"
            ),
        }
    }
}

impl std::error::Error for CronError {}

impl Cron {
    /// Parse a five-field cron expression, or one of the three `@` aliases.
    ///
    /// # Errors
    ///
    /// [`CronError`], naming the field and the term it refused. There is no
    /// lenient path: an unrecognised term is never read as `*`.
    pub fn parse(expr: &str) -> Result<Cron, CronError> {
        let trimmed = expr.trim();
        // The `@` forms first, by exact match on the whole expression: `@daily
        // 5` is a mistake, and the field-count error would be the wrong one.
        let expanded = if trimmed.starts_with('@') {
            match AT_FORMS.iter().find(|(form, _)| *form == trimmed) {
                Some((_, expansion)) => *expansion,
                None => {
                    return Err(CronError::UnknownAtForm {
                        got: trimmed.to_string(),
                    })
                }
            }
        } else {
            trimmed
        };

        let fields: Vec<&str> = expanded.split_whitespace().collect();
        if fields.len() != FIELDS.len() {
            return Err(CronError::FieldCount { got: fields.len() });
        }

        let mut sets = [0u64; 5];
        let mut restricted = [false; 5];
        for (slot, (&(index, name, min, max), field)) in FIELDS.iter().zip(&fields).enumerate() {
            let (bits, is_restricted) = parse_field(field, index, name, min, max)?;
            sets[slot] = bits;
            restricted[slot] = is_restricted;
        }

        Ok(Cron {
            minute: sets[0],
            hour: sets[1],
            day_of_month: sets[2],
            month: sets[3],
            day_of_week: sets[4],
            dom_restricted: restricted[2],
            dow_restricted: restricted[4],
        })
    }

    /// The most recent instant at or before `t` at which this expression
    /// fires, if there is one within [`WALK_DAYS`].
    ///
    /// Seconds are dropped, not rounded: a cron expression fires at second 0,
    /// so two reconciles 40 seconds apart inside one minute compute the same
    /// `due`, which is what G-SLOT needs.
    #[must_use]
    pub fn last_fire_at_or_before(&self, t: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let mut date = t.date_naive();
        // Inclusive: a fire exactly at `t`'s minute counts as at-or-before.
        let mut cap = i64::from(t.hour() * 60 + t.minute());
        for _ in 0..WALK_DAYS {
            if self.day_matches(date) {
                let mut m = cap;
                while m >= 0 {
                    #[allow(clippy::cast_sign_loss)] // `m >= 0` is the loop condition.
                    let minutes = m as u32;
                    if self.time_matches(minutes) {
                        return at(date, minutes);
                    }
                    m -= 1;
                }
            }
            date = date.pred_opt()?;
            cap = 23 * 60 + 59;
        }
        None
    }

    /// The earliest instant strictly after `t` at which this expression fires,
    /// if there is one within [`WALK_DAYS`].
    ///
    /// Strictly after, including within the same minute: `t` at `03:17:00` for
    /// `17 3 * * 1` yields the following Monday. This is `status.nextFireTime`,
    /// and one equal to the slot just fired would read as "already due".
    #[must_use]
    pub fn next_fire_after(&self, t: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let mut date = t.date_naive();
        let mut floor = i64::from(t.hour() * 60 + t.minute()) + 1;
        for _ in 0..WALK_DAYS {
            if floor > 23 * 60 + 59 {
                date = date.succ_opt()?;
                floor = 0;
            }
            if self.day_matches(date) {
                let mut m = floor;
                while m <= 23 * 60 + 59 {
                    #[allow(clippy::cast_sign_loss)] // `m >= floor >= 0` throughout.
                    let minutes = m as u32;
                    if self.time_matches(minutes) {
                        return at(date, minutes);
                    }
                    m += 1;
                }
            }
            date = date.succ_opt()?;
            floor = 0;
        }
        None
    }

    /// Whether `minutes` — minutes since midnight — matches the hour and
    /// minute fields.
    fn time_matches(&self, minutes: u32) -> bool {
        bit(self.hour, minutes / 60) && bit(self.minute, minutes % 60)
    }

    /// Whether this expression's month, day-of-month and day-of-week fields
    /// match the calendar date `date`, by the classic cron day rule.
    ///
    /// It exposes [`Cron::day_matches`] unchanged for [`crate::cadence`], which
    /// matches local calendar dates in a time zone, so the day rule has one
    /// copy.
    #[must_use]
    pub fn matches_date(&self, date: NaiveDate) -> bool {
        self.day_matches(date)
    }

    /// Whether this expression's hour and minute fields match `minutes`
    /// minutes past midnight.
    ///
    /// The companion window to [`Cron::matches_date`]; see its note for why
    /// [`crate::cadence`] needs both rather than a whole-instant predicate.
    /// `minutes` outside `0..1440` simply matches nothing: [`bit`] is `false`
    /// past its field's width, so there is no panic and no wrap.
    #[must_use]
    pub fn matches_minute_of_day(&self, minutes: u32) -> bool {
        self.time_matches(minutes)
    }

    /// Whether `date` matches the month, day-of-month and day-of-week fields.
    ///
    /// The classic cron day rule, as cronie's `find_jobs` computes it:
    ///
    /// ```text
    /// (DOM_STAR || DOW_STAR) ? (dom && dow) : (dom || dow)
    /// ```
    ///
    /// A field is starred when its first character is `*` ([`parse_field`]), so
    /// `*/2` is starred and `1-31` is not; the `*_restricted` flags are the
    /// negation of `DOM_STAR` / `DOW_STAR`.
    ///
    /// * Neither starred: the union. `0 0 1 * 1` is the first of the month or
    ///   every Monday, and `0 0 1-31 * 1` is every day.
    /// * Either starred: the intersection, because a starred field with a step
    ///   still restricts. `0 0 */2 * 1` is the odd-numbered Mondays, `0 0 * * 1`
    ///   the Mondays, and `0 0 * * *` every day.
    ///
    /// Letting a starred-but-stepped field stand aside (so `0 0 */2 * 1` is
    /// every Monday) is a defensible reading that is not cronie's; this follows
    /// cronie exactly, so a migrated crontab fires on the same days.
    /// `the_day_rule_reads_the_first_character_of_the_field` pins it.
    fn day_matches(&self, date: NaiveDate) -> bool {
        if !bit(self.month, date.month()) {
            return false;
        }
        let dom = bit(self.day_of_month, date.day());
        let dow = bit(self.day_of_week, date.weekday().num_days_from_sunday());
        // `!dom_restricted || !dow_restricted` is cronie's `DOM_STAR ||
        // DOW_STAR`; the `else` here is that condition's true branch.
        if self.dom_restricted && self.dow_restricted {
            dom || dow
        } else {
            dom && dow
        }
    }
}

/// Bit `n` of `set`, for `n` inside the field's range.
fn bit(set: u64, n: u32) -> bool {
    n < 64 && set & (1u64 << n) != 0
}

/// The UTC instant at `date`, `minutes` since midnight, second 0.
fn at(date: NaiveDate, minutes: u32) -> Option<DateTime<Utc>> {
    Some(date.and_hms_opt(minutes / 60, minutes % 60, 0)?.and_utc())
}

/// Parse one field into a bitset, and say whether it was restricted.
///
/// The second half of the return is `false` whenever the field's FIRST
/// CHARACTER is `*`, and it is what [`Cron::day_matches`] needs — see its own
/// note for why a bitset alone cannot carry it.
///
/// # Errors
///
/// [`CronError::Field`], naming this field's index, name and the term.
fn parse_field(
    field: &str,
    index: usize,
    name: &'static str,
    min: u32,
    max: u32,
) -> Result<(u64, bool), CronError> {
    let mut bits = 0u64;
    // A comma list is a list of the same terms. `split(',')` yields an empty
    // term for `1,,2` or a trailing comma, and `parse_term` refuses it: a hole
    // in a list is a typo, and dropping it loses the value meant there.
    for term in field.split(',') {
        bits |= parse_term(term, min, max).map_err(|problem| CronError::Field {
            index,
            name,
            term: term.to_string(),
            problem,
        })?;
    }
    // Restricted is a property of the field string's first character, not of
    // the bitset (`*` and `1-31` are identical bitsets) and not `field != "*"`:
    // Vixie's `load_entry` sets `DOM_STAR` / `DOW_STAR` from the first
    // character, so `*/2` is starred although it names fifteen days, and
    // `Cron::day_matches` then intersects its bitset with the day of week. With
    // `field != "*"`, `0 0 */2 * 1` fired on 17 days where its author wrote 2.
    Ok((bits, !field.starts_with('*')))
}

/// Parse one term of a field: `*`, `*/n`, `a-b` or a literal.
///
/// `a-b/n` is not accepted: the grammar states five forms, and a sixth parsed
/// silently is how behaviour and contract drift. `1-30/5` gets an error naming
/// the field.
fn parse_term(term: &str, min: u32, max: u32) -> Result<u64, FieldProblem> {
    // A leading `+` is not a cron number: Rust's `FromStr` accepts `+5`, but
    // Vixie refuses it, and "it parsed" is what an adopter reads as "it is the
    // expression I wrote".
    if term.contains('+') {
        return Err(FieldProblem::Unrecognised);
    }
    if term == "*" {
        return Ok(mask(min, max));
    }
    if let Some(step) = term.strip_prefix("*/") {
        let n: u64 = step.parse().map_err(|_| FieldProblem::Unrecognised)?;
        if n == 0 {
            return Err(FieldProblem::ZeroStep);
        }
        let mut bits = 0u64;
        let mut v = u64::from(min);
        while v <= u64::from(max) {
            bits |= 1u64 << v;
            v += n;
        }
        return Ok(bits);
    }
    if let Some((from, to)) = term.split_once('-') {
        let from: u64 = from.parse().map_err(|_| FieldProblem::Unrecognised)?;
        let to: u64 = to.parse().map_err(|_| FieldProblem::Unrecognised)?;
        for value in [from, to] {
            check_range(value, min, max)?;
        }
        if from > to {
            return Err(FieldProblem::DescendingRange { from, to });
        }
        let mut bits = 0u64;
        for v in from..=to {
            bits |= 1u64 << v;
        }
        return Ok(bits);
    }
    let value: u64 = term.parse().map_err(|_| FieldProblem::Unrecognised)?;
    check_range(value, min, max)?;
    Ok(1u64 << value)
}

/// `value` must be inside `min..=max`.
fn check_range(value: u64, min: u32, max: u32) -> Result<(), FieldProblem> {
    if value < u64::from(min) || value > u64::from(max) {
        return Err(FieldProblem::OutOfRange { value, min, max });
    }
    Ok(())
}

/// The five-field expansion of one of the three accepted `@` aliases, or
/// `None` for anything else — including an `@` form this parser refuses.
///
/// One alias table: `cadence::presets` recognises `@hourly`, `@daily` and
/// `@weekly` as presets (D1 §4.2) by reading what [`AT_FORMS`] expands them
/// to, so a new alias needs no second table. `None` for an unknown `@` form
/// rather than an error: the caller's next step is [`Cron::parse`], which
/// refuses it by name ([`CronError::UnknownAtForm`]).
#[must_use]
pub fn expand_alias(expr: &str) -> Option<&'static str> {
    let trimmed = expr.trim();
    AT_FORMS
        .iter()
        .find(|(form, _)| *form == trimmed)
        .map(|(_, expansion)| *expansion)
}

/// The bitset with every value in `min..=max` set.
fn mask(min: u32, max: u32) -> u64 {
    let mut bits = 0u64;
    for v in min..=max {
        bits |= 1u64 << v;
    }
    bits
}

/// The slot string that goes into a Kubernetes object **name**: UTC,
/// lowercase, DNS-1123 subdomain-safe.
///
/// `20260907-140500`, never `20260907T140500Z` — see the module header for why
/// the ISO-8601 basic form cannot be an object name and where it is still used.
/// `%Y%m%d-%H%M%S` produces digits and one hyphen and nothing else, so the
/// result is lowercase by construction rather than by a `to_lowercase()` call
/// that would hide a future format change.
#[must_use]
pub fn slot_name(t: DateTime<Utc>) -> String {
    t.format("%Y%m%d-%H%M%S").to_string()
}

/// The fixed prefix of every scheduled `Backup`'s name (spec §3.2).
pub const SCHEDULED_BACKUP_PREFIX: &str = "logweir-backup-";

/// The cap [`scheduled_backup_name`] refuses past.
///
/// Sixty-three is a label limit, not a name limit: an object name may be 253
/// characters, but the runner Job's pods carry `batch.kubernetes.io/job-name`,
/// a label whose value derives from this name and may be 63. A longer name
/// gives pods no selector can find, and an exit code nobody can read; minting
/// time is the only moment anything can be done about it.
pub const NAME_LIMIT: usize = 63;

/// Why a name could not be minted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SlotError {
    /// The composed name would exceed [`NAME_LIMIT`].
    NameTooLong {
        /// The limit, named so an operator does not have to look it up.
        limit: usize,
        /// The length the name would have had.
        got: usize,
    },
}

impl fmt::Display for SlotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NameTooLong { limit, got } => write!(
                f,
                "a scheduled Backup's name would be {got} characters, past the {limit}-character \
                 limit on a pod's batch.kubernetes.io/job-name label value; shorten the \
                 BackupSchedule's name by {} characters",
                got.saturating_sub(*limit)
            ),
        }
    }
}

impl std::error::Error for SlotError {}

/// `logweir-backup-<schedule>-<slot>`.
///
/// Refuses a schedule name that would push the result past
/// [`NAME_LIMIT`] characters, naming the limit — see [`NAME_LIMIT`] for why 63
/// and not 253.
///
/// A pure function of its two arguments: `slot` is computed once by the caller
/// from [`Cron::last_fire_at_or_before`].
///
/// # Errors
///
/// [`SlotError::NameTooLong`] when the composed name exceeds [`NAME_LIMIT`].
pub fn scheduled_backup_name(schedule: &str, slot: &str) -> Result<String, SlotError> {
    scheduled_backup_name_for_attempt(schedule, slot, 0)
}

/// The number of characters `-r<k>` adds to a retry's name (D1 §3.1 rule 6).
///
/// Three: `k` is `1..=`[`MAX_RETRIES`], one decimal digit, so the suffix is a
/// hyphen, an `r` and that digit. [`max_schedule_name_len`] turns this into the
/// 32-character budget without retries and 29 with them.
pub const RETRY_SUFFIX_LEN: usize = 3;

/// The highest `spec.retry.maxRetries` this product accepts (D1 §4.1), and
/// therefore the highest attempt number a slot's chain can reach.
///
/// A two-digit `k` would add a fourth character to every retry name and move
/// the budget under schedule names installs already use, so raising this cap
/// is a naming decision as well as a policy one.
pub const MAX_RETRIES: u32 = 3;

/// The number of characters [`slot_name`] produces.
///
/// Asserted, not assumed: [`max_schedule_name_len`] is a `const fn` and cannot
/// call `slot_name`, so
/// `tests/cadence.rs::the_slot_string_is_the_length_the_budget_assumes` walks
/// real instants across a leap day, a year boundary and both DST directions.
pub const SLOT_NAME_LEN: usize = 15;

/// The longest `BackupSchedule` name whose runs still fit [`NAME_LIMIT`].
///
/// **32 without retries, 29 with them** (D1 §3.1 rule 6): the prefix is 15
/// characters, the separator 1 and the slot [`SLOT_NAME_LEN`], which leaves 32
/// of the 63; enabling retries spends [`RETRY_SUFFIX_LEN`] of those.
///
/// A function rather than a documented number: a schedule whose name fits
/// attempt 0 but not `-r1` would admit a slot and then fail to mint the retry,
/// so D1 §4.5 step 0 makes it an invalid policy (`Ready=False`
/// `reason=NameTooLong`), decided by [`retry_names_fit`].
#[must_use]
pub const fn max_schedule_name_len(with_retries: bool) -> usize {
    let fixed = SCHEDULED_BACKUP_PREFIX.len() + 1 + SLOT_NAME_LEN;
    let suffix = if with_retries { RETRY_SUFFIX_LEN } else { 0 };
    NAME_LIMIT - fixed - suffix
}

/// `logweir-backup-<schedule>-<slot>` for attempt 0, and
/// `logweir-backup-<schedule>-<slot>-r<k>` for retry `k` (D1 §3.1).
///
/// Attempt 0 is byte for byte [`scheduled_backup_name`]: one function composes
/// both, so a retry cannot be spelled differently from its attempt. The attempt
/// is a suffix rather than the slot's seconds (D1 §4.10): `20260907-140501`
/// would read as a real instant one second after the slot.
///
/// # Errors
///
/// [`SlotError::NameTooLong`] when the composed name exceeds [`NAME_LIMIT`] —
/// including when the *retry* is what pushes it past, which is the case
/// [`retry_names_fit`] exists to catch before a slot is ever admitted.
pub fn scheduled_backup_name_for_attempt(
    schedule: &str,
    slot: &str,
    attempt: u32,
) -> Result<String, SlotError> {
    let name = if attempt == 0 {
        format!("{SCHEDULED_BACKUP_PREFIX}{schedule}-{slot}")
    } else {
        format!("{SCHEDULED_BACKUP_PREFIX}{schedule}-{slot}-r{attempt}")
    };
    if name.len() > NAME_LIMIT {
        return Err(SlotError::NameTooLong {
            limit: NAME_LIMIT,
            got: name.len(),
        });
    }
    Ok(name)
}

/// Whether every attempt name this schedule could ever need fits
/// [`NAME_LIMIT`], given the retry policy's `max_retries`.
///
/// Checked against the deepest attempt: every retry suffix is the same length,
/// so one probe answers for the chain, and `max_retries == 0` probes attempt 0
/// alone, keeping the 32-character budget. The slot is synthetic, which is
/// sound because every slot is [`SLOT_NAME_LEN`] characters, and lets this run
/// at policy-validation time.
///
/// # Errors
///
/// [`SlotError::NameTooLong`], carrying the length the deepest attempt's name
/// would have had — the value D1 §4.5 step 0 reports as `Ready=False`
/// `reason=NameTooLong`.
pub fn retry_names_fit(schedule: &str, max_retries: u32) -> Result<(), SlotError> {
    let probe = "0".repeat(SLOT_NAME_LEN);
    scheduled_backup_name_for_attempt(schedule, &probe, max_retries)?;
    Ok(())
}

/// `<schedule uid>-<slot>` — the archive's `backup_id` for one scheduled run.
///
/// The UID, not the name: two schedules named `nightly` in two namespaces would
/// otherwise put both 02:00 runs under one archive prefix, the colliding
/// `backup_id` the module header is about.
#[must_use]
pub fn backup_id_for(schedule_uid: &str, slot: &str) -> String {
    backup_id_for_attempt(schedule_uid, slot, 0)
}

/// `<schedule uid>-<slot>` for attempt 0, `<schedule uid>-<slot>-r<k>` for
/// retry `k` — the archive's `backup_id` for one scheduled EXECUTION (D1
/// §3.1).
///
/// A retry is a new execution id, not a second write under the old one: a
/// failed attempt may have written part of an archive, and appending into that
/// prefix is the partial-archive false pass this module exists to prevent.
#[must_use]
pub fn backup_id_for_attempt(schedule_uid: &str, slot: &str, attempt: u32) -> String {
    if attempt == 0 {
        format!("{schedule_uid}-{slot}")
    } else {
        format!("{schedule_uid}-{slot}-r{attempt}")
    }
}
