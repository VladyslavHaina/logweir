//! The trigger, and the names it is a pure function of — guard **G-SLOT**.
//!
//! # The property this module exists to hold
//!
//! A scheduled backup that fires twice for one slot is not a wasted run. A
//! second `backup` into a colliding `backup_id` does **not** accumulate:
//! measured on this project's own stack, a re-run left the manifest describing
//! 2048 records while the broker held 6000 (`scripts/e2e-seed.sh:81-91`) — a
//! partial archive a later drill restores from and calls a success. That is the
//! false-pass class this repository keeps hitting, and a cron reconciler is
//! exactly where it would arrive: any design that derives the object name from
//! a reconcile-time clock, or from `status.lastFireTime`, produces **two**
//! objects when the controller crashes between the `create` and the status
//! write.
//!
//! So the name is a pure function of the trigger. Everything in this module
//! takes its inputs as arguments and reads no clock, no status and no
//! environment: [`Cron::last_fire_at_or_before`] is handed the instant,
//! [`slot_name`] is handed the fired slot, [`scheduled_backup_name`] is handed
//! the schedule name and that slot, and [`backup_id_for`] is handed the
//! schedule's UID and that slot. `Utc::now()` appears nowhere in this file. The
//! consequence is the one the reconciler relies on: a duplicate reconcile after
//! a crash computes the *same* name, so the API server answers its `POST` with
//! **409 `AlreadyExists`**, and that 409 **is** the idempotence key — no lease,
//! no lock, no `status` round trip.
//!
//! # Why the slot is `yyyymmdd-hhmmss` and not `YYYYmmddTHHMMSSZ`
//!
//! A Kubernetes object name is a DNS-1123 subdomain: lowercase alphanumerics,
//! `-` and `.`, and nothing else. The `T` and the `Z` of the ISO-8601 basic
//! form are rejected outright, so a slot spelled `20260907T140500Z` yields an
//! object the API server refuses to create. The `YYYYmmddTHHMMSSZ` form stays
//! for **Kafka topic** names (Task 9b), which permit it. Two spellings of one
//! instant, each legal where it is used, and neither one used in the other's
//! place.
//!
//! # Why the cron parser is hand-written
//!
//! Global Constraint 38 closes the workspace graph: the only packages this plan
//! adds are `kube`, `k8s-openapi` and their closure. Five integer fields, five
//! syntactic forms and three `@` aliases do not justify a dependency edge, and
//! `Cargo.toml`'s comment for this module records the decision where a reader
//! of the manifest will find it. The parser's compensating obligation is that
//! it **refuses what it does not understand, naming the field** — a cron parser
//! whose unknown token quietly becomes `*` is a parser that turns a typo into
//! "every minute of every day".

use std::fmt;

use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};

/// The maximum number of days [`Cron::last_fire_at_or_before`] and
/// [`Cron::next_fire_after`] will walk before giving up and returning `None`.
///
/// TEN YEARS, AND THE FIGURE IS NOT ARBITRARY. The sparsest expression this
/// grammar can express is `0 0 29 2 *` — 29 February — whose gap reaches
/// **eight years** across a non-leap century boundary (2096 → 2104, because
/// 2100 is not a leap year). Anything shorter would report `None` for a legal
/// expression. Anything longer buys nothing: an expression with no match inside
/// ten years has no match a scheduler should act on, and `None` is a reported
/// outcome here rather than a silent one — the reconciler writes a condition
/// for it.
///
/// PUBLIC SINCE D1 W1 BECAUSE [`crate::cadence`] WALKS THE SAME BOUND. A zoned
/// cadence enumerates LOCAL calendar dates rather than UTC ones, so it cannot
/// call the two functions below; it must reproduce their bound, and a second
/// literal `3700` in another file is how the two would come to disagree about
/// which expressions are answerable.
pub const WALK_DAYS: u32 = 3700;

/// A five-field cron expression: minute hour day-of-month month day-of-week.
///
/// Supports `*`, a literal, a comma list, an `a-b` range and a `*/n` step.
/// Anything else is a parse error naming the field — never a silent match-all.
///
/// EACH FIELD IS A BITSET, which is why matching is a shift and a mask rather
/// than a search: the widest field is 60 values, so a `u64` holds any of them
/// with room to spare, and the whole struct is `Copy`-sized.
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
    /// Whether the day-of-month field's FIRST CHARACTER was anything other
    /// than `*`.
    ///
    /// KEPT SEPARATELY FROM THE BITSET because the classic cron day rule
    /// depends on *whether the field was restricted*, not on which bits it
    /// holds: `*` and `1-31` produce identical bitsets and must behave
    /// differently. See [`Cron::day_matches`].
    ///
    /// THE FIRST CHARACTER, NOT THE WHOLE FIELD — see [`parse_field`], which
    /// is where the rule is applied and where Vixie's own line is cited.
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
/// THEY EXIST BECAUSE AN ADOPTER WILL TYPE THEM, and a `@daily` that parses as
/// nothing is a schedule that never fires. Every other `@` form — `@yearly`,
/// `@annually`, `@monthly`, `@midnight`, `@reboot` — is **refused**, naming
/// the three that are accepted: silently treating `@monthly` as `@daily` would
/// be thirty unwanted backups a month, and treating `@reboot` as anything at
/// all is meaningless for a controller that has no boot.
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
        // The `@` forms first, and by EXACT match on the whole expression: a
        // `@daily 5` is not a cron expression with an alias in it, it is a
        // mistake, and the field-count arm below is the wrong error for it.
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
    /// SECONDS ARE DROPPED, NOT ROUNDED. A cron expression fires at second 0 of
    /// a minute, so the answer for `10:00:30` is at most `10:00:00`. Truncating
    /// is what makes this function's result stable across the whole minute the
    /// reconciler might be woken in — two reconciles 40 seconds apart inside
    /// one minute compute the same `due`, which is the property G-SLOT needs.
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
    /// STRICTLY AFTER, INCLUDING WITHIN THE SAME MINUTE. `t` at `03:17:00` for
    /// `17 3 * * 1` yields the *following* Monday, not `t` itself: this is the
    /// value `status.nextFireTime` carries, and a `nextFireTime` equal to the
    /// slot just fired would read as "already due".
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
    /// ADDED BY D1 W1, AND IT IS A WINDOW AND NOT A NEW RULE. It exposes
    /// [`Cron::day_matches`] unchanged, because [`crate::cadence`] matches the
    /// **local** calendar date in a time zone and then maps the matched local
    /// minutes to UTC instants — a walk this module's own two functions cannot
    /// do, since theirs is a walk of UTC dates. Re-deriving the day rule there
    /// would put two copies of cronie's `(DOM_STAR || DOW_STAR) ? … : …` line
    /// in the tree, and the second copy is the one that drifts.
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
    /// THE CLASSIC CRON DAY RULE, AS cronie ITSELF COMPUTES IT. `find_jobs`
    /// reads
    ///
    /// ```text
    /// (DOM_STAR || DOW_STAR) ? (dom && dow) : (dom || dow)
    /// ```
    ///
    /// and that one line is transcribed below and nothing else. A field is
    /// "starred" when its FIRST CHARACTER is `*` — [`parse_field`] holds that
    /// rule and cites Vixie's line for it — so `*/2` is starred and `1-31` is
    /// not, and the `*_restricted` flags on [`Cron`] are the negation of
    /// cronie's `DOM_STAR` / `DOW_STAR`.
    ///
    /// SO THERE ARE TWO ANSWERS, NOT FOUR:
    ///
    /// * **Neither** day field is starred — both were written narrowly — and
    ///   the day is their UNION. `0 0 1 * 1` is "the first of the month *or*
    ///   every Monday", and `0 0 1-31 * 1` is every day of the month, because
    ///   `1-31` is a narrow field that happens to name them all. This arm is
    ///   the only reason the `*_restricted` flags exist: `*` and `1-31` are
    ///   the same bitset and must not be the same rule.
    /// * **Either** is starred and the day is their INTERSECTION — because a
    ///   starred field still restricts when it carries a step. `0 0 */2 * 1`
    ///   is the odd-numbered Mondays (2 days in September 2026, the 7th and
    ///   the 21st), `0 0 */7 * 1` is no day at all of that month, `0 0 * * 1`
    ///   is the four Mondays (intersecting with an all-ones day-of-month
    ///   leaves the Mondays alone) and `0 0 * * *` is every day.
    ///
    /// THE REJECTED READING, NAMED HERE SO IT IS NOT RE-DERIVED. Fix round 1
    /// let a field that was starred *and* narrowed stand aside, giving the
    /// other field the decision alone: `0 0 */2 * 1` became "every Monday" (4
    /// days) and `0 0 */7 * 1` the same 4. It is a defensible reading of
    /// "starred means unrestricted" and it is NOT what cronie does — the step
    /// is parsed into the bitset either way, and cronie intersects that bitset
    /// in. This repository's ruling on review finding HIGH-3 is cronie-exact,
    /// so a crontab an adopter migrates fires here on the days it fired there.
    /// `the_day_rule_reads_the_first_character_of_the_field` counts the eight
    /// expressions that pin the rule, two of which separate the readings.
    fn day_matches(&self, date: NaiveDate) -> bool {
        if !bit(self.month, date.month()) {
            return false;
        }
        let dom = bit(self.day_of_month, date.day());
        let dow = bit(self.day_of_week, date.weekday().num_days_from_sunday());
        // `!dom_restricted || !dow_restricted` IS cronie's `DOM_STAR ||
        // DOW_STAR`, written the way this struct stores the flags; the `else`
        // here is that condition's true branch.
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
    // A COMMA LIST IS A LIST OF THE SAME FOUR TERMS, parsed by one function.
    // `split(',')` yields an empty term for `1,,2` and for a trailing comma,
    // and `parse_term` refuses an empty term rather than skipping it: a list
    // with a hole in it is a typo, and dropping the hole silently loses
    // whichever value the author meant to put there.
    for term in field.split(',') {
        bits |= parse_term(term, min, max).map_err(|problem| CronError::Field {
            index,
            name,
            term: term.to_string(),
            problem,
        })?;
    }
    // RESTRICTED IS A PROPERTY OF THE FIELD **STRING**, NOT OF THE BITSET, and
    // the difference is the whole reason this flag exists: `*` and `1-31`
    // produce identical bitsets and must behave differently under the day rule
    // (see `Cron::day_matches`). Deriving the flag from the bits would make
    // `0 0 1-31 * 1` mean "Mondays only" instead of "every day", which is not
    // what any other cron says.
    //
    // AND IT IS A PROPERTY OF THE FIELD'S **FIRST CHARACTER**, WHICH IS NOT
    // THE SAME AS `field != "*"` (fix round 1, review finding HIGH-3). Vixie's
    // `load_entry` peeks the first character of the day-of-month and
    // day-of-week fields and sets `DOM_STAR` / `DOW_STAR` from THAT, before it
    // parses the list; so `*/2` in day-of-month is STARRED there even though
    // it names only fifteen of the month's days. STARRED IS NOT
    // "MATCHES EVERYTHING": the step is in the bitset either way, and
    // `Cron::day_matches` intersects that bitset with the day-of-week exactly
    // as cronie does, which makes `0 0 */2 * 1` the odd-numbered Mondays — 2
    // days in September 2026. Measured there before the fix, when `restricted`
    // was `field != "*"` and the union arm engaged instead: `0 0 */2 * 1`
    // fired on 17 days, `0 0 */7 * 1` on 9 and `0 0 */1 * 1` on 30. A schedule
    // that fires on 17 days where its author wrote 2 is eight times the broker
    // read they budgeted for.
    Ok((bits, !field.starts_with('*')))
}

/// Parse one term of a field: `*`, `*/n`, `a-b` or a literal.
///
/// `a-b/n` is DELIBERATELY NOT ACCEPTED. The five forms this grammar states are
/// the five forms it parses; a stepped range is a sixth, and accepting it here
/// while the documentation lists five is how a parser's behaviour and its
/// contract drift apart. An adopter who writes `1-30/5` gets an error naming
/// their field, not a schedule that fires on a set they did not choose.
fn parse_term(term: &str, min: u32, max: u32) -> Result<u64, FieldProblem> {
    // A LEADING `+` IS NOT A CRON NUMBER (fix round 1, review finding LOW-1).
    // Rust's integer `FromStr` accepts one, so `+5`, `*/+5`, `+0-+5` and `+1`
    // all parsed here and each happened to mean the obvious thing — while
    // Vixie refuses all four. A parser whose grammar is five forms must refuse
    // a sixth spelling of one of them rather than accept it by accident of the
    // standard library, because "it parsed" is what an adopter reads as "it is
    // the expression I wrote".
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
/// ADDED BY D1 W1 SO THERE IS STILL ONE ALIAS TABLE. `cadence::presets`
/// recognises a saved `schedule` string as a preset by reading its five
/// canonical fields, and D1 §4.2 rules that `@hourly`, `@daily` and `@weekly`
/// map to `hourly{0}`, `daily{0,0}` and `weekly{0,0,0}`. That mapping is not a
/// second decision: it is what [`AT_FORMS`] already says these three expand
/// to, read through the matcher the other expressions go through. A
/// hand-written `match` over the three strings in the preset module would be a
/// second table, and a fourth alias added to [`AT_FORMS`] would then silently
/// stop being recognisable as a preset.
///
/// `None` FOR AN UNKNOWN `@` FORM RATHER THAN AN ERROR: the caller's next step
/// is [`Cron::parse`], which refuses it by name
/// ([`CronError::UnknownAtForm`]), and two refusals of the same string from
/// two functions is one refusal too many.
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
/// SIXTY-THREE, AND IT IS A LABEL LIMIT AND NOT A NAME LIMIT. A Kubernetes
/// object *name* may be 253 characters; a **label value** may be 63. The
/// runner Job's pods carry `batch.kubernetes.io/job-name` as a label whose
/// value is derived from this object's name, so a name longer than 63
/// characters yields pods that cannot be labelled — and therefore pods Task
/// 17's reconciler could never find by label selector, and an exit code it
/// could never read. The refusal is here, at name-minting time, because that is
/// the only moment at which anything can still be done about it.
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
/// A PURE FUNCTION OF ITS TWO ARGUMENTS. It reads no clock: `slot` is the
/// caller's, computed once from [`Cron::last_fire_at_or_before`], and passing
/// it in rather than deriving it here is what makes "the name is a pure
/// function of the trigger" checkable at this signature.
///
/// # Errors
///
/// [`SlotError::NameTooLong`] when the composed name exceeds [`NAME_LIMIT`].
pub fn scheduled_backup_name(schedule: &str, slot: &str) -> Result<String, SlotError> {
    scheduled_backup_name_for_attempt(schedule, slot, 0)
}

/// The number of characters `-r<k>` adds to a retry's name (D1 §3.1 rule 6).
///
/// THREE, NOT FOUR, and the difference is the whole schedule-name budget. `k`
/// is `1..=`[`MAX_RETRIES`] — one decimal digit, always — so the suffix is a
/// hyphen, an `r` and that digit. [`max_schedule_name_len`] turns this into the
/// 32-character budget for a run without retries and the 29-character budget
/// for a schedule that enables them.
pub const RETRY_SUFFIX_LEN: usize = 3;

/// The highest `spec.retry.maxRetries` this product accepts (D1 §4.1), and
/// therefore the highest attempt number a slot's chain can reach.
///
/// THREE IS ALSO WHY [`RETRY_SUFFIX_LEN`] IS THREE: a two-digit `k` would add a
/// fourth character to every retry name and move the budget under any schedule
/// name an existing install already uses. Raising this cap is therefore a name
/// decision as well as a policy one.
pub const MAX_RETRIES: u32 = 3;

/// The number of characters [`slot_name`] produces.
///
/// FIFTEEN — `yyyymmdd` + `-` + `hhmmss` — AND IT IS ASSERTED, NOT ASSUMED.
/// [`max_schedule_name_len`] is a `const fn` and cannot call `slot_name`, so
/// `tests/cadence.rs::the_slot_string_is_the_length_the_budget_assumes` walks
/// real instants across a leap day, a year boundary and both DST directions
/// and fails if any of them is not this many characters.
pub const SLOT_NAME_LEN: usize = 15;

/// The longest `BackupSchedule` name whose runs still fit [`NAME_LIMIT`].
///
/// **32 without retries, 29 with them** (D1 §3.1 rule 6): the prefix is 15
/// characters, the separator 1 and the slot [`SLOT_NAME_LEN`], which leaves 32
/// of the 63; enabling retries spends [`RETRY_SUFFIX_LEN`] of those.
///
/// WHY A FUNCTION AND NOT A DOCUMENTED NUMBER. A schedule whose name fits
/// attempt 0 but not `-r1` would admit its slot, fail, and then be unable to
/// mint the retry the user asked for — a policy that silently does not apply.
/// D1 §4.5 step 0 makes that an INVALID POLICY (`Ready=False`
/// `reason=NameTooLong`) rather than a run without retries, and
/// [`retry_names_fit`] is the check that decides it.
#[must_use]
pub const fn max_schedule_name_len(with_retries: bool) -> usize {
    let fixed = SCHEDULED_BACKUP_PREFIX.len() + 1 + SLOT_NAME_LEN;
    let suffix = if with_retries { RETRY_SUFFIX_LEN } else { 0 };
    NAME_LIMIT - fixed - suffix
}

/// `logweir-backup-<schedule>-<slot>` for attempt 0, and
/// `logweir-backup-<schedule>-<slot>-r<k>` for retry `k` (D1 §3.1).
///
/// ATTEMPT 0 IS BYTE-FOR-BYTE [`scheduled_backup_name`], which is now a
/// one-line call into this function: the retry names are the same names with a
/// suffix, and two functions composing one string is how a retry comes to be
/// spelled differently from the attempt it retries.
///
/// THE ATTEMPT IS A SUFFIX AND NOT SECONDS IN THE SLOT (D1 §4.10, rejected
/// alternative). Encoding `k` in the slot's seconds digits would keep the name
/// at 32 characters and make `20260907-140501` read as a real instant one
/// second after the slot — to `reserved_slot`, to a label selector, and to the
/// operator reading `kubectl get backups`.
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
/// CHECKED AGAINST THE **DEEPEST** ATTEMPT, NOT THE FIRST. `max_retries` is
/// `0..=`[`MAX_RETRIES`] and every retry suffix is the same
/// [`RETRY_SUFFIX_LEN`] characters, so the deepest name is the longest and one
/// probe answers for the whole chain. `max_retries == 0` probes attempt 0
/// alone — a schedule that has not enabled retries keeps the 32-character
/// budget it has always had, and this function must not shrink it.
///
/// THE SLOT IS A SYNTHETIC ONE, AND THAT IS SOUND BECAUSE EVERY SLOT IS THE
/// SAME LENGTH ([`SLOT_NAME_LEN`], asserted by a test that walks real
/// instants). A check that needed the *actual* next slot could not run at
/// policy-validation time, which is the only moment at which the answer is
/// still useful.
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
/// THE UID AND NOT THE NAME. Two `BackupSchedule` objects named `nightly` in
/// two namespaces are two different schedules, and a `backup_id` built from
/// their names would put both of their 02:00 runs under one archive prefix —
/// the exact colliding-`backup_id` case that does not accumulate and leaves a
/// partial archive behind (see the module header). A UID is unique per object
/// per cluster, so the collision cannot be constructed.
#[must_use]
pub fn backup_id_for(schedule_uid: &str, slot: &str) -> String {
    backup_id_for_attempt(schedule_uid, slot, 0)
}

/// `<schedule uid>-<slot>` for attempt 0, `<schedule uid>-<slot>-r<k>` for
/// retry `k` — the archive's `backup_id` for one scheduled EXECUTION (D1
/// §3.1).
///
/// A RETRY IS A NEW EXECUTION ID AND NOT A SECOND WRITE UNDER THE OLD ONE.
/// That is the module header's whole argument, restated for the retry case
/// (D1 §4.6): a failed attempt may have written part of an archive, and a
/// retry that reused its `backup_id` would append into that partial prefix —
/// the manifest-says-2048-broker-holds-6000 false pass this module exists to
/// prevent. The suffix is what keeps attempt `k` and attempt `k+1` in two
/// prefixes, so a later drill restores from one complete archive or from
/// none.
#[must_use]
pub fn backup_id_for_attempt(schedule_uid: &str, slot: &str, attempt: u32) -> String {
    if attempt == 0 {
        format!("{schedule_uid}-{slot}")
    } else {
        format!("{schedule_uid}-{slot}-r{attempt}")
    }
}
