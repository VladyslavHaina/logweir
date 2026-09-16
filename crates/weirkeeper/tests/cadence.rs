//! The cadence engine — decision D1 §4 (PLAT-04.2), worker W1.
//!
//! # What these tests are for
//!
//! A scheduling bug is invisible until the day it costs a recovery point. The
//! cases below are the ones a reviewer cannot check by reading: what happens on
//! the two nights a year the local clock is not a function of UTC, what happens
//! when the controller was down for a week, and what happens to a name when a
//! retry suffix is appended to it.
//!
//! **Every DST case carries a negative control.** A test that asserts "the
//! Berlin 02:30 schedule fires on 2027-03-28" passes for an implementation that
//! fires at the wrong instant; the tests here assert the exact UTC instant AND
//! assert what a dropped rule would have produced instead, with the mutant
//! named in the message. `the_gap_rule_has_a_mutant` and
//! `the_deadline_boundary_has_a_mutant` are the two that exist only to fail if
//! the rule they name is removed.
//!
//! # The table
//!
//! `DST_CASES` is D1 §4.3's worked-example table, transcribed. Two hemispheres
//! (Europe/Berlin and Australia/Lord_Howe, whose transitions are in opposite
//! months and whose DST step is 30 minutes, not 60), a zone with a historical
//! offset change that has nothing to do with DST (Asia/Kathmandu moved from
//! +05:30 to +05:45 on 1986-01-01), and the one zone that skipped a whole
//! calendar day (Pacific/Apia, 2011-12-30).

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, TimeZone, Utc};
use weirkeeper::cadence::presets::{self, Preset};
use weirkeeper::cadence::{
    admit_retry, admit_slot, is_retryable, Adjustment, Cadence, CadenceError, CatchUpPolicy,
    MissedReason, RetryAdmission, RetryPolicy, RetryPolicyProblem, SlotAdmission, TerminalOutcome,
    Zone, DEFAULT_RETRY_DELAY_SECONDS, DEFAULT_STARTING_DEADLINE_SECONDS, MAX_PREVIEW_COUNT,
    MAX_SKIPPED_SLOT_ENUMERATION, RETRYABLE_TERMINAL_STATES, STATUS_NEXT_RUNS, TZDB_SOURCE,
};
use weirkeeper::conditions::TERMINAL_STATES;
use weirkeeper::slot::{
    backup_id_for, backup_id_for_attempt, max_schedule_name_len, retry_names_fit,
    scheduled_backup_name, scheduled_backup_name_for_attempt, slot_name, Cron, SlotError,
    MAX_RETRIES, NAME_LIMIT, SLOT_NAME_LEN,
};

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// The workspace root: this crate's manifest directory is `crates/weirkeeper`.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/weirkeeper sits two levels under the workspace root")
        .to_path_buf()
}

fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, mo, d, h, mi, 0)
        .single()
        .expect("a real UTC instant")
}

/// A deterministic 64-bit generator for the property tests.
///
/// HAND-ROLLED RATHER THAN A `rand` DEPENDENCY, for the reason `slot.rs`'s own
/// header gives about the cron parser: Global Constraint 38 closes the
/// workspace graph, and a test that needs 10 000 arbitrary-but-reproducible
/// pairs needs a sequence, not randomness. The seed is fixed, so a failure
/// reproduces exactly.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*, whose only property this test needs is that it does not
        // repeat inside 10 000 draws.
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

// ---------------------------------------------------------------------------
// 1. The absent field is today's behaviour
// ---------------------------------------------------------------------------

/// **An absent `spec.timeZone` reproduces today's UTC slots, byte for byte.**
///
/// THE 10 000-CASE PROPERTY D1 §4.9 REQUIRES, and it is an equality against the
/// pre-D1 evaluator itself — `Cron::last_fire_at_or_before` and
/// `Cron::next_fire_after`, the two functions every existing `BackupSchedule`
/// has been scheduled by — not against a table of expected values a new
/// implementation could have been written to satisfy.
///
/// The expressions are drawn over the whole grammar (literals, lists, ranges,
/// steps, the three `@` aliases) and the instants over a century, because the
/// interesting disagreements would be at month ends, leap days and the
/// day-rule's union/intersection boundary rather than in the middle of an
/// ordinary Tuesday.
///
/// KILLS: any re-implementation of the UTC path. `Zone::Utc` must DELEGATE.
#[test]
fn absent_timezone_is_byte_identical_to_legacy_utc() {
    let mut rng = Rng(0x5eed_1234_abcd_0001);
    let mut checked = 0usize;
    let mut fired = 0usize;
    for _ in 0..10_000 {
        let expr = random_expression(&mut rng);
        let Ok(cron) = Cron::parse(&expr) else {
            panic!("the generator must only emit expressions this grammar accepts: {expr:?}");
        };
        let cadence = Cadence::parse(&expr, None).expect("absent zone always resolves");
        // A century of instants, to the minute.
        let t = utc(1970, 1, 1, 0, 0) + Duration::minutes(rng.below(60 * 24 * 365 * 100) as i64);

        let legacy_last = cron.last_fire_at_or_before(t);
        let legacy_next = cron.next_fire_after(t);
        assert_eq!(
            cadence.last_fire_at_or_before(t),
            legacy_last,
            "absent timeZone must reproduce the legacy last-fire for {expr:?} at {t}"
        );
        assert_eq!(
            cadence.next_fire_after(t),
            legacy_next,
            "absent timeZone must reproduce the legacy next-fire for {expr:?} at {t}"
        );
        if let Some(due) = legacy_last {
            // The NAME, not only the instant: the slot string is what reaches
            // the API server, and it must be the same string it is today.
            assert_eq!(
                cadence
                    .fire_at_or_before(t)
                    .expect("the same firing, as a Fire")
                    .slot(),
                slot_name(due),
                "the slot string must be unchanged for {expr:?} at {t}"
            );
            fired += 1;
        }
        checked += 1;
    }
    assert_eq!(checked, 10_000);
    // A property test whose generator produced nothing that ever fires is a
    // property test of `None == None`.
    assert!(
        fired > 9_000,
        "the generator must mostly produce expressions that fire; only {fired} of {checked} did"
    );
}

/// One expression over the whole grammar `slot.rs` accepts.
fn random_expression(rng: &mut Rng) -> String {
    match rng.below(12) {
        0 => "@hourly".to_string(),
        1 => "@daily".to_string(),
        2 => "@weekly".to_string(),
        _ => {
            let f = |rng: &mut Rng, min: u64, max: u64| -> String {
                match rng.below(5) {
                    0 => "*".to_string(),
                    1 => (min + rng.below(max - min + 1)).to_string(),
                    2 => {
                        let a = min + rng.below(max - min + 1);
                        let b = a + rng.below(max - a + 1);
                        format!("{a}-{b}")
                    }
                    3 => format!("*/{}", 1 + rng.below(max.max(1))),
                    _ => {
                        let a = min + rng.below(max - min + 1);
                        let b = min + rng.below(max - min + 1);
                        format!("{a},{b}")
                    }
                }
            };
            format!(
                "{} {} {} {} {}",
                f(rng, 0, 59),
                f(rng, 0, 23),
                f(rng, 1, 31),
                f(rng, 1, 12),
                f(rng, 0, 6)
            )
        }
    }
}

/// **The expectations the existing slot tests pin are unchanged under a
/// `Cadence` with no zone.**
///
/// THE EXACT VALUES FROM `schedule_controller.rs::cron_last_fire_is_utc_and_stable`,
/// re-asserted through the new entry point. The property test above compares
/// two implementations to each other; this one compares the new one to the
/// NUMBERS the shipped controller's tests already agree on, so both halves of
/// "unchanged" are covered.
#[test]
fn the_existing_slot_expectations_hold_through_the_cadence() {
    let c = Cadence::parse("17 3 * * 1", None).expect("the brief's expression");
    assert_eq!(
        c.last_fire_at_or_before(utc(2026, 9, 9, 10, 0)),
        Some(utc(2026, 9, 7, 3, 17))
    );
    for t in [
        utc(2026, 9, 7, 3, 17),
        utc(2026, 9, 7, 3, 18),
        utc(2026, 9, 9, 10, 0),
        utc(2026, 9, 14, 3, 16),
    ] {
        assert_eq!(
            c.last_fire_at_or_before(t),
            Some(utc(2026, 9, 7, 3, 17)),
            "the due slot is stable for every instant inside it ({t})"
        );
    }
    assert_eq!(
        c.last_fire_at_or_before(
            Utc.with_ymd_and_hms(2026, 9, 7, 3, 17, 59)
                .single()
                .unwrap()
        ),
        Some(utc(2026, 9, 7, 3, 17)),
        "seconds are dropped, not rounded"
    );
    assert_eq!(
        c.next_fire_after(utc(2026, 9, 7, 3, 17)),
        Some(utc(2026, 9, 14, 3, 17)),
        "next_fire_after is STRICTLY after"
    );
    assert_eq!(
        Cadence::parse("@daily", None)
            .unwrap()
            .next_fire_after(utc(2026, 9, 9, 10, 0)),
        Some(utc(2026, 9, 10, 0, 0))
    );
    assert_eq!(
        Cadence::parse("@hourly", None)
            .unwrap()
            .next_fire_after(utc(2026, 9, 9, 10, 30)),
        Some(utc(2026, 9, 9, 11, 0))
    );
    assert_eq!(
        Cadence::parse("@weekly", None)
            .unwrap()
            .last_fire_at_or_before(utc(2026, 9, 9, 10, 0)),
        Some(utc(2026, 9, 6, 0, 0))
    );
}

/// **An explicit `timeZone: UTC` agrees with the absent field.**
///
/// THE TWO PATHS ARE DIFFERENT CODE and this is the test that says so out loud:
/// `Zone::Utc` delegates to `slot.rs`, `Zone::Named(Tz::UTC)` takes the
/// local-date walk with its `from_local_datetime` calls and its edge slack. A
/// user who writes the zone out must not get different slots from a user who
/// leaves it off.
#[test]
fn explicit_utc_agrees_with_the_absent_field() {
    for expr in ["*/15 * * * *", "30 2 * * *", "17 3 * * 1", "0 3 31 * *"] {
        let absent = Cadence::parse(expr, None).unwrap();
        let explicit = Cadence::parse(expr, Some("UTC")).unwrap();
        let mut t = utc(2026, 1, 1, 0, 0);
        for _ in 0..200 {
            let a = absent.next_fire_after(t);
            assert_eq!(explicit.next_fire_after(t), a, "{expr} at {t}");
            assert_eq!(
                explicit.last_fire_at_or_before(t),
                absent.last_fire_at_or_before(t),
                "{expr} at {t}"
            );
            let Some(next) = a else { break };
            t = next;
        }
    }
    assert_eq!(Zone::resolve(None).unwrap().name(), "UTC");
    assert_eq!(Zone::resolve(Some("UTC")).unwrap().name(), "UTC");

    // AND THE WHOLE PREVIEW AGREES, not only one firing at a time. The two
    // branches reach a multi-entry preview by different routes — the absent
    // field iterates `Cron::next_fire_after`, the named zone makes ONE pass
    // over local dates — and a sparse expression is where they came apart:
    // iterating restarts the ten-year walk at every firing, so `0 0 29 2 *`
    // would report twenty February 29ths reaching into the 2160s while the
    // zoned pass reported the one inside the walk bound. `horizon_date` is the
    // shared bound that closed it, and this is the case that fails without it.
    for expr in [
        "*/15 * * * *",
        "30 2 * * *",
        "17 3 * * 1",
        "0 3 31 * *",
        "0 0 29 2 *",
    ] {
        let absent = Cadence::parse(expr, None).unwrap();
        let explicit = Cadence::parse(expr, Some("UTC")).unwrap();
        for after in [utc(2026, 1, 1, 0, 0), utc(2096, 3, 1, 0, 0)] {
            assert_eq!(
                explicit.next_runs(after, 20),
                absent.next_runs(after, 20),
                "{expr}: the preview from {after} must not depend on whether the zone was \
                 written out"
            );
        }
    }

    // The sparse case really is the short one — a test that compared two empty
    // vectors would pass for an implementation that previewed nothing at all.
    assert_eq!(
        Cadence::parse("0 0 29 2 *", None)
            .unwrap()
            .next_runs(utc(2096, 3, 1, 0, 0), 20)
            .len(),
        1,
        "2100 is not a leap year, so ONE February 29th falls inside the walk bound"
    );
}

// ---------------------------------------------------------------------------
// 2. Time zones and DST
// ---------------------------------------------------------------------------

/// One row of D1 §4.3's worked-example table.
struct DstCase {
    /// What the row is about, quoted in every failure.
    what: &'static str,
    zone: &'static str,
    expr: &'static str,
    /// The instant the walk starts from.
    after: DateTime<Utc>,
    /// The slot strings, in order.
    slots: &'static [&'static str],
    /// The marker each slot carries.
    markers: &'static [Option<Adjustment>],
}

fn dst_cases() -> Vec<DstCase> {
    vec![
        DstCase {
            what: "Europe/Berlin, fixed 02:30, the repeated hour of 2026-10-25: BOTH occurrences",
            zone: "Europe/Berlin",
            expr: "30 2 * * *",
            after: utc(2026, 10, 24, 12, 0),
            slots: &["20261025-003000", "20261025-013000", "20261026-013000"],
            markers: &[
                Some(Adjustment::RepeatedLocalTimeFirst),
                Some(Adjustment::RepeatedLocalTimeSecond),
                None,
            ],
        },
        DstCase {
            what: "Europe/Berlin, fixed 02:30, the gap of 2027-03-28: ONCE, at the gap's end",
            zone: "Europe/Berlin",
            expr: "30 2 * * *",
            after: utc(2027, 3, 27, 12, 0),
            slots: &["20270328-010000", "20270329-003000"],
            markers: &[Some(Adjustment::NonexistentLocalTimeShifted), None],
        },
        DstCase {
            what: "Europe/Berlin, */15 through the gap: a continuous UTC cadence, no burst",
            zone: "Europe/Berlin",
            expr: "*/15 * * * *",
            after: utc(2027, 3, 28, 0, 30),
            slots: &[
                "20270328-004500",
                "20270328-010000",
                "20270328-011500",
                "20270328-013000",
            ],
            markers: &[None, None, None, None],
        },
        DstCase {
            what: "Europe/Berlin, */15 through the repeated hour: still a continuous UTC cadence",
            zone: "Europe/Berlin",
            expr: "*/15 * * * *",
            after: utc(2026, 10, 24, 23, 30),
            slots: &[
                "20261024-234500",
                "20261025-000000",
                "20261025-001500",
                "20261025-003000",
                "20261025-004500",
                "20261025-010000",
            ],
            markers: &[
                None,
                Some(Adjustment::RepeatedLocalTimeFirst),
                Some(Adjustment::RepeatedLocalTimeFirst),
                Some(Adjustment::RepeatedLocalTimeFirst),
                Some(Adjustment::RepeatedLocalTimeFirst),
                Some(Adjustment::RepeatedLocalTimeSecond),
            ],
        },
        DstCase {
            what: "America/New_York, fixed 01:30, the repeated hour of 2026-11-01",
            zone: "America/New_York",
            expr: "30 1 * * *",
            after: utc(2026, 10, 31, 12, 0),
            slots: &["20261101-053000", "20261101-063000", "20261102-063000"],
            markers: &[
                Some(Adjustment::RepeatedLocalTimeFirst),
                Some(Adjustment::RepeatedLocalTimeSecond),
                None,
            ],
        },
        DstCase {
            what: "America/New_York, fixed 02:30, the gap of 2027-03-14",
            zone: "America/New_York",
            expr: "30 2 * * *",
            after: utc(2027, 3, 13, 12, 0),
            slots: &["20270314-070000", "20270315-063000"],
            markers: &[Some(Adjustment::NonexistentLocalTimeShifted), None],
        },
        DstCase {
            what: "Australia/Lord_Howe, southern hemisphere, a THIRTY-minute gap on 2026-10-04",
            zone: "Australia/Lord_Howe",
            expr: "15 2 * * *",
            after: utc(2026, 10, 2, 12, 0),
            slots: &["20261002-154500", "20261003-153000", "20261004-151500"],
            markers: &[None, Some(Adjustment::NonexistentLocalTimeShifted), None],
        },
        DstCase {
            what: "Australia/Lord_Howe, the THIRTY-minute repeated window of 2027-04-04",
            zone: "Australia/Lord_Howe",
            expr: "45 1 * * *",
            after: utc(2027, 4, 3, 0, 0),
            slots: &["20270403-144500", "20270403-151500", "20270404-151500"],
            markers: &[
                Some(Adjustment::RepeatedLocalTimeFirst),
                Some(Adjustment::RepeatedLocalTimeSecond),
                None,
            ],
        },
        DstCase {
            what: "Asia/Kathmandu, a HISTORICAL offset change with no DST at all: +05:30 became \
                   +05:45 on 1986-01-01",
            zone: "Asia/Kathmandu",
            expr: "0 9 * * *",
            after: utc(1985, 12, 30, 0, 0),
            slots: &["19851230-033000", "19851231-033000", "19860101-031500"],
            markers: &[None, None, None],
        },
        DstCase {
            what: "Asia/Kathmandu, the fixed +05:45 offset today",
            zone: "Asia/Kathmandu",
            expr: "0 9 * * *",
            after: utc(2026, 9, 14, 12, 0),
            slots: &["20260915-031500", "20260916-031500"],
            markers: &[None, None],
        },
        DstCase {
            what: "Pacific/Apia skipped the whole of 2011-12-30; every match on that local date \
                   maps to ONE instant",
            zone: "Pacific/Apia",
            expr: "0 9 * * *",
            after: utc(2011, 12, 28, 12, 0),
            slots: &[
                "20111228-190000",
                "20111229-190000",
                "20111230-100000",
                "20111230-190000",
            ],
            markers: &[
                None,
                None,
                Some(Adjustment::NonexistentLocalTimeShifted),
                None,
            ],
        },
    ]
}

/// **A fixed local time inside a spring-forward gap fires ONCE, at the end of
/// the gap** — D1 §4.3, in three zones whose gaps are 60, 60 and 30 minutes.
#[test]
fn tz_gap_fixed_time_fires_once_at_gap_end() {
    for case in dst_cases().into_iter().filter(|c| {
        c.markers
            .contains(&Some(Adjustment::NonexistentLocalTimeShifted))
    }) {
        assert_table(&case);
        // ONCE is the half a slot list alone does not assert: a gap-shifted
        // instant must appear exactly one time in the run, not once per
        // matched-but-nonexistent local minute.
        let shifted: Vec<_> = Cadence::parse(case.expr, Some(case.zone))
            .unwrap()
            .next_runs(case.after, case.slots.len())
            .into_iter()
            .filter(|r| r.adjustment == Some(Adjustment::NonexistentLocalTimeShifted))
            .map(|r| r.at)
            .collect();
        let unique: BTreeSet<_> = shifted.iter().copied().collect();
        assert_eq!(
            shifted.len(),
            unique.len(),
            "{}: a shifted instant must be deduplicated, got {shifted:?}",
            case.what
        );
    }
}

/// **A fixed local time inside a repeated hour fires at BOTH occurrences** —
/// D1 §4.3, in two hemispheres and at two DST step sizes (60 and 30 minutes).
#[test]
fn tz_overlap_fires_at_both_occurrences() {
    for case in dst_cases().into_iter().filter(|c| {
        c.markers
            .contains(&Some(Adjustment::RepeatedLocalTimeFirst))
    }) {
        assert_table(&case);
    }
}

/// **An interval schedule keeps its UTC cadence through both transitions** —
/// D1 §4.3's third and fourth rows. No burst at the fall back, no hole at the
/// spring forward.
#[test]
fn tz_interval_schedule_keeps_utc_cadence_through_both_transitions() {
    for case in dst_cases().into_iter().filter(|c| c.expr.starts_with("*/")) {
        assert_table(&case);
        // THE CADENCE ITSELF, not only the values: every consecutive pair is
        // exactly the interval apart. A burst or a hole shows up here even if
        // someone re-wrote the expected slots to match a broken implementation.
        let runs = Cadence::parse(case.expr, Some(case.zone))
            .unwrap()
            .next_runs(case.after, 40);
        for pair in runs.windows(2) {
            assert_eq!(
                pair[1].at - pair[0].at,
                Duration::minutes(15),
                "{}: {} to {} is not a 15-minute step",
                case.what,
                pair[0].at,
                pair[1].at
            );
        }
    }
}

/// **The one zone that skipped a whole calendar day.**
#[test]
fn apia_skipped_day_maps_all_matches_to_one_instant() {
    let case = dst_cases()
        .into_iter()
        .find(|c| c.zone == "Pacific/Apia")
        .expect("the table carries the Apia row");
    assert_table(&case);

    // EVERY MATCH ON THE SKIPPED DATE, not just the one the daily expression
    // makes: a `*/15` schedule matches 96 local minutes on 2011-12-30 and all
    // of them are nonexistent, so all 96 collapse onto the single instant at
    // which the date line was crossed — 2011-12-31T00:00+14:00, which IS
    // 2011-12-30T10:00Z.
    let interval = Cadence::parse("*/15 * * * *", Some("Pacific/Apia")).unwrap();
    let runs = interval.next_runs(utc(2011, 12, 29, 0, 0), 200);
    let crossing = utc(2011, 12, 30, 10, 0);
    assert_eq!(
        runs.iter().filter(|r| r.at == crossing).count(),
        1,
        "96 nonexistent local minutes must deduplicate to ONE instant, got {:?}",
        runs.iter()
            .filter(|r| r.at == crossing)
            .map(|r| r.at.to_string())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        slot_name(runs.iter().find(|r| r.at == crossing).unwrap().at),
        "20111230-100000"
    );

    // AND THE INSTANT CARRIES NO MARKER, which is the deduplication rule's
    // other half. Local 2011-12-31T00:00 is a REAL local time that `*/15`
    // matches and that maps to this same instant, so the row a user reads is
    // one they can verify on their own clock; `Adjustment::rank` prefers it
    // over the shift for exactly that reason. It is the same answer
    // `Europe/Berlin, */15 through the gap` gets in the table above, and the
    // mutant is `rank` returning the shift instead: the preview would then
    // annotate a run whose local time exists.
    assert_eq!(
        runs.iter().find(|r| r.at == crossing).unwrap().adjustment,
        None,
        "a collision with a REAL local time carries no marker"
    );
    assert_eq!(
        runs.iter()
            .filter(|r| r.adjustment == Some(Adjustment::NonexistentLocalTimeShifted))
            .count(),
        0,
        "every nonexistent minute on the skipped date landed on the real 00:00"
    );

    // THE SAME COLLISION AT MIDNIGHT, and this one is what pins
    // `MAX_OFFSET_HOURS`. `0 0 * * *` matches local 2011-12-30T00:00, which
    // does not exist and shifts to the crossing, AND local 2011-12-31T00:00,
    // which IS the crossing — so the walk has to reach the SECOND of those two
    // local dates before it may stop, or it reports the shift for an instant
    // whose local time exists. Narrowing `MAX_OFFSET_HOURS` from 26 to 12
    // stops the walk one date early and flips this marker; the instant is
    // unchanged, which is exactly why a slot-list assertion alone does not see
    // it.
    let midnight = Cadence::parse("0 0 * * *", Some("Pacific/Apia")).unwrap();
    let daily = midnight.next_runs(utc(2011, 12, 28, 12, 0), 3);
    assert_eq!(
        daily.iter().map(|r| slot_name(r.at)).collect::<Vec<_>>(),
        ["20111229-100000", "20111230-100000", "20111231-100000"],
        "one slot a day across the crossing, and the skipped local date adds none"
    );
    assert_eq!(
        daily[1].local_time, "2011-12-31T00:00:00+14:00",
        "the crossing instant reads as the local midnight that DOES exist"
    );
    // EVERY PREFIX LENGTH, because the walk may stop as soon as it holds
    // `count` firings and the marker depends on whether it looked one local
    // date further. `count == 2` is the length at which the crossing is the
    // LAST entry, and it is the one a narrower `MAX_OFFSET_HOURS` gets wrong.
    for count in 1..=4 {
        let runs = midnight.next_runs(utc(2011, 12, 28, 12, 0), count);
        assert!(
            runs.iter().all(|r| r.adjustment.is_none()),
            "MUTANT `stop the walk at a narrower offset`: with count {count} every instant here \
             is a real local midnight, so none of them is an adjustment; got {:?}",
            runs.iter().map(|r| r.adjustment).collect::<Vec<_>>()
        );
    }

    // NO BURST AND NO HOLE: the 96 collapsed minutes leave a continuous
    // 15-minute UTC cadence across the skipped day.
    for pair in runs.windows(2) {
        assert_eq!(
            pair[1].at - pair[0].at,
            Duration::minutes(15),
            "{} to {} is not a 15-minute step across the date-line crossing",
            pair[0].at,
            pair[1].at
        );
    }
}

/// Assert one table row: the slots, in order, and their markers.
fn assert_table(case: &DstCase) {
    assert_eq!(
        case.slots.len(),
        case.markers.len(),
        "{}: the table row is malformed",
        case.what
    );
    let runs = Cadence::parse(case.expr, Some(case.zone))
        .unwrap_or_else(|e| panic!("{}: {e}", case.what))
        .next_runs(case.after, case.slots.len());
    let got: Vec<String> = runs.iter().map(|r| slot_name(r.at)).collect();
    assert_eq!(got, case.slots, "{}", case.what);
    let markers: Vec<Option<Adjustment>> = runs.iter().map(|r| r.adjustment).collect();
    assert_eq!(markers, case.markers, "{}", case.what);
    // ASCENDING AND STRICT: two slots with the same instant would be one
    // object and one archive, which is the collision `slot.rs` exists to stop.
    for pair in runs.windows(2) {
        assert!(
            pair[0].at < pair[1].at,
            "{}: the preview must be strictly ascending",
            case.what
        );
    }
}

/// **The gap rule has a mutant.** THIS TEST EXISTS TO FAIL if the
/// nonexistent-local-time arm is dropped or changed.
///
/// Two implementations that D1 §4.3 rejects are named here as the values they
/// would have produced, so removing the rule cannot leave a green suite:
///
/// * **Skip the gap.** The 2027-03-28 firing disappears and the next slot is
///   2027-03-29's. Asserted absent.
/// * **Keep the wall time and use the OLD offset.** 02:30 local at +01:00 is
///   01:30 UTC — a real instant, an hour and a half after the one the rule
///   produces, and half an hour *past* a time that no longer existed.
///   Asserted absent.
#[test]
fn the_gap_rule_has_a_mutant() {
    let berlin = Cadence::parse("30 2 * * *", Some("Europe/Berlin")).unwrap();
    let got = berlin
        .fire_after(utc(2027, 3, 27, 12, 0))
        .expect("the gap day still fires");

    assert_eq!(
        got.at,
        utc(2027, 3, 28, 1, 0),
        "the gap-day firing is the END OF THE GAP, 03:00 local = 01:00 UTC"
    );
    assert_ne!(
        got.at,
        utc(2027, 3, 29, 0, 30),
        "MUTANT `skip the gap`: the 2027-03-28 firing must not be dropped in favour of the next \
         day's. A nightly backup that silently does not run on the night the clocks go forward \
         is one recovery point lost every year, and nothing in the status would say so"
    );
    assert_ne!(
        got.at,
        utc(2027, 3, 28, 1, 30),
        "MUTANT `keep the wall time at the old offset`: 02:30+01:00 is 01:30 UTC, which is half \
         an hour after the gap ended and is NOT the first real instant at or after the time the \
         user asked for"
    );
    assert_eq!(
        got.adjustment,
        Some(Adjustment::NonexistentLocalTimeShifted)
    );
}

/// **The repeated-hour rule has a mutant.** THIS TEST EXISTS TO FAIL if the
/// ambiguous arm collapses to one firing.
///
/// D1 §4.3 makes both occurrences real instants whose local wall time matches,
/// so both fire. An implementation that took `.earliest()` or `.latest()` would
/// produce ONE slot here; an implementation that produced one slot but the
/// other instant would be invisible to a test that only counted them.
#[test]
fn the_repeated_hour_rule_has_a_mutant() {
    let runs = Cadence::parse("30 2 * * *", Some("Europe/Berlin"))
        .unwrap()
        .next_runs(utc(2026, 10, 24, 12, 0), 2);
    assert_eq!(
        runs.len(),
        2,
        "MUTANT `fire once in the repeated hour`: both occurrences are real instants whose local \
         time is 02:30, and the preview shows both"
    );
    assert_eq!(runs[0].at, utc(2026, 10, 25, 0, 30));
    assert_eq!(runs[1].at, utc(2026, 10, 25, 1, 30));
    assert_eq!(
        runs[1].at - runs[0].at,
        Duration::hours(1),
        "the two occurrences are one hour apart — the size of the repeated window"
    );
    // The RENDERED local times are the same clock face at two offsets, which is
    // the only thing that lets a user tell the two rows apart.
    assert_eq!(runs[0].local_time, "2026-10-25T02:30:00+02:00");
    assert_eq!(runs[1].local_time, "2026-10-25T02:30:00+01:00");
}

/// **A shifted or repeated slot is still a DNS-1123-safe UTC name.**
///
/// The slot is the UTC instant and nothing else: 15 characters, digits and one
/// hyphen, lowercase by construction. A slot that carried a local time or an
/// offset would not be unique across a fall back — two instants, one name, one
/// object, one partial archive.
#[test]
fn slot_names_stay_utc_dns1123_for_shifted_slots() {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for case in dst_cases() {
        for run in Cadence::parse(case.expr, Some(case.zone))
            .unwrap()
            .next_runs(case.after, case.slots.len())
        {
            let slot = slot_name(run.at);
            assert_eq!(slot.len(), SLOT_NAME_LEN, "{slot} is not 15 characters");
            assert!(
                slot.bytes().all(|b| b.is_ascii_digit() || b == b'-'),
                "{slot} carries a character a DNS-1123 subdomain cannot"
            );
            assert_eq!(slot, slot.to_lowercase());
            // Same instant, same name; different instants, different names.
            let composed = scheduled_backup_name("nightly", &slot).expect("fits");
            assert!(composed.len() <= NAME_LIMIT);
            seen.insert(slot);
        }
    }
    assert!(seen.len() > 20, "the table produced {} slots", seen.len());
}

/// **An unknown zone is refused by name, and refuses nothing else.**
#[test]
fn unknown_timezone_is_refused_by_name() {
    for bad in [
        "Europe/Berlon",
        "not a zone",
        "",
        "UTC+2",
        "Europe/Berlin/Extra/Deep",
    ] {
        let err = Cadence::parse("0 2 * * *", Some(bad))
            .expect_err("an unknown zone is a refusal, not a silent UTC");
        let CadenceError::UnknownTimeZone { got } = &err else {
            panic!("{bad:?} must be refused as an unknown zone, got {err:?}");
        };
        assert_eq!(got, bad, "the refusal quotes what was written");
        let message = err.to_string();
        assert!(
            message.contains(bad) || bad.is_empty(),
            "the message names the string: {message}"
        );
        assert!(
            message.contains(TZDB_SOURCE),
            "the message names the database that has no such zone: {message}"
        );
    }
    // POSIX SIGN INVERSION IS A REAL ZONE, NOT A TYPO (D1 §4.3). `Etc/GMT+5`
    // is UTC-05:00, and the preview's rendered offset is what tells the user.
    let etc = Cadence::parse("0 9 * * *", Some("Etc/GMT+5")).expect("Etc/GMT+5 exists");
    assert_eq!(
        etc.next_runs(utc(2026, 9, 14, 0, 0), 1)[0].local_time,
        "2026-09-14T09:00:00-05:00",
        "Etc/GMT+5 is MINUS five, and the rendered offset is what says so"
    );
}

/// **An invalid cron is refused naming the field, zone or no zone.**
#[test]
fn an_invalid_cron_is_refused_naming_the_field() {
    for (expr, needle) in [
        ("0 2 * *", "five fields"),
        ("0 25 * * *", "hour"),
        ("*/0 * * * *", "step of 0"),
        ("5-1 * * * *", "range"),
        ("@monthly", "@hourly, @daily and @weekly"),
        ("0 2 * * SUN", "day-of-week"),
        ("1-30/5 * * * *", "minute"),
    ] {
        for zone in [None, Some("Europe/Berlin")] {
            let err = Cadence::parse(expr, zone)
                .expect_err("an unparseable expression is a refusal, never a match-all");
            assert!(
                matches!(err, CadenceError::Schedule(_)),
                "{expr:?} must be refused as a schedule problem, got {err:?}"
            );
            let message = err.to_string();
            assert!(
                message.contains(needle),
                "the refusal for {expr:?} must name {needle:?}; got {message}"
            );
        }
    }
    // THE EXPRESSION IS CHECKED BEFORE THE ZONE, so a schedule with two
    // mistakes reports the cron one.
    let both = Cadence::parse("0 99 * * *", Some("Europe/Berlon")).expect_err("both are wrong");
    assert!(matches!(both, CadenceError::Schedule(_)));
}

/// **Leap years and month ends, in a zone.**
///
/// `0 3 31 * *` DOES NOT RUN IN FEBRUARY, APRIL, JUNE, SEPTEMBER OR NOVEMBER,
/// and that is what a `dayOfMonth: 31` schedule means everywhere; the preset
/// catalogue caps `monthly` at 28 for exactly this reason
/// (`the_monthly_preset_stops_at_28`). `0 3 29 2 *` waits eight years across
/// 2100, which is the figure `slot.rs`'s `WALK_DAYS` is sized for.
#[test]
fn leap_year_and_month_end_expressions_answer() {
    let month_end = Cadence::parse("0 3 31 * *", Some("Europe/Berlin")).unwrap();
    let got: Vec<String> = month_end
        .next_runs(utc(2026, 1, 1, 0, 0), 7)
        .into_iter()
        .map(|r| r.local_time)
        .collect();
    assert_eq!(
        got,
        vec![
            "2026-01-31T03:00:00+01:00",
            "2026-03-31T03:00:00+02:00",
            "2026-05-31T03:00:00+02:00",
            "2026-07-31T03:00:00+02:00",
            "2026-08-31T03:00:00+02:00",
            "2026-10-31T03:00:00+01:00",
            "2026-12-31T03:00:00+01:00",
        ],
        "February, April, June, September and November have no 31st"
    );

    let leap = Cadence::parse("0 3 29 2 *", Some("Europe/Berlin")).unwrap();
    assert_eq!(
        leap.next_fire_after(utc(2026, 3, 1, 0, 0)),
        Some(utc(2028, 2, 29, 2, 0)),
        "the next 29 February after March 2026 is 2028's"
    );
    // THE EIGHT-YEAR GAP `WALK_DAYS` IS SIZED FOR: 2100 is not a leap year, so
    // 2096 → 2104. An implementation that walked a year, or five, returns None.
    assert_eq!(
        leap.next_fire_after(utc(2096, 3, 1, 0, 0)),
        Some(utc(2104, 2, 29, 2, 0)),
        "2100 is not a leap year; the walk must reach 2104"
    );
    // 29 February EXISTS; 30 February never does, and `None` is a reported
    // outcome rather than a hang.
    assert_eq!(
        Cadence::parse("0 0 30 2 *", Some("Europe/Berlin"))
            .unwrap()
            .next_fire_after(utc(2026, 1, 1, 0, 0)),
        None,
        "30 February is a legal expression with no match, and the answer is None"
    );
}

/// **Nothing in the cadence module reads a clock or the environment** — guard
/// **G-SLOT**, extended to the module D1 W1 adds.
///
/// A SOURCE-READING TEST, for the same reason
/// `schedule_controller.rs::the_name_never_reads_a_reconcile_clock_or_a_status`
/// is one: the property is about what the code CANNOT do. Every function here
/// takes its instant as an argument, so a duplicate reconcile after a crash
/// computes the same slot and the API server's 409 is the idempotence key.
#[test]
fn the_cadence_module_reads_no_clock() {
    let src = std::fs::read_to_string(workspace_root().join("crates/weirkeeper/src/cadence.rs"))
        .expect("the module is readable");
    let code: String = src
        .lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    for forbidden in [
        "Utc::now()",
        "Local::now()",
        "SystemTime::now",
        "Instant::now",
        "std::env",
        "env!(",
        "include_str!",
    ] {
        assert!(
            !code.contains(forbidden),
            "crates/weirkeeper/src/cadence.rs names {forbidden:?} in code: every input to this \
             module is an argument, which is what makes a duplicate reconcile compute the same \
             slot"
        );
    }
}

// ---------------------------------------------------------------------------
// 3. Deadlines, catch-up and downtime
// ---------------------------------------------------------------------------

/// **The starting deadline's boundary is INCLUSIVE, to the second.**
///
/// D1 §4.5 step 7 is `now - S <= startingDeadlineSeconds`. Exactly at the
/// horizon the slot is still admitted; one second later it is not.
#[test]
fn the_starting_deadline_boundary_is_inclusive() {
    let slot = utc(2026, 9, 15, 3, 0);
    let horizon = DEFAULT_STARTING_DEADLINE_SECONDS;
    let table = [
        (0, SlotAdmission::Scheduled, "the instant it came due"),
        (horizon - 1, SlotAdmission::Scheduled, "one second inside"),
        (horizon, SlotAdmission::Scheduled, "EXACTLY at the horizon"),
        (
            horizon + 1,
            SlotAdmission::Missed {
                reason: MissedReason::PastStartingDeadline,
            },
            "one second past",
        ),
    ];
    for (age, want, what) in table {
        let got = admit_slot(
            slot,
            slot + Duration::seconds(age),
            horizon,
            CatchUpPolicy::None,
            None,
        );
        assert_eq!(got, want, "{what} ({age}s after the slot)");
    }
}

/// **The deadline boundary has a mutant.** THIS TEST EXISTS TO FAIL if the
/// horizon is dropped, inverted or made exclusive.
///
/// Three implementations that would be wrong, each asserted against:
///
/// * **No horizon at all** — every old slot admitted. The 25-hour-old slot
///   below would be `Scheduled`; it must be `Missed`.
/// * **An exclusive boundary** — the slot exactly on the horizon would be
///   `Missed`; it must be `Scheduled`.
/// * **Catch-up as the default** — an absent `catchUpPolicy` would admit a
///   `CatchUp` run for the old slot; it must admit nothing.
#[test]
fn the_deadline_boundary_has_a_mutant() {
    let slot = utc(2026, 9, 15, 3, 0);
    let horizon = DEFAULT_STARTING_DEADLINE_SECONDS;

    let on_the_horizon = admit_slot(
        slot,
        slot + Duration::seconds(horizon),
        horizon,
        CatchUpPolicy::None,
        None,
    );
    assert_eq!(
        on_the_horizon,
        SlotAdmission::Scheduled,
        "MUTANT `exclusive boundary`: a slot that arrives exactly on the horizon must be \
         admitted, or whether it runs depends on which side of a millisecond the reconcile woke"
    );

    let long_past = admit_slot(
        slot,
        slot + Duration::hours(25),
        horizon,
        CatchUpPolicy::None,
        None,
    );
    assert_eq!(
        long_past,
        SlotAdmission::Missed {
            reason: MissedReason::PastStartingDeadline
        },
        "MUTANT `no horizon`: a 25-hour-old slot with catchUpPolicy None must not run. Today's \
         behaviour skips it and records it, and an absent field must keep meaning that"
    );
    assert_ne!(
        long_past,
        SlotAdmission::CatchUp,
        "MUTANT `catch-up by default`: an absent catchUpPolicy is None, not Latest"
    );
}

/// **A long downtime with `catchUpPolicy: Latest` produces exactly ONE run** —
/// and with `None`, none at all.
///
/// D1 §4.7's downtime row, exercised over a week of a schedule that fires every
/// hour: 168 slots came due while nothing was running, and the policy's whole
/// promise is that the backlog is bounded at one.
#[test]
fn a_week_of_downtime_creates_at_most_one_catch_up() {
    for zone in [None, Some("Europe/Berlin")] {
        let cadence = Cadence::parse("0 * * * *", zone).unwrap();
        let down_since = utc(2026, 9, 8, 12, 0);
        let now = utc(2026, 9, 15, 12, 30);

        // ONLY THE LATEST DUE SLOT IS EVER ELIGIBLE. Whatever the downtime was,
        // one reconcile sees one slot.
        let latest = cadence
            .latest_due_slot(now)
            .expect("an hourly schedule always has a due slot");
        assert_eq!(latest, utc(2026, 9, 15, 12, 0), "zone {zone:?}");

        // With `None`: nothing runs, and the skipped slots are counted.
        assert_eq!(
            admit_slot(
                latest,
                now,
                DEFAULT_STARTING_DEADLINE_SECONDS,
                CatchUpPolicy::None,
                None
            ),
            SlotAdmission::Scheduled,
            "the LATEST slot is 30 minutes old and inside the horizon either way"
        );
        let stale = cadence
            .last_fire_at_or_before(down_since + Duration::hours(3))
            .unwrap();
        assert_eq!(
            admit_slot(
                stale,
                now,
                DEFAULT_STARTING_DEADLINE_SECONDS,
                CatchUpPolicy::None,
                None
            ),
            SlotAdmission::Missed {
                reason: MissedReason::PastStartingDeadline
            },
            "an older slot is past the horizon and catchUpPolicy None skips it"
        );

        // With `Latest` and a slot that IS past the horizon: exactly one
        // catch-up, and the rest are counted, not run.
        let old_latest = utc(2026, 9, 15, 10, 0);
        assert_eq!(
            admit_slot(
                old_latest,
                now,
                DEFAULT_STARTING_DEADLINE_SECONDS,
                CatchUpPolicy::Latest,
                Some(down_since)
            ),
            SlotAdmission::CatchUp,
            "zone {zone:?}: the latest due slot, past its deadline, is the one catch-up"
        );
        // 2026-09-08T12:00Z to 2026-09-15T10:00Z is 166 hours, and BOTH ENDS
        // ARE EXCLUSIVE — `lastEvaluatedSlot` was accounted for last time and
        // `old_latest` is the slot being decided — so 165 lie strictly
        // between. The off-by-one is the whole reason the number is written
        // out here: counting either endpoint twice is a `missedSlots.count`
        // that drifts by one every reconcile.
        let skipped = cadence.skipped_slots(down_since, old_latest, MAX_SKIPPED_SLOT_ENUMERATION);
        assert_eq!(
            skipped.count, 165,
            "zone {zone:?}: the slots between are counted, not run"
        );
        assert!(!skipped.capped);
    }
}

/// **Catch-up never runs a slot from before the revision was observed** — D1
/// §4.7 row 19.
#[test]
fn catch_up_never_runs_a_slot_before_the_observed_revision() {
    let effective_since = utc(2026, 9, 15, 12, 0);
    let now = utc(2026, 9, 15, 18, 0);
    assert_eq!(
        admit_slot(
            utc(2026, 9, 15, 11, 0),
            now,
            DEFAULT_STARTING_DEADLINE_SECONDS,
            CatchUpPolicy::Latest,
            Some(effective_since)
        ),
        SlotAdmission::Missed {
            reason: MissedReason::BeforeRevision
        },
        "catch-up recovers downtime; it does not back-fill a policy that did not exist yet"
    );
    assert_eq!(
        admit_slot(
            effective_since,
            now,
            DEFAULT_STARTING_DEADLINE_SECONDS,
            CatchUpPolicy::Latest,
            Some(effective_since)
        ),
        SlotAdmission::CatchUp,
        "the boundary is `slot < effectiveSince`, so a slot AT the revision is caught up"
    );
    // Still inside the horizon, the revision does not matter: D1 §4.7 keeps
    // today's row 17 behaviour deliberately.
    assert_eq!(
        admit_slot(
            utc(2026, 9, 15, 11, 0),
            utc(2026, 9, 15, 11, 30),
            DEFAULT_STARTING_DEADLINE_SECONDS,
            CatchUpPolicy::Latest,
            Some(effective_since)
        ),
        SlotAdmission::Scheduled
    );
}

/// **The latest due slot supersedes the older ones** — and is stable for every
/// instant inside its own slot.
#[test]
fn latest_due_slot_supersedes_older_slots() {
    for zone in [None, Some("America/New_York")] {
        let cadence = Cadence::parse("*/30 * * * *", zone).unwrap();
        let base = cadence.latest_due_slot(utc(2026, 9, 15, 12, 5)).unwrap();
        for extra in [0, 1, 29] {
            assert_eq!(
                cadence.latest_due_slot(base + Duration::minutes(extra)),
                Some(base),
                "zone {zone:?}: the due slot is stable across its whole interval"
            );
        }
        assert_eq!(
            cadence.latest_due_slot(base + Duration::minutes(30)),
            Some(base + Duration::minutes(30)),
            "zone {zone:?}: and advances exactly when the next one comes due"
        );
    }
}

/// **The skipped-slot enumeration caps, and says that it capped.**
#[test]
fn missed_slot_accounting_caps_at_one_thousand() {
    for zone in [None, Some("Europe/Berlin")] {
        let cadence = Cadence::parse("* * * * *", zone).unwrap();
        let from = utc(2026, 9, 1, 0, 0);

        let exact = cadence.skipped_slots(from, from + Duration::minutes(11), 1000);
        assert_eq!(
            (exact.count, exact.capped),
            (10, false),
            "zone {zone:?}: ten slots strictly between, and the walk did not cap"
        );

        let capped = cadence.skipped_slots(
            from,
            from + Duration::days(30),
            MAX_SKIPPED_SLOT_ENUMERATION,
        );
        assert_eq!(
            (capped.count, capped.capped),
            (MAX_SKIPPED_SLOT_ENUMERATION, true),
            "zone {zone:?}: a month of minutely slots stops at the cap and SAYS so"
        );

        // THE CAP IS NOT A LIE WHEN THE ANSWER IS EXACTLY THE CAP: ten slots
        // with a cap of ten is `capped: false`, because there is no eleventh.
        let on_the_cap = cadence.skipped_slots(from, from + Duration::minutes(11), 10);
        assert_eq!((on_the_cap.count, on_the_cap.capped), (10, false));
        let one_under = cadence.skipped_slots(from, from + Duration::minutes(11), 9);
        assert_eq!((one_under.count, one_under.capped), (9, true));

        // BOTH ENDS EXCLUSIVE, so a slot is counted exactly once across
        // reconciles.
        let touching = cadence.skipped_slots(from, from + Duration::minutes(1), 1000);
        assert_eq!((touching.count, touching.capped), (0, false));
    }
}

// ---------------------------------------------------------------------------
// 4. Retries
// ---------------------------------------------------------------------------

/// **A retry's name and execution id carry the attempt** — D1 §3.1.
#[test]
fn retry_name_and_execution_id_carry_the_attempt() {
    let slot = "20260915-030000";
    assert_eq!(
        scheduled_backup_name_for_attempt("nightly", slot, 0).unwrap(),
        "logweir-backup-nightly-20260915-030000"
    );
    assert_eq!(
        scheduled_backup_name_for_attempt("nightly", slot, 0).unwrap(),
        scheduled_backup_name("nightly", slot).unwrap(),
        "attempt 0 is byte-for-byte the name the scheduler already mints"
    );
    for k in 1..=MAX_RETRIES {
        assert_eq!(
            scheduled_backup_name_for_attempt("nightly", slot, k).unwrap(),
            format!("logweir-backup-nightly-20260915-030000-r{k}")
        );
        assert_eq!(
            backup_id_for_attempt("uid-1234", slot, k),
            format!("uid-1234-20260915-030000-r{k}")
        );
    }
    assert_eq!(
        backup_id_for_attempt("uid-1234", slot, 0),
        backup_id_for("uid-1234", slot),
        "attempt 0's execution id is unchanged"
    );
    // EVERY ATTEMPT IS A DISTINCT EXECUTION ID. A retry that reused the failed
    // attempt's id would append into its partial archive prefix.
    let ids: BTreeSet<String> = (0..=MAX_RETRIES)
        .map(|k| backup_id_for_attempt("uid-1234", slot, k))
        .collect();
    assert_eq!(ids.len(), (MAX_RETRIES + 1) as usize);
}

/// **A retry suffix never silently produces an unusable name** — D1 §3.1
/// rule 6, and the 63-character pod-label limit `slot.rs` refuses past.
#[test]
fn retry_names_fit_the_29_character_budget() {
    assert_eq!(max_schedule_name_len(false), 32);
    assert_eq!(max_schedule_name_len(true), 29);
    let slot = "20260915-030000";

    // The boundary from both sides, without retries: 32 fits at exactly the
    // limit and 33 does not — unchanged from today.
    assert_eq!(
        scheduled_backup_name(&"n".repeat(32), slot).unwrap().len(),
        NAME_LIMIT
    );
    assert!(scheduled_backup_name(&"n".repeat(33), slot).is_err());

    // AND WITH RETRIES: a 30-character schedule name whose attempt 0 fits
    // CANNOT mint `-r1`. That is the silent failure this budget exists to
    // prevent — a retry policy that is configured, accepted and inapplicable.
    let thirty = "n".repeat(30);
    assert!(
        scheduled_backup_name_for_attempt(&thirty, slot, 0).is_ok(),
        "a 30-character schedule name fits attempt 0"
    );
    let err = scheduled_backup_name_for_attempt(&thirty, slot, 1)
        .expect_err("but its first retry does not fit");
    assert_eq!(
        err,
        SlotError::NameTooLong {
            limit: NAME_LIMIT,
            got: 64
        }
    );
    let message = err.to_string();
    for needle in ["63", "64", "batch.kubernetes.io/job-name"] {
        assert!(
            message.contains(needle),
            "the refusal names {needle}: {message}"
        );
    }

    // `retry_names_fit` is the policy-level check D1 §4.5 step 0 runs, and it
    // answers for the DEEPEST attempt.
    assert!(
        retry_names_fit(&thirty, 0).is_ok(),
        "no retries, no problem"
    );
    assert!(
        retry_names_fit(&thirty, 1).is_err(),
        "MUTANT `check only attempt 0`: a schedule whose retries cannot be named must be an \
         invalid policy, not a schedule that quietly runs without the retries it configured"
    );
    assert!(retry_names_fit(&"n".repeat(29), MAX_RETRIES).is_ok());
    assert!(retry_names_fit(&"n".repeat(30), MAX_RETRIES).is_err());
    // The deepest retry's name lands exactly on the limit at 29 characters.
    assert_eq!(
        scheduled_backup_name_for_attempt(&"n".repeat(29), slot, MAX_RETRIES)
            .unwrap()
            .len(),
        NAME_LIMIT
    );
}

/// **The slot string is the length the budget assumes** — `slot.rs`'s
/// `SLOT_NAME_LEN`, measured over real instants rather than asserted.
#[test]
fn the_slot_string_is_the_length_the_budget_assumes() {
    for t in [
        utc(2026, 1, 1, 0, 0),
        utc(2028, 2, 29, 23, 59),
        utc(2026, 12, 31, 23, 59),
        utc(2027, 3, 28, 1, 0),
        utc(2026, 10, 25, 0, 30),
        utc(2011, 12, 30, 10, 0),
        utc(1970, 1, 1, 0, 0),
    ] {
        assert_eq!(slot_name(t).len(), SLOT_NAME_LEN, "{t}");
    }
}

/// **The retryable classification is a closed match, and an unknown terminal
/// record is NOT retryable** — D1 §4.6.
///
/// # What a mutant here costs
///
/// Flipping the unrecognised arm to `true` turns every refusal this build has
/// never heard of into `1 + maxRetries` full backups of a customer's cluster,
/// spaced `delaySeconds` apart, for a decision that was never going to change.
/// That is why the test walks `conditions::TERMINAL_STATES` — the WHOLE list,
/// not a sample — and asserts that everything outside
/// `RETRYABLE_TERMINAL_STATES` is refused, so a state added to that list by a
/// later task lands on `false` and is noticed here rather than in a retry
/// storm.
#[test]
fn retry_classification_is_a_closed_match_and_unknown_is_not_retryable() {
    let with_code = |code: i32| TerminalOutcome {
        exit_code: Some(code),
        terminal_state: None,
    };
    let with_state = |state: &'static str| TerminalOutcome {
        exit_code: None,
        terminal_state: Some(state),
    };

    // EXIT 1 IS THE OPERATIONAL FAILURE: nothing was written, and a broker or
    // a network can be back before the delay elapses.
    assert!(
        is_retryable(with_code(1)),
        "exit 1 (Operational) is retryable"
    );

    // ANYTHING OUTSIDE THE CONTRACT IS A CONTAINER THAT WAS KILLED. 137 is the
    // SIGKILL `activeDeadlineSeconds` and the OOM killer both produce, 143 the
    // SIGTERM, 139 a segfault.
    for code in [137, 143, 139, 125, 255, -1] {
        assert!(
            is_retryable(with_code(code)),
            "exit {code} is outside 0..=4, so it is the runner being killed and not a decision"
        );
    }

    // THE FOUR DECISIONS ARE NEVER RETRIED. Exit 2 is the one that matters
    // most: a signed document WAS written, so a retry spends a whole broker
    // read to produce a second copy of a result the product already holds.
    for code in [0, 2, 3, 4] {
        assert!(
            !is_retryable(with_code(code)),
            "MUTANT `retry every nonzero code`: exit {code} is a decision, and a decision \
             repeated is the same decision"
        );
    }

    // NO EXIT CODE: the allowlist, and nothing else.
    for state in RETRYABLE_TERMINAL_STATES {
        assert!(
            is_retryable(with_state(state)),
            "{state} is in RETRYABLE_TERMINAL_STATES"
        );
    }
    assert_eq!(
        RETRYABLE_TERMINAL_STATES,
        &[
            "DisruptedMidDrill",
            "PodUnschedulable",
            "NoExitCode",
            "DiscoveryFailed",
        ],
        "the four spellings D1 §4.6 names; `DiscoveryFailed` is pinned here until \
         conditions.rs declares its constant"
    );

    // EVERY OTHER TERMINAL STATE THIS BUILD KNOWS OF, walked from the source
    // list so a state added later cannot quietly become retryable.
    for state in TERMINAL_STATES {
        if RETRYABLE_TERMINAL_STATES.contains(state) {
            continue;
        }
        assert!(
            !is_retryable(with_state(state)),
            "MUTANT `invert the allowlist`: {state} is a controller refusal made before any \
             POST, and waiting does not change it"
        );
    }

    // THE UNKNOWN ARM ITSELF — the one that makes this a closed match. A state
    // written by a NEWER controller and read by this one, a typo, and no state
    // at all.
    for unknown in ["AFutureTasksReason", "", "discoveryfailed", "Refused"] {
        assert!(
            !is_retryable(with_state(unknown)),
            "MUTANT `unknown means retryable`: {unknown:?} must not start a retry loop"
        );
    }
    assert!(
        !is_retryable(TerminalOutcome {
            exit_code: None,
            terminal_state: None,
        }),
        "a record with neither an exit code nor a state is not retryable"
    );

    // THE EXIT CODE WINS WHEN BOTH ARE PRESENT: a guard refusal carries exit 3
    // AND a terminal state, and the state names WHICH guard rather than
    // re-deciding whether to retry.
    assert!(!is_retryable(TerminalOutcome {
        exit_code: Some(3),
        terminal_state: Some("CredentialNotRenderable"),
    }));
    assert!(is_retryable(TerminalOutcome {
        exit_code: Some(1),
        terminal_state: Some("CredentialNotRenderable"),
    }));

    // AND THE CLASSIFICATION IS WHAT `admit_retry` CONSUMES, which is the
    // seam W2 wires: the same chain retries under exit 1 and stops under
    // exit 3.
    let slot = utc(2026, 9, 15, 3, 0);
    let finished = utc(2026, 9, 15, 3, 10);
    let policy = Some(RetryPolicy {
        max_retries: 2,
        delay_seconds: 60,
    });
    let now = finished + Duration::minutes(5);
    assert_eq!(
        admit_retry(
            slot,
            Some(slot),
            0,
            is_retryable(with_code(1)),
            finished,
            policy,
            now
        ),
        RetryAdmission::Due { attempt: 1 }
    );
    assert_eq!(
        admit_retry(
            slot,
            Some(slot),
            0,
            is_retryable(with_code(3)),
            finished,
            policy,
            now
        ),
        RetryAdmission::NotRetryable
    );
}

/// **Retry admission: exhaustion, the delay, and "never past the next due
/// slot".**
///
/// The table is D1 §4.7 rows 10–13 and 22, evaluated top to bottom. The
/// supersession row is FIRST because the scheduler picks the latest due slot
/// before it looks at any attempt chain.
#[test]
fn retry_admission_follows_the_truth_table() {
    let slot = utc(2026, 9, 15, 3, 0);
    let finished = utc(2026, 9, 15, 3, 10);
    let policy = RetryPolicy {
        max_retries: 2,
        delay_seconds: 300,
    };

    // Row 22 — a newer slot is due, so this chain is superseded whatever else
    // is true of it.
    assert_eq!(
        admit_retry(
            slot,
            Some(utc(2026, 9, 15, 4, 0)),
            0,
            true,
            finished,
            Some(policy),
            utc(2026, 9, 15, 4, 1)
        ),
        RetryAdmission::Superseded {
            by: utc(2026, 9, 15, 4, 0)
        },
        "MUTANT `retry outlives its slot`: a pending retry must never be created once a newer \
         slot is due — a fresh run of the newer slot captures strictly more than a re-run of the \
         older one"
    );
    // Row 9 — not retryable, nothing happens.
    assert_eq!(
        admit_retry(
            slot,
            Some(slot),
            0,
            false,
            finished,
            Some(policy),
            finished + Duration::hours(1)
        ),
        RetryAdmission::NotRetryable
    );
    // Row 10 — no policy at all is exhaustion at attempt 0.
    assert_eq!(
        admit_retry(
            slot,
            Some(slot),
            0,
            true,
            finished,
            None,
            finished + Duration::hours(1)
        ),
        RetryAdmission::Exhausted {
            attempt: 0,
            max_retries: 0
        },
        "an absent `retry` block is no retries, which is today's behaviour"
    );
    // Row 11 — the delay has not elapsed.
    assert_eq!(
        admit_retry(
            slot,
            Some(slot),
            0,
            true,
            finished,
            Some(policy),
            finished + Duration::seconds(299)
        ),
        RetryAdmission::Pending {
            due_at: finished + Duration::seconds(300)
        }
    );
    // The delay boundary is inclusive on the admitting side.
    assert_eq!(
        admit_retry(
            slot,
            Some(slot),
            0,
            true,
            finished,
            Some(policy),
            finished + Duration::seconds(300)
        ),
        RetryAdmission::Due { attempt: 1 }
    );
    // Row 13 — and the chain walks 1, 2, then exhausts.
    assert_eq!(
        admit_retry(
            slot,
            Some(slot),
            1,
            true,
            finished,
            Some(policy),
            finished + Duration::hours(1)
        ),
        RetryAdmission::Due { attempt: 2 }
    );
    assert_eq!(
        admit_retry(
            slot,
            Some(slot),
            2,
            true,
            finished,
            Some(policy),
            finished + Duration::hours(1)
        ),
        RetryAdmission::Exhausted {
            attempt: 2,
            max_retries: 2
        },
        "MUTANT `unbounded retries`: at maxRetries the chain stops, and the slot is done"
    );
    // A `maxRetries` LOWERED by an edit below the attempts already made is
    // exhaustion, not a negative countdown (D1 §4.5 step 6d).
    assert_eq!(
        admit_retry(
            slot,
            Some(slot),
            3,
            true,
            finished,
            Some(RetryPolicy {
                max_retries: 1,
                delay_seconds: 300
            }),
            finished + Duration::hours(1)
        ),
        RetryAdmission::Exhausted {
            attempt: 3,
            max_retries: 1
        }
    );
}

/// **The retry policy's own ranges.**
#[test]
fn the_retry_policy_ranges_are_checked() {
    assert_eq!(RetryPolicy::default().max_retries, 0);
    assert_eq!(
        RetryPolicy::default().delay_seconds,
        DEFAULT_RETRY_DELAY_SECONDS
    );
    for k in 0..=MAX_RETRIES {
        assert!(RetryPolicy {
            max_retries: k,
            delay_seconds: 300
        }
        .validate()
        .is_ok());
    }
    assert_eq!(
        RetryPolicy {
            max_retries: MAX_RETRIES + 1,
            delay_seconds: 300
        }
        .validate(),
        Err(RetryPolicyProblem::MaxRetriesTooHigh {
            got: 4,
            limit: MAX_RETRIES
        })
    );
    for bad in [59, 21_601, 0] {
        assert!(matches!(
            RetryPolicy {
                max_retries: 1,
                delay_seconds: bad
            }
            .validate(),
            Err(RetryPolicyProblem::DelayOutOfRange { .. })
        ));
    }
    for edge in [60, 21_600] {
        assert!(RetryPolicy {
            max_retries: 1,
            delay_seconds: edge
        }
        .validate()
        .is_ok());
    }
}

// ---------------------------------------------------------------------------
// 5. Previews
// ---------------------------------------------------------------------------

/// **Previews: count, order, rendering and markers.**
#[test]
fn previews_mark_shifted_and_repeated_instants() {
    let berlin = Cadence::parse("30 2 * * *", Some("Europe/Berlin")).unwrap();

    // COUNT is what the caller asked for, and the two documented bounds are
    // constants rather than a number each caller re-decides.
    assert_eq!(STATUS_NEXT_RUNS, 5);
    assert_eq!(MAX_PREVIEW_COUNT, 20);
    assert_eq!(berlin.next_runs(utc(2026, 9, 1, 0, 0), 0).len(), 0);
    assert_eq!(berlin.next_runs(utc(2026, 9, 1, 0, 0), 1).len(), 1);
    assert_eq!(
        berlin
            .next_runs(utc(2026, 9, 1, 0, 0), STATUS_NEXT_RUNS)
            .len(),
        STATUS_NEXT_RUNS
    );
    assert_eq!(
        berlin
            .next_runs(utc(2026, 9, 1, 0, 0), MAX_PREVIEW_COUNT)
            .len(),
        MAX_PREVIEW_COUNT
    );

    // ORDER is strictly ascending, and the first entry is strictly after the
    // instant asked about.
    let runs = berlin.next_runs(utc(2026, 10, 24, 12, 0), MAX_PREVIEW_COUNT);
    assert!(runs[0].at > utc(2026, 10, 24, 12, 0));
    for pair in runs.windows(2) {
        assert!(pair[0].at < pair[1].at);
    }

    // MARKERS are written only where the clock did something, and the whole
    // rest of the preview carries none.
    let marked: Vec<_> = runs.iter().filter(|r| r.adjustment.is_some()).collect();
    assert_eq!(marked.len(), 2, "one fall back in a 20-day window");
    assert_eq!(
        marked[0].adjustment,
        Some(Adjustment::RepeatedLocalTimeFirst)
    );
    assert_eq!(
        marked[1].adjustment,
        Some(Adjustment::RepeatedLocalTimeSecond)
    );

    // RENDERING: the UTC instant and the local wall time with its offset, which
    // is D1 §4.4's shape.
    assert_eq!(marked[0].local_time, "2026-10-25T02:30:00+02:00");
    assert_eq!(marked[1].local_time, "2026-10-25T02:30:00+01:00");

    // A UTC preview carries `+00:00` and never a marker.
    let utc_runs = Cadence::parse("30 2 * * *", None)
        .unwrap()
        .next_runs(utc(2026, 10, 24, 12, 0), 5);
    assert_eq!(utc_runs[0].local_time, "2026-10-25T02:30:00+00:00");
    assert!(utc_runs.iter().all(|r| r.adjustment.is_none()));

    // A preview whose expression runs out is SHORTER, not an error.
    let short = Cadence::parse("0 0 29 2 *", None)
        .unwrap()
        .next_runs(utc(2096, 3, 1, 0, 0), 20);
    assert!(
        short.len() < 20 && !short.is_empty(),
        "a sparse expression runs out inside the walk bound, got {}",
        short.len()
    );
}

/// **The preview serialises to the shape D1 §4.4 states.**
#[test]
fn a_preview_entry_serialises_to_the_documented_shape() {
    let runs = Cadence::parse("30 2 * * *", Some("Europe/Berlin"))
        .unwrap()
        .next_runs(utc(2026, 10, 24, 12, 0), 3);
    let json = serde_json::to_value(&runs).expect("previews serialise");
    let first = &json[0];
    assert_eq!(first["at"], "2026-10-25T00:30:00Z");
    assert_eq!(first["localTime"], "2026-10-25T02:30:00+02:00");
    assert_eq!(first["adjustment"], "RepeatedLocalTimeFirst");
    // The marker is OMITTED, not null, when there is none: a status field that
    // is always present would churn every write.
    let third = &json[2];
    assert!(
        third.get("adjustment").is_none(),
        "an unadjusted entry carries no `adjustment` key, got {third}"
    );
    assert_eq!(third["localTime"], "2026-10-26T02:30:00+01:00");
}

// ---------------------------------------------------------------------------
// 6. Presets
// ---------------------------------------------------------------------------

/// **Every preset in range round-trips through its canonical cron.**
///
/// `match_preset(compile(p)) == Some(p)` over the WHOLE parameter space —
/// 60 hourly, 360 everyNHours, 1 440 daily, 10 080 weekly and 40 320 monthly
/// presets. An off-by-one in either direction shows up as a specific preset
/// rather than as a vague mismatch.
#[test]
fn every_preset_round_trips_through_its_cron() {
    let mut count = 0usize;
    let mut check = |p: Preset| {
        let cron = presets::compile(&p).unwrap_or_else(|e| panic!("{p:?}: {e}"));
        Cron::parse(&cron).unwrap_or_else(|e| panic!("{p:?} compiled to {cron:?}: {e}"));
        assert_eq!(
            presets::match_preset(&cron),
            Some(p),
            "{p:?} compiled to {cron:?} and did not read back as itself"
        );
        count += 1;
    };
    for minute in 0..60 {
        check(Preset::Hourly { minute });
        for n in presets::EVERY_N_HOURS_VALUES {
            check(Preset::EveryNHours { n, minute });
        }
        for hour in 0..24 {
            check(Preset::Daily { hour, minute });
            for day_of_week in 0..7 {
                check(Preset::Weekly {
                    day_of_week,
                    hour,
                    minute,
                });
            }
            for day_of_month in 1..=28 {
                check(Preset::Monthly {
                    day_of_month,
                    hour,
                    minute,
                });
            }
        }
    }
    assert_eq!(count, 60 + 360 + 1_440 + 10_080 + 40_320);
}

/// **The five canonical spellings, and the three `@` aliases.**
#[test]
fn presets_compile_to_the_canonical_expressions() {
    let table = [
        (Preset::Hourly { minute: 0 }, "0 * * * *"),
        (Preset::Hourly { minute: 17 }, "17 * * * *"),
        (Preset::EveryNHours { n: 6, minute: 30 }, "30 */6 * * *"),
        (Preset::Daily { hour: 2, minute: 0 }, "0 2 * * *"),
        (
            Preset::Weekly {
                day_of_week: 0,
                hour: 0,
                minute: 0,
            },
            "0 0 * * 0",
        ),
        (
            Preset::Monthly {
                day_of_month: 1,
                hour: 3,
                minute: 15,
            },
            "15 3 1 * *",
        ),
    ];
    for (preset, want) in table {
        assert_eq!(presets::compile(&preset).unwrap(), want, "{preset:?}");
    }
    // The aliases, exactly as D1 §4.2 maps them.
    assert_eq!(
        presets::match_preset("@hourly"),
        Some(Preset::Hourly { minute: 0 })
    );
    assert_eq!(
        presets::match_preset("@daily"),
        Some(Preset::Daily { hour: 0, minute: 0 })
    );
    assert_eq!(
        presets::match_preset("@weekly"),
        Some(Preset::Weekly {
            day_of_week: 0,
            hour: 0,
            minute: 0
        })
    );
    assert_eq!(
        presets::match_preset("@monthly"),
        None,
        "@monthly is not an expression this product accepts, so it is not a preset either"
    );
}

/// **Everything else is Advanced cron.**
///
/// A form that claims a preset it cannot express would silently rewrite the
/// user's expression on the next save.
#[test]
fn expressions_that_are_not_presets_are_not_claimed() {
    for expr in [
        "0,30 2 * * *", // a comma list
        "0 2 * * 1-5",  // a weekday range
        "0 2 1 1 *",    // a month restriction
        "0 2 31 * *",   // a day-of-month past the preset's 28
        "0 2 1 * 1",    // both day fields restricted
        "0 */5 * * *",  // a step that does not divide 24
        "*/15 * * * *", // a minute step
        "0 03 * * *",   // a leading zero, which compile() never emits
        "0 2 29 * *",   // 29, 30 and 31 are advanced
        "not a cron",   // and a string that does not parse at all
    ] {
        assert_eq!(
            presets::match_preset(expr),
            None,
            "{expr:?} is not one of the five presets"
        );
    }
    // A LEADING ZERO PARSES AS CRON but is not the canonical spelling, and the
    // proof that refusing it is right is that accepting it would break the
    // round trip.
    assert!(Cron::parse("0 03 * * *").is_ok());
    assert_eq!(
        presets::compile(&Preset::Daily { hour: 3, minute: 0 }).unwrap(),
        "0 3 * * *"
    );
}

/// **The `monthly` preset stops at 28.**
#[test]
fn the_monthly_preset_stops_at_28() {
    assert!(presets::compile(&Preset::Monthly {
        day_of_month: 28,
        hour: 3,
        minute: 0
    })
    .is_ok());
    let err = presets::compile(&Preset::Monthly {
        day_of_month: 31,
        hour: 3,
        minute: 0,
    })
    .expect_err("31 is not offered by the form");
    assert!(
        err.to_string().contains("1..=28"),
        "the refusal names the accepted range: {err}"
    );
    // And the reason, measured: `0 3 31 * *` runs SEVEN times a year, not
    // twelve.
    let year = Cadence::parse("0 3 31 * *", None)
        .unwrap()
        .next_runs(utc(2026, 1, 1, 0, 0), 12);
    let in_2026 = year
        .iter()
        .filter(|r| r.at.format("%Y").to_string() == "2026")
        .count();
    assert_eq!(
        in_2026, 7,
        "a `dayOfMonth: 31` cadence is seven backups a year, which is why the form caps at 28"
    );
}

/// **Out-of-range preset parameters are refused naming the parameter.**
#[test]
fn preset_parameters_are_range_checked() {
    let cases: Vec<(Preset, &str)> = vec![
        (Preset::Hourly { minute: 60 }, "minute"),
        (Preset::EveryNHours { n: 5, minute: 0 }, "n"),
        (Preset::EveryNHours { n: 0, minute: 0 }, "n"),
        (
            Preset::Daily {
                hour: 24,
                minute: 0,
            },
            "hour",
        ),
        (
            Preset::Weekly {
                day_of_week: 7,
                hour: 0,
                minute: 0,
            },
            "dayOfWeek",
        ),
        (
            Preset::Monthly {
                day_of_month: 0,
                hour: 0,
                minute: 0,
            },
            "dayOfMonth",
        ),
    ];
    for (preset, parameter) in cases {
        let err = presets::compile(&preset).expect_err("out of range");
        assert!(
            err.to_string().contains(parameter),
            "{preset:?} must be refused naming {parameter}: {err}"
        );
    }
}

/// **The preset catalogue fixture the static UI reads matches Rust.**
///
/// D1 §4.2 puts ONE catalogue in the product and lets the browser render string
/// templates from it — never evaluate cron. This test is the drift gate: the
/// checked-in fixture is this test's own output, so a preset added, renamed or
/// re-ranged in Rust fails here until the fixture is regenerated.
///
/// Regenerate with:
///
/// ```text
/// LOGWEIR_WRITE_FIXTURES=1 cargo test --locked -p weirkeeper --test cadence \
///   the_preset_catalogue_fixture_matches_the_rust_catalogue
/// ```
#[test]
fn the_preset_catalogue_fixture_matches_the_rust_catalogue() {
    let fixture = serde_json::json!({
        "presets": presets::catalogue(),
        "aliases": [
            { "schedule": "@hourly", "preset": presets::match_preset("@hourly") },
            { "schedule": "@daily",  "preset": presets::match_preset("@daily") },
            { "schedule": "@weekly", "preset": presets::match_preset("@weekly") },
        ],
        "examples": example_rows(),
    });
    let want = format!(
        "{}\n",
        serde_json::to_string_pretty(&fixture).expect("the catalogue serialises")
    );

    let path = workspace_root().join("ui/tests/fixtures/cadence-presets.json");
    if std::env::var_os("LOGWEIR_WRITE_FIXTURES").is_some() {
        std::fs::write(&path, &want).expect("the fixture is writable");
    }
    let got = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "{} is missing ({e}); regenerate it with LOGWEIR_WRITE_FIXTURES=1",
            path.display()
        )
    });
    assert_eq!(
        got, want,
        "ui/tests/fixtures/cadence-presets.json has drifted from weirkeeper::cadence::presets. \
         Regenerate it with LOGWEIR_WRITE_FIXTURES=1 cargo test --locked -p weirkeeper --test \
         cadence the_preset_catalogue_fixture_matches_the_rust_catalogue"
    );
}

/// One example per preset kind, each with the expression it compiles to.
///
/// THE EXAMPLES ARE THE DRIFT BAIT. A catalogue of parameter ranges alone would
/// still match a Rust `compile` that put the fields in another order; a
/// compiled string per kind pins the template to the function.
fn example_rows() -> serde_json::Value {
    let examples = [
        Preset::Hourly { minute: 5 },
        Preset::EveryNHours { n: 6, minute: 30 },
        Preset::Daily { hour: 2, minute: 0 },
        Preset::Weekly {
            day_of_week: 1,
            hour: 23,
            minute: 45,
        },
        Preset::Monthly {
            day_of_month: 28,
            hour: 4,
            minute: 15,
        },
    ];
    serde_json::Value::Array(
        examples
            .iter()
            .map(|p| {
                serde_json::json!({
                    "preset": p,
                    "schedule": presets::compile(p).expect("the examples are in range"),
                })
            })
            .collect(),
    )
}

// ---------------------------------------------------------------------------
// 7. The database this binary carries
// ---------------------------------------------------------------------------

/// **`TZDB_SOURCE` is the version `Cargo.lock` resolves** — D1 §4.3.
///
/// `status.policy.tzdb` carries this string so an operator can tell which rule
/// set produced a schedule's slots. A string that says one version while the
/// binary links another is worse than no string: it would be read as evidence.
#[test]
fn the_tzdb_source_is_the_version_cargo_lock_resolves() {
    let lock = std::fs::read_to_string(workspace_root().join("Cargo.lock"))
        .expect("the workspace carries a lock file");
    let version = lock
        .split("\n[[package]]\n")
        .find(|block| block.starts_with("name = \"chrono-tz\"\n"))
        .and_then(|block| block.lines().nth(1))
        .and_then(|line| line.strip_prefix("version = \""))
        .and_then(|rest| rest.strip_suffix('"'))
        .expect("Cargo.lock resolves chrono-tz");
    assert_eq!(
        TZDB_SOURCE,
        format!("chrono-tz {version}"),
        "weirkeeper::cadence::TZDB_SOURCE must name the version Cargo.lock resolves; bump the \
         constant in the same commit as the dependency"
    );
    // And the zones the test table names are all in it, so a future database
    // that dropped one fails here rather than at a customer's transition.
    for zone in [
        "Europe/Berlin",
        "America/New_York",
        "Australia/Lord_Howe",
        "Asia/Kathmandu",
        "Pacific/Apia",
        "Etc/GMT+5",
        "UTC",
    ] {
        assert!(
            Zone::resolve(Some(zone)).is_ok(),
            "{zone} must resolve against {TZDB_SOURCE}"
        );
    }
}
