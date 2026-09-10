//! **Guard G-WIN, first half** — the window's floor is the archive's.
//!
//! The engine's PITR filter is `time_window_start`/`time_window_end`, both
//! `Option<i64>` epoch-ms, validated only as `start <= end`, and
//! `render_restore.rs` renders BOTH unconditionally from `RestorePlan.
//! time_window`. Nothing used to bind the start to the archive: a restore that
//! inherited a later start silently lost everything before it, while phase 7
//! reconciled only the *sampled* records and the scorecard said pass.
//!
//! Two properties, at two different seams, because one of them cannot see the
//! other's mutants:
//!
//! 1. **Plan construction** (`logweir::drill::build_plan`) computes
//!    `time_window.0` from the archive set's earliest covered timestamp as
//!    recorded in the manifest, sets `window_floor_source ==
//!    WindowFloorSource::ArchiveManifest`, and ends with an explicit check that
//!    the enum agrees with the value it claims.
//! 2. **Phase 5** (`phase5_preflight::check_rendered_window_floor`) renders the
//!    document, reads the `time_window_start` line back off the BYTES, and
//!    compares that against a floor it re-derives from the manifest — because
//!    `render_restore::render` is a printer, and a printer mutant is invisible
//!    to any assertion about the plan struct. That is exactly why spec §10's
//!    earlier G-WIN row had two mutants that both passed.
//!
//! **No dial token, deliberately** (STANDING RULE 18): every fixture here
//! names `kafka-broker-1:9094` and a filesystem archive URL, so this file is
//! not a chain N member and adds no entry to
//! `crates/logweir/tests/no_network_in_unit_tests.rs`'s `ALLOWED`.
//!
//! Per-test budget (GC22, 15 s): every test builds a `BackupSetFacts` in
//! memory and calls plan construction or the phase-5 check directly. No
//! socket, no subprocess, no archive read.

use logweir::drill::{
    build_plan, build_plan_with_floor, phase5_preflight, DrillError, WindowFloor,
};
use logweir::exit::ExitCode;
use logweir_core::engine::{
    BackupSetFacts, BackupSetRef, PartitionFacts, SegmentFacts, TopicFacts, WindowFloorSource,
};
use logweir_core::spec::DrillSpec;
use std::collections::BTreeMap;

/// The manifest's earliest covered timestamp — `2023-11-14T22:13:20Z`.
const FLOOR_MS: i64 = 1_700_000_000_000;
/// The LATER of the fixture's two segment start timestamps. It exists so
/// "take the maximum instead of the minimum" is a mutant with a witness: a
/// one-segment manifest cannot tell min from max.
const LATER_SEGMENT_MS: i64 = 1_700_001_800_000;
/// The spec's `sample.window_start` — ten minutes AFTER the manifest floor,
/// which is the direction that loses records.
const SPEC_START_MS: i64 = 1_700_000_600_000;
/// The spec's `sample.window_end`.
const SPEC_END_MS: i64 = 1_700_003_600_000;

fn ts(ms: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp_millis(ms).expect("a representable timestamp")
}

fn rfc3339(ms: i64) -> String {
    ts(ms).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// The approved spec BYTES, parsed — never a `DrillSpec` literal, so the
/// `restore:` block's own wire shape is exercised by every row that uses one.
///
/// `bootstrap_servers` is `kafka-broker-1:9094` and the archive is a
/// filesystem path: no dial token in this file.
fn spec_yaml(restore_block: &str) -> String {
    format!(
        "source:\n  \
           storage:\n    backend: filesystem\n    path: /archive\n  \
           backup: latestCompleted\n  \
           topics: [orders]\n\
         target:\n  \
           bootstrap_servers: [kafka-broker-1:9094]\n  \
           marker_topic: logweir.scratch\n  \
           topic_mapping_prefix: \"drill-\"\n  \
           default_replication_factor: 1\n  \
           teardown: delete\n\
         sample:\n  \
           window_start: \"{start}\"\n  \
           window_end: \"{end}\"\n  \
           records_per_partition: 25\n  \
           anchor: head\n\
         {restore_block}\
         objectives:\n  rto_seconds: 900\n\
         evidence:\n  backend: filesystem\n  path: /logweir/evidence\n\
         notifications:\n  webhooks: []\n",
        start = rfc3339(SPEC_START_MS),
        end = rfc3339(SPEC_END_MS),
    )
}

fn spec() -> DrillSpec {
    serde_yaml::from_str(&spec_yaml("")).expect("the approved spec bytes parse")
}

fn set() -> BackupSetRef {
    BackupSetRef {
        backup_id: "backup-2023-11-14T23:00:00Z".into(),
        manifest_key: "drills/backup-2023-11-14T23:00:00Z/manifest.json".into(),
    }
}

fn mapping() -> BTreeMap<String, String> {
    [("orders".to_string(), "drill-orders".to_string())]
        .into_iter()
        .collect()
}

/// One topic, one partition, TWO segments — the earlier starting at
/// `FLOOR_MS`, the later at `LATER_SEGMENT_MS`. The segments are in ascending
/// order, so a `min` mutated to `max`, to `last()`, or to "the first segment's
/// timestamp of the newest partition" all produce a different answer.
fn facts_with_two_segments() -> BackupSetFacts {
    facts_from(&[FLOOR_MS, LATER_SEGMENT_MS])
}

fn facts_from(segment_starts: &[i64]) -> BackupSetFacts {
    BackupSetFacts {
        backup_id: "backup-2023-11-14T23:00:00Z".into(),
        created_at: ts(LATER_SEGMENT_MS),
        source_cluster_id: Some("SRC0000000000000000000".into()),
        manifest_sha256: format!("sha256:{}", "a".repeat(64)),
        manifest_version_id: None,
        consumer_group_snapshot_sha256: None,
        topics: vec![TopicFacts {
            name: "orders".into(),
            original_partition_count: Some(1),
            source_replication_factor: Some(1),
            configurations: BTreeMap::new(),
            partitions: vec![PartitionFacts {
                partition_id: 0,
                segments: segment_starts
                    .iter()
                    .enumerate()
                    .map(|(i, start)| SegmentFacts {
                        key: format!("drills/b/0/{i:012}.kbak"),
                        start_offset: i as i64 * 100,
                        end_offset: i as i64 * 100 + 99,
                        start_timestamp: *start,
                        end_timestamp: *start + 60_000,
                        record_count: 100,
                        sha256: format!("sha256:{}", "b".repeat(64)),
                        uploaded_at: *start + 120_000,
                    })
                    .collect(),
                gaps: vec![],
                pruned: vec![],
            }],
        }],
    }
}

/// The exit code the SHIPPED mapping (`DrillError::exit_code`, Global
/// Constraint 11) gives this result. `Ok` is exit 0 by that contract; nothing
/// here invents a code.
fn exit_of<T>(r: &Result<T, DrillError>) -> ExitCode {
    match r {
        Ok(_) => ExitCode::Ok,
        Err(e) => e.exit_code(),
    }
}

fn guard_message(e: &DrillError) -> String {
    match e {
        DrillError::Guard(g) => g.0.clone(),
        other => panic!("expected a guard refusal, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// G-WIN, first half — the binding
// ---------------------------------------------------------------------------

/// **G-WIN, first half.** The manifest floor is `1_700_000_000_000`; the spec's
/// `sample.window_start` is `1_700_000_600_000`, ten minutes later. The plan
/// carries the ARCHIVE's floor, and says so.
///
/// Two separate `assert_eq!`s, so a reviewer can see which one a mutant killed:
/// the millis is the assertion the enum cannot fake.
#[test]
fn restore_plan_window_start_is_the_archive_floor() {
    let spec = spec();
    assert_eq!(
        spec.sample.window_start.timestamp_millis(),
        SPEC_START_MS,
        "the fixture must actually state a LATER start than the archive's, \
         or this test proves nothing"
    );

    let plan = build_plan(
        &spec,
        &set(),
        &mapping(),
        &facts_with_two_segments(),
        "01J9X",
    )
    .expect("the plan builds");

    assert_eq!(
        plan.time_window.0.timestamp_millis(),
        FLOOR_MS,
        "time_window.0 must be the archive set's earliest covered timestamp, \
         never the spec's window_start"
    );
    assert_eq!(
        plan.window_floor_source,
        WindowFloorSource::ArchiveManifest,
        "and the plan must SAY the floor came from the manifest"
    );
}

/// The enum is checked against the value it claims (critique A F22).
/// `window_floor_source == ArchiveManifest` while `time_window.0` was read
/// from the spec is a lie the enum cannot catch on its own, so plan
/// construction refuses it — exit 3, before anything runs.
///
/// This goes through the seam `build_plan` exposes for exactly this: passing
/// the start, the claim and the manifest floor TOGETHER is what makes the lie
/// constructible, and therefore refusable.
#[test]
fn the_floor_source_enum_agrees_with_the_value() {
    let lying = build_plan_with_floor(
        &spec(),
        &set(),
        &mapping(),
        "01J9X",
        WindowFloor {
            // Read from the SPEC…
            start: ts(SPEC_START_MS),
            // …while claiming the manifest.
            source: WindowFloorSource::ArchiveManifest,
            manifest_floor_ms: FLOOR_MS,
        },
    );
    // THE EXIT CODE FIRST, as its own assertion, so a mutant that lets the lie
    // through fails HERE — at assertion time, on the exit code — and not at an
    // `expect_err` unwrap.
    assert_eq!(
        exit_of(&lying),
        ExitCode::GuardRefused,
        "refused by a guard, before anything ran (Global Constraint 11)"
    );
    let e = lying.expect_err("a plan whose enum lies about its floor is refused");
    assert_eq!(
        guard_message(&e),
        format!(
            "the plan claims its window floor came from the archive manifest, and its \
             time_window start is epoch-ms {SPEC_START_MS} while the manifest floor is \
             epoch-ms {FLOOR_MS}; a Restore's window start is the archive set's earliest \
             covered timestamp, never the spec's"
        )
    );

    // The check reads the ENUM, and the other arm is not a lie: a plan that
    // says its floor was inherited from the spec is describing itself
    // truthfully, and is not this check's business.
    let honest = build_plan_with_floor(
        &spec(),
        &set(),
        &mapping(),
        "01J9X",
        WindowFloor {
            start: ts(SPEC_START_MS),
            source: WindowFloorSource::InheritedFromSpec,
            manifest_floor_ms: FLOOR_MS,
        },
    )
    .expect("a plan that does not claim the manifest is not refused by this check");
    assert_eq!(
        honest.window_floor_source,
        WindowFloorSource::InheritedFromSpec
    );
    assert_eq!(honest.time_window.0.timestamp_millis(), SPEC_START_MS);
}

/// `spec.restore.point_in_time` is the window's END when present; with the
/// `restore` block absent, `time_window.1` is `sample.window_end` and every
/// existing drill fixture is unchanged.
///
/// Both epoch-ms values the brief states are asserted here, at the RFC3339
/// string each actually denotes: `2026-09-07T14:05:00Z` is
/// `1_788_789_900_000` and `2026-08-30T14:05:00Z` is `1_788_098_700_000`. The
/// brief paired the first string with the second integer; rather than pick
/// one and drop the other silently, this test proves the property at both.
#[test]
fn the_point_in_time_is_the_window_end_when_present() {
    for (stated, want_ms) in [
        ("2026-09-07T14:05:00Z", 1_788_789_900_000_i64),
        ("2026-08-30T14:05:00Z", 1_788_098_700_000_i64),
    ] {
        let text = spec_yaml(&format!("restore:\n  point_in_time: \"{stated}\"\n"));
        let spec: DrillSpec = serde_yaml::from_str(&text).expect("the spec bytes parse");
        assert_ne!(
            spec.sample.window_end.timestamp_millis(),
            want_ms,
            "sample.window_end must DIFFER from point_in_time, or the assertion \
             below cannot tell which field was read"
        );
        let plan = build_plan(
            &spec,
            &set(),
            &mapping(),
            &facts_with_two_segments(),
            "01J9X",
        )
        .expect("the plan builds");
        assert_eq!(
            plan.time_window.1.timestamp_millis(),
            want_ms,
            "restore.point_in_time is the window END when the spec states one"
        );
        // …and the floor is still the archive's.
        assert_eq!(plan.time_window.0.timestamp_millis(), FLOOR_MS);
    }

    // The `restore` block absent: unchanged behaviour for the END.
    let spec = spec();
    let plan = build_plan(
        &spec,
        &set(),
        &mapping(),
        &facts_with_two_segments(),
        "01J9X",
    )
    .expect("the plan builds");
    assert!(
        spec.restore.point_in_time.is_none(),
        "an absent `restore:` block deserialises to no point in time"
    );
    assert_eq!(
        plan.time_window.1.timestamp_millis(),
        SPEC_END_MS,
        "with no point_in_time the window END is sample.window_end, exactly as before"
    );
}

// ---------------------------------------------------------------------------
// G-WIN, first half — the phase-5 refusal
// ---------------------------------------------------------------------------

/// Phase 5 refuses, exit 3, when the RENDERED `time_window_start` is not the
/// archive floor.
///
/// The plan's `time_window.0` is forced to the spec value AFTER construction,
/// which is why this test cannot also stand in for the plan-construction
/// mutants: it overrides the very field they change (critique A F22).
#[test]
fn phase5_refuses_a_rendered_start_that_is_not_the_floor() {
    let facts = facts_with_two_segments();
    let mut plan = build_plan(&spec(), &set(), &mapping(), &facts, "01J9X").expect("builds");
    // The state a printer bug, a hand-built plan, or a controller-side edit
    // would produce.
    plan.time_window.0 = ts(SPEC_START_MS);

    let r = phase5_preflight::check_rendered_window_floor(&plan, &facts);
    assert_eq!(
        exit_of(&r),
        ExitCode::GuardRefused,
        "exit 3, refused by a guard before the document is written — NOT ruling R-E's exit 1"
    );
    let e = r.expect_err("phase 5 refuses a rendered start that is not the floor");
    assert_eq!(
        guard_message(&e),
        format!(
            "rendered time_window_start {SPEC_START_MS} is not the archive floor {FLOOR_MS}; \
             a Restore's window start is the archive set's earliest covered timestamp, never \
             the spec's"
        )
    );
}

/// The positive arm. Without it a printer mutant has no failing test: a
/// `render_restore.rs` that emitted the SPEC's start while plan construction
/// stayed correct would leave every assertion above green, because phase 5
/// reads the rendered line and not the plan field.
#[test]
fn phase5_accepts_the_rendered_start_when_it_is_the_floor() {
    let facts = facts_with_two_segments();
    let plan = build_plan(&spec(), &set(), &mapping(), &facts, "01J9X").expect("builds");
    assert_eq!(
        exit_of(&phase5_preflight::check_rendered_window_floor(
            &plan, &facts
        )),
        ExitCode::Ok,
        "a correct plan renders a time_window_start equal to the manifest floor"
    );
}

/// A manifest with no segment at all has no earliest covered timestamp, and
/// neither seam may invent one: plan construction refuses, and so does the
/// phase-5 check if it is ever reached with such a manifest.
#[test]
fn an_archive_set_with_no_segment_has_no_floor_to_bind() {
    let mut facts = facts_with_two_segments();
    facts.topics[0].partitions[0].segments.clear();
    assert_eq!(facts.earliest_covered_timestamp_ms(), None);

    let floorless = build_plan(&spec(), &set(), &mapping(), &facts, "01J9X");
    assert_eq!(exit_of(&floorless), ExitCode::GuardRefused);
    let e = floorless.expect_err("a floorless archive set is refused");
    assert!(
        guard_message(&e).contains("records no segment in its manifest"),
        "{}",
        guard_message(&e)
    );

    let good = facts_with_two_segments();
    let plan = build_plan(&spec(), &set(), &mapping(), &good, "01J9X").expect("builds");
    assert_eq!(
        exit_of(&phase5_preflight::check_rendered_window_floor(
            &plan, &facts
        )),
        ExitCode::GuardRefused,
        "phase 5 will not pass a plan it cannot check against a floor"
    );
}
