//! **PROD-11.1 — replay selection, the plan side** (guard G-WIN as amended:
//! `docs/to-do/decisions/PROD-11.1-replay-selection.md` §2).
//!
//! 1. **Plan construction** binds a stated window start (the interval form of
//!    `restore.point_in_time`) as the plan's own start (`InheritedFromSpec`)
//!    and refuses it before the archive's floor (never moves it there).
//! 2. **Partition subsets are refused by name** (`PartitionSubsetsAwaitOwnerDecision`,
//!    OD-9) on every path that turns a plan into a selection.
//! 3. **Phase 5** re-derives the start and the engine runs from the SPEC and
//!    the manifest — never from the plan's claim — and refuses a rendered
//!    document that disagrees: a start moved to the floor (a silent widening),
//!    and (for a plan built by hand, the only way a subset reaches it) a
//!    subset the spec does not state.
//! 4. **Resolution before phase 2**: a window no segment overlaps is exit 3.
//!
//! No dial token: a filesystem archive URL and `kafka-broker-1:9094` only. No
//! socket, no subprocess, no archive read; every row is in-memory.

use logweir::drill::{
    build_plan, build_plan_with_floor, phase5_preflight, resolve_selection, DrillError, WindowFloor,
};
use logweir::exit::ExitCode;
use logweir_core::engine::{
    BackupSetFacts, BackupSetRef, PartitionFacts, SegmentFacts, TopicFacts, WindowFloorSource,
};
use logweir_core::spec::DrillSpec;
use std::collections::BTreeMap;

/// The archive's floor: the minimum first-record timestamp.
const FLOOR_MS: i64 = 1_700_000_000_000;
/// A stated start, ten minutes after the floor.
const START_MS: i64 = FLOOR_MS + 600_000;
/// The window's end: `sample.window_end`, and the end of the plan's interval.
const END_MS: i64 = FLOOR_MS + 3_600_000;

fn ts(ms: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp_millis(ms).expect("a representable timestamp")
}

fn rfc3339(ms: i64) -> String {
    ts(ms).to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn spec_yaml(restore_block: &str) -> String {
    format!(
        "source:\n  \
           storage:\n    backend: filesystem\n    path: /archive\n  \
           backup: latestCompleted\n  \
           topics: [orders, payments, audit]\n\
         target:\n  \
           bootstrap_servers: [kafka-broker-1:9094]\n  \
           topic_mapping_prefix: \"drill-\"\n\
         sample:\n  \
           window_start: \"{start}\"\n  \
           window_end: \"{end}\"\n\
         {restore_block}\
         objectives: {{}}\n\
         evidence:\n  backend: filesystem\n  path: /logweir/evidence\n",
        start = rfc3339(FLOOR_MS),
        end = rfc3339(END_MS),
    )
}

fn spec(restore_block: &str) -> DrillSpec {
    serde_yaml::from_str(&spec_yaml(restore_block)).expect("the approved spec bytes parse")
}

/// `restore.point_in_time: "<start>/<end>"`.
fn window(start_ms: i64, end_ms: i64) -> String {
    format!(
        "restore:\n  point_in_time: \"{}/{}\"\n",
        rfc3339(start_ms),
        rfc3339(end_ms)
    )
}

fn selecting() -> DrillSpec {
    spec(&window(START_MS, END_MS))
}

fn set() -> BackupSetRef {
    BackupSetRef {
        backup_id: "b".into(),
        manifest_key: "drills/b/manifest.json".into(),
    }
}

fn mapping() -> BTreeMap<String, String> {
    ["orders", "payments", "audit"]
        .iter()
        .map(|t| (t.to_string(), format!("drill-{t}")))
        .collect()
}

/// Three topics of three partitions; every partition one segment
/// `[FLOOR + p·60 s, FLOOR + p·60 s + 30 min]`, 100 records.
fn facts() -> BackupSetFacts {
    let topic = |name: &str| TopicFacts {
        name: name.into(),
        original_partition_count: Some(3),
        source_replication_factor: Some(1),
        configurations: BTreeMap::new(),
        partitions: (0..3)
            .map(|p| PartitionFacts {
                partition_id: p,
                segments: vec![SegmentFacts {
                    key: format!("b/{name}/{p}/0.kbak"),
                    start_offset: 0,
                    end_offset: 99,
                    start_timestamp: FLOOR_MS + i64::from(p) * 60_000,
                    end_timestamp: FLOOR_MS + i64::from(p) * 60_000 + 1_800_000,
                    record_count: 100,
                    sha256: format!("sha256:{}", "b".repeat(64)),
                    uploaded_at: 0,
                }],
                gaps: vec![],
                pruned: vec![],
            })
            .collect(),
    };
    BackupSetFacts {
        backup_id: "b".into(),
        created_at: ts(END_MS),
        source_cluster_id: None,
        manifest_sha256: format!("sha256:{}", "a".repeat(64)),
        manifest_version_id: None,
        consumer_group_snapshot_sha256: None,
        topics: vec![topic("audit"), topic("orders"), topic("payments")],
    }
}

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

fn check(plan: &logweir_core::engine::RestorePlan, spec: &DrillSpec) -> Result<(), DrillError> {
    phase5_preflight::check_rendered_selection(
        plan,
        &facts(),
        &phase5_preflight::StatedSelection::of(spec),
    )
}

/// The plan binds the STATED start as its own and says so; phase 5 accepts
/// what plan construction built. No subset is carried.
#[test]
fn a_stated_start_is_bound_into_the_plan() {
    let s = selecting();
    let plan = build_plan(&s, &set(), &mapping(), &facts(), "01J9X", None).expect("builds");
    assert_eq!(plan.time_window.0.timestamp_millis(), START_MS);
    assert_eq!(plan.time_window.1.timestamp_millis(), END_MS);
    assert_eq!(
        plan.window_floor_source,
        WindowFloorSource::InheritedFromSpec
    );
    assert!(plan.source_partitions.is_empty());
    assert_eq!(exit_of(&check(&plan, &s)), ExitCode::Ok);
}

/// A plan with no selection is the plan it always was: the archive's floor,
/// no subset, one run, and phase 5 is satisfied by the floor alone.
#[test]
fn no_selection_is_the_archive_floor_as_before() {
    let s = spec("");
    let plan = build_plan(&s, &set(), &mapping(), &facts(), "01J9X", None).expect("builds");
    assert_eq!(plan.time_window.0.timestamp_millis(), FLOOR_MS);
    assert_eq!(plan.window_floor_source, WindowFloorSource::ArchiveManifest);
    assert!(plan.source_partitions.is_empty());
    assert!(resolve_selection(&s, &mapping(), &facts())
        .unwrap()
        .is_none());
    assert_eq!(exit_of(&check(&plan, &s)), ExitCode::Ok);
}

/// **A partition subset is refused BY NAME on every path** (the review's H1,
/// OD-9): resolution, plan construction and phase 0's shape check all go
/// through `ReplaySelection::from_spec`, which refuses `restore.partitions`
/// with or without a start. KILLS: the refusal deleted (a subset would be
/// restored and signed under a format a verifier that predates it misreads).
#[test]
fn a_partition_subset_is_refused_by_name_on_every_path() {
    for restore in [
        "restore:\n  partitions:\n    orders: [0, 2]\n".to_string(),
        format!(
            "{}  partitions:\n    orders: [1]\n",
            window(START_MS, END_MS)
        ),
    ] {
        let s = spec(&restore);
        let resolved = resolve_selection(&s, &mapping(), &facts());
        assert_eq!(exit_of(&resolved), ExitCode::GuardRefused, "{restore}");
        let msg = guard_message(&resolved.unwrap_err());
        assert!(
            msg.starts_with("PartitionSubsetsAwaitOwnerDecision: restore.partitions names a partition subset of orders"),
            "{msg}"
        );
        let built = build_plan(&s, &set(), &mapping(), &facts(), "01J9X", None);
        assert_eq!(exit_of(&built), ExitCode::GuardRefused, "{restore}");
    }
}

/// **A start before the archive's coverage is refused, never widened to the
/// floor**: at plan construction, at resolution and at phase 5, each from its
/// own reading. KILLS: clamping the start to the floor, comparing with `<=`
/// (a start AT the floor is admitted), reading the floor of all topics.
#[test]
fn a_start_before_coverage_is_refused_everywhere_and_never_moved() {
    let early = spec(&window(FLOOR_MS - 1, END_MS));
    let built = build_plan(&early, &set(), &mapping(), &facts(), "01J9X", None);
    assert_eq!(exit_of(&built), ExitCode::GuardRefused);
    let msg = guard_message(&built.unwrap_err());
    assert!(
        msg.contains(&format!("epoch-ms {}", FLOOR_MS - 1))
            && msg.contains(&format!(
                "earliest covered timestamp of epoch-ms {FLOOR_MS}"
            ))
            && msg.contains("Refused rather than moved"),
        "{msg}"
    );
    assert_eq!(
        exit_of(&resolve_selection(&early, &mapping(), &facts())),
        ExitCode::GuardRefused
    );
    // At the floor exactly: admitted, and the plan's own.
    let at = spec(&window(FLOOR_MS, END_MS));
    let plan = build_plan(&at, &set(), &mapping(), &facts(), "01J9X", None).expect("at the floor");
    assert_eq!(
        plan.window_floor_source,
        WindowFloorSource::InheritedFromSpec
    );
    assert_eq!(plan.time_window.0.timestamp_millis(), FLOOR_MS);
    // Phase 5's own reading of the SPEC refuses an early start even when the
    // plan it is handed is the floor's (a plan built by anything else).
    let floor_plan = build_plan(&spec(""), &set(), &mapping(), &facts(), "01J9X", None).unwrap();
    let r = check(&floor_plan, &early);
    assert_eq!(exit_of(&r), ExitCode::GuardRefused);
    assert!(guard_message(&r.unwrap_err()).contains("before the archive set's earliest"));
}

/// The other arm of the enum check: a plan that claims its start came from
/// the spec may not hold one earlier than the manifest floor it was handed —
/// by a minute, and by ONE millisecond (review L1: the boundary row that
/// kills `< floor - 1`). The control at the floor itself is admitted.
#[test]
fn an_inherited_start_before_the_floor_is_refused_at_construction() {
    let build = |start_ms: i64| {
        build_plan_with_floor(
            &selecting(),
            &set(),
            &mapping(),
            "01J9X",
            WindowFloor {
                start: ts(start_ms),
                source: WindowFloorSource::InheritedFromSpec,
                manifest_floor_ms: FLOOR_MS,
            },
            None,
        )
    };
    for start_ms in [FLOOR_MS - 60_000, FLOOR_MS - 1] {
        let r = build(start_ms);
        assert_eq!(exit_of(&r), ExitCode::GuardRefused, "start {start_ms}");
        assert!(guard_message(&r.unwrap_err()).contains("Refused rather than moved"));
    }
    let at = build(FLOOR_MS).expect("a start AT the floor is admitted");
    assert_eq!(at.time_window.0.timestamp_millis(), FLOOR_MS);
}

/// **The silent widening, caught at phase 5.** A plan whose start was moved
/// back to the archive's floor while its approved spec states a later one
/// would restore records nobody approved and sign the wider window; phase 5
/// reads the SPEC and refuses it. And the reverse: a spec with no start and a
/// plan that inherited one. KILLS: phase 5 reading the plan's claim or
/// `time_window.0` as the expected value.
#[test]
fn phase5_refuses_a_rendered_start_that_is_not_the_approved_one() {
    let s = selecting();
    let mut plan = build_plan(&s, &set(), &mapping(), &facts(), "01J9X", None).unwrap();
    plan.time_window.0 = ts(FLOOR_MS);
    let r = check(&plan, &s);
    assert_eq!(exit_of(&r), ExitCode::GuardRefused);
    assert_eq!(
        guard_message(&r.unwrap_err()),
        format!(
            "rendered time_window_start {FLOOR_MS} is not the approved window start \
             {START_MS} (the archive floor is {FLOOR_MS}); a Restore's window starts at the \
             archive's floor or at the start its approved plan states, and nowhere else"
        )
    );
    let mut inherited = build_plan(&spec(""), &set(), &mapping(), &facts(), "01J9X", None).unwrap();
    inherited.time_window.0 = ts(START_MS);
    inherited.window_floor_source = WindowFloorSource::InheritedFromSpec;
    let r = check(&inherited, &spec(""));
    assert_eq!(exit_of(&r), ExitCode::GuardRefused);
    assert!(guard_message(&r.unwrap_err()).contains("is not the archive floor"));
}

/// **A subset only a hand-built plan can carry is refused at phase 5 too.**
/// No spec can state one (refused by name), so phase 5's per-run check is the
/// second wall: a plan whose `source_partitions` the approved spec does not
/// state — added, or merged across topics, or swapped — is refused before the
/// engine is handed it; and a stated subset (`StatedSelection` built by hand,
/// the shape OD-9's decision would reach) is checked run by run. KILLS: phase
/// 5 checking the start only; checking the count of runs only.
#[test]
fn phase5_refuses_a_rendered_partition_selection_that_is_not_the_approved_one() {
    let s = selecting();
    let good = build_plan(&s, &set(), &mapping(), &facts(), "01J9X", None).unwrap();
    let mut added = good.clone();
    added.source_partitions.insert("payments".into(), vec![1]);
    let r = check(&added, &s);
    assert_eq!(
        exit_of(&r),
        ExitCode::GuardRefused,
        "a subset the spec does not state"
    );

    let mut stated = phase5_preflight::StatedSelection::of(&s);
    stated.partitions = [
        ("orders".to_string(), vec![2, 0]),
        ("payments".to_string(), vec![1]),
    ]
    .into_iter()
    .collect();
    let check_stated = |plan: &logweir_core::engine::RestorePlan| {
        phase5_preflight::check_rendered_selection(plan, &facts(), &stated)
    };
    let mut hand = good.clone();
    hand.source_partitions = [
        ("orders".to_string(), vec![0, 2]),
        ("payments".to_string(), vec![1]),
    ]
    .into_iter()
    .collect();
    assert_eq!(logweir_engine_oso::render_restore::runs(&hand).len(), 3);
    assert_eq!(exit_of(&check_stated(&hand)), ExitCode::Ok);

    let mut dropped = hand.clone();
    dropped.source_partitions.remove("payments");
    let r = check_stated(&dropped);
    assert_eq!(exit_of(&r), ExitCode::GuardRefused, "a dropped subset");
    assert!(guard_message(&r.unwrap_err()).contains("engine run(s) where the approved"));

    let mut merged = hand.clone();
    merged
        .source_partitions
        .insert("payments".into(), vec![0, 2]);
    assert_eq!(
        exit_of(&check_stated(&merged)),
        ExitCode::GuardRefused,
        "merged"
    );

    let mut swapped = hand.clone();
    swapped.source_partitions.insert("orders".into(), vec![1]);
    swapped
        .source_partitions
        .insert("payments".into(), vec![0, 2]);
    let r = check_stated(&swapped);
    assert_eq!(exit_of(&r), ExitCode::GuardRefused, "swapped");
    assert!(guard_message(&r.unwrap_err()).contains("selection nobody approved"));
}

/// **Refused as soon as the manifest is read**: a window no segment of a
/// selected partition overlaps — an empty restore is never a pass. Exit 3
/// from the same function plan construction binds with.
#[test]
fn an_empty_selection_is_refused_on_resolution() {
    // Every segment ends by FLOOR + 32 min; a window starting at FLOOR + 40
    // min selects nothing.
    let empty = spec(&window(FLOOR_MS + 2_400_000, END_MS));
    let r = resolve_selection(&empty, &mapping(), &facts());
    assert_eq!(exit_of(&r), ExitCode::GuardRefused);
    assert!(guard_message(&r.unwrap_err()).contains("the selection is empty"));
    assert_eq!(
        exit_of(&build_plan(
            &empty,
            &set(),
            &mapping(),
            &facts(),
            "01J9X",
            None
        )),
        ExitCode::GuardRefused
    );
}

/// Inclusive at the start: a start equal to a segment's LAST timestamp still
/// selects that segment (the engine's closed-interval overlap), and one
/// millisecond later does not. KILLS: `<` for `<=` in the overlap.
#[test]
fn a_start_at_a_segments_last_record_still_selects_it() {
    // Partition 2's segments end at FLOOR + 32 min, the last of all; a start
    // exactly there still selects them, one millisecond later nothing does.
    let at_end = spec(&window(FLOOR_MS + 1_920_000, END_MS));
    let r = resolve_selection(&at_end, &mapping(), &facts())
        .unwrap()
        .expect("a selection");
    assert_eq!(r.segment_keys().len(), 3, "{:?}", r.segment_keys());
    let after = spec(&window(FLOOR_MS + 1_920_001, END_MS));
    assert_eq!(
        exit_of(&resolve_selection(&after, &mapping(), &facts())),
        ExitCode::GuardRefused
    );
}

/// **Phase 4 samples from the stated start, and (for a hand-built subset
/// selection, the shape OD-9's decision would reach) only from selected
/// partitions.** The sample window starts at the stated start when the spec's
/// sample window starts earlier — the window the scorecard signs as
/// `sample.window_start`. The control is the same archive with no selection.
/// KILLS: the archive's or the spec's start signed for a narrowed restore; a
/// candidate from an unselected partition.
#[test]
fn phase4_samples_from_the_stated_start_and_only_selected_partitions() {
    use logweir::drill::phase4_sample;
    let s = selecting();
    let r = resolve_selection(&s, &mapping(), &facts())
        .unwrap()
        .expect("a selection");
    let topics: Vec<String> = mapping().keys().cloned().collect();
    let sel =
        phase4_sample::run_selected(&facts(), &s.sample, &topics, Some(&r.selection)).unwrap();
    assert_eq!(
        sel.per_partition.len(),
        9,
        "every partition: a start narrows no partition"
    );
    assert_eq!(
        sel.window.0.timestamp_millis(),
        START_MS,
        "the stated start"
    );
    assert!(sel.per_partition.iter().all(|p| p.window.0 == START_MS));

    let mut subset = r.selection.clone();
    subset.partitions = [
        ("orders".to_string(), [0, 2].into_iter().collect()),
        ("payments".to_string(), [1].into_iter().collect()),
    ]
    .into_iter()
    .collect();
    let sel = phase4_sample::run_selected(&facts(), &s.sample, &topics, Some(&subset)).unwrap();
    let picked: Vec<(String, i32)> = sel
        .per_partition
        .iter()
        .map(|p| (p.topic.clone(), p.partition))
        .collect();
    assert_eq!(
        picked,
        vec![
            ("audit".to_string(), 0),
            ("audit".to_string(), 1),
            ("audit".to_string(), 2),
            ("orders".to_string(), 0),
            ("orders".to_string(), 2),
            ("payments".to_string(), 1),
        ]
    );

    let full = phase4_sample::run(&facts(), &s.sample, &topics).unwrap();
    assert_eq!(full.per_partition.len(), 9, "the control: every partition");
    assert_eq!(
        full.window.0.timestamp_millis(),
        FLOOR_MS,
        "the spec's window"
    );
}
