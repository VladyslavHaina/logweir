//! **PROD-11.1 — replay selection, the plan side** (guard G-WIN as amended:
//! `docs/to-do/decisions/PROD-11.1-replay-selection.md` §2).
//!
//! 1. **Plan construction** binds a stated `restore.window_start` as the
//!    plan's own start (`InheritedFromSpec`), refuses it before the archive's
//!    floor (never moves it there) and carries the per-topic subsets.
//! 2. **Phase 5** re-derives the start and the engine runs from the SPEC and
//!    the manifest — never from the plan's claim — and refuses a rendered
//!    document that disagrees: a start moved to the floor (a silent widening),
//!    a subset dropped, merged or swapped between topics.
//! 3. **Resolution before phase 2**: a partition the archive does not list and
//!    a selection no segment overlaps are exit 3.
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
/// The window's end (`sample.window_end`; no point in time is stated).
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

fn selecting() -> DrillSpec {
    spec(&format!(
        "restore:\n  window_start: \"{}\"\n  partitions:\n    orders: [2, 0]\n    payments: [1]\n",
        rfc3339(START_MS)
    ))
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

/// The plan binds the STATED start as its own, says so, and carries the
/// subsets sorted; phase 5 accepts what plan construction built.
#[test]
fn a_stated_start_and_subsets_are_bound_into_the_plan() {
    let s = selecting();
    let plan = build_plan(&s, &set(), &mapping(), &facts(), "01J9X", None).expect("builds");
    assert_eq!(plan.time_window.0.timestamp_millis(), START_MS);
    assert_eq!(plan.time_window.1.timestamp_millis(), END_MS);
    assert_eq!(
        plan.window_floor_source,
        WindowFloorSource::InheritedFromSpec
    );
    assert_eq!(
        plan.source_partitions,
        [
            ("orders".to_string(), vec![0, 2]),
            ("payments".to_string(), vec![1])
        ]
        .into_iter()
        .collect::<BTreeMap<_, _>>()
    );
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

/// **A start before the archive's coverage is refused, never widened to the
/// floor**: at plan construction, at resolution and at phase 5, each from its
/// own reading. KILLS: clamping the start to the floor, comparing with `<=`
/// (a start AT the floor is admitted), reading the floor of all topics.
#[test]
fn a_start_before_coverage_is_refused_everywhere_and_never_moved() {
    let early = spec(&format!(
        "restore:\n  window_start: \"{}\"\n",
        rfc3339(FLOOR_MS - 1)
    ));
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
    let at = spec(&format!(
        "restore:\n  window_start: \"{}\"\n",
        rfc3339(FLOOR_MS)
    ));
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
/// the spec may not hold one earlier than the manifest floor it was handed.
#[test]
fn an_inherited_start_before_the_floor_is_refused_at_construction() {
    let r = build_plan_with_floor(
        &selecting(),
        &set(),
        &mapping(),
        "01J9X",
        WindowFloor {
            start: ts(FLOOR_MS - 60_000),
            source: WindowFloorSource::InheritedFromSpec,
            manifest_floor_ms: FLOOR_MS,
        },
        None,
    );
    assert_eq!(exit_of(&r), ExitCode::GuardRefused);
    assert!(guard_message(&r.unwrap_err()).contains("Refused rather than moved"));
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
            "rendered time_window_start {FLOOR_MS} is not the approved restore.window_start \
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

/// **The partition filter, caught at phase 5.** A plan that dropped a subset
/// (the topic would restore every partition), merged two subsets into one run
/// (the engine would filter both topics by one list) or swapped them between
/// topics is refused before the engine is handed it. KILLS: phase 5 checking
/// the start only; checking the count of runs only.
#[test]
fn phase5_refuses_a_rendered_partition_selection_that_is_not_the_approved_one() {
    let s = selecting();
    let good = build_plan(&s, &set(), &mapping(), &facts(), "01J9X", None).unwrap();
    // Two different subsets and one unrestricted topic: three runs.
    assert_eq!(logweir_engine_oso::render_restore::runs(&good).len(), 3);

    let mut dropped = good.clone();
    dropped.source_partitions.remove("payments");
    let r = check(&dropped, &s);
    assert_eq!(exit_of(&r), ExitCode::GuardRefused, "a dropped subset");
    assert!(guard_message(&r.unwrap_err()).contains("engine run(s) where the approved"));

    let mut merged = good.clone();
    merged
        .source_partitions
        .insert("payments".into(), vec![0, 2]);
    let r = check(&merged, &s);
    assert_eq!(exit_of(&r), ExitCode::GuardRefused, "two subsets merged");

    let mut swapped = good.clone();
    swapped.source_partitions.insert("orders".into(), vec![1]);
    swapped
        .source_partitions
        .insert("payments".into(), vec![0, 2]);
    let r = check(&swapped, &s);
    assert_eq!(exit_of(&r), ExitCode::GuardRefused, "subsets swapped");
    assert!(
        guard_message(&r.unwrap_err()).contains("selection nobody approved"),
        "the refusal names the partition selection"
    );
}

/// **Refused as soon as the manifest is read**: a subset partition the
/// archive does not list, and a selection no segment of a selected partition
/// overlaps — an empty restore is never a pass. Both exit 3 from the same
/// function plan construction binds with.
#[test]
fn an_unlisted_partition_and_an_empty_selection_are_refused_on_resolution() {
    let unlisted = spec("restore:\n  partitions:\n    orders: [0, 7]\n");
    let r = resolve_selection(&unlisted, &mapping(), &facts());
    assert_eq!(exit_of(&r), ExitCode::GuardRefused);
    assert!(guard_message(&r.unwrap_err()).contains("names partition 7"));
    assert_eq!(
        exit_of(&build_plan(
            &unlisted,
            &set(),
            &mapping(),
            &facts(),
            "01J9X",
            None
        )),
        ExitCode::GuardRefused
    );

    // Every segment ends by FLOOR + 32 min; a window starting at FLOOR + 40
    // min selects nothing.
    let empty = spec(&format!(
        "restore:\n  window_start: \"{}\"\n",
        rfc3339(FLOOR_MS + 2_400_000)
    ));
    let r = resolve_selection(&empty, &mapping(), &facts());
    assert_eq!(exit_of(&r), ExitCode::GuardRefused);
    assert!(guard_message(&r.unwrap_err()).contains("the selection is empty"));
}

/// Inclusive at the start: a start equal to a segment's LAST timestamp still
/// selects that segment (the engine's closed-interval overlap), and one
/// millisecond later does not. KILLS: `<` for `<=` in the overlap.
#[test]
fn a_start_at_a_segments_last_record_still_selects_it() {
    // orders/0's segment ends at FLOOR + 30 min; orders/1's at +31, /2's at
    // +32. Restrict to orders/0 and start exactly at its end.
    let at_end = spec(&format!(
        "restore:\n  window_start: \"{}\"\n  partitions:\n    orders: [0]\n    payments: [0]\n    audit: [0]\n",
        rfc3339(FLOOR_MS + 1_800_000)
    ));
    let r = resolve_selection(&at_end, &mapping(), &facts())
        .unwrap()
        .expect("a selection");
    assert_eq!(r.segment_keys().len(), 3, "{:?}", r.segment_keys());
    let after = spec(&format!(
        "restore:\n  window_start: \"{}\"\n  partitions:\n    orders: [0]\n    payments: [0]\n    audit: [0]\n",
        rfc3339(FLOOR_MS + 1_800_001)
    ));
    assert_eq!(
        exit_of(&resolve_selection(&after, &mapping(), &facts())),
        ExitCode::GuardRefused
    );
}
