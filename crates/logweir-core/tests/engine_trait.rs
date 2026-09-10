// crates/logweir-core/tests/engine_trait.rs
// The trait must stay object-safe, and a signature change must become a compile
// error rather than a surprise at the call site in Task 12.
use logweir_core::engine::*;

struct NullEngine;

impl DataEngine for NullEngine {
    fn id(&self) -> EngineId {
        unimplemented!()
    }
    fn list_backup_sets(&self, _: &StorageUrl) -> Result<Vec<BackupSetRef>, EngineError> {
        unimplemented!()
    }
    fn describe(&self, _: &BackupSetRef) -> Result<BackupSetFacts, EngineError> {
        unimplemented!()
    }
    fn preflight(&self, _: &RestorePlan) -> Result<PreflightReport, EngineError> {
        unimplemented!()
    }
    fn restore(
        &self,
        _: &RestorePlan,
        _: &mut dyn PhaseObserver,
    ) -> Result<RestoreFacts, EngineError> {
        unimplemented!()
    }
    fn fingerprints(&self, _: &SampleSelection) -> Result<Vec<RecordFingerprint>, EngineError> {
        unimplemented!()
    }
}

#[test]
fn the_data_engine_trait_is_object_safe() {
    let _: &dyn DataEngine = &NullEngine;
}

/// Task 19 added `validation_run` to this trait WITH a default body specifically
/// so `NullEngine` above (and every other pre-existing implementor) keeps
/// compiling unchanged — see `DataEngine::validation_run`'s own doc comment.
/// That default must fail loudly (`Operational`), never fabricate a
/// plausible-looking `Ok(EngineRun { exit_code: 0 })`: a silent fake pass here
/// would let a caller believe an engine validation ran when it did not.
#[test]
fn validation_run_defaults_to_an_operational_refusal_not_a_fabricated_pass() {
    let plan = RestorePlan {
        set: BackupSetRef {
            backup_id: "b".into(),
            manifest_key: "b/manifest.json".into(),
        },
        storage: StorageUrl::Filesystem {
            path: "/tmp".into(),
        },
        target_bootstrap: vec!["broker:9092".into()],
        target_auth: logweir_core::engine::AuthRender::Plaintext,
        topic_mapping: Default::default(),
        // A fixed timestamp, not `Utc::now()` — Task 19 fix round 1, review
        // finding F14: `logweir-core` is otherwise clock-free by convention
        // (Global Constraint 1 governs the library; this is a test, but there
        // is no reason to be the one place that reaches for the clock when a
        // fixed value is exactly as good here).
        time_window: (
            "2026-08-29T00:00:00Z".parse().unwrap(),
            "2026-08-30T02:00:00Z".parse().unwrap(),
        ),
        window_floor_source: WindowFloorSource::ArchiveManifest,
        default_replication_factor: 1,
        checkpoint_state: "/tmp/checkpoint.json".into(),
        checkpoint_interval_secs: 30,
        offset_report: "/var/lib/logweir/01J9X/offsets.json".into(),
    };
    let err = NullEngine.validation_run(&plan).unwrap_err();
    assert!(matches!(err, EngineError::Operational(_)));
}

/// Task 4 added `backup` to this trait WITH a default body, for the same
/// reason `validation_run` has one: `NullEngine` above and every other
/// pre-existing implementor keeps compiling unchanged.
///
/// That default must fail loudly, never fabricate `Ok(BackupFacts { exit_code:
/// 0, .. })`. A silent fake pass here is worse than the `validation_run` case
/// it mirrors: `crates/logweir/src/backup/phase_run.rs` reads the ARCHIVE
/// immediately after this call returns, so a fabricated clean exit would put a
/// real `records_per_topic` and a real covered window — read from whatever
/// archive happens to be at the configured location — behind a backup that
/// never ran.
#[test]
fn default_data_engine_backup_is_operational() {
    struct Obs;
    impl PhaseObserver for Obs {
        fn phase_started(&mut self, _: i8, _: &str) {}
        fn phase_finished(&mut self, _: i8, _: &str) {}
        fn engine_line(&mut self, _: &str, _: &str) {}
    }
    let plan = BackupPlan {
        backup_id: "mvp-demo".into(),
        source_bootstrap: vec!["broker:9092".into()],
        source_auth: AuthRender::Plaintext,
        topics: vec!["orders".into()],
        storage: StorageUrl::Filesystem {
            path: "/tmp".into(),
        },
        compression: "zstd".into(),
        segment_max_records: 1000,
        segment_max_bytes: 10_485_760,
        max_concurrent_partitions: 3,
    };
    // `matches!` on the whole `Result`, not `unwrap_err()`: a mutant that
    // returns `Ok(BackupFacts { exit_code: 0, .. })` must fail HERE, at the
    // assertion, and `unwrap_err()` would instead panic one line earlier with
    // rustc's own "called `Result::unwrap_err()` on an `Ok` value" — a real
    // failure, but not the one this test is about, and not a message that
    // names what went wrong.
    let r = NullEngine.backup(&plan, &mut Obs);
    assert!(
        matches!(r, Err(EngineError::Operational(_))),
        "the default body must refuse operationally, never fabricate a clean BackupFacts; \
         got {r:?}"
    );
}

// ---------------------------------------------------------------------------
// Task 10 — `expected_restored_count`, guard **G-WIN**'s second half.

/// Epoch milliseconds, each computed with
/// `python3 -c 'import datetime as d; ...'` before being written here (plan
/// errata E6/E7) and spelled beside its date:
const FLOOR_MS: i64 = 1_788_220_800_000; // 2026-09-01T00:00:00Z
const MID_MS: i64 = 1_788_242_400_000; // 2026-09-01T06:00:00Z
const NEAR_PIT_MS: i64 = 1_788_260_400_000; // 2026-09-01T11:00:00Z
const PIT_MS: i64 = 1_788_264_000_000; // 2026-09-01T12:00:00Z
const PAST_PIT_MS: i64 = 1_788_267_600_000; // 2026-09-01T13:00:00Z

fn facts_with_segments(segs: Vec<SegmentFacts>) -> BackupSetFacts {
    BackupSetFacts {
        backup_id: "backup-bound-test".into(),
        created_at: chrono::DateTime::from_timestamp_millis(PAST_PIT_MS).unwrap(),
        source_cluster_id: None,
        manifest_sha256: "sha256:0".into(),
        manifest_version_id: None,
        consumer_group_snapshot_sha256: None,
        topics: vec![TopicFacts {
            name: "orders".into(),
            original_partition_count: Some(1),
            source_replication_factor: Some(3),
            configurations: std::collections::BTreeMap::new(),
            partitions: vec![PartitionFacts {
                partition_id: 0,
                segments: segs,
                gaps: vec![],
                pruned: vec![],
            }],
        }],
    }
}

fn seg(start_ms: i64, end_ms: i64, record_count: i64) -> SegmentFacts {
    SegmentFacts {
        key: format!("archive/orders/0/{start_ms}.kbak"),
        start_offset: 0,
        end_offset: record_count.max(0) - 1,
        start_timestamp: start_ms,
        end_timestamp: end_ms,
        record_count,
        sha256: String::new(),
        uploaded_at: end_ms,
    }
}

/// **Guard G-WIN, the bound's arithmetic.** A segment wholly inside the window
/// is what the manifest PROVES is there and counts towards both bounds; a
/// segment straddling either bound counts towards `upper` only, because the
/// manifest carries no per-record timestamp and cannot say how many of its
/// records fall on the window's side of the boundary.
///
/// Kills the mutant "count straddling segments into `lower` as well as
/// `upper`", which returns `(150, 150)` here.
#[test]
fn expected_restored_count_separates_whole_from_straddling_segments() {
    let facts = facts_with_segments(vec![
        // Wholly inside: [06:00, 11:00] within [00:00, 12:00].
        seg(MID_MS, NEAR_PIT_MS, 100),
        // Straddles `pit_ms`: starts before 12:00 and ends after it.
        seg(NEAR_PIT_MS, PAST_PIT_MS, 50),
    ]);
    assert_eq!(
        expected_restored_count(&facts, FLOOR_MS, PIT_MS),
        (100, 150),
        "the straddling segment's 50 records belong to `upper` only"
    );

    // A NEGATIVE `record_count` contributes 0, not a wrapped `u64`. Both
    // bounds must lose exactly the 100 the wholly-inside segment carried, and
    // neither may come back near `u64::MAX`.
    let negative = facts_with_segments(vec![
        seg(MID_MS, NEAR_PIT_MS, -100),
        seg(NEAR_PIT_MS, PAST_PIT_MS, 50),
    ]);
    assert_eq!(
        expected_restored_count(&negative, FLOOR_MS, PIT_MS),
        (0, 50),
        "a negative record_count contributes 0 rather than wrapping"
    );
}

/// The three arms the bound's arithmetic needs beyond the split above, each of
/// which a plausible mutant gets wrong on its own.
#[test]
fn expected_restored_count_treats_the_window_as_closed_and_ignores_segments_outside_it() {
    // Both bounds INCLUSIVE: a segment touching `floor_ms` and `pit_ms`
    // exactly is wholly INSIDE, not straddling.
    let flush = facts_with_segments(vec![seg(FLOOR_MS, PIT_MS, 400)]);
    assert_eq!(
        expected_restored_count(&flush, FLOOR_MS, PIT_MS),
        (400, 400),
        "the restore window's end is inclusive (docs/stability.md), so a segment ending \
         exactly on it is inside it"
    );

    // A segment that straddles the FLOOR counts towards `upper` only, exactly
    // as one straddling the point-in-time does.
    let below = facts_with_segments(vec![seg(FLOOR_MS - 1, MID_MS, 70)]);
    assert_eq!(expected_restored_count(&below, FLOOR_MS, PIT_MS), (0, 70));

    // A segment overlapping the window NOWHERE contributes to NEITHER bound:
    // counting it into `upper` would let an arbitrarily large archive make the
    // upper bound unfalsifiable.
    let outside = facts_with_segments(vec![
        seg(MID_MS, NEAR_PIT_MS, 100),
        seg(PAST_PIT_MS, PAST_PIT_MS + 1, 9_000),
        seg(FLOOR_MS - 10, FLOOR_MS - 1, 9_000),
    ]);
    assert_eq!(
        expected_restored_count(&outside, FLOOR_MS, PIT_MS),
        (100, 100)
    );

    // No segment in the window at all is `(0, 0)` — an empty bound, never a
    // fabricated expectation.
    let empty = facts_with_segments(vec![]);
    assert_eq!(expected_restored_count(&empty, FLOOR_MS, PIT_MS), (0, 0));
}
