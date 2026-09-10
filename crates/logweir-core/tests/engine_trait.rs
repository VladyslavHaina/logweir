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
