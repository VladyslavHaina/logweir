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
        topic_mapping: Default::default(),
        time_window: (chrono::Utc::now(), chrono::Utc::now()),
        default_replication_factor: 1,
        checkpoint_state: "/tmp/checkpoint.json".into(),
        checkpoint_interval_secs: 30,
    };
    let err = NullEngine.validation_run(&plan).unwrap_err();
    assert!(matches!(err, EngineError::Operational(_)));
}
