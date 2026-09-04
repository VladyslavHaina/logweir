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
