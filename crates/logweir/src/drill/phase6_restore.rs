//! Phase 6 — run the restore, measure the wall clock, and refuse to score a
//! no-op.
use chrono::{DateTime, Utc};
use logweir_core::engine::{DataEngine, PhaseObserver, RestorePlan};
use logweir_kafka::reader::ClusterReader;
use std::collections::BTreeMap;

pub struct Restored {
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub restored_end_offsets: BTreeMap<String, Vec<(i32, i64)>>,
}

/// A restore that wrote nothing is not a restore. `dry_run` is refused by the
/// phase-0 guard and never rendered by C4, but `RestoreOptions.dry_run` is an
/// ordinary YAML field (config.rs:776-778), so one arriving by any other route
/// would produce a no-op, exit 0 and a meaningless RTO. This is the backstop.
pub fn assert_post_condition(
    end_offsets: &BTreeMap<String, Vec<(i32, i64)>>,
) -> Result<(), crate::drill::DrillError> {
    let any = end_offsets.values().flatten().any(|(_, hi)| *hi > 0);
    if any {
        return Ok(());
    }
    Err(crate::drill::DrillError::Operational(
        "post-condition failed: every selected partition on the target has end offset 0 after \
         the restore. The restore was a no-op (a `dry_run` reached the engine, or the selected \
         window held no records). Refusing to score."
            .into(),
    ))
}

pub fn run(
    engine: &dyn DataEngine,
    plan: &RestorePlan,
    reader: &dyn ClusterReader,
    mapping: &BTreeMap<String, String>,
    obs: &mut dyn PhaseObserver,
) -> Result<Restored, crate::drill::DrillError> {
    // The engine measures nothing for us: `restore` has no --format, writes no
    // report file, and its only machine-readable signal is the exit code
    // (main.rs:47-51). Both timestamps below are Logweir's own clock.
    let facts = engine.restore(plan, obs)?;

    let mut restored_end_offsets = BTreeMap::new();
    for dst in mapping.values() {
        restored_end_offsets.insert(dst.clone(), reader.end_offsets(dst)?);
    }
    assert_post_condition(&restored_end_offsets)?;

    Ok(Restored {
        started_at: facts.started_at,
        finished_at: facts.finished_at,
        restored_end_offsets,
    })
}
