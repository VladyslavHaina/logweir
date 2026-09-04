//! Phase 6 — run the restore, measure the wall clock, and refuse to score a
//! no-op.
use chrono::{DateTime, Utc};
use logweir_core::engine::{DataEngine, PhaseObserver, RestorePlan};
use logweir_kafka::reader::ClusterReader;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct Restored {
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub restored_end_offsets: BTreeMap<String, Vec<(i32, i64)>>,
    /// Every `Ignoring unknown config key <path>` the restore subprocess
    /// logged (Task 18 fix round 1, review finding F8) —
    /// `RestoreFacts.unknown_key_warnings` was otherwise dropped here, and
    /// `Levers.unknown_key_warnings` (`logweir-core/src/scorecard.rs`)
    /// documents "every path the engine logged", a guarantee nothing
    /// downstream could keep if phase 6 discarded its own copy. No consumer
    /// reads this field yet — Task 21a/22 still have to wire it into the
    /// scorecard — but it can no longer be lost here.
    pub unknown_key_warnings: Vec<String>,
}

/// A restore that wrote nothing is not a restore. `dry_run` is refused by the
/// phase-0 guard and never rendered by C4, but `RestoreOptions.dry_run` is an
/// ordinary YAML field (config.rs:776-778), so one arriving by any other route
/// would produce a no-op, exit 0 and a meaningless RTO. This is the backstop.
///
/// Two distinct states reach this function as failures, and Task 18 fix round
/// 1 (review finding F5) stopped conflating them into one message:
/// - An EMPTY `end_offsets` means no destination topic was ever selected, so
///   nothing is known about whether a restore even ran — an upstream
///   guard/spec defect. Maps to `DrillError::Operational` (exit 1, no
///   artifact): correct, because this genuinely says nothing about the
///   archive.
/// - A NON-EMPTY `end_offsets` whose every partition is still `<= 0` is a
///   positively established fact ABOUT the archive: the restore ran, exited
///   0, the target was read successfully, and nothing landed. Per review
///   finding F1, that is a drill RESULT, not an operational failure, so it
///   maps to `DrillError::RestoreNoOp` — see that variant's doc comment in
///   `crate::drill::DrillError` for the exit-2 routing this requires from the
///   orchestrator.
pub fn assert_post_condition(
    end_offsets: &BTreeMap<String, Vec<(i32, i64)>>,
) -> Result<(), crate::drill::DrillError> {
    if end_offsets.is_empty() {
        return Err(crate::drill::DrillError::Operational(
            "post-condition inconclusive: no destination topic was selected for restore (the \
             topic mapping is empty), so nothing can be concluded about whether the restore ran. \
             This is an upstream guard/spec defect, not a finding about the restore itself."
                .into(),
        ));
    }
    let any = end_offsets.values().flatten().any(|(_, hi)| *hi > 0);
    if any {
        return Ok(());
    }
    Err(crate::drill::DrillError::RestoreNoOp(
        "post-condition failed: every selected partition on the target has end offset 0 after \
         the restore. The restore was a no-op (a `dry_run` reached the engine, or the selected \
         window held no records). Refusing to score as a pass."
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
        unknown_key_warnings: facts.unknown_key_warnings,
    })
}
