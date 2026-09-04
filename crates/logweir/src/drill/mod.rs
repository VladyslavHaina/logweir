//! The eleven-phase orchestrator (-1..=9). Phase -1 ships in v0.1 — Global Constraint 18 (reversed 2026-09-03) and
//! docs/adr/0007-from-cluster-in-v0.1.md.
pub mod phase0_admit;
pub mod phase1_approval;
pub mod phase2_target;
pub mod phase3_diff;
pub mod phase4_sample;
pub mod phase5_preflight;
pub mod phase6_restore;

#[derive(Debug, thiserror::Error)]
pub enum DrillError {
    /// The drill could not be attempted or continued, for a reason that says
    /// nothing about the archive. Maps to ExitCode::Operational (1).
    #[error("operational: {0}")]
    Operational(String),
    /// A guard refused the plan before anything ran. Maps to
    /// ExitCode::GuardRefused (3).
    #[error("guard: {0}")]
    Guard(#[from] logweir_core::guard::GuardRefusal),
    #[error("kafka: {0}")]
    Kafka(#[from] logweir_kafka::reader::KafkaError),
    #[error("engine: {0}")]
    Engine(#[from] logweir_core::engine::EngineError),
    /// A drill RESULT, not an operational failure (Task 18 fix round 1,
    /// review finding F1): the restore subprocess ran, exited 0, the target
    /// cluster was read successfully for every selected destination topic,
    /// and every partition was still at end offset <= 0. That is a
    /// positively established fact ABOUT the archive — the backup does not
    /// actually restore — so it must NOT be routed like `Operational` (exit
    /// 1, no artifact). It mirrors phase 5's `Verdict::Block`: the
    /// orchestrator (Task 21a) must catch this variant at the phase-6 call
    /// site, BEFORE the generic `record(...)?` short-circuit, build and sign
    /// a scorecard from it, and return `DrillError::NotPass(Box::new(signed))`
    /// so it reaches ExitCode::DrillNotPass (2). See `task-21a-addendum.md`
    /// ruling A8 for the required orchestrator wiring. If this variant ever
    /// reaches `impl From<DrillError> for ExitCode` unhandled (or handled by
    /// a catch-all that maps it to `Operational`), that is the exact defect
    /// this comment exists to prevent.
    #[error("drill-not-pass: {0}")]
    RestoreNoOp(String),
}
