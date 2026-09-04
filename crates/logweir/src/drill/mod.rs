//! The eleven-phase orchestrator (-1..=9). Phase -1 ships in v0.1 — Global Constraint 18 (reversed 2026-09-03) and
//! docs/adr/0007-from-cluster-in-v0.1.md.
pub mod phase0_admit;

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
}
