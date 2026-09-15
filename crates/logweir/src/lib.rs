//! logweir — the drill orchestrator. Every module here is a LIBRARY module so
//! `crates/logweir/tests/*.rs` can link them (Tasks 15-20 depend on this).
#![forbid(unsafe_code)]
pub mod approve;
/// GC18's phase −1: `logweir backup run`, the source-side capture.
pub mod backup;
pub mod cli;
pub mod doctor;
pub mod drill;
/// The ONE engine-binary resolution `doctor` and `drill run` both consult.
pub mod engine_bin;
pub mod exit;
/// Short-lived Kubernetes installation identity bootstrap. This remains in
/// the signer-capable runner binary; the long-lived controller never links it.
pub mod identity;
pub mod ids;
pub mod metrics;
/// Interface **I14**: `logweir cluster-probe`, the liveness probe the
/// `KafkaCluster` reconciler runs as a Job. It is a subcommand of THIS binary
/// and not an engine command (GC3): the engine's four reachable subcommands are
/// unchanged by it.
pub mod probe;
pub mod schema;
pub mod show;
mod signer;
pub mod verify;
