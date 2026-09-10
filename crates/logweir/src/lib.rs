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
pub mod ids;
pub mod metrics;
pub mod schema;
pub mod show;
pub mod verify;
