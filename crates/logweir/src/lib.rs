//! logweir — the drill orchestrator. Every module here is a LIBRARY module so
//! `crates/logweir/tests/*.rs` can link them (Tasks 15-20 depend on this).
#![forbid(unsafe_code)]
pub mod approve;
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
