//! logweir — the drill orchestrator. Every module here is a LIBRARY module so
//! `crates/logweir/tests/*.rs` can link them (Tasks 15-20 depend on this).
#![forbid(unsafe_code)]
pub mod cli;
pub mod doctor;
pub mod drill;
pub mod exit;
pub mod schema;
pub mod show;
pub mod verify;
