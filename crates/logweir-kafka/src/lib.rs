//! logweir-kafka — the fingerprint (always available) and, behind the `client`
//! feature added in Task 10, the only broker-dialling code in the workspace.
#![forbid(unsafe_code)]
pub mod fingerprint;
pub mod inventory;
pub mod reader;
pub mod token;

#[cfg(feature = "client")]
pub mod rdkafka_reader;
