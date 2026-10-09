//! logweir-kafka — the fingerprint (always available) and, behind the `client`
//! feature added in Task 10, the only broker-dialling code in the workspace.
#![forbid(unsafe_code)]
pub mod access;
pub mod acls;
pub mod capture;
pub mod fingerprint;
pub mod groups;
pub mod inventory;
pub mod positions;
pub mod reader;
pub mod token;
pub mod topic_ids;

#[cfg(feature = "client")]
mod rdkafka_admin;
#[cfg(feature = "client")]
mod rdkafka_capture;
#[cfg(feature = "client")]
mod rdkafka_positions;
#[cfg(feature = "client")]
pub mod rdkafka_reader;
