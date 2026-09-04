#![forbid(unsafe_code)]
pub mod keys;
pub mod pae;
pub mod sign;
pub mod verify;

use serde::{Deserialize, Serialize};

pub const PAYLOAD_TYPE_SCORECARD: &str =
    "application/vnd.logweir.drill-scorecard+json;version=1.0.0";
pub const PAYLOAD_TYPE_TEARDOWN: &str = "application/vnd.logweir.drill-teardown+json;version=1.0.0";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signature {
    pub keyid: String,
    pub sig: String,
}

/// The detached sidecar written beside a directly-readable JSON payload, so a
/// human can `cat` the scorecard and a machine can still verify it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sidecar {
    #[serde(rename = "payloadType")]
    pub payload_type: String,
    pub signatures: Vec<Signature>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("key error: {0}")]
    Key(String),
    #[error("verification failed: {0}")]
    Verify(String),
}
