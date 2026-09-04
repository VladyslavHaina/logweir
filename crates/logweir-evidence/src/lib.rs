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
    /// The sidecar or its signature bytes are structurally invalid: base64
    /// that will not decode, a signature blob that is not valid DER (P-256)
    /// or is the wrong length (Ed25519), or a `payloadType` that does not
    /// match what the caller asked to verify. This is evidence of CORRUPTION
    /// — a truncated file, the wrong file, a bad encoding — never evidence
    /// that a genuine, well-formed document was tampered with after signing.
    /// A caller mapping this crate's errors onto an exit-code contract
    /// SHOULD treat this variant as an operational failure, not as a proof
    /// of tampering.
    #[error("malformed signature data: {0}")]
    Malformed(String),
    /// A structurally valid signature was checked against the payload and
    /// either did not verify, or no signature in the sidecar was made by the
    /// presented key. This is the "tampered" fact: the crypto ran and gave a
    /// definite negative answer.
    #[error("verification failed: {0}")]
    Verify(String),
}
