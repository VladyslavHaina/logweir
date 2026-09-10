#![forbid(unsafe_code)]
//! Verification without signing.
//!
//! Everything in this crate answers the question "does this sidecar verify
//! over these bytes?" and nothing in it can answer "sign these bytes". The
//! signing half — `SigningKey`, `KeyAlg`, `sign_detached`, `generate_p256`,
//! `generate_ed25519`, `to_pkcs8_pem` — stays in `logweir-evidence`, which
//! re-exports this crate wholesale so every existing call site compiles
//! unchanged.
//!
//! WHY A CRATE AND NOT A FEATURE. `scripts/check-one-signer.sh:46-49` ruled
//! the remedy before there was anything to remedy: "`weirkeeper` … does not go
//! on the allowlist — the ruled remedy is to extract a verify-only crate, so
//! that a controller which VERIFIES a signature does not thereby link the
//! signer." `logweir-evidence`'s `p256`, `ed25519-dalek` and `rand_core` are
//! all non-optional under `[features] default = []`, so
//! `default-features = false` removes nothing; and verification needs BOTH
//! primitives, because `VerifyingKey` is an enum over them and
//! `verify_detached` matches both arms. What the verifying half does not need
//! is the signer's `rand_core`/`getrandom` entropy source, and this crate does
//! not declare it.
//!
//! WHAT THIS DOES NOT PROVE. Nothing here bounds anyone's *capability* to
//! sign. A component that can create a pod in the signing key's namespace can
//! mount the key and sign whatever it likes with no Logweir crate involved
//! (Global Constraint 27, accepted as residual **O1**/O0 default (a)). This
//! crate makes a LINKAGE claim, and `scripts/check-withdrawn-claim.sh` keeps
//! the stronger, withdrawn claim off every shipped surface.

pub mod keys;
pub mod pae;
pub mod verify;

use serde::{Deserialize, Serialize};

pub const PAYLOAD_TYPE_SCORECARD: &str =
    "application/vnd.logweir.drill-scorecard+json;version=1.0.0";
pub const PAYLOAD_TYPE_TEARDOWN: &str = "application/vnd.logweir.drill-teardown+json;version=1.0.0";
/// The post-put storage receipt (Task 21a, discharging Task 20's carried
/// obligation). A scorecard is SIGNED before it is PUT — bytes cannot be
/// signed before they are serialised — so `evidence.create_only_enforced`,
/// `immutable`, `retain_until` and `version_id` are unknowable at signing
/// time and the scorecard neutralises all four. This second document carries
/// the readback taken AFTER the put, signed on its own, so the storage claim
/// is something an auditor can check rather than something only Logweir's
/// memory ever held.
pub const PAYLOAD_TYPE_PUT_RECEIPT: &str =
    "application/vnd.logweir.drill-put-receipt+json;version=1.0.0";
/// The BACKUP receipt (Task 5) — the signed record of one `logweir backup
/// run`, and not to be confused with `PAYLOAD_TYPE_PUT_RECEIPT` above, which
/// is the post-put storage readback of a drill SCORECARD.
///
/// It is a new DOCUMENT rather than new scorecard fields (spec §7, Global
/// Constraint 12 as amended): the scorecard stays frozen at 21 top-level
/// properties and 17 required ones, and top-level information about a
/// different operation on a different cluster ships as its own media type
/// with its own `format_version`. The type is
/// `logweir_core::backup_receipt::BackupReceipt`; the schema is
/// `schemas/logweir-backup-receipt-1.0.0.json`.
///
/// DECLARED HERE, in the verify-only crate, and NOT in `logweir-evidence`
/// (critique A F8, critique B H2, chain V: Task 14 → Task 5). `weirkeeper`
/// links `logweir-verify` and never the signer, and Task 24's verifier has to
/// name this constant in order to check a receipt — so a constant declared on
/// the signing side would have dragged the signer into the control plane to
/// serve a string. `docs/test_verify_scorecard.py` reads THIS file for the
/// literals, because `logweir-evidence`'s `pub use logweir_verify::*;`
/// contains none of them.
pub const PAYLOAD_TYPE_BACKUP_RECEIPT: &str =
    "application/vnd.logweir.backup-receipt+json;version=1.0.0";

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
    /// The sidecar's signature bytes are structurally invalid: base64 that
    /// will not decode, or a signature blob that is not valid DER (P-256) or
    /// is the wrong length (Ed25519). This is evidence of CORRUPTION — a
    /// truncated file, a bad encoding — never evidence that a genuine,
    /// well-formed document was tampered with after signing. A caller
    /// mapping this crate's errors onto an exit-code contract SHOULD treat
    /// this variant as an operational failure, not as a proof of tampering.
    #[error("malformed signature data: {0}")]
    Malformed(String),
    /// A structurally valid signature was checked against the payload and
    /// either did not verify, or no signature in the sidecar was made by the
    /// presented key — or the sidecar's `payloadType` does not match what
    /// the caller asked to verify, which is evidence of SUBSTITUTION (a
    /// genuinely-signed sidecar for a different kind of document, handed
    /// over in place of this one). Each of these is a definite negative
    /// answer the crypto/protocol layer actually gave, as opposed to
    /// `Malformed`'s "the input was never well-formed enough to ask".
    #[error("verification failed: {0}")]
    Verify(String),
}

// The crate root carries the three names a caller actually reaches for, so
// `logweir_verify::VerifyingKey` and `logweir_verify::verify_detached` work
// without naming the module. The modules stay public: every existing call
// site in this workspace is written as `…::keys::VerifyingKey`,
// `…::pae::pae` and `…::verify::verify_detached`, and `logweir-evidence`'s
// re-export shims preserve exactly those paths.
pub use keys::VerifyingKey;
pub use pae::pae;
pub use verify::verify_detached;
