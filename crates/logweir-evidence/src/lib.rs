#![forbid(unsafe_code)]
//! The SIGNING half of Logweir's DSSE evidence, and nothing else.
//!
//! `VerifyingKey`, `verify_detached`, `pae`, `Signature`, `Sidecar`, `Error`
//! and the payload-type constants moved to `crates/logweir-verify` so
//! that a component which VERIFIES a signature does not thereby link the
//! signer — the remedy `scripts/check-one-signer.sh:46-49` ruled in writing
//! before there was anything to remedy, recorded in ADR 0008 §E. What stays
//! here is `SigningKey`, `KeyAlg`, `KeyOrigin`, `sign_detached`,
//! `generate_p256`, `generate_ed25519`, `SigningKey::from_pem_file`,
//! `SigningKey::load_or_generate` and `to_pkcs8_pem` — and the
//! `rand_core`/`getrandom` entropy source they need, which `logweir-verify`
//! does not declare.
//!
//! The glob re-export below is deliberate and load-bearing: every existing
//! call site in this workspace — `logweir_evidence::PAYLOAD_TYPE_SCORECARD`,
//! `logweir_evidence::Sidecar`, `logweir_evidence::Error`,
//! `logweir_evidence::keys::VerifyingKey`,
//! `logweir_evidence::verify::verify_detached`,
//! `logweir_evidence::pae::pae` — compiles unchanged across the extraction.
//! `crates/logweir-verify/src/lib.rs` is where the constants are now
//! DECLARED, and `docs/test_verify_scorecard.py` reads that file: a
//! re-export contains none of the media-type literals.
//!
//! THERE ARE FOUR OF THEM SINCE TASK 5. Three moved in Task 14
//! (`PAYLOAD_TYPE_SCORECARD`, `_TEARDOWN`, `_PUT_RECEIPT`) and
//! `PAYLOAD_TYPE_BACKUP_RECEIPT` was declared there, beside them, by Task 5 —
//! never here (critique A F8, critique B H2). The `pub use` below re-exports
//! all four, so `logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT` resolves for
//! `examples/mint_backup_receipt_fixture.rs`, which signs with it, and no
//! call site has to know which crate declares what.
pub mod keys;
pub mod pae;
pub mod sign;
pub mod verify;

pub use logweir_verify::*;
