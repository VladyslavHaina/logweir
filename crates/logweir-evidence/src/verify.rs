//! Verification moved to `logweir-verify` and is re-exported here.
//!
//! `verify_detached` is the whole reason the split exists: a component that
//! VERIFIES a signature must not thereby link the signer
//! (`scripts/check-one-signer.sh:46-49`; ADR 0008 §E). This module exists so
//! that every existing `logweir_evidence::verify::verify_detached` call site
//! compiles unchanged.
pub use logweir_verify::verify::verify_detached;
