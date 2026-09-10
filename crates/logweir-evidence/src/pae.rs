//! DSSE PAE moved to `logweir-verify` and is re-exported here.
//!
//! The encoding is needed by BOTH halves — `sign_detached` and
//! `verify_detached` both call it — and a verifier that had to link the
//! signer to get at the encoding would defeat the extraction. It therefore
//! lives with the verifying half, which is the half a component may link
//! without linking the signer.
//!
//! This module exists so that every existing `logweir_evidence::pae::pae`
//! call site compiles unchanged (ADR 0008 §E).
pub use logweir_verify::pae::pae;
