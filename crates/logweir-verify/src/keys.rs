use crate::Error;
use p256::pkcs8::{DecodePublicKey, EncodePublicKey, LineEnding};
use sha2::{Digest, Sha256};
use std::path::Path;

// `KeyAlg` IS DELIBERATELY NOT HERE. It is `SigningKey::alg`'s return type
// (`crates/logweir-evidence/src/keys.rs`), `impl VerifyingKey` below never
// names it, and moving it would drag a signing-side vocabulary into the
// verify-only crate for no caller's benefit.
// `crates/logweir-verify/tests/deps.rs::key_alg_stays_with_the_signer` keeps
// it where it is.

#[derive(Clone)]
pub enum VerifyingKey {
    P256(p256::ecdsa::VerifyingKey),
    Ed25519(ed25519_dalek::VerifyingKey),
}

/// THE WHOLE INHERENT `impl` BLOCK MOVED, and it had to: Rust coherence
/// forbids `logweir-evidence` adding an inherent method to a type it no
/// longer defines. `to_public_key_pem` came with it for that reason and not
/// because verification needs it — it is what mints the checked-in
/// `e2e/fixtures/signed/public.pem` fixture.
impl VerifyingKey {
    pub fn from_pem_file(path: &Path) -> Result<Self, Error> {
        let pem = std::fs::read_to_string(path)
            .map_err(|e| Error::Key(format!("{}: {e}", path.display())))?;
        Self::from_pem_str(&pem).map_err(|e| match e {
            // `from_pem_str` has no path to name, so the path is restored
            // here: the file reader's error message is unchanged from before
            // the extraction.
            Error::Key(_) => Error::Key(format!(
                "{}: not a P-256 or Ed25519 public key",
                path.display()
            )),
            other => other,
        })
    }

    /// The same parse as `from_pem_file` with no `std::fs`.
    ///
    /// Two reasons, both of them somebody else's requirement. Task 16's
    /// `TrustRoster` reconciler reads an entry's `spkiPem` out of an API
    /// object and has no file to hand. And it removes one of the two WASM
    /// blockers spec §8 names, for free — the other, an unconditional
    /// `rand_core` + `getrandom`, is exactly what this crate's manifest does
    /// not declare. The WASM verifier itself stays out of tag 1.
    pub fn from_pem_str(pem: &str) -> Result<Self, Error> {
        if let Ok(k) = p256::ecdsa::VerifyingKey::from_public_key_pem(pem) {
            return Ok(VerifyingKey::P256(k));
        }
        ed25519_dalek::VerifyingKey::from_public_key_pem(pem)
            .map(VerifyingKey::Ed25519)
            .map_err(|e| Error::Key(format!("not a P-256 or Ed25519 public key: {e}")))
    }

    pub fn key_id(&self) -> String {
        let der = match self {
            VerifyingKey::P256(k) => k.to_public_key_der().expect("SPKI").as_bytes().to_vec(),
            VerifyingKey::Ed25519(k) => k.to_public_key_der().expect("SPKI").as_bytes().to_vec(),
        };
        let mut h = Sha256::new();
        h.update(&der);
        hex::encode(h.finalize())
    }

    /// SubjectPublicKeyInfo PEM. Used to mint the checked-in `public.pem` test
    /// fixture and by any future caller that needs to hand out a public key.
    pub fn to_public_key_pem(&self) -> Result<String, Error> {
        let pem = match self {
            VerifyingKey::P256(k) => k
                .to_public_key_pem(LineEnding::LF)
                .map_err(|e| Error::Key(e.to_string()))?,
            VerifyingKey::Ed25519(k) => k
                .to_public_key_pem(LineEnding::LF)
                .map_err(|e| Error::Key(e.to_string()))?,
        };
        Ok(pem)
    }
}
