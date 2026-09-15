use crate::Error;
use p256::pkcs8::{DecodePrivateKey, EncodePrivateKey, LineEnding};
use std::path::Path;

/// `VerifyingKey` and its entire inherent `impl` block moved to
/// `logweir-verify` (ADR 0008 §E) and are re-exported here, so every
/// existing `logweir_evidence::keys::VerifyingKey` call site — including
/// the `VerifyingKey::P256(_)` / `VerifyingKey::Ed25519(_)` patterns in
/// `crates/logweir/tests/fixtures/mod.rs` — compiles unchanged. The block
/// had to move whole: Rust coherence forbids this crate adding an inherent
/// method to a type it no longer defines, which is why
/// `to_public_key_pem` went with it.
///
/// `KeyAlg` below did NOT move: it is `SigningKey::alg`'s return type and
/// `impl VerifyingKey` never names it.
pub use logweir_verify::keys::VerifyingKey;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyAlg {
    /// ECDSA-P256-SHA256 — the algorithm OSO's own evidence envelope uses.
    EcdsaP256Sha256,
    Ed25519,
}

pub enum SigningKey {
    P256(p256::ecdsa::SigningKey),
    Ed25519(ed25519_dalek::SigningKey),
}

/// Where the key in hand came from. An enum, not a bool: a caller cannot get
/// `minted` backwards without the compiler noticing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyOrigin {
    LoadedFromFile,
    Minted,
}

impl SigningKey {
    pub fn generate_p256() -> Self {
        SigningKey::P256(p256::ecdsa::SigningKey::random(&mut rand_core::OsRng))
    }

    pub fn generate_ed25519() -> Self {
        SigningKey::Ed25519(ed25519_dalek::SigningKey::generate(&mut rand_core::OsRng))
    }

    /// Accepts a PKCS#8 PEM private key of either algorithm; the algorithm is
    /// discovered, never configured, so a mis-set flag cannot pick the wrong one.
    pub fn from_pem_file(path: &Path) -> Result<Self, Error> {
        let pem = std::fs::read_to_string(path)
            .map_err(|e| Error::Key(format!("{}: {e}", path.display())))?;
        Self::from_pkcs8_pem(&pem).map_err(|e| match e {
            Error::Key(detail) => Error::Key(format!("{}: {detail}", path.display())),
            other => other,
        })
    }

    /// Parses an in-memory PKCS#8 PEM private key.
    ///
    /// Kubernetes identity bootstrap receives an existing key from the API;
    /// keeping that value in memory avoids making a second private-key file in
    /// a writable temporary directory merely to use the file parser.
    pub fn from_pkcs8_pem(pem: &str) -> Result<Self, Error> {
        if let Ok(k) = p256::ecdsa::SigningKey::from_pkcs8_pem(pem) {
            return Ok(SigningKey::P256(k));
        }
        ed25519_dalek::SigningKey::from_pkcs8_pem(pem)
            .map(SigningKey::Ed25519)
            .map_err(|e| Error::Key(format!("not a P-256 or Ed25519 PKCS#8 key: {e}")))
    }

    /// Reads `path` as a PKCS#8 PEM private key if it exists; otherwise mints a
    /// fresh P-256 key and writes it to `path` (parent directories must already
    /// exist). Never prints key material.
    ///
    /// This is what pins the checked-in fixture key. A caller that minted
    /// unconditionally orphaned the fingerprint `docs/verify-a-scorecard.md`
    /// teaches auditors to pin every time it ran; here, a key that is already
    /// on disk is READ and the file is left byte-identical, so re-running the
    /// mint can change the DOCUMENT and never the KEY. Recipe and rotation
    /// story: `docs/keys.md`.
    ///
    /// Errors: `Error::Key` if `path` exists but is not a P-256/Ed25519 PKCS#8
    /// PEM (propagated verbatim from `from_pem_file`), or if the mint path
    /// cannot write `path`. A path that exists but is unreadable or malformed
    /// is an ERROR, never a silent re-mint — a silent re-mint over a corrupt
    /// key file is the exact failure this function exists to prevent.
    pub fn load_or_generate(path: &Path) -> Result<(SigningKey, KeyOrigin), Error> {
        if path.exists() {
            return Ok((Self::from_pem_file(path)?, KeyOrigin::LoadedFromFile));
        }
        let key = Self::generate_p256();
        // `to_pkcs8_pem` returns PRIVATE key material: it is written to the
        // path the caller named and never logged, printed or put in an error
        // message. The error below names the PATH only.
        std::fs::write(path, key.to_pkcs8_pem()?)
            .map_err(|e| Error::Key(format!("{}: {e}", path.display())))?;
        Ok((key, KeyOrigin::Minted))
    }

    pub fn alg(&self) -> KeyAlg {
        match self {
            SigningKey::P256(_) => KeyAlg::EcdsaP256Sha256,
            SigningKey::Ed25519(_) => KeyAlg::Ed25519,
        }
    }

    pub fn verifying_key(&self) -> VerifyingKey {
        match self {
            SigningKey::P256(k) => VerifyingKey::P256(*k.verifying_key()),
            SigningKey::Ed25519(k) => VerifyingKey::Ed25519(k.verifying_key()),
        }
    }

    /// Hex sha256 of the SubjectPublicKeyInfo DER. Stable across encodings, so
    /// approval.key_id in one scorecard names the same key in the next.
    pub fn key_id(&self) -> String {
        self.verifying_key().key_id()
    }

    /// PKCS#8 PEM of the PRIVATE key. Used only to mint the checked-in test
    /// fixtures (`crates/logweir-evidence/examples/mint_fixture.rs`) — never
    /// print the result of this method.
    pub fn to_pkcs8_pem(&self) -> Result<String, Error> {
        let pem = match self {
            SigningKey::P256(k) => k
                .to_pkcs8_pem(LineEnding::LF)
                .map_err(|e| Error::Key(e.to_string()))?,
            SigningKey::Ed25519(k) => k
                .to_pkcs8_pem(LineEnding::LF)
                .map_err(|e| Error::Key(e.to_string()))?,
        };
        Ok(pem.to_string())
    }
}
