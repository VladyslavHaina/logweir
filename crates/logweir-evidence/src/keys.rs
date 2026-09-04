use crate::Error;
use p256::pkcs8::{
    DecodePrivateKey, DecodePublicKey, EncodePrivateKey, EncodePublicKey, LineEnding,
};
use sha2::{Digest, Sha256};
use std::path::Path;

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

#[derive(Clone)]
pub enum VerifyingKey {
    P256(p256::ecdsa::VerifyingKey),
    Ed25519(ed25519_dalek::VerifyingKey),
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
        if let Ok(k) = p256::ecdsa::SigningKey::from_pkcs8_pem(&pem) {
            return Ok(SigningKey::P256(k));
        }
        ed25519_dalek::SigningKey::from_pkcs8_pem(&pem)
            .map(SigningKey::Ed25519)
            .map_err(|e| {
                Error::Key(format!(
                    "{}: not a P-256 or Ed25519 PKCS#8 key: {e}",
                    path.display()
                ))
            })
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

impl VerifyingKey {
    pub fn from_pem_file(path: &Path) -> Result<Self, Error> {
        let pem = std::fs::read_to_string(path)
            .map_err(|e| Error::Key(format!("{}: {e}", path.display())))?;
        if let Ok(k) = p256::ecdsa::VerifyingKey::from_public_key_pem(&pem) {
            return Ok(VerifyingKey::P256(k));
        }
        ed25519_dalek::VerifyingKey::from_public_key_pem(&pem)
            .map(VerifyingKey::Ed25519)
            .map_err(|e| {
                Error::Key(format!(
                    "{}: not a P-256 or Ed25519 public key: {e}",
                    path.display()
                ))
            })
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
