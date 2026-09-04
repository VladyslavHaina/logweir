use crate::{keys::VerifyingKey, pae::pae, Error, Sidecar};
use base64::{engine::general_purpose::STANDARD as B64, Engine};

/// Verify-as-read: the caller passes the bytes it actually read from disk or
/// from the bucket, never a re-serialisation of a parsed value. A
/// re-serialisation would verify a document nobody stored (spec §6 C3).
pub fn verify_detached(
    key: &VerifyingKey,
    payload_type: &str,
    payload: &[u8],
    sidecar: &Sidecar,
) -> Result<(), Error> {
    if sidecar.payload_type != payload_type {
        return Err(Error::Verify(format!(
            "payload_type mismatch: sidecar says {}, caller says {payload_type}",
            sidecar.payload_type
        )));
    }
    let msg = pae(payload_type, payload);
    let want = key.key_id();
    for s in &sidecar.signatures {
        if s.keyid != want {
            continue;
        }
        let raw = B64
            .decode(&s.sig)
            .map_err(|e| Error::Verify(e.to_string()))?;
        let ok = match key {
            VerifyingKey::P256(k) => {
                use p256::ecdsa::signature::Verifier as _;
                p256::ecdsa::Signature::from_der(&raw)
                    .ok()
                    .map(|sig| k.verify(&msg, &sig).is_ok())
                    .unwrap_or(false)
            }
            VerifyingKey::Ed25519(k) => {
                use ed25519_dalek::Verifier as _;
                ed25519_dalek::Signature::from_slice(&raw)
                    .ok()
                    .map(|sig| k.verify(&msg, &sig).is_ok())
                    .unwrap_or(false)
            }
        };
        if ok {
            return Ok(());
        }
        return Err(Error::Verify(
            "signature does not verify over the payload".into(),
        ));
    }
    Err(Error::Verify(format!(
        "no signature by key {want} in the sidecar"
    )))
}
