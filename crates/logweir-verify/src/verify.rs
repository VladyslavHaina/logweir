use crate::{keys::VerifyingKey, pae::pae, Error, Sidecar};
use base64::{engine::general_purpose::STANDARD as B64, Engine};

/// Verify-as-read: the caller passes the bytes it actually read from disk or
/// from the bucket, never a re-serialisation of a parsed value. A
/// re-serialisation would verify a document nobody stored (spec §6 C3).
///
/// On success, returns the `keyid` of the signature that was actually
/// checked and matched — never `sidecar.signatures[0]`, which a caller must
/// not assume is the signature that verified when a sidecar carries more
/// than one signature.
pub fn verify_detached(
    key: &VerifyingKey,
    payload_type: &str,
    payload: &[u8],
    sidecar: &Sidecar,
) -> Result<String, Error> {
    if sidecar.payload_type != payload_type {
        // A signed sidecar whose payload_type does not match is evidence of
        // SUBSTITUTION — a genuinely-signed sidecar for a different kind of
        // document handed over in place of this one. PAE binds payload_type
        // cryptographically, so such a sidecar could never verify either
        // way; this is about which signal an operator gets, and substitution
        // is an escalation ("tampered"), not "retry, operational error".
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
        // Base64 that will not decode is corruption of the sidecar itself,
        // not a cryptographic fact about the payload.
        let raw = B64
            .decode(&s.sig)
            .map_err(|e| Error::Malformed(format!("signature is not valid base64: {e}")))?;
        let ok = match key {
            VerifyingKey::P256(k) => {
                use p256::ecdsa::signature::Verifier as _;
                // DER that will not parse, or is the wrong length, is also
                // structural corruption — distinct from a DER blob that
                // parses fine but whose crypto check fails.
                let sig = p256::ecdsa::Signature::from_der(&raw)
                    .map_err(|e| Error::Malformed(format!("signature is not valid DER: {e}")))?;
                k.verify(&msg, &sig).is_ok()
            }
            VerifyingKey::Ed25519(k) => {
                use ed25519_dalek::Verifier as _;
                let sig = ed25519_dalek::Signature::from_slice(&raw).map_err(|e| {
                    Error::Malformed(format!("signature has the wrong length: {e}"))
                })?;
                k.verify(&msg, &sig).is_ok()
            }
        };
        if ok {
            return Ok(s.keyid.clone());
        }
        return Err(Error::Verify(
            "signature does not verify over the payload".into(),
        ));
    }
    Err(Error::Verify(format!(
        "no signature by key {want} in the sidecar"
    )))
}
