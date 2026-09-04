use crate::{keys::SigningKey, pae::pae, Error, Sidecar, Signature};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use p256::ecdsa::signature::Signer as _;

pub fn sign_detached(
    key: &SigningKey,
    payload_type: &str,
    payload: &[u8],
) -> Result<Sidecar, Error> {
    let msg = pae(payload_type, payload);
    let sig: Vec<u8> = match key {
        SigningKey::P256(k) => {
            let s: p256::ecdsa::Signature = k.sign(&msg);
            s.to_der().as_bytes().to_vec()
        }
        SigningKey::Ed25519(k) => {
            use ed25519_dalek::Signer as _;
            k.sign(&msg).to_bytes().to_vec()
        }
    };
    Ok(Sidecar {
        payload_type: payload_type.to_string(),
        signatures: vec![Signature {
            keyid: key.key_id(),
            sig: B64.encode(sig),
        }],
    })
}
