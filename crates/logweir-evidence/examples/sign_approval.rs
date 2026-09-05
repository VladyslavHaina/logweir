//! Signs a drill-approval JSON document as a detached DSSE sidecar.
//!
//! Usage: sign_approval <approver-key.pem> <approval.json> <approval.sig>
//!
//! `openssl dgst` cannot produce this: the signature covers PAE(payloadType,
//! payload), never the bare bytes, so `logweir` and `docs/verify_scorecard.py`
//! agree on what was signed.
use logweir_evidence::{keys::SigningKey, sign::sign_detached};
use std::path::PathBuf;

/// MUST stay byte-for-byte identical to `logweir::drill::phase1_approval::
/// PAYLOAD_TYPE_APPROVAL` (Task 15). It is re-declared rather than imported
/// because `logweir-evidence` is the lower layer and must not depend on the
/// `logweir` binary crate.
const PAYLOAD_TYPE_APPROVAL: &str = "application/vnd.logweir.drill-approval+json;version=1.0.0";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [key_path, payload_path, out_path] = args.as_slice() else {
        eprintln!("usage: sign_approval <key.pem> <approval.json> <approval.sig>");
        std::process::exit(2);
    };
    let key = SigningKey::from_pem_file(&PathBuf::from(key_path))?;
    let payload = std::fs::read(payload_path)?;
    let sidecar = sign_detached(&key, PAYLOAD_TYPE_APPROVAL, &payload)?;
    std::fs::write(out_path, serde_json::to_vec_pretty(&sidecar)?)?;
    Ok(())
}
