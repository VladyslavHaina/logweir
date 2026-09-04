use crate::exit::ExitCode;
use logweir_core::outcome::Outcome;
use logweir_core::scorecard::Scorecard;
use logweir_evidence::{
    keys::VerifyingKey, verify::verify_detached, Error as EvidenceError, Sidecar,
    PAYLOAD_TYPE_SCORECARD,
};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct VerifyReport {
    pub signature_valid: bool,
    pub invariants_ok: bool,
    pub self_attested: bool,
    pub run_id: String,
    pub outcome: Outcome,
    pub approver: String,
    pub ticket: String,
    pub key_id: String,
}

/// The function the Interfaces block promises. `run` is a thin printer over it,
/// which is what keeps `VerifyReport` and its five fields live under
/// `clippy -D warnings` (there is no external consumer of this crate's library
/// in v0.1).
pub fn verify_scorecard(
    scorecard: &Path,
    signature: &Path,
    public_key: &Path,
) -> Result<VerifyReport, ExitCode> {
    // verify-as-read: the exact bytes on disk, never a re-serialisation.
    let bytes = match std::fs::read(scorecard) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("cannot read {}: {e}", scorecard.display());
            return Err(ExitCode::Operational);
        }
    };
    let sidecar: Sidecar = match std::fs::read(signature)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
    {
        Some(s) => s,
        None => {
            eprintln!("cannot read a DSSE sidecar from {}", signature.display());
            return Err(ExitCode::Operational);
        }
    };
    let key = match VerifyingKey::from_pem_file(public_key) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("{e}");
            return Err(ExitCode::Operational);
        }
    };
    if let Err(e) = verify_detached(&key, PAYLOAD_TYPE_SCORECARD, &bytes, &sidecar) {
        // Deferred finding: `logweir-evidence::Error` distinguishes structural
        // corruption of the sidecar (`Malformed` — base64 that will not
        // decode, a DER blob that is invalid or the wrong length, a
        // payload_type mismatch) from a definite cryptographic negative
        // (`Verify` — a well-formed signature that does not match, or no
        // signature by the presented key). The former says nothing about
        // whether the archive itself is trustworthy — it is an operational
        // failure, like a truncated file, and must NOT be reported as
        // tampering. The latter is exactly what tampering after signing
        // produces, so it maps to the same exit code the brief assigns a
        // bad signature: 4, "signing or lock-proof failed".
        return match e {
            EvidenceError::Malformed(msg) => {
                eprintln!("cannot verify: the signature data is malformed: {msg}");
                Err(ExitCode::Operational)
            }
            EvidenceError::Verify(msg) => {
                eprintln!("SIGNATURE INVALID: {msg}");
                Err(ExitCode::SigningOrLock)
            }
            EvidenceError::Key(msg) => {
                eprintln!("{msg}");
                Err(ExitCode::Operational)
            }
        };
    }
    let sc: Scorecard = match serde_json::from_slice(&bytes) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("signature verified but the payload is not a scorecard: {e}");
            return Err(ExitCode::Operational);
        }
    };
    // Global Constraint 12 (a reader must refuse a higher-major
    // format_version) is enforced inside `validate_invariants`, so a
    // well-signed document from a future major bump is refused here, not
    // silently accepted.
    if let Err(e) = sc.validate_invariants() {
        eprintln!("SIGNATURE VALID but the document is self-contradicting: {e}");
        return Err(ExitCode::SigningOrLock);
    }
    Ok(VerifyReport {
        signature_valid: true,
        invariants_ok: true,
        self_attested: sc.approval.self_attested,
        run_id: sc.run_id.clone(),
        outcome: sc.outcome,
        approver: sc.approval.approver.clone(),
        ticket: sc.approval.ticket.clone(),
        key_id: sidecar.signatures[0].keyid.clone(),
    })
}

fn print_report(r: &VerifyReport) {
    println!("signature: VALID  key {}", r.key_id);
    println!("run_id:    {}", r.run_id);
    println!("outcome:   {:?}", r.outcome);
    if r.self_attested {
        // R13: the artifact must survive this reading rather than hide it.
        println!("approval:  SELF-ATTESTED — the approval key equals the signing key");
    } else {
        println!("approval:  {} ({})", r.approver, r.ticket);
    }
}

pub fn run(scorecard: &Path, signature: &Path, public_key: &Path) -> ExitCode {
    match verify_scorecard(scorecard, signature, public_key) {
        Ok(r) => {
            print_report(&r);
            ExitCode::Ok
        }
        Err(c) => c,
    }
}
