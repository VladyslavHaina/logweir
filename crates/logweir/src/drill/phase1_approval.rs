use crate::drill::DrillError;
use chrono::{DateTime, Utc};
use logweir_core::guard::GuardRefusal;
use logweir_core::ids::sha256_prefixed;
use logweir_core::scorecard::ApprovalInfo;
use logweir_core::spec::ApprovalDoc;
use logweir_evidence::{
    keys::VerifyingKey, verify::verify_detached, Error as EvidenceError, Sidecar,
};
use std::path::Path;

pub const PAYLOAD_TYPE_APPROVAL: &str = "application/vnd.logweir.drill-approval+json;version=1.0.0";

#[derive(Debug)]
pub struct Approved {
    pub approval: ApprovalInfo,
    pub validated_at: DateTime<Utc>,
}

/// v0.1: approval is UNCONDITIONAL. There is no "spec_hash changed" disjunct,
/// because v0.1 has no ephemeral-target provisioning, so every target is a
/// pre-existing cluster and that disjunct would be dead code (spec §9.3 p1).
///
/// Exit-code routing (Task 15 fix round 1, review finding F1 — the exit-code
/// contract is a Global Constraint and overrides the brief's frozen
/// `Result<Approved, GuardRefusal>` signature): only a genuine refusal — the
/// signature does not verify over the presented key, or `plan_hash` names a
/// different plan — becomes `DrillError::Guard` (exit 3, "the plan is
/// refused, nothing ran"). Everything where Logweir could not do its own
/// job — the approval file or its `.sig` sidecar cannot be read or parsed,
/// or the approver key is unreadable/malformed, or the sidecar's signature
/// bytes are structurally corrupt (`EvidenceError::Malformed`, which that
/// crate's own doc comment instructs callers to treat as operational, not as
/// evidence of tampering) — becomes `DrillError::Operational` (exit 1,
/// retry), matching the routing `crates/logweir/src/verify.rs` already uses
/// for the same `logweir_evidence::Error` variants over the sibling
/// scorecard-verify path.
pub fn verify(
    spec_text: &str,
    approval_json: &Path,
    approver_key: &Path,
    signing_key: &VerifyingKey,
) -> Result<Approved, DrillError> {
    let bytes = std::fs::read(approval_json)
        .map_err(|e| DrillError::Operational(format!("{}: {e}", approval_json.display())))?;
    let sig_path = approval_json.with_extension("sig");
    let sidecar: Sidecar = std::fs::read(&sig_path)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .ok_or_else(|| {
            DrillError::Operational(format!("no DSSE sidecar at {}", sig_path.display()))
        })?;
    let key = VerifyingKey::from_pem_file(approver_key)
        .map_err(|e| DrillError::Operational(e.to_string()))?;

    match verify_detached(&key, PAYLOAD_TYPE_APPROVAL, &bytes, &sidecar) {
        Ok(_) => {}
        // Structural corruption of the sidecar itself (truncated base64, a
        // DER blob that will not parse or is the wrong length) says nothing
        // about whether the approval was tampered with — operational, per
        // `logweir_evidence::Error::Malformed`'s own doc comment.
        Err(EvidenceError::Malformed(msg)) => {
            return Err(DrillError::Operational(format!(
                "approval sidecar signature data is malformed: {msg}"
            )));
        }
        // A definite cryptographic/protocol negative: the signature does not
        // verify, no signature in the sidecar was made by the presented key,
        // or the payload_type does not match — each is evidence that this
        // approval does not authorise anything, which IS a refusal.
        Err(EvidenceError::Verify(msg)) => {
            return Err(GuardRefusal(format!("approval signature does not verify: {msg}")).into());
        }
        // `verify_detached` never returns `Key` (that variant belongs to
        // `VerifyingKey::from_pem_file`, handled above) — matched
        // exhaustively rather than with a wildcard so a future new variant
        // fails to compile instead of silently routing to the wrong exit
        // code.
        Err(EvidenceError::Key(msg)) => {
            return Err(DrillError::Operational(msg));
        }
    }

    let doc: ApprovalDoc = serde_json::from_slice(&bytes)
        .map_err(|e| DrillError::Operational(format!("approval is not an ApprovalDoc: {e}")))?;

    let actual = sha256_prefixed(spec_text.as_bytes());
    if actual != doc.plan_hash {
        return Err(GuardRefusal(format!(
            "plan_hash mismatch: the approval names {} but this spec hashes to {actual}. \
             Re-approve the exact plan you intend to run.",
            doc.plan_hash
        ))
        .into());
    }

    let key_id = key.key_id();
    // Derived from the ACTUAL approver key compared to the ACTUAL scorecard
    // signing key — never a caller-supplied flag or a bare string a caller
    // could get stale or wrong (Task 15 fix round 1, review finding F2). Not
    // refused either way — LABELLED. Both verifiers and `drill show` surface
    // it.
    let self_attested = key_id == signing_key.key_id();

    Ok(Approved {
        // Logweir's own clock at the moment BOTH the signature verified and
        // the plan hash matched. THIS, not the human's approved_at, is the
        // input to measured.rto_seconds (spec §9.3 phase 8).
        validated_at: Utc::now(),
        approval: ApprovalInfo {
            approver: doc.approver,
            ticket: doc.ticket,
            plan_hash: doc.plan_hash,
            approved_at: doc.approved_at,
            key_id,
            self_attested,
        },
    })
}
