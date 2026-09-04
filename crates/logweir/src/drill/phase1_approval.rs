use chrono::{DateTime, Utc};
use logweir_core::guard::GuardRefusal;
use logweir_core::ids::sha256_prefixed;
use logweir_core::scorecard::ApprovalInfo;
use logweir_core::spec::ApprovalDoc;
use logweir_evidence::{keys::VerifyingKey, verify::verify_detached, Sidecar};
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
pub fn verify(
    spec_text: &str,
    approval_json: &Path,
    approver_key: &Path,
    signing_key_id: &str,
) -> Result<Approved, GuardRefusal> {
    let bytes = std::fs::read(approval_json)
        .map_err(|e| GuardRefusal(format!("{}: {e}", approval_json.display())))?;
    let sig_path = approval_json.with_extension("sig");
    let sidecar: Sidecar = std::fs::read(&sig_path)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .ok_or_else(|| GuardRefusal(format!("no DSSE sidecar at {}", sig_path.display())))?;
    let key = VerifyingKey::from_pem_file(approver_key).map_err(|e| GuardRefusal(e.to_string()))?;

    verify_detached(&key, PAYLOAD_TYPE_APPROVAL, &bytes, &sidecar)
        .map_err(|e| GuardRefusal(format!("approval signature does not verify: {e}")))?;

    let doc: ApprovalDoc = serde_json::from_slice(&bytes)
        .map_err(|e| GuardRefusal(format!("approval is not an ApprovalDoc: {e}")))?;

    let actual = sha256_prefixed(spec_text.as_bytes());
    if actual != doc.plan_hash {
        return Err(GuardRefusal(format!(
            "plan_hash mismatch: the approval names {} but this spec hashes to {actual}. \
             Re-approve the exact plan you intend to run.",
            doc.plan_hash
        )));
    }

    let key_id = key.key_id();
    // Not refused — LABELLED. Both verifiers and `drill show` surface it.
    let self_attested = key_id == signing_key_id;

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
