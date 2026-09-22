use crate::drill::DrillError;
use chrono::{DateTime, Utc};
use logweir_core::approval_policy::{
    self as approval_policy, ApprovalMode, ApprovalPolicy, ExpectedSubject, RestoreAuthorization,
    PAYLOAD_TYPE_RESTORE_AUTHORIZATION,
};
use logweir_core::guard::GuardRefusal;
use logweir_core::ids::sha256_prefixed;
use logweir_core::scorecard::ApprovalInfo;
use logweir_core::spec::ApprovalDoc;
use logweir_evidence::{
    keys::VerifyingKey, verify::verify_detached, Error as EvidenceError, Sidecar,
};
use std::path::Path;

pub const PAYLOAD_TYPE_APPROVAL: &str = "application/vnd.logweir.drill-approval+json;version=1.0.0";

/// The refusal `admit_pinned_approver_key_id` produces, as one function of the
/// two things it compares, so the message a test asserts and the message an
/// operator reads are the same bytes.
///
/// The pinned set is rendered `{a, b}` — comma-and-space inside braces, in the
/// order the flags were given, which is roster order when the operator built
/// the argv (`weirkeeper::controllers::restore::approver_key_ids`). An EMPTY
/// set never reaches here: an empty `--approver-key-ids` list means "not
/// pinned", and the guard returns `Ok` before formatting anything.
#[must_use]
pub fn pinned_set_refusal(key_id: &str, pinned: &[String]) -> String {
    format!(
        "approver key id {key_id} is not in the pinned set {{{}}}; --approver-key-ids pins which \
         approvers this run accepts, and an approval outside it is refused before phase 0 dials \
         anything",
        pinned.join(", ")
    )
}

/// `--approver-key-ids` — the pinned approver set, checked **before phase 0
/// dials anything**.
///
/// # Why this is a separate function and not a parameter of [`verify`]
///
/// The phase record remains phase 1, but production `drill::execute` validates
/// the pinned key and the complete approval bundle at startup. A run whose
/// approver is outside the pinned set is therefore refused before the target
/// cluster is contacted. Global Constraint 11 reserves exit 3 for "refused by
/// a guard, **before anything ran**", so the check has its own entry point
/// here beside the approval logic it belongs to.
///
/// **`drill::execute`, not `drill::execute_with_outcome`** (Task 22 fix round
/// 1, MED-1): the latter takes an already-built `Ctx`, and `drill::context`
/// constructs the rdkafka client, whose construction alone begins bootstrap
/// connections. Called from there, this refusal arrived AFTER the target had
/// been contacted at TCP level — measured against a closed port. Called from
/// `execute`, beside I11's `check_projected_credentials()`, it arrives before
/// any client exists, which is what the message claims.
///
/// Keeping it out of [`verify`]'s signature also keeps that signature at four
/// arguments, which is why `crates/logweir/tests/approval.rs`'s existing suite
/// compiles and passes **unmodified**: omitting the flag is not merely
/// permitted, it is a zero-diff property of every existing caller.
///
/// # What is compared
///
/// The key id of the ACTUAL approver key file this run was given
/// (`--approver-key`), computed the same way [`verify`] computes it, against
/// the ids `--approver-key-ids` names. Not a value read from the approval
/// document: a document cannot be trusted to name the key that signed it, and
/// the id is derived from key material either way.
///
/// # Errors
///
/// * `pinned` empty → `Ok(())`. **Omitting the flag preserves today's
///   behaviour exactly.**
/// * The approver key cannot be read or parsed → [`DrillError::Operational`]
///   (exit 1), matching [`verify`]'s routing for the same failure: a broken
///   input to the tool is not a statement about the plan's authorisation.
/// * The id is outside the set → [`DrillError::Guard`] (exit 3), carrying
///   [`pinned_set_refusal`]'s message.
pub fn admit_pinned_approver_key_id(
    approver_key: &Path,
    pinned: &[String],
) -> Result<(), DrillError> {
    if pinned.is_empty() {
        return Ok(());
    }
    let key = VerifyingKey::from_pem_file(approver_key)
        .map_err(|e| DrillError::Operational(e.to_string()))?;
    let key_id = key.key_id();
    if pinned.iter().any(|p| p == &key_id) {
        return Ok(());
    }
    Err(GuardRefusal(pinned_set_refusal(&key_id, pinned)).into())
}

/// In-memory twin used by the production startup gate after the projected
/// bundle has been captured once. This avoids validating one ConfigMap
/// generation and later using another after kubelet swaps its projection.
pub fn admit_pinned_approver_key_bytes(
    approver_key: &[u8],
    pinned: &[String],
) -> Result<(), DrillError> {
    if pinned.is_empty() {
        return Ok(());
    }
    let pem = std::str::from_utf8(approver_key)
        .map_err(|e| DrillError::Operational(format!("approver public key is not UTF-8: {e}")))?;
    let key =
        VerifyingKey::from_pem_str(pem).map_err(|e| DrillError::Operational(e.to_string()))?;
    let key_id = key.key_id();
    if pinned.iter().any(|p| p == &key_id) {
        return Ok(());
    }
    Err(GuardRefusal(pinned_set_refusal(&key_id, pinned)).into())
}

#[derive(Clone, Debug)]
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
    let sidecar_bytes = std::fs::read(&sig_path).map_err(|_| {
        DrillError::Operational(format!("no DSSE sidecar at {}", sig_path.display()))
    })?;
    let key_bytes = std::fs::read(approver_key)
        .map_err(|e| DrillError::Operational(format!("{}: {e}", approver_key.display())))?;

    verify_bytes(spec_text, &bytes, &sidecar_bytes, &key_bytes, signing_key)
}

/// Verify one already-captured approval bundle. Production startup uses this
/// entry point so the bytes checked before phase 0 are the bytes retained for
/// the run; [`verify`] remains the compatible path-based API.
pub fn verify_bytes(
    spec_text: &str,
    bytes: &[u8],
    sidecar_bytes: &[u8],
    approver_key_bytes: &[u8],
    signing_key: &VerifyingKey,
) -> Result<Approved, DrillError> {
    let sidecar: Sidecar = serde_json::from_slice(sidecar_bytes)
        .map_err(|_| DrillError::Operational("approval DSSE sidecar does not parse".to_string()))?;
    let key_pem = std::str::from_utf8(approver_key_bytes)
        .map_err(|e| DrillError::Operational(format!("approver public key is not UTF-8: {e}")))?;
    let key =
        VerifyingKey::from_pem_str(key_pem).map_err(|e| DrillError::Operational(e.to_string()))?;

    match verify_detached(&key, PAYLOAD_TYPE_APPROVAL, bytes, &sidecar) {
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

    let doc: ApprovalDoc = serde_json::from_slice(bytes)
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

/// The Restore this run's immutable execution contract names — what an
/// authorization document v2 must bind.
#[derive(Clone, Debug)]
pub struct ContractSubject {
    pub namespace: String,
    pub name: String,
    pub uid: String,
}

fn verify_under(
    key: &VerifyingKey,
    bytes: &[u8],
    sidecar: &Sidecar,
    which: &str,
) -> Result<String, DrillError> {
    match verify_detached(key, PAYLOAD_TYPE_RESTORE_AUTHORIZATION, bytes, sidecar) {
        Ok(key_id) => Ok(key_id),
        Err(EvidenceError::Malformed(msg)) => Err(DrillError::Operational(format!(
            "authorization sidecar signature data is malformed: {msg}"
        ))),
        Err(EvidenceError::Verify(msg)) => Err(GuardRefusal(format!(
            "the {which} signature over the authorization document does not verify: {msg}; no \
             data operation was started"
        ))
        .into()),
        Err(EvidenceError::Key(msg)) => Err(DrillError::Operational(msg)),
    }
}

/// **Authorization document v2 at the runner** — PLAT-19.2, D0: "the runner
/// revalidates it before any data-plane work".
///
/// Every input is a mounted bundle member the immutable Job template pins by
/// digest (`validate_execution_contract` has already compared all of them);
/// this re-derives the VERDICT from those bytes, so a controller-side defect
/// cannot turn into a run nobody authorised:
///
/// 1. the policy snapshot is a canonical snapshot, and its digest is the one
///    the signed document names (inside [`approval_policy::check_binding`]);
/// 2. the console's signature verifies under the mounted confirmation key —
///    in BOTH modes;
/// 3. the document binds THIS Restore (namespace, name, UID from the
///    contract), this plan's hash, and the snapshot's name, digest and mode;
///    its window is well formed and within the policy's maximum;
/// 4. `Ordinary`: the mounted approver key IS the console key — the console's
///    confirmation is the whole authorization, and a bundle naming anyone else
///    as the authoriser is not an ordinary run. `Governed`: the approver key
///    is a DIFFERENT key and its signature over the same bytes verifies.
///
/// The document's EXPIRY is deliberately not re-checked against this pod's
/// clock: the controller admitted the run inside the window and D0 says an
/// admitted run "continues under its recorded policy snapshot"; a pod that
/// waited in `Pending` must not turn an admitted restore into a refusal.
///
/// # Errors
///
/// [`DrillError::Guard`] (exit 3) for every refusal, [`DrillError::Operational`]
/// for unreadable inputs — the same routing [`verify_bytes`] uses.
#[allow(clippy::too_many_arguments)]
pub fn verify_authorization_v2_bytes(
    spec_text: &str,
    bytes: &[u8],
    sidecar_bytes: &[u8],
    approver_key_bytes: &[u8],
    confirmation_key_bytes: &[u8],
    snapshot_bytes: &[u8],
    subject: &ContractSubject,
    signing_key: &VerifyingKey,
) -> Result<Approved, DrillError> {
    let sidecar: Sidecar = serde_json::from_slice(sidecar_bytes).map_err(|_| {
        DrillError::Operational("authorization DSSE sidecar does not parse".to_string())
    })?;
    let policy = ApprovalPolicy::from_snapshot_bytes(snapshot_bytes).map_err(|e| {
        DrillError::Guard(GuardRefusal(format!("{e}; no data operation was started")))
    })?;
    let key = |raw: &[u8], label: &str| -> Result<VerifyingKey, DrillError> {
        let pem = std::str::from_utf8(raw)
            .map_err(|e| DrillError::Operational(format!("{label} is not UTF-8: {e}")))?;
        VerifyingKey::from_pem_str(pem).map_err(|e| DrillError::Operational(e.to_string()))
    };
    let confirmation = key(confirmation_key_bytes, "confirmation-issuer public key")?;
    let approver = key(approver_key_bytes, "approver public key")?;

    verify_under(&confirmation, bytes, &sidecar, "console confirmation")?;
    let doc = RestoreAuthorization::from_bytes(bytes).map_err(|e| {
        DrillError::Guard(GuardRefusal(format!("{e}; no data operation was started")))
    })?;
    let expected = ExpectedSubject {
        namespace: subject.namespace.clone(),
        name: subject.name.clone(),
        uid: subject.uid.clone(),
        plan_hash: sha256_prefixed(spec_text.as_bytes()),
    };
    approval_policy::check_binding(&doc, &expected, &policy)
        .and_then(|()| approval_policy::check_window_shape(&doc, &policy))
        .map_err(|e| {
            DrillError::Guard(GuardRefusal(format!("{e}; no data operation was started")))
        })?;

    let confirmation_id = confirmation.key_id();
    let approver_id = approver.key_id();
    let approver_label = match policy.mode {
        ApprovalMode::Ordinary => {
            if approver_id != confirmation_id {
                return Err(GuardRefusal(format!(
                    "policy {} is Ordinary, so the console's confirmation is the whole \
                     authorization, but the bundle names approver key {approver_id} and \
                     confirmation key {confirmation_id}; no data operation was started",
                    policy.name
                ))
                .into());
            }
            doc.requester.principal_id()
        }
        ApprovalMode::Governed => {
            if approver_id == confirmation_id {
                return Err(GuardRefusal(format!(
                    "policy {} is Governed, and the bundle names the console key \
                     {confirmation_id} as the approver; a governed run needs a separate approver \
                     signature; no data operation was started",
                    policy.name
                ))
                .into());
            }
            verify_under(&approver, bytes, &sidecar, "governed approver")?;
            format!("governed approver key {approver_id}")
        }
    };
    let self_attested = approver_id == signing_key.key_id();
    Ok(Approved {
        validated_at: Utc::now(),
        approval: ApprovalInfo {
            approver: approver_label,
            ticket: doc.ticket.clone().unwrap_or_default(),
            plan_hash: doc.plan_hash.clone(),
            approved_at: doc.issued_at,
            key_id: approver_id,
            self_attested,
        },
    })
}
