use crate::exit::ExitCode;
use logweir_core::outcome::Outcome;
use logweir_core::scorecard::Scorecard;
use logweir_evidence::{
    keys::VerifyingKey, verify::verify_detached, Error as EvidenceError, Sidecar,
    PAYLOAD_TYPE_BACKUP_RECEIPT, PAYLOAD_TYPE_PUT_RECEIPT, PAYLOAD_TYPE_SCORECARD,
    PAYLOAD_TYPE_TEARDOWN,
};
use std::path::Path;

/// `--payload-type <name>` to the media type the sidecar must carry.
///
/// Four arms over the four constants `logweir-verify` DECLARES, so there is
/// no fifth spelling of a media type anywhere in this binary. An unknown
/// value is an **error**, never a silent passthrough of anything that happens
/// to contain a slash: a typo'd media type would otherwise turn into
/// "unexpected payloadType" downstream and read like a bad artifact rather
/// than a bad command line.
///
/// The Rust half of the parity with `docs/verify_scorecard.py::
/// resolve_payload_type`, whose short names are these short names. **Task 5b
/// makes it the exact twin** — the Python function also passes a full media
/// type straight through, and this one deliberately does not yet, because
/// accepting a spelling the Python half accepts is a parity claim that has to
/// be made by a test the two readers share rather than by two functions that
/// happen to agree today.
pub fn resolve_payload_type(name: &str) -> Result<&'static str, String> {
    match name {
        "scorecard" => Ok(PAYLOAD_TYPE_SCORECARD),
        "backup-receipt" => Ok(PAYLOAD_TYPE_BACKUP_RECEIPT),
        "receipt" => Ok(PAYLOAD_TYPE_PUT_RECEIPT),
        "teardown" => Ok(PAYLOAD_TYPE_TEARDOWN),
        other => Err(format!(
            "unknown --payload-type {other:?}; use one of backup-receipt, receipt, \
             scorecard, teardown"
        )),
    }
}

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

/// What a verification actually established.
///
/// # Why an enum and not four optional fields on `VerifyReport`
///
/// `--payload-type` lets one command verify four different documents, and
/// only one of them has an `outcome`, an approver and a ticket. A
/// `VerifyReport` with those fields nulled for a receipt would print an empty
/// approval line and let a caller ask a scorecard question of a document that
/// cannot answer it. The two verdicts are different claims, so they are
/// different values.
#[derive(Debug, Clone)]
pub enum Verdict {
    /// The signature verified AND every scorecard invariant AND the derived
    /// approval claim held. The full-strength verdict, and the only one
    /// `--payload-type scorecard` (the default) can produce.
    Scorecard(VerifyReport),
    /// The signature verified over these exact bytes under this key, and the
    /// sidecar's `payloadType` is the one asked for. **Nothing about the
    /// document's own consistency was checked**, because the invariant reader
    /// for this document type is not wired into this command yet — Task 5b
    /// adds the dispatch, starting with the backup receipt's four arms. The
    /// printer says so in as many words; an exit 0 that silently meant less
    /// than the scorecard's exit 0 would be the worst thing this command
    /// could do.
    SignatureOnly {
        payload_type: String,
        key_id: String,
    },
}

/// The function the Interfaces block promises. `run` is a thin printer over it,
/// which is what keeps `VerifyReport` and its five fields live under
/// `clippy -D warnings` (there is no external consumer of this crate's library
/// in v0.1).
///
/// `payload_type` is a **resolved media type** — the output of
/// `resolve_payload_type`, not the short name the operator typed. `run`
/// resolves before calling, exactly as `docs/verify_scorecard.py` resolves in
/// `main` and hands `verify()` a media type, so the two readers cannot end up
/// disagreeing about where the mapping happens.
pub fn verify_scorecard(
    scorecard: &Path,
    signature: &Path,
    public_key: &Path,
    payload_type: &str,
) -> Result<Verdict, ExitCode> {
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
    // Deferred finding: `logweir-evidence::Error` distinguishes structural
    // corruption of the sidecar (`Malformed` — base64 that will not decode,
    // a DER blob that is invalid or the wrong length) from a definite
    // cryptographic/protocol negative (`Verify` — a well-formed signature
    // that does not match, no signature by the presented key, or a
    // payload_type mismatch, which is evidence of substitution). The former
    // says nothing about whether the archive itself is trustworthy — it is
    // an operational failure, like a truncated file, and must NOT be
    // reported as tampering. The latter is exactly what tampering (or
    // substitution) after signing produces, so it maps to the same exit
    // code the brief assigns a bad signature: 4, "signing or lock-proof
    // failed".
    //
    // `verify_detached` returns the `keyid` of the signature that actually
    // matched and verified — never `sidecar.signatures[0]`, which is not
    // necessarily the signature that was checked when a sidecar carries more
    // than one signature.
    let matched_key_id = match verify_detached(&key, payload_type, &bytes, &sidecar) {
        Ok(id) => id,
        Err(e) => {
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
    };
    // THE DISPATCH. Everything below this point is scorecard-specific: the
    // `Scorecard` deserialisation, its invariant arms and the derived
    // approval claim. A receipt, a teardown record or a storage receipt is a
    // different document with different invariants, so running the
    // scorecard's arms over it would either fail at deserialisation (and
    // report a good receipt as a bad scorecard) or, worse, appear to check
    // something.
    //
    // Note what the branch does NOT have to defend against: a document of
    // the wrong type handed to the default `--payload-type scorecard`.
    // `verify_detached` compares the sidecar's `payloadType` in full and has
    // already refused it above — a genuinely-signed sidecar for a different
    // kind of document presented in place of this one is exactly its
    // `Verify` arm's SUBSTITUTION case. So the only way to reach here with a
    // receipt is to have ASKED for a receipt.
    //
    // Task 5b replaces this early return with the invariant dispatch:
    // `BackupReceipt::validate_invariants` for the backup receipt, and the
    // same treatment for the other two when their readers land. Until then
    // the verdict says, on stdout, exactly how much was checked.
    if payload_type != PAYLOAD_TYPE_SCORECARD {
        return Ok(Verdict::SignatureOnly {
            payload_type: payload_type.to_string(),
            key_id: matched_key_id,
        });
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
    // T0-1. `approval.self_attested` in the document is a CLAIM. The finding is
    // derived from the key that actually verified this signature — the same
    // comparison the writer makes at
    // `crates/logweir/src/drill/phase1_approval.rs`'s
    // `let self_attested = key_id == signing_key.key_id();`. A document that
    // claims otherwise is refused: it is a provenance claim the signature
    // cannot support, so it is the same class as a bad signature (Global
    // Constraint 11, exit 4). NOT an invariant in `logweir-core` — that layer
    // has no key. Position matters: this arm runs AFTER `validate_invariants`,
    // so a self-contradicting document still gets the more fundamental
    // finding above rather than this one.
    let derived_self_attested = sc.approval.key_id == matched_key_id;
    if derived_self_attested != sc.approval.self_attested {
        if sc.approval.self_attested {
            eprintln!(
                "APPROVAL CLAIM NOT VERIFIED: the document claims self_attested=true \
                 but the approval key id {} does not match the verifying key id {}",
                sc.approval.key_id, matched_key_id
            );
        } else {
            // The other direction is a refusal too, and it needs its own
            // sentence: a message that said "does not match" here would state
            // a falsehood in the one line whose whole purpose is to be
            // trustworthy.
            eprintln!(
                "APPROVAL CLAIM NOT VERIFIED: the document claims self_attested=false \
                 but the approval key id {} matches the verifying key id {}",
                sc.approval.key_id, matched_key_id
            );
        }
        return Err(ExitCode::SigningOrLock);
    }
    Ok(Verdict::Scorecard(VerifyReport {
        signature_valid: true,
        invariants_ok: true,
        self_attested: derived_self_attested,
        run_id: sc.run_id.clone(),
        outcome: sc.outcome,
        approver: sc.approval.approver.clone(),
        ticket: sc.approval.ticket.clone(),
        key_id: matched_key_id,
    }))
}

fn print_report(r: &VerifyReport) {
    println!("signature: VALID  key {}", r.key_id);
    println!("run_id:    {}", r.run_id);
    // The WIRE spelling, like every other surface. This line was the FOURTH
    // rendering of one enum out of one binary (`Pass` here, `pass` on `drill
    // run`'s stdout, `Pass` in `drill show`'s table, `pass` in the JSON and
    // the Prometheus labels) — and it is the one an auditor reads directly
    // beside the document it is verifying.
    println!("outcome:   {}", r.outcome.wire_name());
    if r.self_attested {
        // R13: the artifact must survive this reading rather than hide it.
        println!("approval:  SELF-ATTESTED — the approval key equals the signing key");
    } else {
        println!("approval:  {} ({})", r.approver, r.ticket);
    }
}

/// What a `SignatureOnly` verdict prints. Separate from `print_report` so the
/// sentence that says how much was checked is a single literal a test can
/// pin, rather than something assembled at the call site.
fn print_signature_only(payload_type: &str, key_id: &str) {
    println!("signature: VALID  key {key_id}");
    println!("payload:   {payload_type}");
    // The honest line. Exit 0 here means "the bytes are signed by this key
    // under this media type", and NOT what exit 0 means for a scorecard.
    println!(
        "checked:   the SIGNATURE only — this build evaluates no invariant \
         for this document type"
    );
}

pub fn run(scorecard: &Path, signature: &Path, public_key: &Path, payload_type: &str) -> ExitCode {
    // Resolved BEFORE anything is read: a bad `--payload-type` is a bad
    // command line, and reporting it after a file-read failure would blame
    // the artifact for the operator's typo.
    let wanted = match resolve_payload_type(payload_type) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::Operational;
        }
    };
    match verify_scorecard(scorecard, signature, public_key, wanted) {
        Ok(Verdict::Scorecard(r)) => {
            print_report(&r);
            ExitCode::Ok
        }
        Ok(Verdict::SignatureOnly {
            payload_type,
            key_id,
        }) => {
            print_signature_only(&payload_type, &key_id);
            ExitCode::Ok
        }
        Err(c) => c,
    }
}
