use crate::exit::ExitCode;
use logweir_core::backup_receipt::BackupReceipt;
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
/// **The Rust twin of `docs/verify_scorecard.py::resolve_payload_type`, byte
/// for byte** (Task 5b). Three clauses, and all three are the Python
/// function's:
///
/// 1. a SHORT NAME maps to the media type of the same constant;
/// 2. a FULL MEDIA TYPE passes straight through — Task 5 deliberately refused
///    this, recording that "accepting a spelling the Python half accepts is a
///    parity claim that has to be made by a test the two readers share"; the
///    test now exists (`crates/logweir/tests/two_reader_parity_receipt.rs::
///    the_two_payload_type_resolvers_agree`), so the clause lands with it;
/// 3. anything else is an **error**, never a silent passthrough of a string
///    that happens to contain a slash: a typo'd media type would otherwise
///    turn into "unexpected payloadType" downstream and read like a bad
///    artifact rather than a bad command line.
///
/// Four arms over the four constants `logweir-verify` DECLARES, so there is
/// no fifth spelling of a media type anywhere in this binary.
pub fn resolve_payload_type(name: &str) -> Result<&'static str, String> {
    const TYPES: [(&str, &str); 4] = [
        ("backup-receipt", PAYLOAD_TYPE_BACKUP_RECEIPT),
        ("receipt", PAYLOAD_TYPE_PUT_RECEIPT),
        ("scorecard", PAYLOAD_TYPE_SCORECARD),
        ("teardown", PAYLOAD_TYPE_TEARDOWN),
    ];
    // The short-name arm.
    if let Some((_, media)) = TYPES.iter().find(|(short, _)| *short == name) {
        return Ok(media);
    }
    // Clause 2: a full media type passes through — returned as the CONSTANT,
    // not as the caller's own string, so a `&'static str` is honest and the
    // value downstream is the one this binary declares.
    if let Some((_, media)) = TYPES.iter().find(|(_, media)| *media == name) {
        return Ok(media);
    }
    // The short names are listed SORTED, which is what Python's
    // `", ".join(sorted(PAYLOAD_TYPES))` produces, and `TYPES` is declared in
    // that order so the two lists cannot come apart. The trailing clause "or
    // a full media type" is Python's too, and it is not decoration: without
    // it the message tells an operator to pass a short name while the
    // function accepts a media type.
    let shorts: Vec<&str> = TYPES.iter().map(|(short, _)| *short).collect();
    Err(format!(
        "unknown --payload-type {}; use one of {} or a full media type",
        python_repr(name),
        shorts.join(", ")
    ))
}

/// `name` as CPython's `repr()` of a `str` renders it.
///
/// The parity claim is BYTE-IDENTICAL refusal text, and the two languages
/// disagree about quoting: `format!("{:?}")` on a Rust `&str` produces
/// `"x"`, while Python's `{name!r}` produces `'x'`. One of them had to move
/// (Task 5's review). Python's moved nothing — `docs/verify_scorecard.py` is
/// the document an auditor is told to read and its message is the one quoted
/// in the interface register — so this function reproduces CPython's rule
/// here instead: single quotes, switching to double quotes when the value
/// contains a single quote and no double quote, with `\`, the active quote
/// and the three whitespace escapes escaped.
///
/// It is a FUNCTION and not an inline `format!` so that
/// `two_reader_parity_receipt.rs::the_two_payload_type_resolvers_agree` can
/// drive both readers over the awkward values (`it's`, `say "hi"`, `both'"`)
/// rather than only over a value with no quote in it, which is the case that
/// would have passed under either convention.
fn python_repr(name: &str) -> String {
    let quote = if name.contains('\'') && !name.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::with_capacity(name.len() + 2);
    out.push(quote);
    for c in name.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c => out.push(c),
        }
    }
    out.push(quote);
    out
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
    /// The signature verified AND all five of
    /// `logweir_core::backup_receipt::BackupReceipt`'s invariants held. The
    /// backup receipt's full-strength verdict, and the second document type
    /// this command evaluates rather than merely authenticates (Task 5b).
    ///
    /// It carries no `outcome`, approver or ticket for the reason the enum's
    /// own doc comment gives: a receipt cannot answer a scorecard's
    /// questions, and a `VerifyReport` with those fields nulled would print
    /// an empty approval line over a document that never had one.
    BackupReceipt {
        payload_type: String,
        key_id: String,
        backup_id: String,
        run_id: String,
        manifest_key: String,
    },
    /// The signature verified over these exact bytes under this key, and the
    /// sidecar's `payloadType` is the one asked for. **Nothing about the
    /// document's own consistency was checked**, because the invariant reader
    /// for this document type is not wired into this command — it is the
    /// verdict for the drill put receipt and the teardown attestation, whose
    /// readers are not in tag 1. The printer says so in as many words; an
    /// exit 0 that silently meant less than the scorecard's exit 0 would be
    /// the worst thing this command could do.
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
    // **F2 (Task 5's review): a payload-type mismatch is not a bad
    // signature, and this line used to say it was.**
    //
    // `verify_detached` returns `Error::Verify` for a mismatch — the right
    // class (substitution, exit 4) and the right escalation — but the printer
    // below renders every `Verify` as `SIGNATURE INVALID:`, so a
    // genuinely-signed receipt presented as a scorecard was reported to an
    // operator as a forgery. The signature over those bytes may be perfectly
    // valid; what is wrong is that it signs a DIFFERENT KIND OF DOCUMENT.
    //
    // Compared HERE, before `verify_detached`, because this is where both the
    // sidecar and the wanted type are in hand and where the message can be
    // specific. `verify_detached` keeps its own comparison — PAE binds
    // `payload_type` cryptographically and the library must refuse a mismatch
    // for every caller, not only this one — so this is a better message in
    // front of an unchanged rule, never a relaxation of it. Exit code
    // unchanged at 4: `drill_verify_exits_signing_or_lock_on_a_payload_type_
    // mismatch` and `the_signed_receipt_fixture_verifies` both pin it.
    if sidecar.payload_type != payload_type {
        eprintln!(
            "PAYLOAD TYPE MISMATCH: the sidecar signs {}, but this check asked for {}. \
             The signature is not reported as invalid: it may be entirely valid over some \
             OTHER document. A signed document of one type presented in place of another \
             is SUBSTITUTION, which is why this exits 4 rather than 1.",
            sidecar.payload_type, payload_type
        );
        return Err(ExitCode::SigningOrLock);
    }
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
    // **THE DISPATCH** (Task 5b). One command verifies four documents, and
    // the invariant reader that runs is chosen by the RESOLVED media type —
    // never by what the bytes happen to parse as. A scorecard runs the
    // scorecard's arms, a backup receipt runs `BackupReceipt::
    // validate_invariants`'s four, and **neither set runs on the other
    // document**: running the scorecard's arms over a receipt would either
    // fail at deserialisation (reporting a good receipt as a bad scorecard)
    // or, far worse, appear to check something.
    //
    // Note what the branch does NOT have to defend against: a document of
    // the wrong type handed to the default `--payload-type scorecard`. The
    // sidecar's `payloadType` is compared in full above, and a
    // genuinely-signed sidecar for a different kind of document presented in
    // place of this one is refused there as substitution. So the only way to
    // reach a receipt's arms is to have ASKED for a receipt.
    if payload_type == PAYLOAD_TYPE_BACKUP_RECEIPT {
        let receipt: BackupReceipt = match serde_json::from_slice(&bytes) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("signature verified but the payload is not a backup receipt: {e}");
                return Err(ExitCode::Operational);
            }
        };
        // The SAME outer sentence the scorecard's arms are reported under, so
        // `scripts/check-verifier-parity.sh` and
        // `crates/logweir/tests/two_reader_parity_receipt.rs` strip one prefix
        // for both document types. `validate_invariants` returns the bare
        // message (no `InvariantError` wrapper — the receipt's arms return a
        // `String`, because the string IS the interface the second reader
        // reproduces byte for byte), so there is no inner prefix here.
        if let Err(e) = receipt.validate_invariants() {
            eprintln!("SIGNATURE VALID but the document is self-contradicting: {e}");
            return Err(ExitCode::SigningOrLock);
        }
        return Ok(Verdict::BackupReceipt {
            payload_type: payload_type.to_string(),
            key_id: matched_key_id,
            backup_id: receipt.backup_id,
            run_id: receipt.run_id,
            manifest_key: receipt.archive.manifest_key,
        });
    }
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

/// What a `BackupReceipt` verdict prints.
///
/// It says which invariant set ran, in as many words, because the whole point
/// of the `SignatureOnly` line beside it is that the two exit-0s do not mean
/// the same thing. A reader who cannot tell them apart is back where the
/// honest-line work started.
fn print_backup_receipt(
    payload_type: &str,
    key_id: &str,
    backup_id: &str,
    run_id: &str,
    manifest_key: &str,
) {
    println!("signature: VALID  key {key_id}");
    println!("payload:   {payload_type}");
    println!("run_id:    {run_id}");
    println!("backup_id: {backup_id}");
    // The one field an auditor takes away and goes looking with. Empty is
    // legal and means the backup did not exit 0 (invariant 2), so it is
    // spelled rather than printed blank.
    println!(
        "manifest:  {}",
        if manifest_key.trim().is_empty() {
            "none — this receipt is for a backup that did not exit 0"
        } else {
            manifest_key
        }
    );
    println!(
        "checked:   the signature AND all five backup-receipt invariants \
         (format_version, exit_code/manifest_key, records/topics, covered window, \
         source.auth.mode)"
    );
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
        Ok(Verdict::BackupReceipt {
            payload_type,
            key_id,
            backup_id,
            run_id,
            manifest_key,
        }) => {
            print_backup_receipt(&payload_type, &key_id, &backup_id, &run_id, &manifest_key);
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
