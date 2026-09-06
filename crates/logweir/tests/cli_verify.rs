use std::process::Command;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_logweir"))
}

#[test]
fn schema_scorecard_prints_the_schema() {
    let out = bin().args(["schema", "scorecard"]).output().unwrap();
    assert!(out.status.success());
    let s = String::from_utf8(out.stdout).unwrap();
    assert!(s.contains("logweir-drill-scorecard-1.0.0.json"));
}

#[test]
fn schema_plan_exits_1_naming_the_sub_project() {
    let out = bin().args(["schema", "plan"]).output().unwrap();
    assert_eq!(out.status.code(), Some(1));
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(
        err.contains("SP3"),
        "must name the sub-project that introduces it, got: {err}"
    );
}

#[test]
fn drill_verify_accepts_a_good_scorecard_and_rejects_a_tampered_one() {
    let dir = tempfile::tempdir().unwrap();
    let sc = dir.path().join("s.json");
    let sig = dir.path().join("s.sig");
    let pubk = dir.path().join("pub.pem");
    // fixtures/ carries a pre-signed scorecard + sidecar + public key, minted
    // once by `just fixtures-sign` (step 4).
    std::fs::copy("../../e2e/fixtures/signed/scorecard.json", &sc).unwrap();
    std::fs::copy("../../e2e/fixtures/signed/scorecard.sig", &sig).unwrap();
    std::fs::copy("../../e2e/fixtures/signed/public.pem", &pubk).unwrap();

    let out = bin()
        .args(["drill", "verify", "--scorecard"])
        .arg(&sc)
        .arg("--signature")
        .arg(&sig)
        .arg("--public-key")
        .arg(&pubk)
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let mut bytes = std::fs::read(&sc).unwrap();
    let pos = bytes.iter().position(|b| *b == b'1').unwrap();
    bytes[pos] = b'2';
    std::fs::write(&sc, bytes).unwrap();

    let out = bin()
        .args(["drill", "verify", "--scorecard"])
        .arg(&sc)
        .arg("--signature")
        .arg(&sig)
        .arg("--public-key")
        .arg(&pubk)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(4), "a tampered payload must exit 4");
}

#[test]
fn drill_verify_labels_a_self_attested_scorecard() {
    let out = bin()
        .args([
            "drill",
            "verify",
            "--scorecard",
            "../../e2e/fixtures/signed/scorecard-self-attested.json",
            "--signature",
            "../../e2e/fixtures/signed/scorecard-self-attested.sig",
            "--public-key",
            "../../e2e/fixtures/signed/public.pem",
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0));
    let s = String::from_utf8(out.stdout).unwrap();
    assert!(
        s.contains("SELF-ATTESTED"),
        "the label must be surfaced, got: {s}"
    );
}

/// ONE SPELLING PER VALUE, on the surface an auditor reads beside the document
/// they are verifying. `drill verify`'s summary printed Rust `Debug` — `Pass`
/// — while the signed JSON it had just checked said `pass`. That was the
/// FOURTH rendering of one enum out of one binary.
#[test]
fn the_verify_summary_prints_the_outcome_as_the_signed_document_spells_it() {
    let dir = tempfile::tempdir().unwrap();
    let sc = dir.path().join("scorecard.json");
    let sig = dir.path().join("scorecard.sig");
    std::fs::copy("../../e2e/fixtures/signed/scorecard.json", &sc).unwrap();
    std::fs::copy("../../e2e/fixtures/signed/scorecard.sig", &sig).unwrap();
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_logweir"))
        .args([
            "drill",
            "verify",
            "--scorecard",
            sc.to_str().unwrap(),
            "--signature",
            sig.to_str().unwrap(),
            "--public-key",
            "../../e2e/fixtures/signed/public.pem",
        ])
        .output()
        .expect("the compiled binary runs");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let printed = String::from_utf8(out.stdout).unwrap();
    assert!(
        printed.contains("outcome:   pass"),
        "the outcome must read as the signed document spells it: {printed}"
    );
    assert!(
        !printed.contains("outcome:   Pass"),
        "Rust Debug is a fourth spelling of a value the document already spells: {printed}"
    );
}

// ---------------------------------------------------------------------------
// T0-1 — `approval.self_attested` is DERIVED, never echoed.
//
// `verify.rs` used to copy `sc.approval.self_attested` — the document's own
// claim about the single most damaging property of its provenance — into the
// verified report and print it under a `signature: VALID` banner, while the
// key that actually verified the signature sat in `matched_key_id` and was
// used only to print itself. The five tests below pin the derivation, both
// directions of the refusal, the exit code, and the fact that the derivation
// reads the signature that MATCHED rather than `signatures[0]`.
//
// Every one of them drives the compiled binary and reads `out.status.code()`
// directly — never an exit code through a pipe.

use logweir_core::det_json::to_deterministic_json;
use logweir_core::scorecard::Scorecard;
use logweir_evidence::keys::SigningKey;
use logweir_evidence::sign::sign_detached;
use logweir_evidence::{Sidecar, PAYLOAD_TYPE_SCORECARD};

const FIX: &str = "../../e2e/fixtures/signed";

fn verify(
    sc: impl AsRef<std::path::Path>,
    sig: impl AsRef<std::path::Path>,
    pubk: impl AsRef<std::path::Path>,
) -> std::process::Output {
    bin()
        .args(["drill", "verify", "--scorecard"])
        .arg(sc.as_ref())
        .arg("--signature")
        .arg(sig.as_ref())
        .arg("--public-key")
        .arg(pubk.as_ref())
        .output()
        .unwrap()
}

/// The fixture private key, loaded — never generated. A test that quietly
/// minted a key here would still pass while proving nothing about the
/// checked-in corpus.
fn fixture_key() -> SigningKey {
    SigningKey::from_pem_file(std::path::Path::new(
        "../../e2e/fixtures/signed/signing.pem",
    ))
    .expect("the pinned fixture signing key loads")
}

/// Write `sc` through the SAME deterministic-JSON path production code uses,
/// sign those exact bytes with the fixture key, and return the two paths.
/// Signing the bytes that are written (never a re-serialisation of them) is
/// what makes a refusal below provably about the claim and not about crypto.
fn sign_into(dir: &std::path::Path, sc: &Scorecard) -> (std::path::PathBuf, std::path::PathBuf) {
    let bytes = to_deterministic_json(sc).expect("scorecard serialises");
    let sc_path = dir.join("s.json");
    let sig_path = dir.join("s.sig");
    std::fs::write(&sc_path, &bytes).unwrap();
    let sidecar = sign_detached(&fixture_key(), PAYLOAD_TYPE_SCORECARD, &bytes).unwrap();
    std::fs::write(&sig_path, serde_json::to_vec(&sidecar).unwrap()).unwrap();
    (sc_path, sig_path)
}

/// A validly signed document whose ONLY defect is its own claim: it says
/// `self_attested: true` while its `approval.key_id` is `"a"*64`, which is not
/// the key that signed it. The signature check must SUCCEED and the reader
/// must still refuse, with exit 4 (Global Constraint 11: a provenance claim
/// the signature cannot support is the same class as a bad signature).
#[test]
fn verify_refuses_self_attested_claim_when_key_ids_differ() {
    let out = verify(
        format!("{FIX}/scorecard-self-attested-bogus.json"),
        format!("{FIX}/scorecard-self-attested-bogus.sig"),
        format!("{FIX}/public.pem"),
    );
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(
        !err.contains("SIGNATURE INVALID"),
        "the signature over this fixture is genuine; a refusal here must come from the \
         derivation, not from the cryptography: {err}"
    );
    assert_eq!(
        out.status.code(),
        Some(4),
        "a document whose self_attested claim the signature cannot support must exit 4: {err}"
    );
    assert!(
        err.contains(
            "APPROVAL CLAIM NOT VERIFIED: the document claims self_attested=true but the \
             approval key id "
        ),
        "the refusal must name the claim and both key ids, byte-exactly: {err}"
    );
}

/// The other direction, which the plan's single message string cannot express
/// truthfully: the ids MATCH while the document claims `false`. A reader that
/// only refuses over-claiming would accept a document that under-reports its
/// own lack of separation of duties — the same defect wearing the other face.
#[test]
fn verify_refuses_a_false_claim_when_the_key_ids_match() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = std::fs::read(format!("{FIX}/scorecard-self-attested.json")).unwrap();
    let mut sc: Scorecard = serde_json::from_slice(&bytes).unwrap();
    // key_id stays equal to the signing key's; only the CLAIM is flipped.
    assert_eq!(sc.approval.key_id, fixture_key().key_id());
    sc.approval.self_attested = false;
    let (sc_path, sig_path) = sign_into(dir.path(), &sc);

    let out = verify(&sc_path, &sig_path, format!("{FIX}/public.pem"));
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(
        !err.contains("SIGNATURE INVALID"),
        "these bytes really were signed by the fixture key: {err}"
    );
    assert_eq!(out.status.code(), Some(4), "{err}");
    let want = format!(
        "APPROVAL CLAIM NOT VERIFIED: the document claims self_attested=false but the \
         approval key id {} matches the verifying key id {}",
        sc.approval.key_id,
        fixture_key().key_id()
    );
    assert!(
        err.contains(&want),
        "the refusal must state the DIRECTION that is actually wrong.\nwant: {want}\ngot:  {err}"
    );
}

/// The positive case, derived rather than echoed: the minted variant's
/// `approval.key_id` really is the signing key's, so the finding is `true`
/// and the existing wording is printed on the strength of the comparison —
/// not on the strength of the document's own say-so.
#[test]
fn verify_derives_self_attested_true_on_the_minted_variant() {
    let out = verify(
        format!("{FIX}/scorecard-self-attested.json"),
        format!("{FIX}/scorecard-self-attested.sig"),
        format!("{FIX}/public.pem"),
    );
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let s = String::from_utf8(out.stdout).unwrap();
    assert!(
        s.contains("approval:  SELF-ATTESTED — the approval key equals the signing key"),
        "the derived-true wording is quoted verbatim by three documents: {s}"
    );
}

/// The negative case, derived: `scorecard.json`'s approver key id is `"a"*64`,
/// which is not the key that signed it, so the finding is `false` and the
/// approver/ticket line is printed instead.
#[test]
fn verify_derives_self_attested_false_on_the_plain_fixture() {
    let out = verify(
        format!("{FIX}/scorecard.json"),
        format!("{FIX}/scorecard.sig"),
        format!("{FIX}/public.pem"),
    );
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let s = String::from_utf8(out.stdout).unwrap();
    let sc: Scorecard =
        serde_json::from_slice(&std::fs::read(format!("{FIX}/scorecard.json")).unwrap()).unwrap();
    assert!(
        s.contains(&format!(
            "approval:  {} ({})",
            sc.approval.approver, sc.approval.ticket
        )),
        "a non-self-attested report names the approver and the ticket: {s}"
    );
    assert!(
        !s.contains("SELF-ATTESTED"),
        "a document whose approval key is not the signing key must never be labelled \
         self-attested: {s}"
    );
}

/// The derivation must read the key of the signature that ACTUALLY VERIFIED,
/// never `sidecar.signatures[0]`. A DSSE sidecar may carry one signature per
/// signing key and the entry for the key the auditor holds need not be first;
/// deriving from `signatures[0].keyid` would make the finding depend on the
/// order of a list an attacker controls.
#[test]
fn verify_uses_the_signature_that_matched() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = std::fs::read(format!("{FIX}/scorecard-self-attested.json")).unwrap();

    let foreign = SigningKey::generate_p256();
    let foreign_sidecar = sign_detached(&foreign, PAYLOAD_TYPE_SCORECARD, &bytes).unwrap();
    let real_sidecar = sign_detached(&fixture_key(), PAYLOAD_TYPE_SCORECARD, &bytes).unwrap();
    assert_ne!(
        foreign_sidecar.signatures[0].keyid, real_sidecar.signatures[0].keyid,
        "the two keys must be distinct or this test proves nothing"
    );
    let mixed = Sidecar {
        payload_type: PAYLOAD_TYPE_SCORECARD.to_string(),
        // FOREIGN FIRST — `signatures[0]` is deliberately the wrong entry.
        signatures: vec![
            foreign_sidecar.signatures[0].clone(),
            real_sidecar.signatures[0].clone(),
        ],
    };

    let sc_path = dir.path().join("s.json");
    let sig_path = dir.path().join("s.sig");
    std::fs::write(&sc_path, &bytes).unwrap();
    std::fs::write(&sig_path, serde_json::to_vec(&mixed).unwrap()).unwrap();

    let out = verify(&sc_path, &sig_path, format!("{FIX}/public.pem"));
    let err = String::from_utf8(out.stderr).unwrap();
    assert_eq!(
        out.status.code(),
        Some(0),
        "the finding must be derived from the signature that matched, not from \
         signatures[0]: {err}"
    );
    assert!(
        String::from_utf8(out.stdout)
            .unwrap()
            .contains("SELF-ATTESTED"),
        "and it must still derive TRUE, which is only possible from the matched key"
    );
}

/// ORDERING. The approval-claim arm runs AFTER `validate_invariants`, so a
/// document that is BOTH self-contradicting AND lying about its approval is
/// reported as self-contradicting — the reader's existing, more fundamental
/// finding is not displaced by the newer one.
///
/// `cli_verify_gc12.rs`'s higher-major test cannot cover this: it signs
/// `scorecard.json` (approval.key_id `"a"*64`, claim `false`) with a fresh
/// key, so the derived value already AGREES with the claim there and the
/// approval arm never fires whichever side of `validate_invariants` it sits
/// on. This document makes both arms fire at once.
#[test]
fn verify_reports_self_contradiction_before_the_approval_claim() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = std::fs::read(format!("{FIX}/scorecard-self-attested.json")).unwrap();
    let mut sc: Scorecard = serde_json::from_slice(&bytes).unwrap();
    sc.format_version = "9.9.9".to_string();
    sc.approval.self_attested = false; // the ids DO match, so this is a lie too
    let (sc_path, sig_path) = sign_into(dir.path(), &sc);

    let out = verify(&sc_path, &sig_path, format!("{FIX}/public.pem"));
    let err = String::from_utf8(out.stderr).unwrap();
    assert_eq!(out.status.code(), Some(4), "{err}");
    assert!(
        err.contains("SIGNATURE VALID but the document is self-contradicting")
            && err.contains("format_version"),
        "the self-contradiction must be reported first: {err}"
    );
    assert!(
        !err.contains("APPROVAL CLAIM NOT VERIFIED"),
        "the approval arm must not pre-empt validate_invariants: {err}"
    );
}
