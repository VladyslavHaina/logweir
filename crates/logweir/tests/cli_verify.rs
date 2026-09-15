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

/// The two-reader parity claim is only as real as the gate that runs it.
///
/// `scripts/check-verifier-parity.sh` is the artefact that makes "both
/// verifiers reach the same verdict" mechanical rather than documented. On the
/// commit that introduced it, it was invoked by the `justfile`'s `lint` recipe
/// and by **no CI job at all** — its three sibling guards
/// (`check-no-oso.sh`, `check-pure-core.sh`, `check-dod.sh`) each have a named
/// step in `.github/workflows/ci.yml`, and CI has never run `just lint`. So
/// deleting the script would have changed no CI result, and deleting the
/// `justfile` line was caught by nothing.
///
/// This test is the cover. It reads both gate files from the repository — the
/// same shape as
/// `crates/logweir-evidence/tests/key_provenance.rs::keygen_recipe_is_documented_and_linked`
/// and `crates/logweir-core/tests/fixture_regen.rs::fixtures_recipe_is_non_destructive`,
/// both of which already assert on checked-in non-Rust files — and requires the
/// script to be invoked by an EXECUTED line in each, never merely mentioned in
/// a comment. Removing either wiring fails it at assertion time.
#[test]
fn the_verifier_parity_script_is_wired_into_lint_and_ci() {
    const SCRIPT: &str = "check-verifier-parity.sh";

    // Lines that actually run, with comments stripped. A comment naming the
    // script is not a gate: without this filter, deleting the step and leaving
    // the comment that explains it would keep this test green — which is the
    // failure mode the test exists to prevent.
    fn executed(text: &str) -> Vec<&str> {
        text.lines()
            .filter(|l| !l.trim_start().starts_with('#'))
            .filter(|l| !l.trim().is_empty())
            .collect()
    }

    // 1. The `lint` recipe body invokes it.
    let justfile = std::fs::read_to_string("../../justfile").expect("read justfile");
    let mut lines = justfile.lines();
    lines
        .by_ref()
        .find(|l| l.trim_end() == "lint:")
        .expect("justfile has a `lint` recipe");
    let recipe: String = lines
        .take_while(|l| l.starts_with(' ') || l.starts_with('\t') || l.trim().is_empty())
        .collect::<Vec<&str>>()
        .join("\n");
    let body = executed(&recipe);
    assert!(
        body.iter().any(|l| l.contains(SCRIPT)),
        "the `lint` recipe must invoke ./scripts/{SCRIPT}: it is the only mechanical \
         check that the two verifiers agree, and `just lint` is the recipe a developer \
         runs before pushing. Recipe body was: {body:?}"
    );

    // CI calls the shared script, which calls the same lint recipe above.
    let ci = std::fs::read_to_string("../../.github/workflows/ci.yml")
        .expect("read .github/workflows/ci.yml");
    assert!(executed(&ci)
        .iter()
        .any(|l| l.contains("bash scripts/ci-check.sh")));
    let check = std::fs::read_to_string("../../scripts/ci-check.sh").expect("read shared check");
    assert!(executed(&check).iter().any(|l| l.trim() == "just lint"));
}

// ---------------------------------------------------------------- Task 5
/// The signed backup-receipt fixture verifies under `--payload-type
/// backup-receipt`, and the SAME bytes are refused under `--payload-type
/// scorecard`.
///
/// Two halves, and the second is the load-bearing one. `verify_detached`
/// compares the sidecar's `payloadType` **in full**, so a genuinely-signed
/// document of one type presented in place of another is refused as
/// SUBSTITUTION rather than accepted — which is what makes the default
/// (`scorecard`) safe to leave in place while the flag exists. If the
/// comparison were a prefix, a substring, or absent, the second half of this
/// test would pass at exit 0 and nothing else in the suite would notice.
///
/// Exit codes are read directly from `Command::output()`'s status.
#[test]
fn the_signed_receipt_fixture_verifies() {
    let verify_typed = |payload_type: &str| {
        bin()
            .args([
                "drill",
                "verify",
                "--payload-type",
                payload_type,
                "--scorecard",
            ])
            .arg(format!("{FIX}/backup-receipt.json"))
            .arg("--signature")
            .arg(format!("{FIX}/backup-receipt.sig"))
            .arg("--public-key")
            .arg(format!("{FIX}/public.pem"))
            .output()
            .unwrap()
    };

    let ok = verify_typed("backup-receipt");
    assert_eq!(
        ok.status.code(),
        Some(0),
        "the signed receipt fixture must verify under its own payload type, stderr: {}",
        String::from_utf8_lossy(&ok.stderr)
    );
    let stdout = String::from_utf8(ok.stdout).unwrap();
    assert!(
        stdout.contains("signature: VALID"),
        "the verdict must say the signature is valid, got: {stdout}"
    );
    assert!(
        stdout.contains("application/vnd.logweir.backup-receipt+json;version=1.0.0"),
        "the verdict must name the media type it checked, got: {stdout}"
    );
    // SINCE TASK 5b THIS IS THE FULL-STRENGTH VERDICT for this document type.
    // Task 5's build checked the signature alone and its printer said so in
    // as many words (`checked: the SIGNATURE only …`); Task 5b wired the
    // invariant dispatch, so `--payload-type backup-receipt` now runs all
    // four of `BackupReceipt::validate_invariants`'s arms and the printer has
    // to say THAT instead. The line still distinguishes the two strengths —
    // which is the property the old assertion was protecting — it just names
    // the stronger one now, and `the_signature_only_verdict_is_still_reachable`
    // below keeps the weaker sentence honest for the two document types that
    // still get it.
    assert!(
        stdout.contains("the signature AND all five backup-receipt invariants"),
        "an exit 0 that checked the invariants must say so on stdout, got: {stdout}"
    );
    assert!(
        !stdout.contains("the SIGNATURE only"),
        "the receipt path evaluates invariants since Task 5b; printing the weaker sentence \
         would understate what exit 0 established, got: {stdout}"
    );

    let substituted = verify_typed("scorecard");
    assert_ne!(
        substituted.status.code(),
        Some(0),
        "the same bytes must be REFUSED as a scorecard: `verify_detached` compares \
         payloadType in full, and a genuinely-signed receipt handed over in place of \
         a scorecard is substitution. stdout: {}",
        String::from_utf8_lossy(&substituted.stdout)
    );
    assert_eq!(
        substituted.status.code(),
        Some(4),
        "a payloadType mismatch is evidence of substitution, which is the same class \
         as a bad signature (Global Constraint 11, exit 4), stderr: {}",
        String::from_utf8_lossy(&substituted.stderr)
    );
}

/// The `SignatureOnly` verdict is still REACHABLE, and still honest.
///
/// Task 5b gave the backup receipt an invariant reader; the drill put receipt
/// and the teardown attestation have none in tag 1, so their exit 0 means
/// strictly less and the printer must keep saying so. Without this row, the
/// sentence could be deleted along with its last caller and nothing would
/// notice — and the next document type to gain a `--payload-type` would
/// inherit an exit 0 that reads like a scorecard's.
#[test]
fn the_signature_only_verdict_is_still_reachable() {
    // A teardown attestation, signed under its own payload type with the
    // checked-in throwaway key. Minted here rather than checked in: the
    // fixtures under e2e/fixtures/signed/ are never re-minted by this task.
    let dir = tempfile::tempdir().unwrap();
    let doc = dir.path().join("teardown.json");
    let sig = dir.path().join("teardown.sig");
    let bytes = br#"{"run_id":"01J9X2QK7C4V0R8YB3ZP6MTS5A","topics_deleted":["drill-orders"]}"#;
    std::fs::write(&doc, bytes).unwrap();
    let key = logweir_evidence::keys::SigningKey::from_pem_file(std::path::Path::new(&format!(
        "{FIX}/signing.pem"
    )))
    .unwrap();
    let sidecar =
        logweir_evidence::sign::sign_detached(&key, logweir_evidence::PAYLOAD_TYPE_TEARDOWN, bytes)
            .unwrap();
    std::fs::write(&sig, serde_json::to_vec(&sidecar).unwrap()).unwrap();

    let out = bin()
        .args([
            "drill",
            "verify",
            "--payload-type",
            "teardown",
            "--scorecard",
        ])
        .arg(&doc)
        .arg("--signature")
        .arg(&sig)
        .arg("--public-key")
        .arg(format!("{FIX}/public.pem"))
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(
        stdout.contains("the SIGNATURE only"),
        "a document type with no invariant reader must say that exit 0 checked the \
         signature alone, got: {stdout}"
    );
}

/// An unrecognised `--payload-type` is an ERROR, never a passthrough.
///
/// A typo'd media type that fell through would surface downstream as
/// "unexpected payloadType" and read like a bad artifact rather than a bad
/// command line — and a value that fell through to the scorecard's constant
/// would silently verify the wrong thing. Exit 1 (operational: no artifact
/// was produced and nothing was refused by a guard), and the message names
/// the four accepted values.
#[test]
fn drill_verify_refuses_an_unknown_payload_type() {
    let out = bin()
        .args([
            "drill",
            "verify",
            "--payload-type",
            "backup_receipt",
            "--scorecard",
        ])
        .arg(format!("{FIX}/backup-receipt.json"))
        .arg("--signature")
        .arg(format!("{FIX}/backup-receipt.sig"))
        .arg("--public-key")
        .arg(format!("{FIX}/public.pem"))
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(1),
        "an unknown --payload-type is a bad command line, stdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let err = String::from_utf8(out.stderr).unwrap();
    for name in ["backup-receipt", "receipt", "scorecard", "teardown"] {
        assert!(
            err.contains(name),
            "the refusal must name the accepted value {name:?}, got: {err}"
        );
    }
}

/// The default is `scorecard`, so every invocation that predates
/// `--payload-type` is byte-for-byte unchanged.
///
/// Asserted by comparing the three-argument form's stdout against the
/// explicit `--payload-type scorecard` form's over the same fixture. A
/// default of anything else, or a printer that mentioned the payload type on
/// the scorecard path, would change output an auditor's script may be reading.
#[test]
fn the_payload_type_default_leaves_the_scorecard_invocation_unchanged() {
    let implicit = verify(
        format!("{FIX}/scorecard.json"),
        format!("{FIX}/scorecard.sig"),
        format!("{FIX}/public.pem"),
    );
    let explicit = bin()
        .args([
            "drill",
            "verify",
            "--payload-type",
            "scorecard",
            "--scorecard",
        ])
        .arg(format!("{FIX}/scorecard.json"))
        .arg("--signature")
        .arg(format!("{FIX}/scorecard.sig"))
        .arg("--public-key")
        .arg(format!("{FIX}/public.pem"))
        .output()
        .unwrap();
    assert_eq!(implicit.status.code(), Some(0), "the fixture must verify");
    assert_eq!(implicit.status.code(), explicit.status.code());
    assert_eq!(
        String::from_utf8_lossy(&implicit.stdout),
        String::from_utf8_lossy(&explicit.stdout),
        "the default must be `scorecard`, and the scorecard path's four printed lines \
         must be exactly what they were before the flag existed"
    );
    assert!(
        String::from_utf8_lossy(&implicit.stdout).contains("run_id:"),
        "the scorecard path still prints its full report, not the signature-only one"
    );
}
