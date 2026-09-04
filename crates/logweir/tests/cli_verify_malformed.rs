//! Deferred finding 1: `logweir-evidence::Error` distinguishes a malformed
//! sidecar (corruption — base64 that will not decode, a signature blob that
//! is not valid DER) from a genuine cryptographic tamper signal (a
//! well-formed signature that does not match). `drill verify` must map the
//! two onto DIFFERENT exit codes: 1 (operational — no artifact, nothing is
//! claimed about the archive) for corruption, versus 4 (signing invalid) for
//! a real tamper detection. This is asserted against the actual process
//! exit status, not merely against `logweir_evidence::Error`'s variant.

use std::process::Command;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_logweir"))
}

/// A truncated/corrupt `.sig` file — the `sig` field is not valid base64 —
/// must exit 1 (operational), never 4 ("tampered").
#[test]
fn a_sidecar_with_undecodable_base64_exits_operational_not_tampered() {
    let dir = tempfile::tempdir().unwrap();
    let sc = dir.path().join("s.json");
    let sig = dir.path().join("s.sig");
    let pubk = dir.path().join("pub.pem");
    std::fs::copy("../../e2e/fixtures/signed/scorecard.json", &sc).unwrap();
    std::fs::copy("../../e2e/fixtures/signed/public.pem", &pubk).unwrap();

    let good_sig = std::fs::read_to_string("../../e2e/fixtures/signed/scorecard.sig").unwrap();
    let mut sidecar: serde_json::Value = serde_json::from_str(&good_sig).unwrap();
    sidecar["signatures"][0]["sig"] = serde_json::Value::String("!!!not-base64!!!".to_string());
    std::fs::write(&sig, serde_json::to_vec(&sidecar).unwrap()).unwrap();

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
        Some(1),
        "malformed signature data must be reported as operational, not tampered: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(
        !err.contains("SIGNATURE INVALID"),
        "a corrupt file must not be reported as a signature-invalid/tamper finding: {err}"
    );
}

/// The other half of the same contract: a signature that parses fine but
/// simply does not match (the existing `drill_verify_accepts_a_good_scorecard
/// _and_rejects_a_tampered_one` test in cli_verify.rs covers the equivalent
/// case via a flipped byte in the payload) must still exit 4.
#[test]
fn a_structurally_valid_but_non_matching_signature_exits_tampered() {
    let dir = tempfile::tempdir().unwrap();
    let sc = dir.path().join("s.json");
    let sig = dir.path().join("s.sig");
    let pubk = dir.path().join("pub.pem");
    std::fs::copy("../../e2e/fixtures/signed/scorecard.json", &sc).unwrap();
    std::fs::copy(
        "../../e2e/fixtures/signed/scorecard-self-attested.sig",
        &sig,
    )
    .unwrap();
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
        Some(4),
        "a well-formed signature over the wrong document must exit 4: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}
