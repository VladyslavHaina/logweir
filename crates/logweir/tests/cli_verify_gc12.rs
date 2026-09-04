//! Deferred finding 2 (Global Constraint 12): a reader must refuse a
//! `format_version` whose major is newer than the one this binary
//! understands. `Scorecard::validate_invariants` (logweir-core) implements
//! the refusal, and `drill verify` calls it after the signature check
//! succeeds — so a well-SIGNED document from a future major bump must still
//! be refused, exactly like any other self-contradicting document, via the
//! same exit code the brief's `verify.rs` assigns that case: 4.
//!
//! This is a real process-exit-status assertion (not just a check on
//! `validate_invariants`'s own return value), so it proves the CLI's mapping
//! end to end.

use logweir_core::det_json::to_deterministic_json;
use logweir_core::scorecard::Scorecard;
use logweir_evidence::keys::SigningKey;
use logweir_evidence::sign::sign_detached;
use logweir_evidence::PAYLOAD_TYPE_SCORECARD;
use std::process::Command;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_logweir"))
}

#[test]
fn drill_verify_refuses_a_higher_major_format_version_even_when_well_signed() {
    let bytes = std::fs::read("../../e2e/fixtures/signed/scorecard.json").unwrap();
    let mut sc: Scorecard = serde_json::from_slice(&bytes).unwrap();
    sc.format_version = "9.9.9".to_string();
    let bumped_bytes = to_deterministic_json(&sc).unwrap();

    // A fresh key: this test asserts the CLI refuses the DOCUMENT, not that
    // it fails to verify a signature — so the signature must be genuinely
    // valid over the bumped bytes.
    let key = SigningKey::generate_p256();
    let sidecar = sign_detached(&key, PAYLOAD_TYPE_SCORECARD, &bumped_bytes).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let sc_path = dir.path().join("s.json");
    let sig_path = dir.path().join("s.sig");
    let pub_path = dir.path().join("pub.pem");
    std::fs::write(&sc_path, &bumped_bytes).unwrap();
    std::fs::write(&sig_path, serde_json::to_vec(&sidecar).unwrap()).unwrap();
    std::fs::write(&pub_path, key.verifying_key().to_public_key_pem().unwrap()).unwrap();

    let out = bin()
        .args(["drill", "verify", "--scorecard"])
        .arg(&sc_path)
        .arg("--signature")
        .arg(&sig_path)
        .arg("--public-key")
        .arg(&pub_path)
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(4),
        "a well-signed document claiming a higher major format_version must still be \
         refused, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(
        err.contains("format_version"),
        "the refusal must name the field, got: {err}"
    );
}
