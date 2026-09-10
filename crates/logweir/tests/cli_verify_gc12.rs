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

// ---------------------------------------------------------------- Task 5
// Global Constraint 13, as REVISED: `logweir schema` accepts `scorecard` AND
// `backup-receipt` in tag 1, and `switchover-record` exits 1 naming tag 2.
//
// Both assertions read the exit code directly from `Command::output()`'s
// status (Global Constraint 11 / STANDING RULE 20 — never through a pipe).

/// `logweir schema backup-receipt` prints, byte for byte, the file the drift
/// gate publishes.
///
/// This is the gate on the SHIPPED SURFACE. `crates/logweir-core/tests/
/// schema_drift.rs::backup_receipt_schema_has_no_drift` compares the
/// generator against the checked-in file; this compares what an operator
/// actually gets from the binary against the same file, so a `schema` arm that
/// printed the scorecard's schema, or a stale copy, or the right bytes plus a
/// banner, fails here rather than at whoever downstream validated against it.
#[test]
fn schema_backup_receipt_is_byte_identical_to_the_checked_in_file() {
    let out = bin().args(["schema", "backup-receipt"]).output().unwrap();
    assert_eq!(
        out.status.code(),
        Some(0),
        "`logweir schema backup-receipt` must exit 0 (GC13 as revised), stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let checked_in =
        std::fs::read("../../schemas/logweir-backup-receipt-1.0.0.json").expect("read the schema");
    assert_eq!(
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&checked_in),
        "stdout must be the published schema byte for byte — including the trailing \
         newline. Run `just schema` if the file is stale."
    );
}

/// `logweir schema switchover-record` exits 1 and names **tag 2**.
///
/// Critique A F31: it used to fall through the catch-all, whose message named
/// no tag. "unknown schema `switchover-record`" reads as "there is no such
/// thing" to an operator who has read tag 2's design and typed the obvious
/// command; every other deferred schema in that function names the thing that
/// introduces it.
#[test]
fn schema_rejects_switchover_record_in_tag_1() {
    let out = bin()
        .args(["schema", "switchover-record"])
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(1),
        "`logweir schema switchover-record` must exit 1, stdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(
        err.contains("introduced by tag 2 (Switchover)"),
        "the refusal must name the tag that introduces it, not merely refuse; the \
         catch-all message names no tag, which is the whole reason this arm exists. \
         got: {err}"
    );
}
