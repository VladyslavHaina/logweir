//! Exit-code contract tests beyond `cli_verify.rs`'s four brief-mandated
//! cases. Global Constraint 11 reserves exit code 2 for "a drill result that
//! is not a pass — a scorecard IS written and signed", and `2` is exactly
//! the code clap's `Parser::parse()` hardcodes for EVERY usage error
//! (`Error::exit()` in `clap_builder`). Left unhandled, a typo'd flag or a
//! flag a later chart bump drops would report "the drill ran and did not
//! pass — go fetch the signed scorecard" when no drill ran and nothing was
//! written. `main.rs` now uses `try_parse` and maps any stderr-routed clap
//! error to `ExitCode::Operational` (1) instead, while `--help`/`--version`
//! (routed to stdout) keep exiting 0. These assert the real process exit
//! status for each shape of usage error, plus the two Global-Constraint-13
//! branches (`snapshot`/`diff` vs `plan`) that only `plan` had process-level
//! coverage for before.

use std::process::Command;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_logweir"))
}

#[test]
fn no_arguments_exits_operational_not_a_drill_result() {
    let out = bin().output().unwrap();
    assert_eq!(
        out.status.code(),
        Some(1),
        "a bare invocation is a usage error, not a drill result: stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn drill_verify_with_no_arguments_exits_operational_not_a_drill_result() {
    // The exact case the finding names: missing required flags on a real
    // subcommand must not be reported as exit code 2.
    let out = bin().args(["drill", "verify"]).output().unwrap();
    assert_eq!(
        out.status.code(),
        Some(1),
        "missing required flags is a usage error, not a drill result: stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn an_unknown_flag_exits_operational() {
    let out = bin().args(["drill", "verify", "--nope"]).output().unwrap();
    assert_eq!(
        out.status.code(),
        Some(1),
        "an unrecognised flag is a usage error, not a drill result: stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn an_unknown_subcommand_exits_operational() {
    let out = bin().args(["frobnicate"]).output().unwrap();
    assert_eq!(
        out.status.code(),
        Some(1),
        "an unrecognised subcommand is a usage error, not a drill result: stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn help_still_exits_ok() {
    let out = bin().args(["--help"]).output().unwrap();
    assert_eq!(out.status.code(), Some(0));
    assert!(!out.stdout.is_empty());
}

#[test]
fn version_still_exits_ok() {
    let out = bin().args(["--version"]).output().unwrap();
    assert_eq!(out.status.code(), Some(0));
    assert!(!out.stdout.is_empty());
}

#[test]
fn schema_snapshot_exits_1_naming_the_sub_project() {
    // Global Constraint 13 names `snapshot` and `diff` explicitly, sharing a
    // branch distinct from `plan`'s; only `plan` had process-level coverage.
    let out = bin().args(["schema", "snapshot"]).output().unwrap();
    assert_eq!(out.status.code(), Some(1));
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(
        err.contains("SP2"),
        "must name the sub-project that introduces it, got: {err}"
    );
}

#[test]
fn drill_verify_exits_signing_or_lock_on_a_payload_type_mismatch() {
    // Controller ruling on the deferred finding: a payload_type mismatch is
    // evidence of SUBSTITUTION (a genuinely-signed sidecar for a different
    // kind of document, handed over in place of a scorecard), not
    // corruption — so it must map to exit 4, the same code a bad signature
    // does, not exit 1.
    let dir = tempfile::tempdir().unwrap();
    let sc = dir.path().join("s.json");
    let sig = dir.path().join("s.sig");
    let pubk = dir.path().join("pub.pem");
    std::fs::copy("../../e2e/fixtures/signed/scorecard.json", &sc).unwrap();
    std::fs::copy("../../e2e/fixtures/signed/public.pem", &pubk).unwrap();

    let good_sig = std::fs::read_to_string("../../e2e/fixtures/signed/scorecard.sig").unwrap();
    let mut sidecar: serde_json::Value = serde_json::from_str(&good_sig).unwrap();
    sidecar["payloadType"] = serde_json::Value::String(
        "application/vnd.logweir.drill-teardown+json;version=1.0.0".to_string(),
    );
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
        Some(4),
        "a payload_type mismatch must exit 4 (substitution), not 1: stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}
