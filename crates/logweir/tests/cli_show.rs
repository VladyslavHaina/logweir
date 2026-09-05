//! `logweir drill show`, exercised through the COMPILED BINARY.
//!
//! Task 22, carried obligation 1. Every other test of the renderer calls
//! `logweir::show::render_table` or `show::run` from the library. Those all
//! passed while `main.rs`'s `_ =>` arm swallowed `DrillCmd::Show` and printed
//! "not yet implemented in this task" — a subcommand can be fully implemented,
//! fully golden-tested, and still absent from the artifact users install.
//!
//! So these tests spawn `CARGO_BIN_EXE_logweir` and assert on the process's
//! own stdout and exit status. A library-only test cannot detect a missing
//! dispatch arm; only this file can.

use std::path::PathBuf;
use std::process::Command;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_logweir"))
}

/// `e2e/fixtures/scorecard-pass.json`, resolved from this crate's manifest dir
/// so the test does not depend on the working directory `cargo test` picks.
fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../e2e/fixtures/scorecard-pass.json")
        .canonicalize()
        .expect("the checked-in scorecard fixture exists")
}

#[test]
fn the_binary_dispatches_drill_show_and_renders_the_table() {
    let out = bin()
        .args(["drill", "show"])
        .arg(fixture())
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        !stderr.contains("not yet implemented"),
        "the binary still routes `drill show` to a stub: {stderr}"
    );
    assert_eq!(
        out.status.code(),
        Some(0),
        "`drill show` over a valid scorecard exits 0; stderr: {stderr}"
    );
    // Three rows that only the real renderer produces. A stub that printed
    // nothing, or echoed the JSON, fails here at assertion time.
    for needle in [
        "logweir drill scorecard",
        "rto excluding preflight",
        "<- compared against objectives.rto_seconds",
    ] {
        assert!(
            stdout.contains(needle),
            "the rendered table is missing {needle:?}:\n{stdout}"
        );
    }
}

#[test]
fn the_binary_dispatches_drill_show_format_json_as_the_exact_stored_bytes() {
    let path = fixture();
    let out = bin()
        .args(["drill", "show"])
        .arg(&path)
        .args(["--format", "json"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0));
    assert_eq!(
        out.stdout,
        std::fs::read(&path).unwrap(),
        "`--format json` must emit the bytes as stored, never a re-serialisation"
    );
}

/// GC13's precedent: an argument the binary does not implement exits 1 naming
/// what is missing. It must never exit 0, which would tell a CronJob the drill
/// surface it asked for exists.
#[test]
fn an_unknown_show_format_exits_operational_and_names_the_accepted_ones() {
    let out = bin()
        .args(["drill", "show"])
        .arg(fixture())
        .args(["--format", "sideways"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("table") && stderr.contains("json"),
        "the refusal must name the formats that do work: {stderr}"
    );
}

#[test]
fn showing_a_file_that_is_not_a_scorecard_exits_operational_not_ok() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("not-a-scorecard.json");
    std::fs::write(&p, b"{\"hello\":\"world\"}").unwrap();
    let out = bin().args(["drill", "show"]).arg(&p).output().unwrap();
    assert_eq!(
        out.status.code(),
        Some(1),
        "a document that is not a scorecard must not exit 0"
    );
}

/// The structural half of carried obligation 1: no subcommand arm may print
/// "not yet implemented" and hand back a success-shaped code. `main.rs` has no
/// catch-all any more, so this walks every subcommand the CLI advertises and
/// asserts none of them answers with that string.
#[test]
fn no_subcommand_is_still_a_not_yet_implemented_stub() {
    // Each entry is a MINIMAL invocation of one advertised subcommand. Most
    // exit non-zero here (missing files, missing flags) — that is fine and is
    // not what this test measures. What it measures is that none of them is
    // answered by a stub.
    for argv in [
        vec!["schema", "scorecard"],
        vec!["drill", "show", "/nonexistent.json"],
        vec![
            "drill",
            "verify",
            "--scorecard",
            "/nonexistent.json",
            "--signature",
            "/nonexistent.sig",
            "--public-key",
            "/nonexistent.pem",
        ],
        vec![
            "doctor",
            "--spec",
            "/nonexistent.yaml",
            "--allowed-clusters",
            "/nonexistent.json",
            "--approver-key",
            "/nonexistent.pem",
        ],
        vec![
            "drill",
            "run",
            "--spec",
            "/nonexistent.yaml",
            "--approval",
            "/nonexistent.json",
            "--approver-key",
            "/nonexistent.pem",
            "--allowed-clusters",
            "/nonexistent.json",
            "--signing-key",
            "/nonexistent.pem",
        ],
    ] {
        let out = bin().args(&argv).output().unwrap();
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            !stderr.contains("not yet implemented"),
            "`logweir {}` is a stub: {stderr}",
            argv.join(" ")
        );
    }
}
