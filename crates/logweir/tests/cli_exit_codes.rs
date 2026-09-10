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

// ------------------------------------------------------------------- [I9]
// The refusal-reason contract, at the process level.
//
// Global Constraint 11: every guard refusal prints
// `refusal-reason=<TerminalState>` as its **final stdout line**. Stdout and
// not stderr because the pod log API has no stream selector — it returns the
// container's two streams interleaved with no marker saying which byte came
// from which, so nothing a runner writes on stderr is distinguishable by a
// controller (spec §7 amendment 4). Last because a controller tailing the log
// reads the final line.
//
// Both arms read the exit code DIRECTLY from `Command::status()` (STANDING
// RULE 20 / GC11: never through a pipe) and capture stdout by handing the
// child a real FILE, so there is no pipe anywhere in either assertion.

/// Runs `logweir drill run` over `spec_text` with the shipped example
/// approval, key and allowed-clusters files, and returns
/// `(exit_code, stdout_text)`.
///
/// `env_remove` on the two password variables is not decoration: a developer
/// with `LOGWEIR_SOURCE_PASSWORD` exported would otherwise see the credential
/// guard fire in the spec arm, and the failure would look like a bug in the
/// spec arm.
fn drill_run_capturing_stdout(spec_text: &str, env: &[(&str, &str)]) -> (Option<i32>, String) {
    let dir = tempfile::tempdir().unwrap();
    let spec = dir.path().join("drill.yaml");
    std::fs::write(&spec, spec_text).unwrap();
    let out_path = dir.path().join("stdout.txt");
    let out_file = std::fs::File::create(&out_path).unwrap();

    let mut cmd = bin();
    cmd.args(["drill", "run", "--spec"])
        .arg(&spec)
        .args([
            "--approval",
            "../../examples/approval.json",
            "--approver-key",
            "../../e2e/fixtures/signed/public.pem",
            "--allowed-clusters",
            "../../examples/allowed-clusters.json",
            "--signing-key",
            "../../e2e/fixtures/signed/signing.pem",
        ])
        .env_remove("LOGWEIR_SOURCE_PASSWORD")
        .env_remove("LOGWEIR_TARGET_PASSWORD")
        .stdout(std::process::Stdio::from(out_file));
    for (k, v) in env {
        cmd.env(k, v);
    }
    // The exit code, read from `status()` itself.
    let status = cmd.status().unwrap();
    let stdout = std::fs::read_to_string(&out_path).unwrap();
    (status.code(), stdout)
}

/// The last non-empty line of a captured stdout, which is what a controller
/// tailing `pods/log` reads.
fn final_stdout_line(stdout: &str) -> &str {
    stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .next_back()
        .unwrap_or("")
}

/// [I9] A plain guard refusal — a forbidden key in the spec, refused by
/// `phase0_admit::run` before anything runs — exits 3 and says so on stdout.
///
/// `dry_run: true` is the payload because it is the refusal whose ABSENCE is
/// most expensive: an ordinary YAML `dry_run: true` reaching phase 6 makes the
/// restore a no-op that exits 0, and the drill then signs a scorecard whose
/// RTO was measured around nothing (Global Constraint 4).
#[test]
fn a_guard_refusal_prints_its_terminal_state_as_the_final_stdout_line() {
    let spec = std::fs::read_to_string("../../examples/drill.yaml").unwrap()
        + "\nengine_overrides:\n  dry_run: true\n";
    let (code, stdout) = drill_run_capturing_stdout(&spec, &[]);
    assert_eq!(
        code,
        Some(3),
        "a forbidden key in the spec is a guard refusal (exit 3): stdout:\n{stdout}"
    );
    assert_eq!(
        final_stdout_line(&stdout),
        "refusal-reason=GuardRefused",
        "the reason must be the FINAL stdout line — stderr is not distinguishable \
         through the pod log API:\n{stdout}"
    );
    // Exactly one such line, and it is not repeated on any other exit path.
    assert_eq!(
        stdout
            .lines()
            .filter(|l| l.starts_with("refusal-reason="))
            .count(),
        1,
        "{stdout}"
    );
}

/// [I9]/[I11] A projected password that cannot be substituted into the
/// engine's pre-parse config text is a GUARD REFUSAL — exit 3 — and names the
/// terminal state `CredentialNotRenderable`.
///
/// The value carries a newline, which ends the physical line the placeholder
/// sits on and turns whatever follows into a new YAML key at whatever
/// indentation it has. The refusal happens on the RUNNER, at the moment it
/// reads the variable, before `context` builds any client: `weirkeeper` holds
/// no `get` on Secrets anywhere (spec §9), so the controller never sees the
/// projected value and cannot be the one to refuse it (spec §7 amendment 4).
///
/// The spec here is the CLEAN shipped example, deliberately: it would be
/// admitted, so an exit 3 can only have come from the credential read. That is
/// also what makes the "map the credential refusal to exit 1" mutant
/// attributable — with the mapping changed, this arm fails on the code while
/// the spec arm above still passes.
#[test]
fn a_credential_refusal_is_a_guard_refusal() {
    let spec = std::fs::read_to_string("../../examples/drill.yaml").unwrap();
    let (code, stdout) = drill_run_capturing_stdout(
        &spec,
        &[("LOGWEIR_SOURCE_PASSWORD", "hunter2\n bootstrap_servers:")],
    );
    assert_eq!(
        code,
        Some(3),
        "an unrenderable projected credential is refused before anything runs \
         (exit 3, not 1): stdout:\n{stdout}"
    );
    assert_eq!(
        final_stdout_line(&stdout),
        "refusal-reason=CredentialNotRenderable",
        "the controller's only machine-readable channel is the final stdout line:\n{stdout}"
    );
    // THE VALUE NEVER APPEARS. Not in the reason line, not in the structured
    // logs the same stream carries.
    assert!(
        !stdout.contains("hunter2"),
        "no fragment of the projected credential may reach any stream:\n{stdout}"
    );
    assert!(
        !stdout.contains("bootstrap_servers:"),
        "no fragment of the projected credential may reach any stream:\n{stdout}"
    );

    // The TARGET variable is checked too, and to the same state.
    let (code, stdout) = drill_run_capturing_stdout(
        &spec,
        &[("LOGWEIR_TARGET_PASSWORD", "hunter2\"and-a-quote")],
    );
    assert_eq!(code, Some(3), "stdout:\n{stdout}");
    assert_eq!(
        final_stdout_line(&stdout),
        "refusal-reason=CredentialNotRenderable",
        "{stdout}"
    );

    // And a RENDERABLE password is NOT refused by this guard. Proved by
    // pairing it with a spec that phase 0 refuses for its own reason: the run
    // still exits 3, but the state is `GuardRefused` and not
    // `CredentialNotRenderable`. A guard that refused every password would
    // pass the two assertions above while being useless, and this is the
    // discrimination that catches it.
    //
    // It is deliberately NOT proved by letting a renderable password run on
    // into the drill: that path reaches `context`, dials the target cluster
    // and spends `rdkafka_reader.rs:16`'s 20 s `const T` waiting for a broker
    // that is not there — measured at 20.8 s, over Global Constraint 22's 15 s
    // per-test bound, in a suite whose whole premise is that it dials nothing.
    let refused_spec = format!("{spec}\nengine_overrides:\n  dry_run: true\n");
    let (code, stdout) = drill_run_capturing_stdout(
        &refused_spec,
        &[("LOGWEIR_SOURCE_PASSWORD", "hunter2-Aa1!")],
    );
    assert_eq!(code, Some(3), "stdout:\n{stdout}");
    assert_eq!(
        final_stdout_line(&stdout),
        "refusal-reason=GuardRefused",
        "a renderable password must not colour another guard's refusal as a \
         credential problem:\n{stdout}"
    );
}
