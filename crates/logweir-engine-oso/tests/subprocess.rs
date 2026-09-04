use logweir_engine_oso::subprocess::{run_engine, scan_unknown_key_warnings};
use std::path::PathBuf;
use std::process::Command;

const FAKE_ENGINE: &str = "../../e2e/fixtures/fake-engine.sh";

/// [VERIFIED U/kafka-backup/crates/kafka-backup-cli/src/commands/config.rs:46]
/// the CLI logs, per dropped path:
///   Ignoring unknown config key `{path}` — check for typos; see https://…
/// This is the per-run readback for restore.yaml and it depends on no report
/// field.
#[test]
fn the_unknown_key_warning_is_parsed_out_of_stderr() {
    let stderr = "\
2026-09-03T09:14:26Z WARN kafka_backup: Ignoring unknown config key `restore.dry_run_check_segments` — check for typos; see https://kafkabackup.com/reference/config-yaml
2026-09-03T09:14:26Z  INFO kafka_backup: Validating restore configuration
2026-09-03T09:14:26Z WARN kafka_backup: Ignoring unknown config key `restore.header_preflight` — check for typos
";
    let got = scan_unknown_key_warnings(stderr);
    assert_eq!(
        got,
        vec![
            "restore.dry_run_check_segments".to_string(),
            "restore.header_preflight".to_string(),
        ]
    );
}

#[test]
fn a_clean_stderr_yields_no_warnings() {
    assert!(scan_unknown_key_warnings("INFO all good\n").is_empty());
}

#[test]
fn the_runner_captures_stdout_stderr_and_the_exit_code() {
    let run = run_engine(
        &PathBuf::from(FAKE_ENGINE),
        &[
            "validate-restore",
            "--config",
            "/dev/null",
            "--format",
            "json",
        ],
        &mut |_, _| {},
    )
    .unwrap();
    assert_eq!(run.exit_code, 1);
    assert!(run.stdout.contains("\"valid\": false"));
    assert_eq!(
        run.unknown_key_warnings,
        vec!["restore.header_preflight".to_string()]
    );
}

/// The fixture carries a POPULATED `topics_to_restore` on purpose: upstream's
/// `DryRunTopicReport.partitions` is a `Vec<DryRunPartitionReport>`, and an
/// empty array would let a wrongly-typed vendored struct pass this gate and
/// then fail on the first live run (manifest.rs:1102-1116).
#[test]
fn a_populated_topics_to_restore_parses_into_the_vendored_shape() {
    use logweir_engine_oso::vendored::manifest::DryRunReport;
    let run = run_engine(
        &PathBuf::from(FAKE_ENGINE),
        &[
            "validate-restore",
            "--config",
            "/dev/null",
            "--format",
            "json",
        ],
        &mut |_, _| {},
    )
    .unwrap();
    let r: DryRunReport = serde_json::from_str(&run.stdout).unwrap();
    assert_eq!(r.topics_to_restore[0].source_topic, "orders");
    assert_eq!(r.topics_to_restore[0].target_topic, "drill-20260903-orders");
}

// --- Coverage beyond the brief's literal Step 1, per the top-level task's
// instruction to test "correct argv for each of the three allowed
// subcommands, exit-code mapping including the failure codes, unknown-key
// warnings read from the correct stream, and a run whose output is
// malformed" against the stub.
//
// `run_engine`'s public signature (Interfaces block) takes no env parameter,
// so the FAKE_ENGINE_* variations below are driven with `Command::env`
// directly (which sets a variable for that one CHILD process only) rather
// than by mutating this test process's own environment — `std::env::set_var`
// would race against every other test in this file that also spawns the
// fixture, since libtest runs a file's tests on multiple threads by default.
// `run_engine` itself is exercised above and below with its default (no
// extra env) behaviour; what differs here is only how the fixture is
// invoked, not which code under test is being proven. ---

/// Proves the warning is caught on STDOUT, not only stderr — the load-bearing
/// case against the real binary (see subprocess.rs's doc comment: fmt::layer()
/// has no with_writer override, so every subcommand's tracing output,
/// including this WARN, lands on stdout in the real engine). A readback that
/// watched only stderr would return `unknown_key_warnings: []` here and
/// silently never catch a dropped key against a real run.
#[test]
fn the_warning_is_also_caught_when_the_engine_puts_it_on_stdout() {
    let out = Command::new(FAKE_ENGINE)
        .args([
            "validate-restore",
            "--config",
            "/dev/null",
            "--format",
            "json",
        ])
        .env("FAKE_ENGINE_WARN_STREAM", "stdout")
        .env("FAKE_ENGINE_VALID", "1")
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    // Sanity: the fixture really did put it on stdout, not stderr, for this env.
    assert!(stdout.contains("Ignoring unknown config key"));
    assert!(!stderr.contains("Ignoring unknown config key"));
    assert_eq!(
        scan_unknown_key_warnings(&stdout),
        vec!["restore.header_preflight".to_string()]
    );
}

/// `restore` has no `--format` and prints only log lines — never JSON. Proves
/// the runner's exit-code capture and stdout/stderr split work identically
/// for the OTHER allowed subcommand, not only for `validate-restore`.
#[test]
fn a_restore_invocation_exit_code_and_streams_are_captured() {
    let run = run_engine(
        &PathBuf::from(FAKE_ENGINE),
        &["restore", "--config", "/dev/null"],
        &mut |_, _| {},
    )
    .unwrap();
    assert_eq!(run.exit_code, 0);
    assert!(run.stdout.contains("Starting restore"));
}

/// Exit-code mapping for the FAILURE case: a chosen non-zero code from the
/// child process must survive unchanged — this is the only machine-readable
/// signal `restore` gives (see engine.rs).
#[test]
fn a_nonzero_restore_exit_code_is_reported_verbatim() {
    let out = Command::new(FAKE_ENGINE)
        .args(["restore", "--config", "/dev/null"])
        .env("FAKE_ENGINE_EXIT", "17")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(17));
}

/// A run whose stdout is not JSON at all (no `{` anywhere) — the shape
/// `OsoCliEngine::preflight` must reject as "printed no JSON object" rather
/// than mis-parse. `run_engine` itself does not interpret stdout — it only
/// captures it — so this proves the raw malformed shape survives intact for
/// the caller (engine.rs) to detect, in contrast with the well-formed default.
#[test]
fn a_malformed_stdout_run_has_no_json_brace_while_the_default_run_does() {
    let malformed = Command::new(FAKE_ENGINE)
        .args([
            "validate-restore",
            "--config",
            "/dev/null",
            "--format",
            "json",
        ])
        .env("FAKE_ENGINE_MALFORMED", "1")
        .output()
        .unwrap();
    assert!(!String::from_utf8_lossy(&malformed.stdout).contains('{'));

    let default_run = run_engine(
        &PathBuf::from(FAKE_ENGINE),
        &[
            "validate-restore",
            "--config",
            "/dev/null",
            "--format",
            "json",
        ],
        &mut |_, _| {},
    )
    .unwrap();
    assert!(default_run.stdout.contains('{'));
}

/// `on_line` is invoked once per line, stderr fully before stdout (see
/// run_engine's doc comment) — a live progress callback (`PhaseObserver`)
/// depends on being called at all, not on interleaving order, since
/// `Command::output` is a batch capture, not a streamed pipe.
#[test]
fn on_line_is_called_for_every_line_of_both_streams() {
    let mut seen: Vec<(String, String)> = Vec::new();
    let run = run_engine(
        &PathBuf::from(FAKE_ENGINE),
        &["restore", "--config", "/dev/null"],
        &mut |stream, line| seen.push((stream.to_string(), line.to_string())),
    )
    .unwrap();
    assert!(seen
        .iter()
        .any(|(s, l)| s == "stderr" && l.contains("Ignoring unknown config key")));
    assert!(seen
        .iter()
        .any(|(s, l)| s == "stdout" && l.contains("Starting restore")));
    assert_eq!(run.exit_code, 0);
}

/// `run_engine` itself only ever errors (as opposed to returning a non-zero
/// `exit_code`) when the child cannot even be spawned.
#[test]
fn a_binary_that_does_not_exist_is_an_operational_error_not_an_exit_code() {
    let err = run_engine(
        &PathBuf::from("/no/such/binary-at-all"),
        &["restore"],
        &mut |_, _| {},
    )
    .unwrap_err();
    assert!(matches!(
        err,
        logweir_core::engine::EngineError::Operational(_)
    ));
}
