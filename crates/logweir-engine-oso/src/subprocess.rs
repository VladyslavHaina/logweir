//! The local subprocess runner for the pinned `kafka-backup` CLI, plus the
//! unknown-config-key readback.
//!
//! v0.1 execution model: a LOCAL SUBPROCESS on the binary extracted at image
//! build time from the digest-pinned image (spec §6 C4). No kube client, no
//! container runtime, no kubeconfig — which is what makes "does not require
//! Kubernetes" true rather than aspirational.
use logweir_core::engine::EngineError;
use std::path::Path;
use std::process::Command;

#[derive(Debug)]
pub struct EngineRun {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub unknown_key_warnings: Vec<String>,
}

/// Extracts the backticked path from every
/// "Ignoring unknown config key `<path>`" line, in order of appearance.
/// The parameter is `text`, not `stderr`, ON PURPOSE — see `run_engine`'s
/// doc comment for why both streams are scanned.
pub fn scan_unknown_key_warnings(text: &str) -> Vec<String> {
    const NEEDLE: &str = "Ignoring unknown config key `";
    text.lines()
        .filter_map(|l| {
            l.split_once(NEEDLE)
                .and_then(|(_, rest)| rest.split_once('`'))
        })
        .map(|(path, _)| path.to_string())
        .collect()
}

/// Runs `binary` with `args`, capturing stdout/stderr in full (this is
/// `Command::output`, a batch capture — the child is not streamed live; every
/// line is handed to `on_line` only after the process has exited), and
/// extracts every unknown-config-key warning.
///
/// # Which stream carries the warning [VERIFIED, both points below]
///
/// The warning is emitted by `tracing::warn!` in `parse_config`
/// [VERIFIED U/kafka-backup/crates/kafka-backup-cli/src/commands/config.rs:46]
/// — `tracing::warn!("Ignoring unknown config key `{path}` — check for typos;
/// see https://kafkabackup.com/reference/config-yaml")`, called identically
/// from both `commands::restore::run` and `commands::validate_restore::run`
/// (both call `super::config::parse_config`). Every subcommand shares exactly
/// ONE global subscriber, built once in `main()`
/// [VERIFIED U/kafka-backup/crates/kafka-backup-cli/src/main.rs:553-556]:
/// `tracing_subscriber::registry().with(fmt::layer()).with(filter).init()` —
/// no `with_writer` call anywhere in that file. `fmt::layer()`'s default
/// `MakeWriter` is `std::io::stdout` [INFERRED from tracing-subscriber 0.3's
/// documented default, pinned at U/kafka-backup/Cargo.toml:44]. So in the REAL
/// binary this warning always lands on stdout, for every subcommand — never on
/// stderr.
///
/// Scanning stdout is therefore the load-bearing case, and this function
/// scans BOTH streams anyway: stdout because that is where the real binary
/// puts it, and stderr as well because (a) it costs nothing extra, (b) a
/// future engine tag could route logs differently (upstream's tracing setup
/// is not part of Logweir's contract, only its OBSERVED behaviour is), and
/// (c) `e2e/fixtures/fake-engine.sh` — this crate's stand-in for the real
/// binary while Docker is unavailable (Task 13) — happens to write the
/// warning to stderr, so scanning stderr too is what lets that fixture stand
/// in for the real binary at all. Scanning only stderr, the naive
/// assumption, would return an empty vector forever against the real engine
/// and let `assert_no_dropped_logweir_key` (engine.rs) pass silently for
/// every dropped key — the exact failure mode this whole mechanism exists to
/// catch.
pub fn run_engine(
    binary: &Path,
    args: &[&str],
    on_line: &mut dyn FnMut(&str, &str),
) -> Result<EngineRun, EngineError> {
    let out = Command::new(binary)
        .args(args)
        // `warn`, not `error`: the unknown-key readback depends on the
        // WARN-level line surviving, while everything at info level would
        // otherwise interleave with the JSON that `preflight()` parses.
        .env("RUST_LOG", "warn")
        .output()
        .map_err(|e| EngineError::Operational(format!("{}: {e}", binary.display())))?;
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    for l in stderr.lines() {
        on_line("stderr", l);
    }
    for l in stdout.lines() {
        on_line("stdout", l);
    }
    Ok(EngineRun {
        exit_code: out.status.code().unwrap_or(-1),
        unknown_key_warnings: {
            // BOTH streams: see the doc comment above.
            let mut v = scan_unknown_key_warnings(&stdout);
            v.extend(scan_unknown_key_warnings(&stderr));
            v
        },
        stdout,
        stderr,
    })
}
