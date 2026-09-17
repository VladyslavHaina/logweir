//! The local subprocess runner for the pinned `kafka-backup` CLI, plus the
//! unknown-config-key readback.
//!
//! v0.1 execution model: a LOCAL SUBPROCESS on the binary extracted at image
//! build time from the digest-pinned image (spec §6 C4). No kube client, no
//! container runtime, no kubeconfig — which is what makes "does not require
//! Kubernetes" true rather than aspirational.
use logweir_core::engine::EngineError;
use std::path::{Path, PathBuf};
use std::process::Command;

/// The destination CA bundle the controller projected into the runner pod —
/// D2 §3.5. Set only for a destination-backed run whose `BackupDestination`
/// declares `spec.transport.caBundle`.
///
/// It is the CONTROLLER's variable name and this crate reads it rather than
/// being handed a path, for one reason: `OsoCliEngine::new` is constructed in
/// four places across two crates and a fifth parameter threaded through all of
/// them would be `None` at every call site but one. The variable is set on the
/// pod by `ResolvedDestination::job_env` and points into the run's own
/// immutable plan `ConfigMap`.
pub const ARCHIVE_CA_FILE_ENV: &str = "LOGWEIR_ARCHIVE_CA_FILE";

/// The platform trust store a Debian-family runner image carries.
///
/// MERGED WITH, NOT REPLACED BY, the destination CA. `SSL_CERT_FILE` is a
/// REPLACEMENT, not an addition: pointing it at a single private root would
/// leave the engine child unable to verify any public certificate at all —
/// including the source cluster's, on an installation where the archive is
/// private and the brokers are not. The merged file is the destination's root
/// APPENDED to this one.
pub const SYSTEM_CA_BUNDLE: &str = "/etc/ssl/certs/ca-certificates.crt";

/// Where the merged bundle is written, inside the pod's own writable work
/// directory.
pub const ENGINE_TRUST_BUNDLE: &str = "/work/trust/archive-bundle.pem";

/// The trust bundle this process hands the engine child, or `None` when no
/// destination CA was projected — D2 §3.5's "engine trust bundle".
///
/// # Why a FILE and not a flag
///
/// The pinned engine takes no CA argument for its object-store client. What it
/// does honour — [UNVERIFIED, D2 §3.5 U1] — is `SSL_CERT_FILE`, through
/// rustls-platform-verifier on Linux. Until that is measured on a recorded
/// engine digest the controller REFUSES a destination carrying a `caBundle`
/// for engine-driven runs (`weirkeeper::controllers::backup::ENGINE_CUSTOM_CA_VERIFIED`),
/// so this path is reachable only under the administrator's explicit
/// `engine.allowUnverifiedCustomCa` opt-in — which is exactly how D2 §14's
/// scenario S2b measures it.
///
/// # It is written, not cached
///
/// Rewritten on every engine invocation: a run makes a handful of them, the
/// bundle is at most 64 KiB plus the platform store, and a cache keyed on
/// nothing would serve a stale root after a rotation. A failure to read either
/// half returns `None` WITH a line on the caller's log rather than an error —
/// the engine then uses the platform store alone and fails its handshake with
/// the engine's own message, which is no worse than not having tried.
#[must_use]
pub fn engine_trust_bundle() -> Option<PathBuf> {
    let destination = std::env::var(ARCHIVE_CA_FILE_ENV)
        .ok()
        .filter(|v| !v.is_empty())?;
    let extra = std::fs::read(&destination).ok()?;
    let mut merged = std::fs::read(SYSTEM_CA_BUNDLE).unwrap_or_default();
    if !merged.ends_with(b"\n") {
        merged.push(b'\n');
    }
    merged.extend_from_slice(&extra);
    let path = PathBuf::from(ENGINE_TRUST_BUNDLE);
    std::fs::create_dir_all(path.parent()?).ok()?;
    std::fs::write(&path, &merged).ok()?;
    Some(path)
}

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
    let mut command = Command::new(binary);
    command
        .args(args)
        // `warn`, not `error`: the unknown-key readback depends on the
        // WARN-level line surviving, while everything at info level would
        // otherwise interleave with the JSON that `preflight()` parses.
        .env("RUST_LOG", "warn");
    // ON THIS SUBPROCESS ONLY — D2 §3.5. `SSL_CERT_FILE` is a process-wide
    // replacement of the trust store, and Logweir's OWN rustls clients (the
    // evidence store, the verifier, the check runner) already take the
    // destination CA explicitly through `StoreOptions::with_root_certificate`.
    // Setting it on this process would change what those clients trust as a
    // side effect of running the engine.
    if let Some(bundle) = engine_trust_bundle() {
        command.env("SSL_CERT_FILE", &bundle);
    }
    let out = command
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
