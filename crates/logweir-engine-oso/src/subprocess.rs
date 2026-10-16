//! The local subprocess runner for the pinned `kafka-backup` CLI, plus the
//! unknown-config-key readback.
//!
//! v0.1 execution model: a LOCAL SUBPROCESS on the binary extracted at image
//! build time from the digest-pinned image. No kube client, no
//! container runtime, no kubeconfig — which is what makes "does not require
//! Kubernetes" true rather than aspirational.
use logweir_core::engine::EngineError;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

/// **PROD-07.2: a stop reaches the engine.** Set once by [`request_stop`]
/// (the runner's SIGTERM or SIGINT); never cleared. While it is set no engine
/// is started, and the run signs nothing.
static STOP: AtomicBool = AtomicBool::new(false);

/// The engine children this process is waiting for, each under the id of the
/// [`run_engine`] call that started it. A child stays here until it has been
/// reaped, so a signal sent while the lock is held never reaches a reused PID.
static CHILDREN: Mutex<Vec<(u64, Child)>> = Mutex::new(Vec::new());
static NEXT_CHILD: AtomicU64 = AtomicU64::new(0);

/// How long an engine has between the forwarded SIGTERM and the SIGKILL.
pub const STOP_GRACE: Duration = Duration::from_secs(10);

/// Whether this process was asked to stop.
#[must_use]
pub fn stop_requested() -> bool {
    STOP.load(Ordering::SeqCst)
}

/// **Stop the engine**: remember the stop, send SIGTERM to every engine child,
/// and SIGKILL each one still running [`STOP_GRACE`] later. Called by the
/// runner's signal handler; idempotent.
pub fn request_stop() {
    if STOP.swap(true, Ordering::SeqCst) {
        return;
    }
    let ids: Vec<u64> = match CHILDREN.lock() {
        Ok(children) => children
            .iter()
            .map(|(id, child)| {
                // `pid_t` fits every PID this process can hold.
                let pid = nix::unistd::Pid::from_raw(child.id() as i32);
                let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGTERM);
                *id
            })
            .collect(),
        Err(_) => return,
    };
    if ids.is_empty() {
        return;
    }
    std::thread::spawn(move || {
        std::thread::sleep(STOP_GRACE);
        if let Ok(mut children) = CHILDREN.lock() {
            for (id, child) in children.iter_mut() {
                if ids.contains(id) {
                    // `Child::kill` is SIGKILL.
                    let _ = child.kill();
                }
            }
        }
    });
}

/// The argument that makes the `logweir` binary the engine's launcher
/// ([`exec_engine`]). Not a subcommand: it is never typed.
pub const ENGINE_EXEC_ARG: &str = "__engine-exec";

/// The launcher [`run_engine`] starts the engine through, when the binary
/// registered one ([`launch_engines_through`]).
static LAUNCHER: OnceLock<PathBuf> = OnceLock::new();

/// **Linux: the engine dies with the runner.** The `logweir` binary registers
/// itself here at start; every engine is then started as `logweir
/// __engine-exec <parent pid> <engine> <args…>`, which sets
/// `PR_SET_PDEATHSIG(SIGKILL)` and execs the engine ([`exec_engine`]). The
/// death signal survives `exec`, so a SIGKILLed runner leaves no engine
/// writing. Library callers (tests) never register one and start the engine
/// directly.
pub fn launch_engines_through(launcher: PathBuf) {
    if cfg!(target_os = "linux") {
        let _ = LAUNCHER.set(launcher);
    }
}

/// The launcher's body: `args` is `<parent pid> <engine> <args…>`. Returns
/// only when it could not exec, with the exit code to use.
#[must_use]
pub fn exec_engine(args: &[std::ffi::OsString]) -> i32 {
    use std::os::unix::process::CommandExt as _;
    let (Some(parent), Some(binary)) = (args.first(), args.get(1)) else {
        eprintln!("{ENGINE_EXEC_ARG}: expected <parent pid> <engine> [args…]");
        return 1;
    };
    #[cfg(target_os = "linux")]
    {
        if let Err(e) = nix::sys::prctl::set_pdeathsig(nix::sys::signal::Signal::SIGKILL) {
            eprintln!("{ENGINE_EXEC_ARG}: PR_SET_PDEATHSIG failed: {e}");
            return 1;
        }
        // The parent may have died before the death signal was armed.
        if parent.to_str() != Some(&nix::unistd::getppid().to_string()) {
            eprintln!("{ENGINE_EXEC_ARG}: the runner is gone; the engine is not started");
            return 1;
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = parent;
    let err = Command::new(binary).args(&args[2..]).exec();
    eprintln!("{ENGINE_EXEC_ARG}: {}: {err}", Path::new(binary).display());
    127
}

/// The destination CA bundle the controller projected into the runner pod.
/// Set only for a destination-backed run whose `BackupDestination`
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

/// The `stream` label [`engine_trust_bundle`]'s own lines carry. Not `stdout`
/// or `stderr`: those two are the engine CHILD's, and a reader correlating a
/// run's output with the child's must be able to tell them apart.
pub const LOGWEIR_STREAM: &str = "logweir";

/// The trust bundle this process hands the engine child, or `None` when no
/// destination CA was projected: the engine trust bundle.
///
/// # Why a FILE and not a flag
///
/// The pinned engine takes no CA argument for its object-store client. What it
/// is claimed to honour —
/// `[UNVERIFIED — U1: whether the engine honours a custom CA file]` — is
/// `SSL_CERT_FILE`, through rustls-platform-verifier on Linux. Until that is
/// measured on a recorded engine digest the controller REFUSES a destination
/// carrying a `caBundle` for engine-driven runs
/// (`weirkeeper::controllers::backup::ENGINE_CUSTOM_CA_VERIFIED`), so this path
/// is reachable only under the administrator's explicit
/// `engine.allowUnverifiedCustomCa` opt-in.
///
/// # It is written, not cached
///
/// Rewritten on every engine invocation: a run makes a handful of them, the
/// bundle is at most 64 KiB plus the platform store, and a cache keyed on
/// nothing would serve a stale root after a rotation. A failure to read either
/// half returns `None` WITH a line on the caller's log rather than an error —
/// the engine then uses the platform store alone and fails its handshake with
/// the engine's own message, which is no worse than not having tried.
/// # The log sink
///
/// `on_line` is the CALLER's, the same sink the engine child's own output goes
/// to — `logweir-engine-oso` declares no logging dependency, and adding one for
/// three warnings would put a package decision in front of a diagnostic. The
/// stream label is `logweir`, not `stdout`/`stderr`, because these lines are
/// this process's and not the child's.
#[must_use]
pub fn engine_trust_bundle(on_line: &mut dyn FnMut(&str, &str)) -> Option<PathBuf> {
    let destination = std::env::var(ARCHIVE_CA_FILE_ENV)
        .ok()
        .filter(|v| !v.is_empty())?;
    let extra = match std::fs::read(&destination) {
        Ok(bytes) => bytes,
        Err(e) => {
            // THE PLAN SAID THERE IS A ROOT AND THE MOUNT DOES NOT HAVE IT.
            // Running anyway is the right call — the engine may not need it —
            // but doing so SILENTLY is how the opaque handshake failure U1
            // warns about arrives with nothing in the log to explain it.
            on_line(
                LOGWEIR_STREAM,
                &format!(
                    "warn: {ARCHIVE_CA_FILE_ENV}={destination} cannot be read ({e}); the engine \
                     child runs against the platform trust store alone, and a private-CA \
                     endpoint will fail its TLS handshake with the engine's own message"
                ),
            );
            return None;
        }
    };
    // THE SYSTEM BUNDLE IS THE BASE AND ITS ABSENCE IS NOT A DETAIL.
    // `SSL_CERT_FILE` REPLACES the trust store rather than adding to it, so a
    // merged file built on an empty base is the private root ALONE — the engine
    // would then reject every public certificate, including the source
    // cluster's on an installation where only the archive is private. Refusing
    // to write the file at all leaves the platform store in place, which is
    // strictly the better of the two failures.
    let mut merged =
        match std::fs::read(SYSTEM_CA_BUNDLE) {
            Ok(bytes) if !bytes.is_empty() => bytes,
            other => {
                on_line(
                    LOGWEIR_STREAM,
                    &format!(
                    "warn: the platform trust store {SYSTEM_CA_BUNDLE} is missing or empty ({}), \
                     so no merged bundle is written: SSL_CERT_FILE REPLACES the trust store, and \
                     a file holding only the destination root would leave the engine unable to \
                     verify any public certificate at all",
                    other.err().map_or_else(|| "empty".to_string(), |e| e.to_string())
                ),
                );
                return None;
            }
        };
    if !merged.ends_with(b"\n") {
        merged.push(b'\n');
    }
    merged.extend_from_slice(&extra);
    let path = PathBuf::from(ENGINE_TRUST_BUNDLE);
    let parent = path.parent()?;
    if let Err(e) = std::fs::create_dir_all(parent) {
        on_line(
            LOGWEIR_STREAM,
            &format!(
                "warn: {} cannot be created ({e}); the engine child runs against the platform \
                 trust store alone",
                parent.display()
            ),
        );
        return None;
    }
    if let Err(e) = std::fs::write(&path, &merged) {
        on_line(
            LOGWEIR_STREAM,
            &format!(
                "warn: {} cannot be written ({e}); the engine child runs against the platform \
                 trust store alone",
                path.display()
            ),
        );
        return None;
    }
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

/// Runs `binary` with `args`, capturing stdout/stderr in full (a batch
/// capture — the child is not streamed live; every line is handed to
/// `on_line` only after the process has exited), and extracts every
/// unknown-config-key warning.
///
/// The child is spawned and supervised rather than run with
/// `Command::output`, so a stop ([`request_stop`]) reaches it: SIGTERM, then
/// SIGKILL after [`STOP_GRACE`]. Once a stop was requested no engine is
/// started.
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
/// Scanning stdout is therefore the load-bearing case, and this function scans
/// BOTH streams anyway: stdout because that is where the real binary puts it,
/// and stderr as well because (a) it costs nothing extra, (b) a future engine
/// tag could route logs differently (upstream's tracing setup is not part of
/// Logweir's contract, only its OBSERVED behaviour is), and (c)
/// `e2e/fixtures/fake-engine.sh` — this crate's stand-in for the real binary —
/// happens to write the warning to stderr, so scanning stderr too is what lets
/// that fixture stand in for the real binary at all. Scanning only stderr, the
/// naive assumption, would return an empty vector forever against the real
/// engine and let `assert_no_dropped_logweir_key` (engine.rs) pass silently for
/// every dropped key — the exact failure mode this whole mechanism exists to
/// catch.
pub fn run_engine(
    binary: &Path,
    args: &[&str],
    on_line: &mut dyn FnMut(&str, &str),
) -> Result<EngineRun, EngineError> {
    let mut command = match LAUNCHER.get() {
        Some(launcher) => {
            let mut c = Command::new(launcher);
            c.arg(ENGINE_EXEC_ARG)
                .arg(std::process::id().to_string())
                .arg(binary);
            c
        }
        None => Command::new(binary),
    };
    command
        .args(args)
        // `warn`, not `error`: the unknown-key readback depends on the
        // WARN-level line surviving, while everything at info level would
        // otherwise interleave with the JSON that `preflight()` parses.
        .env("RUST_LOG", "warn");
    // ON THIS SUBPROCESS ONLY. `SSL_CERT_FILE` is a process-wide
    // replacement of the trust store, and Logweir's OWN rustls clients (the
    // evidence store, the verifier, the check runner) already take the
    // destination CA explicitly through `StoreOptions::with_root_certificate`.
    // Setting it on this process would change what those clients trust as a
    // side effect of running the engine.
    if let Some(bundle) = engine_trust_bundle(on_line) {
        command.env("SSL_CERT_FILE", &bundle);
    }
    let (status, stdout, stderr) = supervise(command, binary)?;
    let stdout = String::from_utf8_lossy(&stdout).into_owned();
    let stderr = String::from_utf8_lossy(&stderr).into_owned();
    for l in stderr.lines() {
        on_line("stderr", l);
    }
    for l in stdout.lines() {
        on_line("stdout", l);
    }
    Ok(EngineRun {
        exit_code: status.code().unwrap_or(-1),
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

/// Spawn `command` with both streams piped, register the child for
/// [`request_stop`], and wait for it while draining both pipes.
fn supervise(
    mut command: Command,
    binary: &Path,
) -> Result<(std::process::ExitStatus, Vec<u8>, Vec<u8>), EngineError> {
    use std::io::Read as _;
    let failed = |e: std::io::Error| EngineError::Operational(format!("{}: {e}", binary.display()));
    let id = NEXT_CHILD.fetch_add(1, Ordering::SeqCst);
    let (out, err) = {
        // The stop check and the registration under one lock, so a stop
        // either finds this child or prevents it.
        let mut children = CHILDREN
            .lock()
            .map_err(|_| EngineError::Operational("the engine registry is poisoned".into()))?;
        if stop_requested() {
            return Err(EngineError::Operational(
                "the run was stopped (SIGTERM or SIGINT) before the engine started".into(),
            ));
        }
        let mut child = command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(failed)?;
        let out = child.stdout.take();
        let err = child.stderr.take();
        children.push((id, child));
        (out, err)
    };
    // Each pipe is drained on its own thread, which hands its bytes back on a
    // channel: after a stop, a grandchild that kept a pipe open (a shell's
    // `sleep`, the docker client of `engine-docker.sh`) must not hold the run
    // past the pod's grace, so the wait for the bytes is then bounded.
    let drain = |pipe: Option<Box<dyn std::io::Read + Send>>| {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut bytes = Vec::new();
            if let Some(mut p) = pipe {
                let _ = p.read_to_end(&mut bytes);
            }
            let _ = tx.send(bytes);
        });
        rx
    };
    let out = drain(out.map(|p| Box::new(p) as Box<dyn std::io::Read + Send>));
    let err = drain(err.map(|p| Box::new(p) as Box<dyn std::io::Read + Send>));
    let status = loop {
        {
            let mut children = CHILDREN
                .lock()
                .map_err(|_| EngineError::Operational("the engine registry is poisoned".into()))?;
            let Some(i) = children.iter().position(|(c, _)| *c == id) else {
                return Err(EngineError::Operational(
                    "the engine child left the registry".into(),
                ));
            };
            match children[i].1.try_wait() {
                Ok(Some(status)) => {
                    children.remove(i);
                    break status;
                }
                Ok(None) => {}
                Err(e) => {
                    let (_, mut child) = children.remove(i);
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(failed(e));
                }
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let collect = |rx: std::sync::mpsc::Receiver<Vec<u8>>| -> Vec<u8> {
        if stop_requested() {
            rx.recv_timeout(Duration::from_secs(2)).unwrap_or_default()
        } else {
            rx.recv().unwrap_or_default()
        }
    };
    Ok((status, collect(out), collect(err)))
}
