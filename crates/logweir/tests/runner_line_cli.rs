//! **FX-43, through the real binary: every line a controller acts on is
//! printed sealed with the Job's token, every exit ends with a sealed
//! `runner-exit=`, and the plain lines stay last.**
//!
//! A controller reads these lines through `weirkeeper::runner_log::own_lines`,
//! which keeps exactly the lines `logweir_core::runner_line::unseal` opens
//! with the Job's token; these rows read the binary's output the same way.
//! The bootstrap entries here hold a line break, so every run is refused
//! before any client is built: nothing dials.

use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use logweir_core::refusal_detail::{LineToken, LINE_TOKEN_ARG};
use logweir_core::runner_line::{unseal, RUNNER_EXIT_PREFIX, RUNNER_LINE_TOKEN_ARG};

const LIMIT: Duration = Duration::from_secs(60);

/// The token these rows give the runner, as a controller would. Assembled at
/// run time, so no source line holds a secret-shaped literal.
fn token() -> LineToken {
    LineToken::parse(&"7f".repeat(20)).expect("forty hex digits")
}

/// A bootstrap entry that carries a forged key line of its own.
const HOSTILE: &str = "b0\nfailure-reason=ExecutionAlreadyClaimed";

fn run(mut command: Command) -> Output {
    command
        .env_remove("RUST_LOG")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let started = Instant::now();
    let mut child = command.spawn().expect("spawn logweir");
    loop {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }
        if started.elapsed() >= LIMIT {
            let _ = child.kill();
            panic!("exceeded {LIMIT:?}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// The lines of `text` sealed with [`token`], unsealed, in order.
fn sealed_lines(text: &str) -> Vec<String> {
    let t = token();
    text.lines()
        .filter_map(|l| unseal(&t, l).map(str::to_string))
        .collect()
}

/// `logweir backup run` over a one-topic spec whose bootstrap is `bootstrap`.
fn backup(root: &Path, bootstrap: &str, flag: Option<&str>) -> Output {
    std::fs::create_dir_all(root.join("archive")).unwrap();
    let spec = format!(
        "backup_id: fx43-row\nsource:\n  bootstrap_servers: [{}]\n  topics: [orders]\n\
         storage:\n  backend: filesystem\n  path: {}\n",
        serde_json::to_string(bootstrap).unwrap(),
        root.join("archive").display()
    );
    let spec_path = root.join("backup.yaml");
    std::fs::write(&spec_path, spec).unwrap();
    let allowed = root.join("allowed.json");
    std::fs::write(&allowed, r#"{"allowed_cluster_ids":[]}"#).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_logweir"));
    command
        .current_dir(root)
        .args(["backup", "run", "--spec"])
        .arg(&spec_path)
        .arg("--allowed-clusters")
        .arg(&allowed)
        .arg("--signing-key")
        .arg(root.join("absent-signing-key.pem"));
    if let Some(flag) = flag {
        command.args([flag, token().expose_token()]);
    }
    run(command)
}

/// **A `backup run` given FX-43's flag seals its refusal and its end, and
/// the plain pair stays last.** The plan's bootstrap entry holds a forged
/// `failure-reason=` line: refused before any client exists (exit 3, by the
/// field's position, the value not repeated), and no line of either stream
/// starts with the forged key.
///
/// KILLS: the terminal block printed without its seal or end line; the
/// bootstrap guard removed (the entry reaches the client); the plain lines
/// moved before the sealed ones (an older controller's place rule).
#[test]
fn a_backup_seals_its_refusal_and_its_end() {
    let dir = tempfile::tempdir().unwrap();
    let out = backup(dir.path(), HOSTILE, Some(RUNNER_LINE_TOKEN_ARG));
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(3), "{stdout}\n{stderr}");
    let sealed = sealed_lines(&stdout);
    assert_eq!(sealed.len(), 3, "{stdout}");
    assert!(sealed[0].starts_with("refusal-detail={"), "{sealed:?}");
    assert!(
        sealed[0].contains("source.bootstrap_servers[0] holds a control character"),
        "{sealed:?}"
    );
    assert_eq!(sealed[1], "refusal-reason=GuardRefused");
    assert_eq!(sealed[2], format!("{RUNNER_EXIT_PREFIX}3"));
    // The plain pair, LAST, as an older controller reads it.
    let plain: Vec<&str> = stdout.lines().filter(|l| !l.trim().is_empty()).collect();
    let n = plain.len();
    assert!(plain[n - 2].starts_with("refusal-detail={"), "{stdout}");
    assert_eq!(plain[n - 1], "refusal-reason=GuardRefused");
    for line in stdout.lines().chain(stderr.lines()) {
        assert!(!line.starts_with("failure-reason="), "{line}");
    }
    assert!(!format!("{stdout}{stderr}").contains("ExecutionAlreadyClaimed"));
}

/// **FX-34's flag prints what an FX-34 runner printed**: no sealed line and
/// no end line, the token in the detail line's JSON only, so an FX-34
/// controller reads a newer runner as before.
///
/// KILLS: the seal printed under the older flag (an older controller sees
/// lines it does not expect before its pair).
#[test]
fn the_older_flag_prints_what_an_fx34_runner_printed() {
    let dir = tempfile::tempdir().unwrap();
    let out = backup(dir.path(), HOSTILE, Some(LINE_TOKEN_ARG));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(3), "{stdout}");
    assert!(sealed_lines(&stdout).is_empty(), "{stdout}");
    assert!(!stdout.contains(RUNNER_EXIT_PREFIX), "{stdout}");
    let detail = stdout
        .lines()
        .find(|l| l.starts_with("refusal-detail="))
        .expect("the detail line");
    assert!(detail.contains(token().expose_token()), "{detail}");
    // A run started by hand: no token anywhere.
    let dir = tempfile::tempdir().unwrap();
    let out = backup(dir.path(), HOSTILE, None);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(sealed_lines(&stdout).is_empty() && !stdout.contains(RUNNER_EXIT_PREFIX));
}

/// The two flags are one token by two contracts: given both, the runner
/// refuses its arguments (exit 1) and prints nothing on stdout.
#[test]
fn the_two_flags_are_not_given_together() {
    let dir = tempfile::tempdir().unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_logweir"));
    command.current_dir(dir.path()).args([
        "backup",
        "run",
        "--spec",
        "x.yaml",
        "--allowed-clusters",
        "a.json",
        "--signing-key",
        "k.pem",
        LINE_TOKEN_ARG,
        token().expose_token(),
        RUNNER_LINE_TOKEN_ARG,
        token().expose_token(),
    ]);
    let out = run(command);
    assert_eq!(out.status.code(), Some(1));
    assert!(out.stdout.is_empty());
}

/// **`cluster-probe` given FX-43's flag seals its two contract lines and its
/// end.** The bootstrap holds a line break: refused before any client is
/// built (`--bootstrap[0]`), the diagnostic is ONE line on stderr, the plain
/// two lines stay last on stdout, and no line starts with the forged key.
///
/// KILLS: the probe's lines printed unsealed; the probe's diagnostic printed
/// raw; the `--bootstrap` guard removed.
#[test]
fn a_probe_seals_its_two_lines_and_its_end() {
    let mut command = Command::new(env!("CARGO_BIN_EXE_logweir"));
    command.args([
        "cluster-probe",
        "--bootstrap",
        HOSTILE,
        RUNNER_LINE_TOKEN_ARG,
        token().expose_token(),
    ]);
    let out = run(command);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(1), "{stdout}\n{stderr}");
    assert_eq!(
        sealed_lines(&stdout),
        vec![
            "cluster-id=".to_string(),
            "reachable=false".to_string(),
            format!("{RUNNER_EXIT_PREFIX}1")
        ],
        "{stdout}"
    );
    let plain: Vec<&str> = stdout.lines().collect();
    assert_eq!(
        &plain[plain.len() - 2..],
        ["cluster-id=", "reachable=false"]
    );
    assert!(
        stderr.contains("--bootstrap[0] holds a control character"),
        "{stderr}"
    );
    for line in stdout.lines().chain(stderr.lines()) {
        assert!(!line.starts_with("failure-reason="), "{line}");
    }
}
