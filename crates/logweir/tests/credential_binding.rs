//! PROD-01.3 security follow-up: the RUNNER's half of the credential binding,
//! and the PLAIN-over-TLS rule, on the shipped binary.
//!
//! A controller-built Job carries the EXPECTED binding (a literal the
//! controller computed from the `KafkaCluster`) and the PROJECTED one (the
//! credential Secret's `logweir-binding` key, an optional `secretKeyRef`). Every
//! runner entry point compares them before any client exists. Each row below
//! runs the real `logweir` binary against a loopback SENTINEL listener that
//! must never be dialled, with a negative control: the same command with the
//! binding the controller expects, which must NOT be refused on the binding.

use std::io::ErrorKind;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

const LIMIT: Duration = Duration::from_secs(20);
const EXPECTED: &str = "v1:00000000-0000-0000-0000-000000000001:sha256:abc";
const FOREIGN: &str = "v1:99999999-9999-9999-9999-999999999999:sha256:def";
/// A password the refusal paths must never print.
const SEEDED: &str = "seeded-binding-row-password-7f3a";

fn sentinel() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap().to_string();
    (listener, address)
}

fn assert_no_connection(listener: &TcpListener, label: &str) {
    match listener.accept() {
        Err(error) if error.kind() == ErrorKind::WouldBlock => {}
        Ok((_, peer)) => panic!("{label}: the sentinel was dialled from {peer}"),
        Err(error) => panic!("{label}: inspect the sentinel: {error}"),
    }
}

fn base(root: &Path) -> Command {
    let tmp = root.join("tmp");
    std::fs::create_dir_all(&tmp).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_logweir"));
    command
        .env_clear()
        .env("TMPDIR", &tmp)
        .env("RUST_LOG", "info")
        .env("LOGWEIR_ENGINE_BIN", root.join("no-engine"))
        .env("LOGWEIR_ENGINE_VERSION", "sentinel-version")
        .env("LOGWEIR_ENGINE_DIGEST", "sha256:sentinel-digest");
    command
}

fn run(mut command: Command, label: &str) -> Output {
    command
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
            let out = child.wait_with_output().unwrap();
            panic!(
                "{label}: exceeded {LIMIT:?}; stdout {} stderr {}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn last_line(stdout: &str) -> &str {
    stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .next_back()
        .unwrap_or("")
}

/// `backup run`: the SOURCE side. The binding is checked right after phase
/// −1's local guards — before the signing key is even opened, before any store
/// or client exists — so the signing key path here need not exist.
fn backup(root: &Path, bootstrap: &str, binding: Option<&str>) -> Output {
    let spec = format!(
        "backup_id: binding-row\nsource:\n  bootstrap_servers: [\"{bootstrap}\"]\n  auth:\n    \
         mode: scramSha512\n    username: logweir\n    tls: false\n  topics: [orders]\n\
         storage:\n  backend: filesystem\n  path: {}\n",
        root.join("archive").display()
    );
    std::fs::create_dir_all(root.join("archive")).unwrap();
    let spec_path = root.join("backup.yaml");
    std::fs::write(&spec_path, spec).unwrap();
    let allowed = root.join("allowed.json");
    std::fs::write(&allowed, r#"{"allowed_cluster_ids":[]}"#).unwrap();
    let mut command = base(root);
    command
        .args(["backup", "run", "--spec"])
        .arg(&spec_path)
        .arg("--allowed-clusters")
        .arg(&allowed)
        .arg("--signing-key")
        .arg(root.join("absent-signing-key.pem"))
        .env("LOGWEIR_SOURCE_PASSWORD", SEEDED)
        .env("LOGWEIR_SOURCE_CREDENTIAL_BINDING_EXPECTED", EXPECTED);
    if let Some(b) = binding {
        command.env("LOGWEIR_SOURCE_CREDENTIAL_BINDING", b);
    }
    run(command, "backup")
}

#[test]
fn backup_run_refuses_an_absent_or_foreign_binding_before_anything_runs() {
    for (label, binding) in [("absent", None), ("foreign", Some(FOREIGN))] {
        let root = tempfile::tempdir().unwrap();
        let (listener, address) = sentinel();
        let out = backup(root.path(), &address, binding);
        let stdout = String::from_utf8_lossy(&out.stdout).to_string();
        let stderr = String::from_utf8_lossy(&out.stderr).to_string();
        assert_eq!(out.status.code(), Some(3), "{label}: {stdout}\n{stderr}");
        assert_eq!(
            last_line(&stdout),
            "refusal-reason=CredentialBindingMismatch",
            "{label}: {stdout}"
        );
        assert!(
            stderr.contains("CredentialBindingMismatch"),
            "{label}: {stderr}"
        );
        assert!(
            !stdout.contains(SEEDED) && !stderr.contains(SEEDED),
            "{label}: the password must never be printed"
        );
        assert!(
            !stderr.contains(FOREIGN) && !stderr.contains(EXPECTED),
            "{label}: neither binding is echoed: {stderr}"
        );
        assert_no_connection(&listener, label);
    }
    // NEGATIVE CONTROL: the binding the controller expects. The run passes the
    // binding check and stops later (the signing key does not exist) — and
    // never on the binding.
    let root = tempfile::tempdir().unwrap();
    let (_listener, address) = sentinel();
    let out = backup(root.path(), &address, Some(EXPECTED));
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(
        !stdout.contains("CredentialBindingMismatch")
            && !stderr.contains("CredentialBindingMismatch"),
        "the bound credential must pass the check: {stdout}\n{stderr}"
    );
}

/// `backup run` with SASL/PLAIN and no TLS: phase −1 refuses it with the named
/// reason, before any client exists. Control: PLAIN over TLS passes phase −1.
#[test]
fn backup_run_refuses_plain_without_tls_with_its_named_reason() {
    for (tls, refused) in [(false, true), (true, false)] {
        let root = tempfile::tempdir().unwrap();
        let (listener, address) = sentinel();
        let spec = format!(
            "backup_id: plain-row\nsource:\n  bootstrap_servers: [\"{address}\"]\n  auth:\n    \
             mode: plain\n    username: logweir\n    tls: {tls}\n  topics: [orders]\n\
             storage:\n  backend: filesystem\n  path: {}\n",
            root.path().join("archive").display()
        );
        std::fs::create_dir_all(root.path().join("archive")).unwrap();
        let spec_path = root.path().join("backup.yaml");
        std::fs::write(&spec_path, spec).unwrap();
        let allowed = root.path().join("allowed.json");
        std::fs::write(&allowed, r#"{"allowed_cluster_ids":[]}"#).unwrap();
        let mut command = base(root.path());
        command
            .args(["backup", "run", "--spec"])
            .arg(&spec_path)
            .arg("--allowed-clusters")
            .arg(&allowed)
            .arg("--signing-key")
            .arg(root.path().join("absent-signing-key.pem"))
            .env("LOGWEIR_SOURCE_PASSWORD", SEEDED);
        let out = run(command, "plain");
        let stdout = String::from_utf8_lossy(&out.stdout).to_string();
        let stderr = String::from_utf8_lossy(&out.stderr).to_string();
        if refused {
            assert_eq!(out.status.code(), Some(3), "{stdout}\n{stderr}");
            assert_eq!(
                last_line(&stdout),
                "refusal-reason=PlainWithoutTls",
                "{stdout}"
            );
            assert_no_connection(&listener, "plain without TLS");
        } else {
            assert!(
                !stdout.contains("PlainWithoutTls") && !stderr.contains("PlainWithoutTls"),
                "PLAIN over TLS is accepted by phase −1: {stdout}\n{stderr}"
            );
        }
        assert!(!stdout.contains(SEEDED) && !stderr.contains(SEEDED));
    }
}

/// `drill run` / the restore runner: the TARGET side, checked first thing in
/// `execute`, before the approval, the signer or the spec is read.
#[test]
fn drill_run_refuses_a_target_binding_that_names_another_connection() {
    for (label, binding, refused) in [
        ("absent", None, true),
        ("foreign", Some(FOREIGN), true),
        ("bound", Some(EXPECTED), false),
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut command = base(root.path());
        command
            .args(["drill", "run", "--spec"])
            .arg(root.path().join("absent-spec.yaml"))
            .arg("--approval")
            .arg(root.path().join("absent-approval.json"))
            .arg("--approver-key")
            .arg(root.path().join("absent-approver.pem"))
            .arg("--allowed-clusters")
            .arg(root.path().join("absent-allowed.json"))
            .arg("--signing-key")
            .arg(root.path().join("absent-signing.pem"))
            .env("LOGWEIR_TARGET_PASSWORD", SEEDED)
            .env("LOGWEIR_TARGET_CREDENTIAL_BINDING_EXPECTED", EXPECTED);
        if let Some(b) = binding {
            command.env("LOGWEIR_TARGET_CREDENTIAL_BINDING", b);
        }
        let out = run(command, label);
        let stdout = String::from_utf8_lossy(&out.stdout).to_string();
        let stderr = String::from_utf8_lossy(&out.stderr).to_string();
        if refused {
            assert_eq!(out.status.code(), Some(3), "{label}: {stdout}\n{stderr}");
            assert_eq!(
                last_line(&stdout),
                "refusal-reason=CredentialBindingMismatch",
                "{label}: {stdout}"
            );
        } else {
            assert!(
                !stdout.contains("CredentialBindingMismatch")
                    && !stderr.contains("CredentialBindingMismatch"),
                "{label}: {stdout}\n{stderr}"
            );
        }
        assert!(
            !stdout.contains(SEEDED) && !stderr.contains(SEEDED),
            "{label}"
        );
    }
}

/// `cluster-probe`: interface I14's two stdout lines stay the contract
/// (`reachable=false`), the named reason travels on stderr as the line the
/// `KafkaCluster` controller reads, and the sentinel is never dialled.
#[test]
fn cluster_probe_refuses_an_unbound_credential_without_dialling() {
    for (label, binding, refused) in [
        ("absent", None, true),
        ("foreign", Some(FOREIGN), true),
        ("bound", Some(EXPECTED), false),
    ] {
        let root = tempfile::tempdir().unwrap();
        let (listener, address) = sentinel();
        let mut command = base(root.path());
        command
            .args(["cluster-probe", "--bootstrap"])
            .arg(&address)
            .args(["--auth-mode", "scramSha512", "--username", "logweir"])
            .env("LOGWEIR_SOURCE_PASSWORD", SEEDED)
            .env("LOGWEIR_SOURCE_CREDENTIAL_BINDING_EXPECTED", EXPECTED);
        if let Some(b) = binding {
            command.env("LOGWEIR_SOURCE_CREDENTIAL_BINDING", b);
        }
        let out = run(command, label);
        let stdout = String::from_utf8_lossy(&out.stdout).to_string();
        let stderr = String::from_utf8_lossy(&out.stderr).to_string();
        if refused {
            assert_eq!(stdout, "cluster-id=\nreachable=false\n", "{label}");
            assert!(
                stderr
                    .lines()
                    .any(|l| l.trim() == "refusal-reason=CredentialBindingMismatch"),
                "{label}: {stderr}"
            );
            assert_no_connection(&listener, label);
        } else {
            assert!(
                !stderr.contains("CredentialBindingMismatch"),
                "{label}: the bound credential dials: {stderr}"
            );
        }
        assert!(
            !stdout.contains(SEEDED) && !stderr.contains(SEEDED),
            "{label}"
        );
    }
}
