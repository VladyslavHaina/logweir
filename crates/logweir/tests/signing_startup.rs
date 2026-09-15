//! Binary-level PLAT-02.2 acceptance: invalid signing material is rejected by
//! the production wrappers before they construct clients or create execution
//! outputs. Redacted terminal diagnostics, including metrics, remain allowed.

#![cfg(unix)]

use logweir_core::engine::StorageUrl;
use logweir_core::spec::{
    AllowedClusters, ApprovalDoc, AuthSpec, BackupSettings, BackupSourceSpec,
};
use logweir_evidence::keys::SigningKey;
use std::io::ErrorKind;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

const PROCESS_LIMIT: Duration = Duration::from_secs(5);
const PROMPT_RETURN_LIMIT: Duration = Duration::from_secs(2);
const SECRET_SENTINEL: &str = "DO-NOT-ECHO-PRIVATE-KEY-MATERIAL";

#[derive(Clone, Copy)]
enum KeyCase {
    Missing,
    Malformed,
    Unreadable,
}

impl KeyCase {
    fn name(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Malformed => "malformed",
            Self::Unreadable => "unreadable",
        }
    }
}

fn invalid_key(root: &Path, case: KeyCase) -> PathBuf {
    let path = root.join(format!("{}-signer.pem", case.name()));
    match case {
        KeyCase::Missing => {}
        KeyCase::Malformed => std::fs::write(
            &path,
            format!("-----BEGIN PRIVATE KEY-----\n{SECRET_SENTINEL}\n-----END PRIVATE KEY-----\n"),
        )
        .unwrap(),
        // A directory at the file path is unreadable as a PKCS#8 file even
        // when the suite runs with elevated filesystem privileges.
        KeyCase::Unreadable => std::fs::create_dir(&path).unwrap(),
    }
    path
}

fn listening_sentinel() -> (std::net::TcpListener, String) {
    let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .expect("bind a loopback sentinel");
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap().to_string();
    (listener, address)
}

fn assert_no_connection(listener: &std::net::TcpListener, label: &str) {
    match listener.accept() {
        Err(error) if error.kind() == ErrorKind::WouldBlock => {}
        Ok((_, peer)) => panic!(
            "{label}: invalid signing material must stop before Kafka client construction and \
             must not use plan-controlled notification destinations; the loopback sentinel \
             accepted a connection from {peer}"
        ),
        Err(error) => panic!("{label}: inspect the loopback sentinel: {error}"),
    }
}

fn trap_engine(root: &Path) -> (PathBuf, PathBuf) {
    let binary = root.join("engine-trap.sh");
    let marker = root.join("engine-was-invoked");
    std::fs::write(
        &binary,
        "#!/bin/sh\nprintf invoked > \"$LOGWEIR_ENGINE_SENTINEL\"\nexit 97\n",
    )
    .unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
    (binary, marker)
}

fn wait_bounded(mut command: Command, label: &str) -> (Output, Duration, u32) {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let started = Instant::now();
    let mut child = command.spawn().expect("spawn the compiled logweir binary");
    let pid = child.id();
    loop {
        if child.try_wait().expect("poll the logweir child").is_some() {
            let elapsed = started.elapsed();
            return (
                child
                    .wait_with_output()
                    .expect("collect the logweir output"),
                elapsed,
                pid,
            );
        }
        if started.elapsed() >= PROCESS_LIMIT {
            let _ = child.kill();
            let output = child.wait_with_output().expect("reap the timed-out child");
            panic!(
                "{label}: invalid-key startup exceeded {PROCESS_LIMIT:?}; the child was killed. \
                 stdout: {}\nstderr: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn controlled_command(root: &Path, engine: &Path, marker: &Path) -> Command {
    let private_tmp = root.join("tmp");
    std::fs::create_dir(&private_tmp).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_logweir"));
    command
        .env_clear()
        .env("TMPDIR", &private_tmp)
        .env("RUST_LOG", "info")
        .env("LOGWEIR_ENGINE_BIN", engine)
        .env("LOGWEIR_ENGINE_SENTINEL", marker)
        .env("LOGWEIR_ENGINE_VERSION", "sentinel-version")
        .env("LOGWEIR_ENGINE_DIGEST", "sha256:sentinel-digest");
    command
}

fn assert_empty_dir(path: &Path, label: &str) {
    assert!(
        std::fs::read_dir(path).unwrap().next().is_none(),
        "{label}: invalid signing material must not write to storage at {}",
        path.display()
    );
}

fn assert_diagnostic_metrics_are_published_safely(path: &Path, label: &str) {
    assert!(
        path.is_file(),
        "{label}: signer prerequisite failure must publish local diagnostic metrics at {}",
        path.display()
    );
    let metrics = std::fs::read_to_string(path).unwrap_or_else(|error| {
        panic!("{label}: diagnostic metrics are not a readable file: {error}")
    });
    assert!(
        metrics.contains("logweir_drill_exit_code{cluster=\"unknown\"} 4"),
        "{label}: prerequisite metrics must report exit 4: {metrics}"
    );
    assert!(
        !metrics.contains("logweir_drill_runs_total"),
        "{label}: prerequisite metrics must not claim a completed drill: {metrics}"
    );
    assert!(
        !metrics.contains(SECRET_SENTINEL),
        "{label}: malformed private-key contents leaked into metrics: {metrics}"
    );
}

fn assert_startup_refusal(output: &Output, elapsed: Duration, key: &Path, label: &str) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let transcript = format!("{stdout}{stderr}");
    assert_eq!(output.status.code(), Some(4), "{label}: {transcript}");
    assert!(
        transcript.contains(&key.display().to_string()),
        "{label}: the diagnostic must name the failed mount path: {transcript}"
    );
    assert!(
        transcript.contains("Mount a readable P-256 or Ed25519 PKCS#8 PEM private key"),
        "{label}: the diagnostic must give an actionable remedy: {transcript}"
    );
    assert!(
        transcript.contains("No engine data operation was started"),
        "{label}: the diagnostic must state the side-effect boundary: {transcript}"
    );
    assert!(
        !transcript.contains(SECRET_SENTINEL),
        "{label}: malformed private-key contents leaked: {transcript}"
    );
    assert!(
        elapsed < PROMPT_RETURN_LIMIT,
        "{label}: invalid-key startup took {elapsed:?}; it should not approach the \
         {PROCESS_LIMIT:?} no-hang kill bound"
    );
}

#[test]
fn backup_binary_rejects_every_invalid_key_before_production_side_effects() {
    for case in [KeyCase::Missing, KeyCase::Malformed, KeyCase::Unreadable] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let archive = root.join("archive");
        std::fs::create_dir(&archive).unwrap();
        let (listener, bootstrap) = listening_sentinel();
        let spec = logweir_core::spec::BackupSpec {
            source: BackupSourceSpec {
                bootstrap_servers: vec![bootstrap],
                auth: AuthSpec::Plaintext,
                topics: vec!["orders".into()],
            },
            storage: StorageUrl::Filesystem {
                path: archive.clone(),
            },
            backup_id: "sentinel-backup".into(),
            backup: BackupSettings::default(),
        };
        let spec_path = root.join("backup.yaml");
        std::fs::write(&spec_path, serde_yaml::to_string(&spec).unwrap()).unwrap();
        let allowed = root.join("allowed.json");
        std::fs::write(
            &allowed,
            serde_json::to_vec(&AllowedClusters {
                allowed_cluster_ids: vec![],
                source_cluster_id: None,
            })
            .unwrap(),
        )
        .unwrap();
        let key = invalid_key(root, case);
        let receipt = root.join("receipt.json");
        let (engine, engine_marker) = trap_engine(root);
        let label = format!("backup/{}", case.name());
        let mut command = controlled_command(root, &engine, &engine_marker);
        command
            .args(["backup", "run", "--spec"])
            .arg(&spec_path)
            .arg("--allowed-clusters")
            .arg(&allowed)
            .arg("--signing-key")
            .arg(&key)
            .arg("--receipt-out")
            .arg(&receipt);

        let (output, elapsed, pid) = wait_bounded(command, &label);
        assert_startup_refusal(&output, elapsed, &key, &label);
        assert_no_connection(&listener, &label);
        assert!(!engine_marker.exists(), "{label}: engine trap was invoked");
        assert!(
            !root.join("tmp").join(format!("logweir-{pid}")).exists(),
            "{label}: the engine work directory was created"
        );
        assert!(!receipt.exists(), "{label}: receipt output was created");
        assert!(
            !receipt.with_extension("sig").exists(),
            "{label}: receipt sidecar output was created"
        );
        assert_empty_dir(&archive, &label);
    }
}

fn restore_inputs(
    root: &Path,
    bootstrap: String,
    archive: &Path,
    evidence: &Path,
) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
    let mut spec: logweir_core::spec::RestoreSpec =
        serde_yaml::from_str(include_str!("../../../examples/drill.yaml")).unwrap();
    spec.target.bootstrap_servers = vec![bootstrap.clone()];
    spec.notifications.pagerduty_routing_key = Some("UNTRUSTED-BEFORE-SIGNER-STARTUP".into());
    spec.notifications.pagerduty_endpoint = Some(format!("https://{bootstrap}/events"));
    spec.source.storage = StorageUrl::Filesystem {
        path: archive.to_path_buf(),
    };
    spec.evidence = StorageUrl::Filesystem {
        path: evidence.to_path_buf(),
    };
    let spec_text = serde_yaml::to_string(&spec).unwrap();
    let spec_path = root.join("restore.yaml");
    std::fs::write(&spec_path, &spec_text).unwrap();

    let approver = SigningKey::generate_p256();
    let approver_key = root.join("approver.pub.pem");
    std::fs::write(
        &approver_key,
        approver.verifying_key().to_public_key_pem().unwrap(),
    )
    .unwrap();
    let approval_doc = ApprovalDoc {
        approver: "startup-test@example.com".into(),
        ticket: "PLAT-02.2".into(),
        plan_hash: logweir_core::ids::sha256_prefixed(spec_text.as_bytes()),
        approved_at: chrono::Utc::now(),
        subject_kind: logweir_core::spec::SUBJECT_KIND_RESTORE.into(),
    };
    let mut approval_bytes = serde_json::to_vec_pretty(&approval_doc).unwrap();
    approval_bytes.push(b'\n');
    let approval = root.join("approval.json");
    std::fs::write(&approval, &approval_bytes).unwrap();
    let sidecar = logweir_evidence::sign::sign_detached(
        &approver,
        logweir::drill::phase1_approval::PAYLOAD_TYPE_APPROVAL,
        &approval_bytes,
    )
    .unwrap();
    std::fs::write(
        approval.with_extension("sig"),
        serde_json::to_vec(&sidecar).unwrap(),
    )
    .unwrap();

    let allowed = root.join("allowed.json");
    std::fs::write(
        &allowed,
        serde_json::to_vec(&AllowedClusters {
            allowed_cluster_ids: vec!["SENTINEL-TARGET".into()],
            source_cluster_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    (spec_path, approval, approver_key, allowed)
}

#[test]
fn restore_and_drill_binaries_reject_every_invalid_key_before_production_side_effects() {
    for verb in ["restore", "drill"] {
        for case in [KeyCase::Missing, KeyCase::Malformed, KeyCase::Unreadable] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let archive = root.join("archive");
            let evidence = root.join("evidence");
            std::fs::create_dir(&archive).unwrap();
            std::fs::create_dir(&evidence).unwrap();
            let (listener, bootstrap) = listening_sentinel();
            let (spec, approval, approver_key, allowed) =
                restore_inputs(root, bootstrap, &archive, &evidence);
            let key = invalid_key(root, case);
            let scorecard = root.join("scorecard.json");
            let metrics = root.join("metrics.prom");
            let offsets = root.join("offsets.json");
            let (engine, engine_marker) = trap_engine(root);
            let label = format!("{verb}/{}", case.name());
            let mut command = controlled_command(root, &engine, &engine_marker);
            command
                .args([verb, "run", "--spec"])
                .arg(&spec)
                .arg("--approval")
                .arg(&approval)
                .arg("--approver-key")
                .arg(&approver_key)
                .arg("--allowed-clusters")
                .arg(&allowed)
                .arg("--signing-key")
                .arg(&key)
                .arg("--out")
                .arg(&scorecard)
                .arg("--metrics-file")
                .arg(&metrics)
                .arg("--offset-report-out")
                .arg(&offsets);

            let (output, elapsed, pid) = wait_bounded(command, &label);
            assert_startup_refusal(&output, elapsed, &key, &label);
            assert_no_connection(&listener, &label);
            assert!(!engine_marker.exists(), "{label}: engine trap was invoked");
            assert!(
                !root.join("tmp").join(format!("logweir-{pid}")).exists(),
                "{label}: the engine work directory was created"
            );
            for output_path in [&scorecard, &scorecard.with_extension("sig"), &offsets] {
                assert!(
                    !output_path.exists(),
                    "{label}: execution output {} was created",
                    output_path.display()
                );
            }
            // A minimal exit-4 metrics file is safe local observability, not
            // Kafka/storage/engine data work, and is mandatory on this path.
            assert_diagnostic_metrics_are_published_safely(&metrics, &label);
            assert_empty_dir(&archive, &label);
            assert_empty_dir(&evidence, &label);
        }
    }
}
