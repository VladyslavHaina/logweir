use chrono::Utc;
use logweir::drill::phase1_approval;
use logweir::drill::{
    execution_contract_for_invocation, execution_contract_from, validate_execution_contract,
    ApprovalBundleBytes, DrillError, ExecutionContract,
};
use logweir_core::execution_contract as wire;
use logweir_core::ids::sha256_prefixed;
use logweir_core::spec::ApprovalDoc;
use logweir_evidence::keys::SigningKey;
use logweir_evidence::sign::sign_detached;
use std::collections::BTreeMap;
#[cfg(unix)]
use std::io::Write;
use std::process::Command;

#[cfg(unix)]
fn observe_connections(
    listener: std::net::TcpListener,
) -> (
    std::sync::Arc<std::sync::atomic::AtomicBool>,
    std::sync::Arc<std::sync::atomic::AtomicBool>,
    std::thread::JoinHandle<()>,
) {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    listener.set_nonblocking(true).unwrap();
    let contacted = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let contacted_in_thread = Arc::clone(&contacted);
    let stop_in_thread = Arc::clone(&stop);
    let observer = std::thread::spawn(move || {
        while !stop_in_thread.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((stream, _)) => {
                    contacted_in_thread.store(true, Ordering::SeqCst);
                    drop(stream);
                    return;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("notification sentinel failed: {error}"),
            }
        }
    });
    (contacted, stop, observer)
}

struct Fixture {
    bundle: ApprovalBundleBytes,
    approver: SigningKey,
    signing: SigningKey,
}

fn fixture() -> Fixture {
    fixture_with_plan(b"name: startup-contract\n".to_vec())
}

fn fixture_with_plan(plan: Vec<u8>) -> Fixture {
    let approver = SigningKey::generate_ed25519();
    let signing = SigningKey::generate_ed25519();
    let doc = ApprovalDoc {
        approver: "operator@example.com".to_string(),
        ticket: "CHG-PLAT-01".to_string(),
        plan_hash: sha256_prefixed(&plan),
        approved_at: Utc::now(),
        subject_kind: "Restore".to_string(),
    };
    let approval = serde_json::to_vec(&doc).unwrap();
    let sidecar =
        sign_detached(&approver, phase1_approval::PAYLOAD_TYPE_APPROVAL, &approval).unwrap();
    let approval_sidecar = serde_json::to_vec(&sidecar).unwrap();
    let approver_key = approver
        .verifying_key()
        .to_public_key_pem()
        .unwrap()
        .into_bytes();
    Fixture {
        bundle: ApprovalBundleBytes {
            plan,
            approval,
            approval_sidecar,
            approver_key,
            allowed_clusters: br#"{"allowed_cluster_ids":["cluster-a"]}"#.to_vec(),
        },
        approver,
        signing,
    }
}

fn contract(bundle: &ApprovalBundleBytes) -> ExecutionContract {
    ExecutionContract {
        subject_api_version: "logweir.dev/v1alpha1".to_string(),
        subject_kind: "Restore".to_string(),
        subject_name: "restore-a".to_string(),
        subject_namespace: "tenant-a".to_string(),
        subject_uid: "restore-uid-a".to_string(),
        approval_name: "approval-a".to_string(),
        approval_uid: "approval-uid-a".to_string(),
        plan_sha256: sha256_prefixed(&bundle.plan),
        approval_sha256: sha256_prefixed(&bundle.approval),
        approval_sidecar_sha256: sha256_prefixed(&bundle.approval_sidecar),
        approver_key_sha256: sha256_prefixed(&bundle.approver_key),
        allowed_clusters_sha256: sha256_prefixed(&bundle.allowed_clusters),
    }
}

fn env_map(contract: &ExecutionContract) -> BTreeMap<String, String> {
    [
        (wire::VERSION_ENV, wire::VERSION.to_string()),
        (
            wire::SUBJECT_API_VERSION_ENV,
            contract.subject_api_version.clone(),
        ),
        (wire::SUBJECT_KIND_ENV, contract.subject_kind.clone()),
        (wire::SUBJECT_NAME_ENV, contract.subject_name.clone()),
        (
            wire::SUBJECT_NAMESPACE_ENV,
            contract.subject_namespace.clone(),
        ),
        (wire::SUBJECT_UID_ENV, contract.subject_uid.clone()),
        (wire::APPROVAL_NAME_ENV, contract.approval_name.clone()),
        (wire::APPROVAL_UID_ENV, contract.approval_uid.clone()),
        (wire::PLAN_SHA256_ENV, contract.plan_sha256.clone()),
        (wire::APPROVAL_SHA256_ENV, contract.approval_sha256.clone()),
        (
            wire::APPROVAL_SIDECAR_SHA256_ENV,
            contract.approval_sidecar_sha256.clone(),
        ),
        (
            wire::APPROVER_KEY_SHA256_ENV,
            contract.approver_key_sha256.clone(),
        ),
        (
            wire::ALLOWED_CLUSTERS_SHA256_ENV,
            contract.allowed_clusters_sha256.clone(),
        ),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_string(), value))
    .collect()
}

#[test]
fn exact_contract_content_hashes_and_approval_verify_independently() {
    let fixture = fixture();
    let contract = contract(&fixture.bundle);
    validate_execution_contract(&contract, Some("approval/approval-a"), &fixture.bundle).unwrap();
    let approved = phase1_approval::verify_bytes(
        std::str::from_utf8(&fixture.bundle.plan).unwrap(),
        &fixture.bundle.approval,
        &fixture.bundle.approval_sidecar,
        &fixture.bundle.approver_key,
        &fixture.signing.verifying_key(),
    )
    .unwrap();
    assert_eq!(approved.approval.plan_hash, contract.plan_sha256);
    assert_eq!(approved.approval.key_id, fixture.approver.key_id());
}

#[test]
fn every_projected_member_is_hash_bound_including_the_allowlist() {
    let fixture = fixture();
    let contract = contract(&fixture.bundle);
    for member in 0..5 {
        let mut changed = fixture.bundle.clone();
        match member {
            0 => changed.plan.push(b'!'),
            1 => changed.approval.push(b'!'),
            2 => changed.approval_sidecar.push(b'!'),
            3 => changed.approver_key.push(b'!'),
            4 => changed.allowed_clusters.push(b'!'),
            _ => unreachable!(),
        }
        let error = validate_execution_contract(&contract, Some("approval/approval-a"), &changed)
            .unwrap_err();
        assert!(
            matches!(error, DrillError::Guard(_)),
            "member {member}: {error}"
        );
        assert!(error.to_string().contains("no data operation was started"));
        assert!(!error.to_string().contains("PRIVATE KEY"));
    }
}

#[test]
fn contract_transport_is_all_or_nothing_while_standalone_is_compatible() {
    assert!(execution_contract_from(|_| None).unwrap().is_none());

    let error = execution_contract_from(|name| {
        (name == wire::VERSION_ENV).then(|| wire::VERSION.to_string())
    })
    .unwrap_err();
    assert!(matches!(error, DrillError::Guard(_)));
    assert!(error.to_string().contains(wire::SUBJECT_API_VERSION_ENV));

    let fixture = fixture();
    let map = env_map(&contract(&fixture.bundle));
    let parsed = execution_contract_from(|name| map.get(name).cloned())
        .unwrap()
        .unwrap();
    assert_eq!(parsed, contract(&fixture.bundle));
}

#[test]
fn argv_and_environment_handshake_is_exact_while_legacy_omission_remains_supported() {
    assert!(
        execution_contract_for_invocation(None, |_| None)
            .unwrap()
            .is_none(),
        "both channels absent is the intentional legacy Job/standalone shape"
    );

    let fixture = fixture();
    let complete = env_map(&contract(&fixture.bundle));
    let missing_argv =
        execution_contract_for_invocation(None, |name| complete.get(name).cloned()).unwrap_err();
    assert!(missing_argv.to_string().contains(wire::VERSION_ARG));

    let missing_environment =
        execution_contract_for_invocation(Some(wire::VERSION), |_| None).unwrap_err();
    assert!(missing_environment
        .to_string()
        .contains("without the required contract environment"));

    let unsupported =
        execution_contract_for_invocation(Some("2"), |name| complete.get(name).cloned())
            .unwrap_err();
    assert!(unsupported.to_string().contains("argv version \"2\""));

    let mut mismatched = complete.clone();
    mismatched.insert(wire::VERSION_ENV.to_string(), "2".to_string());
    let mismatch = execution_contract_for_invocation(Some(wire::VERSION), |name| {
        mismatched.get(name).cloned()
    })
    .unwrap_err();
    assert!(mismatch.to_string().contains("version mismatch"));

    let mut partial = complete.clone();
    partial.remove(wire::PLAN_SHA256_ENV);
    let partial_error =
        execution_contract_for_invocation(Some(wire::VERSION), |name| partial.get(name).cloned())
            .unwrap_err();
    assert!(partial_error.to_string().contains(wire::PLAN_SHA256_ENV));

    let parsed =
        execution_contract_for_invocation(Some(wire::VERSION), |name| complete.get(name).cloned())
            .unwrap()
            .unwrap();
    assert_eq!(parsed, contract(&fixture.bundle));
}

/// Cross-version acceptance is run explicitly against a binary built from the
/// pre-handshake revision. Unlike invoking `CARGO_BIN_EXE_logweir`, this proves
/// that an actual older clap surface rejects a new controller's argv before
/// dispatch can read projected inputs.
#[test]
#[ignore = "set LOGWEIR_OLD_RUNNER_BIN to a pre-handshake logweir binary"]
fn pre_handshake_runner_binary_rejects_the_new_job_argv() {
    let old_runner = std::env::var_os("LOGWEIR_OLD_RUNNER_BIN")
        .expect("LOGWEIR_OLD_RUNNER_BIN must name the pre-handshake binary");
    let output = Command::new(old_runner)
        .args([
            "restore",
            "run",
            wire::VERSION_ARG,
            wire::VERSION,
            "--spec",
            "/must-not-be-read/restore.yaml",
            "--approval",
            "/must-not-be-read/approval.json",
            "--approver-key",
            "/must-not-be-read/approver.pem",
            "--allowed-clusters",
            "/must-not-be-read/allowed.json",
            "--signing-key",
            "/must-not-be-read/signing.pem",
        ])
        .output()
        .unwrap();
    let transcript = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.status.code(), Some(1), "{transcript}");
    assert!(
        transcript.contains("unexpected argument '--execution-contract-version'"),
        "the old runner must refuse at clap parsing, before dispatch: {transcript}"
    );
    assert!(!transcript.contains("/must-not-be-read"), "{transcript}");
    assert!(
        !transcript.contains("BrokerTransportFailure"),
        "{transcript}"
    );
}

#[test]
fn substituted_allowlist_is_refused_before_a_bootstrap_socket_is_touched() {
    let fixture = fixture();
    let contract = contract(&fixture.bundle);
    let dir = tempfile::tempdir().unwrap();
    let plan = dir.path().join("restore.yaml");
    let approval = dir.path().join("approval.json");
    let approver_key = dir.path().join("approver.pub.pem");
    let allowed = dir.path().join("allowed-clusters.json");
    let signing = dir.path().join("signing.pem");
    std::fs::write(&plan, &fixture.bundle.plan).unwrap();
    std::fs::write(&approval, &fixture.bundle.approval).unwrap();
    std::fs::write(
        approval.with_extension("sig"),
        &fixture.bundle.approval_sidecar,
    )
    .unwrap();
    std::fs::write(&approver_key, &fixture.bundle.approver_key).unwrap();
    std::fs::write(&allowed, br#"{"allowed_cluster_ids":["substituted"]}"#).unwrap();
    std::fs::write(&signing, fixture.signing.to_pkcs8_pem().unwrap()).unwrap();

    let mut command = Command::new(env!("CARGO_BIN_EXE_logweir"));
    command
        .args(["restore", "run", wire::VERSION_ARG, wire::VERSION, "--spec"])
        .arg(&plan)
        .arg("--approval")
        .arg(&approval)
        .arg("--approver-key")
        .arg(&approver_key)
        .arg("--approver-key-ids")
        .arg(fixture.approver.key_id())
        .arg("--allowed-clusters")
        .arg(&allowed)
        .arg("--signing-key")
        .arg(&signing)
        .args(["--triggered-by", "approval/approval-a"]);
    for (name, value) in env_map(&contract) {
        command.env(name, value);
    }
    let output = command.output().unwrap();
    let transcript = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.status.code(), Some(3), "{transcript}");
    assert!(transcript.contains("mounted allowed-clusters bytes hash"));
    assert!(
        !transcript.contains("BrokerTransportFailure"),
        "{transcript}"
    );
    assert!(!transcript.contains("Connection refused"), "{transcript}");
}

#[test]
fn a_lost_bundle_member_fails_before_context_or_kafka_is_constructed() {
    let fixture = fixture();
    let contract = contract(&fixture.bundle);
    let dir = tempfile::tempdir().unwrap();
    let plan = dir.path().join("restore.yaml");
    let approval = dir.path().join("approval.json");
    let approver_key = dir.path().join("approver.pub.pem");
    let allowed = dir.path().join("allowed-clusters.json");
    let signing = dir.path().join("signing.pem");
    std::fs::write(&plan, &fixture.bundle.plan).unwrap();
    std::fs::write(&approval, &fixture.bundle.approval).unwrap();
    // Deliberately do not create approval.sig: this models a projection lost
    // after the Job was created but before process startup.
    std::fs::write(&approver_key, &fixture.bundle.approver_key).unwrap();
    std::fs::write(&allowed, &fixture.bundle.allowed_clusters).unwrap();
    std::fs::write(&signing, fixture.signing.to_pkcs8_pem().unwrap()).unwrap();

    let mut command = Command::new(env!("CARGO_BIN_EXE_logweir"));
    command
        .args(["restore", "run", wire::VERSION_ARG, wire::VERSION, "--spec"])
        .arg(&plan)
        .arg("--approval")
        .arg(&approval)
        .arg("--approver-key")
        .arg(&approver_key)
        .arg("--approver-key-ids")
        .arg(fixture.approver.key_id())
        .arg("--allowed-clusters")
        .arg(&allowed)
        .arg("--signing-key")
        .arg(&signing)
        .args(["--triggered-by", "approval/approval-a"]);
    for (name, value) in env_map(&contract) {
        command.env(name, value);
    }
    let output = command.output().unwrap();
    let transcript = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.status.code(), Some(1), "{transcript}");
    assert!(transcript.contains("approval sidecar"), "{transcript}");
    assert!(
        !transcript.contains("BrokerTransportFailure"),
        "{transcript}"
    );
    assert!(!transcript.contains("Connection refused"), "{transcript}");
}

#[test]
fn startup_plan_digest_tamper_cannot_trigger_its_notification_destination() {
    use std::sync::atomic::Ordering;

    let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
    let endpoint = format!("https://{}/events", listener.local_addr().unwrap());
    let (contacted, stop, observer) = observe_connections(listener);

    let trusted_plan = include_bytes!("../../../examples/drill.yaml").to_vec();
    let fixture = fixture_with_plan(trusted_plan);
    let contract = contract(&fixture.bundle);
    let mut tampered: logweir_core::spec::DrillSpec =
        serde_yaml::from_slice(&fixture.bundle.plan).unwrap();
    tampered.notifications.pagerduty_routing_key = Some("UNAUTHENTICATED-ROUTE".into());
    tampered.notifications.pagerduty_endpoint = Some(endpoint);
    let tampered_plan = serde_yaml::to_string(&tampered).unwrap();
    assert_ne!(tampered_plan.as_bytes(), fixture.bundle.plan.as_slice());

    let dir = tempfile::tempdir().unwrap();
    let plan = dir.path().join("restore.yaml");
    let approval = dir.path().join("approval.json");
    let approver_key = dir.path().join("approver.pub.pem");
    let allowed = dir.path().join("allowed-clusters.json");
    let signing = dir.path().join("signing.pem");
    std::fs::write(&plan, tampered_plan).unwrap();
    std::fs::write(&approval, &fixture.bundle.approval).unwrap();
    std::fs::write(
        approval.with_extension("sig"),
        &fixture.bundle.approval_sidecar,
    )
    .unwrap();
    std::fs::write(&approver_key, &fixture.bundle.approver_key).unwrap();
    std::fs::write(&allowed, &fixture.bundle.allowed_clusters).unwrap();
    std::fs::write(&signing, fixture.signing.to_pkcs8_pem().unwrap()).unwrap();

    let mut command = Command::new(env!("CARGO_BIN_EXE_logweir"));
    command
        .args(["restore", "run", wire::VERSION_ARG, wire::VERSION, "--spec"])
        .arg(&plan)
        .arg("--approval")
        .arg(&approval)
        .arg("--approver-key")
        .arg(&approver_key)
        .arg("--allowed-clusters")
        .arg(&allowed)
        .arg("--signing-key")
        .arg(&signing)
        .args(["--triggered-by", "approval/approval-a"]);
    for (name, value) in env_map(&contract) {
        command.env(name, value);
    }
    let output = command.output().unwrap();
    stop.store(true, Ordering::SeqCst);
    observer.join().unwrap();
    let transcript = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.status.code(), Some(3), "{transcript}");
    assert!(
        transcript.contains("mounted plan bytes hash"),
        "{transcript}"
    );
    assert!(
        !contacted.load(Ordering::SeqCst),
        "the digest-rejected plan supplied an unauthenticated network destination"
    );
}

#[cfg(unix)]
#[test]
fn late_projected_plan_replacement_cannot_redirect_the_authenticated_notification() {
    use logweir_core::spec::AuthSpec;
    use std::sync::atomic::Ordering;

    let trusted_listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
    let trusted_endpoint = format!("https://{}/events", trusted_listener.local_addr().unwrap());
    let (trusted_contacted, trusted_stop, trusted_observer) = observe_connections(trusted_listener);
    let substituted_listener =
        std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
    let substituted_endpoint = format!(
        "https://{}/events",
        substituted_listener.local_addr().unwrap()
    );
    let (substituted_contacted, substituted_stop, substituted_observer) =
        observe_connections(substituted_listener);

    let mut trusted: logweir_core::spec::DrillSpec =
        serde_yaml::from_slice(include_bytes!("../../../examples/drill.yaml")).unwrap();
    trusted.target.auth = AuthSpec::ScramSha512 {
        username: "startup-test".into(),
        tls: false,
    };
    trusted.notifications.pagerduty_routing_key = Some("AUTHENTICATED-ROUTE".into());
    trusted.notifications.pagerduty_endpoint = Some(trusted_endpoint);
    let trusted_plan = serde_yaml::to_string(&trusted).unwrap().into_bytes();
    let fixture = fixture_with_plan(trusted_plan.clone());
    let contract = contract(&fixture.bundle);

    let mut substituted = trusted;
    substituted.notifications.pagerduty_endpoint = Some(substituted_endpoint);
    let substituted_plan = serde_yaml::to_string(&substituted).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let plan = dir.path().join("restore.yaml");
    let approval = dir.path().join("approval.pipe");
    let approver_key = dir.path().join("approver.pub.pem");
    let allowed = dir.path().join("allowed-clusters.json");
    let signing = dir.path().join("signing.pem");
    std::fs::write(&plan, &trusted_plan).unwrap();
    let mkfifo = Command::new("mkfifo").arg(&approval).status().unwrap();
    assert!(mkfifo.success(), "mkfifo failed: {mkfifo}");
    std::fs::write(
        approval.with_extension("sig"),
        &fixture.bundle.approval_sidecar,
    )
    .unwrap();
    std::fs::write(&approver_key, &fixture.bundle.approver_key).unwrap();
    std::fs::write(&allowed, &fixture.bundle.allowed_clusters).unwrap();
    std::fs::write(&signing, fixture.signing.to_pkcs8_pem().unwrap()).unwrap();

    let mut command = Command::new(env!("CARGO_BIN_EXE_logweir"));
    command
        .args(["restore", "run", wire::VERSION_ARG, wire::VERSION, "--spec"])
        .arg(&plan)
        .arg("--approval")
        .arg(&approval)
        .arg("--approver-key")
        .arg(&approver_key)
        .arg("--allowed-clusters")
        .arg(&allowed)
        .arg("--signing-key")
        .arg(&signing)
        .args(["--triggered-by", "approval/approval-a"])
        .env_remove("LOGWEIR_SOURCE_PASSWORD")
        .env_remove("LOGWEIR_TARGET_PASSWORD")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    for (name, value) in env_map(&contract) {
        command.env(name, value);
    }
    let child = command.spawn().unwrap();

    // Opening the FIFO for writing returns only after the runner has opened it
    // for reading. The runner reads the plan first, so it is now blocked after
    // capturing trusted bytes and before approval verification completes.
    let mut approval_writer = std::fs::OpenOptions::new()
        .write(true)
        .open(&approval)
        .unwrap();
    std::fs::write(&plan, substituted_plan).unwrap();
    approval_writer.write_all(&fixture.bundle.approval).unwrap();
    drop(approval_writer);

    let output = child.wait_with_output().unwrap();
    trusted_stop.store(true, Ordering::SeqCst);
    substituted_stop.store(true, Ordering::SeqCst);
    trusted_observer.join().unwrap();
    substituted_observer.join().unwrap();
    let transcript = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.status.code(), Some(1), "{transcript}");
    assert!(
        transcript.contains("LOGWEIR_TARGET_PASSWORD is unset"),
        "the run must fail after authenticated startup and before Kafka construction: {transcript}"
    );
    assert!(
        trusted_contacted.load(Ordering::SeqCst),
        "the authenticated notification destination was not used"
    );
    assert!(
        !substituted_contacted.load(Ordering::SeqCst),
        "reporting followed the late projected-plan substitution"
    );
}
