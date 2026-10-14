//! `logweir doctor`: eight checks, one test each (check 8, `target engine`,
//! FX-45, is `doctor::tests::check_8_names_a_target_the_engine_cannot_restore_into`
//! over a stub reader, and live against Redpanda in
//! `e2e/tests/compat_contract.rs`). Each test names the check it
//! exercises and asserts the
//! FAILURE TEXT that check must produce, not just a bare exit code: a check
//! that regresses to a generic "doctor failed" would still exit 1 and pass a
//! test that only checked the exit code.
use std::process::Command;

fn doctor(args: &[(&str, &str)], env: &[(&str, &str)]) -> std::process::Output {
    let mut c = Command::new(env!("CARGO_BIN_EXE_logweir"));
    c.arg("doctor");
    for (k, v) in args {
        c.arg(k).arg(v);
    }
    for (k, v) in env {
        c.env(k, v);
    }
    c.output().unwrap()
}

const OK_ARGS: [(&str, &str); 3] = [
    ("--spec", "../../examples/drill.yaml"),
    ("--allowed-clusters", "../../examples/allowed-clusters.json"),
    ("--approver-key", "../../e2e/fixtures/signed/public.pem"),
];

/// A stub engine so tests that are not testing check 1 or check 2 reach the
/// check they are actually testing, rather than short-circuiting at "no
/// engine at ..." on a runner with no `kafka-backup` on $PATH (addendum A5).
const ENGINE_OK: [(&str, &str); 1] =
    [("LOGWEIR_ENGINE_BIN", "../../e2e/fixtures/fake-engine-ok.sh")];

fn assert_fails_with(out: std::process::Output, needle: &str) {
    let s =
        String::from_utf8_lossy(&out.stdout).to_string() + &String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(1), "{s}");
    assert!(s.contains(needle), "expected `{needle}` in:\n{s}");
}

#[test]
fn check_1_a_missing_engine_names_the_required_digest_and_the_glibc_floor() {
    let out = doctor(
        &OK_ARGS,
        &[("LOGWEIR_ENGINE_BIN", "/nonexistent/kafka-backup")],
    );
    let s =
        String::from_utf8_lossy(&out.stdout).to_string() + &String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(1));
    assert!(
        s.contains("sha256:"),
        "must name the required digest, got: {s}"
    );
    assert!(s.contains("glibc"), "must state the glibc floor, got: {s}");
}

#[test]
fn check_2_an_engine_of_the_wrong_version_is_named_as_a_mismatch() {
    // e2e/fixtures/fake-engine-version.sh prints `kafka-backup 0.19.1`.
    let out = doctor(
        &OK_ARGS,
        &[(
            "LOGWEIR_ENGINE_BIN",
            "../../e2e/fixtures/fake-engine-version.sh",
        )],
    );
    assert_fails_with(
        out,
        &format!("version mismatch: expected {}", logweir::doctor::ENGINE_PIN),
    );
}

#[test]
fn check_3_an_unparsable_drill_spec_is_named() {
    let mut a = OK_ARGS;
    a[0] = ("--spec", "../../Cargo.toml");
    assert_fails_with(doctor(&a, &ENGINE_OK), "drill spec");
}

#[test]
fn check_4_an_unparsable_allowed_clusters_file_is_named() {
    let mut a = OK_ARGS;
    a[1] = ("--allowed-clusters", "../../Cargo.toml");
    assert_fails_with(doctor(&a, &ENGINE_OK), "allowed-clusters");
}

#[test]
fn check_5_an_unloadable_approver_key_is_named() {
    let mut a = OK_ARGS;
    a[2] = ("--approver-key", "../../Cargo.toml");
    assert_fails_with(doctor(&a, &ENGINE_OK), "approver key");
}

#[test]
fn check_6_an_unusable_storage_configuration_is_named_as_storage() {
    let mut a = OK_ARGS;
    a[0] = ("--spec", "../../e2e/fixtures/drill-bad-storage.yaml");
    assert_fails_with(doctor(&a, &ENGINE_OK), "storage");
}

/// Checks 6 and 7 need a broker, so the no-broker case must still be a NAMED
/// failure rather than a generic one: an adopter with a firewall problem must
/// not read "doctor failed" and nothing else.
///
/// **Behind `--features e2e`**: this form takes about 26 s (check 6 retries the
/// EC2 metadata service ten times with no credentials, and check 7 waits out
/// `rdkafka_reader.rs`'s 20 s `const T`; `127.0.0.1:1` fails no faster). The
/// default suite proves the property at
/// `doctor::tests::evaluate_target_fails_and_names_the_target_when_the_cluster_is_unreachable`
/// against the stub reader; this form proves it through the compiled binary.
///
/// The spec gives check 6 a local archive holding exactly one backup set, so
/// check 6 passes with or without a stack and check 7 is always the check
/// under test (an empty reachable archive would fail check 6 first, and `run`
/// stops at the first failure).
#[cfg(feature = "e2e")]
#[test]
fn check_7_an_unreachable_target_is_named_as_a_target_problem() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("archive");
    std::fs::create_dir_all(archive.join("b-0001")).unwrap();
    // `list_manifest_keys` selects on the key ENDING `/manifest.json` and
    // never opens it, so an empty object is a complete backup set for
    // check 6's purposes.
    std::fs::write(archive.join("b-0001/manifest.json"), "{}").unwrap();

    let spec = dir.path().join("drill-unreachable-target.yaml");
    std::fs::write(
        &spec,
        format!(
            "source:\n  storage:\n    backend: filesystem\n    path: {archive}\n  \
             backup: latestCompleted\n  topics: [orders, payments]\n\
             target:\n  bootstrap_servers: [127.0.0.1:1]\n  marker_topic: logweir.scratch\n  \
             topic_mapping_prefix: \"drill-\"\n  default_replication_factor: 1\n  \
             teardown: delete\n\
             sample:\n  window_start: \"2026-08-29T00:00:00Z\"\n  \
             window_end: \"2026-08-30T02:00:00Z\"\n  records_per_partition: 25\n  \
             anchor: random\n\
             objectives:\n  rto_seconds: 900\n  rpo_seconds: 300\n  pass_rate: 1.0\n\
             evidence:\n  backend: filesystem\n  path: {evidence}\n\
             notifications:\n  webhooks: []\n",
            archive = archive.display(),
            evidence = dir.path().join("evidence").display(),
        ),
    )
    .unwrap();

    let mut a = OK_ARGS;
    a[0] = ("--spec", spec.to_str().unwrap());
    assert_fails_with(doctor(&a, &ENGINE_OK), "target");
}

/// A reachable archive holding zero backup sets must be a
/// NAMED failure end-to-end through the CLI, not just at the unit-test
/// level (`doctor::tests::check_storage_fails_on_a_reachable_but_empty_archive`
/// in `src/doctor.rs`) — mirrors `check_6_...`'s own shape for the
/// unconstructable-config case.
#[test]
fn check_6b_a_reachable_but_empty_archive_is_named_as_a_storage_problem() {
    let mut a = OK_ARGS;
    a[0] = ("--spec", "../../e2e/fixtures/drill-empty-archive.yaml");
    assert_fails_with(doctor(&a, &ENGINE_OK), "zero backup sets");
}

/// **`doctor` reports the credential binding
/// `drill run` will refuse**, by its named reason, and dials nothing — the
/// target here is a loopback listener that records any connection. CONTROL:
/// the same run with the binding equal to the expectation gets past the
/// binding (it then fails on the stub target, by a different message).
#[test]
fn check_7_a_target_credential_bound_to_another_connection_is_named() {
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();

    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("archive");
    std::fs::create_dir_all(archive.join("b-0001")).unwrap();
    std::fs::write(archive.join("b-0001/manifest.json"), "{}").unwrap();
    let spec = dir.path().join("drill-bound-target.yaml");
    std::fs::write(
        &spec,
        format!(
            "source:\n  storage:\n    backend: filesystem\n    path: {archive}\n  \
             backup: latestCompleted\n  topics: [orders]\n\
             target:\n  bootstrap_servers: [127.0.0.1:{port}]\n  marker_topic: logweir.scratch\n  \
             topic_mapping_prefix: \"drill-\"\n  default_replication_factor: 1\n  \
             teardown: delete\n  auth:\n    mode: scramSha256\n    username: logweir\n    tls: false\n\
             sample:\n  window_start: \"2026-08-29T00:00:00Z\"\n  \
             window_end: \"2026-08-30T02:00:00Z\"\n  records_per_partition: 25\n  \
             anchor: random\n\
             objectives:\n  rto_seconds: 900\n  rpo_seconds: 300\n  pass_rate: 1.0\n\
             evidence:\n  backend: filesystem\n  path: {evidence}\n\
             notifications:\n  webhooks: []\n",
            archive = archive.display(),
            evidence = dir.path().join("evidence").display(),
        ),
    )
    .unwrap();
    let mut a = OK_ARGS;
    a[0] = ("--spec", spec.to_str().unwrap());
    let password: &'static str = concat!("doctor-", "seeded-pw");
    let env = |binding: &'static str| {
        [
            ENGINE_OK[0],
            ("LOGWEIR_TARGET_PASSWORD", password),
            (
                "LOGWEIR_TARGET_CREDENTIAL_BINDING_EXPECTED",
                "v1:this-uid:sha256:00",
            ),
            ("LOGWEIR_TARGET_CREDENTIAL_BINDING", binding),
        ]
    };

    let refused = doctor(&a, &env("v1:another-uid:sha256:11"));
    let text = String::from_utf8_lossy(&refused.stdout).to_string()
        + &String::from_utf8_lossy(&refused.stderr);
    assert_eq!(refused.status.code(), Some(1), "{text}");
    assert!(text.contains("CredentialBindingMismatch"), "{text}");
    assert!(!text.contains(password), "{text}");
    assert!(
        listener.accept().is_err(),
        "doctor dialled the target with a credential bound to another connection"
    );

    // CONTROL: the matching binding passes the check and reaches the dial.
    let control = doctor(&a, &env("v1:this-uid:sha256:00"));
    let text = String::from_utf8_lossy(&control.stdout).to_string()
        + &String::from_utf8_lossy(&control.stderr);
    assert!(!text.contains("CredentialBindingMismatch"), "{text}");
}
