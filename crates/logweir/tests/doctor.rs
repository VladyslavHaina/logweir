//! Task 14b. Seven checks, one test each — see task-14b-brief.md and its
//! binding addendum. Each test names the check it exercises and asserts the
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
    assert_fails_with(out, "version mismatch: expected 0.21.0");
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
/// failure rather than a generic one — otherwise an adopter with a firewall
/// problem reads "doctor failed" and nothing else.
#[test]
fn check_7_an_unreachable_target_is_named_as_a_target_problem() {
    let mut a = OK_ARGS;
    a[0] = ("--spec", "../../e2e/fixtures/drill-unreachable.yaml");
    assert_fails_with(doctor(&a, &ENGINE_OK), "target");
}
