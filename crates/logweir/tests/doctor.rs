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
///
/// **`--features e2e` since Task 5b, and NOT deleted.** MEASURED at 26.60 s,
/// the single slowest test in the workspace and a third of the whole default
/// suite on its own: ~6.5 s in check 6, where `AmazonS3Builder::from_env()`
/// with no credentials in the environment retries the EC2 metadata service at
/// the link-local `169.254.169.254` ten times, and then 20 s in check 7,
/// which is `crates/logweir-kafka/src/rdkafka_reader.rs:16`'s
/// `const T: Duration = Duration::from_secs(20)`.
///
/// The fixture's `127.0.0.1:1` is NOT a fast-failing mechanism and never was:
/// librdkafka retries a refused connect with backoff and the metadata REQUEST
/// sits until `T` expires, so an unroutable address and a black-holed one
/// cost exactly the same. The only way to make this form fast is to shorten
/// `T`, and `T` is open decision O18's (folded into G19) — changing it here
/// would take a scheduled decision about production behaviour on loaded and
/// cross-AZ clusters in order to win a test-suite second.
///
/// So the property moves rather than the constant. The default suite proves
/// it at `doctor::tests::evaluate_target_fails_and_names_the_target_when_the_cluster_is_unreachable`
/// against the stub reader, in microseconds; this form — the one that proves
/// it through the compiled binary, end to end, with a real client and a real
/// timeout — still runs, under `just e2e`.
/// The spec is BUILT HERE rather than read from
/// `e2e/fixtures/drill-unreachable.yaml`, for a reason that only shows up once
/// this test runs with the stack UP. That fixture's `source.storage` is
/// `s3://kafka-backups/basic-demo`, and `basic-demo` is not where anything
/// writes — `scripts/e2e-seed.sh:40` seeds `local/kafka-backups/drill-demo`,
/// and `examples/drill.yaml:11` documents `basic-demo` as a name borrowed from
/// a different upstream demo. With no stack that prefix was merely
/// unreachable, so check 6 SKIPPED and check 7 was still reached; with the
/// stack up it is reachable and EMPTY, so check 6 FAILS ("zero backup sets"),
/// `run` short-circuits on the first failure, and check 7 — the actual
/// subject — never runs at all. The test would have gone green on a check it
/// never reached.
///
/// So the spec here gives check 6 a local archive holding exactly one backup
/// set: check 6 passes identically with or without a stack, and check 7 is
/// always the check under test. `127.0.0.1:1` stays — the ruling that an
/// unroutable address is not a speed mechanism does not make it a wrong
/// address, it makes it an honest one.
///
/// `e2e/fixtures/drill-unreachable.yaml` is deliberately left byte-identical
/// (Task 5b acceptance check 12 forbids editing it) and is now unreferenced.
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

/// Fix round 2, M3: a reachable archive holding zero backup sets must be a
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
