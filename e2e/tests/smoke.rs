// e2e/tests/smoke.rs
#![cfg(feature = "e2e")]
use std::path::PathBuf;
use std::process::Command;

// Which stack `docker compose` reaches is the environment's (PROD-01.5): this
// file runs compose directly, so it applies the harness's coherence check.
#[allow(dead_code)]
#[path = "harness/stack.rs"]
mod stack;

/// Cargo runs an integration test with cwd = the owning package root (`e2e/`),
/// not the repo root, so the compose file is resolved from CARGO_MANIFEST_DIR.
fn compose_file() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("compose/docker-compose.yml")
}

/// The whole deliverable of this task, mechanised: the stack answers, and the
/// two buckets exist. No Logweir code is exercised — there is none yet.
#[test]
fn the_compose_stack_answers_and_the_buckets_exist() {
    stack::ensure_coherent();
    let cf = compose_file();
    let cf = cf.to_str().expect("compose path is utf-8");

    let out = Command::new("docker")
        .args([
            "compose",
            "-f",
            cf,
            "--profile",
            "setup",
            "run",
            "--rm",
            "-T",
            "--entrypoint",
            "kafka-topics",
            "topic-setup",
            "--bootstrap-server",
            "kafka-broker-1:9094",
            "--list",
        ])
        .output()
        .expect("docker compose");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let topics = String::from_utf8_lossy(&out.stdout);
    assert!(
        topics.contains("orders"),
        "seeded topics missing:\n{topics}"
    );

    let ls = Command::new("docker")
        .args([
            "compose",
            "-f",
            cf,
            "run",
            "--rm",
            "-T",
            "--entrypoint",
            "mc",
            "minio-setup",
            "ls",
            "local/",
        ])
        .output()
        .expect("docker compose");
    let buckets = String::from_utf8_lossy(&ls.stdout);
    for b in ["kafka-backups", "logweir-evidence"] {
        assert!(buckets.contains(b), "bucket `{b}` missing:\n{buckets}");
    }
}
