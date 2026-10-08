#![cfg(feature = "e2e")]
//! FX-20 on a REAL object store: the archive credential's binding is checked
//! by the shipped `logweir backup run` before it builds a store, against the
//! stack's MinIO.
//!
//! The loopback-sentinel rows (`crates/logweir/tests/credential_binding.rs`)
//! prove a refused run dials nothing; this file proves the other half, which a
//! sentinel cannot: with the Secret's binding EQUAL to the expectation the real
//! engine backs up into MinIO and signs its receipt (so the pair breaks no
//! run), and with a FOREIGN binding the same command exits 3 having written
//! not one object under its fresh `backup_id` — the bucket is unchanged.
//!
//! Run on a slot: `eval "$(e2e/compose/stack-env.sh --slot <N>)"`, then
//! `just e2e-up`, `./scripts/e2e-seed.sh`, and
//! `cargo test -p e2e --features e2e --test credential_binding_store -- --test-threads=1`
//! after `cargo build -p logweir` (see `harness::bin`).
mod harness;
use harness::*;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Every `backup_id` this file uses starts with this, so the sweep is exact.
const ID_PREFIX: &str = "fx20bind-";
const EXPECTED: &str = "v1:d0000000-0000-4000-8000-00000000fx20:sha256:aa";
const FOREIGN: &str = "v1:e0000000-0000-4000-8000-00000000fx20:sha256:bb";
const LIMIT: Duration = Duration::from_secs(600);

fn fresh_id() -> String {
    format!(
        "{ID_PREFIX}{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

/// An allowlist that does NOT name the live cluster (a backup's SOURCE must be
/// absent from the restore-target allowlist; `backup_argv.rs` has the reason).
fn allowlist() -> PathBuf {
    let p = demo_dir().join("fx20-backup-allowed-clusters.json");
    std::fs::write(
        &p,
        "{\"allowed_cluster_ids\": [\"SCRATCH-CLUSTER-NOT-THE-SOURCE\"]}\n",
    )
    .unwrap();
    p
}

fn spec(backup_id: &str) -> PathBuf {
    let p = demo_dir().join(format!("{backup_id}.yaml"));
    std::fs::write(
        &p,
        format!(
            "backup_id: {backup_id}\n\
             source:\n\
             \x20 bootstrap_servers: [{bootstrap}]\n\
             \x20 topics: [orders]\n\
             storage:\n\
             \x20 backend: s3\n\
             \x20 bucket: {ARCHIVE_BUCKET}\n\
             \x20 prefix: {backup_id}\n\
             \x20 region: us-east-1\n\
             \x20 endpoint: {s3}\n\
             \x20 path_style: true\n\
             \x20 allow_http: true\n\
             backup:\n\
             \x20 compression: zstd\n\
             \x20 segment_max_records: 1000\n\
             \x20 segment_max_bytes: 10485760\n\
             \x20 max_concurrent_partitions: 3\n",
            bootstrap = bootstrap(),
            s3 = s3_endpoint(),
        ),
    )
    .unwrap();
    p
}

/// `logweir backup run` with the REAL engine and MinIO's compose credential,
/// carrying the archive binding pair a controller-built Job carries:
/// `projected` is what the Secret's `logweir-binding` key holds.
fn backup(spec: &Path, projected: &str) -> Output {
    let mut c = Command::new(bin());
    c.args(["backup", "run", "--spec"])
        .arg(spec)
        .arg("--allowed-clusters")
        .arg(allowlist())
        .arg("--signing-key")
        .arg(root().join("e2e/fixtures/signed/signing.pem"))
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .env("AWS_REGION", "us-east-1")
        .env("LOGWEIR_ENGINE_BIN", engine_bin())
        .env("LOGWEIR_ENGINE_VERSION", engine_version())
        .env("LOGWEIR_ENGINE_DIGEST", engine_digest())
        .env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount())
        .env("TMPDIR", engine_mount())
        .env("LOGWEIR_ARCHIVE_CREDENTIAL_BINDING_EXPECTED", EXPECTED)
        .env("LOGWEIR_ARCHIVE_CREDENTIAL_BINDING", projected)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = c.spawn().expect("logweir spawns");
    let started = Instant::now();
    loop {
        if child.try_wait().expect("try_wait").is_some() {
            return child.wait_with_output().expect("output");
        }
        if started.elapsed() > LIMIT {
            let _ = child.kill();
            let _ = child.wait();
            panic!("`logweir backup run` exceeded {LIMIT:?}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Every key in the bucket that names `backup_id` — its archive prefix and
/// its receipts under `logweir/backups/<backup_id>/`.
fn keys_of(backup_id: &str) -> Vec<String> {
    let listed = mc(&[
        "--json",
        "ls",
        "--recursive",
        &format!("local/{ARCHIVE_BUCKET}"),
    ]);
    listed
        .stdout_utf8()
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|v| v["key"].as_str().map(str::to_string))
        .filter(|k| {
            k.starts_with(&format!("{backup_id}/"))
                || k.starts_with(&format!("logweir/backups/{backup_id}/"))
        })
        .collect()
}

fn sweep(backup_id: &str) {
    for prefix in [
        format!("local/{ARCHIVE_BUCKET}/{backup_id}"),
        format!("local/{ARCHIVE_BUCKET}/logweir/backups/{backup_id}"),
    ] {
        let _ = mc(&["rm", "--recursive", "--force", &prefix]);
    }
    assert!(
        keys_of(backup_id).is_empty(),
        "{backup_id} left objects behind"
    );
}

#[test]
fn a_foreign_archive_binding_writes_nothing_and_a_bound_one_backs_up() {
    // ---- FOREIGN: refused before any store; the bucket is unchanged -----
    let refused_id = fresh_id();
    let out = backup(&spec(&refused_id), FOREIGN);
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert_eq!(
        out.status.code(),
        Some(3),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert_eq!(
        stdout.lines().filter(|l| !l.trim().is_empty()).next_back(),
        Some("refusal-reason=CredentialBindingMismatch"),
        "{stdout}"
    );
    assert!(
        stderr.contains("LOGWEIR_ARCHIVE_CREDENTIAL_BINDING"),
        "{stderr}"
    );
    assert!(
        !stdout.contains("minioadmin") && !stderr.contains("minioadmin"),
        "the credential is never printed"
    );
    assert!(
        keys_of(&refused_id).is_empty(),
        "a refused run wrote to the bucket: {:?}",
        keys_of(&refused_id)
    );

    // ---- CONTROL: bound, the real engine backs up and the receipt lands --
    let bound_id = fresh_id();
    let out = backup(&spec(&bound_id), EXPECTED);
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    let landed = keys_of(&bound_id);
    sweep(&bound_id);
    assert_eq!(
        out.status.code(),
        Some(0),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout
            .lines()
            .any(|l| l.starts_with(&format!("receipt-key=logweir/backups/{bound_id}/"))),
        "{stdout}"
    );
    assert!(
        landed.iter().any(|k| k.ends_with("manifest.json"))
            && landed.iter().any(|k| k.ends_with(".receipt.json")),
        "the bound run's archive and receipt are in MinIO: {landed:?}"
    );
    assert!(!stderr.contains("CredentialBindingMismatch"), "{stderr}");
}
