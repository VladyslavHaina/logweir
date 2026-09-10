#![cfg(feature = "e2e")]
//! **The ONE binary-level assertion in Tasks 4, 5b and 6** (critique A F1),
//! and it lives here rather than in `crates/logweir/tests/` because it runs
//! the shipped binary.
//!
//! A binary cannot be handed an in-process `ClusterReader` double, so a
//! binary-level `backup run` needs a real broker for phase −1's cluster-id
//! read — and with the compose stack down that read blocks on
//! `crates/logweir-kafka/src/rdkafka_reader.rs:16`'s
//! `const T: Duration = Duration::from_secs(20)`, which is over Global
//! Constraint 22's 15 s per-test bound on its own. Everything about the seam
//! is asserted in process in `crates/logweir/tests/backup_run.rs`; what only a
//! process can answer is asserted here:
//!
//! 1. the exact argv the engine is spawned with;
//! 2. that a refused plan opens NO socket, and prints `refusal-reason=` last;
//! 3. that a SCRAM spec routes through Task 3's typed renderer refusal rather
//!    than through a panic.
//!
//! Requires `just e2e` (the stack up, and `LOGWEIR_SEED_REFRESH_FIXTURES=0
//! ./scripts/e2e-seed.sh` already run): the engine stub writes no archive, so
//! the read-back after the run reads the archive the seed put in MinIO. A stub
//! that fabricated a manifest would be testing itself.
mod harness;
use harness::*;
use std::path::{Path, PathBuf};
use std::process::Command;

/// `scripts/e2e-seed.sh`'s `backup_id`, and therefore the archive prefix the
/// read-back finds. Deliberately the SEEDED set: this suite proves the argv
/// and the read-back wiring, not the engine's ability to produce an archive
/// (the stub produces none).
const SEEDED_BACKUP_ID: &str = "drill-demo";

/// An allowlist that does NOT name the live cluster. `--allowed-clusters` is
/// the restore-TARGET allowlist, and GC18(c) rail 4 refuses a SOURCE cluster
/// that appears in it — so the compose broker must be absent from it for a
/// backup of that broker to be admitted at all. (`harness::write_allowlist`
/// writes the opposite, because the drill needs the live id PRESENT.)
fn backup_allowlist() -> PathBuf {
    let p = demo_dir().join("backup-allowed-clusters.json");
    std::fs::write(
        &p,
        "{\"allowed_cluster_ids\": [\"SCRATCH-CLUSTER-NOT-THE-SOURCE\"]}\n",
    )
    .unwrap();
    p
}

/// The host-side backup spec `examples/backup.yaml` documents, bound to the
/// seeded archive.
fn backup_spec(name: &str, topics: &str, auth: &str, bootstrap: &str) -> PathBuf {
    let p = demo_dir().join(name);
    std::fs::write(
        &p,
        format!(
            "backup_id: {SEEDED_BACKUP_ID}\n\
             source:\n\
             \x20 bootstrap_servers: [{bootstrap}]\n\
             \x20 topics: {topics}\n\
             {auth}\
             storage:\n\
             \x20 backend: s3\n\
             \x20 bucket: {ARCHIVE_BUCKET}\n\
             \x20 prefix: {SEEDED_BACKUP_ID}\n\
             \x20 region: us-east-1\n\
             \x20 endpoint: http://localhost:9000\n\
             \x20 path_style: true\n\
             \x20 allow_http: true\n\
             backup:\n\
             \x20 compression: zstd\n\
             \x20 segment_max_records: 1000\n\
             \x20 segment_max_bytes: 10485760\n\
             \x20 max_concurrent_partitions: 3\n"
        ),
    )
    .unwrap();
    p
}

/// `logweir backup run`, with the engine pointed at the argv-recording stub.
fn backup_run(spec: &Path, argv_log: Option<&Path>) -> Command {
    let mut c = Command::new(bin());
    c.args(["backup", "run", "--spec"])
        .arg(spec)
        .arg("--allowed-clusters")
        .arg(backup_allowlist())
        .arg("--signing-key")
        .arg(root().join("e2e/fixtures/signed/signing.pem"))
        // MinIO's compose credentials, for the read-back's object_store handle.
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .env("AWS_REGION", "us-east-1")
        // The STUB, not the real engine: this suite is about the argv, and the
        // stub refuses a `--config` that does not resolve to a document
        // carrying the renderer's own `mode: backup` marker (exit 42), so a
        // broken argv fails loudly instead of passing quietly.
        .env(
            "LOGWEIR_ENGINE_BIN",
            root().join("e2e/fixtures/fake-engine-argv-check.sh"),
        )
        // `std::env::temp_dir()` reads TMPDIR, and that is what puts the
        // rendered backup.yaml inside the one directory this suite knows.
        .env("TMPDIR", engine_mount());
    if let Some(l) = argv_log {
        c.env("LOGWEIR_ARGV_LOG", l);
    }
    c
}

/// The engine's `backup` takes `--config` and NOTHING ELSE
/// [VERIFIED U:crates/kafka-backup-cli/src/main.rs:35-39,559-561]. The argv is
/// read out of the stub's own recording, not out of a string this test built:
/// asserting on the latter would prove nothing about the array `run_engine`
/// passed to `Command::args`.
///
/// The exit code is read DIRECTLY from `Command::status()` — never through a
/// pipe (STANDING RULE 20).
#[test]
fn backup_run_renders_and_invokes_the_engine_backup_command() {
    let log = demo_dir().join("backup-argv.log");
    let _ = std::fs::remove_file(&log);
    let spec = backup_spec("backup.yaml", "[orders, payments]", "", BOOTSTRAP);

    let status = backup_run(&spec, Some(&log)).status().expect("logweir");
    assert_eq!(
        status.code(),
        Some(0),
        "backup run did not exit 0; if the stub exited 42 the --config path did not resolve to \
         the rendered document, and if it exited 64 the subcommand was not `backup`"
    );

    let argv: Vec<String> = std::fs::read_to_string(&log)
        .expect("the stub recorded no argv at all — LOGWEIR_ARGV_LOG did not reach the child")
        .lines()
        .map(str::to_string)
        .collect();
    assert_eq!(
        argv.len(),
        3,
        "the engine's backup argv must be exactly three elements; got {argv:?}"
    );
    assert_eq!(argv[0], "backup");
    assert_eq!(argv[1], "--config");
    // `<workdir>/backup.yaml`, where `<workdir>` is `$TMPDIR/logweir-<pid>` and
    // the pid is the CHILD's — so the shape is asserted rather than the exact
    // string, and then the file itself is read to prove the path resolved to
    // the document the renderer wrote.
    let cfg = Path::new(&argv[2]);
    assert!(
        argv[2].starts_with(engine_mount().to_str().unwrap()),
        "the rendered config is outside TMPDIR: {argv:?}"
    );
    assert_eq!(
        cfg.file_name().unwrap().to_str().unwrap(),
        "backup.yaml",
        "{argv:?}"
    );
    let workdir = cfg.parent().unwrap().file_name().unwrap().to_str().unwrap();
    let pid = workdir.strip_prefix("logweir-").unwrap_or_default();
    assert!(
        !pid.is_empty() && pid.chars().all(|c| c.is_ascii_digit()),
        "the workdir is not `logweir-<pid>`: {workdir}"
    );
    let doc = std::fs::read_to_string(cfg).expect("the rendered backup.yaml is on disk");
    assert!(doc.contains("\nmode: backup\n"), "{doc}");
    assert!(
        doc.contains(&format!("backup_id: \"{SEEDED_BACKUP_ID}\"")),
        "{doc}"
    );
    // GC18(c) rail 3 / Global Constraint 4, in the bytes that actually ran.
    for forbidden in ["purge_topics", "dry_run", "header_preflight_external"] {
        assert!(!doc.contains(forbidden), "{forbidden} in:\n{doc}");
    }
}

/// **A refused `backup run` opens no socket.** Carried finding from Task 3's
/// review: the drill's `context` builds its Kafka client before phase 0's
/// guards run, so a refusal dials the target first. Phase −1 must not, and on
/// the BACKUP path the cluster in question is the SOURCE — the production one.
///
/// `bootstrap_servers` names a port nothing listens on, so a client-first
/// ordering has something to fail at; the assertions are that the exit code is
/// 3 (a client-first ordering that errored would be 1), that
/// `refusal-reason=GuardRefused` is the FINAL stdout line (**I9**), and that
/// no rdkafka/librdkafka line appears in either stream.
#[test]
fn backup_run_refuses_without_opening_a_socket() {
    let spec = backup_spec(
        "backup-glob.yaml",
        "[\"orders*\"]",
        "",
        // Not the compose broker: nothing is bound here, so a reader built
        // before the guards cannot succeed quietly.
        "localhost:19099",
    );
    let out = backup_run(&spec, None).output().expect("logweir");
    assert_eq!(
        out.status.code(),
        Some(3),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert_eq!(
        stdout.lines().last(),
        Some("refusal-reason=GuardRefused"),
        "the refusal reason must be the FINAL stdout line (I9):\n{stdout}"
    );
    for stream in [&stdout, &stderr] {
        for needle in ["rdkafka", "librdkafka", "Connect to", "Connection refused"] {
            assert!(
                !stream.contains(needle),
                "a refused backup run emitted `{needle}`, so it built a broker client before \
                 the guards ran:\nstdout:\n{stdout}\nstderr:\n{stderr}"
            );
        }
    }
    assert!(stderr.contains("glob metacharacter"), "{stderr}");
}

/// Task 2's review carry: a SCRAM spec on the backup path must route through
/// Task 3's TYPED refusal and never reach a placeholder arm.
///
/// It exits **1**, not 3, and prints no `refusal-reason=` line — and that is
/// the contract, not an oversight. The refusal is raised by
/// `render_backup::render` inside `OsoCliEngine::backup`, i.e. after phase −1
/// admitted the plan, so ruling R-E's mapping applies exactly as it does to
/// the phase-5/phase-6 renderer refusals: `EngineError::Operational`, exit 1,
/// no artifact. Task 3's `exit::print_refusal_reason` is exit-3-only by
/// contract. `crates/logweir/tests/backup_run.rs::backup_run_records_the_source_auth`
/// is the other half: the spec's mechanism is recorded faithfully rather than
/// downgraded to plaintext on the operator's behalf.
#[test]
fn a_scram_backup_spec_is_a_typed_refusal_never_a_panic() {
    let spec = backup_spec(
        "backup-scram.yaml",
        "[orders]",
        "  auth:\n    mode: scramSha512\n    username: logweir\n",
        BOOTSTRAP,
    );
    let out = backup_run(&spec, None).output().expect("logweir");
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert_eq!(
        out.status.code(),
        Some(1),
        "stdout:\n{}\nstderr:\n{stderr}",
        String::from_utf8_lossy(&out.stdout)
    );
    assert!(
        stderr.contains("auth mode `sasl-scram-sha-512` is not supported by this build"),
        "{stderr}"
    );
    assert!(
        stderr.contains("interface I1"),
        "the refusal must name where the capability lands:\n{stderr}"
    );
    for panicky in ["panicked at", "not yet implemented", "RUST_BACKTRACE"] {
        assert!(!stderr.contains(panicky), "{stderr}");
    }
    // And nothing was rendered unauthenticated: no backup.yaml naming a
    // plaintext source was left behind for this run.
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        !stdout.contains("refusal-reason="),
        "exit 1 prints no refusal-reason line (Task 3's contract is exit-3-only):\n{stdout}"
    );
}
