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
//! 3. that a flag this build cannot honour is refused with no socket either
//!    (fix round 1, review F-3);
//! 4. that a SCRAM spec RENDERS and is refused only for the reason the local
//!    stack actually gives (Task 6 replaced Task 3's renderer refusal with the
//!    real render);
//! 5. **that the REAL, digest-pinned engine accepts the document this product
//!    renders** and the whole command exits 0 (fix round 1, review F-1).
//!
//! Requires `just e2e` (the stack up, and `LOGWEIR_SEED_REFRESH_FIXTURES=0
//! ./scripts/e2e-seed.sh` already run). Rows 1–4 use the argv-recording stub,
//! which writes no archive, so their read-back reads the archive the seed put
//! in MinIO — a stub that fabricated a manifest would be testing itself. Row 5
//! uses `harness::engine_bin()`, i.e. the pinned `kafka-backup` itself, and
//! captures a FRESH archive of its own: a stub accepts any document carrying
//! `mode: backup` and therefore **structurally cannot** reject one, which is
//! exactly how a rendered key the engine drops as unknown reached a shipped
//! commit (review F-1).
mod harness;
use harness::*;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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
    backup_spec_with_id(name, SEEDED_BACKUP_ID, topics, auth, bootstrap)
}

/// The same shape with the `backup_id` — and therefore the archive prefix —
/// chosen by the caller. Row 5 needs one no earlier run has used: the engine's
/// `backup` does not accumulate into an existing prefix (see `justfile`'s
/// `e2e-seed` header), so a fresh capture needs a fresh id or it leaves a
/// partial archive behind.
fn backup_spec_with_id(
    name: &str,
    backup_id: &str,
    topics: &str,
    auth: &str,
    bootstrap: &str,
) -> PathBuf {
    let p = demo_dir().join(name);
    std::fs::write(
        &p,
        format!(
            "backup_id: {backup_id}\n\
             source:\n\
             \x20 bootstrap_servers: [{bootstrap}]\n\
             \x20 topics: {topics}\n\
             {auth}\
             storage:\n\
             \x20 backend: s3\n\
             \x20 bucket: {ARCHIVE_BUCKET}\n\
             \x20 prefix: {backup_id}\n\
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
        // THE ENGINE IDENTITY, as the runner image sets it — the same two
        // variables `harness::mod.rs`'s drill runner exports, from the same two
        // sources (the engine's own `--version`, and the digest pinned in
        // `third_party/`). Task 5b's `backup run` REFUSES to take a backup it
        // could not name the engine for, because the receipt it signs carries
        // `engine.digest` and GC7 pins by digest and never by tag; without
        // these two the command exits 1 before the engine is spawned.
        .env("LOGWEIR_ENGINE_VERSION", engine_version())
        .env("LOGWEIR_ENGINE_DIGEST", engine_digest())
        // `std::env::temp_dir()` reads TMPDIR, and that is what puts the
        // rendered backup.yaml inside the one directory this suite knows.
        .env("TMPDIR", engine_mount());
    if let Some(l) = argv_log {
        c.env("LOGWEIR_ARGV_LOG", l);
    }
    c
}

/// `logweir backup run` with the **REAL** engine — `harness::engine_bin()`,
/// which is the native `.engine/kafka-backup` where it can exec and otherwise
/// `e2e/fixtures/engine-docker.sh` running the digest-pinned image with argv
/// forwarded verbatim. `LOGWEIR_E2E_ENGINE_MOUNT` is what tells that shim
/// which host directory to bind at the same absolute path, so the `--config`
/// path Logweir rendered resolves inside the container too.
///
/// Everything else is `backup_run`'s environment, so the ONLY difference
/// between this row and the argv rows is which parser reads the document.
fn backup_run_real_engine(spec: &Path) -> Command {
    let mut c = Command::new(bin());
    c.args(["backup", "run", "--spec"])
        .arg(spec)
        .arg("--allowed-clusters")
        .arg(backup_allowlist())
        .arg("--signing-key")
        .arg(root().join("e2e/fixtures/signed/signing.pem"))
        // The engine reads and writes MinIO with these; the shim forwards all
        // three into the container.
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .env("AWS_REGION", "us-east-1")
        .env("LOGWEIR_ENGINE_BIN", engine_bin())
        // See `backup_run` above: the receipt names the engine, so the runner
        // is given the same identity the image would set.
        .env("LOGWEIR_ENGINE_VERSION", engine_version())
        .env("LOGWEIR_ENGINE_DIGEST", engine_digest())
        .env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount())
        .env("TMPDIR", engine_mount());
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

/// Task 2's review carry, as it stands after **Task 6**: a SCRAM spec on the
/// backup path RENDERS. It used to route through Task 3's typed
/// `RenderError::UnsupportedAuthMode`; that refusal is gone, and this row now
/// asserts the two things a process can still answer with `63066d2`'s compose
/// stack.
///
/// # What this row can and cannot prove, and why
///
/// **`63066d2`'s stack has no SCRAM listener.** STANDING RULE 15 gives
/// `e2e/compose/docker-compose.yml` one owner — Task 7, at slot 10 — which
/// adds `SASL://kafka-broker-1:9096` + `SASLEXT://localhost:9097` and the
/// `scram-setup` service that must have exited 0 before any SASL client
/// authenticates. Until then **no test anywhere may claim that a SCRAM
/// connection succeeds**, and this one does not: it asserts the exit code and
/// the message of the two paths that do not need a listener.
///
///   * `$LOGWEIR_SOURCE_PASSWORD` **unset** → exit **1**, before any client
///     exists, naming the variable. Operational and not a refusal: nothing
///     about the plan was found wanting.
///   * `$LOGWEIR_SOURCE_PASSWORD` **unrenderable** → exit **3** with
///     `refusal-reason=CredentialNotRenderable` as the final stdout line, and
///     no socket opened.
///
/// Neither prints a panic, which is what Task 2's F1 was really about.
#[test]
fn a_scram_backup_spec_renders_and_refuses_only_for_a_credential_reason() {
    let spec = backup_spec(
        "backup-scram.yaml",
        "[orders]",
        "  auth:\n    mode: scramSha512\n    username: logweir\n",
        BOOTSTRAP,
    );

    // ---- unset: exit 1, naming the variable, no refusal-reason line ----
    let out = backup_run(&spec, None)
        .env_remove("LOGWEIR_SOURCE_PASSWORD")
        .env_remove("LOGWEIR_TARGET_PASSWORD")
        .output()
        .expect("logweir");
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    assert_eq!(
        out.status.code(),
        Some(1),
        "an absent Secret is operational: stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("auth.mode is scramSha512 but $LOGWEIR_SOURCE_PASSWORD is unset"),
        "the message must name the variable an operator has to project:\n{stderr}"
    );
    assert!(
        !stdout.contains("refusal-reason="),
        "exit 1 prints no refusal-reason line (the contract is exit-3-only):\n{stdout}"
    );
    for panicky in ["panicked at", "not yet implemented", "RUST_BACKTRACE"] {
        assert!(!stderr.contains(panicky), "{stderr}");
    }
    // The refusal Task 3 raised here is GONE — the arm renders now.
    assert!(
        !stderr.contains("is not supported by this build"),
        "Task 6 implemented the arm; a refusal here would be the capability taken back out:\n         {stderr}"
    );

    // ---- unrenderable: exit 3, the credential state, and no value leaked ----
    let out = backup_run(&spec, None)
        .env("LOGWEIR_SOURCE_PASSWORD", "x\"\n bootstrap_servers:")
        .output()
        .expect("logweir");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert_eq!(
        out.status.code(),
        Some(3),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert_eq!(
        stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .next_back()
            .unwrap_or(""),
        "refusal-reason=CredentialNotRenderable",
        "stdout:\n{stdout}"
    );
    for stream in [&stdout, &stderr] {
        assert!(
            !stream.contains("bootstrap_servers:") || !stream.contains("x\""),
            "no fragment of the projected credential may reach any stream:\n{stream}"
        );
    }
}

/// **THE REAL ENGINE ACCEPTS THE RENDERED SASL BLOCK** — the nesting and the
/// mechanism spelling, proved against the digest-pinned binary rather than
/// against a stub (review F-1's whole point: a stub accepts any document
/// carrying `mode: backup` and therefore structurally cannot reject one).
///
/// It does **not** authenticate: `63066d2`'s stack has no SCRAM listener
/// (STANDING RULE 15 — Task 7, slot 10), so the run fails on the connection.
/// What it proves is everything up to that point, which is exactly the part
/// only the real engine can answer:
///
///   * **no unknown-key warning**, so
///     `OsoCliEngine::assert_no_dropped_logweir_key` did not abort — rendered
///     one level too high, the four keys come back as
///     *"Ignoring unknown config key `source.security_protocol`"* and the run
///     exits 1 AFTER writing the document;
///   * **no config PARSE error**, so `sasl_mechanism: "SCRAM-SHA512"` is a
///     value the engine's `SaslMechanism` accepts — the two-hyphen spelling is
///     a serde TYPE error that aborts config load.
///
/// The failure it DOES expect is a broker failure, which is a fact about the
/// stack and not about the document.
#[test]
fn the_real_engine_accepts_the_rendered_sasl_block() {
    let spec = backup_spec(
        "backup-scram-real-engine.yaml",
        "[orders]",
        "  auth:\n    mode: scramSha512\n    username: logweir\n",
        BOOTSTRAP,
    );
    let out = backup_run_real_engine(&spec)
        .env("LOGWEIR_SOURCE_PASSWORD", "not-a-real-secret-Aa1")
        .output()
        .expect("logweir");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    let both = format!("{stdout}\n{stderr}");

    for dropped in [
        "Ignoring unknown config key `source.security_protocol`",
        "Ignoring unknown config key `source.sasl_mechanism`",
        "Ignoring unknown config key `source.sasl_username`",
        "Ignoring unknown config key `source.sasl_password`",
    ] {
        assert!(
            !both.contains(dropped),
            "the engine DROPPED a key logweir rendered — the four SASL keys are fields of \
             KafkaConfig.security, not of KafkaConfig, so a two-space nesting makes the whole \
             block a no-op:\n{both}"
        );
    }
    assert!(
        !both.contains("unknown variant `SCRAM-SHA-512`"),
        "the engine's mechanism spelling has ONE hyphen:\n{both}"
    );
    assert!(
        !both.contains("Failed to parse config"),
        "the rendered document must be a document this engine can load:\n{both}"
    );
    // And the placeholder was expanded, not left in the file: an UNSET
    // variable would produce the engine's own warning instead.
    assert!(
        !both.contains("Environment variable 'LOGWEIR_SOURCE_PASSWORD' is not set"),
        "logweir must project the password before spawning the engine:\n{both}"
    );
    for panicky in ["panicked at", "not yet implemented"] {
        assert!(!both.contains(panicky), "{both}");
    }
}

/// **Fix round 1, review F-3, carried forward by Task 5b.** A flag
/// combination this build cannot honour is refused with ZERO I/O, in phase
/// −1's LOCAL step 4 — before a broker client exists.
///
/// Task 4 refused `--out`/`--receipt-out` outright, because it wrote no
/// receipt; **Task 5b writes one**, so both flags work and the row now drives
/// the one shape that remains impossible: two DIFFERENT paths for the single
/// document this command writes. The property under test is unchanged and is
/// the one the reviewer measured — as landed, the check ran after phase −1's
/// NETWORK step and cost **20.1 s and ~360 librdkafka lines against the SOURCE
/// cluster** (the production one) to answer a question about argv.
///
/// `bootstrap_servers` names a port nothing listens on, so a client-first
/// ordering has something to fail at and cannot pass quietly.
///
/// Four independent assertions, because each catches the move-it-back mutant
/// on its own: the message (the mutant reports `kafka: unreachable` instead),
/// the absence of every rdkafka token, the wall clock (the mutant blocks on
/// `rdkafka_reader.rs:16`'s 20 s metadata timeout; the bound here is 10 s,
/// half of it, against a measured sub-second refusal), and that neither
/// document was written.
#[test]
fn two_receipt_paths_are_refused_without_opening_a_socket() {
    let spec = backup_spec(
        "backup-i6.yaml",
        "[orders]",
        "",
        // Not the compose broker: nothing is bound here.
        "localhost:19099",
    );
    let a = demo_dir().join("t5b-i6-a.json");
    let b = demo_dir().join("t5b-i6-b.json");
    for doc in [&a, &b] {
        let _ = std::fs::remove_file(doc);
    }

    let started = Instant::now();
    let out = backup_run(&spec, None)
        .arg("--receipt-out")
        .arg(&a)
        .arg("--out")
        .arg(&b)
        .output()
        .expect("logweir");
    let elapsed = started.elapsed();

    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert_eq!(
        out.status.code(),
        Some(1),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("name DIFFERENT paths"),
        "the refusal must say what cannot be honoured:\n{stderr}"
    );
    assert!(
        !stderr.contains("no broker answered") && !stderr.contains("unreachable"),
        "the SOURCE cluster was dialled before a purely local refusal:\n{stderr}"
    );
    for stream in [&stdout, &stderr] {
        for needle in ["rdkafka", "librdkafka", "Connect to", "Connection refused"] {
            assert!(
                !stream.contains(needle),
                "a locally-refusable run emitted `{needle}`, so it built a broker client \
                 against the SOURCE cluster before the guards ran (review \
                 F-3):\nstdout:\n{stdout}\nstderr:\n{stderr}"
            );
        }
    }
    assert!(
        elapsed.as_secs() < 10,
        "the refusal took {elapsed:?}; a purely local refusal must not wait on \
         rdkafka_reader.rs:16's 20 s metadata timeout"
    );
    // Exit 1 prints no `refusal-reason=` line — Task 3's contract is
    // exit-3-only — and no I7 key line either, because nothing was uploaded.
    assert!(!stdout.contains("refusal-reason="), "{stdout}");
    assert!(
        !stdout.contains("receipt-key=") && !stdout.contains("sidecar-key="),
        "a refused run must name no evidence key:\n{stdout}"
    );
    for doc in [&a, &b] {
        assert!(
            !doc.exists(),
            "{}: a document was written by a run that refused the flags",
            doc.display()
        );
    }
}

/// **I6 and I7 at PROCESS level, over the real bucket.**
///
/// The in-process rows in `crates/logweir/tests/backup_run.rs` assert the
/// receipt's content, its two create-only keys and the order of the two stdout
/// lines against doubles. This row asserts the parts only a real process and a
/// real object store can answer: that the receipt really lands in MinIO under
/// Global Constraint 6's `logweir/` root, that `--receipt-out` leaves a
/// verifiable local pair on disk, and that the two `*-key=` lines an operator
/// (and Task 20) reads are the LAST two lines of the process's stdout.
///
/// It runs against the SEEDED archive (`drill-demo`) with the argv-recording
/// stub, exactly as `backup_run_renders_and_invokes_the_engine_backup_command`
/// does: the read-back needs an archive that exists, and this row is about the
/// evidence rather than about the engine. Its two evidence objects are swept
/// at both ends, so the shared bucket is left as it was found.
#[test]
fn backup_run_writes_and_prints_its_receipt_keys() {
    sweep_seeded_receipts();
    let spec = backup_spec("backup-i7.yaml", "[orders, payments]", "", BOOTSTRAP);
    let local = demo_dir().join("t5b-receipt.json");
    let local_sig = demo_dir().join("t5b-receipt.sig");
    for f in [&local, &local_sig] {
        let _ = std::fs::remove_file(f);
    }

    let out = backup_run(&spec, None)
        .arg("--receipt-out")
        .arg(&local)
        .output()
        .expect("logweir");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert_eq!(
        out.status.code(),
        Some(0),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // **I7.** The final two lines, in order, with nothing after them.
    let lines: Vec<&str> = stdout.lines().collect();
    let n = lines.len();
    assert!(n >= 2, "stdout is too short:\n{stdout}");
    let receipt_line = lines[n - 2];
    let sidecar_line = lines[n - 1];
    assert!(
        receipt_line.starts_with("receipt-key=logweir/backups/drill-demo/")
            && receipt_line.ends_with(".receipt.json"),
        "the PENULTIMATE stdout line is `receipt-key=<key>`, got {receipt_line:?} \
         in:\n{stdout}"
    );
    assert!(
        sidecar_line.starts_with("sidecar-key=logweir/backups/drill-demo/")
            && sidecar_line.ends_with(".receipt.sig"),
        "the FINAL stdout line is `sidecar-key=<key>`, got {sidecar_line:?} in:\n{stdout}"
    );

    // **GC6 / I6's evidence half.** Both objects are really in the bucket, at
    // exactly those keys.
    let receipt_key = receipt_line.split_once('=').unwrap().1;
    let sidecar_key = sidecar_line.split_once('=').unwrap().1;
    let listed = mc(&[
        "--json",
        "ls",
        "--recursive",
        &format!("local/{ARCHIVE_BUCKET}/{RECEIPT_PREFIX}"),
    ]);
    let keys: Vec<String> = listed
        .stdout_utf8()
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|v| v["key"].as_str().map(|k| format!("{RECEIPT_PREFIX}{k}")))
        .collect();
    for want in [receipt_key, sidecar_key] {
        assert!(
            keys.iter().any(|k| k == want),
            "{want} is not in the bucket; listed: {keys:?}"
        );
    }

    // **I6's local half**, and it verifies — through the auditor's reader, so
    // this row also proves the SHIPPED python verifier accepts what the
    // shipped runner signs.
    assert!(local.exists() && local_sig.exists(), "{stdout}");
    let py = auditor_python();
    let verify = Command::new(&py)
        .current_dir(root())
        .arg("docs/verify_scorecard.py")
        .args(["--payload-type", "backup-receipt"])
        .arg(&local)
        .arg(&local_sig)
        .arg(root().join("e2e/fixtures/signed/public.pem"))
        .output()
        .expect("run docs/verify_scorecard.py");
    assert_eq!(
        verify.status.code(),
        Some(0),
        "the auditor's verifier refused the receipt this run signed:\nstdout:{}\nstderr:{}",
        String::from_utf8_lossy(&verify.stdout),
        String::from_utf8_lossy(&verify.stderr)
    );

    sweep_seeded_receipts();
}

/// Everything `logweir backup run` puts lives under this, and nothing an
/// archive contains does (Global Constraint 6).
const RECEIPT_PREFIX: &str = "logweir/";

/// Remove the evidence objects the two seeded-archive rows leave behind, and
/// prove they are gone.
///
/// `logweir/backups/drill-demo/` only — never the archive, and never another
/// row's prefix. The keys are unique per run (`<run_id>.receipt.json`), so
/// nothing here is create-only-blocked; the sweep exists so the shared bucket
/// is left exactly as it was found, which is the rule every row that adds an
/// object to it follows.
fn sweep_seeded_receipts() {
    let _ = mc(&[
        "rm",
        "--recursive",
        "--force",
        &format!("local/{ARCHIVE_BUCKET}/{RECEIPT_PREFIX}backups/{SEEDED_BACKUP_ID}/"),
    ]);
    let after = mc(&[
        "--json",
        "ls",
        "--recursive",
        &format!("local/{ARCHIVE_BUCKET}/{RECEIPT_PREFIX}backups/{SEEDED_BACKUP_ID}/"),
    ]);
    let left: Vec<String> = after
        .stdout_utf8()
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|v| v["key"].as_str().map(str::to_string))
        .collect();
    assert!(
        left.is_empty(),
        "receipts left under {RECEIPT_PREFIX}backups/{SEEDED_BACKUP_ID}/: {left:?}"
    );
}

/// **Fix round 1, review F-1 — THE ROW A STUB CANNOT BE.**
///
/// `logweir backup run` drives the REAL, digest-pinned engine
/// (`harness::engine_bin()`: the native `.engine/kafka-backup` where it can
/// exec, otherwise `e2e/fixtures/engine-docker.sh` running
/// `osodevops/kafka-backup@<the digest in third_party/>` with argv forwarded
/// verbatim) against the live compose broker, and the whole command exits
/// **0** with a manifest in MinIO.
///
/// Why this row has to exist. The rendered BACKUP document carried
/// `backup.strip_offset_headers`, which is a field of the engine's
/// `RestoreOptions` and of nothing else, so the engine dropped it as an
/// unknown key and `assert_no_dropped_logweir_key` — correctly, per spec
/// §7.2(a) — aborted the run: **exit 1 after a complete archive had been
/// written.** Every one of Task 4's other rows was green, because the
/// argv-recording stub accepts any config carrying `mode: backup` and can
/// therefore never reject a document. Only the real parser can.
///
/// The archive is fresh (`backup_id` carries a unix-nanos suffix): the
/// engine's `backup` does not accumulate into an existing prefix, so re-using
/// the seed's `drill-demo` would corrupt the fixture archive every other e2e
/// row reads.
///
/// **AND IT IS SWEPT AWAY AGAIN, BEFORE AND AFTER.** This row ADDS an archive
/// to the shared bucket, and `harness::corrupt_a_non_oldest_segment` picks its
/// victim by taking the last key in sort order. Since Task 12 the helper it
/// reads (`harness/mod.rs::archive_segment_keys`) is SCOPED to the archive
/// prefix it is given — `drill-demo` — so a stray `t4real-…` can no longer be
/// the victim; before that scoping it listed the bucket ROOT, a `t4real-…`
/// prefix sorts after `drill-demo/…`, and leaving one behind made
/// `a_corrupted_segment_yields_exit_2_and_a_signed_preflight_failed_scorecard`
/// quarantine THIS row's segment and then watch an intact drill archive exit 0
/// — measured, on the first full-recipe run of this fix round. Hence
/// `sweep_real_engine_archives` at both ends: at the start too, so a row that
/// died mid-flight in an earlier run cannot poison a later one.
///
/// The exit code is read DIRECTLY from `Output::status` — never through a pipe
/// (STANDING RULE 20).
#[test]
fn the_real_engine_accepts_the_rendered_backup_document() {
    sweep_real_engine_archives();
    let backup_id = format!(
        "{REAL_ENGINE_ID_PREFIX}{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let spec = backup_spec_with_id(
        "backup-real-engine.yaml",
        &backup_id,
        "[orders, payments]",
        "",
        BOOTSTRAP,
    );

    let out = backup_run_real_engine(&spec).output().expect("logweir");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    eprintln!("[t4-f1] engine route: {}", engine_bin().display());
    eprintln!("[t4-f1] stdout:\n{stdout}");

    assert_eq!(
        out.status.code(),
        Some(0),
        "the real engine refused the document logweir rendered, or the run failed after it \
         \nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    // The exact class F-1 was: a key OF OURS the engine silently ignored.
    for dropped in ["ignored the config key", "strip_offset_headers"] {
        assert!(
            !stderr.contains(dropped) && !stdout.contains(dropped),
            "the engine dropped a key logweir rendered (`{dropped}`); the BACKUP document must \
             carry no restore-side key (review F-1):\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
    }

    // The summary line quotes only measured values, so parsing it is reading
    // this run's own facts back.
    let summary = stdout
        .lines()
        .find(|l| l.starts_with(&format!("backup {backup_id} captured ")))
        .unwrap_or_else(|| panic!("no summary line for {backup_id} in:\n{stdout}"));
    let records: u64 = summary
        .split("captured ")
        .nth(1)
        .and_then(|s| s.split(' ').next())
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no record count in: {summary}"));
    assert!(
        records > 0,
        "the engine reported a complete backup of nothing: {summary}"
    );
    assert!(
        summary.contains("across 2 topic(s)"),
        "both named topics must be in the archive: {summary}"
    );
    // Read from the BROKER by phase −1 step 4, never from the spec.
    assert!(
        summary.contains(&format!("from cluster {}", cluster_id())),
        "the source cluster id must be the live broker's: {summary}"
    );
    assert!(
        summary.contains("sha256:"),
        "the manifest digest is over the bytes this run read back: {summary}"
    );

    // THE MANIFEST IS THERE — checked in MinIO, not taken from the engine's
    // word. `e2e-seed.sh`'s own header is about exactly this: "the command
    // exited 0" is not evidence that an archive exists.
    let manifest_key = format!("{backup_id}/{backup_id}/manifest.json");
    assert!(
        summary.contains(&manifest_key),
        "the summary must name {manifest_key}: {summary}"
    );
    let listed = mc(&[
        "--json",
        "ls",
        "--recursive",
        &format!("local/{ARCHIVE_BUCKET}/{backup_id}/"),
    ]);
    let keys: Vec<String> = listed
        .stdout_utf8()
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|v| v["key"].as_str().map(str::to_string))
        .collect();
    assert!(
        keys.iter().any(|k| k.ends_with("manifest.json")),
        "no manifest under {ARCHIVE_BUCKET}/{backup_id}/ — the engine exited 0 without \
         writing an archive. mc listed: {keys:?}"
    );
    assert!(
        keys.iter().any(|k| k.ends_with(".zst")),
        "no compressed segment under {ARCHIVE_BUCKET}/{backup_id}/: {keys:?}"
    );
    eprintln!(
        "[t4-f1] exit 0 · {records} record(s) · {} object(s) · {manifest_key}",
        keys.len()
    );

    // Leave the shared bucket exactly as it was found — see this row's doc
    // comment for the drill row it otherwise breaks.
    sweep_real_engine_archives();
}

/// Every `backup_id` this row has ever used starts with this, and nothing else
/// in the tree does — so the sweep below can be exact rather than heuristic.
const REAL_ENGINE_ID_PREFIX: &str = "t4real-";

/// Remove every archive `the_real_engine_accepts_the_rendered_backup_document`
/// has left in the shared archive bucket, and prove it.
///
/// `mc rm --recursive --force` over a prefix that does not exist is not an
/// error worth failing on (the first run of a fresh stack has nothing to
/// remove), so the removal's status is not asserted — the LISTING afterwards
/// is, which is the property that actually matters.
fn sweep_real_engine_archives() {
    let listed = mc(&[
        "--json",
        "ls",
        "--recursive",
        &format!("local/{ARCHIVE_BUCKET}"),
    ]);
    let mine: Vec<String> = listed
        .stdout_utf8()
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|v| v["key"].as_str().map(str::to_string))
        // The archive keys, AND (since Task 5b) the two evidence objects this
        // row's own run puts under Global Constraint 6's `logweir/` root:
        // `logweir/backups/t4real-<nanos>/<run_id>.receipt.{json,sig}`. Both
        // classes name this row's `backup_id`, which is what makes the sweep
        // exact rather than heuristic.
        .filter(|k| {
            k.starts_with(REAL_ENGINE_ID_PREFIX)
                || k.starts_with(&format!("{RECEIPT_PREFIX}backups/{REAL_ENGINE_ID_PREFIX}"))
        })
        .collect();
    if !mine.is_empty() {
        // The directory to remove: the archive's top-level `t4real-…/`, or the
        // evidence's `logweir/backups/t4real-…/` (three segments).
        let mut prefixes: Vec<String> = mine
            .iter()
            .map(|k| {
                let segs: Vec<&str> = k.split('/').collect();
                if k.starts_with(RECEIPT_PREFIX) {
                    segs[..3].join("/")
                } else {
                    segs[0].to_string()
                }
            })
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        prefixes.sort();
        for p in prefixes {
            let _ = mc(&[
                "rm",
                "--recursive",
                "--force",
                &format!("local/{ARCHIVE_BUCKET}/{p}/"),
            ]);
        }
    }

    let after = mc(&[
        "--json",
        "ls",
        "--recursive",
        &format!("local/{ARCHIVE_BUCKET}"),
    ]);
    let left: Vec<String> = after
        .stdout_utf8()
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|v| v["key"].as_str().map(str::to_string))
        .filter(|k| {
            k.starts_with(REAL_ENGINE_ID_PREFIX)
                || k.starts_with(&format!("{RECEIPT_PREFIX}backups/{REAL_ENGINE_ID_PREFIX}"))
        })
        .collect();
    assert!(
        left.is_empty(),
        "this row's archive is still in {ARCHIVE_BUCKET}, and \
         harness::corrupt_a_non_oldest_segment would quarantine one of ITS segments instead of \
         the drill's: {left:?}"
    );
}
