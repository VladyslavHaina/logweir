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
//! 4. that a SCRAM spec routes through Task 3's typed renderer refusal rather
//!    than through a panic;
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

/// **Fix round 1, review F-3.** `--out` / `--receipt-out` is interface I6's
/// refusal — a flag this build cannot honour, knowable with ZERO I/O — and it
/// is now phase −1's LOCAL step 4, so it is answered before a broker client
/// exists.
///
/// As landed it ran after phase −1's NETWORK step: the reviewer measured
/// **20.1 s and ~360 librdkafka lines against the SOURCE cluster** (the
/// production one) to say "this build writes no receipt". `bootstrap_servers`
/// names a port nothing listens on, so a client-first ordering has something
/// to fail at and cannot pass quietly.
///
/// Three independent assertions, because each catches the move-it-back mutant
/// on its own: the message (the mutant reports `kafka: unreachable` instead),
/// the absence of every rdkafka token, and the wall clock (the mutant blocks
/// on `rdkafka_reader.rs:16`'s 20 s metadata timeout; the bound here is 10 s,
/// half of it, against a measured sub-second refusal).
#[test]
fn a_receipt_flag_is_refused_without_opening_a_socket() {
    for flag in ["--receipt-out", "--out"] {
        let spec = backup_spec(
            "backup-i6.yaml",
            "[orders]",
            "",
            // Not the compose broker: nothing is bound here.
            "localhost:19099",
        );
        let doc = demo_dir().join("t4-i6-refused.json");
        let _ = std::fs::remove_file(&doc);

        let started = Instant::now();
        let out = backup_run(&spec, None)
            .arg(flag)
            .arg(&doc)
            .output()
            .expect("logweir");
        let elapsed = started.elapsed();

        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        assert_eq!(
            out.status.code(),
            Some(1),
            "{flag}\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
        assert!(
            stderr.contains("interface I6") && stderr.contains("Task 5b"),
            "{flag}: the refusal must name the contract that owes the document:\n{stderr}"
        );
        assert!(
            !stderr.contains("no broker answered") && !stderr.contains("unreachable"),
            "{flag}: the SOURCE cluster was dialled before a purely local refusal:\n{stderr}"
        );
        for stream in [&stdout, &stderr] {
            for needle in ["rdkafka", "librdkafka", "Connect to", "Connection refused"] {
                assert!(
                    !stream.contains(needle),
                    "{flag}: a locally-refusable run emitted `{needle}`, so it built a broker \
                     client against the SOURCE cluster before the guards ran (review \
                     F-3):\nstdout:\n{stdout}\nstderr:\n{stderr}"
                );
            }
        }
        assert!(
            elapsed.as_secs() < 10,
            "{flag}: the refusal took {elapsed:?}; a purely local refusal must not wait on \
             rdkafka_reader.rs:16's 20 s metadata timeout"
        );
        // Exit 1 prints no `refusal-reason=` line — Task 3's contract is
        // exit-3-only — and nothing was written where the operator asked.
        assert!(!stdout.contains("refusal-reason="), "{flag}:\n{stdout}");
        assert!(
            !doc.exists(),
            "{flag}: a document was written by a build that refuses to write one"
        );
    }
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
/// **AND IT IS SWEPT AWAY AGAIN, BEFORE AND AFTER.** This row is the only one
/// in the suite that ADDS an archive to the shared bucket, and
/// `harness::corrupt_a_non_oldest_segment` picks its victim by listing the
/// bucket ROOT and taking the last key in sort order
/// (`harness/mod.rs::archive_segment_keys`). A `t4real-…` prefix sorts after
/// `drill-demo/…`, so leaving one behind makes
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
        .filter(|k| k.starts_with(REAL_ENGINE_ID_PREFIX))
        .collect();
    if !mine.is_empty() {
        let mut prefixes: Vec<&str> = mine
            .iter()
            .filter_map(|k| k.split('/').next())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        prefixes.sort_unstable();
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
        .filter(|k| k.starts_with(REAL_ENGINE_ID_PREFIX))
        .collect();
    assert!(
        left.is_empty(),
        "this row's archive is still in {ARCHIVE_BUCKET}, and \
         harness::corrupt_a_non_oldest_segment would quarantine one of ITS segments instead of \
         the drill's: {left:?}"
    );
}
