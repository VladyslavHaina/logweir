#![cfg(feature = "e2e")]
#![allow(dead_code)]
//! Shared by every `e2e/tests/*.rs`. Host-side, so every bootstrap is
//! `localhost:9092` (the EXTERNAL listener); container-side is
//! `kafka-broker-1:9094`.
//!
//! Each file under `e2e/tests/` is its own test binary, so without one shared
//! module the helpers get reinvented incompatibly — the same reason
//! `crates/logweir/tests/fixtures/mod.rs` exists.
//!
//! # Three things this harness derives at run time rather than checking in
//!
//! 1. **The allowlist.** `examples/allowed-clusters.json` names
//!    `MkU3OEVBNTcwNTJENDM2Qk`, and the compose broker generates a fresh KRaft
//!    cluster id every time its container is recreated (measured: this stack
//!    answers `5L6g3nShT-eMCtK--X86sw`). A checked-in allowlist can therefore
//!    never admit this broker, so `run_with` writes `.e2e/allowed-clusters.json`
//!    from the LIVE cluster id. `drill_run_with_allowlist` exists precisely so
//!    the allowlist row of the refusal table can still be driven by a fixture
//!    the guard actually reads.
//! 2. **The sample window.** `scripts/e2e-seed.sh` produces records NOW, and
//!    `examples/drill.yaml` ships a fixed 2026-08-29..30 window. Left alone,
//!    every drill in this suite would restore zero records and score
//!    `fail-integrity` — a false RED, not a false green, but useless. So
//!    `spec_default` binds the window to the archive: it reads the newest
//!    record timestamp off the broker (the same records the archive holds) and
//!    sets `window_end` one second past it. `window_end` is also the requested
//!    recovery point phase 8 measures RPO against, so this makes RPO the real
//!    "coverage gap at the requested point" (0 s) instead of a measurement of
//!    how long this test suite happens to take.
//! 3. **The engine's execution route.** See `engine_bin`.
//!
//! Nothing here is silent about any of it: `engine_bin` prints the route it
//! chose, and `spec_default` prints the window it bound.
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use logweir_kafka::rdkafka_reader::RdKafkaReader;
use logweir_kafka::reader::{AuthConfig, ClusterReader, TopicDeleter};

/// The EXTERNAL listener, as published by `e2e/compose/docker-compose.yml`.
pub const BOOTSTRAP: &str = "localhost:9092";
/// `scripts/e2e-seed.sh`'s `backup_id`, and therefore the archive prefix.
pub const ARCHIVE_PREFIX: &str = "drill-demo";
pub const ARCHIVE_BUCKET: &str = "kafka-backups";
pub const EVIDENCE_BUCKET: &str = "logweir-evidence";
pub const MARKER_TOPIC: &str = "logweir.scratch";
/// `examples/drill.yaml`'s `target.topic_mapping_prefix`.
pub const SCRATCH_PREFIX: &str = "drill-";
/// Where `corrupt_a_non_oldest_segment` parks the object it removes, so the
/// archive can be put back exactly as it was.
const QUARANTINE: &str = "logweir-e2e-quarantine";

pub fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .canonicalize()
        .expect("the workspace root resolves")
}

/// The SHIPPED binary, driven as a subprocess. Everything this suite asserts
/// about exit codes is an assertion about this file.
///
/// USE `cargo test --workspace --features e2e` (what `just e2e` runs), NOT
/// `cargo test -p e2e --features e2e`. The `e2e` package depends on the
/// `logweir` LIBRARY, so a `-p e2e` run does not rebuild the BINARY — it will
/// happily test whatever `target/debug/logweir` was left over from an earlier
/// build. Measured while mutation-testing this suite: a mutant that mapped
/// `DrillError::NotPass` to exit 0 SURVIVED a `-p e2e` run and was killed
/// immediately once the workspace was rebuilt.
pub fn bin() -> PathBuf {
    let p = root().join("target/debug/logweir");
    assert!(
        p.exists(),
        "{} is missing; run `cargo test --workspace --features e2e`, which builds it",
        p.display()
    );
    p
}

pub fn demo_dir() -> PathBuf {
    let d = root().join(".e2e");
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// The ONE host directory the engine reads and writes: the rendered
/// `restore.yaml` / `validation.yaml`, the restore checkpoint, and the
/// sub-report `oso_evidence_verify` hands back to the engine. It is exported to
/// the `logweir` child as `TMPDIR`, so `std::env::temp_dir()` — which
/// `drill::context` and `build_plan` both use — lands inside it, and it is
/// bind-mounted at the same absolute path when the engine runs in a container.
pub fn engine_mount() -> PathBuf {
    let d = demo_dir().join("tmp");
    std::fs::create_dir_all(&d).unwrap();
    d
}

pub trait StdoutExt {
    fn stderr_utf8(&self) -> String;
    fn stdout_utf8(&self) -> String;
}
impl StdoutExt for Output {
    fn stderr_utf8(&self) -> String {
        String::from_utf8_lossy(&self.stderr).into_owned()
    }
    fn stdout_utf8(&self) -> String {
        String::from_utf8_lossy(&self.stdout).into_owned()
    }
}

// ---------------------------------------------------------------------------
// The engine: native binary where it runs, a container where it cannot.
// ---------------------------------------------------------------------------

/// Upstream publishes `osodevops/kafka-backup` for **linux/amd64 only**, so the
/// binary `scripts/extract-engine.sh` produces is a Linux ELF. On a CI runner
/// it executes directly; on the darwin/arm64 host this repo is developed on it
/// cannot exec at all (ENOEXEC — `logweir doctor` reports exit 126), and every
/// phase that spawns it would fail for a reason that says nothing about
/// Logweir.
///
/// So the route is PROBED, never assumed, and the probe is `--version`, which
/// global ruling GR8 settles as outside Global Constraint 3 (it prints a string
/// and acts on no cluster and no bucket). When the native binary answers, it is
/// used. When it cannot, `e2e/fixtures/engine-docker.sh` runs the same
/// digest-pinned image under `--platform linux/amd64` and forwards argv
/// verbatim.
///
/// The choice is PRINTED. A suite that quietly swapped its engine would be
/// reporting green about something other than what it claims to test.
pub fn engine_bin() -> PathBuf {
    // Probed ONCE: the docker route costs a container start, and `run_with`
    // asks for the path several times per drill.
    static ROUTE: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    ROUTE.get_or_init(probe_engine_bin).clone()
}

fn probe_engine_bin() -> PathBuf {
    let native = root().join(".engine/kafka-backup");
    let ok = Command::new(&native)
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if ok {
        eprintln!("[e2e] engine route: NATIVE {}", native.display());
        native
    } else {
        let shim = root().join("e2e/fixtures/engine-docker.sh");
        assert!(shim.exists(), "{} is missing", shim.display());
        eprintln!(
            "[e2e] engine route: DOCKER via {} — {} cannot exec on this host \
             (upstream publishes linux/amd64 only)",
            shim.display(),
            native.display()
        );
        shim
    }
}

pub fn engine_digest() -> String {
    std::fs::read_to_string(root().join("third_party/kafka-backup-binary.digest"))
        .expect("third_party/kafka-backup-binary.digest")
        .trim()
        .to_string()
}

/// Read off the engine itself rather than hardcoded, so the value that reaches
/// the signed scorecard describes the binary that actually ran.
pub fn engine_version() -> String {
    static V: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    V.get_or_init(read_engine_version).clone()
}

fn read_engine_version() -> String {
    let o = Command::new(engine_bin())
        .env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount())
        .arg("--version")
        .output()
        .expect("engine --version");
    assert!(o.status.success(), "engine --version: {}", o.stderr_utf8());
    o.stdout_utf8()
        .split_whitespace()
        .last()
        .expect("`kafka-backup <version>`")
        .to_string()
}

// ---------------------------------------------------------------------------
// Compose shell-outs (mc / kafka-topics), in one shape.
// ---------------------------------------------------------------------------

fn compose(service: &str, entrypoint: &str, args: &[&str]) -> Output {
    let mut c = Command::new("docker");
    c.args([
        "compose",
        "-f",
        "e2e/compose/docker-compose.yml",
        "run",
        "--rm",
        "-T",
        "--entrypoint",
        entrypoint,
    ]);
    if entrypoint.starts_with("kafka-") {
        // The cp-kafka image sets JMX flags meant for a long-running broker.
        c.args(["-e", "KAFKA_OPTS="]);
    }
    c.arg(service);
    c.args(args);
    c.current_dir(root()).output().expect("docker compose")
}

pub fn mc(args: &[&str]) -> Output {
    compose("minio-setup", "mc", args)
}

pub fn kafka_topics(args: &[&str]) -> Output {
    compose("topic-setup", "kafka-topics", args)
}

fn ok(o: Output, what: &str) -> Output {
    assert!(
        o.status.success(),
        "{what} failed: {}\n{}",
        o.stdout_utf8(),
        o.stderr_utf8()
    );
    o
}

// ---------------------------------------------------------------------------
// Cluster helpers.
// ---------------------------------------------------------------------------

pub fn reader() -> RdKafkaReader {
    RdKafkaReader::connect(&[BOOTSTRAP.to_string()], AuthConfig::Plaintext)
        .expect("the compose broker answers on localhost:9092 — run `just e2e-up`")
}

pub fn cluster_id() -> String {
    reader().cluster_id().expect("cluster id")
}

pub fn count_partitions(topic: &str) -> i32 {
    let r = reader();
    ClusterReader::list_topics(&r)
        .unwrap()
        .into_iter()
        .find(|t| t.name == topic)
        .map(|t| t.partitions)
        .unwrap_or_else(|| panic!("topic {topic} not found"))
}

pub fn topic_exists(topic: &str) -> bool {
    let r = reader();
    ClusterReader::list_topics(&r)
        .unwrap()
        .iter()
        .any(|t| t.name == topic)
}

/// Deletes every `drill-` topic. `RdKafkaReader::delete_topics` refuses every
/// name until a scratch prefix is set, so the prefix is set here too — the
/// harness gets no wider deletion power than the drill itself has.
pub fn delete_all_drill_topics() {
    let r = reader()
        .with_scratch_prefix(SCRATCH_PREFIX)
        .expect("`drill-` is a usable scratch namespace");
    let names: Vec<String> = ClusterReader::list_topics(&r)
        .unwrap()
        .into_iter()
        .map(|t| t.name)
        .filter(|n| n.starts_with(SCRATCH_PREFIX))
        .collect();
    if names.is_empty() {
        return;
    }
    TopicDeleter::delete_topics(&r, &names).unwrap();
    // Deletion is asynchronous on the broker; wait for the metadata to agree
    // rather than assuming it already does.
    for _ in 0..60 {
        let left = ClusterReader::list_topics(&r)
            .unwrap()
            .into_iter()
            .filter(|t| t.name.starts_with(SCRATCH_PREFIX))
            .count();
        if left == 0 {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    panic!("scratch topics still present 15s after delete_topics");
}

pub fn delete_marker_topic() {
    ok(
        kafka_topics(&[
            "--bootstrap-server",
            "kafka-broker-1:9094",
            "--delete",
            "--topic",
            MARKER_TOPIC,
        ]),
        "delete marker topic",
    );
    for _ in 0..60 {
        if !topic_exists(MARKER_TOPIC) {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    panic!("marker topic still present 15s after --delete");
}

pub fn recreate_marker_topic() {
    ok(
        kafka_topics(&[
            "--bootstrap-server",
            "kafka-broker-1:9094",
            "--create",
            "--if-not-exists",
            "--partitions",
            "1",
            "--replication-factor",
            "1",
            "--topic",
            MARKER_TOPIC,
        ]),
        "recreate marker topic",
    );
    for _ in 0..60 {
        if topic_exists(MARKER_TOPIC) {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    panic!("marker topic still absent 15s after --create");
}

/// The newest record timestamp across the SOURCE topics, read the same way
/// `phase7_verify::newest_restored` reads the target: last record of each
/// partition. These are the records `scripts/e2e-seed.sh` backed up, so this is
/// also the newest record the archive holds.
pub fn newest_source_record_ts_ms() -> i64 {
    let r = reader();
    let mut newest = 0i64;
    for topic in ["orders", "payments"] {
        for (p, hi) in ClusterReader::end_offsets(&r, topic).unwrap() {
            if hi <= 0 {
                continue;
            }
            for rec in ClusterReader::consume_range(&r, topic, p, hi - 1, 1).unwrap() {
                newest = newest.max(rec.timestamp_ms);
            }
        }
    }
    assert!(
        newest > 0,
        "the source topics hold no records — run `./scripts/e2e-seed.sh`"
    );
    newest
}

fn rfc3339(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .expect("a representable instant")
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

// ---------------------------------------------------------------------------
// Specs.
// ---------------------------------------------------------------------------

/// `examples/drill.yaml` as shipped, with two run-time bindings the module doc
/// explains: the sample window, and `teardown: keep`.
///
/// `teardown: keep` is here so the suite can INSPECT what the drill built —
/// `count_partitions("drill-orders")` is the only observation that proves the
/// engine honoured the rendered partition count, and a run that deletes its own
/// scratch topics erases the evidence before any assertion can read it. The
/// shipped `delete` policy is not left untested: it gets its own test, and
/// `run_with` deletes every `drill-` topic BEFORE each run, so each drill still
/// starts from an empty scratch namespace.
pub fn spec_default() -> serde_yaml::Value {
    let mut v: serde_yaml::Value =
        serde_yaml::from_str(&std::fs::read_to_string(root().join("examples/drill.yaml")).unwrap())
            .unwrap();
    let newest = newest_source_record_ts_ms();
    let start = rfc3339(newest - 24 * 3600 * 1000);
    let end = rfc3339(newest + 1000);
    eprintln!("[e2e] sample window bound to the archive: {start} .. {end}");
    v["sample"]["window_start"] = start.into();
    v["sample"]["window_end"] = end.into();
    v["target"]["teardown"] = "keep".into();
    // NOTE: `sample.anchor` is NOT overridden here. It was, in fix round 0,
    // to work around the example shipping `anchor: random` — which phase 7
    // cannot reconcile and which therefore reported a byte-for-byte correct
    // restore as `fail-integrity` with `pass_rate_measured: 0.08`. That is now
    // fixed at the source: the anchor is a closed enum defaulting to `head`,
    // `tail`/`random` are refused at phase 0, and the example says `head`. A
    // harness that kept overriding it would hide exactly the defect
    // `the_shipped_example_spec_runs_as_written_and_never_reports_a_false_fail`
    // exists to catch.
    v
}

/// The shipped example, with NOTHING overridden except the one field that is
/// necessarily data-dependent. Used by the regression test for the defect
/// `spec_default`'s note describes.
pub fn spec_example_with_only_the_window_bound() -> serde_yaml::Value {
    let mut v: serde_yaml::Value =
        serde_yaml::from_str(&std::fs::read_to_string(root().join("examples/drill.yaml")).unwrap())
            .unwrap();
    let newest = newest_source_record_ts_ms();
    v["sample"]["window_start"] = rfc3339(newest - 24 * 3600 * 1000).into();
    v["sample"]["window_end"] = rfc3339(newest + 1000).into();
    v
}

/// `sample.anchor` exactly as `examples/drill.yaml` spells it, for a test that
/// needs to prove it read the value from the file rather than from itself.
pub fn example_anchor() -> String {
    let v: serde_yaml::Value =
        serde_yaml::from_str(&std::fs::read_to_string(root().join("examples/drill.yaml")).unwrap())
            .unwrap();
    v["sample"]["anchor"]
        .as_str()
        .expect("examples/drill.yaml states sample.anchor")
        .to_string()
}

/// A topic the archive does not hold. See `full_drill.rs` for why this is the
/// honest replacement for the brief's compacted-topic row.
pub fn spec_with_unrestorable_topic() -> serde_yaml::Value {
    let mut v = spec_default();
    v["source"]["topics"] = serde_yaml::from_str("[orders-compacted]").unwrap();
    v
}

fn write_spec(v: &serde_yaml::Value) -> PathBuf {
    let p = demo_dir().join(format!("drill-{}.yaml", std::process::id()));
    std::fs::write(&p, serde_yaml::to_string(v).unwrap()).unwrap();
    p
}

/// The allowlist the guard actually reads, built from the LIVE cluster id.
fn write_allowlist() -> PathBuf {
    let p = demo_dir().join("allowed-clusters.json");
    let doc = serde_json::json!({
        "allowed_cluster_ids": [cluster_id()],
        "source_cluster_id": null,
    });
    std::fs::write(&p, serde_json::to_vec_pretty(&doc).unwrap()).unwrap();
    p
}

// ---------------------------------------------------------------------------
// Keys and approvals.
// ---------------------------------------------------------------------------

fn gen_p256(to: &Path) {
    let sk = logweir_evidence::keys::SigningKey::generate_p256();
    std::fs::write(to, sk.to_pkcs8_pem().unwrap()).unwrap();
}

fn pubkey_of(private: &Path, to: &Path) {
    let sk = logweir_evidence::keys::SigningKey::from_pem_file(private).unwrap();
    std::fs::write(to, sk.verifying_key().to_public_key_pem().unwrap()).unwrap();
}

/// The approval must bind the EXACT spec bytes, so it is re-minted per run.
/// `over` is the text the `plan_hash` is computed from — the same file for a
/// valid approval, a DIFFERENT one for the stale-approval row.
fn mint_approval(over: &str, approver_pem: &Path) -> PathBuf {
    let doc = serde_json::json!({
        "approver": "e2e@example.com",
        "ticket": "CHG-E2E",
        "plan_hash": logweir_core::ids::sha256_prefixed(over.as_bytes()),
        "approved_at": "2026-09-02T17:40:00Z",
    });
    let bytes = serde_json::to_vec_pretty(&doc).unwrap();
    let p = demo_dir().join("approval.json");
    std::fs::write(&p, &bytes).unwrap();
    let sk = logweir_evidence::keys::SigningKey::from_pem_file(approver_pem).unwrap();
    let side = logweir_evidence::sign::sign_detached(
        &sk,
        logweir::drill::phase1_approval::PAYLOAD_TYPE_APPROVAL,
        &bytes,
    )
    .unwrap();
    std::fs::write(p.with_extension("sig"), serde_json::to_vec(&side).unwrap()).unwrap();
    p
}

// ---------------------------------------------------------------------------
// Running a drill.
// ---------------------------------------------------------------------------

pub struct Run {
    pub out: Output,
    pub scorecard: PathBuf,
    pub sig: PathBuf,
    pub pubkey: PathBuf,
}

impl Run {
    /// The run id Logweir minted, lifted out of its own structured log. Used to
    /// bind an assertion to THIS run rather than to whatever else happens to be
    /// in the bucket.
    pub fn run_id(&self) -> String {
        // The scorecard is authoritative when one exists; it is the document
        // whose key in the bucket the caller is about to look for.
        if let Ok(b) = std::fs::read(&self.scorecard) {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&b) {
                if let Some(id) = v["run_id"].as_str() {
                    return id.to_string();
                }
            }
        }
        let both = format!("{}{}", self.out.stdout_utf8(), self.out.stderr_utf8());
        // `drill::summary_line`: "run <id> — outcome … — last phase completed …".
        // Present on every run that produced a drill result, and the only
        // stdout Logweir emits when RUST_LOG is unset (its JSON subscriber is
        // built with `EnvFilter::from_default_env()`, which defaults to ERROR).
        for line in both.lines() {
            if let Some(rest) = line.strip_prefix("run ") {
                if let Some(id) = rest.split_whitespace().next() {
                    return id.to_string();
                }
            }
        }
        let i = both
            .find("\"run_id\":\"")
            .unwrap_or_else(|| panic!("no run_id in the drill's own output:\n{both}"));
        let rest = &both[i + "\"run_id\":\"".len()..];
        rest[..rest.find('"').expect("closing quote")].to_string()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Approval {
    Valid,
    /// Signed correctly, but over different spec bytes.
    StaleHash,
    /// Over the right bytes, by a key that is not the one presented.
    WrongKey,
}

pub struct RunOpts<'a> {
    pub spec: &'a serde_yaml::Value,
    /// Approver key == signing key: allowed, but labelled `self_attested`.
    pub same_key: bool,
    pub signing: Option<&'a Path>,
    pub allowlist: Option<&'a Path>,
    pub approval: Approval,
}

impl<'a> RunOpts<'a> {
    pub fn new(spec: &'a serde_yaml::Value) -> Self {
        Self {
            spec,
            same_key: false,
            signing: None,
            allowlist: None,
            approval: Approval::Valid,
        }
    }
}

pub fn run_with(o: RunOpts<'_>) -> Run {
    // Every drill in this suite starts from an EMPTY scratch namespace. Without
    // this a second restore would append to the first run's topics and phase 7
    // would reconcile against doubled offsets — a red for a reason that has
    // nothing to do with the thing under test.
    delete_all_drill_topics();

    let sp = write_spec(o.spec);
    let d = demo_dir();
    let signer = o
        .signing
        .map(Path::to_path_buf)
        .unwrap_or_else(|| root().join("e2e/fixtures/signed/signing.pem"));
    let approver_pem = if o.same_key {
        signer.clone()
    } else {
        d.join("approver.pem")
    };
    if !o.same_key && !approver_pem.exists() {
        gen_p256(&approver_pem);
    }
    // ALWAYS under `.e2e/`, never beside the private key: with `same_key` the
    // approver key IS `e2e/fixtures/signed/signing.pem`, and
    // `with_extension("pub.pem")` would drop a generated public key into the
    // TRACKED fixture directory — which `.gitignore` deliberately un-ignores
    // for `*.pem`, so a `git add -A` would commit it.
    let approver_pub = demo_dir().join("approver.pub.pem");
    pubkey_of(&approver_pem, &approver_pub);
    let signer_pub = d.join("signer.pub.pem");
    // A signing key that cannot be read has no public half to write out; the
    // exit-1 row deliberately passes such a key, and its assertions never touch
    // `Run::pubkey`.
    if logweir_evidence::keys::SigningKey::from_pem_file(&signer).is_ok() {
        pubkey_of(&signer, &signer_pub);
    }

    let spec_text = std::fs::read_to_string(&sp).unwrap();
    let (approval, approver_key_arg) = match o.approval {
        Approval::Valid => (
            mint_approval(&spec_text, &approver_pem),
            approver_pub.clone(),
        ),
        // Bound to bytes that are NOT this run's spec.
        Approval::StaleHash => (
            mint_approval(
                &format!("{spec_text}# not the plan that ran\n"),
                &approver_pem,
            ),
            approver_pub.clone(),
        ),
        // Minted by a second, unrelated key; the key PRESENTED is the ordinary
        // approver's, so the signature cannot verify.
        Approval::WrongKey => {
            let other = d.join("other-approver.pem");
            gen_p256(&other);
            (mint_approval(&spec_text, &other), approver_pub.clone())
        }
    };

    let allowlist = o
        .allowlist
        .map(Path::to_path_buf)
        .unwrap_or_else(write_allowlist);
    let out_json = d.join("scorecard.json");
    let _ = std::fs::remove_file(&out_json);
    let _ = std::fs::remove_file(out_json.with_extension("sig"));

    let out = Command::new(bin())
        .args(["drill", "run", "--spec"])
        .arg(&sp)
        .arg("--approval")
        .arg(&approval)
        .arg("--approver-key")
        .arg(&approver_key_arg)
        .arg("--allowed-clusters")
        .arg(&allowlist)
        .arg("--signing-key")
        .arg(&signer)
        .arg("--out")
        .arg(&out_json)
        // MinIO's compose credentials, for BOTH object_store here and the
        // engine in its container (the shim forwards these two).
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .env("AWS_REGION", "us-east-1")
        .env("LOGWEIR_ENGINE_BIN", engine_bin())
        .env("LOGWEIR_ENGINE_VERSION", engine_version())
        .env("LOGWEIR_ENGINE_DIGEST", engine_digest())
        // `std::env::temp_dir()` reads TMPDIR: this is what puts the rendered
        // restore.yaml and the checkpoint inside the one directory the engine
        // container has mounted.
        .env("TMPDIR", engine_mount())
        .env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount())
        .output()
        .unwrap();

    Run {
        out,
        scorecard: out_json.clone(),
        sig: out_json.with_extension("sig"),
        pubkey: signer_pub,
    }
}

pub fn drill_run(spec: &serde_yaml::Value) -> Run {
    run_with(RunOpts::new(spec))
}

pub fn drill_run_with_same_key() -> Run {
    let s = spec_default();
    let mut o = RunOpts::new(&s);
    o.same_key = true;
    run_with(o)
}

pub fn drill_run_with_signing_key(k: &Path) -> Run {
    let s = spec_default();
    let mut o = RunOpts::new(&s);
    o.signing = Some(k);
    run_with(o)
}

pub fn drill_run_with_allowlist(spec: &serde_yaml::Value, allowlist: &Path) -> Run {
    let mut o = RunOpts::new(spec);
    o.allowlist = Some(allowlist);
    run_with(o)
}

pub fn drill_run_with_stale_approval() -> Run {
    let s = spec_default();
    let mut o = RunOpts::new(&s);
    o.approval = Approval::StaleHash;
    run_with(o)
}

pub fn drill_run_with_wrong_approver_key() -> Run {
    let s = spec_default();
    let mut o = RunOpts::new(&s);
    o.approval = Approval::WrongKey;
    run_with(o)
}

pub fn read_scorecard(r: &Run) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(&r.scorecard).unwrap_or_else(|e| {
        panic!(
            "no scorecard at {}: {e}\n{}",
            r.scorecard.display(),
            r.out.stderr_utf8()
        )
    }))
    .unwrap()
}

pub fn logweir_verify(r: &Run) -> std::process::ExitStatus {
    Command::new(bin())
        .args(["drill", "verify", "--scorecard"])
        .arg(&r.scorecard)
        .arg("--signature")
        .arg(&r.sig)
        .arg("--public-key")
        .arg(&r.pubkey)
        .status()
        .unwrap()
}

/// The auditor's independent verifier. It needs `cryptography`; when the
/// interpreter cannot import it this PANICS rather than returning a status a
/// test could read as a pass — a verifier that never ran is not agreement.
pub fn python_verify(r: &Run) -> std::process::ExitStatus {
    let py = python();
    let probe = Command::new(&py)
        .args(["-c", "import cryptography"])
        .output()
        .expect("python3");
    assert!(
        probe.status.success(),
        "{} cannot import `cryptography`, so docs/verify_scorecard.py cannot run. \
         Install it (`pip install cryptography`), or point LOGWEIR_E2E_PYTHON at an \
         interpreter that has it. This is NOT skipped: the second verifier is the point.",
        py.display()
    );
    Command::new(&py)
        .arg(root().join("docs/verify_scorecard.py"))
        .arg(&r.scorecard)
        .arg(&r.sig)
        .arg(&r.pubkey)
        .status()
        .unwrap()
}

fn python() -> PathBuf {
    if let Ok(p) = std::env::var("LOGWEIR_E2E_PYTHON") {
        return PathBuf::from(p);
    }
    let venv = root().join(".e2e/venv/bin/python3");
    if venv.exists() {
        return venv;
    }
    PathBuf::from("python3")
}

/// Decodes `engine_subreport.body_b64` to a file and hands the EXACT bytes to
/// OSO's own verifier. Anything that re-serialises here defeats the test.
///
/// Global Constraint 3 binds the `logweir` BINARY to
/// {restore, validate-restore, validation run}; `validation evidence-verify`
/// here is spec §14 SP1c's exit criterion and is explicitly permitted in this
/// file. The directory is under `engine_mount()` so the containerised engine
/// route can see it.
pub fn oso_evidence_verify(sub: &serde_json::Value) -> std::process::ExitStatus {
    use base64::Engine as _;
    let raw = base64::engine::general_purpose::STANDARD
        .decode(sub["body_b64"].as_str().expect("body_b64"))
        .unwrap();
    let dir = engine_mount().join("engine-subreport");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    let id = v["report_id"].as_str().expect("report_id");
    std::fs::write(dir.join(format!("{id}.json")), &raw).unwrap();
    Command::new(engine_bin())
        .args(["validation", "evidence-verify", "--path"])
        .arg(&dir)
        .args(["--report-id", id])
        .env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount())
        .status()
        .unwrap()
}

// ---------------------------------------------------------------------------
// The archive and the evidence bucket.
// ---------------------------------------------------------------------------

fn archive_segment_keys() -> Vec<String> {
    // Listed from the BUCKET ROOT, because `mc --json ls` reports `key`
    // relative to the path it was given: listing `.../drill-demo` yields
    // `drill-demo/topics/...` (the backup_id under the storage prefix), which
    // is NOT a usable `mc` argument. From the root the keys come back
    // bucket-relative and compose correctly.
    let o = ok(
        mc(&[
            "--json",
            "ls",
            "--recursive",
            &format!("local/{ARCHIVE_BUCKET}"),
        ]),
        "mc ls archive",
    );
    let mut keys: Vec<String> = o
        .stdout_utf8()
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|v| v["key"].as_str().map(str::to_string))
        .filter(|k| k.contains("/topics/") && k.rsplit('/').next().unwrap().starts_with("segment-"))
        .collect();
    keys.sort();
    keys
}

/// Removes a NON-OLDEST segment — the case `dry_run_check_segments` catches and
/// the oldest-segment canary does not — by MOVING it aside rather than deleting
/// it, so `restore_the_corrupted_segment` can put the archive back exactly as it
/// was. Without that, one test would silently break every later one: libtest
/// runs a binary's tests in name order, and `a_corrupted_segment…` sorts before
/// `a_full_drill…`.
pub fn corrupt_a_non_oldest_segment() -> String {
    let keys = archive_segment_keys();
    assert!(
        keys.len() > 1,
        "expected several segments in the archive, found {keys:?} — run ./scripts/e2e-seed.sh"
    );
    let key = keys.last().unwrap().clone();
    ok(
        mc(&[
            "mv",
            &format!("local/{ARCHIVE_BUCKET}/{key}"),
            &format!("local/{ARCHIVE_BUCKET}/{QUARANTINE}/seg.bin.zst"),
        ]),
        "quarantine a segment",
    );
    eprintln!("[e2e] archive segment removed for this test: {key}");
    key
}

pub fn restore_the_corrupted_segment(key: &str) {
    ok(
        mc(&[
            "mv",
            &format!("local/{ARCHIVE_BUCKET}/{QUARANTINE}/seg.bin.zst"),
            &format!("local/{ARCHIVE_BUCKET}/{key}"),
        ]),
        "restore the quarantined segment",
    );
}

/// Every object key in the evidence bucket, sorted.
pub fn list_evidence_bucket() -> Vec<String> {
    let o = mc(&[
        "--json",
        "ls",
        "--recursive",
        &format!("local/{EVIDENCE_BUCKET}"),
    ]);
    // An empty bucket is not an error; `mc ls` prints nothing.
    let mut keys: Vec<String> = o
        .stdout_utf8()
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|v| v["key"].as_str().map(str::to_string))
        .collect();
    keys.sort();
    keys
}

/// The subset of `list_evidence_bucket` that belongs to one run.
pub fn evidence_for_run(run_id: &str) -> Vec<String> {
    list_evidence_bucket()
        .into_iter()
        .filter(|k| k.contains(run_id))
        .collect()
}

/// `phases[n]` by phase number, for assertions that need a specific record.
pub fn phase(sc: &serde_json::Value, n: i64) -> &serde_json::Value {
    sc["phases"]
        .as_array()
        .expect("phases")
        .iter()
        .find(|p| p["phase"].as_i64() == Some(n))
        .unwrap_or_else(|| panic!("no phase {n} in {}", sc["phases"]))
}

/// Convenience for tests that want the whole phase table as a map.
pub fn phase_outcomes(sc: &serde_json::Value) -> BTreeMap<i64, String> {
    sc["phases"]
        .as_array()
        .expect("phases")
        .iter()
        .map(|p| {
            (
                p["phase"].as_i64().unwrap(),
                p["outcome"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The rendered restore document, from a REAL run.
// ---------------------------------------------------------------------------

/// The `restore.yaml` `render_restore::render` produced during a real drill,
/// read back off disk. Rendered once and reused: it costs a full drill.
///
/// Used by the two engine-readback assertions Task 3's addendum deferred to
/// this task (progress.md, "Deferred items raised by the addenda pass"). Both
/// need a document the REAL engine will accept, and hand-writing one here would
/// only prove the harness can write engine config.
pub fn rendered_restore_yaml() -> String {
    static DOC: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    DOC.get_or_init(|| {
        let r = drill_run(&spec_default());
        assert_eq!(
            r.out.status.code(),
            Some(0),
            "the drill that renders the document must itself succeed: {}",
            r.out.stderr_utf8()
        );
        let mut newest: Option<(std::time::SystemTime, PathBuf)> = None;
        for e in std::fs::read_dir(engine_mount()).unwrap().flatten() {
            let f = e.path().join("restore.yaml");
            if !f.is_file() {
                continue;
            }
            let m = f.metadata().unwrap().modified().unwrap();
            if newest.as_ref().is_none_or(|(t, _)| m > *t) {
                newest = Some((m, f));
            }
        }
        let (_, path) = newest.expect("a rendered restore.yaml under the engine mount");
        let doc = std::fs::read_to_string(&path).unwrap();
        assert!(
            doc.contains("backup_id: \"drill-demo\""),
            "the newest rendered document is not this drill\'s:\n{doc}"
        );
        doc
    })
    .clone()
}

/// Runs the engine directly on a config this harness wrote, and hands back
/// (exit code, stdout, stderr). Global Constraint 3 binds the `logweir` BINARY,
/// not this harness; `validate-restore` is in the permitted set regardless.
pub fn engine_validate_restore(doc: &str) -> (Option<i32>, String, String) {
    let dir = engine_mount().join("readback");
    std::fs::create_dir_all(&dir).unwrap();
    let cfg = dir.join("restore.yaml");
    std::fs::write(&cfg, doc).unwrap();
    let out = Command::new(engine_bin())
        .args(["validate-restore", "--config"])
        .arg(&cfg)
        .args(["--format", "json"])
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .env("AWS_REGION", "us-east-1")
        .env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount())
        .output()
        .expect("engine validate-restore");
    (out.status.code(), out.stdout_utf8(), out.stderr_utf8())
}
