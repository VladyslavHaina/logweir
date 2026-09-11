#![cfg(feature = "e2e")]
#![allow(dead_code)]
//! Shared by every `e2e/tests/*.rs`. Host-side, so the default bootstrap is
//! `localhost:9092` (the EXTERNAL listener); container-side is
//! `kafka-broker-1:9094`.
//!
//! # The broker has FIVE listeners from Task 7 onward (STANDING RULE 15)
//!
//! | listener | address | protocol | who reaches it |
//! |---|---|---|---|
//! | `PLAINTEXT` | `kafka-broker-1:9094` | PLAINTEXT | inter-broker, and every in-network setup step |
//! | `EXTERNAL` | `localhost:9092` | PLAINTEXT | this harness, host-side (`BOOTSTRAP`) |
//! | `CONTROLLER` | `kafka-broker-1:9093` | PLAINTEXT | the KRaft quorum |
//! | `SASL` | `kafka-broker-1:9096` | SASL_PLAINTEXT | in-network SCRAM (`BOOTSTRAP_SASL_INNET`) |
//! | `SASLEXT` | `localhost:9097` | SASL_PLAINTEXT | host-side SCRAM (`BOOTSTRAP_SASL`) |
//! | `K8S` | `host.docker.internal:9095` | PLAINTEXT | a POD on docker-desktop (`BOOTSTRAP_K8S`) |
//!
//! `SASL` and `SASLEXT` are ONE credential store advertised twice, because a
//! host-side client cannot resolve `kafka-broker-1` and a pod cannot use
//! `localhost:9092` — the broker's metadata redirects every client to the
//! advertised name whatever address it bootstrapped against.
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
/// The `SASLEXT` listener — SASL_PLAINTEXT, published on 9097 and advertised as
/// `localhost:9097`. **The host-side SCRAM bootstrap**, and also the one the
/// containerised engine uses: `e2e/fixtures/engine-docker.sh` rewrites
/// `localhost` to the Docker host gateway inside the engine container, so a
/// `localhost:9097` bootstrap resolves to this published port from both sides
/// of that boundary. The native (linux/amd64) engine route runs on the host
/// and resolves it directly.
pub const BOOTSTRAP_SASL: &str = "localhost:9097";
/// The `SASL` listener — the SAME SCRAM credential store as `BOOTSTRAP_SASL`,
/// advertised for clients ON `kafka-net`. Unresolvable from the host by
/// design; used by the `kafka-backup` compose service, which is the only
/// engine invocation in this repository that really runs inside the compose
/// network.
pub const BOOTSTRAP_SASL_INNET: &str = "kafka-broker-1:9096";
/// The `K8S` listener — PLAINTEXT, published on 9095 and advertised as
/// `${LOGWEIR_K8S_ADVERTISED_HOST:-host.docker.internal}:9095`. **The bootstrap
/// a POD uses**, and the literal value of `KafkaCluster.spec.bootstrapServers`
/// in Demo 1. `host.docker.internal` resolution inside a pod is a Docker
/// Desktop behaviour and not a Kubernetes one, which is why Task 7 probes it
/// with a real pod rather than with a host-side port check.
pub const BOOTSTRAP_K8S: &str = "host.docker.internal:9095";
/// The SCRAM principal `scram-setup` creates, and the `sasl_username` every
/// SCRAM spec in this suite names (**G-ID**: the plan binds the principal).
pub const SCRAM_USER: &str = "logweir";
/// **A FIXTURE CONSTANT, NOT KEY MATERIAL.** The same literal appears in
/// `e2e/compose/docker-compose.yml`'s `scram-setup` command and in
/// `e2e/compose/config/backup-scram.yaml`'s documented expansion: it
/// authenticates to one throwaway compose broker and to nothing else. It is
/// projected into a child process's environment, never written into a spec, a
/// plan, a rendered document or a receipt — and no signing key is ever
/// checked in anywhere.
pub const SCRAM_PASSWORD: &str = "logweir-e2e-not-a-secret";
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
/// USE **`just e2e`**, NOT `cargo test -p e2e --features e2e`. Two separate
/// reasons, and both bite:
///
/// 1. The `e2e` package depends on the `logweir` LIBRARY, so a `-p e2e` run
///    does not rebuild the BINARY — it will happily test whatever
///    `target/debug/logweir` was left over from an earlier build. Measured
///    while mutation-testing this suite: a mutant that mapped
///    `DrillError::NotPass` to exit 0 SURVIVED a `-p e2e` run and was killed
///    immediately once the workspace was rebuilt.
/// 2. A bare `cargo test --workspace --features e2e` also **fails**: this
///    suite drives ONE compose stack, ONE broker and ONE bucket, so its tests
///    collide when run in parallel. `just e2e` supplies `--test-threads=1`,
///    which is what makes it pass; `ci.yml` and `engine-matrix.yml` pass the
///    same flag. If you must spell it out:
///    `cargo test --workspace --features e2e -- --test-threads=1`.
pub fn bin() -> PathBuf {
    let p = root().join("target/debug/logweir");
    assert!(
        p.exists(),
        "{} is missing; run `just e2e`, which builds it",
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

/// `docker compose exec -T kafka-broker-1 …` against the RUNNING broker.
///
/// `exec` and not `run`: the question every caller of this asks is about the
/// configuration the live broker was started with — `/opt/kafka/config/
/// server.properties`, which `kafka.docker.KafkaDockerWrapper` writes from the
/// container's environment at launch. A fresh `run` container would rewrite
/// that file from the same environment and prove nothing about the process
/// that is actually serving.
///
/// The `Output`'s status is handed back untouched so a caller can read the
/// exit code DIRECTLY (STANDING RULE 20); nothing here pipes it.
pub fn compose_exec_broker(args: &[&str]) -> Output {
    let mut c = Command::new("docker");
    c.args([
        "compose",
        "-f",
        "e2e/compose/docker-compose.yml",
        "exec",
        "-T",
        "kafka-broker-1",
    ]);
    c.args(args);
    c.current_dir(root()).output().expect("docker compose exec")
}

/// The broker's live `server.properties`, read off the running container.
pub fn broker_server_properties() -> String {
    let o = compose_exec_broker(&["cat", "/opt/kafka/config/server.properties"]);
    assert!(
        o.status.success(),
        "could not read the broker's server.properties — is the stack up? \
         run `just e2e-up`\n{}",
        o.stderr_utf8()
    );
    o.stdout_utf8()
}

/// The pinned engine, run as the `kafka-backup` compose service — i.e. **on
/// `kafka-net`**, which is the one engine invocation in this repository that
/// can use `BOOTSTRAP_SASL_INNET`.
///
/// `e2e/fixtures/engine-docker.sh` (the route `engine_bin()` picks on this
/// arm64 host) deliberately does NOT join `kafka-net`: it rewrites `localhost`
/// to the Docker host gateway so the rendered document's `localhost:9092` /
/// `http://localhost:9000` mean the same thing to the engine as they do to
/// `logweir`. That makes it the right route for everything Logweir renders and
/// the wrong one for an in-network advertised name, so the in-network SCRAM
/// arm goes through this service and its bind-mounted `./config` instead.
///
/// `envs` are passed with `-e NAME=value`, which is how the engine's own
/// `expand_env_vars` gets a value for a `${…}` placeholder inside the
/// container.
pub fn compose_engine_innet(envs: &[(&str, &str)], args: &[&str]) -> Output {
    let mut c = Command::new("docker");
    c.args([
        "compose",
        "-f",
        "e2e/compose/docker-compose.yml",
        "run",
        "--rm",
        "-T",
    ]);
    for (k, v) in envs {
        c.args(["-e", &format!("{k}={v}")]);
    }
    c.arg("kafka-backup");
    c.args(args);
    c.current_dir(root()).output().expect("docker compose run")
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

/// Creates one topic and waits for the broker's metadata to agree, so a row
/// can set up a target state the drill is supposed to REFUSE (spec §6.1: a
/// `Restore` refuses if any mapped target topic already exists). Deletion is
/// `delete_all_drill_topics`'s job, which every `run_with` does first.
pub fn create_topic(topic: &str, partitions: i32) {
    ok(
        kafka_topics(&[
            "--bootstrap-server",
            "kafka-broker-1:9094",
            "--create",
            "--if-not-exists",
            "--partitions",
            &partitions.to_string(),
            "--replication-factor",
            "1",
            "--topic",
            topic,
        ]),
        "create topic",
    );
    for _ in 0..60 {
        if topic_exists(topic) {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    panic!("topic {topic} still absent 15s after --create");
}

/// `create_topic` with topic-level config entries — the only way to seed a
/// fixture whose record timestamps are the fixture's own.
///
/// **Why it has to exist.** `message.timestamp.type` is a topic config, and on
/// `LogAppendTime` the broker REPLACES every timestamp a producer states with
/// its own wall clock — which is the exact failure `render_restore.rs:112-114`
/// describes for a restore target and the exact failure a point-in-time
/// FIXTURE has to avoid. Task 8 closed residual 3 by execution on this very
/// broker: Apache Kafka 3.7.1 (KRaft, node 1001) on
/// `log.message.timestamp.type=LogAppendTime` HONOURS a per-topic
/// `message.timestamp.type=CreateTime` override. So the override is stated
/// here rather than inherited: this stack's broker sets no
/// `KAFKA_LOG_MESSAGE_TIMESTAMP_TYPE` and therefore runs Kafka's own
/// `CreateTime` default today, and a fixture that depended on that default
/// would break silently the day the compose file gained the variable.
pub fn create_topic_with_configs(topic: &str, partitions: i32, configs: &[(&str, &str)]) {
    let mut args: Vec<String> = [
        "--bootstrap-server",
        "kafka-broker-1:9094",
        "--create",
        "--if-not-exists",
        "--partitions",
        &partitions.to_string(),
        "--replication-factor",
        "1",
        "--topic",
        topic,
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    for (k, v) in configs {
        args.push("--config".into());
        args.push(format!("{k}={v}"));
    }
    let borrowed: Vec<&str> = args.iter().map(String::as_str).collect();
    ok(kafka_topics(&borrowed), "create topic with configs");
    for _ in 0..60 {
        if topic_exists(topic) {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    panic!("topic {topic} still absent 15s after --create");
}

/// The total number of records the broker holds for `topic`, over every
/// partition — the sum of its end offsets. Reported the same way
/// `phase7_verify::check_restored_count` counts a restored topic, so a fixture
/// that says "the broker has n records" means it in the same units the product
/// does.
pub fn record_count_on_broker(topic: &str) -> Result<i64, String> {
    let r = reader();
    let ends =
        ClusterReader::end_offsets(&r, topic).map_err(|e| format!("end_offsets({topic}): {e}"))?;
    Ok(ends.iter().map(|(_, hi)| (*hi).max(0)).sum())
}

/// **Task 11 — the G-PITR fixture's only writer.** Produces one record per
/// `(timestamp_ms, payload)` with that EXACT `CreateTime`, and returns only
/// once the broker's end offsets have advanced by `records.len()`.
///
/// # Why this is not `kafka-console-producer`
///
/// `scripts/e2e-seed.sh` produces through `kafka-console-producer`, which has
/// no timestamp property of any kind: every record it writes is stamped with
/// the moment it was written, so the seeded archive's whole restore window is
/// seconds wide (measured 4,337 ms and 6,728 ms on two seeds). A boundary test
/// cannot be built on that — `point_in_time ± 1 ms` is not addressable when
/// the data spans 4 seconds of whenever-the-suite-ran, and a fixture bound to
/// `Utc::now()` asserts a different thing on every run. So the fixture states
/// its instants and this function writes them; `rdkafka`'s `BaseRecord`
/// carries a timestamp and is the only writer in the tree that does.
///
/// # Placement is by ORDER, round-robin over the topic's partitions
///
/// `records[i]` goes to partition `i % <the topic's partition count>`, read off
/// the broker rather than assumed. There is deliberately no partition argument
/// — the signature is the one interface **I** of Task 11's brief fixes
/// verbatim — so a caller that wants a specific record on a specific partition
/// controls it by the order it lists them in, which is what
/// `e2e/tests/pitr_boundary.rs` does (nine records, three timestamps × three
/// partitions, listed timestamp-major).
///
/// # It returns only when the BROKER agrees, and a timeout is an `Err`
///
/// `flush` proves librdkafka's queue drained; it does not prove the broker
/// accepted anything. With the default (no-op) delivery callback a rejected
/// record is dropped silently, so the end-offset read is what makes this
/// function fail closed: a bounded foreground poll (40 × 250 ms), never a bare
/// sleep, and on timeout an `Err(String)` naming the topic and the offsets
/// seen. A fixture that produced nothing and reported success would make the
/// whole restore downstream of it a test of an empty window.
pub fn produce_with_timestamps(topic: &str, records: &[(i64, &str)]) -> Result<(), String> {
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

    if records.is_empty() {
        return Err(format!(
            "produce_with_timestamps({topic}) was asked for zero records; a fixture that \
             produces nothing would make every assertion downstream of it vacuous"
        ));
    }
    let before = record_count_on_broker(topic)?;
    let partitions = ClusterReader::list_topics(&reader())
        .map_err(|e| format!("list_topics: {e}"))?
        .into_iter()
        .find(|t| t.name == topic)
        .map(|t| t.partitions)
        .ok_or_else(|| format!("topic {topic} does not exist; create it before producing"))?;
    if partitions <= 0 {
        return Err(format!("topic {topic} reports {partitions} partitions"));
    }

    let producer: BaseProducer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAP)
        .set("message.timeout.ms", "10000")
        // `acks=all` is librdkafka's default and is stated anyway: a fixture
        // whose records are only on the leader's page cache is a fixture whose
        // end offsets can move after this function returns.
        .set("acks", "all")
        .create()
        .map_err(|e| format!("producer for {topic} at {BOOTSTRAP}: {e}"))?;

    for (i, (ts, payload)) in records.iter().enumerate() {
        let partition = (i % partitions as usize) as i32;
        let key = format!("{topic}-{i:03}");
        producer
            .send(
                BaseRecord::to(topic)
                    .partition(partition)
                    .key(&key)
                    .payload(*payload)
                    .timestamp(*ts),
            )
            .map_err(|(e, _)| {
                format!("enqueue {topic}/{partition} ts={ts} payload={payload:?}: {e}")
            })?;
    }
    producer
        .flush(std::time::Duration::from_secs(10))
        .map_err(|e| format!("flush {topic}: {e}"))?;

    let want = before + records.len() as i64;
    let mut seen = before;
    for _ in 0..40 {
        seen = record_count_on_broker(topic)?;
        if seen >= want {
            eprintln!(
                "[e2e] produced {} record(s) into {topic} across {partitions} partition(s) \
                 with explicit CreateTime; end offsets {before} -> {seen}",
                records.len()
            );
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    Err(format!(
        "produce_with_timestamps({topic}): the broker's end offsets are {seen} after 10s, \
         expected {want} ({before} before + {} produced); the records were enqueued and \
         flushed, so the broker rejected some of them",
        records.len()
    ))
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
        // Present on every run that produced a drill result. It used to be the
        // ONLY stdout Logweir emitted with RUST_LOG unset, because its JSON
        // subscriber was built with `EnvFilter::from_default_env()`, whose
        // default level is ERROR. That is no longer true (T0-10): the default
        // is now `info`, so the JSON lines below are emitted at the shipped
        // default too, and an operational failure that produces no summary line
        // still leaves a `run_id` for the last branch to find. This branch is
        // kept because it is the cheaper and more direct read on the runs that
        // do produce a drill result.
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
    /// Topics to create AFTER `run_with`'s `delete_all_drill_topics()` and
    /// before the binary runs, as `(name, partitions)`.
    ///
    /// Empty for every row but the one that proves spec §6.1's refusal, and it
    /// has to be here rather than in the row itself: `run_with` empties the
    /// `drill-` namespace unconditionally, so a topic the row created before
    /// calling it would be deleted again before the drill ever saw it.
    pub pre_create: Vec<(String, i32)>,
    /// Extra environment for the `logweir` child, on top of the fixed set
    /// `run_with` always sets.
    ///
    /// Task 7 needs exactly one entry — `LOGWEIR_TARGET_PASSWORD` — for the
    /// SCRAM drill rows. It is a field rather than a `std::env::set_var` in the
    /// row because `set_var` is process-wide and this suite's rows share one
    /// process; a leaked `LOGWEIR_TARGET_PASSWORD` would make every LATER
    /// plaintext drill carry a projected credential it never asked for, which
    /// is the shape of a false green rather than a false red.
    pub env: Vec<(String, String)>,
    /// Spawn `logweir restore run` instead of `logweir drill run` (Task 11).
    ///
    /// The two take ONE clap struct (`cli::RestoreRunArgs`, flattened into both
    /// subcommands) so the flag list below cannot differ between them, and
    /// `crates/logweir/tests/restore_mode.rs::cli_drill_run_is_an_alias_for_restore_run`
    /// pins that in process. What only a process can show is that the
    /// CANONICAL name really runs the whole thing end to end: every e2e row
    /// before Task 11 drove the tag-0 alias, so `restore run` had no live
    /// coverage at all. `false` keeps every existing caller on `drill run`
    /// byte for byte.
    pub restore_run: bool,
}

impl<'a> RunOpts<'a> {
    pub fn new(spec: &'a serde_yaml::Value) -> Self {
        Self {
            spec,
            same_key: false,
            signing: None,
            allowlist: None,
            approval: Approval::Valid,
            pre_create: Vec::new(),
            env: Vec::new(),
            restore_run: false,
        }
    }
}

pub fn run_with(o: RunOpts<'_>) -> Run {
    // Every drill in this suite starts from an EMPTY scratch namespace. Without
    // this a second restore would append to the first run's topics and phase 7
    // would reconcile against doubled offsets — a red for a reason that has
    // nothing to do with the thing under test.
    delete_all_drill_topics();
    // …and then, for the one row that needs it, puts a target topic back.
    for (topic, partitions) in &o.pre_create {
        create_topic(topic, *partitions);
    }

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

    let mut cmd = Command::new(bin());
    // `restore run` is the canonical name (interface I20); `drill run` is the
    // tag-0 alias, which prints one deprecation line and does nothing else
    // differently. Both flatten the SAME clap struct, so the flags below are
    // one list either way.
    let verb = if o.restore_run { "restore" } else { "drill" };
    cmd.args([verb, "run", "--spec"])
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
        .env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount());
    // Per-row additions, LAST, so a row can override nothing above by
    // accident and everything above it deliberately. Task 7's SCRAM drill
    // rows use this for `LOGWEIR_TARGET_PASSWORD`; every other row passes an
    // empty list and gets exactly the environment it always got.
    for (k, v) in &o.env {
        cmd.env(k, v);
    }
    let out = cmd.output().unwrap();

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

/// The interpreter that can run `docs/verify_scorecard.py`.
///
/// FOUR places in this repository resolve it and they must agree, or a
/// developer who sets one variable gets one interpreter in the e2e harness and
/// a DIFFERENT one in the parity gates — which is a two-reader claim checked
/// against two different second readers. The order is
/// `$LOGWEIR_PYTHON`, `$LOGWEIR_E2E_PYTHON`, `.e2e/venv/bin/python3`,
/// `python3`, and the other three are
/// `scripts/check-verifier-parity.sh`, `scripts/check-invariant-corpus.sh`
/// and `crates/logweir/tests/two_reader_parity.rs::python`.
///
/// This function used to read `LOGWEIR_E2E_PYTHON` ONLY, so it was the outlier
/// of the four: `$LOGWEIR_PYTHON` is the name the README and `scripts/demo.sh`
/// document, and setting it moved every gate except this one.
/// `python()` for the e2e rows that need the auditor's interpreter directly
/// (Task 5b's receipt row runs `docs/verify_scorecard.py` over a receipt the
/// runner just signed).
///
/// A WRAPPER and not a `pub` on `python` itself: the line `fn python() ->
/// PathBuf {` is matched verbatim by
/// `crates/logweir/tests/two_reader_parity.rs::
/// every_gate_resolves_the_auditors_interpreter_the_same_way`, which slices
/// this resolver's body between that exact line and the next `}` at column 0.
/// Renaming it would make that gate stop looking at the chain below.
pub fn auditor_python() -> PathBuf {
    python()
}

fn python() -> PathBuf {
    for var in ["LOGWEIR_PYTHON", "LOGWEIR_E2E_PYTHON"] {
        if let Ok(p) = std::env::var(var) {
            if !p.is_empty() {
                return PathBuf::from(p);
            }
        }
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
/// Global Constraint 3 — **as REVISED by Task 1, and it is FOUR commands, not
/// three** — binds the `logweir` BINARY to
/// {`backup`, `restore`, `validate-restore`, `validation run`}. This comment
/// read `{restore, validate-restore, validation run}` while the gate it
/// describes, `scripts/check-no-oso.sh:96`, already spelled
/// `ENGINE_RUNTIME_ALLOWLIST="backup restore validate-restore validation"` —
/// Task 1's carry F1, fixed here by Task 7, the next task to edit this file.
/// `validation evidence-verify` is NOT in that set; it is spec §14 SP1c's exit
/// criterion, it is run by this HARNESS and never by the binary, and it is
/// explicitly permitted in this file. The directory is under `engine_mount()`
/// so the containerised engine route can see it.
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
