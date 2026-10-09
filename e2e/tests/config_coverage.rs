#![cfg(feature = "e2e")]
//! **FX-4 — topic-configuration capture coverage, and the denied-read class,
//! observed live.**
//!
//! Four rows, each `#[ignore]`d because each needs the stack's `acl` profile
//! (`e2e/README.md`): `kafka-acl` runs KRaft's StandardAuthorizer, every
//! PLAINTEXT client on it is User:ANONYMOUS and a super user, and the SCRAM
//! user `logweir` is the restricted principal whose DescribeConfigs the rows
//! deny with ACLs.
//!
//! | row | proves |
//! |---|---|
//! | `a_denied_describe_configs_is_a_refusal_at_every_reader` | T13 at the reader: rdkafka 0.36.2 answers a refused topic or broker with ZERO entries and no error; `RdKafkaReader` and `KafkaInventory` now return the refusal; the restore readiness row `target.timestampBound` goes from READY (the pre-fix flattening of that live answer) to UNKNOWN |
//! | `phase_0_never_assumes_create_time_for_a_refused_broker_read` | T13 at phase 0, by PROCESS: on a `LogAppendTime` broker, a restore identity without cluster DescribeConfigs is admitted as `CreateTime` by the pre-FX-4 binary (`FX4_BEFORE_BIN`) and stopped at phase 0 by this build |
//! | `capture_coverage_reaches_the_receipt_the_catalog_and_drill_parity` | FX-4 itself: a DENIED DescribeConfigs is `captureDenied` (and empties its neighbour's manifest record: `notCaptured`); overrides and a broker-default and a topic-override `LogAppendTime` are `captured` with their timestamp type and source; the catalog point copies them; a point-bound restore's parity is `notAssessed` exactly where the capture was not; a restore identity that may not DescribeConfigs its TARGET topics gets `targetReadDenied` for a topic whose backup recorded no overrides, and exit 1 in phase 6 with no scorecard for one that did (the pinned engine describes such a target itself); every not-assessed topic also leaves its fail-safe entry in `unexpected_divergence` (review M5), and the signed scorecards are kept for the old-reader check |
//! | `fx8_a_broker_default_log_append_time_is_refused_from_the_bound_receipt` | FX-8's broker-default arm: a topic with no override, backed up under a dynamic broker default of `LogAppendTime`, is recorded only by the receipt; a point-in-time restore bound to that receipt is refused (`PointInTimeByProducerTime`, no target topic), runs labelled `producer_time` with `restore.time_basis: producerTime`, and unbound runs labelled `not_recorded` |
//!
//! # Running them
//!
//! ```text
//! eval "$(e2e/compose/stack-env.sh --slot N --profiles acl)"
//! just e2e-up
//! cargo build -p logweir
//! FX4_BEFORE_BIN=<a logweir binary built before FX-4> AWS_EC2_METADATA_DISABLED=true \
//!   cargo test -p e2e --features e2e --test config_coverage -- --ignored --test-threads=1 --nocapture
//! just e2e-down
//! ```
//!
//! Each row writes what it observed to `config-coverage/<row>.json` under the
//! stack's scratch directory (`harness::demo_dir()`: `.e2e/` on the default
//! stack, `.e2e/<project>/` on a slot).
//!
//! # Every stack address is `stack()`'s
//!
//! Every broker address, S3 endpoint, credential and the compose project this
//! file uses comes from [`stack`], which reads the per-stack harness
//! (`harness::bootstrap_acl()`, `bootstrap_acl_sasl()`, `s3_endpoint()`,
//! `stack::project()`) after `stack::ensure_coherent()`, and nothing else
//! names one.
mod harness;

use harness::{bin, demo_dir, engine_bin, engine_digest, engine_mount, engine_version, root};
use logweir_core::backup_receipt::BackupReceipt;
use logweir_kafka::reader::{AuthConfig, ClusterReader, KafkaError};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ============================================================ the stack

/// Every address and credential of the compose stack this file uses.
struct Stack {
    /// The compose project the running stack belongs to.
    compose_project: String,
    /// The compose file, relative to the repository root.
    compose_file: String,
    /// The profile the broker below belongs to (`acl`).
    profile: String,
    /// The ACL-enforcing broker's service name inside the project.
    broker_service: String,
    /// Its KRaft node id: the broker resource DescribeConfigs names.
    broker_node_id: i32,
    /// PLAINTEXT on the host: User:ANONYMOUS, a super user.
    plaintext: String,
    /// SASL/SCRAM-SHA-512 on the host: User:`scram_user`, NOT a super user.
    sasl: String,
    /// PLAINTEXT inside `kafka-net`, for the broker's own CLIs run through
    /// `docker compose exec`.
    in_network: String,
    scram_user: String,
    scram_password: String,
    s3_endpoint: String,
    s3_user: String,
    s3_secret: String,
    archive_bucket: String,
    evidence_bucket: String,
}

/// THE ONE PLACE this file reads the stack's addresses (see the module doc).
fn stack() -> Stack {
    harness::stack::ensure_coherent();
    let minio = ["minio", "admin"].concat();
    Stack {
        compose_project: harness::stack::project(),
        compose_file: "e2e/compose/docker-compose.yml".into(),
        profile: "acl".into(),
        broker_service: "kafka-acl".into(),
        broker_node_id: 4001,
        plaintext: harness::bootstrap_acl(),
        sasl: harness::bootstrap_acl_sasl(),
        in_network: "kafka-acl:9094".into(),
        scram_user: harness::SCRAM_USER.into(),
        scram_password: harness::SCRAM_PASSWORD.into(),
        s3_endpoint: harness::s3_endpoint(),
        s3_user: minio.clone(),
        s3_secret: minio,
        archive_bucket: harness::ARCHIVE_BUCKET.into(),
        evidence_bucket: harness::EVIDENCE_BUCKET.into(),
    }
}

/// Point this process's `object_store` clients at the stack's MinIO.
fn use_stack_s3_env() {
    let s = stack();
    std::env::set_var("AWS_ACCESS_KEY_ID", s.s3_user);
    std::env::set_var("AWS_SECRET_ACCESS_KEY", s.s3_secret);
    std::env::set_var("AWS_REGION", "us-east-1");
}

// ============================================================ processes

/// Run `cmd` to completion or kill it after `secs`, reading both pipes
/// concurrently so a chatty child cannot deadlock on a full pipe.
fn output_within(mut cmd: Command, secs: u64) -> Output {
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().unwrap_or_else(|e| panic!("spawn {cmd:?}: {e}"));
    let mut so = child.stdout.take().expect("piped stdout");
    let mut se = child.stderr.take().expect("piped stderr");
    let t_out = std::thread::spawn(move || {
        let mut b = Vec::new();
        let _ = so.read_to_end(&mut b);
        b
    });
    let t_err = std::thread::spawn(move || {
        let mut b = Vec::new();
        let _ = se.read_to_end(&mut b);
        b
    });
    let deadline = Instant::now() + Duration::from_secs(secs);
    let status = loop {
        match child.try_wait() {
            Ok(Some(s)) => break s,
            Ok(None) if Instant::now() > deadline => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("{cmd:?}: killed after {secs} s");
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(200)),
            Err(e) => panic!("{cmd:?}: {e}"),
        }
    };
    Output {
        status,
        stdout: t_out.join().unwrap_or_default(),
        stderr: t_err.join().unwrap_or_default(),
    }
}

fn text(o: &Output) -> String {
    format!(
        "{}\n{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    )
}

/// One of the broker's own CLIs, inside the RUNNING `kafka-acl` container, as
/// User:ANONYMOUS (a super user). `exec`, never `run`.
fn broker_cli(args: &[&str]) -> Output {
    let s = stack();
    let mut c = Command::new("docker");
    c.args([
        "compose",
        "-p",
        &s.compose_project,
        "-f",
        &s.compose_file,
        "--profile",
        &s.profile,
        "exec",
        "-T",
        &s.broker_service,
    ])
    .args(args)
    .current_dir(root());
    output_within(c, 120)
}

fn broker_cli_ok(args: &[&str], what: &str) -> String {
    let o = broker_cli(args);
    assert!(o.status.success(), "{what} failed:\n{}", text(&o));
    String::from_utf8_lossy(&o.stdout).into_owned()
}

// ============================================================ the broker

fn nonce() -> String {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970")
        .as_nanos();
    format!("{:010}", n % 10_000_000_000)
}

/// The broker must be running the StandardAuthorizer, or every row below
/// would prove nothing (CreateAcls answers SECURITY_DISABLED without it).
fn assert_the_authorizer_is_on() {
    let props = broker_cli_ok(
        &["cat", "/opt/kafka/config/server.properties"],
        "read the broker's server.properties",
    );
    assert!(
        props.contains(
            "authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer"
        ),
        "kafka-acl is not running the StandardAuthorizer: start the stack with the \
         `acl` profile (this file's module doc)"
    );
}

fn create_topic(topic: &str, configs: &[(&str, &str)]) {
    let s = stack();
    let mut args: Vec<String> = [
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        &s.in_network,
        "--create",
        "--topic",
        topic,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
    ]
    .iter()
    .map(|x| x.to_string())
    .collect();
    for (k, v) in configs {
        args.push("--config".into());
        args.push(format!("{k}={v}"));
    }
    let borrowed: Vec<&str> = args.iter().map(String::as_str).collect();
    broker_cli_ok(&borrowed, &format!("create topic {topic}"));
    // FX-18: rows read the new topic's configuration at once (the fx4 control
    // must answer non-empty); a broker that does not hold it yet answers empty.
    harness::await_created_on(&s.plaintext, topic, 1);
}

fn delete_topic(topic: &str) {
    let s = stack();
    let _ = broker_cli(&[
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        &s.in_network,
        "--delete",
        "--topic",
        topic,
    ]);
}

fn acl(op: &str, args: &[&str]) -> String {
    let s = stack();
    let mut all = vec![
        "/opt/kafka/bin/kafka-acls.sh",
        "--bootstrap-server",
        &s.in_network,
        op,
    ];
    if op == "--remove" {
        all.push("--force");
    }
    all.extend_from_slice(args);
    broker_cli_ok(&all, &format!("kafka-acls {op} {args:?}"))
}

/// The restricted principal may Read and Describe `topic` — and, because the
/// topic now HAS an ACL, nothing else: not DescribeConfigs.
fn deny_describe_configs_on(topic: &str) -> String {
    let who = format!("User:{}", stack().scram_user);
    acl(
        "--add",
        &[
            "--allow-principal",
            &who,
            "--operation",
            "Read",
            "--operation",
            "Describe",
            "--topic",
            topic,
        ],
    )
}

/// ONE cluster ACL for another principal: from now on the cluster resource has
/// ACLs, so the restricted principal (not a super user) may not DescribeConfigs
/// the broker. Topic resources without ACLs stay open to it.
fn deny_cluster_describe_configs() -> String {
    acl(
        "--add",
        &[
            "--allow-principal",
            "User:fx4-ops",
            "--operation",
            "Describe",
            "--cluster",
        ],
    )
}

fn undo_deny_cluster_describe_configs() {
    acl(
        "--remove",
        &[
            "--allow-principal",
            "User:fx4-ops",
            "--operation",
            "Describe",
            "--cluster",
        ],
    );
}

/// The cluster-wide DYNAMIC default `log.message.timestamp.type` — the
/// broker-default arm of FX-8 (`docs/stability.md`'s Task 8 measurement set it
/// the same way).
fn set_broker_default_timestamp_type(value: Option<&str>) -> String {
    let s = stack();
    let mut args = vec![
        "/opt/kafka/bin/kafka-configs.sh",
        "--bootstrap-server",
        &s.in_network,
        "--alter",
        "--entity-type",
        "brokers",
        "--entity-default",
    ];
    let add;
    match value {
        Some(v) => {
            add = format!("log.message.timestamp.type={v}");
            args.extend_from_slice(&["--add-config", &add]);
        }
        None => args.extend_from_slice(&["--delete-config", "log.message.timestamp.type"]),
    }
    broker_cli_ok(&args, "alter the broker default timestamp type");
    broker_cli_ok(
        &[
            "/opt/kafka/bin/kafka-configs.sh",
            "--bootstrap-server",
            &s.in_network,
            "--describe",
            "--entity-type",
            "brokers",
            "--entity-default",
        ],
        "describe the broker defaults",
    )
}

/// Undoes this file's cluster-level changes on EVERY exit path, a panicking
/// assertion included, so a red row cannot poison the next row or the stack's
/// next user.
struct ClusterGuard {
    cluster_acl: bool,
    broker_default: bool,
    topic_acls: Vec<String>,
    /// Prefixes [`allow_restore_without_describe_configs`] granted.
    prefixed_acls: Vec<String>,
    topics: Vec<String>,
}

impl ClusterGuard {
    fn new() -> Self {
        Self {
            cluster_acl: false,
            broker_default: false,
            topic_acls: Vec::new(),
            prefixed_acls: Vec::new(),
            topics: Vec::new(),
        }
    }
}

impl Drop for ClusterGuard {
    fn drop(&mut self) {
        let s = stack();
        if self.cluster_acl {
            let _ = broker_cli(&[
                "/opt/kafka/bin/kafka-acls.sh",
                "--bootstrap-server",
                &s.in_network,
                "--remove",
                "--force",
                "--allow-principal",
                "User:fx4-ops",
                "--operation",
                "Describe",
                "--cluster",
            ]);
        }
        if self.broker_default {
            let _ = broker_cli(&[
                "/opt/kafka/bin/kafka-configs.sh",
                "--bootstrap-server",
                &s.in_network,
                "--alter",
                "--entity-type",
                "brokers",
                "--entity-default",
                "--delete-config",
                "log.message.timestamp.type",
            ]);
        }
        let who = format!("User:{}", s.scram_user);
        for topic in &self.topic_acls {
            let _ = broker_cli(&[
                "/opt/kafka/bin/kafka-acls.sh",
                "--bootstrap-server",
                &s.in_network,
                "--remove",
                "--force",
                "--allow-principal",
                &who,
                "--operation",
                "Read",
                "--operation",
                "Describe",
                "--topic",
                topic,
            ]);
        }
        for prefix in &self.prefixed_acls {
            let args = restore_acl_args("--remove", prefix);
            let borrowed: Vec<&str> = args.iter().map(String::as_str).collect();
            let _ = broker_cli(&borrowed);
        }
        for topic in &self.topics {
            delete_topic(topic);
        }
    }
}

/// What a `newTopic` restore does on its target topics — Create, Write, Read,
/// Describe (and Delete, which a scratch teardown needs) — and deliberately
/// NOT DescribeConfigs (FX-4 review M5).
const RESTORE_OPERATIONS: [&str; 5] = ["Create", "Write", "Read", "Describe", "Delete"];

/// `kafka-acls.sh` argv adding or removing [`RESTORE_OPERATIONS`] for the
/// restricted principal on every topic under `prefix`.
fn restore_acl_args(op: &str, prefix: &str) -> Vec<String> {
    let s = stack();
    let mut args: Vec<String> = [
        "/opt/kafka/bin/kafka-acls.sh",
        "--bootstrap-server",
        &s.in_network,
        op,
    ]
    .iter()
    .map(|x| x.to_string())
    .collect();
    if op == "--remove" {
        args.push("--force".into());
    }
    args.extend(["--allow-principal".into(), format!("User:{}", s.scram_user)]);
    for operation in RESTORE_OPERATIONS {
        args.extend(["--operation".into(), operation.into()]);
    }
    args.extend([
        "--topic".into(),
        prefix.into(),
        "--resource-pattern-type".into(),
        "prefixed".into(),
    ]);
    args
}

/// The restricted principal may restore into `prefix…` but may not
/// DescribeConfigs what it created: phase 7 then reads `targetReadDenied`.
fn allow_restore_without_describe_configs(prefix: &str) -> String {
    let args = restore_acl_args("--add", prefix);
    let borrowed: Vec<&str> = args.iter().map(String::as_str).collect();
    broker_cli_ok(&borrowed, &format!("kafka-acls --add prefixed {prefix}"))
}

fn produce(topic: &str, count: usize) {
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    let producer: BaseProducer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", stack().plaintext)
        .set("message.timeout.ms", "10000")
        .set("acks", "all")
        .create()
        .expect("a producer");
    for i in 0..count {
        let payload = format!("{topic}-{i}");
        let key = format!("k{i}");
        loop {
            match producer.send(BaseRecord::to(topic).key(&key).payload(&payload)) {
                Ok(()) => break,
                Err((e, _)) if e.to_string().contains("QueueFull") => {
                    producer.poll(Duration::from_millis(50));
                }
                Err((e, _)) => panic!("produce to {topic}: {e}"),
            }
        }
    }
    producer
        .flush(Duration::from_secs(20))
        .unwrap_or_else(|e| panic!("flush {topic}: {e}"));
}

fn plaintext_reader() -> logweir_kafka::rdkafka_reader::RdKafkaReader {
    logweir_kafka::rdkafka_reader::RdKafkaReader::connect(
        &[stack().plaintext],
        AuthConfig::Plaintext,
    )
    .expect("a PLAINTEXT reader")
}

fn scram_auth() -> AuthConfig {
    let s = stack();
    AuthConfig::ScramSha512 {
        username: s.scram_user,
        password: s.scram_password,
        tls: false,
        tls_ca_file: None,
    }
}

fn scram_reader() -> logweir_kafka::rdkafka_reader::RdKafkaReader {
    logweir_kafka::rdkafka_reader::RdKafkaReader::connect(&[stack().sasl], scram_auth())
        .expect("a SCRAM reader")
}

fn cluster_id() -> String {
    plaintext_reader().cluster_id().expect("the cluster id")
}

/// librdkafka's OWN answer for one resource — the input the pre-FX-4 readers
/// flattened into "no overrides" — as `(entries, per-resource error)`.
/// rdkafka 0.36.2 reports no per-resource error at all (T13), which is what
/// this row records.
fn raw_describe(scram: bool, broker: Option<i32>, topic: Option<&str>) -> Value {
    use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
    use rdkafka::client::DefaultClientContext;
    let s = stack();
    let mut cfg = rdkafka::config::ClientConfig::new();
    if scram {
        cfg.set("bootstrap.servers", &s.sasl)
            .set("security.protocol", "SASL_PLAINTEXT")
            .set("sasl.mechanism", "SCRAM-SHA-512")
            .set("sasl.username", &s.scram_user)
            .set("sasl.password", &s.scram_password);
    } else {
        cfg.set("bootstrap.servers", &s.plaintext);
    }
    let admin: AdminClient<DefaultClientContext> = cfg.create().expect("an admin client");
    let spec = match (broker, topic) {
        (Some(id), _) => ResourceSpecifier::Broker(id),
        (None, Some(t)) => ResourceSpecifier::Topic(t),
        _ => unreachable!(),
    };
    let res = block_on(admin.describe_configs(
        &[spec],
        &AdminOptions::new().request_timeout(Some(Duration::from_secs(20))),
    ))
    .expect("the DescribeConfigs call itself answers");
    let r = res.into_iter().next().expect("one resource");
    match r {
        Ok(c) => json!({
            "ok": true,
            "entries": c.entries.len(),
            "log.message.timestamp.type": c.entries.iter()
                .find(|e| e.name == "log.message.timestamp.type")
                .and_then(|e| e.value.clone()),
        }),
        Err(code) => json!({"ok": false, "code": code.to_string()}),
    }
}

/// The pre-FX-4 flattening, verbatim in effect (`git show ac76cd0d:crates/
/// logweir-kafka/src/inventory.rs`, `describe`: every entry with a value into
/// a map, and nothing about an EMPTY answer): what `KafkaInventory::
/// broker_configs` returned for the answer [`raw_describe`] records.
fn pre_fx4_broker_configs(scram: bool) -> BTreeMap<String, String> {
    use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
    use rdkafka::client::DefaultClientContext;
    let s = stack();
    let mut cfg = rdkafka::config::ClientConfig::new();
    if scram {
        cfg.set("bootstrap.servers", &s.sasl)
            .set("security.protocol", "SASL_PLAINTEXT")
            .set("sasl.mechanism", "SCRAM-SHA-512")
            .set("sasl.username", &s.scram_user)
            .set("sasl.password", &s.scram_password);
    } else {
        cfg.set("bootstrap.servers", &s.plaintext);
    }
    let admin: AdminClient<DefaultClientContext> = cfg.create().expect("an admin client");
    let res = block_on(admin.describe_configs(
        &[ResourceSpecifier::Broker(s.broker_node_id)],
        &AdminOptions::new().request_timeout(Some(Duration::from_secs(20))),
    ))
    .expect("the DescribeConfigs call itself answers");
    let mut out = BTreeMap::new();
    for r in res {
        let cfg = r.expect("rdkafka 0.36.2 never reports a per-resource error");
        for e in cfg.entries {
            if let Some(v) = e.value {
                out.insert(e.name, v);
            }
        }
    }
    out
}

/// Drive one rdkafka admin future to completion on THIS thread, with a hard
/// deadline. std only: rdkafka's admin client polls librdkafka on its own
/// thread and resolves the future through a oneshot channel, so no async
/// runtime is needed — and none is in this crate's dependencies.
fn block_on<F: std::future::Future>(f: F) -> F::Output {
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};
    struct Unpark(std::thread::Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut f = std::pin::pin!(f);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
        assert!(
            Instant::now() < deadline,
            "an admin call did not answer within 60 s"
        );
        std::thread::park_timeout(Duration::from_millis(100));
    }
}

fn kafka_error_kind(e: &KafkaError) -> &'static str {
    match e {
        KafkaError::NotAuthorized(_) => "NotAuthorized",
        KafkaError::TopicNotFound(_) => "TopicNotFound",
        KafkaError::Unreachable(_) => "Unreachable",
        KafkaError::Timeout(_) => "Timeout",
        KafkaError::Client(_) => "Client",
    }
}

// ============================================================ the pipeline

/// This stack's evidence directory for these rows.
fn evidence_dir() -> PathBuf {
    demo_dir().join("config-coverage")
}

fn write_evidence(row: &str, v: &Value) {
    let dir = evidence_dir();
    std::fs::create_dir_all(&dir).expect("the evidence directory");
    let p = dir.join(format!("{row}.json"));
    std::fs::write(&p, serde_json::to_vec_pretty(v).expect("serialises")).expect("written");
    eprintln!("[fx4] evidence: {}", p.display());
}

fn signing_pem() -> PathBuf {
    root().join("e2e/fixtures/signed/signing.pem")
}

fn backup_allowlist() -> PathBuf {
    let p = demo_dir().join("fx4-backup-allowed-clusters.json");
    std::fs::write(
        &p,
        "{\"allowed_cluster_ids\": [\"SCRATCH-CLUSTER-NOT-THE-SOURCE\"]}\n",
    )
    .expect("written");
    p
}

fn restore_allowlist() -> PathBuf {
    let p = demo_dir().join("fx4-restore-allowed-clusters.json");
    std::fs::write(
        &p,
        serde_json::to_vec(
            &json!({"allowed_cluster_ids": [cluster_id()], "source_cluster_id": null}),
        )
        .expect("serialises"),
    )
    .expect("written");
    p
}

fn engine_env(c: &mut Command) {
    let s = stack();
    c.env("AWS_ACCESS_KEY_ID", &s.s3_user)
        .env("AWS_SECRET_ACCESS_KEY", &s.s3_secret)
        .env("AWS_REGION", "us-east-1")
        .env("LOGWEIR_ENGINE_BIN", engine_bin())
        .env("LOGWEIR_ENGINE_VERSION", engine_version())
        .env("LOGWEIR_ENGINE_DIGEST", engine_digest())
        .env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount())
        .env("TMPDIR", engine_mount());
}

/// What one `logweir backup run` printed and signed.
struct Backup {
    backup_id: String,
    out: Output,
    receipt_key: String,
    receipt_bytes: Vec<u8>,
    receipt: BackupReceipt,
    catalog_key: Option<String>,
}

fn backup(backup_id: &str, topics: &[&str], scram: bool) -> Backup {
    let s = stack();
    let (bootstrap, auth) = if scram {
        (
            s.sasl.clone(),
            format!(
                "\x20 auth:\n\x20   mode: scramSha512\n\x20   username: {}\n",
                s.scram_user
            ),
        )
    } else {
        (s.plaintext.clone(), String::new())
    };
    let spec = demo_dir().join(format!("{backup_id}-backup.yaml"));
    std::fs::write(
        &spec,
        format!(
            "backup_id: {backup_id}\n\
             source:\n\
             \x20 bootstrap_servers: [{bootstrap}]\n\
             \x20 topics: [{}]\n\
             {auth}\
             storage:\n\
             \x20 backend: s3\n\
             \x20 bucket: {}\n\
             \x20 prefix: {backup_id}\n\
             \x20 region: us-east-1\n\
             \x20 endpoint: {}\n\
             \x20 path_style: true\n\
             \x20 allow_http: true\n\
             backup:\n\
             \x20 compression: zstd\n\
             \x20 segment_max_records: 1000\n\
             \x20 segment_max_bytes: 10485760\n\
             \x20 max_concurrent_partitions: 3\n",
            topics.join(", "),
            s.archive_bucket,
            s.s3_endpoint
        ),
    )
    .expect("the backup spec");
    let mut c = Command::new(bin());
    c.args(["backup", "run", "--spec"])
        .arg(&spec)
        .arg("--allowed-clusters")
        .arg(backup_allowlist())
        .arg("--signing-key")
        .arg(signing_pem());
    engine_env(&mut c);
    if scram {
        c.env("LOGWEIR_SOURCE_PASSWORD", &s.scram_password);
    }
    let out = output_within(c, 900);
    assert_eq!(
        out.status.code(),
        Some(0),
        "logweir backup run {backup_id} must exit 0:\n{}",
        text(&out)
    );
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let line = |prefix: &str| {
        stdout
            .lines()
            .find_map(|l| l.strip_prefix(prefix))
            .map(str::to_string)
    };
    let receipt_key = line("receipt-key=").expect("backup run prints receipt-key=");
    let catalog_key = line("catalog-key=");
    let store = archive_store(backup_id);
    let (receipt_bytes, _) = store
        .get_capped(
            &receipt_key,
            logweir_engine_oso::storage::caps::SIGNED_DOCUMENT,
        )
        .unwrap_or_else(|e| panic!("read {receipt_key}: {e}"));
    let receipt: BackupReceipt = serde_json::from_slice(&receipt_bytes).expect("a receipt");
    Backup {
        backup_id: backup_id.to_string(),
        out,
        receipt_key,
        receipt_bytes,
        receipt,
        catalog_key,
    }
}

fn archive_location(backup_id: &str) -> logweir_core::engine::StorageUrl {
    let s = stack();
    serde_yaml::from_str(&format!(
        "backend: s3\nbucket: {}\nprefix: {backup_id}\nregion: us-east-1\nendpoint: {}\n\
         path_style: true\nallow_http: true\n",
        s.archive_bucket, s.s3_endpoint
    ))
    .expect("a storage url")
}

fn archive_store(backup_id: &str) -> logweir_engine_oso::storage::Store {
    use_stack_s3_env();
    logweir_engine_oso::storage::Store::read_only_from_url(&archive_location(backup_id))
        .expect("the archive store")
}

fn catalog_record(b: &Backup) -> Value {
    let Some(key) = &b.catalog_key else {
        return Value::Null;
    };
    let (bytes, _) = archive_store(&b.backup_id)
        .get_capped(key, logweir_engine_oso::storage::caps::SIGNED_DOCUMENT)
        .unwrap_or_else(|e| panic!("read {key}: {e}"));
    serde_json::from_slice(&bytes).expect("a catalog record")
}

/// Both readers over the receipt, exactly as an auditor runs them.
fn verify_receipt_both_readers(b: &Backup) -> Value {
    let dir = demo_dir().join(format!("{}-verify", b.backup_id));
    std::fs::create_dir_all(&dir).expect("dir");
    let doc = dir.join("receipt.json");
    let sig = dir.join("receipt.sig");
    std::fs::write(&doc, &b.receipt_bytes).expect("written");
    let (sidecar, _) = archive_store(&b.backup_id)
        .get_capped(
            &b.receipt_key.replace(".receipt.json", ".receipt.sig"),
            logweir_engine_oso::storage::caps::SIGNED_DOCUMENT,
        )
        .expect("the sidecar");
    std::fs::write(&sig, sidecar).expect("written");
    let pubkey = root().join("e2e/fixtures/signed/public.pem");
    let mut rust = Command::new(bin());
    rust.args([
        "drill",
        "verify",
        "--payload-type",
        "backup-receipt",
        "--scorecard",
    ])
    .arg(&doc)
    .arg("--signature")
    .arg(&sig)
    .arg("--public-key")
    .arg(&pubkey);
    let rust = output_within(rust, 60);
    let mut py = Command::new(harness::auditor_python());
    py.arg(root().join("docs/verify_scorecard.py"))
        .args(["--payload-type", "backup-receipt"])
        .arg(&doc)
        .arg(&sig)
        .arg(&pubkey);
    let py = output_within(py, 60);
    let lines = |o: &Output| -> Vec<String> {
        text(o)
            .lines()
            .filter_map(|l| {
                let i = l
                    .find("config_coverage[")
                    .or_else(|| l.find("config_coverage:"))?;
                Some(l[i..].to_string())
            })
            .collect()
    };
    json!({
        "rust_exit": rust.status.code(),
        "python_exit": py.status.code(),
        "rust_coverage_lines": lines(&rust),
        "python_coverage_lines": lines(&py),
    })
}

fn mint_approval(spec_text: &str, approver_pem: &Path) -> PathBuf {
    let doc = json!({
        "approver": "fx4-e2e@example.com",
        "ticket": "FX-4",
        "plan_hash": logweir_core::ids::sha256_prefixed(spec_text.as_bytes()),
        "approved_at": "2026-09-29T00:00:00Z",
    });
    let bytes = serde_json::to_vec_pretty(&doc).expect("serialises");
    let p = demo_dir().join("fx4-approval.json");
    std::fs::write(&p, &bytes).expect("written");
    let sk = logweir_evidence::keys::SigningKey::from_pem_file(approver_pem).expect("the approver");
    let side = logweir_evidence::sign::sign_detached(
        &sk,
        logweir::drill::phase1_approval::PAYLOAD_TYPE_APPROVAL,
        &bytes,
    )
    .expect("signed");
    std::fs::write(
        p.with_extension("sig"),
        serde_json::to_vec(&side).expect("json"),
    )
    .expect("written");
    p
}

/// The trust a point-bound restore anchors the receipt's signature in: the
/// fixture evidence key, `EvidenceSigning`, valid now.
fn evidence_keyring() -> PathBuf {
    use logweir_core::execution_contract as wire;
    let sk = logweir_evidence::keys::SigningKey::from_pem_file(&signing_pem()).expect("the signer");
    let key_id = sk.verifying_key().key_id();
    let ring = wire::EvidenceKeyring {
        format_version: wire::EVIDENCE_KEYRING_FORMAT_VERSION.to_string(),
        keys: vec![wire::EvidenceKey {
            public_key_pem: sk.verifying_key().to_public_key_pem().expect("pem"),
            trust: logweir_core::trust::TrustedKey {
                key_id: key_id.clone(),
                principal_id: format!("install:{key_id}"),
                usages: vec![logweir_core::trust::KeyUsage::EvidenceSigning],
                not_before: chrono::Utc::now() - chrono::Duration::days(1),
                not_after: chrono::Utc::now() + chrono::Duration::days(1),
                state: logweir_core::trust::KeyState::Active,
                retired_at: None,
                revoked_at: None,
                revocation_reason: None,
                revocation_effective_from: None,
            },
        }],
    };
    let p = demo_dir().join("fx4-evidence-keys.json");
    std::fs::write(&p, serde_json::to_vec(&ring).expect("serialises")).expect("written");
    p
}

fn rfc3339(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .expect("representable")
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// A `newTopic` restore spec over one backup, optionally bound to its point
/// and optionally with SCRAM target auth.
fn restore_spec(
    b: &Backup,
    topics: &[&str],
    naming_prefix: &str,
    bound: bool,
    scram: bool,
) -> String {
    let s = stack();
    let point = if bound {
        format!(
            "\x20 point:\n\
             \x20   point_id: {}\n\
             \x20   receipt_key: {}\n\
             \x20   receipt_sha256: \"{}\"\n\
             \x20   manifest_sha256: \"{}\"\n",
            logweir::catalog::record::point_id(&b.receipt_bytes),
            b.receipt_key,
            logweir_core::ids::sha256_prefixed(&b.receipt_bytes),
            b.receipt.archive.manifest_sha256
        )
    } else {
        String::new()
    };
    let (bootstrap, auth) = if scram {
        (
            s.sasl.clone(),
            format!(
                "\x20 auth:\n\x20   mode: scramSha512\n\x20   username: {}\n",
                s.scram_user
            ),
        )
    } else {
        (s.plaintext.clone(), String::new())
    };
    format!(
        "source:\n\
         \x20 storage:\n\
         \x20   backend: s3\n\
         \x20   bucket: {bucket}\n\
         \x20   prefix: {id}\n\
         \x20   region: us-east-1\n\
         \x20   endpoint: {endpoint}\n\
         \x20   path_style: true\n\
         \x20   allow_http: true\n\
         \x20 backup: {id}\n\
         \x20 topics: [{topics}]\n\
         {point}\
         target:\n\
         \x20 bootstrap_servers: [{bootstrap}]\n\
         {auth}\
         \x20 mode: newTopic\n\
         \x20 topic_mapping_prefix: \"drill-\"\n\
         \x20 topic_naming:\n\
         \x20   prefix: \"{naming_prefix}\"\n\
         \x20 default_replication_factor: 1\n\
         sample:\n\
         \x20 window_start: \"{start}\"\n\
         \x20 window_end: \"{end}\"\n\
         \x20 records_per_partition: 25\n\
         \x20 anchor: head\n\
         objectives:\n\
         \x20 rto_seconds: 3600\n\
         \x20 rpo_seconds: 86400\n\
         \x20 pass_rate: 1.0\n\
         evidence:\n\
         \x20 backend: s3\n\
         \x20 bucket: {evidence}\n\
         \x20 prefix: logweir/\n\
         \x20 region: us-east-1\n\
         \x20 endpoint: {endpoint}\n\
         \x20 path_style: true\n\
         \x20 allow_http: true\n",
        bucket = s.archive_bucket,
        id = b.backup_id,
        endpoint = s.s3_endpoint,
        topics = topics.join(", "),
        start = rfc3339(b.receipt.covered.from_ms - 1000),
        end = rfc3339(b.receipt.covered.to_ms + 1000),
        evidence = s.evidence_bucket,
    )
}

struct Restore {
    out: Output,
    scorecard: Value,
    /// The signed scorecard `--out` wrote; its sidecar is beside it (`.sig`).
    scorecard_path: PathBuf,
}

fn restore(binary: &Path, spec_text: &str, label: &str, bound: bool, scram: bool) -> Restore {
    let spec = demo_dir().join(format!("fx4-{label}.yaml"));
    std::fs::write(&spec, spec_text).expect("written");
    let approver = demo_dir().join("fx4-approver.pem");
    if !approver.exists() {
        let sk = logweir_evidence::keys::SigningKey::generate_p256();
        std::fs::write(&approver, sk.to_pkcs8_pem().expect("pem")).expect("written");
    }
    let approver_pub = demo_dir().join("fx4-approver.pub.pem");
    let sk = logweir_evidence::keys::SigningKey::from_pem_file(&approver).expect("the approver");
    std::fs::write(
        &approver_pub,
        sk.verifying_key().to_public_key_pem().expect("pem"),
    )
    .expect("written");
    let approval = mint_approval(spec_text, &approver);
    let out_json = demo_dir().join(format!("fx4-{label}-scorecard.json"));
    let _ = std::fs::remove_file(&out_json);
    let mut c = Command::new(binary);
    c.args(["restore", "run", "--spec"])
        .arg(&spec)
        .arg("--approval")
        .arg(&approval)
        .arg("--approver-key")
        .arg(&approver_pub)
        .arg("--allowed-clusters")
        .arg(restore_allowlist())
        .arg("--signing-key")
        .arg(signing_pem())
        .arg("--out")
        .arg(&out_json);
    if bound {
        c.arg("--evidence-keys").arg(evidence_keyring());
    }
    engine_env(&mut c);
    if scram {
        c.env("LOGWEIR_TARGET_PASSWORD", stack().scram_password);
    }
    let out = output_within(c, 900);
    let scorecard = std::fs::read(&out_json)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or(Value::Null);
    eprintln!(
        "[fx4] restore {label}: exit={:?} outcome={}",
        out.status.code(),
        scorecard["outcome"]
    );
    Restore {
        out,
        scorecard,
        scorecard_path: out_json,
    }
}

fn tail(o: &Output, n: usize) -> Vec<String> {
    let t = text(o);
    let lines: Vec<&str> = t.lines().collect();
    lines[lines.len().saturating_sub(n)..]
        .iter()
        .map(|l| l.chars().take(600).collect())
        .collect()
}

fn topic_exists(topic: &str) -> bool {
    plaintext_reader()
        .list_topics()
        .expect("metadata")
        .iter()
        .any(|t| t.name == topic && t.error.is_none())
}

// ============================================================ row 1

/// **T13 at every reader, and consumer 1 (the readiness row), live.**
///
/// The restricted principal may Read and Describe `…-rd` but not
/// DescribeConfigs it, and (once one cluster ACL exists) may not
/// DescribeConfigs the broker. For each resource the row records librdkafka's
/// OWN answer (zero entries, no per-resource error: T13), what this build's
/// readers return for it, and — for the broker — the restore readiness row
/// `target.timestampBound` computed by the SHIPPED check kind over the pre-FX-4
/// flattening of that live answer (READY) and over the live `KafkaInventory`
/// (UNKNOWN). The ANONYMOUS super user's answers are the controls.
#[test]
#[ignore = "needs the stack's `acl` profile; see the module doc"]
fn a_denied_describe_configs_is_a_refusal_at_every_reader() {
    assert_the_authorizer_is_on();
    let n = nonce();
    let rd = format!("fx4-{n}-rd");
    let open = format!("fx4-{n}-open");
    let mut guard = ClusterGuard::new();
    guard.topics.extend([rd.clone(), open.clone()]);
    create_topic(&rd, &[("retention.ms", "3600000")]);
    // The control carries NO override (review M4): an authorised describe of
    // the common topic answers only broker and default entries, and must read
    // as a successful, non-empty answer — the no-misfire direction.
    create_topic(&open, &[]);
    guard.topic_acls.push(rd.clone());
    let acl_out = deny_describe_configs_on(&rd);

    // The topic resource.
    let raw_denied = raw_describe(true, None, Some(&rd));
    let raw_control = raw_describe(false, None, Some(&rd));
    let reader = scram_reader();
    let topic_configs = match reader.topic_configs(&rd) {
        Ok(m) => json!({"ok": m.len()}),
        Err(e) => json!({"err": kafka_error_kind(&e), "message": e.to_string()}),
    };
    let batch: Vec<Value> = reader
        .describe_topic_configs(&[rd.clone(), open.clone()])
        .expect("the call answers")
        .into_iter()
        .map(|(t, r)| match r {
            Ok(entries) => json!({
                "topic": t,
                "entries": entries.len(),
                "topic_overrides": entries
                    .iter()
                    .filter(|e| {
                        e.source == logweir_kafka::reader::ConfigSourceKind::DynamicTopicConfig
                    })
                    .count(),
            }),
            Err(e) => json!({"topic": t, "err": kafka_error_kind(&e)}),
        })
        .collect();
    let inventory = logweir_kafka::inventory::KafkaInventory::connect(
        &logweir_kafka::inventory::ConnectionSettings {
            bootstrap_servers: vec![stack().sasl],
            auth: scram_auth(),
            timeouts: logweir_kafka::inventory::ProbeTimeouts::for_budget(Duration::from_secs(60)),
        },
    )
    .expect("an inventory");
    use logweir_kafka::inventory::InventoryProbe;
    let inventory_topic = match inventory.topic_configs(&rd) {
        Ok(m) => json!({"ok": m.len()}),
        Err(f) => json!({"err": f.code.to_string()}),
    };

    // The broker resource.
    guard.cluster_acl = true;
    let cluster_acl_out = deny_cluster_describe_configs();
    let node = stack().broker_node_id;
    let raw_broker_denied = raw_describe(true, Some(node), None);
    let raw_broker_control = raw_describe(false, Some(node), None);
    let broker_configs = match reader.broker_configs() {
        Ok(m) => json!({"ok": m.len()}),
        Err(e) => json!({"err": kafka_error_kind(&e), "message": e.to_string()}),
    };
    let inventory_broker = match inventory.broker_configs() {
        Ok(m) => json!({"ok": m.len()}),
        Err(f) => json!({"err": f.code.to_string()}),
    };
    let before_row = readiness_row(Some(pre_fx4_broker_configs(true)));
    let after_row = readiness_row(None);
    undo_deny_cluster_describe_configs();
    guard.cluster_acl = false;
    let raw_broker_after_undo = raw_describe(true, Some(node), None);

    let evidence = json!({
        "topic": rd,
        "acl": acl_out,
        "cluster_acl": cluster_acl_out,
        "topic_resource": {
            "librdkafka_answer_denied_principal": raw_denied,
            "librdkafka_answer_super_user_control": raw_control,
            "RdKafkaReader::topic_configs": topic_configs,
            "RdKafkaReader::describe_topic_configs": batch,
            "KafkaInventory::topic_configs": inventory_topic,
        },
        "broker_resource": {
            "librdkafka_answer_denied_principal": raw_broker_denied,
            "librdkafka_answer_super_user_control": raw_broker_control,
            "RdKafkaReader::broker_configs": broker_configs,
            "KafkaInventory::broker_configs": inventory_broker,
            "librdkafka_answer_after_the_cluster_acl_is_removed": raw_broker_after_undo,
        },
        "target.timestampBound": {
            "before_pre_fx4_flattening_of_the_live_answer": before_row,
            "after_live_KafkaInventory": after_row,
        },
    });
    write_evidence(
        "a_denied_describe_configs_is_a_refusal_at_every_reader",
        &evidence,
    );

    // T13, as librdkafka reports it: ZERO entries and no error — for the
    // denied principal only.
    assert_eq!(
        raw_denied,
        json!({"ok": true, "entries": 0, "log.message.timestamp.type": null})
    );
    assert!(
        raw_control["entries"].as_u64().unwrap() > 0,
        "{raw_control}"
    );
    assert_eq!(raw_broker_denied["entries"], 0, "{raw_broker_denied}");
    assert!(raw_broker_control["entries"].as_u64().unwrap() > 0);
    assert!(raw_broker_after_undo["entries"].as_u64().unwrap() > 0);
    // …and every reader now says REFUSED.
    assert_eq!(topic_configs["err"], "NotAuthorized", "{topic_configs}");
    assert_eq!(batch[0]["err"], "NotAuthorized", "{batch:?}");
    assert!(batch[1]["entries"].as_u64().unwrap() > 0, "{batch:?}");
    assert_eq!(
        batch[1]["topic_overrides"], 0,
        "the control is the NO-override case, and it reads Ok: {batch:?}"
    );
    assert_eq!(
        inventory_topic["err"], "TopicAuthorizationFailed",
        "{inventory_topic}"
    );
    assert_eq!(broker_configs["err"], "NotAuthorized", "{broker_configs}");
    assert_eq!(
        inventory_broker["err"], "ClusterAuthorizationFailed",
        "{inventory_broker}"
    );
    // Consumer 1: the false green, before and after.
    assert_eq!(before_row["code"], "TimestampWithinBound", "{before_row}");
    assert_eq!(before_row["state"], "ready", "{before_row}");
    assert_eq!(after_row["code"], "BrokerConfigsNotReadable", "{after_row}");
    assert_eq!(after_row["state"], "unknown", "{after_row}");
}

/// The SHIPPED restore check kind (`logweir::check::kinds::run_kind_with`),
/// target half, dialled through the SHIPPED `Live` wiring as the restricted
/// principal — or, for the "before" leg, with its broker read replaced by the
/// pre-FX-4 flattening of the same live answer. The archive half is refused
/// on purpose: this row is about the target rows, which run independently.
fn readiness_row(before: Option<BTreeMap<String, String>>) -> Value {
    use logweir::check::kinds::{Live, Wiring};
    use logweir::check::store::{ObjectAccess, StoreFailure};
    use logweir_core::check_contract::*;
    use logweir_core::destination::*;
    use logweir_kafka::inventory::{CheckFailure, InventoryProbe};

    struct PreFx4 {
        live: Box<dyn InventoryProbe>,
        broker: BTreeMap<String, String>,
    }
    impl InventoryProbe for PreFx4 {
        fn cluster_id(&self) -> Result<Option<String>, CheckFailure> {
            self.live.cluster_id()
        }
        fn list_topics(&self) -> Result<logweir_kafka::inventory::Listing, CheckFailure> {
            self.live.list_topics()
        }
        fn describe_topic(
            &self,
            name: &str,
        ) -> Result<logweir_kafka::inventory::TopicPresence, CheckFailure> {
            self.live.describe_topic(name)
        }
        fn topic_configs(&self, name: &str) -> Result<BTreeMap<String, String>, CheckFailure> {
            self.live.topic_configs(name)
        }
        fn broker_configs(&self) -> Result<BTreeMap<String, String>, CheckFailure> {
            Ok(self.broker.clone())
        }
        fn validate_create_topics(
            &self,
            specs: &[logweir_kafka::reader::NewTopicSpec],
        ) -> Result<Vec<logweir_kafka::inventory::TopicCreateOutcome>, CheckFailure> {
            self.live.validate_create_topics(specs)
        }
    }

    struct TargetOnly {
        before: Option<BTreeMap<String, String>>,
    }
    impl Wiring for TargetOnly {
        fn broker(
            &self,
            plan: &ConnectionPlan,
            budget: Duration,
        ) -> Result<Box<dyn InventoryProbe>, CheckFailure> {
            let live = Live.broker(plan, budget)?;
            Ok(match &self.before {
                None => live,
                Some(broker) => Box::new(PreFx4 {
                    live,
                    broker: broker.clone(),
                }),
            })
        }
        fn objects(
            &self,
            _: &DestinationPlan,
            _: DestinationRole,
            _: Duration,
        ) -> Result<Box<dyn ObjectAccess>, StoreFailure> {
            Err(StoreFailure::new(
                CheckCode::DestinationNotValid,
                "this row reads no archive",
            ))
        }
        fn evidence_writer(
            &self,
            _: &DestinationPlan,
            _: Option<&GrantRef>,
            _: Duration,
        ) -> Result<Box<dyn ObjectAccess>, StoreFailure> {
            Err(StoreFailure::new(
                CheckCode::DestinationNotValid,
                "this row writes no evidence",
            ))
        }
        fn evidence_reader(
            &self,
            _: &DestinationPlan,
            _: Option<&GrantRef>,
            _: Duration,
        ) -> Result<Box<dyn ObjectAccess>, StoreFailure> {
            Err(StoreFailure::new(
                CheckCode::DestinationNotValid,
                "this row reads no evidence",
            ))
        }
        fn signer_key_id(&self, path: &str) -> Result<String, String> {
            Live.signer_key_id(path)
        }
        fn read_bytes(&self, path: &str) -> std::io::Result<Vec<u8>> {
            Live.read_bytes(path)
        }
        fn now(&self) -> chrono::DateTime<chrono::Utc> {
            Live.now()
        }
    }

    let s = stack();
    std::env::set_var("FX4_SCRAM_PASSWORD", &s.scram_password);
    let yaml = format!(
        "source:\n  storage: {{backend: s3, bucket: {}, prefix: fx4, region: us-east-1, \
         endpoint: \"{}\", path_style: true, allow_http: true}}\n  backup: fx4\n  topics: [orders]\n\
         target:\n  bootstrap_servers: [{}]\n  mode: newTopic\n  topic_mapping_prefix: \"drill-\"\n\
         \x20 topic_naming: {{prefix: \"fx4-\"}}\n  default_replication_factor: 1\n\
         \x20 auth: {{mode: scramSha512, username: {}}}\n\
         sample:\n  window_start: \"{}\"\n  window_end: \"{}\"\n  records_per_partition: 25\n\
         objectives: {{rto_seconds: 900, pass_rate: 1.0}}\n\
         evidence: {{backend: s3, bucket: {}, prefix: logweir/, region: us-east-1, \
         endpoint: \"{}\", path_style: true, allow_http: true}}\n",
        s.archive_bucket,
        s.s3_endpoint,
        s.sasl,
        s.scram_user,
        rfc3339(chrono::Utc::now().timestamp_millis() - 3_600_000),
        rfc3339(chrono::Utc::now().timestamp_millis() - 60_000),
        s.evidence_bucket,
        s.s3_endpoint
    );
    let plan_file = demo_dir().join("fx4-readiness-plan.yaml");
    std::fs::write(&plan_file, &yaml).expect("written");
    let location = DestinationLocation {
        provider: StorageProvider::S3,
        bucket: s.archive_bucket.clone(),
        prefix: "fx4".into(),
        region: Some("us-east-1".into()),
        endpoint: Some(s.s3_endpoint.clone()),
        addressing: Addressing::PathStyle,
        transport: TransportSecurity::InsecureHttp,
    };
    let plan = CheckPlan {
        contract: CHECK_PLAN_CONTRACT.to_string(),
        contract_version: CHECK_CONTRACT_VERSION,
        subject_uid: "fx4-readiness".into(),
        timeout_seconds: 120,
        policy_digest: None,
        request: CheckRequest::RestorePreflight(Box::new(RestorePreflightRequest {
            plan_file: plan_file.display().to_string(),
            plan_sha256: logweir_core::ids::sha256_prefixed(yaml.as_bytes()),
            target: ConnectionPlan {
                bootstrap_servers: vec![s.sasl.clone()],
                auth_mode: "scramSha512".into(),
                username: Some(s.scram_user.clone()),
                password_env: Some("FX4_SCRAM_PASSWORD".into()),
                tls: Some(false),
                ca_file: None,
                client_cert_file: None,
                client_key_file: None,
                principal: format!("User:{}", s.scram_user),
            },
            source_destination: DestinationPlan {
                location_digest: location.location_digest(),
                location,
                name: "fx4".into(),
                uid: "fx4".into(),
                ca_file: None,
                credentials: CredentialMode::Static,
            },
            evidence_destination: None,
            backup_id: "fx4".into(),
            manifest_key: "fx4/manifest.json".into(),
            checks: Vec::new(),
            skip_checks: Vec::new(),
        })),
    };
    let bytes = serde_json::to_vec(&plan).expect("serialises");
    let loaded = logweir::check::Loaded {
        plan,
        plan_sha256: logweir_core::ids::sha256_prefixed(&bytes),
        subject_uid: "fx4-readiness".into(),
    };
    let emission = logweir::check::kinds::run_kind_with(
        &loaded,
        logweir::check::Deadline::new(120),
        &TargetOnly { before },
    );
    let row = emission
        .result
        .checks
        .iter()
        .find(|c| c.id == CheckId::TargetTimestampBound)
        .expect("the row is computed");
    serde_json::to_value(row).expect("serialises")
}

// ============================================================ row 2

/// **T13 at phase 0 (consumer 2), by PROCESS: before and after.** The broker's
/// cluster-wide default is `LogAppendTime`; the restore identity (`logweir`)
/// may not DescribeConfigs the cluster. The pre-FX-4 binary reads the empty
/// answer as the Apache default and admits the plan as `CreateTime` without
/// the override probe; this build stops at phase 0 naming the refusal, before
/// creating anything on the target.
#[test]
#[ignore = "needs the stack's `acl` profile and FX4_BEFORE_BIN; see the module doc"]
fn phase_0_never_assumes_create_time_for_a_refused_broker_read() {
    assert_the_authorizer_is_on();
    let before_bin = PathBuf::from(std::env::var("FX4_BEFORE_BIN").expect(
        "FX4_BEFORE_BIN names a logweir binary built BEFORE FX-4; this row's 'before' leg runs it",
    ));
    let n = nonce();
    let topic = format!("fx4-{n}-p0");
    let mut guard = ClusterGuard::new();
    guard.topics.push(topic.clone());
    create_topic(&topic, &[]);
    produce(&topic, 20);
    let b = backup(&format!("fx4-{n}-p0"), &[&topic], false);

    guard.broker_default = true;
    let broker_default = set_broker_default_timestamp_type(Some("LogAppendTime"));
    guard.cluster_acl = true;
    deny_cluster_describe_configs();
    let raw = raw_describe(true, Some(stack().broker_node_id), None);

    let before_prefix = format!("fx4-{n}-before-");
    let after_prefix = format!("fx4-{n}-after-");
    guard.topics.push(format!("{before_prefix}{topic}"));
    guard.topics.push(format!("{after_prefix}{topic}"));
    let before = restore(
        &before_bin,
        &restore_spec(&b, &[&topic], &before_prefix, false, true),
        &format!("{n}-p0-before"),
        false,
        true,
    );
    let after = restore(
        &bin(),
        &restore_spec(&b, &[&topic], &after_prefix, false, true),
        &format!("{n}-p0-after"),
        false,
        true,
    );
    let preflight_line = |o: &Output| {
        String::from_utf8_lossy(&o.stdout)
            .lines()
            .find(|l| l.starts_with("topic-preflight="))
            .map(str::to_string)
    };
    let evidence = json!({
        "broker_default": broker_default,
        "librdkafka_answer_for_the_restore_identity": raw,
        "before": {
            "binary": before_bin.display().to_string(),
            "exit": before.out.status.code(),
            "topic_preflight_line": preflight_line(&before.out),
            "outcome": before.scorecard["outcome"],
            "target_topic_created": topic_exists(&format!("{before_prefix}{topic}")),
            "tail": tail(&before.out, 12),
        },
        "after": {
            "binary": bin().display().to_string(),
            "exit": after.out.status.code(),
            "topic_preflight_line": preflight_line(&after.out),
            "target_topic_created": topic_exists(&format!("{after_prefix}{topic}")),
            "tail": tail(&after.out, 12),
        },
    });
    write_evidence(
        "phase_0_never_assumes_create_time_for_a_refused_broker_read",
        &evidence,
    );

    assert_eq!(raw["entries"], 0, "{raw}");
    // BEFORE: admitted as CreateTime, the probe never ran.
    let line = preflight_line(&before.out).expect("the pre-FX-4 run printed its preflight");
    assert!(line.contains("\"timestampType\":\"CreateTime\""), "{line}");
    // AFTER: stopped at phase 0, exit 1, naming the refusal; nothing created.
    assert_eq!(after.out.status.code(), Some(1), "{}", text(&after.out));
    assert!(
        text(&after.out).contains(&format!(
            "not authorized: broker {} configuration",
            stack().broker_node_id
        )),
        "{}",
        text(&after.out)
    );
    assert!(!topic_exists(&format!("{after_prefix}{topic}")));
}

// ============================================================ row 3

/// **FX-4 itself.** Two backups and three restores over one fixture:
///
/// - backup A, as the restricted principal, of a topic whose DescribeConfigs
///   is DENIED and a topic with overrides: `captureDenied`, and — because the
///   engine's capture is all-or-nothing — `notCaptured`/`manifestDiffers` for
///   the neighbour whose overrides the engine therefore never wrote;
/// - backup B, as the super user, of the overrides topic, a topic on the
///   broker's DYNAMIC DEFAULT `LogAppendTime` and one with a TOPIC OVERRIDE
///   `LogAppendTime`: `captured`, each with its effective timestamp type and
///   its source;
/// - point-bound restores of A and B: `not_assessed` names exactly A's two
///   topics and none of B's; an unbound restore of B names its topic
///   `unknown`;
/// - point-bound restores of B by the RESTRICTED principal, which may do what
///   a restore does on its target prefix but not DescribeConfigs (review M5):
///   the no-override topic `latdef` is `targetReadDenied` — its backup record
///   is empty, so the pinned engine never describes the target and phase 7's
///   own read is the refused one — while the overrides topic `ovr` stops in
///   phase 6 with exit 1 and NO scorecard, because the engine's restore
///   (`restore_topic_configs`, on by default) describes every target whose
///   backup recorded overrides (measured on 3.7.1 on 2026-10-05);
/// - with `FX4_BEFORE_BIN` (as row 2), the same restricted restore of `latdef`
///   under the writer before FX-4: a 1.0.0 scorecard whose
///   `unexpected_divergence` says nothing about the topic — the silence the
///   fail-safe entry ends;
/// - every topic a restore did not assess also has its fail-safe entry in
///   `unexpected_divergence`, and the signed scorecards are kept under
///   `config-coverage/old-reader/` for a reader built before FX-4.
#[test]
#[ignore = "needs the stack's `acl` profile; see the module doc"]
fn capture_coverage_reaches_the_receipt_the_catalog_and_drill_parity() {
    assert_the_authorizer_is_on();
    use_stack_s3_env();
    let n = nonce();
    let denied = format!("fx4-{n}-denied");
    let ovr = format!("fx4-{n}-ovr");
    let lat_default = format!("fx4-{n}-latdef");
    let lat_override = format!("fx4-{n}-latovr");
    let mut guard = ClusterGuard::new();
    guard.topics.extend([
        denied.clone(),
        ovr.clone(),
        lat_default.clone(),
        lat_override.clone(),
    ]);
    create_topic(
        &denied,
        &[
            ("retention.ms", "3600000"),
            ("max.message.bytes", "2000000"),
        ],
    );
    create_topic(
        &ovr,
        &[
            ("retention.ms", "7200000"),
            ("compression.type", "gzip"),
            ("segment.ms", "3600000"),
        ],
    );
    create_topic(&lat_default, &[]);
    create_topic(
        &lat_override,
        &[("message.timestamp.type", "LogAppendTime")],
    );
    for t in [&denied, &ovr, &lat_default, &lat_override] {
        produce(t, 30);
    }
    guard.topic_acls.push(denied.clone());
    deny_describe_configs_on(&denied);

    // Backup A: the restricted principal, a denied topic and its neighbour.
    let a = backup(&format!("fx4-{n}-a"), &[&denied, &ovr], true);

    // Backup B: the broker's DYNAMIC DEFAULT is LogAppendTime for this one.
    guard.broker_default = true;
    let broker_default = set_broker_default_timestamp_type(Some("LogAppendTime"));
    let bb = backup(
        &format!("fx4-{n}-b"),
        &[&ovr, &lat_default, &lat_override],
        false,
    );
    set_broker_default_timestamp_type(None);
    guard.broker_default = false;

    // The drills. Point-bound restores read the coverage from the VERIFIED
    // receipt; the unbound one has none to read.
    let pa = format!("fx4-{n}-ra-");
    let pb = format!("fx4-{n}-rb-");
    let pu = format!("fx4-{n}-ru-");
    for (prefix, topics) in [
        (&pa, vec![&denied, &ovr]),
        (&pb, vec![&ovr, &lat_default, &lat_override]),
        (&pu, vec![&lat_override]),
    ] {
        for t in topics {
            guard.topics.push(format!("{prefix}{t}"));
        }
    }
    let ra = restore(
        &bin(),
        &restore_spec(&a, &[&denied, &ovr], &pa, true, false),
        &format!("{n}-ra"),
        true,
        false,
    );
    let rb = restore(
        &bin(),
        &restore_spec(&bb, &[&ovr, &lat_default, &lat_override], &pb, true, false),
        &format!("{n}-rb"),
        true,
        false,
    );
    let ru = restore(
        &bin(),
        &restore_spec(&bb, &[&lat_override], &pu, false, false),
        &format!("{n}-ru"),
        false,
        false,
    );
    // Restores T (review M5, live): the restricted principal restores B and
    // may not DescribeConfigs its own target topics. `rt` takes the topic
    // whose backup record is EMPTY, so phase 7's read is the refused one;
    // `rto` takes the overrides topic, which the engine's restore describes
    // itself (`restore_topic_configs`), so phase 6 stops first.
    let pt = format!("fx4-{n}-rt-");
    guard.topics.push(format!("{pt}{lat_default}"));
    guard.prefixed_acls.push(pt.clone());
    let restore_acl = allow_restore_without_describe_configs(&pt);
    let rt = restore(
        &bin(),
        &restore_spec(&bb, &[&lat_default], &pt, true, true),
        &format!("{n}-rt"),
        true,
        true,
    );
    let pto = format!("fx4-{n}-rto-");
    guard.topics.push(format!("{pto}{ovr}"));
    guard.prefixed_acls.push(pto.clone());
    let restore_acl_ovr = allow_restore_without_describe_configs(&pto);
    let rto = restore(
        &bin(),
        &restore_spec(&bb, &[&ovr], &pto, true, true),
        &format!("{n}-rto"),
        true,
        true,
    );
    let engine_refusal = format!("DescribeConfigs failed for 2/{pto}{ovr}: error_code=29");
    // BEFORE FX-4 (only with FX4_BEFORE_BIN, as row 2): the same identity and
    // the same no-override topic under the writer before FX-4, which read the
    // refused target configuration as EMPTY and compared it with the topic's
    // empty record — a clean `unexpected_divergence`. The marker in `rt` is what
    // ends that silence (review M5).
    let before_rt = std::env::var_os("FX4_BEFORE_BIN").map(|before_bin| {
        let pbt = format!("fx4-{n}-rtb-");
        guard.topics.push(format!("{pbt}{lat_default}"));
        guard.prefixed_acls.push(pbt.clone());
        allow_restore_without_describe_configs(&pbt);
        let r = restore(
            &PathBuf::from(before_bin),
            &restore_spec(&bb, &[&lat_default], &pbt, false, true),
            &format!("{n}-rtb"),
            false,
            true,
        );
        (pbt, r)
    });
    // The signed documents, kept for the old-reader check (review M5).
    let keep = evidence_dir().join("old-reader");
    std::fs::create_dir_all(&keep).expect("the old-reader directory");
    for (name, r) in [
        ("restore-a", &ra),
        ("restore-b", &rb),
        ("restore-unbound", &ru),
        ("restore-target-read-denied", &rt),
    ] {
        for ext in ["json", "sig"] {
            let from = r.scorecard_path.with_extension(ext);
            if from.exists() {
                std::fs::copy(&from, keep.join(format!("{name}.{ext}"))).expect("copied");
            }
        }
    }

    let parity = |r: &Restore| {
        json!({
            "exit": r.out.status.code(),
            "outcome": r.scorecard["outcome"],
            "format_version": r.scorecard["format_version"],
            "topic_parity": r.scorecard["topic_parity"],
            "target_diff": r.scorecard["target_diff"],
            "tail": if r.scorecard.is_null() { tail(&r.out, 15) } else { vec![] },
        })
    };
    let evidence = json!({
        "topics": {"denied": denied, "overrides": ovr, "broker_default_lat": lat_default,
                   "topic_override_lat": lat_override},
        "broker_default_while_backup_b_ran": broker_default,
        "backup_a": {
            "receipt_key": a.receipt_key,
            "format_version": a.receipt.format_version,
            "config_coverage": a.receipt.config_coverage,
            "engine_warned": text(&a.out).contains("Unable to capture topic configuration"),
            "catalog_record_topics": catalog_record(&a)["topics"],
            "verifiers": verify_receipt_both_readers(&a),
        },
        "backup_b": {
            "receipt_key": bb.receipt_key,
            "config_coverage": bb.receipt.config_coverage,
            "catalog_record_topics": catalog_record(&bb)["topics"],
            "verifiers": verify_receipt_both_readers(&bb),
        },
        "restore_a_point_bound": parity(&ra),
        "restore_b_point_bound": parity(&rb),
        "restore_b_unbound": parity(&ru),
        "restore_b_point_bound_target_read_denied": {
            "acl": restore_acl,
            "parity": parity(&rt),
        },
        "restore_b_target_read_denied_before_fx4": match &before_rt {
            Some((pbt, r)) => json!({
                "binary": std::env::var("FX4_BEFORE_BIN").unwrap_or_default(),
                "prefix": pbt,
                "exit": r.out.status.code(),
                "format_version": r.scorecard["format_version"],
                "topic_parity": r.scorecard["topic_parity"],
            }),
            None => json!("skipped: FX4_BEFORE_BIN unset"),
        },
        "restore_b_overrides_topic_without_target_describe_configs": {
            "acl": restore_acl_ovr,
            "exit": rto.out.status.code(),
            "scorecard_written": !rto.scorecard.is_null(),
            "engine_refusal": text(&rto.out)
                .lines()
                .find(|l| l.contains(&engine_refusal))
                .map(|l| l.chars().take(400).collect::<String>()),
        },
        "old_reader_documents": keep.display().to_string(),
    });
    write_evidence(
        "capture_coverage_reaches_the_receipt_the_catalog_and_drill_parity",
        &evidence,
    );

    // The receipts.
    let ca = a
        .receipt
        .config_coverage
        .clone()
        .expect("receipt A carries the block");
    // Every receipt this build signs carries PROD-05.1's `topic_configuration`
    // (1.3.0); FX-4's 1.1.0 is what a receipt without it would say. Backup A
    // authenticates with SCRAM-SHA-512, which is not one of PROD-01.3's modes.
    assert_eq!(
        a.receipt.format_version,
        logweir_core::backup_receipt::FORMAT_VERSION_WITH_TOPIC_CONFIGURATION
    );
    assert_eq!(ca[&denied].coverage, "captureDenied", "{ca:?}");
    assert_eq!(ca[&denied].timestamp_type, None);
    assert_eq!(ca[&ovr].coverage, "notCaptured", "{ca:?}");
    assert_eq!(ca[&ovr].reason.as_deref(), Some("manifestDiffers"));
    let cb = bb
        .receipt
        .config_coverage
        .clone()
        .expect("receipt B carries the block");
    for t in [&ovr, &lat_default, &lat_override] {
        assert_eq!(cb[t].coverage, "captured", "{t}: {cb:?}");
    }
    let ts = |t: &str| {
        cb[t]
            .timestamp_type
            .as_ref()
            .map(|v| (v.value.clone(), v.source.clone()))
    };
    assert_eq!(
        ts(&lat_default),
        Some(("LogAppendTime".into(), "dynamicDefaultBrokerConfig".into()))
    );
    assert_eq!(
        ts(&lat_override),
        Some(("LogAppendTime".into(), "dynamicTopicConfig".into()))
    );
    // The catalog copies them.
    let cat_a = catalog_record(&a);
    let row = |cat: &Value, t: &str| {
        cat["topics"]
            .as_array()
            .expect("topics")
            .iter()
            .find(|r| r["name"] == t)
            .cloned()
            .expect("a row per topic")
    };
    assert_eq!(
        row(&cat_a, &denied)["config_coverage"]["coverage"],
        "captureDenied"
    );
    // Both readers accept both receipts.
    for b in [&a, &bb] {
        let v = verify_receipt_both_readers(b);
        assert_eq!(
            (v["rust_exit"].clone(), v["python_exit"].clone()),
            (json!(0), json!(0)),
            "{v}"
        );
        assert_eq!(v["rust_coverage_lines"], v["python_coverage_lines"], "{v}");
    }
    // Drill parity. `na` is this row's subject, the CONFIGURATION entries
    // (FX-4). FX-21's `replication_factor (notRecorded)` and
    // `partition_count (notRecorded)` entries depend on the engine: patch 0002
    // (`0.23.3+logweir.2`) records every topic's factor, OSO's 0.23.3 and the
    // rollback the first saved topic's only, and a bound restore reads the
    // receipt's factor where the manifest has none. `layout_na` holds them to
    // their shape; `replication_factor_parity.rs` is their row.
    let all_na = |r: &Restore| -> Vec<String> {
        r.scorecard["topic_parity"]["not_assessed"]
            .as_array()
            .expect("not_assessed")
            .iter()
            .map(|e| e.as_str().expect("a string").to_string())
            .collect()
    };
    let na = |r: &Restore| -> Value {
        json!(all_na(r)
            .into_iter()
            .filter(|e| e.contains(": configuration ("))
            .collect::<Vec<_>>())
    };
    let layout_na = |r: &Restore| -> Vec<String> {
        all_na(r)
            .into_iter()
            .filter(|e| !e.contains(": configuration ("))
            .collect()
    };
    for r in [&ra, &rb, &ru, &rt] {
        for e in layout_na(r) {
            assert!(
                e.ends_with(": replication_factor (notRecorded)")
                    || e.ends_with(": partition_count (notRecorded)"),
                "an entry of neither FX-4's nor FX-21's shape: {e}: {}",
                parity(r)
            );
        }
    }
    // Bound to a 1.3.0 receipt, which records every factor, a restore names
    // none `notRecorded` whatever the engine (FX-21's receipt fallback).
    for r in [&ra, &rb, &rt] {
        assert!(
            !layout_na(r)
                .iter()
                .any(|e| e.contains("replication_factor")),
            "{}",
            parity(r)
        );
    }
    assert_eq!(
        na(&ra),
        json!([
            format!("{pa}{denied}: configuration (captureDenied)"),
            format!("{pa}{ovr}: configuration (notCaptured)"),
        ]),
        "{}",
        parity(&ra)
    );
    assert_eq!(na(&rb), json!([]), "{}", parity(&rb));
    assert_eq!(
        na(&ru),
        json!([format!("{pu}{lat_override}: configuration (unknown)")]),
        "{}",
        parity(&ru)
    );
    assert_eq!(rt.out.status.code(), Some(0), "{}", parity(&rt));
    assert_eq!(
        na(&rt),
        json!([format!(
            "{pt}{lat_default}: configuration (targetReadDenied)"
        )]),
        "{}",
        parity(&rt)
    );
    // The overrides topic never reaches phase 7 under that identity: the
    // engine's own DescribeConfigs of the target is refused (29 is
    // TOPIC_AUTHORIZATION_FAILED), phase 6 exits 1 and no scorecard exists to
    // read as parity.
    assert_eq!(rto.out.status.code(), Some(1), "{}", text(&rto.out));
    assert!(
        rto.scorecard.is_null(),
        "no scorecard may be written: {}",
        rto.scorecard
    );
    assert!(
        text(&rto.out).contains(&engine_refusal),
        "the engine's refused DescribeConfigs of {pto}{ovr}: {}",
        tail(&rto.out, 6).join("\n")
    );
    // Before FX-4 that same restore was SILENT about the topic: a 1.0.0
    // scorecard, no `not_assessed`, and nothing in `unexpected_divergence`.
    if let Some((pbt, r)) = &before_rt {
        assert_eq!(
            r.out.status.code(),
            Some(0),
            "{}",
            tail(&r.out, 8).join("\n")
        );
        assert!(
            r.scorecard["topic_parity"].get("not_assessed").is_none(),
            "{}",
            r.scorecard["topic_parity"]
        );
        let named = format!("{pbt}{lat_default}:");
        assert!(
            !r.scorecard["topic_parity"]["unexpected_divergence"]
                .as_array()
                .expect("unexpected_divergence")
                .iter()
                .any(|u| u.as_str().is_some_and(|u| u.starts_with(&named))),
            "the writer before FX-4 was expected to be silent here: {}",
            r.scorecard["topic_parity"]
        );
    }
    // Review M5: a topic nobody assessed is never silent to a reader that
    // predates `not_assessed` — its fail-safe entry is in the array such a
    // reader shows.
    for r in [&ra, &rb, &ru, &rt] {
        let unexpected = &r.scorecard["topic_parity"]["unexpected_divergence"];
        // Every entry, FX-21's included: `"<topic>: <what> (<why>)"`, whose
        // twin is `"<topic>: <what> not assessed (<why>)"`.
        for entry in all_na(r) {
            let (head, why) = entry
                .rsplit_once(" (")
                .and_then(|(h, w)| Some((h, w.strip_suffix(')')?)))
                .expect("the not_assessed shape");
            let (topic, what) = head.rsplit_once(": ").expect("the not_assessed shape");
            let marker = format!("{topic}: {what} not assessed ({why})");
            assert!(
                unexpected
                    .as_array()
                    .expect("unexpected_divergence")
                    .iter()
                    .any(|u| u == &json!(marker)),
                "{marker} is missing from unexpected_divergence: {}",
                parity(r)
            );
        }
    }
}

// ============================================================ FX-8

/// [`restore_spec`] with a `restore:` block: `restore.point_in_time` at
/// `point_ms`, and with `producer_time` also `restore.time_basis:
/// producerTime` (FX-8).
fn restore_spec_at_a_point(
    b: &Backup,
    topics: &[&str],
    naming_prefix: &str,
    bound: bool,
    point_ms: i64,
    producer_time: bool,
) -> String {
    let basis = if producer_time {
        "\x20 time_basis: producerTime\n"
    } else {
        ""
    };
    let block = format!(
        "restore:\n\x20 point_in_time: \"{}\"\n{basis}",
        rfc3339(point_ms)
    );
    let spec = restore_spec(b, topics, naming_prefix, bound, false);
    assert_eq!(spec.matches("\nsample:\n").count(), 1, "{spec}");
    spec.replacen("\nsample:\n", &format!("\n{block}sample:\n"), 1)
}

/// **FX-8's broker-default arm, live.** A topic with NO `message.timestamp.type`
/// override, backed up while the broker's DYNAMIC DEFAULT is `LogAppendTime`:
/// the archive manifest records no override (the engine keeps explicit
/// overrides only), and the signed receipt records the effective value with
/// its source (FX-4). Then three restores at a point in time inside the
/// receipt's covered window, the broker default back to `CreateTime` so the
/// target needs no override probe:
///
/// | restore | plan | expected |
/// |---|---|---|
/// | `bound` | bound to the point, no `time_basis` | exit 3, `refusal-reason=PointInTimeByProducerTime`, naming the receipt's record; no target topic; no scorecard |
/// | `opted` | bound, `time_basis: producerTime` | exit 0 `pass`, `source.time_basis.producer_time == [topic]` |
/// | `unbound` | not bound, no `time_basis` | exit 0 `pass`, `source.time_basis.not_recorded == [topic]`: nothing the run reads records the type, and it is never read as `CreateTime` |
///
/// The negative control the record (§9) asks for is the last row: a reader of
/// the manifest override alone sees the `bound` plan exactly as the `unbound`
/// one, so it runs instead of refusing.
#[test]
#[ignore = "needs the stack's `acl` profile; see the module doc"]
fn fx8_a_broker_default_log_append_time_is_refused_from_the_bound_receipt() {
    assert_the_authorizer_is_on();
    use_stack_s3_env();
    let n = nonce();
    let topic = format!("fx8-{n}-latdef");
    let mut guard = ClusterGuard::new();
    guard.topics.push(topic.clone());
    create_topic(&topic, &[]);
    // Two batches a second and a half apart, so the covered window holds
    // more than one instant and a point inside it is LATER than its floor.
    produce(&topic, 15);
    std::thread::sleep(Duration::from_millis(1500));
    produce(&topic, 15);

    guard.broker_default = true;
    let broker_default = set_broker_default_timestamp_type(Some("LogAppendTime"));
    let b = backup(&format!("fx8-{n}-b"), &[&topic], false);
    set_broker_default_timestamp_type(None);
    guard.broker_default = false;

    // What the two records say about the topic.
    let coverage = b
        .receipt
        .config_coverage
        .clone()
        .expect("the receipt carries config_coverage");
    let recorded = coverage[&topic]
        .timestamp_type
        .as_ref()
        .map(|v| (v.value.clone(), v.source.clone()));
    let (manifest_bytes, _) = archive_store(&b.backup_id)
        .get_capped(
            &b.receipt.archive.manifest_key,
            logweir_engine_oso::storage::caps::SIGNED_DOCUMENT,
        )
        .expect("the manifest");
    let manifest: Value = serde_json::from_slice(&manifest_bytes).expect("the manifest is JSON");
    let manifest_configurations = manifest["topics"]
        .as_array()
        .and_then(|ts| {
            ts.iter()
                .find(|t| t["name"].as_str() == Some(topic.as_str()))
        })
        .map(|t| t["configurations"].clone())
        .unwrap_or(Value::Null);

    // The point: the newest archived record, inside the covered window.
    let point = b.receipt.covered.to_ms - 1;
    let target_of = |label: &str| format!("fx8-{n}-{label}-{topic}");
    // Swept on EVERY exit path, a panicking assertion included.
    for label in ["bound", "opted", "unbound"] {
        guard.topics.push(target_of(label));
    }
    let run = |label: &str, bound: bool, producer_time: bool| {
        let prefix = format!("fx8-{n}-{label}-");
        let r = restore(
            &bin(),
            &restore_spec_at_a_point(&b, &[&topic], &prefix, bound, point, producer_time),
            &format!("{n}-fx8-{label}"),
            bound,
            false,
        );
        let created = topic_exists(&target_of(label));
        (r, created)
    };
    let (bound, bound_created) = run("bound", true, false);
    let bound_target = target_of("bound");
    let (opted, _) = run("opted", true, true);
    let (unbound, _) = run("unbound", false, false);

    let said = |r: &Restore| {
        json!({
            "exit": r.out.status.code(),
            "outcome": r.scorecard["outcome"],
            "format_version": r.scorecard["format_version"],
            "time_basis": r.scorecard["source"]["time_basis"],
            "tail": tail(&r.out, 6),
        })
    };
    write_evidence(
        "fx8_a_broker_default_log_append_time_is_refused_from_the_bound_receipt",
        &json!({
            "topic": topic,
            "broker_default_while_the_backup_ran": broker_default,
            "receipt_key": b.receipt_key,
            "receipt_timestamp_type": recorded,
            "manifest_configurations": manifest_configurations,
            "point_in_time": rfc3339(point),
            "covered": [b.receipt.covered.from_ms, b.receipt.covered.to_ms],
            "bound": said(&bound),
            "bound_target_created": bound_created,
            "opted": said(&opted),
            "unbound": said(&unbound),
        }),
    );

    // The broker-default arm's only record is the receipt's.
    assert_eq!(
        recorded,
        Some((
            "LogAppendTime".to_string(),
            "dynamicDefaultBrokerConfig".to_string()
        ))
    );
    assert!(
        manifest_configurations
            .get("message.timestamp.type")
            .is_none(),
        "the manifest records no override for a broker default: {manifest_configurations}"
    );
    // bound, no opt-in: refused before any target exists.
    assert_eq!(bound.out.status.code(), Some(3), "{}", said(&bound));
    let out = text(&bound.out);
    assert_eq!(
        out.lines().rev().find(|l| l.starts_with("refusal-reason=")),
        Some("refusal-reason=PointInTimeByProducerTime"),
        "{out}"
    );
    assert!(
        out.contains(&format!(
            "`{topic}` (the bound backup receipt's effective message.timestamp.type \
             LogAppendTime from dynamicDefaultBrokerConfig)"
        )),
        "the refusal names the receipt's record: {out}"
    );
    assert!(bound.scorecard.is_null(), "a refused run signs nothing");
    assert!(!bound_created, "no target topic: {bound_target}");
    // bound, opt-in: runs and is labelled.
    assert_eq!(opted.out.status.code(), Some(0), "{}", said(&opted));
    assert_eq!(opted.scorecard["outcome"], "pass", "{}", said(&opted));
    assert_eq!(
        opted.scorecard["source"]["time_basis"],
        json!({"plan": "producerTime", "producer_time": [topic], "not_recorded": []}),
        "{}",
        said(&opted)
    );
    // unbound: nothing records the type, so it runs and says so.
    assert_eq!(unbound.out.status.code(), Some(0), "{}", said(&unbound));
    assert_eq!(
        unbound.scorecard["source"]["time_basis"],
        json!({"producer_time": [], "not_recorded": [topic]}),
        "{}",
        said(&unbound)
    );
}
