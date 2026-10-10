#![cfg(feature = "e2e")]
//! **PROD-15.1 — a restore under the ORIGINAL topic name into an absent topic,
//! observed live**, on a compose slot with the `cluster2` and `autocreate`
//! profiles (`e2e/README.md`). Every row backs a topic up with `logweir backup
//! run`, binds the restore to that point (so the verified receipt's measured
//! source cluster id is the one condition 3 compares), and runs `logweir
//! restore run` with the pinned engine. The BROKERS are the oracle: a topic's
//! presence and its record count are read off the cluster, never off a
//! document.
//!
//! | row | proves | negative control in the row |
//! |---|---|---|
//! | `a_deleted_topic_is_recovered_under_its_name_on_a_second_cluster` | backed up on the default broker, deleted there, restored under its own name into `cluster2`: exit 0, `pass`, every record on `cluster2`, the topic still there after the run (teardown never deletes it), scorecard 1.8.0 `target.original_name` with `targetIsNotSource` and the receipt's source id; both readers accept and print the same `original name:` lines; the approval was minted by `drill approve --approval-subject original-name` | the same plan under an ORDINARY approval is refused, exit 3, `ApprovalSubjectMismatch`, nothing created; `drill approve` without the flag refuses to sign it |
//! | `a_deleted_topic_is_recovered_under_its_name_on_the_same_cluster` | the default broker (auto-creation off) is both source and target: refused while the name exists (exit 3, records untouched), then after the delete recovered with a COMPLETE verification (phase 7's complete lane, exact counts), `autoCreateDisabled` | the run while the name exists |
//! | `the_same_cluster_with_auto_creation_enabled_is_refused` | backed up on `autocreate`, deleted, restored into `autocreate`: exit 3 `OriginalNameAutoCreateEnabled`, the name stays absent | the assertion that the name is absent fails if anything was written |
//! | `a_producer_that_creates_the_name_after_phase_0_loses_the_race_by_name` | target `autocreate` (another cluster, so admitted): when the runner announces phase 5 a producer sends one record to the name, which the broker auto-creates; the restore stops at creation, exit 1, its LAST line `failure-reason=TargetTopicAppeared` and the line before it naming the topic as `appeared` (review M4), and the topic holds the producer's ONE record and nothing restored | a build that wrote into the existing topic leaves more than one record |
//! | `a_producer_writing_during_the_restore_fails_its_verification` | review M5: the runner is suspended at phase 6 while a producer writes 6 records into the topic the restore created; the engine restores 30, phase 7's count bound finds 36 against a bound of 30, and the run signs `fail-integrity` (exit 2), the topic holding both | a phase 7 that passed the topic |
//! | `a_declarative_owner_blocks_unless_the_owner_path_is_chosen` | a Strimzi `KafkaTopic` for the name given with `--kafka-topic-resources` (simulated: no Strimzi in the lab) refuses, exit 3 `OriginalNameOwnerPresent`; with `owner_path: true` the restore runs and signs the owner and the path; a plan that states no owner and has no resources refuses `OriginalNameOwnerNotChecked` | the two refusals |
//! | `scratch_mode_keeps_the_identity_ban` | an `original_name` block in a scratch drill: exit 3 `OriginalNameNotNewTopic`, nothing created | — |
//!
//! Scorecard versions are asserted as a FLOOR (`harness::assert_format_at_least`),
//! never pinned exactly.
//!
//! # Running it
//!
//! ```text
//! eval "$(e2e/compose/stack-env.sh --slot N --profiles auth,cluster2,autocreate)"
//! just e2e-up
//! cargo build -p logweir
//! LOGWEIR_PYTHON=<python3 with cryptography> AWS_EC2_METADATA_DISABLED=true \
//!   cargo test -p e2e --features e2e --test original_name -- --ignored --test-threads=1 --nocapture
//! just e2e-down
//! ```
//!
//! Each row writes what it observed to `original-name/<row>.json` under the
//! stack's scratch directory (`harness::demo_dir()`).
//!
//! # Every stack address is `stack()`'s
//!
//! Every broker address, S3 endpoint and credential comes from [`stack`], which
//! reads the per-stack harness after `stack::ensure_coherent()`.
mod harness;

use harness::{bin, demo_dir, engine_bin, engine_digest, engine_mount, engine_version, root};
use logweir_core::backup_receipt::BackupReceipt;
use logweir_kafka::reader::{AuthConfig, ClusterReader};
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ============================================================ the stack

/// One cluster of the stack: its compose service, its in-network PLAINTEXT
/// listener (for the broker's own CLIs) and its host-side bootstrap.
#[derive(Clone)]
struct Cluster {
    service: String,
    in_network: String,
    bootstrap: String,
}

struct Stack {
    compose_project: String,
    compose_file: String,
    /// The default broker: auto-creation OFF.
    broker: Cluster,
    /// `cluster2`: a second cluster, auto-creation OFF, the marker topic.
    cluster2: Cluster,
    /// `autocreate`: a cluster whose broker AUTO-CREATES topics.
    autocreate: Cluster,
    s3_endpoint: String,
    s3_user: String,
    s3_secret: String,
    archive_bucket: String,
    evidence_bucket: String,
}

/// THE ONE PLACE this file reads the stack's addresses.
fn stack() -> Stack {
    harness::stack::ensure_coherent();
    let minio = ["minio", "admin"].concat();
    Stack {
        compose_project: harness::stack::project(),
        compose_file: "e2e/compose/docker-compose.yml".into(),
        broker: Cluster {
            service: "kafka-broker-1".into(),
            in_network: "kafka-broker-1:9094".into(),
            bootstrap: harness::bootstrap(),
        },
        cluster2: Cluster {
            service: "kafka-cluster2".into(),
            in_network: "kafka-cluster2:9094".into(),
            bootstrap: harness::bootstrap_cluster2(),
        },
        autocreate: Cluster {
            service: "kafka-autocreate".into(),
            in_network: "kafka-autocreate:9094".into(),
            bootstrap: harness::bootstrap_autocreate(),
        },
        s3_endpoint: harness::s3_endpoint(),
        s3_user: minio.clone(),
        s3_secret: minio,
        archive_bucket: harness::ARCHIVE_BUCKET.into(),
        evidence_bucket: harness::EVIDENCE_BUCKET.into(),
    }
}

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

/// One of a broker's own CLIs, inside its RUNNING container.
fn kafka_cli(service: &str, args: &[&str]) -> Output {
    let s = stack();
    let mut c = Command::new("docker");
    c.args([
        "compose",
        "-p",
        &s.compose_project,
        "-f",
        &s.compose_file,
        "--profile",
        "cluster2",
        "--profile",
        "autocreate",
        "exec",
        "-T",
        service,
    ])
    .args(args)
    .current_dir(root());
    output_within(c, 120)
}

fn nonce() -> String {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970")
        .as_nanos();
    format!("{:010}", n % 10_000_000_000)
}

// ============================================================ the clusters

fn reader(c: &Cluster) -> logweir_kafka::rdkafka_reader::RdKafkaReader {
    logweir_kafka::rdkafka_reader::RdKafkaReader::connect(
        std::slice::from_ref(&c.bootstrap),
        AuthConfig::Plaintext,
    )
    .expect("a PLAINTEXT reader")
}

fn cluster_id(c: &Cluster) -> String {
    reader(c).cluster_id().expect("the cluster id")
}

fn topic_exists(c: &Cluster, topic: &str) -> bool {
    reader(c)
        .list_topics()
        .expect("metadata")
        .iter()
        .any(|t| t.name == topic)
}

/// Every record the cluster holds for `topic`: the sum of its partitions'
/// high watermarks (the topics here are never compacted or truncated).
fn record_count(c: &Cluster, topic: &str) -> i64 {
    reader(c)
        .end_offsets(topic)
        .expect("end offsets")
        .iter()
        .map(|(_, end)| *end)
        .sum()
}

fn create_topic(c: &Cluster, topic: &str) {
    let o = kafka_cli(
        &c.service,
        &[
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            &c.in_network,
            "--create",
            "--topic",
            topic,
            "--partitions",
            "3",
            "--replication-factor",
            "1",
        ],
    );
    assert!(o.status.success(), "create {topic}:\n{}", text(&o));
    harness::await_created_on(&c.bootstrap, topic, 3);
}

/// Delete `topic` and wait, bounded, until the cluster no longer lists it.
fn delete_topic(c: &Cluster, topic: &str) {
    let o = kafka_cli(
        &c.service,
        &[
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            &c.in_network,
            "--delete",
            "--topic",
            topic,
        ],
    );
    assert!(o.status.success(), "delete {topic}:\n{}", text(&o));
    let deadline = Instant::now() + Duration::from_secs(60);
    while topic_exists(c, topic) {
        assert!(
            Instant::now() < deadline,
            "{topic} still listed 60 s after its delete"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// Ten keyed records into each of the three partitions.
fn produce(c: &Cluster, topic: &str, per_partition: usize) {
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    let producer: BaseProducer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", &c.bootstrap)
        .set("message.timeout.ms", "20000")
        .set("acks", "all")
        .create()
        .expect("a producer");
    for i in 0..per_partition * 3 {
        let payload = format!("{topic}-{i}");
        let key = format!("k{i}");
        let partition = (i % 3) as i32;
        loop {
            match producer.send(
                BaseRecord::to(topic)
                    .partition(partition)
                    .key(&key)
                    .payload(&payload),
            ) {
                Ok(()) => break,
                Err((e, _)) if e.to_string().contains("QueueFull") => {
                    producer.poll(Duration::from_millis(50));
                }
                Err((e, _)) => panic!("produce to {topic}: {e}"),
            }
        }
    }
    producer
        .flush(Duration::from_secs(30))
        .unwrap_or_else(|e| panic!("flush {topic}: {e}"));
}

/// ONE record to `topic` with no partition named — the way an application's
/// producer sends — so a broker that auto-creates topics creates it.
fn produce_one_creating(c: &Cluster, topic: &str) {
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    let producer: BaseProducer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", &c.bootstrap)
        .set("message.timeout.ms", "30000")
        .set("acks", "all")
        .set("allow.auto.create.topics", "true")
        .create()
        .expect("a producer");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match producer.send(BaseRecord::<str, str>::to(topic).payload("the-applications-record")) {
            Ok(()) => break,
            Err((e, _)) => {
                assert!(Instant::now() < deadline, "produce to {topic}: {e}");
                producer.poll(Duration::from_millis(100));
            }
        }
    }
    producer
        .flush(Duration::from_secs(30))
        .unwrap_or_else(|e| panic!("flush {topic}: {e}"));
}

// ============================================================ the pipeline

fn evidence_dir() -> PathBuf {
    demo_dir().join("original-name")
}

fn write_evidence(row: &str, v: &Value) {
    let dir = evidence_dir();
    std::fs::create_dir_all(&dir).expect("the evidence directory");
    let p = dir.join(format!("{row}.json"));
    std::fs::write(&p, serde_json::to_vec_pretty(v).expect("serialises")).expect("written");
    eprintln!("[prod-15-1] evidence: {}", p.display());
}

fn signing_pem() -> PathBuf {
    root().join("e2e/fixtures/signed/signing.pem")
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

struct Backup {
    backup_id: String,
    receipt_key: String,
    receipt_bytes: Vec<u8>,
    receipt: BackupReceipt,
}

fn backup(backup_id: &str, source: &Cluster, topic: &str) -> Backup {
    let s = stack();
    let spec = demo_dir().join(format!("{backup_id}-backup.yaml"));
    std::fs::write(
        &spec,
        format!(
            "backup_id: {backup_id}\n\
             source:\n\
             \x20 bootstrap_servers: [{bootstrap}]\n\
             \x20 topics: [{topic}]\n\
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
            s.archive_bucket,
            s.s3_endpoint,
            bootstrap = source.bootstrap,
        ),
    )
    .expect("the backup spec");
    let allow = demo_dir().join("prod151-backup-allowed-clusters.json");
    std::fs::write(
        &allow,
        "{\"allowed_cluster_ids\": [\"SCRATCH-CLUSTER-NOT-THE-SOURCE\"]}\n",
    )
    .expect("written");
    let mut c = Command::new(bin());
    c.args(["backup", "run", "--spec"])
        .arg(&spec)
        .arg("--allowed-clusters")
        .arg(&allow)
        .arg("--signing-key")
        .arg(signing_pem());
    engine_env(&mut c);
    let out = output_within(c, 900);
    assert_eq!(
        out.status.code(),
        Some(0),
        "logweir backup run {backup_id} must exit 0:\n{}",
        text(&out)
    );
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let receipt_key = stdout
        .lines()
        .find_map(|l| l.strip_prefix("receipt-key="))
        .map(str::to_string)
        .expect("backup run prints receipt-key=");
    use_stack_s3_env();
    let url: logweir_core::engine::StorageUrl = serde_yaml::from_str(&format!(
        "backend: s3\nbucket: {}\nprefix: {backup_id}\nregion: us-east-1\nendpoint: {}\n\
         path_style: true\nallow_http: true\n",
        s.archive_bucket, s.s3_endpoint
    ))
    .expect("a storage url");
    let store =
        logweir_engine_oso::storage::Store::read_only_from_url(&url).expect("the archive store");
    let (receipt_bytes, _) = store
        .get(&receipt_key)
        .unwrap_or_else(|e| panic!("read {receipt_key}: {e}"));
    let receipt: BackupReceipt = serde_json::from_slice(&receipt_bytes).expect("a receipt");
    Backup {
        backup_id: backup_id.to_string(),
        receipt_key,
        receipt_bytes,
        receipt,
    }
}

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
    let p = demo_dir().join("prod151-evidence-keys.json");
    std::fs::write(&p, serde_json::to_vec(&ring).expect("serialises")).expect("written");
    p
}

fn rfc3339(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .expect("representable")
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// How the plan names its targets.
enum Naming<'a> {
    /// `newTopic`, `topic_naming: {prefix: "", original_name: ...}` — the
    /// YAML that follows `original_name:` on its line (a flow mapping such as
    /// ` {}` or ` {owners: []}`).
    Original(&'a str),
    /// A scratch drill carrying the block (refused).
    ScratchWithBlock,
}

/// A restore plan over one backup, bound to its point, into `target`.
fn restore_spec(
    b: &Backup,
    topic: &str,
    target: &Cluster,
    naming: Naming<'_>,
    complete: bool,
) -> String {
    let s = stack();
    let target_block = match naming {
        Naming::Original(block) => format!(
            "\x20 mode: newTopic\n\
             \x20 topic_mapping_prefix: \"drill-\"\n\
             \x20 topic_naming:\n\
             \x20   prefix: \"\"\n\
             \x20   original_name:{block}\n"
        ),
        Naming::ScratchWithBlock => "\x20 mode: scratch\n\
             \x20 marker_topic: logweir.scratch\n\
             \x20 topic_mapping_prefix: \"drill-\"\n\
             \x20 teardown: delete\n\
             \x20 topic_naming:\n\
             \x20   prefix: \"\"\n\
             \x20   original_name: {owners: []}\n"
            .to_string(),
    };
    let coverage = if complete {
        "\x20 coverage: complete\n"
    } else {
        ""
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
         \x20 topics: [{topic}]\n\
         \x20 point:\n\
         \x20   point_id: {point_id}\n\
         \x20   receipt_key: {receipt_key}\n\
         \x20   receipt_sha256: \"{receipt_sha256}\"\n\
         \x20   manifest_sha256: \"{manifest_sha256}\"\n\
         target:\n\
         \x20 bootstrap_servers: [{bootstrap}]\n\
         {target_block}\
         \x20 default_replication_factor: 1\n\
         sample:\n\
         \x20 window_start: \"{start}\"\n\
         \x20 window_end: \"{end}\"\n\
         \x20 records_per_partition: 10\n\
         \x20 anchor: head\n\
         {coverage}\
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
        point_id = logweir::catalog::record::point_id(&b.receipt_bytes),
        receipt_key = b.receipt_key,
        receipt_sha256 = logweir_core::ids::sha256_prefixed(&b.receipt_bytes),
        manifest_sha256 = b.receipt.archive.manifest_sha256,
        bootstrap = target.bootstrap,
        start = rfc3339(b.receipt.covered.from_ms - 1000),
        end = rfc3339(b.receipt.covered.to_ms + 1000),
        evidence = s.evidence_bucket,
    )
}

/// The approver's statement that no declarative owner manages the name.
const NO_OWNER: &str = " {owners: []}";

fn approver() -> (PathBuf, PathBuf) {
    let pem = demo_dir().join("prod151-approver.pem");
    if !pem.exists() {
        let sk = logweir_evidence::keys::SigningKey::generate_p256();
        std::fs::write(&pem, sk.to_pkcs8_pem().expect("pem")).expect("written");
    }
    let public = demo_dir().join("prod151-approver.pub.pem");
    let sk = logweir_evidence::keys::SigningKey::from_pem_file(&pem).expect("the approver");
    std::fs::write(
        &public,
        sk.verifying_key().to_public_key_pem().expect("pem"),
    )
    .expect("written");
    (pem, public)
}

/// A v1 approval minted by HAND: `subject` is the signed approval subject
/// (`None`: an ordinary approval, which `drill approve` would refuse to sign
/// for an original-name plan).
fn hand_approval(spec_text: &str, subject: Option<&str>, label: &str) -> PathBuf {
    let (pem, _) = approver();
    let mut doc = json!({
        "approver": "prod151-e2e@example.com",
        "ticket": "PROD-15.1",
        "plan_hash": logweir_core::ids::sha256_prefixed(spec_text.as_bytes()),
        "approved_at": "2026-10-09T00:00:00Z",
    });
    if let Some(s) = subject {
        doc["approval_subject"] = json!(s);
    }
    let bytes = serde_json::to_vec_pretty(&doc).expect("serialises");
    let p = demo_dir().join(format!("prod151-{label}-approval.json"));
    std::fs::write(&p, &bytes).expect("written");
    let sk = logweir_evidence::keys::SigningKey::from_pem_file(&pem).expect("the approver");
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

/// `logweir drill approve` itself, as an approver runs it.
fn cli_approval(spec_path: &Path, original_name: bool, label: &str) -> (Output, PathBuf) {
    let (pem, _) = approver();
    let out_path = demo_dir().join(format!("prod151-{label}-cli-approval.json"));
    let _ = std::fs::remove_file(&out_path);
    let mut c = Command::new(bin());
    c.args(["drill", "approve", "--spec"])
        .arg(spec_path)
        .arg("--key")
        .arg(&pem)
        .args([
            "--approver",
            "prod151-e2e@example.com",
            "--ticket",
            "PROD-15.1",
        ])
        .arg("--out")
        .arg(&out_path);
    if original_name {
        c.args(["--approval-subject", "original-name"]);
    }
    (output_within(c, 60), out_path)
}

struct Restore {
    out: Output,
    scorecard: Value,
    scorecard_path: PathBuf,
}

fn write_spec(spec_text: &str, label: &str) -> PathBuf {
    let spec = demo_dir().join(format!("prod151-{label}.yaml"));
    std::fs::write(&spec, spec_text).expect("written");
    spec
}

fn restore_command(
    spec: &Path,
    approval: &Path,
    target: &Cluster,
    resources: Option<&Path>,
    out_json: &Path,
) -> Command {
    let (_, approver_pub) = approver();
    let allow = demo_dir().join("prod151-restore-allowed-clusters.json");
    std::fs::write(
        &allow,
        serde_json::to_vec(&json!({
            "allowed_cluster_ids": [cluster_id(target)],
            "source_cluster_id": null
        }))
        .expect("serialises"),
    )
    .expect("written");
    let _ = std::fs::remove_file(out_json);
    let mut c = Command::new(bin());
    c.args(["restore", "run", "--spec"])
        .arg(spec)
        .arg("--approval")
        .arg(approval)
        .arg("--approver-key")
        .arg(&approver_pub)
        .arg("--allowed-clusters")
        .arg(&allow)
        .arg("--signing-key")
        .arg(signing_pem())
        .arg("--out")
        .arg(out_json)
        .arg("--evidence-keys")
        .arg(evidence_keyring());
    if let Some(r) = resources {
        c.arg("--kafka-topic-resources").arg(r);
    }
    engine_env(&mut c);
    c
}

fn restore(
    spec: &Path,
    approval: &Path,
    target: &Cluster,
    resources: Option<&Path>,
    label: &str,
) -> Restore {
    let out_json = demo_dir().join(format!("prod151-{label}-scorecard.json"));
    let c = restore_command(spec, approval, target, resources, &out_json);
    let out = output_within(c, 900);
    let scorecard = std::fs::read(&out_json)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or(Value::Null);
    eprintln!(
        "[prod-15-1] restore {label}: exit={:?} outcome={} format={}",
        out.status.code(),
        scorecard["outcome"],
        scorecard["format_version"]
    );
    Restore {
        out,
        scorecard,
        scorecard_path: out_json,
    }
}

/// Both readers of THIS build over one signed scorecard, and the
/// `original name:` lines each printed.
fn readers(r: &Restore) -> Value {
    let sig = r.scorecard_path.with_extension("sig");
    let pubkey = root().join("e2e/fixtures/signed/public.pem");
    let mut rust = Command::new(bin());
    rust.args(["drill", "verify", "--scorecard"])
        .arg(&r.scorecard_path)
        .arg("--signature")
        .arg(&sig)
        .arg("--public-key")
        .arg(&pubkey);
    let rust = output_within(rust, 60);
    let mut py = Command::new(harness::auditor_python());
    py.arg(root().join("docs/verify_scorecard.py"))
        .arg(&r.scorecard_path)
        .arg(&sig)
        .arg(&pubkey);
    let py = output_within(py, 60);
    let lines = |o: &Output| -> Vec<String> {
        text(o)
            .lines()
            .filter_map(|l| {
                l.find("original name: ")
                    .map(|i| l[i..].trim_end().to_string())
            })
            .collect()
    };
    json!({
        "rust_exit": rust.status.code(),
        "python_exit": py.status.code(),
        "rust_lines": lines(&rust),
        "python_lines": lines(&py),
    })
}

/// The first thing the run said that names `token`: a plain line as it was
/// printed, or, from a JSON log line, the field that carries it.
fn said(all: &str, token: &str) -> Option<String> {
    all.lines().filter(|l| l.contains(token)).find_map(|l| {
        if !l.trim_start().starts_with('{') {
            return Some(l.trim().to_string());
        }
        let v: Value = serde_json::from_str(l).ok()?;
        ["outcome", "message", "error", "reason"]
            .iter()
            .filter_map(|k| v["fields"][*k].as_str())
            .find(|f| f.contains(token))
            .map(str::to_string)
    })
}

fn refusal_line(o: &Output) -> Option<String> {
    String::from_utf8_lossy(&o.stdout)
        .lines()
        .rev()
        .find(|l| l.starts_with("refusal-reason="))
        .map(str::to_string)
}

// ============================================================ the rows

/// **A deleted topic, recovered under its own name on a SECOND cluster.**
#[test]
#[ignore = "needs the compose stack with the cluster2 and autocreate profiles"]
fn a_deleted_topic_is_recovered_under_its_name_on_a_second_cluster() {
    let s = stack();
    let topic = format!("on-orders-{}", nonce());
    create_topic(&s.broker, &topic);
    produce(&s.broker, &topic, 10);
    let b = backup(&format!("prod151-second-{}", nonce()), &s.broker, &topic);
    delete_topic(&s.broker, &topic);
    assert!(!topic_exists(&s.cluster2, &topic));
    let spec_text = restore_spec(&b, &topic, &s.cluster2, Naming::Original(NO_OWNER), false);
    let spec = write_spec(&spec_text, "second");

    // `drill approve` refuses an ORDINARY approval for this plan, and signs the
    // separate subject when asked for it.
    let (refused_mint, _) = cli_approval(&spec, false, "second-ordinary");
    assert_eq!(
        refused_mint.status.code(),
        Some(1),
        "{}",
        text(&refused_mint)
    );
    assert!(
        text(&refused_mint).contains("--approval-subject original-name"),
        "{}",
        text(&refused_mint)
    );
    let (minted, approval) = cli_approval(&spec, true, "second");
    assert_eq!(minted.status.code(), Some(0), "{}", text(&minted));
    let signed: Value = serde_json::from_slice(&std::fs::read(&approval).unwrap()).unwrap();
    assert_eq!(signed["approval_subject"], "originalName");

    // NEGATIVE CONTROL: an ordinary approval is refused before anything is
    // written.
    let ordinary = hand_approval(&spec_text, None, "second-ordinary");
    let control = restore(&spec, &ordinary, &s.cluster2, None, "second-ordinary");
    assert_eq!(control.out.status.code(), Some(3), "{}", text(&control.out));
    assert!(
        text(&control.out).contains("ApprovalSubjectMismatch"),
        "{}",
        text(&control.out)
    );
    assert!(
        !topic_exists(&s.cluster2, &topic),
        "a refused run created the topic"
    );

    let r = restore(&spec, &approval, &s.cluster2, None, "second");
    assert_eq!(r.out.status.code(), Some(0), "{}", text(&r.out));
    assert_eq!(r.scorecard["outcome"], "pass");
    // At least the version that defines the block — never an exact pin.
    harness::assert_format_at_least(
        r.scorecard["format_version"].as_str().unwrap_or_default(),
        "1.8.0",
        "an original-name scorecard",
    );
    let on = &r.scorecard["target"]["original_name"];
    assert_eq!(on["approval_subject"], "originalName");
    assert_eq!(on["approval_mode"], "v1Approval");
    assert_eq!(on["cluster_condition"], "targetIsNotSource");
    assert_eq!(on["source_cluster_id"], json!(b.receipt.source.cluster_id));
    assert_eq!(on["owner_detection"], json!(["plan"]));
    assert_eq!(r.scorecard["target"]["topic_mapping_prefix"], "");
    // The broker is the oracle: every record, under the original name, and
    // the topic is still there after the run (teardown never deletes it).
    assert!(topic_exists(&s.cluster2, &topic));
    assert_eq!(record_count(&s.cluster2, &topic), 30);
    let readers = readers(&r);
    assert_eq!(readers["rust_exit"], json!(0), "{readers}");
    assert_eq!(readers["python_exit"], json!(0), "{readers}");
    assert_eq!(readers["rust_lines"], readers["python_lines"], "{readers}");
    assert_eq!(
        readers["rust_lines"].as_array().map(Vec::len),
        Some(2),
        "{readers}"
    );
    write_evidence(
        "second-cluster",
        &json!({
            "topic": topic,
            "source_cluster_id": cluster_id(&s.broker),
            "target_cluster_id": cluster_id(&s.cluster2),
            "receipt_source_cluster_id": b.receipt.source.cluster_id,
            "cli_refused_ordinary_mint_exit": refused_mint.status.code(),
            "cli_refused_ordinary_mint_message": said(&text(&refused_mint), "--approval-subject"),
            "ordinary_approval_exit": control.out.status.code(),
            "ordinary_approval_refusal": refusal_line(&control.out),
            "ordinary_approval_message": said(&text(&control.out), "ApprovalSubjectMismatch"),
            "exit": r.out.status.code(),
            "outcome": r.scorecard["outcome"],
            "format_version": r.scorecard["format_version"],
            "original_name": on,
            "records_on_target": record_count(&s.cluster2, &topic),
            "exists_after_the_run": topic_exists(&s.cluster2, &topic),
            "readers": readers,
        }),
    );
}

/// **The same cluster, auto-creation off**: refused while the name exists,
/// recovered once it is gone, with a COMPLETE verification.
#[test]
#[ignore = "needs the compose stack with the cluster2 and autocreate profiles"]
fn a_deleted_topic_is_recovered_under_its_name_on_the_same_cluster() {
    let s = stack();
    let topic = format!("on-payments-{}", nonce());
    create_topic(&s.broker, &topic);
    produce(&s.broker, &topic, 10);
    let b = backup(&format!("prod151-same-{}", nonce()), &s.broker, &topic);
    let spec_text = restore_spec(&b, &topic, &s.broker, Naming::Original(NO_OWNER), true);
    let spec = write_spec(&spec_text, "same");
    let approval = hand_approval(&spec_text, Some("originalName"), "same");

    // The name exists: refused, and the live topic is untouched.
    let live = restore(&spec, &approval, &s.broker, None, "same-live");
    assert_eq!(live.out.status.code(), Some(3), "{}", text(&live.out));
    assert!(
        text(&live.out).contains("already exists"),
        "{}",
        text(&live.out)
    );
    let records_while_live = record_count(&s.broker, &topic);
    assert_eq!(records_while_live, 30);

    delete_topic(&s.broker, &topic);
    let r = restore(&spec, &approval, &s.broker, None, "same");
    assert_eq!(r.out.status.code(), Some(0), "{}", text(&r.out));
    assert_eq!(r.scorecard["outcome"], "pass");
    let on = &r.scorecard["target"]["original_name"];
    assert_eq!(on["cluster_condition"], "autoCreateDisabled");
    assert_eq!(on["source_cluster_id"], json!(cluster_id(&s.broker)));
    let verification = &r.scorecard["integrity"]["verification"];
    assert_eq!(verification["coverage"], "complete");
    assert_eq!(verification["complete"]["covered"], true, "{verification}");
    assert!(topic_exists(&s.broker, &topic));
    assert_eq!(record_count(&s.broker, &topic), 30);
    let readers = readers(&r);
    assert_eq!(readers["rust_exit"], json!(0), "{readers}");
    assert_eq!(readers["python_exit"], json!(0), "{readers}");
    assert_eq!(readers["rust_lines"], readers["python_lines"], "{readers}");
    write_evidence(
        "same-cluster",
        &json!({
            "topic": topic,
            "cluster_id": cluster_id(&s.broker),
            "while_the_name_existed": {
                "exit": live.out.status.code(),
                "refusal": refusal_line(&live.out),
                "message": said(&text(&live.out), "already exists"),
                "records_after": records_while_live,
            },
            "exit": r.out.status.code(),
            "outcome": r.scorecard["outcome"],
            "original_name": on,
            "verification_coverage": verification["coverage"],
            "verification_covered": verification["complete"]["covered"],
            "records_on_target": record_count(&s.broker, &topic),
            "readers": readers,
        }),
    );
}

/// **The same cluster with auto-creation ENABLED is refused.**
#[test]
#[ignore = "needs the compose stack with the cluster2 and autocreate profiles"]
fn the_same_cluster_with_auto_creation_enabled_is_refused() {
    let s = stack();
    let topic = format!("on-ledger-{}", nonce());
    create_topic(&s.autocreate, &topic);
    produce(&s.autocreate, &topic, 10);
    let b = backup(&format!("prod151-auto-{}", nonce()), &s.autocreate, &topic);
    delete_topic(&s.autocreate, &topic);
    let spec_text = restore_spec(&b, &topic, &s.autocreate, Naming::Original(NO_OWNER), false);
    let spec = write_spec(&spec_text, "auto");
    let approval = hand_approval(&spec_text, Some("originalName"), "auto");
    let r = restore(&spec, &approval, &s.autocreate, None, "auto");
    assert_eq!(r.out.status.code(), Some(3), "{}", text(&r.out));
    assert!(
        text(&r.out).contains("OriginalNameAutoCreateEnabled"),
        "{}",
        text(&r.out)
    );
    assert!(
        !topic_exists(&s.autocreate, &topic),
        "a refused run created the topic"
    );
    write_evidence(
        "same-cluster-auto-creation-enabled",
        &json!({
            "topic": topic,
            "cluster_id": cluster_id(&s.autocreate),
            "exit": r.out.status.code(),
            "refusal": refusal_line(&r.out),
            "message": said(&text(&r.out), "OriginalNameAutoCreateEnabled"),
            "exists_after": topic_exists(&s.autocreate, &topic),
        }),
    );
}

/// **The race**: a producer creates the name after phase 0 proved it absent.
#[test]
#[ignore = "needs the compose stack with the cluster2 and autocreate profiles"]
fn a_producer_that_creates_the_name_after_phase_0_loses_the_race_by_name() {
    let s = stack();
    let topic = format!("on-race-{}", nonce());
    create_topic(&s.broker, &topic);
    produce(&s.broker, &topic, 10);
    let b = backup(&format!("prod151-race-{}", nonce()), &s.broker, &topic);
    let spec_text = restore_spec(&b, &topic, &s.autocreate, Naming::Original(NO_OWNER), false);
    let spec = write_spec(&spec_text, "race");
    let approval = hand_approval(&spec_text, Some("originalName"), "race");
    let out_json = demo_dir().join("prod151-race-scorecard.json");
    let mut c = restore_command(&spec, &approval, &s.autocreate, None, &out_json);
    c.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = c.spawn().expect("spawn the restore");
    let stdout = child.stdout.take().expect("piped stdout");
    let mut se = child.stderr.take().expect("piped stderr");
    let t_err = std::thread::spawn(move || {
        let mut b = Vec::new();
        let _ = se.read_to_end(&mut b);
        b
    });
    // A watchdog: the runner is killed if it outlives 900 s, so the line loop
    // below always ends.
    let pid = child.id();
    let watchdog = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(900);
        while Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(500));
            // `kill -0` fails once the process is gone.
            let alive = Command::new("kill")
                .args(["-0", &pid.to_string()])
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|s| s.success());
            if !alive {
                return false;
            }
        }
        let _ = Command::new("kill")
            .args(["-KILL", &pid.to_string()])
            .status();
        true
    });
    // Read the runner's stdout line by line. When it ANNOUNCES phase 5 —
    // phase 0 proved the name absent long before, and the creation step
    // follows phase 5's verdict — the runner is SUSPENDED (SIGSTOP), the
    // application's producer sends its record (the broker auto-creates the
    // name), and the runner is resumed (SIGCONT). Suspending makes the order
    // certain: the topic exists before the runner's next instruction.
    let mut lines = Vec::new();
    let mut produced_at: Option<String> = None;
    for line in BufReader::new(stdout).lines() {
        let line = line.expect("a stdout line");
        if produced_at.is_none() && line.starts_with("progress-phase=5") {
            let stop = Command::new("kill")
                .args(["-STOP", &pid.to_string()])
                .status();
            assert!(stop.is_ok_and(|s| s.success()), "SIGSTOP the runner");
            produce_one_creating(&s.autocreate, &topic);
            harness::await_created_on(&s.autocreate.bootstrap, &topic, 1);
            let cont = Command::new("kill")
                .args(["-CONT", &pid.to_string()])
                .status();
            assert!(cont.is_ok_and(|s| s.success()), "SIGCONT the runner");
            produced_at = Some(line.clone());
        }
        lines.push(line);
    }
    let status = child.wait().expect("the restore exits");
    assert!(
        !watchdog.join().unwrap_or(true),
        "the restore ran past 900 s and was killed"
    );
    let stderr = String::from_utf8_lossy(&t_err.join().unwrap_or_default()).into_owned();
    let all = format!("{}\n{stderr}", lines.join("\n"));
    assert!(
        produced_at.is_some(),
        "the runner never announced phase 5:\n{all}"
    );
    assert_eq!(status.code(), Some(1), "{all}");
    assert!(all.contains("TargetTopicAppeared"), "{all}");
    // Review M4: the race is NAMED where a controller reads it — the LAST
    // stdout line, and the bounded line before it naming what appeared.
    assert_eq!(
        lines.last().map(String::as_str),
        Some("failure-reason=TargetTopicAppeared"),
        "{all}"
    );
    let race_line: Value = lines
        .iter()
        .find_map(|l| l.strip_prefix("target-topics-appeared="))
        .and_then(|v| serde_json::from_str(v).ok())
        .unwrap_or_else(|| panic!("no target-topics-appeared= line:\n{all}"));
    assert_eq!(race_line["appeared"], json!([topic.clone()]), "{race_line}");
    assert_eq!(race_line["removed"], json!([]), "{race_line}");
    // The broker is the oracle: the topic holds the producer's ONE record and
    // nothing the restore would have written.
    assert_eq!(record_count(&s.autocreate, &topic), 1, "{all}");
    write_evidence(
        "race",
        &json!({
            "topic": topic,
            "target_cluster_id": cluster_id(&s.autocreate),
            "produced_after": produced_at,
            "exit": status.code(),
            "message": said(&all, "TargetTopicAppeared"),
            "failure_reason_line": lines.last(),
            "target_topics_appeared_line": race_line,
            "records_on_target": record_count(&s.autocreate, &topic),
        }),
    );
}

/// `n` unkeyed records to an EXISTING topic, as an application's producer
/// sends them (no partition named).
fn produce_n(c: &Cluster, topic: &str, n: usize) {
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    let producer: BaseProducer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", &c.bootstrap)
        .set("message.timeout.ms", "30000")
        .set("acks", "all")
        .create()
        .expect("a producer");
    for i in 0..n {
        let payload = format!("live-{i}");
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match producer.send(BaseRecord::<str, str>::to(topic).payload(&payload)) {
                Ok(()) => break,
                Err((e, _)) => {
                    assert!(Instant::now() < deadline, "produce to {topic}: {e}");
                    producer.poll(Duration::from_millis(100));
                }
            }
        }
    }
    producer
        .flush(Duration::from_secs(30))
        .unwrap_or_else(|e| panic!("flush {topic}: {e}"));
}

/// **Review M5: a producer still writing to the name during the restore.**
/// The restore creates `<topic>` and, when it announces phase 6, the runner
/// is suspended while an application's producer writes 6 records into it;
/// then the engine restores the 30 archived ones. The product's answer is
/// DETECT AND FAIL: phase 7's count bound finds 36 records where the manifest
/// bounds the window at 30, the run signs `fail-integrity` (exit 2), and the
/// topic holds both — which is why the runbook says stop every producer of a
/// restored name first. KILLS: a phase 7 that passes a topic another writer
/// wrote into.
#[test]
#[ignore = "needs the compose stack with the cluster2 and autocreate profiles"]
fn a_producer_writing_during_the_restore_fails_its_verification() {
    let s = stack();
    let topic = format!("on-live-{}", nonce());
    create_topic(&s.broker, &topic);
    produce(&s.broker, &topic, 10);
    let b = backup(&format!("prod151-live-{}", nonce()), &s.broker, &topic);
    let spec_text = restore_spec(&b, &topic, &s.cluster2, Naming::Original(NO_OWNER), false);
    let spec = write_spec(&spec_text, "live");
    let approval = hand_approval(&spec_text, Some("originalName"), "live");
    let out_json = demo_dir().join("prod151-live-scorecard.json");
    let mut c = restore_command(&spec, &approval, &s.cluster2, None, &out_json);
    c.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = c.spawn().expect("spawn the restore");
    let pid = child.id();
    let stdout = child.stdout.take().expect("piped stdout");
    let mut se = child.stderr.take().expect("piped stderr");
    let t_err = std::thread::spawn(move || {
        let mut b = Vec::new();
        let _ = se.read_to_end(&mut b);
        b
    });
    let watchdog = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(900);
        while Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(500));
            let alive = Command::new("kill")
                .args(["-0", &pid.to_string()])
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|s| s.success());
            if !alive {
                return false;
            }
        }
        let _ = Command::new("kill")
            .args(["-KILL", &pid.to_string()])
            .status();
        true
    });
    let mut lines = Vec::new();
    let mut produced = false;
    for line in BufReader::new(stdout).lines() {
        let line = line.expect("a stdout line");
        if !produced && line.starts_with("progress-phase=6") {
            let stop = Command::new("kill")
                .args(["-STOP", &pid.to_string()])
                .status();
            assert!(stop.is_ok_and(|s| s.success()), "SIGSTOP the runner");
            produce_n(&s.cluster2, &topic, 6);
            let cont = Command::new("kill")
                .args(["-CONT", &pid.to_string()])
                .status();
            assert!(cont.is_ok_and(|s| s.success()), "SIGCONT the runner");
            produced = true;
        }
        lines.push(line);
    }
    let status = child.wait().expect("the restore exits");
    assert!(
        !watchdog.join().unwrap_or(true),
        "the restore ran past 900 s"
    );
    let stderr = String::from_utf8_lossy(&t_err.join().unwrap_or_default()).into_owned();
    let all = format!("{}\n{stderr}", lines.join("\n"));
    assert!(produced, "the runner never announced phase 6:\n{all}");
    assert_eq!(status.code(), Some(2), "{all}");
    let scorecard: Value =
        serde_json::from_slice(&std::fs::read(&out_json).expect("signed")).expect("a scorecard");
    assert_eq!(scorecard["outcome"], "fail-integrity", "{scorecard}");
    // The broker is the oracle: both writers' records are in the topic.
    assert_eq!(record_count(&s.cluster2, &topic), 36);
    let r = Restore {
        out: Output {
            status,
            stdout: lines.join("\n").into_bytes(),
            stderr: stderr.into_bytes(),
        },
        scorecard: scorecard.clone(),
        scorecard_path: out_json,
    };
    let readers = readers(&r);
    assert_eq!(
        readers["rust_exit"],
        json!(0),
        "the signed failure verifies: {readers}"
    );
    write_evidence(
        "producer-during-restore",
        &json!({
            "topic": topic,
            "exit": status.code(),
            "outcome": scorecard["outcome"],
            "partial_reason": scorecard["integrity"]["partial_reason"],
            "original_name": scorecard["target"]["original_name"],
            "records_on_target": record_count(&s.cluster2, &topic),
            "readers": readers,
        }),
    );
}

/// **A declarative owner** (a Strimzi `KafkaTopic`, simulated by the
/// resources file: no Strimzi in the lab).
#[test]
#[ignore = "needs the compose stack with the cluster2 and autocreate profiles"]
fn a_declarative_owner_blocks_unless_the_owner_path_is_chosen() {
    let s = stack();
    let topic = format!("on-owned-{}", nonce());
    create_topic(&s.broker, &topic);
    produce(&s.broker, &topic, 10);
    let b = backup(&format!("prod151-owner-{}", nonce()), &s.broker, &topic);
    let resources = demo_dir().join("prod151-kafkatopics.yaml");
    std::fs::write(
        &resources,
        format!(
            "apiVersion: v1\nkind: List\nitems:\n\
             - apiVersion: kafka.strimzi.io/v1beta2\n  kind: KafkaTopic\n  metadata:\n    \
             name: {topic}\n    namespace: kafka\n    labels:\n      strimzi.io/cluster: prod\n  \
             spec:\n    partitions: 3\n    replicas: 1\n"
        ),
    )
    .expect("written");

    // Nowhere looked: no owner statement, no resources.
    let unchecked_text = restore_spec(&b, &topic, &s.cluster2, Naming::Original(" {}"), false);
    let unchecked_spec = write_spec(&unchecked_text, "owner-unchecked");
    let unchecked = restore(
        &unchecked_spec,
        &hand_approval(&unchecked_text, Some("originalName"), "owner-unchecked"),
        &s.cluster2,
        None,
        "owner-unchecked",
    );
    assert_eq!(
        unchecked.out.status.code(),
        Some(3),
        "{}",
        text(&unchecked.out)
    );
    assert!(
        text(&unchecked.out).contains("OriginalNameOwnerNotChecked"),
        "{}",
        text(&unchecked.out)
    );

    // The KafkaTopic names the topic: refused.
    let blocked = restore(
        &unchecked_spec,
        &hand_approval(&unchecked_text, Some("originalName"), "owner-blocked"),
        &s.cluster2,
        Some(&resources),
        "owner-blocked",
    );
    assert_eq!(blocked.out.status.code(), Some(3), "{}", text(&blocked.out));
    assert!(
        text(&blocked.out).contains("OriginalNameOwnerPresent"),
        "{}",
        text(&blocked.out)
    );
    assert!(
        !topic_exists(&s.cluster2, &topic),
        "a refused run created the topic"
    );

    // The owner path, chosen explicitly: restored, and signed.
    let path_text = restore_spec(
        &b,
        &topic,
        &s.cluster2,
        Naming::Original(" {owner_path: true}"),
        false,
    );
    let path_spec = write_spec(&path_text, "owner-path");
    let r = restore(
        &path_spec,
        &hand_approval(&path_text, Some("originalName"), "owner-path"),
        &s.cluster2,
        Some(&resources),
        "owner-path",
    );
    assert_eq!(r.out.status.code(), Some(0), "{}", text(&r.out));
    let on = &r.scorecard["target"]["original_name"];
    assert_eq!(on["owner_path"], true);
    assert_eq!(on["owner_detection"], json!(["kafkaTopicResources"]));
    assert_eq!(
        on["owners"],
        json!([{"topic": topic, "kind": "strimzi", "reference": format!("kafka/{topic}"),
                "found_in": "kafkaTopicResources"}])
    );
    assert_eq!(record_count(&s.cluster2, &topic), 30);
    let readers = readers(&r);
    assert_eq!(readers["rust_lines"], readers["python_lines"], "{readers}");
    write_evidence(
        "declarative-owner",
        &json!({
            "topic": topic,
            "not_checked": {"exit": unchecked.out.status.code(),
                "message": said(&text(&unchecked.out), "OriginalNameOwnerNotChecked")},
            "blocked": {"exit": blocked.out.status.code(),
                "message": said(&text(&blocked.out), "OriginalNameOwnerPresent")},
            "owner_path": {"exit": r.out.status.code(), "original_name": on,
                "records_on_target": record_count(&s.cluster2, &topic)},
            "readers": readers,
            "limit": "simulated: the KafkaTopic is a resources file as `kubectl get kafkatopics -A -o yaml` writes it; no Strimzi operator runs in the lab, and the controller does not list KafkaTopic resources (PROD-05.1a)",
        }),
    );
}

/// **The identity ban stays in scratch mode.**
#[test]
#[ignore = "needs the compose stack with the cluster2 and autocreate profiles"]
fn scratch_mode_keeps_the_identity_ban() {
    let s = stack();
    let topic = format!("on-scratch-{}", nonce());
    create_topic(&s.broker, &topic);
    produce(&s.broker, &topic, 10);
    let b = backup(&format!("prod151-scratch-{}", nonce()), &s.broker, &topic);
    let spec_text = restore_spec(&b, &topic, &s.cluster2, Naming::ScratchWithBlock, false);
    let spec = write_spec(&spec_text, "scratch");
    let r = restore(
        &spec,
        &hand_approval(&spec_text, Some("originalName"), "scratch"),
        &s.cluster2,
        None,
        "scratch",
    );
    assert_eq!(r.out.status.code(), Some(3), "{}", text(&r.out));
    assert!(
        text(&r.out).contains("OriginalNameNotNewTopic"),
        "{}",
        text(&r.out)
    );
    assert!(!topic_exists(&s.cluster2, &topic));
    assert!(!topic_exists(&s.cluster2, &format!("drill-{topic}")));
    write_evidence(
        "scratch-mode",
        &json!({
            "topic": topic,
            "exit": r.out.status.code(),
            "refusal": refusal_line(&r.out),
            "message": said(&text(&r.out), "OriginalNameNotNewTopic"),
        }),
    );
}
