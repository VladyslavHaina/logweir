#![cfg(feature = "e2e")]
//! **FX-3 — what a `newTopic` restore signs about the source settings it did
//! not reconstruct, observed live.**
//!
//! One row, `#[ignore]`d because it needs the stack's `cluster3` profile
//! (`e2e/README.md`): a three-node cluster whose topics default to replication
//! factor 3, the source a production restore is most often recovering.
//!
//! | row | proves |
//! |---|---|
//! | `a_new_topic_restore_signs_the_source_settings_it_did_not_reconstruct` | A compacted source with a seven-day retention at replication factor 3 is backed up from `cluster3` and restored, bound to its point, into the default broker. The broker itself is the oracle: the restored topic is `cleanup.policy=delete`, `retention.ms=-1` and replication factor 1. The `newTopic` restore's SIGNED scorecard (format 1.2.0) names those three in `not_reconstructed` and in `unexpected_divergence`, and nothing in `intentionally_deviated`; a scratch drill of the same point still signs them as intended, with `not_reconstructed: []`. With `FX3_BEFORE_BIN` set, a `logweir` built before FX-3 restores the same point and signs the defect itself: format 1.1.0, all three `intentionally_deviated`. Both readers of this build accept every document and print the `reconstruction:` line each one implies; the signed documents are kept for the old-reader check |
//!
//! # Running it
//!
//! ```text
//! eval "$(e2e/compose/stack-env.sh --slot N --profiles cluster3)"
//! just e2e-up
//! cargo build -p logweir
//! [FX3_BEFORE_BIN=<a logweir binary built before FX-3>] AWS_EC2_METADATA_DISABLED=true \
//!   cargo test -p e2e --features e2e --test new_topic_parity -- --ignored --test-threads=1 --nocapture
//! just e2e-down
//! ```
//!
//! The row writes what it observed to `new-topic-parity/<row>.json` under the
//! stack's scratch directory (`harness::demo_dir()`: `.e2e/` on the default
//! stack, `.e2e/<project>/` on a slot), and the signed scorecards with their
//! sidecars to `new-topic-parity/old-reader/`.
//!
//! # Every stack address is `stack()`'s
//!
//! Every broker address, S3 endpoint, credential and the compose project this
//! file uses comes from [`stack`], which reads the per-stack harness
//! (`harness::bootstrap()`, `bootstrap_c3()`, `s3_endpoint()`,
//! `stack::project()`) after `stack::ensure_coherent()`, and nothing else
//! names one.
mod harness;

use harness::{bin, demo_dir, engine_bin, engine_digest, engine_mount, engine_version, root};
use logweir_core::backup_receipt::BackupReceipt;
use logweir_kafka::reader::{AuthConfig, ClusterReader};
use serde_json::{json, Value};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ============================================================ the stack

/// Every address and credential of the compose stack this file uses.
struct Stack {
    compose_project: String,
    compose_file: String,
    /// The profile the SOURCE cluster belongs to (`cluster3`).
    profile: String,
    /// The source: `cluster3`'s first node, its in-network PLAINTEXT listener
    /// for the broker's own CLIs, and all three nodes host-side.
    source_service: String,
    source_in_network: String,
    source_bootstrap: String,
    /// The target: the default broker, single node.
    target_service: String,
    target_in_network: String,
    target_bootstrap: String,
    s3_endpoint: String,
    s3_user: String,
    s3_secret: String,
    archive_bucket: String,
    evidence_bucket: String,
    marker_topic: String,
}

/// THE ONE PLACE this file reads the stack's addresses (see the module doc).
fn stack() -> Stack {
    harness::stack::ensure_coherent();
    let minio = ["minio", "admin"].concat();
    Stack {
        compose_project: harness::stack::project(),
        compose_file: "e2e/compose/docker-compose.yml".into(),
        profile: "cluster3".into(),
        source_service: "kafka-c3-1".into(),
        source_in_network: "kafka-c3-1:9094".into(),
        source_bootstrap: harness::bootstrap_c3(),
        target_service: "kafka-broker-1".into(),
        target_in_network: "kafka-broker-1:9094".into(),
        target_bootstrap: harness::bootstrap(),
        s3_endpoint: harness::s3_endpoint(),
        s3_user: minio.clone(),
        s3_secret: minio,
        archive_bucket: harness::ARCHIVE_BUCKET.into(),
        evidence_bucket: harness::EVIDENCE_BUCKET.into(),
        marker_topic: harness::MARKER_TOPIC.into(),
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

/// One of a broker's own CLIs, inside its RUNNING container. `exec`, never
/// `run`: the question is what the live broker holds.
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
        &s.profile,
        "exec",
        "-T",
        service,
    ])
    .args(args)
    .current_dir(root());
    output_within(c, 120)
}

fn kafka_cli_ok(service: &str, args: &[&str], what: &str) -> String {
    let o = kafka_cli(service, args);
    assert!(o.status.success(), "{what} failed:\n{}", text(&o));
    String::from_utf8_lossy(&o.stdout).into_owned()
}

fn nonce() -> String {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970")
        .as_nanos();
    format!("{:010}", n % 10_000_000_000)
}

// ============================================================ the clusters

/// The source topic, created with the broker's own CLI on `cluster3`.
fn create_source_topic(topic: &str, configs: &[(&str, &str)]) {
    let s = stack();
    let mut args: Vec<String> = [
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        &s.source_in_network,
        "--create",
        "--topic",
        topic,
        "--partitions",
        "3",
        "--replication-factor",
        "3",
    ]
    .iter()
    .map(|x| x.to_string())
    .collect();
    for (k, v) in configs {
        args.push("--config".into());
        args.push(format!("{k}={v}"));
    }
    let borrowed: Vec<&str> = args.iter().map(String::as_str).collect();
    kafka_cli_ok(
        &s.source_service,
        &borrowed,
        &format!("create topic {topic}"),
    );
}

/// `topic`'s replication factor and dynamic topic configuration as the broker
/// reports them — the ORACLE this row checks the signed labels against.
fn broker_view(service: &str, in_network: &str, topic: &str) -> Value {
    let describe = kafka_cli_ok(
        service,
        &[
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            in_network,
            "--describe",
            "--topic",
            topic,
        ],
        &format!("describe {topic}"),
    );
    let configs = kafka_cli_ok(
        service,
        &[
            "/opt/kafka/bin/kafka-configs.sh",
            "--bootstrap-server",
            in_network,
            "--describe",
            "--entity-type",
            "topics",
            "--entity-name",
            topic,
        ],
        &format!("describe the configuration of {topic}"),
    );
    // `ReplicationFactor: 3` on the summary line of `kafka-topics --describe`.
    let rf = describe
        .split_whitespace()
        .skip_while(|w| *w != "ReplicationFactor:")
        .nth(1)
        .and_then(|v| v.parse::<i64>().ok());
    let dynamic = |key: &str| -> Option<String> {
        configs
            .split([' ', ',', '\n'])
            .find_map(|w| w.strip_prefix(&format!("{key}=")))
            .map(str::to_string)
    };
    json!({
        "topic": topic,
        "replication_factor": rf,
        "dynamic_cleanup_policy": dynamic("cleanup.policy"),
        "dynamic_retention_ms": dynamic("retention.ms"),
        "kafka_topics_describe": describe.lines().next().unwrap_or("").trim(),
        "kafka_configs_describe": configs.trim(),
    })
}

fn delete_topic(service: &str, in_network: &str, topic: &str) {
    let _ = kafka_cli(
        service,
        &[
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            in_network,
            "--delete",
            "--topic",
            topic,
        ],
    );
}

/// Ten KEYED records into each of the three partitions, round robin, so every
/// sampled partition holds what the sample asks for and compaction (were it to
/// run) would remove nothing: every key is distinct.
fn produce(topic: &str, per_partition: usize) {
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    let producer: BaseProducer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", stack().source_bootstrap)
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

fn target_cluster_id() -> String {
    logweir_kafka::rdkafka_reader::RdKafkaReader::connect(
        &[stack().target_bootstrap],
        AuthConfig::Plaintext,
    )
    .expect("a PLAINTEXT reader on the target")
    .cluster_id()
    .expect("the target's cluster id")
}

fn target_topic_exists(topic: &str) -> bool {
    logweir_kafka::rdkafka_reader::RdKafkaReader::connect(
        &[stack().target_bootstrap],
        AuthConfig::Plaintext,
    )
    .expect("a PLAINTEXT reader on the target")
    .list_topics()
    .expect("metadata")
    .iter()
    .any(|t| t.name == topic && t.error.is_none())
}

// ============================================================ the pipeline

fn evidence_dir() -> PathBuf {
    demo_dir().join("new-topic-parity")
}

fn write_evidence(row: &str, v: &Value) {
    let dir = evidence_dir();
    std::fs::create_dir_all(&dir).expect("the evidence directory");
    let p = dir.join(format!("{row}.json"));
    std::fs::write(&p, serde_json::to_vec_pretty(v).expect("serialises")).expect("written");
    eprintln!("[fx3] evidence: {}", p.display());
}

fn signing_pem() -> PathBuf {
    root().join("e2e/fixtures/signed/signing.pem")
}

fn backup_allowlist() -> PathBuf {
    let p = demo_dir().join("fx3-backup-allowed-clusters.json");
    std::fs::write(
        &p,
        "{\"allowed_cluster_ids\": [\"SCRATCH-CLUSTER-NOT-THE-SOURCE\"]}\n",
    )
    .expect("written");
    p
}

/// The scratch drill's allowlist names the TARGET (the default broker); the
/// `newTopic` restore skips the check by design and is handed the same file.
fn restore_allowlist() -> PathBuf {
    let p = demo_dir().join("fx3-restore-allowed-clusters.json");
    std::fs::write(
        &p,
        serde_json::to_vec(
            &json!({"allowed_cluster_ids": [target_cluster_id()], "source_cluster_id": null}),
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
    receipt_key: String,
    receipt_bytes: Vec<u8>,
    receipt: BackupReceipt,
}

fn backup(backup_id: &str, topic: &str) -> Backup {
    let s = stack();
    let bootstrap = s.source_bootstrap.replace(',', ", ");
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
            s.archive_bucket, s.s3_endpoint
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
    let store = archive_store(backup_id);
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

fn archive_store(backup_id: &str) -> logweir_engine_oso::storage::Store {
    let s = stack();
    use_stack_s3_env();
    let url: logweir_core::engine::StorageUrl = serde_yaml::from_str(&format!(
        "backend: s3\nbucket: {}\nprefix: {backup_id}\nregion: us-east-1\nendpoint: {}\n\
         path_style: true\nallow_http: true\n",
        s.archive_bucket, s.s3_endpoint
    ))
    .expect("a storage url");
    logweir_engine_oso::storage::Store::read_only_from_url(&url).expect("the archive store")
}

fn mint_approval(spec_text: &str, approver_pem: &Path, label: &str) -> PathBuf {
    let doc = json!({
        "approver": "fx3-e2e@example.com",
        "ticket": "FX-3",
        "plan_hash": logweir_core::ids::sha256_prefixed(spec_text.as_bytes()),
        "approved_at": "2026-10-05T00:00:00Z",
    });
    let bytes = serde_json::to_vec_pretty(&doc).expect("serialises");
    let p = demo_dir().join(format!("fx3-{label}-approval.json"));
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
    let p = demo_dir().join("fx3-evidence-keys.json");
    std::fs::write(&p, serde_json::to_vec(&ring).expect("serialises")).expect("written");
    p
}

fn rfc3339(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .expect("representable")
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// A restore spec over one backup, bound to its point. `new_topic` picks the
/// mode: `newTopic` names its targets `<prefix><source>` through
/// `topic_naming.prefix`; a scratch drill maps through `topic_mapping_prefix`
/// and proves its marker topic on the target.
fn restore_spec(b: &Backup, topic: &str, prefix: &str, new_topic: bool) -> String {
    let s = stack();
    let mode = if new_topic {
        format!(
            "\x20 mode: newTopic\n\
             \x20 topic_mapping_prefix: \"drill-\"\n\
             \x20 topic_naming:\n\
             \x20   prefix: \"{prefix}\"\n"
        )
    } else {
        format!(
            "\x20 marker_topic: {}\n\
             \x20 topic_mapping_prefix: \"{prefix}\"\n\
             \x20 teardown: delete\n",
            s.marker_topic
        )
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
         {mode}\
         \x20 default_replication_factor: 1\n\
         sample:\n\
         \x20 window_start: \"{start}\"\n\
         \x20 window_end: \"{end}\"\n\
         \x20 records_per_partition: 10\n\
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
        point_id = logweir::catalog::record::point_id(&b.receipt_bytes),
        receipt_key = b.receipt_key,
        receipt_sha256 = logweir_core::ids::sha256_prefixed(&b.receipt_bytes),
        manifest_sha256 = b.receipt.archive.manifest_sha256,
        bootstrap = s.target_bootstrap,
        start = rfc3339(b.receipt.covered.from_ms - 1000),
        end = rfc3339(b.receipt.covered.to_ms + 1000),
        evidence = s.evidence_bucket,
    )
}

struct Restore {
    out: Output,
    scorecard: Value,
    scorecard_path: PathBuf,
}

fn restore(binary: &Path, spec_text: &str, label: &str) -> Restore {
    let spec = demo_dir().join(format!("fx3-{label}.yaml"));
    std::fs::write(&spec, spec_text).expect("written");
    let approver = demo_dir().join("fx3-approver.pem");
    if !approver.exists() {
        let sk = logweir_evidence::keys::SigningKey::generate_p256();
        std::fs::write(&approver, sk.to_pkcs8_pem().expect("pem")).expect("written");
    }
    let approver_pub = demo_dir().join("fx3-approver.pub.pem");
    let sk = logweir_evidence::keys::SigningKey::from_pem_file(&approver).expect("the approver");
    std::fs::write(
        &approver_pub,
        sk.verifying_key().to_public_key_pem().expect("pem"),
    )
    .expect("written");
    let approval = mint_approval(spec_text, &approver, label);
    let out_json = demo_dir().join(format!("fx3-{label}-scorecard.json"));
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
        .arg(&out_json)
        .arg("--evidence-keys")
        .arg(evidence_keyring());
    engine_env(&mut c);
    let out = output_within(c, 900);
    let scorecard = std::fs::read(&out_json)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or(Value::Null);
    eprintln!(
        "[fx3] restore {label}: exit={:?} outcome={} format={}",
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

/// Both readers of THIS build over one signed scorecard, as an auditor runs
/// them, plus `drill show`'s `topic parity` row.
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
    let mut show = Command::new(bin());
    show.args(["drill", "show"]).arg(&r.scorecard_path);
    let show = output_within(show, 60);
    let line = |o: &Output| -> Option<String> {
        text(o).lines().find_map(|l| {
            l.find("reconstruction: ")
                .map(|i| l[i..].trim_end().to_string())
        })
    };
    json!({
        "rust_exit": rust.status.code(),
        "python_exit": py.status.code(),
        "rust_reconstruction_line": line(&rust),
        "python_reconstruction_line": line(&py),
        "drill_show_exit": show.status.code(),
        "drill_show_topic_parity_row": text(&show)
            .lines()
            .find(|l| l.contains("topic parity"))
            .map(|l| l.trim().to_string()),
    })
}

/// The signed document and its sidecar, copied where the old-reader check
/// reads them.
fn keep_for_the_old_reader_check(r: &Restore, name: &str) {
    let keep = evidence_dir().join("old-reader");
    std::fs::create_dir_all(&keep).expect("the old-reader directory");
    std::fs::copy(&r.scorecard_path, keep.join(format!("{name}.json"))).expect("copied");
    std::fs::copy(
        r.scorecard_path.with_extension("sig"),
        keep.join(format!("{name}.sig")),
    )
    .expect("the sidecar copied");
}

fn strings(v: &Value) -> Vec<String> {
    v.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

// ============================================================ the row

/// **FX-3 itself, live.** See the module doc's table for what it proves.
///
/// Negative controls, each a run that must fail an assertion below: a
/// `logweir` that applies the scratch rationale in every mode (the pre-FX-3
/// writer — the `FX3_BEFORE_BIN` leg records exactly that document); one that
/// drops a deviation instead of moving it (`unexpected_divergence` then lacks
/// it, and phase 8 refuses to sign: arm NR-2); one that never writes the new
/// field (`not_reconstructed` absent); and a reader without the
/// `reconstruction:` line.
#[test]
#[ignore = "needs the stack's `cluster3` profile; see the module doc"]
fn a_new_topic_restore_signs_the_source_settings_it_did_not_reconstruct() {
    let s = stack();
    use_stack_s3_env();
    let n = nonce();
    let source = format!("fx3-{n}-compacted");
    let nt_prefix = format!("fx3-nt-{n}-");
    let sc_prefix = format!("fx3-sc-{n}-");
    let old_prefix = format!("fx3-old-{n}-");
    let nt_topic = format!("{nt_prefix}{source}");
    let sc_topic = format!("{sc_prefix}{source}");
    let old_topic = format!("{old_prefix}{source}");

    // 1. THE SOURCE: compacted, a seven-day retention override, replication
    //    factor 3, on `cluster3`.
    create_source_topic(
        &source,
        &[("cleanup.policy", "compact"), ("retention.ms", "604800000")],
    );
    produce(&source, 10);
    let source_view = broker_view(&s.source_service, &s.source_in_network, &source);
    assert_eq!(source_view["replication_factor"], 3, "{source_view}");
    assert_eq!(source_view["dynamic_cleanup_policy"], "compact");
    assert_eq!(source_view["dynamic_retention_ms"], "604800000");

    // 2. THE BACKUP, from `cluster3`. Its receipt must say the configuration
    //    was CAPTURED, so the restore's parity is assessed and every entry
    //    below is a deviation, not FX-4's not-assessed marker.
    let b = backup(&format!("fx3-{n}"), &source);
    let coverage = b
        .receipt
        .config_coverage
        .as_ref()
        .and_then(|c| c.get(&source))
        .map(|c| c.coverage.clone());
    assert_eq!(
        coverage.as_deref(),
        Some("captured"),
        "{:?}",
        b.receipt.config_coverage
    );

    // 3. THE NEW-TOPIC RESTORE (this build), bound to the point, into the
    //    default broker at the plan's replication factor 1.
    let nt = restore(&bin(), &restore_spec(&b, &source, &nt_prefix, true), "nt");
    assert_eq!(nt.out.status.code(), Some(0), "{}", text(&nt.out));
    let target_view = broker_view(&s.target_service, &s.target_in_network, &nt_topic);
    // THE ORACLE: what the restore created, read off the broker.
    assert_eq!(target_view["replication_factor"], 1, "{target_view}");
    assert_eq!(target_view["dynamic_retention_ms"], "-1", "{target_view}");
    assert_ne!(
        target_view["dynamic_cleanup_policy"], "compact",
        "the restore does not reconstruct compaction: {target_view}"
    );
    let moved = vec![
        format!("{nt_topic}: cleanup.policy"),
        format!("{nt_topic}: replication_factor"),
        format!("{nt_topic}: retention.ms"),
    ];
    let p = &nt.scorecard["topic_parity"];
    assert_eq!(
        nt.scorecard["format_version"],
        logweir_core::FORMAT_VERSION,
        "{}",
        nt.scorecard
    );
    assert_eq!(nt.scorecard["target"]["mode"], "newTopic");
    assert_eq!(nt.scorecard["outcome"], "pass");
    assert!(
        strings(&p["intentionally_deviated"]).is_empty(),
        "a newTopic restore signs nothing as intended: {p}"
    );
    assert_eq!(strings(&p["not_reconstructed"]), moved, "{p}");
    assert_eq!(
        strings(&p["unexpected_divergence"]),
        moved,
        "the twin every reader older than 1.2.0 shows: {p}"
    );
    assert_eq!(strings(&p["not_assessed"]), Vec::<String>::new(), "{p}");
    let nt_readers = readers(&nt);
    keep_for_the_old_reader_check(&nt, "new-topic-1.2.0");

    // 4. THE CONTROL: a scratch drill of the same point. Today's labels.
    let sc = restore(&bin(), &restore_spec(&b, &source, &sc_prefix, false), "sc");
    assert_eq!(sc.out.status.code(), Some(0), "{}", text(&sc.out));
    let p = &sc.scorecard["topic_parity"];
    assert!(
        sc.scorecard["target"].get("mode").is_none(),
        "scratch is absent on the wire"
    );
    assert_eq!(sc.scorecard["outcome"], "pass");
    assert_eq!(
        strings(&p["intentionally_deviated"]),
        vec![
            format!("{sc_topic}: cleanup.policy"),
            format!("{sc_topic}: replication_factor"),
            format!("{sc_topic}: retention.ms"),
        ],
        "a scratch drill's deviations are intended, exactly as before FX-3: {p}"
    );
    assert!(strings(&p["unexpected_divergence"]).is_empty(), "{p}");
    assert_eq!(p["not_reconstructed"], json!([]), "{p}");
    assert!(
        !target_topic_exists(&sc_topic),
        "the scratch drill's phase 9 tears its topic down"
    );
    let sc_readers = readers(&sc);
    keep_for_the_old_reader_check(&sc, "scratch-1.2.0");

    // 5. THE DEFECT ITSELF, when a pre-FX-3 binary is available: the same
    //    point restored by the writer before this change.
    let before = std::env::var("FX3_BEFORE_BIN").ok().map(PathBuf::from);
    let old = before.as_ref().map(|binary| {
        let old = restore(binary, &restore_spec(&b, &source, &old_prefix, true), "old");
        assert_eq!(old.out.status.code(), Some(0), "{}", text(&old.out));
        let p = &old.scorecard["topic_parity"];
        assert_eq!(old.scorecard["format_version"], "1.1.0");
        assert_eq!(old.scorecard["target"]["mode"], "newTopic");
        assert_eq!(
            strings(&p["intentionally_deviated"]),
            vec![
                format!("{old_topic}: cleanup.policy"),
                format!("{old_topic}: replication_factor"),
                format!("{old_topic}: retention.ms"),
            ],
            "the writer before FX-3 signs lost compaction, retention and RF as intended: {p}"
        );
        assert!(p.get("not_reconstructed").is_none(), "{p}");
        keep_for_the_old_reader_check(&old, "new-topic-1.1.0-before-fx3");
        let readers = readers(&old);
        (old, readers)
    });

    // 6. Both readers of this build over every document.
    let nt_line = format!(
        "reconstruction: source settings NOT RECONSTRUCTED for {}",
        moved.join("; ")
    );
    for (name, r, want) in [
        ("new-topic-1.2.0", &nt_readers, Some(nt_line.clone())),
        ("scratch-1.2.0", &sc_readers, None),
    ] {
        assert_eq!(r["rust_exit"], 0, "{name}: {r}");
        assert_eq!(r["python_exit"], 0, "{name}: {r}");
        assert_eq!(r["rust_reconstruction_line"], json!(want), "{name}: {r}");
        assert_eq!(r["python_reconstruction_line"], json!(want), "{name}: {r}");
    }
    if let Some((_, r)) = &old {
        let want = format!(
            "reconstruction: not recorded, so the settings this newTopic document labels \
             intentionally_deviated were NOT reconstructed: {old_topic}: cleanup.policy; \
             {old_topic}: replication_factor; {old_topic}: retention.ms"
        );
        assert_eq!(r["rust_exit"], 0, "{r}");
        assert_eq!(r["python_exit"], 0, "{r}");
        assert_eq!(r["rust_reconstruction_line"], json!(want), "{r}");
        assert_eq!(r["python_reconstruction_line"], json!(want), "{r}");
    }

    // 7. The evidence, then clean up what this row created.
    let old_target_view = old
        .as_ref()
        .map(|_| broker_view(&s.target_service, &s.target_in_network, &old_topic));
    write_evidence(
        "a_new_topic_restore_signs_the_source_settings_it_did_not_reconstruct",
        &json!({
            "source_topic": source_view,
            "backup_id": b.backup_id,
            "receipt_key": b.receipt_key,
            "receipt_config_coverage": b.receipt.config_coverage,
            "new_topic": {
                "exit": nt.out.status.code(),
                "format_version": nt.scorecard["format_version"],
                "target": nt.scorecard["target"],
                "outcome": nt.scorecard["outcome"],
                "topic_parity": nt.scorecard["topic_parity"],
                "target_topic_on_the_broker": target_view,
                "readers": nt_readers,
            },
            "scratch": {
                "exit": sc.out.status.code(),
                "format_version": sc.scorecard["format_version"],
                "outcome": sc.scorecard["outcome"],
                "topic_parity": sc.scorecard["topic_parity"],
                "torn_down": !target_topic_exists(&sc_topic),
                "readers": sc_readers,
            },
            "before_fx3": old.as_ref().map(|(o, r)| json!({
                "binary": before.as_ref().map(|p| p.display().to_string()),
                "exit": o.out.status.code(),
                "format_version": o.scorecard["format_version"],
                "topic_parity": o.scorecard["topic_parity"],
                "target_topic_on_the_broker": old_target_view,
                "readers_of_this_build": r,
            })),
            "old_reader_documents": evidence_dir().join("old-reader").display().to_string(),
        }),
    );
    delete_topic(&s.target_service, &s.target_in_network, &nt_topic);
    if old.is_some() {
        delete_topic(&s.target_service, &s.target_in_network, &old_topic);
    }
    delete_topic(&s.source_service, &s.source_in_network, &source);
}
