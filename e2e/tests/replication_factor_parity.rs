#![cfg(feature = "e2e")]
//! **FX-21 — a source replication factor the archive does not record is never
//! read as matching, and Logweir's engine build records every topic's.**
//!
//! One row, `#[ignore]`d because it needs the stack's `cluster3` profile
//! (`e2e/README.md`): a three-node cluster, so a source topic's replication
//! factor (3 or 2 here) differs from the target's (the default broker, 1) and
//! a restore that read the factor as matching would be visibly wrong.
//!
//! | row | proves |
//! |---|---|
//! | `a_multi_topic_backup_never_reads_an_unrecorded_replication_factor_as_matching` | Three topics on `cluster3` (replication factors 3, 2 and 3, each read back from the broker) are backed up in ONE `logweir backup run` and restored by a `newTopic` restore into the default broker, twice: UNBOUND (no recovery point, so phase 7 knows only the archive's manifest) and BOUND to the point (the verified receipt's 1.3.0 `topic_configuration` factor stands where the manifest records none). The archive's manifest is read back and every topic's `source_replication_factor` recorded. In the unbound scorecard every topic's factor is either a deviation (`not_reconstructed`) where the manifest records it, or `"<target>: replication_factor (notRecorded)"` in `not_assessed` with its twin in `unexpected_divergence` where it does not: never neither, which is the silent match FX-21 closes. The bound scorecard compares every topic's factor. **By engine:** Logweir's build from patch 0002 on (`+logweir.<n>`, n ≥ 2) records all three factors, so nothing is not assessed; OSO's 0.23.3 and `0.23.3+logweir.1`, the two builds FX-21 measured without the patch, record exactly one (the defect the patch fixes, reproduced); any other engine is recorded, not asserted. With `FX21_BEFORE_BIN` set, a `logweir` built before FX-21 restores the same backup unbound and signs the defect itself on such an engine: a topic with no recorded factor has no `replication_factor` entry at all. Both readers of this build accept every document and print the same `configuration parity:` line, naming each factor that was not assessed |
//!
//! # Running it
//!
//! ```text
//! eval "$(e2e/compose/stack-env.sh --slot N --profiles cluster3)"
//! just e2e-up
//! cargo build -p logweir
//! [FX21_BEFORE_BIN=<a logweir binary built before FX-21>] AWS_EC2_METADATA_DISABLED=true \
//!   cargo test -p e2e --features e2e --test replication_factor_parity -- --ignored --test-threads=1 --nocapture
//! just e2e-down
//! ```
//!
//! Run it once per engine: `.engine/kafka-backup` is the one that runs
//! (`harness::engine_bin`), so build Logweir's engine with
//! `scripts/engine-source.sh build` for the patched row, and put an earlier
//! build there (or let the harness fall back to OSO's 0.23.3 image) for the
//! old-engine row. The row writes what it observed to
//! `replication-factor-parity/<engine version>.json` under the stack's scratch
//! directory (`harness::demo_dir()`).
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
use logweir_kafka::reader::AuthConfig;
use serde_json::{json, Value};
use std::collections::BTreeMap;
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

/// Partitions per source topic.
const PARTITIONS: usize = 2;

/// A source topic on `cluster3` at `rf`, created with the broker's own CLI.
fn create_source_topic(topic: &str, rf: u32) {
    let s = stack();
    kafka_cli_ok(
        &s.source_service,
        &[
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            &s.source_in_network,
            "--create",
            "--topic",
            topic,
            "--partitions",
            &PARTITIONS.to_string(),
            "--replication-factor",
            &rf.to_string(),
        ],
        &format!("create topic {topic}"),
    );
}

/// `topic`'s replication factor as the broker reports it — the ORACLE.
fn broker_rf(service: &str, in_network: &str, topic: &str) -> Option<i64> {
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
    describe
        .split_whitespace()
        .skip_while(|w| *w != "ReplicationFactor:")
        .nth(1)
        .and_then(|v| v.parse::<i64>().ok())
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

/// Ten KEYED records into each partition, round robin.
fn produce(topic: &str, per_partition: usize) {
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    let producer: BaseProducer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", stack().source_bootstrap)
        .set("message.timeout.ms", "20000")
        .set("acks", "all")
        .create()
        .expect("a producer");
    for i in 0..per_partition * PARTITIONS {
        let payload = format!("{topic}-{i}");
        let key = format!("k{i}");
        let partition = (i % PARTITIONS) as i32;
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
    use logweir_kafka::reader::ClusterReader;
    logweir_kafka::rdkafka_reader::RdKafkaReader::connect(
        &[stack().target_bootstrap],
        AuthConfig::Plaintext,
    )
    .expect("a PLAINTEXT reader on the target")
    .cluster_id()
    .expect("the target's cluster id")
}

// ============================================================ the pipeline

fn evidence_dir() -> PathBuf {
    demo_dir().join("replication-factor-parity")
}

fn write_evidence(name: &str, v: &Value) {
    let dir = evidence_dir();
    std::fs::create_dir_all(&dir).expect("the evidence directory");
    let p = dir.join(format!("{name}.json"));
    std::fs::write(&p, serde_json::to_vec_pretty(v).expect("serialises")).expect("written");
    eprintln!("[fx21] evidence: {}", p.display());
}

fn signing_pem() -> PathBuf {
    root().join("e2e/fixtures/signed/signing.pem")
}

fn backup_allowlist() -> PathBuf {
    let p = demo_dir().join("fx21-backup-allowed-clusters.json");
    std::fs::write(
        &p,
        "{\"allowed_cluster_ids\": [\"SCRATCH-CLUSTER-NOT-THE-SOURCE\"]}\n",
    )
    .expect("written");
    p
}

fn restore_allowlist() -> PathBuf {
    let p = demo_dir().join("fx21-restore-allowed-clusters.json");
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

/// What one `logweir backup run` printed and signed, and the manifest the
/// archive holds.
struct Backup {
    backup_id: String,
    receipt_key: String,
    receipt_bytes: Vec<u8>,
    receipt: BackupReceipt,
    manifest: Value,
}

fn backup(backup_id: &str, topics: &[String]) -> Backup {
    let s = stack();
    let bootstrap = s.source_bootstrap.replace(',', ", ");
    let spec = demo_dir().join(format!("{backup_id}-backup.yaml"));
    std::fs::write(
        &spec,
        format!(
            "backup_id: {backup_id}\n\
             source:\n\
             \x20 bootstrap_servers: [{bootstrap}]\n\
             \x20 topics: [{}]\n\
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
    let (manifest_bytes, _) = store
        .get(&receipt.archive.manifest_key)
        .unwrap_or_else(|e| panic!("read {}: {e}", receipt.archive.manifest_key));
    let manifest: Value = serde_json::from_slice(&manifest_bytes).expect("the engine's manifest");
    Backup {
        backup_id: backup_id.to_string(),
        receipt_key,
        receipt_bytes,
        receipt,
        manifest,
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

/// The manifest's `source_replication_factor` for `topic`: `None` when the
/// manifest does not record one (absent, `null`).
fn manifest_rf(manifest: &Value, topic: &str) -> Option<i64> {
    manifest["topics"]
        .as_array()
        .expect("the manifest lists topics")
        .iter()
        .find(|t| t["name"] == topic)
        .unwrap_or_else(|| panic!("the manifest names {topic}: {manifest}"))
        .get("source_replication_factor")
        .and_then(Value::as_i64)
}

fn mint_approval(spec_text: &str, approver_pem: &Path, label: &str) -> PathBuf {
    let doc = json!({
        "approver": "fx21-e2e@example.com",
        "ticket": "FX-21",
        "plan_hash": logweir_core::ids::sha256_prefixed(spec_text.as_bytes()),
        "approved_at": "2026-10-08T00:00:00Z",
    });
    let bytes = serde_json::to_vec_pretty(&doc).expect("serialises");
    let p = demo_dir().join(format!("fx21-{label}-approval.json"));
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

/// The trust a point-bound restore anchors the receipt's signature in.
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
    let p = demo_dir().join("fx21-evidence-keys.json");
    std::fs::write(&p, serde_json::to_vec(&ring).expect("serialises")).expect("written");
    p
}

fn rfc3339(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .expect("representable")
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// A `newTopic` restore spec over the backup's topics, naming its targets
/// `<prefix><source>`, bound to the point or not.
fn restore_spec(b: &Backup, topics: &[String], prefix: &str, bound: bool) -> String {
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
         \x20 mode: newTopic\n\
         \x20 topic_mapping_prefix: \"drill-\"\n\
         \x20 topic_naming:\n\
         \x20   prefix: \"{prefix}\"\n\
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
        topics = topics.join(", "),
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

fn restore(binary: &Path, spec_text: &str, label: &str, bound: bool) -> Restore {
    let spec = demo_dir().join(format!("fx21-{label}.yaml"));
    std::fs::write(&spec, spec_text).expect("written");
    let approver = demo_dir().join("fx21-approver.pem");
    if !approver.exists() {
        let sk = logweir_evidence::keys::SigningKey::generate_p256();
        std::fs::write(&approver, sk.to_pkcs8_pem().expect("pem")).expect("written");
    }
    let approver_pub = demo_dir().join("fx21-approver.pub.pem");
    let sk = logweir_evidence::keys::SigningKey::from_pem_file(&approver).expect("the approver");
    std::fs::write(
        &approver_pub,
        sk.verifying_key().to_public_key_pem().expect("pem"),
    )
    .expect("written");
    let approval = mint_approval(spec_text, &approver, label);
    let out_json = demo_dir().join(format!("fx21-{label}-scorecard.json"));
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
    let out = output_within(c, 900);
    let scorecard = std::fs::read(&out_json)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or(Value::Null);
    eprintln!(
        "[fx21] restore {label}: exit={:?} outcome={} format={}",
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

/// Both readers of THIS build over one signed scorecard, plus `drill show`'s
/// `topic parity` row.
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
            l.find("configuration parity: ")
                .map(|i| l[i..].trim_end().to_string())
        })
    };
    json!({
        "rust_exit": rust.status.code(),
        "python_exit": py.status.code(),
        "rust_parity_line": line(&rust),
        "python_parity_line": line(&py),
        "drill_show_exit": show.status.code(),
        "drill_show_topic_parity_row": text(&show)
            .lines()
            .find(|l| l.contains("topic parity"))
            .map(|l| l.trim().to_string()),
    })
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

/// What an engine records in the manifest, judged by what it PRINTS — never
/// compared with a committed statement of the pin (`engine_matrix.rs`'s
/// `no_e2e_test_compares…`), so the row runs on every engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Records {
    /// Logweir's build from patch 0002 on: `<release>+logweir.<n>`, n ≥ 2.
    EveryTopicsFactor,
    /// The two builds FX-21 measured without the patch: OSO's 0.23.3 and
    /// Logweir's `0.23.3+logweir.1`.
    FirstSavedTopicsOnly,
    /// Any other engine: recorded, not asserted.
    NotAsserted,
}

fn records(version: &str) -> Records {
    if let Some((_, n)) = version.split_once("+logweir.") {
        if n.parse::<u32>().is_ok_and(|n| n >= 2) {
            return Records::EveryTopicsFactor;
        }
    }
    if version == "0.23.3" || version == "0.23.3+logweir.1" {
        return Records::FirstSavedTopicsOnly;
    }
    Records::NotAsserted
}

// ============================================================ the row

/// **FX-21, live.** See the module doc's table for what it proves.
///
/// Negative controls, each a run that must fail an assertion below: the
/// phase 7 before FX-21 (`FX21_BEFORE_BIN` records it signing exactly that:
/// on an engine that records one factor, the other topics carry no
/// `replication_factor` entry anywhere — the "never neither" assertion fails
/// for it, which this row asserts of THAT document); an engine without patch
/// 0002 claiming a `+logweir.<n>` build of 2 or more (the every-factor
/// assertion fails, as it does on `0.23.3+logweir.1`, where this row asserts
/// the opposite); and a reader that drops the not-assessed entries from its
/// parity line.
#[test]
#[ignore = "needs the stack's `cluster3` profile; see the module doc"]
fn a_multi_topic_backup_never_reads_an_unrecorded_replication_factor_as_matching() {
    let s = stack();
    use_stack_s3_env();
    let version = engine_version();
    let expect = records(&version);
    eprintln!("[fx21] engine {version}: {expect:?}");
    let n = nonce();
    // 1. THE SOURCE: three topics on `cluster3`, at factors the target (one
    //    broker, the plan's 1) cannot match.
    let sources: Vec<(String, u32)> = [("a", 3), ("b", 2), ("c", 3)]
        .into_iter()
        .map(|(t, rf)| (format!("fx21-{n}-{t}"), rf))
        .collect();
    let names: Vec<String> = sources.iter().map(|(t, _)| t.clone()).collect();
    let mut broker = BTreeMap::new();
    for (topic, rf) in &sources {
        create_source_topic(topic, *rf);
        produce(topic, 10);
        let read = broker_rf(&s.source_service, &s.source_in_network, topic);
        assert_eq!(read, Some(i64::from(*rf)), "{topic} on the broker");
        broker.insert(topic.clone(), *rf);
    }

    // 2. ONE BACKUP of all three. The receipt's own factor (Logweir's metadata
    //    read, PROD-05.1) is the broker's for every topic; the manifest's is
    //    whatever this engine records.
    let b = backup(&format!("fx21-{n}"), &names);
    let receipt_rf: BTreeMap<String, Option<u32>> = names
        .iter()
        .map(|t| {
            let f = b
                .receipt
                .topic_configuration
                .as_ref()
                .and_then(|m| m.get(t))
                .and_then(|c| c.replication_factor);
            (t.clone(), f)
        })
        .collect();
    for t in &names {
        assert_eq!(
            receipt_rf[t],
            Some(broker[t]),
            "{t}: the receipt records the broker's factor"
        );
    }
    let manifest: BTreeMap<String, Option<i64>> = names
        .iter()
        .map(|t| (t.clone(), manifest_rf(&b.manifest, t)))
        .collect();
    let recorded = manifest.values().filter(|f| f.is_some()).count();
    for (t, f) in &manifest {
        if let Some(f) = f {
            assert_eq!(
                *f,
                i64::from(broker[t]),
                "{t}: a recorded factor is the broker's"
            );
        }
    }
    match expect {
        Records::EveryTopicsFactor => assert_eq!(
            recorded,
            names.len(),
            "patch 0002: the manifest records every topic's factor: {manifest:?}"
        ),
        Records::FirstSavedTopicsOnly => assert_eq!(
            recorded, 1,
            "the defect patch 0002 fixes: one topic's factor only: {manifest:?}"
        ),
        Records::NotAsserted => {}
    }

    // 3. THE UNBOUND RESTORE: phase 7 knows only the manifest.
    let nt_prefix = format!("fx21-nt-{n}-");
    let un = restore(
        &bin(),
        &restore_spec(&b, &names, &nt_prefix, false),
        "unbound",
        false,
    );
    assert_eq!(un.out.status.code(), Some(0), "{}", text(&un.out));
    assert_eq!(un.scorecard["target"]["mode"], "newTopic");
    let p = &un.scorecard["topic_parity"];
    let not_assessed = strings(&p["not_assessed"]);
    let unexpected = strings(&p["unexpected_divergence"]);
    let not_reconstructed = strings(&p["not_reconstructed"]);
    let mut unassessed_rf = Vec::new();
    for t in &names {
        let tgt = format!("{nt_prefix}{t}");
        let deviation = format!("{tgt}: replication_factor");
        let entry = format!("{tgt}: replication_factor (notRecorded)");
        let twin = format!("{tgt}: replication_factor not assessed (notRecorded)");
        let compared = not_reconstructed.contains(&deviation);
        let named = not_assessed.contains(&entry);
        // THE RULE: the target's factor (1) is not the source's (3 or 2), so
        // silence here is exactly the false match FX-21 closes.
        assert!(
            compared != named,
            "{t}: its factor must be a deviation or not assessed, never neither and \
             never both: {p}"
        );
        assert_eq!(
            compared,
            manifest[t].is_some(),
            "{t}: compared exactly where the manifest records it: {p}"
        );
        if named {
            assert!(unexpected.contains(&twin), "{t}: the fail-safe twin: {p}");
            unassessed_rf.push(entry);
        } else {
            assert!(unexpected.contains(&deviation), "{t}: {p}");
        }
    }
    match expect {
        Records::EveryTopicsFactor => assert!(unassessed_rf.is_empty(), "{p}"),
        Records::FirstSavedTopicsOnly => assert_eq!(unassessed_rf.len(), 2, "{p}"),
        Records::NotAsserted => {}
    }
    let un_readers = readers(&un);

    // 4. THE BOUND RESTORE: the verified receipt's factor stands where the
    //    manifest records none, so every topic's factor is compared.
    let bd_prefix = format!("fx21-bd-{n}-");
    let bd = restore(
        &bin(),
        &restore_spec(&b, &names, &bd_prefix, true),
        "bound",
        true,
    );
    assert_eq!(bd.out.status.code(), Some(0), "{}", text(&bd.out));
    let p = &bd.scorecard["topic_parity"];
    let bd_not_reconstructed = strings(&p["not_reconstructed"]);
    for t in &names {
        let tgt = format!("{bd_prefix}{t}");
        assert!(
            bd_not_reconstructed.contains(&format!("{tgt}: replication_factor")),
            "{t}: bound to the point, its factor is compared: {p}"
        );
    }
    assert!(
        !strings(&p["not_assessed"])
            .iter()
            .any(|e| e.contains("replication_factor")),
        "{p}"
    );
    let bd_readers = readers(&bd);

    // 5. THE DEFECT ITSELF, when a pre-FX-21 `logweir` is available: the same
    //    unbound restore, signed by the writer before this change.
    let before = std::env::var("FX21_BEFORE_BIN").ok().map(PathBuf::from);
    let old_prefix = format!("fx21-old-{n}-");
    let old = before.as_ref().map(|binary| {
        let old = restore(
            binary,
            &restore_spec(&b, &names, &old_prefix, false),
            "before-fx21",
            false,
        );
        assert_eq!(old.out.status.code(), Some(0), "{}", text(&old.out));
        let p = &old.scorecard["topic_parity"];
        let all: Vec<String> = ["not_assessed", "unexpected_divergence", "not_reconstructed"]
            .iter()
            .flat_map(|k| strings(&p[*k]))
            .collect();
        let silent: Vec<&String> = names
            .iter()
            .filter(|t| {
                let tgt = format!("{old_prefix}{t}");
                !all.iter()
                    .any(|e| e.starts_with(&format!("{tgt}: replication_factor")))
            })
            .collect();
        // Every topic whose factor the manifest lacks is SILENT in the old
        // writer's document: signed as no replication-factor divergence.
        let unrecorded: Vec<&String> = names.iter().filter(|t| manifest[*t].is_none()).collect();
        assert_eq!(silent, unrecorded, "the writer before FX-21: {p}");
        let readers = readers(&old);
        (old, readers)
    });

    // 6. Both readers of this build over every document of this build.
    for (name, r, unassessed) in [
        ("unbound", &un_readers, &unassessed_rf),
        ("bound", &bd_readers, &Vec::new()),
    ] {
        assert_eq!(r["rust_exit"], 0, "{name}: {r}");
        assert_eq!(r["python_exit"], 0, "{name}: {r}");
        assert_eq!(
            r["rust_parity_line"], r["python_parity_line"],
            "{name}: {r}"
        );
        let line = r["rust_parity_line"].as_str().unwrap_or("");
        for entry in unassessed {
            assert!(line.contains(entry.as_str()), "{name}: {entry}: {r}");
        }
        if unassessed.is_empty() {
            assert!(!line.contains("replication_factor"), "{name}: {r}");
        }
    }

    // 7. The evidence, then clean up what this row created.
    write_evidence(
        &version,
        &json!({
            "engine_version": version,
            "engine_digest": engine_digest(),
            "expectation": format!("{expect:?}"),
            "source_topics_rf_on_the_broker": broker,
            "backup_id": b.backup_id,
            "receipt_key": b.receipt_key,
            "receipt_format_version": b.receipt.format_version,
            "receipt_topic_configuration_rf": receipt_rf,
            "manifest_source_replication_factor": manifest,
            "manifest_topics_with_a_factor": recorded,
            "unbound": {
                "exit": un.out.status.code(),
                "format_version": un.scorecard["format_version"],
                "outcome": un.scorecard["outcome"],
                "topic_parity": un.scorecard["topic_parity"],
                "replication_factor_not_assessed": unassessed_rf,
                "readers": un_readers,
            },
            "bound": {
                "exit": bd.out.status.code(),
                "format_version": bd.scorecard["format_version"],
                "outcome": bd.scorecard["outcome"],
                "topic_parity": bd.scorecard["topic_parity"],
                "readers": bd_readers,
            },
            "before_fx21": old.as_ref().map(|(o, r)| json!({
                "binary": before.as_ref().map(|p| p.display().to_string()),
                "exit": o.out.status.code(),
                "format_version": o.scorecard["format_version"],
                "topic_parity": o.scorecard["topic_parity"],
                "readers_of_this_build": r,
            })),
        }),
    );
    for t in &names {
        for prefix in [&nt_prefix, &bd_prefix, &old_prefix] {
            delete_topic(
                &s.target_service,
                &s.target_in_network,
                &format!("{prefix}{t}"),
            );
        }
        delete_topic(&s.source_service, &s.source_in_network, t);
    }
}
