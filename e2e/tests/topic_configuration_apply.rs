#![cfg(feature = "e2e")]
//! **PROD-05.2 — a restore's target topic configuration, applied live**
//! (design `docs/to-do/decisions/PROD-04.2-cutover.md` §4 and §8.1, rows C20
//! to C27). Every row backs topics up with `logweir backup run`, binds a
//! restore to that point with `target.topic_configuration`, runs `logweir
//! restore run` with the pinned engine and complete coverage, and reads the
//! BROKERS: a topic's dynamic overrides and replication factor, read after the
//! run, are the oracle, and the signed scorecard and the runner's
//! `topic-configuration=` line are held to them.
//!
//! | row | design | proves | negative control in the row |
//! |---|---|---|---|
//! | `c20_c24_a_3_9_point_applies_its_model_and_a_removed_key_on_3_9` | C20, C24 (3.9) | on `--kafka 3.9`: the topics are created from the model (partitions, factor and reason), `message.format.version` (removedInKafka4) is applied to the 3.x target, and after the passing verdict each topic holds exactly its desired set (compaction, the source's retention); writes the hand-off the 4.3 row restores | — (the 4.3 row is the line's control) |
//! | `c20_c24_c25_a_3_9_point_restored_to_4_3_before_its_transition` | C20, C24 (4.3), C25 row 7 | on `--kafka 4.3`, from the 3.9 point (a filesystem archive that outlives the slot): with `transition: none` the topic reads Logweir's during-restore set plus the source's at-creation set (`cleanup.policy=delete`, `retention.ms=-1`); the removed key is NOT applied and is named `removedInKafka4`; the inherited `message.timestamp.after.max.ms` (Long.MAX on 3.9) is named against the 4.x target's own default before anything is created | no block: created exactly as today (`message.timestamp.type`, `retention.ms` and nothing else) |
//! | `c21_fewer_target_brokers_bound_the_factor_and_cap_min_insync` | C21 | `cluster3` (factor 3, `min.insync.replicas=2`) to the one-broker target: factor 1 with reason `targetBrokers`, `min.insync.replicas` capped to 1 and stated; the topic accepts `acks=all` | a topic created with `min.insync.replicas=2` at factor 1 refuses `acks=all` (`NOT_ENOUGH_REPLICAS`) |
//! | `c22_compaction_is_applied_after_a_complete_verification` | C22 | a compacted source: the complete verification passes with `offset_holes` disclosed, then `cleanup.policy=compact`; the topic is readable before (records restored) and after (every record consumed) | the unit mutant that applies a deferred key at creation (M1) |
//! | `c23_old_timestamps_restore_under_a_bounded_broker` | C23 | with the target broker's default `log.message.timestamp.before.max.ms` at one hour, 30-day-old records restore and pass; the signed deferred `retention.ms` (10 days) is what the transition applies, and it deletes every restored record (the preview's arithmetic, over the signed block) | the same point without the block: phase 0's `TargetTopicConfigRefused` (today's), exit 3 |
//! | `c25_settings_never_applied_are_listed_and_a_denied_read_is_not_recorded` | C25 rows 2 and 6 | `clusterBound` and `inherited` entries are never applied and are listed with the source's value; a `captureDenied` topic is created with the safe settings only and marked NOT RECORDED | a topic with no override lists only inherited entries |
//! | `c26_an_owned_topic_is_exported_and_its_neighbour_transitioned` | C26 | a Strimzi-owned topic (`--kafka-topic-resources` at backup) is created, its `KafkaTopic` YAML is put and bound by digest, and it is not altered; its unowned neighbour is transitioned; `owner_detection: []` with no statement is `TopicOwnerNotChecked`, nothing created; `owners: []` proceeds | the refusal |
//! | `c27_a_partial_transition_names_what_was_not_applied` | C27 | on `acl`, the restore principal denied `AlterConfigs` on one topic: the run passes, the line, the status shape and the signed attestation say `applied [A]`, `failed [B]` with B's whole desired set; a topic the run did not create keeps its overrides byte for byte | the read-back: without it B would be reported applied |
//!
//! # Running it
//!
//! ```text
//! eval "$(e2e/compose/stack-env.sh --slot N --kafka 3.9)"
//! just e2e-up
//! LOGWEIR_PYTHON=<python3 with cryptography> AWS_EC2_METADATA_DISABLED=true \
//!   cargo test -p e2e --features e2e --test topic_configuration_apply -- --ignored \
//!   --test-threads=1 --nocapture c20_c24_a_3_9
//! just e2e-down
//! eval "$(e2e/compose/stack-env.sh --slot N --kafka 4.3 --profiles cluster3,acl)"
//! just e2e-up
//! … the same command, every other row (the 4.3 row reads the 3.9 row's hand-off)
//! just e2e-down
//! ```
//!
//! Each row writes what it observed to `topic-configuration-apply/<row>.json`
//! under the stack's scratch directory (`harness::demo_dir()`).
mod harness;

use harness::{bin, demo_dir, engine_bin, engine_digest, engine_mount, engine_version, root};
use logweir_core::backup_receipt::BackupReceipt;
use logweir_kafka::reader::{AuthConfig, ClusterReader, ConfigSourceKind};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const LONG_MAX: &str = "9223372036854775807";

// ============================================================ the stack

/// One cluster of the stack.
#[derive(Clone)]
struct Cluster {
    service: String,
    profile: Option<&'static str>,
    in_network: String,
    bootstrap: Vec<String>,
}

struct Stack {
    compose_project: String,
    broker: Cluster,
    c3: Cluster,
    acl: Cluster,
    acl_sasl: String,
    s3_endpoint: String,
    s3_user: String,
    s3_secret: String,
}

/// THE ONE PLACE this file reads the stack's addresses.
fn stack() -> Stack {
    harness::stack::ensure_coherent();
    let minio = ["minio", "admin"].concat();
    Stack {
        compose_project: harness::stack::project(),
        broker: Cluster {
            service: "kafka-broker-1".into(),
            profile: None,
            in_network: "kafka-broker-1:9094".into(),
            bootstrap: vec![harness::bootstrap()],
        },
        c3: Cluster {
            service: "kafka-c3-1".into(),
            profile: Some("cluster3"),
            in_network: "kafka-c3-1:9094".into(),
            bootstrap: harness::bootstrap_c3()
                .split(',')
                .map(str::to_string)
                .collect(),
        },
        acl: Cluster {
            service: "kafka-acl".into(),
            profile: Some("acl"),
            in_network: "kafka-acl:9094".into(),
            bootstrap: vec![harness::bootstrap_acl()],
        },
        acl_sasl: harness::bootstrap_acl_sasl(),
        s3_endpoint: harness::s3_endpoint(),
        s3_user: minio.clone(),
        s3_secret: minio,
    }
}

/// `3.9` or `4.x`, from the slot's broker line.
fn line() -> &'static str {
    let v = std::env::var("KAFKA_VERSION").unwrap_or_default();
    if v.starts_with("3.9") {
        "3.9"
    } else if v.starts_with("4.") {
        "4.x"
    } else {
        panic!("KAFKA_VERSION is {v:?}: start the slot with `--kafka 3.9` or `--kafka 4.3`")
    }
}

fn require_line(want: &str) {
    assert_eq!(
        line(),
        want,
        "this row runs on the {want} line (the module doc's two sessions)"
    );
}

// ============================================================ processes

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
fn kafka_cli(c: &Cluster, args: &[&str]) -> Output {
    let s = stack();
    let mut cmd = Command::new("docker");
    cmd.args([
        "compose",
        "-p",
        &s.compose_project,
        "-f",
        "e2e/compose/docker-compose.yml",
    ]);
    if let Some(p) = c.profile {
        cmd.args(["--profile", p]);
    }
    cmd.args(["exec", "-T", &c.service])
        .args(args)
        .current_dir(root());
    output_within(cmd, 120)
}

fn kafka_cli_ok(c: &Cluster, args: &[&str], what: &str) -> String {
    let o = kafka_cli(c, args);
    assert!(o.status.success(), "{what}:\n{}", text(&o));
    String::from_utf8_lossy(&o.stdout).into_owned()
}

fn nonce() -> String {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970")
        .as_nanos();
    format!("{:08}", n % 100_000_000)
}

// ============================================================ the clusters

fn reader(c: &Cluster) -> logweir_kafka::rdkafka_reader::RdKafkaReader {
    logweir_kafka::rdkafka_reader::RdKafkaReader::connect(&c.bootstrap, AuthConfig::Plaintext)
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

fn create_topic(c: &Cluster, topic: &str, partitions: u32, rf: u32, configs: &[(&str, &str)]) {
    let partitions_s = partitions.to_string();
    let rf_s = rf.to_string();
    let mut args: Vec<String> = [
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        &c.in_network,
        "--create",
        "--topic",
        topic,
        "--partitions",
        &partitions_s,
        "--replication-factor",
        &rf_s,
    ]
    .iter()
    .map(|s| (*s).to_string())
    .collect();
    for (k, v) in configs {
        args.push("--config".into());
        args.push(format!("{k}={v}"));
    }
    let borrowed: Vec<&str> = args.iter().map(String::as_str).collect();
    kafka_cli_ok(c, &borrowed, &format!("create {topic}"));
    harness::await_created_on(&c.bootstrap.join(","), topic, partitions as i32);
}

/// `count` keyed records (four keys), `acks=all`, timestamped `ts_ms` when
/// given.
fn produce(c: &Cluster, topic: &str, count: usize, ts_ms: Option<i64>) {
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    let producer: BaseProducer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", c.bootstrap.join(","))
        .set("message.timeout.ms", "20000")
        .set("acks", "all")
        .create()
        .expect("a producer");
    for i in 0..count {
        let payload = format!("{topic}-{i}");
        let key = format!("k{}", i % 4);
        loop {
            let mut record = BaseRecord::to(topic).key(&key).payload(&payload);
            if let Some(ts) = ts_ms {
                record = record.timestamp(ts + i as i64);
            }
            match producer.send(record) {
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

/// One `acks=all` record with no retry, and the broker's answer: `Err` names
/// the delivery error (`NotEnoughReplicas` for a topic whose
/// `min.insync.replicas` exceeds its in-sync replicas).
fn produce_one_acks_all(c: &Cluster, topic: &str) -> Result<(), String> {
    use rdkafka::producer::{BaseProducer, BaseRecord, DeliveryResult, Producer, ProducerContext};
    use std::sync::{Arc, Mutex};
    struct Answer(Arc<Mutex<Option<Result<(), String>>>>);
    impl rdkafka::ClientContext for Answer {}
    impl ProducerContext for Answer {
        type DeliveryOpaque = ();
        fn delivery(&self, result: &DeliveryResult<'_>, _: ()) {
            *self.0.lock().unwrap() = Some(match result {
                Ok(_) => Ok(()),
                Err((e, _)) => Err(e.to_string()),
            });
        }
    }
    let answer = Arc::new(Mutex::new(None));
    let producer: BaseProducer<Answer> = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", c.bootstrap.join(","))
        .set("message.timeout.ms", "15000")
        .set("acks", "all")
        .set("retries", "0")
        .create_with_context(Answer(Arc::clone(&answer)))
        .expect("a producer");
    producer
        .send(BaseRecord::to(topic).key("k").payload("v"))
        .map_err(|(e, _)| e.to_string())?;
    let _ = producer.flush(Duration::from_secs(20));
    let got = answer.lock().unwrap().take();
    got.unwrap_or_else(|| Err("no delivery report within 20 s".to_string()))
}

/// The topic's DYNAMIC (topic-level) overrides, as the broker reports them.
fn overrides(c: &Cluster, topic: &str) -> BTreeMap<String, String> {
    let answers = reader(c)
        .describe_topic_configs(&[topic.to_string()])
        .expect("DescribeConfigs");
    let (_, read) = answers.into_iter().next().expect("one answer");
    read.expect("the topic's configuration")
        .into_iter()
        .filter(|e| e.source == ConfigSourceKind::DynamicTopicConfig)
        .map(|e| (e.name, e.value.unwrap_or_default()))
        .collect()
}

fn replication_factor(c: &Cluster, topic: &str) -> u32 {
    reader(c)
        .replication_factors(&[topic.to_string()])
        .expect("metadata")[topic]
}

fn record_count(c: &Cluster, topic: &str) -> i64 {
    reader(c)
        .end_offsets(topic)
        .expect("end offsets")
        .iter()
        .map(|(_, end)| *end)
        .sum()
}

fn the_during_restore_set(line: &str) -> BTreeMap<String, String> {
    let mut m: BTreeMap<String, String> = [
        ("message.timestamp.type", "CreateTime"),
        ("retention.ms", "-1"),
        ("retention.bytes", "-1"),
        ("cleanup.policy", "delete"),
        ("message.timestamp.before.max.ms", LONG_MAX),
        ("message.timestamp.after.max.ms", LONG_MAX),
    ]
    .iter()
    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
    .collect();
    if line == "3.9" {
        m.insert(
            "message.timestamp.difference.max.ms".into(),
            LONG_MAX.into(),
        );
    }
    m
}

fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

// ============================================================ evidence

fn evidence_dir() -> PathBuf {
    demo_dir().join("topic-configuration-apply")
}

fn write_evidence(row: &str, v: &Value) {
    let dir = evidence_dir();
    std::fs::create_dir_all(&dir).expect("the evidence directory");
    let p = dir.join(format!("{row}.json"));
    std::fs::write(&p, serde_json::to_vec_pretty(v).expect("serialises")).expect("written");
    eprintln!("[prod-05-2] evidence: {}", p.display());
}

fn signing_pem() -> PathBuf {
    root().join("e2e/fixtures/signed/signing.pem")
}

fn engine_env(c: &mut Command) {
    let s = stack();
    c.env("AWS_ACCESS_KEY_ID", &s.s3_user)
        .env("AWS_SECRET_ACCESS_KEY", &s.s3_secret)
        .env("AWS_REGION", "us-east-1")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .env("LOGWEIR_ENGINE_BIN", engine_bin())
        .env("LOGWEIR_ENGINE_VERSION", engine_version())
        .env("LOGWEIR_ENGINE_DIGEST", engine_digest())
        .env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount())
        .env("TMPDIR", engine_mount());
}

fn use_stack_s3_env() {
    let s = stack();
    std::env::set_var("AWS_ACCESS_KEY_ID", s.s3_user);
    std::env::set_var("AWS_SECRET_ACCESS_KEY", s.s3_secret);
    std::env::set_var("AWS_REGION", "us-east-1");
    std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
}

// ============================================================ the pipeline

/// Where an archive lives.
#[derive(Clone)]
enum Archive {
    /// The stack's MinIO, under the backup id.
    S3,
    /// A host directory the engine sees at the same path: it outlives the
    /// slot, so a 3.9 point can be restored on 4.3.
    Fs(PathBuf),
}

impl Archive {
    fn storage_yaml(&self, backup_id: &str, indent: &str) -> String {
        match self {
            Archive::S3 => {
                let s = stack();
                format!(
                    "{indent}backend: s3\n{indent}bucket: {}\n{indent}prefix: {backup_id}\n\
                     {indent}region: us-east-1\n{indent}endpoint: {}\n{indent}path_style: true\n\
                     {indent}allow_http: true\n",
                    harness::ARCHIVE_BUCKET,
                    s.s3_endpoint
                )
            }
            Archive::Fs(path) => {
                format!(
                    "{indent}backend: filesystem\n{indent}path: \"{}\"\n",
                    path.display()
                )
            }
        }
    }

    fn store(&self, backup_id: &str) -> logweir_engine_oso::storage::Store {
        use_stack_s3_env();
        let url: logweir_core::engine::StorageUrl =
            serde_yaml::from_str(&self.storage_yaml(backup_id, "")).expect("a storage url");
        logweir_engine_oso::storage::Store::read_only_from_url(&url).expect("the archive store")
    }
}

struct Backup {
    backup_id: String,
    archive: Archive,
    receipt_key: String,
    receipt_bytes: Vec<u8>,
    receipt: BackupReceipt,
}

/// `logweir backup run` of `topics` on `source`, as its super user unless
/// `scram` (the acl cluster's restricted principal).
fn backup(
    backup_id: &str,
    source: &Cluster,
    topics: &[&str],
    archive: Archive,
    scram: bool,
    kafka_topics: Option<&Path>,
) -> Backup {
    let s = stack();
    let (bootstrap, auth) = if scram {
        (
            s.acl_sasl.clone(),
            format!(
                "\x20 auth:\n\x20   mode: scramSha512\n\x20   username: {}\n",
                harness::SCRAM_USER
            ),
        )
    } else {
        (source.bootstrap.join(", "), String::new())
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
             storage:\n{}\
             backup:\n\
             \x20 compression: zstd\n\
             \x20 segment_max_records: 1000\n\
             \x20 segment_max_bytes: 10485760\n\
             \x20 max_concurrent_partitions: 3\n",
            topics.join(", "),
            archive.storage_yaml(backup_id, "  "),
        ),
    )
    .expect("the backup spec");
    let allow = demo_dir().join("prod052-backup-allowed-clusters.json");
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
    if let Some(file) = kafka_topics {
        c.arg("--kafka-topic-resources")
            .arg(file)
            .args(["--strimzi-cluster", "prod"]);
    }
    engine_env(&mut c);
    if scram {
        c.env("LOGWEIR_SOURCE_PASSWORD", harness::SCRAM_PASSWORD);
    }
    let out = output_within(c, 900);
    assert_eq!(
        out.status.code(),
        Some(0),
        "logweir backup run {backup_id} must exit 0:\n{}",
        text(&out)
    );
    let receipt_key = String::from_utf8_lossy(&out.stdout)
        .lines()
        .find_map(|l| l.strip_prefix("receipt-key="))
        .map(str::to_string)
        .expect("backup run prints receipt-key=");
    let (receipt_bytes, _) = archive
        .store(backup_id)
        .get_capped(
            &receipt_key,
            logweir_engine_oso::storage::caps::SIGNED_DOCUMENT,
        )
        .unwrap_or_else(|e| panic!("read {receipt_key}: {e}"));
    let receipt: BackupReceipt = serde_json::from_slice(&receipt_bytes).expect("a receipt");
    Backup {
        backup_id: backup_id.to_string(),
        archive,
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
    let p = demo_dir().join("prod052-evidence-keys.json");
    std::fs::write(&p, serde_json::to_vec(&ring).expect("serialises")).expect("written");
    p
}

fn rfc3339(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .expect("representable")
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// A complete-coverage `newTopic` restore of `topics` from `b`, bound to its
/// point, into `bootstrap` under `prefix`. `block` is the YAML of
/// `target.topic_configuration` (indented under `target:`), or empty for
/// none; `auth` the target's auth block, or empty.
fn restore_spec(
    b: &Backup,
    topics: &[&str],
    bootstrap: &str,
    prefix: &str,
    block: &str,
    auth: &str,
) -> String {
    let s = stack();
    format!(
        "source:\n\
         \x20 storage:\n{storage}\
         \x20 backup: {id}\n\
         \x20 topics: [{topics}]\n\
         \x20 point:\n\
         \x20   point_id: {point_id}\n\
         \x20   receipt_key: {receipt_key}\n\
         \x20   receipt_sha256: \"{receipt_sha256}\"\n\
         \x20   manifest_sha256: \"{manifest_sha256}\"\n\
         target:\n\
         \x20 bootstrap_servers: [{bootstrap}]\n\
         {auth}\
         \x20 mode: newTopic\n\
         \x20 topic_mapping_prefix: \"drill-\"\n\
         \x20 topic_naming:\n\
         \x20   prefix: \"{prefix}\"\n\
         \x20 default_replication_factor: 1\n\
         {block}\
         sample:\n\
         \x20 window_start: \"{start}\"\n\
         \x20 window_end: \"{end}\"\n\
         \x20 records_per_partition: 10\n\
         \x20 anchor: head\n\
         \x20 coverage: complete\n\
         objectives:\n\
         \x20 rto_seconds: 3600\n\
         \x20 rpo_seconds: 315360000\n\
         \x20 pass_rate: 1.0\n\
         evidence:\n\
         \x20 backend: s3\n\
         \x20 bucket: {evidence}\n\
         \x20 prefix: logweir/\n\
         \x20 region: us-east-1\n\
         \x20 endpoint: {endpoint}\n\
         \x20 path_style: true\n\
         \x20 allow_http: true\n",
        storage = b.archive.storage_yaml(&b.backup_id, "    "),
        id = b.backup_id,
        topics = topics.join(", "),
        point_id = logweir::catalog::record::point_id(&b.receipt_bytes),
        receipt_key = b.receipt_key,
        receipt_sha256 = logweir_core::ids::sha256_prefixed(&b.receipt_bytes),
        manifest_sha256 = b.receipt.archive.manifest_sha256,
        start = rfc3339(b.receipt.covered.from_ms - 1000),
        end = rfc3339(b.receipt.covered.to_ms + 1000),
        evidence = harness::EVIDENCE_BUCKET,
        endpoint = s.s3_endpoint,
    )
}

/// `target.topic_configuration` under `target:`.
fn block(transition: &str, owners: Option<&str>) -> String {
    let mut b = format!("\x20 topic_configuration:\n\x20   transition: {transition}\n");
    if let Some(o) = owners {
        b.push_str(&format!("\x20   owners: {o}\n"));
    }
    b
}

fn approver() -> (PathBuf, PathBuf) {
    let pem = demo_dir().join("prod052-approver.pem");
    if !pem.exists() {
        let sk = logweir_evidence::keys::SigningKey::generate_p256();
        std::fs::write(&pem, sk.to_pkcs8_pem().expect("pem")).expect("written");
    }
    let public = demo_dir().join("prod052-approver.pub.pem");
    let sk = logweir_evidence::keys::SigningKey::from_pem_file(&pem).expect("the approver");
    std::fs::write(
        &public,
        sk.verifying_key().to_public_key_pem().expect("pem"),
    )
    .expect("written");
    (pem, public)
}

fn approval(spec_text: &str, label: &str) -> PathBuf {
    let (pem, _) = approver();
    let doc = json!({
        "approver": "prod052-e2e@example.com",
        "ticket": "PROD-05.2",
        "plan_hash": logweir_core::ids::sha256_prefixed(spec_text.as_bytes()),
        "approved_at": "2026-10-13T00:00:00Z",
    });
    let bytes = serde_json::to_vec_pretty(&doc).expect("serialises");
    let p = demo_dir().join(format!("prod052-{label}-approval.json"));
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

struct Restore {
    out: Output,
    scorecard: Value,
    scorecard_path: PathBuf,
}

impl Restore {
    fn code(&self) -> Option<i32> {
        self.out.status.code()
    }

    /// The runner's `topic-configuration=` line, parsed.
    fn line(&self) -> Value {
        let raw = String::from_utf8_lossy(&self.out.stdout)
            .lines()
            .rev()
            .find_map(|l| l.strip_prefix("topic-configuration=").map(str::to_string))
            .unwrap_or_else(|| panic!("no topic-configuration= line:\n{}", text(&self.out)));
        serde_json::from_str(&raw).expect("the line is JSON")
    }

    fn block(&self) -> &Value {
        &self.scorecard["target"]["topic_configuration"]
    }

    fn run_id(&self) -> String {
        self.scorecard["run_id"]
            .as_str()
            .expect("run_id")
            .to_string()
    }
}

/// `logweir restore run`, into the cluster whose id `target_id` is.
fn restore(spec_text: &str, label: &str, target_id: &str, scram: bool) -> Restore {
    let spec = demo_dir().join(format!("prod052-{label}.yaml"));
    std::fs::write(&spec, spec_text).expect("written");
    let (_, approver_pub) = approver();
    let allow = demo_dir().join(format!("prod052-{label}-allowed-clusters.json"));
    std::fs::write(
        &allow,
        serde_json::to_vec(&json!({
            "allowed_cluster_ids": [target_id],
            "source_cluster_id": null
        }))
        .expect("serialises"),
    )
    .expect("written");
    let out_json = demo_dir().join(format!("prod052-{label}-scorecard.json"));
    let _ = std::fs::remove_file(&out_json);
    let mut c = Command::new(bin());
    c.args(["restore", "run", "--spec"])
        .arg(&spec)
        .arg("--approval")
        .arg(approval(spec_text, label))
        .arg("--approver-key")
        .arg(&approver_pub)
        .arg("--allowed-clusters")
        .arg(&allow)
        .arg("--signing-key")
        .arg(signing_pem())
        .arg("--out")
        .arg(&out_json)
        .arg("--evidence-keys")
        .arg(evidence_keyring());
    engine_env(&mut c);
    if scram {
        c.env("LOGWEIR_TARGET_PASSWORD", harness::SCRAM_PASSWORD);
    }
    let out = output_within(c, 1200);
    let scorecard = std::fs::read(&out_json)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or(Value::Null);
    eprintln!(
        "[prod-05-2] restore {label}: exit={:?} outcome={} format={}",
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

/// Both readers over the signed scorecard.
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
    json!({"rust_exit": rust.status.code(), "python_exit": py.status.code()})
}

/// An object from the evidence bucket, read through `mc`.
fn evidence_object(key: &str) -> Vec<u8> {
    let o = harness::mc(&["cat", &format!("local/{}/{key}", harness::EVIDENCE_BUCKET)]);
    assert!(o.status.success(), "mc cat {key}:\n{}", text(&o));
    o.stdout
}

fn assert_passed(r: &Restore) {
    assert_eq!(r.code(), Some(0), "{}", text(&r.out));
    assert_eq!(r.scorecard["outcome"], "pass", "{}", r.scorecard);
    harness::assert_format_at_least(
        r.scorecard["format_version"].as_str().unwrap_or_default(),
        "1.10.0",
        "a document carrying target.topic_configuration",
    );
    let v = readers(r);
    assert_eq!(v["rust_exit"], 0, "{v}");
    assert_eq!(v["python_exit"], 0, "{v}");
}

fn not_applied<'a>(block: &'a Value, topic: &str, key: &str) -> &'a Value {
    block["topics"][topic]["not_applied"]
        .as_array()
        .unwrap_or_else(|| panic!("{topic} lists what it does not apply"))
        .iter()
        .find(|n| n["key"] == key)
        .unwrap_or_else(|| panic!("{topic} does not list {key}: {}", block["topics"][topic]))
}

fn handoff_path() -> PathBuf {
    evidence_dir().join("handoff-3.9.json")
}

// ============================================================ the rows

/// **C20 and C24 on 3.9**, and the 3.9 point the 4.3 row restores.
#[test]
#[ignore = "needs the compose stack on --kafka 3.9"]
fn c20_c24_a_3_9_point_applies_its_model_and_a_removed_key_on_3_9() {
    require_line("3.9");
    let s = stack();
    let n = nonce();
    let compact = format!("tc{n}-compact");
    let delete = format!("tc{n}-delete");
    create_topic(
        &s.broker,
        &compact,
        3,
        1,
        &[
            ("cleanup.policy", "compact"),
            ("min.compaction.lag.ms", "1000"),
            ("message.format.version", "3.0-IV1"),
        ],
    );
    create_topic(&s.broker, &delete, 2, 1, &[("retention.ms", "604800000")]);
    produce(&s.broker, &compact, 30, None);
    produce(&s.broker, &delete, 20, None);
    let backup_id = format!("prod052-39-{n}");
    let archive = Archive::Fs(engine_mount().join("prod052-archives").join(&backup_id));
    let b = backup(
        &backup_id,
        &s.broker,
        &[&compact, &delete],
        archive.clone(),
        false,
        None,
    );
    let prefix = format!("r39{n}-");
    let text_ = restore_spec(
        &b,
        &[&compact, &delete],
        &s.broker.bootstrap.join(", "),
        &prefix,
        &block("afterVerification", Some("[]")),
        "",
    );
    let r = restore(&text_, "c20-39", &cluster_id(&s.broker), false);
    assert_passed(&r);
    let tc = format!("{prefix}{compact}");
    let td = format!("{prefix}{delete}");
    let blk = r.block();
    assert_eq!(blk["target_line"], "3.x", "{blk}");
    assert_eq!(
        blk["topics"][&tc]["at_creation"]["message.format.version"], "3.0-IV1",
        "a removedInKafka4 override reaches a 3.x target at creation: {blk}"
    );
    assert_eq!(blk["topics"][&tc]["partitions"], 3);
    assert_eq!(
        blk["topics"][&tc]["replication_factor"],
        json!({"source": 1, "target": 1, "reason": "sourceFactor"})
    );
    let after_compact = overrides(&s.broker, &tc);
    let after_delete = overrides(&s.broker, &td);
    assert_eq!(
        after_compact,
        map(&[
            ("cleanup.policy", "compact"),
            ("message.format.version", "3.0-IV1"),
            ("min.compaction.lag.ms", "1000"),
        ]),
        "after the passing verdict the topic holds exactly its desired set"
    );
    assert_eq!(after_delete, map(&[("retention.ms", "604800000")]));
    assert_eq!(r.line()["transition"], "applied", "{}", r.line());
    std::fs::write(
        handoff_path(),
        serde_json::to_vec_pretty(&json!({
            "backup_id": backup_id,
            "archive": match &archive { Archive::Fs(p) => p.display().to_string(), Archive::S3 => String::new() },
            "receipt_key": b.receipt_key,
            "topics": [compact, delete],
        }))
        .expect("serialises"),
    )
    .expect("the hand-off");
    write_evidence(
        "c20_c24_a_3_9_point_applies_its_model_and_a_removed_key_on_3_9",
        &json!({
            "line": line(),
            "receipt_topic_configuration": b.receipt.topic_configuration,
            "signed_block": blk,
            "runner_line": r.line(),
            "overrides_after": {"compact": after_compact, "delete": after_delete},
            "readers": readers(&r),
        }),
    );
}

/// **C20, C24 and C25 row 7 on 4.3**, from the 3.9 point.
#[test]
#[ignore = "needs the compose stack on --kafka 4.3 and the 3.9 row's hand-off"]
fn c20_c24_c25_a_3_9_point_restored_to_4_3_before_its_transition() {
    require_line("4.x");
    let s = stack();
    let handoff: Value = serde_json::from_slice(
        &std::fs::read(handoff_path()).expect("run the 3.9 row first (the module doc)"),
    )
    .expect("the hand-off is JSON");
    let backup_id = handoff["backup_id"].as_str().unwrap().to_string();
    let archive = Archive::Fs(PathBuf::from(handoff["archive"].as_str().unwrap()));
    let receipt_key = handoff["receipt_key"].as_str().unwrap().to_string();
    let (receipt_bytes, _) = archive
        .store(&backup_id)
        .get_capped(
            &receipt_key,
            logweir_engine_oso::storage::caps::SIGNED_DOCUMENT,
        )
        .expect("the 3.9 receipt");
    let receipt: BackupReceipt = serde_json::from_slice(&receipt_bytes).expect("a receipt");
    let b = Backup {
        backup_id,
        archive,
        receipt_key,
        receipt_bytes,
        receipt,
    };
    let compact = handoff["topics"][0].as_str().unwrap().to_string();
    let delete = handoff["topics"][1].as_str().unwrap().to_string();
    let n = nonce();
    let target_id = cluster_id(&s.broker);
    let bootstrap = s.broker.bootstrap.join(", ");

    // `transition: none`: the topic keeps what it was created with.
    let prefix = format!("r43{n}-");
    let spec = restore_spec(
        &b,
        &[&compact, &delete],
        &bootstrap,
        &prefix,
        &block("none", Some("[]")),
        "",
    );
    let r = restore(&spec, "c20-43", &target_id, false);
    assert_passed(&r);
    let tc = format!("{prefix}{compact}");
    let blk = r.block().clone();
    assert_eq!(blk["target_line"], "4.x");
    let mut expected = the_during_restore_set("4.x");
    expected.insert("min.compaction.lag.ms".into(), "1000".into());
    let now = overrides(&s.broker, &tc);
    assert_eq!(
        now, expected,
        "before any transition the topic reads Logweir's during-restore set and the source's \
         at-creation set"
    );
    assert_eq!(now["cleanup.policy"], "delete");
    assert_eq!(now["retention.ms"], "-1");
    assert_eq!(
        reader(&s.broker).end_offsets(&tc).expect("offsets").len(),
        3
    );
    // C24 on 4.x: the removed key is named, never applied.
    let removed = not_applied(&blk, &tc, "message.format.version");
    assert_eq!(removed["reason"], "removedInKafka4", "{removed}");
    assert_eq!(removed["value"], "3.0-IV1");
    assert!(!now.contains_key("message.format.version"));
    // C25 row 7: the 4.0 default change, named before anything is created.
    let after_max = not_applied(&blk, &tc, "message.timestamp.after.max.ms");
    assert_eq!(after_max["portability"], "inherited");
    assert_eq!(after_max["value"], LONG_MAX);
    assert_eq!(after_max["target_default"], "3600000", "{after_max}");
    assert_eq!(r.line()["transition"], "notRun: transitionNone");

    // THE CONTROL: no block, created exactly as today.
    let plain_prefix = format!("p43{n}-");
    let plain_spec = restore_spec(&b, &[&compact, &delete], &bootstrap, &plain_prefix, "", "");
    let plain = restore(&plain_spec, "c20-43-plain", &target_id, false);
    assert_eq!(plain.code(), Some(0), "{}", text(&plain.out));
    let plain_now = overrides(&s.broker, &format!("{plain_prefix}{compact}"));
    assert_eq!(
        plain_now,
        map(&[
            ("message.timestamp.type", "CreateTime"),
            ("retention.ms", "-1")
        ]),
        "without the block a topic is created exactly as before"
    );
    assert!(plain.scorecard["target"]
        .get("topic_configuration")
        .is_none());
    write_evidence(
        "c20_c24_c25_a_3_9_point_restored_to_4_3_before_its_transition",
        &json!({
            "line": line(),
            "signed_block": blk,
            "runner_line": r.line(),
            "overrides_before_transition": now,
            "control_overrides": plain_now,
            "control_format_version": plain.scorecard["format_version"],
        }),
    );
}

/// **C21**: three brokers to one.
#[test]
#[ignore = "needs the compose stack on --kafka 4.3 with the cluster3 profile"]
fn c21_fewer_target_brokers_bound_the_factor_and_cap_min_insync() {
    require_line("4.x");
    let s = stack();
    let n = nonce();
    let topic = format!("tc{n}-rf3");
    create_topic(&s.c3, &topic, 2, 3, &[("min.insync.replicas", "2")]);
    produce(&s.c3, &topic, 20, None);
    let b = backup(
        &format!("prod052-c21-{n}"),
        &s.c3,
        &[&topic],
        Archive::S3,
        false,
        None,
    );
    let prefix = format!("r21{n}-");
    let spec = restore_spec(
        &b,
        &[&topic],
        &s.broker.bootstrap.join(", "),
        &prefix,
        &block("afterVerification", Some("[]")),
        "",
    );
    let r = restore(&spec, "c21", &cluster_id(&s.broker), false);
    assert_passed(&r);
    let target = format!("{prefix}{topic}");
    let t = &r.block()["topics"][&target];
    assert_eq!(
        t["replication_factor"],
        json!({"source": 3, "target": 1, "reason": "targetBrokers"})
    );
    assert_eq!(
        t["capped"]["min.insync.replicas"],
        json!({"source": "2", "applied": "1"})
    );
    assert_eq!(replication_factor(&s.broker, &target), 1);
    assert_eq!(overrides(&s.broker, &target)["min.insync.replicas"], "1");
    // The restored topic takes `acks=all` writes.
    assert_eq!(produce_one_acks_all(&s.broker, &target), Ok(()));

    // THE CONTROL: `min.insync.replicas=2` at factor 1 refuses `acks=all`.
    let control = format!("tc{n}-mis2");
    create_topic(&s.broker, &control, 1, 1, &[("min.insync.replicas", "2")]);
    let refused_text = match produce_one_acks_all(&s.broker, &control) {
        Ok(()) => String::new(),
        Err(e) => e,
    };
    assert!(
        refused_text.contains("NotEnoughReplicas") || refused_text.contains("NOT_ENOUGH_REPLICAS"),
        "acks=all into min.insync.replicas=2 at factor 1 must be refused: {refused_text:?}"
    );
    write_evidence(
        "c21_fewer_target_brokers_bound_the_factor_and_cap_min_insync",
        &json!({
            "signed_topic": t,
            "target_replication_factor": replication_factor(&s.broker, &target),
            "target_overrides": overrides(&s.broker, &target),
            "control_refusal": refused_text,
        }),
    );
}

/// **C22**: compaction after a complete verification.
#[test]
#[ignore = "needs the compose stack on --kafka 4.3"]
fn c22_compaction_is_applied_after_a_complete_verification() {
    require_line("4.x");
    let s = stack();
    let n = nonce();
    let topic = format!("tc{n}-compacted");
    create_topic(
        &s.broker,
        &topic,
        1,
        1,
        &[
            ("cleanup.policy", "compact"),
            ("segment.ms", "100"),
            ("min.cleanable.dirty.ratio", "0.01"),
            ("delete.retention.ms", "100"),
        ],
    );
    produce(&s.broker, &topic, 40, None);
    // Let the source's cleaner run, so the archive may hold offset holes: an
    // append rolls the segment, and the cleaner's backoff is 15 s.
    std::thread::sleep(Duration::from_secs(2));
    produce(&s.broker, &topic, 4, None);
    std::thread::sleep(Duration::from_secs(20));
    produce(&s.broker, &topic, 1, None);
    let b = backup(
        &format!("prod052-c22-{n}"),
        &s.broker,
        &[&topic],
        Archive::S3,
        false,
        None,
    );
    let prefix = format!("r22{n}-");
    let spec = restore_spec(
        &b,
        &[&topic],
        &s.broker.bootstrap.join(", "),
        &prefix,
        &block("afterVerification", Some("[]")),
        "",
    );
    let r = restore(&spec, "c22", &cluster_id(&s.broker), false);
    assert_passed(&r);
    let complete = &r.scorecard["integrity"]["verification"]["complete"];
    assert_eq!(complete["covered"], true, "{complete}");
    assert_eq!(complete["replay"]["missing"], 0, "{complete}");
    let holes = complete["archive"]["offset_holes"].clone();
    let target = format!("{prefix}{topic}");
    let restored = r.scorecard["sample"]["records_restored"].clone();
    let after = overrides(&s.broker, &target);
    assert_eq!(after["cleanup.policy"], "compact", "{after:?}");
    assert!(!after.contains_key("retention.ms"));
    // Readable after the transition (the target's own cleaner may now
    // compact what the verification already counted).
    let consumed = reader(&s.broker)
        .consume_range(&target, 0, 0, 10_000)
        .expect("consume")
        .len();
    assert!(consumed >= 4, "every key is still readable: {consumed}");
    assert!(record_count(&s.broker, &target) > 0);
    write_evidence(
        "c22_compaction_is_applied_after_a_complete_verification",
        &json!({
            "offset_holes": holes,
            "complete": complete,
            "records_restored": restored,
            "overrides_after": after,
            "consumed_after": consumed,
            "runner_line": r.line(),
        }),
    );
}

/// Puts the target broker's record-timestamp bound back on every exit path.
struct BoundGuard;

impl Drop for BoundGuard {
    fn drop(&mut self) {
        let s = stack();
        let _ = kafka_cli(
            &s.broker,
            &[
                "/opt/kafka/bin/kafka-configs.sh",
                "--bootstrap-server",
                &s.broker.in_network,
                "--entity-type",
                "brokers",
                "--entity-default",
                "--alter",
                "--delete-config",
                "log.message.timestamp.before.max.ms",
            ],
        );
    }
}

/// **C23**: old timestamps restore under a bounded broker.
#[test]
#[ignore = "needs the compose stack on --kafka 4.3 with the acl profile"]
fn c23_old_timestamps_restore_under_a_bounded_broker() {
    require_line("4.x");
    let s = stack();
    let n = nonce();
    let topic = format!("tc{n}-old");
    // On the acl cluster, whose bound is unchanged: the source accepts them.
    create_topic(&s.acl, &topic, 1, 1, &[("retention.ms", "864000000")]);
    let thirty_days_ago = chrono::Utc::now().timestamp_millis() - 30 * 86_400_000;
    produce(&s.acl, &topic, 20, Some(thirty_days_ago));
    let b = backup(
        &format!("prod052-c23-{n}"),
        &s.acl,
        &[&topic],
        Archive::S3,
        false,
        None,
    );
    let _guard = BoundGuard;
    kafka_cli_ok(
        &s.broker,
        &[
            "/opt/kafka/bin/kafka-configs.sh",
            "--bootstrap-server",
            &s.broker.in_network,
            "--entity-type",
            "brokers",
            "--entity-default",
            "--alter",
            "--add-config",
            "log.message.timestamp.before.max.ms=3600000",
        ],
        "bound the target broker's record timestamps to one hour",
    );
    let target_id = cluster_id(&s.broker);
    let bootstrap = s.broker.bootstrap.join(", ");

    // THE CONTROL: without the block, today's refusal.
    let plain = restore(
        &restore_spec(&b, &[&topic], &bootstrap, &format!("p23{n}-"), "", ""),
        "c23-plain",
        &target_id,
        false,
    );
    assert_eq!(plain.code(), Some(3), "{}", text(&plain.out));
    assert!(
        text(&plain.out).contains("refusal-reason=TargetTopicConfigRefused"),
        "{}",
        text(&plain.out)
    );

    let prefix = format!("r23{n}-");
    let r = restore(
        &restore_spec(
            &b,
            &[&topic],
            &bootstrap,
            &prefix,
            &block("afterVerification", Some("[]")),
            "",
        ),
        "c23",
        &target_id,
        false,
    );
    assert_passed(&r);
    let target = format!("{prefix}{topic}");
    let t: logweir_core::topic_transition::TopicDesired =
        serde_json::from_value(r.block()["topics"][&target].clone()).expect("the signed topic");
    let retention =
        logweir_core::topic_transition::retention_after_transition(&t, &BTreeMap::new())
            .expect("the source's retention is deferred");
    assert_eq!(retention, 864_000_000);
    let cutoff = chrono::Utc::now().timestamp_millis() - retention;
    assert!(
        thirty_days_ago + 20 < cutoff,
        "the transition's retention deletes every restored record"
    );
    assert_eq!(overrides(&s.broker, &target)["retention.ms"], "864000000");
    write_evidence(
        "c23_old_timestamps_restore_under_a_bounded_broker",
        &json!({
            "records_stamped_from_ms": thirty_days_ago,
            "control_exit": plain.code(),
            "signed_topic": r.block()["topics"][&target],
            "retention_after_transition_ms": retention,
            "deletes_records_before_ms": cutoff,
            "overrides_after": overrides(&s.broker, &target),
        }),
    );
}

/// The restricted principal's ACLs on one topic.
fn acl(topic: &str, args: &[&str], add: bool) -> Output {
    let s = stack();
    let who = format!("User:{}", harness::SCRAM_USER);
    let mut all = vec![
        "/opt/kafka/bin/kafka-acls.sh",
        "--bootstrap-server",
        &s.acl.in_network,
        if add { "--add" } else { "--remove" },
    ];
    if !add {
        all.push("--force");
    }
    let principal_flag = if args.contains(&"deny") {
        "--deny-principal"
    } else {
        "--allow-principal"
    };
    all.extend_from_slice(&[principal_flag, &who]);
    for a in args.iter().filter(|a| **a != "deny") {
        all.push(a);
    }
    all.extend_from_slice(&["--topic", topic]);
    kafka_cli(&s.acl, &all)
}

/// **C25 rows 2 and 6.**
#[test]
#[ignore = "needs the compose stack on --kafka 4.3 with the acl profile"]
fn c25_settings_never_applied_are_listed_and_a_denied_read_is_not_recorded() {
    require_line("4.x");
    let s = stack();
    let n = nonce();
    let throttled = format!("tc{n}-throttled");
    let plain = format!("tc{n}-plain");
    let denied = format!("tc{n}-denied");
    create_topic(
        &s.acl,
        &throttled,
        1,
        1,
        &[
            ("leader.replication.throttled.replicas", "*"),
            ("segment.bytes", "104857600"),
        ],
    );
    create_topic(&s.acl, &plain, 1, 1, &[]);
    create_topic(&s.acl, &denied, 2, 1, &[("cleanup.policy", "compact")]);
    for t in [&throttled, &plain, &denied] {
        produce(&s.acl, t, 10, None);
    }
    let added = acl(
        &denied,
        &["--operation", "Read", "--operation", "Describe"],
        true,
    );
    assert!(added.status.success(), "{}", text(&added));
    let b = backup(
        &format!("prod052-c25-{n}"),
        &s.acl,
        &[&throttled, &plain, &denied],
        Archive::S3,
        true,
        None,
    );
    let _ = acl(
        &denied,
        &["--operation", "Read", "--operation", "Describe"],
        false,
    );
    assert_eq!(
        b.receipt.config_coverage.as_ref().unwrap()[denied.as_str()].coverage,
        "captureDenied"
    );
    let prefix = format!("r25{n}-");
    let r = restore(
        &restore_spec(
            &b,
            &[&throttled, &plain, &denied],
            &s.broker.bootstrap.join(", "),
            &prefix,
            &block("afterVerification", Some("[]")),
            "",
        ),
        "c25",
        &cluster_id(&s.broker),
        false,
    );
    assert_passed(&r);
    let blk = r.block().clone();
    let t_throttled = format!("{prefix}{throttled}");
    let t_plain = format!("{prefix}{plain}");
    let t_denied = format!("{prefix}{denied}");
    let bound = not_applied(&blk, &t_throttled, "leader.replication.throttled.replicas");
    assert_eq!(bound["reason"], "clusterBound");
    assert_eq!(bound["value"], "*");
    assert!(
        !overrides(&s.broker, &t_throttled).contains_key("leader.replication.throttled.replicas")
    );
    assert_eq!(
        overrides(&s.broker, &t_throttled)["segment.bytes"],
        "104857600"
    );
    // THE CONTROL: no override, only inherited entries listed.
    for entry in blk["topics"][&t_plain]["not_applied"].as_array().unwrap() {
        assert_eq!(entry["reason"], "inherited", "{entry}");
    }
    assert!(blk["topics"][&t_plain]["at_creation"]
        .as_object()
        .unwrap()
        .is_empty());
    // Row 6: NOT RECORDED, safe settings only, and kept: an empty desired
    // set is never read as "no overrides".
    assert_eq!(blk["topics"][&t_denied]["not_recorded"], true, "{blk}");
    assert!(blk["topics"][&t_denied]["deferred"]
        .as_object()
        .unwrap()
        .is_empty());
    assert_eq!(
        overrides(&s.broker, &t_denied),
        the_during_restore_set("4.x"),
        "a topic whose configuration was not recorded keeps the safe settings"
    );
    let line_ = r.line();
    assert!(
        line_["failed"]
            .as_array()
            .unwrap()
            .iter()
            .any(|f| f["topic"] == t_denied.as_str() && f["error"] == "ConfigurationNotRecorded"),
        "{line_}"
    );
    write_evidence(
        "c25_settings_never_applied_are_listed_and_a_denied_read_is_not_recorded",
        &json!({"signed_block": blk, "runner_line": r.line()}),
    );
}

/// **C26**: owners.
#[test]
#[ignore = "needs the compose stack on --kafka 4.3"]
fn c26_an_owned_topic_is_exported_and_its_neighbour_transitioned() {
    require_line("4.x");
    let s = stack();
    let n = nonce();
    let owned = format!("tc{n}-owned");
    let free = format!("tc{n}-free");
    for t in [&owned, &free] {
        create_topic(&s.broker, t, 1, 1, &[("cleanup.policy", "compact")]);
        produce(&s.broker, t, 10, None);
    }
    let resources = demo_dir().join(format!("prod052-{n}-kafkatopics.yaml"));
    std::fs::write(
        &resources,
        format!(
            "apiVersion: v1\nkind: List\nitems:\n\
             - apiVersion: kafka.strimzi.io/v1beta2\n  kind: KafkaTopic\n  metadata:\n    \
             name: {owned}\n    namespace: kafka\n    labels:\n      strimzi.io/cluster: prod\n  \
             spec:\n    partitions: 1\n    replicas: 1\n"
        ),
    )
    .expect("written");
    let b = backup(
        &format!("prod052-c26-{n}"),
        &s.broker,
        &[&owned, &free],
        Archive::S3,
        false,
        Some(&resources),
    );
    assert_eq!(
        b.receipt.owner_detection,
        Some(vec!["kafkaTopicResources".to_string()])
    );
    let target_id = cluster_id(&s.broker);
    let bootstrap = s.broker.bootstrap.join(", ");
    let prefix = format!("r26{n}-");
    let r = restore(
        &restore_spec(
            &b,
            &[&owned, &free],
            &bootstrap,
            &prefix,
            &block("afterVerification", None),
            "",
        ),
        "c26",
        &target_id,
        false,
    );
    assert_passed(&r);
    let t_owned = format!("{prefix}{owned}");
    let t_free = format!("{prefix}{free}");
    let blk = r.block().clone();
    assert_eq!(blk["topics"][&t_owned]["route"], "desiredStateExport");
    assert_eq!(
        overrides(&s.broker, &t_owned),
        the_during_restore_set("4.x"),
        "an owned topic is never altered"
    );
    assert_eq!(overrides(&s.broker, &t_free)["cleanup.policy"], "compact");
    let reference = &blk["desired_state"];
    let doc = evidence_object(reference["key"].as_str().expect("the document's key"));
    assert_eq!(
        logweir_core::ids::sha256_prefixed(&doc),
        reference["sha256"].as_str().unwrap()
    );
    let doc = String::from_utf8(doc).expect("YAML");
    assert!(doc.contains("kind: KafkaTopic"), "{doc}");
    assert!(doc.contains(&format!("topicName: {t_owned}")), "{doc}");
    assert!(doc.contains("cleanup.policy: compact"), "{doc}");
    assert!(!doc.contains(&t_free), "{doc}");
    assert_eq!(r.line()["exported"], json!([t_owned]));
    assert_eq!(r.line()["applied"], json!([t_free]));

    // `owner_detection: []` and no statement: refused, nothing created.
    let b2 = backup(
        &format!("prod052-c26b-{n}"),
        &s.broker,
        &[&free],
        Archive::S3,
        false,
        None,
    );
    let unchecked_prefix = format!("u26{n}-");
    let unchecked = restore(
        &restore_spec(
            &b2,
            &[&free],
            &bootstrap,
            &unchecked_prefix,
            &block("afterVerification", None),
            "",
        ),
        "c26-unchecked",
        &target_id,
        false,
    );
    assert_eq!(unchecked.code(), Some(3), "{}", text(&unchecked.out));
    assert!(
        text(&unchecked.out).contains("TopicOwnerNotChecked"),
        "{}",
        text(&unchecked.out)
    );
    assert!(!topic_exists(
        &s.broker,
        &format!("{unchecked_prefix}{free}")
    ));
    // `owners: []` proceeds.
    let stated = restore(
        &restore_spec(
            &b2,
            &[&free],
            &bootstrap,
            &format!("s26{n}-"),
            &block("afterVerification", Some("[]")),
            "",
        ),
        "c26-stated",
        &target_id,
        false,
    );
    assert_passed(&stated);
    write_evidence(
        "c26_an_owned_topic_is_exported_and_its_neighbour_transitioned",
        &json!({
            "receipt_owner_detection": b.receipt.owner_detection,
            "signed_block": blk,
            "desired_state_document": doc,
            "runner_line": r.line(),
            "unchecked_exit": unchecked.code(),
            "stated_exit": stated.code(),
        }),
    );
}

/// **C27**: a partial transition.
#[test]
#[ignore = "needs the compose stack on --kafka 4.3 with the acl profile"]
fn c27_a_partial_transition_names_what_was_not_applied() {
    require_line("4.x");
    let s = stack();
    let n = nonce();
    let a = format!("tc{n}-a");
    let bt = format!("tc{n}-b");
    for t in [&a, &bt] {
        create_topic(&s.acl, t, 1, 1, &[("cleanup.policy", "compact")]);
        produce(&s.acl, t, 10, None);
    }
    let b = backup(
        &format!("prod052-c27-{n}"),
        &s.acl,
        &[&a, &bt],
        Archive::S3,
        false,
        None,
    );
    let prefix = format!("r27{n}-");
    let t_a = format!("{prefix}{a}");
    let t_b = format!("{prefix}{bt}");
    // A topic this run does not create, under the same prefix.
    let unrelated = format!("{prefix}unrelated");
    create_topic(&s.acl, &unrelated, 1, 1, &[("retention.ms", "123456")]);
    let unrelated_before = overrides(&s.acl, &unrelated);
    // The restricted principal may do everything on B but alter it.
    let allow = acl(&t_b, &["--operation", "All"], true);
    assert!(allow.status.success(), "{}", text(&allow));
    let deny = acl(&t_b, &["deny", "--operation", "AlterConfigs"], true);
    assert!(deny.status.success(), "{}", text(&deny));
    let auth = format!(
        "\x20 auth:\n\x20   mode: scramSha512\n\x20   username: {}\n",
        harness::SCRAM_USER
    );
    let r = restore(
        &restore_spec(
            &b,
            &[&a, &bt],
            &s.acl_sasl,
            &prefix,
            &block("afterVerification", Some("[]")),
            &auth,
        ),
        "c27",
        &cluster_id(&s.acl),
        true,
    );
    let _ = acl(&t_b, &["--operation", "All"], false);
    let _ = acl(&t_b, &["deny", "--operation", "AlterConfigs"], false);
    assert_passed(&r);
    let line_ = r.line();
    assert_eq!(line_["transition"], "partial", "{line_}");
    assert_eq!(line_["applied"], json!([t_a]));
    assert_eq!(line_["failed"][0]["topic"], t_b);
    assert_eq!(
        line_["failed"][0]["desired"],
        json!({"cleanup.policy": "compact"})
    );
    assert_eq!(overrides(&s.acl, &t_a)["cleanup.policy"], "compact");
    assert_eq!(overrides(&s.acl, &t_b)["cleanup.policy"], "delete");
    assert_eq!(
        overrides(&s.acl, &unrelated),
        unrelated_before,
        "a topic the run did not create keeps its overrides byte for byte"
    );
    // The signed phase-9 attestation says the same.
    let attestation: Value = serde_json::from_slice(&evidence_object(&format!(
        "logweir/drills/{}.teardown.json",
        r.run_id()
    )))
    .expect("the attestation");
    let tc = &attestation["topic_configuration"];
    assert_eq!(tc["applied"], json!([t_a]), "{attestation}");
    assert_eq!(tc["failed"][0]["topic"], t_b);
    assert_eq!(
        tc["failed"][0]["desired"],
        json!({"cleanup.policy": "compact"})
    );
    write_evidence(
        "c27_a_partial_transition_names_what_was_not_applied",
        &json!({
            "runner_line": line_,
            "attestation_topic_configuration": tc,
            "unrelated_before": unrelated_before,
            "unrelated_after": overrides(&s.acl, &unrelated),
        }),
    );
}
