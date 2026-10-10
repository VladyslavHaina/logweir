#![cfg(feature = "e2e")]
//! **PROD-01.4a — topic IDs through DescribeTopics, observed live.**
//!
//! `logweir-rdkafka-ffi::topics::describe_topics` (OD-6 (a2)) is the product's
//! only route to a topic's ID (KIP-516). These rows hold it, and what the
//! backup does with it, to the broker's own answer:
//!
//! | row | stack | proves | its control |
//! |---|---|---|---|
//! | `the_product_reads_the_brokers_topic_id` | default | the ID `ClusterReader::topic_ids` returns equals `kafka-topics.sh --describe`'s `TopicId`, character for character, for IDs carrying `-` or `_`; an absent topic is `NotFound` | an absent topic is never an ID; the CLI is the ground truth, never the product |
//! | `a_transport_failure_is_unreachable_never_not_found` | none (a closed port) | no broker within the bound is `KafkaError::Unreachable`, never `NotFound` | the first row's absent topic, answered by a live broker, is `NotFound` |
//! | `a_recreated_topic_is_a_new_generation_and_the_same_topic_continues` | default | two real `logweir backup run`s: the receipts record each topic's ID before and after the engine, equal to the CLI's; the topic deleted and recreated between them is `Generation::New`; both readers print the same `generations` lines; the catalog point copies the IDs | the topic left alone is `Generation::Same` across the same two backups |
//! | `a_principal_without_describe_is_refused_by_name` | `acl` (ignored) | the restricted SCRAM principal's DescribeTopics of a topic it may not Describe is `NotAuthorized`, by name | the super user reads that topic's ID (equal to the CLI's); the restricted principal reads an open topic's ID and an absent topic's `NotFound` |
//!
//! # Running them
//!
//! ```text
//! eval "$(e2e/compose/stack-env.sh --slot N --profiles acl,auth)"   # add --kafka 3.9|4.3
//! just e2e-up
//! cargo build -p logweir
//! AWS_EC2_METADATA_DISABLED=true cargo test -p e2e --features e2e --test topic_ids -- \
//!     --include-ignored --test-threads=1 --nocapture
//! just e2e-down
//! ```
//!
//! Each row writes what it observed to `topic-ids/<row>.json` under the
//! stack's scratch directory (`harness::demo_dir()`). Every stack address
//! comes from the harness (`bootstrap()`, `bootstrap_acl()`,
//! `bootstrap_acl_sasl()`, `s3_endpoint()`).
mod harness;

use harness::{bin, demo_dir, engine_bin, engine_digest, engine_mount, engine_version, root};
use logweir_core::backup_receipt::{BackupReceipt, TopicIdentity};
use logweir_core::topic_identity::{between, Generation};
use logweir_kafka::rdkafka_reader::RdKafkaReader;
use logweir_kafka::reader::{AuthConfig, ClusterReader, KafkaError};
use logweir_kafka::topic_ids::TopicIdRead;
use serde_json::{json, Value};
use std::io::Read;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

fn nonce() -> String {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970")
        .as_nanos();
    format!("{:010}", n % 10_000_000_000)
}

fn write_evidence(row: &str, v: &Value) {
    let dir = demo_dir().join("topic-ids");
    std::fs::create_dir_all(&dir).expect("the evidence directory");
    let p = dir.join(format!("{row}.json"));
    std::fs::write(&p, serde_json::to_vec_pretty(v).expect("serialises")).expect("written");
    eprintln!("[01.4a] evidence: {}", p.display());
}

// ============================================================ the broker

/// One of the default broker's own CLIs, inside its RUNNING container.
fn broker_cli(args: &[&str]) -> Output {
    harness::stack::ensure_coherent();
    let mut c = Command::new("docker");
    c.args([
        "compose",
        "-f",
        "e2e/compose/docker-compose.yml",
        "exec",
        "-T",
        "kafka-broker-1",
    ])
    .args(args)
    .current_dir(root());
    output_within(c, 120)
}

/// The broker's version, from its own CLI.
fn kafka_version(cli: &dyn Fn(&[&str]) -> Output) -> String {
    let o = cli(&["/opt/kafka/bin/kafka-topics.sh", "--version"]);
    assert!(o.status.success(), "kafka-topics --version:\n{}", text(&o));
    String::from_utf8_lossy(&o.stdout)
        .split_whitespace()
        .next()
        .unwrap_or("unknown")
        .to_string()
}

/// The broker's own `TopicId` for `topic`: the GROUND TRUTH every row is
/// judged against, never the product's read.
fn cli_topic_id(cli: &dyn Fn(&[&str]) -> Output, in_network: &str, topic: &str) -> String {
    let o = cli(&[
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        in_network,
        "--describe",
        "--topic",
        topic,
    ]);
    assert!(
        o.status.success(),
        "kafka-topics --describe {topic}:\n{}",
        text(&o)
    );
    let out = String::from_utf8_lossy(&o.stdout).into_owned();
    let id = out
        .split_whitespace()
        .skip_while(|w| *w != "TopicId:")
        .nth(1)
        .unwrap_or_else(|| panic!("no TopicId in the describe output for {topic}:\n{out}"))
        .to_string();
    assert_eq!(
        id.len(),
        22,
        "a Kafka topic ID prints as 22 characters: {id:?}"
    );
    id
}

const IN_NETWORK: &str = "kafka-broker-1:9094";

fn default_cli(args: &[&str]) -> Output {
    broker_cli(args)
}

/// Deletes `topic` on the default broker and waits, bounded, until the
/// broker's metadata no longer lists it, so a create of the same name makes
/// a NEW topic.
fn delete_and_wait(topic: &str) {
    let o = broker_cli(&[
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        IN_NETWORK,
        "--delete",
        "--topic",
        topic,
    ]);
    assert!(o.status.success(), "delete {topic}:\n{}", text(&o));
    for _ in 0..60 {
        if !harness::topic_exists(topic) {
            return;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    panic!("{topic} is still listed 30 s after its deletion");
}

/// Deletes `topics` with the broker's own CLI, best effort, at the end.
struct Topics(Vec<String>);
impl Drop for Topics {
    fn drop(&mut self) {
        for t in &self.0 {
            let _ = broker_cli(&[
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                IN_NETWORK,
                "--delete",
                "--if-exists",
                "--topic",
                t,
            ]);
        }
    }
}

/// `n` keyed records into `topic` (one partition), with the producer's own
/// timestamps.
fn produce(topic: &str, n: usize, tag: &str) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after 1970")
        .as_millis() as i64;
    let payloads: Vec<String> = (0..n).map(|i| format!("{topic}-{tag}-{i}")).collect();
    let records: Vec<(i64, &str)> = payloads
        .iter()
        .enumerate()
        .map(|(i, p)| (now + i as i64, p.as_str()))
        .collect();
    harness::produce_with_timestamps(topic, &records)
        .unwrap_or_else(|e| panic!("produce into {topic}: {e}"));
}

/// The product's read of one topic, through `ClusterReader::topic_ids`.
fn product_read(reader: &RdKafkaReader, topic: &str) -> TopicIdRead {
    let mut got = reader
        .topic_ids(&[topic.to_string()])
        .unwrap_or_else(|e| panic!("DescribeTopics of {topic}: {e}"));
    assert_eq!(got.len(), 1, "one verdict per name: {got:?}");
    let (name, read) = got.remove(0);
    assert_eq!(name, topic);
    read
}

// ============================================================ the rows

/// **The product's ID is the broker's**, on whatever line this stack runs:
/// IDs read through `ClusterReader::topic_ids` equal `kafka-topics.sh
/// --describe`'s `TopicId`, character for character, until at least one ID
/// carrying `-` or `_` has been compared (librdkafka's own text would carry
/// `+` or `/` there, PROD-01.4 §1.3 C4); and a topic the broker does not hold
/// is `NotFound`, never an ID.
#[test]
fn the_product_reads_the_brokers_topic_id() {
    let n = nonce();
    let reader = harness::reader();
    let version = kafka_version(&default_cli);
    let mut topics = Topics(Vec::new());
    let mut compared = Vec::new();
    let mut url_safe_only = 0;
    // About half of all IDs carry `-` or `_`; 24 topics leave a miss
    // probability near 1e-7.
    for i in 0..24 {
        let topic = format!("ti4a-id-{n}-{i:02}");
        harness::create_topic(&topic, 2);
        topics.0.push(topic.clone());
        let cli = cli_topic_id(&default_cli, IN_NETWORK, &topic);
        let product = product_read(&reader, &topic);
        assert_eq!(
            product,
            TopicIdRead::Id(cli.clone()),
            "the product's ID for {topic} is not the broker CLI's"
        );
        let special = cli.contains('-') || cli.contains('_');
        compared.push(json!({
            "topic": topic,
            "cli": cli,
            "product": format!("{product:?}"),
            "dash_or_underscore": special,
        }));
        if special {
            url_safe_only += 1;
            if url_safe_only >= 2 && i >= 3 {
                break;
            }
        }
    }
    assert!(
        url_safe_only > 0,
        "no compared ID carried `-` or `_`, so the alphabet was never tested"
    );
    // The control: an absent topic is not found — never an ID, never a
    // transport failure.
    let absent = format!("ti4a-absent-{n}");
    let absent_read = product_read(&reader, &absent);
    assert_eq!(absent_read, TopicIdRead::NotFound, "{absent}");
    assert!(!harness::topic_exists(&absent), "the read created {absent}");
    write_evidence(
        "the_product_reads_the_brokers_topic_id",
        &json!({
            "kafka_version": version,
            "compared": compared,
            "ids_with_dash_or_underscore": url_safe_only,
            "absent_topic": absent,
            "absent_read": format!("{absent_read:?}"),
        }),
    );
}

/// **A transport failure is `Unreachable`, never `NotFound`.** A reader
/// whose bootstrap is a closed local port gets no answer within its admin
/// bound; the call fails as a whole, and the error says nothing about the
/// topic. (Its control is the first row's absent topic, which a LIVE broker
/// answers `NotFound`.)
#[test]
fn a_transport_failure_is_unreachable_never_not_found() {
    let reader = RdKafkaReader::connect(&["127.0.0.1:1".to_string()], AuthConfig::Plaintext)
        .expect("a reader builds without dialling")
        .with_admin_bound(Duration::from_secs(2))
        .expect("a 2 s bound is legal");
    let started = Instant::now();
    let got = reader.topic_ids(&["ti4a-anything".to_string()]);
    let took = started.elapsed();
    match &got {
        Err(KafkaError::Unreachable(m)) => assert!(m.contains("DescribeTopics"), "{m}"),
        other => panic!("a closed port must be Unreachable, got {other:?}"),
    }
    assert!(
        took < Duration::from_secs(2) + logweir_rdkafka_ffi_poll_margin() + Duration::from_secs(2),
        "the call is bounded: {took:?}"
    );
    write_evidence(
        "a_transport_failure_is_unreachable_never_not_found",
        &json!({"answer": format!("{got:?}"), "took_ms": took.as_millis() as u64}),
    );
}

/// `logweir_rdkafka_ffi::POLL_MARGIN`, which e2e does not depend on: 5 s.
fn logweir_rdkafka_ffi_poll_margin() -> Duration {
    Duration::from_secs(5)
}

// ------------------------------------------------------------ the backups

fn signing_pem() -> PathBuf {
    root().join("e2e/fixtures/signed/signing.pem")
}

fn backup_allowlist() -> PathBuf {
    let p = demo_dir().join("ti4a-backup-allowed-clusters.json");
    std::fs::write(
        &p,
        "{\"allowed_cluster_ids\": [\"SCRATCH-CLUSTER-NOT-THE-SOURCE\"]}\n",
    )
    .expect("written");
    p
}

fn s3_user() -> String {
    ["minio", "admin"].concat()
}

fn engine_env(c: &mut Command) {
    c.env("AWS_ACCESS_KEY_ID", s3_user())
        .env("AWS_SECRET_ACCESS_KEY", s3_user())
        .env("AWS_REGION", "us-east-1")
        .env("LOGWEIR_ENGINE_BIN", engine_bin())
        .env("LOGWEIR_ENGINE_VERSION", engine_version())
        .env("LOGWEIR_ENGINE_DIGEST", engine_digest())
        .env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount())
        .env("TMPDIR", engine_mount());
}

fn storage(prefix: &str) -> logweir_core::engine::StorageUrl {
    serde_yaml::from_str(&format!(
        "backend: s3\nbucket: {}\nprefix: {prefix}\nregion: us-east-1\nendpoint: {}\n\
         path_style: true\nallow_http: true\n",
        harness::ARCHIVE_BUCKET,
        harness::s3_endpoint()
    ))
    .expect("a storage url")
}

fn archive_get(prefix: &str, key: &str, max_bytes: u64) -> Vec<u8> {
    std::env::set_var("AWS_ACCESS_KEY_ID", s3_user());
    std::env::set_var("AWS_SECRET_ACCESS_KEY", s3_user());
    std::env::set_var("AWS_REGION", "us-east-1");
    let store = logweir_engine_oso::storage::Store::read_only_from_url(&storage(prefix))
        .expect("the archive store");
    store
        .get_capped(key, max_bytes)
        .unwrap_or_else(|e| panic!("read {key}: {e}"))
        .0
}

/// What one `logweir backup run` printed and signed.
struct Backup {
    receipt_key: String,
    receipt_bytes: Vec<u8>,
    receipt: BackupReceipt,
    catalog: Value,
    prefix: String,
}

/// `logweir backup run` of `topics` as set `backup_id` under `prefix`.
fn backup(backup_id: &str, prefix: &str, topics: &[&str]) -> Backup {
    let spec = demo_dir().join(format!("{backup_id}-backup.yaml"));
    std::fs::write(
        &spec,
        format!(
            "backup_id: {backup_id}\n\
             source:\n\
             \x20 bootstrap_servers: [{}]\n\
             \x20 topics: [{}]\n\
             storage:\n\
             \x20 backend: s3\n\
             \x20 bucket: {}\n\
             \x20 prefix: {prefix}\n\
             \x20 region: us-east-1\n\
             \x20 endpoint: {}\n\
             \x20 path_style: true\n\
             \x20 allow_http: true\n\
             backup:\n\
             \x20 compression: zstd\n\
             \x20 segment_max_records: 1000\n\
             \x20 segment_max_bytes: 10485760\n\
             \x20 max_concurrent_partitions: 3\n",
            harness::bootstrap(),
            topics.join(", "),
            harness::ARCHIVE_BUCKET,
            harness::s3_endpoint(),
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
    let line = |p: &str| {
        stdout
            .lines()
            .find_map(|l| l.strip_prefix(p))
            .map(str::to_string)
    };
    let receipt_key = line("receipt-key=").expect("backup run prints receipt-key=");
    let catalog_key = line("catalog-key=").expect("backup run prints catalog-key=");
    let receipt_bytes = archive_get(
        prefix,
        &receipt_key,
        logweir_engine_oso::storage::caps::SIGNED_DOCUMENT,
    );
    let receipt: BackupReceipt = serde_json::from_slice(&receipt_bytes).expect("a receipt");
    let catalog: Value = serde_json::from_slice(&archive_get(
        prefix,
        &catalog_key,
        logweir_engine_oso::storage::caps::SIGNED_DOCUMENT,
    ))
    .expect("a catalog record");
    Backup {
        receipt_key,
        receipt_bytes,
        receipt,
        catalog,
        prefix: prefix.to_string(),
    }
}

/// Both readers over a receipt, exactly as an auditor runs them: their exit
/// codes and every `generations` line each printed.
fn both_readers(b: &Backup, label: &str) -> Value {
    let dir = demo_dir().join(format!("ti4a-verify-{label}"));
    std::fs::create_dir_all(&dir).expect("dir");
    let doc = dir.join("receipt.json");
    let sig = dir.join("receipt.sig");
    std::fs::write(&doc, &b.receipt_bytes).expect("written");
    std::fs::write(
        &sig,
        archive_get(
            &b.prefix,
            &b.receipt_key.replace(".receipt.json", ".receipt.sig"),
            logweir_engine_oso::storage::caps::SIDECAR,
        ),
    )
    .expect("written");
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
                let i = l.find("generations[").or_else(|| l.find("generations:"))?;
                Some(l[i..].to_string())
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

fn entry<'a>(b: &'a Backup, topic: &str) -> &'a TopicIdentity {
    b.receipt
        .generations
        .as_ref()
        .expect("every receipt this build signs carries generations")
        .get(topic)
        .unwrap_or_else(|| panic!("no generations entry for {topic}"))
}

/// **A topic deleted and recreated under the same name between two backups
/// is a NEW generation; the same topic is the same generation.** Two real
/// `logweir backup run`s over two topics: `recreated` is deleted and created
/// again between them, `same` is left alone (the control). Each receipt
/// records each topic's ID before and after the engine, equal to the CLI's;
/// the generation rule (`logweir_core::topic_identity::between`) reads
/// `recreated` as `New` and `same` as `Same`; both readers verify and print
/// the same lines; each catalog point copies its receipt's IDs.
#[test]
fn a_recreated_topic_is_a_new_generation_and_the_same_topic_continues() {
    let n = nonce();
    let recreated = format!("ti4a-recreated-{n}");
    let same = format!("ti4a-same-{n}");
    let _cleanup = Topics(vec![recreated.clone(), same.clone()]);
    for t in [&recreated, &same] {
        harness::create_topic(t, 1);
        produce(t, 5, "first");
    }
    let recreated_first = cli_topic_id(&default_cli, IN_NETWORK, &recreated);
    let same_id = cli_topic_id(&default_cli, IN_NETWORK, &same);

    let prefix = format!("ti4a-{n}");
    let topics = [recreated.as_str(), same.as_str()];
    let a = backup(&format!("ti4a-a-{n}"), &prefix, &topics);

    // The recreation: same name, same partition count, new records.
    delete_and_wait(&recreated);
    harness::create_topic(&recreated, 1);
    produce(&recreated, 7, "second");
    let recreated_second = cli_topic_id(&default_cli, IN_NETWORK, &recreated);
    assert_ne!(
        recreated_first, recreated_second,
        "the fixture did not recreate {recreated}: the broker kept its ID"
    );
    assert_eq!(
        cli_topic_id(&default_cli, IN_NETWORK, &same),
        same_id,
        "the control topic must keep its ID"
    );

    let b = backup(&format!("ti4a-b-{n}"), &prefix, &topics);

    // Each receipt records the broker's IDs, before AND after the engine.
    let want = [
        (&a, recreated.as_str(), recreated_first.as_str()),
        (&a, same.as_str(), same_id.as_str()),
        (&b, recreated.as_str(), recreated_second.as_str()),
        (&b, same.as_str(), same_id.as_str()),
    ];
    for (point, topic, id) in want {
        let e = entry(point, topic);
        assert_eq!(e.topic_id.as_deref(), Some(id), "{topic}: {e:?}");
        assert_eq!(e.topic_id_after.as_deref(), Some(id), "{topic}: {e:?}");
        assert_eq!(e.topic_id_source.as_deref(), Some("describeTopics"));
        assert_eq!(
            (e.topic_id_reason.as_ref(), e.topic_id_after_reason.as_ref()),
            (None, None)
        );
    }
    for point in [&a, &b] {
        harness::assert_format_at_least(
            &point.receipt.format_version,
            "1.6.0",
            "the receipt carries generations",
        );
        assert_eq!(point.receipt.validate_invariants(), Ok(()));
    }

    // THE RULE: the recreated topic is a new generation, never a
    // continuation; the control is the same generation.
    let recreated_verdict = between(Some(&a.receipt), &b.receipt, &recreated);
    let same_verdict = between(Some(&a.receipt), &b.receipt, &same);
    assert_eq!(
        recreated_verdict,
        Generation::New {
            previous: recreated_first.clone(),
            current: recreated_second.clone(),
        }
    );
    assert_eq!(
        same_verdict,
        Generation::Same {
            topic_id: same_id.clone()
        }
    );

    // Both readers verify both receipts and print the same lines.
    let va = both_readers(&a, &format!("a-{n}"));
    let vb = both_readers(&b, &format!("b-{n}"));
    for v in [&va, &vb] {
        assert_eq!(v["rust_exit"], 0, "{v}");
        assert_eq!(v["python_exit"], 0, "{v}");
        assert_eq!(v["rust_lines"], v["python_lines"], "{v}");
        let lines = v["rust_lines"].as_array().expect("lines");
        assert_eq!(lines.len(), 2, "{v}");
        assert!(
            lines
                .iter()
                .all(|l| l.as_str().unwrap().ends_with(", one generation")),
            "{v}"
        );
    }

    // Each catalog point copies its receipt's IDs (format 1.6.0 or later).
    for point in [&a, &b] {
        harness::assert_format_at_least(
            point.catalog["format_version"].as_str().expect("a version"),
            "1.6.0",
            "the catalog point copies identity",
        );
        for t in point.catalog["topics"].as_array().expect("topics") {
            let name = t["name"].as_str().expect("a name");
            assert_eq!(
                t["identity"],
                serde_json::to_value(entry(point, name)).unwrap(),
                "{name}"
            );
        }
    }

    write_evidence(
        "a_recreated_topic_is_a_new_generation_and_the_same_topic_continues",
        &json!({
            "kafka_version": kafka_version(&default_cli),
            "cli": {
                "recreated_before": recreated_first,
                "recreated_after": recreated_second,
                "same": same_id,
            },
            "receipt_a": {"key": a.receipt_key, "generations": a.receipt.generations},
            "receipt_b": {"key": b.receipt_key, "generations": b.receipt.generations},
            "verdict_recreated": recreated_verdict.to_string(),
            "verdict_same": same_verdict.to_string(),
            "readers_a": va,
            "readers_b": vb,
            "catalog_a_topics": a.catalog["topics"],
            "catalog_b_topics": b.catalog["topics"],
        }),
    );
}

// ------------------------------------------------------------ the acl profile

/// One of `kafka-acl`'s own CLIs, inside its RUNNING container, as
/// User:ANONYMOUS (a super user).
fn acl_cli(args: &[&str]) -> Output {
    harness::stack::ensure_coherent();
    let mut c = Command::new("docker");
    c.args([
        "compose",
        "-p",
        &harness::stack::project(),
        "-f",
        "e2e/compose/docker-compose.yml",
        "--profile",
        "acl",
        "exec",
        "-T",
        "kafka-acl",
    ])
    .args(args)
    .current_dir(root());
    output_within(c, 120)
}

const ACL_IN_NETWORK: &str = "kafka-acl:9094";

fn acl_ok(args: &[&str], what: &str) -> String {
    let o = acl_cli(args);
    assert!(o.status.success(), "{what}:\n{}", text(&o));
    String::from_utf8_lossy(&o.stdout).into_owned()
}

/// Creates `topic` on `kafka-acl` and waits until it is served.
fn acl_create(topic: &str) {
    acl_ok(
        &[
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            ACL_IN_NETWORK,
            "--create",
            "--topic",
            topic,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        ],
        &format!("create {topic} on kafka-acl"),
    );
    harness::await_created_on(&harness::bootstrap_acl(), topic, 1);
}

/// The ACL that takes Describe on `topic` away from the restricted principal:
/// once the topic HAS an ACL, `allow.everyone.if.no.acl.found` no longer
/// applies to it, and only `User:ti4a-ops` may Describe it.
fn acl_args(topic: &str) -> Vec<String> {
    [
        "--allow-principal",
        "User:ti4a-ops",
        "--operation",
        "Describe",
        "--topic",
        topic,
    ]
    .map(String::from)
    .to_vec()
}

fn acl_change(op: &str, topic: &str) {
    let mut args = vec![
        "/opt/kafka/bin/kafka-acls.sh".to_string(),
        "--bootstrap-server".to_string(),
        ACL_IN_NETWORK.to_string(),
        op.to_string(),
    ];
    if op == "--remove" {
        args.push("--force".to_string());
    }
    args.extend(acl_args(topic));
    let borrowed: Vec<&str> = args.iter().map(String::as_str).collect();
    acl_ok(&borrowed, &format!("kafka-acls {op} on {topic}"));
}

/// Undoes this row's ACL and deletes its topics, best effort, at the end.
struct AclCleanup(Vec<String>, Option<String>);
impl Drop for AclCleanup {
    fn drop(&mut self) {
        if let Some(topic) = &self.1 {
            let mut args = vec![
                "/opt/kafka/bin/kafka-acls.sh".to_string(),
                "--bootstrap-server".to_string(),
                ACL_IN_NETWORK.to_string(),
                "--remove".to_string(),
                "--force".to_string(),
            ];
            args.extend(acl_args(topic));
            let borrowed: Vec<&str> = args.iter().map(String::as_str).collect();
            let _ = acl_cli(&borrowed);
        }
        for t in &self.0 {
            let _ = acl_cli(&[
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                ACL_IN_NETWORK,
                "--delete",
                "--if-exists",
                "--topic",
                t,
            ]);
        }
    }
}

/// **Missing Describe permission is refused by NAME, never read as
/// absent.** On the `acl` profile's broker (StandardAuthorizer), the
/// restricted SCRAM principal's DescribeTopics of a topic it may not Describe
/// is `NotAuthorized`; the super user reads the same topic's ID, equal to the
/// CLI's (so the topic exists and the refusal is the authorizer's); and the
/// restricted principal still reads an open topic's ID and an absent topic's
/// `NotFound` (so the refusal is not a broken client).
#[test]
#[ignore = "needs the `acl` profile: eval \"$(e2e/compose/stack-env.sh --slot N --profiles acl)\""]
fn a_principal_without_describe_is_refused_by_name() {
    let props = acl_ok(
        &["cat", "/opt/kafka/config/server.properties"],
        "read kafka-acl's server.properties",
    );
    assert!(
        props.contains(
            "authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer"
        ),
        "kafka-acl is not running the StandardAuthorizer: start the stack with the `acl` profile"
    );
    let n = nonce();
    let denied = format!("ti4a-denied-{n}");
    let open = format!("ti4a-open-{n}");
    let absent = format!("ti4a-absent-{n}");
    let mut cleanup = AclCleanup(vec![denied.clone(), open.clone()], None);
    acl_create(&denied);
    acl_create(&open);
    acl_change("--add", &denied);
    cleanup.1 = Some(denied.clone());

    let super_user = RdKafkaReader::connect(&[harness::bootstrap_acl()], AuthConfig::Plaintext)
        .expect("a PLAINTEXT reader (a super user on kafka-acl)");
    let restricted = RdKafkaReader::connect(
        &[harness::bootstrap_acl_sasl()],
        AuthConfig::ScramSha512 {
            username: harness::SCRAM_USER.to_string(),
            password: harness::SCRAM_PASSWORD.to_string(),
            tls: false,
            tls_ca_file: None,
        },
    )
    .expect("a SCRAM reader (the restricted principal)");

    let acl_cli_dyn = |args: &[&str]| acl_cli(args);
    let denied_cli = cli_topic_id(&acl_cli_dyn, ACL_IN_NETWORK, &denied);
    let open_cli = cli_topic_id(&acl_cli_dyn, ACL_IN_NETWORK, &open);

    // The refusal, by name — in ONE call with the topics it may read, so a
    // refused topic cannot make its neighbours unknown.
    let got = restricted
        .topic_ids(&[denied.clone(), open.clone(), absent.clone()])
        .expect("the call itself is answered");
    assert_eq!(
        got,
        vec![
            (denied.clone(), TopicIdRead::NotAuthorized),
            (open.clone(), TopicIdRead::Id(open_cli.clone())),
            (absent.clone(), TopicIdRead::NotFound),
        ]
    );
    // What the receipt would record for each: a named reason, never absent.
    let receipt_side: Vec<_> = got
        .iter()
        .map(|(t, r)| (t.clone(), r.to_id_read()))
        .collect();
    assert_eq!(
        receipt_side[0].1,
        logweir_core::topic_identity::IdRead::Unread("notAuthorized")
    );
    // The control: the super user reads the denied topic's ID.
    assert_eq!(
        product_read(&super_user, &denied),
        TopicIdRead::Id(denied_cli.clone())
    );
    // And with the ACL removed, the restricted principal reads it too.
    acl_change("--remove", &denied);
    cleanup.1 = None;
    let mut allowed = TopicIdRead::NotAuthorized;
    for _ in 0..20 {
        allowed = product_read(&restricted, &denied);
        if allowed != TopicIdRead::NotAuthorized {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert_eq!(allowed, TopicIdRead::Id(denied_cli.clone()));

    write_evidence(
        "a_principal_without_describe_is_refused_by_name",
        &json!({
            "kafka_version": kafka_version(&acl_cli_dyn),
            "restricted_reads": got.iter().map(|(t, r)| json!({"topic": t, "read": format!("{r:?}")})).collect::<Vec<_>>(),
            "receipt_side": receipt_side.iter().map(|(t, r)| json!({"topic": t, "read": format!("{r:?}")})).collect::<Vec<_>>(),
            "super_user_read_of_denied": denied_cli,
            "restricted_read_after_the_acl_is_removed": format!("{allowed:?}"),
        }),
    );
}
