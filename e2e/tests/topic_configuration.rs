#![cfg(feature = "e2e")]
//! **PROD-05.1 — the topic configuration model, observed live on the 3.9 and
//! 4.x broker lines.**
//!
//! Three rows, each `#[ignore]`d because each needs the stack's `acl` profile
//! (`e2e/README.md`): `kafka-acl` runs KRaft's StandardAuthorizer, every
//! PLAINTEXT client on it is User:ANONYMOUS and a super user, and the SCRAM
//! user `logweir` is the restricted principal the denied row narrows with ACLs.
//! Run the file once per broker line; each row reads the line from
//! `KAFKA_VERSION`, which `stack-env.sh --kafka` exports.
//!
//! | row | proves |
//! |---|---|
//! | `the_portability_table_is_the_brokers` | `logweir_core::topic_configuration::TABLE` is the broker's own: DescribeConfigs of a fresh topic returns exactly the keys the table defines for the line (36 on 3.9, 33 on 4.x), and `CreateTopics` with `validate_only` and each key's sample is accepted or refused as the table records; the control is a provider's key, refused on both lines, and no probe topic exists afterwards |
//! | `a_backup_records_each_topics_model_and_its_owner` | the receipt's 1.3.0 `topic_configuration`, end to end through `logweir backup run`: a compacted and a delete-policy topic with min-in-sync settings, a topic with no override (the control: nothing `portable`), the 3.9-only keys recorded `removedInKafka4` (on 4.x the broker refuses them, which the row records instead), a Strimzi-labelled topic owned through `--kafka-topic-resources` while an unlabelled, an unmanaged and another cluster's resource own nothing, and a declared external owner; both readers accept the receipt and print the same model lines; the catalog point copies it |
//! | `a_denied_describe_configs_records_no_entries_and_keeps_the_layout` | a topic whose DescribeConfigs the backup principal may not read records NO entries (`captureDenied`), never an empty set, while its partition count and replication factor still come from the archive; its readable neighbour records its entries beside `manifestDiffers`; the control is the same topics backed up by the super user, which records the denied topic's entries; the principal's password is in no output, receipt or catalog record |
//!
//! # Running them
//!
//! ```text
//! eval "$(e2e/compose/stack-env.sh --slot N --kafka 3.9 --profiles acl)"
//! just e2e-up
//! cargo build -p logweir
//! LOGWEIR_PYTHON=<python with cryptography> AWS_EC2_METADATA_DISABLED=true \
//!   cargo test -p e2e --features e2e --test topic_configuration -- --ignored --test-threads=1 --nocapture
//! just e2e-down          # and again with --kafka 4.3
//! ```
//!
//! Each row writes what it observed to `topic-configuration/<line>/<row>.json`
//! under the stack's scratch directory (`harness::demo_dir()`).
mod harness;

use harness::{bin, demo_dir, engine_bin, engine_digest, engine_mount, engine_version, root};
use logweir_core::backup_receipt::{BackupReceipt, TopicConfiguration};
use logweir_core::topic_configuration::{self as model, KAFKA_LINES, TABLE};
use logweir_kafka::reader::{AuthConfig, ClusterReader};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::io::Read;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ============================================================ the stack

/// Every address and credential of the compose stack this file uses.
struct Stack {
    compose_project: String,
    compose_file: String,
    profile: String,
    broker_service: String,
    /// PLAINTEXT on the host: User:ANONYMOUS, a super user.
    plaintext: String,
    /// SASL/SCRAM-SHA-512 on the host: User:`scram_user`, NOT a super user.
    sasl: String,
    /// PLAINTEXT inside `kafka-net`, for the broker's own CLIs.
    in_network: String,
    scram_user: String,
    scram_password: String,
    s3_endpoint: String,
    s3_user: String,
    s3_secret: String,
    archive_bucket: String,
}

/// THE ONE PLACE this file reads the stack's addresses.
fn stack() -> Stack {
    harness::stack::ensure_coherent();
    let minio = ["minio", "admin"].concat();
    Stack {
        compose_project: harness::stack::project(),
        compose_file: "e2e/compose/docker-compose.yml".into(),
        profile: "acl".into(),
        broker_service: "kafka-acl".into(),
        plaintext: harness::bootstrap_acl(),
        sasl: harness::bootstrap_acl_sasl(),
        in_network: "kafka-acl:9094".into(),
        scram_user: harness::SCRAM_USER.into(),
        scram_password: harness::SCRAM_PASSWORD.into(),
        s3_endpoint: harness::s3_endpoint(),
        s3_user: minio.clone(),
        s3_secret: minio,
        archive_bucket: harness::ARCHIVE_BUCKET.into(),
    }
}

/// The broker line the stack runs, as the table names it: `3.9` or `4.x`.
/// Anything else is refused by name: the table is measured on these two lines
/// and a row on another would prove nothing about it.
fn line() -> &'static str {
    let v = std::env::var("KAFKA_VERSION").unwrap_or_default();
    if v.starts_with("3.9") {
        KAFKA_LINES[0]
    } else if v.starts_with("4.") {
        KAFKA_LINES[1]
    } else {
        panic!(
            "KAFKA_VERSION is {v:?}: start the slot with `--kafka 3.9` or `--kafka 4.3` (this \
             file's module doc); the portability table is measured on those two lines"
        )
    }
}

fn line_index() -> usize {
    KAFKA_LINES
        .iter()
        .position(|l| *l == line())
        .expect("a measured line")
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

fn nonce() -> String {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970")
        .as_nanos();
    format!("{:010}", n % 10_000_000_000)
}

/// The broker must be running the StandardAuthorizer, or the denied row would
/// prove nothing.
fn assert_the_authorizer_is_on() {
    let props = broker_cli_ok(
        &["cat", "/opt/kafka/config/server.properties"],
        "read the broker's server.properties",
    );
    assert!(
        props.contains(
            "authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer"
        ),
        "kafka-acl is not running the StandardAuthorizer: start the stack with the `acl` profile"
    );
}

/// Create `topic` with `partitions` and `configs` through the broker's CLI;
/// `Err` carries the broker's refusal (a 4.x broker refusing a removed key).
fn try_create_topic(topic: &str, partitions: u32, configs: &[(&str, &str)]) -> Result<(), String> {
    let s = stack();
    let partitions = partitions.to_string();
    let mut args: Vec<String> = [
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        &s.in_network,
        "--create",
        "--topic",
        topic,
        "--partitions",
        &partitions,
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
    let o = broker_cli(&borrowed);
    if o.status.success() {
        Ok(())
    } else {
        Err(text(&o)
            .lines()
            .find(|l| l.contains("Error") || l.contains("Exception"))
            .unwrap_or("the broker refused the topic")
            .trim()
            .to_string())
    }
}

fn create_topic(topic: &str, partitions: u32, configs: &[(&str, &str)]) {
    try_create_topic(topic, partitions, configs)
        .unwrap_or_else(|e| panic!("create topic {topic}: {e}"));
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

/// The restricted principal may Read and Describe `topic` — and, because the
/// topic now HAS an ACL, nothing else: not DescribeConfigs.
fn allow_read_and_describe_only(topic: &str, add: bool) -> Output {
    let s = stack();
    let who = format!("User:{}", s.scram_user);
    let op = if add { "--add" } else { "--remove" };
    let mut args = vec![
        "/opt/kafka/bin/kafka-acls.sh",
        "--bootstrap-server",
        &s.in_network,
        op,
    ];
    if !add {
        args.push("--force");
    }
    args.extend_from_slice(&[
        "--allow-principal",
        &who,
        "--operation",
        "Read",
        "--operation",
        "Describe",
        "--topic",
        topic,
    ]);
    broker_cli(&args)
}

/// Deletes the row's topics and ACLs on every exit path, a panicking
/// assertion included.
struct Cleanup {
    topics: Vec<String>,
    acls: Vec<String>,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        for topic in &self.acls {
            let _ = allow_read_and_describe_only(topic, false);
        }
        for topic in &self.topics {
            delete_topic(topic);
        }
    }
}

/// Produce `count` records, `acks=1` so a `min.insync.replicas` above the
/// single broker's replica count does not refuse the seed.
fn produce(topic: &str, count: usize) {
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    let producer: BaseProducer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", stack().plaintext)
        .set("message.timeout.ms", "10000")
        .set("acks", "1")
        .create()
        .expect("a producer");
    for i in 0..count {
        let payload = format!("{topic}-{i}");
        let key = format!("k{}", i % 4);
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

/// Drive one rdkafka admin future to completion on THIS thread, with a hard
/// deadline (FX-4's helper: rdkafka resolves admin futures from its own
/// thread, so no async runtime is needed).
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

/// `CreateTopics` with `validate_only` for one topic carrying `key=value`:
/// `Ok` when the broker would create it, `Err(<code>)` when it refuses.
fn validate_only(name: &str, key: &str, value: &str) -> Result<(), String> {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    let admin: AdminClient<DefaultClientContext> = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", stack().plaintext)
        .create()
        .expect("an admin client");
    let topic = NewTopic::new(name, 1, TopicReplication::Fixed(1)).set(key, value);
    let res = block_on(
        admin.create_topics(
            &[topic],
            &AdminOptions::new()
                .validate_only(true)
                .request_timeout(Some(Duration::from_secs(20))),
        ),
    )
    .expect("the CreateTopics call itself answers");
    match res.into_iter().next().expect("one result") {
        Ok(_) => Ok(()),
        Err((_, code)) => Err(code.to_string()),
    }
}

// ============================================================ evidence

fn evidence_dir() -> PathBuf {
    demo_dir().join("topic-configuration").join(line())
}

fn write_evidence(row: &str, v: &Value) {
    let dir = evidence_dir();
    std::fs::create_dir_all(&dir).expect("the evidence directory");
    let p = dir.join(format!("{row}.json"));
    std::fs::write(&p, serde_json::to_vec_pretty(v).expect("serialises")).expect("written");
    eprintln!("[prod-05-1] evidence: {}", p.display());
}

// ============================================================ the pipeline

fn signing_pem() -> PathBuf {
    root().join("e2e/fixtures/signed/signing.pem")
}

fn backup_allowlist() -> PathBuf {
    let p = demo_dir().join("prod051-backup-allowed-clusters.json");
    std::fs::write(
        &p,
        "{\"allowed_cluster_ids\": [\"SCRATCH-CLUSTER-NOT-THE-SOURCE\"]}\n",
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

fn use_stack_s3_env() {
    let s = stack();
    std::env::set_var("AWS_ACCESS_KEY_ID", s.s3_user);
    std::env::set_var("AWS_SECRET_ACCESS_KEY", s.s3_secret);
    std::env::set_var("AWS_REGION", "us-east-1");
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

/// One `logweir backup run`, as the super user (PLAINTEXT) or the restricted
/// principal (SCRAM), with the plan's own `topic_owners` block and the
/// `KafkaTopic` resources file, when given.
fn backup(
    backup_id: &str,
    topics: &[&str],
    scram: bool,
    topic_owners_yaml: &str,
    kafka_topics: Option<(&PathBuf, &str)>,
) -> Backup {
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
             {auth}{topic_owners_yaml}\
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
    if let Some((file, cluster)) = kafka_topics {
        c.arg("--kafka-topic-resources")
            .arg(file)
            .args(["--strimzi-cluster", cluster]);
    }
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
        .get(&receipt_key)
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

fn archive_store(backup_id: &str) -> logweir_engine_oso::storage::Store {
    use_stack_s3_env();
    let s = stack();
    let url: logweir_core::engine::StorageUrl = serde_yaml::from_str(&format!(
        "backend: s3\nbucket: {}\nprefix: {backup_id}\nregion: us-east-1\nendpoint: {}\n\
         path_style: true\nallow_http: true\n",
        s.archive_bucket, s.s3_endpoint
    ))
    .expect("a storage url");
    logweir_engine_oso::storage::Store::read_only_from_url(&url).expect("the archive store")
}

fn catalog_record(b: &Backup) -> Value {
    let key = b
        .catalog_key
        .as_ref()
        .expect("backup run wrote its catalog point");
    let (bytes, _) = archive_store(&b.backup_id)
        .get(key)
        .unwrap_or_else(|e| panic!("read {key}: {e}"));
    serde_json::from_slice(&bytes).expect("a catalog record")
}

/// Both readers over the receipt, exactly as an auditor runs them: exit codes
/// and every `topic_configuration` line each printed.
fn verify_receipt_both_readers(b: &Backup) -> Value {
    let dir = demo_dir().join(format!("{}-verify", b.backup_id));
    std::fs::create_dir_all(&dir).expect("dir");
    let doc = dir.join("receipt.json");
    let sig = dir.join("receipt.sig");
    std::fs::write(&doc, &b.receipt_bytes).expect("written");
    let (sidecar, _) = archive_store(&b.backup_id)
        .get(&b.receipt_key.replace(".receipt.json", ".receipt.sig"))
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
                    .find("topic_configuration[")
                    .or_else(|| l.find("topic_configuration:"))?;
                Some(l[i..].to_string())
            })
            .collect()
    };
    json!({
        "rust_exit": rust.status.code(),
        "python_exit": py.status.code(),
        "rust_model_lines": lines(&rust),
        "python_model_lines": lines(&py),
        "rust_checked": text(&rust).lines().find(|l| l.starts_with("checked:")).unwrap_or(""),
    })
}

fn model_of<'a>(b: &'a Backup, topic: &str) -> &'a TopicConfiguration {
    b.receipt
        .topic_configuration
        .as_ref()
        .expect("a 1.3.0 receipt carries topic_configuration")
        .get(topic)
        .unwrap_or_else(|| panic!("no model entry for {topic}"))
}

/// `(value, source, portability)` of one recorded entry, or `None`.
fn entry_of(m: &TopicConfiguration, key: &str) -> Option<(Option<String>, String, String)> {
    m.entries
        .as_ref()?
        .get(key)
        .map(|e| (e.value.clone(), e.source.clone(), e.portability.clone()))
}

fn portable(value: &str) -> Option<(Option<String>, String, String)> {
    Some((
        Some(value.to_string()),
        "dynamicTopicConfig".to_string(),
        "portable".to_string(),
    ))
}

// ============================================================ row 1

/// **The table is the broker's own.** See the module doc.
#[test]
#[ignore = "needs the compose stack's acl profile on the 3.9 or 4.x line (module doc)"]
fn the_portability_table_is_the_brokers() {
    let line = line();
    let index = line_index();
    let n = nonce();
    let fresh = format!("p051-keys-{n}");
    let _cleanup = Cleanup {
        topics: vec![fresh.clone()],
        acls: Vec::new(),
    };
    create_topic(&fresh, 1, &[]);

    // 1. THE KEY SET: every key the broker reports for a fresh topic, against
    //    every key the table defines for this line — both directions.
    let reader = plaintext_reader();
    let answer = reader
        .describe_topic_configs(std::slice::from_ref(&fresh))
        .expect("DescribeConfigs answers");
    let entries = answer
        .into_iter()
        .next()
        .expect("one answer")
        .1
        .expect("the super user reads the topic's configuration");
    let reported: std::collections::BTreeSet<String> =
        entries.iter().map(|e| e.name.clone()).collect();
    let defined: std::collections::BTreeSet<String> = TABLE
        .iter()
        .filter(|r| model::defined_on(r.key, line))
        .map(|r| r.key.to_string())
        .collect();
    let sensitive: Vec<&str> = entries
        .iter()
        .filter(|e| e.sensitive)
        .map(|e| e.name.as_str())
        .collect();

    // 2. THE CREATE VERDICT: validate_only with each key's sample, against the
    //    table's recorded verdict for this line.
    let mut verdicts = BTreeMap::new();
    let mut disagreements = Vec::new();
    for rule in &TABLE {
        let got = validate_only(&format!("p051-probe-{n}"), rule.key, rule.sample);
        let accepted = got.is_ok();
        if accepted != rule.accepted[index] {
            disagreements.push(format!(
                "{}={}: the broker {} it, the table records {}",
                rule.key,
                rule.sample,
                if accepted { "accepted" } else { "refused" },
                if rule.accepted[index] {
                    "accepted"
                } else {
                    "refused"
                }
            ));
        }
        verdicts.insert(
            rule.key.to_string(),
            json!({
                "sample": rule.sample,
                "class": rule.class.wire_name(),
                "broker": got.err().unwrap_or_else(|| "accepted".to_string()),
                "table": if rule.accepted[index] { "accepted" } else { "refused" },
            }),
        );
    }
    // THE CONTROL: a key neither line defines — a provider's — is refused, so
    // the probe can refuse at all.
    let provider = validate_only(
        &format!("p051-probe-{n}"),
        "confluent.placement.constraints",
        "{}",
    );
    let leftovers: Vec<String> = reader
        .list_topics()
        .expect("list topics")
        .into_iter()
        .map(|t| t.name)
        .filter(|t| t.starts_with("p051-probe-"))
        .collect();
    write_evidence(
        "the_portability_table_is_the_brokers",
        &json!({
            "line": line,
            "kafka_version": std::env::var("KAFKA_VERSION").unwrap_or_default(),
            "reported_keys": reported,
            "table_keys": defined,
            "reported_but_not_in_table": reported.difference(&defined).collect::<Vec<_>>(),
            "in_table_but_not_reported": defined.difference(&reported).collect::<Vec<_>>(),
            "sensitive_entries": sensitive,
            "validate_only": verdicts,
            "provider_key": provider.clone().err().unwrap_or_else(|| "accepted".into()),
            "probe_topics_left": leftovers,
        }),
    );
    assert_eq!(
        reported, defined,
        "the broker's topic keys on {line} are not the table's (the evidence file names the \
         difference)"
    );
    assert_eq!(
        reported.len(),
        if line == "3.9" { 36 } else { 33 },
        "the measured counts"
    );
    assert!(
        disagreements.is_empty(),
        "validate_only disagrees with the table on {line}: {disagreements:#?}"
    );
    assert!(
        provider.is_err(),
        "THE CONTROL: a provider's key must be refused, or the probe proves nothing"
    );
    assert!(
        leftovers.is_empty(),
        "validate_only created a topic: {leftovers:?}"
    );
    assert!(
        sensitive.is_empty(),
        "no Apache Kafka topic key is sensitive (the secret class is defined from the flag and \
         proven on a synthetic reader): {sensitive:?}"
    );
}

// ============================================================ row 2

/// **The model, end to end, with its owners.** See the module doc.
#[test]
#[ignore = "needs the compose stack's acl profile on the 3.9 or 4.x line (module doc)"]
fn a_backup_records_each_topics_model_and_its_owner() {
    let line = line();
    let n = nonce();
    let compacted = format!("p051-{n}-compacted");
    let deleted = format!("p051-{n}-delete");
    let plain = format!("p051-{n}-plain");
    let legacy = format!("p051-{n}-legacy");
    let strimzi = format!("p051-{n}-strimzi");
    let all = [&compacted, &deleted, &plain, &legacy, &strimzi];
    let _cleanup = Cleanup {
        topics: all.iter().map(|t| (*t).clone()).collect(),
        acls: Vec::new(),
    };
    create_topic(
        &compacted,
        3,
        &[
            ("cleanup.policy", "compact"),
            ("min.compaction.lag.ms", "1000"),
            ("min.insync.replicas", "1"),
        ],
    );
    create_topic(
        &deleted,
        2,
        &[
            ("cleanup.policy", "delete"),
            ("retention.ms", "86400000"),
            ("min.insync.replicas", "2"),
        ],
    );
    create_topic(&plain, 1, &[]);
    // 3.9 vs 4.x: the keys Kafka 4.0 removed. On 3.9 the topic carries them;
    // on 4.x the broker REFUSES them (recorded), and the topic carries a key
    // both lines define instead.
    let removed_keys = [
        ("message.format.version", "3.0-IV1"),
        ("message.timestamp.difference.max.ms", "86400000"),
    ];
    let removed_on_this_line = try_create_topic(&legacy, 1, &removed_keys);
    if line == "4.x" {
        assert!(
            removed_on_this_line.is_err(),
            "a 4.x broker must refuse the keys Kafka 4.0 removed"
        );
        create_topic(&legacy, 1, &[("message.timestamp.after.max.ms", "3600000")]);
    } else {
        assert!(
            removed_on_this_line.is_ok(),
            "{removed_on_this_line:?}: a 3.9 broker accepts them"
        );
    }
    create_topic(&strimzi, 1, &[("retention.ms", "3600000")]);
    for t in all {
        produce(t, 20);
    }

    // The KafkaTopic resources, as `kubectl get kafkatopics -A -o yaml` writes
    // them: ONE labelled for this cluster and managed (owns `strimzi`), and
    // three CONTROLS that own nothing — an unmanaged one naming `compacted`, an
    // unlabelled one naming `plain`, and another cluster's naming `delete`.
    let resources = demo_dir().join(format!("prod051-{n}-kafkatopics.yaml"));
    std::fs::write(
        &resources,
        format!(
            "apiVersion: v1\nkind: List\nitems:\n\
             - apiVersion: kafka.strimzi.io/v1beta2\n  kind: KafkaTopic\n  metadata:\n    name: strimzi-kt\n    namespace: kafka\n    labels:\n      strimzi.io/cluster: prod\n  spec:\n    topicName: {strimzi}\n    partitions: 1\n    replicas: 1\n\
             - apiVersion: kafka.strimzi.io/v1beta2\n  kind: KafkaTopic\n  metadata:\n    name: compacted-kt\n    namespace: kafka\n    labels:\n      strimzi.io/cluster: prod\n    annotations:\n      strimzi.io/managed: \"false\"\n  spec:\n    topicName: {compacted}\n\
             - apiVersion: kafka.strimzi.io/v1beta2\n  kind: KafkaTopic\n  metadata:\n    name: plain-kt\n    namespace: kafka\n  spec:\n    topicName: {plain}\n\
             - apiVersion: kafka.strimzi.io/v1beta2\n  kind: KafkaTopic\n  metadata:\n    name: delete-kt\n    namespace: kafka\n    labels:\n      strimzi.io/cluster: staging\n  spec:\n    topicName: {deleted}\n"
        ),
    )
    .expect("the KafkaTopic resources");
    // A DECLARED external owner for `legacy`, in the plan.
    let owners = format!(
        "\x20 topic_owners:\n\x20 - topic: {legacy}\n\x20   kind: external\n\x20   reference: \"terraform: kafka_topic.legacy\"\n"
    );
    let topics: Vec<&str> = all.iter().map(|t| t.as_str()).collect();
    let b = backup(
        &format!("p051-{n}-model"),
        &topics,
        false,
        &owners,
        Some((&resources, "prod")),
    );
    let verified = verify_receipt_both_readers(&b);
    let record = catalog_record(&b);
    let r = &b.receipt;
    write_evidence(
        "a_backup_records_each_topics_model_and_its_owner",
        &json!({
            "line": line,
            "removed_keys_on_this_line": match &removed_on_this_line {
                Ok(()) => "accepted".to_string(),
                Err(e) => e.clone(),
            },
            "receipt_format_version": r.format_version,
            "config_coverage": r.config_coverage,
            "topic_configuration": r.topic_configuration,
            "both_readers": verified,
            "catalog_record_format": record["format_version"],
            "catalog_topics": record["topics"],
        }),
    );

    // The document.
    assert_eq!(r.format_version, "1.3.0");
    assert_eq!(r.validate_invariants(), Ok(()));
    for t in all {
        assert_eq!(
            r.config_coverage.as_ref().unwrap()[t.as_str()].coverage,
            "captured",
            "{t}"
        );
        assert_eq!(model_of(&b, t).replication_factor, Some(1), "{t}");
    }

    // COMPACTED: its overrides portable, retention inherited, the archive's 3
    // partitions, and NO owner — its only KafkaTopic is unmanaged (a control).
    let m = model_of(&b, &compacted);
    assert_eq!(m.partitions, Some(3));
    assert_eq!(entry_of(m, "cleanup.policy"), portable("compact"));
    assert_eq!(entry_of(m, "min.compaction.lag.ms"), portable("1000"));
    assert_eq!(entry_of(m, "min.insync.replicas"), portable("1"));
    let retention = entry_of(m, "retention.ms").expect("a semantic key's inherited value");
    assert_eq!(
        retention.2, "inherited",
        "a broker default is never an override"
    );
    assert_ne!(retention.1, "dynamicTopicConfig");
    assert_eq!(
        m.owner, None,
        "an unmanaged KafkaTopic owns nothing (the control)"
    );

    // DELETE-POLICY with min-in-sync 2: every override portable; its
    // KafkaTopic belongs to ANOTHER Strimzi cluster, so no owner (a control).
    let m = model_of(&b, &deleted);
    assert_eq!(m.partitions, Some(2));
    assert_eq!(entry_of(m, "cleanup.policy"), portable("delete"));
    assert_eq!(entry_of(m, "retention.ms"), portable("86400000"));
    assert_eq!(entry_of(m, "min.insync.replicas"), portable("2"));
    assert_eq!(
        m.owner, None,
        "another cluster's KafkaTopic owns nothing (the control)"
    );

    // NO OVERRIDE: nothing portable at all — THE CONTROL that "portable" is
    // the topic's own setting and never a default — and its unlabelled
    // KafkaTopic owns nothing.
    let m = model_of(&b, &plain);
    let entries = m
        .entries
        .as_ref()
        .expect("a successful read records entries");
    assert!(!entries.is_empty(), "semantic defaults are recorded");
    assert!(
        entries.values().all(|e| e.portability == "inherited"),
        "a topic with no override records only inherited values: {entries:?}"
    );
    assert_eq!(m.owner, None, "an unlabelled KafkaTopic owns nothing");
    let after = entry_of(m, "message.timestamp.after.max.ms").expect("a semantic key");
    assert_eq!(
        after.0.as_deref(),
        Some(if line == "3.9" {
            "9223372036854775807"
        } else {
            "3600000"
        }),
        "the line's own inherited default (measured: it changed in 4.0)"
    );

    // 3.9 vs 4.x, and the DECLARED owner.
    let m = model_of(&b, &legacy);
    if line == "3.9" {
        for (key, value) in removed_keys {
            assert_eq!(
                entry_of(m, key),
                Some((
                    Some(value.to_string()),
                    "dynamicTopicConfig".to_string(),
                    "removedInKafka4".to_string()
                )),
                "{key} on 3.9 is recorded, and marked unportable to 4.x"
            );
        }
    } else {
        assert_eq!(
            entry_of(m, "message.timestamp.after.max.ms"),
            portable("3600000")
        );
        for (key, _) in removed_keys {
            assert_eq!(entry_of(m, key), None, "{key} does not exist on 4.x");
        }
    }
    let owner = m.owner.as_ref().expect("the plan declared an owner");
    assert_eq!(
        (
            owner.kind.as_str(),
            owner.basis.as_str(),
            owner.reference.as_str()
        ),
        ("external", "declared", "terraform: kafka_topic.legacy")
    );

    // THE STRIMZI-LABELLED TOPIC: owned by its KafkaTopic.
    let m = model_of(&b, &strimzi);
    let owner = m.owner.as_ref().expect("the labelled KafkaTopic owns it");
    assert_eq!(
        (
            owner.kind.as_str(),
            owner.basis.as_str(),
            owner.reference.as_str()
        ),
        ("strimzi", "kafkaTopicResource", "kafka/strimzi-kt")
    );
    assert_eq!(entry_of(m, "retention.ms"), portable("3600000"));

    // BOTH READERS accept it and print the SAME model lines, one per topic.
    assert_eq!(verified["rust_exit"], 0, "{verified}");
    assert_eq!(verified["python_exit"], 0, "{verified}");
    assert_eq!(
        verified["rust_model_lines"], verified["python_model_lines"],
        "{verified}"
    );
    assert_eq!(
        verified["rust_model_lines"].as_array().unwrap().len(),
        all.len()
    );
    assert!(verified["rust_checked"]
        .as_str()
        .unwrap()
        .contains("all twenty-one"));
    // WHERE THE RUN LOOKED for owners (fix round, M2): the plan's
    // declarations and the `KafkaTopic` resources — so a topic neither owns is
    // "no declarative owner found", applied through the admin API, and an
    // owned one is restored by desired-state export.
    assert_eq!(
        r.owner_detection,
        Some(vec![
            "declared".to_string(),
            "kafkaTopicResources".to_string()
        ])
    );
    assert_eq!(
        record["owner_detection"],
        json!(["declared", "kafkaTopicResources"])
    );
    for line in verified["rust_model_lines"].as_array().unwrap() {
        let line = line.as_str().unwrap();
        assert!(
            line.ends_with(", so restored by desired-state export")
                || line.ends_with(
                    ", no declarative owner found (declared, kafkaTopicResources), so applied \
                     through the admin API"
                ),
            "{line}"
        );
    }

    // THE CATALOG POINT copies the model and the partition count.
    assert_eq!(record["format_version"], "1.3.0");
    for topic in record["topics"].as_array().expect("topics") {
        let name = topic["name"].as_str().unwrap();
        let m = model_of(&b, name);
        assert_eq!(
            topic["configuration"],
            serde_json::to_value(m).unwrap(),
            "{name}"
        );
        assert_eq!(topic["partitions"], json!(m.partitions), "{name}");
    }

    // A WRITER THAT CLASSED A DEFAULT AS AN OVERRIDE is refused by the
    // reader: the live receipt with one class flipped (a negative control on
    // the data, not only on the corpus).
    let mut tampered = r.clone();
    tampered
        .topic_configuration
        .as_mut()
        .unwrap()
        .get_mut(compacted.as_str())
        .unwrap()
        .entries
        .as_mut()
        .unwrap()
        .get_mut("retention.ms")
        .unwrap()
        .portability = "portable".into();
    let refused = tampered
        .validate_invariants()
        .expect_err("arm 17 refuses a default passed off as an override");
    assert!(refused.contains("is \"portable\" from"), "{refused}");
}

// ============================================================ row 3

/// **A denied read records no entries, never an empty set.** See the module
/// doc.
#[test]
#[ignore = "needs the compose stack's acl profile on the 3.9 or 4.x line (module doc)"]
fn a_denied_describe_configs_records_no_entries_and_keeps_the_layout() {
    assert_the_authorizer_is_on();
    let n = nonce();
    let denied = format!("p051-{n}-denied");
    let allowed = format!("p051-{n}-allowed");
    let mut cleanup = Cleanup {
        topics: vec![denied.clone(), allowed.clone()],
        acls: Vec::new(),
    };
    create_topic(&denied, 2, &[("cleanup.policy", "compact")]);
    create_topic(&allowed, 1, &[("retention.ms", "86400000")]);
    produce(&denied, 20);
    produce(&allowed, 20);
    let acl = allow_read_and_describe_only(&denied, true);
    assert!(acl.status.success(), "{}", text(&acl));
    cleanup.acls.push(denied.clone());

    let topics = [denied.as_str(), allowed.as_str()];
    let narrowed = backup(&format!("p051-{n}-denied"), &topics, true, "", None);
    // THE CONTROL: the same topics, backed up by the super user.
    let control = backup(&format!("p051-{n}-control"), &topics, false, "", None);
    let verified = verify_receipt_both_readers(&narrowed);
    let record = catalog_record(&narrowed);
    let s = stack();
    let leaks: Vec<&str> = [
        ("backup run output", text(&narrowed.out)),
        (
            "receipt",
            String::from_utf8_lossy(&narrowed.receipt_bytes).into_owned(),
        ),
        ("catalog record", record.to_string()),
    ]
    .iter()
    .filter(|(_, t)| t.contains(&s.scram_password))
    .map(|(w, _)| *w)
    .collect();
    write_evidence(
        "a_denied_describe_configs_records_no_entries_and_keeps_the_layout",
        &json!({
            "line": line(),
            "narrowed": {
                "config_coverage": narrowed.receipt.config_coverage,
                "topic_configuration": narrowed.receipt.topic_configuration,
            },
            "control": {
                "config_coverage": control.receipt.config_coverage,
                "topic_configuration": control.receipt.topic_configuration,
            },
            "both_readers": verified,
            "catalog_topics": record["topics"],
            "password_found_in": leaks,
        }),
    );

    let coverage = narrowed.receipt.config_coverage.as_ref().unwrap();
    assert_eq!(coverage[denied.as_str()].coverage, "captureDenied");
    let m = model_of(&narrowed, &denied);
    assert_eq!(
        m.entries, None,
        "a denied read records NO entries — never an empty set that reads as 'no overrides'"
    );
    assert_eq!(
        (m.partitions, m.replication_factor),
        (Some(2), Some(1)),
        "the layout still comes from the archive"
    );
    // The readable neighbour: entries beside `manifestDiffers` (the engine's
    // all-or-nothing capture emptied its manifest record; Logweir's own read
    // did not).
    let neighbour = &coverage[allowed.as_str()];
    let m = model_of(&narrowed, &allowed);
    assert!(m.entries.is_some(), "{neighbour:?}");
    assert_eq!(entry_of(m, "retention.ms"), portable("86400000"));
    assert!(
        neighbour.coverage == "captured" || neighbour.reason.as_deref() == Some("manifestDiffers"),
        "{neighbour:?}"
    );
    // THE CONTROL: the super user reads the denied topic's configuration.
    let m = model_of(&control, &denied);
    assert_eq!(entry_of(m, "cleanup.policy"), portable("compact"));
    // Both readers, and no credential anywhere.
    assert_eq!(verified["rust_exit"], 0, "{verified}");
    assert_eq!(verified["python_exit"], 0, "{verified}");
    assert_eq!(
        verified["rust_model_lines"], verified["python_model_lines"],
        "{verified}"
    );
    // NO OWNER WAS LOOKED FOR (fix round, M2): no declaration, no resources —
    // the shape of every controller-run Backup today. The receipt says so,
    // and neither reader calls it the admin-API route.
    assert_eq!(narrowed.receipt.owner_detection, Some(Vec::new()));
    for line in verified["rust_model_lines"].as_array().unwrap() {
        assert!(
            line.as_str()
                .unwrap()
                .ends_with(", owner not checked, so how it is applied is not known"),
            "{line}"
        );
    }
    assert!(
        leaks.is_empty(),
        "the principal's password reached {leaks:?}"
    );
}
