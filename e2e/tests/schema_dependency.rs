#![cfg(feature = "e2e")]
//! **PROD-03.0 — schema-dependent topics, flagged live from the archived
//! bytes, with no registry contacted.**
//!
//! Two rows. `a_backup_flags_raw_framed_records_with_their_ids` runs in CI on
//! the default `auth` stack: raw Confluent framing produced with rdkafka, the
//! real engine, the real archive store (the review's M2). The second row is
//! `#[ignore]`d because it needs the stack's `registry` profile
//! (`e2e/README.md`): Karapace's Schema-Registry-compatible `registry` and its
//! REST proxy `registry-rest`. The TEST produces through the REST proxy, so
//! the records carry the Confluent wire format a real serializer writes —
//! magic byte 0, the registry's schema id, Protobuf's message-index bytes —
//! and checks those bytes on the broker before it backs anything up. Then it
//! STOPS both registry services, so the backup could not reach a registry if
//! it tried, and runs `logweir backup run`.
//!
//! | topic | produced | the receipt must say |
//! |---|---|---|
//! | `avro-kv` | Avro keys and values (REST), then tombstones under the same framed keys (rdkafka) | `schemaDependent`, both sides, the registry's key and value ids, the tombstones as `nulls` |
//! | `json` | JSON Schema values, null keys | `schemaDependent`, the value side, the JSON Schema id |
//! | `proto` | Protobuf values | `schemaDependent`, the Protobuf id |
//! | `mixed` | 6 Avro values and 30 plain JSON values | `schemaDependent` (6 of 36 non-null is above one in ten) |
//! | `plain` | JSON text, string keys | `notDetected` (the control) |
//! | `zeros` | binary values starting with byte 0 and random id bytes, and 4-byte `0,0,0,n` values | `notDetected`, nothing framed (the false-positive control) |
//! | `empty` | nothing | `notAssessed (noRecords)` |
//!
//! Both readers accept the receipt and print the same `schema_dependency`
//! lines; the catalog point copies each topic's entry; and the console's own
//! render (`ui/tests/emit-schema-dependency.js`), over the point the catalog
//! sync lists for that record, names the schema-dependent topics with the
//! live ids under "Registry not captured: applications may not read these
//! records after restore." The evidence is written to
//! `schema-dependency/live.json` under the stack's scratch directory.
//!
//! ```text
//! eval "$(e2e/compose/stack-env.sh --slot N --profiles auth,registry)"
//! just e2e-up
//! cargo build -p logweir
//! LOGWEIR_PYTHON=<python with cryptography> AWS_EC2_METADATA_DISABLED=true \
//!   cargo test -p e2e --features e2e --test schema_dependency -- --ignored --nocapture
//! just e2e-down
//! ```
mod harness;

use harness::{bin, demo_dir, engine_bin, engine_digest, engine_mount, engine_version, root};
use logweir_core::backup_receipt::BackupReceipt;
use logweir_kafka::reader::{AuthConfig, ClusterReader};
use serde_json::{json, Value};
use std::io::Read;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

fn compose() -> Command {
    harness::stack::ensure_coherent();
    let mut c = Command::new("docker");
    c.args([
        "compose",
        "-p",
        &harness::stack::project(),
        "-f",
        "e2e/compose/docker-compose.yml",
        "--profile",
        "registry",
    ])
    .current_dir(root());
    c
}

fn create_topic(topic: &str) {
    let mut c = compose();
    c.args([
        "exec",
        "-T",
        "kafka-broker-1",
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        "kafka-broker-1:9094",
        "--create",
        "--topic",
        topic,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
    ]);
    let o = output_within(c, 120);
    assert!(o.status.success(), "create {topic}:\n{}", text(&o));
    harness::await_created(topic, 1);
}

/// The REST proxy's URL on this stack (`registry-rest`, published port).
fn rest_url() -> String {
    format!(
        "http://localhost:{}",
        harness::stack::port("LOGWEIR_E2E_REGISTRY_REST_PORT")
    )
}

/// One `POST /topics/<topic>` through the REST proxy, in `format` (`avro`,
/// `jsonschema`, `protobuf`), and its answer.
fn rest_produce(topic: &str, format: &str, body: &Value) -> Value {
    let mut c = Command::new("curl");
    c.args(["-sS", "--max-time", "60", "-X", "POST", "-H"])
        .arg(format!(
            "Content-Type: application/vnd.kafka.{format}.v2+json"
        ))
        .args(["-H", "Accept: application/vnd.kafka.v2+json", "--data"])
        .arg(body.to_string())
        .arg(format!("{}/topics/{topic}", rest_url()));
    let o = output_within(c, 90);
    assert!(o.status.success(), "REST produce {topic}:\n{}", text(&o));
    let v: Value = serde_json::from_slice(&o.stdout)
        .unwrap_or_else(|e| panic!("REST answer for {topic}: {e}\n{}", text(&o)));
    assert!(
        v["offsets"]
            .as_array()
            .is_some_and(|a| a.iter().all(|x| x.get("error").is_none_or(Value::is_null))),
        "REST produce {topic} failed: {v}"
    );
    v
}

/// Raw records through rdkafka: `(key, value)`, `None` is null.
/// A record's key and value bytes, `None` for null.
type Raw = (Option<Vec<u8>>, Option<Vec<u8>>);

fn produce_raw(topic: &str, records: &[Raw]) {
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    let producer: BaseProducer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", harness::bootstrap())
        .set("message.timeout.ms", "10000")
        .set("acks", "1")
        .create()
        .expect("a producer");
    for (k, v) in records {
        let mut rec: BaseRecord<'_, [u8], [u8]> = BaseRecord::to(topic);
        if let Some(k) = k {
            rec = rec.key(k.as_slice());
        }
        if let Some(v) = v {
            rec = rec.payload(v.as_slice());
        }
        loop {
            match producer.send(rec) {
                Ok(()) => break,
                Err((e, back)) if e.to_string().contains("QueueFull") => {
                    rec = back;
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

fn reader() -> logweir_kafka::rdkafka_reader::RdKafkaReader {
    logweir_kafka::rdkafka_reader::RdKafkaReader::connect(
        &[harness::bootstrap()],
        AuthConfig::Plaintext,
    )
    .expect("a PLAINTEXT reader")
}

/// The first `n` records of partition 0, as the broker holds them.
fn on_the_broker(topic: &str, n: usize) -> Vec<logweir_kafka::reader::ConsumedRecord> {
    let got = reader()
        .consume_range(topic, 0, 0, n)
        .unwrap_or_else(|e| panic!("consume {topic}: {e}"));
    assert_eq!(
        got.len(),
        n,
        "{topic}: {} of {n} records read back",
        got.len()
    );
    got
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// The schema id a Confluent frame names (bytes 1..5), asserting the magic.
fn frame_id(b: &[u8]) -> u32 {
    assert!(b.len() > 5 && b[0] == 0, "not framed: {}", hex(b));
    u32::from_be_bytes([b[1], b[2], b[3], b[4]])
}

fn nonce() -> String {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970")
        .as_nanos();
    format!("{:08}", n % 100_000_000)
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

fn use_stack_s3_env() {
    let minio = ["minio", "admin"].concat();
    std::env::set_var("AWS_ACCESS_KEY_ID", &minio);
    std::env::set_var("AWS_SECRET_ACCESS_KEY", &minio);
    std::env::set_var("AWS_REGION", "us-east-1");
}

fn archive_store(backup_id: &str) -> logweir_engine_oso::storage::Store {
    use_stack_s3_env();
    let url: logweir_core::engine::StorageUrl = serde_yaml::from_str(&format!(
        "backend: s3\nbucket: {}\nprefix: {backup_id}\nregion: us-east-1\nendpoint: {}\n\
         path_style: true\nallow_http: true\n",
        harness::ARCHIVE_BUCKET,
        harness::s3_endpoint()
    ))
    .expect("a storage url");
    logweir_engine_oso::storage::Store::read_only_from_url(&url).expect("the archive store")
}

fn backup(backup_id: &str, topics: &[String]) -> Backup {
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
            harness::bootstrap(),
            topics.join(", "),
            harness::ARCHIVE_BUCKET,
            harness::s3_endpoint()
        ),
    )
    .expect("the backup spec");
    let allowed = demo_dir().join("prod030-backup-allowed-clusters.json");
    std::fs::write(
        &allowed,
        "{\"allowed_cluster_ids\": [\"SCRATCH-CLUSTER-NOT-THE-SOURCE\"]}\n",
    )
    .expect("written");
    let minio = ["minio", "admin"].concat();
    let mut c = Command::new(bin());
    c.args(["backup", "run", "--spec"])
        .arg(&spec)
        .arg("--allowed-clusters")
        .arg(&allowed)
        .arg("--signing-key")
        .arg(root().join("e2e/fixtures/signed/signing.pem"))
        .env("AWS_ACCESS_KEY_ID", &minio)
        .env("AWS_SECRET_ACCESS_KEY", &minio)
        .env("AWS_REGION", "us-east-1")
        .env("LOGWEIR_ENGINE_BIN", engine_bin())
        .env("LOGWEIR_ENGINE_VERSION", engine_version())
        .env("LOGWEIR_ENGINE_DIGEST", engine_digest())
        .env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount())
        .env("TMPDIR", engine_mount())
        .env("RUST_LOG", "info");
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
    let (receipt_bytes, _) = archive_store(backup_id)
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

/// Both readers over the receipt, exactly as an auditor runs them.
fn both_readers(b: &Backup) -> (Output, Output) {
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
    let mut py = Command::new(harness::auditor_python());
    py.arg(root().join("docs/verify_scorecard.py"))
        .args(["--payload-type", "backup-receipt"])
        .arg(&doc)
        .arg(&sig)
        .arg(&pubkey);
    (output_within(rust, 60), output_within(py, 60))
}

fn schema_lines(o: &Output) -> Vec<String> {
    text(o)
        .lines()
        .filter_map(|l| {
            let i = l
                .find("schema_dependency[")
                .or_else(|| l.find("schema_dependency:"))?;
            Some(l[i..].to_string())
        })
        .collect()
}

/// **The review's M2: the row CI runs**, on its default `COMPOSE_PROFILES=auth`
/// stack — no registry, no node. Confluent framing written raw (magic byte 0,
/// a 4-byte big-endian id, a payload) into three topics over two partitions,
/// backed up with the real engine into the real archive store, and read back:
/// the receipt flags exactly the framed sides with their ids, judged whole,
/// and the plain topic is `notDetected`. A store read, a decoder or a
/// detection that silently degraded to `notAssessed` fails here.
#[test]
fn a_backup_flags_raw_framed_records_with_their_ids() {
    let n = nonce();
    let t = |s: &str| format!("p030ci-{n}-{s}");
    let names = ["framed", "keyed", "plain"];
    let topics: Vec<String> = names.iter().map(|s| t(s)).collect();
    for topic in &topics {
        harness::create_topic(topic, 2);
    }
    let frame = |id: u32, body: &[u8]| {
        let mut v = vec![0u8];
        v.extend_from_slice(&id.to_be_bytes());
        v.extend_from_slice(body);
        v
    };
    // Values under two schema versions, string keys.
    produce_raw(
        &t("framed"),
        &(0..40u32)
            .map(|i| {
                (
                    Some(format!("order-{i}").into_bytes()),
                    Some(frame(101 + i % 2, &[0x02, 0x06, b'a', b'b', b'c'])),
                )
            })
            .collect::<Vec<_>>(),
    );
    // Framed keys, plain JSON values.
    produce_raw(
        &t("keyed"),
        &(0..30u32)
            .map(|i| {
                (
                    Some(frame(7, &i.to_be_bytes())),
                    Some(format!("{{\"n\":{i}}}").into_bytes()),
                )
            })
            .collect::<Vec<_>>(),
    );
    produce_raw(
        &t("plain"),
        &(0..20u32)
            .map(|i| {
                (
                    Some(format!("k{i}").into_bytes()),
                    Some(format!("{{\"id\":{i}}}").into_bytes()),
                )
            })
            .collect::<Vec<_>>(),
    );

    let b = backup(&format!("p030ci-{n}"), &topics);
    let r = &b.receipt;
    assert_eq!(r.format_version, "1.5.0");
    r.validate_invariants()
        .expect("the signed receipt is valid");
    let sd = r
        .schema_dependency
        .as_ref()
        .expect("every receipt carries the block");
    let framed = &sd[&t("framed")];
    assert_eq!(framed.verdict, "schemaDependent", "{framed:?}");
    assert_eq!(framed.basis.as_deref(), Some("complete"), "{framed:?}");
    let v = framed.value.as_ref().unwrap();
    assert_eq!(
        (v.framed, v.unframed, v.schema_ids.clone()),
        (40, 0, vec![101, 102])
    );
    assert!(!framed.key.as_ref().unwrap().dependent, "string keys");
    let keyed = &sd[&t("keyed")];
    assert_eq!(keyed.verdict, "schemaDependent", "{keyed:?}");
    assert_eq!(keyed.key.as_ref().unwrap().schema_ids, vec![7]);
    assert!(!keyed.value.as_ref().unwrap().dependent, "JSON values");
    let plain = &sd[&t("plain")];
    assert_eq!(plain.verdict, "notDetected", "{plain:?}");
    assert_eq!(plain.basis.as_deref(), Some("complete"));

    // The Rust reader verifies it and says it.
    let dir = demo_dir().join(format!("{}-verify", b.backup_id));
    std::fs::create_dir_all(&dir).unwrap();
    let (doc, sig) = (dir.join("receipt.json"), dir.join("receipt.sig"));
    std::fs::write(&doc, &b.receipt_bytes).unwrap();
    let (sidecar, _) = archive_store(&b.backup_id)
        .get(&b.receipt_key.replace(".receipt.json", ".receipt.sig"))
        .expect("the sidecar");
    std::fs::write(&sig, sidecar).unwrap();
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
    .arg(root().join("e2e/fixtures/signed/public.pem"));
    let rust = output_within(rust, 60);
    assert_eq!(rust.status.code(), Some(0), "{}", text(&rust));
    let lines = schema_lines(&rust);
    assert!(
        lines.iter().any(|l| l.starts_with(&format!(
            "schema_dependency[\"{}\"]: schema-dependent, registry not captured",
            t("framed")
        )) && l.contains("schema ids 101, 102")),
        "{lines:?}"
    );
}

#[test]
#[ignore = "needs the compose stack with the `registry` profile; see the module doc"]
fn a_backup_flags_the_schema_dependent_topics_from_their_bytes() {
    let n = nonce();
    let t = |s: &str| format!("p030-{n}-{s}");
    let names = [
        "avro-kv", "json", "proto", "mixed", "plain", "zeros", "empty",
    ];
    let topics: Vec<String> = names.iter().map(|s| t(s)).collect();
    for topic in &topics {
        create_topic(topic);
    }

    // ---- real serializers, through the registry's REST proxy ----
    let order = r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"int"},{"name":"note","type":"string"}]}"#;
    let avro_records: Vec<Value> = (0..30)
        .map(|i| json!({"key": format!("order-{i}"), "value": {"id": i, "note": "n"}}))
        .collect();
    let avro = rest_produce(
        &t("avro-kv"),
        "avro",
        &json!({"key_schema": r#"{"type":"string"}"#, "value_schema": order, "records": avro_records}),
    );
    let avro_key_id = avro["key_schema_id"].as_u64().expect("a key schema id") as u32;
    let avro_value_id = avro["value_schema_id"].as_u64().expect("a value schema id") as u32;
    let js = rest_produce(
        &t("json"),
        "jsonschema",
        &json!({
            "value_schema": r#"{"type":"object","properties":{"id":{"type":"integer"}}}"#,
            "records": (0..20).map(|i| json!({"value": {"id": i}})).collect::<Vec<_>>(),
        }),
    );
    let json_id = js["value_schema_id"].as_u64().expect("a JSON Schema id") as u32;
    let proto = rest_produce(
        &t("proto"),
        "protobuf",
        &json!({
            "value_schema": "syntax = \"proto3\";\nmessage Order { int32 id = 1; string note = 2; }\nmessage Other { int32 x = 1; }",
            "records": (0..20).map(|i| json!({"value": {"id": i, "note": "p"}})).collect::<Vec<_>>(),
        }),
    );
    let proto_id = proto["value_schema_id"].as_u64().expect("a Protobuf id") as u32;
    let mixed = rest_produce(
        &t("mixed"),
        "avro",
        &json!({
            "value_schema": r#"{"type":"record","name":"Legacy","fields":[{"name":"v","type":"long"}]}"#,
            "records": (0..6).map(|i| json!({"value": {"v": i}})).collect::<Vec<_>>(),
        }),
    );
    let mixed_id = mixed["value_schema_id"].as_u64().expect("an id") as u32;

    // ---- the bytes on the broker ARE the wire format, before any backup ----
    let avro_on = on_the_broker(&t("avro-kv"), 30);
    assert_eq!(frame_id(avro_on[0].key.as_ref().unwrap()), avro_key_id);
    assert_eq!(frame_id(avro_on[0].value.as_ref().unwrap()), avro_value_id);
    let json_on = on_the_broker(&t("json"), 1);
    let json_value = json_on[0].value.clone().unwrap();
    assert_eq!(frame_id(&json_value), json_id);
    assert_eq!(json_value[5], b'{', "JSON text after the id");
    let proto_on = on_the_broker(&t("proto"), 1);
    let proto_value = proto_on[0].value.clone().unwrap();
    assert_eq!(frame_id(&proto_value), proto_id);
    assert_eq!(
        proto_value[5], 0,
        "Protobuf's message index [0] after the id"
    );

    // ---- raw producers: tombstones, the controls, the mixed half ----
    let framed_keys: Vec<Vec<u8>> = avro_on
        .iter()
        .take(5)
        .map(|r| r.key.clone().unwrap())
        .collect();
    produce_raw(
        &t("avro-kv"),
        &framed_keys
            .iter()
            .map(|k| (Some(k.clone()), None))
            .collect::<Vec<_>>(),
    );
    produce_raw(
        &t("mixed"),
        &(0..30)
            .map(|i| (None, Some(format!("{{\"legacy\":{i}}}").into_bytes())))
            .collect::<Vec<_>>(),
    );
    produce_raw(
        &t("plain"),
        &(0..20)
            .map(|i| {
                (
                    Some(format!("plain-{i:06}").into_bytes()),
                    Some(format!("{{\"id\":{i},\"topic\":\"plain\"}}").into_bytes()),
                )
            })
            .collect::<Vec<_>>(),
    );
    // A zero byte, then four "id" bytes whose first is never zero (an id at or
    // above 2^24), then a payload; and 4-byte big-endian integers.
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = || {
        x = x
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (x >> 33) as u8
    };
    let mut zeros: Vec<Raw> = (0..50)
        .map(|_| {
            let mut v = vec![0u8, next() | 1];
            v.extend((0..10).map(|_| next()));
            (None, Some(v))
        })
        .collect();
    zeros.extend((1..=10u32).map(|i| (None, Some(i.to_be_bytes().to_vec()))));
    produce_raw(&t("zeros"), &zeros);

    // ---- no registry from here on: the backup must not need one ----
    let mut stop = compose();
    stop.args(["stop", "registry-rest", "registry"]);
    let o = output_within(stop, 120);
    assert!(o.status.success(), "stop the registry:\n{}", text(&o));

    let b = backup(&format!("p030-{n}"), &topics);
    let r = &b.receipt;
    assert_eq!(r.format_version, "1.5.0");
    r.validate_invariants()
        .expect("the signed receipt is valid");
    let sd = r
        .schema_dependency
        .as_ref()
        .expect("every receipt carries the block");
    let at = |s: &str| sd.get(&t(s)).unwrap_or_else(|| panic!("{s} judged"));

    let kv = at("avro-kv");
    assert_eq!(kv.verdict, "schemaDependent");
    assert_eq!(kv.basis.as_deref(), Some("complete"));
    let (k, v) = (kv.key.as_ref().unwrap(), kv.value.as_ref().unwrap());
    assert!(k.dependent && v.dependent);
    assert_eq!(k.schema_ids, vec![avro_key_id]);
    assert_eq!(v.schema_ids, vec![avro_value_id]);
    assert_eq!(
        (k.framed, v.framed, v.nulls),
        (35, 30, 5),
        "tombstones are nulls"
    );
    let json_e = at("json");
    assert_eq!(json_e.verdict, "schemaDependent");
    assert_eq!(json_e.value.as_ref().unwrap().schema_ids, vec![json_id]);
    assert_eq!(json_e.key.as_ref().unwrap().nulls, 20);
    assert!(!json_e.key.as_ref().unwrap().dependent);
    let proto_e = at("proto");
    assert_eq!(proto_e.verdict, "schemaDependent");
    assert_eq!(proto_e.value.as_ref().unwrap().schema_ids, vec![proto_id]);
    let mixed_e = at("mixed");
    assert_eq!(mixed_e.verdict, "schemaDependent");
    let mv = mixed_e.value.as_ref().unwrap();
    assert_eq!((mv.framed, mv.unframed), (6, 30));
    assert_eq!(mv.schema_ids, vec![mixed_id]);
    // The controls.
    assert_eq!(at("plain").verdict, "notDetected");
    let zeros_e = at("zeros");
    assert_eq!(zeros_e.verdict, "notDetected");
    assert_eq!(zeros_e.value.as_ref().unwrap().framed, 0, "{zeros_e:?}");
    assert_eq!(zeros_e.value.as_ref().unwrap().unframed, 60);
    let empty = at("empty");
    assert_eq!(
        (empty.verdict.as_str(), empty.reason.as_deref()),
        ("notAssessed", Some("noRecords"))
    );

    // ---- both readers ----
    let (rust, py) = both_readers(&b);
    assert_eq!(
        rust.status.code(),
        Some(0),
        "drill verify:\n{}",
        text(&rust)
    );
    assert_eq!(
        py.status.code(),
        Some(0),
        "verify_scorecard.py:\n{}",
        text(&py)
    );
    let (rust_lines, py_lines) = (schema_lines(&rust), schema_lines(&py));
    assert_eq!(rust_lines, py_lines, "the two readers print the same lines");
    assert_eq!(rust_lines.len(), names.len());
    let line_of = |s: &str| {
        rust_lines
            .iter()
            .find(|l| l.starts_with(&format!("schema_dependency[\"{}\"]", t(s))))
            .unwrap_or_else(|| panic!("a line for {s}"))
            .clone()
    };
    assert!(line_of("avro-kv").contains("schema-dependent, registry not captured"));
    assert!(line_of("avro-kv").contains(&format!("schema ids {avro_value_id}")));
    assert!(line_of("plain").contains("no schema framing detected"));
    assert!(line_of("empty").contains("not assessed (noRecords)"));

    // ---- the catalog point copies it ----
    let key = b
        .catalog_key
        .as_ref()
        .expect("the catalog point was written");
    let (bytes, _) = archive_store(&b.backup_id)
        .get(key)
        .unwrap_or_else(|e| panic!("read {key}: {e}"));
    let record: Value = serde_json::from_slice(&bytes).expect("a record");
    assert_eq!(record["format_version"], "1.5.0");
    for topic in record["topics"].as_array().unwrap() {
        let name = topic["name"].as_str().unwrap();
        assert_eq!(
            topic["schema_dependency"],
            serde_json::to_value(&sd[name]).unwrap(),
            "{name}"
        );
    }

    // ---- the console, over the point the catalog sync lists for it ----
    // The sync Job's own projection (the runner), and the API's field-for-field
    // view of it (held by d3_reads.rs over the shared fixture).
    let point_topics: Vec<Value> = record["topics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| {
            let d: logweir_core::backup_receipt::TopicSchemaDependency =
                serde_json::from_value(t["schema_dependency"].clone()).unwrap();
            json!({
                "name": t["name"],
                "applyRoute": "unknown",
                "schemaDependency": logweir::check::kinds::catalog_sync::EntrySchemaDependency::of(&d),
            })
        })
        .collect();
    let point = json!({
        "pointId": record["point_id"], "availability": "Available", "verification": "Verified",
        "selectable": true, "topics": point_topics, "locations": [],
    });
    let dir = demo_dir().join("schema-dependency");
    std::fs::create_dir_all(&dir).unwrap();
    let point_file = dir.join("point.json");
    std::fs::write(&point_file, serde_json::to_vec_pretty(&point).unwrap()).unwrap();
    let mut node = Command::new("node");
    node.arg(root().join("ui/tests/emit-schema-dependency.js"))
        .arg(&point_file)
        .arg(topics.join(","));
    let console = output_within(node, 60);
    assert!(
        console.status.success(),
        "the console render:\n{}",
        text(&console)
    );
    let shown = String::from_utf8_lossy(&console.stdout).into_owned();
    let review = shown
        .split("== review ==")
        .nth(1)
        .expect("the review block");
    assert!(
        review.contains(
            "Registry not captured: applications may not read these records after restore."
        ),
        "{shown}"
    );
    assert!(
        review.contains(&format!(
            "{} (key and value: schema ids {}, {}; complete)",
            t("avro-kv"),
            avro_key_id.min(avro_value_id),
            avro_key_id.max(avro_value_id)
        )),
        "{shown}"
    );
    assert!(
        review.contains(&format!(
            "{} (value: schema ids {json_id}; complete)",
            t("json")
        )),
        "{shown}"
    );
    assert!(
        review.contains(&format!("Not assessed: {}.", t("empty"))),
        "{shown}"
    );
    assert!(
        !review.contains(&format!("{} (", t("plain"))),
        "a control is never named: {shown}"
    );
    let catalog = shown
        .split("== catalog ==")
        .nth(1)
        .unwrap()
        .split("== recovery point ==")
        .next()
        .unwrap();
    assert!(catalog.contains("Schema-dependent topics"), "{shown}");

    std::fs::write(
        dir.join("live.json"),
        serde_json::to_vec_pretty(&json!({
            "registry_ids": {
                "avro-kv key": avro_key_id, "avro-kv value": avro_value_id, "json": json_id,
                "proto": proto_id, "mixed": mixed_id,
            },
            "on_the_broker": {
                "avro-kv key": hex(avro_on[0].key.as_ref().unwrap()),
                "avro-kv value": hex(avro_on[0].value.as_ref().unwrap()),
                "json value": hex(&json_value),
                "proto value": hex(&proto_value),
            },
            "registry_stopped_before_the_backup": true,
            "receipt_key": b.receipt_key,
            "receipt_format_version": r.format_version,
            "schema_dependency": sd,
            "catalog_key": key,
            "catalog_record_topics": record["topics"],
            "rust_lines": rust_lines,
            "python_lines": py_lines,
            "console": shown,
            "backup_log_schema_lines": text(&b.out).lines()
                .filter(|l| l.contains("schema dependency") || l.contains("schema_dependency"))
                .collect::<Vec<_>>(),
        }))
        .unwrap(),
    )
    .unwrap();
    eprintln!("[prod-03-0] evidence: {}", dir.join("live.json").display());
}
