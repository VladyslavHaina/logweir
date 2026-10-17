#![cfg(feature = "e2e")]
//! **PROD-03.1 / 03.2 — the Schema Registry captured with a backup and
//! imported, keeping its IDs, before a restore creates any topic**
//! (`docs/to-do/decisions/OD-23-core.md` §1.4, rows SR-1 to SR-10).
//!
//! The stack's `registry` profile: Karapace (`registry`) is the SOURCE
//! registry, `registry-target` (Redpanda v26.2.4's Schema Registry, HTTP basic
//! auth) the TARGET; `rpk topic consume --use-schema-registry` inside
//! `registry-target` is the deserializing consumer. The source holds, per run
//! (every name carries a nonce): an Avro value with a reference and two
//! versions, the second soft-deleted; a Protobuf message importing another; a
//! JSON Schema; and a subject named by RecordNameStrategy. The records are
//! framed by hand (magic byte, the source ID, the encoded body) and produced
//! with rdkafka.
//!
//! | row | what it proves |
//! |---|---|
//! | SR-1 | the receipt records every subject-version and ID, the soft-deleted one flagged, the snapshot's digest is the object's, and both verifiers accept the receipt; control: the registry stopped, the backup still succeeds and says `registryUnreachable`, and so does its catalog point |
//! | SR-3 | a wrong registry password is `registryUnauthorized` and the backup succeeds; a registry Secret bound elsewhere refuses the run `CredentialBindingMismatch` before any request (exit 3); control: the right binding captures |
//! | SR-4, SR-5 | a restore into the empty target imports the closure with the source's IDs; the consumer decodes every restored record (Avro with a reference, Protobuf with an import, JSON, RecordNameStrategy); control: the same restore with no target registry imports nothing and the consumer cannot decode |
//! | SR-6 | a target ID held by another schema refuses `SchemaConflict`: no target topic, nothing registered |
//! | SR-7 | Karapace as the target refuses `SchemaRegistryCannotKeepIds`: no target topic, nothing registered |
//! | SR-8 | a closure already half in the target (one subject) is continued: the rest is imported, the consumer decodes |
//! | SR-9 | the second restore into the same target, and a restore into the source's own registry, write nothing |
//! | SR-10 | both verifiers print the `schemas:` line of the scorecards above and the `schema_registry` line of the receipts |
//! | checks | `registry.reachable` and `target.schemas` answer from the live registries (`the_registry_check_rows_answer_from_the_live_registries`); controls: a wrong password, a conflicting ID |
//!
//! SR-2 (a reference to a hard-deleted version) is held by
//! `crates/logweir-core/tests/schema_registry.rs::unresolved_references_are_listed`:
//! Karapace refuses deleting a version another schema references.
//!
//! ```text
//! eval "$(e2e/compose/stack-env.sh --slot N --kafka 4.3 --profiles registry)"
//! just e2e-up
//! cargo build -p logweir
//! LOGWEIR_PYTHON=<python with cryptography> AWS_EC2_METADATA_DISABLED=true \
//!   cargo test -p e2e --features e2e --test schema_registry -- --ignored --nocapture
//! just e2e-down
//! ```
mod harness;

use harness::{bin, demo_dir, engine_bin, engine_digest, engine_mount, engine_version, root};
use logweir_core::backup_receipt::BackupReceipt;
use serde_json::{json, Value};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const TARGET_USER: &str = "logweir";
/// The stack's fixture password (`e2e/compose/docker-compose.yml`), split so
/// no line holds it whole.
fn target_password() -> String {
    ["logweir-e2e", "-not-a-secret"].concat()
}

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
    let ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("sr{:x}", ns % 0xffff_ffff)
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

fn compose_ok(args: &[&str], what: &str) -> String {
    let mut c = compose();
    c.args(args);
    let o = output_within(c, 300);
    assert!(o.status.success(), "{what}:\n{}", text(&o));
    String::from_utf8_lossy(&o.stdout).to_string()
}

// ============================================================ the registries

fn source_url() -> String {
    format!(
        "http://localhost:{}",
        harness::stack::port("LOGWEIR_E2E_REGISTRY_PORT")
    )
}

fn target_url() -> String {
    format!(
        "http://localhost:{}",
        harness::stack::port("LOGWEIR_E2E_REGISTRY_TARGET_PORT")
    )
}

/// One registry call through curl: the status and the body as JSON.
fn http(method: &str, url: &str, body: Option<&Value>, auth: bool) -> (u16, Value) {
    let mut c = Command::new("curl");
    c.args(["-sS", "--max-time", "30", "-X", method])
        .args(["-H", "Content-Type: application/vnd.schemaregistry.v1+json"])
        .args(["-w", "\n%{http_code}"]);
    if auth {
        c.arg("-u")
            .arg(format!("{TARGET_USER}:{}", target_password()));
    }
    if let Some(b) = body {
        c.arg("--data").arg(b.to_string());
    }
    c.arg(url);
    let o = output_within(c, 60);
    let out = String::from_utf8_lossy(&o.stdout).to_string();
    let (body, status) = out.rsplit_once('\n').unwrap_or(("", "0"));
    (
        status.trim().parse().unwrap_or(0),
        serde_json::from_str(body).unwrap_or(Value::Null),
    )
}

fn enc(s: &str) -> String {
    logweir_core::schema_registry::encode(s)
}

/// Register `schema` under `subject` in the source registry; its ID.
fn register(subject: &str, schema_type: &str, schema: &str, refs: Value) -> u32 {
    let (status, body) = http(
        "POST",
        &format!("{}/subjects/{}/versions", source_url(), enc(subject)),
        Some(&json!({"schemaType": schema_type, "schema": schema, "references": refs})),
        false,
    );
    assert_eq!(status, 200, "register {subject}: {body}");
    body["id"].as_u64().expect("an id") as u32
}

/// The target's answer for one source ID: its schema text, or `None`.
fn target_schema(id: u32) -> Option<String> {
    let (status, body) = http(
        "GET",
        &format!("{}/schemas/ids/{id}", target_url()),
        None,
        true,
    );
    (status == 200).then(|| body["schema"].as_str().unwrap_or_default().to_string())
}

fn target_subjects() -> Vec<String> {
    let (_, body) = http("GET", &format!("{}/subjects", target_url()), None, true);
    serde_json::from_value(body).unwrap_or_default()
}

// ============================================================ the records

fn varint(mut v: u64, out: &mut Vec<u8>) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
}

fn zigzag(v: i64, out: &mut Vec<u8>) {
    varint(((v << 1) ^ (v >> 63)) as u64, out);
}

fn avro_string(s: &str, out: &mut Vec<u8>) {
    zigzag(s.len() as i64, out);
    out.extend_from_slice(s.as_bytes());
}

fn framed(id: u32, body: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8];
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn produce(topic: &str, values: &[Vec<u8>]) {
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    let producer: BaseProducer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", harness::bootstrap())
        .set("message.timeout.ms", "20000")
        .set("acks", "all")
        .create()
        .expect("a producer");
    for v in values {
        let rec: BaseRecord<'_, [u8], [u8]> = BaseRecord::to(topic).payload(v.as_slice());
        producer
            .send(rec)
            .unwrap_or_else(|(e, _)| panic!("produce to {topic}: {e}"));
    }
    producer
        .flush(Duration::from_secs(30))
        .unwrap_or_else(|e| panic!("flush {topic}: {e}"));
}

/// The subjects, IDs and topics of one run's source state.
struct Source {
    n: String,
    /// subject -> the IDs it holds.
    ids: Vec<(String, u32)>,
    topics: Vec<String>,
    /// topic -> what each decoded value must contain.
    expect: Vec<(String, Vec<String>)>,
}

/// The full source state of SR-1/SR-4/SR-5: Avro with a reference and two
/// versions (the second soft-deleted), Protobuf with an import, JSON Schema,
/// and a RecordNameStrategy subject.
fn populate_source(n: &str) -> Source {
    let ns = format!("com.ex{n}");
    let customer = format!("{n}-customer");
    let orders = format!("{n}-orders-value");
    let common = format!("{n}-common.proto");
    let events = format!("{n}-events-value");
    let audit = format!("{n}-audit-value");
    let rns = format!("{ns}.Order");
    let c_id = register(
        &customer,
        "AVRO",
        &json!({"type":"record","name":"Customer","namespace":ns,"fields":[{"name":"id","type":"string"}]}).to_string(),
        json!([]),
    );
    let order_refs = json!([{"name": format!("{ns}.Customer"), "subject": customer, "version": 1}]);
    let o1 = register(
        &orders,
        "AVRO",
        &json!({"type":"record","name":"Ord","namespace":ns,"fields":[{"name":"n","type":"int"},{"name":"customer","type":format!("{ns}.Customer")}]}).to_string(),
        order_refs.clone(),
    );
    let o2 = register(
        &orders,
        "AVRO",
        &json!({"type":"record","name":"Ord","namespace":ns,"fields":[{"name":"n","type":"int"},{"name":"customer","type":format!("{ns}.Customer")},{"name":"note","type":["null","string"],"default":null}]}).to_string(),
        order_refs,
    );
    let pkg = format!("ex{n}");
    let m_id = register(
        &common,
        "PROTOBUF",
        &format!("syntax = \"proto3\";\npackage {pkg};\nmessage Money {{ int64 cents = 1; }}\n"),
        json!([]),
    );
    let e_id = register(
        &events,
        "PROTOBUF",
        &format!(
            "syntax = \"proto3\";\npackage {pkg};\nimport \"{common}\";\nmessage Paid {{ string id = 1; {pkg}.Money amount = 2; }}\n"
        ),
        json!([{"name": common, "subject": common, "version": 1}]),
    );
    let j_id = register(
        &audit,
        "JSON",
        &json!({"title": n, "type":"object","properties":{"who":{"type":"string"}}}).to_string(),
        json!([]),
    );
    let r_id = register(
        &rns,
        "AVRO",
        &json!({"type":"record","name":"Order","namespace":ns,"fields":[{"name":"k","type":"string"}]}).to_string(),
        json!([]),
    );
    // The second Order version is soft-deleted after its records are written.
    let topics: Vec<String> = ["orders", "events", "audit", "rns"]
        .iter()
        .map(|t| format!("{n}-{t}"))
        .collect();
    for t in &topics {
        harness::create_topic(t, 1);
    }
    let order = |i: i64, v2: bool| {
        let mut b = Vec::new();
        zigzag(i, &mut b);
        avro_string(&format!("cust{i}"), &mut b);
        if v2 {
            b.push(0); // the union's null branch
        }
        framed(if v2 { o2 } else { o1 }, &b)
    };
    produce(
        &topics[0],
        &[
            order(0, false),
            order(1, false),
            order(2, false),
            order(3, true),
            order(4, true),
        ],
    );
    let paid = |i: u64| {
        let id = format!("pay{i}");
        let mut money = vec![0x08];
        varint(100 + i, &mut money);
        let mut b = vec![0x00, 0x0a, id.len() as u8];
        b.extend_from_slice(id.as_bytes());
        b.push(0x12);
        b.push(money.len() as u8);
        b.extend_from_slice(&money);
        framed(e_id, &b)
    };
    produce(&topics[1], &[paid(0), paid(1), paid(2)]);
    let who = |i: u32| framed(j_id, json!({"who": format!("w{i}")}).to_string().as_bytes());
    produce(&topics[2], &[who(0), who(1), who(2)]);
    let rec = |i: u32| {
        let mut b = Vec::new();
        avro_string(&format!("rk{i}"), &mut b);
        framed(r_id, &b)
    };
    produce(&topics[3], &[rec(0), rec(1), rec(2)]);
    let (status, body) = http(
        "DELETE",
        &format!("{}/subjects/{}/versions/2", source_url(), enc(&orders)),
        None,
        false,
    );
    assert_eq!(status, 200, "soft-delete {orders} v2: {body}");
    Source {
        n: n.to_string(),
        ids: vec![
            (customer, c_id),
            (orders.clone(), o1),
            (orders, o2),
            (common, m_id),
            (events, e_id),
            (audit, j_id),
            (rns, r_id),
        ],
        expect: vec![
            (topics[0].clone(), vec!["cust0".into(), "cust4".into()]),
            (topics[1].clone(), vec!["pay0".into(), "pay2".into()]),
            (topics[2].clone(), vec!["w0".into(), "w2".into()]),
            (topics[3].clone(), vec!["rk0".into(), "rk2".into()]),
        ],
        topics,
    }
}

// ============================================================ the pipeline

fn s3_user() -> String {
    ["minio", "admin"].concat()
}

fn signing_pem() -> PathBuf {
    root().join("e2e/fixtures/signed/signing.pem")
}

fn public_pem() -> PathBuf {
    root().join("e2e/fixtures/signed/public.pem")
}

fn engine_env(c: &mut Command) {
    c.env("AWS_ACCESS_KEY_ID", s3_user())
        .env("AWS_SECRET_ACCESS_KEY", s3_user())
        .env("AWS_REGION", "us-east-1")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .env("LOGWEIR_ENGINE_BIN", engine_bin())
        .env("LOGWEIR_ENGINE_VERSION", engine_version())
        .env("LOGWEIR_ENGINE_DIGEST", engine_digest())
        .env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount())
        .env("TMPDIR", engine_mount());
}

fn storage_yaml(prefix: &str, indent: &str) -> String {
    format!(
        "{indent}backend: s3\n\
         {indent}bucket: {}\n\
         {indent}prefix: {prefix}\n\
         {indent}region: us-east-1\n\
         {indent}endpoint: {}\n\
         {indent}path_style: true\n\
         {indent}allow_http: true\n",
        harness::ARCHIVE_BUCKET,
        harness::s3_endpoint()
    )
}

fn registry_yaml(url: &str, user: Option<&str>, indent: &str) -> String {
    let mut s = format!("{indent}schema_registry:\n{indent}  url: {url}\n");
    if let Some(u) = user {
        s.push_str(&format!("{indent}  username: {u}\n"));
    }
    s
}

struct Backup {
    out: Output,
    backup_id: String,
    prefix: String,
    receipt_key: String,
    sidecar_key: String,
    receipt_bytes: Vec<u8>,
    receipt: BackupReceipt,
}

/// `logweir backup run` of `topics` under `prefix`, with `registry` (YAML) and
/// extra `env`.
fn backup(
    backup_id: &str,
    prefix: &str,
    topics: &[String],
    registry: &str,
    env: &[(&str, String)],
) -> Result<Backup, Output> {
    let spec = demo_dir().join(format!("{backup_id}-backup.yaml"));
    std::fs::write(
        &spec,
        format!(
            "backup_id: {backup_id}\n\
             source:\n\
             \x20 bootstrap_servers: [{}]\n\
             \x20 topics: [{}]\n\
             {registry}\
             storage:\n\
             {}\
             backup:\n\
             \x20 compression: zstd\n\
             \x20 segment_max_records: 1000\n\
             \x20 segment_max_bytes: 10485760\n\
             \x20 max_concurrent_partitions: 3\n",
            harness::bootstrap(),
            topics.join(", "),
            storage_yaml(prefix, "  "),
        ),
    )
    .expect("the backup spec");
    let allow = demo_dir().join("sr-backup-allowed-clusters.json");
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
    for (k, v) in env {
        c.env(k, v);
    }
    let out = output_within(c, 900);
    if out.status.code() != Some(0) {
        return Err(out);
    }
    let line = |k: &str| {
        out.stdout
            .split(|b| *b == b'\n')
            .filter_map(|l| std::str::from_utf8(l).ok())
            .find_map(|l| l.strip_prefix(k).map(str::to_string))
            .unwrap_or_else(|| panic!("backup run prints {k}"))
    };
    let receipt_key = line("receipt-key=");
    let sidecar_key = line("sidecar-key=");
    let receipt_bytes = archive_get(prefix, &receipt_key);
    let receipt: BackupReceipt = serde_json::from_slice(&receipt_bytes).expect("a receipt");
    Ok(Backup {
        out,
        backup_id: backup_id.to_string(),
        prefix: prefix.to_string(),
        receipt_key,
        sidecar_key,
        receipt_bytes,
        receipt,
    })
}

fn archive_get(prefix: &str, key: &str) -> Vec<u8> {
    std::env::set_var("AWS_ACCESS_KEY_ID", s3_user());
    std::env::set_var("AWS_SECRET_ACCESS_KEY", s3_user());
    std::env::set_var("AWS_REGION", "us-east-1");
    let url: logweir_core::engine::StorageUrl =
        serde_yaml::from_str(&storage_yaml(prefix, "")).expect("a storage url");
    logweir_engine_oso::storage::Store::read_only_from_url(&url)
        .expect("the archive store")
        .get_capped(key, 64 << 20)
        .unwrap_or_else(|e| panic!("read {key}: {e}"))
        .0
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
    let p = demo_dir().join("sr-evidence-keys.json");
    std::fs::write(&p, serde_json::to_vec(&ring).expect("serialises")).expect("written");
    p
}

fn rfc3339(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .expect("representable")
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// A `newTopic` restore of `point`'s topics into `<naming><topic>`, bound to
/// its receipt, with `registry` (YAML under `target:`).
fn restore_spec(point: &Backup, naming: &str, registry: &str) -> String {
    format!(
        "source:\n\
         \x20 storage:\n\
         {storage}\
         \x20 backup: {backup}\n\
         \x20 topics: [{topics}]\n\
         \x20 point:\n\
         \x20   point_id: {point_id}\n\
         \x20   receipt_key: {receipt_key}\n\
         \x20   receipt_sha256: \"{receipt_sha256}\"\n\
         \x20   manifest_sha256: \"{manifest_sha256}\"\n\
         target:\n\
         \x20 bootstrap_servers: [{bootstrap}]\n\
         \x20 mode: newTopic\n\
         \x20 topic_mapping_prefix: \"drill-\"\n\
         \x20 topic_naming:\n\
         \x20   prefix: \"{naming}\"\n\
         \x20 default_replication_factor: 1\n\
         {registry}\
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
        storage = storage_yaml(&point.prefix, "    "),
        backup = point.backup_id,
        topics = point.receipt.source.topics.join(", "),
        point_id = logweir::catalog::record::point_id(&point.receipt_bytes),
        receipt_key = point.receipt_key,
        receipt_sha256 = logweir_core::ids::sha256_prefixed(&point.receipt_bytes),
        manifest_sha256 = point.receipt.archive.manifest_sha256,
        bootstrap = harness::bootstrap(),
        start = rfc3339(point.receipt.covered.from_ms - 1000),
        end = rfc3339(point.receipt.covered.to_ms + 1000),
        evidence = harness::EVIDENCE_BUCKET,
        endpoint = harness::s3_endpoint(),
    )
}

struct Restore {
    out: Output,
    scorecard: Value,
    scorecard_path: PathBuf,
}

fn restore(spec_text: &str, label: &str, env: &[(&str, String)]) -> Restore {
    let spec = demo_dir().join(format!("sr-{label}.yaml"));
    std::fs::write(&spec, spec_text).expect("written");
    let approver = demo_dir().join("sr-approver.pem");
    if !approver.exists() {
        let sk = logweir_evidence::keys::SigningKey::generate_p256();
        std::fs::write(&approver, sk.to_pkcs8_pem().expect("pem")).expect("written");
    }
    let sk = logweir_evidence::keys::SigningKey::from_pem_file(&approver).expect("the approver");
    let approver_pub = demo_dir().join("sr-approver.pub.pem");
    std::fs::write(
        &approver_pub,
        sk.verifying_key().to_public_key_pem().expect("pem"),
    )
    .expect("written");
    let doc = json!({
        "approver": "sr-e2e@example.com",
        "ticket": "PROD-03",
        "plan_hash": logweir_core::ids::sha256_prefixed(spec_text.as_bytes()),
        "approved_at": "2026-10-16T00:00:00Z",
    });
    let bytes = serde_json::to_vec_pretty(&doc).expect("serialises");
    let approval = demo_dir().join(format!("sr-{label}-approval.json"));
    std::fs::write(&approval, &bytes).expect("written");
    let side = logweir_evidence::sign::sign_detached(
        &sk,
        logweir::drill::phase1_approval::PAYLOAD_TYPE_APPROVAL,
        &bytes,
    )
    .expect("signed");
    std::fs::write(
        approval.with_extension("sig"),
        serde_json::to_vec(&side).expect("json"),
    )
    .expect("written");
    let allow = demo_dir().join("sr-restore-allowed-clusters.json");
    std::fs::write(
        &allow,
        serde_json::to_vec(
            &json!({"allowed_cluster_ids": [harness::cluster_id()], "source_cluster_id": null}),
        )
        .expect("serialises"),
    )
    .expect("written");
    let out_json = demo_dir().join(format!("sr-{label}-scorecard.json"));
    let _ = std::fs::remove_file(&out_json);
    let mut c = Command::new(bin());
    c.args(["restore", "run", "--spec"])
        .arg(&spec)
        .arg("--approval")
        .arg(&approval)
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
    for (k, v) in env {
        c.env(k, v);
    }
    let out = output_within(c, 900);
    let scorecard = std::fs::read(&out_json)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or(Value::Null);
    eprintln!(
        "[sr] restore {label}: exit={:?} outcome={} schemas={}",
        out.status.code(),
        scorecard["outcome"],
        scorecard["target"]["schemas"]
    );
    Restore {
        out,
        scorecard,
        scorecard_path: out_json,
    }
}

fn target_env() -> Vec<(&'static str, String)> {
    vec![("LOGWEIR_TARGET_REGISTRY_PASSWORD", target_password())]
}

/// Decode `topic` with the target registry; the decoded values, or the
/// consumer's error text.
fn consume_decoded(topic: &str, count: usize) -> Result<String, String> {
    let mut c = compose();
    c.args([
        "exec",
        "-T",
        "registry-target",
        "rpk",
        "topic",
        "consume",
        topic,
        "-X",
        "brokers=kafka-broker-1:9096",
        "-X",
        "sasl.mechanism=SCRAM-SHA-512",
        "-X",
        "registry.hosts=localhost:8081",
        "-X",
    ])
    // rpk presents ONE username and password, to the Kafka API (the broker's
    // SCRAM-SHA-512 listener) and to the registry (basic auth): the stack's
    // `logweir` user holds the fixture password on both.
    .arg(format!("user={TARGET_USER}"))
    .arg("-X")
    .arg(format!("pass={}", target_password()))
    .args([
        "--use-schema-registry=value",
        "-o",
        "start",
        "-f",
        "%v\\n",
        "-n",
    ])
    .arg(count.to_string());
    let o = output_within(c, 120);
    if o.status.success() {
        Ok(String::from_utf8_lossy(&o.stdout).to_string())
    } else {
        Err(text(&o))
    }
}

fn topic_exists(topic: &str) -> bool {
    harness::topic_exists(topic)
}

/// Both readers over one signed document; their outputs.
fn verify_both(doc: &Path, sig: &Path, receipt: bool) -> (Output, Output) {
    let mut r = Command::new(bin());
    r.args(["drill", "verify"]);
    if receipt {
        r.args(["--payload-type", "backup-receipt"]);
    }
    r.arg("--scorecard")
        .arg(doc)
        .arg("--signature")
        .arg(sig)
        .arg("--public-key")
        .arg(public_pem());
    let rust = output_within(r, 120);
    let mut p = Command::new(harness::auditor_python());
    p.arg(root().join("docs/verify_scorecard.py"));
    if receipt {
        p.args(["--payload-type", "backup-receipt"]);
    }
    p.arg(doc).arg(sig).arg(public_pem());
    let py = output_within(p, 120);
    (rust, py)
}

fn line_of(o: &Output, prefix: &str) -> String {
    text(o)
        .lines()
        .find_map(|l| l.find(prefix).map(|i| l[i..].to_string()))
        .unwrap_or_default()
}

fn write_local(name: &str, bytes: &[u8]) -> PathBuf {
    let p = demo_dir().join(name);
    std::fs::write(&p, bytes).expect("written");
    p
}

/// IMPORT a version into the TARGET registry by hand, keeping `id`.
fn target_import(
    subject: &str,
    version: u32,
    id: u32,
    schema_type: &str,
    schema: &str,
    refs: Value,
) {
    let base = target_url();
    let (s, b) = http(
        "PUT",
        &format!("{base}/mode/{}?force=true", enc(subject)),
        Some(&json!({"mode": "IMPORT"})),
        true,
    );
    assert_eq!(s, 200, "IMPORT mode on {subject}: {b}");
    let (s, b) = http(
        "POST",
        &format!("{base}/subjects/{}/versions", enc(subject)),
        Some(
            &json!({"schemaType": schema_type, "schema": schema, "references": refs,
                     "id": id, "version": version}),
        ),
        true,
    );
    assert_eq!(s, 200, "import {subject}@{version} as {id}: {b}");
    let (s, b) = http(
        "DELETE",
        &format!("{base}/mode/{}", enc(subject)),
        None,
        true,
    );
    assert!(s == 200 || s == 404, "put {subject}'s mode back: {s} {b}");
}

#[test]
#[ignore = "needs the `registry` profile: --profiles registry (e2e/README.md)"]
fn schemas_are_captured_and_imported_keeping_their_ids() {
    let n = nonce();
    let src = populate_source(&n);
    let mut evidence = serde_json::Map::new();
    let prefix = format!("schema-registry/{n}");

    // ---------------------------------------------------------- SR-1
    let p1 = backup(
        &format!("{n}-p1"),
        &prefix,
        &src.topics,
        &registry_yaml(&source_url(), None, "  "),
        &[],
    )
    .unwrap_or_else(|o| panic!("SR-1 backup must succeed:\n{}", text(&o)));
    harness::assert_format_at_least(&p1.receipt.format_version, "1.9.0", "SR-1 receipt");
    let block = p1.receipt.schema_registry.clone().expect("SR-1: the block");
    assert!(block.captured, "SR-1: {block:?}");
    let key = block.snapshot_key.clone().expect("a key");
    let snapshot_bytes = archive_get(&prefix, &key);
    assert_eq!(
        logweir_core::ids::sha256_prefixed(&snapshot_bytes),
        block.snapshot_sha256.clone().unwrap(),
        "SR-1: the receipt names the snapshot's own digest"
    );
    let snapshot: Value = serde_json::from_slice(&snapshot_bytes).unwrap();
    for (subject, id) in &src.ids {
        let v = snapshot["versions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["subject"] == *subject && v["id"] == *id)
            .unwrap_or_else(|| panic!("SR-1: the snapshot holds {subject} id {id}"));
        let soft = v["version"] == 2 && subject.ends_with("-orders-value");
        // KILLS: a capture that drops (or un-deletes) the soft-deleted version.
        assert_eq!(v["deleted"], soft, "SR-1: {subject} id {id} deleted flag");
    }
    assert!(block.ids_not_in_snapshot.is_empty(), "SR-1: {block:?}");
    let receipt_sig = archive_get(&prefix, &p1.sidecar_key);
    let rdoc = write_local(&format!("{n}-p1.receipt.json"), &p1.receipt_bytes);
    let rsig = write_local(&format!("{n}-p1.receipt.sig"), &receipt_sig);
    let (rust, py) = verify_both(&rdoc, &rsig, true);
    assert!(
        rust.status.success() && py.status.success(),
        "SR-1 verifiers:\n{}\n{}",
        text(&rust),
        text(&py)
    );
    let rline = line_of(&rust, "schema_registry: ");
    assert!(rline.starts_with("schema_registry: captured "), "{rline}");
    assert_eq!(
        rline,
        line_of(&py, "schema_registry: "),
        "SR-1/SR-10: the two readers agree"
    );
    evidence.insert(
        "sr1".into(),
        json!({"receipt": p1.receipt_key, "snapshot": key, "line": rline}),
    );

    // SR-1's control: the registry stopped, the backup still succeeds.
    compose_ok(&["stop", "registry"], "stop the source registry");
    let p0 = backup(
        &format!("{n}-p0"),
        &format!("{prefix}-p0"),
        &src.topics[..1],
        &registry_yaml(&source_url(), None, "  "),
        &[],
    );
    compose_ok(&["start", "registry"], "start the source registry");
    let p0 =
        p0.unwrap_or_else(|o| panic!("SR-1 control: the backup must still succeed:\n{}", text(&o)));
    let b0 = p0.receipt.schema_registry.clone().expect("the block");
    assert_eq!(
        (b0.captured, b0.reason.as_deref()),
        (false, Some("registryUnreachable")),
        "{b0:?}"
    );
    let catalog_key = String::from_utf8_lossy(&p0.out.stdout)
        .lines()
        .find_map(|l| l.strip_prefix("catalog-key=").map(str::to_string))
        .expect("a catalog point");
    let record: Value =
        serde_json::from_slice(&archive_get(&format!("{prefix}-p0"), &catalog_key)).unwrap();
    assert_eq!(
        record["schema_registry"]["captured"], false,
        "SR-1 control: the point says not captured"
    );
    assert_eq!(
        record["schema_registry"]["incomplete_reason"],
        "registryUnreachable"
    );
    for _ in 0..60 {
        if http("GET", &format!("{}/subjects", source_url()), None, false).0 == 200 {
            break;
        }
        std::thread::sleep(Duration::from_secs(2));
    }

    // ---------------------------------------------------------- SR-3
    let wrong = backup(
        &format!("{n}-p3"),
        &format!("{prefix}-p3"),
        &src.topics[..1],
        &registry_yaml(&target_url(), Some(TARGET_USER), "  "),
        &[(
            "LOGWEIR_SOURCE_REGISTRY_PASSWORD",
            "not-the-password".to_string(),
        )],
    )
    .unwrap_or_else(|o| {
        panic!(
            "SR-3: a wrong password never fails the backup:\n{}",
            text(&o)
        )
    });
    let b3 = wrong.receipt.schema_registry.clone().unwrap();
    assert_eq!(
        (b3.captured, b3.reason.as_deref()),
        (false, Some("registryUnauthorized")),
        "{b3:?}"
    );
    let bound = |projected: &str| {
        vec![
            ("LOGWEIR_SOURCE_REGISTRY_PASSWORD", target_password()),
            (
                "LOGWEIR_SOURCE_CREDENTIAL_BINDING_EXPECTED",
                "v1:sr3:expected".to_string(),
            ),
            (
                "LOGWEIR_SOURCE_CREDENTIAL_BINDING",
                "v1:sr3:expected".to_string(),
            ),
            (
                "LOGWEIR_SOURCE_REGISTRY_CREDENTIAL_BINDING",
                projected.to_string(),
            ),
        ]
    };
    let refused = backup(
        &format!("{n}-p3b"),
        &format!("{prefix}-p3b"),
        &src.topics[..1],
        &registry_yaml(&target_url(), Some(TARGET_USER), "  "),
        &bound("v1:another:connection"),
    )
    .err()
    .expect("SR-3: a registry Secret bound elsewhere refuses the run");
    assert_eq!(refused.status.code(), Some(3), "{}", text(&refused));
    assert!(
        text(&refused).contains("refusal-reason=CredentialBindingMismatch"),
        "{}",
        text(&refused)
    );
    let control = backup(
        &format!("{n}-p3c"),
        &format!("{prefix}-p3c"),
        &src.topics[..1],
        &registry_yaml(&target_url(), Some(TARGET_USER), "  "),
        &bound("v1:sr3:expected"),
    )
    .unwrap_or_else(|o| panic!("SR-3 control:\n{}", text(&o)));
    assert!(
        control.receipt.schema_registry.unwrap().captured,
        "SR-3 control captures"
    );

    // ---------------------------------------------------------- SR-4 control
    let none = restore(
        &restore_spec(&p1, &format!("sr4n-{n}-"), ""),
        &format!("{n}-sr4-none"),
        &[],
    );
    assert_eq!(none.out.status.code(), Some(0), "{}", text(&none.out));
    assert_eq!(
        none.scorecard["target"]["schemas"]["not_imported"],
        "noTargetRegistry"
    );
    for (subject, id) in &src.ids {
        assert!(
            target_schema(*id).is_none(),
            "the target holds none of {subject}'s IDs yet"
        );
    }
    let undecodable = consume_decoded(&format!("sr4n-{n}-{n}-orders"), 1);
    assert!(
        undecodable.as_ref().map_or(true, |v| !v.contains("cust0")),
        "SR-4 control: with no import the consumer cannot decode: {undecodable:?}"
    );
    let (rust, py) = verify_both(
        &none.scorecard_path,
        &none.scorecard_path.with_extension("sig"),
        false,
    );
    let sline = line_of(&rust, "schemas: ");
    assert!(
        sline.starts_with("schemas: not imported (noTargetRegistry)"),
        "{sline}\n{}",
        text(&rust)
    );
    assert_eq!(
        sline,
        line_of(&py, "schemas: "),
        "SR-10: the two readers agree"
    );

    // ---------------------------------------------------------- SR-4, SR-5
    let reg = registry_yaml(&target_url(), Some(TARGET_USER), "  ");
    let sr4 = restore(
        &restore_spec(&p1, &format!("sr4-{n}-"), &reg),
        &format!("{n}-sr4"),
        &target_env(),
    );
    assert_eq!(sr4.out.status.code(), Some(0), "{}", text(&sr4.out));
    assert_eq!(sr4.scorecard["outcome"], "pass");
    let schemas = &sr4.scorecard["target"]["schemas"];
    assert_eq!(
        schemas["imported_count"],
        src.ids.len(),
        "SR-4: the closure is imported: {schemas}"
    );
    for (subject, id) in &src.ids {
        let held = target_schema(*id)
            .unwrap_or_else(|| panic!("SR-4: the target answers {subject}'s ID {id}"));
        assert!(!held.is_empty());
        assert!(
            target_subjects().contains(subject),
            "SR-4: {subject} in the target"
        );
        // The subject's mode is put back: it inherited, so it has none.
        let (s, b) = http(
            "GET",
            &format!("{}/mode/{}", target_url(), enc(subject)),
            None,
            true,
        );
        assert!(
            s == 404 || b["mode"] != "IMPORT",
            "SR-4: {subject} left in IMPORT: {s} {b}"
        );
    }
    for (topic, want) in &src.expect {
        let count = if topic.ends_with("-orders") { 5 } else { 3 };
        let decoded = consume_decoded(&format!("sr4-{n}-{topic}"), count)
            .unwrap_or_else(|e| panic!("SR-4: {topic} decodes: {e}"));
        for w in want {
            assert!(
                decoded.contains(w.as_str()),
                "SR-4/SR-5: {topic} decoded {decoded}"
            );
        }
    }
    let (rust, py) = verify_both(
        &sr4.scorecard_path,
        &sr4.scorecard_path.with_extension("sig"),
        false,
    );
    let sline = line_of(&rust, "schemas: ");
    assert!(
        sline.starts_with(&format!(
            "schemas: imported {} subject-version(s)",
            src.ids.len()
        )),
        "{sline}"
    );
    assert_eq!(
        sline,
        line_of(&py, "schemas: "),
        "SR-10: the two readers agree"
    );
    evidence.insert("sr4".into(), json!({"schemas": schemas, "line": sline}));

    // ---------------------------------------------------------- SR-9
    let again = restore(
        &restore_spec(&p1, &format!("sr9-{n}-"), &reg),
        &format!("{n}-sr9"),
        &target_env(),
    );
    assert_eq!(again.out.status.code(), Some(0), "{}", text(&again.out));
    let s9 = &again.scorecard["target"]["schemas"];
    assert_eq!(
        (
            s9["imported_count"].as_u64(),
            s9["already_present"].as_u64()
        ),
        (Some(0), Some(src.ids.len() as u64)),
        "{s9}"
    );
    let own = restore(
        &restore_spec(
            &p1,
            &format!("sr9o-{n}-"),
            &registry_yaml(&source_url(), None, "  "),
        ),
        &format!("{n}-sr9-own"),
        &[],
    );
    assert_eq!(
        own.out.status.code(),
        Some(0),
        "SR-9: the source's own registry writes nothing:\n{}",
        text(&own.out)
    );
    assert_eq!(
        own.scorecard["target"]["schemas"]["already_present"],
        src.ids.len()
    );
    assert!(
        decoded_ok(&format!("sr9-{n}-{n}-audit")),
        "SR-9: the consumer reads"
    );

    // ---------------------------------------------------------- SR-6, SR-7, SR-8
    let n2 = format!("{n}b");
    let y = format!("{n2}-y");
    let x = format!("{n2}-x-value");
    let ns2 = format!("com.ex{n2}");
    let y_schema =
        json!({"type":"record","name":"Y","namespace":ns2,"fields":[{"name":"y","type":"string"}]})
            .to_string();
    let y_id = register(&y, "AVRO", &y_schema, json!([]));
    let x_refs = json!([{"name": format!("{ns2}.Y"), "subject": y, "version": 1}]);
    let x_schema = json!({"type":"record","name":"X","namespace":ns2,"fields":[{"name":"y","type":format!("{ns2}.Y")}]}).to_string();
    let x_id = register(&x, "AVRO", &x_schema, x_refs.clone());
    let xt = format!("{n2}-x");
    harness::create_topic(&xt, 1);
    let xrec = |i: u32| {
        let mut b = Vec::new();
        avro_string(&format!("xy{i}"), &mut b);
        framed(x_id, &b)
    };
    produce(&xt, &[xrec(0), xrec(1), xrec(2)]);
    let p2 = backup(
        &format!("{n2}-p2"),
        &format!("{prefix}-p2"),
        std::slice::from_ref(&xt),
        &registry_yaml(&source_url(), None, "  "),
        &[],
    )
    .unwrap_or_else(|o| panic!("the second point:\n{}", text(&o)));
    // SR-6: the target holds X's ID with another schema.
    let conflict = format!("{n2}-conflict");
    target_import(
        &conflict,
        1,
        x_id,
        "AVRO",
        &json!({"type":"record","name":"Other","namespace":ns2,"fields":[{"name":"o","type":"int"}]}).to_string(),
        json!([]),
    );
    let sr6 = restore(
        &restore_spec(&p2, &format!("sr6-{n}-"), &reg),
        &format!("{n}-sr6"),
        &target_env(),
    );
    assert_eq!(sr6.out.status.code(), Some(3), "{}", text(&sr6.out));
    assert!(
        text(&sr6.out).contains("SchemaConflict"),
        "{}",
        text(&sr6.out)
    );
    assert!(
        !topic_exists(&format!("sr6-{n}-{xt}")),
        "SR-6: no target topic"
    );
    let subjects = target_subjects();
    assert!(
        !subjects.contains(&x) && !subjects.contains(&y),
        "SR-6: nothing registered: {subjects:?}"
    );
    for path in [
        format!("/subjects/{}", enc(&conflict)),
        format!("/subjects/{}?permanent=true", enc(&conflict)),
    ] {
        let (s, b) = http("DELETE", &format!("{}{path}", target_url()), None, true);
        assert_eq!(s, 200, "remove the conflict: {b}");
    }
    // SR-7: Karapace as the target. Karapace keeps every ID it ever issued
    // (a hard-deleted subject's ID still answers), so the point comes from
    // the OTHER registry: a schema held in `registry-target` under an ID
    // Karapace never issued, captured from there, restored into Karapace.
    let k = format!("{n}-k7-value");
    let k_id = 900_000 + (src.ids[0].1 % 1000);
    let k_schema = json!({"type":"record","name":"K7","namespace":format!("com.ex{n}"),"fields":[{"name":"k","type":"string"}]}).to_string();
    target_import(&k, 1, k_id, "AVRO", &k_schema, json!([]));
    let kt = format!("{n}-k7");
    harness::create_topic(&kt, 1);
    let krec = |i: u32| {
        let mut b = Vec::new();
        avro_string(&format!("k7-{i}"), &mut b);
        framed(k_id, &b)
    };
    produce(&kt, &[krec(0), krec(1)]);
    let p7 = backup(
        &format!("{n}-p7"),
        &format!("{prefix}-p7"),
        std::slice::from_ref(&kt),
        &registry_yaml(&target_url(), Some(TARGET_USER), "  "),
        &[("LOGWEIR_SOURCE_REGISTRY_PASSWORD", target_password())],
    )
    .unwrap_or_else(|o| panic!("the SR-7 point:\n{}", text(&o)));
    assert!(p7.receipt.schema_registry.as_ref().unwrap().captured);
    let sr7 = restore(
        &restore_spec(
            &p7,
            &format!("sr7-{n}-"),
            &registry_yaml(&source_url(), None, "  "),
        ),
        &format!("{n}-sr7"),
        &[],
    );
    assert_eq!(sr7.out.status.code(), Some(3), "{}", text(&sr7.out));
    assert!(
        text(&sr7.out).contains("SchemaRegistryCannotKeepIds"),
        "{}",
        text(&sr7.out)
    );
    assert!(
        !topic_exists(&format!("sr7-{n}-{kt}")),
        "SR-7: no target topic"
    );
    let (_, karapace) = http("GET", &format!("{}/subjects", source_url()), None, false);
    assert!(
        !karapace.to_string().contains(&k),
        "SR-7: nothing registered in Karapace: {karapace}"
    );
    // SR-8: the first subject is already in the target (an import stopped
    // after it); the retry imports the rest.
    target_import(&y, 1, y_id, "AVRO", &y_schema, json!([]));
    let sr8 = restore(
        &restore_spec(&p2, &format!("sr8-{n}-"), &reg),
        &format!("{n}-sr8"),
        &target_env(),
    );
    assert_eq!(sr8.out.status.code(), Some(0), "{}", text(&sr8.out));
    let s8 = &sr8.scorecard["target"]["schemas"];
    assert_eq!(
        (
            s8["imported_count"].as_u64(),
            s8["already_present"].as_u64()
        ),
        (Some(1), Some(1)),
        "{s8}"
    );
    let decoded = consume_decoded(&format!("sr8-{n}-{xt}"), 3)
        .unwrap_or_else(|e| panic!("SR-8 decodes: {e}"));
    assert!(
        decoded.contains("xy0") && decoded.contains("xy2"),
        "SR-8: {decoded}"
    );

    evidence.insert("nonce".into(), json!(src.n));
    let dir = demo_dir().join("schema-registry");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("live.json"),
        serde_json::to_vec_pretty(&Value::Object(evidence)).unwrap(),
    )
    .unwrap();
}

fn decoded_ok(topic: &str) -> bool {
    consume_decoded(topic, 1).is_ok_and(|v| v.contains('w'))
}

/// **The two check rows, live** (`registry.reachable`, `target.schemas`; B2's
/// rows had none). Through the runner's own check kinds (`run_kind`, the code
/// a check Job runs) over the stack's registries:
///
/// | row | pass predicate | control |
/// |---|---|---|
/// | `registry.reachable` | the target registry with its password: `ready` `RegistryAnswered` | the wrong password: `notReady` `RegistryUnauthorized` |
/// | `target.schemas` | a restore check of a point that captured the source registry, into the empty target: `ready` `SchemasImportable`, naming how many will be imported | a subject to import made `READONLY`: `notReady` `SchemaRegistryReadOnly` (DUI-58); the same check after a conflicting schema took one of the IDs in the target: `notReady` `SchemaConflict` |
#[test]
#[ignore = "needs the `registry` profile: --profiles registry (e2e/README.md)"]
fn the_registry_check_rows_answer_from_the_live_registries() {
    use logweir_core::check_contract::*;
    use logweir_core::destination::*;
    const PASSWORD_ENV: &str = "LOGWEIR_TARGET_REGISTRY_PASSWORD";
    let n = nonce();
    let src = populate_source(&n);
    let prefix = format!("schema-registry/{n}");
    let point = backup(
        &format!("{n}-chk"),
        &prefix,
        &src.topics,
        &registry_yaml(&source_url(), None, "  "),
        &[],
    )
    .unwrap_or_else(|o| panic!("the backup must succeed:\n{}", text(&o)));
    assert!(
        point
            .receipt
            .schema_registry
            .as_ref()
            .is_some_and(|b| b.captured),
        "the point captured the source registry"
    );
    let connection = || ConnectionPlan {
        bootstrap_servers: vec![harness::bootstrap()],
        auth_mode: "plaintext".into(),
        username: None,
        password_env: None,
        tls: Some(false),
        ca_file: None,
        client_cert_file: None,
        client_key_file: None,
        principal: "User:ANONYMOUS".into(),
        schema_registry: Some(RegistryPlan {
            url: target_url(),
            username: Some(TARGET_USER.into()),
            password_env: Some(PASSWORD_ENV.into()),
        }),
    };
    let answer = |request: CheckRequest, id: CheckId| -> CheckOutcome {
        let plan = CheckPlan {
            contract: CHECK_PLAN_CONTRACT.to_string(),
            contract_version: CHECK_CONTRACT_VERSION,
            subject_uid: "sr-checks".into(),
            timeout_seconds: 120,
            policy_digest: None,
            request,
        };
        let bytes = serde_json::to_vec(&plan).expect("a plan serialises");
        let loaded = logweir::check::Loaded {
            plan,
            plan_sha256: logweir_core::ids::sha256_prefixed(&bytes),
            subject_uid: "sr-checks".into(),
        };
        let emission = logweir::check::kinds::run_kind(&loaded, logweir::check::Deadline::new(120));
        for c in &emission.result.checks {
            eprintln!("[sr-checks]   row {:?}: {:?} {:?}", c.id, c.state, c.code);
        }
        let row = emission
            .result
            .checks
            .into_iter()
            .find(|c| c.id == id)
            .unwrap_or_else(|| panic!("{id:?} is answered"));
        eprintln!(
            "[sr-checks] {id:?}: {:?} {:?} {}",
            row.state, row.code, row.message
        );
        row
    };
    let reachable = || {
        answer(
            CheckRequest::SourceConnection(SourceConnectionRequest {
                connection: connection(),
                capability_checks: vec![CheckId::RegistryReachable],
            }),
            CheckId::RegistryReachable,
        )
    };

    // registry.reachable, and its control.
    std::env::set_var(PASSWORD_ENV, "not-the-password");
    let refused = reachable();
    assert_eq!(
        (refused.state, refused.code),
        (CheckState::NotReady, CheckCode::RegistryUnauthorized),
        "the wrong password: {}",
        refused.message
    );
    std::env::set_var(PASSWORD_ENV, target_password());
    let ok = reachable();
    assert_eq!(
        (ok.state, ok.code),
        (CheckState::Ready, CheckCode::RegistryAnswered),
        "{}",
        ok.message
    );

    // target.schemas over the bound point.
    let spec_text = restore_spec(
        &point,
        &format!("chk-{n}-"),
        &registry_yaml(&target_url(), Some(TARGET_USER), "  "),
    );
    let plan_file = demo_dir().join(format!("sr-chk-{n}.yaml"));
    std::fs::write(&plan_file, &spec_text).expect("the plan");
    std::env::set_var("AWS_ACCESS_KEY_ID", s3_user());
    std::env::set_var("AWS_SECRET_ACCESS_KEY", s3_user());
    std::env::set_var("AWS_REGION", "us-east-1");
    std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    let location = DestinationLocation {
        provider: StorageProvider::S3,
        bucket: harness::ARCHIVE_BUCKET.into(),
        prefix: prefix.clone(),
        region: Some("us-east-1".into()),
        endpoint: Some(harness::s3_endpoint()),
        addressing: Addressing::PathStyle,
        transport: TransportSecurity::InsecureHttp,
    };
    let schemas = || {
        answer(
            CheckRequest::RestorePreflight(Box::new(RestorePreflightRequest {
                plan_file: plan_file.display().to_string(),
                plan_sha256: logweir_core::ids::sha256_prefixed(spec_text.as_bytes()),
                target: connection(),
                source_destination: DestinationPlan {
                    location_digest: location.location_digest(),
                    location: location.clone(),
                    name: "sr-chk".into(),
                    uid: "sr-chk".into(),
                    ca_file: None,
                    credentials: CredentialMode::Static,
                    grant_bindings: Vec::new(),
                },
                evidence_destination: None,
                backup_id: point.backup_id.clone(),
                // Relative to the destination's prefix, as the controller renders it.
                manifest_key: format!("{}/manifest.json", point.backup_id),
                checks: Vec::new(),
                skip_checks: Vec::new(),
                capability_checks: vec![CheckId::TargetSchemas],
            })),
            CheckId::TargetSchemas,
        )
    };
    let importable = schemas();
    assert_eq!(
        (importable.state, importable.code),
        (CheckState::Ready, CheckCode::SchemasImportable),
        "{}",
        importable.message
    );
    assert!(
        importable
            .message
            .contains("will be imported with their source IDs")
            && !importable.message.contains(" 0 will be imported"),
        "the preview names what will be imported: {}",
        importable.message
    );
    // DUI-58: a subject the import would write, read-only in the target (it is
    // absent there; the mode is its own): `notReady` `SchemaRegistryReadOnly`,
    // the run's own refusal, and still nothing written. Then the mode is
    // removed again.
    let (read_only, _) = src
        .ids
        .iter()
        .find(|(subject, _)| subject.ends_with("-audit-value"))
        .cloned()
        .expect("a subject the import writes");
    let mode_url = format!("{}/mode/{}", target_url(), enc(&read_only));
    let (s, b) = http("PUT", &mode_url, Some(&json!({"mode": "READONLY"})), true);
    assert_eq!(s, 200, "the target takes a subject-level READONLY: {b}");
    let refused = schemas();
    let (s, b) = http("DELETE", &mode_url, None, true);
    assert_eq!(s, 200, "the READONLY mode is removed again: {b}");
    assert_eq!(
        (refused.state, refused.code),
        (CheckState::NotReady, CheckCode::SchemaRegistryReadOnly),
        "{}",
        refused.message
    );
    assert!(
        refused.message.contains(&read_only) && refused.message.contains("READONLY"),
        "{}",
        refused.message
    );
    assert!(
        !target_subjects().contains(&read_only),
        "the preview wrote nothing"
    );

    // The control: one of the point's IDs taken in the target by another
    // schema, as SR-6 does it.
    let (_, framed_id) = src
        .ids
        .iter()
        .find(|(subject, _)| subject.ends_with("-audit-value"))
        .cloned()
        .expect("the JSON subject, whose ID the audit records carry");
    target_import(
        &format!("{n}-squat"),
        1,
        framed_id,
        "AVRO",
        &json!({"type":"record","name":"Squat","fields":[{"name":"s","type":"string"}]})
            .to_string(),
        json!([]),
    );
    let conflict = schemas();
    assert_eq!(
        (conflict.state, conflict.code),
        (CheckState::NotReady, CheckCode::SchemaConflict),
        "{}",
        conflict.message
    );
}
