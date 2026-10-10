#![cfg(feature = "e2e")]
//! **FX-33 — a real backup of 500 topics is listed, verified and restorable,
//! observed live.**
//!
//! One row on the DEFAULT stack's broker and MinIO (any slot; the `auth`
//! profile is not used). `#[ignore]`d because it takes minutes: it creates
//! 500 one-partition topics, some with real configuration overrides, backs
//! them up in ONE `logweir backup run`, and restores that point under a
//! prefix.
//!
//! | step | proves |
//! |---|---|
//! | the backup | a 500-topic selection is admitted (count and projected bytes) and signs ONE receipt and ONE catalog record; their real sizes are inside the topic budget's bounds (`logweir_core::topic_budget`) and well over the reads that used to drop them: the catalog walk's 256 KiB and the controller's 1 MiB |
//! | the catalog walk | `catalogSync` through the shipped wiring (`kinds::Live`, a real `Store` over MinIO) lists the point `Available` with its signature VERIFIED under a mounted trust bundle; the body decodes under the controller's relay budget |
//! | the evidence fetch | `evidenceFetch` of the receipt at the receipt's cap relays it whole (not `truncated`), which is the path a `SecretKeys` destination's controller verifies through; the control at the OLD 1 MiB cap is `truncated` and relays nothing |
//! | both verifiers | `logweir drill verify --payload-type backup-receipt` and `docs/verify_scorecard.py` accept the receipt |
//! | the restore | a `newTopic` restore BOUND to the point (its receipt re-verified by the runner under an evidence keyring) restores every topic under a prefix and signs `pass`; both verifiers accept the scorecard; every restored topic holds its source's record count; and the scorecard's parity was judged from the receipt's recorded configuration for every topic (nothing `not_assessed`) |
//!
//! **What the restore does with the recorded configuration.** A `newTopic`
//! restore creates its targets with Logweir's pinned settings and does NOT
//! apply the source's overrides (FX-3): it signs each one as
//! `topic_parity.not_reconstructed`. So the row proves the 500-topic
//! receipt's recorded configuration REACHED the restore and was judged topic
//! by topic, and that the override the receipt recorded for topic 0 is named;
//! it does not, and cannot in this product, prove the override was applied.
//!
//! `FX33_TOPICS` (default 500) sets the count, for the "largest that is
//! stable" fallback the brief allows; `FX33_RESTORE_TOPICS` (default: all)
//! restores only the first N of the point's topics.
//!
//! # Running it
//!
//! ```text
//! eval "$(e2e/compose/stack-env.sh --slot N --profiles auth)"
//! just e2e-up
//! cargo build -p logweir
//! LOGWEIR_PYTHON=<python with cryptography> AWS_EC2_METADATA_DISABLED=true \
//!   cargo test -p e2e --features e2e --test topic_budget -- --ignored --test-threads=1 --nocapture
//! just e2e-down
//! ```
//!
//! The row writes what it observed to `topic-budget/live.json` under the
//! stack's scratch directory (`harness::demo_dir()`), and deletes every topic
//! it created on every exit path.
mod harness;

use harness::{
    bin, demo_dir, engine_bin, engine_digest, engine_mount, engine_version, root, StdoutExt,
};
use logweir::check::{self, kinds, Loaded};
use logweir_core::backup_receipt::BackupReceipt;
use logweir_core::check_contract::frames::Decoder;
use logweir_core::check_contract::{
    CatalogDeepCheck, CatalogSyncMode, CatalogSyncRequest, CheckPlan, CheckRequest, CheckResult,
    CredentialMode, DestinationPlan, EvidenceFetchRequest, EvidenceObjectRequest,
    FrameExpectations, Stream, CHECK_CONTRACT_VERSION, CHECK_PLAN_CONTRACT,
    MAX_EVIDENCE_PAYLOAD_BYTES,
};
use logweir_core::destination::{
    Addressing, DestinationLocation, DestinationRole, StorageProvider, TransportSecurity,
};
use logweir_core::topic_budget::{
    MAX_BACKUP_TOPICS, MAX_RECEIPT_BYTES, MAX_RECORD_BYTES, RECEIPT_TOPIC_BUDGET_BYTES,
};
use serde_json::{json, Value};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const SUBJECT_UID: &str = "33333333-0000-4000-8000-000000000033";
const DEST_UID: &str = "33333333-0000-4000-8000-0000000000d3";

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
    format!("{}\n{}", o.stdout_utf8(), o.stderr_utf8())
}

/// The last `n` lines of a run, for a failure message that does not print
/// megabytes.
fn tail(o: &Output, n: usize) -> String {
    let t = text(o);
    let lines: Vec<&str> = t.lines().collect();
    lines[lines.len().saturating_sub(n)..].join("\n")
}

fn nonce() -> String {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970")
        .as_nanos();
    format!("{:08}", n % 100_000_000)
}

fn env_count(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ============================================================ the cluster

/// Drive one rdkafka admin future to completion on THIS thread, with a hard
/// deadline (rdkafka resolves admin futures from its own thread, so no async
/// runtime is needed).
fn block_on<F: std::future::Future>(f: F, secs: u64) -> F::Output {
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
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
        assert!(
            Instant::now() < deadline,
            "an admin call did not answer within {secs} s"
        );
        std::thread::park_timeout(Duration::from_millis(100));
    }
}

fn admin() -> rdkafka::admin::AdminClient<rdkafka::client::DefaultClientContext> {
    rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", harness::bootstrap())
        .create()
        .expect("an admin client")
}

/// The overrides topic `i` is created with: most topics carry none (a broker
/// default is what most real topics are), and three interleaved groups carry
/// real ones a receipt records as `portable`.
fn overrides(i: usize) -> Vec<(&'static str, &'static str)> {
    let mut configs = Vec::new();
    if i % 5 == 0 {
        configs.push(("retention.ms", "604800000"));
    }
    if i % 7 == 0 {
        configs.push(("cleanup.policy", "compact"));
        configs.push(("min.cleanable.dirty.ratio", "0.1"));
    }
    if i % 11 == 0 {
        configs.push(("max.message.bytes", "2097152"));
    }
    configs
}

/// Create every topic in batches of 50, one partition each, and wait until
/// the cluster serves the last of each batch (FX-18).
fn create_topics(names: &[String]) {
    use rdkafka::admin::{AdminOptions, NewTopic, TopicReplication};
    let admin = admin();
    for (batch_index, batch) in names.chunks(50).enumerate() {
        let specs: Vec<NewTopic<'_>> = batch
            .iter()
            .enumerate()
            .map(|(offset, name)| {
                let mut topic = NewTopic::new(name, 1, TopicReplication::Fixed(1));
                for (key, value) in overrides(batch_index * 50 + offset) {
                    topic = topic.set(key, value);
                }
                topic
            })
            .collect();
        let results = block_on(
            admin.create_topics(
                &specs,
                &AdminOptions::new().request_timeout(Some(Duration::from_secs(60))),
            ),
            120,
        )
        .expect("the CreateTopics call answers");
        for result in results {
            if let Err((topic, code)) = result {
                panic!("create {topic}: {code}");
            }
        }
        harness::await_created(batch.last().expect("a batch is not empty"), 1);
    }
}

/// Deletes the row's topics on every exit path, a panicking assertion
/// included: source topics by name, restored ones by their prefix.
struct Cleanup {
    source: Vec<String>,
    restored_prefix: String,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        use rdkafka::admin::AdminOptions;
        let mut all = self.source.clone();
        all.extend(
            self.source
                .iter()
                .map(|t| format!("{}{t}", self.restored_prefix)),
        );
        let admin = admin();
        for batch in all.chunks(100) {
            let names: Vec<&str> = batch.iter().map(String::as_str).collect();
            // Best effort: a restored topic that was never created answers
            // UnknownTopicOrPartition, which is the state we want.
            let _ = block_on(
                admin.delete_topics(
                    &names,
                    &AdminOptions::new().request_timeout(Some(Duration::from_secs(60))),
                ),
                120,
            );
        }
    }
}

/// `per_topic` keyed records into every topic's one partition.
fn produce(names: &[String], per_topic: usize) {
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    let producer: BaseProducer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", harness::bootstrap())
        .set("message.timeout.ms", "60000")
        .set("acks", "all")
        .create()
        .expect("a producer");
    for topic in names {
        for i in 0..per_topic {
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
        producer.poll(Duration::from_millis(0));
    }
    producer
        .flush(Duration::from_secs(120))
        .unwrap_or_else(|e| panic!("flush: {e}"));
}

/// The records the broker holds for each topic, by its end offset.
fn records_on(names: &[String]) -> Vec<i64> {
    use logweir_kafka::reader::ClusterReader;
    let reader = harness::reader();
    names
        .iter()
        .map(|topic| {
            ClusterReader::end_offsets(&reader, topic)
                .unwrap_or_else(|e| panic!("end_offsets({topic}): {e}"))
                .iter()
                .map(|(_, hi)| (*hi).max(0))
                .sum()
        })
        .collect()
}

// ============================================================ the pipeline

fn s3_user() -> String {
    ["minio", "admin"].concat()
}

fn use_stack_s3_env() {
    std::env::set_var("AWS_ACCESS_KEY_ID", s3_user());
    std::env::set_var("AWS_SECRET_ACCESS_KEY", s3_user());
    std::env::set_var("AWS_REGION", "us-east-1");
}

fn signing_pem() -> PathBuf {
    root().join("e2e/fixtures/signed/signing.pem")
}

fn public_pem() -> PathBuf {
    root().join("e2e/fixtures/signed/public.pem")
}

fn backup_allowlist() -> PathBuf {
    let p = demo_dir().join("fx33-backup-allowed-clusters.json");
    std::fs::write(
        &p,
        "{\"allowed_cluster_ids\": [\"SCRATCH-CLUSTER-NOT-THE-SOURCE\"]}\n",
    )
    .expect("written");
    p
}

fn restore_allowlist() -> PathBuf {
    let p = demo_dir().join("fx33-restore-allowed-clusters.json");
    std::fs::write(
        &p,
        serde_json::to_vec(
            &json!({"allowed_cluster_ids": [harness::cluster_id()], "source_cluster_id": null}),
        )
        .expect("serialises"),
    )
    .expect("written");
    p
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

fn archive_store(prefix: &str) -> logweir_engine_oso::storage::Store {
    use_stack_s3_env();
    let url: logweir_core::engine::StorageUrl =
        serde_yaml::from_str(&storage_yaml(prefix, "")).expect("a storage url");
    logweir_engine_oso::storage::Store::read_only_from_url(&url).expect("the archive store")
}

fn archive_get(prefix: &str, key: &str) -> Vec<u8> {
    archive_store(prefix)
        .get_capped(key, logweir_engine_oso::storage::caps::SIGNED_DOCUMENT)
        .unwrap_or_else(|e| panic!("read {key}: {e}"))
        .0
}

/// What one `logweir backup run` printed and signed.
struct Backup {
    backup_id: String,
    seconds: f64,
    receipt_key: String,
    receipt_bytes: Vec<u8>,
    receipt: BackupReceipt,
    catalog_key: String,
    record_bytes: Vec<u8>,
}

/// One `logweir backup run` of every topic, as set `backup_id` under the
/// prefix of the same name.
fn backup(backup_id: &str, topics: &[String]) -> Backup {
    let spec = demo_dir().join(format!("{backup_id}-backup.yaml"));
    let mut list = String::new();
    for topic in topics {
        list.push_str(&format!("    - {topic}\n"));
    }
    std::fs::write(
        &spec,
        format!(
            "backup_id: {backup_id}\n\
             source:\n\
             \x20 bootstrap_servers: [{}]\n\
             \x20 topics:\n{list}\
             storage:\n\
             {}\
             backup:\n\
             \x20 compression: zstd\n\
             \x20 segment_max_records: 1000\n\
             \x20 segment_max_bytes: 10485760\n\
             \x20 max_concurrent_partitions: 8\n",
            harness::bootstrap(),
            storage_yaml(backup_id, "  "),
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
    let started = Instant::now();
    let out = output_within(c, 2400);
    let seconds = started.elapsed().as_secs_f64();
    assert_eq!(
        out.status.code(),
        Some(0),
        "logweir backup run {backup_id} must exit 0 (after {seconds:.0} s):\n{}",
        tail(&out, 40)
    );
    let stdout = out.stdout_utf8();
    let line = |prefix: &str| {
        stdout
            .lines()
            .find_map(|l| l.strip_prefix(prefix))
            .map(str::to_string)
            .unwrap_or_else(|| panic!("backup run prints {prefix}"))
    };
    let receipt_key = line("receipt-key=");
    let catalog_key = line("catalog-key=");
    let receipt_bytes = archive_get(backup_id, &receipt_key);
    let record_bytes = archive_get(backup_id, &catalog_key);
    let receipt: BackupReceipt = serde_json::from_slice(&receipt_bytes).expect("a receipt");
    Backup {
        backup_id: backup_id.to_string(),
        seconds,
        receipt_key,
        receipt_bytes,
        receipt,
        catalog_key,
        record_bytes,
    }
}

// ============================================================ the checks

/// The compose MinIO's archive bucket as a destination a check reads.
fn minio_destination(prefix: &str) -> DestinationPlan {
    let loc = DestinationLocation {
        provider: StorageProvider::S3,
        bucket: harness::ARCHIVE_BUCKET.to_string(),
        prefix: prefix.to_string(),
        region: Some("us-east-1".to_string()),
        endpoint: Some(harness::s3_endpoint()),
        addressing: Addressing::PathStyle,
        // The compose MinIO serves plaintext, reachable only through an
        // explicit `InsecureHTTP` transport (D2 R3).
        transport: TransportSecurity::InsecureHttp,
    };
    DestinationPlan {
        location_digest: loc.location_digest(),
        location: loc,
        name: "compose-minio".to_string(),
        uid: DEST_UID.to_string(),
        ca_file: None,
        credentials: CredentialMode::Static,
        grant_bindings: Vec::new(),
    }
}

/// What one check relayed: its result document, its streams, and how many
/// bytes of stdout (a pod's log) carried them.
struct Relayed {
    log_bytes: usize,
    result: CheckResult,
    details: Option<Vec<u8>>,
    payload: Option<Vec<u8>>,
}

/// One check through the SHIPPED wiring — a real object store — and the real
/// emission path, decoded with the controller's decoder and its budget.
fn run_check(request: CheckRequest) -> Relayed {
    use_stack_s3_env();
    let plan = CheckPlan {
        contract: CHECK_PLAN_CONTRACT.to_string(),
        contract_version: CHECK_CONTRACT_VERSION,
        subject_uid: SUBJECT_UID.to_string(),
        timeout_seconds: 600,
        policy_digest: None,
        request,
    };
    plan.validate()
        .expect("the plan is one the contract admits");
    let bytes = serde_json::to_vec(&plan).expect("a plan serialises");
    let sha256 = logweir_core::ids::sha256_prefixed(&bytes);
    let loaded = Loaded {
        plan,
        plan_sha256: sha256.clone(),
        subject_uid: SUBJECT_UID.to_string(),
    };
    let mut stdout: Vec<u8> = Vec::new();
    let code = check::execute_with(&loaded, &mut stdout, &kinds::Live);
    assert_eq!(
        code,
        logweir::exit::ExitCode::Ok,
        "a check that ran exits 0"
    );
    let log = String::from_utf8(stdout).expect("frames are UTF-8");
    let mut decoder = Decoder::new();
    for line in log.lines() {
        decoder
            .push_line(line)
            .unwrap_or_else(|e| panic!("the relay decodes under the controller's budget: {e}"));
    }
    let relay = decoder
        .finish(&FrameExpectations {
            plan_sha256: sha256,
            subject_uid: SUBJECT_UID.to_string(),
        })
        .unwrap_or_else(|e| panic!("the relay verifies: {e}"));
    Relayed {
        log_bytes: log.len(),
        result: relay
            .result()
            .expect("a result stream")
            .expect("the result document parses"),
        details: relay.stream(Stream::Details).map(<[u8]>::to_vec),
        payload: relay.stream(Stream::EvidencePayload).map(<[u8]>::to_vec),
    }
}

/// Both readers over a signed document, exactly as an auditor runs them.
fn both_readers(document: &Path, signature: &Path, payload_type: Option<&str>) -> (Output, Output) {
    let mut rust = Command::new(bin());
    rust.args(["drill", "verify"]);
    if let Some(kind) = payload_type {
        rust.args(["--payload-type", kind]);
    }
    rust.arg("--scorecard")
        .arg(document)
        .arg("--signature")
        .arg(signature)
        .arg("--public-key")
        .arg(public_pem());
    let rust = output_within(rust, 300);
    let mut py = Command::new(harness::auditor_python());
    py.arg(root().join("docs/verify_scorecard.py"));
    if let Some(kind) = payload_type {
        py.args(["--payload-type", kind]);
    }
    py.arg(document).arg(signature).arg(public_pem());
    let py = output_within(py, 300);
    (rust, py)
}

// ============================================================ the restore

fn mint_approval(spec_text: &str, approver_pem: &Path) -> PathBuf {
    let doc = json!({
        "approver": "fx33-e2e@example.com",
        "ticket": "FX-33",
        "plan_hash": logweir_core::ids::sha256_prefixed(spec_text.as_bytes()),
        "approved_at": "2026-10-10T00:00:00Z",
    });
    let bytes = serde_json::to_vec_pretty(&doc).expect("serialises");
    let p = demo_dir().join("fx33-approval.json");
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
    let p = demo_dir().join("fx33-evidence-keys.json");
    std::fs::write(&p, serde_json::to_vec(&ring).expect("serialises")).expect("written");
    p
}

fn rfc3339(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .expect("representable")
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// A `newTopic` restore of `topics`, BOUND to the point's receipt, into
/// `<naming><topic>`.
fn restore_spec(point: &Backup, topics: &[String], naming: &str) -> String {
    let mut list = String::new();
    for topic in topics {
        list.push_str(&format!("    - {topic}\n"));
    }
    format!(
        "source:\n\
         \x20 storage:\n\
         {storage}\
         \x20 backup: {backup}\n\
         \x20 topics:\n{list}\
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
         sample:\n\
         \x20 window_start: \"{start}\"\n\
         \x20 window_end: \"{end}\"\n\
         \x20 records_per_partition: 3\n\
         \x20 anchor: head\n\
         objectives:\n\
         \x20 rto_seconds: 7200\n\
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
        storage = storage_yaml(&point.backup_id, "    "),
        backup = point.backup_id,
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
    seconds: f64,
    scorecard: Value,
    scorecard_path: PathBuf,
}

fn restore(spec_text: &str) -> Restore {
    let spec = demo_dir().join("fx33-restore.yaml");
    std::fs::write(&spec, spec_text).expect("written");
    let approver = demo_dir().join("fx33-approver.pem");
    if !approver.exists() {
        let sk = logweir_evidence::keys::SigningKey::generate_p256();
        std::fs::write(&approver, sk.to_pkcs8_pem().expect("pem")).expect("written");
    }
    let approver_pub = demo_dir().join("fx33-approver.pub.pem");
    let sk = logweir_evidence::keys::SigningKey::from_pem_file(&approver).expect("the approver");
    std::fs::write(
        &approver_pub,
        sk.verifying_key().to_public_key_pem().expect("pem"),
    )
    .expect("written");
    let approval = mint_approval(spec_text, &approver);
    let out_json = demo_dir().join("fx33-restore-scorecard.json");
    let _ = std::fs::remove_file(&out_json);
    let mut c = Command::new(bin());
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
    let started = Instant::now();
    let out = output_within(c, 3000);
    let seconds = started.elapsed().as_secs_f64();
    let scorecard = std::fs::read(&out_json)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or(Value::Null);
    eprintln!(
        "[fx33] restore: exit={:?} outcome={} after {seconds:.0} s",
        out.status.code(),
        scorecard["outcome"],
    );
    Restore {
        out,
        seconds,
        scorecard,
        scorecard_path: out_json,
    }
}

fn write_evidence(v: &Value) {
    let dir = demo_dir().join("topic-budget");
    std::fs::create_dir_all(&dir).expect("the evidence directory");
    let p = dir.join("live.json");
    std::fs::write(&p, serde_json::to_vec_pretty(v).expect("serialises")).expect("written");
    eprintln!("[fx33] evidence: {}", p.display());
}

// ============================================================ the row

/// **FX-33, live.** See the module doc's table.
///
/// Negative controls inside the row: the receipt and the record are both
/// LARGER than the reads that dropped them before (256 KiB, 1 MiB), and the
/// evidence fetch asked at the old 1 MiB cap relays nothing.
#[test]
#[ignore = "live, minutes: creates, backs up and restores 500 topics on the stack's broker"]
fn a_backup_of_500_topics_is_listed_verified_and_restored() {
    harness::stack::ensure_coherent();
    let count = env_count("FX33_TOPICS", 500);
    assert!(
        (1..=MAX_BACKUP_TOPICS).contains(&count),
        "FX33_TOPICS is 1 to {MAX_BACKUP_TOPICS}"
    );
    let n = nonce();
    // 46-character names: a realistic length, and what the budget is quoted
    // for (50).
    let topics: Vec<String> = (0..count)
        .map(|i| format!("fx33-{n}-orders-eu-west-1-ledger-events-{i:03}"))
        .collect();
    let restored_prefix = format!("fx33r-{n}-");
    let _cleanup = Cleanup {
        source: topics.clone(),
        restored_prefix: restored_prefix.clone(),
    };
    let mut evidence = json!({
        "topics": count,
        "engine": engine_version(),
        "engine_digest": engine_digest(),
    });

    // ---- 1. the cluster ----------------------------------------------------
    let started = Instant::now();
    create_topics(&topics);
    produce(&topics, 3);
    let source_records = records_on(&topics);
    assert!(
        source_records.iter().all(|n| *n == 3),
        "every source topic holds its three records"
    );
    let with_overrides = (0..count).filter(|i| !overrides(*i).is_empty()).count();
    eprintln!(
        "[fx33] {count} topics created and seeded in {:.0} s ({with_overrides} with overrides)",
        started.elapsed().as_secs_f64()
    );
    evidence["topics_with_overrides"] = json!(with_overrides);

    // ---- 2. the backup -----------------------------------------------------
    let backup_id = format!("fx33-{n}");
    let b = backup(&backup_id, &topics);
    let (receipt_len, record_len) = (b.receipt_bytes.len() as u64, b.record_bytes.len() as u64);
    let per_topic = receipt_len / count as u64;
    eprintln!(
        "[fx33] backup {backup_id}: {:.0} s; receipt {receipt_len} B ({per_topic} B a topic; \
         budget {RECEIPT_TOPIC_BUDGET_BYTES}, bound {MAX_RECEIPT_BYTES}); record {record_len} B \
         (bound {MAX_RECORD_BYTES}); format {}",
        b.seconds, b.receipt.format_version
    );
    evidence["backup"] = json!({
        "backup_id": backup_id,
        "seconds": b.seconds,
        "receipt_key": b.receipt_key,
        "receipt_bytes": receipt_len,
        "receipt_bytes_per_topic": per_topic,
        "receipt_bound": MAX_RECEIPT_BYTES,
        "receipt_format_version": b.receipt.format_version,
        "record_key": b.catalog_key,
        "record_bytes": record_len,
        "record_bound": MAX_RECORD_BYTES,
    });
    assert_eq!(b.receipt.source.topics.len(), count);
    assert_eq!(b.receipt.records.len(), count);
    assert!(b.receipt.records.values().all(|n| *n == 3));
    assert!(receipt_len <= MAX_RECEIPT_BYTES && record_len <= MAX_RECORD_BYTES);
    assert!(
        per_topic <= RECEIPT_TOPIC_BUDGET_BYTES,
        "a real topic costs {per_topic} bytes in the receipt; the budget is \
         {RECEIPT_TOPIC_BUDGET_BYTES}"
    );
    let configuration = b
        .receipt
        .topic_configuration
        .as_ref()
        .expect("the receipt records each topic's configuration");
    assert_eq!(configuration.len(), count);
    let recorded_override = configuration[&topics[0]]
        .entries
        .as_ref()
        .and_then(|e| e.get("retention.ms"))
        .and_then(|e| e.value.clone());
    assert_eq!(
        recorded_override.as_deref(),
        Some("604800000"),
        "topic 0's real override is in the signed receipt"
    );
    if count >= 400 {
        // NEGATIVE CONTROL: these are the documents the old reads dropped.
        assert!(
            record_len > 256 * 1024 && receipt_len > 1 << 20,
            "at {count} topics the record is over the old 256 KiB walk cap and the receipt over \
             the old 1 MiB controller cap"
        );
    }

    // ---- 3. the catalog walk ----------------------------------------------
    let point_id = logweir::catalog::record::point_id(&b.receipt_bytes);
    let walked = run_check(CheckRequest::CatalogSync(Box::new(CatalogSyncRequest {
        destination: minio_destination(&backup_id),
        mode: CatalogSyncMode::Index,
        deep_check: CatalogDeepCheck::ManifestDigest,
        max_objects_per_run: 100_000,
        view_limit: 2000,
        index_shard: None,
        rescan_start_after: None,
        trust_bundle_file: Some(public_pem().to_string_lossy().into_owned()),
    })));
    let body = String::from_utf8(walked.details.expect("a catalog sync relays a body"))
        .expect("the body is UTF-8");
    let entry: Value = body
        .lines()
        .filter_map(|l| l.strip_prefix("catalog-entry="))
        .map(|l| serde_json::from_str::<Value>(l).expect("an entry line is JSON"))
        .find(|e| e["pointId"] == point_id.as_str())
        .unwrap_or_else(|| panic!("the point is not in the catalog body:\n{body}"));
    eprintln!(
        "[fx33] catalog: {} {} (signer {}), topicsOmitted {}; body {} B, relay {} B",
        entry["availability"],
        entry["signature"],
        entry["signerKeyId"],
        entry["topicsOmitted"],
        body.len(),
        walked.log_bytes
    );
    evidence["catalog"] = json!({
        "point_id": point_id,
        "entry": entry,
        "body_bytes": body.len(),
        "relay_log_bytes": walked.log_bytes,
        "checks": walked.result.checks.iter().map(|c| json!({
            "id": c.id.as_str(), "state": format!("{:?}", c.state), "facts": c.facts,
        })).collect::<Vec<_>>(),
    });
    assert_eq!(entry["availability"], "Available", "{entry}");
    assert_eq!(entry["signature"], "verified", "{entry}");
    assert_eq!(entry["receiptKey"], b.receipt_key.as_str());
    assert_eq!(
        entry["receiptSha256"],
        logweir_core::ids::sha256_prefixed(&b.receipt_bytes)
    );
    assert!(entry.get("cause").is_none() && entry.get("factsFrom").is_none());
    if count > 64 {
        assert_eq!(entry["topicsOmitted"], count, "{entry}");
    }

    // ---- 4. the evidence fetch, at the receipt's cap and at the old one ----
    let fetch = |max_bytes: u64| {
        run_check(CheckRequest::EvidenceFetch(EvidenceFetchRequest {
            destination: minio_destination(&backup_id),
            objects: vec![EvidenceObjectRequest {
                role: DestinationRole::EvidenceRead,
                key: b.receipt_key.clone(),
                max_bytes,
                stream: Stream::EvidencePayload,
            }],
        }))
    };
    let relayed = fetch(MAX_EVIDENCE_PAYLOAD_BYTES);
    let answer = &relayed.result.evidence[0];
    eprintln!(
        "[fx33] evidence fetch at {MAX_EVIDENCE_PAYLOAD_BYTES}: present={} truncated={} bytes={:?}; \
         relay {} B",
        answer.present, answer.truncated, answer.bytes, relayed.log_bytes
    );
    assert!(answer.present && !answer.truncated, "{answer:?}");
    assert_eq!(
        relayed.payload.as_deref(),
        Some(b.receipt_bytes.as_slice()),
        "the relay carries the receipt whole"
    );
    evidence["evidence_fetch"] = json!({
        "max_bytes": MAX_EVIDENCE_PAYLOAD_BYTES,
        "relay_log_bytes": relayed.log_bytes,
        "bytes": answer.bytes,
    });
    if receipt_len > 1 << 20 {
        let old = fetch(1 << 20);
        let answer = &old.result.evidence[0];
        assert!(
            answer.present && answer.truncated && old.payload.is_none(),
            "CONTROL: under the old 1 MiB cap this receipt was truncated and never verified: \
             {answer:?}"
        );
        evidence["evidence_fetch"]["old_cap_truncated"] = json!(true);
    }

    // ---- 5. both verifiers over the receipt -------------------------------
    let dir = demo_dir().join("topic-budget");
    std::fs::create_dir_all(&dir).expect("dir");
    let (doc, sig) = (dir.join("receipt.json"), dir.join("receipt.sig"));
    std::fs::write(&doc, &b.receipt_bytes).expect("written");
    std::fs::write(
        &sig,
        archive_get(
            &backup_id,
            &b.receipt_key.replace(".receipt.json", ".receipt.sig"),
        ),
    )
    .expect("written");
    let (rust, py) = both_readers(&doc, &sig, Some("backup-receipt"));
    evidence["receipt_verifiers"] = json!({
        "logweir_verify_exit": rust.status.code(),
        "verify_scorecard_py_exit": py.status.code(),
    });
    assert_eq!(rust.status.code(), Some(0), "{}", tail(&rust, 20));
    assert_eq!(py.status.code(), Some(0), "{}", tail(&py, 20));

    // ---- 6. the restore, bound to the point, under a prefix ----------------
    let restore_count = env_count("FX33_RESTORE_TOPICS", count).min(count);
    let to_restore = &topics[..restore_count];
    let r = restore(&restore_spec(&b, to_restore, &restored_prefix));
    assert_eq!(
        r.out.status.code(),
        Some(0),
        "the bound restore exits 0:\n{}",
        tail(&r.out, 60)
    );
    assert!(
        text(&r.out).contains("recovery point binding verified"),
        "the runner re-verified the 500-topic receipt before it moved any data:\n{}",
        tail(&r.out, 40)
    );
    assert_eq!(r.scorecard["outcome"], "pass", "{}", r.scorecard["outcome"]);
    let restored: Vec<String> = to_restore
        .iter()
        .map(|t| format!("{restored_prefix}{t}"))
        .collect();
    let restored_records = records_on(&restored);
    assert_eq!(
        restored_records,
        source_records[..restore_count].to_vec(),
        "every restored topic holds its source's records"
    );
    let scorecard_bytes = std::fs::metadata(&r.scorecard_path)
        .expect("the scorecard")
        .len();
    let (rust, py) = both_readers(
        &r.scorecard_path,
        &r.scorecard_path.with_extension("sig"),
        None,
    );
    // The verifier's own sentence about the settings this restore did not
    // reconstruct, cut to its head: at 500 topics it names hundreds.
    let parity_line = text(&rust)
        .lines()
        .find(|l| l.starts_with("parity:"))
        .map(|l| l.chars().take(240).collect::<String>());
    let diff = &r.scorecard["topic_parity"];
    eprintln!(
        "[fx33] restore of {restore_count} topics: {:.0} s; scorecard {scorecard_bytes} B (the \
         controller reads a scorecard under {} B); {}",
        r.seconds,
        logweir_core::check_contract::MAX_EVIDENCE_SCORECARD_BYTES,
        parity_line.clone().unwrap_or_default()
    );
    evidence["restore"] = json!({
        "topics": restore_count,
        "prefix": restored_prefix,
        "seconds": r.seconds,
        "exit": r.out.status.code(),
        "outcome": r.scorecard["outcome"],
        "scorecard_format_version": r.scorecard["format_version"],
        "scorecard_bytes": scorecard_bytes,
        "controller_scorecard_cap": logweir_core::check_contract::MAX_EVIDENCE_SCORECARD_BYTES,
        "logweir_verify_exit": rust.status.code(),
        "verify_scorecard_py_exit": py.status.code(),
        "configuration_parity_line": parity_line,
        "not_reconstructed_count": diff["not_reconstructed"].as_array().map(Vec::len),
        "unexpected_divergence_count": diff["unexpected_divergence"].as_array().map(Vec::len),
        "not_assessed_count": diff["not_assessed"].as_array().map(Vec::len),
        "restored_records_total": restored_records.iter().sum::<i64>(),
    });
    write_evidence(&evidence);
    assert_eq!(rust.status.code(), Some(0), "{}", tail(&rust, 20));
    assert_eq!(py.status.code(), Some(0), "{}", tail(&py, 20));
    // THE RECEIPT'S RECORDED CONFIGURATION REACHED THE RESTORE: every restored
    // topic's parity was judged from it, so nothing is "not assessed", and the
    // overrides the receipt recorded are named as not reconstructed (a
    // `newTopic` restore creates its targets at the broker's defaults).
    assert_eq!(
        diff["not_assessed"].as_array().map(Vec::len),
        Some(0),
        "{}",
        diff["not_assessed"]
    );
    let not_reconstructed = diff["not_reconstructed"].to_string();
    assert!(
        not_reconstructed.contains(&format!("{}: retention.ms", restored[0])),
        "the scorecard names the override the receipt recorded for topic 0: {}",
        &not_reconstructed[..not_reconstructed.len().min(600)]
    );
}
