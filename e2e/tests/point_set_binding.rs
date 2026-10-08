#![cfg(feature = "e2e")]
//! **FX-16 — a restore bound to recovery point A restores A's set or nothing,
//! observed live.**
//!
//! One row on the DEFAULT stack (no profile): the default broker is both the
//! source and the target, and the archive is the stack's MinIO.
//!
//! | run | plan | this build | the pre-fix build (`FX16_BEFORE_BIN`) |
//! |---|---|---|---|
//! | `bound` | bound to point A, `backup: A` | restores A, `pass`, the target holds A's records (the control) | the same |
//! | `other-set` | bound to A, `backup: B` (a later set beside A) | exit 3 `PointBindingSetMismatch` before phase 0; no target topic | restores B under A's receipt |
//! | `latest` | bound to A, `backup: latestCompleted` (resolves to B) | exit 3 `PointBindingSetMismatch` before phase 0; no target topic | restores B under A's receipt |
//! | `copy` | bound to A, `backup: A`, storage pointed at a byte-identical copy of A under another prefix | exit 3 `PointBindingSetMismatch` after `describe` (phase 0 ran) and before phase 2: the manifest key is not the receipt's; no target topic | restores the copy under A's receipt |
//! | `edited` | as `copy`, over a copy whose manifest is re-serialised compact (same content, other bytes) | exit 3 naming the manifest key AND digest; no target topic | restores it under A's receipt |
//!
//! The binding (receipt digest, point id, signature under the mounted
//! evidence keyring, the manifest at the receipt's key) verifies in EVERY run:
//! each refused plan is approved, signed, and bound to a real point. What
//! differs is only which set the run would restore.
//!
//! # Running it
//!
//! ```text
//! eval "$(e2e/compose/stack-env.sh --slot N)"
//! bash scripts/extract-engine.sh && just e2e-up
//! cargo build -p logweir
//! [FX16_BEFORE_BIN=<a logweir binary built before FX-16>] AWS_EC2_METADATA_DISABLED=true \
//!   cargo test -p e2e --features e2e --test point_set_binding -- --test-threads=1 --nocapture
//! just e2e-down
//! ```
//!
//! The row writes what it observed to `point-set-binding/<n>.json` under the
//! stack's scratch directory (`harness::demo_dir()`).
mod harness;

use harness::{
    bin, demo_dir, engine_bin, engine_digest, engine_mount, engine_version, root, StdoutExt,
};
use logweir_core::backup_receipt::BackupReceipt;
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
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
    format!("{}\n{}", o.stdout_utf8(), o.stderr_utf8())
}

fn nonce() -> String {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970")
        .as_nanos();
    format!("{:010}", n % 10_000_000_000)
}

/// `mc` in the stack's `minio-setup` image, with the bytes written to its
/// stdin (for `mc pipe`). `harness::mc` passes no stdin.
fn mc_with_stdin(args: &[&str], stdin: &[u8]) -> Output {
    harness::stack::ensure_coherent();
    let mut c = Command::new("docker");
    c.args([
        "compose",
        "-f",
        "e2e/compose/docker-compose.yml",
        "run",
        "--rm",
        "-T",
        "--entrypoint",
        "mc",
        "minio-setup",
    ])
    .args(args)
    .current_dir(root())
    .stdin(Stdio::piped())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped());
    let mut child = c.spawn().expect("docker compose run mc");
    child
        .stdin
        .take()
        .expect("piped stdin")
        .write_all(stdin)
        .expect("the bytes reach mc");
    // The deadline is the harness's: `docker compose run` of a one-shot.
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if Instant::now() > deadline => {
                let _ = child.kill();
                panic!("mc {args:?}: killed after 120 s");
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(200)),
            Err(e) => panic!("mc {args:?}: {e}"),
        }
    }
    child.wait_with_output().expect("mc's output")
}

fn mc_ok(args: &[&str], what: &str) -> String {
    let o = harness::mc(args);
    assert!(o.status.success(), "{what} failed:\n{}", text(&o));
    o.stdout_utf8()
}

// ============================================================ the cluster

/// Three partitions on the default broker.
fn create_source_topic(topic: &str) {
    harness::create_topic(topic, 3);
}

/// Deletes `topics` with the broker's own CLI, best effort, at the end.
struct Topics(Vec<String>);
impl Drop for Topics {
    fn drop(&mut self) {
        for t in &self.0 {
            let _ = harness::kafka_topics(&[
                "--bootstrap-server",
                "kafka-broker-1:9094",
                "--delete",
                "--if-exists",
                "--topic",
                t,
            ]);
        }
    }
}

/// `per_partition` KEYED records into each of the three partitions.
fn produce(topic: &str, per_partition: usize, tag: &str) {
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    let producer: BaseProducer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", harness::bootstrap())
        .set("message.timeout.ms", "20000")
        .set("acks", "all")
        .create()
        .expect("a producer");
    for i in 0..per_partition * 3 {
        let payload = format!("{topic}-{tag}-{i}");
        let key = format!("{tag}{i}");
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

/// The records on `topic`, summed over its partitions' high watermarks.
fn records_on(topic: &str) -> i64 {
    harness::record_count_on_broker(topic).unwrap_or_else(|e| panic!("count {topic}: {e}"))
}

// ============================================================ the pipeline

fn s3_user() -> String {
    ["minio", "admin"].concat()
}

fn signing_pem() -> PathBuf {
    root().join("e2e/fixtures/signed/signing.pem")
}

fn backup_allowlist() -> PathBuf {
    let p = demo_dir().join("fx16-backup-allowed-clusters.json");
    std::fs::write(
        &p,
        "{\"allowed_cluster_ids\": [\"SCRATCH-CLUSTER-NOT-THE-SOURCE\"]}\n",
    )
    .expect("written");
    p
}

fn restore_allowlist() -> PathBuf {
    let p = demo_dir().join("fx16-restore-allowed-clusters.json");
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

/// What one `logweir backup run` printed and signed.
struct Backup {
    backup_id: String,
    receipt_key: String,
    receipt_bytes: Vec<u8>,
    receipt: BackupReceipt,
}

/// `logweir backup run` of `topic` as set `backup_id` under `prefix`.
fn backup(backup_id: &str, prefix: &str, topic: &str) -> Backup {
    let spec = demo_dir().join(format!("{backup_id}-backup.yaml"));
    std::fs::write(
        &spec,
        format!(
            "backup_id: {backup_id}\n\
             source:\n\
             \x20 bootstrap_servers: [{}]\n\
             \x20 topics: [{topic}]\n\
             storage:\n\
             {}\
             backup:\n\
             \x20 compression: zstd\n\
             \x20 segment_max_records: 1000\n\
             \x20 segment_max_bytes: 10485760\n\
             \x20 max_concurrent_partitions: 3\n",
            harness::bootstrap(),
            storage_yaml(prefix, "  "),
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
    let receipt_key = out
        .stdout_utf8()
        .lines()
        .find_map(|l| l.strip_prefix("receipt-key="))
        .map(str::to_string)
        .expect("backup run prints receipt-key=");
    let receipt_bytes = archive_get(prefix, &receipt_key);
    let receipt: BackupReceipt = serde_json::from_slice(&receipt_bytes).expect("a receipt");
    Backup {
        backup_id: backup_id.to_string(),
        receipt_key,
        receipt_bytes,
        receipt,
    }
}

/// One object of the archive bucket, by its bucket-absolute key.
fn archive_get(prefix: &str, key: &str) -> Vec<u8> {
    std::env::set_var("AWS_ACCESS_KEY_ID", s3_user());
    std::env::set_var("AWS_SECRET_ACCESS_KEY", s3_user());
    std::env::set_var("AWS_REGION", "us-east-1");
    let url: logweir_core::engine::StorageUrl =
        serde_yaml::from_str(&storage_yaml(prefix, "")).expect("a storage url");
    logweir_engine_oso::storage::Store::read_only_from_url(&url)
        .expect("the archive store")
        .get(key)
        .unwrap_or_else(|e| panic!("read {key}: {e}"))
        .0
}

fn mint_approval(spec_text: &str, approver_pem: &Path, label: &str) -> PathBuf {
    let doc = json!({
        "approver": "fx16-e2e@example.com",
        "ticket": "FX-16",
        "plan_hash": logweir_core::ids::sha256_prefixed(spec_text.as_bytes()),
        "approved_at": "2026-10-08T00:00:00Z",
    });
    let bytes = serde_json::to_vec_pretty(&doc).expect("serialises");
    let p = demo_dir().join(format!("fx16-{label}-approval.json"));
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
    let p = demo_dir().join("fx16-evidence-keys.json");
    std::fs::write(&p, serde_json::to_vec(&ring).expect("serialises")).expect("written");
    p
}

fn rfc3339(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .expect("representable")
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// A `newTopic` restore of `topic`, bound to `point`'s receipt, restoring set
/// `backup` from the archive under `prefix`, into `<naming><topic>`. The
/// sample window covers both backups' records.
fn restore_spec(
    point: &Backup,
    prefix: &str,
    backup: &str,
    topic: &str,
    naming: &str,
    window: (i64, i64),
) -> String {
    format!(
        "source:\n\
         \x20 storage:\n\
         {storage}\
         \x20 backup: {backup}\n\
         \x20 topics: [{topic}]\n\
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
        storage = storage_yaml(prefix, "    "),
        point_id = logweir::catalog::record::point_id(&point.receipt_bytes),
        receipt_key = point.receipt_key,
        receipt_sha256 = logweir_core::ids::sha256_prefixed(&point.receipt_bytes),
        manifest_sha256 = point.receipt.archive.manifest_sha256,
        bootstrap = harness::bootstrap(),
        start = rfc3339(window.0 - 1000),
        end = rfc3339(window.1 + 1000),
        evidence = harness::EVIDENCE_BUCKET,
        endpoint = harness::s3_endpoint(),
    )
}

struct Restore {
    out: Output,
    scorecard: Value,
}

fn restore(binary: &Path, spec_text: &str, label: &str) -> Restore {
    let spec = demo_dir().join(format!("fx16-{label}.yaml"));
    std::fs::write(&spec, spec_text).expect("written");
    let approver = demo_dir().join("fx16-approver.pem");
    if !approver.exists() {
        let sk = logweir_evidence::keys::SigningKey::generate_p256();
        std::fs::write(&approver, sk.to_pkcs8_pem().expect("pem")).expect("written");
    }
    let approver_pub = demo_dir().join("fx16-approver.pub.pem");
    let sk = logweir_evidence::keys::SigningKey::from_pem_file(&approver).expect("the approver");
    std::fs::write(
        &approver_pub,
        sk.verifying_key().to_public_key_pem().expect("pem"),
    )
    .expect("written");
    let approval = mint_approval(spec_text, &approver, label);
    let out_json = demo_dir().join(format!("fx16-{label}-scorecard.json"));
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
        "[fx16] restore {label}: exit={:?} outcome={}",
        out.status.code(),
        scorecard["outcome"],
    );
    Restore { out, scorecard }
}

/// The lines of a run worth keeping as evidence.
fn kept(o: &Output) -> Vec<String> {
    text(o)
        .lines()
        .filter(|l| {
            l.contains("PointBinding")
                || l.contains("refusal-reason=")
                || l.contains("progress-phase=")
                || l.contains("recovery point binding verified")
                || l.starts_with("exit")
        })
        .map(|l| l.chars().take(1200).collect())
        .collect()
}

/// Asserts `r` was refused `PointBindingSetMismatch` with every `needle`,
/// that the binding itself had verified, and that `target` was never created.
/// `after_phase_0`: the refusal is the restored-set half (phase 0 ran, phase 2
/// did not); otherwise the plan half (no phase began).
fn assert_refused(r: &Restore, target: &str, needles: &[&str], after_phase_0: bool) {
    let t = text(&r.out);
    assert_eq!(r.out.status.code(), Some(3), "exit 3:\n{t}");
    assert!(t.contains("PointBindingSetMismatch. "), "{t}");
    assert!(t.contains("refusal-reason=GuardRefused"), "{t}");
    for needle in needles {
        assert!(t.contains(needle), "{needle:?} missing:\n{t}");
    }
    assert!(
        !t.contains("PointBindingMismatch") && !t.contains("PointUntrusted"),
        "the binding itself verified; only the set differs:\n{t}"
    );
    assert_eq!(
        t.contains("progress-phase=0:admit"),
        after_phase_0,
        "phase 0 ran exactly when the refusal is the restored-set half:\n{t}"
    );
    assert!(
        !t.contains("progress-phase=2:"),
        "phase 2 never began:\n{t}"
    );
    assert!(
        !harness::topic_exists(target),
        "a refused restore created its target topic {target}"
    );
    assert_eq!(r.scorecard, Value::Null, "a refused run signs nothing");
}

// ============================================================ the row

/// **FX-16, live.** See the module doc's table.
///
/// Negative controls: each refused run is a plan that a build without the
/// check runs to completion — `FX16_BEFORE_BIN` records exactly that, and the
/// row then asserts the pre-fix build restored the OTHER set's records under
/// point A's receipt. Without the variable the same rows run this build only.
#[test]
fn a_bound_restore_restores_its_points_set_or_nothing() {
    let n = nonce();
    let topic = format!("fx16src-{n}");
    let mut topics = Topics(vec![topic.clone()]);
    create_source_topic(&topic);

    // Point A: 30 records. Set B, beside it under the same prefix, after 15
    // more: the newest set, so `latestCompleted` resolves to B. Ids `…-a` and
    // `…-b` because `latestCompleted` takes the last manifest key in order.
    let prefix = format!("fx16-{n}");
    produce(&topic, 10, "a");
    let a = backup(&format!("fx16-{n}-a"), &prefix, &topic);
    produce(&topic, 5, "b");
    let b = backup(&format!("fx16-{n}-b"), &prefix, &topic);
    assert_ne!(
        a.receipt.archive.manifest_sha256, b.receipt.archive.manifest_sha256,
        "two different sets"
    );
    let window = (a.receipt.covered.from_ms, b.receipt.covered.to_ms);

    // A byte-identical copy of set A under another prefix, and an EDITED copy
    // whose manifest is the same JSON re-serialised (compact): other bytes,
    // same content, so the engine restores it as happily as the original.
    let copy_prefix = format!("fx16-{n}-copy");
    let edit_prefix = format!("fx16-{n}-edit");
    for p in [&copy_prefix, &edit_prefix] {
        mc_ok(
            &[
                "cp",
                "--recursive",
                &format!(
                    "local/{}/{prefix}/{}/",
                    harness::ARCHIVE_BUCKET,
                    a.backup_id
                ),
                &format!("local/{}/{p}/{}/", harness::ARCHIVE_BUCKET, a.backup_id),
            ],
            &format!("copy set A to {p}"),
        );
    }
    let original_manifest = archive_get(&prefix, &a.receipt.archive.manifest_key);
    let copy_manifest_key = a
        .receipt
        .archive
        .manifest_key
        .replacen(&prefix, &copy_prefix, 1);
    let digest = logweir_core::ids::sha256_prefixed;
    assert_eq!(
        digest(&archive_get(&copy_prefix, &copy_manifest_key)),
        digest(&original_manifest),
        "the copy's manifest is byte-identical"
    );
    // The engine writes its manifest pretty-printed, so the edit is the
    // COMPACT serialisation of the same document.
    let edited: Value = serde_json::from_slice(&original_manifest).expect("the manifest parses");
    let edited = serde_json::to_vec(&edited).expect("re-serialises");
    assert_ne!(digest(&edited), digest(&original_manifest), "other bytes");
    let edit_manifest_key = a
        .receipt
        .archive
        .manifest_key
        .replacen(&prefix, &edit_prefix, 1);
    let piped = mc_with_stdin(
        &[
            "pipe",
            &format!("local/{}/{edit_manifest_key}", harness::ARCHIVE_BUCKET),
        ],
        &edited,
    );
    assert!(piped.status.success(), "mc pipe:\n{}", text(&piped));
    assert_eq!(
        digest(&archive_get(&edit_prefix, &edit_manifest_key)),
        digest(&edited)
    );

    // name, storage prefix, `source.backup`, the refusal's words (or none: the
    // control), and whether phase 0 runs before it.
    let runs: [(&str, &str, &str, Vec<String>, bool); 5] = [
        ("bound", &prefix, &a.backup_id, vec![], false),
        (
            "other-set",
            &prefix,
            &b.backup_id,
            vec![format!("names set `{}`", b.backup_id)],
            false,
        ),
        (
            "latest",
            &prefix,
            "latestCompleted",
            vec!["names `latestCompleted`".to_string()],
            false,
        ),
        (
            "copy",
            &copy_prefix,
            &a.backup_id,
            vec![format!(
                "its manifest is {copy_manifest_key}, not the receipt's {}",
                a.receipt.archive.manifest_key
            )],
            true,
        ),
        (
            "edited",
            &edit_prefix,
            &a.backup_id,
            vec![
                format!(
                    "its manifest is {edit_manifest_key}, not the receipt's {}",
                    a.receipt.archive.manifest_key
                ),
                format!(
                    "its manifest hashes to {}, not the bound {}",
                    logweir_core::ids::sha256_prefixed(&edited),
                    a.receipt.archive.manifest_sha256
                ),
            ],
            true,
        ),
    ];

    let before = std::env::var_os("FX16_BEFORE_BIN").map(PathBuf::from);
    let mut evidence = serde_json::Map::new();
    evidence.insert(
        "fixture".into(),
        json!({
            "topic": topic,
            "point_a": {
                "backup_id": a.backup_id,
                "receipt_key": a.receipt_key,
                "point_id": logweir::catalog::record::point_id(&a.receipt_bytes),
                "manifest_key": a.receipt.archive.manifest_key,
                "manifest_sha256": a.receipt.archive.manifest_sha256,
                "records": a.receipt.records,
            },
            "set_b": {
                "backup_id": b.backup_id,
                "manifest_sha256": b.receipt.archive.manifest_sha256,
                "records": b.receipt.records,
            },
            "copy_manifest_key": copy_manifest_key,
            "edit_manifest_key": edit_manifest_key,
            "edited_manifest_sha256": logweir_core::ids::sha256_prefixed(&edited),
            "engine": engine_version(),
            "binary": bin(),
            "before_binary": before,
        }),
    );

    for (name, storage_prefix, source_backup, needles, after_phase_0) in &runs {
        let naming = format!("fx16{name}-{n}-");
        let target = format!("{naming}{topic}");
        topics.0.push(target.clone());
        let spec = restore_spec(&a, storage_prefix, source_backup, &topic, &naming, window);
        let r = restore(&bin(), &spec, &format!("{n}-{name}"));
        let exists = harness::topic_exists(&target);
        let restored = exists.then(|| records_on(&target));
        evidence.insert(
            format!("{name}/this-build"),
            json!({
                "exit": r.out.status.code(),
                "outcome": r.scorecard["outcome"],
                "target_topic": target,
                "target_exists": exists,
                "target_records": restored,
                "lines": kept(&r.out),
            }),
        );
        if needles.is_empty() {
            // THE CONTROL: the plan that binds A and names A restores A.
            assert_eq!(r.out.status.code(), Some(0), "{}", text(&r.out));
            assert_eq!(r.scorecard["outcome"], "pass");
            assert!(text(&r.out).contains("recovery point binding verified"));
            assert_eq!(restored, Some(30), "A's 30 records, not B's 45");
        } else {
            let needles: Vec<&str> = needles.iter().map(String::as_str).collect();
            assert_refused(&r, &target, &needles, *after_phase_0);
        }

        if let Some(before) = &before {
            let naming = format!("fx16{name}-pre-{n}-");
            let target = format!("{naming}{topic}");
            topics.0.push(target.clone());
            let spec = restore_spec(&a, storage_prefix, source_backup, &topic, &naming, window);
            let r = restore(before, &spec, &format!("{n}-{name}-pre"));
            let exists = harness::topic_exists(&target);
            let restored = exists.then(|| records_on(&target));
            evidence.insert(
                format!("{name}/before-fx16"),
                json!({
                    "exit": r.out.status.code(),
                    "outcome": r.scorecard["outcome"],
                    "target_topic": target,
                    "target_exists": exists,
                    "target_records": restored,
                    "lines": kept(&r.out),
                }),
            );
            // The pre-fix build runs every plan to a signed pass: the
            // control as here, and each refused plan by restoring a set the
            // receipt does not describe (B's 45 records, or A's copy).
            assert_eq!(r.out.status.code(), Some(0), "{name}: {}", text(&r.out));
            assert_eq!(r.scorecard["outcome"], "pass", "{name}");
            let expected = if *source_backup == a.backup_id {
                30
            } else {
                45
            };
            assert_eq!(restored, Some(expected), "{name}");
        }
    }

    let dir = demo_dir().join("point-set-binding");
    std::fs::create_dir_all(&dir).expect("the evidence directory");
    let p = dir.join(format!("{n}.json"));
    std::fs::write(
        &p,
        serde_json::to_vec_pretty(&Value::Object(evidence)).expect("serialises"),
    )
    .expect("written");
    eprintln!("[fx16] evidence: {}", p.display());
}
