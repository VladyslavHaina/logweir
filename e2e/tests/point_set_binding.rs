#![cfg(feature = "e2e")]
//! **FX-16 — a restore bound to recovery point A restores A's set or nothing,
//! observed live.**
//!
//! One row on the DEFAULT stack (no profile): the default broker is both the
//! source and the target, and the archive is the stack's MinIO.
//!
//! | run | plan | this build | the build at FX-16's first round (`FX16_ROUND1_BIN`) | the build before FX-16 (`FX16_BEFORE_BIN`) |
//! |---|---|---|---|---|
//! | `bound` | bound to point A, `backup: A`, with a byte-identical copy of A at `<prefix>/0copy/A/` listed FIRST | restores A, `pass`, A's 30 records (the control, and review L-1) | refused `PointBindingSetMismatch` on the copy's key: L-1's false refusal | restores A |
//! | `other-set` | bound to A, `backup: B` (a later set beside A) | exit 3 before phase 0 | exit 3 | restores B (45) under A's receipt |
//! | `latest` | bound to A, `backup: latestCompleted` (resolves to B) | exit 3 before phase 0 | exit 3 | restores B (45) under A's receipt |
//! | `copy` | bound to A, storage pointed at a byte-identical copy of A under another prefix | exit 3 before phase 0: the engine would read another key than the receipt's | exit 3 after `describe` | restores the copy under A's receipt |
//! | `edited` | as `copy`, over a copy whose manifest carries one more trailing newline | exit 3 before phase 0 | exit 3 after `describe` | restores it under A's receipt |
//! | `nested` | review M-1: point N's set NESTED under the plan's prefix (`<nest>/a/N/`), another set with the same id where the engine reads (`<nest>/N/`) | exit 3 before phase 0, naming both keys and `<nest>/a` | **restores the other set (45) under N's receipt (60)**: M-1 | the same |
//!
//! The binding (receipt digest, point id, signature under the mounted
//! evidence keyring, the manifest at the receipt's key) verifies in EVERY run:
//! each refused plan is approved, signed, and bound to a real point. What
//! differs is only which set the run would restore. The same-id set at the
//! engine's path in `nested` is written by a second `backup run` of the same
//! id after the first one's execution claim is removed: another writer, as an
//! older build or the upstream tool would be.
//!
//! # Running it
//!
//! ```text
//! eval "$(e2e/compose/stack-env.sh --slot N)"
//! bash scripts/extract-engine.sh && just e2e-up
//! cargo build -p logweir
//! [FX16_BEFORE_BIN=<a logweir built before FX-16>] [FX16_ROUND1_BIN=<one at FX-16's first round>] \
//!   AWS_EC2_METADATA_DISABLED=true cargo test -p e2e --features e2e --test point_set_binding -- --test-threads=1 --nocapture
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

/// Asserts `r` was refused `PointBindingSetMismatch` with every `needle`
/// BEFORE phase 0 (no phase began, so no broker was asked anything), that the
/// binding itself had verified, and that `target` was never created.
fn assert_refused(r: &Restore, target: &str, needles: &[String]) {
    let t = text(&r.out);
    assert_eq!(r.out.status.code(), Some(3), "exit 3:\n{t}");
    assert!(t.contains("PointBindingSetMismatch. "), "{t}");
    assert!(t.contains("refusal-reason=GuardRefused"), "{t}");
    for needle in needles {
        assert!(t.contains(needle.as_str()), "{needle:?} missing:\n{t}");
    }
    assert!(
        !t.contains("PointBindingMismatch") && !t.contains("PointUntrusted"),
        "the binding itself verified; only the set differs:\n{t}"
    );
    assert!(
        !t.contains("progress-phase="),
        "refused before phase 0:\n{t}"
    );
    assert!(
        !harness::topic_exists(target),
        "a refused restore created its target topic {target}"
    );
    assert_eq!(r.scorecard, Value::Null, "a refused run signs nothing");
}

/// What one build does with one plan.
enum Expect {
    /// Exit 0 `pass`, the target holding this many records.
    Passes(i64),
    /// NOT refused by FX-16's check (the run went on to restore under the
    /// point's receipt), and any target it created holds this many records —
    /// another set's, not the point's.
    RestoresOther(i64),
    /// Exit 3 `PointBindingSetMismatch` naming every needle, nothing created.
    Refused(Vec<String>),
}

struct Run<'a> {
    name: &'static str,
    point: &'a Backup,
    prefix: String,
    backup: String,
    window: (i64, i64),
    this_build: Expect,
    round1: Expect,
    before: Expect,
}

/// The engine's key message for `point` under `plan_prefix`.
fn engine_key_needles(point: &Backup, plan_prefix: &str, written_under: &str) -> Vec<String> {
    vec![
        format!(
            "attests set `{}`'s manifest at {}",
            point.backup_id, point.receipt.archive.manifest_key
        ),
        format!(
            "the engine would read set `{}` at {plan_prefix}/{}/manifest.json",
            point.backup_id, point.backup_id
        ),
        format!("the prefix the set was written under ({written_under})"),
    ]
}

// ============================================================ the row

/// **FX-16, live, with its fix round.** See the module doc's table.
///
/// Negative controls: `FX16_ROUND1_BIN` (the build at FX-16's first-round
/// tip) refuses the truthful `bound` plan and restores the wrong set for
/// `nested`; `FX16_BEFORE_BIN` (before FX-16) restores the wrong set for every
/// refused plan. Without the variables the same rows run this build only.
#[test]
fn a_bound_restore_restores_its_points_set_or_nothing() {
    let n = nonce();
    let topic = format!("fx16src-{n}");
    let mut topics = Topics(vec![topic.clone()]);
    create_source_topic(&topic);
    let bucket = harness::ARCHIVE_BUCKET;
    let digest = logweir_core::ids::sha256_prefixed;

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
    assert_eq!(
        a.receipt.archive.manifest_key,
        format!("{prefix}/{}/manifest.json", a.backup_id),
        "the backup attests the key the engine wrote"
    );

    // Review M-1's layout: set `…-n` written at `<nest>/…-n/` (45 records) by
    // one writer, then — its execution claim removed, as another writer would
    // never have taken it — 15 more records and point N, the same id, NESTED
    // at `<nest>/a/…-n/` (60 records). The engine-path set comes first so
    // neither backup's read-back lists the other.
    let nest = format!("fx16-{n}-nest");
    let nested_id = format!("fx16-{n}-n");
    let engine_path_set = backup(&nested_id, &nest, &topic);
    mc_ok(
        &[
            "rm",
            &format!(
                "local/{bucket}/{}",
                logweir::backup::phase_run::claim_key(&nested_id)
            ),
        ],
        "remove the first writer's execution claim",
    );
    produce(&topic, 5, "n");
    let n_point = backup(&nested_id, &format!("{nest}/a"), &topic);
    assert_eq!(
        n_point.receipt.archive.manifest_key,
        format!("{nest}/a/{nested_id}/manifest.json")
    );

    // A byte-identical copy of A LISTED FIRST under A's own prefix (`0copy`
    // sorts before `fx16-…`: review L-1); a byte-identical copy under another
    // prefix; and an EDITED copy whose manifest is the same JSON with one more
    // trailing newline.
    let copy_prefix = format!("fx16-{n}-copy");
    let edit_prefix = format!("fx16-{n}-edit");
    for dest in [
        format!("{prefix}/0copy/{}", a.backup_id),
        format!("{copy_prefix}/{}", a.backup_id),
        format!("{edit_prefix}/{}", a.backup_id),
    ] {
        mc_ok(
            &[
                "cp",
                "--recursive",
                &format!("local/{bucket}/{prefix}/{}/", a.backup_id),
                &format!("local/{bucket}/{dest}/"),
            ],
            &format!("copy set A to {dest}"),
        );
    }
    let original_manifest = archive_get(&prefix, &a.receipt.archive.manifest_key);
    let first_copy_key = format!("{prefix}/0copy/{}/manifest.json", a.backup_id);
    assert_eq!(
        digest(&archive_get(&prefix, &first_copy_key)),
        digest(&original_manifest),
        "the 0copy manifest is byte-identical"
    );
    let mut edited = original_manifest.clone();
    edited.push(b'\n');
    assert_eq!(
        serde_json::from_slice::<Value>(&edited).expect("the edit parses"),
        serde_json::from_slice::<Value>(&original_manifest).expect("the original parses"),
        "the same document"
    );
    assert_ne!(digest(&edited), digest(&original_manifest), "other bytes");
    let edit_manifest_key = format!("{edit_prefix}/{}/manifest.json", a.backup_id);
    let piped = mc_with_stdin(
        &["pipe", &format!("local/{bucket}/{edit_manifest_key}")],
        &edited,
    );
    assert!(piped.status.success(), "mc pipe:\n{}", text(&piped));
    assert_eq!(
        digest(&archive_get(&edit_prefix, &edit_manifest_key)),
        digest(&edited)
    );

    let ab = (a.receipt.covered.from_ms, b.receipt.covered.to_ms);
    let nn = (
        engine_path_set.receipt.covered.from_ms,
        n_point.receipt.covered.to_ms,
    );
    let refused_on_the_copys_key = Expect::Refused(vec![format!(
        "its manifest is {first_copy_key}, not the receipt's {}",
        a.receipt.archive.manifest_key
    )]);
    let runs = vec![
        Run {
            name: "bound",
            point: &a,
            prefix: prefix.clone(),
            backup: a.backup_id.clone(),
            window: ab,
            this_build: Expect::Passes(30),
            round1: refused_on_the_copys_key,
            before: Expect::Passes(30),
        },
        Run {
            name: "other-set",
            point: &a,
            prefix: prefix.clone(),
            backup: b.backup_id.clone(),
            window: ab,
            this_build: Expect::Refused(vec![format!("names set `{}`", b.backup_id)]),
            round1: Expect::Refused(vec![format!("names set `{}`", b.backup_id)]),
            before: Expect::Passes(45),
        },
        Run {
            name: "latest",
            point: &a,
            prefix: prefix.clone(),
            backup: "latestCompleted".into(),
            window: ab,
            this_build: Expect::Refused(vec!["names `latestCompleted`".into()]),
            round1: Expect::Refused(vec!["names `latestCompleted`".into()]),
            before: Expect::Passes(45),
        },
        Run {
            name: "copy",
            point: &a,
            prefix: copy_prefix.clone(),
            backup: a.backup_id.clone(),
            window: ab,
            this_build: Expect::Refused(engine_key_needles(&a, &copy_prefix, &prefix)),
            round1: Expect::Refused(vec![format!(
                "its manifest is {copy_prefix}/{}/manifest.json",
                a.backup_id
            )]),
            before: Expect::Passes(30),
        },
        Run {
            name: "edited",
            point: &a,
            prefix: edit_prefix.clone(),
            backup: a.backup_id.clone(),
            window: ab,
            this_build: Expect::Refused(engine_key_needles(&a, &edit_prefix, &prefix)),
            round1: Expect::Refused(vec![format!("its manifest hashes to {}", digest(&edited))]),
            before: Expect::Passes(30),
        },
        Run {
            name: "nested",
            point: &n_point,
            prefix: nest.clone(),
            backup: nested_id.clone(),
            window: nn,
            this_build: Expect::Refused(engine_key_needles(&n_point, &nest, &format!("{nest}/a"))),
            round1: Expect::RestoresOther(45),
            before: Expect::RestoresOther(45),
        },
    ];

    let round1 = std::env::var_os("FX16_ROUND1_BIN").map(PathBuf::from);
    let before = std::env::var_os("FX16_BEFORE_BIN").map(PathBuf::from);
    let mut evidence = serde_json::Map::new();
    let set = |b: &Backup| {
        json!({
            "backup_id": b.backup_id,
            "receipt_key": b.receipt_key,
            "point_id": logweir::catalog::record::point_id(&b.receipt_bytes),
            "manifest_key": b.receipt.archive.manifest_key,
            "manifest_sha256": b.receipt.archive.manifest_sha256,
            "records": b.receipt.records,
        })
    };
    evidence.insert(
        "fixture".into(),
        json!({
            "topic": topic,
            "point_a": set(&a),
            "set_b": set(&b),
            "nested_engine_path_set": set(&engine_path_set),
            "point_n_nested": set(&n_point),
            "first_copy_key": first_copy_key,
            "edit_manifest_key": edit_manifest_key,
            "edited_manifest_sha256": digest(&edited),
            "engine": engine_version(),
            "binary": bin(),
            "round1_binary": round1,
            "before_binary": before,
        }),
    );

    for run in &runs {
        let builds: Vec<(&str, PathBuf, &Expect)> = [
            Some(("this-build", bin(), &run.this_build)),
            round1.clone().map(|p| ("round1", p, &run.round1)),
            before.clone().map(|p| ("before-fx16", p, &run.before)),
        ]
        .into_iter()
        .flatten()
        .collect();
        for (build, binary, expect) in builds {
            let naming = format!("fx16{}-{build}-{n}-", run.name);
            let target = format!("{naming}{topic}");
            topics.0.push(target.clone());
            let spec = restore_spec(
                run.point,
                &run.prefix,
                &run.backup,
                &topic,
                &naming,
                run.window,
            );
            let r = restore(&binary, &spec, &format!("{n}-{}-{build}", run.name));
            let exists = harness::topic_exists(&target);
            let restored = exists.then(|| records_on(&target));
            evidence.insert(
                format!("{}/{build}", run.name),
                json!({
                    "exit": r.out.status.code(),
                    "outcome": r.scorecard["outcome"],
                    "target_topic": target,
                    "target_exists": exists,
                    "target_records": restored,
                    "lines": kept(&r.out),
                }),
            );
            let what = format!("{} / {build}", run.name);
            match expect {
                Expect::Passes(records) => {
                    assert_eq!(r.out.status.code(), Some(0), "{what}: {}", text(&r.out));
                    assert_eq!(r.scorecard["outcome"], "pass", "{what}");
                    assert_eq!(restored, Some(*records), "{what}");
                }
                Expect::RestoresOther(records) => {
                    let t = text(&r.out);
                    assert!(
                        !t.contains("PointBindingSetMismatch"),
                        "{what}: the build under test let it through:\n{t}"
                    );
                    assert!(
                        t.contains("progress-phase=0:admit"),
                        "{what}: it went on past the binding:\n{t}"
                    );
                    if exists {
                        assert_eq!(restored, Some(*records), "{what}: another set's records");
                    }
                }
                Expect::Refused(needles) if build == "this-build" => {
                    assert_refused(&r, &target, needles);
                }
                Expect::Refused(needles) => {
                    let t = text(&r.out);
                    assert_eq!(r.out.status.code(), Some(3), "{what}: {t}");
                    assert!(t.contains("PointBindingSetMismatch. "), "{what}: {t}");
                    for needle in needles {
                        assert!(t.contains(needle.as_str()), "{what}: {needle:?}:\n{t}");
                    }
                    assert!(!exists, "{what}: nothing created");
                }
            }
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
