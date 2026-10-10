#![cfg(feature = "e2e")]
//! **PROD-02.1 — a backup of a source whose records all expired is an
//! honest, named EMPTY CAPTURE, observed live.**
//!
//! The PoC's nightly schedules failed exit 1 "operational" over a source that
//! retention had emptied: the engine exits 0 and writes a manifest naming the
//! topic with no partitions, and the read-back refused it. These rows run the
//! real `logweir backup run` against a topic whose records were produced and
//! then deleted (`kafka-delete-records`), so every partition has
//! `earliest == latest`.
//!
//! | row | proves | its control |
//! |---|---|---|
//! | `r1_an_emptied_source_is_a_signed_empty_capture` | exit 0; the summary names an EMPTY CAPTURE; no catalog point; a 1.8.0 receipt with `low == high` per partition and the empty window; both readers accept it and print the same EMPTY CAPTURE lines | the same topic with records again backs up as an ordinary point (a window, a catalog point, `low < high`) |
//! | `r2_a_partly_empty_source_names_its_empty_partitions` | an emptied topic beside a topic with records is an ordinary backup; the summary and the receipt name every empty partition | the topic with records keeps its counts and window |
//! | `r5_a_restore_of_an_empty_capture_is_refused_before_anything_runs` | `logweir restore run` over the empty capture's set is exit 3 `GuardRefused`, naming the empty capture, and creates no target topic | — (the refusal is by name and creates nothing) |
//!
//! The `Backup` status half of R1 (`Complete=True`, reason `EmptyCapture`,
//! only beside a verified receipt) is the controller's, held by
//! `crates/weirkeeper/tests/backup_controller.rs` and proved live at the PoC.
//!
//! # Running them
//!
//! ```text
//! eval "$(e2e/compose/stack-env.sh --slot N)"
//! just e2e-up
//! cargo build -p logweir
//! AWS_EC2_METADATA_DISABLED=true cargo test -p e2e --features e2e --test empty_capture -- \
//!     --test-threads=1 --nocapture
//! ```
//!
//! Each row writes what it observed to `empty-capture/<row>.json` under the
//! stack's scratch directory (`harness::demo_dir()`).
mod harness;

use harness::{bin, demo_dir, engine_bin, engine_digest, engine_mount, engine_version, root};
use logweir_core::backup_receipt::BackupReceipt;
use logweir_kafka::reader::ClusterReader;
use serde_json::{json, Value};
use std::io::Read;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after 1970")
        .as_millis() as i64
}

fn write_evidence(row: &str, v: &Value) {
    let dir = demo_dir().join("empty-capture");
    std::fs::create_dir_all(&dir).expect("the evidence directory");
    let p = dir.join(format!("{row}.json"));
    std::fs::write(&p, serde_json::to_vec_pretty(v).expect("serialises")).expect("written");
    eprintln!("[02.1] evidence: {}", p.display());
}

// ============================================================ the source

/// One of the broker's own CLIs, inside its RUNNING container.
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

const IN_NETWORK: &str = "kafka-broker-1:9094";

/// `n` records into every partition of `topic`, stamped now.
fn produce(topic: &str, partitions: usize, per_partition: usize) {
    let t = now_ms();
    let payloads: Vec<String> = (0..partitions * per_partition)
        .map(|i| format!("{topic}-{i}"))
        .collect();
    let records: Vec<(i64, &str)> = payloads
        .iter()
        .enumerate()
        .map(|(i, p)| (t + i as i64, p.as_str()))
        .collect();
    harness::produce_with_timestamps(topic, &records)
        .unwrap_or_else(|e| panic!("produce into {topic}: {e}"));
}

/// Every partition's `[low, high]`, through the product's own marks read.
fn marks(topic: &str) -> Vec<(i64, i64)> {
    let got = ClusterReader::partition_marks(&harness::reader(), &[topic.to_string()]);
    let mut parts = got
        .get(topic)
        .cloned()
        .expect("an answer for the topic")
        .unwrap_or_else(|e| panic!("marks of {topic}: {e}"));
    parts.sort_by_key(|(p, _)| *p);
    parts
        .into_iter()
        .map(|(p, m)| {
            let m = m.unwrap_or_else(|e| panic!("{topic}:{p}: {e}"));
            (m.log_start, m.high_watermark)
        })
        .collect()
}

/// A topic of `partitions` partitions whose records were produced and then
/// all deleted (`kafka-delete-records --offset -1`), as retention leaves it:
/// every partition `earliest == latest > 0`.
fn emptied_topic(topic: &str, partitions: usize) {
    harness::create_topic(topic, partitions as i32);
    produce(topic, partitions, 2);
    let spec = json!({
        "version": 1,
        "partitions": (0..partitions)
            .map(|p| json!({"topic": topic, "partition": p, "offset": -1}))
            .collect::<Vec<_>>(),
    })
    .to_string();
    let path = format!("/tmp/delete-{topic}.json");
    let o = broker_cli(&[
        "sh",
        "-c",
        &format!(
            "printf '%s' '{spec}' > {path} && /opt/kafka/bin/kafka-delete-records.sh \
             --bootstrap-server {IN_NETWORK} --offset-json-file {path}"
        ),
    ]);
    assert!(o.status.success(), "kafka-delete-records:\n{}", text(&o));
    let m = marks(topic);
    assert_eq!(m.len(), partitions);
    assert!(
        m.iter().all(|(lo, hi)| lo == hi && *hi > 0),
        "the fixture is a source whose records were all deleted: {m:?}"
    );
}

// ============================================================ the backup

fn signing_pem() -> std::path::PathBuf {
    root().join("e2e/fixtures/signed/signing.pem")
}

fn s3_user() -> String {
    ["minio", "admin"].concat()
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
    stdout: String,
    receipt_key: String,
    receipt_bytes: Vec<u8>,
    receipt: BackupReceipt,
    catalog_key: Option<String>,
    prefix: String,
}

/// `logweir backup run` of `topics` as set `backup_id` under `prefix`, as a
/// schedule runs it (`--triggered-by schedule/...`).
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
    let allow = demo_dir().join("ec-backup-allowed-clusters.json");
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
        .arg(signing_pem())
        .args(["--triggered-by", "schedule/empty-capture"]);
    engine_env(&mut c);
    let out = output_within(c, 900);
    assert_eq!(
        out.status.code(),
        Some(0),
        "logweir backup run {backup_id} must exit 0, never 1 operational:\n{}",
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
    let catalog_key = line("catalog-key=");
    let receipt_bytes = archive_get(
        prefix,
        &receipt_key,
        logweir_engine_oso::storage::caps::SIGNED_DOCUMENT,
    );
    let receipt: BackupReceipt = serde_json::from_slice(&receipt_bytes).expect("a receipt");
    Backup {
        stdout,
        receipt_key,
        receipt_bytes,
        receipt,
        catalog_key,
        prefix: prefix.to_string(),
    }
}

/// The summary line `backup run` prints before its evidence keys.
fn summary(b: &Backup) -> String {
    b.stdout
        .lines()
        .find(|l| l.starts_with("backup ") && l.contains(" captured "))
        .expect("backup run prints its summary line")
        .to_string()
}

/// Both readers over a receipt, as an auditor runs them: their exit codes and
/// every `source_watermarks` line each printed.
fn both_readers(b: &Backup, label: &str) -> Value {
    let dir = demo_dir().join(format!("ec-verify-{label}"));
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
                let i = l
                    .find("source_watermarks[")
                    .or_else(|| l.find("source_watermarks:"))?;
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

fn pairs(b: &Backup, topic: &str) -> Vec<[u64; 2]> {
    b.receipt
        .source_watermarks
        .as_ref()
        .expect("every receipt this build signs over a readable source carries source_watermarks")
        .get(topic)
        .unwrap_or_else(|| panic!("no source_watermarks entry for {topic}"))
        .clone()
}

/// **R1.** A source emptied by retention backs up as a signed EMPTY CAPTURE.
/// Control: the same topic, with records again, is an ordinary backup.
#[test]
fn r1_an_emptied_source_is_a_signed_empty_capture() {
    let n = nonce();
    let topic = format!("ec-src-{n}");
    let prefix = format!("ec-{n}");
    emptied_topic(&topic, 3);

    let empty = backup(&format!("ec-empty-{n}"), &prefix, &[&topic]);
    let line = summary(&empty);
    assert!(line.contains("an EMPTY CAPTURE"), "{line}");
    assert!(line.contains("captured 0 record(s)"), "{line}");
    assert_eq!(
        empty.catalog_key, None,
        "an empty capture is not a recovery point"
    );
    assert_eq!(empty.receipt.validate_invariants(), Ok(()));
    assert!(empty.receipt.is_empty_capture());
    harness::assert_format_at_least(
        &empty.receipt.format_version,
        logweir_core::backup_receipt::FORMAT_VERSION_WITH_SOURCE_WATERMARKS,
        "an empty capture's receipt",
    );
    let wm = pairs(&empty, &topic);
    assert_eq!(wm.len(), 3);
    assert!(
        wm.iter().all(|[lo, hi]| lo == hi && *hi > 0),
        "low == high for every partition, after records were deleted: {wm:?}"
    );
    assert_eq!(empty.receipt.records.get(&topic), Some(&0));
    let readers = both_readers(&empty, "r1-empty");
    assert_eq!(readers["rust_exit"], 0, "{readers}");
    assert_eq!(readers["python_exit"], 0, "{readers}");
    assert_eq!(readers["rust_lines"], readers["python_lines"], "{readers}");
    assert!(
        readers["rust_lines"][0]
            .as_str()
            .is_some_and(|l| l.starts_with("source_watermarks: an EMPTY CAPTURE")),
        "{readers}"
    );

    // CONTROL: the same topic with records is an ordinary point.
    produce(&topic, 3, 2);
    let full = backup(&format!("ec-full-{n}"), &prefix, &[&topic]);
    assert!(
        !summary(&full).contains("EMPTY CAPTURE"),
        "{}",
        summary(&full)
    );
    assert!(
        full.catalog_key.is_some(),
        "an ordinary backup is a recovery point"
    );
    assert!(!full.receipt.is_empty_capture());
    assert!(full.receipt.covered.from_ms < full.receipt.covered.to_ms);
    assert_eq!(full.receipt.records.get(&topic), Some(&6));
    let wm_full = pairs(&full, &topic);
    assert!(wm_full.iter().all(|[lo, hi]| lo < hi), "{wm_full:?}");
    let readers_full = both_readers(&full, "r1-full");
    assert_eq!(readers_full["rust_exit"], 0, "{readers_full}");
    assert_eq!(readers_full["python_exit"], 0, "{readers_full}");
    assert_eq!(readers_full["rust_lines"], readers_full["python_lines"]);

    write_evidence(
        "r1",
        &json!({
            "topic": topic,
            "engine": {"version": engine_version(), "digest": engine_digest()},
            "empty": {"summary": line, "receipt_key": empty.receipt_key,
                      "format_version": empty.receipt.format_version,
                      "covered": [empty.receipt.covered.from_ms, empty.receipt.covered.to_ms],
                      "source_watermarks": empty.receipt.source_watermarks,
                      "catalog_key": empty.catalog_key, "readers": readers},
            "control": {"summary": summary(&full), "receipt_key": full.receipt_key,
                        "covered": [full.receipt.covered.from_ms, full.receipt.covered.to_ms],
                        "source_watermarks": full.receipt.source_watermarks,
                        "catalog_key": full.catalog_key, "readers": readers_full},
        }),
    );
}

/// **R2.** An emptied topic beside a topic with records is an ordinary backup
/// that names every empty partition.
#[test]
fn r2_a_partly_empty_source_names_its_empty_partitions() {
    let n = nonce();
    let emptied = format!("ec-gone-{n}");
    let kept = format!("ec-kept-{n}");
    emptied_topic(&emptied, 2);
    harness::create_topic(&kept, 2);
    produce(&kept, 2, 3);

    let b = backup(
        &format!("ec-mixed-{n}"),
        &format!("ec-{n}"),
        &[&emptied, &kept],
    );
    let line = summary(&b);
    assert!(!line.contains("EMPTY CAPTURE"), "{line}");
    for p in 0..2 {
        assert!(line.contains(&format!("{emptied}:{p}")), "{line}");
    }
    assert!(!b.receipt.is_empty_capture());
    assert!(b.catalog_key.is_some());
    assert_eq!(b.receipt.records.get(&kept), Some(&6));
    assert_eq!(b.receipt.records.get(&emptied), Some(&0));
    let named = logweir_core::backup_receipt::empty_partitions(
        b.receipt
            .source_watermarks
            .as_ref()
            .expect("the watermarks"),
    );
    assert_eq!(
        named,
        vec![(emptied.clone(), 0), (emptied.clone(), 1)],
        "exactly the emptied topic's partitions are named"
    );
    let readers = both_readers(&b, "r2");
    assert_eq!(readers["rust_exit"], 0, "{readers}");
    assert_eq!(readers["python_exit"], 0, "{readers}");
    assert_eq!(readers["rust_lines"], readers["python_lines"]);
    write_evidence(
        "r2",
        &json!({"summary": line, "records": b.receipt.records,
                "source_watermarks": b.receipt.source_watermarks, "readers": readers}),
    );
}

/// **R5.** A restore of an empty capture is refused by name before anything
/// runs: exit 3, `GuardRefused`, and no target topic is created.
#[test]
fn r5_a_restore_of_an_empty_capture_is_refused_before_anything_runs() {
    let n = nonce();
    let topic = format!("ec-r5-{n}");
    let prefix = format!("ec-r5-{n}");
    emptied_topic(&topic, 2);
    let set = format!("ec-r5-set-{n}");
    let empty = backup(&set, &prefix, &[&topic]);
    assert!(empty.receipt.is_empty_capture());

    let mut spec: Value = serde_yaml::from_str(
        &std::fs::read_to_string(root().join("examples/drill.yaml")).expect("the example"),
    )
    .map(|v: serde_yaml::Value| serde_json::to_value(v).expect("json"))
    .expect("yaml");
    spec["source"]["storage"]["prefix"] = json!(prefix);
    spec["source"]["backup"] = json!(set);
    spec["source"]["topics"] = json!([topic]);
    spec["target"]["teardown"] = json!("keep");
    let t = now_ms();
    let rfc = |ms: i64| {
        chrono::DateTime::from_timestamp_millis(ms)
            .expect("an instant")
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    };
    spec["sample"]["window_start"] = json!(rfc(t - 3_600_000));
    spec["sample"]["window_end"] = json!(rfc(t + 60_000));
    let mut yaml: serde_yaml::Value = serde_json::from_value(spec).expect("back to yaml");
    harness::rebind_addresses(&mut yaml);
    let mut opts = harness::RunOpts::new(&yaml);
    opts.restore_run = true;
    let run = harness::run_with(opts);
    let out = text(&run.out);
    assert_eq!(run.out.status.code(), Some(3), "{out}");
    assert!(
        out.lines().any(|l| l == "refusal-reason=GuardRefused"),
        "the refusal is named on its final line:\n{out}"
    );
    assert!(
        out.contains("so it holds no record to restore: an empty capture"),
        "{out}"
    );
    let target = format!("drill-{topic}");
    assert!(
        !harness::topic_exists(&target),
        "a refused restore creates no target topic ({target})"
    );
    write_evidence(
        "r5",
        &json!({"set": set, "exit": run.out.status.code(), "target_exists": false,
                "stderr_tail": out.lines().rev().take(6).collect::<Vec<_>>()}),
    );
}
