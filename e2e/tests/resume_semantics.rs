#![cfg(feature = "e2e")]
//! **PROD-07.1 — what the pinned engine's restore checkpoint does when a
//! restore is interrupted, measured.** Research harness for
//! `docs/to-do/decisions/PROD-07.1-resume-semantics.md`; it ships no product
//! behaviour.
//!
//! One archive (two topics, three partitions, 1,200 records per partition in
//! 300-record segments) is taken with the SHIPPED `logweir backup run`. Each
//! scenario then restores it into its own pair of target topics by running the
//! digest-pinned ENGINE directly, with the restore document Logweir renders
//! (`logweir_engine_oso::render_restore::render`) and a checkpoint path the
//! scenario controls, and interrupts it: `SIGKILL` mid-topic and after a
//! checkpoint commit, `SIGTERM`, a kill while produce requests are in flight
//! (broker frozen), a stale checkpoint over a recreated target, a corrupt
//! checkpoint, two workers, and a foreign write into the target. Logweir
//! cannot be used to interrupt its own engine: a killed `logweir` leaves the
//! engine running to completion (PROD-01.1 §5.2, `docs/stability.md` Later
//! #13), so the engine is driven here the way `e2e/fixtures/engine-docker.sh`
//! runs it, with a container name so a signal reaches exactly one engine.
//!
//! **The oracle is PROD-08.1's complete verification**
//! (`logweir::drill::phase7_verify::run_with_coverage` with
//! `Coverage::Complete`), over the slot's broker and MinIO: every archived
//! record against every restored record, with exact missing, duplicate,
//! out-of-order and unexpected counts per partition. The engine's exit code
//! is recorded and never used as the outcome.
//!
//! Two keys are appended to the rendered document, identically in every
//! attempt of a scenario, so a kill lands at a predictable point:
//! `produce_batch_size: 100` and `rate_limit_records_per_sec: 300` (per
//! partition; the engine sleeps `batch / rate` before each produce request,
//! `kafka-backup-core/src/restore/engine.rs:1846-1851` in 0.23.3). The engine
//! is also run with `kafka_backup_core::kafka::produce=trace`, whose
//! "Produced N records to T:P at offset O" line is written after the broker's
//! acknowledgement is parsed (`kafka/produce.rs:181-196`), so the log names
//! exactly which requests were acknowledged before a kill.
//!
//! `resume_prototype` is the evidence for the record's recommended route: it
//! resumes each partition after the target's last `x-original-offset` by
//! producing the remaining archived records itself (byte-identical to what the
//! engine produces: key, value, headers in order, CreateTime), refusing a
//! target whose prefix is not a clean, in-order copy of the archive. It is a
//! prototype that serves the measurement, not a product path.
//!
//! # Running it
//!
//! `#[ignore]`d: it freezes the slot's broker for about two seconds and runs
//! some twenty engine containers. On a PROD-01.5 slot:
//!
//! ```text
//! eval "$(e2e/compose/stack-env.sh --slot 1)"
//! cargo build -p logweir
//! AWS_EC2_METADATA_DISABLED=true cargo test -p e2e --features e2e \
//!     --test resume_semantics -- --ignored --test-threads=1 --nocapture
//! ```
//!
//! `LOGWEIR_PROD071_ROWS=k1,k2` runs a subset. Every scenario writes
//! `<demo_dir>/resume-semantics/<row>.json`; predictions are asserted only on
//! `CONTRACT_ENGINE` (on another engine the rows record and assert nothing).

mod harness;
#[allow(dead_code)]
mod record_semantics_support;

use harness::*;
use record_semantics_support::kafka::{self, Archive, Isolation, Out};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const PARTS: i32 = 3;
const PER_PARTITION: i64 = 1_200;
const SEGMENT_RECORDS: u64 = 300;
const SEGMENTS_PER_TOPIC: usize = (PARTS as usize) * (PER_PARTITION as usize / 300);
const PER_TOPIC: i64 = PER_PARTITION * PARTS as i64;
const PRODUCE_BATCH: i64 = 100;
const RATE_PER_PARTITION: i64 = 300;
/// The fixture's CreateTime base, `2025-10-09`: fixed, so the archive's window
/// is stated, and past the broker's default retention, so every topic this
/// file creates carries `retention.ms=-1`.
const T: i64 = 1_760_000_000_000;
const ID_PREFIX: &str = "resume71-";
const CONTRACT_ENGINE: &str = "0.23.3";
const ENGINE_DEADLINE_SECS: u64 = 300;

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970")
        .as_millis()
}

// ================================================================ the lab

/// Every topic, archive and engine container one run creates, released on
/// every exit path (a panicking assertion included).
struct Lab {
    nonce: String,
    dir: PathBuf,
    topics: Vec<String>,
    archives: Vec<String>,
    containers: Vec<String>,
}

impl Lab {
    fn new() -> Lab {
        kafka::use_stack_s3_env();
        let nonce = format!("{:010}", now_ms() % 10_000_000_000);
        // Under the engine mount, so every path the rendered document names is
        // the same path inside the engine container.
        let dir = engine_mount().join(format!("{ID_PREFIX}{nonce}"));
        std::fs::create_dir_all(&dir).expect("the scenario directory is creatable");
        Lab {
            nonce,
            dir,
            topics: Vec::new(),
            archives: Vec::new(),
            containers: Vec::new(),
        }
    }

    fn name(&self, suffix: &str) -> String {
        format!("{ID_PREFIX}{}-{suffix}", self.nonce)
    }

    /// A topic with the pair Logweir creates its restore targets with:
    /// `CreateTime` and `retention.ms=-1`.
    fn topic(&mut self, suffix: &str) -> String {
        let t = self.name(suffix);
        self.topics.push(t.clone());
        create_topic_for_fixed_timestamps(&t, PARTS, &[("message.timestamp.type", "CreateTime")]);
        t
    }
}

impl Drop for Lab {
    fn drop(&mut self) {
        for c in &self.containers {
            let _ = run_quiet("docker", &["rm", "-f", c], 60);
        }
        for t in &self.topics {
            let _ = kafka_topics(&[
                "--bootstrap-server",
                "kafka-broker-1:9094",
                "--delete",
                "--if-exists",
                "--topic",
                t,
            ]);
        }
        for b in &self.archives {
            let _ = mc(&[
                "rm",
                "--recursive",
                "--force",
                &format!("local/{ARCHIVE_BUCKET}/{b}/"),
            ]);
            let _ = mc(&[
                "rm",
                "--recursive",
                "--force",
                &format!("local/{ARCHIVE_BUCKET}/logweir/backups/{b}/"),
            ]);
        }
        let left = run_quiet(
            "docker",
            &[
                "ps",
                "-aq",
                "--filter",
                &format!("name={ID_PREFIX}{}", self.nonce),
            ],
            30,
        );
        if !left.trim().is_empty() {
            eprintln!("[resume71] containers left after cleanup: {left}");
        }
    }
}

fn run_quiet(program: &str, args: &[&str], secs: u64) -> String {
    let mut c = Command::new(program);
    c.args(args);
    kafka::output_within(c, secs)
        .map(|o| o.stdout_utf8())
        .unwrap_or_default()
}

/// The archive every scenario restores, and what phase 7 needs to verify a
/// target against it.
struct Ctx {
    backup_id: String,
    sources: Vec<String>,
    archives: BTreeMap<String, Archive>,
    facts: logweir_core::engine::BackupSetFacts,
    window: (i64, i64),
}

fn fixture(tag: &str) -> Vec<Out> {
    let pad = "v".repeat(100);
    (0..PARTS)
        .flat_map(|p| {
            let pad = pad.clone();
            let tag = tag.to_string();
            (0..PER_PARTITION).map(move |i| {
                Out::kv(
                    p,
                    Some(T + i),
                    &format!("{tag}-p{p}-{i}"),
                    &format!("{i} {pad}"),
                )
            })
        })
        .collect()
}

fn setup(lab: &mut Lab) -> Ctx {
    let a = lab.topic("src-a");
    let b = lab.topic("src-b");
    kafka::produce_plain(&a, &fixture("a")).expect("produce topic a");
    kafka::produce_plain(&b, &fixture("b")).expect("produce topic b");
    let backup_id = lab.name("arch");
    lab.archives.push(backup_id.clone());
    let o = kafka::backup_run(&backup_id, &[&a, &b], SEGMENT_RECORDS);
    assert_eq!(
        o.status.code(),
        Some(0),
        "`logweir backup run` must exit 0\nstdout:\n{}\nstderr:\n{}",
        o.stdout_utf8(),
        o.stderr_utf8()
    );
    let mut archives = BTreeMap::new();
    for t in [&a, &b] {
        let arc = kafka::read_archive(&backup_id, t).expect("archive");
        assert_eq!(
            arc.records.len() as i64,
            PER_TOPIC,
            "{t}: the archive holds every fixture record"
        );
        assert_eq!(
            arc.segments.len(),
            SEGMENTS_PER_TOPIC,
            "{t}: {SEGMENT_RECORDS}-record segments"
        );
        archives.insert(t.clone(), arc);
    }
    let facts = facts_of(&backup_id, &archives[&a]);
    let floor = archives
        .values()
        .flat_map(|a| a.segments.iter().map(|s| s.start_timestamp))
        .min()
        .expect("segments");
    Ctx {
        backup_id,
        sources: vec![a, b],
        archives,
        facts,
        window: (floor, T + PER_PARTITION + 10_000),
    }
}

/// A fresh target pair for one scenario: source -> target.
fn targets(lab: &mut Lab, ctx: &Ctx, label: &str) -> BTreeMap<String, String> {
    ctx.sources
        .iter()
        .zip(["a", "b"])
        .map(|(s, x)| (s.clone(), lab.topic(&format!("{label}-{x}"))))
        .collect()
}

// ============================================================ the engine

fn plan(
    ctx: &Ctx,
    mapping: &BTreeMap<String, String>,
    checkpoint: &Path,
    offsets: &Path,
) -> logweir_core::engine::RestorePlan {
    use logweir_core::engine::{AuthRender, BackupSetRef, RestorePlan, WindowFloorSource};
    RestorePlan {
        set: BackupSetRef {
            backup_id: ctx.backup_id.clone(),
            manifest_key: kafka::manifest_key(&ctx.backup_id),
        },
        storage: kafka::archive_location(&ctx.backup_id),
        target_bootstrap: vec![kafka::bootstrap()],
        target_auth: AuthRender::Plaintext,
        topic_mapping: mapping.clone(),
        time_window: (
            chrono::DateTime::from_timestamp_millis(ctx.window.0).expect("floor"),
            chrono::DateTime::from_timestamp_millis(ctx.window.1).expect("end"),
        ),
        window_floor_source: WindowFloorSource::ArchiveManifest,
        default_replication_factor: 1,
        checkpoint_state: checkpoint.to_path_buf(),
        checkpoint_interval_secs: 30,
        offset_report: offsets.to_path_buf(),
    }
}

/// Logweir's rendered restore document, plus the two pacing keys (the same in
/// every attempt of a scenario), written to `doc`.
fn write_doc(
    ctx: &Ctx,
    mapping: &BTreeMap<String, String>,
    checkpoint: &Path,
    offsets: &Path,
    doc: &Path,
) {
    let rendered =
        logweir_engine_oso::render_restore::render(&plan(ctx, mapping, checkpoint, offsets))
            .expect("the restore document renders");
    // The pacing keys go into the `restore:` block, which must be the
    // document's last top-level block for an append to land in it.
    let last_top = rendered
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with(' ') && !l.starts_with('#'))
        .next_back()
        .expect("a rendered document");
    assert_eq!(last_top, "restore:", "the restore block is the last block");
    let text = format!(
        "{rendered}  produce_batch_size: {PRODUCE_BATCH}\n  rate_limit_records_per_sec: {RATE_PER_PARTITION}\n"
    );
    if let Some(d) = doc.parent() {
        std::fs::create_dir_all(d).expect("the attempt directory is creatable");
    }
    for p in [checkpoint, offsets] {
        if let Some(d) = p.parent() {
            std::fs::create_dir_all(d).expect("the checkpoint directory is creatable");
        }
    }
    std::fs::write(doc, text).expect("the restore document is writable");
}

/// Start `kafka-backup restore --config <doc>` in a NAMED container, the way
/// `e2e/fixtures/engine-docker.sh` runs it (same pinned digest, same host
/// mount at the same path, same `localhost` rewrite), detached.
fn start_engine(lab: &mut Lab, label: &str, doc: &Path) -> String {
    let name = lab.name(label);
    let mount = engine_mount().display().to_string();
    let (user, secret) = kafka::s3_credentials();
    let image = format!("osodevops/kafka-backup@{}", engine_digest());
    let script = "gw=$(getent hosts host.docker.internal | cut -d\" \" -f1 | head -1)
      if [ -z \"$gw\" ]; then echo \"no host.docker.internal\" >&2; exit 1; fi
      printf \"%s\\tlocalhost\\n%s\\thost.docker.internal\\n\" \"$gw\" \"$gw\" > /etc/hosts
      exec kafka-backup \"$@\"";
    let args: Vec<String> = [
        "run",
        "-d",
        "--name",
        name.as_str(),
        "--platform",
        "linux/amd64",
        "--user",
        "0:0",
        "-e",
        format!("AWS_ACCESS_KEY_ID={user}").as_str(),
        "-e",
        format!("AWS_SECRET_ACCESS_KEY={secret}").as_str(),
        "-e",
        "AWS_REGION=us-east-1",
        "-e",
        "NO_COLOR=1",
        "-e",
        "RUST_LOG=info,kafka_backup_core::kafka::produce=trace",
        "-v",
        format!("{mount}:{mount}").as_str(),
        "-w",
        mount.as_str(),
        "--entrypoint",
        "/bin/bash",
        image.as_str(),
        "-c",
        script,
        "kafka-backup",
        "restore",
        "--config",
        doc.to_str().expect("a UTF-8 path"),
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let mut c = Command::new("docker");
    c.args(&args);
    lab.containers.push(name.clone());
    let o = kafka::output_within(c, 120).expect("docker run");
    assert_eq!(
        o.status.code(),
        Some(0),
        "docker run {name}: {}",
        o.stderr_utf8()
    );
    name
}

fn running(name: &str) -> bool {
    run_quiet("docker", &["inspect", "-f", "{{.State.Running}}", name], 30).trim() == "true"
}

fn wait_engine(name: &str, secs: u64) -> Option<i64> {
    run_quiet("docker", &["wait", name], secs)
        .trim()
        .parse()
        .ok()
}

fn signal(name: &str, sig: &str) {
    let mut c = Command::new("docker");
    c.args(["kill", "-s", sig, name]);
    let o = kafka::output_within(c, 60).expect("docker kill");
    assert_eq!(
        o.status.code(),
        Some(0),
        "docker kill -s {sig} {name}: {}",
        o.stderr_utf8()
    );
}

fn logs(name: &str) -> String {
    let mut c = Command::new("docker");
    c.args(["logs", name]);
    let o = kafka::output_within(c, 60).expect("docker logs");
    strip_ansi(&format!("{}{}", o.stdout_utf8(), o.stderr_utf8()))
}

fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut it = s.chars().peekable();
    while let Some(ch) = it.next() {
        if ch == '\u{1b}' {
            if it.peek() == Some(&'[') {
                it.next();
                for c in it.by_ref() {
                    if c.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
            continue;
        }
        out.push(ch);
    }
    out
}

fn interesting(log: &str) -> Vec<String> {
    log.lines()
        .filter(|l| {
            [
                "checkpoint",
                "Checkpoint",
                "Shutdown",
                "shutdown",
                "Restore completed",
                "ERROR",
                "WARN",
                "error",
            ]
            .iter()
            .any(|k| l.contains(k))
        })
        .map(str::to_string)
        .collect()
}

/// `(target topic, partition)` -> the end of the acknowledged prefix: the
/// largest `base offset + count` the engine logged as acknowledged
/// ("Produced N records to T:P at offset O", written after the broker's
/// response was parsed).
fn acknowledged(log: &str) -> BTreeMap<(String, i32), i64> {
    let mut out: BTreeMap<(String, i32), i64> = BTreeMap::new();
    for l in log.lines() {
        let Some(i) = l.find("Produced ") else {
            continue;
        };
        let rest = &l[i + "Produced ".len()..];
        let Some((n, rest)) = rest.split_once(" records to ") else {
            continue;
        };
        let Some((tp, off)) = rest.split_once(" at offset ") else {
            continue;
        };
        let Some((t, p)) = tp.rsplit_once(':') else {
            continue;
        };
        let (Ok(n), Ok(p), Ok(off)) = (
            n.trim().parse::<i64>(),
            p.trim().parse::<i32>(),
            off.trim().parse::<i64>(),
        ) else {
            continue;
        };
        let e = out.entry((t.to_string(), p)).or_insert(0);
        *e = (*e).max(off + n);
    }
    out
}

fn hw(topic: &str) -> BTreeMap<i32, i64> {
    kafka::high_watermarks(topic, PARTS)
        .unwrap_or_default()
        .into_iter()
        .collect()
}

fn landed(topic: &str) -> i64 {
    hw(topic).values().sum()
}

/// Poll `f` every `ms` until it holds or `secs` pass.
fn wait_until(secs: u64, ms: u64, mut f: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(ms));
    }
    false
}

/// High watermarks read twice, `secs` apart, until two readings agree: the
/// broker has nothing more to append from a dead writer.
fn settled(topics: &[&String]) -> BTreeMap<String, BTreeMap<i32, i64>> {
    let read = || -> BTreeMap<String, BTreeMap<i32, i64>> {
        topics.iter().map(|t| ((*t).clone(), hw(t))).collect()
    };
    let mut last = read();
    for _ in 0..15 {
        std::thread::sleep(Duration::from_secs(2));
        let now = read();
        if now == last {
            return now;
        }
        last = now;
    }
    last
}

fn checkpoint_json(p: &Path) -> Option<Value> {
    std::fs::read_to_string(p)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
}

fn completed_segments(cp: &Option<Value>) -> Vec<String> {
    cp.as_ref()
        .and_then(|v| v["segments_completed"].as_array().cloned())
        .unwrap_or_default()
        .iter()
        .filter_map(|s| s.as_str().map(str::to_string))
        .collect()
}

// ============================================================ the oracle

/// PROD-08.1's complete verification of `mapping`'s targets against the
/// archive: the signed `integrity` block, and the replay counts summed per
/// SOURCE topic.
fn verify(ctx: &Ctx, mapping: &BTreeMap<String, String>) -> Value {
    use logweir::drill::phase7_verify::run_with_coverage;
    use logweir_core::engine::{BackupSetRef, SampleSelection};
    let store = logweir_engine_oso::storage::Store::read_only_from_url(&kafka::archive_location(
        &ctx.backup_id,
    ))
    .expect("the archive store");
    let set = BackupSetRef {
        backup_id: ctx.backup_id.clone(),
        manifest_key: kafka::manifest_key(&ctx.backup_id),
    };
    let sel: Vec<SampleSelection> = ctx
        .sources
        .iter()
        .flat_map(|t| {
            let set = set.clone();
            (0..PARTS).map(move |p| SampleSelection {
                set: set.clone(),
                topic: t.clone(),
                partition: p,
                anchor: logweir_core::spec::Anchor::Head,
                count: 25,
                window: ctx.window,
            })
        })
        .collect();
    let scratch = engine_mount().join("resume71-verify");
    let v = run_with_coverage(
        &NoEngine,
        &reader(),
        &store,
        &ctx.facts,
        &sel,
        mapping,
        &plan(
            ctx,
            mapping,
            &scratch.join("cp.json"),
            &scratch.join("off.json"),
        ),
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::NewTopic,
        logweir_core::spec::Coverage::Complete,
        None,
    )
    .unwrap_or_else(|e| panic!("phase 7's complete lane did not run: {e:?}"));
    let block = serde_json::to_value(&v.integrity).expect("integrity serialises");
    let complete = &block["verification"]["complete"];
    let fields = [
        "expected",
        "restored",
        "matching",
        "missing",
        "unexpected",
        "duplicates",
        "out_of_order",
        "mismatched",
    ];
    let mut per_topic = serde_json::Map::new();
    for src in &ctx.sources {
        let mut sums = serde_json::Map::new();
        for f in fields {
            let n: u64 = complete["partitions"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|p| p["topic"].as_str() == Some(src.as_str()))
                .map(|p| p["replay"][f].as_u64().unwrap_or(0))
                .sum();
            sums.insert(f.to_string(), json!(n));
        }
        let findings: Vec<Value> = complete["partitions"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|p| p["topic"].as_str() == Some(src.as_str()))
            .flat_map(|p| p["findings"].as_array().cloned().unwrap_or_default())
            .take(6)
            .collect();
        sums.insert("findings".into(), Value::Array(findings));
        per_topic.insert(topic_role(ctx, src).to_string(), Value::Object(sums));
    }
    json!({
        "result": block["result"],
        "covered": complete["covered"],
        "replay": complete["replay"],
        "per_topic": per_topic,
    })
}

fn topic_role(ctx: &Ctx, src: &str) -> &'static str {
    if ctx.sources.first().map(String::as_str) == Some(src) {
        "a"
    } else {
        "b"
    }
}

fn count(v: &Value, topic: &str, field: &str) -> i64 {
    v["per_topic"][topic][field].as_i64().unwrap_or(-1)
}

fn exact(v: &Value, topic: &str) -> bool {
    count(v, topic, "matching") == PER_TOPIC
        && count(v, topic, "restored") == PER_TOPIC
        && [
            "missing",
            "unexpected",
            "duplicates",
            "out_of_order",
            "mismatched",
        ]
        .iter()
        .all(|f| count(v, topic, f) == 0)
}

struct NoEngine;

impl logweir_core::engine::DataEngine for NoEngine {
    fn id(&self) -> logweir_core::engine::EngineId {
        logweir_core::engine::EngineId {
            id: "none".into(),
            version: "none".into(),
            digest: "none".into(),
        }
    }
    fn list_backup_sets(
        &self,
        _: &logweir_core::engine::StorageUrl,
    ) -> Result<Vec<logweir_core::engine::BackupSetRef>, logweir_core::engine::EngineError> {
        Err(logweir_core::engine::EngineError::Operational(
            "unused".into(),
        ))
    }
    fn describe(
        &self,
        _: &logweir_core::engine::BackupSetRef,
    ) -> Result<logweir_core::engine::BackupSetFacts, logweir_core::engine::EngineError> {
        Err(logweir_core::engine::EngineError::Operational(
            "unused".into(),
        ))
    }
    fn preflight(
        &self,
        _: &logweir_core::engine::RestorePlan,
    ) -> Result<logweir_core::engine::PreflightReport, logweir_core::engine::EngineError> {
        Err(logweir_core::engine::EngineError::Operational(
            "unused".into(),
        ))
    }
    fn restore(
        &self,
        _: &logweir_core::engine::RestorePlan,
        _: &mut dyn logweir_core::engine::PhaseObserver,
    ) -> Result<logweir_core::engine::RestoreFacts, logweir_core::engine::EngineError> {
        Err(logweir_core::engine::EngineError::Operational(
            "unused".into(),
        ))
    }
    fn fingerprints(
        &self,
        _: &logweir_core::engine::SampleSelection,
    ) -> Result<Vec<logweir_core::engine::RecordFingerprint>, logweir_core::engine::EngineError>
    {
        Err(logweir_core::engine::EngineError::Operational(
            "the complete lane never asks for sampled fingerprints".into(),
        ))
    }
}

/// The manifest as phase 7's facts, read the way `OsoCliEngine::describe`
/// reads it (the same function as `record_semantics.rs::facts_of`).
fn facts_of(backup_id: &str, archive: &Archive) -> logweir_core::engine::BackupSetFacts {
    use logweir_core::engine::*;
    let store =
        logweir_engine_oso::storage::Store::read_only_from_url(&kafka::archive_location(backup_id))
            .expect("the archive store");
    let m = &archive.manifest;
    let topics = m["topics"]
        .as_array()
        .expect("topics")
        .iter()
        .map(|t| TopicFacts {
            name: t["name"].as_str().expect("name").to_string(),
            original_partition_count: Some(PARTS),
            source_replication_factor: Some(1),
            configurations: Default::default(),
            partitions: t["partitions"]
                .as_array()
                .expect("partitions")
                .iter()
                .map(|p| PartitionFacts {
                    partition_id: p["partition_id"].as_i64().expect("id") as i32,
                    segments: p["segments"]
                        .as_array()
                        .expect("segments")
                        .iter()
                        .map(|s| SegmentFacts {
                            key: store.qualify(s["key"].as_str().expect("key")),
                            start_offset: s["start_offset"].as_i64().expect("start"),
                            end_offset: s["end_offset"].as_i64().expect("end"),
                            start_timestamp: s["start_timestamp"].as_i64().expect("ts"),
                            end_timestamp: s["end_timestamp"].as_i64().expect("ts"),
                            record_count: s["record_count"].as_i64().expect("count"),
                            sha256: s["sha256"].as_str().unwrap_or_default().to_string(),
                            uploaded_at: s["uploaded_at"].as_i64().unwrap_or_default(),
                        })
                        .collect(),
                    gaps: vec![],
                    pruned: vec![],
                })
                .collect(),
        })
        .collect();
    BackupSetFacts {
        backup_id: backup_id.to_string(),
        created_at: chrono::Utc::now(),
        source_cluster_id: None,
        manifest_sha256: String::new(),
        manifest_version_id: None,
        consumer_group_snapshot_sha256: None,
        topics,
    }
}

// ============================================== the recommended route, prototyped

/// Resume every partition after the target's last `x-original-offset`.
///
/// Refuses (and produces nothing) when any target record carries no lineage
/// header, names an offset the archive does not hold, or does not follow the
/// previous one in strictly increasing order: such a prefix is not a clean
/// copy of the archive, and resuming after its tail would leave whatever is
/// wrong in it behind. Otherwise produces each partition's remaining archived
/// records, in offset order, byte-identical to what the engine produces.
fn resume_prototype(ctx: &Ctx, mapping: &BTreeMap<String, String>) -> Result<Value, String> {
    let mut plan: Vec<(String, i32, i64, Vec<Out>)> = Vec::new();
    for (src, tgt) in mapping {
        let archive = &ctx.archives[src];
        for p in 0..PARTS {
            let held: BTreeSet<i64> = archive
                .records
                .iter()
                .filter(|r| r.partition == p)
                .map(|r| r.offset)
                .collect();
            let restored = kafka::read_partition(tgt, p, Isolation::Uncommitted)?;
            let mut tail = -1i64;
            for r in &restored {
                let Some(o) = r.lineage_header() else {
                    return Err(format!(
                        "refused: {tgt}/{p}@{} carries no x-original-offset (a record this \
                         restore did not write)",
                        r.offset
                    ));
                };
                if !held.contains(&o) {
                    return Err(format!(
                        "refused: {tgt}/{p}@{} names source offset {o}, which the archive does \
                         not hold",
                        r.offset
                    ));
                }
                if o <= tail {
                    return Err(format!(
                        "refused: {tgt}/{p}@{} repeats or goes back to source offset {o} after \
                         {tail}; the prefix needs reconciling first",
                        r.offset
                    ));
                }
                tail = o;
            }
            let rest: Vec<Out> = archive
                .records
                .iter()
                .filter(|r| r.partition == p && r.offset > tail)
                .map(|r| Out {
                    partition: p,
                    key: r.key.clone(),
                    value: r.value.clone(),
                    headers: r.headers.clone(),
                    timestamp: Some(r.timestamp),
                })
                .collect();
            plan.push((tgt.clone(), p, tail, rest));
        }
    }
    let mut out = Vec::new();
    for (tgt, p, tail, rest) in plan {
        kafka::produce_plain(&tgt, &rest)?;
        out.push(json!({"target": tgt, "partition": p, "tail": tail, "produced": rest.len()}));
    }
    Ok(Value::Array(out))
}

// ============================================================ the scenarios

struct Outcome {
    row: &'static str,
    facts: serde_json::Map<String, Value>,
    failures: Vec<String>,
}

impl Outcome {
    fn new(row: &'static str) -> Outcome {
        Outcome {
            row,
            facts: serde_json::Map::new(),
            failures: Vec::new(),
        }
    }
    fn put(&mut self, k: &str, v: Value) {
        self.facts.insert(k.to_string(), v);
    }
    /// A prediction from the source. Recorded either way; a failed one fails
    /// the test on `CONTRACT_ENGINE`.
    fn expect(&mut self, what: &str, holds: bool) {
        let mut list = self.facts.remove("predictions").unwrap_or(json!([]));
        list.as_array_mut()
            .expect("an array")
            .push(json!({"prediction": what, "held": holds}));
        self.facts.insert("predictions".into(), list);
        if !holds {
            self.failures.push(format!("{}: {what}", self.row));
        }
    }
    fn save(&self) {
        kafka::write_json(
            &demo_dir()
                .join("resume-semantics")
                .join(format!("{}.json", self.row)),
            &Value::Object(self.facts.clone()),
        );
        eprintln!(
            "[resume71] {}: {}",
            self.row,
            serde_json::to_string(&Value::Object(self.facts.clone())).unwrap_or_default()
        );
    }
}

struct Files {
    doc: PathBuf,
    checkpoint: PathBuf,
    offsets: PathBuf,
}

fn files(lab: &Lab, row: &str, attempt: &str) -> Files {
    let d = lab.dir.join(row).join(attempt);
    Files {
        doc: d.join("restore.yaml"),
        checkpoint: d.join("checkpoint.json"),
        offsets: d.join("offsets.json"),
    }
}

fn a_of(m: &BTreeMap<String, String>, ctx: &Ctx) -> String {
    m[&ctx.sources[0]].clone()
}

fn b_of(m: &BTreeMap<String, String>, ctx: &Ctx) -> String {
    m[&ctx.sources[1]].clone()
}

/// K1 — SIGKILL in the middle of the first topic, before any checkpoint
/// commit; the next attempt runs the same document with the same path.
fn k1(lab: &mut Lab, ctx: &Ctx) -> Outcome {
    let mut o = Outcome::new("k1-kill-before-checkpoint");
    let m = targets(lab, ctx, "k1");
    let (ta, tb) = (a_of(&m, ctx), b_of(&m, ctx));
    let f = files(lab, "k1", "stable");
    write_doc(ctx, &m, &f.checkpoint, &f.offsets, &f.doc);
    let e1 = start_engine(lab, "k1-e1", &f.doc);
    let reached = wait_until(180, 50, || landed(&ta) >= 1_500 || !running(&e1));
    signal(&e1, "KILL");
    let exit1 = wait_engine(&e1, 60);
    let after = settled(&[&ta, &tb]);
    let log1 = logs(&e1);
    let acked = acknowledged(&log1);
    let landed_a: i64 = after[&ta].values().sum();
    let unacked: BTreeMap<String, i64> = after[&ta]
        .iter()
        .map(|(p, h)| {
            (
                p.to_string(),
                h - acked.get(&(ta.clone(), *p)).copied().unwrap_or(0),
            )
        })
        .collect();
    let cp_after_kill = f.checkpoint.exists();
    o.put("kill_reached_mid_topic", json!(reached));
    o.put("attempt1_exit", json!(exit1));
    o.put("landed_after_kill", json!(after));
    o.put(
        "acknowledged_end_per_partition",
        json!(acked
            .iter()
            .map(|((t, p), e)| json!([t, p, e]))
            .collect::<Vec<_>>()),
    );
    o.put("landed_minus_acknowledged_a", json!(unacked));
    o.put("checkpoint_file_after_kill", json!(cp_after_kill));
    let e2 = start_engine(lab, "k1-e2", &f.doc);
    let exit2 = wait_engine(&e2, ENGINE_DEADLINE_SECS);
    let log2 = logs(&e2);
    o.put("attempt2_exit", json!(exit2));
    o.put("attempt2_log", json!(interesting(&log2)));
    let v = verify(ctx, &m);
    o.put("verification", v.clone());
    o.expect(
        "the kill landed while topic a was partly restored",
        reached && landed_a > 0 && landed_a < PER_TOPIC,
    );
    o.expect(
        "no checkpoint file exists after a kill inside the first topic",
        !cp_after_kill,
    );
    o.expect("attempt 2 exits 0", exit2 == Some(0));
    o.expect(
        "attempt 2 loads no checkpoint",
        !log2.contains("Loaded checkpoint"),
    );
    o.expect(
        "topic a: duplicates = every record attempt 1 left in the target",
        count(&v, "a", "duplicates") == landed_a,
    );
    o.expect("topic a: nothing missing", count(&v, "a", "missing") == 0);
    o.expect("topic b: exact", exact(&v, "b"));
    o
}

/// K2 — SIGKILL in the second topic, after the first topic's checkpoint was
/// committed; the next attempt runs the same document with the same path.
fn k2(lab: &mut Lab, ctx: &Ctx) -> Outcome {
    let mut o = Outcome::new("k2-kill-after-checkpoint");
    let m = targets(lab, ctx, "k2");
    let (ta, tb) = (a_of(&m, ctx), b_of(&m, ctx));
    let f = files(lab, "k2", "stable");
    write_doc(ctx, &m, &f.checkpoint, &f.offsets, &f.doc);
    let e1 = start_engine(lab, "k2-e1", &f.doc);
    let reached = wait_until(240, 50, || {
        (f.checkpoint.exists() && landed(&tb) >= 900) || !running(&e1)
    });
    signal(&e1, "KILL");
    let exit1 = wait_engine(&e1, 60);
    let after = settled(&[&ta, &tb]);
    let cp1 = checkpoint_json(&f.checkpoint);
    let segs = completed_segments(&cp1);
    let landed_b: i64 = after[&tb].values().sum();
    o.put("kill_reached_second_topic", json!(reached));
    o.put("attempt1_exit", json!(exit1));
    o.put("landed_after_kill", json!(after));
    o.put("checkpoint_after_kill", cp1.clone().unwrap_or(Value::Null));
    let e2 = start_engine(lab, "k2-e2", &f.doc);
    let exit2 = wait_engine(&e2, ENGINE_DEADLINE_SECS);
    let log2 = logs(&e2);
    let report = checkpoint_json(&f.offsets);
    let report_keys: Vec<String> = report
        .as_ref()
        .and_then(|r| {
            r["entries"]
                .as_object()
                .map(|e| e.keys().cloned().collect())
        })
        .unwrap_or_default();
    o.put("attempt2_exit", json!(exit2));
    o.put("attempt2_log", json!(interesting(&log2)));
    o.put("attempt2_offset_report_entries", json!(report_keys));
    let v = verify(ctx, &m);
    o.put("verification", v.clone());
    let a_name = ctx.sources[0].clone();
    o.expect(
        "the checkpoint lists exactly topic a's segments",
        segs.len() == SEGMENTS_PER_TOPIC
            && segs
                .iter()
                .all(|k| k.contains(&format!("/topics/{a_name}/"))),
    );
    o.expect("attempt 2 exits 0", exit2 == Some(0));
    o.expect(
        "attempt 2 loads the checkpoint",
        log2.contains(&format!(
            "Loaded checkpoint: {SEGMENTS_PER_TOPIC} segments completed"
        )),
    );
    o.expect("topic a: exact (its segments were skipped)", exact(&v, "a"));
    o.expect(
        "topic b: duplicates = every record attempt 1 left in it",
        count(&v, "b", "duplicates") == landed_b && landed_b > 0,
    );
    o.expect("topic b: nothing missing", count(&v, "b", "missing") == 0);
    o.expect(
        "attempt 2's offset report has no entry for topic a (skipped segments add nothing)",
        !report_keys.iter().any(|k| k.starts_with(&format!("{ta}/")))
            && report_keys
                .iter()
                .filter(|k| k.starts_with(&format!("{tb}/")))
                .count()
                == PARTS as usize,
    );
    o
}

/// H1 — K2's interruption, but attempt 2 is rendered the way Logweir renders
/// every attempt: a NEW per-run checkpoint path, with attempt 1's checkpoint
/// file carried to it.
fn h1(lab: &mut Lab, ctx: &Ctx) -> Outcome {
    let mut o = Outcome::new("h1-per-run-path-defeats-the-checkpoint");
    let m = targets(lab, ctx, "h1");
    let (ta, tb) = (a_of(&m, ctx), b_of(&m, ctx));
    let f1 = files(lab, "h1", "run-1");
    let f2 = files(lab, "h1", "run-2");
    write_doc(ctx, &m, &f1.checkpoint, &f1.offsets, &f1.doc);
    write_doc(ctx, &m, &f2.checkpoint, &f2.offsets, &f2.doc);
    let e1 = start_engine(lab, "h1-e1", &f1.doc);
    let reached = wait_until(240, 50, || {
        (f1.checkpoint.exists() && landed(&tb) >= 900) || !running(&e1)
    });
    signal(&e1, "KILL");
    let _ = wait_engine(&e1, 60);
    let after = settled(&[&ta, &tb]);
    let landed_b: i64 = after[&tb].values().sum();
    let carried = std::fs::copy(&f1.checkpoint, &f2.checkpoint).is_ok();
    let segs = completed_segments(&checkpoint_json(&f2.checkpoint));
    o.put("kill_reached_second_topic", json!(reached));
    o.put("landed_after_kill", json!(after));
    o.put("carried_checkpoint_segments", json!(segs.len()));
    let e2 = start_engine(lab, "h1-e2", &f2.doc);
    let exit2 = wait_engine(&e2, ENGINE_DEADLINE_SECS);
    let log2 = logs(&e2);
    o.put("attempt2_exit", json!(exit2));
    o.put("attempt2_log", json!(interesting(&log2)));
    let v = verify(ctx, &m);
    o.put("verification", v.clone());
    o.expect(
        "attempt 1's checkpoint (topic a) was carried",
        carried && segs.len() == SEGMENTS_PER_TOPIC,
    );
    o.expect(
        "attempt 2 discards it on the hash (the path is inside the hash)",
        log2.contains("config hash mismatch"),
    );
    o.expect(
        "topic a: re-produced whole, duplicates = every record of a",
        count(&v, "a", "duplicates") == PER_TOPIC,
    );
    o.expect(
        "topic b: duplicates = every record attempt 1 left in it",
        count(&v, "b", "duplicates") == landed_b,
    );
    o
}

/// T1 — SIGTERM in the middle of the first topic, then a second attempt with
/// the same document and path.
fn t1(lab: &mut Lab, ctx: &Ctx) -> Outcome {
    let mut o = Outcome::new("t1-sigterm");
    let m = targets(lab, ctx, "t1");
    let (ta, tb) = (a_of(&m, ctx), b_of(&m, ctx));
    let f = files(lab, "t1", "stable");
    write_doc(ctx, &m, &f.checkpoint, &f.offsets, &f.doc);
    let e1 = start_engine(lab, "t1-e1", &f.doc);
    let reached = wait_until(180, 50, || landed(&ta) >= 900 || !running(&e1));
    let at_signal = landed(&ta);
    signal(&e1, "TERM");
    let exit1 = wait_engine(&e1, ENGINE_DEADLINE_SECS);
    let log1 = logs(&e1);
    let after = settled(&[&ta, &tb]);
    let segs = completed_segments(&checkpoint_json(&f.checkpoint));
    let v1 = verify(ctx, &m);
    o.put("signal_reached_mid_topic", json!(reached));
    o.put("landed_a_at_signal", json!(at_signal));
    o.put("attempt1_exit", json!(exit1));
    o.put("attempt1_log", json!(interesting(&log1)));
    o.put("landed_after_attempt1", json!(after));
    o.put("checkpoint_segments_after_attempt1", json!(segs.len()));
    o.put("verification_after_attempt1", v1.clone());
    let e2 = start_engine(lab, "t1-e2", &f.doc);
    let exit2 = wait_engine(&e2, ENGINE_DEADLINE_SECS);
    let log2 = logs(&e2);
    let v2 = verify(ctx, &m);
    o.put("attempt2_exit", json!(exit2));
    o.put("attempt2_log", json!(interesting(&log2)));
    o.put("verification_after_attempt2", v2.clone());
    o.expect(
        "the signal landed while topic a was partly restored",
        reached && at_signal < PER_TOPIC,
    );
    o.expect("attempt 1 exits 0 after SIGTERM", exit1 == Some(0));
    o.expect(
        "attempt 1 logs the shutdown between topics",
        log1.contains("Shutdown signal received, stopping restore"),
    );
    o.expect("attempt 1 finished topic a: exact", exact(&v1, "a"));
    o.expect(
        "attempt 1 wrote nothing to topic b: all of it missing",
        count(&v1, "b", "missing") == PER_TOPIC && count(&v1, "b", "restored") == 0,
    );
    o.expect(
        "the checkpoint lists topic a's segments",
        segs.len() == SEGMENTS_PER_TOPIC,
    );
    o.expect("attempt 2 exits 0", exit2 == Some(0));
    o.expect(
        "attempt 2 loads the checkpoint",
        log2.contains(&format!(
            "Loaded checkpoint: {SEGMENTS_PER_TOPIC} segments completed"
        )),
    );
    o.expect(
        "after attempt 2 both topics are exact",
        exact(&v2, "a") && exact(&v2, "b"),
    );
    o
}

/// K3 — kill while produce requests are in flight: freeze the broker, let
/// every partition send its next request, SIGKILL the engine, thaw. Then
/// resume from the target's tail (the prototype).
fn k3(lab: &mut Lab, ctx: &Ctx) -> Outcome {
    let mut o = Outcome::new("k3-kill-before-acknowledgement");
    let m = targets(lab, ctx, "k3");
    let (ta, tb) = (a_of(&m, ctx), b_of(&m, ctx));
    let f = files(lab, "k3", "stable");
    write_doc(ctx, &m, &f.checkpoint, &f.offsets, &f.doc);
    let e1 = start_engine(lab, "k3-e1", &f.doc);
    let reached = wait_until(180, 50, || landed(&ta) >= 1_500 || !running(&e1));
    let before_pause = hw(&ta);
    let mut thaw = kafka::Thaw { armed: true };
    let paused = kafka::compose_broker("pause");
    std::thread::sleep(Duration::from_millis(1_500));
    signal(&e1, "KILL");
    let exit1 = wait_engine(&e1, 60);
    std::thread::sleep(Duration::from_millis(500));
    let unpaused = kafka::compose_broker("unpause");
    thaw.armed = false;
    let after = settled(&[&ta, &tb]);
    let log1 = logs(&e1);
    let acked = acknowledged(&log1);
    let unacked: BTreeMap<String, i64> = after[&ta]
        .iter()
        .map(|(p, h)| {
            (
                p.to_string(),
                h - acked.get(&(ta.clone(), *p)).copied().unwrap_or(0),
            )
        })
        .collect();
    let unacked_total: i64 = unacked.values().sum();
    o.put("kill_reached_mid_topic", json!(reached));
    o.put("pause_exit", json!(paused.status.code()));
    o.put("unpause_exit", json!(unpaused.status.code()));
    o.put("attempt1_exit", json!(exit1));
    o.put("hw_a_before_pause", json!(before_pause));
    o.put("landed_after_thaw", json!(after));
    o.put(
        "acknowledged_end_per_partition",
        json!(acked
            .iter()
            .map(|((t, p), e)| json!([t, p, e]))
            .collect::<Vec<_>>()),
    );
    o.put("appended_without_acknowledgement_a", json!(unacked));
    let resumed = resume_prototype(ctx, &m);
    o.put(
        "resume_prototype",
        match &resumed {
            Ok(v) => v.clone(),
            Err(e) => json!({"refused": e}),
        },
    );
    let v = verify(ctx, &m);
    o.put("verification_after_resume", v.clone());
    o.expect(
        "the freeze and thaw both ran",
        paused.status.code() == Some(0) && unpaused.status.code() == Some(0),
    );
    o.expect(
        "records were appended that the engine never saw acknowledged",
        unacked_total > 0,
    );
    o.expect(
        "at most one produce request per partition was appended unacknowledged",
        unacked.values().all(|n| (0..=PRODUCE_BATCH).contains(n)),
    );
    o.expect("the tail resume ran", resumed.is_ok());
    o.expect(
        "after the tail resume both topics are exact: no duplicate of the unacknowledged batch",
        exact(&v, "a") && exact(&v, "b"),
    );
    o
}

/// S1 — a completed restore's checkpoint, then the target topics deleted and
/// recreated under the same names, then the same document and path again.
fn s1(lab: &mut Lab, ctx: &Ctx) -> (Outcome, PathBuf) {
    let mut o = Outcome::new("s1-stale-checkpoint-new-target");
    let m = targets(lab, ctx, "s1");
    let (ta, tb) = (a_of(&m, ctx), b_of(&m, ctx));
    let f = files(lab, "s1", "stable");
    write_doc(ctx, &m, &f.checkpoint, &f.offsets, &f.doc);
    let e1 = start_engine(lab, "s1-e1", &f.doc);
    let exit1 = wait_engine(&e1, ENGINE_DEADLINE_SECS);
    let v1 = verify(ctx, &m);
    let segs = completed_segments(&checkpoint_json(&f.checkpoint));
    for t in [&ta, &tb] {
        let _ = kafka_topics(&[
            "--bootstrap-server",
            "kafka-broker-1:9094",
            "--delete",
            "--topic",
            t,
        ]);
    }
    let gone = wait_until(60, 500, || !topic_exists(&ta) && !topic_exists(&tb));
    for t in [&ta, &tb] {
        create_topic_for_fixed_timestamps(t, PARTS, &[("message.timestamp.type", "CreateTime")]);
    }
    let e2 = start_engine(lab, "s1-e2", &f.doc);
    let exit2 = wait_engine(&e2, ENGINE_DEADLINE_SECS);
    let log2 = logs(&e2);
    let after = settled(&[&ta, &tb]);
    let v2 = verify(ctx, &m);
    o.put("attempt1_exit", json!(exit1));
    o.put("verification_after_attempt1", v1.clone());
    o.put("checkpoint_segments", json!(segs.len()));
    o.put("targets_deleted", json!(gone));
    o.put("attempt2_exit", json!(exit2));
    o.put("attempt2_log", json!(interesting(&log2)));
    o.put("landed_after_attempt2", json!(after));
    o.put("verification_after_attempt2", v2.clone());
    o.expect(
        "the control: attempt 1 is exact",
        exact(&v1, "a") && exact(&v1, "b"),
    );
    o.expect(
        "the checkpoint lists every segment",
        segs.len() == 2 * SEGMENTS_PER_TOPIC,
    );
    o.expect("attempt 2 exits 0", exit2 == Some(0));
    o.expect(
        "attempt 2 trusts the stale checkpoint",
        log2.contains(&format!(
            "Loaded checkpoint: {} segments completed",
            2 * SEGMENTS_PER_TOPIC
        )),
    );
    o.expect(
        "the recreated target is left empty: every record missing",
        count(&v2, "a", "missing") == PER_TOPIC && count(&v2, "b", "missing") == PER_TOPIC,
    );
    (o, f.checkpoint)
}

/// C1 — a truncated checkpoint file (what a kill during the engine's
/// non-atomic save leaves).
fn c1(lab: &mut Lab, ctx: &Ctx, valid: &Path) -> Outcome {
    let mut o = Outcome::new("c1-corrupt-checkpoint");
    let m = targets(lab, ctx, "c1");
    let (ta, tb) = (a_of(&m, ctx), b_of(&m, ctx));
    let f = files(lab, "c1", "stable");
    write_doc(ctx, &m, &f.checkpoint, &f.offsets, &f.doc);
    let bytes = std::fs::read(valid).unwrap_or_default();
    std::fs::write(&f.checkpoint, &bytes[..bytes.len() / 2]).expect("write the truncated file");
    let e1 = start_engine(lab, "c1-e1", &f.doc);
    let exit1 = wait_engine(&e1, ENGINE_DEADLINE_SECS);
    let log1 = logs(&e1);
    let landed_total = landed(&ta) + landed(&tb);
    o.put("truncated_bytes", json!(bytes.len() / 2));
    o.put("attempt1_exit", json!(exit1));
    o.put(
        "attempt1_log_tail",
        json!(log1.lines().rev().take(6).collect::<Vec<_>>()),
    );
    o.put("landed", json!(landed_total));
    o.expect("the source checkpoint was non-empty", !bytes.is_empty());
    o.expect(
        "the engine refuses to start: non-zero exit",
        matches!(exit1, Some(c) if c != 0),
    );
    o.expect("nothing was produced", landed_total == 0);
    o
}

/// W1 — two engines with the same document and checkpoint path, started
/// together, into the same empty targets.
fn w1(lab: &mut Lab, ctx: &Ctx) -> Outcome {
    let mut o = Outcome::new("w1-two-workers");
    let m = targets(lab, ctx, "w1");
    let f = files(lab, "w1", "stable");
    write_doc(ctx, &m, &f.checkpoint, &f.offsets, &f.doc);
    let e1 = start_engine(lab, "w1-e1", &f.doc);
    let e2 = start_engine(lab, "w1-e2", &f.doc);
    let exit1 = wait_engine(&e1, ENGINE_DEADLINE_SECS);
    let exit2 = wait_engine(&e2, ENGINE_DEADLINE_SECS);
    let v = verify(ctx, &m);
    o.put("exits", json!([exit1, exit2]));
    o.put("verification", v.clone());
    o.expect("both workers exit 0", exit1 == Some(0) && exit2 == Some(0));
    o.expect(
        "every record is restored twice: duplicates = the whole archive",
        count(&v, "a", "duplicates") == PER_TOPIC && count(&v, "b", "duplicates") == PER_TOPIC,
    );
    o.expect(
        "nothing missing",
        count(&v, "a", "missing") == 0 && count(&v, "b", "missing") == 0,
    );
    o
}

/// M1 — a foreign producer writes into a partly restored target; the tail
/// resume must refuse it, and a re-run leaves it behind as unexpected.
fn m1(lab: &mut Lab, ctx: &Ctx) -> Outcome {
    let mut o = Outcome::new("m1-target-mutation");
    let m = targets(lab, ctx, "m1");
    let (ta, tb) = (a_of(&m, ctx), b_of(&m, ctx));
    let f = files(lab, "m1", "stable");
    write_doc(ctx, &m, &f.checkpoint, &f.offsets, &f.doc);
    let e1 = start_engine(lab, "m1-e1", &f.doc);
    let reached = wait_until(180, 50, || landed(&ta) >= 1_500 || !running(&e1));
    signal(&e1, "KILL");
    let _ = wait_engine(&e1, 60);
    let after = settled(&[&ta, &tb]);
    let landed_a: i64 = after[&ta].values().sum();
    let foreign: Vec<Out> = (0..5)
        .map(|i| {
            Out::kv(
                0,
                Some(T + i),
                &format!("foreign-{i}"),
                "not from the archive",
            )
        })
        .collect();
    kafka::produce_plain(&ta, &foreign).expect("the foreign write");
    let resumed = resume_prototype(ctx, &m);
    o.put("kill_reached_mid_topic", json!(reached));
    o.put("landed_after_kill", json!(after));
    o.put(
        "resume_prototype",
        match &resumed {
            Ok(v) => json!({"resumed": v}),
            Err(e) => json!({"refused": e}),
        },
    );
    let e2 = start_engine(lab, "m1-e2", &f.doc);
    let exit2 = wait_engine(&e2, ENGINE_DEADLINE_SECS);
    let v = verify(ctx, &m);
    o.put("rerun_exit", json!(exit2));
    o.put("verification_after_rerun", v.clone());
    o.expect(
        "the tail resume refuses a target holding a record it did not write",
        matches!(&resumed, Err(e) if e.contains("carries no x-original-offset")),
    );
    o.expect("the re-run exits 0", exit2 == Some(0));
    o.expect(
        "the foreign records are unexpected under complete verification",
        count(&v, "a", "unexpected") == 5,
    );
    o.expect(
        "topic a: duplicates = attempt 1's records",
        count(&v, "a", "duplicates") == landed_a,
    );
    o
}

#[test]
#[ignore = "PROD-07.1 research rows: freezes the slot's broker and runs ~20 engine containers"]
fn the_pinned_engines_restore_checkpoint_under_interruption() {
    let rows: Option<BTreeSet<String>> = std::env::var("LOGWEIR_PROD071_ROWS")
        .ok()
        .map(|v| v.split(',').map(|s| s.trim().to_string()).collect());
    let want = |r: &str| rows.as_ref().is_none_or(|s| s.contains(r));
    let engine = engine_version();
    eprintln!("[resume71] engine {engine} (contract {CONTRACT_ENGINE})");
    let mut lab = Lab::new();
    let started = Instant::now();
    let ctx = setup(&mut lab);
    eprintln!(
        "[resume71] archive {} ready in {:?}",
        ctx.backup_id,
        started.elapsed()
    );
    let mut outcomes: Vec<Outcome> = Vec::new();
    type Row = fn(&mut Lab, &Ctx) -> Outcome;
    for (key, row) in [
        ("k1", k1 as Row),
        ("k2", k2 as Row),
        ("h1", h1 as Row),
        ("t1", t1 as Row),
        ("k3", k3 as Row),
        ("w1", w1 as Row),
        ("m1", m1 as Row),
    ] {
        if want(key) {
            let t0 = Instant::now();
            let mut out = row(&mut lab, &ctx);
            out.put("seconds", json!(t0.elapsed().as_secs()));
            out.put("engine", json!(engine));
            out.save();
            outcomes.push(out);
        }
    }
    if want("s1") || want("c1") {
        let t0 = Instant::now();
        let (mut out, valid) = s1(&mut lab, &ctx);
        out.put("seconds", json!(t0.elapsed().as_secs()));
        out.put("engine", json!(engine));
        out.save();
        outcomes.push(out);
        if want("c1") {
            let mut out = c1(&mut lab, &ctx, &valid);
            out.put("engine", json!(engine));
            out.save();
            outcomes.push(out);
        }
    }
    let failures: Vec<String> = outcomes.iter().flat_map(|o| o.failures.clone()).collect();
    eprintln!(
        "[resume71] {} rows in {:?}; failed predictions: {failures:?}",
        outcomes.len(),
        started.elapsed()
    );
    if engine == CONTRACT_ENGINE {
        assert!(
            failures.is_empty(),
            "predictions that did not hold: {failures:#?}"
        );
    }
}
