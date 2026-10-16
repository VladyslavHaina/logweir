#![cfg(feature = "e2e")]
//! **PROD-07.2, live on the CLI path: an honest interrupted restore**
//! (OD-23-core §3.4, rows IR-1 to IR-7; IR-8 and IR-9 are the controller's
//! and the API's rows in `crates/weirkeeper/tests/restore_interruption.rs`
//! and `crates/logweir-api/tests/restore_interruption.rs`; IR-3 is
//! `stopped_restore.rs`, kept green).
//!
//! One archive of two topics of three partitions: `a`, large and first in
//! manifest order, so a stop lands inside it; `b`, small. Every restore is
//! `newTopic` under its own prefix. In order:
//!
//! | Row | What | Pass predicate |
//! |---|---|---|
//! | IR-2 | SIGTERM to `logweir restore run` inside `a` (a cancel) | exit 1, `targets-record-key=` and `failure-reason=Cancelled` last, no engine container of the slot left, the targets' end offsets stop moving, `a` partly restored |
//! | sec | the targets record of that run edited (one name changed), then without its sidecar | `restore reconcile` refuses both, exit 3, signs nothing |
//! | IR-1 | `restore reconcile` over the genuine record | exit 2, `fail-integrity`, per partition `restored` = the target's end offset and `expected` = the archive's count, `target.creation` the two targets, the note names the run; both readers verify; nothing written to the target |
//! | IR-7 | `b`'s target deleted and created again, then `restore cleanup` over the reconcile's scorecard | `a`'s target deleted, `b`'s left and named `CloneTargetReplaced` |
//! | IR-5 | the same plan again, then a fresh prefix | the old names: exit 3 at phase 0; the fresh prefix: exit 0 |
//! | IR-6 | SIGTERM after the engine finished (during phase 7), then `restore reconcile` | the run exits 1 Cancelled with everything restored; the reconcile exits 0 `pass`, and the target is unchanged |
//! | IR-1b | SIGKILL to `logweir` inside `a` and its engine container killed (a deleted pod) | the reconcile over its record: exit 2 with the counts |
//! | IR-4 | the rendered `restore.yaml` of every run | neither `checkpoint_state` nor `checkpoint_interval_secs` (B1's render half; collected, asserted last) |
//!
//! # Running it
//!
//! ```text
//! eval "$(e2e/compose/stack-env.sh --slot 4 --kafka 4.3)"
//! just e2e-up && ./scripts/e2e-seed.sh
//! cargo build -p logweir
//! LOGWEIR_PYTHON=<python3 with cryptography> AWS_EC2_METADATA_DISABLED=true \
//!   cargo test -p e2e --features e2e --test interrupted_restore -- --ignored --nocapture
//! just e2e-down
//! ```
//!
//! What each row observed goes to `<demo_dir>/interrupted-restore/*.json`.
mod harness;
#[allow(dead_code)]
mod record_semantics_support;

use harness::*;
use record_semantics_support::kafka::{self, Out};
use serde_json::{json, Value};
use std::process::{Command, Output};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const PARTS: i32 = 3;
const A_PER_PARTITION: i64 = 200_000;
const B_PER_PARTITION: i64 = 1_000;
const T: i64 = 1_760_000_000_000;
const ID_PREFIX: &str = "ir-";

fn rfc3339(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .expect("a representable instant")
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn quiet(program: &str, args: &[&str], secs: u64) -> String {
    let mut c = Command::new(program);
    c.args(args);
    kafka::output_within(c, secs)
        .map(|o| o.stdout_utf8())
        .unwrap_or_default()
}

fn own_engine_containers() -> Vec<String> {
    let volume = format!("volume={}", engine_mount().display());
    quiet("docker", &["ps", "-q", "--filter", &volume], 30)
        .split_whitespace()
        .map(str::to_string)
        .collect()
}

fn landed(topic: &str) -> i64 {
    kafka::high_watermarks(topic, PARTS)
        .map(|w| w.iter().map(|(_, h)| h).sum())
        .unwrap_or(0)
}

fn offsets(topic: &str) -> Vec<(i32, i64)> {
    kafka::high_watermarks(topic, PARTS).unwrap_or_default()
}

struct Lab {
    nonce: String,
    topics: Vec<String>,
    archives: Vec<String>,
}

impl Lab {
    fn new() -> Lab {
        kafka::use_stack_s3_env();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Lab {
            nonce: format!("{:010}", nanos % 10_000_000_000),
            topics: Vec::new(),
            archives: Vec::new(),
        }
    }
    fn name(&self, suffix: &str) -> String {
        format!("{ID_PREFIX}{}-{suffix}", self.nonce)
    }
    fn source_topic(&mut self, suffix: &str) -> String {
        let t = self.name(suffix);
        self.topics.push(t.clone());
        create_topic_for_fixed_timestamps(&t, PARTS, &[("message.timestamp.type", "CreateTime")]);
        t
    }
    fn targets(&mut self, prefix: &str, sources: &[String]) -> Vec<String> {
        let t: Vec<String> = sources.iter().map(|s| format!("{prefix}{s}")).collect();
        self.topics.extend(t.iter().cloned());
        t
    }
}

impl Drop for Lab {
    fn drop(&mut self) {
        for id in own_engine_containers() {
            let _ = quiet("docker", &["kill", &id], 60);
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
            for p in [format!("{b}/"), format!("logweir/backups/{b}/")] {
                let _ = mc(&[
                    "rm",
                    "--recursive",
                    "--force",
                    &format!("local/{ARCHIVE_BUCKET}/{p}"),
                ]);
            }
        }
    }
}

fn fixture(tag: &str, n: i64) -> Vec<Out> {
    let pad = "z".repeat(200);
    (0..PARTS)
        .flat_map(|p| {
            let pad = pad.clone();
            let tag = tag.to_string();
            (0..n).map(move |i| {
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

fn spec(backup_id: &str, sources: &[String], prefix: &str, coverage: &str) -> serde_yaml::Value {
    let (boot, endpoint) = (kafka::bootstrap(), kafka::s3_endpoint());
    serde_yaml::from_str(&format!(
        "source:\n\
         \x20 storage:\n\
         \x20   backend: s3\n\
         \x20   bucket: {ARCHIVE_BUCKET}\n\
         \x20   prefix: {backup_id}\n\
         \x20   region: us-east-1\n\
         \x20   endpoint: {endpoint}\n\
         \x20   path_style: true\n\
         \x20   allow_http: true\n\
         \x20 backup: {backup_id}\n\
         \x20 topics: [{}]\n\
         target:\n\
         \x20 bootstrap_servers: [{boot}]\n\
         \x20 mode: newTopic\n\
         \x20 topic_mapping_prefix: \"drill-\"\n\
         \x20 topic_naming:\n\
         \x20   prefix: \"{prefix}\"\n\
         \x20 default_replication_factor: 1\n\
         sample:\n\
         \x20 window_start: \"{}\"\n\
         \x20 window_end: \"{}\"\n\
         \x20 records_per_partition: 25\n\
         \x20 anchor: head\n\
         \x20 coverage: {coverage}\n\
         objectives: {{}}\n\
         evidence:\n\
         \x20 backend: s3\n\
         \x20 bucket: {EVIDENCE_BUCKET}\n\
         \x20 prefix: logweir/\n\
         \x20 region: us-east-1\n\
         \x20 endpoint: {endpoint}\n\
         \x20 path_style: true\n\
         \x20 allow_http: true\n",
        sources.join(", "),
        rfc3339(T - 1000),
        rfc3339(T + 10_000_000),
    ))
    .expect("the restore spec is valid YAML")
}

/// The command a row runs `logweir` with, in the environment `run_with`
/// gives its child.
fn logweir(args: &[String]) -> Output {
    let mut c = Command::new(bin());
    c.args(args)
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .env("AWS_REGION", "us-east-1")
        .env("LOGWEIR_ENGINE_BIN", engine_bin())
        .env("LOGWEIR_ENGINE_VERSION", engine_version())
        .env("LOGWEIR_ENGINE_DIGEST", engine_digest())
        .env("TMPDIR", engine_mount())
        .env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount());
    kafka::output_within(c, 1200).expect("logweir answered within 20 minutes")
}

/// What one interrupted run left: its result, its argv, and its run id.
struct Interrupted {
    run: Run,
    argv: Vec<String>,
    run_id: String,
    /// The `logweir` process id, which names its work directory
    /// (`<TMPDIR>/logweir-<pid>`, where it renders `restore.yaml`).
    pid: String,
    landed_at_signal: Option<i64>,
}

/// One `restore run` of `spec`, signalled with `signal` once `until`
/// answers true for what `first` holds.
fn interrupt(
    spec: serde_yaml::Value,
    first: &str,
    signal: &str,
    kill_engine: bool,
    until: impl Fn(i64) -> bool,
) -> Interrupted {
    let handle = std::thread::spawn(move || {
        let mut o = RunOpts::new(&spec);
        o.restore_run = true;
        run_with(o)
    });
    let pattern = format!("restore run --spec {}/drill-", demo_dir().display());
    let deadline = Instant::now() + Duration::from_secs(600);
    let mut pid = None;
    while pid.is_none() && Instant::now() < deadline && !handle.is_finished() {
        pid = quiet("pgrep", &["-f", &pattern], 20)
            .split_whitespace()
            .next()
            .map(str::to_string);
        if pid.is_none() {
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    let pid = pid.expect("the logweir restore process");
    let argv: Vec<String> = quiet("ps", &["-o", "args=", "-p", &pid], 20)
        .split_whitespace()
        .skip(1)
        .map(str::to_string)
        .collect();
    let mut at = None;
    let deadline = Instant::now() + Duration::from_secs(900);
    while Instant::now() < deadline && !handle.is_finished() {
        if topic_exists(first) {
            let n = landed(first);
            if until(n) {
                at = Some(n);
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    if at.is_some() {
        quiet("kill", &[&format!("-{signal}"), &pid], 20);
        if kill_engine {
            for id in own_engine_containers() {
                quiet("docker", &["kill", &id], 60);
            }
        }
    }
    let deadline = Instant::now() + Duration::from_secs(900);
    while !handle.is_finished() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(handle.is_finished(), "the restore did not end");
    let run = handle.join().expect("the restore thread");
    let run_id = run.run_id();
    Interrupted {
        run,
        argv,
        run_id,
        pid,
        landed_at_signal: at,
    }
}

/// `restore reconcile` with the interrupted run's own flags and its id.
fn reconcile(i: &Interrupted) -> (Output, Value) {
    let mut args = i.argv.clone();
    assert_eq!(args[..2], ["restore".to_string(), "run".to_string()]);
    args[1] = "reconcile".to_string();
    args.extend(["--run-id".to_string(), i.run_id.clone()]);
    let out_path = demo_dir().join("scorecard.json");
    let _ = std::fs::remove_file(&out_path);
    let out = logweir(&args);
    let sc = std::fs::read(&out_path)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or(Value::Null);
    (out, sc)
}

fn flag(argv: &[String], name: &str) -> String {
    let i = argv.iter().position(|a| a == name).expect(name);
    argv[i + 1].clone()
}

fn last_line(o: &Output) -> String {
    o.stdout_utf8()
        .lines()
        .rfind(|l| !l.trim().is_empty())
        .unwrap_or_default()
        .to_string()
}

fn evidence_store() -> logweir_engine_oso::storage::Store {
    let endpoint = kafka::s3_endpoint();
    let url: logweir_core::engine::StorageUrl = serde_yaml::from_str(&format!(
        "backend: s3\nbucket: {EVIDENCE_BUCKET}\nprefix: logweir/\nregion: us-east-1\n\
         endpoint: {endpoint}\npath_style: true\nallow_http: true\n"
    ))
    .unwrap();
    logweir_engine_oso::storage::Store::from_url(&url).expect("the evidence store")
}

fn rm_evidence(key: &str) {
    let o = mc(&["rm", "--force", &format!("local/{EVIDENCE_BUCKET}/{key}")]);
    assert_eq!(o.status.code(), Some(0), "mc rm {key}: {}", o.stderr_utf8());
}

/// IR-4's check over a run's rendered documents, in its work directory.
fn rendered_checkpoint(i: &Interrupted) -> Vec<String> {
    let dir = engine_mount().join(format!("logweir-{}", i.pid));
    assert!(
        dir.join("restore.yaml").exists(),
        "IR-4 reads {}: the run rendered no restore.yaml there",
        dir.display()
    );
    let mut found = Vec::new();
    for e in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if name.starts_with("restore") && name.ends_with(".yaml") {
            let text = std::fs::read_to_string(e.path()).unwrap_or_default();
            for key in ["checkpoint_state", "checkpoint_interval_secs"] {
                if text.contains(key) {
                    found.push(format!("{name}: {key}"));
                }
            }
        }
    }
    found
}

#[test]
#[ignore = "signals logweir and its engine container mid-restore; run alone with --ignored"]
fn an_interrupted_restore_is_honest_live() {
    let out_dir = demo_dir().join("interrupted-restore");
    let mut lab = Lab::new();
    let a = lab.source_topic("a");
    let b = lab.source_topic("b");
    kafka::produce_plain(&a, &fixture("a", A_PER_PARTITION)).expect("produce a");
    kafka::produce_plain(&b, &fixture("b", B_PER_PARTITION)).expect("produce b");
    let backup_id = lab.name("arch");
    lab.archives.push(backup_id.clone());
    let o = kafka::backup_run(&backup_id, &[&a, &b], A_PER_PARTITION as u64);
    assert_eq!(o.status.code(), Some(0), "backup: {}", o.stderr_utf8());
    let sources = vec![a.clone(), b.clone()];
    let a_total = A_PER_PARTITION * i64::from(PARTS);
    let b_total = B_PER_PARTITION * i64::from(PARTS);
    let mut late = Vec::new();

    // IR-2: a cancel inside `a`.
    let p1 = lab.name("c-");
    let t1 = lab.targets(&p1, &sources);
    let cancelled = interrupt(
        spec(&backup_id, &sources, &p1, "sampled"),
        &t1[0],
        "TERM",
        false,
        |n| n > 0 && n < a_total,
    );
    let stopped_at = Instant::now();
    let left_after_grace = own_engine_containers();
    let o1 = offsets(&t1[0]);
    std::thread::sleep(Duration::from_secs(5));
    let o2 = offsets(&t1[0]);
    let stdout = cancelled.run.out.stdout_utf8();
    kafka::write_json(
        &out_dir.join("ir2-cancel.json"),
        &json!({"exit": cancelled.run.out.status.code(), "run_id": cancelled.run_id,
                "landed_at_signal": cancelled.landed_at_signal, "offsets": [o1, o2],
                "engines_left": left_after_grace, "stdout_tail": stdout.lines().rev().take(8).collect::<Vec<_>>(),
                "stop_to_exit_ms": stopped_at.elapsed().as_millis()}),
    );
    assert!(
        cancelled.landed_at_signal.is_some(),
        "the restore finished before the cancel"
    );
    assert_eq!(cancelled.run.out.status.code(), Some(1), "{stdout}");
    assert_eq!(
        last_line(&cancelled.run.out),
        "failure-reason=Cancelled",
        "{stdout}"
    );
    assert!(
        stdout.contains("targets-record-key=logweir/drills/"),
        "{stdout}"
    );
    assert!(
        !std::path::Path::new(&cancelled.run.scorecard).exists(),
        "nothing signed"
    );
    assert!(
        left_after_grace.is_empty(),
        "an engine container outlived the cancel"
    );
    assert_eq!(o1, o2, "the target stopped growing");
    assert!(landed(&t1[0]) < a_total, "part of `a` only");
    late.extend(rendered_checkpoint(&cancelled));

    // The security controls: an edited record, then one without its sidecar.
    let store = evidence_store();
    let record_key = format!("logweir/drills/{}.targets.json", cancelled.run_id);
    let sidecar_key = format!("logweir/drills/{}.targets.sig", cancelled.run_id);
    let (genuine, _) = store
        .get_capped(&record_key, 1 << 20)
        .expect("the targets record");
    let (sig, _) = store
        .get_capped(&sidecar_key, 1 << 20)
        .expect("its signature");
    let mut edited: Value = serde_json::from_slice(&genuine).unwrap();
    edited["topics"][0]["name"] = json!(format!("{p1}someone-elses"));
    rm_evidence(&record_key);
    store
        .put_create_only(&record_key, &serde_json::to_vec(&edited).unwrap())
        .unwrap();
    let (refused_edit, sc) = reconcile(&cancelled);
    assert_eq!(
        refused_edit.status.code(),
        Some(3),
        "{}",
        refused_edit.stderr_utf8()
    );
    assert!(sc.is_null(), "an edited record signs nothing");
    rm_evidence(&record_key);
    store.put_create_only(&record_key, &genuine).unwrap();
    rm_evidence(&sidecar_key);
    let (refused_unsigned, sc) = reconcile(&cancelled);
    assert_eq!(
        refused_unsigned.status.code(),
        Some(3),
        "{}",
        refused_unsigned.stderr_utf8()
    );
    assert!(sc.is_null(), "an unsigned record signs nothing");
    store.put_create_only(&sidecar_key, &sig).unwrap();

    // IR-1: the reconcile over the genuine record.
    let before = (offsets(&t1[0]), offsets(&t1[1]));
    let (rec, sc) = reconcile(&cancelled);
    let after = (offsets(&t1[0]), offsets(&t1[1]));
    kafka::write_json(
        &out_dir.join("ir1-reconcile.json"),
        &json!({"exit": rec.status.code(), "scorecard": sc, "offsets": [before, after],
                "stdout_tail": rec.stdout_utf8().lines().rev().take(8).collect::<Vec<_>>()}),
    );
    assert_eq!(
        rec.status.code(),
        Some(2),
        "{}\n{}",
        rec.stdout_utf8(),
        rec.stderr_utf8()
    );
    assert_eq!(sc["outcome"], "fail-integrity");
    assert_eq!(before, after, "the reconcile wrote nothing to the target");
    assert!(sc["sample"]["coverage_note"]
        .as_str()
        .is_some_and(|n| n.starts_with(&format!(
            "reconciliation of interrupted run {}",
            cancelled.run_id
        ))));
    let created: Vec<String> = sc["target"]["creation"]["topics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(created.len(), 2, "{created:?}");
    let parts = sc["integrity"]["verification"]["complete"]["partitions"]
        .as_array()
        .expect("complete per-partition counts");
    for p in parts {
        let target = p["target_topic"].as_str().unwrap();
        let partition = p["partition"].as_i64().unwrap() as i32;
        let end = offsets(target)
            .iter()
            .find(|(q, _)| *q == partition)
            .map(|(_, h)| *h)
            .unwrap();
        let want = if target == t1[0] {
            A_PER_PARTITION
        } else {
            B_PER_PARTITION
        };
        assert_eq!(p["replay"]["restored"].as_i64(), Some(end), "{p}");
        assert_eq!(p["replay"]["expected"].as_i64(), Some(want), "{p}");
        assert_eq!(
            p["replay"]["missing"].as_i64(),
            Some(want - p["replay"]["matching"].as_i64().unwrap()),
            "{p}"
        );
    }
    let rec_run_id = sc["run_id"].as_str().unwrap().to_string();
    let as_run = Run {
        out: rec.clone(),
        scorecard: demo_dir().join("scorecard.json"),
        sig: demo_dir().join("scorecard.sig"),
        pubkey: demo_dir().join("signer.pub.pem"),
    };
    assert_eq!(logweir_verify(&as_run).code(), Some(0), "drill verify");
    assert_eq!(
        python_verify(&as_run).code(),
        Some(0),
        "verify_scorecard.py"
    );

    // IR-7: `b`'s target recreated by someone else, then the cleanup.
    kafka_topics(&[
        "--bootstrap-server",
        "kafka-broker-1:9094",
        "--delete",
        "--topic",
        &t1[1],
    ]);
    for _ in 0..60 {
        if !topic_exists(&t1[1]) {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    create_topic_for_fixed_timestamps(&t1[1], PARTS, &[]);
    let spec_path = flag(&cancelled.argv, "--spec");
    let key_path = flag(&cancelled.argv, "--signing-key");
    let cleanup = logweir(&[
        "restore".into(),
        "cleanup".into(),
        "--spec".into(),
        spec_path,
        "--run-id".into(),
        rec_run_id.clone(),
        "--signing-key".into(),
        key_path,
    ]);
    let line = cleanup
        .stdout_utf8()
        .lines()
        .find_map(|l| l.strip_prefix("clone-cleanup=").map(str::to_string))
        .unwrap_or_default();
    kafka::write_json(
        &out_dir.join("ir7-cleanup.json"),
        &json!({"exit": cleanup.status.code(), "line": line}),
    );
    assert_eq!(
        cleanup.status.code(),
        Some(0),
        "{}\n{}",
        cleanup.stdout_utf8(),
        cleanup.stderr_utf8()
    );
    let report: Value = serde_json::from_str(&line).expect("the clone-cleanup line");
    assert_eq!(report["deleted"], json!([t1[0]]), "{report}");
    assert!(
        report["left"]
            .as_array()
            .unwrap()
            .iter()
            .any(|l| l["topic"] == json!(t1[1])
                && l["reason"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("CloneTargetReplaced")),
        "{report}"
    );
    assert!(topic_exists(&t1[1]), "the recreated topic is left in place");

    // IR-5: the old names are refused at phase 0; a fresh prefix succeeds.
    let s1 = spec(&backup_id, &sources, &p1, "sampled");
    let mut o = RunOpts::new(&s1);
    o.restore_run = true;
    let again = run_with(o);
    assert_eq!(
        again.out.status.code(),
        Some(3),
        "{}",
        again.out.stderr_utf8()
    );
    let p2 = lab.name("r-");
    let _t2 = lab.targets(&p2, &sources);
    let s2 = spec(&backup_id, &sources, &p2, "sampled");
    let mut o = RunOpts::new(&s2);
    o.restore_run = true;
    let retry = run_with(o);
    assert_eq!(
        retry.out.status.code(),
        Some(0),
        "{}",
        retry.out.stderr_utf8()
    );

    // IR-6: stopped after the engine finished, during the verification.
    let p3 = lab.name("v-");
    let t3 = lab.targets(&p3, &sources);
    let t3b = t3[1].clone();
    let verifying = interrupt(
        spec(&backup_id, &sources, &p3, "complete"),
        &t3[0],
        "TERM",
        false,
        move |n| n == a_total && landed(&t3b) == b_total,
    );
    kafka::write_json(
        &out_dir.join("ir6-run.json"),
        &json!({"exit": verifying.run.out.status.code(), "run_id": verifying.run_id,
                "stdout_tail": verifying.run.out.stdout_utf8().lines().rev().take(8).collect::<Vec<_>>()}),
    );
    if verifying.run.out.status.code() == Some(0) {
        eprintln!("[ir6] the run signed before the stop landed; the row is inconclusive this time");
    } else {
        assert_eq!(verifying.run.out.status.code(), Some(1));
        assert_eq!(last_line(&verifying.run.out), "failure-reason=Cancelled");
        let before = (offsets(&t3[0]), offsets(&t3[1]));
        let (rec, sc) = reconcile(&verifying);
        kafka::write_json(
            &out_dir.join("ir6-reconcile.json"),
            &json!({"exit": rec.status.code(), "outcome": sc["outcome"]}),
        );
        assert_eq!(
            rec.status.code(),
            Some(0),
            "{}\n{}",
            rec.stdout_utf8(),
            rec.stderr_utf8()
        );
        assert_eq!(sc["outcome"], "pass");
        assert_eq!(
            before,
            (offsets(&t3[0]), offsets(&t3[1])),
            "no second restore"
        );
    }
    late.extend(rendered_checkpoint(&verifying));

    // IR-1b: the runner killed, and its engine with it (a deleted pod).
    let p4 = lab.name("k-");
    let t4 = lab.targets(&p4, &sources);
    let killed = interrupt(
        spec(&backup_id, &sources, &p4, "sampled"),
        &t4[0],
        "KILL",
        true,
        |n| n > 0 && n < a_total,
    );
    assert!(
        killed.landed_at_signal.is_some(),
        "the restore finished before the kill"
    );
    let (rec, sc) = reconcile(&killed);
    kafka::write_json(
        &out_dir.join("ir1b-reconcile.json"),
        &json!({"exit": rec.status.code(), "outcome": sc["outcome"]}),
    );
    assert_eq!(
        rec.status.code(),
        Some(2),
        "{}\n{}",
        rec.stdout_utf8(),
        rec.stderr_utf8()
    );
    assert_eq!(sc["outcome"], "fail-integrity");
    late.extend(rendered_checkpoint(&killed));

    assert!(
        own_engine_containers().is_empty(),
        "an engine container outlived its row"
    );
    // IR-4 last: the render half is B1's (`render_restore.rs`).
    assert!(
        late.is_empty(),
        "IR-4: a rendered restore document names the checkpoint: {late:?}"
    );
}
