#![cfg(feature = "e2e")]
//! **FX-23, live: a restore the engine stopped early is signed `fail`, never
//! `pass`.**
//!
//! A SIGTERM reaches the pinned engine between topics: it finishes the topic it
//! is on, writes its offset report from the `Ok` arm and exits 0, and the
//! topics after it in manifest order are never restored (PROD-07.1's record,
//! row T1). Logweir sees only the exit 0. This row makes that happen through
//! the SHIPPED `logweir restore run`, against the slot's broker and MinIO, with
//! the digest-pinned engine in its container (`e2e/fixtures/engine-docker.sh`,
//! where the engine is the container's PID 1 and has a signal handler): the
//! moment the first restored record of the FIRST topic lands, the row sends
//! the engine container a SIGTERM (`docker kill --signal TERM`).
//!
//! The archive is two topics of three partitions, `PER_PARTITION` records
//! each, one segment per partition, and the point in time three quarters into
//! every segment — so every segment STRADDLES it, the aggregate count bound's
//! `lower` is 0 and it can never fire (PROD-07.1 review H1, hole A). Two
//! restores, each stopped the same way:
//!
//! - **`max3`**: `sample.max_partitions: 3`. The old first-N truncation kept
//!   the three partitions of the first topic — exactly the topic that
//!   finished — and signed `pass`. Round-robin (FX-23 (b)) samples the second
//!   topic too.
//! - **`max1`**: `sample.max_partitions: 1`. The sample can reach one topic,
//!   so the scorecard names the other in `sample.unsampled_topics` and is
//!   format 1.6.0; the per-partition bound (a) and the engine's report (c)
//!   decide.
//!
//! Each must sign `fail-integrity` (exit 2), name every partition of the topic
//! the engine never started, carry the report's finding, and verify under both
//! readers. `LOGWEIR_FX23_RECORD_ONLY=1` records the verdict without asserting
//! it — how the row is run against a build from before FX-23, to show the hole
//! it closes — and still asserts that the SIGTERM landed where it must.
//!
//! # Running it
//!
//! `#[ignore]`d: it signals engine containers. On a PROD-01.5 slot:
//!
//! ```text
//! eval "$(e2e/compose/stack-env.sh --slot 1)"
//! cargo build -p logweir
//! AWS_EC2_METADATA_DISABLED=true cargo test -p e2e --features e2e \
//!     --test stopped_restore -- --ignored --test-threads=1 --nocapture
//! ```
//!
//! Each restore writes `<demo_dir>/stopped-restore/<variant>.json`.
mod harness;
#[allow(dead_code)]
mod record_semantics_support;

use harness::*;
use record_semantics_support::kafka::{self, Out};
use serde_json::{json, Value};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const PARTS: i32 = 3;
const PER_PARTITION: i64 = 20_000;
/// The fixture's CreateTime base, `2025-10-09T08:53:20Z` — the record-semantics
/// epoch, past the broker's default retention, so every topic this file
/// creates carries `retention.ms=-1`.
const T: i64 = 1_760_000_000_000;
/// Three quarters into every segment: each straddles it.
const PIT: i64 = T + PER_PARTITION * 3 / 4;
/// Records of each partition at or before the point in time.
const IN_WINDOW: i64 = PER_PARTITION * 3 / 4 + 1;
const ID_PREFIX: &str = "fx23-";
const RESTORE_DEADLINE_SECS: u64 = 900;

fn rfc3339(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .expect("a representable instant")
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn run_quiet(program: &str, args: &[&str], secs: u64) -> String {
    let mut c = std::process::Command::new(program);
    c.args(args);
    kafka::output_within(c, secs)
        .map(|o| o.stdout_utf8())
        .unwrap_or_default()
}

/// The engine containers this worktree's slot started: the ones bind-mounting
/// its `harness::engine_mount()`.
fn own_engine_containers() -> Vec<String> {
    let volume = format!("volume={}", engine_mount().display());
    run_quiet("docker", &["ps", "-q", "--filter", &volume], 30)
        .split_whitespace()
        .map(str::to_string)
        .collect()
}

fn landed(topic: &str) -> i64 {
    kafka::high_watermarks(topic, PARTS)
        .map(|w| w.iter().map(|(_, h)| h).sum())
        .unwrap_or(0)
}

/// Every topic and archive one run creates, released on every exit path.
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
            .expect("the clock is after 1970")
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
}

impl Drop for Lab {
    fn drop(&mut self) {
        for id in own_engine_containers() {
            let _ = run_quiet("docker", &["kill", &id], 60);
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

/// `n` records per partition, `T + i`, with a padded value so a restore of one
/// topic takes long enough to signal inside it.
fn fixture(tag: &str) -> Vec<Out> {
    let pad = "z".repeat(200);
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

/// A `newTopic` restore of both topics at `PIT`, sampled, under
/// `max_partitions`.
fn spec(
    backup_id: &str,
    sources: &[String],
    prefix: &str,
    max_partitions: u32,
) -> serde_yaml::Value {
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
         restore:\n\
         \x20 point_in_time: \"{}\"\n\
         sample:\n\
         \x20 window_start: \"{}\"\n\
         \x20 window_end: \"{}\"\n\
         \x20 records_per_partition: 25\n\
         \x20 anchor: head\n\
         \x20 max_partitions: {max_partitions}\n\
         objectives:\n\
         \x20 rto_seconds: 900\n\
         \x20 rpo_seconds: 300\n\
         \x20 pass_rate: 1.0\n\
         evidence:\n\
         \x20 backend: s3\n\
         \x20 bucket: {EVIDENCE_BUCKET}\n\
         \x20 prefix: logweir/\n\
         \x20 region: us-east-1\n\
         \x20 endpoint: {endpoint}\n\
         \x20 path_style: true\n\
         \x20 allow_http: true\n",
        sources.join(", "),
        rfc3339(PIT),
        rfc3339(T - 1000),
        rfc3339(PIT),
    ))
    .expect("the restore spec is valid YAML")
}

/// One restore, stopped by a SIGTERM to the engine the moment `first`'s target
/// holds a record and before it holds them all. Returns the run and what the
/// row observed around the signal.
fn stopped_restore(spec: serde_yaml::Value, first: &str, second: &str) -> (Run, Value) {
    let handle = std::thread::spawn(move || {
        let mut o = RunOpts::new(&spec);
        o.restore_run = true;
        run_with(o)
    });
    let first_total = IN_WINDOW * i64::from(PARTS);
    let deadline = Instant::now() + Duration::from_secs(600);
    let mut at_signal = None;
    let mut first_seen = false;
    while Instant::now() < deadline && !handle.is_finished() {
        if !first_seen {
            first_seen = topic_exists(first) && topic_exists(second);
        }
        if first_seen {
            let n = landed(first);
            if n > 0 && n < first_total {
                at_signal = Some(n);
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let containers = own_engine_containers();
    let mut signalled = Vec::new();
    if at_signal.is_some() {
        for id in &containers {
            let o = run_quiet("docker", &["kill", "--signal", "TERM", id], 60);
            signalled.push(json!({"container": id, "docker_kill_stdout": o.trim()}));
        }
    }
    let deadline = Instant::now() + Duration::from_secs(RESTORE_DEADLINE_SECS);
    while !handle.is_finished() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(200));
    }
    if !handle.is_finished() {
        for id in own_engine_containers() {
            run_quiet("docker", &["kill", &id], 60);
        }
        panic!("logweir restore run: no result after {RESTORE_DEADLINE_SECS} s; engine containers killed");
    }
    let run = handle.join().expect("the restore thread");
    let observed = json!({
        "first_topic_landed_when_signalled": at_signal,
        "engine_containers_signalled": signalled,
        "first_topic_landed_after": kafka::high_watermarks(first, PARTS).ok(),
        "second_topic_landed_after": kafka::high_watermarks(second, PARTS).ok(),
    });
    (run, observed)
}

/// Logweir's verdict as the row records it.
fn verdict(r: &Run) -> Value {
    let sc: Value = std::fs::read(&r.scorecard)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or(Value::Null);
    let tail = |s: String| -> Vec<String> {
        let lines: Vec<String> = s.lines().map(|l| l.chars().take(600).collect()).collect();
        lines[lines.len().saturating_sub(15)..].to_vec()
    };
    json!({
        "exit": r.out.status.code(),
        "format_version": sc["format_version"],
        "outcome": sc["outcome"],
        "integrity": sc["integrity"],
        "sample": sc["sample"],
        "offset_report_key": sc["evidence"]["offset_report_key"],
        "stdout_tail": tail(r.out.stdout_utf8()),
        "stderr_tail": tail(r.out.stderr_utf8()),
    })
}

#[test]
#[ignore = "signals the engine container mid-restore; run alone with --ignored"]
fn a_sigterm_stopped_restore_is_signed_fail_never_pass() {
    let record_only = std::env::var("LOGWEIR_FX23_RECORD_ONLY").is_ok_and(|v| v == "1");
    let mut lab = Lab::new();
    let a = lab.source_topic("a");
    let b = lab.source_topic("b");
    kafka::produce_plain(&a, &fixture("a")).expect("produce the first topic");
    kafka::produce_plain(&b, &fixture("b")).expect("produce the second topic");
    let backup_id = lab.name("arch");
    lab.archives.push(backup_id.clone());
    let o = kafka::backup_run(&backup_id, &[&a, &b], PER_PARTITION as u64);
    assert_eq!(
        o.status.code(),
        Some(0),
        "`logweir backup run` must exit 0\nstdout:\n{}\nstderr:\n{}",
        o.stdout_utf8(),
        o.stderr_utf8()
    );
    // The engine restores in MANIFEST order; "first" and "second" are that
    // order, read off the manifest the backup wrote.
    let archive = kafka::read_archive(&backup_id, &a).expect("the archive");
    let order: Vec<String> = archive.manifest["topics"]
        .as_array()
        .expect("topics")
        .iter()
        .filter_map(|t| t["name"].as_str().map(str::to_string))
        .collect();
    assert_eq!(order.len(), 2, "{order:?}");
    for t in [&a, &b] {
        let arc = kafka::read_archive(&backup_id, t).expect("archive");
        assert_eq!(arc.records.len() as i64, PER_PARTITION * i64::from(PARTS));
        assert_eq!(
            arc.segments.len(),
            PARTS as usize,
            "{t}: one segment per partition, so every segment straddles the point in time"
        );
        for s in &arc.segments {
            assert!(
                s.start_timestamp <= PIT && s.end_timestamp > PIT,
                "{t}: {s:?} must straddle the point in time"
            );
        }
    }

    let out_dir = demo_dir().join("stopped-restore");
    let mut failures = Vec::new();
    for (variant, max_partitions) in [("max3", 3u32), ("max1", 1u32)] {
        let prefix = lab.name(&format!("{variant}-"));
        let first = format!("{prefix}{}", order[0]);
        let second = format!("{prefix}{}", order[1]);
        lab.topics.push(first.clone());
        lab.topics.push(second.clone());
        let (run, observed) = stopped_restore(
            spec(&backup_id, &order, &prefix, max_partitions),
            &first,
            &second,
        );
        let v = verdict(&run);
        let rust_verify = std::path::Path::new(&run.scorecard)
            .exists()
            .then(|| logweir_verify(&run).code());
        let python_verify = std::path::Path::new(&run.scorecard)
            .exists()
            .then(|| python_verify(&run).code());
        let record = json!({
            "variant": variant,
            "max_partitions": max_partitions,
            "manifest_order": order,
            "targets": {"first": first, "second": second},
            "point_in_time_ms": PIT,
            "observed": observed,
            "logweir": v,
            "drill_verify_exit": rust_verify,
            "verify_scorecard_py_exit": python_verify,
            "record_only": record_only,
        });
        kafka::write_json(&out_dir.join(format!("{variant}.json")), &record);
        eprintln!(
            "[fx23] {variant}: exit {:?}, outcome {}, version {}, landed at signal {}, second \
             after {}",
            v["exit"],
            v["outcome"],
            v["format_version"],
            observed["first_topic_landed_when_signalled"],
            observed["second_topic_landed_after"]
        );

        // Wherever this runs, the SIGTERM must have landed inside the first
        // topic: the first finished, the second never started, and the engine
        // exited 0 (a signed scorecard exists only after phase 7, which a
        // non-zero engine exit never reaches).
        assert!(
            observed["first_topic_landed_when_signalled"].is_i64(),
            "{variant}: no record of the first topic landed before the restore finished; \
             nothing was signalled"
        );
        let second_after: i64 = kafka::high_watermarks(&second, PARTS)
            .expect("the second target's watermarks")
            .iter()
            .map(|(_, h)| h)
            .sum();
        assert_eq!(
            second_after, 0,
            "{variant}: the SIGTERM landed too late, the engine started the second topic: \
             {observed}"
        );
        let first_after: i64 = kafka::high_watermarks(&first, PARTS)
            .expect("the first target's watermarks")
            .iter()
            .map(|(_, h)| h)
            .sum();
        assert_eq!(
            first_after,
            IN_WINDOW * i64::from(PARTS),
            "{variant}: the engine finishes the topic it is on: {observed}"
        );
        assert!(
            v["outcome"].is_string(),
            "{variant}: no scorecard, so the engine did not exit 0 or Logweir did not reach \
             phase 8: {v}"
        );
        if record_only {
            continue;
        }

        let reason = v["integrity"]["partial_reason"]
            .as_str()
            .unwrap_or_default();
        let mut check = |ok: bool, what: String| {
            if !ok {
                failures.push(format!("{variant}: {what}"));
            }
        };
        check(
            v["exit"] == json!(2),
            format!("exit {} is not 2", v["exit"]),
        );
        check(
            v["outcome"] == json!("fail-integrity"),
            format!("outcome {} is not fail-integrity", v["outcome"]),
        );
        check(
            v["integrity"]["result"] == json!("fail"),
            format!("integrity.result {}", v["integrity"]["result"]),
        );
        for p in 0..PARTS {
            let want = format!(
                "{}/{p}: {second}/{p} holds no record but the manifest proves at least 1",
                order[1]
            );
            check(
                reason.contains(&want),
                format!("the reason lacks `{want}`: {reason}"),
            );
        }
        check(
            reason.contains(&format!(
                "the engine's offset report has no entry for {PARTS} mapped partition(s)"
            )),
            format!("the reason lacks the engine report's finding: {reason}"),
        );
        check(
            rust_verify == Some(Some(0)),
            format!("drill verify exited {rust_verify:?}"),
        );
        check(
            python_verify == Some(Some(0)),
            format!("verify_scorecard.py exited {python_verify:?}"),
        );
        match variant {
            "max3" => {
                // (b): round-robin reached the second topic.
                check(
                    v["sample"]["topics"] == json!(2) && v["sample"]["partitions"] == json!(3),
                    format!(
                        "the sample is not two topics and three partitions: {}",
                        v["sample"]
                    ),
                );
                check(
                    v["sample"].get("unsampled_topics").is_none(),
                    format!("no topic is unsampled at max 3: {}", v["sample"]),
                );
            }
            _ => {
                check(
                    v["format_version"] == json!("1.6.0"),
                    format!("format_version {}", v["format_version"]),
                );
                check(
                    v["sample"]["unsampled_topics"] == json!([order[1]]),
                    format!("unsampled_topics {}", v["sample"]["unsampled_topics"]),
                );
            }
        }
    }
    assert!(
        own_engine_containers().is_empty(),
        "an engine container of this slot outlived its restore"
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
