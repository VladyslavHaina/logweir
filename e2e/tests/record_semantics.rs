#![cfg(feature = "e2e")]
//! **PROD-01.1 — record and transaction behaviour, observed.**
//!
//! Every row here produces a deterministic fixture into its own source topic,
//! takes a backup with the SHIPPED `logweir backup run` and the digest-pinned
//! engine, restores it with `logweir restore run` (`target.mode: newTopic`),
//! and then compares three readings record by record through
//! `record_semantics_support::oracle::compare`:
//!
//! | reading | how it is taken |
//! |---|---|
//! | the committed input | the source topic, `isolation.level=read_committed` |
//! | the raw log | the source topic, `read_uncommitted` (classifies extras only) |
//! | the archive | the manifest and every segment, decoded by Logweir's own `kbak` decoder |
//! | the output | the restored topic |
//!
//! Each divergence is attributed to CAPTURE (raw log -> archive), REPLAY
//! (archive -> output) or END TO END (committed input -> output). Logweir's
//! own signed verdict for the same restore is recorded beside them, never
//! used as the outcome: the question this file answers is what happened to
//! the records, and the engine's or the drill's exit status is not that.
//!
//! # What the assertions are
//!
//! The expected divergence sets below are PROD-01.1's capability contract for
//! engine 0.21.0 (`docs/to-do/decisions/PROD-01.1-record-semantics.md`): the
//! known counterexamples, stated exactly. They are not desired behaviour.
//! When the engine changes — READ_COMMITTED capture through PROD-00.3, say —
//! the row goes red and the contract and this file change together.
//!
//! Logweir's own verdict (exit code and signed outcome) is asserted as part of
//! the same contract (`assert_verdict`), because the decision record's FX-6
//! sentences quote it. On an engine other than `CONTRACT_ENGINE` a row records
//! its outcome and asserts nothing (`contract_applies`), so `engine-matrix`
//! runs of other releases produce outcome files, not red cells. Every
//! `logweir restore run` has a deadline (`run_restore_within`).
//!
//! Every row also proves its own check can fail: after the real comparison
//! matches the contract, the observed output is mutated (one record dropped,
//! one duplicated) and the same check is required to reject it
//! (`mutants_are_caught`). The comparator's own negative controls are in
//! `e2e/tests/record_semantics_oracle.rs`, in the default test set.
//!
//! # Running it
//!
//! Like every file here it needs the compose stack (`just e2e-up`) and runs
//! under `just e2e`. One row alone:
//!
//! ```text
//! cargo build -p logweir
//! AWS_EC2_METADATA_DISABLED=true cargo test -p e2e --features e2e \
//!     --test record_semantics -- --test-threads=1 --nocapture <row>
//! ```
//!
//! `a_lost_produce_acknowledgement_…` is `#[ignore]`d: it freezes the shared
//! broker for longer than the engine's 60 s response timeout. Run it alone
//! with `--ignored`.
//!
//! Each row writes its outcome to `.e2e/record-semantics/<row>.json`
//! (`.e2e/<project>/record-semantics/` on a PROD-01.5 slot: `demo_dir()`).
mod harness;
mod record_semantics_support;

use harness::*;
use record_semantics_support::kafka::{self, Archive, Isolation, Out, Txn};
use record_semantics_support::oracle::*;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The fixture epoch: `2025-10-09T08:53:20Z`, the same instant G-PITR uses
/// (`e2e/tests/pitr_boundary.rs`). A literal, never a clock read.
const T: i64 = 1_760_000_000_000;

/// A CreateTime far from any append time: `2001-09-09T01:46:40Z`.
const C0: i64 = 1_000_000_000_000;

const PARTS: i32 = 3;

/// Every topic, archive and target this file creates starts with this, and
/// nothing else in the tree does, so the sweeps below are exact.
const ID_PREFIX: &str = "recsem-";

fn rfc3339(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .expect("a representable instant")
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

// ===================================================================== rows

/// One row's resources, released on EVERY exit path (a panicking assertion
/// included): its topics are deleted and its archives swept from the shared
/// bucket, so a red row cannot poison the next.
struct Row {
    name: &'static str,
    nonce: String,
    topics: Vec<String>,
    archives: Vec<String>,
}

impl Row {
    fn new(name: &'static str) -> Row {
        // `Store::read_only_from_url` reads MinIO through
        // `AmazonS3Builder::from_env()`; the credential comes from the one
        // place in `record_semantics_support::kafka`.
        kafka::use_stack_s3_env();
        let left = sweep_archives(None);
        assert!(
            left.is_empty(),
            "an earlier run left {ID_PREFIX} archives that could not be swept: {left:?}"
        );
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is after 1970")
            .as_nanos();
        Row {
            name,
            nonce: format!("{:010}", nanos % 10_000_000_000),
            topics: Vec::new(),
            archives: Vec::new(),
        }
    }

    fn name_for(&self, suffix: &str) -> String {
        format!("{ID_PREFIX}{}-{suffix}", self.nonce)
    }

    /// A source topic. Its fixtures stamp records at `T` (2025-10-09), past
    /// the broker's default retention, so it keeps them: `retention.ms=-1`
    /// (`harness::create_topic_for_fixed_timestamps`; PROD-00.1 4.4).
    fn source_topic(&mut self, suffix: &str, configs: &[(&str, &str)]) -> String {
        let t = self.name_for(suffix);
        self.topics.push(t.clone());
        create_topic_for_fixed_timestamps(&t, PARTS, configs);
        t
    }

    fn backup_id(&mut self, suffix: &str) -> String {
        let b = self.name_for(&format!("{suffix}-b"));
        self.archives.push(b.clone());
        b
    }

    /// The `topicNaming.prefix` for one restore and the target it yields.
    fn target(&mut self, label: &str, source: &str) -> (String, String) {
        let prefix = self.name_for(&format!("{label}-"));
        let target = format!("{prefix}{source}");
        self.topics.push(target.clone());
        (prefix, target)
    }
}

impl Drop for Row {
    fn drop(&mut self) {
        for t in &self.topics {
            let _ = kafka_topics(&[
                "--bootstrap-server",
                "kafka-broker-1:9094",
                "--delete",
                "--topic",
                t,
            ]);
        }
        let left = sweep_archives(Some(&self.archives));
        if !left.is_empty() {
            // Never a second panic while unwinding: it would abort the process
            // and take the row's own failure message with it.
            eprintln!("[recsem] {}: sweep left {left:?}", self.name);
        }
    }
}

/// Remove this file's archives and receipts from the shared archive bucket.
/// `None` sweeps every `recsem-` key (leftovers of a dead run); `Some` only
/// the named backup ids. Returns what survived.
fn sweep_archives(only: Option<&[String]>) -> Vec<String> {
    let mine = |k: &str| -> bool {
        let under = |id: &str| {
            k.starts_with(&format!("{id}/")) || k.starts_with(&format!("logweir/backups/{id}/"))
        };
        match only {
            None => {
                k.starts_with(ID_PREFIX) || k.starts_with(&format!("logweir/backups/{ID_PREFIX}"))
            }
            Some(ids) => ids.iter().any(|id| under(id)),
        }
    };
    let list = || -> Vec<String> {
        mc(&[
            "--json",
            "ls",
            "--recursive",
            &format!("local/{ARCHIVE_BUCKET}"),
        ])
        .stdout_utf8()
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter_map(|v| v["key"].as_str().map(str::to_string))
        .filter(|k| mine(k))
        .collect()
    };
    let prefixes: BTreeSet<String> = list()
        .iter()
        .map(|k| {
            let segs: Vec<&str> = k.split('/').collect();
            if k.starts_with("logweir/") {
                segs[..3.min(segs.len())].join("/")
            } else {
                segs[0].to_string()
            }
        })
        .collect();
    for p in prefixes {
        let _ = mc(&[
            "rm",
            "--recursive",
            "--force",
            &format!("local/{ARCHIVE_BUCKET}/{p}/"),
        ]);
    }
    list()
}

// ============================================================ the pipeline

fn backup_ok(backup_id: &str, topics: &[&str], segment_max_records: u64) {
    let o = kafka::backup_run(backup_id, topics, segment_max_records);
    assert_eq!(
        o.status.code(),
        Some(0),
        "`logweir backup run` {backup_id} must exit 0\nstdout:\n{}\nstderr:\n{}",
        o.stdout_utf8(),
        o.stderr_utf8()
    );
}

fn restore_spec(
    backup_id: &str,
    source: &str,
    prefix: &str,
    pit: Option<i64>,
    sample: (i64, i64),
) -> serde_yaml::Value {
    restore_spec_with_basis(backup_id, source, prefix, pit, sample, false)
}

/// `restore_spec`, and with `producer_time` the plan also states
/// `restore.time_basis: producerTime` (FX-8): it accepts a point-in-time
/// selection by the producers' clocks over a `LogAppendTime` source, which the
/// runner otherwise refuses with `PointInTimeByProducerTime`.
fn restore_spec_with_basis(
    backup_id: &str,
    source: &str,
    prefix: &str,
    pit: Option<i64>,
    sample: (i64, i64),
    producer_time: bool,
) -> serde_yaml::Value {
    let basis = if producer_time {
        "\x20 time_basis: producerTime\n"
    } else {
        ""
    };
    let restore_block = pit
        .map(|ms| format!("restore:\n\x20 point_in_time: \"{}\"\n{basis}", rfc3339(ms)))
        .unwrap_or_default();
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
         \x20 topics: [{source}]\n\
         target:\n\
         \x20 bootstrap_servers: [{boot}]\n\
         \x20 mode: newTopic\n\
         \x20 topic_mapping_prefix: \"drill-\"\n\
         \x20 topic_naming:\n\
         \x20   prefix: \"{prefix}\"\n\
         \x20 default_replication_factor: 1\n\
         {restore_block}\
         sample:\n\
         \x20 window_start: \"{}\"\n\
         \x20 window_end: \"{}\"\n\
         \x20 records_per_partition: 25\n\
         \x20 anchor: head\n\
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
        rfc3339(sample.0),
        rfc3339(sample.1)
    ))
    .expect("the restore spec is valid YAML")
}

/// Logweir's own verdict on a restore: exit code, signed outcome, integrity
/// block, and the phase-7 lines that explain it. Recorded, never the outcome.
fn verdict(r: &Run) -> Value {
    let sc: Value = std::fs::read(&r.scorecard)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or(Value::Null);
    let both = format!("{}\n{}", r.out.stdout_utf8(), r.out.stderr_utf8());
    let pick = |needle: &str| -> Vec<String> {
        both.lines()
            .filter(|l| l.contains(needle))
            .map(|l| l.chars().take(600).collect())
            .collect()
    };
    json!({
        "exit": r.out.status.code(),
        "outcome": sc["outcome"],
        // FX-8: what the signed document says about the clock the selection
        // read, and the refusal line a refused run ends with.
        "time_basis": sc["source"]["time_basis"],
        "refusal_reason": pick("refusal-reason=").into_iter().last(),
        "integrity": sc["integrity"],
        "records_restored": sc["sample"]["records_restored"],
        "summary": pick("run ").into_iter().find(|l| l.starts_with("run ")),
        "count_bound": pick("manifest bounds the window"),
        "selection_verdicts": pick("selection verdict"),
        // The last lines of each stream, so an exit without a scorecard still
        // says why.
        "stdout_tail": tail_lines(&r.out.stdout_utf8(), 25),
        "stderr_tail": tail_lines(&r.out.stderr_utf8(), 25),
    })
}

fn tail_lines(text: &str, n: usize) -> Vec<String> {
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(n)..]
        .iter()
        .map(|l| l.chars().take(800).collect())
        .collect()
}

struct Restored {
    target: String,
    verdict: Value,
    observed: Vec<Rec>,
}

/// How long one `logweir restore run` may take before the row gives up.
const RESTORE_DEADLINE_SECS: u64 = 900;

/// `harness::run_with` for a restore, with a deadline. The harness spawns
/// `logweir` with no timeout (`harness/mod.rs`, `cmd.output()`), so the run
/// happens on its own thread and is joined against `secs` (review L5a).
fn run_restore_within(spec: serde_yaml::Value, secs: u64) -> Run {
    let h = std::thread::spawn(move || {
        let mut o = RunOpts::new(&spec);
        o.restore_run = true;
        run_with(o)
    });
    join_within(h, secs, "logweir restore run")
}

/// Join `h`, or — after `secs` — kill this worktree's `logweir restore run`
/// processes and engine containers (the only ones that could be holding the
/// thread) and fail the row instead of hanging under the compose lock.
fn join_within<T>(h: std::thread::JoinHandle<T>, secs: u64, what: &str) -> T {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !h.is_finished() {
        if Instant::now() > deadline {
            kill_own_restores();
            let grace = Instant::now() + Duration::from_secs(30);
            while !h.is_finished() && Instant::now() < grace {
                std::thread::sleep(Duration::from_millis(200));
            }
            panic!(
                "{what}: no result after {secs} s; this worktree's restore processes were killed"
            );
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    h.join()
        .unwrap_or_else(|_| panic!("{what}: the thread panicked"))
}

/// Run `program args` with a deadline and return its stdout ("" on failure).
fn run_quiet(program: &str, args: &[&str], secs: u64) -> String {
    let mut c = std::process::Command::new(program);
    c.args(args);
    kafka::output_within(c, secs)
        .map(|o| o.stdout_utf8())
        .unwrap_or_default()
}

/// The `logweir restore run` processes this worktree started: their command
/// line names this worktree's `.e2e/` spec (`harness::run_with` writes it).
fn own_restore_pids() -> Vec<String> {
    let pattern = format!("logweir restore run --spec {}/drill-", demo_dir().display());
    run_quiet("pgrep", &["-f", &pattern], 20)
        .split_whitespace()
        .map(str::to_string)
        .collect()
}

/// The engine containers this worktree started: the ones bind-mounting its
/// `harness::engine_mount()`.
fn own_engine_containers() -> Vec<String> {
    let volume = format!("volume={}", engine_mount().display());
    run_quiet("docker", &["ps", "-q", "--filter", &volume], 30)
        .split_whitespace()
        .map(str::to_string)
        .collect()
}

fn kill_own_restores() {
    for pid in own_restore_pids() {
        run_quiet("kill", &["-KILL", &pid], 20);
    }
    for id in own_engine_containers() {
        run_quiet("docker", &["kill", &id], 60);
    }
}

// ========================================================== the contract

/// The engine release whose behaviour the assertions below state (review L4).
/// On any other release — `engine-matrix` runs this suite for others — a row
/// records its outcome and asserts nothing, so a different engine is a
/// finding in the outcome file, not a red matrix cell.
const CONTRACT_ENGINE: &str = "0.21.0";

fn contract_applies(row: &str) -> bool {
    let v = engine_version();
    if v == CONTRACT_ENGINE {
        return true;
    }
    eprintln!(
        "[recsem] {row}: engine {v} is not {CONTRACT_ENGINE}; outcome recorded, contract not asserted"
    );
    false
}

/// Logweir's own signed verdict is part of the contract (review L6): FX-6's
/// "the drill passes / fails" sentences rest on it, so a change in Logweir's
/// verdict must turn the row red with the record, not silently.
fn assert_verdict(what: &str, r: &Restored, exit: i32, outcome: &str) {
    assert_eq!(
        (r.verdict["exit"].as_i64(), r.verdict["outcome"].as_str()),
        (Some(i64::from(exit)), Some(outcome)),
        "{what}: Logweir's verdict changed: {}",
        r.verdict
    );
}

fn restore(
    row: &mut Row,
    label: &str,
    backup_id: &str,
    source: &str,
    pit: Option<i64>,
    sample: (i64, i64),
) -> Restored {
    restore_with_basis(row, label, backup_id, source, pit, sample, false)
}

/// `restore`, with the plan's `restore.time_basis: producerTime` when
/// `producer_time` (FX-8).
fn restore_with_basis(
    row: &mut Row,
    label: &str,
    backup_id: &str,
    source: &str,
    pit: Option<i64>,
    sample: (i64, i64),
    producer_time: bool,
) -> Restored {
    let (prefix, target) = row.target(label, source);
    let spec = restore_spec_with_basis(backup_id, source, &prefix, pit, sample, producer_time);
    restore_spec_run(row, label, target, spec)
}

/// **PROD-08.1.** `restore`, with `sample.coverage: complete` in the plan:
/// phase 7 hashes and decodes every segment of every restored partition,
/// computes the expected output from each record's own timestamp, and
/// compares every restored record with it by `x-original-offset`. The signed
/// result is `verdict["integrity"]["verification"]` ([`complete_block`]).
fn restore_complete(
    row: &mut Row,
    label: &str,
    backup_id: &str,
    source: &str,
    pit: Option<i64>,
    sample: (i64, i64),
) -> Restored {
    let (prefix, target) = row.target(label, source);
    let mut spec = restore_spec(backup_id, source, &prefix, pit, sample);
    spec.get_mut("sample")
        .and_then(serde_yaml::Value::as_mapping_mut)
        .expect("the spec has a sample block")
        .insert("coverage".into(), "complete".into());
    restore_spec_run(row, label, target, spec)
}

/// The signed complete block of a complete restore (PROD-08.1).
fn complete_block(r: &Restored) -> &Value {
    &r.verdict["integrity"]["verification"]["complete"]
}

/// Every finding a complete restore's block lists, one per line.
fn complete_findings(r: &Restored) -> String {
    complete_block(r)["partitions"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|p| {
            let at = format!("p{}", p["partition"]);
            p["findings"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|f| f.as_str())
                .map(move |f| format!("{at}: {f}"))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn restore_spec_run(
    row: &mut Row,
    label: &str,
    target: String,
    spec: serde_yaml::Value,
) -> Restored {
    let run = run_restore_within(spec, RESTORE_DEADLINE_SECS);
    let verdict = verdict(&run);
    eprintln!(
        "[recsem] {} {label}: logweir restore run exit={} outcome={}",
        row.name, verdict["exit"], verdict["outcome"]
    );
    let observed = if topic_exists(&target) {
        kafka::read_topic(&target, PARTS, Isolation::Committed)
            .unwrap_or_else(|e| panic!("reading {target}: {e}"))
    } else {
        Vec::new()
    };
    Restored {
        target,
        verdict,
        observed,
    }
}

/// The archive's time span, one second either side: the sample window of a
/// FULL restore, bound to the archive the way `harness::spec_default` binds
/// it (so RPO measures the archive, not the suite's wall clock).
fn archive_span(a: &Archive) -> (i64, i64) {
    let lo = a
        .records
        .iter()
        .map(|r| r.timestamp)
        .min()
        .expect("records");
    let hi = a
        .records
        .iter()
        .map(|r| r.timestamp)
        .max()
        .expect("records");
    (lo - 1000, hi + 1000)
}

// ============================================================ comparisons

/// Capture: what the log held (`read_uncommitted`) against the archive.
fn capture(raw: &[Rec], archive: &Archive) -> Vec<Divergence> {
    compare(&Comparison {
        expected: raw,
        committed: raw,
        raw_source: raw,
        observed: &archive.records,
        headers: HeaderModel::AppendLineage,
        lineage: LineageFrom::RecordOffset,
    })
}

/// Replay: the archive records the model selects against the output.
fn replay(archive: &Archive, model: impl Fn(&Rec) -> bool, observed: &[Rec]) -> Vec<Divergence> {
    let selected: Vec<Rec> = archive
        .records
        .iter()
        .filter(|r| model(r))
        .cloned()
        .collect();
    compare(&Comparison {
        expected: &selected,
        committed: &archive.records,
        raw_source: &archive.records,
        observed,
        headers: HeaderModel::Identity,
        lineage: LineageFrom::Header,
    })
}

/// End to end: the committed input the model selects against the output.
fn end_to_end(
    committed: &[Rec],
    raw: &[Rec],
    model: impl Fn(&Rec) -> bool,
    observed: &[Rec],
) -> Vec<Divergence> {
    let selected: Vec<Rec> = committed.iter().filter(|r| model(r)).cloned().collect();
    compare(&Comparison {
        expected: &selected,
        committed,
        raw_source: raw,
        observed,
        headers: HeaderModel::AppendLineage,
        lineage: LineageFrom::Header,
    })
}

/// A divergence without the target offset, which depends on the layout of the
/// output and says nothing about the record: `(class, partition, source
/// offset or lineage)`.
type Key = (&'static str, i32, i64);

fn keys(d: &[Divergence]) -> BTreeSet<Key> {
    d.iter()
        .map(|x| {
            let (p, o) = match x {
                Divergence::Missing {
                    partition,
                    source_offset,
                }
                | Divergence::Duplicate {
                    partition,
                    source_offset,
                    ..
                }
                | Divergence::OutOfOrder {
                    partition,
                    source_offset,
                    ..
                }
                | Divergence::KeyChanged {
                    partition,
                    source_offset,
                }
                | Divergence::ValueChanged {
                    partition,
                    source_offset,
                }
                | Divergence::TimestampChanged {
                    partition,
                    source_offset,
                    ..
                }
                | Divergence::TimestampTypeChanged {
                    partition,
                    source_offset,
                    ..
                }
                | Divergence::HeadersCollapsed {
                    partition,
                    source_offset,
                }
                | Divergence::HeadersChanged {
                    partition,
                    source_offset,
                    ..
                } => (*partition, *source_offset),
                Divergence::Extra {
                    partition, lineage, ..
                } => (*partition, lineage.unwrap_or(-1)),
            };
            (x.class(), p, o)
        })
        .collect()
}

fn ids(recs: &[Rec], pred: impl Fn(&Rec) -> bool) -> Vec<(i32, i64)> {
    recs.iter()
        .filter(|r| pred(r))
        .map(|r| (r.partition, r.offset))
        .collect()
}

fn with_class(class: &'static str, at: &[(i32, i64)]) -> BTreeSet<Key> {
    at.iter().map(|(p, o)| (class, *p, *o)).collect()
}

/// The row's own negative control: the contract check, applied to the REAL
/// output with one record dropped and, separately, one duplicated, must
/// reject both. A check that cannot fail on this row's own data is not a
/// check of this row.
fn mutants_are_caught(
    what: &str,
    check: impl Fn(&[Rec]) -> BTreeSet<Key>,
    observed: &[Rec],
    contract: &BTreeSet<Key>,
) {
    let mut dropped = observed.to_vec();
    if dropped.is_empty() {
        // Nothing to drop: fabricate an unexplained record instead.
        dropped.push(Rec {
            partition: 0,
            offset: 0,
            timestamp: 0,
            ts_type: TsType::CreateTime,
            key: None,
            value: None,
            headers: Vec::new(),
        });
    } else {
        dropped.remove(0);
    }
    assert_ne!(
        &check(&dropped),
        contract,
        "{what}: the contract check still matched with one output record removed"
    );
    if let Some(first) = observed.first() {
        let mut dup = observed.to_vec();
        let mut again = first.clone();
        again.offset = i64::MAX;
        dup.push(again);
        assert_ne!(
            &check(&dup),
            contract,
            "{what}: the contract check still matched with one output record duplicated"
        );
    }
}

fn render(d: &[Divergence]) -> Vec<String> {
    d.iter().map(describe).collect()
}

fn summary_json(d: &[Divergence]) -> Value {
    json!({ "count": d.len(), "by_class": summarize(d), "items": render(d) })
}

/// The manifest's `configurations` block for `topic`: the explicit topic
/// overrides the engine captured (`manifest.rs:143-146`), or `null`.
fn manifest_configurations(a: &Archive, topic: &str) -> Value {
    a.manifest["topics"]
        .as_array()
        .and_then(|ts| ts.iter().find(|t| t["name"].as_str() == Some(topic)))
        .map(|t| t["configurations"].clone())
        .unwrap_or(Value::Null)
}

fn segments_json(a: &Archive) -> Value {
    Value::Array(
        a.segments
            .iter()
            .map(|s| {
                json!({
                    "partition": s.partition,
                    "offsets": [s.start_offset, s.end_offset],
                    "first_record_ts": s.start_timestamp,
                    "last_record_ts": s.end_timestamp,
                    "record_count": s.record_count,
                    "min_record_ts": a.records.iter().filter(|r| r.partition == s.partition
                        && r.offset >= s.start_offset && r.offset <= s.end_offset)
                        .map(|r| r.timestamp).min(),
                    "max_record_ts": a.records.iter().filter(|r| r.partition == s.partition
                        && r.offset >= s.start_offset && r.offset <= s.end_offset)
                        .map(|r| r.timestamp).max(),
                })
            })
            .collect(),
    )
}

/// Writes `.e2e/record-semantics/<row>.json` and prints the one-line result.
#[allow(clippy::too_many_arguments)]
fn record_outcome(
    row: &str,
    raw: &[Rec],
    committed: &[Rec],
    archive: &Archive,
    restores: &[(&str, &Restored, &[Divergence], &[Divergence])],
    capture: &[Divergence],
    extra: Value,
) {
    let rs: Vec<Value> = restores
        .iter()
        .map(|(label, r, replay, e2e)| {
            eprintln!(
                "[recsem] {row} {label}: target={} records | replay {:?} | end-to-end {:?} | \
                 logweir exit={} outcome={}",
                r.observed.len(),
                summarize(replay),
                summarize(e2e),
                r.verdict["exit"],
                r.verdict["outcome"]
            );
            json!({
                "label": label,
                "target_topic": r.target,
                "target_records": r.observed.len(),
                "replay": summary_json(replay),
                "end_to_end": summary_json(e2e),
                "logweir": r.verdict,
            })
        })
        .collect();
    eprintln!(
        "[recsem] {row}: raw={} committed={} archive={} | capture {:?}",
        raw.len(),
        committed.len(),
        archive.records.len(),
        summarize(capture)
    );
    let v = json!({
        "row": row,
        "engine_version": engine_version(),
        "engine_digest": engine_digest(),
        "source_raw_records": raw.len(),
        "source_committed_records": committed.len(),
        "archive_records": archive.records.len(),
        "archive_segments": segments_json(archive),
        "manifest_topic_configurations": archive
            .manifest["topics"]
            .as_array()
            .map(|ts| {
                ts.iter()
                    .map(|t| json!({"topic": t["name"], "configurations": t["configurations"]}))
                    .collect::<Vec<_>>()
            }),
        "capture": summary_json(capture),
        "restores": rs,
        "notes": extra,
    });
    kafka::write_json(
        &demo_dir()
            .join("record-semantics")
            .join(format!("{row}.json")),
        &v,
    );
}

/// The fixture as a consumer should read it back: same partition, key,
/// value, headers and (when stated) timestamp, in order.
fn assert_fixture_landed(what: &str, intended: &[Out], got: &[Rec]) {
    for p in 0..PARTS {
        let want: Vec<&Out> = intended.iter().filter(|o| o.partition == p).collect();
        let have: Vec<&Rec> = got.iter().filter(|r| r.partition == p).collect();
        assert_eq!(
            have.len(),
            want.len(),
            "{what}: partition {p} holds {} records, the fixture wrote {}",
            have.len(),
            want.len()
        );
        for (w, h) in want.iter().zip(have.iter()) {
            assert_eq!(
                (&h.key, &h.value, &h.headers),
                (&w.key, &w.value, &w.headers),
                "{what}: p{p}@{} is not the record the fixture wrote",
                h.offset
            );
            if let Some(ts) = w.timestamp {
                if h.ts_type == TsType::CreateTime {
                    assert_eq!(h.timestamp, ts, "{what}: p{p}@{} timestamp", h.offset);
                }
            }
        }
    }
}

// ===================================================== detection signals

/// PROD-01.1 §6.2's three detection signals, computed as a reference
/// implementation from what a backup can observe (review M3):
///
/// 1. `markers`: archived records of marker shape at offsets a
///    `read_uncommitted` consumer skips, so the shape is confirmed by the
///    offset gap (review L1);
/// 2. `open_at_probe`: partitions whose `read_committed` high mark was below
///    the `read_uncommitted` one just before the engine started;
/// 3. `uncommitted_tail`: archived records at or above that `read_committed`
///    mark which a `read_committed` reading taken AFTER the transactions ended
///    does not return, markers excluded. It covers a transaction opened after
///    the probe, which signals 1 and 2 cannot see.
struct Signals {
    markers: BTreeSet<(i32, i64)>,
    open_at_probe: Vec<i32>,
    uncommitted_tail: BTreeSet<(i32, i64)>,
}

impl Signals {
    fn to_json(&self) -> Value {
        json!({
            "markers": self.markers,
            "open_at_probe": self.open_at_probe,
            "uncommitted_tail": self.uncommitted_tail,
        })
    }
}

fn detection_signals(
    archive: &Archive,
    raw_after: &[Rec],
    committed_after: &[Rec],
    wm_committed: &[(i32, i64, i64)],
    wm_uncommitted: &[(i32, i64, i64)],
) -> Signals {
    let raw_ids: BTreeSet<(i32, i64)> = raw_after.iter().map(|x| (x.partition, x.offset)).collect();
    let committed_ids: BTreeSet<(i32, i64)> = committed_after
        .iter()
        .map(|x| (x.partition, x.offset))
        .collect();
    let markers: BTreeSet<(i32, i64)> = archive
        .records
        .iter()
        .filter(|x| is_control_shaped(x) && !raw_ids.contains(&(x.partition, x.offset)))
        .map(|x| (x.partition, x.offset))
        .collect();
    let open_at_probe: Vec<i32> = wm_committed
        .iter()
        .zip(wm_uncommitted)
        .filter(|((_, _, c), (_, _, u))| c < u)
        .map(|((p, _, _), _)| *p)
        .collect();
    let lso_before: std::collections::BTreeMap<i32, i64> =
        wm_committed.iter().map(|(p, _, hi)| (*p, *hi)).collect();
    let uncommitted_tail: BTreeSet<(i32, i64)> = archive
        .records
        .iter()
        .filter(|x| x.offset >= lso_before.get(&x.partition).copied().unwrap_or(i64::MAX))
        .map(|x| (x.partition, x.offset))
        .filter(|id| !markers.contains(id) && !committed_ids.contains(id))
        .collect();
    Signals {
        markers,
        open_at_probe,
        uncommitted_tail,
    }
}

// =================================================================== TXN

/// Transactions: a transactional producer that commits and aborts, and one
/// transaction held OPEN across the backup and aborted after it.
///
/// Contract (engine 0.21.0 captures `READ_UNCOMMITTED`, keeps control
/// records, and replays without a transactional producer):
/// * capture: the archive holds every data record the log held, committed,
///   aborted and open alike, plus every transaction marker below the captured
///   high watermark as an ordinary record;
/// * replay: the output is the archive, record for record;
/// * end to end: every aborted or open-at-capture record and every marker is
///   an extra; no committed record is missing, changed or reordered.
#[test]
fn transactional_topic_committed_input_versus_restored_output() {
    let mut row = Row::new("txn");
    let topic = row.source_topic("txn", &[("message.timestamp.type", "CreateTime")]);
    // Review M3: a second topic whose ONLY transaction (E) opens after the
    // pre-backup probe and aborts after the backup, so it leaves no marker in
    // the archive and no gap in the probe.
    let late = row.source_topic("txn-late", &[("message.timestamp.type", "CreateTime")]);
    let late_plain: Vec<Out> = (0..PARTS)
        .map(|part| Out::kv(part, None, &format!("l{part}"), &format!("l{part}-value")))
        .collect();
    kafka::produce_plain(&late, &late_plain).expect("late topic's committed records");
    let mut p = Txn::new(&format!("{topic}-tx")).expect("transactional producer");
    let rec = |part: i32, name: &str| Out::kv(part, None, name, &format!("{name}-value"));
    let mut intended_raw: Vec<Out> = Vec::new();
    let mut run_txn = |p: &mut Txn, recs: &[Out], end: &str| {
        p.begin().expect("begin");
        for o in recs {
            p.send(&topic, o).expect("send");
        }
        p.flush().expect("flush");
        match end {
            "commit" => p.commit().expect("commit"),
            "abort" => p.abort().expect("abort"),
            _ => {}
        }
        intended_raw.extend_from_slice(recs);
    };
    // A commits across all three partitions; B aborts on two; C commits on
    // two; D is flushed and left open.
    let a = [
        rec(0, "a0"),
        rec(0, "a1"),
        rec(1, "a2"),
        rec(2, "a3"),
        rec(2, "a4"),
    ];
    let b = [rec(0, "b0"), rec(1, "b1"), rec(1, "b2")];
    let c = [rec(1, "c0"), rec(2, "c1")];
    let d = [rec(0, "d0"), rec(2, "d1")];
    run_txn(&mut p, &a, "commit");
    run_txn(&mut p, &b, "abort");
    run_txn(&mut p, &c, "commit");
    run_txn(&mut p, &d, "open");

    let hwm_at_capture = kafka::high_watermarks(&topic, PARTS).expect("watermarks");
    // The open-transaction probes, taken while D is open (see
    // `kafka::watermarks_at` and `kafka::committed_position_at_eof`).
    let wm_committed =
        kafka::watermarks_at(&topic, PARTS, Isolation::Committed).expect("committed watermarks");
    let wm_uncommitted = kafka::watermarks_at(&topic, PARTS, Isolation::Uncommitted)
        .expect("uncommitted watermarks");
    let eof_committed: Vec<(i32, Result<i64, String>)> = (0..PARTS)
        .map(|part| (part, kafka::committed_position_at_eof(&topic, part)))
        .collect();
    let late_wm_committed = kafka::watermarks_at(&late, PARTS, Isolation::Committed)
        .expect("late committed watermarks");
    let late_wm_uncommitted = kafka::watermarks_at(&late, PARTS, Isolation::Uncommitted)
        .expect("late uncommitted watermarks");
    // E opens only now, after every probe, on p1 of the late topic.
    let mut q = Txn::new(&format!("{late}-tx")).expect("second transactional producer");
    let e = [
        Out::kv(1, None, "e0", "e0-value"),
        Out::kv(1, None, "e1", "e1-value"),
    ];
    q.begin().expect("begin E");
    for o in &e {
        q.send(&late, o).expect("send E");
    }
    q.flush().expect("flush E");
    let backup_id = row.backup_id("txn");
    backup_ok(&backup_id, &[&topic, &late], 1000);
    // D and E end AFTER the capture, as aborts.
    p.abort().expect("abort the open transaction");
    q.abort().expect("abort the late transaction");

    let raw = kafka::read_topic(&topic, PARTS, Isolation::Uncommitted).expect("raw");
    let committed = kafka::read_topic(&topic, PARTS, Isolation::Committed).expect("committed");
    // The fixture landed as designed: the committed view is A then C, the
    // raw view is A, B, C, D in order.
    let intended_committed: Vec<Out> = a.iter().chain(c.iter()).cloned().collect();
    let order = |v: &mut Vec<Out>| v.sort_by_key(|o| o.partition);
    let mut ic = intended_committed.clone();
    order(&mut ic);
    let mut ir = intended_raw.clone();
    order(&mut ir);
    assert_fixture_landed("txn committed view", &ic, &committed);
    assert_fixture_landed("txn raw view", &ir, &raw);

    let archive = kafka::read_archive(&backup_id, &topic).expect("archive");
    let r = restore(
        &mut row,
        "full",
        &backup_id,
        &topic,
        None,
        archive_span(&archive),
    );

    let late_raw = kafka::read_topic(&late, PARTS, Isolation::Uncommitted).expect("late raw");
    let late_committed =
        kafka::read_topic(&late, PARTS, Isolation::Committed).expect("late committed");
    assert_fixture_landed("late committed view", &late_plain, &late_committed);
    let mut late_intended_raw: Vec<Out> = late_plain.iter().chain(e.iter()).cloned().collect();
    late_intended_raw.sort_by_key(|o| o.partition);
    assert_fixture_landed("late raw view", &late_intended_raw, &late_raw);
    let late_archive = kafka::read_archive(&backup_id, &late).expect("late archive");
    let rl = restore(
        &mut row,
        "late",
        &backup_id,
        &late,
        None,
        archive_span(&late_archive),
    );
    let late_committed_ids: BTreeSet<(i32, i64)> = late_committed
        .iter()
        .map(|x| (x.partition, x.offset))
        .collect();
    let late_uncommitted = ids(&late_raw, |x| {
        !late_committed_ids.contains(&(x.partition, x.offset))
    });
    let late_cap = capture(&late_raw, &late_archive);
    let late_rep = replay(&late_archive, |_| true, &rl.observed);
    let late_e2e = end_to_end(&late_committed, &late_raw, |_| true, &rl.observed);
    let late_sig = detection_signals(
        &late_archive,
        &late_raw,
        &late_committed,
        &late_wm_committed,
        &late_wm_uncommitted,
    );

    // Offsets below the captured high watermark that no consumer returns are
    // control records; the D abort marker lies above it and was not captured.
    let raw_ids: BTreeSet<(i32, i64)> = raw.iter().map(|r| (r.partition, r.offset)).collect();
    let markers: Vec<(i32, i64)> = hwm_at_capture
        .iter()
        .flat_map(|(part, hi)| (0..*hi).map(move |o| (*part, o)))
        .filter(|id| !raw_ids.contains(id))
        .collect();
    let committed_ids: BTreeSet<(i32, i64)> =
        committed.iter().map(|r| (r.partition, r.offset)).collect();
    let uncommitted = ids(&raw, |x| !committed_ids.contains(&(x.partition, x.offset)));

    let cap = capture(&raw, &archive);
    let rep = replay(&archive, |_| true, &r.observed);
    let e2e = end_to_end(&committed, &raw, |_| true, &r.observed);
    let sig = detection_signals(&archive, &raw, &committed, &wm_committed, &wm_uncommitted);
    let marker_kinds: Vec<Value> = archive
        .records
        .iter()
        .filter(|x| markers.contains(&(x.partition, x.offset)))
        .map(|x| {
            json!({
                "partition": x.partition,
                "offset": x.offset,
                "commit": control_is_commit(x),
                "timestamp": x.timestamp,
            })
        })
        .collect();
    record_outcome(
        "txn",
        &raw,
        &committed,
        &archive,
        &[("full", &r, &rep[..], &e2e[..])],
        &cap,
        json!({
            "open_transaction_probe": {
                "read_committed_watermarks": wm_committed,
                "read_uncommitted_watermarks": wm_uncommitted,
                "read_committed_position_at_eof": eof_committed
                    .iter()
                    .map(|(part, r)| json!({"partition": part, "position": r.as_ref().ok(), "error": r.as_ref().err()}))
                    .collect::<Vec<_>>(),
                "open_transaction_partitions": [0, 2],
            },
            "hwm_at_capture": hwm_at_capture,
            "marker_offsets": markers,
            "uncommitted_offsets": uncommitted,
            "archived_markers": marker_kinds,
            "detection_signals": sig.to_json(),
        }),
    );
    record_outcome(
        "txn-late",
        &late_raw,
        &late_committed,
        &late_archive,
        &[("late", &rl, &late_rep[..], &late_e2e[..])],
        &late_cap,
        json!({
            "probe_read_committed_watermarks": late_wm_committed,
            "probe_read_uncommitted_watermarks": late_wm_uncommitted,
            "uncommitted_offsets": late_uncommitted,
            "detection_signals": late_sig.to_json(),
        }),
    );
    if !contract_applies("txn") {
        return;
    }
    assert_verdict("txn", &r, 0, "pass");
    assert_verdict("txn late", &rl, 0, "pass");
    // §6.2 on the main topic: markers, the open-at-probe gap on p0 and p2,
    // and D's records in the uncommitted tail.
    let d_ids: BTreeSet<(i32, i64)> = raw
        .iter()
        .filter(|x| matches!(x.key.as_deref(), Some(b"d0") | Some(b"d1")))
        .map(|x| (x.partition, x.offset))
        .collect();
    assert_eq!(
        sig.markers,
        markers.iter().copied().collect::<BTreeSet<_>>()
    );
    assert_eq!(sig.open_at_probe, vec![0, 2]);
    assert_eq!(sig.uncommitted_tail, d_ids);
    // §6.2 on the late topic: signals 1 and 2 see nothing; signal 3 finds E.
    assert!(late_sig.markers.is_empty(), "{}", late_sig.to_json());
    assert!(late_sig.open_at_probe.is_empty(), "{}", late_sig.to_json());
    assert_eq!(
        late_sig.uncommitted_tail,
        late_uncommitted.iter().copied().collect::<BTreeSet<_>>()
    );
    assert_eq!(late_uncommitted.len(), 2, "E's two records");
    assert_eq!(
        keys(&late_cap),
        BTreeSet::new(),
        "late capture: {:#?}",
        render(&late_cap)
    );
    assert_eq!(
        keys(&late_rep),
        BTreeSet::new(),
        "late replay: {:#?}",
        render(&late_rep)
    );
    let want_late = with_class("extra:uncommitted", &late_uncommitted);
    assert_eq!(
        keys(&late_e2e),
        want_late,
        "late end to end: {:#?}",
        render(&late_e2e)
    );
    mutants_are_caught(
        "txn late",
        |o| keys(&end_to_end(&late_committed, &late_raw, |_| true, o)),
        &rl.observed,
        &want_late,
    );

    let want_capture = with_class("extra:control-marker", &markers);
    let mut want_e2e = with_class("extra:uncommitted", &uncommitted);
    want_e2e.extend(with_class("extra:control-marker", &markers));
    assert_eq!(keys(&cap), want_capture, "capture: {:#?}", render(&cap));
    assert_eq!(keys(&rep), BTreeSet::new(), "replay: {:#?}", render(&rep));
    assert_eq!(keys(&e2e), want_e2e, "end to end: {:#?}", render(&e2e));
    assert!(!markers.is_empty() && !uncommitted.is_empty());
    mutants_are_caught(
        "txn",
        |o| keys(&end_to_end(&committed, &raw, |_| true, o)),
        &r.observed,
        &want_e2e,
    );
}

// ======================================================== TIMESTAMPS

/// `(partition, [timestamp offsets from T])`, one record per entry, in order.
type Layout = [(i32, &'static [i64])];

fn ts_fixture(tag: &str, layout: &Layout) -> Vec<Out> {
    let mut v = Vec::new();
    for (p, deltas) in layout {
        for (i, d) in deltas.iter().enumerate() {
            v.push(Out::kv(
                *p,
                Some(T + d),
                &format!("{tag}-p{p}-{i}"),
                &format!("{tag} p{p} #{i} ts=T+{d}"),
            ));
        }
    }
    v
}

/// Produce, back up with 4-record segments, read everything back.
fn ts_setup(row: &mut Row, tag: &str, layout: &Layout) -> (String, String, Vec<Rec>, Archive) {
    let topic = row.source_topic(tag, &[("message.timestamp.type", "CreateTime")]);
    let fixture = ts_fixture(tag, layout);
    kafka::produce_plain(&topic, &fixture).expect("produce");
    let source = kafka::read_topic(&topic, PARTS, Isolation::Committed).expect("source");
    assert_fixture_landed(tag, &fixture, &source);
    let backup_id = row.backup_id(tag);
    backup_ok(&backup_id, &[&topic], 4);
    let archive = kafka::read_archive(&backup_id, &topic).expect("archive");
    (topic, backup_id, source, archive)
}

/// **The floor.** Every restore's window starts at the minimum of each
/// segment's FIRST record timestamp (`BackupSetFacts::
/// earliest_covered_timestamp_ms`), and the engine filters every record by its
/// own timestamp. A record older than every segment's first record is below
/// the floor.
///
/// Contract, both measured here: capture keeps it; replay drops it from a full
/// restore AND from a point-in-time restore. Logweir's verdict differs: the
/// full restore's segment is wholly inside the window, so the count bound
/// counts it whole and the drill FAILS; at a point the segment straddles, the
/// bound counts it only as an upper bound (`logweir-core engine.rs:205-210`)
/// and the record lies below the sample window (`logweir-engine-oso
/// engine.rs:511-513`), so the drill PASSES (review M1).
#[test]
fn non_monotonic_create_time_below_the_window_floor() {
    let mut row = Row::new("ts-floor");
    let layout: &Layout = &[
        (0, &[2000, 1000, 3000, 4000]),
        (1, &[2000, 2000, 2000, 2500]),
        (2, &[2100, 2200, 2300, 2400]),
    ];
    let (topic, backup_id, source, archive) = ts_setup(&mut row, "ts-floor", layout);
    let r = restore(&mut row, "full", &backup_id, &topic, None, (T, T + 10_000));
    let cap = capture(&source, &archive);
    let rep = replay(&archive, |_| true, &r.observed);
    let e2e = end_to_end(&source, &source, |_| true, &r.observed);
    // The same archive at a point p0's segment straddles, with the sample
    // window starting at the floor.
    let pit = T + 3500;
    let rp = restore(
        &mut row,
        "pit",
        &backup_id,
        &topic,
        Some(pit),
        (T + 2000, pit),
    );
    let in_window = |x: &Rec| x.timestamp <= pit;
    let rep_pit = replay(&archive, in_window, &rp.observed);
    let e2e_pit = end_to_end(&source, &source, in_window, &rp.observed);
    // PROD-08.1 (acceptance row 08-3): the same two restores with complete
    // coverage, whose expected output has no lower bound under an archive
    // floor — so the record below the floor is expected, and missing.
    let rc = restore_complete(&mut row, "cfull", &backup_id, &topic, None, (T, T + 10_000));
    let rcp = restore_complete(
        &mut row,
        "cpit",
        &backup_id,
        &topic,
        Some(pit),
        (T + 2000, pit),
    );
    let rep_c = replay(&archive, |_| true, &rc.observed);
    let rep_cp = replay(&archive, in_window, &rcp.observed);
    record_outcome(
        "ts-floor",
        &source,
        &source,
        &archive,
        &[
            ("full", &r, &rep[..], &e2e[..]),
            ("pit", &rp, &rep_pit[..], &e2e_pit[..]),
            ("complete full", &rc, &rep_c[..], &[][..]),
            ("complete pit", &rcp, &rep_cp[..], &[][..]),
        ],
        &cap,
        json!({"layout": "p0 [T+2000, T+1000, T+3000, T+4000]; p1 [T+2000 x3, T+2500]; p2 [T+2100..T+2400]",
               "floor_expected": T + 2000, "point_in_time": pit}),
    );
    if !contract_applies("ts-floor") {
        return;
    }
    let lost = with_class("missing", &[(0, 1)]);
    assert_eq!(keys(&cap), BTreeSet::new(), "capture: {:#?}", render(&cap));
    for (what, rep, e2e) in [("full", &rep, &e2e), ("pit", &rep_pit, &e2e_pit)] {
        assert_eq!(keys(rep), lost, "{what} replay: {:#?}", render(rep));
        assert_eq!(keys(e2e), lost, "{what} end to end: {:#?}", render(e2e));
    }
    assert_verdict("ts-floor full", &r, 2, "fail-integrity");
    assert_verdict("ts-floor pit", &rp, 0, "pass");
    // PROD-08.1, 08-3: complete coverage reports the record below the floor
    // MISSING in both restores — at the point too, where the sampled drill
    // above signs `pass` — until PROD-01.1b moves the floor.
    for (what, c) in [("complete full", &rc), ("complete pit", &rcp)] {
        assert_verdict(&format!("ts-floor {what}"), c, 2, "fail-integrity");
        let b = complete_block(c);
        assert_eq!(b["covered"], true, "{what}: {b}");
        assert_eq!(b["replay"]["missing"], 1, "{what}: {b}");
        assert!(
            complete_findings(c).contains("p0: source offset 1 is missing from the target"),
            "{what}: {}",
            complete_findings(c)
        );
    }
    mutants_are_caught(
        "ts-floor",
        |o| keys(&end_to_end(&source, &source, |_| true, o)),
        &r.observed,
        &lost,
    );
    mutants_are_caught(
        "ts-floor pit",
        |o| keys(&end_to_end(&source, &source, in_window, o)),
        &rp.observed,
        &lost,
    );
}

/// **The point-in-time end.** A segment whose FIRST record is after the
/// recovery point is skipped whole, although it holds a record at or before
/// it. Equal timestamps at the point itself are restored (inclusive end).
///
/// Contract: capture keeps the record; replay omits it; the manifest's count
/// bound excludes the skipped segment too, so the omission is invisible to
/// Logweir's verification.
#[test]
fn non_monotonic_create_time_skipped_at_the_point_in_time() {
    let mut row = Row::new("ts-pit");
    let layout: &Layout = &[
        (0, &[2000, 2100, 2200, 2300, 9000, 2500, 9100, 9200]),
        (1, &[2000, 5000, 5000, 5000]),
        (2, &[2000, 2100, 2200, 2300, 2400, 4000, 4500, 5000]),
    ];
    let (topic, backup_id, source, archive) = ts_setup(&mut row, "ts-pit", layout);
    let pit = T + 5000;
    let r = restore(&mut row, "pit", &backup_id, &topic, Some(pit), (T, pit));
    let in_window = |x: &Rec| x.timestamp <= pit;
    let cap = capture(&source, &archive);
    let rep = replay(&archive, in_window, &r.observed);
    let e2e = end_to_end(&source, &source, in_window, &r.observed);
    // PROD-08.1 (acceptance row 08-1): the same restore, complete coverage.
    let rc = restore_complete(&mut row, "cpit", &backup_id, &topic, Some(pit), (T, pit));
    let rep_c = replay(&archive, in_window, &rc.observed);
    record_outcome(
        "ts-pit",
        &source,
        &source,
        &archive,
        &[
            ("pit", &r, &rep[..], &e2e[..]),
            ("complete pit", &rc, &rep_c[..], &[][..]),
        ],
        &cap,
        json!({"point_in_time": pit,
               "layout": "p0 [T+2000..T+2300 | T+9000, T+2500, T+9100, T+9200]; p1 [T+2000, T+5000 x3]; p2 [T+2000..T+2300 | T+2400, T+4000, T+4500, T+5000]"}),
    );
    if !contract_applies("ts-pit") {
        return;
    }
    let lost = with_class("missing", &[(0, 5)]);
    assert_verdict("ts-pit", &r, 0, "pass");
    // PROD-08.1, 08-1: the expected output, from each record's own timestamp,
    // holds p0@5; the engine skipped its segment; complete coverage FAILS
    // naming it, where the sampled drill above signs `pass`.
    assert_verdict("ts-pit complete", &rc, 2, "fail-integrity");
    let b = complete_block(&rc);
    assert_eq!(
        (&b["replay"]["expected"], &b["replay"]["missing"]),
        (&json!(17), &json!(1)),
        "{b}"
    );
    assert!(
        complete_findings(&rc).contains("p0: source offset 5 is missing from the target"),
        "{}",
        complete_findings(&rc)
    );
    assert_eq!(keys(&cap), BTreeSet::new(), "capture: {:#?}", render(&cap));
    assert_eq!(keys(&rep), lost, "replay: {:#?}", render(&rep));
    assert_eq!(keys(&e2e), lost, "end to end: {:#?}", render(&e2e));
    mutants_are_caught(
        "ts-pit",
        |o| keys(&end_to_end(&source, &source, in_window, o)),
        &r.observed,
        &lost,
    );
}

/// **The false failure.** A segment whose first and last records are inside
/// the window but which holds a record after the recovery point: the engine
/// restores exactly the right records, and the manifest's count bound, which
/// counts the segment whole, says a record is missing.
///
/// Contract: capture, replay and end to end are exact; Logweir's verdict is
/// the only thing wrong.
#[test]
fn non_monotonic_create_time_inside_a_wholly_inside_segment() {
    let mut row = Row::new("ts-bound");
    let layout: &Layout = &[
        (0, &[2000, 6000, 2400, 2600]),
        (1, &[2000, 2100, 2200, 2300, 9000, 9100, 9200, 9300]),
        (2, &[2000, 2100, 2200, 2300]),
    ];
    let (topic, backup_id, source, archive) = ts_setup(&mut row, "ts-bound", layout);
    let pit = T + 5000;
    let r = restore(&mut row, "pit", &backup_id, &topic, Some(pit), (T, pit));
    let in_window = |x: &Rec| x.timestamp <= pit;
    let cap = capture(&source, &archive);
    let rep = replay(&archive, in_window, &r.observed);
    let e2e = end_to_end(&source, &source, in_window, &r.observed);
    // PROD-08.1 (acceptance row 08-2): the same restore, complete coverage.
    let rc = restore_complete(&mut row, "cpit", &backup_id, &topic, Some(pit), (T, pit));
    let rep_c = replay(&archive, in_window, &rc.observed);
    record_outcome(
        "ts-bound",
        &source,
        &source,
        &archive,
        &[
            ("pit", &r, &rep[..], &e2e[..]),
            ("complete pit", &rc, &rep_c[..], &[][..]),
        ],
        &cap,
        json!({"point_in_time": pit,
               "layout": "p0 [T+2000, T+6000, T+2400, T+2600]; p1 [T+2000..T+2300 | T+9000..T+9300]; p2 [T+2000..T+2300]"}),
    );
    if !contract_applies("ts-bound") {
        return;
    }
    let exact = BTreeSet::new();
    assert_verdict("ts-bound", &r, 2, "fail-integrity");
    // PROD-08.1, 08-2: the exact model does not fail the correct restore the
    // first/last count bound fails above.
    assert_verdict("ts-bound complete", &rc, 0, "pass");
    let b = complete_block(&rc);
    assert_eq!(b["covered"], true, "{b}");
    assert_eq!(
        (
            &b["replay"]["expected"],
            &b["replay"]["restored"],
            &b["replay"]["matching"]
        ),
        (&json!(11), &json!(11), &json!(11)),
        "{b}"
    );
    assert_eq!(
        keys(&rep_c),
        exact,
        "complete replay: {:#?}",
        render(&rep_c)
    );
    assert_eq!(keys(&cap), exact, "capture: {:#?}", render(&cap));
    assert_eq!(keys(&rep), exact, "replay: {:#?}", render(&rep));
    assert_eq!(keys(&e2e), exact, "end to end: {:#?}", render(&e2e));
    mutants_are_caught(
        "ts-bound",
        |o| keys(&end_to_end(&source, &source, in_window, o)),
        &r.observed,
        &exact,
    );
}

// ====================================================== LogAppendTime

/// A `LogAppendTime` source: every record is produced with an explicit
/// CreateTime in 2001, and the broker stamps its own append time.
///
/// Contract (the engine decodes `first_timestamp + delta` and discards the
/// batch's max timestamp, and produces with `CreateTime`):
/// * capture: the archive holds the PRODUCER's CreateTime, not the time a
///   source consumer sees;
/// * replay: exact;
/// * end to end: every record's timestamp and timestamp type change;
/// * point in time: selection follows the producer's clock, so a recovery
///   point in 2001 restores records the source did not hold until the run.
///
/// **FX-8.** That point-in-time restore is now REFUSED, exit 3,
/// `refusal-reason=PointInTimeByProducerTime`, before any target topic is
/// created, because the manifest records the topic override
/// `message.timestamp.type=LogAppendTime`; the SAME plan with
/// `restore.time_basis: producerTime` runs as before and its signed scorecard
/// lists the topic under `source.time_basis.producer_time`. The full restore
/// is not a selection by time, so it still runs, unlabelled.
#[test]
fn log_append_time_source_versus_restored_output() {
    let mut row = Row::new("lat");
    let topic = row.source_topic("lat", &[("message.timestamp.type", "LogAppendTime")]);
    let mut fixture = Vec::new();
    for p in 0..PARTS {
        for i in 0..3i64 {
            fixture.push(Out::kv(
                p,
                Some(C0 + i * 1000),
                &format!("lat-p{p}-{i}"),
                &format!("lat p{p} #{i} create=C0+{}", i * 1000),
            ));
        }
    }
    kafka::produce_plain(&topic, &fixture).expect("produce");
    let source = kafka::read_topic(&topic, PARTS, Isolation::Committed).expect("source");
    assert_fixture_landed("lat", &fixture, &source);
    assert!(
        source
            .iter()
            .all(|r| r.ts_type == TsType::LogAppendTime && r.timestamp > C0 + 10_000),
        "the source must report broker append times: {source:?}"
    );
    let backup_id = row.backup_id("lat");
    backup_ok(&backup_id, &[&topic], 1000);
    let archive = kafka::read_archive(&backup_id, &topic).expect("archive");

    let full = restore(
        &mut row,
        "full",
        &backup_id,
        &topic,
        None,
        archive_span(&archive),
    );
    let cap = capture(&source, &archive);
    let rep_full = replay(&archive, |_| true, &full.observed);
    let e2e_full = end_to_end(&source, &source, |_| true, &full.observed);

    // The point in time by the PRODUCER's clock: between the first and the
    // second record of every partition. By the source's own clock nothing
    // existed then, so the model is empty.
    let archived_create_time = archive.records.iter().all(|r| r.timestamp < C0 + 10_000);
    let pit = C0 + 1500;
    // FX-8: the plan WITHOUT the opt-in is refused before any target exists.
    let refused = archived_create_time.then(|| {
        restore(
            &mut row,
            "pit-refused",
            &backup_id,
            &topic,
            Some(pit),
            (C0 - 1000, pit),
        )
    });
    let refused_target_created = refused.as_ref().map(|r| topic_exists(&r.target));
    let pit_restore = archived_create_time.then(|| {
        restore_with_basis(
            &mut row,
            "pit",
            &backup_id,
            &topic,
            Some(pit),
            (C0 - 1000, pit),
            true,
        )
    });
    let (rep_pit, e2e_pit) = match &pit_restore {
        Some(pr) => (
            replay(&archive, |x| x.timestamp <= pit, &pr.observed),
            end_to_end(&source, &source, |x| x.timestamp <= pit, &pr.observed),
        ),
        None => (Vec::new(), Vec::new()),
    };
    let mut restores: Vec<(&str, &Restored, &[Divergence], &[Divergence])> =
        vec![("full", &full, &rep_full[..], &e2e_full[..])];
    if let Some(r) = &refused {
        restores.push(("pit-refused", r, &[], &[]));
    }
    if let Some(pr) = &pit_restore {
        restores.push(("pit", pr, &rep_pit[..], &e2e_pit[..]));
    }
    let target_ts_types: BTreeSet<&str> =
        full.observed.iter().map(|r| r.ts_type.as_str()).collect();
    record_outcome(
        "lat",
        &source,
        &source,
        &archive,
        &restores,
        &cap,
        json!({
            "archive_holds_producer_create_time": archived_create_time,
            "manifest_configurations": manifest_configurations(&archive, &topic),
            "point_in_time": pit,
            "pit_refused_target_created": refused_target_created,
            "target_timestamp_types": target_ts_types,
            "source_append_times": source.iter().map(|r| r.timestamp).collect::<Vec<_>>(),
            "archive_timestamps": archive.records.iter().map(|r| r.timestamp).collect::<Vec<_>>(),
        }),
    );

    if !contract_applies("lat") {
        return;
    }
    let all = ids(&source, |_| true);
    let want_cap = with_class("timestamp-changed", &all);
    let mut want_e2e = want_cap.clone();
    want_e2e.extend(with_class("timestamp-type-changed", &all));
    assert!(
        archived_create_time,
        "the archive must hold the producers' CreateTime"
    );
    assert_verdict("lat full", &full, 0, "pass");
    // FX-8: the full restore is not a selection by time: its block is written
    // and names nothing.
    assert_eq!(
        full.verdict["time_basis"],
        json!({"producer_time": [], "not_recorded": []}),
        "lat full: {}",
        full.verdict
    );
    // FX-8: the point in time WITHOUT the opt-in is refused, exit 3, before
    // any target topic exists, naming the topic and the manifest's record.
    let r = refused.as_ref().expect("the refused restore ran");
    assert_eq!(
        (
            r.verdict["exit"].as_i64(),
            r.verdict["refusal_reason"].as_str()
        ),
        (Some(3), Some("refusal-reason=PointInTimeByProducerTime")),
        "lat pit-refused: {}",
        r.verdict
    );
    assert!(
        r.verdict["outcome"].is_null(),
        "a refused run signs nothing: {}",
        r.verdict
    );
    assert_eq!(
        refused_target_created,
        Some(false),
        "the refusal comes before any target topic is created: {}",
        r.target
    );
    let said = r.verdict["stderr_tail"].to_string() + &r.verdict["stdout_tail"].to_string();
    assert!(
        said.contains(&format!(
            "`{topic}` (the archive manifest's topic override message.timestamp.type=LogAppendTime)"
        )),
        "the refusal names the topic and its record: {said}"
    );
    // ...and WITH it the same point runs, labelled.
    let pr = pit_restore.as_ref().expect("the point-in-time restore ran");
    assert_verdict("lat pit", pr, 0, "pass");
    assert_eq!(
        pr.verdict["time_basis"],
        json!({"plan": "producerTime", "producer_time": [topic.clone()], "not_recorded": []}),
        "lat pit: {}",
        pr.verdict
    );
    // The per-RECORD timestamp type is not in the archive, but the topic's
    // explicit `message.timestamp.type` override is in the manifest's
    // `configurations` — the one place a later reader can learn the source
    // was LogAppendTime (and only when it was a topic override the engine
    // could describe; FX-4).
    assert_eq!(
        manifest_configurations(&archive, &topic)["message.timestamp.type"].as_str(),
        Some("LogAppendTime"),
        "the manifest records the topic's timestamp-type override: {}",
        manifest_configurations(&archive, &topic)
    );
    assert_eq!(keys(&cap), want_cap, "capture: {:#?}", render(&cap));
    assert_eq!(
        keys(&rep_full),
        BTreeSet::new(),
        "replay: {:#?}",
        render(&rep_full)
    );
    assert_eq!(
        keys(&e2e_full),
        want_e2e,
        "end to end: {:#?}",
        render(&e2e_full)
    );
    // Each restored record carries its producer's CreateTime.
    for x in &full.observed {
        let src = source
            .iter()
            .find(|s| Some(s.offset) == x.lineage_header() && s.partition == x.partition)
            .expect("lineage");
        let i: i64 = String::from_utf8_lossy(src.key.as_deref().unwrap_or_default())
            .rsplit('-')
            .next()
            .and_then(|s| s.parse().ok())
            .expect("fixture key ends in its index");
        assert_eq!(x.timestamp, C0 + i * 1000, "{:?}", x.key);
    }
    let restored_at_pit: Vec<(i32, i64)> = source
        .iter()
        .filter(|s| {
            let i: i64 = String::from_utf8_lossy(s.key.as_deref().unwrap_or_default())
                .rsplit('-')
                .next()
                .and_then(|v| v.parse().ok())
                .unwrap_or(99);
            C0 + i * 1000 <= pit
        })
        .map(|s| (s.partition, s.offset))
        .collect();
    assert_eq!(restored_at_pit.len(), 6);
    assert_eq!(
        keys(&e2e_pit),
        with_class("extra:outside-model", &restored_at_pit),
        "point in time, end to end: {:#?}",
        render(&e2e_pit)
    );
    mutants_are_caught(
        "lat",
        |o| keys(&end_to_end(&source, &source, |_| true, o)),
        &full.observed,
        &want_e2e,
    );
}

// ============================================================ SHAPES

/// `Some(bytes)` with the literal's array type erased, so byte-string
/// literals of different lengths fit one `Option<&[u8]>` parameter.
fn some(b: &[u8]) -> Option<&[u8]> {
    Some(b)
}

fn shapes_fixture() -> Vec<Out> {
    let hdr = |pairs: &[(&str, Option<&[u8]>)]| -> Headers {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.map(<[u8]>::to_vec)))
            .collect()
    };
    let o = |p: i32, i: i64, key: Option<&[u8]>, value: Option<&[u8]>, headers: Headers| Out {
        partition: p,
        key: key.map(<[u8]>::to_vec),
        value: value.map(<[u8]>::to_vec),
        headers,
        timestamp: Some(T + i * 10),
    };
    let lineage_777 = 777i64.to_le_bytes();
    let mut v = vec![
        o(0, 0, some(b"k-plain"), some(b"v-plain"), vec![]),
        o(0, 1, None, some(b"v-null-key"), vec![]),
        o(0, 2, some(b"k-tombstone"), None, vec![]),
        o(0, 3, some(b""), some(b""), vec![]),
        o(
            0,
            4,
            some(b"k-dup-headers"),
            some(b"v"),
            hdr(&[
                ("h", some(b"a")),
                ("x", some(b"1")),
                ("h", some(b"b")),
                ("h", some(b"c")),
            ]),
        ),
        o(
            0,
            5,
            some(b"k-null-headers"),
            some(b"v"),
            hdr(&[("n", None), ("e", some(b""))]),
        ),
        o(
            0,
            6,
            some(b"k-own-lineage"),
            some(b"v"),
            hdr(&[(X_ORIGINAL_OFFSET, some(&lineage_777))]),
        ),
        o(
            0,
            7,
            some(&[0x00, 0xff, 0x00]),
            some(&[0xde, 0xad, 0x00, 0xbe, 0xef]),
            vec![],
        ),
    ];
    for i in 0..4 {
        let val = format!("{i}");
        v.push(o(1, i, some(b"k-order"), some(val.as_bytes()), vec![]));
    }
    for i in 0..3 {
        let key = format!("k-equal-ts-{i}");
        v.push(Out {
            timestamp: Some(T + 500),
            ..o(2, 0, some(key.as_bytes()), some(b"same instant"), vec![])
        });
    }
    v
}

/// Keys, nulls, empties, tombstones, duplicate headers, a record that already
/// carries `x-original-offset`, binary bytes, one key's order, equal
/// timestamps.
///
/// Contract (the engine holds headers in an `IndexMap` when it decodes a
/// fetch and when it encodes a produce):
/// * capture: a repeated header key keeps its first position and its last
///   value; everything else is exact;
/// * replay: the record whose own `x-original-offset` was archived beside the
///   engine's collapses the same way, losing the source's value;
/// * end to end: those two records' headers, nothing else.
#[test]
fn keys_nulls_tombstones_and_duplicate_headers() {
    let mut row = Row::new("shapes");
    let topic = row.source_topic("shapes", &[("message.timestamp.type", "CreateTime")]);
    let fixture = shapes_fixture();
    kafka::produce_plain(&topic, &fixture).expect("produce");
    let source = kafka::read_topic(&topic, PARTS, Isolation::Committed).expect("source");
    assert_fixture_landed("shapes", &fixture, &source);
    let backup_id = row.backup_id("shapes");
    backup_ok(&backup_id, &[&topic], 1000);
    let archive = kafka::read_archive(&backup_id, &topic).expect("archive");
    let r = restore(
        &mut row,
        "full",
        &backup_id,
        &topic,
        None,
        (T - 1000, T + 10_000),
    );
    // PROD-08.1 (acceptance row 08-6): complete coverage compares headers in
    // order, with every occurrence.
    let rc = restore_complete(
        &mut row,
        "cfull",
        &backup_id,
        &topic,
        None,
        (T - 1000, T + 10_000),
    );
    let cap = capture(&source, &archive);
    let rep = replay(&archive, |_| true, &r.observed);
    let e2e = end_to_end(&source, &source, |_| true, &r.observed);
    let rep_c = replay(&archive, |_| true, &rc.observed);
    let find = |recs: &[Rec], p: i32, off: i64, by_lineage: bool| -> Value {
        recs.iter()
            .find(|x| {
                x.partition == p
                    && if by_lineage {
                        x.lineage_header() == Some(off)
                    } else {
                        x.offset == off
                    }
            })
            .map(|x| json!(render_headers(&x.headers)))
            .unwrap_or(Value::Null)
    };
    record_outcome(
        "shapes",
        &source,
        &source,
        &archive,
        &[
            ("full", &r, &rep[..], &e2e[..]),
            ("complete full", &rc, &rep_c[..], &[][..]),
        ],
        &cap,
        json!({
            "p0@4 headers": {"source": find(&source, 0, 4, false), "archive": find(&archive.records, 0, 4, false), "target": find(&r.observed, 0, 4, true)},
            "p0@6 headers": {"source": find(&source, 0, 6, false), "archive": find(&archive.records, 0, 6, false), "target": find(&r.observed, 0, 6, true)},
        }),
    );
    if !contract_applies("shapes") {
        return;
    }
    // Logweir fails this restore on p0@6 alone (its archive fingerprint holds
    // two x-original-offset headers, the output one); p0@4's loss happened at
    // capture, so the drill does not detect it.
    assert_verdict("shapes", &r, 2, "fail-integrity");
    // PROD-08.1: complete coverage fails the same record, p0@6 (its archive
    // holds two x-original-offset headers, the output one), and only it:
    // p0@4's loss is in the archive too (V3).
    assert_verdict("shapes complete", &rc, 2, "fail-integrity");
    let b = complete_block(&rc);
    assert_eq!(
        (
            &b["replay"]["mismatched"],
            &b["replay"]["missing"],
            &b["replay"]["unexpected"]
        ),
        (&json!(1), &json!(0), &json!(0)),
        "{b}"
    );
    assert!(
        complete_findings(&rc).contains("p0: source offset 6 at target offset"),
        "{}",
        complete_findings(&rc)
    );
    assert_eq!(
        rc.verdict["integrity"]["verification"]["header_order"], "verified",
        "a complete verification compares headers in order"
    );
    assert_eq!(
        keys(&cap),
        with_class("headers-collapsed", &[(0, 4)]),
        "capture: {:#?}",
        render(&cap)
    );
    assert_eq!(
        keys(&rep),
        with_class("headers-collapsed", &[(0, 6)]),
        "replay: {:#?}",
        render(&rep)
    );
    let want = with_class("headers-collapsed", &[(0, 4), (0, 6)]);
    assert_eq!(keys(&e2e), want, "end to end: {:#?}", render(&e2e));
    mutants_are_caught(
        "shapes",
        |o| keys(&end_to_end(&source, &source, |_| true, o)),
        &r.observed,
        &want,
    );
}

// ========================================================== COMPACTION

/// A compacted source whose older values were removed by the log cleaner
/// before the backup, with a tombstone retained.
///
/// Contract: the archive and the output are the compacted log exactly
/// (sparse source offsets in `x-original-offset`, the tombstone as a null
/// value), and the output topic is NOT compacted: Logweir creates targets
/// with `message.timestamp.type` and `retention.ms` only.
#[test]
fn compacted_topic_committed_input_versus_restored_output() {
    let mut row = Row::new("compact");
    let topic = row.source_topic(
        "compact",
        &[
            ("message.timestamp.type", "CreateTime"),
            ("cleanup.policy", "compact"),
            ("segment.ms", "100"),
            ("min.cleanable.dirty.ratio", "0.01"),
            ("min.compaction.lag.ms", "0"),
            ("delete.retention.ms", "86400000"),
        ],
    );
    let mut fixture = Vec::new();
    for p in 0..PARTS {
        for (i, (k, v)) in [
            ("k1", Some("v1")),
            ("k2", Some("v1")),
            ("k1", Some("v2")),
            ("k3", Some("v1")),
            ("k2", None),
            ("k1", Some("v3")),
        ]
        .iter()
        .enumerate()
        {
            fixture.push(Out {
                partition: p,
                key: Some(k.as_bytes().to_vec()),
                value: v.map(|s| s.as_bytes().to_vec()),
                headers: Vec::new(),
                timestamp: Some(T + i as i64 * 10),
            });
        }
    }
    kafka::produce_plain(&topic, &fixture).expect("produce");
    // A record one hour later rolls the active segment (time-based rolling
    // compares record timestamps), so the cleaner may compact the first.
    let rollers: Vec<Out> = (0..PARTS)
        .map(|p| Out::kv(p, Some(T + 3_600_000), "k9", "roll"))
        .collect();
    kafka::produce_plain(&topic, &rollers).expect("roll");

    // Wait, bounded, until the cleaner has removed offsets 0, 1 and 2 on
    // every partition (k1=v1, k2=v1, k1=v2 are superseded).
    let deadline = Instant::now() + Duration::from_secs(180);
    let source = loop {
        let s = kafka::read_topic(&topic, PARTS, Isolation::Committed).expect("source");
        let compacted = (0..PARTS).all(|p| {
            let offs: Vec<i64> = s
                .iter()
                .filter(|r| r.partition == p)
                .map(|r| r.offset)
                .collect();
            offs == vec![3, 4, 5, 6]
        });
        if compacted {
            break s;
        }
        assert!(
            Instant::now() < deadline,
            "the log cleaner did not compact {topic} within 180 s; last reading: {:?}",
            s.iter()
                .map(|r| (r.partition, r.offset))
                .collect::<Vec<_>>()
        );
        std::thread::sleep(Duration::from_secs(3));
    };
    let backup_id = row.backup_id("compact");
    backup_ok(&backup_id, &[&topic], 1000);
    let after = kafka::read_topic(&topic, PARTS, Isolation::Committed).expect("source again");
    assert_eq!(
        after, source,
        "the compacted source changed during the backup"
    );
    let archive = kafka::read_archive(&backup_id, &topic).expect("archive");
    let r = restore(
        &mut row,
        "full",
        &backup_id,
        &topic,
        None,
        archive_span(&archive),
    );
    // PROD-08.1: complete coverage over the compacted archive's sparse
    // source offsets.
    let rc = restore_complete(
        &mut row,
        "cfull",
        &backup_id,
        &topic,
        None,
        archive_span(&archive),
    );
    let cap = capture(&source, &archive);
    let rep = replay(&archive, |_| true, &r.observed);
    let e2e = end_to_end(&source, &source, |_| true, &r.observed);
    let rep_c = replay(&archive, |_| true, &rc.observed);
    let target_configs = if topic_exists(&r.target) {
        logweir_kafka::reader::ClusterReader::topic_configs(&reader(), &r.target)
            .map(|m| json!(m))
            .unwrap_or_else(|e| json!(format!("{e}")))
    } else {
        Value::Null
    };
    let tombstones = r.observed.iter().filter(|x| x.value.is_none()).count();
    record_outcome(
        "compact",
        &source,
        &source,
        &archive,
        &[
            ("full", &r, &rep[..], &e2e[..]),
            ("complete full", &rc, &rep_c[..], &[][..]),
        ],
        &cap,
        json!({
            "source_offsets": source.iter().map(|x| (x.partition, x.offset)).collect::<Vec<_>>(),
            "target_configs": target_configs,
            "target_tombstones": tombstones,
        }),
    );
    if !contract_applies("compact") {
        return;
    }
    let exact = BTreeSet::new();
    assert_verdict("compact", &r, 0, "pass");
    // PROD-08.1: the compacted log is restored exactly as archived; complete
    // coverage passes it, every partition compared.
    assert_verdict("compact complete", &rc, 0, "pass");
    let b = complete_block(&rc);
    assert_eq!(b["covered"], true, "{b}");
    assert_eq!(b["replay"]["expected"], json!(source.len()), "{b}");
    assert_eq!(keys(&cap), exact, "capture: {:#?}", render(&cap));
    assert_eq!(keys(&rep), exact, "replay: {:#?}", render(&rep));
    assert_eq!(keys(&e2e), exact, "end to end: {:#?}", render(&e2e));
    assert_eq!(
        tombstones, PARTS as usize,
        "one tombstone per partition survives"
    );
    assert_ne!(
        target_configs["cleanup.policy"].as_str(),
        Some("compact"),
        "the restored topic is not compacted: {target_configs}"
    );
    mutants_are_caught(
        "compact",
        |o| keys(&end_to_end(&source, &source, |_| true, o)),
        &r.observed,
        &exact,
    );
}

// ========================================================= RECREATION

/// Recursively, every object key in a JSON document.
fn json_keys(v: &Value, out: &mut BTreeSet<String>) {
    match v {
        Value::Object(m) => {
            for (k, x) in m {
                out.insert(k.clone());
                json_keys(x, out);
            }
        }
        Value::Array(a) => a.iter().for_each(|x| json_keys(x, out)),
        _ => {}
    }
}

/// A topic deleted and recreated under the same name between two backups.
///
/// Contract: each archive restores its own generation exactly; the two
/// generations reuse the same source offsets and nothing in either manifest
/// identifies the generation; a second backup under the first archive's
/// `backup_id` is refused (RECEIPT-DUP) and leaves that manifest unchanged.
#[test]
fn recreated_topic_between_two_backups() {
    let mut row = Row::new("recreate");
    let topic = row.source_topic("recreate", &[("message.timestamp.type", "CreateTime")]);
    let gen = |g: i64, n: i64| -> Vec<Out> {
        (0..PARTS)
            .flat_map(|p| {
                (0..n).map(move |i| {
                    Out::kv(
                        p,
                        Some(T + g * 100_000 + i * 100),
                        &format!("g{g}-p{p}-{i}"),
                        &format!("generation {g} p{p} #{i}"),
                    )
                })
            })
            .collect()
    };
    let g1 = gen(1, 4);
    kafka::produce_plain(&topic, &g1).expect("gen 1");
    let src1 = kafka::read_topic(&topic, PARTS, Isolation::Committed).expect("gen 1 source");
    assert_fixture_landed("gen 1", &g1, &src1);
    let b1 = row.backup_id("g1");
    backup_ok(&b1, &[&topic], 1000);
    let m1_before = kafka::manifest_bytes(&b1).expect("gen 1 manifest");

    let _ = kafka_topics(&[
        "--bootstrap-server",
        "kafka-broker-1:9094",
        "--delete",
        "--topic",
        &topic,
    ]);
    let deadline = Instant::now() + Duration::from_secs(60);
    while topic_exists(&topic) {
        assert!(
            Instant::now() < deadline,
            "{topic} still present 60 s after --delete"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
    create_topic_for_fixed_timestamps(&topic, PARTS, &[("message.timestamp.type", "CreateTime")]);
    let g2 = gen(2, 2);
    kafka::produce_plain(&topic, &g2).expect("gen 2");
    let src2 = kafka::read_topic(&topic, PARTS, Isolation::Committed).expect("gen 2 source");
    assert_fixture_landed("gen 2", &g2, &src2);
    let b2 = row.backup_id("g2");
    backup_ok(&b2, &[&topic], 1000);

    // The first archive's backup_id again, over the recreated topic.
    let again = kafka::backup_run(&b1, &[&topic], 1000);
    let again_text = format!("{}{}", again.stdout_utf8(), again.stderr_utf8());
    let m1_after = kafka::manifest_bytes(&b1).expect("gen 1 manifest after");

    let a1 = kafka::read_archive(&b1, &topic).expect("archive 1");
    let a2 = kafka::read_archive(&b2, &topic).expect("archive 2");
    let r1 = restore(&mut row, "g1", &b1, &topic, None, archive_span(&a1));
    let r2 = restore(&mut row, "g2", &b2, &topic, None, archive_span(&a2));
    let e1 = end_to_end(&src1, &src1, |_| true, &r1.observed);
    let e2 = end_to_end(&src2, &src2, |_| true, &r2.observed);
    let rep1 = replay(&a1, |_| true, &r1.observed);
    let rep2 = replay(&a2, |_| true, &r2.observed);
    let cap1 = capture(&src1, &a1);
    let cap2 = capture(&src2, &a2);

    let lineage = |recs: &[Rec]| -> BTreeSet<(i32, i64)> {
        recs.iter()
            .filter_map(|x| x.lineage_header().map(|l| (x.partition, l)))
            .collect()
    };
    let shared: Vec<(i32, i64)> = lineage(&r1.observed)
        .intersection(&lineage(&r2.observed))
        .cloned()
        .collect();
    // Both generations' manifests (review L7b).
    let mut mk = BTreeSet::new();
    json_keys(&a1.manifest, &mut mk);
    json_keys(&a2.manifest, &mut mk);
    let identity_fields: Vec<&String> = mk
        .iter()
        .filter(|k| {
            let k = k.to_ascii_lowercase();
            k.contains("topic_id")
                || k.contains("uuid")
                || k.contains("generation")
                || k.contains("incarnation")
        })
        .collect();
    record_outcome(
        "recreate",
        &src2,
        &src2,
        &a2,
        &[
            ("g1", &r1, &rep1[..], &e1[..]),
            ("g2", &r2, &rep2[..], &e2[..]),
        ],
        &cap2,
        json!({
            "gen1_capture": summary_json(&cap1),
            "second_run_same_backup_id": {
                "exit": again.status.code(),
                "names_execution_already_claimed": again_text.contains("ExecutionAlreadyClaimed"),
                "manifest_unchanged": m1_before == m1_after,
            },
            "lineage_offsets_in_both_generations": shared,
            "manifest_keys": mk,
            "manifest_identity_fields": identity_fields,
        }),
    );
    if !contract_applies("recreate") {
        return;
    }
    let exact = BTreeSet::new();
    assert_verdict("recreate g1", &r1, 0, "pass");
    assert_verdict("recreate g2", &r2, 0, "pass");
    for (what, d) in [
        ("gen 1 capture", &cap1),
        ("gen 2 capture", &cap2),
        ("gen 1 replay", &rep1),
        ("gen 2 replay", &rep2),
        ("gen 1 end to end", &e1),
        ("gen 2 end to end", &e2),
    ] {
        assert_eq!(keys(d), exact, "{what}: {:#?}", render(d));
    }
    assert_eq!(
        again.status.code(),
        Some(1),
        "a second run under {b1} must be refused: {again_text}"
    );
    assert!(
        again_text.contains("ExecutionAlreadyClaimed"),
        "{again_text}"
    );
    assert!(
        m1_before == m1_after,
        "the refused run must not touch {b1}'s manifest"
    );
    assert_eq!(
        shared.len(),
        (PARTS * 2) as usize,
        "generation 2's offsets 0 and 1 reuse generation 1's on every partition"
    );
    assert!(
        identity_fields.is_empty(),
        "neither generation's manifest names a topic identity: {identity_fields:?}"
    );
    // The same (partition, x-original-offset) names different records in the
    // two outputs, and only the payload tells them apart.
    for (p, l) in &shared {
        let v1 = r1
            .observed
            .iter()
            .find(|x| x.partition == *p && x.lineage_header() == Some(*l));
        let v2 = r2
            .observed
            .iter()
            .find(|x| x.partition == *p && x.lineage_header() == Some(*l));
        assert_ne!(v1.map(|x| &x.value), v2.map(|x| &x.value), "p{p}@{l}");
    }
    mutants_are_caught(
        "recreate",
        |o| keys(&end_to_end(&src2, &src2, |_| true, o)),
        &r2.observed,
        &exact,
    );
}

// ============================================== ACKNOWLEDGEMENT FAULTS

/// **Fault injection around a produce acknowledgement.** The broker is frozen
/// (`docker compose pause`) while the engine is producing a restore, for
/// longer than the engine's 60 s response timeout
/// (`kafka/client.rs RESPONSE_TIMEOUT_SECS`), then thawed. The engine
/// classifies "timed out" as a connection error and resends the same batch
/// non-idempotently (`kafka/partition_router.rs:500-551`); whether the first
/// copy was appended as well is what this row measures, and records.
///
/// A single-node KRaft broker frozen that long is also fenced and re-registers
/// when it thaws, so the resend can meet a partition with no leader. The
/// outcome therefore varies between runs (PROD-01.1 §5.1 records three), and
/// the row asserts only what must hold in every one: the fault was injected,
/// and Logweir never signs `pass` over an output that differs from the
/// committed input.
///
/// `#[ignore]`: it stops the shared broker for over a minute. Run it alone:
/// `… --test record_semantics -- --ignored --exact
/// a_lost_produce_acknowledgement_during_restore`.
#[test]
#[ignore = "freezes the shared compose broker for over 60 s; run alone with --ignored"]
fn a_lost_produce_acknowledgement_during_restore() {
    let mut row = Row::new("ack-fault");
    let topic = row.source_topic("ack", &[("message.timestamp.type", "CreateTime")]);
    const PER_PARTITION: i64 = 20_000;
    let pad = "x".repeat(200);
    let fixture: Vec<Out> = (0..PARTS)
        .flat_map(|p| {
            let pad = pad.clone();
            (0..PER_PARTITION).map(move |i| {
                Out::kv(
                    p,
                    Some(T + i),
                    &format!("ack-p{p}-{i}"),
                    &format!("{i} {pad}"),
                )
            })
        })
        .collect();
    kafka::produce_plain(&topic, &fixture).expect("produce");
    let source = kafka::read_topic(&topic, PARTS, Isolation::Committed).expect("source");
    assert_eq!(source.len() as i64, PER_PARTITION * PARTS as i64);
    let backup_id = row.backup_id("ack");
    backup_ok(&backup_id, &[&topic], 1000);
    let archive = kafka::read_archive(&backup_id, &topic).expect("archive");
    let (prefix, target) = row.target("fault", &topic);
    let spec = restore_spec(&backup_id, &topic, &prefix, None, archive_span(&archive));

    // The restore runs on its own thread; this one watches the target and
    // freezes the broker as soon as the first records have landed. Polled
    // every 10 ms once the target exists (the first version of this row
    // polled slowly and froze anywhere between 5,000 and 57,000 records).
    let handle = std::thread::spawn(move || {
        let mut o = RunOpts::new(&spec);
        o.restore_run = true;
        let r = run_with(o);
        verdict(&r)
    });
    let total = PER_PARTITION * PARTS as i64;
    let deadline = Instant::now() + Duration::from_secs(900);
    let mut landed_at_pause = None;
    let mut target_seen = false;
    while Instant::now() < deadline && !handle.is_finished() {
        if !target_seen {
            target_seen = topic_exists(&target);
        }
        if target_seen {
            if let Ok(w) = kafka::high_watermarks(&target, PARTS) {
                let n: i64 = w.iter().map(|(_, h)| h).sum();
                if n > 0 && n < total {
                    landed_at_pause = Some(n);
                    break;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let paused_for = Duration::from_secs(75);
    // Armed before the pause: whatever happens next, the broker is thawed.
    let mut thaw = kafka::Thaw { armed: false };
    let pause_result = landed_at_pause.map(|n| {
        thaw.armed = true;
        let p = kafka::compose_broker("pause");
        let t0 = Instant::now();
        std::thread::sleep(paused_for);
        let u = kafka::compose_broker("unpause");
        thaw.armed = false;
        eprintln!(
            "[recsem] ack-fault: paused the broker at {n}/{total} restored records for {:?} \
             (pause exit {:?}, unpause exit {:?})",
            t0.elapsed(),
            p.status.code(),
            u.status.code()
        );
        (n, p.status.code(), u.status.code())
    });
    let v = join_within(handle, RESTORE_DEADLINE_SECS, "the restore thread");
    // Logweir's own account first, so a failed read below cannot lose it.
    kafka::write_json(
        &demo_dir()
            .join("record-semantics")
            .join("ack-fault-logweir.json"),
        &json!({"paused_after_records": pause_result.map(|x| x.0), "logweir": v}),
    );
    // A single-node KRaft broker frozen past its session timeout is fenced
    // and re-registers when it thaws, so for a moment its partitions have no
    // leader. Read the target once leadership is back (bounded).
    let deadline = Instant::now() + Duration::from_secs(180);
    let observed = loop {
        match kafka::read_topic(&target, PARTS, Isolation::Committed) {
            Ok(o) => break o,
            Err(e) if Instant::now() < deadline => {
                eprintln!("[recsem] ack-fault: target not readable yet ({e}); retrying");
                std::thread::sleep(Duration::from_secs(2));
            }
            Err(e) => panic!("target unreadable for 180 s after the thaw: {e}"),
        }
    };
    let e2e = end_to_end(&source, &source, |_| true, &observed);
    let dups: Vec<&Divergence> = e2e
        .iter()
        .filter(|d| matches!(d, Divergence::Duplicate { .. }))
        .collect();
    let copies_total: usize = e2e
        .iter()
        .map(|d| match d {
            Divergence::Duplicate { copies, .. } => copies - 1,
            _ => 0,
        })
        .sum();
    let summary = summarize(&e2e);
    // Per partition: the source-offset range and count of each class, so a
    // duplicate can be read as whole batches (the engine produces 1,000
    // records per request).
    let ranges = |pick: &dyn Fn(&Divergence) -> Option<(i32, i64)>| -> Value {
        let mut m: std::collections::BTreeMap<i32, (i64, i64, u64)> = Default::default();
        for d in &e2e {
            if let Some((p, o)) = pick(d) {
                let e = m.entry(p).or_insert((i64::MAX, i64::MIN, 0));
                e.0 = e.0.min(o);
                e.1 = e.1.max(o);
                e.2 += 1;
            }
        }
        json!(m
            .into_iter()
            .map(|(p, (lo, hi, n))| json!({"partition": p, "from": lo, "to": hi, "count": n}))
            .collect::<Vec<_>>())
    };
    let duplicate_ranges = ranges(&|d| match d {
        Divergence::Duplicate {
            partition,
            source_offset,
            ..
        } => Some((*partition, *source_offset)),
        _ => None,
    });
    let missing_ranges = ranges(&|d| match d {
        Divergence::Missing {
            partition,
            source_offset,
        } => Some((*partition, *source_offset)),
        _ => None,
    });
    eprintln!(
        "[recsem] ack-fault: target={} source={} end to end {summary:?}; logweir {v}",
        observed.len(),
        source.len()
    );
    kafka::write_json(
        &demo_dir().join("record-semantics").join("ack-fault.json"),
        &json!({
            "row": "ack-fault",
            "engine_version": engine_version(),
            "records_per_partition": PER_PARTITION,
            "paused_after_records": pause_result.map(|x| x.0),
            "pause_exit": pause_result.map(|x| x.1),
            "unpause_exit": pause_result.map(|x| x.2),
            "pause_seconds": paused_for.as_secs(),
            "target_records": observed.len(),
            "source_records": source.len(),
            "duplicated_source_offsets": dups.len(),
            "extra_copies": copies_total,
            "duplicate_ranges": duplicate_ranges,
            "missing_ranges": missing_ranges,
            "end_to_end": { "by_class": summary, "first": render(&e2e).into_iter().take(40).collect::<Vec<_>>() },
            "logweir": v,
        }),
    );
    assert!(
        pause_result.is_some(),
        "the restore finished before the first records could be observed; raise PER_PARTITION"
    );
    // The fault was injected: the pause happened mid-restore and both compose
    // verbs succeeded (review L5c).
    let (_, pause_exit, unpause_exit) = pause_result.expect("checked above");
    assert_eq!(
        (pause_exit, unpause_exit),
        (Some(0), Some(0)),
        "docker compose pause/unpause must both succeed"
    );
    // What holds in every sample, whatever the timing: Logweir never signs
    // `pass` over an output that differs from the committed input, and an
    // operational exit (1) leaves no scorecard claiming anything.
    if v["outcome"].as_str() == Some("pass") {
        assert!(
            e2e.is_empty(),
            "Logweir signed pass over a divergent output: {summary:?}"
        );
    }
    if v["exit"].as_i64() == Some(1) {
        assert!(
            v["outcome"].is_null(),
            "an operational failure must not come with a scorecard: {}",
            v["outcome"]
        );
    }
}

/// **Why termination-based fault injection is blocked (`docs/stability.md`
/// Later #13).** `logweir restore run` is killed with SIGKILL as soon as the
/// first restored record lands, and the row then reads, without waiting, which
/// engine containers are alive and how many records have landed, and keeps
/// sampling the target for 20 s. If the engine is alive after its parent died
/// and the target keeps growing, the writer outlived the process that owned
/// it, and "kill before or after an acknowledgement" cannot be injected
/// through Logweir until a cancel reaches the engine.
///
/// It finds ONLY this worktree's processes and containers: the `logweir`
/// command line names this worktree's `.e2e/` spec (resolved BEFORE the engine
/// starts, so the kill costs one `kill` call), and the engine container is the
/// one bind-mounting this worktree's `harness::engine_mount()`. Both are
/// killed before the row ends, whatever it observed.
#[test]
#[ignore = "kills a restore mid-run and the engine container it leaves behind; run alone with --ignored"]
fn a_killed_restore_leaves_its_engine_writing() {
    let mut row = Row::new("kill");
    let topic = row.source_topic("kill", &[("message.timestamp.type", "CreateTime")]);
    const PER_PARTITION: i64 = 20_000;
    let pad = "y".repeat(200);
    let fixture: Vec<Out> = (0..PARTS)
        .flat_map(|p| {
            let pad = pad.clone();
            (0..PER_PARTITION).map(move |i| {
                Out::kv(
                    p,
                    Some(T + i),
                    &format!("kill-p{p}-{i}"),
                    &format!("{i} {pad}"),
                )
            })
        })
        .collect();
    kafka::produce_plain(&topic, &fixture).expect("produce");
    let backup_id = row.backup_id("kill");
    backup_ok(&backup_id, &[&topic], 1000);
    let archive = kafka::read_archive(&backup_id, &topic).expect("archive");
    let (prefix, target) = row.target("killed", &topic);
    let spec = restore_spec(&backup_id, &topic, &prefix, None, archive_span(&archive));
    let handle = std::thread::spawn(move || {
        let mut o = RunOpts::new(&spec);
        o.restore_run = true;
        verdict(&run_with(o))
    });
    let total = PER_PARTITION * PARTS as i64;
    let landed = |t: &str| -> i64 {
        kafka::high_watermarks(t, PARTS)
            .map(|w| w.iter().map(|(_, h)| h).sum())
            .unwrap_or(0)
    };
    let run = |program: &str, args: &[&str], secs: u64| -> String {
        let mut c = std::process::Command::new(program);
        c.args(args);
        kafka::output_within(c, secs)
            .map(|o| o.stdout_utf8())
            .unwrap_or_default()
    };
    let mount = engine_mount().display().to_string();
    let volume = format!("volume={mount}");
    let containers = || -> Vec<String> {
        run("docker", &["ps", "-q", "--filter", &volume], 30)
            .split_whitespace()
            .map(str::to_string)
            .collect()
    };

    // 1. The logweir PID, found while phases 0-5 run, before the engine
    //    produces anything.
    let pattern = format!("logweir restore run --spec {}/drill-", demo_dir().display());
    let deadline = Instant::now() + Duration::from_secs(300);
    let mut pids: Vec<String> = Vec::new();
    while pids.is_empty() && Instant::now() < deadline && !handle.is_finished() {
        pids = run("pgrep", &["-f", &pattern], 20)
            .split_whitespace()
            .map(str::to_string)
            .collect();
        if pids.is_empty() {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    // 2. The first landed record, polled every 10 ms once the target exists.
    let deadline = Instant::now() + Duration::from_secs(900);
    let mut at_kill = None;
    let mut target_seen = false;
    while Instant::now() < deadline && !handle.is_finished() && !pids.is_empty() {
        if !target_seen {
            target_seen = topic_exists(&target);
        }
        if target_seen {
            let n = landed(&target);
            if n > 0 && n < total {
                at_kill = Some(n);
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    // 3. Kill, then read the engine's containers and the target at once.
    for pid in &pids {
        run("kill", &["-KILL", pid], 20);
    }
    let killed_at = Instant::now();
    let alive_after_kill = containers();
    let landed_after_kill = landed(&target);
    // 4. Growth after the parent is gone, once a second for 20 s.
    let mut samples: Vec<(u64, i64)> = Vec::new();
    for _ in 0..20 {
        std::thread::sleep(Duration::from_secs(1));
        samples.push((killed_at.elapsed().as_millis() as u64, landed(&target)));
    }
    let alive_20s_later = containers();
    for id in &alive_20s_later {
        run("docker", &["kill", id], 60);
    }
    let left = containers();
    let v = join_within(handle, RESTORE_DEADLINE_SECS, "the restore thread");
    kafka::write_json(
        &demo_dir().join("record-semantics").join("kill.json"),
        &json!({
            "row": "kill",
            "records_total": total,
            "logweir_pids_killed": pids,
            "landed_when_killed": at_kill,
            "engine_containers_alive_right_after_kill": alive_after_kill,
            "landed_right_after_kill": landed_after_kill,
            "landed_samples_ms_after_kill": samples,
            "engine_containers_alive_20s_after_kill": alive_20s_later,
            "engine_containers_left_after_cleanup": left,
            "logweir": v,
        }),
    );
    eprintln!(
        "[recsem] kill: landed {at_kill:?} at kill, {landed_after_kill} right after, samples \
         {samples:?}; engine containers alive after kill {alive_after_kill:?}, 20 s later \
         {alive_20s_later:?}, left {left:?}"
    );
    assert!(
        !pids.is_empty(),
        "no logweir restore process was found to kill"
    );
    assert!(
        at_kill.is_some(),
        "the restore finished before it could be killed"
    );
    // The claim this row exists for (review L5d): the engine outlived the
    // process that owned it, and the target reached the whole archive after
    // that process was gone.
    assert!(
        !alive_after_kill.is_empty(),
        "no engine container of this worktree was running right after the kill"
    );
    assert_eq!(
        samples.last().map(|(_, n)| *n),
        Some(total),
        "the orphaned engine completed the restore: {samples:?}"
    );
    assert!(
        left.is_empty(),
        "engine containers survived cleanup: {left:?}"
    );
}
