#![cfg(feature = "e2e")]
//! **PROD-11.1 — replay selection, observed on a real broker and archive.**
//!
//! Every row produces a deterministic fixture into its own source topics,
//! takes a backup with the SHIPPED `logweir backup run` and the pinned engine,
//! and restores it with `logweir restore run` (`target.mode: newTopic`) under
//! a plan that states a replay selection: an inclusive window START, written
//! as the interval form of `restore.point_in_time` (`"<start>/<end>"`). A
//! partition subset (`restore.partitions`) is REFUSED by name until the owner
//! decides OD-9 (`PartitionSubsetsAwaitOwnerDecision`). The outcome is decided
//! by THIS file's oracle, never by Logweir's exit status: the archive is read
//! with Logweir's own `kbak` decoder, the expected output is every archived
//! record whose OWN timestamp is in `[start, end]` — computed here,
//! independently of `logweir_core::replay_selection` — and the restored topic
//! is read back and mapped to source offsets by its last `x-original-offset`
//! header. Logweir's signed verdict and its `source.selection` block are then
//! asserted as part of the contract.
//!
//! | row | what it proves |
//! |---|---|
//! | `the_start_is_inclusive_…` | inclusive vs exclusive at the start; equal timestamps at it; non-monotonic timestamps across it inside one segment |
//! | `a_segment_whose_bounds_hide_…` | the engine's segment rule skips an in-window record whose segment's last record is before the start (PROD-01.1 S7 at the start): complete coverage fails it, never `pass` |
//! | `a_topic_subset_from_a_start_…` | a topic subset (C not restored) from a start, under BOTH coverages: the start-only block signed, both readers accept it, the scorecards kept for the older-reader rows |
//! | `refusals_before_anything_runs` | a start before coverage, an empty selection, an existing target name and a partition subset are exit 3 with no target topic and no signed scorecard |
//! | `a_compaction_hole_inside_a_sub_window_…` | a compacted source restored from a stated start: exact, the hole disclosed |
//! | `a_new_point_does_not_change_…` | a newer backup arriving after the plan was approved does not change what the plan restores |
//! | `an_older_runner_refuses_…` (ignored; needs `LOGWEIR_E2E_OLDER_RUNNER`) | main's runner from BEFORE PROD-11.1 refuses a plan stating a start (it cannot parse the interval), creates nothing and signs nothing; the same binary restores the same plan without the start (the control); and it IGNORES a hand-written `restore.partitions` (the residual the current runner closes by refusing it) |
//!
//! Each row's own check is shown able to fail: the observed output is mutated
//! (a record dropped, a record from outside the selection added) and the
//! oracle must reject it (`oracle_rejects_mutants`).
//!
//! # Running it (PROD-01.5 slot, e.g. 2, with the CI profile)
//!
//! ```text
//! eval "$(e2e/compose/stack-env.sh --slot 2 --profiles auth)"
//! just e2e-up
//! cargo build -p logweir
//! AWS_EC2_METADATA_DISABLED=true cargo test -p e2e --features e2e \
//!     --test replay_selection -- --test-threads=1 --nocapture
//! # the older-runner row, with a `logweir` built from main before PROD-11.1:
//! LOGWEIR_E2E_OLDER_RUNNER=/path/to/older/logweir AWS_EC2_METADATA_DISABLED=true \
//!     cargo test -p e2e --features e2e --test replay_selection \
//!     an_older_runner -- --ignored --test-threads=1 --nocapture
//! ```
//!
//! Each row writes `.e2e/<project>/replay-selection/<row>.json`; the
//! topic-subset row also keeps its two signed scorecards there
//! (`scorecard-<coverage>.json` and `.sig`).
mod harness;
mod record_semantics_support;

use harness::*;
use record_semantics_support::kafka::{self, Archive, Isolation, Out};
use record_semantics_support::oracle::{decode_le_i64, Rec};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The fixture epoch, `2025-10-09T08:53:20Z` (PROD-01.1's). A literal.
const T: i64 = 1_760_000_000_000;
/// Every fixture topic has three partitions.
const PARTS: i32 = 3;
/// Every topic, target and archive of this file starts with it.
const ID_PREFIX: &str = "repsel-";
/// How long one `logweir restore run` may take.
const RESTORE_DEADLINE_SECS: u64 = 900;

fn rfc3339(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .expect("a representable instant")
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

// ===================================================================== rows

/// One row's resources, released on every exit path.
struct Row {
    name: &'static str,
    nonce: String,
    topics: Vec<String>,
    archives: Vec<String>,
}

impl Row {
    fn new(name: &'static str) -> Row {
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

    fn source_topic(&mut self, suffix: &str, configs: &[(&str, &str)]) -> String {
        let t = self.name_for(suffix);
        self.topics.push(t.clone());
        let mut c = vec![("message.timestamp.type", "CreateTime")];
        c.extend_from_slice(configs);
        create_topic_for_fixed_timestamps(&t, PARTS, &c);
        t
    }

    fn backup_id(&mut self, suffix: &str) -> String {
        let b = self.name_for(&format!("{suffix}-b"));
        self.archives.push(b.clone());
        b
    }

    /// The `topicNaming.prefix` of one restore.
    fn prefix(&mut self, label: &str, sources: &[&str]) -> String {
        let prefix = self.name_for(&format!("{label}-"));
        for s in sources {
            self.topics.push(format!("{prefix}{s}"));
        }
        prefix
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
            eprintln!("[repsel] {}: sweep left {left:?}", self.name);
        }
    }
}

/// Remove this file's archives and receipts from the shared archive bucket.
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

/// `logweir backup run` of `backup_id` into the storage prefix `prefix` (not
/// its own id, as `kafka::backup_run` does), so two sets share one archive.
fn backup_into(prefix: &str, backup_id: &str, topics: &[&str]) {
    let spec = demo_dir().join(format!("{backup_id}-backup.yaml"));
    let allow = demo_dir().join("repsel-backup-allowed-clusters.json");
    std::fs::write(
        &allow,
        "{\"allowed_cluster_ids\": [\"SCRATCH-CLUSTER-NOT-THE-SOURCE\"]}\n",
    )
    .expect("the allowlist is writable");
    let (boot, endpoint) = (kafka::bootstrap(), kafka::s3_endpoint());
    std::fs::write(
        &spec,
        format!(
            "backup_id: {backup_id}\nsource:\n  bootstrap_servers: [{boot}]\n  topics: [{}]\n\
             storage:\n  backend: s3\n  bucket: {ARCHIVE_BUCKET}\n  prefix: {prefix}\n  \
             region: us-east-1\n  endpoint: {endpoint}\n  path_style: true\n  allow_http: true\n\
             backup:\n  compression: zstd\n  segment_max_records: 1000\n  \
             segment_max_bytes: 10485760\n  max_concurrent_partitions: 3\n",
            topics.join(", ")
        ),
    )
    .expect("the backup spec is writable");
    let (user, secret) = kafka::s3_credentials();
    let mut c = std::process::Command::new(bin());
    c.args(["backup", "run", "--spec"])
        .arg(&spec)
        .arg("--allowed-clusters")
        .arg(&allow)
        .arg("--signing-key")
        .arg(root().join("e2e/fixtures/signed/signing.pem"))
        .env("AWS_ACCESS_KEY_ID", user)
        .env("AWS_SECRET_ACCESS_KEY", secret)
        .env("AWS_REGION", "us-east-1")
        .env("LOGWEIR_ENGINE_BIN", engine_bin())
        .env("LOGWEIR_ENGINE_VERSION", engine_version())
        .env("LOGWEIR_ENGINE_DIGEST", engine_digest())
        .env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount())
        .env("TMPDIR", engine_mount());
    let o = kafka::output_within(c, 900).expect("logweir backup run");
    assert_eq!(
        o.status.code(),
        Some(0),
        "backup {backup_id} into {prefix}\nstdout:\n{}\nstderr:\n{}",
        o.stdout_utf8(),
        o.stderr_utf8()
    );
}

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

/// What a plan selects, stated in the plan and modelled by the oracle.
#[derive(Clone, Debug, Default)]
struct Selection {
    start: Option<i64>,
    end: i64,
    /// `topic -> partitions`, written as `restore.partitions`. Only the
    /// refusal rows set it: the current runner refuses any subset
    /// (`PartitionSubsetsAwaitOwnerDecision`, OD-9 open), and an older runner
    /// ignores the key. The oracle therefore never narrows by it.
    partitions: BTreeMap<String, Vec<i32>>,
}

impl Selection {
    fn selects(&self, ts: i64) -> bool {
        ts <= self.end && self.start.is_none_or(|s| ts >= s)
    }

    /// The plan's `restore:` block: a stated start is the INTERVAL form of
    /// `point_in_time`, `"<start>/<end>"` — the one grammar for it, which an
    /// older runner cannot parse and so refuses.
    fn restore_block(&self) -> String {
        let point = match self.start {
            Some(s) => format!("{}/{}", rfc3339(s), rfc3339(self.end)),
            None => rfc3339(self.end),
        };
        let mut b = format!("restore:\n  point_in_time: \"{point}\"\n");
        if !self.partitions.is_empty() {
            b.push_str("  partitions:\n");
            for (t, ps) in &self.partitions {
                let list: Vec<String> = ps.iter().map(i32::to_string).collect();
                b.push_str(&format!("    {t}: [{}]\n", list.join(", ")));
            }
        }
        b
    }
}

fn restore_spec(
    backup_id: &str,
    sources: &[&str],
    prefix: &str,
    sel: &Selection,
    sample: (i64, i64),
    complete: bool,
) -> serde_yaml::Value {
    let (boot, endpoint) = (kafka::bootstrap(), kafka::s3_endpoint());
    let coverage = if complete {
        "  coverage: complete\n"
    } else {
        ""
    };
    serde_yaml::from_str(&format!(
        "source:\n  storage:\n    backend: s3\n    bucket: {ARCHIVE_BUCKET}\n    prefix: {backup_id}\n    \
         region: us-east-1\n    endpoint: {endpoint}\n    path_style: true\n    allow_http: true\n  \
         backup: {backup_id}\n  topics: [{}]\n\
         target:\n  bootstrap_servers: [{boot}]\n  mode: newTopic\n  topic_mapping_prefix: \"drill-\"\n  \
         topic_naming:\n    prefix: \"{prefix}\"\n  default_replication_factor: 1\n\
         {}\
         sample:\n  window_start: \"{}\"\n  window_end: \"{}\"\n  records_per_partition: 25\n  \
         anchor: head\n{coverage}\
         objectives:\n  rto_seconds: 900\n\
         evidence:\n  backend: s3\n  bucket: {EVIDENCE_BUCKET}\n  prefix: logweir/\n  region: us-east-1\n  \
         endpoint: {endpoint}\n  path_style: true\n  allow_http: true\n",
        sources.join(", "),
        sel.restore_block(),
        rfc3339(sample.0),
        rfc3339(sample.1),
    ))
    .expect("the restore spec is valid YAML")
}

/// One finished `logweir restore run`: its exit, its signed scorecard (or
/// `Null`) and its output.
struct Restored {
    exit: Option<i32>,
    scorecard: Value,
    /// The signed bytes and their DSSE envelope, empty when nothing was
    /// signed: the topic-subset row keeps them for the older-reader rows.
    signed: (Vec<u8>, Vec<u8>),
    stdout: String,
    stderr: String,
}

impl Restored {
    fn outcome(&self) -> Option<&str> {
        self.scorecard["outcome"].as_str()
    }
    fn refusal(&self) -> Option<String> {
        format!("{}\n{}", self.stdout, self.stderr)
            .lines()
            .rev()
            .find(|l| l.contains("refusal-reason="))
            .map(str::to_string)
    }
    fn summary(&self) -> Value {
        json!({
            "exit": self.exit,
            "outcome": self.scorecard["outcome"],
            "format_version": self.scorecard["format_version"],
            "selection": self.scorecard["source"]["selection"],
            "sample": self.scorecard["sample"],
            "integrity": self.scorecard["integrity"],
            "phase6_notes": self.scorecard["phases"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|p| p["phase"] == 6)
                .map(|p| p["notes"].clone())
                .unwrap_or(Value::Null),
            "refusal": self.refusal(),
            "stderr_tail": self.stderr.lines().rev().take(15).collect::<Vec<_>>(),
        })
    }
}

fn restore_run(spec: serde_yaml::Value, pre_create: Vec<(String, i32)>) -> Restored {
    restore_run_by(spec, pre_create, None)
}

/// [`restore_run`] with the `logweir` binary named: `None` is this tree's.
fn restore_run_by(
    spec: serde_yaml::Value,
    pre_create: Vec<(String, i32)>,
    bin: Option<std::path::PathBuf>,
) -> Restored {
    let h = std::thread::spawn(move || {
        let mut o = RunOpts::new(&spec);
        o.restore_run = true;
        o.pre_create = pre_create;
        o.bin = bin;
        run_with(o)
    });
    let deadline = Instant::now() + Duration::from_secs(RESTORE_DEADLINE_SECS);
    while !h.is_finished() {
        assert!(
            Instant::now() < deadline,
            "logweir restore run: no result after {RESTORE_DEADLINE_SECS} s"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    let run = h.join().expect("the restore thread");
    let bytes = std::fs::read(&run.scorecard).unwrap_or_default();
    let scorecard = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    Restored {
        exit: run.out.status.code(),
        scorecard,
        signed: (bytes, std::fs::read(&run.sig).unwrap_or_default()),
        stdout: run.out.stdout_utf8(),
        stderr: run.out.stderr_utf8(),
    }
}

// ================================================================ the oracle

/// `(source offset, timestamp, key, value)`: what a restored record must
/// carry over from the archive.
type Cell = (i64, i64, Option<Vec<u8>>, Option<Vec<u8>>);
/// `partition -> cells in order`.
type Output = BTreeMap<i32, Vec<Cell>>;

/// Every archived record the selection selects, by its OWN timestamp, per
/// partition in source-offset order. Every partition of the topic appears, so
/// a partition with nothing in the window must be EMPTY.
fn expected(archive: &Archive, sel: &Selection) -> Output {
    let mut out: Output = (0..PARTS).map(|p| (p, Vec::new())).collect();
    for r in &archive.records {
        if sel.selects(r.timestamp) {
            out.entry(r.partition).or_default().push((
                r.offset,
                r.timestamp,
                r.key.clone(),
                r.value.clone(),
            ));
        }
    }
    out
}

/// The restored topic, each record mapped to its source offset by its LAST
/// `x-original-offset` header (a record without one maps to -1).
fn observed(target: &str) -> Output {
    let recs: Vec<Rec> = if topic_exists(target) {
        kafka::read_topic(target, PARTS, Isolation::Committed)
            .unwrap_or_else(|e| panic!("reading {target}: {e}"))
    } else {
        Vec::new()
    };
    let mut out: Output = (0..PARTS).map(|p| (p, Vec::new())).collect();
    for r in recs {
        let lineage = r
            .headers
            .iter()
            .rev()
            .find(|(k, _)| k == "x-original-offset")
            .and_then(|(_, v)| decode_le_i64(v.as_deref()))
            .unwrap_or(-1);
        out.entry(r.partition)
            .or_default()
            .push((lineage, r.timestamp, r.key, r.value));
    }
    out
}

/// Every difference between the expected and the observed output, in words.
fn diff(want: &Output, got: &Output) -> Vec<String> {
    let mut d = Vec::new();
    for p in 0..PARTS {
        let (w, g) = (
            want.get(&p).cloned().unwrap_or_default(),
            got.get(&p).cloned().unwrap_or_default(),
        );
        if w != g {
            let wo: Vec<(i64, i64)> = w.iter().map(|c| (c.0, c.1)).collect();
            let go: Vec<(i64, i64)> = g.iter().map(|c| (c.0, c.1)).collect();
            d.push(format!(
                "p{p}: expected (offset, ts) {wo:?}, restored {go:?}"
            ));
        }
    }
    d
}

/// The oracle can fail: a dropped record and a record in an unselected
/// partition (or below the start) are both reported.
fn oracle_rejects_mutants(want: &Output, got: &Output, foreign: Cell, foreign_partition: i32) {
    let mut dropped = got.clone();
    if let Some((_, cells)) = dropped.iter_mut().find(|(_, c)| !c.is_empty()) {
        cells.pop();
        assert!(!diff(want, &dropped).is_empty(), "a dropped record passed");
    }
    let mut extra = got.clone();
    extra.entry(foreign_partition).or_default().push(foreign);
    assert!(!diff(want, &extra).is_empty(), "a foreign record passed");
}

fn write_outcome(row: &str, v: &Value) {
    kafka::write_json(
        &demo_dir()
            .join("replay-selection")
            .join(format!("{row}.json")),
        v,
    );
}

/// `(partition, [timestamp offsets from base])`, one record per entry.
fn layout(base: i64, tag: &str, parts: &[(i32, &[i64])]) -> Vec<Out> {
    let mut v = Vec::new();
    for (p, deltas) in parts {
        for (i, d) in deltas.iter().enumerate() {
            v.push(Out::kv(
                *p,
                Some(base + d),
                &format!("{tag}-p{p}-{i}"),
                &format!("{tag} p{p} #{i} ts=+{d}"),
            ));
        }
    }
    v
}

fn signed_selection(r: &Restored) -> &Value {
    &r.scorecard["source"]["selection"]
}

// ====================================================================== rows

/// **Inclusive at the start, equal and non-monotonic timestamps across it.**
/// `S = T + 10 s`. p0 is monotonic with two records AT `S` and one a
/// millisecond either side; p1 is one segment whose records go back and forth
/// across `S` (first `S+300`, last `S+5`, so the segment overlaps the window);
/// p2 is wholly after `S`. Restored from `S` (complete coverage): the target
/// holds exactly the records with `ts >= S`, both records at `S` included,
/// `S-1` and `S-50` not, in source order. Restored from `S+1` (the exclusive
/// control): the two records at `S` are not restored. Both runs pass and sign
/// their start in `source.selection`, `complete.window` and
/// `sample.window_start`.
#[test]
fn the_start_is_inclusive_and_selects_equal_and_non_monotonic_timestamps() {
    let mut row = Row::new("inclusive");
    let s = T + 10_000;
    let topic = row.source_topic("inc", &[]);
    let fixture = layout(
        s,
        "inc",
        &[
            (0, &[-2000, -1, 0, 0, 1, 500]),
            (1, &[300, -50, 0, -1, 5]),
            (2, &[1000, 2000]),
        ],
    );
    kafka::produce_plain(&topic, &fixture).expect("produce");
    let backup_id = row.backup_id("inc");
    backup_ok(&backup_id, &[&topic], 1000);
    let archive = kafka::read_archive(&backup_id, &topic).expect("archive");
    assert_eq!(
        archive.records.len(),
        fixture.len(),
        "every record archived"
    );

    let mut runs = Vec::new();
    for (label, start) in [("at", s), ("after", s + 1)] {
        let sel = Selection {
            start: Some(start),
            end: s + 5_000,
            ..Selection::default()
        };
        let prefix = row.prefix(label, &[&topic]);
        let r = restore_run(
            restore_spec(
                &backup_id,
                &[&topic],
                &prefix,
                &sel,
                (start, s + 5_000),
                true,
            ),
            Vec::new(),
        );
        let want = expected(&archive, &sel);
        let got = observed(&format!("{prefix}{topic}"));
        let d = diff(&want, &got);
        runs.push(json!({"label": label, "start": start, "verdict": r.summary(), "diff": d}));
        write_outcome("inclusive", &json!({ "runs": runs }));
        assert!(
            d.is_empty(),
            "{label}: the restored output is not the selection: {d:#?}"
        );
        assert_eq!(
            (r.exit, r.outcome()),
            (Some(0), Some("pass")),
            "{label}: {}",
            r.summary()
        );
        assert_eq!(signed_selection(&r)["window_start_ms"], json!(start));
        assert_eq!(
            r.scorecard["integrity"]["verification"]["complete"]["window"]["start_ms"],
            json!(start)
        );
        let sample_start = chrono::DateTime::parse_from_rfc3339(
            r.scorecard["sample"]["window_start"].as_str().unwrap(),
        )
        .unwrap()
        .timestamp_millis();
        assert_eq!(
            sample_start, start,
            "sample.window_start names the selection"
        );
        let at_s = got[&0].iter().filter(|c| c.1 == s).count();
        assert_eq!(
            at_s,
            if start == s { 2 } else { 0 },
            "{label}: records AT S"
        );
        assert!(
            got.values().flatten().all(|c| c.1 >= start),
            "{label}: nothing below the start"
        );
        oracle_rejects_mutants(&want, &got, (1, s - 1, None, None), 0);
    }
}

/// **The engine's segment rule at the start (PROD-01.1 S7, mirrored).** p0's
/// first segment holds `[S-100, S+50, S-90, S-80]`: its LAST record is before
/// `S`, so the engine skips the whole segment and loses `S+50`, which is in
/// the window. Complete coverage computes the expected output from each
/// record's own timestamp and FAILS the restore (`missing`); it is never
/// signed `pass`. The oracle measures the same loss. This is the engine's
/// limit, PROD-01.1b's to fix; Logweir's job is not to sign it.
#[test]
fn a_segment_whose_bounds_hide_an_in_window_record_is_never_signed_pass() {
    let mut row = Row::new("hidden");
    let s = T + 10_000;
    let topic = row.source_topic("hid", &[]);
    let fixture = layout(
        s,
        "hid",
        &[
            (0, &[-100, 50, -90, -80, 100, 110, 120, 130]),
            (1, &[10, 20, 30, 40]),
            (2, &[10, 20, 30, 40]),
        ],
    );
    kafka::produce_plain(&topic, &fixture).expect("produce");
    let backup_id = row.backup_id("hid");
    backup_ok(&backup_id, &[&topic], 4);
    let archive = kafka::read_archive(&backup_id, &topic).expect("archive");
    let sel = Selection {
        start: Some(s),
        end: s + 5_000,
        ..Selection::default()
    };
    let prefix = row.prefix("c", &[&topic]);
    let r = restore_run(
        restore_spec(&backup_id, &[&topic], &prefix, &sel, (s, s + 5_000), true),
        Vec::new(),
    );
    let want = expected(&archive, &sel);
    let got = observed(&format!("{prefix}{topic}"));
    let d = diff(&want, &got);
    write_outcome("hidden", &json!({"verdict": r.summary(), "diff": d}));
    assert_eq!(
        d,
        vec![format!(
            "p0: expected (offset, ts) [(1, {}), (4, {}), (5, {}), (6, {}), (7, {})], restored \
             [(4, {}), (5, {}), (6, {}), (7, {})]",
            s + 50,
            s + 100,
            s + 110,
            s + 120,
            s + 130,
            s + 100,
            s + 110,
            s + 120,
            s + 130
        )],
        "the engine skips the segment whose last record is before the start"
    );
    assert_eq!(
        (r.exit, r.outcome()),
        (Some(2), Some("fail-integrity")),
        "complete coverage must catch it: {}",
        r.summary()
    );
    assert_eq!(
        r.scorecard["integrity"]["verification"]["complete"]["replay"]["missing"],
        json!(1)
    );
}

/// Both readers on one signed scorecard this file kept: exit codes and the
/// lines each prints, `(rust exit, python exit, rust output, python output)`.
fn read_with_both(doc: &std::path::Path, sig: &std::path::Path) -> (i32, i32, String, String) {
    let key = root().join("e2e/fixtures/signed/public.pem");
    let rust = std::process::Command::new(bin())
        .args(["drill", "verify", "--scorecard"])
        .arg(doc)
        .arg("--signature")
        .arg(sig)
        .arg("--public-key")
        .arg(&key)
        .output()
        .expect("logweir drill verify");
    let py = std::process::Command::new(auditor_python())
        .arg(root().join("docs/verify_scorecard.py"))
        .arg(doc)
        .arg(sig)
        .arg(&key)
        .output()
        .expect("docs/verify_scorecard.py");
    (
        rust.status.code().unwrap_or(-1),
        py.status.code().unwrap_or(-1),
        format!("{}{}", rust.stdout_utf8(), rust.stderr_utf8()),
        format!("{}{}", py.stdout_utf8(), py.stderr_utf8()),
    )
}

/// The lines starting `replay selection:` or `sample coverage:`, in order.
fn selection_lines(out: &str) -> Vec<String> {
    out.lines()
        .filter_map(|l| {
            ["replay selection: ", "sample coverage: "]
                .iter()
                .find_map(|p| l.find(p).map(|i| l[i..].to_string()))
        })
        .collect()
}

/// **A topic subset from a stated start, under both coverages.** The archive
/// holds topics A, B and C; the plan restores A and B from `S = T + 30`, a
/// start inside every partition's span. The target holds exactly the records
/// at or after `S` on every partition of A and B; C is not restored. Signed:
/// `format_version` 1.7.0 and `source.selection` = exactly the plan's window
/// (`window_start_ms`, `window_end_ms`, nothing else). Both readers accept each
/// scorecard and print the same `replay selection:` line, and the sampled one
/// prints the QUALIFIED sampled-pass line (review H1). The two signed
/// scorecards are kept as `scorecard-<coverage>.json` for the older-reader
/// rows (`docs/decisions/prod-11-1-replay-selection.md` §10).
#[test]
fn a_topic_subset_from_a_start_is_restored_and_signed_under_both_coverages() {
    let mut row = Row::new("topics");
    let base = T;
    let a = row.source_topic("a", &[]);
    let b = row.source_topic("b", &[]);
    let c = row.source_topic("c", &[]);
    for t in [&a, &b, &c] {
        let recs = layout(
            base,
            t,
            &[
                (0, &[10, 20, 30, 40, 50]),
                (1, &[11, 21, 31, 41, 51]),
                (2, &[12, 22, 32, 42, 52]),
            ],
        );
        kafka::produce_plain(t, &recs).expect("produce");
    }
    let backup_id = row.backup_id("ab");
    backup_ok(&backup_id, &[&a, &b, &c], 1000);
    let archives: BTreeMap<&str, Archive> = [&a, &b]
        .into_iter()
        .map(|t| {
            (
                t.as_str(),
                kafka::read_archive(&backup_id, t).expect("archive"),
            )
        })
        .collect();
    let (start, end) = (base + 30, base + 10_000);
    let sel = Selection {
        start: Some(start),
        end,
        ..Selection::default()
    };
    let mut runs = Vec::new();
    for (label, complete) in [("complete", true), ("sampled", false)] {
        let prefix = row.prefix(label, &[&a, &b, &c]);
        let r = restore_run(
            restore_spec(&backup_id, &[&a, &b], &prefix, &sel, (start, end), complete),
            Vec::new(),
        );
        let mut diffs = BTreeMap::new();
        for t in [&a, &b] {
            let want = expected(&archives[t.as_str()], &sel);
            let got = observed(&format!("{prefix}{t}"));
            diffs.insert(t.clone(), diff(&want, &got));
            oracle_rejects_mutants(&want, &got, (0, base + 10, None, None), 0);
        }
        let dir = demo_dir().join("replay-selection");
        std::fs::create_dir_all(&dir).expect("the outcome directory");
        let (doc, sig) = (
            dir.join(format!("scorecard-{label}.json")),
            dir.join(format!("scorecard-{label}.sig")),
        );
        std::fs::write(&doc, &r.signed.0).expect("keep the scorecard");
        std::fs::write(&sig, &r.signed.1).expect("keep the signature");
        let (rust_rc, py_rc, rust_out, py_out) = read_with_both(&doc, &sig);
        runs.push(json!({
            "label": label,
            "verdict": r.summary(),
            "diffs": diffs,
            "readers": {"rust": rust_rc, "python": py_rc,
                        "rust_lines": selection_lines(&rust_out),
                        "python_lines": selection_lines(&py_out)},
        }));
        write_outcome("topics", &json!({ "runs": runs }));
        assert!(
            diffs.values().all(Vec::is_empty),
            "{label}: the restored output is not the selection: {diffs:#?}"
        );
        assert!(
            !topic_exists(&format!("{prefix}{c}")),
            "{label}: a topic the plan did not select was restored"
        );
        assert_eq!(
            (r.exit, r.outcome()),
            (Some(0), Some("pass")),
            "{label}: {}",
            r.summary()
        );
        assert_eq!(r.scorecard["format_version"], json!("1.7.0"), "{label}");
        assert_eq!(
            signed_selection(&r),
            &json!({"window_start_ms": start, "window_end_ms": end}),
            "{label}: the block is the plan's window and nothing else"
        );
        assert_eq!((rust_rc, py_rc), (0, 0), "{label}: {rust_out}\n{py_out}");
        let lines = selection_lines(&rust_out);
        assert_eq!(lines, selection_lines(&py_out), "{label}: the readers differ");
        let selection_line = format!(
            "replay selection: every partition of every restored topic, from epoch-ms {start} \
             (the plan's stated window start, inclusive) to epoch-ms {end} (inclusive); no record \
             before the start was restored or expected"
        );
        assert!(lines.contains(&selection_line), "{label}: {lines:#?}");
        let qualified = lines
            .iter()
            .any(|l| l.starts_with("sample coverage: a sampled pass over a replay selection"));
        assert_eq!(qualified, !complete, "{label}: {lines:#?}");
    }
}

/// **Refused before anything runs.** A start a millisecond before the
/// archive's floor (never moved to it), a selection no segment overlaps, a
/// target name that already exists, and a partition subset with and without
/// a start (`PartitionSubsetsAwaitOwnerDecision`, OD-9 open): each exit 3,
/// with no target topic created and no scorecard signed. The control is the same plan with a start
/// AT the floor, which restores.
#[test]
fn refusals_before_anything_runs() {
    let mut row = Row::new("refusals");
    let s = T;
    let topic = row.source_topic("ref", &[]);
    let recs = layout(s, "ref", &[(0, &[0, 10, 20]), (1, &[5, 15]), (2, &[7])]);
    kafka::produce_plain(&topic, &recs).expect("produce");
    let backup_id = row.backup_id("ref");
    backup_ok(&backup_id, &[&topic], 1000);

    let mut cases = Vec::new();
    let mut check = |label: &str, sel: Selection, pre_create: bool, want: &str| {
        let prefix = row.prefix(label, &[&topic]);
        let target = format!("{prefix}{topic}");
        let pre = if pre_create {
            vec![(target.clone(), PARTS)]
        } else {
            Vec::new()
        };
        let r = restore_run(
            restore_spec(&backup_id, &[&topic], &prefix, &sel, (s, s + 1_000), false),
            pre,
        );
        let both = format!("{}\n{}", r.stdout, r.stderr);
        cases.push(json!({"label": label, "verdict": r.summary()}));
        write_outcome("refusals", &json!({ "cases": cases }));
        assert_eq!(r.exit, Some(3), "{label}: {}", r.summary());
        assert!(
            r.scorecard.is_null(),
            "{label}: a refused run signs nothing"
        );
        assert!(both.contains(want), "{label}: {}", r.summary());
        if !pre_create {
            assert!(
                !topic_exists(&target),
                "{label}: a target topic was created"
            );
        }
    };
    check(
        "early",
        Selection {
            start: Some(s - 1),
            end: s + 1_000,
            ..Selection::default()
        },
        false,
        "before the archive set's earliest covered timestamp",
    );
    check(
        "empty",
        Selection {
            start: Some(s + 500),
            end: s + 1_000,
            ..Selection::default()
        },
        false,
        "the selection is empty",
    );
    check(
        "exists",
        Selection {
            start: Some(s),
            end: s + 1_000,
            ..Selection::default()
        },
        true,
        "already exist",
    );
    // A partition subset, with and without a start: refused by name until the
    // owner decides OD-9 (review H1), before any target exists.
    for (label, start) in [("subset", None), ("subset-start", Some(s))] {
        check(
            label,
            Selection {
                start,
                end: s + 1_000,
                partitions: [(topic.clone(), vec![0])].into_iter().collect(),
            },
            false,
            "PartitionSubsetsAwaitOwnerDecision: restore.partitions names a partition subset",
        );
    }

    // The control: a start AT the floor restores.
    let sel = Selection {
        start: Some(s),
        end: s + 1_000,
        ..Selection::default()
    };
    let prefix = row.prefix("floor", &[&topic]);
    let r = restore_run(
        restore_spec(&backup_id, &[&topic], &prefix, &sel, (s, s + 1_000), false),
        Vec::new(),
    );
    assert_eq!(
        (r.exit, r.outcome()),
        (Some(0), Some("pass")),
        "{}",
        r.summary()
    );
}

/// **A compaction hole inside a sub-window.** Each partition is written
/// `k1, k2, k3, k2', k4` and the cleaner removes the superseded `k2` (offset
/// 1), a hole INSIDE the archived span. Restored from the third record's
/// timestamp: the target holds exactly the selected records of the compacted
/// log, complete coverage passes, and the hole is disclosed in
/// `offset_holes`.
#[test]
fn a_compaction_hole_inside_a_sub_window_is_restored_and_disclosed() {
    let mut row = Row::new("compaction");
    let topic = row.source_topic(
        "cmp",
        &[
            ("cleanup.policy", "compact"),
            ("segment.ms", "100"),
            ("min.cleanable.dirty.ratio", "0.01"),
            ("min.compaction.lag.ms", "0"),
            ("delete.retention.ms", "86400000"),
        ],
    );
    let mut fixture = Vec::new();
    for p in 0..PARTS {
        for (i, k) in ["k1", "k2", "k3", "k2", "k4"].iter().enumerate() {
            fixture.push(Out::kv(p, Some(T + i as i64 * 10), k, &format!("v{i}")));
        }
    }
    kafka::produce_plain(&topic, &fixture).expect("produce");
    let rollers: Vec<Out> = (0..PARTS)
        .map(|p| Out::kv(p, Some(T + 3_600_000), "k9", "roll"))
        .collect();
    kafka::produce_plain(&topic, &rollers).expect("roll");
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let src = kafka::read_topic(&topic, PARTS, Isolation::Committed).expect("source");
        let compacted = (0..PARTS).all(|p| {
            let offs: Vec<i64> = src
                .iter()
                .filter(|r| r.partition == p)
                .map(|r| r.offset)
                .collect();
            offs == vec![0, 2, 3, 4, 5]
        });
        if compacted {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the cleaner did not remove offset 1 within 180 s"
        );
        std::thread::sleep(Duration::from_secs(3));
    }
    let backup_id = row.backup_id("cmp");
    backup_ok(&backup_id, &[&topic], 1000);
    let archive = kafka::read_archive(&backup_id, &topic).expect("archive");
    let sel = Selection {
        start: Some(T + 20),
        end: T + 3_600_000,
        ..Selection::default()
    };
    let prefix = row.prefix("c", &[&topic]);
    let r = restore_run(
        restore_spec(
            &backup_id,
            &[&topic],
            &prefix,
            &sel,
            (T + 20, T + 3_600_000),
            true,
        ),
        Vec::new(),
    );
    let want = expected(&archive, &sel);
    let got = observed(&format!("{prefix}{topic}"));
    let d = diff(&want, &got);
    write_outcome("compaction", &json!({"verdict": r.summary(), "diff": d}));
    assert!(d.is_empty(), "{d:#?}");
    assert_eq!(
        got[&0].iter().map(|c| c.0).collect::<Vec<_>>(),
        vec![2, 3, 4, 5]
    );
    assert_eq!(
        (r.exit, r.outcome()),
        (Some(0), Some("pass")),
        "{}",
        r.summary()
    );
    let holes = r.scorecard["integrity"]["verification"]["complete"]["archive"]["offset_holes"]
        .as_u64()
        .unwrap_or(0);
    assert_eq!(
        holes, PARTS as u64,
        "one hole (offset 1) per partition, disclosed"
    );
    oracle_rejects_mutants(&want, &got, (1, T + 10, None, None), 0);
}

/// **A new point arriving does not change an approved plan.** The plan is
/// bound to backup set B1 (`source.backup`) and selects every partition from
/// a start. More records arrive and a second backup B2 — a newer recovery point
/// — is taken before the plan runs. The run restores B1's selection exactly
/// (the oracle reads B1's archive) and signs B1; nothing of B2 is restored.
#[test]
fn a_new_point_does_not_change_an_approved_selection() {
    let mut row = Row::new("newpoint");
    let topic = row.source_topic("np", &[]);
    let first = layout(T, "np1", &[(0, &[0, 10, 20]), (1, &[0, 10]), (2, &[0])]);
    kafka::produce_plain(&topic, &first).expect("produce");
    let b1 = row.backup_id("np1");
    backup_ok(&b1, &[&topic], 1000);
    let archive = kafka::read_archive(&b1, &topic).expect("archive");
    let sel = Selection {
        start: Some(T + 10),
        end: T + 60_000,
        ..Selection::default()
    };
    let prefix = row.prefix("np", &[&topic]);
    let spec = restore_spec(&b1, &[&topic], &prefix, &sel, (T + 10, T + 60_000), true);
    let plan_bytes = serde_yaml::to_string(&spec).unwrap();

    // The new point: more records, and a second backup set written under the
    // SAME storage prefix — the archive the plan names now holds a newer set,
    // which `latestCompleted` would pick.
    let later = layout(T, "np2", &[(0, &[30, 40]), (1, &[30]), (2, &[30])]);
    kafka::produce_plain(&topic, &later).expect("produce later");
    let b2 = row.backup_id("np2");
    backup_into(&b1, &b2, &[&topic]);
    assert!(
        mc(&[
            "stat",
            &format!("local/{ARCHIVE_BUCKET}/{b1}/{b2}/manifest.json")
        ])
        .status
        .success(),
        "the newer set is in the archive the plan reads"
    );
    assert_eq!(
        serde_yaml::to_string(&spec).unwrap(),
        plan_bytes,
        "the plan's bytes are what was approved"
    );

    let r = restore_run(spec, Vec::new());
    let want = expected(&archive, &sel);
    let got = observed(&format!("{prefix}{topic}"));
    let d = diff(&want, &got);
    write_outcome("newpoint", &json!({"verdict": r.summary(), "diff": d}));
    assert!(d.is_empty(), "{d:#?}");
    assert_eq!(
        (r.exit, r.outcome()),
        (Some(0), Some("pass")),
        "{}",
        r.summary()
    );
    assert_eq!(r.scorecard["source"]["backup_id"], json!(b1));
    assert_eq!(got[&0].len(), 2, "B1's records at or after the start only");
    oracle_rejects_mutants(&want, &got, (3, T + 30, None, None), 0);
}

/// **An older runner refuses a plan stating a start (review M2).** Run with
/// `LOGWEIR_E2E_OLDER_RUNNER` naming a `logweir` built from main BEFORE
/// PROD-11.1 (the fix round used main at the merge base it names in the
/// report). Three plans over one archive:
///
/// 1. a start, written `point_in_time: "<start>/<end>"`: the older runner
///    parses `point_in_time` as one RFC 3339 instant, so the plan does not
///    parse — exit 1, `drill spec does not parse`, no target topic, nothing
///    signed. It cannot restore from the floor what the plan said to restore
///    from a start.
/// 2. the control: the same plan with `point_in_time: "<end>"` — the older
///    runner restores it, `pass`, every record up to the end.
/// 3. the RESIDUAL: a hand-written `restore.partitions` with an instant
///    `point_in_time`. The older runner ignores a key it does not know and
///    restores EVERY partition, signed `pass`. No Logweir writer emits that
///    key (the current runner refuses it, `refusals_before_anything_runs`);
///    this row keeps the older runner's behaviour on record.
///
/// IGNORED by default: the older binary is built outside this tree, and an
/// unset variable is a PANIC here, never a silent pass.
#[test]
#[ignore = "needs LOGWEIR_E2E_OLDER_RUNNER: a logweir built from main before PROD-11.1"]
fn an_older_runner_refuses_a_plan_stating_a_start() {
    let older = std::path::PathBuf::from(std::env::var("LOGWEIR_E2E_OLDER_RUNNER").expect(
        "LOGWEIR_E2E_OLDER_RUNNER names the older logweir binary; this row never passes without it",
    ));
    assert!(older.is_file(), "{} is not a file", older.display());
    let version = std::process::Command::new(&older)
        .arg("--version")
        .output()
        .expect("the older logweir runs");
    let mut row = Row::new("older");
    let s = T;
    let topic = row.source_topic("old", &[]);
    let recs = layout(s, "old", &[(0, &[0, 10, 20]), (1, &[5, 15]), (2, &[7, 17])]);
    kafka::produce_plain(&topic, &recs).expect("produce");
    let backup_id = row.backup_id("old");
    backup_ok(&backup_id, &[&topic], 1000);
    let archive = kafka::read_archive(&backup_id, &topic).expect("archive");
    let end = s + 1_000;

    let mut cases = Vec::new();
    let mut run = |label: &str, sel: &Selection| {
        let prefix = row.prefix(label, &[&topic]);
        let target = format!("{prefix}{topic}");
        let r = restore_run_by(
            restore_spec(&backup_id, &[&topic], &prefix, sel, (s, end), true),
            Vec::new(),
            Some(older.clone()),
        );
        let got = observed(&target);
        cases.push(json!({
            "label": label,
            "older_runner": older.display().to_string(),
            "older_version": version.stdout_utf8().trim(),
            "plan_restore_block": sel.restore_block(),
            "target_exists": topic_exists(&target),
            "restored": got.values().map(Vec::len).sum::<usize>(),
            "verdict": r.summary(),
        }));
        write_outcome("older-runner", &json!({ "cases": cases }));
        (r, target, got)
    };

    // 1. A stated start: refused before anything runs.
    let start = Selection {
        start: Some(s + 10),
        end,
        ..Selection::default()
    };
    let (r, target, _) = run("start", &start);
    let both = format!("{}\n{}", r.stdout, r.stderr);
    assert_eq!(r.exit, Some(1), "{}", r.summary());
    assert!(both.contains("drill spec does not parse"), "{}", r.summary());
    assert!(r.scorecard.is_null(), "the older runner signed a scorecard");
    assert!(
        !topic_exists(&target),
        "the older runner created a target topic"
    );

    // 2. The control: the same plan without the start restores.
    let full = Selection {
        start: None,
        end,
        ..Selection::default()
    };
    let (r, _, got) = run("control", &full);
    let want = expected(&archive, &full);
    assert!(diff(&want, &got).is_empty(), "{:#?}", diff(&want, &got));
    assert_eq!(
        (r.exit, r.outcome()),
        (Some(0), Some("pass")),
        "{}",
        r.summary()
    );
    assert!(
        r.scorecard["source"].get("selection").is_none(),
        "the older runner writes no selection block"
    );

    // 3. The residual: a hand-written `restore.partitions` is IGNORED by the
    // older runner, which restores every partition.
    let subset = Selection {
        start: None,
        end,
        partitions: [(topic.clone(), vec![0])].into_iter().collect(),
    };
    let (r, _, got) = run("partitions-residual", &subset);
    assert!(
        diff(&want, &got).is_empty(),
        "the older runner restored every partition: {:#?}",
        diff(&want, &got)
    );
    assert_eq!(
        (r.exit, r.outcome()),
        (Some(0), Some("pass")),
        "{}",
        r.summary()
    );
}
