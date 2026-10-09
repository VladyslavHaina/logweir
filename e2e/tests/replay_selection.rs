#![cfg(feature = "e2e")]
//! **PROD-11.1 / PROD-11.1b — replay selection, observed on a real broker and
//! archive.**
//!
//! Every row produces a deterministic fixture into its own source topics,
//! takes a backup with the SHIPPED `logweir backup run` and the pinned engine,
//! and restores it with `logweir restore run` (`target.mode: newTopic`) under
//! a plan that states a replay selection: an inclusive window START, written
//! as the interval form of `restore.point_in_time` (`"<start>/<end>"`), and
//! (PROD-11.1b, the owner's decision OD-9 (a)) per-topic PARTITION SUBSETS,
//! `restore.partitions`, written beside an interval form (`"<start>/<end>"`,
//! or `"../<end>"` from the archive's floor). A subset restore signs scorecard
//! format 2.0.0. The outcome is decided by THIS file's oracle, never by
//! Logweir's exit status: the archive is read with Logweir's own `kbak`
//! decoder, the expected output is every archived record of a SELECTED
//! partition whose OWN timestamp is in `[start, end]` — computed here,
//! independently of `logweir_core::replay_selection` — and the restored topic
//! is read back and mapped to source offsets by its last `x-original-offset`
//! header. Logweir's signed verdict and its `source.selection` block are then
//! asserted as part of the contract.
//!
//! | row | what it proves |
//! |---|---|
//! | `the_start_is_inclusive_…` | inclusive vs exclusive at the start; equal timestamps at it; non-monotonic timestamps across it inside one segment |
//! | `a_segment_whose_bounds_hide_…` | the engine's segment rule skips an in-window record whose segment's last record is before the start (PROD-01.1 S7 at the start): complete coverage fails it, never `pass` |
//! | `a_topic_subset_from_a_start_…` | a topic subset (C not restored) from a start, under BOTH coverages: the start-only block signed at 1.7.0, unchanged, both readers accept it |
//! | `partition_subsets_on_two_topics_…` | `A: [0, 2]` and `B: [1]` (two engine runs) from the floor under both coverages and from a start: every unselected partition empty, C not restored, signed 2.0.0, both readers print the same subset lines; the unnarrowed control is 1.x with no block |
//! | `a_record_in_an_unselected_partition_fails` | an engine that ignores the partition filter restores an unselected partition: `fail-integrity` under both coverages, never `pass` |
//! | `a_selected_partition_with_nothing_in_the_window_…` | a selected partition with no record in the window is signed `preflight-failed` (2.0.0), naming it, and nothing is created |
//! | `refusals_before_anything_runs` | a start before coverage, an empty selection, an existing target name, a subset naming a partition the archive lacks (exit 3) and a subset beside a plain instant (does not parse, exit 1): no target topic, no signed scorecard |
//! | `a_compaction_hole_inside_a_sub_window_…` | a compacted source restored from a stated start: exact, the hole disclosed |
//! | `a_new_point_does_not_change_…` | a newer backup arriving after the plan was approved does not change what the plan restores |
//! | `a_partition_with_nothing_in_the_window_…` | a start that leaves a partition with no record in the window is signed `preflight-failed` naming it, never `pass`, and creates nothing |
//! | `an_older_runner_refuses_…` (ignored; needs `LOGWEIR_E2E_OLDER_RUNNER`) | main's runner from BEFORE PROD-11.1 refuses a plan stating a start, and a subset plan in the only form this build accepts; the same binary restores the same plan without either (the control, compared with this build's document); it IGNORES a subset beside a plain instant (the residual this build closes by refusing that plan at parse) |
//! | `an_older_runner_refuses_a_partition_subset_plan` (ignored; needs `LOGWEIR_E2E_OLDER_RUNNER`) | any runner before PROD-11.1b — before PROD-11.1, or main's after it — refuses both subset forms: nothing created, nothing signed |
//! | `older_readers_refuse_a_2_0_0_document` (ignored; needs `LOGWEIR_E2E_OLDER_READERS`) | every reader before 1.25.0 refuses the 2.0.0 documents the subset row signed, never `VALID` |
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
//! # the older-runner rows, with a `logweir` built from main before PROD-11.1:
//! LOGWEIR_E2E_OLDER_RUNNER=/path/to/older/logweir AWS_EC2_METADATA_DISABLED=true \
//!     cargo test -p e2e --features e2e --test replay_selection \
//!     an_older_runner -- --ignored --test-threads=1 --nocapture
//! # the older-reader row, after the subset row, with older readers' files
//! # (`verify_scorecard.py` from git history, or older `logweir` binaries):
//! LOGWEIR_E2E_OLDER_READERS=/a/verify_scorecard.py:/b/logweir \
//!     cargo test -p e2e --features e2e --test replay_selection \
//!     older_readers -- --ignored --test-threads=1 --nocapture
//! ```
//!
//! Each row writes `.e2e/<project>/replay-selection/<row>.json`; the
//! topic-subset row also keeps its two signed scorecards there
//! (`scorecard-<coverage>.json` and `.sig`), and the partition-subset row its
//! 2.0.0 ones (`scorecard-subset-<label>.json` and `.sig`).
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
    /// `topic -> partitions`, written as `restore.partitions` (PROD-11.1b):
    /// a named topic restores only these partitions, and the oracle expects
    /// nothing from its others.
    partitions: BTreeMap<String, Vec<i32>>,
}

impl Selection {
    fn selects(&self, ts: i64) -> bool {
        ts <= self.end && self.start.is_none_or(|s| ts >= s)
    }

    fn selects_partition(&self, topic: &str, p: i32) -> bool {
        self.partitions.get(topic).is_none_or(|ps| ps.contains(&p))
    }

    /// The plan's `restore:` block: a stated start is the INTERVAL form of
    /// `point_in_time`, `"<start>/<end>"` — the one grammar for it, which an
    /// older runner cannot parse and so refuses; a subset with no start is
    /// the OPEN-START interval `"../<end>"`, for the same reason.
    fn restore_block(&self) -> String {
        let point = match self.start {
            Some(s) => format!("{}/{}", rfc3339(s), rfc3339(self.end)),
            None if !self.partitions.is_empty() => format!("../{}", rfc3339(self.end)),
            None => rfc3339(self.end),
        };
        self.block_with(&point)
    }

    /// The pre-PROD-11.1b spelling of a subset: beside a PLAIN instant. This
    /// build refuses it at parse; a runner from before PROD-11.1 ignores the
    /// key and restores every partition (the residual the interval closes).
    fn restore_block_beside_an_instant(&self) -> String {
        self.block_with(&rfc3339(self.end))
    }

    fn block_with(&self, point: &str) -> String {
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
    restore_spec_with(
        backup_id,
        sources,
        prefix,
        &sel.restore_block(),
        sample,
        complete,
    )
}

/// [`restore_spec`] with the `restore:` block as written.
fn restore_spec_with(
    backup_id: &str,
    sources: &[&str],
    prefix: &str,
    restore_block: &str,
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
        restore_block,
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
            "phases": self.scorecard["phases"],
            "stderr_tail": self.stderr.lines().rev().take(15).collect::<Vec<_>>(),
        })
    }
}

fn restore_run(spec: serde_yaml::Value, pre_create: Vec<(String, i32)>) -> Restored {
    restore_run_by(spec, pre_create, None, Vec::new())
}

/// [`restore_run`] with the `logweir` binary named (`None` is this tree's)
/// and extra environment for it (the widening engine's).
fn restore_run_by(
    spec: serde_yaml::Value,
    pre_create: Vec<(String, i32)>,
    bin: Option<std::path::PathBuf>,
    env: Vec<(String, String)>,
) -> Restored {
    let h = std::thread::spawn(move || {
        let mut o = RunOpts::new(&spec);
        o.restore_run = true;
        o.pre_create = pre_create;
        o.bin = bin;
        o.env = env;
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
    expected_of(archive, "", sel)
}

/// [`expected`] for `topic`, whose partitions outside its subset (PROD-11.1b)
/// expect NOTHING: the target must hold them empty.
fn expected_of(archive: &Archive, topic: &str, sel: &Selection) -> Output {
    let mut out: Output = (0..PARTS).map(|p| (p, Vec::new())).collect();
    for r in &archive.records {
        if sel.selects(r.timestamp) && sel.selects_partition(topic, r.partition) {
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
/// prints the QUALIFIED sampled-pass line (review H1); only the complete one
/// says no record before the start was restored (review N1). The two signed
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
        assert_eq!(
            lines,
            selection_lines(&py_out),
            "{label}: the readers differ"
        );
        // Review N1: only the complete pass proves no record before the
        // start was restored; the sampled lane says it does not.
        let before = if complete {
            "no record before the start was restored or expected"
        } else {
            "no record before the start was expected; a sampled check does not prove that none \
             was restored"
        };
        let selection_line = format!(
            "replay selection: every partition of every restored topic, from epoch-ms {start} \
             (the plan's stated window start, inclusive) to epoch-ms {end} (inclusive); {before}"
        );
        assert!(lines.contains(&selection_line), "{label}: {lines:#?}");
        let qualified = lines
            .iter()
            .any(|l| l.starts_with("sample coverage: a sampled pass over a replay selection"));
        assert_eq!(qualified, !complete, "{label}: {lines:#?}");
    }
}

/// The `replay selection:` line a 2.0.0 subset document prints, from both
/// readers alike: `subsets` named in order, from `start` or the floor, `runs`
/// engine runs, and what the verdict proves (`proved`).
fn subset_line(
    subsets: &BTreeMap<String, Vec<i32>>,
    start: Option<i64>,
    end: i64,
    runs: u32,
    proved: bool,
    before: Option<&str>,
) -> String {
    let named: Vec<String> = subsets
        .iter()
        .map(|(t, ps)| {
            let list: Vec<String> = ps.iter().map(i32::to_string).collect();
            format!("{t} partitions [{}]", list.join(", "))
        })
        .collect();
    let from = match start {
        Some(s) => format!("from epoch-ms {s} (the plan's stated window start, inclusive)"),
        None => "from the archive's floor".to_string(),
    };
    let outside = if proved {
        "no record of another partition of these topics was restored or expected"
    } else {
        "no record of another partition of these topics was expected"
    };
    let mut line = format!(
        "replay selection: ONLY {} (every partition of any other restored topic), {from} to \
         epoch-ms {end} (inclusive), in {runs} engine run(s); {outside}",
        named.join("; ")
    );
    if let Some(b) = before {
        line.push_str("; ");
        line.push_str(b);
    }
    line
}

/// A JSON document's key paths (array indices folded), sorted: the SHAPE two
/// builds' documents of one plan are compared by.
fn key_paths(v: &Value) -> BTreeSet<String> {
    fn walk(v: &Value, at: String, out: &mut BTreeSet<String>) {
        match v {
            Value::Object(m) => {
                for (k, x) in m {
                    let p = format!("{at}/{k}");
                    out.insert(p.clone());
                    walk(x, p, out);
                }
            }
            Value::Array(a) => {
                for x in a {
                    walk(x, format!("{at}[]"), out);
                }
            }
            _ => {}
        }
    }
    let mut out = BTreeSet::new();
    walk(v, String::new(), &mut out);
    out
}

/// **Different partition subsets on two topics: two engine runs, signed
/// 2.0.0** (PROD-11.1b, the owner's decision OD-9 (a)). The archive holds
/// topics A, B and C of three partitions; the plan restores A and B with
/// `A: [0, 2]` and `B: [1]`. The engine's partition filter applies to every
/// topic of a run, so this is TWO engine runs. Three restores: from the
/// archive's floor (`"../<end>"`) under complete and sampled coverage, and
/// from a stated start under sampled coverage. Each target holds exactly the
/// selected partitions' records in the window and every unselected partition
/// is EMPTY; C is not restored. Signed: format 2.0.0, `source.selection` =
/// exactly the subsets, the end, the start when stated and `engine_runs: 2`.
/// Both readers accept each document and print the same subset lines (and the
/// qualified sampled-pass line). The signed documents are kept as
/// `scorecard-subset-<label>.json` for the older-reader row. The control is
/// the same archive restored with no selection: 1.x, no block, every
/// partition.
#[test]
fn partition_subsets_on_two_topics_are_two_engine_runs_and_sign_2_0_0() {
    let mut row = Row::new("subsets");
    let s = T;
    let a = row.source_topic("a", &[]);
    let b = row.source_topic("b", &[]);
    let c = row.source_topic("c", &[]);
    for t in [&a, &b, &c] {
        let recs = layout(
            s,
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
    let subsets: BTreeMap<String, Vec<i32>> = [(a.clone(), vec![0, 2]), (b.clone(), vec![1])]
        .into_iter()
        .collect();
    let end = s + 10_000;
    let dir = demo_dir().join("replay-selection");
    std::fs::create_dir_all(&dir).expect("the outcome directory");
    let mut runs = Vec::new();
    for (label, start, complete) in [
        ("complete", None, true),
        ("sampled", None, false),
        ("sampled-start", Some(s + 30), false),
    ] {
        let sel = Selection {
            start,
            end,
            partitions: subsets.clone(),
        };
        let prefix = row.prefix(label, &[&a, &b, &c]);
        let r = restore_run(
            restore_spec(
                &backup_id,
                &[&a, &b],
                &prefix,
                &sel,
                (start.unwrap_or(s), end),
                complete,
            ),
            Vec::new(),
        );
        let mut diffs = BTreeMap::new();
        let mut unselected_restored = 0usize;
        for t in [&a, &b] {
            let want = expected_of(&archives[t.as_str()], t, &sel);
            let got = observed(&format!("{prefix}{t}"));
            diffs.insert(t.clone(), diff(&want, &got));
            unselected_restored += (0..PARTS)
                .filter(|p| !sel.selects_partition(t, *p))
                .map(|p| got.get(&p).map_or(0, Vec::len))
                .sum::<usize>();
            oracle_rejects_mutants(&want, &got, (0, s + 11, None, None), 1);
        }
        let (doc, sig) = (
            dir.join(format!("scorecard-subset-{label}.json")),
            dir.join(format!("scorecard-subset-{label}.sig")),
        );
        std::fs::write(&doc, &r.signed.0).expect("keep the scorecard");
        std::fs::write(&sig, &r.signed.1).expect("keep the signature");
        let (rust_rc, py_rc, rust_out, py_out) = read_with_both(&doc, &sig);
        runs.push(json!({
            "label": label,
            "plan_restore_block": sel.restore_block(),
            "verdict": r.summary(),
            "diffs": diffs,
            "unselected_records_restored": unselected_restored,
            "readers": {"rust": rust_rc, "python": py_rc,
                        "rust_lines": selection_lines(&rust_out),
                        "python_lines": selection_lines(&py_out)},
        }));
        write_outcome("subsets", &json!({ "runs": runs }));
        assert!(
            diffs.values().all(Vec::is_empty),
            "{label}: the restored output is not the selection: {diffs:#?}"
        );
        assert_eq!(
            unselected_restored, 0,
            "{label}: an unselected partition holds a record"
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
        assert_eq!(r.scorecard["format_version"], json!("2.0.0"), "{label}");
        let mut want_block = json!({
            "window_end_ms": end,
            "partitions": [
                {"topic": a, "partitions": [0, 2]},
                {"topic": b, "partitions": [1]},
            ],
            "engine_runs": 2,
        });
        if let Some(st) = start {
            want_block["window_start_ms"] = json!(st);
        }
        assert_eq!(signed_selection(&r), &want_block, "{label}");
        assert_eq!((rust_rc, py_rc), (0, 0), "{label}: {rust_out}\n{py_out}");
        let lines = selection_lines(&rust_out);
        assert_eq!(
            lines,
            selection_lines(&py_out),
            "{label}: the readers differ"
        );
        let before = start.map(|_| {
            "no record before the start was expected; a sampled check does not prove that none \
             was restored"
        });
        let held = "every selected partition was held to its own count bound over that window, \
                    every other partition of a narrowed topic was held empty, max_partitions \
                    reached every topic before a second partition of any, and a readable engine \
                    report lacking a selected partition with records in that window was refused";
        let mut want_lines = Vec::new();
        if !complete {
            let from = start.map_or("the archive's floor".to_string(), |st| {
                format!("epoch-ms {st}")
            });
            let mut l = format!(
                "sample coverage: a sampled pass over a partition subset from {from} to epoch-ms \
                 {end}: {held}"
            );
            if start.is_some() {
                l.push_str(
                    "; no record before the start was expected, and a sampled check does not \
                     prove that none was restored",
                );
            }
            want_lines.push(l);
        }
        want_lines.push(subset_line(&subsets, start, end, 2, true, before));
        assert_eq!(lines, want_lines, "{label}");
    }

    // The control: the same archive with no selection is 1.x, carries no
    // block and restores every partition of A and B.
    let sel = Selection {
        start: None,
        end,
        partitions: BTreeMap::new(),
    };
    let prefix = row.prefix("unnarrowed", &[&a, &b, &c]);
    let r = restore_run(
        restore_spec(&backup_id, &[&a, &b], &prefix, &sel, (s, end), true),
        Vec::new(),
    );
    let mut diffs = BTreeMap::new();
    for t in [&a, &b] {
        let want = expected_of(&archives[t.as_str()], t, &sel);
        diffs.insert(t.clone(), diff(&want, &observed(&format!("{prefix}{t}"))));
    }
    runs.push(json!({"label": "unnarrowed", "verdict": r.summary(), "diffs": diffs}));
    write_outcome("subsets", &json!({ "runs": runs }));
    assert!(diffs.values().all(Vec::is_empty), "{diffs:#?}");
    assert_eq!(
        (r.exit, r.outcome()),
        (Some(0), Some("pass")),
        "{}",
        r.summary()
    );
    assert_eq!(r.scorecard["format_version"], json!("1.4.0"));
    assert!(r.scorecard["source"].get("selection").is_none());
}

/// The environment that puts the WIDENING engine
/// (`e2e/fixtures/engine-widening.sh`) in front of the real one.
fn widening_engine() -> Vec<(String, String)> {
    vec![
        (
            "LOGWEIR_ENGINE_BIN".to_string(),
            root()
                .join("e2e/fixtures/engine-widening.sh")
                .display()
                .to_string(),
        ),
        (
            "LOGWEIR_E2E_REAL_ENGINE".to_string(),
            engine_bin().display().to_string(),
        ),
    ]
}

/// **A record in an unselected partition fails the restore** (PROD-11.1b's
/// negative control). The plan narrows A to `[0, 2]`; a FAULTY engine
/// (`engine-widening.sh`) drops the rendered partition filter and restores
/// partition 1 as well. Under complete coverage the record is `unexpected` in
/// a partition that expects nothing; under sampled coverage the per-partition
/// check holds partition 1 to empty. Both sign `fail-integrity` (2.0.0), never
/// `pass`, and the readers say only that no record of another partition was
/// EXPECTED. The control is the same plan with the real engine: `pass`.
#[test]
fn a_record_in_an_unselected_partition_fails_the_restore() {
    let mut row = Row::new("widened");
    let s = T;
    let topic = row.source_topic("wid", &[]);
    let recs = layout(
        s,
        "wid",
        &[(0, &[10, 20, 30]), (1, &[11, 21, 31]), (2, &[12, 22, 32])],
    );
    kafka::produce_plain(&topic, &recs).expect("produce");
    let backup_id = row.backup_id("wid");
    backup_ok(&backup_id, &[&topic], 1000);
    let archive = kafka::read_archive(&backup_id, &topic).expect("archive");
    let sel = Selection {
        start: None,
        end: s + 10_000,
        partitions: [(topic.clone(), vec![0, 2])].into_iter().collect(),
    };
    let mut cases = Vec::new();
    for (label, complete, widened) in [
        ("complete", true, true),
        ("sampled", false, true),
        ("control", true, false),
    ] {
        let prefix = row.prefix(label, &[&topic]);
        let target = format!("{prefix}{topic}");
        let env = if widened {
            widening_engine()
        } else {
            Vec::new()
        };
        let r = restore_run_by(
            restore_spec(
                &backup_id,
                &[&topic],
                &prefix,
                &sel,
                (s, s + 10_000),
                complete,
            ),
            Vec::new(),
            None,
            env,
        );
        let got = observed(&target);
        let d = diff(&expected_of(&archive, &topic, &sel), &got);
        let (doc, sig) = (
            demo_dir().join(format!("replay-selection/scorecard-widened-{label}.json")),
            demo_dir().join(format!("replay-selection/scorecard-widened-{label}.sig")),
        );
        std::fs::write(&doc, &r.signed.0).expect("keep the scorecard");
        std::fs::write(&sig, &r.signed.1).expect("keep the signature");
        let (rust_rc, py_rc, rust_out, py_out) = read_with_both(&doc, &sig);
        cases.push(json!({
            "label": label,
            "widening_engine": widened,
            "partition_1_restored": got.get(&1).map_or(0, Vec::len),
            "diff": d,
            "verdict": r.summary(),
            "readers": {"rust": rust_rc, "python": py_rc,
                        "rust_lines": selection_lines(&rust_out),
                        "python_lines": selection_lines(&py_out)},
        }));
        write_outcome("widened", &json!({ "cases": cases }));
        assert_eq!(r.scorecard["format_version"], json!("2.0.0"), "{label}");
        assert_eq!((rust_rc, py_rc), (0, 0), "{label}: {rust_out}\n{py_out}");
        assert_eq!(
            selection_lines(&rust_out),
            selection_lines(&py_out),
            "{label}: the readers differ"
        );
        if !widened {
            assert!(d.is_empty(), "{d:#?}");
            assert_eq!(
                (r.exit, r.outcome()),
                (Some(0), Some("pass")),
                "{}",
                r.summary()
            );
            continue;
        }
        assert_eq!(
            got.get(&1).map_or(0, Vec::len),
            3,
            "{label}: the widening engine restored the unselected partition"
        );
        assert!(!d.is_empty(), "{label}: the oracle sees the stray records");
        assert_eq!(
            (r.exit, r.outcome()),
            (Some(2), Some("fail-integrity")),
            "{label}: a record in an unselected partition is never a pass: {}",
            r.summary()
        );
        let reason = r.scorecard["integrity"]["partial_reason"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        if complete {
            let p1 = r.scorecard["integrity"]["verification"]["complete"]["partitions"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|p| p["partition"] == 1)
                .cloned()
                .unwrap_or(Value::Null);
            assert_eq!(p1["replay"]["expected"], json!(0), "{label}: {p1}");
            assert_eq!(p1["replay"]["unexpected"], json!(3), "{label}: {p1}");
        } else {
            assert!(
                reason.contains(&format!(
                    "holds 3 records but the plan's restore.partitions does not select \
                     partition 1 of {topic}"
                )),
                "{label}: {reason}"
            );
        }
        assert!(
            selection_lines(&rust_out).contains(&subset_line(
                &sel.partitions,
                None,
                s + 10_000,
                1,
                false,
                None
            )),
            "{label}: a failed verdict proves only that none was expected: {:?}",
            selection_lines(&rust_out)
        );
    }
}

/// **A selected partition with no record in the window is `preflight-failed`,
/// never `pass`** (PROD-11.1b). The plan narrows the topic to `[0, 2]` from
/// the archive's floor to `T + 1 s`; partition 2's records all come after the
/// end, so the engine's header preflight finds none of it in the window
/// (`empty`), which phase 5 holds is never a positive pass. Signed
/// `preflight-failed` (exit 2) at format 2.0.0, naming the partition; nothing
/// is created (target topics are created only after phase 5).
#[test]
fn a_selected_partition_with_nothing_in_the_window_is_preflight_failed() {
    let mut row = Row::new("emptysubset");
    let topic = row.source_topic("es", &[]);
    let recs = layout(
        T,
        "es",
        &[(0, &[0, 10, 20]), (1, &[5, 15]), (2, &[5_000, 5_010])],
    );
    kafka::produce_plain(&topic, &recs).expect("produce");
    let backup_id = row.backup_id("es");
    backup_ok(&backup_id, &[&topic], 1000);
    let sel = Selection {
        start: None,
        end: T + 1_000,
        partitions: [(topic.clone(), vec![0, 2])].into_iter().collect(),
    };
    let prefix = row.prefix("es", &[&topic]);
    let r = restore_run(
        restore_spec(&backup_id, &[&topic], &prefix, &sel, (T, T + 1_000), true),
        Vec::new(),
    );
    write_outcome("emptysubset", &json!({"verdict": r.summary()}));
    assert_eq!(
        (r.exit, r.outcome()),
        (Some(2), Some("preflight-failed")),
        "{}",
        r.summary()
    );
    let notes: Vec<String> = r.scorecard["phases"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|p| p["phase"] == 5)
        .flat_map(|p| p["notes"].as_array().cloned().unwrap_or_default())
        .filter_map(|n| n.as_str().map(str::to_string))
        .collect();
    assert_eq!(
        notes,
        vec![format!(
            "{topic}/2 empty: no records in the selected window for this partition"
        )],
        "{}",
        r.summary()
    );
    assert_eq!(r.scorecard["format_version"], json!("2.0.0"));
    assert_eq!(
        signed_selection(&r)["partitions"],
        json!([{"topic": topic, "partitions": [0, 2]}])
    );
    assert!(
        !topic_exists(&format!("{prefix}{topic}")),
        "a preflight-failed run created a target topic"
    );
}

/// **Refused before anything runs.** A start a millisecond before the
/// archive's floor (never moved to it), a selection no segment overlaps, a
/// target name that already exists, and (PROD-11.1b) a partition subset that
/// names a partition the archive does not list: each exit 3, with no target
/// topic created and no scorecard signed. And a subset written beside a PLAIN
/// instant — the spelling a runner before PROD-11.1 would ignore and widen —
/// does not parse: exit 1, nothing created or signed. The control is the same
/// plan with a start AT the floor, which restores.
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
    let mut check = |label: &str, block: String, pre_create: bool, exit: i32, want: &str| {
        let prefix = row.prefix(label, &[&topic]);
        let target = format!("{prefix}{topic}");
        let pre = if pre_create {
            vec![(target.clone(), PARTS)]
        } else {
            Vec::new()
        };
        let r = restore_run(
            restore_spec_with(
                &backup_id,
                &[&topic],
                &prefix,
                &block,
                (s, s + 1_000),
                false,
            ),
            pre,
        );
        let both = format!("{}\n{}", r.stdout, r.stderr);
        cases.push(json!({"label": label, "plan_restore_block": block, "verdict": r.summary()}));
        write_outcome("refusals", &json!({ "cases": cases }));
        assert_eq!(r.exit, Some(exit), "{label}: {}", r.summary());
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
    let window = |start: Option<i64>, partitions: &[(String, Vec<i32>)]| Selection {
        start,
        end: s + 1_000,
        partitions: partitions.iter().cloned().collect(),
    };
    check(
        "early",
        window(Some(s - 1), &[]).restore_block(),
        false,
        3,
        "before the archive set's earliest covered timestamp",
    );
    check(
        "empty",
        window(Some(s + 500), &[]).restore_block(),
        false,
        3,
        "the selection is empty",
    );
    check(
        "exists",
        window(Some(s), &[]).restore_block(),
        true,
        3,
        "already exist",
    );
    // PROD-11.1b: a subset is accepted, so what is refused is a subset no
    // archive can satisfy, and one written where an older runner would
    // ignore it.
    check(
        "subset-not-in-archive",
        window(None, &[(topic.clone(), vec![0, 7])]).restore_block(),
        false,
        3,
        &format!(
            "restore.partitions.{topic} names partition 7, which the archive set's manifest \
             does not list"
        ),
    );
    check(
        "subset-beside-an-instant",
        window(None, &[(topic.clone(), vec![0])]).restore_block_beside_an_instant(),
        false,
        1,
        "restore.partitions is written only beside the interval form of restore.point_in_time",
    );

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

/// **A partition with nothing in the window is `preflight-failed`, never
/// `pass`.** A start selects every partition; p2's records all precede it, so
/// the engine's header preflight finds no record of p2 in the window
/// (`empty`), which phase 5 holds is "explicitly not a positive pass" — the
/// same rule as a partition with nothing before the window's end. The run is
/// signed `preflight-failed` (exit 2) naming the partition, and nothing is
/// created: target topics are created only after phase 5. A start makes this
/// more likely than a point alone, so the row keeps it on record (the
/// decision record's §8). Found by the live run of the fix round, where the
/// new-point row's first fixture had such a partition.
#[test]
fn a_partition_with_nothing_in_the_window_is_preflight_failed_never_pass() {
    let mut row = Row::new("emptypart");
    let s = T + 10;
    let topic = row.source_topic("ep", &[]);
    let recs = layout(T, "ep", &[(0, &[0, 10, 20]), (1, &[5, 15]), (2, &[0, 1])]);
    kafka::produce_plain(&topic, &recs).expect("produce");
    let backup_id = row.backup_id("ep");
    backup_ok(&backup_id, &[&topic], 1000);
    let sel = Selection {
        start: Some(s),
        end: T + 60_000,
        ..Selection::default()
    };
    let prefix = row.prefix("ep", &[&topic]);
    let r = restore_run(
        restore_spec(&backup_id, &[&topic], &prefix, &sel, (s, T + 60_000), true),
        Vec::new(),
    );
    write_outcome("emptypart", &json!({"verdict": r.summary()}));
    assert_eq!(
        (r.exit, r.outcome()),
        (Some(2), Some("preflight-failed")),
        "{}",
        r.summary()
    );
    let notes: Vec<String> = r.scorecard["phases"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|p| p["phase"] == 5)
        .flat_map(|p| p["notes"].as_array().cloned().unwrap_or_default())
        .filter_map(|n| n.as_str().map(str::to_string))
        .collect();
    assert_eq!(
        notes,
        vec![format!(
            "{topic}/2 empty: no records in the selected window for this partition"
        )],
        "{}",
        r.summary()
    );
    assert_eq!(signed_selection(&r)["window_start_ms"], json!(s));
    assert!(
        !topic_exists(&format!("{prefix}{topic}")),
        "a preflight-failed run created a target topic"
    );
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
    // Every partition holds a record at or after the start: a partition with
    // none is `preflight-failed` (`a_partition_with_nothing_in_the_window_…`).
    let first = layout(T, "np1", &[(0, &[0, 10, 20]), (1, &[0, 10]), (2, &[0, 15])]);
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

/// **An older runner refuses a plan stating a start (review M2), and a
/// partition-subset plan as this build writes it (PROD-11.1b).** Run with
/// `LOGWEIR_E2E_OLDER_RUNNER` naming a `logweir` built from main BEFORE
/// PROD-11.1. Plans over one archive:
///
/// 1. a start, written `point_in_time: "<start>/<end>"`: the older runner
///    parses `point_in_time` as one RFC 3339 instant, so the plan does not
///    parse — exit 1, `drill spec does not parse`, no target topic, nothing
///    signed. It cannot restore from the floor what the plan said to restore
///    from a start.
/// 2. the control: the same plan with `point_in_time: "<end>"` — the older
///    runner restores it, `pass`, every record up to the end; this build
///    restores it too, and the two documents are the same shape (version,
///    key paths, no selection block): an unnarrowed run is unchanged.
/// 3. a partition subset in the only form this build accepts,
///    `point_in_time: "../<end>"` beside `restore.partitions`: refused exactly
///    like (1) — the residual PROD-11.1 recorded is closed.
/// 4. the RESIDUAL's old spelling: `restore.partitions` beside a plain
///    instant. The older runner ignores a key it does not know and restores
///    EVERY partition, signed `pass`; this build refuses that plan at parse
///    (`refusals_before_anything_runs`), so no Logweir writer produces it.
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
    let mut run = |label: &str, block: &str, by: Option<std::path::PathBuf>| {
        let prefix = row.prefix(label, &[&topic]);
        let target = format!("{prefix}{topic}");
        let runner = by.clone().unwrap_or_else(bin);
        let r = restore_run_by(
            restore_spec_with(&backup_id, &[&topic], &prefix, block, (s, end), true),
            Vec::new(),
            by,
            Vec::new(),
        );
        let got = observed(&target);
        cases.push(json!({
            "label": label,
            "runner": runner.display().to_string(),
            "older_version": version.stdout_utf8().trim(),
            "plan_restore_block": block,
            "target_exists": topic_exists(&target),
            "restored": got.values().map(Vec::len).sum::<usize>(),
            "verdict": r.summary(),
        }));
        write_outcome("older-runner", &json!({ "cases": cases }));
        (r, target, got)
    };
    let refused = |label: &str, r: &Restored, target: &str| {
        let both = format!("{}\n{}", r.stdout, r.stderr);
        assert_eq!(r.exit, Some(1), "{label}: {}", r.summary());
        assert!(
            both.contains("drill spec does not parse"),
            "{label}: {}",
            r.summary()
        );
        assert!(
            r.scorecard.is_null(),
            "{label}: the older runner signed a scorecard"
        );
        assert!(
            !topic_exists(target),
            "{label}: the older runner created a target topic"
        );
    };

    // 1. A stated start: refused before anything runs.
    let start = Selection {
        start: Some(s + 10),
        end,
        ..Selection::default()
    };
    let (r, target, _) = run("start", &start.restore_block(), Some(older.clone()));
    refused("start", &r, &target);

    // 2. The control: the same plan without the start restores, with the
    // older runner and with this build alike.
    let full = Selection {
        start: None,
        end,
        ..Selection::default()
    };
    let (r, _, got) = run("control", &full.restore_block(), Some(older.clone()));
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
    let (this, _, got) = run("control-this-build", &full.restore_block(), None);
    assert!(diff(&want, &got).is_empty(), "{:#?}", diff(&want, &got));
    assert_eq!(
        (this.exit, this.outcome()),
        (Some(0), Some("pass")),
        "{}",
        this.summary()
    );
    assert_eq!(
        this.scorecard["format_version"],
        r.scorecard["format_version"]
    );
    assert!(this.scorecard["source"].get("selection").is_none());
    assert_eq!(
        key_paths(&this.scorecard),
        key_paths(&r.scorecard),
        "an unnarrowed run's document has the shape it had"
    );

    // 3. A partition subset as this build writes it: refused like (1).
    let subset = Selection {
        start: None,
        end,
        partitions: [(topic.clone(), vec![0])].into_iter().collect(),
    };
    let (r, target, _) = run("subset", &subset.restore_block(), Some(older.clone()));
    refused("subset", &r, &target);

    // 4. The residual's old spelling: a subset beside a plain instant is
    // IGNORED by the older runner, which restores every partition.
    let (r, _, got) = run(
        "partitions-residual",
        &subset.restore_block_beside_an_instant(),
        Some(older.clone()),
    );
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

/// **Any runner before PROD-11.1b refuses a partition-subset plan as this
/// build writes it** — a runner from before PROD-11.1 (it cannot parse the
/// interval) and main's runner after it (it cannot parse `"../<end>"`, and
/// refuses `restore.partitions` beside `"<start>/<end>"` by name,
/// `PartitionSubsetsAwaitOwnerDecision`). Each: not exit 0, nothing signed, no
/// target topic. And where the older runner reads a window start (main's,
/// after PROD-11.1), its 1.7.0 document and this build's for the same plan
/// are the same: version, block, key paths and coverage note — a start-only
/// run is unchanged.
///
/// IGNORED by default: `LOGWEIR_E2E_OLDER_RUNNER` names the older binary.
#[test]
#[ignore = "needs LOGWEIR_E2E_OLDER_RUNNER: a logweir built before PROD-11.1b"]
fn an_older_runner_refuses_a_partition_subset_plan() {
    let older = std::path::PathBuf::from(std::env::var("LOGWEIR_E2E_OLDER_RUNNER").expect(
        "LOGWEIR_E2E_OLDER_RUNNER names the older logweir binary; this row never passes without it",
    ));
    assert!(older.is_file(), "{} is not a file", older.display());
    let label = std::env::var("LOGWEIR_E2E_OLDER_RUNNER_LABEL")
        .unwrap_or_else(|_| "older-runner".to_string());
    let mut row = Row::new("oldersubset");
    let s = T;
    let topic = row.source_topic("os", &[]);
    let recs = layout(s, "os", &[(0, &[0, 10, 20]), (1, &[5, 15]), (2, &[7, 17])]);
    kafka::produce_plain(&topic, &recs).expect("produce");
    let backup_id = row.backup_id("os");
    backup_ok(&backup_id, &[&topic], 1000);
    let end = s + 1_000;
    let mut cases = Vec::new();
    let mut run = |case: &str, sel: &Selection, by: Option<std::path::PathBuf>| {
        let prefix = row.prefix(case, &[&topic]);
        let target = format!("{prefix}{topic}");
        let r = restore_run_by(
            restore_spec(&backup_id, &[&topic], &prefix, sel, (s, end), false),
            Vec::new(),
            by.clone(),
            Vec::new(),
        );
        cases.push(json!({
            "case": case,
            "runner": by.unwrap_or_else(bin).display().to_string(),
            "plan_restore_block": sel.restore_block(),
            "target_exists": topic_exists(&target),
            "verdict": r.summary(),
        }));
        write_outcome(&format!("{label}-subset"), &json!({ "cases": cases }));
        (r, target)
    };
    for (case, start) in [("subset-floor", None), ("subset-start", Some(s + 10))] {
        let sel = Selection {
            start,
            end,
            partitions: [(topic.clone(), vec![0, 2])].into_iter().collect(),
        };
        let (r, target) = run(case, &sel, Some(older.clone()));
        assert_ne!(r.exit, Some(0), "{case}: {}", r.summary());
        assert!(r.exit.is_some(), "{case}: {}", r.summary());
        assert!(
            r.scorecard.is_null(),
            "{case}: the older runner signed a scorecard"
        );
        assert!(
            !topic_exists(&target),
            "{case}: the older runner created a target topic"
        );
    }
    // A start only: where the older runner reads it, its document is this
    // build's.
    let start_only = Selection {
        start: Some(s + 10),
        end,
        ..Selection::default()
    };
    let (theirs, _) = run("start-only", &start_only, Some(older.clone()));
    if theirs.exit == Some(0) {
        let (ours, _) = run("start-only-this-build", &start_only, None);
        assert_eq!((ours.exit, ours.outcome()), (Some(0), Some("pass")));
        assert_eq!(theirs.scorecard["format_version"], json!("1.7.0"));
        assert_eq!(ours.scorecard["format_version"], json!("1.7.0"));
        assert_eq!(
            ours.scorecard["source"]["selection"],
            theirs.scorecard["source"]["selection"]
        );
        assert_eq!(
            ours.scorecard["sample"]["coverage_note"],
            theirs.scorecard["sample"]["coverage_note"]
        );
        assert_eq!(key_paths(&ours.scorecard), key_paths(&theirs.scorecard));
    }
}

/// **Every reader before 1.25.0 refuses a 2.0.0 document** (the owner's
/// decision OD-9 (a)), never prints it `VALID`. Reads the signed 2.0.0
/// documents `partition_subsets_on_two_topics_…` kept
/// (`scorecard-subset-<label>.json`) with each reader named in
/// `LOGWEIR_E2E_OLDER_READERS` (`:`-separated): a `verify_scorecard.py` from
/// git history runs under the auditor's Python and must refuse with its
/// major-version sentence (exit 1); an older `logweir` runs `drill verify`
/// and must not exit 0 (a reader from before PROD-11.1 refuses the major,
/// exit 4; main's after it refuses a block without `window_start_ms` at
/// deserialisation, exit 1, and the others as a newer major, exit 4). The
/// control: this tree's two readers accept every one.
///
/// IGNORED by default: the older readers are files outside this tree.
#[test]
#[ignore = "needs LOGWEIR_E2E_OLDER_READERS and the subset row's kept scorecards"]
fn older_readers_refuse_a_2_0_0_document() {
    let readers = std::env::var("LOGWEIR_E2E_OLDER_READERS").expect(
        "LOGWEIR_E2E_OLDER_READERS names older readers, `:`-separated; this row never passes \
         without it",
    );
    let dir = demo_dir().join("replay-selection");
    let key = root().join("e2e/fixtures/signed/public.pem");
    let mut results = Vec::new();
    for label in ["complete", "sampled", "sampled-start"] {
        let doc = dir.join(format!("scorecard-subset-{label}.json"));
        let sig = dir.join(format!("scorecard-subset-{label}.sig"));
        assert!(
            doc.is_file() && sig.is_file(),
            "{} is missing: run partition_subsets_on_two_topics_… first",
            doc.display()
        );
        let version = serde_json::from_slice::<Value>(&std::fs::read(&doc).unwrap()).unwrap()
            ["format_version"]
            .clone();
        assert_eq!(version, json!("2.0.0"), "{label}");
        let (rust_rc, py_rc, rust_out, py_out) = read_with_both(&doc, &sig);
        assert_eq!(
            (rust_rc, py_rc),
            (0, 0),
            "this tree's readers accept it: {rust_out}\n{py_out}"
        );
        for reader in readers.split(':').filter(|r| !r.is_empty()) {
            let path = std::path::Path::new(reader);
            assert!(path.is_file(), "{reader} is not a file");
            let o = if reader.ends_with(".py") {
                std::process::Command::new(auditor_python())
                    .arg(path)
                    .arg(&doc)
                    .arg(&sig)
                    .arg(&key)
                    .output()
                    .expect("an older verify_scorecard.py")
            } else {
                std::process::Command::new(path)
                    .args(["drill", "verify", "--scorecard"])
                    .arg(&doc)
                    .arg("--signature")
                    .arg(&sig)
                    .arg("--public-key")
                    .arg(&key)
                    .output()
                    .expect("an older logweir")
            };
            let out = format!("{}{}", o.stdout_utf8(), o.stderr_utf8());
            results.push(json!({
                "document": label,
                "reader": reader,
                "exit": o.status.code(),
                "output": out,
            }));
            write_outcome("older-readers", &json!({ "results": results }));
            assert_ne!(o.status.code(), Some(0), "{reader} accepted {label}: {out}");
            assert!(
                !out.contains("VALID  run_id") && !out.contains("signature: VALID  key"),
                "{reader} printed {label} VALID: {out}"
            );
            if reader.ends_with(".py") {
                assert_eq!(o.status.code(), Some(1), "{reader}: {out}");
                assert!(
                    out.contains(
                        "format_version 2.0.0 has a major version newer than this reader \
                         understands"
                    ),
                    "{reader} must refuse {label} as an unsupported major: {out}"
                );
            } else {
                assert!(
                    matches!(o.status.code(), Some(1) | Some(4)),
                    "{reader}: {out}"
                );
            }
        }
    }
}
