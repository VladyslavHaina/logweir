#![cfg(feature = "e2e")]
//! **Guard G-PITR — the boundary oracle the engine's own test suite does not
//! have.** Task 11.
//!
//! # Why this file exists
//!
//! `U:crates/kafka-backup-core/tests/integration_suite/pitr_accuracy.rs`
//! declares **six** tests — accuracy, boundary-inclusive, multi-partition
//! consistency, millisecond precision, empty window, and
//! `test_full_restore_no_pitr` (`:25,40,54,66,78,86`) — and every one is
//! `#[ignore = "requires Docker"]` with a body that is one `println!` and a
//! comment beginning "This test would:". **The file contains zero assertions**
//! (`grep -c assert` → 0). So the filter at the centre of this product has no
//! executable evidence at engine v0.21.0, the `<=` boundary included.
//!
//! The source-level answer is readable and inclusive —
//! `r.timestamp >= s && r.timestamp <= e`
//! (`U:crates/kafka-backup-core/src/restore/helpers.rs:74-82`) — so this row
//! **confirms a stated expectation** rather than discovering one, which is
//! what makes it cheap and sharp. It runs on hardware that already exists
//! (Global Constraint 17): the compose stack and the digest-pinned engine.
//!
//! # The fixture states its instants, and does not read a clock
//!
//! `T` below is a literal and is never read from the wall clock: a
//! wall-clock-relative fixture makes the assertion irreproducible, and
//! `crates/logweir/tests/pitr_fixture_shape.rs::pitr_fixture_uses_a_fixed_epoch`
//! reads THIS FILE as text and refuses the chrono clock-read token in it (that
//! test is in the default set, where this file — being
//! `#![cfg(feature = "e2e")]` — cannot reach). The one `SystemTime::now()`
//! below is the `backup_id` nonce and reaches no assertion: it exists so each
//! run writes a FRESH archive prefix, because the engine's `backup` does not
//! accumulate into an existing one.
//!
//! It also cannot be the seeded archive. `scripts/e2e-seed.sh` produces
//! through `kafka-console-producer`, which stamps every record with the moment
//! it was written, so the seeded archive's whole restore window is seconds
//! wide (measured 4,337 ms and 6,728 ms on two seeds) and `point_in_time ± 1
//! ms` is not addressable in it. So this row produces its own records with
//! explicit `CreateTime` (`harness::produce_with_timestamps`), takes its own
//! backup with `logweir backup run` under its own `backup_id`, and **sweeps
//! that archive out of the shared bucket at the start of the row and from a
//! `Drop` guard** — so the panicking path leaves the bucket clean too. See
//! `sweep_pitr_archives` and `SweptArchive`.
//!
//! # What it asserts, and what it deliberately does not
//!
//! Nine records: `T − 1 ms`, `T`, `T + 1 ms` on **each** of three partitions.
//! A restore at `point_in_time = T` must bring back the six at or before `T`
//! and none of the three after it — asserted as a **payload SET**, never as a
//! count equality. Spec amendment 1 and critique A F6: `point_in_time` falls
//! INSIDE a segment on any real archive, so the manifest can only bound the
//! restored count (`expected_restored_count` → `[lower, upper]`), and a count
//! equality here would fail a correct implementation the moment the fixture
//! grew. The count is proved only to be inside the bound; the boundary is
//! proved by the set.
mod harness;
use harness::*;

use logweir_core::engine::DataEngine;
use logweir_kafka::reader::ClusterReader;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

/// **THE POINT IN TIME. A FIXED LITERAL, NEVER A CLOCK READ.**
///
/// `1_760_000_000_000` ms = **2025-10-09T08:53:20Z**, computed before it was
/// used (plan errata E6/E7 — a brief's quoted epoch is checked with
/// `python3 -c 'import datetime as d; …'` first):
///
/// ```text
/// 1760000000000 -> 2025-10-09T08:53:20Z          (T, the recovery point)
/// 1759999999999 -> 2025-10-09T08:53:19.999000Z   (T - 1 ms, restored)
/// 1760000000001 -> 2025-10-09T08:53:20.001000Z   (T + 1 ms, NOT restored)
/// 1759999999000 -> 2025-10-09T08:53:19Z          (T - 1000 ms, the sample floor)
/// ```
const T: i64 = 1_760_000_000_000;

/// `T` as the spec spells it. Parsed by the SHIPPED binary, and this file
/// asserts the round trip rather than trusting it:
/// `the parsed point_in_time is T` below compares
/// `RestoreSpec.restore.point_in_time.timestamp_millis()` with `T`.
const PIT_RFC3339: &str = "2025-10-09T08:53:20Z";

/// `T − 1000 ms`. The SAMPLE window's start — the restore window's floor is
/// the ARCHIVE's and is never spec-supplied (guard G-WIN).
const SAMPLE_START_RFC3339: &str = "2025-10-09T08:53:19Z";

/// The sample window's END is `T` exactly, which matters: `phase4_sample::run`
/// takes `sample.window_{start,end}` verbatim (`point_in_time` supersedes it
/// only for the RESTORE window), and `OsoCliEngine::fingerprints` filters the
/// archive side with `r.timestamp < window.0 || r.timestamp > window.1`. A
/// sample window reaching past `T` would fingerprint the three `T + 1 ms`
/// records the restore correctly did not write, and report them as missing.
const SAMPLE_END_RFC3339: &str = PIT_RFC3339;

/// `sample.records_per_partition`, and **2 is load-bearing** — measured, not
/// guessed.
///
/// It is the CANARY SIZE: how many records per partition this restore sets out
/// to reconcile. Two is how many records each partition holds inside the
/// window (`T − 1 ms` and `T`), and asking for more is refused by the product
/// over a real property of a MANIFEST — segment granularity — though the
/// refusal itself is a known limitation of the sampled reconciliation's
/// `claimed`, filed as Task 10b. With `records_per_partition: 25` this row
/// measured, on the first full run:
///
/// ```text
/// selection pitr-src/1  claimed 3  records Unverified { why: "the archive returned 2
///   fingerprints where the manifest claims 3 for this selection; a short sample is
///   coverage the drill did not obtain, not a smaller successful sample" }
/// ```
///
/// `verdict_for_selection` computes its claim as
/// `min(records_per_partition, Σ manifest record_count over the segments overlapping the
/// sample window)`. The manifest's finest granularity is the SEGMENT, and the
/// one segment per partition here holds all three records — so it claims 3 —
/// while `OsoCliEngine::fingerprints` correctly returns only the 2 inside the
/// window. That gap is inherent to a recovery point that falls INSIDE a
/// segment, which is the normal case for point-in-time recovery and the same
/// fact that makes `expected_restored_count` a BOUND rather than an equality.
/// What is NOT inherent is counting that gap as a shortfall: nothing was
/// truncated, the archive returned every record the window holds, and
/// `claimed` sums records the window excludes — so a correct restore scores
/// `fail-integrity`, exit 2, with `mismatches: 0`, on all three partitions.
/// Task 10b owns that fix. Until it lands a canary of 2 asks for what the
/// window really holds, so the record lane reconciles 2 of 2 per partition and
/// the run scores `pass`. Recorded in `docs/stability.md` beside the result.
const RECORDS_PER_PARTITION: usize = 2;

const SRC_TOPIC: &str = "pitr-src";
const PARTITIONS: i32 = 3;

/// Every `backup_id` this row has ever used starts with this, and nothing else
/// in the tree does — which is what lets `sweep_pitr_archives` be exact rather
/// than heuristic.
const PITR_ID_PREFIX: &str = "pitr-";

/// Global Constraint 6: everything `logweir backup run` puts lives under this.
const RECEIPT_PREFIX: &str = "logweir/";

/// **THE FIXTURE. Nine records, timestamp-major.**
///
/// `produce_with_timestamps` places `records[i]` on partition
/// `i % <partition count>` — there is no partition argument, so placement is
/// by ORDER. Listed timestamp-major with three partitions, that puts exactly
/// `(T − 1, T, T + 1)` on each of partitions 0, 1, 2, at that partition's
/// offsets 0, 1, 2. The payload names the partition it is meant for, so a
/// misplacement is visible in the assertion's own message rather than hidden
/// in an index.
///
/// | i | partition | timestamp | payload | restored at `point_in_time = T`? |
/// |---|---|---|---|---|
/// | 0 | 0 | `T − 1` | `p0-before-1ms` | yes |
/// | 1 | 1 | `T − 1` | `p1-before-1ms` | yes |
/// | 2 | 2 | `T − 1` | `p2-before-1ms` | yes |
/// | 3 | 0 | `T`     | `p0-boundary`   | **yes — the whole point** |
/// | 4 | 1 | `T`     | `p1-boundary`   | **yes** |
/// | 5 | 2 | `T`     | `p2-boundary`   | **yes** |
/// | 6 | 0 | `T + 1` | `p0-after-1ms`  | no |
/// | 7 | 1 | `T + 1` | `p1-after-1ms`  | no |
/// | 8 | 2 | `T + 1` | `p2-after-1ms`  | no |
const FIXTURE: [(i64, &str); 9] = [
    (T - 1, "p0-before-1ms"),
    (T - 1, "p1-before-1ms"),
    (T - 1, "p2-before-1ms"),
    (T, "p0-boundary"),
    (T, "p1-boundary"),
    (T, "p2-boundary"),
    (T + 1, "p0-after-1ms"),
    (T + 1, "p1-after-1ms"),
    (T + 1, "p2-after-1ms"),
];

/// The six payloads a `point_in_time = T` restore must bring back — the
/// EXPECTED SET, stated once.
const RESTORED_PAYLOADS: [&str; 6] = [
    "p0-before-1ms",
    "p0-boundary",
    "p1-before-1ms",
    "p1-boundary",
    "p2-before-1ms",
    "p2-boundary",
];

/// The three payloads it must NOT bring back.
const ABSENT_PAYLOADS: [&str; 3] = ["p0-after-1ms", "p1-after-1ms", "p2-after-1ms"];

/// One source or target record, keyed by payload — the only stable identity a
/// restored record has, because a restored topic's own offsets need not equal
/// the source's (which is why `x-original-offset` exists at all).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Rec {
    partition: i32,
    offset: i64,
    timestamp_ms: i64,
    /// `x-original-offset`, exactly as the record carries it. `None` when the
    /// header is absent — which is itself asserted, never tolerated.
    original_offset_raw: Option<Vec<u8>>,
}

/// Reads every record of `topic` across `partitions` partitions, keyed by
/// payload. Used on BOTH sides — the source and the restored topic — so the
/// comparison below is between two readings of the same kind.
fn read_all(topic: &str, partitions: i32) -> BTreeMap<String, Rec> {
    let r = reader();
    let mut out = BTreeMap::new();
    for p in 0..partitions {
        // 16 > 3, so the read cannot truncate this fixture; `consume_range`
        // returns what is there rather than blocking for `max`.
        for c in ClusterReader::consume_range(&r, topic, p, 0, 16).unwrap_or_else(|e| {
            panic!("consume_range({topic}, {p}): {e}");
        }) {
            let payload = String::from_utf8_lossy(c.value.as_deref().unwrap_or(b"")).into_owned();
            let original_offset_raw = c
                .headers
                .iter()
                .find(|(k, _)| k == "x-original-offset")
                .and_then(|(_, v)| v.clone());
            let prev = out.insert(
                payload.clone(),
                Rec {
                    partition: c.partition,
                    offset: c.offset,
                    timestamp_ms: c.timestamp_ms,
                    original_offset_raw,
                },
            );
            assert!(
                prev.is_none(),
                "payload {payload:?} appears twice in {topic}; this fixture's payloads are \
                 unique by construction and are its record identity"
            );
        }
    }
    out
}

/// Deletes the topics this row owns and waits for the broker's metadata to
/// agree, so a run that died mid-flight cannot leave the next one producing
/// into a topic that already holds records.
fn delete_topics_and_wait(topics: &[String]) {
    for t in topics {
        // Best effort: a topic that is not there is the state we want, and
        // `kafka-topics --delete` on an absent topic is an error we do not
        // care about. The POLL below is the assertion.
        let _ = kafka_topics(&[
            "--bootstrap-server",
            "kafka-broker-1:9094",
            "--delete",
            "--topic",
            t,
        ]);
    }
    for _ in 0..60 {
        if topics.iter().all(|t| !topic_exists(t)) {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    panic!("{topics:?}: still present 15s after --delete");
}

/// Puts the SHARED broker state back on **every** exit path, including a
/// panicking assertion.
///
/// This is Task 9b's `RestoredBrokerState` (`e2e/tests/offset_side.rs`) reused
/// verbatim in shape, and for its reason: `#[test]` unwinds (this workspace
/// sets no `panic = "abort"`), so `Drop` runs on the panicking path too, and
/// one red row must not turn into four by leaving `pitr-src` and a
/// `restore-<pit>-…` topic behind. Neither name is swept by anything else —
/// `delete_all_drill_topics` is `drill-` scoped and phase 9 deliberately tears
/// nothing down in `newTopic` mode (Global Constraint 19).
///
/// The marker topic is NOT touched by this row, so unlike Task 9b's guard
/// there is nothing to recreate.
struct RestoredBrokerState {
    targets: Vec<String>,
}

impl Drop for RestoredBrokerState {
    fn drop(&mut self) {
        for t in &self.targets {
            let _ = kafka_topics(&[
                "--bootstrap-server",
                "kafka-broker-1:9094",
                "--delete",
                "--topic",
                t,
            ]);
        }
    }
}

/// An allowlist that does NOT name the live cluster. `--allowed-clusters` is
/// the restore-TARGET allowlist, and GC18(c) rail 4 refuses a SOURCE cluster
/// that appears in it — so the compose broker must be absent from it for a
/// backup OF that broker to be admitted at all. (`harness`'s own allowlist
/// writer does the opposite, because a `scratch` drill needs the live id
/// present; `newTopic` needs neither, which is difference 2 of the four.)
fn backup_allowlist() -> PathBuf {
    let p = demo_dir().join("pitr-backup-allowed-clusters.json");
    std::fs::write(
        &p,
        "{\"allowed_cluster_ids\": [\"SCRATCH-CLUSTER-NOT-THE-SOURCE\"]}\n",
    )
    .unwrap();
    p
}

/// The host-side backup spec, over THIS row's topic and `backup_id`.
fn backup_spec(backup_id: &str) -> PathBuf {
    let p = demo_dir().join("pitr-backup.yaml");
    std::fs::write(
        &p,
        format!(
            "backup_id: {backup_id}\n\
             source:\n\
             \x20 bootstrap_servers: [{BOOTSTRAP}]\n\
             \x20 topics: [{SRC_TOPIC}]\n\
             storage:\n\
             \x20 backend: s3\n\
             \x20 bucket: {ARCHIVE_BUCKET}\n\
             \x20 prefix: {backup_id}\n\
             \x20 region: us-east-1\n\
             \x20 endpoint: http://localhost:9000\n\
             \x20 path_style: true\n\
             \x20 allow_http: true\n\
             backup:\n\
             \x20 compression: zstd\n\
             \x20 segment_max_records: 1000\n\
             \x20 segment_max_bytes: 10485760\n\
             \x20 max_concurrent_partitions: 3\n"
        ),
    )
    .unwrap();
    p
}

/// `logweir backup run` with the **REAL**, digest-pinned engine. A stub
/// accepts any document carrying `mode: backup` and therefore cannot write an
/// archive at all, and an archive is what this row's restore reads.
fn backup_run_real_engine(spec: &Path) -> Command {
    let mut c = Command::new(bin());
    c.args(["backup", "run", "--spec"])
        .arg(spec)
        .arg("--allowed-clusters")
        .arg(backup_allowlist())
        .arg("--signing-key")
        .arg(root().join("e2e/fixtures/signed/signing.pem"))
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .env("AWS_REGION", "us-east-1")
        .env("LOGWEIR_ENGINE_BIN", engine_bin())
        .env("LOGWEIR_ENGINE_VERSION", engine_version())
        .env("LOGWEIR_ENGINE_DIGEST", engine_digest())
        .env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount())
        .env("TMPDIR", engine_mount());
    c
}

/// The restore plan: `mode: newTopic`, `point_in_time = T`, no
/// `topic_naming` block (so the default `restore-<YYYYmmddTHHMMSSZ>-` prefix
/// is exercised), over the archive this row just wrote.
fn restore_spec(backup_id: &str) -> serde_yaml::Value {
    serde_yaml::from_str(&format!(
        "source:\n\
         \x20 storage:\n\
         \x20   backend: s3\n\
         \x20   bucket: {ARCHIVE_BUCKET}\n\
         \x20   prefix: {backup_id}\n\
         \x20   region: us-east-1\n\
         \x20   endpoint: http://localhost:9000\n\
         \x20   path_style: true\n\
         \x20   allow_http: true\n\
         \x20 backup: {backup_id}\n\
         \x20 topics: [{SRC_TOPIC}]\n\
         target:\n\
         \x20 bootstrap_servers: [{BOOTSTRAP}]\n\
         \x20 mode: newTopic\n\
         \x20 topic_mapping_prefix: \"drill-\"\n\
         \x20 default_replication_factor: 1\n\
         restore:\n\
         \x20 point_in_time: \"{PIT_RFC3339}\"\n\
         sample:\n\
         \x20 window_start: \"{SAMPLE_START_RFC3339}\"\n\
         \x20 window_end: \"{SAMPLE_END_RFC3339}\"\n\
         \x20 records_per_partition: {RECORDS_PER_PARTITION}\n\
         \x20 anchor: head\n\
         objectives:\n\
         \x20 rto_seconds: 900\n\
         \x20 rpo_seconds: 300\n\
         \x20 pass_rate: 1.0\n\
         evidence:\n\
         \x20 backend: s3\n\
         \x20 bucket: {EVIDENCE_BUCKET}\n\
         \x20 prefix: {RECEIPT_PREFIX}\n\
         \x20 region: us-east-1\n\
         \x20 endpoint: http://localhost:9000\n\
         \x20 path_style: true\n\
         \x20 allow_http: true\n"
    ))
    .expect("the pitr restore spec is valid YAML")
}

/// Remove every archive and receipt this row has ever left in the SHARED
/// buckets, and prove it.
///
/// Not optional, and not only at the end. `harness::corrupt_a_non_oldest_
/// segment` picks its victim by listing the archive bucket's ROOT and taking
/// the LAST key in sort order, and `pitr-…` sorts after `drill-demo/…` — so a
/// leftover archive here makes
/// `a_corrupted_segment_yields_exit_2_and_a_signed_preflight_failed_scorecard`
/// quarantine THIS row's segment and then watch an intact drill archive exit
/// 0. That was measured once already, in Task 4's fix round, for the
/// `t4real-…` prefix. Swept at the START of the row as well, so a row that died
/// mid-flight in an EARLIER run cannot poison a later one — and, since Task 11
/// fix round 1, from a `Drop` guard rather than from a last statement, so THIS
/// row dying mid-flight cannot poison a later one either (see `SweptArchive`).
///
/// Returns the keys that SURVIVED the sweep instead of asserting on them, so
/// each caller can decide what a survivor means: an assertion on a normal
/// path, a stderr diagnostic on an unwinding one — where a second panic would
/// abort the process and take this row's real failure message with it.
fn sweep_archive_keys() -> Vec<String> {
    let mine = |keys: Vec<String>| -> Vec<String> {
        keys.into_iter()
            .filter(|k| {
                k.starts_with(PITR_ID_PREFIX)
                    || k.starts_with(&format!("{RECEIPT_PREFIX}backups/{PITR_ID_PREFIX}"))
            })
            .collect()
    };
    let list = || -> Vec<String> {
        let o = mc(&[
            "--json",
            "ls",
            "--recursive",
            &format!("local/{ARCHIVE_BUCKET}"),
        ]);
        o.stdout_utf8()
            .lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .filter_map(|v| v["key"].as_str().map(str::to_string))
            .collect()
    };

    let found = mine(list());
    let prefixes: BTreeSet<String> = found
        .iter()
        .map(|k| {
            let segs: Vec<&str> = k.split('/').collect();
            if k.starts_with(RECEIPT_PREFIX) {
                // `logweir/backups/<backup_id>/…`
                segs[..3].join("/")
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
    mine(list())
}

/// Sweep, and REQUIRE that nothing of this row's survived it.
///
/// The start-of-row call. A survivor here means an earlier run left an archive
/// behind, which is precisely the condition that would mis-aim
/// `harness::corrupt_a_non_oldest_segment`, so it is an assertion.
fn sweep_pitr_archives() {
    let left = sweep_archive_keys();
    assert!(
        left.is_empty(),
        "this row's archive is still in {ARCHIVE_BUCKET}, and \
         harness::corrupt_a_non_oldest_segment would quarantine one of ITS segments instead \
         of the drill's: {left:?}"
    );
}

/// Puts the SHARED ARCHIVE BUCKET back on **every** exit path, including a
/// panicking assertion — the object-store half of what `RestoredBrokerState`
/// does for the broker.
///
/// **Why this is a second guard and not a call inside
/// `RestoredBrokerState::drop`.** Two reasons, and the second is the
/// load-bearing one:
///
/// 1. Two shared resources, two owners. `RestoredBrokerState` is Task 9b's
///    guard reused verbatim in shape (see its own doc comment), it deletes
///    broker topics, and every statement in its `drop` is an ignored `let _`.
///    Bucket keys are a different resource with a different failure mode.
/// 2. **A sweep that fails must be able to assert, and a `Drop` that panics
///    while the thread is already unwinding ABORTS the process** — which would
///    destroy the assertion message this row exists to print. Only a
///    purpose-built guard can branch on `std::thread::panicking()`; folding
///    that branch into the topic guard would make a guard that never panics
///    panic conditionally.
///
/// Before this guard existed the sweep ran as the row's LAST STATEMENT, so a
/// panic anywhere after the backup left `pitr-<nonce>/…` in the shared
/// `kafka-backups` bucket. `harness::archive_segment_keys` lists the bucket
/// ROOT and `corrupt_a_non_oldest_segment` takes the LAST key in sort order —
/// and `pitr-…` sorts after `drill-demo/…` — so the leftover would be
/// quarantined on a LATER run in place of the drill's own segment, and
/// `full_drill`'s corruption row would then watch an intact drill archive exit
/// 0. Within one suite run the alphabet hides it (`full_drill` runs before
/// `pitr_boundary`) and the start-of-row sweep protects this row itself;
/// neither protects the next run against the same volume. `--no-fail-fast`
/// (this task's own addition to `just e2e`) makes "the suite carried on after a
/// panicking row" the normal case, so the panicking path is not exotic.
struct SweptArchive;

impl Drop for SweptArchive {
    fn drop(&mut self) {
        let left = sweep_archive_keys();
        if std::thread::panicking() {
            // The row is already failing and its message is already on stderr.
            // Say what the sweep could not remove and let the real failure
            // stand; an assertion here would abort the process instead.
            if !left.is_empty() {
                eprintln!(
                    "[pitr] SWEEP INCOMPLETE on the panicking path — still in \
                     {ARCHIVE_BUCKET}: {left:?}. \
                     harness::corrupt_a_non_oldest_segment may quarantine one of these \
                     instead of the drill's segment on the next run; \
                     `just e2e-down -v` clears it."
                );
            }
        } else {
            assert!(
                left.is_empty(),
                "this row's archive is still in {ARCHIVE_BUCKET}, and \
                 harness::corrupt_a_non_oldest_segment would quarantine one of ITS segments \
                 instead of the drill's: {left:?}"
            );
        }
    }
}

/// **G-PITR.** The inclusive point-in-time boundary, across three partitions,
/// on the real stack.
///
/// Every claim is its own `assert_eq!`/`assert!` so a reviewer can see which
/// one a mutant killed.
#[test]
fn pitr_boundary_includes_the_record_whose_timestamp_equals_point_in_time() {
    // ---------------------------------------------------------------- 0. setup
    // The in-process `Store` handle at the end of this row reads MinIO through
    // `AmazonS3Builder::from_env()`. `set_var` is process-wide, and each file
    // under `e2e/tests/` is its OWN test binary holding exactly one `#[test]`
    // — so this reaches nothing but this row, unlike a `set_var` in the shared
    // harness (see `RunOpts::env`'s doc comment for why that one is a field).
    std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
    std::env::set_var("AWS_REGION", "us-east-1");

    sweep_pitr_archives();

    let backup_id = format!(
        "{PITR_ID_PREFIX}{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let spec = restore_spec(&backup_id);

    // The expected target name comes from the PRODUCT's own rule, through the
    // very `RestoreSpec` the binary parses and the very `target_topic_prefix`
    // phase 0 maps through — never from a string this test builds by hand
    // (Task 9b fix round 1, F5a: a test that re-implements the rule it is
    // checking asserts a property of itself).
    let parsed: logweir_core::spec::RestoreSpec =
        serde_yaml::from_value(spec.clone()).expect("the binary parses this spec as a RestoreSpec");
    let pit_parsed = parsed
        .restore
        .point_in_time
        .expect("the spec states a recovery point");
    // E6/E7: the epoch is asserted, not assumed. `PIT_RFC3339` is
    // 2025-10-09T08:53:20Z and `T` is 1_760_000_000_000 — the same instant.
    assert_eq!(
        pit_parsed.timestamp_millis(),
        T,
        "the spec's point_in_time ({PIT_RFC3339}) must be T = {T} ms"
    );
    let prefix = logweir_core::spec::target_topic_prefix(&parsed);
    let target_topic = format!("{prefix}{SRC_TOPIC}");
    eprintln!("[pitr] point_in_time={PIT_RFC3339} (T={T}) -> target topic {target_topic}");

    // Armed BEFORE the first topic is created, so every exit path — including
    // a panicking assertion — puts the shared broker back.
    let _restored = RestoredBrokerState {
        targets: vec![SRC_TOPIC.to_string(), target_topic.clone()],
    };
    // The archive half of the same invariant, armed at the same point and for
    // the same reason (F-MED, Task 11 review). Declared AFTER `_restored`, so
    // it drops FIRST: the bucket is swept, then the topics go. Before section 2
    // there is no archive and its drop is a no-op listing.
    let _archive = SweptArchive;
    delete_topics_and_wait(&[SRC_TOPIC.to_string(), target_topic.clone()]);

    // ------------------------------------------------- 1. the fixture, produced
    // `message.timestamp.type=CreateTime` is STATED, not inherited: on a
    // `LogAppendTime` broker every timestamp below would be replaced by the
    // producer's wall clock and this row would be asserting about now(). Task
    // 8 closed residual 3 by execution — this broker honours the per-topic
    // override.
    create_topic_with_configs(
        SRC_TOPIC,
        PARTITIONS,
        &[("message.timestamp.type", "CreateTime")],
    );
    produce_with_timestamps(SRC_TOPIC, &FIXTURE).expect("the fixture is produced");

    // The SOURCE, read back off the broker: nine records, and the explicit
    // timestamps survived. Without this the boundary claim would rest on what
    // the producer was ASKED for rather than on what the broker holds.
    let source = read_all(SRC_TOPIC, PARTITIONS);
    assert_eq!(
        source.len(),
        FIXTURE.len(),
        "the source topic must hold all {} fixture records, got {:?}",
        FIXTURE.len(),
        source.keys().collect::<Vec<_>>()
    );
    for (i, (ts, payload)) in FIXTURE.iter().enumerate() {
        let rec = source
            .get(*payload)
            .unwrap_or_else(|| panic!("{payload} is absent from the source topic"));
        assert_eq!(
            rec.timestamp_ms, *ts,
            "{payload} was produced with CreateTime {ts} and the broker reports \
             {}; a LogAppendTime topic would overwrite every fixture timestamp",
            rec.timestamp_ms
        );
        assert_eq!(
            rec.partition,
            (i % PARTITIONS as usize) as i32,
            "{payload} is record {i} of the fixture, so round-robin placement puts it on \
             partition {}",
            i % PARTITIONS as usize
        );
    }

    // ------------------------------------------------------- 2. the backup
    let bspec = backup_spec(&backup_id);
    let bout = backup_run_real_engine(&bspec).output().expect("logweir");
    assert_eq!(
        bout.status.code(),
        Some(0),
        "`logweir backup run` over {SRC_TOPIC} must exit 0\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&bout.stdout),
        String::from_utf8_lossy(&bout.stderr)
    );
    let manifest_key = format!("{backup_id}/{backup_id}/manifest.json");
    let listed = mc(&[
        "--json",
        "ls",
        "--recursive",
        &format!("local/{ARCHIVE_BUCKET}/{backup_id}/"),
    ]);
    let keys: Vec<String> = listed
        .stdout_utf8()
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|v| v["key"].as_str().map(str::to_string))
        .collect();
    assert!(
        keys.iter().any(|k| k.ends_with("manifest.json")),
        "no manifest under {ARCHIVE_BUCKET}/{backup_id}/ — the engine exited 0 without \
         writing an archive. mc listed: {keys:?}"
    );

    // ------------------------------------------------------- 3. the restore
    let mut o = RunOpts::new(&spec);
    // The CANONICAL subcommand, not the tag-0 alias.
    o.restore_run = true;
    let r = run_with(o);
    assert!(
        topic_exists(&target_topic),
        "the newTopic restore must have created {target_topic}; the run exited {:?}\n\
         stdout:\n{}\nstderr:\n{}",
        r.out.status.code(),
        r.out.stdout_utf8(),
        r.out.stderr_utf8()
    );

    // The partition COUNT, asserted before the topic is read — a restore into
    // one partition instead of three is a real mutant (`restore_partition_count`
    // → 1), and without this line it surfaces as an `UnknownPartition` error
    // inside the test's own read helper (measured) instead of as the assertion
    // that names what went wrong. `read_all` below reads partitions 0..3 and
    // is allowed to assume they exist because THIS is what established it.
    assert_eq!(
        count_partitions(&target_topic),
        PARTITIONS,
        "the restored topic must keep the source's partition count; a restore that \
         collapsed three partitions into one would satisfy the payload-set assertion \
         below and lose the per-partition boundary claim"
    );

    // --------------------------------------------- 4. THE BOUNDARY ASSERTIONS
    //
    // THESE COME BEFORE THE EXIT CODE, deliberately. The boundary is a
    // property of the RESTORED TOPIC, read straight off the broker; the exit
    // code and the scorecard below are the product's own verdict ABOUT that
    // topic, and phase 7 reconciles the same records, so a mutant that moves
    // the rendered window edge by one millisecond fails both. Asserting the
    // topic's contents first makes such a mutant report itself as the boundary
    // failure it is — naming the record that moved — instead of as "the run
    // did not exit 0", which is true, is what a reviewer sees second, and says
    // nothing about which millisecond was wrong.
    let restored = read_all(&target_topic, PARTITIONS);

    // (a) the restored payload SET is exactly the six at or before T.
    let got: BTreeSet<&str> = restored.keys().map(String::as_str).collect();
    let want: BTreeSet<&str> = RESTORED_PAYLOADS.iter().copied().collect();
    assert_eq!(
        got, want,
        "the restored payload set must be exactly the six records at or before \
         point_in_time={PIT_RFC3339}"
    );

    // (b) the three records one millisecond AFTER the boundary are absent,
    //     each as its own assertion.
    for p in ABSENT_PAYLOADS {
        assert!(
            !restored.contains_key(p),
            "{p} is at T + 1 ms and must NOT be restored at point_in_time={PIT_RFC3339}; \
             got {got:?}"
        );
    }

    // (c) **THE ASSERTION THE WHOLE GUARD EXISTS FOR**: the record whose
    //     timestamp EQUALS point_in_time is present — on every partition —
    //     and is still at that timestamp. The window is a closed interval
    //     (`timestamp >= start && timestamp <= end`), so an implementation
    //     that rendered `point_in_time - 1` as the window end would pass every
    //     other assertion in this file and fail exactly here.
    for p in ["p0-boundary", "p1-boundary", "p2-boundary"] {
        let rec = restored.get(p).unwrap_or_else(|| {
            panic!(
                "{p} has timestamp == point_in_time and the boundary is INCLUSIVE, so it must \
                 be restored; got {got:?}"
            )
        });
        assert_eq!(
            rec.timestamp_ms, T,
            "{p} must come back at exactly T = {T} ms ({PIT_RFC3339})"
        );
    }

    // (d) every restored record carries an `x-original-offset` header of
    //     exactly 8 bytes whose `decode_original_offset` value is its SOURCE
    //     offset. Read through the product's own decoder (Task 10, I5), never
    //     re-implemented here: it is an 8-byte little-endian i64 or `None`.
    for (payload, rec) in &restored {
        let raw = rec.original_offset_raw.as_ref().unwrap_or_else(|| {
            panic!(
                "{payload} came back without an `x-original-offset` header; the backup writes \
                 one per record and the restore renders `strip_offset_headers: false`, so its \
                 absence means reconciliation fell back to the target's own offsets"
            )
        });
        assert_eq!(
            raw.len(),
            8,
            "`x-original-offset` on {payload} must be 8 bytes (a little-endian i64), got {raw:?}"
        );
        let decoded = logweir::drill::phase7_verify::decode_original_offset(raw)
            .unwrap_or_else(|| panic!("`x-original-offset` on {payload} did not decode: {raw:?}"));
        assert_eq!(
            decoded, source[payload].offset,
            "`x-original-offset` on {payload} must be its SOURCE offset"
        );
    }

    // (e) all three source partitions are represented, and each record came
    //     back on the partition it was produced to. A restore that collapsed
    //     three partitions into one would satisfy (a)-(d) and fail here.
    let restored_partitions: BTreeSet<i32> = restored.values().map(|r| r.partition).collect();
    assert_eq!(
        restored_partitions,
        BTreeSet::from([0, 1, 2]),
        "all three source partitions must be represented in {target_topic}"
    );
    for (payload, rec) in &restored {
        assert_eq!(
            rec.partition, source[payload].partition,
            "{payload} was produced to partition {} and came back on {}",
            source[payload].partition, rec.partition
        );
    }
    // (f) **THE BOUNDARY, STATED PER PARTITION.** The global set in (a) is
    //     satisfiable in principle by a restore that brought back six records
    //     concentrated anywhere; this says the inclusive boundary held on each
    //     of the three partitions independently — exactly two records, the one
    //     at `T − 1 ms` and the one AT `T`. It is also the transcript this row
    //     files in `docs/stability.md`.
    for part in 0..PARTITIONS {
        let mut got_on_part: Vec<&str> = restored
            .iter()
            .filter(|(_, rec)| rec.partition == part)
            .map(|(payload, _)| payload.as_str())
            .collect();
        got_on_part.sort_unstable();
        let want_on_part = vec![format!("p{part}-before-1ms"), format!("p{part}-boundary")];
        assert_eq!(
            got_on_part, want_on_part,
            "partition {part} must hold exactly its `T - 1 ms` and its `T` record — the \
             boundary is inclusive on every partition, not on average"
        );
        eprintln!(
            "[pitr] partition {part}: {}",
            got_on_part
                .iter()
                .map(|payload| {
                    let rec = &restored[*payload];
                    let orig = logweir::drill::phase7_verify::decode_original_offset(
                        rec.original_offset_raw.as_deref().unwrap_or(&[]),
                    );
                    format!(
                        "{payload}@ts={} offset={} x-original-offset={:?}",
                        rec.timestamp_ms, rec.offset, orig
                    )
                })
                .collect::<Vec<_>>()
                .join("  ")
        );
    }

    // -------------------------------------- 5. the scorecard, and the BOUND
    assert_eq!(
        r.out.status.code(),
        Some(0),
        "`logweir restore run` at point_in_time={PIT_RFC3339} must exit 0\nstdout:\n{}\n\
         stderr:\n{}",
        r.out.stdout_utf8(),
        r.out.stderr_utf8()
    );
    let sc = read_scorecard(&r);
    assert_eq!(
        sc["outcome"].as_str(),
        Some("pass"),
        "the signed document must report a pass: {}",
        sc["integrity"]
    );
    assert_eq!(
        sc["sample"]["records_restored"].as_u64(),
        Some(RESTORED_PAYLOADS.len() as u64),
        "the scorecard's restored count is the six records inside the window: {}",
        sc["sample"]
    );
    assert_eq!(
        sc["integrity"]["mismatches"].as_u64(),
        Some(0),
        "per-record reconciliation must find no mismatch: {}",
        sc["integrity"]
    );
    assert_eq!(
        sc["target"]["mode"].as_str(),
        Some("newTopic"),
        "the signed document names the mode it ran in: {}",
        sc["target"]
    );
    assert!(
        logweir_verify(&r).success(),
        "logweir's own verifier must accept the document it signed"
    );
    assert!(
        python_verify(&r).success(),
        "the auditor's independent verifier must accept it too"
    );

    // **SIX IS INSIDE THE MANIFEST'S BOUND, AND IS NOT COMPARED TO IT BY
    // EQUALITY** (spec amendment 1, critique A F6). The nine records land in
    // ONE segment per partition (`segment_max_records: 1000`), and each of
    // those segments STRADDLES `T`: it starts at `T − 1` and ends at `T + 1`.
    // So no segment is wholly inside `[floor, T]`, `lower` is 0, and `upper`
    // is every record the archive holds over this topic — 9. A count equality
    // of 6 here would pass on this fixture and fail on a correct
    // implementation of a larger one, which is exactly what spec §10's G-PITR
    // row forbids.
    let loc = logweir_core::engine::StorageUrl::S3 {
        bucket: ARCHIVE_BUCKET.to_string(),
        prefix: backup_id.clone(),
        region: Some("us-east-1".to_string()),
        endpoint: Some("http://localhost:9000".to_string()),
        path_style: true,
        allow_http: true,
    };
    let store = logweir_engine_oso::storage::Store::read_only_from_url(&loc)
        .expect("a read-only handle over this row's archive");
    let engine = logweir_engine_oso::engine::OsoCliEngine::new(
        engine_bin(),
        engine_version(),
        engine_digest(),
        engine_mount(),
        store,
    );
    let sets = engine.list_backup_sets(&loc).expect("list_backup_sets");
    let set = sets
        .iter()
        .find(|s| s.backup_id == backup_id)
        .unwrap_or_else(|| panic!("{backup_id} is not among {sets:?}"));
    assert_eq!(
        set.manifest_key, manifest_key,
        "the manifest this bound is computed from is the one this row wrote"
    );
    let facts = engine.describe(set).expect("describe");
    let named = BTreeSet::from([SRC_TOPIC]);
    let floor = facts
        .earliest_covered_timestamp_ms(&named)
        .expect("the archive covers the topic this restore names");
    assert_eq!(
        floor,
        T - 1,
        "the restore window's floor is the ARCHIVE's over the named topics (guard G-WIN), \
         which for this fixture is T - 1 ms"
    );
    let (lower, upper) = logweir_core::engine::expected_restored_count(&facts, floor, T);
    eprintln!(
        "[pitr] manifest bound over [{floor}, {T}] = [{lower}, {upper}]; restored {}",
        RESTORED_PAYLOADS.len()
    );
    let restored_count = RESTORED_PAYLOADS.len() as u64;
    assert!(
        lower <= restored_count && restored_count <= upper,
        "the six restored records must be INSIDE the manifest's bound [{lower}, {upper}] over \
         [{floor}, {T}]"
    );
    assert_eq!(
        (lower, upper),
        (0, FIXTURE.len() as u64),
        "one straddling segment per partition: nothing is wholly inside [{floor}, {T}], so \
         lower is 0, and every record the archive holds over {SRC_TOPIC} is in upper"
    );

    // The shared bucket and the two topics both go with their `Drop` guards —
    // `SweptArchive` first, then `RestoredBrokerState` — so this row leaves the
    // stack as it found it on the panicking path too, not only on this one.
}
