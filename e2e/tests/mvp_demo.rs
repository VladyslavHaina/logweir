#![cfg(feature = "e2e")]
//! **`just mvp-demo`, asserted.** Task 12 — Phase A's exit criterion.
//!
//! `scripts/mvp-demo.sh` prints a green summary. This file is what makes that
//! summary mean something: it shells the script, reads its exit code
//! **directly** (never through a pipe — STANDING RULE 20), and then asserts on
//! the ARTEFACTS rather than on the transcript.
//!
//! # Why the row recreates `orders` and `payments`
//!
//! The demo REFUSES a stack whose source topics already hold records, and it
//! is right to: a second `backup` into a colliding `backup_id` does not
//! accumulate, and the manifest would then describe fewer records than the
//! broker holds — a partial archive a restore reads from happily
//! (`scripts/e2e-seed.sh:81-91` measured it). But `just e2e` runs on a SEEDED
//! stack (plan erratum E12(g)), where both topics hold a thousand records
//! each, so this row gives the demo the fresh source topics it requires:
//! `orders` and `payments` are deleted and recreated EMPTY, with the three
//! partitions `topic-setup` gives them, and the demo then produces a thousand
//! records into each — which is the state the seed left, with newer
//! timestamps.
//!
//! Nothing after this binary in the alphabet reads those RECORDS. It reads
//! their EXISTENCE: `offset_side` restores from the `drill-demo` ARCHIVE,
//! `scram.rs` asserts the authenticated principal can see a topic named
//! `orders`, and `smoke.rs` asserts the seeded topic list contains it. The
//! `drill-demo` archive in MinIO — which is what `full_drill`, `guards` and
//! `offset_side` actually reconcile against — is never touched by this row or
//! by the demo, and `the_demo_leaves_the_seeded_archive_and_the_marker_alone`
//! below asserts exactly that rather than claiming it.
//!
//! # Where the context-before-guards ordering is pinned (closeout carry (b))
//!
//! `drill::context` is built BEFORE phase 0's guards run, and that is safe
//! only because **every field it builds costs no network round trip** — the
//! rdkafka client is constructed, not connected; both `Store` handles are
//! built from a URL; the engine identity is read from the environment. The
//! consequence is what a test can see, and it is already pinned, in the
//! DEFAULT suite where it costs nothing:
//!
//! * `crates/logweir/tests/guard_cli.rs::an_unmapped_selected_topic_exits_3_on_the_mapping_check_not_on_a_dead_broker`
//!   — exit 3 **with no broker up**, asserted on the refusal MESSAGE and not
//!   on the code alone, precisely so a deleted mapping guard cannot pass by
//!   dying on a dead broker instead;
//! * `…::a_globbed_topic_in_the_spec_exits_3_at_phase_0_with_its_reason_line`
//!   and `…::a_dollar_brace_in_a_spec_topic_exits_3_at_phase_0_naming_the_expansion`
//!   — the same property for **G-GLOB** and **G-EXP**;
//! * `crates/logweir/src/drill/mod.rs`'s `Ctx` doc comment states the rule the
//!   three tests enforce, and names the archive read that deliberately lives
//!   in `execute_with` AFTER phases 0 and 1 for this reason.
//!
//! So this row asserts nothing about it: a second, stack-bound copy of a
//! property three no-network tests already hold would be slower, weaker and
//! one more thing to keep true. Recorded here because the carry asked for the
//! location in writing.

mod harness;
use harness::*;

use logweir_core::engine::DataEngine;
use logweir_kafka::reader::ClusterReader;
use std::collections::{BTreeSet, HashSet};
use std::path::PathBuf;
use std::process::Command;

/// The `backup_id` and storage prefix `examples/backup.yaml` carries, which is
/// what the demo backs up under. Distinct from `harness::ARCHIVE_PREFIX`
/// (`drill-demo`, the seeded archive) on purpose.
const DEMO_BACKUP_ID: &str = "mvp-demo";

/// Global Constraint 6: everything `logweir backup run` puts lives under this.
const RECEIPT_PREFIX: &str = "logweir/";

/// The two source topics `examples/backup.yaml` names.
const SOURCE_TOPICS: [&str; 2] = ["orders", "payments"];

/// The three payloads the source topics are put back with — one per partition.
const SEED_RESTORE_PAYLOADS: [&str; 3] = [
    "mvp-demo-restored-source-0",
    "mvp-demo-restored-source-1",
    "mvp-demo-restored-source-2",
];

fn demo_out() -> PathBuf {
    root().join(".demo/mvp")
}

/// MinIO's compose credentials, for the in-process `Store` handles below.
///
/// `AmazonS3Builder::from_env()` reads these three, and with none of them set
/// it falls through to the **EC2 instance-metadata** credential provider and
/// spends ten retries against the link-local `169.254.169.254` before the
/// configured endpoint is contacted at all. Measured, on this row's first
/// `just e2e` run: `list_backup_sets: Operational("Generic S3 error: Error
/// performing PUT http://169.254.169.254/latest/api/token in 7.44s, after 10
/// retries")`. `AWS_EC2_METADATA_DISABLED` is what `just e2e` sets for the
/// cargo process; it stops the retries, not the missing credentials.
///
/// `set_var` is process-wide. Each file under `e2e/tests/` is its own test
/// binary, so this reaches nothing outside this file, and all three rows here
/// want exactly these values. (`pitr_boundary.rs` uses the same device for the
/// same reason; the shared harness deliberately does not — see `RunOpts::env`.)
fn minio_env() {
    std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
    std::env::set_var("AWS_REGION", "us-east-1");
    std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
}

/// One archive's `BackupSetFacts`, read through the PRODUCT's own engine
/// rather than by parsing the manifest here. Call `minio_env()` first.
fn archive_facts(prefix: &str) -> logweir_core::engine::BackupSetFacts {
    let loc = logweir_core::engine::StorageUrl::S3 {
        bucket: ARCHIVE_BUCKET.to_string(),
        prefix: prefix.to_string(),
        region: Some("us-east-1".to_string()),
        endpoint: Some("http://localhost:9000".to_string()),
        path_style: true,
        allow_http: true,
    };
    let store = logweir_engine_oso::storage::Store::read_only_from_url(&loc)
        .unwrap_or_else(|e| panic!("a read-only handle over {prefix}: {e}"));
    let engine = logweir_engine_oso::engine::OsoCliEngine::new(
        engine_bin(),
        engine_version(),
        engine_digest(),
        engine_mount(),
        store,
    );
    let sets = engine
        .list_backup_sets(&loc)
        .unwrap_or_else(|e| panic!("list_backup_sets over {prefix}: {e}"));
    let set = sets
        .iter()
        .find(|s| s.backup_id == prefix)
        .unwrap_or_else(|| {
            panic!("no backup set `{prefix}` among {sets:?} — run ./scripts/e2e-seed.sh")
        });
    engine
        .describe(set)
        .unwrap_or_else(|e| panic!("describe {prefix}: {e}"))
}

/// The newest instant an archive covers over the topics named, from its
/// manifest.
fn newest_covered_ms(facts: &logweir_core::engine::BackupSetFacts, topics: &BTreeSet<&str>) -> i64 {
    facts
        .topics
        .iter()
        .filter(|t| topics.contains(t.name.as_str()))
        .flat_map(|t| t.partitions.iter())
        .flat_map(|p| p.segments.iter())
        .map(|s| s.end_timestamp)
        .max()
        .expect("the archive covers the named topics")
}

/// Run `scripts/mvp-demo.sh` with the end-of-run sweep left to this row's
/// `Drop` guard, so the manifest is still there when the bound below is
/// computed. Stdout and stderr are CAPTURED — no pipe carries either, and the
/// status comes off `Output::status`.
fn run_demo(keep_archive: bool) -> std::process::Output {
    let mut c = Command::new("bash");
    c.arg(root().join("scripts/mvp-demo.sh"))
        .current_dir(root())
        .env("LOGWEIR_BIN", bin())
        .env("LOGWEIR_PYTHON", auditor_python())
        .env("AWS_EC2_METADATA_DISABLED", "true");
    if keep_archive {
        c.env("LOGWEIR_MVP_DEMO_KEEP_ARCHIVE", "1");
    }
    c.output().expect("scripts/mvp-demo.sh runs")
}

fn text(out: &[u8]) -> String {
    String::from_utf8_lossy(out).to_string()
}

/// Delete a topic and wait for the broker's metadata to agree. Best effort on
/// the delete — a topic that is not there is the state we want — with the POLL
/// as the assertion.
fn delete_topic_and_wait(topic: &str) {
    let _ = kafka_topics(&[
        "--bootstrap-server",
        "kafka-broker-1:9094",
        "--delete",
        "--topic",
        topic,
    ]);
    for _ in 0..60 {
        if !topic_exists(topic) {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    panic!("{topic}: still present 15 s after --delete");
}

/// Puts `orders` and `payments` back the way a freshly seeded stack has them,
/// on **every** exit path.
///
/// # The invariant this exists to preserve
///
/// `harness::newest_source_record_ts_ms` says it in its own doc comment:
/// *"These are the records `scripts/e2e-seed.sh` backed up, so this is also
/// the newest record the archive holds."* `harness::spec_default` binds every
/// later drill's sample window to that reading —
/// `[newest − 24 h, newest + 1 s]` — and `offset_side.rs` binds
/// `restore.point_in_time` to the same end.
///
/// A row that produces NEW records into the source topics breaks that
/// invariant, and the break is not visible where it happens: the window end
/// moves forward while the `drill-demo` archive's coverage does not, so
/// `measured.rpo_seconds` — *the archive coverage gap at the requested point*
/// — grows by however long ago this row ran, sails past
/// `objectives.rpo_seconds: 300`, and four rows in `offset_side.rs` exit **2**
/// on a tree with nothing wrong with it. Measured exactly that way on this
/// row's first `just e2e` run.
///
/// So the topics go back with their records stamped at the archive's OWN
/// newest covered instant (`harness::produce_with_timestamps`, Task 11), three
/// per topic — one per partition, which is all `newest_source_record_ts_ms`
/// reads. The record COUNT is not part of the invariant: nothing downstream
/// reads it, and `smoke.rs` and `scram.rs` want the topics to exist, not to be
/// full.
struct SeededSourceTopics {
    newest_ms: i64,
}

fn restore_source_topics(newest_ms: i64) {
    for t in SOURCE_TOPICS {
        delete_topic_and_wait(t);
        create_topic(t, 3);
    }
    for t in SOURCE_TOPICS {
        let records: Vec<(i64, &str)> = SEED_RESTORE_PAYLOADS
            .iter()
            .map(|p| (newest_ms, *p))
            .collect();
        produce_with_timestamps(t, &records)
            .unwrap_or_else(|e| panic!("putting {t} back at {newest_ms}: {e}"));
    }
}

impl Drop for SeededSourceTopics {
    fn drop(&mut self) {
        // `catch_unwind`, like `offset_side.rs`'s marker guard: a panic
        // escaping a `Drop` while the thread is already unwinding ABORTS the
        // process and takes the row's real failure message with it.
        let newest = self.newest_ms;
        if std::panic::catch_unwind(move || restore_source_topics(newest)).is_err() {
            eprintln!(
                "[mvp-demo] COULD NOT put `orders` and `payments` back at {newest} ms. Later \
                 rows bind their sample window to the newest SOURCE record and will read a \
                 window the `drill-demo` archive does not cover; `just e2e-down && just \
                 e2e-up` plus a re-seed clears it."
            );
        }
    }
}

/// Puts the SHARED stack back on **every** exit path, including a panicking
/// assertion.
///
/// This is `pitr_boundary`'s `SweptArchive` and `RestoredBrokerState` folded
/// into one guard, because this row owns both kinds of leftover:
///
/// 1. **The broker.** `target.mode: newTopic` tears NOTHING down (Global
///    Constraint 19 — deleting the restored topic would delete the recovery),
///    so the two `restore-<stamp>-…` topics survive the run. Nothing else
///    sweeps them: `delete_all_drill_topics` is `drill-` scoped. They are
///    identified by DIFFERENCE against the topic list taken before the run
///    rather than by recomputing the stamp, so a run that died before printing
///    its summary still cleans up after itself.
/// 2. **The shared archive bucket.** `harness::corrupt_a_non_oldest_segment`
///    takes the last key in sort order, and `mvp-demo` sorts AFTER
///    `drill-demo`, so an archive left here would be quarantined in place of
///    the drill's on a later run and `full_drill`'s corruption row would then
///    watch an intact archive exit 0. (Task 12 also scopes that helper to the
///    prefix it is given, which removes the hazard structurally; this stays
///    because a shared bucket carrying one run's leftovers is its own problem.)
///
/// `Drop` runs on the panicking path too (`#[test]` unwinds; this workspace
/// sets no `panic = "abort"`), and every statement here is an ignored `let _`
/// for the reason Task 9b's guard records: a `Drop` that panics while the
/// thread is already unwinding ABORTS the process and destroys the assertion
/// message this row exists to print.
struct DemoLeftovers {
    topics_before: HashSet<String>,
}

impl DemoLeftovers {
    fn arm() -> Self {
        let r = reader();
        let topics_before = ClusterReader::list_topics(&r)
            .map(|ts| ts.into_iter().map(|t| t.name).collect())
            .unwrap_or_default();
        DemoLeftovers { topics_before }
    }
}

impl Drop for DemoLeftovers {
    fn drop(&mut self) {
        let r = reader();
        if let Ok(now) = ClusterReader::list_topics(&r) {
            for t in now {
                if t.name.starts_with("restore-") && !self.topics_before.contains(&t.name) {
                    let _ = kafka_topics(&[
                        "--bootstrap-server",
                        "kafka-broker-1:9094",
                        "--delete",
                        "--topic",
                        &t.name,
                    ]);
                }
            }
        }
        for prefix in [
            format!("{DEMO_BACKUP_ID}/"),
            format!("{RECEIPT_PREFIX}backups/{DEMO_BACKUP_ID}/"),
        ] {
            let _ = mc(&[
                "rm",
                "--recursive",
                "--force",
                &format!("local/{ARCHIVE_BUCKET}/{prefix}"),
            ]);
        }
    }
}

/// **THE ROW.** One recipe from a source topic to a verified point-in-time
/// restore into a new topic and a signed receipt.
///
/// Mutants this kills (Task 12's brief):
/// * restore into the scratch prefix instead of a new topic → the topic-name
///   assertion fails, `restore-` absent;
/// * assert an EQUALITY on the restored count instead of the manifest bound →
///   not a mutant of the product but of this oracle; the bound below is why
///   this row stays green the moment a thousand records straddle a segment
///   boundary, which is the failure spec amendment 1 exists to remove.
#[test]
fn mvp_demo_backs_up_restores_at_a_point_in_time_and_verifies() {
    minio_env();
    let _leftovers = DemoLeftovers::arm();

    // Read the SEEDED archive's own newest covered instant BEFORE the source
    // topics are touched, and arm the guard that puts them back at it. See
    // `SeededSourceTopics` for what happens to four rows in `offset_side.rs`
    // when this is skipped.
    let named: BTreeSet<&str> = SOURCE_TOPICS.into_iter().collect();
    let seeded_newest = newest_covered_ms(&archive_facts(ARCHIVE_PREFIX), &named);
    let _source = SeededSourceTopics {
        newest_ms: seeded_newest,
    };

    // The demo wants what `just e2e-down && just e2e-up` gives it: empty
    // source topics. See this file's header for why that is safe here.
    for t in SOURCE_TOPICS {
        delete_topic_and_wait(t);
        create_topic(t, 3);
    }

    let out = run_demo(true);
    let stdout = text(&out.stdout);
    let stderr = text(&out.stderr);
    assert_eq!(
        out.status.code(),
        Some(0),
        "scripts/mvp-demo.sh must exit 0\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
    );

    // ---------------------------------------------------------------- receipt
    let receipt: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(demo_out().join("receipt.json")).unwrap())
            .expect("the receipt is JSON");
    assert!(
        demo_out().join("receipt.sig").exists(),
        "--receipt-out writes the .json/.sig pair (interface I6)"
    );
    for t in SOURCE_TOPICS {
        let on_broker = record_count_on_broker(t).expect("end offsets") as u64;
        assert_eq!(
            receipt["records"][t].as_u64(),
            Some(on_broker),
            "the receipt's record count for `{t}` must be the number the BROKER holds, \
             not the number the spec asked for: {}",
            receipt["records"]
        );
        assert!(on_broker > 0, "`{t}` holds no records after the demo");
    }

    // -------------------------------------------------------------- scorecard
    let sc: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(demo_out().join("scorecard.json")).unwrap())
            .expect("the scorecard is JSON");
    assert_eq!(
        sc["outcome"].as_str(),
        Some("pass"),
        "the demo's restore must score pass: {sc}"
    );
    assert_eq!(
        sc["integrity"]["level"].as_str(),
        Some("byte-fingerprint"),
        "per-record reconciliation, not a count comparison: {}",
        sc["integrity"]
    );
    assert_eq!(
        sc["integrity"]["mismatches"].as_u64(),
        Some(0),
        "{}",
        sc["integrity"]
    );
    assert_eq!(
        sc["target"]["mode"].as_str(),
        Some("newTopic"),
        "the signed document names the mode it ran in: {}",
        sc["target"]
    );

    // -------------------------------------------------------- restored topics
    //
    // The expected names come from the PRODUCT's own rule, applied to the
    // plan the demo actually ran: the rendered spec is parsed by the very
    // `DrillSpec` the binary parses and handed to the very
    // `target_topic_prefix` phase 0 maps through. A test that re-implements
    // the rule it is checking asserts a property of itself (`offset_side.rs`
    // fix round 1, F5a).
    let plan_text = std::fs::read_to_string(demo_out().join("restore.yaml")).unwrap();
    let plan: logweir_core::spec::RestoreSpec =
        serde_yaml::from_str(&plan_text).expect("the rendered plan is the one the binary parses");
    let prefix = logweir_core::spec::target_topic_prefix(&plan);
    assert!(
        prefix.starts_with("restore-"),
        "the default naming rule stamps the recovery point into a `restore-…` prefix; \
         got `{prefix}`"
    );
    let pit = plan
        .restore
        .point_in_time
        .expect("the demo binds restore.point_in_time");
    let stamp = pit.format("%Y%m%dT%H%M%SZ").to_string();
    assert!(
        prefix.contains(&stamp),
        "the new topics must carry the point_in_time stamp `{stamp}`; prefix is `{prefix}`"
    );

    let mut restored_total = 0u64;
    for t in SOURCE_TOPICS {
        let name = format!("{prefix}{t}");
        assert!(
            topic_exists(&name),
            "`{name}` is not on the broker; a `newTopic` restore creates topics that did \
             not exist and tears nothing down"
        );
        let n = record_count_on_broker(&name).expect("end offsets") as u64;
        assert!(n > 0, "`{name}` holds no records");
        restored_total += n;
    }

    // -------------------------------------- the restored count is inside I5's bound
    //
    // A BOUND, never an equality (spec amendment 1, critique A F6). A thousand
    // records per topic across three partitions lands inside one segment per
    // partition here, and `point_in_time` is past every one of them, so the
    // bound is tight today — but the moment a segment straddles the recovery
    // point (the NORMAL case for point-in-time recovery) `lower` drops and an
    // equality assertion would fail on a correct tree.
    let loc = logweir_core::engine::StorageUrl::S3 {
        bucket: ARCHIVE_BUCKET.to_string(),
        prefix: DEMO_BACKUP_ID.to_string(),
        region: Some("us-east-1".to_string()),
        endpoint: Some("http://localhost:9000".to_string()),
        path_style: true,
        allow_http: true,
    };
    let store = logweir_engine_oso::storage::Store::read_only_from_url(&loc)
        .expect("a read-only handle over the demo's archive");
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
        .find(|s| s.backup_id == DEMO_BACKUP_ID)
        .unwrap_or_else(|| panic!("{DEMO_BACKUP_ID} is not among {sets:?}"));
    let facts = engine.describe(set).expect("describe");
    let named: BTreeSet<&str> = SOURCE_TOPICS.into_iter().collect();
    let floor = facts
        .earliest_covered_timestamp_ms(&named)
        .expect("the archive covers the topics this restore names");
    let pit_ms = pit.timestamp_millis();
    let (lower, upper) = logweir_core::engine::expected_restored_count(&facts, floor, pit_ms);
    eprintln!(
        "[mvp-demo] manifest bound over [{floor}, {pit_ms}] = [{lower}, {upper}]; \
         restored {restored_total}"
    );
    assert!(
        lower <= restored_total && restored_total <= upper,
        "the restored count {restored_total} must be INSIDE the manifest's bound \
         [{lower}, {upper}] over [{floor}, {pit_ms}] — this is a bound, not an equality"
    );

    // ---------------------------------------------------------- the summary line
    let summary = stdout
        .lines()
        .find(|l| l.starts_with("mvp-demo: "))
        .unwrap_or_else(|| panic!("no `mvp-demo: …` summary line:\n{stdout}"));
    for needle in [
        "pass",
        &format!("{prefix}orders"),
        &format!("{prefix}payments"),
        "receipt-key=",
        "scorecard-key=",
    ] {
        assert!(
            summary.contains(needle),
            "the summary line must carry `{needle}`: {summary}"
        );
    }
    eprintln!("[mvp-demo] {summary}");
}

/// **The demo touches neither the seeded archive nor the marker topic.**
///
/// The controller's ruling in one assertion each. `drill-demo` is what
/// `full_drill`, `guards` and `offset_side` reconcile against, and
/// `logweir.scratch` is the segregation proof a `mode: scratch` drill refuses
/// without — a demo that swept either would turn one green run into four red
/// ones on the next.
#[test]
fn the_demo_leaves_the_seeded_archive_and_the_marker_alone() {
    assert!(
        topic_exists(MARKER_TOPIC),
        "`{MARKER_TOPIC}` is gone; `target.mode: newTopic` needs no marker topic and the \
         demo must not touch the one a `scratch` drill does need"
    );
    let listing = mc(&[
        "--json",
        "ls",
        "--recursive",
        &format!("local/{ARCHIVE_BUCKET}/{ARCHIVE_PREFIX}"),
    ]);
    assert!(
        listing.status.success(),
        "the seeded `{ARCHIVE_PREFIX}` archive is not listable: {}",
        listing.stderr_utf8()
    );
    assert!(
        listing.stdout_utf8().contains("manifest.json"),
        "the seeded `{ARCHIVE_PREFIX}` archive has no manifest after the demo ran:\n{}",
        listing.stdout_utf8()
    );
}

/// **A DIRTY STACK IS REFUSED, exit 1, with the remedy.**
///
/// This is also the second half of Task 12's acceptance — *running
/// `just mvp-demo` a second time on the same stack exits 1 at step 1 rather
/// than producing a partial archive* — because by the time this row runs the
/// source topics hold the records the row above produced. It does not DEPEND
/// on that (libtest runs a binary's tests in name order and
/// `…_backs_up_…` sorts before `…_refuses_…`, but a row that is only correct
/// under an ordering is a row that breaks when someone renames it): if the
/// topics are empty it fills one first.
///
/// Mutant: delete step 1's dirty-stack refusal → this fails at assertion time,
/// exit 0 against an expected 1.
#[test]
fn mvp_demo_refuses_a_dirty_stack() {
    minio_env();
    let _leftovers = DemoLeftovers::arm();

    // If the topics are empty, the records that dirty them are stamped at the
    // SEEDED archive's newest covered instant and never at the wall clock —
    // see `SeededSourceTopics` for what a forward-moving source timestamp does
    // to every later row's sample window.
    if record_count_on_broker("orders").expect("end offsets") == 0 {
        let named: BTreeSet<&str> = SOURCE_TOPICS.into_iter().collect();
        let seeded_newest = newest_covered_ms(&archive_facts(ARCHIVE_PREFIX), &named);
        let records: Vec<(i64, &str)> = SEED_RESTORE_PAYLOADS
            .iter()
            .map(|p| (seeded_newest, *p))
            .collect();
        produce_with_timestamps("orders", &records).expect("records into orders");
    }

    let out = run_demo(false);
    let stdout = text(&out.stdout);
    let stderr = text(&out.stderr);
    assert_eq!(
        out.status.code(),
        Some(1),
        "a dirty stack is an operational refusal (exit 1), before anything is produced \
         and before any archive exists\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
    );
    assert!(
        stderr.contains("just e2e-down"),
        "the refusal must carry the remedy `just e2e-down && just e2e-up`: {stderr}"
    );
    assert!(
        stderr.contains("already holds"),
        "the refusal must name the topic and the count it found: {stderr}"
    );
}
