//! **Interface I33 / I20 / I8 — restore into a NEW topic at a point in time.**
//!
//! Tag 1's flagship is not a drill into a throwaway cluster; it is a restore of
//! a point in time into brand-new topics on a REAL cluster. This file holds the
//! four differences between that and `mode: scratch`, the naming rule, the
//! offset side of the rendered document, and the runner's two names.
//!
//! **Every row here runs IN PROCESS over doubles**, with two exceptions that
//! both shell a binary and neither of which needs a broker: the two `--help`
//! rows, and `the_runner_prints_its_three_evidence_keys_last`, which re-execs
//! THIS test binary's `#[ignore]`d child row for the reason interface I7's
//! twin in `backup_run.rs` does — the claim is about a process's stdout, and
//! `println!` cannot be captured in process without an fd redirect, which
//! would mean a new dependency (Global Constraint 38 forbids one).
//!
//! The `localhost:9092` literals below are why this file is in
//! `no_network_in_unit_tests.rs`'s `ALLOWED` (chain N, STANDING RULE 18): they
//! are DATA in a `DrillSpec`, handed to `ClusterReader`/`TopicCreator`/
//! `TopicDeleter` doubles, and no client is ever constructed from them.
mod fixtures;

use logweir::drill::{self, phase0_admit, DrillError};
use logweir::exit::ExitCode;
use logweir_core::spec::{
    default_topic_prefix, target_topic_prefix, AllowedClusters, Anchor, DrillSpec, Notifications,
    ObjectivesSpec, RestoreSpec, RestoreSpecBlock, SampleSpec, SourceSpec, TargetMode, TargetSpec,
    TopicNaming,
};
use logweir_kafka::reader::{
    ClusterReader, ConsumedRecord, KafkaError, NewTopicSpec, TopicCreator, TopicDeleter, TopicMeta,
};
use std::collections::BTreeMap;
use std::process::Command;
use std::sync::Mutex;

/// The target cluster this file's doubles report. **Deliberately absent from
/// `allowed()`** and simultaneously equal to its `source_cluster_id`, so a
/// `scratch` run trips checks 1 and 2 and a `newTopic` run trips neither.
const CLUSTER: &str = "PROD0000000000000000AA";
const MARKER: &str = "logweir.scratch";
const SCRATCH_PREFIX: &str = "drill-";
/// `2026-09-07T14:05:00Z`, the recovery point every naming row uses.
///
/// Its epoch-millisecond value is **1_788_789_900_000**, computed with
/// `python3 -c 'import datetime as d; print(int(d.datetime(2026,9,7,14,5,0,
/// tzinfo=d.timezone.utc).timestamp()*1000))'` — plan erratum E6 is the brief
/// that quoted `1_788_098_700_000` for this instant, which is
/// `2026-08-30T14:05:00Z`, eight days earlier.
const POINT_IN_TIME: &str = "2026-09-07T14:05:00Z";
const POINT_IN_TIME_MS: i64 = 1_788_789_900_000;

fn ts(s: &str) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339(s)
        .expect("an RFC 3339 instant")
        .with_timezone(&chrono::Utc)
}

/// A spec in the given mode, selecting `orders` and `payments`, whose
/// `bootstrap_servers` is the literal `localhost:9092` (data, never dialled)
/// and whose recovery point is `POINT_IN_TIME`.
fn spec_in(mode: TargetMode, topic_naming: Option<TopicNaming>) -> DrillSpec {
    DrillSpec {
        name: None,
        source: SourceSpec {
            storage: logweir_core::engine::StorageUrl::Filesystem {
                path: "/tmp/src".into(),
            },
            backup: "latestCompleted".into(),
            topics: vec!["orders".into(), "payments".into()],
        },
        target: TargetSpec {
            bootstrap_servers: vec!["localhost:9092".into()],
            auth: logweir_core::spec::AuthSpec::Plaintext,
            mode,
            topic_naming,
            marker_topic: MARKER.into(),
            topic_mapping_prefix: SCRATCH_PREFIX.into(),
            default_replication_factor: 1,
            teardown: "delete".into(),
        },
        sample: SampleSpec {
            window_start: ts("2026-09-07T12:00:00Z"),
            window_end: ts("2026-09-07T15:00:00Z"),
            records_per_partition: 25,
            anchor: Anchor::Head,
            max_partitions: None,
        },
        restore: RestoreSpecBlock {
            point_in_time: Some(ts(POINT_IN_TIME)),
        },
        objectives: ObjectivesSpec {
            rto_seconds: None,
            rpo_seconds: None,
            pass_rate: None,
        },
        evidence: logweir_core::engine::StorageUrl::Filesystem {
            path: "/tmp/evidence".into(),
        },
        engine_overrides: Default::default(),
        notifications: Notifications::default(),
    }
}

/// An allowlist that does NOT contain `CLUSTER` and whose source cluster IS
/// `CLUSTER`. Both of `scratch`'s cluster-identity checks fail against it,
/// which is what makes "`newTopic` skips them" a claim with teeth rather than a
/// claim over a permissive fixture.
fn allowed() -> AllowedClusters {
    AllowedClusters {
        allowed_cluster_ids: vec!["SOME00OTHER00CLUSTER00".into()],
        source_cluster_id: Some(CLUSTER.into()),
    }
}

/// A target with **no marker topic**, benign broker defaults, and whatever
/// topics it was told it already has.
///
/// `broker_configs` reports `CreateTime` and an unbounded retention with NO
/// timestamp-bound key, so guard G-TS's two refusing arms and its
/// `LogAppendTime` probe are all silent — this file is about the MODE branch,
/// and a fixture that tripped G-TS would prove nothing about it.
struct NoMarkerBroker {
    already_there: Vec<TopicMeta>,
}

impl NoMarkerBroker {
    fn new() -> Self {
        Self {
            already_there: Vec::new(),
        }
    }
    fn already_has(mut self, topic: &str, partitions: i32) -> Self {
        self.already_there.push(TopicMeta::new(topic, partitions));
        self
    }
}

impl ClusterReader for NoMarkerBroker {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        Ok(CLUSTER.to_string())
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        // NOT the marker. That absence is the whole fixture.
        Ok(self.already_there.clone())
    }
    fn end_offsets(&self, _topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        Ok(vec![])
    }
    fn topic_configs(&self, topic: &str) -> Result<BTreeMap<String, String>, KafkaError> {
        panic!(
            "phase 0 read topic_configs({topic:?}); the mapped target topics do not exist at \
             phase 0 by construction, and this fixture arms no LogAppendTime probe"
        )
    }
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(BTreeMap::from([
            (
                "log.message.timestamp.type".to_string(),
                "CreateTime".to_string(),
            ),
            ("log.retention.ms".to_string(), "-1".to_string()),
        ]))
    }
    fn consume_range(
        &self,
        _t: &str,
        _p: i32,
        _from: i64,
        _max: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        Ok(vec![])
    }
}

#[derive(Default)]
struct RecordingCreator {
    calls: Mutex<Vec<NewTopicSpec>>,
}

impl TopicCreator for RecordingCreator {
    fn create_topics(
        &self,
        topics: &[NewTopicSpec],
    ) -> Result<Vec<(String, Result<(), String>)>, KafkaError> {
        self.calls.lock().unwrap().extend_from_slice(topics);
        Ok(topics.iter().map(|t| (t.name.clone(), Ok(()))).collect())
    }
}

#[derive(Default)]
struct RecordingDeleter {
    calls: Mutex<Vec<String>>,
}

impl TopicDeleter for RecordingDeleter {
    fn delete_topics(
        &self,
        names: &[String],
    ) -> Result<Vec<(String, Result<(), String>)>, KafkaError> {
        self.calls.lock().unwrap().extend_from_slice(names);
        Ok(names.iter().map(|n| (n.clone(), Ok(()))).collect())
    }
}

/// Phase 0 over this file's doubles. Returns the whole `Result` so a row can
/// assert the EXIT CODE first and the message second.
fn phase0(
    spec: &DrillSpec,
    reader: &dyn ClusterReader,
) -> Result<phase0_admit::Admitted, DrillError> {
    let creator = RecordingCreator::default();
    let deleter = RecordingDeleter::default();
    phase0_admit::run(
        spec,
        &serde_yaml::to_string(spec).expect("the spec serialises"),
        &allowed(),
        reader,
        &creator,
        &deleter,
    )
}

fn exit_of<T>(r: &Result<T, DrillError>) -> ExitCode {
    match r {
        Ok(_) => ExitCode::Ok,
        Err(e) => e.exit_code(),
    }
}

fn message_of<T: std::fmt::Debug>(r: Result<T, DrillError>) -> String {
    match r {
        Ok(v) => panic!("expected a refusal, got Ok({v:?})"),
        Err(e) => e.to_string(),
    }
}

// ===========================================================================
// THE MODE BRANCH (interface I33)
// ===========================================================================

/// **The `newTopic` arm.** A cluster with no marker topic, absent from the
/// allowlist, and equal to the source cluster id — all three of `scratch`'s
/// segregation checks would refuse it, and `newTopic` runs anyway.
///
/// The mutant this exists for: run the marker check in `newTopic` mode too (or
/// delete the `match` and keep one code path). Either fails HERE, at assertion
/// time, on the exit code.
#[test]
fn a_new_topic_restore_skips_the_marker_and_allowlist_checks() {
    let spec = spec_in(TargetMode::NewTopic, None);
    let admitted = phase0(&spec, &NoMarkerBroker::new());
    assert_eq!(
        exit_of(&admitted),
        ExitCode::Ok,
        "a newTopic restore must be admitted with no marker topic, off the allowlist, and on \
         the SOURCE cluster — each of those three is the scratch segregation proof and none of \
         them says anything about a restore into a topic that does not exist yet. Got: {:?}",
        admitted.as_ref().err().map(|e| e.to_string())
    );
    let admitted = admitted.expect("admitted");
    assert_eq!(admitted.target_cluster_id, CLUSTER);
    // …and the mapping really is the newTopic one, so this row cannot pass
    // over a spec that quietly behaved like `scratch`.
    assert_eq!(
        admitted.topic_mapping.get("orders").map(String::as_str),
        Some("restore-20260907T140500Z-orders")
    );
}

/// **The `scratch` arm, unchanged.** The same fixture, the same doubles, one
/// field different — and all three checks are back.
///
/// Three separate assertions, one per check, because "skips those three
/// checks" is three claims and a single `is_err()` would survive a mutant that
/// dropped two of them.
#[test]
fn a_scratch_restore_still_requires_them() {
    let spec = spec_in(TargetMode::Scratch, None);
    // 1 — the cluster allowlist refuses FIRST, because it runs first.
    let r = phase0(&spec, &NoMarkerBroker::new());
    assert_eq!(exit_of(&r), ExitCode::GuardRefused);
    let m = message_of(r);
    assert!(
        m.contains(&format!(
            "target cluster id {CLUSTER} is not in allowedClusterIds"
        )),
        "got: {m}"
    );

    // 2 — with the cluster allowed, the source-equality check refuses.
    let creator = RecordingCreator::default();
    let deleter = RecordingDeleter::default();
    let r = phase0_admit::run(
        &spec,
        &serde_yaml::to_string(&spec).unwrap(),
        &AllowedClusters {
            allowed_cluster_ids: vec![CLUSTER.into()],
            source_cluster_id: Some(CLUSTER.into()),
        },
        &NoMarkerBroker::new(),
        &creator,
        &deleter,
    );
    assert_eq!(exit_of(&r), ExitCode::GuardRefused);
    let m = message_of(r);
    assert!(
        m.contains(&format!(
            "target cluster id {CLUSTER} equals the source cluster id"
        )),
        "got: {m}"
    );

    // 3 — with both cluster checks satisfied, the MARKER refuses, on the exact
    // `phase0_admit.rs` message.
    let r = phase0_admit::run(
        &spec,
        &serde_yaml::to_string(&spec).unwrap(),
        &AllowedClusters {
            allowed_cluster_ids: vec![CLUSTER.into()],
            source_cluster_id: None,
        },
        &NoMarkerBroker::new(),
        &creator,
        &deleter,
    );
    assert_eq!(exit_of(&r), ExitCode::GuardRefused);
    let m = message_of(r);
    assert!(
        m.contains(&format!(
            "marker topic `{MARKER}` does not exist on cluster {CLUSTER}"
        )),
        "got: {m}"
    );
    assert!(
        m.contains("its existence is the v0.1 segregation proof"),
        "got: {m}"
    );
    // Nothing was created or deleted on any of the three refusals: exit 3 is
    // "refused before anything ran".
    assert!(creator.calls.lock().unwrap().is_empty());
    assert!(deleter.calls.lock().unwrap().is_empty());
}

/// **Spec §6.1's existence rule, in BOTH modes**, on the exact sentence, with
/// the cluster id and every offending topic named.
///
/// The mutant this exists for: delete the refusal (or restore Task 8's
/// pre-fix "reuse the topic" behaviour). Either returns `ExitCode::Ok` where
/// this asserts `GuardRefused`.
#[test]
fn a_restore_refuses_a_target_topic_that_already_exists() {
    // `newTopic`: the mapped name is the default point-in-time one.
    let spec = spec_in(TargetMode::NewTopic, None);
    let broker = NoMarkerBroker::new().already_has("restore-20260907T140500Z-payments", 3);
    let r = phase0(&spec, &broker);
    assert_eq!(
        exit_of(&r),
        ExitCode::GuardRefused,
        "appending into a half-populated topic is refused BEFORE anything runs"
    );
    let m = message_of(r);
    assert!(
        m.contains(
            "mapped target topic `restore-20260907T140500Z-payments` already exists on cluster \
             PROD0000000000000000AA"
        ),
        "the refusal names the topic AND the cluster: {m}"
    );
    assert!(
        m.contains(
            "appending into a half-populated topic produces a restore that reconciles against \
             records it did not write"
        ),
        "the refusal carries spec §6.1's own sentence: {m}"
    );
    assert!(m.contains("mode newTopic"), "got: {m}");

    // `scratch`: the same one refusal path, the same sentence, the scratch
    // prefix. ONE refusal, not two — a second code path is a second place for
    // the two to drift.
    let spec = spec_in(TargetMode::Scratch, None);
    let broker = NoMarkerBroker::new()
        .already_has(MARKER, 1)
        .already_has("drill-orders", 3)
        .already_has("drill-payments", 3);
    let r = phase0_admit::run(
        &spec,
        &serde_yaml::to_string(&spec).unwrap(),
        &AllowedClusters {
            allowed_cluster_ids: vec![CLUSTER.into()],
            source_cluster_id: None,
        },
        &broker,
        &RecordingCreator::default(),
        &RecordingDeleter::default(),
    );
    assert_eq!(exit_of(&r), ExitCode::GuardRefused);
    let m = message_of(r);
    assert!(
        m.contains(
            "mapped target topic `drill-orders` already exists on cluster PROD0000000000000000AA"
        ),
        "got: {m}"
    );
    // EVERY offending topic is named, not only the first.
    assert!(
        m.contains("`drill-orders`, `drill-payments`"),
        "the refusal names every mapped target topic that already exists: {m}"
    );
    assert!(m.contains("mode scratch"), "got: {m}");
}

// ===========================================================================
// THE NAMING (interface I33)
// ===========================================================================

/// `default_topic_prefix` and the name it produces for `orders`.
///
/// Both halves are asserted: the prefix ALONE (so a mutant that changed the
/// format string fails on the string itself) and the mapped name phase 0
/// actually derives from it.
#[test]
fn the_default_new_topic_prefix_carries_the_point_in_time() {
    // The instant, and its epoch-millisecond value, computed with python3
    // before it was written here (plan erratum E6's rule):
    //   python3 -c 'import datetime as d; print(int(d.datetime(2026,9,7,14,5,0,
    //     tzinfo=d.timezone.utc).timestamp()*1000))'  ->  1788789900000
    let t = ts(POINT_IN_TIME);
    assert_eq!(t.timestamp_millis(), POINT_IN_TIME_MS);
    assert_eq!(default_topic_prefix(t), "restore-20260907T140500Z-");

    // The mapped name for `orders`, off the spec, through the one function
    // phase 0 uses.
    let spec = spec_in(TargetMode::NewTopic, None);
    assert_eq!(target_topic_prefix(&spec), "restore-20260907T140500Z-");
    let admitted = phase0(&spec, &NoMarkerBroker::new()).expect("admitted");
    assert_eq!(
        admitted.topic_mapping,
        BTreeMap::from([
            (
                "orders".to_string(),
                "restore-20260907T140500Z-orders".to_string()
            ),
            (
                "payments".to_string(),
                "restore-20260907T140500Z-payments".to_string()
            ),
        ])
    );
    // The SIGNED scorecard's `target.topic_mapping_prefix` is the prefix this
    // run mapped through — not `spec.target.topic_mapping_prefix`, which stays
    // `drill-` in this document and is unread in this mode.
    assert_eq!(admitted.topic_mapping_prefix, "restore-20260907T140500Z-");
    assert_eq!(spec.target.topic_mapping_prefix, SCRATCH_PREFIX);

    // An EXPLICIT `topic_naming.prefix` wins over the default.
    let spec = spec_in(
        TargetMode::NewTopic,
        Some(TopicNaming {
            prefix: "incident-4471-".into(),
        }),
    );
    assert_eq!(target_topic_prefix(&spec), "incident-4471-");
    let admitted = phase0(&spec, &NoMarkerBroker::new()).expect("admitted");
    assert_eq!(
        admitted.topic_mapping.get("orders").map(String::as_str),
        Some("incident-4471-orders")
    );

    // And `scratch` is untouched by any of it.
    let spec = spec_in(TargetMode::Scratch, None);
    assert_eq!(target_topic_prefix(&spec), SCRATCH_PREFIX);
}

// ===========================================================================
// PHASE 9 (Global Constraint 19)
// ===========================================================================

/// **Phase 9 tears nothing down outside `mode: scratch`**, and DOES tear down
/// inside it — the second half is the control, without which a mutant that
/// disabled teardown everywhere would pass.
///
/// The mutant this exists for: run phase 9's deletion in `newTopic` mode too.
/// The recording `TopicDeleter` is then non-empty and this fails at assertion
/// time.
#[test]
fn a_new_topic_restore_tears_nothing_down() {
    let mapping = BTreeMap::from([(
        "orders".to_string(),
        "restore-20260907T140500Z-orders".to_string(),
    )]);

    let deleter = RecordingDeleter::default();
    let att = drill::phase9_teardown::run(
        &deleter,
        &mapping,
        // `delete` — the policy an adopter most likely left in the spec. The
        // MODE is what refuses, at any value of it.
        "delete",
        TargetMode::NewTopic,
        "RUN-NEW",
        "sha256:0",
    );
    assert!(
        deleter.calls.lock().unwrap().is_empty(),
        "a newTopic restore must not delete the topics it just created — they ARE the recovery"
    );
    assert!(att.topics_deleted.is_empty());
    assert!(att.topics_failed.is_empty());
    assert_eq!(
        att.target_mode,
        TargetMode::NewTopic,
        "the attestation says WHICH restore this was, so `deleted: [], failed: []` can be told \
         apart from a scratch teardown that silently did nothing"
    );
    assert_eq!(att.teardown_policy, "delete");

    // THE CONTROL: the same call in `scratch` mode deletes.
    let deleter = RecordingDeleter::default();
    let att = drill::phase9_teardown::run(
        &deleter,
        &mapping,
        "delete",
        TargetMode::Scratch,
        "RUN-SCRATCH",
        "sha256:0",
    );
    assert_eq!(
        *deleter.calls.lock().unwrap(),
        vec!["restore-20260907T140500Z-orders".to_string()],
        "a scratch drill still deletes exactly the mapped topics, by exact name"
    );
    assert_eq!(
        att.topics_deleted,
        vec!["restore-20260907T140500Z-orders".to_string()]
    );
    assert_eq!(att.target_mode, TargetMode::Scratch);

    // And `teardown: keep` still means keep, in `scratch` mode, so the mode
    // check did not swallow the policy check.
    let deleter = RecordingDeleter::default();
    let att = drill::phase9_teardown::run(
        &deleter,
        &mapping,
        "keep",
        TargetMode::Scratch,
        "RUN-KEEP",
        "sha256:0",
    );
    assert!(deleter.calls.lock().unwrap().is_empty());
    assert!(att.topics_deleted.is_empty());
}

// ===========================================================================
// THE OFFSET SIDE (Global Constraint 20, spec §6.1 N6/N9)
// ===========================================================================

/// **The four offset-side keys are in the GOLDEN and in the RENDERER**, each as
/// its own assertion.
///
/// Both layers, because either alone can be balanced by a two-file edit: the
/// golden is the artefact a reviewer reads, and the fresh render at the end is
/// what a coordinated "drop the line from the renderer AND from the golden"
/// cannot get past.
///
/// The mutant this exists for: drop `reset_consumer_offsets: false` and rely on
/// the engine's own default. That default is `false` today
/// [U:crates/kafka-backup-core/src/config.rs:1023], so nothing about the run
/// changes — which is exactly why the invariant has to be an assertion in this
/// document rather than a belief about upstream.
#[test]
fn the_rendered_restore_pins_the_four_offset_side_keys() {
    let golden = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../logweir-engine-oso/tests/snapshots/render__restore_yaml.snap"),
    )
    .expect("the restore.yaml golden");

    assert!(
        golden.contains("\n  consumer_group_strategy: skip\n"),
        "Global Constraint 20: the strategy is rendered, not inherited:\n{golden}"
    );
    assert!(
        golden.contains("\n  reset_consumer_offsets: false\n"),
        "Global Constraint 20: tag 1 commits no consumer-group offset anywhere, and this line \
         is what says so in the approved bytes:\n{golden}"
    );
    assert!(
        golden.contains("\n  auto_consumer_groups: false\n"),
        "`auto_consumer_groups: true` additionally enables reset_consumer_offsets behind the \
         operator's back [U:config.rs:897]:\n{golden}"
    );
    assert!(
        golden.contains("\n  offset_report: \"/var/lib/logweir/01J9X/offsets.json\"\n"),
        "the report's path is rendered from the plan, so plan_hash covers it:\n{golden}"
    );
    // All four are under `restore:` at the same indent as the keys around
    // them. Indentation is part of the contract — plan erratum E3 is the SASL
    // block that landed one indent too shallow and ran UNAUTHENTICATED behind
    // four "Ignoring unknown config key" lines.
    for key in [
        "consumer_group_strategy",
        "reset_consumer_offsets",
        "auto_consumer_groups",
        "offset_report",
    ] {
        let line = golden
            .lines()
            .find(|l| l.trim_start().starts_with(&format!("{key}:")))
            .unwrap_or_else(|| panic!("{key} is not in the golden at all"));
        assert!(
            line.starts_with("  ") && !line.starts_with("   "),
            "{key} must sit at two spaces, under `restore:`, got {line:?}"
        );
    }
    // And GC4's three keys are still absent, at any value. Matched as a KEY —
    // `<name>:` at the start of a trimmed line — because `dry_run` is a
    // substring of `dry_run_check_segments`, which this document renders on
    // purpose and which is not the forbidden key.
    for forbidden in ["purge_topics", "dry_run", "header_preflight_external"] {
        let offender = golden
            .lines()
            .find(|l| l.trim_start().starts_with(&format!("{forbidden}:")));
        assert!(
            offender.is_none(),
            "Global Constraint 4: {forbidden} is never emitted, at any value; found \
             {offender:?} in:\n{golden}"
        );
    }

    // AND THE RENDERER ITSELF, freshly, in this process. The golden read above
    // is the artefact a reviewer looks at; this half is what makes the mutant
    // "delete a line from `render_restore::render` AND from the golden in one
    // edit" fail here rather than balance. Without it that pair of edits leaves
    // insta green (the golden matches the render again) and this test green
    // (the golden it reads no longer has the line) — the coordinated deletion
    // the corpus arithmetic exists to catch, in the render layer.
    let doc = logweir_engine_oso::render_restore::render(&fixtures::plan())
        .expect("the fixture plan holds no glob metacharacter");
    for want in [
        "  consumer_group_strategy: skip\n",
        "  reset_consumer_offsets: false\n",
        "  auto_consumer_groups: false\n",
        "  offset_report: ",
    ] {
        assert!(
            doc.contains(want),
            "the RENDERER must emit {want:?} — an inherited default is not an assertion \
             (Global Constraint 20):\n{doc}"
        );
    }
}

/// **Phase 8 uploads the engine's offset report** and binds it into the signed
/// document.
///
/// Three objects under `logweir/drills/`, not two, and the digest in the
/// SIGNED bytes equals `sha256_prefixed` of what was uploaded.
///
/// The mutant this exists for: skip the upload. The store then holds two
/// objects and this fails at assertion time.
#[test]
fn phase8_uploads_the_offset_report() {
    let dir = tempfile::tempdir().expect("a scratch directory");
    let report = dir.path().join("offsets.json");
    let bytes = br#"{"orders":{"0":{"source":41,"target":0}}}"#;
    std::fs::write(&report, bytes).expect("write the engine's report");

    let store = fixtures::recording_store();
    let sc = fixtures::scorecard_pass();
    let run_id = sc.run_id.clone();
    let signed = drill::phase8_score::run(
        &sc,
        &fixtures::good_signing_key().path,
        &store,
        Some(report.as_path()),
    )
    .expect("phase 8 signs and uploads");

    assert_eq!(
        store.puts(),
        vec![
            format!("logweir/drills/{run_id}.json"),
            format!("logweir/drills/{run_id}.offsets.json"),
            format!("logweir/drills/{run_id}.sig"),
        ],
        "three objects: the scorecard, its sidecar and the offset report"
    );

    // The KEY the run put at, carried out rather than reconstructed.
    assert_eq!(
        signed.offset_report_key.as_deref(),
        Some(format!("logweir/drills/{run_id}.offsets.json").as_str())
    );
    assert_eq!(signed.key, format!("logweir/drills/{run_id}.json"));
    assert_eq!(signed.sidecar_key, format!("logweir/drills/{run_id}.sig"));

    // The DIGEST is over the bytes that were uploaded, and it is in the SIGNED
    // document — read back out of `signed.bytes`, never off the in-memory
    // scorecard, because the signature covers the bytes.
    let (stored, _v) = store
        .get(&format!("logweir/drills/{run_id}.offsets.json"))
        .expect("the offset report is in the bucket");
    assert_eq!(stored, bytes.to_vec(), "the exact bytes, never a re-emit");
    let doc: serde_json::Value = serde_json::from_slice(&signed.bytes).expect("signed JSON");
    assert_eq!(
        doc["evidence"]["offset_report_sha256"].as_str(),
        Some(logweir_core::ids::sha256_prefixed(&stored).as_str()),
        "the digest in the SIGNED bytes covers what was uploaded"
    );
    assert_eq!(
        doc["evidence"]["offset_report_key"].as_str(),
        Some(format!("logweir/drills/{run_id}.offsets.json").as_str())
    );

    // ABSENT IS LEGAL, and it serialises to NOTHING — which is what keeps the
    // three checked-in signed fixtures byte-identical (GC12's price, paid
    // without re-minting them).
    let store2 = fixtures::recording_store();
    let mut sc2 = fixtures::scorecard_pass();
    sc2.run_id = "01J9XNOOFFSETS0000000000AA".into();
    let run2 = sc2.run_id.clone();
    let signed2 = drill::phase8_score::run(&sc2, &fixtures::good_signing_key().path, &store2, None)
        .expect("a run with no offset report still signs");
    assert_eq!(
        store2.puts(),
        vec![
            format!("logweir/drills/{run2}.json"),
            format!("logweir/drills/{run2}.sig"),
        ]
    );
    assert!(signed2.offset_report_key.is_none());
    let doc2: serde_json::Value = serde_json::from_slice(&signed2.bytes).expect("signed JSON");
    assert!(
        doc2["evidence"].get("offset_report_key").is_none()
            && doc2["evidence"].get("offset_report_sha256").is_none(),
        "an absent report adds no key at all, not a null one: {}",
        doc2["evidence"]
    );
}

/// **The offset report reaches the bucket on a WHOLE RUN too**, not only
/// through the phase-8 seam.
///
/// This is the row that kills a mutant that passes `None` at the orchestrator's
/// own call site — the phase-8 test above cannot see that, because it calls
/// phase 8 directly.
#[test]
fn a_whole_run_uploads_the_offset_report_and_names_its_prefix() {
    let f = fixtures::orchestrator_args_against_fixture_engine();
    let outcome =
        drill::execute_with_outcome(&f.args, &f.run_id, &f.ctx).expect("the fixture drill passes");
    let run_id = &f.run_id;
    assert_eq!(
        outcome.evidence.offset_report_key.as_deref(),
        Some(format!("logweir/drills/{run_id}.offsets.json").as_str()),
        "the run names the key it put the engine's report at"
    );
    assert_eq!(
        outcome.evidence.scorecard_key,
        format!("logweir/drills/{run_id}.json")
    );
    assert_eq!(
        outcome.evidence.sidecar_key,
        format!("logweir/drills/{run_id}.sig")
    );
    assert_eq!(
        outcome.scorecard.evidence.offset_report_sha256,
        Some(logweir_core::ids::sha256_prefixed(
            fixtures::OFFSET_REPORT_BYTES
        )),
        "and binds the bytes the engine actually wrote"
    );
    // The fixture spec is `scratch` (it names no mode), so its signed prefix is
    // the scratch one — the control for the `newTopic` prefix assertion above.
    assert_eq!(outcome.scorecard.target.topic_mapping_prefix, "drill-");
}

// ===========================================================================
// I8 — THE THREE STDOUT LINES
// ===========================================================================

/// **I8.** The final three stdout lines of a successful restore are
/// `scorecard-key=<k>`, `sidecar-key=<k>`, `offset-report-key=<k>`, in that
/// order, with nothing after them.
///
/// # Why this row re-execs the test binary
///
/// Exactly interface I7's reason in `crates/logweir/tests/backup_run.rs`: the
/// claim is about a PROCESS's stdout, `println!` cannot be captured in process
/// without an fd redirect (a new dependency, which Global Constraint 38
/// forbids), and `run_with`'s doubles cannot be handed to the `logweir` binary,
/// which would additionally need a broker, a bucket and the engine. So the
/// parent runs this binary's `#[ignore]`d child row, which performs the same
/// in-process `run_with` against the same doubles and calls
/// `std::process::exit` so libtest's own summary never reaches stdout after the
/// three lines under test.
///
/// The child asserts nothing about ordering; the parent asserts everything,
/// over bytes the child actually wrote to fd 1.
///
/// The mutant this exists for: reorder the three `println!`s. The
/// `starts_with` rows below then fail at assertion time.
#[test]
fn the_runner_prints_its_three_evidence_keys_last() {
    let out = Command::new(std::env::current_exe().expect("this test binary's own path"))
        .args([
            "--ignored",
            "--exact",
            "the_i8_child_runs_one_restore_and_exits",
            "--nocapture",
        ])
        .output()
        .expect("re-exec this test binary");
    let stdout = String::from_utf8(out.stdout).expect("stdout is utf-8");
    assert_eq!(
        out.status.code(),
        Some(0),
        "the child must succeed, or its stdout is not a successful run's stdout. \
         stdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let lines: Vec<&str> = stdout.lines().collect();
    let last3 = &lines[lines.len().saturating_sub(3)..];
    assert_eq!(
        last3.len(),
        3,
        "a successful restore prints at least the three evidence keys, got:\n{stdout}"
    );
    assert!(
        last3[0].starts_with("scorecard-key=logweir/drills/") && last3[0].ends_with(".json"),
        "line 1 of 3 is `scorecard-key=<k>`, got {:?} in:\n{stdout}",
        last3[0]
    );
    assert!(
        last3[1].starts_with("sidecar-key=logweir/drills/") && last3[1].ends_with(".sig"),
        "line 2 of 3 is `sidecar-key=<k>`, got {:?} in:\n{stdout}",
        last3[1]
    );
    assert!(
        last3[2].starts_with("offset-report-key=logweir/drills/")
            && last3[2].ends_with(".offsets.json"),
        "line 3 of 3 is `offset-report-key=<k>`, got {:?} in:\n{stdout}",
        last3[2]
    );
    assert!(
        stdout.ends_with(&format!("{}\n{}\n{}\n", last3[0], last3[1], last3[2])),
        "nothing may be printed after the three keys — a controller reads a bounded TAIL of the \
         pod log, which has no stream selector (plan erratum E4):\n{stdout}"
    );
    // The three keys name ONE run: same prefix, same run-id stem.
    let stem = |line: &str| {
        line.split('=')
            .nth(1)
            .expect("key=value")
            .trim_start_matches("logweir/drills/")
            .split('.')
            .next()
            .expect("a stem")
            .to_string()
    };
    assert_eq!(stem(last3[0]), stem(last3[1]));
    assert_eq!(stem(last3[0]), stem(last3[2]));
}

/// The child of `the_runner_prints_its_three_evidence_keys_last`. `#[ignore]`d
/// so it never runs in the default set; the parent invokes it by exact name.
///
/// It calls `std::process::exit` rather than returning, so libtest prints no
/// summary after the three keys. Nothing here asserts an ordering — that is
/// the parent's whole job, over the bytes this process wrote.
#[test]
#[ignore]
fn the_i8_child_runs_one_restore_and_exits() {
    let f = fixtures::orchestrator_args_against_fixture_engine();
    let mut discarded = Vec::new();
    let code = drill::run_with(
        &f.args,
        &f.run_id,
        &f.ctx,
        drill::InvokedAs::Restore,
        &mut discarded,
    );
    // The deprecation line is NOT printed for `restore run`, which the parent
    // would otherwise see on stderr.
    assert!(discarded.is_empty());
    std::process::exit(code as u8 as i32);
}

// ===========================================================================
// I20 — ONE GRAMMAR, TWO NAMES
// ===========================================================================

/// **I20.** `RestoreSpec` is the canonical name and `DrillSpec` is the same
/// type, so every checked-in reference keeps compiling and neither name has
/// drifted from the other.
///
/// It asserts the two accept the SAME BYTES rather than that the alias exists:
/// a `pub type` that compiles proves nothing an adopter can observe, and if
/// `RestoreSpec` were ever made a distinct struct this row fails.
#[test]
fn restore_spec_is_the_canonical_name() {
    let text = include_str!("../../../examples/restore.yaml");
    let as_restore: RestoreSpec =
        serde_yaml::from_str(text).expect("RestoreSpec accepts the shipped example");
    let as_drill: DrillSpec =
        serde_yaml::from_str(text).expect("DrillSpec accepts the very same bytes");

    // Same bytes in, same document out — compared through serde, because the
    // type has no `PartialEq` and the wire form is what a plan-bytes producer
    // and a reader actually share.
    assert_eq!(
        serde_yaml::to_string(&as_restore).unwrap(),
        serde_yaml::to_string(&as_drill).unwrap(),
        "`RestoreSpec` and `DrillSpec` must be one type; two grammars is what interface I20 \
         exists to prevent"
    );
    // And the tag-0 example parses under the canonical name too, in the other
    // direction, so the alias is not one-way.
    let drill_example = include_str!("../../../examples/drill.yaml");
    let _: RestoreSpec = serde_yaml::from_str(drill_example)
        .expect("the drill example is a RestoreSpec with mode: scratch");
    let parsed: RestoreSpec = serde_yaml::from_str(drill_example).unwrap();
    assert_eq!(
        parsed.target.mode,
        TargetMode::Scratch,
        "a document that names no mode is a scratch restore — which is what a drill IS"
    );
}

/// **I20.** The shipped `newTopic` sample is the restore plan document.
///
/// The mutant this exists for: give `examples/restore.yaml` a shape
/// `RestoreSpec` does not accept — `pointInTime` at the top level instead of
/// `restore.point_in_time`, say. This fails at deserialisation, which is the
/// whole reason I20 names one grammar.
#[test]
fn the_shipped_restore_example_deserialises_as_a_restore_spec() {
    let text = include_str!("../../../examples/restore.yaml");
    let spec: RestoreSpec = serde_yaml::from_str(text).expect("the shipped example must parse");

    assert_eq!(spec.target.mode, TargetMode::NewTopic);
    assert!(
        spec.restore.point_in_time.is_some(),
        "the flagship sample names a recovery point"
    );
    assert_eq!(
        spec.restore.point_in_time.map(|t| t.timestamp_millis()),
        Some(POINT_IN_TIME_MS)
    );
    assert_eq!(spec.target.bootstrap_servers, vec!["localhost:9092"]);
    assert_eq!(spec.source.topics, vec!["orders", "payments"]);
    // No explicit naming block, so the default prefix applies and `orders`
    // becomes `restore-20260907T140500Z-orders`.
    assert!(spec.target.topic_naming.is_none());
    assert_eq!(target_topic_prefix(&spec), "restore-20260907T140500Z-");
    // GC14's footer sentence, in the header comment.
    assert!(
        text.contains("Logweir is not affiliated with or endorsed by the ASF."),
        "examples/restore.yaml is missing the ASF footer sentence"
    );
}

/// The shipped example names the HOST-side MinIO endpoint in both storage
/// blocks. `logweir` and the engine run on the host, so the compose SERVICE
/// name does not resolve there (critique A F23).
#[test]
fn the_shipped_restore_example_is_host_side() {
    let text = include_str!("../../../examples/restore.yaml");
    assert!(
        text.contains("http://localhost:9000"),
        "examples/restore.yaml must name the HOST-side endpoint"
    );
    assert!(
        !text.contains("http://minio:9000"),
        "examples/restore.yaml names the compose service endpoint, which does not resolve on \
         the host the engine actually runs on"
    );
    // BOTH storage blocks, counted over `endpoint:` LINES rather than over the
    // whole text: the header comment names the endpoint too, and a count over
    // the raw text would drift with every prose edit.
    assert_eq!(
        text.lines()
            .filter(|l| l.trim() == "endpoint: http://localhost:9000")
            .count(),
        2,
        "both storage blocks — the archive and the evidence sink — are host-side:\n{text}"
    );
}

/// **`logweir drill run` is an alias for `logweir restore run`.**
///
/// Three claims, three assertions: the two command lines parse to the SAME args
/// value; one whole invocation over doubles reaches the same function and the
/// same exit code under either name; and the alias prints EXACTLY ONE line on
/// stderr while the name prints none.
///
/// No binary is run against a broker (critique A F1, Global Constraint 22); the
/// byte-identity of two full runs against the real stack is `just mvp-demo`'s,
/// in Task 12, under `#![cfg(feature = "e2e")]`.
#[test]
fn cli_drill_run_is_an_alias_for_restore_run() {
    use clap::Parser;

    let flags = [
        "--spec",
        "restore.yaml",
        "--approval",
        "approval.json",
        "--approver-key",
        "approver.pem",
        "--allowed-clusters",
        "allowed.json",
        "--signing-key",
        "signer.pem",
        "--offset-report-out",
        "offsets.json",
    ];
    let parse = |verb: &str| {
        let mut argv = vec!["logweir", verb, "run"];
        argv.extend_from_slice(&flags);
        match logweir::cli::Cli::try_parse_from(argv)
            .unwrap_or_else(|e| panic!("`logweir {verb} run` must parse: {e}"))
            .command
        {
            logweir::cli::Command::Restore(logweir::cli::RestoreCmd::Run(a)) => a,
            logweir::cli::Command::Drill(logweir::cli::DrillCmd::Run(a)) => a,
            _ => panic!("`logweir {verb} run` parsed as some other subcommand entirely"),
        }
    };
    let by_restore = parse("restore");
    let by_drill = parse("drill");
    assert_eq!(
        by_restore, by_drill,
        "the two names must yield the SAME args value — one clap struct, flattened into both \
         subcommands, is what makes that a property of the type rather than a promise"
    );
    assert_eq!(
        by_restore.offset_report_out.as_deref(),
        Some(std::path::Path::new("offsets.json")),
        "`drill run` accepts --offset-report-out too: the alias's flag set is a SUPERSET, never \
         a second list to keep in step"
    );

    // ONE invocation of the shared function, over in-process doubles, under
    // each name — and the exit code is the same.
    let mut restore_err: Vec<u8> = Vec::new();
    let f = fixtures::orchestrator_args_against_fixture_engine();
    let code_restore = drill::run_with(
        &f.args,
        &f.run_id,
        &f.ctx,
        drill::InvokedAs::Restore,
        &mut restore_err,
    );
    assert_eq!(code_restore, ExitCode::Ok);
    assert!(
        restore_err.is_empty(),
        "`restore run` is the NAME and prints no deprecation line, got {:?}",
        String::from_utf8_lossy(&restore_err)
    );

    let mut drill_err: Vec<u8> = Vec::new();
    let g = fixtures::orchestrator_args_against_fixture_engine();
    let code_drill = drill::run_with(
        &g.args,
        &g.run_id,
        &g.ctx,
        drill::InvokedAs::DrillAlias,
        &mut drill_err,
    );
    assert_eq!(
        code_drill, code_restore,
        "the alias delegates to the same function and cannot reach a different exit code"
    );
    let printed = String::from_utf8(drill_err).expect("stderr is utf-8");
    assert_eq!(
        printed,
        format!("{}\n", drill::DRILL_RUN_DEPRECATION),
        "EXACTLY ONE line, and it is the constant"
    );
    assert_eq!(printed.lines().count(), 1, "one line, not two: {printed:?}");
    assert_eq!(
        drill::DRILL_RUN_DEPRECATION,
        "logweir drill run is the tag-0 name for logweir restore run and will be removed in tag 2"
    );
}

/// Both `--help` screens exit 0, read directly (STANDING RULE 20 — never
/// through a pipe).
///
/// `--help` routes through clap's `Err` path, which `main.rs` maps to exit 0
/// for the help and version cases and to exit 1 for a usage error; a new
/// subcommand that regressed that mapping would report "logweir could not do
/// its job" for `--help`.
#[test]
fn both_run_help_screens_exit_zero() {
    for verb in ["restore", "drill"] {
        let out = Command::new(env!("CARGO_BIN_EXE_logweir"))
            .args([verb, "run", "--help"])
            .output()
            .unwrap_or_else(|e| panic!("run the compiled binary: {e}"));
        assert_eq!(
            out.status.code(),
            Some(0),
            "`logweir {verb} run --help` must exit 0, got {:?}\n{}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr)
        );
        let help = String::from_utf8_lossy(&out.stdout).to_string();
        for flag in [
            "--spec",
            "--approval",
            "--approver-key",
            "--allowed-clusters",
            "--signing-key",
            "--triggered-by",
            "--out",
            "--metrics-file",
            "--offset-report-out",
        ] {
            assert!(
                help.contains(flag),
                "`logweir {verb} run --help` must list {flag}:\n{help}"
            );
        }
    }
}
