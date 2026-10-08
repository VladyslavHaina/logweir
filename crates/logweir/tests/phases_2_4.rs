mod fixtures;
use logweir::drill::{phase2_target, phase3_diff, phase4_sample, DrillError};
use logweir_core::engine::BackupSetRef;
use logweir_core::spec::Anchor;
use logweir_kafka::reader::{ClusterReader, ConsumedRecord, KafkaError, TopicMeta};
use std::collections::BTreeMap;

/// A fixed, arbitrary cluster id shared by the `target_with`/`empty_target`
/// calls in this file — nothing below asserts on its value except the phase-2
/// test, which checks that `phase2_target::run` reports back exactly the
/// cluster id its `ClusterReader` was built with.
const TARGET_CLUSTER_ID: &str = "MkU3OEVBNTcwNTJENDM2Qk";

#[test]
fn the_diff_reports_an_existing_target_topic_as_a_collision_with_its_current_state() {
    // The target's `cleanup.policy` is set to "delete", NOT the manifest's
    // "compact" (see `fixtures::backup_facts_orders`) — a genuine value
    // mismatch is required to exercise `existing_configs_differing` at all.
    // The brief's own literal fixture reused "compact" on both sides, which
    // makes `contains(&"cleanup.policy".to_string())` FAIL no matter how
    // phase 3 is implemented (equal values can never be "differing") — this
    // is a correction to the test's own data, not to the diff logic. Task 16
    // review round 1 also found a mutant that replaces the value comparison
    // with `contains_key` (i.e. "present at all", not "present and unequal")
    // and still passes every test; `retention.ms` below, set to the SAME
    // value on both sides, is what kills that mutant — it must NOT appear in
    // `existing_configs_differing`, while `cleanup.policy` (genuinely
    // different) must.
    let target = fixtures::target_with(
        TARGET_CLUSTER_ID,
        "drill-orders",
        3,
        1_200,
        &[("cleanup.policy", "delete"), ("retention.ms", "604800000")],
    );
    let facts = fixtures::backup_facts_orders(3);
    let d = phase3_diff::run(
        &target,
        &facts,
        &fixtures::mapping("orders", "drill-orders"),
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
    );
    assert_eq!(d.collisions.len(), 1);
    let c = &d.collisions[0];
    assert_eq!(c.topic, "drill-orders");
    assert_eq!(c.existing_partitions, 3);
    assert_eq!(
        c.existing_end_offsets, 1_200,
        "a restore into a non-empty topic is exactly what OSO's dry run cannot tell you"
    );
    assert!(
        c.existing_configs_differing
            .contains(&"cleanup.policy".to_string()),
        "a genuinely differing value must be reported"
    );
    assert!(
        !c.existing_configs_differing
            .contains(&"retention.ms".to_string()),
        "a key present on both sides with an EQUAL value must not be reported as differing \
         — a mutant that replaces the value comparison with `contains_key` would include it"
    );
}

#[test]
fn an_absent_target_topic_becomes_a_would_create_entry_at_the_manifest_partition_count() {
    let target = fixtures::empty_target(TARGET_CLUSTER_ID);
    let facts = fixtures::backup_facts_orders(3);
    let d = phase3_diff::run(
        &target,
        &facts,
        &fixtures::mapping("orders", "drill-orders"),
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
    );
    assert!(d.collisions.is_empty());
    assert_eq!(d.would_create, vec![("drill-orders".to_string(), 3)]);
}

/// `TargetDiff.absent` used to be computed and never read anywhere,
/// including inside `summarise()` — deleting the push in `phase3_diff::run`
/// passed all tests. A topic present in the backup and absent from the
/// target is a first-order fact about whether the restore can work, so it
/// must reach `TargetDiffSummary`, the signed document's own copy — not only
/// the in-process `TargetDiff`.
#[test]
fn the_summary_carries_absent_topics_not_only_would_create() {
    let target = fixtures::empty_target(TARGET_CLUSTER_ID);
    let facts = fixtures::backup_facts_orders(3);
    let d = phase3_diff::run(
        &target,
        &facts,
        &fixtures::mapping("orders", "drill-orders"),
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
    );
    assert_eq!(d.absent, vec!["drill-orders".to_string()]);
    let sum = d.summarise();
    assert_eq!(
        sum.absent,
        vec!["drill-orders".to_string()],
        "absent topics must reach the scorecard's target_diff block, not just would_create"
    );
}

/// The diff must reach a reader. Without `summarise` feeding
/// `Scorecard.target_diff`, phase 3 computes a value nothing consumes.
#[test]
fn the_diff_summarises_into_the_scorecard_block_that_drill_show_renders() {
    let target = fixtures::target_with(
        TARGET_CLUSTER_ID,
        "drill-orders",
        3,
        1_200,
        &[("cleanup.policy", "compact")],
    );
    let d = phase3_diff::run(
        &target,
        &fixtures::backup_facts_orders(3),
        &fixtures::mapping("orders", "drill-orders"),
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
    );
    let sum = d.summarise();
    assert_eq!(sum.level, "full");
    assert_eq!(sum.collisions.len(), 1);
    assert!(sum.collisions[0].contains("1200"));
}

#[test]
fn the_partition_count_falls_back_to_max_partition_id_plus_one() {
    // Old manifests lack original_partition_count; the engine derives the count
    // as max(partition_id)+1 (restore/engine.rs:1421-1436), so our plan must
    // agree or `would_create` lies about what the restore will build.
    let mut facts = fixtures::backup_facts_orders(3);
    facts.topics[0].original_partition_count = None;
    let d = phase3_diff::run(
        &fixtures::empty_target(TARGET_CLUSTER_ID),
        &facts,
        &fixtures::mapping("orders", "drill-orders"),
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
    );
    assert_eq!(d.would_create, vec![("drill-orders".to_string(), 3)]);
}

#[test]
fn sample_select_narrows_to_the_window_and_counts_expected_records() {
    let facts = fixtures::backup_facts_orders(3);
    let spec = fixtures::sample_spec(
        "2026-08-29T00:00:00Z",
        "2026-08-30T02:00:00Z",
        25,
        Anchor::Head,
    );
    let s = phase4_sample::run(&facts, &spec, &["orders".into()]).unwrap();
    assert_eq!(s.topics, 1);
    assert_eq!(s.partitions, 3);
    assert!(s.records_expected > 0);
    assert_eq!(s.per_partition.len(), 3);
    assert!(s
        .per_partition
        .iter()
        .all(|p| p.count == 25 && p.anchor == Anchor::Head));
}

#[test]
fn a_window_covering_no_segment_is_an_error_not_an_empty_pass() {
    // Mirrors OSO's own rule: "Zero records scanned is never reported as a
    // positive pass" (docs/restore-preflight.md @ v0.21.0).
    let facts = fixtures::backup_facts_orders(3);
    let spec = fixtures::sample_spec(
        "2020-01-01T00:00:00Z",
        "2020-01-02T00:00:00Z",
        25,
        Anchor::Head,
    );
    assert!(phase4_sample::run(&facts, &spec, &["orders".into()]).is_err());
}

/// Task 19 fix round 1 (review finding F2): `records_per_partition: 0` reaches
/// `SampleSelection.count` with nothing to reject it, which lets
/// `OsoCliEngine::fingerprints` return `Ok(vec![])` (an empty archive, not an
/// error) and phase 7 sign a byte-fingerprint `Pass` over zero comparisons.
/// Refused here, at the source.
#[test]
fn records_per_partition_zero_is_rejected_before_it_reaches_a_sample_selection() {
    let facts = fixtures::backup_facts_orders(3);
    let spec = fixtures::sample_spec(
        "2026-08-29T00:00:00Z",
        "2026-08-30T02:00:00Z",
        0,
        Anchor::Head,
    );
    let err = phase4_sample::run(&facts, &spec, &["orders".into()]).unwrap_err();
    assert!(matches!(err, DrillError::Operational(_)));
    assert!(err.to_string().contains("records_per_partition"));
}

#[test]
fn a_partition_with_a_gap_or_a_pruned_range_inside_the_window_is_reported() {
    let mut facts = fixtures::backup_facts_orders(3);
    facts.topics[0].partitions[0].gaps.push((150, 200));
    let s = phase4_sample::run(
        &facts,
        &fixtures::sample_spec(
            "2026-08-29T00:00:00Z",
            "2026-08-30T02:00:00Z",
            25,
            Anchor::Head,
        ),
        &["orders".into()],
    )
    .unwrap();
    assert!(
        s.notes.iter().any(|n| n.contains("gap")),
        "a known capture gap inside the sampled window must be stated, not silently sampled around"
    );
}

/// `gaps`/`pruned` are OFFSET ranges; the sample window is a TIMESTAMP range
/// — the two are not directly comparable, and a gap that is real but sits
/// OUTSIDE the offsets this window actually reads must not be claimed to
/// "overlap the sampled window" (that string is copied verbatim into the
/// signed scorecard's `sample.coverage_note`; Task 16 review round 1,
/// MAJOR-1). `backup_facts_orders(3)`'s segments span offsets 0..499 for the
/// window used here, so a gap at 100_000..200_000 is nowhere near what was
/// read and must produce no note at all.
#[test]
fn a_gap_outside_the_sampled_offsets_is_not_reported_as_overlapping() {
    let mut facts = fixtures::backup_facts_orders(3);
    facts.topics[0].partitions[0].gaps.push((100_000, 200_000));
    facts.topics[0].partitions[0]
        .pruned
        .push((900_000, 999_999));
    let s = phase4_sample::run(
        &facts,
        &fixtures::sample_spec(
            "2026-08-29T00:00:00Z",
            "2026-08-30T02:00:00Z",
            25,
            Anchor::Head,
        ),
        &["orders".into()],
    )
    .unwrap();
    assert!(
        s.notes.is_empty(),
        "a gap/pruned range far outside the offsets this window actually read must not be \
         claimed to overlap it: {:?}",
        s.notes
    );
}

/// Phase 2 is the input phase 3 diffs against; an untested reader would make
/// every collision assertion above rest on an unverified read.
#[test]
fn phase_2_records_the_actual_state_of_only_the_topics_of_interest() {
    let reader = fixtures::FakeReader {
        state: fixtures::target_with(
            TARGET_CLUSTER_ID,
            "drill-orders",
            3,
            1_200,
            &[("cleanup.policy", "compact")],
        ),
    };
    let st = phase2_target::run(
        &reader,
        &["drill-orders".to_string(), "drill-absent".to_string()],
    )
    .unwrap();
    assert_eq!(st.cluster_id, TARGET_CLUSTER_ID);
    assert_eq!(
        st.topics.len(),
        1,
        "a topic of interest that the target does not have is skipped, not an error — \
         phase 3 turns it into a would_create entry"
    );
    let t = &st.topics["drill-orders"];
    assert_eq!(t.partitions, 3);
    assert_eq!(t.end_offsets.iter().map(|(_, hi)| *hi).sum::<i64>(), 1_200);
    assert_eq!(
        t.configs.get("cleanup.policy").map(String::as_str),
        Some("compact")
    );
}

/// A `ClusterReader` double whose `list_topics` reports a topic PRESENT but
/// carrying a metadata error — a leader election, an authorization gap, a
/// topic mid-delete (see `TopicMeta::error`'s own doc comment). Its
/// `end_offsets`/`topic_configs` answer Ok with empty data, exactly as a
/// naive phase 2 implementation (one that skips the `meta.error` check)
/// would happily accept: that is precisely the failure mode this double
/// exists to catch.
struct ErroredTopicReader;

impl ClusterReader for ErroredTopicReader {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        Ok("ERRORED0000000000000000".to_string())
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        Ok(vec![TopicMeta::errored(
            "drill-orders",
            "leader election in progress",
        )])
    }
    fn end_offsets(&self, _topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        Ok(vec![])
    }
    fn topic_configs(&self, _topic: &str) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(BTreeMap::new())
    }
    /// Task 8 (guard **G-TS**) added this to `ClusterReader`. An empty map is
    /// a broker that surfaces neither `log.message.timestamp.type` nor a
    /// timestamp bound, which is the harmless case: phase 0's preflight then
    /// treats the broker as the Apache default (`CreateTime`) and refuses
    /// nothing. G-TS's own arms live in
    /// `crates/logweir/tests/topic_preflight.rs`.
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(BTreeMap::new())
    }
    fn consume_range(
        &self,
        _topic: &str,
        _partition: i32,
        _from: i64,
        _max: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        Ok(vec![])
    }
}

/// The property this phase exists to protect: a topic the reader could not
/// describe must never be recorded as a topic that is empty (a `TopicState`
/// with zero end offsets) or absent (skipped from `TargetState.topics`) —
/// those are different facts, and phase 3 draws opposite conclusions from
/// them ("safe to create fresh" vs. "restoring into this will collide with
/// what's already there"). Proven at the TYPE level, not just in a log line:
/// `ErroredTopicReader`'s `end_offsets`/`topic_configs` would happily return
/// an empty-but-Ok answer for `drill-orders` if phase 2 asked, so a version
/// of `phase2_target::run` that omitted the `meta.error` check would pass
/// this topic through as "known, empty" and this test would then observe
/// `Ok` with either an absent or a zeroed entry instead of `Err` — i.e. this
/// test fails if that routing is inverted or dropped.
#[test]
fn a_topic_present_but_unreadable_is_never_recorded_as_absent_or_empty() {
    let reader = ErroredTopicReader;
    let err = phase2_target::run(&reader, &["drill-orders".to_string()]).unwrap_err();
    match err {
        DrillError::Operational(msg) => {
            assert!(msg.contains("drill-orders"), "{msg}");
            assert!(msg.contains("leader election"), "{msg}");
        }
        other => panic!("expected an operational failure (exit 1), not {other:?}"),
    }
}

/// `max_partitions` truncates `per_partition` — a global cap across every
/// topic's partitions. The reported aggregates (`records_expected`,
/// `topics`, `partitions`) must describe exactly what survives that
/// truncation, never a pre-truncation total for partitions the `Selection`
/// no longer names (Task 16 review round 1, MINOR-4: this was previously
/// computed before truncation ran and was entirely untested). With
/// `max_partitions: Some(1)` over `backup_facts_orders(3)`'s 3 partitions
/// (each contributing 500 records — two 250-record segments), only the
/// first partition survives, so `records_expected` must be 500, not the
/// pre-truncation total of 1500 across all three.
#[test]
fn max_partitions_truncates_consistently_with_the_reported_counts() {
    let facts = fixtures::backup_facts_orders(3);
    let mut spec = fixtures::sample_spec(
        "2026-08-29T00:00:00Z",
        "2026-08-30T02:00:00Z",
        25,
        Anchor::Head,
    );
    spec.max_partitions = Some(1);
    let s = phase4_sample::run(&facts, &spec, &["orders".into()]).unwrap();
    assert_eq!(
        s.per_partition.len(),
        1,
        "truncation must actually shrink per_partition"
    );
    assert_eq!(
        s.partitions, 1,
        "partitions must count what survived truncation, not the pre-truncation total"
    );
    assert_eq!(
        s.records_expected, 500,
        "records_expected must sum only the segments behind the partition actually \
         selected after truncation, not the pre-truncation total across all 3 partitions"
    );
    assert_eq!(s.topics, 1);
}

/// PROD-08.1: a COMPLETE selection is every partition the manifest lists for
/// a restored topic — even one whose segments' first/last timestamps all lie
/// outside the sample window (the engine's selection, which complete coverage
/// does not read), and even with `max_partitions` set (phase 0 refuses the
/// pairing; phase 4 never truncates a complete selection on its own). Every
/// gap and pruned range of those partitions is noted. KILLS: keeping the
/// window filter in complete mode (the far-away partition disappears), or
/// applying `max_partitions` (two partitions disappear).
#[test]
fn a_complete_selection_is_every_listed_partition_whatever_the_window() {
    let mut facts = fixtures::backup_facts_orders(3);
    // Partition 2's segments move to 2020: no first/last bound overlaps the
    // window below.
    for seg in &mut facts.topics[0].partitions[2].segments {
        seg.start_timestamp = 1_577_836_800_000;
        seg.end_timestamp = 1_577_836_800_000;
    }
    facts.topics[0].partitions[2].gaps.push((100_000, 200_000));
    let mut spec = fixtures::sample_spec(
        "2026-08-29T00:00:00Z",
        "2026-08-30T02:00:00Z",
        25,
        Anchor::Head,
    );
    let sampled = phase4_sample::run(&facts, &spec, &["orders".into()]).unwrap();
    assert_eq!(
        sampled.partitions, 2,
        "the control: sampling follows the window"
    );
    spec.coverage = logweir_core::spec::Coverage::Complete;
    spec.max_partitions = Some(1);
    let s = phase4_sample::run(&facts, &spec, &["orders".into()]).unwrap();
    assert_eq!(s.partitions, 3);
    assert_eq!(
        s.per_partition
            .iter()
            .map(|p| p.partition)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(s.records_expected, 1500);
    assert!(
        s.notes
            .iter()
            .any(|n| n.contains("capture gap 100000..200000 in a completely verified partition")),
        "{:?}",
        s.notes
    );
}

/// `phase4_sample::run` cannot populate `SampleSelection.set.manifest_key` —
/// `BackupSetFacts` does not carry one. `Selection::bind_backup_set` is the
/// structural fix (Task 16 review round 1, MAJOR-2): a caller who forgets to
/// call it before `DataEngine::fingerprints` gets a specific, loud
/// `EngineError::Operational` (see `logweir-engine-oso/src/engine.rs`'s
/// `fingerprints`), rather than correctness resting on a comment.
#[test]
fn selection_set_arrives_empty_and_bind_backup_set_patches_every_entry() {
    let facts = fixtures::backup_facts_orders(3);
    let spec = fixtures::sample_spec(
        "2026-08-29T00:00:00Z",
        "2026-08-30T02:00:00Z",
        25,
        Anchor::Head,
    );
    let mut s = phase4_sample::run(&facts, &spec, &["orders".into()]).unwrap();
    assert!(
        s.per_partition
            .iter()
            .all(|p| p.set.manifest_key.is_empty()),
        "phase 4 has no manifest key to populate — the caller must bind one"
    );
    let real_ref = BackupSetRef {
        backup_id: facts.backup_id.clone(),
        manifest_key: "drills/backup-2026-08-30T02:00:00Z/manifest.json".to_string(),
    };
    s.bind_backup_set(&real_ref);
    assert!(s
        .per_partition
        .iter()
        .all(|p| p.set.manifest_key == real_ref.manifest_key
            && p.set.backup_id == real_ref.backup_id));
}

// ---------------------------------------------------------------------------
// FX-4 / T13: consumer 4 (phase 2's target state) and phase 3's collision diff.
// ---------------------------------------------------------------------------

/// A target that HAS `drill-orders` (healthy metadata) but refuses its
/// DescribeConfigs — what `RdKafkaReader::topic_configs` returns now instead
/// of the empty map rdkafka 0.36.2 used to hand back for a refused resource.
struct RefusedConfigsReader;

impl ClusterReader for RefusedConfigsReader {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        Ok(TARGET_CLUSTER_ID.to_string())
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        Ok(vec![TopicMeta::new("drill-orders", 3)])
    }
    fn end_offsets(&self, _topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        Ok(vec![(0, 10), (1, 10), (2, 10)])
    }
    fn topic_configs(&self, topic: &str) -> Result<BTreeMap<String, String>, KafkaError> {
        Err(logweir_kafka::reader::empty_topic_config_answer(
            topic,
            &logweir_kafka::reader::TopicVisibility::Visible,
        ))
    }
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(BTreeMap::new())
    }
    fn consume_range(
        &self,
        _topic: &str,
        _partition: i32,
        _from: i64,
        _max: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        Ok(vec![])
    }
}

/// **FX-4 / T13, consumer 4.** An existing target topic whose configuration
/// read is REFUSED is never recorded with an empty configuration: phase 3
/// would then report "differing config: []" about a topic it never read.
///
/// Negative control: `configs: reader.topic_configs(name).unwrap_or_default()`
/// in phase 2 makes this `Ok` with an empty map, and this test fails.
#[test]
fn a_target_topic_whose_configuration_is_refused_is_never_recorded_as_empty() {
    let err = phase2_target::run(&RefusedConfigsReader, &["drill-orders".to_string()])
        .expect_err("a refused configuration read is not an empty configuration");
    match err {
        DrillError::Kafka(KafkaError::NotAuthorized(m)) => {
            assert!(m.starts_with("drill-orders"), "{m}")
        }
        other => panic!("expected the refusal, not {other:?}"),
    }
}

/// **FX-4, phase 3.** A collision's configuration difference is ASSESSED only
/// where the source topic's capture coverage is `captured`; otherwise the
/// collision is named in the scorecard's `target_diff.not_assessed`, and an
/// empty `differing config: []` is never read as "no difference".
///
/// The collision STRING is byte for byte what the writer before FX-4 produced,
/// whatever the coverage: the qualifier lives in the new optional field, never
/// inside an existing string (the owner's OD-7 rulings of 2026-10-05 made only
/// the new receipt arms and phase 7's fail-safe entry MINOR). The expected
/// lines are LITERALS of main's `summarise` format, not rebuilt from it.
///
/// Negative controls: a qualifier appended to the string fails the
/// exact-string asserts; dropping the entry for an uncaptured source, or
/// writing one for a captured source, fails the `not_assessed` asserts; a
/// `summarise` that leaves the field absent fails every `Some(..)`.
#[test]
fn a_collision_says_its_configuration_was_not_assessed_unless_the_capture_was() {
    let target = fixtures::target_with(
        TARGET_CLUSTER_ID,
        "drill-orders",
        3,
        1_200,
        &[("cleanup.policy", "delete")],
    );
    let facts = fixtures::backup_facts_orders(3);
    let mapping = fixtures::mapping("orders", "drill-orders");
    let line = "drill-orders: 3 partition(s), 1200 record(s) already present, \
                differing config: [cleanup.policy]";

    // Unbound plan: coverage UNKNOWN.
    let d = phase3_diff::run(
        &target,
        &facts,
        &mapping,
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
    );
    assert_eq!(d.collisions[0].configuration_not_assessed, Some("unknown"));
    let s = d.summarise();
    assert_eq!(s.collisions, vec![line.to_string()]);
    assert_eq!(
        s.not_assessed,
        Some(vec!["drill-orders: configuration (unknown)".to_string()])
    );

    // A verified receipt that says the capture was DENIED. The archive's
    // record is then EMPTY, so the line reads `differing config: []` — the
    // silence the `not_assessed` entry withdraws.
    let mut denied_facts = fixtures::backup_facts_orders(3);
    denied_facts.topics[0].configurations.clear();
    let denied = coverage_for("orders", "captureDenied");
    let d = phase3_diff::run(&target, &denied_facts, &mapping, &denied);
    assert_eq!(
        d.collisions[0].configuration_not_assessed,
        Some("captureDenied")
    );
    let s = d.summarise();
    assert_eq!(
        s.collisions,
        vec![
            "drill-orders: 3 partition(s), 1200 record(s) already present, differing config: []"
                .to_string()
        ]
    );
    assert_eq!(
        s.not_assessed,
        Some(vec![
            "drill-orders: configuration (captureDenied)".to_string()
        ])
    );

    // …and one that says it was captured: assessed, the same line, and an
    // empty list that IS the claim.
    let captured = coverage_for("orders", "captured");
    let d = phase3_diff::run(&target, &facts, &mapping, &captured);
    assert_eq!(d.collisions[0].configuration_not_assessed, None);
    let s = d.summarise();
    assert_eq!(s.collisions, vec![line.to_string()]);
    assert_eq!(s.not_assessed, Some(vec![]));

    // No collision at all — the normal case, since phase 0 refuses a mapped
    // target topic that already exists: phase 3 ran, so the field is the
    // claim `[]`, never absent.
    let d = phase3_diff::run(
        &fixtures::empty_target(TARGET_CLUSTER_ID),
        &facts,
        &mapping,
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
    );
    let s = d.summarise();
    assert!(s.collisions.is_empty(), "{:?}", s.collisions);
    assert_eq!(s.not_assessed, Some(vec![]));
}

/// A `SourceConfigCoverage` as a VERIFIED receipt carrying one entry reads.
fn coverage_for(topic: &str, coverage: &str) -> logweir_core::backup_receipt::SourceConfigCoverage {
    let mut receipt: logweir_core::backup_receipt::BackupReceipt = serde_json::from_slice(
        &std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../e2e/fixtures/signed/backup-receipt.json"),
        )
        .unwrap(),
    )
    .unwrap();
    receipt.format_version = "1.1.0".into();
    receipt.config_coverage = Some(BTreeMap::from([(
        topic.to_string(),
        logweir_core::backup_receipt::TopicConfigCoverage {
            coverage: coverage.to_string(),
            reason: None,
            timestamp_type: None,
        },
    )]));
    logweir_core::backup_receipt::SourceConfigCoverage::from_receipt(&receipt)
}
