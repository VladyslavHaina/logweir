mod fixtures;
use logweir::drill::{phase2_target, phase3_diff, phase4_sample, DrillError};
use logweir_core::engine::BackupSetRef;
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
    );
    assert_eq!(d.would_create, vec![("drill-orders".to_string(), 3)]);
}

#[test]
fn sample_select_narrows_to_the_window_and_counts_expected_records() {
    let facts = fixtures::backup_facts_orders(3);
    let spec = fixtures::sample_spec("2026-08-29T00:00:00Z", "2026-08-30T02:00:00Z", 25, "head");
    let s = phase4_sample::run(&facts, &spec, &["orders".into()]).unwrap();
    assert_eq!(s.topics, 1);
    assert_eq!(s.partitions, 3);
    assert!(s.records_expected > 0);
    assert_eq!(s.per_partition.len(), 3);
    assert!(s
        .per_partition
        .iter()
        .all(|p| p.count == 25 && p.anchor == "head"));
}

#[test]
fn a_window_covering_no_segment_is_an_error_not_an_empty_pass() {
    // Mirrors OSO's own rule: "Zero records scanned is never reported as a
    // positive pass" (docs/restore-preflight.md @ v0.21.0).
    let facts = fixtures::backup_facts_orders(3);
    let spec = fixtures::sample_spec("2020-01-01T00:00:00Z", "2020-01-02T00:00:00Z", 25, "head");
    assert!(phase4_sample::run(&facts, &spec, &["orders".into()]).is_err());
}

#[test]
fn a_partition_with_a_gap_or_a_pruned_range_inside_the_window_is_reported() {
    let mut facts = fixtures::backup_facts_orders(3);
    facts.topics[0].partitions[0].gaps.push((150, 200));
    let s = phase4_sample::run(
        &facts,
        &fixtures::sample_spec("2026-08-29T00:00:00Z", "2026-08-30T02:00:00Z", 25, "head"),
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
        &fixtures::sample_spec("2026-08-29T00:00:00Z", "2026-08-30T02:00:00Z", 25, "head"),
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
    let mut spec =
        fixtures::sample_spec("2026-08-29T00:00:00Z", "2026-08-30T02:00:00Z", 25, "head");
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

/// `phase4_sample::run` cannot populate `SampleSelection.set.manifest_key` —
/// `BackupSetFacts` does not carry one. `Selection::bind_backup_set` is the
/// structural fix (Task 16 review round 1, MAJOR-2): a caller who forgets to
/// call it before `DataEngine::fingerprints` gets a specific, loud
/// `EngineError::Operational` (see `logweir-engine-oso/src/engine.rs`'s
/// `fingerprints`), rather than correctness resting on a comment.
#[test]
fn selection_set_arrives_empty_and_bind_backup_set_patches_every_entry() {
    let facts = fixtures::backup_facts_orders(3);
    let spec = fixtures::sample_spec("2026-08-29T00:00:00Z", "2026-08-30T02:00:00Z", 25, "head");
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
