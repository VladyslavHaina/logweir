mod fixtures; // crates/logweir/tests/fixtures/mod.rs — Task 14 step 5c
use logweir::drill::phase7_verify::{classify_parity, compare};
use logweir_core::outcome::{IntegrityLevel, IntegrityResult};

// ---------------------------------------------------------------------------
// task-19-brief.md Step 1, verbatim. Six tests.

#[test]
fn identical_records_reconcile_at_pass_rate_one() {
    let (arch, cons) = fixtures::matching_pair(50);
    let (sampled, matching, mismatches) = compare(&arch, &cons);
    assert_eq!((sampled, matching), (50, 50));
    assert!(mismatches.is_empty());
}

#[test]
fn a_single_changed_value_is_a_mismatch_not_a_count_difference() {
    let (arch, mut cons) = fixtures::matching_pair(50);
    cons[7].value = Some(b"tampered".to_vec());
    let (sampled, matching, mismatches) = compare(&arch, &cons);
    assert_eq!(sampled, 50);
    assert_eq!(matching, 49);
    assert_eq!(mismatches.len(), 1);
    assert!(mismatches[0].contains("offset")); // harness-only: asserts on `compare`'s own mismatch wording, not a kafka-backup CLI subcommand
}

#[test]
fn an_archive_offset_absent_from_the_target_is_a_mismatch() {
    let (arch, mut cons) = fixtures::matching_pair(50);
    cons.remove(7);
    let (sampled, matching, _) = compare(&arch, &cons);
    assert_eq!((sampled, matching), (50, 49));
}

/// A compacted topic cannot be reconciled record-for-record: the target
/// legitimately holds fewer records. That is `partial`, and partial is NOT a
/// pass — it produces outcome fail-integrity with a non-null reason.
#[test]
fn a_compacted_topic_is_partial_with_a_reason() {
    let o = fixtures::verify_outcome_for_compacted_topic();
    assert_eq!(o.integrity.result, IntegrityResult::Partial);
    assert!(o
        .integrity
        .partial_reason
        .as_ref()
        .unwrap()
        .contains("compact"));
}

#[test]
fn an_unsupported_kbak_segment_degrades_to_consume_only_never_to_a_silent_pass() {
    let o = fixtures::verify_outcome_when_fingerprints_unsupported();
    assert_eq!(o.integrity.level, IntegrityLevel::ConsumeOnly);
    assert!(
        o.pass_rate().is_none(),
        "pass_rate is null when fingerprints were not computed"
    );
}

/// Phase 6 sets create_topics, an explicit replication factor and the partition
/// count; a scratch cluster has infinite retention and cleanup.policy=delete.
/// Those deviations are INTENDED and are reported as such, never as failures.
#[test]
fn scratch_deviations_are_intentional_and_anything_else_is_not() {
    let (intended, unexpected) = classify_parity(
        &fixtures::source_configs(&[
            ("cleanup.policy", "compact"),
            ("retention.ms", "604800000"),
            ("max.message.bytes", "1048576"),
        ]),
        &fixtures::target_configs(&[
            ("cleanup.policy", "delete"),
            ("retention.ms", "-1"),
            ("max.message.bytes", "999"),
        ]),
        /*src_partitions*/ 3,
        /*tgt_partitions*/ 3,
        /*src_rf*/ 3,
        /*tgt_rf*/ 1,
    );
    assert!(intended.contains(&"cleanup.policy".to_string()));
    assert!(intended.contains(&"retention.ms".to_string()));
    assert!(
        intended.contains(&"replication_factor".to_string()),
        "spec §9.3 phase 7(d): the RF divergence phase 6 deliberately creates must be reported"
    );
    assert!(
        !intended.contains(&"partition_count".to_string()),
        "the partition count matched, so it is not a deviation at all"
    );
    assert_eq!(unexpected, vec!["max.message.bytes".to_string()]);
}

// ---------------------------------------------------------------------------
// Task 19 fix round 1: direct `compare`/`classify_parity` tests closing
// surviving mutants r4, r6, r8 from task-19-review.md.

/// r4: `compare` must match by the `x-original-offset` HEADER, not the
/// consumed record's own target-side offset — that is the whole point of
/// the header (a restored topic's offsets need not start where the
/// source's did; see `compare`'s own doc comment, "never by position").
/// Every prior test's fixture happened to set the header equal to the
/// target offset, so a mutant deleting the header lookup entirely survived.
/// Here the two are DELIBERATELY different: the record's own offset is 99
/// (wherever it actually landed on the target), but its header names its
/// TRUE original archive offset, 5.
#[test]
fn compare_matches_by_the_original_offset_header_not_the_targets_own_offset() {
    // The `x-original-offset` header is part of the record's OWN content —
    // `logweir_kafka::fingerprint::record_fingerprint` hashes every header,
    // and both sides of a genuine backup/restore round trip carry the same
    // header set (see `fixtures::matching_pair`) — so it is included on
    // BOTH the archive fingerprint and the consumed record here, exactly as
    // it would be in a real restored record.
    let headers = vec![("x-original-offset".to_string(), Some(b"5".to_vec()))];
    let archive = vec![logweir_core::engine::RecordFingerprint {
        topic: "orders".into(),
        partition: 0,
        offset: 5,
        sha256: logweir_kafka::fingerprint::record_fingerprint(
            Some(b"k"),
            Some(b"v"),
            &headers,
            100,
        ),
    }];
    let consumed = vec![logweir_kafka::reader::ConsumedRecord {
        // The record's OWN offset on the target is 99 — wherever it
        // actually landed, unrelated to its true original offset (5).
        partition: 0,
        offset: 99,
        timestamp_ms: 100,
        key: Some(b"k".to_vec()),
        value: Some(b"v".to_vec()),
        headers,
    }];
    let (sampled, matching, mismatches) = compare(&archive, &consumed);
    assert_eq!((sampled, matching), (1, 1));
    assert!(mismatches.is_empty());
}

/// r6: a source config key ABSENT from the target's config map entirely
/// (not merely present-with-a-different-value) must still be reported as a
/// divergence — `target_cfg.get(k).map(|t| t != v).unwrap_or(true)`'s
/// `unwrap_or(true)` is the "absent ≠ agreement" rule. No prior test
/// presented a source key the target's map lacked outright, so a mutant
/// flipping that default to `false` (treating "I don't know" as "matches")
/// survived.
#[test]
fn a_source_config_key_absent_from_the_target_is_reported_not_silently_agreed() {
    let (intended, unexpected) = classify_parity(
        &fixtures::source_configs(&[("max.message.bytes", "1048576")]),
        &BTreeMap::new(),
        3,
        3,
        1,
        1,
    );
    assert!(intended.is_empty());
    assert_eq!(unexpected, vec!["max.message.bytes".to_string()]);
}

/// r8: the POSITIVE case for partition-count divergence. The brief's own
/// `scratch_deviations_are_intentional_and_anything_else_is_not` only ever
/// asserts the NEGATIVE (`!intended.contains("partition_count")` when the
/// counts are equal), so a mutant deleting the
/// `if src_partitions != tgt_partitions { intended.push("partition_count") }`
/// line survived undetected. Spec §9.3 phase 7(d) requires this reported.
#[test]
fn a_partition_count_divergence_is_reported_as_intended() {
    let (intended, unexpected) = classify_parity(&BTreeMap::new(), &BTreeMap::new(), 3, 6, 1, 1);
    assert!(intended.contains(&"partition_count".to_string()));
    assert!(unexpected.is_empty());
}

// ---------------------------------------------------------------------------
// Additions beyond the brief's six tests. None of the six above are touched.
//
// The brief's Step 1 tests exercise `compare`/`classify_parity` directly and
// the two fixture constructors — none of them ever call `phase7_verify::run`
// itself, so none of them would fail if `run`'s OWN wiring (the topic-rename
// mapping, the empty-selection/empty-mapping guards, the "known issue"
// engine-validation-run decoupling) were broken or deleted outright. That is
// exactly the "call site not pinned" failure mode Tasks 17/18's reviews found
// real defects through — these tests pin `run`'s call sites specifically.
use logweir::drill::phase7_verify::run;
use logweir::drill::DrillError;
use logweir_core::engine::{
    BackupSetFacts, BackupSetRef, DataEngine, EngineError, EngineId, EngineRun, PartitionFacts,
    PhaseObserver, PreflightReport, RecordFingerprint, RestoreFacts, RestorePlan, SampleSelection,
    SegmentFacts, StorageUrl, TopicFacts,
};
use logweir_kafka::reader::{ClusterReader, ConsumedRecord, KafkaError, TopicMeta};
use std::collections::BTreeMap;

const WINDOW: (i64, i64) = (0, 1_000_000);
/// The exact timestamp base `fixtures::matching_pair` bakes into every
/// record it builds (offset `i` gets `BASE_TS + i`) — duplicated here (not
/// exported by the fixture) only so `newest_restored_ts_ms` can be asserted
/// against an independently-known value rather than "whatever the fixture
/// happens to produce".
const MATCHING_PAIR_BASE_TS: i64 = 1_756_425_600_000;

fn facts_with_segment(sha256: &str) -> BackupSetFacts {
    BackupSetFacts {
        backup_id: "backup-verify-test".into(),
        created_at: fixtures::ts("2026-09-03T09:00:00Z"),
        source_cluster_id: Some("SRC0000000000000000000".into()),
        manifest_sha256: "sha256:0".into(),
        manifest_version_id: None,
        consumer_group_snapshot_sha256: None,
        topics: vec![TopicFacts {
            name: "orders".into(),
            original_partition_count: Some(1),
            source_replication_factor: Some(3),
            configurations: fixtures::source_configs(&[
                ("cleanup.policy", "compact"),
                ("retention.ms", "604800000"),
            ]),
            partitions: vec![PartitionFacts {
                partition_id: 0,
                segments: vec![SegmentFacts {
                    key: "logweir/seg.kbak".into(),
                    start_offset: 0,
                    end_offset: 49,
                    start_timestamp: WINDOW.0,
                    end_timestamp: WINDOW.1,
                    record_count: 50,
                    sha256: sha256.into(),
                    uploaded_at: WINDOW.1,
                }],
                gaps: vec![],
                pruned: vec![],
            }],
        }],
    }
}

fn sel_orders() -> Vec<SampleSelection> {
    vec![SampleSelection {
        set: BackupSetRef {
            backup_id: "backup-verify-test".into(),
            manifest_key: "backup-verify-test/manifest.json".into(),
        },
        topic: "orders".into(),
        partition: 0,
        anchor: "head".into(),
        count: 50,
        window: WINDOW,
    }]
}

fn plan_orders_to_drill_orders() -> RestorePlan {
    RestorePlan {
        set: BackupSetRef {
            backup_id: "backup-verify-test".into(),
            manifest_key: "backup-verify-test/manifest.json".into(),
        },
        storage: StorageUrl::Filesystem {
            path: "/tmp".into(),
        },
        target_bootstrap: vec!["broker:9092".into()],
        topic_mapping: fixtures::mapping("orders", "drill-orders"),
        time_window: (
            fixtures::ts("2026-08-29T00:00:00Z"),
            fixtures::ts("2026-08-30T02:00:00Z"),
        ),
        default_replication_factor: 1,
        checkpoint_state: "/tmp/logweir/checkpoint.json".into(),
        checkpoint_interval_secs: 30,
    }
}

#[derive(Default, Clone)]
struct TopicData {
    end_offsets: Vec<(i32, i64)>,
    configs: BTreeMap<String, String>,
    records: Vec<ConsumedRecord>,
}

/// A `ClusterReader` keyed strictly by topic NAME, refusing (never silently
/// substituting) any name it was not built with. This is what makes the
/// mapping-application tests below meaningful: if `phase7_verify` ever read
/// the archive-side (source) name instead of the mapped target-side name,
/// this double either errors loudly (`KafkaError::TopicNotFound`, when the
/// source name was never registered) or, where a test deliberately also
/// registers the source name as a trap, returns OBSERVABLY WRONG data
/// instead of merely erroring.
struct MapReader {
    topics: BTreeMap<String, TopicData>,
}

impl ClusterReader for MapReader {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        Ok("TARGET0000000000000000".into())
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        Ok(self
            .topics
            .iter()
            .map(|(n, d)| TopicMeta::new(n.clone(), d.end_offsets.len() as i32))
            .collect())
    }
    fn end_offsets(&self, topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        self.topics
            .get(topic)
            .map(|d| d.end_offsets.clone())
            .ok_or_else(|| KafkaError::TopicNotFound(topic.to_string()))
    }
    fn topic_configs(&self, topic: &str) -> Result<BTreeMap<String, String>, KafkaError> {
        self.topics
            .get(topic)
            .map(|d| d.configs.clone())
            .ok_or_else(|| KafkaError::TopicNotFound(topic.to_string()))
    }
    fn consume_range(
        &self,
        topic: &str,
        partition: i32,
        from: i64,
        max: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        let d = self
            .topics
            .get(topic)
            .ok_or_else(|| KafkaError::TopicNotFound(topic.to_string()))?;
        Ok(d.records
            .iter()
            .filter(|r| r.partition == partition && r.offset >= from)
            .take(max)
            .cloned()
            .collect())
    }
}

enum ValidationBehavior {
    Success(i32),
    Failing(String),
}

struct VerifyEngine {
    facts: BackupSetFacts,
    fingerprints: Vec<RecordFingerprint>,
    unsupported: Option<String>,
    validation: ValidationBehavior,
}

impl DataEngine for VerifyEngine {
    fn id(&self) -> EngineId {
        EngineId {
            id: "fake".into(),
            version: "v0.0.0".into(),
            digest: format!("sha256:{}", "0".repeat(64)),
        }
    }
    fn list_backup_sets(&self, _: &StorageUrl) -> Result<Vec<BackupSetRef>, EngineError> {
        Ok(vec![])
    }
    fn describe(&self, _: &BackupSetRef) -> Result<BackupSetFacts, EngineError> {
        Ok(self.facts.clone())
    }
    fn preflight(&self, _: &RestorePlan) -> Result<PreflightReport, EngineError> {
        Err(EngineError::Operational("not used by this fixture".into()))
    }
    fn restore(
        &self,
        _: &RestorePlan,
        _: &mut dyn PhaseObserver,
    ) -> Result<RestoreFacts, EngineError> {
        Err(EngineError::Operational("not used by this fixture".into()))
    }
    fn fingerprints(&self, _: &SampleSelection) -> Result<Vec<RecordFingerprint>, EngineError> {
        if let Some(reason) = &self.unsupported {
            return Err(EngineError::Unsupported(reason.clone()));
        }
        Ok(self.fingerprints.clone())
    }
    fn validation_run(&self, _: &RestorePlan) -> Result<EngineRun, EngineError> {
        match &self.validation {
            ValidationBehavior::Success(code) => Ok(EngineRun { exit_code: *code }),
            ValidationBehavior::Failing(msg) => Err(EngineError::Operational(msg.clone())),
        }
    }
}

/// `logweir-core::ids::sha256_prefixed` over deterministic bytes, plus a
/// `Store` those bytes are actually put into at the key `facts_with_segment`
/// names. Every full-`run` test below needs this pair, so it is built once.
fn store_with_matching_segment() -> (logweir_engine_oso::storage::Store, String) {
    let bytes = b"segment payload for verify phase test";
    let sha = logweir_core::ids::sha256_prefixed(bytes);
    let store = logweir_engine_oso::storage::Store::in_memory("logweir");
    store.put_create_only("logweir/seg.kbak", bytes).unwrap();
    (store, sha)
}

/// A full, healthy `run` — the baseline every other test below is a
/// deliberate variation of.
#[test]
fn a_healthy_drill_reconciles_to_integrity_pass_and_reports_intended_parity_only() {
    let (store, sha) = store_with_matching_segment();
    let facts = facts_with_segment(&sha);
    let (archive, cons) = fixtures::matching_pair(50);

    let mut topics = BTreeMap::new();
    topics.insert(
        "drill-orders".to_string(),
        TopicData {
            end_offsets: vec![(0, 50)],
            configs: fixtures::target_configs(&[
                ("cleanup.policy", "delete"),
                ("retention.ms", "-1"),
            ]),
            records: cons,
        },
    );
    let reader = MapReader { topics };
    let engine = VerifyEngine {
        facts: facts.clone(),
        fingerprints: archive,
        unsupported: None,
        validation: ValidationBehavior::Success(0),
    };
    let mapping = fixtures::mapping("orders", "drill-orders");
    let plan = plan_orders_to_drill_orders();

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel_orders(),
        &mapping,
        &plan,
    )
    .unwrap();

    assert_eq!(out.integrity.result, IntegrityResult::Pass);
    assert_eq!(out.integrity.level, IntegrityLevel::ByteFingerprint);
    assert_eq!(out.integrity.records_sampled, 50);
    assert_eq!(out.integrity.records_sampled_matching, 50);
    assert_eq!(out.integrity.mismatches, 0);
    assert_eq!(out.pass_rate(), Some(1.0));
    assert_eq!(out.records_restored, 50);
    assert_eq!(out.newest_restored_ts_ms, MATCHING_PAIR_BASE_TS + 49);
    assert_eq!(out.topic_parity.unexpected_divergence, Vec::<String>::new());
    assert!(out
        .topic_parity
        .intentionally_deviated
        .iter()
        .any(|s| s.contains("cleanup.policy")));
    assert!(out
        .topic_parity
        .intentionally_deviated
        .iter()
        .any(|s| s.contains("retention.ms")));
    assert!(
        out.topic_parity
            .intentionally_deviated
            .iter()
            .any(|s| s.contains("replication_factor")),
        "the source's rf=3 vs the plan's default_replication_factor=1 is an INTENDED \
         divergence phase 6 creates on purpose (spec §9.3 phase 7(d))"
    );
}

/// REGRESSION (Task 21c, found against MinIO). A real 0.21.0 manifest stores
/// each segment's digest as BARE HEX (`"sha256": "27f6c448…"`), while
/// `logweir_core::ids::sha256_prefixed` produces Logweir's own `sha256:<hex>`
/// form. `segment_evidence` compared the two directly, so every segment of
/// every real archive reported "sha256 mismatch against the manifest" and the
/// segment lane could never return `Verified` outside these tests — which
/// construct `SegmentFacts` by hand and happen to write the prefixed form.
///
/// This is the same scenario as
/// `a_healthy_drill_reconciles_to_integrity_pass_and_reports_intended_parity_only`
/// with ONE difference: the manifest's digest is spelled the way the engine
/// actually spells it.
#[test]
fn a_manifest_sha256_in_the_engines_bare_hex_form_still_verifies() {
    let bytes = b"segment payload for verify phase test";
    let bare_hex = logweir_core::ids::sha256_hex(bytes);
    assert!(
        !bare_hex.starts_with("sha256:"),
        "this test is only meaningful if the value really is unprefixed"
    );
    let store = logweir_engine_oso::storage::Store::in_memory("logweir");
    store.put_create_only("logweir/seg.kbak", bytes).unwrap();
    let facts = facts_with_segment(&bare_hex);
    let (archive, cons) = fixtures::matching_pair(50);

    let mut topics = BTreeMap::new();
    topics.insert(
        "drill-orders".to_string(),
        TopicData {
            end_offsets: vec![(0, 50)],
            configs: fixtures::target_configs(&[
                ("cleanup.policy", "delete"),
                ("retention.ms", "-1"),
            ]),
            records: cons,
        },
    );
    let reader = MapReader { topics };
    let engine = VerifyEngine {
        facts: facts.clone(),
        fingerprints: archive,
        unsupported: None,
        validation: ValidationBehavior::Success(0),
    };
    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel_orders(),
        &fixtures::mapping("orders", "drill-orders"),
        &plan_orders_to_drill_orders(),
    )
    .unwrap();

    assert_eq!(
        out.integrity.result,
        IntegrityResult::Pass,
        "partial_reason: {:?}",
        out.integrity.partial_reason
    );
    assert_eq!(out.integrity.level, IntegrityLevel::ByteFingerprint);
    assert_eq!(out.integrity.mismatches, 0);
}

/// Pins the sha256 check's OWN contribution to `integrity.result`, distinct
/// from the canary comparison: the canary side matches perfectly here, so a
/// `Fail` can only have come from the sha256 mismatch. Guards against a
/// mutant that swaps or drops the sha256 `result = Fail` assignment.
#[test]
fn a_sha256_mismatch_against_the_manifest_fails_integrity_even_when_the_canary_matches() {
    let (store, _real_sha) = store_with_matching_segment();
    let wrong_sha = format!("sha256:{}", "0".repeat(64));
    let facts = facts_with_segment(&wrong_sha);
    let (archive, cons) = fixtures::matching_pair(50);

    let mut topics = BTreeMap::new();
    topics.insert(
        "drill-orders".to_string(),
        TopicData {
            end_offsets: vec![(0, 50)],
            configs: fixtures::target_configs(&[
                ("cleanup.policy", "delete"),
                ("retention.ms", "-1"),
            ]),
            records: cons,
        },
    );
    let reader = MapReader { topics };
    let engine = VerifyEngine {
        facts: facts.clone(),
        fingerprints: archive,
        unsupported: None,
        validation: ValidationBehavior::Success(0),
    };
    let mapping = fixtures::mapping("orders", "drill-orders");
    let plan = plan_orders_to_drill_orders();

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel_orders(),
        &mapping,
        &plan,
    )
    .unwrap();

    assert_eq!(out.integrity.result, IntegrityResult::Fail);
    // The canary side still shows a full match — proving the FAIL is
    // attributable to the sha256 check, not to a canary mismatch.
    assert_eq!(
        out.integrity.records_sampled_matching,
        out.integrity.records_sampled
    );
    assert_eq!(out.integrity.mismatches, 0);
}

/// The canary comparison's own contribution, counted precisely (Step 1's
/// `a_single_changed_value_is_a_mismatch_not_a_count_difference`, but pinned
/// at `run`'s actual call site rather than only at `compare` in isolation).
#[test]
fn a_canary_fingerprint_mismatch_fails_integrity_and_is_counted_precisely() {
    let (store, sha) = store_with_matching_segment();
    let facts = facts_with_segment(&sha);
    let (archive, mut cons) = fixtures::matching_pair(50);
    cons[7].value = Some(b"tampered".to_vec());

    let mut topics = BTreeMap::new();
    topics.insert(
        "drill-orders".to_string(),
        TopicData {
            end_offsets: vec![(0, 50)],
            configs: fixtures::target_configs(&[
                ("cleanup.policy", "delete"),
                ("retention.ms", "-1"),
            ]),
            records: cons,
        },
    );
    let reader = MapReader { topics };
    let engine = VerifyEngine {
        facts: facts.clone(),
        fingerprints: archive,
        unsupported: None,
        validation: ValidationBehavior::Success(0),
    };
    let mapping = fixtures::mapping("orders", "drill-orders");
    let plan = plan_orders_to_drill_orders();

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel_orders(),
        &mapping,
        &plan,
    )
    .unwrap();

    assert_eq!(out.integrity.result, IntegrityResult::Fail);
    assert_eq!(out.integrity.records_sampled, 50);
    assert_eq!(out.integrity.records_sampled_matching, 49);
    assert_eq!(out.integrity.mismatches, 1);
    assert_eq!(out.pass_rate(), Some(49.0 / 50.0));
}

/// An unsupported KBAK level, driven through the FULL `run` (not just the
/// fixture constructor Step 1 already tests): the drill still consumes and
/// reports `records_restored`, but samples nothing and the pass rate stays
/// null. Also proves `verdict_for_selection` still consumes (and still
/// applies the mapping) on the consume-only branch.
///
/// Task 19 fix round 3: `partial_reason` is now asserted by CONTAINMENT, not
/// equality. Round 2 emitted the engine's bare reason ("kbak level below the
/// gate") as the whole string; round 3 prefixes every reason with the
/// selection it belongs to, because a whole-backup-set reason is exactly what
/// let one selection's downgrade speak for every other one (the fifth
/// reproduction). The engine's own words are still carried through verbatim —
/// that part of the premise is unchanged and is still asserted.
///
/// The result assertion is NEW and is the fourth door's headline: this drill
/// reaches `Pass` only because the target actually gave back at least the 50
/// records the manifest claims for this selection. Consume-only is a weaker
/// CLAIM (`level` says so, and `pass_rate` stays null), never a weaker CHECK.
#[test]
fn an_unsupported_engine_degrades_to_consume_only_through_the_full_run() {
    let (store, sha) = store_with_matching_segment();
    let facts = facts_with_segment(&sha);
    let (_, cons) = fixtures::matching_pair(50);

    let mut topics = BTreeMap::new();
    topics.insert(
        "drill-orders".to_string(),
        TopicData {
            end_offsets: vec![(0, 50)],
            configs: fixtures::target_configs(&[
                ("cleanup.policy", "delete"),
                ("retention.ms", "-1"),
            ]),
            records: cons,
        },
    );
    let reader = MapReader { topics };
    let engine = VerifyEngine {
        facts: facts.clone(),
        fingerprints: vec![],
        unsupported: Some("kbak level below the gate".into()),
        validation: ValidationBehavior::Success(0),
    };
    let mapping = fixtures::mapping("orders", "drill-orders");
    let plan = plan_orders_to_drill_orders();

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel_orders(),
        &mapping,
        &plan,
    )
    .unwrap();

    assert_eq!(out.integrity.level, IntegrityLevel::ConsumeOnly);
    assert_eq!(out.integrity.records_sampled, 0);
    assert_eq!(out.integrity.records_sampled_matching, 0);
    assert_eq!(out.integrity.mismatches, 0);
    assert_eq!(out.pass_rate(), None);
    assert_eq!(out.records_restored, 50);
    assert_eq!(
        out.integrity.result,
        IntegrityResult::Pass,
        "the target gave back all 50 records the manifest claims, which is the whole of the \
         consume-only claim — got {:?}",
        out.integrity
    );
    let reason = out
        .integrity
        .partial_reason
        .as_deref()
        .expect("the downgrade must be named, even on a pass");
    assert!(
        reason.contains("kbak level below the gate"),
        "the engine's own reason must reach the signed document verbatim: {reason:?}"
    );
    assert!(
        reason.contains("orders/0"),
        "the downgrade must name the selection it applies to, never the whole backup set: \
         {reason:?}"
    );
}

/// `run` refuses a zero-selection request outright — the top-level guard
/// against the empty-comparison false pass, pinned here rather than only via
/// `fingerprints_for`'s own unit test.
#[test]
fn run_rejects_a_verify_request_with_zero_sample_selections() {
    let (store, sha) = store_with_matching_segment();
    let facts = facts_with_segment(&sha);
    let engine = VerifyEngine {
        facts: facts.clone(),
        fingerprints: vec![],
        unsupported: None,
        validation: ValidationBehavior::Success(0),
    };
    let reader = MapReader {
        topics: BTreeMap::new(),
    };
    let mapping = fixtures::mapping("orders", "drill-orders");
    let plan = plan_orders_to_drill_orders();

    let err = run(&engine, &reader, &store, &facts, &[], &mapping, &plan).unwrap_err();
    assert!(matches!(err, DrillError::Operational(_)));
    assert!(err.to_string().contains("zero sample selections"));
}

/// THE topic-rename mapping test for `verdict_for_selection`. `MapReader`
/// registers the ARCHIVE-side name "orders" as present but EMPTY, and the
/// mapped TARGET-side name "drill-orders" with the real, matching records.
/// If `verdict_for_selection` ever
/// read `sel.topic` ("orders") directly instead of `mapping[&sel.topic]`
/// ("drill-orders"), the canary would see 50 archive fingerprints against 0
/// consumed records — every one reported "absent from the target" — and this
/// assertion would fail. This is where the mapping is applied for the canary
/// comparison: `crates/logweir/src/drill/phase7_verify.rs`'s
/// `verdict_for_selection`, via its `mapped_topic` call.
#[test]
fn verdict_for_selection_reads_the_mapped_target_topic_never_the_archive_name() {
    let (store, sha) = store_with_matching_segment();
    let facts = facts_with_segment(&sha);
    let (archive, cons) = fixtures::matching_pair(50);

    let mut topics = BTreeMap::new();
    topics.insert(
        "orders".to_string(),
        TopicData {
            end_offsets: vec![(0, 0)],
            configs: BTreeMap::new(),
            records: vec![],
        },
    );
    topics.insert(
        "drill-orders".to_string(),
        TopicData {
            end_offsets: vec![(0, 50)],
            configs: fixtures::target_configs(&[
                ("cleanup.policy", "delete"),
                ("retention.ms", "-1"),
            ]),
            records: cons,
        },
    );
    let reader = MapReader { topics };
    let engine = VerifyEngine {
        facts: facts.clone(),
        fingerprints: archive,
        unsupported: None,
        validation: ValidationBehavior::Success(0),
    };
    let mapping = fixtures::mapping("orders", "drill-orders");
    let plan = plan_orders_to_drill_orders();

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel_orders(),
        &mapping,
        &plan,
    )
    .unwrap();

    assert_eq!(out.integrity.result, IntegrityResult::Pass);
    assert_eq!(out.integrity.records_sampled_matching, 50);
}

/// THE topic-rename mapping test for `classify_parity_all`. The archive-side
/// name "orders" is registered as a TRAP: its configs mirror the SOURCE
/// configuration exactly, so if `classify_parity_all` ever compared against
/// it instead of the mapped "drill-orders", `cleanup.policy`/`retention.ms`
/// would show as EQUAL (no divergence at all) rather than the real, intended
/// divergence the actual target carries. This is where the mapping is
/// applied for topic parity: `crates/logweir/src/drill/phase7_verify.rs`'s
/// `classify_parity_all`.
#[test]
fn classify_parity_all_reads_the_mapped_target_topic_never_the_archive_name() {
    let (store, sha) = store_with_matching_segment();
    let facts = facts_with_segment(&sha);
    let (archive, cons) = fixtures::matching_pair(50);

    let mut topics = BTreeMap::new();
    topics.insert(
        "orders".to_string(),
        TopicData {
            end_offsets: vec![(0, 50)],
            configs: fixtures::source_configs(&[
                ("cleanup.policy", "compact"),
                ("retention.ms", "604800000"),
            ]),
            records: cons.clone(),
        },
    );
    topics.insert(
        "drill-orders".to_string(),
        TopicData {
            end_offsets: vec![(0, 50)],
            configs: fixtures::target_configs(&[
                ("cleanup.policy", "delete"),
                ("retention.ms", "-1"),
            ]),
            records: cons,
        },
    );
    let reader = MapReader { topics };
    let engine = VerifyEngine {
        facts: facts.clone(),
        fingerprints: archive,
        unsupported: None,
        validation: ValidationBehavior::Success(0),
    };
    let mapping = fixtures::mapping("orders", "drill-orders");
    let plan = plan_orders_to_drill_orders();

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel_orders(),
        &mapping,
        &plan,
    )
    .unwrap();

    assert!(
        out.topic_parity
            .intentionally_deviated
            .iter()
            .any(|s| s.contains("cleanup.policy")),
        "got {:?} — classify_parity_all must have compared against the MAPPED target \
         (`drill-orders`), not the archive-side name (`orders`)",
        out.topic_parity
    );
    assert!(out
        .topic_parity
        .intentionally_deviated
        .iter()
        .any(|s| s.contains("retention.ms")));
}

/// The "known issue" resolution, end to end: `kafka-backup`'s own `validation
/// run` compares against unmapped SOURCE topic names and is therefore
/// EXPECTED to report a discrepancy (here, simulated as an outright `Err`)
/// on every renamed drill, healthy or not. This must never fail or abort
/// `run` — only Logweir's own canary comparison may set `integrity.result`.
#[test]
fn a_failing_engine_validation_run_never_fails_or_aborts_the_drill() {
    let (store, sha) = store_with_matching_segment();
    let facts = facts_with_segment(&sha);
    let (archive, cons) = fixtures::matching_pair(50);

    let mut topics = BTreeMap::new();
    topics.insert(
        "drill-orders".to_string(),
        TopicData {
            end_offsets: vec![(0, 50)],
            configs: fixtures::target_configs(&[
                ("cleanup.policy", "delete"),
                ("retention.ms", "-1"),
            ]),
            records: cons,
        },
    );
    let reader = MapReader { topics };
    let engine = VerifyEngine {
        facts: facts.clone(),
        fingerprints: archive,
        unsupported: None,
        validation: ValidationBehavior::Failing(
            "message_count mismatch: expected topic `orders`, target has no such topic".into(),
        ),
    };
    let mapping = fixtures::mapping("orders", "drill-orders");
    let plan = plan_orders_to_drill_orders();

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel_orders(),
        &mapping,
        &plan,
    )
    .expect("an engine-side validation-run failure must never abort phase 7");

    assert_eq!(out.integrity.result, IntegrityResult::Pass);
    assert_eq!(out.integrity.records_sampled_matching, 50);
}

/// A `DataEngine` whose `fingerprints` answers PER-TOPIC, unlike `VerifyEngine`
/// above (which returns one constant list regardless of the selection —
/// adequate for every single-topic test above, but unable to distinguish
/// "two selections, each contributing its own fingerprints" from "one
/// selection's fingerprints returned twice"). Local to this one test.
struct MultiTopicEngine {
    facts: BackupSetFacts,
    by_topic: BTreeMap<String, Vec<RecordFingerprint>>,
}

impl DataEngine for MultiTopicEngine {
    fn id(&self) -> EngineId {
        EngineId {
            id: "fake".into(),
            version: "v0.0.0".into(),
            digest: format!("sha256:{}", "0".repeat(64)),
        }
    }
    fn list_backup_sets(&self, _: &StorageUrl) -> Result<Vec<BackupSetRef>, EngineError> {
        Ok(vec![])
    }
    fn describe(&self, _: &BackupSetRef) -> Result<BackupSetFacts, EngineError> {
        Ok(self.facts.clone())
    }
    fn preflight(&self, _: &RestorePlan) -> Result<PreflightReport, EngineError> {
        Err(EngineError::Operational("not used by this fixture".into()))
    }
    fn restore(
        &self,
        _: &RestorePlan,
        _: &mut dyn PhaseObserver,
    ) -> Result<RestoreFacts, EngineError> {
        Err(EngineError::Operational("not used by this fixture".into()))
    }
    fn fingerprints(&self, s: &SampleSelection) -> Result<Vec<RecordFingerprint>, EngineError> {
        Ok(self.by_topic.get(&s.topic).cloned().unwrap_or_default())
    }
    fn validation_run(&self, _: &RestorePlan) -> Result<EngineRun, EngineError> {
        Ok(EngineRun { exit_code: 0 })
    }
}

/// Like `fixtures::matching_pair`, but the key/value bytes are distinguished
/// by `tag` — Task 19 fix round 1 (review finding F1). `fixtures::matching_pair`
/// always builds byte-IDENTICAL records (`k{i}`/`v{i}`) regardless of which
/// topic it is called for — the record's fingerprint does not fold in a
/// topic name at all. Reusing it for TWO topics in the same test made the
/// original pooled-`compare()` bug (offsets colliding across topics)
/// invisible: a record from one topic "matched" the other topic's archive
/// fingerprint just fine, because the bytes were identical either way.
/// Distinct content per topic makes a cross-topic collision observable — a
/// record built with `tag = "payments"` can never fingerprint-match an
/// archive entry built with `tag = "orders"`.
fn distinct_matching_pair(tag: &str, n: usize) -> (Vec<RecordFingerprint>, Vec<ConsumedRecord>) {
    let mut arch = Vec::with_capacity(n);
    let mut cons = Vec::with_capacity(n);
    for i in 0..n {
        let off = i as i64;
        let headers = vec![(
            "x-original-offset".to_string(),
            Some(off.to_string().into_bytes()),
        )];
        let key = format!("{tag}-k{i}").into_bytes();
        let value = format!("{tag}-v{i}").into_bytes();
        let tsms = 1_756_425_600_000i64 + off;
        arch.push(RecordFingerprint {
            topic: tag.into(),
            partition: 0,
            offset: off,
            sha256: logweir_kafka::fingerprint::record_fingerprint(
                Some(&key),
                Some(&value),
                &headers,
                tsms,
            ),
        });
        cons.push(ConsumedRecord {
            partition: 0,
            offset: off,
            timestamp_ms: tsms,
            key: Some(key),
            value: Some(value),
            headers,
        });
    }
    (arch, cons)
}

/// Proves `probe_archive_modes`, `verdict_for_selection` and
/// `classify_parity_all` all aggregate ACROSS every entry in
/// `sel`/`mapping` rather than only the first — a mutant that stopped after
/// the first selection, or that folded `classify_parity` over only one
/// mapped topic, would still pass every single-topic test above (every one
/// of them names exactly one topic) but must fail this one.
#[test]
fn run_aggregates_across_every_selection_and_mapped_topic_not_just_the_first() {
    let bytes_orders = b"segment payload for verify phase test - orders";
    let bytes_payments = b"segment payload for verify phase test - payments";
    let sha_orders = logweir_core::ids::sha256_prefixed(bytes_orders);
    let sha_payments = logweir_core::ids::sha256_prefixed(bytes_payments);
    let store = logweir_engine_oso::storage::Store::in_memory("logweir");
    store
        .put_create_only("logweir/seg-orders.kbak", bytes_orders)
        .unwrap();
    store
        .put_create_only("logweir/seg-payments.kbak", bytes_payments)
        .unwrap();

    let facts = BackupSetFacts {
        backup_id: "backup-verify-test".into(),
        created_at: fixtures::ts("2026-09-03T09:00:00Z"),
        source_cluster_id: Some("SRC0000000000000000000".into()),
        manifest_sha256: "sha256:0".into(),
        manifest_version_id: None,
        consumer_group_snapshot_sha256: None,
        topics: vec![
            TopicFacts {
                name: "orders".into(),
                original_partition_count: Some(1),
                source_replication_factor: Some(3),
                configurations: fixtures::source_configs(&[("cleanup.policy", "compact")]),
                partitions: vec![PartitionFacts {
                    partition_id: 0,
                    segments: vec![SegmentFacts {
                        key: "logweir/seg-orders.kbak".into(),
                        start_offset: 0,
                        end_offset: 24,
                        start_timestamp: WINDOW.0,
                        end_timestamp: WINDOW.1,
                        record_count: 25,
                        sha256: sha_orders,
                        uploaded_at: WINDOW.1,
                    }],
                    gaps: vec![],
                    pruned: vec![],
                }],
            },
            TopicFacts {
                name: "payments".into(),
                original_partition_count: Some(1),
                source_replication_factor: Some(3),
                configurations: fixtures::source_configs(&[("cleanup.policy", "compact")]),
                partitions: vec![PartitionFacts {
                    partition_id: 0,
                    segments: vec![SegmentFacts {
                        key: "logweir/seg-payments.kbak".into(),
                        start_offset: 0,
                        end_offset: 24,
                        start_timestamp: WINDOW.0,
                        end_timestamp: WINDOW.1,
                        record_count: 25,
                        sha256: sha_payments,
                        uploaded_at: WINDOW.1,
                    }],
                    gaps: vec![],
                    pruned: vec![],
                }],
            },
        ],
    };

    // Task 19 fix round 1 (review finding F1): DISTINCT content per topic,
    // not `fixtures::matching_pair` reused for both — see
    // `distinct_matching_pair`'s own doc comment for why identical content
    // made the original pooled-`compare()` bug invisible.
    let (archive_o, cons_o) = distinct_matching_pair("orders", 25);
    let (archive_p, cons_p) = distinct_matching_pair("payments", 25);

    let mut by_topic = BTreeMap::new();
    by_topic.insert("orders".to_string(), archive_o);
    by_topic.insert("payments".to_string(), archive_p);
    let engine = MultiTopicEngine {
        facts: facts.clone(),
        by_topic,
    };

    let mut topics = BTreeMap::new();
    topics.insert(
        "drill-orders".to_string(),
        TopicData {
            end_offsets: vec![(0, 25)],
            configs: fixtures::target_configs(&[("cleanup.policy", "delete")]),
            records: cons_o,
        },
    );
    topics.insert(
        "drill-payments".to_string(),
        TopicData {
            end_offsets: vec![(0, 25)],
            configs: fixtures::target_configs(&[("cleanup.policy", "delete")]),
            records: cons_p,
        },
    );
    let reader = MapReader { topics };

    let sel = vec![
        SampleSelection {
            set: BackupSetRef {
                backup_id: "backup-verify-test".into(),
                manifest_key: "backup-verify-test/manifest.json".into(),
            },
            topic: "orders".into(),
            partition: 0,
            anchor: "head".into(),
            count: 25,
            window: WINDOW,
        },
        SampleSelection {
            set: BackupSetRef {
                backup_id: "backup-verify-test".into(),
                manifest_key: "backup-verify-test/manifest.json".into(),
            },
            topic: "payments".into(),
            partition: 0,
            anchor: "head".into(),
            count: 25,
            window: WINDOW,
        },
    ];
    let mut mapping = BTreeMap::new();
    mapping.insert("orders".to_string(), "drill-orders".to_string());
    mapping.insert("payments".to_string(), "drill-payments".to_string());
    let mut plan = plan_orders_to_drill_orders();
    plan.topic_mapping = mapping.clone();

    let out = run(&engine, &reader, &store, &facts, &sel, &mapping, &plan).unwrap();

    assert_eq!(out.integrity.result, IntegrityResult::Pass);
    // 25 + 25: wrong (25) if either `probe_archive_modes` or `run`'s own
    // per-selection verdict loop ever stopped after the first selection.
    assert_eq!(out.integrity.records_sampled, 50);
    assert_eq!(out.integrity.records_sampled_matching, 50);
    assert_eq!(out.records_restored, 50);
    // One entry per mapped topic: wrong (missing one) if `classify_parity_all`
    // ever folded over only the first mapping entry.
    let deviated = &out.topic_parity.intentionally_deviated;
    assert!(deviated.iter().any(|s| s.starts_with("drill-orders:")));
    assert!(deviated.iter().any(|s| s.starts_with("drill-payments:")));
}

/// THE critical fix for review finding F1, proven directly against the exact
/// shape the reviewer demonstrated: two mapped topics, one of them restored
/// to ZERO records. Before the fix, `compare()` was called ONCE over both
/// topics' fingerprints and both topics' consumed records pooled into one
/// flat pair — since `ConsumedRecord` carries no topic field and every
/// partition starts at offset 0, `payments`'s healthy records satisfied
/// `orders`'s archive lookups by coincidence of matching offsets, and the
/// drill signed `integrity: pass` for a topic that was never restored at
/// all. Reconciling per selection (this fix) makes that impossible: `orders`
/// has nothing to match against on the target, so every one of its archive
/// fingerprints is "present in the archive, absent from the target" — a
/// real, counted mismatch — regardless of what `payments` looks like.
/// Task 19 fix round 2 (review finding FIX 2b): this test previously used
/// `distinct_matching_pair` for both topics, which the reviewer showed makes
/// it pass IDENTICALLY whether `compare()` is called per-selection (fixed)
/// or once over everything pooled (the original bug reintroduced) — with
/// distinct content, pooling can only ever turn a would-be match into a
/// "fingerprint mismatch", never into a false MATCH, so the asserted numbers
/// (50/25/25/0.5) come out the same under both implementations and the test
/// could not actually distinguish them.
///
/// Fixed by using COLLIDING content instead — `fixtures::matching_pair`
/// called for BOTH topics, which builds byte-IDENTICAL records (`k{i}`/`v{i}`,
/// no topic name folded into the fingerprint) — exactly the shape the
/// reviewer's original demonstration used. Under the FIXED (per-selection)
/// implementation this still correctly reports `Fail` (orders' own archive
/// has nothing to match against on the target). Under the ORIGINAL pooled
/// bug, `payments`'s real consumed records would satisfy `orders`'s archive
/// lookups too (same content, same offsets), and the drill would report a
/// full `Pass` — 50 matched — for a topic that restored ZERO records. This
/// is what "the test that would fail if this regressed" now means literally:
/// re-pooling `compare()`'s inputs (reverting to one flat call instead of
/// one call per selection) turns this test's expected `Fail`/`0.5` into
/// `Pass`/`1.0`, and IS verified to do so as part of this round's mutation
/// pass (see the report's mutant table, mutant p1).
#[test]
fn a_topic_restored_to_zero_records_must_fail_not_pass_even_when_pooled_with_a_healthy_topic() {
    let bytes_orders = b"segment payload for verify phase test - orders (unrestored)";
    let bytes_payments = b"segment payload for verify phase test - payments (healthy)";
    let sha_orders = logweir_core::ids::sha256_prefixed(bytes_orders);
    let sha_payments = logweir_core::ids::sha256_prefixed(bytes_payments);
    let store = logweir_engine_oso::storage::Store::in_memory("logweir");
    store
        .put_create_only("logweir/seg-orders-unrestored.kbak", bytes_orders)
        .unwrap();
    store
        .put_create_only("logweir/seg-payments-healthy.kbak", bytes_payments)
        .unwrap();

    let facts = BackupSetFacts {
        backup_id: "backup-verify-test".into(),
        created_at: fixtures::ts("2026-09-03T09:00:00Z"),
        source_cluster_id: Some("SRC0000000000000000000".into()),
        manifest_sha256: "sha256:0".into(),
        manifest_version_id: None,
        consumer_group_snapshot_sha256: None,
        topics: vec![
            TopicFacts {
                name: "orders".into(),
                original_partition_count: Some(1),
                source_replication_factor: Some(3),
                configurations: fixtures::source_configs(&[("cleanup.policy", "compact")]),
                partitions: vec![PartitionFacts {
                    partition_id: 0,
                    segments: vec![SegmentFacts {
                        key: "logweir/seg-orders-unrestored.kbak".into(),
                        start_offset: 0,
                        end_offset: 24,
                        start_timestamp: WINDOW.0,
                        end_timestamp: WINDOW.1,
                        record_count: 25,
                        sha256: sha_orders,
                        uploaded_at: WINDOW.1,
                    }],
                    gaps: vec![],
                    pruned: vec![],
                }],
            },
            TopicFacts {
                name: "payments".into(),
                original_partition_count: Some(1),
                source_replication_factor: Some(3),
                configurations: fixtures::source_configs(&[("cleanup.policy", "compact")]),
                partitions: vec![PartitionFacts {
                    partition_id: 0,
                    segments: vec![SegmentFacts {
                        key: "logweir/seg-payments-healthy.kbak".into(),
                        start_offset: 0,
                        end_offset: 24,
                        start_timestamp: WINDOW.0,
                        end_timestamp: WINDOW.1,
                        record_count: 25,
                        sha256: sha_payments,
                        uploaded_at: WINDOW.1,
                    }],
                    gaps: vec![],
                    pruned: vec![],
                }],
            },
        ],
    };

    // COLLIDING content, deliberately: `fixtures::matching_pair` builds the
    // SAME `k{i}`/`v{i}` bytes regardless of which topic calls it, so
    // `archive_o` and `archive_p` carry byte-IDENTICAL fingerprints at each
    // offset. That is exactly what makes this test discriminate a
    // pooled-vs-per-selection implementation (see the doc comment above) —
    // `distinct_matching_pair` (used by `run_aggregates_...`, whose own
    // purpose is aggregation-completeness, not collision-sensitivity) would
    // not.
    let (archive_o, _cons_o_unused) = fixtures::matching_pair(25);
    let (archive_p, cons_p) = fixtures::matching_pair(25);

    let mut by_topic = BTreeMap::new();
    by_topic.insert("orders".to_string(), archive_o);
    by_topic.insert("payments".to_string(), archive_p);
    let engine = MultiTopicEngine {
        facts: facts.clone(),
        by_topic,
    };

    let mut topics = BTreeMap::new();
    // `drill-orders` was never restored: present, but empty.
    topics.insert(
        "drill-orders".to_string(),
        TopicData {
            end_offsets: vec![(0, 0)],
            configs: fixtures::target_configs(&[("cleanup.policy", "delete")]),
            records: vec![],
        },
    );
    topics.insert(
        "drill-payments".to_string(),
        TopicData {
            end_offsets: vec![(0, 25)],
            configs: fixtures::target_configs(&[("cleanup.policy", "delete")]),
            records: cons_p,
        },
    );
    let reader = MapReader { topics };

    let sel = vec![
        SampleSelection {
            set: BackupSetRef {
                backup_id: "backup-verify-test".into(),
                manifest_key: "backup-verify-test/manifest.json".into(),
            },
            topic: "orders".into(),
            partition: 0,
            anchor: "head".into(),
            count: 25,
            window: WINDOW,
        },
        SampleSelection {
            set: BackupSetRef {
                backup_id: "backup-verify-test".into(),
                manifest_key: "backup-verify-test/manifest.json".into(),
            },
            topic: "payments".into(),
            partition: 0,
            anchor: "head".into(),
            count: 25,
            window: WINDOW,
        },
    ];
    let mut mapping = BTreeMap::new();
    mapping.insert("orders".to_string(), "drill-orders".to_string());
    mapping.insert("payments".to_string(), "drill-payments".to_string());
    let mut plan = plan_orders_to_drill_orders();
    plan.topic_mapping = mapping.clone();

    let out = run(&engine, &reader, &store, &facts, &sel, &mapping, &plan).unwrap();

    assert_eq!(
        out.integrity.result,
        IntegrityResult::Fail,
        "an entirely un-restored topic pooled with a healthy one must never read as a pass"
    );
    // orders: 25 sampled, 0 matching (every one absent from the target).
    // payments: 25 sampled, 25 matching. Summed: 50 sampled, 25 matching.
    assert_eq!(out.integrity.records_sampled, 50);
    assert_eq!(out.integrity.records_sampled_matching, 25);
    assert_eq!(out.integrity.mismatches, 25);
    assert_eq!(out.pass_rate(), Some(0.5));
    // Only `payments` actually produced consumed records.
    assert_eq!(out.records_restored, 25);
}

/// Task 19 fix round 2, FIX 2 (the SAME critical finding surviving fix
/// round 1 through a narrower door): fix round 1's guard fired only on the
/// AGGREGATE `sampled == 0`. Here `orders`'s own `engine.fingerprints`
/// returns `Ok(vec![])` — no error, no `Unsupported` — for a topic restored
/// to zero, POOLED with a healthy `payments` selection whose archive and
/// consumed records genuinely match. Under fix round 1's aggregate-only
/// guard this reported `Pass, byte-fingerprint, 25/25, pass_rate 1.0,
/// partial_reason None` — `orders` was never compared against anything, but
/// its silence was invisible inside `payments`'s healthy total. The
/// per-selection guard closes this: ANY selection with zero of its OWN
/// archive fingerprints disqualifies the result from `Pass`, regardless of
/// what a sibling selection contributed.
#[test]
fn a_selection_that_sampled_zero_archive_fingerprints_cannot_hide_inside_a_passing_aggregate() {
    let (store, sha) = store_with_matching_segment();
    // `facts_with_segment`/`sel_orders`/`plan_orders_to_drill_orders` only
    // ever describe "orders"; build a second topic, "payments", by hand so
    // this test can pool one EMPTY selection with one HEALTHY one.
    let facts_orders = facts_with_segment(&sha);
    let bytes_payments = b"segment payload for verify phase test - payments (healthy, FIX 2)";
    let sha_payments = logweir_core::ids::sha256_prefixed(bytes_payments);
    store
        .put_create_only("logweir/seg-payments-fix2.kbak", bytes_payments)
        .unwrap();
    let mut facts = facts_orders.clone();
    facts.topics.push(TopicFacts {
        name: "payments".into(),
        original_partition_count: Some(1),
        source_replication_factor: Some(3),
        configurations: fixtures::source_configs(&[("cleanup.policy", "compact")]),
        partitions: vec![PartitionFacts {
            partition_id: 0,
            segments: vec![SegmentFacts {
                key: "logweir/seg-payments-fix2.kbak".into(),
                start_offset: 0,
                end_offset: 24,
                start_timestamp: WINDOW.0,
                end_timestamp: WINDOW.1,
                record_count: 25,
                sha256: sha_payments,
                uploaded_at: WINDOW.1,
            }],
            gaps: vec![],
            pruned: vec![],
        }],
    });

    let (archive_payments, cons_payments) = fixtures::matching_pair(25);
    let mut by_topic = BTreeMap::new();
    // "orders": Ok(vec![]) — support IS present, this specific selection's
    // archive simply came back with nothing (e.g. `segment_keys_for_set`
    // found no key, or no decoded timestamp landed in the window).
    by_topic.insert("orders".to_string(), vec![]);
    by_topic.insert("payments".to_string(), archive_payments);
    let engine = MultiTopicEngine {
        facts: facts.clone(),
        by_topic,
    };

    let mut topics = BTreeMap::new();
    // orders was genuinely restored to zero — consistent with its archive
    // side also having nothing to compare.
    topics.insert(
        "drill-orders".to_string(),
        TopicData {
            end_offsets: vec![(0, 0)],
            configs: fixtures::target_configs(&[
                ("cleanup.policy", "delete"),
                ("retention.ms", "-1"),
            ]),
            records: vec![],
        },
    );
    topics.insert(
        "drill-payments".to_string(),
        TopicData {
            end_offsets: vec![(0, 25)],
            configs: fixtures::target_configs(&[("cleanup.policy", "delete")]),
            records: cons_payments,
        },
    );
    let reader = MapReader { topics };

    let sel = vec![
        sel_orders().into_iter().next().unwrap(),
        SampleSelection {
            set: BackupSetRef {
                backup_id: "backup-verify-test".into(),
                manifest_key: "backup-verify-test/manifest.json".into(),
            },
            topic: "payments".into(),
            partition: 0,
            anchor: "head".into(),
            count: 25,
            window: WINDOW,
        },
    ];
    let mut mapping = fixtures::mapping("orders", "drill-orders");
    mapping.insert("payments".to_string(), "drill-payments".to_string());
    let mut plan = plan_orders_to_drill_orders();
    plan.topic_mapping = mapping.clone();

    let out = run(&engine, &reader, &store, &facts, &sel, &mapping, &plan).unwrap();

    assert_ne!(
        out.integrity.result,
        IntegrityResult::Pass,
        "orders sampled zero archive fingerprints; pooling it with a healthy payments \
         selection must never let the aggregate read as Pass — got {:?}",
        out.integrity
    );
    assert_eq!(out.integrity.result, IntegrityResult::Partial);
    assert!(
        out.integrity
            .partial_reason
            .as_deref()
            .is_some_and(|r| r.contains("orders/0")),
        "the reason must name the specific under-sampled selection: {:?}",
        out.integrity.partial_reason
    );
}

/// Review finding F2/F3: a byte-fingerprint comparison that samples ZERO
/// records — `engine.fingerprints` returns `Ok(vec![])`, not
/// `Err(Unsupported(..))` — must never read as `Pass`. This is now the
/// production path for `IntegrityResult::Partial` (see the module doc
/// comment): "compared nothing" is reported honestly as "could not
/// reconcile", never silently upgraded to success.
#[test]
fn a_byte_fingerprint_comparison_that_samples_zero_records_is_partial_never_pass() {
    let (store, sha) = store_with_matching_segment();
    let facts = facts_with_segment(&sha);

    // The TARGET genuinely holds restored records (so `newest_ts` has
    // something honest to measure) — it is the ARCHIVE side that returns
    // zero fingerprints, a plausible real scenario distinct from
    // `unsupported: Some(..)` (KBAK-level-not-supported takes the
    // ConsumeOnly path, covered elsewhere): e.g. a decode boundary that
    // silently produced no in-window fingerprints.
    let (_archive_unused, cons) = fixtures::matching_pair(5);
    let mut topics = BTreeMap::new();
    topics.insert(
        "drill-orders".to_string(),
        TopicData {
            end_offsets: vec![(0, 5)],
            configs: fixtures::target_configs(&[
                ("cleanup.policy", "delete"),
                ("retention.ms", "-1"),
            ]),
            records: cons,
        },
    );
    let reader = MapReader { topics };
    let engine = VerifyEngine {
        facts: facts.clone(),
        // Ok(vec![]) — support IS present, the archive simply returned
        // nothing to compare.
        fingerprints: vec![],
        unsupported: None,
        validation: ValidationBehavior::Success(0),
    };
    let mapping = fixtures::mapping("orders", "drill-orders");
    let plan = plan_orders_to_drill_orders();

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel_orders(),
        &mapping,
        &plan,
    )
    .unwrap();

    assert_eq!(out.integrity.level, IntegrityLevel::ByteFingerprint);
    assert_eq!(
        out.integrity.result,
        IntegrityResult::Partial,
        "zero sampled records must never report Pass, and is a different case from ConsumeOnly"
    );
    // Task 19 fix round 3: the reason is asserted by CONTENT, not merely
    // `is_some()`. "Compared nothing" and "compared less than claimed" are
    // different findings that an auditor must be able to tell apart in the
    // signed document, and a bare `is_some()` let a mutant collapsing the
    // zero case into the short-sample wording survive with nothing red.
    assert!(
        out.integrity
            .partial_reason
            .as_deref()
            .is_some_and(|r| r.contains("zero archive fingerprints")),
        "the reason must say the comparison sampled NOTHING, not merely that something was \
         wrong: {:?}",
        out.integrity.partial_reason
    );
    assert_eq!(out.integrity.records_sampled, 0);
    assert_eq!(out.pass_rate(), None);
}

/// The consume-only lane's own SHORT case — the counterpart to
/// `a_short_archive_fingerprint_list_is_unverified_coverage_not_a_smaller_successful_sample`,
/// which exercises the byte-fingerprint lane only.
///
/// Found by mutation: dropping `records_restored >= claimed` from the
/// consume-only lane's positive predicate left every test in this suite
/// green, because nothing drove a consume-only selection to a read-back that
/// was non-empty but SHORT. That is "compared less than claimed" reaching
/// `Pass` on a lane — the fourth door's exact shape, one guard over. The
/// manifest claims 50 records for this selection and the target hands back 5.
#[test]
fn a_short_read_back_on_the_consume_only_lane_is_unverified_not_a_smaller_successful_sample() {
    let (store, sha) = store_with_matching_segment();
    let facts = facts_with_segment(&sha); // one segment, record_count 50
    let (_, mut cons) = fixtures::matching_pair(50);
    cons.truncate(5); // 90% of the claimed records never came back

    let engine = LaneEngine {
        facts: facts.clone(),
        by_selection: [(
            "orders/0".to_string(),
            Err("kbak level 1 (pre-0.21)".to_string()),
        )]
        .into_iter()
        .collect(),
    };
    let reader = MapReader {
        topics: [target("drill-orders", vec![(0, 5)], cons)]
            .into_iter()
            .collect(),
    };

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel_orders(), // count 50
        &fixtures::mapping("orders", "drill-orders"),
        &plan_orders_to_drill_orders(),
    )
    .expect("an under-delivering target is a DRILL RESULT, never an Err");

    assert_ne!(
        out.integrity.result,
        IntegrityResult::Pass,
        "5 records back against a manifest claim of 50 leaves 45 unaccounted for; \
         consume-only is a weaker CLAIM, never a weaker CHECK — got {:?}",
        out.integrity
    );
    assert_eq!(out.integrity.result, IntegrityResult::Partial);
    assert_eq!(out.integrity.level, IntegrityLevel::ConsumeOnly);
    assert_eq!(out.records_restored, 5);
    let reason = out
        .integrity
        .partial_reason
        .expect("partial must carry a reason");
    assert!(reason.contains("orders/0"), "{reason:?}");
    assert!(
        reason.contains("gave back 5 records") && reason.contains("claims 50"),
        "the reason must name BOTH the shortfall and the claim, so an auditor can size it: \
         {reason:?}"
    );
}

/// Review finding r13 (surviving mutant): the per-partition consume cap.
/// `SpyReader` records every `max` argument `consume_range` actually
/// received; the target genuinely holds 40 records but `sel_orders()` caps
/// `count` at 50 (its own fixture value) — this test instead builds a
/// selection with a SMALL count against a LARGE target, so a mutant
/// replacing `s.count` with (say) `usize::MAX` at the call site changes what
/// is observed here, not just how many records happen to exist.
struct SpyReader {
    inner: MapReader,
    seen_max: std::sync::Mutex<Vec<usize>>,
}
impl ClusterReader for SpyReader {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        self.inner.cluster_id()
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        self.inner.list_topics()
    }
    fn end_offsets(&self, topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        self.inner.end_offsets(topic)
    }
    fn topic_configs(&self, topic: &str) -> Result<BTreeMap<String, String>, KafkaError> {
        self.inner.topic_configs(topic)
    }
    fn consume_range(
        &self,
        topic: &str,
        partition: i32,
        from: i64,
        max: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        self.seen_max.lock().unwrap().push(max);
        self.inner.consume_range(topic, partition, from, max)
    }
}

#[test]
fn verdict_for_selection_caps_the_read_at_the_selections_own_count() {
    let (store, sha) = store_with_matching_segment();
    let facts = facts_with_segment(&sha);
    // 40 records actually available on the target...
    let (_archive_unused, cons) = fixtures::matching_pair(40);
    let mut topics = BTreeMap::new();
    topics.insert(
        "drill-orders".to_string(),
        TopicData {
            end_offsets: vec![(0, 40)],
            configs: fixtures::target_configs(&[
                ("cleanup.policy", "delete"),
                ("retention.ms", "-1"),
            ]),
            records: cons,
        },
    );
    let reader = SpyReader {
        inner: MapReader { topics },
        seen_max: std::sync::Mutex::new(vec![]),
    };
    // ...but only 7 archive fingerprints were sampled.
    let (archive, _) = fixtures::matching_pair(7);
    let engine = VerifyEngine {
        facts: facts.clone(),
        fingerprints: archive,
        unsupported: None,
        validation: ValidationBehavior::Success(0),
    };
    let mapping = fixtures::mapping("orders", "drill-orders");
    let plan = plan_orders_to_drill_orders();
    let mut sel = sel_orders();
    sel[0].count = 7;

    let out = run(&engine, &reader, &store, &facts, &sel, &mapping, &plan).unwrap();

    // `newest_ts` (run at the very end of `run`, for the RPO input) makes
    // its OWN trailing `consume_range` call with `max = 1` — that call is
    // expected and is not what this test pins. The canary reconciliation's
    // own call, made first, is what must be capped at the selection's count.
    let seen = reader.seen_max.lock().unwrap();
    assert_eq!(
        seen.first().copied(),
        Some(7),
        "verdict_for_selection's own consume_range call must use the SELECTION's own count \
         (7), not the target's actual size (40) or an unbounded cap: saw {seen:?}"
    );
    assert!(
        !seen.contains(&40) && !seen.contains(&usize::MAX),
        "the cap must never widen to the target's real size or an unbounded read: saw {seen:?}"
    );
    // The read itself was capped to 7, so only 7 (of the 40 available) were
    // ever consumed, matching the 7 archive fingerprints exactly.
    assert_eq!(out.records_restored, 7);
    assert_eq!(out.integrity.records_sampled_matching, 7);
}

/// Review finding r15: addendum A1 explicitly redefined `records_restored` as
/// "the count of records actually consumed, not `matched + mismatched`" —
/// every prior fixture made the two figures equal, so the distinction was
/// unpinned. Here the target holds MORE records than the archive sampled
/// (8 consumed vs. 5 archived), so `records_restored` (8) must differ from
/// `records_sampled_matching + mismatches` (5): a mutant reverting to the
/// old `matched + mismatched` definition changes the observed value.
#[test]
fn records_restored_is_the_consumed_count_not_matched_plus_mismatched() {
    let (store, sha) = store_with_matching_segment();
    let facts = facts_with_segment(&sha);
    // The target holds 8 records; only the first 5 have a corresponding
    // archive fingerprint (a genuine over-restore/extra-records scenario).
    // `matching_pair` builds each record deterministically by index
    // (`k{i}`/`v{i}`, base timestamp + i) regardless of `n`, so
    // `matching_pair(5)`'s 5 records are byte-identical to the first 5 of
    // `matching_pair(8)`'s 8 — the extra 3 are simply additional, unsampled
    // records already present on the target.
    let (archive, _) = fixtures::matching_pair(5);
    let (_, cons) = fixtures::matching_pair(8);
    let mut topics = BTreeMap::new();
    topics.insert(
        "drill-orders".to_string(),
        TopicData {
            end_offsets: vec![(0, 8)],
            configs: fixtures::target_configs(&[
                ("cleanup.policy", "delete"),
                ("retention.ms", "-1"),
            ]),
            records: cons,
        },
    );
    let reader = MapReader { topics };
    let engine = VerifyEngine {
        facts: facts.clone(),
        fingerprints: archive,
        unsupported: None,
        validation: ValidationBehavior::Success(0),
    };
    let mapping = fixtures::mapping("orders", "drill-orders");
    let plan = plan_orders_to_drill_orders();
    let mut sel = sel_orders();
    sel[0].count = 8;

    let out = run(&engine, &reader, &store, &facts, &sel, &mapping, &plan).unwrap();

    assert_eq!(out.integrity.records_sampled, 5);
    assert_eq!(out.integrity.records_sampled_matching, 5);
    assert_eq!(out.integrity.mismatches, 0);
    assert_eq!(
        out.records_restored, 8,
        "records_restored must be the CONSUMED count (8), not matched+mismatched (5)"
    );
}

/// Task 19 fix round 2: "no shipped test drives two partitions of one topic
/// through `run()`" (review). Every prior multi-selection test used two
/// DIFFERENT topics; this one uses ONE topic ("orders") across TWO
/// partitions, each independently and correctly restored but with DISTINCT
/// per-partition content — the shape the reviewer's very first F1
/// demonstration also used ("Same defect within one topic. Two partitions of
/// orders, each restored perfectly, distinct content"). If `compare()` were
/// ever pooled across selections again, partition 1's consumed records would
/// displace partition 0's in one shared `by_offset` map (both partitions
/// start at offset 0), and partition 0's archive entries would be checked
/// against partition 1's — different — content, fabricating mismatches on a
/// perfectly healthy restore.
struct PerPartitionEngine {
    facts: BackupSetFacts,
    by_partition: BTreeMap<i32, Vec<RecordFingerprint>>,
}

impl DataEngine for PerPartitionEngine {
    fn id(&self) -> EngineId {
        EngineId {
            id: "fake".into(),
            version: "v0.0.0".into(),
            digest: format!("sha256:{}", "0".repeat(64)),
        }
    }
    fn list_backup_sets(&self, _: &StorageUrl) -> Result<Vec<BackupSetRef>, EngineError> {
        Ok(vec![])
    }
    fn describe(&self, _: &BackupSetRef) -> Result<BackupSetFacts, EngineError> {
        Ok(self.facts.clone())
    }
    fn preflight(&self, _: &RestorePlan) -> Result<PreflightReport, EngineError> {
        Err(EngineError::Operational("not used by this fixture".into()))
    }
    fn restore(
        &self,
        _: &RestorePlan,
        _: &mut dyn PhaseObserver,
    ) -> Result<RestoreFacts, EngineError> {
        Err(EngineError::Operational("not used by this fixture".into()))
    }
    fn fingerprints(&self, s: &SampleSelection) -> Result<Vec<RecordFingerprint>, EngineError> {
        Ok(self
            .by_partition
            .get(&s.partition)
            .cloned()
            .unwrap_or_default())
    }
    fn validation_run(&self, _: &RestorePlan) -> Result<EngineRun, EngineError> {
        Ok(EngineRun { exit_code: 0 })
    }
}

/// Distinct content per PARTITION (not per topic) — `partition` is folded
/// into the key/value bytes so two partitions' records can never
/// fingerprint-match each other.
fn distinct_pair_for_partition(
    partition: i32,
    n: usize,
) -> (Vec<RecordFingerprint>, Vec<ConsumedRecord>) {
    let mut arch = Vec::with_capacity(n);
    let mut cons = Vec::with_capacity(n);
    for i in 0..n {
        let off = i as i64;
        let headers = vec![(
            "x-original-offset".to_string(),
            Some(off.to_string().into_bytes()),
        )];
        let key = format!("p{partition}-k{i}").into_bytes();
        let value = format!("p{partition}-v{i}").into_bytes();
        let tsms = 1_756_425_600_000i64 + off;
        arch.push(RecordFingerprint {
            topic: "orders".into(),
            partition,
            offset: off,
            sha256: logweir_kafka::fingerprint::record_fingerprint(
                Some(&key),
                Some(&value),
                &headers,
                tsms,
            ),
        });
        cons.push(ConsumedRecord {
            partition,
            offset: off,
            timestamp_ms: tsms,
            key: Some(key),
            value: Some(value),
            headers,
        });
    }
    (arch, cons)
}

#[test]
fn run_reconciles_two_partitions_of_one_topic_independently_not_pooled() {
    let bytes = b"segment payload for verify phase test - two partitions";
    let sha = logweir_core::ids::sha256_prefixed(bytes);
    let store = logweir_engine_oso::storage::Store::in_memory("logweir");
    store.put_create_only("logweir/seg-2p.kbak", bytes).unwrap();

    let facts = BackupSetFacts {
        backup_id: "backup-verify-test".into(),
        created_at: fixtures::ts("2026-09-03T09:00:00Z"),
        source_cluster_id: Some("SRC0000000000000000000".into()),
        manifest_sha256: "sha256:0".into(),
        manifest_version_id: None,
        consumer_group_snapshot_sha256: None,
        topics: vec![TopicFacts {
            name: "orders".into(),
            original_partition_count: Some(2),
            source_replication_factor: Some(3),
            configurations: fixtures::source_configs(&[("cleanup.policy", "compact")]),
            partitions: vec![
                PartitionFacts {
                    partition_id: 0,
                    segments: vec![SegmentFacts {
                        key: "logweir/seg-2p.kbak".into(),
                        start_offset: 0,
                        end_offset: 9,
                        start_timestamp: WINDOW.0,
                        end_timestamp: WINDOW.1,
                        record_count: 10,
                        sha256: sha.clone(),
                        uploaded_at: WINDOW.1,
                    }],
                    gaps: vec![],
                    pruned: vec![],
                },
                PartitionFacts {
                    partition_id: 1,
                    segments: vec![SegmentFacts {
                        key: "logweir/seg-2p.kbak".into(),
                        start_offset: 0,
                        end_offset: 9,
                        start_timestamp: WINDOW.0,
                        end_timestamp: WINDOW.1,
                        record_count: 10,
                        sha256: sha,
                        uploaded_at: WINDOW.1,
                    }],
                    gaps: vec![],
                    pruned: vec![],
                },
            ],
        }],
    };

    let (archive_p0, cons_p0) = distinct_pair_for_partition(0, 10);
    let (archive_p1, cons_p1) = distinct_pair_for_partition(1, 10);

    let mut by_partition = BTreeMap::new();
    by_partition.insert(0, archive_p0);
    by_partition.insert(1, archive_p1);
    let engine = PerPartitionEngine {
        facts: facts.clone(),
        by_partition,
    };

    // Both partitions land on the SAME target topic ("drill-orders"),
    // distinguished only by their `partition` field — exactly the layout
    // that would collide if `compare()` were ever pooled across selections.
    let mut records = cons_p0;
    records.extend(cons_p1);
    let mut topics = BTreeMap::new();
    topics.insert(
        "drill-orders".to_string(),
        TopicData {
            end_offsets: vec![(0, 10), (1, 10)],
            configs: fixtures::target_configs(&[("cleanup.policy", "delete")]),
            records,
        },
    );
    let reader = MapReader { topics };

    let sel = vec![
        SampleSelection {
            set: BackupSetRef {
                backup_id: "backup-verify-test".into(),
                manifest_key: "backup-verify-test/manifest.json".into(),
            },
            topic: "orders".into(),
            partition: 0,
            anchor: "head".into(),
            count: 10,
            window: WINDOW,
        },
        SampleSelection {
            set: BackupSetRef {
                backup_id: "backup-verify-test".into(),
                manifest_key: "backup-verify-test/manifest.json".into(),
            },
            topic: "orders".into(),
            partition: 1,
            anchor: "head".into(),
            count: 10,
            window: WINDOW,
        },
    ];
    let mapping = fixtures::mapping("orders", "drill-orders");
    let mut plan = plan_orders_to_drill_orders();
    plan.topic_mapping = mapping.clone();

    let out = run(&engine, &reader, &store, &facts, &sel, &mapping, &plan).unwrap();

    assert_eq!(
        out.integrity.result,
        IntegrityResult::Pass,
        "two independently-healthy partitions of the same topic must not fabricate \
         mismatches against each other — got {:?}",
        out.integrity
    );
    assert_eq!(out.integrity.records_sampled, 20);
    assert_eq!(out.integrity.records_sampled_matching, 20);
    assert_eq!(out.integrity.mismatches, 0);
    assert_eq!(out.pass_rate(), Some(1.0));
}

// ===========================================================================
// Task 19 fix round 3 — THE FIVE REPRODUCTIONS.
//
// Five independent reviewers each produced an EXECUTED reproduction of the
// same critical defect: phase 7 signing `Pass` over a restore it had not
// actually checked. Rounds 1 and 2 each closed the lane their own defect was
// found in and left another open. These five tests are the regression suite
// for that defect, one named test per reproduction, each driven through the
// FULL `run` (never through a helper in isolation) because every one of the
// four shipped doors was in `run`'s own wiring rather than in a leaf.
//
// Each test states the OLD (broken) observation in its doc comment and
// asserts the new one, so a future reader can tell at a glance what the test
// is defending and what it looked like when it was wrong.

/// A `DataEngine` answering per SELECTION (`"topic/partition"`), able to hand
/// back either real fingerprints or `EngineError::Unsupported` — the mixed
/// case `VerifyEngine` (one answer for the whole run) and `MultiTopicEngine`
/// (fingerprints only, never `Unsupported`) between them cannot express, and
/// the case reproductions 1, 2, 3 and 5 all turn on.
struct LaneEngine {
    facts: BackupSetFacts,
    /// `Ok(fingerprints)`, or `Err(reason)` rendered as
    /// `EngineError::Unsupported(reason)` — a pre-0.21 archive or a KBAK
    /// level-1 segment, which is a NORMAL production condition.
    by_selection: BTreeMap<String, Result<Vec<RecordFingerprint>, String>>,
}

impl DataEngine for LaneEngine {
    fn id(&self) -> EngineId {
        EngineId {
            id: "fake".into(),
            version: "v0.0.0".into(),
            digest: format!("sha256:{}", "0".repeat(64)),
        }
    }
    fn list_backup_sets(&self, _: &StorageUrl) -> Result<Vec<BackupSetRef>, EngineError> {
        Ok(vec![])
    }
    fn describe(&self, _: &BackupSetRef) -> Result<BackupSetFacts, EngineError> {
        Ok(self.facts.clone())
    }
    fn preflight(&self, _: &RestorePlan) -> Result<PreflightReport, EngineError> {
        Err(EngineError::Operational("not used by this fixture".into()))
    }
    fn restore(
        &self,
        _: &RestorePlan,
        _: &mut dyn PhaseObserver,
    ) -> Result<RestoreFacts, EngineError> {
        Err(EngineError::Operational("not used by this fixture".into()))
    }
    fn fingerprints(&self, s: &SampleSelection) -> Result<Vec<RecordFingerprint>, EngineError> {
        match self
            .by_selection
            .get(&format!("{}/{}", s.topic, s.partition))
        {
            Some(Ok(fp)) => Ok(fp.clone()),
            Some(Err(reason)) => Err(EngineError::Unsupported(reason.clone())),
            None => Ok(vec![]),
        }
    }
    fn validation_run(&self, _: &RestorePlan) -> Result<EngineRun, EngineError> {
        Ok(EngineRun { exit_code: 0 })
    }
}

/// Two topics ("orders", "payments"), one partition each, one in-window
/// segment each whose manifest sha256 is the REAL hash of the bytes put into
/// the returned `Store`, and whose `record_count` is `records_each`.
///
/// Both selections' SEGMENT lane therefore reaches `Evidence::Verified` on
/// its own merits, which is the point: every assertion in the reproductions
/// below is then attributable to the RECORD lane alone. A test whose segment
/// lane was incidentally unverified would report `Partial` for the wrong
/// reason and would keep reporting it after the record-lane fix was reverted.
fn two_topic_facts_and_store(
    records_each: i64,
) -> (BackupSetFacts, logweir_engine_oso::storage::Store) {
    let store = logweir_engine_oso::storage::Store::in_memory("logweir");
    let mut topics = Vec::new();
    for name in ["orders", "payments"] {
        let key = format!("logweir/seg-{name}-r3.kbak");
        let bytes = format!("segment payload for {name}, round 3 reproductions").into_bytes();
        store.put_create_only(&key, &bytes).unwrap();
        topics.push(TopicFacts {
            name: name.into(),
            original_partition_count: Some(1),
            source_replication_factor: Some(3),
            configurations: fixtures::source_configs(&[("cleanup.policy", "compact")]),
            partitions: vec![PartitionFacts {
                partition_id: 0,
                segments: vec![SegmentFacts {
                    key,
                    start_offset: 0,
                    end_offset: records_each - 1,
                    start_timestamp: WINDOW.0,
                    end_timestamp: WINDOW.1,
                    record_count: records_each,
                    sha256: logweir_core::ids::sha256_prefixed(&bytes),
                    uploaded_at: WINDOW.1,
                }],
                gaps: vec![],
                pruned: vec![],
            }],
        });
    }
    let facts = BackupSetFacts {
        backup_id: "backup-verify-test".into(),
        created_at: fixtures::ts("2026-09-03T09:00:00Z"),
        source_cluster_id: Some("SRC0000000000000000000".into()),
        manifest_sha256: "sha256:0".into(),
        manifest_version_id: None,
        consumer_group_snapshot_sha256: None,
        topics,
    };
    (facts, store)
}

fn two_topic_mapping() -> BTreeMap<String, String> {
    [
        ("orders".to_string(), "drill-orders".to_string()),
        ("payments".to_string(), "drill-payments".to_string()),
    ]
    .into_iter()
    .collect()
}

fn sel_for(topic: &str, partition: i32, count: usize) -> SampleSelection {
    SampleSelection {
        set: BackupSetRef {
            backup_id: "backup-verify-test".into(),
            manifest_key: "backup-verify-test/manifest.json".into(),
        },
        topic: topic.into(),
        partition,
        anchor: "head".into(),
        count,
        window: WINDOW,
    }
}

fn target(
    name: &str,
    end_offsets: Vec<(i32, i64)>,
    records: Vec<ConsumedRecord>,
) -> (String, TopicData) {
    (
        name.to_string(),
        TopicData {
            end_offsets,
            configs: fixtures::target_configs(&[("cleanup.policy", "delete")]),
            records,
        },
    )
}

fn two_topic_plan() -> RestorePlan {
    let mut plan = plan_orders_to_drill_orders();
    plan.topic_mapping = two_topic_mapping();
    plan
}

// ---------------------------------------------------------------------------
// REPRODUCTION 1 — one of two topics restored to ZERO records, on a legacy
// archive.
//
// OLD OBSERVATION (the fourth door, shipped):
//     level=ConsumeOnly result=Pass sampled=0 records_restored=25
//
// Both zero-record guards lived inside the `ByteFingerprint` arm, so the
// `ConsumeOnly` lane had no zero-record guard of ANY kind: `result` was
// initialised to `Pass` and nothing in that lane could move it. The trigger —
// a pre-0.21 archive or a KBAK level-1 segment — is a NORMAL production
// condition the code treated as a routine downgrade, so this signed a
// successful restore drill over a topic that restored nothing at all.
//
// NOW: the obligation is not written in the lane. `verdict_for_selection`'s
// consume-only arm must construct `Evidence` exactly as the byte-fingerprint
// arm does, and "the target partition gave back zero records" is
// `Unverified`, which `roll_up` refuses to fold into a `Pass`.

/// Reproduction 1. A legacy archive downgrades BOTH selections to
/// consume-only; `payments` restored healthily, `orders` restored to zero.
/// The drill must not report `Pass`, and must name `orders/0`.
#[test]
fn one_of_two_topics_restored_to_zero_records_cannot_pass_on_the_consume_only_lane() {
    let (facts, store) = two_topic_facts_and_store(25);
    let (_, cons_payments) = distinct_matching_pair("payments", 25);

    let engine = LaneEngine {
        facts: facts.clone(),
        by_selection: [
            (
                "orders/0".to_string(),
                Err("kbak level 1 (pre-0.21)".to_string()),
            ),
            (
                "payments/0".to_string(),
                Err("kbak level 1 (pre-0.21)".to_string()),
            ),
        ]
        .into_iter()
        .collect(),
    };

    let reader = MapReader {
        topics: [
            // Never restored: the topic exists, and it is empty.
            target("drill-orders", vec![(0, 0)], vec![]),
            target("drill-payments", vec![(0, 25)], cons_payments),
        ]
        .into_iter()
        .collect(),
    };

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &[sel_for("orders", 0, 25), sel_for("payments", 0, 25)],
        &two_topic_mapping(),
        &two_topic_plan(),
    )
    .expect("an un-restored topic is a DRILL RESULT (exit 2, signed), never an Err (exit 1)");

    assert_ne!(
        out.integrity.result,
        IntegrityResult::Pass,
        "a topic restored to ZERO records must never reach Pass, on any lane — got {:?}",
        out.integrity
    );
    assert_eq!(out.integrity.result, IntegrityResult::Partial);
    assert_eq!(out.integrity.level, IntegrityLevel::ConsumeOnly);
    // The old observation's exact numbers, now attached to a verdict that is
    // not a pass: `sampled = 0` because the consume-only lane reconciles
    // nothing, and `records_restored = 25` from the healthy sibling alone.
    assert_eq!(out.integrity.records_sampled, 0);
    assert_eq!(out.records_restored, 25);
    assert_eq!(out.pass_rate(), None);
    let reason = out
        .integrity
        .partial_reason
        .expect("partial must carry a reason");
    assert!(
        reason.contains("orders/0"),
        "the reason must name the partition that gave back nothing: {reason:?}"
    );
    assert!(
        reason.contains("zero records"),
        "the reason must say what was wrong, not merely that something was: {reason:?}"
    );
}

// ---------------------------------------------------------------------------
// REPRODUCTION 2 — `orders` restored 100% CORRUPT and `payments` not restored
// at all.
//
// OLD OBSERVATION: Pass, records_restored=50.
//
// `compare()` pooled every selection's records into one offset-keyed map.
// `ConsumedRecord` carries no topic field and every Kafka partition starts at
// offset 0, so one topic's records satisfied ANOTHER topic's archive lookups
// by coincidence of matching offsets. Two independently broken topics
// reconciled against each other into a clean pass.
//
// NOW: `compare()` is called from `verdict_for_selection`, which is handed ONE
// selection, so it cannot see two selections' data at once even by accident.
// Both defects are detected, separately, and both are `Failed` — "examined,
// and found wrong", which outranks every other verdict.

/// Reproduction 2. Corruption and absence are each detected on their own
/// merits and neither can be papered over by the other.
#[test]
fn a_wholly_corrupt_topic_beside_an_unrestored_one_fails_and_never_reconciles_against_it() {
    let (facts, store) = two_topic_facts_and_store(25);

    // `fixtures::matching_pair` builds byte-IDENTICAL `k{i}`/`v{i}` records
    // regardless of the topic it is called for (the fingerprint folds in no
    // topic name), and both partitions start at offset 0 — deliberately, so
    // that a pooled `compare()` COULD satisfy one topic's archive from the
    // other's records. That is what makes this test discriminate the pooled
    // implementation from the per-selection one, exactly as
    // `a_topic_restored_to_zero_records_...` documents for its own case.
    let (archive_orders, mut cons_orders) = fixtures::matching_pair(25);
    let (archive_payments, _cons_payments_never_restored) = fixtures::matching_pair(25);
    // 100% corrupt: every restored value differs from what the archive
    // fingerprint was computed over.
    for (i, r) in cons_orders.iter_mut().enumerate() {
        r.value = Some(format!("corrupt-{i}").into_bytes());
    }

    let engine = LaneEngine {
        facts: facts.clone(),
        by_selection: [
            ("orders/0".to_string(), Ok(archive_orders)),
            ("payments/0".to_string(), Ok(archive_payments)),
        ]
        .into_iter()
        .collect(),
    };

    let reader = MapReader {
        topics: [
            target("drill-orders", vec![(0, 25)], cons_orders),
            // Not restored at all.
            target("drill-payments", vec![(0, 0)], vec![]),
        ]
        .into_iter()
        .collect(),
    };

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &[sel_for("orders", 0, 25), sel_for("payments", 0, 25)],
        &two_topic_mapping(),
        &two_topic_plan(),
    )
    .expect("a failed reconciliation is a DRILL RESULT, never an Err");

    assert_ne!(
        out.integrity.result,
        IntegrityResult::Pass,
        "a 100%-corrupt topic and an un-restored topic must never reconcile into a pass — \
         got {:?}",
        out.integrity
    );
    assert_eq!(out.integrity.result, IntegrityResult::Fail);
    assert_eq!(out.integrity.records_sampled, 50);
    assert_eq!(
        out.integrity.records_sampled_matching, 0,
        "not one of the 50 archive fingerprints has an honest counterpart on the target; a \
         non-zero count here means one topic was matched against the other's records"
    );
    assert_eq!(out.integrity.mismatches, 50);
    assert_eq!(out.pass_rate(), Some(0.0));
    // Only `orders` gave anything back at all.
    assert_eq!(out.records_restored, 25);
}

// ---------------------------------------------------------------------------
// REPRODUCTION 3 — ZERO records consumed from a sampled partition.
//
// OLD OBSERVATION: Pass, records_restored=0.
//
// The purest form of the fourth door: the drill consumed nothing whatsoever
// from the partition it sampled, and still signed a pass. `result` began at
// `Pass` on the consume-only lane and nothing wrote to it.
//
// NOW: `Pass` is COMPUTED from positive evidence, never initialised. There is
// no `result` variable in `run` to forget to move.

/// Reproduction 3. The sampled partition gives back nothing, while a
/// DIFFERENT partition of the same target topic holds data — so the drill has
/// an honest `newest_restored_ts_ms` to report and cannot be dismissed as
/// "the whole target was empty". The sample still proved nothing.
#[test]
fn zero_records_consumed_from_a_sampled_partition_is_never_a_pass() {
    let (store, sha) = store_with_matching_segment();
    let facts = facts_with_segment(&sha);
    // Partition 1 of the SAME target topic was restored; partition 0 — the
    // one this drill actually sampled — was not.
    let (_, cons_p1) = distinct_pair_for_partition(1, 5);

    let engine = LaneEngine {
        facts: facts.clone(),
        by_selection: [(
            "orders/0".to_string(),
            Err("kbak level 1 (pre-0.21)".to_string()),
        )]
        .into_iter()
        .collect(),
    };
    let reader = MapReader {
        topics: [target("drill-orders", vec![(0, 0), (1, 5)], cons_p1)]
            .into_iter()
            .collect(),
    };

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel_orders(),
        &fixtures::mapping("orders", "drill-orders"),
        &plan_orders_to_drill_orders(),
    )
    .expect("a partition that gave back nothing is a DRILL RESULT, never an Err");

    assert_ne!(
        out.integrity.result,
        IntegrityResult::Pass,
        "the sampled partition returned zero records; nothing was verified — got {:?}",
        out.integrity
    );
    assert_eq!(out.integrity.result, IntegrityResult::Partial);
    assert_eq!(out.records_restored, 0);
    assert_eq!(out.pass_rate(), None);
    assert!(out
        .integrity
        .partial_reason
        .expect("partial must carry a reason")
        .contains("orders/0"));
    // A healthy sibling PARTITION is still measured for the RPO — the drill
    // reports what it honestly saw, it simply does not call it a pass.
    assert_eq!(out.newest_restored_ts_ms, MATCHING_PAIR_BASE_TS + 4);
}

// ---------------------------------------------------------------------------
// REPRODUCTION 4 — a SHORT archive fingerprint list over a 96%-lossy
// partition.
//
// OLD OBSERVATION:
//     level=ByteFingerprint result=Pass sampled=26 matching=26 pass_rate=Some(1.0)
//
// The archive returned 1 fingerprint where the manifest claims 25. Every
// guard asked "did we compare zero?", none asked "did we compare LESS than we
// claim to have?" — so 24 unexamined records read as a smaller successful
// sample, and `Some(1.0)` was signed over a partition that had lost 96% of
// its data.
//
// NOW: short IS unverified. What came back is held against
// `min(sel.count, Σ record_count)` — both already-signed figures — and
// `pass_rate_measured` is a ratio over the WHOLE sample or it is null.

/// Reproduction 4. A short sample is coverage the drill did not obtain, not a
/// smaller successful sample; and no `Some(1.0)` may sit beside it.
#[test]
fn a_short_archive_fingerprint_list_is_unverified_coverage_not_a_smaller_successful_sample() {
    let (facts, store) = two_topic_facts_and_store(25);

    // `orders` lost 96% of its records: the manifest claims 25 in this
    // window, the archive can offer exactly 1, and the target holds that 1.
    let (mut archive_orders, mut cons_orders) = distinct_matching_pair("orders", 25);
    archive_orders.truncate(1);
    cons_orders.truncate(1);
    let (archive_payments, cons_payments) = distinct_matching_pair("payments", 25);

    let engine = LaneEngine {
        facts: facts.clone(),
        by_selection: [
            ("orders/0".to_string(), Ok(archive_orders)),
            ("payments/0".to_string(), Ok(archive_payments)),
        ]
        .into_iter()
        .collect(),
    };
    let reader = MapReader {
        topics: [
            target("drill-orders", vec![(0, 1)], cons_orders),
            target("drill-payments", vec![(0, 25)], cons_payments),
        ]
        .into_iter()
        .collect(),
    };

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &[sel_for("orders", 0, 25), sel_for("payments", 0, 25)],
        &two_topic_mapping(),
        &two_topic_plan(),
    )
    .expect("an under-delivering archive is a DRILL RESULT, never an Err");

    assert_ne!(
        out.integrity.result,
        IntegrityResult::Pass,
        "1 fingerprint against a manifest claim of 25 leaves 24 records unexamined — {:?}",
        out.integrity
    );
    assert_eq!(out.integrity.result, IntegrityResult::Partial);
    // The old observation's exact counters are still reported honestly — 26
    // records really were compared and really did match. What may NOT be
    // published beside them is a whole-sample pass rate.
    assert_eq!(out.integrity.records_sampled, 26);
    assert_eq!(out.integrity.records_sampled_matching, 26);
    assert_eq!(
        out.pass_rate(),
        None,
        "Some(1.0) over a 96%-lossy partition is the exact false assurance the fourth door \
         signed; a rate is a ratio over the WHOLE sample or it is null"
    );
    let reason = out
        .integrity
        .partial_reason
        .expect("partial must carry a reason");
    assert!(reason.contains("orders/0"), "{reason:?}");
    assert!(
        reason.contains("25"),
        "the reason must name what the manifest claimed, so an auditor can see the size of \
         the shortfall: {reason:?}"
    );
}

// ---------------------------------------------------------------------------
// REPRODUCTION 5 — one selection's `Unsupported` archive silently ERASED the
// byte-level claim for every other selection.
//
// OLD OBSERVATION: `probe_archive_mode` returned a single `ConsumeOnly` for
// the whole backup set on the FIRST `Unsupported`, discarding every
// fingerprint set it had already collected. One legacy partition therefore
// (a) threw away a sibling's genuinely-established byte-level reconciliation,
// and (b) moved the entire run onto the lane that had no zero-record guard —
// which is how reproduction 1 became reachable in the first place.
//
// NOW: the mode is PER SELECTION. A legacy segment downgrades only its own
// partition; `level` still reports the WEAKEST claim any selection could
// support (a drill containing a legacy segment cannot honestly tell an
// auditor "byte-fingerprint" for the restore as a whole), but the counters
// and `partial_reason` preserve what was genuinely established.

/// Reproduction 5. A downgrade names itself and costs the drill its
/// whole-run LEVEL; it does not cost a sibling selection its CHECK.
#[test]
fn one_selections_unsupported_archive_never_erases_another_selections_byte_level_claim() {
    let (facts, store) = two_topic_facts_and_store(25);
    let (_, cons_orders) = distinct_matching_pair("orders", 25);
    let (archive_payments, cons_payments) = distinct_matching_pair("payments", 25);

    let engine = LaneEngine {
        facts: facts.clone(),
        by_selection: [
            // A legacy segment in ONE partition.
            (
                "orders/0".to_string(),
                Err("kbak level 1 (pre-0.21)".to_string()),
            ),
            ("payments/0".to_string(), Ok(archive_payments)),
        ]
        .into_iter()
        .collect(),
    };
    let reader = MapReader {
        topics: [
            target("drill-orders", vec![(0, 25)], cons_orders),
            target("drill-payments", vec![(0, 25)], cons_payments),
        ]
        .into_iter()
        .collect(),
    };

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &[sel_for("orders", 0, 25), sel_for("payments", 0, 25)],
        &two_topic_mapping(),
        &two_topic_plan(),
    )
    .unwrap();

    // Both selections produced positive evidence — `orders` at the only
    // strength its archive permits, `payments` at full byte level — so the
    // drill passes, at the weaker of the two claims.
    assert_eq!(out.integrity.result, IntegrityResult::Pass);
    assert_eq!(
        out.integrity.level,
        IntegrityLevel::ConsumeOnly,
        "`level` reports the WEAKEST claim any selection could support, never the strongest"
    );
    assert_eq!(
        out.integrity.records_sampled, 25,
        "payments' 25 genuine reconciliations must SURVIVE orders' downgrade; the old \
         `probe_archive_mode` discarded them and reported 0"
    );
    assert_eq!(out.integrity.records_sampled_matching, 25);
    assert_eq!(out.records_restored, 50);
    assert_eq!(
        out.pass_rate(),
        None,
        "the level is not byte-fingerprint, so a measured rate would violate the scorecard's \
         own invariant"
    );
    let reason = out
        .integrity
        .partial_reason
        .expect("the downgrade must be named");
    assert!(
        reason.contains("orders/0") && reason.contains("kbak level 1"),
        "the downgrade must name the selection it applies to, and carry the engine's own \
         reason: {reason:?}"
    );
    assert!(
        reason.contains("payments/0"),
        "the selection whose byte-level claim WAS established must be named, so the downgrade \
         cannot erase it from the signed document: {reason:?}"
    );
}

// ---------------------------------------------------------------------------
// Round 3's two deliberate severity changes, pinned at `run`'s own call site.
// Both were `DrillError::Operational` (exit 1, NO signed artifact) before this
// round and are drill results (exit 2, signed scorecard) now — see
// `verdict_for_selection`'s "Severity" doc paragraph. A test that only
// asserted "not a Pass" would keep passing if either reverted to an `Err`, so
// each asserts `Ok` explicitly.

/// A `(topic, partition, window)` the plan names but no archive segment
/// matches. The archive holding no segment in this window is a positively
/// established fact ABOUT THE ARCHIVE, so it is reported in a signed document
/// naming the exact partition — strictly more use to an auditor than exit 1
/// and no document at all.
#[test]
fn a_selection_matching_no_archive_segment_is_a_signed_partial_not_an_operational_error() {
    let (store, sha) = store_with_matching_segment();
    let facts = facts_with_segment(&sha);
    let (archive, cons) = fixtures::matching_pair(50);

    let engine = VerifyEngine {
        facts: facts.clone(),
        fingerprints: archive,
        unsupported: None,
        validation: ValidationBehavior::Success(0),
    };
    let reader = MapReader {
        topics: [target("drill-orders", vec![(0, 50)], cons)]
            .into_iter()
            .collect(),
    };
    // `facts_with_segment`'s only segment spans WINDOW; this selection asks
    // for a window that overlaps none of it.
    let mut sel = sel_orders();
    sel[0].window = (WINDOW.1 + 1, WINDOW.1 + 2);

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel,
        &fixtures::mapping("orders", "drill-orders"),
        &plan_orders_to_drill_orders(),
    )
    .expect(
        "round 3: an archive holding no segment in the window is a DRILL RESULT (exit 2, \
             signed), not an Operational error (exit 1, nothing signed)",
    );

    assert_ne!(out.integrity.result, IntegrityResult::Pass);
    assert_eq!(out.integrity.result, IntegrityResult::Partial);
    let reason = out
        .integrity
        .partial_reason
        .expect("partial must carry a reason");
    assert!(reason.contains("orders/0"), "{reason:?}");
    assert!(reason.contains("no archive segment matches"), "{reason:?}");
}

/// A segment written before 0.21 carries an EMPTY sha256 and cannot be
/// checked. Round 2 skipped it with a logged note and let the run report
/// `Pass` anyway — the module doc claimed such segments were "never counted
/// as a pass", and nothing in the code made that true. A skip is coverage the
/// drill did not obtain.
#[test]
fn a_pre_0_21_segment_with_no_sha256_is_partial_never_a_silent_pass() {
    let (store, _sha) = store_with_matching_segment();
    // The manifest carries no sha256 for this segment at all.
    let facts = facts_with_segment("");
    let (archive, cons) = fixtures::matching_pair(50);

    let engine = VerifyEngine {
        facts: facts.clone(),
        fingerprints: archive,
        unsupported: None,
        validation: ValidationBehavior::Success(0),
    };
    let reader = MapReader {
        topics: [target("drill-orders", vec![(0, 50)], cons)]
            .into_iter()
            .collect(),
    };

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel_orders(),
        &fixtures::mapping("orders", "drill-orders"),
        &plan_orders_to_drill_orders(),
    )
    .unwrap();

    assert_ne!(
        out.integrity.result,
        IntegrityResult::Pass,
        "the record lane reconciled perfectly, but the SEGMENT lane checked nothing — a \
         selection needs positive evidence on BOTH lanes: {:?}",
        out.integrity
    );
    assert_eq!(out.integrity.result, IntegrityResult::Partial);
    assert!(out
        .integrity
        .partial_reason
        .expect("partial must carry a reason")
        .contains("before 0.21"));
}

/// Task 19 fix round 3, third pass — the REACHABLE half of the coordinator's
/// surviving mutant M3, driven through the full `run`.
///
/// `verdict_for_selection` calls `segment_evidence` BEFORE it matches on the
/// archive mode, so the segment sha256 check runs on every lane — including
/// consume-only. A pre-0.21 / KBAK-level-1 archive whose segment bytes do not
/// match its own manifest is therefore a real production path on which a
/// DOWNGRADED selection is `Evidence::Failed`.
///
/// The records here reconcile perfectly and the target returns everything the
/// manifest claims, so the record lane is `Verified` and the `Fail` is
/// attributable to the segment lane alone. Under M3 — which filtered
/// downgraded selections out of `roll_up`'s `any(failed)` — this reported
/// `Partial` ("the drill could not fully check this") for an archive the
/// drill HAD checked and found corrupt. Partial and Fail are not
/// interchangeable in a signed document.
#[test]
fn a_consume_only_selection_with_a_corrupt_segment_fails_the_drill_never_merely_partial() {
    let (store, _real_sha) = store_with_matching_segment();
    // The manifest claims a sha256 the stored segment bytes do not hash to.
    let facts = facts_with_segment(&format!("sha256:{}", "0".repeat(64)));
    let (_, cons) = fixtures::matching_pair(50);

    let engine = LaneEngine {
        facts: facts.clone(),
        by_selection: [(
            "orders/0".to_string(),
            Err("kbak level 1 (pre-0.21)".to_string()),
        )]
        .into_iter()
        .collect(),
    };
    let reader = MapReader {
        topics: [target("drill-orders", vec![(0, 50)], cons)]
            .into_iter()
            .collect(),
    };

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel_orders(),
        &fixtures::mapping("orders", "drill-orders"),
        &plan_orders_to_drill_orders(),
    )
    .expect("a corrupt archive segment is a DRILL RESULT, never an Err");

    assert_eq!(
        out.integrity.result,
        IntegrityResult::Fail,
        "the segment sha256 did not match the manifest — the drill EXAMINED this archive and \
         found it wrong. Reporting that as Partial would tell an auditor the opposite: that \
         the drill could not check it. Got {:?}",
        out.integrity
    );
    assert_eq!(
        out.integrity.level,
        IntegrityLevel::ConsumeOnly,
        "the archive is still unsupported, so the LEVEL is still the weakest claim available \
         — a downgraded lane may weaken the claim, never the verdict"
    );
    // The record lane was healthy: 50 records back, all the manifest claims.
    // So the Fail is attributable to the segment lane alone.
    assert_eq!(out.records_restored, 50);
    let reason = out
        .integrity
        .partial_reason
        .expect("the failure and the downgrade must both be named");
    assert!(reason.contains("orders/0"), "{reason:?}");
    assert!(
        reason.contains("sha256 mismatch"),
        "the reason must say the archive was found WRONG, not merely unchecked: {reason:?}"
    );
}
