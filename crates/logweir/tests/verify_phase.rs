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
/// null. Also proves `consume_all` still runs (and still applies the
/// mapping) on the consume-only branch.
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
        out.integrity.partial_reason.as_deref(),
        Some("kbak level below the gate")
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

/// THE topic-rename mapping test for `consume_all`. `MapReader` registers the
/// ARCHIVE-side name "orders" as present but EMPTY, and the mapped TARGET-side
/// name "drill-orders" with the real, matching records. If `consume_all` ever
/// read `sel.topic` ("orders") directly instead of `mapping[&sel.topic]`
/// ("drill-orders"), the canary would see 50 archive fingerprints against 0
/// consumed records — every one reported "absent from the target" — and this
/// assertion would fail. This is where the mapping is applied for the canary
/// comparison: `crates/logweir/src/drill/phase7_verify.rs`'s `consume_all`.
#[test]
fn consume_all_reads_the_mapped_target_topic_never_the_archive_name() {
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

/// Proves `fingerprints_for`, `consume_all` and `classify_parity_all` all
/// aggregate ACROSS every entry in `sel`/`mapping` rather than only the
/// first — a mutant that stopped after the first selection, or that folded
/// `classify_parity` over only one mapped topic, would still pass every
/// single-topic test above (every one of them names exactly one topic) but
/// must fail this one.
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

    let (archive_o, cons_o) = fixtures::matching_pair(25);
    let (archive_p, cons_p) = fixtures::matching_pair(25);

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
    // 25 + 25: wrong (25) if either `fingerprints_for` or `consume_all` ever
    // stopped after the first selection.
    assert_eq!(out.integrity.records_sampled, 50);
    assert_eq!(out.integrity.records_sampled_matching, 50);
    assert_eq!(out.records_restored, 50);
    // One entry per mapped topic: wrong (missing one) if `classify_parity_all`
    // ever folded over only the first mapping entry.
    let deviated = &out.topic_parity.intentionally_deviated;
    assert!(deviated.iter().any(|s| s.starts_with("drill-orders:")));
    assert!(deviated.iter().any(|s| s.starts_with("drill-payments:")));
}
