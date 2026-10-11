mod fixtures; // crates/logweir/tests/fixtures/mod.rs — Task 14 step 5c
mod support;
use logweir::drill::phase7_verify::{classify_parity, compare, ParityClasses};
use logweir_core::outcome::{IntegrityLevel, IntegrityResult};
use logweir_core::spec::Anchor;

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
    assert!(mismatches[0].contains("offset")); // engine-token-ok: asserts on `compare`'s own mismatch wording, not a kafka-backup CLI subcommand
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
///
/// **SHAPE ONLY — this is not coverage of compacted topics.** It asserts over
/// a hand-built `VerifyOutcome`, because `phase7_verify::run` has no reachable
/// path from a compacted target to `Partial` (module doc; and
/// `IntegrityResult::Partial`'s own doc comment).
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
///
/// This is the SCRATCH row: a scratch drill leaves nothing
/// `not_reconstructed`.
#[test]
fn scratch_deviations_are_intentional_and_anything_else_is_not() {
    let ParityClasses {
        intended,
        unexpected,
        not_reconstructed,
        not_assessed,
    } = classify_parity(
        logweir_core::spec::TargetMode::Scratch,
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
        /*src_partitions*/ Some(3),
        /*tgt_partitions*/ 3,
        /*src_rf*/ Some(3),
        /*tgt_rf*/ 1,
    );
    assert!(
        not_assessed.is_empty(),
        "both counts are recorded: {not_assessed:?}"
    );
    assert!(
        not_reconstructed.is_empty(),
        "a scratch drill's deviations are intended, never not reconstructed: {not_reconstructed:?}"
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

// Direct `compare`/`classify_parity` tests.

/// `compare` must match by the `x-original-offset` HEADER, not the consumed
/// record's own target-side offset: a restored topic's offsets need not start
/// where the source's did. Here the two DIFFER: the record's own offset is 99,
/// but its header names its TRUE original archive offset, 5.
#[test]
fn compare_matches_by_the_original_offset_header_not_the_targets_own_offset() {
    // The `x-original-offset` header is part of the record's OWN content —
    // `logweir_kafka::fingerprint::record_fingerprint` hashes every header,
    // and both sides of a genuine backup/restore round trip carry the same
    // header set (see `fixtures::matching_pair`) — so it is included on
    // BOTH the archive fingerprint and the consumed record here, exactly as
    // it would be in a real restored record.
    // The header value is the engine's 8-byte little-endian i64
    // `\x05\x00\x00\x00\x00\x00\x00\x00` (guard **G-HDR**), never ASCII `b"5"`,
    // which `decode_original_offset` answers `None` for, falling back to the
    // target's own offset and never exercising the decode.
    let headers = vec![(
        "x-original-offset".to_string(),
        Some(fixtures::le_offset(5)),
    )];
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

/// A source config key ABSENT from the target's config map entirely (not
/// merely present-with-a-different-value) is still a divergence:
/// `unwrap_or(true)` is the "absent ≠ agreement" rule.
#[test]
fn a_source_config_key_absent_from_the_target_is_reported_not_silently_agreed() {
    let ParityClasses {
        intended,
        unexpected,
        ..
    } = classify_parity(
        logweir_core::spec::TargetMode::Scratch,
        &fixtures::source_configs(&[("max.message.bytes", "1048576")]),
        &BTreeMap::new(),
        Some(3),
        3,
        Some(1),
        1,
    );
    assert!(intended.is_empty());
    assert_eq!(unexpected, vec!["max.message.bytes".to_string()]);
}

/// The POSITIVE case for partition-count divergence: spec §9.3 phase 7(d)
/// requires `partition_count` reported when the counts differ.
#[test]
fn a_partition_count_divergence_is_reported_as_intended() {
    let ParityClasses {
        intended,
        unexpected,
        ..
    } = classify_parity(
        logweir_core::spec::TargetMode::Scratch,
        &BTreeMap::new(),
        &BTreeMap::new(),
        Some(3),
        6,
        Some(1),
        1,
    );
    assert!(intended.contains(&"partition_count".to_string()));
    assert!(unexpected.is_empty());
}

// Tests that pin `run`'s own call sites: the topic-rename mapping and the
// empty-selection/empty-mapping guards, which the direct tests above cannot
// see.
use logweir::drill::phase7_verify::run;
use logweir::drill::DrillError;
use logweir_core::engine::{
    BackupSetFacts, BackupSetRef, EngineError, PartitionFacts, RecordFingerprint, RestorePlan,
    SampleSelection, SegmentFacts, StorageUrl, TopicFacts, WindowFloorSource,
};
use logweir_kafka::reader::{ClusterReader, ConsumedRecord, KafkaError, TopicMeta};
use std::collections::BTreeMap;
use support::doubles::{Engine, Reader, TopicData};

/// The instants every hand-built `SegmentFacts` in this file spans, and every
/// `SampleSelection` here asks for.
///
/// It MUST be the same pair of instants as `plan_orders_to_drill_orders`'s
/// `time_window`: guard **G-WIN**'s second half bounds the restored count by
/// the segments the manifest places inside the PLAN's window, so a fixture
/// whose segments sit outside that window proves a bound of `[0, 0]`.
/// `2026-08-29T00:00:00Z` and `2026-08-30T02:00:00Z`.
const WINDOW: (i64, i64) = (1_787_961_600_000, 1_788_055_200_000);
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
        anchor: Anchor::Head,
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
        target_auth: logweir_core::engine::AuthRender::Plaintext,
        topic_mapping: fixtures::mapping("orders", "drill-orders"),
        time_window: (
            fixtures::ts("2026-08-29T00:00:00Z"),
            fixtures::ts("2026-08-30T02:00:00Z"),
        ),
        window_floor_source: WindowFloorSource::ArchiveManifest,
        source_partitions: Default::default(),
        default_replication_factor: 1,
        checkpoint_state: "/tmp/logweir/checkpoint.json".into(),
        checkpoint_interval_secs: 30,
        offset_report: "/var/lib/logweir/01J9X/offsets.json".into(),
    }
}

/// The same plan with BOTH ends of its window moved ONE MILLISECOND inside the
/// segment every fixture here spans — which makes guard **G-WIN**'s
/// restored-count bound deliberately non-binding, `[0, Σ record_count]`
/// instead of `[Σ, Σ]`, for the whole drill and for every partition.
///
/// Every test that reaches for this one is about a SHORT or EMPTY read-back,
/// modelled by a target high watermark as short as the read-back. Against a
/// wholly-inside segment the count bound would fire too, a second,
/// independent cause of `Fail`, and the test would no longer pin its own
/// property. A segment whose first or last record lies inside the window
/// proves that record restored, so the floor moves too: a segment that covers
/// the window from both sides proves no record inside it, and the count check
/// is silent here BY CONSTRUCTION. The selections' own `window` is untouched.
fn with_window_inside_the_segment(mut plan: RestorePlan) -> RestorePlan {
    plan.time_window.0 += chrono::Duration::milliseconds(1);
    plan.time_window.1 -= chrono::Duration::milliseconds(1);
    plan
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
    let reader = Reader::target(topics);
    let engine = Engine::new()
        .describing(facts.clone())
        .fingerprinting(archive);
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
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
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
    // No key diverged; the one entry is the fail-safe marker of a run whose
    // coverage is UNKNOWN (no bound receipt), which an older reader must never
    // read as parity.
    assert_eq!(
        out.topic_parity.unexpected_divergence,
        vec!["drill-orders: configuration not assessed (unknown)".to_string()]
    );
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

/// A real 0.21.0 manifest stores each segment's digest as BARE HEX
/// (`"sha256": "27f6c448…"`), while `logweir_core::ids::sha256_prefixed`
/// produces `sha256:<hex>`; the segment lane must accept the engine's
/// spelling or it can never return `Verified` against a real archive.
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
    let reader = Reader::target(topics);
    let engine = Engine::new()
        .describing(facts.clone())
        .fingerprinting(archive);
    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel_orders(),
        &fixtures::mapping("orders", "drill-orders"),
        &plan_orders_to_drill_orders(),
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
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
    let reader = Reader::target(topics);
    let engine = Engine::new()
        .describing(facts.clone())
        .fingerprinting(archive);
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
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
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
    let reader = Reader::target(topics);
    let engine = Engine::new()
        .describing(facts.clone())
        .fingerprinting(archive);
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
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
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
/// `partial_reason` is asserted by CONTAINMENT: every reason is prefixed with
/// the selection it belongs to, so one selection's downgrade cannot speak for
/// another, and the engine's own words are carried through verbatim. The
/// drill reaches `Pass` only because the target gave back at least the 50
/// records the manifest claims: consume-only is a weaker CLAIM (`level` says
/// so, and `pass_rate` stays null), never a weaker CHECK.
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
    let reader = Reader::target(topics);
    let engine = Engine::new()
        .describing(facts.clone())
        .unsupported("kbak level below the gate");
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
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
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
    let engine = Engine::new()
        .describing(facts.clone())
        .fingerprinting(vec![]);
    let reader = Reader::target(BTreeMap::new());
    let mapping = fixtures::mapping("orders", "drill-orders");
    let plan = plan_orders_to_drill_orders();

    let err = run(
        &engine,
        &reader,
        &store,
        &facts,
        &[],
        &mapping,
        &plan,
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
    )
    .unwrap_err();
    assert!(matches!(err, DrillError::Operational(_)));
    assert!(err.to_string().contains("zero sample selections"));
}

/// THE topic-rename mapping test for `verdict_for_selection`. The reader
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
    let reader = Reader::target(topics);
    let engine = Engine::new()
        .describing(facts.clone())
        .fingerprinting(archive);
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
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
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
    let reader = Reader::target(topics);
    let engine = Engine::new()
        .describing(facts.clone())
        .fingerprinting(archive);
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
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
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

/// A `DataEngine` whose `fingerprints` answers PER-TOPIC, unlike the
/// `fingerprinting` engines above (one constant list regardless of the selection —
/// adequate for every single-topic test above, but unable to distinguish
/// "two selections, each contributing its own fingerprints" from "one
/// selection's fingerprints returned twice"). Local to this one test.
fn multi_topic_engine(
    facts: BackupSetFacts,
    by_topic: BTreeMap<String, Vec<RecordFingerprint>>,
) -> Engine<'static> {
    Engine::new()
        .describing(facts)
        .on_fingerprints(move |s| Ok(by_topic.get(&s.topic).cloned().unwrap_or_default()))
}

/// Like `fixtures::matching_pair`, but the key/value bytes are distinguished
/// by `tag`: `matching_pair` builds byte-IDENTICAL records for every topic,
/// so a cross-topic collision is invisible with it. A record built with
/// `tag = "payments"` can never fingerprint-match an archive entry built with
/// `tag = "orders"`.
fn distinct_matching_pair(tag: &str, n: usize) -> (Vec<RecordFingerprint>, Vec<ConsumedRecord>) {
    let mut arch = Vec::with_capacity(n);
    let mut cons = Vec::with_capacity(n);
    for i in 0..n {
        let off = i as i64;
        let headers = vec![(
            "x-original-offset".to_string(),
            Some(fixtures::le_offset(off)),
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

    // DISTINCT content per topic (see `distinct_matching_pair`).
    let (archive_o, cons_o) = distinct_matching_pair("orders", 25);
    let (archive_p, cons_p) = distinct_matching_pair("payments", 25);

    let mut by_topic = BTreeMap::new();
    by_topic.insert("orders".to_string(), archive_o);
    by_topic.insert("payments".to_string(), archive_p);
    let engine = multi_topic_engine(facts.clone(), by_topic);

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
    let reader = Reader::target(topics);

    let sel = vec![
        SampleSelection {
            set: BackupSetRef {
                backup_id: "backup-verify-test".into(),
                manifest_key: "backup-verify-test/manifest.json".into(),
            },
            topic: "orders".into(),
            partition: 0,
            anchor: Anchor::Head,
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
            anchor: Anchor::Head,
            count: 25,
            window: WINDOW,
        },
    ];
    let mut mapping = BTreeMap::new();
    mapping.insert("orders".to_string(), "drill-orders".to_string());
    mapping.insert("payments".to_string(), "drill-payments".to_string());
    let mut plan = plan_orders_to_drill_orders();
    plan.topic_mapping = mapping.clone();

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel,
        &mapping,
        &plan,
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
    )
    .unwrap();

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

/// Two mapped topics, one of them restored to ZERO records. `ConsumedRecord`
/// carries no topic field and every partition starts at offset 0, so a
/// `compare()` over both topics' records pooled would let `payments`'s healthy
/// records satisfy `orders`'s archive lookups and sign `integrity: pass` for a
/// topic never restored. Reconciling per selection, every `orders` archive
/// fingerprint is "present in the archive, absent from the target".
///
/// The content COLLIDES on purpose (`fixtures::matching_pair` for both
/// topics): with distinct content, pooling can only turn a match into a
/// mismatch, so the numbers would come out the same under both
/// implementations. Re-pooling turns this test's `Fail`/`0.5` into
/// `Pass`/`1.0`.
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
    // SAME `k{i}`/`v{i}` bytes for every topic, which is what makes this test
    // tell a pooled implementation from a per-selection one.
    let (archive_o, _cons_o_unused) = fixtures::matching_pair(25);
    let (archive_p, cons_p) = fixtures::matching_pair(25);

    let mut by_topic = BTreeMap::new();
    by_topic.insert("orders".to_string(), archive_o);
    by_topic.insert("payments".to_string(), archive_p);
    let engine = multi_topic_engine(facts.clone(), by_topic);

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
    let reader = Reader::target(topics);

    let sel = vec![
        SampleSelection {
            set: BackupSetRef {
                backup_id: "backup-verify-test".into(),
                manifest_key: "backup-verify-test/manifest.json".into(),
            },
            topic: "orders".into(),
            partition: 0,
            anchor: Anchor::Head,
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
            anchor: Anchor::Head,
            count: 25,
            window: WINDOW,
        },
    ];
    let mut mapping = BTreeMap::new();
    mapping.insert("orders".to_string(), "drill-orders".to_string());
    mapping.insert("payments".to_string(), "drill-payments".to_string());
    let mut plan = plan_orders_to_drill_orders();
    plan.topic_mapping = mapping.clone();

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel,
        &mapping,
        &plan,
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
    )
    .unwrap();

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

/// `orders`'s `engine.fingerprints` returns `Ok(vec![])` (no error, no
/// `Unsupported`) for a topic restored to zero, beside a healthy `payments`
/// selection. A guard on the AGGREGATE `sampled == 0` would report `Pass`;
/// ANY selection with zero of its OWN archive fingerprints disqualifies the
/// result from `Pass`, whatever a sibling contributed.
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
    let engine = multi_topic_engine(facts.clone(), by_topic);

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
    let reader = Reader::target(topics);

    let sel = vec![
        sel_orders().into_iter().next().unwrap(),
        SampleSelection {
            set: BackupSetRef {
                backup_id: "backup-verify-test".into(),
                manifest_key: "backup-verify-test/manifest.json".into(),
            },
            topic: "payments".into(),
            partition: 0,
            anchor: Anchor::Head,
            count: 25,
            window: WINDOW,
        },
    ];
    let mut mapping = fixtures::mapping("orders", "drill-orders");
    mapping.insert("payments".to_string(), "drill-payments".to_string());
    // The count bound is deliberately non-binding here — see
    // `with_window_inside_the_segment`.
    let mut plan = with_window_inside_the_segment(plan_orders_to_drill_orders());
    plan.topic_mapping = mapping.clone();

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel,
        &mapping,
        &plan,
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
    )
    .unwrap();

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

/// A byte-fingerprint comparison that samples ZERO
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
    let reader = Reader::target(topics);
    let engine = Engine::new()
        .describing(facts.clone())
        // Ok(vec![]) — support IS present, the archive simply returned
        // nothing to compare.
        .fingerprinting(vec![]);
    let mapping = fixtures::mapping("orders", "drill-orders");
    // The count bound is deliberately non-binding here — see
    // `with_window_inside_the_segment`.
    let plan = with_window_inside_the_segment(plan_orders_to_drill_orders());

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel_orders(),
        &mapping,
        &plan,
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
    )
    .unwrap();

    assert_eq!(out.integrity.level, IntegrityLevel::ByteFingerprint);
    assert_eq!(
        out.integrity.result,
        IntegrityResult::Partial,
        "zero sampled records must never report Pass, and is a different case from ConsumeOnly"
    );
    // The reason is asserted by CONTENT, not `is_some()`: "compared nothing"
    // and "compared less than claimed" are different findings an auditor must
    // tell apart in the signed document.
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
/// Dropping `records_restored >= claimed` from the consume-only lane's
/// positive predicate would let "compared less than claimed" reach `Pass`:
/// the manifest claims 50 records for this selection and the target hands
/// back 5.
#[test]
fn a_short_read_back_on_the_consume_only_lane_is_unverified_not_a_smaller_successful_sample() {
    let (store, sha) = store_with_matching_segment();
    let facts = facts_with_segment(&sha); // one segment, record_count 50
    let (_, mut cons) = fixtures::matching_pair(50);
    cons.truncate(5); // 90% of the claimed records never came back

    let engine = lane_engine(
        facts.clone(),
        [(
            "orders/0".to_string(),
            Err("kbak level 1 (pre-0.21)".to_string()),
        )]
        .into_iter()
        .collect(),
    );
    let reader = Reader::target(
        [target("drill-orders", vec![(0, 5)], cons)]
            .into_iter()
            .collect(),
    );

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel_orders(), // count 50
        &fixtures::mapping("orders", "drill-orders"),
        // The count bound is deliberately non-binding here — see
        // `with_window_inside_the_segment`.
        &with_window_inside_the_segment(plan_orders_to_drill_orders()),
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
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
    // "supports at least 50", not "claims 50": the figure is a lower bound,
    // and a straddling segment would not contribute to it.
    assert!(
        reason.contains("gave back 5 records") && reason.contains("supports at least 50"),
        "the reason must name BOTH the shortfall and the claim, so an auditor can size it: \
         {reason:?}"
    );
    assert!(
        !reason.contains("straddling"),
        "this selection's segment lies wholly inside the window; the message must not \
         disclose a straddler that is not there: {reason:?}"
    );
}

/// The per-partition consume cap.
/// `SpyReader` records every `max` argument `consume_range` actually
/// received; the target genuinely holds 40 records but `sel_orders()` caps
/// `count` at 50 (its own fixture value) — this test instead builds a
/// selection with a SMALL count against a LARGE target, so a mutant
/// replacing `s.count` with (say) `usize::MAX` at the call site changes what
/// is observed here, not just how many records happen to exist.
struct SpyReader {
    inner: Reader,
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
    /// Guard **G-TS**: forwarded like every other read, so the spy
    /// keeps observing exactly what the inner reader answers.
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
        self.inner.broker_configs()
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
        inner: Reader::target(topics),
        seen_max: std::sync::Mutex::new(vec![]),
    };
    // ...but only 7 archive fingerprints were sampled.
    let (archive, _) = fixtures::matching_pair(7);
    let engine = Engine::new()
        .describing(facts.clone())
        .fingerprinting(archive);
    let mapping = fixtures::mapping("orders", "drill-orders");
    let plan = plan_orders_to_drill_orders();
    let mut sel = sel_orders();
    sel[0].count = 7;

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel,
        &mapping,
        &plan,
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
    )
    .unwrap();

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

/// `records_restored` is "the count of records actually consumed, not
/// `matched + mismatched`" (addendum A1). Here the target holds MORE records
/// than the archive sampled (8 consumed vs. 5 archived), so the two figures
/// differ.
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
    let reader = Reader::target(topics);
    let engine = Engine::new()
        .describing(facts.clone())
        .fingerprinting(archive);
    let mapping = fixtures::mapping("orders", "drill-orders");
    let plan = plan_orders_to_drill_orders();
    let mut sel = sel_orders();
    sel[0].count = 8;

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel,
        &mapping,
        &plan,
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
    )
    .unwrap();

    assert_eq!(out.integrity.records_sampled, 5);
    assert_eq!(out.integrity.records_sampled_matching, 5);
    assert_eq!(out.integrity.mismatches, 0);
    assert_eq!(
        out.records_restored, 8,
        "records_restored must be the CONSUMED count (8), not matched+mismatched (5)"
    );
}

/// ONE topic ("orders") across TWO partitions, each correctly restored with
/// DISTINCT content. Pooled across selections, partition 1's records would
/// displace partition 0's in one shared `by_offset` map (both start at offset
/// 0), fabricating mismatches on a healthy restore.
fn per_partition_engine(
    facts: BackupSetFacts,
    by_partition: BTreeMap<i32, Vec<RecordFingerprint>>,
) -> Engine<'static> {
    Engine::new()
        .describing(facts)
        .on_fingerprints(move |s| Ok(by_partition.get(&s.partition).cloned().unwrap_or_default()))
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
            Some(fixtures::le_offset(off)),
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
    let engine = per_partition_engine(facts.clone(), by_partition);

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
    let reader = Reader::target(topics);

    let sel = vec![
        SampleSelection {
            set: BackupSetRef {
                backup_id: "backup-verify-test".into(),
                manifest_key: "backup-verify-test/manifest.json".into(),
            },
            topic: "orders".into(),
            partition: 0,
            anchor: Anchor::Head,
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
            anchor: Anchor::Head,
            count: 10,
            window: WINDOW,
        },
    ];
    let mapping = fixtures::mapping("orders", "drill-orders");
    let mut plan = plan_orders_to_drill_orders();
    plan.topic_mapping = mapping.clone();

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel,
        &mapping,
        &plan,
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
    )
    .unwrap();

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

// THE FIVE REPRODUCTIONS: phase 7 signing `Pass` over a restore it had not
// checked, one named test per shape, each driven through the FULL `run`
// because every one was in `run`'s own wiring rather than in a leaf.

/// A `DataEngine` answering per SELECTION (`"topic/partition"`), able to hand
/// back either real fingerprints or `EngineError::Unsupported` — the mixed
/// case a `fingerprinting` engine (one answer for the whole run) and `multi_topic_engine`
/// (fingerprints only, never `Unsupported`) between them cannot express, and
/// the case reproductions 1, 2, 3 and 5 all turn on.
///
/// `by_selection` holds `Ok(fingerprints)`, or `Err(reason)` rendered as
/// `EngineError::Unsupported(reason)` — a pre-0.21 archive or a KBAK level-1
/// segment, which is a NORMAL production condition.
fn lane_engine(
    facts: BackupSetFacts,
    by_selection: BTreeMap<String, Result<Vec<RecordFingerprint>, String>>,
) -> Engine<'static> {
    Engine::new().describing(facts).on_fingerprints(move |s| {
        match by_selection.get(&format!("{}/{}", s.topic, s.partition)) {
            Some(Ok(fp)) => Ok(fp.clone()),
            Some(Err(reason)) => Err(EngineError::Unsupported(reason.clone())),
            None => Ok(vec![]),
        }
    })
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
        anchor: Anchor::Head,
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

// REPRODUCTION 1: one of two topics restored to ZERO records, on a legacy
// archive (a pre-0.21 archive or a KBAK level-1 segment, a NORMAL production
// condition). `verdict_for_selection`'s consume-only arm constructs
// `Evidence` exactly as the byte-fingerprint arm does, and "the target
// partition gave back zero records" is `Unverified`, which `roll_up` refuses
// to fold into a `Pass`.

/// Reproduction 1. A legacy archive downgrades BOTH selections to
/// consume-only; `payments` restored healthily, `orders` restored to zero.
/// The drill must not report `Pass`, and must name `orders/0`.
#[test]
fn one_of_two_topics_restored_to_zero_records_cannot_pass_on_the_consume_only_lane() {
    let (facts, store) = two_topic_facts_and_store(25);
    let (_, cons_payments) = distinct_matching_pair("payments", 25);

    let engine = lane_engine(
        facts.clone(),
        [
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
    );

    let reader = Reader::target(
        [
            // Never restored: the topic exists, and it is empty.
            target("drill-orders", vec![(0, 0)], vec![]),
            target("drill-payments", vec![(0, 25)], cons_payments),
        ]
        .into_iter()
        .collect(),
    );

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &[sel_for("orders", 0, 25), sel_for("payments", 0, 25)],
        &two_topic_mapping(),
        // The count bound is deliberately non-binding here — see
        // `with_window_inside_the_segment`.
        &with_window_inside_the_segment(two_topic_plan()),
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
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
    // `sampled = 0` because the consume-only lane reconciles nothing, and
    // `records_restored = 25` from the healthy sibling alone: not a pass.
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

// REPRODUCTION 2: `orders` restored 100% CORRUPT and `payments` not restored
// at all. `compare()` is called from `verdict_for_selection`, which is handed
// ONE selection, so two selections' records can never reconcile against each
// other; both defects are detected, separately, and both are `Failed`.

/// Reproduction 2. Corruption and absence are each detected on their own
/// merits and neither can be papered over by the other.
#[test]
fn a_wholly_corrupt_topic_beside_an_unrestored_one_fails_and_never_reconciles_against_it() {
    let (facts, store) = two_topic_facts_and_store(25);

    // `fixtures::matching_pair` builds byte-IDENTICAL records for every topic
    // and both partitions start at offset 0, so a pooled `compare()` COULD
    // satisfy one topic's archive from the other's records.
    let (archive_orders, mut cons_orders) = fixtures::matching_pair(25);
    let (archive_payments, _cons_payments_never_restored) = fixtures::matching_pair(25);
    // 100% corrupt: every restored value differs from what the archive
    // fingerprint was computed over.
    for (i, r) in cons_orders.iter_mut().enumerate() {
        r.value = Some(format!("corrupt-{i}").into_bytes());
    }

    let engine = lane_engine(
        facts.clone(),
        [
            ("orders/0".to_string(), Ok(archive_orders)),
            ("payments/0".to_string(), Ok(archive_payments)),
        ]
        .into_iter()
        .collect(),
    );

    let reader = Reader::target(
        [
            target("drill-orders", vec![(0, 25)], cons_orders),
            // Not restored at all.
            target("drill-payments", vec![(0, 0)], vec![]),
        ]
        .into_iter()
        .collect(),
    );

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &[sel_for("orders", 0, 25), sel_for("payments", 0, 25)],
        &two_topic_mapping(),
        &two_topic_plan(),
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
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

// REPRODUCTION 3: ZERO records consumed from a sampled partition. `Pass` is
// COMPUTED from positive evidence, never initialised.

/// Reproduction 3. The sampled partition gives back nothing, while a
/// DIFFERENT partition of the same target topic holds data — so the drill has
/// an honest `newest_restored_ts_ms` to report and cannot be dismissed as
/// "the whole target was empty". The sample still proved nothing.
#[test]
fn zero_records_consumed_from_a_sampled_partition_is_never_a_pass() {
    let (store, sha) = store_with_matching_segment();
    let mut facts = facts_with_segment(&sha);
    // The manifest lists partition 1 too (the same segment shape), so
    // the records the target holds there are records the archive accounts
    // for; a target partition the manifest has no partition for is now a
    // count-bound failure of its own, which is not what this test pins.
    let mut p1 = facts.topics[0].partitions[0].clone();
    p1.partition_id = 1;
    facts.topics[0].partitions.push(p1);
    // Partition 1 of the SAME target topic was restored; partition 0 — the
    // one this drill actually sampled — was not.
    let (_, cons_p1) = distinct_pair_for_partition(1, 5);

    let engine = lane_engine(
        facts.clone(),
        [(
            "orders/0".to_string(),
            Err("kbak level 1 (pre-0.21)".to_string()),
        )]
        .into_iter()
        .collect(),
    );
    let reader = Reader::target(
        [target("drill-orders", vec![(0, 0), (1, 5)], cons_p1)]
            .into_iter()
            .collect(),
    );

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel_orders(),
        &fixtures::mapping("orders", "drill-orders"),
        // The count bound is deliberately non-binding here — see
        // `with_window_inside_the_segment`.
        &with_window_inside_the_segment(plan_orders_to_drill_orders()),
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
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

// REPRODUCTION 4: a SHORT archive fingerprint list over a 96%-lossy
// partition (1 fingerprint where the manifest claims 25). Short IS
// unverified: what came back is held against `min(sel.count, Σ
// record_count)`, both already-signed figures, and `pass_rate_measured` is a
// ratio over the WHOLE sample or it is null.

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

    let engine = lane_engine(
        facts.clone(),
        [
            ("orders/0".to_string(), Ok(archive_orders)),
            ("payments/0".to_string(), Ok(archive_payments)),
        ]
        .into_iter()
        .collect(),
    );
    let reader = Reader::target(
        [
            target("drill-orders", vec![(0, 1)], cons_orders),
            target("drill-payments", vec![(0, 25)], cons_payments),
        ]
        .into_iter()
        .collect(),
    );

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &[sel_for("orders", 0, 25), sel_for("payments", 0, 25)],
        &two_topic_mapping(),
        // The count bound is deliberately non-binding here — see
        // `with_window_inside_the_segment`.
        &with_window_inside_the_segment(two_topic_plan()),
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
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
    // The byte-fingerprint lane's own wording, pinned as a phrase
    // rather than as a bare "25" — a lone digit is satisfied by any number in
    // any sentence, and the message this arm is about is the one an auditor
    // reads out of the SIGNED `partial_reason`.
    assert!(
        reason.contains(
            "the archive returned 1 fingerprints where the manifest supports at least 25 \
             for this selection"
        ),
        "the reason must name what the manifest supports, so an auditor can see the size of \
         the shortfall: {reason:?}"
    );
}

// REPRODUCTION 5: one selection's `Unsupported` archive must not erase the
// byte-level claim for every other selection. The mode is PER SELECTION: a
// legacy segment downgrades only its own partition; `level` still reports
// the WEAKEST claim any selection could support, but the counters and
// `partial_reason` preserve what was established.

/// Reproduction 5. A downgrade names itself and costs the drill its
/// whole-run LEVEL; it does not cost a sibling selection its CHECK.
#[test]
fn one_selections_unsupported_archive_never_erases_another_selections_byte_level_claim() {
    let (facts, store) = two_topic_facts_and_store(25);
    let (_, cons_orders) = distinct_matching_pair("orders", 25);
    let (archive_payments, cons_payments) = distinct_matching_pair("payments", 25);

    let engine = lane_engine(
        facts.clone(),
        [
            // A legacy segment in ONE partition.
            (
                "orders/0".to_string(),
                Err("kbak level 1 (pre-0.21)".to_string()),
            ),
            ("payments/0".to_string(), Ok(archive_payments)),
        ]
        .into_iter()
        .collect(),
    );
    let reader = Reader::target(
        [
            target("drill-orders", vec![(0, 25)], cons_orders),
            target("drill-payments", vec![(0, 25)], cons_payments),
        ]
        .into_iter()
        .collect(),
    );

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &[sel_for("orders", 0, 25), sel_for("payments", 0, 25)],
        &two_topic_mapping(),
        &two_topic_plan(),
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
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

// Two outcomes that are drill results (exit 2, signed scorecard), not
// `DrillError::Operational`; see `verdict_for_selection`'s "Severity" doc
// paragraph. Each asserts `Ok` explicitly.

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

    let engine = Engine::new()
        .describing(facts.clone())
        .fingerprinting(archive);
    let reader = Reader::target(
        [target("drill-orders", vec![(0, 50)], cons)]
            .into_iter()
            .collect(),
    );
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
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
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
/// checked: a skip is coverage the drill did not obtain, never a pass.
#[test]
fn a_pre_0_21_segment_with_no_sha256_is_partial_never_a_silent_pass() {
    let (store, _sha) = store_with_matching_segment();
    // The manifest carries no sha256 for this segment at all.
    let facts = facts_with_segment("");
    let (archive, cons) = fixtures::matching_pair(50);

    let engine = Engine::new()
        .describing(facts.clone())
        .fingerprinting(archive);
    let reader = Reader::target(
        [target("drill-orders", vec![(0, 50)], cons)]
            .into_iter()
            .collect(),
    );

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel_orders(),
        &fixtures::mapping("orders", "drill-orders"),
        &plan_orders_to_drill_orders(),
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
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

/// `verdict_for_selection` calls `segment_evidence` BEFORE it matches on the
/// archive mode, so the segment sha256 check runs on every lane, consume-only
/// included: a downgraded selection whose segment bytes do not match its own
/// manifest is `Evidence::Failed`. The records reconcile perfectly here, so
/// the `Fail` is the segment lane's alone, and it must not read `Partial`:
/// Partial and Fail are not interchangeable in a signed document.
#[test]
fn a_consume_only_selection_with_a_corrupt_segment_fails_the_drill_never_merely_partial() {
    let (store, _real_sha) = store_with_matching_segment();
    // The manifest claims a sha256 the stored segment bytes do not hash to.
    let facts = facts_with_segment(&format!("sha256:{}", "0".repeat(64)));
    let (_, cons) = fixtures::matching_pair(50);

    let engine = lane_engine(
        facts.clone(),
        [(
            "orders/0".to_string(),
            Err("kbak level 1 (pre-0.21)".to_string()),
        )]
        .into_iter()
        .collect(),
    );
    let reader = Reader::target(
        [target("drill-orders", vec![(0, 50)], cons)]
            .into_iter()
            .collect(),
    );

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel_orders(),
        &fixtures::mapping("orders", "drill-orders"),
        &plan_orders_to_drill_orders(),
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
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

// THE PHASE-7 → PHASE-8 SEAM. `roll_up` withholds `pass_rate_measured` when
// part of the sample was never reconciled, so phase 8 must not recompute the
// ratio from the two counters: a signed `integrity.result: "partial"` beside
// `pass_rate_measured: 1.0` and `objectives.met: true` is the defect. The
// second test crosses the seam into the SIGNED BYTES.

/// A REAL phase-7 verdict for the shape that produces it: one selection
/// reconciles record-for-record, a second returns zero archive fingerprints.
/// The sample is therefore non-empty (`records_sampled: 25`) and every figure
/// in `25/25` is true — and the ratio over the WHOLE sample is still unknown,
/// so `roll_up` publishes no rate.
fn a_partial_verdict_over_a_partly_reconciled_sample() -> logweir_core::scorecard::Integrity {
    let (facts, store) = two_topic_facts_and_store(25);
    let (archive_orders, cons_orders) = distinct_matching_pair("orders", 25);
    let (_archive_payments, cons_payments) = distinct_matching_pair("payments", 25);

    let engine = lane_engine(
        facts.clone(),
        [
            ("orders/0".to_string(), Ok(archive_orders)),
            // Support IS present; the archive simply returned nothing to
            // compare for this selection. `Ok(vec![])`, never `Err`, so the
            // level stays `byte-fingerprint` and no downgrade is recorded.
            ("payments/0".to_string(), Ok(Vec::new())),
        ]
        .into_iter()
        .collect(),
    );
    let reader = Reader::target(
        [
            target("drill-orders", vec![(0, 25)], cons_orders),
            target("drill-payments", vec![(0, 25)], cons_payments),
        ]
        .into_iter()
        .collect(),
    );

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &[sel_for("orders", 0, 25), sel_for("payments", 0, 25)],
        &two_topic_mapping(),
        &two_topic_plan(),
        &logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        logweir_core::spec::TargetMode::Scratch,
    )
    .expect("a partly-reconciled sample is a DRILL RESULT, never an Err");

    assert_eq!(out.integrity.level, IntegrityLevel::ByteFingerprint);
    assert_eq!(out.integrity.result, IntegrityResult::Partial);
    assert_eq!(out.integrity.records_sampled, 25);
    assert_eq!(out.integrity.records_sampled_matching, 25);
    assert_eq!(
        out.integrity.pass_rate_measured, None,
        "phase 7 WITHHOLDS the rate when part of the sample was never reconciled"
    );
    out.integrity
}

/// Phase 8 consumes phase 7's rate as given. It does not recompute it, and it
/// does not answer `met` from a number phase 7 refused to publish.
#[test]
fn phase_8_never_republishes_a_pass_rate_phase_7_withheld() {
    let integ = a_partial_verdict_over_a_partly_reconciled_sample();
    let (outcome, objectives) = logweir::drill::phase8_score::decide(
        &fixtures::measured(1, 1, 0),
        &fixtures::objectives(900, 300, 1.0),
        &integ,
    );
    assert_eq!(outcome, logweir_core::outcome::Outcome::FailIntegrity);
    assert_eq!(
        objectives.met, None,
        "`met` is answered from the rate phase 7 published; a withheld rate makes the \
         aggregate verdict unmeasurable, never true"
    );
}

/// The same property asserted against the SIGNED BYTES, because that is the
/// artifact the claim is made in.
#[test]
fn the_signed_document_carries_no_pass_rate_beside_a_partial_verdict() {
    let integ = a_partial_verdict_over_a_partly_reconciled_sample();
    let (outcome, objectives) = logweir::drill::phase8_score::decide(
        &fixtures::measured(1, 1, 0),
        &fixtures::objectives(900, 300, 1.0),
        &integ,
    );

    let mut sc = fixtures::scorecard_pass();
    sc.integrity = integ;
    sc.outcome = outcome;
    sc.objectives = objectives;
    // Phase 7's integrity carries `integrity.verification`, a 1.4.0 field
    // (arm IV-1), so the document is the writer's version.
    sc.format_version = logweir_core::FORMAT_VERSION.into();

    let store = fixtures::recording_store();
    let signed = logweir::drill::phase8_score::run(
        &sc,
        &fixtures::good_signing_key().path,
        &store.inner,
        None,
    )
    .expect("a partial verdict is a signable document");

    // Parsed from `signed.bytes` — the exact bytes the signature covers.
    let doc: serde_json::Value = serde_json::from_slice(&signed.bytes).unwrap();
    assert_eq!(
        doc["integrity"]["result"], "partial",
        "the fixture must actually be the shape under test"
    );
    assert_eq!(
        doc["integrity"]["pass_rate_measured"],
        serde_json::Value::Null,
        "a signed `pass_rate_measured` beside `result: partial` tells an auditor the \
         drill reconciled the whole sample when it did not: {}",
        String::from_utf8_lossy(&signed.bytes)
    );
    assert_eq!(
        doc["objectives"]["met"],
        serde_json::Value::Null,
        "`met: true` beside a partial integrity result is the same overstatement one \
         field along"
    );
}

// Configuration parity is ASSESSED only where the capture was, and a refused
// TARGET read is recorded, not compared as empty.

/// The healthy baseline's reader, wrapped so the TARGET's DescribeConfigs is
/// REFUSED, as `RdKafkaReader::topic_configs` reports it.
fn refusing_target_configs(reader: Reader) -> Reader {
    reader.on_topic_configs(|topic| {
        Err(logweir_kafka::reader::empty_topic_config_answer(
            topic,
            &logweir_kafka::reader::TopicVisibility::Visible,
        ))
    })
}

/// The healthy baseline's target, with a target configuration that DIFFERS
/// from the source's on a key that is not intended (`min.insync.replicas`),
/// so a comparison that ran is visible in `unexpected_divergence`.
fn healthy_parts() -> (
    logweir_engine_oso::storage::Store,
    BackupSetFacts,
    Reader,
    Engine<'static>,
) {
    let (store, sha) = store_with_matching_segment();
    let mut facts = facts_with_segment(&sha);
    facts.topics[0]
        .configurations
        .insert("min.insync.replicas".into(), "2".into());
    let (archive, cons) = fixtures::matching_pair(50);
    let mut topics = BTreeMap::new();
    topics.insert(
        "drill-orders".to_string(),
        TopicData {
            end_offsets: vec![(0, 50)],
            configs: fixtures::target_configs(&[
                ("cleanup.policy", "delete"),
                ("retention.ms", "-1"),
                ("min.insync.replicas", "1"),
            ]),
            records: cons,
        },
    );
    let engine = Engine::new()
        .describing(facts.clone())
        .fingerprinting(archive);
    (store, facts, Reader::target(topics), engine)
}

/// A `SourceConfigCoverage` as a verified 1.1.0 receipt naming `orders` reads.
fn receipt_coverage(coverage: &str) -> logweir_core::backup_receipt::SourceConfigCoverage {
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
        "orders".to_string(),
        logweir_core::backup_receipt::TopicConfigCoverage {
            coverage: coverage.to_string(),
            reason: (coverage == "notCaptured").then(|| "manifestDiffers".to_string()),
            timestamp_type: None,
        },
    )]));
    logweir_core::backup_receipt::SourceConfigCoverage::from_receipt(&receipt)
}

/// **The parity arm that ignores coverage.** Every coverage that is not
/// `captured` — no receipt (`unknown`), a denied capture, a manifest that
/// disagreed — names the topic in `not_assessed`; `captured` is the only state
/// that yields `Some([])`, the "every topic assessed" claim. The archive's own
/// record is still compared either way: the difference it shows stays
/// reported, only its SILENCE is withdrawn.
///
/// Negative control: a `classify_parity_all` that stopped consulting
/// `coverage` (always `Some(vec![])`) fails every non-captured row below.
#[test]
fn configuration_parity_is_not_assessed_unless_the_source_capture_was() {
    for (coverage, want) in [
        (
            logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
            Some(vec!["drill-orders: configuration (unknown)".to_string()]),
        ),
        (
            receipt_coverage("captureDenied"),
            Some(vec![
                "drill-orders: configuration (captureDenied)".to_string()
            ]),
        ),
        (
            receipt_coverage("notCaptured"),
            Some(vec!["drill-orders: configuration (notCaptured)".to_string()]),
        ),
        (receipt_coverage("captured"), Some(vec![])),
    ] {
        let (store, facts, reader, engine) = healthy_parts();
        let out = run(
            &engine,
            &reader,
            &store,
            &facts,
            &sel_orders(),
            &fixtures::mapping("orders", "drill-orders"),
            &plan_orders_to_drill_orders(),
            &coverage,
            logweir_core::spec::TargetMode::Scratch,
        )
        .unwrap();
        assert_eq!(out.topic_parity.not_assessed, want);
        // The archive's own record is compared whatever the coverage, and a
        // topic that was not assessed ALSO carries its fail-safe marker there.
        let mut expected = vec!["drill-orders: min.insync.replicas".to_string()];
        for entry in want.iter().flatten() {
            let why = entry
                .strip_prefix("drill-orders: configuration (")
                .and_then(|r| r.strip_suffix(')'))
                .expect("the not_assessed shape");
            expected.push(format!("drill-orders: configuration not assessed ({why})"));
        }
        expected.sort();
        assert_eq!(out.topic_parity.unexpected_divergence, expected);
    }
}

/// **What a reader older than `not_assessed` sees.** For every coverage
/// state, a topic phase 7 did not assess leaves a non-empty
/// `unexpected_divergence`, the array older readers read, so no reader that
/// ignores `not_assessed` can take its silence for parity. Only `captured`
/// with a readable target and agreeing keys may leave the array empty.
///
/// Negative control: drop the marker from `classify_parity_all` and every
/// not-assessed row fails, the `targetReadDenied` one included.
#[test]
fn a_topic_that_was_not_assessed_is_never_silent_to_an_older_reader() {
    let quiet = |out: &logweir::drill::phase7_verify::VerifyOutcome, why: &str| {
        assert!(
            out.topic_parity.unexpected_divergence.contains(
                &logweir::drill::phase7_verify::not_assessed_marker("drill-orders", why)
            ),
            "{why}: an older reader would read this topic's parity as clean: {:?}",
            out.topic_parity.unexpected_divergence
        );
    };
    for (why, coverage) in [
        (
            "unknown",
            logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
        ),
        ("captureDenied", receipt_coverage("captureDenied")),
        ("notCaptured", receipt_coverage("notCaptured")),
    ] {
        let (store, facts, reader, engine) = healthy_parts();
        let out = run(
            &engine,
            &reader,
            &store,
            &facts,
            &sel_orders(),
            &fixtures::mapping("orders", "drill-orders"),
            &plan_orders_to_drill_orders(),
            &coverage,
            logweir_core::spec::TargetMode::Scratch,
        )
        .unwrap();
        quiet(&out, why);
    }
    let (store, facts, reader, engine) = healthy_parts();
    let out = run(
        &engine,
        &refusing_target_configs(reader),
        &store,
        &facts,
        &sel_orders(),
        &fixtures::mapping("orders", "drill-orders"),
        &plan_orders_to_drill_orders(),
        &receipt_coverage("captured"),
        logweir_core::spec::TargetMode::Scratch,
    )
    .unwrap();
    quiet(&out, "targetReadDenied");
}

/// **OD-7, as the owner ruled it.** `docs/stability.md` states both rulings
/// beside the MINOR/MAJOR rule, and the entry the follow-up ruling covers is
/// EXACTLY the shape phase 7 writes: the ruling names that string, so a
/// different marker would be an unruled change to an existing field's
/// content. No doc still calls the classification pending.
///
/// Negative controls: the old "OD-7, still pending" text restored in
/// `docs/stability.md`, a rule that loses either ruling, or a
/// `not_assessed_marker` of another shape, fails this test.
#[test]
fn the_stability_rule_carries_the_owners_od_7_rulings_for_the_entry_phase_7_writes() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let flat = |rel: &str| -> String {
        std::fs::read_to_string(root.join(rel))
            .unwrap_or_else(|e| panic!("{rel}: {e}"))
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    };
    let stability = flat("docs/stability.md");
    let rule = stability
        .split("## The signed formats")
        .next()
        .expect("docs/stability.md states the rule before its list of signed formats");
    for ruled in [
        "The owner's decision [OD-7](to-do/product-expansion.md#owner-decisions), taken on \
         2026-10-05, rules two cases MINOR",
        "**A `validate_invariants` arm that reads ONLY a new optional block is MINOR.** An arm \
         that changes how an existing field is judged stays MAJOR.",
        "Nothing else is ruled: any other change to an existing field's content is still a \
         MAJOR bump.",
    ] {
        assert!(
            rule.contains(ruled),
            "docs/stability.md's MINOR/MAJOR rule no longer says: {ruled}"
        );
    }
    let marker = logweir::drill::phase7_verify::not_assessed_marker("<target topic>", "<why>");
    let entry = format!(
        "**FX-4's entry `\"{marker}\"` in the existing `topic_parity.unexpected_divergence` is \
         MINOR**"
    );
    assert!(
        rule.contains(&entry),
        "OD-7's follow-up ruling covers the entry phase 7 writes, `{marker}`; \
         docs/stability.md's rule must name exactly that shape"
    );
    for doc in [
        "docs/stability.md",
        "docs/formats/drill-scorecard.md",
        "docs/formats/backup-receipt.md",
        "docs/formats/catalog-point.md",
        "docs/verify-a-scorecard.md",
        "MAINTAINERS.md",
    ] {
        let text = flat(doc);
        for stale in [
            "OD-7, still pending",
            "part of the owner's decision OD-7",
            "part of OD-7 too",
        ] {
            assert!(!text.contains(stale), "{doc} still says {stale:?}");
        }
    }
}

/// **A REFUSED read of the TARGET's configuration is named
/// `targetReadDenied`**, and the topic's keys are not compared at all;
/// partition count and replication factor still are.
///
/// Negative control: `reader.topic_configs(tgt).unwrap_or_default()` in phase
/// 7 reports `min.insync.replicas` as an unexpected divergence and names no
/// `not_assessed` entry, and this test fails.
#[test]
fn a_refused_target_configuration_read_is_not_assessed_never_compared_as_empty() {
    let (store, facts, reader, engine) = healthy_parts();
    let out = run(
        &engine,
        &refusing_target_configs(reader),
        &store,
        &facts,
        &sel_orders(),
        &fixtures::mapping("orders", "drill-orders"),
        &plan_orders_to_drill_orders(),
        &receipt_coverage("captured"),
        logweir_core::spec::TargetMode::Scratch,
    )
    .expect("a refused TARGET configuration read is a recorded fact, not an aborted drill");
    assert_eq!(
        out.topic_parity.not_assessed,
        Some(vec![
            "drill-orders: configuration (targetReadDenied)".to_string()
        ])
    );
    // No KEY was compared; the one entry is the fail-safe marker.
    assert_eq!(
        out.topic_parity.unexpected_divergence,
        vec!["drill-orders: configuration not assessed (targetReadDenied)".to_string()]
    );
    assert!(
        !out.topic_parity
            .intentionally_deviated
            .iter()
            .any(|d| d.contains("cleanup.policy") || d.contains("retention.ms")),
        "no configuration key was compared: {:?}",
        out.topic_parity.intentionally_deviated
    );
    assert!(
        out.topic_parity
            .intentionally_deviated
            .contains(&"drill-orders: replication_factor".to_string()),
        "metadata-derived parity is still assessed: {:?}",
        out.topic_parity.intentionally_deviated
    );
}

// The MODE decides the label. A scratch drill's deviations on the four kinds
// the restore's topic creation decides are INTENDED; a `newTopic` restore's
// are NOT RECONSTRUCTED and also UNEXPECTED, never intended, so a reader
// older than format 1.2.0 sees a divergence, not a label that calls lost
// compaction or replication factor 1 "intended".

use logweir_core::spec::TargetMode;

/// `classify_parity`'s inputs after the mode: source and target configs, the
/// source's partition count (as recorded) and the target's, the source's
/// replication factor (as recorded) and the target's.
type ParityInputs = (
    BTreeMap<String, String>,
    BTreeMap<String, String>,
    Option<i32>,
    i32,
    Option<i16>,
    i16,
);

/// One deviation kind, alone: the configs and counts that differ on `kind`
/// and on nothing else.
fn only(kind: &str) -> ParityInputs {
    let mut src = BTreeMap::new();
    let mut tgt = BTreeMap::new();
    let (mut src_partitions, mut src_rf) = (3, 3);
    match kind {
        "cleanup.policy" => {
            src.insert("cleanup.policy".to_string(), "compact".to_string());
            tgt.insert("cleanup.policy".to_string(), "delete".to_string());
        }
        "retention.ms" => {
            src.insert("retention.ms".to_string(), "604800000".to_string());
            tgt.insert("retention.ms".to_string(), "-1".to_string());
        }
        "partition_count" => src_partitions = 6,
        "replication_factor" => src_rf = 1,
        other => panic!("not a kind: {other}"),
    }
    // The target: three partitions at replication factor 3, so only `kind`
    // differs from the source.
    (src, tgt, Some(src_partitions), 3, Some(src_rf), 3)
}

/// Both modes for one kind, every list asserted EXACTLY: the label moves, the
/// deviation is never dropped, and the scratch label is today's.
fn assert_the_mode_decides_the_label(kind: &str) {
    let (src, tgt, sp, tp, sr, tr) = only(kind);
    let scratch = classify_parity(TargetMode::Scratch, &src, &tgt, sp, tp, sr, tr);
    assert_eq!(
        scratch,
        ParityClasses {
            intended: vec![kind.to_string()],
            unexpected: vec![],
            not_reconstructed: vec![],
            not_assessed: vec![],
        },
        "{kind}: a scratch drill's deviation is INTENDED, exactly as before FX-3"
    );
    let new_topic = classify_parity(TargetMode::NewTopic, &src, &tgt, sp, tp, sr, tr);
    assert_eq!(
        new_topic,
        ParityClasses {
            intended: vec![],
            unexpected: vec![kind.to_string()],
            not_reconstructed: vec![kind.to_string()],
            not_assessed: vec![],
        },
        "{kind}: a newTopic restore did NOT reconstruct it; it is never intended, and its \
         twin in `unexpected` is what a reader older than format 1.2.0 sees"
    );
}

#[test]
fn lost_compaction_is_intended_in_a_scratch_drill_and_not_reconstructed_in_a_new_topic_restore() {
    assert_the_mode_decides_the_label("cleanup.policy");
}

#[test]
fn a_changed_retention_is_intended_in_a_scratch_drill_and_not_reconstructed_in_a_new_topic_restore()
{
    assert_the_mode_decides_the_label("retention.ms");
}

#[test]
fn a_partition_count_divergence_is_intended_in_a_scratch_drill_and_not_reconstructed_in_a_new_topic_restore(
) {
    assert_the_mode_decides_the_label("partition_count");
}

#[test]
fn a_lower_replication_factor_is_intended_in_a_scratch_drill_and_not_reconstructed_in_a_new_topic_restore(
) {
    assert_the_mode_decides_the_label("replication_factor");
}

/// The controls for the four rows above: any OTHER differing key is
/// unexpected in both modes and never "not reconstructed" (only the four kinds
/// are reclassified), and a topic that matches the source on every count
/// and key has nothing in any list, in either mode.
#[test]
fn other_keys_stay_unexpected_and_a_matching_topic_has_nothing_in_either_mode() {
    let src = fixtures::source_configs(&[("max.message.bytes", "1048576")]);
    let tgt = fixtures::target_configs(&[("max.message.bytes", "999")]);
    for mode in [TargetMode::Scratch, TargetMode::NewTopic] {
        assert_eq!(
            classify_parity(mode, &src, &tgt, Some(3), 3, Some(3), 3),
            ParityClasses {
                intended: vec![],
                unexpected: vec!["max.message.bytes".to_string()],
                not_reconstructed: vec![],
                not_assessed: vec![],
            },
            "{mode}"
        );
        assert_eq!(
            classify_parity(mode, &src, &src, Some(3), 3, Some(3), 3),
            ParityClasses::default(),
            "{mode}: nothing differs"
        );
    }
}

/// A compacted source with a seven-day retention at replication factor 3,
/// restored into a NEW topic at the plan's replication factor 1: its
/// deviations are not reconstructed, never the scratch drill's
/// `intentionally_deviated`.
///
/// The verified receipt says the capture was `captured`, so no `not_assessed`
/// marker shares the list and every entry below is a deviation.
#[test]
fn a_new_topic_restore_signs_lost_compaction_retention_and_rf_as_not_reconstructed_never_intended()
{
    let (store, sha) = store_with_matching_segment();
    let facts = facts_with_segment(&sha);
    let (archive, cons) = fixtures::matching_pair(50);
    let mut topics = BTreeMap::new();
    topics.insert(
        "restore-orders".to_string(),
        TopicData {
            end_offsets: vec![(0, 50)],
            configs: fixtures::target_configs(&[
                ("cleanup.policy", "delete"),
                ("retention.ms", "-1"),
            ]),
            records: cons,
        },
    );
    let reader = Reader::target(topics);
    let engine = Engine::new()
        .describing(facts.clone())
        .fingerprinting(archive);
    let mut plan = plan_orders_to_drill_orders();
    plan.topic_mapping = fixtures::mapping("orders", "restore-orders");
    let moved = vec![
        "restore-orders: cleanup.policy".to_string(),
        "restore-orders: replication_factor".to_string(),
        "restore-orders: retention.ms".to_string(),
    ];

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel_orders(),
        &fixtures::mapping("orders", "restore-orders"),
        &plan,
        &receipt_coverage("captured"),
        TargetMode::NewTopic,
    )
    .unwrap();
    assert_eq!(out.integrity.result, IntegrityResult::Pass);
    assert!(
        out.topic_parity.intentionally_deviated.is_empty(),
        "a newTopic restore's lost settings are never INTENDED: {:?}",
        out.topic_parity.intentionally_deviated
    );
    assert_eq!(out.topic_parity.not_reconstructed, Some(moved.clone()));
    assert_eq!(
        out.topic_parity.unexpected_divergence, moved,
        "MOVED, never dropped: the twin in the list a reader older than 1.2.0 shows"
    );
    assert_eq!(out.topic_parity.not_assessed, Some(vec![]));

    // What phase 8 does with it: a 1.2.0 `newTopic` document carrying this
    // parity is one `validate_invariants` lets it sign (NR-1..NR-5 hold).
    let mut sc = fixtures::scorecard_pass();
    sc.format_version = logweir_core::FORMAT_VERSION.to_string();
    sc.target.mode = TargetMode::NewTopic;
    sc.target.marker_topic = None;
    sc.topic_parity = out.topic_parity.clone();
    sc.validate_invariants()
        .expect("phase 7's newTopic parity is a signable 1.2.0 document");

    // THE CONTROL, the same restore as a scratch drill: today's labels, and
    // the claim that nothing was left unreconstructed.
    let (store, sha) = store_with_matching_segment();
    let facts = facts_with_segment(&sha);
    let (archive, cons) = fixtures::matching_pair(50);
    let mut topics = BTreeMap::new();
    topics.insert(
        "restore-orders".to_string(),
        TopicData {
            end_offsets: vec![(0, 50)],
            configs: fixtures::target_configs(&[
                ("cleanup.policy", "delete"),
                ("retention.ms", "-1"),
            ]),
            records: cons,
        },
    );
    let engine = Engine::new()
        .describing(facts.clone())
        .fingerprinting(archive);
    let out = run(
        &engine,
        &Reader::target(topics),
        &store,
        &facts,
        &sel_orders(),
        &fixtures::mapping("orders", "restore-orders"),
        &plan,
        &receipt_coverage("captured"),
        TargetMode::Scratch,
    )
    .unwrap();
    assert_eq!(out.topic_parity.intentionally_deviated, moved);
    assert!(out.topic_parity.unexpected_divergence.is_empty());
    assert_eq!(out.topic_parity.not_reconstructed, Some(vec![]));

    // THE MODE LOST ON ITS WAY TO PHASE 7: scratch labels signed in a
    // document whose `target.mode` is newTopic. Phase 8's
    // `validate_invariants` refuses to sign them (NR-4), and both readers
    // refuse them.
    let mut sc = fixtures::scorecard_pass();
    sc.format_version = logweir_core::FORMAT_VERSION.to_string();
    sc.target.mode = TargetMode::NewTopic;
    sc.target.marker_topic = None;
    sc.topic_parity = out.topic_parity.clone();
    let err = sc
        .validate_invariants()
        .expect_err("a newTopic document with the scratch labels is never signed");
    assert!(
        err.0
            .starts_with("topic_parity.intentionally_deviated is not empty in a newTopic document"),
        "{}",
        err.0
    );
}

/// A refused TARGET configuration read (`targetReadDenied`) compares no
/// keys, but replication factor comes from metadata and is classified either
/// way — so even then a `newTopic` restore names it not reconstructed, beside
/// the fail-safe marker.
#[test]
fn a_new_topic_restore_whose_target_read_was_refused_still_names_its_replication_factor() {
    let (store, facts, reader, engine) = healthy_parts();
    let out = run(
        &engine,
        &refusing_target_configs(reader),
        &store,
        &facts,
        &sel_orders(),
        &fixtures::mapping("orders", "drill-orders"),
        &plan_orders_to_drill_orders(),
        &receipt_coverage("captured"),
        TargetMode::NewTopic,
    )
    .unwrap();
    assert_eq!(
        out.topic_parity.not_reconstructed,
        Some(vec!["drill-orders: replication_factor".to_string()])
    );
    assert_eq!(
        out.topic_parity.unexpected_divergence,
        vec![
            "drill-orders: configuration not assessed (targetReadDenied)".to_string(),
            "drill-orders: replication_factor".to_string(),
        ]
    );
    assert!(out.topic_parity.intentionally_deviated.is_empty());
}

/// The bullet `docs/stability.md`'s FX-3 section once carried, verbatim
/// (whitespace-flattened), kept so [`fx3_stale_wording`] is proven to catch it.
const FX3_PENDING_BULLET_OF_2026_10_05: &str = "- **PENDING THE OWNER'S RULING: the content \
     of the two existing lists.** In a `newTopic` document, entries of the existing shape \
     `\"<target topic>: <key>\"` leave `intentionally_deviated` and are written into \
     `unexpected_divergence` instead. That is a change to existing fields' content, which the \
     rule above calls MAJOR unless ruled otherwise, and OD-7's follow-up ruling covers FX-4's \
     entry only. It is FX-4's class, a change that can only weaken an older reader's \
     conclusion: what such a reader showed as intended it now shows as unexpected, and no \
     verdict moves, because no reader has a `topic_parity` arm and `outcome` does not depend \
     on it (measured, the next bullet). It is implemented as if MINOR and is not MINOR until \
     the owner rules. If the owner rules it MAJOR, the choices are format `2.0.0`, which needs \
     two maintainer approvals and makes every older reader refuse every new scorecard, or \
     leaving `intentionally_deviated` as it was and carrying the correction in \
     `not_reconstructed` alone, a plain MINOR under which every older reader keeps reading \
     the old label.";

/// The wording that would say FX-3's classification is still open, in a
/// whitespace-flattened, lowercased document.
fn fx3_stale_wording(flat_lowercase: &str) -> Vec<&'static str> {
    [
        "pending the owner's ruling",
        "pending the owner’s ruling",
        "implemented as if minor",
        "not minor until the owner rules",
        "if the owner rules it major",
    ]
    .into_iter()
    .filter(|stale| flat_lowercase.contains(stale))
    .collect()
}

/// **FX-3's classification is RULED, and no document calls it pending.** The
/// new field and arms NR-1..NR-5 are MINOR under OD-7 (a); moving a
/// `newTopic` restore's deviations into the existing `unexpected_divergence`
/// is OD-7's third, general case (it can only move a reader's verdict to the
/// safer side). `docs/stability.md`'s rule states that case,
/// `docs/formats/drill-scorecard.md`'s FX-3 section states the ruling, and no
/// listed document or source still says the classification awaits it.
///
/// Negative controls: [`FX3_PENDING_BULLET_OF_2026_10_05`] must trip
/// [`fx3_stale_wording`] (asserted first, so the check cannot go blind); a
/// rule without the third case's FX-3 sentence, a "pending" sentence in any
/// listed file, or a section that drops NR-4 and NR-5's classification or says
/// no reader has a `topic_parity` arm, fails here.
#[test]
fn fx3s_change_to_the_two_existing_lists_is_ruled_minor_and_no_doc_calls_it_pending() {
    assert_eq!(
        fx3_stale_wording(&FX3_PENDING_BULLET_OF_2026_10_05.to_lowercase()).len(),
        4,
        "the stale-wording check must catch every pending phrase of the 2026-10-05 bullet"
    );
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let flat = |rel: &str| -> String {
        std::fs::read_to_string(root.join(rel))
            .unwrap_or_else(|e| panic!("{rel}: {e}"))
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    };
    let stability = flat("docs/stability.md");
    // The rule itself carries the third case and names FX-3's move, so the
    // FX-3 section's link to the rule points at a ruling that is there.
    let rule = stability
        .split("## The signed formats")
        .next()
        .expect("docs/stability.md states the rule before its list of signed formats");
    for ruled in [
        "On 2026-10-07 the owner added a third, general case",
        "**A new cause for an existing value, or new content in an existing field, is MINOR \
         when it can only move a reader's verdict to the safer side**: not restorable, not \
         trusted, not intended. It is never MINOR when it can make any verdict stronger.",
        "So is FX-3's move of the new-topic deviations from `intentionally_deviated` to \
         `unexpected_divergence`.",
    ] {
        assert!(
            rule.contains(ruled),
            "docs/stability.md's MINOR/MAJOR rule no longer says: {ruled}"
        );
    }
    // FX-3's classification lives with the field it classifies.
    let scorecard = flat("docs/formats/drill-scorecard.md");
    let section = scorecard
        .split("### `topic_parity` in a `newTopic` restore: not reconstructed (1.2.0)")
        .nth(1)
        .expect("docs/formats/drill-scorecard.md has FX-3's `not_reconstructed` section")
        .split(" ### ")
        .next()
        .expect("the section");
    for said in [
        "**The content of the two existing lists is MINOR: the owner ruled it on 2026-10-07, \
         OD-7's third case** ([the rule](../stability.md#the-v010-tag-is-the-compatibility-boundary)).",
        "It makes no verdict stronger",
        "**The field and its five arms are MINOR** under the owner's OD-7 (a)",
        "**NR-4 and NR-5 are MINOR under OD-7 too**",
        "no reader built before FX-3 has a `topic_parity` arm",
    ] {
        assert!(
            section.contains(said),
            "docs/formats/drill-scorecard.md's FX-3 section no longer says: {said}"
        );
    }
    // This build's readers DO have `topic_parity` arms.
    assert!(
        !section.contains("because no reader has a `topic_parity` arm"),
        "docs/formats/drill-scorecard.md's FX-3 section says no reader has a topic_parity arm; \
         NR-1 to NR-5 are"
    );
    for doc in [
        "docs/stability.md",
        "docs/formats/drill-scorecard.md",
        "docs/verify-a-scorecard.md",
        "docs/release-notes.md",
        "docs/formats/backup-receipt.md",
        "e2e/fixtures/invariants/README.md",
        "MAINTAINERS.md",
        "docs/verify_scorecard.py",
        "crates/logweir-core/src/scorecard.rs",
        "crates/logweir/src/drill/phase7_verify.rs",
        "crates/logweir/src/verify.rs",
        "crates/logweir/src/show.rs",
    ] {
        let stale = fx3_stale_wording(&flat(doc).to_lowercase());
        assert!(
            stale.is_empty(),
            "{doc} still calls FX-3's classification pending: {stale:?}"
        );
    }
}

// A source replication factor (or partition count) the archive does not
// record is NOT ASSESSED, never read as matching: engine 0.23.3 records the
// factor in the manifest for the FIRST topic it saves only.

/// A verified receipt's coverage, `captured` for `orders`, whose 1.3.0
/// `topic_configuration` records `orders`' replication factor as `factor`
/// (`None`: no `topic_configuration` at all, a receipt before 1.3.0).
fn receipt_with_factor(factor: Option<u32>) -> logweir_core::backup_receipt::SourceConfigCoverage {
    let mut receipt: logweir_core::backup_receipt::BackupReceipt = serde_json::from_slice(
        &std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../e2e/fixtures/signed/backup-receipt.json"),
        )
        .unwrap(),
    )
    .unwrap();
    receipt.format_version = "1.3.0".into();
    receipt.config_coverage = Some(BTreeMap::from([(
        "orders".to_string(),
        logweir_core::backup_receipt::TopicConfigCoverage {
            coverage: "captured".to_string(),
            reason: None,
            timestamp_type: None,
        },
    )]));
    receipt.topic_configuration = factor.map(|f| {
        BTreeMap::from([(
            "orders".to_string(),
            logweir_core::backup_receipt::TopicConfiguration {
                partitions: Some(1),
                replication_factor: Some(f),
                entries: None,
                owner: None,
            },
        )])
    });
    logweir_core::backup_receipt::SourceConfigCoverage::from_receipt(&receipt)
}

/// Phase 7 over `healthy_parts`, with the manifest's factor and partition count
/// for `orders` replaced, in `mode`.
fn parity_with(
    manifest_factor: Option<i16>,
    manifest_partitions: Option<i32>,
    coverage: &logweir_core::backup_receipt::SourceConfigCoverage,
    mode: TargetMode,
) -> logweir_core::scorecard::TopicParity {
    let (store, mut facts, reader, engine) = healthy_parts();
    facts.topics[0].source_replication_factor = manifest_factor;
    facts.topics[0].original_partition_count = manifest_partitions;
    run(
        &engine,
        &reader,
        &store,
        &facts,
        &sel_orders(),
        &fixtures::mapping("orders", "drill-orders"),
        &plan_orders_to_drill_orders(),
        coverage,
        mode,
    )
    .unwrap()
    .topic_parity
}

/// **The factor not recorded.** The manifest records no replication factor
/// for the topic and the bound receipt none either (a 1.1.0 receipt, or no
/// binding): the factor is named in `not_assessed` and its fail-safe twin in
/// `unexpected_divergence`, in both modes, and it is in neither the intended
/// nor the not-reconstructed list. The plan's factor here is 1, the target's
/// own value, which must not stand in for the missing one.
///
/// Negative control: `t.source_replication_factor.unwrap_or(tgt_rf)` in
/// `classify_parity_all` names nothing in `not_assessed` and fails every row
/// below.
#[test]
fn a_replication_factor_the_archive_does_not_record_is_not_assessed_never_matched() {
    let entry = "drill-orders: replication_factor (notRecorded)".to_string();
    let twin = "drill-orders: replication_factor not assessed (notRecorded)".to_string();
    for coverage in [
        receipt_with_factor(None),
        logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
    ] {
        for mode in [TargetMode::Scratch, TargetMode::NewTopic] {
            let p = parity_with(None, Some(1), &coverage, mode);
            let named = p.not_assessed.clone().expect("phase 7 ran");
            assert!(named.contains(&entry), "{mode}: {p:?}");
            assert!(p.unexpected_divergence.contains(&twin), "{mode}: {p:?}");
            for list in [
                &p.intentionally_deviated,
                p.not_reconstructed.as_ref().expect("phase 7 ran"),
            ] {
                assert!(
                    !list.iter().any(|e| e == "drill-orders: replication_factor"),
                    "{mode}: an unrecorded factor is no measured deviation: {p:?}"
                );
            }
        }
    }
    // Recorded nowhere, with the configuration captured: the factor is the
    // ONLY thing not assessed, so the list is exactly the one entry.
    let p = parity_with(
        None,
        Some(1),
        &receipt_with_factor(None),
        TargetMode::Scratch,
    );
    assert_eq!(p.not_assessed, Some(vec![entry]));
}

/// **The receipt's factor.** Where the manifest records none, the bound
/// receipt's `topic_configuration` factor (1.3.0, Logweir's own metadata read)
/// is the source's, and is compared: 3 against the plan's 1 is a deviation,
/// intended in a scratch drill and not reconstructed in a `newTopic` restore,
/// and nothing is not assessed. The MANIFEST's factor comes first, so a
/// document whose manifest records one is decided by the manifest: manifest 1
/// and receipt 3 is no deviation.
///
/// Negative controls: a phase 7 that ignores the receipt fails the first row
/// (it names the factor not assessed); one that prefers the receipt fails the
/// second (it reports a deviation).
#[test]
fn the_bound_receipts_factor_stands_where_the_manifest_records_none() {
    let p = parity_with(
        None,
        Some(1),
        &receipt_with_factor(Some(3)),
        TargetMode::Scratch,
    );
    assert_eq!(p.not_assessed, Some(vec![]), "{p:?}");
    assert!(p
        .intentionally_deviated
        .contains(&"drill-orders: replication_factor".to_string()));
    let p = parity_with(
        None,
        Some(1),
        &receipt_with_factor(Some(3)),
        TargetMode::NewTopic,
    );
    assert_eq!(p.not_assessed, Some(vec![]), "{p:?}");
    assert!(p
        .not_reconstructed
        .as_ref()
        .unwrap()
        .contains(&"drill-orders: replication_factor".to_string()));
    let p = parity_with(
        Some(1),
        Some(1),
        &receipt_with_factor(Some(3)),
        TargetMode::Scratch,
    );
    assert_eq!(p.not_assessed, Some(vec![]), "{p:?}");
    assert!(
        !p.intentionally_deviated
            .iter()
            .any(|e| e.ends_with("replication_factor")),
        "the manifest's factor (1, the plan's) wins: {p:?}"
    );
}

/// **The same class for the partition count.** A manifest that records no
/// `original_partition_count` (an archive before engine 0.17) is named in
/// `not_assessed` with its twin, exactly like the factor. The control: the
/// recorded count (1, the target's 1) is a match and adds nothing.
#[test]
fn a_partition_count_the_archive_does_not_record_is_not_assessed_never_matched() {
    let p = parity_with(
        Some(3),
        None,
        &receipt_with_factor(None),
        TargetMode::Scratch,
    );
    assert_eq!(
        p.not_assessed,
        Some(vec![
            "drill-orders: partition_count (notRecorded)".to_string()
        ])
    );
    assert!(p
        .unexpected_divergence
        .contains(&"drill-orders: partition_count not assessed (notRecorded)".to_string()));
    let p = parity_with(
        Some(3),
        Some(1),
        &receipt_with_factor(None),
        TargetMode::Scratch,
    );
    assert_eq!(p.not_assessed, Some(vec![]));
    assert!(!p
        .unexpected_divergence
        .iter()
        .any(|e| e.contains("partition_count")));
}

/// **Through phase 8's checks.** The `newTopic` document phase 7 writes
/// for a topic whose factor is not recorded passes `validate_invariants` in
/// full (NR-2 to NR-5 included: the twin names no setting the restore
/// decides), so phase 8 signs it, and its `not_assessed` names the factor.
#[test]
fn a_new_topic_document_naming_an_unrecorded_factor_is_signable() {
    let p = parity_with(
        None,
        Some(1),
        &receipt_with_factor(None),
        TargetMode::NewTopic,
    );
    let mut sc = fixtures::scorecard_pass();
    sc.format_version = logweir_core::FORMAT_VERSION.to_string();
    sc.target.mode = TargetMode::NewTopic;
    sc.target.marker_topic = None;
    sc.topic_parity = p;
    sc.validate_invariants()
        .expect("phase 8 signs the FX-21 document");
    assert!(sc
        .topic_parity
        .not_assessed
        .as_ref()
        .unwrap()
        .contains(&"drill-orders: replication_factor (notRecorded)".to_string()));
}
