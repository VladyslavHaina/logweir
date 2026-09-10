//! Task 10 — guards **G-HDR** and **G-WIN** (second half).
//!
//! ## G-HDR: the header is little-endian, and the old decode was silent
//!
//! `phase7_verify::compare` decoded `x-original-offset` as DECIMAL TEXT
//! (`std::str::from_utf8(v).ok().and_then(|s| s.parse::<i64>().ok())`) while
//! the engine writes an 8-byte little-endian i64
//! (`U:crates/kafka-backup-core/src/backup/engine.rs:1846`,
//! `record.offset.to_le_bytes()`; the restore-side injection matches at
//! `U:…/restore/helpers.rs:130`). So `orig` was `None` against EVERY real
//! archive, and reconciliation fell back to the target's OWN offsets — which
//! is correct only for a scratch topic restored from offset 0 with no window,
//! and is precisely why today's drills passed.
//!
//! The defect was fixture-invisible: every fixture in this tree fabricated the
//! header as ASCII **and** restored from offset 0, so the fallback happened to
//! equal the right answer. `phase7_reconciles_a_windowed_restore_by_original_offset`
//! below is the case no existing fixture could express — an archive whose
//! offsets start at the window floor (1000..1050) restored into a topic whose
//! own offsets start at 0. Under the decimal decode nothing matches and the
//! run reports 0/50, a **false red**; under the little-endian decode all 50
//! match.
//!
//! ## G-WIN, second half: the restored count is BOUNDED by the manifest
//!
//! `logweir_core::engine::expected_restored_count` returns `(lower, upper)`
//! over `[floor_ms, pit_ms]`: `lower` sums the segments the manifest places
//! wholly inside the window, `upper` adds every straddler. It is a BOUND and
//! not an equality because the manifest's finest granularity is the segment
//! and it carries no per-record timestamp, so a `point_in_time` landing inside
//! a segment — the normal case — makes the exact figure underivable. An
//! equality would fail a CORRECT implementation and would then be "fixed" by
//! weakening it, which STANDING RULE 21 forbids.
//!
//! Every epoch millisecond asserted in this file was computed with
//! `python3 -c 'import datetime as d; ...'` before it was written, and is
//! spelled beside its date (plan errata E6/E7).
mod fixtures;

use logweir::drill::phase7_verify::{compare, decode_original_offset, run};
use logweir::drill::{phase8_score, DrillError};
use logweir::exit::ExitCode;
use logweir_core::engine::{
    expected_restored_count, BackupSetFacts, BackupSetRef, DataEngine, EngineError, EngineId,
    EngineRun, PartitionFacts, PhaseObserver, PreflightReport, RecordFingerprint, RestoreFacts,
    RestorePlan, SampleSelection, SegmentFacts, StorageUrl, TopicFacts, WindowFloorSource,
};
use logweir_core::outcome::{IntegrityResult, Outcome};
use logweir_core::spec::{Anchor, ObjectivesSpec};
use logweir_engine_oso::storage::Store;
use logweir_kafka::reader::{ClusterReader, ConsumedRecord, KafkaError, TopicMeta};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// The window. `2026-09-01T00:00:00Z` .. `2026-09-01T12:00:00Z`, and the two
// instants inside it the segments are cut on.

/// 2026-09-01T00:00:00Z — the archive floor, and the plan window's start.
const FLOOR_MS: i64 = 1_788_220_800_000;
/// 2026-09-01T06:00:00Z.
const MID_MS: i64 = 1_788_242_400_000;
/// 2026-09-01T11:00:00Z.
const NEAR_PIT_MS: i64 = 1_788_260_400_000;
/// 2026-09-01T12:00:00Z — `restore.point_in_time`, and the plan window's end.
/// INCLUSIVE (`docs/stability.md`; the receipt's `covered.to_ms` is the
/// exclusive one).
const PIT_MS: i64 = 1_788_264_000_000;
/// 2026-09-01T13:00:00Z — past the point-in-time, so a segment ending here
/// STRADDLES it.
const PAST_PIT_MS: i64 = 1_788_267_600_000;

// ---------------------------------------------------------------------------
// Doubles. Deliberately local to this file rather than reused from
// `verify_phase.rs` (whose own doubles are private to that binary): the two
// files assert different properties over different manifests, and a shared
// double that has to serve both is a double that stops expressing either.

/// A `ClusterReader` answering from three maps. `consume_range` filters by
/// `offset >= from` and takes `max`, exactly as `fixtures::FixtureClient` does,
/// so a caller asking for `hi - 1` (which is what `newest_ts` does) gets the
/// record at the high watermark and a caller asking from 0 gets the canary.
struct WindowReader {
    end_offsets: BTreeMap<String, Vec<(i32, i64)>>,
    configs: BTreeMap<String, BTreeMap<String, String>>,
    records: BTreeMap<String, Vec<ConsumedRecord>>,
}

impl ClusterReader for WindowReader {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        Ok("MkU3OEVBNTcwNTJENDM2Qk".into())
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        Ok(self
            .end_offsets
            .keys()
            .map(|n| TopicMeta::new(n, 1))
            .collect())
    }
    fn end_offsets(&self, topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        Ok(self.end_offsets.get(topic).cloned().unwrap_or_default())
    }
    fn topic_configs(&self, topic: &str) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(self.configs.get(topic).cloned().unwrap_or_default())
    }
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(BTreeMap::new())
    }
    fn consume_range(
        &self,
        t: &str,
        p: i32,
        from: i64,
        max: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        Ok(self
            .records
            .get(t)
            .map(|rs| {
                rs.iter()
                    .filter(|r| r.partition == p && r.offset >= from)
                    .take(max)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }
}

/// A `DataEngine` that answers `describe` and `fingerprints` from fields and
/// refuses everything phase 7 does not call. `validation_run` returns exit 0:
/// phase 7 records it and never reads it as corroboration.
struct WindowEngine {
    facts: BackupSetFacts,
    fingerprints: Vec<RecordFingerprint>,
}

impl DataEngine for WindowEngine {
    fn id(&self) -> EngineId {
        EngineId {
            id: "oso-cli".into(),
            version: "v0.21.0-fixture".into(),
            digest: format!("sha256:{}", "0".repeat(64)),
        }
    }
    fn list_backup_sets(&self, _: &StorageUrl) -> Result<Vec<BackupSetRef>, EngineError> {
        unimplemented!("phase 7 never lists backup sets")
    }
    fn describe(&self, _: &BackupSetRef) -> Result<BackupSetFacts, EngineError> {
        Ok(self.facts.clone())
    }
    fn preflight(&self, _: &RestorePlan) -> Result<PreflightReport, EngineError> {
        unimplemented!("phase 7 never runs preflight")
    }
    fn restore(
        &self,
        _: &RestorePlan,
        _: &mut dyn PhaseObserver,
    ) -> Result<RestoreFacts, EngineError> {
        unimplemented!("phase 7 never restores")
    }
    fn fingerprints(&self, _: &SampleSelection) -> Result<Vec<RecordFingerprint>, EngineError> {
        Ok(self.fingerprints.clone())
    }
    fn validation_run(&self, _: &RestorePlan) -> Result<EngineRun, EngineError> {
        Ok(EngineRun { exit_code: 0 })
    }
}

// ---------------------------------------------------------------------------
// Fixture builders.

/// `logweir/`-prefixed because `Store::put_create_only` asserts Global
/// Constraint 6 on every handle; `segment_evidence` reads the key back with
/// `Store::get`, which has no prefix rule.
fn seed_segment(store: &Store, key: &str, payload: &[u8]) -> String {
    store.put_create_only(key, payload).unwrap();
    logweir_core::ids::sha256_prefixed(payload)
}

/// One archive record and the consumed record a correct restore produces from
/// it: the SAME key, value, timestamp and header set, at a DIFFERENT target
/// offset. The header is the archive offset — `original_offset` — because that
/// is what the engine stamps, and it is part of the record's own content
/// (`logweir_kafka::fingerprint::record_fingerprint` hashes every header), so
/// it must be identical on both sides, which it is.
fn archived_and_restored(
    topic: &str,
    partition: i32,
    original_offset: i64,
    target_offset: i64,
    ts_ms: i64,
) -> (RecordFingerprint, ConsumedRecord) {
    let key = format!("k{original_offset}").into_bytes();
    let value = format!("v{original_offset}").into_bytes();
    let headers = vec![(
        "x-original-offset".to_string(),
        Some(fixtures::le_offset(original_offset)),
    )];
    (
        RecordFingerprint {
            topic: topic.into(),
            partition,
            offset: original_offset,
            sha256: logweir_kafka::fingerprint::record_fingerprint(
                Some(&key),
                Some(&value),
                &headers,
                ts_ms,
            ),
        },
        ConsumedRecord {
            partition,
            offset: target_offset,
            timestamp_ms: ts_ms,
            key: Some(key),
            value: Some(value),
            headers,
        },
    )
}

fn facts_with(segments: Vec<SegmentFacts>) -> BackupSetFacts {
    BackupSetFacts {
        backup_id: "backup-windowed".into(),
        created_at: chrono::DateTime::from_timestamp_millis(PAST_PIT_MS).unwrap(),
        source_cluster_id: Some("SRC0000000000000000000".into()),
        manifest_sha256: "sha256:0".into(),
        manifest_version_id: None,
        consumer_group_snapshot_sha256: None,
        topics: vec![TopicFacts {
            name: "orders".into(),
            original_partition_count: Some(1),
            source_replication_factor: Some(1),
            configurations: fixtures::source_configs(&[("cleanup.policy", "delete")]),
            partitions: vec![PartitionFacts {
                partition_id: 0,
                segments,
                gaps: vec![],
                pruned: vec![],
            }],
        }],
    }
}

/// `target_bootstrap` is the compose stack's own host-side address, as a
/// STRING handed to `WindowReader`; no client is constructed anywhere in this
/// file (STANDING RULE 18, chain N — this path is on
/// `no_network_in_unit_tests.rs`'s `ALLOWED` for exactly that reason).
fn window_plan() -> RestorePlan {
    RestorePlan {
        set: BackupSetRef {
            backup_id: "backup-windowed".into(),
            manifest_key: "backup-windowed/manifest.json".into(),
        },
        storage: StorageUrl::Filesystem {
            path: "/tmp".into(),
        },
        target_bootstrap: vec!["localhost:9092".into()],
        target_auth: logweir_core::engine::AuthRender::Plaintext,
        topic_mapping: fixtures::mapping("orders", "drill-orders"),
        time_window: (
            chrono::DateTime::from_timestamp_millis(FLOOR_MS).unwrap(),
            chrono::DateTime::from_timestamp_millis(PIT_MS).unwrap(),
        ),
        window_floor_source: WindowFloorSource::ArchiveManifest,
        default_replication_factor: 1,
        checkpoint_state: "/tmp/logweir/checkpoint.json".into(),
        checkpoint_interval_secs: 30,
        offset_report: "/tmp/logweir/offsets.json".into(),
    }
}

fn selection(count: usize) -> Vec<SampleSelection> {
    vec![SampleSelection {
        set: BackupSetRef {
            backup_id: "backup-windowed".into(),
            manifest_key: "backup-windowed/manifest.json".into(),
        },
        topic: "orders".into(),
        partition: 0,
        anchor: Anchor::Head,
        count,
        window: (FLOOR_MS, PIT_MS),
    }]
}

fn scratch_target_configs() -> BTreeMap<String, String> {
    fixtures::target_configs(&[("cleanup.policy", "delete"), ("retention.ms", "-1")])
}

// ---------------------------------------------------------------------------
// G-HDR.

/// **Guard G-HDR.** An archive whose offsets start at the window floor
/// (1000..1050) restored into a topic whose own offsets start at 0.
///
/// Two separate assertions, per the task's acceptance:
///
/// 1. the run reconciles **50/50** — under the decimal-text decode this fixture
///    reports **0/50**, because every `orig` is `None`, the fallback uses the
///    target's own 0..49, and none of those matches the archive's 1000..1049;
/// 2. the fixture header really is the engine's byte layout: `le_offset(5)` is
///    exactly `[5, 0, 0, 0, 0, 0, 0, 0]`, eight bytes, **never** `b"5"`.
///
/// Kills the mutant "restore the decimal-string parse in `compare`", which
/// fails at assertion time on 50 vs 0 matching.
#[test]
fn phase7_reconciles_a_windowed_restore_by_original_offset() {
    // (2) The header layout, asserted on its own before anything reads it.
    assert_eq!(
        fixtures::le_offset(5),
        vec![5u8, 0, 0, 0, 0, 0, 0, 0],
        "`x-original-offset` is an 8-byte little-endian i64 \
         (U:crates/kafka-backup-core/src/backup/engine.rs:1846), never decimal ASCII"
    );

    let store = Store::in_memory("logweir");
    let sha = seed_segment(
        &store,
        "logweir/windowed-seg.kbak",
        b"windowed segment payload",
    );

    // The archive covers original offsets 1000..1050 — the restore's window
    // begins well inside the topic's life, which is what a point-in-time
    // restore looks like and what no other fixture in this tree expresses.
    let facts = facts_with(vec![SegmentFacts {
        key: "logweir/windowed-seg.kbak".into(),
        start_offset: 1000,
        end_offset: 1049,
        start_timestamp: MID_MS,
        end_timestamp: NEAR_PIT_MS,
        record_count: 50,
        sha256: sha,
        uploaded_at: NEAR_PIT_MS,
    }]);

    let mut archive = Vec::new();
    let mut consumed = Vec::new();
    for i in 0..50i64 {
        // The target topic is one Logweir created in this run, so it starts at
        // offset 0: original offset 1000 + i lands at target offset i.
        let (a, c) = archived_and_restored("orders", 0, 1000 + i, i, MID_MS + i);
        archive.push(a);
        consumed.push(c);
    }
    assert_eq!(
        consumed[0].headers[0].1.as_deref().map(<[u8]>::len),
        Some(8),
        "the fixture must carry the engine's 8-byte header, or this test is asserting \
         nothing about the decode"
    );

    let reader = WindowReader {
        end_offsets: [("drill-orders".to_string(), vec![(0, 50)])]
            .into_iter()
            .collect(),
        configs: [("drill-orders".to_string(), scratch_target_configs())]
            .into_iter()
            .collect(),
        records: [("drill-orders".to_string(), consumed)]
            .into_iter()
            .collect(),
    };
    let engine = WindowEngine {
        facts: facts.clone(),
        fingerprints: archive,
    };

    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &selection(50),
        &fixtures::mapping("orders", "drill-orders"),
        &window_plan(),
    )
    .expect("a healthy windowed restore is not an operational failure");

    // (1) The reconciliation itself. BOTH numbers, not just the verdict: the
    // decimal decode makes this 50 sampled / 0 matching.
    assert_eq!(
        (
            out.integrity.records_sampled,
            out.integrity.records_sampled_matching
        ),
        (50, 50),
        "every record the archive offered must reconcile by its ORIGINAL offset; got {:?}",
        out.integrity
    );
    assert_eq!(out.integrity.result, IntegrityResult::Pass);
    assert_eq!(out.integrity.mismatches, 0);
    assert_eq!(out.pass_rate(), Some(1.0));
}

/// The decode itself, at its boundaries. `le_offset(5)` decodes; the ASCII
/// `b"5"` does NOT — that row is the whole point, and it is what a
/// re-introduced text fallback fails on.
///
/// Kills the mutant "add an ASCII fallback when the length is not 8", which
/// returns `Some(5)` for `b"5"` where `None` is expected.
#[test]
fn decode_original_offset_accepts_only_eight_bytes() {
    assert_eq!(decode_original_offset(&fixtures::le_offset(5)), Some(5));

    assert_eq!(
        decode_original_offset(b"5"),
        None,
        "one ASCII byte is not an original offset; the old decimal decode read it as 5 and \
         was `None` against every real archive in exchange"
    );

    // Eight ASCII bytes ARE eight bytes, and are decoded as the little-endian
    // integer they are. Asserted as that exact number, which documents that
    // there is deliberately no ASCII fallback: under one there would be two
    // readings of this input and no way to tell which was meant.
    assert_eq!(
        decode_original_offset(b"12345678"),
        Some(i64::from_le_bytes(*b"12345678")),
        "an 8-byte value is an offset, whatever its bytes happen to spell"
    );
    assert_eq!(
        i64::from_le_bytes(*b"12345678"),
        4_050_765_991_979_987_505,
        "spelled out so the assertion above cannot agree with itself by construction"
    );

    assert_eq!(decode_original_offset(b"1234567"), None, "seven bytes");
    assert_eq!(decode_original_offset(b"123456789"), None, "nine bytes");
    assert_eq!(decode_original_offset(b""), None, "no bytes");
    assert_eq!(
        decode_original_offset(&(-1i64).to_le_bytes()),
        Some(-1),
        "the value is a signed i64, so every bit pattern is a legal offset"
    );
}

/// `compare` falls back to the target's own offset ONLY when there is no
/// usable header — and that fallback must not be reachable through a header
/// the engine actually wrote. Both halves in one test, because the pair is the
/// property: an 8-byte header wins over the target offset, and a record with
/// no header at all still reconciles on a from-zero restore.
#[test]
fn compare_falls_back_to_the_target_offset_only_when_there_is_no_header() {
    // No header at all: the archive offset and the target offset agree, which
    // is the from-zero scratch restore the fallback exists for.
    let key = b"k".to_vec();
    let value = b"v".to_vec();
    let no_headers: Vec<(String, Option<Vec<u8>>)> = vec![];
    let archive = vec![RecordFingerprint {
        topic: "orders".into(),
        partition: 0,
        offset: 7,
        sha256: logweir_kafka::fingerprint::record_fingerprint(
            Some(&key),
            Some(&value),
            &no_headers,
            MID_MS,
        ),
    }];
    let consumed = vec![ConsumedRecord {
        partition: 0,
        offset: 7,
        timestamp_ms: MID_MS,
        key: Some(key),
        value: Some(value),
        headers: no_headers,
    }];
    let (sampled, matching, mismatches) = compare(&archive, &consumed);
    assert_eq!((sampled, matching), (1, 1));
    assert!(mismatches.is_empty());

    // A header the engine wrote, at a target offset that is NOT the original:
    // the header decides, so a fallback to offset 99 cannot match archive
    // offset 1005.
    let (a, c) = archived_and_restored("orders", 0, 1005, 99, MID_MS);
    let (sampled, matching, mismatches) = compare(&[a], &[c]);
    assert_eq!(
        (sampled, matching),
        (1, 1),
        "the 8-byte header names 1005; the fallback would have said 99: {mismatches:?}"
    );
}

// ---------------------------------------------------------------------------
// G-WIN, second half.

/// Builds the [5900, 6000] manifest and runs phase 7 against a target holding
/// `restored` records.
///
/// The archive and the target reconcile perfectly in every arm, so the count
/// bound is the ONLY thing that can move the verdict — which is what makes the
/// 5800 arm's failure attributable.
fn run_with_restored_count(restored: i64) -> logweir::drill::phase7_verify::VerifyOutcome {
    let store = Store::in_memory("logweir");
    let sha_inside = seed_segment(
        &store,
        "logweir/win-inside.kbak",
        b"wholly inside the window",
    );
    let sha_straddling = seed_segment(&store, "logweir/win-straddling.kbak", b"straddles the pit");

    let facts = facts_with(vec![
        // Wholly inside [FLOOR_MS, PIT_MS]: the manifest PROVES 5900 records.
        SegmentFacts {
            key: "logweir/win-inside.kbak".into(),
            start_offset: 0,
            end_offset: 5899,
            start_timestamp: MID_MS,
            end_timestamp: NEAR_PIT_MS,
            record_count: 5900,
            sha256: sha_inside,
            uploaded_at: NEAR_PIT_MS,
        },
        // Straddles the point-in-time: at most 100 more, at least none.
        SegmentFacts {
            key: "logweir/win-straddling.kbak".into(),
            start_offset: 5900,
            end_offset: 5999,
            start_timestamp: NEAR_PIT_MS,
            end_timestamp: PAST_PIT_MS,
            record_count: 100,
            sha256: sha_straddling,
            uploaded_at: PAST_PIT_MS,
        },
    ]);

    let mut archive = Vec::new();
    let mut consumed = Vec::new();
    for i in 0..50i64 {
        let (a, c) = archived_and_restored("orders", 0, i, i, MID_MS + i);
        archive.push(a);
        consumed.push(c);
    }
    // One record at the high watermark, so `newest_ts` (which consumes at
    // `hi - 1`) has something honest to read. `consume_range` takes the FIRST
    // `max` records at or after `from`, so the canary above is what a
    // from-zero read of 50 returns and this one is never part of it.
    let (_, hwm_record) = archived_and_restored("orders", 0, restored - 1, restored - 1, PIT_MS);
    consumed.push(hwm_record);

    let reader = WindowReader {
        end_offsets: [("drill-orders".to_string(), vec![(0, restored)])]
            .into_iter()
            .collect(),
        configs: [("drill-orders".to_string(), scratch_target_configs())]
            .into_iter()
            .collect(),
        records: [("drill-orders".to_string(), consumed)]
            .into_iter()
            .collect(),
    };
    let engine = WindowEngine {
        facts: facts.clone(),
        fingerprints: archive,
    };

    run(
        &engine,
        &reader,
        &store,
        &facts,
        &selection(50),
        &fixtures::mapping("orders", "drill-orders"),
        &window_plan(),
    )
    .expect("a count outside the bound is a DRILL RESULT, never an operational failure")
}

/// The exact failure text the bound produces, spelled out here rather than
/// built from the same `format!` the production code uses — an expectation
/// assembled by the code under test cannot disagree with it.
const BOUND_FAILURE: &str = "restored 5800 records but the manifest bounds the window \
                             [1788220800000, 1788264000000] at [5900, 6000]";

/// **Guard G-WIN, second half.** A manifest whose segments bound `[5900, 6000]`
/// over `[floor, pit]`:
///
/// - a target holding **6000** passes — the UPPER edge, and the mutant
///   "make the assertion an equality against `lower`" fails here (`6000 != 5900`),
///   which is what proves the bound is not an equality in disguise;
/// - a target holding **5900** passes — the LOWER edge;
/// - a target holding **5950** passes — strictly inside;
/// - a target holding **5800** is `fail-integrity`, exit **2**, with the exact
///   failure text, and the scorecard is still written and signed (GC11).
#[test]
fn restored_count_is_inside_the_manifest_bound() {
    for restored in [5900i64, 5950, 6000] {
        let out = run_with_restored_count(restored);
        assert_eq!(
            out.integrity.result,
            IntegrityResult::Pass,
            "{restored} is inside [5900, 6000] and the reconciliation matched 50/50; \
             got {:?}",
            out.integrity
        );
        assert_eq!(out.integrity.records_sampled_matching, 50);
    }

    // Below the lower bound: the manifest PROVES 5900 records are in the
    // window and the target holds 5800.
    let out = run_with_restored_count(5800);
    assert_eq!(
        out.integrity.result,
        IntegrityResult::Fail,
        "5800 is below the 5900 the manifest proves; got {:?}",
        out.integrity
    );
    let reason = out
        .integrity
        .partial_reason
        .as_deref()
        .expect("a failing bound must say so in the signed document");
    assert!(
        reason.contains(BOUND_FAILURE),
        "the reason must carry the exact failure text.\n  expected to contain: \
         {BOUND_FAILURE}\n  got: {reason}"
    );
    // The sampled reconciliation still MATCHED — which is the point: inside
    // the bound the sample is the finer check, and outside it the sample
    // cannot vouch for a count it never looked at.
    assert_eq!(out.integrity.records_sampled_matching, 50);

    // The outcome and the exit code, read through the same two functions the
    // binary uses. `decide` maps any non-`Pass` integrity to
    // `fail-integrity`; `From<DrillError> for ExitCode` maps the resulting
    // non-pass scorecard to 2. Read directly, never through a pipe
    // (STANDING RULE 20).
    let measured = fixtures::scorecard_pass().measured;
    // No objective requested: `met` is `null`, so the outcome can only come
    // from `integrity.result` — the variable this test is about.
    let no_objectives = ObjectivesSpec {
        rto_seconds: None,
        rpo_seconds: None,
        pass_rate: None,
    };
    let (outcome, _objectives) = phase8_score::decide(&measured, &no_objectives, &out.integrity);
    assert_eq!(outcome, Outcome::FailIntegrity);
    assert_eq!(outcome.wire_name(), "fail-integrity");

    let mut sc = fixtures::scorecard_pass();
    sc.outcome = outcome;
    sc.integrity = out.integrity.clone();
    let code = ExitCode::from(DrillError::NotPass(Box::new(sc)));
    assert_eq!(code, ExitCode::DrillNotPass);
    assert_eq!(
        code as i32, 2,
        "a result that is not a pass is exit 2 (GC11)"
    );

    // And a count ABOVE the upper bound is refused too — the bound is closed
    // at both ends, and an archive cannot account for records it never held.
    let over = run_with_restored_count(6001);
    assert_eq!(over.integrity.result, IntegrityResult::Fail);
    assert!(over
        .integrity
        .partial_reason
        .as_deref()
        .is_some_and(|r| r.contains("restored 6001 records but the manifest bounds the window")));
}

// ---------------------------------------------------------------------------
// G-WIN, second half: the bound's SCOPE (plan erratum E7(b)).

/// How many records the manifest places in the window for the topic the
/// restore NAMES, and how many the target gets — a healthy, exact restore.
const NAMED_TOPIC_RECORDS: i64 = 50;
/// And how many an archive topic the restore does NOT name holds in the same
/// window. 180× the named topic's, so an unfiltered walk cannot be mistaken
/// for a rounding difference — and wholly INSIDE `[FLOOR_MS, PIT_MS]`, because
/// a segment outside the window contributes to neither bound and a fixture
/// built that way could not tell the two walks apart at all.
const UNNAMED_TOPIC_RECORDS: i64 = 9_000;

/// `facts_with`'s `orders` plus a SECOND topic `topic_mapping` does not name.
fn facts_with_an_unnamed_topic(named: SegmentFacts, unnamed: SegmentFacts) -> BackupSetFacts {
    let mut facts = facts_with(vec![named]);
    facts.topics.push(TopicFacts {
        name: "audit-log".into(),
        original_partition_count: Some(1),
        source_replication_factor: Some(1),
        configurations: fixtures::source_configs(&[("cleanup.policy", "delete")]),
        partitions: vec![PartitionFacts {
            partition_id: 0,
            segments: vec![unnamed],
            gaps: vec![],
            pruned: vec![],
        }],
    });
    facts
}

/// **Plan erratum E7(b), the bound's half.** The bound is computed over the
/// topics the restore NAMES, and an archive topic the mapping leaves out
/// cannot raise it.
///
/// Both behaviours are pinned, because they are two different contracts held
/// by two different pieces of code and only the pair is the property:
///
/// 1. `logweir_core::engine::expected_restored_count` is UNFILTERED BY
///    CONTRACT — its own doc comment says it "sums over every topic in the
///    `facts` it is handed, deliberately", so that the ONE scope decision
///    lives at the call site rather than in a signature that cannot carry a
///    mapping. Handed every topic it counts every topic: `(9050, 9050)`.
/// 2. `phase7_verify::check_restored_count` is where the reduction is applied,
///    beside the count it is compared against — the same rule
///    `earliest_covered_timestamp_ms` applies to the FLOOR
///    (`window_binding.rs::a_topic_the_restore_does_not_name_does_not_lower_the_floor`,
///    which is the floor-side twin of this test).
///
/// Kills the mutant "delete the `mapping.contains_key(&t.name)` filter in
/// `check_restored_count`" — measured by Task 10's reviewer to leave the whole
/// 924-test suite green. It is not an equivalent mutant: with the filter gone
/// the bound over this fixture becomes `[9050, 9050]`, and the healthy,
/// exact 50-of-50 restore below is signed `fail-integrity` with
/// `restored 50 records but the manifest bounds the window [.., ..] at
/// [9050, 9050]`. Half 2 is what fails, on the COUNT.
#[test]
fn the_bound_counts_only_the_topics_the_restore_names() {
    let store = Store::in_memory("logweir");
    let sha_named = seed_segment(
        &store,
        "logweir/win-named.kbak",
        b"the topic the restore names",
    );
    let sha_unnamed = seed_segment(
        &store,
        "logweir/win-unnamed.kbak",
        b"a topic the restore does not name",
    );
    let facts = facts_with_an_unnamed_topic(
        SegmentFacts {
            key: "logweir/win-named.kbak".into(),
            start_offset: 0,
            end_offset: NAMED_TOPIC_RECORDS - 1,
            start_timestamp: MID_MS,
            end_timestamp: NEAR_PIT_MS,
            record_count: NAMED_TOPIC_RECORDS,
            sha256: sha_named,
            uploaded_at: NEAR_PIT_MS,
        },
        SegmentFacts {
            key: "logweir/win-unnamed.kbak".into(),
            start_offset: 0,
            end_offset: UNNAMED_TOPIC_RECORDS - 1,
            start_timestamp: MID_MS,
            end_timestamp: NEAR_PIT_MS,
            record_count: UNNAMED_TOPIC_RECORDS,
            sha256: sha_unnamed,
            uploaded_at: NEAR_PIT_MS,
        },
    );
    let mapping = fixtures::mapping("orders", "drill-orders");

    // The fixture's own discriminating power, read OFF THE FIXTURE rather than
    // compared between two constants (an assertion the compiler would discard).
    assert!(
        !mapping.contains_key("audit-log"),
        "`audit-log` is the topic the restore does NOT name; if the mapping named it \
         this test would be about nothing"
    );
    let unnamed = facts
        .topics
        .iter()
        .find(|t| t.name == "audit-log")
        .expect("the fixture carries a topic the mapping does not name");
    let seg = &unnamed.partitions[0].segments[0];
    assert!(
        seg.start_timestamp >= FLOOR_MS && seg.end_timestamp <= PIT_MS,
        "the unnamed topic's segment must be WHOLLY INSIDE [{FLOOR_MS}, {PIT_MS}] — a \
         segment outside the window lands in neither bound, and a fixture built that way \
         cannot tell a filtered walk from an unfiltered one"
    );

    // Half 1. The unit function counts what it is handed, and must not grow a
    // filter of its own.
    let all_topics = (NAMED_TOPIC_RECORDS + UNNAMED_TOPIC_RECORDS) as u64;
    assert_eq!(
        expected_restored_count(&facts, FLOOR_MS, PIT_MS),
        (all_topics, all_topics),
        "`expected_restored_count` carries no topic filter BY CONTRACT: over all-topics \
         facts both bounds are the whole {all_topics}"
    );

    // Half 2. The call site's reduction, through the whole of phase 7: the
    // bound is [50, 50] over the named topic alone, so a target holding
    // exactly 50 PASSES.
    let mut archive = Vec::new();
    let mut consumed = Vec::new();
    for i in 0..NAMED_TOPIC_RECORDS {
        let (a, c) = archived_and_restored("orders", 0, i, i, MID_MS + i);
        archive.push(a);
        consumed.push(c);
    }
    let reader = WindowReader {
        end_offsets: [("drill-orders".to_string(), vec![(0, NAMED_TOPIC_RECORDS)])]
            .into_iter()
            .collect(),
        configs: [("drill-orders".to_string(), scratch_target_configs())]
            .into_iter()
            .collect(),
        records: [("drill-orders".to_string(), consumed)]
            .into_iter()
            .collect(),
    };
    let engine = WindowEngine {
        facts: facts.clone(),
        fingerprints: archive,
    };
    let out = run(
        &engine,
        &reader,
        &store,
        &facts,
        &selection(NAMED_TOPIC_RECORDS as usize),
        &mapping,
        &window_plan(),
    )
    .expect("a count inside the bound is never an operational failure");

    assert_eq!(
        out.integrity.result,
        IntegrityResult::Pass,
        "the restore wrote exactly the {NAMED_TOPIC_RECORDS} records the manifest proves \
         for the topic it NAMES; the {UNNAMED_TOPIC_RECORDS} records of a topic it does \
         not name were never going to be written. Got {:?}",
        out.integrity
    );
    // Stronger than `result == Pass` on its own: a bound failure inserts its
    // text at position 0 of the notes, so an empty `partial_reason` is the
    // assertion that NOTHING was reported against this run at all.
    assert_eq!(
        out.integrity.partial_reason, None,
        "a healthy restore of the named window has nothing to report"
    );
    assert_eq!(
        out.integrity.records_sampled_matching, NAMED_TOPIC_RECORDS as u64,
        "and the sample reconciled, so the bound is the only thing this test's verdict \
         could have come from"
    );
}

// ---------------------------------------------------------------------------
// Source-reading structural assertions.

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

fn read_source(rel: &str) -> String {
    let p = workspace_root().join(rel);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("{}: {e}", p.display()))
}

/// The text between a function's `fn <name>(` and its balanced closing brace —
/// the SIGNATURE AND BODY, and deliberately not the doc comment above it: a
/// doc comment cannot read a document, and scanning it would make the
/// assertion below fail on the very sentence that explains it.
fn function_source(src: &str, name: &str) -> String {
    let needle = format!("fn {name}(");
    // The DEFINITION, not a call: the match must begin a line, modulo
    // indentation and a `pub`. `fixtures::le_offset(` is a call and must not
    // be mistaken for one.
    let start = src
        .match_indices(&needle)
        .map(|(i, _)| i)
        .find(|&i| {
            let line_start = src[..i].rfind('\n').map_or(0, |n| n + 1);
            let prefix = &src[line_start..i];
            prefix.trim() == "" || prefix.trim() == "pub"
        })
        .unwrap_or_else(|| panic!("no `fn {name}(` definition in the source"));
    let open = src[start..]
        .find('{')
        .expect("a function with no body is not a function")
        + start;
    let mut depth = 0usize;
    for (i, c) in src[open..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return src[start..open + i + 1].to_string();
                }
            }
            _ => {}
        }
    }
    panic!("unbalanced braces after `fn {name}(`");
}

/// Lines of `src` up to the `#[cfg(test)]` module, with whole-line `//`
/// comments removed. Full-line comments only: truncating at an inline `//`
/// would cut the `https://…` inside a string literal, which this file has.
fn production_code(src: &str) -> String {
    let end = src.find("\n#[cfg(test)]").unwrap_or(src.len());
    src[..end]
        .lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The count expectation is MEASURED from the manifest and from the broker,
/// never parsed back out of the document Logweir itself rendered. Both halves
/// of a rendered-window comparison shrink together — the engine's restore
/// filter is `r.timestamp >= s && r.timestamp <= e`
/// (`U:crates/kafka-backup-core/src/restore/helpers.rs:74-82`) — so such a
/// check could never fire.
///
/// Kills the mutant "compute the bound from the rendered `time_window`
/// instead of the manifest": `check_restored_count` takes its two instants as
/// `i64` parameters and names neither token, and no non-test line of the whole
/// phase reaches a renderer at all.
#[test]
fn the_count_expectation_never_reads_the_rendered_document() {
    let src = read_source("crates/logweir/src/drill/phase7_verify.rs");

    let f = function_source(&src, "check_restored_count");
    assert!(
        f.contains("expected_restored_count"),
        "this test is only meaningful if it found the right function: {f}"
    );
    for token in ["time_window", "rendered"] {
        assert!(
            !f.contains(token),
            "`check_restored_count` names `{token}`; the count expectation comes from the \
             manifest and the broker, and a document Logweir rendered is a tautology \
             against them"
        );
    }

    // The broader form of the same property: NO non-test line of phase 7
    // reaches a renderer, so the mutant cannot be applied anywhere in the file
    // and stay invisible.
    let prod = production_code(&src);
    assert!(
        prod.contains("fn check_restored_count"),
        "the production half of the file was not found; the scan asserts nothing"
    );
    for token in ["render_restore", "render_and_digest", "time_window_start"] {
        assert!(
            !prod.contains(token),
            "phase 7 names `{token}`: it must measure the cluster and read the manifest, \
             never re-read its own rendered bytes"
        );
    }
}

/// Every `x-original-offset` header in the three fixture files is paired with
/// the `le_offset` helper, and no site is left spelling the value as decimal
/// ASCII.
///
/// The assertion is POSITIVE — the value paired with the header NAME is the
/// helper, and nothing else — rather than a blanket "no `b\"` within four
/// lines", which also flags the records' own key and value literals
/// (`crates/logweir-kafka/tests/reader.rs`) and would fail on correct code.
/// The `to_string().into_bytes()` clause alone is not enough either: it misses
/// the `b"5"` at `verify_phase.rs` and the `b"100"` at `reader.rs`, which are
/// two of the six sites this task rewrote.
///
/// Comment lines are skipped, and only comment lines: three of the
/// occurrences in these files are prose ABOUT the header, and a prose sentence
/// encodes nothing. The count of code occurrences is asserted to be at least
/// six so an over-eager filter cannot make this test vacuous.
///
/// Kills the mutant "leave one ASCII fixture behind", which fails naming the
/// file and the line.
#[test]
fn no_test_fixture_encodes_an_offset_header_as_ascii() {
    const FILES: [&str; 3] = [
        "crates/logweir/tests/fixtures/mod.rs",
        "crates/logweir/tests/verify_phase.rs",
        "crates/logweir-kafka/tests/reader.rs",
    ];
    let mut code_sites = 0usize;
    let mut offences: Vec<String> = Vec::new();

    for rel in FILES {
        let body = read_source(rel);
        let lines: Vec<&str> = body.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            if !line.contains("x-original-offset") {
                continue;
            }
            let trimmed = line.trim_start();
            if trimmed.starts_with("//") {
                continue;
            }
            code_sites += 1;
            let next = lines.get(i + 1).copied().unwrap_or("");
            let pair = format!("{line}\n{next}");
            if !pair.contains("le_offset(") {
                offences.push(format!(
                    "{rel}:{}: the header value is not `le_offset(...)`:\n    {}",
                    i + 1,
                    pair.trim()
                ));
            }
            if pair.contains("to_string().into_bytes()") {
                offences.push(format!(
                    "{rel}:{}: the header value is decimal ASCII:\n    {}",
                    i + 1,
                    pair.trim()
                ));
            }
        }
    }

    assert!(
        offences.is_empty(),
        "{} offset-header site(s) do not go through `le_offset`:\n  {}",
        offences.len(),
        offences.join("\n  ")
    );
    assert!(
        code_sites >= 6,
        "only {code_sites} code occurrences of `x-original-offset` were found across {FILES:?}; \
         the six this task rewrote must all be visible to this scan, or the filter above has \
         made the test vacuous"
    );
}

/// The ONE encoding, in both of the two places it has to be written.
///
/// `crates/logweir-kafka/tests/reader.rs` cannot reach
/// `crates/logweir/tests/fixtures::le_offset` — that is a test-only module of a
/// different crate — so there are two definitions. This asserts they are the
/// same function, which is what "one place the encoding is written" has to
/// mean when the language will not let it be one place.
#[test]
fn every_le_offset_definition_has_the_same_body() {
    const DEFINERS: [&str; 2] = [
        "crates/logweir/tests/fixtures/mod.rs",
        "crates/logweir-kafka/tests/reader.rs",
    ];
    for rel in DEFINERS {
        let body = read_source(rel);
        let f = function_source(&body, "le_offset");
        assert!(
            f.contains("n.to_le_bytes().to_vec()"),
            "{rel}'s `le_offset` does not encode the offset as little-endian bytes:\n{f}"
        );
        assert!(
            !f.contains("to_string"),
            "{rel}'s `le_offset` reaches for a string; the header is bytes, not text:\n{f}"
        );
    }
    // And the helper this file itself uses is the fixtures one, so the two
    // above are the whole population.
    assert!(
        Path::new(&workspace_root().join(DEFINERS[0])).exists(),
        "the fixtures module moved; this test's file list is stale"
    );
}
