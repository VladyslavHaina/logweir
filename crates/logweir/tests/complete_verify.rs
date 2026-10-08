//! **PROD-08.1 — complete verification, the fault matrix at the phase-7
//! seam.** Every row builds a REAL archive — KBAK segments encoded here,
//! independently of the decoder (the layout of `segment/format.rs:18-46`),
//! put into an in-memory store with the sha256 a manifest would carry — and
//! a target whose records are what the pinned engine's selection writes
//! (segments chosen by their FIRST and LAST timestamps, then each record by
//! its own: `restore/engine.rs:1684, 1957-1964`, `restore/helpers.rs:67-85`),
//! then injects one fault. The archive is the oracle; Logweir's verdict is
//! what is asserted.
//!
//! Each fault is asserted CAUGHT by the complete lane and, as its negative
//! control, either MISSED by the sampled lane (where that is the gap PROD-08.1
//! closes) or absent from an unfaulted twin that passes. The live twins of
//! these rows are the complete restores and the `complete_coverage_*` rows of
//! `e2e/tests/record_semantics.rs`.
mod fixtures;

use logweir::drill::phase7_verify::complete::{lineage_offset, LINEAGE_HEADER};
use logweir::drill::phase7_verify::{compare, lineage_faults, run_with_coverage, VerifyOutcome};
use logweir::drill::DrillError;
use logweir_core::backup_receipt::SourceConfigCoverage;
use logweir_core::engine::{
    BackupSetFacts, BackupSetRef, DataEngine, EngineError, EngineId, EngineRun, PartitionFacts,
    PhaseObserver, PreflightReport, RecordFingerprint, RestoreFacts, RestorePlan, SampleSelection,
    SegmentFacts, StorageUrl, TopicFacts, WindowFloorSource,
};
use logweir_core::outcome::{IntegrityLevel, IntegrityResult};
use logweir_core::scorecard::CompleteVerification;
use logweir_core::spec::{Anchor, Coverage, TargetMode};
use logweir_engine_oso::storage::Store;
use logweir_kafka::reader::{ClusterReader, ConsumedRecord, KafkaError, TopicMeta};
use std::collections::BTreeMap;
use std::sync::Mutex;

/// `2025-10-09T08:53:20Z`, PROD-01.1's fixture epoch; `+n` below is
/// milliseconds after it.
const T: i64 = 1_760_000_000_000;
const SOURCE: &str = "orders";
const TARGET: &str = "drill-orders";
const SET: &str = "complete-verify-set";

// ===================================================== the archive's records

#[derive(Clone, Debug)]
struct Rec {
    offset: i64,
    ts: i64,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: Vec<(String, Option<Vec<u8>>)>,
}

/// An archived record as the backup writes it: the record's own headers,
/// then `x-original-offset` and `x-original-timestamp` (PROD-01.1 S5).
fn archived(offset: i64, ts: i64) -> Rec {
    Rec {
        offset,
        ts,
        key: Some(format!("k{offset}").into_bytes()),
        value: Some(format!("value at {offset}").into_bytes()),
        headers: vec![
            ("trace".into(), Some(format!("t{offset}").into_bytes())),
            (LINEAGE_HEADER.into(), Some(offset.to_le_bytes().to_vec())),
            (
                "x-original-timestamp".into(),
                Some(ts.to_le_bytes().to_vec()),
            ),
        ],
    }
}

/// The segment the backup would write for `records`: the shared fixture
/// encoder (`fixtures::kbak_segment`), which encodes the documented layout
/// independently of the decoder phase 7 reads it with.
fn encode_segment(records: &[Rec]) -> Vec<u8> {
    let as_archived: Vec<ConsumedRecord> = records
        .iter()
        .map(|r| ConsumedRecord {
            partition: 0,
            offset: r.offset,
            timestamp_ms: r.ts,
            key: r.key.clone(),
            value: r.value.clone(),
            headers: r.headers.clone(),
        })
        .collect();
    fixtures::kbak_segment(&as_archived)
}

// ================================================================ a fixture

/// One archive: per partition, a list of segments, each a list of records.
struct Archive {
    parts: BTreeMap<i32, Vec<Vec<Rec>>>,
}

impl Archive {
    fn new() -> Archive {
        Archive {
            parts: BTreeMap::new(),
        }
    }

    /// Partition `p`'s segments, each given by its records' `+n` timestamps;
    /// offsets run on from 0 across the segments.
    fn partition(mut self, p: i32, segments: &[&[i64]]) -> Archive {
        let mut next = 0i64;
        let segs = segments
            .iter()
            .map(|ts| {
                ts.iter()
                    .map(|d| {
                        let r = archived(next, T + d);
                        next += 1;
                        r
                    })
                    .collect()
            })
            .collect();
        self.parts.insert(p, segs);
        self
    }

    /// Under `logweir/` only because the in-memory double writes there and
    /// nowhere else (Global Constraint 6); phase 7 reads a segment by the key
    /// the manifest names, whatever its prefix.
    fn key(p: i32, seg: &[Rec]) -> String {
        format!(
            "logweir/{SET}/topics/{SOURCE}/partition={p}/segment-{:020}.bin",
            seg[0].offset
        )
    }

    /// The store holding every segment — the in-memory double, whose second
    /// writer can REPLACE an object, as a corrupting writer would — and the
    /// manifest facts describing them: first/last timestamps, as the engine
    /// writes them (S6).
    fn build(&self) -> Built {
        let (store, bucket) = Store::in_memory_versioned("");
        let mut partitions = Vec::new();
        for (p, segs) in &self.parts {
            let mut facts = Vec::new();
            for seg in segs {
                let bytes = encode_segment(seg);
                let key = Archive::key(*p, seg);
                bucket.overwrite(&key, &bytes);
                facts.push(SegmentFacts {
                    key,
                    start_offset: seg[0].offset,
                    end_offset: seg[seg.len() - 1].offset,
                    start_timestamp: seg[0].ts,
                    end_timestamp: seg[seg.len() - 1].ts,
                    record_count: seg.len() as i64,
                    sha256: logweir_core::ids::sha256_hex(&bytes),
                    uploaded_at: T,
                });
            }
            partitions.push(PartitionFacts {
                partition_id: *p,
                segments: facts,
                gaps: vec![],
                pruned: vec![],
            });
        }
        let facts = BackupSetFacts {
            backup_id: SET.into(),
            created_at: fixtures::ts("2026-09-03T09:00:00Z"),
            source_cluster_id: Some("SRC0000000000000000000".into()),
            manifest_sha256: "sha256:0".into(),
            manifest_version_id: None,
            consumer_group_snapshot_sha256: None,
            topics: vec![TopicFacts {
                name: SOURCE.into(),
                original_partition_count: Some(self.parts.len() as i32),
                source_replication_factor: Some(1),
                configurations: BTreeMap::new(),
                partitions,
            }],
        };
        Built {
            bucket,
            store,
            facts,
        }
    }

    /// The window floor the plan binds (guard G-WIN): the minimum FIRST
    /// record timestamp (PROD-01.1 S8).
    fn floor(&self) -> i64 {
        self.parts
            .values()
            .flatten()
            .map(|seg| seg[0].ts)
            .min()
            .unwrap()
    }

    /// What the pinned engine writes for `[floor, end]`: segments selected
    /// by their first/last bounds overlapping the window, then records by
    /// their own timestamp — PER PARTITION, in archive order.
    fn engine_output(&self, end: i64) -> BTreeMap<i32, Vec<Rec>> {
        let floor = self.floor();
        self.parts
            .iter()
            .map(|(p, segs)| {
                let out = segs
                    .iter()
                    .filter(|seg| {
                        let (first, last) = (seg[0].ts, seg[seg.len() - 1].ts);
                        last >= floor && first <= end
                    })
                    .flatten()
                    .filter(|r| r.ts >= floor && r.ts <= end)
                    .cloned()
                    .collect();
                (*p, out)
            })
            .collect()
    }

    /// Every archived record at or before `end`: the exact model's output.
    fn exact_output(&self, end: i64) -> BTreeMap<i32, Vec<Rec>> {
        self.parts
            .iter()
            .map(|(p, segs)| {
                (
                    *p,
                    segs.iter()
                        .flatten()
                        .filter(|r| r.ts <= end)
                        .cloned()
                        .collect(),
                )
            })
            .collect()
    }
}

/// A built archive: the second writer on its bucket, the store phase 7
/// reads it through, and the manifest facts.
struct Built {
    bucket: logweir_engine_oso::storage::VersionedBucket,
    store: Store,
    facts: BackupSetFacts,
}

/// The restored topic: the records each target partition holds, in target
/// offset order, each a byte-for-byte copy of an archived record.
struct Target {
    parts: BTreeMap<i32, Vec<Rec>>,
    reads: Mutex<Vec<(i32, i64, usize)>>,
    /// `(partition, offset)`: from this offset on, `consume_range` returns
    /// nothing below the high watermark, as the real reader does when
    /// librdkafka reports end-of-partition early (`rdkafka_reader.rs` breaks
    /// on `PartitionEOF`, e.g. over a tail of control records).
    stops_at: Option<(i32, i64)>,
}

impl Target {
    fn of(parts: BTreeMap<i32, Vec<Rec>>) -> Target {
        Target {
            parts,
            reads: Mutex::new(Vec::new()),
            stops_at: None,
        }
    }
    fn consumed(&self, p: i32) -> Vec<ConsumedRecord> {
        self.parts
            .get(&p)
            .map(|recs| {
                recs.iter()
                    .enumerate()
                    .map(|(i, r)| ConsumedRecord {
                        partition: p,
                        offset: i as i64,
                        timestamp_ms: r.ts,
                        key: r.key.clone(),
                        value: r.value.clone(),
                        headers: r.headers.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl ClusterReader for Target {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        Ok("TARGET0000000000000000".into())
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        Ok(vec![TopicMeta::new(
            TARGET.to_string(),
            self.parts.len() as i32,
        )])
    }
    fn end_offsets(&self, topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        if topic != TARGET {
            return Err(KafkaError::TopicNotFound(topic.into()));
        }
        Ok(self
            .parts
            .iter()
            .map(|(p, r)| (*p, r.len() as i64))
            .collect())
    }
    fn topic_configs(&self, topic: &str) -> Result<BTreeMap<String, String>, KafkaError> {
        if topic != TARGET {
            return Err(KafkaError::TopicNotFound(topic.into()));
        }
        Ok(BTreeMap::new())
    }
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(BTreeMap::new())
    }
    fn consume_range(
        &self,
        topic: &str,
        partition: i32,
        from: i64,
        max: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        if topic != TARGET {
            return Err(KafkaError::TopicNotFound(topic.into()));
        }
        self.reads.lock().unwrap().push((partition, from, max));
        let stop = match self.stops_at {
            Some((p, at)) if p == partition => at,
            _ => i64::MAX,
        };
        Ok(self
            .consumed(partition)
            .into_iter()
            .filter(|r| r.offset >= from && r.offset < stop)
            .take(max)
            .collect())
    }
}

/// The sampled lane's archive side: what `OsoCliEngine::fingerprints`
/// returns — the selection's first `count` in-window records, by the
/// segments its window overlaps (the engine's selection).
struct SampledEngine {
    archive: BTreeMap<i32, Vec<Vec<Rec>>>,
}

impl DataEngine for SampledEngine {
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
        Err(EngineError::Operational("not used".into()))
    }
    fn preflight(&self, _: &RestorePlan) -> Result<PreflightReport, EngineError> {
        Err(EngineError::Operational("not used".into()))
    }
    fn restore(
        &self,
        _: &RestorePlan,
        _: &mut dyn PhaseObserver,
    ) -> Result<RestoreFacts, EngineError> {
        Err(EngineError::Operational("not used".into()))
    }
    fn fingerprints(&self, s: &SampleSelection) -> Result<Vec<RecordFingerprint>, EngineError> {
        let (w0, w1) = s.window;
        Ok(self
            .archive
            .get(&s.partition)
            .into_iter()
            .flatten()
            .filter(|seg| seg[0].ts <= w1 && seg[seg.len() - 1].ts >= w0)
            .flatten()
            .filter(|r| r.ts >= w0 && r.ts <= w1)
            .take(s.count)
            .map(|r| RecordFingerprint {
                topic: s.topic.clone(),
                partition: s.partition,
                offset: r.offset,
                sha256: logweir_kafka::fingerprint::record_fingerprint(
                    r.key.as_deref(),
                    r.value.as_deref(),
                    &r.headers,
                    r.ts,
                ),
            })
            .collect())
    }
    fn validation_run(&self, _: &RestorePlan) -> Result<EngineRun, EngineError> {
        Ok(EngineRun { exit_code: 0 })
    }
}

fn plan(floor: i64, end: i64) -> RestorePlan {
    RestorePlan {
        set: BackupSetRef {
            backup_id: SET.into(),
            manifest_key: format!("{SET}/manifest.json"),
        },
        storage: StorageUrl::Filesystem {
            path: "/tmp".into(),
        },
        target_bootstrap: vec!["broker:9092".into()],
        target_auth: logweir_core::engine::AuthRender::Plaintext,
        topic_mapping: fixtures::mapping(SOURCE, TARGET),
        time_window: (
            chrono::DateTime::from_timestamp_millis(floor).unwrap(),
            chrono::DateTime::from_timestamp_millis(end).unwrap(),
        ),
        window_floor_source: WindowFloorSource::ArchiveManifest,
        source_partitions: Default::default(),
        default_replication_factor: 1,
        checkpoint_state: "/tmp/logweir/checkpoint.json".into(),
        checkpoint_interval_secs: 30,
        offset_report: "/tmp/logweir/offsets.json".into(),
    }
}

/// Phase 4's selection for `facts`: every listed partition, `count` records
/// over the sample window `[floor, end]`.
fn selections(facts: &BackupSetFacts, floor: i64, end: i64, count: usize) -> Vec<SampleSelection> {
    facts.topics[0]
        .partitions
        .iter()
        .map(|p| SampleSelection {
            set: BackupSetRef {
                backup_id: SET.into(),
                manifest_key: format!("{SET}/manifest.json"),
            },
            topic: SOURCE.into(),
            partition: p.partition_id,
            anchor: Anchor::Head,
            count,
            window: (floor, end),
        })
        .collect()
}

struct Case {
    archive: Archive,
    end: i64,
    target: BTreeMap<i32, Vec<Rec>>,
}

impl Case {
    fn verify(&self, coverage: Coverage, bound: Option<u64>) -> Result<VerifyOutcome, DrillError> {
        let b = self.archive.build();
        self.verify_with(&b.store, &b.facts, coverage, bound)
    }

    fn verify_with(
        &self,
        store: &Store,
        facts: &BackupSetFacts,
        coverage: Coverage,
        bound: Option<u64>,
    ) -> Result<VerifyOutcome, DrillError> {
        let floor = self.archive.floor();
        let engine = SampledEngine {
            archive: self.archive.parts.clone(),
        };
        let reader = Target::of(self.target.clone());
        run_with_coverage(
            &engine,
            &reader,
            store,
            facts,
            &selections(facts, floor, self.end, 25),
            &fixtures::mapping(SOURCE, TARGET),
            &plan(floor, self.end),
            &SourceConfigCoverage::unknown(),
            TargetMode::NewTopic,
            coverage,
            bound,
        )
    }

    fn complete(&self) -> VerifyOutcome {
        self.verify(Coverage::Complete, None)
            .expect("a complete verification is a drill result, not an operational error")
    }

    fn sampled(&self) -> VerifyOutcome {
        self.verify(Coverage::Sampled, None)
            .expect("a sampled verification is a drill result")
    }
}

fn block(v: &VerifyOutcome) -> &CompleteVerification {
    v.integrity
        .verification
        .as_ref()
        .and_then(|b| b.complete.as_ref())
        .expect("a complete verification signs its complete block")
}

fn all_findings(v: &VerifyOutcome) -> String {
    block(v)
        .partitions
        .iter()
        .flat_map(|p| p.findings.iter().cloned())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Two partitions, two segments each, every timestamp increasing.
fn healthy() -> Case {
    let archive = Archive::new()
        .partition(0, &[&[1000, 1100, 1200], &[1300, 1400]])
        .partition(1, &[&[1000, 1500], &[2000, 2500, 3000]]);
    let end = T + 10_000;
    let target = archive.engine_output(end);
    Case {
        archive,
        end,
        target,
    }
}

// =========================================================== the rows

/// The baseline every row below varies: a correct restore passes, covered,
/// with exact counts, every segment verified, header order verified.
#[test]
fn a_correct_restore_passes_complete_verification_with_exact_counts() {
    let v = healthy().complete();
    assert_eq!(
        v.integrity.result,
        IntegrityResult::Pass,
        "{:?}",
        v.integrity
    );
    assert_eq!(v.integrity.level, IntegrityLevel::ByteFingerprint);
    let b = block(&v);
    assert!(b.covered && b.incomplete_reason.is_none());
    assert_eq!(
        (
            b.archive.segments,
            b.archive.segments_verified,
            b.archive.records_decoded
        ),
        (4, 4, 10)
    );
    assert!(b.replay.is_exact(), "{:?}", b.replay);
    assert_eq!(b.replay.expected, 10);
    assert_eq!(
        b.window.start_ms, None,
        "an archive-floor plan has no lower bound"
    );
    assert_eq!(b.window.end_ms, T + 10_000);
    let ver = v.integrity.verification.as_ref().unwrap();
    assert_eq!(
        (
            ver.coverage.as_str(),
            ver.header_order.as_str(),
            ver.comparison_basis.as_str(),
            ver.application.as_str()
        ),
        ("complete", "verified", "archive", "notAttempted")
    );
    assert_eq!(
        (
            v.integrity.records_sampled,
            v.integrity.records_sampled_matching,
            v.integrity.mismatches
        ),
        (10, 10, 0)
    );
    assert_eq!(v.integrity.pass_rate_measured, Some(1.0));
}

/// The sampled lane signs what it is: coverage sampled, header order NOT
/// verified, no complete block.
#[test]
fn a_sampled_verification_says_it_sampled_and_did_not_verify_header_order() {
    let v = healthy().sampled();
    assert_eq!(
        v.integrity.result,
        IntegrityResult::Pass,
        "{:?}",
        v.integrity
    );
    let ver = v
        .integrity
        .verification
        .as_ref()
        .expect("1.4.0 writes the block");
    assert_eq!(ver.coverage, "sampled");
    assert_eq!(ver.header_order, "notVerified");
    assert!(ver.complete.is_none());
}

/// 08-1 (PROD-01.1 ts-pit): a segment whose FIRST record is after the point
/// holds a record at or before it; the engine skips the segment. The complete
/// lane expects the record from its own timestamp and FAILS naming it. The
/// sampled lane, whose archive side selects by the same first/last bounds,
/// passes (the gap), and a target that holds the record passes complete.
#[test]
fn a_record_skipped_by_first_last_selection_fails_complete_and_passes_sampled() {
    let archive = Archive::new()
        .partition(0, &[&[2000, 2100, 2200, 2300], &[9000, 2500, 9100, 9200]])
        .partition(1, &[&[2000, 5000, 5000, 5000]]);
    let end = T + 5000;
    let case = Case {
        target: archive.engine_output(end),
        archive,
        end,
    };
    let v = case.complete();
    assert_eq!(
        v.integrity.result,
        IntegrityResult::Fail,
        "{:?}",
        v.integrity
    );
    let b = block(&v);
    assert_eq!(
        (b.replay.expected, b.replay.restored, b.replay.missing),
        (9, 8, 1)
    );
    assert!(
        all_findings(&v).contains("source offset 5 is missing from the target"),
        "{}",
        all_findings(&v)
    );
    // Negative control 1: the sampled lane cannot see it.
    assert_eq!(case.sampled().integrity.result, IntegrityResult::Pass);
    // Negative control 2: the record restored, the same model passes.
    let fixed = Case {
        target: case.archive.exact_output(end),
        archive: Archive::new()
            .partition(0, &[&[2000, 2100, 2200, 2300], &[9000, 2500, 9100, 9200]])
            .partition(1, &[&[2000, 5000, 5000, 5000]]),
        end,
    };
    assert_eq!(fixed.complete().integrity.result, IntegrityResult::Pass);
}

/// 08-2 (PROD-01.1 ts-bound): a wholly-inside segment holds a record past
/// the point; the engine correctly leaves it out. The complete lane passes;
/// the sampled lane's first/last count BOUND fails the correct restore.
#[test]
fn a_correct_point_in_time_restore_passes_complete_where_the_bound_fails_it() {
    let archive = Archive::new()
        .partition(0, &[&[2000, 6000, 2400, 2600]])
        .partition(1, &[&[2000, 2100, 2200, 2300], &[9000, 9100, 9200, 9300]])
        .partition(2, &[&[2000, 2100, 2200, 2300]]);
    let end = T + 5000;
    let case = Case {
        target: archive.engine_output(end),
        archive,
        end,
    };
    let v = case.complete();
    assert_eq!(
        v.integrity.result,
        IntegrityResult::Pass,
        "{:?}",
        v.integrity
    );
    assert_eq!(block(&v).replay.expected, 11);
    let s = case.sampled();
    assert_eq!(
        s.integrity.result,
        IntegrityResult::Fail,
        "the bound's false failure"
    );
    assert!(
        s.integrity
            .partial_reason
            .as_deref()
            .unwrap_or_default()
            .contains("manifest bounds the window"),
        "{:?}",
        s.integrity.partial_reason
    );
}

/// 08-3 (PROD-01.1 ts-floor): a record older than every segment's first
/// record is below the plan's window floor; the engine drops it from every
/// restore. The complete model has no lower bound under an archive floor, so
/// it is expected and reported missing — at a point the segment straddles,
/// where the sampled lane PASSES — until PROD-01.1b moves the floor.
#[test]
fn a_record_below_the_window_floor_is_missing_under_complete_and_passes_sampled_at_a_point() {
    let archive = Archive::new()
        .partition(0, &[&[2000, 1000, 3000, 4000]])
        .partition(1, &[&[2000, 2000, 2000, 2500]])
        .partition(2, &[&[2100, 2200, 2300, 2400]]);
    let end = T + 3500;
    let case = Case {
        target: archive.engine_output(end),
        archive,
        end,
    };
    let v = case.complete();
    assert_eq!(v.integrity.result, IntegrityResult::Fail);
    assert!(
        all_findings(&v).contains("source offset 1 is missing"),
        "{}",
        all_findings(&v)
    );
    assert_eq!(
        case.sampled().integrity.result,
        IntegrityResult::Pass,
        "signed as a pass today"
    );
}

/// Corrupt an UNSAMPLED segment: the bytes of a segment outside every
/// sampled window (after the point) no longer match the manifest. The
/// complete lane hashes every segment and fails naming it; the sampled lane
/// never reads it and passes.
#[test]
fn a_corrupt_segment_outside_the_sample_fails_complete_and_passes_sampled() {
    let case = Case {
        archive: Archive::new()
            .partition(0, &[&[1000, 1100], &[1200, 1300]])
            .partition(1, &[&[1000, 1100], &[90_000, 90_100]]),
        end: T + 5000,
        target: BTreeMap::new(),
    };
    let case = Case {
        target: case.archive.engine_output(case.end),
        ..case
    };
    let Built {
        bucket,
        store,
        facts,
    } = case.archive.build();
    let corrupt = facts.topics[0].partitions[1].segments[1].key.clone();
    let mut bytes = store.get(&corrupt).unwrap().0;
    bytes[40] ^= 0xFF;
    bucket.overwrite(&corrupt, &bytes);
    let v = case
        .verify_with(&store, &facts, Coverage::Complete, None)
        .unwrap();
    assert_eq!(v.integrity.result, IntegrityResult::Fail);
    let b = block(&v);
    assert_eq!(b.archive.segments_failed, vec![corrupt.clone()]);
    assert!(
        !b.covered,
        "a partition whose segment failed is not compared"
    );
    assert!(all_findings(&v).contains("does not match the manifest's sha256"));
    let s = case
        .verify_with(&store, &facts, Coverage::Sampled, None)
        .unwrap();
    assert_eq!(
        s.integrity.result,
        IntegrityResult::Pass,
        "the sampled lane never read it"
    );
}

/// Omit a segment, two ways. From the STORE: the manifest lists it and the
/// object is gone — a signed fail naming it, not an operational exit 1. From
/// the TARGET: one segment's records were never restored — missing, counted
/// exactly.
#[test]
fn an_omitted_segment_fails_whether_the_store_or_the_target_lacks_it() {
    let case = healthy();
    let Built {
        bucket: _bucket,
        store,
        mut facts,
    } = case.archive.build();
    let gone = facts.topics[0].partitions[0].segments[1].clone();
    let mut ghost = gone.clone();
    ghost.key = format!("{}-never-written", gone.key);
    facts.topics[0].partitions[0].segments[1] = ghost.clone();
    let v = case
        .verify_with(&store, &facts, Coverage::Complete, None)
        .unwrap();
    assert_eq!(v.integrity.result, IntegrityResult::Fail);
    assert_eq!(block(&v).archive.segments_failed, vec![ghost.key]);
    assert!(all_findings(&v).contains("the store does not hold it"));

    let mut target = case.target.clone();
    target.get_mut(&1).unwrap().truncate(2);
    let v = Case {
        target,
        ..healthy()
    }
    .complete();
    assert_eq!(v.integrity.result, IntegrityResult::Fail);
    let p1 = &block(&v).partitions[1];
    assert_eq!(
        (p1.replay.expected, p1.replay.restored, p1.replay.missing),
        (5, 2, 3)
    );
}

/// 08-4, duplicates: a resent batch's records appear twice. An offset-keyed
/// map collapses them (`compare` reconciles every archive record); the
/// complete lane counts the duplicate and the exact count fails.
#[test]
fn a_duplicated_record_is_counted_not_collapsed() {
    let mut case = healthy();
    let dup = case.target[&0][2].clone();
    case.target.get_mut(&0).unwrap().insert(3, dup);
    let v = case.complete();
    assert_eq!(v.integrity.result, IntegrityResult::Fail);
    let p0 = &block(&v).partitions[0];
    assert_eq!(
        (p0.replay.duplicates, p0.replay.restored, p0.replay.expected),
        (1, 6, 5)
    );
    assert!(p0
        .findings
        .iter()
        .any(|f| f.contains("source offset 2 is restored again")));
    // The collapse, measured: the offset-keyed comparison alone sees nothing.
    let target = Target::of(case.target.clone());
    let archive_fp: Vec<RecordFingerprint> = case.archive.parts[&0]
        .iter()
        .flatten()
        .map(|r| RecordFingerprint {
            topic: SOURCE.into(),
            partition: 0,
            offset: r.offset,
            sha256: logweir_kafka::fingerprint::record_fingerprint(
                r.key.as_deref(),
                r.value.as_deref(),
                &r.headers,
                r.ts,
            ),
        })
        .collect();
    let (sampled, matching, _) = compare(&archive_fp, &target.consumed(0));
    assert_eq!((sampled, matching), (5, 5), "the map collapses the copy");
    assert_eq!(lineage_faults(&target.consumed(0)).len(), 1);
}

/// 08-4, order: two records swapped. Content and count still match; the
/// complete lane reports the out-of-order record and fails; the sampled lane,
/// over the same head, now fails too (its share of 08-4).
#[test]
fn reordered_records_fail_on_their_lineage_order() {
    let mut case = healthy();
    case.target.get_mut(&1).unwrap().swap(1, 2);
    let v = case.complete();
    assert_eq!(v.integrity.result, IntegrityResult::Fail);
    let p1 = &block(&v).partitions[1];
    assert_eq!(
        (
            p1.replay.out_of_order,
            p1.replay.matching,
            p1.replay.restored
        ),
        (1, 5, 5)
    );
    assert_eq!(case.sampled().integrity.result, IntegrityResult::Fail);
}

/// 08-6: headers reordered on one record. The ordered comparison fails it;
/// the sorted fingerprint cannot see it, and the sampled lane's block says
/// header order was not verified.
#[test]
fn reordered_headers_fail_complete_and_are_disclosed_by_sampled() {
    let mut case = healthy();
    let r = &mut case.target.get_mut(&0).unwrap()[1];
    r.headers.swap(0, 1);
    let v = case.complete();
    assert_eq!(v.integrity.result, IntegrityResult::Fail);
    assert_eq!(block(&v).partitions[0].replay.mismatched, 1);
    let s = case.sampled();
    assert_eq!(s.integrity.result, IntegrityResult::Pass);
    assert_eq!(
        s.integrity.verification.as_ref().unwrap().header_order,
        "notVerified"
    );
}

/// Compaction holes: a compacted source's sparse offsets are archived and
/// restored as they stood; the restore is correct and passes, and the holes
/// are DISCLOSED, net of a recorded gap.
#[test]
fn compaction_holes_pass_and_are_disclosed() {
    let mut archive = Archive::new().partition(0, &[&[1000, 1100, 1200, 1300]]);
    // Source offsets 3, 4, 7, 9: holes at 5, 6 and 8.
    let seg = &mut archive.parts.get_mut(&0).unwrap()[0];
    for (r, o) in seg.iter_mut().zip([3i64, 4, 7, 9]) {
        *r = archived(o, r.ts);
    }
    let end = T + 5000;
    let case = Case {
        target: archive.engine_output(end),
        archive,
        end,
    };
    let v = case.complete();
    assert_eq!(
        v.integrity.result,
        IntegrityResult::Pass,
        "{:?}",
        v.integrity
    );
    assert_eq!(block(&v).archive.offset_holes, 3);
    // A recorded gap over offset 8 explains one of them.
    let Built {
        bucket: _bucket,
        store,
        mut facts,
    } = case.archive.build();
    facts.topics[0].partitions[0].gaps = vec![(8, 8)];
    let v = case
        .verify_with(&store, &facts, Coverage::Complete, None)
        .unwrap();
    assert_eq!(block(&v).archive.offset_holes, 2);
    assert_eq!(v.integrity.verification.as_ref().unwrap().gaps.len(), 1);
}

/// The bound: complete coverage is never silently replaced by sampling. A
/// bound below the archive's size stops it, the block says `covered: false`
/// naming the bound, and a perfect restore is NOT a pass. Control: a bound at
/// the archive's size passes.
#[test]
fn a_bound_that_stops_complete_verification_is_incomplete_never_a_pass() {
    let case = healthy();
    let v = case.verify(Coverage::Complete, Some(6)).unwrap();
    assert_eq!(
        v.integrity.result,
        IntegrityResult::Partial,
        "{:?}",
        v.integrity
    );
    let b = block(&v);
    assert!(!b.covered);
    assert!(
        b.incomplete_reason
            .as_deref()
            .unwrap()
            .contains("sample.complete_max_records = 6"),
        "{:?}",
        b.incomplete_reason
    );
    assert_eq!(b.max_records, Some(6));
    assert!(!b.partitions[1].compared && b.partitions[0].compared);
    assert_eq!(b.archive.segments_unverified.len(), 2);
    // Review L-4: each lane says its own thing about the stopped partition,
    // so `partial_reason` names it twice in two different sentences, never the
    // same sentence twice.
    let reason = v.integrity.partial_reason.as_deref().unwrap_or_default();
    let notes: Vec<&str> = reason.split("; ").collect();
    let mut unique = notes.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), notes.len(), "a note repeated: {reason}");
    assert!(
        reason.contains("orders/1: 2 archived segments were not read, past the bound"),
        "{reason}"
    );
    let ok = case.verify(Coverage::Complete, Some(10)).unwrap();
    assert_eq!(ok.integrity.result, IntegrityResult::Pass);
}

/// A target partition the manifest does not list holds records: every one is
/// unexpected. The partition is in the block, and the verdict fails.
#[test]
fn records_in_a_target_partition_the_archive_does_not_list_are_unexpected() {
    let mut case = healthy();
    let stray = case.target[&0][0].clone();
    case.target.insert(2, vec![stray]);
    let v = case.complete();
    assert_eq!(v.integrity.result, IntegrityResult::Fail);
    let p2 = &block(&v).partitions[2];
    assert_eq!((p2.partition, p2.segments, p2.replay.unexpected), (2, 0, 1));
}

/// An archive whose records carry no `x-original-offset` cannot be mapped
/// back: not compared, Partial — never a pass, never a fabricated fail.
#[test]
fn an_archive_without_lineage_headers_is_not_compared() {
    let mut case = healthy();
    for seg in case.archive.parts.values_mut().flatten() {
        for r in seg.iter_mut() {
            r.headers.retain(|(k, _)| k != LINEAGE_HEADER);
        }
    }
    case.target = case.archive.engine_output(case.end);
    let v = case.complete();
    assert_eq!(
        v.integrity.result,
        IntegrityResult::Partial,
        "{:?}",
        v.integrity
    );
    assert!(!block(&v).covered);
    assert!(all_findings(&v).contains("carry no x-original-offset"));
}

/// A segment written before 0.21 (no sha256) cannot be verified: Partial.
#[test]
fn a_segment_without_a_sha256_leaves_its_partition_unverified() {
    let case = healthy();
    let Built {
        bucket: _bucket,
        store,
        mut facts,
    } = case.archive.build();
    facts.topics[0].partitions[1].segments[0].sha256.clear();
    let v = case
        .verify_with(&store, &facts, Coverage::Complete, None)
        .unwrap();
    assert_eq!(v.integrity.result, IntegrityResult::Partial);
    assert_eq!(block(&v).archive.segments_unverified.len(), 1);
}

/// A manifest whose record count disagrees with the segment's decoded
/// records is an archive integrity FAILURE even when the sha256 matches.
#[test]
fn a_manifest_count_that_disagrees_with_the_decoded_segment_fails() {
    let case = healthy();
    let Built {
        bucket: _bucket,
        store,
        mut facts,
    } = case.archive.build();
    facts.topics[0].partitions[0].segments[0].record_count += 1;
    let v = case
        .verify_with(&store, &facts, Coverage::Complete, None)
        .unwrap();
    assert_eq!(v.integrity.result, IntegrityResult::Fail);
    assert!(all_findings(&v).contains("decodes to 3 records, the manifest says 4"));
}

/// The lineage is the LAST `x-original-offset` (PROD-00.3e's prerequisite):
/// a record that already carried its own keeps the backup's after it.
#[test]
fn a_record_carrying_its_own_lineage_header_is_mapped_by_the_last() {
    let mut case = healthy();
    for seg in case.archive.parts.values_mut().flatten() {
        for r in seg.iter_mut() {
            r.headers.insert(
                0,
                (LINEAGE_HEADER.into(), Some(777i64.to_le_bytes().to_vec())),
            );
        }
    }
    case.target = case.archive.engine_output(case.end);
    let v = case.complete();
    assert_eq!(
        v.integrity.result,
        IntegrityResult::Pass,
        "{:?}",
        v.integrity
    );
    assert_eq!(
        lineage_offset(&case.target[&0][0].headers),
        Some(0),
        "the backup's own header is the last"
    );
}

/// The target is read in bounded chunks from offset 0 to its high watermark,
/// never one unbounded read.
#[test]
fn the_target_is_read_whole_in_bounded_chunks() {
    let case = healthy();
    let Built {
        bucket: _bucket,
        store,
        facts,
    } = case.archive.build();
    let reader = Target::of(case.target.clone());
    let floor = case.archive.floor();
    run_with_coverage(
        &SampledEngine {
            archive: case.archive.parts.clone(),
        },
        &reader,
        &store,
        &facts,
        &selections(&facts, floor, case.end, 25),
        &fixtures::mapping(SOURCE, TARGET),
        &plan(floor, case.end),
        &SourceConfigCoverage::unknown(),
        TargetMode::NewTopic,
        Coverage::Complete,
        None,
    )
    .unwrap();
    let reads = reader.reads.lock().unwrap().clone();
    assert!(reads.contains(&(
        0,
        0,
        logweir::drill::phase7_verify::complete::TARGET_READ_CHUNK
    )));
    assert!(reads.contains(&(
        1,
        0,
        logweir::drill::phase7_verify::complete::TARGET_READ_CHUNK
    )));
}

/// The signed document the complete lane produces satisfies every invariant
/// at the writer's version, pass and fail alike — the arms IV-1..IV-7 hold of
/// the writer, not only of hand-built blocks.
#[test]
fn the_complete_block_the_writer_builds_satisfies_the_invariants() {
    let mut short = healthy();
    short.target.get_mut(&1).unwrap().truncate(1);
    for (case, pass) in [(healthy(), true), (short, false)] {
        let v = case.complete();
        let mut sc = fixtures::scorecard_pass();
        sc.format_version = logweir_core::FORMAT_VERSION.into();
        sc.integrity = v.integrity.clone();
        sc.sample.records_expected = block(&v).replay.expected;
        if !pass {
            sc.outcome = logweir_core::outcome::Outcome::FailIntegrity;
            sc.engine.matrix_verdict = logweir_core::outcome::MatrixVerdict::PassDegraded;
        }
        assert_eq!(
            sc.validate_invariants().map_err(|e| e.0),
            Ok(()),
            "pass={pass}"
        );
    }
}

/// **Review M-2: a short read is refused, never a smaller comparison.** The
/// target's partition 0 holds every expected record and then ONE stray record
/// past them; the reader answers nothing from the stray's offset on, below the
/// high watermark. Ending the comparison there would have matched every
/// expected record, undercounted `restored` and signed a covered `pass` over a
/// target that holds an unexpected record. The lane refuses instead —
/// `Operational`, exit 1, nothing signed — naming the offset it stopped at.
/// The negative control is the same target read whole: the stray is counted
/// `unexpected` and the run fails.
#[test]
fn a_short_target_read_is_refused_and_never_a_smaller_comparison() {
    let mut case = healthy();
    let mut stray = case.target[&0][0].clone();
    stray.headers = vec![(LINEAGE_HEADER.into(), Some(999i64.to_le_bytes().to_vec()))];
    let expected_on_p0 = case.target[&0].len() as i64;
    case.target.get_mut(&0).unwrap().push(stray);

    let b = case.archive.build();
    let floor = case.archive.floor();
    let mut reader = Target::of(case.target.clone());
    reader.stops_at = Some((0, expected_on_p0));
    let out = run_with_coverage(
        &SampledEngine {
            archive: case.archive.parts.clone(),
        },
        &reader,
        &b.store,
        &b.facts,
        &selections(&b.facts, floor, case.end, 25),
        &fixtures::mapping(SOURCE, TARGET),
        &plan(floor, case.end),
        &SourceConfigCoverage::unknown(),
        TargetMode::NewTopic,
        Coverage::Complete,
        None,
    );
    match out {
        Err(DrillError::Operational(msg)) => {
            assert!(
                msg.contains(&format!(
                    "stopped at offset {expected_on_p0} below the high watermark {}",
                    expected_on_p0 + 1
                )),
                "{msg}"
            );
        }
        other => panic!("a short read must be refused, got {other:?}"),
    }

    // The control: read whole, the stray is unexpected and the run fails.
    let v = case.complete();
    assert_eq!(v.integrity.result, IntegrityResult::Fail);
    assert_eq!(block(&v).partitions[0].replay.unexpected, 1);
}
