//! **FX-30: the complete verification holds a bounded amount of memory,
//! whatever the archive, and a segment past a cap is a not-covered result,
//! never a crash.**
//!
//! Before FX-30 the complete lane decoded each archived segment whole
//! (`kbak::decode_segment`: the body decompressed into one buffer, every
//! record materialised), so a decompression bomb could OOM-kill the runner
//! mid-verification, and a segment over the store's read cap failed the run
//! with exit 1. Now each segment is streamed (`kbak::digest_segment`) under a
//! stored and a decompressed cap, and past either the partition is not
//! compared: `complete.covered` is `false` and `incomplete_reason` names the
//! caps.
//!
//! ONE test, measured in CHILD processes as
//! `schema_dependency_memory.rs` and `crates/logweir-engine-oso/tests/
//! offset_report_memory.rs` measure theirs: the test binary runs itself again
//! with `FX30_MEM_CHILD` set, the child runs one complete verification and
//! exits, and the parent reads the children's peak resident set from
//! `getrusage(RUSAGE_CHILDREN)` (monotonic over the children, so they run
//! smallest first).
//!
//! The archive has two partitions. Partition 1 is always healthy and carries a
//! captured consumer position (PROD-04.2's probe); partition 0's second
//! segment is what each child varies:
//!
//! | child | partition 0's second segment | must |
//! |---|---|---|
//! | `baseline` | one ordinary record | covered, `pass` (the CONTROL and the reference) |
//! | `large` | 3 000 records of 64 KiB of zeros, zstd, ~190 MiB decompressed, after the window's end | add at most `BOUND`: streamed and hashed, never held; covered, `pass` |
//! | `oversized` | a sparse object one byte over the stored cap, on a filesystem store | add at most `BOUND`; not covered, naming the stored cap, nothing read |
//! | `bomb` | one record whose value is 1 GiB + 1 MiB of zeros, zstd, about 40 KiB stored | add at most `BOUND`; not covered, naming the decompressed cap |
//! | `meter` | (no verification) `kbak::decode_segment` over the `large` segment | add many times `BOUND`: the meter sees a reader that holds records |
//!
//! In the two capped children the healthy partition is still compared, its
//! probe still scans, the capped partition's probe yields no scan, and the run
//! is never a pass, so no positions mapping is written over it
//! (`drill::maps_positions`).
mod fixtures;
mod support;

use logweir::drill::phase7_verify::complete::{
    LINEAGE_HEADER, SEGMENT_DECOMPRESSED_CAP, SEGMENT_STORED_CAP,
};
use logweir::drill::phase7_verify::positions::Probes;
use logweir::drill::phase7_verify::run_after_restore_with_probes;
use logweir_core::backup_receipt::SourceConfigCoverage;
use logweir_core::engine::{
    BackupSetFacts, BackupSetRef, EngineReport, PartitionFacts, RestorePlan, SampleSelection,
    SegmentFacts, StorageUrl, TopicFacts, WindowFloorSource,
};
use logweir_core::outcome::{IntegrityLevel, IntegrityResult};
use logweir_core::spec::{Anchor, Coverage, TargetMode};
use logweir_engine_oso::storage::Store;
use logweir_kafka::reader::ConsumedRecord;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write as _;
use std::path::Path;
use support::doubles::{Engine, Reader, TopicData};

/// The most resident memory one verification may add to the baseline child:
/// the stored bomb (tens of KiB), the zstd window (2 MiB at level 3, at most
/// 8 MiB), the stream buffers, the healthy partition's digests and the
/// allocator's slack.
const BOUND: u64 = 32 << 20;

const T: i64 = 1_760_000_000_000;
const SOURCE: &str = "orders";
const TARGET: &str = "drill-orders";
const SET: &str = "complete-memory-set";
const BOMB_VALUE: u64 = (1 << 30) + (1 << 20);
const LARGE_RECORDS: u64 = 3_000;
const LARGE_VALUE: u64 = 64 << 10;
/// After the plan's window end (`T + 10_000`): archived, never expected.
const LARGE_TS: i64 = T + 20_000;

const TEST_NAME: &str = "the_complete_lane_holds_a_bounded_amount_whatever_the_segment";

fn children_peak_rss() -> u64 {
    let usage = nix::sys::resource::getrusage(nix::sys::resource::UsageWho::RUSAGE_CHILDREN)
        .expect("getrusage(RUSAGE_CHILDREN)");
    let max = u64::try_from(usage.max_rss()).unwrap_or(0);
    if cfg!(target_os = "macos") {
        max
    } else {
        max * 1024
    }
}

fn child_peak(mode: &str, dir: &Path) -> u64 {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST_NAME, "--nocapture", "--test-threads=1"])
        .env("FX30_MEM_CHILD", mode)
        .env("FX30_MEM_DIR", dir)
        .status()
        .expect("the child test process starts");
    assert!(status.success(), "the {mode} child failed: {status}");
    children_peak_rss()
}

/// An archived record as the backup writes it: lineage header included.
fn archived(offset: i64, ts: i64) -> ConsumedRecord {
    ConsumedRecord {
        partition: 0,
        offset,
        timestamp_ms: ts,
        key: Some(format!("k{offset}").into_bytes()),
        value: Some(format!("value at {offset}").into_bytes()),
        headers: vec![(LINEAGE_HEADER.into(), Some(offset.to_le_bytes().to_vec()))],
    }
}

/// The 32-byte header, `body` under `codec`, the CRC-32 and the end magic.
fn envelope(records: u64, first: i64, last: i64, codec: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 40);
    out.extend_from_slice(b"KBAK");
    out.extend_from_slice(&[1, codec, 0, 0]);
    out.extend_from_slice(&records.to_le_bytes());
    out.extend_from_slice(&first.to_le_bytes());
    out.extend_from_slice(&last.to_le_bytes());
    out.extend_from_slice(body);
    let crc = fixtures::crc32_ieee(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(b"BKAE");
    out
}

/// `count` records of `len` zeros from `offset`, at `ts`, streamed through a
/// zstd encoder (never held whole), as one segment.
fn zero_records(offset: i64, count: u64, len: u64, ts: i64) -> Vec<u8> {
    let mut enc = zstd::stream::Encoder::new(Vec::new(), 3).unwrap();
    for i in 0..count {
        let o = offset + i as i64;
        let frame = 8 + 8 + 4 + 4 + len + 2;
        enc.write_all(&(frame as u32).to_le_bytes()).unwrap();
        enc.write_all(&ts.to_le_bytes()).unwrap();
        enc.write_all(&o.to_le_bytes()).unwrap();
        enc.write_all(&(-1i32).to_le_bytes()).unwrap();
        enc.write_all(&(len as i32).to_le_bytes()).unwrap();
        std::io::copy(&mut std::io::Read::take(std::io::repeat(0), len), &mut enc).unwrap();
        enc.write_all(&0u16.to_le_bytes()).unwrap();
    }
    let body = enc.finish().unwrap();
    envelope(count, offset, offset + count as i64 - 1, 1, &body)
}

fn key(p: i32, first: i64) -> String {
    format!("logweir/{SET}/topics/{SOURCE}/partition={p}/segment-{first:020}.bin")
}

fn facts_of(key: String, records: &[ConsumedRecord], bytes: &[u8]) -> SegmentFacts {
    SegmentFacts {
        key,
        start_offset: records[0].offset,
        end_offset: records[records.len() - 1].offset,
        start_timestamp: records[0].timestamp_ms,
        end_timestamp: records[records.len() - 1].timestamp_ms,
        record_count: records.len() as i64,
        sha256: logweir_core::ids::sha256_hex(bytes),
        uploaded_at: T,
    }
}

/// What the child verifies: the objects to store, the manifest facts and the
/// target's records.
struct Case {
    objects: Vec<(String, Vec<u8>)>,
    facts: BackupSetFacts,
    target: Vec<ConsumedRecord>,
}

/// Partition 0: three ordinary records, then `second` (offset 3). Partition
/// 1: three ordinary records. The target holds every ordinary record.
fn case(second: Second, dir: &Path) -> Case {
    let p0: Vec<ConsumedRecord> = (0..3).map(|o| archived(o, T + 100 * o)).collect();
    let p1: Vec<ConsumedRecord> = (0..3)
        .map(|o| ConsumedRecord {
            partition: 1,
            ..archived(o, T + 100 * o)
        })
        .collect();
    let mut objects = Vec::new();
    let mut p0_segments = Vec::new();
    let a = fixtures::kbak_segment(&p0);
    p0_segments.push(facts_of(key(0, 0), &p0, &a));
    objects.push((key(0, 0), a));
    let one = vec![archived(3, T + 500)];
    let mut target = p0.clone();
    match second {
        Second::Ordinary => {
            let b = fixtures::kbak_segment(&one);
            p0_segments.push(facts_of(key(0, 3), &one, &b));
            objects.push((key(0, 3), b));
            target.extend(one);
        }
        Second::Large => {
            let b = std::fs::read(dir.join("large")).unwrap();
            let listed: Vec<ConsumedRecord> = (3..3 + LARGE_RECORDS as i64)
                .map(|o| archived(o, LARGE_TS))
                .collect();
            p0_segments.push(facts_of(key(0, 3), &listed, &b));
            objects.push((key(0, 3), b));
        }
        Second::Bomb => {
            // Written once by the parent: encoding it is not the row.
            let b = std::fs::read(dir.join("bomb")).unwrap();
            assert!(b.len() < 2 << 20, "the bomb is small stored: {}", b.len());
            p0_segments.push(facts_of(key(0, 3), &one, &b));
            objects.push((key(0, 3), b));
        }
        Second::Oversized => {
            // Written by the parent as a sparse file; the manifest's digest
            // is never compared, because the object is never read.
            p0_segments.push(SegmentFacts {
                sha256: "0".repeat(64),
                ..facts_of(key(0, 3), &one, &[])
            });
        }
    }
    let b1 = fixtures::kbak_segment(&p1);
    let p1_segment = facts_of(key(1, 0), &p1, &b1);
    objects.push((key(1, 0), b1));
    target.extend(p1.iter().cloned());
    let facts = BackupSetFacts {
        backup_id: SET.into(),
        created_at: fixtures::ts("2026-09-03T09:00:00Z"),
        source_cluster_id: Some("SRC0000000000000000000".into()),
        manifest_sha256: "sha256:0".into(),
        manifest_version_id: None,
        consumer_group_snapshot_sha256: None,
        topics: vec![TopicFacts {
            name: SOURCE.into(),
            original_partition_count: Some(2),
            source_replication_factor: Some(1),
            configurations: BTreeMap::new(),
            partitions: vec![
                PartitionFacts {
                    partition_id: 0,
                    segments: p0_segments,
                    gaps: vec![],
                    pruned: vec![],
                },
                PartitionFacts {
                    partition_id: 1,
                    segments: vec![p1_segment],
                    gaps: vec![],
                    pruned: vec![],
                },
            ],
        }],
    };
    Case {
        objects,
        facts,
        target,
    }
}

#[derive(Clone, Copy)]
enum Second {
    Ordinary,
    Large,
    Bomb,
    Oversized,
}

fn plan() -> RestorePlan {
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
            chrono::DateTime::from_timestamp_millis(T).unwrap(),
            chrono::DateTime::from_timestamp_millis(T + 10_000).unwrap(),
        ),
        window_floor_source: WindowFloorSource::ArchiveManifest,
        source_partitions: Default::default(),
        default_replication_factor: 1,
        rate_limit_bytes_per_sec: None,
        offset_report: "/tmp/logweir/offsets.json".into(),
    }
}

/// The child's half: one complete verification (or the meter's whole decode).
fn run_child(mode: &str, dir: &Path) {
    if mode == "meter" {
        let bytes = std::fs::read(dir.join("large")).unwrap();
        let records = logweir_engine_oso::kbak::decode_segment(&bytes).unwrap();
        assert_eq!(records.len() as u64, LARGE_RECORDS);
        return;
    }
    let second = match mode {
        "baseline" => Second::Ordinary,
        "large" => Second::Large,
        "bomb" => Second::Bomb,
        "oversized" => Second::Oversized,
        other => panic!("unknown FX30_MEM_CHILD mode {other}"),
    };
    let c = case(second, dir);
    let store = match second {
        // The parent wrote the objects, the sparse one among them.
        Second::Oversized => Store::read_only_from_url(&StorageUrl::Filesystem {
            path: dir.join("fs"),
        })
        .unwrap(),
        _ => {
            let (store, bucket) = Store::in_memory_versioned("");
            for (k, b) in c.objects {
                bucket.overwrite(&k, &b);
            }
            store
        }
    };
    let mut ends: Vec<(i32, i64)> = Vec::new();
    for p in [0, 1] {
        ends.push((
            p,
            c.target.iter().filter(|r| r.partition == p).count() as i64,
        ));
    }
    let mut target = c.target;
    for p in [0, 1] {
        for (i, r) in target.iter_mut().filter(|r| r.partition == p).enumerate() {
            r.offset = i as i64;
        }
    }
    let reader = Reader::target(
        [(
            TARGET.to_string(),
            TopicData {
                end_offsets: ends,
                configs: BTreeMap::new(),
                records: target,
            },
        )]
        .into_iter()
        .collect(),
    );
    let sel: Vec<SampleSelection> = [0, 1]
        .into_iter()
        .map(|p| SampleSelection {
            set: BackupSetRef {
                backup_id: SET.into(),
                manifest_key: format!("{SET}/manifest.json"),
            },
            topic: SOURCE.into(),
            partition: p,
            anchor: Anchor::Head,
            count: 25,
            window: (T, T + 10_000),
        })
        .collect();
    let probes: Probes = [0u32, 1]
        .into_iter()
        .map(|p| ((SOURCE.to_string(), p), BTreeSet::from([1i64])))
        .collect();
    let (v, scans) = run_after_restore_with_probes(
        &Engine::new(),
        &reader,
        &store,
        &c.facts,
        &sel,
        &fixtures::mapping(SOURCE, TARGET),
        &plan(),
        &SourceConfigCoverage::unknown(),
        TargetMode::NewTopic,
        Coverage::Complete,
        None,
        &EngineReport::Absent,
        Some(&probes),
    )
    .unwrap_or_else(|e| panic!("{mode}: a cap is a drill result, never an error: {e}"));
    let block = v
        .integrity
        .verification
        .as_ref()
        .and_then(|b| b.complete.as_ref())
        .expect("a complete block");
    let p = |n: i32| block.partitions.iter().find(|x| x.partition == n).unwrap();
    // The healthy partition is compared in every child, and its probe scans.
    assert!(p(1).compared, "{mode}: {:?}", p(1));
    assert!(scans.contains_key(&(SOURCE.to_string(), 1)), "{mode}");
    if mode == "baseline" || mode == "large" {
        assert!(block.covered, "{mode}: {block:?}");
        if mode == "large" {
            // Every record was read and hashed, not refused.
            assert_eq!(block.archive.records_decoded, 3 + LARGE_RECORDS + 3);
        }
        assert!(block.incomplete_reason.is_none());
        assert_eq!(
            v.integrity.result,
            IntegrityResult::Pass,
            "{:?}",
            v.integrity
        );
        assert!(scans.contains_key(&(SOURCE.to_string(), 0)));
        return;
    }
    let (cap, word) = if mode == "bomb" {
        (SEGMENT_DECOMPRESSED_CAP, "decompressed cap")
    } else {
        (SEGMENT_STORED_CAP, "stored cap")
    };
    assert!(!block.covered, "{mode}: {block:?}");
    assert_ne!(v.integrity.result, IntegrityResult::Pass, "{mode}");
    // A cap is not a segment format this build cannot read: the level stays.
    assert_eq!(v.integrity.level, IntegrityLevel::ByteFingerprint, "{mode}");
    let reason = block.incomplete_reason.as_deref().unwrap_or_default();
    assert!(
        reason.contains("1 segment(s) are past the per-segment caps")
            && reason.contains(&SEGMENT_STORED_CAP.to_string())
            && reason.contains(&SEGMENT_DECOMPRESSED_CAP.to_string()),
        "{mode}: the reason names the caps: {reason}"
    );
    assert!(!p(0).compared, "{mode}");
    let finding = p(0).findings.join("\n");
    assert!(
        finding.contains(&format!("{cap}-byte {word}")),
        "{mode}: the finding names the {word}: {finding}"
    );
    assert_eq!(
        block.archive.segments_unverified,
        vec![key(0, 3)],
        "{mode}: the capped segment is unverified, never failed"
    );
    // No scan for the capped partition, and the run is no pass: no mapping.
    assert!(!scans.contains_key(&(SOURCE.to_string(), 0)), "{mode}");
}

#[test]
fn the_complete_lane_holds_a_bounded_amount_whatever_the_segment() {
    if let Ok(mode) = std::env::var("FX30_MEM_CHILD") {
        let dir = std::env::var("FX30_MEM_DIR").expect("FX30_MEM_DIR");
        run_child(&mode, Path::new(&dir));
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    // The oversized child's filesystem store: every object written, the
    // second segment of partition 0 as a sparse file one byte over the cap.
    let fs = dir.path().join("fs");
    for (k, b) in case(Second::Oversized, dir.path()).objects {
        let path = fs.join(&k);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, b).unwrap();
    }
    let sparse = fs.join(key(0, 3));
    std::fs::File::create(&sparse)
        .unwrap()
        .set_len(SEGMENT_STORED_CAP + 1)
        .unwrap();
    std::fs::write(
        dir.path().join("bomb"),
        zero_records(3, 1, BOMB_VALUE, T + 500),
    )
    .unwrap();
    std::fs::write(
        dir.path().join("large"),
        zero_records(3, LARGE_RECORDS, LARGE_VALUE, LARGE_TS),
    )
    .unwrap();

    let baseline = child_peak("baseline", dir.path());
    let mut lines = vec![format!("baseline {baseline}")];
    for mode in ["oversized", "bomb", "large"] {
        let peak = child_peak(mode, dir.path());
        let added = peak.saturating_sub(baseline);
        lines.push(format!("{mode} {peak} (+{added})"));
        assert!(
            peak <= baseline + BOUND,
            "the complete lane over `{mode}` held {added} bytes over the baseline, more than \
             {BOUND}"
        );
    }
    eprintln!("peak RSS: {}", lines.join(", "));
    // THE METER WORKS: a reader that materialises the records is seen.
    let meter = child_peak("meter", dir.path());
    eprintln!("peak RSS: meter {meter}");
    assert!(
        meter > baseline + 4 * BOUND,
        "the meter child materialised ~190 MiB and the meter saw only {} bytes over the \
         baseline, so it cannot see a reader that holds records",
        meter.saturating_sub(baseline)
    );
}
