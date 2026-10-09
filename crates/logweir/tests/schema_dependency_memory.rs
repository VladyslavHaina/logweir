//! **PROD-03.0 (the security review of the first version): schema dependency
//! detection holds a bounded amount of memory, whatever the segment.**
//!
//! Detection runs inside a backup that has already succeeded, so a segment
//! that made it hold its records — or a decompression bomb — could OOM-kill a
//! runner whose archive exists (the class of FX-23's M1). The first version
//! decoded every record of a sampled segment into owned keys and values; this
//! one streams it and keeps six bytes of each key and value.
//!
//! ONE test, measured in CHILD processes, the way
//! `crates/logweir-engine-oso/tests/offset_report_memory.rs` measures the
//! engine report: the test binary runs itself again with `P030_MEM_CHILD` set,
//! the child runs one detection over one segment file and exits, and the
//! parent takes the children's peak resident set from
//! `getrusage(RUSAGE_CHILDREN)` — a safe API; a counting global allocator
//! would need `unsafe`, which the workspace lint forbids. The peak is
//! monotonic over the children, so they run smallest first:
//!
//! | child | segment | must |
//! |---|---|---|
//! | `baseline` | one small record | (the reference) |
//! | `large` | 3 000 records of 64 KiB values, ~190 MiB decompressed, under the default caps | add at most `BOUND` to the baseline, and judge it `schemaDependent (sampled)` |
//! | `bomb` | one record whose value is 1 GiB of zeros, ~40 KiB stored | add at most `BOUND`, and read `segmentTooLargeForDetection` at the 256 MiB cap |
//! | `control` | the `large` segment through `kbak::decode_segment`, which materialises its records | add many times `BOUND` — the meter sees a reader that holds the records |
use logweir::backup::schema_dependency::{detect_within, DetectionLimits, SegmentSource};
use logweir_core::engine::{BackupSetFacts, PartitionFacts, SegmentFacts, TopicFacts};
use std::collections::BTreeMap;
use std::path::Path;

/// The most resident memory one detection may add to the baseline child: the
/// stream buffers, the zstd window, a thousand six-byte prefixes and the
/// allocator's slack.
const BOUND: u64 = 32 << 20;

const LARGE_RECORDS: u64 = 3_000;
const LARGE_VALUE: u64 = 64 << 10;
const BOMB_VALUE: u64 = 1 << 30;

const TEST_NAME: &str = "detection_holds_a_bounded_amount_whatever_the_segment";

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

fn child_peak(mode: &str, path: &Path) -> u64 {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST_NAME, "--nocapture", "--test-threads=1"])
        .env("P030_MEM_CHILD", mode)
        .env("P030_MEM_PATH", path)
        .status()
        .expect("the child test process starts");
    assert!(status.success(), "the {mode} child failed: {status}");
    children_peak_rss()
}

/// The header, the zstd body the caller writes through `write`, the CRC-32 and
/// the end magic — the body streamed through the encoder, never held whole.
fn write_segment(path: &Path, records: u64, write: impl FnOnce(&mut dyn std::io::Write)) {
    let mut enc = zstd::stream::Encoder::new(Vec::new(), 3).unwrap();
    write(&mut enc);
    let body = enc.finish().unwrap();
    let mut out = Vec::new();
    out.extend_from_slice(b"KBAK");
    out.extend_from_slice(&[1, 1, 0, 0]);
    out.extend_from_slice(&records.to_le_bytes());
    out.extend_from_slice(&0i64.to_le_bytes());
    out.extend_from_slice(&(records as i64 - 1).to_le_bytes());
    out.extend_from_slice(&body);
    let crc = crc32(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(b"BKAE");
    std::fs::write(path, out).unwrap();
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in bytes {
        crc ^= u32::from(b);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// One record: a null key and a value of `len` bytes that starts with the
/// Confluent framing for schema id 7, then zeros.
fn write_record(w: &mut dyn std::io::Write, offset: i64, len: u64) {
    let frame = 8 + 8 + 4 + 4 + len + 2;
    w.write_all(&(frame as u32).to_le_bytes()).unwrap();
    w.write_all(&1_760_000_000_000i64.to_le_bytes()).unwrap();
    w.write_all(&offset.to_le_bytes()).unwrap();
    w.write_all(&(-1i32).to_le_bytes()).unwrap();
    w.write_all(&(len as i32).to_le_bytes()).unwrap();
    let head = [0u8, 0, 0, 0, 7, 1];
    let shown = len.min(head.len() as u64);
    w.write_all(&head[..shown as usize]).unwrap();
    std::io::copy(&mut std::io::Read::take(std::io::repeat(0), len - shown), w).unwrap();
    w.write_all(&0u16.to_le_bytes()).unwrap();
}

struct OneFile(Vec<u8>);

impl SegmentSource for OneFile {
    fn segment_bounded(&self, _: &str, max: u64) -> Result<Option<Vec<u8>>, String> {
        Ok((self.0.len() as u64 <= max).then(|| self.0.clone()))
    }
}

fn archive(records: i64) -> BackupSetFacts {
    BackupSetFacts {
        backup_id: "b".into(),
        created_at: "2026-10-09T00:00:00Z".parse().unwrap(),
        source_cluster_id: None,
        manifest_sha256: "sha256:00".into(),
        manifest_version_id: None,
        consumer_group_snapshot_sha256: None,
        topics: vec![TopicFacts {
            name: "t".into(),
            original_partition_count: Some(1),
            source_replication_factor: Some(1),
            configurations: BTreeMap::new(),
            partitions: vec![PartitionFacts {
                partition_id: 0,
                segments: vec![SegmentFacts {
                    key: "seg".into(),
                    start_offset: 0,
                    end_offset: records - 1,
                    start_timestamp: 0,
                    end_timestamp: 0,
                    record_count: records,
                    sha256: String::new(),
                    uploaded_at: 0,
                }],
                gaps: vec![],
                pruned: vec![],
            }],
        }],
    }
}

/// The child's half: one detection (or the control's whole decode), then
/// return (the test passes).
fn run_child(mode: &str, path: &Path) {
    let bytes = std::fs::read(path).unwrap();
    let topics = vec!["t".to_string()];
    let limits = DetectionLimits::default();
    match mode {
        "baseline" => {
            let got = detect_within(&archive(1), &topics, &OneFile(bytes), &limits);
            assert_eq!(got["t"].verdict, "schemaDependent", "{got:?}");
        }
        "large" => {
            let got = detect_within(
                &archive(LARGE_RECORDS as i64),
                &topics,
                &OneFile(bytes),
                &limits,
            );
            assert_eq!(got["t"].verdict, "schemaDependent", "{got:?}");
            assert_eq!(got["t"].basis.as_deref(), Some("sampled"));
        }
        "bomb" => {
            let got = detect_within(&archive(1), &topics, &OneFile(bytes), &limits);
            assert_eq!(
                got["t"].reason.as_deref(),
                Some("segmentTooLargeForDetection"),
                "{got:?}"
            );
        }
        "control" => {
            let records = logweir_engine_oso::kbak::decode_segment(&bytes).unwrap();
            assert_eq!(records.len() as u64, LARGE_RECORDS);
        }
        other => panic!("unknown P030_MEM_CHILD mode {other}"),
    }
}

#[test]
fn detection_holds_a_bounded_amount_whatever_the_segment() {
    if let Ok(mode) = std::env::var("P030_MEM_CHILD") {
        let path = std::env::var("P030_MEM_PATH").expect("P030_MEM_PATH");
        run_child(&mode, Path::new(&path));
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let small = dir.path().join("small.kbak");
    write_segment(&small, 1, |w| write_record(w, 0, 32));
    let large = dir.path().join("large.kbak");
    write_segment(&large, LARGE_RECORDS, |w| {
        for i in 0..LARGE_RECORDS {
            write_record(w, i as i64, LARGE_VALUE);
        }
    });
    let bomb = dir.path().join("bomb.kbak");
    write_segment(&bomb, 1, |w| write_record(w, 0, BOMB_VALUE));
    let stored = |p: &Path| std::fs::metadata(p).unwrap().len();
    assert!(
        stored(&bomb) < 1 << 20,
        "the bomb is small stored: {}",
        stored(&bomb)
    );
    assert!(
        stored(&large) < 64 << 20,
        "under the fetch cap: {}",
        stored(&large)
    );

    let baseline = child_peak("baseline", &small);
    let after_large = child_peak("large", &large);
    let after_bomb = child_peak("bomb", &bomb);
    eprintln!(
        "peak RSS: baseline {baseline}, large {after_large}, bomb {after_bomb} (bound {BOUND} \
         over the baseline)"
    );
    assert!(
        after_large <= baseline + BOUND,
        "detection over a ~190 MiB segment held {} bytes over the baseline, more than {BOUND}",
        after_large - baseline
    );
    assert!(
        after_bomb <= baseline + BOUND,
        "detection over a decompression bomb held {} bytes over the baseline, more than {BOUND}",
        after_bomb - baseline
    );
    // THE METER WORKS: a reader that materialises the records is seen.
    let control = child_peak("control", &large);
    eprintln!("peak RSS: control {control}");
    assert!(
        control > baseline + 4 * BOUND,
        "the control child materialised ~190 MiB and the meter saw only {} bytes over the \
         baseline, so it cannot see a reader that holds records",
        control.saturating_sub(baseline)
    );
}
