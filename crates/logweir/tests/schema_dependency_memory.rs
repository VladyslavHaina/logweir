//! **PROD-03.0: schema dependency detection holds a bounded amount of memory,
//! whatever the segment.**
//!
//! Detection runs inside a backup that has already succeeded, so a segment
//! that made it hold its records — or a decompression bomb — could OOM-kill a
//! runner whose archive exists (the class of FX-23's M1). The first version
//! decoded every record of a sampled segment into owned keys and values. The
//! security review made it stream, keeping six bytes of each key and value. The
//! Tier A review then measured two shapes that still cost 135–263 MB (M1),
//! fixed in the fix round:
//! - a zstd frame declaring a 2^27 window: the decoder allocated and touched it;
//! - an lz4 body decompressed whole.
//!
//! ONE test, measured in CHILD processes, the way
//! `crates/logweir-engine-oso/tests/offset_report_memory.rs` measures the
//! engine report:
//! 1. the test binary runs itself again with `P030_MEM_CHILD` set;
//! 2. the child runs one detection over one segment file and exits;
//! 3. the parent takes the children's peak resident set from
//!    `getrusage(RUSAGE_CHILDREN)`.
//!
//! That is a safe API; a counting global allocator would need `unsafe`, which
//! the workspace lint forbids. The peak is monotonic over the children, so they
//! run smallest first:
//!
//! | child | segment | must |
//! |---|---|---|
//! | `baseline` | one small record | (the reference) |
//! | `large` | 3 000 records of 64 KiB zero-padded values, zstd, ~190 MiB decompressed | add at most `BOUND`, judged `schemaDependent (sampled)` |
//! | `bomb` | one value of 1 GiB of zeros, zstd level 3, ~40 KiB stored | add at most `BOUND`, `segmentTooLargeForDetection` at the 256 MiB output cap |
//! | `window` | the same bomb in a frame declaring a 2^27 window (review M1) | add at most `BOUND`, `segmentTooLargeForDetection` before decoding |
//! | `lz4` | one ~250 MiB value as an lz4 block, ~1 MiB stored (review M1) | add at most `BOUND`, judged (streamed, never held) |
//! | `stored` | one ~16 MiB incompressible value, uncompressed, just under the 16 MiB stored cap | add at most `BOUND`: the stored bytes are held, nothing more |
//! | `control` | the `large` segment through `kbak::decode_segment`, which materialises its records | add many times `BOUND` (the meter sees a reader that holds records) |
//!
//! The largest addition measured is the documented worst case
//! (`docs/kubernetes.md`, `docs/formats/backup-receipt.md`, release-notes item
//! 45).
use logweir::backup::schema_dependency::{detect_within, DetectionLimits, SegmentSource};
use logweir_core::engine::{BackupSetFacts, PartitionFacts, SegmentFacts, TopicFacts};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

/// The most resident memory one detection may add to the baseline child: the
/// stored segment (at most 16 MiB), the zstd window (at most 8 MiB), the
/// stream buffers, a thousand six-byte prefixes and the allocator's slack.
const BOUND: u64 = 32 << 20;

const LARGE_RECORDS: u64 = 3_000;
const LARGE_VALUE: u64 = 64 << 10;
const BOMB_VALUE: u64 = 1 << 30;
const LZ4_VALUE: u64 = 250 << 20;
/// Just under the 16 MiB stored cap once the header, the frame and the footer
/// are added.
const STORED_VALUE: u64 = (16 << 20) - 1024;

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

/// The 32-byte header, `body` under `codec`, the CRC-32 and the end magic.
fn write_envelope(path: &Path, records: u64, codec: u8, body: &[u8]) {
    let mut out = Vec::with_capacity(body.len() + 40);
    out.extend_from_slice(b"KBAK");
    out.extend_from_slice(&[1, codec, 0, 0]);
    out.extend_from_slice(&records.to_le_bytes());
    out.extend_from_slice(&0i64.to_le_bytes());
    out.extend_from_slice(&(records as i64 - 1).to_le_bytes());
    out.extend_from_slice(body);
    let crc = crc32(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(b"BKAE");
    std::fs::write(path, out).unwrap();
}

/// A zstd body the caller writes through `write`, streamed through the
/// encoder (never held whole), with the frame's window log when given.
fn write_zstd_segment(
    path: &Path,
    records: u64,
    window_log: Option<u32>,
    write: impl FnOnce(&mut dyn std::io::Write),
) {
    let mut enc = zstd::stream::Encoder::new(Vec::new(), 3).unwrap();
    if let Some(w) = window_log {
        enc.window_log(w).unwrap();
    }
    write(&mut enc);
    write_envelope(path, records, 1, &enc.finish().unwrap());
}

/// The bytes before a record's value: its length prefix, timestamp, offset,
/// a null key and the value's length.
fn record_head(offset: i64, len: u64) -> Vec<u8> {
    let frame = 8 + 8 + 4 + 4 + len + 2;
    let mut h = Vec::new();
    h.extend_from_slice(&(frame as u32).to_le_bytes());
    h.extend_from_slice(&1_760_000_000_000i64.to_le_bytes());
    h.extend_from_slice(&offset.to_le_bytes());
    h.extend_from_slice(&(-1i32).to_le_bytes());
    h.extend_from_slice(&(len as i32).to_le_bytes());
    h
}

/// The Confluent framing for schema id 7, then zeros: `len` bytes.
const FRAMED_HEAD: [u8; 6] = [0, 0, 0, 0, 7, 1];

/// One record: a null key and a value of `len` bytes that starts with the
/// Confluent framing for schema id 7, then zeros.
fn write_record(w: &mut dyn std::io::Write, offset: i64, len: u64) {
    w.write_all(&record_head(offset, len)).unwrap();
    let shown = len.min(FRAMED_HEAD.len() as u64);
    w.write_all(&FRAMED_HEAD[..shown as usize]).unwrap();
    std::io::copy(&mut std::io::Read::take(std::io::repeat(0), len - shown), w).unwrap();
    w.write_all(&0u16.to_le_bytes()).unwrap();
}

/// An lz4 length's extension bytes (the format's 255-run encoding).
fn lz4_length(out: &mut Vec<u8>, mut rest: u64) {
    while rest >= 255 {
        out.push(255);
        rest -= 255;
    }
    out.push(rest as u8);
}

/// ONE record whose value is `len` bytes (the framing, then zeros), as an lz4
/// block in lz4_flex's size-prepended format, encoded by hand: the record's
/// head and the framing as literals, the zeros as one match of offset 1 (its
/// length in 255-runs, about `len / 255` bytes), and the header count as the
/// last literals.
fn write_lz4_segment(path: &Path, len: u64) {
    let mut lit = record_head(0, len);
    lit.extend_from_slice(&FRAMED_HEAD);
    lit.push(0); // the first zero, which the match then repeats
    let zeros = len - FRAMED_HEAD.len() as u64 - 1;
    let total = lit.len() as u64 + zeros + 2;
    let mut block = (total as u32).to_le_bytes().to_vec();
    block.push(0xFF); // 15 literals + more, a 15+ match
    lz4_length(&mut block, lit.len() as u64 - 15);
    block.extend_from_slice(&lit);
    block.extend_from_slice(&1u16.to_le_bytes());
    lz4_length(&mut block, zeros - 4 - 15);
    block.push(0x20); // the last sequence: two literals, no match
    block.extend_from_slice(&0u16.to_le_bytes());
    write_envelope(path, 1, 2, &block);
}

/// One uncompressed record of `len` incompressible bytes after the framing.
fn write_stored_segment(path: &Path, len: u64) {
    let mut body = record_head(0, len);
    body.extend_from_slice(&FRAMED_HEAD);
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    body.extend((0..len - FRAMED_HEAD.len() as u64).map(|_| {
        x = x
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (x >> 33) as u8
    }));
    body.extend_from_slice(&0u16.to_le_bytes());
    write_envelope(path, 1, 0, &body);
}

/// The segment, handed over ONCE and moved, never copied — as the store's
/// ranged read hands its bytes over.
struct OneFile(RefCell<Option<Vec<u8>>>);

impl SegmentSource for OneFile {
    fn segment_bounded(&self, _: &str, max: u64, _: Duration) -> Result<Option<Vec<u8>>, String> {
        let bytes = self.0.borrow_mut().take().expect("read once");
        Ok((bytes.len() as u64 <= max).then_some(bytes))
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
    if mode == "control" {
        let records = logweir_engine_oso::kbak::decode_segment(&bytes).unwrap();
        assert_eq!(records.len() as u64, LARGE_RECORDS);
        return;
    }
    let topics = vec!["t".to_string()];
    let records = if mode == "large" {
        LARGE_RECORDS as i64
    } else {
        1
    };
    let got = detect_within(
        &archive(records),
        &topics,
        &OneFile(RefCell::new(Some(bytes))),
        &DetectionLimits::default(),
    );
    let t = &got["t"];
    match mode {
        "baseline" | "lz4" | "stored" => {
            assert_eq!(t.verdict, "schemaDependent", "{mode}: {t:?}");
            assert_eq!(t.basis.as_deref(), Some("complete"), "{mode}: {t:?}");
        }
        "large" => {
            assert_eq!(t.verdict, "schemaDependent", "{t:?}");
            assert_eq!(t.basis.as_deref(), Some("sampled"));
        }
        "bomb" | "window" => {
            assert_eq!(
                t.reason.as_deref(),
                Some("segmentTooLargeForDetection"),
                "{mode}: {t:?}"
            );
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
    let path = |name: &str| dir.path().join(name);
    write_zstd_segment(&path("small"), 1, None, |w| write_record(w, 0, 32));
    write_zstd_segment(&path("large"), LARGE_RECORDS, None, |w| {
        for i in 0..LARGE_RECORDS {
            write_record(w, i as i64, LARGE_VALUE);
        }
    });
    write_zstd_segment(&path("bomb"), 1, None, |w| write_record(w, 0, BOMB_VALUE));
    write_zstd_segment(&path("window"), 1, Some(27), |w| {
        write_record(w, 0, BOMB_VALUE)
    });
    write_lz4_segment(&path("lz4"), LZ4_VALUE);
    write_stored_segment(&path("stored"), STORED_VALUE);
    let stored = |name: &str| std::fs::metadata(path(name)).unwrap().len();
    for name in ["bomb", "window", "lz4"] {
        assert!(
            stored(name) < 2 << 20,
            "{name} is small stored: {}",
            stored(name)
        );
    }
    assert!(
        stored("stored") <= 16 << 20,
        "under the stored cap: {}",
        stored("stored")
    );
    assert!(
        stored("stored") > 15 << 20,
        "near the stored cap: {}",
        stored("stored")
    );

    let baseline = child_peak("baseline", &path("small"));
    let mut worst = 0u64;
    let mut lines = vec![format!("baseline {baseline}")];
    for mode in ["large", "bomb", "window", "lz4", "stored"] {
        let peak = child_peak(mode, &path(mode));
        let added = peak.saturating_sub(baseline);
        worst = worst.max(added);
        lines.push(format!("{mode} {peak} (+{added})"));
        assert!(
            peak <= baseline + BOUND,
            "detection over `{mode}` held {added} bytes over the baseline, more than {BOUND}"
        );
    }
    eprintln!(
        "peak RSS: {}; worst addition {worst} (bound {BOUND})",
        lines.join(", ")
    );
    // THE METER WORKS: a reader that materialises the records is seen.
    let control = child_peak("control", &path("large"));
    eprintln!("peak RSS: control {control}");
    assert!(
        control > baseline + 4 * BOUND,
        "the control child materialised ~190 MiB and the meter saw only {} bytes over the \
         baseline, so it cannot see a reader that holds records",
        control.saturating_sub(baseline)
    );
}
