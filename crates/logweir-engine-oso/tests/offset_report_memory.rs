//! **FX-23 review M1: reading the engine's offset report must not hold its
//! per-record section in memory.**
//!
//! The pinned engine writes one `detailed_mappings` pair per restored record
//! (`restore/engine.rs:1903` in 0.23.3), so the report of a large restore is
//! hundreds of megabytes. Phase 7 needs only `entries[].{topic, partition}`.
//! The first FX-23 reader parsed the whole file — `std::fs::read`, then a
//! `#[serde(flatten)]` catch-all that rebuilt every unmatched key as
//! `serde_json::Value` — at about 750 bytes of memory per record (measured by
//! the review: 1.5 GB peak RSS for a 2,000,000-pair report), enough to
//! OOM-kill a memory-capped runner after a restore that exited 0.
//!
//! This file holds ONE test, so the allocations it meters are its own. A
//! counting global allocator (test-only; it delegates every call to the
//! system allocator) records the peak of live heap bytes while
//! `read_engine_report` reads a synthetic report in the engine's
//! pretty-printed shape. The read must stay under `BOUND` however many pairs
//! the report holds; the control parses the same file into a `Value` and must
//! exceed it many times over, which is what shows the meter would see a reader
//! that materialises the section.
//!
//! `FX23_REPORT_PAIRS` sets the size (default 200,000 pairs, about 23 MB);
//! `FX23_SKIP_CONTROL=1` skips the control, for a measurement run under
//! `/usr/bin/time -l` at millions of pairs.
use logweir_core::engine::EngineReport;
use logweir_engine_oso::engine::read_engine_report;
use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

/// Counts live heap bytes and their peak; every call is the system
/// allocator's own.
struct Counting;

// SAFETY: every method forwards to `System` with the caller's arguments
// unchanged; the two atomics are bookkeeping only.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            let now = LIVE.fetch_add(layout.size(), Ordering::SeqCst) + layout.size();
            PEAK.fetch_max(now, Ordering::SeqCst);
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::SeqCst);
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// The most live heap the read may add, whatever the report's size: the
/// `BufReader`'s buffer, serde's scratch string and the handful of entries.
const BOUND: usize = 1 << 20;

/// Peak live heap bytes `f` adds above what was live when it started.
fn peak_added(f: impl FnOnce()) -> usize {
    let base = LIVE.load(Ordering::SeqCst);
    PEAK.store(base, Ordering::SeqCst);
    f();
    PEAK.load(Ordering::SeqCst) - base
}

/// A report in the shape `serde_json::to_string_pretty(&OffsetMapping)` writes:
/// one entry per target partition and `pairs` per-record pairs spread over
/// them, streamed to disk so writing it holds nothing.
fn write_report(path: &std::path::Path, pairs: u64) -> u64 {
    let mut w = std::io::BufWriter::new(std::fs::File::create(path).unwrap());
    let parts = ["drill-a/0", "drill-a/1", "drill-a/2"];
    writeln!(w, "{{\n  \"entries\": {{").unwrap();
    for (i, k) in parts.iter().enumerate() {
        let (t, p) = k.split_once('/').unwrap();
        write!(
            w,
            "    \"{k}\": {{\n      \"topic\": \"{t}\",\n      \"partition\": {p},\n      \
             \"source_first_offset\": 0,\n      \"source_last_offset\": 9,\n      \
             \"target_first_offset\": null,\n      \"target_last_offset\": null,\n      \
             \"first_timestamp\": 1760000000000,\n      \"last_timestamp\": 1760000150000\n    }}{}\n",
            if i + 1 < parts.len() { "," } else { "" }
        )
        .unwrap();
    }
    writeln!(w, "  }},\n  \"detailed_mappings\": {{").unwrap();
    let per = pairs / parts.len() as u64;
    for (i, k) in parts.iter().enumerate() {
        writeln!(w, "    \"{k}\": [").unwrap();
        for n in 0..per {
            write!(
                w,
                "      {{\n        \"source_offset\": {n},\n        \"target_offset\": {n},\n        \
                 \"timestamp\": {}\n      }}{}\n",
                1_760_000_000_000u64 + n,
                if n + 1 < per { "," } else { "" }
            )
            .unwrap();
        }
        writeln!(w, "    ]{}", if i + 1 < parts.len() { "," } else { "" }).unwrap();
    }
    writeln!(
        w,
        "  }},\n  \"consumer_groups\": {{}},\n  \"source_cluster_id\": null,\n  \
         \"target_cluster_id\": null,\n  \"created_at\": 1760000200000\n}}"
    )
    .unwrap();
    w.flush().unwrap();
    drop(w);
    std::fs::metadata(path).unwrap().len()
}

#[test]
fn reading_the_engine_report_never_holds_its_per_record_section() {
    let pairs: u64 = std::env::var("FX23_REPORT_PAIRS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    let dir = std::env::temp_dir().join(format!(
        "logweir-fx23-report-mem-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("offsets.json");
    let size = write_report(&path, pairs);

    let mut report = None;
    let read = peak_added(|| report = Some(read_engine_report(&path)));
    eprintln!(
        "[fx23-mem] report {size} bytes, {pairs} pairs: read_engine_report peak heap +{read} bytes"
    );
    assert_eq!(
        report.unwrap(),
        EngineReport::Read(
            [0, 1, 2]
                .into_iter()
                .map(|p| ("drill-a".to_string(), p))
                .collect()
        ),
        "the reader still answers what phase 7 needs"
    );
    assert!(
        read < BOUND,
        "reading a {size}-byte report held {read} bytes of heap at its peak; the per-record \
         section must be skipped as it streams past, never materialised (bound {BOUND})"
    );

    if std::env::var("FX23_SKIP_CONTROL").as_deref() != Ok("1") {
        // The control: a reader that materialises the document. Its peak must
        // dwarf the bound, or this meter could not see the defect it guards.
        let whole = peak_added(|| {
            let f = std::fs::File::open(&path).unwrap();
            let v: serde_json::Value = serde_json::from_reader(std::io::BufReader::new(f)).unwrap();
            assert!(v["detailed_mappings"].is_object());
        });
        eprintln!("[fx23-mem] control (whole document as Value): peak heap +{whole} bytes");
        assert!(
            whole > 20 * BOUND,
            "the control held only {whole} bytes; the meter cannot tell a streaming reader \
             from a materialising one at this size"
        );
    }
    std::fs::remove_dir_all(&dir).unwrap();
}
