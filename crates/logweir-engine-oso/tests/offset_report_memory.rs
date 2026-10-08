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
//! This file holds ONE test, and it measures the read in a CHILD process: the
//! test binary runs itself again with `FX23_MEM_CHILD` set, the child does the
//! one read and exits, and the parent takes the child's peak resident set from
//! `getrusage(RUSAGE_CHILDREN)` (a safe API; a counting global allocator would
//! need `unsafe`, which the workspace lint forbids outside the FFI crate,
//! PROD-04.0b). A baseline child that reads a report with no per-record pairs
//! is measured first; the read may add at most `BOUND` to it, however many
//! pairs the report holds. The control child parses the same file into a
//! `Value` and must add many times `BOUND`, which is what shows the meter would
//! see a reader that materialises the section.
//!
//! `FX23_REPORT_PAIRS` sets the size (default 200,000 pairs, about 23 MB);
//! `FX23_SKIP_CONTROL=1` skips the control.
use logweir_core::engine::EngineReport;
use logweir_engine_oso::engine::read_engine_report;
use std::io::Write;
use std::path::Path;

/// The most resident memory the streaming read may add to the baseline child,
/// whatever the report's size: the `BufReader`'s buffer, serde's scratch and
/// the allocator's own slack.
const BOUND: u64 = 8 << 20;

const TEST_NAME: &str = "reading_the_engine_report_never_holds_its_per_record_section";

/// The peak resident set, in bytes, of every child this process has waited
/// for (`ru_maxrss` is kilobytes on Linux and bytes on macOS).
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

/// Runs this test again in a child, in `mode`, over `path`, and returns the
/// peak resident set of every child so far (the maximum is monotonic, so the
/// caller runs the smallest first).
fn child_peak(mode: &str, path: &Path) -> u64 {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST_NAME, "--nocapture", "--test-threads=1"])
        .env("FX23_MEM_CHILD", mode)
        .env("FX23_MEM_PATH", path)
        .status()
        .expect("the child test process starts");
    assert!(status.success(), "the {mode} child failed: {status}");
    children_peak_rss()
}

/// The child's half: one read of the file, then return (the test passes).
fn run_child(mode: &str, path: &Path) {
    match mode {
        "read" | "baseline" => {
            let report = read_engine_report(path);
            assert!(matches!(report, EngineReport::Read(_)), "{report:?}");
        }
        "control" => {
            let f = std::fs::File::open(path).unwrap();
            let v: serde_json::Value = serde_json::from_reader(std::io::BufReader::new(f)).unwrap();
            assert!(v["detailed_mappings"].is_object());
        }
        other => panic!("unknown FX23_MEM_CHILD mode {other}"),
    }
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
    if let Ok(mode) = std::env::var("FX23_MEM_CHILD") {
        let path = std::env::var("FX23_MEM_PATH").expect("FX23_MEM_PATH");
        run_child(&mode, Path::new(&path));
        return;
    }
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
    let small = dir.join("baseline.json");
    write_report(&small, 0);
    let path = dir.join("offsets.json");
    let size = write_report(&path, pairs);

    // What phase 7 needs is still answered, in this process.
    assert_eq!(
        read_engine_report(&path),
        EngineReport::Read(
            [0, 1, 2]
                .into_iter()
                .map(|p| ("drill-a".to_string(), p))
                .collect()
        ),
        "the reader still answers what phase 7 needs"
    );

    let baseline = child_peak("baseline", &small);
    let read = child_peak("read", &path).saturating_sub(baseline);
    eprintln!(
        "[fx23-mem] report {size} bytes, {pairs} pairs: baseline child {baseline} bytes RSS, \
         read_engine_report adds {read}"
    );
    assert!(
        read < BOUND,
        "reading a {size}-byte report added {read} bytes of resident memory at its peak; the \
         per-record section must be skipped as it streams past, never materialised (bound {BOUND})"
    );

    if std::env::var("FX23_SKIP_CONTROL").as_deref() != Ok("1") {
        // The control: a reader that materialises the document. Its peak must
        // dwarf the bound, or this meter could not see the defect it guards.
        let whole = child_peak("control", &path).saturating_sub(baseline);
        eprintln!("[fx23-mem] control (whole document as Value): adds {whole} bytes");
        assert!(
            whole > 4 * BOUND,
            "the control added only {whole} bytes; the meter cannot tell a streaming reader \
             from a materialising one at this size"
        );
    }
    std::fs::remove_dir_all(&dir).unwrap();
}
