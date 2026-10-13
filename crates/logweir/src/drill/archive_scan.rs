//! The one pass over the restored partitions' archive segments before phase 2:
//! transaction markers (PROD-01.1a) and record times (PROD-01.1b).
//!
//! Every segment of every selected partition is streamed once through the
//! bounded `kbak::scan_segment` (one stored segment and a few hundred KiB held
//! at a time). For each it keeps its records' minimum and maximum timestamp
//! and how many the window selects (`logweir_core::record_times`), and, unless
//! the plan allows them, counts the records shaped like a transaction marker in
//! the segments the window overlaps and every segment after its end.
//!
//! What a read failure means depends on whether the restore reads the segment
//! (the engine's rule over the manifest's first and last timestamps):
//! - a segment the restore reads and the store does not hold is left to the
//!   restore, whose preflight names it; one it reads and the pass cannot decode
//!   refuses the run (`GuardRefused`), with or without the marker override;
//! - a segment the restore does not read (after the window's end, or ending
//!   before a stated start) that cannot be read never refuses: it is counted in
//!   the scorecard's note.

use crate::drill::DrillError;
use logweir_core::engine::BackupSetFacts;
use logweir_core::guard::GuardRefusal;
use logweir_core::record_times::{RecordTimes, SegmentTimes};
use logweir_core::replay_selection::ReplaySelection;
use logweir_core::transaction_markers::{is_control_shaped, MarkerCounts, SHAPE_PREFIX};
use logweir_engine_oso::kbak::{scan_segment, ScanLimits};
use logweir_engine_oso::storage::{caps, Store, StoreError};

/// What the pass found.
pub struct ArchiveScan {
    /// Marker-shaped records per topic and partition; empty when markers were
    /// not counted.
    pub markers: MarkerCounts,
    /// The segments after the window's end the marker count could not read.
    pub markers_unread: Vec<String>,
    /// Per segment read, what its records say about time.
    pub times: RecordTimes,
    /// The segments the restore does not read whose record times could not be
    /// read.
    pub times_unread: Vec<String>,
}

/// Reads every segment of `window`'s selected partitions in `facts`, counting
/// transaction markers when `count_markers`. See the module doc for what a
/// failed read means.
pub fn run(
    archive: &Store,
    facts: &BackupSetFacts,
    window: &ReplaySelection,
    count_markers: bool,
) -> Result<ArchiveScan, DrillError> {
    let limits = ScanLimits {
        max_decompressed: caps::SEGMENT,
        deadline: None,
    };
    let mut scan = ArchiveScan {
        markers: MarkerCounts::new(),
        markers_unread: Vec::new(),
        times: RecordTimes::new(),
        times_unread: Vec::new(),
    };
    for topic in facts
        .topics
        .iter()
        .filter(|t| window.topics.contains(&t.name))
    {
        for p in topic
            .partitions
            .iter()
            .filter(|p| window.selects_partition(&topic.name, p.partition_id))
        {
            for seg in &p.segments {
                // The engine's segment rule, over the manifest's first and
                // last. Before a stated start the marker check does not look
                // (out-of-order timestamps can put a closing marker there:
                // `docs/stability.md`); the record times do.
                let before = window
                    .window_start_ms
                    .is_some_and(|s| seg.end_timestamp < s);
                let after = seg.start_timestamp > window.window_end_ms;
                let read_by_restore = !before && !after;
                let markers_here = count_markers && !before;
                let unread = |scan: &mut ArchiveScan| {
                    scan.times_unread.push(seg.key.clone());
                    if markers_here {
                        scan.markers_unread.push(seg.key.clone());
                    }
                };
                let bytes = match archive.get_capped(&seg.key, caps::SEGMENT) {
                    Ok((bytes, _)) => bytes,
                    Err(_) if !read_by_restore => {
                        unread(&mut scan);
                        continue;
                    }
                    Err(StoreError::NotFound(_)) => continue,
                    Err(e) => return Err(logweir_core::engine::EngineError::from(e).into()),
                };
                let mut found = 0u64;
                let mut times: Option<SegmentTimes> = None;
                let read = scan_segment(&bytes, &limits, SHAPE_PREFIX, &mut |r| {
                    times = Some(SegmentTimes::add(
                        times,
                        r.timestamp,
                        window.selects_timestamp(r.timestamp),
                    ));
                    if markers_here
                        && is_control_shaped(r.key.as_deref(), r.value.as_deref(), &r.header_keys)
                    {
                        found += 1;
                    }
                    true
                });
                if found > 0 {
                    *scan
                        .markers
                        .entry(topic.name.clone())
                        .or_default()
                        .entry(p.partition_id)
                        .or_default() += found;
                }
                match read {
                    Ok(_) => {
                        if let Some(t) = times {
                            scan.times.insert(seg.key.clone(), t);
                        }
                    }
                    Err(_) if !read_by_restore => unread(&mut scan),
                    Err(e) => {
                        return Err(DrillError::Guard(GuardRefusal(
                            logweir_core::record_times::unreadable_refusal(
                                &seg.key,
                                &topic.name,
                                p.partition_id,
                                &e.to_string(),
                            ),
                        )))
                    }
                }
            }
        }
    }
    Ok(scan)
}

/// The note for `keys`, segments a check could not read, from `note`; printed
/// on stderr with the keys, on one line whatever a key holds
/// (`exit::one_line`). `None` when every segment was read.
pub fn noted(keys: &[String], note: fn(usize) -> String) -> Option<String> {
    (!keys.is_empty()).then(|| {
        let note = note(keys.len());
        eprintln!(
            "{}",
            crate::exit::one_line(&format!("{note}: {}", keys.join(", ")))
        );
        note
    })
}
