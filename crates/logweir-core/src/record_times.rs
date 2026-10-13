//! Recovery-point selection over out-of-order timestamps (PROD-01.1b).
//!
//! The archive manifest describes a segment by its FIRST and LAST record
//! timestamps, not its minimum and maximum, and the pinned engine picks the
//! segments a restore reads by those two values before it filters each record
//! by its own timestamp. With timestamps out of order inside a segment that
//! reading goes wrong three ways (decision record PROD-01.1 §2.2): a window
//! floor taken from first records drops an older record from every restore; a
//! segment whose first record is after the window's end, or whose last is
//! before a stated start, is skipped although it holds a record the window
//! selects; and a count bound that reads first/last as min/max fails a correct
//! restore.
//!
//! Before phase 2 the runner reads every segment of every restored partition
//! once (the pass the transaction-marker check makes) and keeps, per segment,
//! its records' minimum and maximum timestamp and how many of them the plan's
//! window selects ([`SegmentTimes`]). Then:
//!
//! 1. [`widen`]: every later step (the window floor guard G-WIN binds and phase
//!    5 re-derives, the time basis, the sample, the count bounds) reads the
//!    facts with each read segment's bounds widened to its records' minimum and
//!    maximum. The floor is then the oldest archived record, so no window
//!    drops one, and the bounds are true bounds.
//! 2. [`skipped`] and [`refusal`]: a segment the engine's first/last rule skips
//!    over the rendered window while it holds a record the window selects is
//!    refused by name, `PointInTimeSelectionIncomplete`, before any target
//!    topic exists. Nothing tells the engine to read it; a later point or an
//!    earlier start does.
//! 3. [`unread_note`]: a segment the restore does not read and the pass could
//!    not read is counted in the scorecard's note, never silent.
//!
//! An archive whose segments hold their records in timestamp order is
//! unchanged by all three: its first and last records are its minimum and
//! maximum.
//!
//! # Global Constraint 1
//!
//! No I/O, no clock, no network: every input is a value the caller read.

use crate::engine::BackupSetFacts;
use crate::guard::TERMINAL_STATE_POINT_IN_TIME_SELECTION_INCOMPLETE;
use crate::replay_selection::ReplaySelection;
use std::collections::BTreeMap;

/// What one decoded segment's records say about time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentTimes {
    /// The oldest record timestamp, epoch milliseconds.
    pub min_ms: i64,
    /// The newest record timestamp, epoch milliseconds.
    pub max_ms: i64,
    /// How many records the plan's window selects
    /// ([`ReplaySelection::selects_timestamp`]).
    pub selected: u64,
}

impl SegmentTimes {
    /// `times` with one more record folded in; `None` is a segment with no
    /// record read yet.
    #[must_use]
    pub fn add(times: Option<Self>, ts: i64, selected: bool) -> Self {
        let n = u64::from(selected);
        match times {
            None => Self {
                min_ms: ts,
                max_ms: ts,
                selected: n,
            },
            Some(t) => Self {
                min_ms: t.min_ms.min(ts),
                max_ms: t.max_ms.max(ts),
                selected: t.selected + n,
            },
        }
    }
}

/// Per segment, by its manifest key: what its records say. A segment the pass
/// did not read has no entry.
pub type RecordTimes = BTreeMap<String, SegmentTimes>;

/// `facts` with every segment in `times` widened to its records' bounds: its
/// `start_timestamp` lowered to the oldest record and its `end_timestamp`
/// raised to the newest. Never narrowed: a record's timestamp is at most its
/// segment's first only when the segment is out of order, so for an archive in
/// order this is `facts` unchanged. A segment without an entry keeps the
/// manifest's first and last.
#[must_use]
pub fn widen(facts: &BackupSetFacts, times: &RecordTimes) -> BackupSetFacts {
    let mut out = facts.clone();
    for seg in out
        .topics
        .iter_mut()
        .flat_map(|t| t.partitions.iter_mut())
        .flat_map(|p| p.segments.iter_mut())
    {
        if let Some(t) = times.get(&seg.key) {
            seg.start_timestamp = seg.start_timestamp.min(t.min_ms);
            seg.end_timestamp = seg.end_timestamp.max(t.max_ms);
        }
    }
    out
}

/// A segment the engine skips although it holds records the window selects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skipped {
    pub topic: String,
    pub partition: i32,
    pub key: String,
    /// The manifest's first and last record timestamps, which the engine reads.
    pub first_ms: i64,
    pub last_ms: i64,
    /// How many of its records the window selects.
    pub selected: u64,
}

/// Every segment of a selected partition that the engine's rule
/// ([`ReplaySelection::segment_overlaps`] over the manifest's first and last
/// timestamps) skips over the rendered window while it holds a record the
/// window selects. The rendered window starts at the plan's stated start, else
/// at `floor_ms`, the floor the plan binds (over the widened facts); it ends
/// at the selection's end.
///
/// `manifest` is the archive as described, NOT widened: the engine reads the
/// manifest's first and last.
#[must_use]
pub fn skipped(
    manifest: &BackupSetFacts,
    times: &RecordTimes,
    selection: &ReplaySelection,
    floor_ms: i64,
) -> Vec<Skipped> {
    let start = selection.window_start_ms.unwrap_or(floor_ms);
    let end = selection.window_end_ms;
    let mut out = Vec::new();
    for t in manifest
        .topics
        .iter()
        .filter(|t| selection.topics.contains(&t.name))
    {
        for p in t
            .partitions
            .iter()
            .filter(|p| selection.selects_partition(&t.name, p.partition_id))
        {
            for seg in &p.segments {
                let Some(times) = times.get(&seg.key) else {
                    continue;
                };
                if times.selected > 0
                    && !ReplaySelection::segment_overlaps(
                        start,
                        end,
                        seg.start_timestamp,
                        seg.end_timestamp,
                    )
                {
                    out.push(Skipped {
                        topic: t.name.clone(),
                        partition: p.partition_id,
                        key: seg.key.clone(),
                        first_ms: seg.start_timestamp,
                        last_ms: seg.end_timestamp,
                        selected: times.selected,
                    });
                }
            }
        }
    }
    out
}

/// How many skipped segments a refusal names; the rest are counted.
const NAMED: usize = 5;

/// The refusal for `skipped` over the window `[start_ms, end_ms]`, or `None`
/// when it is empty. The message opens with
/// [`TERMINAL_STATE_POINT_IN_TIME_SELECTION_INCOMPLETE`], so the runner's
/// `refusal-reason=` line names it, and names each segment (at most five, the
/// rest counted) with what moves the window over it.
#[must_use]
pub fn refusal(skipped: &[Skipped], start_ms: i64, end_ms: i64) -> Option<String> {
    if skipped.is_empty() {
        return None;
    }
    let total: u64 = skipped.iter().map(|s| s.selected).sum();
    let mut named: Vec<String> = skipped
        .iter()
        .take(NAMED)
        .map(|s| {
            let remedy = if s.start_after(end_ms) {
                format!(
                    "a recovery point at or after epoch-ms {} reads it",
                    s.first_ms
                )
            } else {
                format!(
                    "a window start at or before epoch-ms {} reads it",
                    s.last_ms
                )
            };
            format!(
                "segment `{}` of topic `{}` partition {} (first record epoch-ms {}, last \
                 epoch-ms {}) holds {} ({remedy})",
                s.key, s.topic, s.partition, s.first_ms, s.last_ms, s.selected
            )
        })
        .collect();
    if skipped.len() > NAMED {
        named.push(format!("and {} more segment(s)", skipped.len() - NAMED));
    }
    Some(format!(
        "{TERMINAL_STATE_POINT_IN_TIME_SELECTION_INCOMPLETE}: the restore window [{start_ms}, \
         {end_ms}] selects {total} archived record(s) the engine would not restore. The engine \
         reads only the segments whose first and last record timestamps overlap the window, and \
         these segments hold records of the window out of timestamp order: {}. The run stopped \
         before any target topic was created. Choose a window that reads every such segment.",
        named.join("; ")
    ))
}

impl Skipped {
    /// Whether the engine skips it at the window's END (its first record is
    /// after the end), rather than below a stated start.
    fn start_after(&self, end_ms: i64) -> bool {
        self.first_ms > end_ms
    }
}

/// What a run adds to its scorecard's `sample.coverage_note`, and prints, when
/// `n` segments the restore does not read could not be read for their record
/// times: they never refuse the run (the restore does not read them), and they
/// are never silent.
#[must_use]
pub fn unread_note(n: usize) -> String {
    format!(
        "the record-time check could not read {n} archive segment(s) the restore does not read; \
         a record of the window held there out of timestamp order is not restored and not \
         detected"
    )
}

/// The refusal for a segment the restore READS and the pass cannot decode: it
/// can neither be checked for transaction markers nor bounded by its records'
/// own timestamps. Not a terminal state of its own (`GuardRefused`).
#[must_use]
pub fn unreadable_refusal(key: &str, topic: &str, partition: i32, why: &str) -> String {
    format!(
        "the runner could not read archive segment `{key}` of topic `{topic}` partition \
         {partition} ({why}), which the restore reads, so it cannot bound the window by its \
         records' own timestamps (nor, unless the plan allows them, check it for transaction \
         markers). The run stopped before any target topic was created."
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{PartitionFacts, SegmentFacts, TopicFacts};

    /// `T` of decision record §2.2: `2025-10-09T08:53:20Z`.
    const T: i64 = 1_760_000_000_000;

    /// One topic `t`, each partition a list of segments, each segment its
    /// records' offsets from `T` in archive order. The manifest's first and
    /// last are the first and last records', as the engine writes them.
    fn archive(parts: &[(i32, &[&[i64]])]) -> (BackupSetFacts, Vec<(String, Vec<i64>)>) {
        let mut records = Vec::new();
        let partitions = parts
            .iter()
            .map(|(id, segs)| PartitionFacts {
                partition_id: *id,
                segments: segs
                    .iter()
                    .enumerate()
                    .map(|(i, recs)| {
                        let key = format!("t/{id}/{i}");
                        records.push((key.clone(), recs.iter().map(|d| T + d).collect()));
                        SegmentFacts {
                            key,
                            start_offset: 0,
                            end_offset: recs.len() as i64 - 1,
                            start_timestamp: T + recs[0],
                            end_timestamp: T + recs[recs.len() - 1],
                            record_count: recs.len() as i64,
                            sha256: String::new(),
                            uploaded_at: 0,
                        }
                    })
                    .collect(),
                gaps: Vec::new(),
                pruned: Vec::new(),
            })
            .collect();
        let facts = BackupSetFacts {
            backup_id: "b".into(),
            created_at: chrono::DateTime::from_timestamp_millis(T).unwrap(),
            source_cluster_id: None,
            manifest_sha256: String::new(),
            manifest_version_id: None,
            consumer_group_snapshot_sha256: None,
            topics: vec![TopicFacts {
                name: "t".into(),
                original_partition_count: Some(parts.len() as i32),
                source_replication_factor: None,
                configurations: BTreeMap::new(),
                partitions,
            }],
        };
        (facts, records)
    }

    fn window(start: Option<i64>, end: i64) -> ReplaySelection {
        ReplaySelection {
            topics: ["t".to_string()].into_iter().collect(),
            window_start_ms: start,
            window_end_ms: end,
            window_end_field: "restore.point_in_time",
            partitions: BTreeMap::new(),
        }
    }

    /// What the runner's pass computes: every segment's times over `sel`.
    fn times_of(records: &[(String, Vec<i64>)], sel: &ReplaySelection) -> RecordTimes {
        records
            .iter()
            .map(|(key, ts)| {
                let t = ts.iter().fold(None, |acc, &ts| {
                    Some(SegmentTimes::add(acc, ts, sel.selects_timestamp(ts)))
                });
                (key.clone(), t.unwrap())
            })
            .collect()
    }

    fn named() -> std::collections::BTreeSet<&'static str> {
        ["t"].into_iter().collect()
    }

    /// **ts-floor** (§2.2): p0 `[+2000, +1000, +3000, +4000]`. The widened
    /// floor is `+1000`, the oldest record, so a full restore and the point
    /// `+3500` both keep p0@1; nothing is skipped; and the count bounds hold
    /// the eleven and twelve records the engine restores. KILLS: the floor
    /// read off first records (`+2000`), `widen` raising a start, the bound
    /// read off the manifest (lower 12 at the full restore is right, at the
    /// point the manifest says [12, 12] for 11).
    #[test]
    fn ts_floor_widens_the_floor_to_the_oldest_record() {
        let (facts, recs) = archive(&[
            (0, &[&[2000, 1000, 3000, 4000]]),
            (1, &[&[2000, 2000, 2000, 2500]]),
            (2, &[&[2100, 2200, 2300, 2400]]),
        ]);
        assert_eq!(
            facts.earliest_covered_timestamp_ms(&named()),
            Some(T + 2000)
        );
        for (end, restored) in [(T + 10_000, 12u64), (T + 3500, 11)] {
            let sel = window(None, end);
            let times = times_of(&recs, &sel);
            let wide = widen(&facts, &times);
            let floor = wide.earliest_covered_timestamp_ms(&named()).unwrap();
            assert_eq!(floor, T + 1000, "the oldest record");
            assert_eq!(skipped(&facts, &times, &sel, floor), vec![]);
            let (lo, up) = crate::engine::expected_restored_count(&wide, floor, end);
            assert!(lo <= restored && restored <= up, "{end}: [{lo}, {up}]");
            let (lo, up) = crate::engine::expected_restored_count(&facts, T + 2000, end);
            if end == T + 10_000 {
                // The manifest's own reading at its own floor: 12, and the
                // engine restored 11 of them.
                assert_eq!((lo, up), (12, 12));
            }
        }
    }

    /// **ts-pit** (§2.2): p0's second segment `[+9000, +2500, +9100, +9200]`
    /// starts after the point `+5000` and holds `+2500`: skipped, named, and
    /// refused with its key and the point that reads it. p2's second segment
    /// `[+2400 … +5000]` overlaps and is read. KILLS: the skip test on the
    /// widened bounds (nothing is skipped then), `selected` ignored, the
    /// refusal dropped.
    #[test]
    fn ts_pit_names_the_skipped_segment() {
        let (facts, recs) = archive(&[
            (0, &[&[2000, 2100, 2200, 2300], &[9000, 2500, 9100, 9200]]),
            (1, &[&[2000, 5000, 5000, 5000]]),
            (2, &[&[2000, 2100, 2200, 2300], &[2400, 4000, 4500, 5000]]),
        ]);
        let sel = window(None, T + 5000);
        let times = times_of(&recs, &sel);
        let floor = widen(&facts, &times)
            .earliest_covered_timestamp_ms(&named())
            .unwrap();
        let s = skipped(&facts, &times, &sel, floor);
        assert_eq!(
            s,
            vec![Skipped {
                topic: "t".into(),
                partition: 0,
                key: "t/0/1".into(),
                first_ms: T + 9000,
                last_ms: T + 9200,
                selected: 1,
            }]
        );
        let text = refusal(&s, floor, T + 5000).unwrap();
        assert!(
            text.starts_with("PointInTimeSelectionIncomplete: ")
                && text.contains("`t/0/1`")
                && text.contains(&format!(
                    "a recovery point at or after epoch-ms {}",
                    T + 9000
                )),
            "{text}"
        );
        // The control: at the segment's first record the engine reads it.
        let later = window(None, T + 9000);
        let times = times_of(&recs, &later);
        assert_eq!(skipped(&facts, &times, &later, floor), vec![]);
    }

    /// **ts-bound** (§2.2): p0 `[+2000, +6000, +2400, +2600]` at `+5000`. Its
    /// first and last are inside the point, so the manifest counts it whole
    /// (12) for the 11 the engine correctly restores; widened to
    /// `[+2000, +6000]` it straddles, and the bound holds 11. Nothing is
    /// skipped. KILLS: `widen` keeping the manifest's last.
    #[test]
    fn ts_bound_holds_the_correct_count() {
        let (facts, recs) = archive(&[
            (0, &[&[2000, 6000, 2400, 2600]]),
            (1, &[&[2000, 2100, 2200, 2300], &[9000, 9100, 9200, 9300]]),
            (2, &[&[2000, 2100, 2200, 2300]]),
        ]);
        let sel = window(None, T + 5000);
        let times = times_of(&recs, &sel);
        let wide = widen(&facts, &times);
        let floor = wide.earliest_covered_timestamp_ms(&named()).unwrap();
        assert_eq!(skipped(&facts, &times, &sel, floor), vec![]);
        assert_eq!(
            crate::engine::expected_restored_count(&facts, floor, T + 5000),
            (12, 12),
            "the manifest's reading"
        );
        let (lo, up) = crate::engine::expected_restored_count(&wide, floor, T + 5000);
        assert!(lo <= 11 && 11 <= up, "[{lo}, {up}]");
        let p0 = &wide.topics[0].partitions[0];
        let (lo, up) = crate::engine::partition_count_bound(p0, floor, T + 5000);
        assert!(lo <= 3 && 3 <= up, "p0 [{lo}, {up}]");
    }

    /// **A stated start** (PROD-11.1's E3, mirrored): p0's first segment
    /// `[S-100, S+50, S-90, S-80]` ends before `S`, so the engine skips it and
    /// `S+50` with it. Refused, with the start that reads it; a start at its
    /// last record reads it. KILLS: the floor used in place of a stated start.
    #[test]
    fn a_segment_ending_before_a_stated_start_is_named() {
        let s0 = 10_000;
        let (facts, recs) = archive(&[(
            0,
            &[
                &[s0 - 100, s0 + 50, s0 - 90, s0 - 80],
                &[s0 + 100, s0 + 130],
            ],
        )]);
        let sel = window(Some(T + s0), T + s0 + 5000);
        let times = times_of(&recs, &sel);
        let floor = widen(&facts, &times)
            .earliest_covered_timestamp_ms(&named())
            .unwrap();
        let s = skipped(&facts, &times, &sel, floor);
        assert_eq!(s.len(), 1);
        assert_eq!((s[0].key.as_str(), s[0].selected), ("t/0/0", 1));
        let text = refusal(&s, T + s0, T + s0 + 5000).unwrap();
        assert!(
            text.contains(&format!(
                "a window start at or before epoch-ms {}",
                T + s0 - 80
            )),
            "{text}"
        );
        let earlier = window(Some(T + s0 - 80), T + s0 + 5000);
        let times = times_of(&recs, &earlier);
        assert_eq!(skipped(&facts, &times, &earlier, floor), vec![]);
        // The in-window record FIRST in its segment (`[S+50, S-90, S-80]`,
        // last before `S`): still named. KILLS: `SegmentTimes::add` dropping
        // the first record's selection (review L1).
        let (facts, recs) = archive(&[(0, &[&[s0 + 50, s0 - 90, s0 - 80], &[s0 + 100]])]);
        let times = times_of(&recs, &sel);
        let s = skipped(&facts, &times, &sel, T + s0 - 90);
        assert_eq!(
            s.iter()
                .map(|s| (s.key.as_str(), s.selected))
                .collect::<Vec<_>>(),
            vec![("t/0/0", 1)]
        );
    }

    /// An archive in order is unchanged, and a segment the pass did not read
    /// keeps the manifest's bounds. A partition the plan does not select is
    /// never named. KILLS: widening every segment, ignoring the subset.
    #[test]
    fn an_archive_in_order_and_an_unread_segment_are_unchanged() {
        let (facts, recs) = archive(&[(0, &[&[1, 2, 3], &[4, 5]]), (1, &[&[9000, 1]])]);
        let sel = window(None, T + 4);
        let mut times = times_of(&recs, &sel);
        times.remove("t/1/0");
        let wide = widen(&facts, &times);
        for (a, b) in facts.topics[0].partitions[0]
            .segments
            .iter()
            .zip(&wide.topics[0].partitions[0].segments)
        {
            assert_eq!(
                (a.start_timestamp, a.end_timestamp),
                (b.start_timestamp, b.end_timestamp)
            );
        }
        assert_eq!(
            wide.topics[0].partitions[1].segments[0].start_timestamp,
            T + 9000
        );
        // p1's segment holds `+1` and starts after the end; selected only
        // when the plan selects partition 1.
        let all = times_of(&recs, &sel);
        assert_eq!(skipped(&facts, &all, &sel, T + 1).len(), 1);
        let mut only_p0 = window(None, T + 4);
        only_p0
            .partitions
            .insert("t".into(), [0].into_iter().collect());
        assert_eq!(skipped(&facts, &all, &only_p0, T + 1), vec![]);
    }

    /// The refusal names at most five segments and counts the rest.
    #[test]
    fn a_long_refusal_names_five_and_counts_the_rest() {
        let s: Vec<Skipped> = (0..7)
            .map(|i| Skipped {
                topic: "t".into(),
                partition: 0,
                key: format!("k{i}"),
                first_ms: 10,
                last_ms: 10,
                selected: 1,
            })
            .collect();
        let text = refusal(&s, 0, 5).unwrap();
        assert!(text.contains("selects 7 archived record(s)"), "{text}");
        assert!(text.contains("`k4`") && !text.contains("`k5`"), "{text}");
        assert!(text.contains("and 2 more segment(s)"), "{text}");
        assert_eq!(refusal(&[], 0, 5), None);
    }
}
