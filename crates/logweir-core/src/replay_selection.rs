//! **PROD-11.1 — replay selection.** The ONE function a restore's preview and
//! its execution select records by.
//!
//! A plan may narrow a restore in two ways, both stated in its bytes (inside
//! `plan_hash`): an inclusive window START (`restore.window_start`) and
//! per-topic PARTITION SUBSETS (`restore.partitions`). Absent both, the
//! selection is exactly what every plan before PROD-11.1 restored: every
//! partition of every selected topic, from the archive's floor to the window's
//! end (guard G-WIN).
//!
//! # Who calls it
//!
//! - **The preview**: the restore preflight's `archive.coverage` and
//!   `archive.segments` rows (`logweir::check::kinds::restore`), which read the
//!   manifest through the check's own object seam and hand its topics here.
//! - **Execution**: plan construction (`logweir::drill::build_plan`, the
//!   window's start and its claim, the partition subsets), phase 0 (the shape
//!   refusals), phase 4 (which partitions a sample may come from), phase 5
//!   (the independent re-derivation of the rendered start), phase 7 (which
//!   partitions must hold records, which must be empty, and the expected
//!   output of a complete verification) and the engine adapter (one engine
//!   run per distinct subset).
//!
//! The two sides therefore cannot disagree about which segments, partitions
//! and instants a plan selects: there is one predicate for each, here.
//!
//! # The rules
//!
//! - **Record**: selected when `timestamp <= end` and, when the plan states a
//!   start, `timestamp >= start` — both ends INCLUSIVE, as the engine's own
//!   restore filter is (`r.timestamp >= s && r.timestamp <= e`,
//!   `restore/helpers.rs` in the pinned source). Without a stated start there
//!   is NO lower bound on a record: the window starts at the archive's floor,
//!   and a record older than every segment's first record (PROD-01.1 S8) is
//!   still expected — its absence is the engine's floor dropping it, a fault
//!   (PROD-08.1 §2).
//! - **Partition**: selected when its topic is selected and the plan names no
//!   subset for the topic, or names one containing it.
//! - **Segment** (what the engine reads, and what a preview lists): a segment of
//!   a selected partition whose first/last timestamps overlap
//!   `[start-or-floor, end]` (`overlaps_time_window` in the pinned source).
//!   That is the ENGINE's selection, which PROD-01.1 S6/S7 shows can skip an
//!   in-window record of a non-monotonic segment; a complete verification
//!   computes its expected output from each record's own timestamp instead
//!   ([`ReplaySelection::selects_timestamp`]) and so detects the skip.
//!
//! # What is refused, and never silently widened
//!
//! [`SelectionRefusal`]: a start before the archive's coverage, an empty
//! window, a subset for a topic the plan does not select, an empty subset, a
//! repeated or negative partition, a partition the archive does not list, and
//! a selection no segment overlaps. Each is a plan the archive cannot satisfy
//! as stated, refused before anything runs (exit 3) — never answered by moving
//! the start to the floor or by restoring partitions the plan did not name.
//!
//! # Global Constraint 1
//!
//! No I/O, no clock, no network: every input is a value the caller read.

use crate::engine::{TopicFacts, WindowFloorSource};
use crate::spec::DrillSpec;
use std::collections::{BTreeMap, BTreeSet};

/// A plan's replay selection, read off its bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplaySelection {
    /// The selected source topics (`source.topics`), sorted and unique.
    pub topics: BTreeSet<String>,
    /// The stated INCLUSIVE start, epoch milliseconds. `None` = the archive's
    /// floor (guard G-WIN, `WindowFloorSource::ArchiveManifest`).
    pub window_start_ms: Option<i64>,
    /// The INCLUSIVE end: `restore.point_in_time` when stated, else
    /// `sample.window_end` (the rule `build_plan_with_floor` applies).
    pub window_end_ms: i64,
    /// Which spec field the end came from, for a refusal an operator acts on.
    pub window_end_field: &'static str,
    /// Per-topic partition subsets. A selected topic not named here selects
    /// every partition the archive lists for it.
    pub partitions: BTreeMap<String, BTreeSet<i32>>,
}

/// Why a plan's selection cannot be restored as stated. Every variant is a
/// refusal before anything runs (exit 3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectionRefusal {
    /// `restore.partitions` names a topic `source.topics` does not select.
    SubsetForUnselectedTopic { topic: String },
    /// `restore.partitions.<topic>` is an empty list.
    EmptySubset { topic: String },
    /// A partition is named twice in one subset.
    RepeatedPartition { topic: String, partition: i32 },
    /// A negative partition number.
    NegativePartition { topic: String, partition: i32 },
    /// The stated start is at or after the window's end.
    EmptyWindow {
        start_ms: i64,
        end_ms: i64,
        end_field: &'static str,
    },
    /// The manifest records no segment for any selected topic, so there is no
    /// floor to bind or to check a start against.
    NoCoverage { topics: Vec<String> },
    /// The stated start is earlier than the archive's coverage.
    StartBeforeCoverage { start_ms: i64, floor_ms: i64 },
    /// A subset names a partition the archive does not list for the topic.
    PartitionNotInArchive { topic: String, partition: i32 },
    /// No segment of any selected partition overlaps the window: the restore
    /// would produce nothing, and an empty restore is never a pass.
    EmptySelection { start_ms: i64, end_ms: i64 },
}

impl std::fmt::Display for SelectionRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SubsetForUnselectedTopic { topic } => write!(
                f,
                "restore.partitions names `{topic}`, which source.topics does not select; a \
                 partition subset narrows a selected topic and cannot add one"
            ),
            Self::EmptySubset { topic } => write!(
                f,
                "restore.partitions.{topic} is empty; a subset that selects no partition would \
                 restore nothing from `{topic}`. Remove the topic from source.topics, or name at \
                 least one partition"
            ),
            Self::RepeatedPartition { topic, partition } => write!(
                f,
                "restore.partitions.{topic} names partition {partition} more than once"
            ),
            Self::NegativePartition { topic, partition } => write!(
                f,
                "restore.partitions.{topic} names partition {partition}; a partition number is \
                 never negative"
            ),
            Self::EmptyWindow {
                start_ms,
                end_ms,
                end_field,
            } => write!(
                f,
                "this plan's restore.window_start is epoch-ms {start_ms}, at or after its \
                 {end_field} of epoch-ms {end_ms}, so the restore window [{start_ms}, {end_ms}] \
                 holds no instant after its start and would restore at most one instant; a \
                 window start must be EARLIER than the window's end"
            ),
            Self::NoCoverage { topics } => write!(
                f,
                "the archive set records no segment in its manifest for any of the topics this \
                 restore names ({}), so it has no earliest covered timestamp to bind the window \
                 to or to check a stated window start against",
                topics.join(", ")
            ),
            Self::StartBeforeCoverage { start_ms, floor_ms } => write!(
                f,
                "this plan's restore.window_start is epoch-ms {start_ms}, before the archive \
                 set's earliest covered timestamp of epoch-ms {floor_ms}: the archive does not \
                 cover the start of the requested window. Refused rather than moved to the \
                 archive's floor, which would restore a window nobody approved; state a start \
                 at or after epoch-ms {floor_ms}, or pick a recovery point that covers it"
            ),
            Self::PartitionNotInArchive { topic, partition } => write!(
                f,
                "restore.partitions.{topic} names partition {partition}, which the archive set's \
                 manifest does not list for `{topic}`"
            ),
            Self::EmptySelection { start_ms, end_ms } => write!(
                f,
                "no archived segment of a selected partition overlaps the restore window \
                 [{start_ms}, {end_ms}]: the selection is empty and the restore would produce \
                 nothing, which is never a pass. Widen the window or the partition selection"
            ),
        }
    }
}

/// One selected partition as the manifest describes it for this window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedPartition {
    pub topic: String,
    pub partition: i32,
    /// The manifest keys of the segments the engine reads for the window
    /// (first/last overlap), in manifest order.
    pub segment_keys: Vec<String>,
    /// The records the manifest PROVES the window holds: segments wholly
    /// inside it (by their first and last timestamps).
    pub records_lower: u64,
    /// `records_lower` plus every record of a segment the window cuts across.
    pub records_upper: u64,
}

/// One engine run: the topics it restores and the partition filter it
/// renders. The pinned engine's `restore.source_partitions` applies to EVERY
/// topic of one run, so topics with different subsets need different runs.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct EngineRunSelection {
    /// `None` renders no `source_partitions` key: every partition.
    pub source_partitions: Option<Vec<i32>>,
    /// The source topics this run restores, sorted.
    pub topics: Vec<String>,
}

/// A selection resolved against an archive's manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSelection {
    /// The archive's floor over the selected topics: the minimum FIRST-record
    /// timestamp of their segments (`BackupSetFacts::earliest_covered_timestamp_ms`).
    pub floor_ms: i64,
    /// The window's effective INCLUSIVE start: the stated start, else the floor.
    pub start_ms: i64,
    /// Where the start came from — `RestorePlan.window_floor_source`.
    pub start_source: WindowFloorSource,
    /// The INCLUSIVE end.
    pub end_ms: i64,
    /// Every selected partition the manifest lists, sorted by topic then
    /// partition — including those no segment of the window overlaps.
    pub partitions: Vec<SelectedPartition>,
    /// The engine runs, in render order.
    pub runs: Vec<EngineRunSelection>,
}

impl ResolvedSelection {
    /// Every segment key the engine reads for this selection, sorted, unique.
    #[must_use]
    pub fn segment_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self
            .partitions
            .iter()
            .flat_map(|p| p.segment_keys.iter().cloned())
            .collect();
        keys.sort();
        keys.dedup();
        keys
    }
}

impl ReplaySelection {
    /// Reads the selection off a plan, refusing a subset whose SHAPE is wrong
    /// (an unselected topic, an empty list, a repeated or negative partition)
    /// and a stated start at or after the window's end. Nothing here reads
    /// the archive; [`ReplaySelection::resolve`] does.
    ///
    /// # Errors
    ///
    /// The first [`SelectionRefusal`] found, topics in sorted order.
    pub fn from_spec(spec: &DrillSpec) -> Result<Self, SelectionRefusal> {
        let topics: BTreeSet<String> = spec.source.topics.iter().cloned().collect();
        let (window_end_field, end) = match spec.restore.point_in_time {
            Some(t) => ("restore.point_in_time", t),
            None => ("sample.window_end", spec.sample.window_end),
        };
        let window_end_ms = end.timestamp_millis();
        let window_start_ms = spec.restore.window_start.map(|t| t.timestamp_millis());
        let mut partitions = BTreeMap::new();
        for (topic, list) in &spec.restore.partitions {
            if !topics.contains(topic) {
                return Err(SelectionRefusal::SubsetForUnselectedTopic {
                    topic: topic.clone(),
                });
            }
            if list.is_empty() {
                return Err(SelectionRefusal::EmptySubset {
                    topic: topic.clone(),
                });
            }
            let mut set = BTreeSet::new();
            for &p in list {
                if p < 0 {
                    return Err(SelectionRefusal::NegativePartition {
                        topic: topic.clone(),
                        partition: p,
                    });
                }
                if !set.insert(p) {
                    return Err(SelectionRefusal::RepeatedPartition {
                        topic: topic.clone(),
                        partition: p,
                    });
                }
            }
            partitions.insert(topic.clone(), set);
        }
        if let Some(start_ms) = window_start_ms {
            // `>=`: a window `[t, t]` holds one instant, which is the same
            // refusal `build_plan_with_floor` makes for a recovery point at
            // the floor (plan erratum E7(a)).
            if start_ms >= window_end_ms {
                return Err(SelectionRefusal::EmptyWindow {
                    start_ms,
                    end_ms: window_end_ms,
                    end_field: window_end_field,
                });
            }
        }
        Ok(Self {
            topics,
            window_start_ms,
            window_end_ms,
            window_end_field,
            partitions,
        })
    }

    /// `true` when the plan states no selection of its own — no start, no
    /// subset — so it restores what every plan before PROD-11.1 restored.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.window_start_ms.is_none() && self.partitions.is_empty()
    }

    /// Whether `topic`'s `partition` is selected. A topic the plan does not
    /// select is never selected, whatever its partitions.
    #[must_use]
    pub fn selects_partition(&self, topic: &str, partition: i32) -> bool {
        self.topics.contains(topic)
            && self
                .partitions
                .get(topic)
                .is_none_or(|set| set.contains(&partition))
    }

    /// Whether a record with this timestamp is in the window: `<= end`, and
    /// `>= start` when the plan states one. Both ends inclusive. With no stated
    /// start there is no lower bound (module doc).
    #[must_use]
    pub fn selects_timestamp(&self, ts: i64) -> bool {
        ts <= self.window_end_ms && self.window_start_ms.is_none_or(|s| ts >= s)
    }

    /// The engine's segment rule over the window `[start, end]`: the segment's
    /// first/last timestamps overlap it (closed interval, not containment).
    #[must_use]
    pub fn segment_overlaps(start_ms: i64, end_ms: i64, first_ts: i64, last_ts: i64) -> bool {
        first_ts <= end_ms && last_ts >= start_ms
    }

    /// The engine runs this selection needs over `topics` (a restore's mapped
    /// source topics): one with no partition filter for every topic without a
    /// subset, and one per distinct subset. Deterministic: the unfiltered run
    /// first, then the subsets in ascending order. A selection with no subset
    /// is ONE run with no filter — the document every plan before PROD-11.1
    /// rendered.
    #[must_use]
    pub fn engine_runs<'a>(
        partitions: &BTreeMap<String, BTreeSet<i32>>,
        topics: impl IntoIterator<Item = &'a String>,
    ) -> Vec<EngineRunSelection> {
        let mut by_filter: BTreeMap<Option<Vec<i32>>, Vec<String>> = BTreeMap::new();
        for topic in topics {
            let filter = partitions
                .get(topic)
                .map(|set| set.iter().copied().collect::<Vec<i32>>());
            by_filter.entry(filter).or_default().push(topic.clone());
        }
        by_filter
            .into_iter()
            .map(|(source_partitions, mut topics)| {
                topics.sort();
                topics.dedup();
                EngineRunSelection {
                    source_partitions,
                    topics,
                }
            })
            .collect()
    }

    /// Resolves the selection against the archive's topics: the floor, the
    /// effective start and its source, every selected partition with the
    /// segments the engine reads for it and the record bound the manifest
    /// proves, and the engine runs.
    ///
    /// `topics` is the manifest's topic list (`BackupSetFacts::topics`, or the
    /// preview's projection of the same JSON). Topics the plan does not select
    /// are ignored, as the G-WIN floor ignores them (plan erratum E7(b)). A
    /// selected topic the manifest does not describe contributes nothing
    /// here: whether that is refused is the caller's (`TopicNotInBackupSet`
    /// in the preview).
    ///
    /// # Errors
    ///
    /// [`SelectionRefusal::NoCoverage`], [`SelectionRefusal::StartBeforeCoverage`],
    /// [`SelectionRefusal::PartitionNotInArchive`] or
    /// [`SelectionRefusal::EmptySelection`], in that order.
    pub fn resolve(&self, topics: &[TopicFacts]) -> Result<ResolvedSelection, SelectionRefusal> {
        let named: Vec<&TopicFacts> = topics
            .iter()
            .filter(|t| self.topics.contains(&t.name))
            .collect();
        // The G-WIN floor, by the same walk `earliest_covered_timestamp_ms`
        // takes: the minimum FIRST-record timestamp over every segment of
        // every partition of every selected topic.
        let floor_ms = named
            .iter()
            .flat_map(|t| t.partitions.iter())
            .flat_map(|p| p.segments.iter())
            .map(|s| s.start_timestamp)
            .min()
            .ok_or_else(|| SelectionRefusal::NoCoverage {
                topics: self.topics.iter().cloned().collect(),
            })?;
        let (start_ms, start_source) = match self.window_start_ms {
            Some(s) if s < floor_ms => {
                return Err(SelectionRefusal::StartBeforeCoverage {
                    start_ms: s,
                    floor_ms,
                })
            }
            Some(s) => (s, WindowFloorSource::InheritedFromSpec),
            None => (floor_ms, WindowFloorSource::ArchiveManifest),
        };
        for (topic, set) in &self.partitions {
            let listed: BTreeSet<i32> = named
                .iter()
                .filter(|t| &t.name == topic)
                .flat_map(|t| t.partitions.iter().map(|p| p.partition_id))
                .collect();
            if let Some(&p) = set.iter().find(|p| !listed.contains(p)) {
                return Err(SelectionRefusal::PartitionNotInArchive {
                    topic: topic.clone(),
                    partition: p,
                });
            }
        }
        let end_ms = self.window_end_ms;
        let mut partitions: Vec<SelectedPartition> = Vec::new();
        for t in &named {
            for p in &t.partitions {
                if !self.selects_partition(&t.name, p.partition_id) {
                    continue;
                }
                let mut sp = SelectedPartition {
                    topic: t.name.clone(),
                    partition: p.partition_id,
                    segment_keys: Vec::new(),
                    records_lower: 0,
                    records_upper: 0,
                };
                for seg in &p.segments {
                    if !Self::segment_overlaps(
                        start_ms,
                        end_ms,
                        seg.start_timestamp,
                        seg.end_timestamp,
                    ) {
                        continue;
                    }
                    let n = seg.record_count.max(0) as u64;
                    sp.segment_keys.push(seg.key.clone());
                    if seg.start_timestamp >= start_ms && seg.end_timestamp <= end_ms {
                        sp.records_lower += n;
                    }
                    sp.records_upper += n;
                }
                partitions.push(sp);
            }
        }
        partitions.sort_by(|a, b| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));
        if partitions.iter().all(|p| p.segment_keys.is_empty()) {
            return Err(SelectionRefusal::EmptySelection { start_ms, end_ms });
        }
        let runs = Self::engine_runs(&self.partitions, named.iter().map(|t| &t.name));
        Ok(ResolvedSelection {
            floor_ms,
            start_ms,
            start_source,
            end_ms,
            partitions,
            runs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{PartitionFacts, SegmentFacts};

    fn seg(key: &str, first: i64, last: i64, n: i64) -> SegmentFacts {
        SegmentFacts {
            key: key.into(),
            start_offset: 0,
            end_offset: n - 1,
            start_timestamp: first,
            end_timestamp: last,
            record_count: n,
            sha256: String::new(),
            uploaded_at: 0,
        }
    }

    fn topic(name: &str, parts: Vec<(i32, Vec<SegmentFacts>)>) -> TopicFacts {
        TopicFacts {
            name: name.into(),
            original_partition_count: Some(parts.len() as i32),
            source_replication_factor: None,
            configurations: BTreeMap::new(),
            partitions: parts
                .into_iter()
                .map(|(id, segments)| PartitionFacts {
                    partition_id: id,
                    segments,
                    gaps: Vec::new(),
                    pruned: Vec::new(),
                })
                .collect(),
        }
    }

    fn selection(start: Option<i64>, end: i64, subsets: &[(&str, &[i32])]) -> ReplaySelection {
        ReplaySelection {
            topics: ["a".to_string(), "b".to_string()].into_iter().collect(),
            window_start_ms: start,
            window_end_ms: end,
            window_end_field: "restore.point_in_time",
            partitions: subsets
                .iter()
                .map(|(t, ps)| (t.to_string(), ps.iter().copied().collect()))
                .collect(),
        }
    }

    fn archive() -> Vec<TopicFacts> {
        vec![
            topic(
                "a",
                vec![
                    (0, vec![seg("a0s0", 100, 200, 3), seg("a0s1", 300, 400, 2)]),
                    (1, vec![seg("a1s0", 150, 250, 4)]),
                ],
            ),
            topic("b", vec![(0, vec![seg("b0s0", 120, 420, 5)])]),
            // Not selected: its older segment must not move the floor.
            topic("c", vec![(0, vec![seg("c0s0", 1, 2, 1)])]),
        ]
    }

    /// The start is INCLUSIVE and the end is INCLUSIVE; with no stated start
    /// there is no lower bound. KILLS: `>` for `>=` at the start, `<` for
    /// `<=` at the end, a lower bound read from anything when none is stated.
    #[test]
    fn both_ends_are_inclusive_and_an_absent_start_bounds_nothing() {
        let s = selection(Some(300), 400, &[]);
        assert!(s.selects_timestamp(300), "the start itself is selected");
        assert!(!s.selects_timestamp(299), "one millisecond before is not");
        assert!(s.selects_timestamp(400), "the end itself is selected");
        assert!(!s.selects_timestamp(401));
        let full = selection(None, 400, &[]);
        assert!(full.selects_timestamp(i64::MIN), "no start, no lower bound");
        assert!(!full.selects_timestamp(401));
    }

    /// A topic without a subset selects every partition; a subset selects its
    /// members only; an unselected topic selects nothing. KILLS: `is_none_or`
    /// read as `is_some_and`, dropping the topic check.
    #[test]
    fn partition_selection_follows_the_subset_and_the_topic_list() {
        let s = selection(None, 400, &[("a", &[1])]);
        assert!(!s.selects_partition("a", 0));
        assert!(s.selects_partition("a", 1));
        assert!(s.selects_partition("b", 0), "no subset: every partition");
        assert!(s.selects_partition("b", 7), "no subset: every partition");
        assert!(!s.selects_partition("c", 0), "an unselected topic");
    }

    /// The floor is the minimum FIRST-record timestamp over the SELECTED
    /// topics; a start before it is refused, never moved to it; a start at it
    /// is admitted as the plan's own (`InheritedFromSpec`).
    #[test]
    fn a_start_before_coverage_is_refused_and_one_at_the_floor_is_the_plans() {
        let refused = selection(Some(99), 400, &[]).resolve(&archive());
        assert_eq!(
            refused,
            Err(SelectionRefusal::StartBeforeCoverage {
                start_ms: 99,
                floor_ms: 100
            }),
            "topic c's segment at 1 is not selected and must not lower the floor"
        );
        let at = selection(Some(100), 400, &[]).resolve(&archive()).unwrap();
        assert_eq!((at.floor_ms, at.start_ms), (100, 100));
        assert_eq!(at.start_source, WindowFloorSource::InheritedFromSpec);
        let none = selection(None, 400, &[]).resolve(&archive()).unwrap();
        assert_eq!((none.floor_ms, none.start_ms), (100, 100));
        assert_eq!(none.start_source, WindowFloorSource::ArchiveManifest);
    }

    /// The engine's segment rule over the effective window, per selected
    /// partition, with the manifest's bound. KILLS: containment for overlap,
    /// segments of an unselected partition, the floor for a stated start.
    #[test]
    fn segments_are_the_engines_overlap_over_the_selected_partitions() {
        let r = selection(Some(260), 400, &[("a", &[0])])
            .resolve(&archive())
            .unwrap();
        let got: Vec<(&str, i32, Vec<&str>, u64, u64)> = r
            .partitions
            .iter()
            .map(|p| {
                (
                    p.topic.as_str(),
                    p.partition,
                    p.segment_keys.iter().map(String::as_str).collect(),
                    p.records_lower,
                    p.records_upper,
                )
            })
            .collect();
        assert_eq!(
            got,
            vec![
                // a0s0 ends at 200 < 260: not read. a0s1 is wholly inside.
                ("a", 0, vec!["a0s1"], 2, 2),
                // b0s0 [120, 420] straddles both ends: read, proves nothing.
                ("b", 0, vec!["b0s0"], 0, 5),
            ],
            "partition a/1 is not selected"
        );
        assert_eq!(r.segment_keys(), vec!["a0s1", "b0s0"]);
    }

    /// Partition subsets on two topics with DIFFERENT subsets are two engine
    /// runs, and topics without a subset share one unfiltered run first.
    /// KILLS: one run for everything (the engine's filter applies to every
    /// topic of a run), a nondeterministic order.
    #[test]
    fn different_subsets_are_different_engine_runs() {
        let subsets: BTreeMap<String, BTreeSet<i32>> = [
            ("a".to_string(), [0].into_iter().collect()),
            ("b".to_string(), [1, 2].into_iter().collect()),
            ("d".to_string(), [0].into_iter().collect()),
        ]
        .into_iter()
        .collect();
        let topics = ["d", "c", "b", "a", "e"].map(String::from);
        let runs = ReplaySelection::engine_runs(&subsets, topics.iter());
        assert_eq!(
            runs,
            vec![
                EngineRunSelection {
                    source_partitions: None,
                    topics: vec!["c".into(), "e".into()]
                },
                EngineRunSelection {
                    source_partitions: Some(vec![0]),
                    topics: vec!["a".into(), "d".into()]
                },
                EngineRunSelection {
                    source_partitions: Some(vec![1, 2]),
                    topics: vec!["b".into()]
                },
            ]
        );
        let none = ReplaySelection::engine_runs(&BTreeMap::new(), topics.iter());
        assert_eq!(
            none.len(),
            1,
            "no subset: one run, the pre-PROD-11.1 document"
        );
        assert_eq!(none[0].source_partitions, None);
    }

    fn spec(restore: &str) -> DrillSpec {
        let yaml = format!(
            "source:\n  storage: {{backend: filesystem, path: /a}}\n  topics: [a, b]\n\
             target:\n  bootstrap_servers: [k:9092]\n  topic_mapping_prefix: d-\n\
             sample:\n  window_start: \"2026-01-01T00:00:00Z\"\n  window_end: \"2026-01-02T00:00:00Z\"\n\
             restore:\n{restore}\
             objectives: {{}}\n\
             evidence: {{backend: filesystem, path: /e}}\n"
        );
        serde_yaml::from_str(&yaml).expect("the test spec parses")
    }

    /// The plan grammar: absent fields are the full selection; the end follows
    /// `build_plan_with_floor`'s rule; every SHAPE refusal fires before the
    /// archive is read. KILLS: accepting a subset of an unselected topic, an
    /// empty subset, a repeat, a negative number, a start at the end.
    #[test]
    fn the_shape_of_a_selection_is_refused_before_the_archive_is_read() {
        let full = ReplaySelection::from_spec(&spec("  point_in_time: \"2026-01-01T12:00:00Z\"\n"))
            .unwrap();
        assert!(full.is_full());
        assert_eq!(full.window_end_field, "restore.point_in_time");
        let no_pit = ReplaySelection::from_spec(&spec("  {}\n")).unwrap();
        assert_eq!(no_pit.window_end_field, "sample.window_end");
        let ok = ReplaySelection::from_spec(&spec(
            "  window_start: \"2026-01-01T06:00:00Z\"\n  partitions: {a: [2, 0]}\n",
        ))
        .unwrap();
        assert!(!ok.is_full());
        assert_eq!(ok.partitions["a"], [0, 2].into_iter().collect());
        for (body, want) in [
            (
                "  partitions: {c: [0]}\n",
                SelectionRefusal::SubsetForUnselectedTopic { topic: "c".into() },
            ),
            (
                "  partitions: {a: []}\n",
                SelectionRefusal::EmptySubset { topic: "a".into() },
            ),
            (
                "  partitions: {a: [1, 1]}\n",
                SelectionRefusal::RepeatedPartition {
                    topic: "a".into(),
                    partition: 1,
                },
            ),
            (
                "  partitions: {b: [-1]}\n",
                SelectionRefusal::NegativePartition {
                    topic: "b".into(),
                    partition: -1,
                },
            ),
            (
                "  window_start: \"2026-01-02T00:00:00Z\"\n",
                SelectionRefusal::EmptyWindow {
                    start_ms: 1_767_312_000_000,
                    end_ms: 1_767_312_000_000,
                    end_field: "sample.window_end",
                },
            ),
        ] {
            assert_eq!(ReplaySelection::from_spec(&spec(body)), Err(want), "{body}");
        }
    }

    /// Absent fields serialise to nothing, so a plan that states no selection
    /// (a rehearsal slot's) keeps its bytes and its `plan_hash`.
    #[test]
    fn an_absent_selection_serialises_to_the_bytes_it_had() {
        let block = crate::spec::RestoreSpecBlock::default();
        assert!(block.selects_everything());
        assert_eq!(
            serde_yaml::to_string(&block).unwrap(),
            "point_in_time: null\n"
        );
    }

    /// An empty selection is refused, not restored into a vacuous pass; so is
    /// a subset partition the archive does not list.
    #[test]
    fn an_empty_selection_and_an_unlisted_partition_are_refused() {
        assert_eq!(
            selection(Some(430), 500, &[]).resolve(&archive()),
            Err(SelectionRefusal::EmptySelection {
                start_ms: 430,
                end_ms: 500
            })
        );
        // a/1 only, after its single segment: nothing selected anywhere else
        // because b is restricted to an empty... b has no subset, so restrict
        // the window instead.
        let only_a1 = ReplaySelection {
            topics: ["a".to_string()].into_iter().collect(),
            ..selection(Some(260), 400, &[("a", &[1])])
        };
        assert_eq!(
            only_a1.resolve(&archive()),
            Err(SelectionRefusal::EmptySelection {
                start_ms: 260,
                end_ms: 400
            }),
            "a/1's only segment ends at 250"
        );
        assert_eq!(
            selection(None, 400, &[("b", &[0, 3])]).resolve(&archive()),
            Err(SelectionRefusal::PartitionNotInArchive {
                topic: "b".into(),
                partition: 3
            })
        );
        let nothing = ReplaySelection {
            topics: ["zz".to_string()].into_iter().collect(),
            ..selection(None, 400, &[])
        };
        assert!(matches!(
            nothing.resolve(&archive()),
            Err(SelectionRefusal::NoCoverage { .. })
        ));
    }
}
