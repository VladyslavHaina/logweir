use crate::drill::DrillError;
use chrono::{DateTime, Utc};
use logweir_core::engine::{BackupSetFacts, BackupSetRef, SampleSelection};
use logweir_core::replay_selection::ReplaySelection;
use logweir_core::spec::{Coverage, SampleSpec};

#[derive(Debug, Clone)]
pub struct Selection {
    pub window: (DateTime<Utc>, DateTime<Utc>),
    pub per_partition: Vec<SampleSelection>,
    /// How many records the MANIFEST says the sampled window holds, summed
    /// over the selected partitions.
    ///
    /// It is NOT the canary size, and it is NOT what the scorecard's
    /// identically-named `sample.records_expected` publishes: that field is
    /// the sum of `count` over `per_partition` — how many records the drill
    /// set out to reconcile. On this repository's own fixtures the two are 500
    /// and 25 for a single partition. `crate::drill::sample_info` computes the
    /// scorecard's figure from `per_partition[..].count` for exactly this
    /// reason; substituting this field there would make the signed document
    /// claim it verified twenty times what it did (Task 16's parked item,
    /// discharged in Task 21a).
    pub records_expected: u64,
    pub topics: u32,
    pub partitions: u32,
    /// Human-readable facts that must reach the operator: known capture gaps
    /// and retention-pruned ranges overlapping the sampled window.
    pub notes: Vec<String>,
    /// **FX-23.** The restored topics with at least one partition in the
    /// window that `max_partitions` left WITHOUT a sampled partition, sorted
    /// and each once — empty when every such topic has one. It can be
    /// non-empty only when `max_partitions` is below the number of those
    /// topics: the cap keeps a partition of every topic first (round-robin)
    /// before it keeps a second of any. The scorecard signs it as
    /// `sample.unsampled_topics`; phase 7 still holds every partition of these
    /// topics to its count bound.
    pub unsampled_topics: Vec<String>,
}

impl Selection {
    /// Patches every `per_partition[..].set` to `set`. `phase4_sample::run`
    /// cannot populate `SampleSelection.set.manifest_key` — `BackupSetFacts`
    /// (its only input describing the backup) does not carry a manifest key;
    /// `DataEngine::describe` consumes and drops the original `BackupSetRef`
    /// before returning `BackupSetFacts` (logweir-engine-oso/src/engine.rs).
    /// The caller that still holds that `BackupSetRef` (from
    /// `DataEngine::list_backup_sets`) MUST call this before any
    /// `SampleSelection` reaches `DataEngine::fingerprints`, which reads
    /// `sel.set.manifest_key` to scope its segment read
    /// (`Store::segment_keys_for_set`). `OsoCliEngine::fingerprints` refuses
    /// with `EngineError::Operational` if `manifest_key` is still empty when
    /// it is called, so an implementer who forgets this step gets a loud,
    /// specific failure rather than a wrong or silent read.
    pub fn bind_backup_set(&mut self, set: &BackupSetRef) {
        for sel in &mut self.per_partition {
            sel.set = set.clone();
        }
    }
}

/// One partition's contribution before `max_partitions` truncation is
/// applied — carries everything the final `Selection`'s aggregate fields
/// (`records_expected`, `topics`, `notes`) are derived from, so those fields
/// can be recomputed from exactly what survives truncation rather than
/// accumulated before it runs.
struct Candidate {
    sel: SampleSelection,
    expected: u64,
    topic: String,
    notes: Vec<String>,
}

pub fn run(
    facts: &BackupSetFacts,
    spec: &SampleSpec,
    topics: &[String],
) -> Result<Selection, DrillError> {
    run_selected(facts, spec, topics, None)
}

/// [`run`], over a plan's replay selection (PROD-11.1). `None` — a plan that
/// states none — is [`run`] exactly.
///
/// With a selection, a sample may come only from a SELECTED partition (a
/// partition the plan did not select holds no restored record to reconcile),
/// and the sample window starts no earlier than the plan's stated start (an
/// archived record below it was never restored). The window this returns is
/// the one the scorecard signs as `sample.window_start`/`window_end`, so that
/// existing field names the narrowed start, never the archive's floor.
pub fn run_selected(
    facts: &BackupSetFacts,
    spec: &SampleSpec,
    topics: &[String],
    selection: Option<&ReplaySelection>,
) -> Result<Selection, DrillError> {
    // Task 19 fix round 1 (review finding F2): `records_per_partition: 0` is
    // an ordinary YAML value nothing else rejects, and it reaches every
    // `SampleSelection.count` this function builds. A zero-count selection
    // makes `OsoCliEngine::fingerprints` return `Ok(vec![])` — zero archive
    // fingerprints, no error — which let phase 7's canary reconciliation
    // report a byte-fingerprint `Pass` over a comparison that checked
    // nothing. Refused at the root, not only where it was found reachable.
    if spec.records_per_partition == 0 {
        return Err(DrillError::Operational(
            "sample.records_per_partition is 0; a canary sample of zero records per partition \
             can never establish integrity and must not reach a SampleSelection"
                .into(),
        ));
    }
    let w0 = match selection.and_then(|sel| sel.window_start_ms) {
        Some(start_ms) if start_ms > spec.window_start.timestamp_millis() => {
            DateTime::<Utc>::from_timestamp_millis(start_ms).ok_or_else(|| {
                DrillError::Operational(format!(
                    "restore.window_start epoch-ms {start_ms} is outside the representable range"
                ))
            })?
        }
        _ => spec.window_start,
    };
    let w1 = spec.window_end;
    let (ms0, ms1) = (w0.timestamp_millis(), w1.timestamp_millis());
    let mut candidates: Vec<Candidate> = Vec::new();

    // PROD-08.1: a COMPLETE verification selects every partition the
    // manifest lists for a restored topic — never a `max_partitions` subset
    // (phase 0 refuses that pairing), and never only the partitions whose
    // segments' first/last timestamps overlap the window: those bounds are
    // the engine's selection, and the complete lane's expected output is
    // computed from each record's own timestamp instead. Every segment counts
    // toward `expected` and every recorded gap and pruned range is noted.
    let complete = spec.coverage == Coverage::Complete;
    for t in facts.topics.iter().filter(|t| topics.contains(&t.name)) {
        for p in &t.partitions {
            // PROD-11.1: a partition the plan did not select restored nothing.
            if selection.is_some_and(|sel| !sel.selects_partition(&t.name, p.partition_id)) {
                continue;
            }
            let in_window: Vec<_> = p
                .segments
                .iter()
                .filter(|s| complete || (s.start_timestamp <= ms1 && s.end_timestamp >= ms0))
                .collect();
            if in_window.is_empty() && !complete {
                continue;
            }
            let expected: u64 = in_window.iter().map(|s| s.record_count as u64).sum();
            // The offset EXTENT actually covered by the in-window segments —
            // `gaps`/`pruned` are OFFSET ranges (see `PartitionFacts`'s own
            // field docs), while the window itself is a TIMESTAMP range,
            // so the two cannot be compared directly. This is the bridge:
            // only a gap/pruned range that intersects what was actually
            // read for THIS window is reported as overlapping it: a gap or
            // pruned range entirely outside this partition's in-window
            // segments is real, but it does not overlap THIS sample.
            // A complete selection notes every range of the partition; a
            // partition the manifest lists with no segment has no extent.
            let (lo, hi) = if complete {
                (i64::MIN, i64::MAX)
            } else {
                (
                    in_window.iter().map(|s| s.start_offset).min().unwrap(),
                    in_window.iter().map(|s| s.end_offset).max().unwrap(),
                )
            };
            let mut notes = Vec::new();
            let (gap_where, pruned_where) = if complete {
                (
                    "in a completely verified partition",
                    "in a completely verified partition",
                )
            } else {
                ("overlaps the sampled window", "inside the sampled window")
            };
            for (g0, g1) in &p.gaps {
                if *g0 <= hi && *g1 >= lo {
                    notes.push(format!(
                        "{}/{}: capture gap {g0}..{g1} {gap_where}",
                        t.name, p.partition_id
                    ));
                }
            }
            for (g0, g1) in &p.pruned {
                if *g0 <= hi && *g1 >= lo {
                    notes.push(format!(
                        "{}/{}: retention pruned {g0}..{g1} {pruned_where}",
                        t.name, p.partition_id
                    ));
                }
            }
            candidates.push(Candidate {
                sel: SampleSelection {
                    // See `Selection::bind_backup_set`'s doc comment: this
                    // cannot be populated here and MUST be patched by the
                    // caller before `fingerprints` is called.
                    set: BackupSetRef {
                        backup_id: facts.backup_id.clone(),
                        manifest_key: String::new(),
                    },
                    topic: t.name.clone(),
                    partition: p.partition_id,
                    anchor: spec.anchor,
                    count: spec.records_per_partition,
                    window: (ms0, ms1),
                },
                expected,
                topic: t.name.clone(),
                notes,
            });
        }
    }

    if candidates.is_empty() {
        // OSO's own rule, adopted: zero records scanned is never a positive pass.
        return Err(DrillError::Operational(format!(
            "no segment in backup {} overlaps the window {} .. {}; a drill over an \
             empty window would report a pass that means nothing",
            facts.backup_id,
            w0.to_rfc3339(),
            w1.to_rfc3339()
        )));
    }
    let mut unsampled_topics = Vec::new();
    if let (Some(max), false) = (spec.max_partitions, complete) {
        // Truncate BEFORE deriving the aggregate counts below, not after:
        // `records_expected`/`topics`/`notes` must describe exactly the
        // partitions `per_partition` ends up naming, never a pre-truncation
        // total for partitions the `Selection` no longer contains.
        let (kept, left) = round_robin(candidates, max as usize);
        candidates = kept;
        unsampled_topics = left;
    }

    let records_expected = candidates.iter().map(|c| c.expected).sum();
    let mut topic_names: Vec<&str> = candidates.iter().map(|c| c.topic.as_str()).collect();
    topic_names.sort_unstable();
    topic_names.dedup();
    let topics_count = topic_names.len() as u32;
    let partitions = candidates.len() as u32;
    let notes: Vec<String> = candidates
        .iter()
        .flat_map(|c| c.notes.iter().cloned())
        .collect();
    let per_partition: Vec<SampleSelection> = candidates.into_iter().map(|c| c.sel).collect();

    Ok(Selection {
        window: (w0, w1),
        per_partition,
        records_expected,
        topics: topics_count,
        partitions,
        notes,
        unsampled_topics,
    })
}

/// **FX-23 (b).** Keep at most `max` candidates, ROUND-ROBIN across topics:
/// the first partition of every topic, in manifest order, then the second of
/// every topic that has one, and so on. Returns what is kept, in the
/// candidates' own (manifest) order, and the topics left with no kept
/// partition, sorted.
///
/// The cap used to keep the FIRST `max` candidates in manifest order, which
/// is the order the engine restores topics in. A restore the engine stopped
/// early (a SIGTERM is honoured between topics, with exit 0) restores a
/// manifest-order PREFIX of the topics, so that sample covered exactly the
/// topics that finished, and reconciled them. Round-robin samples every topic
/// as soon as the cap allows one partition each; below that, the topics it
/// could not reach are named in the signed `sample` block rather than left to
/// be inferred, and phase 7's per-partition count bound still holds them.
fn round_robin(candidates: Vec<Candidate>, max: usize) -> (Vec<Candidate>, Vec<String>) {
    // Rank of each candidate within its topic (0 for a topic's first listed
    // partition), and each topic's first appearance, both in manifest order.
    let mut topic_order: Vec<String> = Vec::new();
    let mut topic_index: std::collections::BTreeMap<&str, usize> =
        std::collections::BTreeMap::new();
    let mut seen: std::collections::BTreeMap<&str, usize> = std::collections::BTreeMap::new();
    let mut key: Vec<(usize, usize)> = Vec::with_capacity(candidates.len());
    for c in &candidates {
        let t = c.topic.as_str();
        let idx = *topic_index.entry(t).or_insert_with(|| {
            topic_order.push(t.to_string());
            topic_order.len() - 1
        });
        let n = seen.entry(t).or_insert(0);
        key.push((*n, idx));
        *n += 1;
    }
    // Picking order: by rank, then by the topic's manifest position.
    let mut order: Vec<usize> = (0..candidates.len()).collect();
    order.sort_by_key(|&i| key[i]);
    let keep: std::collections::BTreeSet<usize> = order.into_iter().take(max).collect();
    let mut kept = Vec::with_capacity(keep.len());
    let mut kept_topics = std::collections::BTreeSet::new();
    for (i, c) in candidates.into_iter().enumerate() {
        if keep.contains(&i) {
            kept_topics.insert(c.topic.clone());
            kept.push(c);
        }
    }
    let mut left: Vec<String> = topic_order
        .into_iter()
        .filter(|t| !kept_topics.contains(t))
        .collect();
    left.sort_unstable();
    (kept, left)
}
