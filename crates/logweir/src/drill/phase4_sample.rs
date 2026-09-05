use crate::drill::DrillError;
use chrono::{DateTime, Utc};
use logweir_core::engine::{BackupSetFacts, BackupSetRef, SampleSelection};
use logweir_core::spec::SampleSpec;

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
    let (w0, w1) = (spec.window_start, spec.window_end);
    let (ms0, ms1) = (w0.timestamp_millis(), w1.timestamp_millis());
    let mut candidates: Vec<Candidate> = Vec::new();

    for t in facts.topics.iter().filter(|t| topics.contains(&t.name)) {
        for p in &t.partitions {
            let in_window: Vec<_> = p
                .segments
                .iter()
                .filter(|s| s.start_timestamp <= ms1 && s.end_timestamp >= ms0)
                .collect();
            if in_window.is_empty() {
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
            let lo = in_window.iter().map(|s| s.start_offset).min().unwrap();
            let hi = in_window.iter().map(|s| s.end_offset).max().unwrap();
            let mut notes = Vec::new();
            for (g0, g1) in &p.gaps {
                if *g0 <= hi && *g1 >= lo {
                    notes.push(format!(
                        "{}/{}: capture gap {g0}..{g1} overlaps the sampled window",
                        t.name, p.partition_id
                    ));
                }
            }
            for (g0, g1) in &p.pruned {
                if *g0 <= hi && *g1 >= lo {
                    notes.push(format!(
                        "{}/{}: retention pruned {g0}..{g1} inside the sampled window",
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
    if let Some(max) = spec.max_partitions {
        // Truncate BEFORE deriving the aggregate counts below, not after:
        // `records_expected`/`topics`/`notes` must describe exactly the
        // partitions `per_partition` ends up naming, never a pre-truncation
        // total for partitions the `Selection` no longer contains.
        candidates.truncate(max as usize);
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
    })
}
