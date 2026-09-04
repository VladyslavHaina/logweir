use crate::drill::DrillError;
use chrono::{DateTime, Utc};
use logweir_core::engine::{BackupSetFacts, BackupSetRef, SampleSelection};
use logweir_core::spec::SampleSpec;

#[derive(Debug, Clone)]
pub struct Selection {
    pub window: (DateTime<Utc>, DateTime<Utc>),
    pub per_partition: Vec<SampleSelection>,
    pub records_expected: u64,
    pub topics: u32,
    pub partitions: u32,
    /// Human-readable facts that must reach the operator: known capture gaps
    /// and retention-pruned ranges inside the window.
    pub notes: Vec<String>,
}

pub fn run(
    facts: &BackupSetFacts,
    spec: &SampleSpec,
    topics: &[String],
) -> Result<Selection, DrillError> {
    let (w0, w1) = (spec.window_start, spec.window_end);
    let (ms0, ms1) = (w0.timestamp_millis(), w1.timestamp_millis());
    let mut per_partition = Vec::new();
    let mut expected = 0u64;
    let mut notes = Vec::new();
    let mut n_topics = 0u32;

    for t in facts.topics.iter().filter(|t| topics.contains(&t.name)) {
        let mut used = false;
        for p in &t.partitions {
            let in_window: Vec<_> = p
                .segments
                .iter()
                .filter(|s| s.start_timestamp <= ms1 && s.end_timestamp >= ms0)
                .collect();
            if in_window.is_empty() {
                continue;
            }
            used = true;
            expected += in_window.iter().map(|s| s.record_count as u64).sum::<u64>();
            for (g0, g1) in &p.gaps {
                notes.push(format!(
                    "{}/{}: capture gap {g0}..{g1} overlaps the sampled window",
                    t.name, p.partition_id
                ));
            }
            for (g0, g1) in &p.pruned {
                notes.push(format!(
                    "{}/{}: retention pruned {g0}..{g1} inside the sampled window",
                    t.name, p.partition_id
                ));
            }
            per_partition.push(SampleSelection {
                // `BackupSetFacts` does not carry the manifest key that
                // `DataEngine::describe` read it from — that `BackupSetRef`
                // is consumed and dropped inside `describe` itself
                // (logweir-engine-oso/src/engine.rs), and this function only
                // ever sees the derived `BackupSetFacts`, not the original
                // ref. `manifest_key` cannot be populated here; whatever
                // later wires phase 4 into the orchestrator (out of this
                // task's scope) still holds the real `BackupSetRef` from
                // `list_backup_sets` and must set it before a `Selection`
                // reaches `DataEngine::fingerprints`, which scopes its read
                // by `sel.set.manifest_key`.
                set: BackupSetRef {
                    backup_id: facts.backup_id.clone(),
                    manifest_key: String::new(),
                },
                topic: t.name.clone(),
                partition: p.partition_id,
                anchor: spec.anchor.clone(),
                count: spec.records_per_partition,
                window: (ms0, ms1),
            });
        }
        if used {
            n_topics += 1;
        }
    }

    if per_partition.is_empty() {
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
        per_partition.truncate(max as usize);
    }
    let partitions = per_partition.len() as u32;
    Ok(Selection {
        window: (w0, w1),
        per_partition,
        records_expected: expected,
        topics: n_topics,
        partitions,
        notes,
    })
}
