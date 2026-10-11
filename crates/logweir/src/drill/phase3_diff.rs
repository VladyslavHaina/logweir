use super::phase2_target::TargetState;
use logweir_core::backup_receipt::{ConfigCoverage, SourceConfigCoverage};
use logweir_core::engine::BackupSetFacts;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct Collision {
    pub topic: String,
    pub existing_partitions: i32,
    pub existing_end_offsets: i64,
    pub existing_configs_differing: Vec<String>,
    /// `Some(<coverage>)` when the SOURCE topic's configuration was
    /// not captured (`unknown`, `notCaptured` or `captureDenied`): the keys
    /// above are the differences the archive's own record shows, and their
    /// ABSENCE proves nothing. `None` only for `captured`. It reaches the
    /// signed document as a `target_diff.not_assessed` entry, never inside
    /// the collision string.
    pub configuration_not_assessed: Option<&'static str>,
}

#[derive(Debug, Clone, Default)]
pub struct TargetDiff {
    /// Target topics that already EXIST on the target, whether or not they
    /// hold records: refusing to write into an existing empty topic costs at
    /// most a false alarm, while the narrower rule would let a restore write
    /// into a topic someone else created.
    pub collisions: Vec<Collision>,
    /// Target topics that do not exist — the normal case on a scratch
    /// cluster. Every entry here has a matching `would_create` entry.
    pub absent: Vec<String>,
    /// (target topic, partition count the restore will create).
    pub would_create: Vec<(String, i32)>,
}

impl TargetDiff {
    /// The sink: the orchestrator writes this into `Scorecard.target_diff`,
    /// `drill show` renders it, and phase 6 takes its rendered partition
    /// counts from `would_create` rather than re-deriving them. `absent` is
    /// carried across: a topic in the backup and absent from the target is a
    /// first-order fact about whether the restore can work.
    ///
    /// A collision whose SOURCE configuration was not captured is named in
    /// `not_assessed` (`"<target topic>: configuration (<why>)"`), so its
    /// `differing config: []` is never read as "no divergence"; the collision
    /// string itself keeps its format (an existing field's content does not
    /// change). `Some`, always, from here: phase 3 ran, so "every collision
    /// assessed" (`Some([])`) is a claim this function can make, and absent
    /// stays the spelling of "not recorded".
    pub fn summarise(&self) -> logweir_core::scorecard::TargetDiffSummary {
        let mut not_assessed: Vec<String> = self
            .collisions
            .iter()
            .filter_map(|c| {
                c.configuration_not_assessed
                    .map(|why| format!("{}: configuration ({why})", c.topic))
            })
            .collect();
        not_assessed.sort();
        logweir_core::scorecard::TargetDiffSummary {
            collisions: self
                .collisions
                .iter()
                .map(|c| {
                    format!(
                        "{}: {} partition(s), {} record(s) already present, differing config: [{}]",
                        c.topic,
                        c.existing_partitions,
                        c.existing_end_offsets,
                        c.existing_configs_differing.join(", ")
                    )
                })
                .collect(),
            absent: self.absent.clone(),
            would_create: self.would_create.clone(),
            // "shallow" only if spec §15 cut 0d is ever taken; v0.1 always
            // reads the target's real state, so this is "full".
            level: "full".into(),
            not_assessed: Some(not_assessed),
        }
    }
}

/// The partition count one target topic must be created with, derived the way
/// the ENGINE derives it: `original_partition_count` when the manifest carries
/// one, else `max(partition_id) + 1`
/// [U:crates/kafka-backup-core/src/restore/engine.rs:1421-1436].
///
/// It is shared with `phase0_admit::create_target_topics` (guard G-TS) on
/// purpose: the rendered document says `create_topics: false`, so this is
/// the count Logweir passes to `TopicCreator`, and two copies of the
/// derivation would let `target_diff.would_create` sign one count while the
/// broker was asked for another.
pub fn restore_partition_count(t: &logweir_core::engine::TopicFacts) -> i32 {
    t.original_partition_count.unwrap_or_else(|| {
        t.partitions
            .iter()
            .map(|p| p.partition_id)
            .max()
            .unwrap_or(-1)
            + 1
    })
}

/// A diff against ACTUAL TARGET STATE, which OSO's dry run never performs and
/// which the operator's dry run fakes with an unconditional DryRunPassed.
///
/// `coverage` is the SOURCE topics' configuration capture coverage from the
/// verified backup receipt: a collision's configuration difference is
/// assessed only where it is `captured`.
pub fn run(
    target: &TargetState,
    facts: &BackupSetFacts,
    mapping: &BTreeMap<String, String>,
    coverage: &SourceConfigCoverage,
) -> TargetDiff {
    let mut d = TargetDiff::default();
    for t in &facts.topics {
        let Some(dst) = mapping.get(&t.name) else {
            continue;
        };
        let want = restore_partition_count(t);
        match target.topics.get(dst) {
            None => {
                d.absent.push(dst.clone());
                d.would_create.push((dst.clone(), want));
            }
            Some(st) => {
                let total: i64 = st.end_offsets.iter().map(|(_, hi)| *hi).sum();
                // A source key the target does not report differs. Absent
                // is never agreement (`phase7_verify::classify_parity`'s rule).
                let differing = t
                    .configurations
                    .iter()
                    .filter(|(k, v)| st.configs.get(*k).map(|cur| cur != *v).unwrap_or(true))
                    .map(|(k, _)| k.clone())
                    .collect();
                let configuration_not_assessed = match coverage.of(&t.name) {
                    ConfigCoverage::Captured => None,
                    other => Some(other.wire_name()),
                };
                d.collisions.push(Collision {
                    topic: dst.clone(),
                    existing_partitions: st.partitions,
                    existing_end_offsets: total,
                    existing_configs_differing: differing,
                    configuration_not_assessed,
                });
            }
        }
    }
    d
}
