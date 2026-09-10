use super::phase2_target::TargetState;
use logweir_core::engine::BackupSetFacts;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct Collision {
    pub topic: String,
    pub existing_partitions: i32,
    pub existing_end_offsets: i64,
    pub existing_configs_differing: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct TargetDiff {
    /// Target topics that already EXIST on the target — regardless of
    /// whether they currently hold any records (Task 16 fix round 1,
    /// MINOR-2: this comment previously said "and already hold records",
    /// which is narrower than what the code below actually does — the
    /// `Some(st) => ...` arm pushes a collision unconditionally, without
    /// consulting `existing_end_offsets`. The code is right: refusing to
    /// write into an existing empty topic costs at most a false alarm,
    /// while the narrower rule would let a restore silently write into a
    /// topic someone else created. This comment was corrected to match the
    /// code, not the other way round).
    pub collisions: Vec<Collision>,
    /// Target topics that do not exist — the normal case on a scratch
    /// cluster. Every entry here has a matching `would_create` entry.
    pub absent: Vec<String>,
    /// (target topic, partition count the restore will create).
    pub would_create: Vec<(String, i32)>,
}

impl TargetDiff {
    /// THE SINK. Phase 3's whole justification is that no shipped artifact
    /// performs this diff; a value computed and then dropped would leave the
    /// §4 positioning claim resting on something no reader ever sees. The
    /// orchestrator writes this into `Scorecard.target_diff`, `drill show`
    /// renders it, and phase 6 takes its rendered partition counts from
    /// `would_create` rather than re-deriving them.
    ///
    /// `absent` is carried across explicitly (Task 16 fix round 1, MINOR-3):
    /// it used to be computed and never read anywhere, including here — a
    /// topic present in the backup and absent from the target is a
    /// first-order fact about whether the restore can work, and belongs in
    /// the signed document a reader actually sees, not only in an
    /// in-process struct nothing consumes.
    pub fn summarise(&self) -> logweir_core::scorecard::TargetDiffSummary {
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
        }
    }
}

/// The partition count one target topic must be created with, derived the way
/// the ENGINE derives it: `original_partition_count` when the manifest carries
/// one, else `max(partition_id) + 1`
/// [U:crates/kafka-backup-core/src/restore/engine.rs:1421-1436].
///
/// It is public and shared with `phase0_admit::create_target_topics` (guard
/// **G-TS**) on purpose. Since Task 8 the engine no longer creates the target
/// topics — the rendered document says `create_topics: false` — so this number
/// is no longer only a REPORT in `would_create`: it is the count Logweir
/// actually passes to `TopicCreator`. Two copies of the derivation would let
/// `target_diff.would_create` claim one count in the signed scorecard while the
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
pub fn run(
    target: &TargetState,
    facts: &BackupSetFacts,
    mapping: &BTreeMap<String, String>,
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
                let differing = t
                    .configurations
                    .iter()
                    .filter(|(k, v)| st.configs.get(*k).map(|cur| cur != *v).unwrap_or(false))
                    .map(|(k, _)| k.clone())
                    .collect();
                d.collisions.push(Collision {
                    topic: dst.clone(),
                    existing_partitions: st.partitions,
                    existing_end_offsets: total,
                    existing_configs_differing: differing,
                });
            }
        }
    }
    d
}
