//! The sibling bucket object `<backup_id>/consumer-groups-snapshot.json`.
//!
//! SOURCE (the two WRITERS, whose bytes this reads, both @ tag v0.21.0):
//! - U/kafka-backup/crates/kafka-backup-core/src/backup/engine.rs:846-933,
//!   `snapshot_consumer_groups`: the fn-local `GroupEntry` (:869-873) and
//!   `Snapshot` (:876-879), written by `backup.consumer_group_snapshot: true`
//!   after each backup pass (:584-589);
//! - U/kafka-backup/crates/kafka-backup-cli/src/commands/snapshot_groups.rs:25-36,
//!   the `snapshot-groups` subcommand: the same shape under the same key.
//!
//! and upstream's own READER, U/kafka-backup/crates/kafka-backup-core/src/
//! restore/engine.rs:169-182 (`AutoConsumerGroupSnapshot`). `cargo xtask
//! sync-upstream` compares this file's two structs with all three, field names
//! AND types (FX-1).
//!
//! # The shape
//!
//! ```text
//! { "snapshot_time": <i64, ms since the epoch>,
//!   "groups": [ { "group_id": "<id>",
//!                 "offsets": { "<topic>": { "<partition>": <offset> } } } ] }
//! ```
//!
//! Pretty-printed. Partition ids are JSON object keys, so decimal STRINGS
//! (`co.partition.to_string()`, :893); offsets are the committed positions.
//! What the writer keeps: a group appears only when it has a committed offset
//! `>= 0` on a topic the backup archived (:883-903), so a group with no such
//! offset is left out, and so is each position on an unarchived topic; no
//! group type, state or generation is recorded; and an empty snapshot never
//! overwrites an existing one (:905-916). Measured on the compose stack with
//! the pinned engine: `e2e/fixtures/README.md` holds the provenance of the
//! fixtures cut from its output.
//!
//! # FX-1
//!
//! The shape that stood here before (`captured_at`, a per-group `state`, and
//! `offsets` as a LIST) was invented, not ported. Every non-empty snapshot the
//! engine writes failed it with "invalid type: map, expected a sequence", and
//! `describe()` turned that into `EngineError::Operational`: every drill and
//! every backup receipt of such an archive exited 1 with no artifact.
//!
//! # Compatibility choices
//!
//! - **An absent object** is not this module's concern: `OsoCliEngine`
//!   reports it as absent, as it always did.
//! - **An empty snapshot** (`"groups": []`, or `{}`) parses to zero groups. It
//!   is never an error.
//! - **An unknown field is TOLERATED and KEPT**, at the top level and in each
//!   group, in the flattened `extra` map: the rule every vendored struct
//!   follows (spec §7.2). An engine that adds, say, a group state reads here as
//!   it always did, and the addition stays visible rather than dropped.
//! - **`snapshot_time` absent** reads as `None`, never as an invented epoch
//!   (upstream's reader also defaults it).
//! - **`group_id` absent is refused**: it is the group's identity, and
//!   upstream's reader requires it too.
//! - **A wrong type is refused** (`offsets` as a list, a non-integer offset).
//! - **A partition key that is not a canonical non-negative integer, a
//!   negative offset, and a repeated `group_id` are refused**, although
//!   upstream's reader skips the first two with a warning. The writer never
//!   emits any of them, and an import that silently dropped a position would
//!   read as "no committed offset", which PROD-04.1 forbids.
//!
//! Refusals are [`SnapshotShapeError`]s from [`parse`], the one entry point.
//! Logweir restores no consumer offsets (spec §2 non-goals), so nothing on the
//! drill or backup path consumes a parsed snapshot: see
//! `OsoCliEngine::consumer_group_snapshot` for what a read reports, and
//! `OsoCliEngine::describe` for why a refusal here fails neither.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// The object's name beside a backup set's `manifest.json`.
pub const OBJECT_NAME: &str = "consumer-groups-snapshot.json";

/// upstream backup/engine.rs:876-879 (`Snapshot`) and
/// snapshot_groups.rs:32-36 (`ConsumerGroupsSnapshot`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConsumerGroupsSnapshot {
    /// When the engine took the snapshot, in ms since the epoch. `i64` and
    /// always written upstream; `None` here only when an object omits it.
    #[serde(default)]
    pub snapshot_time: Option<i64>,
    #[serde(default)]
    pub groups: Vec<ConsumerGroupEntry>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

/// upstream backup/engine.rs:869-873 and snapshot_groups.rs:25-30
/// (`GroupEntry`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConsumerGroupEntry {
    pub group_id: String,
    /// topic -> partition id (a decimal string) -> committed offset. A
    /// `BTreeMap` where upstream has a `HashMap`: the same JSON object, read in
    /// a stable order.
    #[serde(default)]
    pub offsets: BTreeMap<String, BTreeMap<String, i64>>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

/// A consumer-groups snapshot that is not in the shape the engine writes.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("not the consumer-groups snapshot shape kafka-backup writes: {0}")]
pub struct SnapshotShapeError(pub String);

/// One committed position, typed: the group's next-to-consume offset on one
/// partition of one topic, as the engine recorded it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Position {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

impl ConsumerGroupEntry {
    /// Every position of this group, ordered by topic, then partition as a
    /// NUMBER (the map orders `"10"` before `"2"`). Refuses a partition key
    /// that is not a canonical non-negative integer and a negative offset,
    /// rather than skipping them.
    pub fn positions(&self) -> Result<Vec<Position>, SnapshotShapeError> {
        let mut out = Vec::new();
        for (topic, partitions) in &self.offsets {
            for (key, &offset) in partitions {
                let partition = key
                    .parse::<i32>()
                    .ok()
                    .filter(|p| *p >= 0 && p.to_string() == *key)
                    .ok_or_else(|| {
                        SnapshotShapeError(format!(
                            "group `{}` topic `{topic}`: partition key `{key}` is not a partition id",
                            self.group_id
                        ))
                    })?;
                if offset < 0 {
                    return Err(SnapshotShapeError(format!(
                        "group `{}` topic `{topic}` partition {partition}: offset {offset} is \
                         negative; the engine records only committed offsets >= 0",
                        self.group_id
                    )));
                }
                out.push(Position {
                    topic: topic.clone(),
                    partition,
                    offset,
                });
            }
        }
        out.sort();
        Ok(out)
    }
}

/// Reads the object's bytes as the engine writes them, or says why not. The
/// one entry point: a snapshot built by deserialising directly skips the
/// checks after the type check.
pub fn parse(bytes: &[u8]) -> Result<ConsumerGroupsSnapshot, SnapshotShapeError> {
    let snapshot: ConsumerGroupsSnapshot =
        serde_json::from_slice(bytes).map_err(|e| SnapshotShapeError(e.to_string()))?;
    let mut seen = BTreeSet::new();
    for group in &snapshot.groups {
        if !seen.insert(group.group_id.as_str()) {
            return Err(SnapshotShapeError(format!(
                "group `{}` appears twice",
                group.group_id
            )));
        }
        group.positions()?;
    }
    Ok(snapshot)
}
