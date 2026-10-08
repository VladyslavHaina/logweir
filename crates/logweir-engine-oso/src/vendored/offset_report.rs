//! Vendored serde shape of the engine's offset-mapping report, the file a
//! restore writes to `restore.offset_report` (FX-23).
//! SOURCE: U/kafka-backup/crates/kafka-backup-core/src/manifest.rs @ tag v0.23.3
//! (`OffsetMapping` :682-706, `OffsetMappingEntry` :1103-1129), written by
//! `restore/engine.rs:1390-1395` (`serde_json::to_string_pretty`).
//! Ported, not linked (Global Constraint 2). Only the two fields Logweir reads
//! are typed; everything else is the flattened catch-all, so an upstream
//! addition degrades to `extra` instead of failing the parse.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// upstream manifest.rs:682-706. `entries` is keyed `"<topic>/<partition>"`,
/// and the topic is the TARGET topic: the restore adds an entry per
/// `(self.target_topic, self.target_partition)`, once for every segment whose
/// time-filtered records are not empty (`restore/engine.rs:1794-1811`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetMappingReport {
    #[serde(default)]
    pub entries: HashMap<String, OffsetMappingEntry>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

/// upstream manifest.rs:1103-1129.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetMappingEntry {
    pub topic: String,
    pub partition: i32,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}
