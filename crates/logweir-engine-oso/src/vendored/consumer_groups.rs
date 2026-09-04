//! The sibling bucket object `<backup>/consumer-groups-snapshot.json`.
//! v0.1 RECORDS its presence and hash and nothing more: restoring consumer
//! offsets is a spec §2 non-goal, and `reset_consumer_offsets` /
//! `auto_consumer_groups` are never rendered (Task 11).
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupsSnapshot {
    #[serde(default)]
    pub backup_id: String,
    #[serde(default)]
    pub captured_at: i64,
    #[serde(default)]
    pub groups: Vec<ConsumerGroupEntry>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupEntry {
    #[serde(default)]
    pub group_id: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub offsets: Vec<Value>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}
