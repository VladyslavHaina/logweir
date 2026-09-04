//! Vendored serde structs for OSO's on-bucket artifacts.
//! SOURCE: U/kafka-backup/crates/kafka-backup-core/src/manifest.rs @ tag v0.21.0
//! Ported, not linked (Global Constraint 2). Every field carries serde(default)
//! and each struct a flattened catch-all, so an upstream addition degrades to
//! `extra` instead of failing the parse (spec §7.2).
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

/// upstream manifest.rs:8-29
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupManifest {
    pub backup_id: String,
    pub created_at: i64,
    #[serde(default)]
    pub source_cluster_id: Option<String>,
    #[serde(default)]
    pub source_brokers: Vec<String>,
    #[serde(default)]
    pub compression: String,
    #[serde(default)]
    pub topics: Vec<TopicBackup>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

/// upstream manifest.rs:124-149
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicBackup {
    pub name: String,
    #[serde(default)]
    pub original_partition_count: Option<i32>,
    #[serde(default)]
    pub source_replication_factor: Option<i16>,
    #[serde(default)]
    pub configurations: BTreeMap<String, String>,
    #[serde(default)]
    pub partitions: Vec<PartitionBackup>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

/// upstream manifest.rs:176-198
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionBackup {
    pub partition_id: i32,
    #[serde(default)]
    pub segments: Vec<SegmentMetadata>,
    #[serde(default)]
    pub gaps: Vec<OffsetGap>,
    #[serde(default)]
    pub pruned: Vec<PrunedRange>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

/// upstream manifest.rs:349-387. `sha256` is "Written since 0.21; empty for
/// older segments"; `uploaded_at` is "`0` for segments written before 0.21".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMetadata {
    pub key: String,
    pub start_offset: i64,
    pub end_offset: i64,
    pub start_timestamp: i64,
    pub end_timestamp: i64,
    pub record_count: i64,
    #[serde(default)]
    pub uncompressed_size: u64,
    #[serde(default)]
    pub compressed_size: u64,
    #[serde(default)]
    pub sha256: String,
    #[serde(default)]
    pub uploaded_at: i64,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

/// upstream manifest.rs:302-315. `reason` is typed `OffsetGapReason` upstream —
/// an enum that serialises as a string, so `String` here reads it losslessly and
/// never fails on a variant we do not know.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetGap {
    pub start_offset: i64,
    pub end_offset: i64,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub detected_at: i64,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

/// upstream manifest.rs:248-269. `reason` is `PruneReason` upstream (again a
/// string-serialising enum).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrunedRange {
    pub start_offset: i64,
    pub end_offset: i64,
    #[serde(default)]
    pub segments: u32,
    #[serde(default)]
    pub bytes: u64,
    #[serde(default)]
    pub pruned_at: i64,
    #[serde(default)]
    pub cutoff_timestamp: i64,
    #[serde(default)]
    pub reason: String,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

/// upstream manifest.rs:1062-1098. This is what `validate-restore --format json`
/// prints on stdout.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DryRunReport {
    pub backup_id: String,
    pub valid: bool,
    #[serde(default)]
    pub errors: Vec<String>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub segments_to_process: u64,
    #[serde(default)]
    pub records_to_restore: u64,
    #[serde(default)]
    pub bytes_to_restore: u64,
    #[serde(default)]
    pub time_range: Option<(i64, i64)>,
    #[serde(default)]
    pub topics_to_restore: Vec<DryRunTopicReport>,
    #[serde(default)]
    pub consumer_offset_actions: Vec<String>,
    #[serde(default)]
    pub header_preflight: Option<super::preflight::HeaderPreflightReport>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

/// upstream manifest.rs:1102-1116. **`partitions` is a SEQUENCE, not a count.**
/// The earlier `{ name, partitions: i32, records }` shape was invented; a real
/// `validate-restore --format json` with a non-empty `topics_to_restore` fails
/// to deserialise against it, and `serde(default)` cannot rescue a type
/// mismatch — so `DataEngine::preflight` would return `Operational` on every
/// live run and phase 5 could never adjudicate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DryRunTopicReport {
    pub source_topic: String,
    pub target_topic: String,
    /// upstream `Option<DryRunRepartitioningInfo>`; kept opaque because nothing
    /// in v0.1 reads inside it.
    #[serde(default)]
    pub repartitioning: Option<Value>,
    /// upstream `Vec<DryRunPartitionReport>`; opaque for the same reason.
    #[serde(default)]
    pub partitions: Vec<Value>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}
