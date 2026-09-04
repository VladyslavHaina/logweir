use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct EngineId {
    pub id: String,      // "oso-cli"
    pub version: String, // "v0.21.0"
    pub digest: String,  // "sha256:…"
}

/// A storage location shaped EXACTLY like OSO's own `StorageBackendConfig`, so
/// the rendered YAML and our own reads cannot drift apart. Upstream types it as
/// an internally tagged enum whose variants have incompatible REQUIRED fields
/// [VERIFIED U/kafka-backup/crates/kafka-backup-core/src/storage/config.rs:14-105
/// and config.rs:24] — `filesystem` requires `path` and has no `bucket`;
/// `azure` requires `account_name` + `container_name` and has no `bucket`; `gcs`
/// accepts only `bucket`/`service_account_path`/`prefix`. A stringly-typed
/// struct would render `backend: filesystem` alongside a `bucket:` key, and
/// serde would fail the config load with a hard "missing field" error that the
/// unknown-key readback of Task 12 CANNOT catch — the same argument this plan
/// already makes for `time_window_start`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "backend", rename_all = "lowercase")]
pub enum StorageUrl {
    S3 {
        bucket: String,
        #[serde(default)]
        prefix: String,
        #[serde(default)]
        region: Option<String>,
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default)]
        path_style: bool,
        #[serde(default)]
        allow_http: bool,
    },
    Azure {
        account_name: String,
        container_name: String,
        #[serde(default)]
        prefix: String,
    },
    Gcs {
        bucket: String,
        #[serde(default)]
        prefix: String,
    },
    Filesystem {
        path: std::path::PathBuf,
    },
}

impl StorageUrl {
    /// The key prefix, for the object_store wrapper's own listing. `filesystem`
    /// has none: upstream's variant carries only `path`.
    pub fn prefix(&self) -> &str {
        match self {
            Self::S3 { prefix, .. } | Self::Azure { prefix, .. } | Self::Gcs { prefix, .. } => {
                prefix
            }
            Self::Filesystem { .. } => "",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupSetRef {
    pub backup_id: String,
    pub manifest_key: String,
}

#[derive(Debug, Clone)]
pub struct BackupSetFacts {
    pub backup_id: String,
    pub created_at: DateTime<Utc>,
    pub source_cluster_id: Option<String>,
    pub manifest_sha256: String,
    pub manifest_version_id: Option<String>,
    /// `sha256:<hex>` of the sibling `consumer-groups-snapshot.json` when the
    /// object is present, else `None`. v0.1 records presence and hash only —
    /// restoring consumer offsets is a spec §2 non-goal.
    pub consumer_group_snapshot_sha256: Option<String>,
    pub topics: Vec<TopicFacts>,
}

impl BackupSetFacts {
    pub fn consumer_group_snapshot_present(&self) -> bool {
        self.consumer_group_snapshot_sha256.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct TopicFacts {
    pub name: String,
    pub original_partition_count: Option<i32>,
    pub source_replication_factor: Option<i16>,
    pub configurations: BTreeMap<String, String>,
    pub partitions: Vec<PartitionFacts>,
}

#[derive(Debug, Clone)]
pub struct PartitionFacts {
    pub partition_id: i32,
    pub segments: Vec<SegmentFacts>,
    /// Ranges the backup could NOT capture.
    pub gaps: Vec<(i64, i64)>,
    /// Ranges deliberately removed by retention.
    pub pruned: Vec<(i64, i64)>,
}

#[derive(Debug, Clone)]
pub struct SegmentFacts {
    pub key: String,
    pub start_offset: i64,
    pub end_offset: i64,
    pub start_timestamp: i64,
    pub end_timestamp: i64,
    pub record_count: i64,
    /// Empty for segments written before 0.21.
    pub sha256: String,
    /// 0 for segments written before 0.21.
    pub uploaded_at: i64,
}

#[derive(Debug, Clone)]
pub struct RestorePlan {
    pub set: BackupSetRef,
    pub storage: StorageUrl,
    pub target_bootstrap: Vec<String>,
    /// One explicit entry per selected topic: "<source>" -> "<prefix><source>".
    pub topic_mapping: BTreeMap<String, String>,
    pub time_window: (DateTime<Utc>, DateTime<Utc>),
    pub default_replication_factor: i16,
    pub checkpoint_state: std::path::PathBuf,
    pub checkpoint_interval_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoverageState {
    Full,
    Partial,
    Missing,
    Empty,
    DataMissing,
    Corrupt,
    Indeterminate,
    Unknown(String),
}

#[derive(Debug, Clone)]
pub struct PartitionCoverage {
    pub topic: String,
    pub partition: i32,
    pub state: CoverageState,
    pub detail: String,
}

#[derive(Debug, Clone)]
pub struct PreflightReport {
    pub valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub segments_to_process: u64,
    pub records_to_restore: u64,
    pub time_range: Option<(i64, i64)>,
    pub partitions: Vec<PartitionCoverage>,
    /// The per-run readback of spec §9.3 phase 5(1).
    pub header_preflight_honoured: bool,
    /// Paths the engine logged as `Ignoring unknown config key <path>`.
    pub unknown_key_warnings: Vec<String>,
}

/// Logweir-measured, never engine-reported: `restore` has no --format and
/// writes no report file [VERIFIED U/kafka-backup/crates/kafka-backup-cli/src/main.rs:47-51].
#[derive(Debug, Clone)]
pub struct RestoreFacts {
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub exit_code: i32,
    pub unknown_key_warnings: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SampleSelection {
    pub topic: String,
    pub partition: i32,
    pub anchor: String, // head | tail | random
    pub count: usize,
    pub window: (i64, i64),
}

/// sha256(key‖value‖headers‖timestamp), with headers sorted by key then value
/// and each field length-prefixed — see logweir_kafka::fingerprint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordFingerprint {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub sha256: String,
}

pub trait PhaseObserver {
    fn phase_started(&mut self, phase: i8, name: &str);
    fn phase_finished(&mut self, phase: i8, outcome: &str);
    fn engine_line(&mut self, stream: &str, line: &str);
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("operational: {0}")]
    Operational(String),
    /// The KBAK decoder cannot read this segment. The drill records
    /// integrity.level "consume-only" rather than failing (spec §11).
    #[error("unsupported: {0}")]
    Unsupported(String),
}

pub trait DataEngine {
    fn id(&self) -> EngineId;
    fn list_backup_sets(&self, loc: &StorageUrl) -> Result<Vec<BackupSetRef>, EngineError>;
    fn describe(&self, set: &BackupSetRef) -> Result<BackupSetFacts, EngineError>;
    fn preflight(&self, plan: &RestorePlan) -> Result<PreflightReport, EngineError>;
    fn restore(
        &self,
        plan: &RestorePlan,
        obs: &mut dyn PhaseObserver,
    ) -> Result<RestoreFacts, EngineError>;
    fn fingerprints(&self, sel: &SampleSelection) -> Result<Vec<RecordFingerprint>, EngineError>;
}
