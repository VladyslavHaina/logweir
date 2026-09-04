//! SOURCE: U/kafka-backup/crates/kafka-backup-core/src/restore/preflight.rs @ v0.21.0
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// upstream preflight.rs:~30-61. `Unknown` is Logweir's addition: an enum value
/// we do not know degrades, carrying the raw string, instead of failing the
/// parse (spec §11). Serialised through `String` rather than as a serde enum:
/// serde's `untagged` is a container attribute, so there is no way to mix a
/// catch-all variant into an externally-tagged enum. Round-trip is lossless,
/// including for `Unknown`.
///
/// [ADDENDUM A1] Replaces the brief's Step 3 declaration (a per-variant
/// `#[serde(untagged)]` on `Unknown`), which does not compile: `untagged` is a
/// whole-enum container attribute, not a per-variant one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "String", into = "String")]
pub enum PartitionCoverageState {
    Full,
    Partial,
    Missing,
    Empty,
    DataMissing,
    Corrupt,
    Indeterminate,
    Unknown(String),
}

impl PartitionCoverageState {
    /// The wire string. Matches upstream's snake_case serialisation exactly.
    pub fn as_wire_str(&self) -> &str {
        match self {
            Self::Full => "full",
            Self::Partial => "partial",
            Self::Missing => "missing",
            Self::Empty => "empty",
            Self::DataMissing => "data_missing",
            Self::Corrupt => "corrupt",
            Self::Indeterminate => "indeterminate",
            Self::Unknown(raw) => raw.as_str(),
        }
    }
}

impl From<String> for PartitionCoverageState {
    fn from(s: String) -> Self {
        match s.as_str() {
            "full" => Self::Full,
            "partial" => Self::Partial,
            "missing" => Self::Missing,
            "empty" => Self::Empty,
            "data_missing" => Self::DataMissing,
            "corrupt" => Self::Corrupt,
            "indeterminate" => Self::Indeterminate,
            _ => Self::Unknown(s),
        }
    }
}

impl From<PartitionCoverageState> for String {
    fn from(v: PartitionCoverageState) -> Self {
        match v {
            PartitionCoverageState::Unknown(raw) => raw,
            other => other.as_wire_str().to_string(),
        }
    }
}

/// upstream preflight.rs:80-131. There is **no** `scanned`, `offsets` or
/// `detail` field upstream — those three belong to `SnapshotCheck`
/// (preflight.rs:117-131) and were mis-ported. `detail()` below derives the
/// human string Logweir needs from the fields that do exist, so a
/// `data_missing` / `corrupt` scorecard carries a non-empty block reason.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionHeaderCoverage {
    pub topic: String,
    pub partition: i32,
    pub state: PartitionCoverageState,
    #[serde(default)]
    pub segments_selected: usize,
    #[serde(default)]
    pub segments_scanned: usize,
    #[serde(default)]
    pub segments_missing: usize,
    #[serde(default)]
    pub segments_corrupt: usize,
    #[serde(default)]
    pub segments_unreadable: usize,
    #[serde(default)]
    pub segments_legacy_format: usize,
    #[serde(default)]
    pub manifest_record_count: i64,
    #[serde(default)]
    pub records_scanned: u64,
    #[serde(default)]
    pub records_with_offset_header: u64,
    #[serde(default)]
    pub records_with_timestamp_header: u64,
    #[serde(default)]
    pub records_with_source_cluster_header: u64,
    #[serde(default)]
    pub records_with_required_headers: u64,
    /// upstream "Actionable per-partition problems (bounded examples)".
    #[serde(default)]
    pub problems: Vec<String>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl PartitionHeaderCoverage {
    /// The block reason Logweir prints. Upstream carries no `detail` string, so
    /// we build one from the counts plus any bounded `problems` examples.
    pub fn detail(&self) -> String {
        let base = format!(
            "selected={} scanned={} missing={} corrupt={} unreadable={} legacy={} records={}",
            self.segments_selected,
            self.segments_scanned,
            self.segments_missing,
            self.segments_corrupt,
            self.segments_unreadable,
            self.segments_legacy_format,
            self.records_scanned
        );
        if self.problems.is_empty() {
            base
        } else {
            format!("{base}; {}", self.problems.join("; "))
        }
    }
}

/// upstream preflight.rs:132-163
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeaderPreflightReport {
    pub backup_id: String,
    pub generated_at: String,
    /// `auto` | `full` | `skip`
    pub mode: String,
    pub offset_recovery_requested: bool,
    /// "Whether segments were actually opened and records inspected."
    pub scan_performed: bool,
    #[serde(default)]
    pub partitions: Vec<PartitionHeaderCoverage>,
    #[serde(default)]
    pub consumer_group_snapshot: Option<Value>,
    #[serde(default)]
    pub records_scanned_total: u64,
    #[serde(default)]
    pub passed: bool,
    #[serde(default)]
    pub errors: Vec<String>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}
