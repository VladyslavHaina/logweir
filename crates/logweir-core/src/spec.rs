use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrillSpec {
    pub source: SourceSpec,
    pub target: TargetSpec,
    pub sample: SampleSpec,
    pub objectives: ObjectivesSpec,
    /// The evidence sink. A DIFFERENT bucket and principal from `source` by
    /// default (spec §7.1); the guard warns loudly when they are equal.
    pub evidence: crate::engine::StorageUrl,
    /// Deliberately a free-form map so an adopter CAN try to pass an engine
    /// key — and be refused by name. Silently ignoring it would hide the guard.
    #[serde(default)]
    pub engine_overrides: BTreeMap<String, serde_yaml::Value>,
    /// Spec §13. Absent means "notify nobody"; it is never an error.
    #[serde(default)]
    pub notifications: Notifications,
}

/// Spec §13's notification shape. v0.1 POSTs one JSON summary per sink and
/// treats every transport failure as a logged warning — a drill result that is
/// already signed and uploaded must not be downgraded because a webhook was
/// down.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Notifications {
    #[serde(default)]
    pub webhooks: Vec<String>,
    #[serde(default)]
    pub slack_webhook: Option<String>,
    #[serde(default)]
    pub pagerduty_routing_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceSpec {
    pub storage: crate::engine::StorageUrl,
    /// "latestCompleted" or a pinned backup id.
    #[serde(default = "latest")]
    pub backup: String,
    pub topics: Vec<String>,
}
fn latest() -> String {
    "latestCompleted".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetSpec {
    pub bootstrap_servers: Vec<String>,
    /// The v0.1 segregation proof. Must EXIST on the target.
    #[serde(default = "marker")]
    pub marker_topic: String,
    pub topic_mapping_prefix: String,
    #[serde(default = "rf1")]
    pub default_replication_factor: i16,
    /// "delete" (default) or "keep".
    #[serde(default = "delete")]
    pub teardown: String,
}
fn marker() -> String {
    "logweir.scratch".into()
}
fn rf1() -> i16 {
    1
}
fn delete() -> String {
    "delete".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SampleSpec {
    pub window_start: chrono::DateTime<chrono::Utc>,
    pub window_end: chrono::DateTime<chrono::Utc>,
    #[serde(default = "n25")]
    pub records_per_partition: usize,
    /// head | tail | random. Rotating across runs is the adopter's job; the
    /// scorecard always records which was used.
    #[serde(default = "rand_anchor")]
    pub anchor: String,
    #[serde(default)]
    pub max_partitions: Option<u32>,
}
fn n25() -> usize {
    25
}
fn rand_anchor() -> String {
    "random".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectivesSpec {
    #[serde(default)]
    pub rto_seconds: Option<u64>,
    #[serde(default)]
    pub rpo_seconds: Option<i64>,
    #[serde(default)]
    pub pass_rate: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllowedClusters {
    /// Supplied as a SEPARATE file argument, never read from the drill spec,
    /// so an edited spec cannot widen its own allowlist (spec §9.3 phase 0).
    pub allowed_cluster_ids: Vec<String>,
    /// Refused as a target even if it appears above.
    #[serde(default)]
    pub source_cluster_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalDoc {
    pub approver: String,
    pub ticket: String,
    /// sha256 over the canonical bytes of the drill spec.
    pub plan_hash: String,
    pub approved_at: chrono::DateTime<chrono::Utc>,
}
