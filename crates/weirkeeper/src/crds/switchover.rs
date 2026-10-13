//! `Switchover` (PROD-04.2; ADR 0008 Amendment A's reserved kind, built by
//! Amendment K): apply reviewed consumer positions, taken from a verified
//! `Restore`'s signed positions mapping, to stopped consumer groups on that
//! restore's target. The only path that commits a consumer-group offset.
//!
//! It writes and deletes no topic, and no `Restore` creates one. The commit
//! runs in a runner Job (`logweir switchover run`): the controller never
//! dials Kafka.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{Condition, LocalRef, SpecRule};

/// The rules on `Switchover`'s `.spec`: immutable, like every plan-bearing
/// kind — the approval binds `planBytes`.
pub const SPEC_RULES: [SpecRule; 1] = [SpecRule::new(
    super::SPEC_IMMUTABLE_RULE,
    super::SPEC_IMMUTABLE_MESSAGE,
)];

/// The most groups `status.groups` lists: a plan names at most this many.
pub const MAX_STATUS_GROUPS: usize = logweir_core::switchover::MAX_GROUPS;

/// `Switchover.spec`.
#[derive(kube::CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "logweir.dev",
    version = "v1alpha1",
    kind = "Switchover",
    doc = "Apply reviewed consumer positions from a verified Restore's signed positions mapping to stopped consumer groups on that restore's target. Each group is compare-and-set against the plan; the broker refuses a group that has members. Executed as a Job; nothing commits a position automatically after a restore. `spec` is immutable: the approval binds `planBytes`.",
    plural = "switchovers",
    singular = "switchover",
    namespaced,
    status = "SwitchoverStatus",
    printcolumn = r#"{"name":"RESTORE","type":"string","jsonPath":".spec.restoreRef.name"}"#,
    printcolumn = r#"{"name":"PHASE","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"APPLIED","type":"integer","jsonPath":".status.counts.applied"}"#,
    printcolumn = r#"{"name":"REFUSED","type":"integer","jsonPath":".status.counts.refused"}"#,
    printcolumn = r#"{"name":"REASON","type":"string","jsonPath":".status.reason"}"#,
    printcolumn = r#"{"name":"AGE","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct SwitchoverSpec {
    /// The switchover plan (format 1.0.0), as the exact bytes the approver
    /// signed: an opaque string, never a modelled schema, for the reason
    /// `Restore.spec.planBytes` gives. `logweir switchover plan` writes it.
    pub plan_bytes: String,
    /// The `Approval` (subject kind `Switchover`) that authorises it.
    pub approval_ref: LocalRef,
    /// The `Restore` whose mapping and target the plan applies. A
    /// declaration the controller holds to the plan's own `restore` block.
    pub restore_ref: SwitchoverRestoreRef,
    /// The Job's `activeDeadlineSeconds`.
    #[schemars(range(min = 1))]
    pub deadline_seconds: i64,
}

/// The restore a `Switchover` is about.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SwitchoverRestoreRef {
    /// The `Restore`'s name, in this namespace.
    pub name: String,
    /// Its UID: a same-named restore recreated since is another restore.
    pub uid: String,
}

/// `Switchover.status`.
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SwitchoverStatus {
    /// `Pending`, `Running`, `Succeeded`, `PartiallyApplied`, `Failed`,
    /// `Refused`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// The runner's exit code: 0 every group applied, 2 some refused or
    /// failed, 3 refused before any group, 1 operational, 4 the record could
    /// not be signed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// The runner's `refusal-reason=` or `failure-reason=` word.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_reason: Option<String>,
    /// Why the object is where it is: the admission refusal or hold, or the
    /// run's outcome.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The sentence beside `reason`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// The Job this object created.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_ref: Option<String>,
    /// One entry per plan group, in plan order (at most 100).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(length(max = 100))]
    pub groups: Vec<SwitchoverGroupStatus>,
    /// The groups counted by outcome.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub counts: Option<SwitchoverCounts>,
    /// The signed switchover record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence: Option<SwitchoverEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}

/// One plan group's outcome.
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SwitchoverGroupStatus {
    /// The captured group.
    pub source: String,
    /// The target group id.
    pub target: String,
    /// `applied`, `alreadyApplied`, `refused`, `failed` or `untouched`.
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Whether the commit created the group.
    #[serde(default)]
    pub created: bool,
    /// How many partitions the plan names for it.
    #[serde(default)]
    pub partitions: u32,
}

/// The groups counted by outcome.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SwitchoverCounts {
    pub applied: u32,
    pub already_applied: u32,
    pub refused: u32,
    pub failed: u32,
    pub untouched: u32,
}

/// Where the signed record is.
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SwitchoverEvidence {
    /// `logweir/switchovers/<run_id>.record.json`, in the restore's evidence
    /// store; its `.sig` beside it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_key: Option<String>,
}
