//! **The `Switchover`'s pure half** (PROD-04.2; design
//! `docs/to-do/decisions/PROD-04.2-cutover.md` §3): the plan an approver
//! signs, its agreement with a restore's positions mapping, the
//! compare-and-set every group is applied under, the rollback plan built from
//! a record, and the signed switchover record.
//!
//! Nothing here dials Kafka. The runner (`logweir switchover run`) reads the
//! target's groups and positions, asks [`admit_group`] and [`compare`] what to
//! do, commits through Logweir's own non-member client (`commit_positions`,
//! never the engine: ADR 0008 Amendment D), and writes a
//! [`SwitchoverRecord`].
//!
//! # The plan (format 1.0.0)
//!
//! ```yaml
//! format_version: "1.0.0"
//! restore: {name: incident-4471, uid: 6f1c…, plan_hash: "sha256:…",
//!           scorecard_sha256: "sha256:…", mapping_sha256: "sha256:…"}
//! groups:
//!   - source: orders-app            # the captured group
//!     target: orders-app            # the target group id
//!     topics:
//!       restore-…-orders:           # the RESTORED topic
//!         offsets: {0: 1200, 1: 50} # partition -> target offset to commit
//!         approximate: [1]
//!         expected: none            # or {0: 1180, …} (a rollback plan)
//! ```
//!
//! Bounds: [`MAX_GROUPS`] groups, [`MAX_POSITIONS`] positions,
//! [`MAX_PLAN_BYTES`] bytes. A forward plan's every entry equals the mapping
//! document's and is never `unavailable` ([`check_against_mapping`]); a
//! rollback plan names the record it reverses (`rollback_of`) and its every
//! entry equals that record's prior and applied positions
//! ([`check_against_record`]).

use crate::positions_mapping::{MappingDocument, APPROXIMATE};
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// The plan grammar's format. Major 1; any other is refused.
pub const PLAN_FORMAT_VERSION: &str = "1.0.0";

/// The switchover record's format.
pub const RECORD_FORMAT_VERSION: &str = "1.0.0";

/// The most groups one plan names: a backup selects at most this many
/// (`consumer_positions::MAX_SELECTED_GROUPS`).
pub const MAX_GROUPS: usize = crate::consumer_positions::MAX_SELECTED_GROUPS;

/// The most positions one plan commits: what one approval scope can show
/// (`approval_scope::MAX_SCOPE_PARTITIONS`).
pub const MAX_POSITIONS: usize = crate::approval_scope::MAX_SCOPE_PARTITIONS;

/// The largest plan document: the product API's plan bound.
pub const MAX_PLAN_BYTES: usize = 256 * 1024;

/// The longest group id a plan may name, in bytes (as a selection's).
pub const MAX_GROUP_ID_BYTES: usize = crate::consumer_positions::MAX_GROUP_ID_BYTES;

/// Where a run's record lives, in the restore's evidence store.
#[must_use]
pub fn record_key(run_id: &str) -> String {
    format!("logweir/switchovers/{run_id}.record.json")
}

/// The record's detached signature, beside it.
#[must_use]
pub fn record_sidecar_key(run_id: &str) -> String {
    format!("logweir/switchovers/{run_id}.record.sig")
}

// Refusals and outcomes (design §6). A refusal of the whole run names one of
// the first group; a group's own outcome reason one of the second.
pub const RESTORE_NOT_SUCCEEDED: &str = "RestoreNotSucceeded";
pub const RESTORE_IS_SCRATCH: &str = "RestoreIsScratch";
pub const SWITCHOVER_IN_PROGRESS: &str = "SwitchoverInProgress";
pub const NO_POSITIONS_MAPPING: &str = "NoPositionsMapping";
pub const VERIFICATION_NOT_COMPLETE: &str = "VerificationNotComplete";
pub const MAPPING_DIGEST_MISMATCH: &str = "MappingDigestMismatch";
pub const PLAN_DISAGREES_WITH_MAPPING: &str = "PlanDisagreesWithMapping";
pub const TARGET_CLUSTER_CHANGED: &str = "TargetClusterChanged";
pub const TARGET_GENERATION_CHANGED: &str = "TargetGenerationChanged";
pub const TARGET_IDENTITY_UNKNOWN: &str = "TargetIdentityUnknown";
pub const BELOW_TARGET_LOG_START: &str = "BelowTargetLogStart";
pub const ABOVE_TARGET_END: &str = "AboveTargetEnd";
pub const GROUP_ACTIVE: &str = "GroupActive";
pub const TARGET_GROUP_TYPE_UNSUPPORTED: &str = "TargetGroupTypeUnsupported";
pub const TARGET_GROUP_TYPE_UNPROVEN: &str = "TargetGroupTypeUnproven";
pub const TARGET_NOT_VISIBLE: &str = "TargetNotVisible";
pub const TARGET_POSITIONS_CHANGED: &str = "TargetPositionsChanged";
pub const NOT_AUTHORIZED: &str = "NotAuthorized";
pub const COMMIT_FAILED: &str = "CommitFailed";
/// A plan that does not parse, breaks a bound, or names an inconsistent
/// restore: refused before anything runs.
pub const PLAN_INVALID: &str = "SwitchoverPlanInvalid";

/// A group's outcome (§3.5).
pub const APPLIED: &str = "applied";
pub const ALREADY_APPLIED: &str = "alreadyApplied";
pub const REFUSED: &str = "refused";
pub const FAILED: &str = "failed";
/// A group the run never reached (a whole-run refusal, or the deadline).
pub const UNTOUCHED: &str = "untouched";
pub const GROUP_OUTCOMES: [&str; 5] = [APPLIED, ALREADY_APPLIED, REFUSED, FAILED, UNTOUCHED];

/// The run's outcome, the `Switchover`'s terminal phase.
pub const SUCCEEDED: &str = "Succeeded";
pub const PARTIALLY_APPLIED: &str = "PartiallyApplied";
pub const RUN_FAILED: &str = "Failed";
pub const RUN_REFUSED: &str = "Refused";

/// **The plan an approver signs.**
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SwitchoverPlan {
    /// [`PLAN_FORMAT_VERSION`].
    pub format_version: String,
    /// The verified `Restore` whose target and mapping the plan applies.
    pub restore: RestoreRef,
    /// A ROLLBACK plan names the switchover record it reverses; a forward
    /// plan does not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback_of: Option<RecordRef>,
    /// The groups, applied in this order, each independently.
    pub groups: Vec<PlanGroup>,
}

/// The restore a plan is about, bound by its evidence's digests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RestoreRef {
    pub name: String,
    pub uid: String,
    /// The restore plan's hash (the scorecard's `approval.plan_hash`): the
    /// target cluster, credentials and evidence location come from it.
    pub plan_hash: String,
    /// The signed scorecard's digest.
    pub scorecard_sha256: String,
    /// The positions-mapping document's digest, as the scorecard binds it.
    pub mapping_sha256: String,
}

/// The switchover record a rollback plan reverses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RecordRef {
    /// [`record_key`] of the run.
    pub key: String,
    pub sha256: String,
}

/// One group of a plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PlanGroup {
    /// The captured (source) group.
    pub source: String,
    /// The target group id to commit for.
    pub target: String,
    /// Per RESTORED topic, the partitions to commit.
    pub topics: BTreeMap<String, PlanTopic>,
}

/// One restored topic of a plan group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PlanTopic {
    /// Partition → target offset to commit.
    pub offsets: BTreeMap<u32, i64>,
    /// The partitions whose class is `approximate`; the rest are `exact`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub approximate: Vec<u32>,
    /// The committed positions the apply requires.
    pub expected: Expected,
}

/// What the target's committed positions must be before the apply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum Expected {
    /// `none`: every planned partition holds no committed position.
    Word(ExpectedNone),
    /// Each planned partition's committed position (a rollback plan).
    Positions(BTreeMap<u32, i64>),
}

/// The one word `expected` may be.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ExpectedNone {
    #[serde(rename = "none")]
    None,
}

impl Expected {
    /// `expected: none`.
    #[must_use]
    pub const fn none() -> Self {
        Expected::Word(ExpectedNone::None)
    }

    /// The position `partition` must hold: `None` for "no committed
    /// position".
    #[must_use]
    pub fn at(&self, partition: u32) -> Option<i64> {
        match self {
            Expected::Word(_) => None,
            Expected::Positions(m) => m.get(&partition).copied(),
        }
    }
}

impl SwitchoverPlan {
    /// The plan's bytes as written: deterministic JSON.
    ///
    /// # Errors
    ///
    /// It does not serialise (it always does).
    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        crate::det_json::to_deterministic_json(self)
            .map_err(|e| format!("the switchover plan does not serialise: {e}"))
    }

    /// Every position the plan commits.
    #[must_use]
    pub fn positions(&self) -> usize {
        self.groups
            .iter()
            .flat_map(|g| g.topics.values())
            .map(|t| t.offsets.len())
            .sum()
    }

    /// Parses and bounds plan bytes (YAML or JSON), refusing with a sentence
    /// that opens with [`PLAN_INVALID`].
    ///
    /// # Errors
    ///
    /// The bytes are over [`MAX_PLAN_BYTES`], do not parse, or break
    /// [`validate`].
    pub fn parse(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() > MAX_PLAN_BYTES {
            return Err(format!(
                "{PLAN_INVALID}: the plan is {} bytes, over its bound of {MAX_PLAN_BYTES}",
                bytes.len()
            ));
        }
        let plan: SwitchoverPlan = serde_yaml::from_slice(bytes)
            .map_err(|e| format!("{PLAN_INVALID}: the plan does not parse: {e}"))?;
        validate(&plan).map_err(|e| format!("{PLAN_INVALID}: {e}"))?;
        Ok(plan)
    }
}

fn id_fault(what: &str, id: &str) -> Option<String> {
    if id.trim().is_empty() || id.chars().any(char::is_control) || id.len() > MAX_GROUP_ID_BYTES {
        Some(format!(
            "{what} {id:?} is blank, carries a control character or is over \
             {MAX_GROUP_ID_BYTES} bytes"
        ))
    } else {
        None
    }
}

/// **The plan's own shape**: format 1; 1 to [`MAX_GROUPS`] groups; 1 to
/// [`MAX_POSITIONS`] positions; each group naming at least one topic with at
/// least one partition; valid and distinct target ids; non-negative offsets;
/// `approximate` a subset of the partitions; an `expected` map over exactly
/// the planned partitions; a rollback plan's expectations a map.
///
/// # Errors
///
/// The first fault, in words.
pub fn validate(plan: &SwitchoverPlan) -> Result<(), String> {
    if plan.format_version.split('.').next() != Some("1") {
        return Err(format!(
            "format_version {:?} is not format 1",
            plan.format_version
        ));
    }
    for (what, v) in [
        ("restore.name", &plan.restore.name),
        ("restore.uid", &plan.restore.uid),
    ] {
        if v.trim().is_empty() {
            return Err(format!("{what} is blank"));
        }
    }
    for (what, v) in [
        ("restore.plan_hash", &plan.restore.plan_hash),
        ("restore.scorecard_sha256", &plan.restore.scorecard_sha256),
        ("restore.mapping_sha256", &plan.restore.mapping_sha256),
    ] {
        if !crate::check_contract::is_sha256_prefixed(v) {
            return Err(format!("{what} is not sha256:<64 lowercase hex>"));
        }
    }
    if plan.groups.is_empty() || plan.groups.len() > MAX_GROUPS {
        return Err(format!(
            "the plan names {} groups; it names 1 to {MAX_GROUPS}",
            plan.groups.len()
        ));
    }
    let n = plan.positions();
    if n == 0 || n > MAX_POSITIONS {
        return Err(format!(
            "the plan commits {n} positions; it commits 1 to {MAX_POSITIONS}, which is what one \
             approval scope shows"
        ));
    }
    let mut targets = BTreeSet::new();
    for g in &plan.groups {
        if let Some(f) =
            id_fault("source group", &g.source).or_else(|| id_fault("target group", &g.target))
        {
            return Err(f);
        }
        if !targets.insert(g.target.as_str()) {
            return Err(format!(
                "target group {:?} is named twice; one group is applied once per plan",
                g.target
            ));
        }
        if g.topics.is_empty() {
            return Err(format!("group {:?} names no topic", g.source));
        }
        for (topic, t) in &g.topics {
            if topic.trim().is_empty() || t.offsets.is_empty() {
                return Err(format!(
                    "group {:?} topic {topic:?} is blank or names no partition",
                    g.source
                ));
            }
            if let Some((p, o)) = t.offsets.iter().find(|(_, o)| **o < 0) {
                return Err(format!(
                    "group {:?} {topic}:{p} plans offset {o}; an offset is never negative",
                    g.source
                ));
            }
            if let Some(p) = t.approximate.iter().find(|p| !t.offsets.contains_key(p)) {
                return Err(format!(
                    "group {:?} {topic}: approximate names partition {p}, which it does not plan",
                    g.source
                ));
            }
            match &t.expected {
                Expected::Word(_) if plan.rollback_of.is_some() => {
                    return Err(format!(
                        "group {:?} {topic}: a rollback plan names the positions it expects, \
                         never none",
                        g.source
                    ));
                }
                Expected::Word(_) => {}
                Expected::Positions(m) => {
                    if m.keys().ne(t.offsets.keys()) || m.values().any(|o| *o < 0) {
                        return Err(format!(
                            "group {:?} {topic}: expected names exactly the planned partitions, \
                             each at a non-negative offset",
                            g.source
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

/// **[`PLAN_DISAGREES_WITH_MAPPING`]'s check**: every planned position of a
/// forward plan is the mapping document's, mapped (`exact` or `approximate`,
/// never `unavailable`), for the plan group's SOURCE group, at the restored
/// topic and partition the plan names, and `approximate` lists exactly the
/// approximate ones. `expected` is only a precondition, never an offset to
/// commit: `logweir switchover plan` writes `none`, and an operator may state
/// the positions a target group already holds (an earlier cutover attempt's),
/// which the approver sees and the compare holds the group to.
///
/// # Errors
///
/// The first disagreement, in words.
pub fn check_against_mapping(
    plan: &SwitchoverPlan,
    mapping: &MappingDocument,
) -> Result<(), String> {
    for g in &plan.groups {
        let Some(m) = mapping.groups.get(&g.source) else {
            return Err(format!("group {:?} is not in the mapping", g.source));
        };
        for (topic, t) in &g.topics {
            for (p, offset) in &t.offsets {
                let entry = m.entries.iter().find(|e| {
                    e.target_topic.as_deref() == Some(topic.as_str()) && e.partition == *p
                });
                let Some(e) = entry.filter(|e| e.mapped()) else {
                    return Err(format!(
                        "group {:?} {topic}:{p} is not a mapped position of the mapping",
                        g.source
                    ));
                };
                if e.target_offset != Some(*offset) {
                    return Err(format!(
                        "group {:?} {topic}:{p} plans {offset}, the mapping maps {}",
                        g.source,
                        e.target_offset.unwrap_or_default()
                    ));
                }
                if (e.class == APPROXIMATE) != t.approximate.contains(p) {
                    return Err(format!(
                        "group {:?} {topic}:{p}: the plan's approximate list does not say what \
                         the mapping's class says ({})",
                        g.source, e.class
                    ));
                }
            }
        }
    }
    Ok(())
}

/// **A rollback plan's check**: every planned position is a partition the
/// record APPLIED for that target group, the planned offset its prior and the
/// expected its applied (read-back) position.
///
/// # Errors
///
/// The first disagreement, in words.
pub fn check_against_record(
    plan: &SwitchoverPlan,
    record: &SwitchoverRecord,
) -> Result<(), String> {
    for g in &plan.groups {
        let Some(r) = record
            .groups
            .iter()
            .find(|r| r.target == g.target && r.outcome == APPLIED)
        else {
            return Err(format!(
                "target group {:?} was not applied by the record this plan reverses",
                g.target
            ));
        };
        for (topic, t) in &g.topics {
            for (p, offset) in &t.offsets {
                let Some(rp) = r
                    .partitions
                    .iter()
                    .find(|rp| &rp.topic == topic && rp.partition == *p)
                else {
                    return Err(format!(
                        "target group {:?} {topic}:{p} is not in the record",
                        g.target
                    ));
                };
                if rp.prior != Some(*offset) || t.expected.at(*p) != rp.readback {
                    return Err(format!(
                        "target group {:?} {topic}:{p}: a rollback commits the recorded prior \
                         and expects the recorded readback",
                        g.target
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Which mapped groups a plan applies, and under which target id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupChoice {
    pub source: String,
    pub target: String,
}

/// **`logweir switchover plan`**: a forward plan from a mapping. `chosen`
/// names the groups and their target ids (empty: every mapped group, each
/// under its own id). Every mapped entry of a chosen group is planned;
/// `unavailable` ones are listed in the notes, never planned.
///
/// # Errors
///
/// A chosen group the mapping does not hold, one with no mapped position, or
/// a plan that breaks [`validate`].
pub fn plan_from_mapping(
    mapping: &MappingDocument,
    restore: RestoreRef,
    chosen: &[GroupChoice],
) -> Result<(SwitchoverPlan, Vec<String>), String> {
    let all: Vec<GroupChoice>;
    let (choices, explicit) = if chosen.is_empty() {
        all = mapping
            .groups
            .iter()
            .filter(|(_, g)| g.entries.iter().any(|e| e.mapped()))
            .map(|(id, _)| GroupChoice {
                source: id.clone(),
                target: id.clone(),
            })
            .collect();
        (&all[..], false)
    } else {
        (chosen, true)
    };
    let mut notes = Vec::new();
    let mut groups = Vec::new();
    for c in choices {
        let Some(m) = mapping.groups.get(&c.source) else {
            return Err(format!("group {:?} is not in the mapping", c.source));
        };
        let mut topics: BTreeMap<String, PlanTopic> = BTreeMap::new();
        for e in &m.entries {
            match (&e.target_topic, e.target_offset) {
                (Some(t), Some(o)) if e.mapped() => {
                    let pt = topics.entry(t.clone()).or_insert_with(|| PlanTopic {
                        offsets: BTreeMap::new(),
                        approximate: Vec::new(),
                        expected: Expected::none(),
                    });
                    pt.offsets.insert(e.partition, o);
                    if e.class == APPROXIMATE {
                        pt.approximate.push(e.partition);
                        notes.push(format!(
                            "group {:?} {t}:{}: approximate, {} archived record(s) the restore \
                             excluded are skipped",
                            c.source,
                            e.partition,
                            e.skipped.unwrap_or_default()
                        ));
                    }
                }
                _ => notes.push(format!(
                    "group {:?} {}:{}: not planned, unavailable ({})",
                    c.source,
                    e.topic,
                    e.partition,
                    e.reason.as_deref().unwrap_or("-")
                )),
            }
        }
        if let Some(r) = &m.reason {
            notes.push(format!("group {:?}: not planned, {r}", c.source));
        }
        if topics.is_empty() {
            if explicit {
                return Err(format!(
                    "group {:?} has no mapped position to apply",
                    c.source
                ));
            }
            continue;
        }
        if m.active_at_capture {
            notes.push(format!(
                "group {:?} had members when its positions were captured: they may have moved \
                 after, so the target can re-deliver records it had already read",
                c.source
            ));
        }
        groups.push(PlanGroup {
            source: c.source.clone(),
            target: c.target.clone(),
            topics,
        });
    }
    let plan = SwitchoverPlan {
        format_version: PLAN_FORMAT_VERSION.to_string(),
        restore,
        rollback_of: None,
        groups,
    };
    validate(&plan)?;
    Ok((plan, notes))
}

/// **`logweir switchover rollback-plan`**: `offsets := prior`,
/// `expected := applied` (the readback) for every partition a record APPLIED.
/// A partition whose prior was absent cannot be restored through the safe
/// API (a commit cannot delete an offset): it is listed in the notes with the
/// command that removes it, and left out.
///
/// # Errors
///
/// Nothing in the record can be rolled back, or the plan breaks [`validate`].
pub fn rollback_plan(
    record: &SwitchoverRecord,
    record_ref: RecordRef,
) -> Result<(SwitchoverPlan, Vec<String>), String> {
    let mut notes = Vec::new();
    let mut groups = Vec::new();
    for g in record.groups.iter().filter(|g| g.outcome == APPLIED) {
        let mut topics: BTreeMap<String, PlanTopic> = BTreeMap::new();
        for p in &g.partitions {
            match (p.prior, p.readback) {
                (Some(prior), Some(readback)) => {
                    let t = topics.entry(p.topic.clone()).or_insert_with(|| PlanTopic {
                        offsets: BTreeMap::new(),
                        approximate: Vec::new(),
                        expected: Expected::Positions(BTreeMap::new()),
                    });
                    t.offsets.insert(p.partition, prior);
                    if let Expected::Positions(m) = &mut t.expected {
                        m.insert(p.partition, readback);
                    }
                }
                (None, _) => notes.push(format!(
                    "group {:?} {}:{}: had no committed position before the switchover; the safe \
                     API cannot remove one, so it is left (kafka-consumer-groups.sh \
                     --delete-offsets --group {} --topic {}:{})",
                    g.target, p.topic, p.partition, g.target, p.topic, p.partition
                )),
                (Some(_), None) => notes.push(format!(
                    "group {:?} {}:{}: the switchover did not read its applied position back, so \
                     a rollback cannot say what it expects; left",
                    g.target, p.topic, p.partition
                )),
            }
        }
        if !topics.is_empty() {
            groups.push(PlanGroup {
                source: g.source.clone(),
                target: g.target.clone(),
                topics,
            });
        }
    }
    if groups.is_empty() {
        let mut why = "the record applied no position that had a prior one, so there is \
                       nothing a rollback plan can restore"
            .to_string();
        for n in &notes {
            why.push_str("\n  note: ");
            why.push_str(n);
        }
        return Err(why);
    }
    let plan = SwitchoverPlan {
        format_version: PLAN_FORMAT_VERSION.to_string(),
        restore: record.restore_ref.clone(),
        rollback_of: Some(record_ref),
        groups,
    };
    validate(&plan)?;
    Ok((plan, notes))
}

/// The target group as the runner classified it (PROD-04.0's rules).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetGroup {
    /// The listings are complete and do not show it (or a targeted describe
    /// answered it absent): the commit creates it.
    Absent,
    /// A classic or consumer group, in this described state with these
    /// members.
    Consumer {
        group_type: String,
        state: String,
        members: u32,
    },
    /// A share, streams or other-protocol group (only the name listing shows
    /// it, or a type the client cannot name on a broker that types groups).
    OtherType,
    /// The broker cannot type groups (ListGroups below v5) and the group
    /// exists.
    TypeUnproven,
    /// It may exist and this principal may not see it.
    NotVisible,
    /// The classification itself failed.
    Unreachable(String),
}

/// **§3.5's admission of one target group**, before any position is read:
/// `Ok(created)` when the group may be committed to (`created` for an absent
/// one), or the refusal reason.
///
/// # Errors
///
/// The refusal: [`GROUP_ACTIVE`] for members or a state the client cannot
/// name; [`TARGET_GROUP_TYPE_UNSUPPORTED`]; [`TARGET_GROUP_TYPE_UNPROVEN`];
/// [`TARGET_NOT_VISIBLE`] (never `created`); [`COMMIT_FAILED`] for a failed
/// classification.
pub fn admit_group(target: &TargetGroup) -> Result<bool, &'static str> {
    match target {
        TargetGroup::Absent => Ok(true),
        TargetGroup::Consumer { state, members, .. } => {
            if *members > 0 || !crate::consumer_positions::INACTIVE_STATES.contains(&state.as_str())
            {
                Err(GROUP_ACTIVE)
            } else {
                Ok(false)
            }
        }
        TargetGroup::OtherType => Err(TARGET_GROUP_TYPE_UNSUPPORTED),
        TargetGroup::TypeUnproven => Err(TARGET_GROUP_TYPE_UNPROVEN),
        TargetGroup::NotVisible => Err(TARGET_NOT_VISIBLE),
        TargetGroup::Unreachable(_) => Err(COMMIT_FAILED),
    }
}

/// What [`compare`] decides for one group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// Every planned partition holds what the plan expects: commit.
    Apply,
    /// Every planned partition already holds the planned offset: no commit.
    AlreadyApplied,
    /// Anything else: no commit, and these partitions say why
    /// (`(topic, partition, current)`).
    Changed(Vec<(String, u32, Option<i64>)>),
}

/// One planned partition's current, expected and planned positions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedPartition {
    pub topic: String,
    pub partition: u32,
    pub planned: i64,
    /// `None`: no committed position.
    pub expected: Option<i64>,
    /// `None`: no committed position.
    pub current: Option<i64>,
}

/// **The compare-and-set** (§3.5): already applied when every planned
/// partition holds its planned offset; apply when every one holds what the
/// plan expects; otherwise changed, naming the partitions that hold neither.
#[must_use]
pub fn compare(partitions: &[PlannedPartition]) -> Decision {
    if !partitions.is_empty() && partitions.iter().all(|p| p.current == Some(p.planned)) {
        return Decision::AlreadyApplied;
    }
    if partitions.iter().all(|p| p.current == p.expected) {
        return Decision::Apply;
    }
    Decision::Changed(
        partitions
            .iter()
            .filter(|p| p.current != p.expected)
            .map(|p| (p.topic.clone(), p.partition, p.current))
            .collect(),
    )
}

/// What a group's partitions say against the target's marks now: the first
/// planned offset outside `[log start, high watermark]`.
///
/// # Errors
///
/// [`BELOW_TARGET_LOG_START`] or [`ABOVE_TARGET_END`], with the partition.
pub fn check_bounds(
    partitions: &[PlannedPartition],
    marks: &BTreeMap<(String, u32), (i64, i64)>,
) -> Result<(), String> {
    for p in partitions {
        let Some(&(start, end)) = marks.get(&(p.topic.clone(), p.partition)) else {
            return Err(format!(
                "{ABOVE_TARGET_END}: {}:{} has no partition on the target",
                p.topic, p.partition
            ));
        };
        if p.planned < start {
            return Err(format!(
                "{BELOW_TARGET_LOG_START}: {}:{} plans {} below the log start {start}",
                p.topic, p.partition, p.planned
            ));
        }
        if p.planned > end {
            return Err(format!(
                "{ABOVE_TARGET_END}: {}:{} plans {} above the high watermark {end}",
                p.topic, p.partition, p.planned
            ));
        }
    }
    Ok(())
}

/// **[`VERIFICATION_NOT_COMPLETE`]'s check** over the restore's signed
/// scorecard (08.1-A4): the first field that keeps it from being a passing
/// complete verification, named, or `None`.
#[must_use]
pub fn verification_refusal(sc: &crate::scorecard::Scorecard) -> Option<String> {
    use crate::outcome::Outcome;
    let Some(v) = sc.integrity.verification.as_ref() else {
        return Some(format!(
            "{VERIFICATION_NOT_COMPLETE}: integrity.verification is not recorded, so the restore \
             covered a sample"
        ));
    };
    if v.coverage != crate::scorecard::COVERAGE_COMPLETE {
        return Some(format!(
            "{VERIFICATION_NOT_COMPLETE}: integrity.verification.coverage is {:?}; positions are \
             translated only over a complete verification",
            v.coverage
        ));
    }
    let Some(c) = v.complete.as_ref() else {
        return Some(format!(
            "{VERIFICATION_NOT_COMPLETE}: integrity.verification.complete is absent"
        ));
    };
    if !c.covered {
        return Some(format!(
            "{VERIFICATION_NOT_COMPLETE}: integrity.verification.complete.covered is false ({})",
            c.incomplete_reason.as_deref().unwrap_or("no reason")
        ));
    }
    let r = &c.replay;
    for (name, n) in [
        ("missing", r.missing),
        ("unexpected", r.unexpected),
        ("duplicates", r.duplicates),
        ("out_of_order", r.out_of_order),
        ("mismatched", r.mismatched),
    ] {
        if n > 0 {
            return Some(format!(
                "{VERIFICATION_NOT_COMPLETE}: integrity.verification.complete.replay.{name} is \
                 {n}"
            ));
        }
    }
    if sc.outcome != Outcome::Pass {
        return Some(format!(
            "{VERIFICATION_NOT_COMPLETE}: outcome is {}, not pass",
            sc.outcome.wire_name()
        ));
    }
    None
}

/// **The switchover record** (`logweir/switchovers/<run_id>.record.json`,
/// format 1.0.0, payload type
/// `application/vnd.logweir.switchover-record+json;version=1.0.0`). Signed;
/// both readers verify it signature-only. Field order is byte order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SwitchoverRecord {
    /// [`RECORD_FORMAT_VERSION`].
    pub format_version: String,
    pub run_id: String,
    /// The `Switchover` object.
    pub switchover: ObjectRef,
    /// The plan's hash (`sha256:` over the approved plan bytes).
    pub plan_hash: String,
    /// The restore, as the plan bound it.
    pub restore_ref: RestoreRef,
    /// The record a rollback plan reversed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback_of: Option<RecordRef>,
    /// How the plan was approved, and by whom.
    pub approval: ApprovalSummary,
    /// The target cluster id read now, which must be the scorecard's.
    pub target_cluster_id: String,
    /// Each restored topic checked, with the topic ID read now.
    pub topics: Vec<TopicChecked>,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    /// [`SUCCEEDED`], [`PARTIALLY_APPLIED`], [`RUN_FAILED`] or [`RUN_REFUSED`].
    pub outcome: String,
    /// A whole-run refusal's sentence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub counts: GroupCounts,
    /// One per plan group, in plan order.
    pub groups: Vec<GroupRecord>,
}

/// A Kubernetes object, named.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ObjectRef {
    pub namespace: String,
    pub name: String,
    pub uid: String,
}

/// The approval a run was admitted under.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
pub struct ApprovalSummary {
    /// The namespace's approval mode as the controller applied it.
    pub mode: String,
    /// Who approved (`approver`), and who asked when that is known.
    pub approver: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requester: Option<String>,
    /// The key that signed the approval.
    pub key_id: String,
}

/// A restored topic the run checked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TopicChecked {
    pub name: String,
    /// The topic ID recorded at creation (`target.creation`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_id: Option<String>,
    /// The topic ID read now.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_id: Option<String>,
}

/// Groups counted by outcome.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GroupCounts {
    pub applied: u32,
    pub already_applied: u32,
    pub refused: u32,
    pub failed: u32,
    pub untouched: u32,
}

impl GroupCounts {
    /// The counts of `groups`.
    #[must_use]
    pub fn of(groups: &[GroupRecord]) -> Self {
        let mut c = Self::default();
        for g in groups {
            match g.outcome.as_str() {
                APPLIED => c.applied += 1,
                ALREADY_APPLIED => c.already_applied += 1,
                REFUSED => c.refused += 1,
                FAILED => c.failed += 1,
                _ => c.untouched += 1,
            }
        }
        c
    }

    /// The run's outcome from its groups: every group applied or already
    /// applied is `Succeeded`; none is `Failed` when any failed, else
    /// `Refused`; some is `PartiallyApplied`.
    #[must_use]
    pub fn outcome(&self) -> &'static str {
        let done = self.applied + self.already_applied;
        let total = done + self.refused + self.failed + self.untouched;
        if total > 0 && done == total {
            SUCCEEDED
        } else if done > 0 {
            PARTIALLY_APPLIED
        } else if self.failed > 0 {
            RUN_FAILED
        } else {
            RUN_REFUSED
        }
    }

    /// The line the runner prints and the controller lifts.
    #[must_use]
    pub fn render(&self) -> String {
        format!(
            "applied={} alreadyApplied={} refused={} failed={} untouched={}",
            self.applied, self.already_applied, self.refused, self.failed, self.untouched
        )
    }
}

/// One group's outcome, as recorded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GroupRecord {
    pub source: String,
    pub target: String,
    /// How the target group was classified before the commit.
    pub classification: String,
    /// [`GROUP_OUTCOMES`].
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Whether the commit created the group.
    pub created: bool,
    /// Whether this run sent a commit for the group.
    pub committed: bool,
    pub partitions: Vec<PartitionRecord>,
}

/// One planned partition, as recorded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PartitionRecord {
    /// The restored topic.
    pub topic: String,
    pub partition: u32,
    /// The committed position read before any commit; absent: none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prior: Option<i64>,
    pub planned: i64,
    /// `true`, `false`, or `unknown` (a commit that failed after it may have
    /// been applied, whose readback failed too).
    pub applied: AppliedState,
    /// The committed position read back after the commit; absent: none, or
    /// not read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readback: Option<i64>,
}

/// `applied`: a boolean, or the word `unknown`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum AppliedState {
    Known(bool),
    Unknown(UnknownWord),
}

/// The word `unknown`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum UnknownWord {
    #[serde(rename = "unknown")]
    Unknown,
}

impl SwitchoverRecord {
    /// The record's exact bytes: deterministic JSON.
    ///
    /// # Errors
    ///
    /// It does not serialise (it always does).
    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        crate::det_json::to_deterministic_json(self)
            .map_err(|e| format!("the switchover record does not serialise: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::positions_mapping::{GroupMapping, MappingEntry, EXACT, UNAVAILABLE};

    fn sha(c: char) -> String {
        format!("sha256:{}", c.to_string().repeat(64))
    }

    fn restore() -> RestoreRef {
        RestoreRef {
            name: "incident-4471".into(),
            uid: "6f1c".into(),
            plan_hash: sha('a'),
            scorecard_sha256: sha('b'),
            mapping_sha256: sha('c'),
        }
    }

    fn entry(p: u32, class: &str, offset: Option<i64>) -> MappingEntry {
        MappingEntry {
            topic: "orders".into(),
            partition: p,
            position: Some(40),
            target_topic: Some("r-orders".into()),
            class: class.into(),
            target_offset: offset,
            skipped: (class == APPROXIMATE).then_some(2),
            reason: match class {
                APPROXIMATE => Some("selectionExcluded".into()),
                UNAVAILABLE => Some("ArchiveGap".into()),
                _ => None,
            },
        }
    }

    fn mapping() -> MappingDocument {
        let mut groups = BTreeMap::new();
        groups.insert(
            "app".to_string(),
            GroupMapping {
                group_type: Some("classic".into()),
                active_at_capture: false,
                reason: None,
                entries: vec![
                    entry(0, EXACT, Some(40)),
                    entry(1, APPROXIMATE, Some(38)),
                    entry(2, UNAVAILABLE, None),
                ],
                no_committed_position: 0,
            },
        );
        groups.insert(
            "gone".to_string(),
            GroupMapping {
                group_type: None,
                active_at_capture: false,
                reason: Some("NotCaptured: GroupNotFound".into()),
                entries: vec![],
                no_committed_position: 0,
            },
        );
        MappingDocument {
            format_version: "1.0.0".into(),
            run_id: "R".into(),
            point_id: "P".into(),
            positions_sha256: sha('d'),
            rule: crate::positions_mapping::RULE.into(),
            groups,
        }
    }

    #[test]
    fn a_plan_from_the_mapping_plans_every_mapped_entry_and_agrees_with_it() {
        let m = mapping();
        let (plan, notes) = plan_from_mapping(&m, restore(), &[]).unwrap();
        assert_eq!(plan.groups.len(), 1, "the uncaptured group is not planned");
        let t = &plan.groups[0].topics["r-orders"];
        assert_eq!(t.offsets, [(0, 40), (1, 38)].into());
        assert_eq!(t.approximate, vec![1]);
        assert_eq!(t.expected, Expected::none());
        assert!(notes.iter().any(|n| n.contains("unavailable (ArchiveGap)")));
        check_against_mapping(&plan, &m).expect("it agrees");
        // Round trip through YAML, as the approver sees it.
        let yaml = serde_yaml::to_string(&plan).unwrap();
        assert!(yaml.contains("expected: none"), "{yaml}");
        assert_eq!(SwitchoverPlan::parse(yaml.as_bytes()).unwrap(), plan);
        // A target id of the operator's choosing.
        let (renamed, _) = plan_from_mapping(
            &m,
            restore(),
            &[GroupChoice {
                source: "app".into(),
                target: "app-v2".into(),
            }],
        )
        .unwrap();
        assert_eq!(renamed.groups[0].target, "app-v2");
        // An explicitly chosen group with nothing mapped is refused.
        assert!(plan_from_mapping(
            &m,
            restore(),
            &[GroupChoice {
                source: "gone".into(),
                target: "gone".into()
            }]
        )
        .is_err());
    }

    /// PlanDisagreesWithMapping: another offset, an unavailable entry, a
    /// wrong approximate list, a group the mapping lacks; an expected map is
    /// a precondition and accepted.
    #[test]
    fn a_plan_that_disagrees_with_the_mapping_is_refused() {
        let m = mapping();
        let (plan, _) = plan_from_mapping(&m, restore(), &[]).unwrap();
        let edit = |f: &dyn Fn(&mut PlanTopic)| {
            let mut p = plan.clone();
            f(p.groups[0].topics.get_mut("r-orders").unwrap());
            check_against_mapping(&p, &m).unwrap_err()
        };
        assert!(edit(&|t| {
            t.offsets.insert(0, 41);
        })
        .contains("plans 41, the mapping maps 40"));
        assert!(edit(&|t| {
            t.offsets.insert(2, 0);
        })
        .contains("not a mapped position"));
        assert!(edit(&|t| t.approximate.clear()).contains("approximate list"));
        // An expected map is a precondition, never an offset: accepted.
        let mut expecting = plan.clone();
        expecting.groups[0]
            .topics
            .get_mut("r-orders")
            .unwrap()
            .expected = Expected::Positions([(0, 1), (1, 1)].into());
        check_against_mapping(&expecting, &m).expect("a precondition the approver sees");
        let mut other = plan.clone();
        other.groups[0].source = "other".into();
        assert!(check_against_mapping(&other, &m)
            .unwrap_err()
            .contains("not in the mapping"));
    }

    #[test]
    fn the_plan_bounds_are_enforced() {
        let (plan, _) = plan_from_mapping(&mapping(), restore(), &[]).unwrap();
        let mut twice = plan.clone();
        twice.groups.push(twice.groups[0].clone());
        assert!(validate(&twice).unwrap_err().contains("named twice"));
        let mut many = plan.clone();
        many.groups = (0..=MAX_GROUPS)
            .map(|i| {
                let mut g = plan.groups[0].clone();
                g.target = format!("t{i}");
                g
            })
            .collect();
        assert!(validate(&many).unwrap_err().contains("names 101 groups"));
        let mut wide = plan.clone();
        let t = wide.groups[0].topics.get_mut("r-orders").unwrap();
        for p in 0..=MAX_POSITIONS as u32 {
            t.offsets.insert(p, 0);
        }
        assert!(validate(&wide).unwrap_err().contains("8193 positions"));
        let mut negative = plan.clone();
        negative.groups[0]
            .topics
            .get_mut("r-orders")
            .unwrap()
            .offsets
            .insert(0, -1);
        assert!(validate(&negative).unwrap_err().contains("never negative"));
        let big = vec![b' '; MAX_PLAN_BYTES + 1];
        assert!(SwitchoverPlan::parse(&big)
            .unwrap_err()
            .contains("over its bound"));
        let mut bad_hash = plan;
        bad_hash.restore.mapping_sha256 = "sha256:x".into();
        assert!(validate(&bad_hash).unwrap_err().contains("mapping_sha256"));
    }

    fn pp(planned: i64, expected: Option<i64>, current: Option<i64>) -> PlannedPartition {
        PlannedPartition {
            topic: "r-orders".into(),
            partition: 0,
            planned,
            expected,
            current,
        }
    }

    /// §3.5's compare. Mutants killed: a compare that always applies (the
    /// moved group is overwritten); one that never answers alreadyApplied (a
    /// re-run commits twice); one that reads absence as offset 0.
    #[test]
    fn the_compare_is_a_compare_and_set() {
        assert_eq!(compare(&[pp(40, None, None)]), Decision::Apply);
        assert_eq!(compare(&[pp(40, None, Some(40))]), Decision::AlreadyApplied);
        assert_eq!(
            compare(&[pp(40, None, Some(0))]),
            Decision::Changed(vec![("r-orders".into(), 0, Some(0))]),
            "absence is never offset 0"
        );
        assert_eq!(
            compare(&[pp(40, None, Some(45))]),
            Decision::Changed(vec![("r-orders".into(), 0, Some(45))])
        );
        // A rollback expects the applied position and restores the prior.
        assert_eq!(compare(&[pp(30, Some(40), Some(40))]), Decision::Apply);
        assert_eq!(
            compare(&[pp(30, Some(40), Some(30))]),
            Decision::AlreadyApplied
        );
        assert!(matches!(
            compare(&[pp(30, Some(40), Some(50))]),
            Decision::Changed(_)
        ));
        // Mixed partitions: one applied, one not: changed, never a partial
        // re-commit.
        let mut b = pp(10, None, None);
        b.partition = 1;
        assert!(matches!(
            compare(&[pp(40, None, Some(40)), b]),
            Decision::Changed(_)
        ));
    }

    #[test]
    fn the_bounds_refuse_offsets_outside_the_target() {
        let marks: BTreeMap<(String, u32), (i64, i64)> = [(("r-orders".into(), 0), (5, 50))].into();
        check_bounds(&[pp(5, None, None)], &marks).unwrap();
        check_bounds(&[pp(50, None, None)], &marks).unwrap();
        assert!(check_bounds(&[pp(4, None, None)], &marks)
            .unwrap_err()
            .starts_with(BELOW_TARGET_LOG_START));
        assert!(check_bounds(&[pp(51, None, None)], &marks)
            .unwrap_err()
            .starts_with(ABOVE_TARGET_END));
    }

    /// AP-04.2-1/-2/-5/-6/-7.
    #[test]
    fn target_groups_are_admitted_by_the_table() {
        let consumer = |state: &str, members| TargetGroup::Consumer {
            group_type: "classic".into(),
            state: state.into(),
            members,
        };
        assert_eq!(admit_group(&TargetGroup::Absent), Ok(true));
        assert_eq!(admit_group(&consumer("Empty", 0)), Ok(false));
        assert_eq!(admit_group(&consumer("Stable", 1)), Err(GROUP_ACTIVE));
        assert_eq!(
            admit_group(&consumer("stateUnknownToClient", 0)),
            Err(GROUP_ACTIVE),
            "a state the client cannot name is active (AP-04.2-5)"
        );
        assert_eq!(admit_group(&consumer("Empty", 1)), Err(GROUP_ACTIVE));
        assert_eq!(
            admit_group(&TargetGroup::OtherType),
            Err(TARGET_GROUP_TYPE_UNSUPPORTED)
        );
        assert_eq!(
            admit_group(&TargetGroup::TypeUnproven),
            Err(TARGET_GROUP_TYPE_UNPROVEN)
        );
        assert_eq!(
            admit_group(&TargetGroup::NotVisible),
            Err(TARGET_NOT_VISIBLE),
            "never created (AP-04.2-7)"
        );
    }

    fn record() -> SwitchoverRecord {
        let at = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        let part = |p: u32, prior: Option<i64>| PartitionRecord {
            topic: "r-orders".into(),
            partition: p,
            prior,
            planned: 40,
            applied: AppliedState::Known(true),
            readback: Some(40),
        };
        let groups = vec![
            GroupRecord {
                source: "app".into(),
                target: "app".into(),
                classification: "classic Empty, 0 members".into(),
                outcome: APPLIED.into(),
                reason: None,
                created: false,
                committed: true,
                partitions: vec![part(0, Some(30)), part(1, None)],
            },
            GroupRecord {
                source: "b".into(),
                target: "b".into(),
                classification: "classic Stable, 1 members".into(),
                outcome: REFUSED.into(),
                reason: Some(GROUP_ACTIVE.into()),
                created: false,
                committed: false,
                partitions: vec![],
            },
        ];
        SwitchoverRecord {
            format_version: RECORD_FORMAT_VERSION.into(),
            run_id: "S1".into(),
            switchover: ObjectRef {
                namespace: "ns".into(),
                name: "sw".into(),
                uid: "u".into(),
            },
            plan_hash: sha('e'),
            restore_ref: restore(),
            rollback_of: None,
            approval: ApprovalSummary::default(),
            target_cluster_id: "C".into(),
            topics: vec![],
            started_at: at,
            finished_at: at,
            outcome: PARTIALLY_APPLIED.into(),
            reason: None,
            counts: GroupCounts::of(&groups),
            groups,
        }
    }

    /// The rollback plan's construction and its limits (C15's unit half).
    #[test]
    fn a_rollback_plan_restores_priors_and_lists_what_it_cannot() {
        let r = record();
        let rref = RecordRef {
            key: record_key("S1"),
            sha256: sha('f'),
        };
        let (plan, notes) = rollback_plan(&r, rref.clone()).unwrap();
        assert_eq!(plan.rollback_of, Some(rref));
        assert_eq!(plan.groups.len(), 1, "only applied groups");
        let t = &plan.groups[0].topics["r-orders"];
        assert_eq!(t.offsets, [(0, 30)].into());
        assert_eq!(t.expected, Expected::Positions([(0, 40)].into()));
        assert!(notes.iter().any(|n| n.contains("--delete-offsets")));
        check_against_record(&plan, &r).expect("it agrees with its record");
        let mut wrong = plan.clone();
        wrong.groups[0]
            .topics
            .get_mut("r-orders")
            .unwrap()
            .offsets
            .insert(0, 31);
        assert!(check_against_record(&wrong, &r).is_err());
        // A record with no prior anywhere has nothing to roll back.
        let mut none = r.clone();
        none.groups[0].partitions.retain(|p| p.prior.is_none());
        let why = rollback_plan(&none, record_key_ref()).unwrap_err();
        assert!(
            why.contains("--delete-offsets"),
            "the absent priors are listed: {why}"
        );
    }

    fn record_key_ref() -> RecordRef {
        RecordRef {
            key: record_key("S1"),
            sha256: sha('f'),
        }
    }

    #[test]
    fn counts_decide_the_run_outcome() {
        let r = record();
        assert_eq!(
            r.counts.render(),
            "applied=1 alreadyApplied=0 refused=1 failed=0 untouched=0"
        );
        assert_eq!(r.counts.outcome(), PARTIALLY_APPLIED);
        let all = GroupCounts {
            applied: 1,
            already_applied: 2,
            ..GroupCounts::default()
        };
        assert_eq!(all.outcome(), SUCCEEDED);
        let refused = GroupCounts {
            refused: 1,
            ..GroupCounts::default()
        };
        assert_eq!(refused.outcome(), RUN_REFUSED);
        let failed = GroupCounts {
            failed: 1,
            refused: 1,
            ..GroupCounts::default()
        };
        assert_eq!(failed.outcome(), RUN_FAILED);
        // The record's `applied` reads true, false, or "unknown".
        let json = serde_json::to_string(&AppliedState::Unknown(UnknownWord::Unknown)).unwrap();
        assert_eq!(json, "\"unknown\"");
        assert_eq!(
            serde_json::to_string(&AppliedState::Known(true)).unwrap(),
            "true"
        );
    }
}
