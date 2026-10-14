//! **PROD-11.1a: what a restore created, a clone's declared lifetime, and the
//! one rule a clone's cleanup deletes by** (design
//! `docs/to-do/decisions/PROD-04.2-cutover.md` §5).
//!
//! 1. [`TargetCreation`], the scorecard's `target.creation` block, written for
//!    EVERY run that created topics: each created topic's name, its partition
//!    count and the topic ID Logweir's own DescribeTopics read returned right
//!    after the create (or why there is none), the statement that restored
//!    records keep their lineage headers, and a TTL clone's lifetime.
//! 2. [`Teardown`], the plan's `target.teardown`: the string policy every
//!    runner reads (`delete`, `keep`), or the clone form `{ttl_seconds: N}`.
//!    A runner from before this row types the field as a string, so it
//!    refuses the map form at parse and restores nothing, rather than keeping
//!    a clone forever.
//! 3. [`cleanup_plan`], what `logweir restore cleanup` deletes: exactly the
//!    recorded names whose current topic ID is the recorded one. A changed ID
//!    is someone else's topic under the name (`CloneTargetReplaced`), an ID
//!    that cannot be compared is never guessed (`CloneTargetIdentityUnknown`),
//!    and a missing topic is `alreadyAbsent`.
//!
//! Global Constraint 1: no I/O, no clock.

use crate::spec::{DrillSpec, TargetMode};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// `target.creation.lineage_headers`'s one value: every restored record
/// keeps `x-original-offset` and `x-original-timestamp`. Stripping them
/// would make the restore unverifiable and its positions unmappable.
pub const LINEAGE_HEADERS_KEPT: &str = "kept";

/// The shortest clone lifetime a plan may declare: five minutes.
pub const CLONE_TTL_MIN_SECONDS: u64 = 300;

/// The longest clone lifetime a plan may declare: thirty days.
pub const CLONE_TTL_MAX_SECONDS: u64 = 2_592_000;

/// A clone lifetime outside [`CLONE_TTL_MIN_SECONDS`] to
/// [`CLONE_TTL_MAX_SECONDS`].
pub const CLONE_TTL_OUT_OF_RANGE: &str = "CloneTtlOutOfRange";

/// The clone form of `target.teardown` outside scratch mode.
pub const CLONE_TTL_NEEDS_SCRATCH: &str = "CloneTtlNeedsScratch";

/// The cleanup's target cluster is not the one the scorecard records: a
/// repointed `KafkaCluster` deletes nothing.
pub const CLONE_TARGET_CLUSTER_CHANGED: &str = "CloneTargetClusterChanged";

/// A recorded topic whose current ID is not the recorded one: the topic was
/// deleted and created again by someone else, and is never deleted.
pub const CLONE_TARGET_REPLACED: &str = "CloneTargetReplaced";

/// A recorded topic whose ID was not recorded at creation, or cannot be read
/// now: never deleted, because nothing proves it is the topic the run made.
pub const CLONE_TARGET_IDENTITY_UNKNOWN: &str = "CloneTargetIdentityUnknown";

/// `teardown_policy` on the cleanup's attestation (the existing teardown
/// attestation, under its own key).
pub const TEARDOWN_POLICY_TTL: &str = "ttl";

/// **The scorecard's `target.creation` block** (scorecard 1.10.0 and 2.2.0).
/// Present exactly on a run that created topics; nested optional (Global
/// Constraint 12 as amended), so every other document keeps its bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TargetCreation {
    /// Every topic this run created, in the order the broker confirmed them.
    pub topics: Vec<CreatedTopic>,
    /// Always [`LINEAGE_HEADERS_KEPT`].
    pub lineage_headers: String,
    /// A TTL clone's declared lifetime, from the end of the run; absent on
    /// every other run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub teardown_after_seconds: Option<u64>,
}

/// One created topic and its identity at creation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CreatedTopic {
    /// The target topic's name.
    pub name: String,
    /// The partition count it was created with.
    pub partitions: u32,
    /// Its topic ID in Kafka's text (`crate::topic_identity`), or `null`.
    pub topic_id: Option<String>,
    /// Why there is no ID (`crate::topic_identity::TOPIC_ID_REASONS`);
    /// present exactly when `topic_id` is `null`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id_reason: Option<String>,
}

impl TargetCreation {
    /// The block for the topics a run created, from `(name, partitions, the
    /// ID read)` in creation order.
    #[must_use]
    pub fn of(
        created: &[(String, u32, crate::topic_identity::IdRead)],
        teardown_after_seconds: Option<u64>,
    ) -> Self {
        Self {
            topics: created
                .iter()
                .map(|(name, partitions, read)| CreatedTopic {
                    name: name.clone(),
                    partitions: *partitions,
                    topic_id: read.id().map(str::to_string),
                    topic_id_reason: read.reason().map(str::to_string),
                })
                .collect(),
            lineage_headers: LINEAGE_HEADERS_KEPT.to_string(),
            teardown_after_seconds,
        }
    }
}

impl TargetCreation {
    /// **The `target creation:` line both readers print** (`logweir drill
    /// verify`, `docs/verify_scorecard.py::_target_creation_lines`): every
    /// created topic with its partitions and its ID at creation, that the
    /// lineage headers are kept, and a clone's lifetime.
    #[must_use]
    pub fn lines(&self) -> Vec<String> {
        let topics: Vec<String> = self
            .topics
            .iter()
            .map(|t| {
                let id = match (&t.topic_id, &t.topic_id_reason) {
                    (Some(id), _) => format!("topic ID {id}"),
                    (None, Some(reason)) => format!("no topic ID ({reason})"),
                    (None, None) => "no topic ID".to_string(),
                };
                format!(
                    "{} ({} {}, {id})",
                    t.name,
                    t.partitions,
                    if t.partitions == 1 {
                        "partition"
                    } else {
                        "partitions"
                    }
                )
            })
            .collect();
        let mut line = format!(
            "target creation: this run created {}: {}; restored records keep their lineage \
             headers ({})",
            if self.topics.len() == 1 {
                "1 topic".to_string()
            } else {
                format!("{} topics", self.topics.len())
            },
            topics.join(", "),
            self.lineage_headers
        );
        if let Some(ttl) = self.teardown_after_seconds {
            line.push_str(&format!(
                "; a clone, deleted {ttl} seconds after the run, each topic only while its ID is \
                 the one recorded here"
            ));
        }
        vec![line]
    }
}

/// `target.teardown`: the string policy (`delete`, the default, or `keep`),
/// or the clone form `{ttl_seconds: N}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Teardown {
    /// The policy every runner reads. Anything but `delete` keeps.
    Policy(String),
    /// A TTL clone: phase 9 keeps a passing run's topics, and the controller
    /// deletes them N seconds after the run.
    Ttl(CloneTtl),
}

/// `target.teardown: {ttl_seconds: N}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloneTtl {
    /// The clone's lifetime in seconds, [`CLONE_TTL_MIN_SECONDS`] to
    /// [`CLONE_TTL_MAX_SECONDS`].
    pub ttl_seconds: u64,
}

impl Default for Teardown {
    fn default() -> Self {
        Teardown::Policy("delete".to_string())
    }
}

impl From<&str> for Teardown {
    fn from(policy: &str) -> Self {
        Teardown::Policy(policy.to_string())
    }
}

impl Teardown {
    /// The declared clone lifetime, when this is the clone form.
    #[must_use]
    pub fn ttl_seconds(&self) -> Option<u64> {
        match self {
            Teardown::Policy(_) => None,
            Teardown::Ttl(ttl) => Some(ttl.ttl_seconds),
        }
    }

    /// The word the approval scope and the teardown attestation carry: the
    /// string policy as written, or [`TEARDOWN_POLICY_TTL`].
    #[must_use]
    pub fn policy(&self) -> &str {
        match self {
            Teardown::Policy(policy) => policy,
            Teardown::Ttl(_) => TEARDOWN_POLICY_TTL,
        }
    }

    /// The policy phase 9 applies to a run with this `passed` verdict: a TTL
    /// clone keeps a passing run's topics until its cleanup and tears a run
    /// that did not pass down at once, as a scratch drill does.
    #[must_use]
    pub fn phase9_policy(&self, passed: bool) -> &str {
        match self {
            Teardown::Policy(policy) => policy,
            Teardown::Ttl(_) if passed => "keep",
            Teardown::Ttl(_) => "delete",
        }
    }
}

/// **The clone form's shape rule**, purely local: `None` for every plan
/// without it. Refused by name, exit 3, before anything is read or dialled.
#[must_use]
pub fn refuse_teardown_shape(spec: &DrillSpec) -> Option<String> {
    let ttl = spec.target.teardown.ttl_seconds()?;
    if spec.target.mode != TargetMode::Scratch {
        return Some(format!(
            "{CLONE_TTL_NEEDS_SCRATCH}: target.teardown declares ttl_seconds and target.mode is \
             {}. A clone is a scratch restore: its target is an allowed scratch cluster, never \
             the source, and only scratch mode deletes what it created; set target.mode: \
             scratch, or write target.teardown as a policy",
            spec.target.mode
        ));
    }
    if !(CLONE_TTL_MIN_SECONDS..=CLONE_TTL_MAX_SECONDS).contains(&ttl) {
        return Some(format!(
            "{CLONE_TTL_OUT_OF_RANGE}: target.teardown.ttl_seconds is {ttl}; a clone is kept \
             from {CLONE_TTL_MIN_SECONDS} seconds (five minutes) to {CLONE_TTL_MAX_SECONDS} \
             seconds (thirty days)"
        ));
    }
    None
}

/// One recorded topic's ID as the cleanup reads it now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CurrentId {
    /// The topic exists with this ID.
    Id(String),
    /// The broker does not hold the topic.
    NotFound,
    /// The ID cannot be read, with why.
    Unread(String),
}

/// What the cleanup does with each recorded topic. Every recorded name is in
/// exactly one list.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CleanupPlan {
    /// Recorded topics whose current ID is the recorded one: deleted.
    pub delete: Vec<String>,
    /// `(topic, why)`: recorded topics the ID rule leaves in place for good:
    /// [`CLONE_TARGET_REPLACED`], or [`CLONE_TARGET_IDENTITY_UNKNOWN`] for a
    /// topic whose ID was never recorded. Retrying cannot change these.
    pub skipped: Vec<(String, String)>,
    /// `(topic, why)`: recorded topics whose ID could not be read now
    /// ([`CLONE_TARGET_IDENTITY_UNKNOWN`]); a later attempt may read it.
    pub unreadable: Vec<(String, String)>,
    /// Recorded topics the cluster no longer holds.
    pub already_absent: Vec<String>,
}

/// **The cleanup's rule.** A recorded topic is deleted only when its current
/// ID is the ID recorded at creation. Nothing outside `recorded` is ever
/// named, whatever `current` holds.
#[must_use]
pub fn cleanup_plan(
    recorded: &TargetCreation,
    current: &BTreeMap<String, CurrentId>,
) -> CleanupPlan {
    let mut plan = CleanupPlan::default();
    for topic in &recorded.topics {
        let Some(recorded_id) = topic.topic_id.as_deref() else {
            plan.skipped.push((
                topic.name.clone(),
                format!(
                    "{CLONE_TARGET_IDENTITY_UNKNOWN}: no topic ID was recorded at creation ({}), \
                     so nothing proves the topic is the one this restore created",
                    topic
                        .topic_id_reason
                        .as_deref()
                        .unwrap_or("no reason recorded")
                ),
            ));
            continue;
        };
        match current.get(&topic.name) {
            Some(CurrentId::Id(now)) if now == recorded_id => plan.delete.push(topic.name.clone()),
            Some(CurrentId::Id(now)) => plan.skipped.push((
                topic.name.clone(),
                format!(
                    "{CLONE_TARGET_REPLACED}: the topic's ID is {now}, and this restore created \
                     it as {recorded_id}; it was deleted and created again since, and is left \
                     in place"
                ),
            )),
            Some(CurrentId::NotFound) => plan.already_absent.push(topic.name.clone()),
            Some(CurrentId::Unread(why)) => plan.unreadable.push((
                topic.name.clone(),
                format!(
                    "{CLONE_TARGET_IDENTITY_UNKNOWN}: the topic's ID could not be read ({why}), \
                     so it is not deleted"
                ),
            )),
            None => plan.unreadable.push((
                topic.name.clone(),
                format!(
                    "{CLONE_TARGET_IDENTITY_UNKNOWN}: the topic's ID could not be read (no \
                     answer for it), so it is not deleted"
                ),
            )),
        }
    }
    plan
}

/// The stdout key of the cleanup's one machine line: `clone-cleanup=` and a
/// [`CloneCleanupReport`] as one JSON object. The controller reads it off the
/// cleanup Job's pod log into `Restore.status.teardown` and
/// `status.clone`.
pub const CLONE_CLEANUP_PREFIX: &str = "clone-cleanup=";

/// What one cleanup attempt did, or why it refused.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloneCleanupReport {
    /// The restore run the cleanup acted for.
    #[serde(default)]
    pub run_id: String,
    /// The recorded topics it deleted.
    #[serde(default)]
    pub deleted: Vec<String>,
    /// The recorded topics the cluster no longer held.
    #[serde(default)]
    pub already_absent: Vec<String>,
    /// The recorded topics left in place, with why: the ID rule's refusals
    /// and the broker's failures.
    #[serde(default)]
    pub left: Vec<CloneCleanupLeft>,
    /// The signed cleanup attestation's key, when it was persisted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attestation_key: Option<String>,
    /// The refusal, when the cleanup deleted nothing by rule (exit 3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refused: Option<String>,
    /// Why the attempt could not do its job (exit 1, 2 or 4), for example a
    /// scorecard the store would not hand over (`EvidenceUnreadable`, D1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// One topic the cleanup left, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloneCleanupLeft {
    /// The topic.
    pub topic: String,
    /// Why it is still there.
    pub reason: String,
}

impl CloneCleanupReport {
    /// The `clone-cleanup=` line.
    #[must_use]
    pub fn line(&self) -> String {
        format!(
            "{CLONE_CLEANUP_PREFIX}{}",
            serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
        )
    }

    /// The report a `clone-cleanup=` line's value carries, or `None`.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        serde_json::from_str(value).ok()
    }
}

/// `logweir/drills/<run_id>.cleanup.json` for the first attempt, and
/// `.cleanup-<n>.json` for attempt `n` after it: each attempt's attestation
/// is its own create-only object.
#[must_use]
pub fn cleanup_attestation_key(run_id: &str, attempt: u32) -> String {
    if attempt <= 1 {
        format!("logweir/drills/{run_id}.cleanup.json")
    } else {
        format!("logweir/drills/{run_id}.cleanup-{attempt}.json")
    }
}

/// `CloneTargetClusterChanged`, or `Ok` when the cleanup's target is the
/// cluster the scorecard records.
///
/// # Errors
///
/// The refusal, naming both cluster IDs.
pub fn check_cluster(recorded: &str, current: &str) -> Result<(), String> {
    if recorded == current {
        Ok(())
    } else {
        Err(format!(
            "{CLONE_TARGET_CLUSTER_CHANGED}: the target cluster's ID is {current:?}, and the \
             scorecard records {recorded:?}; the cleanup deletes nothing on another cluster"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_identity::IdRead;

    const A: &str = "Cf6zT_mcTNCoxuPmv1Ztxw";
    const B: &str = "Zz6zT_mcTNCoxuPmv1Ztxw";

    fn recorded(topics: &[(&str, Option<&str>)]) -> TargetCreation {
        TargetCreation {
            topics: topics
                .iter()
                .map(|(name, id)| CreatedTopic {
                    name: (*name).to_string(),
                    partitions: 3,
                    topic_id: id.map(str::to_string),
                    topic_id_reason: id.is_none().then(|| "noTopicId".to_string()),
                })
                .collect(),
            lineage_headers: LINEAGE_HEADERS_KEPT.into(),
            teardown_after_seconds: Some(300),
        }
    }

    fn now(entries: &[(&str, CurrentId)]) -> BTreeMap<String, CurrentId> {
        entries
            .iter()
            .map(|(n, c)| ((*n).to_string(), c.clone()))
            .collect()
    }

    /// The cleanup rule, row by row. KILLS: deleting on a name match alone
    /// (the replaced row), deleting a topic whose ID was never recorded or
    /// cannot be read, and deleting a name the record does not hold.
    #[test]
    fn the_cleanup_deletes_only_recorded_names_whose_id_is_unchanged() {
        let plan = cleanup_plan(
            &recorded(&[
                ("c-same", Some(A)),
                ("c-replaced", Some(A)),
                ("c-gone", Some(A)),
                ("c-unread", Some(A)),
                ("c-unanswered", Some(A)),
                ("c-never-recorded", None),
            ]),
            &now(&[
                ("c-same", CurrentId::Id(A.into())),
                ("c-replaced", CurrentId::Id(B.into())),
                ("c-gone", CurrentId::NotFound),
                ("c-unread", CurrentId::Unread("notAuthorized".into())),
                ("c-never-recorded", CurrentId::Id(A.into())),
                ("c-not-ours", CurrentId::Id(A.into())),
            ]),
        );
        assert_eq!(plan.delete, ["c-same"]);
        assert_eq!(plan.already_absent, ["c-gone"]);
        let codes = |list: &[(String, String)]| -> Vec<(String, String)> {
            list.iter()
                .map(|(n, why)| (n.clone(), why.split(':').next().unwrap().to_string()))
                .collect()
        };
        let pair = |n: &str, c: &str| (n.to_string(), c.to_string());
        assert_eq!(
            codes(&plan.skipped),
            [
                pair("c-replaced", CLONE_TARGET_REPLACED),
                pair("c-never-recorded", CLONE_TARGET_IDENTITY_UNKNOWN),
            ]
        );
        assert_eq!(
            codes(&plan.unreadable),
            [
                pair("c-unread", CLONE_TARGET_IDENTITY_UNKNOWN),
                pair("c-unanswered", CLONE_TARGET_IDENTITY_UNKNOWN),
            ]
        );
        assert!(plan.skipped[0].1.contains(A) && plan.skipped[0].1.contains(B));
    }

    /// KILLS: a cluster check that compares nothing, or the wrong way round.
    #[test]
    fn a_repointed_cluster_is_refused_by_name() {
        assert_eq!(check_cluster("k1", "k1"), Ok(()));
        let refusal = check_cluster("k1", "k2").unwrap_err();
        assert!(refusal.starts_with("CloneTargetClusterChanged: "));
        assert!(refusal.contains("\"k2\"") && refusal.contains("\"k1\""));
    }

    fn plan_text(mode: &str, teardown: &str) -> String {
        format!(
            "source:\n  storage: {{backend: s3, bucket: b, prefix: p, region: us-east-1}}\n  \
             backup: latestCompleted\n  topics: [orders]\n\
             target:\n  bootstrap_servers: [\"k:9092\"]\n  mode: {mode}\n  \
             topic_mapping_prefix: \"clone-\"\n  teardown: {teardown}\n\
             sample:\n  window_start: \"2026-10-13T00:00:00Z\"\n  \
             window_end: \"2026-10-13T01:00:00Z\"\n\
             objectives: {{}}\n\
             evidence: {{backend: s3, bucket: e, prefix: logweir/, region: us-east-1}}\n"
        )
    }

    fn spec(mode: &str, teardown: &str) -> DrillSpec {
        let text = plan_text(mode, teardown);
        serde_yaml::from_str(&text).unwrap_or_else(|e| panic!("{text}: {e}"))
    }

    /// The TTL bounds, inclusive, and the scratch-only rule. KILLS: an
    /// exclusive bound at either end, a missing upper bound, and a map form
    /// accepted in `newTopic` mode.
    #[test]
    fn the_ttl_is_bounded_and_scratch_only() {
        for ok in [CLONE_TTL_MIN_SECONDS, 3600, CLONE_TTL_MAX_SECONDS] {
            let s = spec("scratch", &format!("{{ttl_seconds: {ok}}}"));
            assert_eq!(refuse_teardown_shape(&s), None, "{ok}");
            assert_eq!(s.target.teardown.ttl_seconds(), Some(ok));
            assert_eq!(s.target.teardown.policy(), TEARDOWN_POLICY_TTL);
        }
        for bad in [0, CLONE_TTL_MIN_SECONDS - 1, CLONE_TTL_MAX_SECONDS + 1] {
            let s = spec("scratch", &format!("{{ttl_seconds: {bad}}}"));
            assert!(
                refuse_teardown_shape(&s).is_some_and(
                    |r| r.starts_with("CloneTtlOutOfRange: ") && r.contains(&bad.to_string())
                ),
                "{bad}"
            );
        }
        let s = spec("newTopic", "{ttl_seconds: 300}");
        assert!(refuse_teardown_shape(&s).is_some_and(|r| r.starts_with("CloneTtlNeedsScratch: ")));
        // The string forms are untouched.
        for policy in ["delete", "keep"] {
            let s = spec("scratch", policy);
            assert_eq!(refuse_teardown_shape(&s), None);
            assert_eq!(s.target.teardown.policy(), policy);
            assert_eq!(s.target.teardown.phase9_policy(true), policy);
            assert_eq!(s.target.teardown.phase9_policy(false), policy);
        }
        // An unknown key in the map form, or a negative lifetime, is no plan.
        for bad in [
            "{ttl_seconds: 300, strip_headers: true}",
            "{ttl_seconds: -1}",
        ] {
            assert!(serde_yaml::from_str::<DrillSpec>(&plan_text("scratch", bad)).is_err());
        }
    }

    /// Phase 9 keeps a passing clone and tears a failed one down at once.
    #[test]
    fn a_ttl_clone_is_kept_only_when_it_passed() {
        let ttl = Teardown::Ttl(CloneTtl { ttl_seconds: 300 });
        assert_eq!(ttl.phase9_policy(true), "keep");
        assert_eq!(ttl.phase9_policy(false), "delete");
    }

    #[test]
    fn the_block_records_each_topic_and_its_id_or_reason() {
        let block = TargetCreation::of(
            &[
                ("c-a".into(), 3, IdRead::Id(A.into())),
                ("c-b".into(), 1, IdRead::Unread("noTopicId")),
            ],
            None,
        );
        assert_eq!(
            block.lines(),
            [format!(
                "target creation: this run created 2 topics: c-a (3 partitions, topic ID {A}), \
                 c-b (1 partition, no topic ID (noTopicId)); restored records keep their lineage \
                 headers (kept)"
            )]
        );
        let json = serde_json::to_value(&block).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "topics": [
                    {"name": "c-a", "partitions": 3, "topic_id": A},
                    {"name": "c-b", "partitions": 1, "topic_id": null,
                     "topic_id_reason": "noTopicId"}
                ],
                "lineage_headers": "kept"
            })
        );
    }
}
