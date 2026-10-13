//! **PROD-05.2: a restore's target topic configuration, in stages.**
//!
//! A `Restore` whose plan carries `target.topic_configuration` creates each
//! target topic from the bound point's configuration model (receipt 1.3.0,
//! [`crate::backup_receipt::TopicConfiguration`]) instead of from cluster
//! defaults, and, once the signed verdict is a pass, applies the settings that
//! would have dropped, compacted, rejected or re-stamped restored records
//! before it. This module is the pure part, in one place, so the runner's
//! phase 0, the restore preflight's preview and phase 9 cannot disagree:
//!
//! * [`TopicConfigurationPlan`], the plan block;
//! * [`stage_of`], **THE STAGE TABLE**: when each recorded entry reaches the
//!   target (design `PROD-04.2-cutover.md` §4.2);
//! * [`plan_topics`], every topic's desired state and diff, or the refusal
//!   that stops the restore before anything is created;
//! * [`unchanged_since_creation`], the phase-9 guard
//!   (`ConfigChangedSinceCreation`);
//! * [`TopicConfigurationBlock`], the signed scorecard block, and
//!   [`TransitionOutcome`], the phase-9 attestation's;
//! * [`desired_state_document`], the `KafkaTopic` YAML an owned topic's
//!   configuration is exported as.
//!
//! | stage | keys | value |
//! |---|---|---|
//! | during restore (Logweir's) | [`during_restore`] | nothing restored is deleted, compacted, rejected or re-stamped before the verdict |
//! | at creation (the source's) | every other `portable` override; `removedInKafka4` only to a 3.x target; `requiresTieredStorage` where `validate_only` accepts it; `min.insync.replicas` capped at the replication factor | applied with the create |
//! | after verification (the source's) | [`AFTER_VERIFICATION_KEYS`] | phase 9, after a passing verdict |
//! | never | `inherited`, `clusterBound`, `providerOnly`, `secret` (and `removedInKafka4` to a 4.x target) | listed with the source's value |

use crate::backup_receipt::{ConfigEntry, TopicConfiguration, TopicOwner};
use crate::topic_configuration::{
    self as model, DeclaredOwner, CLUSTER_BOUND, INHERITED, PORTABLE, PROVIDER_ONLY,
    REMOVED_IN_KAFKA_4, REQUIRES_TIERED_STORAGE, SECRET,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The plan block `target.topic_configuration` (opt-in). Absent: a restore
/// creates its topics as it always did, byte for byte.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicConfigurationPlan {
    /// When the source's deferred settings reach the target.
    pub transition: Transition,
    /// The approver's statement of the topics' declarative owners, where the
    /// bound point's `owner_detection` is empty. Present, even empty, it is a
    /// place the restore looked (`declared`): `[]` says no topic has one.
    /// Keyed by SOURCE topic, with `crate::topic_configuration`'s rules.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owners: Option<Vec<DeclaredOwner>>,
}

/// `target.topic_configuration.transition`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Transition {
    /// Phase 9 applies the deferred settings once the signed verdict passed.
    AfterVerification,
    /// The topics keep the during-restore settings; the deferred set is
    /// reported, never applied.
    None,
}

impl Transition {
    /// The wire spelling.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::AfterVerification => TRANSITION_AFTER_VERIFICATION,
            Self::None => TRANSITION_NONE,
        }
    }
}

/// `transition: afterVerification`.
pub const TRANSITION_AFTER_VERIFICATION: &str = "afterVerification";
/// `transition: none`.
pub const TRANSITION_NONE: &str = "none";
/// The block's `transition` closed set (arm TC-2).
pub const TRANSITIONS: [&str; 2] = [TRANSITION_AFTER_VERIFICATION, TRANSITION_NONE];

/// The plan asks for the block and the bound point records no model (no
/// point, or a receipt before 1.3.0).
pub const TOPIC_CONFIGURATION_NOT_RECORDED: &str = "TopicConfigurationNotRecorded";
/// `CreateTopics validate_only` refused a topic's settings.
pub const TOPIC_CONFIG_REFUSED_BY_TARGET: &str = "TopicConfigRefusedByTarget";
/// A recorded class this build's table re-derives differently.
pub const TOPIC_CLASS_DISAGREES: &str = "TopicClassDisagrees";
/// A topic whose owner nobody looked for, with no statement in the plan.
pub const TOPIC_OWNER_NOT_CHECKED: &str = "TopicOwnerNotChecked";
/// Phase 9 found a topic's overrides changed since phase 0 set them.
pub const CONFIG_CHANGED_SINCE_CREATION: &str = "ConfigChangedSinceCreation";

/// `Long.MAX_VALUE`, Kafka's "no bound" for the timestamp keys.
pub const LONG_MAX: &str = "9223372036854775807";

/// The keys whose source value reaches the target only after verification:
/// each can drop, compact, reject or re-stamp a restored record.
pub const AFTER_VERIFICATION_KEYS: [&str; 9] = [
    "cleanup.policy",
    "local.retention.bytes",
    "local.retention.ms",
    "message.timestamp.after.max.ms",
    "message.timestamp.before.max.ms",
    "message.timestamp.difference.max.ms",
    "message.timestamp.type",
    "retention.bytes",
    "retention.ms",
];

/// The key `min.insync.replicas` is capped at the replication factor under.
pub const MIN_INSYNC_REPLICAS: &str = "min.insync.replicas";

/// `replication_factor.reason`: the source's factor fits the target.
pub const RF_SOURCE_FACTOR: &str = "sourceFactor";
/// `replication_factor.reason`: bounded by the target's broker count.
pub const RF_TARGET_BROKERS: &str = "targetBrokers";
/// `replication_factor.reason`: the model records none; the plan's
/// `default_replication_factor` was used.
pub const RF_NOT_RECORDED: &str = "notRecorded";
/// `replication_factor.reason`'s closed set (arm TC-3).
pub const RF_REASONS: [&str; 3] = [RF_SOURCE_FACTOR, RF_TARGET_BROKERS, RF_NOT_RECORDED];

/// A topic applied through Kafka's admin API.
pub const ROUTE_ADMIN_API: &str = "adminApi";
/// An owned topic, whose configuration is exported for its owner.
pub const ROUTE_DESIRED_STATE_EXPORT: &str = "desiredStateExport";
/// The routes a restore proceeds with (arm TC-3); `unknown` refuses.
pub const ROUTES: [&str; 2] = [ROUTE_ADMIN_API, ROUTE_DESIRED_STATE_EXPORT];

/// `not_applied[].reason` for a `removedInKafka4` override to a target that
/// is not on the 3.x line.
pub const NOT_APPLIED_REMOVED: &str = "removedInKafka4";

/// The broker line a target is on, as far as its broker configuration says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetLine {
    /// Kafka 3.x: it still defines the keys Kafka 4.0 removed.
    Kafka3,
    /// Kafka 4.x.
    Kafka4,
    /// Not known: a removed key is never applied, and nothing assumes 3.x.
    Unknown,
}

impl TargetLine {
    /// The table's line name ([`model::KAFKA_LINES`]); `4.x` for an unknown
    /// target, the line that defines fewer keys.
    #[must_use]
    pub const fn table_line(self) -> &'static str {
        match self {
            Self::Kafka3 => model::KAFKA_LINES[0],
            Self::Kafka4 | Self::Unknown => model::KAFKA_LINES[1],
        }
    }

    /// The line's name in a diff.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Kafka3 => "3.x",
            Self::Kafka4 => "4.x",
            Self::Unknown => "unknown",
        }
    }
}

/// The target's line, from its broker configuration: a 3.x broker reports
/// `log.message.format.version` (removed in 4.0); a broker that reports
/// `log.message.timestamp.after.max.ms` (3.6 and later) and not that key is
/// 4.x; anything else is unknown.
#[must_use]
pub fn target_line_of(broker: &BTreeMap<String, String>) -> TargetLine {
    if broker.contains_key("log.message.format.version") {
        TargetLine::Kafka3
    } else if broker.contains_key("log.message.timestamp.after.max.ms") {
        TargetLine::Kafka4
    } else {
        TargetLine::Unknown
    }
}

/// The settings every topic is created with while the restore runs, in
/// order: nothing restored is deleted (`retention.*`), compacted
/// (`cleanup.policy`), rejected (the timestamp bounds) or re-stamped
/// (`message.timestamp.type`) before the verdict. A 3.x broker falls back
/// from an unbounded `message.timestamp.before.max.ms` to
/// `message.timestamp.difference.max.ms`, so that key is pinned too there.
#[must_use]
pub fn during_restore(line: TargetLine) -> Vec<(&'static str, &'static str)> {
    let mut keys = vec![
        ("message.timestamp.type", "CreateTime"),
        ("retention.ms", "-1"),
        ("retention.bytes", "-1"),
        ("cleanup.policy", "delete"),
        ("message.timestamp.before.max.ms", LONG_MAX),
        ("message.timestamp.after.max.ms", LONG_MAX),
    ];
    if line == TargetLine::Kafka3 {
        keys.push(("message.timestamp.difference.max.ms", LONG_MAX));
    }
    keys
}

/// When one recorded entry reaches the target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stage {
    /// Applied with the create.
    AtCreation,
    /// Applied by phase 9, after a passing verdict.
    AfterVerification,
    /// Never applied; listed with the reason.
    Never(&'static str),
}

/// **THE STAGE TABLE.** When an entry of class `class` (the class this
/// build re-derived, never only the recorded one) for `key` reaches a target
/// on `line`.
#[must_use]
pub fn stage_of(key: &str, class: &str, line: TargetLine) -> Stage {
    let deferred = AFTER_VERIFICATION_KEYS.contains(&key);
    match class {
        PORTABLE | REQUIRES_TIERED_STORAGE => {
            if deferred {
                Stage::AfterVerification
            } else {
                Stage::AtCreation
            }
        }
        REMOVED_IN_KAFKA_4 if line == TargetLine::Kafka3 => {
            if deferred {
                Stage::AfterVerification
            } else {
                Stage::AtCreation
            }
        }
        REMOVED_IN_KAFKA_4 => Stage::Never(NOT_APPLIED_REMOVED),
        INHERITED => Stage::Never(INHERITED),
        CLUSTER_BOUND => Stage::Never(CLUSTER_BOUND),
        SECRET => Stage::Never(SECRET),
        _ => Stage::Never(PROVIDER_ONLY),
    }
}

/// The class this build's table gives a recorded entry: `secret` stays
/// `secret` (the broker flagged it; the table cannot), everything else is
/// [`model::classify`]'s answer for its key and source.
#[must_use]
pub fn rederived_class(key: &str, entry: &ConfigEntry) -> &'static str {
    if entry.portability == SECRET {
        return SECRET;
    }
    model::classify(key, &entry.source, false)
}

/// The target's default for a semantic topic key, read from its broker
/// configuration under the broker's own name, when the broker reported it.
#[must_use]
pub fn target_default<'a>(key: &str, broker: &'a BTreeMap<String, String>) -> Option<&'a String> {
    let synonym = match key {
        "cleanup.policy" => "log.cleanup.policy",
        "delete.retention.ms" => "log.cleaner.delete.retention.ms",
        "max.compaction.lag.ms" => "log.cleaner.max.compaction.lag.ms",
        "max.message.bytes" => "message.max.bytes",
        "message.timestamp.after.max.ms" => "log.message.timestamp.after.max.ms",
        "message.timestamp.before.max.ms" => "log.message.timestamp.before.max.ms",
        "message.timestamp.difference.max.ms" => "log.message.timestamp.difference.max.ms",
        "message.timestamp.type" => "log.message.timestamp.type",
        "min.compaction.lag.ms" => "log.cleaner.min.compaction.lag.ms",
        "min.insync.replicas" => "min.insync.replicas",
        "retention.bytes" => "log.retention.bytes",
        "retention.ms" => "log.retention.ms",
        "unclean.leader.election.enable" => "unclean.leader.election.enable",
        _ => return None,
    };
    broker.get(synonym)
}

/// The replication factor a topic is created with, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReplicationChoice {
    /// The source's factor, as the model records it. ABSENT: not recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<u32>,
    /// The factor the topic is created with; at least 1.
    pub target: u32,
    /// [`RF_REASONS`]: the source's, bounded by the target's brokers, or the
    /// plan's default because the model records none.
    pub reason: String,
}

/// `min(model, target brokers)`, with the reason; the plan's default when
/// the model records none.
#[must_use]
pub fn replication_choice(
    source: Option<u32>,
    brokers: Option<u32>,
    default_replication_factor: i16,
) -> ReplicationChoice {
    match source.filter(|f| *f >= 1) {
        Some(factor) => match brokers.filter(|b| *b >= 1) {
            Some(b) if b < factor => ReplicationChoice {
                source: Some(factor),
                target: b,
                reason: RF_TARGET_BROKERS.to_string(),
            },
            _ => ReplicationChoice {
                source: Some(factor),
                target: factor,
                reason: RF_SOURCE_FACTOR.to_string(),
            },
        },
        None => ReplicationChoice {
            source: None,
            target: u32::try_from(default_replication_factor.max(1)).unwrap_or(1),
            reason: RF_NOT_RECORDED.to_string(),
        },
    }
}

/// A recorded entry the restore does not apply, with the source's value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct NotApplied {
    /// The configuration key.
    pub key: String,
    /// The source's value. ABSENT exactly for a `secret` (arm TC-5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// The entry's class.
    pub portability: String,
    /// Why: the class, or `removedInKafka4` to a target not on the 3.x line.
    pub reason: String,
    /// The target broker's default for the key, where it reported one: the
    /// value the topic has once the transition removes Logweir's settings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_default: Option<String>,
}

/// A source value that was applied changed: `min.insync.replicas` capped at
/// the replication factor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Capped {
    /// The source's value.
    pub source: String,
    /// The value applied.
    pub applied: String,
}

/// **One topic's desired state and diff**, as phase 0 creates it and the
/// preview shows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TopicDesired {
    /// The source topic.
    pub source_topic: String,
    /// The model's partition count (the manifest's).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partitions: Option<u32>,
    /// The replication factor and its reason.
    pub replication_factor: ReplicationChoice,
    /// Logweir's settings while the restore runs ([`during_restore`]).
    pub during_restore: BTreeMap<String, String>,
    /// The source's settings applied with the create.
    pub at_creation: BTreeMap<String, String>,
    /// The source's settings applied after verification; for an owned topic,
    /// every source setting, exported for its owner instead.
    pub deferred: BTreeMap<String, String>,
    /// What is never applied, with the source's value.
    pub not_applied: Vec<NotApplied>,
    /// What was applied changed.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub capped: BTreeMap<String, Capped>,
    /// [`ROUTES`].
    pub route: String,
    /// The topic's owner, when it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<TopicOwner>,
    /// The model records no entry for the topic (its configuration read was
    /// denied or failed at backup): it is created with the during-restore
    /// settings only, and its configuration is NOT RECORDED.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub not_recorded: bool,
}

impl TopicDesired {
    /// The overrides the create carries: Logweir's, then the source's
    /// at-creation set.
    #[must_use]
    pub fn created_overrides(&self) -> BTreeMap<String, String> {
        let mut all = self.during_restore.clone();
        all.extend(self.at_creation.clone());
        all
    }

    /// The topic's whole desired override set: the at-creation set and the
    /// deferred set. Applied by ONE full-set `AlterConfigs`, which replaces
    /// every override, Logweir's included.
    #[must_use]
    pub fn final_overrides(&self) -> BTreeMap<String, String> {
        let mut all = self.at_creation.clone();
        all.extend(self.deferred.clone());
        all
    }
}

/// What [`plan_topics`] decides from.
#[derive(Debug, Clone)]
pub struct PlanInputs<'a> {
    /// The plan block.
    pub plan: &'a TopicConfigurationPlan,
    /// The bound, verified receipt's model; `None` when it records none.
    pub model: Option<&'a BTreeMap<String, TopicConfiguration>>,
    /// The receipt's `owner_detection`.
    pub owner_detection: &'a [String],
    /// Source topic to target topic.
    pub mapping: &'a BTreeMap<String, String>,
    /// The target's line.
    pub line: TargetLine,
    /// The target's broker count, when known.
    pub brokers: Option<u32>,
    /// The plan's `target.default_replication_factor`.
    pub default_replication_factor: i16,
    /// The target's broker configuration (for the inherited differences).
    pub broker: &'a BTreeMap<String, String>,
}

/// **Every topic's desired state**, keyed by TARGET topic, or the refusal
/// (a message opening with its code and `": "`) that stops the restore
/// before anything is created.
///
/// # Errors
///
/// [`TOPIC_CONFIGURATION_NOT_RECORDED`] for no model or a topic it does not
/// name; the plan's own owner statement refused by
/// `topic_configuration::refuse_declarations`;
/// [`TOPIC_OWNER_NOT_CHECKED`] for a topic whose owner nobody looked for;
/// [`TOPIC_CLASS_DISAGREES`] for a recorded class this table re-derives
/// differently.
pub fn plan_topics(inputs: &PlanInputs<'_>) -> Result<BTreeMap<String, TopicDesired>, String> {
    let Some(model) = inputs.model else {
        return Err(format!(
            "{TOPIC_CONFIGURATION_NOT_RECORDED}: the plan asks for target.topic_configuration and \
             the bound recovery point records no configuration model (a plan bound to no point, \
             or a receipt before format 1.3.0); restore without the block, or from a point \
             backed up by this build"
        ));
    };
    let sources: Vec<String> = inputs.mapping.keys().cloned().collect();
    if let Some(owners) = &inputs.plan.owners {
        if let Some(why) = model::refuse_declarations(owners, &sources) {
            return Err(format!(
                "target.topic_configuration.owners is refused: {}",
                why.replace("source.topic_owners", "target.topic_configuration.owners")
            ));
        }
    }
    let detection_ran = !inputs.owner_detection.is_empty() || inputs.plan.owners.is_some();
    let mut out = BTreeMap::new();
    for (source, target) in inputs.mapping {
        let Some(topic) = model.get(source) else {
            return Err(format!(
                "{TOPIC_CONFIGURATION_NOT_RECORDED}: the bound point's configuration model has \
                 no entry for source topic `{source}`"
            ));
        };
        let declared = inputs
            .plan
            .owners
            .iter()
            .flatten()
            .find(|o| &o.topic == source)
            .map(model::declared_owner);
        let owner = declared.or_else(|| topic.owner.clone());
        let route = model::apply_route(owner.is_some(), detection_ran);
        if !ROUTES.contains(&route) {
            return Err(format!(
                "{TOPIC_OWNER_NOT_CHECKED}: source topic `{source}` has no recorded owner and \
                 its backup looked for none (owner_detection is empty); state the owners in \
                 target.topic_configuration.owners (`[]` when no topic has one), or restore from \
                 a point whose backup looked"
            ));
        }
        out.insert(
            target.clone(),
            desired_for(source, topic, owner, route, inputs)?,
        );
    }
    Ok(out)
}

fn desired_for(
    source: &str,
    topic: &TopicConfiguration,
    owner: Option<TopicOwner>,
    route: &str,
    inputs: &PlanInputs<'_>,
) -> Result<TopicDesired, String> {
    let replication_factor = replication_choice(
        topic.replication_factor,
        inputs.brokers,
        inputs.default_replication_factor,
    );
    let during_restore: BTreeMap<String, String> = during_restore(inputs.line)
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let mut at_creation = BTreeMap::new();
    let mut deferred = BTreeMap::new();
    let mut not_applied = Vec::new();
    let mut capped = BTreeMap::new();
    for (key, entry) in topic.entries.iter().flatten() {
        let class = rederived_class(key, entry);
        if class != entry.portability {
            return Err(format!(
                "{TOPIC_CLASS_DISAGREES}: source topic `{source}` key `{key}` is recorded as \
                 `{}` and this build's table classes it `{class}`; nothing was created",
                entry.portability
            ));
        }
        match (stage_of(key, class, inputs.line), entry.value.as_ref()) {
            (Stage::AtCreation, Some(value)) => {
                let applied = if key == MIN_INSYNC_REPLICAS {
                    cap_min_insync(value, replication_factor.target)
                } else {
                    value.clone()
                };
                if &applied != value {
                    capped.insert(
                        key.clone(),
                        Capped {
                            source: value.clone(),
                            applied: applied.clone(),
                        },
                    );
                }
                at_creation.insert(key.clone(), applied);
            }
            (Stage::AfterVerification, Some(value)) => {
                deferred.insert(key.clone(), value.clone());
            }
            (Stage::Never(reason), value) => not_applied.push(NotApplied {
                key: key.clone(),
                value: if class == SECRET {
                    None
                } else {
                    value.cloned()
                },
                portability: entry.portability.clone(),
                reason: reason.to_string(),
                target_default: target_default(key, inputs.broker).cloned(),
            }),
            // An applied class with no value cannot be applied; the writer's
            // arm 17 refuses such a receipt, so this is listed, never guessed.
            (_, None) => not_applied.push(NotApplied {
                key: key.clone(),
                value: None,
                portability: entry.portability.clone(),
                reason: entry.portability.clone(),
                target_default: None,
            }),
        }
    }
    // An owned topic's configuration is its owner's: the topic is created with
    // Logweir's settings only, and every source setting is exported.
    if route == ROUTE_DESIRED_STATE_EXPORT {
        deferred.extend(std::mem::take(&mut at_creation));
    }
    Ok(TopicDesired {
        source_topic: source.to_string(),
        partitions: topic.partitions,
        replication_factor,
        during_restore,
        at_creation,
        deferred,
        not_applied,
        capped,
        route: route.to_string(),
        owner,
        not_recorded: topic.entries.is_none(),
    })
}

/// `min.insync.replicas` no larger than the replication factor: a topic whose
/// value exceeds it refuses every `acks=all` write (05.1 §2, measured).
fn cap_min_insync(value: &str, replication_factor: u32) -> String {
    match value.trim().parse::<u32>() {
        Ok(n) if n > replication_factor => replication_factor.to_string(),
        _ => value.to_string(),
    }
}

/// **The phase-9 guard**: a topic is transitioned only while its dynamic
/// overrides are EXACTLY the ones phase 0 created it with. Anything else —
/// an operator's change, another tool's, a key added or removed — is
/// `ConfigChangedSinceCreation`, and the topic is left alone.
#[must_use]
pub fn unchanged_since_creation(
    created: &BTreeMap<String, String>,
    now: &BTreeMap<String, String>,
) -> bool {
    created == now
}

/// Why phase 9 does not transition at all, or `None` when it does: only with
/// `transition: afterVerification`, only after a PASSING verdict, and only
/// for topics the run keeps.
#[must_use]
pub fn not_run_reason(transition: Transition, passed: bool, kept: bool) -> Option<&'static str> {
    if transition == Transition::None {
        Some("transitionNone")
    } else if !passed {
        Some("verdictNotPass")
    } else if !kept {
        Some("topicsNotKept")
    } else {
        None
    }
}

/// What phase 9 does with one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicStep {
    /// One full-set `AlterConfigs` with this set.
    Alter(BTreeMap<String, String>),
    /// An owned topic: exported, never altered.
    Export,
    /// Left alone, with the reason.
    Skip(String),
}

/// **The per-topic transition decision**: an owned topic is exported; a
/// topic whose overrides changed since creation is skipped; every other
/// topic gets its whole desired set.
#[must_use]
pub fn step_for(topic: &TopicDesired, overrides_now: &BTreeMap<String, String>) -> TopicStep {
    if topic.route == ROUTE_DESIRED_STATE_EXPORT {
        return TopicStep::Export;
    }
    if !unchanged_since_creation(&topic.created_overrides(), overrides_now) {
        return TopicStep::Skip(CONFIG_CHANGED_SINCE_CREATION.to_string());
    }
    TopicStep::Alter(topic.final_overrides())
}

/// **Scorecard `target.topic_configuration`** (MINOR, both lines): what
/// phase 0 created each topic with, what phase 9 will apply, and what it
/// never applies. Signed at phase 8, before the transition runs; the
/// transition's own outcome is the phase-9 attestation's
/// ([`TransitionOutcome`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TopicConfigurationBlock {
    /// [`TRANSITIONS`].
    pub transition: String,
    /// The target's line as its broker configuration said it.
    pub target_line: String,
    /// Per TARGET topic.
    pub topics: BTreeMap<String, TopicDesired>,
    /// The desired-state document of the owned topics, bound by digest;
    /// present exactly when a topic's route is `desiredStateExport` (arm
    /// TC-4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub desired_state: Option<DesiredStateRef>,
}

/// The desired-state document, by key, digest and length.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DesiredStateRef {
    /// `logweir/drills/<run_id>.desired-state.yaml`.
    pub key: String,
    /// `sha256:<64 lowercase hex>` of its bytes.
    pub sha256: String,
    /// Its length in bytes.
    pub bytes: u64,
}

/// `logweir/drills/<run_id>.desired-state.yaml`.
#[must_use]
pub fn desired_state_key(run_id: &str) -> String {
    format!("logweir/drills/{run_id}.desired-state.yaml")
}

/// The arms' answer for a block, or `None`: TC-2 to TC-5 (TC-1, the
/// version, is the scorecard's). Each message is a fixed sentence: nothing a
/// document's author chose reaches it.
#[must_use]
pub fn block_fault(block: &TopicConfigurationBlock) -> Option<&'static str> {
    if !TRANSITIONS.contains(&block.transition.as_str()) {
        return Some(
            "target.topic_configuration.transition is not \"afterVerification\" or \"none\"",
        );
    }
    for topic in block.topics.values() {
        if !ROUTES.contains(&topic.route.as_str())
            || !RF_REASONS.contains(&topic.replication_factor.reason.as_str())
            || topic.replication_factor.target == 0
        {
            return Some(
                "target.topic_configuration names a route outside adminApi and \
                 desiredStateExport, a replication factor reason outside its closed set, or a \
                 replication factor of 0",
            );
        }
    }
    let exported = block
        .topics
        .values()
        .any(|t| t.route == ROUTE_DESIRED_STATE_EXPORT);
    let shaped = block.desired_state.as_ref().is_none_or(|d| {
        d.sha256.strip_prefix("sha256:").is_some_and(|h| {
            h.len() == 64
                && h.bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        })
    });
    if exported != block.desired_state.is_some() || !shaped {
        return Some(
            "target.topic_configuration.desired_state is present exactly when a topic's route \
             is desiredStateExport, and names its document by sha256",
        );
    }
    for topic in block.topics.values() {
        if topic
            .not_applied
            .iter()
            .any(|n| (n.portability == SECRET) == n.value.is_some())
        {
            return Some(
                "target.topic_configuration lists a secret with a value, or another entry \
                 without one",
            );
        }
    }
    None
}

/// The topics of the desired-state document: every owned topic, in target
/// name order. `None` when no topic is owned.
#[must_use]
pub fn desired_state_document(topics: &BTreeMap<String, TopicDesired>) -> Option<String> {
    let mut docs = Vec::new();
    for (target, t) in topics {
        if t.route != ROUTE_DESIRED_STATE_EXPORT {
            continue;
        }
        let owner = t.owner.as_ref();
        let doc = if owner.is_some_and(|o| o.kind == "strimzi") {
            let namespace = owner
                .and_then(|o| o.reference.split_once('/'))
                .map(|(ns, _)| ns.to_string());
            let mut metadata = serde_yaml::Mapping::new();
            metadata.insert("name".into(), kubernetes_name(target).into());
            if let Some(ns) = namespace {
                metadata.insert("namespace".into(), ns.into());
            }
            let mut annotations = serde_yaml::Mapping::new();
            annotations.insert(
                "logweir.dev/restored-from".into(),
                t.source_topic.clone().into(),
            );
            if let Some(o) = owner {
                annotations.insert(
                    "logweir.dev/source-owner".into(),
                    o.reference.clone().into(),
                );
            }
            metadata.insert("annotations".into(), annotations.into());
            let mut spec = serde_yaml::Mapping::new();
            spec.insert("topicName".into(), target.clone().into());
            if let Some(p) = t.partitions {
                spec.insert("partitions".into(), u64::from(p).into());
            }
            spec.insert(
                "replicas".into(),
                u64::from(t.replication_factor.target).into(),
            );
            spec.insert("config".into(), config_mapping(&t.deferred));
            let mut root = serde_yaml::Mapping::new();
            root.insert("apiVersion".into(), "kafka.strimzi.io/v1beta2".into());
            root.insert("kind".into(), "KafkaTopic".into());
            root.insert("metadata".into(), metadata.into());
            root.insert("spec".into(), spec.into());
            root
        } else {
            let mut root = serde_yaml::Mapping::new();
            root.insert("apiVersion".into(), "logweir.dev/desired-state/v1".into());
            root.insert("kind".into(), "TopicDesiredState".into());
            root.insert("topic".into(), target.clone().into());
            root.insert("restoredFrom".into(), t.source_topic.clone().into());
            if let Some(o) = owner {
                let mut ow = serde_yaml::Mapping::new();
                ow.insert("kind".into(), o.kind.clone().into());
                ow.insert("reference".into(), o.reference.clone().into());
                root.insert("owner".into(), ow.into());
            }
            if let Some(p) = t.partitions {
                root.insert("partitions".into(), u64::from(p).into());
            }
            root.insert(
                "replicationFactor".into(),
                u64::from(t.replication_factor.target).into(),
            );
            root.insert("config".into(), config_mapping(&t.deferred));
            root
        };
        docs.push(serde_yaml::to_string(&serde_yaml::Value::Mapping(doc)).ok()?);
    }
    if docs.is_empty() {
        return None;
    }
    Some(docs.iter().map(|d| format!("---\n{d}")).collect::<String>())
}

fn config_mapping(config: &BTreeMap<String, String>) -> serde_yaml::Value {
    let mut m = serde_yaml::Mapping::new();
    for (k, v) in config {
        m.insert(k.clone().into(), v.clone().into());
    }
    m.into()
}

/// A Kubernetes object name for a topic name: lowercase, `_` and every other
/// character outside `[a-z0-9.-]` as `-`, at most 253 characters; when that
/// changed the name, the first 8 hex digits of the topic's sha256 are
/// appended so two topics never share one.
#[must_use]
pub fn kubernetes_name(topic: &str) -> String {
    let cleaned: String = topic
        .chars()
        .map(|c| {
            let c = c.to_ascii_lowercase();
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect();
    let cleaned = cleaned
        .trim_matches(|c: char| c == '-' || c == '.')
        .to_string();
    if cleaned == topic && !cleaned.is_empty() {
        return cleaned;
    }
    let digest = crate::ids::sha256_prefixed(topic.as_bytes());
    let short = digest.trim_start_matches("sha256:").get(..8).unwrap_or("0");
    let stem: String = cleaned.chars().take(240).collect();
    if stem.is_empty() {
        format!("topic-{short}")
    } else {
        format!("{stem}-{short}")
    }
}

/// One topic phase 9 could not transition, with its whole desired set: the
/// remaining step, which the operator applies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitionFailure {
    /// The target topic.
    pub topic: String,
    /// Why, bounded.
    pub error: String,
    /// The topic's whole desired override set.
    pub desired: BTreeMap<String, String>,
}

/// A topic phase 9 left alone, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitionSkip {
    /// The target topic.
    pub topic: String,
    /// [`CONFIG_CHANGED_SINCE_CREATION`].
    pub reason: String,
    /// The topic's whole desired override set.
    pub desired: BTreeMap<String, String>,
}

/// **The phase-9 attestation's `topic_configuration`** (signed; readers
/// verify it signature-only): what the transition did. Logweir never retries
/// on its own.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitionOutcome {
    /// [`TRANSITIONS`].
    pub transition: String,
    /// Why nothing ran (`transitionNone`, `verdictNotPass`, `topicsNotKept`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_run: Option<String>,
    /// Topics whose desired set was applied and read back.
    pub applied: Vec<String>,
    /// Topics the transition tried and could not.
    pub failed: Vec<TransitionFailure>,
    /// Topics left alone because their overrides changed since creation.
    pub skipped: Vec<TransitionSkip>,
    /// Owned topics, exported in the desired-state document.
    pub exported: Vec<String>,
    /// The desired-state document's key, when one was written.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub desired_state_key: Option<String>,
}

/// The runner's stdout key the controller lifts onto
/// `Restore.status.topicConfiguration`.
pub const TOPIC_CONFIGURATION_KEY_PREFIX: &str = "topic-configuration=";

/// The most topics one status line names per list.
pub const STATUS_TOPICS_MAX: usize = 256;

/// The `topic-configuration=` line's value: the outcome and, per topic, the
/// replication factor and the overrides before (Logweir's and the at-creation
/// set) and after (the whole desired set), in the status's camelCase.
#[must_use]
pub fn status_line_value(
    outcome: &TransitionOutcome,
    topics: &BTreeMap<String, TopicDesired>,
) -> String {
    let transition = match (
        &outcome.not_run,
        outcome.failed.is_empty() && outcome.skipped.is_empty(),
    ) {
        (Some(reason), _) => format!("notRun: {reason}"),
        (None, true) => "applied".to_string(),
        (None, false) => "partial".to_string(),
    };
    let mut o = serde_json::Map::new();
    o.insert("transition".into(), transition.into());
    o.insert(
        "applied".into(),
        serde_json::json!(outcome
            .applied
            .iter()
            .take(STATUS_TOPICS_MAX)
            .collect::<Vec<_>>()),
    );
    let failed: Vec<serde_json::Value> = outcome
        .failed
        .iter()
        .map(|f| serde_json::json!({"topic": f.topic, "error": f.error, "desired": f.desired}))
        .chain(outcome.skipped.iter().map(
            |s| serde_json::json!({"topic": s.topic, "error": s.reason, "desired": s.desired}),
        ))
        .take(STATUS_TOPICS_MAX)
        .collect();
    o.insert("failed".into(), failed.into());
    o.insert(
        "exported".into(),
        serde_json::json!(outcome
            .exported
            .iter()
            .take(STATUS_TOPICS_MAX)
            .collect::<Vec<_>>()),
    );
    if let Some(key) = &outcome.desired_state_key {
        o.insert("desiredStateKey".into(), key.clone().into());
    }
    let per_topic: Vec<serde_json::Value> = topics
        .iter()
        .take(STATUS_TOPICS_MAX)
        .map(|(name, t)| {
            let mut rf = serde_json::Map::new();
            if let Some(s) = t.replication_factor.source {
                rf.insert("source".into(), s.into());
            }
            rf.insert("target".into(), t.replication_factor.target.into());
            rf.insert("reason".into(), t.replication_factor.reason.clone().into());
            serde_json::json!({
                "topic": name,
                "route": t.route,
                "replicationFactor": rf,
                "before": t.created_overrides(),
                "after": t.final_overrides(),
            })
        })
        .collect();
    o.insert("topics".into(), per_topic.into());
    serde_json::Value::Object(o).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup_receipt::ConfigEntry;

    fn entry(value: Option<&str>, source: &str, portability: &str) -> ConfigEntry {
        ConfigEntry {
            value: value.map(str::to_string),
            source: source.to_string(),
            portability: portability.to_string(),
        }
    }

    const OVERRIDE: &str = "dynamicTopicConfig";

    fn compacted() -> TopicConfiguration {
        TopicConfiguration {
            partitions: Some(3),
            replication_factor: Some(3),
            entries: Some(BTreeMap::from([
                (
                    "cleanup.policy".to_string(),
                    entry(Some("compact"), OVERRIDE, PORTABLE),
                ),
                (
                    "min.compaction.lag.ms".to_string(),
                    entry(Some("1000"), OVERRIDE, PORTABLE),
                ),
                (
                    "min.insync.replicas".to_string(),
                    entry(Some("2"), OVERRIDE, PORTABLE),
                ),
                (
                    "retention.ms".to_string(),
                    entry(Some("604800000"), "defaultConfig", INHERITED),
                ),
                (
                    "message.format.version".to_string(),
                    entry(Some("3.0-IV1"), OVERRIDE, REMOVED_IN_KAFKA_4),
                ),
                (
                    "leader.replication.throttled.replicas".to_string(),
                    entry(Some("*"), OVERRIDE, CLUSTER_BOUND),
                ),
                (
                    "confluent.placement.constraints".to_string(),
                    entry(Some("{}"), OVERRIDE, PROVIDER_ONLY),
                ),
                ("vendor.token".to_string(), entry(None, OVERRIDE, SECRET)),
                (
                    "remote.storage.enable".to_string(),
                    entry(Some("true"), OVERRIDE, REQUIRES_TIERED_STORAGE),
                ),
                (
                    "message.timestamp.after.max.ms".to_string(),
                    entry(Some("9223372036854775807"), "defaultConfig", INHERITED),
                ),
            ])),
            owner: None,
        }
    }

    fn plan(transition: Transition, owners: Option<Vec<DeclaredOwner>>) -> TopicConfigurationPlan {
        TopicConfigurationPlan { transition, owners }
    }

    fn mapping() -> BTreeMap<String, String> {
        BTreeMap::from([("orders".to_string(), "r-orders".to_string())])
    }

    fn inputs<'a>(
        p: &'a TopicConfigurationPlan,
        model: Option<&'a BTreeMap<String, TopicConfiguration>>,
        detection: &'a [String],
        mapping: &'a BTreeMap<String, String>,
        line: TargetLine,
        brokers: Option<u32>,
        broker: &'a BTreeMap<String, String>,
    ) -> PlanInputs<'a> {
        PlanInputs {
            plan: p,
            model,
            owner_detection: detection,
            mapping,
            line,
            brokers,
            default_replication_factor: 1,
            broker,
        }
    }

    /// Every row of the stage table. KILLS: a deferred key applied at
    /// creation; a removed key applied to 4.x; an inherited, cluster-bound,
    /// provider or secret entry applied; a tiered-storage key never offered to
    /// `validate_only`.
    #[test]
    fn the_stage_table() {
        use Stage::{AfterVerification, AtCreation, Never};
        let k3 = TargetLine::Kafka3;
        let k4 = TargetLine::Kafka4;
        let rows: [(&str, &str, TargetLine, Stage); 16] = [
            ("cleanup.policy", PORTABLE, k4, AfterVerification),
            ("retention.ms", PORTABLE, k4, AfterVerification),
            ("retention.bytes", PORTABLE, k4, AfterVerification),
            ("message.timestamp.type", PORTABLE, k4, AfterVerification),
            (
                "message.timestamp.before.max.ms",
                PORTABLE,
                k3,
                AfterVerification,
            ),
            ("min.insync.replicas", PORTABLE, k4, AtCreation),
            ("segment.bytes", PORTABLE, k4, AtCreation),
            ("message.format.version", REMOVED_IN_KAFKA_4, k3, AtCreation),
            (
                "message.format.version",
                REMOVED_IN_KAFKA_4,
                k4,
                Never(NOT_APPLIED_REMOVED),
            ),
            (
                "message.timestamp.difference.max.ms",
                REMOVED_IN_KAFKA_4,
                k3,
                AfterVerification,
            ),
            (
                "message.timestamp.difference.max.ms",
                REMOVED_IN_KAFKA_4,
                TargetLine::Unknown,
                Never(NOT_APPLIED_REMOVED),
            ),
            (
                "remote.storage.enable",
                REQUIRES_TIERED_STORAGE,
                k4,
                AtCreation,
            ),
            (
                "local.retention.ms",
                REQUIRES_TIERED_STORAGE,
                k4,
                AfterVerification,
            ),
            ("retention.ms", INHERITED, k4, Never(INHERITED)),
            (
                "leader.replication.throttled.replicas",
                CLUSTER_BOUND,
                k3,
                Never(CLUSTER_BOUND),
            ),
            ("vendor.token", SECRET, k3, Never(SECRET)),
        ];
        for (key, class, line, want) in rows {
            assert_eq!(stage_of(key, class, line), want, "{key} {class} {line:?}");
        }
        assert_eq!(
            stage_of("confluent.x", PROVIDER_ONLY, k3),
            Never(PROVIDER_ONLY)
        );
    }

    #[test]
    fn the_during_restore_set_pins_every_key_that_drops_or_restamps_a_record() {
        let k4: BTreeMap<_, _> = during_restore(TargetLine::Kafka4).into_iter().collect();
        assert_eq!(k4["cleanup.policy"], "delete");
        assert_eq!(k4["retention.ms"], "-1");
        assert_eq!(k4["retention.bytes"], "-1");
        assert_eq!(k4["message.timestamp.type"], "CreateTime");
        assert_eq!(k4["message.timestamp.before.max.ms"], LONG_MAX);
        assert_eq!(k4["message.timestamp.after.max.ms"], LONG_MAX);
        assert!(!k4.contains_key("message.timestamp.difference.max.ms"));
        let k3: BTreeMap<_, _> = during_restore(TargetLine::Kafka3).into_iter().collect();
        assert_eq!(k3["message.timestamp.difference.max.ms"], LONG_MAX);
        // Every during-restore key is a deferred key: the full-set transition
        // replaces it.
        for (k, _) in during_restore(TargetLine::Kafka3) {
            assert!(AFTER_VERIFICATION_KEYS.contains(&k), "{k}");
        }
    }

    #[test]
    fn the_line_is_read_from_the_brokers_keys() {
        let b = |keys: &[&str]| -> BTreeMap<String, String> {
            keys.iter()
                .map(|k| ((*k).to_string(), "x".to_string()))
                .collect()
        };
        assert_eq!(
            target_line_of(&b(&[
                "log.message.format.version",
                "log.message.timestamp.after.max.ms"
            ])),
            TargetLine::Kafka3
        );
        assert_eq!(
            target_line_of(&b(&["log.message.timestamp.after.max.ms"])),
            TargetLine::Kafka4
        );
        assert_eq!(
            target_line_of(&b(&["log.retention.ms"])),
            TargetLine::Unknown
        );
    }

    /// KILLS: `max` for `min`; the reason not naming the brokers; a factor of
    /// 0 from an absent model.
    #[test]
    fn the_replication_factor_is_bounded_by_the_targets_brokers_with_its_reason() {
        let c = replication_choice(Some(3), Some(1), 1);
        assert_eq!((c.target, c.reason.as_str()), (1, RF_TARGET_BROKERS));
        let c = replication_choice(Some(3), Some(3), 1);
        assert_eq!((c.target, c.reason.as_str()), (3, RF_SOURCE_FACTOR));
        let c = replication_choice(Some(2), Some(5), 1);
        assert_eq!((c.target, c.reason.as_str()), (2, RF_SOURCE_FACTOR));
        let c = replication_choice(Some(3), None, 1);
        assert_eq!((c.target, c.reason.as_str()), (3, RF_SOURCE_FACTOR));
        let c = replication_choice(None, Some(3), 2);
        assert_eq!(
            (c.target, c.source, c.reason.as_str()),
            (2, None, RF_NOT_RECORDED)
        );
        let c = replication_choice(None, Some(3), 0);
        assert_eq!(c.target, 1);
    }

    #[test]
    fn a_compacted_topic_to_4_x_with_one_broker() {
        let model = BTreeMap::from([("orders".to_string(), compacted())]);
        let p = plan(Transition::AfterVerification, None);
        let m = mapping();
        let detection = vec!["kafkaTopicResources".to_string()];
        let broker = BTreeMap::from([
            (
                "log.message.timestamp.after.max.ms".to_string(),
                "3600000".to_string(),
            ),
            ("log.retention.ms".to_string(), "86400000".to_string()),
        ]);
        let topics = plan_topics(&inputs(
            &p,
            Some(&model),
            &detection,
            &m,
            TargetLine::Kafka4,
            Some(1),
            &broker,
        ))
        .unwrap();
        let t = &topics["r-orders"];
        assert_eq!(t.route, ROUTE_ADMIN_API);
        assert_eq!(
            (
                t.replication_factor.target,
                t.replication_factor.reason.as_str()
            ),
            (1, RF_TARGET_BROKERS)
        );
        // min.insync.replicas capped and stated.
        assert_eq!(t.at_creation["min.insync.replicas"], "1");
        assert_eq!(t.capped["min.insync.replicas"].source, "2");
        assert_eq!(t.at_creation["min.compaction.lag.ms"], "1000");
        assert_eq!(t.at_creation["remote.storage.enable"], "true");
        assert!(!t.at_creation.contains_key("cleanup.policy"));
        assert_eq!(t.deferred["cleanup.policy"], "compact");
        assert_eq!(t.created_overrides()["cleanup.policy"], "delete");
        assert_eq!(t.final_overrides()["cleanup.policy"], "compact");
        // The final set drops Logweir's keys the source did not set.
        assert!(!t.final_overrides().contains_key("retention.ms"));
        let reasons: BTreeMap<&str, (&str, Option<&str>, Option<&str>)> = t
            .not_applied
            .iter()
            .map(|n| {
                (
                    n.key.as_str(),
                    (
                        n.reason.as_str(),
                        n.value.as_deref(),
                        n.target_default.as_deref(),
                    ),
                )
            })
            .collect();
        assert_eq!(reasons["message.format.version"].0, NOT_APPLIED_REMOVED);
        assert_eq!(reasons["vendor.token"], (SECRET, None, None));
        assert_eq!(
            reasons["retention.ms"],
            (INHERITED, Some("604800000"), Some("86400000"))
        );
        // The 4.0 default change is named before Create.
        assert_eq!(
            reasons["message.timestamp.after.max.ms"],
            (INHERITED, Some(LONG_MAX), Some("3600000"))
        );
        assert_eq!(
            reasons["leader.replication.throttled.replicas"].0,
            CLUSTER_BOUND
        );
        assert_eq!(reasons["confluent.placement.constraints"].0, PROVIDER_ONLY);
        // The same point to 3.x applies the removed key.
        let topics = plan_topics(&inputs(
            &p,
            Some(&model),
            &detection,
            &m,
            TargetLine::Kafka3,
            Some(3),
            &broker,
        ))
        .unwrap();
        assert_eq!(
            topics["r-orders"].at_creation["message.format.version"],
            "3.0-IV1"
        );
        assert_eq!(topics["r-orders"].at_creation["min.insync.replicas"], "2");
        assert!(topics["r-orders"].capped.is_empty());
    }

    #[test]
    fn refusals_name_their_code() {
        let p = plan(Transition::AfterVerification, None);
        let m = mapping();
        let broker = BTreeMap::new();
        let none: Vec<String> = Vec::new();
        let err = plan_topics(&inputs(
            &p,
            None,
            &none,
            &m,
            TargetLine::Kafka4,
            None,
            &broker,
        ))
        .unwrap_err();
        assert!(err.starts_with("TopicConfigurationNotRecorded: "), "{err}");
        // NEGATIVE CONTROL: no owner detection and no statement refuses.
        let model = BTreeMap::from([("orders".to_string(), compacted())]);
        let err = plan_topics(&inputs(
            &p,
            Some(&model),
            &none,
            &m,
            TargetLine::Kafka4,
            None,
            &broker,
        ))
        .unwrap_err();
        assert!(err.starts_with("TopicOwnerNotChecked: "), "{err}");
        // `owners: []` is the statement that no topic has one.
        let stated = plan(Transition::AfterVerification, Some(Vec::new()));
        assert!(plan_topics(&inputs(
            &stated,
            Some(&model),
            &none,
            &m,
            TargetLine::Kafka4,
            None,
            &broker
        ))
        .is_ok());
        // A class the table disagrees with.
        let mut bad = compacted();
        bad.entries.as_mut().unwrap().insert(
            "segment.bytes".to_string(),
            entry(Some("1"), OVERRIDE, CLUSTER_BOUND),
        );
        let model = BTreeMap::from([("orders".to_string(), bad)]);
        let err = plan_topics(&inputs(
            &stated,
            Some(&model),
            &none,
            &m,
            TargetLine::Kafka4,
            None,
            &broker,
        ))
        .unwrap_err();
        assert!(err.starts_with("TopicClassDisagrees: "), "{err}");
        assert!(
            err.contains("`clusterBound`") && err.contains("`portable`"),
            "{err}"
        );
        // A topic the model does not name.
        let model = BTreeMap::from([("other".to_string(), compacted())]);
        let err = plan_topics(&inputs(
            &stated,
            Some(&model),
            &none,
            &m,
            TargetLine::Kafka4,
            None,
            &broker,
        ))
        .unwrap_err();
        assert!(err.starts_with("TopicConfigurationNotRecorded: "), "{err}");
    }

    #[test]
    fn an_owned_topic_is_created_with_logweirs_settings_and_exported() {
        let mut owned = compacted();
        owned.owner = Some(TopicOwner {
            kind: "strimzi".into(),
            basis: "kafkaTopicResource".into(),
            reference: "kafka/orders-kt".into(),
        });
        let model = BTreeMap::from([("orders".to_string(), owned)]);
        let p = plan(Transition::AfterVerification, None);
        let m = mapping();
        let detection = vec!["kafkaTopicResources".to_string()];
        let broker = BTreeMap::new();
        let topics = plan_topics(&inputs(
            &p,
            Some(&model),
            &detection,
            &m,
            TargetLine::Kafka4,
            Some(3),
            &broker,
        ))
        .unwrap();
        let t = &topics["r-orders"];
        assert_eq!(t.route, ROUTE_DESIRED_STATE_EXPORT);
        assert!(t.at_creation.is_empty());
        assert_eq!(t.deferred["cleanup.policy"], "compact");
        assert_eq!(t.deferred["min.insync.replicas"], "2");
        assert_eq!(step_for(t, &t.created_overrides()), TopicStep::Export);
        let doc = desired_state_document(&topics).unwrap();
        assert!(doc.contains("kind: KafkaTopic"), "{doc}");
        assert!(doc.contains("topicName: r-orders"), "{doc}");
        assert!(doc.contains("namespace: kafka"), "{doc}");
        assert!(doc.contains("cleanup.policy: compact"), "{doc}");
        assert!(doc.contains("replicas: 3"), "{doc}");
        // An unowned topic exports nothing.
        let model = BTreeMap::from([("orders".to_string(), compacted())]);
        let topics = plan_topics(&inputs(
            &p,
            Some(&model),
            &detection,
            &m,
            TargetLine::Kafka4,
            Some(3),
            &broker,
        ))
        .unwrap();
        assert_eq!(desired_state_document(&topics), None);
        // A declared external owner gets the neutral document.
        let declared = plan(
            Transition::AfterVerification,
            Some(vec![DeclaredOwner {
                topic: "orders".into(),
                kind: "external".into(),
                reference: "terraform: kafka_topic.orders".into(),
            }]),
        );
        let topics = plan_topics(&inputs(
            &declared,
            Some(&model),
            &[],
            &m,
            TargetLine::Kafka4,
            Some(3),
            &broker,
        ))
        .unwrap();
        let doc = desired_state_document(&topics).unwrap();
        assert!(doc.contains("kind: TopicDesiredState"), "{doc}");
    }

    #[test]
    fn a_topic_with_no_recorded_entries_gets_safe_settings_only() {
        let denied = TopicConfiguration {
            partitions: Some(2),
            replication_factor: Some(1),
            entries: None,
            owner: None,
        };
        let model = BTreeMap::from([("orders".to_string(), denied)]);
        let p = plan(Transition::AfterVerification, Some(Vec::new()));
        let m = mapping();
        let broker = BTreeMap::new();
        let topics = plan_topics(&inputs(
            &p,
            Some(&model),
            &[],
            &m,
            TargetLine::Kafka4,
            Some(1),
            &broker,
        ))
        .unwrap();
        let t = &topics["r-orders"];
        assert!(t.not_recorded);
        assert!(t.at_creation.is_empty() && t.deferred.is_empty() && t.not_applied.is_empty());
        assert_eq!(t.created_overrides().len(), 6);
    }

    /// `ConfigChangedSinceCreation`. KILLS: a subset compare; comparing keys
    /// only; transitioning a topic whose overrides changed.
    #[test]
    fn a_topic_whose_overrides_changed_since_creation_is_skipped() {
        let model = BTreeMap::from([("orders".to_string(), compacted())]);
        let p = plan(Transition::AfterVerification, Some(Vec::new()));
        let m = mapping();
        let broker = BTreeMap::new();
        let topics = plan_topics(&inputs(
            &p,
            Some(&model),
            &[],
            &m,
            TargetLine::Kafka4,
            Some(3),
            &broker,
        ))
        .unwrap();
        let t = &topics["r-orders"];
        let created = t.created_overrides();
        assert_eq!(step_for(t, &created), TopicStep::Alter(t.final_overrides()));
        let mut value_changed = created.clone();
        value_changed.insert("retention.ms".into(), "1000".into());
        let mut key_added = created.clone();
        key_added.insert("segment.ms".into(), "1".into());
        let mut key_removed = created.clone();
        key_removed.remove("cleanup.policy");
        for now in [value_changed, key_added, key_removed] {
            assert_eq!(
                step_for(t, &now),
                TopicStep::Skip(CONFIG_CHANGED_SINCE_CREATION.to_string())
            );
            assert!(!unchanged_since_creation(&created, &now));
        }
        assert!(unchanged_since_creation(&created, &created.clone()));
    }

    #[test]
    fn the_transition_runs_only_after_a_pass_on_kept_topics() {
        assert_eq!(
            not_run_reason(Transition::AfterVerification, true, true),
            None
        );
        assert_eq!(
            not_run_reason(Transition::AfterVerification, false, true),
            Some("verdictNotPass")
        );
        assert_eq!(
            not_run_reason(Transition::AfterVerification, true, false),
            Some("topicsNotKept")
        );
        assert_eq!(
            not_run_reason(Transition::None, true, true),
            Some("transitionNone")
        );
    }

    #[test]
    fn the_block_arms() {
        let model = BTreeMap::from([("orders".to_string(), compacted())]);
        let p = plan(Transition::AfterVerification, Some(Vec::new()));
        let m = mapping();
        let broker = BTreeMap::new();
        let topics = plan_topics(&inputs(
            &p,
            Some(&model),
            &[],
            &m,
            TargetLine::Kafka4,
            Some(3),
            &broker,
        ))
        .unwrap();
        let good = TopicConfigurationBlock {
            transition: TRANSITION_AFTER_VERIFICATION.into(),
            target_line: "4.x".into(),
            topics,
            desired_state: None,
        };
        assert_eq!(block_fault(&good), None);
        let mut b = good.clone();
        b.transition = "later".into();
        assert!(block_fault(&b).unwrap().contains("transition"));
        let mut b = good.clone();
        b.topics.get_mut("r-orders").unwrap().route = "unknown".into();
        assert!(block_fault(&b).unwrap().contains("route"));
        let mut b = good.clone();
        b.topics
            .get_mut("r-orders")
            .unwrap()
            .replication_factor
            .target = 0;
        assert!(block_fault(&b).unwrap().contains("replication factor of 0"));
        let mut b = good.clone();
        b.desired_state = Some(DesiredStateRef {
            key: "k".into(),
            sha256: format!("sha256:{}", "a".repeat(64)),
            bytes: 1,
        });
        assert!(block_fault(&b).unwrap().contains("desired_state"));
        let mut b = good.clone();
        b.topics.get_mut("r-orders").unwrap().route = ROUTE_DESIRED_STATE_EXPORT.into();
        assert!(block_fault(&b).unwrap().contains("desired_state"));
        b.desired_state = Some(DesiredStateRef {
            key: "k".into(),
            sha256: format!("sha256:{}", "A".repeat(64)),
            bytes: 1,
        });
        assert!(block_fault(&b).unwrap().contains("desired_state"));
        b.desired_state.as_mut().unwrap().sha256 = format!("sha256:{}", "a".repeat(64));
        assert_eq!(block_fault(&b), None);
        let mut b = good.clone();
        b.topics
            .get_mut("r-orders")
            .unwrap()
            .not_applied
            .iter_mut()
            .find(|n| n.portability == SECRET)
            .unwrap()
            .value = Some("hunter2".into());
        assert!(block_fault(&b).unwrap().contains("secret"));
    }

    #[test]
    fn a_kubernetes_name_is_lowercase_and_distinct() {
        assert_eq!(kubernetes_name("orders"), "orders");
        let a = kubernetes_name("Orders_A");
        let b = kubernetes_name("orders-a");
        assert_ne!(a, b);
        assert!(a.starts_with("orders-a-"), "{a}");
        assert!(kubernetes_name("___").starts_with("topic-"));
    }

    #[test]
    fn the_status_line_names_the_outcome_and_each_topics_factor() {
        let model = BTreeMap::from([("orders".to_string(), compacted())]);
        let p = plan(Transition::AfterVerification, Some(Vec::new()));
        let m = mapping();
        let broker = BTreeMap::new();
        let topics = plan_topics(&inputs(
            &p,
            Some(&model),
            &[],
            &m,
            TargetLine::Kafka4,
            Some(1),
            &broker,
        ))
        .unwrap();
        let outcome = TransitionOutcome {
            transition: TRANSITION_AFTER_VERIFICATION.into(),
            applied: vec!["r-orders".into()],
            ..TransitionOutcome::default()
        };
        let line: serde_json::Value =
            serde_json::from_str(&status_line_value(&outcome, &topics)).unwrap();
        assert_eq!(line["transition"], "applied");
        assert_eq!(
            line["topics"][0]["replicationFactor"]["reason"],
            RF_TARGET_BROKERS
        );
        assert_eq!(line["topics"][0]["before"]["cleanup.policy"], "delete");
        assert_eq!(line["topics"][0]["after"]["cleanup.policy"], "compact");
        let partial = TransitionOutcome {
            failed: vec![TransitionFailure {
                topic: "r-orders".into(),
                error: "denied".into(),
                desired: BTreeMap::new(),
            }],
            ..outcome
        };
        let line: serde_json::Value =
            serde_json::from_str(&status_line_value(&partial, &topics)).unwrap();
        assert_eq!(line["transition"], "partial");
        assert_eq!(line["failed"][0]["topic"], "r-orders");
    }

    #[test]
    fn the_plan_block_parses_and_refuses_unknown_fields() {
        let p: TopicConfigurationPlan =
            serde_yaml::from_str("transition: afterVerification\nowners: []\n").unwrap();
        assert_eq!(p.transition, Transition::AfterVerification);
        assert_eq!(p.owners, Some(Vec::new()));
        assert!(serde_yaml::from_str::<TopicConfigurationPlan>("transition: later\n").is_err());
        assert!(
            serde_yaml::from_str::<TopicConfigurationPlan>("transition: none\nretry: true\n")
                .is_err()
        );
    }
}
