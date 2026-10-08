//! **PROD-05.1: the topic configuration model and its portability table.**
//!
//! A backup receipt from format 1.3.0 records, per named topic, what a later
//! restore needs to rebuild the topic rather than guess at it
//! ([`crate::backup_receipt::TopicConfiguration`]): the source's partition
//! count and replication factor as the archive records them, the topic's
//! configuration entries as Logweir's own DescribeConfigs read returned them,
//! each with its source and a PORTABILITY CLASS from the table below, and the
//! topic's declarative owner when one manages it. This module holds the parts
//! of that model that are pure functions of a key, a source and a flag: the
//! table, the classification, the capture rule and the detection of Strimzi
//! owners from `KafkaTopic` resources. `docs/to-do/decisions/
//! PROD-05.1-configuration-model.md` is the decision record PROD-05.2 consumes.
//!
//! # The table is MEASURED, on both broker lines
//!
//! [`TABLE`] names every topic configuration key Apache Kafka 3.9 and 4.x
//! define, and nothing else. Two facts, each measured against the compose
//! stack's pinned brokers (`apache/kafka` 3.9.2 and 4.3.1 by digest) by
//! `e2e/tests/topic_configuration.rs::the_portability_table_is_the_brokers`:
//!
//! 1. **The key set.** DescribeConfigs of a fresh topic returns exactly the
//!    keys [`defined_on`] says for that line: 36 on 3.9, 33 on 4.x. The three
//!    keys Kafka 4.0 removed (`message.downconversion.enable`,
//!    `message.format.version`, `message.timestamp.difference.max.ms`) are the
//!    whole difference.
//! 2. **The CreateTopics verdict.** `CreateTopics` with `validate_only` and
//!    each key's [`KeyRule::sample`] is accepted or refused exactly as
//!    [`KeyRule::accepted`] records, on a broker without remote log storage:
//!    the removed keys are "Unknown topic config name" on 4.x, and
//!    `remote.storage.enable=true` is refused on both lines ("Tiered Storage
//!    functionality is disabled in the broker").
//!
//! A key the table does not name is [`PROVIDER_ONLY`]: a provider's own
//! setting (measured: `confluent.placement.constraints` is "Unknown topic
//! config name" on both lines), or one a later Kafka adds. It is recorded, so
//! a restore can say it was there, and never applied.
//!
//! # What the classes mean for a restore (PROD-05.2)
//!
//! | class | meaning | applied by a restore |
//! |---|---|---|
//! | `portable` | an explicit topic override of a key both lines define | yes, as an override |
//! | `inherited` | the value came from the broker (any source but the topic's own override) | never: the target's own broker decides; recorded so a difference can be explained |
//! | `removedInKafka4` | an explicit override of a key Kafka 4.0 removed | to a 3.9 target only; never to 4.x, and never read as "the default" |
//! | `clusterBound` | names the SOURCE cluster's replicas (the reassignment throttles) | never |
//! | `requiresTieredStorage` | means something only where the target enables remote log storage | only after the target is checked for it |
//! | `providerOnly` | a key neither line defines | never |
//! | `secret` | the broker flagged the entry sensitive | never: the value is not captured, only the key |
//!
//! "Never applied as a default" is the rule FX-4 and this row share: an entry
//! that is not applied is still RECORDED, so its absence on the target is a
//! named difference, not a silent default.

use crate::backup_receipt::{ConfigEntry, TopicOwner};
use std::collections::BTreeMap;

/// The source a DescribeConfigs entry carries when it is the TOPIC's own
/// explicit override — `DYNAMIC_TOPIC_CONFIG`, camel-cased as
/// [`crate::backup_receipt::CONFIG_SOURCES`] spells it.
pub const TOPIC_OVERRIDE_SOURCE: &str = "dynamicTopicConfig";

/// The broker lines the table is measured on, in order: [`KeyRule::accepted`]
/// is indexed by position in this list.
pub const KAFKA_LINES: [&str; 2] = ["3.9", "4.x"];

/// `portable`.
pub const PORTABLE: &str = "portable";
/// `inherited`.
pub const INHERITED: &str = "inherited";
/// `removedInKafka4`.
pub const REMOVED_IN_KAFKA_4: &str = "removedInKafka4";
/// `clusterBound`.
pub const CLUSTER_BOUND: &str = "clusterBound";
/// `requiresTieredStorage`.
pub const REQUIRES_TIERED_STORAGE: &str = "requiresTieredStorage";
/// `providerOnly`.
pub const PROVIDER_ONLY: &str = "providerOnly";
/// `secret`.
pub const SECRET: &str = "secret";

/// `ConfigEntry::portability`'s closed set (receipt arm 16), in the order the
/// refusal names them.
pub const PORTABILITY_CLASSES: [&str; 7] = [
    PORTABLE,
    INHERITED,
    REMOVED_IN_KAFKA_4,
    CLUSTER_BOUND,
    REQUIRES_TIERED_STORAGE,
    PROVIDER_ONLY,
    SECRET,
];

/// `TopicOwner::kind`'s closed set (receipt arm 18).
pub const OWNER_KINDS: [&str; 2] = ["strimzi", "external"];

/// `TopicOwner::basis`'s closed set (receipt arm 18): found from a Strimzi
/// `KafkaTopic` resource, or declared by the plan.
pub const OWNER_BASES: [&str; 2] = ["kafkaTopicResource", "declared"];

/// The longest `TopicOwner::reference`, in characters (receipt arm 18).
pub const MAX_OWNER_REFERENCE_CHARS: usize = 256;

/// The Strimzi label that names the Kafka cluster a `KafkaTopic` belongs to.
pub const STRIMZI_CLUSTER_LABEL: &str = "strimzi.io/cluster";

/// The Strimzi annotation that takes a `KafkaTopic` out of the topic
/// operator's hands (`"false"`): such a resource does not manage the topic.
pub const STRIMZI_MANAGED_ANNOTATION: &str = "strimzi.io/managed";

/// What a key IS, independently of how a topic got its value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyClass {
    /// Defined on every line in [`KAFKA_LINES`]; an override of it is applied.
    Portable,
    /// Defined on 3.9, removed in Kafka 4.0.
    RemovedInKafka4,
    /// Names the source cluster's own replicas; never applied.
    ClusterBound,
    /// Means something only where the target enables remote log storage.
    RequiresTieredStorage,
}

impl KeyClass {
    /// The wire name an OVERRIDE of a key of this class is recorded with.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Portable => PORTABLE,
            Self::RemovedInKafka4 => REMOVED_IN_KAFKA_4,
            Self::ClusterBound => CLUSTER_BOUND,
            Self::RequiresTieredStorage => REQUIRES_TIERED_STORAGE,
        }
    }
}

/// One row of the portability table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyRule {
    /// The topic configuration key.
    pub key: &'static str,
    /// What the key is.
    pub class: KeyClass,
    /// Whether the key's EFFECTIVE value decides which records a restored
    /// topic keeps, accepts or how it stamps and replicates them — so its
    /// inherited value is recorded too, not only an override.
    pub semantic: bool,
    /// A valid value, for the `CreateTopics` `validate_only` probe.
    pub sample: &'static str,
    /// Whether that probe is accepted, per [`KAFKA_LINES`] entry, on a broker
    /// without remote log storage.
    pub accepted: [bool; 2],
}

const fn rule(
    key: &'static str,
    class: KeyClass,
    semantic: bool,
    sample: &'static str,
    accepted: [bool; 2],
) -> KeyRule {
    KeyRule {
        key,
        class,
        semantic,
        sample,
        accepted,
    }
}

const BOTH: [bool; 2] = [true, true];
const ONLY_3_9: [bool; 2] = [true, false];
const NEITHER: [bool; 2] = [false, false];

/// **THE PORTABILITY TABLE**, in key order: every topic configuration key
/// Apache Kafka 3.9 and 4.x define (measured; see the module doc).
pub const TABLE: [KeyRule; 36] = {
    use KeyClass::{ClusterBound, Portable, RemovedInKafka4, RequiresTieredStorage};
    [
        rule("cleanup.policy", Portable, true, "compact", BOTH),
        rule("compression.gzip.level", Portable, false, "5", BOTH),
        rule("compression.lz4.level", Portable, false, "9", BOTH),
        rule("compression.type", Portable, false, "zstd", BOTH),
        rule("compression.zstd.level", Portable, false, "3", BOTH),
        rule("delete.retention.ms", Portable, true, "86400000", BOTH),
        rule("file.delete.delay.ms", Portable, false, "60000", BOTH),
        rule("flush.messages", Portable, false, "10000", BOTH),
        rule("flush.ms", Portable, false, "1000", BOTH),
        rule(
            "follower.replication.throttled.replicas",
            ClusterBound,
            false,
            "*",
            BOTH,
        ),
        rule("index.interval.bytes", Portable, false, "4096", BOTH),
        rule(
            "leader.replication.throttled.replicas",
            ClusterBound,
            false,
            "*",
            BOTH,
        ),
        rule(
            "local.retention.bytes",
            RequiresTieredStorage,
            false,
            "1000000",
            BOTH,
        ),
        rule(
            "local.retention.ms",
            RequiresTieredStorage,
            false,
            "60000",
            BOTH,
        ),
        rule("max.compaction.lag.ms", Portable, true, "86400000", BOTH),
        rule("max.message.bytes", Portable, true, "2097152", BOTH),
        rule(
            "message.downconversion.enable",
            RemovedInKafka4,
            false,
            "false",
            ONLY_3_9,
        ),
        rule(
            "message.format.version",
            RemovedInKafka4,
            false,
            "3.0-IV1",
            ONLY_3_9,
        ),
        rule(
            "message.timestamp.after.max.ms",
            Portable,
            true,
            "3600000",
            BOTH,
        ),
        rule(
            "message.timestamp.before.max.ms",
            Portable,
            true,
            "86400000",
            BOTH,
        ),
        rule(
            "message.timestamp.difference.max.ms",
            RemovedInKafka4,
            true,
            "86400000",
            ONLY_3_9,
        ),
        rule(
            "message.timestamp.type",
            Portable,
            true,
            "LogAppendTime",
            BOTH,
        ),
        rule("min.cleanable.dirty.ratio", Portable, false, "0.3", BOTH),
        rule("min.compaction.lag.ms", Portable, true, "1000", BOTH),
        rule("min.insync.replicas", Portable, true, "2", BOTH),
        rule("preallocate", Portable, false, "true", BOTH),
        rule(
            "remote.log.copy.disable",
            RequiresTieredStorage,
            false,
            "true",
            BOTH,
        ),
        rule(
            "remote.log.delete.on.disable",
            RequiresTieredStorage,
            false,
            "true",
            BOTH,
        ),
        rule(
            "remote.storage.enable",
            RequiresTieredStorage,
            true,
            "true",
            NEITHER,
        ),
        rule("retention.bytes", Portable, true, "1073741824", BOTH),
        rule("retention.ms", Portable, true, "86400000", BOTH),
        rule("segment.bytes", Portable, false, "104857600", BOTH),
        rule("segment.index.bytes", Portable, false, "10485760", BOTH),
        rule("segment.jitter.ms", Portable, false, "1000", BOTH),
        rule("segment.ms", Portable, false, "3600000", BOTH),
        rule(
            "unclean.leader.election.enable",
            Portable,
            true,
            "true",
            BOTH,
        ),
    ]
};

/// The table's row for `key`, or `None` for a key neither line defines.
#[must_use]
pub fn rule_for(key: &str) -> Option<&'static KeyRule> {
    TABLE.iter().find(|r| r.key == key)
}

/// Whether Apache Kafka on `line` (an entry of [`KAFKA_LINES`]) defines `key`
/// as a topic configuration.
#[must_use]
pub fn defined_on(key: &str, line: &str) -> bool {
    match rule_for(key) {
        None => false,
        Some(r) => match r.class {
            KeyClass::RemovedInKafka4 => line == KAFKA_LINES[0],
            _ => KAFKA_LINES.contains(&line),
        },
    }
}

/// **THE CLASSIFICATION**: one entry's portability class, from its key, the
/// source the broker reported it with, and whether the broker flagged it
/// sensitive. Precedence, highest first:
///
/// 1. sensitive: `secret`, whatever the key and source;
/// 2. any source but the topic's own override: `inherited`;
/// 3. the table's class for the key, or `providerOnly` for a key it does not
///    name.
#[must_use]
pub fn classify(key: &str, source: &str, sensitive: bool) -> &'static str {
    if sensitive {
        return SECRET;
    }
    if source != TOPIC_OVERRIDE_SOURCE {
        return INHERITED;
    }
    rule_for(key).map_or(PROVIDER_ONLY, |r| r.class.wire_name())
}

/// **THE CAPTURE RULE**: whether a DescribeConfigs entry is recorded at all.
///
/// Every explicit override (any key, the provider's own included), and the
/// effective value of every SEMANTIC key whatever its source. Nothing else:
/// a non-semantic key the topic inherits is the target broker's business and
/// recording 30-odd defaults per topic would bury the overrides.
#[must_use]
pub fn records(key: &str, source: &str) -> bool {
    source == TOPIC_OVERRIDE_SOURCE || rule_for(key).is_some_and(|r| r.semantic)
}

/// One DescribeConfigs entry as the model records it: `None` for an entry the
/// capture rule leaves out, or one that is not sensitive and carries no value
/// (nothing was observed).
///
/// A sensitive entry is recorded with NO value whatever the broker answered —
/// Kafka answers `null` for one, and a provider that did not would still not
/// have its secret copied into a signed document.
#[must_use]
pub fn entry_of(
    key: &str,
    value: Option<&str>,
    source: &str,
    sensitive: bool,
) -> Option<ConfigEntry> {
    if !records(key, source) {
        return None;
    }
    let portability = classify(key, source, sensitive);
    if portability == SECRET {
        return Some(ConfigEntry {
            value: None,
            source: source.to_string(),
            portability: SECRET.to_string(),
        });
    }
    value.map(|v| ConfigEntry {
        value: Some(v.to_string()),
        source: source.to_string(),
        portability: portability.to_string(),
    })
}

/// A plan's declaration that a topic is managed outside Kafka's admin API —
/// `source.topic_owners[]` in the backup grammar (`spec::BackupSourceSpec`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeclaredOwner {
    /// The named source topic it is about. Must be one of `source.topics`.
    pub topic: String,
    /// `strimzi` or `external` ([`OWNER_KINDS`]).
    pub kind: String,
    /// Where the desired state lives — a `KafkaTopic` `namespace/name`, a
    /// Terraform address, a repository path. 1 to
    /// [`MAX_OWNER_REFERENCE_CHARS`] characters, no control character. Never
    /// a credential: it is copied into a signed receipt.
    pub reference: String,
}

/// Why a declared owner is refused, or `None` — the same rule receipt arm 18
/// enforces on the recorded owner, plus "the topic is one the plan names".
#[must_use]
pub fn refuse_declared(owner: &DeclaredOwner, named: &[String]) -> Option<String> {
    if !named.contains(&owner.topic) {
        return Some(format!(
            "source.topic_owners names topic {:?}, which is not one of source.topics",
            owner.topic
        ));
    }
    if !OWNER_KINDS.contains(&owner.kind.as_str()) {
        return Some(format!(
            "source.topic_owners[{:?}].kind {:?} is not \"strimzi\" or \"external\"",
            owner.topic, owner.kind
        ));
    }
    if !reference_fits(&owner.reference) {
        return Some(format!(
            "source.topic_owners[{:?}].reference must be 1 to {MAX_OWNER_REFERENCE_CHARS} \
             characters with no control character",
            owner.topic
        ));
    }
    None
}

/// Arm 18's reference rule: non-blank, at most
/// [`MAX_OWNER_REFERENCE_CHARS`] characters, no control character.
#[must_use]
pub fn reference_fits(reference: &str) -> bool {
    !reference.trim().is_empty()
        && reference.chars().count() <= MAX_OWNER_REFERENCE_CHARS
        && !reference.chars().any(char::is_control)
}

/// **STRIMZI OWNERS, FROM `KafkaTopic` RESOURCES.**
///
/// `resources` are parsed YAML documents as `kubectl get kafkatopics -o yaml`
/// writes them: single resources, or a `List` (any kind ending in `List`)
/// whose `items` are. A resource names a topic when it is
/// `kafka.strimzi.io/*` `KafkaTopic`, carries a non-empty
/// [`STRIMZI_CLUSTER_LABEL`] (equal to `cluster` when one is given), is not
/// annotated [`STRIMZI_MANAGED_ANNOTATION`]`: "false"`, and its topic name
/// (`spec.topicName`, else `metadata.name`) is one of `topics`. Such a topic
/// is owned: `{kind: strimzi, basis: kafkaTopicResource, reference:
/// "<namespace>/<name>"}`. Two resources naming one topic (Strimzi itself
/// reports that as a conflict) give the reference that sorts first.
#[must_use]
pub fn strimzi_owners(
    resources: &[serde_yaml::Value],
    topics: &[String],
    cluster: Option<&str>,
) -> BTreeMap<String, TopicOwner> {
    let mut found: BTreeMap<String, TopicOwner> = BTreeMap::new();
    let mut flat: Vec<&serde_yaml::Value> = Vec::new();
    for doc in resources {
        let kind = doc.get("kind").and_then(serde_yaml::Value::as_str);
        match (
            kind,
            doc.get("items").and_then(serde_yaml::Value::as_sequence),
        ) {
            (Some(k), Some(items)) if k.ends_with("List") => flat.extend(items.iter()),
            _ => flat.push(doc),
        }
    }
    for resource in flat {
        let Some((topic, reference)) = strimzi_topic_of(resource, cluster) else {
            continue;
        };
        if !topics.contains(&topic) || !reference_fits(&reference) {
            continue;
        }
        let candidate = TopicOwner {
            kind: OWNER_KINDS[0].to_string(),
            basis: OWNER_BASES[0].to_string(),
            reference,
        };
        found
            .entry(topic)
            .and_modify(|kept| {
                if candidate.reference < kept.reference {
                    *kept = candidate.clone();
                }
            })
            .or_insert(candidate);
    }
    found
}

/// `(topic, "<namespace>/<name>")` for a `KafkaTopic` that manages its topic.
fn strimzi_topic_of(
    resource: &serde_yaml::Value,
    cluster: Option<&str>,
) -> Option<(String, String)> {
    let str_at = |v: &serde_yaml::Value, key: &str| -> Option<String> {
        v.get(key)
            .and_then(serde_yaml::Value::as_str)
            .map(str::to_string)
    };
    let api = str_at(resource, "apiVersion")?;
    if !api.starts_with("kafka.strimzi.io/") || str_at(resource, "kind")? != "KafkaTopic" {
        return None;
    }
    let metadata = resource.get("metadata")?;
    let label = metadata
        .get("labels")
        .and_then(|l| l.get(STRIMZI_CLUSTER_LABEL))
        .and_then(serde_yaml::Value::as_str)
        .filter(|c| !c.trim().is_empty())?;
    if cluster.is_some_and(|wanted| wanted != label) {
        return None;
    }
    let unmanaged = metadata
        .get("annotations")
        .and_then(|a| a.get(STRIMZI_MANAGED_ANNOTATION))
        .and_then(serde_yaml::Value::as_str)
        == Some("false");
    if unmanaged {
        return None;
    }
    let name = str_at(metadata, "name")?;
    let topic = resource
        .get("spec")
        .and_then(|s| str_at(s, "topicName"))
        .filter(|t| !t.is_empty())
        .unwrap_or_else(|| name.clone());
    let reference = match str_at(metadata, "namespace").filter(|n| !n.is_empty()) {
        Some(ns) => format!("{ns}/{name}"),
        None => name,
    };
    Some((topic, reference))
}

/// The owner a plan's declaration records: basis `declared`.
#[must_use]
pub fn declared_owner(owner: &DeclaredOwner) -> TopicOwner {
    TopicOwner {
        kind: owner.kind.clone(),
        basis: OWNER_BASES[1].to_string(),
        reference: owner.reference.clone(),
    }
}

/// **The run's owners**: the owners detected from `KafkaTopic` resources, with
/// every declaration of the plan laid OVER them — a declaration is what the
/// approved plan says, so a resource for the same topic does not replace it.
#[must_use]
pub fn merge_owners(
    detected: BTreeMap<String, TopicOwner>,
    declared: &[DeclaredOwner],
) -> BTreeMap<String, TopicOwner> {
    let mut owners = detected;
    for owner in declared {
        owners.insert(owner.topic.clone(), declared_owner(owner));
    }
    owners
}

/// How a topic's configuration reaches a target: through Kafka's admin API,
/// or — for a topic a declarative owner manages, which would revert a change
/// made around it — as desired state exported for that owner to apply.
#[must_use]
pub const fn apply_route(owned: bool) -> &'static str {
    if owned {
        "desiredStateExport"
    } else {
        "adminApi"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_table_is_in_key_order_with_no_duplicate() {
        let keys: Vec<&str> = TABLE.iter().map(|r| r.key).collect();
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(keys, sorted);
    }

    /// The measured counts: 36 keys on 3.9 and 33 on 4.x, the difference
    /// being exactly Kafka 4.0's three removals.
    #[test]
    fn the_table_defines_36_keys_on_3_9_and_33_on_4_x() {
        let on = |line: &str| TABLE.iter().filter(|r| defined_on(r.key, line)).count();
        assert_eq!(on("3.9"), 36);
        assert_eq!(on("4.x"), 33);
        let removed: Vec<&str> = TABLE
            .iter()
            .filter(|r| defined_on(r.key, "3.9") && !defined_on(r.key, "4.x"))
            .map(|r| r.key)
            .collect();
        assert_eq!(
            removed,
            [
                "message.downconversion.enable",
                "message.format.version",
                "message.timestamp.difference.max.ms"
            ]
        );
        assert!(!defined_on("confluent.placement.constraints", "3.9"));
        assert!(!defined_on("retention.ms", "3.7"));
    }

    /// A class and its measured verdict agree: a key defined on a line is
    /// accepted there unless it needs remote storage the probe broker lacks.
    #[test]
    fn every_rules_measured_verdict_follows_its_class() {
        for r in &TABLE {
            for (i, line) in KAFKA_LINES.iter().enumerate() {
                let expect = defined_on(r.key, line)
                    && !(r.class == KeyClass::RequiresTieredStorage
                        && r.key == "remote.storage.enable");
                assert_eq!(r.accepted[i], expect, "{} on {line}", r.key);
            }
        }
    }

    #[test]
    fn classify_puts_secret_first_then_inherited_then_the_table() {
        assert_eq!(
            classify("retention.ms", TOPIC_OVERRIDE_SOURCE, true),
            SECRET
        );
        assert_eq!(classify("retention.ms", "defaultConfig", true), SECRET);
        assert_eq!(classify("retention.ms", "defaultConfig", false), INHERITED);
        assert_eq!(
            classify("retention.ms", "staticBrokerConfig", false),
            INHERITED
        );
        assert_eq!(classify("retention.ms", "unknown", false), INHERITED);
        assert_eq!(
            classify("retention.ms", TOPIC_OVERRIDE_SOURCE, false),
            PORTABLE
        );
        assert_eq!(
            classify("message.format.version", TOPIC_OVERRIDE_SOURCE, false),
            REMOVED_IN_KAFKA_4
        );
        assert_eq!(
            classify(
                "leader.replication.throttled.replicas",
                TOPIC_OVERRIDE_SOURCE,
                false
            ),
            CLUSTER_BOUND
        );
        assert_eq!(
            classify("remote.storage.enable", TOPIC_OVERRIDE_SOURCE, false),
            REQUIRES_TIERED_STORAGE
        );
        assert_eq!(
            classify(
                "confluent.placement.constraints",
                TOPIC_OVERRIDE_SOURCE,
                false
            ),
            PROVIDER_ONLY
        );
        for r in &TABLE {
            assert!(PORTABILITY_CLASSES.contains(&r.class.wire_name()));
        }
    }

    #[test]
    fn the_capture_rule_keeps_overrides_and_semantic_inherited_values_only() {
        assert!(records("segment.bytes", TOPIC_OVERRIDE_SOURCE));
        assert!(records("confluent.anything", TOPIC_OVERRIDE_SOURCE));
        assert!(records("retention.ms", "defaultConfig"));
        assert!(records("min.insync.replicas", "dynamicDefaultBrokerConfig"));
        assert!(!records("segment.bytes", "staticBrokerConfig"));
        assert!(!records("confluent.anything", "defaultConfig"));
        let semantic: Vec<&str> = TABLE.iter().filter(|r| r.semantic).map(|r| r.key).collect();
        assert_eq!(
            semantic,
            [
                "cleanup.policy",
                "delete.retention.ms",
                "max.compaction.lag.ms",
                "max.message.bytes",
                "message.timestamp.after.max.ms",
                "message.timestamp.before.max.ms",
                "message.timestamp.difference.max.ms",
                "message.timestamp.type",
                "min.compaction.lag.ms",
                "min.insync.replicas",
                "remote.storage.enable",
                "retention.bytes",
                "retention.ms",
                "unclean.leader.election.enable",
            ]
        );
    }

    #[test]
    fn a_secret_is_recorded_by_key_and_never_by_value() {
        let e = entry_of(
            "vendor.secret",
            Some("hunter2"),
            TOPIC_OVERRIDE_SOURCE,
            true,
        )
        .expect("a sensitive override is recorded");
        assert_eq!(e.value, None);
        assert_eq!(e.portability, SECRET);
        assert!(!format!("{e:?}").contains("hunter2"));
        // Not an override and not semantic: not recorded at all.
        assert_eq!(
            entry_of("vendor.secret", Some("x"), "defaultConfig", true),
            None
        );
        // No value observed for a non-sensitive entry: nothing to record.
        assert_eq!(entry_of("retention.ms", None, "defaultConfig", false), None);
        let e = entry_of("retention.ms", Some("1000"), "defaultConfig", false).unwrap();
        assert_eq!(
            (e.value.as_deref(), e.portability.as_str()),
            (Some("1000"), INHERITED)
        );
    }

    fn yaml(text: &str) -> Vec<serde_yaml::Value> {
        serde_yaml::Deserializer::from_str(text)
            .map(|d| serde::Deserialize::deserialize(d).unwrap())
            .collect()
    }

    #[test]
    fn a_labelled_kafka_topic_owns_its_topic_and_nothing_else_does() {
        let docs = yaml(
            "apiVersion: kafka.strimzi.io/v1beta2\nkind: KafkaTopic\nmetadata:\n  name: orders-kt\n  namespace: kafka\n  labels:\n    strimzi.io/cluster: prod\nspec:\n  topicName: orders\n---\n\
             apiVersion: kafka.strimzi.io/v1beta2\nkind: KafkaTopic\nmetadata:\n  name: payments\n  namespace: kafka\nspec: {}\n---\n\
             apiVersion: kafka.strimzi.io/v1beta2\nkind: KafkaTopic\nmetadata:\n  name: audit\n  namespace: kafka\n  labels:\n    strimzi.io/cluster: prod\n  annotations:\n    strimzi.io/managed: \"false\"\n---\n\
             apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: refunds\n  labels:\n    strimzi.io/cluster: prod\n---\n\
             apiVersion: kafka.strimzi.io/v1beta2\nkind: KafkaTopic\nmetadata:\n  name: other\n  labels:\n    strimzi.io/cluster: prod\n",
        );
        let topics: Vec<String> = ["orders", "payments", "audit", "refunds", "clicks"]
            .iter()
            .map(|t| (*t).to_string())
            .collect();
        let owners = strimzi_owners(&docs, &topics, None);
        assert_eq!(owners.len(), 1, "{owners:?}");
        let o = &owners["orders"];
        assert_eq!(
            (o.kind.as_str(), o.basis.as_str(), o.reference.as_str()),
            ("strimzi", "kafkaTopicResource", "kafka/orders-kt")
        );
        // The cluster filter: a label naming another cluster owns nothing.
        assert!(strimzi_owners(&docs, &topics, Some("staging")).is_empty());
        assert_eq!(strimzi_owners(&docs, &topics, Some("prod")).len(), 1);
    }

    #[test]
    fn a_list_is_read_item_by_item_and_the_first_reference_wins_a_conflict() {
        let docs = yaml(
            "apiVersion: v1\nkind: List\nitems:\n- apiVersion: kafka.strimzi.io/v1\n  kind: KafkaTopic\n  metadata:\n    name: z-orders\n    namespace: b\n    labels: {strimzi.io/cluster: c}\n  spec: {topicName: orders}\n- apiVersion: kafka.strimzi.io/v1\n  kind: KafkaTopic\n  metadata:\n    name: orders\n    namespace: a\n    labels: {strimzi.io/cluster: c}\n",
        );
        let owners = strimzi_owners(&docs, &["orders".to_string()], None);
        assert_eq!(owners["orders"].reference, "a/orders");
    }

    #[test]
    fn a_declared_owner_must_name_a_planned_topic_a_known_kind_and_a_reference() {
        let named = vec!["orders".to_string()];
        let ok = DeclaredOwner {
            topic: "orders".into(),
            kind: "external".into(),
            reference: "terraform: kafka_topic.orders".into(),
        };
        assert_eq!(refuse_declared(&ok, &named), None);
        assert_eq!(declared_owner(&ok).basis, "declared");
        let mut bad = ok.clone();
        bad.topic = "clicks".into();
        assert!(refuse_declared(&bad, &named)
            .unwrap()
            .contains("not one of source.topics"));
        let mut bad = ok.clone();
        bad.kind = "terraform".into();
        assert!(refuse_declared(&bad, &named).unwrap().contains("kind"));
        for reference in ["", "   ", "a\nb"] {
            let mut bad = ok.clone();
            bad.reference = reference.into();
            assert!(refuse_declared(&bad, &named).is_some(), "{reference:?}");
        }
        let mut bad = ok.clone();
        bad.reference = "x".repeat(MAX_OWNER_REFERENCE_CHARS + 1);
        assert!(refuse_declared(&bad, &named).is_some());
        assert_eq!(apply_route(true), "desiredStateExport");
        // A declaration is laid over a detected resource for the same topic.
        let detected = BTreeMap::from([(
            "orders".to_string(),
            TopicOwner {
                kind: "strimzi".into(),
                basis: "kafkaTopicResource".into(),
                reference: "kafka/orders".into(),
            },
        )]);
        let merged = merge_owners(detected, &[ok.clone()]);
        assert_eq!(merged["orders"].basis, "declared");
        assert_eq!(merged["orders"].reference, ok.reference);
        assert_eq!(apply_route(false), "adminApi");
    }
}
