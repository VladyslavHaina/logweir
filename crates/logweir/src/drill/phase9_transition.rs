//! Phase 9's configuration transition (PROD-05.2, design §4.4).
//!
//! After the signed verdict, and only when it is a pass, a restore whose plan
//! asked for `target.topic_configuration` with `transition:
//! afterVerification` applies each kept topic's deferred settings
//! (compaction, retention, the timestamp bounds and type). Per `adminApi`
//! topic:
//!
//! 1. read its dynamic overrides; when they are not EXACTLY what phase 0
//!    created it with, leave it alone (`ConfigChangedSinceCreation`);
//! 2. one legacy `AlterConfigs` carrying the topic's WHOLE desired override
//!    set, which replaces every override, Logweir's during-restore keys
//!    included;
//! 3. read the overrides back until they are that set: rust-rdkafka 0.36.2
//!    answers `Ok` for every resource of an `AlterConfigs` and never reads
//!    its per-resource error, so the read-back is the only proof.
//!
//! An owned topic is never altered: its configuration is the desired-state
//! document phase 0 wrote. No other topic is touched, and nothing is retried:
//! a topic that did not transition is named with its whole desired set, the
//! remaining step, which the operator applies.

use logweir_core::topic_transition::{
    not_run_reason, step_for, TopicStep, TransitionFailure, TransitionOutcome, TransitionSkip,
    ROUTE_ADMIN_API, ROUTE_DESIRED_STATE_EXPORT,
};
use logweir_kafka::reader::{
    settle, ClusterReader, ConfigSourceKind, KafkaError, Settling, TopicCreator,
};
use std::collections::BTreeMap;
use std::time::Duration;

use super::phase0_admit::TopicConfigurationAdmission;

/// How long phase 9 waits for an altered topic's overrides to read back.
pub const READBACK_WITHIN: Duration = Duration::from_secs(15);

/// The longest error a failure carries (`Restore.status`'s bound).
const ERROR_MAX_CHARS: usize = 256;

/// The transition, attested. `passed` is the signed verdict; `kept` whether
/// the run keeps its topics (a `newTopic` restore, or a scratch one whose
/// teardown keeps them); `desired_state_key` the document phase 0 wrote.
#[must_use]
pub fn run(
    admission: &TopicConfigurationAdmission,
    passed: bool,
    kept: bool,
    desired_state_key: Option<&str>,
    reader: &dyn ClusterReader,
    alterer: &dyn TopicCreator,
    readback_within: Duration,
) -> TransitionOutcome {
    let mut outcome = TransitionOutcome {
        transition: admission.transition.wire_name().to_string(),
        exported: admission
            .topics
            .iter()
            .filter(|(_, t)| t.route == ROUTE_DESIRED_STATE_EXPORT)
            .map(|(name, _)| name.clone())
            .collect(),
        desired_state_key: desired_state_key.map(str::to_string),
        ..TransitionOutcome::default()
    };
    if let Some(reason) = not_run_reason(admission.transition, passed, kept) {
        outcome.not_run = Some(reason.to_string());
        return outcome;
    }
    let names: Vec<String> = admission
        .topics
        .iter()
        .filter(|(_, t)| t.route == ROUTE_ADMIN_API)
        .map(|(name, _)| name.clone())
        .collect();
    let mut now = read_overrides(reader, &names);
    for name in &names {
        let topic = &admission.topics[name];
        let desired = topic.final_overrides();
        let fail = |error: String| TransitionFailure {
            topic: name.clone(),
            error: bounded(&error),
            desired: desired.clone(),
        };
        let current = match now.remove(name) {
            Some(Ok(current)) => current,
            Some(Err(e)) => {
                outcome.failed.push(fail(format!(
                    "the topic's overrides could not be read before the transition: {e}"
                )));
                continue;
            }
            None => {
                outcome.failed.push(fail(
                    "the topic's overrides were not in the configuration read's answer".to_string(),
                ));
                continue;
            }
        };
        match step_for(topic, &current) {
            TopicStep::Export => {}
            TopicStep::Skip(reason) => outcome.skipped.push(TransitionSkip {
                topic: name.clone(),
                reason,
                desired: desired.clone(),
            }),
            TopicStep::Alter(set) => match alterer.alter_topic_configs(name, &set) {
                Err(e) => outcome
                    .failed
                    .push(fail(format!("AlterConfigs failed: {e}"))),
                Ok(()) => match read_back(reader, name, &set, readback_within) {
                    Ok(()) => outcome.applied.push(name.clone()),
                    Err(e) => outcome.failed.push(fail(e)),
                },
            },
        }
    }
    outcome
}

/// Each topic's dynamic (topic-level) overrides, from one DescribeConfigs.
fn read_overrides(
    reader: &dyn ClusterReader,
    names: &[String],
) -> BTreeMap<String, Result<BTreeMap<String, String>, String>> {
    if names.is_empty() {
        return BTreeMap::new();
    }
    match reader.describe_topic_configs(names) {
        Ok(answers) => answers
            .into_iter()
            .map(|(topic, read)| {
                let overrides = read
                    .map(|entries| {
                        entries
                            .into_iter()
                            .filter(|e| e.source == ConfigSourceKind::DynamicTopicConfig)
                            .map(|e| (e.name, e.value.unwrap_or_default()))
                            .collect()
                    })
                    .map_err(|e| e.to_string());
                (topic, overrides)
            })
            .collect(),
        Err(e) => names
            .iter()
            .map(|n| (n.clone(), Err(e.to_string())))
            .collect(),
    }
}

/// Wait, at most `within`, until `topic`'s overrides read back as `set`.
fn read_back(
    reader: &dyn ClusterReader,
    topic: &str,
    set: &BTreeMap<String, String>,
    within: Duration,
) -> Result<(), String> {
    let mut last: Option<BTreeMap<String, String>> = None;
    let answer = settle(within, || {
        match read_overrides(reader, &[topic.to_string()]).remove(topic) {
            Some(Ok(now)) if &now == set => Settling::Done(()),
            Some(Ok(now)) => {
                last = Some(now);
                Settling::NotYet(KafkaError::Client(
                    "the overrides have not read back as the desired set".to_string(),
                ))
            }
            Some(Err(e)) => Settling::Failed(KafkaError::Client(e)),
            None => Settling::Failed(KafkaError::Client(
                "the topic was not in the configuration read's answer".to_string(),
            )),
        }
    });
    match answer {
        Ok(()) => Ok(()),
        Err(e) => Err(match last {
            Some(now) => format!(
                "AlterConfigs answered, and within {}s the topic's overrides read back as {} \
                 rather than the desired set: the broker refused it (an authorizer's \
                 ALTER_CONFIGS denial is not reported per topic by this client) or did not apply \
                 it",
                within.as_secs(),
                shown(&now)
            ),
            None => format!("the overrides could not be read back after AlterConfigs: {e}"),
        }),
    }
}

fn shown(overrides: &BTreeMap<String, String>) -> String {
    let keys: Vec<&str> = overrides.keys().map(String::as_str).collect();
    format!("{} override(s) [{}]", keys.len(), keys.join(", "))
}

fn bounded(error: &str) -> String {
    let one = crate::exit::one_line(error);
    if one.chars().count() <= ERROR_MAX_CHARS {
        return one;
    }
    let mut cut: String = one.chars().take(ERROR_MAX_CHARS - 1).collect();
    cut.push('…');
    cut
}

#[cfg(test)]
mod tests {
    use super::*;
    use logweir_core::topic_transition::{
        ReplicationChoice, TargetLine, TopicDesired, Transition, CONFIG_CHANGED_SINCE_CREATION,
    };
    use logweir_kafka::reader::{
        ConfigEntryObservation, ConsumedRecord, NewTopicSpec, TopicConfigRead, TopicMeta,
    };
    use std::sync::Mutex;

    fn topic(route: &str) -> TopicDesired {
        TopicDesired {
            source_topic: "orders".into(),
            partitions: Some(1),
            replication_factor: ReplicationChoice {
                source: Some(1),
                target: 1,
                reason: "sourceFactor".into(),
            },
            during_restore: BTreeMap::from([
                ("cleanup.policy".to_string(), "delete".to_string()),
                ("retention.ms".to_string(), "-1".to_string()),
            ]),
            at_creation: BTreeMap::from([("segment.ms".to_string(), "1000".to_string())]),
            deferred: BTreeMap::from([("cleanup.policy".to_string(), "compact".to_string())]),
            not_applied: Vec::new(),
            capped: BTreeMap::new(),
            route: route.into(),
            owner: None,
            not_recorded: false,
        }
    }

    /// A cluster of topic overrides that applies an `AlterConfigs` to the
    /// topics it allows and silently ignores the rest — as an authorizer's
    /// per-resource denial reads through rust-rdkafka 0.36.2.
    struct Cluster {
        overrides: Mutex<BTreeMap<String, BTreeMap<String, String>>>,
        deny: Vec<String>,
        altered: Mutex<Vec<String>>,
    }

    impl ClusterReader for Cluster {
        fn cluster_id(&self) -> Result<String, KafkaError> {
            Ok("c".into())
        }
        fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
            Ok(Vec::new())
        }
        fn end_offsets(&self, _: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
            Ok(Vec::new())
        }
        fn topic_configs(&self, _: &str) -> Result<BTreeMap<String, String>, KafkaError> {
            Ok(BTreeMap::new())
        }
        fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
            Ok(BTreeMap::new())
        }
        fn describe_topic_configs(
            &self,
            topics: &[String],
        ) -> Result<Vec<(String, TopicConfigRead)>, KafkaError> {
            let all = self.overrides.lock().unwrap();
            Ok(topics
                .iter()
                .map(|t| {
                    let entries = all
                        .get(t)
                        .cloned()
                        .unwrap_or_default()
                        .into_iter()
                        .map(|(name, value)| ConfigEntryObservation {
                            name,
                            value: Some(value),
                            source: ConfigSourceKind::DynamicTopicConfig,
                            read_only: false,
                            sensitive: false,
                        })
                        .chain(std::iter::once(ConfigEntryObservation {
                            name: "segment.bytes".into(),
                            value: Some("1073741824".into()),
                            source: ConfigSourceKind::DefaultConfig,
                            read_only: false,
                            sensitive: false,
                        }))
                        .collect();
                    (t.clone(), Ok(entries))
                })
                .collect())
        }
        fn consume_range(
            &self,
            _: &str,
            _: i32,
            _: i64,
            _: usize,
        ) -> Result<Vec<ConsumedRecord>, KafkaError> {
            Ok(Vec::new())
        }
    }

    impl TopicCreator for Cluster {
        fn create_topics(
            &self,
            _: &[NewTopicSpec],
        ) -> Result<Vec<(String, Result<(), String>)>, KafkaError> {
            Ok(Vec::new())
        }
        fn alter_topic_configs(
            &self,
            topic: &str,
            configs: &BTreeMap<String, String>,
        ) -> Result<(), KafkaError> {
            self.altered.lock().unwrap().push(topic.to_string());
            if !self.deny.iter().any(|d| d == topic) {
                self.overrides
                    .lock()
                    .unwrap()
                    .insert(topic.to_string(), configs.clone());
            }
            Ok(())
        }
    }

    fn admission(topics: &[(&str, &str)]) -> TopicConfigurationAdmission {
        TopicConfigurationAdmission {
            transition: Transition::AfterVerification,
            line: TargetLine::Kafka4,
            topics: topics
                .iter()
                .map(|(name, route)| ((*name).to_string(), topic(route)))
                .collect(),
        }
    }

    fn cluster(names: &[&str], deny: &[&str]) -> Cluster {
        let created = topic(ROUTE_ADMIN_API).created_overrides();
        Cluster {
            overrides: Mutex::new(
                names
                    .iter()
                    .map(|n| ((*n).to_string(), created.clone()))
                    .collect(),
            ),
            deny: deny.iter().map(|d| (*d).to_string()).collect(),
            altered: Mutex::new(Vec::new()),
        }
    }

    #[test]
    fn a_pass_applies_the_whole_set_and_proves_it_by_reading_it_back() {
        let a = admission(&[("r-a", ROUTE_ADMIN_API), ("r-b", ROUTE_ADMIN_API)]);
        let c = cluster(&["r-a", "r-b", "unrelated"], &["r-b"]);
        let unrelated_before = c.overrides.lock().unwrap()["unrelated"].clone();
        let out = run(&a, true, true, None, &c, &c, Duration::from_millis(200));
        assert_eq!(out.applied, vec!["r-a".to_string()]);
        // NEGATIVE CONTROL: the broker ignored r-b's request; without the
        // read-back it would be reported applied.
        assert_eq!(out.failed.len(), 1);
        assert_eq!(out.failed[0].topic, "r-b");
        assert_eq!(out.failed[0].desired["cleanup.policy"], "compact");
        assert!(
            out.failed[0].error.contains("read back"),
            "{}",
            out.failed[0].error
        );
        let after = c.overrides.lock().unwrap().clone();
        assert_eq!(after["r-a"]["cleanup.policy"], "compact");
        assert!(!after["r-a"].contains_key("retention.ms"));
        assert_eq!(after["unrelated"], unrelated_before);
        assert!(!c.altered.lock().unwrap().contains(&"unrelated".to_string()));
    }

    #[test]
    fn a_changed_topic_is_skipped_and_an_owned_one_exported() {
        let a = admission(&[
            ("r-a", ROUTE_ADMIN_API),
            ("r-owned", ROUTE_DESIRED_STATE_EXPORT),
        ]);
        let c = cluster(&["r-a", "r-owned"], &[]);
        c.overrides
            .lock()
            .unwrap()
            .get_mut("r-a")
            .unwrap()
            .insert("retention.ms".into(), "1000".into());
        let out = run(
            &a,
            true,
            true,
            Some("k"),
            &c,
            &c,
            Duration::from_millis(100),
        );
        assert!(out.applied.is_empty());
        assert_eq!(out.skipped[0].reason, CONFIG_CHANGED_SINCE_CREATION);
        assert_eq!(out.exported, vec!["r-owned".to_string()]);
        assert!(c.altered.lock().unwrap().is_empty(), "nothing is altered");
    }

    #[test]
    fn nothing_runs_without_a_pass() {
        let a = admission(&[("r-a", ROUTE_ADMIN_API)]);
        let c = cluster(&["r-a"], &[]);
        let out = run(&a, false, true, None, &c, &c, Duration::from_millis(100));
        assert_eq!(out.not_run.as_deref(), Some("verdictNotPass"));
        assert!(c.altered.lock().unwrap().is_empty());
    }
}
