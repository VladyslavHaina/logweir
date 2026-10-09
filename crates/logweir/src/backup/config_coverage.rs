//! **FX-4: per-topic topic-configuration capture coverage**, recorded in the
//! backup receipt (format 1.1.0) and projected into the catalog point.
//!
//! # The defect this closes
//!
//! The engine captures topic configuration NON-FATALLY:
//! `crates/logweir-engine-oso/src/render_backup.rs::render` writes no
//! `capture_topic_configs` or `require_topic_configs` key, so the engine's
//! defaults apply — capture on, required off
//! (`U:crates/kafka-backup-core/src/config.rs:524-533`, `:639-640`) — and a
//! failed capture is one `warn!` and nothing else (`backup/engine.rs:382-393`).
//! The capture is also ALL-OR-NOTHING across the run's topics: the engine's
//! DescribeConfigs returns `Err` on the first resource that carries an error
//! code (`kafka/admin.rs:476-487`), so one denied topic leaves EVERY topic's
//! `configurations` empty. And the manifest spells "captured, no overrides"
//! and "not captured" identically, as `configurations: {}`. A later parity
//! check therefore compared a restored topic against an empty record and
//! reported no divergence.
//!
//! # Where coverage comes from, and why not from the engine
//!
//! The engine's own capture result is not observable per topic — a log line
//! naming at most the FIRST failing resource, from a subprocess whose output is
//! not a contract — and it keeps explicit overrides only (`config_source == 1`,
//! `kafka/admin.rs:90-92`), so it can never say what a topic's EFFECTIVE
//! `message.timestamp.type` was when it came from the broker (FX-8's
//! broker-default arm). So `logweir backup run` reads each topic's
//! configuration ITSELF, through the same principal the engine uses,
//! IMMEDIATELY before the engine starts ([`observe`]), and after the engine
//! compares what the engine WOULD have kept (its own filter,
//! `logweir_engine_oso::vendored::topic_config`) with what the manifest DOES
//! hold ([`classify`]). Coverage is the outcome of that comparison, not of
//! either read alone.
//!
//! # rdkafka 0.36.2 hides a refused read (T13), and this module does not
//!
//! `logweir_kafka::reader::ClusterReader::describe_topic_configs` never
//! returns an empty `Ok` for a refused topic: an empty DescribeConfigs answer is
//! turned into the refusal it stands for (`NotAuthorized` for a topic the same
//! principal can see — see `logweir_kafka::reader::empty_topic_config_answer`
//! for the broker source). That `NotAuthorized` is what [`Observation::Denied`]
//! and `captureDenied` are made of.
//!
//! # Nothing here fails the backup
//!
//! Coverage is a RECORDED FACT, not a refusal: a denied read is written down
//! as `captureDenied`, and the backup proceeds exactly as it did before. Every
//! per-topic outcome — including "this reader cannot answer at all" — is a
//! value, so [`observe`] has no error path.

use logweir_core::backup_receipt::{
    ConfigEntry, EffectiveConfigValue, TopicConfigCoverage, TopicConfiguration, TopicOwner,
    NOT_CAPTURED_REASONS, TIMESTAMP_TYPES,
};
use logweir_kafka::reader::{
    ClusterReader, ConfigEntryObservation, ConfigSourceKind, KafkaError, TopicConfigRead,
};
use std::collections::BTreeMap;

/// The one configuration key whose EFFECTIVE value the receipt records (FX-8).
pub const TIMESTAMP_TYPE_KEY: &str = "message.timestamp.type";

/// What Logweir's own DescribeConfigs read established about ONE topic,
/// before the engine ran.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Observation {
    /// The read succeeded (the broker answered with entries).
    Read {
        /// What the engine WOULD capture from this answer: its own filter
        /// (`engine_captures`) over every entry.
        engine_view: BTreeMap<String, String>,
        /// The effective `message.timestamp.type` and its source, when the
        /// broker reported one of the two values the receipt defines.
        timestamp_type: Option<EffectiveConfigValue>,
        /// **PROD-05.1.** The entries the configuration MODEL records, by key
        /// — `logweir_core::topic_configuration::entry_of` over every entry
        /// of the answer: each override and each semantic key's effective
        /// value, classified, and a sensitive entry by key only. Computed
        /// here, at the read, so a sensitive value never outlives it.
        model: BTreeMap<String, ConfigEntry>,
    },
    /// The broker's authorizer refused the read — directly, or by T13's rule
    /// for an empty answer on a visible topic. The detail is the reader's.
    Denied(String),
    /// Any other failure: no broker answered, the topic is unknown, a reader
    /// that cannot answer, an answer that named no such topic.
    Failed(String),
}

/// Read every named topic's configuration, ONCE, as one request.
///
/// Returns exactly one [`Observation`] per name in `topics` — a topic the
/// reader's answer does not mention is `Failed`, never absent — which is what
/// lets [`classify`]'s output satisfy the receipt's arm 7 by construction.
#[must_use]
pub fn observe(reader: &dyn ClusterReader, topics: &[String]) -> BTreeMap<String, Observation> {
    let answers: BTreeMap<String, TopicConfigRead> = match reader.describe_topic_configs(topics) {
        Ok(answers) => answers.into_iter().collect(),
        Err(call) => {
            let why = format!("the DescribeConfigs call failed: {call}");
            return topics
                .iter()
                .map(|t| (t.clone(), Observation::Failed(why.clone())))
                .collect();
        }
    };
    topics
        .iter()
        .map(|topic| {
            let observed = match answers.get(topic) {
                Some(Ok(entries)) => read_of(entries),
                Some(Err(KafkaError::NotAuthorized(detail))) => {
                    Observation::Denied(format!("not authorized: {detail}"))
                }
                Some(Err(other)) => Observation::Failed(other.to_string()),
                None => Observation::Failed(format!(
                    "the DescribeConfigs answer named no result for topic {topic}"
                )),
            };
            (topic.clone(), observed)
        })
        .collect()
}

/// One successful answer, reduced to the two facts coverage needs.
fn read_of(entries: &[ConfigEntryObservation]) -> Observation {
    let engine_view = entries
        .iter()
        .filter(|e| {
            logweir_engine_oso::vendored::topic_config::engine_captures(
                &e.name,
                e.value.is_some(),
                e.source == ConfigSourceKind::DynamicTopicConfig,
                e.read_only,
                e.sensitive,
            )
        })
        .filter_map(|e| e.value.clone().map(|v| (e.name.clone(), v)))
        .collect();
    // ONLY a value the receipt defines. A broker that reported something else
    // leaves the type NOT RECORDED rather than writing a value arm 11 refuses
    // — which would make this run refuse its own receipt after the archive
    // already exists.
    let timestamp_type = entries
        .iter()
        .find(|e| e.name == TIMESTAMP_TYPE_KEY)
        .and_then(|e| e.value.as_deref().map(|v| (v, e.source)))
        .filter(|(v, _)| TIMESTAMP_TYPES.contains(v))
        .map(|(v, source)| EffectiveConfigValue {
            value: v.to_string(),
            source: source.wire_name().to_string(),
        });
    let model = entries
        .iter()
        .filter_map(|e| {
            logweir_core::topic_configuration::entry_of(
                &e.name,
                e.value.as_deref(),
                e.source.wire_name(),
                e.sensitive,
            )
            .map(|recorded| (e.name.clone(), recorded))
        })
        .collect();
    Observation::Read {
        engine_view,
        timestamp_type,
        model,
    }
}

/// Coverage per named topic, from the observation and the manifest the engine
/// wrote. `manifest` holds each named topic the manifest mentions, with its
/// `configurations` (an empty map when the manifest names the topic with none).
///
/// | observation | manifest | coverage |
/// |---|---|---|
/// | read | equals the engine view | `captured` |
/// | read | anything else, or no entry | `notCaptured`, `manifestDiffers` |
/// | denied | — | `captureDenied` |
/// | failed | — | `notCaptured`, `describeFailed` |
///
/// The effective timestamp type is carried from a successful read in both of
/// its rows: it is Logweir's own observation and does not depend on the
/// engine's.
#[must_use]
pub fn classify(
    observations: &BTreeMap<String, Observation>,
    manifest: &BTreeMap<String, BTreeMap<String, String>>,
) -> BTreeMap<String, TopicConfigCoverage> {
    observations
        .iter()
        .map(|(topic, observed)| {
            let coverage = match observed {
                Observation::Read {
                    engine_view,
                    timestamp_type,
                    ..
                } => {
                    let complete = manifest.get(topic) == Some(engine_view);
                    TopicConfigCoverage {
                        coverage: if complete { "captured" } else { "notCaptured" }.to_string(),
                        reason: (!complete).then(|| NOT_CAPTURED_REASONS[1].to_string()),
                        timestamp_type: timestamp_type.clone(),
                    }
                }
                Observation::Denied(_) => TopicConfigCoverage {
                    coverage: "captureDenied".to_string(),
                    reason: None,
                    timestamp_type: None,
                },
                Observation::Failed(_) => TopicConfigCoverage {
                    coverage: "notCaptured".to_string(),
                    reason: Some(NOT_CAPTURED_REASONS[0].to_string()),
                    timestamp_type: None,
                },
            };
            (topic.clone(), coverage)
        })
        .collect()
}

/// The archive's record of one topic's layout: the manifest's
/// `original_partition_count` and `source_replication_factor`.
pub type Layout = (Option<i32>, Option<i16>);

/// **PROD-05.1: the receipt's `topic_configuration`**, one entry per observed
/// topic — which [`observe`] makes exactly the named set, so arm 14 holds by
/// construction.
///
/// - `entries`: the read's model when the read SUCCEEDED, and absent when it
///   was denied or failed — exactly arm 15's rule, because [`classify`] calls
///   a successful read `captured` or `notCaptured`/`manifestDiffers` and
///   nothing else.
/// - `partitions`: the manifest's `original_partition_count` (`layouts`, the
///   topics the manifest mentions) — the count the restore creates the topic
///   with (`drill::phase3_diff::restore_partition_count`).
/// - `replication_factor`: Logweir's OWN metadata read before the engine
///   (`factors`, `ClusterReader::replication_factors`), and the manifest's
///   `source_replication_factor` only where that read named none. Not the
///   manifest first, because engine 0.23.3 records the factor reliably only
///   for the FIRST topic it saves: its `merge_manifests` carries
///   `original_partition_count` from each later save and drops
///   `source_replication_factor` (engine 0.23.3 `backup/engine.rs:1683-1705`;
///   measured on compose, PROD-05.1 report). Logweir's build from
///   `0.23.3+logweir.2` records every topic's (patch 0002, FX-21), but OSO's
///   release, the one-release rollback, still does not.
/// - A count that is absent, or `0` or less, is NOT RECORDED rather than
///   written as a value arm 19 refuses — a run must not refuse its own receipt
///   after the archive exists.
/// - `owner`: from `owners`, whatever the read said: who manages a topic does
///   not depend on whether its configuration could be read.
#[must_use]
pub fn model(
    observations: &BTreeMap<String, Observation>,
    layouts: &BTreeMap<String, Layout>,
    factors: &BTreeMap<String, u32>,
    owners: &BTreeMap<String, TopicOwner>,
) -> BTreeMap<String, TopicConfiguration> {
    let count = |n: Option<i64>| n.filter(|n| *n >= 1).and_then(|n| u32::try_from(n).ok());
    observations
        .iter()
        .map(|(topic, observed)| {
            let (partitions, manifest_factor) = layouts.get(topic).copied().unwrap_or((None, None));
            let factor = factors
                .get(topic)
                .map(|n| i64::from(*n))
                .or(manifest_factor.map(i64::from));
            let entries = match observed {
                Observation::Read { model, .. } => Some(model.clone()),
                Observation::Denied(_) | Observation::Failed(_) => None,
            };
            (
                topic.clone(),
                TopicConfiguration {
                    partitions: count(partitions.map(i64::from)),
                    replication_factor: count(factor),
                    entries,
                    owner: owners.get(topic).cloned(),
                },
            )
        })
        .collect()
}

/// One line per topic for the run's log, so an operator reading `backup run`'s
/// output learns a denied read without opening the receipt.
pub fn log(observations: &BTreeMap<String, Observation>) {
    for (topic, observed) in observations {
        match observed {
            Observation::Read {
                engine_view,
                timestamp_type,
                model,
            } => tracing::info!(
                topic = %topic,
                overrides = engine_view.len(),
                // Counts only: a configuration VALUE is the adopter's data, and
                // a secret's is never held at all.
                recorded = model.len(),
                unportable = model
                    .values()
                    .filter(|e| !matches!(
                        e.portability.as_str(),
                        logweir_core::topic_configuration::PORTABLE
                            | logweir_core::topic_configuration::INHERITED
                    ))
                    .count(),
                timestamp_type = ?timestamp_type,
                "topic configuration read before the engine"
            ),
            Observation::Denied(detail) => tracing::warn!(
                topic = %topic,
                detail = %detail,
                "topic configuration read DENIED: its configuration coverage is captureDenied, \
                 and a configuration parity check over this backup will say `not assessed`. \
                 Grant DescribeConfigs on the topic to the backup principal"
            ),
            Observation::Failed(detail) => tracing::warn!(
                topic = %topic,
                detail = %detail,
                "topic configuration could not be read: its configuration coverage is \
                 notCaptured (describeFailed)"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logweir_kafka::reader::{ConsumedRecord, TopicMeta};

    fn entry(name: &str, value: &str, source: ConfigSourceKind) -> ConfigEntryObservation {
        ConfigEntryObservation {
            name: name.to_string(),
            value: Some(value.to_string()),
            source,
            read_only: false,
            sensitive: false,
        }
    }

    /// A reader whose DescribeConfigs answer per topic is scripted.
    struct Scripted(Result<Vec<(String, TopicConfigRead)>, KafkaError>);

    impl ClusterReader for Scripted {
        fn cluster_id(&self) -> Result<String, KafkaError> {
            unimplemented!()
        }
        fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
            unimplemented!()
        }
        fn end_offsets(&self, _t: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
            unimplemented!()
        }
        fn topic_configs(&self, _t: &str) -> Result<BTreeMap<String, String>, KafkaError> {
            unimplemented!()
        }
        fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
            unimplemented!()
        }
        fn consume_range(
            &self,
            _t: &str,
            _p: i32,
            _f: i64,
            _m: usize,
        ) -> Result<Vec<ConsumedRecord>, KafkaError> {
            unimplemented!()
        }
        fn describe_topic_configs(
            &self,
            _topics: &[String],
        ) -> Result<Vec<(String, TopicConfigRead)>, KafkaError> {
            self.0.clone()
        }
    }

    fn names(topics: &[&str]) -> Vec<String> {
        topics.iter().map(|t| (*t).to_string()).collect()
    }

    fn manifest(rows: &[(&str, &[(&str, &str)])]) -> BTreeMap<String, BTreeMap<String, String>> {
        rows.iter()
            .map(|(topic, cfg)| {
                (
                    (*topic).to_string(),
                    cfg.iter()
                        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                        .collect(),
                )
            })
            .collect()
    }

    /// **The FX-4 defect, and the mutant the brief names first: "coverage
    /// forced to `captured` on a denied read".** A refused read is
    /// `captureDenied` whatever the manifest says — including an EMPTY
    /// manifest record, which is exactly what the engine writes for a denied
    /// topic and exactly what used to compare as "no overrides".
    #[test]
    fn a_denied_read_is_capture_denied_even_beside_an_empty_manifest_record() {
        let observed = observe(
            &Scripted(Ok(vec![(
                "orders".into(),
                Err(KafkaError::NotAuthorized(
                    "orders (DescribeConfigs …)".into(),
                )),
            )])),
            &names(&["orders"]),
        );
        let coverage = classify(&observed, &manifest(&[("orders", &[])]));
        assert_eq!(
            coverage["orders"],
            TopicConfigCoverage {
                coverage: "captureDenied".into(),
                reason: None,
                timestamp_type: None,
            }
        );
    }

    /// **The no-misfire direction (FX-4 review M4, mutant K1): the common
    /// case.** A topic with NO override answers only broker and default
    /// entries; the engine keeps none of them, so its manifest record is
    /// empty and EQUAL to what Logweir read — `captured`, with the effective
    /// timestamp type from the broker's default. It stays `captured` beside a
    /// denied neighbour, whose refusal emptied every record in the run: the
    /// empty record is still the accurate one for this topic (measured live
    /// by the review, `plain` beside `denied`).
    #[test]
    fn a_read_with_no_override_beside_an_empty_manifest_record_is_captured() {
        let observed = observe(
            &Scripted(Ok(vec![
                (
                    "plain".into(),
                    Ok(vec![
                        entry("cleanup.policy", "delete", ConfigSourceKind::DefaultConfig),
                        entry(
                            "message.timestamp.type",
                            "CreateTime",
                            ConfigSourceKind::DefaultConfig,
                        ),
                        entry(
                            "retention.ms",
                            "604800000",
                            ConfigSourceKind::DynamicDefaultBrokerConfig,
                        ),
                    ]),
                ),
                (
                    "denied".into(),
                    Err(KafkaError::NotAuthorized(
                        "denied (DescribeConfigs …)".into(),
                    )),
                ),
            ])),
            &names(&["plain", "denied"]),
        );
        let coverage = classify(&observed, &manifest(&[("plain", &[]), ("denied", &[])]));
        assert_eq!(
            coverage["plain"],
            TopicConfigCoverage {
                coverage: "captured".into(),
                reason: None,
                timestamp_type: Some(EffectiveConfigValue {
                    value: "CreateTime".into(),
                    source: "defaultConfig".into(),
                }),
            },
            "a no-override topic whose empty record is accurate is captured"
        );
        assert_eq!(coverage["denied"].coverage, "captureDenied");
    }

    #[test]
    fn a_read_the_manifest_agrees_with_is_captured_and_records_the_effective_timestamp_type() {
        let observed = observe(
            &Scripted(Ok(vec![(
                "orders".into(),
                Ok(vec![
                    entry(
                        "retention.ms",
                        "3600000",
                        ConfigSourceKind::DynamicTopicConfig,
                    ),
                    // A broker default: the engine does not keep it, so the
                    // manifest cannot hold it either and equality ignores it.
                    entry("cleanup.policy", "delete", ConfigSourceKind::DefaultConfig),
                    entry(
                        TIMESTAMP_TYPE_KEY,
                        "LogAppendTime",
                        ConfigSourceKind::DynamicDefaultBrokerConfig,
                    ),
                ]),
            )])),
            &names(&["orders"]),
        );
        let coverage = classify(
            &observed,
            &manifest(&[("orders", &[("retention.ms", "3600000")])]),
        );
        assert_eq!(
            coverage["orders"],
            TopicConfigCoverage {
                coverage: "captured".into(),
                reason: None,
                timestamp_type: Some(EffectiveConfigValue {
                    value: "LogAppendTime".into(),
                    source: "dynamicDefaultBrokerConfig".into(),
                }),
            },
            "FX-8's broker-default arm: the value AND its source are recorded"
        );
    }

    /// The engine's all-or-nothing capture: one denied topic empties EVERY
    /// topic's record. A topic Logweir read fine but whose manifest record is
    /// empty while it has overrides is NOT captured.
    #[test]
    fn a_manifest_record_that_disagrees_with_the_read_is_not_captured() {
        let observed = observe(
            &Scripted(Ok(vec![
                (
                    "orders".into(),
                    Ok(vec![
                        entry(
                            "retention.ms",
                            "3600000",
                            ConfigSourceKind::DynamicTopicConfig,
                        ),
                        entry(
                            TIMESTAMP_TYPE_KEY,
                            "CreateTime",
                            ConfigSourceKind::DefaultConfig,
                        ),
                    ]),
                ),
                (
                    "payments".into(),
                    Ok(vec![entry(
                        "retention.ms",
                        "1",
                        ConfigSourceKind::DynamicTopicConfig,
                    )]),
                ),
            ])),
            &names(&["orders", "payments"]),
        );
        // `orders` is in the manifest with nothing; `payments` is not in it.
        let coverage = classify(&observed, &manifest(&[("orders", &[])]));
        for topic in ["orders", "payments"] {
            assert_eq!(coverage[topic].coverage, "notCaptured", "{topic}");
            assert_eq!(coverage[topic].reason.as_deref(), Some("manifestDiffers"));
        }
        assert_eq!(
            coverage["orders"].timestamp_type,
            Some(EffectiveConfigValue {
                value: "CreateTime".into(),
                source: "defaultConfig".into(),
            }),
            "Logweir's own observation stands even where the engine's did not"
        );
    }

    /// Every other failure — the call itself, a topic the answer does not
    /// name, an unknown topic, a reader that cannot answer — is
    /// `notCaptured`/`describeFailed`, never absent and never captured.
    #[test]
    fn every_other_failure_is_not_captured_describe_failed_and_every_topic_is_answered() {
        let failed = TopicConfigCoverage {
            coverage: "notCaptured".into(),
            reason: Some("describeFailed".into()),
            timestamp_type: None,
        };
        let call_failed = observe(
            &Scripted(Err(KafkaError::Unreachable("no broker".into()))),
            &names(&["a", "b"]),
        );
        let coverage = classify(&call_failed, &BTreeMap::new());
        assert_eq!(coverage.len(), 2);
        assert!(coverage.values().all(|c| *c == failed));

        let partial = observe(
            &Scripted(Ok(vec![(
                "a".into(),
                Err(KafkaError::TopicNotFound("a".into())),
            )])),
            &names(&["a", "b"]),
        );
        let coverage = classify(&partial, &BTreeMap::new());
        assert_eq!(coverage["a"], failed, "an unknown topic");
        assert_eq!(coverage["b"], failed, "a topic the answer did not name");
    }

    /// A value the receipt does not define is NOT RECORDED rather than
    /// written — arm 11 would otherwise make the run refuse its own receipt
    /// after the archive exists.
    #[test]
    fn a_timestamp_value_outside_the_two_is_not_recorded() {
        let observed = observe(
            &Scripted(Ok(vec![(
                "orders".into(),
                Ok(vec![entry(
                    TIMESTAMP_TYPE_KEY,
                    "BrokerTime",
                    ConfigSourceKind::DynamicTopicConfig,
                )]),
            )])),
            &names(&["orders"]),
        );
        let coverage = classify(&observed, &manifest(&[("orders", &[])]));
        assert_eq!(coverage["orders"].timestamp_type, None);
    }

    /// Whatever `classify` writes, a receipt carrying it satisfies arms 6-11:
    /// a writer that could produce a block its own reader refuses would exit 4
    /// after the archive exists.
    #[test]
    fn every_shape_classify_produces_satisfies_the_receipts_arms() {
        let observed = observe(
            &Scripted(Ok(vec![
                ("a".into(), Err(KafkaError::NotAuthorized("a".into()))),
                ("b".into(), Err(KafkaError::Client("x".into()))),
                (
                    "c".into(),
                    Ok(vec![entry(
                        TIMESTAMP_TYPE_KEY,
                        "CreateTime",
                        ConfigSourceKind::Unknown,
                    )]),
                ),
                (
                    "d".into(),
                    Ok(vec![entry(
                        "retention.ms",
                        "5",
                        ConfigSourceKind::DynamicTopicConfig,
                    )]),
                ),
            ])),
            &names(&["a", "b", "c", "d"]),
        );
        let block = classify(
            &observed,
            &manifest(&[("c", &[]), ("d", &[("retention.ms", "5")])]),
        );
        let receipt = logweir_core::backup_receipt::BackupReceipt {
            format_version: logweir_core::backup_receipt::RECEIPT_FORMAT_VERSION.into(),
            run_id: "r".into(),
            backup_id: "b".into(),
            requested_at: chrono::Utc::now(),
            started_at: chrono::Utc::now(),
            finished_at: chrono::Utc::now(),
            exit_code: 0,
            triggered_by: String::new(),
            source: logweir_core::backup_receipt::ReceiptSource {
                cluster_id: "c".into(),
                bootstrap_servers: vec![],
                auth: logweir_core::backup_receipt::ReceiptAuth {
                    mode: "plaintext".into(),
                    username: None,
                },
                topics: names(&["a", "b", "c", "d"]),
            },
            engine: logweir_core::backup_receipt::ReceiptEngine {
                id: "e".into(),
                version: "v".into(),
                digest: "d".into(),
            },
            archive: logweir_core::backup_receipt::ReceiptArchive {
                manifest_key: "k".into(),
                manifest_sha256: "s".into(),
                manifest_version_id: None,
                prefix: "p".into(),
            },
            records: names(&["a", "b", "c", "d"])
                .into_iter()
                .map(|t| (t, 1))
                .collect(),
            covered: logweir_core::backup_receipt::ReceiptCovered {
                from_ms: 1,
                to_ms: 2,
            },
            config_coverage: Some(block),
            topic_configuration: None,
            owner_detection: None,
            consumer_positions: None,
            schema_dependency: None,
            generations: None,
        };
        assert_eq!(receipt.validate_invariants(), Ok(()));
    }

    fn sensitive(name: &str, source: ConfigSourceKind) -> ConfigEntryObservation {
        ConfigEntryObservation {
            name: name.to_string(),
            value: Some("hunter2-not-a-secret-fixture".to_string()),
            source,
            read_only: false,
            sensitive: true,
        }
    }

    /// **PROD-05.1, the projection.** A compacted, min-in-sync-2 topic with
    /// an inherited retention, a removed-in-4.0 override, a provider-only
    /// override and a secret; a delete-policy topic denied; a third whose
    /// read failed. Every class lands where the table puts it, a secret is
    /// recorded by key with no value, a refused read records NO entries (not
    /// an empty set), and the manifest's counts and the owner travel.
    #[test]
    fn the_model_records_overrides_semantic_defaults_and_secrets_by_key_only() {
        let observed = observe(
            &Scripted(Ok(vec![
                (
                    "orders".into(),
                    Ok(vec![
                        entry(
                            "cleanup.policy",
                            "compact",
                            ConfigSourceKind::DynamicTopicConfig,
                        ),
                        entry(
                            "min.insync.replicas",
                            "2",
                            ConfigSourceKind::DynamicTopicConfig,
                        ),
                        entry("retention.ms", "604800000", ConfigSourceKind::DefaultConfig),
                        // Not semantic and inherited: not recorded.
                        entry(
                            "segment.bytes",
                            "1073741824",
                            ConfigSourceKind::StaticBrokerConfig,
                        ),
                        entry(
                            "message.format.version",
                            "3.0-IV1",
                            ConfigSourceKind::DynamicTopicConfig,
                        ),
                        entry(
                            "confluent.placement.constraints",
                            "{}",
                            ConfigSourceKind::DynamicTopicConfig,
                        ),
                        sensitive("vendor.token", ConfigSourceKind::DynamicTopicConfig),
                    ]),
                ),
                (
                    "payments".into(),
                    Err(KafkaError::NotAuthorized("payments".into())),
                ),
                ("audit".into(), Err(KafkaError::Client("timeout".into()))),
            ])),
            &names(&["orders", "payments", "audit"]),
        );
        let mut layouts = BTreeMap::new();
        layouts.insert("orders".to_string(), (Some(3), Some(3)));
        layouts.insert("payments".to_string(), (Some(6), Some(1)));
        // A manifest that holds a zero count: NOT RECORDED, never 0.
        layouts.insert("audit".to_string(), (Some(0), None));
        let mut owners = BTreeMap::new();
        owners.insert(
            "orders".to_string(),
            TopicOwner {
                kind: "strimzi".into(),
                basis: "kafkaTopicResource".into(),
                reference: "kafka/orders".into(),
            },
        );
        let m = model(&observed, &layouts, &BTreeMap::new(), &owners);
        let orders = &m["orders"];
        assert_eq!(
            (orders.partitions, orders.replication_factor),
            (Some(3), Some(3))
        );
        assert_eq!(orders.owner.as_ref().unwrap().reference, "kafka/orders");
        let e = orders
            .entries
            .as_ref()
            .expect("a successful read records entries");
        let class = |k: &str| e[k].portability.as_str();
        assert_eq!(class("cleanup.policy"), "portable");
        assert_eq!(e["cleanup.policy"].value.as_deref(), Some("compact"));
        assert_eq!(class("min.insync.replicas"), "portable");
        assert_eq!(class("retention.ms"), "inherited");
        assert_eq!(e["retention.ms"].source, "defaultConfig");
        assert_eq!(class("message.format.version"), "removedInKafka4");
        assert_eq!(class("confluent.placement.constraints"), "providerOnly");
        assert_eq!(class("vendor.token"), "secret");
        assert_eq!(e["vendor.token"].value, None);
        assert!(!e.contains_key("segment.bytes"));
        assert!(
            !format!("{m:?}").contains("hunter2"),
            "a sensitive value never reaches the model"
        );
        let payments = &m["payments"];
        assert_eq!(payments.entries, None, "a denied read records no entries");
        assert_eq!(
            (payments.partitions, payments.replication_factor),
            (Some(6), Some(1))
        );
        assert_eq!(payments.owner, None);
        let audit = &m["audit"];
        assert_eq!(audit.entries, None, "a failed read records no entries");
        assert_eq!((audit.partitions, audit.replication_factor), (None, None));
    }

    /// Whatever `model` and `classify` write together, a 1.3.0 receipt
    /// carrying both satisfies arms 12-21: the writer cannot produce a model
    /// its own reader refuses.
    #[test]
    fn every_shape_model_produces_satisfies_the_receipts_arms() {
        let observed = observe(
            &Scripted(Ok(vec![
                ("a".into(), Err(KafkaError::NotAuthorized("a".into()))),
                ("b".into(), Err(KafkaError::Client("x".into()))),
                (
                    "c".into(),
                    Ok(vec![
                        entry(TIMESTAMP_TYPE_KEY, "CreateTime", ConfigSourceKind::Unknown),
                        sensitive("s", ConfigSourceKind::DynamicTopicConfig),
                    ]),
                ),
                (
                    "d".into(),
                    Ok(vec![
                        entry("retention.ms", "5", ConfigSourceKind::DynamicTopicConfig),
                        entry("x.vendor", "1", ConfigSourceKind::DynamicTopicConfig),
                    ]),
                ),
            ])),
            &names(&["a", "b", "c", "d"]),
        );
        let coverage = classify(
            &observed,
            // `c`'s manifest record disagrees (manifestDiffers): entries stand.
            &manifest(&[
                ("c", &[("retention.ms", "1")]),
                ("d", &[("retention.ms", "5")]),
            ]),
        );
        let mut layouts = BTreeMap::new();
        layouts.insert("a".to_string(), (Some(-1), Some(0)));
        layouts.insert("d".to_string(), (Some(2), Some(1)));
        let mut owners = BTreeMap::new();
        owners.insert(
            "b".to_string(),
            TopicOwner {
                kind: "external".into(),
                basis: "declared".into(),
                reference: "gitops: topics/b.yaml".into(),
            },
        );
        let m = model(&observed, &layouts, &BTreeMap::new(), &owners);
        let topics = names(&["a", "b", "c", "d"]);
        let receipt = logweir_core::backup_receipt::BackupReceipt {
            format_version: logweir_core::backup_receipt::FORMAT_VERSION_WITH_TOPIC_CONFIGURATION
                .into(),
            run_id: "r".into(),
            backup_id: "b".into(),
            requested_at: chrono::Utc::now(),
            started_at: chrono::Utc::now(),
            finished_at: chrono::Utc::now(),
            exit_code: 0,
            triggered_by: String::new(),
            source: logweir_core::backup_receipt::ReceiptSource {
                cluster_id: "c".into(),
                bootstrap_servers: vec![],
                auth: logweir_core::backup_receipt::ReceiptAuth {
                    mode: "plaintext".into(),
                    username: None,
                },
                topics: topics.clone(),
            },
            engine: logweir_core::backup_receipt::ReceiptEngine {
                id: "e".into(),
                version: "v".into(),
                digest: "d".into(),
            },
            archive: logweir_core::backup_receipt::ReceiptArchive {
                manifest_key: "k".into(),
                manifest_sha256: "s".into(),
                manifest_version_id: None,
                prefix: "p".into(),
            },
            records: topics.into_iter().map(|t| (t, 1)).collect(),
            covered: logweir_core::backup_receipt::ReceiptCovered {
                from_ms: 1,
                to_ms: 2,
            },
            config_coverage: Some(coverage),
            topic_configuration: Some(m),
            owner_detection: Some(vec!["declared".into()]),
            consumer_positions: None,
            schema_dependency: None,
            generations: None,
        };
        assert_eq!(receipt.validate_invariants(), Ok(()));
    }

    /// **The factor's source** (PROD-05.1, measured): Logweir's own metadata
    /// read wins over the manifest, which the pinned engine fills for the
    /// first topic it saves only; the manifest's stands where the read named
    /// none; a zero from either is NOT RECORDED.
    #[test]
    fn the_factor_is_logweirs_own_read_and_the_manifests_only_where_that_named_none() {
        let observed = observe(
            &Scripted(Ok(vec![
                ("a".into(), Err(KafkaError::Client("x".into()))),
                ("b".into(), Err(KafkaError::Client("x".into()))),
                ("c".into(), Err(KafkaError::Client("x".into()))),
                ("d".into(), Err(KafkaError::Client("x".into()))),
            ])),
            &names(&["a", "b", "c", "d"]),
        );
        let mut layouts = BTreeMap::new();
        layouts.insert("a".to_string(), (Some(6), Some(2)));
        layouts.insert("b".to_string(), (Some(6), Some(1)));
        layouts.insert("c".to_string(), (Some(1), None));
        layouts.insert("d".to_string(), (Some(1), Some(0)));
        let mut factors = BTreeMap::new();
        factors.insert("a".to_string(), 3);
        factors.insert("c".to_string(), 0);
        let m = model(&observed, &layouts, &factors, &BTreeMap::new());
        assert_eq!(m["a"].replication_factor, Some(3), "the read wins");
        assert_eq!(
            m["b"].replication_factor,
            Some(1),
            "the manifest where the read named none"
        );
        assert_eq!(m["c"].replication_factor, None, "a zero is not recorded");
        assert_eq!(m["d"].replication_factor, None, "a zero is not recorded");
        assert_eq!(m["a"].partitions, Some(6), "partitions are the manifest's");
    }
}
