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
    EffectiveConfigValue, TopicConfigCoverage, NOT_CAPTURED_REASONS, TIMESTAMP_TYPES,
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
    Observation::Read {
        engine_view,
        timestamp_type,
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

/// One line per topic for the run's log, so an operator reading `backup run`'s
/// output learns a denied read without opening the receipt.
pub fn log(observations: &BTreeMap<String, Observation>) {
    for (topic, observed) in observations {
        match observed {
            Observation::Read {
                engine_view,
                timestamp_type,
            } => tracing::info!(
                topic = %topic,
                overrides = engine_view.len(),
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
