//! **PROD-01.4a: the topic IDs a backup receipt records** (format 1.5.0,
//! `generations`), and projected into the catalog point.
//!
//! # The defect this closes
//!
//! A topic deleted and recreated under the same name is a NEW topic: its
//! offsets restart at zero and mean other records. Before this row Logweir
//! could not tell: the pinned engine asks for Metadata v9, which carries no
//! topic ID, and rust-rdkafka 0.36.2 has no DescribeTopics. Two backups of a
//! recreated topic read as two points of one history, and PROD-01.4 measured
//! two recreations its offset heuristic misses (c13, c14:
//! `docs/to-do/decisions/PROD-01.4-topic-identity.md` §4.7, FN1 and FN2).
//!
//! # Where the IDs come from
//!
//! `logweir backup run` reads each named topic's ID ITSELF, through the same
//! reader — the same principal and source cluster — that admitted the run:
//! once IMMEDIATELY before the engine starts, the last read of phase −1
//! ([`observe`]), and once immediately after it exits. The read is
//! `logweir_kafka::reader::ClusterReader::topic_ids` (DescribeTopics through
//! `logweir-rdkafka-ffi`, OD-6 (a2)). [`block`] writes the two reads into the
//! receipt's entry per topic (`logweir_core::topic_identity::observed`): each ID
//! in Kafka's text, or `null` with the reason.
//!
//! # Nothing here fails the backup
//!
//! An ID is a RECORDED FACT, not a refusal. A broker below inter-broker
//! protocol 2.8 has no IDs (`noTopicId`); a principal that may not Describe the
//! topic is refused by name (`notAuthorized`), never read as "absent"; a failed
//! read is `readFailed`. In each case the generation stays UNKNOWN — today's
//! fallback, stated in the receipt and never guessed — and the backup proceeds
//! exactly as before. A topic whose two reads DIFFER was deleted and recreated
//! while the engine ran: the receipt records both IDs, and this module warns
//! ([`log_changes`]).

use logweir_core::backup_receipt::TopicIdentity;
use logweir_core::topic_identity::{observed, within_capture, IdRead, WithinCapture, READ_FAILED};
use logweir_kafka::reader::ClusterReader;
use logweir_kafka::topic_ids::TopicIdRead;
use std::collections::BTreeMap;

/// **One DescribeTopics read of every named topic**, as the receipt's side
/// ([`IdRead`]): one entry per name, whatever happened. A whole-call failure,
/// and a name the reader did not answer for, is `readFailed`. `when` names the
/// read in the log (`before the engine`, `after the engine`).
#[must_use]
pub fn observe(
    reader: &dyn ClusterReader,
    topics: &[String],
    when: &str,
) -> BTreeMap<String, IdRead> {
    let reads = match reader.topic_ids(topics) {
        Ok(reads) => reads,
        Err(e) => {
            tracing::warn!(
                error = %e,
                read = when,
                "the topics' IDs could not be read; the receipt records them as null \
                 (readFailed), so their generation is UNKNOWN for this point"
            );
            return topics
                .iter()
                .map(|t| (t.clone(), IdRead::Unread(READ_FAILED)))
                .collect();
        }
    };
    let mut out: BTreeMap<String, IdRead> = BTreeMap::new();
    for (topic, read) in reads {
        match &read {
            TopicIdRead::Id(id) => {
                tracing::info!(topic = %topic, topic_id = %id, read = when, "topic ID read");
            }
            TopicIdRead::NoId => tracing::warn!(
                topic = %topic,
                read = when,
                "the broker has no topic ID for this topic (a cluster below inter-broker \
                 protocol 2.8): recorded as null (noTopicId), its generation UNKNOWN"
            ),
            TopicIdRead::NotAuthorized => tracing::warn!(
                topic = %topic,
                read = when,
                "topic ID read REFUSED: the backup principal may not Describe the topic; \
                 recorded as null (notAuthorized), its generation UNKNOWN"
            ),
            TopicIdRead::NotFound => tracing::warn!(
                topic = %topic,
                read = when,
                "topic ID read: the broker does not hold the topic; recorded as null \
                 (topicNotFound)"
            ),
            TopicIdRead::Failed(why) => tracing::warn!(
                topic = %topic,
                read = when,
                error = %why,
                "topic ID read failed; recorded as null (readFailed)"
            ),
            TopicIdRead::NotRead => tracing::info!(
                topic = %topic,
                read = when,
                "this reader does not read topic IDs; recorded as null (notRead)"
            ),
        }
        out.insert(topic, read.to_id_read());
    }
    // One entry per named topic, whatever the reader returned.
    for topic in topics {
        out.entry(topic.clone())
            .or_insert(IdRead::Unread(READ_FAILED));
    }
    out
}

/// **The receipt's `generations` block**: one entry per named topic, from the
/// read before the engine and the read after it. A topic missing from either
/// read is `readFailed` there.
#[must_use]
pub fn block(
    topics: &[String],
    before: &BTreeMap<String, IdRead>,
    after: &BTreeMap<String, IdRead>,
) -> BTreeMap<String, TopicIdentity> {
    let failed = IdRead::Unread(READ_FAILED);
    topics
        .iter()
        .map(|t| {
            (
                t.clone(),
                observed(
                    before.get(t).unwrap_or(&failed),
                    after.get(t).unwrap_or(&failed),
                ),
            )
        })
        .collect()
}

/// Warns for every topic whose ID changed while the engine ran: it was deleted
/// and recreated during the capture, so the point mixes two generations
/// (decision §4.4). The receipt records both IDs; nothing is refused.
pub fn log_changes(block: &BTreeMap<String, TopicIdentity>) {
    for (topic, entry) in block {
        if let WithinCapture::Changed { before, after } = within_capture(entry) {
            tracing::warn!(
                topic = %topic,
                topic_id_before = %before,
                topic_id_after = %after,
                "the topic's ID CHANGED while the engine ran: it was deleted and recreated \
                 during the capture, so this point mixes two generations. The receipt \
                 records both IDs"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logweir_kafka::reader::{ConsumedRecord, KafkaError, TopicMeta};

    /// A reader whose `topic_ids` answers from a fixed script.
    struct Ids(Result<Vec<(String, TopicIdRead)>, KafkaError>);

    impl ClusterReader for Ids {
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
        fn topic_ids(&self, _: &[String]) -> Result<Vec<(String, TopicIdRead)>, KafkaError> {
            self.0.clone()
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

    const A: &str = "gtOq2VXiTCK1QM2UtERijA";
    const B: &str = "tpWwuKExQo2lN9NziDMpYg";

    fn topics() -> Vec<String> {
        ["orders", "payments", "refunds"].map(String::from).to_vec()
    }

    #[test]
    fn every_named_topic_gets_its_id_or_its_named_reason() {
        let reader = Ids(Ok(vec![
            ("orders".into(), TopicIdRead::Id(A.into())),
            ("payments".into(), TopicIdRead::NotAuthorized),
            // `refunds` is not answered at all.
        ]));
        let got = observe(&reader, &topics(), "before the engine");
        assert_eq!(got["orders"], IdRead::Id(A.into()));
        assert_eq!(got["payments"], IdRead::Unread("notAuthorized"));
        assert_eq!(got["refunds"], IdRead::Unread("readFailed"));
        assert_eq!(got.len(), 3);
    }

    #[test]
    fn a_failed_call_is_read_failed_for_every_topic_and_never_an_id() {
        let reader = Ids(Err(KafkaError::Unreachable("no broker".into())));
        let got = observe(&reader, &topics(), "after the engine");
        assert!(got.values().all(|r| *r == IdRead::Unread("readFailed")));
        assert_eq!(got.len(), 3);
    }

    /// A reader that does not implement the read (every test double) leaves
    /// every ID null with `notRead`: the trait's default, never a guess.
    #[test]
    fn a_reader_that_reads_no_ids_records_not_read() {
        struct Plain;
        impl ClusterReader for Plain {
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
        let got = observe(&Plain, &topics(), "before the engine");
        assert!(got.values().all(|r| *r == IdRead::Unread("notRead")));
    }

    #[test]
    fn the_block_pairs_the_two_reads_per_topic() {
        let t = topics();
        let before: BTreeMap<String, IdRead> = [
            ("orders".to_string(), IdRead::Id(A.into())),
            ("payments".to_string(), IdRead::Id(A.into())),
        ]
        .into_iter()
        .collect();
        let after: BTreeMap<String, IdRead> = [
            ("orders".to_string(), IdRead::Id(A.into())),
            ("payments".to_string(), IdRead::Id(B.into())),
            ("refunds".to_string(), IdRead::Unread("topicNotFound")),
        ]
        .into_iter()
        .collect();
        let b = block(&t, &before, &after);
        assert_eq!(b.len(), 3);
        assert_eq!(b["orders"].topic_id.as_deref(), Some(A));
        assert_eq!(b["orders"].topic_id_after.as_deref(), Some(A));
        assert_eq!(
            b["orders"].topic_id_source.as_deref(),
            Some("describeTopics")
        );
        assert_eq!(
            within_capture(&b["payments"]),
            WithinCapture::Changed {
                before: A,
                after: B
            }
        );
        assert_eq!(b["refunds"].topic_id, None);
        assert_eq!(b["refunds"].topic_id_reason.as_deref(), Some("readFailed"));
        assert_eq!(
            b["refunds"].topic_id_after_reason.as_deref(),
            Some("topicNotFound")
        );
        assert_eq!(b["refunds"].topic_id_source, None);
    }
}
