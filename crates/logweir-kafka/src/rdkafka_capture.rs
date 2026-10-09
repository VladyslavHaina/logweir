//! **PROD-04.1's broker glue**: one capture of the selected consumer groups
//! ([`crate::capture`]), from PROD-04.0b's classification and descriptions,
//! PROD-04.0a's positions, and two plain reads this module adds — a topic's
//! partitions (metadata) and its partitions' marks, READ_UNCOMMITTED. No
//! `unsafe`; every call is bounded by the reader's admin or position bound.
use crate::capture::{GroupsObservation, Marks, ObservedGroup, TopicMarks};
use crate::groups::GroupVerdict;
use crate::positions::{GroupListing, TopicPartition};
use crate::rdkafka_reader::RdKafkaReader;
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::collections::BTreeMap;

impl RdKafkaReader {
    /// The partition ids of `topic`, from one metadata read, sorted. A topic
    /// the answer does not name, or names with an error, is `Err`: never an
    /// empty list read as "no partitions".
    fn partition_ids(&self, topic: &str) -> Result<Vec<i32>, String> {
        let md = self
            .metadata_consumer()
            .fetch_metadata(Some(topic), self.admin_bound())
            .map_err(|e| format!("{topic}: metadata: {e}"))?;
        let t = md
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .ok_or_else(|| format!("{topic}: the metadata answer does not name the topic"))?;
        if let Some(err) = t.error() {
            return Err(format!(
                "{topic}: metadata: {}",
                rdkafka::error::RDKafkaErrorCode::from(err)
            ));
        }
        if t.partitions().is_empty() {
            return Err(format!("{topic}: the metadata answer lists no partition"));
        }
        let mut ids: Vec<i32> = t.partitions().iter().map(|p| p.id()).collect();
        ids.sort_unstable();
        Ok(ids)
    }

    /// A handle that reads marks READ_UNCOMMITTED, so a high watermark is the
    /// log end and never a transaction's last stable offset (PROD-01.4 §4.5,
    /// c11): a committed position above the stable offset is not "beyond the
    /// end". It never subscribes, assigns or commits.
    fn marks_handle(&self) -> Result<BaseConsumer, String> {
        let mut cfg = self.base_config().clone();
        cfg.set("group.id", "logweir-marks-do-not-commit")
            .set("enable.auto.commit", "false")
            .set("isolation.level", "read_uncommitted");
        cfg.create().map_err(|e| format!("the marks handle: {e}"))
    }

    /// The marks of exactly `partitions` of `topic`.
    fn marks_of(
        handle: &BaseConsumer,
        topic: &str,
        partitions: &[i32],
        bound: std::time::Duration,
    ) -> Vec<(i32, Result<Marks, String>)> {
        partitions
            .iter()
            .map(|&p| {
                let read = handle
                    .fetch_watermarks(topic, p, bound)
                    .map_err(|e| format!("{topic}:{p}: marks: {e}"))
                    .and_then(|(lo, hi)| {
                        if 0 <= lo && lo <= hi {
                            Ok(Marks {
                                log_start: lo,
                                high_watermark: hi,
                            })
                        } else {
                            Err(format!(
                                "{topic}:{p}: the broker answered log start {lo} and high \
                                 watermark {hi}, which are not marks"
                            ))
                        }
                    });
                (p, read)
            })
            .collect()
    }

    /// **PROD-04.1.** Per named topic, its partitions and each one's marks,
    /// read now (`ClusterReader::partition_marks`).
    pub fn read_partition_marks(&self, topics: &[String]) -> BTreeMap<String, TopicMarks> {
        let handle = match self.marks_handle() {
            Ok(h) => h,
            Err(e) => return topics.iter().map(|t| (t.clone(), Err(e.clone()))).collect(),
        };
        topics
            .iter()
            .map(|t| {
                let read = self
                    .partition_ids(t)
                    .map(|ids| Self::marks_of(&handle, t, &ids, self.admin_bound()));
                (t.clone(), read)
            })
            .collect()
    }

    /// **PROD-04.1.** One capture of `selected` over the partitions of
    /// `topics`, in `crate::capture`'s order.
    pub fn capture_consumer_groups(
        &self,
        selected: &[String],
        topics: &[String],
    ) -> GroupsObservation {
        // 1. Classify. A refused selection (a blank id) observes nothing.
        let classification = match self.classify_groups(selected) {
            Ok(c) => c,
            Err(e) => return GroupsObservation::unavailable(topics, e.to_string()),
        };
        // 2. Describe what may be captured.
        let capturable: Vec<_> = classification
            .verdicts
            .iter()
            .filter_map(|(_, v)| match v {
                GroupVerdict::Capture(g) => Some(g.clone()),
                _ => None,
            })
            .collect();
        let mut descriptions: BTreeMap<String, _> =
            self.describe_groups(&capturable).into_iter().collect();
        // 3. The partitions every position is asked for.
        let asked: BTreeMap<String, Result<Vec<i32>, String>> = topics
            .iter()
            .map(|t| (t.clone(), self.partition_ids(t)))
            .collect();
        let tps: Vec<TopicPartition> = asked
            .iter()
            .filter_map(|(t, ids)| ids.as_ref().ok().map(|ids| (t, ids)))
            .flat_map(|(t, ids)| ids.iter().map(move |p| TopicPartition::new(t.clone(), *p)))
            .collect();
        // 4. The positions of every described group, one fetch each.
        let groups = classification
            .verdicts
            .into_iter()
            .map(|(group_id, verdict)| {
                let description = match &verdict {
                    GroupVerdict::Capture(_) => descriptions.remove(&group_id),
                    _ => None,
                };
                let positions = match &description {
                    Some(Ok(_)) => {
                        Some(self.committed_positions(&group_id, &tps, GroupListing::Listed))
                    }
                    _ => None,
                };
                ObservedGroup {
                    group_id,
                    verdict,
                    description,
                    positions,
                }
            })
            .collect();
        // 5. The marks of exactly the asked partitions, AFTER the positions.
        let handle = self.marks_handle();
        let topics = asked
            .into_iter()
            .map(|(t, ids)| {
                let read = match (&handle, ids) {
                    (_, Err(e)) => Err(e),
                    (Err(e), Ok(_)) => Err(e.clone()),
                    (Ok(h), Ok(ids)) => Ok(Self::marks_of(h, &t, &ids, self.admin_bound())),
                };
                (t, read)
            })
            .collect();
        GroupsObservation {
            completeness: Some(classification.completeness),
            groups,
            topics,
            unavailable: None,
        }
    }
}
