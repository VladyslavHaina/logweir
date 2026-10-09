//! **PROD-04.1's broker glue**: one capture of the selected consumer groups
//! ([`crate::capture`]), from PROD-04.0b's classification and descriptions,
//! PROD-04.0a's positions, and two plain reads this module adds — a topic's
//! partitions (metadata) and its partitions' marks, READ_UNCOMMITTED. No
//! `unsafe`; every call is bounded by the reader's admin or position bound,
//! and one pass over the marks by [`MARKS_BUDGET`] in all.
//!
//! **The marks are read only when a position was asked for** (PROD-04.1
//! review L8): a capture that described no group — every group excluded, as
//! on a broker that types none (Kafka 3.7.x), or failed — reads no partition
//! and no mark, so an unavailable leader costs nothing there.
use crate::capture::{GroupsObservation, Marks, ObservedGroup, TopicMarks};
use crate::groups::GroupVerdict;
use crate::positions::{GroupListing, TopicPartition};
use crate::rdkafka_reader::RdKafkaReader;
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// The most one pass over the marks may take, across every partition of every
/// named topic. Each watermark read is bounded by the admin bound already; a
/// broker whose leaders are down would otherwise cost that bound once per
/// partition. A partition the pass did not reach in time is recorded with
/// its marks NOT read (`MarksNotRead` for a position there), never guessed.
pub const MARKS_BUDGET: Duration = Duration::from_secs(60);

/// Why a topic's partitions were not read at capture: no group was described,
/// so no position was asked for.
pub const NOTHING_ASKED: &str =
    "not read: no selected group was described, so no position was asked for";

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

    /// The marks of exactly `partitions` of `topic`, each bounded by `bound`,
    /// none started after `deadline`.
    fn marks_of(
        handle: &BaseConsumer,
        topic: &str,
        partitions: &[i32],
        bound: Duration,
        deadline: Instant,
    ) -> Vec<(i32, Result<Marks, String>)> {
        partitions
            .iter()
            .map(|&p| {
                if Instant::now() >= deadline {
                    return (
                        p,
                        Err(format!(
                            "{topic}:{p}: marks not read: the {}-second budget for one pass over the marks was spent",
                            MARKS_BUDGET.as_secs()
                        )),
                    );
                }
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
        let deadline = Instant::now() + MARKS_BUDGET;
        topics
            .iter()
            .map(|t| {
                let read = self
                    .partition_ids(t)
                    .map(|ids| Self::marks_of(&handle, t, &ids, self.admin_bound(), deadline));
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
        // 3. The partitions every position is asked for — only when a group
        //    was described, so a capture that will ask nothing reads nothing.
        let any_described = descriptions.values().any(Result::is_ok);
        let asked: BTreeMap<String, Result<Vec<i32>, String>> = topics
            .iter()
            .map(|t| {
                let ids = if any_described {
                    self.partition_ids(t)
                } else {
                    Err(format!("{t}: {NOTHING_ASKED}"))
                };
                (t.clone(), ids)
            })
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
        let handle = if any_described {
            self.marks_handle()
        } else {
            Err(NOTHING_ASKED.to_string())
        };
        let deadline = Instant::now() + MARKS_BUDGET;
        let topics = asked
            .into_iter()
            .map(|(t, ids)| {
                let read = match (&handle, ids) {
                    (_, Err(e)) => Err(e),
                    (Err(e), Ok(_)) => Err(e.clone()),
                    (Ok(h), Ok(ids)) => {
                        Ok(Self::marks_of(h, &t, &ids, self.admin_bound(), deadline))
                    }
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
