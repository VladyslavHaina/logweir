//! What a switchover reads and writes on the restore's TARGET cluster, behind
//! one trait so the per-group rules ([`super::apply`]) are unit rows with no
//! broker. The one implementation dials Kafka through Logweir's own client
//! (`RdKafkaReader`: PROD-04.0's listing and describe, the safe
//! RequireStable fetch and the non-member commit), never the engine.

use logweir_core::switchover::TargetGroup;
use logweir_kafka::groups::{code, Excluded, GroupFailure, GroupVerdict, OtherType};
use logweir_kafka::positions::{
    CommitError, CommittedPosition, GroupListing, PartitionPosition, PositionsError, TopicPartition,
};
use logweir_kafka::rdkafka_reader::RdKafkaReader;
use logweir_kafka::reader::ClusterReader;
use std::collections::BTreeMap;

/// A partition of a restored topic.
pub type Part = (String, u32);

/// Why a group's committed positions could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadFailure {
    /// GROUP_AUTHORIZATION_FAILED on a listed group.
    NotAuthorized,
    /// The group may exist and this principal may not see it.
    NotVisible,
    /// A pending transactional offset commit (or a silent coordinator).
    Unstable,
    /// Anything else, in words.
    Other(String),
}

/// Why a commit was not (or not wholly) applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitFailure {
    /// UNKNOWN_MEMBER_ID: a member joined after the classification.
    GroupActive,
    NotAuthorized,
    /// GROUP_ID_NOT_FOUND: a group that takes no offset commits.
    NotAConsumerGroup,
    /// No coordinator answered; nothing was sent.
    NotVisible,
    /// Anything else; `may_have_applied` says whether to trust the readback
    /// over "nothing applied".
    Failed {
        may_have_applied: bool,
        message: String,
    },
}

/// The target cluster, as the switchover sees it.
pub trait SwitchoverTarget {
    /// The cluster id the brokers report.
    fn cluster_id(&self) -> Result<String, String>;
    /// Each topic's ID now (`None`: no ID could be read, with the reason).
    fn topic_ids(
        &self,
        topics: &[String],
    ) -> Result<BTreeMap<String, Result<String, String>>, String>;
    /// Each partition's `(log start, high watermark)` now.
    fn marks(&self, topics: &[String]) -> BTreeMap<Part, (i64, i64)>;
    /// PROD-04.0's classification of one target group id.
    fn classify(&self, group: &str) -> TargetGroup;
    /// The group's committed positions on `partitions` (`None`: none).
    fn committed(
        &self,
        group: &str,
        partitions: &[Part],
        listed: bool,
    ) -> Result<BTreeMap<Part, Option<i64>>, ReadFailure>;
    /// One non-member commit of `positions` (leader epoch −1, metadata
    /// `logweir:positions`).
    fn commit(&self, group: &str, positions: &[(Part, i64)]) -> Result<(), CommitFailure>;
}

fn tp(p: &Part) -> TopicPartition {
    TopicPartition::new(p.0.clone(), i32::try_from(p.1).unwrap_or(i32::MAX))
}

impl SwitchoverTarget for RdKafkaReader {
    fn cluster_id(&self) -> Result<String, String> {
        ClusterReader::cluster_id(self).map_err(|e| e.to_string())
    }

    fn topic_ids(
        &self,
        topics: &[String],
    ) -> Result<BTreeMap<String, Result<String, String>>, String> {
        use logweir_kafka::topic_ids::TopicIdRead;
        let read = ClusterReader::topic_ids(self, topics).map_err(|e| e.to_string())?;
        Ok(read
            .into_iter()
            .map(|(t, r)| {
                let v = match r {
                    TopicIdRead::Id(id) => Ok(id),
                    TopicIdRead::NotFound => Err("NotFound".to_string()),
                    other => Err(format!("{other:?}")),
                };
                (t, v)
            })
            .collect())
    }

    fn marks(&self, topics: &[String]) -> BTreeMap<Part, (i64, i64)> {
        let mut out = BTreeMap::new();
        for (topic, marks) in ClusterReader::partition_marks(self, topics) {
            let Ok(parts) = marks else { continue };
            for (p, m) in parts {
                if let (Ok(m), Ok(p)) = (m, u32::try_from(p)) {
                    out.insert((topic.clone(), p), (m.log_start, m.high_watermark));
                }
            }
        }
        out
    }

    fn classify(&self, group: &str) -> TargetGroup {
        let classification = match self.classify_groups(&[group.to_string()]) {
            Ok(c) => c,
            Err(e) => return TargetGroup::Unreachable(e.to_string()),
        };
        let Some((_, verdict)) = classification.verdicts.into_iter().next() else {
            return TargetGroup::Unreachable("the classification answered nothing".into());
        };
        match verdict {
            GroupVerdict::Capture(capturable) => {
                let described = self.describe_groups(std::slice::from_ref(&capturable));
                match described.into_iter().next().map(|(_, d)| d) {
                    Some(Ok(d)) => TargetGroup::Consumer {
                        group_type: d.group_type.wire_name().to_string(),
                        state: d.state.wire_name().to_string(),
                        members: u32::try_from(d.members.len()).unwrap_or(u32::MAX),
                    },
                    Some(Err(logweir_kafka::groups::DescribeFailure::NotAuthorized)) => {
                        TargetGroup::NotVisible
                    }
                    Some(Err(e)) => TargetGroup::Unreachable(format!("{e:?}")),
                    None => TargetGroup::Unreachable("the description answered nothing".into()),
                }
            }
            GroupVerdict::Excluded(Excluded::GroupNotFound { .. }) => TargetGroup::Absent,
            // A broker below ListGroups v5 types every group Unknown (0):
            // the group exists, and its type is not proven.
            GroupVerdict::Excluded(Excluded::GroupTypeNotCaptured {
                why: OtherType::UnknownType { raw },
            }) if raw == code::TYPE_UNKNOWN => TargetGroup::TypeUnproven,
            GroupVerdict::Excluded(Excluded::GroupTypeNotCaptured { .. }) => TargetGroup::OtherType,
            GroupVerdict::Failed(GroupFailure::NotVisibleToPrincipal) => TargetGroup::NotVisible,
            GroupVerdict::Failed(GroupFailure::TypeUnproven(_)) => TargetGroup::TypeUnproven,
            GroupVerdict::Failed(other) => TargetGroup::Unreachable(format!("{other:?}")),
        }
    }

    fn committed(
        &self,
        group: &str,
        partitions: &[Part],
        listed: bool,
    ) -> Result<BTreeMap<Part, Option<i64>>, ReadFailure> {
        let tps: Vec<TopicPartition> = partitions.iter().map(tp).collect();
        let listing = if listed {
            GroupListing::Listed
        } else {
            GroupListing::NotListed
        };
        let answer = self
            .committed_positions(group, &tps, listing)
            .map_err(|e| match e {
                PositionsError::NotAuthorized { .. } => ReadFailure::NotAuthorized,
                PositionsError::NotVisibleOrUnreachable { .. }
                | PositionsError::NotVisibleToPrincipal { .. } => ReadFailure::NotVisible,
                PositionsError::PositionsUnstable { .. } => ReadFailure::Unstable,
                other => ReadFailure::Other(other.to_string()),
            })?;
        let mut out = BTreeMap::new();
        for p in partitions {
            let v = match answer.get(&tp(p)) {
                Some(PartitionPosition::Committed(c)) => Some(c.offset),
                Some(PartitionPosition::NoCommittedPosition) => None,
                Some(PartitionPosition::Failed(f)) => {
                    return Err(ReadFailure::Other(format!("{}:{}: {f:?}", p.0, p.1)))
                }
                None => {
                    return Err(ReadFailure::Other(format!(
                        "{}:{}: the fetch answered nothing for it",
                        p.0, p.1
                    )))
                }
            };
            out.insert(p.clone(), v);
        }
        Ok(out)
    }

    fn commit(&self, group: &str, positions: &[(Part, i64)]) -> Result<(), CommitFailure> {
        let request: Vec<(TopicPartition, CommittedPosition)> = positions
            .iter()
            .map(|(p, o)| {
                (
                    tp(p),
                    CommittedPosition {
                        offset: *o,
                        leader_epoch: None,
                        metadata: Some(logweir_kafka::positions::COMMIT_METADATA_MARKER.into()),
                    },
                )
            })
            .collect();
        self.commit_positions(group, &request).map_err(|e| match e {
            CommitError::GroupActive { .. } => CommitFailure::GroupActive,
            CommitError::NotAuthorized { .. } => CommitFailure::NotAuthorized,
            CommitError::NotAConsumerGroup { .. } => CommitFailure::NotAConsumerGroup,
            CommitError::NotVisibleOrUnreachable { .. } => CommitFailure::NotVisible,
            other => CommitFailure::Failed {
                may_have_applied: other.may_have_applied(),
                message: other.to_string(),
            },
        })
    }
}
