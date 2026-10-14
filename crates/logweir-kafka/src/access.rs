//! **What DescribeCluster says the capturing principal may do on
//! the cluster** (KIP-430's authorized operations). Pure: the broker glue is
//! `crate::rdkafka_admin` (feature `client`), through
//! `logweir-rdkafka-ffi::cluster::describe_cluster`.
//!
//! It decides **T14, listing completeness**: ListGroups shows a principal
//! without Describe on the cluster only the groups it may Describe, silently.
//! With Describe on the cluster the listing is unfiltered.
//!
//! The answer has THREE states, and the first two must never be confused:
//! the broker REPORTED a set (possibly empty: a refusal), the broker reported
//! NOTHING (librdkafka's NULL: not requested, or a broker that does not
//! compute it), or the call failed.

/// `RD_KAFKA_ACL_OPERATION_DESCRIBE`, and Kafka's `AclOperation.DESCRIBE`.
pub const ACL_OPERATION_DESCRIBE: i32 = 8;

/// `RD_KAFKA_ACL_OPERATION_READ`, and Kafka's `AclOperation.READ`.
pub const ACL_OPERATION_READ: i32 = 3;

/// `RD_KAFKA_ACL_OPERATION_ALL`. KIP-430 never reports it (librdkafka skips
/// it when it parses the set), so it is read as a grant only if a broker ever
/// sends it, never required.
pub const ACL_OPERATION_ALL: i32 = 2;

/// The capturing principal's operations on the cluster resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterAccess {
    /// The broker reported the principal's authorized operations, as
    /// `rd_kafka_AclOperation_t` integers. An EMPTY set is a refusal of every
    /// operation, not "unknown".
    Reported(Vec<i32>),
    /// The broker reported no set at all (librdkafka's NULL array). Nothing is
    /// known: never read as a refusal, never as a grant.
    NotReported,
    /// DescribeCluster itself failed.
    Unread(String),
}

impl ClusterAccess {
    /// Whether the broker REPORTED Describe on the cluster. `None` when it
    /// reported nothing or the read failed: unknown, which is neither.
    #[must_use]
    pub fn describe(&self) -> Option<bool> {
        match self {
            ClusterAccess::Reported(ops) => Some(ops.contains(&ACL_OPERATION_DESCRIBE)),
            ClusterAccess::NotReported | ClusterAccess::Unread(_) => None,
        }
    }

    /// The state from DescribeCluster's raw answer: `None` is librdkafka's
    /// NULL array (not reported).
    #[must_use]
    pub fn from_operations(operations: Option<Vec<i32>>) -> ClusterAccess {
        match operations {
            Some(ops) => ClusterAccess::Reported(ops),
            None => ClusterAccess::NotReported,
        }
    }
}

/// **Whether a principal may READ one topic**, as one DescribeTopics answer
/// with the caller's authorized operations says it (FX-45). A backup asks
/// before its engine starts, so a principal without `Read` is refused by
/// name instead of failing in the engine with "kafka-backup backup exited 1".
///
/// Three states, as for the cluster: the broker said yes, the broker said
/// no, or nothing is known. "Nothing is known" is never read as either. A
/// fourth, [`TopicRead::Absent`], says the topic is not there at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicRead {
    /// The broker reported the principal's operations on the topic, and READ
    /// is among them.
    Granted,
    /// The broker refused it, and why.
    Denied(ReadDenied),
    /// **The topic does not exist on the cluster** (DUI-5): DescribeTopics
    /// answered `UNKNOWN_TOPIC_OR_PART` for the name, and DescribeCluster
    /// reported Describe on the cluster for the same principal, so the answer
    /// is the cluster's own and not a visibility artefact. A backup of it
    /// would fail in the engine with "exited 1" and no cause.
    Absent,
    /// Nothing is known, and why: no set reported (a broker that does not
    /// compute it), a set without DESCRIBE (not a computed answer), the topic
    /// not found by a principal not seen to Describe the cluster, another
    /// per-topic error, no answer for the name, or a reader that does not
    /// ask.
    Unknown(String),
}

/// Why a principal may not READ a topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadDenied {
    /// The broker reported the principal's operations on the topic, DESCRIBE
    /// among them and READ not (it may Describe the topic, and not consume
    /// it). A set without DESCRIBE is not a computed answer, and is unknown.
    OperationsExcludeRead,
    /// The broker refused even to describe the topic to this principal
    /// (`TOPIC_AUTHORIZATION_FAILED`). READ implies DESCRIBE, so it may not
    /// READ it either.
    NotDescribable,
}

impl TopicRead {
    /// Whether the broker refused READ.
    #[must_use]
    pub fn is_denied(&self) -> bool {
        matches!(self, TopicRead::Denied(_))
    }

    /// Whether the broker said the topic does not exist ([`TopicRead::Absent`]).
    #[must_use]
    pub fn is_absent(&self) -> bool {
        matches!(self, TopicRead::Absent)
    }
}

/// Whether any answer is `UNKNOWN_TOPIC_OR_PART`: the one case
/// [`topic_read`] needs the principal's Describe on the cluster for, so a
/// reader asks DescribeCluster only then.
#[must_use]
pub fn needs_cluster_describe(answers: &[crate::topic_ids::TopicAnswer]) -> bool {
    answers.iter().any(|a| {
        matches!(
            a.error,
            Some((crate::topic_ids::UNKNOWN_TOPIC_OR_PARTITION, _))
        )
    })
}

/// **One answer's READ verdict.** The error decides first: an entry with an
/// error carries no operations. `TOPIC_AUTHORIZATION_FAILED` is a refusal;
/// `UNKNOWN_TOPIC_OR_PART` is [`TopicRead::Absent`] only when
/// `describes_cluster` is `Some(true)` (DescribeCluster reported Describe on
/// the cluster for this principal); every other error, and that one without
/// the cluster's Describe, says nothing about READ (FX-45's review rule).
#[must_use]
pub fn topic_read(
    answer: &crate::topic_ids::TopicAnswer,
    describes_cluster: Option<bool>,
) -> TopicRead {
    use crate::topic_ids::{TOPIC_AUTHORIZATION_FAILED, UNKNOWN_TOPIC_OR_PARTITION};
    match (&answer.error, &answer.operations) {
        (Some((TOPIC_AUTHORIZATION_FAILED, _)), _) => TopicRead::Denied(ReadDenied::NotDescribable),
        (Some((UNKNOWN_TOPIC_OR_PARTITION, _)), _) => match describes_cluster {
            Some(true) => TopicRead::Absent,
            Some(false) => TopicRead::Unknown(
                "the broker does not hold the topic, and this principal may not Describe the \
                 cluster, so that is not taken as the topic's absence"
                    .to_string(),
            ),
            None => TopicRead::Unknown(
                "the broker does not hold the topic, and whether this principal may Describe the \
                 cluster was not reported, so that is not taken as the topic's absence"
                    .to_string(),
            ),
        },
        (Some((code, message)), _) => TopicRead::Unknown(format!(
            "DescribeTopics answered error {code} for the topic: {message}"
        )),
        (None, None) => TopicRead::Unknown(
            "the broker reported no authorized operations for the topic (KIP-430), so whether \
             this principal may READ it is known only when the engine fetches"
                .to_string(),
        ),
        (None, Some(ops))
            if ops.contains(&ACL_OPERATION_READ) || ops.contains(&ACL_OPERATION_ALL) =>
        {
            TopicRead::Granted
        }
        // A COMPUTED set for a topic the broker described always holds
        // DESCRIBE: Kafka answers an error-free entry only to a principal that
        // may Describe the topic, and authorizes every topic operation to
        // build the set. So READ is refused only beside DESCRIBE.
        (None, Some(ops)) if ops.contains(&ACL_OPERATION_DESCRIBE) => {
            TopicRead::Denied(ReadDenied::OperationsExcludeRead)
        }
        // A set without DESCRIBE (empty included) is not a computed answer:
        // an endpoint that writes 0 where KIP-430 says INT32_MIN ("not
        // computed") would otherwise have every backup refused for a grant
        // the principal holds.
        (None, Some(_)) => TopicRead::Unknown(
            "the broker described the topic and reported no Describe on it, so its set is not \
             a computed answer"
                .to_string(),
        ),
    }
}

/// **Every requested name gets exactly one READ verdict**, in request order,
/// as [`crate::topic_ids::join`] does for IDs: a name with no answer, or two,
/// is [`TopicRead::Unknown`], never a grant and never a refusal.
/// `describes_cluster` is [`topic_read`]'s.
#[must_use]
pub fn join_reads(
    requested: &[String],
    answers: &[crate::topic_ids::TopicAnswer],
    describes_cluster: Option<bool>,
) -> Vec<(String, TopicRead)> {
    requested
        .iter()
        .map(|name| {
            let mut matching = answers
                .iter()
                .filter(|a| a.name.as_deref() == Some(name.as_str()));
            let read = match (matching.next(), matching.next()) {
                (Some(answer), None) => topic_read(answer, describes_cluster),
                (None, _) => {
                    TopicRead::Unknown("DescribeTopics returned no answer for the topic".into())
                }
                (Some(_), Some(_)) => TopicRead::Unknown(
                    "DescribeTopics answered for the topic more than once".into(),
                ),
            };
            (name.clone(), read)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_ids::TopicAnswer;

    /// [`topic_read`] for a principal DescribeCluster reported Describe on
    /// the cluster for: every FX-45 arm but the not-found one is unchanged by
    /// it.
    fn topic_read_described(a: &TopicAnswer) -> TopicRead {
        topic_read(a, Some(true))
    }

    fn answer(name: &str, error: Option<i32>, ops: Option<Vec<i32>>) -> TopicAnswer {
        TopicAnswer {
            name: Some(name.to_string()),
            halves: Some((1, 2)),
            error: error.map(|c| (c, format!("code {c}"))),
            operations: ops,
        }
    }

    /// FX-45's decision, each arm. Mutants killed: reading "not reported" as
    /// a refusal (every backup against a broker that does not compute the set
    /// would be refused) or as a grant; reading DESCRIBE as READ; reading a
    /// not-describable topic as unknown (the measured `acl` case); reading a
    /// set without DESCRIBE (an empty one, or `[10]`) as a refusal (review
    /// M1: not a computed answer); letting a not-found topic read as refused.
    #[test]
    fn read_is_granted_only_when_reported_and_refused_only_when_seen() {
        // The measured sets: no authorizer gives every topic operation;
        // `Read` alone implies `Describe`; `Describe` alone is not `Read`.
        assert_eq!(
            topic_read_described(&answer("t", None, Some(vec![3, 4, 5, 6, 7, 8, 10, 11]))),
            TopicRead::Granted
        );
        assert_eq!(
            topic_read_described(&answer("t", None, Some(vec![3, 8]))),
            TopicRead::Granted
        );
        assert_eq!(
            topic_read_described(&answer("t", None, Some(vec![8, 10]))),
            TopicRead::Denied(ReadDenied::OperationsExcludeRead)
        );
        // No DESCRIBE in the set: not a computed answer (review M1).
        for set in [vec![], vec![10]] {
            assert!(
                matches!(
                    topic_read_described(&answer("t", None, Some(set.clone()))),
                    TopicRead::Unknown(m) if m.contains("reported no Describe on it")
                ),
                "{set:?}"
            );
        }
        assert_eq!(
            topic_read_described(&answer("t", Some(29), None)),
            TopicRead::Denied(ReadDenied::NotDescribable)
        );
        assert!(
            matches!(topic_read_described(&answer("t", None, None)), TopicRead::Unknown(m) if m.contains("KIP-430"))
        );
        assert!(
            matches!(topic_read(&answer("t", Some(3), None), None), TopicRead::Unknown(m) if m.contains("does not hold"))
        );
        assert!(
            matches!(topic_read_described(&answer("t", Some(5), Some(vec![3]))), TopicRead::Unknown(m) if m.contains("error 5"))
        );
        assert_eq!(
            topic_read_described(&answer("t", None, Some(vec![2]))),
            TopicRead::Granted
        );
    }

    #[test]
    fn every_requested_name_gets_one_read_verdict() {
        let requested = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let answers = vec![
            answer("c", Some(29), None),
            answer("a", None, Some(vec![3, 8])),
            answer("z", None, Some(vec![8])),
        ];
        let got = join_reads(&requested, &answers, Some(true));
        assert_eq!(got[0], ("a".to_string(), TopicRead::Granted));
        assert!(matches!(&got[1].1, TopicRead::Unknown(m) if m.contains("no answer")));
        assert_eq!(
            got[2],
            (
                "c".to_string(),
                TopicRead::Denied(ReadDenied::NotDescribable)
            )
        );
        let twice = join_reads(
            &["a".to_string()],
            &[
                answer("a", None, Some(vec![8])),
                answer("a", None, Some(vec![8])),
            ],
            Some(true),
        );
        assert!(matches!(&twice[0].1, TopicRead::Unknown(m) if m.contains("more than once")));
    }

    /// Mutant killed: reading "not reported" as an empty set (a refusal), or
    /// an empty set as "unknown".
    #[test]
    fn an_empty_set_is_a_refusal_and_no_set_is_unknown() {
        assert_eq!(
            ClusterAccess::from_operations(Some(vec![])).describe(),
            Some(false)
        );
        assert_eq!(ClusterAccess::from_operations(None).describe(), None);
        assert_eq!(ClusterAccess::Unread("timed out".into()).describe(), None);
        // The measured sets: no authorizer gives every operation; the
        // control principal was allowed Describe alone.
        let all = vec![3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
        assert_eq!(
            ClusterAccess::from_operations(Some(all)).describe(),
            Some(true)
        );
        assert_eq!(ClusterAccess::Reported(vec![8]).describe(), Some(true));
        // DescribeConfigs (10) and ClusterAction (9) are not Describe.
        assert_eq!(ClusterAccess::Reported(vec![9, 10]).describe(), Some(false));
    }

    /// **DUI-5: a topic the broker does not hold is ABSENT only for a
    /// principal that may Describe the cluster.** Then the backup is refused
    /// by name before the engine. Without the cluster's Describe (refused, or
    /// not reported) it stays unknown, as every other describe failure does
    /// (FX-45's review rule), and the engine's own fetch answers.
    ///
    /// KILLS: absence without the cluster's Describe; another error code
    /// read as absence; DescribeCluster asked when no answer needs it.
    #[test]
    fn dui5_a_missing_topic_is_absent_only_when_the_principal_describes_the_cluster() {
        let missing = answer("nosuch", Some(3), None);
        assert_eq!(topic_read(&missing, Some(true)), TopicRead::Absent);
        assert!(TopicRead::Absent.is_absent() && !TopicRead::Absent.is_denied());
        for (describes, why) in [
            (Some(false), "may not Describe the cluster"),
            (None, "was not reported"),
        ] {
            assert!(
                matches!(topic_read(&missing, describes), TopicRead::Unknown(m)
                    if m.contains("does not hold") && m.contains(why)),
                "{describes:?}"
            );
        }
        // Every other per-topic error stays unknown WITH the cluster's
        // Describe (a leader change, a timeout, an unknown code).
        for code in [5, 6, 7, 41, 100] {
            assert!(
                matches!(
                    topic_read(&answer("t", Some(code), None), Some(true)),
                    TopicRead::Unknown(_)
                ),
                "{code}"
            );
        }
        // One verdict per name, the absent one among them.
        let got = join_reads(
            &["orders".to_string(), "nosuch".to_string()],
            &[
                answer("nosuch", Some(3), None),
                answer("orders", None, Some(vec![3, 8])),
            ],
            Some(true),
        );
        assert_eq!(
            got,
            vec![
                ("orders".to_string(), TopicRead::Granted),
                ("nosuch".to_string(), TopicRead::Absent)
            ]
        );
        // DescribeCluster is needed only when a name answered "not held".
        assert!(needs_cluster_describe(std::slice::from_ref(&missing)));
        assert!(!needs_cluster_describe(&[
            answer("t", Some(29), None),
            answer("u", None, Some(vec![3, 8])),
            answer("v", Some(5), None),
        ]));
    }
}
