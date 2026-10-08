//! **PROD-04.0b: what DescribeCluster says the capturing principal may do on
//! the cluster** (KIP-430's authorized operations). Pure: the broker glue is
//! `crate::rdkafka_admin` (feature `client`), through
//! `logweir-rdkafka-ffi::cluster::describe_cluster`.
//!
//! Two decisions read it (`docs/to-do/decisions/PROD-04.0-admin-path.md`):
//!
//! - **T14, listing completeness.** ListGroups shows a principal without
//!   Describe on the cluster only the groups it may Describe, silently. With
//!   Describe on the cluster the listing is unfiltered (§3.9).
//! - **T9, the second positive probe.** DescribeAcls answers "0 bindings, no
//!   error" both with no authorizer and to a principal it refuses; Describe on
//!   the cluster is what DescribeAcls itself needs (§3.8).
//!
//! The answer has THREE states, and the first two must never be confused:
//! the broker REPORTED a set (possibly empty: a refusal), the broker reported
//! NOTHING (librdkafka's NULL: not requested, or a broker that does not
//! compute it), or the call failed.

/// `RD_KAFKA_ACL_OPERATION_DESCRIBE`, and Kafka's `AclOperation.DESCRIBE`.
pub const ACL_OPERATION_DESCRIBE: i32 = 8;

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

#[cfg(test)]
mod tests {
    use super::*;

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
        // §3.8's measured sets: no authorizer gives every operation; the
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
}
