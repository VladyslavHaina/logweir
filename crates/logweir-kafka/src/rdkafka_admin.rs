//! **PROD-04.0b's broker glue**: the owned values `logweir-rdkafka-ffi`
//! returns, moved into the pure decisions of [`crate::groups`],
//! [`crate::acls`] and [`crate::access`]. No `unsafe` here: this crate keeps
//! `#![forbid(unsafe_code)]`, and every librdkafka call is the FFI crate's
//! (OD-6 (a2); `scripts/check-unsafe-scope.sh`).
//!
//! The calls are made on the reader's admin client, each bounded by the
//! reader's admin bound ([`RdKafkaReader::with_admin_bound`]), on the calling
//! thread. Nothing here retries: a failed read becomes an `Unread`, a
//! `NoAnswer` or a listing error, and the pure layer turns it into the weaker
//! verdict.
use crate::access::ClusterAccess;
use crate::acls::{acl_coverage, AclCapture, AuthorizerProbe, RawAcl};
use crate::groups::{
    classify_with, describe_answer, selected_ids, CapturableGroup, DescribeAnswer, DescribeFailure,
    DescribedObservation, GroupClassification, GroupDescription, GroupListings, MemberDescription,
    NameEntry, TypedEntry,
};
use crate::positions::TopicPartition;
use crate::rdkafka_reader::RdKafkaReader;
use crate::reader::{ClusterReader, KafkaError};
use logweir_rdkafka_ffi::groups::{AssignedPartition, DescribedGroup, MAX_DESCRIBE_GROUPS};
use logweir_rdkafka_ffi::{acls, cluster, groups, CText, CallError};
use std::collections::BTreeMap;
use std::time::Duration;

/// Text that is an identifier: `Some` only for UTF-8 (never lossy).
fn id(t: CText) -> Option<String> {
    match t {
        CText::Utf8(s) => Some(s),
        CText::Null | CText::NotUtf8(_) => None,
    }
}

/// A whole-call failure as `(code, message)`; `-1` where the failure carries
/// no librdkafka code (refused input, no result within the bound).
fn call_failure(e: &CallError) -> (i32, String) {
    match e {
        CallError::Call { code, message } => (*code, message.display()),
        CallError::Options { code, message } => (*code, message.clone()),
        other => (-1, other.to_string()),
    }
}

fn partitions(list: Vec<AssignedPartition>, unreadable: &mut usize) -> Vec<TopicPartition> {
    let mut out = Vec::with_capacity(list.len());
    for p in list {
        match id(p.topic) {
            Some(topic) => out.push(TopicPartition::new(topic, p.partition)),
            None => *unreadable += 1,
        }
    }
    out
}

fn observation(d: DescribedGroup) -> DescribedObservation {
    let mut unreadable_topics = 0;
    let members = d
        .members
        .into_iter()
        .map(|m| MemberDescription {
            client_id: id(m.client_id),
            consumer_id: id(m.consumer_id),
            group_instance_id: id(m.group_instance_id),
            host: id(m.host),
            assignment: partitions(m.assignment, &mut unreadable_topics),
            target_assignment: m
                .target_assignment
                .map(|t| partitions(t, &mut unreadable_topics)),
        })
        .collect();
    DescribedObservation {
        error: d.error.map(|e| (e.code, e.message.display())),
        state: d.state,
        group_type: d.group_type,
        is_simple: d.is_simple,
        partition_assignor: id(d.partition_assignor),
        coordinator: d.coordinator,
        members,
        unreadable_topics,
    }
}

impl RdKafkaReader {
    /// Sets the bound of every later admin call (DescribeCluster, the two
    /// group listings, descriptions, DescribeAcls): librdkafka's request
    /// timeout, plus the FFI crate's poll margin (default
    /// [`crate::groups::DEFAULT_ADMIN_BOUND`], 15 s).
    ///
    /// # Errors
    ///
    /// [`KafkaError::Client`] outside 1 s..=300 s.
    pub fn with_admin_bound(mut self, bound: Duration) -> Result<Self, KafkaError> {
        if !(logweir_rdkafka_ffi::MIN_TIMEOUT..=logweir_rdkafka_ffi::MAX_TIMEOUT).contains(&bound) {
            return Err(KafkaError::Client(format!(
                "an admin bound of {bound:?} is outside {:?}..={:?}",
                logweir_rdkafka_ffi::MIN_TIMEOUT,
                logweir_rdkafka_ffi::MAX_TIMEOUT
            )));
        }
        self.set_admin_bound(bound);
        Ok(self)
    }

    /// **What DescribeCluster says this principal may do on the cluster**
    /// (KIP-430). Never fails: a failed read is [`ClusterAccess::Unread`].
    pub fn cluster_access(&self) -> ClusterAccess {
        match cluster::describe_cluster(self.admin_client().inner(), self.admin_bound()) {
            Ok(d) => ClusterAccess::from_operations(d.authorized_operations),
            Err(e) => ClusterAccess::Unread(e.to_string()),
        }
    }

    /// **Both group listings and the cluster access, read once**: the input
    /// of every rule in [`crate::groups`]. The name listing is read before the
    /// typed one, so a group created in between can only make the listings
    /// look incomplete (T19's check), never exclude a captured type.
    pub fn group_listings(&self) -> GroupListings {
        let client = self.admin_client().inner();
        let bound = self.admin_bound();
        let access = self.cluster_access();
        let mut unreadable_ids = 0;
        let (names, names_incomplete) = match groups::list_group_names(client, bound) {
            Ok(listing) => {
                let mut names = Vec::with_capacity(listing.groups.len());
                for g in listing.groups {
                    match id(g.group_id) {
                        Some(group_id) => names.push(NameEntry {
                            group_id,
                            error: g.error,
                        }),
                        None => unreadable_ids += 1,
                    }
                }
                let partial = listing
                    .partial
                    .then(|| "rd_kafka_list_groups answered _PARTIAL".to_string());
                (names, partial)
            }
            Err(e) => (Vec::new(), Some(e.to_string())),
        };
        let (typed, typed_errors) = match groups::list_consumer_groups(client, bound) {
            Ok(listing) => {
                let mut typed = Vec::with_capacity(listing.groups.len());
                for g in listing.groups {
                    match id(g.group_id) {
                        Some(group_id) => typed.push(TypedEntry {
                            group_id,
                            is_simple: g.is_simple,
                            state: g.state,
                            group_type: g.group_type,
                        }),
                        None => unreadable_ids += 1,
                    }
                }
                let errors = listing
                    .errors
                    .into_iter()
                    .map(|e| (e.code, e.message.display()))
                    .collect();
                (typed, errors)
            }
            Err(e) => (Vec::new(), vec![call_failure(&e)]),
        };
        GroupListings {
            typed,
            typed_errors,
            names,
            names_incomplete,
            access,
            unreadable_ids,
        }
    }

    /// Describes `ids`, at most [`MAX_DESCRIBE_GROUPS`] per call, keyed by the
    /// id each answer names; an id with no answer is absent from the map.
    fn describe_ids(&self, ids: &[String]) -> BTreeMap<String, Result<DescribedGroup, String>> {
        let client = self.admin_client().inner();
        let mut out = BTreeMap::new();
        for chunk in ids.chunks(MAX_DESCRIBE_GROUPS) {
            let refs: Vec<&str> = chunk.iter().map(String::as_str).collect();
            match groups::describe_consumer_groups(client, &refs, self.admin_bound()) {
                Ok(answers) => {
                    for d in answers {
                        if let CText::Utf8(g) = &d.group_id {
                            out.insert(g.clone(), Ok(d));
                        }
                    }
                }
                Err(e) => {
                    for g in chunk {
                        out.insert(g.clone(), Err(e.to_string()));
                    }
                }
            }
        }
        out
    }

    /// **PROD-04.0 §5 for every selected id**: one verdict each, from both
    /// listings, DescribeCluster, and a targeted describe of every id no
    /// listing shows when the listings are not complete (T14). See
    /// [`crate::groups`] for the rules.
    ///
    /// # Errors
    ///
    /// [`KafkaError::Client`] for a blank selected id, before any call. Every
    /// broker-side failure is a verdict, never an `Err`.
    pub fn classify_groups(&self, selected: &[String]) -> Result<GroupClassification, KafkaError> {
        // Refused before any call: a blank id.
        selected_ids(selected).map_err(KafkaError::Client)?;
        let listings = self.group_listings();
        // The whole decision is `classify_with`'s, pure and unit-tested; this
        // only supplies the describe call (PROD-04.0b review L3).
        classify_with(&listings, selected, |ids| {
            self.describe_ids(ids)
                .into_iter()
                .map(|(g, answer)| {
                    let answer = match answer {
                        Err(e) => DescribeAnswer::CallFailed(e),
                        Ok(d) => DescribeAnswer::Answered {
                            error: d.error.map(|e| (e.code, e.message.display())),
                            state: d.state,
                            group_type: d.group_type,
                            members: d.members.len(),
                        },
                    };
                    (g, answer)
                })
                .collect()
        })
        .map_err(KafkaError::Client)
    }

    /// **The description of each captured group**, taken only when it agrees
    /// with the listing that classified it (T2). One entry per group, in
    /// order.
    pub fn describe_groups(
        &self,
        groups: &[CapturableGroup],
    ) -> Vec<(String, Result<GroupDescription, DescribeFailure>)> {
        let mut seen = std::collections::BTreeSet::new();
        let ids: Vec<String> = groups
            .iter()
            .map(|g| g.group_id().to_string())
            .filter(|g| seen.insert(g.clone()))
            .collect();
        let answers = self.describe_ids(&ids);
        groups
            .iter()
            .map(|g| {
                let answer = match answers.get(g.group_id()) {
                    None => describe_answer(g, None),
                    Some(Err(e)) => Err(DescribeFailure::Failed {
                        code: None,
                        message: e.clone(),
                    }),
                    Some(Ok(d)) => describe_answer(g, Some(&observation(d.clone()))),
                };
                (g.group_id().to_string(), answer)
            })
            .collect()
    }

    /// **Every ACL binding the broker returns to this principal, and what
    /// they are worth** (T9: the authorizer's class through the safe
    /// DescribeConfigs, then DescribeCluster's operations, then DescribeAcls;
    /// T10, T11). See [`crate::acls`].
    pub fn capture_acls(&self) -> AclCapture {
        let authorizer = AuthorizerProbe::from_broker_configs(&self.broker_configs());
        let access = self.cluster_access();
        let answer = acls::describe_acls(self.admin_client().inner(), self.admin_bound())
            .map(|bindings| {
                bindings
                    .into_iter()
                    .map(|b| RawAcl {
                        resource_type: b.resource_type,
                        name: id(b.name),
                        pattern_type: b.pattern_type,
                        principal: id(b.principal),
                        host: id(b.host),
                        operation: b.operation,
                        permission: b.permission,
                    })
                    .collect()
            })
            .map_err(|e| e.to_string());
        acl_coverage(&authorizer, &access, &answer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logweir_rdkafka_ffi::groups::GroupMember;
    use logweir_rdkafka_ffi::RawError;

    /// T18's class at the glue: an id that is not UTF-8 is never matched and
    /// never converted; a topic that is not UTF-8 is counted, so the
    /// description is refused rather than silently missing a partition.
    #[test]
    fn text_that_is_not_utf8_is_withheld_and_counted() {
        assert_eq!(id(CText::Utf8("g".into())), Some("g".into()));
        assert_eq!(id(CText::NotUtf8(vec![0xff])), None);
        assert_eq!(id(CText::Null), None);
        let d = DescribedGroup {
            group_id: CText::Utf8("g".into()),
            error: Some(RawError {
                code: 30,
                name: CText::Utf8("GROUP_AUTHORIZATION_FAILED".into()),
                message: CText::Utf8("Broker: Group authorization failed".into()),
            }),
            is_simple: false,
            partition_assignor: CText::NotUtf8(vec![0xfe]),
            state: 3,
            group_type: 2,
            coordinator: Some(1),
            members: vec![GroupMember {
                client_id: CText::Utf8("c".into()),
                consumer_id: CText::Null,
                group_instance_id: CText::Null,
                host: CText::Utf8("/h".into()),
                assignment: vec![
                    AssignedPartition {
                        topic: CText::Utf8("t".into()),
                        partition: 0,
                    },
                    AssignedPartition {
                        topic: CText::NotUtf8(vec![0xff]),
                        partition: 1,
                    },
                ],
                target_assignment: None,
            }],
        };
        let o = observation(d);
        assert_eq!(
            o.error,
            Some((30, "Broker: Group authorization failed".into()))
        );
        assert_eq!(o.partition_assignor, None);
        assert_eq!(o.unreadable_topics, 1);
        assert_eq!(o.members[0].assignment, vec![TopicPartition::new("t", 0)]);
        assert_eq!(o.members[0].consumer_id, None);
    }

    #[test]
    fn a_whole_call_failure_keeps_its_integer_code() {
        assert_eq!(
            call_failure(&CallError::Call {
                code: -185,
                message: CText::Utf8("Local: Timed out".into())
            }),
            (-185, "Local: Timed out".to_string())
        );
        assert_eq!(
            call_failure(&CallError::NoResult {
                waited: Duration::from_secs(20)
            })
            .0,
            -1
        );
    }
}
