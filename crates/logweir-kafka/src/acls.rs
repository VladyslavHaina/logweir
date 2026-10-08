//! **PROD-04.0b: the broker's ACL bindings, and what they are worth**
//! (`docs/to-do/decisions/PROD-04.0-admin-path.md` §4 #5, §4.4, §6 T9–T11).
//! Pure: the values arrive from librdkafka through `logweir-rdkafka-ffi`
//! (`acls::describe_acls`, `cluster::describe_cluster`) and from the safe
//! DescribeConfigs ([`crate::reader::ClusterReader::broker_configs`]), and
//! every rule here is unit-tested with no broker. PROD-05.3 builds its export
//! on [`AclCapture`].
//!
//! # T9: "0 bindings" means nothing until two positive probes say so
//!
//! librdkafka drops DescribeAcls' top-level error code, so "no authorizer" and
//! "not authorised" both arrive as zero bindings with no error, in every
//! release through 2.15.1 (§3.5). [`acl_coverage`] therefore decides from:
//!
//! 1. the broker's `authorizer.class.name` (safe DescribeConfigs): present and
//!    empty is "no authorizer"; a class outside [`ACL_AUTHORIZERS`] is not an
//!    ACL authorizer; a MISSING key is a refused read (T13), never "disabled";
//! 2. DescribeCluster's authorized operations for the capturing principal:
//!    a REPORTED set without Describe is a refusal; no set is unknown.
//!
//! | authorizer | cluster operations | DescribeAcls | coverage |
//! |---|---|---|---|
//! | present, `""` | any | any | `aclsNotApplicable: authorizerDisabled` |
//! | a class outside the allowlist | any | any | `unverified: authorizerNotAclBased` |
//! | anything else | reported, without Describe | any | `captureDenied` |
//! | missing or unread | reported with Describe, not reported, or unread | any | `unverified` |
//! | an allowlisted class | not reported or unread | any | `unverified` |
//! | an allowlisted class | reported with Describe | failed | `unverified: aclsUnread` |
//! | an allowlisted class | reported with Describe | answered | `captured` |
//!
//! `captureDenied` outranks a missing authorizer key: both describe the
//! principal of §3.8 (refused DescribeConfigs AND no cluster operations), and
//! the reported refusal is the positive fact (AP-05.3-2); a missing key with
//! Describe allowed stays `unverified` (AP-05.3-1). Even `captured` claims
//! only "the bindings this broker returned to this principal".
//!
//! # T10, T11: what librdkafka cannot name is counted, never exported
//!
//! librdkafka clamps a resource type above TransactionalId (DelegationToken,
//! User) and an operation above IdempotentWrite (CreateTokens, DescribeTokens,
//! TwoPhaseCommit) to Unknown, so distinct bindings collapse into identical
//! ones (§3.5). Such a binding is [`NotRepresentable`], with its principal and
//! name, and is counted beside the exported ones. Kafka's CLUSTER resource is
//! librdkafka's BROKER (4) and is named `CLUSTER` here.
use crate::access::ClusterAccess;
use crate::reader::KafkaError;
use std::collections::BTreeMap;

/// The broker configuration key whose value names the authorizer.
pub const AUTHORIZER_CLASS_KEY: &str = "authorizer.class.name";

/// The authorizer classes whose rules ARE ACL bindings (§4.4 item 3): KRaft's
/// and ZooKeeper-mode 3.x's. Any other class gives `authorizerNotAclBased`.
pub const ACL_AUTHORIZERS: [&str; 2] = [
    "org.apache.kafka.metadata.authorizer.StandardAuthorizer",
    "kafka.security.authorizer.AclAuthorizer",
];

/// librdkafka's ACL enums, as integers (T12).
pub mod code {
    /// `rd_kafka_ResourceType_t`: TOPIC.
    pub const RESOURCE_TOPIC: u32 = 2;
    /// GROUP.
    pub const RESOURCE_GROUP: u32 = 3;
    /// BROKER, which is Kafka's CLUSTER (T11).
    pub const RESOURCE_BROKER: u32 = 4;
    /// TRANSACTIONAL_ID.
    pub const RESOURCE_TRANSACTIONAL_ID: u32 = 5;
    /// `rd_kafka_ResourcePatternType_t`: LITERAL.
    pub const PATTERN_LITERAL: u32 = 3;
    /// PREFIXED.
    pub const PATTERN_PREFIXED: u32 = 4;
    /// `rd_kafka_AclPermissionType_t`: DENY.
    pub const PERMISSION_DENY: u32 = 2;
    /// ALLOW.
    pub const PERMISSION_ALLOW: u32 = 3;
}

/// A resource type librdkafka can name in a binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ResourceType {
    /// TOPIC.
    Topic,
    /// GROUP.
    Group,
    /// Kafka's CLUSTER, which librdkafka calls BROKER (T11).
    Cluster,
    /// TRANSACTIONAL_ID.
    TransactionalId,
}

impl ResourceType {
    fn from_raw(raw: u32) -> Option<ResourceType> {
        match raw {
            code::RESOURCE_TOPIC => Some(ResourceType::Topic),
            code::RESOURCE_GROUP => Some(ResourceType::Group),
            code::RESOURCE_BROKER => Some(ResourceType::Cluster),
            code::RESOURCE_TRANSACTIONAL_ID => Some(ResourceType::TransactionalId),
            _ => None,
        }
    }

    /// Kafka's name, as `kafka-acls.sh --list` prints it.
    #[must_use]
    pub const fn kafka_name(self) -> &'static str {
        match self {
            ResourceType::Topic => "TOPIC",
            ResourceType::Group => "GROUP",
            ResourceType::Cluster => "CLUSTER",
            ResourceType::TransactionalId => "TRANSACTIONAL_ID",
        }
    }
}

/// A binding's resource pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PatternType {
    /// LITERAL (a name of `*` is the wildcard).
    Literal,
    /// PREFIXED.
    Prefixed,
}

impl PatternType {
    fn from_raw(raw: u32) -> Option<PatternType> {
        match raw {
            code::PATTERN_LITERAL => Some(PatternType::Literal),
            code::PATTERN_PREFIXED => Some(PatternType::Prefixed),
            _ => None,
        }
    }

    /// Kafka's name.
    #[must_use]
    pub const fn kafka_name(self) -> &'static str {
        match self {
            PatternType::Literal => "LITERAL",
            PatternType::Prefixed => "PREFIXED",
        }
    }
}

/// An operation librdkafka can name in a binding (2 ALL … 12
/// IDEMPOTENT_WRITE; 0 Unknown and 1 ANY never are one).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AclOperation {
    /// ALL.
    All,
    /// READ.
    Read,
    /// WRITE.
    Write,
    /// CREATE.
    Create,
    /// DELETE.
    Delete,
    /// ALTER.
    Alter,
    /// DESCRIBE.
    Describe,
    /// CLUSTER_ACTION.
    ClusterAction,
    /// DESCRIBE_CONFIGS.
    DescribeConfigs,
    /// ALTER_CONFIGS.
    AlterConfigs,
    /// IDEMPOTENT_WRITE.
    IdempotentWrite,
}

impl AclOperation {
    fn from_raw(raw: u32) -> Option<AclOperation> {
        Some(match raw {
            2 => AclOperation::All,
            3 => AclOperation::Read,
            4 => AclOperation::Write,
            5 => AclOperation::Create,
            6 => AclOperation::Delete,
            7 => AclOperation::Alter,
            8 => AclOperation::Describe,
            9 => AclOperation::ClusterAction,
            10 => AclOperation::DescribeConfigs,
            11 => AclOperation::AlterConfigs,
            12 => AclOperation::IdempotentWrite,
            _ => return None,
        })
    }

    /// Kafka's name.
    #[must_use]
    pub const fn kafka_name(self) -> &'static str {
        match self {
            AclOperation::All => "ALL",
            AclOperation::Read => "READ",
            AclOperation::Write => "WRITE",
            AclOperation::Create => "CREATE",
            AclOperation::Delete => "DELETE",
            AclOperation::Alter => "ALTER",
            AclOperation::Describe => "DESCRIBE",
            AclOperation::ClusterAction => "CLUSTER_ACTION",
            AclOperation::DescribeConfigs => "DESCRIBE_CONFIGS",
            AclOperation::AlterConfigs => "ALTER_CONFIGS",
            AclOperation::IdempotentWrite => "IDEMPOTENT_WRITE",
        }
    }
}

/// ALLOW or DENY.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Permission {
    /// ALLOW.
    Allow,
    /// DENY.
    Deny,
}

impl Permission {
    fn from_raw(raw: u32) -> Option<Permission> {
        match raw {
            code::PERMISSION_ALLOW => Some(Permission::Allow),
            code::PERMISSION_DENY => Some(Permission::Deny),
            _ => None,
        }
    }

    /// Kafka's name.
    #[must_use]
    pub const fn kafka_name(self) -> &'static str {
        match self {
            Permission::Allow => "ALLOW",
            Permission::Deny => "DENY",
        }
    }
}

/// A binding exactly as the broker holds it: every field named.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AclBinding {
    /// The resource type.
    pub resource_type: ResourceType,
    /// The resource name (`*` for the wildcard, `kafka-cluster` for CLUSTER).
    pub name: String,
    /// The pattern type.
    pub pattern_type: PatternType,
    /// The principal, `User:…`.
    pub principal: String,
    /// The host, `*` for any.
    pub host: String,
    /// The operation.
    pub operation: AclOperation,
    /// ALLOW or DENY.
    pub permission: Permission,
}

/// A binding as librdkafka returned it: the input of [`binding`]. Text that
/// is NULL or not UTF-8 is `None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawAcl {
    /// `rd_kafka_ResourceType_t`.
    pub resource_type: u32,
    /// The resource name.
    pub name: Option<String>,
    /// `rd_kafka_ResourcePatternType_t`.
    pub pattern_type: u32,
    /// The principal.
    pub principal: Option<String>,
    /// The host.
    pub host: Option<String>,
    /// `rd_kafka_AclOperation_t`.
    pub operation: u32,
    /// `rd_kafka_AclPermissionType_t`.
    pub permission: u32,
}

/// A binding librdkafka cannot name exactly (T10): counted, never exported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotRepresentable {
    /// The principal, when it is text.
    pub principal: Option<String>,
    /// The resource name, when it is text.
    pub name: Option<String>,
    /// What librdkafka returned, unchanged.
    pub raw: RawAcl,
    /// Why it cannot be exported.
    pub why: &'static str,
}

/// **One binding, named exactly, or why it cannot be (T10, T11, T12).**
pub fn binding(raw: RawAcl) -> Result<AclBinding, Box<NotRepresentable>> {
    let not = |raw: RawAcl, why: &'static str| {
        Box::new(NotRepresentable {
            principal: raw.principal.clone(),
            name: raw.name.clone(),
            raw,
            why,
        })
    };
    let Some(resource_type) = ResourceType::from_raw(raw.resource_type) else {
        return Err(not(
            raw,
            "a resource type librdkafka cannot name (DelegationToken, User, or one newer than \
             librdkafka 2.12.1): it reports them all as Unknown",
        ));
    };
    let Some(operation) = AclOperation::from_raw(raw.operation) else {
        return Err(not(
            raw,
            "an operation librdkafka cannot name (CreateTokens, DescribeTokens, TwoPhaseCommit, \
             or one newer than librdkafka 2.12.1): it reports them all as Unknown",
        ));
    };
    let Some(pattern_type) = PatternType::from_raw(raw.pattern_type) else {
        return Err(not(
            raw,
            "a pattern type that is neither LITERAL nor PREFIXED",
        ));
    };
    let Some(permission) = Permission::from_raw(raw.permission) else {
        return Err(not(raw, "a permission that is neither ALLOW nor DENY"));
    };
    let (Some(name), Some(principal), Some(host)) =
        (raw.name.clone(), raw.principal.clone(), raw.host.clone())
    else {
        return Err(not(raw, "a name, principal or host that is not UTF-8 text"));
    };
    Ok(AclBinding {
        resource_type,
        name,
        pattern_type,
        principal,
        host,
        operation,
        permission,
    })
}

/// What the broker's configuration says about its authorizer (T9's first
/// probe, read through the safe DescribeConfigs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorizerProbe {
    /// `authorizer.class.name` is present and empty: no authorizer.
    Disabled,
    /// `authorizer.class.name` names a class.
    Class(String),
    /// The read answered without the key. A refused DescribeConfigs reads this
    /// way through rust-rdkafka (T13): never "disabled".
    Missing,
    /// The read failed, or was refused and said so.
    Unread(String),
}

impl AuthorizerProbe {
    /// The probe from [`crate::reader::ClusterReader::broker_configs`]'s answer.
    #[must_use]
    pub fn from_broker_configs(
        configs: &Result<BTreeMap<String, String>, KafkaError>,
    ) -> AuthorizerProbe {
        match configs {
            Err(e) => AuthorizerProbe::Unread(e.to_string()),
            Ok(map) => match map.get(AUTHORIZER_CLASS_KEY) {
                None => AuthorizerProbe::Missing,
                Some(v) if v.trim().is_empty() => AuthorizerProbe::Disabled,
                Some(v) => AuthorizerProbe::Class(v.trim().to_string()),
            },
        }
    }
}

/// Why ACL coverage is not `captured`, though bindings may have been read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unverified {
    /// The authorizer is not one whose rules are ACL bindings (AP-05.3-5).
    AuthorizerNotAclBased(String),
    /// The broker configuration answered without `authorizer.class.name`.
    AuthorizerMissing,
    /// The broker configuration could not be read.
    AuthorizerUnread(String),
    /// DescribeCluster reported no operations for the principal.
    ClusterOperationsNotReported,
    /// DescribeCluster failed.
    ClusterOperationsUnread(String),
    /// DescribeAcls failed.
    AclsUnread(String),
}

/// What an ACL capture's bindings are worth.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AclCoverage {
    /// Every guard passed: these are the bindings this broker returned to this
    /// principal (never "the access this cluster allows").
    Captured,
    /// No authorizer: ACLs do not apply (`aclsNotApplicable: authorizerDisabled`).
    AuthorizerDisabled,
    /// The broker reported that the principal may not Describe the cluster,
    /// which DescribeAcls needs: an empty answer is a refusal.
    CaptureDenied,
    /// Not verified, with the reason.
    Unverified(Unverified),
}

/// An ACL capture: the coverage, and what DescribeAcls answered (empty when
/// it was not read or failed). Only `Captured` vouches for the set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclCapture {
    /// What the bindings are worth.
    pub coverage: AclCoverage,
    /// Every binding named exactly, sorted.
    pub bindings: Vec<AclBinding>,
    /// Every binding librdkafka could not name, counted (T10).
    pub not_representable: Vec<NotRepresentable>,
}

/// **T9's decision**: what DescribeAcls' answer is worth, from the two
/// positive probes. See the module table.
#[must_use]
pub fn acl_coverage(
    authorizer: &AuthorizerProbe,
    access: &ClusterAccess,
    acls: &Result<Vec<RawAcl>, String>,
) -> AclCapture {
    let (mut bindings, mut not_representable) = (Vec::new(), Vec::new());
    if let Ok(raw) = acls {
        for r in raw {
            match binding(r.clone()) {
                Ok(b) => bindings.push(b),
                Err(n) => not_representable.push(*n),
            }
        }
        bindings.sort();
    }
    let coverage = match (authorizer, access.describe()) {
        (AuthorizerProbe::Disabled, _) => AclCoverage::AuthorizerDisabled,
        (AuthorizerProbe::Class(c), _) if !ACL_AUTHORIZERS.contains(&c.as_str()) => {
            AclCoverage::Unverified(Unverified::AuthorizerNotAclBased(c.clone()))
        }
        (_, Some(false)) => AclCoverage::CaptureDenied,
        (AuthorizerProbe::Missing, _) => AclCoverage::Unverified(Unverified::AuthorizerMissing),
        (AuthorizerProbe::Unread(e), _) => {
            AclCoverage::Unverified(Unverified::AuthorizerUnread(e.clone()))
        }
        (AuthorizerProbe::Class(_), None) => AclCoverage::Unverified(match access {
            ClusterAccess::Unread(e) => Unverified::ClusterOperationsUnread(e.clone()),
            _ => Unverified::ClusterOperationsNotReported,
        }),
        (AuthorizerProbe::Class(_), Some(true)) => match acls {
            Err(e) => AclCoverage::Unverified(Unverified::AclsUnread(e.clone())),
            Ok(_) => AclCoverage::Captured,
        },
    };
    // With no authorizer, or a refusal, the answer is not a binding set.
    if matches!(
        coverage,
        AclCoverage::AuthorizerDisabled | AclCoverage::CaptureDenied
    ) {
        bindings.clear();
        not_representable.clear();
    }
    AclCapture {
        coverage,
        bindings,
        not_representable,
    }
}

#[cfg(test)]
mod tests {
    //! One row per trap and acceptance row; each names the mutant it kills.
    use super::*;

    const STANDARD: &str = "org.apache.kafka.metadata.authorizer.StandardAuthorizer";

    fn raw(restype: u32, name: &str, pattern: u32, principal: &str, op: u32, perm: u32) -> RawAcl {
        RawAcl {
            resource_type: restype,
            name: Some(name.to_string()),
            pattern_type: pattern,
            principal: Some(principal.to_string()),
            host: Some("*".to_string()),
            operation: op,
            permission: perm,
        }
    }

    fn configs(v: &[(&str, &str)]) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(v.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect())
    }

    fn every_op() -> ClusterAccess {
        ClusterAccess::Reported(vec![3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
    }

    /// AP-05.3-1. Mutants killed: trusting DescribeAcls' 0 bindings with no
    /// authorizer (`captured, 0`); reading a MISSING key as disabled.
    #[test]
    fn no_authorizer_is_decided_from_the_configuration_never_from_describe_acls() {
        let probe =
            AuthorizerProbe::from_broker_configs(&configs(&[("authorizer.class.name", "")]));
        assert_eq!(probe, AuthorizerProbe::Disabled);
        let c = acl_coverage(&probe, &every_op(), &Ok(vec![]));
        assert_eq!(c.coverage, AclCoverage::AuthorizerDisabled);
        assert!(c.bindings.is_empty());
        // A refused DescribeConfigs reads as an answer without the key (T13).
        let missing = AuthorizerProbe::from_broker_configs(&configs(&[("log.dirs", "/x")]));
        assert_eq!(missing, AuthorizerProbe::Missing);
        let c = acl_coverage(&missing, &ClusterAccess::Reported(vec![8]), &Ok(vec![]));
        assert_eq!(
            c.coverage,
            AclCoverage::Unverified(Unverified::AuthorizerMissing)
        );
        let refused = AuthorizerProbe::from_broker_configs(&Err(KafkaError::NotAuthorized(
            "broker 1".into(),
        )));
        assert!(matches!(refused, AuthorizerProbe::Unread(_)));
        let c = acl_coverage(&refused, &ClusterAccess::Reported(vec![8]), &Ok(vec![]));
        assert!(matches!(
            c.coverage,
            AclCoverage::Unverified(Unverified::AuthorizerUnread(_))
        ));
    }

    /// AP-05.3-2. Mutants killed: trusting 0 bindings for a principal the
    /// broker reported without Describe; reading "not reported" as a refusal
    /// or as a grant.
    #[test]
    fn a_principal_without_describe_on_the_cluster_is_capture_denied() {
        let standard = AuthorizerProbe::Class(STANDARD.into());
        let c = acl_coverage(&standard, &ClusterAccess::Reported(vec![]), &Ok(vec![]));
        assert_eq!(c.coverage, AclCoverage::CaptureDenied);
        // §3.8's denied principal: DescribeConfigs refused (key missing) AND
        // no cluster operation. The reported refusal is the positive fact.
        let c = acl_coverage(
            &AuthorizerProbe::Missing,
            &ClusterAccess::Reported(vec![]),
            &Ok(vec![]),
        );
        assert_eq!(c.coverage, AclCoverage::CaptureDenied);
        let c = acl_coverage(&standard, &ClusterAccess::NotReported, &Ok(vec![]));
        assert_eq!(
            c.coverage,
            AclCoverage::Unverified(Unverified::ClusterOperationsNotReported)
        );
        let c = acl_coverage(&standard, &ClusterAccess::Unread("t".into()), &Ok(vec![]));
        assert!(matches!(
            c.coverage,
            AclCoverage::Unverified(Unverified::ClusterOperationsUnread(_))
        ));
        // A refusal's answer is never a binding set, even a non-empty one
        // (mutant A8: exporting it).
        let stray = Ok(vec![
            raw(2, "pa-orders", 3, "User:alice", 3, 3),
            raw(0, "User:bob", 3, "User:alice", 0, 3),
        ]);
        for (probe, access) in [
            (&standard, ClusterAccess::Reported(vec![])),
            (&AuthorizerProbe::Disabled, every_op()),
        ] {
            let c = acl_coverage(probe, &access, &stray);
            assert!(
                c.bindings.is_empty() && c.not_representable.is_empty(),
                "{c:?}"
            );
        }
        // The control: Describe granted, the same answer is a capture.
        let one = vec![raw(2, "pa-orders", 3, "User:alice", 3, 3)];
        let c = acl_coverage(&standard, &ClusterAccess::Reported(vec![8]), &Ok(one));
        assert_eq!(c.coverage, AclCoverage::Captured);
        assert_eq!(c.bindings.len(), 1);
        let c = acl_coverage(
            &standard,
            &ClusterAccess::Reported(vec![8]),
            &Err("timed out".into()),
        );
        assert!(matches!(
            c.coverage,
            AclCoverage::Unverified(Unverified::AclsUnread(_))
        ));
    }

    /// AP-05.3-5. Mutant killed: a coverage function that checks only for a
    /// non-empty class.
    #[test]
    fn a_non_acl_authorizer_is_never_captured() {
        let other = AuthorizerProbe::Class("com.example.RbacAuthorizer".into());
        let c = acl_coverage(&other, &every_op(), &Ok(vec![]));
        assert_eq!(
            c.coverage,
            AclCoverage::Unverified(Unverified::AuthorizerNotAclBased(
                "com.example.RbacAuthorizer".into()
            ))
        );
        let zk = AuthorizerProbe::Class("kafka.security.authorizer.AclAuthorizer".into());
        assert_eq!(
            acl_coverage(&zk, &every_op(), &Ok(vec![])).coverage,
            AclCoverage::Captured
        );
    }

    /// T10 and AP-05.3-4. Mutants killed: exporting a clamped Unknown field;
    /// de-duplicating two collapsed bindings into one count.
    #[test]
    fn what_librdkafka_cannot_name_is_counted_and_never_exported() {
        let standard = AuthorizerProbe::Class(STANDARD.into());
        let collapsed = raw(0, "User:bob", 3, "User:alice", 0, 3);
        let answer = Ok(vec![
            collapsed.clone(),
            collapsed,
            raw(0, "tok1", 3, "User:alice", 8, 3),
            raw(5, "pa-2pc", 3, "User:alice", 0, 3),
            raw(2, "pa-orders", 3, "User:alice", 3, 3),
        ]);
        let c = acl_coverage(&standard, &every_op(), &answer);
        assert_eq!(c.coverage, AclCoverage::Captured);
        assert_eq!(c.bindings.len(), 1);
        assert_eq!(c.not_representable.len(), 4);
        assert_eq!(
            c.not_representable[0].principal.as_deref(),
            Some("User:alice")
        );
        assert_eq!(c.not_representable[0].name.as_deref(), Some("User:bob"));
        assert!(c.not_representable[3].why.contains("operation"));
    }

    /// T11 and AP-05.3-3. Mutants killed: naming resource type 4 BROKER;
    /// dropping the pattern type (a PREFIXED binding read as LITERAL).
    #[test]
    fn cluster_is_named_cluster_and_every_field_round_trips() {
        let b = binding(raw(4, "kafka-cluster", 3, "User:ops", 8, 3)).expect("named");
        assert_eq!(b.resource_type, ResourceType::Cluster);
        assert_eq!(b.resource_type.kafka_name(), "CLUSTER");
        let p = binding(raw(2, "pa-", 4, "User:*", 4, 2)).expect("named");
        assert_eq!(
            (
                p.resource_type.kafka_name(),
                p.name.as_str(),
                p.pattern_type.kafka_name(),
                p.principal.as_str(),
                p.operation.kafka_name(),
                p.permission.kafka_name()
            ),
            ("TOPIC", "pa-", "PREFIXED", "User:*", "WRITE", "DENY")
        );
        let names: Vec<&str> = (2..=12)
            .map(|op| AclOperation::from_raw(op).expect("named").kafka_name())
            .collect();
        assert_eq!(
            names,
            [
                "ALL",
                "READ",
                "WRITE",
                "CREATE",
                "DELETE",
                "ALTER",
                "DESCRIBE",
                "CLUSTER_ACTION",
                "DESCRIBE_CONFIGS",
                "ALTER_CONFIGS",
                "IDEMPOTENT_WRITE"
            ]
        );
        assert_eq!(
            binding(raw(3, "g", 3, "User:a", 3, 3))
                .expect("g")
                .resource_type
                .kafka_name(),
            "GROUP"
        );
        assert_eq!(
            binding(raw(5, "t", 3, "User:a", 4, 3))
                .expect("t")
                .resource_type
                .kafka_name(),
            "TRANSACTIONAL_ID"
        );
    }

    /// T12's class at this layer. Mutant killed: a mapping that assumes the
    /// integers stay inside librdkafka's enums (a future value, ANY, or
    /// Unknown) or that text is present.
    #[test]
    fn integers_outside_the_enums_and_absent_text_are_not_representable() {
        for r in [
            raw(1, "x", 3, "User:a", 3, 3),
            raw(6, "x", 3, "User:a", 3, 3),
            raw(u32::MAX, "x", 3, "User:a", 3, 3),
            raw(2, "x", 3, "User:a", 1, 3),
            raw(2, "x", 3, "User:a", 13, 3),
            raw(2, "x", 0, "User:a", 3, 3),
            raw(2, "x", 2, "User:a", 3, 3),
            raw(2, "x", 5, "User:a", 3, 3),
            raw(2, "x", 3, "User:a", 3, 1),
            raw(2, "x", 3, "User:a", 3, 4),
        ] {
            assert!(binding(r.clone()).is_err(), "{r:?}");
        }
        let mut no_host = raw(2, "x", 3, "User:a", 3, 3);
        no_host.host = None;
        assert!(binding(no_host).is_err());
    }
}
