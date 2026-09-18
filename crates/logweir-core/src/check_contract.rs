//! The PURE contract shared by every Logweir check: the plan a runner is
//! handed, the result frames it prints, the closed code vocabulary both sides
//! name, the redactor every message passes through, the visibility policy and
//! the binding digest (decision D2 §4.1).
//!
//! ONE runner, not two (D-SEAMS S1). `logweir check run` serves topic
//! inventory, operation readiness, restore preflight, destination access and
//! evidence fetch; the controller side (`weirkeeper::check`) and the API read
//! the same frames through the same decoder. Everything here is pure: no I/O,
//! no clock, no entropy (Global Constraint 1, `scripts/check-pure-core.sh`).
//!
//! Two properties this module exists to make provable:
//!
//! 1. **A credential value can never reach a frame, a status or a log.**
//!    [`redact`] is the single chokepoint, its rules are a LIST so each one can
//!    be deleted in a test and observed to leak (`redaction_rules`), and every
//!    message field is capped at [`MESSAGE_MAX_CHARS`].
//! 2. **A successful Kafka listing never means "all topics".** [`visibility`]
//!    can answer `attestedComplete` only from an administrator attestation
//!    that matches the observed cluster id and principal and has not expired
//!    (D-SEAMS S3).

use crate::destination::DestinationRole;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The contract string a check PLAN carries. A runner that does not know it
/// refuses before it builds a client.
pub const CHECK_PLAN_CONTRACT: &str = "logweir.dev/check-plan/v1";
/// The contract string the end frame carries.
pub const CHECK_RESULT_CONTRACT: &str = "logweir.dev/check-result/v1";
/// The value of `--check-contract-version` and `LOGWEIR_CHECK_CONTRACT_VERSION`.
pub const CHECK_CONTRACT_VERSION: u32 = 1;
/// The topic-inventory result format string (`status.result.format`).
pub const TOPIC_INVENTORY_FORMAT: &str = "logweir.dev/topic-inventory/v1";

/// Every frame line, INCLUDING its newline, stays under this. CRI splits a
/// container log line at 16 KiB; staying at 4 KiB keeps a frame whole through
/// the split and through the controller's `LogParams` read.
pub const FRAME_MAX_BYTES: usize = 4096;
/// Base64 characters per `logweir-check-part` frame.
pub const PART_MAX_BASE64_CHARS: usize = 3000;
/// Messages and remedies are capped here, after redaction.
pub const MESSAGE_MAX_CHARS: usize = 512;
/// The default relay budget for topic lines (D2 §5.5): 6 MiB.
pub const DEFAULT_RELAY_BUDGET_BYTES: usize = 6 * 1024 * 1024;
/// `spec.request.expectedTopics` maxItems.
pub const MAX_EXPECTED_TOPICS: usize = 500;
/// `topicInventory` hard ceiling, matching `policy.discovery.hardMaxTopics`.
pub const MAX_TOPICS_CEILING: u32 = 50_000;
/// `operationReadiness` topic cap.
pub const MAX_READINESS_TOPICS: usize = 1_000;
/// `evidenceFetch` object cap.
pub const MAX_EVIDENCE_OBJECTS: usize = 3;
/// `evidenceFetch` payload cap: 1 MiB.
pub const MAX_EVIDENCE_PAYLOAD_BYTES: u64 = 1024 * 1024;
/// `evidenceFetch` sidecar cap: 64 KiB.
pub const MAX_EVIDENCE_SIDECAR_BYTES: u64 = 64 * 1024;
/// A result carries at most this many per-check entries (D2 §6.4).
pub const MAX_CHECK_ENTRIES: usize = 64;

/// The ceiling on [`CheckPlan::timeout_seconds`] for the five probe kinds.
///
/// TEN MINUTES. Every one of them is a bounded probe — a metadata call, a
/// handful of `get`s — and a probe that needs longer is a probe that is not
/// answering.
pub const MAX_CHECK_TIMEOUT_SECONDS: u32 = 600;

/// The ceiling on [`CheckPlan::timeout_seconds`] for a `catalogSync`.
///
/// THIRTY MINUTES, AND IT IS A DIFFERENT NUMBER ON PURPOSE. A catalog sync is
/// not a probe: it is a paged walk of an adopter's own bucket, whose cost is
/// set by how many recovery points they hold and not by how fast one endpoint
/// answers. `weirkeeper::controllers::recovery_catalog::SYNC_TIMEOUT_SECONDS`
/// is 900, which the single 600-second ceiling would have refused at step 4
/// with `CheckContractMismatch` — every sync Job, on every cadence, before a
/// credential was read. `the_controllers_sync_plan_is_one_the_runner_accepts`
/// is the guard that would have caught it.
pub const MAX_CATALOG_SYNC_TIMEOUT_SECONDS: u32 = 1800;

/// `spec.sync.viewLimit`'s range, as the CRD's `schemars(range)` states it.
pub const MIN_CATALOG_VIEW_LIMIT: i64 = 100;
/// See [`MIN_CATALOG_VIEW_LIMIT`].
pub const MAX_CATALOG_VIEW_LIMIT: i64 = 5_000;
/// `spec.sync.maxObjectsPerRun`'s range, as the CRD's `schemars(range)` states
/// it.
pub const MIN_CATALOG_OBJECTS_PER_RUN: i64 = 1_000;
/// See [`MIN_CATALOG_OBJECTS_PER_RUN`].
pub const MAX_CATALOG_OBJECTS_PER_RUN: i64 = 1_000_000;

/// Frame prefixes. Public because `weirkeeper::check::relay` and the runner
/// both write and read them, and a second spelling is a second contract.
pub const TOPIC_FRAME_PREFIX: &str = "logweir-check-topic=";
/// See [`TOPIC_FRAME_PREFIX`].
pub const PART_FRAME_PREFIX: &str = "logweir-check-part=";
/// See [`TOPIC_FRAME_PREFIX`].
pub const END_FRAME_PREFIX: &str = "logweir-check-end=";

// ---------------------------------------------------------------- vocabulary

/// A macro for the two closed vocabularies below.
///
/// Both the code table and the id table are CLOSED on purpose: a controller
/// that could invent a reason string would put an unreviewed value into a
/// `metav1.Condition.reason` (which Kubernetes validates against
/// `^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$`) and into a UI that has no
/// remedy text for it. Generating `as_str`, `FromStr` and `ALL` from ONE list
/// means the three can never disagree — the defect a hand-written second match
/// arm always eventually has.
macro_rules! closed_vocabulary {
    ($(#[$meta:meta])* $name:ident { $($variant:ident => $text:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[non_exhaustive]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            /// The wire spelling.
            #[must_use]
            pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $text),+
                }
            }

            /// Every member, in declaration order.
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];

            /// The wire spelling back to a member. `None` for anything not in
            /// the table — never a fallback member, because "I did not
            /// recognise this" and "this specific thing happened" are two
            /// different facts.
            #[must_use]
            pub fn parse(s: &str) -> Option<Self> {
                match s {
                    $($text => Some(Self::$variant),)+
                    _ => None,
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let s = String::deserialize(d)?;
                Self::parse(&s).ok_or_else(|| {
                    serde::de::Error::custom(format!(
                        concat!("`{}` is not a ", stringify!($name)),
                        s
                    ))
                })
            }
        }
    };
}

closed_vocabulary! {
    /// The CLOSED code vocabulary (D2 §4.2 classification, §4.3 waiting
    /// classification, §6.3 check catalogue, §3.3 destination validation).
    ///
    /// Every spelling is CamelCase so it can be a `metav1.Condition.reason`
    /// verbatim; `every_code_is_a_valid_condition_reason` asserts it.
    CheckCode {
        // -- object store (D2 §4.2) --------------------------------------
        AccessDenied => "AccessDenied",
        InvalidCredentials => "InvalidCredentials",
        BucketNotFound => "BucketNotFound",
        ObjectNotFound => "ObjectNotFound",
        EndpointUnreachable => "EndpointUnreachable",
        TlsTrustFailed => "TlsTrustFailed",
        RegionMismatch => "RegionMismatch",
        Timeout => "Timeout",
        StoreErrorUnclassified => "StoreErrorUnclassified",
        // -- Kafka (D2 §4.2) ---------------------------------------------
        BrokerUnreachable => "BrokerUnreachable",
        AuthenticationFailed => "AuthenticationFailed",
        TlsHandshakeFailed => "TlsHandshakeFailed",
        MetadataTimeout => "MetadataTimeout",
        ClusterAuthorizationFailed => "ClusterAuthorizationFailed",
        TopicAuthorizationFailed => "TopicAuthorizationFailed",
        UnknownTopicOrPartition => "UnknownTopicOrPartition",
        // -- pod / Job waiting classification (D2 §4.3) ------------------
        CredentialSecretNotFound => "CredentialSecretNotFound",
        CredentialSecretKeyMissing => "CredentialSecretKeyMissing",
        TrustBundleNotFound => "TrustBundleNotFound",
        RunnerImagePullFailed => "RunnerImagePullFailed",
        RunnerImageNotPresent => "RunnerImageNotPresent",
        RunnerImageInvalid => "RunnerImageInvalid",
        PodUnschedulable => "PodUnschedulable",
        SigningKeyMissing => "SigningKeyMissing",
        VolumeMountFailed => "VolumeMountFailed",
        RunnerServiceAccountMissing => "RunnerServiceAccountMissing",
        PodCreateRejected => "PodCreateRejected",
        DisruptedMidCheck => "DisruptedMidCheck",
        ForeignPodIgnored => "ForeignPodIgnored",
        // -- ready codes (D2 §6.3) ---------------------------------------
        Resolved => "Resolved",
        Projected => "Projected",
        Authenticated => "Authenticated",
        ClusterIdentityMatches => "ClusterIdentityMatches",
        TopicsDescribable => "TopicsDescribable",
        DestinationValid => "DestinationValid",
        ArchiveListable => "ArchiveListable",
        MarkerWritten => "MarkerWritten",
        MarkerAlreadyPresent => "MarkerAlreadyPresent",
        EvidenceReadable => "EvidenceReadable",
        SignerUsable => "SignerUsable",
        SignerRostered => "SignerRostered",
        ImageAvailable => "ImageAvailable",
        PodStarted => "PodStarted",
        ContractSupported => "ContractSupported",
        PolicyLoaded => "PolicyLoaded",
        PlanParsed => "PlanParsed",
        PlanMatchesReferences => "PlanMatchesReferences",
        MappedNamesLegal => "MappedNamesLegal",
        RecoveryPointSucceeded => "RecoveryPointSucceeded",
        ManifestReadable => "ManifestReadable",
        PointInTimeCovered => "PointInTimeCovered",
        SegmentsPresent => "SegmentsPresent",
        TargetAllowed => "TargetAllowed",
        MarkerHealthy => "MarkerHealthy",
        MappedTopicsAbsent => "MappedTopicsAbsent",
        TopicCreateValidated => "TopicCreateValidated",
        TimestampWithinBound => "TimestampWithinBound",
        ApprovalVerified => "ApprovalVerified",
        ApproverKeyValid => "ApproverKeyValid",
        Valid => "Valid",
        Succeeded => "Succeeded",
        // -- notReady codes (D2 §6.3, §3.3, §3.4) ------------------------
        ConnectionNotFound => "ConnectionNotFound",
        ConnectionInvalid => "ConnectionInvalid",
        CredentialReferenceMissing => "CredentialReferenceMissing",
        ClusterIdentityChanged => "ClusterIdentityChanged",
        SourceIsAllowlistedTarget => "SourceIsAllowlistedTarget",
        TopicNotFound => "TopicNotFound",
        TopicNotAuthorized => "TopicNotAuthorized",
        DestinationNotFound => "DestinationNotFound",
        DestinationNotValid => "DestinationNotValid",
        DestinationRoleNotConfigured => "DestinationRoleNotConfigured",
        ExecutionContextConflict => "ExecutionContextConflict",
        CaBundleUnsupportedByEngine => "CaBundleUnsupportedByEngine",
        CaBundleNotFound => "CaBundleNotFound",
        CaBundleKeyMissing => "CaBundleKeyMissing",
        CaBundleTooLarge => "CaBundleTooLarge",
        CaBundleInvalid => "CaBundleInvalid",
        AddressingUnsupportedByEngine => "AddressingUnsupportedByEngine",
        ControllerIdentityNotAllowlisted => "ControllerIdentityNotAllowlisted",
        WorkloadIdentityNotInjected => "WorkloadIdentityNotInjected",
        SigningKeyUnreadable => "SigningKeyUnreadable",
        SigningKeyInvalid => "SigningKeyInvalid",
        TrustRosterNotFound => "TrustRosterNotFound",
        TrustRosterNotLoaded => "TrustRosterNotLoaded",
        SignerNotRostered => "SignerNotRostered",
        SignerKeyExpired => "SignerKeyExpired",
        RunnerContractUnsupported => "RunnerContractUnsupported",
        PolicyUnreadable => "PolicyUnreadable",
        PlanUnparseable => "PlanUnparseable",
        PlanHashMismatch => "PlanHashMismatch",
        PlanDestinationMismatch => "PlanDestinationMismatch",
        PlanEvidenceDestinationMismatch => "PlanEvidenceDestinationMismatch",
        PlanTargetMismatch => "PlanTargetMismatch",
        PlanTopicsNotInRecoveryPoint => "PlanTopicsNotInRecoveryPoint",
        MappedTopicNameIllegal => "MappedTopicNameIllegal",
        TopicMappingIdentity => "TopicMappingIdentity",
        GlobInTopic => "GlobInTopic",
        ExpansionInTopic => "ExpansionInTopic",
        RecoveryPointNotFound => "RecoveryPointNotFound",
        RecoveryPointNotSucceeded => "RecoveryPointNotSucceeded",
        RecoveryPointUidChanged => "RecoveryPointUidChanged",
        RecoveryPointLocationMismatch => "RecoveryPointLocationMismatch",
        BackupSetNotFound => "BackupSetNotFound",
        ManifestUnreadable => "ManifestUnreadable",
        PointInTimeBeforeCoverage => "PointInTimeBeforeCoverage",
        PointInTimeAfterCoverage => "PointInTimeAfterCoverage",
        TopicNotInBackupSet => "TopicNotInBackupSet",
        SegmentMissing => "SegmentMissing",
        TargetNotAllowlisted => "TargetNotAllowlisted",
        TargetEqualsSource => "TargetEqualsSource",
        MarkerTopicMissing => "MarkerTopicMissing",
        MarkerTopicErrored => "MarkerTopicErrored",
        MappedTopicExists => "MappedTopicExists",
        TopicCreateNotAuthorized => "TopicCreateNotAuthorized",
        TopicConfigRejected => "TopicConfigRejected",
        ReplicationFactorExceedsBrokers => "ReplicationFactorExceedsBrokers",
        TimestampBoundExceeded => "TimestampBoundExceeded",
        ApprovalNotVerified => "ApprovalNotVerified",
        ApprovalExpired => "ApprovalExpired",
        ApprovalPlanMismatch => "ApprovalPlanMismatch",
        ApprovalSubjectMismatch => "ApprovalSubjectMismatch",
        ApproverKeyExpiresBeforeDeadline => "ApproverKeyExpiresBeforeDeadline",
        ArchiveUrlUnreadable => "ArchiveUrlUnreadable",
        NotReady => "NotReady",
        // -- unknown / execution-only codes (D2 §6.3) --------------------
        PodNotStarted => "PodNotStarted",
        BlockedByPrerequisite => "BlockedByPrerequisite",
        ClusterIdentityNotObserved => "ClusterIdentityNotObserved",
        TopicVisibilityUnknown => "TopicVisibilityUnknown",
        WriteNotProbed => "WriteNotProbed",
        EvidenceReadNotConfigured => "EvidenceReadNotConfigured",
        SignerKeyIdNotObserved => "SignerKeyIdNotObserved",
        ReadVerifiedOnlyAtExecution => "ReadVerifiedOnlyAtExecution",
        ArchivePrefixWriteVerifiedOnlyAtExecution => "ArchivePrefixWriteVerifiedOnlyAtExecution",
        NetworkPolicyEnforcementNotObservable => "NetworkPolicyEnforcementNotObservable",
        SegmentListTooLarge => "SegmentListTooLarge",
        MappedTopicVisibilityUnknown => "MappedTopicVisibilityUnknown",
        TopicCreateValidationUnsupported => "TopicCreateValidationUnsupported",
        BrokerConfigsNotReadable => "BrokerConfigsNotReadable",
        LogAppendTimeOverrideVerifiedOnlyAtExecution => "LogAppendTimeOverrideVerifiedOnlyAtExecution",
        ApprovalPending => "ApprovalPending",
        SubjectNotCreated => "SubjectNotCreated",
        // -- framework / phase codes (D2 §4.2, §4.3, §5.1, §6.2) ---------
        CheckContractMismatch => "CheckContractMismatch",
        ResultUnreadable => "ResultUnreadable",
        DeadlineExceeded => "DeadlineExceeded",
        CancelRequested => "CancelRequested",
        Stalled => "Stalled",
        ConcurrencyLimited => "ConcurrencyLimited",
        CheckPlanConflict => "CheckPlanConflict",
        ResultStorageConflict => "ResultStorageConflict",
    }
}

closed_vocabulary! {
    /// The CLOSED check-id vocabulary (D2 §6.3). Dotted `category.name`, so
    /// [`CheckId::category`] is the part before the dot and needs no second
    /// table.
    CheckId {
        ConnectionResolved => "connection.resolved",
        ConnectionCredentialProjected => "connection.credentialProjected",
        ConnectionAuthenticated => "connection.authenticated",
        ConnectionClusterIdentity => "connection.clusterIdentity",
        ConnectionTopicsDescribable => "connection.topicsDescribable",
        ConnectionTopicsReadable => "connection.topicsReadable",
        DestinationResolved => "destination.resolved",
        DestinationCredentialProjected => "destination.credentialProjected",
        DestinationArchiveListable => "destination.archiveListable",
        DestinationEvidenceWritable => "destination.evidenceWritable",
        DestinationArchivePrefixWritable => "destination.archivePrefixWritable",
        DestinationEvidenceReadable => "destination.evidenceReadable",
        SignerPrivateKeyUsable => "signer.privateKeyUsable",
        SignerRostered => "signer.rostered",
        RunnerImage => "runner.image",
        RunnerPod => "runner.pod",
        RunnerContract => "runner.contract",
        ConfigurationPolicy => "configuration.policy",
        ConfigurationEgress => "configuration.egress",
        TargetResolved => "target.resolved",
        TargetCredentialProjected => "target.credentialProjected",
        TargetAuthenticated => "target.authenticated",
        TargetClusterIdentity => "target.clusterIdentity",
        TargetScratchMarker => "target.scratchMarker",
        TargetMappedTopics => "target.mappedTopics",
        TargetTopicCreate => "target.topicCreate",
        TargetTimestampBound => "target.timestampBound",
        TargetLogAppendTime => "target.logAppendTime",
        PlanParse => "plan.parse",
        PlanBindings => "plan.bindings",
        PlanNames => "plan.names",
        RecoveryPointState => "recoveryPoint.state",
        ArchiveBackupSet => "archive.backupSet",
        ArchiveCoverage => "archive.coverage",
        ArchiveSegments => "archive.segments",
        ApprovalState => "approval.state",
        ApprovalKeyValidity => "approval.keyValidity",
    }
}

impl CheckId {
    /// The part before the dot — `status.result.checks[].category`.
    #[must_use]
    pub fn category(self) -> &'static str {
        let s = self.as_str();
        match s.split_once('.') {
            Some((head, _)) => head,
            None => s,
        }
    }
}

/// A per-check state (D2 §4.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CheckState {
    Ready,
    NotReady,
    Unknown,
    Skipped,
}

/// Whether a check's verdict gates the operation (D2 §6.3 legend).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Gating {
    /// A `notReady` here makes the whole operation `notReady`.
    Blocking,
    /// Reported as a warning; never changes the aggregate.
    Advisory,
    /// Cannot be checked before execution. Always `unknown`, always excluded
    /// from aggregation — an execution-only check that could turn the
    /// aggregate `unknown` would make every operation permanently `unknown`.
    ExecutionOnly,
}

/// Who observed the fact (D2 §6.4 `authority`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Authority {
    /// The controller, with no credential.
    Controller,
    /// The credential-consuming check Job's own output.
    CheckJob,
    /// Pod status or Kubernetes events.
    PodStatus,
}

/// The aggregate of a result (D2 §6.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OverallState {
    Ready,
    NotReady,
    Unknown,
}

/// The object a check is about.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CheckScope {
    pub kind: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
}

/// One per-check record — the shape D2 §6.4 publishes in
/// `status.result.checks[]`.
///
/// `message` and `remedy` are REDACTED and capped by [`CheckOutcome::new`];
/// the struct's fields are public so a decoder can round-trip a record it
/// read, but every construction site in Logweir goes through the constructor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CheckOutcome {
    pub id: CheckId,
    pub category: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<CheckScope>,
    pub state: CheckState,
    pub gating: Gating,
    pub authority: Authority,
    pub code: CheckCode,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub remedy: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
    /// Non-secret facts a UI shows verbatim: `clusterId`, `brokerCount`,
    /// `imageID`, `signerKeyId`. Values pass [`redact`] like every other
    /// relayed string.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub facts: BTreeMap<String, String>,
    /// Bounded structured detail (`{"count":2,"sample":[…]}`). The full list
    /// goes to the details `ConfigMap`, never here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<serde_json::Value>,
}

impl CheckOutcome {
    /// The ONE construction site. It fills `category` from the id, redacts and
    /// caps `message` and `remedy`, and forces an execution-only check to
    /// `unknown` — D2 §6.3: "E checks are always reported `unknown` with a
    /// note".
    #[must_use]
    pub fn new(
        id: CheckId,
        state: CheckState,
        gating: Gating,
        authority: Authority,
        code: CheckCode,
    ) -> Self {
        let state = if gating == Gating::ExecutionOnly {
            CheckState::Unknown
        } else {
            state
        };
        Self {
            id,
            category: id.category().to_string(),
            scope: None,
            state,
            gating,
            authority,
            code,
            message: String::new(),
            remedy: String::new(),
            observed_at: None,
            expires_at: None,
            facts: BTreeMap::new(),
            detail: None,
        }
    }

    /// Sets a redacted, capped message.
    #[must_use]
    pub fn with_message(mut self, message: &str) -> Self {
        self.message = redact(message);
        self
    }

    /// Sets a redacted, capped remedy.
    #[must_use]
    pub fn with_remedy(mut self, remedy: &str) -> Self {
        self.remedy = redact(remedy);
        self
    }

    #[must_use]
    pub fn with_scope(mut self, scope: CheckScope) -> Self {
        self.scope = Some(scope);
        self
    }

    #[must_use]
    pub fn with_times(mut self, observed_at: DateTime<Utc>, expires_at: DateTime<Utc>) -> Self {
        self.observed_at = Some(observed_at);
        self.expires_at = Some(expires_at);
        self
    }

    /// Adds a non-secret fact. The value is redacted like any relayed string.
    #[must_use]
    pub fn with_fact(mut self, key: &str, value: &str) -> Self {
        self.facts.insert(key.to_string(), redact(value));
        self
    }

    #[must_use]
    pub fn with_detail(mut self, detail: serde_json::Value) -> Self {
        self.detail = Some(detail);
        self
    }
}

/// D2 §6.4's aggregation, and nothing else:
///
/// - `notReady` if any BLOCKING check is `notReady`;
/// - otherwise `unknown` if any BLOCKING check is `unknown` or `skipped`;
/// - otherwise `ready`.
///
/// Advisory checks are warnings and execution-only checks are excluded
/// entirely. An EMPTY set is `unknown`, never `ready`: "nothing was checked"
/// is not "everything passed", and a bug that dropped every check would
/// otherwise report a green readiness.
#[must_use]
pub fn aggregate(checks: &[CheckOutcome]) -> OverallState {
    let blocking: Vec<&CheckOutcome> = checks
        .iter()
        .filter(|c| c.gating == Gating::Blocking)
        .collect();
    if blocking.is_empty() {
        return OverallState::Unknown;
    }
    if blocking.iter().any(|c| c.state == CheckState::NotReady) {
        return OverallState::NotReady;
    }
    if blocking
        .iter()
        .any(|c| matches!(c.state, CheckState::Unknown | CheckState::Skipped))
    {
        return OverallState::Unknown;
    }
    OverallState::Ready
}

/// The minimum `expiresAt` over non-skipped checks (D2 §6.4). `None` when no
/// non-skipped check carries one.
#[must_use]
pub fn aggregate_expires_at(checks: &[CheckOutcome]) -> Option<DateTime<Utc>> {
    checks
        .iter()
        .filter(|c| c.state != CheckState::Skipped)
        .filter_map(|c| c.expires_at)
        .min()
}

/// Advisory checks that are `notReady` — the warnings a UI shows beside a
/// `ready` verdict.
#[must_use]
pub fn advisory_warnings(checks: &[CheckOutcome]) -> Vec<&CheckOutcome> {
    checks
        .iter()
        .filter(|c| c.gating == Gating::Advisory && c.state == CheckState::NotReady)
        .collect()
}

// --------------------------------------------------------------- check plan

/// Which of the six plan kinds a request is (D2 §4.2, D3 §5.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CheckPlanKind {
    TopicInventory,
    OperationReadiness,
    RestorePreflight,
    DestinationAccess,
    EvidenceFetch,
    /// D3 §5.3's `RecoveryCatalog` sync: one bounded, read-only walk of the
    /// durable catalog in object storage, relayed as the body
    /// `docs/kubernetes.md` §7d grammars.
    CatalogSync,
}

impl CheckPlanKind {
    /// The wire spelling.
    ///
    /// `const fn` so a caller that needs the string in a CONSTANT position can
    /// derive it from this table rather than writing a second literal —
    /// `weirkeeper::catalog_view::PLAN_KIND` is exactly that caller, and it
    /// spent D3 W8 as a hand-written `"catalogSync"` because this was not
    /// `const`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TopicInventory => "topicInventory",
            Self::OperationReadiness => "operationReadiness",
            Self::RestorePreflight => "restorePreflight",
            Self::DestinationAccess => "destinationAccess",
            Self::EvidenceFetch => "evidenceFetch",
            Self::CatalogSync => "catalogSync",
        }
    }

    /// The two-letter Job-name discriminator of D2 §4.3 (`td`, `rd`, `rp`,
    /// `da`, `ev`, `cs`). It lives here so the controller and any tooling that
    /// has to recognise a check Job by name read one table.
    ///
    /// `const fn` for the reason [`CheckPlanKind::as_str`] gives.
    #[must_use]
    pub const fn job_discriminator(self) -> &'static str {
        match self {
            Self::TopicInventory => "td",
            Self::OperationReadiness => "rd",
            Self::RestorePreflight => "rp",
            Self::DestinationAccess => "da",
            Self::EvidenceFetch => "ev",
            Self::CatalogSync => "cs",
        }
    }

    pub const ALL: [Self; 6] = [
        Self::TopicInventory,
        Self::OperationReadiness,
        Self::RestorePreflight,
        Self::DestinationAccess,
        Self::EvidenceFetch,
        Self::CatalogSync,
    ];
}

/// Which operation a readiness request is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum CheckOperation {
    Backup,
    Restore,
    DestinationAccess,
}

/// How a Kafka connection is authenticated, spelled as the plan spells it.
/// The plan carries NO credential value: a password reaches the runner only as
/// a projected environment variable named by the Kubernetes Secret key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ConnectionPlan {
    pub bootstrap_servers: Vec<String>,
    /// `plaintext` | `scramSha256` | `scramSha512`, matching
    /// `logweir_core::spec::AuthSpec::mode_str`.
    pub auth_mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// The NAME of the environment variable the password is projected into —
    /// never the password.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_env: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<bool>,
    /// A path inside the pod, e.g. `/check/source-ca.pem`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<String>,
    /// `User:<name>` or `User:ANONYMOUS` (D2 §5.4).
    pub principal: String,
}

/// A destination, rendered into the plan from the resolved `BackupDestination`
/// so the runner never resolves anything itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DestinationPlan {
    pub name: String,
    pub uid: String,
    pub location: crate::destination::DestinationLocation,
    pub location_digest: String,
    /// A path inside the pod, e.g. `/check/archive-ca.pem`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<String>,
    /// How the credential for each requested role reaches the process. Never
    /// a value: `static` means "read `AWS_ACCESS_KEY_ID` and friends, which
    /// the kubelet projected", `workloadIdentity` means "use the injected web
    /// identity only", `ambient` means the object_store chain.
    pub credentials: CredentialMode,
}

/// How the store credential reaches the process (D2 §3.5,
/// `LOGWEIR_ARCHIVE_CREDENTIALS`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CredentialMode {
    /// Explicit static keys, projected by the kubelet into the pod.
    Static,
    /// Web identity / container credentials ONLY. Static keys in the
    /// environment are ignored, and a missing injection is a refusal
    /// (`WorkloadIdentityNotInjected`), never a silent fall-through to a node
    /// role.
    WorkloadIdentity,
    /// The object_store chain as configured for this process. Only the
    /// controller's own allowlisted `ControllerIdentity` reads uses it.
    Ambient,
}

/// `topicInventory` (D2 §4.2, §5.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TopicInventoryRequest {
    pub connection: ConnectionPlan,
    #[serde(default)]
    pub include_internal: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub expected_topics: Vec<String>,
    pub max_topics: u32,
    /// Bytes of topic LINES the runner may relay before it truncates with
    /// `RelayLimit`.
    pub relay_budget_bytes: u64,
}

/// `operationReadiness` (D2 §4.2, §6.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OperationReadinessRequest {
    pub operation: CheckOperation,
    pub connection: ConnectionPlan,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination: Option<DestinationPlan>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub roles: Vec<DestinationRole>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub topics: Vec<String>,
    /// A path inside the pod; the runner loads it with
    /// `logweir::signer::ValidatedSigner::load` and reports the PUBLIC key id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signer_path: Option<String>,
    /// `writeProbe: CreateOnlyMarker` on the destination.
    #[serde(default)]
    pub write_probe: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skip_checks: Vec<CheckId>,
}

/// `restorePreflight` (D2 §4.2, §6.7).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RestorePreflightRequest {
    /// Where the verbatim plan bytes are mounted, e.g. `/check/plan.yaml`.
    pub plan_file: String,
    /// `sha256:<64 hex>` of those bytes; the runner recomputes and refuses a
    /// mismatch before it opens a socket.
    pub plan_sha256: String,
    pub target: ConnectionPlan,
    pub source_destination: DestinationPlan,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_destination: Option<DestinationPlan>,
    pub backup_id: String,
    pub manifest_key: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checks: Vec<CheckId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skip_checks: Vec<CheckId>,
}

/// `destinationAccess` (D2 §4.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DestinationAccessRequest {
    pub destination: DestinationPlan,
    pub roles: Vec<DestinationRole>,
    /// The optional create-only marker probe. `logweir/readiness/<uid>.json`
    /// is the ONLY key a check may ever write (D2 §4.2).
    #[serde(default)]
    pub write_probe: bool,
}

/// One object an `evidenceFetch` relays.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EvidenceObjectRequest {
    pub role: DestinationRole,
    pub key: String,
    pub max_bytes: u64,
    /// Which relay stream the bytes go to.
    pub stream: Stream,
}

/// `evidenceFetch` (D2 §4.2, §3.9).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EvidenceFetchRequest {
    pub destination: DestinationPlan,
    pub objects: Vec<EvidenceObjectRequest>,
}

/// How much of the durable catalog one `catalogSync` walks (D3 §5.3).
///
/// The wire spellings are `"Index"` and `"Full"` — PascalCase and NOT this
/// module's usual `camelCase`, because they are the spellings
/// `RecoveryCatalog.spec.sync.mode` already publishes in a CRD an operator
/// edits. A plan that renamed them would make the controller translate between
/// two spellings of one value, which is the defect every closed vocabulary in
/// this file exists to avoid.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub enum CatalogSyncMode {
    /// Day shards of `logweir/catalog/v1/log/`, newest first, down to the
    /// floor the recorded cursor implies.
    #[default]
    Index,
    /// A resumable rescan of `logweir/catalog/v1/points/`, continuing after
    /// [`CatalogSyncRequest::rescan_start_after`].
    Full,
}

impl CatalogSyncMode {
    /// The wire spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Index => "Index",
            Self::Full => "Full",
        }
    }
}

/// How hard a `catalogSync` checks each point it finds (D3 §5.3).
///
/// PascalCase for the reason [`CatalogSyncMode`] gives.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub enum CatalogDeepCheck {
    /// The record and its receipt exist.
    None,
    /// Additionally: the manifest is readable and its digest is the receipt's.
    #[default]
    ManifestDigest,
    /// Additionally: sample segment bytes. **Not implemented by this build** —
    /// a plan naming it is honoured as [`CatalogDeepCheck::ManifestDigest`] and
    /// the result says so, rather than reporting a check that did not run.
    SegmentSample,
}

impl CatalogDeepCheck {
    /// The wire spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "None",
            Self::ManifestDigest => "ManifestDigest",
            Self::SegmentSample => "SegmentSample",
        }
    }
}

/// `catalogSync` (D3 §5.3, `docs/kubernetes.md` §7d).
///
/// It carries NO credential and NO endpoint of its own: `destination` is the
/// resolved [`DestinationPlan`], and every `AWS_*` variable is projected onto
/// the Job by `secretKeyRef` (D-SEAMS **S5**). The grant is `archiveRead` and
/// the walk writes nothing at all — not even the create-only readiness marker
/// a `destinationAccess` may write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CatalogSyncRequest {
    /// Where the catalog is.
    pub destination: DestinationPlan,
    /// `Index` or `Full`.
    pub mode: CatalogSyncMode,
    /// How hard to check each point.
    pub deep_check: CatalogDeepCheck,
    /// The object budget for one run — how many objects the walk may `get` or
    /// `list` before it stops and records a cursor.
    pub max_objects_per_run: i64,
    /// How many NEWEST points to relay. The body carries at most this many
    /// `catalog-entry=` lines; everything else the walk saw is counted and
    /// histogrammed.
    pub view_limit: i64,
    /// The oldest day shard the previous `Index` walk reached, `YYYY-MM-DD`.
    /// The next walk aims at that day MINUS one day of overlap and still
    /// starts at today, so the window stays newest-first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_shard: Option<String>,
    /// The key a `Full` rescan continues strictly after.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rescan_start_after: Option<String>,
    /// Where the PEM bundle of this installation's PUBLIC signing keys is
    /// mounted, or `None` when it holds no trust material at all.
    ///
    /// `None` is a real answer and not an omission: with no bundle the runner
    /// reports the `notAttempted` signature verdict for every point, which is
    /// how `unverified` ends up equal to `total` and `TrustAvailable=False`
    /// ends up on the status. **A runner that silently verified against
    /// nothing would report `verified` for a forgery.**
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trust_bundle_file: Option<String>,
}

/// The six requests, externally tagged so an unknown kind is a parse error
/// with the kind named, and so each variant keeps its own
/// `deny_unknown_fields`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CheckRequest {
    TopicInventory(TopicInventoryRequest),
    // Boxed: these two carry a connection AND one or two destinations, which
    // makes them several hundred bytes larger than the other three. A plan is
    // deserialised once per process, so the indirection costs nothing and
    // keeps `CheckRequest` small enough to pass by value.
    OperationReadiness(Box<OperationReadinessRequest>),
    RestorePreflight(Box<RestorePreflightRequest>),
    DestinationAccess(DestinationAccessRequest),
    EvidenceFetch(EvidenceFetchRequest),
    // Boxed for the same reason: a destination plus three optional paths is
    // the largest of the unboxed shapes, and one oversized variant sets the
    // size of every `CheckRequest` value in the process.
    CatalogSync(Box<CatalogSyncRequest>),
}

impl CheckRequest {
    #[must_use]
    pub fn kind(&self) -> CheckPlanKind {
        match self {
            Self::TopicInventory(_) => CheckPlanKind::TopicInventory,
            Self::OperationReadiness(_) => CheckPlanKind::OperationReadiness,
            Self::RestorePreflight(_) => CheckPlanKind::RestorePreflight,
            Self::DestinationAccess(_) => CheckPlanKind::DestinationAccess,
            Self::EvidenceFetch(_) => CheckPlanKind::EvidenceFetch,
            Self::CatalogSync(_) => CheckPlanKind::CatalogSync,
        }
    }
}

/// The document mounted at `/check/check-plan.json` (D2 §4.2).
///
/// `deny_unknown_fields` everywhere: a plan a newer controller wrote with a
/// field this runner does not understand is refused BEFORE a client is built,
/// which is the whole point of the version handshake. Silently ignoring an
/// unknown field is how a runner ends up doing less than the controller
/// believes it did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CheckPlan {
    pub contract: String,
    pub contract_version: u32,
    /// The UID of the object that owns this check. The runner refuses unless
    /// it equals `LOGWEIR_CHECK_SUBJECT_UID`, so a plan `ConfigMap` swapped
    /// under a Job cannot be executed against the wrong subject.
    pub subject_uid: String,
    pub timeout_seconds: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_digest: Option<String>,
    pub request: CheckRequest,
}

/// Why a plan was refused. Every variant maps to
/// [`CheckCode::CheckContractMismatch`] and exit 3: the runner has printed no
/// frame and has opened no socket.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CheckPlanError {
    #[error("check plan contract `{0}` is not `{CHECK_PLAN_CONTRACT}`")]
    Contract(String),
    #[error("check plan contract version {0} is not {CHECK_CONTRACT_VERSION}")]
    ContractVersion(u32),
    #[error("check plan does not parse: {0}")]
    Parse(String),
    #[error("check plan subject uid `{got}` is not the expected `{want}`")]
    SubjectUid { got: String, want: String },
    #[error("check plan sha256 `{got}` is not the expected `{want}`")]
    PlanSha256 { got: String, want: String },
    #[error("check plan field {field}: {message}")]
    Field { field: String, message: String },
}

impl CheckPlanError {
    /// Always [`CheckCode::CheckContractMismatch`] — the runner refused before
    /// it built a client, so there is nothing else to report.
    #[must_use]
    pub fn code(&self) -> CheckCode {
        CheckCode::CheckContractMismatch
    }

    fn field(field: &str, message: impl Into<String>) -> Self {
        Self::Field {
            field: field.to_string(),
            message: message.into(),
        }
    }
}

impl CheckPlan {
    /// Steps 2-4 of D2 §4.2's startup order, as ONE function so no caller can
    /// do them out of order: hash the bytes, compare them against the digest
    /// the Job env pins, parse strictly, check the subject UID, validate the
    /// bounds.
    ///
    /// Nothing here opens anything. The caller reads the bytes (step 2) and
    /// builds clients (step 5) only after this returns `Ok`.
    pub fn parse_and_verify(
        bytes: &[u8],
        expected_sha256: &str,
        expected_subject_uid: &str,
    ) -> Result<Self, CheckPlanError> {
        let got = crate::ids::sha256_prefixed(bytes);
        if got != expected_sha256 {
            return Err(CheckPlanError::PlanSha256 {
                got,
                want: expected_sha256.to_string(),
            });
        }
        let plan: Self =
            serde_json::from_slice(bytes).map_err(|e| CheckPlanError::Parse(e.to_string()))?;
        if plan.contract != CHECK_PLAN_CONTRACT {
            return Err(CheckPlanError::Contract(plan.contract));
        }
        if plan.contract_version != CHECK_CONTRACT_VERSION {
            return Err(CheckPlanError::ContractVersion(plan.contract_version));
        }
        if plan.subject_uid != expected_subject_uid {
            return Err(CheckPlanError::SubjectUid {
                got: plan.subject_uid,
                want: expected_subject_uid.to_string(),
            });
        }
        plan.validate()?;
        Ok(plan)
    }

    #[must_use]
    pub fn kind(&self) -> CheckPlanKind {
        self.request.kind()
    }

    /// Every bound D2 §4.2 states, checked on the READ side as well as the
    /// write side. A controller that rendered 5,000 expected topics is a
    /// controller bug, and the runner must not act on it: the budgets are what
    /// make the relay, the etcd footprint and the Kafka call count bounded.
    pub fn validate(&self) -> Result<(), CheckPlanError> {
        // TWO CEILINGS, BECAUSE THERE ARE TWO KINDS OF WORK. See
        // [`MAX_CATALOG_SYNC_TIMEOUT_SECONDS`].
        let ceiling = match &self.request {
            CheckRequest::CatalogSync(_) => MAX_CATALOG_SYNC_TIMEOUT_SECONDS,
            _ => MAX_CHECK_TIMEOUT_SECONDS,
        };
        if self.timeout_seconds == 0 || self.timeout_seconds > ceiling {
            return Err(CheckPlanError::field(
                "timeoutSeconds",
                format!("{} is outside 1..={ceiling}", self.timeout_seconds),
            ));
        }
        match &self.request {
            CheckRequest::TopicInventory(r) => {
                if r.expected_topics.len() > MAX_EXPECTED_TOPICS {
                    return Err(CheckPlanError::field(
                        "request.topicInventory.expectedTopics",
                        format!(
                            "{} entries exceeds the cap of {MAX_EXPECTED_TOPICS}",
                            r.expected_topics.len()
                        ),
                    ));
                }
                if r.max_topics == 0 || r.max_topics > MAX_TOPICS_CEILING {
                    return Err(CheckPlanError::field(
                        "request.topicInventory.maxTopics",
                        format!("{} is outside 1..={MAX_TOPICS_CEILING}", r.max_topics),
                    ));
                }
                if r.relay_budget_bytes == 0 {
                    return Err(CheckPlanError::field(
                        "request.topicInventory.relayBudgetBytes",
                        "a zero relay budget would return nothing",
                    ));
                }
            }
            CheckRequest::OperationReadiness(r) => {
                if r.topics.len() > MAX_READINESS_TOPICS {
                    return Err(CheckPlanError::field(
                        "request.operationReadiness.topics",
                        format!(
                            "{} entries exceeds the cap of {MAX_READINESS_TOPICS}",
                            r.topics.len()
                        ),
                    ));
                }
                if r.write_probe && r.destination.is_none() {
                    return Err(CheckPlanError::field(
                        "request.operationReadiness.writeProbe",
                        "a write probe needs a destination",
                    ));
                }
            }
            CheckRequest::RestorePreflight(r) => {
                if !is_sha256_prefixed(&r.plan_sha256) {
                    return Err(CheckPlanError::field(
                        "request.restorePreflight.planSha256",
                        "planSha256 is sha256:<64 lowercase hex>",
                    ));
                }
            }
            CheckRequest::DestinationAccess(r) => {
                if r.roles.is_empty() || r.roles.len() > DestinationRole::ALL.len() {
                    return Err(CheckPlanError::field(
                        "request.destinationAccess.roles",
                        format!("{} roles is outside 1..=4", r.roles.len()),
                    ));
                }
            }
            CheckRequest::CatalogSync(r) => {
                if r.view_limit < MIN_CATALOG_VIEW_LIMIT || r.view_limit > MAX_CATALOG_VIEW_LIMIT {
                    return Err(CheckPlanError::field(
                        "request.catalogSync.viewLimit",
                        format!(
                            "{} is outside {MIN_CATALOG_VIEW_LIMIT}..={MAX_CATALOG_VIEW_LIMIT}",
                            r.view_limit
                        ),
                    ));
                }
                if r.max_objects_per_run < MIN_CATALOG_OBJECTS_PER_RUN
                    || r.max_objects_per_run > MAX_CATALOG_OBJECTS_PER_RUN
                {
                    return Err(CheckPlanError::field(
                        "request.catalogSync.maxObjectsPerRun",
                        format!(
                            "{} is outside \
                             {MIN_CATALOG_OBJECTS_PER_RUN}..={MAX_CATALOG_OBJECTS_PER_RUN}",
                            r.max_objects_per_run
                        ),
                    ));
                }
                // A DAY SHARD IS A DAY, and the runner turns this into a key
                // prefix. A value that is not `YYYY-MM-DD` would silently
                // become a prefix that lists nothing, and a sync that walked
                // nothing would publish an empty view of a full archive.
                if let Some(day) = r.index_shard.as_deref() {
                    if !is_iso_day(day) {
                        return Err(CheckPlanError::field(
                            "request.catalogSync.indexShard",
                            "an index cursor is a UTC day, `YYYY-MM-DD`",
                        ));
                    }
                }
            }
            CheckRequest::EvidenceFetch(r) => {
                if r.objects.is_empty() || r.objects.len() > MAX_EVIDENCE_OBJECTS {
                    return Err(CheckPlanError::field(
                        "request.evidenceFetch.objects",
                        format!(
                            "{} objects is outside 1..={MAX_EVIDENCE_OBJECTS}",
                            r.objects.len()
                        ),
                    ));
                }
                for (i, o) in r.objects.iter().enumerate() {
                    if o.role != DestinationRole::EvidenceRead {
                        return Err(CheckPlanError::field(
                            &format!("request.evidenceFetch.objects[{i}].role"),
                            "an evidence fetch reads with the evidenceRead grant and no other",
                        ));
                    }
                    let cap = match o.stream {
                        Stream::EvidenceSidecar => MAX_EVIDENCE_SIDECAR_BYTES,
                        _ => MAX_EVIDENCE_PAYLOAD_BYTES,
                    };
                    if o.max_bytes == 0 || o.max_bytes > cap {
                        return Err(CheckPlanError::field(
                            &format!("request.evidenceFetch.objects[{i}].maxBytes"),
                            format!("{} is outside 1..={cap}", o.max_bytes),
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

/// `sha256:` plus 64 lowercase hex characters — the form
/// [`crate::ids::sha256_prefixed`] produces and the form CEL rule P9 enforces.
#[must_use]
pub fn is_sha256_prefixed(s: &str) -> bool {
    match s.strip_prefix("sha256:") {
        Some(hex) => {
            hex.len() == 64
                && hex
                    .chars()
                    .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
        }
        None => false,
    }
}

/// `YYYY-MM-DD`, UTC — the day a catalog index shard names.
///
/// SHAPE ONLY, and deliberately: `2026-02-30` passes. The runner turns this
/// into a key prefix and a `chrono` date; what this guards is that a plan
/// cannot carry `../` or an empty string into a listing, and a nonexistent
/// calendar day simply lists nothing. A second calendar implementation here
/// would be a second answer to a question `chrono` already answers on the
/// side that actually needs the date.
#[must_use]
pub fn is_iso_day(day: &str) -> bool {
    day.len() == 10
        && day.as_bytes()[4] == b'-'
        && day.as_bytes()[7] == b'-'
        && day
            .bytes()
            .enumerate()
            .all(|(i, b)| i == 4 || i == 7 || b.is_ascii_digit())
}

// ------------------------------------------------------------------- frames

/// The four relay streams. A closed enum, so an unknown stream name in a frame
/// is `ResultUnreadable` rather than a silently dropped part.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Stream {
    /// The structured [`CheckResult`] document.
    Result,
    /// Bounded structured detail (missing segments, collisions), JSON lines.
    Details,
    /// Evidence bytes: the receipt or the scorecard.
    #[serde(rename = "evidence.payload")]
    EvidencePayload,
    /// The DSSE sidecar for the payload.
    #[serde(rename = "evidence.sidecar")]
    EvidenceSidecar,
}

impl Stream {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Result => "result",
            Self::Details => "details",
            Self::EvidencePayload => "evidence.payload",
            Self::EvidenceSidecar => "evidence.sidecar",
        }
    }

    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "result" => Some(Self::Result),
            "details" => Some(Self::Details),
            "evidence.payload" => Some(Self::EvidencePayload),
            "evidence.sidecar" => Some(Self::EvidenceSidecar),
            _ => None,
        }
    }

    pub const ALL: [Self; 4] = [
        Self::Result,
        Self::Details,
        Self::EvidencePayload,
        Self::EvidenceSidecar,
    ];
}

/// Why an inventory stopped early.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TruncationReason {
    /// The request's `maxTopics` was reached.
    MaxTopics,
    /// The relay budget was reached. This is the bound that keeps the pod log
    /// under the kubelet's `containerLogMaxSize`.
    RelayLimit,
}

/// One inventory entry, exactly as a topic frame carries it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TopicEntry {
    pub name: String,
    pub partitions: u32,
    #[serde(default)]
    pub internal: bool,
    #[serde(default)]
    pub expected: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<CheckCode>,
}

impl TopicEntry {
    #[must_use]
    pub fn new(name: &str, partitions: u32) -> Self {
        Self {
            name: name.to_string(),
            partitions,
            internal: false,
            expected: false,
            error: None,
        }
    }

    /// D2 §5.3: a topic is internal IFF its name starts with `__`. Kafka's
    /// reserved-name convention, and the ONLY rule — `_schemas`,
    /// `_confluent-*` and Connect internal topics have configurable names and
    /// are never guessed (a wrong "this is internal" silently drops a user's
    /// data from a backup).
    #[must_use]
    pub fn name_is_internal(name: &str) -> bool {
        name.starts_with("__")
    }

    /// The flags field of a topic frame: `-`, or a comma list in this fixed
    /// order so the canonical TSV is byte-stable.
    #[must_use]
    pub fn flags(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        if self.internal {
            parts.push("internal".to_string());
        }
        if self.expected {
            parts.push("expected".to_string());
        }
        if let Some(code) = self.error {
            parts.push(format!("error:{code}"));
        }
        if parts.is_empty() {
            "-".to_string()
        } else {
            parts.join(",")
        }
    }

    /// The canonical TSV line, `name \t partitions \t flags \n` (D2 §5.5).
    /// This is the ONE rendering: the relay frame carries it after a prefix,
    /// the result `ConfigMap` chunks store it verbatim, and `topicsSha256` is
    /// taken over it. Three consumers, one function, so they cannot disagree
    /// about what was hashed.
    #[must_use]
    pub fn tsv_line(&self) -> String {
        format!("{}\t{}\t{}\n", self.name, self.partitions, self.flags())
    }
}

/// The canonical TSV of a whole inventory, and the bytes `topicsSha256` and
/// the end frame's `topicLines.sha256` are taken over.
#[must_use]
pub fn topic_tsv(entries: &[TopicEntry]) -> String {
    entries.iter().map(TopicEntry::tsv_line).collect()
}

/// `sha256:<hex>` over [`topic_tsv`].
#[must_use]
pub fn topic_tsv_sha256(entries: &[TopicEntry]) -> String {
    crate::ids::sha256_prefixed(topic_tsv(entries).as_bytes())
}

/// One stream's summary in the end frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct StreamSummary {
    pub parts: u32,
    pub sha256: String,
}

/// The topic-line summary in the end frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TopicLineSummary {
    pub count: u32,
    pub sha256: String,
}

/// `logweir-check-end=<compact JSON>` — the LAST stdout line. Its presence is
/// what distinguishes "the check ran and produced a result" from "the process
/// died halfway through printing one".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EndFrame {
    pub contract: String,
    pub plan_sha256: String,
    pub subject_uid: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub streams: BTreeMap<String, StreamSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_lines: Option<TopicLineSummary>,
}

/// What a decoder must be able to prove about the frames it read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameExpectations {
    pub plan_sha256: String,
    pub subject_uid: String,
}

/// Everything one check Job relayed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckRelay {
    pub topics: Vec<TopicEntry>,
    pub streams: BTreeMap<Stream, Vec<u8>>,
    pub end: EndFrame,
}

impl CheckRelay {
    #[must_use]
    pub fn stream(&self, s: Stream) -> Option<&[u8]> {
        self.streams.get(&s).map(Vec::as_slice)
    }

    /// The `result` stream, parsed, bounds-checked and REDACTED. `None` when
    /// the check relayed none (an evidence fetch does not).
    ///
    /// The error type is [`FrameError`], not `serde_json::Error`, so the
    /// "every malformation is `ResultUnreadable`" property that
    /// `FrameError::code()` guarantees for the frames also holds for the
    /// document inside them — the caller gets one code instead of having to
    /// re-establish it. And the document is sanitised before it is returned,
    /// so a controller cannot forget: the decoded fields are whatever the
    /// runner wrote, and nothing else in the pure layer redacts them.
    pub fn result(&self) -> Option<Result<CheckResult, FrameError>> {
        self.stream(Stream::Result).map(|bytes| {
            let mut doc: CheckResult = serde_json::from_slice(bytes)
                .map_err(|_| FrameError::ResultDocument(CheckResultError::Parse))?;
            doc.validate().map_err(FrameError::ResultDocument)?;
            doc.sanitise();
            Ok(doc)
        })
    }
}

/// Why a relay could not be read. Every variant is reported as
/// [`CheckCode::ResultUnreadable`], WITHOUT the log content: a relay that does
/// not decode is exactly the case where echoing what was read would put
/// unvalidated bytes into a status.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FrameError {
    #[error("a frame line is {0} bytes, over the {FRAME_MAX_BYTES}-byte limit")]
    LineTooLong(usize),
    #[error("malformed {kind} frame")]
    Malformed { kind: &'static str },
    #[error("unknown relay stream")]
    UnknownStream,
    #[error("stream {stream} part {seq} arrived twice")]
    DuplicatePart { stream: &'static str, seq: u32 },
    #[error("stream {stream} is missing part {seq} of {total}")]
    MissingPart {
        stream: &'static str,
        seq: u32,
        total: u32,
    },
    #[error("stream {stream} declared {declared} parts and {got} arrived")]
    PartCountMismatch {
        stream: &'static str,
        declared: u32,
        got: u32,
    },
    #[error("stream {stream} does not match its declared digest")]
    StreamDigestMismatch { stream: &'static str },
    #[error("the end frame declares stream {0}, which no part carried")]
    UndeclaredStream(String),
    #[error("{got} topic lines arrived and the end frame declares {declared}")]
    TopicCountMismatch { declared: u32, got: u32 },
    #[error("the topic lines do not match the digest the end frame declares")]
    TopicDigestMismatch,
    #[error("no end frame: the check printed no complete result")]
    MissingEndFrame,
    #[error("a frame arrived after the end frame")]
    FrameAfterEnd,
    #[error("the end frame's contract is not `{CHECK_RESULT_CONTRACT}`")]
    ResultContract,
    #[error("the end frame's planSha256 is not the plan this Job was given")]
    PlanShaMismatch,
    #[error("the end frame's subjectUid is not the subject this Job was created for")]
    SubjectUidMismatch,
    #[error("the relay exceeded its {0}-byte budget")]
    BudgetExceeded(usize),
    #[error("a part frame does not decode as base64")]
    Base64,
    #[error("the relayed result document is unusable: {0}")]
    ResultDocument(#[from] CheckResultError),
}

impl FrameError {
    /// Always [`CheckCode::ResultUnreadable`] (D2 §4.3).
    #[must_use]
    pub fn code(&self) -> CheckCode {
        CheckCode::ResultUnreadable
    }
}

/// The frame writer and the incremental decoder.
pub mod frames {
    use super::{
        CheckRelay, EndFrame, FrameError, FrameExpectations, Stream, StreamSummary, TopicEntry,
        TopicLineSummary, CHECK_RESULT_CONTRACT, END_FRAME_PREFIX, FRAME_MAX_BYTES,
        PART_FRAME_PREFIX, PART_MAX_BASE64_CHARS, TOPIC_FRAME_PREFIX,
    };
    use base64::Engine as _;
    use std::collections::BTreeMap;

    fn b64() -> base64::engine::general_purpose::GeneralPurpose {
        base64::engine::general_purpose::STANDARD
    }

    /// One inventory line. `Err` when the rendered line would break the frame
    /// bound — a Kafka topic name is at most 249 characters, so this is
    /// unreachable for legal names and is a refusal rather than a truncation
    /// for anything else.
    pub fn write_topic_line(entry: &TopicEntry) -> Result<String, FrameError> {
        // The TSV line ends in `\n`; the frame carries its body.
        let body = entry.tsv_line();
        let line = format!("{TOPIC_FRAME_PREFIX}{}", body.trim_end_matches('\n'));
        check_len(&line)?;
        Ok(line)
    }

    /// One stream's bytes as an ordered list of part frames. An EMPTY payload
    /// still yields one part, so "the stream was present and empty" and "the
    /// stream was absent" stay distinguishable in the end frame.
    pub fn write_parts(stream: Stream, payload: &[u8]) -> Result<Vec<String>, FrameError> {
        let encoded = b64().encode(payload);
        let chunks: Vec<&str> = if encoded.is_empty() {
            vec![""]
        } else {
            encoded
                .as_bytes()
                .chunks(PART_MAX_BASE64_CHARS)
                // base64 output is ASCII, so a byte chunk is a char boundary.
                .map(|c| std::str::from_utf8(c).expect("base64 output is ASCII"))
                .collect()
        };
        let total = chunks.len();
        let mut out = Vec::with_capacity(total);
        for (i, c) in chunks.iter().enumerate() {
            let line = format!(
                "{PART_FRAME_PREFIX}{}:{}/{}:{}",
                stream.as_str(),
                i + 1,
                total,
                c
            );
            check_len(&line)?;
            out.push(line);
        }
        Ok(out)
    }

    /// The summary a caller passes to [`write_end`] for one stream.
    #[must_use]
    pub fn stream_summary(payload: &[u8], parts: usize) -> StreamSummary {
        StreamSummary {
            parts: parts as u32,
            sha256: crate::ids::sha256_prefixed(payload),
        }
    }

    /// The last line. Compact JSON, because pretty JSON would carry newlines
    /// and a frame is one line by definition.
    pub fn write_end(end: &EndFrame) -> Result<String, FrameError> {
        let json = serde_json::to_string(end).map_err(|_| FrameError::Malformed { kind: "end" })?;
        let line = format!("{END_FRAME_PREFIX}{json}");
        check_len(&line)?;
        Ok(line)
    }

    /// Builds the end frame from what was actually written, so the declared
    /// counts and digests cannot drift from the parts.
    #[must_use]
    pub fn end_frame(
        plan_sha256: &str,
        subject_uid: &str,
        streams: &BTreeMap<Stream, (Vec<u8>, usize)>,
        topics: Option<&[TopicEntry]>,
    ) -> EndFrame {
        EndFrame {
            contract: CHECK_RESULT_CONTRACT.to_string(),
            plan_sha256: plan_sha256.to_string(),
            subject_uid: subject_uid.to_string(),
            streams: streams
                .iter()
                .map(|(s, (bytes, parts))| (s.as_str().to_string(), stream_summary(bytes, *parts)))
                .collect(),
            topic_lines: topics.map(|t| TopicLineSummary {
                count: t.len() as u32,
                sha256: super::topic_tsv_sha256(t),
            }),
        }
    }

    fn check_len(line: &str) -> Result<(), FrameError> {
        // +1 for the newline the writer appends.
        if line.len() + 1 > FRAME_MAX_BYTES {
            return Err(FrameError::LineTooLong(line.len() + 1));
        }
        Ok(())
    }

    /// The incremental decoder. Fed one stdout line at a time, it holds only
    /// what it has seen, so a controller can stream an 8 MiB pod log through
    /// it without buffering a second copy of the frames.
    ///
    /// Lines that are not frames are IGNORED: the `KafkaCluster` probe prints
    /// its own I14 lines on the same stdout, and a decoder that refused them
    /// could not be reused there (D2 §4.5).
    #[derive(Debug)]
    pub struct Decoder {
        topics: Vec<TopicEntry>,
        parts: BTreeMap<Stream, BTreeMap<u32, String>>,
        totals: BTreeMap<Stream, u32>,
        end: Option<EndFrame>,
        used: usize,
        budget: usize,
    }

    impl Default for Decoder {
        fn default() -> Self {
            Self::with_budget(super::DEFAULT_RELAY_BUDGET_BYTES + (2 * 1024 * 1024))
        }
    }

    impl Decoder {
        #[must_use]
        pub fn new() -> Self {
            Self::default()
        }

        /// A decoder that refuses to accumulate more than `budget` bytes of
        /// frame content. The controller reads at most 8 MiB of log, and this
        /// is the second, independent bound on what a runner can make it hold.
        #[must_use]
        pub fn with_budget(budget: usize) -> Self {
            Self {
                topics: Vec::new(),
                parts: BTreeMap::new(),
                totals: BTreeMap::new(),
                end: None,
                used: 0,
                budget,
            }
        }

        /// Feeds one line, WITHOUT its newline.
        pub fn push_line(&mut self, line: &str) -> Result<(), FrameError> {
            let is_frame = line.starts_with(TOPIC_FRAME_PREFIX)
                || line.starts_with(PART_FRAME_PREFIX)
                || line.starts_with(END_FRAME_PREFIX);
            if !is_frame {
                return Ok(());
            }
            if line.len() + 1 > FRAME_MAX_BYTES {
                return Err(FrameError::LineTooLong(line.len() + 1));
            }
            if self.end.is_some() {
                return Err(FrameError::FrameAfterEnd);
            }
            self.used = self.used.saturating_add(line.len());
            if self.used > self.budget {
                return Err(FrameError::BudgetExceeded(self.budget));
            }
            if let Some(body) = line.strip_prefix(TOPIC_FRAME_PREFIX) {
                self.topics.push(parse_topic(body)?);
                return Ok(());
            }
            if let Some(body) = line.strip_prefix(PART_FRAME_PREFIX) {
                return self.push_part(body);
            }
            let body = line
                .strip_prefix(END_FRAME_PREFIX)
                .expect("the prefix set is exhaustive");
            let end: EndFrame =
                serde_json::from_str(body).map_err(|_| FrameError::Malformed { kind: "end" })?;
            self.end = Some(end);
            Ok(())
        }

        fn push_part(&mut self, body: &str) -> Result<(), FrameError> {
            let (stream_name, rest) = body
                .split_once(':')
                .ok_or(FrameError::Malformed { kind: "part" })?;
            let stream = Stream::parse(stream_name).ok_or(FrameError::UnknownStream)?;
            let (counter, payload) = rest
                .split_once(':')
                .ok_or(FrameError::Malformed { kind: "part" })?;
            let (seq, total) = counter
                .split_once('/')
                .ok_or(FrameError::Malformed { kind: "part" })?;
            let seq: u32 = seq
                .parse()
                .map_err(|_| FrameError::Malformed { kind: "part" })?;
            let total: u32 = total
                .parse()
                .map_err(|_| FrameError::Malformed { kind: "part" })?;
            if seq == 0 || total == 0 || seq > total {
                return Err(FrameError::Malformed { kind: "part" });
            }
            match self.totals.get(&stream) {
                Some(t) if *t != total => {
                    return Err(FrameError::PartCountMismatch {
                        stream: stream.as_str(),
                        declared: *t,
                        got: total,
                    })
                }
                _ => {
                    self.totals.insert(stream, total);
                }
            }
            let slot = self.parts.entry(stream).or_default();
            if slot.insert(seq, payload.to_string()).is_some() {
                return Err(FrameError::DuplicatePart {
                    stream: stream.as_str(),
                    seq,
                });
            }
            Ok(())
        }

        /// Requires the end frame, its contract, its plan digest, its subject
        /// UID, every declared part, every declared stream digest and the
        /// topic-line count and digest. Any mismatch is an error, which the
        /// caller reports as `ResultUnreadable` (D2 §4.3).
        pub fn finish(self, expect: &FrameExpectations) -> Result<CheckRelay, FrameError> {
            let end = self.end.ok_or(FrameError::MissingEndFrame)?;
            if end.contract != CHECK_RESULT_CONTRACT {
                return Err(FrameError::ResultContract);
            }
            if end.plan_sha256 != expect.plan_sha256 {
                return Err(FrameError::PlanShaMismatch);
            }
            if end.subject_uid != expect.subject_uid {
                return Err(FrameError::SubjectUidMismatch);
            }

            let mut streams: BTreeMap<Stream, Vec<u8>> = BTreeMap::new();
            for (name, summary) in &end.streams {
                let stream = Stream::parse(name).ok_or(FrameError::UnknownStream)?;
                let Some(got) = self.parts.get(&stream) else {
                    return Err(FrameError::UndeclaredStream(name.clone()));
                };
                if got.len() as u32 != summary.parts {
                    return Err(FrameError::PartCountMismatch {
                        stream: stream.as_str(),
                        declared: summary.parts,
                        got: got.len() as u32,
                    });
                }
                let mut encoded = String::new();
                for seq in 1..=summary.parts {
                    let Some(chunk) = got.get(&seq) else {
                        return Err(FrameError::MissingPart {
                            stream: stream.as_str(),
                            seq,
                            total: summary.parts,
                        });
                    };
                    encoded.push_str(chunk);
                }
                let bytes = b64()
                    .decode(encoded.as_bytes())
                    .map_err(|_| FrameError::Base64)?;
                if crate::ids::sha256_prefixed(&bytes) != summary.sha256 {
                    return Err(FrameError::StreamDigestMismatch {
                        stream: stream.as_str(),
                    });
                }
                streams.insert(stream, bytes);
            }
            // A part for a stream the end frame does NOT declare is a
            // mismatch too: it means the writer and its own summary disagree.
            for stream in self.parts.keys() {
                if !end.streams.contains_key(stream.as_str()) {
                    return Err(FrameError::PartCountMismatch {
                        stream: stream.as_str(),
                        declared: 0,
                        got: self.parts[stream].len() as u32,
                    });
                }
            }

            match &end.topic_lines {
                Some(t) => {
                    if self.topics.len() as u32 != t.count {
                        return Err(FrameError::TopicCountMismatch {
                            declared: t.count,
                            got: self.topics.len() as u32,
                        });
                    }
                    if super::topic_tsv_sha256(&self.topics) != t.sha256 {
                        return Err(FrameError::TopicDigestMismatch);
                    }
                }
                None => {
                    if !self.topics.is_empty() {
                        return Err(FrameError::TopicCountMismatch {
                            declared: 0,
                            got: self.topics.len() as u32,
                        });
                    }
                }
            }

            Ok(CheckRelay {
                topics: self.topics,
                streams,
                end,
            })
        }
    }

    fn parse_topic(body: &str) -> Result<TopicEntry, FrameError> {
        let mut it = body.split('\t');
        let (Some(name), Some(partitions), Some(flags), None) =
            (it.next(), it.next(), it.next(), it.next())
        else {
            return Err(FrameError::Malformed { kind: "topic" });
        };
        if name.is_empty() {
            return Err(FrameError::Malformed { kind: "topic" });
        }
        let partitions: u32 = partitions
            .parse()
            .map_err(|_| FrameError::Malformed { kind: "topic" })?;
        let mut entry = TopicEntry::new(name, partitions);
        if flags != "-" {
            for f in flags.split(',') {
                match f {
                    "internal" => entry.internal = true,
                    "expected" => entry.expected = true,
                    other => {
                        let code = other
                            .strip_prefix("error:")
                            .and_then(super::CheckCode::parse)
                            .ok_or(FrameError::Malformed { kind: "topic" })?;
                        entry.error = Some(code);
                    }
                }
            }
        }
        Ok(entry)
    }
}

// ------------------------------------------------------------ result stream

/// How one expected topic resolved (D2 §5.2 step 5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ExpectedTopicState {
    /// Present in the listing, or a targeted request answered with metadata.
    Visible,
    /// The broker answered `TOPIC_AUTHORIZATION_FAILED` — which it does for a
    /// principal without `DESCRIBE` whether or not the topic exists, so this
    /// says nothing about existence and everything about visibility.
    NotAuthorized,
    /// `UNKNOWN_TOPIC_OR_PARTITION`.
    NotFound,
    /// The targeted request did not complete within its budget.
    Unknown,
}

/// The per-name results of the expected-topic probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ExpectedTopicResult {
    pub name: String,
    pub state: ExpectedTopicState,
}

/// The counts D2 §5.1 publishes under `status.result.expected`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ExpectedSummary {
    pub requested: u32,
    pub visible: u32,
    pub not_authorized: u32,
    pub not_found: u32,
    pub unknown: u32,
}

/// `status.result.counts`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct InventoryCounts {
    /// Entries the broker returned.
    pub listed: u32,
    /// Entries relayed after internal exclusion and truncation.
    pub returned: u32,
    pub internal_excluded: u32,
    pub errored: u32,
}

/// The inventory half of a check result. Carries SIGNALS, never a visibility
/// verdict: [`visibility`] is computed by the controller, which is the only
/// side that holds the administrator attestation (D2 §5.2 step 6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct InventoryResult {
    pub format: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker_count: Option<u32>,
    pub counts: InventoryCounts,
    #[serde(default)]
    pub truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub truncation_reason: Option<TruncationReason>,
    /// (i) of D2 §5.4: a listing entry carried `TopicAuthorizationFailed`.
    #[serde(default)]
    pub topic_authorization_error_in_listing: bool,
    pub expected: ExpectedSummary,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub expected_results: Vec<ExpectedTopicResult>,
    /// `sha256:` over [`topic_tsv`] of the relayed entries.
    pub topics_sha256: String,
}

/// One relayed evidence object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EvidenceObjectResult {
    pub key: String,
    pub stream: Stream,
    /// `true` only when the object was read. `false` requires a `NotFound`
    /// from the backend — a denial is `present: false` with a `code`, never a
    /// claim of absence (the `NotFound` versus `Io` distinction
    /// `logweir_store::StoreError` already makes).
    pub present: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<CheckCode>,
    /// `true` when the object was longer than the request's `maxBytes`, so the
    /// relayed bytes are a prefix and the digest is NOT the object's digest.
    #[serde(default)]
    pub truncated: bool,
}

/// The `result` stream document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CheckResult {
    pub contract: String,
    pub kind: CheckPlanKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inventory: Option<InventoryResult>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checks: Vec<CheckOutcome>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<EvidenceObjectResult>,
}

impl CheckResult {
    #[must_use]
    pub fn new(kind: CheckPlanKind) -> Self {
        Self {
            contract: CHECK_RESULT_CONTRACT.to_string(),
            kind,
            inventory: None,
            checks: Vec::new(),
            evidence: Vec::new(),
        }
    }

    /// Canonical bytes for the `result` stream: the repository's deterministic
    /// JSON, so the digest the end frame declares is reproducible by anyone
    /// holding the same document.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, crate::det_json::DetJsonError> {
        crate::det_json::to_deterministic_json(self)
    }

    /// D2 §6.4's "≤ 64 entries", ENFORCED rather than declared.
    ///
    /// [`MAX_CHECK_ENTRIES`] existed as a constant with no reader, so a W4 or
    /// W9 bug that emitted 400 outcomes would have written a half-megabyte
    /// status and nothing would have refused it. A declared bound is not a
    /// bound.
    pub fn validate(&self) -> Result<(), CheckResultError> {
        if self.checks.len() > MAX_CHECK_ENTRIES {
            return Err(CheckResultError::TooManyChecks(self.checks.len()));
        }
        if self.contract != CHECK_RESULT_CONTRACT {
            return Err(CheckResultError::Contract(self.contract.clone()));
        }
        Ok(())
    }

    /// Redacts and caps every message, remedy and fact IN PLACE.
    ///
    /// The fields of [`CheckOutcome`] are public and `Deserialize`d directly,
    /// so a document decoded from a relay carries whatever the runner wrote:
    /// `redact` and `cap` run inside `with_message` / `with_remedy` /
    /// `with_fact`, which a decoder never calls. D2 §4.1 applies redaction "to
    /// every relayed status message", i.e. on the controller side too, so the
    /// controller needs ONE call here rather than a hand-rolled walk in
    /// `weirkeeper::check::relay`. `CheckRelay::result` already calls it.
    pub fn sanitise(&mut self) {
        for c in &mut self.checks {
            c.message = redact(&c.message);
            c.remedy = redact(&c.remedy);
            for v in c.facts.values_mut() {
                *v = redact(v);
            }
        }
    }
}

/// Why a decoded `result` document was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CheckResultError {
    #[error("the result document declares {0} checks, over the {MAX_CHECK_ENTRIES} cap")]
    TooManyChecks(usize),
    #[error("the result document's contract `{0}` is not `{CHECK_RESULT_CONTRACT}`")]
    Contract(String),
    #[error("the result document does not parse")]
    Parse,
}

// ---------------------------------------------------------------- redaction

/// One redaction rule. The rules are a LIST rather than one function body so
/// that a test can run every rule BUT ONE and observe the secret survive —
/// "a guard without a mutant is not a guard", and a redactor is the guard
/// where a silently deleted clause is least likely to be noticed.
#[derive(Clone, Copy)]
pub struct RedactionRule {
    pub name: &'static str,
    pub apply: fn(&str) -> String,
}

impl std::fmt::Debug for RedactionRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedactionRule")
            .field("name", &self.name)
            .finish()
    }
}

/// The marker every rule substitutes.
pub const REDACTED: &str = "[redacted]";

/// The rules, IN THE ORDER [`redact`] applies them. PEM first, because a PEM
/// body would otherwise be chopped up by the long-run rule and the marker
/// lines left behind; the S3 XML rule next, because it discards a whole body
/// and the later rules would only rewrite parts of it.
#[must_use]
pub fn redaction_rules() -> &'static [RedactionRule] {
    &[
        RedactionRule {
            name: "pem",
            apply: redact_pem,
        },
        RedactionRule {
            name: "s3-xml",
            apply: redact_s3_xml,
        },
        RedactionRule {
            name: "url-userinfo",
            apply: redact_url_userinfo,
        },
        RedactionRule {
            name: "secret-key-value",
            apply: redact_key_values,
        },
        RedactionRule {
            name: "aws-access-key-id",
            apply: redact_access_key_ids,
        },
        RedactionRule {
            name: "long-base64-or-hex-run",
            apply: redact_long_runs,
        },
    ]
}

/// THE chokepoint. Every `message`, `remedy`, relayed status message and
/// relayed fact passes through it before it can reach a `ConfigMap`, a status,
/// a log line or an API response (D2 §4.1, §6.5).
///
/// It removes URL userinfo, AWS access key ids, secret/password/token value
/// forms, PEM blocks, S3 XML bodies (keeping only `<Code>`) and unstructured
/// base64 or hex runs of 40 characters or more, then caps the result at
/// [`MESSAGE_MAX_CHARS`] characters.
///
/// # What it deliberately KEEPS
///
/// Redaction is by key NAME and by secret SHAPE, never by "any long token".
/// A public identifier an operator needs in order to act — a `signerKeyId`, a
/// content digest, an `imageID`, a Secret or data-key name, an object key, a
/// segment path — survives whole; see [`is_public_identifier`]. A credential
/// VALUE does not, whichever of the six rules catches it.
#[must_use]
pub fn redact(s: &str) -> String {
    cap(&apply_rules(s, redaction_rules()))
}

/// [`redact`] with an explicit rule list — the entry point the mutant tests
/// use to delete one rule and observe the leak.
#[must_use]
pub fn apply_rules(s: &str, rules: &[RedactionRule]) -> String {
    rules.iter().fold(s.to_string(), |acc, r| (r.apply)(&acc))
}

/// Truncates to [`MESSAGE_MAX_CHARS`] CHARACTERS (not bytes — a cap that split
/// a multi-byte character would panic on the slice).
#[must_use]
pub fn cap(s: &str) -> String {
    if s.chars().count() <= MESSAGE_MAX_CHARS {
        return s.to_string();
    }
    let mut out: String = s.chars().take(MESSAGE_MAX_CHARS - 1).collect();
    out.push('…');
    out
}

fn redact_pem(s: &str) -> String {
    // A PEM block is `-----BEGIN <label>-----` … `-----END <label>-----`.
    // Anything between the two markers goes, and so do the markers.
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find("-----BEGIN ") {
        out.push_str(&rest[..start]);
        let tail = &rest[start..];
        match tail.find("-----END ") {
            Some(end_at) => {
                // Consume through the closing marker's own terminator when
                // there is one, so the label does not survive.
                let after_end = &tail[end_at..];
                let consumed = match after_end
                    .find("-----\n")
                    .or_else(|| after_end.find("-----"))
                {
                    Some(i) => end_at + i + 5,
                    None => tail.len(),
                };
                out.push_str(REDACTED);
                rest = &tail[consumed..];
            }
            None => {
                // An unterminated block: everything from the marker on is
                // suspect, so none of it is kept.
                out.push_str(REDACTED);
                return out;
            }
        }
    }
    out.push_str(rest);
    out
}

fn redact_s3_xml(s: &str) -> String {
    // An S3 error body is `<?xml …?><Error><Code>…</Code><Message>…</Message>
    // <RequestId>…</RequestId><HostId>…</HostId></Error>`. Only `<Code>` is
    // kept: `Message`, `RequestId` and `HostId` carry the request's own
    // identifiers and sometimes the key, and no remedy needs them.
    //
    // EVERY block, not the first. S3 and MinIO return several `<Error>`
    // elements in one body for `DeleteObjects` and multipart completion, and
    // the first version of this rule re-emitted everything after the first
    // `</Error>` untouched.
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find("<Error>") {
        // A leading `<?xml …?>` declaration belongs to the body.
        let head = &rest[..start];
        let head_end = head.rfind("<?xml").unwrap_or(start);
        out.push_str(&rest[..head_end]);
        let tail = &rest[start..];
        let (body, after) = match tail.find("</Error>") {
            Some(i) => tail.split_at(i + "</Error>".len()),
            None => (tail, ""),
        };
        out.push_str(&format!(
            "<Error><Code>{}</Code></Error>",
            s3_error_code(body)
        ));
        rest = after;
    }
    out.push_str(rest);
    out
}

/// The `<Code>` of one `<Error>` block, or `Unknown`. Restricted to
/// alphanumerics and a sane length so a hostile body cannot smuggle text
/// through the one element this rule keeps.
fn s3_error_code(body: &str) -> &str {
    body.find("<Code>")
        .and_then(|i| {
            let after = &body[i + "<Code>".len()..];
            after.find("</Code>").map(|j| &after[..j])
        })
        .filter(|c| {
            !c.is_empty() && c.len() <= 64 && c.chars().all(|ch| ch.is_ascii_alphanumeric())
        })
        .unwrap_or("Unknown")
}

fn redact_url_userinfo(s: &str) -> String {
    // `scheme://user:password@host…` -> `scheme://host…`.
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find("://") {
        let (head, tail) = rest.split_at(i + 3);
        out.push_str(head);
        // The authority ends at the first `/`, `?`, `#`, whitespace or quote.
        let auth_end = tail
            .find(|c: char| c == '/' || c == '?' || c == '#' || c.is_whitespace() || c == '"')
            .unwrap_or(tail.len());
        let (authority, after) = tail.split_at(auth_end);
        match authority.rsplit_once('@') {
            Some((_creds, host)) => out.push_str(host),
            None => out.push_str(authority),
        }
        rest = after;
    }
    out.push_str(rest);
    out
}

/// Where a keyword may appear before its value is redacted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeywordForm {
    /// `k=v`, `k: v`, `k => v`, `"k": "v"`, `k v`.
    Any,
    /// The quoted-key form ONLY: `"k": v` / `'k': v`. Used for the bare word
    /// `secret`, which in prose names a Kubernetes Secret — D2 §6.5 says a
    /// Secret NAME may appear in a message, so `secret \`logweir-s3\` key
    /// \`access-key-id\`` must survive while `{"secret":"…"}` must not.
    QuotedKeyOnly,
}

/// The keyword set of D2 §4.1 — `aws_secret_access_key`,
/// `secret[_-]?access[_-]?key`, `password`, `sasl.password`, `token` — plus
/// the bare `secret` in its quoted-key form, ordered LONGEST FIRST so the more
/// specific spelling wins.
const SECRET_KEYWORDS: [(&str, KeywordForm); 8] = [
    ("aws_secret_access_key", KeywordForm::Any),
    ("secret_access_key", KeywordForm::Any),
    ("secret-access-key", KeywordForm::Any),
    ("secretaccesskey", KeywordForm::Any),
    ("sasl.password", KeywordForm::Any),
    ("password", KeywordForm::Any),
    ("token", KeywordForm::Any),
    ("secret", KeywordForm::QuotedKeyOnly),
];

/// Removes the VALUE after any of [`SECRET_KEYWORDS`], case-insensitively,
/// in every quoting and spacing shape a real error uses.
///
/// # The shapes, and why each one is here
///
/// - `password=hunter2`, `password: hunter2`, `password => hunter2` — config
///   and log forms.
/// - `{"password":"hunter2"}`, `"sasl.password": "hunter2"` — JSON and
///   quoted-key YAML. **This is the form the first version missed**: it
///   skipped only spaces and tabs after the keyword and then required a
///   separator, so the `"` between the two abandoned the match and the value
///   was copied verbatim. W5's `waiting.rs` relays an admission-webhook or
///   API-server body — which is JSON — through `redact` into a status message,
///   so a short password in that body reached the `Preflight` status, the
///   details `ConfigMap`, the API and the UI. Values of 40+ base64/hex
///   characters were still caught by the long-run rule; `password` and
///   `sasl.password` are exactly the short ones.
/// - `--token hunter2` — a bare whitespace separator, **argv form only**: the
///   keyword must sit in flag position, immediately after a `-`. An earlier
///   version accepted whitespace after ANY occurrence of the keyword, so the
///   kubelet's own `couldn't find key password in Secret <ns>/<name>` lost the
///   word `in` and became `key password [redacted] Secret …`. That sentence is
///   the one an operator acts on, the following word is English and not a
///   value, and a credential written as bare prose (`password hunter2`, no
///   flag, no separator) is not a form any config, log or API body uses.
/// - `sessionToken=abcdef` — the keyword as a SUFFIX of a longer identifier.
///   The first version required the preceding byte to be non-alphanumeric and
///   its comment claimed `mypassword` was caught; it was not. Matching a
///   suffix over-redacts (`notoken=1` loses its `1`), which is the safe
///   direction.
/// - `password=[redacted]` — a value that is ALREADY the marker is copied
///   whole. Without that clause the `]` terminated the value one byte early,
///   the scanner re-wrote `[redacted` as `[redacted]` and left the old `]`
///   behind, so every extra pass grew another bracket. [`redact`] runs at
///   least twice on any published row — once in `CheckOutcome::with_message`
///   and again when the controller renders the entry — which is how live
///   statuses came to read `key password [redacted]]] Secret`.
///
/// The keyword must NOT be a prefix of a longer identifier — `tokenizer` is a
/// word, not a key — which is what keeps `the tokenizer failed` intact.
fn redact_key_values(s: &str) -> String {
    let lower = s.to_ascii_lowercase();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0usize;
    'outer: while i < bytes.len() {
        for (kw, form) in SECRET_KEYWORDS {
            if !lower[i..].starts_with(kw) {
                continue;
            }
            let mut p = i + kw.len();
            // A keyword that is the PREFIX of a longer identifier is a word,
            // not a key: `tokenizer`, `password_file`.
            if bytes
                .get(p)
                .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_')
            {
                continue;
            }
            // An optional closing quote: `"password"` / `'password'`.
            let quoted_key = matches!(bytes.get(p), Some(b'"') | Some(b'\''));
            if quoted_key {
                p += 1;
            }
            if form == KeywordForm::QuotedKeyOnly && !quoted_key {
                continue;
            }
            let before_ws = p;
            while matches!(bytes.get(p), Some(b' ') | Some(b'\t')) {
                p += 1;
            }
            let had_ws = p > before_ws;
            let sep = if lower[p..].starts_with("=>") {
                2
            } else if matches!(bytes.get(p), Some(b'=') | Some(b':')) {
                1
            } else if had_ws
                && !quoted_key
                && form == KeywordForm::Any
                && i > 0
                && bytes[i - 1] == b'-'
            {
                // `--token hunter2`: whitespace alone separates them, and
                // ONLY in flag position. See the doc comment.
                0
            } else {
                continue;
            };
            p += sep;
            while matches!(bytes.get(p), Some(b' ') | Some(b'\t')) {
                p += 1;
            }
            // Already redacted: copy the marker whole, brackets included, so a
            // second pass is a no-op rather than another `]`.
            if s[p..].starts_with(REDACTED) {
                let end = p + REDACTED.len();
                out.push_str(&s[i..end]);
                i = end;
                continue 'outer;
            }
            let (value_start, terminator): (usize, fn(u8) -> bool) = match bytes.get(p) {
                Some(b'"') => (p + 1, |c| c == b'"'),
                Some(b'\'') => (p + 1, |c| c == b'\''),
                _ => (p, |c| {
                    matches!(
                        c,
                        b' ' | b'\t'
                            | b'\n'
                            | b'\r'
                            | b','
                            | b';'
                            | b'&'
                            | b'}'
                            | b']'
                            | b')'
                            | b'"'
                    )
                }),
            };
            let mut e = value_start;
            while e < bytes.len() && !terminator(bytes[e]) {
                e += 1;
            }
            if e == value_start {
                // Nothing between the separator and the terminator.
                out.push_str(&s[i..p]);
                i = p;
                continue 'outer;
            }
            out.push_str(&s[i..value_start]);
            out.push_str(REDACTED);
            i = e;
            continue 'outer;
        }
        // Not a keyword start: copy one CHARACTER, so UTF-8 survives.
        let ch = s[i..].chars().next().expect("i is a char boundary");
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

fn redact_access_key_ids(s: &str) -> String {
    // `(AKIA|ASIA)[A-Z0-9]{16}` — an AWS access key id. 20 characters, which
    // is under the long-run rule's threshold, so it needs its own rule.
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0usize;
    while i < bytes.len() {
        let looks = (s[i..].starts_with("AKIA") || s[i..].starts_with("ASIA"))
            && bytes.len() >= i + 20
            && bytes[i + 4..i + 20]
                .iter()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit());
        if looks {
            out.push_str(REDACTED);
            i += 20;
            continue;
        }
        let ch = s[i..].chars().next().expect("i is a char boundary");
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// The length at which an unkeyed run is treated as material.
///
/// FORTY. An AWS secret access key is EXACTLY 40 characters of
/// `[A-Za-z0-9/+=]`, which is what sets it, and nothing shorter is redacted by
/// shape alone — the short forms are [`redact_key_values`]'s, by key name.
const LONG_RUN_MIN: usize = 40;

/// A run of 40 or more characters from the base64 / hex alphabet that carries
/// no PUBLIC structure.
///
/// The threshold is an AWS secret access key's length; the EXEMPTION is D2
/// §6.5's other half. A key id, a content digest, an image id, a backup set's
/// UUID, an object key and a segment path are all longer than forty characters
/// and all public — they are on the `TrustRoster`, in the manifest, in the
/// bucket listing — and a remedy that names none of them ("do not restore
/// until `[redacted]` is recovered") cannot be acted on. Redaction is by key
/// NAME ([`redact_key_values`]) and by known secret SHAPE (PEM, S3 bodies,
/// URL userinfo, AWS access key ids); "any long token" is not a shape.
fn redact_long_runs(s: &str) -> String {
    redact_runs_where(s, is_public_identifier)
}

/// The long-run rule as `logweir::check::redact_path` applies it: the same
/// scanner, the same threshold, and one extra way for a run to be public —
/// [`is_object_key_shaped`].
///
/// `redact_path`'s values are archive object keys and Kafka topic names, often
/// already wrapped in a JSON line, so the decision has to be made RUN BY RUN
/// exactly as [`redact`] makes it. Its previous spelling split the value on `/`
/// and ran the rule over each piece, which lowered every piece below the
/// threshold and let a `/`-bearing credential through whole.
#[must_use]
pub fn redact_long_runs_in_keys(s: &str) -> String {
    redact_runs_where(s, |run| {
        is_public_identifier(run) || is_object_key_shaped(run)
    })
}

/// The scanner both spellings share: maximal runs of the base64/hex alphabet,
/// replaced at [`LONG_RUN_MIN`] unless `keep` calls the run public.
fn redact_runs_where(s: &str, keep: impl Fn(&str) -> bool) -> String {
    fn is_run_char(c: char) -> bool {
        c.is_ascii_alphanumeric() || c == '+' || c == '/' || c == '=' || c == '_' || c == '-'
    }
    let mut out = String::with_capacity(s.len());
    let mut run = String::new();
    let flush = |out: &mut String, run: &mut String| {
        if run.chars().count() >= LONG_RUN_MIN && !keep(run) {
            out.push_str(REDACTED);
        } else {
            out.push_str(run);
        }
        run.clear();
    };
    for c in s.chars() {
        if is_run_char(c) {
            run.push(c);
            continue;
        }
        flush(&mut out, &mut run);
        out.push(c);
    }
    flush(&mut out, &mut run);
    out
}

/// A lowercase hex string of SHA-256 or SHA-512 width.
///
/// The two widths Logweir prints, and only those: a `signerKeyId` is the
/// SHA-256 of a SubjectPublicKeyInfo DER, an `imageID` and a `manifestSha256`
/// are SHA-256, and `sha512` is the one other digest the formats allow. A
/// forty-character hex run is NOT exempt — that is a SHA-1 nobody here prints
/// and the width several hosted services use for bearer tokens, so it keeps
/// the conservative answer.
fn is_hex_digest(c: &str) -> bool {
    matches!(c.len(), 64 | 128) && c.bytes().all(|b| b.is_ascii_hexdigit())
}

/// `8-4-4-4-12` hex — a Kubernetes UID or a backup set id.
fn is_uuid(c: &str) -> bool {
    let groups: Vec<&str> = c.split('-').collect();
    groups.len() == 5
        && [8usize, 4, 4, 4, 12]
            .iter()
            .zip(&groups)
            .all(|(n, g)| g.len() == *n && g.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// A NAME rather than material: lower case, and short enough that it cannot be
/// a secret on its own.
///
/// DNS-1123 object names, Kafka topic names, `partition=2`,
/// `segment-00000000000000000000` and `manifest` are all of this form. Upper
/// case is what separates it from base64 — a forty-character base64 run with no
/// upper-case letter at all has probability `(38/64)^40 ≈ 2e-9` — and `+` is
/// excluded for the same reason.
fn is_public_name(c: &str) -> bool {
    c.len() < LONG_RUN_MIN
        && c.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'-' | b'_' | b'=')
        })
}

/// Anything an object key may hold once the run is known to BE an object key:
/// no `+`, and no component long enough to be a credential.
fn is_key_component(c: &str) -> bool {
    c.len() < LONG_RUN_MIN
        && c.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_' | b'='))
}

/// `<factName>=<public identifier>` — a fact rendered into a message.
///
/// The shipped `Preflight` CRD carries no `facts` map, so the controller folds
/// them into `message` as `[key=value; …]` and the whole sentence goes through
/// [`redact`] again. `=` is in the run alphabet, so `signerKeyId=` plus a
/// SHA-256 is ONE run of 76 characters and the digest half would otherwise be
/// unreachable by any of the forms above.
///
/// The key half is deliberately narrow — lower camel case, letters and digits
/// only, at most 24 characters, exactly one `=`, a non-empty value — because
/// the alternative (allowing any short mixed-case component) would let
/// `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` through. An AWS secret access key
/// is 30 random bytes as 40 unpadded base64 characters and contains no `=` at
/// all; a padded base64 blob's `=` is trailing, which leaves the whole blob on
/// the KEY side of the split, where `+`, `/` and the 24-character cap refuse
/// it.
///
/// The VALUE half must be a digest or a UUID and NOTHING ELSE. Every fact
/// `entry_of` folds into a message is one of those two (`signerKeyId`,
/// `imageID`, `podUID`, `clusterId`), so the clause costs nothing; accepting a
/// lower-case run as well let an unkeyed `k=v` credential ride out under any
/// name an adopter's tool happens to print — `datadogapikey=<32 hex>` is 46
/// characters, over the threshold, and was returned whole (review finding F2).
const FACT_KEY_MAX: usize = 24;

fn is_fact_pair(c: &str) -> bool {
    if c.bytes().filter(|b| *b == b'=').count() != 1 {
        return false;
    }
    let Some((key, value)) = c.split_once('=') else {
        return false;
    };
    let key_ok = !key.is_empty()
        && key.len() <= FACT_KEY_MAX
        && key.as_bytes()[0].is_ascii_lowercase()
        && key.bytes().all(|b| b.is_ascii_alphanumeric());
    key_ok && (is_hex_digest(value) || is_uuid(value))
}

/// Whether a long run is a public identifier rather than material.
///
/// The run is split on `/` — an object key's own separator — and answered two
/// ways:
///
/// * **every component is a public FORM**: a SHA-256/512 digest, a UUID, a
///   lower-case name, or a `<factName>=<identifier>` pair. This is what lets a
///   bare `signerKeyId`, an `imageID`'s digest, a Secret `<ns>/<name>` and a
///   prefix-joined manifest key through.
/// * **or the run is ANCHORED** by a component that is a UUID or a digest — a
///   backup set id or a content digest — which makes the run an object key
///   rather than a token, and **at most one** of its remaining components may
///   be something other than a public form. Kafka topic names may carry upper
///   case, and the strict clause above would have redacted the whole segment
///   path for `payments-EU`; a segment key has exactly ONE adopter-chosen
///   component, which is that topic name.
///
/// # Why the anchored branch is capped at one
///
/// Review finding **F1**, critical. Without the cap the branch asked only that
/// every component be short and `[A-Za-z0-9._=-]` — which the canonical AWS
/// secret access key satisfies component by component, because its own `/`
/// characters split its 40 into 13, 7 and 18. So the moment a backup set id
/// shared the token run, the credential was "an object key" and survived
/// whole, where the unstructured rule had replaced it. A 40-character key with
/// *n* internal slashes yields *n*+1 upper-case-bearing components, and at
/// *n* = 0 the single component is 40 characters and already fails
/// [`is_key_component`]'s length cap — so the whole family dies at one of the
/// two clauses, while a real segment path keeps its one adopter-chosen name.
///
/// An AWS secret access key satisfies no branch: its components carry upper
/// case (and often `+`), nothing in it is a UUID or a digest, and it has more
/// than one component that is neither.
fn is_public_identifier(run: &str) -> bool {
    let components: Vec<&str> = run.split('/').collect();
    let public = |c: &&str| {
        c.is_empty() || is_hex_digest(c) || is_uuid(c) || is_public_name(c) || is_fact_pair(c)
    };
    if components.iter().all(public) {
        return true;
    }
    // The anchor may itself be a digest: a digest component is a public form,
    // so it satisfies the `all` below through `public` rather than through
    // `is_key_component`, whose length cap no 64- or 128-character component
    // can meet. Before that was made explicit the `is_hex_digest` disjunct in
    // the anchor test was dead code.
    components.iter().any(|c| is_uuid(c) || is_hex_digest(c))
        && components.iter().all(|c| public(c) || is_key_component(c))
        && components.iter().filter(|c| !public(c)).count() <= 1
}

/// Whether a VALUE is shaped like an object key: `/`-separated components,
/// none of them long enough to be a credential on its own, and **at most one**
/// that is not a public form.
///
/// This is [`is_public_identifier`]'s anchored clause with the anchor
/// requirement dropped, and it exists for exactly one caller —
/// `logweir::check::redact_path`, which is applied to values already known to
/// be archive object keys and Kafka topic names rather than to prose. A message
/// may say anything, so [`redact`] insists on a UUID or a digest before it will
/// read a run as a key; a `missingSegment` value cannot, so the same run may be
/// read as a key on the strength of its shape alone.
///
/// The ONE non-public component is the adopter-chosen one: a backup set id like
/// `20260915T030000Z`, which carries upper case and is neither a UUID nor a
/// digest, or a topic name like `payments-EU`. A credential is refused by the
/// same two clauses that refuse it in [`is_public_identifier`]: unsliced it is
/// 40 characters and fails the length cap, and its own `/` characters —
/// base64's 64th — split it into two or three components that are all
/// non-public.
#[must_use]
pub fn is_object_key_shaped(value: &str) -> bool {
    let components: Vec<&str> = value.split('/').collect();
    let public = |c: &str| {
        c.is_empty() || is_hex_digest(c) || is_uuid(c) || is_public_name(c) || is_fact_pair(c)
    };
    components.iter().all(|c| public(c) || is_key_component(c))
        && components.iter().filter(|c| !public(c)).count() <= 1
}

// --------------------------------------------------------------- visibility

/// The completeness vocabulary — `unknown | limited | attestedComplete`, and
/// nothing else, everywhere (D-SEAMS S3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum VisibilityState {
    /// A successful listing ALONE. Kafka silently omits topics the principal
    /// may not `DESCRIBE`, so this is the honest default and not a degraded
    /// answer.
    Unknown,
    /// An authorization failure was OBSERVED — in a listing entry or on a
    /// targeted request for an expected topic.
    Limited,
    /// An administrator attestation matched the observed cluster id and
    /// principal and has not expired. Logweir did not verify the claim; it
    /// records who made it and when.
    AttestedComplete,
}

impl VisibilityState {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Limited => "limited",
            Self::AttestedComplete => "attestedComplete",
        }
    }
}

/// Why the state is what it is. Sorted, deduplicated and bounded, so a UI can
/// render an explanation without a second computation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum VisibilityBasis {
    ListingOnly,
    TopicAuthorizationErrorInListing,
    ExpectedTopicNotAuthorized,
    ExpectedTopicsAllVisible,
    Truncated,
    AdministratorAttestation,
    AttestationExpired,
    AttestationPrincipalMismatch,
    AttestationClusterIdMismatch,
}

/// An administrator's attestation, read from the installation policy
/// `ConfigMap` (D2 §4.4). Only a principal who can write that `ConfigMap` in
/// the release namespace can create one; a namespace operator cannot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Attestation {
    pub id: String,
    pub namespace: String,
    pub kafka_cluster: String,
    pub cluster_id: String,
    pub principal: String,
    pub attested_by: String,
    pub attested_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub statement: String,
}

/// What an attestation-backed result records. Never a claim Logweir verified:
/// the UI renders "attested by <attestedBy> at <attestedAt>; not verified by
/// Logweir" (D2 §5.4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AttestationRef {
    pub id: String,
    pub attested_by: String,
    pub attested_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

/// What was OBSERVED, which is the only input to [`visibility`] besides the
/// attestation and the clock reading the caller passes in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct VisibilitySignals {
    pub namespace: String,
    pub kafka_cluster: String,
    /// The cluster id the runner read from the broker, not the one cached on
    /// the `KafkaCluster` status.
    pub cluster_id: String,
    /// `User:<name>` or `User:ANONYMOUS`.
    pub principal: String,
    /// (i) of D2 §5.4.
    pub topic_authorization_error_in_listing: bool,
    pub expected: ExpectedSummary,
    pub truncated: bool,
}

/// The computed verdict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Visibility {
    pub state: VisibilityState,
    pub basis: Vec<VisibilityBasis>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attestation: Option<AttestationRef>,
}

/// D2 §5.4's state algorithm, and NOTHING else may compute completeness.
///
/// ```text
/// limited          if topicAuthorizationErrorInListing || expected.notAuthorized > 0
/// attestedComplete else if an attestation matches (namespace, kafkaCluster name,
///                         observed clusterId, principal) && now < expiresAt && !truncated
/// unknown          otherwise
/// ```
///
/// `now` is a PARAMETER: this crate reads no clock (Global Constraint 1), and
/// making the caller supply the instant is also what makes expiry testable.
///
/// An attestation whose namespace or `kafkaCluster` name does not match is not
/// a candidate at all and leaves no basis entry — it is about a different
/// cluster. A principal or cluster-id mismatch DOES leave one, because those
/// are the two dimensions an operator would expect the attestation to cover
/// and silently ignoring them is how "attested" would drift onto the wrong
/// credential.
#[must_use]
pub fn visibility(
    signals: &VisibilitySignals,
    attestation: Option<&Attestation>,
    now: DateTime<Utc>,
) -> Visibility {
    let mut basis: Vec<VisibilityBasis> = Vec::new();
    let limited =
        signals.topic_authorization_error_in_listing || signals.expected.not_authorized > 0;
    if signals.topic_authorization_error_in_listing {
        basis.push(VisibilityBasis::TopicAuthorizationErrorInListing);
    }
    if signals.expected.not_authorized > 0 {
        basis.push(VisibilityBasis::ExpectedTopicNotAuthorized);
    }
    if signals.expected.requested > 0
        && signals.expected.not_authorized == 0
        && signals.expected.visible == signals.expected.requested
    {
        basis.push(VisibilityBasis::ExpectedTopicsAllVisible);
    }
    if signals.truncated {
        basis.push(VisibilityBasis::Truncated);
    }

    // Is there a candidate attestation, and does it apply?
    let candidate = attestation
        .filter(|a| a.namespace == signals.namespace && a.kafka_cluster == signals.kafka_cluster);
    let mut attested: Option<AttestationRef> = None;
    if let Some(a) = candidate {
        let mut applies = true;
        if a.principal != signals.principal {
            basis.push(VisibilityBasis::AttestationPrincipalMismatch);
            applies = false;
        }
        if a.cluster_id != signals.cluster_id {
            basis.push(VisibilityBasis::AttestationClusterIdMismatch);
            applies = false;
        }
        if now >= a.expires_at {
            basis.push(VisibilityBasis::AttestationExpired);
            applies = false;
        }
        if applies && !limited && !signals.truncated {
            basis.push(VisibilityBasis::AdministratorAttestation);
            attested = Some(AttestationRef {
                id: a.id.clone(),
                attested_by: a.attested_by.clone(),
                attested_at: a.attested_at,
                expires_at: a.expires_at,
            });
        }
    }

    let state = if limited {
        VisibilityState::Limited
    } else if attested.is_some() {
        VisibilityState::AttestedComplete
    } else {
        VisibilityState::Unknown
    };

    if basis.is_empty() {
        basis.push(VisibilityBasis::ListingOnly);
    }
    basis.sort_unstable();
    basis.dedup();
    Visibility {
        state,
        basis,
        attestation: attested,
    }
}

// ------------------------------------------------------------ binding digest

/// One object a check result depends on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Referent {
    pub kind: String,
    pub namespace: String,
    pub name: String,
    pub uid: String,
    /// `None` for a kind whose generation is not meaningful (a `Backup` is
    /// identified by UID alone in D2 §6.2's example).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<i64>,
}

/// One destination's CA bundle digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CaBundleRef {
    pub destination_uid: String,
    pub sha256: String,
}

/// The `TrustRoster` a signer check was evaluated against.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RosterRef {
    pub uid: String,
    pub generation: i64,
}

/// The `Approval` a restore preflight was evaluated against. The RESOURCE
/// VERSION is in the digest on purpose: it changes when verification status
/// moves, so a preflight taken while an approval was pending goes stale the
/// moment it is verified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ApprovalRef {
    pub uid: String,
    pub resource_version: String,
}

/// Everything a check result is bound to (D2 §6.6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingInputs {
    pub operation: CheckOperation,
    pub plan_hash: Option<String>,
    /// Backup only: the exact topic set. Sorted by [`inputs_digest`].
    pub topics: Option<Vec<String>>,
    pub referents: Vec<Referent>,
    pub ca_bundles: Vec<CaBundleRef>,
    pub roster: RosterRef,
    pub approval: Option<ApprovalRef>,
    pub policy_digest: String,
}

/// The canonical document [`inputs_digest`] hashes. Declaration order IS the
/// serialisation order (`det_json`), so the bytes are reproducible.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct CanonicalBinding<'a> {
    version: u32,
    operation: CheckOperation,
    #[serde(skip_serializing_if = "Option::is_none")]
    plan_hash: Option<&'a String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    topics: Option<Vec<&'a String>>,
    referents: Vec<&'a Referent>,
    ca_bundles: Vec<&'a CaBundleRef>,
    roster: &'a RosterRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    approval: Option<&'a ApprovalRef>,
    policy_digest: &'a String,
}

/// `sha256:` over the canonical JSON of [`BindingInputs`] (D2 §6.6).
///
/// The API recomputes this from CURRENT objects and compares it with the
/// recorded one; any difference makes the stored result inapplicable. That is
/// the whole invalidation mechanism, so the normalisation (sorting topics,
/// referents and CA bundles) happens HERE and not at any call site — two
/// callers sorting differently would make a result spuriously stale, and a
/// caller that forgot to sort would make it spuriously fresh.
#[must_use]
pub fn inputs_digest(b: &BindingInputs) -> String {
    let mut topics: Option<Vec<&String>> = b.topics.as_ref().map(|t| t.iter().collect());
    if let Some(t) = topics.as_mut() {
        t.sort();
        t.dedup();
    }
    let mut referents: Vec<&Referent> = b.referents.iter().collect();
    referents
        .sort_by(|a, c| (&a.kind, &a.namespace, &a.name).cmp(&(&c.kind, &c.namespace, &c.name)));
    let mut ca_bundles: Vec<&CaBundleRef> = b.ca_bundles.iter().collect();
    ca_bundles.sort_by(|a, c| a.destination_uid.cmp(&c.destination_uid));
    let doc = CanonicalBinding {
        version: 1,
        operation: b.operation,
        plan_hash: b.plan_hash.as_ref(),
        topics,
        referents,
        ca_bundles,
        roster: &b.roster,
        approval: b.approval.as_ref(),
        policy_digest: &b.policy_digest,
    };
    let bytes = crate::det_json::to_deterministic_json(&doc)
        .expect("the canonical binding carries no float and cannot fail to serialise");
    crate::ids::sha256_prefixed(&bytes)
}

/// Why a stored check result is no longer applicable (D2 §6.6).
///
/// D2's five reasons, plus [`StaleReason::InputsDigestChanged`]. The sixth is
/// ADDITIVE and exists so this function can never report "stale" without
/// saying why: the recorded and recomputed digests can differ through a field
/// the other five do not name (the topic set of a Backup readiness request,
/// for instance). A consumer that knows only D2's five renders it as a
/// generic "the inputs changed", which is exactly what it means.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StaleReason {
    Expired,
    PlanHashChanged,
    /// `referentChanged:<Kind>/<name>`.
    ReferentChanged(String),
    CaBundleChanged,
    PolicyChanged,
    InputsDigestChanged,
}

impl std::fmt::Display for StaleReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Expired => f.write_str("expired"),
            Self::PlanHashChanged => f.write_str("planHashChanged"),
            Self::ReferentChanged(w) => write!(f, "referentChanged:{w}"),
            Self::CaBundleChanged => f.write_str("caBundleChanged"),
            Self::PolicyChanged => f.write_str("policyChanged"),
            Self::InputsDigestChanged => f.write_str("inputsDigestChanged"),
        }
    }
}

/// The applicability test of D2 §6.6, as ONE function so the API and the UI
/// cannot each implement half of it.
///
/// `recorded` is the binding the check stored; `current` is the binding the
/// caller recomputed from live objects. The returned list is EMPTY exactly
/// when the result still applies, and every entry names a concrete reason:
/// a stale flag with no reason is the defect PLAT-03.2 is about ("a green
/// preview cannot bypass a later collision" needs the user to know WHY the
/// preview went away).
///
/// `now` is a parameter for the same reason it is in [`visibility`].
#[must_use]
pub fn stale_reasons(
    recorded: &BindingInputs,
    recorded_expires_at: Option<DateTime<Utc>>,
    current: &BindingInputs,
    now: DateTime<Utc>,
) -> Vec<StaleReason> {
    let mut out: Vec<StaleReason> = Vec::new();
    if recorded_expires_at.is_none_or(|e| now >= e) {
        out.push(StaleReason::Expired);
    }
    if recorded.plan_hash != current.plan_hash {
        out.push(StaleReason::PlanHashChanged);
    }

    // Referents, by identity (kind, namespace, name), so an added, removed or
    // re-created object is each named.
    let key = |r: &Referent| (r.kind.clone(), r.namespace.clone(), r.name.clone());
    let recorded_map: BTreeMap<_, _> = recorded.referents.iter().map(|r| (key(r), r)).collect();
    let current_map: BTreeMap<_, _> = current.referents.iter().map(|r| (key(r), r)).collect();
    let mut names: Vec<&(String, String, String)> =
        recorded_map.keys().chain(current_map.keys()).collect();
    names.sort();
    names.dedup();
    for k in names {
        let a = recorded_map.get(k);
        let b = current_map.get(k);
        let changed = match (a, b) {
            (Some(a), Some(b)) => a.uid != b.uid || a.generation != b.generation,
            _ => true,
        };
        if changed {
            out.push(StaleReason::ReferentChanged(format!("{}/{}", k.0, k.2)));
        }
    }

    if recorded.roster != current.roster {
        out.push(StaleReason::ReferentChanged(format!(
            "TrustRoster/{}",
            current.roster.uid
        )));
    }
    if recorded.approval != current.approval {
        let uid = current
            .approval
            .as_ref()
            .or(recorded.approval.as_ref())
            .map_or_else(String::new, |a| a.uid.clone());
        out.push(StaleReason::ReferentChanged(format!("Approval/{uid}")));
    }
    // Sorted on BOTH sides, exactly as `inputs_digest` sorts them. Comparing
    // positionally made two bindings with the same digest report
    // `caBundleChanged` forever: a two-destination restore preflight where the
    // controller resolved source-then-evidence and the API recomputed from a
    // map got the other order, so every GET answered `stale` and the restore
    // could never be approved from the UI. It failed safe and was undiagnosable.
    let sorted = |v: &[CaBundleRef]| {
        let mut c: Vec<CaBundleRef> = v.to_vec();
        c.sort_by(|a, b| a.destination_uid.cmp(&b.destination_uid));
        c
    };
    if sorted(&recorded.ca_bundles) != sorted(&current.ca_bundles) {
        out.push(StaleReason::CaBundleChanged);
    }
    if recorded.policy_digest != current.policy_digest {
        out.push(StaleReason::PolicyChanged);
    }

    // The catch-all, last: the digest is the authority on "did anything
    // change", and a difference no named reason explains must still be
    // reported rather than swallowed.
    let digests_differ = inputs_digest(recorded) != inputs_digest(current);
    let only_expiry = out.iter().all(|r| *r == StaleReason::Expired);
    if digests_differ && only_expiry {
        out.push(StaleReason::InputsDigestChanged);
    }
    out
}
