//! How a run decides which topics it covers — the one type `Backup` and
//! `BackupSchedule` both use.
//!
//! # Two modes, and the third shape is refused
//!
//! | `topics` | `allUserTopics` | Mode |
//! |---|---|---|
//! | non-empty | absent | **SelectedTopics** — today's named allowlist, unchanged |
//! | `[]` | present | **AllUserTopics** — resolved per run by discovery |
//! | anything else | | refused before any POST |
//!
//! `topics` STAYS REQUIRED AND STAYS PRESENT IN BOTH MODES. That is what keeps
//! an older controller able to DESERIALIZE a dynamic object at all: it reads
//! `topics: []`, renders an empty list, and the runner's own empty-list rail
//! (`phase_minus1_admit.rs`) exits 3 without contacting the engine. Making
//! `topics` optional would have turned a rollback into a reflector decode
//! error across the whole kind — the same reason D2's destination sentinel
//! keeps `archive` required.
//!
//! # `incompleteDiscovery` is required and has no default
//!
//! Kafka silently omits topics a principal cannot describe, so **no discovery
//! can prove whole-cluster visibility** without an administrator attestation.
//! Defaulting to refusal would make dynamic mode unusable out of the box;
//! defaulting to visible-only would silently weaken the promise the mode's own
//! name makes. So the choice is the user's, it is explicit, and the schema has
//! no `default` for it — an operator who has not thought about it cannot
//! accidentally ship either answer.
//!
//! Every run then records what it actually covered, and no surface renders
//! "all topics" unless the coverage is `AllUserTopicsAttested`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The Kafka topic-name grammar, as the broker enforces it. Guard **G-GLOB**:
/// a glob metacharacter is not in this set, so a pattern cannot be written in
/// an exclusion any more than in an allowlist.
pub const TOPIC_NAME_PATTERN: &str = r"^[a-zA-Z0-9._-]{1,249}$";

/// A topic-name PREFIX. The same character set as a name, and deliberately not
/// a pattern language: `orders-` excludes `orders-eu` because it is a literal
/// prefix, and `orders*` is not writable at all.
pub const TOPIC_PREFIX_PATTERN: &str = r"^[a-zA-Z0-9._-]{1,249}$";

/// What to leave out of a dynamic selection.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicExclusions {
    /// Exact names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 1000), inner(regex(path = "TOPIC_NAME_PATTERN")))]
    pub topics: Option<Vec<String>>,
    /// Literal prefixes, never patterns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 32), inner(regex(path = "TOPIC_PREFIX_PATTERN")))]
    pub prefixes: Option<Vec<String>>,
}

/// What to do when discovery cannot prove it saw everything.
///
/// **REQUIRED, WITH NO DEFAULT.** See the module header: both possible
/// defaults are wrong in a way the operator would not notice.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
pub enum IncompleteDiscovery {
    /// Fail the run. The strict answer: a backup labelled "all user topics"
    /// that silently missed the topics this principal cannot describe is worse
    /// than no backup, because somebody will restore from it.
    Refuse,
    /// Back up what was visible, and label the run
    /// `VisibleUserTopicsOnly` — coverage not established — everywhere it is
    /// rendered.
    BackUpVisibleTopics,
}

/// Dynamic selection: every user topic the run's principal can see, minus the
/// exclusions.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AllUserTopics {
    /// What to leave out.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude: Option<TopicExclusions>,
    /// What to do about incomplete visibility. Required.
    pub incomplete_discovery: IncompleteDiscovery,
}

/// Which selection mode a run used.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
pub enum SelectionMode {
    /// A named allowlist, from `spec.topics`.
    SelectedTopics,
    /// Resolved from discovery, from `spec.allUserTopics`.
    AllUserTopics,
}

/// How much of the cluster a run can honestly claim to have covered.
///
/// # Three labels, and why the middle one exists
///
/// `NamedTopics` claims nothing about the cluster; the user named a set and
/// the run covered it. `AllUserTopicsAttested` is the only label that means
/// "everything", and it is reachable only through an administrator
/// attestation, because Kafka cannot be asked. `VisibleUserTopicsOnly` is the
/// honest middle: the run covered what its principal could see and nobody has
/// established that this was all of it.
///
/// The signed receipt attests the exact named set and nothing more; coverage
/// is controller-recorded and labelled as such.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
pub enum Coverage {
    /// The user named the topics.
    NamedTopics,
    /// Every user topic, with an attestation that this was all of them.
    AllUserTopicsAttested,
    /// The topics this principal could see. **Completeness not established.**
    VisibleUserTopicsOnly,
}

impl Coverage {
    /// The label every surface renders, verbatim (D1 §7.4).
    ///
    /// ONE FUNCTION, SO THE THREE SURFACES CANNOT DISAGREE. The API, the
    /// console and `kubectl describe` all read this; a second wording
    /// somewhere else is how "visible user topics only" becomes "all topics"
    /// in the one place an auditor reads.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::NamedTopics => "Named topics",
            Self::AllUserTopicsAttested => "All user topics (attested complete)",
            Self::VisibleUserTopicsOnly => {
                "Visible user topics only — completeness not established"
            }
        }
    }

    /// Whether this coverage may be rendered as "all topics".
    ///
    /// EXACTLY ONE VALUE MAY, and a caller that asks this question by hand is
    /// a caller that can get it wrong once.
    #[must_use]
    pub const fn claims_whole_cluster(self) -> bool {
        matches!(self, Self::AllUserTopicsAttested)
    }
}

/// What a run's topic resolution actually found.
///
/// NAMES ARE DELIBERATELY ABSENT. A resolved list is unbounded and a status is
/// not a store; the names live in the run's immutable execution inputs and in
/// the signed receipt, and this block carries counts, a digest and the instant
/// the observation was taken.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SelectionStatus {
    /// Which mode the run used.
    pub mode: SelectionMode,
    /// What it may claim to have covered.
    pub coverage: Coverage,
    /// `unknown`, `limited` or `attestedComplete`, as discovery reported it.
    /// **A successful listing alone is `unknown`.**
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visibility: Option<String>,
    /// How many topics the run froze.
    pub resolved_topic_count: i64,
    /// How many bytes those names take, so a reader can see the run
    /// approaching the plan-size bound before it hits it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_topic_bytes: Option<i64>,
    /// How many internal topics were excluded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub internal_excluded_count: Option<i64>,
    /// How many the exclusion rules removed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub excluded_by_rule_count: Option<i64>,
    /// How many the broker refused to describe. **Non-zero with coverage
    /// `VisibleUserTopicsOnly` is the case this whole block exists to make
    /// visible.**
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limited_topic_count: Option<i64>,
    /// When discovery observed the cluster.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery_observed_at: Option<super::Time>,
    /// `sha256:<lowercase hex>` over the discovery result the run froze.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery_sha256: Option<String>,
}
