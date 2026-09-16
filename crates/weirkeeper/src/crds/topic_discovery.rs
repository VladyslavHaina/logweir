//! `TopicDiscovery` — one bounded, time-limited observation of the topics a
//! saved connection can see.
//!
//! # A request, not a cache (ADR 0008 Amendment F)
//!
//! Every object here is ONE observation with a recorded instant. `weirkeeper`
//! turns it into one isolated runner-image Job in this namespace; that Job
//! carries no Kubernetes token and uses the same credential projection
//! execution does. The result is advisory: no reconciler and no runner ever
//! treats it as authorization or as a substitute for an execution-time guard.
//!
//! # Why the spec is split into `request` and `cancelRequested`
//!
//! `spec.request` is a REQUIRED object, so a transition rule attached to it is
//! evaluated on every update. That is what closes the absent → present hole
//! [`super::backup_schedule`]'s module header measures: a per-field
//! `self == oldSelf` on an OPTIONAL field never fires, and `optionalOldSelf`
//! is 1.30+, above the 1.29 floor. One rule on the required sub-object
//! therefore seals the whole request, and `cancelRequested` — the one thing an
//! operator may change — sits outside it with a monotonic rule of its own.
//!
//! # The name is unconstrained, deliberately
//!
//! The Job name is derived from this object's UID and never from its name, so
//! there is no name-length path that could refuse a request at admission and
//! no `NameTooLong` terminal state for this kind.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{Condition, LocalRef, Time};

/// The Kafka topic-name grammar, as the broker enforces it.
pub const TOPIC_NAME_PATTERN: &str = r"^[a-zA-Z0-9._-]{1,249}$";

/// T1 — the request is immutable; a new observation is a new object.
pub const T1_REQUEST_IMMUTABLE_RULE: &str = super::SPEC_IMMUTABLE_RULE;
/// T1's message.
pub const T1_REQUEST_IMMUTABLE_MESSAGE: &str =
    "spec.request is immutable; create a new TopicDiscovery";

/// T2 — `cancelRequested` moves only from `false` to `true`.
///
/// AN UNCANCEL IS NOT A THING. The controller may already have deleted the
/// Job; letting the flag fall back to `false` would ask a reconciler to
/// resurrect work it has provably stopped.
pub const CANCEL_MONOTONIC_RULE: &str = "(!has(oldSelf.cancelRequested) || !oldSelf.cancelRequested) || (has(self.cancelRequested) && self.cancelRequested)";
/// The message [`CANCEL_MONOTONIC_RULE`] travels with.
pub const CANCEL_MONOTONIC_MESSAGE: &str =
    "spec.cancelRequested may only change from false to true";

/// The rules on `.spec` itself.
pub const SPEC_RULES: [super::SpecRule; 1] = [super::SpecRule::new(
    CANCEL_MONOTONIC_RULE,
    CANCEL_MONOTONIC_MESSAGE,
)];

/// The transition rule attached to `.spec.request`.
pub const REQUEST_RULE: (&[&str], &str, &str) = (
    &["request"],
    T1_REQUEST_IMMUTABLE_RULE,
    T1_REQUEST_IMMUTABLE_MESSAGE,
);

/// The default topic ceiling.
pub const DEFAULT_MAX_TOPICS: i32 = 20_000;
/// The default in-Job Kafka budget, in seconds.
pub const DEFAULT_TIMEOUT_SECONDS: i32 = 60;

fn default_max_topics() -> i32 {
    DEFAULT_MAX_TOPICS
}
fn default_timeout_seconds() -> i32 {
    DEFAULT_TIMEOUT_SECONDS
}

/// What to observe. Sealed by T1 once created.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicDiscoveryRequest {
    /// The `KafkaCluster` to list topics from, in this namespace.
    pub connection_ref: LocalRef,
    /// Whether Kafka's internal topics (`__consumer_offsets` and the rest) are
    /// counted in the returned inventory. Absent means `false`; they are
    /// always counted separately in `counts.internalExcluded`.
    #[serde(default)]
    pub include_internal: bool,
    /// Topics the requester expects to exist. Each one is reported as visible,
    /// not authorized, not found or unknown, which is the only way to tell
    /// "the topic is gone" from "this principal cannot describe it" — Kafka
    /// silently omits both.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 500), inner(regex(path = "TOPIC_NAME_PATTERN")))]
    pub expected_topics: Option<Vec<String>>,
    /// The ceiling on returned entries. The installation policy may lower it,
    /// never raise it.
    #[serde(default = "default_max_topics")]
    #[schemars(range(min = 1, max = 50000))]
    pub max_topics: i32,
    /// The in-Job Kafka budget.
    #[serde(default = "default_timeout_seconds")]
    #[schemars(range(min = 10, max = 300))]
    pub timeout_seconds: i32,
}

/// `TopicDiscovery.spec`.
#[derive(kube::CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "logweir.dev",
    version = "v1alpha1",
    kind = "TopicDiscovery",
    doc = "One bounded observation of the topics a saved connection can see, executed as an isolated Job with no Kubernetes token. `spec.request` is immutable and `spec.cancelRequested` may only move from false to true. The result is ADVISORY: it is never authorization and never replaces an execution-time guard.",
    plural = "topicdiscoveries",
    singular = "topicdiscovery",
    namespaced,
    status = "TopicDiscoveryStatus",
    printcolumn = r#"{"name":"CONNECTION","type":"string","jsonPath":".spec.request.connectionRef.name"}"#,
    printcolumn = r#"{"name":"PHASE","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"VISIBILITY","type":"string","jsonPath":".status.result.visibility.state","description":"unknown, limited or attestedComplete — a successful listing alone is unknown"}"#,
    printcolumn = r#"{"name":"TOPICS","type":"integer","jsonPath":".status.result.counts.returned"}"#,
    printcolumn = r#"{"name":"OBSERVED","type":"date","jsonPath":".status.observedAt"}"#,
    printcolumn = r#"{"name":"AGE","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct TopicDiscoverySpec {
    /// What to observe. Immutable (T1).
    pub request: TopicDiscoveryRequest,
    /// Ask the controller to stop. `false` → `true` only (T2).
    #[serde(default)]
    pub cancel_requested: bool,
}

/// What the observation was taken against, recorded so a reader can tell
/// whether the connection has changed since.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveryBinding {
    /// The `KafkaCluster` name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_name: Option<String>,
    /// Its UID: a same-named replacement is a different connection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_uid: Option<String>,
    /// Its `metadata.generation` at resolution time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_generation: Option<i64>,
    /// The SASL principal Logweir PRESENTED — `User:<name>`, or
    /// `User:ANONYMOUS`. Kafka exposes no call for the principal the broker
    /// authenticated, so this is what was offered and never what was accepted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
    /// `plaintext` or `scramSha512`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_mode: Option<String>,
    /// `sha256:<lowercase hex>` over the resolved bootstrap list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrap_sha256: Option<String>,
    /// `sha256:<lowercase hex>` over the installation policy in force.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_digest: Option<String>,
}

/// How much of the cluster this observation can honestly claim to have seen.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveryVisibility {
    /// `unknown`, `limited` or `attestedComplete`. **A successful listing
    /// alone is `unknown`**: Kafka omits topics a principal cannot describe,
    /// so a complete-looking list proves nothing about completeness.
    pub state: String,
    /// Why the state is what it is — `expectedTopicNotAuthorized`,
    /// `describeAclDenied`, and the rest of the closed basis vocabulary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 8))]
    pub basis: Option<Vec<String>>,
    /// The administrator attestation that upgraded the state to
    /// `attestedComplete`, when there was one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attestation: Option<String>,
}

/// How many topics were listed, returned, excluded and errored.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveryCounts {
    /// Entries the broker listed.
    pub listed: i64,
    /// Entries in the stored inventory.
    pub returned: i64,
    /// Internal topics left out.
    pub internal_excluded: i64,
    /// Entries the broker reported an error for.
    pub errored: i64,
}

/// What became of each `expectedTopics` entry.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExpectedTopicOutcome {
    /// How many were asked about.
    pub requested: i64,
    /// How many the principal can see.
    pub visible: i64,
    /// How many the broker refused to describe.
    pub not_authorized: i64,
    /// How many are provably absent.
    pub not_found: i64,
    /// How many could not be classified.
    pub unknown: i64,
}

/// One immutable `ConfigMap` chunk of the inventory.
///
/// THE NAMES DO NOT LIVE IN THE STATUS. A topic inventory is unbounded and a
/// status is not a store; the chunks are owned, immutable `ConfigMap`s and
/// this block carries their names and digests only.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct InventoryChunk {
    /// The `ConfigMap` name, in this namespace, owned by this object.
    pub name: String,
    /// `sha256:<lowercase hex>` over the chunk's canonical TSV.
    pub sha256: String,
    /// How many entries it carries.
    pub count: i64,
    /// The first topic name in it, so a reader can seek without fetching.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_name: Option<String>,
    /// The last topic name in it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_name: Option<String>,
}

/// The inventory this observation produced.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicInventoryResult {
    /// `logweir.dev/topic-inventory/v1` — the format a reader must understand.
    pub format: String,
    /// The cluster id READ FROM THE BROKER, never from a spec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_id: Option<String>,
    /// How many brokers answered metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker_count: Option<i64>,
    /// The four counts.
    pub counts: DiscoveryCounts,
    /// Whether the inventory was cut short.
    #[serde(default)]
    pub truncated: bool,
    /// `MaxTopics` or `RelayLimit`, when it was.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub truncation_reason: Option<String>,
    /// The completeness claim, and its basis.
    pub visibility: DiscoveryVisibility,
    /// The `expectedTopics` verdict, when any were asked about.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected: Option<ExpectedTopicOutcome>,
    /// `sha256:<lowercase hex>` over the canonical TSV of every returned
    /// entry, so a consumer can prove the chunks it read are the ones this
    /// status describes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topics_sha256: Option<String>,
    /// The `ConfigMap` chunks holding the names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 64))]
    pub chunks: Option<Vec<InventoryChunk>>,
}

/// `TopicDiscovery.status`.
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicDiscoveryStatus {
    /// `Pending`, `Queued`, `Running`, `Succeeded`, `Failed` or `Cancelled`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// The reason of the condition this patch writes, promoted to a scalar —
    /// `Succeeded`, `ConnectionNotFound`, `ConnectionInvalid`,
    /// `ResultUnreadable`, `RunnerContractUnsupported`, `DeadlineExceeded`,
    /// `CancelRequested`, `Stalled`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// A redacted, bounded explanation. Never a credential, never a broker
    /// error body, never a URL carrying userinfo.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 1024))]
    pub message: Option<String>,
    /// What the observation was taken against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding: Option<DiscoveryBinding>,
    /// The Job that ran, or is running, the observation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_ref: Option<LocalRef>,
    /// When the Job was created.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued_at: Option<Time>,
    /// When the runner container started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Time>,
    /// **When the observation was taken** — the runner container's
    /// `finishedAt`, the same rule the `KafkaCluster` probe uses. Not the
    /// instant the controller noticed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<Time>,
    /// `observedAt` plus the installation policy's freshness window. After it,
    /// a consumer treats the inventory as stale rather than wrong.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fresh_until: Option<Time>,
    /// The inventory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<TopicInventoryResult>,
    /// The condition set. One type, `Complete`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}
