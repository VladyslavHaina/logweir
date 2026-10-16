//! Dynamic topic selection for one run (D1 §7.2).
//!
//! The second producer of a [`ResolvedSelection`]: where
//! [`ResolvedSelection::named`] reads `spec.topics`, this resolves
//! `spec.allUserTopics` by running one `topicInventory` check Job owned by the
//! `Backup` itself, classifying what it saw, and handing the exact byte-sorted
//! names to the same freeze the named path uses.
//!
//! # The rulings it is built on
//!
//! * **Seam S1, one check runner.** Discovery runs
//!   `logweir check run --plan <file> --check-contract-version 1` with plan kind
//!   `topicInventory` through [`crate::check`] (not D1 §7.3's superseded
//!   `logweir topics discover`), reusing its frames, digests and closed error
//!   codes.
//! * **Seam S2, a discovery result is never an execution input.** Nothing here
//!   reads a `TopicDiscovery`. A dynamic run discovers afresh in its own Job and
//!   freezes the names into its own immutable plan, so its topic set is never
//!   stale and a retry (a new `Backup`) rediscovers.
//! * **Seam S3, completeness.** `unknown | limited | attestedComplete` is
//!   [`logweir_core::check_contract::visibility`]'s, and D1's coverage labels
//!   are derived from it ([`coverage_for`]).
//! * **Seam S6.** The relay is read off the pod
//!   [`crate::check::pod::find_owned_pod`] proved, never off a label match.
//! * **Seams S4/S7.** The names land in the one frozen execution grammar, and
//!   every status write is a `resourceVersion`-preconditioned merge PATCH.
//!
//! # One pass
//!
//! `resolve` is called only from `controllers::backup`'s freeze branch, before
//! `status.execution` is recorded, and answers:
//!
//! | answer | what the controller does |
//! |---|---|
//! | [`Resolution::Pending`] | nothing more this pass; the reconciler's own 15 s requeue is D1 §7.2 R2's |
//! | [`Resolution::Resolved`] | freeze it, exactly as a named allowlist is frozen |
//! | [`Resolution::Refused`] | stop: the terminal status is already on the object |
//!
//! A refusal is written here rather than raised as `BackupError::Refused`
//! because `TopicsResolved` and `Failed` must land in the same patch: a merge
//! patch replaces `status.conditions`, and the shared refusal writer builds its
//! array from the `Backup` this pass started with, which would publish `Failed`
//! beside a stale `TopicsResolved: DiscoveryRunning` (D1 §3.4).
//!
//! # What this module never does
//!
//! It dials no broker, reads no `Secret`, writes no archive and deletes
//! nothing. Its two objects (the discovery Job and its immutable plan
//! `ConfigMap`) are owned by the `Backup` with `controller: true`, so the
//! `Backup`'s cascade collects them and no `delete` verb is needed.

use chrono::{DateTime, Utc};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ConfigMap, Pod};
use kube::api::{Patch, PatchParams, PostParams};
use kube::{Api, Resource as _, ResourceExt as _};
use serde::Serialize;
use serde_json::{json, Value};
use tracing::{debug, info, warn};

use logweir_core::check_contract::{
    redact, visibility, CheckCode, CheckOutcome, CheckPlan, CheckPlanKind, CheckRequest,
    CheckState, ConnectionPlan, FrameExpectations, Gating, InventoryResult, TopicEntry,
    TopicInventoryRequest, VisibilityState, CHECK_CONTRACT_VERSION, CHECK_PLAN_CONTRACT,
    DEFAULT_RELAY_BUDGET_BYTES,
};

use crate::backup_execution::{
    DiscoveryInputs, DiscoveryNames, ExclusionsInputs, ResolvedSelection, SelectionInputs,
};
use crate::check::{self, job as cjob, plan, policy};
use crate::conditions::{
    current_condition, merge_condition, status_unchanged, StatusVersion, CONDITION_TOPICS_RESOLVED,
    REASON_DISCOVERY_RUNNING, REASON_RESOLVED, TERMINAL_STATE_DISCOVERY_FAILED,
    TERMINAL_STATE_DISCOVERY_INCOMPLETE, TERMINAL_STATE_DISCOVERY_RESULT_UNREADABLE,
    TERMINAL_STATE_INVALID_TOPIC_SELECTION, TERMINAL_STATE_JOB_NAME_CONFLICT,
    TERMINAL_STATE_POD_CREATION_FORBIDDEN, TERMINAL_STATE_REFERENT_NOT_FOUND,
    TERMINAL_STATE_SELECTION_EMPTY, TERMINAL_STATE_SELECTION_TOO_LARGE,
    TERMINAL_STATE_SOURCE_CHANGED_DURING_RESOLUTION,
};
use crate::connection::{self, ConnectionUse, ResolvedConnection};
use crate::controllers::backup::{
    carry_conditions, job_finished, refused_status_patch, BackupError,
};
use crate::crds::backup::Backup;
use crate::crds::kafka_cluster::KafkaCluster;
use crate::crds::selection::{AllUserTopics, Coverage, IncompleteDiscovery, SelectionMode};
use crate::crds::Condition;
use crate::job::{RunnerImage, RunnerOwner};
use crate::runner_log;

/// The prefix of a per-run discovery Job's name (D1 §7.2 R2):
/// `lwd-<backup uid>`, always 40 characters. Not
/// [`crate::check::job::check_job_name`]'s hashed form, so an operator with
/// `kubectl` can find it from the `Backup`'s UID; it is recorded verbatim in
/// the frozen `selection.discovery.discoveryJob`.
pub const DISCOVERY_JOB_PREFIX: &str = "lwd-";

/// `logweir.dev/purpose` on the discovery Job and its pod — D1 §7.2 R2.
pub const LABEL_PURPOSE: &str = "logweir.dev/purpose";
/// The value of [`LABEL_PURPOSE`].
pub const PURPOSE_TOPIC_DISCOVERY: &str = "topic-discovery";

/// `app.kubernetes.io/component` on the discovery Job and its pod.
///
/// `run-discovery`, not [`cjob::COMPONENT_CHECK`]: one `list` finds both
/// populations, so [`crate::check::limits::active_selector`] counts a run's
/// discovery against the per-connection ceiling on simultaneous dials, while
/// [`crate::check::limits::check_selector`] (the console pool) selects `check`
/// alone, so a nightly schedule cannot queue a browser click. A run's discovery
/// goes through no `limits::admit`.
pub const COMPONENT_RUN_DISCOVERY: &str = "run-discovery";

/// The annotation carrying the digest of the source resolution the discovery
/// Job was dispatched against (D1 §7.2 R1 and R4): the record R4 compares
/// against in a later pass. On the Job, not in the plan `ConfigMap`, because the
/// plan's digest also covers the request's limits, and a mid-run policy edit
/// would otherwise read as a changed source.
pub const SOURCE_SHA256_ANNOTATION: &str = "logweir.dev/discovery-source-sha256";

/// D1 §7.2 R8: the most names one run may freeze, the topic budget's maximum
/// (`logweir_core::topic_budget`). A larger backup would sign a receipt the
/// recovery catalog cannot list and this controller cannot verify, so a dynamic
/// selection over it is refused `SelectionTooLarge` before any runner Job
/// exists, naming both numbers; nothing is truncated to fit.
pub const MAX_RESOLVED_TOPICS: usize = logweir_core::topic_budget::MAX_BACKUP_TOPICS;

/// D1 §7.2 R8: the most bytes those names may take.
///
/// A quarter of the 1 MiB a `ConfigMap` holds, which leaves room for the rest
/// of the frozen document beside the list.
pub const MAX_RESOLVED_TOPIC_BYTES: usize = 256 * 1024;

/// D1 §7.2 R2: the ceiling on a discovery Job's `activeDeadlineSeconds`.
pub const MAX_DISCOVERY_SECONDS: i64 = 300;

/// The `maxTopics` a run's discovery plan asks for.
///
/// Many times [`MAX_RESOLVED_TOPICS`]: a truncated listing is refused
/// ([`SelectionTooLarge`]), because freezing the first N names of a cut-off
/// listing would claim coverage over a set nobody chose, and asking for far
/// more gives the precise refusal ("more user topics than one run may name",
/// after internal topics and exclusions) rather than "the listing was cut". A
/// constant, not the policy's `hardMaxTopics`, which bounds tenant requests.
///
/// [`SelectionTooLarge`]: crate::conditions::TERMINAL_STATE_SELECTION_TOO_LARGE
pub const DISCOVERY_MAX_TOPICS: u32 = 20_000;

/// How many internal names the frozen `selection.discovery` records — D1 §3.3.
pub const MAX_INTERNAL_NAMES: usize = 50;

/// How many rule-excluded names it records — D1 §3.3.
pub const MAX_EXCLUDED_NAMES: usize = 200;

/// The most bytes of names the frozen `selection.discovery` block may carry,
/// `internalExcluded.names` and `excludedByRule.names` together.
///
/// The count bounds alone admit 250 names of 249 bytes (about 61 KiB) beside a
/// topic list already bounded at [`MAX_RESOLVED_TOPIC_BYTES`] in a one-MiB
/// `ConfigMap`. These lists are a sample an operator reads, not a store: the
/// counts stay exact and `truncated` says when the sample was cut. Spent in a
/// fixed order (internal first, then rule-excluded) so two runs that observed
/// the same cluster freeze the same bytes.
pub const MAX_PROVENANCE_NAME_BYTES: usize = 16 * 1024;

/// `selection.discovery.basis` — how the listing was obtained.
pub const DISCOVERY_BASIS: &str = "metadata-list";

/// The discovery Job's name for a `Backup` UID.
///
/// A PURE FUNCTION OF THE UID, so a duplicate reconcile computes the same name
/// and gets a 409 from the API server rather than starting a second discovery.
#[must_use]
pub fn discovery_job_name(backup_uid: &str) -> String {
    format!("{DISCOVERY_JOB_PREFIX}{backup_uid}")
}

/// The exclusions of one dynamic policy, canonicalised.
///
/// SORTED AND DEDUPLICATED, because the frozen block carries them and two runs
/// that applied the same exclusions must freeze the same bytes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Exclusions {
    /// Exact names.
    pub topics: Vec<String>,
    /// Literal prefixes.
    pub prefixes: Vec<String>,
}

impl Exclusions {
    /// The exclusions `policy` states.
    #[must_use]
    pub fn from_policy(policy: &AllUserTopics) -> Self {
        let canonical = |v: Option<&Vec<String>>| -> Vec<String> {
            let mut out: Vec<String> = v.cloned().unwrap_or_default();
            out.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
            out.dedup();
            out
        };
        let exclude = policy.exclude.as_ref();
        Self {
            topics: canonical(exclude.and_then(|e| e.topics.as_ref())),
            prefixes: canonical(exclude.and_then(|e| e.prefixes.as_ref())),
        }
    }

    /// Whether `name` is excluded: literal in both halves (`==` and
    /// [`str::starts_with`]). Guard **G-GLOB**: neither field is a pattern
    /// language, and a `*` that somehow arrived excludes nothing rather than
    /// everything.
    #[must_use]
    pub fn excludes(&self, name: &str) -> bool {
        self.topics.iter().any(|t| t == name)
            || self
                .prefixes
                .iter()
                .any(|p| !p.is_empty() && name.starts_with(p.as_str()))
    }

    /// The frozen rendering of these exclusions, or `None` when there are none.
    #[must_use]
    pub fn inputs(&self) -> Option<ExclusionsInputs> {
        if self.topics.is_empty() && self.prefixes.is_empty() {
            return None;
        }
        Some(ExclusionsInputs {
            topics: self.topics.clone(),
            prefixes: self.prefixes.clone(),
        })
    }
}

/// What [`classify`] made of one relayed inventory — D1 §7.2 R5.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Classification {
    /// The names this run will freeze: visible − internal − limited −
    /// excluded, byte-sorted and deduplicated.
    pub resolved: Vec<String>,
    /// The internal topics that were taken out.
    pub internal: Vec<String>,
    /// The names an exclusion rule took out.
    pub excluded: Vec<String>,
    /// The names the broker refused to describe.
    pub limited: Vec<String>,
    /// How many entries the principal could see at all.
    pub visible: usize,
}

/// Split a relayed inventory into the four buckets D1 §7.2 R5 names. The order
/// of the tests decides where a name that qualifies for two lands:
///
/// 1. **internal**: `entry.internal` or a `__` prefix. Both, so an older or a
///    lying runner cannot slip `__consumer_offsets` into a backup
///    ([`TopicEntry::name_is_internal`]).
/// 2. **limited**: the entry carries an error, such as
///    [`CheckCode::TopicAuthorizationFailed`]; freezing it would put a name in
///    `backup.yaml` the run cannot read.
/// 3. **excludedByRule**: [`Exclusions::excludes`].
/// 4. everything left is **resolved**.
///
/// Sorted by bytes (`String`'s `Ord`), which every digest over the frozen list
/// depends on; re-applied here rather than taken on a runner's word.
#[must_use]
pub fn classify(entries: &[TopicEntry], exclusions: &Exclusions) -> Classification {
    let mut out = Classification {
        visible: entries.len(),
        ..Classification::default()
    };
    for entry in entries {
        let name = entry.name.clone();
        if entry.internal || TopicEntry::name_is_internal(&entry.name) {
            out.internal.push(name);
        } else if entry.error.is_some() {
            out.limited.push(name);
        } else if exclusions.excludes(&entry.name) {
            out.excluded.push(name);
        } else {
            out.resolved.push(name);
        }
    }
    for bucket in [
        &mut out.resolved,
        &mut out.internal,
        &mut out.excluded,
        &mut out.limited,
    ] {
        bucket.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        bucket.dedup();
    }
    out
}

/// A bounded sample of `names` for the frozen provenance block, by count and by
/// bytes, spending from a shared `budget`. The count is always exact; only the
/// sample is cut, and `truncated` says so. Both bounds are needed: a count
/// alone admits 249-byte names, a byte bound alone a hundred thousand
/// one-character ones.
fn bounded(names: &[String], cap: usize, budget: &mut usize) -> DiscoveryNames {
    let mut kept: Vec<String> = Vec::new();
    for name in names.iter().take(cap) {
        if name.len() > *budget {
            break;
        }
        *budget -= name.len();
        kept.push(name.clone());
    }
    DiscoveryNames {
        count: i64::try_from(names.len()).unwrap_or(i64::MAX),
        truncated: kept.len() < names.len(),
        names: kept,
    }
}

/// What the run may claim, from the visibility verdict and the policy (D1 §7.2
/// R6 and §7.4, seam **S3**). `None` means refuse: the policy chose
/// [`IncompleteDiscovery::Refuse`], which is terminal `DiscoveryIncomplete`.
/// Only `VisibilityState::AttestedComplete`, reachable only through an
/// administrator attestation, gives [`Coverage::AllUserTopicsAttested`], so a
/// successful listing alone never claims whole-cluster coverage.
#[must_use]
pub fn coverage_for(state: VisibilityState, policy: IncompleteDiscovery) -> Option<Coverage> {
    match state {
        VisibilityState::AttestedComplete => Some(Coverage::AllUserTopicsAttested),
        VisibilityState::Unknown | VisibilityState::Limited => match policy {
            IncompleteDiscovery::BackUpVisibleTopics => Some(Coverage::VisibleUserTopicsOnly),
            IncompleteDiscovery::Refuse => None,
        },
    }
}

/// The fields a run's source resolution consists of, for the R4 comparison.
///
/// `ConnectionPlan`, the use-independent half (addresses, auth mode,
/// principal, CA path, password variable name), so resolving the same
/// `KafkaCluster` for `Discovery` and then `BackupSource` digests the same,
/// while a changed bootstrap list, principal, auth mode or cluster object does
/// not.
///
/// `status.clusterId` is deliberately left out: the probe controller writes it
/// asynchronously and clears it on every unreachable or unreadable probe, so it
/// flips on ordinary churn within a discovery's 300 s and would refuse a good
/// run as `SourceChangedDuringResolution`. D1 §7.2 R1 does not list it; the
/// repointed-endpoint case is [`source_unchanged`]'s explicit comparison, which
/// fires only when the broker-reported and observed ids are both present and
/// differ.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SourceFacts {
    cluster_uid: String,
    connection: ConnectionPlan,
}

/// `sha256:<hex>` over the source this run resolved — **pure**, D1 §7.2 R1.
///
/// # Errors
///
/// Never in practice; [`logweir_core::det_json::DetJsonError`] is named rather
/// than unwrapped for a shape that cannot produce one.
pub fn source_digest(
    resolved: &ResolvedConnection,
    cluster: &KafkaCluster,
) -> Result<String, logweir_core::det_json::DetJsonError> {
    let facts = SourceFacts {
        cluster_uid: cluster.uid().unwrap_or_default(),
        connection: connection_plan(resolved),
    };
    Ok(logweir_core::ids::sha256_prefixed(
        &logweir_core::det_json::to_deterministic_json(&facts)?,
    ))
}

/// The connection half of the check plan: the interactive discovery's own
/// [`crate::controllers::topic_discovery::connection_plan`], so a run's
/// discovery dials exactly what an interactive one dials.
#[must_use]
pub fn connection_plan(resolved: &ResolvedConnection) -> ConnectionPlan {
    super::topic_discovery::connection_plan(resolved)
}

/// The least a runner may be given to list a catalogue, in seconds: a refusal,
/// not a clamp. The Job's deadline is R2's `min(300, spec.deadlineSeconds)`
/// less [`cjob::DEADLINE_MARGIN_SECONDS`] for start-up, so a 60-second
/// `deadlineSeconds` leaves under a second to list; raising the deadline would
/// break R2, and dispatching would end `DiscoveryFailed` without naming the
/// deadline. The run is refused up front, naming the field and this number.
pub const MIN_DISCOVERY_BUDGET_SECONDS: i64 = 30;

/// The smallest `spec.deadlineSeconds` that can fund a discovery at all —
/// [`MIN_DISCOVERY_BUDGET_SECONDS`] plus the Job's start-up margin.
pub const MIN_DYNAMIC_DEADLINE_SECONDS: i64 =
    MIN_DISCOVERY_BUDGET_SECONDS + cjob::DEADLINE_MARGIN_SECONDS;

/// The runner's own budget for one discovery, in seconds: R2's Job deadline
/// `min(300, spec.deadlineSeconds)` less [`cjob::DEADLINE_MARGIN_SECONDS`]
/// (image pull, scheduling, start-up, which the runner cannot bound). `None`
/// below [`MIN_DISCOVERY_BUDGET_SECONDS`]; the caller refuses.
#[must_use]
pub fn discovery_budget(deadline_seconds: i64) -> Option<(i64, u32)> {
    let job_deadline = deadline_seconds.min(MAX_DISCOVERY_SECONDS);
    let plan = job_deadline - cjob::DEADLINE_MARGIN_SECONDS;
    if plan < MIN_DISCOVERY_BUDGET_SECONDS {
        return None;
    }
    // The check contract bounds `timeoutSeconds` at 1..=600; the ceiling above
    // already keeps this under 210, and the clamp says so rather than relying
    // on it.
    Some((job_deadline, u32::try_from(plan.min(600)).unwrap_or(600)))
}

/// The `topicInventory` plan one run's discovery runs.
///
/// `include_internal: true`, unlike an interactive discovery: the frozen block
/// records internal topics by name (`internalExcluded.names`, D1 §3.3), and
/// [`classify`] applies the exclusion itself. `expected_topics` is empty: a
/// dynamic selection names nothing.
#[must_use]
pub fn plan_document(resolved: &ResolvedConnection, subject_uid: &str, timeout: u32) -> CheckPlan {
    CheckPlan {
        contract: CHECK_PLAN_CONTRACT.to_string(),
        contract_version: CHECK_CONTRACT_VERSION,
        subject_uid: subject_uid.to_string(),
        timeout_seconds: timeout,
        // No policy digest: this plan's limits are this module's constants, so
        // there is no policy fact for the document to carry.
        policy_digest: None,
        request: CheckRequest::TopicInventory(TopicInventoryRequest {
            connection: connection_plan(resolved),
            include_internal: true,
            expected_topics: Vec::new(),
            max_topics: DISCOVERY_MAX_TOPICS,
            relay_budget_bytes: DEFAULT_RELAY_BUDGET_BYTES as u64,
        }),
    }
}

/// The owner reference every object this module creates carries, built from
/// `kube::Resource`'s own `api_version`/`kind`: a hand-written `apiVersion` that
/// drifts from the CRD leaves the cascade silently undone.
#[must_use]
pub fn owner_of(backup: &Backup, uid: &str) -> RunnerOwner {
    RunnerOwner {
        api_version: Backup::api_version(&()).to_string(),
        kind: Backup::kind(&()).to_string(),
        name: backup.name_any(),
        uid: uid.to_string(),
        workspace: crate::workspace::label_of(backup).map(str::to_string),
    }
}

/// The labels on the discovery Job and on its pod template; the component is
/// `run-discovery`, not `check` (see [`COMPONENT_RUN_DISCOVERY`]).
#[must_use]
pub fn labels(
    backup_uid: &str,
    connection_uid: Option<&str>,
) -> std::collections::BTreeMap<String, String> {
    let mut out = std::collections::BTreeMap::from([
        (
            cjob::LABEL_MANAGED_BY.to_string(),
            cjob::MANAGED_BY.to_string(),
        ),
        (
            cjob::LABEL_COMPONENT.to_string(),
            COMPONENT_RUN_DISCOVERY.to_string(),
        ),
        (
            LABEL_PURPOSE.to_string(),
            PURPOSE_TOPIC_DISCOVERY.to_string(),
        ),
        (
            cjob::LABEL_CHECK_KIND.to_string(),
            CheckPlanKind::TopicInventory.as_str().to_string(),
        ),
        (
            cjob::LABEL_CHECK_OWNER_UID.to_string(),
            backup_uid.to_string(),
        ),
    ]);
    if let Some(uid) = connection_uid.filter(|u| !u.trim().is_empty()) {
        out.insert(
            cjob::LABEL_CHECK_CONNECTION_UID.to_string(),
            uid.to_string(),
        );
    }
    out
}

/// The discovery Job, built from the shared check-Job shape.
///
/// [`cjob::runner_job_spec`] decides what a check pod is: the argv, the
/// `/check` mount, the pinned environment, the runner ServiceAccount with no
/// token, no signing key, no archive and no `/plan`. This module adds the D1
/// §7.2 parts: the name (`lwd-<backup uid>`), the deadline
/// (`min(300, spec.deadlineSeconds)`) and the labels plus the R4 annotation.
#[must_use]
pub fn build_discovery_job(
    spec: &cjob::CheckJobSpec,
    job_name: &str,
    job_deadline: i64,
    source_sha256: &str,
) -> Job {
    let mut runner = cjob::runner_job_spec(spec);
    runner.name = job_name.to_string();
    let mut job = crate::job::build(&runner);
    let labels = labels(&spec.owner.uid, spec.connection_uid.as_deref());
    job.metadata
        .labels
        .get_or_insert_with(Default::default)
        .extend(labels.clone());
    job.metadata.annotations = Some(std::collections::BTreeMap::from([(
        SOURCE_SHA256_ANNOTATION.to_string(),
        source_sha256.to_string(),
    )]));
    if let Some(job_spec) = job.spec.as_mut() {
        // R2 exactly: the framework adds its margin to a check's own budget,
        // but here the ceiling is the run's, and `discovery_budget` already
        // took the margin out of the plan's side.
        job_spec.active_deadline_seconds = Some(job_deadline);
        let mut meta = job_spec.template.metadata.take().unwrap_or_default();
        meta.labels = Some(labels);
        job_spec.template.metadata = Some(meta);
    }
    job
}

/// Everything the freeze needs about one finished discovery.
#[derive(Clone, Debug)]
pub struct Observed {
    /// The relayed inventory document, already held to its frames.
    pub inventory: InventoryResult,
    /// The classification of the relayed entries.
    pub classification: Classification,
    /// The completeness verdict, from `check_contract::visibility`.
    pub visibility: VisibilityState,
    /// When the runner observed the cluster.
    pub observed_at: DateTime<Utc>,
    /// The discovery Job the result was read from.
    pub job_name: String,
}

/// Turn one observation plus the run's policy into the value the freeze takes —
/// **pure**, D1 §7.2 R5–R9.
///
/// # Errors
///
/// The terminal state D1 §7.2 names, as `(state, message)`:
/// `DiscoveryResultUnreadable` (R3, a name no Kafka broker would accept),
/// `DiscoveryIncomplete` (R6, `Refuse`), `SelectionEmpty` (R7),
/// `SelectionTooLarge` (R8, and for a truncated listing).
pub fn resolved_selection(
    observed: &Observed,
    exclusions: &Exclusions,
    policy: IncompleteDiscovery,
    backup_name: &str,
) -> Result<ResolvedSelection, (&'static str, String)> {
    // R8, FIRST HALF: a listing the runner had to cut is not a listing this run
    // may select from. Every later count would be a count of a prefix.
    if observed.inventory.truncated {
        return Err((
            TERMINAL_STATE_SELECTION_TOO_LARGE,
            format!(
                "the discovery for {backup_name} was truncated after {} of the cluster's topics \
                 ({}), so the names it returned are a PREFIX of what the principal can see; a \
                 dynamic run may not freeze a prefix and call it every user topic. Name the \
                 topics explicitly, or split the cluster across schedules",
                observed.inventory.counts.returned,
                observed
                    .inventory
                    .truncation_reason
                    .map_or("unspecified", |r| match r {
                        logweir_core::check_contract::TruncationReason::MaxTopics =>
                            "the plan's maxTopics",
                        logweir_core::check_contract::TruncationReason::RelayLimit =>
                            "the relay budget",
                    })
            ),
        ));
    }

    // R3: a name no broker would accept is not adopted. These came off a
    // runner's stdout, not a pattern-checked CRD field, and the glob rail does
    // not cover a space, a slash, a control character or a 250-character name.
    // `DiscoveryResultUnreadable`, not `InvalidTopicSelection`: the operator's
    // selection is fine, the runner's output did not verify.
    if let Err(entry) =
        logweir_core::guard::reject_non_kafka_topic_names(&observed.classification.resolved)
    {
        return Err((
            TERMINAL_STATE_DISCOVERY_RESULT_UNREADABLE,
            format!(
                "the discovery for {backup_name} relayed `{}`, which is not a name a Kafka \
                 broker accepts (`^[a-zA-Z0-9._-]{{1,249}}$`); a listing carrying a name the \
                 cluster could not hold is not a listing this run selects from",
                crate::backup_execution::shown(&redact(&entry))
            ),
        ));
    }

    // R6: the coverage label, DERIVED from the visibility verdict (S3).
    let Some(coverage) = coverage_for(observed.visibility, policy) else {
        return Err((
            TERMINAL_STATE_DISCOVERY_INCOMPLETE,
            format!(
                "the discovery for {backup_name} could not establish that it saw every user \
                 topic (visibility `{}`, {} topic(s) the broker refused to describe) and \
                 spec.allUserTopics.incompleteDiscovery is `Refuse`. Kafka omits topics a \
                 principal cannot describe, so a successful listing alone is never proof; grant \
                 the principal Describe on the cluster, record an administrator attestation, or \
                 choose `BackUpVisibleTopics` and accept the `VisibleUserTopicsOnly` label",
                observed.visibility.as_str(),
                observed.classification.limited.len()
            ),
        ));
    };

    let topics = observed.classification.resolved.clone();

    // R7: no runner Job is created, and it is not retried.
    if topics.is_empty() {
        return Err((
            TERMINAL_STATE_SELECTION_EMPTY,
            format!(
                "the discovery for {backup_name} resolved no topics at all: {} visible, {} \
                 internal, {} removed by spec.allUserTopics.exclude, {} the broker refused to \
                 describe. A mandatory allowlist whose absence means `all topics` is not an \
                 allowlist (guard G-GLOB), so no runner Job is created and this run is not \
                 retried",
                observed.classification.visible,
                observed.classification.internal.len(),
                observed.classification.excluded.len(),
                observed.classification.limited.len()
            ),
        ));
    }

    // R8, SECOND HALF: the two bounds on what one plan `ConfigMap` may carry.
    let bytes: usize = topics.iter().map(String::len).sum();
    if topics.len() > MAX_RESOLVED_TOPICS || bytes > MAX_RESOLVED_TOPIC_BYTES {
        return Err((
            TERMINAL_STATE_SELECTION_TOO_LARGE,
            format!(
                "{}: the discovery for {backup_name} resolved {} topics taking {bytes} bytes \
                 of names, over the {MAX_RESOLVED_TOPICS} topics / {MAX_RESOLVED_TOPIC_BYTES} \
                 bytes one backup may name. A backup signs a receipt of up to {} bytes a topic, \
                 and one over {} bytes could not be listed by the recovery catalog or verified \
                 by the controller; no topic is left out to make the selection fit. Exclude \
                 more, or split the cluster across schedules",
                logweir_core::topic_budget::SELECTION_TOO_LARGE,
                topics.len(),
                logweir_core::topic_budget::RECEIPT_TOPIC_BUDGET_BYTES,
                logweir_core::topic_budget::MAX_RECEIPT_BYTES
            ),
        ));
    }

    // ONE SHARED BYTE BUDGET, SPENT IN A FIXED ORDER, so two runs that observed
    // the same cluster freeze the same bytes. See `MAX_PROVENANCE_NAME_BYTES`.
    let mut name_budget = MAX_PROVENANCE_NAME_BYTES;
    let internal_names = bounded(
        &observed.classification.internal,
        MAX_INTERNAL_NAMES,
        &mut name_budget,
    );
    let excluded_names = bounded(
        &observed.classification.excluded,
        MAX_EXCLUDED_NAMES,
        &mut name_budget,
    );

    let discovery = DiscoveryInputs {
        observed_at: observed
            .observed_at
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        cluster_id: observed.inventory.cluster_id.clone().unwrap_or_default(),
        visibility: observed.visibility.as_str().to_string(),
        basis: DISCOVERY_BASIS.to_string(),
        // The controller's digest over the frames, which the runner's document
        // was already held to. It names the observation this selection came
        // from; the frozen names are a subset and do not reproduce it.
        result_sha256: observed.inventory.topics_sha256.clone(),
        visible_topic_count: i64::try_from(observed.classification.visible).unwrap_or(i64::MAX),
        internal_excluded: internal_names,
        excluded_by_rule: excluded_names,
        limited_topic_count: i64::try_from(observed.classification.limited.len())
            .unwrap_or(i64::MAX),
        discovery_job: observed.job_name.clone(),
    };

    Ok(ResolvedSelection {
        selection: SelectionInputs {
            mode: SelectionMode::AllUserTopics,
            coverage,
            resolved_topic_count: i64::try_from(topics.len()).unwrap_or(i64::MAX),
            resolved_topic_bytes: i64::try_from(bytes).unwrap_or(i64::MAX),
            exclude: exclusions.inputs(),
            incomplete_discovery: Some(policy),
            discovery: Some(discovery),
        },
        topics,
    })
}

/// What [`resolve`] decided this pass.
#[derive(Debug)]
pub enum Resolution {
    /// The discovery Job exists and has not produced a readable result yet.
    /// The caller ends the pass; the `Backup` reconciler's own 15 s requeue is
    /// D1 §7.2 R2's.
    Pending,
    /// The topic list, ready to freeze exactly as a named allowlist is.
    Resolved(Box<ResolvedSelection>),
    /// Terminal. **The status is already on the object**; the caller returns
    /// without writing a second one. See the module header.
    Refused {
        /// One of [`crate::conditions::TERMINAL_STATES`].
        state: &'static str,
    },
}

/// What [`resolve`] produced before the refusal wrapper.
enum Inner {
    Pending,
    Resolved(Box<ResolvedSelection>),
}

/// The `TopicsResolved` condition, merged against what is on the object.
fn topics_resolved(
    backup: &Backup,
    status: &str,
    reason: &str,
    message: &str,
    now: DateTime<Utc>,
) -> Value {
    json!(merge_condition(
        current_condition(
            backup.status.as_ref().and_then(|s| s.conditions.as_ref()),
            CONDITION_TOPICS_RESOLVED,
        ),
        Condition {
            r#type: CONDITION_TOPICS_RESOLVED.to_string(),
            status: status.to_string(),
            observed_generation: backup.meta().generation,
            last_transition_time: Some(now),
            reason: Some(reason.to_string()),
            // REDACTED: some of these messages are derived from a runner's own
            // output, and a condition is read by every surface.
            message: Some(redact(message)),
        },
    ))
}

/// The `/status` merge patch for a run whose discovery is running — D1 §7.2 R2.
///
/// `phase: Resolving` is nonterminal and `backup_is_terminal` already treats an
/// unknown phase as active, so an older reader is not confused by it.
#[must_use]
pub fn resolving_status_patch(backup: &Backup, job_name: &str, now: DateTime<Utc>) -> Value {
    let conditions = carry_conditions(
        backup.status.as_ref().and_then(|s| s.conditions.as_ref()),
        vec![topics_resolved(
            backup,
            "False",
            REASON_DISCOVERY_RUNNING,
            &format!(
                "the topic discovery Job {job_name} is resolving spec.allUserTopics for this run; \
                 no runner Job is created until the exact names are frozen"
            ),
            now,
        )],
    );
    json!({ "status": {
        "phase": crate::conditions::PHASE_RESOLVING,
        "conditions": conditions,
    }})
}

/// The `/status` merge patch that records a resolved selection — D1 §7.2 R9.
///
/// Sent AFTER the freeze wrote `status.execution` and `status.selection`, so
/// `TopicsResolved=True` is never on an object whose names are not yet frozen.
#[must_use]
pub fn resolved_status_patch(backup: &Backup, now: DateTime<Utc>) -> Value {
    let count = backup
        .status
        .as_ref()
        .and_then(|s| s.selection.as_ref())
        .map_or(0, |s| s.resolved_topic_count);
    let conditions = carry_conditions(
        backup.status.as_ref().and_then(|s| s.conditions.as_ref()),
        vec![topics_resolved(
            backup,
            "True",
            REASON_RESOLVED,
            &format!(
                "spec.allUserTopics resolved to {count} topic(s), frozen in this run's immutable \
                 execution inputs; the coverage this run may claim is status.selection.coverage"
            ),
            now,
        )],
    );
    json!({ "status": { "conditions": conditions } })
}

/// Patch `/status` with the S7 precondition, skipping a patch that changes
/// nothing.
///
/// Returns `false` when the API server answered 409: this pass's conclusion is
/// not on the server, which the TTL ordering depends on. The second half is
/// where the object now stands ([`StatusVersion`]), for a later write in the
/// same pass to precondition on; after a 409 it is unknowable, so
/// [`StatusVersion::default`].
async fn write_status(
    api: &Api<Backup>,
    backup: &Backup,
    patch: Value,
) -> Result<(bool, StatusVersion), BackupError> {
    let name = backup.name_any();
    if status_unchanged(
        backup
            .status
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok())
            .as_ref(),
        &patch,
    ) {
        debug!(
            backup = %name,
            "the computed status equals the one on the object; no patch is sent"
        );
        return Ok((true, StatusVersion::observed(&backup.metadata)));
    }
    let Some(resource_version) = backup
        .metadata
        .resource_version
        .clone()
        .filter(|v| !v.is_empty())
    else {
        // Unreachable for an object that came from a watch or a `get`; named
        // rather than silently written without its precondition.
        return Err(BackupError::Refused(
            crate::conditions::TERMINAL_STATE_EXECUTION_SPEC_INVALID,
            format!(
                "Backup {name} carries no metadata.resourceVersion, which a /status \
                 compare-and-set needs (D-SEAMS S7)"
            ),
        ));
    };
    let mut body = patch;
    body.as_object_mut()
        .expect("a status patch is always a JSON object")
        .insert(
            "metadata".to_string(),
            json!({ "name": name, "resourceVersion": resource_version }),
        );
    match api
        .patch_status(&name, &PatchParams::default(), &Patch::Merge(body))
        .await
    {
        Ok(applied) => Ok((true, StatusVersion::observed(applied.meta()))),
        Err(kube::Error::Api(e)) if e.code == 409 => {
            debug!(
                backup = %name,
                "the status changed under this reconcile (409); the next pass reads it"
            );
            Ok((false, StatusVersion::default()))
        }
        Err(e) => Err(BackupError::Api(e)),
    }
}

/// Resolve `spec.allUserTopics` for one run: the entry point
/// `controllers::backup` calls (D1 §7.2).
///
/// Called only from the freeze branch, before `status.execution` is recorded:
/// a frozen run reads its selection back from its own plan
/// ([`crate::backup_execution::stored_selection`]) and never discovers again
/// (D1 §12's `a_frozen_dynamic_backup_never_reruns_discovery`).
///
/// `runner` is the process's runner image and pull policy, the value the run's
/// own Job takes; `None` means the compiled pin ([`crate::job::RUNNER_IMAGE`]
/// under [`crate::job::IMAGE_PULL_POLICY`]). Without it the discovery Job would
/// fail `ErrImageNeverPull` on an installation that configured another image.
///
/// # Errors
///
/// [`BackupError::Api`] for a transport failure, which requeues. A refusal is
/// not an error: it is [`Resolution::Refused`] with the terminal status already
/// written.
pub async fn resolve(
    backup: &Backup,
    client: &kube::Client,
    namespace: &str,
    now: DateTime<Utc>,
    runner: &RunnerImage,
) -> Result<Resolution, BackupError> {
    // A finished discovery Job this pass observed, for the TTL on the refusal
    // path; `None` before the Job exists or finishes, where a TTL patch would
    // 404 or arm the collector against a pod still holding a relay.
    let mut finished_job: Option<String> = None;
    match resolve_inner(backup, client, namespace, now, runner, &mut finished_job).await {
        Ok(Inner::Pending) => Ok(Resolution::Pending),
        Ok(Inner::Resolved(selection)) => Ok(Resolution::Resolved(selection)),
        Err(BackupError::Refused(state, message)) => {
            let name = backup.name_any();
            warn!(
                backup = %name,
                namespace = %namespace,
                terminal_state = state,
                reason = %message,
                "refusing this dynamic selection terminally: no runner Job runs for it, and a \
                 retry is a NEW Backup, which rediscovers"
            );
            // ONE PATCH, BOTH FACTS. See the module header for why the refusal
            // is written here rather than raised: `Failed` and `TopicsResolved`
            // have to be in the same array, and a merge patch replaces arrays.
            let api: Api<Backup> = Api::namespaced(client.clone(), namespace);
            let carried = json!({ "status": { "conditions": carry_conditions(
                backup.status.as_ref().and_then(|s| s.conditions.as_ref()),
                vec![topics_resolved(backup, "False", state, &message, now)],
            )}});
            let view = crate::controllers::backup::with_status_patch(backup, &carried);
            let patch = refused_status_patch(&view, state, &message, now);
            let (committed, _) = write_status(&api, backup, patch).await?;
            // The ordering the resolved path uses: the Job may be collected
            // only once the terminal status is on the server; after a 409 the
            // next pass must read the relay again.
            if committed {
                if let Some(job_name) = finished_job.as_deref() {
                    check::set_ttl(client, namespace, job_name)
                        .await
                        .map_err(BackupError::Api)?;
                }
            }
            Ok(Resolution::Refused { state })
        }
        Err(other) => Err(other),
    }
}

/// [`resolve`]'s body: every refusal is a `?` on [`BackupError::Refused`], so
/// the one status write above cannot be forgotten at one of them.
async fn resolve_inner(
    backup: &Backup,
    client: &kube::Client,
    namespace: &str,
    now: DateTime<Utc>,
    runner: &RunnerImage,
    finished_job: &mut Option<String>,
) -> Result<Inner, BackupError> {
    let name = backup.name_any();
    let uid = backup.uid().filter(|u| !u.is_empty()).ok_or_else(|| {
        BackupError::Refused(
            crate::conditions::TERMINAL_STATE_EXECUTION_SPEC_INVALID,
            format!(
                "Backup {name} carries no metadata.uid, so the discovery Job this run needs \
                     cannot be named after it"
            ),
        )
    })?;
    let policy = backup.spec.all_user_topics.clone().ok_or_else(|| {
        BackupError::Refused(
            TERMINAL_STATE_INVALID_TOPIC_SELECTION,
            format!("Backup {name} declares no spec.allUserTopics to resolve"),
        )
    })?;
    let job_name = discovery_job_name(&uid);

    // R1: the one resolution, and the digest R4 compares against.
    let cluster = source_cluster(backup, client, namespace).await?;
    let resolved = connection::resolve(&cluster, ConnectionUse::Discovery)
        .map_err(|r| BackupError::Refused(r.reason, r.message))?;
    // One `namespace` feeds the `KafkaCluster` GET and the Job; this makes it a
    // checked property rather than a coincidence of two lookups.
    resolved
        .check_job_namespace(namespace)
        .map_err(|r| BackupError::Refused(r.reason, r.message))?;
    let source_sha256 = source_digest(&resolved, &cluster).map_err(|e| {
        BackupError::Refused(
            crate::conditions::TERMINAL_STATE_CONNECTION_CONFIG_INVALID,
            format!("the source resolution for {name} could not be digested: {e}"),
        )
    })?;

    let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
    let existing = jobs.get_opt(&job_name).await.map_err(BackupError::Api)?;

    let Some(job) = existing else {
        return start(
            backup,
            client,
            namespace,
            &uid,
            &job_name,
            &resolved,
            &source_sha256,
            now,
            runner,
        )
        .await;
    };

    // A Job this Backup does not control is never observed: its name derives
    // from this object's UID, so a foreign Job there is a forgery, and its
    // stdout would otherwise become this run's allowlist.
    if !controlled_by(&job, &uid) {
        return Err(BackupError::Refused(
            TERMINAL_STATE_JOB_NAME_CONFLICT,
            format!(
                "Job {namespace}/{job_name} already exists but is not controlled by exactly \
                 Backup {namespace}/{name} with UID {uid}; its output was neither read nor \
                 adopted. Remove the foreign Job and create a new Backup"
            ),
        ));
    }

    let api: Api<Backup> = Api::namespaced(client.clone(), namespace);
    if !job_finished(&job) {
        // A discovery pod the namespace refused ends the run now. No request
        // while the Job's status counts a pod or it is within the grace;
        // otherwise one pod list and, finding none, one events list by the
        // Job's UID: the same `FailedCreate` row `check::classify` reads later.
        if let Some(refusal) = check::refused_pod_creation(client, namespace, &job, now)
            .await
            .map_err(BackupError::Api)?
        {
            check::cancel(client, namespace, &job, &uid)
                .await
                .map_err(BackupError::Api)?;
            return Err(discovery_refused(
                &job_name,
                &name,
                refusal.code,
                &refusal.message,
            ));
        }
        write_status(&api, backup, resolving_status_patch(backup, &job_name, now)).await?;
        return Ok(Inner::Pending);
    }

    // FROM HERE ON A REFUSAL MAY ARM THE JOB'S TTL: the Job has finished, so
    // the relay is complete and the only reason to keep the pod is a status
    // that has not landed yet.
    *finished_job = Some(job_name.clone());

    observe(
        backup,
        client,
        namespace,
        &uid,
        &job_name,
        &job,
        &cluster,
        &resolved,
        &source_sha256,
        &policy,
        now,
    )
    .await
}

/// The `KafkaCluster` `spec.sourceRef` names.
///
/// The same read and the same refusal `controllers::backup`'s own
/// `plan_source_cluster` makes, so a dynamic run and a named one refuse a
/// missing referent identically.
async fn source_cluster(
    backup: &Backup,
    client: &kube::Client,
    namespace: &str,
) -> Result<KafkaCluster, BackupError> {
    let referent = backup.spec.source_ref.name.clone();
    let clusters: Api<KafkaCluster> = Api::namespaced(client.clone(), namespace);
    clusters
        .get_opt(&referent)
        .await
        .map_err(BackupError::Api)?
        .ok_or_else(|| {
            BackupError::Refused(
                TERMINAL_STATE_REFERENT_NOT_FOUND,
                format!(
                    "spec.sourceRef names the KafkaCluster `{referent}`, which does not exist in \
                     namespace {namespace}; there is nothing to discover topics from"
                ),
            )
        })
}

/// Whether `job`'s CONTROLLER owner reference is this `Backup`.
#[must_use]
fn controlled_by(job: &Job, backup_uid: &str) -> bool {
    job.meta()
        .owner_references
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|o| o.uid == backup_uid && o.controller == Some(true))
}

/// D1 §7.2 R2: render the plan, create the Job, record `Resolving`.
#[allow(clippy::too_many_arguments)]
async fn start(
    backup: &Backup,
    client: &kube::Client,
    namespace: &str,
    uid: &str,
    job_name: &str,
    resolved: &ResolvedConnection,
    source_sha256: &str,
    now: DateTime<Utc>,
    runner: &RunnerImage,
) -> Result<Inner, BackupError> {
    let name = backup.name_any();
    // Refuse up front rather than dispatch a Job that cannot finish (R2): see
    // `MIN_DISCOVERY_BUDGET_SECONDS`.
    let Some((job_deadline, plan_timeout)) = discovery_budget(backup.spec.deadline_seconds) else {
        return Err(BackupError::Refused(
            crate::conditions::TERMINAL_STATE_EXECUTION_SPEC_INVALID,
            format!(
                "spec.deadlineSeconds is {} on {name}, which leaves {} second(s) for the topic \
                 discovery this run needs: the discovery Job's deadline is \
                 min({MAX_DISCOVERY_SECONDS}, spec.deadlineSeconds) and {} of that is image \
                 pull, scheduling and container start. A dynamic selection needs at least \
                 {MIN_DYNAMIC_DEADLINE_SECONDS} seconds ({MIN_DISCOVERY_BUDGET_SECONDS} for the \
                 runner). Raise spec.deadlineSeconds, or name the topics explicitly",
                backup.spec.deadline_seconds,
                (backup.spec.deadline_seconds.min(MAX_DISCOVERY_SECONDS)
                    - cjob::DEADLINE_MARGIN_SECONDS)
                    .max(0),
                cjob::DEADLINE_MARGIN_SECONDS,
            ),
        ));
    };
    let owner = owner_of(backup, uid);
    let document = plan_document(resolved, uid, plan_timeout);
    let bytes = logweir_core::det_json::to_deterministic_json(&document).map_err(|e| {
        BackupError::Refused(
            TERMINAL_STATE_DISCOVERY_FAILED,
            format!("the discovery plan for {name} could not be serialised: {e}"),
        )
    })?;
    let documents = plan::PlanDocuments {
        check_plan: bytes,
        // No `source-ca.pem`: see `topic_discovery::connection_plan`.
        ..plan::PlanDocuments::default()
    };
    let digest = documents.check_plan_sha256();
    let config_map = plan::build(job_name, namespace, &owner, &documents).map_err(|e| {
        BackupError::Refused(
            TERMINAL_STATE_DISCOVERY_FAILED,
            format!("the discovery plan ConfigMap for {name} could not be rendered: {e}"),
        )
    })?;

    // THE PLAN FIRST. A Job whose plan `ConfigMap` does not exist is a pod that
    // stalls in `ContainerCreating` until its deadline and reports nothing.
    match plan::ensure(client, namespace, &config_map, uid, &digest).await {
        Ok(_) => {}
        Err(plan::EnsureError::Api(e)) => return Err(BackupError::Api(e)),
        Err(plan::EnsureError::Plan(e)) => {
            return Err(BackupError::Refused(
                TERMINAL_STATE_DISCOVERY_FAILED,
                format!("{e}"),
            ))
        }
    }

    let projection = resolved.project();
    let spec = cjob::CheckJobSpec {
        kind: CheckPlanKind::TopicInventory,
        namespace: namespace.to_string(),
        owner,
        connection_uid: resolved.uid.clone(),
        plan_config_map: plan::plan_config_map_name(job_name),
        plan_sha256: digest,
        subject_uid: uid.to_string(),
        timeout_seconds: i64::from(plan_timeout),
        service_account_name: ConnectionUse::Discovery.service_account_name().to_string(),
        secret_mounts: projection.secret_mounts,
        config_map_mounts: projection.config_map_mounts,
        env_from_secret: projection.env_from_secret,
        env_literal: projection.env_literal,
        // The process's image and pull policy, as every controller-built Job
        // takes them; `None` leaves the compiled-in constant.
        image: runner.image.clone(),
        image_pull_policy: runner.image_pull_policy.clone(),
        attempt: None,
    };
    // FX-43: the discovery Job's relay is read only through its token.
    let desired = crate::job::tokened(build_discovery_job(
        &spec,
        job_name,
        job_deadline,
        source_sha256,
    ));
    let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
    match jobs.create(&PostParams::default(), &desired).await {
        Ok(_) => {}
        // A 409 IS THE DUPLICATE-RECONCILE CASE AND IT IS HEALTHY: the name is
        // a pure function of this object's UID, so a 409 means "the discovery
        // this pass wanted already exists". The next pass observes it.
        Err(kube::Error::Api(e)) if e.code == 409 => {
            debug!(
                backup = %name,
                job = %job_name,
                "the discovery Job already exists; this pass adopts it"
            );
        }
        Err(e) => return Err(BackupError::Api(e)),
    }

    info!(
        backup = %name,
        namespace = %namespace,
        job = %job_name,
        principal = %resolved.principal,
        deadline_seconds = job_deadline,
        "created the per-run topic discovery Job; this controller never dials a broker itself \
         and never reads a Secret, which is why a discovery is a Job"
    );

    let api: Api<Backup> = Api::namespaced(client.clone(), namespace);
    write_status(&api, backup, resolving_status_patch(backup, job_name, now)).await?;
    Ok(Inner::Pending)
}

/// D1 §7.2 R2 (second half) through R9: prove the pod, read the relay, refuse
/// or resolve.
#[allow(clippy::too_many_arguments)]
async fn observe(
    backup: &Backup,
    client: &kube::Client,
    namespace: &str,
    uid: &str,
    job_name: &str,
    job: &Job,
    cluster: &KafkaCluster,
    resolved: &ResolvedConnection,
    source_sha256: &str,
    policy: &AllUserTopics,
    now: DateTime<Utc>,
) -> Result<Inner, BackupError> {
    let name = backup.name_any();

    // Seam S6: the `job-name` label narrows the list and the controller
    // `ownerReference` UID decides; a pod owned by something else is ignored and
    // logged, or its stdout would become this run's allowlist.
    let pod = check::pod::find_owned_pod(client, namespace, job)
        .await
        .map_err(BackupError::Api)?;
    let log = match pod.as_ref() {
        Some(p) => {
            let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
            // The full log through the framework's bounded parameters, not the
            // short tail the exit-code path reads; a body over the relay budget
            // is refused rather than parsed.
            match runner_log::read_text(&pods, &p.name_any(), &runner_log::RELAY).await {
                Ok(log) => Some(log),
                Err(e) if check::is_log_absent(&e) => None,
                Err(e) => return Err(BackupError::Api(e)),
            }
        }
        None => None,
    };

    let expect = expectations_for(client, namespace, job_name, uid).await?;
    // The Events that explain a pod that never started, read only when the
    // classifier could use one, so a refused pod ends `PodCreationForbidden`
    // with the admission's words 30 seconds after the Job.
    let events = check::job_events(client, namespace, job, pod.as_ref(), now).await;
    let observation = check::classify(&check::Input {
        job,
        pod: pod.as_ref(),
        events: &events,
        log: log.as_deref(),
        expect: &expect,
        now,
    });

    match observation.phase {
        // A finished Job the classifier still calls `Running` is a Job whose
        // pod has not reported; the next pass reads it.
        check::CheckPhase::Running => {
            let api: Api<Backup> = Api::namespaced(client.clone(), namespace);
            write_status(&api, backup, resolving_status_patch(backup, job_name, now)).await?;
            return Ok(Inner::Pending);
        }
        // NO CANCEL HERE: this function only ever sees a FINISHED Job (the
        // unfinished one is failed fast, and cancelled, in `resolve_inner`), and
        // a finished Job is not cancellable (`check::is_cancellable`).
        check::CheckPhase::Failed => {
            return Err(discovery_refused(
                job_name,
                &name,
                observation.reason,
                &observation.message,
            ));
        }
        check::CheckPhase::Succeeded => {}
    }

    let relay = observation
        .relay
        .as_ref()
        .expect("check::classify returns a relay with every Succeeded phase");
    // The whole document is bound, not just `inventory`: `result.checks` is
    // where a runner that could not reach the broker puts its answer.
    let mut document = match relay.result() {
        Some(Ok(result)) => Some(result),
        Some(Err(e)) => {
            return Err(BackupError::Refused(
                TERMINAL_STATE_DISCOVERY_RESULT_UNREADABLE,
                format!(
                    "the topic discovery Job {job_name} for {name} relayed a result document that \
                     did not verify: {}",
                    redact(&e.to_string())
                ),
            ))
        }
        None => None,
    };
    let inventory = document.as_mut().and_then(|d| d.inventory.take());
    let Some(inventory) = inventory else {
        // No inventory is not the same fact as no result, and neither means the
        // runner is broken: an unreachable broker relays a document with no
        // `inventory` and one classified `connection.authenticated` row (see
        // `check::kinds::inventory`), and this reads it.
        let (code, message) = no_inventory_refusal(document.as_ref().map(|d| d.checks.as_slice()));
        return Err(BackupError::Refused(
            // The D1 vocabulary, not the raw code: mapped through the same
            // `discovery_failure_state` as the `Failed`-phase branch, and named
            // in the message. An unreachable broker or a rotated password is
            // `DiscoveryFailed` (a new `Backup` is the remedy), not
            // `DiscoveryResultUnreadable` (fix the runner).
            discovery_failure_state(code),
            format!(
                "the topic discovery Job {job_name} for {name} could not list this cluster's \
                 topics ({code}): {message}"
            ),
        ));
    };
    // R3: the document is held to the frames it travelled with — the count and
    // the digest the decoder already measured. Where the two disagree the
    // DOCUMENT is what is wrong, and this run selects from neither.
    super::topic_discovery::check_result_against_frames(&inventory, &relay.topics).map_err(
        |refusal| {
            BackupError::Refused(
                TERMINAL_STATE_DISCOVERY_RESULT_UNREADABLE,
                format!(
                    "the topic discovery Job {job_name} for {name} is not adopted: {}",
                    redact(&refusal.to_string())
                ),
            )
        },
    )?;

    // R4. THE SOURCE MUST NOT HAVE MOVED UNDER THE RESOLUTION.
    source_unchanged(&inventory, cluster, job, source_sha256, &name, job_name)?;

    // R5.
    let exclusions = Exclusions::from_policy(policy);
    let classification = classify(&relay.topics, &exclusions);

    // R6.
    let verdict = run_time_visibility(
        client,
        &inventory,
        namespace,
        &resolved.cluster_name,
        &resolved.principal,
        now,
    )
    .await
    .map_err(BackupError::Api)?;

    let observed = Observed {
        observed_at: recorded_finish(job, pod.as_ref()).ok_or_else(|| {
            BackupError::Refused(
                TERMINAL_STATE_DISCOVERY_RESULT_UNREADABLE,
                format!(
                    "the topic discovery Job {job_name} for {name} reports no finish instant — no \
                     terminated `runner` container, no completionTime and no terminal condition \
                     timestamp — so there is no observedAt to freeze. A discovery whose \
                     observation cannot be dated is not one this run may record the provenance of"
                ),
            )
        })?,
        inventory,
        classification,
        visibility: verdict.state,
        job_name: job_name.to_string(),
    };

    // R6 (refusal), R7, R8 and the frozen block.
    let selection = resolved_selection(&observed, &exclusions, policy.incomplete_discovery, &name)
        .map_err(|(state, message)| BackupError::Refused(state, message))?;

    info!(
        backup = %name,
        namespace = %namespace,
        job = %job_name,
        topics = selection.topics.len(),
        visibility = verdict.state.as_str(),
        coverage = ?selection.selection.coverage,
        "the per-run topic discovery resolved a selection; the names are about to be frozen"
    );
    Ok(Inner::Resolved(Box::new(selection)))
}

/// R6: a run-time discovery's visibility verdict, which is
/// `check_contract::visibility`'s alone (seam S3); the attestation that could
/// raise it to `attestedComplete` comes from this process's installation policy
/// `ConfigMap`, through the fail-closed predicate an interactive discovery
/// uses.
///
/// # Errors
///
/// [`kube::Error`] from the policy read.
pub async fn run_time_visibility(
    client: &kube::Client,
    inventory: &InventoryResult,
    namespace: &str,
    cluster_name: &str,
    principal: &str,
    now: DateTime<Utc>,
) -> Result<logweir_core::check_contract::Visibility, kube::Error> {
    let load = policy::load(
        client,
        super::topic_discovery::configured_policy_ref().as_ref(),
        &policy::PolicyCache::new(),
        now,
    )
    .await?;
    let signals =
        super::topic_discovery::visibility_signals(inventory, namespace, cluster_name, principal);
    let attestation = super::topic_discovery::attestation_candidate(
        &load.policy().discovery.visibility_attestations,
        namespace,
        cluster_name,
        principal,
        &signals.cluster_id,
    );
    Ok(visibility(&signals, attestation, now))
}

/// The refusal a discovery Job that did not produce a usable result ends its
/// `Backup` with: [`discovery_failure_state`]'s terminal state, and a message
/// naming the Job, the classifier's code and its (redacted) explanation; for a
/// refused pod, the admission's own words.
fn discovery_refused(job_name: &str, name: &str, code: CheckCode, message: &str) -> BackupError {
    BackupError::Refused(
        discovery_failure_state(code),
        format!(
            "the topic discovery Job {job_name} for {name} did not produce a usable result \
             ({code}): {}",
            redact(message)
        ),
    )
}

/// D1 §7.2 R3's split: which refusals a retry could survive.
///
/// `DiscoveryResultUnreadable` is the malformed-output class and is not
/// retryable: a runner that printed something unverifiable will again.
/// Everything else is operational, and a retry (a new `Backup`, a fresh
/// discovery) can succeed. A pod the namespace refused at creation is
/// `PodCreationForbidden`, the state a `Backup`'s runner reaches for the same
/// refusal; a new `Backup` can succeed once the namespace admits the pod, but a
/// schedule's `spec.retry` does not retry it (not in
/// [`crate::cadence::RETRYABLE_TERMINAL_STATES`]).
///
/// Both are terminal for this run: `Backup.spec` is CEL-immutable, so a requeue
/// could never resolve differently (D1 §7.7).
#[must_use]
pub fn discovery_failure_state(code: CheckCode) -> &'static str {
    match code {
        CheckCode::ResultUnreadable
        | CheckCode::CheckContractMismatch
        | CheckCode::RunnerContractUnsupported => TERMINAL_STATE_DISCOVERY_RESULT_UNREADABLE,
        // The runner kind's own word for a refused pod
        // (`diagnostics::Code::runner_ready_reason`); `DiscoveryFailed` would
        // send an operator to the broker.
        CheckCode::PodCreateRejected | CheckCode::RunnerServiceAccountMissing => {
            TERMINAL_STATE_POD_CREATION_FORBIDDEN
        }
        _ => TERMINAL_STATE_DISCOVERY_FAILED,
    }
}

/// What a relay that carried NO result document at all says — the one case the
/// word "unreadable" still fits for a `Backup`, because nothing was read.
const NO_RESULT_DOCUMENT: &str = "the check Job's relay carries no result document, so neither a \
     topic inventory nor a check outcome could be read; nothing is inferred from the exit code";

/// A result document with no inventory AND no blocking check that is not
/// `ready`. The document contradicts itself — everything it checked passed and
/// it measured nothing — so there is nothing to project and nothing to select
/// from.
const NO_INVENTORY_AND_NO_FAILURE: &str = "the check Job relayed a verified result that carries \
     no topic inventory and no blocking check that is not ready; a topicInventory check that \
     produced no inventory has nothing to select from";

/// The blocking check a no-inventory result is about.
///
/// [`logweir_core::check_contract::aggregate`]'s precedence: `notReady` first,
/// then `unknown` or `skipped`, the first in document order within a state. The
/// second arm matters: `MetadataTimeout` is an `unknown` code and one of the two
/// reasons D2 §14.4 S11 accepts for a timed-out discovery. Advisory rows are
/// excluded (a warning would name the wrong cause), and so are execution-only
/// rows, which describe a run that has not happened.
fn blocking_failure(checks: &[CheckOutcome]) -> Option<&CheckOutcome> {
    let blocking = || checks.iter().filter(|c| c.gating == Gating::Blocking);
    blocking()
        .find(|c| c.state == CheckState::NotReady)
        .or_else(|| {
            blocking().find(|c| matches!(c.state, CheckState::Unknown | CheckState::Skipped))
        })
}

/// The [`CheckCode`] and sentence for a `Succeeded` discovery whose relay
/// carried no topic inventory.
///
/// `None` means no result document was relayed; `Some(checks)` is the
/// document's own `checks` array, already redacted and capped. The code is from
/// a closed vocabulary, so no runner text reaches the terminal state; the caller
/// maps it through [`discovery_failure_state`]. The sentence carries the check
/// id, the runner's message and its remedy.
fn no_inventory_refusal(checks: Option<&[CheckOutcome]>) -> (CheckCode, String) {
    let Some(checks) = checks else {
        return (CheckCode::ResultUnreadable, NO_RESULT_DOCUMENT.to_string());
    };
    let Some(failed) = blocking_failure(checks) else {
        return (
            CheckCode::ResultUnreadable,
            NO_INVENTORY_AND_NO_FAILURE.to_string(),
        );
    };
    let mut message = failed.id.as_str().to_string();
    if !failed.message.is_empty() {
        message.push_str(": ");
        message.push_str(&failed.message);
    }
    if !failed.remedy.is_empty() {
        if !message.ends_with('.') {
            message.push('.');
        }
        message.push(' ');
        message.push_str(&failed.remedy);
    }
    (failed.code, message)
}

/// D1 §7.2 R4: the cluster the runner dialled is the cluster this run resolved.
///
/// Two comparisons, and neither can be left out:
///
/// * the broker-reported `clusterId` against the `KafkaCluster`'s own observed
///   id, WHEN it has one — a `KafkaCluster` whose endpoint was repointed at a
///   different cluster between the dispatch and the read would otherwise hand
///   this run somebody else's topic names;
/// * the digest of the source resolution, recomputed NOW, against the one the
///   dispatching pass annotated on the Job — a changed bootstrap list, auth
///   mode, principal or CA reference.
fn source_unchanged(
    inventory: &InventoryResult,
    cluster: &KafkaCluster,
    job: &Job,
    source_sha256: &str,
    name: &str,
    job_name: &str,
) -> Result<(), BackupError> {
    if let (Some(observed), Some(reported)) = (
        cluster
            .status
            .as_ref()
            .and_then(|s| s.cluster_id.as_deref())
            .filter(|id| !id.is_empty()),
        inventory.cluster_id.as_deref().filter(|id| !id.is_empty()),
    ) {
        if observed != reported {
            return Err(BackupError::Refused(
                TERMINAL_STATE_SOURCE_CHANGED_DURING_RESOLUTION,
                format!(
                    "the topic discovery for {name} listed cluster `{reported}` and the \
                     KafkaCluster this run names has observed cluster `{observed}`; the source \
                     moved under the resolution and the names are not adopted"
                ),
            ));
        }
    }
    let dispatched = job
        .meta()
        .annotations
        .as_ref()
        .and_then(|a| a.get(SOURCE_SHA256_ANNOTATION))
        .map(String::as_str);
    if dispatched != Some(source_sha256) {
        return Err(BackupError::Refused(
            TERMINAL_STATE_SOURCE_CHANGED_DURING_RESOLUTION,
            format!(
                "the topic discovery Job {job_name} was dispatched against source resolution {} \
                 and {name} resolves {source_sha256} now; the saved connection changed while the \
                 discovery ran and its result is not adopted",
                dispatched.unwrap_or("<none>")
            ),
        ));
    }
    Ok(())
}

/// The digest the relay must carry, read back off this run's own plan
/// `ConfigMap` after [`plan::accepts_existing`]'s three-part rule (owner UID,
/// digest annotation, `immutable: true`) has proved it belongs to this
/// `Backup`.
async fn expectations_for(
    client: &kube::Client,
    namespace: &str,
    job_name: &str,
    uid: &str,
) -> Result<FrameExpectations, BackupError> {
    let maps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    let plan_name = plan::plan_config_map_name(job_name);
    let Some(existing) = maps.get_opt(&plan_name).await.map_err(BackupError::Api)? else {
        return Err(BackupError::Refused(
            TERMINAL_STATE_DISCOVERY_RESULT_UNREADABLE,
            format!(
                "the discovery plan ConfigMap {plan_name} this Job mounts no longer exists, so \
                 the digest its relay must carry cannot be established"
            ),
        ));
    };
    let Some(digest) = existing
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(plan::DIGEST_ANNOTATION))
        .cloned()
    else {
        return Err(BackupError::Refused(
            TERMINAL_STATE_DISCOVERY_RESULT_UNREADABLE,
            format!(
                "the discovery plan ConfigMap {plan_name} carries no {} annotation",
                plan::DIGEST_ANNOTATION
            ),
        ));
    };
    plan::accepts_existing(&existing, uid, &digest).map_err(|e| {
        BackupError::Refused(TERMINAL_STATE_DISCOVERY_RESULT_UNREADABLE, format!("{e}"))
    })?;
    Ok(FrameExpectations {
        plan_sha256: digest,
        subject_uid: uid.to_string(),
    })
}

/// The instant this discovery actually finished, or `None` when the API server
/// recorded none.
///
/// It refuses rather than read a clock: [`super::kafka_cluster::observed_at`]
/// falls back to `now`, which is right for a status field and wrong for an
/// immutable frozen plan. A pass that died between the plan `POST` and the
/// `status.execution` patch would re-render with a different
/// `selection.discovery.observedAt`, a different digest, and a terminal
/// `PlanConfigMapConflict` on a run that was fine. The `MIN_UTC` sentinel reuses
/// `observed_at` unchanged: it returns its `now` argument when it finds nothing,
/// and no Job finished at the start of the representable range.
#[must_use]
pub fn recorded_finish(job: &Job, pod: Option<&Pod>) -> Option<DateTime<Utc>> {
    let sentinel = DateTime::<Utc>::MIN_UTC;
    let found = super::kafka_cluster::observed_at(job, pod, sentinel);
    (found != sentinel).then_some(found)
}

/// D1 §7.2 R9's last two steps, in order: record `TopicsResolved=True`, then
/// arm the discovery Job's TTL. The one call `controllers::backup` makes after
/// the freeze, with `backup` as the freeze left it (`status.execution` and
/// `status.selection` applied), since the message quotes its count and the
/// patch carries its conditions.
///
/// Returns the patch it sent (`{}` when the status already said this) and where
/// the object now stands ([`StatusVersion`], seam **S7**) for the caller's next
/// write. The TTL is patched only when the status write landed; after a 409 the
/// relay must outlive the pass that reads the newer object.
///
/// # Errors
///
/// [`BackupError::Api`] for a transport failure.
pub async fn record_resolved(
    backup: &Backup,
    client: &kube::Client,
    namespace: &str,
    job_name: &str,
    now: DateTime<Utc>,
) -> Result<(Value, StatusVersion), BackupError> {
    let api: Api<Backup> = Api::namespaced(client.clone(), namespace);
    let patch = resolved_status_patch(backup, now);
    let (committed, at) = write_status(&api, backup, patch.clone()).await?;
    if committed {
        check::set_ttl(client, namespace, job_name)
            .await
            .map_err(BackupError::Api)?;
    }
    Ok((patch, at))
}
