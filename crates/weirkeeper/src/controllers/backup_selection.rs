//! Dynamic topic selection for one run — D1 §7.2, PLAT-09.2.
//!
//! # What this module is, in one sentence
//!
//! The second producer of a [`ResolvedSelection`]: where
//! [`ResolvedSelection::named`] reads `spec.topics`, this resolves
//! `spec.allUserTopics` by running ONE `topicInventory` check Job owned by the
//! `Backup` itself, classifying what it saw, and handing the exact byte-sorted
//! names to the same freeze the named path uses.
//!
//! # The four rulings this file is built on, and the one it supersedes
//!
//! * **D-SEAMS S1 — one check runner.** D1 §7.3 proposed a
//!   `logweir topics discover` subcommand with its own `discovery-topic=`
//!   stdout grammar. That is SUPERSEDED: per-run discovery runs D2's
//!   `logweir check run --plan <file> --check-contract-version 1` with plan
//!   kind `topicInventory`, through [`crate::check`], and reuses D2's frames,
//!   digests and closed error codes. There is one runner contract, one argv
//!   allowlist surface and one classification table.
//! * **D-SEAMS S2 — a discovery RESULT is never an execution input.** Nothing
//!   here reads a `TopicDiscovery`. A dynamic run discovers afresh, per run, in
//!   its own owned Job, and freezes the names and the discovery summary into
//!   its own immutable plan `ConfigMap`. That is also why a run's topic set is
//!   never stale and why a retry — which is a NEW `Backup` — rediscovers.
//! * **D-SEAMS S3 — completeness vocabulary.** `unknown | limited |
//!   attestedComplete` is [`logweir_core::check_contract::visibility`] and
//!   nothing else, and D1's coverage labels are DERIVED from it
//!   ([`coverage_for`]), never computed a second way.
//! * **D-SEAMS S6 — pod identity is the owner UID.** The relay is read off the
//!   pod [`crate::check::pod::find_owned_pod`] proved, never off a label match.
//! * **D-SEAMS S4/S7** — the names land in PLAT-06.1's one frozen grammar, and
//!   every status write here is a `resourceVersion`-preconditioned merge PATCH.
//!
//! # The shape of a pass
//!
//! `resolve` is called from ONE place (`controllers::backup`'s freeze branch,
//! and only when `status.execution` is not yet recorded), and it answers with
//! one of three things:
//!
//! | answer | what the controller does |
//! |---|---|
//! | [`Resolution::Pending`] | nothing more this pass; the reconciler's own 15 s requeue is D1 §7.2 R2's |
//! | [`Resolution::Resolved`] | freeze it, exactly as a named allowlist is frozen |
//! | [`Resolution::Refused`] | stop: the terminal status is already on the object |
//!
//! # Why a refusal is written HERE and not raised as `BackupError::Refused`
//!
//! Because `TopicsResolved` and `Failed` have to land in the SAME patch. A
//! merge patch replaces `status.conditions`, and the one refusal writer
//! (`controllers::backup`'s `BackupError::Refused` arm) builds its array from
//! the `Backup` this pass STARTED with — which cannot carry a condition this
//! pass has just written. Two patches would therefore publish `Failed:
//! DiscoveryIncomplete` beside a stale `TopicsResolved: DiscoveryRunning`. One
//! patch, built here, says both true things at once (D1 §3.4).
//!
//! # What this module never does
//!
//! It dials no broker, reads no `Secret`, writes no archive and deletes
//! nothing. It creates exactly two objects — the discovery Job and its
//! immutable plan `ConfigMap` — both owned by the `Backup` with
//! `controller: true`, so the `Backup`'s own cascade collects them and no
//! `delete` verb is needed (`config/rbac/role.yaml` grants it on nothing).

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
use crate::check::{self, job as cjob, plan, policy, relay};
use crate::conditions::{
    current_condition, merge_condition, status_unchanged, StatusVersion, CONDITION_TOPICS_RESOLVED,
    REASON_DISCOVERY_RUNNING, REASON_RESOLVED, TERMINAL_STATE_DISCOVERY_FAILED,
    TERMINAL_STATE_DISCOVERY_INCOMPLETE, TERMINAL_STATE_DISCOVERY_RESULT_UNREADABLE,
    TERMINAL_STATE_INVALID_TOPIC_SELECTION, TERMINAL_STATE_JOB_NAME_CONFLICT,
    TERMINAL_STATE_REFERENT_NOT_FOUND, TERMINAL_STATE_SELECTION_EMPTY,
    TERMINAL_STATE_SELECTION_TOO_LARGE, TERMINAL_STATE_SOURCE_CHANGED_DURING_RESOLUTION,
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

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// The prefix of a per-run discovery Job's name — D1 §7.2 R2.
///
/// `lwd-<backup uid>` is **40 characters**, always, because a UID is 36. It is
/// deliberately NOT [`crate::check::job::check_job_name`]'s
/// `lwc-<k>-<20 hex of sha256(uid)>`: a `Backup`'s discovery Job has to be
/// findable from the `Backup`'s own UID by an operator holding nothing but
/// `kubectl`, and the name is recorded verbatim in the frozen
/// `selection.discovery.discoveryJob` that D1 §3.3 names.
pub const DISCOVERY_JOB_PREFIX: &str = "lwd-";

/// `logweir.dev/purpose` on the discovery Job and its pod — D1 §7.2 R2.
pub const LABEL_PURPOSE: &str = "logweir.dev/purpose";
/// The value of [`LABEL_PURPOSE`].
pub const PURPOSE_TOPIC_DISCOVERY: &str = "topic-discovery";

/// `app.kubernetes.io/component` on the discovery Job and its pod.
///
/// **`run-discovery`, AND DELIBERATELY NOT [`cjob::COMPONENT_CHECK`].** The two
/// values are how one `list` finds both populations while the two ceilings stay
/// separate: [`crate::check::limits::active_selector`] selects
/// `component in (check, run-discovery)` so a run's discovery is counted
/// against the per-connection ceiling that bounds simultaneous dials at one
/// broker, and [`crate::check::limits::check_selector`] — the console pool's
/// own membership — still selects `check` alone, so a nightly schedule cannot
/// queue a browser click. Nothing calls `limits::admit` for a run's discovery
/// (D1 §7.2 defines no admission for work an operator already scheduled), so
/// wearing `check` would have spent the interactive pool without ever being
/// bounded by it.
pub const COMPONENT_RUN_DISCOVERY: &str = "run-discovery";

/// The annotation carrying the digest of the source resolution the discovery
/// Job was dispatched against — D1 §7.2 R1 and R4.
///
/// **THE RECORD R4 COMPARES AGAINST.** The Job is created in one pass and read
/// in another; "the source did not change under the resolution" is only
/// checkable if the first pass wrote down what it resolved. It lives on the Job
/// rather than in the plan `ConfigMap` because the plan's own digest also
/// covers the request's limits, and an installation policy edited mid-run would
/// otherwise read as a changed SOURCE.
pub const SOURCE_SHA256_ANNOTATION: &str = "logweir.dev/discovery-source-sha256";

/// D1 §7.2 R8: the most names one run may freeze.
pub const MAX_RESOLVED_TOPICS: usize = 5_000;

/// D1 §7.2 R8: the most bytes those names may take.
///
/// A quarter of the 1 MiB a `ConfigMap` holds, which leaves room for the rest
/// of the frozen document beside the list.
pub const MAX_RESOLVED_TOPIC_BYTES: usize = 256 * 1024;

/// D1 §7.2 R2: the ceiling on a discovery Job's `activeDeadlineSeconds`.
pub const MAX_DISCOVERY_SECONDS: i64 = 300;

/// The `maxTopics` a run's discovery plan asks for.
///
/// **FOUR TIMES [`MAX_RESOLVED_TOPICS`], AND THAT GAP IS THE POINT.** A
/// truncated listing is refused ([`SelectionTooLarge`]), because a dynamic run
/// that froze the first N names of a cut-off listing would claim coverage over
/// a set nobody chose. Asking for far more than a run may freeze means the
/// refusal an operator gets is the precise one — "this cluster has more user
/// topics than one run may name" — computed after internal topics and the
/// exclusions have been taken off, rather than the blunt "the listing was cut".
///
/// It is a constant and not the installation policy's `hardMaxTopics`: that
/// ceiling bounds what a TENANT may ask an interactive discovery for, and a
/// run's own discovery is not a tenant request.
///
/// [`SelectionTooLarge`]: crate::conditions::TERMINAL_STATE_SELECTION_TOO_LARGE
pub const DISCOVERY_MAX_TOPICS: u32 = 20_000;

/// How many internal names the frozen `selection.discovery` records — D1 §3.3.
pub const MAX_INTERNAL_NAMES: usize = 50;

/// How many rule-excluded names it records — D1 §3.3.
pub const MAX_EXCLUDED_NAMES: usize = 200;

/// The most bytes of NAMES the frozen `selection.discovery` block may carry —
/// `internalExcluded.names` and `excludedByRule.names` together.
///
/// **SIXTEEN KIBIBYTES, SHARED, AND A BYTE BOUND BESIDE THE COUNT BOUNDS.**
/// [`MAX_INTERNAL_NAMES`] and [`MAX_EXCLUDED_NAMES`] bound how MANY names are
/// recorded; on their own they admit 250 names of 249 bytes each — about 61 KiB
/// of provenance beside a topic list already bounded at
/// [`MAX_RESOLVED_TOPIC_BYTES`], in a `ConfigMap` bounded at one MiB. These
/// lists are a SAMPLE an operator reads to answer "which ones?", not a store:
/// the counts are exact whatever happens to the names, and `truncated` says
/// when the sample was cut. Sixteen KiB is roughly 250 names at a typical
/// length and leaves the plan's budget to the thing the run actually executes.
///
/// The budget is spent in a fixed order — internal first, then rule-excluded —
/// so two runs that observed the same cluster freeze the same bytes.
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

// ---------------------------------------------------------------------------
// Pure: the exclusion policy
// ---------------------------------------------------------------------------

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

    /// Whether `name` is excluded — **LITERAL, in both halves**.
    ///
    /// An exact name is `==` and a prefix is [`str::starts_with`]. Guard
    /// **G-GLOB** is the reason: neither field is a pattern language, `orders*`
    /// is not writable in either (the CRD's `TOPIC_NAME_PATTERN` has no glob
    /// characters in its set), and a `*` that somehow arrived would exclude
    /// nothing rather than everything.
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

// ---------------------------------------------------------------------------
// Pure: classification
// ---------------------------------------------------------------------------

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

/// Split a relayed inventory into the four buckets D1 §7.2 R5 names — **pure**.
///
/// The order of the tests is the order of the rule, and it decides which bucket
/// a name that qualifies for two lands in:
///
/// 1. **internal** — `entry.internal` OR the name starts with `__`. BOTH, and
///    not one: the flag is the runner's reading of the same `__` convention
///    ([`TopicEntry::name_is_internal`]), and applying the rule here as well is
///    what makes an older or a lying runner unable to slip `__consumer_offsets`
///    into a backup.
/// 2. **limited** — the entry carries an error. A listing entry the broker
///    answered [`CheckCode::TopicAuthorizationFailed`] for names a topic this
///    principal cannot describe; freezing it would put a name in `backup.yaml`
///    that the run then cannot read. Any other per-entry error is the same
///    class of fact — the entry is not usable — and is counted the same way.
/// 3. **excludedByRule** — [`Exclusions::excludes`].
/// 4. everything left is **resolved**.
///
/// Byte order, not `char` order: the frozen list's stability and every digest
/// over it depend on `String`'s own `Ord`, which is byte order, and that is
/// what `logweir_kafka::inventory::assemble` already sorted the relay by. It is
/// re-applied here anyway, because the sort is a property of the FROZEN list
/// and not something a run may take a runner's word for.
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

/// A bounded name list for the frozen block.
/// A bounded SAMPLE of `names` for the frozen provenance block — by count and
/// by bytes, spending from a shared `budget`.
///
/// **THE COUNT IS ALWAYS EXACT.** Only the sample is cut, and `truncated` says
/// so; a reader that needs the number reads `count`, and a reader that needs
/// every name reads the plan's own `topics` or asks the cluster. Both bounds
/// are needed: the count bound alone admits 249-byte names, and a byte bound
/// alone would admit a hundred thousand one-character ones.
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

// ---------------------------------------------------------------------------
// Pure: coverage
// ---------------------------------------------------------------------------

/// What the run may claim, from the visibility verdict and the policy —
/// **pure**, D1 §7.2 R6 and §7.4, D-SEAMS **S3**.
///
/// `None` means REFUSE: the visibility could not be established and the policy
/// chose [`IncompleteDiscovery::Refuse`], which is terminal
/// `DiscoveryIncomplete`.
///
/// Note what is NOT here: there is no path from an observation to
/// [`Coverage::AllUserTopicsAttested`] except `VisibilityState::AttestedComplete`,
/// and that state is reachable only through an administrator attestation in the
/// installation policy `ConfigMap` — so a run cannot claim whole-cluster
/// coverage because a listing happened to succeed.
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

// ---------------------------------------------------------------------------
// Pure: the source digest (R1 / R4)
// ---------------------------------------------------------------------------

/// The fields a run's source resolution consists of, for the R4 comparison.
///
/// `ConnectionPlan` and not the whole [`ResolvedConnection`]: the plan is the
/// use-independent half — the addresses, the auth mode, the principal, the CA
/// path and the password VARIABLE name — so resolving the same
/// `KafkaCluster` as `Discovery` and later as `BackupSource` digests to the
/// same value, while a changed bootstrap list, a changed principal, a changed
/// auth mode or a replaced cluster object does not.
///
/// # `status.clusterId` IS DELIBERATELY NOT IN HERE
///
/// It would look like the obvious thing to pin, and it is the one field that
/// must not be: `KafkaCluster.status.clusterId` is written **asynchronously by
/// another controller**, and `controllers::kafka_cluster` clears it to `None`
/// on every probe pass that reports the cluster unreachable or whose output it
/// could not read. It therefore flips `Some → None → Some` on ordinary probe
/// churn, inside the ≤ 300 s a discovery Job runs — so hashing it would refuse
/// a perfectly good run as `SourceChangedDuringResolution`, with a message
/// saying the saved connection changed when nothing about it had. D1 §7.2 R1
/// enumerates the digest's inputs as "clusterUid, bootstrapServers, auth,
/// credential env, TLS refs", and the observed id is not among them. The
/// repointed-endpoint case it would have caught is covered by
/// [`source_unchanged`]'s explicit comparison, which fires only when the
/// broker-reported id and the observed id are BOTH present and differ.
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

/// The connection half of the check plan.
///
/// **REUSED, NOT REIMPLEMENTED**: this is
/// [`crate::controllers::topic_discovery::connection_plan`], so a run's own
/// discovery dials exactly what an interactive one dials, with the CA reaching
/// the pod as a projected mount (never inlined — a `KafkaCluster` may name its
/// CA in a `Secret` this controller holds no verb on) and no credential at any
/// field of a world-readable `ConfigMap`.
#[must_use]
pub fn connection_plan(resolved: &ResolvedConnection) -> ConnectionPlan {
    super::topic_discovery::connection_plan(resolved)
}

// ---------------------------------------------------------------------------
// Pure: the plan and the Job
// ---------------------------------------------------------------------------

/// The least a runner may be given to list a catalogue, in seconds.
///
/// **THIRTY, AND IT IS A REFUSAL AND NOT A CLAMP.** The Job's
/// `activeDeadlineSeconds` is D1 §7.2 R2's `min(300, spec.deadlineSeconds)`
/// and [`cjob::DEADLINE_MARGIN_SECONDS`] of that is image pull, scheduling and
/// container start — so a `Backup` whose own `deadlineSeconds` is 60 leaves the
/// runner well under a second to connect, authenticate and list. Raising the
/// Job's deadline to compensate would break R2's ceiling; silently dispatching
/// a Job that cannot succeed produces `DiscoveryFailed` with nothing naming the
/// deadline as the cause, which is the failure an operator cannot act on. So
/// the run is refused up front, naming the field and this number.
pub const MIN_DISCOVERY_BUDGET_SECONDS: i64 = 30;

/// The smallest `spec.deadlineSeconds` that can fund a discovery at all —
/// [`MIN_DISCOVERY_BUDGET_SECONDS`] plus the Job's start-up margin.
pub const MIN_DYNAMIC_DEADLINE_SECONDS: i64 =
    MIN_DISCOVERY_BUDGET_SECONDS + cjob::DEADLINE_MARGIN_SECONDS;

/// The runner's own budget for one discovery, in seconds — **pure**.
///
/// D1 §7.2 R2 caps the Job's `activeDeadlineSeconds` at
/// `min(300, spec.deadlineSeconds)`; [`cjob::DEADLINE_MARGIN_SECONDS`] of that
/// is image pull, scheduling and container start, which the runner cannot bound
/// and must not be charged for. So the budget written into the PLAN is the
/// Job's deadline minus that margin.
///
/// `None` when what is left is under [`MIN_DISCOVERY_BUDGET_SECONDS`]: there is
/// no budget to write and the caller refuses rather than dispatching a Job that
/// cannot finish.
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

/// The `topicInventory` plan one run's discovery runs — **pure**.
///
/// `include_internal: true`, which is the opposite of an interactive
/// discovery's default and is deliberate: D1 §3.3 requires the frozen block to
/// record the internal topics BY NAME (`internalExcluded.names`), and a runner
/// that dropped them would leave the controller with a count and nothing to
/// show an operator asking which ones. The exclusion itself is
/// [`classify`]'s — applied by the controller, twice over (the flag and the
/// `__` rule), which is where D1 §7.2 R5 puts it.
///
/// `expected_topics` is empty: the targeted probe answers "is this NAMED topic
/// visible", and a dynamic selection names nothing.
#[must_use]
pub fn plan_document(resolved: &ResolvedConnection, subject_uid: &str, timeout: u32) -> CheckPlan {
    CheckPlan {
        contract: CHECK_PLAN_CONTRACT.to_string(),
        contract_version: CHECK_CONTRACT_VERSION,
        subject_uid: subject_uid.to_string(),
        timeout_seconds: timeout,
        // NO POLICY DIGEST. An interactive discovery pins one because the
        // installation policy supplied its `maxTopics`; this plan's limits are
        // this module's constants, so there is no policy fact for the document
        // to carry and an always-present digest of an unread policy would be a
        // claim nothing supports.
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

/// The owner reference every object this module creates carries.
///
/// From `kube::Resource`'s own `api_version`/`kind`, never two string literals:
/// a hand-written `apiVersion` that drifts from the CRD makes the garbage
/// collector refuse to resolve the owner, and an unresolvable owner is a
/// cascade that silently does not happen.
#[must_use]
pub fn owner_of(backup: &Backup, uid: &str) -> RunnerOwner {
    RunnerOwner {
        api_version: Backup::api_version(&()).to_string(),
        kind: Backup::kind(&()).to_string(),
        name: backup.name_any(),
        uid: uid.to_string(),
    }
}

/// The labels on the discovery Job and on its pod template.
///
/// # `app.kubernetes.io/component` is `run-discovery`, not `check`
///
/// See [`COMPONENT_RUN_DISCOVERY`]. The two values let ONE `list` find both
/// populations — which is what puts a run's discovery inside the per-connection
/// ceiling — while the console pool's own membership stays `check` alone, so a
/// nightly schedule cannot queue a browser click over a bound nothing here is
/// subject to.
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
/// # What is the framework's, and what is this module's
///
/// [`cjob::runner_job_spec`] decides everything a check pod IS: the argv
/// (`check run --plan /check/check-plan.json --check-contract-version 1`), the
/// `/check` mount, the three pinned environment variables, the runner
/// ServiceAccount with **no** token, no signing key, no archive and no `/plan`
/// mount. Three things are this module's, and each is a D1 §7.2 requirement the
/// framework has no opinion on: the NAME (`lwd-<backup uid>`), the
/// `activeDeadlineSeconds` (`min(300, spec.deadlineSeconds)` rather than the
/// framework's budget-plus-margin), and the labels plus the R4 annotation.
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
    job.metadata.labels = Some(labels.clone());
    job.metadata.annotations = Some(std::collections::BTreeMap::from([(
        SOURCE_SHA256_ANNOTATION.to_string(),
        source_sha256.to_string(),
    )]));
    if let Some(job_spec) = job.spec.as_mut() {
        // D1 §7.2 R2, EXACTLY. `cjob::runner_job_spec` adds a ninety-second
        // margin to the plan's own budget, which is right for a check whose
        // deadline is its own; here the ceiling is the RUN's, and
        // `discovery_budget` already took the margin out of the plan's side so
        // the two agree.
        job_spec.active_deadline_seconds = Some(job_deadline);
        let mut meta = job_spec.template.metadata.take().unwrap_or_default();
        meta.labels = Some(labels);
        job_spec.template.metadata = Some(meta);
    }
    job
}

// ---------------------------------------------------------------------------
// Pure: building the frozen selection (R5–R9)
// ---------------------------------------------------------------------------

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

    // R3: A NAME NO BROKER WOULD ACCEPT IS A RESULT THIS RUN DOES NOT ADOPT.
    // Every name here came off a runner's stdout, not out of a CRD field the
    // API server pattern-checked, and the glob rail two functions away covers
    // six characters — not a space, a slash, a control character or a
    // 250-character name. `DiscoveryResultUnreadable` and not
    // `InvalidTopicSelection`, because nothing is wrong with the SELECTION the
    // operator wrote: the runner's output is what did not verify, and that is
    // the not-retryable class.
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
                "the discovery for {backup_name} resolved {} topics taking {bytes} bytes of \
                 names, over the {MAX_RESOLVED_TOPICS} / {MAX_RESOLVED_TOPIC_BYTES} bytes one \
                 run may freeze; the plan ConfigMap a run mounts is bounded at one MiB. Exclude \
                 more, or split the cluster across schedules",
                topics.len()
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
        // THE DIGEST THE CONTROLLER COMPUTED OVER THE FRAMES, which
        // `check_result_against_frames` has already held the runner's own
        // document to. A consumer re-hashing the frozen names cannot reproduce
        // it (the names here are a subset), and it is not meant to: it names
        // the OBSERVATION this selection came from.
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

// ---------------------------------------------------------------------------
// The answer one pass gives
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Status writes (D-SEAMS S7)
// ---------------------------------------------------------------------------

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
/// Returns `false` when the API server answered 409 — something wrote this
/// status between the read and the write, so this pass's conclusion is NOT on
/// the server and the next pass reads the newer object. That is the value the
/// TTL ordering depends on.
///
/// The second half is WHERE THE OBJECT NOW STANDS ([`StatusVersion`]): a pass
/// that writes again after this one has to precondition on the version this
/// write left behind and not on the one it was handed. It is
/// [`StatusVersion::default`] — no version at all — after a 409, because after
/// a 409 the version is genuinely unknowable from here.
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

// ---------------------------------------------------------------------------
// The resolution
// ---------------------------------------------------------------------------

/// Resolve `spec.allUserTopics` for one run — **the entry point
/// `controllers::backup` calls**, D1 §7.2.
///
/// It is called ONLY from the freeze branch and ONLY when `status.execution` is
/// not yet recorded: a run whose inputs are frozen reads its selection back out
/// of its own plan ([`crate::backup_execution::stored_selection`]) and never
/// discovers again, which is D1 §12's
/// `a_frozen_dynamic_backup_never_reruns_discovery`.
///
/// # The image is the PROCESS's, not the compiled-in pin
///
/// `runner` is `controllers::Context::runner_image`, threaded from the one
/// call site — the same value the run's own runner Job takes. A discovery Job
/// runs the same image a run does, so a `None` here (the default, and what
/// every route-table test sees) means the compiled pin
/// [`crate::job::RUNNER_IMAGE`] under [`crate::job::IMAGE_PULL_POLICY`], and
/// the values an operator set through [`crate::job::RUNNER_IMAGE_ENV`] and
/// [`crate::job::RUNNER_PULL_POLICY_ENV`] reach this Job too.
/// Defect **D1-DISCOVERY-IMAGE**: this parameter did not exist, so on every
/// installation the discovery Job named the compile-time digest under
/// `imagePullPolicy: Never` (`ErrImageNeverPull` -> `DeadlineExceeded` ->
/// `TopicsResolved=False/DiscoveryFailed`) while the runner Job of the same
/// namespace and minute ran the configured image. Every other Job builder
/// takes the same override at its own call site
/// (`kafka_cluster.rs`, `topic_discovery.rs`, `recovery_catalog.rs`,
/// `retention_policy.rs`).
///
/// # Errors
///
/// [`BackupError::Api`] for a transport failure, which requeues. A refusal is
/// **not** an error: it is [`Resolution::Refused`] with the terminal status
/// already written (see the module header).
pub async fn resolve(
    backup: &Backup,
    client: &kube::Client,
    namespace: &str,
    now: DateTime<Utc>,
    runner: &RunnerImage,
) -> Result<Resolution, BackupError> {
    // THE NAME OF A FINISHED DISCOVERY JOB THIS PASS OBSERVED, for the TTL on
    // the refusal path (L6). It stays `None` for a refusal raised before the
    // Job exists or before it finished, where a TTL patch would be a 404 or
    // would arm the collector against a pod still holding a relay.
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
            // THE SAME ORDERING THE RESOLVED PATH USES, AND FOR THE SAME
            // REASON. A refused run has reached its conclusion, so the relay on
            // the discovery pod is no longer needed and the Job may be
            // collected — but only once the terminal status is ON THE SERVER. A
            // patch that answered 409 did not land, so the next pass has to
            // read the relay again and the pod must outlive this one.
            if committed {
                if let Some(job_name) = finished_job.as_deref() {
                    set_discovery_ttl(client, namespace, job_name).await?;
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

    // R1. THE ONE RESOLUTION (PLAT-07.1), and the digest R4 compares against.
    let cluster = source_cluster(backup, client, namespace).await?;
    let resolved = connection::resolve(&cluster, ConnectionUse::Discovery)
        .map_err(|r| BackupError::Refused(r.reason, r.message))?;
    // The guard the other connection-projecting paths call: one `namespace`
    // variable feeds the `KafkaCluster` GET and the Job, so it is provably the
    // same namespace — this makes that a checked property rather than a
    // coincidence of two lookups.
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

    // A JOB THIS BACKUP DOES NOT CONTROL IS NEVER OBSERVED. The name is derived
    // from this object's UID, so a foreign Job at that name is a forgery rather
    // than a collision — and its stdout would otherwise become this run's
    // allowlist.
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
    // M3 / D1 §7.2 R2: REFUSE UP FRONT RATHER THAN DISPATCH A JOB THAT CANNOT
    // FINISH. `spec.deadlineSeconds` has no `minimum` on the CRD, so a legal
    // `deadlineSeconds: 60` would otherwise leave the runner under a second to
    // connect, authenticate and list — and the run would die `DiscoveryFailed`
    // with nothing naming the deadline as the cause. Raising the Job's own
    // deadline instead would break R2's `min(300, spec.deadlineSeconds)`.
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
        // NO `source-ca.pem`: the CA reaches the pod through the resolver's own
        // projection, because a `KafkaCluster` may name it in a Secret this
        // controller holds no verb on.
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
        // THE PROCESS'S IMAGE, AS EVERY OTHER CONTROLLER-BUILT JOB TAKES IT
        // (Task 33's image, Task 37's pull policy). A discovery Job runs the
        // same runner image a run does, so it takes the same override the
        // run's own Job takes — `None` in either leaves the compiled-in
        // constant in place, which is what every test that does not pass one
        // sees. Defect D1-DISCOVERY-IMAGE: these two lines used to be a
        // hard-coded `None` pair, so a configured
        // `job::RUNNER_IMAGE_ENV` reached the runner Job and NOT this one.
        image: runner.image.clone(),
        image_pull_policy: runner.image_pull_policy.clone(),
    };
    let desired = build_discovery_job(&spec, job_name, job_deadline, source_sha256);
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

    // D-SEAMS S6. The `batch.kubernetes.io/job-name` label narrows the list;
    // the controller `ownerReference` UID decides. A pod that wears the label
    // and is owned by something else is IGNORED and logged, never read — its
    // stdout would otherwise become this run's allowlist.
    let pod = check::pod::find_owned_pod(client, namespace, job)
        .await
        .map_err(BackupError::Api)?;
    let log = match pod.as_ref() {
        Some(p) => {
            let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
            // THE FULL LOG, through the framework's own bounded parameters —
            // not the eight-line tail the exit-code path reads. `limitBytes`
            // bounds what the API server sends and the decoder's own budget
            // bounds what it will accumulate; a body over the relay budget is
            // refused rather than parsed.
            match pods.logs(&p.name_any(), &relay::log_params()).await {
                Ok(log) => Some(log),
                Err(e) if check::is_log_absent(&e) => None,
                Err(e) => return Err(BackupError::Api(e)),
            }
        }
        None => None,
    };

    let expect = expectations_for(client, namespace, job_name, uid).await?;
    let observation = check::classify(&check::Input {
        job,
        pod: pod.as_ref(),
        // EMPTY: `events: list` is a grant this controller does not hold, and
        // `manifest_lint::every_call_site_has_a_grant` fails the moment an
        // `Api<Event>` appears here without it. Every pod-status-sourced
        // waiting code still works.
        events: &[],
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
        check::CheckPhase::Failed => {
            return Err(BackupError::Refused(
                discovery_failure_state(observation.reason),
                format!(
                    "the topic discovery Job {job_name} for {name} did not produce a usable \
                     result ({}): {}",
                    observation.reason,
                    redact(&observation.message)
                ),
            ))
        }
        check::CheckPhase::Succeeded => {}
    }

    let relay = observation
        .relay
        .as_ref()
        .expect("check::classify returns a relay with every Succeeded phase");
    // THE WHOLE DOCUMENT IS BOUND, not just `inventory` — defect
    // `D2-RESULTUNREADABLE`, the dynamic-`Backup` twin of the one fixed in
    // `topic_discovery::commit`. `result.checks` is where a runner that could
    // not reach the broker puts its answer, and a binding that dropped it made
    // every classified failure read `DiscoveryResultUnreadable`.
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
        // NO INVENTORY IS NOT THE SAME FACT AS NO RESULT, and neither of them
        // is "the runner is broken". `check::kinds::inventory`'s own module
        // doc says so: "an unreachable broker produces a result document with
        // no `inventory` block and one `connection.authenticated` row carrying
        // the classified code … so the controller reads an actionable reason
        // instead of `ResultUnreadable`". This is the half that reads it.
        let (code, message) = no_inventory_refusal(document.as_ref().map(|d| d.checks.as_slice()));
        return Err(BackupError::Refused(
            // THE D1 VOCABULARY, NOT THE RAW CODE. A `Backup`'s terminal
            // states are a closed, documented set and `TopicsResolved`'s
            // reason is one of them, so the broker's code is mapped through
            // the SAME `discovery_failure_state` the `Failed`-phase branch
            // above uses and is NAMED in the message. That mapping is the
            // user-visible half of this defect: `DiscoveryResultUnreadable`
            // documents "no, not without fixing the runner", so an unreachable
            // broker or a rotated password used to tell an operator their
            // RUNNER was broken and that a new `Backup` could not help — when
            // both are `DiscoveryFailed`, and a new `Backup` is exactly the
            // remedy.
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

    // R6. THE VERDICT IS `check_contract::visibility`'s AND NOBODY ELSE'S
    // (D-SEAMS S3), and the attestation that could raise it to
    // `attestedComplete` comes from the installation policy `ConfigMap` through
    // the SAME fail-closed predicate an interactive discovery uses.
    let load = policy::load(
        client,
        super::topic_discovery::configured_policy_ref().as_ref(),
        &policy::PolicyCache::new(),
        now,
    )
    .await
    .map_err(BackupError::Api)?;
    let signals = super::topic_discovery::visibility_signals(
        &inventory,
        namespace,
        &resolved.cluster_name,
        &resolved.principal,
    );
    let attestation = super::topic_discovery::attestation_candidate(
        &load.policy().discovery.visibility_attestations,
        namespace,
        &resolved.cluster_name,
        &resolved.principal,
        &signals.cluster_id,
    );
    let verdict = visibility(&signals, attestation, now);

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

/// D1 §7.2 R3's split: which refusals a retry could survive.
///
/// `DiscoveryResultUnreadable` is the MALFORMED-OUTPUT class and is not
/// retryable — a runner that printed something unverifiable will print it
/// again. Everything else is operational (an unreachable broker, a pod that
/// never started, a deadline) and a retry — which is a NEW `Backup`, and
/// therefore a fresh discovery — can succeed.
///
/// Both are terminal for THIS run: `Backup.spec` is CEL-immutable, so a requeue
/// could never resolve differently, and D1 §7.7 says a refused dynamic run is
/// replaced rather than restarted.
#[must_use]
pub fn discovery_failure_state(code: CheckCode) -> &'static str {
    match code {
        CheckCode::ResultUnreadable
        | CheckCode::CheckContractMismatch
        | CheckCode::RunnerContractUnsupported => TERMINAL_STATE_DISCOVERY_RESULT_UNREADABLE,
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

/// The blocking check a no-inventory result is ABOUT — **pure**.
///
/// The precedence is [`logweir_core::check_contract::aggregate`]'s own and is
/// not re-invented here in a second spelling: `notReady` first, then `unknown`
/// or `skipped`, and among several rows of one state the FIRST in document
/// order. That second arm is load-bearing rather than defensive —
/// `MetadataTimeout` is an `unknown` code and it is one of the two reasons D2
/// §14.4 S11 accepts for a timed-out discovery, so a rule that projected only
/// `notReady` would leave exactly that scenario unclassified.
///
/// ADVISORY rows are excluded: an advisory `notReady` is a warning by
/// definition (D2 §6.4) and a warning that became the terminal reason would
/// name the wrong cause. EXECUTION-ONLY rows are excluded with them —
/// `CheckOutcome::new` forces one to `unknown`, and it describes a run that has
/// not happened (D2 §6.3), so projecting it would refuse this `Backup` for a
/// check that never ran.
fn blocking_failure(checks: &[CheckOutcome]) -> Option<&CheckOutcome> {
    let blocking = || checks.iter().filter(|c| c.gating == Gating::Blocking);
    blocking()
        .find(|c| c.state == CheckState::NotReady)
        .or_else(|| {
            blocking().find(|c| matches!(c.state, CheckState::Unknown | CheckState::Skipped))
        })
}

/// The [`CheckCode`] and sentence for a `Succeeded` discovery whose relay
/// carried no topic inventory — **pure**. Defect `D2-RESULTUNREADABLE`.
///
/// `None` means no result document was relayed at all; `Some(checks)` is the
/// document's own `checks` array, already redacted and capped by
/// [`logweir_core::check_contract::CheckRelay::result`].
///
/// The code is the failing check's [`CheckCode`], a CLOSED vocabulary —
/// `BrokerUnreachable`, `MetadataTimeout`, `AuthenticationFailed`, … — so
/// nothing a runner writes reaches the terminal state as free text; the caller
/// maps it through [`discovery_failure_state`] exactly as the `Failed`-phase
/// branch does. The sentence carries the check id, the runner's message and its
/// remedy, and `refused_status_patch` caps it as it caps every other string.
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
/// `ConfigMap`.
///
/// **READ, NOT RE-RENDERED.** The three-part rule — owner UID, digest
/// annotation, `immutable: true` — is [`plan::accepts_existing`]'s; only
/// "which digest" is this module's, and it is taken from the object that rule
/// has just proved belongs to this `Backup`.
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

/// The instant this discovery ACTUALLY finished, or `None` when the API server
/// recorded none — **pure**.
///
/// # Why this refuses instead of reading a clock
///
/// [`super::kafka_cluster::observed_at`] takes three sources in order — the
/// runner container's own `finishedAt`, the Job's `completionTime`, its
/// terminal condition's `lastTransitionTime` — and falls back to `now`. That
/// fallback is right for a `KafkaCluster` probe, whose `observedAt` is a status
/// field, and wrong here, where the value goes into an **immutable frozen
/// plan**: a pass that dies between the plan `POST` and the `status.execution`
/// patch would re-render the document on the next pass with a different
/// `selection.discovery.observedAt`, therefore a different digest, and
/// `freeze_execution_inputs` would refuse the run as a terminal
/// `PlanConfigMapConflict` — on a run that was fine, for no reason but the
/// passage of time.
///
/// The sentinel is how the one implementation above is reused rather than
/// copied: `observed_at` returns its `now` argument verbatim when it found
/// nothing, and no Job carries a finish instant at the beginning of the
/// representable range.
#[must_use]
pub fn recorded_finish(job: &Job, pod: Option<&Pod>) -> Option<DateTime<Utc>> {
    let sentinel = DateTime::<Utc>::MIN_UTC;
    let found = super::kafka_cluster::observed_at(job, pod, sentinel);
    (found != sentinel).then_some(found)
}

/// Patch the finished discovery Job's TTL — **only ever after the freeze's
/// status write landed**, D1 §7.2 R9 and D-SEAMS **S7**.
///
/// The TTL controller deletes a Job and its pods together and the relay lives
/// on the pod, so a TTL patched before the commit lets garbage collection race
/// a read a later pass might still have to make. The ordering is a guarantee
/// because the status write is `?`-propagated at the call site: a patch that
/// did not return 200 leaves the reconcile before this line.
///
/// # Errors
///
/// [`BackupError::Api`].
pub async fn set_discovery_ttl(
    client: &kube::Client,
    namespace: &str,
    job_name: &str,
) -> Result<(), BackupError> {
    check::set_ttl(client, namespace, job_name)
        .await
        .map_err(BackupError::Api)
}

/// D1 §7.2 R9's last two steps, in order: record `TopicsResolved=True` and then
/// arm the discovery Job's TTL — **the one call `controllers::backup` makes
/// after the freeze**.
///
/// `backup` is the object AS THE FREEZE LEFT IT (`status.execution` and
/// `status.selection` applied), because the condition's message quotes the
/// count the freeze recorded and because the merge patch has to carry the
/// conditions that object holds.
///
/// Returns the patch it sent, so the caller can apply it to its own views —
/// `{}` when the status already said this, which is not an error — and WHERE
/// THE OBJECT NOW STANDS, which the caller's own next write of this pass
/// preconditions on ([`StatusVersion`], seam **S7**).
///
/// **The TTL is patched only when the status write landed** (D-SEAMS **S7**):
/// the write is a `resourceVersion` compare-and-set, a 409 means the object
/// moved under this pass, and the relay on the discovery pod still has to
/// outlive the pass that will read the newer object.
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
        set_discovery_ttl(client, namespace, job_name).await?;
    }
    Ok((patch, at))
}
