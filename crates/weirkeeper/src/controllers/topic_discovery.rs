//! The `TopicDiscovery` reconciler — D2 §5, PLAT-09.1's controller half.
//!
//! # One object, one observation, one Job
//!
//! A `TopicDiscovery` is a REQUEST with an immutable `spec.request` (CEL rule
//! T1), so this reconciler never re-plans: it resolves the connection once,
//! renders one immutable plan `ConfigMap`, creates one isolated check Job with
//! no Kubernetes token, reads that Job's framed stdout through the shared
//! framework, stores the inventory as owned immutable `ConfigMap` chunks, and
//! commits ONE status. A refresh is a NEW object — which is exactly why the old
//! result stays readable while the new one runs, and why nothing here ever
//! rewrites a result in place.
//!
//! # The three things this file is not allowed to decide
//!
//! 1. **Completeness.** `unknown | limited | attestedComplete` is
//!    [`logweir_core::check_contract::visibility`] and nothing else (D-SEAMS
//!    **S3**). A successful listing is `unknown`; `limited` needs an OBSERVED
//!    authorization failure; `attestedComplete` needs an administrator
//!    attestation out of the installation policy `ConfigMap`, which only a
//!    release-namespace administrator can write. This file supplies signals and
//!    a clock reading, and copies the verdict.
//! 2. **Which pod may be read.** [`crate::check::pod::find_owned_pod`] proves
//!    the pod by its controller `ownerReference` UID (D-SEAMS **S6**, defect
//!    `SEC-PODLOG`). The `batch.kubernetes.io/job-name` label narrows the list
//!    and decides nothing: a check's stdout becomes a status and then an API
//!    response, and that label is writable by anything that can create a pod.
//! 3. **What a relay means.** [`crate::check::classify`] is the pure state
//!    machine; this file turns its [`crate::check::Observation`] into a phase
//!    and a condition, and never re-derives "did the Secret exist?" itself.
//!
//! # The result is advisory, and never an execution input (D-SEAMS **S2**)
//!
//! The plan this reconciler renders is an input to a CHECK Job. It is never an
//! input to a `Backup`, and nothing reads a `TopicDiscovery` result to decide
//! what a run captures: a dynamic run discovers afresh in its own owned Job and
//! freezes the names in its own immutable snapshot (D2 §5.11, PLAT-09.2). This
//! file creates one kind of object — a check Job and its `ConfigMap`s — and
//! patches one `/status`.
//!
//! # The commit point, and the ordering that makes a restart safe
//!
//! Chunks are written FIRST and the status patch that indexes them is the
//! commit; the Job's TTL is patched only after that patch returned 200. A
//! restart between the chunk writes and the commit re-reads the relay (the Job
//! is still there, because no TTL exists yet) and re-writes byte-identical
//! chunks, which [`crate::check::chunks::accepts_existing`]'s three-part rule
//! accepts. A restart after the commit finds a terminal object and stops.
//!
//! # What this file deliberately does NOT do
//!
//! * **It deletes nothing.** D2 §4.3's `gc.rs` — retention, keep-last-N per
//!   connection, and `DeleteParams { preconditions: { uid } }` — needs a
//!   `delete` verb that `config/rbac/role.yaml` grants on nothing and that
//!   `crates/logweir/tests/manifest_lint.rs` asserts twice is granted nowhere
//!   and called nowhere. Narrowing that tested claim is a deliberate RBAC
//!   decision and belongs to W11. The PURE half is here and tested —
//!   [`expired_terminal`] — so the wiring is a grant plus a call and not a
//!   design. Owned `ConfigMap`s and the check Job go by owner cascade either
//!   way, which is why a collected `TopicDiscovery` leaves nothing behind.
//! * **It builds no `Api<Event>`.** D2 §4.3's event-sourced waiting codes
//!   (`RunnerServiceAccountMissing`, `PodCreateRejected`, `SigningKeyMissing`,
//!   `VolumeMountFailed`) need `events: list`, which W11 grants. Until then the
//!   classifier is handed an EMPTY fact slice: every pod-status-sourced code
//!   still works, and the four event-sourced ones report as the more general
//!   state rather than as a fabricated one.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ConfigMap, Pod};
use kube::api::{Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::runtime::{watcher, Controller};
use kube::{Api, ResourceExt};
use serde_json::json;
use tracing::{debug, info, warn};

use logweir_core::check_contract::{
    redact, topic_tsv_sha256, visibility, Attestation, CheckCode, CheckPlan, CheckPlanKind,
    CheckRequest, ConnectionPlan, FrameExpectations, InventoryResult, TopicEntry,
    TopicInventoryRequest, TruncationReason, Visibility, VisibilityBasis, VisibilitySignals,
    CHECK_CONTRACT_VERSION, CHECK_PLAN_CONTRACT, DEFAULT_RELAY_BUDGET_BYTES,
    TOPIC_INVENTORY_FORMAT,
};

use super::approval::ReconcileError;
use crate::check::{self, chunks, job as cjob, limits, plan, policy, relay, CheckPhase};
use crate::conditions::{current_condition, merge_condition, status_unchanged};
use crate::connection::{self, ConnectionUse, ResolvedConnection};
use crate::crds::kafka_cluster::KafkaCluster;
use crate::crds::topic_discovery::{
    DiscoveryBinding, DiscoveryCounts, DiscoveryVisibility, ExpectedTopicOutcome, InventoryChunk,
    TopicDiscovery, TopicDiscoveryStatus, TopicInventoryResult,
};
use crate::crds::{Condition, Time};
use crate::job::RunnerOwner;

// ---------------------------------------------------------------------------
// The vocabulary a reader of this status is held to
// ---------------------------------------------------------------------------

/// The ONE condition type this kind carries — D2 §5.1.
pub const CONDITION_COMPLETE: &str = "Complete";

/// `status.phase` before the controller has written anything.
///
/// **NEVER WRITTEN BY THIS FILE**, and that is the point: an object with no
/// status reads as `Pending`, and the first patch this reconciler makes is
/// already `Queued`, `Running` or terminal. A controller that wrote `Pending`
/// first would spend an API call, and a status write, saying "I have seen this
/// and done nothing".
pub const PHASE_PENDING: &str = "Pending";
/// Over a concurrency ceiling (D2 §4.4). Not an error: it is retried.
pub const PHASE_QUEUED: &str = "Queued";
/// The check Job exists and has not finished.
pub const PHASE_RUNNING: &str = "Running";
/// A verified relay was decoded and its chunks committed.
pub const PHASE_SUCCEEDED: &str = "Succeeded";
/// Terminal, with a closed [`CheckCode`] in `status.reason`.
pub const PHASE_FAILED: &str = "Failed";
/// `spec.cancelRequested` reached a non-terminal object.
pub const PHASE_CANCELLED: &str = "Cancelled";

/// Every terminal phase. A terminal object is reconciled no further.
pub const TERMINAL_PHASES: [&str; 3] = [PHASE_SUCCEEDED, PHASE_FAILED, PHASE_CANCELLED];

/// How long a running check waits before it is looked at again.
///
/// The Job is also WATCHED (`.owns(jobs, …)`), so this is the backstop and not
/// the mechanism: a Job that finishes wakes this reconciler immediately, and
/// the requeue covers the pod-status transitions (`ImagePullBackOff`,
/// `Unschedulable`) that change no Job field at all.
pub const REQUEUE_RUNNING_SECS: u64 = 10;

/// How long a terminal object waits before it is looked at again.
///
/// One hour, and it exists for the `gc.rs` W11 will wire ([`expired_terminal`]).
/// Until then it is the cheapest possible no-op: the reconcile returns at the
/// terminal guard before it reads anything.
pub const REQUEUE_TERMINAL_SECS: u64 = 3600;

/// How long a reconcile that ERRORED waits.
pub const ERROR_REQUEUE_SECONDS: u64 = 30;

/// Seconds added to `2 × timeoutSeconds` before a non-terminal object whose Job
/// has vanished is declared `Stalled` — D2 §4.3's `gc.rs` row.
///
/// FIVE MINUTES on top of twice the check's own budget. The margin is
/// deliberately generous because the observation this rule acts on — "the Job
/// this status names is not there" — is also what a stale watch cache looks
/// like for a Job that was created seconds ago, and declaring a healthy check
/// dead is worse than waiting.
pub const STALLED_GRACE_SECONDS: i64 = 300;

// ---------------------------------------------------------------------------
// Pure: the plan
// ---------------------------------------------------------------------------

/// The `maxTopics` this check actually runs with — D2 §5.2 step 4.
///
/// `min(request, policy.hardMaxTopics)`. **The policy may only LOWER it**: a
/// request is a tenant's number and the ceiling is an administrator's, and an
/// installation that could be talked into a bigger relay by a spec field would
/// have no ceiling at all. The CRD already caps the field at
/// `MAX_TOPICS_CEILING`, so this is the second of two bounds and not the only
/// one.
#[must_use]
pub fn effective_max_topics(requested: i32, hard_max: u32) -> u32 {
    let requested = u32::try_from(requested.max(1)).unwrap_or(hard_max);
    requested.min(hard_max.max(1))
}

/// The connection half of the check plan — references and public settings only.
///
/// **NO CREDENTIAL, AT ANY FIELD.** `password_env` is the NAME of the
/// environment variable the kubelet projects the SASL password into, taken from
/// the same [`crate::connection::Side`] an execution Job uses, and `ca_file` is
/// a path inside the pod. The plan `ConfigMap` is world-readable to anything
/// that can read `ConfigMap`s in the namespace; a plan that carried a password
/// would be a credential in a `ConfigMap` (Global Constraint 6's sibling, and
/// the reason the resolver returns references).
#[must_use]
pub fn connection_plan(resolved: &ResolvedConnection) -> ConnectionPlan {
    let side = resolved.execution.side;
    ConnectionPlan {
        bootstrap_servers: resolved.bootstrap_servers.clone(),
        auth_mode: resolved.auth.mode_str().to_string(),
        username: match &resolved.auth {
            logweir_core::spec::AuthSpec::ScramSha512 { username, .. } => Some(username.clone()),
            logweir_core::spec::AuthSpec::Plaintext => None,
        },
        password_env: resolved
            .password
            .as_ref()
            .map(|_| side.password_env().to_string()),
        tls: Some(resolved.tls()),
        // THE PATH THE RESOLVER'S OWN PROJECTION MOUNTS, and not
        // `/check/source-ca.pem`. D2 §4.3's plan `ConfigMap` can carry a
        // `source-ca.pem` key, and this kind deliberately does not use it: a
        // `KafkaCluster` may name its CA in a **Secret**, which this controller
        // holds no verb on and must never read. Projecting the reference the
        // way an execution Job does works for both source kinds, keeps the
        // controller out of the credential path entirely, and makes the check's
        // answer one about the path the real run takes.
        ca_file: resolved.tls_ca.as_ref().map(|_| side.ca_file_path()),
        principal: resolved.principal.clone(),
    }
}

/// The whole check plan document for one discovery — **pure**.
#[must_use]
pub fn plan_document(
    discovery: &TopicDiscovery,
    resolved: &ResolvedConnection,
    hard_max_topics: u32,
    policy_digest: &str,
    subject_uid: &str,
) -> CheckPlan {
    let request = &discovery.spec.request;
    CheckPlan {
        contract: CHECK_PLAN_CONTRACT.to_string(),
        contract_version: CHECK_CONTRACT_VERSION,
        subject_uid: subject_uid.to_string(),
        timeout_seconds: u32::try_from(request.timeout_seconds.max(1)).unwrap_or(60),
        // THE POLICY THE VERDICT WILL BE COMPUTED UNDER, pinned into the
        // document the runner's digest covers. It is what makes a result say
        // which attestation set was in force when it was taken, rather than
        // which one happens to be in force when it is read.
        policy_digest: Some(policy_digest.to_string()),
        request: CheckRequest::TopicInventory(TopicInventoryRequest {
            connection: connection_plan(resolved),
            include_internal: request.include_internal,
            expected_topics: request.expected_topics.clone().unwrap_or_default(),
            max_topics: effective_max_topics(request.max_topics, hard_max_topics),
            relay_budget_bytes: DEFAULT_RELAY_BUDGET_BYTES as u64,
        }),
    }
}

/// The plan document's bytes — deterministic, so a second pass renders the
/// SAME digest and the 409 rule is a comparison rather than a coin toss.
///
/// # Errors
///
/// [`logweir_core::det_json::DetJsonError`] — unreachable for this shape, named
/// rather than unwrapped.
pub fn plan_bytes(document: &CheckPlan) -> Result<Vec<u8>, logweir_core::det_json::DetJsonError> {
    logweir_core::det_json::to_deterministic_json(document)
}

// ---------------------------------------------------------------------------
// Pure: the binding
// ---------------------------------------------------------------------------

/// What an observation was taken against — D2 §5.1, and the input D2 §5.7's
/// staleness rule compares the CURRENT connection to.
///
/// **Written once, when the Job is created, and never refreshed.** The whole
/// value of this block is that it describes the connection AS IT WAS; a
/// reconciler that re-resolved and overwrote it on every pass would make
/// "the binding changed, so this result is stale" unobservable, because the
/// binding would always match.
#[must_use]
pub fn binding_of(resolved: &ResolvedConnection, policy_digest: &str) -> DiscoveryBinding {
    DiscoveryBinding {
        connection_name: Some(resolved.cluster_name.clone()),
        connection_uid: resolved.uid.clone(),
        connection_generation: resolved.generation,
        principal: Some(resolved.principal.clone()),
        auth_mode: Some(resolved.auth.mode_str().to_string()),
        bootstrap_sha256: Some(resolved.bootstrap_sha256.clone()),
        policy_digest: Some(policy_digest.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Pure: the result
// ---------------------------------------------------------------------------

/// The attestation that could apply to this observation, out of the
/// installation policy — **pure**.
///
/// Narrowed here by namespace and `KafkaCluster` NAME only; the principal, the
/// observed cluster id and the expiry are
/// [`logweir_core::check_contract::visibility`]'s to judge, and it records a
/// mismatch in the basis rather than silently dropping it. Two entries for one
/// (namespace, cluster) is an administrator's mistake and the FIRST is taken,
/// deterministically, because the policy list has a stable order and picking
/// "the best" would let a later entry upgrade a claim an earlier one refused.
#[must_use]
pub fn attestation_for<'a>(
    attestations: &'a [Attestation],
    namespace: &str,
    cluster_name: &str,
) -> Option<&'a Attestation> {
    attestations
        .iter()
        .find(|a| a.namespace == namespace && a.kafka_cluster == cluster_name)
}

/// What the runner OBSERVED, as the visibility policy's input — **pure**.
#[must_use]
pub fn visibility_signals(
    inventory: &InventoryResult,
    namespace: &str,
    cluster_name: &str,
    principal: &str,
) -> VisibilitySignals {
    VisibilitySignals {
        namespace: namespace.to_string(),
        kafka_cluster: cluster_name.to_string(),
        // THE CLUSTER ID THE BROKER ANSWERED WITH, never one cached on a
        // `KafkaCluster` status: an attestation is about the cluster that was
        // actually dialled, and matching it against a remembered id would let a
        // replaced cluster inherit the old one's attestation.
        cluster_id: inventory.cluster_id.clone().unwrap_or_default(),
        principal: principal.to_string(),
        topic_authorization_error_in_listing: inventory.topic_authorization_error_in_listing,
        expected: inventory.expected,
        truncated: inventory.truncated,
    }
}

/// D2 §5.1's status spelling of a truncation reason.
///
/// **TWO SPELLINGS, ON PURPOSE, AND THIS IS THE SEAM BETWEEN THEM.** The wire
/// contract serialises [`TruncationReason`] as `maxTopics` / `relayLimit`
/// (camelCase, like every other field of a JSON document); the CRD's
/// `truncationReason` is a status REASON next to `status.reason`, and every
/// reason in this repository is CamelCase because `metav1.Condition.reason`
/// must be. Mapping once, here, is what stops a third spelling appearing in a
/// UI.
#[must_use]
pub fn truncation_reason_str(reason: TruncationReason) -> &'static str {
    match reason {
        TruncationReason::MaxTopics => "MaxTopics",
        TruncationReason::RelayLimit => "RelayLimit",
    }
}

/// The status rendering of a computed [`Visibility`] — **pure**.
///
/// The basis list is capped at the CRD's `maxItems: 8`. It cannot be reached by
/// any combination the policy produces (the most any single observation can
/// carry is six), and the cap is here anyway because a status patch that
/// violates its own schema is a 422 that loses the WHOLE result, including the
/// counts that were fine.
#[must_use]
pub fn visibility_status(v: &Visibility) -> DiscoveryVisibility {
    DiscoveryVisibility {
        state: v.state.as_str().to_string(),
        basis: Some(
            v.basis
                .iter()
                .take(8)
                .map(|b| basis_str(*b).to_string())
                .collect(),
        ),
        // THE ID, AND NOT THE STATEMENT. The CRD's field is a string, and what
        // a reader needs from a status is which attestation applied; who made
        // it and when live in the policy `ConfigMap` the API can read, beside
        // the statement itself. Copying an administrator's free text into every
        // result would put an unbounded, unredacted string on a status.
        attestation: v.attestation.as_ref().map(|a| a.id.clone()),
    }
}

/// The wire spelling of one basis entry.
///
/// Derived from the enum's own serde rendering so the vocabulary cannot fork:
/// `serde_json::to_value` of a `#[serde(rename_all = "camelCase")]` unit
/// variant is its wire string.
fn basis_str(basis: VisibilityBasis) -> String {
    serde_json::to_value(basis)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "listingOnly".to_string())
}

/// The chunk index a status carries — D2 §5.1, §5.5.
///
/// **THE NAMES DO NOT LIVE IN THE STATUS.** A topic inventory is unbounded and
/// a status is not a store, so this is the names, digests, counts and the first
/// and last topic in each chunk — enough for the API to page and to seek by
/// prefix without fetching, and for a reader to prove the chunk it fetched is
/// the one this status describes.
#[must_use]
pub fn chunk_index(
    entries: &[TopicEntry],
    split: &[chunks::Chunk],
    job_name: &str,
) -> Vec<InventoryChunk> {
    let mut out = Vec::with_capacity(split.len());
    let mut offset = 0usize;
    for chunk in split {
        let slice = &entries[offset..offset + chunk.lines];
        offset += chunk.lines;
        out.push(InventoryChunk {
            name: chunks::chunk_name(job_name, chunk.index),
            sha256: chunk.sha256.clone(),
            count: i64::try_from(chunk.lines).unwrap_or(i64::MAX),
            first_name: slice.first().map(|e| e.name.clone()),
            last_name: slice.last().map(|e| e.name.clone()),
        });
    }
    out
}

/// Why a verified relay still cannot become a result.
///
/// Every variant is [`CheckCode::ResultUnreadable`]: from a consumer's side
/// "the runner's output did not verify" is one fact, and a result document that
/// disagrees with the frames it travelled with is exactly that. The frames are
/// the measured half — the decoder proved their count and digest — so where the
/// two disagree the DOCUMENT is what is wrong.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultRefusal(pub String);

impl ResultRefusal {
    /// The closed code a status carries for this.
    #[must_use]
    pub fn code(&self) -> CheckCode {
        CheckCode::ResultUnreadable
    }
}

impl std::fmt::Display for ResultRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Hold the runner's result document to the frames it arrived with — **pure**.
///
/// Two comparisons, and each one has been a real defect somewhere:
///
/// * `counts.returned` against the number of topic frames. A document that
///   claims 5,003 entries beside 12 frames would produce a status whose counts
///   no chunk supports.
/// * `topicsSha256` against the digest of the frames the decoder already
///   verified. This is what makes the status digest a PROOF rather than a
///   copy: a consumer that fetches the chunks and hashes them gets the value
///   the status published, and neither side took the runner's word for it.
///
/// # Errors
///
/// [`ResultRefusal`], which is [`CheckCode::ResultUnreadable`].
pub fn check_result_against_frames(
    inventory: &InventoryResult,
    entries: &[TopicEntry],
) -> Result<(), ResultRefusal> {
    let framed = u32::try_from(entries.len()).unwrap_or(u32::MAX);
    if inventory.counts.returned != framed {
        return Err(ResultRefusal(format!(
            "the runner's result document claims {} returned entries and {framed} topic frames \
             were relayed; the frames are the measured half and the document is not adopted",
            inventory.counts.returned
        )));
    }
    let measured = topic_tsv_sha256(entries);
    if inventory.topics_sha256 != measured {
        return Err(ResultRefusal(
            "the runner's result document carries a topicsSha256 that is not the digest of the \
             topic frames it relayed; the status publishes only a digest it computed itself"
                .to_string(),
        ));
    }
    Ok(())
}

/// The whole `status.result` block — **pure**.
#[must_use]
pub fn inventory_status(
    inventory: &InventoryResult,
    entries: &[TopicEntry],
    v: &Visibility,
    index: Vec<InventoryChunk>,
) -> TopicInventoryResult {
    TopicInventoryResult {
        format: TOPIC_INVENTORY_FORMAT.to_string(),
        cluster_id: inventory.cluster_id.clone(),
        broker_count: inventory.broker_count.map(i64::from),
        counts: DiscoveryCounts {
            listed: i64::from(inventory.counts.listed),
            returned: i64::from(inventory.counts.returned),
            internal_excluded: i64::from(inventory.counts.internal_excluded),
            errored: i64::from(inventory.counts.errored),
        },
        truncated: inventory.truncated,
        truncation_reason: inventory
            .truncation_reason
            .map(|r| truncation_reason_str(r).to_string()),
        visibility: visibility_status(v),
        // ABSENT WHEN NOTHING WAS EXPECTED, and not a block of zeroes. "Nobody
        // named a topic" and "two were named and both are invisible" are
        // different facts, and a UI that renders `requested: 0` as a row is
        // rendering a question nobody asked.
        expected: (inventory.expected.requested > 0).then_some(ExpectedTopicOutcome {
            requested: i64::from(inventory.expected.requested),
            visible: i64::from(inventory.expected.visible),
            not_authorized: i64::from(inventory.expected.not_authorized),
            not_found: i64::from(inventory.expected.not_found),
            unknown: i64::from(inventory.expected.unknown),
        }),
        // COMPUTED HERE, over the frames, and never copied from the document —
        // see `check_result_against_frames`.
        topics_sha256: Some(topic_tsv_sha256(entries)),
        // AN EMPTY CLUSTER HAS NO CHUNKS AND `counts.returned: 0`, WHICH IS NOT
        // "unknown" (PLAT-09.1's acceptance). `Some(vec![])` and not `None`, so
        // a reader can tell "this observation stored nothing" from "this
        // observation has no result yet".
        chunks: Some(index),
    }
}

// ---------------------------------------------------------------------------
// Pure: freshness, staleness, stall and retention
// ---------------------------------------------------------------------------

/// `observedAt + discovery.freshSeconds` — D2 §5.7's first staleness rule.
///
/// The OTHER two rules (the binding changed, a newer success supersedes this
/// one) are the API's, because both compare this object against something else:
/// `status.binding` against the connection as it is NOW, and this object
/// against its siblings. A controller that computed them would publish a
/// `stale` flag that goes out of date the moment it is written.
#[must_use]
pub fn fresh_until(observed_at: DateTime<Utc>, fresh_seconds: u32) -> DateTime<Utc> {
    observed_at + chrono::Duration::seconds(i64::from(fresh_seconds))
}

/// Whether a non-terminal object whose Job has VANISHED is past hope — D2
/// §4.3's `gc.rs` row.
///
/// The Job's name is a pure function of this object's UID, so "not found" is
/// not a lookup that could have gone to the wrong place: either the Job was
/// collected (its TTL fires only after a status commit, so a non-terminal
/// object should never see that) or it was deleted out from under the check.
/// Either way the relay is gone and no later pass can read it.
///
/// `None` for an object that never created a Job — that is the start path, not
/// a stall.
#[must_use]
pub fn is_stalled(status: Option<&TopicDiscoveryStatus>, timeout_seconds: i32, now: Time) -> bool {
    let Some(status) = status else {
        return false;
    };
    if status.job_ref.is_none() {
        return false;
    }
    let Some(since) = status.started_at.or(status.queued_at) else {
        return false;
    };
    let budget =
        chrono::Duration::seconds(i64::from(timeout_seconds.max(0)) * 2 + STALLED_GRACE_SECONDS);
    now > since + budget
}

/// One terminal discovery, as [`expired_terminal`] reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalDiscovery {
    /// `metadata.name`.
    pub name: String,
    /// `metadata.uid` — the DELETE precondition, not the name.
    pub uid: String,
    /// The connection UID this observation was taken against, when one was
    /// recorded. `None` groups with no other object: a discovery that never
    /// resolved a connection has no cohort to be the sixth of.
    pub connection_uid: Option<String>,
    /// When the observation was taken, or when it failed.
    pub observed_at: Time,
}

/// Which terminal discoveries are collectable — **pure**, D2 §4.3's `gc.rs`
/// and §5.8's retention.
///
/// Two independent rules, and an object matching EITHER is returned:
///
/// * `now > observedAt + retentionSeconds` — the age bound;
/// * outside the newest `keepPerConnection` for the same connection UID — the
///   cohort bound, which is what keeps a namespace that refreshes a connection
///   every minute from filling etcd inside the retention window.
///
/// The order is newest-first by `observedAt` with the UID as the tie-break, so
/// the answer is a total order and two passes over the same input agree.
///
/// **NOTHING CALLS THIS YET.** The `delete` verb it implies is granted nowhere
/// and asserted absent twice in `crates/logweir/tests/manifest_lint.rs`;
/// narrowing that claim is W11's RBAC decision. The rule is here, and tested,
/// so that decision is a grant and a call site rather than a design.
#[must_use]
pub fn expired_terminal(
    discoveries: &[TerminalDiscovery],
    retention_seconds: u32,
    keep_per_connection: u32,
    now: Time,
) -> Vec<String> {
    let mut sorted: Vec<&TerminalDiscovery> = discoveries.iter().collect();
    sorted.sort_by(|a, b| {
        b.observed_at
            .cmp(&a.observed_at)
            .then_with(|| a.uid.cmp(&b.uid))
    });
    let retention = chrono::Duration::seconds(i64::from(retention_seconds));
    let mut seen: std::collections::BTreeMap<String, u32> = std::collections::BTreeMap::new();
    let mut out = Vec::new();
    for d in sorted {
        let too_old = now > d.observed_at + retention;
        let over_cohort = match d.connection_uid.as_ref() {
            Some(uid) => {
                let n = seen.entry(uid.clone()).or_default();
                *n += 1;
                *n > keep_per_connection
            }
            None => false,
        };
        if too_old || over_cohort {
            out.push(d.uid.clone());
        }
    }
    out
}

// ---------------------------------------------------------------------------
// The reconciler's own context
// ---------------------------------------------------------------------------

/// What this reconciler needs, and nothing more.
///
/// ITS OWN CONTEXT AND NOT [`super::Context`], because the two fields below are
/// this kind's alone: an installation-policy cache that would be dead weight on
/// the five reconcilers that never read a policy, and the runner image, which
/// [`super::Context`] also carries but which arrives here beside the policy
/// reference rather than beside an archive handle this file has no use for.
#[derive(Clone)]
pub struct DiscoveryContext {
    /// The client every `Api` in this module is built from.
    pub client: kube::Client,
    /// The image and pull policy every check Job this controller creates will
    /// name — interface **I15**, read once in `main`.
    pub runner_image: crate::job::RunnerImage,
    /// `(namespace, name)` of the installation policy `ConfigMap`, or `None`
    /// when none is configured — which is a supported install and reads as the
    /// documented defaults, never as an error.
    pub policy_ref: Option<(String, String)>,
    /// The 30-second policy cache, so a reconcile does not `get` the
    /// `ConfigMap` on every pass.
    pub policy_cache: Arc<policy::PolicyCache>,
}

/// What one reconcile pass concluded — the value the tests assert over.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outcome {
    /// The phase this pass published, or the one already on the object.
    pub phase: String,
    /// The reason that travelled with it.
    pub reason: String,
    /// Whether this pass created the check Job.
    pub created_job: bool,
    /// How many result chunks this pass wrote or adopted.
    pub chunks_written: usize,
    /// Whether this pass patched the Job's TTL — which happens ONLY after a
    /// status commit returned 200.
    pub ttl_patched: bool,
    /// How long until the next pass.
    pub requeue_seconds: u64,
}

impl Outcome {
    fn new(phase: &str, reason: &str, requeue_seconds: u64) -> Self {
        Self {
            phase: phase.to_string(),
            reason: reason.to_string(),
            created_job: false,
            chunks_written: 0,
            ttl_patched: false,
            requeue_seconds,
        }
    }

    /// Whether the phase this pass published is terminal.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        TERMINAL_PHASES.contains(&self.phase.as_str())
    }
}

// ---------------------------------------------------------------------------
// Status patches
// ---------------------------------------------------------------------------

/// A status carrying one `Complete` condition, merged against what is there.
fn status_with_condition(
    discovery: &TopicDiscovery,
    phase: &str,
    reason: &str,
    message: &str,
    now: Time,
    mut status: TopicDiscoveryStatus,
) -> TopicDiscoveryStatus {
    let previous = discovery.status.as_ref();
    let condition_status = match phase {
        PHASE_SUCCEEDED => "True",
        PHASE_FAILED | PHASE_CANCELLED => "False",
        _ => "Unknown",
    };
    status.phase = Some(phase.to_string());
    status.reason = Some(reason.to_string());
    // REDACTED AND CAPPED AT THE CRD'S 1,024, wherever the string came from.
    // Most of these are this file's own prose, and one — the relay refusal — is
    // derived from a runner's own document. One chokepoint, so the exception
    // cannot be the one that is forgotten.
    status.message = Some(cap_message(&redact(message)));
    status.conditions = Some(vec![merge_condition(
        current_condition(
            previous.and_then(|s| s.conditions.as_ref()),
            CONDITION_COMPLETE,
        ),
        Condition {
            r#type: CONDITION_COMPLETE.to_string(),
            status: condition_status.to_string(),
            observed_generation: discovery.metadata.generation,
            last_transition_time: Some(now),
            reason: Some(reason.to_string()),
            message: Some(cap_message(&redact(message))),
        },
    )]);
    status
}

/// The CRD's `maxLength: 1024` on `status.message`, applied in CHARACTERS and
/// truncated on a character boundary.
///
/// A status patch that violates its own schema is a 422, and a 422 on the
/// commit loses the whole result — including the counts and the chunk index
/// that were perfectly fine.
fn cap_message(message: &str) -> String {
    const MAX: usize = 1024;
    if message.chars().count() <= MAX {
        return message.to_string();
    }
    message.chars().take(MAX - 1).collect::<String>() + "…"
}

/// Add the optimistic-concurrency precondition to a `/status` merge patch —
/// D-SEAMS **S7**.
///
/// THE SAME BODY SHAPE `controllers::backup_schedule` and
/// `controllers::backup_destination` use, and not a third invention: the API
/// server applies a `metadata.resourceVersion` carried in a patch BODY as an
/// update precondition and answers `409 Conflict` on a mismatch, which is how a
/// merge PATCH gets a compare-and-set without the `update` verb this role
/// grants on nothing.
///
/// # Errors
///
/// A [`kube::Error`] shaped as the API server's own "no resourceVersion"
/// answer, for an object that carries none. Unreachable for anything that came
/// from a watch or a `get`; named rather than unwrapped.
fn status_patch_with_preconditions(
    discovery: &TopicDiscovery,
    mut patch: serde_json::Value,
) -> Result<serde_json::Value, Box<kube::Error>> {
    let name = discovery.name_any();
    let resource_version = discovery
        .metadata
        .resource_version
        .clone()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            Box::new(kube::Error::Discovery(
                kube::error::DiscoveryError::MissingResource(format!(
                    "TopicDiscovery {name} carries no metadata.resourceVersion, which a /status \
                     compare-and-set needs (D-SEAMS S7)"
                )),
            ))
        })?;
    patch
        .as_object_mut()
        .expect("a status patch is always a JSON object")
        .insert(
            "metadata".to_string(),
            json!({ "name": name, "resourceVersion": resource_version }),
        );
    Ok(patch)
}

/// What [`write_status`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Commit {
    /// The API server accepted the patch.
    Written,
    /// The computed status equals the one on the object; nothing was sent.
    Unchanged,
    /// Something wrote this status between the read and the write. The object
    /// in hand is stale and the next pass reads the newer one.
    Conflicted,
}

impl Commit {
    /// Whether the status on the server now says what this pass computed —
    /// which is what the TTL patch's ordering depends on.
    fn is_committed(self) -> bool {
        matches!(self, Self::Written | Self::Unchanged)
    }
}

/// Patch `/status`, with the S7 precondition and the no-op skip.
async fn write_status(
    api: &Api<TopicDiscovery>,
    discovery: &TopicDiscovery,
    status: &TopicDiscoveryStatus,
) -> Result<Commit, ReconcileError> {
    let name = discovery.name_any();
    let patch = json!({ "status": status });
    // NO WRITE WHEN NOTHING CHANGED — erratum E11(d). This reconciler's own
    // status patch is what wakes it, and a patch that changed nothing but a
    // clock reading spins the loop at whatever rate the API server will serve.
    // That is the measured `KafkaCluster` defect (3,388 reconciles in ninety
    // seconds), and the reason `observedAt` here is the runner container's
    // `finishedAt` and never `now`.
    if status_unchanged(
        discovery
            .status
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok())
            .as_ref(),
        &patch,
    ) {
        debug!(
            discovery = %name,
            "the computed status equals the one on the object; no patch is sent"
        );
        return Ok(Commit::Unchanged);
    }
    let body =
        status_patch_with_preconditions(discovery, patch).map_err(|e| ReconcileError::Api(*e))?;
    match api
        .patch_status(&name, &PatchParams::default(), &Patch::Merge(body))
        .await
    {
        Ok(_) => Ok(Commit::Written),
        Err(kube::Error::Api(e)) if e.code == 409 => {
            // A 409 IS NOT A FAILURE, IT IS THE PRECONDITION WORKING.
            debug!(
                discovery = %name,
                "the status changed under this reconcile (409); the next pass reads it"
            );
            Ok(Commit::Conflicted)
        }
        Err(e) => Err(ReconcileError::Api(e)),
    }
}

// ---------------------------------------------------------------------------
// The reconcile
// ---------------------------------------------------------------------------

/// Reconcile one `TopicDiscovery`.
///
/// # The order, and why each step is where it is
///
/// 1. **A terminal object is done.** Nothing is read, nothing is patched. This
///    is also what makes the whole loop cheap: a namespace full of finished
///    discoveries costs one guard each.
/// 2. **The Job is looked up before anything else that could create one**, by
///    a name that is a pure function of this object's UID.
/// 3. **A cancel is honoured before the start path.** A `cancelRequested`
///    object never creates a Job, and one that already has a Job has its
///    deadline collapsed by [`crate::check::cancel`] — which verifies the
///    owner UID first, so a foreign Job wearing the same name is never
///    touched.
/// 4. **The start path resolves, then binds, then admits, then plans, then
///    creates.** D2 §5.2's order exactly: a connection that cannot be resolved
///    must not consume a concurrency slot, and a check that is over the ceiling
///    must not leave a plan `ConfigMap` behind.
/// 5. **The observe path proves the pod, then reads, then classifies, then
///    stores, then commits, then sets the TTL.**
///
/// # Errors
///
/// [`ReconcileError`] for anything that is not a verdict — a missing namespace
/// or UID, or an API server that would not answer.
pub async fn reconcile_discovery(
    discovery: &TopicDiscovery,
    ctx: &DiscoveryContext,
) -> Result<Outcome, ReconcileError> {
    let name = discovery.name_any();
    let namespace = discovery
        .namespace()
        .filter(|n| !n.is_empty())
        .ok_or_else(|| ReconcileError::NoNamespace(name.clone()))?;
    let uid = discovery
        .uid()
        .filter(|u| !u.is_empty())
        .ok_or_else(|| ReconcileError::NoUid(name.clone()))?;

    // ONE CLOCK READ for the whole pass, for the reason every reconciler in
    // this directory takes one: two reads could put a verdict and the condition
    // that reports it on either side of the same instant.
    let now = Utc::now();

    // STEP 1. Terminal is done.
    let phase = discovery
        .status
        .as_ref()
        .and_then(|s| s.phase.as_deref())
        .unwrap_or(PHASE_PENDING);
    if TERMINAL_PHASES.contains(&phase) {
        let reason = discovery
            .status
            .as_ref()
            .and_then(|s| s.reason.as_deref())
            .unwrap_or_default()
            .to_string();
        return Ok(Outcome {
            reason,
            ..Outcome::new(phase, "", REQUEUE_TERMINAL_SECS)
        });
    }

    let api: Api<TopicDiscovery> = Api::namespaced(ctx.client.clone(), &namespace);
    let jobs: Api<Job> = Api::namespaced(ctx.client.clone(), &namespace);
    let job_name = cjob::check_job_name(CheckPlanKind::TopicInventory, &uid);
    let job = jobs.get_opt(&job_name).await.map_err(ReconcileError::Api)?;

    // STEP 3. Cancel.
    if discovery.spec.cancel_requested {
        return cancel_now(discovery, ctx, &api, &namespace, &uid, job.as_ref(), now).await;
    }

    // STEP 4/5.
    match job {
        None => {
            // A status that NAMES a Job which is not there is the stall case:
            // the relay lived on that Job's pod and no later pass can read it.
            if discovery
                .status
                .as_ref()
                .is_some_and(|s| s.job_ref.is_some())
            {
                if is_stalled(
                    discovery.status.as_ref(),
                    discovery.spec.request.timeout_seconds,
                    now,
                ) {
                    return terminal(
                        discovery,
                        &api,
                        PHASE_FAILED,
                        CheckCode::Stalled.as_str(),
                        &format!(
                            "the check Job {job_name} this observation was running in no longer \
                             exists and its output can no longer be read; a new TopicDiscovery is \
                             the way to observe this connection again, because spec.request is \
                             immutable"
                        ),
                        TopicDiscoveryStatus::default(),
                        now,
                    )
                    .await;
                }
                // Not yet past hope: a Job created seconds ago can be invisible
                // to a stale cache, and declaring a healthy check dead is worse
                // than waiting.
                return Ok(Outcome::new(
                    phase,
                    CheckCode::PodNotStarted.as_str(),
                    REQUEUE_RUNNING_SECS,
                ));
            }
            start(discovery, ctx, &api, &namespace, &uid, &job_name, now).await
        }
        Some(job) => observe(discovery, ctx, &api, &namespace, &uid, &job_name, &job, now).await,
    }
}

/// D2 §5.8's cancel: a non-terminal object, whatever it was doing.
async fn cancel_now(
    discovery: &TopicDiscovery,
    ctx: &DiscoveryContext,
    api: &Api<TopicDiscovery>,
    namespace: &str,
    uid: &str,
    job: Option<&Job>,
    now: Time,
) -> Result<Outcome, ReconcileError> {
    // THE OWNER UID IS VERIFIED INSIDE `check::cancel`, and a Job that is
    // already finished, or that this object does not control, is not patched at
    // all. A check Job's name is a pure function of a UID, but a name is not an
    // identity.
    if let Some(job) = job {
        check::cancel(&ctx.client, namespace, job, uid)
            .await
            .map_err(ReconcileError::Api)?;
    }
    info!(
        discovery = %discovery.name_any(),
        namespace = %namespace,
        "cancel requested; no result chunks are written and frames that still arrive are \
         discarded"
    );
    terminal(
        discovery,
        api,
        PHASE_CANCELLED,
        CheckCode::CancelRequested.as_str(),
        "the observation was cancelled before it produced a result; no topic inventory was \
         stored, and any frames the runner still prints are discarded",
        TopicDiscoveryStatus::default(),
        now,
    )
    .await
}

/// D2 §5.2 steps 2–4: resolve, bind, admit, plan, create.
async fn start(
    discovery: &TopicDiscovery,
    ctx: &DiscoveryContext,
    api: &Api<TopicDiscovery>,
    namespace: &str,
    uid: &str,
    job_name: &str,
    now: Time,
) -> Result<Outcome, ReconcileError> {
    let connection_name = discovery.spec.request.connection_ref.name.clone();

    // STEP 2. The connection, through PLAT-07.1's resolver.
    let clusters: Api<KafkaCluster> = Api::namespaced(ctx.client.clone(), namespace);
    let Some(cluster) = clusters
        .get_opt(&connection_name)
        .await
        .map_err(ReconcileError::Api)?
    else {
        // TERMINAL, because `spec.request` is immutable: this object can never
        // name a different connection, so a later pass would ask the same
        // question and get the same answer. Creating the KafkaCluster and
        // creating a new TopicDiscovery is the way forward, and saying so is
        // the message's job.
        return terminal(
            discovery,
            api,
            PHASE_FAILED,
            CheckCode::ConnectionNotFound.as_str(),
            &format!(
                "no KafkaCluster named `{connection_name}` exists in namespace {namespace}; \
                 spec.request is immutable, so this observation cannot be repointed — create the \
                 connection and then a new TopicDiscovery"
            ),
            TopicDiscoveryStatus::default(),
            now,
        )
        .await;
    };

    let resolved = match connection::resolve(&cluster, ConnectionUse::Discovery) {
        Ok(resolved) => resolved,
        Err(refusal) => {
            // THE RESOLVER'S REFUSALS ARE THE SAME ONES A BACKUP GETS. A
            // discovery that accepted a connection a run would refuse would be
            // a green light for work that cannot happen (`ConnectionUse` does
            // not change a single refusal, deliberately).
            return terminal(
                discovery,
                api,
                PHASE_FAILED,
                CheckCode::ConnectionInvalid.as_str(),
                &format!(
                    "KafkaCluster {connection_name} cannot be used for a discovery check: {} \
                     ({})",
                    refusal.message, refusal.field
                ),
                TopicDiscoveryStatus::default(),
                now,
            )
            .await;
        }
    };

    // The installation policy. An ABSENT policy is the documented defaults and
    // not an error; an UNREADABLE one fails closed — empty attestations, so no
    // result taken under it can reach `attestedComplete`.
    let load = policy::load(&ctx.client, ctx.policy_ref.as_ref(), &ctx.policy_cache, now)
        .await
        .map_err(ReconcileError::Api)?;
    let policy_digest = load.policy().digest();
    let binding = binding_of(&resolved, &policy_digest);

    // STEP 3. The concurrency ceilings, counted from Jobs.
    let existing = limits::check_jobs(&ctx.client)
        .await
        .map_err(ReconcileError::Api)?;
    let counts = limits::count(&existing, namespace, resolved.uid.as_deref());
    if let limits::Admission::Queued(code) = limits::admit(
        &counts,
        &load.policy().checks,
        CheckPlanKind::TopicInventory,
    ) {
        // QUEUED IS NOT A FAILURE AND NOT TERMINAL. The binding travels with it
        // so an operator can see which connection is waiting, and the requeue
        // is the framework's own ten seconds.
        let status = status_with_condition(
            discovery,
            PHASE_QUEUED,
            code.as_str(),
            &format!(
                "this installation is at a check concurrency ceiling ({} active in this \
                 namespace, {} in total, {} against this connection); the observation is queued \
                 and retried",
                counts.namespace, counts.total, counts.per_connection
            ),
            now,
            TopicDiscoveryStatus {
                binding: Some(binding),
                ..TopicDiscoveryStatus::default()
            },
        );
        write_status(api, discovery, &status).await?;
        return Ok(Outcome::new(
            PHASE_QUEUED,
            code.as_str(),
            limits::QUEUED_REQUEUE_SECS,
        ));
    }

    // STEP 4. The plan, then the Job. In that order, because a Job whose plan
    // `ConfigMap` does not exist sits in `ContainerCreating` until its deadline
    // and reports nothing useful.
    let owner = owner_of(&discovery.name_any(), uid);
    let document = plan_document(
        discovery,
        &resolved,
        load.policy().discovery.hard_max_topics,
        &policy_digest,
        uid,
    );
    let bytes = plan_bytes(&document).map_err(|e| {
        ReconcileError::Api(kube::Error::Discovery(
            kube::error::DiscoveryError::MissingResource(format!(
                "the check plan for TopicDiscovery {} could not be serialised: {e}",
                discovery.name_any()
            )),
        ))
    })?;
    let documents = plan::PlanDocuments {
        check_plan: bytes,
        // NO `source-ca.pem`. See `connection_plan`: the CA reaches the pod
        // through the resolver's own projection, because a `KafkaCluster` may
        // name it in a Secret and this controller holds no verb on Secrets.
        ..plan::PlanDocuments::default()
    };
    let digest = documents.check_plan_sha256();
    let config_map = match plan::build(job_name, namespace, &owner, &documents) {
        Ok(config_map) => config_map,
        Err(e) => {
            return terminal(
                discovery,
                api,
                PHASE_FAILED,
                e.code().as_str(),
                &format!("the check plan could not be rendered: {e}"),
                TopicDiscoveryStatus {
                    binding: Some(binding),
                    ..TopicDiscoveryStatus::default()
                },
                now,
            )
            .await;
        }
    };
    match plan::ensure(&ctx.client, namespace, &config_map, uid, &digest).await {
        Ok(_) => {}
        Err(plan::EnsureError::Api(e)) => return Err(ReconcileError::Api(e)),
        Err(plan::EnsureError::Plan(e)) => {
            return terminal(
                discovery,
                api,
                PHASE_FAILED,
                e.code().as_str(),
                &format!("{e}"),
                TopicDiscoveryStatus {
                    binding: Some(binding),
                    ..TopicDiscoveryStatus::default()
                },
                now,
            )
            .await;
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
        timeout_seconds: i64::from(discovery.spec.request.timeout_seconds),
        service_account_name: ConnectionUse::Discovery.service_account_name().to_string(),
        secret_mounts: projection.secret_mounts,
        config_map_mounts: projection.config_map_mounts,
        env_from_secret: projection.env_from_secret,
        env_literal: projection.env_literal,
        image: ctx.runner_image.image.clone(),
        image_pull_policy: ctx.runner_image.image_pull_policy.clone(),
    };
    match check::create_job(&ctx.client, &spec).await {
        Ok(_) => {}
        // A 409 IS THE DUPLICATE-RECONCILE CASE AND IT IS HEALTHY. The Job's
        // name is a pure function of this object's UID, so a 409 means "the
        // check this pass wanted already exists"; the next pass observes it.
        Err(kube::Error::Api(e)) if e.code == 409 => {
            debug!(
                discovery = %discovery.name_any(),
                job = %job_name,
                "the check Job already exists; this pass adopts it"
            );
        }
        Err(e) => return Err(ReconcileError::Api(e)),
    }

    info!(
        discovery = %discovery.name_any(),
        namespace = %namespace,
        job = %job_name,
        connection = %connection_name,
        principal = %resolved.principal,
        max_topics = effective_max_topics(
            discovery.spec.request.max_topics,
            load.policy().discovery.hard_max_topics
        ),
        "created the topic discovery check Job; this controller never dials a broker itself and \
         never reads a Secret, which is why a discovery is a Job"
    );

    let status = status_with_condition(
        discovery,
        PHASE_RUNNING,
        CheckCode::PodNotStarted.as_str(),
        "the discovery check Job was created and has not finished",
        now,
        TopicDiscoveryStatus {
            binding: Some(binding),
            job_ref: Some(crate::crds::LocalRef {
                name: job_name.to_string(),
            }),
            queued_at: Some(now),
            ..TopicDiscoveryStatus::default()
        },
    );
    write_status(api, discovery, &status).await?;
    Ok(Outcome {
        created_job: true,
        ..Outcome::new(
            PHASE_RUNNING,
            CheckCode::PodNotStarted.as_str(),
            REQUEUE_RUNNING_SECS,
        )
    })
}

/// D2 §5.2 steps 5–6: prove the pod, read the relay, store, commit, TTL.
#[allow(clippy::too_many_arguments)]
async fn observe(
    discovery: &TopicDiscovery,
    ctx: &DiscoveryContext,
    api: &Api<TopicDiscovery>,
    namespace: &str,
    uid: &str,
    job_name: &str,
    job: &Job,
    now: Time,
) -> Result<Outcome, ReconcileError> {
    // D-SEAMS S6. The `batch.kubernetes.io/job-name` label narrows the list;
    // the controller `ownerReference` UID decides. A pod that wears the label
    // and is owned by something else is IGNORED and logged, never read.
    //
    // WHY THIS IS HERE AND NOT `check::observe`. That function does exactly
    // this and then throws the pod away, and D2 §5.1 takes `observedAt` and
    // `startedAt` from the RUNNER CONTAINER'S OWN terminated/running state —
    // the `KafkaCluster` probe's rule, and the reason this status is stable
    // across passes instead of spinning the loop with a fresh `now`. So the
    // proven pod is kept, and the log is read off THAT pod and no other.
    let pod = check::pod::find_owned_pod(&ctx.client, namespace, job)
        .await
        .map_err(ReconcileError::Api)?;
    let finished = super::backup::job_finished(job);

    let log = match (finished, pod.as_ref()) {
        (true, Some(p)) => {
            let pods: Api<Pod> = Api::namespaced(ctx.client.clone(), namespace);
            match pods.logs(&p.name_any(), &relay::log_params()).await {
                Ok(log) => Some(log),
                // THERE IS NO LOG, AND THAT IS AN ANSWER. A finished Job whose
                // pod never started its `runner` container has no stdout, and
                // the API server says so with 400 or 404. Everything else —
                // 401, 403, 429, 5xx, transport — is a reason to requeue,
                // because a verdict published from a control-plane failure is a
                // claim about somebody's cluster that nothing measured.
                Err(e) if check::is_log_absent(&e) => None,
                Err(e) => return Err(ReconcileError::Api(e)),
            }
        }
        _ => None,
    };

    // The digest the RUNNER was actually pinned to, read off this object's own
    // plan `ConfigMap` after its owner UID, digest annotation and immutability
    // have been checked. Re-rendering the plan here instead would compare the
    // relay against a document written under whatever policy is in force NOW,
    // and an administrator editing the policy mid-run would turn a good result
    // into `ResultUnreadable`.
    let expect = expectations_for(&ctx.client, namespace, job_name, uid).await?;
    let expect = match expect {
        Ok(expect) => expect,
        Err(refusal) => {
            return terminal(
                discovery,
                api,
                PHASE_FAILED,
                refusal.code().as_str(),
                &format!("{refusal}"),
                TopicDiscoveryStatus::default(),
                now,
            )
            .await;
        }
    };

    let observation = check::classify(&check::Input {
        job,
        pod: pod.as_ref(),
        // EMPTY, AND SAID SO IN THE MODULE HEADER: `events: list` is W11's
        // grant, and `manifest_lint::every_call_site_has_a_grant` fails the
        // moment an `Api<Event>` appears here without it.
        events: &[],
        log: log.as_deref(),
        expect: &expect,
        now,
    });

    let started_at = container_started_at(pod.as_ref());
    let observed_at = super::kafka_cluster::observed_at(job, pod.as_ref(), now);

    match observation.phase {
        CheckPhase::Running => {
            let status = status_with_condition(
                discovery,
                PHASE_RUNNING,
                observation.reason.as_str(),
                &observation.message,
                now,
                TopicDiscoveryStatus {
                    started_at,
                    ..TopicDiscoveryStatus::default()
                },
            );
            write_status(api, discovery, &status).await?;
            Ok(Outcome::new(
                PHASE_RUNNING,
                observation.reason.as_str(),
                REQUEUE_RUNNING_SECS,
            ))
        }
        CheckPhase::Failed => {
            // EARLY CANCEL. A terminal waiting state cannot succeed and its
            // `activeDeadlineSeconds` is minutes away; collapsing the deadline
            // now is the difference between reading
            // `CredentialSecretNotFound` and watching a spinner.
            if observation.cancel_now {
                check::cancel(&ctx.client, namespace, job, uid)
                    .await
                    .map_err(ReconcileError::Api)?;
            }
            let outcome = terminal(
                discovery,
                api,
                PHASE_FAILED,
                observation.reason.as_str(),
                &observation.message,
                TopicDiscoveryStatus {
                    started_at,
                    observed_at: finished.then_some(observed_at),
                    ..TopicDiscoveryStatus::default()
                },
                now,
            )
            .await?;
            finish_job(ctx, namespace, job_name, finished, outcome).await
        }
        CheckPhase::Succeeded => {
            commit(
                discovery,
                ctx,
                api,
                namespace,
                uid,
                job_name,
                &observation,
                started_at,
                observed_at,
                now,
            )
            .await
        }
    }
}

/// D2 §5.2 step 6: decode, chunk, write, compute visibility, COMMIT, TTL.
#[allow(clippy::too_many_arguments)]
async fn commit(
    discovery: &TopicDiscovery,
    ctx: &DiscoveryContext,
    api: &Api<TopicDiscovery>,
    namespace: &str,
    uid: &str,
    job_name: &str,
    observation: &check::Observation,
    started_at: Option<Time>,
    observed_at: Time,
    now: Time,
) -> Result<Outcome, ReconcileError> {
    let relay = observation
        .relay
        .as_ref()
        .expect("check::classify returns a relay with every Succeeded phase");

    // The result DOCUMENT, parsed, bounds-checked and redacted by the pure
    // crate. `None` or a parse failure is `ResultUnreadable`: a topic inventory
    // whose counts nobody wrote is not a result.
    let inventory = match relay.result() {
        Some(Ok(result)) => result.inventory,
        Some(Err(e)) => {
            return terminal(
                discovery,
                api,
                PHASE_FAILED,
                e.code().as_str(),
                &format!("the check Job's result document did not verify: {e}"),
                TopicDiscoveryStatus {
                    started_at,
                    observed_at: Some(observed_at),
                    ..TopicDiscoveryStatus::default()
                },
                now,
            )
            .await;
        }
        None => None,
    };
    let Some(inventory) = inventory else {
        return terminal(
            discovery,
            api,
            PHASE_FAILED,
            CheckCode::ResultUnreadable.as_str(),
            "the check Job relayed a verified result that carries no topic inventory; a \
             topicInventory check that produced no inventory has nothing to store",
            TopicDiscoveryStatus {
                started_at,
                observed_at: Some(observed_at),
                ..TopicDiscoveryStatus::default()
            },
            now,
        )
        .await;
    };
    if let Err(refusal) = check_result_against_frames(&inventory, &relay.topics) {
        return terminal(
            discovery,
            api,
            PHASE_FAILED,
            refusal.code().as_str(),
            &format!("{refusal}"),
            TopicDiscoveryStatus {
                started_at,
                observed_at: Some(observed_at),
                ..TopicDiscoveryStatus::default()
            },
            now,
        )
        .await;
    }

    // THE CHUNKS, FIRST. D2 §5.5's two bounds together — at most 2,500 entries
    // AND at most 768 KiB per chunk — are `chunks::split`'s, so this file never
    // decides a size.
    let split = chunks::split(&relay.topics);
    let owner = owner_of(&discovery.name_any(), uid);
    let objects: Vec<ConfigMap> = split
        .iter()
        .map(|c| chunks::build_chunk(job_name, namespace, &owner, c, split.len()))
        .collect();
    let written = match chunks::write_all(&ctx.client, namespace, uid, &objects).await {
        Ok(written) => written,
        Err(chunks::WriteError::Api(e)) => return Err(ReconcileError::Api(e)),
        Err(chunks::WriteError::Conflict(e)) => {
            // TERMINAL. Two different inventories want one name: retrying
            // cannot fix it, and adopting the other one would publish a digest
            // over bytes this pass never produced.
            return terminal(
                discovery,
                api,
                PHASE_FAILED,
                e.code().as_str(),
                &format!("{e}"),
                TopicDiscoveryStatus {
                    started_at,
                    observed_at: Some(observed_at),
                    ..TopicDiscoveryStatus::default()
                },
                now,
            )
            .await;
        }
    };

    // The completeness verdict — D-SEAMS S3, and the ONE place an attestation
    // is consulted. An UNREADABLE policy fails closed: `Policy::fail_closed`
    // carries no attestations at all, so nothing taken under it can be
    // `attestedComplete`.
    let load = policy::load(&ctx.client, ctx.policy_ref.as_ref(), &ctx.policy_cache, now)
        .await
        .map_err(ReconcileError::Api)?;
    let binding = discovery.status.as_ref().and_then(|s| s.binding.as_ref());
    let cluster_name = binding
        .and_then(|b| b.connection_name.clone())
        .unwrap_or_else(|| discovery.spec.request.connection_ref.name.clone());
    let principal = binding
        .and_then(|b| b.principal.clone())
        .unwrap_or_default();
    let signals = visibility_signals(&inventory, namespace, &cluster_name, &principal);
    let attestation = attestation_for(
        &load.policy().discovery.visibility_attestations,
        namespace,
        &cluster_name,
    );
    let verdict = visibility(&signals, attestation, now);

    let index = chunk_index(&relay.topics, &split, job_name);
    let result = inventory_status(&inventory, &relay.topics, &verdict, index);
    let returned = result.counts.returned;
    let visibility_state = result.visibility.state.clone();

    let status = status_with_condition(
        discovery,
        PHASE_SUCCEEDED,
        CheckCode::Succeeded.as_str(),
        &format!(
            "observed {returned} visible topics at {observed_at}; completeness is \
             `{visibility_state}` — Kafka omits topics this principal cannot describe, so a \
             successful listing alone is never proof that the cluster holds no others"
        ),
        now,
        TopicDiscoveryStatus {
            started_at,
            observed_at: Some(observed_at),
            fresh_until: Some(fresh_until(
                observed_at,
                load.policy().discovery.fresh_seconds,
            )),
            result: Some(result),
            ..TopicDiscoveryStatus::default()
        },
    );

    // THE COMMIT. Every chunk exists before this line; nothing before it is
    // visible to a reader of the custom resource.
    let commit = write_status(api, discovery, &status).await?;

    info!(
        discovery = %discovery.name_any(),
        namespace = %namespace,
        job = %job_name,
        topics = returned,
        chunks = written.len(),
        visibility = %visibility_state,
        observed_at = %observed_at,
        "topic discovery committed"
    );

    let outcome = Outcome {
        chunks_written: written.len(),
        ..Outcome::new(
            PHASE_SUCCEEDED,
            CheckCode::Succeeded.as_str(),
            REQUEUE_TERMINAL_SECS,
        )
    };
    if commit.is_committed() {
        finish_job(ctx, namespace, job_name, true, outcome).await
    } else {
        // The status changed under this pass. NO TTL: the relay lives on the
        // pod, and a TTL set before a commit lets garbage collection race the
        // read the next pass still has to make.
        Ok(outcome)
    }
}

/// Patch the finished Job's TTL — **only ever after a status commit**.
///
/// The TTL controller deletes a Job and its pods together and the relay lives
/// on the pod, so a TTL patched before the commit lets garbage collection race
/// the log read. The ordering is a guarantee because the status write is
/// `?`-propagated: a patch that did not return 200 leaves the reconcile before
/// this line.
async fn finish_job(
    ctx: &DiscoveryContext,
    namespace: &str,
    job_name: &str,
    finished: bool,
    outcome: Outcome,
) -> Result<Outcome, ReconcileError> {
    if !finished {
        // An unfinished Job has no TTL to set — `ttlSecondsAfterFinished` on a
        // running Job is legal and would fire the moment it finishes, which is
        // the race this whole ordering exists to avoid.
        return Ok(outcome);
    }
    check::set_ttl(&ctx.client, namespace, job_name)
        .await
        .map_err(ReconcileError::Api)?;
    Ok(Outcome {
        ttl_patched: true,
        ..outcome
    })
}

/// Write a terminal status and return the outcome it describes.
async fn terminal(
    discovery: &TopicDiscovery,
    api: &Api<TopicDiscovery>,
    phase: &str,
    reason: &str,
    message: &str,
    status: TopicDiscoveryStatus,
    now: Time,
) -> Result<Outcome, ReconcileError> {
    let status = status_with_condition(discovery, phase, reason, message, now, status);
    write_status(api, discovery, &status).await?;
    warn!(
        discovery = %discovery.name_any(),
        phase = phase,
        reason = reason,
        "topic discovery reached a terminal state"
    );
    Ok(Outcome::new(phase, reason, REQUEUE_TERMINAL_SECS))
}

/// The owner reference every object this reconciler creates carries.
///
/// From `kube::Resource`'s own `api_version`/`kind` and never two string
/// literals, exactly as `controllers::backup` builds its own: a hand-written
/// `apiVersion` that drifts from the CRD makes the garbage collector refuse to
/// resolve the owner, and an unresolvable owner is a cascade that silently does
/// not happen.
#[must_use]
pub fn owner_of(name: &str, uid: &str) -> RunnerOwner {
    use kube::Resource as _;
    RunnerOwner {
        api_version: TopicDiscovery::api_version(&()).to_string(),
        kind: TopicDiscovery::kind(&()).to_string(),
        name: name.to_string(),
        uid: uid.to_string(),
    }
}

/// The runner container's start instant, from the pod this Job owns.
fn container_started_at(pod: Option<&Pod>) -> Option<Time> {
    let status = pod?.status.as_ref()?;
    let container = status
        .container_statuses
        .as_ref()?
        .iter()
        .find(|c| c.name == crate::job::CONTAINER_NAME)?;
    let state = container.state.as_ref()?;
    state
        .running
        .as_ref()
        .and_then(|r| r.started_at.as_ref())
        .or_else(|| {
            state
                .terminated
                .as_ref()
                .and_then(|t| t.started_at.as_ref())
        })
        .map(|t| t.0)
}

/// The frame expectations for this check, taken from its OWN plan `ConfigMap`.
///
/// The outer `Result` is a transport failure (requeue); the inner one is a
/// terminal [`plan::PlanError`], which is
/// [`CheckCode::CheckPlanConflict`] — a plan object that is missing, not owned
/// by this subject, mutable, or carrying no digest is not this check's plan,
/// and a relay verified against a digest from such an object would be verified
/// against nothing.
async fn expectations_for(
    client: &kube::Client,
    namespace: &str,
    job_name: &str,
    uid: &str,
) -> Result<Result<FrameExpectations, plan::PlanError>, ReconcileError> {
    let maps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    let plan_name = plan::plan_config_map_name(job_name);
    let Some(existing) = maps
        .get_opt(&plan_name)
        .await
        .map_err(ReconcileError::Api)?
    else {
        return Ok(Err(plan::PlanError::Conflict(format!(
            "the check plan ConfigMap {plan_name} this Job mounts no longer exists, so the \
             digest its relay must carry cannot be established"
        ))));
    };
    let Some(digest) = existing
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(plan::DIGEST_ANNOTATION))
        .cloned()
    else {
        return Ok(Err(plan::PlanError::Conflict(format!(
            "the check plan ConfigMap {plan_name} carries no {} annotation",
            plan::DIGEST_ANNOTATION
        ))));
    };
    // THE THREE-PART RULE IS THE FRAMEWORK'S — owner UID, digest, immutability.
    // Only "which digest" is this file's, and it is read from the object the
    // rule has just proved belongs to this subject.
    if let Err(e) = plan::accepts_existing(&existing, uid, &digest) {
        return Ok(Err(e));
    }
    Ok(Ok(FrameExpectations {
        plan_sha256: digest,
        subject_uid: uid.to_string(),
    }))
}

// ---------------------------------------------------------------------------
// The kube-runtime wiring
// ---------------------------------------------------------------------------

/// The `kube::runtime` reconcile entry point.
async fn reconcile(
    discovery: Arc<TopicDiscovery>,
    ctx: Arc<DiscoveryContext>,
) -> Result<Action, ReconcileError> {
    let outcome = reconcile_discovery(&discovery, &ctx).await?;
    Ok(Action::requeue(std::time::Duration::from_secs(
        outcome.requeue_seconds,
    )))
}

/// Requeue on an error, naming it.
fn error_policy(
    discovery: Arc<TopicDiscovery>,
    err: &ReconcileError,
    _ctx: Arc<DiscoveryContext>,
) -> Action {
    warn!(
        discovery = %discovery.name_any(),
        error = %err,
        "topic discovery reconcile failed; requeueing"
    );
    Action::requeue(std::time::Duration::from_secs(ERROR_REQUEUE_SECONDS))
}

/// `(namespace, name)` of the installation policy `ConfigMap` this process
/// reads, out of the environment — D2 §4.4.
///
/// # Why the two reads are HERE and not in `main`
///
/// `main.rs` is a registration point: D2 §13.4 gives W7, W8 and W9 ONE
/// `controllers.push(…)` line each, and three env reads threaded through it
/// would make each of those a rewrite of the same function. The DECISION is
/// still a pure, tested predicate — [`policy::configured_ref`], which
/// `check_framework.rs` exercises over the empty string a Kubernetes `env:`
/// entry with an empty `value:` actually produces (erratum **E19(e)**) — and
/// this function is only the read plus one log line. `None` means no policy is
/// configured, which is a supported install and reads as the documented
/// defaults, never as an error.
#[must_use]
pub fn configured_policy_ref() -> Option<(String, String)> {
    let reference = policy::configured_ref(
        std::env::var(policy::POLICY_CONFIGMAP_ENV).ok().as_deref(),
        std::env::var(policy::INSTALLATION_NAMESPACE_ENV)
            .ok()
            .as_deref(),
    );
    match reference.as_ref() {
        Some((namespace, name)) => info!(
            policy_namespace = %namespace,
            policy_name = %name,
            "the installation policy every check this controller runs is bound to"
        ),
        None => info!(
            env = policy::POLICY_CONFIGMAP_ENV,
            "no installation policy is configured: the documented defaults apply and no \
             administrator attestation exists, so no discovery can report attestedComplete"
        ),
    }
    reference
}

/// Run the `TopicDiscovery` controller until the process ends.
///
/// ALL NAMESPACES (`Api::all`), like every other controller in this directory,
/// and `.owns(jobs, …)` so a check Job that finishes wakes its own discovery
/// rather than waiting out the requeue.
pub fn controller(
    client: kube::Client,
    runner_image: crate::job::RunnerImage,
    policy_ref: Option<(String, String)>,
) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<TopicDiscovery> = Api::all(client.clone());
    let jobs: Api<Job> = Api::all(client.clone());
    let ctx = Arc::new(DiscoveryContext {
        client,
        runner_image,
        policy_ref,
        policy_cache: Arc::new(policy::PolicyCache::new()),
    });
    async move {
        Controller::new(api, watcher::Config::default())
            .owns(jobs, watcher::Config::default())
            .run(reconcile, error_policy, ctx)
            .for_each(|_| std::future::ready(()))
            .await;
    }
}
