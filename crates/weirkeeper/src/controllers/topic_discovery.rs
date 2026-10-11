//! The `TopicDiscovery` reconciler (D2 §5).
//!
//! # One object, one observation, one Job
//!
//! A `TopicDiscovery` is a request with an immutable `spec.request` (CEL rule
//! T1), so this reconciler never re-plans: it resolves the connection once,
//! renders one immutable plan `ConfigMap`, creates one isolated check Job with
//! no Kubernetes token, reads that Job's framed stdout through the shared
//! framework, stores the inventory as owned immutable `ConfigMap` chunks, and
//! commits one status. A refresh is a new object, so the old result stays
//! readable while the new one runs and no result is rewritten in place.
//!
//! # What this file does not decide
//!
//! 1. **Completeness.** `unknown | limited | attestedComplete` is
//!    [`logweir_core::check_contract::visibility`]'s (seam **S3**): a successful
//!    listing is `unknown`, `limited` needs an observed authorization failure,
//!    and `attestedComplete` needs an administrator attestation from the
//!    installation policy `ConfigMap`. This file supplies signals and a clock
//!    reading, and copies the verdict.
//! 2. **Which pod may be read.** [`crate::check::pod::find_owned_pod`] proves
//!    the pod by its controller `ownerReference` UID (seam **S6**); the
//!    `job-name` label only narrows the list, since anything that can create a
//!    pod can write it.
//! 3. **What a relay means.** [`crate::check::classify`] is the state machine;
//!    this file turns its [`crate::check::Observation`] into a phase and a
//!    condition.
//!
//! # The result is advisory, never an execution input (seam **S2**)
//!
//! Nothing reads a `TopicDiscovery` result to decide what a run captures: a
//! dynamic run discovers afresh in its own Job and freezes the names in its own
//! snapshot (D2 §5.11).
//!
//! # The commit point
//!
//! Chunks are written first and the status patch that indexes them is the
//! commit; the Job's TTL is patched only after that patch returned. A restart
//! before the commit re-reads the relay (the Job is still there, with no TTL)
//! and re-writes byte-identical chunks, which
//! [`crate::check::chunks::accepts_existing`] accepts. A restart after the
//! commit finds a terminal object and stops.
//!
//! # Deletes and Events
//!
//! * **It deletes only its own kind.** [`collect_expired`] runs on the terminal
//!   guard and deletes, by UID precondition, up to [`GC_MAX_DELETES_PER_PASS`]
//!   discoveries that [`expired_terminal`] finds past
//!   `observedAt + retentionSeconds` or outside the newest `keepPerConnection`
//!   for their connection. The owned `ConfigMap`s and the check Job go by owner
//!   cascade. The grant is `charts/logweir/templates/clusterrole.yaml`'s one
//!   `delete` rule, on `topicdiscoveries` and `preflights` only.
//! * **It reads Events only to explain a pod that has not started**, through
//!   [`crate::check::job_events`] by `involvedObject.uid`, so a pod refused at
//!   creation ends `PodCreateRejected` with the admission's own words 30 seconds
//!   after the Job, and the Job is cancelled. A failed list is an empty one: the
//!   classification stays the more general state.

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
    redact, topic_tsv_sha256, visibility, Attestation, CheckCode, CheckOutcome, CheckPlan,
    CheckPlanKind, CheckRequest, CheckState, ConnectionPlan, FrameExpectations, Gating,
    InventoryResult, TopicEntry, TopicInventoryRequest, TruncationReason, Visibility,
    VisibilityBasis, VisibilitySignals, CHECK_CONTRACT_VERSION, CHECK_PLAN_CONTRACT,
    DEFAULT_RELAY_BUDGET_BYTES, TOPIC_INVENTORY_FORMAT,
};

use super::approval::ReconcileError;
use crate::check::{self, chunks, job as cjob, limits, plan, policy, CheckPhase};
use crate::conditions::{current_condition, merge_condition, status_unchanged};
use crate::connection::{self, ConnectionUse, ResolvedConnection};
use crate::crds::kafka_cluster::KafkaCluster;
use crate::crds::topic_discovery::{
    DiscoveryBinding, DiscoveryCounts, DiscoveryVisibility, ExpectedTopicOutcome, InventoryChunk,
    TopicDiscovery, TopicDiscoveryStatus, TopicInventoryResult,
};
use crate::crds::{Condition, Time};
use crate::job::RunnerOwner;
use crate::runner_log;

/// The one condition type this kind carries (D2 §5.1).
pub const CONDITION_COMPLETE: &str = "Complete";

/// `status.phase` before the controller has written anything. Never written by
/// this file: an object with no status reads as `Pending`, and the first patch
/// is already `Queued`, `Running` or terminal.
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

/// How long a running check waits before it is looked at again. The Job is also
/// watched, so this is the backstop for pod-status transitions
/// (`ImagePullBackOff`, `Unschedulable`) that change no Job field.
pub const REQUEUE_RUNNING_SECS: u64 = 10;

/// How long a terminal object waits before it is looked at again: one hour,
/// which paces the GC ([`collect_expired`]).
pub const REQUEUE_TERMINAL_SECS: u64 = 3600;

/// The most objects one garbage-collection pass may delete (D2 §4.3): a bound on
/// the blast radius and on the API server, not on the total. A backlog drains
/// over passes an hour apart, and a rule gone wrong removes twenty objects
/// before anybody has to notice.
pub const GC_MAX_DELETES_PER_PASS: usize = 20;

/// The page size one garbage-collection pass lists.
///
/// The API server returns items in name order, so a partial page is not the
/// newest anything, and the cohort rule over a truncated listing could delete a
/// namespace's newest observation. A truncated pass applies only the per-object
/// age rule, and logs it.
pub const GC_LIST_LIMIT: u32 = 500;

/// How long a reconcile that ERRORED waits.
pub const ERROR_REQUEUE_SECONDS: u64 = 30;

/// Seconds added to `2 × timeoutSeconds` before a non-terminal object whose Job
/// has vanished is declared `Stalled` (D2 §4.3). Generous, because "the Job is
/// not there" is also what a stale watch cache shows for a Job created seconds
/// ago, and declaring a healthy check dead is worse than waiting.
pub const STALLED_GRACE_SECONDS: i64 = 300;

/// The CRD's `maxItems` on `status.result.chunks`: a hard bound, because a
/// status patch that violates its schema is a 422, the object never reaches a
/// terminal phase, and the same doomed pass requeues forever with its chunk
/// `ConfigMap`s orphaned.
pub const MAX_STATUS_CHUNKS: usize = 64;

/// The most inventory entries this controller will store for one observation.
/// The lower of two ceilings wins:
///
/// * **The plan's own `maxTopics`.** A surplus relayed past the plan is not a
///   result this controller may publish as if it had been asked for.
/// * **`MAX_STATUS_CHUNKS × MAX_CHUNK_LINES`** (160,000 entries), the schema
///   backstop, reached only if a policy raised `hardMaxTopics` above it.
///
/// A line is at most ~308 bytes and a chunk holds 2,500 of them (~752 KiB), so
/// [`chunks::MAX_CHUNK_LINES`] binds before [`chunks::MAX_CHUNK_BYTES`] for
/// every legal Kafka name, which is what makes an entry count a chunk count.
#[must_use]
pub fn storable_entry_ceiling(max_topics: u32) -> usize {
    (max_topics as usize).min(MAX_STATUS_CHUNKS * chunks::MAX_CHUNK_LINES)
}

/// Cut a relayed inventory down to what may be stored.
///
/// The entries are cut, not the index, so the stored slice, the chunks, the
/// index and `topicsSha256` are one set and the API's §5.6 integrity triple
/// holds. The reason is [`TruncationReason::MaxTopics`] unless the runner
/// supplied one (`RelayLimit` is the more specific fact).
#[must_use]
pub fn clamp_to_storable<'a>(
    inventory: &InventoryResult,
    entries: &'a [TopicEntry],
    ceiling: usize,
) -> (InventoryResult, &'a [TopicEntry]) {
    if entries.len() <= ceiling {
        return (inventory.clone(), entries);
    }
    let stored = &entries[..ceiling];
    let mut out = inventory.clone();
    out.counts.returned = u32::try_from(stored.len()).unwrap_or(u32::MAX);
    out.truncated = true;
    out.truncation_reason = Some(
        inventory
            .truncation_reason
            .unwrap_or(TruncationReason::MaxTopics),
    );
    out.topics_sha256 = topic_tsv_sha256(stored);
    (out, stored)
}

/// The `maxTopics` this check actually runs with (D2 §5.2 step 4):
/// `min(request, policy.hardMaxTopics)`. The policy may only lower it: a
/// request is a tenant's number and the ceiling an administrator's.
#[must_use]
pub fn effective_max_topics(requested: i32, hard_max: u32) -> u32 {
    let requested = u32::try_from(requested.max(1)).unwrap_or(hard_max);
    requested.min(hard_max.max(1))
}

/// The connection half of the check plan: references and public settings only.
///
/// `password_env` is the name of the variable the kubelet projects the SASL
/// password into (the same [`crate::connection::Side`] an execution Job uses)
/// and `ca_file` is a path inside the pod. The plan `ConfigMap` is readable by
/// anything that can read `ConfigMap`s in the namespace, so it carries no
/// credential.
#[must_use]
pub fn connection_plan(resolved: &ResolvedConnection) -> ConnectionPlan {
    let side = resolved.execution.side;
    ConnectionPlan {
        bootstrap_servers: resolved.bootstrap_servers.clone(),
        auth_mode: resolved.auth.mode_str().to_string(),
        username: resolved.auth.username().map(str::to_string),
        password_env: resolved
            .password
            .as_ref()
            .map(|_| side.password_env().to_string()),
        tls: Some(resolved.tls()),
        // The path the resolver's own projection mounts, not the plan
        // `ConfigMap`'s `source-ca.pem`: a `KafkaCluster` may name its CA in a
        // Secret, which this controller must never read, and projecting the
        // reference as an execution Job does keeps the check on the real run's
        // path.
        ca_file: resolved.tls_ca.as_ref().map(|_| side.ca_file_path()),
        // The `mtls` pair's in-pod paths, where the resolver's projection
        // mounts them; never key material.
        client_cert_file: resolved
            .client_certificate
            .as_ref()
            .map(|_| side.client_cert_file_path()),
        client_key_file: resolved
            .client_certificate
            .as_ref()
            .map(|_| side.client_key_file_path()),
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
        // The policy as it was when this plan was rendered, pinned into the
        // digest-covered document. Not the digest the verdict is computed
        // under: the completeness verdict is computed at commit against the
        // attestations then in force, and `status.binding.policyDigest` is
        // updated to that digest.
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

/// What an observation was taken against (D2 §5.1), and what D2 §5.7's
/// staleness rule compares the current connection to. Written once, when the
/// Job is created, and never refreshed: re-resolving on every pass would make
/// "the binding changed" unobservable.
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

/// The attestation that could apply to this observation, out of the
/// installation policy.
///
/// Narrowed by namespace and `KafkaCluster` name only; the principal, cluster
/// id and expiry are [`logweir_core::check_contract::visibility`]'s to judge.
/// Of two entries for one (namespace, cluster) the first is taken: picking "the
/// best" would let a later entry upgrade a claim an earlier one refused.
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

/// The attestation that may be applied to this observation, fail-closed.
///
/// The policy's matcher compares the attestation's `principal` and `clusterId`
/// against the observation's, and an absent observation value arrives as `""`,
/// which equals an attestation's empty field: a false `attestedComplete` from
/// two unknowns. So no attestation is a candidate unless the observation
/// carries a non-blank principal and cluster id and the attestation names all
/// four; `Policy::validate` requires only a non-empty `id`.
#[must_use]
pub fn attestation_candidate<'a>(
    attestations: &'a [Attestation],
    namespace: &str,
    cluster_name: &str,
    principal: &str,
    cluster_id: &str,
) -> Option<&'a Attestation> {
    let blank = |v: &str| v.trim().is_empty();
    if blank(namespace) || blank(cluster_name) || blank(principal) || blank(cluster_id) {
        return None;
    }
    let candidate = attestation_for(attestations, namespace, cluster_name)?;
    if blank(&candidate.namespace)
        || blank(&candidate.kafka_cluster)
        || blank(&candidate.principal)
        || blank(&candidate.cluster_id)
    {
        return None;
    }
    Some(candidate)
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
        // The cluster id the broker answered with, never a cached one: a
        // replaced cluster must not inherit the old one's attestation.
        cluster_id: inventory.cluster_id.clone().unwrap_or_default(),
        principal: principal.to_string(),
        topic_authorization_error_in_listing: inventory.topic_authorization_error_in_listing,
        expected: inventory.expected,
        truncated: inventory.truncated,
    }
}

/// D2 §5.1's status spelling of a truncation reason: the wire contract says
/// `maxTopics`/`relayLimit`, the CRD's `truncationReason` is a CamelCase status
/// reason, and mapping once here stops a third spelling reaching a UI.
#[must_use]
pub fn truncation_reason_str(reason: TruncationReason) -> &'static str {
    match reason {
        TruncationReason::MaxTopics => "MaxTopics",
        TruncationReason::RelayLimit => "RelayLimit",
    }
}

/// The status rendering of a computed [`Visibility`]. The basis list is capped
/// at the CRD's `maxItems: 8` although at most six can occur, because a status
/// patch that violates its schema is a 422 that loses the whole result.
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
        // The attestation's id, not its statement: who made it and when live in
        // the policy `ConfigMap`, and copying an administrator's free text onto
        // every result would put an unbounded, unredacted string on a status.
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

/// The chunk index a status carries (D2 §5.1, §5.5). The names do not live in
/// the status: an inventory is unbounded, so this is the names, digests, counts
/// and first and last topic of each chunk, enough to page, to seek by prefix,
/// and to prove a fetched chunk is the one described.
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

/// Why a verified relay still cannot become a result. Always
/// [`CheckCode::ResultUnreadable`]: the frames are the measured half (the
/// decoder proved their count and digest), so where they and the document
/// disagree, the document is wrong.
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

/// What a relay that carried NO result document at all says — the one case the
/// word "unreadable" still fits, because nothing was read.
const NO_RESULT_DOCUMENT: &str = "the check Job's relay carries no result document, so neither a \
     topic inventory nor a check outcome could be read; nothing is inferred from the exit code";

/// A result document with no inventory AND no blocking check that is not
/// `ready`. The document contradicts itself — everything it checked passed and
/// it measured nothing — so there is nothing to project and nothing to store.
const NO_INVENTORY_AND_NO_FAILURE: &str = "the check Job relayed a verified result that carries \
     no topic inventory and no blocking check that is not ready; a topicInventory check that \
     produced no inventory has nothing to store";

/// The blocking check a no-inventory result is about.
///
/// [`logweir_core::check_contract::aggregate`]'s precedence: `notReady` first,
/// then `unknown` or `skipped`. The second arm matters: `MetadataTimeout` is an
/// `unknown` code and one of the two reasons D2 §14.4 S11 accepts for a
/// timed-out discovery. Advisory rows are excluded (an advisory `notReady` is a
/// warning, and would name the wrong cause); execution-only rows describe a run
/// that has not happened.
fn blocking_failure(checks: &[CheckOutcome]) -> Option<&CheckOutcome> {
    let blocking = || checks.iter().filter(|c| c.gating == Gating::Blocking);
    blocking()
        .find(|c| c.state == CheckState::NotReady)
        .or_else(|| {
            blocking().find(|c| matches!(c.state, CheckState::Unknown | CheckState::Skipped))
        })
}

/// `status.reason` and `status.message` for a `Succeeded` check phase whose
/// relay carried no topic inventory.
///
/// `None` means no result document was relayed; `Some(checks)` is the
/// document's own `checks` array, already redacted and capped. The reason is
/// the failing check's [`CheckCode`], a closed vocabulary of valid
/// `metav1.Condition` reasons, so no runner text reaches `status.reason`. The
/// message carries the check id, the runner's sentence and its remedy; the
/// effective bound is [`logweir_core::check_contract::MESSAGE_MAX_CHARS`] (512),
/// because `redact` itself caps, and a longer join loses its tail to `…` rather
/// than 422-ing the patch.
fn no_inventory_verdict(checks: Option<&[CheckOutcome]>) -> (&'static str, String) {
    let Some(checks) = checks else {
        return (
            CheckCode::ResultUnreadable.as_str(),
            NO_RESULT_DOCUMENT.to_string(),
        );
    };
    let Some(failed) = blocking_failure(checks) else {
        return (
            CheckCode::ResultUnreadable.as_str(),
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
    (failed.code.as_str(), message)
}

/// Hold the runner's result document to the frames it arrived with.
///
/// * `counts.returned` against the number of topic frames, so no status count
///   lacks a chunk.
/// * `topicsSha256` against the digest of the frames the decoder verified, so
///   the status digest is a proof, not a copy: hashing the fetched chunks gives
///   the published value.
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
        // Absent when nothing was expected, not a block of zeroes: "nobody named
        // a topic" and "two were named and both are invisible" differ.
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
        // An empty cluster has no chunks and `counts.returned: 0`, which is not
        // "unknown": `Some(vec![])` tells "stored nothing" from "no result yet".
        chunks: Some(index),
    }
}

/// `observedAt + discovery.freshSeconds` (D2 §5.7's first staleness rule). The
/// other two (the binding changed, a newer success supersedes this) compare
/// against the connection now and against siblings, so they are the API's; a
/// controller-written `stale` flag would go out of date as it was written.
#[must_use]
pub fn fresh_until(observed_at: DateTime<Utc>, fresh_seconds: u32) -> DateTime<Utc> {
    observed_at + chrono::Duration::seconds(i64::from(fresh_seconds))
}

/// Whether a non-terminal object whose Job has vanished is past hope (D2 §4.3).
/// The Job's name is a pure function of this object's UID and its TTL fires
/// only after a status commit, so a vanished Job was deleted from under the
/// check and its relay is gone. `false` for an object that never created a
/// Job: that is the start path.
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

/// Which terminal discoveries are collectable (D2 §4.3 and §5.8). An object
/// matching either rule is returned:
///
/// * `now > observedAt + retentionSeconds`, the age bound;
/// * outside the newest `keepPerConnection` for the same connection UID, the
///   cohort bound, which keeps a connection refreshed every minute from filling
///   etcd inside the retention window.
///
/// Newest-first by `observedAt` with the UID as tie-break, so two passes agree.
/// Its one caller is [`collect_expired`]; the `delete` grant it implies is held
/// to exactly this shape by
/// `crates/logweir/tests/manifest_lint.rs::every_granted_verb_has_a_caller`.
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

/// The age basis of one terminal discovery: `observedAt`, else the `Complete`
/// condition's transition time, else `metadata.creationTimestamp`, so a
/// discovery that failed before observing anything is still collectable.
fn gc_basis(discovery: &TopicDiscovery) -> Option<Time> {
    let status = discovery.status.as_ref();
    status
        .and_then(|s| s.observed_at)
        .or_else(|| {
            status
                .and_then(|s| s.conditions.as_ref())
                .and_then(|c| c.iter().find(|c| c.r#type == CONDITION_COMPLETE))
                .and_then(|c| c.last_transition_time)
        })
        .or(discovery.metadata.creation_timestamp.as_ref().map(|t| t.0))
}

/// One listed object as [`expired_terminal`] reads it, or `None` when it is not
/// a collectable terminal discovery.
fn gc_row(discovery: &TopicDiscovery) -> Option<(TerminalDiscovery, String)> {
    let phase = discovery.status.as_ref().and_then(|s| s.phase.as_deref())?;
    // A non-terminal object's Job pod holds the only copy of an unread relay.
    if !TERMINAL_PHASES.contains(&phase) {
        return None;
    }
    // An object being deleted is not deleted again.
    if discovery.metadata.deletion_timestamp.is_some() {
        return None;
    }
    let name = discovery.name_any();
    let uid = discovery.uid().filter(|u| !u.is_empty())?;
    let observed_at = gc_basis(discovery)?;
    Some((
        TerminalDiscovery {
            name: name.clone(),
            uid,
            connection_uid: discovery
                .status
                .as_ref()
                .and_then(|s| s.binding.as_ref())
                .and_then(|b| b.connection_uid.clone())
                .filter(|u| !u.is_empty()),
            observed_at,
        },
        name,
    ))
}

/// Collect this namespace's expired terminal discoveries (D2 §4.3's `gc.rs`).
///
/// * Only `TopicDiscovery` objects, through an `Api<TopicDiscovery>`, in one
///   namespace.
/// * Only terminal ones: [`gc_row`] drops everything else first, so no policy
///   value can collect a discovery whose relay is unread.
/// * Only by UID precondition, so a same-named replacement is a 409, not a
///   casualty; a 409 or 404 means already gone, which is success.
/// * At most [`GC_MAX_DELETES_PER_PASS`] per pass.
///
/// The owned `ConfigMap`s and the check Job go by owner cascade.
///
/// # Errors
///
/// [`ReconcileError::Api`] only when the list fails. A failed delete is logged
/// and the pass continues.
async fn collect_expired(
    api: &Api<TopicDiscovery>,
    namespace: &str,
    retention_seconds: u32,
    keep_per_connection: u32,
    now: Time,
) -> Result<usize, ReconcileError> {
    let page = api
        .list(&kube::api::ListParams::default().limit(GC_LIST_LIMIT))
        .await
        .map_err(ReconcileError::Api)?;
    let truncated = page
        .metadata
        .continue_
        .as_deref()
        .is_some_and(|c| !c.is_empty());
    let rows: Vec<(TerminalDiscovery, String)> = page.items.iter().filter_map(gc_row).collect();
    let terminal: Vec<TerminalDiscovery> = rows.iter().map(|(t, _)| t.clone()).collect();
    // A TRUNCATED PAGE IS NOT THE NEWEST ANYTHING — see [`GC_LIST_LIMIT`]. The
    // cohort rule is disabled by making the cohort unreachably large rather
    // than by a second code path, so both passes run the same pure function.
    let keep = if truncated {
        u32::MAX
    } else {
        keep_per_connection
    };
    if truncated {
        debug!(
            namespace = %namespace,
            limit = GC_LIST_LIMIT,
            "the discovery listing was truncated; this GC pass applies the retention window only"
        );
    }
    let doomed = expired_terminal(&terminal, retention_seconds, keep, now);
    let mut collected = 0usize;
    for uid in doomed.iter().take(GC_MAX_DELETES_PER_PASS) {
        let Some((_, name)) = rows.iter().find(|(t, _)| &t.uid == uid) else {
            continue;
        };
        let params = kube::api::DeleteParams {
            preconditions: Some(kube::api::Preconditions {
                uid: Some(uid.clone()),
                resource_version: None,
            }),
            ..kube::api::DeleteParams::default()
        };
        // `api.delete(`: the receiver spelling `scripts/check-no-archive-write.sh`
        // treats as legitimate (its delete token is anchored to store-shaped
        // identifiers), and the one `manifest_lint::every_granted_verb_has_a_caller`
        // can pair with its grant through the `Api<T>` declaration.
        match api.delete(name, &params).await {
            Ok(_) => {
                collected += 1;
                info!(
                    namespace = %namespace,
                    discovery = %name,
                    "a terminal TopicDiscovery past its retention window was collected"
                );
            }
            Err(e) => warn!(
                namespace = %namespace,
                discovery = %name,
                error = %e,
                "a terminal TopicDiscovery could not be collected; the next pass tries again"
            ),
        }
    }
    if doomed.len() > GC_MAX_DELETES_PER_PASS {
        debug!(
            namespace = %namespace,
            remaining = doomed.len() - GC_MAX_DELETES_PER_PASS,
            "more expired discoveries than one pass may collect; the rest wait for the next pass"
        );
    }
    Ok(collected)
}

/// What this reconciler needs: its own context rather than [`super::Context`],
/// because the installation-policy cache would be dead weight on the
/// reconcilers that never read a policy, and this file has no use for an
/// archive handle.
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
    /// Whether the status on the server now says what this pass computed;
    /// `false` when the `/status` PATCH answered 409 (seam S7). It gates
    /// [`Outcome::ttl_patched`].
    pub committed: bool,
    /// Whether this pass patched the Job's TTL — which happens ONLY after a
    /// status commit returned 200.
    pub ttl_patched: bool,
    /// How long until the next pass.
    pub requeue_seconds: u64,
    /// How many EXPIRED TERMINAL discoveries this pass collected — D2 §4.3's
    /// `gc.rs`. Non-zero only on the terminal guard, and never more than
    /// [`GC_MAX_DELETES_PER_PASS`].
    pub collected: usize,
}

impl Outcome {
    fn new(phase: &str, reason: &str, requeue_seconds: u64) -> Self {
        Self {
            phase: phase.to_string(),
            reason: reason.to_string(),
            created_job: false,
            chunks_written: 0,
            committed: false,
            ttl_patched: false,
            requeue_seconds,
            collected: 0,
        }
    }

    /// Whether the phase this pass published is terminal.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        TERMINAL_PHASES.contains(&self.phase.as_str())
    }
}

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
    // Redacted and capped at the CRD's 1,024 at this one chokepoint: one message
    // (the relay refusal) derives from a runner's own document.
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

/// The CRD's `maxLength: 1024` on `status.message`, in characters, cut on a
/// character boundary: a 422 on the commit would lose the whole result.
fn cap_message(message: &str) -> String {
    const MAX: usize = 1024;
    if message.chars().count() <= MAX {
        return message.to_string();
    }
    message.chars().take(MAX - 1).collect::<String>() + "…"
}

/// Add the optimistic-concurrency precondition to a `/status` merge patch (seam
/// **S7**), the body shape the other reconcilers use: the API server applies a
/// `metadata.resourceVersion` in a patch body as an update precondition and
/// answers `409 Conflict` on a mismatch, a compare-and-set without the `update`
/// verb.
///
/// # Errors
///
/// A [`kube::Error`] shaped as the API server's own "no resourceVersion"
/// answer, for an object that carries none (unreachable for a watched or read
/// object).
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
    // No write when nothing changed: this reconciler's own status patch wakes
    // it, so a patch that changed only a clock reading would spin the loop,
    // which is why `observedAt` is the runner container's `finishedAt`, never
    // `now`.
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
            // A 409 is the precondition working, not a failure.
            debug!(
                discovery = %name,
                "the status changed under this reconcile (409); the next pass reads it"
            );
            Ok(Commit::Conflicted)
        }
        Err(e) => Err(ReconcileError::Api(e)),
    }
}

/// Reconcile one `TopicDiscovery`.
///
/// 1. **A terminal object is done**: nothing read, nothing patched, so a
///    namespace of finished discoveries costs one guard each.
/// 2. **The Job is looked up first**, by a name that is a pure function of this
///    object's UID.
/// 3. **A cancel is honoured before the start path.** A cancelled object never
///    creates a Job, and an existing Job's deadline is collapsed by
///    [`crate::check::cancel`], which verifies the owner UID first.
/// 4. **The start path resolves, binds, admits, plans, then creates** (D2
///    §5.2): an unresolvable connection consumes no concurrency slot, and a
///    check over the ceiling leaves no plan `ConfigMap` behind.
/// 5. **The observe path proves the pod, reads, classifies, stores, commits,
///    then sets the TTL.**
///
/// # Errors
///
/// [`ReconcileError`] for anything that is not a verdict: a missing namespace
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

    // One clock read for the whole pass, so a verdict and the condition that
    // reports it cannot straddle two instants.
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
        // Retention is the only work a terminal discovery has left (D2 §4.3,
        // §5.8). An absent policy is the documented defaults (24 h, keep five),
        // never an error.
        let policy = policy::load(&ctx.client, ctx.policy_ref.as_ref(), &ctx.policy_cache, now)
            .await
            .map_err(ReconcileError::Api)?;
        let discovery_policy = &policy.policy().discovery;
        let api: Api<TopicDiscovery> = Api::namespaced(ctx.client.clone(), &namespace);
        let collected = collect_expired(
            &api,
            &namespace,
            discovery_policy.retention_seconds,
            discovery_policy.keep_per_connection,
            now,
        )
        .await?;
        return Ok(Outcome {
            reason,
            collected,
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
                // to a stale cache.
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
    // `check::cancel` verifies the owner UID and leaves a finished or foreign
    // Job alone: a name is not an identity.
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

    // STEP 2. The connection, through the shared resolver.
    let clusters: Api<KafkaCluster> = Api::namespaced(ctx.client.clone(), namespace);
    let Some(cluster) = clusters
        .get_opt(&connection_name)
        .await
        .map_err(ReconcileError::Api)?
    else {
        // Terminal: `spec.request` is immutable, so a later pass would ask the
        // same question. The way forward is a KafkaCluster and a new
        // TopicDiscovery, which the message says.
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
            // The same refusals a backup gets (`ConnectionUse` changes none): a
            // discovery must not green-light a connection a run would refuse.
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
        // Queued is neither a failure nor terminal. The binding travels with it
        // so an operator can see which connection is waiting.
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
        let commit = write_status(api, discovery, &status).await?;
        return Ok(Outcome {
            committed: commit.is_committed(),
            ..Outcome::new(PHASE_QUEUED, code.as_str(), limits::QUEUED_REQUEUE_SECS)
        });
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
        // No `source-ca.pem`: see `connection_plan`.
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

    // The guard every connection-projecting path calls. One `namespace` feeds
    // both the `KafkaCluster` GET and the Job today; this makes it a checked
    // property, so a later edit cannot project a same-named Secret from another
    // namespace.
    if let Err(refusal) = resolved.check_job_namespace(namespace) {
        return terminal(
            discovery,
            api,
            PHASE_FAILED,
            CheckCode::ConnectionInvalid.as_str(),
            &format!("{} ({})", refusal.message, refusal.field),
            TopicDiscoveryStatus {
                binding: Some(binding),
                ..TopicDiscoveryStatus::default()
            },
            now,
        )
        .await;
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
        attempt: None,
    };
    match check::create_job(&ctx.client, &spec).await {
        Ok(_) => {}
        // A 409 is the duplicate-reconcile case: the Job's name is a pure
        // function of this object's UID, so the check already exists.
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
    let commit = write_status(api, discovery, &status).await?;
    Ok(Outcome {
        created_job: true,
        committed: commit.is_committed(),
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
    // Seam S6: the `job-name` label narrows the list and the controller
    // `ownerReference` UID decides; a pod owned by something else is ignored and
    // logged. Not `check::observe`, which discards the pod: D2 §5.1 takes
    // `observedAt` and `startedAt` from the runner container's own state (so the
    // status is stable across passes), so the proven pod is kept and its log is
    // the one read.
    let pod = check::pod::find_owned_pod(&ctx.client, namespace, job)
        .await
        .map_err(ReconcileError::Api)?;
    let finished = super::backup::job_finished(job);

    let log = match (finished, pod.as_ref()) {
        (true, Some(p)) => {
            let pods: Api<Pod> = Api::namespaced(ctx.client.clone(), namespace);
            match runner_log::read_text(&pods, &p.name_any(), &runner_log::RELAY).await {
                Ok(log) => Some(log),
                // No log is an answer: a pod whose `runner` never started has
                // no stdout, which the API server reports as 400 or 404. Any
                // other failure requeues, because a verdict from a
                // control-plane failure measures nothing.
                Err(e) if check::is_log_absent(&e) => None,
                Err(e) => return Err(ReconcileError::Api(e)),
            }
        }
        _ => None,
    };

    // The digest the runner was pinned to, read off this object's own plan
    // `ConfigMap` after its owner UID, digest annotation and immutability are
    // checked. Re-rendering the plan would compare against the policy in force
    // now, and a mid-run policy edit would turn a good result unreadable.
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

    // The Events that explain a pod that never started, read only when the
    // classifier could use one; a running or finished pod costs no request.
    let events = check::job_events(&ctx.client, namespace, job, pod.as_ref(), now).await;
    let observation = check::classify(&check::Input {
        job,
        pod: pod.as_ref(),
        events: &events,
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
            let commit = write_status(api, discovery, &status).await?;
            Ok(Outcome {
                committed: commit.is_committed(),
                ..Outcome::new(
                    PHASE_RUNNING,
                    observation.reason.as_str(),
                    REQUEUE_RUNNING_SECS,
                )
            })
        }
        CheckPhase::Failed => {
            // Early cancel: a terminal waiting state cannot succeed and its
            // deadline is minutes away, so collapsing it now shows the reason
            // instead of a spinner.
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
            // The same gate `commit` uses: a terminal status that 409'd is not
            // on the server, and a TTL here would let the Job and the pod
            // holding the relay be deleted with the reason.
            if outcome.committed {
                finish_job(ctx, namespace, job_name, finished, outcome).await
            } else {
                Ok(outcome)
            }
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

    // The result document, parsed, bounds-checked and redacted by the pure
    // crate; a parse failure is `ResultUnreadable`. The whole document is
    // bound, not just `inventory`: `result.checks` is where a runner that
    // could not reach the broker puts its answer.
    let mut document = match relay.result() {
        Some(Ok(result)) => Some(result),
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
    let inventory = document.as_mut().and_then(|d| d.inventory.take());
    let Some(inventory) = inventory else {
        // No inventory is not the same fact as no result: an unreachable broker
        // produces a result document with no `inventory` block and one
        // classified `connection.authenticated` row (see
        // `check::kinds::inventory`), and this reads it.
        let (reason, message) =
            no_inventory_verdict(document.as_ref().map(|d| d.checks.as_slice()));
        return terminal(
            discovery,
            api,
            PHASE_FAILED,
            reason,
            &message,
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

    // The policy the verdict will be computed under, loaded before anything is
    // written because it also supplies the storage ceiling. An unreadable
    // policy fails closed: `Policy::fail_closed` carries no attestations.
    let load = policy::load(&ctx.client, ctx.policy_ref.as_ref(), &ctx.policy_cache, now)
        .await
        .map_err(ReconcileError::Api)?;

    // The schema bound, before any `ConfigMap` is written: a runner that relayed
    // more entries than its plan allowed could otherwise produce more than the
    // CRD's 64 chunks inside the relay budget, and the `/status` PATCH would
    // then be refused 422 forever with the chunks orphaned. The entries are cut
    // here, so the chunks, the index and the digest are one set.
    let ceiling = storable_entry_ceiling(effective_max_topics(
        discovery.spec.request.max_topics,
        load.policy().discovery.hard_max_topics,
    ));
    let (inventory, entries) = clamp_to_storable(&inventory, &relay.topics, ceiling);
    let clamped = entries.len() < relay.topics.len();

    // The chunks, first. D2 §5.5's two bounds (at most 2,500 entries and at
    // most 768 KiB per chunk) are `chunks::split`'s.
    let split = chunks::split(entries);
    if split.len() > MAX_STATUS_CHUNKS {
        // Unreachable by construction (`storable_entry_ceiling` bounds the
        // entry count), and a refusal rather than an assertion: nothing is
        // written, because a status that cannot name its own chunks is worse
        // than no result.
        return terminal(
            discovery,
            api,
            PHASE_FAILED,
            CheckCode::ResultUnreadable.as_str(),
            &format!(
                "the inventory split into {} chunks, over the {MAX_STATUS_CHUNKS} a \
                 TopicDiscovery status can index; nothing was stored",
                split.len()
            ),
            TopicDiscoveryStatus {
                started_at,
                observed_at: Some(observed_at),
                ..TopicDiscoveryStatus::default()
            },
            now,
        )
        .await;
    }
    let owner = owner_of(&discovery.name_any(), uid);
    let objects: Vec<ConfigMap> = split
        .iter()
        .map(|c| chunks::build_chunk(job_name, namespace, &owner, c, split.len()))
        .collect();
    let written = match chunks::write_all(&ctx.client, namespace, uid, &objects).await {
        Ok(written) => written,
        Err(chunks::WriteError::Api(e)) => return Err(ReconcileError::Api(e)),
        Err(chunks::WriteError::Conflict(e)) => {
            // Terminal: two different inventories want one name. Retrying
            // cannot fix it, and adopting the other would publish a digest over
            // bytes this pass never produced.
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

    // The completeness verdict (seam S3), and the one place an attestation is
    // consulted.
    let binding = discovery.status.as_ref().and_then(|s| s.binding.as_ref());
    let cluster_name = binding
        .and_then(|b| b.connection_name.clone())
        .unwrap_or_else(|| discovery.spec.request.connection_ref.name.clone());
    let principal = binding
        .and_then(|b| b.principal.clone())
        .unwrap_or_default();
    let signals = visibility_signals(&inventory, namespace, &cluster_name, &principal);
    // FAIL-CLOSED. An absent principal or an unobserved cluster id is not a
    // wildcard; see `attestation_candidate`.
    let attestation = attestation_candidate(
        &load.policy().discovery.visibility_attestations,
        namespace,
        &cluster_name,
        &principal,
        &signals.cluster_id,
    );
    let verdict = visibility(&signals, attestation, now);

    let index = chunk_index(entries, &split, job_name);
    let result = inventory_status(&inventory, entries, &verdict, index);
    let returned = result.counts.returned;
    let visibility_state = result.visibility.state.clone();

    // Named in the message, and machine-readable in `result.truncated` /
    // `result.truncationReason`. The condition stays `Complete=True/Succeeded`:
    // a truncated inventory is an honest observation, and a reason outside the
    // closed `CheckCode` vocabulary would have no remedy attached.
    let note = if clamped {
        format!(
            "; the runner relayed {} entries and the plan allowed {ceiling}, so the inventory \
             was cut to the first {} and is reported truncated",
            relay.topics.len(),
            entries.len()
        )
    } else {
        String::new()
    };
    let status = status_with_condition(
        discovery,
        PHASE_SUCCEEDED,
        CheckCode::Succeeded.as_str(),
        &format!(
            "observed {returned} visible topics at {observed_at}; completeness is \
             `{visibility_state}` — Kafka omits topics this principal cannot describe, so a \
             successful listing alone is never proof that the cluster holds no others{note}"
        ),
        now,
        TopicDiscoveryStatus {
            started_at,
            observed_at: Some(observed_at),
            fresh_until: Some(fresh_until(
                observed_at,
                load.policy().discovery.fresh_seconds,
            )),
            // The policy the verdict was computed under: the one binding field
            // this pass may move. The rest describes the connection at Job
            // creation and is never refreshed; `policyDigest` is a commit-time
            // fact, or an attestation added while the Job ran would sit beside
            // the pre-attestation digest and a re-derived verdict would differ.
            // Every other field is `None`, so the merge patch touches nothing
            // else.
            binding: Some(DiscoveryBinding {
                connection_name: None,
                connection_uid: None,
                connection_generation: None,
                principal: None,
                auth_mode: None,
                bootstrap_sha256: None,
                policy_digest: Some(load.policy().digest()),
            }),
            result: Some(result),
            ..TopicDiscoveryStatus::default()
        },
    );

    // The commit: every chunk exists before this line, and nothing before it
    // is visible to a reader of the custom resource.
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
        committed: commit.is_committed(),
        ..Outcome::new(
            PHASE_SUCCEEDED,
            CheckCode::Succeeded.as_str(),
            REQUEUE_TERMINAL_SECS,
        )
    };
    if commit.is_committed() {
        finish_job(ctx, namespace, job_name, true, outcome).await
    } else {
        // The status changed under this pass. No TTL: the relay lives on the
        // pod, and a TTL before a commit lets garbage collection race the read.
        Ok(outcome)
    }
}

/// Patch the finished Job's TTL, only ever after a status commit: the TTL
/// controller deletes a Job and its pods together and the relay lives on the
/// pod. The status write is `?`-propagated, so a patch that did not return 200
/// leaves the reconcile before this line.
async fn finish_job(
    ctx: &DiscoveryContext,
    namespace: &str,
    job_name: &str,
    finished: bool,
    outcome: Outcome,
) -> Result<Outcome, ReconcileError> {
    if !finished {
        // An unfinished Job gets no TTL: on a running Job it would fire the
        // moment it finished, the race this ordering avoids.
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

/// Write a terminal status and return the outcome it describes, carrying its
/// commit: [`Outcome::committed`] is what the `Failed` arm gates the TTL on, so
/// a terminal status whose PATCH answered 409 leaves the pod holding the relay
/// alive for the pass that reads the newer object.
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
    let commit = write_status(api, discovery, &status).await?;
    warn!(
        discovery = %discovery.name_any(),
        phase = phase,
        reason = reason,
        committed = commit.is_committed(),
        "topic discovery reached a terminal state"
    );
    Ok(Outcome {
        committed: commit.is_committed(),
        ..Outcome::new(phase, reason, REQUEUE_TERMINAL_SECS)
    })
}

/// The owner reference every object this reconciler creates carries, built from
/// `kube::Resource`'s own `api_version`/`kind`: a hand-written `apiVersion` that
/// drifts from the CRD leaves the garbage collector unable to resolve the
/// owner, and the cascade silently does not happen.
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

/// The frame expectations for this check, taken from its own plan `ConfigMap`.
///
/// The outer `Result` is a transport failure (requeue); the inner one is a
/// terminal [`plan::PlanError`] ([`CheckCode::CheckPlanConflict`]): a plan
/// object that is missing, not owned by this subject, mutable or without a
/// digest is not this check's plan.
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
    // The three-part rule (owner UID, digest, immutability) is the framework's;
    // only which digest is this file's.
    if let Err(e) = plan::accepts_existing(&existing, uid, &digest) {
        return Ok(Err(e));
    }
    Ok(Ok(FrameExpectations {
        plan_sha256: digest,
        subject_uid: uid.to_string(),
    }))
}

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
/// reads, out of the environment (D2 §4.4).
///
/// The decision is the pure, tested [`policy::configured_ref`], which treats the
/// empty string an `env:` entry with an empty `value:` produces as unset; this
/// is the read plus one log line, kept out of `main` so `main` stays a list of
/// registrations. `None` is a supported install that reads as the documented
/// defaults.
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

/// Run the `TopicDiscovery` controller until the process ends. It `owns` its
/// check Jobs, so a Job that finishes wakes its discovery rather than waiting
/// out the requeue.
pub fn controller(
    client: kube::Client,
    runner_image: crate::job::RunnerImage,
    policy_ref: Option<(String, String)>,
) -> impl std::future::Future<Output = ()> + Send {
    // One watch per watched namespace: the whole cluster unless
    // `LOGWEIR_WATCH_NAMESPACES` names namespaces, and then an `Api::namespaced`
    // watch each, the only shape the scoped chart's RoleBindings permit.
    crate::scope::run_everywhere(move |namespace| {
        controller_in(
            client.clone(),
            runner_image.clone(),
            policy_ref.clone(),
            namespace,
        )
    })
}

/// One watch of [`controller`], over `namespace` (`None` is the whole
/// cluster).
fn controller_in(
    client: kube::Client,
    runner_image: crate::job::RunnerImage,
    policy_ref: Option<(String, String)>,
    namespace: Option<String>,
) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<TopicDiscovery> = crate::scope::api(&client, namespace.as_deref());
    let jobs: Api<Job> = crate::scope::api(&client, namespace.as_deref());
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
