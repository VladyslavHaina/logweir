//! The `RehearsalSchedule` reconciler — PLAT-14.3, decision D3 §4.
//!
//! # The thin half
//!
//! Every verdict here is computed by [`crate::rehearsal`], which holds no
//! client and reads no clock. This module reads what the API server says, calls
//! [`decide`], and then — for a slot that fires — renders the plan, proves it
//! falls inside the SIGNED scope, reserves the child, creates it and patches
//! `rehearsalschedules/status`. That is the whole of it.
//!
//! # THE FOUR RULES THIS FILE EXISTS TO KEEP
//!
//! 1. **The controller never mints its own authorization** (D3 §4.3). There is
//!    one signed standing document, carried by an immutable `Approval`, and
//!    this reconciler COPIES its envelope and sidecar into the bundle byte for
//!    byte. It holds no signing key — `weirkeeper` does not link
//!    `logweir-evidence` at all (Global Constraint 27, `tests/linkage.rs`) — so
//!    "the controller could sign its own approval" is not a rule this file
//!    keeps but a capability the crate does not have.
//!
//! 2. **A plan outside the signed scope never reaches a Job.** The scope check
//!    runs on the RENDERED bytes, before the reservation, before the `POST`,
//!    and it is
//!    [`logweir_core::execution_contract::plan_within_scope`] — the same
//!    predicate the runner runs against the mounted bundle, from the same
//!    [`logweir_core::execution_contract::plan_scope_facts`] projection. Two
//!    predicates would be exactly the drift the split exists to prevent, so
//!    there is no second one here.
//!
//! 3. **The controller deletes no topic, ever** (D3 §4.4). Teardown is the
//!    runner's phase 9, inside a prefix-scoped deletion guard. What this file
//!    does with a failed teardown is REFUSE THE NEXT SLOT
//!    ([`crate::rehearsal::SkipReason::LeftoverTopics`]) — the run that would
//!    otherwise collide is not allowed to adopt or delete topics it did not
//!    create.
//!
//! 4. **A skip is recorded, never a silent no-op.** Every path that produces no
//!    rehearsal writes `status.lastSkipped` with a closed-set reason and a
//!    sentence naming the rule. An unattended rehearsal that quietly did
//!    nothing for eleven weeks is indistinguishable, from the outside, from one
//!    that passed eleven times.
//!
//! # What it writes, and what it does not
//!
//! It patches `rehearsalschedules/status` with a merge PATCH carrying
//! `metadata.resourceVersion` as the update precondition (seam **S7**), creates
//! `Restore` objects and their approval-bundle `ConfigMap`s, and touches
//! nothing else. There is no `Api<Restore>::patch_status` anywhere in this
//! file: the `Restore` reconciler owns that object's status, and a rehearsal
//! that could rewrite its own child's result would be its own auditor.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::{ListParams, ObjectMeta, Patch, PatchParams, PostParams};
use kube::runtime::controller::Action;
use kube::runtime::{watcher, Controller};
use kube::{Api, Resource as _, ResourceExt as _};
use serde_json::{json, Value};
use tracing::{debug, info, warn};

use logweir_core::execution_contract as wire;
use logweir_core::rehearsal_scope::RehearsalScope;
use logweir_core::spec::AllowedClusters;

use crate::crds::approval::{Approval, SubjectKind};
use crate::crds::backup::Backup;
use crate::crds::backup_destination::BackupDestination;
use crate::crds::kafka_cluster::KafkaCluster;
use crate::crds::recovery_catalog::RecoveryCatalog;
use crate::crds::rehearsal_schedule::RehearsalSchedule;
use crate::crds::restore::{
    AuthorizationKind, Restore, RestoreAuthorization, RestoreSpec, RestoreTarget, TargetMode,
    TopicNaming,
};
use crate::crds::{ArchiveRef, LocalRef};
use crate::rehearsal::{self, PointCandidate, Selected, Skip, SkipReason, Window};

use super::approval::ReconcileError;
use super::Context;

// ===========================================================================
// Constants
// ===========================================================================

/// `Ready` — this controller looked at the object and could act on it.
pub const CONDITION_READY: &str = "Ready";
/// `Authorized` — D3 §4.1: the standing authorization currently admits a slot.
pub const CONDITION_AUTHORIZED: &str = "Authorized";
/// `RehearsalHealthy` — the last finished rehearsal passed.
pub const CONDITION_REHEARSAL_HEALTHY: &str = "RehearsalHealthy";

/// `Ready`'s reason for a schedule this pass could evaluate.
pub const REASON_SCHEDULED: &str = "Scheduled";
/// `Ready`'s reason for `spec.suspend: true`.
pub const REASON_SUSPENDED: &str = "Suspended";
/// `Ready`'s reason for an object with no namespace or UID.
pub const REASON_NOT_EVALUATED: &str = "NotEvaluated";
/// `Authorized`'s reason when the standing document admits this slot.
pub const REASON_AUTHORIZED: &str = "Authorized";
/// `RehearsalHealthy`'s reason before anything has finished.
pub const REASON_NO_RESULT: &str = "NoResult";
/// `RehearsalHealthy`'s reason for a pass.
pub const REASON_PASSED: &str = "Passed";
/// `RehearsalHealthy`'s reason for a fail.
pub const REASON_FAILED: &str = "Failed";
/// The steady requeue. A cron with a one-minute resolution needs to be looked
/// at more often than it fires, and a rehearsal's own child changes state
/// without changing the schedule, so this is the same thirty seconds the
/// `Restore` reconciler uses for a run in flight.
pub const REQUEUE_SECONDS: u64 = 30;
/// The requeue after an API error.
pub const ERROR_REQUEUE_SECONDS: u64 = 60;

/// How many `Backup` objects one pass projects into candidates. Bounded for the
/// same reason `protection::MAX_BACKUPS_SCANNED` is: a namespace with a year of
/// hourly backups must not turn one reconcile into a ten-thousand-object walk.
pub const MAX_BACKUPS_SCANNED: usize = 200;
/// The page size of the `Backup` walk [`candidates`] makes.
pub const BACKUP_PAGE_LIMIT: u32 = 500;
/// How many `Backup` pages one pass follows — 10 000 objects, the retention
/// controller's bound. Past it, catalog-only candidates are refused.
pub const MAX_BACKUP_PAGES: usize = 20;
/// How many catalog page `ConfigMap`s one pass reads.
pub const MAX_CATALOG_PAGES: usize = 8;

/// The `ConfigMap` key carrying D3 §4.3(e)'s "the trusted public keys".
///
/// A FIFTH BUNDLE MEMBER, and standing-only. The per-run approval path pins
/// exactly one approver key (`approver.pub.pem`) because exactly one `Approval`
/// authorises exactly one plan; a standing authorization is checked by the
/// runner against a KEYRING, so that the same document remains checkable after
/// the approver's key has been joined by a second one.
pub const AUTHORIZATION_KEYS_FILE: &str = "authorization-keys.json";

/// `AuthorizationKeyring.formatVersion`.
pub const KEYRING_FORMAT_VERSION: &str = "1.0.0";

// ===========================================================================
// The verdict
// ===========================================================================

/// What this schedule's standing `Approval` established.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Authorization {
    /// The `Approval` object's name.
    pub name: String,
    /// Its UID.
    pub uid: String,
    /// The key id its signature verified under.
    pub key_id: String,
    /// The signed envelope, verbatim.
    pub envelope: String,
    /// Its DSSE sidecar, verbatim.
    pub sidecar: String,
    /// The scope inside the signed bytes.
    pub scope: RehearsalScope,
}

/// What one pass decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Nothing to do: suspended, or no slot is due.
    Idle,
    /// A due slot produced no rehearsal, and why.
    Skipped(Skip),
    /// A slot is due and every check passed up to the scope proof, which needs
    /// the rendered plan and therefore the destination.
    Fire(Box<FireOrder>),
}

/// Everything a firing slot needs that [`decide`] could establish without I/O.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FireOrder {
    /// The slot name, `YYYYmmddThhmmss`-shaped, from [`crate::slot::slot_name`].
    pub slot: String,
    /// The instant the slot was due.
    pub due: DateTime<Utc>,
    /// The chosen point.
    pub selected: Selected,
    /// The verified standing authorization.
    pub authorization: Authorization,
    /// The rendered per-schedule topic prefix.
    pub prefix: String,
    /// The target cluster's reported id.
    pub target_cluster_id: String,
    /// The target cluster's saved connection, resolved for
    /// [`crate::connection::ConnectionUse::RestoreTarget`] — where the plan's
    /// `target.bootstrap_servers` and `target.auth` come from.
    pub target: crate::connection::ResolvedConnection,
}

/// Everything [`decide`] reads. All of it is already fetched.
#[derive(Debug, Clone)]
pub struct Facts<'a> {
    /// The schedule.
    pub schedule: &'a RehearsalSchedule,
    /// Its UID.
    pub uid: &'a str,
    /// The recomputed template digest.
    pub template_digest: &'a str,
    /// The `Approval` `spec.authorization.standingApprovalRef` names, when it
    /// exists.
    pub approval: Option<&'a Approval>,
    /// The namespace's resolved trust.
    pub trust: &'a crate::trust::ResolvedTrust,
    /// The target `KafkaCluster`, when it exists.
    pub cluster: Option<&'a KafkaCluster>,
    /// Every candidate point, in any order.
    pub candidates: &'a [PointCandidate],
    /// The `Backup` walk behind `candidates` hit its bound, so no catalog-only
    /// candidate was admitted; a `NoQualifyingPoint` skip says so.
    pub backup_verdicts_incomplete: bool,
    /// The `Restore` this schedule's status names as active or pending, when
    /// one exists. Discovered by GET of the deterministic name, never by a
    /// list.
    pub active: Option<&'a Restore>,
    /// Another schedule's unfinished rehearsal against this same target
    /// cluster, by name.
    pub target_busy: Option<String>,
    /// Now.
    pub now: DateTime<Utc>,
}

/// D3 §4's whole admission order, as one pure function.
///
/// # The order, and why each check is where it is
///
/// 1. **Suspended** — before anything else, because `suspend` is the control an
///    operator reaches for in an incident and it must not be able to fail.
/// 2. **Leftover topics** — before the cadence, because a schedule with
///    topics it could not tear down has a cluster-side problem that no slot may
///    paper over (D3 §4.4).
/// 3. **The cadence** — is a slot due at all, and is it inside
///    `startingDeadlineSeconds`? A slot older than that horizon is SKIPPED and
///    RECORDED, never run late.
/// 4. **Concurrency** — this schedule's own unfinished child
///    ([`SkipReason::ConcurrencyBlocked`]) and then another schedule's against
///    the same target ([`SkipReason::TargetBusy`]). Before the authorization
///    check, because "already running" is a cheaper and more accurate thing to
///    say than "your approval is fine but you cannot run".
/// 5. **The authorization** — present, `Verified=True`, bound to THIS
///    schedule's UID, signed by a key that may still authorise, inside its
///    life bound, not expired, and its scope agrees with the sealed spec.
/// 6. **The target** — reachable, with a reported cluster id, and a saved
///    connection `connection::resolve` accepts for `RestoreTarget` (the plan's
///    `target.auth` is rendered from it).
/// 7. **The point** — D3 §4.2's filter chain.
///
/// The scope proof over the RENDERED plan is deliberately NOT here: it needs
/// the plan, which needs the destination, which is I/O. It runs in
/// [`fire`] before the reservation and before any `POST`.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn decide(facts: &Facts<'_>) -> Verdict {
    let spec = &facts.schedule.spec;

    // ---- 1. suspended ----------------------------------------------------
    if spec.suspend {
        return Verdict::Idle;
    }

    // ---- 2. leftover topics ---------------------------------------------
    if let Err(skip) = rehearsal::cleanup_clear(facts.schedule) {
        return Verdict::Skipped(skip);
    }

    // ---- 3. the cadence --------------------------------------------------
    let cadence = match crate::cadence::Cadence::parse(&spec.schedule, None) {
        Ok(c) => c,
        Err(e) => {
            return Verdict::Skipped(Skip::new(
                SkipReason::NoQualifyingPoint,
                format!(
                    "spec.schedule is not a five-field UTC cron expression this build reads: {e}"
                ),
            ))
        }
    };
    let Some(due) = latest_owned_slot(facts.schedule, &cadence, facts.now) else {
        return Verdict::Idle;
    };
    let slot = crate::slot::slot_name(due);
    let already = facts
        .schedule
        .status
        .as_ref()
        .and_then(|s| s.last_scheduled_slot.as_deref());
    if already == Some(slot.as_str()) {
        return Verdict::Idle;
    }
    let horizon = i64::from(spec.bounds.starting_deadline_seconds);
    if (facts.now - due).num_seconds() > horizon {
        return Verdict::Skipped(Skip::new(
            SkipReason::ConcurrencyBlocked,
            format!(
                "the slot due at {due} is older than spec.bounds.startingDeadlineSeconds \
                 ({horizon}s); a missed rehearsal is recorded and not run late, because a \
                 rehearsal's whole value is that it measures recovery AT a cadence"
            ),
        ));
    }

    // ---- 4. concurrency, this schedule's then anyone's -------------------
    //
    // A run is this schedule's until it is DECIDED, not merely terminal: a
    // finished rehearsal whose evidence verdict is still owed has not produced
    // its result yet, and firing over it would replace `activeRestoreRef` and
    // lose that result (REHEARSAL-PASS-RECORDED-AS-FAILED). The wait is
    // bounded by `VERDICT_WAIT_SECONDS`.
    if let Some(active) = facts.active {
        let seen = observe(Some(active), facts.now);
        if !seen.decided {
            let why = match seen.awaiting {
                Some(owed) => format!(
                    "the rehearsal {} from the previous slot finished, but {owed}; its result is \
                     recorded once the verdict is reached, and spec.bounds.concurrencyPolicy is \
                     Forbid",
                    active.name_any()
                ),
                None => format!(
                    "the rehearsal {} from the previous slot has not finished, and \
                     spec.bounds.concurrencyPolicy is Forbid",
                    active.name_any()
                ),
            };
            return Verdict::Skipped(Skip::new(SkipReason::ConcurrencyBlocked, why));
        }
    }
    if let Some(other) = facts.target_busy.as_deref() {
        return Verdict::Skipped(Skip::new(
            SkipReason::TargetBusy,
            format!(
                "the Restore {other} is rehearsing against the same target cluster `{}`; at most \
                 one rehearsal per target cluster runs at a time, because two would race over the \
                 same broker even though their topic names differ",
                spec.target.cluster_ref.name
            ),
        ));
    }

    // ---- 5. the authorization -------------------------------------------
    let authorization = match authorize(facts) {
        Ok(a) => a,
        Err(skip) => return Verdict::Skipped(skip),
    };

    // ---- 6. the target ---------------------------------------------------
    let Some(cluster) = facts.cluster else {
        return Verdict::Skipped(Skip::new(
            SkipReason::TargetUnavailable,
            format!(
                "the KafkaCluster `{}` this schedule targets does not exist in this namespace",
                spec.target.cluster_ref.name
            ),
        ));
    };
    let status = cluster.status.as_ref();
    if status.and_then(|s| s.reachable) != Some(true) {
        return Verdict::Skipped(Skip::new(
            SkipReason::TargetUnavailable,
            format!(
                "the KafkaCluster `{}` does not report status.reachable: true",
                spec.target.cluster_ref.name
            ),
        ));
    }
    let Some(target_cluster_id) = status.and_then(|s| s.cluster_id.clone()) else {
        return Verdict::Skipped(Skip::new(
            SkipReason::TargetUnavailable,
            format!(
                "the KafkaCluster `{}` reports no status.clusterId, and the signed scope names a \
                 cluster ID rather than an object name — `spec.role` is free-form and is not \
                 authority",
                spec.target.cluster_ref.name
            ),
        ));
    };
    // THE TARGET'S SAVED CONNECTION, FROM THE ONE RESOLVER (PLAT-07.1). The
    // `Restore` admission resolves this same object for `RestoreTarget` and
    // refuses a plan whose target block differs from it, so the plan is
    // rendered from this value and a connection that admission would refuse
    // is a recorded skip here rather than a `Failed` child there.
    let target = match crate::connection::resolve(
        cluster,
        crate::connection::ConnectionUse::RestoreTarget,
    ) {
        Ok(target) => target,
        Err(refusal) => {
            return Verdict::Skipped(Skip::new(
                SkipReason::TargetUnavailable,
                format!(
                    "the KafkaCluster `{}` does not resolve to a usable connection ({}): {}",
                    spec.target.cluster_ref.name, refusal.field, refusal
                ),
            ))
        }
    };
    let prefix = rehearsal::rendered_prefix(&spec.target.topic_prefix, facts.uid);
    let expected =
        rehearsal::expected_scope(spec, facts.template_digest, &target_cluster_id, &prefix);
    if let Err(detail) = rehearsal::scope_agrees(&authorization.scope, &expected) {
        return Verdict::Skipped(Skip::new(SkipReason::AuthorizationInvalid, detail));
    }

    // ---- 7. the point ----------------------------------------------------
    let topics = spec.point.topics.clone().unwrap_or_default();
    let selected = match rehearsal::select_point(
        facts.candidates,
        &rehearsal::SelectionRules {
            topics: &topics,
            min_age_seconds: i64::from(spec.point.min_age_seconds),
            max_partitions: u32::try_from(spec.bounds.max_partitions).unwrap_or(u32::MAX),
            target_cluster_id: &target_cluster_id,
        },
        facts.now,
    ) {
        Ok(s) => s,
        Err(mut skip) => {
            if facts.backup_verdicts_incomplete && skip.reason == SkipReason::NoQualifyingPoint {
                skip.detail = format!(
                    "{} (the namespace holds more than {} Backup objects, the most one pass \
                     reads, so no catalog-only point was admitted: a refusal on a Backup the \
                     walk did not reach could not be ruled out — prune the Backup history)",
                    skip.detail,
                    MAX_BACKUP_PAGES * BACKUP_PAGE_LIMIT as usize
                );
            }
            return Verdict::Skipped(skip);
        }
    };

    Verdict::Fire(Box::new(FireOrder {
        slot,
        due,
        selected,
        authorization,
        prefix,
        target_cluster_id,
        target,
    }))
}

/// D3 §4.3(a)–(c): the standing authorization, checked from the schedule's side.
///
/// # Why `Verified=True` is necessary AND not sufficient
///
/// `Verified=True` is the `Approval` reconciler's verdict, which is where the
/// signature is actually checked against the namespace's resolved trust. This
/// function does not re-check crypto — one verifier is the point of the seam —
/// but it does re-check four things a cached verdict cannot answer:
///
/// * the verdict is about THIS schedule object, by UID, not by name (a
///   deleted-and-recreated schedule reuses the name and does not reuse the UID);
/// * the key that verified it may STILL authorise something new today, which is
///   `may_sign_new_for` and not the weaker "has not expired" — a revocation
///   between one slot and the next must stop the next slot;
/// * the key's USAGE is the approver's, never `EvidenceSigning`, so the
///   installation's own signing identity cannot authorise its own rehearsals;
/// * the document has not expired and was not minted with a life longer than D3
///   §4.3 permits.
///
/// # Errors
///
/// A [`Skip`] whose reason is [`SkipReason::AuthorizationInvalid`] or
/// [`SkipReason::AuthorizationExpired`].
pub fn authorize(facts: &Facts<'_>) -> Result<Authorization, Skip> {
    let wanted = &facts.schedule.spec.authorization.standing_approval_ref.name;
    let Some(approval) = facts.approval else {
        return Err(Skip::new(
            SkipReason::AuthorizationInvalid,
            format!(
                "spec.authorization.standingApprovalRef names the Approval `{wanted}`, which does \
                 not exist in this namespace"
            ),
        ));
    };
    let status = approval.status.as_ref();
    if status.and_then(|s| s.verified) != Some(true) {
        return Err(Skip::new(
            SkipReason::AuthorizationInvalid,
            format!("the Approval `{wanted}` is not Verified=True"),
        ));
    }
    if approval.spec.subject_ref.kind != SubjectKind::RehearsalSchedule {
        return Err(Skip::new(
            SkipReason::AuthorizationInvalid,
            format!(
                "the Approval `{wanted}` names subjectRef.kind {} and a standing rehearsal \
                 authorization names RehearsalSchedule",
                approval.spec.subject_ref.kind
            ),
        ));
    }
    // THE UID, AND NOT THE NAME. An `Approval` for a schedule that was deleted
    // and recreated names the same name and a different object; accepting it
    // would let a signed document authorise a template nobody signed.
    let bound = status.and_then(|s| s.verified_subject_ref.as_ref());
    match bound {
        None => {
            return Err(Skip::new(
                SkipReason::AuthorizationInvalid,
                format!(
                    "the Approval `{wanted}` carries no status.verifiedSubjectRef, so there is \
                     nothing that says WHICH object it was verified against"
                ),
            ))
        }
        Some(bound) if bound.uid != facts.uid => {
            return Err(Skip::new(
                SkipReason::AuthorizationInvalid,
                format!(
                    "the Approval `{wanted}` was verified against RehearsalSchedule uid {} and \
                     this object's uid is {}; a standing authorization is bound to one object, \
                     not to one name",
                    bound.uid, facts.uid
                ),
            ))
        }
        Some(bound) if bound.name != facts.schedule.name_any() => {
            return Err(Skip::new(
                SkipReason::AuthorizationInvalid,
                format!(
                    "the Approval `{wanted}` was verified against `{}` and this object is `{}`",
                    bound.name,
                    facts.schedule.name_any()
                ),
            ))
        }
        Some(_) => {}
    }
    if approval.spec.plan_hash != facts.template_digest {
        return Err(Skip::new(
            SkipReason::AuthorizationInvalid,
            format!(
                "the Approval `{wanted}` declares planHash {} and this schedule's sealed spec \
                 hashes to {}",
                approval.spec.plan_hash, facts.template_digest
            ),
        ));
    }
    let Some(key_id) = status.and_then(|s| s.matched_key_id.clone()) else {
        return Err(Skip::new(
            SkipReason::AuthorizationInvalid,
            format!("the Approval `{wanted}` records no status.matchedKeyId"),
        ));
    };
    // THE USAGE, AND `EvidenceSigning` IS NOT ON THE LIST. D3 §7.3's key-usage
    // separation: the installation's own signing identity must never be able to
    // authorise its own rehearsals, which is exactly what a rehearsal signed by
    // the evidence key would be.
    let key = facts.trust.key(&key_id).ok_or_else(|| {
        Skip::new(
            SkipReason::AuthorizationInvalid,
            format!(
                "the Approval `{wanted}` verified under key {key_id}, which the trust this \
                 namespace resolves to does not carry"
            ),
        )
    })?;
    let usage = logweir_core::trust::KeyUsage::GovernedApproval;
    if !key.trust.has_usage(usage) {
        return Err(Skip::new(
            SkipReason::AuthorizationInvalid,
            format!(
                "the Approval `{wanted}` verified under key {key_id}, whose usages are [{}]; a \
                 rehearsal in the current format is authorised only by GovernedApproval; \
                 ConsoleConfirmation requires PLAT-19.2's immutable policy-mode binding and \
                 EvidenceSigning never authorises",
                key.trust
                    .usages
                    .iter()
                    .map(|u| format!("{u:?}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ));
    };
    if let Err(refusal) = facts.trust.may_sign_new_for(&key_id, usage, facts.now) {
        return Err(Skip::new(
            SkipReason::AuthorizationInvalid,
            format!(
                "the Approval `{wanted}` verified under key {key_id}, which may no longer \
                 authorise anything new ({}); the next slot does not run under a withdrawn key",
                refusal.as_str()
            ),
        ));
    }

    // ---- the signed bytes, parsed only now -------------------------------
    let doc: wire::StandingAuthorization = serde_json::from_str(&approval.spec.approval_bytes)
        .map_err(|e| {
            Skip::new(
                SkipReason::AuthorizationInvalid,
                format!(
                    "the Approval `{wanted}` verified, but its bytes are not a standing rehearsal \
                     authorization: {e}"
                ),
            )
        })?;
    if let Err(refusal) = wire::admit_standing_authorization(&doc, Some(facts.uid), facts.now) {
        let reason = if refusal.token == wire::AUTHORIZATION_EXPIRED {
            SkipReason::AuthorizationExpired
        } else {
            SkipReason::AuthorizationInvalid
        };
        return Err(Skip::new(reason, refusal.detail));
    }
    if let Err(detail) = rehearsal::life_within_bound(doc.issued_at, doc.expires_at) {
        return Err(Skip::new(SkipReason::AuthorizationInvalid, detail));
    }
    if doc.subject_ref.namespace != facts.schedule.namespace().unwrap_or_default() {
        return Err(Skip::new(
            SkipReason::AuthorizationInvalid,
            format!(
                "the standing authorization names namespace `{}` and this schedule is in `{}`",
                doc.subject_ref.namespace,
                facts.schedule.namespace().unwrap_or_default()
            ),
        ));
    }

    // A BLANK UID IS NOT A UID. It becomes the contract's mandatory
    // `LOGWEIR_EXECUTION_APPROVAL_UID`, and the runner treats a value that is
    // present and empty as MISSING — so a default here would render a bundle
    // every Job refuses, before phase 0, with a message about the contract
    // rather than about this Approval. Unreachable for an object the API server
    // returned, and named rather than defaulted for exactly that reason.
    let approval_uid = approval
        .uid()
        .filter(|uid| !uid.trim().is_empty())
        .ok_or_else(|| {
            Skip::new(
                SkipReason::AuthorizationInvalid,
                format!("the Approval `{wanted}` carries no metadata.uid"),
            )
        })?;

    Ok(Authorization {
        name: approval.name_any(),
        uid: approval_uid,
        key_id,
        envelope: approval.spec.approval_bytes.clone(),
        sidecar: approval.spec.sidecar_bytes.clone(),
        scope: doc.scope,
    })
}

// ===========================================================================
// The keyring — D3 §4.3(e)'s "the trusted public keys"
// ===========================================================================

/// The [`wire::AuthorizationKeyring`] this namespace's resolved trust yields.
///
/// # Lifecycle is the controller's and stays here
///
/// The keyring carries KEY MATERIAL and no lifecycle: `state`,
/// `notBefore`/`notAfter` and revocation are `trust::decide`'s, and they are
/// evaluated HERE, before these bytes are written. What the runner re-checks is
/// what a credential-less process can: that a signature verifies under a key the
/// controller PINNED, and that the key carries a usage allowed to authorise.
/// Writing a retired key into this file would be writing a key the controller
/// had already refused.
///
/// Only keys that may authorise TODAY are rendered, and the key the `Approval`
/// verified under must be among them or the caller has already refused.
#[must_use]
pub fn keyring(
    trust: &crate::trust::ResolvedTrust,
    now: DateTime<Utc>,
) -> wire::AuthorizationKeyring {
    let mut keys: Vec<wire::AuthorizationKey> = Vec::new();
    let usage = logweir_core::trust::KeyUsage::GovernedApproval;
    for key in trust.keys_for(usage) {
        if trust
            .may_sign_new_for(&key.trust.key_id, usage, now)
            .is_err()
        {
            continue;
        }
        keys.push(wire::AuthorizationKey {
            key_id: key.trust.key_id.clone(),
            public_key_pem: key.spki_pem.clone(),
            usages: vec![usage],
        });
    }
    wire::AuthorizationKeyring {
        format_version: KEYRING_FORMAT_VERSION.to_string(),
        keys,
    }
}

// ===========================================================================
// The child Restore
// ===========================================================================

/// The `Restore` one firing slot creates.
///
/// # Errors
///
/// [`rehearsal::NameError`] when the composed name would not fit.
pub fn child_restore(
    schedule: &RehearsalSchedule,
    uid: &str,
    order: &FireOrder,
    plan_bytes: String,
    destination: &str,
) -> Result<Restore, rehearsal::NameError> {
    let spec = &schedule.spec;
    let schedule_name = schedule.name_any();
    let name = rehearsal::restore_name(&schedule_name, &order.slot)?;
    let labels: BTreeMap<String, String> = [
        (rehearsal::SCHEDULE_LABEL.to_string(), schedule_name.clone()),
        (
            rehearsal::TARGET_LABEL.to_string(),
            spec.target.cluster_ref.name.clone(),
        ),
        (rehearsal::SLOT_LABEL.to_string(), order.slot.clone()),
    ]
    .into_iter()
    .collect();
    let annotations: BTreeMap<String, String> = [
        (
            rehearsal::SIZE_BASIS_ANNOTATION.to_string(),
            order.selected.size_basis.to_string(),
        ),
        // **THE APPROVAL'S UID, PINNED ON THE CHILD — PLAT-14.3b fix round 1.**
        //
        // `spec.authorization.approvalRef` is a `LocalRef` and carries only a
        // NAME (D3 W0's CRD), so the `Restore` reconciler resolves the standing
        // `Approval` by name and would accept a DIFFERENT object that later
        // took the same name. This annotation is the UID of the object THIS
        // slot actually authorised against, written by the only component that
        // knows it, and `restore::admit` requires the `Approval` it resolves to
        // carry it. It closes the window between this write and the first
        // bundle write, after which the bundle's own immutable
        // `logweir.dev/approval-uid` already makes a substitution a terminal
        // `ApprovalBundleConflict`. The same annotation KEY is used on purpose:
        // one name for one fact.
        (
            super::restore::BUNDLE_APPROVAL_UID_ANNOTATION.to_string(),
            order.authorization.uid.clone(),
        ),
    ]
    .into_iter()
    .collect();
    Ok(Restore {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: schedule.namespace(),
            labels: Some(labels),
            annotations: Some(annotations),
            // `blockOwnerDeletion: false` — D3 §4.4: deleting the schedule
            // collects its Restore CRs and never the signed evidence, and a
            // blocking owner would make the schedule undeletable while a
            // rehearsal was in flight.
            owner_references: Some(vec![
                k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
                    api_version: RehearsalSchedule::api_version(&()).to_string(),
                    kind: RehearsalSchedule::kind(&()).to_string(),
                    name: schedule_name,
                    uid: uid.to_string(),
                    controller: Some(true),
                    block_owner_deletion: Some(false),
                },
            ]),
            ..ObjectMeta::default()
        },
        spec: RestoreSpec {
            plan_bytes,
            // ABSENT, AND THAT IS THE ROLLBACK BEHAVIOUR. An older controller
            // reading this object resolves the empty name to nothing and
            // refuses terminally with `ApprovalNotReceived` — fail closed.
            approval_ref: None,
            authorization: Some(RestoreAuthorization {
                kind: AuthorizationKind::Standing,
                approval_ref: LocalRef {
                    name: order.authorization.name.clone(),
                },
                rehearsal_schedule_ref: LocalRef {
                    name: schedule.name_any(),
                },
            }),
            source_archive: ArchiveRef {
                url: format!("logweir-destination://{destination}"),
                secret_ref: None,
            },
            source_destination_ref: Some(LocalRef {
                name: destination.to_string(),
            }),
            evidence_destination_ref: Some(LocalRef {
                name: destination.to_string(),
            }),
            backup_set_ref: order.selected.point.backup_id.clone(),
            point_in_time: order.selected.point_in_time(),
            target: RestoreTarget {
                cluster_ref: spec.target.cluster_ref.clone(),
                mode: TargetMode::Scratch,
                topic_naming: TopicNaming {
                    prefix: order.prefix.clone(),
                },
            },
            deadline_seconds: i64::from(spec.bounds.deadline_seconds),
            runner_resources: spec.bounds.runner_resources.clone(),
        },
        status: None,
    })
}

// ===========================================================================
// Reconcile
// ===========================================================================

/// What one pass did, for the caller's requeue and for a test to assert on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    /// The verdict [`decide`] reached.
    pub verdict: Verdict,
    /// The `Restore` this pass created, if any.
    pub created: Option<String>,
    /// How long until the next look.
    pub requeue_seconds: u64,
}

fn idle() -> Outcome {
    Outcome {
        verdict: Verdict::Idle,
        created: None,
        requeue_seconds: REQUEUE_SECONDS,
    }
}

/// Reconcile one `RehearsalSchedule`.
///
/// # Errors
///
/// [`ReconcileError`] for an API failure. Every verdict — including every
/// refusal — is an `Ok`, because a refusal is an answer and a requeue storm is
/// not.
#[allow(clippy::too_many_lines)]
pub async fn reconcile_schedule(
    schedule: &RehearsalSchedule,
    ctx: &Context,
    now: DateTime<Utc>,
) -> Result<Outcome, ReconcileError> {
    let name = schedule.name_any();
    let Some(namespace) = schedule.namespace() else {
        warn!(schedule = %name, "RehearsalSchedule carries no metadata.namespace; nothing is done");
        return Ok(idle());
    };
    let Some(uid) = schedule.uid() else {
        warn!(
            schedule = %name,
            namespace = %namespace,
            "RehearsalSchedule carries no metadata.uid; the rendered topic prefix and the signed \
             subject binding are both functions of it, so nothing is done"
        );
        return Ok(idle());
    };
    let api: Api<RehearsalSchedule> = Api::namespaced(ctx.client.clone(), &namespace);

    let template_digest = match rehearsal::template_digest(&schedule.spec) {
        Ok(d) => d,
        Err(e) => {
            warn!(schedule = %name, error = %e, "the sealed spec could not be canonicalised");
            return Ok(idle());
        }
    };

    // ---- THE TWO ANSWERS THAT NEED NO READ AT ALL ------------------------
    //
    // Suspension and leftover topics are decided from the object in hand, and
    // they are decided HERE rather than inside `decide` so that a suspended
    // schedule costs the API server nothing: a controller that resolved trust,
    // fetched an Approval, a KafkaCluster, a Restore list and a Backup list
    // every thirty seconds for a schedule an operator switched off would make
    // `suspend` the most expensive field on the object. `decide` re-checks both
    // — they are part of its stated order and its tests assert it — so the two
    // can never disagree.
    if schedule.spec.suspend || schedule.status.as_ref().is_some_and(has_pending_topics) {
        let verdict = match rehearsal::cleanup_clear(schedule) {
            Ok(()) => Verdict::Idle,
            Err(skip) => Verdict::Skipped(skip),
        };
        let verdict = if schedule.spec.suspend {
            Verdict::Idle
        } else {
            verdict
        };
        let slot = match verdict {
            Verdict::Skipped(_) => due_unconsumed_slot(schedule, now),
            _ => None,
        };
        commit(
            &api,
            schedule,
            &StatusUpdate {
                verdict: verdict.clone(),
                slot,
                next_fire: next_fire(schedule, now),
                observation: Observation::default(),
                template_digest,
                created: None,
            },
            now,
        )
        .await?;
        return Ok(Outcome {
            verdict,
            created: None,
            requeue_seconds: REQUEUE_SECONDS,
        });
    }

    // ---- what the previous slot's child says -----------------------------
    let restores: Api<Restore> = Api::namespaced(ctx.client.clone(), &namespace);
    let previous = previous_child(schedule, &restores).await?;
    let observation = observe(previous.as_ref(), now);

    // ---- the facts -------------------------------------------------------
    let trust = match crate::trust::resolve(&ctx.client, &namespace)
        .await
        .map_err(ReconcileError::Api)?
    {
        crate::trust::Resolution::Trust(trust) => *trust,
        crate::trust::Resolution::Conflict { policies, .. } => {
            let skip = Skip::new(
                SkipReason::AuthorizationInvalid,
                crate::verification::trust_policy_conflict_detail(&namespace, &policies),
            );
            return commit(
                &api,
                schedule,
                &StatusUpdate {
                    verdict: Verdict::Skipped(skip.clone()),
                    slot: due_unconsumed_slot(schedule, now),
                    next_fire: next_fire(schedule, now),
                    observation,
                    template_digest,
                    created: None,
                },
                now,
            )
            .await
            .map(|()| Outcome {
                verdict: Verdict::Skipped(skip),
                created: None,
                requeue_seconds: REQUEUE_SECONDS,
            });
        }
        crate::trust::Resolution::Unconfigured => {
            let skip = Skip::new(
                SkipReason::AuthorizationInvalid,
                crate::verification::NO_POLICY_KEYS_DETAIL.to_string(),
            );
            return commit(
                &api,
                schedule,
                &StatusUpdate {
                    verdict: Verdict::Skipped(skip.clone()),
                    slot: due_unconsumed_slot(schedule, now),
                    next_fire: next_fire(schedule, now),
                    observation,
                    template_digest,
                    created: None,
                },
                now,
            )
            .await
            .map(|()| Outcome {
                verdict: Verdict::Skipped(skip),
                created: None,
                requeue_seconds: REQUEUE_SECONDS,
            });
        }
    };

    let approvals: Api<Approval> = Api::namespaced(ctx.client.clone(), &namespace);
    let approval = approvals
        .get_opt(&schedule.spec.authorization.standing_approval_ref.name)
        .await
        .map_err(ReconcileError::Api)?;
    let clusters: Api<KafkaCluster> = Api::namespaced(ctx.client.clone(), &namespace);
    let cluster = clusters
        .get_opt(&schedule.spec.target.cluster_ref.name)
        .await
        .map_err(ReconcileError::Api)?;
    let target_busy = busy_target(&restores, schedule, &name).await?;
    let (candidates, backup_verdicts_incomplete) =
        candidates(ctx, schedule, &namespace, now).await?;

    let facts = Facts {
        schedule,
        uid: &uid,
        template_digest: &template_digest,
        approval: approval.as_ref(),
        trust: &trust,
        cluster: cluster.as_ref(),
        candidates: &candidates,
        backup_verdicts_incomplete,
        active: previous.as_ref(),
        target_busy,
        now,
    };
    let verdict = decide(&facts);

    let mut created = None;
    let mut slot = None;
    let verdict = match verdict {
        Verdict::Fire(order) => {
            slot = Some(order.slot.clone());
            match fire(ctx, schedule, &uid, &namespace, &order, &trust, now).await? {
                Fired::Created(child) => {
                    created = Some(child);
                    Verdict::Fire(order)
                }
                Fired::Refused(skip) => Verdict::Skipped(skip),
            }
        }
        // A SKIP NAMES AND CONSUMES THE SLOT IT REFUSED — see `status_patch`.
        Verdict::Skipped(skip) => {
            slot = due_unconsumed_slot(schedule, now);
            Verdict::Skipped(skip)
        }
        Verdict::Idle => Verdict::Idle,
    };

    commit(
        &api,
        schedule,
        &StatusUpdate {
            verdict: verdict.clone(),
            slot,
            next_fire: next_fire(schedule, now),
            observation,
            template_digest,
            created: created.clone(),
        },
        now,
    )
    .await?;

    Ok(Outcome {
        verdict,
        created,
        requeue_seconds: REQUEUE_SECONDS,
    })
}

/// Whether a status carries topics a previous teardown could not remove.
fn has_pending_topics(status: &crate::crds::rehearsal_schedule::RehearsalScheduleStatus) -> bool {
    status
        .cleanup
        .as_ref()
        .and_then(|c| c.pending_topics.as_ref())
        .is_some_and(|t| !t.is_empty())
}

/// What [`fire`] did.
enum Fired {
    /// The child exists, by this name.
    Created(String),
    /// Something refused after the plan was rendered. NOTHING was created.
    Refused(Skip),
}

/// Render, PROVE, reserve, create — in that order, and the order is the rule.
///
/// D3 §4.3(d): the scope proof runs over the bytes that will be frozen, BEFORE
/// the reservation and before any `POST`. A plan outside the signed scope
/// therefore never reaches a `Restore`, let alone a Job — which is what the
/// `zero ConfigMap and zero Job calls` row of D3 §13 asserts over a route table
/// that HAS those routes present.
async fn fire(
    ctx: &Context,
    schedule: &RehearsalSchedule,
    uid: &str,
    namespace: &str,
    order: &FireOrder,
    trust: &crate::trust::ResolvedTrust,
    now: DateTime<Utc>,
) -> Result<Fired, ReconcileError> {
    // ---- the destination the chosen point lives in ------------------------
    let Some(destination_name) = order.selected.point.destination.clone() else {
        return Ok(Fired::Refused(Skip::new(
            SkipReason::NoQualifyingPoint,
            "the chosen point names no BackupDestination, and a rehearsal renders its plan's \
             storage block from one; a legacy archive URL is not a rehearsal source in this build"
                .to_string(),
        )));
    };
    let destinations: Api<BackupDestination> = Api::namespaced(ctx.client.clone(), namespace);
    let Some(destination) = destinations
        .get_opt(&destination_name)
        .await
        .map_err(ReconcileError::Api)?
    else {
        return Ok(Fired::Refused(Skip::new(
            SkipReason::NoQualifyingPoint,
            format!(
                "the chosen point lives in the BackupDestination `{destination_name}`, which does \
                 not exist in this namespace"
            ),
        )));
    };
    let location = crate::destination::location_of(&destination);

    // ---- the plan ---------------------------------------------------------
    let plan = rehearsal::render_plan(&rehearsal::PlanInputs {
        spec: &schedule.spec,
        selected: &order.selected,
        prefix: &order.prefix,
        archive_storage: location.archive_storage_url(),
        evidence_storage: location.evidence_storage_url(),
        target: &order.target,
        plan_name: format!("rehearsal/{}", schedule.name_any()),
    });
    let plan_bytes = match rehearsal::plan_bytes(&plan) {
        Ok(bytes) => bytes,
        Err(e) => {
            return Ok(Fired::Refused(Skip::new(
                SkipReason::AuthorizationInvalid,
                format!("the rendered rehearsal plan could not be serialised: {e}"),
            )))
        }
    };

    // ---- D3 §4.3(d): plan ∈ scope, over the bytes that will be frozen -----
    //
    // The allowlist is EXACTLY the signed target cluster id, which is what
    // turns "the signed scope names cluster X" into "this run cannot reach
    // anything but X" without a second broker round trip (W5's report, R1.3
    // obligation 1).
    let allowed = AllowedClusters {
        allowed_cluster_ids: vec![order.authorization.scope.target_cluster_id.clone()],
        source_cluster_id: None,
    };
    let facts = wire::plan_scope_facts(&plan, &allowed);
    if let Err(refusal) = wire::plan_within_scope(&facts, &order.authorization.scope) {
        return Ok(Fired::Refused(Skip::new(
            SkipReason::AuthorizationInvalid,
            refusal.to_string(),
        )));
    }
    let child = match child_restore(schedule, uid, order, plan_bytes, &destination_name) {
        Ok(c) => c,
        Err(e) => {
            return Ok(Fired::Refused(Skip::new(
                SkipReason::AuthorizationInvalid,
                e.to_string(),
            )))
        }
    };
    let child_name = child.name_any();

    // ---- PLAT-04.1's reservation, then the deterministic child ------------
    //
    // A merge PATCH with `metadata.resourceVersion` as the precondition, never
    // `replace_status` (seam S7). A controller that dies between the two finds
    // the reservation on restart and resumes exactly this name, because the
    // name is a pure function of the trigger.
    let api: Api<RehearsalSchedule> = Api::namespaced(ctx.client.clone(), namespace);
    let body = status_patch_with_preconditions(
        schedule,
        json!({
            "status": {
                "pendingRestoreRef": { "name": child_name },
                "lastScheduledSlot": order.slot,
            }
        }),
    )?;
    match api
        .patch_status(
            &schedule.name_any(),
            &PatchParams::default(),
            &Patch::Merge(body),
        )
        .await
    {
        Ok(_) => {}
        Err(kube::Error::Api(e)) if e.code == 409 => {
            // ANOTHER RECONCILE RESERVED FIRST. Nothing is created: the winner
            // creates the child, and this pass reads it on the next look.
            debug!(
                schedule = %schedule.name_any(),
                "the reservation lost a 409; the winning pass owns this slot"
            );
            return Ok(Fired::Refused(Skip::new(
                SkipReason::ConcurrencyBlocked,
                "another reconcile reserved this slot first".to_string(),
            )));
        }
        Err(e) => return Err(ReconcileError::Api(e)),
    }

    let restores: Api<Restore> = Api::namespaced(ctx.client.clone(), namespace);
    // THE OBJECT THE API SERVER RETURNED, and not the one this pass composed.
    // The bundle's `ownerReferences` and its four binding annotations are built
    // from `metadata.uid`, which only exists after the create — a bundle owned
    // by an empty UID would be collected immediately and would bind nothing.
    let created = match restores.create(&PostParams::default(), &child).await {
        Ok(created) => created,
        Err(kube::Error::Api(e)) if e.code == 409 => {
            // The deterministic name already exists, which is the reservation
            // resuming after a crash. It is the same object this pass intended,
            // and it is re-read so the bundle binds the UID that actually
            // exists rather than the one this pass would have created.
            debug!(restore = %child_name, "the deterministic rehearsal Restore already exists");
            match restores
                .get_opt(&child_name)
                .await
                .map_err(ReconcileError::Api)?
            {
                Some(existing) => existing,
                None => return Err(ReconcileError::NoUid(child_name)),
            }
        }
        Err(e) => return Err(ReconcileError::Api(e)),
    };

    // ---- the bundle, owned by the child it authorises ---------------------
    write_bundle(ctx, namespace, &created, &order.authorization, trust, now).await?;

    info!(
        schedule = %schedule.name_any(),
        namespace = %namespace,
        restore = %child_name,
        slot = %order.slot,
        point = %order.selected.point.point_id,
        "a rehearsal slot fired"
    );
    Ok(Fired::Created(child_name))
}

/// Write the standing bundle for one rehearsal `Restore`.
///
/// It goes through [`super::restore::approval_bundle_config_map`] — the SAME
/// function the per-run approval path uses, extended with its standing arm —
/// and never a second renderer. Two renderers would be two answers to "what did
/// the controller commit this run to", and the digests in the Job template are
/// computed from whichever one ran.
async fn write_bundle(
    ctx: &Context,
    namespace: &str,
    restore: &Restore,
    authorization: &Authorization,
    trust: &crate::trust::ResolvedTrust,
    now: DateTime<Utc>,
) -> Result<(), ReconcileError> {
    let keyring = keyring(trust, now);
    let desired = match super::restore::standing_bundle_config_map(
        restore,
        &super::restore::StandingInputs {
            envelope: &authorization.envelope,
            sidecar: &authorization.sidecar,
            approval_uid: &authorization.uid,
            key_id: &authorization.key_id,
            keyring: &keyring,
            target_cluster_id: &authorization.scope.target_cluster_id,
        },
        trust,
    ) {
        Ok(map) => map,
        Err(e) => {
            // The bundle is the last thing written before a runner could
            // execute; a bundle that cannot be rendered means nothing runs, and
            // the `Restore` holds. It is a warn and not an error because a
            // requeue cannot fix a document.
            warn!(
                restore = %restore.name_any(),
                error = %e,
                "the standing approval bundle could not be rendered; the Restore holds"
            );
            return Ok(());
        }
    };
    let maps: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), namespace);
    match maps.create(&PostParams::default(), &desired).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 409 => {
            let name = desired.name_any();
            let existing = maps.get_opt(&name).await.map_err(ReconcileError::Api)?;
            let uid = restore.uid().unwrap_or_default();
            if existing
                .as_ref()
                .is_some_and(|m| super::restore::compatible_approval_bundle(m, &desired, &uid))
            {
                Ok(())
            } else {
                warn!(
                    restore = %restore.name_any(),
                    bundle = %name,
                    "a ConfigMap of the bundle's name exists and is not the bundle this rehearsal \
                     intended; nothing is overwritten"
                );
                Ok(())
            }
        }
        Err(e) => Err(ReconcileError::Api(e)),
    }
}

// ===========================================================================
// Reading the world
// ===========================================================================

/// The `Restore` this schedule's status names, by GET of its recorded name.
///
/// **BY GET AND NEVER BY LIST** (D3 §4.4). A list with a label selector answers
/// "which objects carry this label", which is a different question from "which
/// object did this schedule reserve" — and the two differ exactly when it
/// matters, which is after a crash between the reservation and the create.
async fn previous_child(
    schedule: &RehearsalSchedule,
    restores: &Api<Restore>,
) -> Result<Option<Restore>, ReconcileError> {
    let status = schedule.status.as_ref();
    let named = status
        .and_then(|s| s.active_restore_ref.as_ref())
        .or_else(|| status.and_then(|s| s.pending_restore_ref.as_ref()));
    let Some(reference) = named else {
        return Ok(None);
    };
    restores
        .get_opt(&reference.name)
        .await
        .map_err(ReconcileError::Api)
}

/// How many pages [`busy_target`] follows before it gives up and says so.
///
/// Bounded because a reconcile may not become an unbounded walk, and generous
/// because the page size below is already large.
pub const MAX_TARGET_PAGES: u32 = 32;

/// The page size [`busy_target`] asks for.
pub const TARGET_PAGE_SIZE: u32 = 200;

/// Another schedule's unfinished rehearsal against this same target cluster.
///
/// THE ONE LIST IN THIS FILE, and it is the one question a GET cannot answer:
/// "is anybody else using this broker". It is narrowed by the target label, so
/// it reads only rehearsal `Restore`s, and it excludes this schedule's own.
///
/// # It FOLLOWS THE CONTINUE TOKEN, because one capped page is a guard that
/// stops guarding
///
/// A rehearsal child is named `logweir-rehearsal-<schedule>-<YYYYmmdd-HHMMSS>`
/// and the API server returns a label-selected list in NAME order, so one
/// capped page holds the OLDEST rehearsals against this cluster. Nothing prunes
/// them — they are owned by the schedule with `blockOwnerDeletion: false` and
/// are kept as the audit trail — so once a target carries more than a page of
/// them the page contains only finished runs and the one still IN FLIGHT is
/// exactly the object outside it. `TargetBusy` would then stop being raised and
/// two rehearsals would run against the same broker: the race D3 §4.4 exists to
/// forbid, arriving silently after about two months of a daily schedule. A
/// guard that quietly stops guarding is worse than no guard at all.
///
/// The walk stops at the FIRST non-terminal hit — the question is "is anybody
/// else running", not "how many" — so the ordinary case costs one round trip.
///
/// # And it is still bounded
///
/// At [`MAX_TARGET_PAGES`] pages the walk stops, reports `None` and LOGS that
/// it did not finish. That is the one case in which this function can be wrong,
/// and it is recorded rather than hidden: an installation with 6 400 retained
/// rehearsals against one cluster has a history-retention problem, not a
/// concurrency question this function can answer.
async fn busy_target(
    restores: &Api<Restore>,
    schedule: &RehearsalSchedule,
    schedule_name: &str,
) -> Result<Option<String>, ReconcileError> {
    let selector = format!(
        "{}={}",
        rehearsal::TARGET_LABEL,
        schedule.spec.target.cluster_ref.name
    );
    let mut token: Option<String> = None;
    for page in 0..MAX_TARGET_PAGES {
        let mut params = ListParams::default()
            .labels(&selector)
            .limit(TARGET_PAGE_SIZE);
        if let Some(token) = token.as_deref() {
            params = params.continue_token(token);
        }
        let list = restores.list(&params).await.map_err(ReconcileError::Api)?;
        let hit = list.items.iter().find(|r| {
            r.labels()
                .get(rehearsal::SCHEDULE_LABEL)
                .map(String::as_str)
                != Some(schedule_name)
                && !super::restore::status_is_terminal(r)
        });
        if let Some(hit) = hit {
            return Ok(Some(hit.name_any()));
        }
        match list.metadata.continue_.filter(|t| !t.is_empty()) {
            Some(next) => token = Some(next),
            None => return Ok(None),
        }
        if page + 1 == MAX_TARGET_PAGES {
            warn!(
                schedule = %schedule_name,
                target = %schedule.spec.target.cluster_ref.name,
                pages = MAX_TARGET_PAGES,
                "the per-target concurrency walk hit its page bound without reaching the end of \
                 the list; TargetBusy is not raised from an incomplete walk, and the retained \
                 rehearsal history for this cluster wants pruning"
            );
        }
    }
    Ok(None)
}

/// Every candidate point, from the `Backup`s the schedule's `scheduleRefs` name
/// and from the catalog's materialised view.
///
/// The two sources are MERGED BY POINT ID, and the catalog wins on the
/// availability and verification axes: it is the one that actually listed the
/// archive, and a `Backup` that succeeded says nothing about whether its objects
/// are still there.
///
/// The `bool` is "the `Backup` walk hit its bound", which [`decide`] names in
/// a `NoQualifyingPoint` skip.
async fn candidates(
    ctx: &Context,
    schedule: &RehearsalSchedule,
    namespace: &str,
    now: DateTime<Utc>,
) -> Result<(Vec<PointCandidate>, bool), ReconcileError> {
    let mut by_id: BTreeMap<String, PointCandidate> = BTreeMap::new();
    let mut refusals = crate::catalog_view::ControllerRefusals::default();

    // ---- the Backups ------------------------------------------------------
    //
    // Listed for `catalogRef` as well as for `scheduleRefs`: a catalog-only
    // point whose `Backup` the controller refused is refused too, whichever
    // schedule produced it (`catalog_view::ControllerRefusals`).
    //
    // EVERY PAGE, BOUNDED (review L1). The API server lists in NAME order, and
    // a schedule's Backups sort chronologically, so one capped page held the
    // OLDEST runs and missed exactly the newest — the ones `NewestAvailable`
    // picks. The walk follows the continue token up to
    // [`MAX_BACKUP_PAGES`] × [`BACKUP_PAGE_LIMIT`]; a walk the bound cuts
    // short marks the refusal set incomplete, and then no catalog-only row is
    // selectable (the skip says why). Candidates keep the NEWEST
    // [`MAX_BACKUPS_SCANNED`] by recovery point, not the first in name order.
    //
    // LENIENT (review M1). Objects are read untyped: a `Backup` this build
    // cannot type still contributes its verdict (through
    // `BackupVerdictFacts`) and is simply not a candidate, rather than failing
    // the whole pass.
    let schedule_refs = schedule.spec.point.schedule_refs.as_ref();
    if schedule_refs.is_some() || schedule.spec.point.catalog_ref.is_some() {
        let resource = kube::api::ApiResource::erase::<Backup>(&());
        let backups: Api<kube::api::DynamicObject> =
            Api::namespaced_with(ctx.client.clone(), namespace, &resource);
        let wanted: Vec<&str> = schedule_refs
            .into_iter()
            .flatten()
            .map(|r| r.name.as_str())
            .collect();
        let mut facts: Vec<crate::catalog_view::BackupVerdictFacts> = Vec::new();
        let mut from_backups: Vec<PointCandidate> = Vec::new();
        let mut token: Option<String> = None;
        let mut complete = false;
        for _ in 0..MAX_BACKUP_PAGES {
            let mut params = ListParams::default().limit(BACKUP_PAGE_LIMIT);
            if let Some(cursor) = token.as_deref() {
                params = params.continue_token(cursor);
            }
            let page = backups.list(&params).await.map_err(ReconcileError::Api)?;
            token = page.metadata.continue_.clone().filter(|t| !t.is_empty());
            for object in page.items {
                facts.push(crate::catalog_view::BackupVerdictFacts::from_json(
                    &object.data,
                ));
                let Ok(backup) =
                    serde_json::to_value(&object).and_then(serde_json::from_value::<Backup>)
                else {
                    continue;
                };
                let from = backup
                    .spec
                    .schedule_ref
                    .as_ref()
                    .map(|s| s.name.as_str())
                    .unwrap_or_default();
                if !wanted.contains(&from) {
                    continue;
                }
                if let Some(candidate) = candidate_from_backup(&backup) {
                    from_backups.push(candidate);
                }
            }
            if token.is_none() {
                complete = true;
                break;
            }
        }
        refusals = crate::catalog_view::ControllerRefusals::from_facts(facts);
        if !complete {
            warn!(
                schedule = %schedule.name_any(),
                namespace = %namespace,
                pages = MAX_BACKUP_PAGES,
                "the Backup walk hit its page bound; no catalog-only point is selectable from \
                 an incomplete refusal set"
            );
            refusals = refusals.incomplete();
        }
        from_backups.sort_by(|a, b| {
            b.recovery_point_at
                .cmp(&a.recovery_point_at)
                .then_with(|| a.point_id.cmp(&b.point_id))
        });
        for candidate in from_backups.into_iter().take(MAX_BACKUPS_SCANNED) {
            by_id.insert(candidate.point_id.clone(), candidate);
        }
    }

    // ---- the catalog view -------------------------------------------------
    if let Some(reference) = schedule.spec.point.catalog_ref.as_ref() {
        let catalogs: Api<RecoveryCatalog> = Api::namespaced(ctx.client.clone(), namespace);
        let Some(catalog) = catalogs
            .get_opt(&reference.name)
            .await
            .map_err(ReconcileError::Api)?
        else {
            return Ok((by_id.into_values().collect(), !refusals.is_complete()));
        };
        let status = catalog.status.as_ref();
        // AN EXPIRED VIEW IS NOT A VIEW. Reading its pages past
        // `viewExpiresAt` would answer "the point is there" from a listing taken
        // before a retention run.
        if status
            .and_then(|s| s.view_expires_at)
            .is_none_or(|at| at <= now)
        {
            return Ok((by_id.into_values().collect(), !refusals.is_complete()));
        }
        let destination = catalog
            .spec
            .destination_ref
            .as_ref()
            .map(|d| d.name.clone());
        let pages: Vec<String> = status
            .and_then(|s| s.pages.as_ref())
            .map(|pages| {
                pages
                    .iter()
                    .take(MAX_CATALOG_PAGES)
                    .map(|p| p.config_map_name.clone())
                    .collect()
            })
            .unwrap_or_default();
        let maps: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), namespace);
        for page in &pages {
            let Some(map) = maps.get_opt(page).await.map_err(ReconcileError::Api)? else {
                continue;
            };
            let Some(body) = map
                .data
                .as_ref()
                .and_then(|d| d.get(crate::catalog_view::PAGE_DATA_KEY))
            else {
                continue;
            };
            for line in body.lines().filter(|l| !l.trim().is_empty()) {
                let Ok(entry) = serde_json::from_str::<crate::catalog_view::ViewEntry>(line) else {
                    continue;
                };
                merge_catalog_entry(&mut by_id, entry, destination.clone(), &refusals);
            }
        }
    }

    Ok((by_id.into_values().collect(), !refusals.is_complete()))
}

/// A `Backup` object as a candidate point, or `None` when it is not one.
#[must_use]
pub fn candidate_from_backup(backup: &Backup) -> Option<PointCandidate> {
    let status = backup.status.as_ref()?;
    if status.phase.as_deref() != Some("Succeeded") || status.exit_code != Some(0) {
        return None;
    }
    let evidence = status.evidence.as_ref();
    let receipt_sha256 = evidence.and_then(|e| e.receipt_sha256.clone())?;
    let point_id = point_id_from_receipt_digest(&receipt_sha256)?;
    // `capture` is receipt-derived and therefore absent when verification was
    // NotAttempted. Keep the Backup as a joinable candidate in that state: a
    // matching catalog row below supplies the authoritative capture/window and
    // selectability axes. A real API object always has creationTimestamp; it is
    // only a non-selectable placeholder until that merge happens.
    let receipt_capture_at = status.capture.as_ref().and_then(|c| c.started_at);
    let needs_catalog_capture = receipt_capture_at.is_none();
    let recovery_point_at =
        receipt_capture_at.or_else(|| backup.metadata.creation_timestamp.as_ref().map(|t| t.0))?;
    let covered = status.window_covered.as_ref().map(|w| Window {
        from_ms: w.from_ms,
        to_ms: w.to_ms,
    });
    let verification = evidence.and_then(|e| e.verification.as_ref());
    let verdict = verification.and_then(|v| v.result.as_deref());
    // A `Valid` IS READ WITH ITS BASIS (TRUST-VALID-BASIS-CLASS): the one rule
    // the controller's badge uses, so a `Valid` + `Unverified` an interim
    // build left on a lab object is not selectable on its own, and a `Valid`
    // on `RecordedBeforeRevocation` or `None` is a refusal.
    let basis =
        crate::verification::ValidBasis::of_block(verification.and_then(|v| v.trust.as_ref()));
    // A VERDICT THE CONTROLLER REACHED STILL DECIDES. Only "I could not look"
    // (`NotAttempted`, `Pending`, `Valid` on `Unverified`, or no verdict at
    // all) defers to the catalog; a passing `Valid` is a pass the catalog may
    // still narrow. Everything else — `Invalid`, `Untrusted`, a `Valid` on a
    // basis this installation does not accept, or a spelling this build does
    // not know — is a refusal no catalog row may overrule
    // (`protection::evidence_objective_met`).
    let verdict_refused = crate::catalog_view::is_reached_refusal(verdict, basis);
    Some(PointCandidate {
        point_id,
        backup_id: status.backup_id.clone()?,
        backup_name: Some(backup.name_any()),
        recovery_point_at,
        covered,
        // Backup.spec is immutable. For named selection this is the exact list
        // frozen into the runner plan; retaining it across the catalog merge is
        // what proves D3 §4.2's `topics ⊆ point.topics` filter. Dynamic mode's
        // spec list is deliberately empty, so it makes no invented claim.
        topics: Some(backup.spec.topics.clone()),
        partitions: None,
        receipt_key: evidence
            .and_then(|e| e.receipt_key.clone())
            .unwrap_or_default(),
        receipt_sha256,
        manifest_sha256: status.manifest_sha256.clone(),
        destination: backup.spec.destination_ref.as_ref().map(|d| d.name.clone()),
        source_cluster_id: None,
        // A `Backup` on its own establishes that the run succeeded, never that
        // its archive objects are still readable — that is the catalog's axis,
        // and it overwrites this below when a catalog is consulted.
        selectable: !needs_catalog_capture
            && crate::verification::stored_result_is_pass(verdict, basis),
        verdict_refused,
        retention_lease: false,
    })
}

/// D3 §5.1's point identity, derived from the receipt digest the controller
/// already recorded — the same derivation `protection.rs` makes, and it must
/// stay the same or a `Backup`-derived candidate and a catalog entry for the
/// same point would not merge.
#[must_use]
pub fn point_id_from_receipt_digest(digest: &str) -> Option<String> {
    let hex = digest.strip_prefix("sha256:")?;
    if hex.len() < 32 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Some(format!("lwp1-{}", &hex[..32]))
}

/// Fold one catalog view entry into the candidate set.
///
/// `refusals` are the reached refusals among the `Backup`s this pass listed.
/// A row they name is never selectable — as a catalog-only candidate too,
/// where no `Backup` candidate of this schedule carries the verdict.
pub fn merge_catalog_entry(
    by_id: &mut BTreeMap<String, PointCandidate>,
    entry: crate::catalog_view::ViewEntry,
    destination: Option<String>,
    refusals: &crate::catalog_view::ControllerRefusals,
) {
    let refused_elsewhere = refusals.refusal_for(&entry).is_some();
    // An IN-RANGE capture time or none. `0` is what a row that never wrote the
    // field would carry, not a capture in 1970 (the protection join reads it
    // the same way), and a value chrono cannot represent is not a time either.
    let recovery_point_at = Some(entry.recovery_point_at_ms)
        .filter(|ms| *ms > 0)
        .and_then(DateTime::from_timestamp_millis);
    match by_id.get_mut(&entry.point_id) {
        Some(existing) => {
            // `pointId` deliberately carries only the first 128 digest bits.
            // It is an index, not the equality proof: a catalog row may enrich
            // this Backup only when the full receipt digest agrees.
            if entry.receipt_sha256 != existing.receipt_sha256 {
                return;
            }
            // A REFUSAL THE CONTROLLER REACHED IS NOT THE CATALOG'S TO UNDO.
            // The view is served until `viewExpiresAt`, so a row harvested
            // before the receipt was replaced or its signer revoked still says
            // `selectable`; it supplies neither selectability nor the capture
            // facts of a point the controller refused.
            if existing.verdict_refused || refused_elsewhere {
                existing.verdict_refused = true;
                existing.selectable = false;
                return;
            }
            // THE CATALOG DECIDES SELECTABILITY, because it is the axis it
            // actually measured: it listed the archive and re-evaluated trust.
            // It decides only together with an in-range capture time: a row
            // that cannot place the point leaves the creationTimestamp
            // placeholder in place, and a placeholder is never selectable.
            let Some(at) = recovery_point_at else {
                existing.selectable = false;
                return;
            };
            existing.selectable = entry.selectable;
            existing.recovery_point_at = at;
            existing.covered = Some(Window {
                from_ms: entry.covered_from_ms,
                to_ms: entry.covered_to_ms,
            });
            if existing.manifest_sha256.is_none() {
                existing.manifest_sha256 = entry.manifest_sha256.clone();
            }
            if existing.receipt_key.is_empty() {
                existing.receipt_key = entry.receipt_key.clone();
            }
            if existing.destination.is_none() {
                existing.destination = destination;
            }
        }
        None => {
            let Some(at) = recovery_point_at else { return };
            by_id.insert(
                entry.point_id.clone(),
                PointCandidate {
                    point_id: entry.point_id,
                    backup_id: entry.backup_id,
                    backup_name: None,
                    recovery_point_at: at,
                    covered: Some(Window {
                        from_ms: entry.covered_from_ms,
                        to_ms: entry.covered_to_ms,
                    }),
                    // THE CATALOG RECORDS NO TOPIC LIST. `select_point` refuses
                    // such a candidate rather than assuming the subset claim —
                    // see `rehearsal::PointCandidate::topics`.
                    topics: None,
                    partitions: None,
                    receipt_key: entry.receipt_key,
                    receipt_sha256: entry.receipt_sha256,
                    manifest_sha256: entry.manifest_sha256,
                    destination,
                    source_cluster_id: None,
                    // A CATALOG-ONLY ROW IS STILL NOT THE CATALOG'S TO DECIDE
                    // when a `Backup` for the same receipt was refused.
                    // Nor when the refusal set is incomplete: "no listed
                    // Backup refused it" is then not "no Backup refused it".
                    selectable: entry.selectable && !refused_elsewhere && refusals.is_complete(),
                    verdict_refused: refused_elsewhere,
                    retention_lease: false,
                },
            );
        }
    }
}

// ===========================================================================
// Observing the previous slot's result
// ===========================================================================

/// How long a FINISHED rehearsal may wait for its evidence verdict before the
/// schedule records it anyway — as NOT passed, with
/// [`REASON_VERDICT_NOT_REACHED`] when the run itself exited 0.
///
/// An hour, and it is a backstop rather than the expected path. A
/// destination-backed `Restore` owes an evidence-fetch Job after it turns
/// terminal (D2 §3.9): at most [`crate::evidence_fetch::MAX_ATTEMPTS`]
/// attempts, retried at +1 m, +5 m and +15 m, each bounded by the fetch's own
/// deadline — about half an hour in the worst case, and seconds in the usual
/// one. Once the attempts are spent the `Restore` itself records
/// `NotAttempted` with no retry scheduled, which this schedule reads as a
/// REACHED verdict well inside the hour. The bound exists for the verdict that
/// never arrives at all (a controller that stopped between the terminal patch
/// and the verdict, a status nobody will write again): the schedule must not
/// hold its own concurrency slot forever, and it must not call that run a
/// pass. `tests/rehearsal_controller.rs::the_verdict_wait_outlasts_the_whole_fetch_schedule`
/// pins the margin.
pub const VERDICT_WAIT_SECONDS: i64 = 3600;

/// `lastFailed.reason` for an exit-0 rehearsal whose evidence verdict was
/// still owed [`VERDICT_WAIT_SECONDS`] after it finished.
pub const REASON_VERDICT_NOT_REACHED: &str = "EvidenceVerdictNotReached";

/// What the previous slot's `Restore` finished as.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Observation {
    /// The object.
    pub restore: Option<String>,
    /// Whether the `Restore` is terminal (`Succeeded` or `Failed`).
    pub terminal: bool,
    /// Whether this schedule may RECORD the run now: it is terminal AND its
    /// evidence verdict is reached ([`verdict_owed`] says nothing is owed), or
    /// the wait for it ran out. Only a decided observation writes
    /// `lastSucceeded`/`lastFailed`, releases `activeRestoreRef` and moves
    /// `RehearsalHealthy` (REHEARSAL-PASS-RECORDED-AS-FAILED).
    pub decided: bool,
    /// Whether it passed: decided, exit `0`, `outcome: pass`, and a GREEN
    /// verdict by the shared `Restore` badge rule
    /// ([`crate::verification::restore_badge`], whose `Valid` half is
    /// [`logweir_core::trust::ValidBasis`]).
    pub passed: bool,
    /// Its `status.outcome`, verbatim.
    pub outcome: Option<String>,
    /// Why it did not pass, for a failure.
    pub reason: Option<String>,
    /// Why a terminal run is not decided yet — the verdict still owed.
    pub awaiting: Option<String>,
    /// The signed evidence key, for a pass.
    pub evidence: Option<String>,
    /// Topics teardown could not remove.
    pub pending_topics: Vec<String>,
    /// The measured recovery time.
    pub rto_seconds: Option<i64>,
}

/// The evidence verdict a TERMINAL `Restore` still owes, as a sentence — or
/// `None` when the verdict is reached (or no evidence was ever named, so there
/// is nothing to reach).
///
/// # Why this exists (REHEARSAL-PASS-RECORDED-AS-FAILED)
///
/// A destination-backed `Restore` turns terminal FIRST and learns its verdict
/// LATER: the terminal patch carries the exit code and the evidence keys, and
/// the evidence-fetch pass writes `outcome` and `evidence.verification` from
/// the fetched scorecard afterwards (`restore.rs`, D2 §3.9). Reading
/// "terminal" as "decided" recorded every destination-backed passing
/// rehearsal as FAILED — measured three times on lab-refresh-9 (L6 step 5:
/// `lastFailed.reason: ok` 0.4 s after the terminal patch, the verdict `Valid`
/// eleven seconds later).
///
/// Owed, in the `Restore`'s own vocabulary:
///
/// * `evidence.verification.result: Pending` — a fetch is queued or running;
/// * `NotAttempted` with `observation.retryAfter` set and attempts left — a
///   retry is SCHEDULED, so `NotAttempted` is not yet the answer (the last
///   attempt writes no `retryAfter`, and that `NotAttempted` IS the answer);
/// * no verification block while both mandatory keys are named — the window
///   between the terminal patch and the first verdict write, or a crash
///   inside it.
///
/// Everything else is reached: `Valid`, `Invalid`, `Untrusted`, a spent
/// `NotAttempted`, or a run that named no evidence at all (exits 1, 3, 4, a
/// refusal, a crash, or an exit 2 from a runner older than interface I8's
/// amendment).
#[must_use]
pub fn verdict_owed(restore: &Restore) -> Option<String> {
    let evidence = restore.status.as_ref().and_then(|s| s.evidence.as_ref())?;
    let verification = evidence.verification.as_ref();
    let observation = evidence.observation.as_ref();
    match verification.and_then(|v| v.result.as_deref()) {
        Some("Pending") => Some(
            "its evidence verdict is Pending: the evidence-fetch Job has not relayed the signed \
             scorecard yet"
                .to_string(),
        ),
        Some("NotAttempted") => {
            let attempt = observation
                .and_then(|o| o.attempt)
                .and_then(|a| u32::try_from(a).ok())
                .unwrap_or(1);
            let retry = observation.and_then(|o| o.retry_after.as_ref());
            match retry {
                Some(at) if attempt < crate::evidence_fetch::MAX_ATTEMPTS => Some(format!(
                    "its evidence fetch attempt {attempt} did not relay the scorecard and attempt \
                     {} is scheduled at {}",
                    attempt + 1,
                    at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                )),
                _ => None,
            }
        }
        None if evidence.scorecard_key.is_some() && evidence.sidecar_key.is_some() => Some(
            "it named its signed scorecard and no evidence verdict is recorded yet".to_string(),
        ),
        _ => None,
    }
}

/// When the `Restore` finished: the `lastTransitionTime` of its terminal
/// `Complete`/`Failed` condition, the instant the restore reconciler wrote
/// the exit — `metadata.creationTimestamp` only as a fallback, which is
/// EARLIER and so can only shorten a wait, never extend it.
fn finished_at(restore: &Restore) -> Option<DateTime<Utc>> {
    restore
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|conditions| {
            conditions
                .iter()
                .filter(|c| {
                    matches!(c.r#type.as_str(), "Complete" | "Failed") && c.status == "True"
                })
                .filter_map(|c| c.last_transition_time)
                .max()
        })
        .or_else(|| restore.metadata.creation_timestamp.as_ref().map(|t| t.0))
}

/// Project one `Restore` into [`Observation`], as seen at `now`.
///
/// # A pass is decided, exit 0, `outcome: pass` AND green — never less
///
/// The exit code is the runner's own verdict and stays authoritative: since
/// interface I8's amendment an exit-2 run publishes a signed scorecard that
/// verifies `Valid`, and that is a verified FAILURE. `outcome` and the verdict
/// arrive with the evidence-fetch pass, so a terminal run whose verdict is
/// still owed ([`verdict_owed`]) is not decided at all — neither pass nor
/// fail — until it is reached or [`VERDICT_WAIT_SECONDS`] run out. A verdict
/// that never arrives is recorded as a NON-pass with its reason; it is never
/// promoted to a pass.
#[must_use]
pub fn observe(restore: Option<&Restore>, now: DateTime<Utc>) -> Observation {
    let Some(restore) = restore else {
        return Observation::default();
    };
    let status = restore.status.as_ref();
    let terminal = super::restore::status_is_terminal(restore);
    let outcome = status.and_then(|s| s.outcome.clone());
    let exit_code = status.and_then(|s| s.exit_code);
    let owed = if terminal {
        verdict_owed(restore)
    } else {
        None
    };
    let waited_out = owed.is_some()
        && finished_at(restore).is_none_or(|t| (now - t).num_seconds() >= VERDICT_WAIT_SECONDS);
    let decided = terminal && (owed.is_none() || waited_out);
    let status_json = status
        .and_then(|s| serde_json::to_value(s).ok())
        .unwrap_or_else(|| json!({}));
    let badge = crate::verification::restore_badge(&status_json);
    let passed = decided
        && owed.is_none()
        && exit_code == Some(0)
        && outcome.as_deref() == Some("pass")
        && badge.green;
    // **THE PLAT-14.3b HOLD IS GONE.** Until 14.3b the `Restore` reconciler
    // refused every standing `Restore` with `ApprovalNotReceived` before a Job
    // could exist, and this projection had to name that separately so an
    // operator was not sent to look at their archive, their broker and their
    // approver's key for a rehearsal that never ran. `restore.rs` now reads
    // `spec.authorization`, so a standing `Restore` that ends terminally
    // ended for a reason about THIS rehearsal — an expired or withdrawn
    // authorization (`StandingAuthorizationRefused`), a preflight refusal, a
    // failed verification — and each is recorded as the failure it is, with
    // the Restore's own terminal reason in the message.
    let reason = if passed || !decided {
        None
    } else if exit_code != Some(0) {
        // THE RUN SAID "NOT PASSED". Its signed `outcome` names why when the
        // document verified; otherwise the exit's own reason does, and the
        // outcome only as the last resort (an unverified claim is not the
        // first thing an operator should read).
        let verified_outcome = outcome
            .clone()
            .filter(|o| o != "pass")
            .filter(|_| crate::verification::verification_is_valid(&status_json));
        verified_outcome
            .or_else(|| status.and_then(|s| s.exit_reason.clone()))
            .or_else(|| status.and_then(|s| s.reason.clone()))
            .or_else(|| outcome.clone())
    } else if waited_out {
        Some(REASON_VERDICT_NOT_REACHED.to_string())
    } else {
        // EXIT 0, AND STILL NOT A PASS: the verdict was reached and is not
        // green — `VerificationInvalid`, `VerificationUntrusted`,
        // `VerificationNotAttempted` (the fetch's attempts were spent) — or
        // the scorecard's own outcome is not `pass`.
        outcome
            .clone()
            .filter(|o| o != "pass")
            .or_else(|| Some(badge.reason.to_string()))
    };
    Observation {
        restore: Some(restore.name_any()),
        terminal,
        decided,
        passed,
        outcome: outcome.clone(),
        reason,
        awaiting: if decided { None } else { owed },
        evidence: status
            .and_then(|s| s.evidence.as_ref())
            .and_then(|e| e.scorecard_key.clone()),
        pending_topics: rehearsal::pending_topics(status.and_then(|s| s.teardown.as_ref())),
        rto_seconds: status
            .and_then(|s| s.measured.as_ref())
            .and_then(|m| m.rto_seconds),
    }
}

// ===========================================================================
// The status write
// ===========================================================================

/// Everything one pass says about the object.
#[derive(Debug, Clone)]
pub struct StatusUpdate {
    /// The verdict.
    pub verdict: Verdict,
    /// The due slot this pass decided — fired, or refused and therefore
    /// consumed ([`due_unconsumed_slot`]). `None` when no undecided slot is
    /// due, in which case a skip verdict writes neither `lastSkipped` nor
    /// `lastScheduledSlot`.
    pub slot: Option<String>,
    /// The next fire time.
    pub next_fire: Option<DateTime<Utc>>,
    /// What the previous child said.
    pub observation: Observation,
    /// The recomputed digest, published so an operator minting a new
    /// authorization can read it off the object instead of recomputing it.
    pub template_digest: String,
    /// The child this pass created.
    pub created: Option<String>,
}

/// The `/status` merge patch one pass produces.
///
/// BUILT AS JSON RATHER THAN BY SERIALISING THE STATUS STRUCT, for the reason
/// `backup_schedule::status_patch` records: every optional field carries
/// `skip_serializing_if`, so a serialised `None` is an ABSENT key, and an absent
/// key in a merge patch means "leave it alone". Clearing `nextFireTime` on a
/// suspended schedule and clearing `pendingRestoreRef` once the child is active
/// both need an explicit JSON `null`, which only a hand-built body can carry.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn status_patch(
    schedule: &RehearsalSchedule,
    update: &StatusUpdate,
    now: DateTime<Utc>,
) -> Value {
    let existing = schedule.status.as_ref();
    let mut status = serde_json::Map::new();
    status.insert(
        "observedGeneration".to_string(),
        json!(schedule.metadata.generation),
    );
    status.insert("templateDigest".to_string(), json!(update.template_digest));
    status.insert(
        "nextFireTime".to_string(),
        match update.next_fire {
            Some(t) => json!(t),
            None => Value::Null,
        },
    );

    // ---- the child --------------------------------------------------------
    if let Some(created) = update.created.as_deref() {
        status.insert("activeRestoreRef".to_string(), json!({ "name": created }));
        status.insert("pendingRestoreRef".to_string(), Value::Null);
        if let Some(slot) = update.slot.as_deref() {
            status.insert("lastScheduledSlot".to_string(), json!(slot));
        }
    }
    let observation = &update.observation;
    // DECIDED, NOT MERELY TERMINAL (REHEARSAL-PASS-RECORDED-AS-FAILED). A
    // finished run whose evidence verdict is still owed is left exactly where
    // it is — `activeRestoreRef` kept, nothing recorded, `RehearsalHealthy`
    // carried — so the next pass reads it again and records it once, with its
    // verdict. `observe` is the only place that decides.
    if observation.decided {
        // The run finished, so it is no longer active. `lastSucceeded` and
        // `lastFailed` are never cleared: they are the audit trail.
        if update.created.is_none() {
            status.insert("activeRestoreRef".to_string(), Value::Null);
            status.insert("pendingRestoreRef".to_string(), Value::Null);
        }
        if observation.passed {
            status.insert(
                "lastSucceeded".to_string(),
                json!({
                    "restoreRef": { "name": observation.restore },
                    "at": now,
                    "evidence": observation.evidence,
                    "rtoSeconds": observation.rto_seconds,
                }),
            );
        } else {
            status.insert(
                "lastFailed".to_string(),
                json!({
                    "restoreRef": { "name": observation.restore },
                    "at": now,
                    "reason": observation
                        .reason
                        .clone()
                        .unwrap_or_else(|| "NotPassed".to_string()),
                }),
            );
        }
        if observation.pending_topics.is_empty() {
            status.insert("cleanup".to_string(), Value::Null);
        } else {
            status.insert(
                "cleanup".to_string(),
                json!({
                    "pendingTopics": observation.pending_topics,
                    "since": now,
                }),
            );
        }
    }

    // ---- the skip ---------------------------------------------------------
    //
    // A SKIPPED SLOT IS SKIPPED (REHEARSAL-SKIP-DEFERS-SLOT). `lastSkipped.slot`
    // names the DUE slot that was refused — never the instant this pass ran —
    // and `lastScheduledSlot` advances to it in the same compare-and-set
    // write, so `decide` reads that slot as already decided and does not fire
    // it late when the blocker clears inside `startingDeadlineSeconds`. A
    // rehearsal's value is that it measures recovery AT a cadence; one that ran
    // forty minutes late because its predecessor overran would record an RTO
    // for a slot that never happened. Every reason consumes its slot (D3 §4.1,
    // §4.3, §4.4, §13; `docs/kubernetes.md`).
    //
    // A skip with NO due, undecided slot (a pass inside a slot this schedule
    // already decided, or a cron this build cannot read) writes neither: the
    // `Ready` message still carries the refusal, and `lastSkipped` stays the
    // record of the last slot that was actually refused rather than being
    // rewritten every requeue.
    if let (Verdict::Skipped(skip), Some(slot)) = (&update.verdict, update.slot.as_deref()) {
        status.insert(
            "lastSkipped".to_string(),
            json!({
                "slot": slot,
                "reason": skip.reason.as_str(),
            }),
        );
        status.insert("lastScheduledSlot".to_string(), json!(slot));
    }

    // ---- conditions -------------------------------------------------------
    let existing_conditions = existing
        .and_then(|s| s.conditions.as_deref())
        .unwrap_or(&[]);
    let mut conditions = Vec::new();
    let (ready_status, ready_reason, ready_message) = if schedule.spec.suspend {
        (
            "True".to_string(),
            REASON_SUSPENDED.to_string(),
            "spec.suspend is true; no slot fires and nothing is skipped".to_string(),
        )
    } else {
        match &update.verdict {
            Verdict::Fire(order) => (
                "True".to_string(),
                REASON_SCHEDULED.to_string(),
                format!(
                    "slot {} rehearses point {} ({} partition basis)",
                    order.slot, order.selected.point.point_id, order.selected.size_basis
                ),
            ),
            Verdict::Skipped(skip) => (
                "True".to_string(),
                REASON_SCHEDULED.to_string(),
                skip.to_string(),
            ),
            Verdict::Idle => (
                "True".to_string(),
                REASON_SCHEDULED.to_string(),
                "no slot is due".to_string(),
            ),
        }
    };
    conditions.push(condition(
        existing_conditions,
        CONDITION_READY,
        &ready_status,
        &ready_reason,
        &ready_message,
        schedule.metadata.generation,
        now,
    ));

    let (auth_status, auth_reason, auth_message) = match &update.verdict {
        Verdict::Skipped(skip)
            if matches!(
                skip.reason,
                SkipReason::AuthorizationInvalid | SkipReason::AuthorizationExpired
            ) =>
        {
            (
                "False".to_string(),
                skip.reason.as_str().to_string(),
                skip.detail.clone(),
            )
        }
        Verdict::Fire(order) => (
            "True".to_string(),
            REASON_AUTHORIZED.to_string(),
            format!(
                "the Approval `{}` authorises this template until the document's expiry",
                order.authorization.name
            ),
        ),
        _ => {
            // NOT RE-ASSERTED BY A PASS THAT DID NOT LOOK. A slot that skipped
            // for concurrency proves nothing about the authorization, so the
            // previous verdict is carried rather than refreshed.
            carried(
                existing_conditions,
                CONDITION_AUTHORIZED,
                "no slot has evaluated the standing authorization yet",
            )
        }
    };
    conditions.push(condition(
        existing_conditions,
        CONDITION_AUTHORIZED,
        &auth_status,
        &auth_reason,
        &auth_message,
        schedule.metadata.generation,
        now,
    ));

    let (health_status, health_reason, health_message) = if observation.decided {
        if observation.passed {
            (
                "True".to_string(),
                REASON_PASSED.to_string(),
                format!(
                    "the rehearsal {} passed",
                    observation.restore.clone().unwrap_or_default()
                ),
            )
        } else {
            (
                "False".to_string(),
                REASON_FAILED.to_string(),
                format!(
                    "the rehearsal {} did not pass: {}",
                    observation.restore.clone().unwrap_or_default(),
                    observation
                        .reason
                        .clone()
                        .unwrap_or_else(|| "NotPassed".to_string())
                ),
            )
        }
    } else {
        carried(
            existing_conditions,
            CONDITION_REHEARSAL_HEALTHY,
            "no rehearsal has finished yet",
        )
    };
    conditions.push(condition(
        existing_conditions,
        CONDITION_REHEARSAL_HEALTHY,
        &health_status,
        &health_reason,
        &health_message,
        schedule.metadata.generation,
        now,
    ));

    status.insert("conditions".to_string(), json!(conditions));
    json!({ "status": Value::Object(status) })
}

/// The condition this pass did NOT re-evaluate, carried forward verbatim.
///
/// A pass that skipped for concurrency proves nothing about the authorization,
/// and a rehearsal still running proves nothing about health. Re-asserting
/// `Unknown` over a `False` an earlier pass computed would erase a verdict, and
/// re-asserting `True` would claim one nobody checked.
fn carried(
    existing: &[crate::crds::Condition],
    r#type: &str,
    nothing_yet: &str,
) -> (String, String, String) {
    match existing.iter().find(|c| c.r#type == r#type) {
        Some(c) => (
            match c.status.as_str() {
                "True" => "True".to_string(),
                "False" => "False".to_string(),
                _ => "Unknown".to_string(),
            },
            c.reason
                .clone()
                .unwrap_or_else(|| REASON_NO_RESULT.to_string()),
            c.message.clone().unwrap_or_default(),
        ),
        None => (
            "Unknown".to_string(),
            REASON_NO_RESULT.to_string(),
            nothing_yet.to_string(),
        ),
    }
}

fn condition(
    existing: &[crate::crds::Condition],
    r#type: &str,
    status: &str,
    reason: &str,
    message: &str,
    generation: Option<i64>,
    now: DateTime<Utc>,
) -> crate::crds::Condition {
    crate::conditions::merge_condition(
        existing.iter().find(|c| c.r#type == r#type),
        crate::crds::Condition {
            r#type: r#type.to_string(),
            status: status.to_string(),
            observed_generation: generation,
            last_transition_time: Some(now),
            reason: Some(reason.to_string()),
            message: Some(message.to_string()),
        },
    )
}

/// Add the optimistic-concurrency precondition every status write in this file
/// carries — seam **S7**.
fn status_patch_with_preconditions(
    schedule: &RehearsalSchedule,
    mut patch: Value,
) -> Result<Value, ReconcileError> {
    let name = schedule.name_any();
    let resource_version = schedule
        .metadata
        .resource_version
        .clone()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            ReconcileError::Api(kube::Error::Discovery(
                kube::error::DiscoveryError::MissingResource(format!(
                    "RehearsalSchedule {name} carries no metadata.resourceVersion, which a \
                     /status compare-and-set needs (D-SEAMS S7)"
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

async fn commit(
    api: &Api<RehearsalSchedule>,
    schedule: &RehearsalSchedule,
    update: &StatusUpdate,
    now: DateTime<Utc>,
) -> Result<(), ReconcileError> {
    let body = status_patch_with_preconditions(schedule, status_patch(schedule, update, now))?;
    match api
        .patch_status(
            &schedule.name_any(),
            &PatchParams::default(),
            &Patch::Merge(body),
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 409 => {
            debug!(
                schedule = %schedule.name_any(),
                "the status changed under this reconcile (409); the next pass reads it"
            );
            Ok(())
        }
        Err(e) => Err(ReconcileError::Api(e)),
    }
}

/// The slot a pass at `now` is deciding, or `None` when there is none to
/// decide: the cron does not parse, nothing is due yet, or the latest due slot
/// is already `status.lastScheduledSlot` (fired, or skipped and consumed).
///
/// The same arithmetic as [`decide`]'s cadence step, so a skip can name the
/// slot it refused rather than the instant it looked.
#[must_use]
pub fn due_unconsumed_slot(schedule: &RehearsalSchedule, now: DateTime<Utc>) -> Option<String> {
    let cadence = crate::cadence::Cadence::parse(&schedule.spec.schedule, None).ok()?;
    let due = latest_owned_slot(schedule, &cadence, now)?;
    let slot = crate::slot::slot_name(due);
    let decided = schedule
        .status
        .as_ref()
        .and_then(|s| s.last_scheduled_slot.as_deref());
    (decided != Some(slot.as_str())).then_some(slot)
}

/// The latest slot due at `now` that belongs to this schedule: `None` when
/// nothing is due, or when the latest due slot came due before the schedule's
/// own `metadata.creationTimestamp`.
///
/// D1 / PLAT-04.2's creation bound (SCHEDULE-FIRES-SLOT-BEFORE-CREATION), the
/// same rule as `backup_schedule::bound_by_creation` and the Kubernetes
/// `CronJob`'s: a schedule never fires a slot whose due time is before it
/// existed. Without it a `RehearsalSchedule` created at 00:53 with
/// `30 * * * *` and a `startingDeadlineSeconds` of an hour or more rehearsed
/// its 00:30 slot. A pre-creation slot is IDLE, not a skip: it is not named in
/// `status.lastSkipped` and does not advance `status.lastScheduledSlot`,
/// because it was never this schedule's slot to refuse. Both [`decide`] and
/// [`due_unconsumed_slot`] read the slot through here, so the slot a skip
/// names and the slot the cadence step decided cannot disagree.
#[must_use]
pub fn latest_owned_slot(
    schedule: &RehearsalSchedule,
    cadence: &crate::cadence::Cadence,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let due = cadence.latest_due_slot(now)?;
    let created = schedule.metadata.creation_timestamp.as_ref().map(|t| t.0);
    (!super::backup_schedule::slot_predates_creation(due, created)).then_some(due)
}

/// The next instant this schedule fires, or `None` when it is suspended or its
/// cron does not parse.
#[must_use]
pub fn next_fire(schedule: &RehearsalSchedule, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    if schedule.spec.suspend {
        return None;
    }
    crate::cadence::Cadence::parse(&schedule.spec.schedule, None)
        .ok()?
        .next_fire_after(now)
}

// ===========================================================================
// Registration
// ===========================================================================

async fn reconcile(
    schedule: Arc<RehearsalSchedule>,
    ctx: Arc<Context>,
) -> Result<Action, ReconcileError> {
    let outcome = reconcile_schedule(&schedule, &ctx, Utc::now()).await?;
    Ok(Action::requeue(std::time::Duration::from_secs(
        outcome.requeue_seconds,
    )))
}

fn error_policy(
    schedule: Arc<RehearsalSchedule>,
    err: &ReconcileError,
    _ctx: Arc<Context>,
) -> Action {
    warn!(
        schedule = %schedule.name_any(),
        error = %err,
        "rehearsal schedule reconcile failed; requeueing"
    );
    Action::requeue(std::time::Duration::from_secs(ERROR_REQUEUE_SECONDS))
}

/// Run the `RehearsalSchedule` controller until the process ends.
///
/// ALL NAMESPACES, like every other controller in this directory, and
/// `.owns(restores, …)` so a finished rehearsal wakes its own schedule rather
/// than waiting out the requeue — a `RehearsalHealthy=False` published thirty
/// seconds late is a protection alert thirty seconds late.
pub fn controller(client: kube::Client) -> impl std::future::Future<Output = ()> + Send {
    // D0 STAGE 5: ONE WATCH PER WATCHED NAMESPACE. `crate::scope` is the whole
    // cluster unless `LOGWEIR_WATCH_NAMESPACES` names the execution
    // namespaces, and then this reconciler runs once per namespace with an
    // `Api::namespaced` watch — the only shape the scoped chart's RoleBindings
    // permit.
    crate::scope::run_everywhere(move |namespace| controller_in(client.clone(), namespace))
}

/// One watch of [`controller`], over `namespace` (`None` is the whole
/// cluster, the behaviour before D0 stage 5).
fn controller_in(
    client: kube::Client,
    namespace: Option<String>,
) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<RehearsalSchedule> = crate::scope::api(&client, namespace.as_deref());
    let restores: Api<Restore> = crate::scope::api(&client, namespace.as_deref());
    let ctx = Arc::new(Context {
        client,
        archive: None,
        runner_image: crate::job::RunnerImage::default(),
    });
    async move {
        Controller::new(api, watcher::Config::default())
            .owns(restores, watcher::Config::default())
            .run(reconcile, error_policy, ctx)
            .for_each(|_| std::future::ready(()))
            .await;
    }
}
