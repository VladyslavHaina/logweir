//! The `ProtectionPolicy` reconciler — PLAT-14.2, decision D3 §§3.1–3.3.
//!
//! # The thin half
//!
//! Every verdict this file publishes is computed by [`crate::protection`],
//! which holds no client. This module reads what the API server says, calls
//! [`crate::protection::evaluate`] and [`crate::protection::reconcile_alerts`],
//! and patches `protectionpolicies/status`. That is the whole of it.
//!
//! **It writes nothing but this status.** There is no `Api<Backup>` or
//! `Api<Restore>` status write anywhere in this file;
//! `the_controller_names_no_backup_or_restore_status_write` asserts it. A
//! protection verdict is a projection of runs that already happened, and this
//! controller is not the execution authority for any of them.
//!
//! # Alert delivery is withdrawn
//!
//! v1 delivers no alert (owner decision OD-12). The alert ledger in
//! `status.alerts` still records what is open and what resolved; a policy that
//! still carries `spec.notifications` says by name, in its
//! `NotificationsDelivered` condition, that nothing is delivered.

use std::sync::Arc;

use futures::StreamExt as _;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::{ListParams, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::runtime::{watcher, Controller};
use kube::{Api, ResourceExt as _};
use serde_json::{json, Value};
use tracing::{debug, warn};

use crate::conditions::{current_condition, merge_condition, status_unchanged};
use crate::crds::backup::Backup;
use crate::crds::backup_destination::BackupDestination;
use crate::crds::backup_schedule::BackupSchedule;
use crate::crds::kafka_cluster::KafkaCluster;
use crate::crds::protection_policy::{AlertEntry, ProtectionPolicy, ProtectionPolicyStatus};
use crate::crds::recovery_catalog::RecoveryCatalog;
use crate::crds::restore::Restore;
use crate::crds::{Condition, Time};
use crate::identity;
use crate::protection as p;

use super::approval::ReconcileError;

// ===========================================================================
// Constants
// ===========================================================================

/// `Ready` — this controller looked at the object and could publish a verdict.
pub const CONDITION_READY: &str = "Ready";
/// `Protected` — D3 §3.2: `True` only for `Healthy`, `Unknown` for `Unknown`.
pub const CONDITION_PROTECTED: &str = "Protected";
/// `NotificationsDelivered` — written only for a policy that still carries
/// the withdrawn `spec.notifications`, and then always `False`.
pub const CONDITION_NOTIFICATIONS_DELIVERED: &str = "NotificationsDelivered";

/// `Ready`'s reason when a verdict was published.
pub const REASON_EVALUATED: &str = "Evaluated";
/// `Ready`'s reason for an object with no namespace or UID.
pub const REASON_NOT_EVALUATED: &str = "NotEvaluated";
/// `NotificationsDelivered`'s reason: the policy names alert sinks, and alert
/// delivery is withdrawn.
pub const REASON_NOTIFICATIONS_WITHDRAWN: &str = "NotificationsWithdrawn";
/// What that condition says.
pub const NOTIFICATIONS_WITHDRAWN_MESSAGE: &str =
    "spec.notifications is withdrawn: Logweir delivers no alert to PagerDuty, Slack or a \
     webhook, and this field is read by nothing. Remove it; health and the alert ledger are \
     unchanged";

/// How long before an errored reconcile is retried.
pub const ERROR_REQUEUE_SECONDS: u64 = 30;

/// One page of the bounded `Backup` listing.
pub const BACKUP_PAGE_LIMIT: u32 = 200;
/// How many pages of that listing one evaluation walks.
///
/// `200 × 5` objects, at most once per `evaluationIntervalSeconds` (60 s
/// floor). The cap is what makes the read bounded; `status.lastAvailablePoint`
/// is a projection of the newest run, not a census.
pub const MAX_BACKUP_PAGES: usize = 5;
/// How many catalog page `ConfigMap`s one evaluation reads — the CRD's
/// `status.pages` `maxItems`.
pub const MAX_CATALOG_PAGES: usize = 8;
/// The `ConfigMap` key a catalog page's entries live under, one compact JSON
/// object per line.
pub const CATALOG_PAGE_DATA_KEY: &str = "entries.jsonl";

// ===========================================================================
// Errors, context and outcome
// ===========================================================================

/// What this reconciler needs.
#[derive(Clone)]
pub struct ProtectionContext {
    /// The client every `Api` here is built from.
    pub client: kube::Client,
}

/// What one pass did — the shape the tests assert over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    /// The health it published.
    pub health: p::Health,
    /// The freshness it published.
    pub freshness: p::Freshness,
    /// Why.
    pub reason: p::FreshnessReason,
    /// Whether the status patch landed (or was already what this pass
    /// computed).
    pub committed: bool,
    /// Seconds until the next pass.
    pub requeue_seconds: u64,
}

// ===========================================================================
// The reconcile
// ===========================================================================

/// Reconcile one `ProtectionPolicy`.
///
/// # Errors
///
/// [`ReconcileError::Api`] for anything that is not a verdict.
#[allow(clippy::too_many_lines)]
pub async fn reconcile_policy(
    policy: &ProtectionPolicy,
    ctx: &ProtectionContext,
    now: Time,
) -> Result<Outcome, ReconcileError> {
    let name = policy.name_any();
    let Some(namespace) = policy.namespace() else {
        warn!(policy = %name, "ProtectionPolicy carries no metadata.namespace; nothing is done");
        return Ok(idle());
    };
    let Some(uid) = policy.uid() else {
        warn!(
            policy = %name,
            namespace = %namespace,
            "ProtectionPolicy carries no metadata.uid; an alert key built from an empty UID \
             would be one incident per kind across every policy in the cluster, so nothing is \
             done"
        );
        return Ok(idle());
    };
    let api: Api<ProtectionPolicy> = Api::namespaced(ctx.client.clone(), &namespace);
    let spec = &policy.spec;
    let interval = spec.evaluation_interval_seconds;

    // ------------------------------------------------------------------
    // Read.
    // ------------------------------------------------------------------
    let clusters: Api<KafkaCluster> = Api::namespaced(ctx.client.clone(), &namespace);
    let source_exists = clusters
        .get_opt(&spec.protects.source_ref.name)
        .await
        .map_err(ReconcileError::Api)?
        .is_some();

    let destination_exists = match spec.protects.destination_ref.as_ref() {
        None => true,
        Some(reference) => {
            let destinations: Api<BackupDestination> =
                Api::namespaced(ctx.client.clone(), &namespace);
            destinations
                .get_opt(&reference.name)
                .await
                .map_err(ReconcileError::Api)?
                .is_some()
        }
    };

    let schedules_api: Api<BackupSchedule> = Api::namespaced(ctx.client.clone(), &namespace);
    let mut schedule_objects: Vec<(String, Option<BackupSchedule>)> = Vec::new();
    for reference in spec.protects.schedule_refs.as_deref().unwrap_or_default() {
        let found = schedules_api
            .get_opt(&reference.name)
            .await
            .map_err(ReconcileError::Api)?;
        schedule_objects.push((reference.name.clone(), found));
    }
    let schedule_facts: Vec<p::ScheduleFacts> =
        schedule_objects.iter().map(schedule_facts).collect();

    let backups = list_backups(&ctx.client, &namespace).await?;
    let members: Vec<&Backup> = backups
        .iter()
        .filter(|b| is_member(b, spec, &schedule_objects))
        .collect();

    let catalog = read_catalog(&ctx.client, &namespace, spec, now).await?;

    // ------------------------------------------------------------------
    // Project, then evaluate. Both pure.
    // ------------------------------------------------------------------
    let mut candidates: Vec<p::PointCandidate> = members
        .iter()
        .map(|b| candidate_from_backup(b, spec, &catalog))
        .collect();
    candidates.sort_by_key(|c| std::cmp::Reverse(c.recovery_point_at));
    candidates.truncate(p::MAX_BACKUPS_SCANNED);

    let mut slots: Vec<p::SlotRun> = members.iter().map(|b| slot_run(b)).collect();
    // Newest first. `creationTimestamp` is set by the API server on every
    // object, so it orders a retry chain (attempt n+1 is created after n) and
    // a manual run alike; the name breaks a second-granular tie so the order
    // does not vary between two reconciles over one cluster state.
    slots.sort_by(|a, b| {
        b.at.cmp(&a.at)
            .then_with(|| b.attempt.cmp(&a.attempt))
            .then_with(|| b.backup_name.cmp(&a.backup_name))
    });
    slots.truncate(p::MAX_BACKUPS_SCANNED);

    let rehearsal = p::RehearsalFacts::default();
    let verdict = p::evaluate(&p::Inputs {
        spec,
        source_exists,
        destination_exists,
        candidates: &candidates,
        catalog: &catalog,
        schedules: &schedule_facts,
        slots: &slots,
        rehearsal: &rehearsal,
        now,
    });

    // ------------------------------------------------------------------
    // Move the ledger: D3 §3.3's open and resolve, and the fifth kind.
    // ------------------------------------------------------------------
    let stored_alerts: Vec<AlertEntry> = policy
        .status
        .as_ref()
        .and_then(|s| s.alerts.clone())
        .unwrap_or_default();
    let backup_ids: Vec<String> = members.iter().filter_map(|b| backup_set_id(b)).collect();
    let completions = read_recoveries(&ctx.client, &namespace, spec, &backup_ids).await?;
    let observed = p::fold_recoveries(&stored_alerts, &completions, now);
    let alerts = p::reconcile_alerts(&observed, &verdict.open_kinds, verdict.health, &uid, now);

    // ------------------------------------------------------------------
    // Status. One merge PATCH, `metadata.resourceVersion` as the
    // precondition (seam S7).
    // ------------------------------------------------------------------
    let status = build_status(policy, &verdict, alerts, now, interval);
    let commit = write_status(&api, policy, &status).await?;

    Ok(Outcome {
        health: verdict.health,
        freshness: verdict.freshness,
        reason: verdict.reason,
        committed: commit.is_committed(),
        requeue_seconds: u64::try_from(interval).unwrap_or(300),
    })
}

/// The outcome for an object this controller cannot identify.
///
/// Nothing was read, nothing was written and no verdict was computed —
/// [`REASON_NOT_EVALUATED`] is the reason a `Ready` condition would carry, and
/// the freshness reason says the same thing rather than borrowing one that
/// would describe a check that never ran.
fn idle() -> Outcome {
    Outcome {
        health: p::Health::Unknown,
        freshness: p::Freshness::Unknown,
        reason: p::FreshnessReason::NotEvaluated,
        committed: false,
        requeue_seconds: ERROR_REQUEUE_SECONDS,
    }
}

// ===========================================================================
// Reading
// ===========================================================================

/// Every `Backup` in the namespace, bounded by [`MAX_BACKUP_PAGES`] pages of
/// [`BACKUP_PAGE_LIMIT`].
///
/// # Why this is not narrowed by `logweir.dev/schedule-uid`
///
/// D3 §3.2 names that label as the INDEX and `spec.scheduleRef.uid` as the
/// AUTHORITY. Nothing on `main` writes the label yet (it is D1 W2's), so a
/// label-selected list answers empty and every installation would read
/// `Unprotected` — and a **manual run of a schedule is part of its history**
/// (`identity::is_run_of_schedule`), which a label written only by the
/// scheduler would miss even once it exists. Membership is therefore decided
/// by the authority, over a bounded listing. Narrowing by the label when D1's
/// writer lands is an optimisation, not a change to the rule.
async fn list_backups(
    client: &kube::Client,
    namespace: &str,
) -> Result<Vec<Backup>, ReconcileError> {
    let api: Api<Backup> = Api::namespaced(client.clone(), namespace);
    let mut out: Vec<Backup> = Vec::new();
    let mut token: Option<String> = None;
    for _ in 0..MAX_BACKUP_PAGES {
        let mut params = ListParams::default().limit(BACKUP_PAGE_LIMIT);
        if let Some(cursor) = token.as_deref() {
            params = params.continue_token(cursor);
        }
        let page = api.list(&params).await.map_err(ReconcileError::Api)?;
        token = page.metadata.continue_.clone().filter(|t| !t.is_empty());
        out.extend(page.items);
        if token.is_none() {
            break;
        }
    }
    if token.is_some() {
        debug!(
            namespace = %namespace,
            pages = MAX_BACKUP_PAGES,
            "the Backup listing hit its page bound; the protection verdict is a projection of \
             the runs it saw and says so"
        );
    }
    Ok(out)
}

/// Whether this `Backup` counts towards this policy's objective.
///
/// With `scheduleRefs`: membership is `identity::is_run_of_schedule` against
/// each named schedule's UID — which accepts `spec.scheduleRef.{name,uid}`,
/// the legacy controller `ownerReference` and PLAT-05.2's retention
/// annotation, and therefore **counts a manual run of the schedule as
/// history**. Without `scheduleRefs`: every run of the named source that
/// writes to the named destination.
fn is_member(
    backup: &Backup,
    spec: &crate::crds::protection_policy::ProtectionPolicySpec,
    schedules: &[(String, Option<BackupSchedule>)],
) -> bool {
    if backup.spec.source_ref.name != spec.protects.source_ref.name {
        return false;
    }
    if !destination_matches(backup, spec) {
        return false;
    }
    let refs = spec.protects.schedule_refs.as_deref().unwrap_or_default();
    if refs.is_empty() {
        return true;
    }
    schedules.iter().any(|(name, found)| {
        found
            .as_ref()
            .and_then(kube::ResourceExt::uid)
            .is_some_and(|uid| identity::is_run_of_schedule(backup, name, &uid))
    })
}

/// Whether the run wrote where this policy protects.
///
/// A saved destination matches by NAME; an inline archive matches by URL. The
/// two are never compared across: a `destinationRef` run carries the sentinel
/// `logweir-destination://<name>` in `spec.archive.url`, so comparing URLs
/// would make a destination-backed run match a `legacyArchive` policy whose
/// URL happened to be the sentinel.
fn destination_matches(
    backup: &Backup,
    spec: &crate::crds::protection_policy::ProtectionPolicySpec,
) -> bool {
    match (
        spec.protects.destination_ref.as_ref(),
        spec.protects.legacy_archive.as_ref(),
    ) {
        (Some(wanted), _) => {
            backup
                .spec
                .destination_ref
                .as_ref()
                .map(|d| d.name.as_str())
                == Some(&wanted.name)
        }
        (None, Some(archive)) => {
            backup.spec.destination_ref.is_none() && backup.spec.archive.url == archive.url
        }
        // The CEL rule H1 makes this unreachable; answered rather than
        // asserted, because a panic here would be a controller crash on an
        // object the API server accepted.
        (None, None) => false,
    }
}

fn schedule_facts((name, found): &(String, Option<BackupSchedule>)) -> p::ScheduleFacts {
    let Some(schedule) = found else {
        return p::ScheduleFacts {
            name: name.clone(),
            exists: false,
            ..p::ScheduleFacts::default()
        };
    };
    let status = schedule.status.as_ref();
    let ready = status
        .and_then(|s| s.conditions.as_ref())
        .and_then(|c| c.iter().find(|c| c.r#type == "Ready"))
        .map(|c| c.status.clone());
    p::ScheduleFacts {
        name: name.clone(),
        suspended: schedule.spec.suspend,
        ready,
        next_fire_time: status.and_then(|s| s.next_fire_time),
        last_missed_slot: status.and_then(|s| s.last_missed_slot.clone()),
        last_fire_time: status.and_then(|s| s.last_fire_time),
        missed_slots: declared_missed_slots(schedule),
        exists: true,
    }
}

/// D1 W2's `status.missedSlots`, read **defensively** off the serialized
/// status rather than off a typed field.
///
/// The field is a CONTRACT this worker consumes and not one it can require:
/// D1 W2 is in review, not on `main`. Reading it through `serde_json` means
/// the day it lands the count becomes exact with no edit here, and until then
/// the absence is an absence rather than a compile error or a pinned
/// dependency on another worker's branch.
fn declared_missed_slots(schedule: &BackupSchedule) -> Option<i64> {
    let status = serde_json::to_value(schedule.status.as_ref()?).ok()?;
    match status.get("missedSlots")? {
        // D1 W2's landed shape: `{count, countCapped, recent, …}`. `count` is a
        // FLOOR when `countCapped` is set — the enumeration stopped at its
        // 1000-slot cap — and a floor is the honest number to alert on.
        Value::Object(block) => block.get("count").and_then(Value::as_i64),
        // The two shapes the field was drafted as, kept because this read is
        // deliberately defensive: a protection verdict does not wait on a
        // neighbouring worker's rebase, and it does not break on one either.
        Value::Number(n) => n.as_i64(),
        Value::Array(items) => i64::try_from(items.len()).ok(),
        _ => None,
    }
}

/// One `Backup` as a candidate recovery point.
fn candidate_from_backup(
    backup: &Backup,
    spec: &crate::crds::protection_policy::ProtectionPolicySpec,
    catalog: &p::CatalogAnswer,
) -> p::PointCandidate {
    let status = backup.status.as_ref();
    let verification = status
        .and_then(|s| s.evidence.as_ref())
        .and_then(|e| e.verification.as_ref());
    let evidence = p::Evidence::from_verification(
        verification.and_then(|v| v.result.as_deref()),
        verification.and_then(|v| v.trust.as_ref()),
    );
    let receipt_sha256 = status
        .and_then(|s| s.evidence.as_ref())
        .and_then(|e| e.receipt_sha256.as_deref())
        .filter(|digest| p::point_id_from_receipt_digest(digest).is_some())
        .map(str::to_string);
    let point_id = receipt_sha256
        .as_deref()
        .and_then(p::point_id_from_receipt_digest);
    // `windowCovered.toMs` is EXCLUSIVE — the newest archived record is one
    // millisecond before it — and it is published under its own name so
    // nothing confuses it with the capture start the objective is measured
    // from.
    let newest_record_at = status
        .and_then(|s| s.window_covered.as_ref())
        .and_then(|w| chrono::DateTime::from_timestamp_millis(w.to_ms.saturating_sub(1)));
    // The archive set this run wrote into, from the pre-Job patch — BEFORE any
    // verification is attempted, which is why it is there on the credential
    // posture where the receipt digest is not. THE SAME `backup_set_id` the
    // `Restore` match uses, deliberately: a second reading of the same two
    // fields is how the two halves of this file come to disagree about which
    // set a run belongs to.
    let backup_id = backup_set_id(backup).filter(|id| !id.is_empty());
    // PROD-02.1 (review EC-1): an EMPTY CAPTURE holds no record, so it is
    // never a recovery point. Its `Complete` reason is written only beside a
    // passing verdict, on the same patch as `status.capture`, so it is read
    // here together with it; with no capture time the point is not available
    // and the policy ages from the last real point.
    let empty_capture = status
        .and_then(|s| s.conditions.as_ref())
        .and_then(|cs| {
            cs.iter()
                .find(|c| c.r#type == crate::conditions::CONDITION_COMPLETE)
        })
        .and_then(|c| c.reason.as_deref())
        == Some(crate::conditions::CONDITION_REASON_EMPTY_CAPTURE);
    let candidate = p::PointCandidate {
        backup_name: Some(backup.name_any()),
        point_id,
        receipt_sha256,
        backup_id,
        recovery_point_at: status
            .filter(|_| !empty_capture)
            .and_then(|s| s.capture.as_ref())
            .and_then(|c| c.started_at),
        newest_record_at,
        phase: status.and_then(|s| s.phase.clone()),
        exit_code: status.and_then(|s| s.exit_code),
        evidence,
        topics: backup.spec.topics.clone(),
        covers_all_topics: backup.spec.all_user_topics.is_some(),
        destination_matches: destination_matches(backup, spec),
        source_matches: backup.spec.source_ref.name == spec.protects.source_ref.name,
    };
    with_catalog_facts(candidate, catalog)
}

/// Fill the two point facts the controller could not read from the catalog row
/// for the SAME point — defect `PROTECTION-SECRETKEYS-UNPROTECTED`, clause 3.
///
/// `status.capture` is written only on a `Valid` verification verdict
/// (`controllers::backup`), so on a destination the controller holds no Secret
/// verb the point could be neither aged nor — where the receipt bytes were not
/// fetched either — named. The catalog controller DID read that receipt:
/// `recoveryPointAtMs` is `BackupReceipt.started_at` carried through the view
/// (D3 §3.2) and `pointId` is D3 §5.1's identity over the same digest. This is
/// the same number and the same name from the same signed document, by the
/// route this installation's credential model leaves open.
///
/// # ONLY for the verdict the catalog is allowed to answer — review **MEDIUM-1**
///
/// The fourth site of [`p::Evidence::was_reached`]'s rule, and the one that was
/// missing it. A point the controller fetched and refused (`Invalid`,
/// `Untrusted`) also has no `status.capture`, so it reached this function and
/// was handed a capture time by the view — and with `requireVerifiedEvidence`
/// turned OFF nothing downstream refused it again, so a digest mismatch became
/// the policy's newest available point: `health: Healthy`, `Protected=True`, no
/// alert, where `main` said `Unprotected`. The objective governs whether an
/// UNVERIFIED point may count as protection; it never governs whether a REFUSED
/// one may, and D3 §3.2's `Unprotected` is the answer for a refused point at
/// every setting of it. A reached verdict therefore leaves here with the facts
/// it came in with, which is no capture time, which is not available.
///
/// **Nothing is invented.** A candidate the view does not hold, or holds twice
/// ([`p::catalog_entry_for`]), keeps its absent facts and is reported as
/// unplaceable. A `recoveryPointAtMs` of `0` is `serde(default)`'s stand-in for
/// a field the view did not write, not a capture in 1970, and is not read.
fn with_catalog_facts(
    mut candidate: p::PointCandidate,
    catalog: &p::CatalogAnswer,
) -> p::PointCandidate {
    // The fallback-not-authority rule, in three statements. THIS one carries
    // behaviour of its own (mutant `MED1-M1`): the catalog answers only for a
    // verdict the controller never reached. It also spares a policy whose
    // points the controller DID read a view scan per candidate, up to
    // `MAX_BACKUPS_SCANNED` times a pass.
    if candidate.evidence.was_reached() {
        return candidate;
    }
    let Some(entry) = p::catalog_entry_for(&candidate, catalog) else {
        return candidate;
    };
    // The other two statements are each redundant against a DIFFERENT invariant
    // — `point_id` against the join key's own equality (where it is `Some`,
    // `entries_for` matched on it, so the assignment would be a no-op), and the
    // capture time against `capture` being written only on a `Valid` verdict,
    // which the early return above has already excluded. Neither is removable
    // on its own with an observable result; removing all three at once makes
    // the row authoritative, which is mutant `MED3-M1` and which the
    // `ControllerIdentity` row catches.
    if candidate.recovery_point_at.is_none() && entry.recovery_point_at_ms > 0 {
        candidate.recovery_point_at =
            chrono::DateTime::from_timestamp_millis(entry.recovery_point_at_ms);
    }
    if candidate.point_id.is_none() && !entry.point_id.is_empty() {
        candidate.point_id = Some(entry.point_id.clone());
    }
    candidate
}

/// One `Backup` as a slot of history.
fn slot_run(backup: &Backup) -> p::SlotRun {
    let (_, attempt, _) = identity::declared_trigger(backup);
    let status = backup.status.as_ref();
    let phase = status.and_then(|s| s.phase.as_deref());
    let outcome = match phase {
        Some("Succeeded") => p::SlotOutcome::Succeeded,
        Some("Failed") => p::SlotOutcome::Failed,
        _ => p::SlotOutcome::Running,
    };
    p::SlotRun {
        // A manual run is its own slot: it belongs to the schedule's history
        // and has no cron slot of its own, so keying it on the object name
        // keeps it from collapsing into a scheduled slot's retry chain.
        slot: backup
            .spec
            .slot
            .clone()
            .unwrap_or_else(|| backup.name_any()),
        attempt,
        outcome,
        at: status
            .and_then(|s| s.capture.as_ref())
            .and_then(|c| c.finished_at.or(c.started_at))
            .or_else(|| backup.creation_timestamp().map(|t| t.0)),
        backup_name: backup.name_any(),
        reason: status.and_then(|s| s.exit_reason.clone()),
    }
}

/// The archive-set identity of one run — the value a `Restore` names in
/// `spec.backupSetRef`.
///
/// `status.execution.id` is the server-derived identity PLAT-06.1 froze;
/// `status.backupId` is what an older controller wrote. Both are the same
/// string for every object either of them produced, and reading the newer one
/// first is what makes a `Restore` of a legacy run still match.
fn backup_set_id(backup: &Backup) -> Option<String> {
    let status = backup.status.as_ref()?;
    status
        .execution
        .as_ref()
        .map(|e| e.id.clone())
        .or_else(|| status.backup_id.clone())
}

/// Terminal `Restore`s that recovered a point this policy protects.
///
/// # What "matching this policy's points" means, exactly
///
/// `spec.backupSetRef` is a `backupId` — the ARCHIVE SET, D3 §5.1's
/// `backup_id` — and it has to be one of the sets this policy's own member
/// runs produced. The destination has to match too: two policies over two
/// destinations can name the same schedule's `backupId` only if the same run
/// wrote to both, which it does not.
///
/// A restore of somebody else's archive is not this policy's recovery, and
/// recording it here would tell one team that another team's incident is over.
async fn read_recoveries(
    client: &kube::Client,
    namespace: &str,
    spec: &crate::crds::protection_policy::ProtectionPolicySpec,
    backup_ids: &[String],
) -> Result<Vec<p::RecoveryCompletion>, ReconcileError> {
    if backup_ids.is_empty() {
        return Ok(Vec::new());
    }
    let api: Api<Restore> = Api::namespaced(client.clone(), namespace);
    let restores = api
        .list(&ListParams::default().limit(BACKUP_PAGE_LIMIT))
        .await
        .map_err(ReconcileError::Api)?;
    let mut out: Vec<p::RecoveryCompletion> = Vec::new();
    for restore in restores.items {
        let Some(uid) = restore.uid() else { continue };
        if !backup_ids.contains(&restore.spec.backup_set_ref) {
            continue;
        }
        if !restore_destination_matches(&restore, spec) {
            continue;
        }
        let status = restore.status.as_ref();
        let phase = status.and_then(|s| s.phase.clone()).unwrap_or_default();
        if !matches!(phase.as_str(), "Succeeded" | "Failed") {
            continue;
        }
        out.push(p::RecoveryCompletion {
            restore_name: restore.name_any(),
            restore_uid: uid,
            phase,
            outcome: status.and_then(|s| s.outcome.clone()),
            at: restore_finished_at(&restore),
        });
        if out.len() >= p::MAX_RECOVERY_ALERTS {
            break;
        }
    }
    Ok(out)
}

/// When this `Restore` actually FINISHED — review **F10**.
///
/// `RestoreStatus` carries no `finishedAt`, so the terminal instant is the
/// `lastTransitionTime` of its terminal condition (`Complete` or `Failed`),
/// which the restore reconciler writes when the run ends.
/// `metadata.creationTimestamp` is the fallback and only the fallback: for a
/// long recovery it is HOURS before completion, and it is that instant that
/// lands in the ledger's `openedAt`/`resolvedAt` and in the event a responder
/// reads on D3 §3.5's incident-facing surface.
fn restore_finished_at(restore: &Restore) -> Option<Time> {
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
        .or_else(|| restore.creation_timestamp().map(|t| t.0))
}

/// The `Restore` read the archive this policy protects.
fn restore_destination_matches(
    restore: &Restore,
    spec: &crate::crds::protection_policy::ProtectionPolicySpec,
) -> bool {
    match (
        spec.protects.destination_ref.as_ref(),
        spec.protects.legacy_archive.as_ref(),
    ) {
        (Some(wanted), _) => {
            restore
                .spec
                .source_destination_ref
                .as_ref()
                .map(|d| d.name.as_str())
                == Some(&wanted.name)
        }
        (None, Some(archive)) => {
            restore.spec.source_destination_ref.is_none()
                && restore.spec.source_archive.url == archive.url
        }
        (None, None) => false,
    }
}

/// What the catalog can say about this policy's points.
async fn read_catalog(
    client: &kube::Client,
    namespace: &str,
    spec: &crate::crds::protection_policy::ProtectionPolicySpec,
    now: Time,
) -> Result<p::CatalogAnswer, ReconcileError> {
    if !spec.objectives.require_catalog_availability {
        return Ok(p::CatalogAnswer::NotConsulted);
    }
    let Some(reference) = spec.protects.catalog_ref.as_ref() else {
        return Ok(p::CatalogAnswer::NotConsulted);
    };
    let catalogs: Api<RecoveryCatalog> = Api::namespaced(client.clone(), namespace);
    let Some(catalog) = catalogs
        .get_opt(&reference.name)
        .await
        .map_err(ReconcileError::Api)?
    else {
        return Ok(p::CatalogAnswer::Stale(
            p::FreshnessReason::CatalogUnreadable,
        ));
    };
    let status = catalog.status.as_ref();
    // AN EXPIRED VIEW IS NOT A VIEW. `viewExpiresAt` is the catalog
    // controller's own statement of how long its pages describe the archive;
    // reading them past it would answer "the point is there" from a listing
    // taken before a retention run.
    let fresh = status
        .and_then(|s| s.view_expires_at)
        .is_some_and(|at| at > now);
    if !fresh {
        return Ok(p::CatalogAnswer::Stale(p::FreshnessReason::CatalogStale));
    }
    let pages: Vec<String> = status
        .and_then(|s| s.pages.as_ref())
        .map(|pages| {
            pages
                .iter()
                .take(MAX_CATALOG_PAGES)
                .map(|page| page.config_map_name.clone())
                .collect()
        })
        .unwrap_or_default();
    let maps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    let mut entries: Vec<p::CatalogEntry> = Vec::new();
    for page in &pages {
        let Some(map) = maps.get_opt(page).await.map_err(ReconcileError::Api)? else {
            // A page the status names and the API server does not have is an
            // UNREADABLE view, not an empty one: answering "no entry" would
            // read as "the point is gone".
            return Ok(p::CatalogAnswer::Stale(
                p::FreshnessReason::CatalogUnreadable,
            ));
        };
        let Some(body) = map.data.as_ref().and_then(|d| d.get(CATALOG_PAGE_DATA_KEY)) else {
            return Ok(p::CatalogAnswer::Stale(
                p::FreshnessReason::CatalogUnreadable,
            ));
        };
        for line in body.lines().filter(|l| !l.trim().is_empty()) {
            match serde_json::from_str::<p::CatalogEntry>(line) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    debug!(
                        namespace = %namespace,
                        catalog = %reference.name,
                        page = %page,
                        error = %e,
                        "a catalog page line did not parse as a view entry; it is skipped"
                    );
                }
            }
        }
    }
    Ok(p::CatalogAnswer::Fresh(entries))
}

// ===========================================================================
// Status
// ===========================================================================

/// What [`write_status`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Commit {
    /// The API server accepted the patch.
    Written,
    /// The computed status equals the one on the object; nothing was sent.
    Unchanged,
    /// Something wrote this status between the read and the write.
    Conflicted,
}

impl Commit {
    /// Whether the server's status now says what this pass computed.
    #[must_use]
    pub fn is_committed(self) -> bool {
        matches!(self, Self::Written | Self::Unchanged)
    }
}

/// Assemble the whole status block.
#[must_use]
pub fn build_status(
    policy: &ProtectionPolicy,
    verdict: &p::Verdict,
    alerts: Vec<AlertEntry>,
    now: Time,
    interval: i32,
) -> ProtectionPolicyStatus {
    let stored = policy.status.as_ref();
    let existing = stored.and_then(|s| s.conditions.as_ref());
    let generation = policy.metadata.generation;

    let ready = merge_condition(
        current_condition(existing, CONDITION_READY),
        Condition {
            r#type: CONDITION_READY.to_string(),
            status: "True".to_string(),
            observed_generation: generation,
            last_transition_time: Some(now),
            reason: Some(REASON_EVALUATED.to_string()),
            // `condition_summary` AND NOT `summary` — review F4. A condition
            // `message` is part of the object; the event's sentence carries the
            // concrete age and would make the status differ every time a minute
            // rolled over (erratum E11(d)).
            message: Some(verdict.condition_summary.clone()),
        },
    );
    let protected = merge_condition(
        current_condition(existing, CONDITION_PROTECTED),
        Condition {
            r#type: CONDITION_PROTECTED.to_string(),
            status: verdict.health.condition_status().to_string(),
            observed_generation: generation,
            last_transition_time: Some(now),
            reason: Some(verdict.reason.as_str().to_string()),
            message: Some(verdict.condition_summary.clone()),
        },
    );
    let mut conditions = vec![ready, protected];
    // The withdrawn sinks, said by name. The field still parses (an older
    // policy keeps working); it is read for nothing but this condition.
    if policy.spec.notifications.is_some() {
        conditions.push(merge_condition(
            current_condition(existing, CONDITION_NOTIFICATIONS_DELIVERED),
            Condition {
                r#type: CONDITION_NOTIFICATIONS_DELIVERED.to_string(),
                status: "False".to_string(),
                observed_generation: generation,
                last_transition_time: Some(now),
                reason: Some(REASON_NOTIFICATIONS_WITHDRAWN.to_string()),
                message: Some(NOTIFICATIONS_WITHDRAWN_MESSAGE.to_string()),
            },
        ));
    }

    let status = ProtectionPolicyStatus {
        observed_generation: generation,
        evaluated_at: None,
        health: Some(verdict.health.as_str().to_string()),
        availability_basis: Some(verdict.basis.as_str().to_string()),
        last_available_point: verdict.point.as_ref().map(p::available_point_status),
        last_attempt: verdict.last_attempt.clone(),
        consecutive_failed_runs: Some(verdict.consecutive_failed_runs),
        missed: (verdict.missed.last_missed_slot.is_some()
            || verdict.missed.since_last_fire.is_some())
        .then(|| verdict.missed.clone()),
        schedules: (!verdict.schedules.is_empty()).then(|| verdict.schedules.clone()),
        rehearsal: verdict.rehearsal.clone(),
        stale_since: stale_since(stored, verdict.health, now),
        alerts: (!alerts.is_empty()).then_some(alerts),
        conditions: Some(conditions),
    };
    settle_clock_fields(stored, status, interval, now)
}

/// Decide `evaluatedAt` and the two fields measured FROM it, together.
///
/// # D3 §3.1's rule, and the two fields that were outside it (review F4)
///
/// `evaluatedAt` is "rewritten only on change or when older than interval/2".
/// `lastAvailablePoint.ageSeconds` and `missed.sinceLastFire` are both
/// documented as "at `evaluatedAt`" — they are *derived from that instant*, not
/// from `now` — but they were computed from a live clock read, so a pass over
/// unchanged cluster state produced a different status every second: a PATCH,
/// a `resourceVersion` bump, the reconciler's own write waking it again. That
/// is the measured `KafkaCluster` defect (erratum **E11(d)**) on every policy,
/// for ever, and the test that claimed otherwise passed only because it froze
/// the clock.
///
/// All three move together or none of them does. The comparison is over the
/// computed status with all three stripped, so "did the VERDICT change?" is
/// asked about the cluster and not about the clock. When nothing changed and
/// the stored instant is younger than half the interval, the stored values for
/// all three are restored and [`crate::conditions::status_unchanged`] then
/// sends nothing at all.
fn settle_clock_fields(
    stored: Option<&ProtectionPolicyStatus>,
    mut next: ProtectionPolicyStatus,
    interval: i32,
    now: Time,
) -> ProtectionPolicyStatus {
    let fresh = |stored_at: Time| {
        let half = i64::from(interval).max(2) / 2;
        (now - stored_at).num_seconds() < half
    };
    let keep = stored
        .filter(|s| s.evaluated_at.is_some_and(fresh) && verdict_bytes(s) == verdict_bytes(&next));
    match keep {
        Some(stored) => {
            next.evaluated_at = stored.evaluated_at;
            if let (Some(point), Some(stored_point)) = (
                next.last_available_point.as_mut(),
                stored.last_available_point.as_ref(),
            ) {
                point.age_seconds = stored_point.age_seconds;
            }
            if let (Some(missed), Some(stored_missed)) =
                (next.missed.as_mut(), stored.missed.as_ref())
            {
                missed.since_last_fire = stored_missed.since_last_fire;
            }
        }
        None => next.evaluated_at = Some(now),
    }
    next
}

/// `staleSince` — set the first time health becomes `Stale` or `Unprotected`
/// and KEPT while it stays there; cleared when it does not.
///
/// It is a "since when" and not a heartbeat: rewriting it on every pass would
/// make every pass a write, and a reconciler's own status patch is what wakes
/// it (erratum **E11(d)**).
fn stale_since(
    stored: Option<&ProtectionPolicyStatus>,
    health: p::Health,
    now: Time,
) -> Option<Time> {
    if !matches!(health, p::Health::Stale | p::Health::Unprotected) {
        return None;
    }
    stored.and_then(|s| s.stale_since).or(Some(now))
}

/// A status serialized with every CLOCK-DERIVED field removed, for the
/// equality [`settle_clock_fields`] asks.
///
/// The three: `evaluatedAt`, `lastAvailablePoint.ageSeconds` and
/// `missed.sinceLastFire`. Each is a measurement of an instant against a stored
/// fact, so each moves on its own with no change to the cluster; comparing them
/// would make "did the verdict change?" answer "yes" for ever.
fn verdict_bytes(status: &ProtectionPolicyStatus) -> Value {
    let mut value = serde_json::to_value(status).unwrap_or(Value::Null);
    if let Some(point) = value
        .get_mut("lastAvailablePoint")
        .and_then(Value::as_object_mut)
    {
        point.remove("ageSeconds");
    }
    if let Some(missed) = value.get_mut("missed").and_then(Value::as_object_mut) {
        missed.remove("sinceLastFire");
    }
    if let Some(map) = value.as_object_mut() {
        map.remove("evaluatedAt");
    }
    value
}

/// Add the optimistic-concurrency precondition to a `/status` merge patch —
/// D-SEAMS **S7**.
///
/// The API server applies a `metadata.resourceVersion` carried in a patch BODY
/// as an update precondition and answers **409** on a mismatch, which is how a
/// merge PATCH gets a compare-and-set without the `update` verb this role
/// grants on nothing.
fn status_patch_with_preconditions(
    policy: &ProtectionPolicy,
    mut patch: Value,
) -> Result<Value, Box<kube::Error>> {
    let name = policy.name_any();
    let resource_version = policy
        .metadata
        .resource_version
        .clone()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            Box::new(kube::Error::Discovery(
                kube::error::DiscoveryError::MissingResource(format!(
                    "ProtectionPolicy {name} carries no metadata.resourceVersion, which a \
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

/// Every `status` key that this pass may need to REMOVE, and therefore must
/// spell as an explicit `null` when it computed `None`.
///
/// # An omitted key is "leave it alone", not "clear it" (review F3)
///
/// Every field of `ProtectionPolicyStatus` is `skip_serializing_if =
/// "Option::is_none"`, and RFC 7386 removes a key only for an explicit `null`.
/// So a status serialized straight to a merge patch could SET each of these and
/// never clear one — and the consequences are the two claims this controller
/// exists to avoid making:
///
/// * `lastAvailablePoint` survived a verdict that just decided no point is
///   available, so the API and the console kept naming a recovery point while
///   `health` read `Unknown` — the exact conflation D3 §3.2 is written against,
///   on the surface an incident responder reads;
/// * `staleSince` from a long-resolved outage sat on a `Healthy` policy,
///   contradicting its own field documentation.
///
/// `conditions` is deliberately NOT here: this controller always writes them,
/// so a `null` would be a removal that never applies, and an array in a merge
/// patch is replaced wholesale anyway.
///
/// `metadata` is not a status key and never appears in this list;
/// [`status_patch_with_preconditions`] adds it to the body afterwards.
pub const CLEARABLE_STATUS_FIELDS: [&str; 7] = [
    "lastAvailablePoint",
    "lastAttempt",
    "missed",
    "schedules",
    "rehearsal",
    "staleSince",
    "alerts",
];

/// The `{"status": …}` merge-patch body, with an explicit `null` for every
/// clearable field this pass computed as `None` — and, since FX-29, for every
/// key at ANY depth that `stored` carries and this status does not.
///
/// [`crate::conditions::status_unchanged`] applies the body exactly as the API
/// server would, so the no-op skip keeps working: a `null` for a key the object
/// does not have is itself a no-op.
///
/// # Why the seven named fields were not enough (FX-29's class sweep)
///
/// No optional field INSIDE `lastAvailablePoint`, `lastAttempt`, `missed` or
/// `rehearsal` is among them. Each of those, once it became `None`, stayed on the object under the merge — and
/// [`settle_clock_fields`] compares the stored status with the computed one,
/// so the stale key made "did the verdict change?" answer yes on every pass:
/// `evaluatedAt` was `now` each time, and each write woke this reconciler's own
/// watch. This controller is the only writer of the status, so
/// [`crate::conditions::replacing`] makes the body replace what is stored.
#[must_use]
pub fn status_patch_body(
    status: &ProtectionPolicyStatus,
    stored: Option<&ProtectionPolicyStatus>,
) -> Value {
    let mut body = serde_json::to_value(status).unwrap_or(Value::Null);
    if let Some(map) = body.as_object_mut() {
        for field in CLEARABLE_STATUS_FIELDS {
            map.entry(field.to_string()).or_insert(Value::Null);
        }
    }
    let stored = stored.and_then(|s| serde_json::to_value(s).ok());
    json!({ "status": crate::conditions::replacing(body, stored.as_ref()) })
}

/// Patch `/status`, with the S7 precondition and the no-op skip.
async fn write_status(
    api: &Api<ProtectionPolicy>,
    policy: &ProtectionPolicy,
    status: &ProtectionPolicyStatus,
) -> Result<Commit, ReconcileError> {
    let name = policy.name_any();
    let patch = status_patch_body(status, policy.status.as_ref());
    if status_unchanged(
        policy
            .status
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok())
            .as_ref(),
        &patch,
    ) {
        debug!(
            policy = %name,
            "the computed protection status equals the one on the object; no patch is sent"
        );
        return Ok(Commit::Unchanged);
    }
    let body =
        status_patch_with_preconditions(policy, patch).map_err(|e| ReconcileError::Api(*e))?;
    match api
        .patch_status(&name, &PatchParams::default(), &Patch::Merge(body))
        .await
    {
        Ok(_) => Ok(Commit::Written),
        Err(kube::Error::Api(e)) if e.code == 409 => {
            debug!(
                policy = %name,
                "the status changed under this reconcile (409); the next pass reads it"
            );
            Ok(Commit::Conflicted)
        }
        Err(e) => Err(ReconcileError::Api(e)),
    }
}

// ===========================================================================
// Registration
// ===========================================================================

async fn reconcile(
    policy: Arc<ProtectionPolicy>,
    ctx: Arc<ProtectionContext>,
) -> Result<Action, ReconcileError> {
    let outcome = reconcile_policy(&policy, &ctx, chrono::Utc::now()).await?;
    Ok(policy_action(&outcome))
}

/// The [`Action`] a pass returns: a timed requeue of the outcome's
/// `requeue_seconds` — the evaluation interval — never `await_change()`.
///
/// A recovery point AGES with no event on any object, so this requeue is
/// what turns a policy `Stale` when its newest point crosses the objective
/// (FX-29 review M-1, pinned by
/// `protection_controller::a_point_that_ages_out_is_seen_by_the_timed_requeue`).
#[must_use]
pub fn policy_action(outcome: &Outcome) -> Action {
    Action::requeue(std::time::Duration::from_secs(outcome.requeue_seconds))
}

/// The [`Action`] a failed pass returns: a timed requeue of
/// [`ERROR_REQUEUE_SECONDS`].
#[must_use]
pub fn policy_error_action() -> Action {
    Action::requeue(std::time::Duration::from_secs(ERROR_REQUEUE_SECONDS))
}

fn error_policy(
    policy: Arc<ProtectionPolicy>,
    err: &ReconcileError,
    _ctx: Arc<ProtectionContext>,
) -> Action {
    warn!(
        policy = %policy.name_any(),
        error = %err,
        "protection policy reconcile failed; requeueing"
    );
    policy_error_action()
}

/// Run the `ProtectionPolicy` controller until the process ends.
///
/// One watch per watched namespace (`crate::scope`), like every other
/// controller in this directory.
pub fn controller(client: kube::Client) -> impl std::future::Future<Output = ()> + Send {
    crate::scope::run_everywhere(move |namespace| controller_in(client.clone(), namespace))
}

/// One watch of [`controller`], over `namespace` (`None` is the whole
/// cluster, the behaviour before D0 stage 5).
fn controller_in(
    client: kube::Client,
    namespace: Option<String>,
) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<ProtectionPolicy> = crate::scope::api(&client, namespace.as_deref());
    let ctx = Arc::new(ProtectionContext { client });
    async move {
        Controller::new(api, watcher::Config::default())
            .run(reconcile, error_policy, ctx)
            .for_each(|_| std::future::ready(()))
            .await;
    }
}
