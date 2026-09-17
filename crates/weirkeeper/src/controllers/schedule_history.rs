//! Retained history: the schedule's runs stop being the schedule's children.
//!
//! PLAT-05.2, D1 §6. Until this module a `Backup` a schedule created carried a
//! controller ownerReference to that schedule, so `kubectl delete
//! backupschedule nightly` handed every run, every plan ConfigMap and every Job
//! to the API server's garbage collector. The recovery points survived in
//! object storage, but the record of which run wrote them did not — and
//! `docs/kubernetes.md` had to tell operators never to delete a schedule.
//!
//! # The three jobs this module does
//!
//! 1. **Migration** (§6.2). Every `Backup` that still carries the legacy owner
//!    entry is detached from it by ONE merge PATCH per object, carrying the
//!    object's own `resourceVersion` as the precondition, rewriting
//!    `ownerReferences` to everything EXCEPT the matching entry, and adding the
//!    `logweir.dev/schedule-uid` label and the
//!    `logweir.dev/history-retained-from-owner` annotation that
//!    [`crate::identity::is_run_of_schedule`] reads as membership rule 3.
//!    Progress is derived from observation and never from a cursor, so a crash
//!    between two patches leaves every object either fully migrated or
//!    untouched.
//! 2. **Inventory** (§6.7). One paginated, bounded list per schedule, at most
//!    once an hour in steady state, that counts the history, estimates what it
//!    costs, repairs `status.activeRuns`, and finds the runs migration 1 has
//!    left. It replaces the namespace-wide unbounded LIST the scheduler used to
//!    take whenever `status.activeRuns` was absent.
//! 3. **`HistoryRetained`** (§6.1/§6.3). The condition that says whether
//!    deleting this schedule would now be safe, computed on EVERY reconcile
//!    from [`ScheduleHistory`] rather than carried forward from the stored
//!    condition.
//!
//! # NOTHING HERE DELETES ANYTHING, AND THE GRANT SAYS SO
//!
//! §6.5: no Logweir component deletes a `Backup`, a ConfigMap or an archive
//! object. `config/rbac/role.yaml` grants this controller `create` and `patch`
//! on `backups` and no `delete` on any resource, and
//! `scripts/check-no-archive-write.sh` plus `manifest_lint`'s two delete
//! assertions keep that true. The one write this module makes is a metadata
//! merge PATCH: it cannot reach a sealed `spec` (CEL would refuse it) and it
//! cannot reach a `status` (that is a subresource with its own rule).
//!
//! # Why the migration only touches TERMINAL runs
//!
//! §6.2 step 2. A legacy `Backup` that has not frozen its plan yet derives its
//! scheduled identity from the owner UID — [`crate::identity::run_identity`]'s
//! rule 3 last clause — so removing the entry mid-flight would leave a run with
//! no identity at all, and the `Backup` reconciler would refuse it
//! `ScheduledIdentityMismatch` before its POST. A nonterminal legacy run
//! therefore keeps its owner until it is terminal, and the condition says
//! `ActiveLegacyRunsOwned` while that is the only thing left.

use chrono::{DateTime, Duration, Utc};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::{ListParams, Patch, PatchParams};
use kube::{Api, Resource as _, ResourceExt as _};
use serde_json::json;
use tracing::{debug, warn};

use crate::conditions::{
    current_condition, merge_condition, CONDITION_HISTORY_RETAINED,
    REASON_ACTIVE_LEGACY_RUNS_OWNED, REASON_HISTORY_LARGE, REASON_LEGACY_OWNER_REFERENCES_REMAIN,
    REASON_MIGRATION_BLOCKED, REASON_RETAINED,
};
use crate::crds::backup::Backup;
use crate::crds::backup_schedule::{
    ActiveRun, BackupSchedule, BackupScheduleStatus, MigrationBlocked, ScheduleHistory,
};
use crate::crds::Condition;
use crate::identity::{is_run_of_schedule, RETAINED_FROM_OWNER_ANNOTATION, SCHEDULE_UID_LABEL};

/// How long an inventory stays fresh (D1 §6.7: "every 60 min").
pub const INVENTORY_INTERVAL_SECS: i64 = 3_600;

/// The `limit` every inventory page carries (D1 §6.7).
///
/// A LIST WITHOUT ONE IS THE DEFECT THIS MODULE EXISTS TO REMOVE. The API
/// server streams every matching object into one response body, so an
/// unbounded list of a schedule with a year of 15-minute history is a
/// multi-megabyte read taken every reconcile.
pub const INVENTORY_PAGE_SIZE: u32 = 500;

/// How many pages one inventory follows before it stops and says so.
///
/// TEN THOUSAND OBJECTS IS A BOUND AND NOT A TARGET. D1 §6.6 already calls
/// 2 000 runs per schedule "large"; a schedule past 10 000 is one whose
/// operator has been told to prune for a long time, and the honest answer for
/// it is a floor plus `runCountCapped`, not a reconcile that reads for a
/// minute and times out.
pub const MAX_INVENTORY_PAGES: usize = 20;

/// How many migration PATCHes one pass sends.
///
/// A CAP PLUS A PROMPT RE-INVENTORY, not one unbounded burst. An upgrade over a
/// schedule with 35 040 legacy runs (D1 §6.6's worst row) would otherwise send
/// 35 040 sequential PATCHes inside ONE reconcile, holding that schedule's slot
/// evaluation for minutes. [`inventory_due`] returns `true` while
/// `legacyMigratableRuns` is positive, so the next pass — 30 s later, the
/// scheduler's requeue — continues from where observation says it got to.
pub const MAX_MIGRATIONS_PER_PASS: usize = 200;

/// D1 §6.6: above this many retained runs the condition is `HistoryLarge`.
pub const HISTORY_LARGE_RUNS: i64 = 2_000;

/// D1 §6.6: above this many estimated bytes the condition is `HistoryLarge`.
pub const HISTORY_LARGE_BYTES: i64 = 64 * 1024 * 1024;

/// D1 §6.2 step 4: how many blocked runs the status and the message name.
pub const MIGRATION_BLOCKED_SAMPLE: usize = 10;

/// D1 §6.6: the per-run overhead added to the serialized object — the plan
/// ConfigMap's fixed part.
const PLAN_OVERHEAD_BYTES: i64 = 2_048;

/// What step 1 observed, whichever branch it took.
#[derive(Clone, Debug)]
pub struct Observation {
    /// Every nonterminal schedule-created run, at most ten, sorted by name.
    pub active: Vec<ActiveRun>,
    /// The reservation's child, if the caller named one and it exists.
    pub pending_child: Option<Backup>,
    /// The history block the final status write carries.
    pub history: ScheduleHistory,
    /// Whether this pass took an inventory (a paginated LIST) rather than the
    /// O(active) GETs.
    pub inventoried: bool,
}

/// Why a step-1 read could not complete.
#[derive(Debug)]
pub enum HistoryError {
    /// Anything the API server said that was not a 404 on a named object.
    Api(kube::Error),
    /// The reservation's deterministic name is held by an object this schedule
    /// does not own.
    ForeignBackup(String),
}

impl From<kube::Error> for HistoryError {
    fn from(e: kube::Error) -> Self {
        Self::Api(e)
    }
}

impl std::fmt::Display for HistoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Api(e) => write!(f, "{e}"),
            Self::ForeignBackup(name) => {
                write!(
                    f,
                    "`{name}` is held by an object this schedule does not own"
                )
            }
        }
    }
}

/// Whether this reconcile must inventory before it admits anything
/// (D1 §4.5 step 1).
///
/// # Four reasons, and the one D1 names that is deliberately absent
///
/// 1. **No `history` block.** The inventory has never run against this
///    schedule, so nothing is known about its legacy owner entries. This is the
///    upgrade case, and it is what replaces the bootstrap LIST.
/// 2. **No `activeRuns` block.** The O(active) branch reads that list; an
///    absent one is "never computed" and not "computed, and empty" (the final
///    status always writes it, `[]` included), so there is nothing to read.
/// 3. **`legacyMigratableRuns > 0`.** There are terminal runs still carrying
///    the owner entry, and a pass can patch them now. A pass that was capped by
///    [`MAX_MIGRATIONS_PER_PASS`], and a pass whose PATCH lost a `409`, both
///    land here and continue on the next reconcile. Runs that are only
///    NONTERMINAL do not, which is D1 §6.2's "never retried faster than the
///    inventory interval": they are waiting on a Job, not on this controller.
/// 4. **The last inventory is older than [`INVENTORY_INTERVAL_SECS`].**
///
/// D1 also says "or this process has not inventoried this schedule since
/// start". THAT RULE IS NOT IMPLEMENTED, and the reason is that it buys
/// nothing the status does not already give while costing process-global
/// mutable state that no test can see. What a fresh process needs from an
/// inventory is a repaired `activeRuns`, and rule 2 is exactly that: a crash
/// that lost the status lost `activeRuns` with it, and a restart that did NOT
/// lose it is reading a list that is complete by construction, because every
/// schedule-created run is recorded by a resourceVersion-conditional status
/// write BEFORE it is created.
#[must_use]
pub fn inventory_due(stored: Option<&BackupScheduleStatus>, now: DateTime<Utc>) -> bool {
    let Some(status) = stored else {
        return true;
    };
    if status.active_runs.is_none() {
        return true;
    }
    let Some(history) = status.history.as_ref() else {
        return true;
    };
    history.legacy_migratable_runs > 0
        || now - history.inventoried_at >= Duration::seconds(INVENTORY_INTERVAL_SECS)
}

/// Whether the inventory may narrow its list to the UID label (D1 §6.7).
///
/// A LEGACY OBJECT CARRIES NO UID LABEL. Only runs this controller created
/// carry `logweir.dev/schedule-uid`, and the whole point of the migration is
/// the ones that do not — so until the condition has said `HistoryRetained=True`
/// the list must be namespace-wide, and after it the label selector is what
/// makes the read O(this schedule) instead of O(namespace).
#[must_use]
pub fn may_use_label_selector(stored: Option<&BackupScheduleStatus>) -> bool {
    current_condition(
        stored.and_then(|s| s.conditions.as_ref()),
        CONDITION_HISTORY_RETAINED,
    )
    .is_some_and(|c| c.status == "True")
}

/// D1 §6.6's per-run etcd estimate: the serialized object, the plan
/// ConfigMap's fixed part, and the topic names twice (they appear in
/// `backup.yaml` and in `execution-inputs.json`).
///
/// `spec.topics` AND NOT `status.selection`, because PLAT-09.2 has not landed:
/// a dynamically-selected run has an empty `spec.topics` and its resolved names
/// are not on the object yet, so its estimate is a floor. W5 adds the second
/// term; the shape here is the one D1 §6.6 names.
#[must_use]
pub fn estimated_bytes(backup: &Backup) -> i64 {
    let serialized =
        serde_json::to_string(backup).map_or(0, |s| i64::try_from(s.len()).unwrap_or(i64::MAX / 4));
    let topic_bytes: i64 = backup
        .spec
        .topics
        .iter()
        .map(|t| i64::try_from(t.len() + 6).unwrap_or(0))
        .sum();
    serialized
        .saturating_add(PLAN_OVERHEAD_BYTES)
        .saturating_add(topic_bytes.saturating_mul(2))
}

/// The index of the controller ownerReference naming this schedule, if any.
///
/// THE WHOLE TRIPLE, NOT THE UID ALONE. `apiVersion`, `kind`, `name` and `uid`
/// all have to match, because removing an entry on a UID match alone would
/// strip an ownerReference some other operator wrote that happened to carry the
/// same string — and because `is_owned_by_schedule` recognises membership by
/// exactly this triple, so anything looser would detach an object that was
/// never a member.
#[must_use]
pub fn owner_entry_index(
    backup: &Backup,
    schedule_name: &str,
    schedule_uid: &str,
) -> Option<usize> {
    backup
        .metadata
        .owner_references
        .as_deref()?
        .iter()
        .position(|owner| {
            owner.controller == Some(true)
                && owner.uid == schedule_uid
                && owner.name == schedule_name
                && owner.kind == BackupSchedule::kind(&())
                && owner.api_version == BackupSchedule::api_version(&())
        })
}

/// Why one `Backup` may not be migrated even though it carries the entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationRefusal {
    /// The run has not reached a terminal phase, so its scheduled identity
    /// still derives from the owner UID (D1 §6.2 step 2).
    NotTerminal,
    /// `spec.scheduleRef.name` does not name this schedule, so membership rule
    /// 3 would not hold after the entry is gone and the run would be orphaned.
    NoScheduleReference,
}

impl MigrationRefusal {
    /// The string the status records.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotTerminal => "NotTerminal",
            Self::NoScheduleReference => "NoScheduleReference",
        }
    }
}

/// Whether this legacy-owned `Backup` may be detached now, or why not.
///
/// # The second refusal is a hazard D1 §6.2 does not name, and it orphans history
///
/// [`crate::identity::is_run_of_schedule`]'s rule 3 is "the migration
/// annotation carries the UID **and `spec.scheduleRef.name` matches**". A
/// legacy object whose `spec.scheduleRef` names something else — or is absent —
/// is a member today only through rule 2, the ownerReference. Removing that
/// entry would therefore make it a member of NOTHING: not inventoried, not in
/// any history view, not restorable through the schedule. So it keeps its owner
/// and is recorded as blocked, which is visible, rather than silently detached,
/// which is not.
#[must_use]
pub fn migration_refusal(backup: &Backup, schedule_name: &str) -> Option<MigrationRefusal> {
    if !crate::controllers::backup_schedule::backup_is_terminal(backup) {
        return Some(MigrationRefusal::NotTerminal);
    }
    if backup
        .spec
        .schedule_ref
        .as_ref()
        .is_none_or(|r| r.name != schedule_name)
    {
        return Some(MigrationRefusal::NoScheduleReference);
    }
    None
}

/// The merge PATCH body that detaches one `Backup` from its schedule
/// (D1 §6.2 step 3), or `None` when the object carries no matching entry.
///
/// # PURE, AND THAT IS WHY IT IS A FUNCTION AND NOT A STATEMENT IN THE LOOP
///
/// The body a test asserts over is byte-identical to the one the API server
/// receives. The three properties that matter are all assertions over this
/// value: the `resourceVersion` precondition is present, the surviving
/// `ownerReferences` entries are byte-identical to the observed ones, and no
/// key outside `metadata.ownerReferences`, `metadata.labels` and
/// `metadata.annotations` appears at all.
///
/// # Merge-patch semantics, field by field
///
/// * `ownerReferences` is an ARRAY, so RFC 7386 replaces it wholesale. It is
///   rewritten from the OBSERVED object under the `resourceVersion`
///   precondition, which is what makes "remove one entry" safe against a
///   concurrent writer: a racing edit bumps the version and this patch loses
///   with a `409`.
/// * The empty case is `null` and not `[]`, per D1 §6.2 step 3. Both leave the
///   object with no owners; `null` is the spelling the decision names.
/// * `labels` and `annotations` are OBJECTS, so the two keys below are merged
///   into whatever is there. Every other label and annotation survives without
///   being named, which is what
///   `foreign_owner_references_labels_and_annotations_are_preserved_byte_for_byte`
///   asserts.
#[must_use]
pub fn migration_patch(
    backup: &Backup,
    schedule_name: &str,
    schedule_uid: &str,
) -> Option<serde_json::Value> {
    let index = owner_entry_index(backup, schedule_name, schedule_uid)?;
    let resource_version = backup.metadata.resource_version.clone()?;
    let survivors: Vec<&OwnerReference> = backup
        .metadata
        .owner_references
        .as_deref()
        .unwrap_or_default()
        .iter()
        .enumerate()
        .filter_map(|(i, owner)| (i != index).then_some(owner))
        .collect();
    let owners = if survivors.is_empty() {
        serde_json::Value::Null
    } else {
        json!(survivors)
    };
    Some(json!({
        "metadata": {
            "resourceVersion": resource_version,
            "ownerReferences": owners,
            "labels": { SCHEDULE_UID_LABEL: schedule_uid },
            "annotations": { RETAINED_FROM_OWNER_ANNOTATION: schedule_uid },
        }
    }))
}

/// The `HistoryRetained` condition one history block implies (D1 §6.1, §6.3,
/// §3.4).
///
/// # COMPUTED, NEVER COPIED
///
/// W2 left this condition as a carry-forward: its status builder emits a merge
/// patch, a merge patch REPLACES `status.conditions`, and a builder that
/// emitted only `Ready` would delete whatever this module wrote. The
/// carry-forward was the right no-op while nothing computed the condition, and
/// it is the wrong steady state — a copied condition outlives the facts it was
/// derived from, so a schedule whose last legacy run finished an hour ago would
/// still read `ActiveLegacyRunsOwned` until something else happened to move the
/// status. This function is a pure map from the recorded block, applied on
/// EVERY reconcile, inventory pass or not.
///
/// # The order of the five arms
///
/// A blocked migration is reported first because it is the only one that needs
/// a human: `403` and `422` do not clear themselves. Then the two counts, most
/// actionable first — terminal runs the controller can still detach, then runs
/// it must wait for. `HistoryLarge` is a WARNING on a True condition: the
/// history IS retained, and §6.6's point is that retaining it is not free.
#[must_use]
pub fn history_condition(
    history: &ScheduleHistory,
    existing: Option<&Condition>,
    observed_generation: Option<i64>,
    now: DateTime<Utc>,
) -> Condition {
    let blocked = history.migration_blocked.as_deref().unwrap_or_default();
    let (status, reason, message) = if !blocked.is_empty() {
        let names = blocked
            .iter()
            .take(MIGRATION_BLOCKED_SAMPLE)
            .map(|b| format!("{} ({})", b.name, b.reason))
            .collect::<Vec<_>>()
            .join(", ");
        (
            "False",
            REASON_MIGRATION_BLOCKED,
            format!(
                "{} run(s) of this schedule could not be detached from it: {names}. Deleting \
                 this schedule without `--cascade=orphan` would still collect them.",
                blocked.len()
            ),
        )
    } else if history.legacy_migratable_runs > 0 {
        (
            "False",
            REASON_LEGACY_OWNER_REFERENCES_REMAIN,
            format!(
                "{} of {} retained run(s) still carry this schedule's ownerReference and are \
                 being detached. Use `kubectl delete backupschedule <name> --cascade=orphan` \
                 until this is `True`.",
                history.legacy_migratable_runs, history.run_count
            ),
        )
    } else if history.legacy_owned_runs > 0 {
        (
            "False",
            REASON_ACTIVE_LEGACY_RUNS_OWNED,
            format!(
                "{} run(s) created before this controller are still active and keep their \
                 ownerReference until they are terminal; their scheduled identity derives from \
                 it.",
                history.legacy_owned_runs
            ),
        )
    } else if history.run_count > HISTORY_LARGE_RUNS
        || history.estimated_bytes > HISTORY_LARGE_BYTES
        // A CAPPED WALK IS LARGE BY CONSTRUCTION. The inventory stopped only
        // because the API server was still handing it `continue` tokens after
        // MAX_INVENTORY_PAGES × INVENTORY_PAGE_SIZE objects, so the real count
        // is above the advisory whatever the floor below it says.
        || history.run_count_capped
    {
        (
            "True",
            REASON_HISTORY_LARGE,
            format!(
                "History is retained: deleting this schedule leaves all {}{} run(s) in place. \
                 They are estimated at {} bytes, which is above the {HISTORY_LARGE_RUNS}-run / \
                 {HISTORY_LARGE_BYTES}-byte advisory — see `docs/kubernetes.md` §9 for the \
                 pruning recipe.",
                history.run_count,
                if history.run_count_capped { "+" } else { "" },
                history.estimated_bytes
            ),
        )
    } else {
        (
            "True",
            REASON_RETAINED,
            format!(
                "History is retained: no run is owned by this schedule, so deleting it leaves \
                 all {} run(s), their plan ConfigMaps and their archives in place.",
                history.run_count
            ),
        )
    };
    merge_condition(
        existing,
        Condition {
            r#type: CONDITION_HISTORY_RETAINED.to_string(),
            status: status.to_string(),
            observed_generation,
            last_transition_time: Some(now),
            reason: Some(reason.to_string()),
            message: Some(message),
        },
    )
}

/// D1 §4.5 step 1 and §6.7: refresh the active set, and inventory when due.
///
/// # The two branches
///
/// **Steady state** GETs the ≤ 10 names `status.activeRuns` records plus the
/// reservation. That is O(active) and independent of how much history the
/// schedule has, which is the whole point of §6.7: listing every `Backup` every
/// 30 s stops being affordable the moment history is retained.
///
/// **Inventory** takes one paginated list, migrates what it can, and rebuilds
/// both the active set and the history block from what it saw. It is the only
/// LIST this controller makes, it carries a `limit`, and it follows at most
/// [`MAX_INVENTORY_PAGES`] pages.
///
/// # Correctness does not depend on list freshness
///
/// Every schedule-created run is recorded in `pendingRun`/`activeRuns` by a
/// resourceVersion-conditional status write BEFORE it is created, so admission
/// always sees the most recent admitted run whichever branch ran. The inventory
/// repairs a status that was lost; it is not what makes admission safe.
///
/// # Errors
///
/// [`HistoryError::Api`] for anything the API server refused that is not a 404
/// on a named object, and [`HistoryError::ForeignBackup`] when the reservation's
/// name is held by an object this schedule does not own.
pub async fn observe(
    api: &Api<Backup>,
    stored: Option<&BackupScheduleStatus>,
    schedule_name: &str,
    schedule_uid: &str,
    pending: Option<&str>,
    now: DateTime<Utc>,
) -> Result<Observation, HistoryError> {
    if inventory_due(stored, now) {
        inventory(api, stored, schedule_name, schedule_uid, pending, now).await
    } else {
        steady_state(api, stored, schedule_name, schedule_uid, pending, now).await
    }
}

/// The O(active) branch: GET what the status already names.
async fn steady_state(
    api: &Api<Backup>,
    stored: Option<&BackupScheduleStatus>,
    schedule_name: &str,
    schedule_uid: &str,
    pending: Option<&str>,
    now: DateTime<Utc>,
) -> Result<Observation, HistoryError> {
    let recorded = stored
        .and_then(|s| s.active_runs.as_deref())
        .unwrap_or_default();
    let mut active: Vec<ActiveRun> = Vec::new();
    let mut pending_child: Option<Backup> = None;

    for entry in recorded.iter().take(MAX_ACTIVE_RUNS) {
        if let Some(backup) = api.get_opt(&entry.name).await? {
            if member_of_concurrency(&backup, schedule_name, schedule_uid) {
                active.push(active_entry(&backup));
            }
            if Some(entry.name.as_str()) == pending {
                pending_child = Some(backup);
            }
        }
    }
    if let Some(pending) = pending {
        if pending_child.is_none() && !recorded.iter().any(|e| e.name == pending) {
            if let Some(backup) = api.get_opt(pending).await? {
                if !crate::controllers::backup_schedule::participates_in_concurrency(
                    &backup,
                    schedule_name,
                    schedule_uid,
                ) {
                    return Err(HistoryError::ForeignBackup(pending.to_string()));
                }
                if member_of_concurrency(&backup, schedule_name, schedule_uid) {
                    active.push(active_entry(&backup));
                }
                pending_child = Some(backup);
            }
        }
    }

    // THE HISTORY BLOCK IS CARRIED, NOT RECOMPUTED, and that is not the same
    // thing as carrying the CONDITION. A steady pass took no inventory, so it
    // has no new facts; writing back what the last inventory recorded is what
    // keeps `status_unchanged` true and the schedule unpatched. The condition
    // is then recomputed FROM these facts, which is why a human who edits this
    // block sees the condition follow.
    let history = stored
        .and_then(|s| s.history.clone())
        .unwrap_or_else(|| empty_history(now));
    Ok(Observation {
        active: bounded(active),
        pending_child,
        history,
        inventoried: false,
    })
}

/// The paginated branch: one bounded list, the migration, and a fresh block.
async fn inventory(
    api: &Api<Backup>,
    stored: Option<&BackupScheduleStatus>,
    schedule_name: &str,
    schedule_uid: &str,
    pending: Option<&str>,
    now: DateTime<Utc>,
) -> Result<Observation, HistoryError> {
    let label_selected = may_use_label_selector(stored);
    let mut active: Vec<ActiveRun> = Vec::new();
    let mut pending_child: Option<Backup> = None;
    let mut run_count: i64 = 0;
    let mut estimated: i64 = 0;
    let mut owned_nonterminal: i64 = 0;
    let mut migratable: Vec<Backup> = Vec::new();
    let mut blocked: Vec<MigrationBlocked> = Vec::new();
    let mut capped = false;
    let mut token: Option<String> = None;

    for page in 0..MAX_INVENTORY_PAGES {
        let mut params = ListParams::default().limit(INVENTORY_PAGE_SIZE);
        if label_selected {
            params = params.labels(&format!("{SCHEDULE_UID_LABEL}={schedule_uid}"));
        }
        if let Some(cursor) = token.as_deref() {
            params = params.continue_token(cursor);
        }
        let listed = api.list(&params).await?;
        for backup in &listed.items {
            let this = backup.name_any();
            // MEMBERSHIP IS ASKED OF EVERY OBJECT, LABEL SELECTOR OR NOT. The
            // selector is a hint the API server applies; the authority is
            // `is_run_of_schedule`, and a namespace-wide page is full of other
            // schedules' runs and of manual runs that belong to nobody.
            if !is_run_of_schedule(backup, schedule_name, schedule_uid) {
                continue;
            }
            run_count += 1;
            estimated = estimated.saturating_add(estimated_bytes(backup));
            if owner_entry_index(backup, schedule_name, schedule_uid).is_some() {
                match migration_refusal(backup, schedule_name) {
                    None => migratable.push(backup.clone()),
                    // A run still going keeps its owner and is WAITED for, not
                    // reported: it is not blocked on anything a human can do.
                    Some(MigrationRefusal::NotTerminal) => owned_nonterminal += 1,
                    Some(refusal) => blocked.push(MigrationBlocked {
                        name: this.clone(),
                        reason: refusal.as_str().to_string(),
                    }),
                }
            }
            if Some(this.as_str()) == pending {
                if !crate::controllers::backup_schedule::participates_in_concurrency(
                    backup,
                    schedule_name,
                    schedule_uid,
                ) {
                    return Err(HistoryError::ForeignBackup(this));
                }
                pending_child = Some(backup.clone());
            }
            if member_of_concurrency(backup, schedule_name, schedule_uid) {
                active.push(active_entry(backup));
            }
        }
        token = listed
            .metadata
            .continue_
            .clone()
            .filter(|cursor| !cursor.is_empty());
        if token.is_none() {
            break;
        }
        if page + 1 == MAX_INVENTORY_PAGES {
            capped = true;
            warn!(
                schedule = %schedule_name,
                pages = MAX_INVENTORY_PAGES,
                page_size = INVENTORY_PAGE_SIZE,
                "the history inventory stopped at its page cap; status.history.runCount is a \
                 floor. Prune terminal runs — docs/kubernetes.md §9."
            );
        }
    }

    // ---- the migration (D1 §6.2) ----------------------------------------
    //
    // AFTER the whole list and not inside it, so a patch that fails cannot
    // truncate the inventory the counts are derived from, and so the cap below
    // is a cap on WRITES rather than on what was observed.
    let total_migratable = i64::try_from(migratable.len()).unwrap_or(i64::MAX);
    let mut still_owned: i64 = 0;
    for backup in migratable.iter().take(MAX_MIGRATIONS_PER_PASS) {
        match migrate_one(api, backup, schedule_name, schedule_uid).await {
            MigrationOutcome::Migrated => {}
            MigrationOutcome::Retry => still_owned += 1,
            MigrationOutcome::Blocked(reason) => blocked.push(MigrationBlocked {
                name: backup.name_any(),
                reason,
            }),
        }
    }
    // Everything PAST the cap is untouched and still owned — and never a
    // negative number: `saturating_sub` on a signed integer happily returns
    // one, which would have made `legacyOwnedRuns` read `-198` on a schedule
    // with two legacy runs and turned `inventory_due`'s "> 0" into "never
    // again".
    still_owned = still_owned.saturating_add(
        total_migratable
            .saturating_sub(i64::try_from(MAX_MIGRATIONS_PER_PASS).unwrap_or(i64::MAX))
            .max(0),
    );
    // THE COUNT IS TAKEN BEFORE THE SAMPLE IS TRUNCATED. `migrationBlocked` is
    // a bounded SAMPLE (D1 §6.2 step 4 names up to ten); `legacyOwnedRuns` is a
    // TOTAL, and reading it off the truncated list would report ten owned runs
    // on a schedule with twenty-five — an undercount of exactly the thing an
    // operator checks before deleting.
    let blocked_count = i64::try_from(blocked.len()).unwrap_or(i64::MAX);
    blocked.truncate(MIGRATION_BLOCKED_SAMPLE);

    let history = ScheduleHistory {
        run_count,
        run_count_capped: capped,
        estimated_bytes: estimated,
        legacy_owned_runs: owned_nonterminal
            .saturating_add(still_owned)
            .saturating_add(blocked_count),
        // BLOCKED RUNS ARE NOT MIGRATABLE, and keeping them out of this count
        // is what stops `inventory_due` from re-listing every 30 s forever on a
        // schedule holding one object the API server will keep refusing.
        legacy_migratable_runs: still_owned,
        migration_blocked: (!blocked.is_empty()).then_some(blocked),
        inventoried_at: now,
    };
    debug!(
        schedule = %schedule_name,
        runs = history.run_count,
        legacy_owned = history.legacy_owned_runs,
        label_selected,
        "history inventory complete"
    );
    Ok(Observation {
        active: bounded(active),
        pending_child,
        history,
        inventoried: true,
    })
}

/// What one migration PATCH did.
enum MigrationOutcome {
    /// The entry is gone.
    Migrated,
    /// A `409`, or a body this pass could not build: re-read next inventory.
    Retry,
    /// A `403` or `422`: a human has to act, and the status names the object.
    Blocked(String),
}

/// One `Backup`, one merge PATCH (D1 §6.2 steps 3 and 4).
async fn migrate_one(
    api: &Api<Backup>,
    backup: &Backup,
    schedule_name: &str,
    schedule_uid: &str,
) -> MigrationOutcome {
    let Some(patch) = migration_patch(backup, schedule_name, schedule_uid) else {
        return MigrationOutcome::Retry;
    };
    match api
        .patch(
            &backup.name_any(),
            &PatchParams::default(),
            &Patch::Merge(patch),
        )
        .await
    {
        Ok(_) => MigrationOutcome::Migrated,
        Err(kube::Error::Api(e)) if e.code == 403 || e.code == 422 => {
            warn!(
                schedule = %schedule_name,
                backup = %backup.name_any(),
                code = e.code,
                reason = %e.reason,
                "the API server refused the history-detach patch; the run keeps its \
                 ownerReference and is named in HistoryRetained=False MigrationBlocked"
            );
            MigrationOutcome::Blocked(e.code.to_string())
        }
        Err(e) => {
            debug!(
                schedule = %schedule_name,
                backup = %backup.name_any(),
                error = %e,
                "the history-detach patch did not land; it is re-read on the next inventory"
            );
            MigrationOutcome::Retry
        }
    }
}

/// The block a schedule has before its first inventory, so that the condition
/// is computable on a status this controller has not written yet.
fn empty_history(now: DateTime<Utc>) -> ScheduleHistory {
    ScheduleHistory {
        run_count: 0,
        run_count_capped: false,
        estimated_bytes: 0,
        legacy_owned_runs: 0,
        legacy_migratable_runs: 0,
        migration_blocked: None,
        inventoried_at: now,
    }
}

/// D1 §4.8: at most ten, sorted, no duplicates.
fn bounded(mut active: Vec<ActiveRun>) -> Vec<ActiveRun> {
    active.sort_by(|a, b| a.name.cmp(&b.name));
    active.dedup_by(|a, b| a.name == b.name);
    active.truncate(MAX_ACTIVE_RUNS);
    active
}

/// A nonterminal run that counts against `concurrencyPolicy`.
fn member_of_concurrency(backup: &Backup, schedule_name: &str, schedule_uid: &str) -> bool {
    crate::controllers::backup_schedule::participates_in_concurrency(
        backup,
        schedule_name,
        schedule_uid,
    ) && !crate::controllers::backup_schedule::backup_is_terminal(backup)
}

/// One active run, as the status records it.
fn active_entry(backup: &Backup) -> ActiveRun {
    let (kind, attempt, _) = crate::identity::declared_trigger(backup);
    ActiveRun {
        name: backup.name_any(),
        kind: format!("{kind:?}"),
        attempt: i32::try_from(attempt).unwrap_or(0),
    }
}

/// D1 §4.8's bound on `status.activeRuns`, re-exported from the scheduler so
/// the two branches cannot disagree about it.
const MAX_ACTIVE_RUNS: usize = crate::controllers::backup_schedule::MAX_ACTIVE_RUNS;
