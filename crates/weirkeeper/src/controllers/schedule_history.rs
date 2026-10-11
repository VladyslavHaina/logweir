//! Retained history: a schedule's runs are not the schedule's children (D1 §6).
//!
//! Scheduled `Backup`s once carried a controller ownerReference to their
//! schedule, so deleting the schedule handed every run, plan ConfigMap and Job
//! to the garbage collector and lost the record of which run wrote which
//! recovery point. This module does three jobs:
//!
//! 1. **Migration** (§6.2). Every `Backup` still carrying the legacy owner entry
//!    is detached by one merge PATCH per object, preconditioned on its own
//!    `resourceVersion`, rewriting `ownerReferences` to everything except the
//!    matching entry and adding the `logweir.dev/schedule-uid` label and the
//!    `logweir.dev/history-retained-from-owner` annotation that
//!    [`crate::identity::is_run_of_schedule`] reads as membership rule 3.
//!    Progress is derived from observation, never a cursor, so a crash between
//!    two patches leaves every object fully migrated or untouched.
//! 2. **Inventory** (§6.7). One paginated, bounded list per schedule, at most
//!    hourly in steady state, that counts the history, estimates its cost,
//!    repairs `status.activeRuns` and finds the runs migration has left.
//! 3. **`HistoryRetained`** (§6.1/§6.3). Whether deleting this schedule would
//!    now be safe, computed on every reconcile from [`ScheduleHistory`], never
//!    carried forward.
//!
//! # Nothing here deletes anything
//!
//! No Logweir component deletes a `Backup`, a ConfigMap or an archive object
//! (§6.5): `charts/logweir/templates/clusterrole.yaml` grants `create` and
//! `patch` on `backups` and no `delete` on them or on ConfigMaps. The one write
//! here is a metadata merge PATCH, which cannot reach a sealed `spec` (CEL
//! refuses it) or the `status` subresource.
//!
//! # Only terminal runs are migrated
//!
//! §6.2 step 2. A legacy `Backup` that has not frozen its plan derives its
//! scheduled identity from the owner UID ([`crate::identity::run_identity`]'s
//! rule 3), so removing the entry mid-flight would leave it with no identity
//! and the `Backup` reconciler would refuse it `ScheduledIdentityMismatch`. A
//! nonterminal legacy run keeps its owner until it is terminal, and the
//! condition says `ActiveLegacyRunsOwned` while that is all that is left.

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

/// The `limit` every inventory page carries (D1 §6.7): an unbounded list of a
/// schedule with a year of 15-minute history would be a multi-megabyte read
/// every reconcile.
pub const INVENTORY_PAGE_SIZE: u32 = 500;

/// How many pages one inventory follows before it stops and says so: ten
/// thousand objects is a bound, not a target. D1 §6.6 calls 2 000 runs per
/// schedule large; past 10 000 the honest answer is a floor plus
/// `runCountCapped`, not a reconcile that reads for a minute and times out.
pub const MAX_INVENTORY_PAGES: usize = 20;

/// How many migration PATCHes one pass sends: one page's worth, after which the
/// walk stops.
///
/// An upgrade over 35 040 legacy runs (D1 §6.6's worst row) would otherwise send
/// them all inside one reconcile. [`inventory_due`] stays `true` while
/// `legacyMigratableRuns` is positive, so the next pass (30 s later) continues
/// from where observation says it got to. Patching page by page and stopping
/// when the budget is spent avoids a full namespace-wide walk per pass, but the
/// walk restarts at page 0 and a page of already-detached runs does not stop
/// it, so pass `k` reads `k + 1` pages: `P(P+1)/2 + P − 1` LISTs for `P` pages
/// (`a_multi_page_migration_costs_a_quadratic_number_of_lists`). Bounded by
/// [`MAX_INVENTORY_PAGES`] per pass; carrying the `continue` token across
/// passes would make it linear.
pub const MAX_MIGRATIONS_PER_PASS: usize = INVENTORY_PAGE_SIZE as usize;

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

/// Whether this reconcile must inventory before it admits anything (D1 §4.5
/// step 1):
///
/// 1. **No `history` block**: nothing is known about legacy owner entries (the
///    upgrade case).
/// 2. **No `activeRuns` block**: "never computed", not "empty" (the final
///    status always writes it, `[]` included).
/// 3. **`legacyMigratableRuns > 0`**: terminal runs still carry the owner
///    entry and a pass can patch them (a capped pass, or a lost `409`). Runs
///    that are only nonterminal do not count: they wait on a Job, not on this
///    controller.
/// 4. **The last inventory is older than [`INVENTORY_INTERVAL_SECS`].**
///
/// D1's "or this process has not inventoried this schedule since start" is not
/// implemented: a fresh process needs a repaired `activeRuns`, which rule 2
/// gives, and a surviving `activeRuns` is complete by construction, because
/// every schedule-created run is recorded by a resourceVersion-conditional
/// status write before it is created.
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
/// A legacy object carries no UID label, so until `HistoryRetained=True` the
/// list must be namespace-wide; after it, the label selector makes the read
/// O(this schedule). `True` alone is not enough, because the selector latches:
/// it excludes the objects the migration looks for, so the next walk finds
/// none and stays `True`. The gate is also
/// [`ScheduleHistory::ownership_scan_complete`], which a capped namespace-wide
/// walk sets to `false`; both halves are required.
#[must_use]
pub fn may_use_label_selector(stored: Option<&BackupScheduleStatus>) -> bool {
    let retained = current_condition(
        stored.and_then(|s| s.conditions.as_ref()),
        CONDITION_HISTORY_RETAINED,
    )
    .is_some_and(|c| c.status == "True");
    let complete = stored
        .and_then(|s| s.history.as_ref())
        .is_some_and(|h| h.ownership_scan_complete);
    retained && complete
}

/// D1 §6.6's per-run etcd estimate: the serialized object, the plan
/// ConfigMap's fixed part, and the topic names twice (`backup.yaml` and
/// `execution-inputs.json`). It counts `spec.topics`, so a dynamically selected
/// run (empty `spec.topics`) is estimated as a floor.
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
/// `apiVersion`, `kind`, `name` and `uid` must all match: a UID match alone
/// could strip an ownerReference another operator wrote, and this is the same
/// predicate [`crate::identity::is_run_of_schedule`] applies as membership rule
/// 2, so anything looser would detach a non-member. The one implementation in
/// this crate.
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

/// The `status.history.migrationBlocked[].reason` a `403` is recorded under.
pub const BLOCKED_API_FORBIDDEN: &str = "ApiForbidden";

/// The `status.history.migrationBlocked[].reason` a `422` is recorded under.
pub const BLOCKED_API_INVALID: &str = "ApiInvalid";

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
            Self::NoScheduleReference => BLOCKED_NO_SCHEDULE_REFERENCE,
        }
    }
}

/// The `status.history.migrationBlocked[].reason` for the one refusal that is
/// this controller's own and that never clears.
///
/// The vocabulary is these three and nothing else: every entry names a
/// `Backup`, and an unfinished walk is not about one object. The capped state
/// is reported in the condition and in
/// [`crate::crds::backup_schedule::ScheduleHistory::ownership_scan_complete`].
pub const BLOCKED_NO_SCHEDULE_REFERENCE: &str = "NoScheduleReference";

/// Whether a recorded block reason is one a later pass could clear by itself.
///
/// `ApiForbidden` and `ApiInvalid` are the API server saying "not now": fix the
/// RoleBinding or the object, and the next hourly inventory lands the patch.
/// `NoScheduleReference` is "not ever": `Backup.spec` is `self == oldSelf`, so
/// the `scheduleRef` that would make the detach safe cannot be added. D1 §6.9
/// tells an operator to wait for `HistoryRetained=True` before deleting a
/// schedule without `--cascade=orphan`; with one such run that wait never ends,
/// so the condition says so and names the two things that do work.
#[must_use]
pub fn blocked_reason_clears_itself(reason: &str) -> bool {
    match reason {
        BLOCKED_API_FORBIDDEN | BLOCKED_API_INVALID => true,
        BLOCKED_NO_SCHEDULE_REFERENCE => false,
        // An unknown reason is terminal, the safe direction: the message then
        // says `--cascade=orphan`, which is correct whatever the reason.
        _ => false,
    }
}

/// Whether this legacy-owned `Backup` may be detached now, or why not.
///
/// The second refusal is a hazard D1 §6.2 does not name: membership rule 3 is
/// "the migration annotation carries the UID and `spec.scheduleRef.name`
/// matches", so a legacy object whose `scheduleRef` names something else (or
/// nothing) is a member only through its ownerReference. Removing it would
/// make the run a member of nothing, so it keeps its owner and is recorded as
/// blocked, visibly.
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
/// Pure, so the body a test asserts over is the one the API server receives:
/// the `resourceVersion` precondition is present, the surviving
/// `ownerReferences` are byte-identical to the observed ones, and no key outside
/// `metadata.ownerReferences`, `metadata.labels` and `metadata.annotations`
/// appears.
///
/// * `ownerReferences` is an array, so RFC 7386 replaces it wholesale; it is
///   rewritten from the observed object under the `resourceVersion`
///   precondition, so a racing edit makes this patch lose with a `409`.
/// * The empty case is `null` (D1 §6.2 step 3), not `[]`.
/// * `labels` and `annotations` are objects, so the two keys are merged in and
///   every other label and annotation survives
///   (`foreign_owner_references_labels_and_annotations_are_preserved_byte_for_byte`).
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
/// Computed, never copied: a copied condition outlives the facts it came from,
/// so a schedule whose last legacy run finished an hour ago would still read
/// `ActiveLegacyRunsOwned`. This is a pure map from the recorded block, applied
/// on every reconcile.
///
/// A blocked migration is reported first, because only it needs a human (`403`
/// and `422` do not clear themselves). Then the two counts, most actionable
/// first: terminal runs the controller can still detach, then runs it must
/// wait for. `HistoryLarge` is a warning on a `True` condition: the history is
/// retained, and retaining it is not free (§6.6).
#[must_use]
pub fn history_condition(
    history: &ScheduleHistory,
    existing: Option<&Condition>,
    observed_generation: Option<i64>,
    now: DateTime<Utc>,
) -> Condition {
    let blocked = history.migration_blocked.as_deref().unwrap_or_default();

    // An unfinished scan is a suffix on whichever arm explains it (migration in
    // progress, runs still active, refusals) and gets its own arm only when
    // nothing else does. It can never produce `True`: arms 1-3 are `False`, arm 4
    // catches the rest, the `True` arms come after it
    // (`an_incomplete_scan_is_never_reported_as_true`).
    let incomplete = if history.ownership_scan_complete {
        String::new()
    } else {
        format!(
            " The inventory also did not finish — it stopped at its page bound \
             ({MAX_INVENTORY_PAGES} × {INVENTORY_PAGE_SIZE}) or with this pass's detach \
             budget spent — so `status.history.ownershipScanComplete` is false and every \
             count here is a floor."
        )
    };

    let (status, reason, message) = if !blocked.is_empty() {
        let names = blocked
            .iter()
            .take(MIGRATION_BLOCKED_SAMPLE)
            .map(|b| format!("{} ({})", b.name, b.reason))
            .collect::<Vec<_>>()
            .join(", ");
        // A `403` or `422` clears once the RoleBinding or object is fixed;
        // `NoScheduleReference` never does (`Backup.spec` is CEL-sealed), so the
        // two get different sentences and the terminal one names the two things
        // that work. Bounded like the sample it is drawn from: a condition
        // message reaches every watcher in the cluster.
        let terminal: Vec<&str> = blocked
            .iter()
            .filter(|b| !blocked_reason_clears_itself(&b.reason))
            .map(|b| b.name.as_str())
            .take(MIGRATION_BLOCKED_SAMPLE)
            .collect();
        let remedy = if terminal.is_empty() {
            "Both refusals clear themselves: fix the RoleBinding or the object and the next \
             inventory retries. Until then, delete only with `--cascade=orphan`."
                .to_string()
        } else {
            format!(
                "{} of them can NEVER be detached, because `Backup.spec` is immutable and the \
                 `spec.scheduleRef` that would keep the run a member of this schedule cannot \
                 be added to an existing object: {}. This condition will not reach `True` \
                 while they exist. The only two remedies are `kubectl delete backupschedule \
                 <name> --cascade=orphan`, which is always safe, or deleting those runs \
                 yourself once their archives are recorded (`docs/kubernetes.md` §9).",
                terminal.len(),
                terminal.join(", ")
            )
        };
        (
            "False",
            REASON_MIGRATION_BLOCKED,
            format!(
                "{} run(s) of this schedule could not be detached from it: {names}. \
                 {remedy}{incomplete}",
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
                 until this is `True`.{incomplete}",
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
                 it.{incomplete}",
                history.legacy_owned_runs
            ),
        )
    } else if !history.ownership_scan_complete {
        // Nothing above explains it, so the unfinished scan is the finding: more
        // objects than the walk's bound, no migration under way, nothing
        // refused or active, and still no proof that no run is owned.
        (
            "False",
            REASON_MIGRATION_BLOCKED,
            format!(
                "The history inventory did not finish: it stopped after {} object(s) at its \
                 page bound ({MAX_INVENTORY_PAGES} × {INVENTORY_PAGE_SIZE}), so it cannot show \
                 that no run is still owned by this schedule. \
                 `status.history.ownershipScanComplete` is false and every count in \
                 `status.history` is a floor. Prune terminal runs until the namespace fits \
                 inside the bound (`docs/kubernetes.md` §9); until then, delete only with \
                 `--cascade=orphan`.",
                history.run_count
            ),
        )
    } else if history.run_count > HISTORY_LARGE_RUNS
        || history.estimated_bytes > HISTORY_LARGE_BYTES
        // A capped walk is large by construction: reaching here means it was
        // label-selected, so every object past the bound carries this
        // schedule's UID label and the count is a floor.
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
/// **Steady state** GETs the ≤ 10 names `status.activeRuns` records plus the
/// reservation: O(active), independent of how much history is retained.
/// **Inventory** takes one paginated list (the only LIST this controller makes,
/// with a `limit`, at most [`MAX_INVENTORY_PAGES`] pages), migrates what it can,
/// and rebuilds the active set and the history block.
///
/// Correctness does not depend on list freshness: every schedule-created run is
/// recorded in `pendingRun`/`activeRuns` by a resourceVersion-conditional
/// status write before it is created, so admission always sees the latest
/// admitted run. The inventory repairs a lost status.
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
    // The steady branch takes the block as a value, so it has no "no history"
    // case: [`inventory_due`] returns `true` for an absent block, so the first
    // pass observes rather than assumes, and nothing invents an empty block
    // whose `ownershipScanComplete` would claim a scan nothing ran.
    match stored.and_then(|status| status.history.clone()) {
        Some(history) if !inventory_due(stored, now) => {
            steady_state(api, stored, history, schedule_name, schedule_uid, pending).await
        }
        _ => inventory(api, stored, schedule_name, schedule_uid, pending, now).await,
    }
}

/// The O(active) branch: GET what the status already names.
///
/// No clock reaches this function: a steady pass observes nothing new. The
/// stored `inventoriedAt` is carried through untouched, because refreshing it
/// would re-arm the hourly timer forever.
#[allow(clippy::too_many_arguments)]
async fn steady_state(
    api: &Api<Backup>,
    stored: Option<&BackupScheduleStatus>,
    history: ScheduleHistory,
    schedule_name: &str,
    schedule_uid: &str,
    pending: Option<&str>,
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

    // The history block is carried, not recomputed: a steady pass has no new
    // facts, and writing back the last inventory keeps the schedule unpatched.
    // The condition is then recomputed from these facts.
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
    let mut still_owned: i64 = 0;
    let mut patched: usize = 0;
    let mut blocked: Vec<MigrationBlocked> = Vec::new();
    let mut capped = false;
    let mut budget_spent = false;
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

        // One page's migration candidates, patched before the next page is
        // fetched, and the walk stops once the budget is spent (see
        // [`MAX_MIGRATIONS_PER_PASS`] for the resulting cost).
        let mut page_candidates: Vec<&Backup> = Vec::new();
        for backup in &listed.items {
            let this = backup.name_any();
            // Membership is asked of every object, selector or not: the
            // selector is a hint, `is_run_of_schedule` the authority.
            if !is_run_of_schedule(backup, schedule_name, schedule_uid) {
                continue;
            }
            run_count += 1;
            estimated = estimated.saturating_add(estimated_bytes(backup));
            if owner_entry_index(backup, schedule_name, schedule_uid).is_some() {
                match migration_refusal(backup, schedule_name) {
                    None => page_candidates.push(backup),
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

        for backup in page_candidates {
            if patched >= MAX_MIGRATIONS_PER_PASS {
                // Not attempted this pass, so still owned and still migratable
                // — the next reconcile re-arms on `legacyMigratableRuns`.
                still_owned += 1;
                budget_spent = true;
                continue;
            }
            patched += 1;
            match migrate_one(api, backup, schedule_name, schedule_uid).await {
                MigrationOutcome::Migrated => {}
                MigrationOutcome::Retry => still_owned += 1,
                MigrationOutcome::Blocked(reason) => blocked.push(MigrationBlocked {
                    name: backup.name_any(),
                    reason,
                }),
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
        if budget_spent {
            // The counts become floors once the walk stops early, and
            // `ownershipScanComplete` becomes false with them.
            capped = true;
            debug!(
                schedule = %schedule_name,
                patched,
                "the detach budget for this pass is spent; the walk stops here and the next \
                 reconcile continues"
            );
            break;
        }
        if page + 1 == MAX_INVENTORY_PAGES {
            capped = true;
            warn!(
                schedule = %schedule_name,
                pages = MAX_INVENTORY_PAGES,
                page_size = INVENTORY_PAGE_SIZE,
                "the history inventory stopped at its page cap; every status.history count is \
                 a floor and HistoryRetained cannot reach True. Prune terminal runs — \
                 docs/kubernetes.md §9."
            );
        }
    }

    // The count is taken before the sample is truncated: `migrationBlocked` is
    // a bounded sample (up to ten), `legacyOwnedRuns` a total an operator
    // checks before deleting.
    let blocked_count = i64::try_from(blocked.len()).unwrap_or(i64::MAX);
    // Sorted before truncation, so two passes report the same ten.
    blocked.sort_by(|a, b| a.name.cmp(&b.name));
    blocked.truncate(MIGRATION_BLOCKED_SAMPLE);

    let history = ScheduleHistory {
        run_count,
        run_count_capped: capped,
        estimated_bytes: estimated,
        legacy_owned_runs: owned_nonterminal
            .saturating_add(still_owned)
            .saturating_add(blocked_count),
        // Blocked runs are not migratable, or `inventory_due` would re-list
        // every 30 s forever over one object the API server keeps refusing.
        legacy_migratable_runs: still_owned,
        migration_blocked: (!blocked.is_empty()).then_some(blocked),
        // A label-selected walk is complete by construction: it runs only after
        // a complete namespace-wide walk proved nothing was owned. A
        // namespace-wide walk is complete only if it ran out of pages first.
        ownership_scan_complete: label_selected || !capped,
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
            // One vocabulary: CamelCase reasons, not the bare status digits.
            MigrationOutcome::Blocked(
                if e.code == 403 {
                    BLOCKED_API_FORBIDDEN
                } else {
                    BLOCKED_API_INVALID
                }
                .to_string(),
            )
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
