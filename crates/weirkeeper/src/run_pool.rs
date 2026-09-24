//! The manual-run pool — P10: how many "Back up now" and manual-restore runner
//! Jobs one namespace may have ACTIVE at once, and the order the rest wait in.
//!
//! # The defect this closes
//!
//! D1 §8.3 keeps manual runs outside a schedule's `concurrencyPolicy` on
//! purpose — "Back up now" must work while the nightly run is still going —
//! and nothing else bounded them. On the PoC install one operator's hundred
//! accepted `POST …/backups` (all `201` within 2.4 s) became a hundred
//! simultaneous runner pods: docker-desktop hit its 110-pod limit ("Too many
//! pods"), the node went `NotReady`, and MinIO answered `503 SlowDown`. The
//! evidence-fetch Jobs on the same install WERE bounded (four per namespace,
//! `check::limits`), and the queue they formed drained correctly. This module
//! is the same idea for the runs themselves.
//!
//! # The rule, in one sentence
//!
//! A manual run that is about to create its first object is ADMITTED when the
//! runs of its kind that hold a slot in its namespace — the ones whose own
//! status says so ([`Standing::Occupying`]) PLUS the ones this process
//! admitted and the watch has not shown yet ([`Reservations`]) — plus the
//! OLDER runs still waiting for one, are below the ceiling; otherwise it is
//! QUEUED, and nothing at all is created for it.
//!
//! # Why a reservation, and why a status alone is not enough (review H1, M1)
//!
//! The snapshot is the reflector store the controller already runs, and a
//! store lags the controller's own writes. The first round of this module
//! decided from the store alone, so every pass that decided between one
//! admission and the watch delivering that admission saw the same free slot:
//! three console restores released together from their approval hold, a burst
//! of "Back up now" released from a destination hold, or every run re-enqueued
//! at once by a restart or a `TrustPolicy` event were ALL admitted past the
//! ceiling (probes P1–P3 of the Tier-A review). So admission is now a
//! check-and-reserve under ONE process-wide lock:
//!
//! 1. take the snapshot;
//! 2. forget every reservation the snapshot has caught up with (the run is
//!    now `Occupying` by its own status, or terminal, or gone) and every one
//!    older than [`RESERVATION_TTL_SECONDS`];
//! 3. decide, counting a reserved peer as occupying whatever its phase says;
//! 4. on admit, reserve.
//!
//! Every pass that decides after an admission therefore counts it, whatever
//! the watch has delivered, and concurrent passes are serialised by the lock:
//! a burst of any size admits at most the ceiling.
//!
//! # And why a status record as well: restarts (review H1)
//!
//! A reservation lives in this process. So the pass that admits a run WRITES
//! the admission onto the run before it creates anything — `Admitted=True`
//! ([`admitted_by_status`]) — and a run whose status carries it is
//! `Occupying` from then on, in every process and after every restart. A
//! failed write creates nothing (the create steps are behind it), and the
//! reservation covers the window until the watch shows the write.
//!
//! # FIFO by arrival (review M1)
//!
//! [`QueueKey`] is `metadata.creationTimestamp`, then the UID: the API
//! server's own clock, never a name a client chose (a manual name is random
//! base32, so ordering by it put later arrivals ahead of earlier ones).
//!
//! # What "occupying" is read from
//!
//! A recorded `status.execution` or `jobRef`, an `Admitted=True` condition, a
//! `Running`/`Resolving` phase, and ANY PHASE THIS BUILD DOES NOT KNOW (the
//! safe direction: an over-count costs a requeue, an under-count a pod).
//! `Succeeded`, `Failed` and `Refused` are terminal — the one terminal set the
//! rest of the controller uses. The only non-terminal statuses that are not
//! occupying are "nothing has been created": no phase at all, `Queued`, and
//! `Pending` — a hold on a missing destination or on an approval, which may
//! last hours under a Governed policy and must not block the line. A held run
//! rejoins the line at its own creation time when the hold clears, and from
//! that instant it is reserved.
//!
//! # The residual, stated
//!
//! Two controller PROCESSES do not share reservations. The chart runs one
//! replica with `Recreate` and no leader election (Global Constraint 30), so
//! the only overlap is a rolling replacement, where the old process is killed
//! before the new one starts. A crash after the admission write is covered by
//! the write; a crash BEFORE it created nothing.
//!
//! # What this module is not
//!
//! It reads no clock (`now` is an argument) and holds no client. It does not
//! decide anything about a scheduled, catch-up or retry `Backup`, or about a
//! `RehearsalSchedule`'s `Restore` — those are [`Standing::Outside`] and their
//! own policies bound them — and it does not change what a run executes: a
//! queued run has no plan `ConfigMap`, no Job and no execution claim, and it
//! freezes its inputs on the pass that admits it, exactly as it would have on
//! its first pass.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};

use crate::conditions::{PHASE_FAILED, PHASE_PENDING, PHASE_QUEUED, PHASE_SUCCEEDED};
use crate::crds::backup::{Backup, TriggerKind};
use crate::crds::restore::Restore;
use crate::crds::Condition;

/// The installation-policy field a `Backup` is queued behind.
pub const BACKUP_LIMIT_FIELD: &str = "runs.maxManualBackupsActivePerNamespace";
/// The installation-policy field a `Restore` is queued behind.
pub const RESTORE_LIMIT_FIELD: &str = "runs.maxManualRestoresActivePerNamespace";

/// `phase: Refused` — terminal, as `retention_policy::TERMINAL_RESTORE_PHASES`
/// and `backup_schedule` already treat it (review L5).
pub const PHASE_REFUSED: &str = "Refused";

/// How long a reservation is honoured without the watch catching up with it.
///
/// Two minutes: a watch that has not delivered a status write in that time is
/// a controller whose every other decision is also stale, and the durable
/// `Admitted=True` record takes over as soon as it does. A reservation that
/// outlives its run (a pass that errored after admitting) under-admits for at
/// most this long — the safe direction.
pub const RESERVATION_TTL_SECONDS: i64 = 120;

/// Which pool a run belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PoolKind {
    /// Manual `Backup`s.
    Backup,
    /// Manual `Restore`s.
    Restore,
}

/// Where one run stands, as far as its namespace's pool is concerned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Standing {
    /// Holds a slot: admitted, and its Job exists or is being created.
    Occupying,
    /// Waits for one: nothing has been created for it and nothing but the
    /// pool is holding it.
    Waiting,
    /// Neither: held on something the pool does not own (a missing
    /// destination or approval), or not a pool member at all (a scheduled
    /// run, a rehearsal's restore).
    Outside,
    /// A pool member that has finished (`Succeeded`, `Failed`, `Refused`): it
    /// holds nothing, and a reservation for it is released.
    Finished,
}

/// The order a queue drains in: `metadata.creationTimestamp`, then the UID.
///
/// THE API SERVER'S OWN VALUES, NOT A CLIENT'S. `creationTimestamp` is the
/// one instant on the object no controller wrote, and the UID is the server's
/// too, so two controller passes — or a controller restarted in between —
/// always agree about the order. A name is NOT part of the key: a manual name
/// is random base32, and ordering by it put a later arrival ahead of an
/// earlier one (review M1). A fixture with no timestamp sorts first
/// (`None < Some`); an API server never produces one.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct QueueKey {
    created: Option<DateTime<Utc>>,
    uid: String,
}

impl QueueKey {
    /// The key of any object.
    #[must_use]
    pub fn of<K: kube::Resource>(object: &K) -> Self {
        Self {
            created: object.meta().creation_timestamp.as_ref().map(|t| t.0),
            uid: object.meta().uid.clone().unwrap_or_default(),
        }
    }
}

/// What the gate decided for one candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Create what the run needs, now. The candidate is RESERVED.
    Admit {
        /// Runs of this kind holding a slot in the namespace (by status or by
        /// reservation), not counting the candidate.
        active: u32,
        /// Older runs still waiting.
        ahead: u32,
        /// The ceiling.
        limit: u32,
    },
    /// Create nothing; record `phase: Queued` and look again later.
    Queued {
        /// Runs of this kind holding a slot in the namespace.
        active: u32,
        /// Older runs still waiting.
        ahead: u32,
        /// The ceiling.
        limit: u32,
    },
    /// The snapshot is not trustworthy yet (the watch has not finished its
    /// first list). Create nothing, WRITE nothing, and look again later.
    Unsynced,
}

impl Decision {
    /// Whether this admits the candidate.
    #[must_use]
    pub fn is_admitted(self) -> bool {
        matches!(self, Self::Admit { .. })
    }
}

/// Whether a `Backup` is a pool member at all: a MANUAL run.
///
/// D1 §3.1's own derivation ([`crate::identity::declared_trigger`], rule 4
/// included), so an object created before `spec.trigger` existed is manual
/// exactly when the controller that created it would have said so. The same
/// predicate `backup_schedule` uses to keep manual runs OUT of
/// `concurrencyPolicy` keeps them IN here, so the two pools cannot overlap.
///
/// A SELF-DECLARED TRIGGER, AND THAT IS A KNOWN LIMIT (review L1): a subject
/// who may create `Backup`s directly can declare a scheduled kind for an
/// existing schedule and a valid slot, and such a run is bounded by neither
/// this pool nor `concurrencyPolicy`. RBAC on `create backups` governs direct
/// object creation; D1 §15 records the residual.
#[must_use]
pub fn is_manual_backup(backup: &Backup) -> bool {
    crate::identity::declared_trigger(backup).0 == TriggerKind::Manual
}

/// Whether a `Restore` is a pool member at all: not a rehearsal's run.
///
/// A `RehearsalSchedule` creates its `Restore`s with `spec.authorization` (the
/// standing, once-signed authorization) and bounds them itself — one active
/// rehearsal per schedule. Every other `Restore` — the console's, `kubectl`'s,
/// a runbook's — was asked for by a person and is manual.
#[must_use]
pub fn is_manual_restore(restore: &Restore) -> bool {
    restore.spec.authorization.is_none()
}

/// Whether a phase is terminal — the one set the rest of the controller uses.
#[must_use]
pub fn is_terminal_phase(phase: Option<&str>) -> bool {
    matches!(phase, Some(PHASE_SUCCEEDED | PHASE_FAILED | PHASE_REFUSED))
}

/// Whether the conditions carry the durable admission record: `Admitted=True`.
///
/// Written by the gate BEFORE anything is created for an admitted run (and by
/// every running pass after it), and never by anything that holds or queues
/// one — those write `Admitted=False`. So it is the run's own statement that
/// it holds a slot, readable in every process and after every restart.
#[must_use]
pub fn admitted_by_status(conditions: Option<&Vec<Condition>>) -> bool {
    conditions.is_some_and(|cs| {
        cs.iter()
            .any(|c| c.r#type == crate::conditions::CONDITION_ADMITTED && c.status == "True")
    })
}

/// Where a phase leaves a pool member that is not terminal and has no
/// recorded execution, Job or admission — the one table both kinds share.
fn standing_of_phase(phase: Option<&str>) -> Standing {
    match phase {
        None | Some(PHASE_QUEUED) => Standing::Waiting,
        Some(PHASE_PENDING) => Standing::Outside,
        // `Running`, `Resolving`, and EVERY PHASE THIS BUILD DOES NOT KNOW: the
        // safe direction for a ceiling is to count what it cannot classify.
        Some(_) => Standing::Occupying,
    }
}

/// Where one `Backup` stands in its namespace's manual-backup pool.
#[must_use]
pub fn backup_standing(backup: &Backup) -> Standing {
    if !is_manual_backup(backup) {
        return Standing::Outside;
    }
    let status = backup.status.as_ref();
    let phase = status.and_then(|s| s.phase.as_deref());
    if is_terminal_phase(phase) {
        return Standing::Finished;
    }
    // FROZEN, RUNNING OR ADMITTED. `status.execution` is written BEFORE the
    // Job is created and `Admitted=True` before anything is, both
    // `?`-propagated, so each is a superset of "a Job of this run may exist".
    if status.is_some_and(|s| {
        s.execution.is_some() || s.job_ref.is_some() || admitted_by_status(s.conditions.as_ref())
    }) {
        return Standing::Occupying;
    }
    standing_of_phase(phase)
}

/// Where one `Restore` stands in its namespace's manual-restore pool.
#[must_use]
pub fn restore_standing(restore: &Restore) -> Standing {
    if !is_manual_restore(restore) {
        return Standing::Outside;
    }
    let status = restore.status.as_ref();
    let phase = status.and_then(|s| s.phase.as_deref());
    if is_terminal_phase(phase) {
        return Standing::Finished;
    }
    if status.is_some_and(|s| s.job_ref.is_some() || admitted_by_status(s.conditions.as_ref())) {
        return Standing::Occupying;
    }
    standing_of_phase(phase)
}

/// Whether THIS pass of a `Backup` goes through the gate — **pure**, and the
/// one predicate `controllers::backup` asks.
///
/// Only a MANUAL run with no frozen inputs that does not already hold a slot.
/// A frozen or admitted run was admitted by an earlier pass and is never
/// re-queued (its Job may be gone and must be re-created from the frozen
/// inputs, D1 §3.3); a `Resolving` run's own discovery Job is already running;
/// a scheduled, catch-up or retry run is bounded by its schedule and never
/// waits behind a browser click.
#[must_use]
pub fn backup_gated(backup: &Backup) -> bool {
    backup
        .status
        .as_ref()
        .and_then(|s| s.execution.as_ref())
        .is_none()
        && is_manual_backup(backup)
        && backup_standing(backup) != Standing::Occupying
}

/// Whether THIS pass of an ADMITTED `Restore` goes through the gate — **pure**,
/// and the one predicate `controllers::restore` asks: a manual restore (not a
/// rehearsal's) that does not already hold a slot.
#[must_use]
pub fn restore_gated(restore: &Restore) -> bool {
    is_manual_restore(restore) && restore_standing(restore) != Standing::Occupying
}

/// One admission this process made and the watch may not have shown yet.
#[derive(Clone, Debug)]
struct Reservation {
    kind: PoolKind,
    namespace: String,
    at: DateTime<Utc>,
}

/// The admissions this process has made, keyed by UID — the check-and-reserve
/// half of the gate (review H1, M1).
///
/// ONE LOCK, HELD ONLY OVER PURE WORK: the snapshot, the pruning, the decision
/// and the insert. No `await` happens under it, so it serialises decisions and
/// nothing else.
#[derive(Debug, Default)]
pub struct Reservations {
    held: Mutex<HashMap<String, Reservation>>,
}

impl Reservations {
    /// An empty registry — what a test uses, so rows never share one.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The ONE registry the running controller uses, shared by the `Backup`
    /// and `Restore` reconcilers and by every per-namespace copy of each.
    #[must_use]
    pub fn global() -> &'static Reservations {
        static GLOBAL: std::sync::OnceLock<Reservations> = std::sync::OnceLock::new();
        GLOBAL.get_or_init(Reservations::new)
    }

    /// How many reservations are held — for a row's assertion.
    #[must_use]
    pub fn len(&self) -> usize {
        self.held.lock().map_or(0, |held| held.len())
    }

    /// Whether none is held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Forget one run's reservation — a pass that queued it, or that admitted
    /// it and then created nothing.
    pub fn release(&self, uid: &str) {
        if let Ok(mut held) = self.held.lock() {
            held.remove(uid);
        }
    }

    /// Decide one candidate and, on admit, RESERVE it — atomically with every
    /// other decision this process makes.
    ///
    /// `peers` is the snapshot, or `None` while the watch has not synced (then
    /// nothing is decided and nothing reserved). `standing` is
    /// [`backup_standing`] or [`restore_standing`].
    pub fn decide<K, F>(
        &self,
        kind: PoolKind,
        candidate: &K,
        peers: Option<Vec<Arc<K>>>,
        limit: u32,
        standing: F,
        now: DateTime<Utc>,
    ) -> Decision
    where
        K: kube::Resource,
        F: Fn(&K) -> Standing,
    {
        let Some(peers) = peers else {
            return Decision::Unsynced;
        };
        let Ok(mut held) = self.held.lock() else {
            // A poisoned lock is a panic elsewhere in this process; admitting
            // nothing is the safe answer.
            return Decision::Unsynced;
        };
        let by_uid: HashMap<&str, &K> = peers
            .iter()
            .filter_map(|p| p.meta().uid.as_deref().map(|u| (u, p.as_ref())))
            .collect();
        let namespace = candidate.meta().namespace.clone().unwrap_or_default();
        // FORGET WHAT THE SNAPSHOT HAS CAUGHT UP WITH — in THIS kind and THIS
        // namespace only, because that is all the snapshot is known to hold
        // (a scoped controller runs one store per namespace): the run's own
        // status now counts it (occupying) or frees it (finished), or the run
        // is gone. And, everywhere, whatever is older than the TTL.
        held.retain(|uid, r| {
            if (now - r.at).num_seconds() > RESERVATION_TTL_SECONDS {
                return false;
            }
            if r.kind != kind || r.namespace != namespace {
                return true;
            }
            match by_uid.get(uid.as_str()) {
                None => false,
                Some(object) => {
                    !matches!(standing(object), Standing::Occupying | Standing::Finished)
                }
            }
        });
        let uid = candidate.meta().uid.clone().unwrap_or_default();
        let key = QueueKey::of(candidate);
        let mut active = 0_u32;
        let mut ahead = 0_u32;
        for peer in &peers {
            let meta = peer.meta();
            let peer_uid = meta.uid.as_deref().unwrap_or_default();
            if meta.namespace.as_deref().unwrap_or_default() != namespace || peer_uid == uid {
                continue;
            }
            let reserved = held
                .get(peer_uid)
                .is_some_and(|r| r.kind == kind && r.namespace == namespace);
            match standing(peer) {
                Standing::Occupying => active = active.saturating_add(1),
                Standing::Finished => {}
                // RESERVED BY THIS PROCESS: admitted, whatever its status says
                // yet — waiting (not reflected), or held (released from a hold
                // together with its peers).
                _ if reserved => active = active.saturating_add(1),
                Standing::Waiting if QueueKey::of(peer.as_ref()) < key => {
                    ahead = ahead.saturating_add(1);
                }
                Standing::Waiting | Standing::Outside => {}
            }
        }
        if active.saturating_add(ahead) < limit {
            held.insert(
                uid,
                Reservation {
                    kind,
                    namespace,
                    at: now,
                },
            );
            Decision::Admit {
                active,
                ahead,
                limit,
            }
        } else {
            held.remove(&uid);
            Decision::Queued {
                active,
                ahead,
                limit,
            }
        }
    }

    /// How many runs of `kind` hold a slot or wait ahead of `candidate` —
    /// WITHOUT deciding or reserving anything. For the sentence a refusal
    /// writes about a run that waited ("expired while queued behind N").
    pub fn behind<K, F>(&self, kind: PoolKind, candidate: &K, peers: &[Arc<K>], standing: F) -> u32
    where
        K: kube::Resource,
        F: Fn(&K) -> Standing,
    {
        let Ok(held) = self.held.lock() else {
            return 0;
        };
        let namespace = candidate.meta().namespace.as_deref().unwrap_or_default();
        let uid = candidate.meta().uid.as_deref().unwrap_or_default();
        let key = QueueKey::of(candidate);
        let mut n = 0_u32;
        for peer in peers {
            let meta = peer.meta();
            let peer_uid = meta.uid.as_deref().unwrap_or_default();
            if meta.namespace.as_deref().unwrap_or_default() != namespace || peer_uid == uid {
                continue;
            }
            let reserved = held
                .get(peer_uid)
                .is_some_and(|r| r.kind == kind && r.namespace == namespace);
            let counts = match standing(peer) {
                Standing::Occupying => true,
                Standing::Finished => false,
                _ if reserved => true,
                Standing::Waiting => QueueKey::of(peer.as_ref()) < key,
                Standing::Outside => false,
            };
            if counts {
                n = n.saturating_add(1);
            }
        }
        n
    }
}

/// Where the gate reads its peers, its ceiling and its reservations from.
///
/// A CLOSURE AND NOT A STORE, so a route-table row can hand the gate the exact
/// snapshot it is about and production can hand it the reflector store
/// ([`store_snapshot`]) without either knowing about the other.
pub struct Pool<'a, K> {
    /// Every object of this kind the controller copy watches, or `None` while
    /// its watch has not finished its first list.
    pub peers: &'a (dyn Fn() -> Option<Vec<Arc<K>>> + Send + Sync),
    /// The ceiling, or `None` for the installation policy's own value
    /// ([`BACKUP_LIMIT_FIELD`] / [`RESTORE_LIMIT_FIELD`]).
    pub limit: Option<u32>,
    /// The admissions this process made ([`Reservations::global`] in
    /// production; a row's own registry in a test).
    pub reservations: &'a Reservations,
}

/// A reflector store's contents, or `None` until it has synced.
///
/// `wait_until_ready` resolves on its first poll once the store has seen its
/// first `InitDone`, so `now_or_never` is a non-blocking "is it ready yet".
/// kube-runtime 0.99 swaps the whole buffer into the store at `InitDone`, so a
/// ready store is a complete list, never a prefix of one.
#[must_use]
pub fn store_snapshot<K>(store: &kube::runtime::reflector::Store<K>) -> Option<Vec<Arc<K>>>
where
    K: kube::Resource + Clone + 'static,
    K::DynamicType: Eq + std::hash::Hash + Clone,
{
    use futures::FutureExt as _;
    match store.wait_until_ready().now_or_never() {
        Some(Ok(())) => Some(store.state()),
        _ => None,
    }
}

/// The ONE installation-policy cache the two gates share.
///
/// A process-wide `OnceLock` for the reason `controllers::backup` gives for
/// its own: the reconcilers' `Context` is shared with every route-table double.
pub fn policy_cache() -> &'static crate::check::policy::PolicyCache {
    static CACHE: std::sync::OnceLock<crate::check::policy::PolicyCache> =
        std::sync::OnceLock::new();
    CACHE.get_or_init(crate::check::policy::PolicyCache::new)
}

/// The installation policy's reference, read from the environment WITHOUT the
/// startup log line `topic_discovery::configured_policy_ref` prints — a queued
/// run asks for it on every pass.
#[must_use]
pub fn policy_ref() -> Option<(String, String)> {
    crate::check::policy::configured_ref(
        std::env::var(crate::check::policy::POLICY_CONFIGMAP_ENV)
            .ok()
            .as_deref(),
        std::env::var(crate::check::policy::INSTALLATION_NAMESPACE_ENV)
            .ok()
            .as_deref(),
    )
}

/// Whether a status says the run was QUEUED by an earlier pass.
#[must_use]
pub fn was_queued(phase: Option<&str>, has_queue: bool, admitted_reason: Option<&str>) -> bool {
    phase == Some(PHASE_QUEUED)
        || has_queue
        || admitted_reason == Some(crate::conditions::REASON_CONCURRENCY_LIMITED)
}

/// The `Admitted=False` message a queued run carries.
///
/// STABLE FOR A GIVEN CEILING, AND THAT IS DELIBERATE. It names the ceiling
/// and the policy field, and neither the count nor the position: both move
/// every time a run finishes, and a message that moved with them would be one
/// status write per queued run per finished run.
#[must_use]
pub fn queued_message(kind: &str, namespace: &str, limit: u32, field: &str) -> String {
    format!(
        "this manual {kind} is queued: namespace {namespace} already has {limit} manual \
         {kind}(s) active or ahead of it in line, the ceiling `{field}` in the installation \
         policy sets. Nothing has been created for it yet — no plan, no Job, no execution claim \
         — and it starts in creation order when a slot frees"
    )
}

/// The `Admitted=True` message the admission record carries.
#[must_use]
pub fn admitted_message(kind: &str, namespace: &str, limit: u32, field: &str) -> String {
    format!(
        "admitted to namespace {namespace}'s manual {kind} pool (`{field}` = {limit}); this run \
         holds a slot from now until it finishes"
    )
}
