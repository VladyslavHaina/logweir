//! PLAT-14.3 / decision D3 §4: the pure half of a recurring recovery rehearsal.
//!
//! Everything in this module is a function of its arguments. No clock, no
//! client, no environment — `now` is always a parameter, exactly as
//! [`crate::protection`] and [`crate::cadence`] do it, so the whole of D3 §4.2's
//! selection order and §4.3's scope arithmetic is testable without a
//! `kube::Client`. [`crate::controllers::rehearsal_schedule`] is the thin half:
//! it reads objects, calls these functions, and patches one `/status`.
//!
//! # The three things that live here and nowhere else
//!
//! 1. **[`template_digest`]** — `sha256` over the canonical JSON of
//!    `RehearsalSchedule.spec` MINUS `suspend`. It is the binding between one
//!    schedule object and one signed standing authorization (D3 §4.3), it is
//!    what `Approval.spec.planHash` carries for this kind, and it is recomputed
//!    from the referent's own sealed spec every slot. The spec is sealed except
//!    `suspend` (`crds/rehearsal_schedule.rs::SUSPEND_ONLY_RULE`), so the digest
//!    cannot drift under a running authorization; a NEW schedule needs a NEW
//!    authorization, which is what makes "approve this rehearsal" mean a
//!    specific rehearsal.
//!
//! 2. **[`select_point`]** — D3 §4.2's filter chain, in its stated order, each
//!    filter NAMED so that an empty set produces a recorded skip that says which
//!    rule emptied it rather than a bare "no point". A rehearsal that fabricated
//!    a run when nothing qualified would be the defect PLAT-14.3 exists to
//!    prevent, so the failure mode is a `lastSkipped` row and no `Restore` at
//!    all.
//!
//! 3. **[`render_plan`]** — the slot's restore plan, rendered from the schedule
//!    and the chosen point. It is pure because D3 §4.3(d)'s "checked twice" only
//!    means anything if the controller's half compares the SAME projection the
//!    runner's half does: both go through
//!    [`logweir_core::execution_contract::plan_scope_facts`] and
//!    [`logweir_core::execution_contract::plan_within_scope`], and this module
//!    writes no second predicate. (`rehearsal_scope.rs`'s own header says the
//!    same thing from the other side.)
//!
//! # What is deliberately NOT here
//!
//! Teardown. The controller deletes no topic, ever (D3 §4.4): phase 9 deletes
//! the exact names it created through a prefix-scoped deleter, and
//! [`pending_topics`] only READS what the signed teardown attestation said could
//! not be removed. While that list is non-empty the next slot is skipped with
//! [`SkipReason::LeftoverTopics`] — the run that would otherwise collide is not
//! allowed to adopt or delete topics it did not create.

use chrono::{DateTime, Duration, Utc};
use logweir_core::execution_contract::{PointBinding, MAX_STANDING_AUTHORIZATION_DAYS};
use logweir_core::ids::sha256_prefixed;
use logweir_core::rehearsal_scope::{RehearsalScope, MODE_SCRATCH};
use logweir_core::spec::{
    Anchor, DrillSpec, ObjectivesSpec, RestoreSpecBlock, SampleSpec, SourceSpec, TargetSpec,
};

use crate::crds::rehearsal_schedule::{RehearsalSchedule, RehearsalScheduleSpec};
use crate::crds::restore::Teardown;

// ===========================================================================
// Names, labels and the rendered prefix
// ===========================================================================

/// The label a rehearsal `Restore` carries naming the schedule that created it.
///
/// D3 §15's L6 asserts this label by name, and the controller's own
/// per-schedule discovery uses the deterministic NAME rather than this label
/// (`attempt discovery by GET, never by listing`). The label exists for the
/// operator and for the per-target concurrency question below, which genuinely
/// is a list.
pub const SCHEDULE_LABEL: &str = "logweir.dev/rehearsal-schedule";

/// The label naming the target `KafkaCluster` a rehearsal `Restore` writes to.
///
/// D3 §4.4: at most one active rehearsal per target cluster in the namespace, so
/// a SECOND schedule pointed at the same scratch cluster skips with
/// [`SkipReason::TargetBusy`] instead of racing over topic names that are unique
/// per schedule but not per broker connection.
pub const TARGET_LABEL: &str = "logweir.dev/rehearsal-target";

/// The label carrying the slot a rehearsal `Restore` belongs to.
pub const SLOT_LABEL: &str = "logweir.dev/rehearsal-slot";

/// The annotation recording which basis bounded this run's size — `catalog`
/// when the partition total was known and `unknown` when it was not (D3 §4.2).
///
/// AN ANNOTATION AND NOT A STATUS FIELD, because `RehearsalScheduleStatus` is
/// sealed by W0 and carries no home for it. It travels on the object the
/// statement is about, which is the `Restore`, and it is repeated in the
/// `Ready` condition message so `kubectl describe rehearsalschedule` says it
/// too.
pub const SIZE_BASIS_ANNOTATION: &str = "logweir.dev/rehearsal-size-basis";

/// [`SIZE_BASIS_ANNOTATION`]'s value when the catalog supplied partition counts.
pub const SIZE_BASIS_CATALOG: &str = "catalog";
/// [`SIZE_BASIS_ANNOTATION`]'s value when it did not — D3 §4.2's `sizeBasis:
/// unknown`, where the deadline and `recordsPerPartition` are the only bound.
pub const SIZE_BASIS_UNKNOWN: &str = "unknown";

/// The name prefix every rehearsal-created `Restore` carries.
///
/// Distinct from [`crate::slot::SCHEDULED_BACKUP_PREFIX`] so a name says which
/// controller composed it, and fixed so [`restore_name`] is a pure function of
/// the trigger — guard **G-SLOT**'s rule, applied to the second reconciler that
/// creates objects.
pub const REHEARSAL_RESTORE_PREFIX: &str = "logweir-rehearsal-";

/// How many characters of the schedule UID the rendered topic prefix carries.
pub const UID_PREFIX_LEN: usize = 8;

/// The name of the `Restore` a schedule creates for one slot.
///
/// PURE, AND THE WHOLE POINT IS THE 409. A duplicate reconcile composes the
/// same name and receives `409 AlreadyExists` from the API server rather than
/// starting a second rehearsal — the same property [`crate::slot`] gives the
/// backup scheduler. Attempt discovery is then a `GET` of this name and never a
/// `LIST` with a selector (D3 §4.4, and the reservation is what makes the two
/// agree).
///
/// # Errors
///
/// [`NameError::TooLong`] when the composed name exceeds Kubernetes' 63-character
/// limit for a DNS-1123 label. It is an error and not a truncation: two
/// schedules whose names differ only past the cut would produce ONE object name,
/// and the collision would be silent.
pub fn restore_name(schedule: &str, slot: &str) -> Result<String, NameError> {
    let name = format!("{REHEARSAL_RESTORE_PREFIX}{schedule}-{slot}");
    if name.len() > crate::slot::NAME_LIMIT {
        return Err(NameError::TooLong {
            name,
            limit: crate::slot::NAME_LIMIT,
        });
    }
    Ok(name)
}

/// The longest `metadata.name` a `RehearsalSchedule` may carry and still have
/// every slot's `Restore` name fit.
#[must_use]
pub const fn max_schedule_name_len() -> usize {
    crate::slot::NAME_LIMIT
        - REHEARSAL_RESTORE_PREFIX.len()
        - 1 // the '-' between the schedule name and the slot
        - crate::slot::SLOT_NAME_LEN
}

/// A composed object name that does not fit.
///
/// `Display` by hand rather than through a derive, because this crate does not
/// link `thiserror` and a new dependency for one variant would be a decision
/// (Global Constraint 38).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NameError {
    /// The composed name is longer than Kubernetes accepts.
    TooLong {
        /// What was composed.
        name: String,
        /// The limit it exceeded.
        limit: usize,
    },
}

impl std::fmt::Display for NameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooLong { name, limit } => write!(
                f,
                "the rehearsal Restore name `{name}` is {} characters and Kubernetes accepts \
                 {limit}; rename the RehearsalSchedule to at most {} characters",
                name.len(),
                max_schedule_name_len()
            ),
        }
    }
}

impl std::error::Error for NameError {}

/// D3 §4.4's rendered prefix: `<spec.target.topicPrefix><schedule-uid[..8]>-`.
///
/// UNIQUE PER SCHEDULE OBJECT, which is the property that matters: two
/// schedules can never map a source topic to the same target name, so the
/// runner's own `with_scratch_prefix` deletion guard can be scoped to one run
/// without a second schedule's topics ever falling inside it. A
/// deleted-and-recreated schedule gets a new UID and therefore a new prefix,
/// which is correct — it is a different object and its authorization is a
/// different document.
///
/// `spec.target.topicPrefix` is CEL-shaped `^rehearsal-[a-z0-9-]*-$` AFTER
/// rendering, so the schedule states `rehearsal-` and this appends the
/// discriminator and the trailing `-`.
#[must_use]
pub fn rendered_prefix(topic_prefix: &str, schedule_uid: &str) -> String {
    let discriminator: String = schedule_uid
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(UID_PREFIX_LEN)
        .collect::<String>()
        .to_ascii_lowercase();
    format!("{topic_prefix}{discriminator}-")
}

// ===========================================================================
// The template digest — D3 §4.3(a)
// ===========================================================================

/// The canonical bytes `Approval.spec.planHash` is the digest of, for a
/// `RehearsalSchedule` subject.
///
/// # Why `suspend` is removed rather than pinned
///
/// `suspend` is the ONE mutable field (`SUSPEND_ONLY_RULE`), and it has to be:
/// an operator must be able to stop an unattended rehearsal without minting a
/// new signed document first. Including it in the digest would make pausing a
/// rehearsal invalidate its authorization, so the one safety control an operator
/// reaches for in an incident would be the control that breaks the schedule.
/// Every other field is inside the digest and sealed by CEL, so what an approver
/// signed is what will run.
///
/// # Errors
///
/// [`logweir_core::det_json::DetJsonError`] when the spec cannot be rendered as
/// deterministic JSON. Unreachable for this type — it carries no float — and
/// named rather than unwrapped because a signature boundary is not a place to
/// panic.
pub fn template_bytes(
    spec: &RehearsalScheduleSpec,
) -> Result<Vec<u8>, logweir_core::det_json::DetJsonError> {
    let mut value = serde_json::to_value(spec)?;
    if let Some(map) = value.as_object_mut() {
        map.remove("suspend");
    }
    logweir_core::det_json::to_deterministic_json(&value)
}

/// `sha256:<hex>` over [`template_bytes`] — D3 §4.3's `templateDigest`.
///
/// `sha256_prefixed` AND NOT A BARE HEX STRING, deliberately: this value is
/// compared against `Approval.spec.planHash` and against the `plan_hash` inside
/// the signed approval document, both of which are
/// [`logweir_core::ids::sha256_prefixed`] everywhere else in the product. One
/// spelling means the approval controller's check 7 needs no special case for
/// this kind.
///
/// # Errors
///
/// As [`template_bytes`].
pub fn template_digest(
    spec: &RehearsalScheduleSpec,
) -> Result<String, logweir_core::det_json::DetJsonError> {
    Ok(sha256_prefixed(&template_bytes(spec)?))
}

// ===========================================================================
// Skips — D3 §4.1's `status.lastSkipped.reason` vocabulary
// ===========================================================================

/// Why a due slot produced no rehearsal.
///
/// A CLOSED SET, and every one of them is RECORDED. D3 §4.3's rule is "a skip is
/// recorded, never a silent no-op": an unattended rehearsal that quietly did
/// nothing for eleven weeks is indistinguishable, from the outside, from one
/// that passed eleven times.
///
/// The two authorization spellings are shared verbatim with the runner
/// ([`logweir_core::execution_contract::AUTHORIZATION_INVALID`] and
/// [`logweir_core::execution_contract::AUTHORIZATION_EXPIRED`]) so the
/// controller's `status.lastSkipped.reason` and a runner refusal say the same
/// word about the same fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// D3 §4.2's filter chain emptied the candidate set.
    NoQualifyingPoint,
    /// The target `KafkaCluster` is absent or does not report reachable.
    TargetUnavailable,
    /// The standing authorization is absent, unverified, bound to another
    /// subject, signed by a key that may no longer authorise, or the rendered
    /// plan falls outside its signed scope.
    AuthorizationInvalid,
    /// The standing authorization's `expiresAt` is in the past.
    AuthorizationExpired,
    /// This schedule's own previous rehearsal has not finished.
    ConcurrencyBlocked,
    /// ANOTHER schedule is rehearsing against the same target cluster.
    TargetBusy,
    /// Teardown left topics behind. The next slot does not adopt them.
    LeftoverTopics,
    /// The chosen point is inside a retention lease (D3 §6.6).
    PointRetentionInProgress,
}

impl SkipReason {
    /// The wire spelling, which is the CRD's own enum value.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoQualifyingPoint => "NoQualifyingPoint",
            Self::TargetUnavailable => "TargetUnavailable",
            Self::AuthorizationInvalid => logweir_core::execution_contract::AUTHORIZATION_INVALID,
            Self::AuthorizationExpired => logweir_core::execution_contract::AUTHORIZATION_EXPIRED,
            Self::ConcurrencyBlocked => "ConcurrencyBlocked",
            Self::TargetBusy => "TargetBusy",
            Self::LeftoverTopics => "LeftoverTopics",
            Self::PointRetentionInProgress => "PointRetentionInProgress",
        }
    }
}

impl std::fmt::Display for SkipReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A skip, with the sentence an operator reads beside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skip {
    /// The closed-set reason.
    pub reason: SkipReason,
    /// What happened, in one sentence, naming the rule that refused.
    pub detail: String,
}

impl Skip {
    /// Build one.
    #[must_use]
    pub fn new(reason: SkipReason, detail: impl Into<String>) -> Self {
        Self {
            reason,
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for Skip {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}. {}", self.reason, self.detail)
    }
}

// ===========================================================================
// Point selection — D3 §4.2
// ===========================================================================

/// The covered window a point attests, in epoch milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    /// `windowCovered.fromMs`.
    pub from_ms: i64,
    /// `windowCovered.toMs`. EXCLUSIVE on the archive side.
    pub to_ms: i64,
}

/// One candidate recovery point, projected from a `Backup` or from a catalog
/// view entry.
///
/// # Why `topics` is an `Option` and an absent list is fatal to the candidate
///
/// D3 §4.1 says `spec.point.topics` "must be a subset of the chosen point", and
/// §4.2 makes `topics ⊆ point.topics` the FIRST filter. A `Backup` records the
/// topic set its run resolved; the catalog's view entry does NOT
/// (`catalog_view::ViewEntry` carries identity, window, availability and
/// verification, and no topic list). A point whose topic set nobody recorded is
/// a point in which the subset claim cannot be proven, so it is filtered out
/// rather than assumed — fail closed. See this module's tests and the worker
/// report's gap list: giving the catalog record a topic list is what would make
/// a catalog-only point rehearsable, and it belongs to the record format's
/// owner, not here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PointCandidate {
    /// D3 §5.1's identity, `lwp1-…`.
    pub point_id: String,
    /// The archive's backup set id, which is what the plan's `source.backup`
    /// pins.
    pub backup_id: String,
    /// The `Backup` object, while one exists. `None` for a catalog-only point.
    pub backup_name: Option<String>,
    /// Capture start — the instant the freshness order is taken on.
    pub recovery_point_at: DateTime<Utc>,
    /// The covered window, when the point attests one.
    pub covered: Option<Window>,
    /// The topics the point covers, or `None` when the source recorded none.
    pub topics: Option<Vec<String>>,
    /// The partition total, when the catalog or the run supplied it.
    pub partitions: Option<u32>,
    /// The signed receipt's object key.
    pub receipt_key: String,
    /// `sha256:<hex>` over the stored receipt bytes — the v2 point binding.
    pub receipt_sha256: String,
    /// `sha256:<hex>` over the manifest the receipt attests.
    pub manifest_sha256: Option<String>,
    /// The `BackupDestination` this point lives in, in this namespace.
    pub destination: Option<String>,
    /// The source cluster id the point was captured from, when recorded.
    pub source_cluster_id: Option<String>,
    /// `availability.selectable() && verification.selectable()` — the catalog's
    /// own materialised conjunction (D3 §5.4), or the `Backup` path's
    /// "succeeded, and its evidence verified" for a point with no catalog entry.
    pub selectable: bool,
    /// The `Backup`'s own evidence verdict was REACHED and is not a pass
    /// (`Invalid`, `Untrusted`, or any result other than `Valid`/`NotAttempted`/`Pending`).
    ///
    /// Such a candidate is never selectable, whatever a catalog row says. The
    /// catalog may decide only where the controller could not look
    /// (`NotAttempted`, `Pending`, or no verdict at all) — the same rule as
    /// `protection::evidence_objective_met`. A view is served until
    /// `viewExpiresAt`, so a row harvested before the controller found a
    /// replaced receipt or a revoked signer would otherwise overrule the
    /// controller's refusal. For a catalog-only point it is `true` when a
    /// `Backup` the pass listed refused the same receipt
    /// (`catalog_view::ControllerRefusals`), and `false` otherwise.
    pub verdict_refused: bool,
    /// Whether a retention run currently holds a lease over this point
    /// (D3 §6.6).
    pub retention_lease: bool,
}

/// What [`select_point`] was asked to satisfy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectionRules<'a> {
    /// `spec.point.topics` — the set that must be a subset of the point's.
    pub topics: &'a [String],
    /// `spec.point.minAgeSeconds`.
    pub min_age_seconds: i64,
    /// `spec.bounds.maxPartitions`.
    pub max_partitions: u32,
    /// The cluster id the rehearsal will restore INTO.
    ///
    /// A point captured FROM that cluster is not a rehearsal candidate: the
    /// runner's phase 0 refuses `source == target` outright, so selecting one
    /// would spend a slot on a run that cannot admit. Checked here as well as
    /// there because a refusal that costs no Job is strictly better than one
    /// that costs a pod, and because a recorded skip says WHY while a
    /// `GuardRefused` exit says only that a guard fired.
    pub target_cluster_id: &'a str,
}

/// The point this slot will rehearse, and what bounded it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selected {
    /// The winning candidate.
    pub point: PointCandidate,
    /// `catalog` when the partition total was known, `unknown` otherwise.
    pub size_basis: &'static str,
}

impl Selected {
    /// The closed recovery point: `covered.to_ms - 1 ms`.
    ///
    /// D3 §4.2, and `docs/stability.md`'s "the restore window's end is
    /// inclusive". The archive's `to_ms` is EXCLUSIVE, so a plan that asked for
    /// it would ask for a record the window does not contain.
    ///
    /// # Panics
    ///
    /// Never: [`select_point`] refuses a candidate with no covered window, so a
    /// `Selected` always has one.
    #[must_use]
    pub fn point_in_time(&self) -> DateTime<Utc> {
        let window = self
            .point
            .covered
            .expect("select_point refuses a candidate with no covered window");
        DateTime::from_timestamp_millis(window.to_ms - 1).unwrap_or(self.point.recovery_point_at)
    }

    /// The sample window's start — the covered window's own start.
    ///
    /// # Panics
    ///
    /// Never, for the reason [`Self::point_in_time`] gives.
    #[must_use]
    pub fn window_start(&self) -> DateTime<Utc> {
        let window = self
            .point
            .covered
            .expect("select_point refuses a candidate with no covered window");
        DateTime::from_timestamp_millis(window.from_ms).unwrap_or(self.point.recovery_point_at)
    }
}

/// D3 §4.2's filter chain, in its stated order.
///
/// # The order is the contract, and so is the reason each filter reports
///
/// Each filter that EMPTIES the set names itself, because "no qualifying point"
/// is the least useful thing an unattended rehearsal can say. An operator whose
/// points are all four minutes old needs to read `minAgeSeconds`, not go
/// looking. The filters run in §4.2's order — subset, age, window, size, lease —
/// and the LAST one to empty a non-empty set is the one reported, which is the
/// one that actually refused the survivors.
///
/// Every filter is applied to a set that is already `selectable`: D3 §4.2's
/// candidate definition is "available by §3.2's rule ∪ catalog entries that are
/// `Available` + `Verified`", and [`PointCandidate::selectable`] is that
/// conjunction, materialised by whoever projected the candidate.
///
/// Order among survivors: `recovery_point_at` DESCENDING, tie broken by
/// `point_id` ASCENDING. The tie-break is not decoration — two points captured
/// in the same millisecond would otherwise make the chosen point depend on list
/// order, and a rehearsal that rehearses a different point on each reconcile is
/// a rehearsal whose result means nothing.
///
/// # Errors
///
/// A [`Skip`] whose reason is [`SkipReason::NoQualifyingPoint`], or
/// [`SkipReason::PointRetentionInProgress`] when a retention lease is what
/// removed the last survivor — D3 §16's own distinction, because the remedy
/// differs: one is "take a backup", the other is "wait".
pub fn select_point(
    candidates: &[PointCandidate],
    rules: &SelectionRules<'_>,
    now: DateTime<Utc>,
) -> Result<Selected, Skip> {
    let mut set: Vec<&PointCandidate> = candidates.iter().filter(|c| c.selectable).collect();
    if set.is_empty() {
        return Err(Skip::new(
            SkipReason::NoQualifyingPoint,
            format!(
                "none of the {} candidate points is both available and verified; a rehearsal \
                 never runs against evidence this installation has not verified \
                 (spec.point.requireVerifiedEvidence is true in v1)",
                candidates.len()
            ),
        ));
    }

    // ---- 1. topics ⊆ point.topics ---------------------------------------
    let before = set.len();
    set.retain(|c| match c.topics.as_ref() {
        Some(known) => rules.topics.iter().all(|t| known.contains(t)),
        // AN UNRECORDED TOPIC SET REFUSES A CLAIM, AND AN EMPTY REQUIREMENT
        // MAKES NO CLAIM. D3 §4.1 requires `spec.point.topics` to be a subset of
        // the chosen point's, and a point whose own set nobody recorded cannot
        // be shown to contain them — so it is refused rather than assumed. When
        // the schedule requires NO topics the subset claim is vacuous: there is
        // nothing to prove, and refusing anyway is over-strict rather than
        // fail-closed. It also mattered: it locked every topic-agnostic
        // schedule, and PLAT-15.2's disaster path, out of the catalog entirely.
        None => rules.topics.is_empty(),
    });
    if set.is_empty() {
        return Err(Skip::new(
            SkipReason::NoQualifyingPoint,
            format!(
                "none of the {before} available points covers every topic in spec.point.topics \
                 [{}] — a point whose own topic set was never recorded cannot be proven to \
                 contain them and is refused rather than assumed (a schedule that requires no \
                 topics makes no such claim, and such a point is admitted)",
                rules.topics.join(", ")
            ),
        ));
    }

    // ---- 2. now - recoveryPointAt >= minAgeSeconds ------------------------
    let before = set.len();
    set.retain(|c| (now - c.recovery_point_at).num_seconds() >= rules.min_age_seconds);
    if set.is_empty() {
        return Err(Skip::new(
            SkipReason::NoQualifyingPoint,
            format!(
                "all {before} matching points are younger than spec.point.minAgeSeconds ({}s)",
                rules.min_age_seconds
            ),
        ));
    }

    // ---- 3. a covered window, and it must be non-empty --------------------
    let before = set.len();
    set.retain(|c| c.covered.is_some_and(|w| w.to_ms > w.from_ms));
    if set.is_empty() {
        return Err(Skip::new(
            SkipReason::NoQualifyingPoint,
            format!(
                "none of the {before} matching points attests a covered window with toMs > \
                 fromMs; there is nothing to restore between two equal instants"
            ),
        ));
    }

    // ---- 4. partitions <= maxPartitions, WHEN the count is known ----------
    //
    // An unknown count is NOT a refusal, and that asymmetry is D3 §4.2's own:
    // the catalog does not always carry partition counts, and refusing every
    // point it could not size would make the whole feature unusable on an
    // archive that predates the count. What the decision requires instead is
    // that the run be recorded as bounded by something else — the deadline and
    // `recordsPerPartition` — which is what `sizeBasis: unknown` says.
    let before = set.len();
    set.retain(|c| c.partitions.is_none_or(|p| p <= rules.max_partitions));
    if set.is_empty() {
        return Err(Skip::new(
            SkipReason::NoQualifyingPoint,
            format!(
                "all {before} matching points have more than spec.bounds.maxPartitions ({}) \
                 partitions",
                rules.max_partitions
            ),
        ));
    }

    // ---- 4b. the point's own source is not the rehearsal target -----------
    let before = set.len();
    set.retain(|c| c.source_cluster_id.as_deref() != Some(rules.target_cluster_id));
    if set.is_empty() {
        return Err(Skip::new(
            SkipReason::TargetUnavailable,
            format!(
                "all {before} matching points were captured from cluster id `{}`, which is the \
                 rehearsal target; a rehearsal restores into an ISOLATED cluster and never into \
                 its source",
                rules.target_cluster_id
            ),
        ));
    }

    // ---- 5. not inside a retention lease (D3 §6.6) ------------------------
    let before = set.len();
    set.retain(|c| !c.retention_lease);
    if set.is_empty() {
        return Err(Skip::new(
            SkipReason::PointRetentionInProgress,
            format!(
                "all {before} otherwise-qualifying points are inside a retention lease; a \
                 rehearsal does not race a deletion run over the same objects"
            ),
        ));
    }

    set.sort_by(|a, b| {
        b.recovery_point_at
            .cmp(&a.recovery_point_at)
            .then_with(|| a.point_id.cmp(&b.point_id))
    });
    let point = set[0].clone();
    let size_basis = if point.partitions.is_some() {
        SIZE_BASIS_CATALOG
    } else {
        SIZE_BASIS_UNKNOWN
    };
    Ok(Selected { point, size_basis })
}

// ===========================================================================
// The signed scope — D3 §4.3
// ===========================================================================

/// The [`RehearsalScope`] this schedule's authorization must carry, recomputed
/// from the sealed spec every slot.
///
/// It is built here, by the controller, and then COMPARED against the scope
/// inside the signed document — never substituted for it. The signed scope is
/// what a human authorised; this is what the schedule currently asks for; a
/// difference is a refusal, not a merge.
#[must_use]
pub fn expected_scope(
    spec: &RehearsalScheduleSpec,
    template_digest: &str,
    target_cluster_id: &str,
    rendered_prefix: &str,
) -> RehearsalScope {
    RehearsalScope {
        template_digest: template_digest.to_string(),
        target_cluster_id: target_cluster_id.to_string(),
        topic_prefix: rendered_prefix.to_string(),
        topics: spec.point.topics.clone().unwrap_or_default(),
        max_partitions: u32::try_from(spec.bounds.max_partitions).unwrap_or(u32::MAX),
        records_per_partition: u32::try_from(spec.bounds.records_per_partition).unwrap_or(u32::MAX),
        deadline_seconds: u32::try_from(spec.bounds.deadline_seconds).unwrap_or(u32::MAX),
        modes: vec![MODE_SCRATCH.to_string()],
    }
}

/// Everything about a signed scope the controller checks that
/// [`logweir_core::execution_contract::plan_within_scope`] cannot.
///
/// # What this adds, and why it is not in the shared predicate
///
/// `plan_within_scope` compares a PLAN against a scope, and two of D3 §4.3(d)'s
/// obligations are not plan facts at all:
///
/// * **`templateDigest`** is recomputed from the `RehearsalSchedule`'s own
///   sealed spec, which a credential-less runner cannot read. It is
///   structurally controller-only (W5's report, gap 4).
/// * **`deadlineSeconds`** is the Job's `activeDeadlineSeconds`, not a field of
///   the restore plan. The shared predicate says so in its own doc comment and
///   deliberately does not pretend to check it.
///
/// Both are checked HERE, every slot, before anything is created.
///
/// # Errors
///
/// A one-sentence refusal naming which of the two disagreed, and with what.
pub fn scope_agrees(signed: &RehearsalScope, expected: &RehearsalScope) -> Result<(), String> {
    if signed.template_digest != expected.template_digest {
        return Err(format!(
            "the standing authorization signs templateDigest {} and this RehearsalSchedule's \
             sealed spec hashes to {}; the signed document authorises a different template, so a \
             new authorization is required",
            signed.template_digest, expected.template_digest
        ));
    }
    if signed.deadline_seconds < expected.deadline_seconds {
        return Err(format!(
            "spec.bounds.deadlineSeconds is {}s and the signed scope permits {}s",
            expected.deadline_seconds, signed.deadline_seconds
        ));
    }
    if signed.target_cluster_id != expected.target_cluster_id {
        return Err(format!(
            "the signed scope names target cluster id `{}` and the target KafkaCluster reports \
             `{}`",
            signed.target_cluster_id, expected.target_cluster_id
        ));
    }
    Ok(())
}

/// D3 §4.3's life bound on a standing authorization, re-checked by the
/// controller because the runner's own check reads the NODE clock (W5's report,
/// gap 5) and this one reads the controller's.
///
/// # Errors
///
/// A refusal naming the window that was minted.
pub fn life_within_bound(
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) -> Result<(), String> {
    if expires_at <= issued_at {
        return Err(format!(
            "the standing authorization expires at {expires_at} and was issued at {issued_at}"
        ));
    }
    if expires_at - issued_at > Duration::days(MAX_STANDING_AUTHORIZATION_DAYS) {
        return Err(format!(
            "the standing authorization runs from {issued_at} to {expires_at}, which is longer \
             than the {MAX_STANDING_AUTHORIZATION_DAYS} days D3 §4.3 permits"
        ));
    }
    Ok(())
}

// ===========================================================================
// The rendered plan — D3 §4.2's derived values
// ===========================================================================

/// Everything [`render_plan`] needs that is not on the schedule.
#[derive(Debug, Clone)]
pub struct PlanInputs<'a> {
    /// The schedule's own spec.
    pub spec: &'a RehearsalScheduleSpec,
    /// The chosen point.
    pub selected: &'a Selected,
    /// The rendered per-schedule prefix, from [`rendered_prefix`].
    pub prefix: &'a str,
    /// The archive the point lives in, from the resolved destination.
    pub archive_storage: logweir_core::engine::StorageUrl,
    /// Where this run's evidence is written, from the resolved destination.
    pub evidence_storage: logweir_core::engine::StorageUrl,
    /// The target cluster's bootstrap servers.
    pub bootstrap_servers: Vec<String>,
    /// The schedule's own name, which becomes the plan's `name` so two
    /// schedules pointed at one cluster do not share a notification dedup key.
    pub plan_name: String,
}

/// The slot's restore plan.
///
/// # Every derived value is D3 §4.2's, and each one is load-bearing
///
/// * `source.backup` = the point's `backupId`, PINNED. `latestCompleted` would
///   resolve at execution time against an archive that may have grown since the
///   controller proved the plan is inside the signed scope.
/// * `source.point` = execution contract v2's [`PointBinding`], so the runner
///   re-derives the identity from the bytes it actually read and a point swapped
///   underneath an approved plan is a digest mismatch rather than a quiet
///   substitution.
/// * `restore.point_in_time` = `to_ms - 1 ms`, because the restore window's end
///   is inclusive and the archive's `to_ms` is not.
/// * `sample.window_start` = `from_ms`, `sample.window_end` = `to_ms - 1 ms`,
///   `sample.anchor` = `head` — the only anchor phase 7 implements
///   (`spec::Anchor`'s own doc comment says why `tail` and `random` are refused).
/// * `sample.max_partitions` is ALWAYS set. W5's report makes an absent bound a
///   refusal at the runner, because "unbounded" is not inside any finite ceiling
///   a human signed.
/// * `target.mode` = `scratch`, always. A rehearsal in `newTopic` mode would
///   restore into names an application might be reading, and the prefix-scoped
///   deletion guard that makes teardown safe only applies to scratch names.
#[must_use]
pub fn render_plan(inputs: &PlanInputs<'_>) -> DrillSpec {
    let spec = inputs.spec;
    let point = &inputs.selected.point;
    let point_in_time = inputs.selected.point_in_time();
    DrillSpec {
        name: Some(inputs.plan_name.clone()),
        source: SourceSpec {
            storage: inputs.archive_storage.clone(),
            backup: point.backup_id.clone(),
            topics: spec.point.topics.clone().unwrap_or_default(),
            point: Some(PointBinding {
                point_id: point.point_id.clone(),
                receipt_key: point.receipt_key.clone(),
                receipt_sha256: point.receipt_sha256.clone(),
                manifest_sha256: point.manifest_sha256.clone().unwrap_or_default(),
            }),
        },
        target: TargetSpec {
            bootstrap_servers: inputs.bootstrap_servers.clone(),
            auth: logweir_core::spec::AuthSpec::default(),
            mode: logweir_core::spec::TargetMode::Scratch,
            topic_naming: None,
            marker_topic: spec.target.marker_topic.clone(),
            topic_mapping_prefix: inputs.prefix.to_string(),
            default_replication_factor: i16::try_from(spec.target.replication_factor).unwrap_or(1),
            teardown: "delete".to_string(),
        },
        sample: SampleSpec {
            window_start: inputs.selected.window_start(),
            window_end: point_in_time,
            records_per_partition: usize::try_from(spec.bounds.records_per_partition).unwrap_or(25),
            anchor: Anchor::default(),
            max_partitions: Some(u32::try_from(spec.bounds.max_partitions).unwrap_or(u32::MAX)),
        },
        restore: RestoreSpecBlock {
            point_in_time: Some(point_in_time),
        },
        objectives: ObjectivesSpec {
            rto_seconds: spec
                .objectives
                .as_ref()
                .and_then(|o| o.rto_seconds)
                .and_then(|s| u64::try_from(s).ok()),
            rpo_seconds: None,
            pass_rate: spec.objectives.as_ref().and_then(|o| o.pass_rate),
        },
        evidence: inputs.evidence_storage.clone(),
        engine_overrides: std::collections::BTreeMap::new(),
        notifications: logweir_core::spec::Notifications::default(),
    }
}

/// The plan as the exact bytes `Restore.spec.planBytes` carries and the runner's
/// `--spec` parses.
///
/// YAML, because interface **I20** says this document has ONE grammar and it is
/// the runner's own. Rendered ONCE and hashed as rendered: the bytes on the
/// `Restore` and the bytes the scope check ran against are the same bytes, so
/// the controller cannot prove one document and freeze another.
///
/// # Errors
///
/// [`serde_yaml::Error`] when the plan cannot be serialised. Unreachable for
/// this shape and named rather than unwrapped, for the reason
/// [`template_bytes`] gives.
pub fn plan_bytes(plan: &DrillSpec) -> Result<String, serde_yaml::Error> {
    serde_yaml::to_string(plan)
}

// ===========================================================================
// Cleanup — D3 §4.4
// ===========================================================================

/// The topics a signed teardown attestation says phase 9 could NOT remove.
///
/// Sorted and de-duplicated, so the `status.cleanup.pendingTopics` list a
/// reconcile writes is a function of the attestation and not of its ordering —
/// otherwise two reconciles over one unchanged `Restore` would bump the
/// `resourceVersion` for no reason (`merge_condition`'s own rule, one level up).
///
/// THE CONTROLLER DELETES NOTHING HERE OR ANYWHERE. This reads what phase 9
/// attested and mirrors it; the remedy is documented and it is an operator's.
#[must_use]
pub fn pending_topics(teardown: Option<&Teardown>) -> Vec<String> {
    let mut names: Vec<String> = teardown
        .and_then(|t| t.failed.as_ref())
        .map(|failures| failures.iter().map(|f| f.topic.clone()).collect())
        .unwrap_or_default();
    names.sort();
    names.dedup();
    names
}

/// Whether this schedule may start a slot at all, given what teardown left.
///
/// # Errors
///
/// A [`Skip`] with [`SkipReason::LeftoverTopics`], naming the topics. D3 §4.4:
/// the run that would otherwise collide is not allowed to adopt or delete
/// topics it did not create, and an operator clears them with the documented
/// command.
pub fn cleanup_clear(schedule: &RehearsalSchedule) -> Result<(), Skip> {
    let pending: Vec<String> = schedule
        .status
        .as_ref()
        .and_then(|s| s.cleanup.as_ref())
        .and_then(|c| c.pending_topics.clone())
        .unwrap_or_default();
    if pending.is_empty() {
        return Ok(());
    }
    Err(Skip::new(
        SkipReason::LeftoverTopics,
        format!(
            "teardown could not remove {} topic(s) from a previous rehearsal ({}); this slot does \
             not adopt or delete topics it did not create. Delete them with `kubectl exec` against \
             your Kafka tooling, or `kafka-topics.sh --delete --topic <name>`, then clear \
             status.cleanup.pendingTopics",
            pending.len(),
            pending.join(", ")
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(ms: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(ms).expect("a valid instant")
    }

    fn candidate(id: &str, at_ms: i64) -> PointCandidate {
        PointCandidate {
            point_id: id.to_string(),
            backup_id: format!("b-{id}"),
            backup_name: Some(format!("backup-{id}")),
            recovery_point_at: at(at_ms),
            covered: Some(Window {
                from_ms: at_ms - 60_000,
                to_ms: at_ms,
            }),
            topics: Some(vec!["orders".to_string(), "payments".to_string()]),
            partitions: Some(12),
            receipt_key: format!("logweir/backups/{id}.json"),
            receipt_sha256: format!("sha256:{id}"),
            manifest_sha256: Some("sha256:m".to_string()),
            destination: Some("primary".to_string()),
            source_cluster_id: Some("src".to_string()),
            selectable: true,
            verdict_refused: false,
            retention_lease: false,
        }
    }

    fn rules(topics: &[String]) -> SelectionRules<'_> {
        SelectionRules {
            topics,
            min_age_seconds: 0,
            max_partitions: 200,
            target_cluster_id: "scratch-cluster-id",
        }
    }

    #[test]
    fn the_freshest_qualifying_point_wins_and_ties_break_on_the_id() {
        let topics = vec!["orders".to_string()];
        let older = candidate("lwp1-b", 1_000_000);
        let newer = candidate("lwp1-c", 2_000_000);
        let tie = candidate("lwp1-a", 2_000_000);
        let chosen = select_point(&[older, newer, tie], &rules(&topics), at(9_000_000))
            .expect("one point qualifies");
        assert_eq!(chosen.point.point_id, "lwp1-a", "the id breaks the tie");
        assert_eq!(chosen.size_basis, SIZE_BASIS_CATALOG);
    }

    #[test]
    fn a_point_whose_topics_were_never_recorded_is_refused_and_not_assumed() {
        let topics = vec!["orders".to_string()];
        let mut only = candidate("lwp1-a", 1_000);
        only.topics = None;
        let skip = select_point(&[only], &rules(&topics), at(9_000)).expect_err("no point");
        assert_eq!(skip.reason, SkipReason::NoQualifyingPoint);
        assert!(skip.detail.contains("never recorded"), "{skip}");
    }

    /// The OTHER arm of the same rule, and the one the first revision got
    /// wrong: with no required topics there is no subset claim to prove, so a
    /// point whose topic set nobody recorded — every catalog-only point — is
    /// admitted. Refusing it was over-strict, not fail-closed, and it locked
    /// PLAT-15.2's disaster path out of the catalog entirely.
    #[test]
    fn a_schedule_that_requires_no_topics_may_rehearse_a_point_that_records_none() {
        let none: Vec<String> = Vec::new();
        let mut only = candidate("lwp1-a", 1_000);
        only.topics = None;
        let chosen =
            select_point(&[only], &rules(&none), at(9_000)).expect("no claim, nothing to refuse");
        assert_eq!(chosen.point.point_id, "lwp1-a");

        // …and a point that DOES record its topics is still admitted.
        let known = candidate("lwp1-b", 1_000);
        assert!(select_point(&[known], &rules(&none), at(9_000)).is_ok());
    }

    #[test]
    fn each_filter_names_itself_when_it_empties_the_set() {
        let topics = vec!["orders".to_string()];
        let young = candidate("lwp1-a", 8_000);
        let skip = select_point(
            &[young],
            &SelectionRules {
                topics: &topics,
                min_age_seconds: 3600,
                max_partitions: 200,
                target_cluster_id: "scratch-cluster-id",
            },
            at(9_000),
        )
        .expect_err("too young");
        assert!(skip.detail.contains("minAgeSeconds"), "{skip}");

        let mut empty_window = candidate("lwp1-a", 1_000);
        empty_window.covered = Some(Window {
            from_ms: 1_000,
            to_ms: 1_000,
        });
        let skip = select_point(&[empty_window], &rules(&topics), at(9_000)).expect_err("empty");
        assert!(skip.detail.contains("toMs > fromMs"), "{skip}");

        let mut big = candidate("lwp1-a", 1_000);
        big.partitions = Some(5_000);
        let skip = select_point(&[big], &rules(&topics), at(9_000)).expect_err("too big");
        assert!(skip.detail.contains("maxPartitions"), "{skip}");
    }

    #[test]
    fn a_point_captured_from_the_rehearsal_target_is_never_selected() {
        let topics = vec!["orders".to_string()];
        let mut own = candidate("lwp1-a", 1_000);
        own.source_cluster_id = Some("scratch-cluster-id".to_string());
        let skip = select_point(&[own], &rules(&topics), at(9_000)).expect_err("refused");
        assert_eq!(skip.reason, SkipReason::TargetUnavailable);
        assert!(skip.detail.contains("ISOLATED"), "{skip}");
    }

    #[test]
    fn a_retention_lease_is_its_own_skip_reason_and_not_no_qualifying_point() {
        let topics = vec!["orders".to_string()];
        let mut leased = candidate("lwp1-a", 1_000);
        leased.retention_lease = true;
        let skip = select_point(&[leased], &rules(&topics), at(9_000)).expect_err("leased");
        assert_eq!(skip.reason, SkipReason::PointRetentionInProgress);
    }

    #[test]
    fn an_unknown_partition_count_is_selectable_and_records_an_unknown_size_basis() {
        let topics = vec!["orders".to_string()];
        let mut unsized_point = candidate("lwp1-a", 1_000);
        unsized_point.partitions = None;
        let chosen =
            select_point(&[unsized_point], &rules(&topics), at(9_000)).expect("selectable");
        assert_eq!(chosen.size_basis, SIZE_BASIS_UNKNOWN);
    }

    #[test]
    fn an_unverified_point_is_never_a_candidate() {
        let topics = vec!["orders".to_string()];
        let mut untrusted = candidate("lwp1-a", 1_000);
        untrusted.selectable = false;
        let skip = select_point(&[untrusted], &rules(&topics), at(9_000)).expect_err("refused");
        assert!(skip.detail.contains("available and verified"), "{skip}");
    }

    #[test]
    fn the_rendered_prefix_is_unique_per_schedule_object() {
        let a = rendered_prefix("rehearsal-", "3f2a91c7-1111-2222-3333-444444444444");
        let b = rendered_prefix("rehearsal-", "9e0b1234-1111-2222-3333-444444444444");
        assert_eq!(a, "rehearsal-3f2a91c7-");
        assert_ne!(a, b);
        assert!(a.ends_with('-'), "the guard's own shape");
    }

    #[test]
    fn a_name_that_would_not_fit_is_an_error_and_never_a_truncation() {
        let long = "s".repeat(max_schedule_name_len() + 1);
        assert!(restore_name(&long, "20260101t030000").is_err());
        let ok = "s".repeat(max_schedule_name_len());
        assert!(restore_name(&ok, "20260101t030000").is_ok());
    }

    #[test]
    fn a_standing_authorization_may_not_outlive_ninety_days() {
        let issued = at(0);
        assert!(life_within_bound(issued, issued + Duration::days(90)).is_ok());
        assert!(life_within_bound(issued, issued + Duration::days(91)).is_err());
        assert!(life_within_bound(issued, issued).is_err());
    }

    #[test]
    fn pending_topics_are_sorted_and_deduplicated() {
        let teardown = Teardown {
            attestation_key: None,
            deleted: None,
            failed: Some(vec![
                crate::crds::restore::TeardownFailure {
                    topic: "b".to_string(),
                    error: "x".to_string(),
                },
                crate::crds::restore::TeardownFailure {
                    topic: "a".to_string(),
                    error: "y".to_string(),
                },
                crate::crds::restore::TeardownFailure {
                    topic: "a".to_string(),
                    error: "z".to_string(),
                },
            ]),
        };
        assert_eq!(pending_topics(Some(&teardown)), vec!["a", "b"]);
        assert!(pending_topics(None).is_empty());
    }

    #[test]
    fn the_skip_vocabulary_is_the_runners_own_two_words_for_authorization() {
        assert_eq!(
            SkipReason::AuthorizationInvalid.as_str(),
            logweir_core::execution_contract::AUTHORIZATION_INVALID
        );
        assert_eq!(
            SkipReason::AuthorizationExpired.as_str(),
            logweir_core::execution_contract::AUTHORIZATION_EXPIRED
        );
    }
}
