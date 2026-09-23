//! What a `RetentionPolicy` would remove from ONE destination — D3 §6.4.
//!
//! # Pure, and that is the whole point
//!
//! Nothing in this module reads a clock, dials a bucket or holds a
//! `kube::Client`. `now` is an argument, the catalog points are an argument and
//! the protection set is an argument, which is what lets every rule in D3 §6.4
//! and §6.5 be a table test rather than a route table — and what lets the plan
//! document be a pure function of its inputs, so an administrator who approves
//! `planSha256` is approving bytes anyone can recompute.
//!
//! # RET-WRONGBUCKET closes here, by construction
//!
//! The defect (`backup_schedule.rs:1417`) is that the legacy report lists
//! manifests through the controller's ONE global store while rendering removal
//! commands for the schedule's own URL, so a schedule writing to another bucket
//! is reported against `LOGWEIR_ARCHIVE_URL`'s catalog. Since D1 W2 made
//! `destinationRef` editable the same mismatch is reachable by an edit between
//! runs.
//!
//! This module cannot make that mistake, and not because it is careful:
//! [`Located`] is a private newtype whose ONLY constructor is
//! [`Located::at`], which refuses a point whose `locations[]` does not contain
//! the destination being evaluated. `kept`, `candidates` and `protected` are
//! built from `Located` values alone, so a point frozen against destination A
//! is not merely *not counted* against destination B — there is no value of the
//! right type to count. `a_point_at_another_destination_is_never_a_candidate`
//! is the row; deleting the membership check makes it fail.
//!
//! The evaluation input is the destination's **catalog view** (D3 §5.3,
//! `crate::catalog_view`), never a second bucket walk with a global handle, and
//! never the schedule's current URL.
//!
//! # `Unknown` is retained
//!
//! Step 1 of D3 §6.4 is the safety rule the rest of the design rests on: a
//! point is a deletion candidate only if the catalog said `Available` AND
//! (`Verified` | `VerifiedHistorical`). Everything else — `Unreadable`,
//! `Partial`, `Conflict`, `UntrustedSigner`, `NotAttempted` — is `skipped`, and
//! a skipped point is never a candidate. A retention pass that cannot read the
//! archive therefore proposes nothing, which is the opposite of what a
//! timestamp-driven bucket lifecycle rule does.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::catalog_view::{Availability, Verification};

// ---------------------------------------------------------------------------
// The media types and the one root nothing may delete under
// ---------------------------------------------------------------------------

/// The plan document's media type — D3 §6.4 step 6.
pub const PLAN_MEDIA_TYPE: &str = "application/vnd.logweir.retention-plan+json;version=1.0.0";

/// The plan document's `format_version`.
pub const PLAN_FORMAT_VERSION: &str = "1.0.0";

/// The evidence root a retention run may never delete under — Global
/// Constraint 6, and the K4 CEL rule's other half.
///
/// Receipts, sidecars, scorecards, catalog records, tombstones and the
/// retention records themselves live here. A deleted point's audit trail
/// survives the point precisely because this prefix is excluded from every
/// plan, from the credential's documented IAM scope, and from the worker's own
/// pre-delete validation.
pub const EVIDENCE_ROOT: &str = "logweir/";

// ---------------------------------------------------------------------------
// The inputs
// ---------------------------------------------------------------------------

/// The destination being evaluated, as the two facts retention needs.
///
/// `location_id` is the catalog view's own spelling of a location — the
/// destination's canonical `s3://bucket/prefix` URL (`ResolvedDestination::
/// canonical_url`). `scope_prefix` is `RetentionPolicy.spec.scope.prefix`, the
/// immutable prefix outside which no key may ever be named.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Destination {
    /// The catalog's location id for this destination.
    pub location_id: String,
    /// `spec.scope.prefix`.
    pub scope_prefix: String,
}

/// One catalog point, as retention sees it.
///
/// Built from a `catalog_view::ViewEntry` plus the point's own object keys.
/// Every field is a FACT the catalog established; nothing here is derived from
/// a bucket walk performed by the control plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PointFacts {
    /// `lwp1-<32 hex>` — content-derived from the receipt bytes.
    pub point_id: String,
    /// The archive set id. A key may only be named under
    /// `<scope_prefix>/<backup_id>/`.
    pub backup_id: String,
    /// `capture.started_at`, in epoch milliseconds. The ordering axis.
    pub recovery_point_at_ms: i64,
    /// Every location holding this point, as location ids. **The membership
    /// test that makes a wrong-destination evaluation unconstructible.**
    pub locations: Vec<String>,
    /// The catalog's availability verdict.
    pub availability: Availability,
    /// The catalog's verification verdict.
    pub verification: Verification,
    /// The manifest key, when the catalog read one. A point with no manifest
    /// key can be no candidate: there is nothing to delete first.
    pub manifest_key: Option<String>,
    /// Every segment key this point's manifest names, in the order the
    /// manifest names them.
    pub segment_keys: Vec<String>,
    /// How many bytes the point occupies, when the catalog knew.
    pub bytes: Option<i64>,
    /// This point's own `Backup` carries a verification verdict the controller
    /// REACHED and that is not a pass (`Invalid`, `Untrusted`, or a result this
    /// build does not know) — `catalog_view::ControllerRefusals`.
    ///
    /// **The catalog decides only where the controller could not look.** A view
    /// row is served until `viewExpiresAt`, so a row harvested before the
    /// receipt was replaced or its signer revoked still reads
    /// `Available`/`Verified`; counted as usable, it would take a `keepLast` or
    /// `minUsablePoints` rank and push an older GOOD point out of the keep set
    /// and into the plan. Such a point is skipped `Unreadable` (see
    /// [`skip_reason`]). Always `false` for a point no `Backup` in the namespace
    /// names (a catalog-only point), whose behaviour is unchanged.
    pub refused_by_controller: bool,
}

/// What to keep — `RetentionPolicy.spec.rules`, in the pure layer's shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rules {
    /// Keep the most recent N usable points.
    pub keep_last: Option<i64>,
    /// Keep usable points newer than N days.
    pub keep_days: Option<i64>,
    /// The floor that survives BOTH rules above.
    pub min_usable_points: i64,
}

/// One `spec.holds[]` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hold {
    /// The point it holds.
    pub point_id: String,
    /// Why — a case reference, an auditor, a regulation.
    pub reason: String,
    /// When it lapses. `None` means it does not.
    pub until: Option<DateTime<Utc>>,
}

impl Hold {
    /// Whether this hold is still in force at `now`.
    #[must_use]
    pub fn in_force(&self, now: DateTime<Utc>) -> bool {
        self.until.is_none_or(|until| now < until)
    }
}

/// The protection set the CONTROLLER supplies — the facts that live in the
/// cluster rather than in the catalog.
///
/// Separate from [`Rules`] because these are observations, not policy: an
/// active restore is a `Restore` object, and a provider refusal is something a
/// previous run was told. Passing them in keeps [`evaluate`] pure.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Protection {
    /// Points named by a nonterminal `Restore` or `RehearsalSchedule` — D3
    /// §6.4 step 4's `ActiveRestore`.
    pub active_restore: BTreeSet<String>,
    /// Points a provider refused on a previous attempt, keyed by point id with
    /// the closed code that refused them (`LegalHold`, `Denied`). They are kept
    /// and excluded from the next plan until the reason clears (D3 §6.5,
    /// "bounded retry").
    pub refused: BTreeMap<String, String>,
}

/// Everything one evaluation reads.
#[derive(Debug, Clone)]
pub struct Input<'a> {
    /// The destination this evaluation is FOR. Nothing outside it is seen.
    pub destination: &'a Destination,
    /// The catalog view's entries, in any order.
    pub points: &'a [PointFacts],
    /// `spec.rules`.
    pub rules: Rules,
    /// `spec.holds`.
    pub holds: &'a [Hold],
    /// The controller-supplied protection set.
    pub protection: &'a Protection,
    /// This evaluation's instant.
    pub now: DateTime<Utc>,
    /// `spec.enforcement.maxDeletionsPerRun`, or the CRD default when the
    /// policy is not in `Enforce` — a preview is bounded by the same ceiling
    /// the run would be, so approving a preview approves a runnable plan.
    pub max_deletions_per_run: i64,
}

// ---------------------------------------------------------------------------
// The outputs
// ---------------------------------------------------------------------------

/// Why a point qualifies for removal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CandidateReason {
    /// Older than `keepDays`.
    OlderThanKeepDays,
    /// Beyond `keepLast` by rank.
    BeyondKeepLast,
}

impl CandidateReason {
    /// The wire spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OlderThanKeepDays => "OlderThanKeepDays",
            Self::BeyondKeepLast => "BeyondKeepLast",
        }
    }
}

/// Why a point the rules selected is kept anyway.
///
/// `Unknown` PROTECTS. A point the evaluation could not classify is retained
/// with a reason, never removed — D3 §6.4 step 4's last line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtectReason {
    /// A nonterminal restore or rehearsal references it.
    ActiveRestore,
    /// The newest `minUsablePoints` usable points, whatever the rules say.
    MinUsablePoints,
    /// A provider refusal recorded it as held.
    LegalHold,
    /// A segment of this point appears in another retained point's manifest.
    SharedSegment,
    /// `spec.holds[]` still in force.
    Hold,
    /// The evaluation could not establish something it needed.
    Unknown,
}

impl ProtectReason {
    /// The wire spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ActiveRestore => "ActiveRestore",
            Self::MinUsablePoints => "MinUsablePoints",
            Self::LegalHold => "LegalHold",
            Self::SharedSegment => "SharedSegment",
            Self::Hold => "Hold",
            Self::Unknown => "Unknown",
        }
    }
}

/// Why a point was not considered at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// The catalog could not read it (`Unreadable`, `Missing`, `Partial`) or
    /// could not establish trust (`UntrustedSigner`, `Invalid`, `NoEvidence`,
    /// `NotAttempted`, `Revoked`) — or the controller itself refused the
    /// point's `Backup` evidence (`Invalid`, `Untrusted`), which no catalog row
    /// overrules ([`PointFacts::refused_by_controller`]). One reason for both,
    /// because to retention they are the same fact: trust in this point could
    /// not be established, so it is neither counted as usable nor deleted.
    Unreadable,
    /// The record's major version is above this build's.
    UnsupportedFormat,
    /// Two records disagree for one identity.
    Conflict,
    /// A tombstone says it is already gone.
    AlreadyDeleted,
}

impl SkipReason {
    /// The wire spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unreadable => "Unreadable",
            Self::UnsupportedFormat => "UnsupportedFormat",
            Self::Conflict => "Conflict",
            Self::AlreadyDeleted => "AlreadyDeleted",
        }
    }
}

/// One point the evaluation would remove, with the exact keys that removal
/// means.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    /// The point id.
    pub point_id: String,
    /// The archive set id.
    pub backup_id: String,
    /// Why it qualifies.
    pub reason: CandidateReason,
    /// Its capture start, in epoch milliseconds.
    pub recovery_point_at_ms: i64,
    /// The manifest key — deleted FIRST, so a half-deleted set never looks
    /// usable (D3 §6.5's execution order).
    pub manifest_key: String,
    /// Every segment key, in manifest order. Deleted after the manifest.
    pub segment_keys: Vec<String>,
    /// How many bytes, when known.
    pub bytes: Option<i64>,
}

impl Candidate {
    /// How many object keys removing this point means, **or `None` when this
    /// build cannot say** (review `d3w9` M1).
    ///
    /// The catalog view carries a point's `manifestKey` and no segment list, so
    /// for every plan this build writes the honest answer is "not observed".
    /// The first landing published `1` — the manifest, and nothing else — which
    /// an administrator approving a three-line plan would read as "this run
    /// removes three objects" while the run removed several thousand.
    ///
    /// **Absent means not observed, never zero and never a floor dressed as a
    /// total**, which is D3 §12's rule everywhere else. `status.lastEvaluation
    /// .candidates[].objects` is therefore omitted rather than wrong, the
    /// `Evaluated` condition says the plan does not enumerate, and
    /// `logweir-retention --dry-run` — which holds the list grant the run needs
    /// anyway — prints the real count per point.
    #[must_use]
    pub fn objects(&self) -> Option<i64> {
        if self.segment_keys.is_empty() {
            return None;
        }
        i64::try_from(self.segment_keys.len())
            .ok()
            .map(|n| n.saturating_add(1))
    }
}

/// One point that is kept for a reason the rules did not choose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Protected {
    /// The point id.
    pub point_id: String,
    /// Why.
    pub reason: ProtectReason,
}

/// One point the evaluation did not consider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skipped {
    /// The point id.
    pub point_id: String,
    /// Why.
    pub reason: SkipReason,
}

/// What one evaluation found. **Nothing here has been deleted.**
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Evaluation {
    /// How many points at this destination were considered.
    pub points_evaluated: i64,
    /// The point ids that stay, newest first.
    pub kept: Vec<String>,
    /// The points that would go, newest first, truncated to
    /// `maxDeletionsPerRun`.
    pub candidates: Vec<Candidate>,
    /// The points something protected.
    pub protected: Vec<Protected>,
    /// The points the evaluation could not classify.
    pub skipped: Vec<Skipped>,
    /// How many candidates the `maxDeletionsPerRun` truncation dropped. A
    /// number rather than a bool, so a console can say "50 of 380".
    pub truncated_by_cap: i64,
    /// What every point at this destination that is NOT a candidate still
    /// names — its backup set and the object keys the view knows for it.
    ///
    /// Not a status field and not in the plan bytes: it is the input of
    /// [`plan_document`]'s own rail, which refuses to render a line whose set
    /// a retained point still names (defect SHARED-SET-RETENTION). Derived
    /// from the evaluation's OUTPUT, independently of the grouping step 4
    /// performs — so it is a second line against a REGRESSION in step 4 or in
    /// the ceiling. It reads the same `PointFacts`, so it defends nothing
    /// against missing or wrong view data (review L2).
    pub retained: RetainedObjects,
}

/// The objects retained points still name — [`Evaluation::retained`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RetainedObjects {
    /// Every retained point's `backup_id`.
    pub backup_ids: BTreeSet<String>,
    /// Every retained point's manifest key and every segment key the view
    /// carried for it (none, on every view this build reads).
    pub keys: BTreeSet<String>,
}

// ---------------------------------------------------------------------------
// Deletion groups — which points one deletion would take together
// ---------------------------------------------------------------------------

/// Points linked by what deleting them removes: the same `backup_id`, the same
/// manifest key, or a common segment key, transitively (a union-find over the
/// points at this destination).
///
/// Keyed by the point's ADDRESS in the caller's slice, not by `point_id`: two
/// rows of one id are a catalog conflict, and grouping must not merge or split
/// them by spelling.
struct Groups {
    root_of: BTreeMap<usize, usize>,
}

impl Groups {
    fn of(points: &[&PointFacts]) -> Self {
        let addr = |p: &PointFacts| std::ptr::from_ref(p) as usize;
        let mut parent: Vec<usize> = (0..points.len()).collect();
        fn find(parent: &mut [usize], mut i: usize) -> usize {
            while parent[i] != i {
                parent[i] = parent[parent[i]];
                i = parent[i];
            }
            i
        }
        let mut first_with: BTreeMap<String, usize> = BTreeMap::new();
        for (i, p) in points.iter().enumerate() {
            // An EMPTY backup id names no set, so it links nothing; a point
            // with no set id and no keys is its own group.
            let links = (!p.backup_id.is_empty())
                .then(|| format!("set:{}", p.backup_id))
                .into_iter()
                .chain(p.manifest_key.iter().map(|m| format!("key:{m}")))
                .chain(p.segment_keys.iter().map(|k| format!("key:{k}")));
            for link in links {
                match first_with.get(&link) {
                    Some(&j) => {
                        let (a, b) = (find(&mut parent, i), find(&mut parent, j));
                        if a != b {
                            parent[a] = b;
                        }
                    }
                    None => {
                        first_with.insert(link, i);
                    }
                }
            }
        }
        let mut root_of = BTreeMap::new();
        for (i, p) in points.iter().enumerate() {
            let root = find(&mut parent, i);
            root_of.insert(addr(p), root);
        }
        Self { root_of }
    }

    /// The group of a point that is in the slice [`Groups::of`] was built
    /// from. A point that is not (impossible by construction: every verdict
    /// is built from that slice) falls into ONE shared group past every real
    /// root — the conservative direction, since merging groups can only
    /// protect more.
    fn of_point(&self, point: &PointFacts) -> usize {
        let addr = std::ptr::from_ref(point) as usize;
        self.root_of.get(&addr).copied().unwrap_or(usize::MAX)
    }
}

// ---------------------------------------------------------------------------
// The membership newtype — the wrong-destination rail
// ---------------------------------------------------------------------------

/// A point PROVED to be held at the destination under evaluation.
///
/// The type exists so that "we checked the location" is a fact the compiler
/// carries rather than a line somebody could delete. There is exactly one
/// constructor and it performs the check; `kept`, `candidates` and `protected`
/// are built from these alone.
#[derive(Debug, Clone, Copy)]
struct Located<'a> {
    point: &'a PointFacts,
}

impl<'a> Located<'a> {
    /// `Some` only when `point.locations` names `location_id`.
    ///
    /// A point with NO `locations[]` at all is refused too. The catalog writes
    /// one entry per receipt it actually read, and an entry that names no
    /// location is one whose location the catalog could not establish — which
    /// is the "could not tell" case, and "could not tell" never authorises a
    /// delete.
    fn at(point: &'a PointFacts, location_id: &str) -> Option<Self> {
        point
            .locations
            .iter()
            .any(|l| l == location_id)
            .then_some(Self { point })
    }
}

// ---------------------------------------------------------------------------
// The evaluation
// ---------------------------------------------------------------------------

/// D3 §6.4, in order.
///
/// 1. `usable` = `Available` ∧ (`Verified` | `VerifiedHistorical`) ∧ not
///    [`PointFacts::refused_by_controller`]. Everything else is `skipped` and
///    is **never** a candidate.
/// 2. Sort `usable` by `recovery_point_at_ms` DESC, tie-break `point_id` ASC.
/// 3. Keep: rank ≤ `keepLast`; age ≤ `keepDays`; and ALWAYS the newest
///    `minUsablePoints`, whatever the rules say.
/// 4. Protect, with a reason: `ActiveRestore`, `Hold`, `LegalHold`,
///    `SharedSegment`, `Unknown`.
/// 5. Candidates = `usable` − kept − protected, truncated to
///    `maxDeletionsPerRun`.
///
/// # The newest selectable point is never a candidate
///
/// `minUsablePoints` has a schema floor of 1, so step 3 always keeps at least
/// the newest usable point. [`evaluate`] does not rely on that: it takes
/// `max(min_usable_points, 1)`, so a policy that somehow carried 0 still keeps
/// one. `the_newest_selectable_point_is_never_a_candidate` is the row, and
/// `a_policy_that_would_empty_the_archive_deletes_nothing` is the extreme.
#[must_use]
pub fn evaluate(input: &Input<'_>) -> Evaluation {
    let location = input.destination.location_id.as_str();

    // 0. Membership. Everything after this line works on `Located` values.
    let here: Vec<Located<'_>> = input
        .points
        .iter()
        .filter_map(|p| Located::at(p, location))
        .collect();

    let mut out = Evaluation {
        points_evaluated: i64::try_from(here.len()).unwrap_or(i64::MAX),
        ..Evaluation::default()
    };

    // 1. usable, and the skip reasons for everything else.
    let mut usable: Vec<Located<'_>> = Vec::new();
    for located in &here {
        match skip_reason(located.point) {
            Some(reason) => out.skipped.push(Skipped {
                point_id: located.point.point_id.clone(),
                reason,
            }),
            None => usable.push(*located),
        }
    }
    out.skipped.sort_by(|a, b| a.point_id.cmp(&b.point_id));

    // 2. Newest first, tie-break on the id so the order is total and the plan
    //    bytes are a function of the inputs and not of the input ORDER.
    usable.sort_by(|a, b| {
        b.point
            .recovery_point_at_ms
            .cmp(&a.point.recovery_point_at_ms)
            .then_with(|| a.point.point_id.cmp(&b.point.point_id))
    });

    // 3. The keep set.
    let floor = usize::try_from(input.rules.min_usable_points.max(1)).unwrap_or(usize::MAX);
    let keep_last = input
        .rules
        .keep_last
        .map(|n| usize::try_from(n.max(0)).unwrap_or(usize::MAX));
    let cutoff_ms = input
        .rules
        .keep_days
        .map(|days| input.now.timestamp_millis() - days.saturating_mul(86_400_000));

    let holds: BTreeMap<&str, &Hold> = input
        .holds
        .iter()
        .filter(|h| h.in_force(input.now))
        .map(|h| (h.point_id.as_str(), h))
        .collect();

    // What each usable point's verdict is, before shared-segment analysis.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Verdict {
        Kept,
        Protected(ProtectReason),
        Candidate(CandidateReason),
    }

    let mut verdicts: Vec<(Located<'_>, Verdict)> = Vec::with_capacity(usable.len());
    for (rank, located) in usable.iter().enumerate() {
        let point = located.point;
        // THE FLOOR FIRST, and it is not a tie-break: D3 §6.4 step 3 says
        // "always the newest `minUsablePoints` usable points, whatever the
        // rules say". A policy that would leave fewer is REPORTED, not obeyed
        // — so the reason is `MinUsablePoints` and not `Kept`, and the console
        // can say which points the rules wanted and the floor saved.
        if rank < floor {
            let wanted_gone = keep_last.is_some_and(|n| rank >= n)
                || cutoff_ms.is_some_and(|c| point.recovery_point_at_ms < c);
            verdicts.push((
                *located,
                if wanted_gone {
                    Verdict::Protected(ProtectReason::MinUsablePoints)
                } else {
                    Verdict::Kept
                },
            ));
            continue;
        }
        if input.protection.active_restore.contains(&point.point_id) {
            verdicts.push((*located, Verdict::Protected(ProtectReason::ActiveRestore)));
            continue;
        }
        if holds.contains_key(point.point_id.as_str()) {
            verdicts.push((*located, Verdict::Protected(ProtectReason::Hold)));
            continue;
        }
        if input.protection.refused.contains_key(&point.point_id) {
            verdicts.push((*located, Verdict::Protected(ProtectReason::LegalHold)));
            continue;
        }
        // A point with no manifest key cannot be planned: the execution order
        // starts by deleting the manifest, and a set whose manifest key the
        // catalog never established is one this pass could not classify.
        let Some(_) = point.manifest_key.as_ref() else {
            verdicts.push((*located, Verdict::Protected(ProtectReason::Unknown)));
            continue;
        };
        // NOR can a point whose set id names no single directory (review L8):
        // an empty id is the bound `<scope>//`, and an id with a `/` is a
        // bound that CONTAINS other sets — set `a`'s enumeration would remove
        // set `a/b`. Generated ids never look like either; "could not tell"
        // never authorises a delete, so such a point is kept `Unknown` rather
        // than left to refuse the whole plan at the writer.
        if point.backup_id.is_empty() || point.backup_id.contains('/') {
            verdicts.push((*located, Verdict::Protected(ProtectReason::Unknown)));
            continue;
        }

        // WITHIN the rules is kept; outside either rule is a candidate.
        // D3 §6.4's union, with `OlderThanKeepDays` taking precedence over
        // `BeyondKeepLast` — the existing rule in `crate::retention`.
        let too_old = cutoff_ms.is_some_and(|c| point.recovery_point_at_ms < c);
        let beyond_rank = keep_last.is_some_and(|n| rank >= n);
        let verdict = if too_old {
            Verdict::Candidate(CandidateReason::OlderThanKeepDays)
        } else if beyond_rank {
            Verdict::Candidate(CandidateReason::BeyondKeepLast)
        } else {
            Verdict::Kept
        };
        verdicts.push((*located, verdict));
    }

    // 4. Shared sets and shared segments (defect SHARED-SET-RETENTION).
    //
    //    Points are GROUPED by what their deletion would remove: two points are
    //    in one group when they name the same backup set (`backup_id`), the
    //    same manifest key, or a common segment key — transitively. A group
    //    that holds ANY retained point (kept, protected, or skipped) is a group
    //    none of whose candidates may be deleted: every candidate in it is
    //    protected `SharedSegment`. v1 never partially deletes a shared set.
    //
    //    WHY THE SET AND NOT ONLY THE SEGMENTS. The catalog view carries no
    //    segment keys, so a segment-only rule had nothing to apply to — and two
    //    receipts CAN name one set: a runner Job re-created from its frozen
    //    inputs rewrites the same `<scope>/<backup_id>/` and signs a second
    //    receipt over it (PLAT-06.1 case e; live on the lab as harness-rows-11's
    //    `retention-shared-set-is-never-planned-under-a-retained-point`). With
    //    `keepLast 1` the older receipt was planned, and deleting it enumerates
    //    and removes the set the KEPT receipt names. Every key a plan line may
    //    delete is bounded by `<scope>/<backup_id>/` (`validate_key`, and the
    //    worker's own re-derivation), so the set is exactly the unit a deletion
    //    removes, and the unit that must be matched.
    //
    //    A SKIPPED point is retained too — it is never a candidate — so it
    //    protects a candidate that shares with it (review L3). Otherwise a
    //    point the catalog or the controller refused could lose its set through
    //    another point's deletion.
    //
    //    Transitive, so a candidate linked to a retained point through ANOTHER
    //    candidate is protected as well: that other candidate is now retained.
    //
    //    AND A ROW WHOSE LOCATION THE CATALOG COULD NOT ESTABLISH (no
    //    `locations[]` at all, review L1) is retained too. It is never counted
    //    here and never a candidate — but if it names a candidate's set, "could
    //    not tell where it lives" must not authorise deleting what it names.
    let location_less: Vec<&PointFacts> = input
        .points
        .iter()
        .filter(|p| p.locations.is_empty())
        .collect();
    let grouped: Vec<&PointFacts> = here
        .iter()
        .map(|l| l.point)
        .chain(location_less.iter().copied())
        .collect();
    let groups = Groups::of(&grouped);
    let retained_groups: BTreeSet<usize> = verdicts
        .iter()
        .filter(|(_, v)| !matches!(v, Verdict::Candidate(_)))
        .map(|(l, _)| l.point)
        .chain(
            here.iter()
                .map(|l| l.point)
                .filter(|p| skip_reason(p).is_some()),
        )
        .chain(location_less.iter().copied())
        .map(|p| groups.of_point(p))
        .collect();
    for (located, verdict) in &mut verdicts {
        if matches!(verdict, Verdict::Candidate(_))
            && retained_groups.contains(&groups.of_point(located.point))
        {
            *verdict = Verdict::Protected(ProtectReason::SharedSegment);
        }
    }

    // 5. Project. `kept` and `candidates` stay in newest-first order; the
    //    protected list is sorted by id so the status block is stable.
    //
    //    THE CEILING IS APPLIED PER GROUP, NEVER THROUGH ONE. Every group that
    //    still holds a candidate holds ONLY candidates (step 4), and a group is
    //    selected whole or not at all: selecting one receipt of a shared set
    //    and keeping its sibling "over the ceiling" would remove the set a
    //    point this evaluation reports as kept still names. A group larger
    //    than the room left is kept and counted in `truncated_by_cap`, and the
    //    walk continues with the groups that fit.
    let cap = usize::try_from(input.max_deletions_per_run.max(0)).unwrap_or(usize::MAX);
    let mut group_size: BTreeMap<usize, usize> = BTreeMap::new();
    for (located, verdict) in &verdicts {
        if matches!(verdict, Verdict::Candidate(_)) {
            *group_size
                .entry(groups.of_point(located.point))
                .or_default() += 1;
        }
    }
    let mut group_selected: BTreeMap<usize, bool> = BTreeMap::new();
    let mut selected = 0usize;
    let mut over_cap = 0i64;
    for (located, verdict) in &verdicts {
        let point = located.point;
        match verdict {
            Verdict::Kept => out.kept.push(point.point_id.clone()),
            Verdict::Protected(reason) => {
                out.kept.push(point.point_id.clone());
                out.protected.push(Protected {
                    point_id: point.point_id.clone(),
                    reason: *reason,
                });
            }
            Verdict::Candidate(reason) => {
                let group = groups.of_point(point);
                let take = *group_selected.entry(group).or_insert_with(|| {
                    let size = group_size.get(&group).copied().unwrap_or(1);
                    let fits = selected.saturating_add(size) <= cap;
                    if fits {
                        selected += size;
                    }
                    fits
                });
                if !take {
                    // Over the ceiling is KEPT and counted, never silently
                    // dropped: a console that showed 50 candidates out of 380
                    // without saying so would read as "380 is all there is".
                    out.kept.push(point.point_id.clone());
                    over_cap += 1;
                    continue;
                }
                out.candidates.push(Candidate {
                    point_id: point.point_id.clone(),
                    backup_id: point.backup_id.clone(),
                    reason: *reason,
                    recovery_point_at_ms: point.recovery_point_at_ms,
                    manifest_key: point
                        .manifest_key
                        .clone()
                        .expect("a point with no manifest key was protected as Unknown above"),
                    segment_keys: point.segment_keys.clone(),
                    bytes: point.bytes,
                });
            }
        }
    }
    // WHAT STAYS, as the objects it names — the input of `plan_document`'s
    // own rail, derived from the OUTPUT rather than from the grouping above, so
    // a regression in step 4 is refused at the writer instead of approved.
    let chosen: BTreeSet<&str> = out.candidates.iter().map(|c| c.point_id.as_str()).collect();
    let mut retained = RetainedObjects::default();
    for p in &grouped {
        if chosen.contains(p.point_id.as_str()) {
            continue;
        }
        retained.backup_ids.insert(p.backup_id.clone());
        if let Some(m) = &p.manifest_key {
            retained.keys.insert(m.clone());
        }
        retained.keys.extend(p.segment_keys.iter().cloned());
    }
    out.retained = retained;
    out.protected.sort_by(|a, b| a.point_id.cmp(&b.point_id));
    out.truncated_by_cap = over_cap;
    out
}

/// D3 §6.4 step 1, as one function so no surface writes a second copy.
///
/// `None` means usable. Availability is asked first: "the bytes are not there"
/// and "the signature did not verify" are different facts, and reporting the
/// second when the first is true sends an operator to the wrong place.
#[must_use]
pub fn skip_reason(point: &PointFacts) -> Option<SkipReason> {
    match point.availability {
        Availability::Available => {}
        Availability::Deleted => return Some(SkipReason::AlreadyDeleted),
        Availability::Conflict => return Some(SkipReason::Conflict),
        Availability::UnsupportedFormat => return Some(SkipReason::UnsupportedFormat),
        Availability::Missing | Availability::Unreadable | Availability::Partial => {
            return Some(SkipReason::Unreadable)
        }
    }
    // A VERDICT THE CONTROLLER REACHED OUTRANKS THE ROW. The row may predate
    // the refusal (a view is served until `viewExpiresAt`); the controller's
    // `Invalid`/`Untrusted` on this point's own `Backup` is newer information
    // about the same receipt bytes.
    if point.refused_by_controller {
        return Some(SkipReason::Unreadable);
    }
    match point.verification {
        Verification::Verified | Verification::VerifiedHistorical => None,
        _ => Some(SkipReason::Unreadable),
    }
}

// ---------------------------------------------------------------------------
// Scope validation — the rail that runs BEFORE any plan is written
// ---------------------------------------------------------------------------

/// Why a key may not be named in a plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeViolation {
    /// The key does not start with `<scope_prefix>/<backup_id>/`.
    OutsideScope {
        /// The offending key.
        key: String,
        /// The prefix it had to start with.
        expected_prefix: String,
    },
    /// The key is under `logweir/`, the evidence root.
    EvidenceRoot {
        /// The offending key.
        key: String,
    },
}

impl std::fmt::Display for ScopeViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutsideScope {
                key,
                expected_prefix,
            } => write!(
                f,
                "key `{key}` is not under `{expected_prefix}`, which is this policy's scope for \
                 that backup set"
            ),
            Self::EvidenceRoot { key } => write!(
                f,
                "key `{key}` is under `{EVIDENCE_ROOT}`, the evidence root no retention run may \
                 delete under: receipts, scorecards, catalog records, tombstones and the \
                 retention records themselves live there, so a deleted point's audit trail \
                 outlives the point"
            ),
        }
    }
}

impl std::error::Error for ScopeViolation {}

/// The per-key rule, stated once.
///
/// A key must start with `<scope_prefix>/<backup_id>/` and must not start with
/// `logweir/`. The evidence check is made INDEPENDENTLY of the prefix check
/// rather than derived from it: a scope prefix that itself began with
/// `logweir/` is refused by the CRD's K4 rule, but this function is also the
/// worker's last rail and must not depend on an admission rule having run.
///
/// # Errors
///
/// [`ScopeViolation`], naming the key and what it had to satisfy.
pub fn validate_key(key: &str, scope_prefix: &str, backup_id: &str) -> Result<(), ScopeViolation> {
    if key.starts_with(EVIDENCE_ROOT) {
        return Err(ScopeViolation::EvidenceRoot {
            key: key.to_string(),
        });
    }
    let expected = format!("{}/{}/", scope_prefix.trim_end_matches('/'), backup_id);
    if !key.starts_with(&expected) {
        return Err(ScopeViolation::OutsideScope {
            key: key.to_string(),
            expected_prefix: expected,
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The plan document
// ---------------------------------------------------------------------------

/// Who wrote a plan, and against what.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanIdentity {
    /// The policy's `metadata.namespace`.
    pub namespace: String,
    /// The policy's `metadata.name`.
    pub name: String,
    /// The policy's `metadata.uid`.
    pub uid: String,
    /// The `metadata.generation` this plan was computed from.
    ///
    /// **It does NOT reach the digested bytes** (review `d3w9` C1): approving a
    /// plan is a spec patch, a spec patch bumps the generation, and a digest
    /// that the act of approving changes can never be approved. What
    /// invalidates an approval is the CONTENT — the rules as applied and the
    /// exact lines — and both are in the bytes. This value is published on
    /// `status.lastEvaluation` and recorded in the enforcement record, where it
    /// is a fact about the pass rather than about the deletion.
    pub generation: i64,
}

/// One line of the plan: everything deleting one point means.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct PlanLine {
    /// The point id. Every delete is attributable to exactly one of these.
    pub point_id: String,
    /// The archive set id.
    pub backup_id: String,
    /// Why the rules chose it.
    pub reason: String,
    /// Its capture start, in epoch milliseconds.
    pub recovery_point_at_ms: i64,
    /// The manifest key. Deleted FIRST.
    pub manifest_key: String,
    /// The set's own key prefix, `<scope_prefix>/<backup_id>/`. Every key on
    /// this line must be under it, and it is the bound the worker enumerates
    /// within when [`PlanLine::enumerate_set`] is set.
    pub set_prefix: String,
    /// Whether the worker must list `set_prefix` for the keys this plan could
    /// not name.
    ///
    /// **`true` for every plan this build writes, and that is a recorded
    /// deviation from D3 §6.4 step 6**, which asks for "the complete, explicit
    /// list of object keys". The catalog view the evaluation reads carries a
    /// point's `manifestKey` and no segment list (D3 W8's contract: the window
    /// is bounded by bytes, so an unbounded per-point key list is not in it),
    /// and this controller holds no archive credential for the destination with
    /// which to go and look. So the approved document fixes the point set, the
    /// manifest keys and the key BOUND, and the worker re-validates every
    /// enumerated key against that bound before deleting it — which is the
    /// second half of D3 §6.5's own wrong-prefix rule ("listed from that
    /// point's own manifest or set directory"). It becomes `false`, with no
    /// other change, the day a view entry carries its segment keys.
    pub enumerate_set: bool,
    /// Every key this plan could name, manifest first — never a glob.
    pub object_keys: Vec<String>,
    /// Other points whose receipts name this SAME set and are removed with it
    /// (review M2). Two receipts over one set that are both due used to render
    /// two lines with one manifest key, which the worker refuses as
    /// `DuplicateKey` — the WHOLE plan, on every run, so one shared set stopped
    /// every deletion the policy owed. One line per set, with every point it
    /// removes named here: each delete stays attributable to one line and each
    /// point gets its own outcome and tombstones in the record. Skipped when
    /// empty, so a plan with no shared set is byte-identical to one from before
    /// this field and its approved digest still holds.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub co_point_ids: Vec<String>,
}

/// The plan document — D3 §6.4 step 6.
///
/// Serialised with `logweir_core::det_json::to_deterministic_json`, so the
/// bytes are a pure function of the fields in declaration order and
/// `planSha256` is reproducible by anyone holding the same inputs.
///
/// # WHAT IS DELIBERATELY *NOT* IN HERE, AND WHY (review `d3w9` C1)
///
/// Three fields were in the first landing and are gone: `evaluated_at`,
/// `policy_generation` and `points_evaluated`. Each of them moves without what
/// would be deleted moving, and **a digest that changes on its own can never be
/// approved**:
///
/// * `evaluated_at` was `ctx.now`, and the reconciler requeues every 60 s. Two
///   renderings of one archive three seconds apart digested differently, so an
///   administrator who copied `status.lastEvaluation.planSha256` onto
///   `spec.enforcement.approvedPlanSha256` was always copying a value that had
///   already expired. **No Job could ever be created.**
/// * `policy_generation` is worse, and is the same defect wearing a different
///   hat: `approvedPlanSha256` is a SPEC field, so the very act of approving
///   bumps `metadata.generation`, which would change the digest the approval
///   names. Approval was circularly impossible.
/// * `points_evaluated` counts skipped points too, so a newly-arrived
///   `Unreadable` point invalidated an approval without changing one key.
///
/// All three are on `status.lastEvaluation` (and in the enforcement record),
/// which is where a reader wants them; none is a fact about what the run
/// removes.
///
/// **What still invalidates an approval is content, not a counter.** A rules
/// edit changes `keep_last` / `keep_days` / `min_usable_points` AND the
/// candidate set; a `holds` edit changes the protected set and therefore the
/// lines; a new backup shifts ranks and therefore the lines. `policy_uid`,
/// `location_id` and `scope_prefix` pin *where*. The digest covers exactly
/// "these keys, at this location, under these rules" — which is what an
/// administrator is being asked to approve.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct PlanDocument {
    /// The media type, so a reader that got the wrong document says so.
    pub format: String,
    /// The document version.
    pub format_version: String,
    /// The policy's namespace.
    pub policy_namespace: String,
    /// The policy's name.
    pub policy_name: String,
    /// The policy's UID. A deleted-and-recreated policy of the same name is a
    /// different policy and its old plan does not apply.
    pub policy_uid: String,
    /// The destination's location id — `s3://bucket/prefix`.
    pub location_id: String,
    /// The immutable scope prefix. The worker re-derives every key bound from
    /// this and refuses the plan if any key escapes it.
    pub scope_prefix: String,
    /// `keepLast`, as applied.
    pub keep_last: Option<i64>,
    /// `keepDays`, as applied.
    pub keep_days: Option<i64>,
    /// `minUsablePoints`, as applied.
    pub min_usable_points: i64,
    /// The lines. Empty is a legitimate plan: it deletes nothing.
    pub lines: Vec<PlanLine>,
}

impl PlanDocument {
    /// Every object key this plan names, manifests first, in line order.
    #[must_use]
    pub fn object_count(&self) -> i64 {
        self.lines
            .iter()
            .map(|l| i64::try_from(l.object_keys.len()).unwrap_or(i64::MAX))
            .fold(0i64, i64::saturating_add)
    }
}

/// Why a plan could not be rendered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanError {
    /// A key escaped the scope. The plan is not written at all.
    Scope(ScopeViolation),
    /// The document did not serialise — named rather than unwrapped.
    Encode(String),
    /// A line would remove a set, or an object, that a point this evaluation
    /// RETAINS still names (defect SHARED-SET-RETENTION). The plan is not
    /// written at all.
    SharedWithRetained {
        /// The candidate whose line was refused.
        point_id: String,
        /// What the retained point still names: its set, or one of its keys.
        named: String,
    },
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scope(v) => write!(f, "the plan would name a key outside its scope: {v}"),
            Self::Encode(e) => write!(f, "the plan document did not serialise: {e}"),
            Self::SharedWithRetained { point_id, named } => write!(
                f,
                "the plan would remove point `{point_id}`'s set, and a point this evaluation \
                 retains still names {named}; deleting it would take the retained point's data \
                 with it, so no plan is written"
            ),
        }
    }
}

impl std::error::Error for PlanError {}

/// Render the plan document for an evaluation.
///
/// **Every key is validated before a single byte is written.** A plan
/// containing one out-of-scope key is not a plan with one bad line; it is not a
/// plan, and this returns `Err` rather than emitting a document an
/// administrator could approve.
///
/// **It takes no clock.** See [`PlanDocument`]'s own note: an instant in the
/// digested bytes is what made the first landing's approval gate unreachable,
/// and the surest way not to put one back is to have none to put.
///
/// # Errors
///
/// [`PlanError::Scope`] if any key escapes `<scope_prefix>/<backup_id>/` or
/// names the evidence root; [`PlanError::Encode`] if the document does not
/// serialise.
pub fn plan_document(
    identity: &PlanIdentity,
    destination: &Destination,
    rules: Rules,
    evaluation: &Evaluation,
) -> Result<PlanDocument, PlanError> {
    let mut lines: Vec<PlanLine> = Vec::with_capacity(evaluation.candidates.len());
    for candidate in &evaluation.candidates {
        let mut object_keys = Vec::with_capacity(candidate.segment_keys.len() + 1);
        // THE MANIFEST FIRST, and the order is the execution order: a run
        // interrupted after the manifest and before the segments leaves a set
        // that cannot be mistaken for usable.
        object_keys.push(candidate.manifest_key.clone());
        object_keys.extend(candidate.segment_keys.iter().cloned());
        for key in &object_keys {
            validate_key(key, &destination.scope_prefix, &candidate.backup_id)
                .map_err(PlanError::Scope)?;
        }
        let set_prefix = format!(
            "{}/{}/",
            destination.scope_prefix.trim_end_matches('/'),
            candidate.backup_id
        );
        // THE WRITER'S OWN RAIL (defect SHARED-SET-RETENTION). The worker may
        // enumerate and remove everything under `set_prefix`, so no retained
        // point may name this set, nor any key under it. Checked against what
        // the evaluation RETAINED — not against the grouping that decided it —
        // so a regression in `evaluate` step 4 is a refused plan, never an
        // approvable one.
        if evaluation
            .retained
            .backup_ids
            .contains(&candidate.backup_id)
        {
            return Err(PlanError::SharedWithRetained {
                point_id: candidate.point_id.clone(),
                named: format!("backup set `{}`", candidate.backup_id),
            });
        }
        if let Some(key) = evaluation
            .retained
            .keys
            .iter()
            .find(|k| k.starts_with(&set_prefix) || object_keys.contains(k))
        {
            return Err(PlanError::SharedWithRetained {
                point_id: candidate.point_id.clone(),
                named: format!("object `{key}`"),
            });
        }
        // ONE LINE PER SET (review M2). A candidate naming the same set and
        // manifest as a line already written joins that line as a co-point:
        // `evaluate` selects a shared set's receipts together, so they arrive
        // here together, and two lines over one manifest are a plan the worker
        // refuses whole.
        if let Some(existing) = lines.iter_mut().find(|l| {
            l.backup_id == candidate.backup_id && l.manifest_key == candidate.manifest_key
        }) {
            existing.co_point_ids.push(candidate.point_id.clone());
            for key in object_keys.into_iter().skip(1) {
                if !existing.object_keys.contains(&key) {
                    existing.object_keys.push(key);
                }
            }
            existing.enumerate_set &= candidate.segment_keys.is_empty();
            continue;
        }
        lines.push(PlanLine {
            point_id: candidate.point_id.clone(),
            backup_id: candidate.backup_id.clone(),
            reason: candidate.reason.as_str().to_string(),
            recovery_point_at_ms: candidate.recovery_point_at_ms,
            manifest_key: candidate.manifest_key.clone(),
            set_prefix,
            // See `PlanLine::enumerate_set`: the view carries no segment keys,
            // so today every line names its manifest and its bound.
            enumerate_set: candidate.segment_keys.is_empty(),
            object_keys,
            co_point_ids: Vec::new(),
        });
    }
    Ok(PlanDocument {
        format: PLAN_MEDIA_TYPE.to_string(),
        format_version: PLAN_FORMAT_VERSION.to_string(),
        policy_namespace: identity.namespace.clone(),
        policy_name: identity.name.clone(),
        policy_uid: identity.uid.clone(),
        location_id: destination.location_id.clone(),
        scope_prefix: destination.scope_prefix.clone(),
        keep_last: rules.keep_last,
        keep_days: rules.keep_days,
        min_usable_points: rules.min_usable_points,
        lines,
    })
}

/// The plan's canonical bytes, and their digest.
///
/// # Errors
///
/// [`PlanError::Encode`] if the document does not serialise.
pub fn plan_bytes(document: &PlanDocument) -> Result<(Vec<u8>, String), PlanError> {
    let bytes = logweir_core::det_json::to_deterministic_json(document)
        .map_err(|e| PlanError::Encode(e.to_string()))?;
    let digest = logweir_core::ids::sha256_prefixed(&bytes);
    Ok((bytes, digest))
}

/// The plan `ConfigMap`'s name — **the one implementation, and the one the
/// controller calls** (review `d3w9` L3).
///
/// `<stem>-plan-<12 hex of planSha256>`, where the stem is
/// [`config_map_stem`] over the policy's UID. CONTENT-NAMED rather than
/// generation-named, because a generation-named object collides the moment the
/// archive moves under an unchanged spec, which is the normal case; and
/// UID-stemmed rather than name-stemmed, because `metadata.name` is a
/// 253-character DNS subdomain and a policy name may already use all of it.
///
/// The `ConfigMap` is immutable and owned by the policy, so a second pass at the
/// same digest gets 409 `AlreadyExists` rather than rewriting bytes under an
/// administrator who is reading them. **The controller publishes the result in
/// `status.lastEvaluation.planRef`** (finding M9), so no reader ever recomputes
/// it — but this is the function it would recompute it with, and it is short
/// enough to be checkable.
///
/// **THE EVALUATION PUBLISHES THE REF, NOT THE RUN** (defect
/// RET-STALE-PLANREF). `publish_evaluation` names the plan it just rendered, so
/// the ref and the `planSha256` beside it always describe the same plan. The
/// `ConfigMap` is materialized by the pass that STARTS a run, so in `Report`
/// mode — and on any evaluation that does not start one — the ref names an
/// object that does not exist yet. That is the documented absent-object
/// behaviour and not a fault: the name is a pure function of the policy UID and
/// the digest, so it is exactly as true as the digest it sits next to.
#[must_use]
pub fn plan_config_map_name(policy_uid: &str, plan_sha256: &str) -> String {
    let digest = plan_sha256.trim_start_matches("sha256:");
    let tail: String = digest.chars().take(12).collect();
    format!("{}-plan-{tail}", config_map_stem(policy_uid))
}

/// The per-policy object-name stem: `lwr-<20 hex of sha256(uid)>`.
///
/// From the UID and never the name, for the reason
/// [`plan_config_map_name`] gives, and 20 hex characters because that is what
/// the catalog view's own names use (`catalog_view::OWNER_UID_HEX_CHARS`).
#[must_use]
pub fn config_map_stem(policy_uid: &str) -> String {
    format!(
        "{NAME_PREFIX}{}",
        &logweir_core::ids::sha256_hex(policy_uid.as_bytes())[..20]
    )
}

/// The prefix every object this controller creates wears.
pub const NAME_PREFIX: &str = "lwr-";

/// The plan `ConfigMap`'s one data key.
pub const PLAN_DATA_KEY: &str = "plan.json";

/// The annotation carrying the plan digest, so a reader that fetched the
/// `ConfigMap` can check what it read without re-serialising.
pub const PLAN_DIGEST_ANNOTATION: &str = "logweir.dev/retention-plan-sha256";

// ---------------------------------------------------------------------------
// Where an enforcement record goes
// ---------------------------------------------------------------------------

/// The key one run's attributable record is written to — D3 §6.5.
///
/// Under `logweir/`, which the retention credential cannot delete and which
/// this plan's own validation refuses to name. That is the point: the audit
/// trail of a deleted point survives the point.
#[must_use]
pub fn record_key(policy_uid: &str, run_id: &str) -> String {
    format!("{EVIDENCE_ROOT}retention/{policy_uid}/{run_id}.json")
}

/// A run id — `r<16 hex>` of the policy UID, the plan digest and the slot.
///
/// DETERMINISTIC, so a controller that crashed between writing the lease and
/// creating the Job computes the same id on the next pass and gets 409
/// `AlreadyExists` from the API server instead of running a second deletion.
#[must_use]
pub fn run_id(policy_uid: &str, plan_sha256: &str, slot: i64) -> String {
    let material = format!("{policy_uid}:{plan_sha256}:{slot}");
    format!(
        "r{}",
        &logweir_core::ids::sha256_hex(material.as_bytes())[..16]
    )
}

// ---------------------------------------------------------------------------
// The legacy `BackupSchedule` report's honesty note — D3 §6.3
// ---------------------------------------------------------------------------

/// What replaces a legacy retention report when the controller's one archive
/// handle points somewhere else — decision D3 §6.3, defect **RET-WRONGBUCKET**.
///
/// The defect: `controllers/backup_schedule.rs` lists manifests through
/// `Context::archive` (built once from `LOGWEIR_ARCHIVE_URL`) while rendering
/// `aws s3 rm` / `mc rm` commands for `spec.archive.url`. A schedule writing to
/// another bucket therefore got a report of the CONTROLLER's bucket wearing its
/// own bucket's commands — and since D1 W2 made `destinationRef` editable, the
/// mismatch is also reachable by an edit between runs.
///
/// The fix for a destination-backed archive is a `RetentionPolicy`, which reads
/// its own destination's catalog view. The fix for a legacy schedule is this:
/// say so, and report nothing.
pub const LEGACY_DESTINATION_MISMATCH_NOTE: &str =
    "the controller's archive handle points at a different destination; this schedule's \
     retention is not evaluated here — create a RetentionPolicy";

/// Whether the controller's one archive handle is over the same location the
/// schedule names.
///
/// Compared on the CONTAINER AND THE PREFIX after
/// [`crate::retention::bucket_and_prefix`] has normalised both, so
/// `s3://b/p` and `s3://b/p/` are one location and `s3://b/p` and `s3://b/pp`
/// are two.
///
/// # An UNKNOWN controller location answers `true`, and here is why
///
/// `None` means the caller could not say where the handle points. In the
/// SHIPPED BINARY that pairing cannot occur: `main` builds the handle only when
/// [`crate::retention::configured_archive_url`] returned `Some`, and the
/// schedule reconciler re-reads the same process variable, which cannot change
/// under a running process. So `Some(handle)` with `None` location is reachable
/// only from a test that constructs the two inconsistently — an in-memory
/// `Store` with no `LOGWEIR_ARCHIVE_URL` — and answering `false` there would
/// replace a REPORT that a landed test asserts with a mismatch note about a
/// mismatch nobody can be in.
///
/// The polarity is therefore "evaluate, as before, unless the two locations are
/// both known and differ". `the_legacy_report_is_replaced_when_the_handle_is_
/// elsewhere` is the row that drives the real mismatch, with both locations
/// named.
#[must_use]
pub fn legacy_report_applies(schedule_url: &str, controller_url: Option<&str>) -> bool {
    let Some(controller_url) = controller_url.filter(|u| !u.is_empty()) else {
        return true;
    };
    let scheme_of = |u: &str| u.split_once("://").map(|(s, _)| s.to_string());
    if scheme_of(schedule_url) != scheme_of(controller_url) {
        return false;
    }
    let mine = crate::retention::bucket_and_prefix(schedule_url);
    let theirs = crate::retention::bucket_and_prefix(controller_url);
    let norm =
        |(container, prefix): (String, String)| (container, prefix.trim_matches('/').to_string());
    norm(mine) == norm(theirs)
}
