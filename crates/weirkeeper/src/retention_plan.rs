//! What a `RetentionPolicy` would remove from one destination (D3 §6.4).
//!
//! # Pure
//!
//! Nothing here reads a clock, dials a bucket or holds a `kube::Client`: `now`,
//! the catalog points and the protection set are arguments. Every rule in D3
//! §6.4 and §6.5 is therefore a table test, and the plan document is a pure
//! function of its inputs, so an administrator who approves `planSha256` is
//! approving bytes anyone can recompute.
//!
//! # A point at another destination cannot be counted
//!
//! [`Located`] is a private newtype whose only constructor, [`Located::at`],
//! refuses a point whose `locations[]` does not contain the destination being
//! evaluated, and `kept`, `candidates` and `protected` are built from `Located`
//! values alone. A point frozen against destination A has no value of the
//! right type to be counted against destination B
//! (`a_point_at_another_destination_is_never_a_candidate`). The input is the
//! destination's catalog view (D3 §5.3), never a bucket walk with a global
//! handle and never a schedule's current URL.
//!
//! # `Unknown` is retained
//!
//! D3 §6.4 step 1: a point is a deletion candidate only if the catalog said
//! `Available` and (`Verified` | `VerifiedHistorical`). Everything else
//! (`Unreadable`, `Partial`, `Conflict`, `UntrustedSigner`, `NotAttempted`) is
//! skipped and never a candidate, so a pass that cannot read the archive
//! proposes nothing, unlike a timestamp-driven bucket lifecycle rule.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::catalog_view::{Availability, Verification};
use logweir_core::check_contract::REDACTED as REDACTION_MARKER;

/// The plan document's media type — D3 §6.4 step 6.
pub const PLAN_MEDIA_TYPE: &str = "application/vnd.logweir.retention-plan+json;version=1.0.0";

/// The plan document's `format_version`.
pub const PLAN_FORMAT_VERSION: &str = "1.0.0";

/// The evidence root a retention run may never delete under (Global
/// Constraint 6, and the K4 CEL rule's other half). Receipts, sidecars,
/// scorecards, catalog records, tombstones and the retention records live
/// here, excluded from every plan, from the credential's documented IAM scope
/// and from the worker's own pre-delete validation, so a deleted point's audit
/// trail survives it.
pub const EVIDENCE_ROOT: &str = "logweir/";

/// The destination being evaluated, as the two facts retention needs:
/// `location_id`, the catalog view's spelling of the destination's canonical
/// `s3://bucket/prefix` URL, and `scope_prefix`, the immutable
/// `RetentionPolicy.spec.scope.prefix` outside which no key may be named.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Destination {
    /// The catalog's location id for this destination.
    pub location_id: String,
    /// `spec.scope.prefix`.
    pub scope_prefix: String,
}

/// One catalog point, as retention sees it: a `catalog_view::ViewEntry` plus
/// the point's own object keys. Every field is a fact the catalog established,
/// never a control-plane bucket walk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PointFacts {
    /// `lwp1-<32 hex>` — content-derived from the receipt bytes.
    pub point_id: String,
    /// The archive set id. A key may only be named under
    /// `<scope_prefix>/<backup_id>/`.
    pub backup_id: String,
    /// `capture.started_at`, in epoch milliseconds. The ordering axis.
    pub recovery_point_at_ms: i64,
    /// Every location holding this point, as location ids: the membership test
    /// that makes a wrong-destination evaluation unconstructible.
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
    /// reached that is not a pass (`Invalid`, `Untrusted`, or unknown to this
    /// build); see `catalog_view::ControllerRefusals`.
    ///
    /// The catalog decides only where the controller could not look: a view
    /// row served until `viewExpiresAt` may predate a replaced receipt or a
    /// revoked signer, and counted as usable it would take a `keepLast` or
    /// `minUsablePoints` rank and push an older good point into the plan. Such
    /// a point is skipped `Unreadable` ([`skip_reason`]). Always `false` for a
    /// catalog-only point.
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

/// The protection set the controller supplies: observations that live in the
/// cluster rather than the catalog (an active restore is a `Restore` object, a
/// provider refusal is something a previous run was told). Passed in so
/// [`evaluate`] stays pure.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Protection {
    /// Points named by a nonterminal `Restore` — D3 §6.4 step 4's
    /// `ActiveRestore`.
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
/// `Unknown` protects: a point the evaluation could not classify is retained
/// with a reason, never removed (D3 §6.4 step 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtectReason {
    /// A nonterminal restore references it.
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
    /// `NotAttempted`, `Revoked`), or the controller refused the point's
    /// `Backup` evidence ([`PointFacts::refused_by_controller`]). One reason,
    /// because to retention it is one fact: trust could not be established, so
    /// the point is neither counted as usable nor deleted.
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
    /// How many object keys removing this point means, or `None` when this
    /// build cannot say.
    ///
    /// The catalog view carries a point's `manifestKey` and no segment list, so
    /// for every plan this build writes the answer is "not observed". Absent
    /// means not observed, never zero and never a floor dressed as a total (D3
    /// §12): `status.lastEvaluation.candidates[].objects` is omitted, the
    /// `Evaluated` condition says the plan does not enumerate, and
    /// `logweir-retention --dry-run` prints the real count per point.
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

/// What one evaluation found. Nothing here has been deleted.
///
/// The accounting closes: every point at this destination is in exactly one of
/// four places ([`Evaluation::accounts_for_every_point`]):
///
/// ```text
/// points_evaluated = kept + candidates + held_back + skipped
/// ```
///
/// `protected` is not a fifth: every protected point is in `kept`, with its
/// reason beside it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Evaluation {
    /// How many points at this destination were considered.
    pub points_evaluated: i64,
    /// The point ids that stay, newest first: the rules keep them, or something
    /// protects them (those are in `protected` too). Never a point the per-run
    /// ceiling held back: those are in [`Evaluation::held_back`].
    pub kept: Vec<String>,
    /// The points THIS PLAN would remove, newest first — at most
    /// `maxDeletionsPerRun` of them.
    pub candidates: Vec<Candidate>,
    /// The points the rules would remove and nothing protects, that the per-run
    /// ceiling left out of this plan, newest first. Due, not kept: nothing
    /// deletes them in this run, and a later plan takes them.
    pub held_back: Vec<String>,
    /// The points something protected.
    pub protected: Vec<Protected>,
    /// The points the evaluation could not classify.
    pub skipped: Vec<Skipped>,
    /// How many points the `maxDeletionsPerRun` ceiling held back
    /// (`held_back.len()`), so a console can say "50 of 380".
    pub truncated_by_cap: i64,
    /// What every point at this destination that is not a candidate still
    /// names: its backup set and the object keys the view knows for it.
    ///
    /// The input of [`plan_document`]'s own rail, which refuses a line whose set
    /// a retained point still names. Derived from the evaluation's output,
    /// independently of step 4's grouping, so it catches a regression there or
    /// in the ceiling; it reads the same `PointFacts`, so it does not defend
    /// against wrong view data.
    pub retained: RetainedObjects,
}

impl Evaluation {
    /// Whether every point considered is in exactly one of `kept`,
    /// `candidates`, `held_back` and `skipped`, and `truncated_by_cap` is the
    /// held-back count. `true` for every value [`evaluate`] returns.
    #[must_use]
    pub fn accounts_for_every_point(&self) -> bool {
        let count = |n: usize| i64::try_from(n).unwrap_or(i64::MAX);
        self.truncated_by_cap == count(self.held_back.len())
            && self.points_evaluated
                == count(self.kept.len())
                    .saturating_add(count(self.candidates.len()))
                    .saturating_add(count(self.held_back.len()))
                    .saturating_add(count(self.skipped.len()))
    }
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

    /// The group of a point from the slice [`Groups::of`] was built from. A
    /// point that is not (impossible: every verdict is built from that slice)
    /// falls into one shared group past every real root, the conservative
    /// direction, since merging groups can only protect more.
    fn of_point(&self, point: &PointFacts) -> usize {
        let addr = std::ptr::from_ref(point) as usize;
        self.root_of.get(&addr).copied().unwrap_or(usize::MAX)
    }
}

/// A point proved to be held at the destination under evaluation: "we checked
/// the location" is a fact the compiler carries. The one constructor performs
/// the check.
#[derive(Debug, Clone, Copy)]
struct Located<'a> {
    point: &'a PointFacts,
}

impl<'a> Located<'a> {
    /// `Some` only when `point.locations` names `location_id`. A point with no
    /// `locations[]` at all is refused too: its location could not be
    /// established, and "could not tell" never authorises a delete.
    fn at(point: &'a PointFacts, location_id: &str) -> Option<Self> {
        point
            .locations
            .iter()
            .any(|l| l == location_id)
            .then_some(Self { point })
    }
}

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
///    `maxDeletionsPerRun`. What the ceiling leaves out is `held_back`:
///    counted, named, and never reported as kept.
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
        // The floor first, not as a tie-break (D3 §6.4 step 3: "always the
        // newest `minUsablePoints` usable points, whatever the rules say"). A
        // policy that would leave fewer is reported, not obeyed, so the reason
        // is `MinUsablePoints` and the console can say which points it saved.
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
        // Nor a point whose set id names no single directory: an empty id is
        // the bound `<scope>//`, and an id with a `/` contains other sets (set
        // `a`'s enumeration would remove `a/b`). Kept `Unknown` rather than left
        // to refuse the whole plan at the writer.
        if point.backup_id.is_empty() || point.backup_id.contains('/') {
            verdicts.push((*located, Verdict::Protected(ProtectReason::Unknown)));
            continue;
        }
        // Nor a point whose set id or manifest key the catalog published as the
        // redactor's output: `[redacted]` names no directory, and one such
        // candidate makes `validate_key` refuse the whole plan, blocking every
        // other set's expiry. "Could not tell" is `Unknown`.
        if point.backup_id.contains(REDACTION_MARKER)
            || point
                .manifest_key
                .as_deref()
                .is_some_and(|m| m.contains(REDACTION_MARKER))
        {
            verdicts.push((*located, Verdict::Protected(ProtectReason::Unknown)));
            continue;
        }

        // Within the rules is kept; outside either rule is a candidate, with
        // `OlderThanKeepDays` taking precedence over `BeyondKeepLast`.
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

    // 4. Shared sets and shared segments. Points are grouped by what their
    //    deletion would remove: the same backup set (`backup_id`), the same
    //    manifest key, or a common segment key, transitively. A group holding
    //    any retained point (kept, protected or skipped) deletes none of its
    //    candidates: each is protected `SharedSegment`. v1 never partially
    //    deletes a shared set.
    //
    //    The set, not only the segments: the view carries no segment keys, and
    //    two receipts can name one set (a runner Job re-created from its frozen
    //    inputs rewrites `<scope>/<backup_id>/` and signs a second receipt).
    //    Every key a plan line may delete is bounded by `<scope>/<backup_id>/`,
    //    so the set is exactly the unit a deletion removes. A row whose location
    //    the catalog could not establish is retained too: "could not tell where
    //    it lives" must not authorise deleting what it names.
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

    // 5. Project. `kept` and `candidates` stay newest first; `protected` is
    //    sorted by id so the status block is stable. The ceiling applies per
    //    group: every group still holding a candidate holds only candidates
    //    (step 4), and a group is selected whole or held back whole, or
    //    deleting one receipt of a shared set would remove what a kept sibling
    //    names.
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
                    // Over the ceiling is held back: counted, named, never
                    // `kept`. It stays in the archive after this run (so it is
                    // in `retained`), but the rules would remove it and nothing
                    // protects it.
                    out.held_back.push(point.point_id.clone());
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
    // What stays, as the objects it names: the input of `plan_document`'s rail,
    // derived from the output rather than from the grouping above.
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
    out.truncated_by_cap = i64::try_from(out.held_back.len()).unwrap_or(i64::MAX);
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
    // A verdict the controller reached outranks the row: the row may predate
    // the refusal, and the controller's `Invalid`/`Untrusted` on this point's
    // own `Backup` is newer information about the same receipt bytes.
    if point.refused_by_controller {
        return Some(SkipReason::Unreadable);
    }
    match point.verification {
        Verification::Verified | Verification::VerifiedHistorical => None,
        _ => Some(SkipReason::Unreadable),
    }
}

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

/// The per-key rule: a key must start with `<scope_prefix>/<backup_id>/` and
/// must not start with `logweir/`. The evidence check is independent of the
/// prefix check: the CRD's K4 rule refuses a scope prefix under `logweir/`, but
/// this is also the worker's last rail and must not depend on admission.
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
    /// The `metadata.generation` this plan was computed from. Not in the
    /// digested bytes: approving a plan is a spec patch, which bumps the
    /// generation, and a digest the approval changes can never be approved.
    /// Published on `status.lastEvaluation` and in the enforcement record.
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
    /// `true` for every plan this build writes, a recorded deviation from D3
    /// §6.4 step 6's "complete, explicit list of object keys": the catalog view
    /// carries a manifest key and no segment list, and this controller holds no
    /// archive credential to look. So the approved document fixes the points,
    /// the manifest keys and the key bound, and the worker re-validates every
    /// enumerated key against that bound before deleting it (D3 §6.5's
    /// wrong-prefix rule). It becomes `false` when a view entry carries its
    /// segment keys.
    pub enumerate_set: bool,
    /// Every key this plan could name, manifest first — never a glob.
    pub object_keys: Vec<String>,
    /// Other points whose receipts name this same set and are removed with it:
    /// one line per set, since two lines over one manifest key are refused by
    /// the worker as `DuplicateKey` for the whole plan. Each point still gets
    /// its own outcome and tombstones in the record. Skipped when empty, so a
    /// plan with no shared set keeps the bytes (and approved digest) it had
    /// before this field.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub co_point_ids: Vec<String>,
}

/// The plan document — D3 §6.4 step 6.
///
/// Serialised with `logweir_core::det_json::to_deterministic_json`, so the
/// bytes are a pure function of the fields in declaration order and
/// `planSha256` is reproducible by anyone holding the same inputs.
///
/// What is deliberately not in here: `evaluated_at`, `policy_generation` and
/// `points_evaluated`. Each moves without what would be deleted moving (a
/// requeue, the approval's own spec patch, a newly unreadable point), and a
/// digest that changes on its own can never be approved. All three are on
/// `status.lastEvaluation` and in the enforcement record.
///
/// What invalidates an approval is content: a rules edit, a `holds` edit or a
/// new backup changes the rules as applied or the lines, and `policy_uid`,
/// `location_id` and `scope_prefix` pin where. The digest covers exactly "these
/// keys, at this location, under these rules".
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
    /// retains still names. The plan is not written at all.
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
/// Every key is validated before a byte is written: a plan with one
/// out-of-scope key is not a plan, and this returns `Err` rather than a
/// document an administrator could approve. It takes no clock: an instant in
/// the digested bytes would make the approval gate unreachable.
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
        // The manifest first, in execution order: a run interrupted between the
        // manifest and the segments leaves a set that cannot look usable.
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
        // The writer's own rail: the worker may remove everything under
        // `set_prefix`, so no retained point may name this set or any key under
        // it. Checked against what the evaluation retained, not the grouping,
        // so a regression in `evaluate` step 4 is a refused plan.
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
        // One line per set: a candidate naming the same set and manifest as a
        // line already written joins it as a co-point, since two lines over one
        // manifest are a plan the worker refuses whole.
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

/// The plan `ConfigMap`'s name, the one implementation the controller calls:
/// `<stem>-plan-<12 hex of planSha256>`, the stem being [`config_map_stem`] of
/// the policy's UID.
///
/// Content-named, because a generation-named object collides when the archive
/// moves under an unchanged spec, and UID-stemmed, because a policy name may
/// already use all 253 characters. The `ConfigMap` is immutable and owned by
/// the policy, so a second pass at the same digest gets 409 rather than
/// rewriting bytes under a reader. The evaluation publishes the name in
/// `status.lastEvaluation.planRef` beside `planSha256`, but the `ConfigMap` is
/// created only by the pass that starts a run, so in `Report` mode the ref
/// names an object that does not exist yet: the documented absent-object
/// behaviour, exactly as true as the digest beside it.
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
