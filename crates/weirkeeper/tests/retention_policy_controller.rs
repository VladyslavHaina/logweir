//! `RetentionPolicy`: what would be removed, and the four gates between a rule
//! and a removed object — D3 §6.3, §6.4, §6.5, PLAT-16.1 / PLAT-16.2.
//!
//! EVERY TEST HERE IS A PURE-FUNCTION TEST, A `mock_client` TEST OR A
//! SOURCE/GRAPH-READING TEST. Nothing dials a socket, nothing deletes anything
//! and nothing runs `kubectl`. The double PANICS on a request it was not given
//! a route for, which is what makes a ZERO COUNT — no `POST` on a Job when a
//! plan is unapproved, no `DELETE` anywhere ever — mean "the reconciler did not
//! ask" rather than "the table forgot a route".
//!
//! READ `a_point_at_another_destination_is_never_a_candidate` AND
//! `the_newest_selectable_point_is_never_a_candidate` FIRST. They are the two
//! properties this whole module exists for: retention evaluates the destination
//! it is FOR and no other (defect RET-WRONGBUCKET), and a policy that would
//! empty an archive is reported rather than obeyed.
//!
//! **The live proof is owed to W14** (D3 §15 L9 and L11). Nothing here deletes
//! a real object; the worker's own behaviour is
//! `crates/logweir-reaper/tests/reaper.rs`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use chrono::{DateTime, TimeZone as _, Utc};
use serde_json::{json, Value};

use weirkeeper::catalog_view::{Availability, Verification};
use weirkeeper::check;
use weirkeeper::controllers::retention_policy as ctrl;
use weirkeeper::crds::retention_policy::RetentionPolicy;
use weirkeeper::job::RunnerImage;
use weirkeeper::retention_plan as plan;
use weirkeeper::testing::{mock_client_recording_bodies, Recorder, Route, SeenBody};

// ===========================================================================
// Fixtures
// ===========================================================================

/// This task's namespace (STANDING RULE 13).
const NS: &str = "logweir-d3w9";
const NAME: &str = "primary";
const UID: &str = "16161616-0000-4000-8000-000000000016";
const DEST: &str = "archive";
const DEST_UID: &str = "d0d0d0d0-0000-4000-8000-0000000000d1";
const CATALOG: &str = "primary";
const SCOPE: &str = "team-a";
const LOCATION: &str = "s3://lw-archive/team-a";

const DAY_MS: i64 = 86_400_000;

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 17, 4, 17, 0)
        .single()
        .expect("a real instant")
}

fn now_ms() -> i64 {
    now().timestamp_millis()
}

// ---------------------------------------------------------------------------
// The pure layer's fixtures
// ---------------------------------------------------------------------------

fn point(id: &str, age_days: i64) -> plan::PointFacts {
    plan::PointFacts {
        point_id: id.to_string(),
        backup_id: format!("set-{id}"),
        recovery_point_at_ms: now_ms() - age_days * DAY_MS,
        locations: vec![LOCATION.to_string()],
        availability: Availability::Available,
        verification: Verification::Verified,
        manifest_key: Some(format!("{SCOPE}/set-{id}/manifest.json")),
        segment_keys: Vec::new(),
        bytes: Some(1024),
    }
}

fn destination() -> plan::Destination {
    plan::Destination {
        location_id: LOCATION.to_string(),
        scope_prefix: SCOPE.to_string(),
    }
}

fn rules(keep_last: Option<i64>, keep_days: Option<i64>, floor: i64) -> plan::Rules {
    plan::Rules {
        keep_last,
        keep_days,
        min_usable_points: floor,
    }
}

fn evaluate(points: &[plan::PointFacts], rules: plan::Rules) -> plan::Evaluation {
    evaluate_with(points, rules, &plan::Protection::default(), &[])
}

fn evaluate_with(
    points: &[plan::PointFacts],
    rules: plan::Rules,
    protection: &plan::Protection,
    holds: &[plan::Hold],
) -> plan::Evaluation {
    plan::evaluate(&plan::Input {
        destination: &destination(),
        points,
        rules,
        holds,
        protection,
        now: now(),
        max_deletions_per_run: 50,
    })
}

fn candidate_ids(evaluation: &plan::Evaluation) -> Vec<&str> {
    evaluation
        .candidates
        .iter()
        .map(|c| c.point_id.as_str())
        .collect()
}

fn protected_reason<'a>(evaluation: &'a plan::Evaluation, point: &str) -> Option<&'a str> {
    evaluation
        .protected
        .iter()
        .find(|p| p.point_id == point)
        .map(|p| p.reason.as_str())
}

// ===========================================================================
// D3 §6.4 step 1 — unknown is retained
// ===========================================================================

/// A point the catalog could not read is SKIPPED and is never a candidate,
/// whatever the rules say.
///
/// The tracker row is "unreadable manifest". The property is wider: every
/// availability that is not `Available` and every verification that is not
/// `Verified`/`VerifiedHistorical` lands in `skipped`, which
/// `candidates` is computed as a subset of `usable` and therefore cannot reach.
#[test]
fn nothing_the_catalog_could_not_establish_is_ever_a_candidate() {
    let cases: [(Availability, Verification, &str); 9] = [
        (
            Availability::Unreadable,
            Verification::Verified,
            "Unreadable",
        ),
        (Availability::Missing, Verification::Verified, "Unreadable"),
        (Availability::Partial, Verification::Verified, "Unreadable"),
        (Availability::Conflict, Verification::Verified, "Conflict"),
        (
            Availability::UnsupportedFormat,
            Verification::Verified,
            "UnsupportedFormat",
        ),
        (
            Availability::Deleted,
            Verification::Verified,
            "AlreadyDeleted",
        ),
        (
            Availability::Available,
            Verification::UntrustedSigner,
            "Unreadable",
        ),
        (Availability::Available, Verification::Revoked, "Unreadable"),
        (
            Availability::Available,
            Verification::NotAttempted,
            "Unreadable",
        ),
    ];
    for (availability, verification, reason) in cases {
        let mut old = point("old", 400);
        old.availability = availability;
        old.verification = verification;
        // Three usable newer points so the floor is not what saves it.
        let points = vec![point("n1", 1), point("n2", 2), point("n3", 3), old.clone()];
        let evaluation = evaluate(&points, rules(Some(1), Some(30), 3));
        assert!(
            candidate_ids(&evaluation).is_empty(),
            "{availability:?}/{verification:?} must never be a deletion candidate: a retention \
             pass that cannot read the archive proposes nothing, which is the opposite of what a \
             timestamp-driven bucket lifecycle rule does. Candidates: {:?}",
            candidate_ids(&evaluation)
        );
        assert_eq!(
            evaluation
                .skipped
                .iter()
                .find(|s| s.point_id == "old")
                .map(|s| s.reason.as_str()),
            Some(reason),
            "{availability:?}/{verification:?} is skipped with its own reason, not folded into \
             one bucket"
        );
    }
}

/// `VerifiedHistorical` IS usable: a retired key's signature still verifies
/// within its validity, and D3 §5.4 says the point stays selectable.
#[test]
fn a_historically_verified_point_is_usable_and_can_be_a_candidate() {
    let mut old = point("old", 400);
    old.verification = Verification::VerifiedHistorical;
    let points = vec![point("n1", 1), point("n2", 2), point("n3", 3), old];
    let evaluation = evaluate(&points, rules(Some(3), None, 3));
    assert_eq!(candidate_ids(&evaluation), vec!["old"]);
}

// ===========================================================================
// RET-WRONGBUCKET — the wrong-destination rail
// ===========================================================================

/// A point frozen against destination A is never counted against B.
///
/// Not "not counted" by an `if`: `retention_plan::Located` is a private newtype
/// whose only constructor checks `locations[]`, and `kept`, `candidates` and
/// `protected` are built from `Located` values alone. Deleting the membership
/// check makes this row fail, and there is no other way to reach the candidate
/// list.
#[test]
fn a_point_at_another_destination_is_never_a_candidate() {
    let mut elsewhere = point("elsewhere", 400);
    elsewhere.locations = vec!["s3://other-bucket/team-b".to_string()];
    let points = vec![point("n1", 1), point("n2", 2), point("n3", 3), elsewhere];
    let evaluation = evaluate(&points, rules(Some(1), Some(1), 3));

    assert!(
        candidate_ids(&evaluation).is_empty(),
        "the defect RET-WRONGBUCKET names is reporting one bucket's catalog against another's \
         URL; this is the constructive fix. Candidates: {:?}",
        candidate_ids(&evaluation)
    );
    assert_eq!(
        evaluation.points_evaluated, 3,
        "a point at another location is not even COUNTED here: `pointsEvaluated` is the number \
         at THIS destination, so a console cannot read another tenant's total as its own"
    );
    assert!(
        !evaluation.kept.iter().any(|k| k == "elsewhere")
            && !evaluation.skipped.iter().any(|s| s.point_id == "elsewhere"),
        "and it appears in no list at all"
    );
}

/// A point held at BOTH destinations is evaluated here — the merge rule is
/// "best of its locations" (D3 §5.4 as amended), and one of them is this one.
#[test]
fn a_point_held_here_and_elsewhere_is_evaluated_here() {
    let mut both = point("both", 400);
    both.locations = vec!["s3://other-bucket/team-b".to_string(), LOCATION.to_string()];
    let points = vec![point("n1", 1), point("n2", 2), point("n3", 3), both];
    let evaluation = evaluate(&points, rules(Some(3), None, 3));
    assert_eq!(candidate_ids(&evaluation), vec!["both"]);
}

/// A point whose `locations[]` is EMPTY is not evaluated either: the catalog
/// could not establish where it is, and "could not tell" never authorises a
/// delete.
#[test]
fn a_point_with_no_location_is_not_evaluated() {
    let mut nowhere = point("nowhere", 400);
    nowhere.locations.clear();
    let points = vec![point("n1", 1), point("n2", 2), point("n3", 3), nowhere];
    let evaluation = evaluate(&points, rules(Some(1), Some(1), 3));
    assert!(candidate_ids(&evaluation).is_empty());
    assert_eq!(evaluation.points_evaluated, 3);
}

/// Every key a plan names is bounded by `<scope>/<backupId>/`, and a plan that
/// would name one outside it is not written at all.
#[test]
fn a_plan_naming_a_key_outside_its_scope_is_refused_whole() {
    let mut rogue = point("rogue", 400);
    rogue.manifest_key = Some("kafka-backups/team-b/set-rogue/manifest.json".to_string());
    let points = vec![point("n1", 1), point("n2", 2), point("n3", 3), rogue];
    let evaluation = evaluate(&points, rules(Some(3), None, 3));
    assert_eq!(candidate_ids(&evaluation), vec!["rogue"]);

    let err = plan::plan_document(
        &identity(),
        &destination(),
        rules(Some(3), None, 3),
        &evaluation,
        now(),
    )
    .expect_err("a plan that would name a key outside its scope is not a plan");
    assert!(
        matches!(err, plan::PlanError::Scope(_)),
        "a plan containing one out-of-scope key is not a plan with one bad line; it is not a \
         plan, and nothing an administrator could approve is emitted. Got: {err:?}"
    );
}

/// And a key under `logweir/` is refused by that name.
#[test]
fn a_plan_naming_the_evidence_root_is_refused_by_that_name() {
    assert!(matches!(
        plan::validate_key("logweir/backups/x/manifest.json", SCOPE, "set-x"),
        Err(plan::ScopeViolation::EvidenceRoot { .. })
    ));
    assert!(matches!(
        plan::validate_key("other/set-x/manifest.json", SCOPE, "set-x"),
        Err(plan::ScopeViolation::OutsideScope { .. })
    ));
    assert!(plan::validate_key(&format!("{SCOPE}/set-x/manifest.json"), SCOPE, "set-x").is_ok());
}

// ===========================================================================
// D3 §6.4 step 3 — the rules, and the floor that beats them
// ===========================================================================

/// The newest selectable point is never a candidate.
#[test]
fn the_newest_selectable_point_is_never_a_candidate() {
    // A policy whose rules want EVERY point gone: each is older than
    // `keepDays: 1`, and the floor is at its schema minimum of 1.
    let points: Vec<plan::PointFacts> = (2..=6).map(|d| point(&format!("p{d}"), d)).collect();
    let evaluation = evaluate(&points, rules(None, Some(1), 1));
    assert!(
        !candidate_ids(&evaluation).contains(&"p2"),
        "p2 is the newest usable point and is kept whatever the rules say. Candidates: {:?}",
        candidate_ids(&evaluation)
    );
    assert_eq!(
        candidate_ids(&evaluation),
        vec!["p3", "p4", "p5", "p6"],
        "everything else the rules wanted IS a candidate, so the floor is not simply refusing          to evaluate"
    );
    assert_eq!(
        protected_reason(&evaluation, "p2"),
        Some("MinUsablePoints"),
        "and it is REPORTED as the floor's doing, so an operator can see which points the rules \
         wanted and the floor saved"
    );
}

/// A policy that would empty the archive deletes nothing — the extreme of the
/// row above, with `minUsablePoints` at its schema floor of 1.
#[test]
fn a_policy_that_would_empty_the_archive_deletes_nothing_it_should_not() {
    let points = vec![point("only", 4000)];
    let evaluation = evaluate(&points, rules(None, Some(1), 1));
    assert!(candidate_ids(&evaluation).is_empty());
    assert_eq!(
        protected_reason(&evaluation, "only"),
        Some("MinUsablePoints")
    );
}

/// `minUsablePoints` overrides BOTH rules, and the override is reported rather
/// than obeyed.
#[test]
fn min_usable_points_overrides_both_rules() {
    // Six points, `keepLast: 2`, `minUsablePoints: 3` — D3 §15 L9's numbers.
    let points: Vec<plan::PointFacts> = (1..=6).map(|d| point(&format!("p{d}"), d)).collect();
    let evaluation = evaluate(&points, rules(Some(2), None, 3));
    assert_eq!(
        candidate_ids(&evaluation),
        vec!["p4", "p5", "p6"],
        "exactly three candidates — L9's own assertion"
    );
    assert_eq!(
        protected_reason(&evaluation, "p3"),
        Some("MinUsablePoints"),
        "p3 is beyond keepLast: 2 and is kept by the floor, and `protected` says so"
    );
    assert_eq!(
        protected_reason(&evaluation, "p1"),
        None,
        "p1 and p2 are inside keepLast, so the floor did not have to save them and they are \
         plain `kept` rather than `protected` — a console that showed three MinUsablePoints \
         overrides would be overstating what the floor did"
    );
}

/// `keepLast` and `keepDays` are a UNION, with `OlderThanKeepDays` taking
/// precedence in the reason — the existing rule, preserved.
#[test]
fn overlapping_keep_rules_are_a_union_with_the_age_reason_winning() {
    let points = vec![
        point("p1", 1),
        point("p2", 2),
        point("p3", 3),
        point("p4", 40),
        point("p5", 50),
    ];
    let evaluation = evaluate(&points, rules(Some(4), Some(30), 3));
    assert_eq!(
        candidate_ids(&evaluation),
        vec!["p4", "p5"],
        "p4 is inside keepLast: 4 by rank and outside keepDays: 30 by age; the union removes it"
    );
    assert_eq!(
        evaluation.candidates[0].reason,
        plan::CandidateReason::OlderThanKeepDays,
        "and the AGE reason wins over the rank one, which is the existing rule"
    );
    assert_eq!(
        evaluation.candidates[1].reason,
        plan::CandidateReason::OlderThanKeepDays
    );
}

/// `keepLast` alone gives the rank reason.
#[test]
fn beyond_keep_last_is_named_by_rank_when_age_is_not_the_cause() {
    let points: Vec<plan::PointFacts> = (1..=5).map(|d| point(&format!("p{d}"), d)).collect();
    let evaluation = evaluate(&points, rules(Some(3), None, 3));
    assert_eq!(candidate_ids(&evaluation), vec!["p4", "p5"]);
    assert!(evaluation
        .candidates
        .iter()
        .all(|c| c.reason == plan::CandidateReason::BeyondKeepLast));
}

/// No rule at all removes nothing.
#[test]
fn no_rule_removes_nothing() {
    let points: Vec<plan::PointFacts> = (1..=5).map(|d| point(&format!("p{d}"), d)).collect();
    let evaluation = evaluate(&points, rules(None, None, 3));
    assert!(candidate_ids(&evaluation).is_empty());
    assert_eq!(evaluation.kept.len(), 5);
}

/// The order is total: the plan bytes are a function of the INPUTS and not of
/// the input order.
#[test]
fn the_evaluation_is_independent_of_the_input_order() {
    let points: Vec<plan::PointFacts> = (1..=6).map(|d| point(&format!("p{d}"), d)).collect();
    let forward = evaluate(&points, rules(Some(2), None, 3));
    let mut reversed = points.clone();
    reversed.reverse();
    let backward = evaluate(&reversed, rules(Some(2), None, 3));
    assert_eq!(forward, backward);
}

/// Two points captured in the same millisecond tie-break on the point id, so
/// the order is still total.
#[test]
fn a_tie_on_the_capture_instant_breaks_on_the_point_id() {
    let mut a = point("aaa", 5);
    let mut b = point("bbb", 5);
    a.recovery_point_at_ms = 1_000;
    b.recovery_point_at_ms = 1_000;
    let points = vec![b, a, point("p1", 1), point("p2", 2)];
    let evaluation = evaluate(&points, rules(Some(3), None, 1));
    assert_eq!(
        candidate_ids(&evaluation),
        vec!["bbb"],
        "`aaa` sorts before `bbb` at an equal instant, so `bbb` is the older of the two"
    );
}

// ===========================================================================
// D3 §6.4 step 4 — the protections
// ===========================================================================

/// A nonterminal restore's point is protected and named.
#[test]
fn an_active_restore_protects_its_point() {
    let points: Vec<plan::PointFacts> = (1..=6).map(|d| point(&format!("p{d}"), d)).collect();
    let protection = plan::Protection {
        active_restore: BTreeSet::from(["p6".to_string()]),
        refused: BTreeMap::new(),
    };
    let evaluation = evaluate_with(&points, rules(Some(2), None, 3), &protection, &[]);
    assert!(!candidate_ids(&evaluation).contains(&"p6"));
    assert_eq!(protected_reason(&evaluation, "p6"), Some("ActiveRestore"));
}

/// A `spec.holds[]` entry still in force protects; one that has lapsed does
/// not.
#[test]
fn a_hold_protects_until_it_lapses() {
    let points: Vec<plan::PointFacts> = (1..=6).map(|d| point(&format!("p{d}"), d)).collect();
    let live = plan::Hold {
        point_id: "p6".to_string(),
        reason: "legal-2026-11".to_string(),
        until: Some(now() + chrono::Duration::days(30)),
    };
    let evaluation = evaluate_with(
        &points,
        rules(Some(2), None, 3),
        &plan::Protection::default(),
        &[live],
    );
    assert_eq!(protected_reason(&evaluation, "p6"), Some("Hold"));

    let lapsed = plan::Hold {
        point_id: "p6".to_string(),
        reason: "legal-2026-11".to_string(),
        until: Some(now() - chrono::Duration::days(1)),
    };
    let after = evaluate_with(
        &points,
        rules(Some(2), None, 3),
        &plan::Protection::default(),
        &[lapsed],
    );
    assert!(
        candidate_ids(&after).contains(&"p6"),
        "a hold with an `until` in the past is not in force, and reporting it as one would keep \
         an archive forever on the strength of a spent case reference"
    );
}

/// A hold with no `until` never lapses.
#[test]
fn a_hold_with_no_expiry_never_lapses() {
    let points: Vec<plan::PointFacts> = (1..=6).map(|d| point(&format!("p{d}"), d)).collect();
    let forever = plan::Hold {
        point_id: "p6".to_string(),
        reason: "indefinite".to_string(),
        until: None,
    };
    let evaluation = evaluate_with(
        &points,
        rules(Some(2), None, 3),
        &plan::Protection::default(),
        &[forever],
    );
    assert_eq!(protected_reason(&evaluation, "p6"), Some("Hold"));
}

/// A provider refusal recorded on the last run keeps the point and excludes it
/// from the next plan.
#[test]
fn a_provider_refusal_keeps_the_point_out_of_the_next_plan() {
    let points: Vec<plan::PointFacts> = (1..=6).map(|d| point(&format!("p{d}"), d)).collect();
    let protection = plan::Protection {
        active_restore: BTreeSet::new(),
        refused: BTreeMap::from([("p6".to_string(), "Locked".to_string())]),
    };
    let evaluation = evaluate_with(&points, rules(Some(2), None, 3), &protection, &[]);
    assert!(!candidate_ids(&evaluation).contains(&"p6"));
    assert_eq!(
        protected_reason(&evaluation, "p6"),
        Some("LegalHold"),
        "`object_store` exposes no WORM readback, so this means exactly 'a provider refusal is \
         authoritative and recorded' — never 'Logweir knows the hold exists'"
    );
}

/// A segment another RETAINED point's manifest names protects the candidate
/// that shares it: v1 never partially deletes a shared set.
#[test]
fn a_shared_segment_protects_the_candidate_that_shares_it() {
    let mut old = point("p6", 6);
    old.segment_keys = vec![format!("{SCOPE}/set-shared/seg-0")];
    let mut kept = point("p1", 1);
    kept.segment_keys = vec![format!("{SCOPE}/set-shared/seg-0")];
    let points = vec![kept, point("p2", 2), point("p3", 3), old];
    let evaluation = evaluate(&points, rules(Some(3), None, 3));
    assert!(!candidate_ids(&evaluation).contains(&"p6"));
    assert_eq!(protected_reason(&evaluation, "p6"), Some("SharedSegment"));
}

/// Mutant for the row above: two candidates sharing a segment with EACH OTHER
/// and with nothing retained are both removed — the rule is about segments a
/// RETAINED point still needs, not about sharing in general.
#[test]
fn two_candidates_sharing_only_with_each_other_are_both_removed() {
    let mut a = point("p5", 5);
    let mut b = point("p6", 6);
    a.segment_keys = vec![format!("{SCOPE}/set-pair/seg-0")];
    b.segment_keys = vec![format!("{SCOPE}/set-pair/seg-0")];
    let points = vec![point("p1", 1), point("p2", 2), point("p3", 3), a, b];
    let evaluation = evaluate(&points, rules(Some(3), None, 3));
    assert_eq!(candidate_ids(&evaluation), vec!["p5", "p6"]);
}

/// A point whose manifest key the catalog never established cannot be planned:
/// the execution order starts by deleting the manifest.
#[test]
fn a_point_with_no_manifest_key_is_protected_as_unknown() {
    let mut headless = point("p6", 6);
    headless.manifest_key = None;
    let points = vec![point("p1", 1), point("p2", 2), point("p3", 3), headless];
    let evaluation = evaluate(&points, rules(Some(3), None, 3));
    assert!(candidate_ids(&evaluation).is_empty());
    assert_eq!(protected_reason(&evaluation, "p6"), Some("Unknown"));
}

/// The per-run point ceiling truncates the candidate list AND says how many it
/// left behind.
#[test]
fn the_deletion_ceiling_truncates_and_counts_what_it_left() {
    let points: Vec<plan::PointFacts> = (1..=10).map(|d| point(&format!("p{d:02}"), d)).collect();
    let evaluation = plan::evaluate(&plan::Input {
        destination: &destination(),
        points: &points,
        rules: rules(Some(3), None, 3),
        holds: &[],
        protection: &plan::Protection::default(),
        now: now(),
        max_deletions_per_run: 2,
    });
    assert_eq!(evaluation.candidates.len(), 2);
    assert_eq!(
        evaluation.truncated_by_cap, 5,
        "seven points are beyond keepLast: 3, two fit the ceiling and five are counted — a \
         console that showed 2 of 7 without saying so would read as '2 is all there is'"
    );
    assert!(
        evaluation.kept.iter().any(|k| k == "p10"),
        "and the ones over the ceiling are KEPT, never silently dropped"
    );
}

// ===========================================================================
// The plan document — D3 §6.4 step 6
// ===========================================================================

fn identity() -> plan::PlanIdentity {
    plan::PlanIdentity {
        namespace: NS.to_string(),
        name: NAME.to_string(),
        uid: UID.to_string(),
        generation: 4,
    }
}

fn plan_for(points: &[plan::PointFacts], r: plan::Rules) -> (plan::PlanDocument, String) {
    let evaluation = evaluate(points, r);
    let document = plan::plan_document(&identity(), &destination(), r, &evaluation, now())
        .expect("the plan renders");
    let (_, digest) = plan::plan_bytes(&document).expect("the plan serialises");
    (document, digest)
}

/// The plan bytes and `planSha256` are a PURE FUNCTION of the inputs.
#[test]
fn the_plan_digest_is_a_pure_function_of_its_inputs() {
    let points: Vec<plan::PointFacts> = (1..=6).map(|d| point(&format!("p{d}"), d)).collect();
    let (_, a) = plan_for(&points, rules(Some(2), None, 3));
    let (_, b) = plan_for(&points, rules(Some(2), None, 3));
    assert_eq!(a, b, "two renderings of one evaluation digest identically");
    assert!(a.starts_with("sha256:"));
}

/// Changing the rules changes the plan, which changes the digest — which is how
/// a policy edit invalidates an approval without anything having to remember
/// to.
#[test]
fn changing_the_rules_invalidates_the_approved_digest() {
    let points: Vec<plan::PointFacts> = (1..=6).map(|d| point(&format!("p{d}"), d)).collect();
    let (_, before) = plan_for(&points, rules(Some(2), None, 3));
    let (_, after) = plan_for(&points, rules(Some(4), None, 3));
    assert_ne!(before, after);
}

/// And changing the GENERATION alone changes it too: the generation is inside
/// the bytes, so any spec edit at all invalidates the approval.
#[test]
fn a_generation_bump_alone_invalidates_the_approved_digest() {
    let points: Vec<plan::PointFacts> = (1..=6).map(|d| point(&format!("p{d}"), d)).collect();
    let r = rules(Some(2), None, 3);
    let evaluation = evaluate(&points, r);
    let (_, at_four) = plan::plan_bytes(
        &plan::plan_document(&identity(), &destination(), r, &evaluation, now()).expect("renders"),
    )
    .expect("serialises");
    let mut five = identity();
    five.generation = 5;
    let (_, at_five) = plan::plan_bytes(
        &plan::plan_document(&five, &destination(), r, &evaluation, now()).expect("renders"),
    )
    .expect("serialises");
    assert_ne!(
        at_four, at_five,
        "`policy_generation` is inside the plan bytes precisely so that a spec change cannot \
         leave a stale approval looking current"
    );
}

/// The plan's first key on every line is the manifest, and every key is under
/// the line's own set prefix.
#[test]
fn every_plan_line_names_its_manifest_first_and_stays_in_its_set() {
    let points: Vec<plan::PointFacts> = (1..=6).map(|d| point(&format!("p{d}"), d)).collect();
    let (document, _) = plan_for(&points, rules(Some(2), None, 3));
    assert!(!document.lines.is_empty());
    for line in &document.lines {
        assert_eq!(line.object_keys.first(), Some(&line.manifest_key));
        assert_eq!(line.set_prefix, format!("{SCOPE}/{}/", line.backup_id));
        for key in &line.object_keys {
            assert!(key.starts_with(&line.set_prefix), "{key} escapes its set");
            assert!(!key.starts_with(plan::EVIDENCE_ROOT));
        }
    }
    assert_eq!(document.format, plan::PLAN_MEDIA_TYPE);
    assert_eq!(document.scope_prefix, SCOPE);
    assert_eq!(document.location_id, LOCATION);
}

/// The plan `ConfigMap` name is bounded even at a 253-character policy name.
#[test]
fn the_plan_config_map_name_is_bounded() {
    let long = "a".repeat(253);
    let name = plan::plan_config_map_name(&long, 7);
    assert!(
        name.len() <= 253,
        "a name longer than the budget is replaced by a digest of itself; got {} characters",
        name.len()
    );
    assert_eq!(plan::plan_config_map_name("primary", 7), "primary-plan-7");
}

/// The run id is deterministic, so a controller that crashed between the lease
/// and the Job computes the same id and gets a 409 rather than a second run.
#[test]
fn the_run_id_is_deterministic() {
    assert_eq!(
        plan::run_id(UID, "sha256:aa", 100),
        plan::run_id(UID, "sha256:aa", 100)
    );
    assert_ne!(
        plan::run_id(UID, "sha256:aa", 100),
        plan::run_id(UID, "sha256:bb", 100)
    );
    assert_ne!(
        plan::run_id(UID, "sha256:aa", 100),
        plan::run_id(UID, "sha256:aa", 101)
    );
}

/// The record key is under the evidence root, which the run's own credential
/// cannot delete from.
#[test]
fn the_record_key_is_under_the_evidence_root() {
    let key = plan::record_key(UID, "r00");
    assert!(key.starts_with(plan::EVIDENCE_ROOT));
    assert_eq!(key, format!("logweir/retention/{UID}/r00.json"));
}

// ===========================================================================
// The legacy report's honesty note — D3 §6.3, RET-WRONGBUCKET
// ===========================================================================

/// The controller's handle and the schedule's URL must be the same LOCATION.
#[test]
fn the_legacy_report_applies_only_at_the_same_location() {
    let same = [
        ("s3://b/p", "s3://b/p"),
        ("s3://b/p/", "s3://b/p"),
        ("s3://b/p", "s3://b/p/"),
    ];
    for (schedule, controller) in same {
        assert!(
            plan::legacy_report_applies(schedule, Some(controller)),
            "{schedule} and {controller} are one location"
        );
    }
    let different = [
        ("s3://b/p", "s3://b/q"),
        ("s3://b/p", "s3://c/p"),
        ("s3://b/p", "s3://b/pp"),
        ("s3://b/p", "file:///b/p"),
    ];
    for (schedule, controller) in different {
        assert!(
            !plan::legacy_report_applies(schedule, Some(controller)),
            "{schedule} and {controller} are two locations, and reporting one against the other \
             is defect RET-WRONGBUCKET"
        );
    }
}

/// An UNKNOWN controller location evaluates as before.
///
/// `Some(handle)` with `None` location cannot occur in the shipped binary —
/// `main` builds the handle only when the same variable this reads returned
/// `Some`, and process environment does not change under a running process — so
/// the only caller that can produce it is a test pairing an in-memory `Store`
/// with an unset `LOGWEIR_ARCHIVE_URL`. Answering "mismatch" there would replace
/// a report with a note about a mismatch nobody can be in. The rule is
/// therefore: evaluate, unless the two locations are both KNOWN and differ.
#[test]
fn an_unknown_controller_location_evaluates_as_before() {
    assert!(plan::legacy_report_applies("s3://b/p", None));
    assert!(plan::legacy_report_applies("s3://b/p", Some("")));
}

/// And the real mismatch, through the shipped reconciler: the report is
/// REPLACED by the note, with every set list empty.
///
/// **The `Store` and the runtime are built in this order on purpose.** `Store`
/// drives its own current-thread runtime, so constructing one inside an
/// `async` context — or dropping one there — panics (*Cannot drop a runtime in
/// a context where blocking is not allowed*). The handle is therefore built
/// first, on a thread driving nothing, exactly as `main` builds the real one,
/// and the reconcile runs inside a runtime this test owns.
#[test]
fn the_legacy_report_is_replaced_when_the_handle_is_elsewhere() {
    use weirkeeper::controllers::backup_schedule as schedule;
    use weirkeeper::crds::backup_schedule::BackupSchedule;

    let schedule_value = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupSchedule",
        "metadata": {
            "name": "nightly", "namespace": NS, "uid": "s1",
            "generation": 1, "resourceVersion": "7"
        },
        "spec": {
            "sourceRef": {"name": "src"},
            "schedule": "0 3 * * *",
            "topics": ["orders"],
            "archive": {"url": "s3://tenant-a/backups"},
            "retention": {"keepLast": 2},
            "suspend": true
        },
        "status": {}
    });
    let object: BackupSchedule =
        serde_json::from_value(schedule_value.clone()).expect("the fixture is a schedule");
    let routes = vec![
        route("GET", "/backups", empty_list("Backup")),
        route(
            "PATCH",
            "/backupschedules/nightly/status",
            schedule_value.to_string(),
        ),
    ];
    // OUTSIDE THE RUNTIME BUILT BELOW — see the note above.
    let store = std::sync::Arc::new(logweir_store::Store::in_memory("logweir/"));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the test runtime builds");
    let f = rt.block_on(async {
        let f = fixture(routes);
        schedule::reconcile_schedule_with_archive_at(
            &object,
            &f.client,
            Some(&store),
            // The controller's ONE handle is over a DIFFERENT bucket — which
            // since D1 W2 made `destinationRef` editable is reachable by an
            // edit between runs, not only by creating a schedule elsewhere.
            Some("s3://tenant-b/backups"),
            now(),
        )
        .await
        .expect("a suspended schedule still reports");
        f
    });

    let patch = f
        .bodies
        .lock()
        .expect("bodies")
        .iter()
        .find(|b| b.method == "PATCH" && b.uri.contains("/backupschedules/"))
        .map(|b| serde_json::from_str::<Value>(&b.body).expect("JSON"))
        .expect("a status patch");
    let report = &patch["status"]["retentionReport"];
    assert_eq!(
        report["note"],
        plan::LEGACY_DESTINATION_MISMATCH_NOTE,
        "the report is REPLACED, not corrected: {report}"
    );
    for empty in [
        "setsKept",
        "setsThatWouldBeRemoved",
        "awsCli",
        "mcCli",
        "skipped",
    ] {
        assert_eq!(
            report[empty],
            json!([]),
            "`{empty}` must be empty — \"not evaluated\" and \"nothing to remove\" are \
             different claims and the empty lists are what say so. Report: {report}"
        );
    }
    assert!(
        f.seen().iter().all(|(m, _)| m != "DELETE"),
        "and nothing was removed, here or anywhere"
    );
}

/// The note names the remedy.
#[test]
fn the_mismatch_note_names_the_remedy() {
    assert!(
        plan::LEGACY_DESTINATION_MISMATCH_NOTE.contains("RetentionPolicy"),
        "an operator reading the note has to be told what to do instead: {}",
        plan::LEGACY_DESTINATION_MISMATCH_NOTE
    );
    assert!(plan::LEGACY_DESTINATION_MISMATCH_NOTE.contains("different destination"));
}

// ===========================================================================
// The controller
// ===========================================================================

fn destination_body() -> String {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupDestination",
        "metadata": {
            "name": DEST, "namespace": NS, "uid": DEST_UID,
            "generation": 1, "resourceVersion": "77"
        },
        "spec": {
            "storage": {
                "provider": "S3", "bucket": "lw-archive", "prefix": SCOPE,
                "region": "us-east-1", "endpoint": "http://minio.storage.svc:9000",
                "addressing": "PathStyle"
            },
            "transport": {"security": "InsecureHTTP"},
            "access": {
                "archiveWrite": {"mode": "SecretKeys", "secret": {
                    "name": "lw-writer", "accessKeyIdKey": "id", "secretAccessKeyKey": "key"
                }},
                "archiveRead": {"mode": "SecretKeys", "secret": {
                    "name": "lw-reader", "accessKeyIdKey": "id", "secretAccessKeyKey": "key"
                }}
            }
        },
        "status": {
            "observedGeneration": 1, "reason": "Valid",
            "conditions": [{"type": "Valid", "status": "True", "reason": "Valid",
                            "observedGeneration": 1}]
        }
    })
    .to_string()
}

fn catalog_body(destination: Option<&str>, pages: Value) -> String {
    let mut spec = json!({
        "sync": {
            "intervalSeconds": 3600, "mode": "Index", "maxObjectsPerRun": 100000,
            "deepCheck": "ManifestDigest", "viewLimit": 2000
        }
    });
    if let Some(name) = destination {
        spec["destinationRef"] = json!({"name": name});
    }
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "RecoveryCatalog",
        "metadata": {
            "name": CATALOG, "namespace": NS, "uid": "c0", "generation": 1,
            "resourceVersion": "10"
        },
        "spec": spec,
        "status": {"pages": pages}
    })
    .to_string()
}

fn view_entry(id: &str, age_days: i64) -> Value {
    json!({
        "pointId": id,
        "backupId": format!("set-{id}"),
        "runId": "run-1",
        "recoveryPointAtMs": now_ms() - age_days * DAY_MS,
        "coveredFromMs": now_ms() - age_days * DAY_MS - 3_600_000,
        "coveredToMs": now_ms() - age_days * DAY_MS,
        "locations": [{"locationId": LOCATION, "availability": "Available"}],
        "receiptKey": format!("logweir/backups/set-{id}/r.receipt.json"),
        "receiptSha256": format!("sha256:{}", "0".repeat(64)),
        "manifestKey": format!("{SCOPE}/set-{id}/manifest.json"),
        "manifestSha256": format!("sha256:{}", "1".repeat(64)),
        "availability": "Available",
        "verification": "Verified",
        "signerKeyId": "aa",
        "selectable": true
    })
}

fn page_config_map(entries: &[Value]) -> String {
    let body: String = entries
        .iter()
        .map(|e| {
            format!(
                "{}\n",
                serde_json::to_string(e).expect("an entry serialises")
            )
        })
        .collect();
    json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": "page-0", "namespace": NS, "resourceVersion": "1"},
        "immutable": true,
        "data": {weirkeeper::catalog_view::PAGE_DATA_KEY: body}
    })
    .to_string()
}

fn six_points() -> Vec<Value> {
    (1..=6).map(|d| view_entry(&format!("p{d}"), d)).collect()
}

fn policy_value(spec_extra: Value, status: Value) -> Value {
    let mut spec = json!({
        "destinationRef": {"name": DEST},
        "catalogRef": {"name": CATALOG},
        "scope": {"prefix": SCOPE},
        "rules": {"keepLast": 2, "minUsablePoints": 3},
        "mode": "Report"
    });
    if let Some(extra) = spec_extra.as_object() {
        for (k, v) in extra {
            spec[k] = v.clone();
        }
    }
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "RetentionPolicy",
        "metadata": {
            "name": NAME, "namespace": NS, "uid": UID,
            "generation": 4, "resourceVersion": "4242"
        },
        "spec": spec,
        "status": status
    })
}

fn policy(spec_extra: Value, status: Value) -> RetentionPolicy {
    serde_json::from_value(policy_value(spec_extra, status)).expect("the fixture is a policy")
}

struct Fixture {
    client: kube::Client,
    recorder: Recorder,
    bodies: std::sync::Arc<std::sync::Mutex<Vec<SeenBody>>>,
}

impl Fixture {
    fn seen(&self) -> Vec<(String, String)> {
        self.recorder
            .lock()
            .expect("the recorder")
            .iter()
            .map(|r| (r.method.clone(), r.uri.clone()))
            .collect()
    }

    fn status_patches(&self) -> Vec<Value> {
        self.bodies
            .lock()
            .expect("the body recorder")
            .iter()
            .filter(|b| b.method == "PATCH" && b.uri.contains("/retentionpolicies/"))
            .map(|b| serde_json::from_str(&b.body).expect("JSON"))
            .collect()
    }

    /// The merged status, as an object that has seen every patch in order.
    fn status(&self) -> Value {
        let mut merged = json!({});
        for patch in self.status_patches() {
            if let Some(status) = patch.get("status") {
                weirkeeper::conditions::apply_merge_patch(&mut merged, status);
            }
        }
        merged
    }

    fn posted(&self, fragment: &str) -> Vec<Value> {
        self.bodies
            .lock()
            .expect("the body recorder")
            .iter()
            .filter(|b| b.method == "POST" && b.uri.contains(fragment))
            .map(|b| serde_json::from_str(&b.body).expect("JSON"))
            .collect()
    }

    fn condition(&self, r#type: &str) -> Value {
        self.status()
            .get("conditions")
            .and_then(|c| c.as_array())
            .and_then(|rows| {
                rows.iter()
                    .find(|c| c.get("type").and_then(Value::as_str) == Some(r#type))
                    .cloned()
            })
            .unwrap_or_else(|| {
                panic!(
                    "no `{}` condition was written; status: {}",
                    r#type,
                    self.status()
                )
            })
    }
}

fn route(method: &'static str, path_suffix: &'static str, body: String) -> Route {
    Route {
        method,
        path_suffix,
        status: 200,
        body,
    }
}

/// A `GET` on the plan `ConfigMap` that answers "it does not exist yet".
///
/// A real 404, not an empty body: a 200 with `{}` would deserialise as a
/// `ConfigMap` and the reconciler would read it as "the plan is already there"
/// and create nothing — which is the opposite of what these rows assert.
fn plan_config_map_route(digest: &str) -> Route {
    let name = format!(
        "{}-plan-{}",
        stem(),
        &digest.trim_start_matches("sha256:")[..12]
    );
    let suffix: &'static str = Box::leak(format!("/configmaps/{name}").into_boxed_str());
    Route {
        method: "GET",
        path_suffix: suffix,
        status: 404,
        body: json!({
            "kind": "Status", "apiVersion": "v1", "status": "Failure",
            "reason": "NotFound", "code": 404, "message": "configmaps not found"
        })
        .to_string(),
    }
}

fn empty_list(kind: &str) -> String {
    json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": format!("{kind}List"),
        "metadata": {"resourceVersion": "1"},
        "items": []
    })
    .to_string()
}

fn policy_list(items: Vec<Value>) -> String {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "RetentionPolicyList",
        "metadata": {"resourceVersion": "1"},
        "items": items
    })
    .to_string()
}

/// The happy route table: one policy, one destination, one catalog with one
/// page of six points, no restores.
fn happy_routes(entries: &[Value]) -> Vec<Route> {
    vec![
        route("GET", "/retentionpolicies", policy_list(vec![])),
        route("GET", "/backupdestinations/archive", destination_body()),
        route(
            "GET",
            "/recoverycatalogs/primary",
            catalog_body(
                Some(DEST),
                json!([{"configMapName": "page-0", "index": 0, "count": 6}]),
            ),
        ),
        route("GET", "/configmaps/page-0", page_config_map(entries)),
        route("GET", "/restores", empty_list("Restore")),
        route(
            "PATCH",
            "/retentionpolicies/primary/status",
            policy_value(json!({}), json!({})).to_string(),
        ),
    ]
}

fn fixture(routes: Vec<Route>) -> Fixture {
    let (client, recorder, bodies) = mock_client_recording_bodies(routes);
    Fixture {
        client,
        recorder,
        bodies,
    }
}

async fn run(fixture: &Fixture, policy: &RetentionPolicy) -> ctrl::Outcome {
    let installation = check::policy::Policy::defaults();
    let image = RunnerImage::default();
    ctrl::reconcile_policy(
        policy,
        &ctrl::PolicyContext {
            client: &fixture.client,
            policy: &installation,
            runner_image: &image,
            now: now(),
        },
    )
    .await
    .expect("the reconcile reaches a verdict")
}

// ---------------------------------------------------------------------------
// Report mode
// ---------------------------------------------------------------------------

/// A `Report` policy publishes what it would remove and creates NOTHING.
#[tokio::test]
async fn report_mode_publishes_an_evaluation_and_creates_nothing() {
    let f = fixture(happy_routes(&six_points()));
    let outcome = run(&f, &policy(json!({}), json!({}))).await;

    assert_eq!(outcome.phase, ctrl::RetentionPhase::Evaluated);
    assert_eq!(outcome.enforcement, ctrl::ENFORCEMENT_RECOMMENDATION_ONLY);
    assert_eq!(
        outcome.candidates, 3,
        "keepLast 2, minUsablePoints 3, 6 points"
    );
    assert_eq!(outcome.points_evaluated, 6);
    assert_eq!(outcome.deletes_performed, 0);

    let status = f.status();
    assert_eq!(status["enforcement"], ctrl::ENFORCEMENT_RECOMMENDATION_ONLY);
    assert_eq!(status["lastEvaluation"]["candidateCount"], 3);
    assert!(status["lastEvaluation"]["planSha256"]
        .as_str()
        .expect("a digest")
        .starts_with("sha256:"));
    assert_eq!(
        f.condition(ctrl::CONDITION_ENFORCED)["reason"],
        ctrl::REASON_RECOMMENDATION_ONLY
    );

    assert!(
        f.seen().iter().all(|(m, _)| m != "POST"),
        "nothing is created in Report mode. Requests: {:?}",
        f.seen()
    );
    assert!(
        f.seen().iter().all(|(m, _)| m != "DELETE"),
        "and nothing is deleted, ever, by this controller. Requests: {:?}",
        f.seen()
    );
}

/// The evaluation names only THIS destination's points, and a catalog covering
/// another one is refused with the field name.
#[tokio::test]
async fn a_catalog_covering_another_destination_is_refused() {
    let mut routes = happy_routes(&six_points());
    routes.retain(|r| r.path_suffix != "/recoverycatalogs/primary");
    routes.push(route(
        "GET",
        "/recoverycatalogs/primary",
        catalog_body(Some("somewhere-else"), json!([])),
    ));
    let f = fixture(routes);
    let outcome = run(&f, &policy(json!({}), json!({}))).await;

    assert_eq!(outcome.ready, "False");
    assert_eq!(outcome.ready_reason, ctrl::REASON_CATALOG_UNUSABLE);
    let message = f.condition(ctrl::CONDITION_EVALUATED)["message"]
        .as_str()
        .expect("a message")
        .to_string();
    assert!(
        message.contains("somewhere-else") && message.contains(DEST),
        "the refusal names BOTH destinations, which is what tells an operator which object to \
         fix: {message}"
    );
}

/// Two policies for one destination put both in `Ready=False/Conflict` and
/// NEITHER evaluates.
#[tokio::test]
async fn two_policies_for_one_destination_both_refuse_and_neither_evaluates() {
    let other = policy_value(json!({}), json!({}));
    let mut other = other;
    other["metadata"]["name"] = json!("second");
    other["metadata"]["uid"] = json!("99999999-0000-4000-8000-000000000099");
    let routes = vec![
        route("GET", "/retentionpolicies", policy_list(vec![other])),
        route(
            "PATCH",
            "/retentionpolicies/primary/status",
            policy_value(json!({}), json!({})).to_string(),
        ),
    ];
    let f = fixture(routes);
    let outcome = run(&f, &policy(json!({}), json!({}))).await;

    assert_eq!(outcome.phase, ctrl::RetentionPhase::Refused);
    assert_eq!(outcome.ready_reason, ctrl::REASON_CONFLICT);
    assert!(
        f.seen()
            .iter()
            .all(|(_, uri)| !uri.contains("/recoverycatalogs/")),
        "a conflicted policy does not so much as compute a plan: a plan an administrator could \
         approve is the thing that makes two contesting policies dangerous. Requests: {:?}",
        f.seen()
    );
    assert!(f.condition(ctrl::CONDITION_READY)["message"]
        .as_str()
        .expect("a message")
        .contains("second"));
}

/// A catalog that has published no view is `Evaluated=False` and nothing else —
/// a retention evaluation failure blocks no backup.
#[tokio::test]
async fn an_unreadable_view_is_evaluated_false_and_touches_nothing_else() {
    let mut routes = happy_routes(&six_points());
    routes.retain(|r| r.path_suffix != "/recoverycatalogs/primary");
    routes.push(route(
        "GET",
        "/recoverycatalogs/primary",
        catalog_body(Some(DEST), Value::Null),
    ));
    let f = fixture(routes);
    let outcome = run(&f, &policy(json!({}), json!({}))).await;

    assert_eq!(outcome.ready_reason, ctrl::REASON_CATALOG_UNUSABLE);
    assert_eq!(
        f.condition(ctrl::CONDITION_EVALUATED)["reason"],
        ctrl::REASON_VIEW_UNREADABLE
    );
    assert!(
        f.seen()
            .iter()
            .all(|(_, uri)| !uri.contains("/backups") && !uri.contains("/backupschedules")),
        "it is a different controller, a different object and a different condition; no Backup \
         is read and none is touched. Requests: {:?}",
        f.seen()
    );
    assert!(f.seen().iter().all(|(m, _)| m != "POST"));
}

/// A page whose bytes do not match the digest the catalog published is not
/// turned into a plan.
#[tokio::test]
async fn a_page_that_does_not_match_its_published_digest_is_refused() {
    let mut routes = happy_routes(&six_points());
    routes.retain(|r| r.path_suffix != "/recoverycatalogs/primary");
    routes.push(route(
        "GET",
        "/recoverycatalogs/primary",
        catalog_body(
            Some(DEST),
            json!([{
                "configMapName": "page-0", "index": 0, "count": 6,
                "sha256": format!("sha256:{}", "e".repeat(64))
            }]),
        ),
    ));
    let f = fixture(routes);
    let outcome = run(&f, &policy(json!({}), json!({}))).await;
    assert_eq!(outcome.ready_reason, ctrl::REASON_CATALOG_UNUSABLE);
    assert!(f.condition(ctrl::CONDITION_EVALUATED)["message"]
        .as_str()
        .expect("a message")
        .contains("digests to"));
}

/// An entry this build cannot read refuses the whole view: a view it cannot
/// fully read never authorises a deletion.
#[tokio::test]
async fn an_unreadable_entry_refuses_the_whole_view() {
    let mut entries = six_points();
    entries[0]["availability"] = json!("SomethingNewer");
    let f = fixture(happy_routes(&entries));
    let outcome = run(&f, &policy(json!({}), json!({}))).await;
    assert_eq!(outcome.ready_reason, ctrl::REASON_CATALOG_UNUSABLE);
}

// ---------------------------------------------------------------------------
// Enforce mode — the approval gate
// ---------------------------------------------------------------------------

fn enforcement(approved: Option<&str>) -> Value {
    let mut block = json!({
        "credentialSecretRef": {"name": "retention-delete"},
        "schedule": "17 4 * * *",
        "requireApprovedPlan": true,
        "planMaxAgeSeconds": 3600,
        "maxDeletionsPerRun": 50,
        "maxObjectsPerRun": 20000,
        "deadlineSeconds": 1800
    });
    if let Some(digest) = approved {
        block["approvedPlanSha256"] = json!(digest);
    }
    block
}

fn enforcing(approved: Option<&str>) -> Value {
    json!({"mode": "Enforce", "enforcement": enforcement(approved)})
}

/// No approved digest: no Job. Not "a Job that does nothing" — ZERO `POST`s.
#[tokio::test]
async fn an_unapproved_plan_creates_no_job() {
    let f = fixture(happy_routes(&six_points()));
    let outcome = run(&f, &policy(enforcing(None), json!({}))).await;

    assert_eq!(outcome.phase, ctrl::RetentionPhase::Evaluated);
    assert_eq!(outcome.enforced_reason, ctrl::REASON_AWAITING_APPROVAL);
    assert!(
        f.seen().iter().all(|(m, _)| m != "POST"),
        "with `requireApprovedPlan: true` no Job is created until the digest matches. \
         Requests: {:?}",
        f.seen()
    );
    let message = f.condition(ctrl::CONDITION_ENFORCED)["message"]
        .as_str()
        .expect("a message")
        .to_string();
    assert!(
        message.contains("approvedPlanSha256") && message.contains("sha256:"),
        "the condition tells the administrator the exact digest to approve: {message}"
    );
}

/// A STALE approved digest is refused, with both digests named.
#[tokio::test]
async fn a_stale_approved_digest_creates_no_job() {
    let f = fixture(happy_routes(&six_points()));
    let outcome = run(
        &f,
        &policy(
            enforcing(Some(&format!("sha256:{}", "9".repeat(64)))),
            json!({}),
        ),
    )
    .await;
    assert_eq!(outcome.enforced_reason, ctrl::REASON_PLAN_SUPERSEDED);
    assert!(f.seen().iter().all(|(m, _)| m != "POST"));
    let message = f.condition(ctrl::CONDITION_ENFORCED)["message"]
        .as_str()
        .expect("a message")
        .to_string();
    assert!(
        message.contains("999999"),
        "the stale digest is named: {message}"
    );
}

/// The digest the status publishes is the one that authorises the run: approve
/// it and the Job is created.
#[tokio::test]
async fn approving_the_published_digest_creates_exactly_one_job() {
    // First pass: learn the digest the evaluation publishes.
    let f = fixture(happy_routes(&six_points()));
    run(&f, &policy(enforcing(None), json!({}))).await;
    let digest = f.status()["lastEvaluation"]["planSha256"]
        .as_str()
        .expect("a digest")
        .to_string();

    // Second pass: the administrator has approved exactly that.
    let mut routes = happy_routes(&six_points());
    routes.push(plan_config_map_route(&digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let g = fixture(routes);
    let outcome = run(&g, &policy(enforcing(Some(&digest)), json!({}))).await;

    assert_eq!(outcome.phase, ctrl::RetentionPhase::Started);
    assert_eq!(outcome.enforcement, ctrl::ENFORCEMENT_LOGWEIR_WORKER);
    let jobs = g.posted("/jobs");
    assert_eq!(jobs.len(), 1, "exactly one Job");
    assert_eq!(
        g.posted("/configmaps").len(),
        1,
        "and exactly one plan ConfigMap"
    );
    assert!(
        g.seen().iter().all(|(m, _)| m != "DELETE"),
        "and still no DELETE from the controller, ever"
    );

    // The lease was written BEFORE the Job: the ORDER is the property.
    let patches = g.status_patches();
    let lease_index = patches
        .iter()
        .position(|p| p["status"].get("lease").is_some())
        .expect("a lease patch");
    let run_index = patches
        .iter()
        .position(|p| p["status"].get("lastEnforcement").is_some())
        .expect("a run patch");
    assert!(
        lease_index < run_index,
        "the lease is written first, then the consistent re-list, then the Job: a restore that \
         arrives after the lease is seen by the re-list, and one that arrives after the re-list \
         is held by restore admission"
    );
    let lease = &patches[lease_index]["status"]["lease"];
    assert_eq!(
        lease["pointIds"].as_array().expect("point ids").len(),
        3,
        "the lease names exactly the points the plan would remove"
    );
}

/// The Job runs `logweir-retention` and NOT `logweir`, under its own
/// ServiceAccount, with the delete credential by `secretKeyRef` only.
#[tokio::test]
async fn the_job_runs_the_separate_binary_and_never_carries_a_credential_value() {
    let f = fixture(happy_routes(&six_points()));
    run(&f, &policy(enforcing(None), json!({}))).await;
    let digest = f.status()["lastEvaluation"]["planSha256"]
        .as_str()
        .expect("a digest")
        .to_string();

    let mut routes = happy_routes(&six_points());
    routes.push(plan_config_map_route(&digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let g = fixture(routes);
    run(&g, &policy(enforcing(Some(&digest)), json!({}))).await;

    let job = g.posted("/jobs").remove(0);
    let pod = &job["spec"]["template"]["spec"];
    assert_eq!(pod["serviceAccountName"], ctrl::SERVICE_ACCOUNT);
    assert_eq!(pod["automountServiceAccountToken"], json!(false));
    let container = &pod["containers"][0];
    assert_eq!(
        container["command"],
        json!([ctrl::RETENTION_BINARY]),
        "D-SEAMS S1's third exception: deletion linkage must not be reachable from the everyday \
         binary, so the Job runs a DIFFERENT executable"
    );
    let args: Vec<String> = serde_json::from_value(container["args"].clone()).expect("argv");
    assert!(args.contains(&"--retention-contract-version".to_string()));
    assert!(args.contains(&ctrl::RETENTION_CONTRACT_VERSION.to_string()));

    let env = container["env"].as_array().expect("env");
    let literal = |name: &str| {
        env.iter()
            .find(|e| e["name"] == name)
            .and_then(|e| e["value"].as_str())
            .map(str::to_string)
    };
    assert_eq!(
        literal(ctrl::env::PLAN_SHA256).as_deref(),
        Some(digest.as_str())
    );
    assert_eq!(literal(ctrl::env::POLICY_UID).as_deref(), Some(UID));
    assert_eq!(literal(ctrl::env::SCOPE_PREFIX).as_deref(), Some(SCOPE));
    assert_eq!(literal(ctrl::env::POLICY_GENERATION).as_deref(), Some("4"));

    // THE CREDENTIAL. By `secretKeyRef` and never by value.
    let delete_key = env
        .iter()
        .find(|e| e["name"] == "AWS_ACCESS_KEY_ID")
        .expect("the delete credential is projected");
    assert_eq!(
        delete_key["valueFrom"]["secretKeyRef"]["name"], "retention-delete",
        "from `spec.enforcement.credentialSecretRef` and from nowhere else"
    );
    assert!(
        delete_key.get("value").is_none(),
        "a literal credential in a Job spec is visible to anyone with `get jobs`"
    );
    let rendered = serde_json::to_string(&job).expect("the Job serialises");
    for forbidden in ["AKIA", "secretAccessKey\":\"", "BEGIN PRIVATE KEY"] {
        assert!(
            !rendered.contains(forbidden),
            "no credential VALUE appears anywhere in the Job: `{forbidden}`"
        );
    }

    // The owner reference, and `blockOwnerDeletion: false` (D3 §6.5).
    let owner = &job["metadata"]["ownerReferences"][0];
    assert_eq!(owner["kind"], "RetentionPolicy");
    assert_eq!(owner["uid"], UID);
    assert_eq!(owner["controller"], json!(true));
}

/// The plan `ConfigMap` is immutable and owned by the policy, and it carries the
/// exact bytes the digest covers.
#[tokio::test]
async fn the_plan_config_map_is_immutable_and_digest_annotated() {
    let f = fixture(happy_routes(&six_points()));
    run(&f, &policy(enforcing(None), json!({}))).await;
    let digest = f.status()["lastEvaluation"]["planSha256"]
        .as_str()
        .expect("a digest")
        .to_string();

    let mut routes = happy_routes(&six_points());
    routes.push(plan_config_map_route(&digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let g = fixture(routes);
    run(&g, &policy(enforcing(Some(&digest)), json!({}))).await;

    let cm = g.posted("/configmaps").remove(0);
    assert_eq!(cm["immutable"], json!(true));
    assert_eq!(
        cm["metadata"]["annotations"][plan::PLAN_DIGEST_ANNOTATION],
        digest
    );
    assert_eq!(
        cm["metadata"]["ownerReferences"][0]["blockOwnerDeletion"],
        json!(false),
        "`true` asks for `update` on the owner's finalizers subresource under \
         OwnerReferencesPermissionEnforcement, which this ClusterRole grants on nothing"
    );
    let body = cm["data"][plan::PLAN_DATA_KEY]
        .as_str()
        .expect("the plan bytes");
    assert_eq!(
        logweir_core::ids::sha256_prefixed(body.as_bytes()),
        digest,
        "the bytes in the ConfigMap are exactly the bytes the administrator approved"
    );
    let parsed: Value = serde_json::from_str(body).expect("the plan is JSON");
    assert_eq!(parsed["format"], plan::PLAN_MEDIA_TYPE);
    assert_eq!(parsed["policy_uid"], UID);
}

/// `requireApprovedPlan: false` runs unattended AND records that choice on the
/// object, so it is visible in `kubectl describe`.
#[tokio::test]
async fn unattended_deletion_is_recorded_on_the_object() {
    let learn = fixture(happy_routes(&six_points()));
    run(&learn, &policy(enforcing(None), json!({}))).await;
    let digest = learn.status()["lastEvaluation"]["planSha256"]
        .as_str()
        .expect("a digest")
        .to_string();

    let mut spec = enforcing(None);
    spec["enforcement"]["requireApprovedPlan"] = json!(false);
    let mut routes = happy_routes(&six_points());
    routes.push(plan_config_map_route(&digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let f = fixture(routes);
    let outcome = run(&f, &policy(spec, json!({}))).await;
    assert_eq!(outcome.phase, ctrl::RetentionPhase::Started);
    // The `Enforced` condition is rewritten to `RunInProgress` once the Job is
    // created, so the UNATTENDED reason is read off the evaluation patch that
    // preceded it.
    let reasons: Vec<String> = f
        .status_patches()
        .iter()
        .filter_map(|p| p["status"].get("conditions").cloned())
        .flat_map(|c| c.as_array().cloned().unwrap_or_default())
        .filter(|c| c["type"] == ctrl::CONDITION_ENFORCED)
        .filter_map(|c| c["reason"].as_str().map(str::to_string))
        .collect();
    assert!(
        reasons.contains(&ctrl::REASON_UNATTENDED.to_string()),
        "D3 §6.5 requires the choice to be visible ON the object. Reasons written: {reasons:?}"
    );
}

/// Nothing to remove: no Job, and the reason says why.
#[tokio::test]
async fn an_empty_candidate_list_creates_no_job() {
    // Three points and `keepLast: 2` with `minUsablePoints: 3` keeps all three.
    let entries: Vec<Value> = (1..=3).map(|d| view_entry(&format!("p{d}"), d)).collect();
    let f = fixture(happy_routes(&entries));
    let outcome = run(&f, &policy(enforcing(None), json!({}))).await;
    assert_eq!(outcome.candidates, 0);
    assert_eq!(outcome.enforced_reason, ctrl::REASON_NOTHING_TO_DO);
    assert!(f.seen().iter().all(|(m, _)| m != "POST"));
}

// ---------------------------------------------------------------------------
// The active-restore race
// ---------------------------------------------------------------------------

fn restore_body(set: &str, phase: Option<&str>) -> Value {
    let mut status = json!({});
    if let Some(phase) = phase {
        status["phase"] = json!(phase);
    }
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
        "metadata": {"name": "r1", "namespace": NS, "uid": "r1", "resourceVersion": "1"},
        "spec": {
            "planBytes": "{}",
            "approvalRef": {"name": "a1"},
            "sourceArchive": {"url": LOCATION},
            "backupSetRef": set,
            "pointInTime": "2026-09-17T00:00:00Z",
            "target": {"clusterRef": {"name": "t"}, "mode": "scratch",
                       "topicNaming": {"prefix": "restored-"}},
            "deadlineSeconds": 600
        },
        "status": status
    })
}

fn restore_list(items: Vec<Value>) -> String {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "RestoreList",
        "metadata": {"resourceVersion": "1"},
        "items": items
    })
    .to_string()
}

/// A nonterminal restore protects its point and stops the run.
#[tokio::test]
async fn a_nonterminal_restore_protects_its_point_and_blocks_the_run() {
    // `p6` is the oldest candidate; a restore names its set.
    let mut routes = happy_routes(&six_points());
    routes.retain(|r| r.path_suffix != "/restores");
    routes.push(route(
        "GET",
        "/restores",
        restore_list(vec![restore_body("set-p6", Some("Running"))]),
    ));
    let f = fixture(routes);
    let outcome = run(&f, &policy(enforcing(None), json!({}))).await;

    assert_eq!(
        outcome.candidates, 2,
        "p6 is protected, so two candidates remain"
    );
    let protected = f.status()["lastEvaluation"]["protected"]
        .as_array()
        .expect("protected")
        .clone();
    assert!(
        protected
            .iter()
            .any(|p| p["pointId"] == "p6" && p["reason"] == "ActiveRestore"),
        "protected: {protected:?}"
    );
    assert!(f.seen().iter().all(|(m, _)| m != "POST"));
}

/// A TERMINAL restore protects nothing — the mutant for the row above.
#[tokio::test]
async fn a_terminal_restore_protects_nothing() {
    let mut routes = happy_routes(&six_points());
    routes.retain(|r| r.path_suffix != "/restores");
    routes.push(route(
        "GET",
        "/restores",
        restore_list(vec![restore_body("set-p6", Some("Succeeded"))]),
    ));
    let f = fixture(routes);
    let outcome = run(&f, &policy(json!({}), json!({}))).await;
    assert_eq!(outcome.candidates, 3);
    assert_eq!(outcome.protected, 1, "only the minUsablePoints override");
}

/// A restore with NO phase at all is nonterminal: the reconciler has not seen
/// it yet, and "not seen yet" is not "finished".
#[tokio::test]
async fn a_restore_with_no_phase_is_nonterminal() {
    let mut routes = happy_routes(&six_points());
    routes.retain(|r| r.path_suffix != "/restores");
    routes.push(route(
        "GET",
        "/restores",
        restore_list(vec![restore_body("set-p6", None)]),
    ));
    let f = fixture(routes);
    let outcome = run(&f, &policy(json!({}), json!({}))).await;
    assert_eq!(outcome.candidates, 2);
}

/// A restore reading ANOTHER destination protects nothing here.
#[tokio::test]
async fn a_restore_at_another_destination_protects_nothing_here() {
    let mut routes = happy_routes(&six_points());
    routes.retain(|r| r.path_suffix != "/restores");
    let mut elsewhere = restore_body("set-p6", Some("Running"));
    elsewhere["spec"]["sourceArchive"]["url"] = json!("s3://other-bucket/team-b");
    routes.push(route("GET", "/restores", restore_list(vec![elsewhere])));
    let f = fixture(routes);
    let outcome = run(&f, &policy(json!({}), json!({}))).await;
    assert_eq!(
        outcome.candidates, 3,
        "two destinations legitimately hold sets of the same backupId, and protecting one \
         because the other is being restored would be the wrong-destination defect wearing a \
         helpful face"
    );
}

// ---------------------------------------------------------------------------
// Harvest, degradation and the lease
// ---------------------------------------------------------------------------

fn job_body(name: &str, finished: bool) -> String {
    let status = if finished {
        json!({"conditions": [{"type": "Complete", "status": "True"}]})
    } else {
        json!({"active": 1})
    };
    json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {
            "name": name, "namespace": NS, "uid": "j1", "resourceVersion": "9",
            "ownerReferences": [{
                "apiVersion": "logweir.dev/v1alpha1", "kind": "RetentionPolicy",
                "name": NAME, "uid": UID, "controller": true, "blockOwnerDeletion": false
            }]
        },
        "spec": {"template": {"spec": {"containers": [{"name": "runner"}]}}},
        "status": status
    })
    .to_string()
}

fn pod_list(exit_code: i32) -> String {
    json!({
        "apiVersion": "v1", "kind": "PodList", "metadata": {},
        "items": [{
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {
                "name": "retention-pod", "namespace": NS, "uid": "p1",
                "ownerReferences": [{
                    "apiVersion": "batch/v1", "kind": "Job", "name": "job",
                    "uid": "j1", "controller": true, "blockOwnerDeletion": true
                }]
            },
            "status": {"phase": "Succeeded", "containerStatuses": [{
                "name": "runner", "ready": false, "restartCount": 0, "image": "i",
                "imageID": "i",
                "state": {"terminated": {"exitCode": exit_code, "reason": "Completed"}}
            }]}
        }]
    })
    .to_string()
}

fn stem() -> String {
    format!(
        "lwr-{}",
        &logweir_core::ids::sha256_hex(UID.as_bytes())[..20]
    )
}

/// A finished run's exit code is published, the lease is cleared and the Job's
/// TTL is patched — in that order.
#[tokio::test]
async fn a_finished_run_is_harvested_and_clears_its_lease() {
    let run_id = "r00000000deadbeef";
    let job_name = format!("{}-{run_id}", stem());
    let leaked: &'static str = Box::leak(format!("/jobs/{job_name}").into_boxed_str());
    let routes = vec![
        route("GET", "/retentionpolicies", policy_list(vec![])),
        route("GET", leaked, job_body(&job_name, true)),
        route("GET", "/pods", pod_list(0)),
        route(
            "PATCH",
            "/retentionpolicies/primary/status",
            policy_value(json!({}), json!({})).to_string(),
        ),
        route("PATCH", leaked, "{}".to_string()),
    ];
    let f = fixture(routes);
    let status = json!({
        "lastEnforcement": {"runId": run_id, "startedAt": "2026-09-17T04:00:00Z"},
        "lease": {"runId": run_id, "pointIds": ["p6"]}
    });
    let outcome = run(&f, &policy(enforcing(None), status)).await;

    assert_eq!(outcome.phase, ctrl::RetentionPhase::Harvested);
    assert_eq!(outcome.enforced_reason, ctrl::REASON_RUN_COMPLETE);
    let patch = f.status_patches().remove(0);
    assert_eq!(patch["status"]["lastEnforcement"]["exitCode"], 0);
    assert!(
        patch["status"]["lease"].is_null(),
        "RFC 7386: `null` DELETES the key. A lease left behind would hold restore admission for \
         nothing. Patch: {patch}"
    );
    // The status PATCH precedes the Job TTL PATCH.
    let order: Vec<String> = f
        .bodies
        .lock()
        .expect("bodies")
        .iter()
        .filter(|b| b.method == "PATCH")
        .map(|b| b.uri.clone())
        .collect();
    assert!(
        order[0].contains("/retentionpolicies/"),
        "pod garbage collection must never race the exit-code read, so the status is written \
         first. Order: {order:?}"
    );
    assert!(order.iter().any(|u| u.contains("/jobs/")));
}

/// A non-zero exit is a failure, counted; three in a row set
/// `EnforcementDegraded` and stop scheduling.
#[tokio::test]
async fn three_consecutive_failures_degrade_and_stop_scheduling() {
    let run_id = "r00000000deadbee2";
    let job_name = format!("{}-{run_id}", stem());
    let leaked: &'static str = Box::leak(format!("/jobs/{job_name}").into_boxed_str());
    let routes = vec![
        route("GET", "/retentionpolicies", policy_list(vec![])),
        route("GET", leaked, job_body(&job_name, true)),
        route("GET", "/pods", pod_list(1)),
        route(
            "PATCH",
            "/retentionpolicies/primary/status",
            policy_value(json!({}), json!({})).to_string(),
        ),
        route("PATCH", leaked, "{}".to_string()),
    ];
    let f = fixture(routes);
    let status = json!({
        "lastEnforcement": {"runId": run_id, "startedAt": "2026-09-17T04:00:00Z"},
        "consecutiveRunFailures": 2
    });
    let outcome = run(&f, &policy(enforcing(None), status)).await;
    assert_eq!(outcome.enforced_reason, ctrl::REASON_RUN_FAILED);
    let patch = f.status_patches().remove(0);
    assert_eq!(patch["status"]["consecutiveRunFailures"], 3);
    let degraded = patch["status"]["conditions"]
        .as_array()
        .expect("conditions")
        .iter()
        .find(|c| c["type"] == ctrl::CONDITION_DEGRADED)
        .cloned()
        .expect("a degraded condition");
    assert_eq!(degraded["status"], "True");
    assert_eq!(degraded["reason"], ctrl::REASON_CONSECUTIVE_FAILURES);
}

/// A degraded policy at the SAME generation schedules nothing further.
#[tokio::test]
async fn a_degraded_policy_creates_no_further_run_until_the_spec_changes() {
    let f = fixture(happy_routes(&six_points()));
    let status = json!({"consecutiveRunFailures": 3, "observedGeneration": 4});
    let outcome = run(&f, &policy(enforcing(None), status)).await;
    assert_eq!(outcome.enforced_reason, ctrl::REASON_RUN_FAILED);
    assert!(f.seen().iter().all(|(m, _)| m != "POST"));
}

/// A Job wearing this run's name that something else controls is refused, and
/// nothing is read from it — D-SEAMS S6 applied to a Job.
#[tokio::test]
async fn a_foreign_job_on_this_runs_name_is_refused() {
    let run_id = "r00000000deadbee3";
    let job_name = format!("{}-{run_id}", stem());
    let leaked: &'static str = Box::leak(format!("/jobs/{job_name}").into_boxed_str());
    let mut foreign: Value =
        serde_json::from_str(&job_body(&job_name, true)).expect("the fixture is a Job");
    foreign["metadata"]["ownerReferences"][0]["uid"] = json!("somebody-else");
    let routes = vec![
        route("GET", "/retentionpolicies", policy_list(vec![])),
        route("GET", leaked, foreign.to_string()),
        route(
            "PATCH",
            "/retentionpolicies/primary/status",
            policy_value(json!({}), json!({})).to_string(),
        ),
    ];
    let f = fixture(routes);
    let status = json!({"lastEnforcement": {"runId": run_id, "startedAt": "2026-09-17T04:00:00Z"}});
    let outcome = run(&f, &policy(enforcing(None), status)).await;
    assert_eq!(outcome.ready_reason, ctrl::REASON_JOB_NAME_CONFLICT);
    assert!(
        f.seen().iter().all(|(_, uri)| !uri.contains("/pods")),
        "a name is not an identity: nothing is read from a Job somebody else controls. \
         Requests: {:?}",
        f.seen()
    );
}

// ---------------------------------------------------------------------------
// `ExternalLifecycle`
// ---------------------------------------------------------------------------

fn external(expiration_days: i64) -> Value {
    json!({
        "mode": "ExternalLifecycle",
        "rules": {"keepLast": 30, "keepDays": 30, "minUsablePoints": 3},
        "externalLifecycle": {
            "provider": "s3",
            "ruleId": "expire-archive-90d",
            "expirationDays": expiration_days,
            "prefix": SCOPE
        }
    })
}

/// `ExternalLifecycle` is a DECLARATION: the guarantees table says what nobody
/// is enforcing, and Logweir evaluates nothing.
#[tokio::test]
async fn external_lifecycle_declares_and_claims_nothing() {
    let routes = vec![
        route("GET", "/retentionpolicies", policy_list(vec![])),
        route(
            "PATCH",
            "/retentionpolicies/primary/status",
            policy_value(json!({}), json!({})).to_string(),
        ),
    ];
    let f = fixture(routes);
    let outcome = run(&f, &policy(external(90), json!({}))).await;

    assert_eq!(outcome.phase, ctrl::RetentionPhase::Declared);
    assert_eq!(outcome.enforcement, ctrl::ENFORCEMENT_EXTERNAL);
    let status = f.status();
    assert_eq!(
        status["guarantees"]["ageExpiry"],
        ctrl::GUARANTEE_PROVIDER_UNVERIFIED
    );
    assert_eq!(
        status["guarantees"]["legalHold"],
        ctrl::GUARANTEE_PROVIDER_UNVERIFIED
    );
    for unenforced in [
        "minUsablePoints",
        "activeRestoreProtection",
        "sharedSegments",
    ] {
        assert_eq!(
            status["guarantees"][unenforced],
            ctrl::GUARANTEE_NOT_ENFORCED,
            "a bucket lifecycle rule cannot count usable points, cannot see an in-flight \
             restore and cannot reason about a shared segment; `{unenforced}` says so"
        );
    }
    assert_eq!(
        f.condition(ctrl::CONDITION_EVALUATED)["reason"],
        ctrl::REASON_NEVER_EVALUATED
    );
    assert!(
        f.seen()
            .iter()
            .all(|(_, uri)| !uri.contains("/recoverycatalogs/")),
        "nothing here reads the provider's rule, so there is nothing to evaluate. \
         Requests: {:?}",
        f.seen()
    );
    assert!(
        f.condition(ctrl::CONDITION_READY)["message"]
            .as_str()
            .expect("a message")
            .contains("expire-archive-90d"),
        "the rule id is on the object, so an auditor can find it in the provider's console"
    );
}

/// A declared expiry SHORTER than the rules keep is a conflict, and the bucket
/// wins.
#[tokio::test]
async fn a_declared_expiry_shorter_than_the_rules_is_a_conflict() {
    let routes = vec![
        route("GET", "/retentionpolicies", policy_list(vec![])),
        route(
            "PATCH",
            "/retentionpolicies/primary/status",
            policy_value(json!({}), json!({})).to_string(),
        ),
    ];
    let f = fixture(routes);
    run(&f, &policy(external(7), json!({}))).await;
    let conflict = f.condition(ctrl::CONDITION_EXTERNAL_CONFLICT);
    assert_eq!(conflict["status"], "True");
    assert_eq!(conflict["reason"], ctrl::REASON_DECLARED_EXPIRY_CONFLICTS);
    assert!(conflict["message"]
        .as_str()
        .expect("a message")
        .contains("the bucket wins"));
}

/// And a declared expiry LONGER than the rules keep is not a conflict — the
/// mutant for the row above.
#[tokio::test]
async fn a_declared_expiry_longer_than_the_rules_is_not_a_conflict() {
    let routes = vec![
        route("GET", "/retentionpolicies", policy_list(vec![])),
        route(
            "PATCH",
            "/retentionpolicies/primary/status",
            policy_value(json!({}), json!({})).to_string(),
        ),
    ];
    let f = fixture(routes);
    run(&f, &policy(external(3650), json!({}))).await;
    assert_eq!(
        f.condition(ctrl::CONDITION_EXTERNAL_CONFLICT)["status"],
        "False"
    );
}

// ---------------------------------------------------------------------------
// Status write shape — D-SEAMS S7
// ---------------------------------------------------------------------------

/// Every status write is a merge PATCH carrying `metadata.resourceVersion`.
#[tokio::test]
async fn every_status_write_carries_the_resource_version_precondition() {
    let f = fixture(happy_routes(&six_points()));
    run(&f, &policy(json!({}), json!({}))).await;
    let bodies = f.bodies.lock().expect("bodies");
    let patches: Vec<&SeenBody> = bodies
        .iter()
        .filter(|b| b.method == "PATCH" && b.uri.contains("/retentionpolicies/"))
        .collect();
    assert!(!patches.is_empty(), "at least one status write");
    for patch in patches {
        let value: Value = serde_json::from_str(&patch.body).expect("JSON");
        assert_eq!(
            value["metadata"]["resourceVersion"], "4242",
            "no `update` on any status subresource, and every merge PATCH carries the version \
             it was computed from: {}",
            patch.body
        );
        assert!(
            patch.uri.contains("/status"),
            "the write goes to the /status SUBRESOURCE and never to the object: {}",
            patch.uri
        );
    }
}

/// A policy with no `resourceVersion` sends no patch at all rather than a
/// last-writer-wins one.
#[tokio::test]
async fn a_policy_with_no_resource_version_sends_no_patch() {
    let mut value = policy_value(json!({}), json!({}));
    value["metadata"]
        .as_object_mut()
        .expect("metadata")
        .remove("resourceVersion");
    let p: RetentionPolicy = serde_json::from_value(value).expect("a policy");
    let f = fixture(happy_routes(&six_points()));
    run(&f, &p).await;
    assert!(f.status_patches().is_empty(), "requests: {:?}", f.seen());
}

/// Every condition reason this controller writes is in the closed set, and
/// every one is a valid `metav1.Condition.reason`.
#[test]
fn every_condition_reason_is_in_the_closed_set_and_is_camel_case() {
    for reason in ctrl::CONDITION_REASONS {
        assert!(
            !reason.is_empty() && reason.chars().all(|c| c.is_ascii_alphanumeric()),
            "`{reason}` is not a valid metav1.Condition.reason"
        );
        assert!(
            reason
                .chars()
                .next()
                .expect("non-empty")
                .is_ascii_uppercase(),
            "`{reason}` is not CamelCase"
        );
    }
    let unique: BTreeSet<&&str> = ctrl::CONDITION_REASONS.iter().collect();
    assert_eq!(unique.len(), ctrl::CONDITION_REASONS.len(), "no duplicates");
    assert_eq!(
        ctrl::CONDITION_TYPES.len(),
        5,
        "D3 §6.2's five condition types"
    );
}

// ===========================================================================
// The linkage gate — `scripts/check-no-archive-write.sh` check 3
// ===========================================================================

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the workspace root is two levels above crates/weirkeeper")
}

/// Run a child with a DEADLINE, killing and REAPING it on an overrun.
///
/// Every subprocess a test spawns must have a timeout: a hung child blocks the
/// whole worker, which is the failure WORKER-RULES records from 2026-09-15.
/// `Command::output()` has no bound of its own.
fn bounded(
    cmd: &mut std::process::Command,
    deadline: std::time::Duration,
) -> (Option<i32>, String) {
    let out_path = std::env::temp_dir().join(format!(
        "lw-d3w9-gate-{}-{:?}.log",
        std::process::id(),
        std::thread::current().id()
    ));
    let file = std::fs::File::create(&out_path).expect("a log file");
    let err_file = file.try_clone().expect("the same file for stderr");
    let mut child = cmd
        .current_dir(repo_root())
        .stdin(std::process::Stdio::null())
        .stdout(file)
        .stderr(err_file)
        .spawn()
        .expect("the child is spawnable");
    let started = std::time::Instant::now();
    let code = loop {
        match child.try_wait().expect("try_wait on the child") {
            Some(status) => break status.code(),
            None => {
                if started.elapsed() > deadline {
                    let _ = child.kill();
                    // REAPED, not merely killed.
                    let _ = child.wait();
                    panic!("the child did not exit within {deadline:?}");
                }
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
        }
    };
    let text = std::fs::read_to_string(&out_path).unwrap_or_default();
    let _ = std::fs::remove_file(&out_path);
    (code, text)
}

/// The shipped gate passes on the unmodified tree, and its linkage walk names
/// exactly the one binary allowed to reach the deleter.
#[test]
fn the_gate_reports_exactly_one_crate_reaching_the_reaper() {
    let mut cmd = std::process::Command::new("bash");
    cmd.arg(repo_root().join("scripts/check-no-archive-write.sh"));
    let (code, text) = bounded(&mut cmd, std::time::Duration::from_secs(120));
    assert_eq!(code, Some(0), "the gate is green on this tree:\n{text}");
    assert!(
        text.contains("the crates reaching logweir-reaper are exactly {logweir-retention}"),
        "the walk must RUN and must name the reaching set, so a reader of the log sees the \
         claim rather than a silent pass:\n{text}"
    );
}

/// The walk itself, over a synthetic graph — hermetic, so it can be driven
/// through every arm without editing a real `Cargo.toml` while other test
/// binaries are reading the tree.
#[test]
fn the_linkage_walk_counts_dev_edges_and_fails_closed() {
    let script = repo_root().join("scripts/reaper-linkage-walk.py");
    let write = |packages: Value| -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "lw-d3w9-meta-{}-{:?}.json",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::write(&path, json!({"packages": packages}).to_string()).expect("write");
        path
    };
    let walk = |path: &Path, target: &str| -> (Option<i32>, String) {
        let mut cmd = std::process::Command::new("python3");
        cmd.arg(&script).arg(path).arg(target);
        bounded(&mut cmd, std::time::Duration::from_secs(30))
    };

    // A DEV edge is a link. Dropping dev edges is the mutation the walk must
    // not survive, and this is the row that says so.
    let path = write(json!([
        {"name": "logweir-reaper", "dependencies": []},
        {"name": "logweir-retention", "dependencies": [{"name": "logweir-reaper", "kind": null}]},
        {"name": "weirkeeper", "dependencies": [{"name": "logweir-reaper", "kind": "dev"}]}
    ]));
    let (code, text) = walk(&path, "logweir-reaper");
    assert_eq!(code, Some(0));
    let names: Vec<&str> = text.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(
        names,
        vec!["logweir-retention", "weirkeeper"],
        "a `logweir-reaper` added under `[dev-dependencies]` links the deleter just as hard as \
         one under `[dependencies]`"
    );

    // TRANSITIVE edges count too.
    let path = write(json!([
        {"name": "logweir-reaper", "dependencies": []},
        {"name": "logweir-retention", "dependencies": [{"name": "logweir-reaper", "kind": null}]},
        {"name": "logweir-api", "dependencies": [{"name": "logweir-retention", "kind": null}]}
    ]));
    let (_, text) = walk(&path, "logweir-reaper");
    assert!(
        text.contains("logweir-api"),
        "reachability is transitive: {text}"
    );

    // FAIL-CLOSED on a target that is not in the workspace: a walk with no
    // target would report an empty reaching set forever, which reads exactly
    // like "nobody links the deleter".
    let path = write(json!([{"name": "weirkeeper", "dependencies": []}]));
    let (code, text) = walk(&path, "logweir-reaper");
    assert_eq!(code, Some(1), "a missing target is a failure: {text}");
    assert!(text.contains("is not a workspace member"));
    let _ = std::fs::remove_file(&path);
}

/// The gate's allowlist holds exactly one name, and it is the binary. A second
/// name is a widening of ADR 0008 Amendment H and must show up in a diff.
#[test]
fn the_reaper_allowlist_holds_exactly_one_binary() {
    let gate = std::fs::read_to_string(repo_root().join("scripts/check-no-archive-write.sh"))
        .expect("the gate is readable");
    let line = gate
        .lines()
        .find(|l| l.trim_start().starts_with("ALLOWED_REAPER_LINK="))
        .expect("the gate declares the allowlist");
    assert_eq!(
        line.trim(),
        "ALLOWED_REAPER_LINK=\"logweir-retention\"",
        "one name, and it is the separate binary. Widening this is an Amendment H change and a \
         security review, not an edit."
    );
    assert!(
        gate.contains("REAPER=\"logweir-reaper\""),
        "and the walk's target is named in the gate rather than derived"
    );
}

/// The everyday `logweir` binary links no delete path, read from the graph
/// rather than from a grep.
#[test]
fn the_everyday_binary_links_no_delete_path() {
    let mut cmd = std::process::Command::new("cargo");
    cmd.args(["metadata", "--no-deps", "--format-version", "1"]);
    let (code, text) = bounded(&mut cmd, std::time::Duration::from_secs(120));
    assert_eq!(code, Some(0), "cargo metadata: {text}");
    let meta: Value = serde_json::from_str(&text).expect("cargo metadata is JSON");
    let packages = meta["packages"].as_array().expect("packages");
    for crate_name in ["logweir", "weirkeeper", "logweir-store", "logweir-api"] {
        let package = packages
            .iter()
            .find(|p| p["name"] == crate_name)
            .unwrap_or_else(|| panic!("{crate_name} is a workspace member"));
        let deps: Vec<&str> = package["dependencies"]
            .as_array()
            .expect("dependencies")
            .iter()
            .filter_map(|d| d["name"].as_str())
            .collect();
        assert!(
            !deps.contains(&"logweir-reaper"),
            "{crate_name} must not link the deleter — D3 §6.5 and ADR 0008 Amendment H. Its \
             dependencies are {deps:?}"
        );
    }
}
