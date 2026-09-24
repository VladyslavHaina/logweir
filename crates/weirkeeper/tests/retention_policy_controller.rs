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
        refused_by_controller: false,
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

// ---------------------------------------------------------------------------
// Defect SHARED-SET-RETENTION — two receipts over ONE backup set
// ---------------------------------------------------------------------------

/// A second receipt over an existing set: same `backupId`, same manifest key,
/// a different point id. This is what a runner Job re-created from its frozen
/// inputs produces (live: harness-rows-11, `shared/473fe2d4…/`, both points
/// `Available`/`Verified`, manifest sha256 unchanged across the two runs).
fn second_receipt_over(original: &plan::PointFacts, id: &str, age_days: i64) -> plan::PointFacts {
    plan::PointFacts {
        point_id: id.to_string(),
        recovery_point_at_ms: now_ms() - age_days * DAY_MS,
        ..original.clone()
    }
}

/// THE LIVE DEFECT. Two receipts name one set and `keepLast 1` keeps the
/// newer: the older is the kept point's OWN set, so nothing of it is planned.
///
/// MUTANT: link on segment keys only — drop both the `set:` and the manifest
/// link from `Groups::of`, which is the pre-fix rule. `p-old` is planned
/// `BeyondKeepLast` and this row fails, with seven others (and
/// `a_shared_set_never_reaches_an_approvable_plan` fails at the writer's own
/// rail, which refuses the plan). Each link alone has its own row below.
#[test]
fn two_receipts_over_one_set_plan_nothing_of_that_set() {
    let newer = point("p-new", 1);
    let older = second_receipt_over(&newer, "p-old", 2);
    let evaluation = evaluate(&[newer, older], rules(Some(1), None, 1));
    assert!(
        candidate_ids(&evaluation).is_empty(),
        "the older receipt names the kept receipt's set: {:?}",
        candidate_ids(&evaluation)
    );
    assert_eq!(evaluation.kept, vec!["p-new", "p-old"]);
    assert_eq!(
        protected_reason(&evaluation, "p-old"),
        Some("SharedSegment")
    );
    assert_eq!(evaluation.truncated_by_cap, 0);
}

/// The same rule one layer out: the plan the controller would publish for the
/// live shape has no line at all.
#[test]
fn a_shared_set_never_reaches_an_approvable_plan() {
    let newer = point("p-new", 1);
    let older = second_receipt_over(&newer, "p-old", 2);
    let evaluation = evaluate(&[newer, older, point("p3", 3)], rules(Some(1), None, 1));
    let document = plan::plan_document(
        &identity(),
        &destination(),
        rules(Some(1), None, 1),
        &evaluation,
    )
    .expect("a plan");
    let lines: Vec<&str> = document.lines.iter().map(|l| l.point_id.as_str()).collect();
    assert_eq!(lines, vec!["p3"], "only the unshared set is planned");
    assert!(document.lines.iter().all(|l| l.backup_id != "set-p-new"));
}

/// NEGATIVE CONTROL for the two rows above: DISTINCT sets behave exactly as
/// before — the older point is a `BeyondKeepLast` candidate. A rule that
/// protected everything would pass the rows above and fail this one.
#[test]
fn distinct_sets_are_unchanged_by_the_shared_set_rule() {
    let evaluation = evaluate(
        &[point("p-new", 1), point("p-old", 2)],
        rules(Some(1), None, 1),
    );
    assert_eq!(candidate_ids(&evaluation), vec!["p-old"]);
    assert_eq!(protected_reason(&evaluation, "p-old"), None);
    assert_eq!(evaluation.candidates[0].reason.as_str(), "BeyondKeepLast");
}

/// The set is matched on `backupId` ALONE when the manifest key differs in
/// spelling: the set directory is what a line removes.
///
/// MUTANT: link on the manifest key only. This row fails.
#[test]
fn a_shared_backup_id_protects_even_when_the_manifest_keys_differ() {
    let newer = point("p-new", 1);
    let mut older = second_receipt_over(&newer, "p-old", 2);
    older.manifest_key = Some(format!("{SCOPE}/set-p-new/manifest.json.v0"));
    let evaluation = evaluate(&[newer, older], rules(Some(1), None, 1));
    assert!(candidate_ids(&evaluation).is_empty());
    assert_eq!(
        protected_reason(&evaluation, "p-old"),
        Some("SharedSegment")
    );
}

/// Two rows naming one MANIFEST KEY name one set whatever their `backupId`
/// says — a manifest is the first object a line deletes.
///
/// MUTANT: drop the manifest-key link from `Groups::of`. This row fails.
#[test]
fn a_shared_manifest_key_protects_even_when_the_backup_ids_differ() {
    let newer = point("p-new", 1);
    let mut older = point("p-old", 2);
    older.manifest_key = newer.manifest_key.clone();
    let evaluation = evaluate(&[newer, older], rules(Some(1), None, 1));
    assert!(candidate_ids(&evaluation).is_empty());
    assert_eq!(
        protected_reason(&evaluation, "p-old"),
        Some("SharedSegment")
    );
}

/// Protection is TRANSITIVE: a candidate that shares a segment with a
/// candidate that is itself protected (it shares the kept point's set) is
/// protected too — the protected one is retained, so its objects stay.
///
/// MUTANT: a single non-transitive pass (protect only candidates linked
/// DIRECTLY to a retained point). `p-c2` is planned and this row fails.
#[test]
fn shared_set_protection_is_transitive() {
    let kept = point("p-new", 1);
    let mut c1 = second_receipt_over(&kept, "p-c1", 2);
    c1.segment_keys = vec![format!("{SCOPE}/set-p-new/seg-0")];
    let mut c2 = point("p-c2", 3);
    c2.segment_keys = vec![format!("{SCOPE}/set-p-new/seg-0")];
    let evaluation = evaluate(&[kept, c1, c2], rules(Some(1), None, 1));
    assert!(
        candidate_ids(&evaluation).is_empty(),
        "{:?}",
        candidate_ids(&evaluation)
    );
    assert_eq!(protected_reason(&evaluation, "p-c2"), Some("SharedSegment"));
}

/// A skipped receipt over a set protects a usable receipt over the same set:
/// the L3 rule, now at set level.
#[test]
fn a_skipped_receipt_over_a_set_protects_the_usable_one() {
    let newest = point("p1", 1);
    let usable = point("p3", 3);
    let mut refused = second_receipt_over(&usable, "p2", 2);
    refused.refused_by_controller = true;
    let evaluation = evaluate(&[newest, refused, usable], rules(Some(1), None, 1));
    assert!(candidate_ids(&evaluation).is_empty());
    assert_eq!(protected_reason(&evaluation, "p3"), Some("SharedSegment"));
}

/// Two receipts over one set that are BOTH beyond the rules go TOGETHER or not
/// at all: the deletion ceiling never selects one and keeps its sibling, which
/// would remove the set a point reported as kept still names.
///
/// MUTANT: apply the ceiling per point (the pre-fix loop). With a ceiling of 1
/// `p2` is selected and `p3` — same set — is kept over the ceiling.
#[test]
fn the_deletion_ceiling_never_splits_a_shared_set() {
    let first = point("p2", 2);
    let second = second_receipt_over(&first, "p3", 3);
    let points = vec![point("p1", 1), first, second, point("p4", 4)];
    let at_cap = |cap: i64| {
        plan::evaluate(&plan::Input {
            destination: &destination(),
            points: &points,
            rules: rules(Some(1), None, 1),
            holds: &[],
            protection: &plan::Protection::default(),
            now: now(),
            max_deletions_per_run: cap,
        })
    };
    let one = at_cap(1);
    assert_eq!(
        candidate_ids(&one),
        vec!["p4"],
        "the shared pair does not fit a ceiling of 1, the next group does"
    );
    assert_eq!(
        one.truncated_by_cap, 2,
        "both receipts of the pair are counted"
    );
    let two = at_cap(2);
    assert_eq!(candidate_ids(&two), vec!["p2", "p3"]);
    assert_eq!(two.truncated_by_cap, 1);
}

/// Review M2: two receipts over ONE set that are BOTH due render ONE plan
/// line, the second receipt a co-point — never two lines over one manifest,
/// which the worker refuses as `DuplicateKey` (the whole plan, every run).
///
/// MUTANT: drop the one-line-per-set join in `plan_document`. Two lines with
/// one manifest key come back and this row fails.
#[test]
fn both_receipts_of_a_due_shared_set_render_one_line() {
    let first = point("p2", 2);
    let second = second_receipt_over(&first, "p3", 3);
    let evaluation = evaluate(
        &[point("p1", 1), first, second, point("p4", 4)],
        rules(Some(1), None, 1),
    );
    assert_eq!(candidate_ids(&evaluation), vec!["p2", "p3", "p4"]);
    let document = plan::plan_document(
        &identity(),
        &destination(),
        rules(Some(1), None, 1),
        &evaluation,
    )
    .expect("a plan");
    let lines: Vec<(&str, &[String])> = document
        .lines
        .iter()
        .map(|l| (l.point_id.as_str(), l.co_point_ids.as_slice()))
        .collect();
    assert_eq!(
        lines,
        vec![("p2", &["p3".to_string()][..]), ("p4", &[][..])],
        "one line per set, every point named"
    );
    let manifests: BTreeSet<&str> = document
        .lines
        .iter()
        .map(|l| l.manifest_key.as_str())
        .collect();
    assert_eq!(
        manifests.len(),
        document.lines.len(),
        "no manifest on two lines"
    );
    // A plan with no shared set carries no `co_point_ids` key at all, so its
    // bytes — and an approved digest — are what they were before the field.
    let unshared = plan::plan_document(
        &identity(),
        &destination(),
        rules(Some(1), None, 1),
        &evaluate(&[point("p1", 1), point("p4", 4)], rules(Some(1), None, 1)),
    )
    .expect("a plan");
    let (bytes, _) = plan::plan_bytes(&unshared).expect("bytes");
    assert!(!String::from_utf8(bytes)
        .expect("utf8")
        .contains("co_point_ids"));
}

/// Review L1: a row over the candidate's set whose location the catalog could
/// not establish (`locations[]` empty) is never counted here — and it still
/// protects the set it names.
///
/// MUTANT: leave location-less rows out of the grouping. `p-old` is planned
/// and this row fails.
#[test]
fn a_location_less_receipt_over_a_set_protects_it() {
    let newest = point("p1", 1);
    let old = point("p-old", 3);
    let mut unplaced = second_receipt_over(&old, "p-unplaced", 2);
    unplaced.locations.clear();
    let evaluation = evaluate(&[newest, old, unplaced], rules(Some(1), None, 1));
    assert!(
        candidate_ids(&evaluation).is_empty(),
        "{:?}",
        candidate_ids(&evaluation)
    );
    assert_eq!(
        protected_reason(&evaluation, "p-old"),
        Some("SharedSegment")
    );
    assert_eq!(
        evaluation.points_evaluated, 2,
        "and it is not counted as here"
    );
}

/// Review L8: a set id that names no single directory — empty, or containing a
/// `/` (a bound that would contain OTHER sets) — is kept `Unknown`, not planned.
///
/// MUTANT: drop the check. `p-nested` is planned (its bound `team-a/a/b/` is
/// fine) or the empty id refuses the whole plan at the writer; this row fails
/// either way.
#[test]
fn a_set_id_that_names_no_single_directory_is_never_planned() {
    let mut empty = point("p-empty", 3);
    empty.backup_id = String::new();
    let mut nested = point("p-nested", 4);
    nested.backup_id = "a/b".to_string();
    nested.manifest_key = Some(format!("{SCOPE}/a/b/manifest.json"));
    let evaluation = evaluate(
        &[point("p1", 1), point("p2", 2), empty, nested],
        rules(Some(1), None, 1),
    );
    assert_eq!(candidate_ids(&evaluation), vec!["p2"]);
    assert_eq!(protected_reason(&evaluation, "p-empty"), Some("Unknown"));
    assert_eq!(protected_reason(&evaluation, "p-nested"), Some("Unknown"));
    plan::plan_document(
        &identity(),
        &destination(),
        rules(Some(1), None, 1),
        &evaluation,
    )
    .expect("the rest of the plan is still written");
}

/// THE WRITER'S OWN RAIL, independent of the grouping: an evaluation that
/// names a candidate over a set a retained point still names is refused, and
/// no plan is written.
///
/// Built by hand, because `evaluate` no longer produces one — which is the
/// point of a second rail. MUTANT: delete the `retained.backup_ids` check in
/// `plan_document`. This row fails.
#[test]
fn the_plan_writer_refuses_a_line_over_a_retained_set() {
    let evaluation = plan::Evaluation {
        candidates: vec![plan::Candidate {
            point_id: "p-old".to_string(),
            backup_id: "set-shared".to_string(),
            reason: plan::CandidateReason::BeyondKeepLast,
            recovery_point_at_ms: now_ms(),
            manifest_key: format!("{SCOPE}/set-shared/manifest.json"),
            segment_keys: Vec::new(),
            bytes: None,
        }],
        retained: plan::RetainedObjects {
            backup_ids: BTreeSet::from(["set-shared".to_string()]),
            keys: BTreeSet::new(),
        },
        ..plan::Evaluation::default()
    };
    let refused = plan::plan_document(
        &identity(),
        &destination(),
        rules(Some(1), None, 1),
        &evaluation,
    );
    assert!(
        matches!(refused, Err(plan::PlanError::SharedWithRetained { ref point_id, .. }) if point_id == "p-old"),
        "{refused:?}"
    );

    // The key half: a retained point naming an object UNDER the candidate's
    // set directory refuses the line too, whatever its own backup id says.
    let mut by_key = evaluation.clone();
    by_key.retained = plan::RetainedObjects {
        backup_ids: BTreeSet::from(["set-other".to_string()]),
        keys: BTreeSet::from([format!("{SCOPE}/set-shared/topics/t/partition=0/seg-1")]),
    };
    assert!(matches!(
        plan::plan_document(
            &identity(),
            &destination(),
            rules(Some(1), None, 1),
            &by_key
        ),
        Err(plan::PlanError::SharedWithRetained { .. })
    ));

    // And the negative control: retained objects elsewhere refuse nothing.
    let mut elsewhere = evaluation;
    elsewhere.retained = plan::RetainedObjects {
        backup_ids: BTreeSet::from(["set-other".to_string()]),
        keys: BTreeSet::from([format!("{SCOPE}/set-other/manifest.json")]),
    };
    let document = plan::plan_document(
        &identity(),
        &destination(),
        rules(Some(1), None, 1),
        &elsewhere,
    )
    .expect("nothing retained names this set");
    assert_eq!(document.lines.len(), 1);
}

/// `evaluate` publishes what it retained, from its OUTPUT: every non-candidate
/// point's set — kept, protected, over-cap and skipped alike.
#[test]
fn the_evaluation_names_every_retained_set() {
    let mut skipped = point("p9", 9);
    skipped.availability = Availability::Unreadable;
    let points = vec![point("p1", 1), point("p2", 2), point("p3", 3), skipped];
    let evaluation = evaluate(&points, rules(Some(1), None, 1));
    assert_eq!(candidate_ids(&evaluation), vec!["p2", "p3"]);
    assert_eq!(
        evaluation.retained.backup_ids,
        BTreeSet::from(["set-p1".to_string(), "set-p9".to_string()])
    );
    assert!(evaluation
        .retained
        .keys
        .contains(&format!("{SCOPE}/set-p9/manifest.json")));
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
    let document =
        plan::plan_document(&identity(), &destination(), r, &evaluation).expect("the plan renders");
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

/// **The generation does NOT reach the digest** — review `d3w9` C1, second leg.
///
/// The first landing put `policy_generation` in the bytes and this row asserted
/// that a bump changed the digest, as a feature. It is a deadlock: approving a
/// plan means patching `spec.enforcement.approvedPlanSha256`, a spec patch
/// bumps `metadata.generation`, and the digest the approval names changes in the
/// same write. There is no ordering of events in which the two agree.
#[test]
fn a_generation_bump_alone_leaves_the_digest_alone() {
    let points: Vec<plan::PointFacts> = (1..=6).map(|d| point(&format!("p{d}"), d)).collect();
    let r = rules(Some(2), None, 3);
    let evaluation = evaluate(&points, r);
    let (_, at_four) = plan::plan_bytes(
        &plan::plan_document(&identity(), &destination(), r, &evaluation).expect("renders"),
    )
    .expect("serialises");
    let mut five = identity();
    five.generation = 5;
    let (_, at_five) = plan::plan_bytes(
        &plan::plan_document(&five, &destination(), r, &evaluation).expect("renders"),
    )
    .expect("serialises");
    assert_eq!(
        at_four, at_five,
        "approving a plan is a SPEC patch and a spec patch bumps the generation; a digest the \
         act of approving changes can never be approved. What invalidates an approval is the \
         CONTENT — the rules as applied and the exact lines — and both are in the bytes."
    );
}

/// **The digest does not move with the clock** — review `d3w9` C1, first leg.
///
/// The reconciler requeues every 60 s and `reconcile` builds `Utc::now()`
/// fresh each time. With `evaluated_at` in the bytes, two renderings of one
/// archive three seconds apart digested differently, so the value an
/// administrator copied onto the spec had already expired when they copied it.
#[test]
fn the_digest_does_not_move_with_the_clock() {
    let points: Vec<plan::PointFacts> = (1..=6).map(|d| point(&format!("p{d}"), d)).collect();
    let r = rules(Some(2), None, 3);
    // The SAME archive, evaluated at two instants. `evaluate` takes `now` for
    // the age rule, so the evaluations themselves are computed at both.
    let first = plan::evaluate(&plan::Input {
        destination: &destination(),
        points: &points,
        rules: r,
        holds: &[],
        protection: &plan::Protection::default(),
        now: now(),
        max_deletions_per_run: 50,
    });
    let second = plan::evaluate(&plan::Input {
        destination: &destination(),
        points: &points,
        rules: r,
        holds: &[],
        protection: &plan::Protection::default(),
        now: now() + chrono::Duration::seconds(3),
        max_deletions_per_run: 50,
    });
    let digest_of = |e: &plan::Evaluation| {
        plan::plan_bytes(&plan::plan_document(&identity(), &destination(), r, e).expect("renders"))
            .expect("serialises")
            .1
    };
    assert_eq!(
        digest_of(&first),
        digest_of(&second),
        "there must be no instant in the digested bytes: a digest that changes every 60 s can \
         never equal an approvedPlanSha256, so `mode: Enforce` could never create a Job"
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

/// The plan `ConfigMap`'s name is UID-stemmed, content-named and bounded — and
/// it is the name the controller actually uses (review `d3w9` L3).
#[test]
fn the_plan_config_map_name_is_the_one_the_controller_uses() {
    let digest = format!("sha256:{}", "ab".repeat(32));
    let name = plan::plan_config_map_name(UID, &digest);
    assert!(name.starts_with("lwr-"));
    assert!(name.ends_with("-plan-abababababab"));
    assert!(
        name.len() <= 253,
        "a DNS subdomain is 253 characters; got {}",
        name.len()
    );
    // Stable in the UID, not in the policy NAME, so a 253-character policy name
    // cannot overflow it.
    assert_eq!(name, plan::plan_config_map_name(UID, &digest));
    assert_ne!(name, plan::plan_config_map_name("another-uid", &digest));
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

/// THE FIXTURE DESTINATION SEPARATES ITS PRINCIPALS, because that is the
/// destination `docs/kubernetes.md` §7a recommends and the one the live U6
/// measurement ran on. A fixture that named one Secret four times could not
/// tell `archiveRead` from `evidenceWrite` in a rendered Job, which is exactly
/// the confusion defect RET-EVIDENCE-GRANT-IS-ARCHIVEREAD lived in: the
/// enforcement Job's `LOGWEIR_EVIDENCE_AWS_*` carried the READER's Secret and
/// every test passed.
fn destination_body() -> String {
    destination_with_access(json!({
        "archiveWrite": {"mode": "SecretKeys", "secret": {
            "name": "lw-writer", "accessKeyIdKey": "id", "secretAccessKeyKey": "key"
        }},
        "archiveRead": {"mode": "SecretKeys", "secret": {
            "name": "lw-reader", "accessKeyIdKey": "id", "secretAccessKeyKey": "key"
        }},
        "evidenceWrite": {"mode": "SecretKeys", "secret": {
            "name": "lw-evidence", "accessKeyIdKey": "eid", "secretAccessKeyKey": "ekey"
        }}
    }))
}

/// The same destination with `spec.access` written by the caller — for the rows
/// that turn one grant off.
fn destination_with_access(access: Value) -> String {
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
            "access": access
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

/// The digest the catalog publishes for a page, **in the spelling production
/// publishes it in**: `sha256:<lowercase hex>`.
///
/// A page with NO published digest is now a view failure (review `d3w9` M4), so
/// every fixture that wants a readable view has to publish one — which is the
/// production shape: `catalog_view::materialise` always writes it.
///
/// THE PREFIX IS THE POINT, AND ITS ABSENCE WAS A LIVE DEFECT. This helper used
/// to return `catalog_view::page_digest`'s bare hex, which
/// `catalog_view::seal` never writes and the CRD does not document
/// (`config/crd/recoverycatalogs.yaml`: "`sha256:<lowercase hex>` over its
/// entry lines"). Every route table below therefore published a spelling no
/// catalog produces, the positive path was green in CI and red in every
/// cluster, and RET-DIGEST-PREFIX — `Evaluated=False/ViewUnreadable` for every
/// `RetentionPolicy` on the build, so no retention report at all — survived six
/// tests that all read the view. The fixture is now the production shape, so
/// the whole happy path is the regression row.
///
/// It is built from `sha256_prefixed` over the page bytes rather than by
/// prefixing `page_digest`'s answer, so the fixture agrees with
/// `catalog_view::seal` by construction and not by a second re-derivation of
/// the same idea.
fn published_page_digest_of(entries: &[Value]) -> String {
    let body: String = entries
        .iter()
        .map(|e| {
            format!(
                "{}\n",
                serde_json::to_string(e).expect("an entry serialises")
            )
        })
        .collect();
    logweir_core::ids::sha256_prefixed(body.as_bytes())
}

/// The same digest in the LEGACY bare spelling, for the compatibility row.
fn bare_page_digest_of(entries: &[Value]) -> String {
    let bodies: Vec<String> = entries
        .iter()
        .map(|e| serde_json::to_string(e).expect("an entry serialises"))
        .collect();
    let refs: Vec<&str> = bodies.iter().map(String::as_str).collect();
    weirkeeper::catalog_view::page_digest(&refs)
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

/// The `GET …/jobs/<name>` that `start_run` reads BEFORE it writes the run
/// record — defect RET-STARTRUN-PATCH-OUTCOME.
///
/// The record is written first now, so that a record write refused after a Job
/// was created can no longer leave a deletion Job the status never tracks. That
/// order needs this read: reaching the create's own `409 AlreadyExists` arm
/// with the record already written would be defect RET-COUNT-EARLY's
/// resurrection again. A real 404, because the double refuses an unrouted
/// request and a 404 is what "no Job stands at this name" looks like.
///
/// The two slots `start_run` can name a run after at `at`, and therefore the
/// two Job names a pass can ask for.
///
/// `reconcile` uses `enforcement_slot().unwrap_or(ctx.now)`: the fixture's
/// `"17 4 * * *"` due instant at or before `at` when the cadence answers, and
/// `at` itself when it does not. Both are routed rather than one guessed,
/// because a guess that goes stale fails as a missing route — a failure about
/// this table and not about the product.
fn slot_candidates(at: DateTime<Utc>) -> Vec<DateTime<Utc>> {
    let due = at
        .date_naive()
        .and_hms_opt(4, 17, 0)
        .expect("04:17 is a real time")
        .and_utc();
    let due = if due <= at {
        due
    } else {
        due - chrono::Duration::days(1)
    };
    if due == at {
        vec![at]
    } else {
        vec![due, at]
    }
}

/// `job_name` for one slot.
fn job_name_for(digest: &str, slot: DateTime<Utc>) -> String {
    format!("{}-{}", stem(), plan::run_id(UID, digest, slot.timestamp()))
}

/// The `GET …/jobs/<name>` routes that answer "no Job stands at this run's
/// name", one per slot `start_run` could name at `at`.
fn absent_job_routes(digest: &str, at: DateTime<Utc>) -> Vec<Route> {
    slot_candidates(at)
        .into_iter()
        .map(|slot| {
            let suffix: &'static str =
                Box::leak(format!("/jobs/{}", job_name_for(digest, slot)).into_boxed_str());
            Route {
                method: "GET",
                path_suffix: suffix,
                status: 404,
                body: json!({
                    "kind": "Status", "apiVersion": "v1", "status": "Failure",
                    "reason": "NotFound", "code": 404, "message": "jobs.batch not found"
                })
                .to_string(),
            }
        })
        .collect()
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
    routes_for_destination(entries, destination_body())
}

/// [`happy_routes`] over a destination the caller wrote — the one seam the
/// `evidenceWrite` rows need, because the grant they turn off is read from the
/// object and from nowhere else.
fn routes_for_destination(entries: &[Value], destination: String) -> Vec<Route> {
    vec![
        route("GET", "/retentionpolicies", policy_list(vec![])),
        route("GET", "/backupdestinations/archive", destination),
        route(
            "GET",
            "/recoverycatalogs/primary",
            catalog_body(
                Some(DEST),
                json!([{
                    "configMapName": "page-0", "index": 0,
                    "count": entries.len(),
                    "sha256": published_page_digest_of(entries)
                }]),
            ),
        ),
        route("GET", "/configmaps/page-0", page_config_map(entries)),
        route("GET", "/backups", empty_list("Backup")),
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
    run_at(fixture, policy, now()).await
}

/// One pass at an EXPLICIT instant.
///
/// The first landing had only the frozen-`now()` form, which is why C1 — a plan
/// digest that moved with the wall clock — survived
/// `approving_the_published_digest_creates_exactly_one_job`: both of its passes
/// ran at the same instant, so the row asserted nothing about the thing that
/// was broken.
async fn run_at(fixture: &Fixture, policy: &RetentionPolicy, at: DateTime<Utc>) -> ctrl::Outcome {
    let installation = check::policy::Policy::defaults();
    let image = RunnerImage::default();
    ctrl::reconcile_policy(
        policy,
        &ctrl::PolicyContext {
            client: &fixture.client,
            policy: &installation,
            runner_image: &image,
            now: at,
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

/// The page route table, with one hand-chosen `status.pages[].sha256`.
fn routes_publishing_digest(entries: &[Value], sha256: &str) -> Vec<Route> {
    let mut routes = happy_routes(entries);
    routes.retain(|r| r.path_suffix != "/recoverycatalogs/primary");
    routes.push(route(
        "GET",
        "/recoverycatalogs/primary",
        catalog_body(
            Some(DEST),
            json!([{
                "configMapName": "page-0", "index": 0, "count": entries.len(),
                "sha256": sha256
            }]),
        ),
    ));
    routes
}

/// **RET-DIGEST-PREFIX.** A page whose published digest carries the `sha256:`
/// prefix — which is the ONLY spelling `catalog_view::seal` writes and the only
/// one `config/crd/recoverycatalogs.yaml` documents — is read, not refused.
///
/// WHY THIS ROW EXISTS AS ITS OWN TEST when `happy_routes` already publishes
/// that spelling: the fixture can be changed back by accident, and this row
/// says in its name and in its body which spelling is production's. Before the
/// fix the controller compared `catalog_view::page_digest`'s bare hex against
/// this value with `!=`, so it was unequal for every page any real catalog ever
/// published and every `RetentionPolicy` in a cluster answered
/// `Evaluated=False/ViewUnreadable` — no retention report for any destination,
/// which is the whole of PLAT-16.1 and the controller half of PLAT-16.2. It was
/// green in CI only because the fixture published the bare form.
#[tokio::test]
async fn a_page_published_with_the_sha256_prefix_is_read_not_refused() {
    let entries = six_points();
    let published = published_page_digest_of(&entries);
    assert!(
        published.starts_with("sha256:"),
        "the fixture must publish the production spelling, or this row asserts nothing: {published}"
    );
    let f = fixture(routes_publishing_digest(&entries, &published));
    let outcome = run(&f, &policy(json!({}), json!({}))).await;

    assert_eq!(
        outcome.phase,
        ctrl::RetentionPhase::Evaluated,
        "the view is readable: {}",
        f.condition(ctrl::CONDITION_EVALUATED)["message"]
    );
    assert_ne!(outcome.ready_reason, ctrl::REASON_CATALOG_UNUSABLE);
    assert_eq!(
        f.condition(ctrl::CONDITION_EVALUATED)["status"],
        json!("True")
    );
    assert_eq!(outcome.points_evaluated, 6);
}

/// The two spellings are the same digest over the same bytes, so the row below
/// is about the PREFIX and not about a page whose contents differ.
#[test]
fn the_published_and_bare_spellings_differ_only_in_the_prefix() {
    let entries = six_points();
    assert_eq!(
        published_page_digest_of(&entries),
        format!("sha256:{}", bare_page_digest_of(&entries))
    );
}

/// A page published in the BARE spelling is still read. This is defensive
/// breadth, **not** history: no released build has written that spelling
/// (`catalog_view::seal` has always used `sha256_prefixed` — review finding
/// L3). What it buys is that the accepted set only ever grew, so upgrade and
/// rollback need no catalog resync. See `bare_hex` in the controller.
#[tokio::test]
async fn a_page_published_in_the_bare_spelling_is_still_read() {
    let entries = six_points();
    let bare = bare_page_digest_of(&entries);
    assert!(!bare.starts_with("sha256:"));
    let f = fixture(routes_publishing_digest(&entries, &bare));
    let outcome = run(&f, &policy(json!({}), json!({}))).await;

    assert_eq!(outcome.phase, ctrl::RetentionPhase::Evaluated);
    assert_eq!(outcome.points_evaluated, 6);
}

/// And the normalisation strips a PREFIX, never a digest: a prefixed value
/// whose hex is wrong is still refused, and so is a bare value whose hex is
/// wrong. Without this the fix could be written as "accept anything that starts
/// with `sha256:`" and no row above would notice.
///
/// **THE MATCH IS FULL-LENGTH, NOT A PREFIX MATCH** (review finding L6). The
/// last two values are the CORRECT digest truncated to half its hex, in both
/// spellings: every other value here differs in its first character, so a
/// comparison degraded to `found.starts_with(bare_hex(expected))` — the
/// reviewer's surviving mutant R2 — passed the whole suite, and a catalog that
/// published a truncated digest would have verified against bytes it does not
/// describe. A prefix of a digest is not a digest.
#[tokio::test]
async fn a_wrong_digest_is_refused_in_either_spelling() {
    let correct = bare_page_digest_of(&six_points());
    let half = &correct[..correct.len() / 2];
    for published in [
        format!("sha256:{}", "e".repeat(64)),
        "e".repeat(64),
        format!("sha256:{half}"),
        half.to_string(),
    ] {
        let entries = six_points();
        let f = fixture(routes_publishing_digest(&entries, &published));
        let outcome = run(&f, &policy(json!({}), json!({}))).await;
        assert_eq!(
            outcome.ready_reason,
            ctrl::REASON_CATALOG_UNUSABLE,
            "published {published}"
        );
        assert_eq!(
            f.condition(ctrl::CONDITION_EVALUATED)["reason"],
            ctrl::REASON_VIEW_UNREADABLE,
            "published {published}"
        );
    }
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
    routes.extend(absent_job_routes(&digest, now()));
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
    routes.extend(absent_job_routes(&digest, now()));
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

// ---------------------------------------------------------------------------
// The two credentials, and which grant lands on which variable — defect
// RET-EVIDENCE-GRANT-IS-ARCHIVEREAD
// ---------------------------------------------------------------------------

/// Render one enforcement Job against `destination` and hand the caller its
/// `secretKeyRef` variables, as `name -> (Secret, key)`.
///
/// Two passes, because the digest an administrator approves is only knowable
/// after the first one publishes it — the same shape every enforcing row here
/// uses.
async fn rendered_job_credentials(destination: String) -> BTreeMap<String, (String, String)> {
    let learn = fixture(routes_for_destination(&six_points(), destination.clone()));
    run(&learn, &policy(enforcing(None), json!({}))).await;
    let digest = learn.status()["lastEvaluation"]["planSha256"]
        .as_str()
        .expect("a digest")
        .to_string();

    let mut routes = routes_for_destination(&six_points(), destination);
    routes.push(plan_config_map_route(&digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.extend(absent_job_routes(&digest, now()));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let f = fixture(routes);
    run(&f, &policy(enforcing(Some(&digest)), json!({}))).await;

    let job = f.posted("/jobs").remove(0);
    let mut out = BTreeMap::new();
    for entry in job["spec"]["template"]["spec"]["containers"][0]["env"]
        .as_array()
        .expect("env")
    {
        if let Some(reference) = entry["valueFrom"].get("secretKeyRef") {
            out.insert(
                entry["name"].as_str().expect("a name").to_string(),
                (
                    reference["name"].as_str().expect("a Secret").to_string(),
                    reference["key"].as_str().expect("a key").to_string(),
                ),
            );
        }
    }
    out
}

/// `spec.access` with every principal separated — the destination
/// `docs/kubernetes.md` §7a recommends, and the one the live U6 run used.
fn four_principals() -> Value {
    json!({
        "archiveWrite": {"mode": "SecretKeys", "secret": {
            "name": "lw-writer", "accessKeyIdKey": "id", "secretAccessKeyKey": "key"
        }},
        "archiveRead": {"mode": "SecretKeys", "secret": {
            "name": "lw-reader", "accessKeyIdKey": "id", "secretAccessKeyKey": "key"
        }},
        "evidenceWrite": {"mode": "SecretKeys", "secret": {
            "name": "lw-evidence", "accessKeyIdKey": "eid", "secretAccessKeyKey": "ekey"
        }}
    })
}

/// **THE ROW THE LIVE DEFECT OWES.** `LOGWEIR_EVIDENCE_AWS_*` names the
/// destination's `evidenceWrite` Secret; `AWS_*` names the delete Secret; and
/// the `archiveRead` grant — which `evaluate` still resolves, for the location —
/// reaches the pod on NO variable at all.
///
/// Before the fix the evidence variables carried `resolved.grant`, the
/// `ArchiveRead` one, so live on a destination separating the four principals
/// the first intent tombstone came back `403 AccessDenied` (`state=Kept
/// code=TombstoneRefused`, `deleted=0 failed=1`,
/// `claude/artifacts/d2-live/u620260921t140000z`). The old fixture named one
/// Secret for everything and could not have seen it.
#[tokio::test]
async fn the_record_credential_is_the_evidence_write_grant_and_not_the_archive_one() {
    let env = rendered_job_credentials(destination_with_access(four_principals())).await;

    assert_eq!(
        env.get(weirkeeper::destination::EVIDENCE_ACCESS_KEY_ID_ENV),
        Some(&("lw-evidence".to_string(), "eid".to_string())),
        "D3 §7f: the record under `logweir/` is written with `spec.access.evidenceWrite`, \
         whose OWN data keys it names. Rendered: {env:?}"
    );
    assert_eq!(
        env.get(weirkeeper::destination::EVIDENCE_SECRET_ACCESS_KEY_ENV),
        Some(&("lw-evidence".to_string(), "ekey".to_string()))
    );
    assert_eq!(
        env.get("AWS_ACCESS_KEY_ID"),
        Some(&("retention-delete".to_string(), "access-key-id".to_string())),
        "and the deletes' own grant is `spec.enforcement.credentialSecretRef`, unchanged"
    );
    assert_eq!(
        env.get("AWS_SECRET_ACCESS_KEY"),
        Some(&(
            "retention-delete".to_string(),
            "secret-access-key".to_string()
        ))
    );
    let named: BTreeSet<&str> = env.values().map(|(secret, _)| secret.as_str()).collect();
    assert_eq!(
        named,
        BTreeSet::from(["lw-evidence", "retention-delete"]),
        "TWO credentials reach a retention pod and these are the two. The `archiveRead` grant \
         is resolved for the LOCATION and its Secret is named on no variable — reading the \
         archive is not something this Job does, and writing the record with the reader is the \
         defect. Rendered: {env:?}"
    );
}

/// The reader's session token is not a third part of the deleter's credential.
///
/// The filter that drops the destination's archive grant from the retention pod
/// named two variables and `AWS_SESSION_TOKEN` was not one of them, so an
/// `archiveRead` grant declaring `sessionTokenKey` put the READER's token beside
/// the DELETER's id and secret — a triple from two principals, which every call
/// would have rejected with a signature error naming neither. The evidence
/// grant's own token still reaches the pod, on its own variable.
#[tokio::test]
async fn the_archive_grants_session_token_never_reaches_the_retention_pod() {
    let mut access = four_principals();
    access["archiveRead"]["secret"]["sessionTokenKey"] = json!("reader-token");
    access["evidenceWrite"]["secret"]["sessionTokenKey"] = json!("etoken");
    let env = rendered_job_credentials(destination_with_access(access)).await;

    assert!(
        !env.contains_key("AWS_SESSION_TOKEN"),
        "`AWS_*` in a retention pod is the DELETE grant and all three of its variables; the \
         destination's archive credential is dropped as ONE credential, not two thirds of \
         one. Rendered: {env:?}"
    );
    assert_eq!(
        env.get(weirkeeper::destination::EVIDENCE_SESSION_TOKEN_ENV),
        Some(&("lw-evidence".to_string(), "etoken".to_string())),
        "the evidence grant's own token is projected, on its own variable"
    );
}

/// A destination that declares no `evidenceWrite` **enforces**, with the
/// `archiveWrite` grant — the documented default, unchanged by this branch.
///
/// **THE FALL-BACK IS `archiveWrite` AND HAS NEVER BEEN `archiveRead`.** That
/// is the whole of defect RET-EVIDENCE-GRANT-IS-ARCHIVEREAD: the build put the
/// READ-ONLY principal on the record variables, so a destination separating the
/// two had every intent tombstone refused `403`. `docs/kubernetes.md` §7 and
/// `docs/install.md` both say *absent grants do not widen — `archiveRead` and
/// `evidenceWrite` absent mean `archiveWrite` is used*, and an installation that
/// never separated its principals does not move on an upgrade. This row is what
/// holds that sentence true.
#[tokio::test]
async fn a_destination_with_no_evidence_write_grant_uses_archive_write_and_never_archive_read() {
    let mut access = four_principals();
    access
        .as_object_mut()
        .expect("access")
        .remove("evidenceWrite");
    let env = rendered_job_credentials(destination_with_access(access)).await;

    assert_eq!(
        env.get(weirkeeper::destination::EVIDENCE_ACCESS_KEY_ID_ENV),
        Some(&("lw-writer".to_string(), "id".to_string())),
        "absent evidenceWrite defaults to archiveWrite (D2 §3.4), and the Job is created. \
         Rendered: {env:?}"
    );
    assert_eq!(
        env.get(weirkeeper::destination::EVIDENCE_SECRET_ACCESS_KEY_ENV),
        Some(&("lw-writer".to_string(), "key".to_string()))
    );
    let named: BTreeSet<&str> = env.values().map(|(secret, _)| secret.as_str()).collect();
    assert!(
        !named.contains("lw-reader"),
        "and NEVER the archiveRead Secret, on any variable, however the evidence role \
         defaulted: {env:?}"
    );
}

/// A `WorkloadIdentity` `evidenceWrite` grant is a role that resolves to
/// nothing a Job can use: `logweir-retention`'s `EvidenceSink::open` builds its
/// sink with `StoreOptions::static_keys` and has no workload-identity path, so
/// the Job could only ever refuse itself at exit 3 — after spending a lease, a
/// `ConfigMap`, a record and one of three retry-budget slots.
///
/// **AND THE OBJECT STOPS CLAIMING DELETION** — review finding M1. Nothing runs
/// here until a human edits the `BackupDestination`, so `status.enforcement`
/// and `status.guarantees.ageExpiry` — the two fields the console renders, not
/// the condition reason — say so too, and the four guarantees this refusal does
/// not falsify survive the object-wise merge.
#[tokio::test]
async fn a_workload_identity_evidence_grant_creates_no_job_and_claims_no_enforcement() {
    let mut access = four_principals();
    access["evidenceWrite"] = json!({
        "mode": "WorkloadIdentity",
        "workloadIdentity": {"serviceAccountName": "lw-evidence-writer"}
    });
    let object = destination_with_access(access);

    let learn = fixture(routes_for_destination(&six_points(), object.clone()));
    run(&learn, &policy(enforcing(None), json!({}))).await;
    let digest = learn.status()["lastEvaluation"]["planSha256"]
        .as_str()
        .expect("a digest")
        .to_string();

    let mut routes = routes_for_destination(&six_points(), object);
    routes.push(plan_config_map_route(&digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.extend(absent_job_routes(&digest, now()));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let f = fixture(routes);
    let outcome = run(&f, &policy(enforcing(Some(&digest)), json!({}))).await;

    assert_eq!(
        outcome.enforced_reason,
        ctrl::REASON_EVIDENCE_GRANT_UNUSABLE
    );
    assert!(f.posted("/jobs").is_empty());
    let message = f.condition(ctrl::CONDITION_ENFORCED)["message"]
        .as_str()
        .expect("a message")
        .to_string();
    assert!(
        message.contains("spec.access.evidenceWrite") && message.contains("WorkloadIdentity"),
        "the message names the field AND the mode the operator wrote: {message}"
    );
    assert!(
        !message.contains("lw-evidence-writer"),
        "and never the ServiceAccount name — `ResolvedGrant`'s `Debug` carries names a status \
         field must not: {message}"
    );

    // M1. `publish_evaluation` wrote `LogweirWorker`/`LogweirEnforced` earlier
    // in the same pass, and `ui/render.js` renders those two fields as "an
    // isolated Logweir retention worker deletes archive objects under this
    // policy" and "enforced by Logweir".
    let status = f.status();
    assert_eq!(
        status["enforcement"],
        json!(ctrl::ENFORCEMENT_RECOMMENDATION_ONLY),
        "a policy that will not create a Job until a human edits the destination does not \
         report that a worker is deleting under it: {status}"
    );
    assert_eq!(
        outcome.enforcement,
        ctrl::ENFORCEMENT_RECOMMENDATION_ONLY,
        "and the returned Outcome agrees with the object"
    );
    assert_eq!(
        status["guarantees"]["ageExpiry"],
        json!(ctrl::GUARANTEE_NOT_ENFORCED)
    );
    assert_eq!(
        status["guarantees"]["minUsablePoints"],
        json!(ctrl::GUARANTEE_LOGWEIR),
        "the guarantees this refusal does not falsify survive: `guarantees` is patched \
         object-wise and names ONE key (RFC 7386 merges member by member): {status}"
    );
    assert_eq!(
        status["guarantees"]["legalHold"],
        json!(ctrl::GUARANTEE_PROVIDER_UNVERIFIED)
    );
    assert_eq!(
        f.condition(ctrl::CONDITION_DEGRADED)["status"],
        json!("False"),
        "and the full condition array is still upserted, so nothing is dropped"
    );
}

/// When the role DEFAULTED, the refusal names the line the operator actually
/// wrote — `spec.access.archiveWrite`, not an `evidenceWrite` that is not on
/// the object.
///
/// Telling someone to fix a field their YAML does not contain sends them
/// looking for a line that is not there. This is the only thing
/// `destination::declares` decides now that the default itself stands.
#[tokio::test]
async fn a_defaulted_record_grant_that_no_job_can_use_names_archive_write() {
    let mut access = four_principals();
    access
        .as_object_mut()
        .expect("access")
        .remove("evidenceWrite");
    access["archiveWrite"] = json!({
        "mode": "WorkloadIdentity",
        "workloadIdentity": {"serviceAccountName": "lw-archive-writer"}
    });
    let object = destination_with_access(access);

    let learn = fixture(routes_for_destination(&six_points(), object.clone()));
    run(&learn, &policy(enforcing(None), json!({}))).await;
    let digest = learn.status()["lastEvaluation"]["planSha256"]
        .as_str()
        .expect("a digest")
        .to_string();

    let mut routes = routes_for_destination(&six_points(), object);
    routes.push(plan_config_map_route(&digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.extend(absent_job_routes(&digest, now()));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let f = fixture(routes);
    let outcome = run(&f, &policy(enforcing(Some(&digest)), json!({}))).await;

    assert_eq!(
        outcome.enforced_reason,
        ctrl::REASON_EVIDENCE_GRANT_UNUSABLE
    );
    assert!(f.posted("/jobs").is_empty());
    let message = f.condition(ctrl::CONDITION_ENFORCED)["message"]
        .as_str()
        .expect("a message")
        .to_string();
    assert!(
        message.contains("spec.access.archiveWrite")
            && message.contains("spec.access.evidenceWrite is absent and defaults to it"),
        "the message names the field that exists AND says why it is the one being read: \
         {message}"
    );
}

/// The record credential naming the SAME Secret as the delete credential is the
/// one shape of the forbidden aggregation a controller can actually see —
/// review finding L3.
///
/// One principal that both removes a point and writes the record attributing
/// its removal can forge that record, and §7f's rule is that neither alone is
/// enough. The check compares NAMES, which catches the spelling and not the
/// material — two differently named Secrets can hold identical keys and this
/// controller reads neither — and the message says exactly that rather than
/// reading as a proof of distinctness.
#[tokio::test]
async fn the_record_credential_naming_the_delete_secret_creates_no_job() {
    let mut access = four_principals();
    access["evidenceWrite"]["secret"]["name"] = json!("retention-delete");
    let object = destination_with_access(access);

    let learn = fixture(routes_for_destination(&six_points(), object.clone()));
    run(&learn, &policy(enforcing(None), json!({}))).await;
    let digest = learn.status()["lastEvaluation"]["planSha256"]
        .as_str()
        .expect("a digest")
        .to_string();

    let mut routes = routes_for_destination(&six_points(), object);
    routes.push(plan_config_map_route(&digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.extend(absent_job_routes(&digest, now()));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let f = fixture(routes);
    let outcome = run(&f, &policy(enforcing(Some(&digest)), json!({}))).await;

    assert_eq!(
        outcome.enforced_reason,
        ctrl::REASON_EVIDENCE_GRANT_UNUSABLE
    );
    assert!(
        f.seen().iter().all(|(m, _)| m != "POST"),
        "no Job and no plan ConfigMap: {:?}",
        f.seen()
    );
    let message = f.condition(ctrl::CONDITION_ENFORCED)["message"]
        .as_str()
        .expect("a message")
        .to_string();
    assert!(
        message.contains("spec.enforcement.credentialSecretRef"),
        "the condition names the other half of the collision: {message}"
    );
    assert!(
        message.contains("NAMES only"),
        "and says what it does NOT catch, so nobody reads it as a proof that the two \
         principals are distinct: {message}"
    );
}

/// `Report` needs no record credential, so a destination whose `evidenceWrite`
/// role resolves to nothing a Job could use still previews.
///
/// The refusal lives in `start_run` and not in `evaluate` for this reason: a
/// policy that deletes nothing has no deletion to attribute, and refusing its
/// preview would tell an operator to fix a field their policy does not use.
#[tokio::test]
async fn a_report_mode_policy_needs_no_evidence_write_grant() {
    let mut access = four_principals();
    access["evidenceWrite"] = json!({
        "mode": "WorkloadIdentity",
        "workloadIdentity": {"serviceAccountName": "lw-evidence-writer"}
    });
    let f = fixture(routes_for_destination(
        &six_points(),
        destination_with_access(access),
    ));
    let outcome = run(&f, &policy(json!({}), json!({}))).await;

    assert_eq!(outcome.phase, ctrl::RetentionPhase::Evaluated);
    assert_eq!(outcome.ready_reason, ctrl::REASON_POLICY_READY);
    assert_eq!(outcome.enforced_reason, ctrl::REASON_RECOMMENDATION_ONLY);
    assert!(outcome.points_evaluated > 0);
}

/// A malformed `evidenceWrite` grant does not take a `Report` policy's
/// evaluation down with it, and in `Enforce` it is the refusal it always was —
/// carrying the resolver's own sentence.
///
/// The second role is resolved from the same read, so its refusal is available
/// on every pass; it is READ where the credential is needed. Surfacing it at
/// the resolve would have made `Ready=False/DestinationUnusable` out of a field
/// a `Report` policy never reads, which is a preview an operator loses over a
/// deletion they did not ask for.
#[tokio::test]
async fn a_malformed_evidence_write_grant_stops_enforcement_and_not_the_preview() {
    let mut access = four_principals();
    access["evidenceWrite"]["secret"]["name"] = json!("");
    let object = destination_with_access(access);

    let report = fixture(routes_for_destination(&six_points(), object.clone()));
    let previewed = run(&report, &policy(json!({}), json!({}))).await;
    assert_eq!(previewed.phase, ctrl::RetentionPhase::Evaluated);
    assert_eq!(previewed.ready_reason, ctrl::REASON_POLICY_READY);
    assert!(
        previewed.points_evaluated > 0,
        "a preview reads no record credential, so it is still a preview"
    );

    let digest = report.status()["lastEvaluation"]["planSha256"]
        .as_str()
        .expect("a digest")
        .to_string();
    let mut routes = routes_for_destination(&six_points(), object);
    routes.push(plan_config_map_route(&digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.extend(absent_job_routes(&digest, now()));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let f = fixture(routes);
    let outcome = run(&f, &policy(enforcing(Some(&digest)), json!({}))).await;

    assert_eq!(
        outcome.enforced_reason,
        ctrl::REASON_EVIDENCE_GRANT_UNUSABLE
    );
    assert!(f.posted("/jobs").is_empty());
    let message = f.condition(ctrl::CONDITION_ENFORCED)["message"]
        .as_str()
        .expect("a message")
        .to_string();
    assert!(
        message.contains("evidenceWrite") && message.contains("nothing was deleted"),
        "the resolver's own sentence, and the reassurance every retention refusal owes: \
         {message}"
    );
}

/// `evidence_credential` itself, at every answer a Job can get — the pure half
/// of the rows above.
#[test]
fn the_record_credential_is_decided_in_one_pure_place() {
    let keys =
        |name: &str, token: Option<&str>| weirkeeper::destination::ResolvedGrant::SecretKeys {
            secret: name.to_string(),
            access_key_id_key: "eid".to_string(),
            secret_access_key_key: "ekey".to_string(),
            session_token_key: token.map(str::to_string),
        };
    let projected = ctrl::evidence_credential(
        Ok(&resolved_for(keys("lw-evidence", None))),
        true,
        Some("retention-delete"),
    )
    .expect("projected");
    assert_eq!(
        projected.len(),
        2,
        "two variables when no token is declared"
    );
    let with_token = ctrl::evidence_credential(
        Ok(&resolved_for(keys("lw-evidence", Some("t")))),
        true,
        Some("retention-delete"),
    )
    .expect("projected");
    assert_eq!(with_token.len(), 3);

    // THE DOCUMENTED DEFAULT STANDS. `evidenceWrite` absent means
    // `archiveWrite`, exactly as `docs/kubernetes.md` §7 and `docs/install.md`
    // say — an installation that never separated its principals does not move
    // on an upgrade, and this row is what says so.
    let defaulted = ctrl::evidence_credential(
        Ok(&resolved_for(keys("lw-writer", None))),
        false,
        Some("retention-delete"),
    )
    .expect("an absent evidenceWrite is the archiveWrite grant, not a refusal");
    assert_eq!(defaulted[0].secret_name, "lw-writer");

    let workload = ctrl::evidence_credential(
        Ok(&resolved_for(
            weirkeeper::destination::ResolvedGrant::WorkloadIdentity {
                service_account_name: "sa".to_string(),
            },
        )),
        true,
        Some("retention-delete"),
    )
    .expect_err("a grant with no static keys is refused");
    assert!(workload.contains("WorkloadIdentity") && !workload.contains("\"sa\""));
    assert!(
        workload.contains("spec.access.evidenceWrite"),
        "the field the operator wrote: {workload}"
    );
    let defaulted_workload = ctrl::evidence_credential(
        Ok(&resolved_for(
            weirkeeper::destination::ResolvedGrant::WorkloadIdentity {
                service_account_name: "sa".to_string(),
            },
        )),
        false,
        Some("retention-delete"),
    )
    .expect_err("the default is refused too when IT has no static keys");
    assert!(
        defaulted_workload.contains("spec.access.archiveWrite"),
        "and then the message names archiveWrite, because that is the line the operator would \
         have to edit: {defaulted_workload}"
    );

    // THE ONE AGGREGATION A CONTROLLER CAN SEE — review finding L3.
    let same = ctrl::evidence_credential(
        Ok(&resolved_for(keys("retention-delete", None))),
        true,
        Some("retention-delete"),
    )
    .expect_err("the record credential is not the delete credential");
    assert!(
        same.contains("credentialSecretRef") && same.contains("NAMES only"),
        "and the message says what it does not catch: {same}"
    );
    ctrl::evidence_credential(
        Ok(&resolved_for(keys("retention-delete", None))),
        true,
        None,
    )
    .expect("with no delete credential configured there is nothing to compare");
}

/// A `ResolvedDestination` carrying `grant`, for the pure row above.
fn resolved_for(
    grant: weirkeeper::destination::ResolvedGrant,
) -> weirkeeper::destination::ResolvedDestination {
    let dest: weirkeeper::crds::backup_destination::BackupDestination =
        serde_json::from_str(&destination_with_access(four_principals()))
            .expect("the fixture is a destination");
    let mut resolved = weirkeeper::destination::resolve(
        &dest,
        weirkeeper::destination::DestinationRole::EvidenceWrite,
        &check::policy::Policy::defaults(),
    )
    .expect("the fixture resolves");
    resolved.grant = grant;
    resolved
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
    routes.extend(absent_job_routes(&digest, now()));
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
    routes.extend(absent_job_routes(&digest, now()));
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

/// A `Restore` that names its archive by URL — the LEGACY shape.
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

/// A `Restore` that names its destination BY REFERENCE — the D2/D3 shape, and
/// the one the first landing could not see at all (review `d3w9` C2).
///
/// `crds/restore.rs`'s CEL rule forces `sourceArchive.url` to the sentinel
/// `logweir-destination://<name>` whenever `sourceDestinationRef` is set, so a
/// URL comparison cannot rescue this shape either.
fn destination_backed_restore(destination: &str, set: &str, phase: Option<&str>) -> Value {
    let mut restore = restore_body(set, phase);
    restore["spec"]["sourceDestinationRef"] = json!({"name": destination});
    restore["spec"]["evidenceDestinationRef"] = json!({"name": destination});
    restore["spec"]["sourceArchive"] =
        json!({"url": format!("logweir-destination://{destination}")});
    restore
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
        // The run's own key lines — the controller reads them through the same
        // owner-UID pod path it reads the exit code through (review `d3w9` H2).
        route(
            "GET",
            "/log",
            "retention-point=p6 state=Deleted objects=4\n\
             retention-result=deleted=1 failed=0 objects=4\n"
                .to_string(),
        ),
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
            "GET",
            "/log",
            "retention-point=p6 state=Kept objects=0 code=AccessDenied\n\
             retention-result=deleted=0 failed=1 objects=0\n"
                .to_string(),
        ),
        route(
            "PATCH",
            "/retentionpolicies/primary/status",
            policy_value(json!({}), json!({})).to_string(),
        ),
        route("PATCH", leaked, "{}".to_string()),
    ];
    let f = fixture(routes);
    // `observedGeneration` MATTERS TO THIS ROW NOW: a spec change releases the
    // consecutive-failure budget, so a status carrying a count and NO observed
    // generation means "the spec changed since the count was taken" and the
    // count is zero. The controller writes both fields in the same patch, so a
    // count without a generation is not a state it can produce — this fixture
    // now says which generation the count belongs to.
    let status = json!({
        "lastEnforcement": {"runId": run_id, "startedAt": "2026-09-17T04:00:00Z"},
        "consecutiveRunFailures": 2,
        "observedGeneration": 4
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

// ===========================================================================
// FIX ROUND 1 — one row per finding, each failing without its fix
// ===========================================================================

/// **C1, through the reconciler.** Two passes at DIFFERENT instants publish one
/// digest, and approving it starts exactly one Job.
///
/// The first landing's `approving_the_published_digest_creates_exactly_one_job`
/// ran both passes at the frozen `now()`, so it asserted nothing about the
/// clock — which is why the defect survived it. Here the second pass is five
/// minutes later, which is longer than `IDLE_REQUEUE_SECONDS`.
#[tokio::test]
async fn two_passes_at_different_instants_publish_one_digest_and_approving_it_starts_one_job() {
    let first = fixture(happy_routes(&six_points()));
    run(&first, &policy(enforcing(None), json!({}))).await;
    let digest_at_0417 = first.status()["lastEvaluation"]["planSha256"]
        .as_str()
        .expect("a digest")
        .to_string();

    let second = fixture(happy_routes(&six_points()));
    run_at(
        &second,
        &policy(enforcing(None), json!({})),
        now() + chrono::Duration::minutes(5),
    )
    .await;
    let digest_later = second.status()["lastEvaluation"]["planSha256"]
        .as_str()
        .expect("a digest")
        .to_string();

    assert_eq!(
        digest_at_0417, digest_later,
        "the digest must not move with the clock: an administrator copies \
         `status.lastEvaluation.planSha256` onto the spec, and the copy itself triggers a \
         reconcile. If the two differ there is no ordering of events in which an approval ever \
         matches, and `mode: Enforce` can never create a Job."
    );

    // And the digest learned at one instant authorises a run at another.
    let mut routes = happy_routes(&six_points());
    routes.push(plan_config_map_route(&digest_at_0417));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.extend(absent_job_routes(
        &digest_at_0417,
        now() + chrono::Duration::minutes(5),
    ));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let third = fixture(routes);
    let outcome = run_at(
        &third,
        &policy(enforcing(Some(&digest_at_0417)), json!({})),
        now() + chrono::Duration::minutes(5),
    )
    .await;
    assert_eq!(outcome.phase, ctrl::RetentionPhase::Started);
    assert_eq!(third.posted("/jobs").len(), 1, "exactly one Job");
}

/// **C2.** A nonterminal `Restore` that names this destination BY REFERENCE
/// protects its point and stops the run.
///
/// Every row in the first landing built the `Restore` with `sourceArchive.url`
/// only, so this branch — the one the whole D2/D3 design steers toward — had
/// zero coverage, and the comparison it used (`location_id.contains(name)`)
/// could never be true: `location_id` is `s3://<bucket>/<prefix>`, and the CEL
/// rule forces the URL to the `logweir-destination://` sentinel whenever the
/// reference is set.
#[tokio::test]
async fn a_destination_backed_restore_protects_its_point_and_blocks_the_run() {
    let mut routes = happy_routes(&six_points());
    routes.retain(|r| r.path_suffix != "/restores");
    routes.push(route(
        "GET",
        "/restores",
        restore_list(vec![destination_backed_restore(
            DEST,
            "set-p6",
            Some("Running"),
        )]),
    ));
    let f = fixture(routes);
    let outcome = run(&f, &policy(enforcing(None), json!({}))).await;

    assert_eq!(
        outcome.candidates, 2,
        "p6 is protected, so two candidates remain. A restore naming its destination by \
         reference is the D2/D3 path, and the reaper would otherwise delete the manifest and \
         segments of a set a live restore is reading."
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

/// **C2, the negative control.** The same restore in ANOTHER namespace protects
/// nothing: a `destinationRef` is namespace-local, so the same name elsewhere
/// is a different object.
#[tokio::test]
async fn a_destination_backed_restore_in_another_namespace_protects_nothing() {
    let mut routes = happy_routes(&six_points());
    routes.retain(|r| r.path_suffix != "/restores");
    let mut elsewhere = destination_backed_restore(DEST, "set-p6", Some("Running"));
    elsewhere["metadata"]["namespace"] = json!("some-other-namespace");
    routes.push(route("GET", "/restores", restore_list(vec![elsewhere])));
    let f = fixture(routes);
    let outcome = run(&f, &policy(json!({}), json!({}))).await;
    assert_eq!(outcome.candidates, 3);
}

/// **C2, as a table over the pure predicate.** The substring comparison the
/// first landing used was true or false by accident of spelling.
#[test]
fn restore_touches_compares_names_and_never_substrings() {
    let named = destination_backed_restore(DEST, "set-a", Some("Running"));
    let named: weirkeeper::crds::restore::Restore =
        serde_json::from_value(named).expect("a Restore");
    assert!(
        ctrl::restore_touches(&named, DEST, LOCATION, NS),
        "the reference names this destination in this namespace"
    );
    assert!(
        !ctrl::restore_touches(&named, "another-destination", LOCATION, NS),
        "a different destination of the same location is not this one"
    );
    assert!(
        !ctrl::restore_touches(&named, DEST, LOCATION, "elsewhere"),
        "a destinationRef is namespace-local"
    );

    // The substring trap, stated: a destination NAMED `kafka` would have
    // matched `s3://lw-archive/kafka-backups/…` under the old comparison, and a
    // destination named `archive` would not have matched its own URL at all.
    let mut coincidence = destination_backed_restore("kafka", "set-a", Some("Running"));
    coincidence["spec"]["sourceDestinationRef"] = json!({"name": "kafka"});
    let coincidence: weirkeeper::crds::restore::Restore =
        serde_json::from_value(coincidence).expect("a Restore");
    assert!(
        !ctrl::restore_touches(&coincidence, DEST, "s3://lw-archive/kafka-backups", NS),
        "a destination whose NAME happens to appear in another destination's URL is still a \
         different destination"
    );

    // The legacy shape keeps URL equality, and only it.
    let legacy: weirkeeper::crds::restore::Restore =
        serde_json::from_value(restore_body("set-a", Some("Running"))).expect("a Restore");
    assert!(ctrl::restore_touches(&legacy, DEST, LOCATION, NS));
    assert!(!ctrl::restore_touches(
        &legacy,
        DEST,
        "s3://other-bucket/team-b",
        NS
    ));
}

/// **C3.** A status conflict means no Job — the lease did not land, so nothing
/// is holding these points.
///
/// The first landing mapped 409 to `Ok(())`. Every enforcing pass patched three
/// times with the same, already-superseded version: the evaluation landed, the
/// LEASE 409'd and was discarded, and the Job was created anyway.
#[tokio::test]
async fn a_status_conflict_creates_no_job() {
    let digest = {
        let learn = fixture(happy_routes(&six_points()));
        run(&learn, &policy(enforcing(None), json!({}))).await;
        learn.status()["lastEvaluation"]["planSha256"]
            .as_str()
            .expect("a digest")
            .to_string()
    };
    let mut routes = happy_routes(&six_points());
    routes.retain(|r| r.path_suffix != "/retentionpolicies/primary/status");
    routes.push(Route {
        method: "PATCH",
        path_suffix: "/retentionpolicies/primary/status",
        status: 409,
        body: json!({
            "kind": "Status", "apiVersion": "v1", "status": "Failure",
            "reason": "Conflict", "code": 409,
            "message": "the object has been modified"
        })
        .to_string(),
    });
    routes.push(plan_config_map_route(&digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.extend(absent_job_routes(&digest, now()));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let f = fixture(routes);
    let outcome = run(&f, &policy(enforcing(Some(&digest)), json!({}))).await;

    assert!(
        f.seen().iter().all(|(m, _)| m != "POST"),
        "a run whose lease did not land is a run nothing is holding points for, and it must not \
         start. Requests: {:?}",
        f.seen()
    );
    assert_eq!(outcome.enforced_reason, ctrl::REASON_LEASE_NOT_HELD);
}

/// **C3, the other half.** Each status PATCH preconditions on the version the
/// LAST one returned, not on the one the watcher delivered.
#[tokio::test]
async fn each_status_patch_carries_the_version_the_last_one_returned() {
    let digest = {
        let learn = fixture(happy_routes(&six_points()));
        run(&learn, &policy(enforcing(None), json!({}))).await;
        learn.status()["lastEvaluation"]["planSha256"]
            .as_str()
            .expect("a digest")
            .to_string()
    };
    // The API server answers every status write with a NEW version, exactly as
    // a real one does.
    let mut bumped = policy_value(json!({}), json!({}));
    bumped["metadata"]["resourceVersion"] = json!("9001");
    let mut routes = happy_routes(&six_points());
    routes.retain(|r| r.path_suffix != "/retentionpolicies/primary/status");
    routes.push(route(
        "PATCH",
        "/retentionpolicies/primary/status",
        bumped.to_string(),
    ));
    routes.push(plan_config_map_route(&digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.extend(absent_job_routes(&digest, now()));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let f = fixture(routes);
    run(&f, &policy(enforcing(Some(&digest)), json!({}))).await;

    let versions: Vec<String> = f
        .status_patches()
        .iter()
        .filter_map(|p| {
            p["metadata"]["resourceVersion"]
                .as_str()
                .map(str::to_string)
        })
        .collect();
    assert!(
        versions.len() >= 2,
        "an enforcing pass patches more than once"
    );
    assert_eq!(
        versions[0], "4242",
        "the first patch preconditions on what the watcher delivered"
    );
    // THE DOUBLE ENFORCES SEAM S7 (REHEARSAL-FIRE-PASS-STATUS-LOST): the route
    // moves the object to 9001 on the first write, and every accepted write
    // after it moves the object one further, as a real API server does — so
    // each later patch must carry exactly what the previous one returned.
    assert_eq!(versions[1], "9001", "Got: {versions:?}");
    for pair in versions[1..].windows(2) {
        let previous: u64 = pair[0].parse().expect("a numeric version");
        assert_eq!(
            pair[1],
            (previous + 1).to_string(),
            "and every later one on what the API server returned — a reconciler that holds no \
             `get` on its own kind has no other fresh version. Got: {versions:?}"
        );
    }
}

/// **H1.** `sharedSegments` reports `NotEnforced` while the view carries no
/// segment keys, and the condition says why.
///
/// Asserted through `point_facts` over a real `ViewEntry`, which is the
/// production shape: the unit rows above feed `segment_keys` straight into
/// `PointFacts`, which no view can do.
#[tokio::test]
async fn shared_segment_protection_is_reported_not_enforced() {
    let entry: weirkeeper::catalog_view::ViewEntry =
        serde_json::from_value(view_entry("p1", 1)).expect("a view entry");
    let facts = ctrl::point_facts(&entry, &Default::default());
    assert!(
        facts.segment_keys.is_empty(),
        "the catalog view entry has no segment field at all, so there is nothing to protect with"
    );

    let f = fixture(happy_routes(&six_points()));
    run(&f, &policy(json!({}), json!({}))).await;
    assert_eq!(
        f.status()["guarantees"]["sharedSegments"],
        ctrl::GUARANTEE_NOT_ENFORCED,
        "writing LogweirEnforced for a rule that has nothing to apply is the withdrawn-guarantee \
         defect class on a status field"
    );
    let message = f.condition(ctrl::CONDITION_EVALUATED)["message"]
        .as_str()
        .expect("a message")
        .to_string();
    assert!(
        message.contains("no segment keys"),
        "and the condition says why: {message}"
    );
    // SHARED-SET-RETENTION: the half that IS in force is said too, and the
    // message is one sentence stream — no run of source indentation inside it.
    assert!(
        message.contains("protected together (SharedSegment, matched on backupId)"),
        "{message}"
    );
    assert!(!message.contains("  "), "{message:?}");
}

/// SHARED-SET-RETENTION through the production path: two VIEW ENTRIES naming
/// one `backupId` (the live shape — a second receipt over the same set) reach
/// the evaluation through `point_facts`, and the published status protects the
/// older one `SharedSegment` instead of listing it as a candidate.
///
/// MUTANT: `point_facts` drops `backup_id` (an empty string). The two entries
/// no longer link, `p5` is a candidate, and this row fails.
#[tokio::test]
async fn a_second_receipt_over_a_kept_set_is_protected_on_the_object() {
    let mut entries = six_points();
    // p5 is a second receipt over p1's set: same backupId, same manifest.
    entries[4]["backupId"] = json!("set-p1");
    entries[4]["manifestKey"] = json!(format!("{SCOPE}/set-p1/manifest.json"));
    let f = fixture(happy_routes(&entries));
    run(&f, &policy(json!({}), json!({}))).await;
    let status = f.status();
    let candidates: Vec<&str> = status["lastEvaluation"]["candidates"]
        .as_array()
        .expect("candidates")
        .iter()
        .filter_map(|c| c["pointId"].as_str())
        .collect();
    assert_eq!(candidates, vec!["p4", "p6"], "{status}");
    assert!(
        status["lastEvaluation"]["protected"]
            .as_array()
            .expect("protected")
            .iter()
            .any(|p| p["pointId"] == "p5" && p["reason"] == "SharedSegment"),
        "{status}"
    );
    assert_eq!(
        status["guarantees"]["sharedSegments"],
        ctrl::GUARANTEE_NOT_ENFORCED,
        "set-level protection is not the segment-level guarantee; it is not claimed as one"
    );
}

/// **H2.** A finished run records what it deleted, what it could not, and where
/// the record is — from its own key lines, through the owner-UID pod path.
#[tokio::test]
async fn a_finished_run_records_what_it_deleted_and_what_it_could_not() {
    let run_id = "r00000000deadbee4";
    let job_name = format!("{}-{run_id}", stem());
    let leaked: &'static str = Box::leak(format!("/jobs/{job_name}").into_boxed_str());
    let log = "retention-plan=sha256:aa points=2 objects=4\n\
               retention-point=p5 state=Deleted objects=2\n\
               retention-point=p6 state=Kept objects=0 code=Locked\n\
               retention-record=logweir/retention/uid/r00000000deadbee4.json sha256=sha256:bb\n\
               retention-result=deleted=1 failed=1 objects=2\n";
    let routes = vec![
        route("GET", "/retentionpolicies", policy_list(vec![])),
        route("GET", leaked, job_body(&job_name, true)),
        route("GET", "/pods", pod_list(1)),
        route("GET", "/log", log.to_string()),
        route(
            "PATCH",
            "/retentionpolicies/primary/status",
            policy_value(json!({}), json!({})).to_string(),
        ),
        route("PATCH", leaked, "{}".to_string()),
    ];
    let f = fixture(routes);
    let status = json!({
        "lastEnforcement": {"runId": run_id, "startedAt": "2026-09-17T04:00:00Z"}
    });
    run(&f, &policy(enforcing(None), status)).await;

    let last = &f.status()["lastEnforcement"];
    assert_eq!(last["deleted"], json!(["p5"]));
    assert_eq!(last["failed"], json!([{"pointId": "p6", "code": "Locked"}]));
    assert_eq!(last["objectsDeleted"], 2);
    assert_eq!(
        last["recordKey"],
        "logweir/retention/uid/r00000000deadbee4.json"
    );
    assert_eq!(
        last["recordSha256"], "sha256:bb",
        "W14's L9 compares the record's bytes against this field; without it there is nothing \
         to compare against"
    );
}

/// **H2, the consequence.** A recorded provider refusal protects that point on
/// the next pass — D3 §6.5's "excluded from the next plan until the reason
/// clears", which could not fire while `failed[]` was never written.
#[tokio::test]
async fn a_recorded_provider_refusal_protects_that_point_on_the_next_pass() {
    let f = fixture(happy_routes(&six_points()));
    let status = json!({
        "lastEnforcement": {
            "runId": "r0", "finishedAt": "2026-09-17T04:00:00Z", "exitCode": 1,
            "failed": [{"pointId": "p6", "code": "Locked"}]
        }
    });
    let outcome = run(&f, &policy(json!({}), status)).await;
    assert_eq!(outcome.candidates, 2, "p6 is excluded from this plan");
    let protected = f.status()["lastEvaluation"]["protected"]
        .as_array()
        .expect("protected")
        .clone();
    assert!(
        protected
            .iter()
            .any(|p| p["pointId"] == "p6" && p["reason"] == "LegalHold"),
        "protected: {protected:?}"
    );
}

/// OBJECT-LOCK-DELETE-MARKER: a point the worker refused because the bucket
/// is VERSIONED is not relabelled a legal hold. Nothing established that it is
/// held — only that a delete by key would have been a marker — so it stays a
/// candidate, the next run is refused the same way, and the retry budget
/// (`EnforcementDegraded` after three) is what stops enforcement on a bucket
/// this worker does not support. `LegalHold` stays the provider's verdict.
///
/// MUTANT: add `VersionedBucket` to `previously_refused`'s codes. `p6` is
/// protected `LegalHold` and this row fails.
#[tokio::test]
async fn a_versioned_bucket_refusal_is_not_recorded_as_a_legal_hold() {
    let f = fixture(happy_routes(&six_points()));
    let status = json!({
        "lastEnforcement": {
            "runId": "r0", "finishedAt": "2026-09-17T04:00:00Z", "exitCode": 1,
            "failed": [{"pointId": "p6", "code": "VersionedBucket"}]
        }
    });
    let outcome = run(&f, &policy(json!({}), status)).await;
    assert_eq!(outcome.candidates, 3, "p6 is still planned: {}", f.status());
    assert!(!f.status()["lastEvaluation"]["protected"]
        .as_array()
        .expect("protected")
        .iter()
        .any(|p| p["pointId"] == "p6"));
    assert_eq!(
        f.status()["guarantees"]["legalHold"],
        ctrl::GUARANTEE_PROVIDER_UNVERIFIED
    );
}

/// **H4.** An approved plan older than `planMaxAgeSeconds` starts no Job.
#[tokio::test]
async fn an_expired_plan_starts_no_job() {
    let digest = {
        let learn = fixture(happy_routes(&six_points()));
        run(&learn, &policy(enforcing(None), json!({}))).await;
        learn.status()["lastEvaluation"]["planSha256"]
            .as_str()
            .expect("a digest")
            .to_string()
    };
    let mut routes = happy_routes(&six_points());
    routes.push(plan_config_map_route(&digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.extend(absent_job_routes(&digest, now()));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let f = fixture(routes);
    // The status remembers the same digest with a window that closed an hour
    // ago — `planMaxAgeSeconds` is 3600 in the fixture.
    let status = json!({
        "lastEvaluation": {
            "planSha256": digest,
            "planExpiresAt": "2026-09-17T03:00:00Z"
        }
    });
    let outcome = run(&f, &policy(enforcing(Some(&digest)), status)).await;

    assert_eq!(outcome.enforced_reason, ctrl::REASON_PLAN_EXPIRED);
    assert!(
        f.seen().iter().all(|(m, _)| m != "POST"),
        "one of D3 §6.5's four gates, and the first landing declared the reason and never \
         emitted it. Requests: {:?}",
        f.seen()
    );
    // And the window re-anchors, so the policy is not wedged forever.
    let expires: chrono::DateTime<chrono::Utc> =
        serde_json::from_value(f.status()["lastEvaluation"]["planExpiresAt"].clone())
            .expect("an instant");
    assert!(expires > now(), "the re-anchored window is in the future");
}

/// **H4, the negative control.** A window that is still open starts the run.
#[tokio::test]
async fn a_plan_inside_its_window_starts() {
    let digest = {
        let learn = fixture(happy_routes(&six_points()));
        run(&learn, &policy(enforcing(None), json!({}))).await;
        learn.status()["lastEvaluation"]["planSha256"]
            .as_str()
            .expect("a digest")
            .to_string()
    };
    let mut routes = happy_routes(&six_points());
    routes.push(plan_config_map_route(&digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.extend(absent_job_routes(&digest, now()));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let f = fixture(routes);
    let status = json!({
        "lastEvaluation": {"planSha256": digest, "planExpiresAt": "2026-09-17T05:00:00Z"}
    });
    let outcome = run(&f, &policy(enforcing(Some(&digest)), status)).await;
    assert_eq!(outcome.phase, ctrl::RetentionPhase::Started);
}

/// **M1.** The preview omits the object count it cannot observe, rather than
/// publishing `1`.
#[tokio::test]
async fn the_preview_omits_the_object_count_it_cannot_observe() {
    let f = fixture(happy_routes(&six_points()));
    run(&f, &policy(json!({}), json!({}))).await;
    let candidates = f.status()["lastEvaluation"]["candidates"]
        .as_array()
        .expect("candidates")
        .clone();
    assert!(!candidates.is_empty());
    for candidate in &candidates {
        assert!(
            candidate["objects"].is_null(),
            "the view carries no segment keys, so `1` — the manifest and nothing else — would \
             tell an administrator that a three-line plan removes three objects while the run \
             removes several thousand. Absent is `not observed`, which is this API's rule \
             everywhere else. Got: {candidate}"
        );
    }
    let message = f.condition(ctrl::CONDITION_EVALUATED)["message"]
        .as_str()
        .expect("a message")
        .to_string();
    assert!(
        message.contains("does not enumerate") || message.contains("omitted"),
        "and the condition says the plan does not enumerate: {message}"
    );
}

/// **M2.** A run that stopped on its own object ceiling is not counted as a
/// failure, so three of them do not stop retention for good.
#[tokio::test]
async fn a_budget_bounded_run_does_not_count_toward_degradation() {
    let run_id = "r00000000deadbee5";
    let job_name = format!("{}-{run_id}", stem());
    let leaked: &'static str = Box::leak(format!("/jobs/{job_name}").into_boxed_str());
    let log = "retention-point=p6 state=Orphaned objects=3 code=BudgetExhausted\n\
               retention-result=deleted=0 failed=1 objects=3\n";
    let routes = vec![
        route("GET", "/retentionpolicies", policy_list(vec![])),
        route("GET", leaked, job_body(&job_name, true)),
        route("GET", "/pods", pod_list(1)),
        route("GET", "/log", log.to_string()),
        route(
            "PATCH",
            "/retentionpolicies/primary/status",
            policy_value(json!({}), json!({})).to_string(),
        ),
        route("PATCH", leaked, "{}".to_string()),
    ];
    let f = fixture(routes);
    // `observedGeneration` MATTERS TO THIS ROW NOW: a spec change releases the
    // consecutive-failure budget, so a status carrying a count and NO observed
    // generation means "the spec changed since the count was taken" and the
    // count is zero. The controller writes both fields in the same patch, so a
    // count without a generation is not a state it can produce — this fixture
    // now says which generation the count belongs to.
    let status = json!({
        "lastEnforcement": {"runId": run_id, "startedAt": "2026-09-17T04:00:00Z"},
        "consecutiveRunFailures": 2,
        "observedGeneration": 4
    });
    run(&f, &policy(enforcing(None), status)).await;
    assert_eq!(
        f.status()["consecutiveRunFailures"],
        2,
        "the ceiling is the bound working, not a failure; three bounded runs on a large archive \
         must not set EnforcementDegraded and stop retention for good"
    );
    assert_eq!(f.condition(ctrl::CONDITION_DEGRADED)["status"], "False");
}

/// **M3.** A sibling prefix is not a narrowing of the destination.
#[tokio::test]
async fn a_sibling_prefix_is_not_a_narrowing_of_the_destination() {
    let f = fixture(happy_routes(&six_points()));
    // The destination is rooted at `team-a`; `team-ab` is a different tenant.
    let outcome = run(
        &f,
        &policy(json!({"scope": {"prefix": "team-ab"}}), json!({})),
    )
    .await;
    assert_eq!(outcome.ready_reason, ctrl::REASON_DESTINATION_UNUSABLE);
    assert!(f.condition(ctrl::CONDITION_READY)["message"]
        .as_str()
        .expect("a message")
        .contains("team-ab"));
}

/// **M3, the negative control.** The destination's own prefix, and a genuine
/// narrowing of it, both pass the SCOPE guard.
///
/// A narrowing whose points then lie outside it is refused later and
/// differently — by `plan_document`, with `PlanRefused` — which is the correct
/// separation: "this scope is not under the destination" and "this scope
/// excludes every candidate" are two findings an operator fixes in two places.
#[tokio::test]
async fn the_destination_prefix_and_a_genuine_narrowing_both_pass_the_scope_guard() {
    let f = fixture(happy_routes(&six_points()));
    let outcome = run(&f, &policy(json!({"scope": {"prefix": SCOPE}}), json!({}))).await;
    assert_eq!(outcome.ready, "True", "the destination's own prefix passes");

    let g = fixture(happy_routes(&six_points()));
    let narrowed = run(
        &g,
        &policy(json!({"scope": {"prefix": "team-a/nightly"}}), json!({})),
    )
    .await;
    assert_ne!(
        narrowed.ready_reason,
        ctrl::REASON_DESTINATION_UNUSABLE,
        "`team-a/nightly` IS under `team-a`, so the scope guard must not be what refuses it"
    );
    assert_eq!(
        narrowed.ready_reason,
        ctrl::REASON_UNSUPPORTED_COMBINATION,
        "what refuses it is the plan: no candidate's keys are under that narrower prefix"
    );
}

/// **M4.** A page the catalog published no digest for is a view failure.
#[tokio::test]
async fn a_page_with_no_published_digest_is_a_view_failure() {
    let mut routes = happy_routes(&six_points());
    routes.retain(|r| r.path_suffix != "/recoverycatalogs/primary");
    routes.push(route(
        "GET",
        "/recoverycatalogs/primary",
        catalog_body(
            Some(DEST),
            json!([{"configMapName": "page-0", "index": 0, "count": 6}]),
        ),
    ));
    let f = fixture(routes);
    let outcome = run(&f, &policy(json!({}), json!({}))).await;
    assert_eq!(outcome.ready_reason, ctrl::REASON_CATALOG_UNUSABLE);
    assert!(
        f.condition(ctrl::CONDITION_EVALUATED)["message"]
            .as_str()
            .expect("a message")
            .contains("sha256"),
        "the same function refuses an INCOMPLETE view; accepting an UNVERIFIED one would be the \
         same defect through the other door"
    );
}

/// **M5.** The cadence is read: a schedule that has not come due starts no Job.
#[tokio::test]
async fn a_cadence_that_has_not_come_due_starts_no_job() {
    let digest = {
        let learn = fixture(happy_routes(&six_points()));
        run(&learn, &policy(enforcing(None), json!({}))).await;
        learn.status()["lastEvaluation"]["planSha256"]
            .as_str()
            .expect("a digest")
            .to_string()
    };
    let mut spec = enforcing(Some(&digest));
    // A yearly cron whose last firing was nine months before `now()` — far
    // older than `planMaxAgeSeconds`, so it is a slot to wait past rather than
    // one to catch up on.
    spec["enforcement"]["schedule"] = json!("0 3 25 12 *");
    let mut routes = happy_routes(&six_points());
    routes.push(plan_config_map_route(&digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.extend(absent_job_routes(&digest, now()));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let f = fixture(routes);
    let outcome = run(&f, &policy(spec, json!({}))).await;
    assert_eq!(outcome.enforced_reason, ctrl::REASON_NOTHING_TO_DO);
    assert!(
        f.seen().iter().all(|(m, _)| m != "POST"),
        "an operator who wrote a nightly cron must not get a run every reconcile"
    );
}

/// **M5.** An unparseable schedule is refused, not silently hourly.
#[tokio::test]
async fn an_unparseable_schedule_is_refused() {
    let mut spec = enforcing(None);
    spec["enforcement"]["schedule"] = json!("not a cron");
    let f = fixture(happy_routes(&six_points()));
    let outcome = run(&f, &policy(spec, json!({}))).await;
    assert_eq!(outcome.enforced_reason, ctrl::REASON_UNSUPPORTED_SCHEDULE);
    assert!(f.seen().iter().all(|(m, _)| m != "POST"));
}

/// **M5 / Q1.** One run per SLOT, not one per minute: a pass that has already
/// recorded this plan's run in this slot creates nothing.
#[tokio::test]
async fn a_second_pass_in_one_slot_creates_no_second_job() {
    let digest = {
        let learn = fixture(happy_routes(&six_points()));
        run(&learn, &policy(enforcing(None), json!({}))).await;
        learn.status()["lastEvaluation"]["planSha256"]
            .as_str()
            .expect("a digest")
            .to_string()
    };
    // The slot the fixture's `"17 4 * * *"` names at `now()`.
    let slot = now();
    let run_id = plan::run_id(UID, &digest, slot.timestamp());
    let mut routes = happy_routes(&six_points());
    routes.push(plan_config_map_route(&digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.extend(absent_job_routes(&digest, now()));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let f = fixture(routes);
    let status = json!({
        "lastEnforcement": {
            "runId": run_id, "startedAt": "2026-09-17T04:17:00Z",
            "finishedAt": "2026-09-17T04:17:30Z", "exitCode": 0
        }
    });
    // Ten minutes later, inside the same daily slot.
    let outcome = run_at(
        &f,
        &policy(enforcing(Some(&digest)), status),
        now() + chrono::Duration::minutes(10),
    )
    .await;
    assert_eq!(outcome.enforced_reason, ctrl::REASON_NOTHING_TO_DO);
    assert!(
        f.seen().iter().all(|(m, _)| m != "POST"),
        "the deterministic name makes a duplicate a 409; the slot is what makes it one run per \
         CADENCE rather than one per minute"
    );
}

/// **M6.** An object squatting the plan `ConfigMap`'s name creates no Job, and
/// the refusal names the object.
#[tokio::test]
async fn a_squatted_plan_config_map_name_creates_no_job() {
    let digest = {
        let learn = fixture(happy_routes(&six_points()));
        run(&learn, &policy(enforcing(None), json!({}))).await;
        learn.status()["lastEvaluation"]["planSha256"]
            .as_str()
            .expect("a digest")
            .to_string()
    };
    let name = plan::plan_config_map_name(UID, &digest);
    let suffix: &'static str = Box::leak(format!("/configmaps/{name}").into_boxed_str());
    let mut routes = happy_routes(&six_points());
    routes.push(route(
        "GET",
        suffix,
        json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {
                "name": name, "namespace": NS, "resourceVersion": "1",
                "annotations": {plan::PLAN_DIGEST_ANNOTATION: "sha256:0000"}
            },
            "immutable": true,
            "data": {plan::PLAN_DATA_KEY: "{\"squatted\":true}"}
        })
        .to_string(),
    ));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.extend(absent_job_routes(&digest, now()));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let f = fixture(routes);
    let outcome = run(&f, &policy(enforcing(Some(&digest)), json!({}))).await;

    assert_eq!(
        outcome.enforced_reason,
        ctrl::REASON_PLAN_CONFIG_MAP_CONFLICT
    );
    assert!(
        f.posted("/jobs").is_empty(),
        "the plan an administrator approved is not the plan at that name"
    );
    assert!(f.condition(ctrl::CONDITION_ENFORCED)["message"]
        .as_str()
        .expect("a message")
        .contains(&name));
}

/// **M9.** The plan `ConfigMap`'s name is published, so a reader never
/// recomputes it.
#[tokio::test]
async fn the_plan_ref_is_published_once_a_run_is_authorised() {
    let digest = {
        let learn = fixture(happy_routes(&six_points()));
        run(&learn, &policy(enforcing(None), json!({}))).await;
        learn.status()["lastEvaluation"]["planSha256"]
            .as_str()
            .expect("a digest")
            .to_string()
    };
    let mut routes = happy_routes(&six_points());
    routes.push(plan_config_map_route(&digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.extend(absent_job_routes(&digest, now()));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let f = fixture(routes);
    run(&f, &policy(enforcing(Some(&digest)), json!({}))).await;
    assert_eq!(
        f.status()["lastEvaluation"]["planRef"]["name"],
        plan::plan_config_map_name(UID, &digest),
        "the contract says `read the names from status; never compute one`, and W11's /preview \
         is told to read this field"
    );
}

/// **H2's parser**, as a table. Key lines are read BY NAME and never by
/// position: a pod log is stdout and stderr merged in nondeterministic order.
#[test]
fn the_run_line_parser_reads_by_name_and_skips_what_it_cannot_read() {
    let log = "some unrelated line\n\
               retention-result=deleted=2 failed=1 objects=17\n\
               retention-point=p1 state=Deleted objects=9\n\
               retention-point=p2 state=Orphaned objects=3 code=AccessDenied\n\
               retention-point=p3 objects=0\n\
               retention-record=logweir/retention/u/r.json sha256=sha256:cc\n\
               retention-point=\n";
    let report = ctrl::parse_run_lines(log, Some(1));
    assert_eq!(report.deleted, vec!["p1"]);
    assert_eq!(
        report.failed,
        vec![("p2".to_string(), "AccessDenied".to_string())]
    );
    assert_eq!(
        report.objects_deleted, 17,
        "read from the result line wherever it appeared"
    );
    assert_eq!(
        report.record_key.as_deref(),
        Some("logweir/retention/u/r.json")
    );
    assert_eq!(report.record_sha256.as_deref(), Some("sha256:cc"));
    assert_eq!(report.exit_code, Some(1));

    // A point line with no `state=` is skipped rather than guessed at, and an
    // empty one produces nothing at all.
    assert!(!report.deleted.contains(&"p3".to_string()));
    assert!(report.failed.iter().all(|(p, _)| p != "p3"));

    // And a run that stopped on its ceiling is not a failed run.
    let bounded = ctrl::parse_run_lines(
        "retention-point=p9 state=Orphaned objects=1 code=BudgetExhausted\n",
        Some(1),
    );
    assert!(bounded.bounded_only());
    assert!(!report.bounded_only(), "an AccessDenied is not a bound");
}

// ---------------------------------------------------------------------------
// RET-DEGRADED-UNREACHABLE — the bounded retry, driven as a SEQUENCE
// ---------------------------------------------------------------------------

/// The object's status after a pass, which is the previous status with this
/// pass's patches merged onto it — what the API server would hold and what the
/// next pass reads. Every row below threads this rather than hand-writing the
/// intermediate status, because the defect this file now pins lives ONLY in
/// what one pass leaves behind for the next.
fn after(previous: &Value, f: &Fixture) -> Value {
    // EACH RAW PATCH, IN ORDER, ONTO THE PREVIOUS STATUS — never `Fixture::status()`.
    // That helper folds the pass's patches into an EMPTY object, so an RFC 7386
    // `null` (which deletes) has no key to delete and disappears; folding its
    // result onto the previous status could then never remove anything, and a
    // row about explicit nulls would assert the opposite of what it means to.
    // The API server applies each patch to the object as it then stands, and so
    // does this.
    let mut merged = previous.clone();
    for patch in f.status_patches() {
        if let Some(status) = patch.get("status") {
            weirkeeper::conditions::apply_merge_patch(&mut merged, status);
        }
    }
    merged
}

/// A pass that should start a run. Returns the status it leaves and the run id.
async fn start_pass(
    spec: &Value,
    status: &Value,
    at: DateTime<Utc>,
    digest: &str,
) -> (Value, String) {
    let mut routes = happy_routes(&six_points());
    routes.push(plan_config_map_route(digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.extend(absent_job_routes(digest, at));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let f = fixture(routes);
    let outcome = run_at(&f, &policy(spec.clone(), status.clone()), at).await;
    assert_eq!(
        outcome.phase,
        ctrl::RetentionPhase::Started,
        "the pass at {at} did not start a run"
    );
    let next = after(status, &f);
    let run_id = next["lastEnforcement"]["runId"]
        .as_str()
        .expect("a run id")
        .to_string();
    (next, run_id)
}

/// A pass that should harvest the named run at `exit`.
async fn harvest_pass(
    spec: &Value,
    status: &Value,
    at: DateTime<Utc>,
    run_id: &str,
    exit: i32,
    digest: &str,
) -> Value {
    let job_name = format!("{}-{run_id}", stem());
    let leaked: &'static str = Box::leak(format!("/jobs/{job_name}").into_boxed_str());
    // THE EVALUATION ROUTES ARE HERE TOO, DELIBERATELY. A pass that fails to
    // recognise its own in-flight run falls through to `evaluate()` and starts
    // ANOTHER one; with only the harvest routes the mock would panic on the
    // first unrouted request and the failure would read as a gap in the table.
    // With them present the pass completes and the phase assertion below is
    // what fails, naming the defect.
    let mut routes = happy_routes(&six_points());
    routes.push(route("GET", leaked, job_body(&job_name, true)));
    routes.push(route("GET", "/pods", pod_list(exit)));
    routes.push(route(
        "GET",
        "/log",
        "retention-result=deleted=0 failed=0 objects=0\n".to_string(),
    ));
    routes.push(route("PATCH", leaked, "{}".to_string()));
    routes.push(plan_config_map_route(digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.extend(absent_job_routes(digest, at));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let f = fixture(routes);
    let outcome = run_at(&f, &policy(spec.clone(), status.clone()), at).await;
    assert_eq!(
        outcome.phase,
        ctrl::RetentionPhase::Harvested,
        "the pass at {at} did not harvest run {run_id}"
    );
    after(status, &f)
}

/// One condition off a status object, by type.
fn condition_of<'a>(status: &'a Value, r#type: &str) -> Option<&'a Value> {
    status["conditions"]
        .as_array()?
        .iter()
        .find(|c| c["type"] == r#type)
}

/// A pass that is expected NOT to start a run. Returns the Fixture so the
/// caller can assert on what was and was not sent.
async fn quiet_pass(policy: &RetentionPolicy, at: DateTime<Utc>, digest: &str) -> Fixture {
    let mut routes = happy_routes(&six_points());
    routes.push(plan_config_map_route(digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.extend(absent_job_routes(digest, at));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let f = fixture(routes);
    run_at(&f, policy, at).await;
    f
}

/// The plan digest `six_points()` evaluates to — learned from a pass, never
/// written down, so it cannot drift from the evaluator.
async fn learned_digest() -> String {
    let learn = fixture(happy_routes(&six_points()));
    run(&learn, &policy(enforcing(None), json!({}))).await;
    learn.status()["lastEvaluation"]["planSha256"]
        .as_str()
        .expect("a digest")
        .to_string()
}

/// `spec.enforcement.requireApprovedPlan: false`, so each cycle below starts a
/// run without an administrator re-approving a digest between them. The
/// property under test is the COUNTING, not the approval.
fn unattended_enforcing() -> Value {
    let mut spec = enforcing(None);
    spec["enforcement"]["requireApprovedPlan"] = json!(false);
    spec
}

/// **RET-DEGRADED-UNREACHABLE.** Three consecutive failed runs degrade the
/// policy, and a fourth is not scheduled until the spec changes.
///
/// DRIVEN AS A SEQUENCE OF REAL PASSES, each starting from the status the
/// previous one wrote, and that is the whole point of the row. Every other test
/// in this file hands the reconciler a hand-written status, so the defect —
/// which lives ONLY in what one pass leaves behind for the next — was invisible
/// to all of them: `three_consecutive_failures_degrade_and_stop_scheduling`
/// passed while the product could not reach two.
///
/// What was wrong: `start_run`'s `lastEnforcement` patch is an RFC 7386 merge,
/// and it wrote `{runId, startedAt, planSha256}` without clearing the previous
/// run's `finishedAt`. `tracked_run` reads exactly that field to decide whether
/// the run named by `runId` still needs harvesting, so from the SECOND run
/// onward every pass concluded there was nothing to track, no run was ever
/// harvested, no exit code was ever read, and `consecutiveRunFailures` stopped
/// at 1. D3 §6.5's bounded retry was unreachable and the policy kept creating
/// deletion Jobs while showing `Enforced=True` and `EnforcementDegraded=False`.
/// Live at `7b4fae9`: five enforcement Jobs in 140 s, all failed, count 1.
#[tokio::test]
async fn three_failed_runs_degrade_and_a_fourth_is_not_scheduled_until_the_spec_changes() {
    let digest = learned_digest().await;
    let spec = unattended_enforcing();

    let mut status = json!({});
    for day in 0..3 {
        let at = now() + chrono::Duration::days(day);
        let (started, run_id) = start_pass(&spec, &status, at, &digest).await;
        assert!(
            started["lastEnforcement"].get("finishedAt").is_none(),
            "a run that has just started has not finished; day {day} status: {started}"
        );
        status = harvest_pass(
            &spec,
            &started,
            at + chrono::Duration::minutes(1),
            &run_id,
            1,
            &digest,
        )
        .await;
        assert_eq!(
            status["consecutiveRunFailures"],
            json!(day + 1),
            "one failed run per cycle, counted; day {day}"
        );
    }

    let degraded = status["conditions"]
        .as_array()
        .expect("conditions")
        .iter()
        .find(|c| c["type"] == ctrl::CONDITION_DEGRADED)
        .cloned()
        .expect("a Degraded condition");
    assert_eq!(degraded["status"], "True");
    assert_eq!(degraded["reason"], ctrl::REASON_CONSECUTIVE_FAILURES);

    // THE FOURTH RUN IS NOT SCHEDULED. Three failures with the spec unchanged
    // stop scheduling; `observedGeneration == metadata.generation` is the only
    // thing on the object that says the spec has not changed.
    let fourth = now() + chrono::Duration::days(3);
    let f = quiet_pass(&policy(spec.clone(), status.clone()), fourth, &digest).await;
    assert!(
        f.posted("/jobs").is_empty(),
        "a degraded policy schedules no further run. Requests: {:?}",
        f.seen()
    );

    // AND THE SPEC CHANGING RELEASES IT. Nothing else does: not time, not a
    // restart, not the count being high enough for long enough.
    let mut bumped = policy_value(spec, status.clone());
    bumped["metadata"]["generation"] = json!(5);
    let bumped: RetentionPolicy = serde_json::from_value(bumped).expect("a policy");
    let g = quiet_pass(&bumped, fourth, &digest).await;
    assert_eq!(
        g.posted("/jobs").len(),
        1,
        "a spec change is what releases a degraded policy"
    );

    // AND IT RELEASES THE BUDGET, not merely the stop. A release that left the
    // counter at its ceiling would give the operator exactly ONE run before the
    // next failure re-degraded the policy, which is not a retry budget — and it
    // is what `d387f87` did: `consecutiveRunFailures` stayed at 3 across the
    // generation bump that correctly resumed scheduling.
    // ONE PATCH CARRIES BOTH. Adopting the generation and releasing the budget
    // are the same event: split across two patches, a 409 between them leaves
    // `observedGeneration` new and the count at its ceiling, which is the
    // defect with an extra step. Asserting only the merged result would not
    // notice (review finding G2).
    let together = g
        .status_patches()
        .into_iter()
        .filter(|p| {
            p["status"].get("observedGeneration").is_some()
                && p["status"].get("consecutiveRunFailures").is_some()
        })
        .count();
    assert_eq!(
        together,
        1,
        "exactly one status patch carries both the adopted generation and the reset count. \
         Patches: {:?}",
        g.status_patches()
    );

    let released = after(&status, &g);
    assert_eq!(
        released["consecutiveRunFailures"],
        json!(0),
        "the spec change that releases the stop resets the count in the same patch that adopts \
         the new generation. Status: {released}"
    );
    let degraded = condition_of(&released, ctrl::CONDITION_DEGRADED).expect("a Degraded condition");
    assert_eq!(degraded["status"], "False");
    assert_eq!(degraded["reason"], ctrl::REASON_HEALTHY);
}

/// **The condition D3 §6.5 asks for, and the array replace that used to eat
/// it.** At the third consecutive failure the object carries
/// `EnforcementDegraded=True` with a reason and a message that says WHY in
/// words — the count, the last run's exit code, and its closed per-point codes.
///
/// THE SECOND HALF IS THE REGRESSION. `status.conditions` is an array and an
/// RFC 7386 merge PATCH replaces an array whole, so a pass that named only its
/// own conditions DELETED every other one. `harvest` published
/// `EnforcementDegraded` correctly and the very next evaluation pass — four
/// conditions, none of them that one — took it straight off the object. Live at
/// `d387f87`: `consecutiveRunFailures 3`, scheduling correctly stopped, and
/// `EnforcementDegraded` **absent**; not `False`, not present at all. A console
/// saw `Ready=True` and `Evaluated=True` and nothing that said the policy had
/// spent its budget. So this row runs the evaluation pass AFTER the harvest and
/// asserts the condition is still there.
#[tokio::test]
async fn the_third_failure_publishes_enforcement_degraded_and_the_next_pass_keeps_it() {
    let digest = learned_digest().await;
    let spec = unattended_enforcing();

    let mut status = json!({});
    let mut at = now();
    for _ in 0..3 {
        let (started, run_id) = start_pass(&spec, &status, at, &digest).await;
        status = harvest_pass(
            &spec,
            &started,
            at + chrono::Duration::minutes(1),
            &run_id,
            1,
            &digest,
        )
        .await;
        at += chrono::Duration::days(1);
    }

    let degraded = condition_of(&status, ctrl::CONDITION_DEGRADED).expect("a Degraded condition");
    assert_eq!(degraded["status"], "True");
    assert_eq!(degraded["reason"], ctrl::REASON_CONSECUTIVE_FAILURES);
    let message = degraded["message"].as_str().expect("a message");
    assert!(
        message.contains('3'),
        "the message names the count: {message}"
    );
    assert!(
        message.contains("exited 1"),
        "and the last run's exit code: {message}"
    );
    assert!(
        message.contains("no further run is scheduled until spec changes")
            || message.contains("No further run is scheduled until spec changes"),
        "and what it means for scheduling: {message}"
    );

    // THE NEXT PASS EVALUATES AND MUST NOT EAT IT.
    let f = quiet_pass(&policy(spec, status.clone()), at, &digest).await;
    assert!(
        f.posted("/jobs").is_empty(),
        "a degraded policy schedules nothing"
    );
    let next = after(&status, &f);
    let still = condition_of(&next, ctrl::CONDITION_DEGRADED)
        .expect("EnforcementDegraded survives a pass that does not name it");
    assert_eq!(still["status"], "True");
    assert_eq!(still["reason"], ctrl::REASON_CONSECUTIVE_FAILURES);
    // AND IT DID NOT "TRANSITION" AGAIN. `metav1.Condition` says
    // `lastTransitionTime` is the instant the condition last CHANGED, so a
    // condition republished unchanged must keep its own. Without this the
    // whole suite passed with `merge_condition` removed (review finding G3),
    // and an operator reading "degraded 4 seconds ago" on a policy that has
    // been degraded for a day has been told something false.
    assert_eq!(
        still["lastTransitionTime"], degraded["lastTransitionTime"],
        "a condition that did not change keeps its lastTransitionTime"
    );
    for kept in [
        ctrl::CONDITION_READY,
        ctrl::CONDITION_EVALUATED,
        ctrl::CONDITION_ENFORCED,
    ] {
        assert!(
            condition_of(&next, kept).is_some(),
            "{kept} is still on the object too. Conditions: {}",
            next["conditions"]
        );
    }
}

/// **The upgrade path, and the reason the EVALUATION publishes the condition
/// rather than leaving it to the harvest.**
///
/// A policy that reached the ceiling on a build that never published
/// `EnforcementDegraded` — `d387f87` in the lab, `consecutiveRunFailures 3`,
/// scheduling stopped, the condition absent — creates no further Job, so it
/// never harvests again, so a harvest-only writer would never publish the
/// condition at all. The object would stay unexplained for as long as the
/// policy stayed stopped, which is forever until someone edits the spec.
///
/// The evaluation is the pass that READS the budget and STOPS the scheduling,
/// so it is the pass that has to say so, and it says it out of
/// `status.lastEnforcement` — the exit code and the closed per-point codes the
/// last run left behind — so that a pass which harvested nothing still says why
/// in words.
#[tokio::test]
async fn a_policy_already_at_the_ceiling_publishes_the_condition_on_its_next_pass() {
    let digest = learned_digest().await;
    let status = json!({
        "observedGeneration": 4,
        "consecutiveRunFailures": 3,
        "lastEnforcement": {
            "runId": "r00000000deadbeea",
            "startedAt": "2026-09-17T04:00:00Z",
            "finishedAt": "2026-09-17T04:05:00Z",
            "exitCode": 1,
            "failed": [
                {"pointId": "p-not-in-this-view", "code": "AccessDenied"},
                {"pointId": "p-also-not-here", "code": "AccessDenied"}
            ]
        }
        // NO `conditions` AT ALL — the shape the defect left behind.
    });
    let f = quiet_pass(
        &policy(unattended_enforcing(), status.clone()),
        now(),
        &digest,
    )
    .await;

    assert!(
        f.posted("/jobs").is_empty(),
        "the budget is spent; nothing is scheduled"
    );
    let after_pass = after(&status, &f);
    let degraded =
        condition_of(&after_pass, ctrl::CONDITION_DEGRADED).expect("EnforcementDegraded");
    assert_eq!(degraded["status"], "True");
    assert_eq!(degraded["reason"], ctrl::REASON_CONSECUTIVE_FAILURES);
    let message = degraded["message"].as_str().expect("a message");
    assert!(message.contains('3'), "the count: {message}");
    assert!(message.contains("exited 1"), "the exit code: {message}");
    assert!(
        message.contains("AccessDenied on 2 point(s)"),
        "the closed per-point codes, counted rather than listed: {message}"
    );
    assert!(
        message.contains("recordKey"),
        "and where the durable evidence is, because the Job is TTL-collected: {message}"
    );
    // The count is history and this pass must not touch it: the spec has not changed.
    assert_eq!(after_pass["consecutiveRunFailures"], json!(3));
}

/// OBJECT-LOCK-DELETE-MARKER on the object: a policy stopped by the retry
/// budget over a VERSIONED bucket says so in words, with the remedy — the
/// refusal is about the bucket, nothing about it clears on a retry, and an
/// operator reading "VersionedBucket on 2 point(s)" alone would not know what
/// to change.
///
/// MUTANT: drop the remedy sentence from `failure_detail`. This row fails.
#[tokio::test]
async fn a_policy_stopped_on_a_versioned_bucket_says_why_and_what_to_change() {
    let digest = learned_digest().await;
    let status = json!({
        "observedGeneration": 4,
        "consecutiveRunFailures": 3,
        "lastEnforcement": {
            "runId": "r00000000deadbeeb",
            "startedAt": "2026-09-17T04:00:00Z",
            "finishedAt": "2026-09-17T04:05:00Z",
            "exitCode": 1,
            "failed": [
                {"pointId": "p5", "code": "VersionedBucket"},
                {"pointId": "p6", "code": "VersionedBucket"}
            ]
        }
    });
    let f = quiet_pass(
        &policy(unattended_enforcing(), status.clone()),
        now(),
        &digest,
    )
    .await;
    assert!(f.posted("/jobs").is_empty());
    let after_pass = after(&status, &f);
    let degraded =
        condition_of(&after_pass, ctrl::CONDITION_DEGRADED).expect("EnforcementDegraded");
    let message = degraded["message"].as_str().expect("a message");
    assert!(
        message.contains("VersionedBucket on 2 point(s)"),
        "{message}"
    );
    assert!(message.contains("delete marker"), "{message}");
    assert!(message.contains("mode: ExternalLifecycle"), "{message}");
    assert!(!message.contains("  "), "{message:?}");
}

/// A policy at the retry ceiling, its last run refused with `codes`, that
/// run finished at `finished_at`.
fn degraded_status(codes: &[&str], finished_at: &str) -> Value {
    json!({
        "observedGeneration": 4,
        "consecutiveRunFailures": 3,
        "lastEnforcement": {
            "runId": "r00000000deadbeec",
            "startedAt": "2026-09-15T04:00:00Z",
            "finishedAt": finished_at,
            "exitCode": 1,
            "failed": codes
                .iter()
                .enumerate()
                .map(|(i, c)| json!({"pointId": format!("p{}", i + 5), "code": c}))
                .collect::<Vec<Value>>()
        }
    })
}

/// Review M1: a policy degraded ONLY by a bucket- or credential-level refusal
/// resumes on its own. The operator grants `s3:GetObject` (or unversions the
/// bucket) — nothing on the object changes — and 24 h after the last run one
/// re-probe run is scheduled, with no spec edit. A refusal about the PLAN
/// (`AccessDenied` on a delete, here mixed in) still waits for the spec, as
/// D3 §6.5 says, and so does a re-probe inside the 24 h.
///
/// MUTANT: `reprobe_due` always false (the pre-fix rule). The first arm posts
/// no Job and this row fails. MUTANT: drop the all-bucket-level clause. The
/// mixed arm posts a Job and this row fails.
#[tokio::test]
async fn a_bucket_level_refusal_reprobes_a_day_later_without_a_spec_edit() {
    let digest = learned_digest().await;
    for (label, codes, finished, expect_job) in [
        (
            "VersionProbeRefused, 24 h 12 min ago",
            vec!["VersionProbeRefused", "VersionProbeRefused"],
            "2026-09-16T04:05:00Z",
            true,
        ),
        (
            "VersionedBucket, 24 h 12 min ago",
            vec!["VersionedBucket"],
            "2026-09-16T04:05:00Z",
            true,
        ),
        (
            "VersionProbeRefused, one hour ago",
            vec!["VersionProbeRefused"],
            "2026-09-17T03:17:00Z",
            false,
        ),
        (
            "mixed with a plan-level AccessDenied",
            vec!["VersionedBucket", "AccessDenied"],
            "2026-09-16T04:05:00Z",
            false,
        ),
    ] {
        let status = degraded_status(&codes, finished);
        let f = quiet_pass(
            &policy(unattended_enforcing(), status.clone()),
            now(),
            &digest,
        )
        .await;
        assert_eq!(
            !f.posted("/jobs").is_empty(),
            expect_job,
            "{label}: {}",
            after(&status, &f)
        );
        let after_pass = after(&status, &f);
        let degraded =
            condition_of(&after_pass, ctrl::CONDITION_DEGRADED).expect("EnforcementDegraded");
        let message = degraded["message"].as_str().expect("a message");
        assert_eq!(
            message.contains("no spec edit is needed to resume"),
            !label.contains("mixed"),
            "{label}: the condition says whether it resumes on its own: {message}"
        );
        // …AND NEVER THE OPPOSITE IN THE SAME BREATH (re-check RL1).
        assert_eq!(
            message.contains("No further run is scheduled until spec changes"),
            label.contains("mixed"),
            "{label}: the stop sentence agrees with the re-probe rule: {message}"
        );
    }
}

/// Review M4: while the retry budget is spent, `ageExpiry` is not claimed as
/// Logweir-enforced — nothing is being deleted, and on a versioned bucket
/// nothing ever can be.
///
/// MUTANT: drop `&& !self.budget_spent()` from the `ageExpiry` derivation.
/// This row fails.
#[tokio::test]
async fn a_degraded_policy_does_not_claim_age_expiry() {
    let digest = learned_digest().await;
    let status = degraded_status(&["VersionedBucket"], "2026-09-17T03:17:00Z");
    let f = quiet_pass(
        &policy(unattended_enforcing(), status.clone()),
        now(),
        &digest,
    )
    .await;
    let after_pass = after(&status, &f);
    assert_eq!(
        after_pass["guarantees"]["ageExpiry"],
        ctrl::GUARANTEE_NOT_ENFORCED,
        "{after_pass}"
    );
    // …and a healthy enforcing policy still says LogweirEnforced.
    let healthy = quiet_pass(&policy(unattended_enforcing(), json!({})), now(), &digest).await;
    assert_eq!(
        after(&json!({}), &healthy)["guarantees"]["ageExpiry"],
        ctrl::GUARANTEE_LOGWEIR
    );
}

/// **Review finding G1: a refusal path must not eat the operator's spec edit.**
///
/// Seven writers adopt `metadata.generation` onto the status. Only the
/// evaluation used to release the consecutive-failure budget with it — and
/// `evaluate()` returns through the REFUSAL writers before the evaluation is
/// ever reached. So a policy degraded at the ceiling, whose operator edits the
/// spec, and whose catalog view is still unreadable, had its edit consumed:
/// `observedGeneration` moved, `consecutiveRunFailures` stayed at 3,
/// `spec_changed()` went false, and the budget was spent again. The runs were
/// failing for a reason, so a refusal co-occurring with the edit is the likely
/// case; every further edit would be eaten the same way.
///
/// `adopt_generation()` is the one place the two happen together, and this row
/// drives the path that proved it was needed.
#[tokio::test]
async fn a_spec_edit_releases_the_budget_even_when_the_view_is_unreadable() {
    let mut routes = happy_routes(&six_points());
    routes.retain(|r| r.path_suffix != "/recoverycatalogs/primary");
    routes.push(route(
        "GET",
        "/recoverycatalogs/primary",
        catalog_body(Some(DEST), Value::Null),
    ));
    let f = fixture(routes);

    let status = json!({
        "observedGeneration": 4,
        "consecutiveRunFailures": 3,
        "conditions": [{
            "type": ctrl::CONDITION_DEGRADED, "status": "True",
            "reason": ctrl::REASON_CONSECUTIVE_FAILURES,
            "message": "3 consecutive retention runs have failed",
            "lastTransitionTime": "2026-09-17T04:00:00Z", "observedGeneration": 4
        }]
    });
    // THE OPERATOR'S EDIT: generation 5 against observedGeneration 4.
    let mut value = policy_value(unattended_enforcing(), status.clone());
    value["metadata"]["generation"] = json!(5);
    let policy: RetentionPolicy = serde_json::from_value(value).expect("a policy");

    let outcome = run(&f, &policy).await;
    assert_eq!(outcome.ready_reason, ctrl::REASON_CATALOG_UNUSABLE);

    let patches = f.status_patches();
    assert_eq!(
        patches.len(),
        1,
        "the refusal is one patch. Patches: {patches:?}"
    );
    let written = &patches[0]["status"];
    assert_eq!(
        written["observedGeneration"],
        json!(5),
        "the edit is adopted"
    );
    assert_eq!(
        written["consecutiveRunFailures"],
        json!(0),
        "AND the budget is released in the SAME patch, or the edit is consumed for nothing. \
         Written: {written}"
    );

    let after_pass = after(&status, &f);
    let degraded =
        condition_of(&after_pass, ctrl::CONDITION_DEGRADED).expect("EnforcementDegraded");
    assert_eq!(degraded["status"], "False");
    assert_eq!(degraded["reason"], ctrl::REASON_HEALTHY);
    assert!(
        degraded["message"]
            .as_str()
            .expect("a message")
            .contains("the spec changed"),
        "and it says why: {degraded}"
    );
    // The refusal's own conditions are all there too.
    for kept in [ctrl::CONDITION_READY, ctrl::CONDITION_EVALUATED] {
        assert!(
            condition_of(&after_pass, kept).is_some(),
            "{kept} is written"
        );
    }
}

/// The general rule behind the row above: **no status write drops a condition
/// it did not name.** Proved on the narrowest writer in the file —
/// `publish_enforcement_refusal` names `Enforced` alone, mid-pass, right after
/// `publish_evaluation` wrote four. Before this branch that single-element
/// array replaced the other three on the object.
#[tokio::test]
async fn a_status_write_keeps_the_conditions_it_does_not_name() {
    let digest = learned_digest().await;
    let mut routes = happy_routes(&six_points());
    routes.retain(|r| r.path_suffix != "/restores");
    routes.push(route(
        "GET",
        "/restores",
        restore_list(vec![destination_backed_restore(
            DEST,
            "s1",
            Some("Restoring"),
        )]),
    ));
    routes.push(plan_config_map_route(&digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.extend(absent_job_routes(&digest, now()));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let f = fixture(routes);
    let outcome = run(&f, &policy(unattended_enforcing(), json!({}))).await;
    assert_eq!(outcome.enforced_reason, ctrl::REASON_ACTIVE_RESTORE);

    let status = after(&json!({}), &f);
    let enforced = condition_of(&status, ctrl::CONDITION_ENFORCED).expect("Enforced");
    assert_eq!(enforced["reason"], ctrl::REASON_ACTIVE_RESTORE);
    for kept in [
        ctrl::CONDITION_READY,
        ctrl::CONDITION_EVALUATED,
        ctrl::CONDITION_EXTERNAL_CONFLICT,
        ctrl::CONDITION_DEGRADED,
    ] {
        assert!(
            condition_of(&status, kept).is_some(),
            "the refusal names only `Enforced`; {kept} must survive it. Conditions: {}",
            status["conditions"]
        );
    }
}

/// One successful run clears the count, so "three consecutive" means
/// consecutive and a policy that recovers is not degraded by history.
#[tokio::test]
async fn a_successful_run_clears_the_consecutive_failure_count() {
    let digest = learned_digest().await;
    let spec = unattended_enforcing();

    let mut status = json!({});
    for day in 0..2 {
        let at = now() + chrono::Duration::days(day);
        let (started, run_id) = start_pass(&spec, &status, at, &digest).await;
        status = harvest_pass(
            &spec,
            &started,
            at + chrono::Duration::minutes(1),
            &run_id,
            1,
            &digest,
        )
        .await;
    }
    assert_eq!(status["consecutiveRunFailures"], json!(2));

    let at = now() + chrono::Duration::days(2);
    let (started, run_id) = start_pass(&spec, &status, at, &digest).await;
    status = harvest_pass(
        &spec,
        &started,
        at + chrono::Duration::minutes(1),
        &run_id,
        0,
        &digest,
    )
    .await;

    assert_eq!(status["consecutiveRunFailures"], json!(0));
    let degraded = status["conditions"]
        .as_array()
        .expect("conditions")
        .iter()
        .find(|c| c["type"] == ctrl::CONDITION_DEGRADED)
        .cloned()
        .expect("a Degraded condition");
    assert_eq!(degraded["status"], "False");
    assert_eq!(degraded["reason"], ctrl::REASON_HEALTHY);
    assert!(
        degraded["message"]
            .as_str()
            .expect("a message")
            .contains('0'),
        "and it says so in words: {degraded}"
    );
}

/// The shape, asserted directly: a started run DELETES the previous run's
/// terminal fields with explicit `null`s, all seven of them.
///
/// The row above proves the consequence; this one proves the mechanism, so a
/// future change that clears `finishedAt` some other way and leaves `exitCode`
/// or `failed[]` behind is still caught. `failed[]` matters beyond cosmetics:
/// `previously_refused()` reads it to exclude a point from the next plan.
#[tokio::test]
async fn a_started_run_deletes_the_previous_runs_terminal_fields() {
    let digest = learned_digest().await;
    let status = json!({
        "lastEnforcement": {
            "runId": "r00000000deadbee9",
            "startedAt": "2026-09-16T04:17:00Z",
            "finishedAt": "2026-09-16T04:19:00Z",
            "exitCode": 1,
            "deleted": ["p1"],
            // A point id the view does not carry, DELIBERATELY: `previously_refused()`
            // reads `failed[]` and excludes those points from the next plan, so
            // seeding a real one would change the plan digest this pass computes
            // and the row would be about the evaluator instead of about the patch.
            // That sensitivity is itself why this field must not outlive its run.
            "failed": [{"pointId": "p-not-in-this-view", "code": "AccessDenied"}],
            "objectsDeleted": 3,
            "recordKey": "logweir/retention/uid/r00000000deadbee9.json",
            "recordSha256": "sha256:aa"
        },
        "consecutiveRunFailures": 1,
        "observedGeneration": 4
    });
    let (after_start, _) = start_pass(&unattended_enforcing(), &status, now(), &digest).await;

    let record = &after_start["lastEnforcement"];
    for field in [
        "finishedAt",
        "exitCode",
        "deleted",
        "failed",
        "objectsDeleted",
        "recordKey",
        "recordSha256",
    ] {
        assert!(
            record.get(field).is_none(),
            "`{field}` describes a run that has finished and must not survive onto the run that \
             has just started; a merge PATCH deletes it only with an explicit null. Record: \
             {record}"
        );
    }
    assert_eq!(record["startedAt"], json!(now()));
    assert_ne!(record["runId"], json!("r00000000deadbee9"));
    // The count is HISTORY and is deliberately untouched by a start: it is what
    // the third consecutive failure will be added to.
    assert_eq!(after_start["consecutiveRunFailures"], json!(1));
}

/// The write guard, pinned: a status PATCH with no `metadata.resourceVersion`
/// to precondition on is REFUSED, and the harvest that could not publish its
/// exit code does not then let the Job's pod be collected.
///
/// THE MESSAGE IS PART OF THE CONTRACT. It is the line an operator greps for
/// when a status stops moving — it was 2013 lines in a three-minute window on
/// the lab at `7b4fae9` — so it is named once in the controller and asserted
/// here, rather than spelled twice.
///
/// THE SECOND HALF IS THE ONE THAT COSTS DATA. `harvest` writes the status and
/// then patches the Job's `ttlSecondsAfterFinished`, in that order, because
/// garbage collection must not race the exit-code read (D-SEAMS S7). A write
/// that did not land has lost that race, not won it: setting the TTL then
/// collects the pod whose exit code was never published, and every later pass
/// can only record "produced no exit code". So the TTL waits.
#[tokio::test]
async fn a_status_write_with_no_precondition_is_refused_and_the_pod_is_kept() {
    assert!(
        ctrl::NO_RESOURCE_VERSION.contains("carries no metadata.resourceVersion")
            && ctrl::NO_RESOURCE_VERSION.contains("no patch is sent"),
        "the guard names the missing field and says it sent nothing: {}",
        ctrl::NO_RESOURCE_VERSION
    );

    let run_id = "r00000000deadbee7";
    let job_name = format!("{}-{run_id}", stem());
    let leaked: &'static str = Box::leak(format!("/jobs/{job_name}").into_boxed_str());
    let routes = vec![
        route("GET", "/retentionpolicies", policy_list(vec![])),
        route("GET", leaked, job_body(&job_name, true)),
        route("GET", "/pods", pod_list(1)),
        route(
            "GET",
            "/log",
            "retention-result=deleted=0 failed=0 objects=0\n".to_string(),
        ),
        route(
            "PATCH",
            "/retentionpolicies/primary/status",
            policy_value(json!({}), json!({})).to_string(),
        ),
        route("PATCH", leaked, "{}".to_string()),
    ];
    let f = fixture(routes);

    let mut value = policy_value(
        enforcing(None),
        json!({
            "lastEnforcement": {"runId": run_id, "startedAt": "2026-09-17T04:00:00Z"},
            "consecutiveRunFailures": 2
        }),
    );
    // THE ONE DIFFERENCE FROM EVERY OTHER ROW: no resourceVersion to
    // precondition on.
    value["metadata"]
        .as_object_mut()
        .expect("metadata")
        .remove("resourceVersion");
    let policy: RetentionPolicy = serde_json::from_value(value).expect("a policy");

    run(&f, &policy).await;

    assert!(
        f.status_patches().is_empty(),
        "a /status compare-and-set with nothing to precondition on is not sent as a blind write. \
         Patches: {:?}",
        f.status_patches()
    );
    assert!(
        f.seen()
            .iter()
            .all(|(method, uri)| !(method == "PATCH" && uri.contains("/jobs/"))),
        "and the Job keeps its pod, so the next pass can still read the exit code this one could \
         not publish. Requests: {:?}",
        f.seen()
    );
}

// ---------------------------------------------------------------------------
// RET-COUNT-EARLY — the failure count is a claim about the Jobs
// ---------------------------------------------------------------------------

/// The plan digest a pass would compute from `status`, learned rather than
/// written down.
///
/// It MOVES between runs: a harvest records the run's per-point refusals and
/// `previously_refused()` excludes those points from the next plan. The live
/// run showed that as two enforcement Jobs with two different ids.
async fn digest_for(status: &Value, at: DateTime<Utc>) -> String {
    let learn = fixture(happy_routes(&six_points()));
    run_at(&learn, &policy(enforcing(None), status.clone()), at).await;
    learn.status()["lastEvaluation"]["planSha256"]
        .as_str()
        .expect("a digest")
        .to_string()
}

/// The log of a run that failed with a per-point refusal — the shape the live
/// runs produced, and the one that moves the next plan's digest.
const REFUSED_RUN_LOG: &str = "retention-point=p6 state=Kept objects=0 code=AccessDenied\n\
                               retention-result=deleted=0 failed=1 objects=0\n";

/// A run that failed WITHOUT naming a point, so `previously_refused()` excludes
/// nothing and the next pass computes the SAME plan — and therefore the same
/// run id in the same slot. That repetition is what the guard is about, and a
/// log that moved the digest would quietly make these rows assert nothing.
const PLAIN_FAIL_LOG: &str = "retention-result=deleted=0 failed=0 objects=0\n";

/// One full enforcement cycle: start a run, harvest it at `exit`, and return
/// the status it leaves plus the Job name the pass created.
async fn failed_cycle(
    spec: &Value,
    status: &Value,
    at: DateTime<Utc>,
    exit: i32,
    log: &str,
) -> (Value, String) {
    let digest = digest_for(status, at).await;
    let (started, run_id) = start_pass(spec, status, at, &digest).await;
    let job_name = format!("{}-{run_id}", stem());
    let leaked: &'static str = Box::leak(format!("/jobs/{job_name}").into_boxed_str());
    let mut routes = happy_routes(&six_points());
    routes.push(route("GET", leaked, job_body(&job_name, true)));
    routes.push(route("GET", "/pods", pod_list(exit)));
    routes.push(route("GET", "/log", log.to_string()));
    routes.push(route("PATCH", leaked, "{}".to_string()));
    routes.push(plan_config_map_route(&digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.extend(absent_job_routes(
        &digest,
        at + chrono::Duration::minutes(1),
    ));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let f = fixture(routes);
    let outcome = run_at(
        &f,
        &policy(spec.clone(), started.clone()),
        at + chrono::Duration::minutes(1),
    )
    .await;
    assert_eq!(
        outcome.phase,
        ctrl::RetentionPhase::Harvested,
        "the cycle at {at} did not harvest run {run_id}"
    );
    (after(&started, &f), job_name)
}

/// A pass in a slot whose run has already been harvested.
async fn same_slot_pass(spec: &Value, status: &Value, at: DateTime<Utc>, digest: &str) -> Fixture {
    let mut routes = happy_routes(&six_points());
    routes.push(plan_config_map_route(digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    // THE JOB IS ALREADY THERE, AND `start_run` READS IT BEFORE IT WRITES THE
    // RUN RECORD — defect RET-STARTRUN-PATCH-OUTCOME's ordering. The record is
    // written first now, so this read is what keeps a Job that already stands
    // at this run's name from being re-recorded as a fresh run, which is
    // defect RET-COUNT-EARLY's resurrection.
    for slot in slot_candidates(at) {
        let job_name = job_name_for(digest, slot);
        let leaked: &'static str = Box::leak(format!("/jobs/{job_name}").into_boxed_str());
        routes.push(route("GET", leaked, job_body(&job_name, true)));
    }
    // AND THE 409 THE DETERMINISTIC NAME PRODUCES, KEPT. If the read above
    // stops working the pass reaches this, and the mock answers exactly as the
    // API server would rather than failing on a missing route — so the row
    // still fails on the behaviour and not on the table.
    routes.push(Route {
        method: "POST",
        path_suffix: "/jobs",
        status: 409,
        body: json!({
            "kind": "Status", "apiVersion": "v1", "status": "Failure",
            "reason": "AlreadyExists", "code": 409, "message": "jobs already exists"
        })
        .to_string(),
    });
    let f = fixture(routes);
    run_at(&f, &policy(spec.clone(), status.clone()), at).await;
    f
}

/// **RET-COUNT-EARLY, reproduced.** Two failed enforcement Jobs leave the
/// counter at 2 — including when extra passes look at the policy in between,
/// which is what a real cluster does.
///
/// The live run on `af64073` reached `consecutiveRunFailures: 3` and fired
/// `EnforcementDegraded` after the controller logged "created the retention
/// Job" exactly TWICE, with both Jobs still present at the census (600 s TTL
/// untouched) and the controller pod never restarted. The budget was declared
/// spent a full run early.
#[tokio::test]
async fn two_failed_jobs_are_two_failures_however_many_passes_look_at_them() {
    let spec = unattended_enforcing();

    let (after_first, first_job) = failed_cycle(&spec, &json!({}), now(), 1, PLAIN_FAIL_LOG).await;
    assert_eq!(after_first["consecutiveRunFailures"], json!(1));

    // AN EXTRA PASS IN THE SAME SLOT, which is the whole defect. The run id is
    // a pure function of the policy, the plan digest and the slot, so a pass
    // that recomputes the same plan inside the same slot recomputes the same
    // id; `jobs.create` answers 409 and the pass used to re-record
    // `lastEnforcement` as if a new run had begun — clearing `finishedAt` and
    // handing the finished Job back to `tracked_run` to harvest a second time.
    let at = now() + chrono::Duration::minutes(2);
    let same_slot = digest_for(&after_first, at).await;
    let f = same_slot_pass(&spec, &after_first, at, &same_slot).await;
    let after_extra = after(&after_first, &f);
    assert_eq!(
        after_extra["consecutiveRunFailures"],
        json!(1),
        "a pass that creates no Job creates no failure. Status: {after_extra}"
    );

    let (after_second, second_job) = failed_cycle(
        &spec,
        &after_extra,
        now() + chrono::Duration::days(1),
        1,
        PLAIN_FAIL_LOG,
    )
    .await;
    assert_ne!(first_job, second_job, "two distinct Jobs");
    assert_eq!(
        after_second["consecutiveRunFailures"],
        json!(2),
        "two failed Jobs are two failures — the count is a claim about the Jobs, and on the \
         build this row was written for it claimed three. Status: {after_second}"
    );
    let degraded = condition_of(&after_second, ctrl::CONDITION_DEGRADED).expect("a condition");
    assert_eq!(
        degraded["status"], "False",
        "and the budget is not spent yet: {degraded}"
    );
}

/// And THREE failed Jobs — three distinct names, every one derived from this
/// policy's UID — are what make the count 3 and the budget spent.
#[tokio::test]
async fn three_failed_jobs_by_owner_uid_are_what_make_the_count_three() {
    let spec = unattended_enforcing();
    let mut status = json!({});
    let mut jobs: Vec<String> = Vec::new();
    for day in 0..3 {
        let (next, job) = failed_cycle(
            &spec,
            &status,
            now() + chrono::Duration::days(day),
            1,
            REFUSED_RUN_LOG,
        )
        .await;
        status = next;
        jobs.push(job);
        assert_eq!(
            status["consecutiveRunFailures"],
            json!(day + 1),
            "one Job, one failure; day {day}"
        );
    }

    jobs.sort();
    jobs.dedup();
    assert_eq!(
        jobs.len(),
        3,
        "three DISTINCT Jobs, not one looked at three times"
    );
    // Every one is this policy's: the name is derived from the policy UID, and
    // `job_body` owner-references the same UID — which is the census the live
    // harness performs.
    for job in &jobs {
        assert!(
            job.starts_with(&stem()),
            "{job} is named from the policy UID {UID}"
        );
    }

    let degraded = condition_of(&status, ctrl::CONDITION_DEGRADED).expect("a condition");
    assert_eq!(degraded["status"], "True");
    assert_eq!(degraded["reason"], ctrl::REASON_CONSECUTIVE_FAILURES);
}

/// The guard itself: a run this policy has already harvested is not started
/// again, so no Job is asked for and the record is left exactly as the harvest
/// wrote it.
#[tokio::test]
async fn a_run_already_harvested_in_this_slot_is_not_started_again() {
    let spec = unattended_enforcing();
    let (harvested, job_name) = failed_cycle(&spec, &json!({}), now(), 1, PLAIN_FAIL_LOG).await;
    let run_id = harvested["lastEnforcement"]["runId"]
        .as_str()
        .expect("a run id")
        .to_string();
    assert_eq!(job_name, format!("{}-{run_id}", stem()));
    let finished = harvested["lastEnforcement"]["finishedAt"].clone();
    assert!(finished.is_string(), "the harvest recorded a finish");

    // The SAME slot and the SAME plan, so the same run id.
    let at = now() + chrono::Duration::minutes(2);
    let digest = digest_for(&harvested, at).await;
    let f = same_slot_pass(&spec, &harvested, at, &digest).await;

    assert!(
        f.seen()
            .iter()
            .all(|(method, uri)| !(method == "POST" && uri.contains("/jobs"))),
        "no Job is even asked for: the 409 is avoided, not absorbed. Requests: {:?}",
        f.seen()
    );
    let after_pass = after(&harvested, &f);
    assert_eq!(
        after_pass["lastEnforcement"]["runId"],
        json!(run_id),
        "the record still names the run that ran"
    );
    assert_eq!(
        after_pass["lastEnforcement"]["finishedAt"], finished,
        "and it is still finished — clearing this is what handed the same Job back to be \
         harvested a second time. Record: {}",
        after_pass["lastEnforcement"]
    );
    assert_eq!(after_pass["consecutiveRunFailures"], json!(1));
}

/// **The oscillation path, end to end — review finding F1.** A policy whose
/// plan digest returns to an EARLIER run's digest inside one slot does not
/// re-count that run's failure.
///
/// This is the arm the guard at the top of `start_run` does not reach.
/// `previously_refused()` reads the LAST run's `failed[]`, so the exclusion set
/// oscillates: run 1's plan, minus run 1's refusals, is run 2's; minus run 2's
/// refusals it is run 1's again. Inside one slot that is the same run id, and
/// the object's last record names run 2 — so "the recorded run is this run and
/// it finished" is false and the pass proceeds to `jobs.create`, which answers
/// 409 for run 1's Job. The old fall-through then wrote `runId: run1` with all
/// seven terminal fields nulled, `tracked_run` found run 1's Job present and
/// finished, and `harvest` counted it a second time.
///
/// Live on `af64073` the artifact caught it exactly: `lastEnforcement` naming
/// the FIRST Job with `startedAt` 17.5 s after that Job was created,
/// `finishedAt` 100 ms later, its real `exitCode 1` and its own `recordKey`.
#[tokio::test]
async fn a_digest_that_returns_to_an_earlier_runs_id_does_not_re_count_it() {
    let spec = unattended_enforcing();

    // Run 1, which refuses a point — so the next plan excludes it.
    let (after_first, first_job) = failed_cycle(&spec, &json!({}), now(), 1, REFUSED_RUN_LOG).await;
    let first_run = after_first["lastEnforcement"]["runId"]
        .as_str()
        .expect("a run id")
        .to_string();
    assert_eq!(after_first["consecutiveRunFailures"], json!(1));

    // Run 2 in the SAME slot, refusing nothing — so the exclusion set empties
    // and the plan, and therefore the run id, returns to run 1's.
    let (after_second, second_job) = failed_cycle(
        &spec,
        &after_first,
        now() + chrono::Duration::minutes(2),
        1,
        PLAIN_FAIL_LOG,
    )
    .await;
    assert_ne!(first_job, second_job, "run 2 is a different Job");
    assert_eq!(after_second["consecutiveRunFailures"], json!(2));

    let back = digest_for(&after_second, now() + chrono::Duration::minutes(4)).await;
    let first_digest = after_first["lastEnforcement"]["planSha256"]
        .as_str()
        .expect("run 1's digest");
    assert_eq!(
        back, first_digest,
        "the fixture must actually oscillate, or this row asserts nothing"
    );
    assert_ne!(
        after_second["lastEnforcement"]["runId"],
        json!(first_run),
        "and the object's last record names run 2, which is why the run-id guard cannot fire"
    );

    // The third pass: same slot, run 1's digest, so run 1's id — and run 1's
    // Job is still there.
    let f = same_slot_pass(
        &spec,
        &after_second,
        now() + chrono::Duration::minutes(4),
        &back,
    )
    .await;
    let after_third = after(&after_second, &f);
    assert_eq!(
        after_third["consecutiveRunFailures"],
        json!(2),
        "two Jobs ran, so two failures — the third pass created nothing. Status: {after_third}"
    );
    assert_eq!(
        after_third["lastEnforcement"]["runId"], after_second["lastEnforcement"]["runId"],
        "and the record still describes run 2, not a resurrected run 1: {}",
        after_third["lastEnforcement"]
    );
    assert!(
        after_third["lastEnforcement"]["finishedAt"].is_string(),
        "which is still finished, so nothing hands its Job back to be harvested again"
    );
    let degraded = condition_of(&after_third, ctrl::CONDITION_DEGRADED).expect("a condition");
    assert_eq!(
        degraded["status"], "False",
        "the budget is not spent: {degraded}"
    );
}

// ===========================================================================
// RET-STALE-PLANREF and RET-STARTRUN-PATCH-OUTCOME
// ===========================================================================

/// **`planRef` MOVES WITH THE EVALUATION THAT RENDERED THE PLAN** — defect
/// RET-STALE-PLANREF.
///
/// `planRef` was written by `start_run` and by nothing else, so an evaluation
/// that rendered a NEW plan WITHOUT starting a run left the ref naming the
/// previous run's `ConfigMap` while `planSha256` beside it named the new
/// plan's bytes: two fields of one `lastEvaluation` block describing two
/// different plans. The ref is the one an administrator follows to preview
/// what a run would delete, so the stale one is the one that gets read.
///
/// The two passes evaluate DIFFERENT catalogs, so the digest really moves —
/// a row over one catalog would pass with the field never written at all.
/// Neither pass starts a run (`requireApprovedPlan: true`, nothing approved),
/// which is exactly the case the defect lived in.
///
/// KILLS: removing `planRef` from `publish_evaluation`'s `lastEvaluation`;
/// writing it from anywhere that only a starting pass reaches.
#[tokio::test]
async fn a_re_evaluation_moves_the_plan_ref_with_the_digest() {
    let first = fixture(happy_routes(&six_points()));
    run(&first, &policy(enforcing(None), json!({}))).await;
    let first_status = first.status();
    let first_digest = first_status["lastEvaluation"]["planSha256"]
        .as_str()
        .expect("a digest")
        .to_string();
    let first_ref = first_status["lastEvaluation"]["planRef"]["name"]
        .as_str()
        .expect("an evaluation names the plan it rendered")
        .to_string();
    assert_eq!(
        first_ref,
        format!(
            "{}-plan-{}",
            stem(),
            &first_digest.trim_start_matches("sha256:")[..12]
        ),
        "the ref names the ConfigMap this digest's plan is carried in"
    );
    assert!(
        first.posted("/jobs").is_empty(),
        "and no run started, which is the whole case: {:?}",
        first.seen()
    );

    // A DIFFERENT CATALOG, so a different plan and a different digest.
    let mut fewer = six_points();
    fewer.truncate(4);
    let second = fixture(happy_routes(&fewer));
    run(&second, &policy(enforcing(None), first_status.clone())).await;
    let second_status = after(&first_status, &second);
    let second_digest = second_status["lastEvaluation"]["planSha256"]
        .as_str()
        .expect("a digest")
        .to_string();
    assert_ne!(
        first_digest, second_digest,
        "the fixture must actually re-render, or this row asserts nothing"
    );

    let second_ref = second_status["lastEvaluation"]["planRef"]["name"]
        .as_str()
        .expect("the re-evaluation names its own plan")
        .to_string();
    assert_ne!(
        first_ref, second_ref,
        "the ref moved with the digest rather than naming the plan of a run that is over"
    );
    assert_eq!(
        second_ref,
        format!(
            "{}-plan-{}",
            stem(),
            &second_digest.trim_start_matches("sha256:")[..12]
        ),
        "and it names THIS evaluation's plan: {}",
        second_status["lastEvaluation"]
    );
}

/// **THE RUN RECORD IS WRITTEN BEFORE THE JOB EXISTS** — defect
/// RET-STARTRUN-PATCH-OUTCOME, the ordering half.
///
/// The Job used to be created first and the record patch's outcome discarded,
/// so a record write refused — or a controller that died — after the create
/// left a deletion Job the status never tracks: `tracked_run` reads
/// `status.lastEnforcement`, finds the previous run or none, and never harvests
/// this one. An orphan that deletes objects, reports its outcome to nobody and
/// is counted against no retry budget.
///
/// KILLS: swapping the two writes back.
#[tokio::test]
async fn the_run_record_is_written_before_the_job_is_created() {
    let digest = learned_digest().await;
    let mut routes = happy_routes(&six_points());
    routes.push(plan_config_map_route(&digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.extend(absent_job_routes(&digest, now()));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let f = fixture(routes);
    let outcome = run(&f, &policy(unattended_enforcing(), json!({}))).await;
    assert_eq!(outcome.phase, ctrl::RetentionPhase::Started);

    let bodies = f.bodies.lock().expect("the body recorder").clone();
    let record_at = bodies
        .iter()
        .position(|b| {
            b.method == "PATCH"
                && b.uri.contains("/retentionpolicies/")
                && serde_json::from_str::<Value>(&b.body)
                    .ok()
                    .and_then(|v| {
                        v["status"]["lastEnforcement"]["runId"]
                            .as_str()
                            .map(str::to_string)
                    })
                    .is_some()
        })
        .expect("the run record was written");
    let create_at = bodies
        .iter()
        .position(|b| b.method == "POST" && b.uri.contains("/jobs"))
        .expect("the Job was created");
    assert!(
        record_at < create_at,
        "the record is on the server before the Job that deletes anything exists; got record at \
         {record_at} and create at {create_at}: {:?}",
        bodies
            .iter()
            .map(|b| (b.method.clone(), b.uri.clone()))
            .collect::<Vec<_>>()
    );

    // AND THE JOB IS READ BEFORE THE RECORD IS WRITTEN, which is what keeps the
    // new order from re-recording a run whose Job already stands (defect
    // RET-COUNT-EARLY's resurrection).
    let read_at = bodies
        .iter()
        .position(|b| b.method == "GET" && b.uri.contains("/jobs/"))
        .expect("the Job name was read");
    assert!(
        read_at < record_at,
        "got read at {read_at} and record at {record_at}"
    );
}

/// **A REFUSED RECORD WRITE CREATES NO JOB** — defect
/// RET-STARTRUN-PATCH-OUTCOME, the outcome half.
///
/// The third `/status` write of an enforcing pass is the run record (the
/// evaluation, then the lease, then the record). The double refuses exactly
/// that one with a `409`, which is the ordinary answer here — the lease patch
/// two steps up exists because this object's version moves under the pass
/// constantly.
///
/// ZERO `POST …/jobs`. Not "a Job that is later cleaned up": this controller
/// holds no `delete` verb on `jobs` by Global Constraint 6, so a Job created
/// past a refused record could never be withdrawn. The outcome names the
/// refusal so an operator can see why no run started.
///
/// KILLS: discarding the record patch's outcome; creating the Job first.
#[tokio::test]
async fn a_refused_run_record_creates_no_job() {
    let digest = learned_digest().await;
    let mut routes = happy_routes(&six_points());
    routes.push(plan_config_map_route(&digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.extend(absent_job_routes(&digest, now()));
    routes.push(route("POST", "/jobs", "{}".to_string()));
    let (client, recorder, bodies) = weirkeeper::testing::mock_client_failing_after(
        routes,
        "PATCH",
        "/retentionpolicies/primary/status",
        2,
        409,
        json!({
            "kind": "Status", "apiVersion": "v1", "status": "Failure",
            "reason": "Conflict", "code": 409,
            "message": "the object has been modified"
        })
        .to_string(),
    );
    let f = Fixture {
        client,
        recorder,
        bodies,
    };
    let outcome = run(&f, &policy(unattended_enforcing(), json!({}))).await;

    assert!(
        f.posted("/jobs").is_empty(),
        "no Job exists for a run the status does not record — and this controller could not \
         delete one if it did. Requests: {:?}",
        f.seen()
    );
    assert_eq!(
        outcome.enforced_reason,
        ctrl::REASON_RUN_NOT_RECORDED,
        "and the object says why no run started"
    );
    assert_eq!(
        f.status_patches().len(),
        3,
        "the evaluation, the lease and the refused record — the refusal is the THIRD write, \
         which is the sequence the defect lives in"
    );
}

/// **A JOB CREATE THAT FAILS LEAVES THE RETRY BUDGET ALONE** — review finding
/// MED-1.
///
/// The record is written before the Job (defect RET-STARTRUN-PATCH-OUTCOME), so
/// a create that answers anything but `201` or `409 AlreadyExists` leaves a
/// record with no Job. The next pass would reach `tracked_run`'s absent-Job arm
/// — `runId` present, `finishedAt` absent, Job absent — harvest it with no exit
/// code, count one consecutive FAILURE and say "its pod is gone or was never
/// readable" about a pod that never existed. Three transient `403`s from a
/// quota or an admission webhook would then wedge retention at
/// `EnforcementDegraded=True` until an operator edited the spec.
///
/// A run that was never created is not a failed run. The record is withdrawn
/// with an explicit `null`, and the row asserts BOTH halves: the withdrawal is
/// on the wire, and a pass over the status it leaves starts fresh with the
/// count and the budget untouched.
///
/// KILLS: `Err(e) => return Err(ReconcileError::Api(e))` without the
/// withdrawal.
#[tokio::test]
async fn a_job_create_failure_after_the_record_leaves_no_run_to_harvest() {
    let digest = learned_digest().await;
    let spec = unattended_enforcing();
    let mut routes = happy_routes(&six_points());
    routes.push(plan_config_map_route(&digest));
    routes.push(route("POST", "/configmaps", "{}".to_string()));
    routes.extend(absent_job_routes(&digest, now()));
    // THE CREATE IS REFUSED BY SOMETHING THAT IS NOT A NAME COLLISION — a
    // ResourceQuota, an admission webhook, a broken API server. Not a 409:
    // that arm means the Job exists and the record correctly describes it.
    routes.push(Route {
        method: "POST",
        path_suffix: "/jobs",
        status: 403,
        body: json!({
            "kind": "Status", "apiVersion": "v1", "status": "Failure",
            "reason": "Forbidden", "code": 403,
            "message": "exceeded quota: jobs"
        })
        .to_string(),
    });
    let f = fixture(routes);
    let before = json!({});
    let outcome = ctrl::reconcile_policy(
        &policy(spec.clone(), before.clone()),
        &ctrl::PolicyContext {
            client: &f.client,
            policy: &check::policy::Policy::defaults(),
            runner_image: &RunnerImage::default(),
            now: now(),
        },
    )
    .await;
    assert!(
        outcome.is_err(),
        "a create this controller cannot complete is an error the reconciler requeues on"
    );

    // THE WITHDRAWAL IS ON THE WIRE, as an explicit RFC 7386 null and not an
    // omission: an omitted key leaves the record exactly where it was.
    let withdrawal = f
        .status_patches()
        .into_iter()
        .filter(|p| p["status"].get("lastEnforcement").is_some())
        .next_back()
        .expect("the record was written and then withdrawn");
    assert_eq!(
        withdrawal["status"]["lastEnforcement"],
        Value::Null,
        "the last word about the record is that there is none; got {withdrawal}"
    );

    // AND THE STATUS THIS PASS LEAVES NAMES NO RUN, so the next pass has
    // nothing to harvest and nothing to count.
    let after_pass = after(&before, &f);
    assert!(
        after_pass.get("lastEnforcement").is_none_or(Value::is_null),
        "no run is recorded, so `tracked_run` finds none. Status: {after_pass}"
    );
    assert!(
        after_pass
            .get("consecutiveRunFailures")
            .is_none_or(|v| v == &json!(0)),
        "and the retry budget is untouched — a run that never ran is not a failed run. \
         Status: {after_pass}"
    );
    assert!(
        condition_of(&after_pass, ctrl::CONDITION_DEGRADED)
            .is_none_or(|c| c["status"] == json!("False")),
        "nothing is degraded by a Job that was never created. Status: {after_pass}"
    );
}

// ===========================================================================
// RETENTION-PLAN-IGNORES-REFUSED-VERDICT — the controller's reached verdict
// outranks a (possibly stale) view row
// ===========================================================================

/// A distinct, well-formed receipt digest per point id.
fn receipt_digest(id: &str) -> String {
    let hex: String = id
        .bytes()
        .map(|b| format!("{b:02x}"))
        .collect::<String>()
        .chars()
        .chain(std::iter::repeat('a'))
        .take(64)
        .collect();
    format!("sha256:{hex}")
}

/// [`view_entry`] with its own receipt digest — a stale, selectable,
/// `Available`/`Verified` row for exactly that receipt.
fn view_entry_for_receipt(id: &str, age_days: i64) -> Value {
    let mut entry = view_entry(id, age_days);
    entry["receiptSha256"] = json!(receipt_digest(id));
    entry
}

/// A `Backup` whose own evidence verdict is `result`, over `digest` (or none).
fn verdict_backup(name: &str, set: &str, digest: Option<&str>, result: &str) -> Value {
    let mut evidence = json!({"verification": {"result": result}});
    if let Some(d) = digest {
        evidence["receiptSha256"] = json!(d);
    }
    json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "metadata": {"name": name, "namespace": NS, "uid": format!("uid-{name}"), "resourceVersion": "9"},
        "spec": {
            "sourceRef": {"name": "prod-kafka"},
            "topics": ["orders"],
            "archive": {"url": format!("logweir-destination://{DEST}")},
            "destinationRef": {"name": DEST},
            "triggeredBy": "schedule/nightly",
            "deadlineSeconds": 3600
        },
        "status": {
            "phase": "Succeeded",
            "exitCode": 0,
            "backupId": set,
            "evidence": evidence
        }
    })
}

fn backup_objects(values: Vec<Value>) -> Vec<weirkeeper::crds::backup::Backup> {
    values
        .into_iter()
        .map(|v| serde_json::from_value(v).expect("the fixture is a Backup"))
        .collect()
}

fn backup_list_body(items: Vec<Value>, continue_token: Option<&str>) -> String {
    let mut metadata = json!({"resourceVersion": "1"});
    if let Some(t) = continue_token {
        metadata["continue"] = json!(t);
    }
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupList",
        "metadata": metadata,
        "items": items
    })
    .to_string()
}

/// Three rows, newest first: `p1` (1 day), `p2` (2 days), `p3` (3 days).
fn three_rows() -> Vec<weirkeeper::catalog_view::ViewEntry> {
    (1..=3)
        .map(|d| {
            serde_json::from_value(view_entry_for_receipt(&format!("p{d}"), d))
                .expect("the row parses")
        })
        .collect()
}

/// **The defect row.** `p1`'s view row is stale — `Available`, `Verified`,
/// `selectable` — but the controller fetched `p1`'s receipt and recorded its
/// `Backup` `Invalid` (or `Untrusted`, or a verdict this build does not know).
/// With `keepLast: 2` the refused point used to take a keep rank and push the
/// older GOOD point `p3` into the plan. It is now skipped `Unreadable` — never
/// counted as usable and never a candidate itself — and `p3` is kept.
///
/// MUTANT: drop the `refused_by_controller` arm of
/// `retention_plan::skip_reason` (or make `point_facts` ignore `refusals`).
/// `p1` is usable again and `p3` is planned `BeyondKeepLast`; this row fails.
#[test]
fn a_point_the_controller_refused_never_makes_an_older_good_point_a_candidate() {
    let rows = three_rows();
    for result in ["Invalid", "Untrusted", "SomeFutureVerdict"] {
        let refusals =
            weirkeeper::catalog_view::ControllerRefusals::from_backups(&backup_objects(vec![
                verdict_backup("b-p1", "set-p1", Some(&receipt_digest("p1")), result),
            ]));
        let points: Vec<plan::PointFacts> = rows
            .iter()
            .map(|e| ctrl::point_facts(e, &refusals))
            .collect();
        assert!(
            points[0].refused_by_controller,
            "{result} is a reached refusal"
        );
        assert!(!points[1].refused_by_controller && !points[2].refused_by_controller);

        let evaluation = evaluate(&points, rules(Some(2), None, 1));
        assert!(
            candidate_ids(&evaluation).is_empty(),
            "a {result} Backup under a stale selectable row must not push p3 out of keepLast; \
             candidates: {:?}",
            candidate_ids(&evaluation)
        );
        assert_eq!(
            evaluation.skipped,
            vec![plan::Skipped {
                point_id: "p1".to_string(),
                reason: plan::SkipReason::Unreadable,
            }],
            "the refused point is skipped, so it is neither usable nor deletable ({result})"
        );
        assert_eq!(evaluation.kept, vec!["p2".to_string(), "p3".to_string()]);
    }

    // CONTROL: the honest "could not look" defers to the same row, and the
    // rule then plans p3 exactly as before — which is what makes the row above
    // a measurement and not a constant.
    // `Pending` is the evidence-fetch Job still reading: not a verdict.
    for result in ["NotAttempted", "Pending", "Valid"] {
        let refusals =
            weirkeeper::catalog_view::ControllerRefusals::from_backups(&backup_objects(vec![
                verdict_backup("b-p1", "set-p1", Some(&receipt_digest("p1")), result),
            ]));
        assert!(refusals.is_empty(), "{result} is not a refusal");
        let points: Vec<plan::PointFacts> = rows
            .iter()
            .map(|e| ctrl::point_facts(e, &refusals))
            .collect();
        let evaluation = evaluate(&points, rules(Some(2), None, 1));
        assert_eq!(
            candidate_ids(&evaluation),
            vec!["p3"],
            "{result}: the row decides, p1 counts, p3 is beyond keepLast"
        );
        assert!(evaluation.skipped.is_empty());
    }
}

/// The join is the FULL receipt digest where the `Backup` has one, and the
/// archive set id only where it has none — and a catalog-only point (no
/// `Backup` names it) is unchanged.
#[test]
fn the_refusal_join_is_by_receipt_digest_then_by_set_id() {
    let rows = three_rows();
    let refusals =
        weirkeeper::catalog_view::ControllerRefusals::from_backups(&backup_objects(vec![
            // A digest-less (legacy-runner) Backup: joins p2 by `backupId`.
            verdict_backup("b-p2", "set-p2", None, "Invalid"),
            // A Backup of p3's SET whose digest names other bytes: the digest
            // decides, so p3's row is not refused by it.
            verdict_backup("b-p3", "set-p3", Some(&receipt_digest("other")), "Invalid"),
        ]));
    let facts: Vec<bool> = rows
        .iter()
        .map(|e| ctrl::point_facts(e, &refusals).refused_by_controller)
        .collect();
    assert_eq!(facts, vec![false, true, false]);
    assert_eq!(refusals.refusal_for(&rows[1]), Some("Invalid"));
}

/// The controller path: the namespace's `Backup`s are listed, and a stale row
/// for a refused `Backup` changes the plan the status publishes.
///
/// Six rows, `keepLast: 2`, `minUsablePoints: 3`: with nothing refused the plan
/// is p4, p5, p6. With p1's `Backup` recorded `Invalid`, p1 is skipped and the
/// floor keeps p2..p4, so p4 — an older good point — is NOT planned.
#[tokio::test]
async fn the_controller_joins_backup_verdicts_before_it_counts_a_row() {
    let entries: Vec<Value> = (1..=6)
        .map(|d| view_entry_for_receipt(&format!("p{d}"), d))
        .collect();
    let mut routes = happy_routes(&entries);
    routes.retain(|r| r.path_suffix != "/backups");
    routes.push(route(
        "GET",
        "/backups",
        backup_list_body(
            vec![verdict_backup(
                "b-p1",
                "set-p1",
                Some(&receipt_digest("p1")),
                "Invalid",
            )],
            None,
        ),
    ));
    let f = fixture(routes);
    let outcome = run(&f, &policy(json!({}), json!({}))).await;

    assert_eq!(outcome.phase, ctrl::RetentionPhase::Evaluated);
    assert_eq!(outcome.candidates, 2, "p5 and p6 only");
    assert_eq!(outcome.skipped, 1);
    let status = f.status();
    let planned: Vec<&str> = status["lastEvaluation"]["candidates"]
        .as_array()
        .expect("candidates")
        .iter()
        .filter_map(|c| c["pointId"].as_str())
        .collect();
    assert_eq!(planned, vec!["p5", "p6"]);
    assert_eq!(
        status["lastEvaluation"]["skipped"],
        json!([{"pointId": "p1", "reason": "Unreadable"}])
    );
    assert!(
        f.seen()
            .iter()
            .any(|(m, uri)| m == "GET" && uri.contains("/backups")),
        "the Backup verdicts were read"
    );
}

/// A `Backup` listing the bound cut short is an evaluation that could not be
/// completed: `Evaluated=False`, no plan, nothing to approve — the refusal the
/// walk did not reach is exactly the one that would have mattered.
///
/// MUTANT: return the partial set instead of `Err` at the bound. The pass
/// evaluates and publishes a plan; this row fails.
#[tokio::test]
async fn an_incomplete_backup_listing_plans_nothing() {
    let mut routes = happy_routes(&six_points());
    routes.retain(|r| r.path_suffix != "/backups");
    // Every page says there is more, so the walk reaches its bound.
    routes.push(route(
        "GET",
        "/backups",
        backup_list_body(vec![], Some("eyJwYWdlIjoyfQ")),
    ));
    let f = fixture(routes);
    let outcome = run(&f, &policy(json!({}), json!({}))).await;

    // REVIEW L2: the operator is sent to the Backup history, not the catalog.
    assert_eq!(outcome.ready_reason, ctrl::REASON_BACKUP_HISTORY_TOO_LARGE);
    assert_eq!(outcome.candidates, 0);
    assert_eq!(
        f.condition(ctrl::CONDITION_READY)["reason"],
        ctrl::REASON_BACKUP_HISTORY_TOO_LARGE
    );
    assert_eq!(
        f.condition(ctrl::CONDITION_EVALUATED)["reason"],
        ctrl::REASON_BACKUP_VERDICTS_INCOMPLETE
    );
    let message = f.condition(ctrl::CONDITION_READY)["message"]
        .as_str()
        .expect("a message")
        .to_string();
    assert!(message.contains("Backup objects") && message.contains("Backup history"));
    assert!(f.condition(ctrl::CONDITION_ENFORCED)["message"]
        .as_str()
        .expect("a message")
        .contains("Backup verdicts"));
    assert!(
        f.status()["lastEvaluation"].is_null(),
        "no evaluation is published from an incomplete listing"
    );
    let pages = f
        .seen()
        .iter()
        .filter(|(m, uri)| m == "GET" && uri.contains("/backups"))
        .count();
    assert_eq!(pages, ctrl::MAX_BACKUP_PAGES, "the walk is bounded");
}

// ---- review M1: the Backup read is lenient, and its failures are published --

fn routes_with_backups(body: String, status: u16) -> Vec<Route> {
    let entries: Vec<Value> = (1..=6)
        .map(|d| view_entry_for_receipt(&format!("p{d}"), d))
        .collect();
    let mut routes = happy_routes(&entries);
    routes.retain(|r| r.path_suffix != "/backups");
    let mut backups = route("GET", "/backups", body);
    backups.status = status;
    routes.push(backups);
    routes
}

/// ONE `Backup` this build cannot type (a trigger kind a newer build wrote, and
/// one with no `spec` at all) does not wedge the evaluation, and the refusal on
/// another `Backup` still applies: the same plan as
/// `the_controller_joins_backup_verdicts_before_it_counts_a_row`.
///
/// MUTANT: list typed `Backup`s again. The reconcile errors and this row fails.
#[tokio::test]
async fn a_malformed_backup_neither_wedges_retention_nor_hides_a_refusal() {
    let mut future = verdict_backup("b-future", "set-x", Some(&receipt_digest("x")), "Valid");
    // `spec.trigger.kind` is a CLOSED enum in this build (`TriggerKind`).
    future["spec"]["trigger"] = json!({"kind": "SomeFutureTriggerKind", "attempt": 0});
    let bare = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Backup",
        "metadata": {"name": "b-bare", "namespace": NS, "resourceVersion": "3"},
        "status": {"phase": "Running"}
    });
    let f = fixture(routes_with_backups(
        backup_list_body(
            vec![
                future,
                bare,
                verdict_backup("b-p1", "set-p1", Some(&receipt_digest("p1")), "Invalid"),
            ],
            None,
        ),
        200,
    ));
    let outcome = run(&f, &policy(json!({}), json!({}))).await;
    assert_eq!(outcome.phase, ctrl::RetentionPhase::Evaluated);
    assert_eq!(outcome.candidates, 2, "p5 and p6 only");
    assert_eq!(
        f.status()["lastEvaluation"]["skipped"],
        json!([{"pointId": "p1", "reason": "Unreadable"}])
    );
}

/// A verdict field that is present but not a verdict word refuses its point.
///
/// MUTANT: read an unreadable verdict as absent in
/// `ControllerRefusals::from_facts`. p1 counts again and p4 is planned.
#[tokio::test]
async fn an_unreadable_backup_verdict_refuses_its_point() {
    let mut odd = verdict_backup("b-p1", "set-p1", Some(&receipt_digest("p1")), "Invalid");
    odd["status"]["evidence"]["verification"] = json!("not-an-object");
    let f = fixture(routes_with_backups(backup_list_body(vec![odd], None), 200));
    let outcome = run(&f, &policy(json!({}), json!({}))).await;
    assert_eq!(outcome.phase, ctrl::RetentionPhase::Evaluated);
    assert_eq!(outcome.candidates, 2);
    assert_eq!(
        f.status()["lastEvaluation"]["skipped"],
        json!([{"pointId": "p1", "reason": "Unreadable"}])
    );
}

/// A failed `Backup` LIST is an answer on the object — `Evaluated=False` with
/// its own reason, nothing planned — not a silent error-requeue loop.
///
/// MUTANT: propagate the list error (`?`) again. `reconcile_policy` returns
/// `Err` and this row fails at `run`.
#[tokio::test]
async fn a_failed_backup_list_is_published_and_plans_nothing() {
    let forbidden = json!({
        "kind": "Status", "apiVersion": "v1", "status": "Failure",
        "message": "backups is forbidden", "reason": "Forbidden", "code": 403
    })
    .to_string();
    let f = fixture(routes_with_backups(forbidden, 403));
    let outcome = run(&f, &policy(json!({}), json!({}))).await;
    assert_eq!(
        outcome.ready_reason,
        ctrl::REASON_BACKUP_VERDICTS_UNREADABLE
    );
    assert_eq!(outcome.candidates, 0);
    assert_eq!(
        f.condition(ctrl::CONDITION_EVALUATED)["reason"],
        ctrl::REASON_BACKUP_VERDICTS_INCOMPLETE
    );
    assert!(f.condition(ctrl::CONDITION_READY)["message"]
        .as_str()
        .expect("a message")
        .contains("could not be listed"));
    assert!(f.status()["lastEvaluation"].is_null());
}

/// A refusal that names neither a digest nor a set id cannot be tied to any
/// point, so retention cannot prove it spares the one that matters: nothing is
/// planned, and the object says why.
///
/// MUTANT: ignore `unattributed()` in `controller_refusals`. The pass
/// evaluates and publishes a plan; this row fails.
#[tokio::test]
async fn an_unattributable_refusal_plans_nothing() {
    let mut anonymous = verdict_backup("b-anon", "", None, "Invalid");
    anonymous["status"]
        .as_object_mut()
        .expect("status")
        .remove("backupId");
    let f = fixture(routes_with_backups(
        backup_list_body(vec![anonymous], None),
        200,
    ));
    let outcome = run(&f, &policy(json!({}), json!({}))).await;
    assert_eq!(
        outcome.ready_reason,
        ctrl::REASON_BACKUP_VERDICTS_UNREADABLE
    );
    assert_eq!(outcome.candidates, 0);
    assert!(f.status()["lastEvaluation"].is_null());
}

/// Review L3: a SKIPPED point is retained, so a segment it shares with a
/// candidate protects that candidate (`SharedSegment`) — a refused point never
/// loses a segment through another point's deletion.
///
/// MUTANT: build `retained_segments` from the usable verdicts only. The
/// candidate is planned and this row fails.
#[test]
fn a_skipped_points_segments_protect_a_candidate_that_shares_them() {
    let mut points: Vec<plan::PointFacts> = (1..=3).map(|d| point(&format!("p{d}"), d)).collect();
    // p1 is refused by the controller; p3 would be BeyondKeepLast.
    points[0].refused_by_controller = true;
    points[0].segment_keys = vec![format!("{SCOPE}/shared/seg-0001")];
    points[2].segment_keys = vec![format!("{SCOPE}/shared/seg-0001")];
    let evaluation = evaluate(&points, rules(Some(1), None, 1));
    assert!(
        candidate_ids(&evaluation).is_empty(),
        "p3 shares a segment with the retained, skipped p1: {:?}",
        candidate_ids(&evaluation)
    );
    assert_eq!(protected_reason(&evaluation, "p3"), Some("SharedSegment"));
}
