//! FX-22: the product API reads the SAME four counts a `RetentionPolicy`'s
//! status carries, and never calls a point the per-run ceiling held back
//! "kept".
//!
//! THE EVIDENCE (PoC batch 3, F-3): 371 points, and the status read "321 kept,
//! 50 candidate(s)" for `keepLast: 300` and for `keepLast: 10` alike. The
//! controller now publishes `keptCount`, `candidateCount`, `truncatedByCap`
//! and the ceiling it applied; this is the row that the API passes them on,
//! exactly, and publishes NOTHING as kept when they are not recorded.
//!
//! THREE SIDES READ ONE CHAIN OF FIXTURES.
//!
//! * `ui/tests/fixtures/retention-held-back.json` is two `RetentionPolicy`
//!   objects as the reconciler leaves them —
//!   `weirkeeper/tests/retention_policy_controller.rs`
//!   `fx22_the_shared_fixture_is_what_the_controller_writes` fails when the
//!   file is not that.
//! * `ui/tests/fixtures/console/retention-policies-held-back.json` is this
//!   crate's answer for those two objects, served through the real router
//!   (below).
//! * `ui/tests/d3.spec.js` decodes and renders the console fixture, and
//!   `ui/tests/contract.spec.js` holds it to the published schema.

mod support;

use logweir_api::routes::retention::{published_view_incomplete, view, MAX_ROWS};
use serde_json::{json, Value};
use support::{repo_root, FakeKube, Options, TestApp, NS_A};
use weirkeeper::crds::retention_policy::RetentionPolicy;

/// The console's copy of this crate's answer.
const CONSOLE_FIXTURE: &str = "ui/tests/fixtures/console/retention-policies-held-back.json";

/// A fixed request id for the checked-in answer; the router mints a new one
/// on every request.
const FIXTURE_REQUEST_ID: &str = "01FX22HELDBACK0000000000000";

/// The two objects the controller's own test wrote: `keep-300` and `keep-10`.
fn held_back() -> Vec<Value> {
    support::fixture("retention-held-back.json")["items"]
        .as_array()
        .expect("the fixture is a list")
        .clone()
}

fn named(name: &str) -> Value {
    held_back()
        .into_iter()
        .find(|o| o["metadata"]["name"] == name)
        .unwrap_or_else(|| panic!("the fixture has no policy named {name}"))
}

fn project(object: Value) -> Value {
    let policy: RetentionPolicy = serde_json::from_value(object).expect("a RetentionPolicy");
    serde_json::to_value(view(
        &policy,
        "2026-09-17T04:20:00Z".parse().expect("an instant"),
    ))
    .expect("the view serialises")
}

fn ids(list: &Value) -> Vec<String> {
    list.as_array()
        .map(|rows| {
            rows.iter()
                .map(|v| {
                    v.as_str()
                        .or_else(|| v["pointId"].as_str())
                        .expect("a point id")
                        .to_string()
                })
                .collect()
        })
        .unwrap_or_default()
}

/// **THE ROW.** The two policies, through the real router: each answer
/// carries a kept count, a plan count and a held-back count that are the
/// status's own, and the two answers differ.
///
/// MUTANTS: A1 `keptCount` derived from the rows (`keep-300` publishes 200
/// rows of its 300, so a count of rows reads 200); A2 `keptCount` and
/// `truncatedByCap` swapped; A3 `truncatedByCap` never published.
#[tokio::test]
async fn the_api_reads_the_counts_the_controller_wrote() {
    let fake = FakeKube::new();
    for object in held_back() {
        fake.seed("retentionpolicies", NS_A, object);
    }
    let app = TestApp::with(fake, Options::default());
    let body = app
        .get("/api/v1/namespaces/team-a/retention-policies")
        .await;
    assert_eq!(body.status.as_u16(), 200);
    let answer = body.json();
    let item = |name: &str| -> Value {
        answer["items"]
            .as_array()
            .expect("items")
            .iter()
            .find(|i| i["name"] == name)
            .unwrap_or_else(|| panic!("no {name} in {answer}"))
            .clone()
    };

    // ---- keepLast: 300 ----------------------------------------------------
    let three_hundred = item("keep-300");
    let ev = &three_hundred["lastEvaluation"];
    assert_eq!(ev["pointsEvaluated"], 371, "{ev}");
    assert_eq!(ev["accounting"], "Recorded", "{ev}");
    assert_eq!(
        ev["viewIncomplete"],
        json!(false),
        "the fixture's catalog said its view is the whole archive"
    );
    assert_eq!(ev["keptCount"], 300);
    assert_eq!(ev["candidateCount"], 50);
    assert_eq!(ev["truncatedByCap"], 21);
    assert_eq!(ev["maxDeletionsPerRun"], 50);
    assert_eq!(
        ev["kept"].as_array().expect("kept rows").len(),
        MAX_ROWS,
        "the ROWS are cut at this route's bound"
    );
    assert_eq!(ev["truncated"], true, "and the response says so");
    assert_eq!(
        ev["keptCount"], 300,
        "while the COUNT is the status's own and not the number of rows"
    );
    assert_eq!(ev["candidates"].as_array().expect("candidates").len(), 50);

    // ---- keepLast: 10 -----------------------------------------------------
    let ten = item("keep-10");
    let ev = &ten["lastEvaluation"];
    assert_eq!(ev["pointsEvaluated"], 371, "{ev}");
    assert_eq!(ev["accounting"], "Recorded", "{ev}");
    assert_eq!(ev["keptCount"], 10);
    assert_eq!(ev["candidateCount"], 50);
    assert_eq!(ev["truncatedByCap"], 311);
    assert_eq!(ev["maxDeletionsPerRun"], 50);
    assert_eq!(ev["truncated"], false);
    let kept = ids(&ev["kept"]);
    assert_eq!(kept.len(), 10);
    assert_eq!(kept.first().map(String::as_str), Some("p001"));
    assert_eq!(kept.last().map(String::as_str), Some("p010"));

    // NO HELD-BACK POINT IS PUBLISHED AS ANYTHING: `p061`…`p371` are due and
    // over the ceiling, and they are in none of the four lists.
    let listed: Vec<String> = ids(&ev["kept"])
        .into_iter()
        .chain(ids(&ev["candidates"]))
        .chain(ids(&ev["protected"]))
        .chain(ids(&ev["skipped"]))
        .collect();
    assert_eq!(listed.len(), 60);
    for held in (61..=371).map(|d| format!("p{d:03}")) {
        assert!(!listed.contains(&held), "{held} is held back and is listed");
    }
    // And the two policies no longer read alike.
    assert_ne!(
        (
            &three_hundred["lastEvaluation"]["keptCount"],
            &three_hundred["lastEvaluation"]["truncatedByCap"]
        ),
        (
            &ten["lastEvaluation"]["keptCount"],
            &ten["lastEvaluation"]["truncatedByCap"]
        )
    );

    // THE ACCOUNTING CLOSES ON THE RESPONSE TOO.
    for policy in [&three_hundred, &ten] {
        let ev = &policy["lastEvaluation"];
        let number = |key: &str| ev[key].as_i64().expect("a count");
        assert_eq!(
            number("pointsEvaluated"),
            number("keptCount")
                + number("candidateCount")
                + number("truncatedByCap")
                + ev["skipped"].as_array().map_or(0, Vec::len) as i64
        );
    }

    // THE CONSOLE'S FIXTURE IS THIS ANSWER, with the request id pinned.
    let mut document = answer.clone();
    document["requestId"] = json!(FIXTURE_REQUEST_ID);
    let want = format!(
        "{}\n",
        serde_json::to_string_pretty(&document).expect("the answer serialises")
    );
    let path = repo_root().join(CONSOLE_FIXTURE);
    if std::env::var_os("LOGWEIR_WRITE_FIXTURES").is_some() {
        std::fs::write(&path, &want).expect("the fixture is writable");
    }
    let got = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "{} is missing ({e}); regenerate it with LOGWEIR_WRITE_FIXTURES=1",
            path.display()
        )
    });
    assert_eq!(
        got, want,
        "{CONSOLE_FIXTURE} has drifted from this crate's answer for \
         ui/tests/fixtures/retention-held-back.json. Regenerate it with LOGWEIR_WRITE_FIXTURES=1 \
         cargo test --locked -p logweir-api --test retention_accounting"
    );
    app.fake.assert_strict();
}

/// **Absent means not recorded, and then nothing is published as kept.**
///
/// A status written by a controller that did not record what the per-run
/// ceiling held back lists those points under `kept` — 321 ids for
/// `keepLast: 10`, which is the defect. The API cannot tell which of them the
/// rules keep, so it publishes no kept count, no held-back count and no
/// `kept` rows; the plan (`candidateCount`, `candidates`) is unaffected.
///
/// MUTANTS: A4 publish the `kept` rows whatever the accounting says; A5 fall
/// back to the list's length for `keptCount`.
#[test]
fn an_evaluation_without_the_accounting_publishes_nothing_as_kept() {
    // CONTROL: the controller's own object publishes all of it.
    let current = project(named("keep-10"));
    assert_eq!(current["lastEvaluation"]["accounting"], "Recorded");
    assert_eq!(current["lastEvaluation"]["keptCount"], 10);
    assert_eq!(current["lastEvaluation"]["truncatedByCap"], 311);
    assert_eq!(ids(&current["lastEvaluation"]["kept"]).len(), 10);

    // THE PRE-FIX SHAPE: no counts, and the held-back points under `kept`.
    let mut older = named("keep-10");
    {
        let ev = older["status"]["lastEvaluation"]
            .as_object_mut()
            .expect("lastEvaluation");
        for key in [
            "keptCount",
            "truncatedByCap",
            "maxDeletionsPerRun",
            "viewIncomplete",
        ] {
            ev.remove(key);
        }
        let old_kept: Vec<String> = (1..=10)
            .chain(61..=371)
            .map(|d| format!("p{d:03}"))
            .collect();
        assert_eq!(old_kept.len(), 321, "as PoC batch 3 read it");
        ev.insert("kept".to_string(), json!(old_kept));
    }
    let view = project(older);
    let ev = &view["lastEvaluation"];
    assert_eq!(
        ev["accounting"], "NotRecorded",
        "and the response SAYS the accounting is not recorded (review M1): {ev}"
    );
    for absent in [
        "keptCount",
        "truncatedByCap",
        "maxDeletionsPerRun",
        "viewIncomplete",
        "kept",
    ] {
        assert!(
            ev.get(absent).is_none(),
            "{absent} is published from a status that does not record the accounting: {ev}"
        );
    }
    assert_eq!(ev["pointsEvaluated"], 371);
    assert_eq!(ev["candidateCount"], 50, "the plan is still the plan");
    assert_eq!(ev["candidates"].as_array().expect("candidates").len(), 50);
    assert_eq!(
        ev["truncated"], false,
        "a list that is not published was not cut short: 321 ids are over this route's {MAX_ROWS} \
         and none of them is in the response"
    );

    // A LIVE PRE-FIX OBJECT, as the lab's API server returned it.
    let live: Value = serde_json::from_str(
        &std::fs::read_to_string(
            repo_root().join("crates/logweir-api/tests/fixtures/retention-policy-report.json"),
        )
        .expect("the live fixture"),
    )
    .expect("JSON");
    assert_eq!(
        live["status"]["lastEvaluation"]["kept"]
            .as_array()
            .expect("the live status lists kept ids")
            .len(),
        1
    );
    let view = project(live);
    let ev = &view["lastEvaluation"];
    assert_eq!(ev["accounting"], "NotRecorded", "{ev}");
    assert!(ev.get("keptCount").is_none(), "{ev}");
    assert!(ev.get("truncatedByCap").is_none(), "{ev}");
    assert!(ev.get("kept").is_none(), "{ev}");
    assert_eq!(ev["candidateCount"], 3);
}

/// **The response says whether the accounting is recorded** (review M1).
/// `lastEvaluation.accounting` is on every answer, `Recorded` or
/// `NotRecorded`, so a client never has to infer it from a member that is
/// missing.
///
/// THE TWO ANSWERS THE ABSENCE OF `kept` COULD NOT TELL APART, through the
/// real router:
///
/// * an older-shape status (no counts, the held-back points under `kept`)
///   answers `NotRecorded`, with no `kept` member and no counts;
/// * THE CONTROL, a policy that keeps NOTHING and records it (both of its
///   points skipped), answers `Recorded` with `keptCount: 0` — and no `kept`
///   member either, because an empty list is not serialised.
///
/// Before this member the two read alike to a client of `kept`: no member,
/// `truncated: false`.
///
/// MUTANTS: F1a — the member is the constant `Recorded`: the older shape reads
/// `Recorded` beside no counts. F1b — the constant `NotRecorded`: the control
/// and the controller's own two policies read `NotRecorded` beside their
/// counts.
#[tokio::test]
async fn the_response_says_whether_the_accounting_is_recorded() {
    // The pre-FX-22 shape of `keep-10`.
    let mut older = named("keep-10");
    older["metadata"]["name"] = json!("older-shape");
    older["metadata"]["uid"] = json!("22220001-0000-4000-8000-000000000022");
    {
        let ev = older["status"]["lastEvaluation"]
            .as_object_mut()
            .expect("lastEvaluation");
        for key in [
            "keptCount",
            "truncatedByCap",
            "maxDeletionsPerRun",
            "viewIncomplete",
        ] {
            ev.remove(key);
        }
        let old_kept: Vec<String> = (1..=10)
            .chain(61..=371)
            .map(|d| format!("p{d:03}"))
            .collect();
        ev.insert("kept".to_string(), json!(old_kept));
    }
    // A policy that keeps nothing, and says so.
    let mut nothing_kept = named("keep-10");
    nothing_kept["metadata"]["name"] = json!("keeps-nothing");
    nothing_kept["metadata"]["uid"] = json!("22220000-0000-4000-8000-000000000022");
    {
        let ev = &mut nothing_kept["status"]["lastEvaluation"];
        ev["pointsEvaluated"] = json!(2);
        ev["keptCount"] = json!(0);
        ev["candidateCount"] = json!(0);
        ev["truncatedByCap"] = json!(0);
        ev["kept"] = json!([]);
        ev["candidates"] = json!([]);
        ev["skipped"] = json!([
            {"pointId": "a", "reason": "Unreadable"},
            {"pointId": "b", "reason": "Unreadable"}
        ]);
    }

    let fake = FakeKube::new();
    fake.seed("retentionpolicies", NS_A, older);
    fake.seed("retentionpolicies", NS_A, nothing_kept);
    fake.seed("retentionpolicies", NS_A, named("keep-300"));
    let app = TestApp::with(fake, Options::default());
    let body = app
        .get("/api/v1/namespaces/team-a/retention-policies")
        .await;
    assert_eq!(body.status.as_u16(), 200);
    let answer = body.json();
    let evaluation = |name: &str| -> Value {
        answer["items"]
            .as_array()
            .expect("items")
            .iter()
            .find(|i| i["name"] == name)
            .unwrap_or_else(|| panic!("no {name} in {answer}"))["lastEvaluation"]
            .clone()
    };

    let older = evaluation("older-shape");
    assert_eq!(older["accounting"], "NotRecorded", "{older}");
    for absent in ["kept", "keptCount", "truncatedByCap", "maxDeletionsPerRun"] {
        assert!(older.get(absent).is_none(), "{absent}: {older}");
    }
    assert_eq!(
        older["candidateCount"], 50,
        "the plan is still the plan: {older}"
    );

    let nothing = evaluation("keeps-nothing");
    assert_eq!(nothing["accounting"], "Recorded", "{nothing}");
    assert_eq!(
        nothing["keptCount"],
        json!(0),
        "zero is an answer: {nothing}"
    );
    assert_eq!(nothing["truncatedByCap"], json!(0));
    assert!(
        nothing.get("kept").is_none(),
        "PREMISE: a policy that keeps nothing has no `kept` member either, which is why the \
         absence of the member could not be the signal: {nothing}"
    );
    assert_eq!(
        (older.get("kept"), older["truncated"].clone()),
        (nothing.get("kept"), nothing["truncated"].clone()),
        "PREMISE: to a reader of `kept` and `truncated` alone the two are the same answer"
    );
    assert_ne!(older["accounting"], nothing["accounting"]);

    let recorded = evaluation("keep-300");
    assert_eq!(recorded["accounting"], "Recorded");
    assert_eq!(recorded["keptCount"], 300);

    // The member is one of exactly two words, and always there.
    for item in answer["items"].as_array().expect("items") {
        let word = item["lastEvaluation"]["accounting"]
            .as_str()
            .unwrap_or_else(|| panic!("no `accounting` on {}", item["name"]));
        assert!(matches!(word, "Recorded" | "NotRecorded"), "{word}");
    }
    app.fake.assert_strict();
}

/// **A `kept` list that is not the recorded count is not published** (review
/// M2). After a rollback of the controller image alone, and while no point
/// lands or leaves, the older controller has rewritten `kept` in its own
/// shape — 321 ids for `keepLast: 10`, the held-back points among them — and
/// could not remove `keptCount: 10` and `truncatedByCap: 311`, which still
/// add up to the unchanged 371. The sum closes; the list is not the count's.
///
/// The review's probe of this shape read `kept_rows=200 of which held-back
/// ids=190 truncated=true`: 190 points that are due, published as kept.
///
/// MUTANT F2a: drop the list comparison from `RetentionEvaluation::accounting`.
#[test]
fn a_kept_list_that_is_not_the_recorded_count_is_not_published() {
    let mut rolled = named("keep-10");
    let old_kept: Vec<String> = (1..=10)
        .chain(61..=371)
        .map(|d| format!("p{d:03}"))
        .collect();
    assert_eq!(old_kept.len(), 321);
    rolled["status"]["lastEvaluation"]["kept"] = json!(old_kept);
    {
        let ev = &rolled["status"]["lastEvaluation"];
        assert_eq!(
            (
                ev["pointsEvaluated"].as_i64(),
                ev["keptCount"].as_i64(),
                ev["candidateCount"].as_i64(),
                ev["truncatedByCap"].as_i64()
            ),
            (Some(371), Some(10), Some(50), Some(311)),
            "PREMISE: the four counts are untouched and still add up"
        );
    }
    let ev = project(rolled)["lastEvaluation"].clone();
    assert_eq!(ev["accounting"], "NotRecorded", "{ev}");
    assert!(
        ev.get("kept").is_none(),
        "no `kept` rows: 311 of the 321 ids are due, not kept: {ev}"
    );
    for absent in ["keptCount", "truncatedByCap", "maxDeletionsPerRun"] {
        assert!(ev.get(absent).is_none(), "{absent}: {ev}");
    }
    assert_eq!(
        ev["truncated"], false,
        "a list that is not published was not cut short"
    );
    assert_eq!(ev["pointsEvaluated"], 371);
    assert_eq!(ev["candidateCount"], 50, "the plan is still the plan");
    let listed: Vec<String> = ids(&ev["kept"])
        .into_iter()
        .chain(ids(&ev["candidates"]))
        .collect();
    for held in (61..=371).map(|d| format!("p{d:03}")) {
        assert!(!listed.contains(&held), "{held} is held back and is listed");
    }

    // CONTROL: the controller's own block, list and count together.
    let ev = project(named("keep-10"))["lastEvaluation"].clone();
    assert_eq!(ev["accounting"], "Recorded");
    assert_eq!(ids(&ev["kept"]).len(), 10);
    // CONTROL: `keep-300` publishes 200 of its 300 rows and is still an
    // accounting — the comparison is with the STATUS's list, not the rows.
    let ev = project(named("keep-300"))["lastEvaluation"].clone();
    assert_eq!(ev["accounting"], "Recorded");
    assert_eq!(ids(&ev["kept"]).len(), MAX_ROWS);
    assert_eq!(ev["keptCount"], 300);
}

/// **A count an older controller left behind is not a count.** After a
/// rollback of the controller image alone, the older controller's merge patch
/// rewrites `pointsEvaluated`, `candidateCount` and the lists and cannot
/// remove the two counts it does not know — they describe an earlier archive.
/// The four counts then do not add up, and the API publishes them as not
/// recorded.
///
/// MUTANT A6: read `keptCount` and `truncatedByCap` straight off the status
/// (not through `RetentionEvaluation::accounting`).
#[test]
fn counts_that_do_not_add_up_are_published_as_not_recorded() {
    // The archive grew by one point under the older controller.
    let mut stale = named("keep-10");
    stale["status"]["lastEvaluation"]["pointsEvaluated"] = json!(372);
    let ev = project(stale)["lastEvaluation"].clone();
    assert_eq!(ev["accounting"], "NotRecorded", "{ev}");
    assert!(ev.get("keptCount").is_none(), "{ev}");
    assert!(ev.get("truncatedByCap").is_none(), "{ev}");
    assert!(ev.get("maxDeletionsPerRun").is_none(), "{ev}");
    assert!(ev.get("kept").is_none(), "{ev}");
    assert_eq!(ev["pointsEvaluated"], 372);

    // CONTROL: the same edit made consistently IS an accounting.
    let mut moved = named("keep-10");
    moved["status"]["lastEvaluation"]["pointsEvaluated"] = json!(372);
    moved["status"]["lastEvaluation"]["truncatedByCap"] = json!(312);
    let ev = project(moved)["lastEvaluation"].clone();
    assert_eq!(ev["accounting"], "Recorded");
    assert_eq!(ev["keptCount"], 10);
    assert_eq!(ev["truncatedByCap"], 312);
}

/// A plan under the ceiling publishes `truncatedByCap: 0` — present, an
/// answer.
#[test]
fn zero_held_back_is_published_as_zero() {
    let mut under = named("keep-300");
    {
        let ev = &mut under["status"]["lastEvaluation"];
        // 371 points, 350 kept, 21 due and all of them in the plan.
        ev["keptCount"] = json!(350);
        ev["kept"] = json!((1..=350).map(|d| format!("p{d:03}")).collect::<Vec<_>>());
        ev["candidateCount"] = json!(21);
        ev["truncatedByCap"] = json!(0);
    }
    let ev = project(under)["lastEvaluation"].clone();
    assert_eq!(ev["accounting"], "Recorded", "{ev}");
    assert_eq!(ev["truncatedByCap"], json!(0), "present and zero: {ev}");
    assert_eq!(ev["keptCount"], 350);
}

/// **`viewIncomplete`, one rule** (review L3 and L5; the console applies the
/// same one): never hide a warning, and never assert a completeness that is
/// not recorded.
///
/// | the status says | accounting | the API publishes |
/// |---|---|---|
/// | `true` | recorded | `true` |
/// | `true` | not recorded | `true` |
/// | `false` | recorded | `false` |
/// | `false` | not recorded | absent |
/// | absent | either | absent |
///
/// MUTANTS: A8 (the worker's) — publish `false` when the status is silent;
/// F6a — withhold `true` without the accounting (the branch's first rule);
/// F6b — publish `false` without the accounting.
#[test]
fn the_view_flag_is_never_hidden_and_never_asserted_without_the_accounting() {
    // The pure rule, every cell of the table.
    assert_eq!(published_view_incomplete(Some(true), true), Some(true));
    assert_eq!(published_view_incomplete(Some(true), false), Some(true));
    assert_eq!(published_view_incomplete(Some(false), true), Some(false));
    assert_eq!(published_view_incomplete(Some(false), false), None);
    assert_eq!(published_view_incomplete(None, true), None);
    assert_eq!(published_view_incomplete(None, false), None);

    // And through the projection. `recorded` is the controller's own block;
    // `unrecorded` is the same block with one count gone.
    let with = |accounting: bool, flag: Option<bool>| -> Value {
        let mut object = named("keep-10");
        let ev = object["status"]["lastEvaluation"]
            .as_object_mut()
            .expect("lastEvaluation");
        if !accounting {
            ev.remove("truncatedByCap");
        }
        match flag {
            Some(value) => {
                ev.insert("viewIncomplete".to_string(), json!(value));
            }
            None => {
                ev.remove("viewIncomplete");
            }
        }
        project(object)["lastEvaluation"].clone()
    };

    let ev = with(true, Some(true));
    assert_eq!(ev["accounting"], "Recorded");
    assert_eq!(ev["viewIncomplete"], json!(true));

    let ev = with(false, Some(true));
    assert_eq!(ev["accounting"], "NotRecorded", "PREMISE: {ev}");
    assert_eq!(
        ev["viewIncomplete"],
        json!(true),
        "a warning is published whether or not the counts are: {ev}"
    );
    assert!(ev.get("keptCount").is_none() && ev.get("kept").is_none());

    let ev = with(true, Some(false));
    assert_eq!(
        ev["viewIncomplete"],
        json!(false),
        "the catalog said its view is the whole archive, beside the counts of that evaluation"
    );

    let ev = with(false, Some(false));
    assert_eq!(ev["accounting"], "NotRecorded", "PREMISE: {ev}");
    assert!(
        ev.get("viewIncomplete").is_none(),
        "completeness is not asserted beside an accounting that is not recorded: {ev}"
    );

    for accounting in [true, false] {
        let ev = with(accounting, None);
        assert!(
            ev.get("viewIncomplete").is_none(),
            "absent is never `false` (accounting recorded: {accounting}): {ev}"
        );
    }
}

/// **The class sweep, in this route: a run's `failed` list says when it was
/// cut.** `deleted` has carried `deletedTruncated` since the route existed;
/// `failed` was cut at the same 200 rows with nothing saying so. A run may
/// name up to 500 points (`maxDeletionsPerRun`), and a delete credential the
/// store refuses fails every one of them.
///
/// CONTROL: exactly 200 failures are whole, and the member is ABSENT — an
/// older client reads an absent member as it always did.
///
/// MUTANT A9: never publish the member.
#[test]
fn a_cut_failed_list_says_it_was_cut() {
    let live: Value = serde_json::from_str(
        &std::fs::read_to_string(
            repo_root().join("crates/logweir-api/tests/fixtures/retention-policy-enforce.json"),
        )
        .expect("the live fixture"),
    )
    .expect("JSON");
    let with_failures = |n: usize| {
        let mut object = live.clone();
        object["status"]["lastEnforcement"]["failed"] = json!((0..n)
            .map(|i| json!({"pointId": format!("p{i:03}"), "code": "AccessDenied"}))
            .collect::<Vec<_>>());
        project(object)["lastEnforcement"].clone()
    };

    let cut = with_failures(MAX_ROWS + 1);
    assert_eq!(cut["failed"].as_array().expect("failed").len(), MAX_ROWS);
    assert_eq!(cut["failedTruncated"], json!(true), "{cut}");

    let whole = with_failures(MAX_ROWS);
    assert_eq!(whole["failed"].as_array().expect("failed").len(), MAX_ROWS);
    assert!(
        whole.get("failedTruncated").is_none(),
        "absent when nothing was cut: {whole}"
    );
}
