//! `GET /api/v1/cadence-previews` (PLAT-04.2, D1 §4.4).
//!
//! WHAT THESE TESTS ARE FOR. The cadence engine's own DST behaviour is W1's
//! (`crates/weirkeeper/tests/cadence.rs`); this file asserts that the ROUTE
//! hands the engine the right question and publishes its answer unaltered —
//! the preset really compiles, the zone really reaches the evaluator, the
//! markers really survive the DTO, the bounds are the engine's bounds, and no
//! Kubernetes call happens at all.
//!
//! THE FIXTURES ARE D1 §4.3's OWN WORKED EXAMPLES, so a preview that
//! disagrees with the decision record fails here rather than in a console.

mod support;

use support::{FakeKube, Options, TestApp, NS_A};

/// D1 §4.3: `Europe/Berlin`, `30 2 * * *`, the repeated hour of 2026-10-25.
const BERLIN_FALL_BACK: &str =
    "/api/v1/cadence-previews?schedule=30%202%20*%20*%20*&timeZone=Europe/Berlin\
     &after=2026-10-24T12:00:00Z&count=3";

/// D1 §4.3: the same expression across the 2027-03-28 gap.
const BERLIN_SPRING_FORWARD: &str =
    "/api/v1/cadence-previews?schedule=30%202%20*%20*%20*&timeZone=Europe/Berlin\
     &after=2027-03-27T12:00:00Z&count=2";

fn instants(body: &serde_json::Value) -> Vec<String> {
    body["runs"]
        .as_array()
        .expect("runs is a list")
        .iter()
        .map(|r| r["at"].as_str().unwrap_or_default().to_string())
        .collect()
}

/// **A fixed local time inside a repeated hour previews BOTH instants, and
/// says which is which.**
///
/// D1 §4.3 accepts one extra run a year for a fixed-time schedule in a
/// fall-back hour, on the explicit condition that the previews show both. If
/// the marker were dropped the two rows would read as one clock face twice and
/// look like a bug in the console.
///
/// KILLS: evaluating in UTC and ignoring `timeZone` (one run at 02:30Z);
/// de-duplicating by local wall time (one run); dropping `adjustment` from the
/// DTO; rendering `localTime` without its offset, which is the only thing that
/// distinguishes `+02:00` from `+01:00`.
#[tokio::test]
async fn a_repeated_local_hour_previews_both_instants_with_their_offsets() {
    let app = TestApp::new();
    let response = app.get(BERLIN_FALL_BACK).await;
    assert_eq!(response.status, 200);
    let body = response.json();
    assert_eq!(
        instants(&body),
        vec![
            "2026-10-25T00:30:00Z",
            "2026-10-25T01:30:00Z",
            "2026-10-26T01:30:00Z"
        ]
    );
    assert_eq!(body["runs"][0]["localTime"], "2026-10-25T02:30:00+02:00");
    assert_eq!(body["runs"][0]["adjustment"], "RepeatedLocalTimeFirst");
    assert_eq!(body["runs"][1]["localTime"], "2026-10-25T02:30:00+01:00");
    assert_eq!(body["runs"][1]["adjustment"], "RepeatedLocalTimeSecond");
    // The ordinary day after the transition carries no marker at all: a marker
    // on every row would train a reader to ignore them.
    assert!(body["runs"][2].get("adjustment").is_none());
    assert_eq!(body["timeZone"], "Europe/Berlin");
    assert_eq!(body["tzdb"], weirkeeper::cadence::TZDB_SOURCE);
    assert_eq!(body["after"], "2026-10-24T12:00:00Z");
    app.fake.assert_strict();
}

/// **A local time that does not exist fires once, at the end of the gap.**
///
/// KILLS: skipping the day (a nightly backup silently missing a night once a
/// year); firing at the same wall time on the wrong offset; reporting the
/// shifted instant without the marker that explains why 02:30 became 03:00
/// local.
#[tokio::test]
async fn a_nonexistent_local_time_previews_the_end_of_the_gap() {
    let app = TestApp::new();
    let body = app.get(BERLIN_SPRING_FORWARD).await.json();
    assert_eq!(
        instants(&body),
        vec!["2027-03-28T01:00:00Z", "2027-03-29T00:30:00Z"]
    );
    assert_eq!(body["runs"][0]["adjustment"], "NonexistentLocalTimeShifted");
    assert_eq!(body["runs"][0]["localTime"], "2027-03-28T03:00:00+02:00");
    assert!(body["runs"][1].get("adjustment").is_none());
}

/// **The absent zone is UTC, and says so.**
///
/// A reader must not have to know the default: `timeZone` in the answer is the
/// EFFECTIVE zone, which is what `status.policy.timeZone` records too.
#[tokio::test]
async fn an_absent_zone_is_utc_and_the_answer_says_so() {
    let app = TestApp::new();
    let body = app
        .get("/api/v1/cadence-previews?schedule=30%202%20*%20*%20*&after=2026-10-24T12:00:00Z&count=1")
        .await
        .json();
    assert_eq!(body["timeZone"], "UTC");
    assert_eq!(instants(&body), vec!["2026-10-25T02:30:00Z"]);
    assert_eq!(body["runs"][0]["localTime"], "2026-10-25T02:30:00+00:00");
}

/// **A preset compiles to the canonical expression, and the answer carries
/// it.**
///
/// D1 §4.2 stores no preset field: `spec.schedule` is the single source of
/// truth. So the form's job is to save the compiled string, and the preview
/// has to hand it back — otherwise the console needs its own compiler, which
/// is the second implementation this route exists to avoid.
///
/// KILLS: compiling `daily{2,0}` to anything but `0 2 * * *`; returning the
/// preset without the expression; losing the round trip (an expression that
/// does not match back to the preset it came from).
#[tokio::test]
async fn a_preset_compiles_to_the_canonical_expression_and_round_trips() {
    let app = TestApp::new();
    let body = app
        .get("/api/v1/cadence-previews?preset=daily&hour=2&minute=0&timeZone=Asia/Kathmandu&count=1&after=2026-09-15T00:00:00Z")
        .await
        .json();
    assert_eq!(body["schedule"], "0 2 * * *");
    assert_eq!(
        body["preset"],
        serde_json::json!({"kind": "daily", "hour": 2, "minute": 0})
    );
    // D1 §4.3's fixed +05:45 example, one field over: 02:00 local is 20:15 UTC
    // the previous day.
    assert_eq!(instants(&body), vec!["2026-09-15T20:15:00Z"]);
    assert_eq!(body["runs"][0]["localTime"], "2026-09-16T02:00:00+05:45");

    // And the other direction: an expression that IS a preset says so.
    let round_trip = app
        .get("/api/v1/cadence-previews?schedule=0%20*/6%20*%20*%20*&count=1")
        .await
        .json();
    assert_eq!(
        round_trip["preset"],
        serde_json::json!({"kind": "everyNHours", "n": 6, "minute": 0})
    );
    // An expression that is not a preset is "Advanced cron", not a guess.
    let advanced = app
        .get("/api/v1/cadence-previews?schedule=0,30%202%20*%20*%20*&count=1")
        .await
        .json();
    assert!(advanced.get("preset").is_none());
    assert_eq!(advanced["schedule"], "0,30 2 * * *");
}

/// **The catalogue is the only copy of the preset ranges.**
///
/// KILLS: a second range table in the route (accepting `n=5`, which fires at
/// 00:00, 05:00, 10:00, 15:00, 20:00 and then again at 00:00 — a four-hour gap
/// once a day that nobody chooses); silently ignoring a parameter that belongs
/// to a different preset, which is a form that has lost track of its shape.
#[tokio::test]
async fn preset_parameters_are_checked_against_the_catalogue() {
    let app = TestApp::new();
    let bad_n = app
        .get("/api/v1/cadence-previews?preset=everyNHours&n=5&minute=0")
        .await;
    bad_n.assert_problem(422, "validation_failed");
    assert_eq!(bad_n.json()["errors"][0]["field"], "n");

    let wrong_parameter = app
        .get("/api/v1/cadence-previews?preset=daily&hour=2&minute=0&dayOfWeek=3")
        .await;
    wrong_parameter.assert_problem(422, "validation_failed");
    assert_eq!(wrong_parameter.json()["errors"][0]["field"], "dayOfWeek");

    let missing = app
        .get("/api/v1/cadence-previews?preset=weekly&hour=2")
        .await;
    missing.assert_problem(422, "validation_failed");
    let fields: Vec<String> = missing.json()["errors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["field"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(fields, vec!["dayOfWeek", "minute"]);

    app.get("/api/v1/cadence-previews?preset=fortnightly&minute=0")
        .await
        .assert_problem(422, "validation_failed");
    // Day 29 is not a monthly cadence: four months a year would not have it.
    app.get("/api/v1/cadence-previews?preset=monthly&dayOfMonth=29&hour=0&minute=0")
        .await
        .assert_problem(422, "validation_failed");
}

/// **An invalid expression and an unknown zone name their OWN field.**
///
/// D1 §0.2 fixes these two field codes because the console highlights an
/// input with them. A single `schedule_invalid` for both would point the
/// person at the cron box when they mistyped a zone.
#[tokio::test]
async fn an_invalid_cron_and_an_unknown_zone_name_their_own_field() {
    let app = TestApp::new();
    let cron = app
        .get("/api/v1/cadence-previews?schedule=61%20*%20*%20*%20*")
        .await;
    cron.assert_problem(422, "validation_failed");
    assert_eq!(cron.json()["errors"][0]["field"], "schedule");
    assert_eq!(cron.json()["errors"][0]["code"], "schedule_invalid");

    let zone = app
        .get("/api/v1/cadence-previews?schedule=0%202%20*%20*%20*&timeZone=Mars/Olympus")
        .await;
    zone.assert_problem(422, "validation_failed");
    assert_eq!(zone.json()["errors"][0]["field"], "timeZone");
    assert_eq!(zone.json()["errors"][0]["code"], "timezone_unknown");

    // An empty zone is a field the user SET, and reading a set field as
    // "absent" is how a schedule ends up running in a zone nobody chose.
    app.get("/api/v1/cadence-previews?schedule=0%202%20*%20*%20*&timeZone=")
        .await
        .assert_problem(422, "validation_failed");
}

/// **The bounds, the source and the query surface are the ones D1 §4.4 fixes.**
#[tokio::test]
async fn the_bounds_and_the_query_surface_are_exact() {
    let app = TestApp::new();
    for bad in ["count=0", "count=21", "count=x", "count=-1"] {
        app.get(&format!(
            "/api/v1/cadence-previews?schedule=0%20*%20*%20*%20*&{bad}"
        ))
        .await
        .assert_problem(422, "validation_failed");
    }
    let capped = app
        .get("/api/v1/cadence-previews?schedule=0%20*%20*%20*%20*&count=20")
        .await
        .json();
    assert_eq!(capped["runs"].as_array().unwrap().len(), 20);
    // The documented default is ten, and it is the ENGINE's constant.
    let default = app
        .get("/api/v1/cadence-previews?schedule=0%20*%20*%20*%20*")
        .await
        .json();
    assert_eq!(
        default["runs"].as_array().unwrap().len(),
        weirkeeper::cadence::DEFAULT_PREVIEW_COUNT
    );

    // Exactly one source of the expression.
    app.get("/api/v1/cadence-previews")
        .await
        .assert_problem(422, "validation_failed");
    app.get("/api/v1/cadence-previews?schedule=0%20*%20*%20*%20*&preset=hourly&minute=0")
        .await
        .assert_problem(422, "validation_failed");
    // An unknown parameter is refused rather than ignored.
    app.get("/api/v1/cadence-previews?schedule=0%20*%20*%20*%20*&timezone=UTC")
        .await
        .assert_problem(400, "malformed_request");
    app.get("/api/v1/cadence-previews?schedule=0%20*%20*%20*%20*&after=yesterday")
        .await
        .assert_problem(422, "validation_failed");
}

/// **An expression that runs out of firings returns a SHORT list, not an
/// error.**
///
/// `0 0 29 2 *` fires on 29 February. Asking for twenty of those is asking for
/// eighty years, and the engine walks a bounded window — so the honest answer
/// is the firings it found, not twenty rows and not an error. A console that
/// renders four rows is telling the truth; a padded list would not be.
///
/// KILLS: clamping the walk and reporting it as a full page; turning a short
/// walk into a 500.
#[tokio::test]
async fn an_expression_that_runs_out_of_firings_answers_short() {
    let app = TestApp::new();
    let body = app
        .get("/api/v1/cadence-previews?schedule=0%200%2029%202%20*&count=20&after=2026-03-01T00:00:00Z")
        .await
        .json();
    let runs = body["runs"].as_array().unwrap();
    assert!(
        runs.len() < 20,
        "twenty leap days are eighty years, past the engine's walk: {runs:?}"
    );
    for run in runs {
        assert!(
            run["at"]
                .as_str()
                .unwrap()
                .starts_with(&format!("{}-02-29", &run["at"].as_str().unwrap()[..4])),
            "every firing is a 29 February: {run}"
        );
    }
}

/// **The preview reads NOTHING from Kubernetes.**
///
/// A draft cadence is not an object. This is the assertion that keeps it that
/// way: a route that started reading a schedule to "fill in the zone" would
/// need a namespace, a grant and a cursor, and would answer differently for
/// two people looking at the same form.
#[tokio::test]
async fn the_preview_reads_nothing_from_kubernetes() {
    let app = TestApp::new();
    app.get(BERLIN_FALL_BACK).await;
    app.get("/api/v1/cadence-previews?preset=hourly&minute=7")
        .await;
    app.get("/api/v1/cadence-previews?schedule=nonsense").await;
    assert!(
        app.fake.requests().is_empty(),
        "a preview reached Kubernetes: {:#?}",
        app.fake.requests()
    );
    app.fake.assert_strict();
}

/// **An actor with no schedule-read grant anywhere gets no evaluator.**
///
/// The answer is cheap, but it is a product feature and not a public
/// calculator: an unauthenticated or unbound caller is refused before the
/// engine runs.
#[tokio::test]
async fn an_actor_with_no_grant_is_refused() {
    /// Authenticated, bound to nothing.
    struct NoGrants;
    impl logweir_api::authz::Authorizer for NoGrants {
        fn namespaces(&self, _actor: &logweir_api::auth::Actor) -> Vec<String> {
            Vec::new()
        }
        fn allows(
            &self,
            _actor: &logweir_api::auth::Actor,
            _namespace: &str,
            _action: logweir_api::authz::Action,
        ) -> bool {
            false
        }
    }
    let app = TestApp::with(
        FakeKube::new(),
        Options {
            authorizer: Some(std::sync::Arc::new(NoGrants)),
            ..Options::default()
        },
    );
    app.get("/api/v1/cadence-previews?schedule=0%20*%20*%20*%20*")
        .await
        .assert_problem(403, "forbidden");
    assert!(app.fake.requests().is_empty());
}

/// **The contract's preset enum is byte-identical to the engine's.**
///
/// Two declarations of the same five shapes, in two crates, because the
/// contract needs a `JsonSchema` derive the engine does not carry. This is the
/// drift test that makes the duplication safe.
#[test]
fn the_contract_preset_is_the_engines_preset() {
    use weirkeeper::cadence::presets::Preset;
    for preset in [
        Preset::Hourly { minute: 7 },
        Preset::EveryNHours { n: 6, minute: 0 },
        Preset::Daily {
            hour: 2,
            minute: 30,
        },
        Preset::Weekly {
            day_of_week: 0,
            hour: 1,
            minute: 5,
        },
        Preset::Monthly {
            day_of_month: 28,
            hour: 23,
            minute: 59,
        },
    ] {
        assert_eq!(
            serde_json::to_value(preset).unwrap(),
            serde_json::to_value(logweir_api::projection::preset_view(preset)).unwrap(),
            "{preset:?}"
        );
    }
}

/// **A saved schedule's own previews are published in the SAME shape.**
///
/// D1 §4.4 has two producers — this route for a draft, the controller's
/// `status.nextRuns` for a saved schedule — and a console renders them with
/// one branch. A different spelling in either would be two branches and two
/// chances to be wrong.
///
/// KILLS: dropping `status.nextRuns` from the projection; renaming
/// `localTime`; camel-casing the adjustment on one side only; publishing
/// `evaluatedAt` as a liveness signal (D1 §4.9 as amended) — the staleness
/// signal a console must use is `nextRuns[0].at`, and both are here.
#[tokio::test]
async fn a_saved_schedules_previews_are_the_same_shape() {
    let app = TestApp::new();
    app.fake.seed(
        "backupschedules",
        NS_A,
        serde_json::json!({
            "metadata": {"name": "nightly", "generation": 7},
            "spec": {
                "schedule": "30 2 * * *",
                "timeZone": "Europe/Berlin",
                "sourceRef": {"name": "source"},
                "topics": ["orders"],
                "archive": {"url": "s3://b/p"},
                "suspend": false
            },
            "status": {
                "observedGeneration": 7,
                "policy": {
                    "generation": 7,
                    "runPolicySha256": "sha256:abc",
                    "timeZone": "Europe/Berlin",
                    "tzdb": weirkeeper::cadence::TZDB_SOURCE,
                    "effectiveSince": "2026-09-15T10:00:00Z",
                    "evaluatedAt": "2026-09-15T10:00:00Z"
                },
                "nextRuns": [
                    {"at": "2026-10-25T00:30:00Z", "localTime": "2026-10-25T02:30:00+02:00", "adjustment": "RepeatedLocalTimeFirst"},
                    {"at": "2026-10-25T01:30:00Z", "localTime": "2026-10-25T02:30:00+01:00", "adjustment": "RepeatedLocalTimeSecond"},
                    {"at": "2026-10-26T01:30:00Z", "localTime": "2026-10-26T02:30:00+01:00"}
                ],
                "activeRuns": [{"name": "logweir-backup-nightly-20261025-003000", "kind": "Scheduled", "attempt": 0}]
            }
        }),
    );
    let saved = app
        .get(&format!("/api/v1/namespaces/{NS_A}/schedules/nightly"))
        .await
        .json();
    let drafted = app.get(BERLIN_FALL_BACK).await.json();
    assert_eq!(saved["item"]["status"]["nextRuns"], drafted["runs"]);
    assert_eq!(saved["item"]["generation"], 7);
    assert_eq!(saved["item"]["timeZone"], "Europe/Berlin");
    assert_eq!(saved["item"]["status"]["observedGeneration"], 7);
    assert_eq!(
        saved["item"]["status"]["policy"]["evaluatedAt"],
        "2026-09-15T10:00:00Z"
    );
    assert_eq!(
        saved["item"]["status"]["activeRuns"][0]["kind"],
        "Scheduled"
    );
    // An adjustment word this build does not know is DROPPED rather than
    // echoed: an unrecognised marker is not a fact about a time zone.
    app.fake.seed(
        "backupschedules",
        NS_A,
        serde_json::json!({
            "metadata": {"name": "odd"},
            "spec": {"schedule": "0 2 * * *", "sourceRef": {"name": "s"}, "topics": ["t"], "archive": {"url": "s3://b/p"}},
            "status": {"nextRuns": [{"at": "2026-10-25T00:30:00Z", "localTime": "x", "adjustment": "SomethingNewer"}]}
        }),
    );
    let odd = app
        .get(&format!("/api/v1/namespaces/{NS_A}/schedules/odd"))
        .await
        .json();
    assert!(odd["item"]["status"]["nextRuns"][0]
        .get("adjustment")
        .is_none());
}
