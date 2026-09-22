//! `POST /api/v1/namespaces/{ns}/schedules` after PLAT-10.1.
//!
//! WHAT CHANGED AND WHY THERE IS A FILE FOR IT. The create route used to take
//! seven fields while the edit route took thirteen, so the console's guided form
//! could edit a policy it had no way to create: a saved destination, a dynamic
//! selection, a zone, a catch-up policy and retries all had to be bolted on
//! afterwards, by a second request, against an object that was already
//! admitting slots under a policy nobody had asked for. D2 W13's record listed
//! `CreateScheduleRequest.destinationRef` under *owed by the API before these
//! pages are complete*; `ui/pages/schedules.js` printed the debt on screen.
//!
//! THE FOUR PROPERTIES THESE ROWS EXIST FOR:
//!
//! 1. **The destination sentinel is BUILT here and never read from a body** —
//!    the same rule, and the same helper, the edit route has.
//! 2. **An empty selection is still not a run**, and a dynamic one is stored as
//!    the CRD's own two fields.
//! 3. **The cadence is parsed in the zone it will be stored with**, so an
//!    unknown zone is a field error and never `Ready=False` on a created
//!    object.
//! 4. **A pre-PLAT-10.1 body creates exactly what it created before**, key for
//!    key — the compatibility clause the DTO's documentation promises.

mod support;

use serde_json::{json, Value};
use support::{TestApp, NS_A};

fn create_path() -> String {
    format!("/api/v1/namespaces/{NS_A}/schedules")
}

/// The guided form's body: a saved destination, a dynamic selection, a zone
/// and the advanced fields, all in one create.
fn guided() -> Value {
    json!({
        "schedule": "30 2 * * *",
        "timeZone": "Europe/Berlin",
        "sourceRef": {"name": "source"},
        "allUserTopics": {
            "exclude": {"topics": ["scratch"], "prefixes": ["dev-"]},
            "incompleteDiscovery": "Refuse"
        },
        "destinationRef": {"name": "primary"},
        "concurrencyPolicy": "Allow",
        "startingDeadlineSeconds": 900,
        "catchUpPolicy": "Latest",
        "retry": {"maxRetries": 2, "delaySeconds": 600},
        "activeDeadlineSeconds": 7200,
        "retention": {"keepLast": 7},
        "suspended": false
    })
}

/// **One create writes the whole policy the edit route can write.**
///
/// KILLS: dropping any of the new fields on the floor (the pre-PLAT-10.1
/// `build` wrote `None` for every one of them and returned 201, so a form that
/// asked for Berlin, retries and a destination got UTC, no retries and an empty
/// archive URL with a 201 to say it had worked); writing the destination as an
/// inline archive; storing the preset instead of the expression.
#[tokio::test]
async fn one_create_writes_the_whole_policy() {
    let app = TestApp::new();
    let response = app
        .post(
            &create_path(),
            Some("guided-create-0001"),
            &guided().to_string(),
        )
        .await;
    assert_eq!(response.status, 201, "{}", response.text());
    let name = response.json()["item"]["name"]
        .as_str()
        .unwrap()
        .to_string();
    let stored = app.fake.object("backupschedules", NS_A, &name).unwrap();
    let spec = &stored["spec"];

    assert_eq!(spec["schedule"], "30 2 * * *");
    assert_eq!(spec["timeZone"], "Europe/Berlin");
    assert_eq!(spec["sourceRef"], json!({"name": "source"}));
    // THE SENTINEL, BUILT HERE. No `secretRef`: a destination carries its own
    // access block and an inline credential beside it would be a second,
    // contradictory answer to "which credential writes this archive".
    assert_eq!(
        spec["archive"],
        json!({"url": "logweir-destination://primary"})
    );
    assert_eq!(spec["destinationRef"], json!({"name": "primary"}));
    assert_eq!(spec["topics"], json!([]));
    assert_eq!(
        spec["allUserTopics"],
        json!({
            "exclude": {"topics": ["scratch"], "prefixes": ["dev-"]},
            "incompleteDiscovery": "Refuse"
        })
    );
    assert_eq!(spec["concurrencyPolicy"], "Allow");
    assert_eq!(spec["startingDeadlineSeconds"], 900);
    assert_eq!(spec["catchUpPolicy"], "Latest");
    assert_eq!(spec["retry"], json!({"maxRetries": 2, "delaySeconds": 600}));
    assert_eq!(spec["activeDeadlineSeconds"], 7200);
    assert_eq!(spec["retention"], json!({"keepLast": 7}));
    assert_eq!(spec["suspend"], false);

    // And the projection the console re-renders from says the same.
    let item = &response.json()["item"];
    assert_eq!(item["destinationRef"]["name"], "primary");
    assert_eq!(item["timeZone"], "Europe/Berlin");
    assert_eq!(item["allUserTopics"]["incompleteDiscovery"], "Refuse");
    app.fake.assert_strict();
}

/// **Exactly one of `archive` and `destinationRef`, and the reserved scheme
/// cannot be typed.**
///
/// KILLS: accepting both (two answers to where runs are written); accepting
/// neither (an empty `archive.url` written to the cluster); accepting
/// `logweir-destination://` in `archive.url`, which is the case the CRD's
/// sentinel rule exists to refuse — a client that could type that URL could
/// type it WITHOUT a `destinationRef`.
#[tokio::test]
async fn the_location_is_exactly_one_of_the_two_spellings() {
    let app = TestApp::new();
    let mut both = guided();
    both["archive"] = json!({"url": "s3://kafka-backups/logweir"});
    app.post(&create_path(), Some("location-0001"), &both.to_string())
        .await
        .assert_problem(422, "validation_failed");

    let mut neither = guided();
    neither.as_object_mut().unwrap().remove("destinationRef");
    app.post(&create_path(), Some("location-0002"), &neither.to_string())
        .await
        .assert_problem(422, "validation_failed");

    let mut smuggled = guided();
    smuggled.as_object_mut().unwrap().remove("destinationRef");
    smuggled["archive"] = json!({"url": "logweir-destination://primary"});
    app.post(&create_path(), Some("location-0003"), &smuggled.to_string())
        .await
        .assert_problem(422, "validation_failed");

    assert_eq!(
        app.fake.count("backupschedules", NS_A),
        0,
        "a refused create reached Kubernetes"
    );

    // THE CONTROL: the same body with one spelling is created. Without it the
    // three refusals above would also pass against a route that refused
    // everything.
    let ok = app
        .post(&create_path(), Some("location-0004"), &guided().to_string())
        .await;
    assert_eq!(ok.status, 201, "{}", ok.text());
    app.fake.assert_strict();
}

/// **A selection is named topics or `allUserTopics`, and never neither.**
///
/// KILLS: the old `topics` 1..256 rule surviving as "a dynamic create needs a
/// topic anyway"; an empty selection creating a schedule with nothing to do;
/// a glob reaching the cluster; an exclusion PATTERN being taken for a prefix.
#[tokio::test]
async fn an_empty_selection_is_not_a_run_and_a_dynamic_one_is_stored_as_the_crds_two_fields() {
    let app = TestApp::new();
    let mut empty = guided();
    empty.as_object_mut().unwrap().remove("allUserTopics");
    let response = app
        .post(&create_path(), Some("selection-0001"), &empty.to_string())
        .await;
    response.assert_problem(422, "validation_failed");
    let error = &response.json()["errors"][0];
    assert_eq!(
        error["field"], "topics",
        "the create route's selection fields are top level, so the console has an input to \
         highlight"
    );
    assert_eq!(error["code"], "selection_invalid");

    type Mutation = (&'static str, fn(&mut Value));
    let mutations: [Mutation; 3] = [
        ("a glob", |b| {
            b.as_object_mut().unwrap().remove("allUserTopics");
            b["topics"] = json!(["orders*"]);
        }),
        ("a duplicate", |b| {
            b.as_object_mut().unwrap().remove("allUserTopics");
            b["topics"] = json!(["orders", "orders"]);
        }),
        ("an exclusion pattern", |b| {
            b["allUserTopics"] =
                json!({"exclude": {"prefixes": ["dev*"]}, "incompleteDiscovery": "Refuse"});
        }),
    ];
    for (label, mutate) in mutations {
        let mut body = guided();
        mutate(&mut body);
        let refused = app
            .post(&create_path(), Some("selection-probe"), &body.to_string())
            .await;
        refused.assert_problem(422, "validation_failed");
        assert!(
            !refused.text().is_empty(),
            "{label} was refused with an empty body"
        );
    }
    assert_eq!(app.fake.count("backupschedules", NS_A), 0);

    // A NAMED allowlist still creates, and stores no dynamic block.
    let mut named = guided();
    named.as_object_mut().unwrap().remove("allUserTopics");
    named["topics"] = json!(["orders", "payments"]);
    let ok = app
        .post(&create_path(), Some("selection-0002"), &named.to_string())
        .await;
    assert_eq!(ok.status, 201, "{}", ok.text());
    let name = ok.json()["item"]["name"].as_str().unwrap().to_string();
    let stored = app.fake.object("backupschedules", NS_A, &name).unwrap();
    assert_eq!(stored["spec"]["topics"], json!(["orders", "payments"]));
    assert!(
        stored["spec"]["allUserTopics"].is_null(),
        "a named allowlist stored a dynamic block: {}",
        stored["spec"]
    );
    app.fake.assert_strict();
}

/// **The cadence is parsed in the zone it will be stored with.**
///
/// KILLS: parsing the expression against UTC and storing a zone nobody
/// checked — `Mars/Olympus` would then be a created object with
/// `Ready=False`/`UnknownTimeZone` and a 201 in front of the person who typed
/// it. KILLS TOO: a second range table here instead of the cadence engine's
/// constants.
#[tokio::test]
async fn an_invalid_cadence_or_range_is_refused_before_any_write() {
    let app = TestApp::new();
    type Mutation = (&'static str, &'static str, fn(&mut Value));
    let mutations: [Mutation; 3] = [
        ("schedule", "schedule_invalid", |b| {
            b["schedule"] = json!("61 * * * *");
        }),
        ("timeZone", "timezone_unknown", |b| {
            b["timeZone"] = json!("Mars/Olympus");
        }),
        ("sourceRef.name", "invalid_name", |b| {
            b["sourceRef"] = json!({"name": "NOT..A..NAME"});
        }),
    ];
    for (field, code, mutate) in mutations {
        let mut body = guided();
        mutate(&mut body);
        let response = app
            .post(&create_path(), Some("cadence-probe"), &body.to_string())
            .await;
        response.assert_problem(422, "validation_failed");
        let error = &response.json()["errors"][0];
        assert_eq!(error["field"], field);
        assert_eq!(error["code"], code);
    }
    for (field, value) in [
        ("startingDeadlineSeconds", json!(30)),
        ("startingDeadlineSeconds", json!(604_801)),
        ("activeDeadlineSeconds", json!(86_401)),
        ("retention", json!({"keepLast": 100_001})),
    ] {
        let mut body = guided();
        body[field] = value;
        app.post(&create_path(), Some("range-probe"), &body.to_string())
            .await
            .assert_problem(422, "validation_failed");
    }
    let mut retries = guided();
    retries["retry"] = json!({"maxRetries": 4});
    app.post(&create_path(), Some("retry-probe"), &retries.to_string())
        .await
        .assert_problem(422, "validation_failed");

    assert_eq!(
        app.fake.count("backupschedules", NS_A),
        0,
        "a refused create reached Kubernetes"
    );
    app.fake.assert_strict();
}

/// **A pre-PLAT-10.1 body creates exactly the object it created before.**
///
/// The DTO's compatibility clause, asserted rather than asserted-about: the
/// five-field body names an inline archive and a named allowlist, and every
/// field PLAT-10.1 added is ABSENT from the stored spec — not `null`, not a
/// default this route invented.
///
/// KILLS: writing `timeZone: "UTC"`, `catchUpPolicy: "None"` or
/// `retry: {maxRetries: 0}` as "the default", each of which would be a spec
/// change on every existing caller and a different run-policy digest.
#[tokio::test]
async fn a_pre_plat_10_1_body_creates_what_it_always_created() {
    let app = TestApp::new();
    let response = app
        .post(
            &create_path(),
            Some("legacy-create-0001"),
            &support::schedule_body().to_string(),
        )
        .await;
    assert_eq!(response.status, 201, "{}", response.text());
    let name = response.json()["item"]["name"]
        .as_str()
        .unwrap()
        .to_string();
    let stored = app.fake.object("backupschedules", NS_A, &name).unwrap();
    let spec = stored["spec"].as_object().unwrap();
    assert_eq!(spec["topics"], json!(["orders", "payments"]));
    assert_eq!(
        spec["archive"],
        json!({"url": "s3://kafka-backups/orders", "secretRef": {"name": "archive-credentials"}})
    );
    assert_eq!(spec["concurrencyPolicy"], "Forbid");
    assert_eq!(spec["suspend"], true);
    for added in [
        "timeZone",
        "allUserTopics",
        "destinationRef",
        "startingDeadlineSeconds",
        "catchUpPolicy",
        "retry",
        "activeDeadlineSeconds",
    ] {
        assert!(
            !spec.contains_key(added) || spec[added].is_null(),
            "PLAT-10.1 wrote {added} into a body that did not ask for it: {spec:#?}"
        );
    }
    app.fake.assert_strict();
}

/// **A pre-PLAT-10.1 body hashes to the bytes it hashed to before the upgrade.**
///
/// The idempotency request hash is taken over `serde_json::to_vec` of the
/// validated DTO (`routes::create_idempotent`). A script that creates its
/// schedule under a stable `Idempotency-Key` and re-runs after the upgrade must
/// get the replay, not `409 idempotency_conflict`, so the canonical bytes of an
/// old-shape body must not change.
///
/// THE EXPECTED STRINGS ARE THE PRE-UPGRADE DTO'S OWN OUTPUT, captured by
/// deserialising these two bodies into main `454cd6b`'s `CreateScheduleRequest`
/// (schedule, sourceRef, topics, archive, concurrencyPolicy, retention,
/// suspended) and serialising them. `concurrencyPolicy` and `retention`
/// predate PLAT-10.1 and always serialised, `null` included.
///
/// MUTANT: drop `skip_serializing_if` from any PLAT-10.1 member (for example
/// `time_zone`). The output gains `"timeZone":null` and this row fails.
#[test]
fn a_pre_plat_10_1_body_hashes_to_its_pre_upgrade_bytes() {
    let cases = [
        (
            r#"{"schedule":"0 2 * * *","sourceRef":{"name":"source"},"topics":["orders"],"archive":{"url":"s3://b/p","credentialRef":{"name":"s3-creds"}},"concurrencyPolicy":"Forbid","suspended":false}"#,
            r#"{"schedule":"0 2 * * *","sourceRef":{"name":"source"},"topics":["orders"],"archive":{"url":"s3://b/p","credentialRef":{"name":"s3-creds"}},"concurrencyPolicy":"Forbid","retention":null,"suspended":false}"#,
        ),
        (
            r#"{"schedule":"0 2 * * *","sourceRef":{"name":"source"},"topics":["orders"],"archive":{"url":"s3://b/p"},"retention":{"keepLast":7},"suspended":true}"#,
            r#"{"schedule":"0 2 * * *","sourceRef":{"name":"source"},"topics":["orders"],"archive":{"url":"s3://b/p","credentialRef":null},"concurrencyPolicy":null,"retention":{"keepLast":7,"keepDays":null},"suspended":true}"#,
        ),
    ];
    for (body, pre_upgrade) in cases {
        let parsed: logweir_api::contract::CreateScheduleRequest =
            serde_json::from_str(body).expect("an old-shape body still parses");
        let canonical = serde_json::to_string(&parsed).expect("serialises");
        assert_eq!(
            canonical, pre_upgrade,
            "an old-shape create no longer hashes to its pre-upgrade bytes, so its retry after \
             the upgrade is 409 idempotency_conflict instead of a replay"
        );
    }

    // THE COUNTER-CONTROL: a body that USES an added member serialises it, so
    // the skip is about absence and not a field silently left out of the hash.
    let guided: logweir_api::contract::CreateScheduleRequest =
        serde_json::from_value(guided()).expect("the guided body parses");
    let canonical = serde_json::to_string(&guided).expect("serialises");
    for member in [
        "\"timeZone\":\"Europe/Berlin\"",
        "\"allUserTopics\":",
        "\"destinationRef\":{\"name\":\"primary\"}",
        "\"startingDeadlineSeconds\":900",
        "\"catchUpPolicy\":\"Latest\"",
        "\"retry\":",
        "\"activeDeadlineSeconds\":7200",
    ] {
        assert!(
            canonical.contains(member),
            "{member} is missing from the hash input"
        );
    }
    assert!(
        !canonical.contains("\"archive\""),
        "an absent archive is not hashed"
    );
}

/// **The same old-shape body, sent twice under one key through the route, is
/// one schedule and a replay** -- the property the pinned bytes above protect,
/// exercised end to end.
#[tokio::test]
async fn an_old_shape_create_retried_under_its_key_replays() {
    let app = TestApp::new();
    let body = json!({
        "schedule": "0 2 * * *",
        "sourceRef": {"name": "source"},
        "topics": ["orders"],
        "archive": {"url": "s3://b/p", "credentialRef": {"name": "s3-creds"}},
        "suspended": false
    });
    let first = app
        .post(
            &create_path(),
            Some("old-shape-key-0001"),
            &body.to_string(),
        )
        .await;
    assert_eq!(first.status.as_u16(), 201, "{}", first.text());
    let second = app
        .post(
            &create_path(),
            Some("old-shape-key-0001"),
            &body.to_string(),
        )
        .await;
    assert_eq!(second.status.as_u16(), 200, "{}", second.text());
    assert_eq!(second.json()["replayed"], json!(true));
    assert_eq!(
        second.json()["item"]["name"],
        first.json()["item"]["name"],
        "the replay names the object the first create made"
    );
    assert_eq!(app.fake.count("backupschedules", NS_A), 1);
}

/// **Create and replace validate the future policy through ONE path** (review
/// LOW-3), so the same faults answer the same field errors on both routes.
///
/// MUTANT: widen `validate_policy_fields`' retry bound for one caller only (or
/// reintroduce a create-side copy that checks `activeDeadlineSeconds` against a
/// different range). The two error sets differ and this row fails.
#[tokio::test]
async fn create_and_replace_refuse_the_same_policy_faults_with_the_same_errors() {
    let app = TestApp::new();
    let faults = json!({
        "schedule": "61 * * * *",
        "timeZone": "Mars/Olympus",
        "startingDeadlineSeconds": 5,
        "activeDeadlineSeconds": 999999,
        "retry": {"maxRetries": 9, "delaySeconds": 1},
        "retention": {"keepLast": -1},
        "archive": {"url": "s3://b/p"},
        "destinationRef": {"name": "primary"}
    });
    let mut create = faults.clone();
    create["sourceRef"] = json!({"name": "source"});
    create["topics"] = json!(["orders"]);
    create["suspended"] = json!(false);
    let mut replace = faults.clone();
    replace["expectedGeneration"] = json!(1);
    replace["topicSelection"] = json!({"topics": ["orders"]});
    replace["suspended"] = json!(false);

    let created = app
        .post(
            &create_path(),
            Some("parity-create-0001"),
            &create.to_string(),
        )
        .await;
    created.assert_problem(422, "validation_failed");
    let replaced = app
        .put(&format!("{}/nightly", create_path()), &replace.to_string())
        .await;
    replaced.assert_problem(422, "validation_failed");

    let errors = |body: &Value| -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = body["errors"]
            .as_array()
            .expect("field errors")
            .iter()
            .map(|e| {
                (
                    e["field"].as_str().unwrap_or_default().to_string(),
                    e["code"].as_str().unwrap_or_default().to_string(),
                )
            })
            .collect();
        out.sort();
        out
    };
    let on_create = errors(&created.json());
    let on_replace = errors(&replaced.json());
    assert!(
        on_create.len() >= 6,
        "the faults were not all refused: {on_create:?}"
    );
    assert_eq!(
        on_create, on_replace,
        "the two routes validate the same policy differently"
    );
    assert_eq!(app.fake.count("backupschedules", NS_A), 0);
}
