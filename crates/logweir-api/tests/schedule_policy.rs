//! `PUT /api/v1/namespaces/{ns}/schedules/{name}` (PLAT-05.1, D1 §5).
//!
//! THE FOUR THINGS THIS ROUTE HAS TO GET RIGHT, and what each test is for.
//!
//! 1. **It changes the future and nothing else.** A `Backup` already created
//!    keeps its copied policy; the edit's whole effect is one PATCH on one
//!    `BackupSchedule`.
//! 2. **It cannot reach `spec.sourceRef`.** Refused before any read, with the
//!    CRD's own sentence, and unreachable through the adapter regardless.
//! 3. **It does not keep a second copy of the CRD's rules.** R2 and R3 are
//!    the API SERVER's to refuse (D1 §5.2); the route maps the refusal onto
//!    the field the person typed.
//! 4. **Two people editing at once do not silently overwrite each other.**
//!    `expectedGeneration` and the resourceVersion the read came back with are
//!    both preconditions, and both answer 412.

mod support;

use serde_json::{json, Value};
use support::{FakeKube, TestApp, NS_A};

/// A schedule as it stands BEFORE PLAT-04.2 and PLAT-05.1: no time zone, no
/// deadlines, no catch-up, no retries, no dynamic selection. Every stored
/// object in an installation upgraded into this release looks like this.
fn seed_legacy(fake: &FakeKube, name: &str) -> Value {
    fake.seed(
        "backupschedules",
        NS_A,
        json!({
            "metadata": {"name": name, "generation": 4},
            "spec": {
                "schedule": "0 3 * * *",
                "sourceRef": {"name": "source"},
                "topics": ["orders", "payments"],
                "archive": {"url": "s3://kafka-backups/logweir", "secretRef": {"name": "logweir-s3"}},
                "concurrencyPolicy": "Forbid",
                "suspend": false
            },
            "status": {"observedGeneration": 4}
        }),
    )
}

/// The whole editable policy, as a console that read the schedule would send
/// it back.
fn edit() -> Value {
    json!({
        "expectedGeneration": 4,
        "schedule": "30 2 * * *",
        "timeZone": "Europe/Berlin",
        "topicSelection": {"topics": ["orders", "payments", "shipments"]},
        "archive": {"url": "s3://kafka-backups/logweir", "credentialRef": {"name": "logweir-s3"}},
        "concurrencyPolicy": "Allow",
        "startingDeadlineSeconds": 900,
        "catchUpPolicy": "Latest",
        "retry": {"maxRetries": 2, "delaySeconds": 600},
        "activeDeadlineSeconds": 7200,
        "suspended": false
    })
}

fn patches(app: &TestApp) -> Vec<Value> {
    app.fake
        .requests()
        .iter()
        .filter(|r| r.method == "PATCH")
        .map(|r| serde_json::from_str(&r.body).expect("a patch body is JSON"))
        .collect()
}

/// **The edit writes every mutable field, names none of the immutable one, and
/// carries its precondition.**
///
/// KILLS: sending a diff instead of the whole policy (a field the form cleared
/// would keep its old value); dropping `metadata.resourceVersion` from the
/// patch (a lost-update race); letting `sourceRef` into the patch document;
/// patching anything other than the schedule.
#[tokio::test]
async fn the_edit_writes_the_whole_policy_under_a_precondition() {
    let app = TestApp::new();
    let before = seed_legacy(&app.fake, "nightly");
    let version = before["metadata"]["resourceVersion"].as_str().unwrap();

    let response = app
        .put(
            &format!("/api/v1/namespaces/{NS_A}/schedules/nightly"),
            &edit().to_string(),
        )
        .await;
    assert_eq!(response.status, 200, "{}", response.text());

    let sent = patches(&app);
    assert_eq!(sent.len(), 1, "one PATCH, on one object: {sent:?}");
    let patch = &sent[0];
    assert_eq!(patch["metadata"]["resourceVersion"], version);
    assert!(
        patch["metadata"].as_object().unwrap().len() == 1,
        "the patch's metadata is the precondition and nothing else: {patch}"
    );
    assert!(
        patch["spec"].get("sourceRef").is_none(),
        "the patch named the one immutable field: {patch}"
    );
    assert_eq!(patch["spec"]["schedule"], "30 2 * * *");
    assert_eq!(patch["spec"]["timeZone"], "Europe/Berlin");
    assert_eq!(patch["spec"]["catchUpPolicy"], "Latest");
    assert_eq!(
        patch["spec"]["retry"],
        json!({"maxRetries": 2, "delaySeconds": 600})
    );
    assert_eq!(patch["spec"]["activeDeadlineSeconds"], 7200);
    assert_eq!(patch["spec"]["concurrencyPolicy"], "Allow");

    let stored = app.fake.object("backupschedules", NS_A, "nightly").unwrap();
    assert_eq!(stored["spec"]["sourceRef"], json!({"name": "source"}));
    assert_eq!(
        stored["spec"]["topics"],
        json!(["orders", "payments", "shipments"])
    );
    // The generation moved, and the response says what it moved TO — so a
    // console can show "revision g5" without a second read.
    assert_eq!(stored["metadata"]["generation"], 5);
    assert_eq!(response.json()["item"]["generation"], 5);
    // `30 2 * * *` IS the guided `daily` shape, so a form can round-trip it.
    assert_eq!(
        response.json()["item"]["preset"],
        json!({"kind": "daily", "hour": 2, "minute": 30})
    );
    app.fake.assert_strict();
}

/// **A field omitted is REMOVED, not left behind.**
///
/// That is what makes "absent means the documented default" reachable from a
/// form. A schedule edited back to no retries really has no `spec.retry`; a
/// merge patch that only ever added keys would leave a retry policy nobody can
/// see and the scheduler still obeys.
///
/// KILLS: building the patch from `Some` values only.
#[tokio::test]
async fn a_field_omitted_is_removed() {
    let app = TestApp::new();
    seed_legacy(&app.fake, "nightly");
    app.put(
        &format!("/api/v1/namespaces/{NS_A}/schedules/nightly"),
        &edit().to_string(),
    )
    .await;
    let with_retry = app.fake.object("backupschedules", NS_A, "nightly").unwrap();
    assert!(with_retry["spec"].get("retry").is_some());
    let generation = with_retry["metadata"]["generation"].as_i64().unwrap();

    let mut back = edit();
    back["expectedGeneration"] = json!(generation);
    let object = back.as_object_mut().unwrap();
    for field in [
        "timeZone",
        "retry",
        "catchUpPolicy",
        "startingDeadlineSeconds",
        "activeDeadlineSeconds",
    ] {
        object.remove(field);
    }
    let response = app
        .put(
            &format!("/api/v1/namespaces/{NS_A}/schedules/nightly"),
            &back.to_string(),
        )
        .await;
    assert_eq!(response.status, 200, "{}", response.text());
    let stored = app.fake.object("backupschedules", NS_A, "nightly").unwrap();
    for field in [
        "timeZone",
        "retry",
        "catchUpPolicy",
        "startingDeadlineSeconds",
        "activeDeadlineSeconds",
    ] {
        assert!(
            stored["spec"].get(field).is_none(),
            "{field} survived an edit that omitted it: {stored}"
        );
    }
    assert_eq!(response.json()["item"].get("timeZone"), None);
    app.fake.assert_strict();
}

/// **An edit during a run leaves the run, its inputs and its Job alone.**
///
/// D1 §5.5's first row, on the API side: the edit's entire footprint is one
/// PATCH on the `BackupSchedule`. The running `Backup` is stored beside it and
/// must come back byte-identical, resourceVersion included.
///
/// KILLS: a route that "helpfully" updated in-flight runs to the new policy —
/// which is exactly the bug that would make a receipt describe a topic list
/// the run never read.
#[tokio::test]
async fn an_edit_during_a_run_does_not_touch_the_running_backup() {
    let app = TestApp::new();
    seed_legacy(&app.fake, "nightly");
    let running = app.fake.seed(
        "backups",
        NS_A,
        json!({
            "metadata": {"name": "logweir-backup-nightly-20260915-030000"},
            "spec": {
                "sourceRef": {"name": "source"},
                "topics": ["orders", "payments"],
                "archive": {"url": "s3://kafka-backups/logweir", "secretRef": {"name": "logweir-s3"}},
                "triggeredBy": "schedule",
                "slot": "20260915-030000",
                "trigger": {"kind": "Scheduled", "attempt": 0},
                "scheduleRef": {"name": "nightly", "uid": "sched-uid", "generation": 4, "runPolicySha256": "sha256:frozen"},
                "deadlineSeconds": 3600
            },
            "status": {"phase": "Running"}
        }),
    );
    app.put(
        &format!("/api/v1/namespaces/{NS_A}/schedules/nightly"),
        &edit().to_string(),
    )
    .await;
    let after = app
        .fake
        .object("backups", NS_A, "logweir-backup-nightly-20260915-030000")
        .unwrap();
    assert_eq!(
        after, running,
        "the edit changed a run that was already going"
    );
    assert!(
        !app.fake
            .requests()
            .iter()
            .any(|r| r.method != "GET" && r.path.contains("/backups/")),
        "the edit wrote to a Backup: {:#?}",
        app.fake.requests()
    );
    app.fake.assert_strict();
}

/// **A concurrent edit is 412, at either of the two preconditions.**
///
/// `expectedGeneration` catches the edit that landed BEFORE this request was
/// read; the resourceVersion the read came back with catches the one that
/// lands BETWEEN the read and the write. Both are the same answer to the
/// person — read it again — and both must refuse the write.
///
/// KILLS: comparing the generation and then writing without a precondition
/// (a lost update whose window is a Kubernetes round trip); treating the
/// API server's 409 as a state conflict a client should retry blindly.
#[tokio::test]
async fn a_concurrent_edit_is_412_at_either_precondition() {
    let app = TestApp::new();
    seed_legacy(&app.fake, "nightly");

    // Stale generation: refused before any write.
    let mut stale = edit();
    stale["expectedGeneration"] = json!(3);
    let response = app
        .put(
            &format!("/api/v1/namespaces/{NS_A}/schedules/nightly"),
            &stale.to_string(),
        )
        .await;
    response.assert_problem(412, "precondition_failed");
    assert!(
        patches(&app).is_empty(),
        "a stale generation still wrote: {:#?}",
        app.fake.requests()
    );

    // An edit landing between the read and the write: the API server refuses
    // the conditional patch, and the answer is the same.
    app.fake.clear_requests();
    app.fake.inject(support::Fault {
        method: "PATCH",
        path_contains: "/backupschedules/nightly".to_string(),
        status: 409,
        reason: "Conflict",
        delay: None,
        remaining: 1,
    });
    let raced = app
        .put(
            &format!("/api/v1/namespaces/{NS_A}/schedules/nightly"),
            &edit().to_string(),
        )
        .await;
    raced.assert_problem(412, "precondition_failed");
    // And the message never echoes the injected Kubernetes text, which in this
    // harness carries a token-shaped string on purpose.
    assert!(!raced.text().contains("eyJhbGciOiJIUzI1NiJ9"));
    app.fake.assert_strict();
}

/// **`sourceRef` is refused before anything is read, with the CRD's own
/// sentence.**
///
/// It is on the request DTO ONLY so that the refusal is a field error: a
/// console that writes back everything it read sends `sourceRef`, and
/// `malformed_request: unknown field` would not tell the person that the
/// answer is "create a new schedule".
///
/// KILLS: dropping `sourceRef` silently (the edit would appear to succeed and
/// the cluster would not change); answering 400; reading the object first.
#[tokio::test]
async fn source_ref_is_refused_before_any_read() {
    let app = TestApp::new();
    seed_legacy(&app.fake, "nightly");
    app.fake.clear_requests();
    let mut body = edit();
    body["sourceRef"] = json!({"name": "somewhere-else"});
    let response = app
        .put(
            &format!("/api/v1/namespaces/{NS_A}/schedules/nightly"),
            &body.to_string(),
        )
        .await;
    response.assert_problem(422, "validation_failed");
    let error = &response.json()["errors"][0];
    assert_eq!(error["field"], "sourceRef");
    assert_eq!(error["code"], "field_immutable");
    assert_eq!(
        error["message"],
        weirkeeper::crds::backup_schedule::SOURCE_REF_IMMUTABLE_MESSAGE
    );
    assert!(
        app.fake.requests().is_empty(),
        "a refused edit read the object: {:#?}",
        app.fake.requests()
    );
}

/// **The CRD's own rules are refused by the API SERVER and mapped back to the
/// field the person typed.**
///
/// D1 §5.2 is explicit that the API must not pre-empt R2 and R3: a second copy
/// of a CEL rule drifts from the schema, and a stored object can break a rule
/// this build has never heard of. So the write goes out, the API server
/// decides, and the refusal comes back naming the CRD's own message.
///
/// KILLS: pre-empting R2 in the route (the test's fake would then never be
/// asked, and the mapping would be dead code that rots); answering a generic
/// `validation_failed` with no field; echoing the API server's raw message
/// instead of the compiled-in constant.
#[tokio::test]
async fn the_rules_the_api_does_not_pre_empt_are_mapped_from_the_refusal() {
    let app = TestApp::new();
    seed_legacy(&app.fake, "nightly");

    // R2: a named allowlist AND a dynamic block.
    let mut both = edit();
    both["topicSelection"] = json!({
        "topics": ["orders"],
        "allUserTopics": {"incompleteDiscovery": "BackUpVisibleTopics"}
    });
    let response = app
        .put(
            &format!("/api/v1/namespaces/{NS_A}/schedules/nightly"),
            &both.to_string(),
        )
        .await;
    response.assert_problem(422, "validation_failed");
    let error = &response.json()["errors"][0];
    assert_eq!(error["field"], "topicSelection");
    assert_eq!(error["code"], "selection_invalid");
    assert_eq!(
        error["message"],
        weirkeeper::crds::backup_schedule::SELECTION_SHAPE_MESSAGE
    );
    // The API SERVER was asked: this is the assertion that keeps the route
    // from quietly growing its own copy of R2.
    assert_eq!(
        patches(&app).len(),
        1,
        "the rule was pre-empted, not mapped"
    );
    let stored = app.fake.object("backupschedules", NS_A, "nightly").unwrap();
    assert_eq!(stored["metadata"]["generation"], 4, "nothing was stored");

    // R3: retries on a name longer than 29 characters.
    let long = "nightly-orders-and-payments-eu";
    assert_eq!(long.len(), 30);
    seed_legacy(&app.fake, long);
    let response = app
        .put(
            &format!("/api/v1/namespaces/{NS_A}/schedules/{long}"),
            &edit().to_string(),
        )
        .await;
    response.assert_problem(422, "validation_failed");
    let error = &response.json()["errors"][0];
    assert_eq!(error["field"], "retry.maxRetries");
    assert_eq!(
        error["message"],
        weirkeeper::crds::backup_schedule::RETRY_NAME_BUDGET_MESSAGE
    );
    // 29 characters, same policy: accepted. Without this the test above would
    // pass for a route that refused every retry policy.
    let ok = "nightly-orders-and-payments-e";
    assert_eq!(ok.len(), 29);
    seed_legacy(&app.fake, ok);
    let accepted = app
        .put(
            &format!("/api/v1/namespaces/{NS_A}/schedules/{ok}"),
            &edit().to_string(),
        )
        .await;
    assert_eq!(accepted.status, 200, "{}", accepted.text());
    app.fake.assert_strict();
}

/// **A cadence the scheduler would refuse never reaches the cluster.**
///
/// An expression this API accepts and the controller refuses leaves
/// `Ready=False` on an object the console said was fine. The parser here is
/// the controller's own, and so is the zone table.
#[tokio::test]
async fn an_invalid_cadence_is_refused_before_any_write() {
    let app = TestApp::new();
    seed_legacy(&app.fake, "nightly");
    app.fake.clear_requests();
    /// One field, the code it must carry, and how to break it.
    type Mutation = (&'static str, &'static str, fn(&mut Value));
    let mutations: [Mutation; 3] = [
        ("schedule", "schedule_invalid", |b| {
            b["schedule"] = json!("61 * * * *");
        }),
        ("timeZone", "timezone_unknown", |b| {
            b["timeZone"] = json!("Mars/Olympus");
        }),
        ("topicSelection", "selection_invalid", |b| {
            b["topicSelection"] = json!({"topics": []});
        }),
    ];
    for (field, code, mutate) in mutations {
        let mut body = edit();
        mutate(&mut body);
        let response = app
            .put(
                &format!("/api/v1/namespaces/{NS_A}/schedules/nightly"),
                &body.to_string(),
            )
            .await;
        response.assert_problem(422, "validation_failed");
        let error = &response.json()["errors"][0];
        assert_eq!(error["field"], field);
        assert_eq!(error["code"], code);
    }
    // Ranges are the cadence engine's constants, not a second table here.
    for (field, value) in [
        ("startingDeadlineSeconds", json!(30)),
        ("startingDeadlineSeconds", json!(604_801)),
        ("activeDeadlineSeconds", json!(86_401)),
    ] {
        let mut body = edit();
        body[field] = value;
        app.put(
            &format!("/api/v1/namespaces/{NS_A}/schedules/nightly"),
            &body.to_string(),
        )
        .await
        .assert_problem(422, "validation_failed");
    }
    let mut retries = edit();
    retries["retry"] = json!({"maxRetries": 4});
    app.put(
        &format!("/api/v1/namespaces/{NS_A}/schedules/nightly"),
        &retries.to_string(),
    )
    .await
    .assert_problem(422, "validation_failed");
    // A glob is a refusal, not a match-all.
    let mut glob = edit();
    glob["topicSelection"] = json!({"topics": ["orders*"]});
    app.put(
        &format!("/api/v1/namespaces/{NS_A}/schedules/nightly"),
        &glob.to_string(),
    )
    .await
    .assert_problem(422, "validation_failed");
    assert!(
        app.fake.requests().is_empty(),
        "a refused edit reached Kubernetes: {:#?}",
        app.fake.requests()
    );
}

/// **A pre-PLAT-05.1 schedule needs no rewrite, and an edit adds only what was
/// sent.**
///
/// D1 §5.7: one stored version, no conversion webhook, no object rewrite. The
/// conversion test is therefore about the PROJECTION and the PATCH: a legacy
/// object reads back with every new field absent, and editing it writes the
/// new fields without inventing values for the ones the form did not offer.
#[tokio::test]
async fn a_pre_plat_05_1_schedule_reads_and_edits_without_a_rewrite() {
    let app = TestApp::new();
    seed_legacy(&app.fake, "legacy");
    let read = app
        .get(&format!("/api/v1/namespaces/{NS_A}/schedules/legacy"))
        .await
        .json();
    let item = &read["item"];
    assert_eq!(item["generation"], 4);
    assert_eq!(item["schedule"], "0 3 * * *");
    // ABSENT, not defaulted: a console that prints "3600" must do so from the
    // documented default, not from a value this API invented.
    for absent in [
        "timeZone",
        "allUserTopics",
        "startingDeadlineSeconds",
        "catchUpPolicy",
        "retry",
        "activeDeadlineSeconds",
        "destinationRef",
    ] {
        assert!(item.get(absent).is_none(), "{absent} was invented");
    }
    assert!(item["status"].get("policy").is_none());
    assert!(item["status"].get("nextRuns").is_none());
    // `0 3 * * *` IS a preset, so a form can offer the guided shape.
    assert_eq!(
        item["preset"],
        json!({"kind": "daily", "hour": 3, "minute": 0})
    );

    // A minimal edit: exactly the fields the pre-PLAT-04.2 form has.
    let minimal = json!({
        "expectedGeneration": 4,
        "schedule": "0 4 * * *",
        "topicSelection": {"topics": ["orders"]},
        "archive": {"url": "s3://kafka-backups/logweir", "credentialRef": {"name": "logweir-s3"}},
        "suspended": false
    });
    let response = app
        .put(
            &format!("/api/v1/namespaces/{NS_A}/schedules/legacy"),
            &minimal.to_string(),
        )
        .await;
    assert_eq!(response.status, 200, "{}", response.text());
    let stored = app.fake.object("backupschedules", NS_A, "legacy").unwrap();
    assert_eq!(stored["spec"]["schedule"], "0 4 * * *");
    assert_eq!(stored["spec"]["concurrencyPolicy"], "Forbid");
    for absent in ["timeZone", "retry", "catchUpPolicy", "allUserTopics"] {
        assert!(stored["spec"].get(absent).is_none(), "{absent} was written");
    }
    app.fake.assert_strict();
}

/// **A saved destination is written as the sentinel this crate builds, and the
/// sentinel is never accepted from a body.**
///
/// The CRD ties `destinationRef` to `archive.url ==
/// logweir-destination://<name>` with no `secretRef`. A client that could send
/// that URL itself could also send it WITHOUT a `destinationRef`, which is the
/// reserved-scheme case the rule exists to refuse.
#[tokio::test]
async fn a_destination_is_written_as_the_sentinel_and_never_read_from_a_body() {
    let app = TestApp::new();
    seed_legacy(&app.fake, "nightly");
    let mut body = edit();
    body.as_object_mut().unwrap().remove("archive");
    body["destinationRef"] = json!({"name": "primary"});
    let response = app
        .put(
            &format!("/api/v1/namespaces/{NS_A}/schedules/nightly"),
            &body.to_string(),
        )
        .await;
    assert_eq!(response.status, 200, "{}", response.text());
    let stored = app.fake.object("backupschedules", NS_A, "nightly").unwrap();
    assert_eq!(
        stored["spec"]["archive"],
        json!({"url": "logweir-destination://primary"})
    );
    assert_eq!(stored["spec"]["destinationRef"], json!({"name": "primary"}));
    assert_eq!(response.json()["item"]["destinationRef"]["name"], "primary");

    // The scheme is reserved: it cannot be typed into `archive.url`.
    let mut smuggled = edit();
    smuggled["archive"] = json!({"url": "logweir-destination://primary"});
    app.put(
        &format!("/api/v1/namespaces/{NS_A}/schedules/nightly"),
        &smuggled.to_string(),
    )
    .await
    .assert_problem(422, "validation_failed");

    // And exactly one of the two is required.
    let mut both = edit();
    both["destinationRef"] = json!({"name": "primary"});
    app.put(
        &format!("/api/v1/namespaces/{NS_A}/schedules/nightly"),
        &both.to_string(),
    )
    .await
    .assert_problem(422, "validation_failed");
    let mut neither = edit();
    neither.as_object_mut().unwrap().remove("archive");
    app.put(
        &format!("/api/v1/namespaces/{NS_A}/schedules/nightly"),
        &neither.to_string(),
    )
    .await
    .assert_problem(422, "validation_failed");
    app.fake.assert_strict();
}

/// **The edit refuses `Idempotency-Key`, and says what to use instead.**
///
/// A durable create replays; an edit cannot. The same body sent twice under
/// the same `expectedGeneration` is a 412 the second time, because the first
/// one moved the generation — and that IS the idempotence. Accepting the
/// header would promise a replay this route has no way to give.
#[tokio::test]
async fn the_edit_refuses_an_idempotency_key() {
    let app = TestApp::new();
    seed_legacy(&app.fake, "nightly");
    let response = app
        .send(
            http::Request::builder()
                .method("PUT")
                .uri(format!("/api/v1/namespaces/{NS_A}/schedules/nightly"))
                .header("host", support::HOST)
                .header("origin", support::ORIGIN)
                .header("content-type", "application/json")
                .header("idempotency-key", "an-edit-is-not-a-create")
                .body(axum::body::Body::from(edit().to_string()))
                .unwrap(),
        )
        .await;
    response.assert_problem(400, "idempotency_key_invalid");
    assert!(response.text().contains("expectedGeneration"));

    // The same body twice: the second is 412, because the first moved the
    // generation.
    let first = app
        .put(
            &format!("/api/v1/namespaces/{NS_A}/schedules/nightly"),
            &edit().to_string(),
        )
        .await;
    assert_eq!(first.status, 200);
    app.put(
        &format!("/api/v1/namespaces/{NS_A}/schedules/nightly"),
        &edit().to_string(),
    )
    .await
    .assert_problem(412, "precondition_failed");
}

/// **A schedule that is not there is `not_found`, and an unsafe method still
/// needs its Origin.**
#[tokio::test]
async fn the_ordinary_refusals_still_apply() {
    let app = TestApp::new();
    app.put(
        &format!("/api/v1/namespaces/{NS_A}/schedules/absent"),
        &edit().to_string(),
    )
    .await
    .assert_problem(404, "not_found");
    app.put(
        &format!("/api/v1/namespaces/{NS_A}/schedules/NOT..A..NAME"),
        &edit().to_string(),
    )
    .await
    .assert_problem(404, "not_found");
    app.put(
        "/api/v1/namespaces/not-granted/schedules/nightly",
        &edit().to_string(),
    )
    .await
    .assert_problem(403, "namespace_forbidden");
    let no_origin = app
        .send(
            http::Request::builder()
                .method("PUT")
                .uri(format!("/api/v1/namespaces/{NS_A}/schedules/nightly"))
                .header("host", support::HOST)
                .header("content-type", "application/json")
                .body(axum::body::Body::from(edit().to_string()))
                .unwrap(),
        )
        .await;
    no_origin.assert_problem(403, "origin_mismatch");
    // A malformed name, an ungranted namespace and a missing Origin are all
    // refused WITHOUT a Kubernetes call; only the `absent` probe, whose name is
    // legal, gets as far as a GET.
    let recorded = app.fake.requests();
    let paths: Vec<&str> = recorded.iter().map(|r| r.path.as_str()).collect();
    assert_eq!(
        paths,
        vec!["/apis/logweir.dev/v1alpha1/namespaces/team-a/backupschedules/absent"]
    );
    assert!(
        !recorded.iter().any(|r| r.method == "PATCH"),
        "a refused edit wrote"
    );
    app.fake.assert_strict();
}
