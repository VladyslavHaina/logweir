//! Preflights: the create contract, the binding recomputed on every read, the
//! aggregate that never rounds up, the detail page and the approver narrowing.

mod support;

use serde_json::{json, Value};
use support::{
    seed_preflight, seed_running_preflight, TestApp, ACTOR_ANNOTATION, LOCAL_ADMIN_ACTOR, NS_A,
};

fn plan() -> String {
    support::golden_plan()
}

fn plan_hash(plan: &str) -> String {
    logweir_core::ids::sha256_prefixed(plan.as_bytes())
}

fn restore_request(plan: &str) -> Value {
    json!({
        "operation": "restore",
        "restore": {
            "planBytes": plan,
            "planHash": plan_hash(plan),
            "target": "target",
            "sourceDestination": "primary",
            "evidenceDestination": "evidence",
            "recoveryPoint": {"backupName": "logweir-backup-nightly-20260915-030000", "backupUid": "uid-b"}
        }
    })
}

#[tokio::test]
async fn a_restore_preflight_forwards_its_plan_bytes_verbatim() {
    let app = TestApp::new();
    let plan = plan();
    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/preflights"),
            Some("preflight-key-00001"),
            &restore_request(&plan).to_string(),
        )
        .await;
    assert_eq!(response.status.as_u16(), 202, "{}", response.text());
    let v = response.json();
    assert_eq!(v["item"]["operation"], "restore");
    assert_eq!(v["item"]["state"], "pending");
    let id = v["item"]["id"].as_str().unwrap().to_string();
    assert!(id.starts_with("pf-"), "{id}");

    let stored = app.fake.object("preflights", NS_A, &id).unwrap();
    // BYTE FOR BYTE. Nothing here parses or re-emits a plan document.
    assert_eq!(
        stored["spec"]["request"]["restore"]["planBytes"]
            .as_str()
            .unwrap(),
        plan
    );
    assert_eq!(
        stored["spec"]["request"]["restore"]["planHash"],
        plan_hash(&plan)
    );
    assert_eq!(stored["spec"]["request"]["timeoutSeconds"], 120);
    assert_eq!(
        stored["metadata"]["annotations"][ACTOR_ANNOTATION],
        LOCAL_ADMIN_ACTOR
    );
    app.fake.assert_strict();
}

#[tokio::test]
async fn a_plan_hash_that_is_not_the_hash_of_the_bytes_is_refused_without_echoing_either() {
    let app = TestApp::new();
    let plan = plan();
    let mut request = restore_request(&plan);
    request["restore"]["planHash"] = json!(format!("sha256:{}", "b".repeat(64)));
    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/preflights"),
            Some("preflight-key-00002"),
            &request.to_string(),
        )
        .await;
    response.assert_problem(422, "validation_failed");
    let text = response.text();
    assert!(
        !text.contains(&"b".repeat(64)),
        "the refusal echoed the hash"
    );
    assert!(!text.contains("kafka"), "the refusal echoed the plan");
    assert_eq!(response.json()["errors"][0]["code"], "hash_mismatch");
    assert!(app.fake.requests().is_empty());
}

#[tokio::test]
async fn the_block_must_match_the_operation_and_the_exclusive_pairs_hold() {
    let app = TestApp::new();
    let path = format!("/api/v1/namespaces/{NS_A}/preflights");

    // P3: a block that is not the operation's.
    let response = app
        .post(
            &path,
            Some("preflight-bad-00001"),
            &json!({"operation": "backup", "restore": {"restoreName": "r"}}).to_string(),
        )
        .await;
    response.assert_problem(422, "validation_failed");
    let fields = field_names(&response.json());
    assert!(fields.contains(&"restore".to_string()), "{fields:?}");
    assert!(fields.contains(&"backup".to_string()), "{fields:?}");

    // P4: a backup names a destination or a legacy archive, never both.
    let response = app
        .post(
            &path,
            Some("preflight-bad-00002"),
            &json!({
                "operation": "backup",
                "backup": {"sourceConnection": "source", "destination": "primary", "legacyArchive": {"url": "s3://b/p"}, "topics": ["orders"]}
            })
            .to_string(),
        )
        .await;
    assert!(field_names(&response.json()).contains(&"backup.destination".to_string()));

    // P5: a draft or an existing restore, never both and never neither.
    let response = app
        .post(
            &path,
            Some("preflight-bad-00003"),
            &json!({"operation": "restore", "restore": {}}).to_string(),
        )
        .await;
    assert!(field_names(&response.json()).contains(&"restore.planBytes".to_string()));

    // P7: source and evidence destinations travel together.
    let plan = plan();
    let mut request = restore_request(&plan);
    request["restore"]["evidenceDestination"] = Value::Null;
    let response = app
        .post(&path, Some("preflight-bad-00004"), &request.to_string())
        .await;
    assert!(field_names(&response.json()).contains(&"restore.sourceDestination".to_string()));

    // A pattern is never a topic.
    let response = app
        .post(
            &path,
            Some("preflight-bad-00005"),
            &json!({
                "operation": "backup",
                "backup": {"sourceConnection": "source", "destination": "primary", "topics": ["orders*"]}
            })
            .to_string(),
        )
        .await;
    assert!(field_names(&response.json()).contains(&"backup.topics[0]".to_string()));

    // The budget is bounded.
    let response = app
        .post(
            &path,
            Some("preflight-bad-00006"),
            &json!({
                "operation": "backup",
                "backup": {"sourceConnection": "source", "destination": "primary", "topics": ["orders"]},
                "timeoutSeconds": 5
            })
            .to_string(),
        )
        .await;
    assert!(field_names(&response.json()).contains(&"timeoutSeconds".to_string()));

    assert_eq!(app.fake.count("preflights", NS_A), 0);
    assert!(app.fake.requests().is_empty());
}

#[tokio::test]
async fn a_duplicate_request_replays_and_a_changed_one_conflicts() {
    let app = TestApp::new();
    let plan = plan();
    let path = format!("/api/v1/namespaces/{NS_A}/preflights");
    let first = app
        .post(
            &path,
            Some("preflight-dup-00001"),
            &restore_request(&plan).to_string(),
        )
        .await;
    assert_eq!(first.status.as_u16(), 202);
    let replay = app
        .post(
            &path,
            Some("preflight-dup-00001"),
            &restore_request(&plan).to_string(),
        )
        .await;
    assert_eq!(replay.status.as_u16(), 200);
    assert_eq!(replay.json()["replayed"], true);
    assert_eq!(replay.json()["item"]["uid"], first.json()["item"]["uid"]);

    let mut changed = restore_request(&plan);
    changed["skipChecks"] = json!(["archive.segments"]);
    app.post(&path, Some("preflight-dup-00001"), &changed.to_string())
        .await
        .assert_problem(409, "idempotency_conflict");
    assert_eq!(app.fake.count("preflights", NS_A), 1);
    app.fake.assert_strict();
}

// ======================================================================
// The result, and what it is a result about
// ======================================================================

#[tokio::test]
async fn a_result_names_its_gating_and_never_rounds_the_aggregate_up() {
    let app = TestApp::new();
    seed_preflight(
        &app.fake,
        NS_A,
        "pf-0001",
        "Backup",
        None,
        Some(LOCAL_ADMIN_ACTOR),
    );
    let response = app
        .get(&format!("/api/v1/namespaces/{NS_A}/preflights/pf-0001"))
        .await;
    assert_eq!(response.status.as_u16(), 200, "{}", response.text());
    let v = response.json();
    let item = &v["item"];
    assert_eq!(item["state"], "notReady");
    assert_eq!(item["terminal"], true);

    // BLOCKING CHECKS, ADVISORY WARNINGS AND EXECUTION-ONLY NOTES ARE THREE
    // LISTS, because folding them into one is how "ready" comes to mean "every
    // permission is verified".
    let checks = item["checks"].as_array().unwrap();
    assert!(checks.iter().any(|c| c["id"] == "target.mappedTopics"));
    assert_eq!(item["warnings"][0]["id"], "archive.retention");
    assert_eq!(item["executionOnly"][0]["id"], "target.logAppendTime");
    assert!(item["executionOnly"][0]["note"]
        .as_str()
        .unwrap()
        .contains("run executes"));
    assert_eq!(item["detailsAvailable"], true);
    assert_eq!(item["checks"][0]["gating"], "blocking");
    assert_eq!(item["checks"][0]["remedy"], "Choose another prefix.");
    app.fake.assert_strict();
}

#[tokio::test]
async fn applicability_is_recomputed_against_the_callers_current_plan() {
    let app = TestApp::new();
    let bound = format!("sha256:{}", "a".repeat(64));
    seed_preflight(
        &app.fake,
        NS_A,
        "pf-0002",
        "Restore",
        Some(&bound),
        Some(LOCAL_ADMIN_ACTOR),
    );

    // Bound to the plan the caller is looking at: applicable.
    let same = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/preflights/pf-0002?planHash={bound}"
        ))
        .await;
    assert_eq!(same.json()["item"]["applicable"], true);
    assert_eq!(same.json()["item"]["stale"], false);
    assert_eq!(same.json()["item"]["binding"]["planHash"], bound);

    // ONE EDIT TO THE PLAN AND THE RESULT IS ABOUT SOMETHING ELSE.
    let edited = format!("sha256:{}", "c".repeat(64));
    let changed = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/preflights/pf-0002?planHash={edited}"
        ))
        .await;
    assert_eq!(changed.json()["item"]["applicable"], false);
    assert_eq!(changed.json()["item"]["stale"], true);
    assert_eq!(changed.json()["item"]["staleReasons"][0], "planHashChanged");

    // And an expiry is the other way a verdict stops describing now.
    app.clock.advance(3600);
    let expired = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/preflights/pf-0002?planHash={bound}"
        ))
        .await;
    assert_eq!(expired.json()["item"]["applicable"], false);
    assert_eq!(expired.json()["item"]["staleReasons"][0], "expired");

    // A malformed hash is refused rather than silently treated as absent.
    app.get(&format!(
        "/api/v1/namespaces/{NS_A}/preflights/pf-0002?planHash=deadbeef"
    ))
    .await
    .assert_problem(422, "validation_failed");
    app.fake.assert_strict();
}

#[tokio::test]
async fn the_detail_page_is_verified_filtered_and_paged() {
    let app = TestApp::new();
    seed_preflight(
        &app.fake,
        NS_A,
        "pf-0003",
        "Restore",
        None,
        Some(LOCAL_ADMIN_ACTOR),
    );
    let all = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/preflights/pf-0003/details"
        ))
        .await;
    assert_eq!(all.status.as_u16(), 200, "{}", all.text());
    assert_eq!(all.json()["items"].as_array().unwrap().len(), 2);
    assert_eq!(all.json()["items"][0]["check"], "target.mappedTopics");

    let filtered = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/preflights/pf-0003/details?check=archive.segments"
        ))
        .await;
    assert_eq!(filtered.json()["items"].as_array().unwrap().len(), 1);
    assert_eq!(filtered.json()["items"][0]["entry"]["key"], "missing-0");

    let first = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/preflights/pf-0003/details?limit=1"
        ))
        .await;
    assert_eq!(first.json()["items"].as_array().unwrap().len(), 1);
    let cursor = first.json()["page"]["nextCursor"]
        .as_str()
        .unwrap()
        .to_string();
    let next = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/preflights/pf-0003/details?limit=1&cursor={cursor}"
        ))
        .await;
    assert_eq!(next.json()["items"][0]["check"], "archive.segments");

    // A tampered cursor is refused before anything is read.
    let mut bytes = cursor.into_bytes();
    bytes[0] = if bytes[0] == b'A' { b'B' } else { b'A' };
    app.get(&format!(
        "/api/v1/namespaces/{NS_A}/preflights/pf-0003/details?limit=1&cursor={}",
        String::from_utf8(bytes).unwrap()
    ))
    .await
    .assert_problem(400, "cursor_invalid");
    app.fake.assert_strict();
}

/// THE INTEGRITY MUTANT for the detail document: bytes whose digest does not
/// match the one the status indexes are refused, not rendered.
#[tokio::test]
async fn a_details_document_that_fails_its_digest_refuses_the_page() {
    let app = TestApp::new();
    seed_preflight(
        &app.fake,
        NS_A,
        "pf-0004",
        "Restore",
        None,
        Some(LOCAL_ADMIN_ACTOR),
    );
    let mut document = app
        .fake
        .object("configmaps", NS_A, "lwc-pf-0004-details")
        .unwrap();
    document["data"]["details.jsonl"] = json!("{\"check\":\"planted\"}\n");
    app.fake.seed("configmaps", NS_A, document);
    let response = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/preflights/pf-0004/details"
        ))
        .await;
    response.assert_problem(409, "result_integrity_failed");
    assert!(!response.text().contains("planted"));
}

#[tokio::test]
async fn a_preflight_with_no_details_answers_an_empty_page_rather_than_404() {
    let app = TestApp::new();
    app.fake.seed(
        "preflights",
        NS_A,
        json!({
            "metadata": {"name": "pf-0005"},
            "spec": {"request": {"operation": "Backup", "backup": {"sourceRef": {"name": "s"}, "destinationRef": {"name": "d"}, "topics": ["o"]}, "timeoutSeconds": 120}, "cancelRequested": false},
            "status": {"phase": "Completed", "result": {"state": "ready"}}
        }),
    );
    let response = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/preflights/pf-0005/details"
        ))
        .await;
    assert_eq!(response.status.as_u16(), 200);
    assert_eq!(response.json()["items"], json!([]));
}

#[tokio::test]
async fn cancel_is_idempotent_and_exact_owner() {
    let app = TestApp::new();
    seed_running_preflight(&app.fake, NS_A, "pf-mine", LOCAL_ADMIN_ACTOR);
    seed_running_preflight(&app.fake, NS_A, "pf-theirs", "urn:test#another-operator");
    let path = format!("/api/v1/namespaces/{NS_A}/preflights/pf-mine:cancel");

    let first = app.post(&path, None, "{}").await;
    assert_eq!(first.status.as_u16(), 200, "{}", first.text());
    assert_eq!(first.json()["alreadyTerminal"], false);
    assert_eq!(
        app.fake.object("preflights", NS_A, "pf-mine").unwrap()["spec"]["cancelRequested"],
        true
    );

    app.fake.clear_requests();
    let again = app.post(&path, None, "{}").await;
    assert_eq!(again.status.as_u16(), 200);
    assert!(!app.fake.requests().iter().any(|r| r.method == "PATCH"));

    app.post(
        &format!("/api/v1/namespaces/{NS_A}/preflights/pf-theirs:cancel"),
        None,
        "{}",
    )
    .await
    .assert_problem(403, "forbidden");
    assert_eq!(
        app.fake.object("preflights", NS_A, "pf-theirs").unwrap()["spec"]["cancelRequested"],
        false
    );
    app.fake.assert_strict();
}

#[tokio::test]
async fn the_operation_route_reports_a_preflight_without_an_evidence_verdict() {
    let app = TestApp::new();
    seed_preflight(
        &app.fake,
        NS_A,
        "pf-0006",
        "Restore",
        None,
        Some(LOCAL_ADMIN_ACTOR),
    );
    let response = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/operations/preflight/pf-0006"
        ))
        .await;
    let v = response.json();
    assert_eq!(v["item"]["kind"], "preflight");
    // A CHECK THAT WORKED AND FOUND A PROBLEM `succeeded`. The VERDICT is the
    // preflight's own state; calling this `failed` would say the check broke.
    assert_eq!(v["item"]["state"], "succeeded");
    assert_eq!(v["item"]["terminal"], true);
    assert_eq!(v["item"]["cancellable"], false);
    for absent in ["verification", "result", "evidence", "verifiedSuccess"] {
        assert!(v["item"].get(absent).is_none(), "{absent} is published");
    }
    app.fake.assert_strict();
}

#[tokio::test]
async fn a_completed_check_with_no_recorded_aggregate_is_unknown_and_not_ready() {
    let app = TestApp::new();
    app.fake.seed(
        "preflights",
        NS_A,
        json!({
            "metadata": {"name": "pf-0007"},
            "spec": {"request": {"operation": "Backup", "backup": {"sourceRef": {"name": "s"}, "destinationRef": {"name": "d"}, "topics": ["o"]}, "timeoutSeconds": 120}, "cancelRequested": false},
            "status": {"phase": "Completed", "reason": "Completed"}
        }),
    );
    let response = app
        .get(&format!("/api/v1/namespaces/{NS_A}/preflights/pf-0007"))
        .await;
    assert_eq!(response.json()["item"]["state"], "unknown");
}

// ------------------------------------------------------------------ helpers

fn field_names(problem: &Value) -> Vec<String> {
    problem["errors"]
        .as_array()
        .map(|errors| {
            errors
                .iter()
                .map(|e| e["field"].as_str().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default()
}
