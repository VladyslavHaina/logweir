//! Topic discoveries: the create contract, the paged and searched result, the
//! integrity rule on every stored chunk, and cancellation.

mod support;

use serde_json::{json, Value};
use support::{
    seed_discovery, seed_running_discovery, topic_line, TestApp, ACTOR_ANNOTATION,
    LOCAL_ADMIN_ACTOR, NS_A,
};

fn seed_connection(app: &TestApp) {
    seed_connection_in(app, NS_A);
}

fn seed_connection_in(app: &TestApp, namespace: &str) {
    app.fake.seed(
        "kafkaclusters",
        namespace,
        json!({
            "metadata": {"name": "source", "uid": "seed-connection-uid", "generation": 1},
            "spec": {
                "bootstrapServers": ["kafka:9092"],
                "auth": {"mode": "scramSha512", "username": "scram-user", "secretRef": {"name": "s"}, "tls": false},
                "role": "source"
            }
        }),
    );
}

fn lines(prefix: &str, from: usize, to: usize) -> Vec<String> {
    (from..to)
        .map(|i| topic_line(&format!("{prefix}-{i:05}"), 3, "-"))
        .collect()
}

#[tokio::test]
async fn a_discovery_is_created_against_a_connection_that_exists() {
    let app = TestApp::new();
    seed_connection(&app);
    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/connections/source/topic-discoveries"),
            Some("discovery-key-00001"),
            r#"{"expectedTopics": ["orders", "payments"]}"#,
        )
        .await;
    assert_eq!(response.status.as_u16(), 202, "{}", response.text());
    let v = response.json();
    assert_eq!(v["item"]["state"], "pending");
    assert_eq!(v["item"]["connection"]["name"], "source");
    assert_eq!(v["reused"], false);
    assert_eq!(v["item"]["stale"], false);
    let id = v["item"]["id"].as_str().unwrap().to_string();
    assert!(id.starts_with("td-"), "{id}");

    let stored = app.fake.object("topicdiscoveries", NS_A, &id).unwrap();
    assert_eq!(stored["spec"]["request"]["maxTopics"], 20000);
    assert_eq!(stored["spec"]["request"]["timeoutSeconds"], 60);
    assert_eq!(stored["spec"]["request"]["includeInternal"], false);
    // The expected names are sorted, so two spellings of one set hash and
    // store identically.
    assert_eq!(
        stored["spec"]["request"]["expectedTopics"],
        json!(["orders", "payments"])
    );
    assert_eq!(
        stored["metadata"]["labels"]["logweir.dev/connection"],
        "source"
    );
    assert_eq!(
        stored["metadata"]["annotations"][ACTOR_ANNOTATION],
        LOCAL_ADMIN_ACTOR
    );
    app.fake.assert_strict();
}

#[tokio::test]
async fn a_discovery_against_a_connection_that_does_not_exist_is_404_and_stores_nothing() {
    let app = TestApp::new();
    app.post(
        &format!("/api/v1/namespaces/{NS_A}/connections/absent/topic-discoveries"),
        Some("discovery-key-00002"),
        "{}",
    )
    .await
    .assert_problem(404, "not_found");
    assert_eq!(app.fake.count("topicdiscoveries", NS_A), 0);
    app.fake.assert_strict();
}

#[tokio::test]
async fn malformed_parameters_are_refused_by_field() {
    let app = TestApp::new();
    seed_connection(&app);
    let path = format!("/api/v1/namespaces/{NS_A}/connections/source/topic-discoveries");
    for (body, field) in [
        (r#"{"maxTopics": 0}"#, "maxTopics"),
        (r#"{"maxTopics": 50001}"#, "maxTopics"),
        (r#"{"timeoutSeconds": 5}"#, "timeoutSeconds"),
        (r#"{"expectedTopics": ["orders*"]}"#, "expectedTopics[0]"),
    ] {
        let response = app.post(&path, Some("discovery-bad-000001"), body).await;
        response.assert_problem(422, "validation_failed");
        assert_eq!(response.json()["errors"][0]["field"], field, "{body}");
    }
    // A pattern is refused here, as everywhere: G-GLOB.
    assert_eq!(app.fake.count("topicdiscoveries", NS_A), 0);
}

#[tokio::test]
async fn a_fresh_identical_discovery_is_reused_rather_than_repeated() {
    let app = TestApp::new();
    seed_connection(&app);
    let path = format!("/api/v1/namespaces/{NS_A}/connections/source/topic-discoveries");
    let first = app.post(&path, Some("discovery-reuse-0001"), "{}").await;
    assert_eq!(first.status.as_u16(), 202);
    let id = first.json()["item"]["id"].as_str().unwrap().to_string();

    // Mark it succeeded and fresh, as the controller would.
    let mut stored = app.fake.object("topicdiscoveries", NS_A, &id).unwrap();
    stored["status"] = json!({
        "phase": "Succeeded",
        "reason": "Succeeded",
        "binding": {"connectionName": "source", "connectionUid": "seed-connection-uid", "connectionGeneration": 1, "principal": "User:scram-user"},
        "observedAt": "2026-09-15T11:55:00Z",
        "freshUntil": "2026-09-15T12:10:00Z",
        "result": {"format": "logweir.dev/topic-inventory/v1", "counts": {"listed": 0, "returned": 0, "internalExcluded": 0, "errored": 0}, "visibility": {"state": "unknown"}}
    });
    app.fake.seed("topicdiscoveries", NS_A, stored);

    // A NEW key, the same parameters: the fresh result is returned instead of
    // another Kafka connection being made.
    let reused = app.post(&path, Some("discovery-reuse-0002"), "{}").await;
    assert_eq!(reused.status.as_u16(), 200, "{}", reused.text());
    assert_eq!(reused.json()["reused"], true);
    assert_eq!(reused.json()["item"]["id"], id);
    assert_eq!(app.fake.count("topicdiscoveries", NS_A), 1);

    // reuseFresh: false always starts one.
    let fresh = app
        .post(
            &path,
            Some("discovery-reuse-0003"),
            r#"{"reuseFresh": false}"#,
        )
        .await;
    assert_eq!(fresh.status.as_u16(), 202);
    assert_eq!(app.fake.count("topicdiscoveries", NS_A), 2);
    app.fake.assert_strict();
}

#[tokio::test]
async fn the_create_rate_is_bounded_per_actor_and_namespace() {
    let app = TestApp::new();
    // ITS OWN NAMESPACE, BECAUSE THE WINDOW IS KEYED BY ONE. The limiter is a
    // process global (`routes::check_create_rate` says why), and every test in
    // this binary runs in the same process; counting in a namespace no other
    // test creates in is what makes this arm deterministic instead of
    // order-dependent.
    let namespace = support::NS_B;
    seed_connection_in(&app, namespace);
    logweir_api::routes::reset_check_rate_limits();
    let path = format!("/api/v1/namespaces/{namespace}/connections/source/topic-discoveries");
    for i in 0..6 {
        let response = app
            .post(
                &path,
                Some(&format!("rate-key-{i:010}")),
                r#"{"reuseFresh": false}"#,
            )
            .await;
        assert_eq!(response.status.as_u16(), 202, "create {i}");
    }
    let limited = app
        .post(
            &path,
            Some("rate-key-overflow1"),
            r#"{"reuseFresh": false}"#,
        )
        .await;
    limited.assert_problem(429, "rate_limited");
    assert!(limited.header("retry-after").is_some());
    assert_eq!(app.fake.count("topicdiscoveries", namespace), 6);

    // The window resets, and the clock is injected so nothing sleeps.
    app.clock.advance(61);
    let after = app
        .post(
            &path,
            Some("rate-key-afterwind"),
            r#"{"reuseFresh": false}"#,
        )
        .await;
    assert_eq!(after.status.as_u16(), 202);
    logweir_api::routes::reset_check_rate_limits();
}

// ======================================================================
// The topics page
// ======================================================================

#[tokio::test]
async fn the_topics_page_is_bounded_ordered_and_searchable() {
    let app = TestApp::new();
    let mut chunk_a = lines("bulk", 0, 4);
    chunk_a.push(topic_line("__consumer_offsets", 50, "internal"));
    let chunk_b = vec![
        topic_line("orders", 6, "expected"),
        topic_line("payments", 3, "error:TopicAuthorizationFailed"),
    ];
    seed_discovery(
        &app.fake,
        NS_A,
        "td-0001",
        "source",
        Some(LOCAL_ADMIN_ACTOR),
        &[chunk_a, chunk_b],
    );

    let first = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/topic-discoveries/td-0001/topics?limit=2"
        ))
        .await;
    assert_eq!(first.status.as_u16(), 200, "{}", first.text());
    let v = first.json();
    assert_eq!(v["items"].as_array().unwrap().len(), 2);
    assert_eq!(v["items"][0]["name"], "bulk-00000");
    assert_eq!(v["items"][0]["partitions"], 3);
    assert_eq!(v["items"][0]["internal"], false);
    // THE SNAPSHOT NAMES THE EXACT RESULT, so a continued page cannot land on
    // a different inventory.
    let snapshot = v["page"]["snapshot"].as_str().unwrap().to_string();
    assert!(snapshot.starts_with("uid-td-0001@sha256:"), "{snapshot}");
    assert_eq!(v["scan"]["complete"], false);
    let cursor = v["page"]["nextCursor"].as_str().unwrap().to_string();

    let next = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/topic-discoveries/td-0001/topics?limit=2&cursor={cursor}"
        ))
        .await;
    assert_eq!(next.json()["items"][0]["name"], "bulk-00002");
    assert_eq!(next.json()["page"]["snapshot"], snapshot);

    // Internal topics are excluded by default and only appear when asked for.
    let all = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/topic-discoveries/td-0001/topics?limit=200"
        ))
        .await;
    let all_body = all.json();
    let names: Vec<&str> = all_body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["name"].as_str().unwrap())
        .collect();
    assert!(!names.contains(&"__consumer_offsets"), "{names:?}");
    assert_eq!(all_body["scan"]["complete"], true);
    let internal = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/topic-discoveries/td-0001/topics?limit=200&internal=include"
        ))
        .await;
    assert!(internal.text().contains("__consumer_offsets"));

    // `q` is a case-insensitive substring; `prefix` skips whole chunks.
    let q = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/topic-discoveries/td-0001/topics?q=ORDER"
        ))
        .await;
    assert_eq!(q.json()["items"].as_array().unwrap().len(), 1);
    assert_eq!(q.json()["items"][0]["name"], "orders");
    assert_eq!(q.json()["items"][0]["expected"], true);

    let prefix = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/topic-discoveries/td-0001/topics?prefix=orders"
        ))
        .await;
    assert_eq!(prefix.json()["items"].as_array().unwrap().len(), 1);
    // ONE CHUNK READ, NOT TWO: the first chunk's stored name range cannot
    // contain the prefix, so it is skipped without being fetched.
    assert_eq!(prefix.json()["scan"]["chunksScanned"], 1);

    // `errored` selects the entries whose metadata could not be read.
    let errored = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/topic-discoveries/td-0001/topics?errored=only"
        ))
        .await;
    assert_eq!(errored.json()["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        errored.json()["items"][0]["errorCode"],
        "TopicAuthorizationFailed"
    );
    app.fake.assert_strict();
}

#[tokio::test]
async fn a_topics_cursor_is_bound_to_the_actor_the_result_and_the_filters() {
    let app = TestApp::new();
    seed_discovery(
        &app.fake,
        NS_A,
        "td-0002",
        "source",
        Some(LOCAL_ADMIN_ACTOR),
        &[lines("bulk", 0, 6)],
    );
    let first = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/topic-discoveries/td-0002/topics?limit=2"
        ))
        .await;
    let cursor = first.json()["page"]["nextCursor"]
        .as_str()
        .unwrap()
        .to_string();

    // A CHANGED FILTER IS A DIFFERENT LIST. Reusing the cursor with `q` set is
    // refused rather than answered from a position that meant something else.
    app.get(&format!(
        "/api/v1/namespaces/{NS_A}/topic-discoveries/td-0002/topics?limit=2&q=bulk&cursor={cursor}"
    ))
    .await
    .assert_problem(400, "cursor_invalid");

    // A flipped character fails the MAC before anything else is read.
    let mut bytes = cursor.clone().into_bytes();
    bytes[1] = if bytes[1] == b'A' { b'B' } else { b'A' };
    let tampered = String::from_utf8(bytes).unwrap();
    app.get(&format!(
        "/api/v1/namespaces/{NS_A}/topic-discoveries/td-0002/topics?limit=2&cursor={tampered}"
    ))
    .await
    .assert_problem(400, "cursor_invalid");

    // And it expires.
    app.clock.advance(901);
    app.get(&format!(
        "/api/v1/namespaces/{NS_A}/topic-discoveries/td-0002/topics?limit=2&cursor={cursor}"
    ))
    .await
    .assert_problem(410, "cursor_expired");
    app.fake.assert_strict();
}

/// THE INTEGRITY RULE, WITH ITS THREE MUTANTS. A chunk that is not owned by
/// this discovery, is not immutable, or whose bytes do not hash to the digest
/// the status indexes, is refused — the page is never served from bytes whose
/// provenance did not hold.
#[tokio::test]
async fn a_chunk_that_fails_its_integrity_check_refuses_the_page() {
    for mutant in ["owner", "immutable", "digest"] {
        let app = TestApp::new();
        seed_discovery(
            &app.fake,
            NS_A,
            "td-0003",
            "source",
            Some(LOCAL_ADMIN_ACTOR),
            &[lines("bulk", 0, 3)],
        );
        let mut chunk = app
            .fake
            .object("configmaps", NS_A, "lwc-td-0003-r000")
            .unwrap();
        match mutant {
            "owner" => chunk["metadata"]["ownerReferences"][0]["uid"] = json!("someone-else"),
            "immutable" => chunk["immutable"] = json!(false),
            _ => chunk["data"]["topics.tsv"] = json!(topic_line("planted", 1, "-")),
        }
        app.fake.seed("configmaps", NS_A, chunk);

        let response = app
            .get(&format!(
                "/api/v1/namespaces/{NS_A}/topic-discoveries/td-0003/topics"
            ))
            .await;
        response.assert_problem(409, "result_integrity_failed");
        assert!(
            !response.text().contains("planted"),
            "the refused page still leaked a row ({mutant})"
        );
    }
}

#[tokio::test]
async fn a_collected_chunk_is_gone_rather_than_half_a_page() {
    let app = TestApp::new();
    seed_discovery(
        &app.fake,
        NS_A,
        "td-0004",
        "source",
        Some(LOCAL_ADMIN_ACTOR),
        &[lines("bulk", 0, 3)],
    );
    // The owner cascade removed the chunk; the status still indexes it.
    let mut state = app
        .fake
        .object("topicdiscoveries", NS_A, "td-0004")
        .unwrap();
    state["status"]["result"]["chunks"][0]["name"] = json!("lwc-td-0004-gone");
    app.fake.seed("topicdiscoveries", NS_A, state);
    app.get(&format!(
        "/api/v1/namespaces/{NS_A}/topic-discoveries/td-0004/topics"
    ))
    .await
    .assert_problem(410, "cursor_expired");
    app.fake.assert_strict();
}

#[tokio::test]
async fn the_read_never_lists_config_maps() {
    let app = TestApp::new();
    seed_discovery(
        &app.fake,
        NS_A,
        "td-0005",
        "source",
        Some(LOCAL_ADMIN_ACTOR),
        &[lines("bulk", 0, 3)],
    );
    app.fake.clear_requests();
    app.get(&format!(
        "/api/v1/namespaces/{NS_A}/topic-discoveries/td-0005/topics"
    ))
    .await;
    for request in app.fake.requests() {
        assert!(
            !(request.path.ends_with("/configmaps") && request.method == "GET"),
            "a ConfigMap LIST was issued: {} {}",
            request.method,
            request.path
        );
    }
    app.fake.assert_strict();
}

// ======================================================================
// Freshness, the two slots, and cancellation
// ======================================================================

#[tokio::test]
async fn a_result_goes_stale_when_its_connection_binding_changes() {
    let app = TestApp::new();
    seed_connection(&app);
    seed_discovery(
        &app.fake,
        NS_A,
        "td-0006",
        "source",
        Some(LOCAL_ADMIN_ACTOR),
        &[lines("bulk", 0, 2)],
    );
    let fresh = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/topic-discoveries/td-0006"
        ))
        .await;
    assert_eq!(fresh.json()["item"]["stale"], false);
    // A SUCCESSFUL LIST IS NEVER CALLED COMPLETE.
    assert_eq!(fresh.json()["item"]["visibility"]["state"], "unknown");

    // The connection's principal changed under it.
    let mut cluster = app.fake.object("kafkaclusters", NS_A, "source").unwrap();
    cluster["spec"]["auth"]["username"] = json!("someone-else");
    app.fake.seed("kafkaclusters", NS_A, cluster);
    let stale = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/topic-discoveries/td-0006"
        ))
        .await;
    assert_eq!(stale.json()["item"]["stale"], true);
    assert_eq!(stale.json()["item"]["staleReasons"][0], "principalChanged");

    // And so does the clock.
    app.clock.advance(3600);
    let expired = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/topic-discoveries/td-0006"
        ))
        .await;
    let expired_body = expired.json();
    let reasons: Vec<&str> = expired_body["item"]["staleReasons"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r.as_str().unwrap())
        .collect();
    assert!(reasons.contains(&"expired"), "{reasons:?}");
    app.fake.assert_strict();
}

#[tokio::test]
async fn a_failed_attempt_never_hides_the_last_successful_inventory() {
    let app = TestApp::new();
    seed_connection(&app);
    seed_discovery(
        &app.fake,
        NS_A,
        "td-a-good",
        "source",
        Some(LOCAL_ADMIN_ACTOR),
        &[lines("bulk", 0, 2)],
    );
    app.fake.seed(
        "topicdiscoveries",
        NS_A,
        json!({
            "metadata": {"name": "td-b-failed", "labels": {"logweir.dev/connection": "source"}, "creationTimestamp": "2026-09-15T11:59:00Z"},
            "spec": {"request": {"connectionRef": {"name": "source"}, "includeInternal": false, "maxTopics": 20000, "timeoutSeconds": 60}, "cancelRequested": false},
            "status": {"phase": "Failed", "reason": "AuthenticationFailed", "message": "SASL authentication failed"}
        }),
    );
    let response = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/connections/source/topic-discoveries?latest=true"
        ))
        .await;
    let v = response.json();
    assert_eq!(v["latestAttempt"]["id"], "td-b-failed");
    assert_eq!(v["latestAttempt"]["state"], "failed");
    assert_eq!(v["latestAttempt"]["error"]["code"], "AuthenticationFailed");
    assert_eq!(v["lastSuccessful"]["id"], "td-a-good");
    app.fake.assert_strict();
}

#[tokio::test]
async fn cancel_is_idempotent_and_exact_owner() {
    let app = TestApp::new();
    seed_running_discovery(&app.fake, NS_A, "td-mine", "source", LOCAL_ADMIN_ACTOR);
    seed_running_discovery(
        &app.fake,
        NS_A,
        "td-theirs",
        "source",
        "urn:test#another-operator",
    );
    let path = format!("/api/v1/namespaces/{NS_A}/topic-discoveries/td-mine:cancel");

    let first = app.post(&path, None, "{}").await;
    assert_eq!(first.status.as_u16(), 200, "{}", first.text());
    assert_eq!(first.json()["alreadyTerminal"], false);
    assert_eq!(
        app.fake
            .object("topicdiscoveries", NS_A, "td-mine")
            .unwrap()["spec"]["cancelRequested"],
        true
    );

    // REPEATING IT WRITES NOTHING AND STILL ANSWERS 200.
    app.fake.clear_requests();
    let again = app.post(&path, None, "{}").await;
    assert_eq!(again.status.as_u16(), 200);
    assert_eq!(again.json()["alreadyTerminal"], false);
    assert!(
        !app.fake.requests().iter().any(|r| r.method == "PATCH"),
        "a repeated cancel patched again"
    );

    // A finished check is 200 `alreadyTerminal`, with nothing written.
    let mut terminal = app
        .fake
        .object("topicdiscoveries", NS_A, "td-mine")
        .unwrap();
    terminal["status"]["phase"] = json!("Cancelled");
    app.fake.seed("topicdiscoveries", NS_A, terminal);
    app.fake.clear_requests();
    let done = app.post(&path, None, "{}").await;
    assert_eq!(done.status.as_u16(), 200);
    assert_eq!(done.json()["alreadyTerminal"], true);
    assert!(!app.fake.requests().iter().any(|r| r.method == "PATCH"));

    // ANOTHER ACTOR'S CHECK IS NOT THIS ACTOR'S TO STOP.
    let theirs = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/topic-discoveries/td-theirs:cancel"),
            None,
            "{}",
        )
        .await;
    theirs.assert_problem(403, "forbidden");
    assert_eq!(
        app.fake
            .object("topicdiscoveries", NS_A, "td-theirs")
            .unwrap()["spec"]["cancelRequested"],
        false
    );
    app.fake.assert_strict();
}

#[tokio::test]
async fn a_cancel_refuses_an_idempotency_key_and_a_bad_command() {
    let app = TestApp::new();
    seed_running_discovery(&app.fake, NS_A, "td-mine", "source", LOCAL_ADMIN_ACTOR);
    app.post(
        &format!("/api/v1/namespaces/{NS_A}/topic-discoveries/td-mine:cancel"),
        Some("cancel-key-000001"),
        "{}",
    )
    .await
    .assert_problem(400, "idempotency_key_invalid");
    app.post(
        &format!("/api/v1/namespaces/{NS_A}/topic-discoveries/td-mine:stop"),
        None,
        "{}",
    )
    .await
    .assert_problem(404, "not_found");
}

#[tokio::test]
async fn the_operation_route_serves_a_check_without_inventing_a_verification() {
    let app = TestApp::new();
    seed_running_discovery(&app.fake, NS_A, "td-mine", "source", LOCAL_ADMIN_ACTOR);
    let response = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/operations/discovery/td-mine"
        ))
        .await;
    let v = response.json();
    assert_eq!(v["item"]["kind"], "discovery");
    assert_eq!(v["item"]["state"], "running");
    assert_eq!(v["item"]["terminal"], false);
    assert_eq!(v["item"]["cancellable"], true);
    // A CHECK HAS NO SIGNED EVIDENCE AND NO VERDICT, and the contract does not
    // pretend otherwise.
    for absent in ["verification", "result", "evidence", "verifiedSuccess"] {
        assert!(v["item"].get(absent).is_none(), "{absent} is published");
    }
    app.fake.assert_strict();
}

/// A response body may carry a field this build does not know, and the read
/// still succeeds. The server side of that rule: an unrecognised phase is read
/// as `unknown` rather than as success.
#[tokio::test]
async fn an_unrecognised_phase_is_never_read_as_success() {
    let app = TestApp::new();
    let mut object: Value =
        seed_running_discovery(&app.fake, NS_A, "td-odd", "source", LOCAL_ADMIN_ACTOR);
    object["status"]["phase"] = json!("Ascendant");
    object["status"]["somethingNewer"] = json!({"a": 1});
    app.fake.seed("topicdiscoveries", NS_A, object);
    let response = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/topic-discoveries/td-odd"
        ))
        .await;
    assert_eq!(response.status.as_u16(), 200, "{}", response.text());
    assert_eq!(response.json()["item"]["state"], "unknown");
    assert_eq!(response.json()["item"]["terminal"], false);
}
