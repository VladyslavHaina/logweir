//! Destinations: the create contract, the rules, the rotation precondition,
//! and the one property that matters most — a credential value entered once
//! never comes back out.

mod support;

use serde_json::{json, Value};
use support::{
    seed_destination, TestApp, ACCESS_KEY_ID, ACTOR_ANNOTATION, NS_A, SECRET_ACCESS_KEY,
};

fn body(name: &str) -> String {
    support::destination_body(name).to_string()
}

#[tokio::test]
async fn a_destination_is_created_under_its_own_name_with_references_only() {
    let app = TestApp::new();
    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/destinations"),
            Some("destination-create-01"),
            &body("primary"),
        )
        .await;
    assert_eq!(response.status.as_u16(), 201, "{}", response.text());
    let v = response.json();
    let item = &v["item"];
    // THE NAME IS THE OPERATOR'S, because every schedule, backup and restore
    // references a destination by name.
    assert_eq!(item["name"], "primary");
    assert_eq!(item["storage"]["addressing"], "pathStyle");
    assert_eq!(item["transport"]["security"], "tls");
    assert_eq!(item["transport"]["caBundle"]["key"], "ca.crt");
    assert_eq!(item["access"]["archiveWrite"]["mode"], "secretKeys");
    assert_eq!(item["access"]["archiveWrite"]["secretName"], "logweir-s3");
    assert_eq!(
        item["access"]["archiveWrite"]["keys"],
        json!(["access-key-id", "secret-access-key"])
    );
    // ABSENT IS A STATED ANSWER, not a blank field.
    assert_eq!(
        item["access"]["evidenceWrite"]["mode"],
        "inheritsArchiveWrite"
    );
    assert_eq!(item["access"]["evidenceRead"]["mode"], "archiveReadGrant");
    // The canonical URL is computed before the controller has ever seen the
    // object, from the same pure function the controller will use.
    assert_eq!(item["canonicalUrl"], "s3://kafka-backups/team-a/prod");
    assert_eq!(item["default"], false);
    assert_eq!(item["writeProbe"], "createOnlyMarker");
    // The controller has no verdict yet, and `valid` is absent rather than
    // false: "not judged" is not "invalid".
    assert!(item["status"].get("valid").is_none());

    let stored = app
        .fake
        .object("backupdestinations", NS_A, "primary")
        .expect("the object exists under the operator's name");
    assert_eq!(stored["spec"]["storage"]["addressing"], "PathStyle");
    assert_eq!(stored["spec"]["transport"]["security"], "TLS");
    assert_eq!(
        stored["spec"]["readiness"]["writeProbe"],
        "CreateOnlyMarker"
    );
    app.fake.assert_strict();
}

#[tokio::test]
async fn the_same_key_and_the_same_request_replays_and_a_different_one_conflicts() {
    let app = TestApp::new();
    let path = format!("/api/v1/namespaces/{NS_A}/destinations");
    let first = app
        .post(&path, Some("destination-create-02"), &body("primary"))
        .await;
    assert_eq!(first.status.as_u16(), 201);
    let replay = app
        .post(&path, Some("destination-create-02"), &body("primary"))
        .await;
    assert_eq!(replay.status.as_u16(), 200);
    assert_eq!(replay.json()["replayed"], true);
    assert_eq!(replay.json()["item"]["uid"], first.json()["item"]["uid"]);

    let mut changed = support::destination_body("primary");
    changed["description"] = json!("something else");
    let conflict = app
        .post(&path, Some("destination-create-02"), &changed.to_string())
        .await;
    conflict.assert_problem(409, "idempotency_conflict");
    assert_eq!(app.fake.count("backupdestinations", NS_A), 1);
    app.fake.assert_strict();
}

#[tokio::test]
async fn a_name_another_scope_created_is_never_adopted() {
    let app = TestApp::new();
    seed_destination(&app.fake, NS_A, "primary");
    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/destinations"),
            Some("destination-create-03"),
            &body("primary"),
        )
        .await;
    response.assert_problem(409, "state_conflict");
    app.fake.assert_strict();
}

#[tokio::test]
async fn the_rules_are_evaluated_before_the_object_exists() {
    let app = TestApp::new();
    let path = format!("/api/v1/namespaces/{NS_A}/destinations");

    // R3, in the direction the CRD spells `insecureHttp`.
    let mut insecure = support::destination_body("d1");
    insecure["transport"] = json!({"security": "insecureHttp"});
    insecure["storage"]["endpoint"] = json!("https://minio.storage.svc:9000");
    let response = app
        .post(&path, Some("destination-bad-0001"), &insecure.to_string())
        .await;
    response.assert_problem(422, "destination_invalid");
    let codes = field_codes(&response.json());
    assert!(
        codes.contains(&"insecure_http_requires_http_endpoint".to_string()),
        "{codes:?}"
    );

    // R3, the other direction.
    let mut mismatch = support::destination_body("d2");
    mismatch["storage"]["endpoint"] = json!("http://minio.storage.svc:9000");
    let response = app
        .post(&path, Some("destination-bad-0002"), &mismatch.to_string())
        .await;
    response.assert_problem(422, "destination_invalid");
    assert!(field_codes(&response.json()).contains(&"transport_scheme_mismatch".to_string()));

    // R5: an endpoint with a path is not an origin.
    let mut endpoint = support::destination_body("d3");
    endpoint["storage"]["endpoint"] = json!("https://minio.storage.svc:9000/bucket");
    let response = app
        .post(&path, Some("destination-bad-0003"), &endpoint.to_string())
        .await;
    assert!(field_codes(&response.json()).contains(&"endpoint_not_origin".to_string()));

    // R6: the reserved evidence root.
    let mut prefix = support::destination_body("d4");
    prefix["storage"]["prefix"] = json!("logweir/evidence");
    let response = app
        .post(&path, Some("destination-bad-0004"), &prefix.to_string())
        .await;
    assert!(field_codes(&response.json()).contains(&"prefix_reserved".to_string()));

    // The bucket pattern.
    let mut bucket = support::destination_body("d5");
    bucket["storage"]["bucket"] = json!("NotABucket");
    let response = app
        .post(&path, Some("destination-bad-0005"), &bucket.to_string())
        .await;
    assert!(field_codes(&response.json()).contains(&"bucket_invalid".to_string()));

    // G4: virtual-hosted addressing with a custom endpoint is a setting the
    // pinned engine cannot honour, and is REFUSED rather than advertised.
    let mut addressing = support::destination_body("d6");
    addressing["storage"]["addressing"] = json!("virtualHosted");
    let response = app
        .post(&path, Some("destination-bad-0006"), &addressing.to_string())
        .await;
    assert!(field_codes(&response.json()).contains(&"addressing_unsupported_by_engine".to_string()));

    // R4: a CA bundle without TLS.
    let mut ca = support::destination_body("d7");
    ca["transport"] = json!({"security": "insecureHttp", "caBundle": {"configMapName": "x"}});
    ca["storage"]["endpoint"] = json!("http://minio.storage.svc:9000");
    let response = app
        .post(&path, Some("destination-bad-0007"), &ca.to_string())
        .await;
    assert!(field_codes(&response.json()).contains(&"ca_bundle_requires_tls".to_string()));

    // R9: reusing a write grant to read evidence.
    let mut grant = support::destination_body("d8");
    grant["access"] = json!({
        "archiveWrite": {"mode": "secretKeys", "secret": {"existing": {"name": "logweir-s3"}}},
        "evidenceRead": {"mode": "archiveReadGrant"}
    });
    let response = app
        .post(&path, Some("destination-bad-0008"), &grant.to_string())
        .await;
    assert!(field_codes(&response.json()).contains(&"grant_mode_fields".to_string()));

    // A response-only mode is refused in a request.
    let mut mode = support::destination_body("d9");
    mode["access"]["archiveRead"] = json!({"mode": "inheritsArchiveWrite"});
    let response = app
        .post(&path, Some("destination-bad-0009"), &mode.to_string())
        .await;
    assert!(field_codes(&response.json()).contains(&"grant_mode_fields".to_string()));

    // Nothing reached Kubernetes: every rule above is evaluated here.
    assert!(
        app.fake.requests().is_empty(),
        "a refused destination reached Kubernetes: {:#?}",
        app.fake.requests()
    );
}

/// THE ADDRESSING/TRANSPORT INDEPENDENCE, BOTH WAYS.
///
/// Defect G5 was a UI that derived `allowHttp` from a path-style checkbox.
/// This asserts the server contract that makes the derivation impossible:
/// path-style over TLS is accepted with `security: tls`, and choosing
/// path-style never changes the transport that was asked for.
#[tokio::test]
async fn addressing_never_decides_transport() {
    let app = TestApp::new();
    let path = format!("/api/v1/namespaces/{NS_A}/destinations");
    let mut request = support::destination_body("path-style-tls");
    request["storage"]["addressing"] = json!("pathStyle");
    let response = app
        .post(&path, Some("destination-g5-00001"), &request.to_string())
        .await;
    assert_eq!(response.status.as_u16(), 201);
    assert_eq!(response.json()["item"]["transport"]["security"], "tls");
    let stored = app
        .fake
        .object("backupdestinations", NS_A, "path-style-tls")
        .unwrap();
    assert_eq!(stored["spec"]["transport"]["security"], "TLS");
    assert_eq!(stored["spec"]["storage"]["addressing"], "PathStyle");
}

// ======================================================================
// The write-only credential
// ======================================================================

/// A credential entered once is created as a Secret and is never echoed, in
/// the response, in the stored object, or anywhere in the process's own view
/// of what it created.
#[tokio::test]
async fn a_credential_value_is_created_once_and_never_comes_back() {
    let app = TestApp::new();
    let request = support::destination_body_with_new_credential("primary");
    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/destinations"),
            Some("destination-cred-001"),
            &request.to_string(),
        )
        .await;
    assert_eq!(response.status.as_u16(), 201, "{}", response.text());

    let text = response.text();
    assert!(
        !text.contains(SECRET_ACCESS_KEY) && !text.contains(ACCESS_KEY_ID),
        "the response echoed a credential: {text}"
    );
    // What comes back is a NAME and the KEY NAMES, which are public
    // references.
    let grant = &response.json()["item"]["access"]["archiveRead"];
    assert_eq!(grant["secretName"], "lwd-primary-archive-read");
    assert_eq!(grant["keys"], json!(["access-key-id", "secret-access-key"]));
    assert!(grant.get("secret").is_none());

    // The Secret exists, was created with the distinct type, is owned by the
    // destination and was never read back.
    let secret = app
        .fake
        .object("secrets", NS_A, "lwd-primary-archive-read")
        .expect("the credential Secret was created");
    assert_eq!(secret["type"], "logweir.dev/object-store-credential");
    assert_eq!(
        secret["metadata"]["ownerReferences"][0]["kind"],
        "BackupDestination"
    );
    assert_eq!(
        secret["metadata"]["labels"]["logweir.dev/credential-for"],
        "primary"
    );
    assert_eq!(
        secret["metadata"]["annotations"]["logweir.dev/write-only"],
        "true"
    );

    // The stored destination names the Secret and holds no value.
    let stored = app
        .fake
        .object("backupdestinations", NS_A, "primary")
        .unwrap();
    let stored_text = stored.to_string();
    assert!(!stored_text.contains(SECRET_ACCESS_KEY) && !stored_text.contains(ACCESS_KEY_ID));
    assert_eq!(
        stored["spec"]["access"]["archiveRead"]["secret"]["name"],
        "lwd-primary-archive-read"
    );

    // EXACTLY ONE SECRET VERB WAS USED, AND IT WAS A CREATE. No GET, no LIST,
    // no PATCH on `secrets` anywhere in the exchange.
    let secret_calls: Vec<String> = app
        .fake
        .requests()
        .iter()
        .filter(|r| r.path.contains("/secrets"))
        .map(|r| format!("{} {}", r.method, r.path))
        .collect();
    assert_eq!(
        secret_calls,
        vec![format!("POST /api/v1/namespaces/{NS_A}/secrets")]
    );
    app.fake.assert_strict();
}

/// THE MUTANT. The fake API server echoes `data` on a create exactly as
/// Kubernetes does. If the adapter's Secret type could deserialize `data`, the
/// value would be inside this process and one careless projection would ship
/// it. This asserts the echo really is in the response the adapter received —
/// so the guard above is testing something — and that it is gone by the time
/// anything can see it.
#[tokio::test]
async fn the_api_servers_credential_echo_is_dropped_by_the_parser() {
    let app = TestApp::new();
    let request = support::destination_body_with_new_credential("primary");
    app.post(
        &format!("/api/v1/namespaces/{NS_A}/destinations"),
        Some("destination-cred-002"),
        &request.to_string(),
    )
    .await;

    // The REQUEST this service sent carried the value — that is the one
    // direction it may travel.
    let create = app
        .fake
        .requests()
        .into_iter()
        .find(|r| r.method == "POST" && r.path.ends_with("/secrets"))
        .expect("a credential Secret was created");
    let sent: Value = serde_json::from_str(&create.body).expect("the body is JSON");
    let encoded = sent["data"]["secret-access-key"].as_str().unwrap();
    use base64::Engine as _;
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .unwrap(),
        SECRET_ACCESS_KEY,
        "the create really did carry the value"
    );

    // And the stored object the fake echoes back carries it too, so the
    // response body the adapter parsed was NOT credential-free.
    let stored = app
        .fake
        .object("secrets", NS_A, "lwd-primary-archive-read")
        .unwrap();
    assert!(stored["data"]["secret-access-key"].is_string());

    // Nothing that left this service carries it.
    let read = app
        .get(&format!("/api/v1/namespaces/{NS_A}/destinations/primary"))
        .await;
    assert!(!read.text().contains(SECRET_ACCESS_KEY));
    let list = app
        .get(&format!("/api/v1/namespaces/{NS_A}/destinations"))
        .await;
    assert!(!list.text().contains(SECRET_ACCESS_KEY));
}

#[tokio::test]
async fn a_malformed_credential_is_refused_by_class_and_never_by_value() {
    let app = TestApp::new();
    let mut request = support::destination_body("primary");
    request["access"]["archiveRead"] = json!({
        "mode": "secretKeys",
        "secret": {"new": {"accessKeyId": "AKIA1", "secretAccessKey": " padded-secret "}}
    });
    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/destinations"),
            Some("destination-cred-003"),
            &request.to_string(),
        )
        .await;
    response.assert_problem(422, "destination_invalid");
    let text = response.text();
    assert!(
        !text.contains("padded-secret"),
        "the refusal echoed the value: {text}"
    );
    assert!(field_codes(&response.json()).contains(&"edge_whitespace".to_string()));
    // Nothing was created: a malformed request never reaches the cluster.
    assert!(app.fake.requests().is_empty());
}

#[tokio::test]
async fn an_unknown_field_in_a_destination_request_is_refused() {
    let app = TestApp::new();
    let mut request = support::destination_body("primary");
    request["storage"]["pathStyle"] = json!(true);
    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/destinations"),
            Some("destination-strict-01"),
            &request.to_string(),
        )
        .await;
    // The unknown field is NAMED, not merely refused: an operator who typed a
    // field this contract does not have is told which one.
    response.assert_problem(422, "validation_failed");
    assert_eq!(response.json()["errors"][0]["field"], "pathStyle");
}

// ======================================================================
// Rotation
// ======================================================================

#[tokio::test]
async fn update_access_rotates_the_grants_under_a_generation_precondition() {
    let app = TestApp::new();
    seed_destination(&app.fake, NS_A, "primary");
    let path = format!("/api/v1/namespaces/{NS_A}/destinations/primary:update-access");
    let rotation = json!({
        "expectedGeneration": 3,
        "access": {
            "archiveWrite": {"mode": "secretKeys", "secret": {"existing": {"name": "logweir-s3-new"}}},
            "archiveRead": {"mode": "secretKeys", "secret": {"existing": {"name": "archive-reader"}}},
            "evidenceRead": {"mode": "archiveReadGrant"}
        }
    });
    let response = app.post(&path, None, &rotation.to_string()).await;
    assert_eq!(response.status.as_u16(), 200, "{}", response.text());
    assert_eq!(
        response.json()["item"]["access"]["archiveWrite"]["secretName"],
        "logweir-s3-new"
    );

    // THE PATCH NAMES ONLY THE MUTABLE HALF. The fake refuses anything else,
    // so this assertion is carried by the harness as well as by the string.
    let patch = app
        .fake
        .requests()
        .into_iter()
        .find(|r| r.method == "PATCH")
        .expect("a patch was sent");
    let body: Value = serde_json::from_str(&patch.body).unwrap();
    assert!(body["spec"].get("storage").is_none());
    assert!(body["metadata"]["resourceVersion"].is_string());

    // A stale generation is refused, and nothing is written.
    app.fake.clear_requests();
    let stale = app.post(&path, None, &rotation.to_string()).await;
    stale.assert_problem(412, "precondition_failed");
    assert!(
        !app.fake.requests().iter().any(|r| r.method == "PATCH"),
        "a stale rotation still patched"
    );
    app.fake.assert_strict();
}

#[tokio::test]
async fn update_access_refuses_an_idempotency_key_and_a_ca_bundle_on_plaintext() {
    let app = TestApp::new();
    seed_destination(&app.fake, NS_A, "primary");
    let path = format!("/api/v1/namespaces/{NS_A}/destinations/primary:update-access");
    let rotation = json!({
        "expectedGeneration": 3,
        "access": {"archiveWrite": {"mode": "secretKeys", "secret": {"existing": {"name": "s"}}}}
    });
    app.post(&path, Some("rotation-key-00001"), &rotation.to_string())
        .await
        .assert_problem(400, "idempotency_key_invalid");

    // A plaintext destination may never be given trust material, because its
    // transport is immutable and a CA means nothing without TLS.
    app.fake.seed(
        "backupdestinations",
        NS_A,
        json!({
            "metadata": {"name": "plain", "generation": 1},
            "spec": {
                "storage": {"provider": "S3", "bucket": "b", "prefix": "", "endpoint": "http://minio:9000", "addressing": "PathStyle"},
                "transport": {"security": "InsecureHTTP"},
                "access": {"archiveWrite": {"mode": "SecretKeys", "secret": {"name": "s", "accessKeyIdKey": "access-key-id", "secretAccessKeyKey": "secret-access-key"}}}
            }
        }),
    );
    let mut with_ca = rotation.clone();
    with_ca["expectedGeneration"] = json!(1);
    with_ca["transport"] = json!({"caBundle": {"configMapName": "ca"}});
    app.post(
        &format!("/api/v1/namespaces/{NS_A}/destinations/plain:update-access"),
        None,
        &with_ca.to_string(),
    )
    .await
    .assert_problem(409, "transport_downgrade_forbidden");
    app.fake.assert_strict();
}

#[tokio::test]
async fn at_most_one_destination_is_the_namespace_default() {
    let app = TestApp::new();
    let path = format!("/api/v1/namespaces/{NS_A}/destinations");
    let mut first = support::destination_body("primary");
    first["default"] = json!(true);
    let response = app
        .post(&path, Some("default-key-000001"), &first.to_string())
        .await;
    assert_eq!(response.status.as_u16(), 201);
    assert_eq!(response.json()["item"]["default"], true);

    let mut second = support::destination_body("secondary");
    second["default"] = json!(true);
    let conflict = app
        .post(&path, Some("default-key-000002"), &second.to_string())
        .await;
    conflict.assert_problem(409, "state_conflict");
    assert_eq!(app.fake.count("backupdestinations", NS_A), 1);
    app.fake.assert_strict();
}

// ======================================================================
// Reads
// ======================================================================

#[tokio::test]
async fn the_list_is_a_bounded_page_of_rows_and_a_tampered_cursor_is_refused() {
    let app = TestApp::new();
    for i in 0..5 {
        app.fake.seed(
            "backupdestinations",
            NS_A,
            json!({
                "metadata": {"name": format!("d{i}"), "generation": 1},
                "spec": {
                    "storage": {"provider": "S3", "bucket": "kafka-backups", "prefix": format!("p{i}"), "addressing": "VirtualHosted"},
                    "transport": {"security": "TLS"},
                    "access": {"archiveWrite": {"mode": "SecretKeys", "secret": {"name": "s", "accessKeyIdKey": "access-key-id", "secretAccessKeyKey": "secret-access-key"}}}
                }
            }),
        );
    }
    let first = app
        .get(&format!("/api/v1/namespaces/{NS_A}/destinations?limit=2"))
        .await;
    let v = first.json();
    assert_eq!(v["items"].as_array().unwrap().len(), 2);
    assert_eq!(v["items"][0]["canonicalUrl"], "s3://kafka-backups/p0");
    let cursor = v["page"]["nextCursor"].as_str().unwrap().to_string();

    let next = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/destinations?limit=2&cursor={cursor}"
        ))
        .await;
    assert_eq!(next.json()["items"][0]["name"], "d2");

    // A cursor from another list is not this list's cursor.
    let other = app
        .get(&format!(
            "/api/v1/namespaces/{NS_B}/destinations?limit=2&cursor={cursor}",
            NS_B = support::NS_B
        ))
        .await;
    other.assert_problem(400, "cursor_invalid");

    // And a flipped character is refused before the expiry is even read.
    let mut bytes = cursor.into_bytes();
    bytes[2] = if bytes[2] == b'A' { b'B' } else { b'A' };
    let tampered = String::from_utf8(bytes).unwrap();
    app.get(&format!(
        "/api/v1/namespaces/{NS_A}/destinations?limit=2&cursor={tampered}"
    ))
    .await
    .assert_problem(400, "cursor_invalid");
    app.fake.assert_strict();
}

#[tokio::test]
async fn usage_is_a_bounded_label_read_that_states_its_own_basis() {
    let app = TestApp::new();
    seed_destination(&app.fake, NS_A, "primary");
    app.fake.seed(
        "backupschedules",
        NS_A,
        json!({
            "metadata": {"name": "nightly", "labels": {"logweir.dev/destination": "primary"}},
            "spec": {"schedule": "0 3 * * *", "sourceRef": {"name": "source"}, "topics": ["orders"], "archive": {"url": "s3://kafka-backups/team-a/prod"}}
        }),
    );
    let response = app
        .get(&format!(
            "/api/v1/namespaces/{NS_A}/destinations/primary/usage"
        ))
        .await;
    let v = response.json();
    assert_eq!(v["schedules"][0]["name"], "nightly");
    assert_eq!(v["schedules"][0]["kind"], "BackupSchedule");
    assert_eq!(v["backups"], json!([]));
    assert_eq!(v["truncated"], false);
    // THE BASIS IS PART OF THE ANSWER: an empty list is "nothing carries the
    // label", not "nothing uses this destination".
    assert!(v["basis"]
        .as_str()
        .unwrap()
        .contains("logweir.dev/destination"));
    app.fake.assert_strict();
}

#[tokio::test]
async fn a_test_starts_a_destination_access_preflight_labelled_for_the_destination() {
    let app = TestApp::new();
    seed_destination(&app.fake, NS_A, "primary");
    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/destinations/primary:test"),
            Some("destination-test-0001"),
            "{}",
        )
        .await;
    assert_eq!(response.status.as_u16(), 202, "{}", response.text());
    let v = response.json();
    assert_eq!(v["item"]["operation"], "destinationAccess");
    assert_eq!(v["item"]["state"], "pending");
    let id = v["item"]["id"].as_str().unwrap();
    assert!(id.starts_with("pf-"), "{id}");
    let stored = app.fake.object("preflights", NS_A, id).unwrap();
    assert_eq!(
        stored["metadata"]["labels"]["logweir.dev/destination"],
        "primary"
    );
    // The roles default to the ones the destination actually configures.
    assert_eq!(
        stored["spec"]["request"]["destinationAccess"]["roles"],
        json!(["ArchiveWrite", "ArchiveRead", "EvidenceRead"])
    );
    assert_eq!(
        stored["metadata"]["annotations"][ACTOR_ANNOTATION],
        support::LOCAL_ADMIN_ACTOR
    );

    // And the destination read now reports it as the last test.
    let read = app
        .get(&format!("/api/v1/namespaces/{NS_A}/destinations/primary"))
        .await;
    assert_eq!(read.json()["item"]["lastTest"]["preflightId"], id);
    app.fake.assert_strict();
}

#[tokio::test]
async fn a_legacy_schedule_becomes_a_destination_from_facts_or_is_refused() {
    let app = TestApp::new();
    app.fake.seed(
        "backupschedules",
        NS_A,
        json!({
            "metadata": {"name": "nightly"},
            "spec": {"schedule": "0 3 * * *", "sourceRef": {"name": "source"}, "topics": ["orders"], "archive": {"url": "s3://legacy-bucket/team-a"}}
        }),
    );
    let request = json!({
        "name": "adopted",
        "sourceSchedule": "nightly",
        "access": {"archiveWrite": {"mode": "secretKeys", "secret": {"existing": {"name": "logweir-s3"}}}}
    });
    let response = app
        .post(
            &format!("/api/v1/namespaces/{NS_A}/destinations:from-legacy"),
            Some("from-legacy-000001"),
            &request.to_string(),
        )
        .await;
    assert_eq!(response.status.as_u16(), 201, "{}", response.text());
    let v = response.json();
    assert_eq!(v["item"]["storage"]["bucket"], "legacy-bucket");
    assert_eq!(v["item"]["storage"]["prefix"], "team-a");
    assert_eq!(v["item"]["transport"]["security"], "tls");
    // WHAT IT WAS DERIVED FROM IS PART OF THE ANSWER, and the note says what
    // was not recovered rather than guessing it.
    assert_eq!(v["addressingSource"], "installationConfig");
    assert!(v["notes"][0].as_str().unwrap().contains("frozen execution"));

    // A scheme this adoption cannot describe is refused, not guessed.
    app.fake.seed(
        "backupschedules",
        NS_A,
        json!({
            "metadata": {"name": "filey"},
            "spec": {"schedule": "0 3 * * *", "sourceRef": {"name": "source"}, "topics": ["orders"], "archive": {"url": "file:///var/archive"}}
        }),
    );
    let mut other = request.clone();
    other["name"] = json!("adopted-file");
    other["sourceSchedule"] = json!("filey");
    app.post(
        &format!("/api/v1/namespaces/{NS_A}/destinations:from-legacy"),
        Some("from-legacy-000002"),
        &other.to_string(),
    )
    .await
    .assert_problem(404, "legacy_location_unknown");
    app.fake.assert_strict();
}

// ------------------------------------------------------------------ helpers

fn field_codes(problem: &Value) -> Vec<String> {
    problem["errors"]
        .as_array()
        .map(|errors| {
            errors
                .iter()
                .map(|e| e["code"].as_str().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default()
}
