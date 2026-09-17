//! Every problem code, malformed/unknown/oversize input, and the Kubernetes
//! failure mapping with message redaction.

mod support;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use http::request::Parts;
use http::Request;
use logweir_api::auth::{Actor, AuthenticationMode, Authenticator};
use logweir_api::authz::{Action, Authorizer};
use logweir_api::problem::{self, ApiError, ProblemCode};
use serde_json::json;
use support::{FakeKube, Fault, Options, TestApp, HOST, NS_A, ORIGIN};

/// Codes observed end to end in this file, checked against the full list at
/// the end so a new code cannot be added without a producing test or an
/// explicit reservation.
const RESERVED_FOR_LATER_STAGES: [ProblemCode; 4] = [
    // PLAT-17.2 session authenticator.
    ProblemCode::SessionExpired,
    // PLAT-19.2 approval policy.
    ProblemCode::ApprovalRequired,
    ProblemCode::PolicyMismatch,
    // D2 §3.12 step 4: refusing a REPLACEMENT schedule whose destination's
    // locationDigest differs from the legacy one. The replacement is created
    // through `POST .../schedules`, which does not accept a destinationRef
    // yet (PLAT-06.2 / D2 W13), so nothing in this build can produce it. The
    // code is published so the adoption contract is complete and a client can
    // branch on it before the producing route exists.
    ProblemCode::LegacyLocationMismatch,
];

fn schedules() -> String {
    format!("/api/v1/namespaces/{NS_A}/schedules")
}

#[tokio::test]
async fn malformed_unknown_and_oversize_bodies() {
    let app = TestApp::new();
    let path = schedules();

    // Not JSON at all.
    app.post(&path, Some("key-malformed-1"), "{not json")
        .await
        .assert_problem(400, "malformed_request");
    app.post(&path, Some("key-malformed-2"), "")
        .await
        .assert_problem(400, "malformed_request");
    // A lone surrogate is not a valid JSON string for a Rust String.
    app.post(&path, Some("key-malformed-3"), r#"{"schedule":"\ud800"}"#)
        .await
        .assert_problem(400, "malformed_request");

    // Unknown fields, at the top level and nested.
    let mut body = support::schedule_body();
    body["schedulee"] = json!("x");
    let response = app
        .post(&path, Some("key-unknown-1"), &body.to_string())
        .await;
    response.assert_problem(422, "validation_failed");
    assert_eq!(response.json()["errors"][0]["code"], "unknown_field");
    assert_eq!(response.json()["errors"][0]["field"], "schedulee");

    let mut body = support::schedule_body();
    body["archive"]["password"] = json!("hunter2");
    let response = app
        .post(&path, Some("key-unknown-2"), &body.to_string())
        .await;
    response.assert_problem(422, "validation_failed");
    assert!(!String::from_utf8_lossy(&response.body).contains("hunter2"));

    // Missing and mistyped fields.
    let mut body = support::schedule_body();
    body.as_object_mut().unwrap().remove("suspended");
    let response = app
        .post(&path, Some("key-missing-1"), &body.to_string())
        .await;
    response.assert_problem(422, "validation_failed");
    assert_eq!(response.json()["errors"][0]["field"], "suspended");
    let mut body = support::schedule_body();
    body["suspended"] = json!("yes");
    app.post(&path, Some("key-type-1"), &body.to_string())
        .await
        .assert_problem(422, "validation_failed");

    // Oversize: more than 1 MiB.
    let mut body = support::schedule_body();
    body["schedule"] = json!("x".repeat(1024 * 1024 + 1));
    app.post(&path, Some("key-oversize-1"), &body.to_string())
        .await
        .assert_problem(413, "payload_too_large");

    // Semantic validation: several fields at once, each named.
    let body = json!({
        "schedule": "61 * * * *",
        "sourceRef": {"name": "Not_A_Name"},
        "topics": ["orders*", "orders", "orders"],
        "archive": {"url": "s3://key:secret@bucket/x"},
        "retention": {"keepLast": -1},
        "suspended": false
    });
    let response = app
        .post(&path, Some("key-semantic-1"), &body.to_string())
        .await;
    response.assert_problem(422, "validation_failed");
    let fields: BTreeSet<String> = response.json()["errors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["field"].as_str().unwrap().to_string())
        .collect();
    for f in [
        "schedule",
        "sourceRef.name",
        "topics[0]",
        "topics[2]",
        "archive.url",
        "retention.keepLast",
    ] {
        assert!(
            fields.contains(f),
            "missing field error for {f}: {fields:?}"
        );
    }
    assert!(!String::from_utf8_lossy(&response.body).contains("secret@"));

    // Query strings on a create are refused.
    app.post(
        &format!("{path}?dryRun=All"),
        Some("key-query-1"),
        &support::schedule_body().to_string(),
    )
    .await
    .assert_problem(400, "malformed_request");

    assert!(
        app.fake.requests().is_empty(),
        "invalid input reached Kubernetes"
    );
}

#[tokio::test]
async fn idempotency_key_problems() {
    let app = TestApp::new();
    let body = support::schedule_body().to_string();
    app.post(&schedules(), None, &body)
        .await
        .assert_problem(400, "idempotency_key_required");
    app.post(&schedules(), Some("short"), &body)
        .await
        .assert_problem(400, "idempotency_key_invalid");
    app.post(&schedules(), Some(&"k".repeat(129)), &body)
        .await
        .assert_problem(400, "idempotency_key_invalid");
    let response = app
        .send(
            Request::builder()
                .method("POST")
                .uri(schedules())
                .header("host", HOST)
                .header("origin", ORIGIN)
                .header("content-type", "application/json")
                .header("idempotency-key", "key-duplicate-1")
                .header("idempotency-key", "key-duplicate-2")
                .body(Body::from(body.clone()))
                .unwrap(),
        )
        .await;
    response.assert_problem(400, "idempotency_key_invalid");
    // The command route refuses the header rather than silently ignoring it.
    app.post(
        &format!("/api/v1/namespaces/{NS_A}/schedules/nightly:set-suspension"),
        Some("key-not-accepted"),
        r#"{"suspended":true,"expectedResourceVersion":"1"}"#,
    )
    .await
    .assert_problem(400, "idempotency_key_invalid");
    assert!(app.fake.requests().is_empty());
}

#[tokio::test]
async fn namespace_forbidden_is_decided_before_any_lookup() {
    let app = TestApp::new();
    for path in [
        "/api/v1/namespaces/team-c/backups",
        "/api/v1/namespaces/team-c/backups/exists-or-not",
        "/api/v1/namespaces/kube-system/connections",
        "/api/v1/namespaces/default/restores",
        "/api/v1/namespaces/Team-A/restores",
        "/api/v1/namespaces/team-c/operations/backup/x",
        "/api/v1/namespaces/team-c/approvals/x/packet",
    ] {
        app.get(path)
            .await
            .assert_problem(403, "namespace_forbidden");
    }
    app.post(
        "/api/v1/namespaces/team-c/schedules",
        Some("key-forbidden-1"),
        &support::schedule_body().to_string(),
    )
    .await
    .assert_problem(403, "namespace_forbidden");
    assert!(app.fake.requests().is_empty());
}

struct DenyingAuthorizer;

impl Authorizer for DenyingAuthorizer {
    fn namespaces(&self, _actor: &Actor) -> Vec<String> {
        vec![NS_A.to_string()]
    }

    fn allows(&self, _actor: &Actor, _namespace: &str, action: Action) -> bool {
        // A viewer: every read, no write.
        matches!(
            action,
            Action::ReadConnections
                | Action::ReadSchedules
                | Action::ReadBackups
                | Action::ReadRestores
                | Action::ReadApprovals
                | Action::ReadOperations
        )
    }
}

struct RefusingAuthenticator;

impl Authenticator for RefusingAuthenticator {
    fn mode(&self) -> AuthenticationMode {
        AuthenticationMode::LocalAdmin
    }

    fn authenticate(&self, _parts: &Parts) -> Result<Actor, ApiError> {
        Err(ApiError::new(ProblemCode::Unauthenticated, "No session."))
    }
}

/// The seam: a different authorizer or authenticator changes decisions with
/// no route change, and the session's capabilities follow the authorizer.
#[tokio::test]
async fn the_authorization_seam_denies_without_route_changes() {
    let viewer = TestApp::with(
        FakeKube::new(),
        Options {
            authorizer: Some(Arc::new(DenyingAuthorizer)),
            ..Options::default()
        },
    );
    viewer
        .post(
            &schedules(),
            Some("key-viewer-00001"),
            &support::schedule_body().to_string(),
        )
        .await
        .assert_problem(403, "forbidden");
    viewer
        .get(&format!("/api/v1/namespaces/{NS_A}/approvals/x/packet"))
        .await
        .assert_problem(403, "forbidden");
    let session = viewer.get("/api/v1/session").await.json();
    assert_eq!(session["namespaces"].as_array().unwrap().len(), 1);
    let caps = &session["namespaces"][0]["capabilities"];
    assert_eq!(caps["schedulesRead"], true);
    assert_eq!(caps["scheduleCreate"], false);
    assert_eq!(caps["approvalPacketRead"], false);
    assert!(viewer.fake.requests().is_empty());

    let anonymous = TestApp::with(
        FakeKube::new(),
        Options {
            authenticator: Some(Arc::new(RefusingAuthenticator)),
            ..Options::default()
        },
    );
    anonymous
        .get("/api/v1/session")
        .await
        .assert_problem(401, "unauthenticated");
    anonymous
        .get(&format!("/api/v1/namespaces/{NS_A}/backups"))
        .await
        .assert_problem(401, "unauthenticated");
    // The static UI and health do not require an actor.
    assert_eq!(anonymous.get("/ui/").await.status, 200);
    assert_eq!(anonymous.get("/healthz").await.status, 200);
    assert!(anonymous.fake.requests().is_empty());
}

fn fault(method: &'static str, path: &str, status: u16, reason: &'static str) -> Fault {
    Fault {
        method,
        path_contains: path.to_string(),
        status,
        reason,
        delay: None,
        remaining: 1,
    }
}

#[tokio::test]
async fn kubernetes_failures_map_to_stable_codes_without_echoing_messages() {
    let backups = format!("/api/v1/namespaces/{NS_A}/backups");
    for (status, reason, expect_status, expect_code) in [
        (403, "Forbidden", 503, "kubernetes_unavailable"),
        (401, "Unauthorized", 503, "kubernetes_unavailable"),
        (500, "InternalError", 503, "kubernetes_unavailable"),
        (503, "ServiceUnavailable", 503, "kubernetes_unavailable"),
        (429, "TooManyRequests", 429, "rate_limited"),
        (410, "Expired", 410, "cursor_expired"),
        (400, "BadRequest", 422, "validation_failed"),
    ] {
        let app = TestApp::new();
        app.fake.inject(fault("GET", "/backups", status, reason));
        let response = app.get(&backups).await;
        response.assert_problem(expect_status, expect_code);
        let text = String::from_utf8_lossy(&response.body);
        assert!(
            !text.contains("eyJ"),
            "a Kubernetes message was echoed: {text}"
        );
        assert!(
            !text.contains("injected failure"),
            "a Kubernetes message was echoed: {text}"
        );
        if expect_code == "rate_limited" {
            assert_eq!(response.header("retry-after").as_deref(), Some("1"));
            assert_eq!(response.json()["retryable"], true);
        }
    }

    // 422 on create.
    let app = TestApp::new();
    app.fake
        .inject(fault("POST", "/backupschedules", 422, "Invalid"));
    let response = app
        .post(
            &schedules(),
            Some("key-invalid-1"),
            &support::schedule_body().to_string(),
        )
        .await;
    response.assert_problem(422, "validation_failed");
    assert!(!String::from_utf8_lossy(&response.body).contains("injected"));
}

#[tokio::test]
async fn a_kubernetes_call_past_its_deadline_is_upstream_timeout() {
    let app = TestApp::with(
        FakeKube::new(),
        Options {
            deadline: Duration::from_millis(50),
            ..Options::default()
        },
    );
    app.fake.inject(Fault {
        method: "GET",
        path_contains: "/restores".into(),
        status: 200,
        reason: "",
        delay: Some(Duration::from_millis(500)),
        remaining: 1,
    });
    let response = app
        .get(&format!("/api/v1/namespaces/{NS_A}/restores"))
        .await;
    response.assert_problem(504, "upstream_timeout");
    assert_eq!(response.json()["retryable"], true);
}

#[test]
fn the_production_deadline_is_ten_seconds() {
    assert_eq!(logweir_api::kube::KUBE_DEADLINE, Duration::from_secs(10));
}

#[tokio::test]
async fn cursor_and_precondition_and_conflict_codes() {
    let app = TestApp::new();
    let backups = format!("/api/v1/namespaces/{NS_A}/backups");
    app.get(&format!("{backups}?cursor=forged.cursor"))
        .await
        .assert_problem(400, "cursor_invalid");
    app.get(&format!("{backups}?limit=0"))
        .await
        .assert_problem(422, "validation_failed");
    app.get(&format!("{backups}?watch=true"))
        .await
        .assert_problem(400, "malformed_request");

    app.fake.seed(
        "backupschedules",
        NS_A,
        json!({"metadata": {"name": "nightly"}, "spec": {"schedule": "0 3 * * *", "sourceRef": {"name": "source"}, "topics": ["orders"], "archive": {"url": "s3://b/p"}, "suspend": false}}),
    );
    app.post(
        &format!("/api/v1/namespaces/{NS_A}/schedules/nightly:set-suspension"),
        None,
        r#"{"suspended":true,"expectedResourceVersion":"1"}"#,
    )
    .await
    .assert_problem(412, "precondition_failed");
    app.fake.assert_strict();
}

#[test]
fn every_code_renders_as_a_complete_problem_document() {
    for code in ProblemCode::ALL {
        let response = problem::render(
            &ApiError::new(code, "detail sentence"),
            "01TESTREQUESTID0000000000",
        );
        assert_eq!(response.status(), code.status());
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "application/problem+json"
        );
        let doc = ApiError::new(code, "detail sentence").to_problem("01TESTREQUESTID0000000000");
        let value = serde_json::to_value(&doc).unwrap();
        assert_eq!(value["code"], code.as_str());
        assert_eq!(value["status"], code.status().as_u16());
        assert_eq!(value["requestId"], "01TESTREQUESTID0000000000");
        assert_eq!(
            value["type"],
            format!(
                "https://logweir.dev/problems/{}",
                code.as_str().replace('_', "-")
            )
        );
        assert!(value.get("errors").is_none(), "no empty errors array");
    }
    // The decision's baseline statuses, pinned.
    for (code, status) in [
        (ProblemCode::Unauthenticated, 401),
        (ProblemCode::SessionExpired, 401),
        (ProblemCode::Forbidden, 403),
        (ProblemCode::NamespaceForbidden, 403),
        (ProblemCode::NotFound, 404),
        (ProblemCode::IdempotencyConflict, 409),
        (ProblemCode::StateConflict, 409),
        (ProblemCode::ApprovalRequired, 409),
        (ProblemCode::PolicyMismatch, 409),
        (ProblemCode::PreconditionFailed, 412),
        (ProblemCode::ValidationFailed, 422),
        (ProblemCode::RateLimited, 429),
        (ProblemCode::KubernetesUnavailable, 503),
        (ProblemCode::UpstreamTimeout, 504),
        (ProblemCode::CursorInvalid, 400),
        (ProblemCode::CursorExpired, 410),
    ] {
        assert_eq!(code.status().as_u16(), status, "{}", code.as_str());
    }
}

/// Every code is produced by some test in this crate, or is explicitly
/// reserved for a later stage. The produced set is written out here; each
/// entry names the test that produces it.
#[test]
fn every_code_is_produced_or_reserved() {
    let produced: BTreeSet<&str> = [
        "malformed_request",        // malformed_unknown_and_oversize_bodies
        "header_not_allowed",       // boundary::impersonation_headers_are_refused_on_every_route
        "idempotency_key_required", // idempotency_key_problems
        "idempotency_key_invalid",  // idempotency_key_problems
        "cursor_invalid",           // cursor_and_precondition_and_conflict_codes
        "unauthenticated",          // the_authorization_seam_denies_without_route_changes
        "forbidden",                // the_authorization_seam_denies_without_route_changes
        "namespace_forbidden",      // namespace_forbidden_is_decided_before_any_lookup
        "origin_mismatch", // boundary::unsafe_methods_need_the_exact_origin_and_json_before_anything_else
        "not_found", // boundary::kubernetes_shaped_and_unlisted_paths_are_404_and_reach_nothing
        "method_not_allowed", // boundary::unimplemented_mutations_have_no_route
        "idempotency_conflict", // idempotency::a_different_request_with_the_same_key_is_a_conflict
        "state_conflict", // idempotency::a_foreign_object_under_the_name_is_never_adopted
        "cursor_expired", // pagination::an_expired_cursor_is_410
        "precondition_failed", // cursor_and_precondition_and_conflict_codes
        "payload_too_large", // malformed_unknown_and_oversize_bodies
        "unsupported_media_type", // boundary::unsafe_methods_need_the_exact_origin_and_json_before_anything_else
        "misdirected_request",    // boundary::a_host_this_listener_does_not_serve_is_refused
        "validation_failed",      // malformed_unknown_and_oversize_bodies
        "rate_limited", // kubernetes_failures_map_to_stable_codes_without_echoing_messages
        "kubernetes_unavailable", // kubernetes_failures_map_to_stable_codes_without_echoing_messages
        "upstream_timeout",       // a_kubernetes_call_past_its_deadline_is_upstream_timeout
        "internal_error",         // render-only: an invariant failure has no honest trigger
        // D2 W12.
        "destination_invalid", // destinations::the_rules_are_evaluated_before_the_object_exists
        "destination_location_immutable", // a_rejected_rotation_is_reported_as_an_immutable_location
        "transport_downgrade_forbidden", // destinations::update_access_refuses_an_idempotency_key_and_a_ca_bundle_on_plaintext
        "legacy_location_unknown", // destinations::a_legacy_schedule_becomes_a_destination_from_facts_or_is_refused
        "result_integrity_failed", // topic_discoveries::a_chunk_that_fails_its_integrity_check_refuses_the_page
        // D1 W6.
        "policy_changed", // manual_backups::an_expected_generation_that_moved_is_policy_changed
    ]
    .into_iter()
    .collect();
    for code in ProblemCode::ALL {
        let reserved = RESERVED_FOR_LATER_STAGES.contains(&code);
        assert!(
            produced.contains(code.as_str()) != reserved,
            "{} must be either produced by a test or reserved, and not both",
            code.as_str()
        );
    }
}

/// **A Kubernetes refusal of a rotation is reported as what it is.**
///
/// `spec.storage` and `spec.transport.security` are immutable, and the CRD's
/// own CEL is what finally refuses a change to either. The API cannot build a
/// patch that names them — `set_destination_access` has no key for them — so
/// this drives the other half: when the API server rejects the patch as
/// invalid, the answer is `destination_location_immutable` and NOT a generic
/// validation failure, because the operator's next action is different.
#[tokio::test]
async fn a_rejected_rotation_is_reported_as_an_immutable_location() {
    let app = TestApp::new();
    support::seed_destination(&app.fake, support::NS_A, "primary");
    app.fake.inject(support::Fault {
        method: "PATCH",
        path_contains: "/backupdestinations/primary".to_string(),
        status: 422,
        reason: "Invalid",
        delay: None,
        remaining: 1,
    });
    let response = app
        .post(
            &format!(
                "/api/v1/namespaces/{}/destinations/primary:update-access",
                support::NS_A
            ),
            None,
            &serde_json::json!({
                "expectedGeneration": 3,
                "access": {"archiveWrite": {"mode": "secretKeys", "secret": {"existing": {"name": "s"}}}}
            })
            .to_string(),
        )
        .await;
    response.assert_problem(409, "destination_location_immutable");
    // The injected Kubernetes message carries a token-shaped string; none of
    // it is echoed.
    assert!(!response.text().contains("eyJhbGciOiJIUzI1NiJ9"));
}
