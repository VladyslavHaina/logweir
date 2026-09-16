//! Durable-create idempotency with Kubernetes as the only store.
//!
//! 201 → replay 200 with the same UID → different body 409
//! `idempotency_conflict` → foreign object 409 `state_conflict` (never
//! adopted) → a NEW router instance over the same cluster (a restart) replays
//! the same object → two concurrent identical requests create one object.

mod support;

use std::sync::Arc;

use logweir_api::auth::LocalAdminAuthenticator;
use logweir_api::idempotency::{
    ANNOTATION_ACTOR, ANNOTATION_REQUEST, ANNOTATION_REQUEST_ID, ANNOTATION_SCOPE,
};
use serde_json::{json, Value};
use support::{FakeKube, Options, TestApp, NS_A, NS_B};

fn path(ns: &str, resource: &str) -> String {
    format!("/api/v1/namespaces/{ns}/{resource}")
}

fn posts(app: &TestApp) -> Vec<support::Recorded> {
    app.fake
        .requests()
        .into_iter()
        .filter(|r| r.method == "POST")
        .collect()
}

#[tokio::test]
async fn create_then_replay_returns_the_same_object() {
    let app = TestApp::new();
    let body = support::schedule_body().to_string();
    let key = "schedule-key-0001";

    let first = app.post(&path(NS_A, "schedules"), Some(key), &body).await;
    assert_eq!(
        first.status,
        201,
        "{}",
        String::from_utf8_lossy(&first.body)
    );
    let item = first.json()["item"].clone();
    assert_eq!(first.json()["replayed"], false);
    let name = item["name"].as_str().unwrap().to_string();
    let uid = item["uid"].as_str().unwrap().to_string();
    assert!(name.starts_with("sch-") && name.len() == 30, "{name}");
    assert!(
        name.len() <= 32,
        "the schedule-name budget is 32 characters"
    );

    let replay = app.post(&path(NS_A, "schedules"), Some(key), &body).await;
    assert_eq!(replay.status, 200);
    assert_eq!(replay.json()["replayed"], true);
    assert_eq!(replay.json()["item"]["uid"], uid.as_str());
    assert_eq!(replay.json()["item"]["name"], name.as_str());
    assert_eq!(app.fake.count("backupschedules", NS_A), 1);

    // Both attempts POSTed the identical name: Kubernetes' AlreadyExists is the
    // idempotency guard, not process memory.
    let names: Vec<String> = posts(&app)
        .iter()
        .map(|r| {
            serde_json::from_str::<Value>(&r.body).unwrap()["metadata"]["name"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(names, vec![name.clone(), name.clone()]);

    // The stored object carries hashes, the request ID and the actor — and
    // never the raw key.
    let stored = app.fake.object("backupschedules", NS_A, &name).unwrap();
    let annotations = stored["metadata"]["annotations"].as_object().unwrap();
    for a in [ANNOTATION_SCOPE, ANNOTATION_REQUEST] {
        let v = annotations[a].as_str().unwrap();
        assert!(v.starts_with("sha256:") && v.len() == 71, "{a}={v}");
    }
    assert_eq!(
        annotations[ANNOTATION_REQUEST_ID],
        first.json()["requestId"]
    );
    assert_eq!(
        annotations[ANNOTATION_ACTOR],
        "urn:logweir:local-admin#admin"
    );
    assert!(
        !stored.to_string().contains(key),
        "the raw Idempotency-Key was stored"
    );
    // The projection does not expose annotations.
    assert!(!first
        .body
        .windows(ANNOTATION_SCOPE.len())
        .any(|w| w == ANNOTATION_SCOPE.as_bytes()));
    app.fake.assert_strict();
}

#[tokio::test]
async fn a_different_request_with_the_same_key_is_a_conflict() {
    let app = TestApp::new();
    let key = "schedule-key-0002";
    assert_eq!(
        app.post(
            &path(NS_A, "schedules"),
            Some(key),
            &support::schedule_body().to_string()
        )
        .await
        .status,
        201
    );
    let mut changed = support::schedule_body();
    changed["topics"] = json!(["orders"]);
    let response = app
        .post(&path(NS_A, "schedules"), Some(key), &changed.to_string())
        .await;
    response.assert_problem(409, "idempotency_conflict");
    assert_eq!(app.fake.count("backupschedules", NS_A), 1);
    // The stored object is unchanged.
    let stored = app.fake.requests();
    assert!(stored
        .iter()
        .all(|r| r.method != "PUT" && r.method != "PATCH" && r.method != "DELETE"));
}

#[tokio::test]
async fn a_foreign_object_under_the_name_is_never_adopted() {
    let app = TestApp::new();
    let key = "schedule-key-0003";
    let first = app
        .post(
            &path(NS_A, "schedules"),
            Some(key),
            &support::schedule_body().to_string(),
        )
        .await;
    let name = first.json()["item"]["name"].as_str().unwrap().to_string();

    // Another cluster: an object with the SAME deterministic name, created by
    // someone else (no annotations, then someone else's annotations).
    for annotations in [
        None,
        Some(json!({ANNOTATION_SCOPE: "sha256:00", ANNOTATION_REQUEST: "sha256:00"})),
    ] {
        let other = TestApp::new();
        let mut metadata = json!({"name": name});
        if let Some(a) = annotations {
            metadata["annotations"] = a;
        }
        other.fake.seed(
            "backupschedules",
            NS_A,
            json!({"metadata": metadata, "spec": {"schedule": "0 3 * * *", "sourceRef": {"name": "source"}, "topics": ["orders", "payments"], "archive": {"url": "s3://kafka-backups/orders"}, "suspend": true}}),
        );
        let response = other
            .post(
                &path(NS_A, "schedules"),
                Some(key),
                &support::schedule_body().to_string(),
            )
            .await;
        response.assert_problem(409, "state_conflict");
        // Never adopted: nothing was written to it.
        assert!(other
            .fake
            .requests()
            .iter()
            .all(|r| r.method == "POST" || r.method == "GET"));
        other.fake.assert_strict();
    }
}

#[tokio::test]
async fn a_restarted_api_replays_from_the_cluster() {
    let app = TestApp::new();
    let body = serde_json::to_string(&support::connection_body()).unwrap();
    let key = "connection-key-01";
    let first = app.post(&path(NS_A, "connections"), Some(key), &body).await;
    assert_eq!(first.status, 201);
    let uid = first.json()["item"]["uid"].clone();

    // A new router, new AppState, new adapter: nothing shared but the cluster.
    let restarted = app.restart();
    let replay = restarted
        .post(&path(NS_A, "connections"), Some(key), &body)
        .await;
    assert_eq!(replay.status, 200);
    assert_eq!(replay.json()["item"]["uid"], uid);
    assert_eq!(app.fake.count("kafkaclusters", NS_A), 1);

    // A deliberate new operation uses a new key and gets a new object.
    let second = restarted
        .post(&path(NS_A, "connections"), Some("connection-key-02"), &body)
        .await;
    assert_eq!(second.status, 201);
    assert_ne!(second.json()["item"]["uid"], uid);
    assert_eq!(app.fake.count("kafkaclusters", NS_A), 2);
    app.fake.assert_strict();
}

#[tokio::test]
async fn the_scope_includes_actor_namespace_and_route() {
    let fake = FakeKube::new();
    let admin = TestApp::with(fake.clone(), Options::default());
    let other_actor = TestApp::with(
        fake.clone(),
        Options {
            authenticator: Some(Arc::new(LocalAdminAuthenticator::new(
                "second-admin",
                "Second",
            ))),
            ..Options::default()
        },
    );
    let key = "shared-key-00001";
    let schedule = support::schedule_body().to_string();

    let a = admin
        .post(&path(NS_A, "schedules"), Some(key), &schedule)
        .await;
    let b = other_actor
        .post(&path(NS_A, "schedules"), Some(key), &schedule)
        .await;
    let c = admin
        .post(&path(NS_B, "schedules"), Some(key), &schedule)
        .await;
    let d = admin
        .post(
            &path(NS_A, "connections"),
            Some(key),
            &support::connection_body().to_string(),
        )
        .await;
    for r in [&a, &b, &c, &d] {
        assert_eq!(r.status, 201, "{}", String::from_utf8_lossy(&r.body));
    }
    let names: std::collections::BTreeSet<String> = [&a, &b, &c, &d]
        .iter()
        .map(|r| r.json()["item"]["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(names.len(), 4, "one key in four scopes is four objects");
    fake.assert_strict();
}

#[tokio::test]
async fn equivalent_spellings_of_one_request_replay() {
    let app = TestApp::new();
    let key = "schedule-key-0004";
    let explicit = {
        let mut b = support::schedule_body();
        b["concurrencyPolicy"] = json!("Forbid");
        b
    };
    assert_eq!(
        app.post(
            &path(NS_A, "schedules"),
            Some(key),
            &support::schedule_body().to_string()
        )
        .await
        .status,
        201
    );
    // Omitted policy and explicit `Forbid` are one request.
    assert_eq!(
        app.post(&path(NS_A, "schedules"), Some(key), &explicit.to_string())
            .await
            .status,
        200
    );
    // Key order and whitespace do not matter.
    let reordered = r#"{ "suspended": true, "topics": ["orders","payments"], "archive": {"credentialRef": {"name": "archive-credentials"}, "url": "s3://kafka-backups/orders"}, "sourceRef": {"name": "source"}, "schedule": "0 3 * * *" }"#;
    assert_eq!(
        app.post(&path(NS_A, "schedules"), Some(key), reordered)
            .await
            .status,
        200
    );

    // Restores: two RFC 3339 spellings of one instant are one request; the
    // plan bytes are not canonicalised.
    let plan = support::golden_plan();
    let rkey = "restore-key-00001";
    let mut body = support::restore_body(&plan);
    assert_eq!(
        app.post(&path(NS_A, "restores"), Some(rkey), &body.to_string())
            .await
            .status,
        201
    );
    body["pointInTime"] = json!("2026-09-07T16:05:00+02:00");
    assert_eq!(
        app.post(&path(NS_A, "restores"), Some(rkey), &body.to_string())
            .await
            .status,
        200
    );
    let mut changed = support::restore_body(&format!("{plan} "));
    changed["pointInTime"] = json!("2026-09-07T14:05:00Z");
    app.post(&path(NS_A, "restores"), Some(rkey), &changed.to_string())
        .await
        .assert_problem(409, "idempotency_conflict");
    app.fake.assert_strict();
}

#[tokio::test]
async fn concurrent_identical_requests_create_one_object() {
    let app = Arc::new(TestApp::new());
    let body = support::schedule_body().to_string();
    let mut handles = Vec::new();
    for _ in 0..8 {
        let app = Arc::clone(&app);
        let body = body.clone();
        handles.push(tokio::spawn(async move {
            app.post(&path(NS_A, "schedules"), Some("concurrent-key-01"), &body)
                .await
        }));
    }
    let mut statuses = Vec::new();
    let mut uids = std::collections::BTreeSet::new();
    for h in handles {
        let r = h.await.unwrap();
        statuses.push(r.status.as_u16());
        uids.insert(r.json()["item"]["uid"].as_str().unwrap().to_string());
    }
    statuses.sort_unstable();
    assert_eq!(
        statuses.iter().filter(|s| **s == 201).count(),
        1,
        "{statuses:?}"
    );
    assert_eq!(
        statuses.iter().filter(|s| **s == 200).count(),
        7,
        "{statuses:?}"
    );
    assert_eq!(uids.len(), 1);
    assert_eq!(app.fake.count("backupschedules", NS_A), 1);
    app.fake.assert_strict();
}

#[tokio::test]
async fn an_object_deleted_between_conflict_and_read_is_created_again() {
    let app = TestApp::new();
    let key = "schedule-key-race1";
    let body = support::schedule_body().to_string();
    // First create, then inject: the next POST answers AlreadyExists but the
    // GET finds nothing (the object was deleted in between).
    let first = app.post(&path(NS_A, "schedules"), Some(key), &body).await;
    let name = first.json()["item"]["name"].as_str().unwrap().to_string();
    let other = TestApp::new();
    other.fake.inject(support::Fault {
        method: "POST",
        path_contains: "/backupschedules".into(),
        status: 409,
        reason: "AlreadyExists",
        delay: None,
        remaining: 1,
    });
    let response = other.post(&path(NS_A, "schedules"), Some(key), &body).await;
    assert_eq!(
        response.status,
        201,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    assert_eq!(response.json()["item"]["name"], name.as_str());
    let methods: Vec<String> = other
        .fake
        .requests()
        .iter()
        .map(|r| r.method.clone())
        .collect();
    assert_eq!(methods, vec!["POST", "GET", "POST"]);
    other.fake.assert_strict();
}
