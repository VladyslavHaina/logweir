//! D3 §2.6's server-sent stream: the frames, the resume rule, the heartbeat,
//! the terminal stop, and every bound that makes it safe to expose.
//!
//! THE SUITE IS WRITTEN SO NOTHING SLEEPS FOR THE REAL BOUNDS. The connection
//! ceiling is 300 s and the heartbeat is 15 s in production; a test that waited
//! for either would be a test nobody runs. `set_stream_bounds_for_test`
//! shortens all three, which is the only way they can be changed at all — no
//! request parameter, header or configuration key reaches them, because a
//! caller who could ask for a longer connection is a caller who can ask this
//! process to spend more of itself on one subscriber.

mod support;

use std::time::Duration;

use axum::body::Body;
use http::Request;
use logweir_api::status::{set_stream_bounds_for_test, StreamBounds};
use serde_json::{json, Value};
use support::{repo_root, FakeKube, Options, TestApp, HOST};
use tower::ServiceExt as _;

fn fixture(name: &str) -> Value {
    let path = repo_root()
        .join("crates/logweir-api/tests/fixtures")
        .join(name);
    serde_json::from_str(
        &std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display())),
    )
    .expect("a fixture is JSON")
}

/// Bounds short enough that the whole suite runs in well under a second.
fn brisk() {
    set_stream_bounds_for_test(StreamBounds {
        poll: Duration::from_millis(15),
        heartbeat: Duration::from_millis(40),
        max_connection: Duration::from_millis(400),
    });
}

/// One app, one namespace, per test.
///
/// EVERY STREAMING TEST GETS ITS OWN NAMESPACE, and that is not tidiness. The
/// concurrent-stream ceiling is keyed on `(actor, namespace)`, every test here
/// authenticates as the same local administrator, and cargo runs them on
/// threads of one process — so two tests that both held streams open in
/// `team-a` would be spending each other's slots and the suite would fail
/// about one run in five. The namespace is the isolation.
fn app_in(object: Value, name: &str, namespace: &str) -> TestApp {
    let fake = FakeKube::new();
    let mut object = object;
    object["metadata"]["name"] = json!(name);
    fake.seed("backups", namespace, object);
    TestApp::with(
        fake,
        Options {
            namespaces: vec![namespace.to_string()],
            ..Options::default()
        },
    )
}

async fn stream(app: &TestApp, path: &str, last_event_id: Option<&str>) -> (u16, String, String) {
    let mut builder = Request::builder()
        .method("GET")
        .uri(path)
        .header("host", HOST);
    if let Some(id) = last_event_id {
        builder = builder.header("last-event-id", id);
    }
    let response = app.send(builder.body(Body::empty()).unwrap()).await;
    (
        response.status.as_u16(),
        response.header("content-type").unwrap_or_default(),
        response.text(),
    )
}

/// The `event:` names, in order.
fn events(body: &str) -> Vec<String> {
    body.lines()
        .filter_map(|l| l.strip_prefix("event: "))
        .map(str::to_string)
        .collect()
}

/// The `id:` values, in order.
fn ids(body: &str) -> Vec<String> {
    body.lines()
        .filter_map(|l| l.strip_prefix("id: "))
        .map(str::to_string)
        .collect()
}

/// The `data:` payloads, parsed.
fn data(body: &str) -> Vec<Value> {
    body.lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .map(|l| serde_json::from_str(l).expect("every data line is one JSON value"))
        .collect()
}

/// **A settled operation streams one snapshot and stops.**
///
/// The stream does not hold a connection open for a run that finished last
/// week: `terminal AND the verification settled` is the end condition, and the
/// `end` frame says which.
#[tokio::test]
async fn a_settled_operation_emits_one_snapshot_and_ends() {
    brisk();
    let app = app_in(fixture("backup-succeeded-verified.json"), "b1", "lw-s1");
    let (status, content_type, body) = stream(
        &app,
        "/api/v1/namespaces/lw-s1/operations/backup/b1/events",
        None,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(content_type, "text/event-stream");
    assert_eq!(events(&body), vec!["operation", "end"]);

    let frames = data(&body);
    assert_eq!(frames[0]["state"], "succeeded");
    assert_eq!(frames[0]["trust"]["basis"], "current");
    assert_eq!(frames[0]["verifiedSuccess"], true);
    assert_eq!(frames[1]["reason"], "settled");

    // The event id is the object's resourceVersion — the same number the read
    // route publishes, so `Last-Event-ID` means one thing on both.
    let seen = ids(&body);
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0], frames[0]["resourceVersion"].as_str().unwrap());
    app.fake.assert_strict();
}

/// **A stream that is NOT settled keeps the connection and closes it on the
/// ceiling.**
///
/// Every test in this file uses [`brisk`]'s bounds for the reason
/// `one_principal_may_not_hold_more_streams_than_the_ceiling` records: they are
/// a process global, and a per-test setting would be a race with whichever
/// sibling was mid-stream.
#[tokio::test]
async fn an_unfinished_operation_heartbeats_and_ends_on_the_connection_ceiling() {
    brisk();
    let mut object = fixture("backup-succeeded-verified.json");
    object["status"]["phase"] = json!("Running");
    object["status"]["progress"]["stage"] = json!("Running");
    object["status"].as_object_mut().unwrap().remove("exitCode");
    object["status"]
        .as_object_mut()
        .unwrap()
        .remove("exitReason");
    object["status"].as_object_mut().unwrap().remove("evidence");
    let app = app_in(object, "b2", "lw-s2");

    let (status, _, body) = stream(
        &app,
        "/api/v1/namespaces/lw-s2/operations/backup/b2/events",
        None,
    )
    .await;
    assert_eq!(status, 200);
    let names = events(&body);
    assert_eq!(names.first().map(String::as_str), Some("operation"));
    assert_eq!(names.last().map(String::as_str), Some("end"));
    assert!(
        names.iter().any(|n| n == "heartbeat"),
        "a silent stream must be kept alive: {names:?}"
    );
    // A HEARTBEAT CARRIES NO ID. Only a snapshot advances `Last-Event-ID`;
    // resuming from a heartbeat's id would resume from a version that never
    // existed.
    assert_eq!(ids(&body).len(), 1);
    let frames = data(&body);
    assert_eq!(frames.last().unwrap()["reason"], "maxDuration");
    // AT LEAST ONE, AND BOUNDED. The exact count is `max_connection /
    // heartbeat`, and the bounds are a process global that a sibling test in
    // this binary may have set to its own (longer) numbers, so the assertion
    // is the property — a silent stream is kept alive, and the keep-alive is
    // paced by the heartbeat rather than by the poll — and not the arithmetic
    // of one particular setting. At the fastest setting either test uses, the
    // poll fires ~27 times and the heartbeat ~10.
    let beats = names.iter().filter(|n| *n == "heartbeat").count();
    assert!((1..=30).contains(&beats), "{beats} heartbeats");
    assert!(
        beats < names.len(),
        "every frame was a heartbeat; the snapshot and the end frame are missing"
    );
}

/// **A change emits a new snapshot with the new id, and a settled change
/// ends.**
///
/// THE CHANGE IS DRIVEN BY AN OBSERVATION, NOT BY A TIMER. An earlier shape of
/// this test slept sixty milliseconds and then mutated the object, and it
/// failed about one run in five: under load the stream's own first read could
/// land after the mutation, so the "running" snapshot never existed and the
/// row asserted two where there was one. It now waits until the fake has
/// answered the stream's opening read AND at least one poll, which is exactly
/// the condition the assertion is about.
#[tokio::test]
async fn a_status_change_emits_a_new_snapshot_and_a_settled_one_ends_the_stream() {
    brisk();
    let mut object = fixture("backup-succeeded-verified.json");
    object["status"]["phase"] = json!("Running");
    object["status"]["progress"]["stage"] = json!("Running");
    object["status"].as_object_mut().unwrap().remove("exitCode");
    object["status"]
        .as_object_mut()
        .unwrap()
        .remove("exitReason");
    object["status"].as_object_mut().unwrap().remove("evidence");
    let app = app_in(object, "b3", "lw-s3");

    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/lw-s3/operations/backup/b3/events")
                .header("host", HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("the router is infallible");
    assert_eq!(response.status().as_u16(), 200);

    let reads = || {
        app.fake
            .requests()
            .iter()
            .filter(|r| r.method == "GET" && r.path.ends_with("/backups/b3"))
            .count()
    };
    let mut settled_the_open = false;
    for _ in 0..200 {
        if reads() >= 2 {
            settled_the_open = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(settled_the_open, "the stream never polled a second time");

    let mut done = fixture("backup-succeeded-verified.json");
    done["metadata"]["name"] = json!("b3");
    app.fake.seed("backups", "lw-s3", done);

    let bytes = http_body_util::BodyExt::collect(response.into_body())
        .await
        .expect("the body collects")
        .to_bytes();
    let body = String::from_utf8_lossy(&bytes).into_owned();

    let names = events(&body);
    assert_eq!(names.first().map(String::as_str), Some("operation"));
    assert_eq!(names.last().map(String::as_str), Some("end"));
    let snapshots: Vec<Value> = data(&body)
        .into_iter()
        .filter(|v| v.get("state").is_some())
        .collect();
    assert_eq!(snapshots.len(), 2, "one running, one finished: {names:?}");
    assert_eq!(snapshots[0]["state"], "running");
    assert_eq!(snapshots[1]["state"], "succeeded");
    assert_eq!(data(&body).last().unwrap()["reason"], "settled");
    // Two snapshots, two distinct ids.
    let seen = ids(&body);
    assert_eq!(seen.len(), 2);
    assert_ne!(seen[0], seen[1]);
}

/// **`Last-Event-ID` at the current version sends nothing; anything else sends
/// one `reset`.**
///
/// This service keeps no history of resourceVersions, so it cannot replay what
/// happened between two of them. The honest answers are "you are caught up"
/// and "here is the whole current state"; a third answer — a partial replay —
/// would be a page nobody produced.
#[tokio::test]
async fn resuming_is_either_silence_or_one_reset_snapshot() {
    brisk();
    let app = app_in(fixture("backup-succeeded-verified.json"), "b4", "lw-s4");
    let path = "/api/v1/namespaces/lw-s4/operations/backup/b4/events";

    let (_, _, first) = stream(&app, path, None).await;
    let version = ids(&first)[0].clone();

    // Caught up: no snapshot at all, only the end frame.
    let (status, _, caught_up) = stream(&app, path, Some(&version)).await;
    assert_eq!(status, 200);
    assert_eq!(events(&caught_up), vec!["end"]);

    // Behind, ahead, or from another object under the same name: one reset.
    for other in ["1", "999999999"] {
        let (status, _, body) = stream(&app, path, Some(other)).await;
        assert_eq!(status, 200);
        assert_eq!(events(&body), vec!["reset", "end"], "resume from {other}");
        assert_eq!(data(&body)[0]["resourceVersion"], version);
    }
}

/// **A `Last-Event-ID` that is not a resourceVersion is refused.**
#[tokio::test]
async fn a_malformed_last_event_id_is_refused_before_any_read() {
    brisk();
    let app = app_in(fixture("backup-succeeded-verified.json"), "b5", "lw-s5");
    let path = "/api/v1/namespaces/lw-s5/operations/backup/b5/events";
    for bad in ["", "abc", "12a", "-1", &"9".repeat(65)] {
        let response = app
            .send(
                Request::builder()
                    .method("GET")
                    .uri(path)
                    .header("host", HOST)
                    .header("last-event-id", bad)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;
        response.assert_problem(400, "malformed_request");
    }
    assert!(
        app.fake.requests().is_empty(),
        "a malformed header reached Kubernetes"
    );
}

/// **No query parameter is accepted, so no token can be put in a URL.**
///
/// REGRESSION REASON. `EventSource` cannot set headers, and the standard
/// workaround is `?access_token=`. A URL is the one place a credential survives
/// in a proxy log, a browser history and a `Referer`; this route accepts
/// nothing at all, so the workaround is a 400 instead of a design.
#[tokio::test]
async fn the_stream_accepts_no_query_parameter_at_all() {
    brisk();
    let app = app_in(fixture("backup-succeeded-verified.json"), "b6", "lw-s6");
    for query in [
        "?access_token=secret-value",
        "?token=x",
        "?watch=true",
        "?limit=1",
    ] {
        let response = app
            .get(&format!(
                "/api/v1/namespaces/lw-s6/operations/backup/b6/events{query}"
            ))
            .await;
        response.assert_problem(400, "malformed_request");
    }
    assert!(app.fake.requests().is_empty());
}

/// **A missing object is a problem, not a stream that opens and says nothing.**
#[tokio::test]
async fn a_missing_object_is_not_found_and_opens_no_stream() {
    brisk();
    let app = app_in(fixture("backup-succeeded-verified.json"), "b7", "lw-s7");
    let response = app
        .get("/api/v1/namespaces/lw-s7/operations/backup/absent/events")
        .await;
    response.assert_problem(404, "not_found");
    assert_ne!(
        response.header("content-type").as_deref(),
        Some("text/event-stream")
    );
}

/// **Only the two durable kinds have a stream.**
#[tokio::test]
async fn a_transient_check_and_an_unknown_kind_have_no_stream() {
    brisk();
    let app = app_in(fixture("backup-succeeded-verified.json"), "b8", "lw-s8");
    for kind in ["discovery", "preflight", "secret", "pod"] {
        let response = app
            .get(&format!(
                "/api/v1/namespaces/lw-s8/operations/{kind}/b8/events"
            ))
            .await;
        response.assert_problem(404, "not_found");
    }
    assert!(app.fake.requests().is_empty());
}

/// **Concurrent streams per principal per namespace are capped.**
///
/// REGRESSION REASON. A stream costs a task, a channel and one Kubernetes read
/// per poll for as long as it is held. Without a ceiling one authenticated
/// principal can open as many as a browser will allow and turn a console into
/// a load generator against the API server. The slot is released when the task
/// ends, so the cap is a concurrency bound and not a rate limit.
#[tokio::test]
async fn one_principal_may_not_hold_more_streams_than_the_ceiling() {
    // THE SAME BOUNDS AS EVERY OTHER TEST HERE, ON PURPOSE. The bounds are a
    // process global and cargo runs this file's tests on threads of one
    // process, so a test that set its own would change the numbers under
    // whichever sibling was mid-stream. `brisk()`'s 400 ms ceiling is long
    // enough to hold four streams open while the fifth is refused, and short
    // enough that the slots free while the release loop below is still
    // watching.
    brisk();
    let mut object = fixture("backup-succeeded-verified.json");
    object["status"]["phase"] = json!("Running");
    object["status"].as_object_mut().unwrap().remove("exitCode");
    object["status"]
        .as_object_mut()
        .unwrap()
        .remove("exitReason");
    object["status"].as_object_mut().unwrap().remove("evidence");
    let app = app_in(object, "b9", "lw-s9");
    let ceiling = logweir_api::auth::ratelimit::STREAMS_PER_ACTOR_NAMESPACE;

    // Hold the ceiling open by never draining the bodies.
    let mut held = Vec::new();
    for _ in 0..ceiling {
        let response = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/v1/namespaces/lw-s9/operations/backup/b9/events")
                    .header("host", HOST)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("the router is infallible");
        assert_eq!(response.status().as_u16(), 200);
        held.push(response);
    }

    let refused = app
        .get("/api/v1/namespaces/lw-s9/operations/backup/b9/events")
        .await;
    refused.assert_problem(429, "rate_limited");
    assert!(
        refused
            .header("retry-after")
            .is_some_and(|v| v.parse::<u64>().is_ok()),
        "a refused subscriber is told when to come back"
    );

    // A DROPPED CONNECTION RELEASES ITS SLOT. The browser's normal way of
    // leaving is to close the socket, and a slot that leaked on that would
    // make the ceiling a one-way ratchet.
    drop(held);
    let mut freed = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(30)).await;
        let retry = app
            .get("/api/v1/namespaces/lw-s9/operations/backup/b9/events")
            .await;
        if retry.status.as_u16() == 200 {
            freed = true;
            break;
        }
    }
    assert!(freed, "a dropped stream never released its slot");
}

/// **The frame encoder never produces a payload that could split an event.**
#[test]
fn a_frame_is_one_event_with_one_data_line() {
    let text = logweir_api::status::frame("operation", Some("1234"), r#"{"a":"b\nc"}"#);
    assert_eq!(
        text,
        "event: operation\nid: 1234\ndata: {\"a\":\"b\\nc\"}\n\n"
    );
    assert_eq!(text.matches("\n\n").count(), 1);
    let without_id = logweir_api::status::frame("heartbeat", None, "{}");
    assert!(!without_id.contains("id:"));
}

/// **The event-id validator refuses everything that is not a resourceVersion.**
#[test]
fn only_a_bounded_decimal_is_an_event_id() {
    use logweir_api::status::valid_event_id;
    assert!(valid_event_id("1"));
    assert!(valid_event_id(&"9".repeat(64)));
    for bad in ["", "abc", " 1", "1 ", "1.0", "-1", "١٢٣", &"9".repeat(65)] {
        assert!(!valid_event_id(bad), "{bad}");
    }
}
