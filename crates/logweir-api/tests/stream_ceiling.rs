//! The connection ceiling is WALL CLOCK from subscribe, even for a client that
//! never reads (review finding F3).
//!
//! ITS OWN TEST BINARY, ON PURPOSE. `StreamBounds` is a process global and
//! `operation_stream.rs` shares one setting across its file so its rows cannot
//! race each other; this row needs a different setting — a heartbeat fast
//! enough to fill the eight-frame channel well before the deadline, so the
//! producer is genuinely parked in `send` for most of the connection. A
//! separate integration test is a separate process, which is the only way to
//! hold both settings at once.

mod support;

use std::time::{Duration, Instant};

use axum::body::Body;
use http::Request;
use logweir_api::auth::ratelimit::STREAMS_PER_ACTOR_NAMESPACE;
use logweir_api::status::{set_stream_bounds_for_test, StreamBounds};
use serde_json::{json, Value};
use support::{repo_root, FakeKube, Options, TestApp, HOST};
use tower::ServiceExt as _;

/// One namespace per test: the slot table is keyed `(actor, namespace)` and
/// both rows here authenticate as the same local administrator.
const NS_HELD: &str = "lw-ceiling-held";
const NS_READ: &str = "lw-ceiling-read";
/// The ceiling this file measures against.
const CEILING: Duration = Duration::from_millis(300);

fn running_backup() -> Value {
    let path = repo_root().join("crates/logweir-api/tests/fixtures/backup-succeeded-verified.json");
    let mut object: Value = serde_json::from_str(
        &std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display())),
    )
    .expect("a fixture is JSON");
    object["metadata"]["name"] = json!("held");
    let status = object["status"].as_object_mut().expect("a status");
    status.insert("phase".into(), json!("Running"));
    status.remove("exitCode");
    status.remove("exitReason");
    status.remove("evidence");
    object
}

fn app(namespace: &str) -> TestApp {
    // The heartbeat is a twentieth of the ceiling, so the channel's eight
    // frames are gone about forty milliseconds in and the producer spends the
    // remaining ~260 ms blocked on `send` — which is the state the first
    // implementation never left.
    set_stream_bounds_for_test(StreamBounds {
        poll: Duration::from_millis(5),
        heartbeat: Duration::from_millis(5),
        max_connection: CEILING,
    });
    let fake = FakeKube::new();
    fake.seed("backups", namespace, running_backup());
    TestApp::with(
        fake,
        Options {
            namespaces: vec![namespace.to_string()],
            ..Options::default()
        },
    )
}

fn events_path(namespace: &str) -> String {
    format!("/api/v1/namespaces/{namespace}/operations/backup/held/events")
}

async fn open_without_reading(app: &TestApp, namespace: &str) -> axum::response::Response {
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(events_path(namespace))
                .header("host", HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("the router is infallible");
    assert_eq!(response.status().as_u16(), 200);
    response
}

/// **A client that never reads does not hold its slot past the ceiling.**
///
/// REGRESSION REASON, MEASURED BY THE REVIEWER. The ceiling used to be checked
/// only BETWEEN sends. A client that opened the stream and stopped reading
/// filled the eight-frame channel, parked the producer in `tx.send(...).await`
/// and kept its task and its stream slot for as long as it held the socket
/// open and silent — 1.212 s against a 120 ms ceiling. Memory was bounded (the
/// channel is), so this is a liveness and slot finding: four silent sockets
/// per `(actor, namespace)` are held indefinitely instead of for five minutes,
/// and `docs/api.md` states flatly that the connection closes after 300 s.
///
/// The probe is the ceiling itself: hold every slot with non-reading clients,
/// then wait for one to come back. Before the fix none ever does.
#[tokio::test]
async fn a_client_that_never_reads_releases_its_slot_at_the_ceiling() {
    let app = app(NS_HELD);
    let mut held = Vec::new();
    for _ in 0..STREAMS_PER_ACTOR_NAMESPACE {
        held.push(open_without_reading(&app, NS_HELD).await);
    }
    // The ceiling is full: the next subscribe is refused.
    app.get(&events_path(NS_HELD))
        .await
        .assert_problem(429, "rate_limited");

    // Nobody reads any of the four. Their producers fill the channel, block,
    // and must still give up at the deadline.
    let started = Instant::now();
    let mut freed = None;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let retry = app.get(&events_path(NS_HELD)).await;
        if retry.status.as_u16() == 200 {
            freed = Some(started.elapsed());
            break;
        }
    }
    let elapsed = freed.expect(
        "no slot came back: a client that never reads is holding its stream past the ceiling",
    );
    // Generous, because the harness itself is doing work — but nowhere near
    // the "for ever" the defect produced.
    assert!(
        elapsed < CEILING * 8,
        "a slot took {elapsed:?} to come back against a {CEILING:?} ceiling"
    );
    drop(held);
}

/// **A client that DOES read still gets its `end` frame at the ceiling.**
///
/// The fix must not throw the last frame away: the bounded send's budget is
/// exhausted at exactly the moment `end` is written, so `send_end` falls back
/// to a non-blocking offer. A reader has room and is told why its stream
/// closed; a non-reader has a full channel and gets nothing, which is correct.
#[tokio::test]
async fn a_reading_client_is_told_why_the_stream_closed() {
    let app = app(NS_READ);
    let started = Instant::now();
    let response = app.get(&events_path(NS_READ)).await;
    let elapsed = started.elapsed();
    assert_eq!(response.status.as_u16(), 200);
    let body = response.text();
    let events: Vec<&str> = body
        .lines()
        .filter_map(|l| l.strip_prefix("event: "))
        .collect();
    assert_eq!(events.first(), Some(&"operation"));
    assert_eq!(
        events.last(),
        Some(&"end"),
        "the last frame was lost to its own deadline: {events:?}"
    );
    let last: Value = body
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .next_back()
        .map(|l| serde_json::from_str(l).expect("JSON"))
        .expect("a final frame");
    assert_eq!(last["reason"], "maxDuration");
    assert!(
        elapsed < CEILING * 4,
        "the stream ran {elapsed:?} against a {CEILING:?} ceiling"
    );
}
