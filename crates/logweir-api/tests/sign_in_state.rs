//! FX-13a: the sign-in `state` is single-use, on every replica.
//!
//! WHAT WAS WRONG. The login cookie is sealed and stateless: it opens for 600
//! seconds and nothing recorded that its `state` had been used. Someone who
//! kept a copy could drive `/auth/callback` with it again and again, and every
//! callback was a token request to the provider, authenticated as this client
//! (FX-13's review, M1: one login cookie, five callbacks, five token POSTs).
//!
//! WHAT IS ASSERTED. A state is claimed — one Kubernetes Event created under a
//! keyed hash of it, which the API server decides atomically — after the cookie
//! has opened and its `state` matched and BEFORE the code is exchanged. So:
//!
//! * a replayed callback is refused `login_state_replayed`, audited, with its
//!   login cookie cleared and NO token request; the first one is the control
//!   and signs in;
//! * two callbacks with one state at the same moment: exactly one signs in and
//!   exactly one token request is made;
//! * the same across two API processes over one cluster — the PoC runs two
//!   replicas, and the claim is in the cluster, not in either process;
//! * a claim that cannot be recorded refuses the sign-in before any token
//!   request, and a console that cannot record one is not ready;
//! * a replay that reaches the same replica is refused without a Kubernetes
//!   call, and a flood past the per-process cluster-claim budget writes no
//!   more claims and locks nobody out (the degradation is bounded and stated
//!   as rows);
//! * a successful sign-in is unchanged, and its claim says nothing about the
//!   state.
//!
//! THE PROVIDER DOUBLE IS ON THE REPLAY'S SIDE. `MockIdp` exchanges a granted
//! code as often as it is asked (a real provider would refuse a second use of
//! a code; a junk code still costs it a request). So without the claim, the
//! replays below would not merely cost a token request: they would sign in a
//! second time. That makes each row's failure loud, not subtle.

mod support;

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use http::Request;
use logweir_api::auth::keys::{CookieKeys, VersionedKey, SIGN_IN_CLAIM_PREFIX};
use logweir_api::auth::login::SignInClaims;
use logweir_api::kube::{KubeAdapter, SIGN_IN_CLAIM_REASON};
use serde_json::{json, Value};
use support::idp::{Grant, MockIdp, TestKey};
use support::{
    FakeKube, Fault, SharedApp, SharedOptions, TestResponse, CLAIM_NAMESPACE, ISSUER, SHARED_HOST,
};

// ------------------------------------------------------------ log capture

#[derive(Clone, Default)]
struct Buffer(Arc<Mutex<Vec<u8>>>);

struct BufferWriter(Arc<Mutex<Vec<u8>>>);

impl Write for BufferWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(data);
        Ok(data.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buffer {
    type Writer = BufferWriter;
    fn make_writer(&'a self) -> Self::Writer {
        BufferWriter(Arc::clone(&self.0))
    }
}

impl Buffer {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }

    /// The one audit record for a request id.
    fn audit(&self, request_id: &str) -> Value {
        let found: Vec<Value> = self
            .text()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|line| line["target"] == "logweir_api::audit")
            .filter_map(|line| {
                line["fields"]["audit"]
                    .as_str()
                    .and_then(|audit| serde_json::from_str::<Value>(audit).ok())
            })
            .filter(|record| record["auditId"] == request_id)
            .collect();
        assert_eq!(found.len(), 1, "one audit record for {request_id}");
        found[0].clone()
    }

    /// One note of the audit line for a request id (`signInClaim`, …).
    fn note(&self, request_id: &str, name: &str) -> Option<String> {
        self.text()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|line| line["target"] == "logweir_api::audit")
            .find(|line| {
                line["fields"]["audit"]
                    .as_str()
                    .and_then(|audit| serde_json::from_str::<Value>(audit).ok())
                    .is_some_and(|record| record["auditId"] == request_id)
            })
            .and_then(|line| {
                line["fields"]["notes"]
                    .as_str()
                    .and_then(|notes| serde_json::from_str::<Value>(notes).ok())
            })
            .and_then(|notes| notes[name].as_str().map(str::to_string))
    }

    /// How many log lines carry `needle`.
    fn count(&self, needle: &str) -> usize {
        self.text().lines().filter(|l| l.contains(needle)).count()
    }
}

fn capture() -> (Buffer, tracing::subscriber::DefaultGuard) {
    let buffer = Buffer::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(buffer.clone())
        .with_env_filter(logweir_api::audit::log_filter_from("debug"))
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);
    (buffer, guard)
}

// ------------------------------------------------------------------ setup

fn now() -> i64 {
    chrono::DateTime::parse_from_rfc3339("2026-09-15T12:00:00Z")
        .unwrap()
        .timestamp()
}

fn claims(nonce: &str) -> Value {
    json!({
        "iss": ISSUER,
        "sub": "u-ada",
        "aud": support::CLIENT_ID,
        "exp": now() + 300,
        "iat": now(),
        "auth_time": now() - 30,
        "nonce": nonce,
        "name": "Ada Lovelace",
        "groups": ["lw-a-operators"],
    })
}

/// One replica: its own process state (limiter, readiness, caches), over a
/// cluster and a provider that `fake` and `idp` may share with another.
fn replica(fake: &FakeKube, idp: &MockIdp, name: &str) -> SharedApp {
    replica_with(fake, idp, name, None)
}

/// A replica with its cluster-claim budget and record capacity shrunk.
fn replica_with(
    fake: &FakeKube,
    idp: &MockIdp,
    name: &str,
    bounds: Option<(u32, usize)>,
) -> SharedApp {
    SharedApp::new(
        fake.clone(),
        idp.clone(),
        SharedOptions {
            bindings: support::default_bindings(),
            replica: Some(name.to_string()),
            claim_bounds: bounds,
            ..SharedOptions::default()
        },
    )
}

/// The `POST …/events` requests the fake has received.
fn event_posts(fake: &FakeKube) -> usize {
    fake.requests()
        .iter()
        .filter(|r| r.method == "POST" && r.path.ends_with("/events"))
        .count()
}

struct Started {
    cookie: String,
    state: String,
    nonce: String,
}

async fn start_login(app: &SharedApp) -> Started {
    let response = app
        .app
        .send(
            Request::builder()
                .method("GET")
                .uri("/auth/login")
                .header("host", SHARED_HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(response.status.as_u16(), 303, "login redirects");
    let location = response.header("location").unwrap();
    let cookie = response
        .headers
        .get_all("set-cookie")
        .iter()
        .map(|v| v.to_str().unwrap().to_string())
        .find(|c| c.starts_with("__Host-logweir_login="))
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    let query: std::collections::BTreeMap<String, String> =
        serde_urlencoded::from_str(location.split_once('?').unwrap().1).unwrap();
    Started {
        cookie,
        state: query["state"].clone(),
        nonce: query["nonce"].clone(),
    }
}

/// The callback a browser (or someone holding a copy of its cookie) sends.
async fn callback(app: &SharedApp, started: &Started, code: &str) -> TestResponse {
    app.app
        .send(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/auth/callback?code={code}&state={}",
                    started.state
                ))
                .header("host", SHARED_HOST)
                .header("cookie", &started.cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
}

fn grant(idp: &MockIdp, key: &TestKey, code: &str, started: &Started) {
    idp.grant(
        code,
        Grant {
            id_token: key.mint(&claims(&started.nonce)),
            code_challenge: None,
            redirect_uri: None,
        },
    );
}

fn session_of(response: &TestResponse) -> Option<String> {
    response
        .headers
        .get_all("set-cookie")
        .iter()
        .map(|v| v.to_str().unwrap().to_string())
        .find(|c| c.starts_with("__Host-logweir_session=") && !c.contains("Max-Age=0"))
}

fn clears_login_cookie(response: &TestResponse) -> bool {
    response
        .headers
        .get_all("set-cookie")
        .iter()
        .map(|v| v.to_str().unwrap().to_string())
        .any(|c| c.starts_with("__Host-logweir_login=;") && c.contains("Max-Age=0"))
}

/// A signed-in callback: 303 to the console with a session.
fn assert_signed_in(response: &TestResponse, label: &str) {
    assert_eq!(
        response.status.as_u16(),
        303,
        "{label}: {}",
        response.text()
    );
    assert!(session_of(response).is_some(), "{label}: a session");
}

/// A replay: refused by name, cookie cleared, no session.
fn assert_replay_refused(response: &TestResponse, label: &str) {
    assert_eq!(
        response.status.as_u16(),
        401,
        "{label}: a replay must be refused, and it got {} with {:?}",
        response.status,
        response.header("location")
    );
    response.assert_problem(401, "unauthenticated");
    assert!(
        response.text().contains("already used"),
        "{label}: the refusal says why: {}",
        response.text()
    );
    assert!(session_of(response).is_none(), "{label}: no session");
    assert!(
        clears_login_cookie(response),
        "{label}: the login cookie is cleared"
    );
}

/// The sign-in claims the cluster holds.
fn claim_events(fake: &FakeKube) -> Vec<Value> {
    fake.objects("events", CLAIM_NAMESPACE)
}

// -------------------------------------------------------------------- rows

/// **A replayed callback is refused by name, audited, and makes no token
/// request — even well inside the 600 s.** The first callback is the control:
/// it signs in, with exactly one token request.
#[tokio::test]
async fn a_replayed_callback_is_refused_by_name_with_no_token_request() {
    let (logs, _guard) = capture();
    let key = TestKey::ec("k-ec-1");
    let idp = MockIdp::new(ISSUER, &[&key]);
    let fake = FakeKube::new();
    let app = replica(&fake, &idp, "console-a");
    let started = start_login(&app).await;
    grant(&idp, &key, "code-1", &started);

    // The control: the first callback signs in.
    let first = callback(&app, &started, "code-1").await;
    assert_signed_in(&first, "the first callback");
    assert_eq!(idp.token_calls(), 1);

    // The same URL and cookie again — what a copied cookie and a captured
    // callback URL give anyone. Then with a junk code, which is what the
    // amplification looked like, and then 599 s later, still inside the
    // cookie's life.
    let replay = callback(&app, &started, "code-1").await;
    assert_replay_refused(&replay, "the same callback again");
    let junk = callback(&app, &started, "a-code-nobody-issued").await;
    assert_replay_refused(&junk, "the same state, another code");
    app.app.clock.advance(599);
    let late = callback(&app, &started, "code-1").await;
    assert_replay_refused(&late, "599 s later");

    assert_eq!(
        idp.token_calls(),
        1,
        "a replay is refused BEFORE the code is exchanged: one token request in all"
    );

    // Audited, by name.
    for response in [&replay, &junk, &late] {
        let record = logs.audit(&response.header("x-request-id").unwrap());
        assert_eq!(record["action"], "auth.callback");
        assert_eq!(record["decision"], "deny");
        assert_eq!(record["failureCode"], "login_state_replayed");
    }
    let first_record = logs.audit(&first.header("x-request-id").unwrap());
    assert_eq!(first_record["decision"], "allow");
    assert!(
        logs.text().contains("refused before any token request"),
        "a replay is also a WARN line an operator can alert on"
    );
    assert!(
        !logs.text().contains(&started.state),
        "the state itself is never logged"
    );
    fake.assert_strict();
}

/// **Two callbacks with one state at the same moment: exactly one signs in,
/// and exactly one token request is made.**
///
/// THE RACE IS REAL. The provider takes 150 ms to answer a token request, so
/// a design that exchanged first and recorded after — or checked a mark and
/// set it later — would let both callbacks into the exchange while the first
/// is still waiting. On one replica the process's own record decides, under
/// one lock; across replicas (the next row) the API server's create does.
#[tokio::test]
async fn two_concurrent_callbacks_with_one_state_exactly_one_wins() {
    let key = TestKey::ec("k-ec-1");
    let idp = MockIdp::new(ISSUER, &[&key]);
    let fake = FakeKube::new();
    let app = replica(&fake, &idp, "console-a");
    let started = start_login(&app).await;
    grant(&idp, &key, "code-1", &started);
    idp.set_token_delay(Some(Duration::from_millis(150)));

    let (a, b) = tokio::join!(
        callback(&app, &started, "code-1"),
        callback(&app, &started, "code-1")
    );
    let wins = [&a, &b].iter().filter(|r| r.status.as_u16() == 303).count();
    assert_eq!(
        wins, 1,
        "exactly one callback signs in: {} and {}",
        a.status, b.status
    );
    let loser = if a.status.as_u16() == 303 { &b } else { &a };
    assert_replay_refused(loser, "the concurrent loser");
    assert_eq!(idp.token_calls(), 1, "and exactly one token request");
    assert_eq!(claim_events(&fake).len(), 1);
    fake.assert_strict();
}

/// **Across two API processes over one cluster: a state redeems once, whichever
/// replica each callback reaches, in sequence and at the same moment.**
///
/// The PoC runs two console replicas behind one ingress, and either may get a
/// callback. Each `replica(...)` below is a separate process state — its own
/// limiter, readiness latch and caches — sharing only what two Pods share: the
/// cluster (`FakeKube`), the provider and the session key. A per-process
/// record would let each replica accept the state once; this row is what a
/// per-process design fails (FX-13a's mutant "per-process used-set").
#[tokio::test]
async fn across_two_replicas_a_state_redeems_once() {
    let key = TestKey::ec("k-ec-1");
    let idp = MockIdp::new(ISSUER, &[&key]);
    let fake = FakeKube::new();
    let a = replica(&fake, &idp, "console-a");
    let b = replica(&fake, &idp, "console-b");

    // In sequence. The login starts on A and finishes on B: a login begun on
    // one replica must be finishable on another (the control).
    let started = start_login(&a).await;
    grant(&idp, &key, "code-1", &started);
    let done = callback(&b, &started, "code-1").await;
    assert_signed_in(&done, "a callback on the other replica");
    assert_replay_refused(&callback(&a, &started, "code-1").await, "replayed on A");
    assert_replay_refused(&callback(&b, &started, "code-1").await, "replayed on B");
    assert_eq!(idp.token_calls(), 1);
    let stored = claim_events(&fake);
    assert_eq!(stored.len(), 1);
    assert_eq!(
        stored[0]["involvedObject"]["name"], "console-b",
        "the claim names the replica that redeemed the state"
    );
    assert_eq!(stored[0]["reportingInstance"], "console-b");

    // At the same moment, one callback to each replica.
    idp.set_token_delay(Some(Duration::from_millis(150)));
    let started = start_login(&a).await;
    grant(&idp, &key, "code-2", &started);
    let (on_a, on_b) = tokio::join!(
        callback(&a, &started, "code-2"),
        callback(&b, &started, "code-2")
    );
    let wins = [&on_a, &on_b]
        .iter()
        .filter(|r| r.status.as_u16() == 303)
        .count();
    assert_eq!(
        wins, 1,
        "one state, two replicas, two callbacks at once: exactly one signs in ({} on A, {} on B)",
        on_a.status, on_b.status
    );
    let loser = if on_a.status.as_u16() == 303 {
        &on_b
    } else {
        &on_a
    };
    assert_replay_refused(loser, "the loser on the other replica");
    assert_eq!(idp.token_calls(), 2, "one token request per state, in all");
    assert_eq!(claim_events(&fake).len(), 2);
    fake.assert_strict();
}

/// **A claim that cannot be recorded refuses the sign-in, before any token
/// request, and says so.** Kubernetes refusing the grant (403) and failing
/// (500) both: an unclaimed state is never exchanged.
#[tokio::test]
async fn a_claim_that_cannot_be_recorded_refuses_the_sign_in_before_any_token_request() {
    let (logs, _guard) = capture();
    let key = TestKey::ec("k-ec-1");
    let idp = MockIdp::new(ISSUER, &[&key]);
    let fake = FakeKube::new();
    let app = replica(&fake, &idp, "console-a");

    for (status, reason) in [(403, "Forbidden"), (500, "InternalError")] {
        let started = start_login(&app).await;
        grant(&idp, &key, "code-1", &started);
        fake.inject(Fault {
            method: "POST",
            path_contains: "/events".into(),
            status,
            reason,
            delay: None,
            remaining: 1,
        });
        let refused = callback(&app, &started, "code-1").await;
        refused.assert_problem(503, "kubernetes_unavailable");
        assert!(session_of(&refused).is_none());
        assert!(clears_login_cookie(&refused), "{status}: cookie cleared");
        assert_eq!(idp.token_calls(), 0, "{status}: no token request");
        let record = logs.audit(&refused.header("x-request-id").unwrap());
        assert_eq!(record["failureCode"], "login_state_claim_failed");
    }

    // The control: the same flow with the cluster answering signs in.
    let started = start_login(&app).await;
    grant(&idp, &key, "code-2", &started);
    assert_signed_in(&callback(&app, &started, "code-2").await, "the control");
    assert_eq!(idp.token_calls(), 1);
}

/// **A successful sign-in is unchanged, and records one claim that says nothing
/// about the state.**
#[tokio::test]
async fn a_successful_sign_in_records_one_claim_and_nothing_about_the_state() {
    let key = TestKey::ec("k-ec-1");
    let idp = MockIdp::new(ISSUER, &[&key]);
    let fake = FakeKube::new();
    let app = replica(&fake, &idp, "console-a");
    let started = start_login(&app).await;
    grant(&idp, &key, "code-1", &started);

    let done = callback(&app, &started, "code-1").await;
    assert_signed_in(&done, "the sign-in");
    assert_eq!(done.header("location").as_deref(), Some("/ui/"));
    assert!(clears_login_cookie(&done), "the login cookie is consumed");
    assert_eq!(idp.token_calls(), 1);

    let stored = claim_events(&fake);
    assert_eq!(stored.len(), 1, "one claim per sign-in");
    let claim = &stored[0];
    let keys = CookieKeys::new(&VersionedKey::from_parts(1, vec![0x7e; 32]));
    assert_eq!(
        claim["metadata"]["name"],
        keys.sign_in_claim_name(&started.state),
        "named by the keyed hash of this state"
    );
    assert!(claim["metadata"]["name"]
        .as_str()
        .unwrap()
        .starts_with(SIGN_IN_CLAIM_PREFIX));
    assert_eq!(claim["metadata"]["namespace"], CLAIM_NAMESPACE);
    assert_eq!(claim["reason"], SIGN_IN_CLAIM_REASON);
    assert_eq!(claim["type"], "Normal");
    assert_eq!(claim["count"], 1);
    assert_eq!(claim["involvedObject"]["kind"], "Pod");
    assert_eq!(claim["involvedObject"]["namespace"], CLAIM_NAMESPACE);
    let text = claim.to_string();
    for secret in [
        started.state.as_str(),
        started.nonce.as_str(),
        "code-1",
        started.cookie.as_str(),
        "u-ada",
        "Ada Lovelace",
    ] {
        assert!(
            !text.contains(secret),
            "the claim carries `{secret}`: {text}"
        );
    }
    // No other Kubernetes write, and nothing but the create on `events`.
    let event_requests: Vec<_> = fake
        .requests()
        .into_iter()
        .filter(|r| r.path.contains("/events"))
        .collect();
    assert_eq!(event_requests.len(), 1);
    assert_eq!(event_requests[0].method, "POST");
    assert!(
        !event_requests[0].query.contains("dryRun"),
        "the sign-in's claim is a real create"
    );
    fake.assert_strict();
}

/// **A replay that reaches the replica that redeemed the state is refused
/// without a Kubernetes call.** The process remembers what it redeemed, so the
/// commonest replay (the same ingress, the same replica) costs the cluster
/// nothing; the control is the first callback's one create.
#[tokio::test]
async fn a_same_replica_replay_is_refused_without_a_kubernetes_call() {
    let key = TestKey::ec("k-ec-1");
    let idp = MockIdp::new(ISSUER, &[&key]);
    let fake = FakeKube::new();
    let app = replica(&fake, &idp, "console-a");
    let started = start_login(&app).await;
    grant(&idp, &key, "code-1", &started);
    assert_signed_in(&callback(&app, &started, "code-1").await, "the control");
    assert_eq!(
        event_posts(&fake),
        1,
        "the first callback claims in the cluster"
    );
    for code in ["code-1", "junk", "code-1"] {
        assert_replay_refused(&callback(&app, &started, code).await, code);
    }
    assert_eq!(event_posts(&fake), 1, "no replay reached Kubernetes");
    assert_eq!(idp.token_calls(), 1);
    fake.assert_strict();
}

/// **A flood past the per-process cluster-claim budget writes no more claims
/// and locks nobody out; the degradation is bounded and announced.**
///
/// Replica A's budget is shrunk to three claims a minute (production: 120).
/// Its first three sign-ins claim in the cluster; the fourth and fifth are
/// SERVED (a refusal here would be the lockout FX-13 rules out), recorded in
/// A only, with the audit note `signInClaim: processOnly` and ONE warning for
/// the window. What that costs, stated as rows rather than prose: a replay of
/// the fourth on A is still refused, while replica B, which has its own budget
/// and has not seen it, accepts it once — and then nobody accepts it again.
/// The next window claims in the cluster again (the control).
#[tokio::test]
async fn a_flood_past_the_claim_budget_writes_no_more_and_locks_nobody_out() {
    let (logs, _guard) = capture();
    let key = TestKey::ec("k-ec-1");
    let idp = MockIdp::new(ISSUER, &[&key]);
    let fake = FakeKube::new();
    let a = replica_with(&fake, &idp, "console-a", Some((3, 100)));
    let b = replica(&fake, &idp, "console-b");

    let mut started = Vec::new();
    for i in 0..5 {
        let s = start_login(&a).await;
        grant(&idp, &key, &format!("code-{i}"), &s);
        let done = callback(&a, &s, &format!("code-{i}")).await;
        assert_signed_in(&done, &format!("sign-in {i} on A"));
        let note = logs.note(&done.header("x-request-id").unwrap(), "signInClaim");
        let want = if i < 3 { "cluster" } else { "processOnly" };
        assert_eq!(note.as_deref(), Some(want), "sign-in {i}");
        started.push(s);
    }
    assert_eq!(
        claim_events(&fake).len(),
        3,
        "the budget is the most A writes in a window"
    );
    assert_eq!(
        logs.count("sign-in claim budget for the minute is spent"),
        1,
        "announced once a window, not once a request"
    );
    assert_eq!(idp.token_calls(), 5);

    // The bound on the degradation.
    let fourth = &started[3];
    assert_replay_refused(
        &callback(&a, fourth, "code-3").await,
        "the 4th replayed on A",
    );
    assert_signed_in(
        &callback(&b, fourth, "code-3").await,
        "the 4th on B, which never saw it: the stated limit",
    );
    assert_replay_refused(&callback(&b, fourth, "code-3").await, "…and on B again");
    assert_replay_refused(&callback(&a, fourth, "code-3").await, "…and on A again");
    assert_eq!(idp.token_calls(), 6, "one extra token request, never more");

    // The control: the next window claims in the cluster again.
    a.app.clock.advance(60);
    let s = start_login(&a).await;
    grant(&idp, &key, "code-next", &s);
    let done = callback(&a, &s, "code-next").await;
    assert_signed_in(&done, "the next window");
    assert_eq!(
        logs.note(&done.header("x-request-id").unwrap(), "signInClaim")
            .as_deref(),
        Some("cluster")
    );
    assert_eq!(
        claim_events(&fake).len(),
        5,
        "B's claim of the 4th, and this one"
    );
    fake.assert_strict();
}

/// **With the budget spent and the process's record full of live states, a
/// sign-in is served unrecorded — memory stays bounded, nobody is refused.**
/// The boundary stated as a row: such a state is not refused again either.
#[tokio::test]
async fn a_full_record_past_the_budget_serves_unrecorded() {
    let (logs, _guard) = capture();
    let key = TestKey::ec("k-ec-1");
    let idp = MockIdp::new(ISSUER, &[&key]);
    let fake = FakeKube::new();
    let a = replica_with(&fake, &idp, "console-a", Some((0, 1)));

    let first = start_login(&a).await;
    grant(&idp, &key, "code-1", &first);
    let done = callback(&a, &first, "code-1").await;
    assert_signed_in(&done, "the first, recorded in A");
    assert_eq!(
        logs.note(&done.header("x-request-id").unwrap(), "signInClaim")
            .as_deref(),
        Some("processOnly")
    );
    let second = start_login(&a).await;
    grant(&idp, &key, "code-2", &second);
    let done = callback(&a, &second, "code-2").await;
    assert_signed_in(&done, "the second, unrecorded");
    assert_eq!(
        logs.note(&done.header("x-request-id").unwrap(), "signInClaim")
            .as_deref(),
        Some("unrecorded")
    );
    assert_eq!(logs.count("its own record is full of live states"), 1);
    assert_replay_refused(&callback(&a, &first, "code-1").await, "the recorded one");
    assert_signed_in(
        &callback(&a, &second, "code-2").await,
        "the unrecorded one is not refused: the stated limit",
    );
    assert_eq!(claim_events(&fake).len(), 0, "a budget of 0 writes nothing");
    fake.assert_strict();
}

/// **Readiness dry-runs a claim until one succeeds, then holds it.** A console
/// that cannot record a claim — no grant, a refused shape, an admission in the
/// way — is not ready, so a rollout does not put it in front of sign-ins; the
/// dry run stores nothing; and once proven, a later outage refuses sign-ins
/// by name rather than cutting the sessions already issued.
#[tokio::test]
async fn readiness_dry_runs_a_claim_until_one_succeeds_then_holds() {
    let fake = FakeKube::new();
    let kube = KubeAdapter::new(fake.client());
    let claims = SignInClaims::new(CLAIM_NAMESPACE, Some("console-a"));
    let now = chrono::DateTime::from_timestamp(now(), 0).unwrap();

    fake.inject(Fault {
        method: "POST",
        path_contains: "/events".into(),
        status: 403,
        reason: "Forbidden",
        delay: None,
        remaining: 1,
    });
    assert!(!claims.ready(&kube, now).await, "no grant: not ready");
    assert!(claims.ready(&kube, now).await, "the grant is there: ready");
    let asked = fake.requests().len();
    fake.inject(Fault {
        method: "POST",
        path_contains: "/events".into(),
        status: 500,
        reason: "InternalError",
        delay: None,
        remaining: 1,
    });
    assert!(claims.ready(&kube, now).await, "latched");
    assert_eq!(fake.requests().len(), asked, "and not asked again");

    let posts: Vec<_> = fake
        .requests()
        .into_iter()
        .filter(|r| r.path.ends_with("/events"))
        .collect();
    assert_eq!(posts.len(), 2);
    for post in &posts {
        assert!(
            post.query.contains("dryRun=All"),
            "readiness only ever dry-runs: {}",
            post.query
        );
    }
    assert_eq!(fake.count("events", CLAIM_NAMESPACE), 0, "nothing stored");

    // Through the route: a console whose claims are refused is not ready.
    let key = TestKey::ec("k-ec-1");
    let idp = MockIdp::new(ISSUER, &[&key]);
    let refused = FakeKube::new();
    refused.inject(Fault {
        method: "POST",
        path_contains: "/events".into(),
        status: 403,
        reason: "Forbidden",
        delay: None,
        remaining: 1,
    });
    let app = replica(&refused, &idp, "console-a");
    assert_eq!(app.app.get("/readyz").await.status.as_u16(), 503);
    let fresh = FakeKube::new();
    let app = replica(&fresh, &idp, "console-b");
    assert_eq!(app.app.get("/readyz").await.status.as_u16(), 200);
}

/// **A strange `HOSTNAME` cannot make every claim invalid.** The replica name
/// a claim carries must be a DNS subdomain; anything else is recorded as the
/// fixed fallback.
#[test]
fn a_replica_name_that_is_not_an_object_name_falls_back() {
    assert_eq!(
        SignInClaims::new("ns", Some("logweir-api-7d9c-xk2p")).replica(),
        "logweir-api-7d9c-xk2p"
    );
    for odd in ["", "UPPER", "with space", "under_score", &"a".repeat(200)] {
        assert_eq!(
            SignInClaims::new("ns", Some(odd)).replica(),
            logweir_api::auth::login::UNNAMED_REPLICA,
            "{odd:?}"
        );
    }
    assert_eq!(
        SignInClaims::new("ns", None).replica(),
        logweir_api::auth::login::UNNAMED_REPLICA
    );
}
