//! FX-13a: a sign-in `state` is single-use on each replica, and the
//! provider's single-use authorization code is the backstop across replicas.
//!
//! WHAT WAS WRONG. The login cookie is sealed and stateless: it opens for 600
//! seconds and nothing recorded that its `state` had been used. Someone who
//! kept a copy could drive `/auth/callback` with it again and again, and every
//! callback was a token request to the provider, authenticated as this client
//! (FX-13's review, M1: one login cookie, five callbacks, five token POSTs).
//!
//! WHAT IS ASSERTED. Once the cookie has opened and its `state` matched, and
//! before the code is exchanged, the callback redeems the state in the
//! process's bounded in-memory record. So:
//!
//! * a replay on the same replica is refused `login_state_replayed`, audited,
//!   with its login cookie cleared and NO token request; the first callback
//!   is the control and signs in;
//! * two callbacks with one state at once on one replica: exactly one signs
//!   in and one token request is made;
//! * a replay on ANOTHER replica reaches the token request once, and the
//!   provider refuses the code it already exchanged: no session; that replica
//!   then refuses it without a token request (the behaviour, pinned);
//! * a full record forgets its oldest entry and never refuses a new sign-in;
//!   a replay of a forgotten state meets the same provider backstop.
//!
//! NOTHING IS WRITTEN TO KUBERNETES (the orchestrator's note of 2026-10-09):
//! every row asserts the callback made no Kubernetes write at all, and ends
//! with `assert_strict` on a fake API server that records any request outside
//! the console's surface.
//!
//! NOTHING SECRET IS LOGGED: the rows that drive a replay and a refused
//! exchange read the captured logs for the `state`, the authorization codes,
//! the nonce and the sealed cookies, and find none of them.
//!
//! EVERY ROW HERE CAPTURES THE LOGS, the first thing it does (see `capture`).
//!
//! THE PROVIDER DOUBLE IS SET UP AS A REAL PROVIDER IS: `set_single_use_codes`
//! makes a code exchangeable once (RFC 6749 §4.1.2). The rows that turn it on
//! say so; where it is off, a replay that reached the exchange would sign in a
//! second time, which is what makes the same-replica refusals loud.

mod support;

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use http::Request;
use serde_json::{json, Value};
use support::idp::{Grant, MockIdp, TestKey};
use support::{FakeKube, SharedApp, SharedOptions, TestResponse, ISSUER, SHARED_HOST};

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

    /// One note of the audit line for a request id (`usedSignInStates`, …).
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

/// Capture this thread's logs, as JSON lines, for the life of the guard.
///
/// EVERY ROW IN THIS FILE CALLS THIS FIRST, INCLUDING ONE THAT READS NO LOG
/// (FX-13a review, L1). The subscriber is the thread's default, not the
/// process's, and `tracing-core` decides once, when a log statement is first
/// reached, whether anyone wants it. While exactly one subscriber exists in
/// the process it asks only the reaching thread's default (0.1.36,
/// `callsite.rs`, `Rebuilder::JustOne`). So a row on a thread with no
/// subscriber that reaches a statement first switches it off for the
/// capturing row too: that row's WARN and audit lines go missing and it fails
/// for no product reason. Measured before this rule: 31 runs in 40 failed
/// with the concurrent row not capturing, under a two-row filter.
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

/// One replica: its own process state (limiter, record of redeemed states,
/// caches), over a cluster and a provider that `fake` and `idp` may share with
/// another — what two console Pods share.
fn replica(fake: &FakeKube, idp: &MockIdp) -> SharedApp {
    replica_with(fake, idp, None)
}

/// A replica whose record of redeemed states is shrunk to `capacity`.
fn replica_with(fake: &FakeKube, idp: &MockIdp, capacity: Option<usize>) -> SharedApp {
    SharedApp::new(
        fake.clone(),
        idp.clone(),
        SharedOptions {
            bindings: support::default_bindings(),
            used_state_capacity: capacity,
            ..SharedOptions::default()
        },
    )
}

/// The Kubernetes requests that are not reads: a sign-in makes none.
fn kube_writes(fake: &FakeKube) -> Vec<String> {
    fake.requests()
        .iter()
        .filter(|r| r.method != "GET")
        .map(|r| format!("{} {}", r.method, r.path))
        .collect()
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

/// The value of the login cookie as the browser holds it: the sealed state.
fn sealed_login_cookie(started: &Started) -> &str {
    started.cookie.split_once('=').expect("a cookie pair").1
}

/// **Nothing secret reached the logs** (FX-13a review, L2): not the `state`,
/// an authorization code, the nonce, the login cookie's sealed value or a
/// session cookie's.
///
/// ONLY FOR A ROW THAT HAS ALSO FOUND ITS LOG LINES. An absence read from a
/// capture that missed the lines proves nothing, so each caller first counts
/// the WARN lines of the paths it drove.
fn assert_no_secret_logged(
    logs: &Buffer,
    logins: &[&Started],
    codes: &[&str],
    signed_in: &[&TestResponse],
) {
    let text = logs.text();
    let mut secrets: Vec<(&str, String)> = Vec::new();
    for started in logins {
        secrets.push(("state", started.state.clone()));
        secrets.push(("nonce", started.nonce.clone()));
        secrets.push((
            "login cookie's sealed value",
            sealed_login_cookie(started).to_string(),
        ));
    }
    for code in codes {
        secrets.push(("authorization code", (*code).to_string()));
    }
    for response in signed_in {
        let cookie = session_of(response).expect("a signed-in response sets a session");
        let pair = cookie.split(';').next().expect("a cookie pair");
        secrets.push((
            "session cookie's sealed value",
            pair.split_once('=').expect("a cookie pair").1.to_string(),
        ));
    }
    for (what, secret) in &secrets {
        assert!(
            secret.len() >= 16,
            "the {what} is too short to be told from ordinary log text"
        );
        // The message names the kind and never prints the value or the line.
        assert!(
            !text.contains(secret.as_str()),
            "the {what} must never reach the logs"
        );
    }
}

/// A replay refused by this replica's record: by name, cookie cleared.
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

/// A replay that reached the provider and was refused there: no session.
fn assert_refused_by_the_provider(response: &TestResponse, label: &str) {
    assert_eq!(
        response.status.as_u16(),
        401,
        "{label}: {}",
        response.text()
    );
    response.assert_problem(401, "unauthenticated");
    assert!(
        response.text().contains("could not be completed"),
        "{label}: the exchange's refusal, not the record's: {}",
        response.text()
    );
    assert!(session_of(response).is_none(), "{label}: no session");
    assert!(clears_login_cookie(response), "{label}: cookie cleared");
}

// -------------------------------------------------------------------- rows

/// **A replay on the same replica is refused by name, audited, and makes no
/// token request — even well inside the 600 s.** The first callback is the
/// control: it signs in, with exactly one token request. The provider here
/// accepts a code twice, so a replay that reached the exchange would SIGN IN
/// again: the refusal is what stands between them.
#[tokio::test]
async fn a_same_replica_replay_is_refused_with_no_token_request() {
    // Codes no other log text could contain, so their absence means something.
    const CODE: &str = "granted-code-kept-out-of-the-logs-7f3a";
    const JUNK_CODE: &str = "junk-code-nobody-issued-51d2";
    let (logs, _guard) = capture();
    let key = TestKey::ec("k-ec-1");
    let idp = MockIdp::new(ISSUER, &[&key]);
    let fake = FakeKube::new();
    let app = replica(&fake, &idp);
    let started = start_login(&app).await;
    grant(&idp, &key, CODE, &started);

    let first = callback(&app, &started, CODE).await;
    assert_signed_in(&first, "the first callback");
    assert_eq!(idp.token_calls(), 1);

    // The same URL and cookie again — what a copied cookie and a captured
    // callback URL give anyone. Then with a junk code, which is what the
    // amplification looked like, and then 599 s later, inside the cookie's
    // life.
    let replay = callback(&app, &started, CODE).await;
    assert_replay_refused(&replay, "the same callback again");
    let junk = callback(&app, &started, JUNK_CODE).await;
    assert_replay_refused(&junk, "the same state, another code");
    app.app.clock.advance(599);
    let late = callback(&app, &started, CODE).await;
    assert_replay_refused(&late, "599 s later");

    assert_eq!(
        idp.token_calls(),
        1,
        "a replay is refused BEFORE the code is exchanged: one token request in all"
    );
    for response in [&replay, &junk, &late] {
        let record = logs.audit(&response.header("x-request-id").unwrap());
        assert_eq!(record["action"], "auth.callback");
        assert_eq!(record["decision"], "deny");
        assert_eq!(record["failureCode"], "login_state_replayed");
    }
    assert_eq!(
        logs.audit(&first.header("x-request-id").unwrap())["decision"],
        "allow"
    );
    assert_eq!(
        logs.count("refused before any token request (login_state_replayed)"),
        3,
        "each replay is a WARN line an operator can alert on"
    );
    // Those three lines were captured, so this absence is not an empty read:
    // neither the state, nor the code a replay carried (the granted one or
    // the junk one), nor the nonce, nor either sealed cookie is in the logs.
    assert_no_secret_logged(&logs, &[&started], &[CODE, JUNK_CODE], &[&first]);
    assert_eq!(app.app.state.shared().unwrap().used_states.len(), 1);
    assert!(kube_writes(&fake).is_empty(), "{:?}", kube_writes(&fake));
    fake.assert_strict();
}

/// **Two callbacks with one state at once, on one replica: exactly one signs
/// in, and exactly one token request is made.**
///
/// THE RACE IS REAL. The provider takes 150 ms to answer a token request and
/// accepts a code twice here, so a design that exchanged first and recorded
/// after — or checked the record and wrote it later — would let both
/// callbacks into the exchange and sign in twice. The record's one lock is
/// what makes "first" atomic on a replica.
///
/// WHAT THIS ROW CANNOT SEE. Both callbacks run on this test's one thread and
/// interleave only at an `await`, so a check and a mark split with no `await`
/// between them passes here. The unit row
/// `of_eight_threads_redeeming_one_state_exactly_one_is_first` holds that
/// with real threads.
#[tokio::test]
async fn concurrent_callbacks_on_one_replica_exactly_one_wins() {
    let (logs, _guard) = capture();
    let key = TestKey::ec("k-ec-1");
    let idp = MockIdp::new(ISSUER, &[&key]);
    let fake = FakeKube::new();
    let app = replica(&fake, &idp);
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
    let (winner, loser) = if a.status.as_u16() == 303 {
        (&a, &b)
    } else {
        (&b, &a)
    };
    assert_signed_in(winner, "the concurrent winner");
    assert_replay_refused(loser, "the concurrent loser");
    assert_eq!(idp.token_calls(), 1, "and exactly one token request");
    let record = logs.audit(&loser.header("x-request-id").unwrap());
    assert_eq!(record["action"], "auth.callback");
    assert_eq!(record["decision"], "deny");
    assert_eq!(
        record["failureCode"], "login_state_replayed",
        "the loser is refused by this replica's record, by name"
    );
    assert_eq!(
        logs.audit(&winner.header("x-request-id").unwrap())["decision"],
        "allow"
    );
    assert_eq!(
        logs.count("refused before any token request (login_state_replayed)"),
        1,
        "the loser's WARN line"
    );
    assert_eq!(app.app.state.shared().unwrap().used_states.len(), 1);
    assert!(kube_writes(&fake).is_empty(), "{:?}", kube_writes(&fake));
    fake.assert_strict();
}

/// **A replay on ANOTHER replica reaches the token request once, the provider
/// refuses the code it already exchanged, and no session is created** — the
/// behaviour pinned, not a guarantee of this service. The record is per
/// replica (nothing is shared or written to the cluster), so replica A has
/// not seen a state B redeemed; what stops the replay there is the
/// provider's single-use code (RFC 6749 §4.1.2). A then remembers the state,
/// so a second replay on A costs no token request. Two callbacks at once on
/// two replicas: both reach the provider, and the provider lets exactly one
/// sign in.
#[tokio::test]
async fn a_cross_replica_replay_reaches_the_provider_which_refuses_the_reused_code() {
    // Codes no other log text could contain, so their absence means something.
    const CODE_1: &str = "first-code-kept-out-of-the-logs-93b1";
    const CODE_2: &str = "second-code-kept-out-of-the-logs-c4e7";
    let (logs, _guard) = capture();
    let key = TestKey::ec("k-ec-1");
    let idp = MockIdp::new(ISSUER, &[&key]);
    idp.set_single_use_codes(true);
    let fake = FakeKube::new();
    let a = replica(&fake, &idp);
    let b = replica(&fake, &idp);

    // In sequence. The login starts on A and finishes on B: a login begun on
    // one replica finishes on another (the control).
    let started = start_login(&a).await;
    grant(&idp, &key, CODE_1, &started);
    let finished_on_b = callback(&b, &started, CODE_1).await;
    assert_signed_in(&finished_on_b, "finished on B");
    assert_eq!(idp.token_calls(), 1);

    let on_a = callback(&a, &started, CODE_1).await;
    assert_refused_by_the_provider(&on_a, "replayed on A, which never saw it");
    assert_eq!(
        idp.token_calls(),
        2,
        "the cross-replica replay costs one token request"
    );
    assert_eq!(
        logs.audit(&on_a.header("x-request-id").unwrap())["failureCode"],
        "code_exchange_failed",
        "refused at the exchange, by the provider"
    );
    assert_replay_refused(&callback(&a, &started, CODE_1).await, "again on A");
    assert_replay_refused(&callback(&b, &started, CODE_1).await, "again on B");
    assert_eq!(idp.token_calls(), 2, "once per replica, never more");

    // At the same moment, one callback to each replica.
    idp.set_token_delay(Some(Duration::from_millis(150)));
    let second = start_login(&a).await;
    grant(&idp, &key, CODE_2, &second);
    let (on_a, on_b) = tokio::join!(callback(&a, &second, CODE_2), callback(&b, &second, CODE_2));
    let wins = [&on_a, &on_b]
        .iter()
        .filter(|r| r.status.as_u16() == 303)
        .count();
    assert_eq!(
        wins, 1,
        "two replicas at once: the provider's single-use code lets exactly one sign in \
         ({} on A, {} on B)",
        on_a.status, on_b.status
    );
    let (winner, loser) = if on_a.status.as_u16() == 303 {
        (&on_a, &on_b)
    } else {
        (&on_b, &on_a)
    };
    assert_refused_by_the_provider(loser, "the loser");
    assert_eq!(idp.token_calls(), 4, "one token request per replica");

    // The two refused exchanges and the two record refusals each left their
    // WARN line, so the absence below is not an empty read: neither state,
    // neither code (each was replayed into a refused exchange), no nonce and
    // no sealed cookie is in the logs.
    assert_eq!(
        logs.count("a sign-in was refused"),
        2,
        "one WARN line for each exchange the provider refused"
    );
    assert_eq!(
        logs.count("refused before any token request (login_state_replayed)"),
        2
    );
    assert_no_secret_logged(
        &logs,
        &[&started, &second],
        &[CODE_1, CODE_2],
        &[&finished_on_b, winner],
    );
    assert!(kube_writes(&fake).is_empty(), "{:?}", kube_writes(&fake));
    fake.assert_strict();
}

/// **A full record forgets its oldest entry and never refuses a new sign-in.**
///
/// The record is shrunk to two. The third sign-in still signs in; the record
/// stays at two, having forgotten the first (whose login could still open),
/// which the audit line notes (`usedSignInStates: full`) and ONE warning a
/// minute says. The first's replay then meets the backstop: one token
/// request, refused by the provider, no session. The third's replay is still
/// refused by the record, with no token request.
#[tokio::test]
async fn a_full_record_evicts_the_oldest_and_never_blocks_a_sign_in() {
    let (logs, _guard) = capture();
    let key = TestKey::ec("k-ec-1");
    let idp = MockIdp::new(ISSUER, &[&key]);
    idp.set_single_use_codes(true);
    let fake = FakeKube::new();
    let app = replica_with(&fake, &idp, Some(2));

    let mut started = Vec::new();
    let mut notes = Vec::new();
    for i in 0..4 {
        let s = start_login(&app).await;
        grant(&idp, &key, &format!("code-{i}"), &s);
        let done = callback(&app, &s, &format!("code-{i}")).await;
        assert_signed_in(&done, &format!("sign-in {i}: a full record never blocks"));
        notes.push(logs.note(&done.header("x-request-id").unwrap(), "usedSignInStates"));
        started.push(s);
    }
    assert_eq!(
        notes,
        vec![
            None,
            None,
            Some("full".to_string()),
            Some("full".to_string())
        ],
        "the 3rd and 4th each forgot a live entry"
    );
    assert_eq!(app.app.state.shared().unwrap().used_states.len(), 2);
    assert_eq!(
        logs.count("this console's record of redeemed sign-in states is full: the oldest"),
        1,
        "announced once a minute, not once an eviction"
    );
    assert_eq!(idp.token_calls(), 4);

    let forgotten = callback(&app, &started[0], "code-0").await;
    assert_refused_by_the_provider(&forgotten, "the forgotten first state, replayed");
    assert_eq!(
        idp.token_calls(),
        5,
        "one token request, refused by the provider"
    );
    assert_replay_refused(
        &callback(&app, &started[3], "code-3").await,
        "the newest state, still remembered",
    );
    assert_eq!(idp.token_calls(), 5);
    assert!(kube_writes(&fake).is_empty());
    fake.assert_strict();
}
