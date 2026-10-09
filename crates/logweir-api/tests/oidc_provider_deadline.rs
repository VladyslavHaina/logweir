//! FX-28: a provider that stalls its body is bounded by the provider deadline.
//!
//! THE PROVIDER IS ON THE WIRE. Every other sign-in test serves the provider
//! in-process (`support::idp::MockIdp`), which never reaches the code this file
//! is about: the production `HyperHttpClient`, its socket and its timer. Here a
//! loopback HTTP/1.1 server this file binds itself puts the same documents on
//! a real socket — discovery, the key set and the token endpoint answered by a
//! `MockIdp` behind it — and can, per path, send the head and then stall, send
//! the body slowly but steadily, or send a document over the 512 KiB cap. The
//! console reaches it as `http://127.0.0.1:<port>`, the loopback escape hatch
//! the configuration allows for a local provider, so the rows need no
//! certificate; `tests/oidc_trust.rs` holds the TLS half of the same client.
//!
//! WHAT A ROW PROVES, AND WHY IT CANNOT PASS FOR THE WRONG REASON.
//! * A stall row answers within [`EXPECTED_DEADLINE`] plus [`SLACK`], and NOT
//!   BEFORE the deadline less a little: a provider that refused or failed fast
//!   would answer early and fail the row. The wire records that it sent the
//!   head and withheld the body, and that the console then hung up on it.
//! * The bound is this file's own literal, not `PROVIDER_DEADLINE`: a mutant
//!   that doubles the constant, or the call site's use of it, is still caught
//!   (the FX-24 review's L1).
//! * The NEGATIVE CONTROL is the pre-fix code, with the timer around the
//!   request alone: each stall row then fails "still pending after 15 s"
//!   (`claude/fx-28.result.md`, the mutant table).
//! * The slow-but-steady row is the positive control: a provider that takes
//!   six seconds per document, twelve for the callback's two, still signs in.
//!   So the bound is per request, it is not an idle timer shorter than a slow
//!   provider's pace, and it is not one deadline over the whole sign-in.
//!
//! The wire binds `127.0.0.1` only; nothing dials off the loopback interface.

mod support;

use std::collections::BTreeMap;
use std::io::{Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Body;
use http::Request;
use logweir_api::app::Clock as _;
use logweir_api::auth::oidc::{
    HttpClient, HttpError, HyperHttpClient, TlsTrust, MAX_PROVIDER_BODY, PROVIDER_DEADLINE,
};
use serde_json::{json, Value};
use support::idp::{Grant, MockIdp, TestKey};
use support::{FakeKube, SharedApp, SharedOptions, TestResponse, SHARED_HOST};

/// The deadline this file expects, written out rather than read from the
/// crate, so a changed constant fails [`the_provider_deadline_is_ten_seconds`]
/// and a changed call site fails every stall row.
const EXPECTED_DEADLINE: Duration = Duration::from_secs(10);
/// How late past the deadline an answer may come on a loaded host.
const SLACK: Duration = Duration::from_secs(5);
/// How early before the deadline a stall row may answer: never meaningfully
/// (a timer does not fire early), so an earlier answer is a fast failure that
/// is not the stall this row is about.
const EARLY: Duration = Duration::from_millis(500);
/// How long the wire holds a stalled response open before it gives up on the
/// client. Far past `EXPECTED_DEADLINE + SLACK`, so under the pre-fix code the
/// row's own bound expires first and the row fails as "still pending".
const HOLD_LIMIT: Duration = Duration::from_secs(60);

const DISCOVERY: &str = "/.well-known/openid-configuration";
const JWKS: &str = "/jwks";
const TOKEN: &str = "/token";

// ------------------------------------------------------------------ the wire

/// What the wire does with one path.
#[derive(Clone, Copy, Debug)]
enum Behaviour {
    /// Answer at once, whole.
    Serve,
    /// Send the head (with the document's full `content-length`) and the
    /// first `sent` bytes of the body, then nothing.
    StallAfterHead { sent: usize },
    /// Read the request and send nothing at all: the stall is BEFORE the
    /// response head (FX-28 review M1).
    NoHead,
    /// Trickle the head over `head_over`, then send one body byte and stall:
    /// with one deadline over the request and the body the answer comes at
    /// the deadline; with a timer per phase it would come at `head_over`
    /// plus the deadline (FX-28 review M1).
    SlowHeadThenStall { head_over: Duration },
    /// Send the head, then the body in `chunks` pieces, `gap` apart.
    Steady { chunks: usize, gap: Duration },
    /// Serve the document with a padding field that takes it past
    /// [`MAX_PROVIDER_BODY`]. Still a valid document of the right shape, so
    /// only the cap can refuse it.
    Oversize,
}

#[derive(Default)]
struct WireState {
    behaviours: BTreeMap<&'static str, Behaviour>,
    /// Every request path, in arrival order.
    requests: Vec<String>,
    /// Stalled responses: the body, or the whole answer, withheld.
    withheld: usize,
    /// For each stalled response, how long after the stall began the console
    /// hung up on it — `None` while it has not.
    hangups: Vec<Option<Duration>>,
}

/// A loopback HTTP/1.1 provider: a `MockIdp`'s documents on a real socket.
struct Wire {
    issuer: String,
    idp: MockIdp,
    state: Arc<Mutex<WireState>>,
}

impl Wire {
    fn start(keys: &[&TestKey]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a loopback port");
        let issuer = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let idp = MockIdp::new(&issuer, keys);
        let state = Arc::new(Mutex::new(WireState::default()));
        let (shared_idp, shared_state) = (idp.clone(), Arc::clone(&state));
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let (idp, state) = (shared_idp.clone(), Arc::clone(&shared_state));
                std::thread::spawn(move || answer(stream, &idp, &state));
            }
        });
        Self { issuer, idp, state }
    }

    fn set(&self, path: &'static str, behaviour: Behaviour) {
        self.state
            .lock()
            .unwrap()
            .behaviours
            .insert(path, behaviour);
    }

    fn requests_to(&self, path: &str) -> usize {
        self.state
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|p| *p == path)
            .count()
    }

    fn withheld(&self) -> usize {
        self.state.lock().unwrap().withheld
    }

    /// Wait (bounded) until every stalled response has been hung up on, and
    /// return how long after its stall began each one was.
    ///
    /// ASYNC, for the router rows: the client's connection task runs on the
    /// test's own single-threaded runtime, and it is that task which closes
    /// the socket once the request was dropped. A blocking sleep here would
    /// starve it and report a leak that is not there.
    async fn hangups_within(&self, limit: Duration) -> Vec<Option<Duration>> {
        let until = Instant::now() + limit;
        loop {
            let hangups = self.state.lock().unwrap().hangups.clone();
            if hangups.iter().all(Option::is_some) || Instant::now() >= until {
                return hangups;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// [`Self::hangups_within`] for a row with no runtime of its own (the
    /// built binary, whose connections are its own process's).
    fn hangups_within_blocking(&self, limit: Duration) -> Vec<Option<Duration>> {
        let until = Instant::now() + limit;
        loop {
            let hangups = self.state.lock().unwrap().hangups.clone();
            if hangups.iter().all(Option::is_some) || Instant::now() >= until {
                return hangups;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// The console's production client for this wire: plain HTTP is allowed
    /// because the issuer is loopback, exactly as `oidc.insecureLoopbackIssuer`
    /// allows it.
    fn client(&self) -> Box<dyn HttpClient> {
        Box::new(HyperHttpClient::new(true, &TlsTrust::system()).expect("the client builds"))
    }
}

/// Read one request, answer it from `idp` as the path's behaviour says.
fn answer(mut stream: TcpStream, idp: &MockIdp, state: &Arc<Mutex<WireState>>) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if let Some(at) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            break at + 4;
        }
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return,
            Ok(n) => buffer.extend_from_slice(&chunk[..n]),
        }
        if buffer.len() > 64 * 1024 {
            return;
        }
    };
    let head = String::from_utf8_lossy(&buffer[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let mut request_line = lines.next().unwrap_or_default().split(' ');
    let method = request_line.next().unwrap_or_default().to_string();
    let path = request_line.next().unwrap_or_default().to_string();
    let header = |name: &str| {
        head.split("\r\n").skip(1).find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case(name)
                .then(|| value.trim().to_string())
        })
    };
    let length: usize = header("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let mut body = buffer[head_end..].to_vec();
    while body.len() < length {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return,
            Ok(n) => body.extend_from_slice(&chunk[..n]),
        }
    }
    let behaviour = {
        let mut state = state.lock().unwrap();
        state.requests.push(path.clone());
        state
            .behaviours
            .get(path.as_str())
            .copied()
            .unwrap_or(Behaviour::Serve)
    };

    // The MockIdp's answer, exactly as the in-process rows get it. Its
    // futures never wait on anything, so a throwaway runtime finishes them.
    let url = format!("{}{path}", idp.issuer());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime");
    let answered = runtime.block_on(async {
        if method == "POST" {
            let form = String::from_utf8_lossy(&body).into_owned();
            idp.post_form(&url, &form, header("authorization").as_deref())
                .await
        } else {
            idp.get(&url).await
        }
    });
    let (status, mut document) = match answered {
        Ok(document) => ("200 OK", document),
        Err(HttpError::Failed(reason)) if reason.contains("HTTP 400") => {
            ("400 Bad Request", br#"{"error":"invalid_grant"}"#.to_vec())
        }
        Err(_) => (
            "503 Service Unavailable",
            br#"{"error":"unavailable"}"#.to_vec(),
        ),
    };
    if let Behaviour::Oversize = behaviour {
        let mut value: Value = serde_json::from_slice(&document).expect("a JSON document");
        value["padding"] = Value::String("x".repeat(MAX_PROVIDER_BODY + 1024));
        document = serde_json::to_vec(&value).unwrap();
    }
    let head = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\
         connection: close\r\n\r\n",
        document.len()
    );
    match behaviour {
        Behaviour::NoHead => return hold(stream, state),
        Behaviour::SlowHeadThenStall { head_over } => {
            let pieces = 14;
            let gap = head_over / pieces;
            for piece in head.as_bytes().chunks(head.len().div_ceil(pieces as usize)) {
                std::thread::sleep(gap);
                if stream
                    .write_all(piece)
                    .and_then(|()| stream.flush())
                    .is_err()
                {
                    return;
                }
            }
            if stream
                .write_all(&document[..1])
                .and_then(|()| stream.flush())
                .is_err()
            {
                return;
            }
            return hold(stream, state);
        }
        _ => {}
    }
    if stream.write_all(head.as_bytes()).is_err() {
        return;
    }
    match behaviour {
        Behaviour::NoHead | Behaviour::SlowHeadThenStall { .. } => unreachable!("answered above"),
        Behaviour::Serve | Behaviour::Oversize => {
            let _ = stream.write_all(&document);
            let _ = stream.flush();
        }
        Behaviour::Steady { chunks, gap } => {
            for piece in document.chunks(document.len().div_ceil(chunks).max(1)) {
                std::thread::sleep(gap);
                if stream
                    .write_all(piece)
                    .and_then(|()| stream.flush())
                    .is_err()
                {
                    return;
                }
            }
        }
        Behaviour::StallAfterHead { sent } => {
            let _ = stream.write_all(&document[..sent.min(document.len())]);
            let _ = stream.flush();
            hold(stream, state);
        }
    }
}

/// Send nothing more, and watch (bounded by [`HOLD_LIMIT`]) for the client
/// hanging up, recording when it did.
fn hold(mut stream: TcpStream, state: &Arc<Mutex<WireState>>) {
    let since = Instant::now();
    let index = {
        let mut state = state.lock().unwrap();
        state.withheld += 1;
        state.hangups.push(None);
        state.hangups.len() - 1
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
    let mut probe = [0u8; 64];
    while since.elapsed() < HOLD_LIMIT {
        match stream.read(&mut probe) {
            Ok(0) => {
                state.lock().unwrap().hangups[index] = Some(since.elapsed());
                return;
            }
            Ok(_) => {}
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(_) => {
                state.lock().unwrap().hangups[index] = Some(since.elapsed());
                return;
            }
        }
    }
}

// ------------------------------------------------------------- log capture

#[derive(Clone, Default)]
struct Buffer(Arc<Mutex<Vec<u8>>>);

struct BufferWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for BufferWriter {
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
    fn count(&self, needle: &str) -> usize {
        String::from_utf8_lossy(&self.0.lock().unwrap())
            .lines()
            .filter(|line| line.contains(needle))
            .count()
    }

    /// The audit record of one request, by its `x-request-id`.
    fn audit(&self, response: &TestResponse) -> Value {
        let id = response.header("x-request-id").expect("a request id");
        let text = String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned();
        text.lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|line| line["target"] == "logweir_api::audit")
            .find_map(|line| {
                let record: Value = serde_json::from_str(line["fields"]["audit"].as_str()?).ok()?;
                (record["auditId"] == id.as_str()).then_some(record)
            })
            .unwrap_or_else(|| panic!("no audit record for {id}:\n{text}"))
    }
}

/// Capture this thread's log lines. `#[tokio::test]` runs the router on the
/// test's own thread, so its audit record lands here.
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

// ------------------------------------------------------------- the console

fn console(wire: &Wire) -> SharedApp {
    SharedApp::over_http(
        FakeKube::new(),
        wire.idp.clone(),
        &wire.issuer,
        wire.client(),
        SharedOptions {
            bindings: support::default_bindings(),
            ..SharedOptions::default()
        },
    )
}

/// One request through the router, timed, and refused as a TEST FAILURE if
/// it is still pending at the deadline plus slack — which is what the pre-fix
/// code does with a stalled body.
async fn timed(app: &SharedApp, request: Request<Body>) -> (TestResponse, Duration) {
    let started = Instant::now();
    let bound = EXPECTED_DEADLINE + SLACK;
    match tokio::time::timeout(bound, app.app.send(request)).await {
        Ok(response) => (response, started.elapsed()),
        Err(_) => panic!(
            "the request was still pending after {bound:?}: the provider's body read has no \
             deadline (the pre-FX-28 behaviour)"
        ),
    }
}

fn get(path: &str, cookie: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder()
        .method("GET")
        .uri(path)
        .header("host", SHARED_HOST);
    if let Some(cookie) = cookie {
        builder = builder.header("cookie", cookie);
    }
    builder.body(Body::empty()).unwrap()
}

/// What `/auth/login` handed the browser.
struct Started {
    cookie: String,
    state: String,
    nonce: String,
}

fn started(response: &TestResponse) -> Started {
    assert_eq!(response.status, 303, "{}", response.text());
    let cookie = response
        .headers
        .get_all("set-cookie")
        .iter()
        .map(|v| v.to_str().unwrap().to_string())
        .find(|c| c.starts_with("__Host-logweir_login="))
        .expect("the login cookie is set")
        .split(';')
        .next()
        .unwrap()
        .to_string();
    let location = response.header("location").expect("a Location");
    let query: BTreeMap<String, String> =
        serde_urlencoded::from_str(location.split_once('?').unwrap().1).unwrap();
    Started {
        cookie,
        state: query["state"].clone(),
        nonce: query["nonce"].clone(),
    }
}

/// Register `code` at the provider for this login, with a valid token.
fn grant(app: &SharedApp, wire: &Wire, key: &TestKey, started: &Started, code: &str) {
    let now = app.app.clock.now().timestamp();
    let token = key.mint(&json!({
        "iss": wire.issuer,
        "sub": "u-ada",
        "aud": support::CLIENT_ID,
        "exp": now + 300,
        "iat": now,
        "nonce": started.nonce,
        "name": "Ada Lovelace",
        "groups": ["lw-a-operators"],
    }));
    wire.idp.grant(
        code,
        Grant {
            id_token: token,
            code_challenge: None,
            redirect_uri: None,
        },
    );
}

fn callback(started: &Started, code: &str) -> Request<Body> {
    get(
        &format!("/auth/callback?code={code}&state={}", started.state),
        Some(&started.cookie),
    )
}

/// The three properties every stall row shares: answered after the deadline
/// (so it WAS the stall), within it plus slack, and the console hung up on
/// the provider's connection rather than leaving it open.
async fn assert_stalled_and_bounded(wire: &Wire, elapsed: Duration) {
    assert!(
        elapsed >= EXPECTED_DEADLINE - EARLY,
        "answered after {elapsed:?}, before the deadline: a fast failure, not the stall"
    );
    assert!(
        elapsed <= EXPECTED_DEADLINE + SLACK,
        "answered after {elapsed:?}, past the deadline plus slack"
    );
    assert_eq!(
        wire.withheld(),
        1,
        "the wire sent one head and withheld its body"
    );
    let hangups = wire.hangups_within(SLACK).await;
    eprintln!("FX-28 evidence: answered after {elapsed:?}; the provider saw the hang-up {hangups:?} after the stall began");
    assert!(
        hangups
            .iter()
            .all(|h| h.is_some_and(|at| at <= EXPECTED_DEADLINE + SLACK)),
        "the console released its connection to the stalled provider: {hangups:?}"
    );
}

// ------------------------------------------------------------------- rows

/// **The deadline is ten seconds**, and the rows below measure against that
/// literal. A changed constant fails here; a changed call site fails them.
#[test]
fn the_provider_deadline_is_ten_seconds() {
    assert_eq!(PROVIDER_DEADLINE, EXPECTED_DEADLINE);
    assert_eq!(MAX_PROVIDER_BODY, 512 * 1024);
}

/// **A provider that accepts the request and never sends a head fails the
/// sign-in within the deadline** (FX-28 review M1). The deadline covers the
/// request phase, not only the body: a timer around the body alone leaves
/// this row "still pending after 15 s".
#[tokio::test]
async fn a_provider_that_stalls_before_its_head_fails_the_login_within_the_deadline() {
    let (log, _guard) = capture();
    let key = TestKey::ec("k1");
    let wire = Wire::start(&[&key]);
    wire.set(DISCOVERY, Behaviour::NoHead);
    let app = console(&wire);

    let (response, elapsed) = timed(&app, get("/auth/login", None)).await;
    response.assert_problem(503, "kubernetes_unavailable");
    assert_eq!(log.audit(&response)["failureCode"], "provider_timeout");
    assert_eq!(wire.requests_to(DISCOVERY), 1);
    assert_stalled_and_bounded(&wire, elapsed).await;
}

/// How long the slow-head row's provider takes over its head.
const SLOW_HEAD: Duration = Duration::from_secs(7);

/// **ONE deadline covers the request and the body together** (FX-28 review
/// M1). The provider trickles its head over seven seconds, then sends one
/// body byte and stalls. One deadline answers at ten seconds; a timer per
/// phase (ten for the head, ten more for the body) would answer at about
/// seventeen, past this row's bound.
#[tokio::test]
async fn a_slow_head_then_a_stalled_body_shares_one_deadline() {
    let (log, _guard) = capture();
    let key = TestKey::ec("k1");
    let wire = Wire::start(&[&key]);
    wire.set(
        DISCOVERY,
        Behaviour::SlowHeadThenStall {
            head_over: SLOW_HEAD,
        },
    );
    let app = console(&wire);

    let (response, elapsed) = timed(&app, get("/auth/login", None)).await;
    response.assert_problem(503, "kubernetes_unavailable");
    assert_eq!(log.audit(&response)["failureCode"], "provider_timeout");
    assert!(
        elapsed < SLOW_HEAD + EXPECTED_DEADLINE - Duration::from_secs(2),
        "answered after {elapsed:?}: the body had a deadline of its own after the head"
    );
    // `assert_stalled_and_bounded`'s `withheld == 1` says the wire got its
    // whole head and the body byte out before the console hung up.
    assert_stalled_and_bounded(&wire, elapsed).await;
}

/// **A TLS handshake that stalls is bounded by the same deadline**, at the
/// production client over an `https://` URL: the peer accepts TCP and never
/// answers the ClientHello. The stall is named `HttpError::Deadline` (the
/// request's own timer, not the dial's longer one), and the console hangs up.
#[tokio::test]
async fn a_tls_handshake_that_stalls_is_bounded_by_the_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a loopback port");
    let port = listener.local_addr().unwrap().port();
    let hung_up: Arc<Mutex<Option<Duration>>> = Arc::default();
    let seen = Arc::clone(&hung_up);
    std::thread::spawn(move || {
        let Ok((mut stream, _)) = listener.accept() else {
            return;
        };
        let since = Instant::now();
        let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
        let mut probe = [0u8; 1024];
        while since.elapsed() < HOLD_LIMIT {
            match stream.read(&mut probe) {
                Ok(0) => break,
                Ok(_) => {}
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) => {}
                Err(_) => break,
            }
        }
        *seen.lock().unwrap() = Some(since.elapsed());
    });
    let client = HyperHttpClient::new(false, &TlsTrust::system()).expect("the client builds");

    let started = Instant::now();
    let bound = EXPECTED_DEADLINE + SLACK;
    let answered = tokio::time::timeout(
        bound,
        client.get(&format!("https://127.0.0.1:{port}{DISCOVERY}")),
    )
    .await
    .unwrap_or_else(|_| panic!("a stalled TLS handshake was still pending after {bound:?}"));
    let elapsed = started.elapsed();
    assert_eq!(answered, Err(HttpError::Deadline));
    assert!(
        elapsed >= EXPECTED_DEADLINE - EARLY && elapsed <= bound,
        "answered after {elapsed:?}"
    );
    let until = Instant::now() + SLACK;
    while hung_up.lock().unwrap().is_none() && Instant::now() < until {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let hang_up = *hung_up.lock().unwrap();
    eprintln!("FX-28 evidence (TLS stall): answered after {elapsed:?}; the peer saw the hang-up {hang_up:?} after accept");
    assert!(
        hang_up.is_some_and(|at| at <= bound),
        "the console released the stalled handshake's connection: {hang_up:?}"
    );
}

/// **A discovery document whose body stalls fails the sign-in within the
/// deadline**: `/auth/login` — unauthenticated, and the route that fetches
/// discovery whenever its cache is stale — answers `503` with the audit
/// failure `provider_timeout`, after ten seconds and not much later.
#[tokio::test]
async fn a_stalled_discovery_body_fails_the_login_within_the_deadline() {
    let (log, _guard) = capture();
    let key = TestKey::ec("k1");
    let wire = Wire::start(&[&key]);
    wire.set(DISCOVERY, Behaviour::StallAfterHead { sent: 1 });
    let app = console(&wire);

    let (response, elapsed) = timed(&app, get("/auth/login", None)).await;
    response.assert_problem(503, "kubernetes_unavailable");
    assert!(
        !response.text().contains("127.0.0.1"),
        "the answer names no endpoint: {}",
        response.text()
    );
    assert_eq!(log.audit(&response)["failureCode"], "provider_timeout");
    assert_eq!(wire.requests_to(DISCOVERY), 1);
    assert_stalled_and_bounded(&wire, elapsed).await;
}

/// **A key set whose body stalls fails the callback within the deadline.**
/// Discovery and the token endpoint answer at once; the key set sends its
/// head and stops. The callback is refused `401` with `provider_timeout`.
#[tokio::test]
async fn a_stalled_jwks_body_fails_the_callback_within_the_deadline() {
    let (log, _guard) = capture();
    let key = TestKey::ec("k1");
    let wire = Wire::start(&[&key]);
    wire.set(JWKS, Behaviour::StallAfterHead { sent: 1 });
    let app = console(&wire);

    let login = started(&app.app.send(get("/auth/login", None)).await);
    grant(&app, &wire, &key, &login, "code-jwks");
    let (response, elapsed) = timed(&app, callback(&login, "code-jwks")).await;
    response.assert_problem(401, "unauthenticated");
    assert_eq!(log.audit(&response)["failureCode"], "provider_timeout");
    assert_eq!(wire.idp.token_calls(), 1, "the code was exchanged");
    assert_eq!(wire.requests_to(JWKS), 1);
    assert_stalled_and_bounded(&wire, elapsed).await;
}

/// **A token response whose body stalls fails the callback within the
/// deadline**, named `provider_timeout` — not `code_exchange_failed`, which
/// is a provider that refused the code — and the key set is never asked for.
#[tokio::test]
async fn a_stalled_token_body_fails_the_callback_within_the_deadline() {
    let (log, _guard) = capture();
    let key = TestKey::ec("k1");
    let wire = Wire::start(&[&key]);
    wire.set(TOKEN, Behaviour::StallAfterHead { sent: 1 });
    let app = console(&wire);

    let login = started(&app.app.send(get("/auth/login", None)).await);
    grant(&app, &wire, &key, &login, "code-token");
    let (response, elapsed) = timed(&app, callback(&login, "code-token")).await;
    response.assert_problem(401, "unauthenticated");
    assert_eq!(log.audit(&response)["failureCode"], "provider_timeout");
    assert_eq!(
        log.count("did not complete a request within the 10-second provider deadline"),
        1,
        "the refusal's WARN line names the deadline"
    );
    assert_eq!(wire.requests_to(TOKEN), 1);
    assert_eq!(wire.requests_to(JWKS), 0);
    assert_stalled_and_bounded(&wire, elapsed).await;
}

/// **A slow but steady provider still signs in** (the positive control).
/// Every document arrives in twelve pieces half a second apart — six seconds
/// each, inside the deadline — so the login takes about six seconds and the
/// callback, which reads the token and then the key set, about twelve: longer
/// than one deadline, because the bound is per request.
#[tokio::test]
async fn a_slow_but_steady_provider_still_signs_in() {
    let key = TestKey::ec("k1");
    let wire = Wire::start(&[&key]);
    let steady = Behaviour::Steady {
        chunks: 12,
        gap: Duration::from_millis(500),
    };
    for path in [DISCOVERY, JWKS, TOKEN] {
        wire.set(path, steady);
    }
    let app = console(&wire);

    let (response, login_elapsed) = timed(&app, get("/auth/login", None)).await;
    let login = started(&response);
    assert!(
        login_elapsed >= Duration::from_secs(5),
        "discovery arrived slowly ({login_elapsed:?})"
    );
    grant(&app, &wire, &key, &login, "code-steady");
    let started_at = Instant::now();
    let response = tokio::time::timeout(
        Duration::from_secs(40),
        app.app.send(callback(&login, "code-steady")),
    )
    .await
    .expect("a steady provider's callback finishes");
    let elapsed = started_at.elapsed();
    eprintln!("FX-28 evidence (steady): login {login_elapsed:?}, callback {elapsed:?}");
    assert_eq!(response.status, 303, "{}", response.text());
    assert!(
        response
            .headers
            .get_all("set-cookie")
            .iter()
            .any(|c| c.to_str().unwrap().starts_with("__Host-logweir_session=")),
        "a session was issued"
    );
    assert!(
        elapsed > EXPECTED_DEADLINE,
        "the callback's two slow documents took {elapsed:?}, more than one deadline"
    );
    assert_eq!(wire.requests_to(TOKEN), 1);
    assert_eq!(wire.requests_to(JWKS), 1);
}

/// **A document over 512 KiB is refused, and says so.** The padded
/// discovery document is otherwise valid, so only the cap refuses it; the
/// same wire serving it unpadded signs in (the control).
#[tokio::test]
async fn a_provider_document_over_the_cap_is_refused() {
    let (log, _guard) = capture();
    let key = TestKey::ec("k1");
    let wire = Wire::start(&[&key]);
    wire.set(DISCOVERY, Behaviour::Oversize);
    let app = console(&wire);

    let (response, _) = timed(&app, get("/auth/login", None)).await;
    response.assert_problem(503, "kubernetes_unavailable");
    assert_eq!(log.audit(&response)["failureCode"], "provider_unreachable");
    assert_eq!(log.count("the provider response is larger than 512 KiB"), 1);

    wire.set(DISCOVERY, Behaviour::Serve);
    let (response, _) = timed(&app, get("/auth/login", None)).await;
    assert_eq!(response.status, 303, "{}", response.text());
}

// ------------------------------------------------------- the built binary

/// The built server, killed and reaped when it leaves scope — a panicking
/// assertion included — with both pipes drained for its whole life.
struct Server {
    child: std::process::Child,
    output: Arc<Mutex<Vec<u8>>>,
}

impl Server {
    fn log(&self) -> String {
        String::from_utf8_lossy(&self.output.lock().unwrap()).into_owned()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A scratch directory, removed when it leaves scope.
struct Scratch(std::path::PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Start `logweir-api` in shared mode against `issuer`, on a free loopback
/// port, and wait (bounded) for its own "started" line naming that port.
fn shared_server(issuer: &str) -> (Server, u16, Scratch) {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let scratch =
        Scratch(std::env::temp_dir().join(format!("logweir-fx28-{}-{stamp}", std::process::id())));
    let dir = &scratch.0;
    std::fs::create_dir_all(dir).unwrap();
    let key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    for file in ["session.key.yaml", "cursor.key.yaml"] {
        std::fs::write(dir.join(file), format!("version: 1\nkey: \"{key}\"\n")).unwrap();
    }
    std::fs::write(dir.join("client.secret"), "a-client-secret\n").unwrap();
    // A kubeconfig whose server answers nothing: these rows never reach it.
    std::fs::write(
        dir.join("kubeconfig.yaml"),
        "apiVersion: v1\nkind: Config\nclusters:\n- name: fixture\n  cluster:\n    server: \
         https://127.0.0.1:1\ncontexts:\n- name: fixture\n  context:\n    cluster: fixture\n    \
         user: fixture\nusers:\n- name: fixture\n  user: {}\n",
    )
    .unwrap();
    let ui = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../ui");
    for _attempt in 0..5 {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let config = dir.join("console.yaml");
        std::fs::write(
            &config,
            format!(
                "mode: shared\nlisten: \"127.0.0.1:{port}\"\npublicBaseUrl: \
                 \"https://console.test\"\nuiDirectory: {ui}\n\
                 oidc:\n  issuer: {issuer}\n  clientId: logweir-console\n  \
                 clientSecretFile: {dir}/client.secret\n  insecureLoopbackIssuer: true\n\
                 roles:\n  revision: r1\n  bindings:\n  - role: viewer\n    namespace: team-a\n    \
                 groups: [lw-viewers]\n\
                 sessionKey:\n  file: {dir}/session.key.yaml\n  expectedVersion: 1\n\
                 cursorKey:\n  file: {dir}/cursor.key.yaml\n  expectedVersion: 1\n\
                 namespaces: [team-a]\nkubernetes:\n  source: kubeconfig\n  kubeconfig: \
                 {dir}/kubeconfig.yaml\n  context: fixture\n",
                ui = ui.display(),
                dir = dir.display(),
            ),
        )
        .unwrap();
        let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_logweir-api"))
            .arg("--config")
            .arg(&config)
            .env("RUST_LOG", "info")
            .env_remove("KUBECONFIG")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("the binary starts");
        let output = Arc::new(Mutex::new(Vec::new()));
        let pipes: [Box<dyn std::io::Read + Send>; 2] = [
            Box::new(child.stdout.take().unwrap()),
            Box::new(child.stderr.take().unwrap()),
        ];
        for mut pipe in pipes {
            let sink = Arc::clone(&output);
            std::thread::spawn(move || {
                let mut buffer = [0u8; 4096];
                while let Ok(read) = pipe.read(&mut buffer) {
                    if read == 0 {
                        break;
                    }
                    sink.lock().unwrap().extend_from_slice(&buffer[..read]);
                }
            });
        }
        let mut server = Server { child, output };
        let announced = format!("\"listen\":\"127.0.0.1:{port}\"");
        let until = Instant::now() + Duration::from_secs(40);
        loop {
            let log = server.log();
            if log.contains("logweir-api started") && log.contains(&announced) {
                return (server, port, scratch);
            }
            if let Some(exit) = server.child.try_wait().unwrap() {
                std::thread::sleep(Duration::from_millis(200));
                let log = server.log();
                if log.contains("cannot bind the listener") {
                    break; // the free port went stale; try another
                }
                panic!("the server exited before it listened ({exit}):\n{log}");
            }
            assert!(
                Instant::now() < until,
                "the server did not listen within 40 s:\n{log}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    panic!("five free ports in a row were taken");
}

/// **On the built binary, a stalled provider's sign-in is answered and its
/// connection closed** — the connection, and the permit it holds in the
/// server's accept loop, is released at the deadline rather than when the
/// provider gives up. A real socket sends `GET /auth/login`; discovery sends
/// its head and stalls. The status line arrives after ten seconds and not
/// much later, it is `503`, and the server then closes the connection. Under
/// the pre-fix code nothing arrives within the bound.
#[test]
fn the_built_binary_answers_a_stalled_sign_in_and_closes_the_connection() {
    let key = TestKey::ec("k1");
    let wire = Wire::start(&[&key]);
    wire.set(DISCOVERY, Behaviour::StallAfterHead { sent: 1 });
    let (server, port, _scratch) = shared_server(&wire.issuer);

    let mut socket = TcpStream::connect(("127.0.0.1", port)).expect("the server accepts");
    socket
        .set_read_timeout(Some(Duration::from_millis(250)))
        .unwrap();
    let started = Instant::now();
    socket
        .write_all(b"GET /auth/login HTTP/1.1\r\nhost: console.test\r\nconnection: close\r\n\r\n")
        .unwrap();
    let bound = EXPECTED_DEADLINE + SLACK;
    let mut received = Vec::new();
    let mut first_byte: Option<Duration> = None;
    let mut closed: Option<Duration> = None;
    let mut chunk = [0u8; 4096];
    while started.elapsed() < bound + Duration::from_secs(5) {
        match socket.read(&mut chunk) {
            Ok(0) => {
                closed = Some(started.elapsed());
                break;
            }
            Ok(n) => {
                first_byte.get_or_insert(started.elapsed());
                received.extend_from_slice(&chunk[..n]);
            }
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(e) => panic!("the connection failed: {e}\n{}", server.log()),
        }
    }
    let text = String::from_utf8_lossy(&received);
    let first_byte = first_byte.unwrap_or_else(|| {
        panic!(
            "no answer within {bound:?}: the provider's body read has no deadline (the \
             pre-FX-28 behaviour)\n{}",
            server.log()
        )
    });
    assert!(
        text.starts_with("HTTP/1.1 503"),
        "a stalled provider is a 503: {text}"
    );
    assert!(
        first_byte >= EXPECTED_DEADLINE - EARLY && first_byte <= bound,
        "answered after {first_byte:?}"
    );
    let closed = closed.unwrap_or_else(|| panic!("the server kept the connection open: {text}"));
    assert!(closed <= bound, "closed after {closed:?}");
    assert!(
        server.log().contains("provider_timeout"),
        "the server logged the named reason:\n{}",
        server.log()
    );
    assert_eq!(wire.withheld(), 1);
    let hangups = wire.hangups_within_blocking(SLACK);
    eprintln!(
        "FX-28 evidence (binary): status line after {first_byte:?}, closed after {closed:?}; \
         the provider saw the hang-up {hangups:?} after the stall began"
    );
    assert!(
        hangups.iter().all(|h| h.is_some_and(|at| at <= bound)),
        "the server released its connection to the stalled provider: {hangups:?}"
    );
}
