//! The trusted-proxy contract at the shared entry point (PLAT-17.2).
//!
//! D0 §"Identity, session, proxy, and browser trust boundary" fixes three
//! things, and this file holds all three against the real router:
//!
//! 1. an identity header from ANY peer — trusted proxy or not — is never an
//!    identity (it is stripped and recorded by name; without a session the
//!    request is `401`);
//! 2. forwarded client and scheme headers are read only from a peer inside
//!    `trustedProxyCidrs`, and only for transport facts;
//! 3. with `requireTrustedProxy`, a request that did not arrive from such a
//!    peer, or that the peer did not vouch for as `X-Forwarded-Proto: https`,
//!    is refused `421 misdirected_request` before routing — audit code
//!    `untrusted_entry_point` — while the two kubelet probes stay reachable.

mod support;

use std::io::Write;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};

use axum::body::Body;
use http::Request;
use logweir_api::authz::Role;
use logweir_api::config::Cidr;
use logweir_api::http::PeerAddr;
use serde_json::Value;
use support::{FakeKube, SharedApp, SharedOptions, TestResponse, ISSUER, NS_A, SHARED_HOST};

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
    fn record(&self, audit_id: &str) -> (Value, Value) {
        let text = String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned();
        text.lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|line| line["target"] == "logweir_api::audit")
            .find_map(|line| {
                let record: Value = serde_json::from_str(line["fields"]["audit"].as_str()?).ok()?;
                let notes: Value =
                    serde_json::from_str(line["fields"]["notes"].as_str().unwrap_or("{}"))
                        .unwrap_or(Value::Null);
                (record["auditId"] == audit_id).then_some((record, notes))
            })
            .unwrap_or_else(|| panic!("no audit record for {audit_id}:\n{text}"))
    }
}

/// Every test in this binary captures; see `tests/attribution.rs` for why.
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

const INGRESS: &str = "10.42.0.17";
const POD_ELSEWHERE: &str = "10.99.3.4";

fn app(require: bool) -> SharedApp {
    SharedApp::new(
        FakeKube::new(),
        support::idp::MockIdp::new(ISSUER, &[]),
        SharedOptions {
            bindings: support::RoleBindings {
                revision: "entry-rev-1".into(),
                bindings: vec![support::binding(Role::Viewer, NS_A, &["lw-a-viewers"])],
            },
            trusted_proxy_cidrs: vec![Cidr::parse("10.42.0.0/16").unwrap()],
            require_trusted_proxy: require,
            ..SharedOptions::default()
        },
    )
}

async fn get(
    app: &SharedApp,
    path: &str,
    peer: Option<&str>,
    headers: &[(&str, &str)],
) -> TestResponse {
    let mut builder = Request::builder()
        .method("GET")
        .uri(path)
        .header("host", SHARED_HOST);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    if let Some(peer) = peer {
        builder = builder.extension(PeerAddr(peer.parse::<IpAddr>().unwrap()));
    }
    app.app.send(builder.body(Body::empty()).unwrap()).await
}

/// **Through the trusted ingress over HTTPS, a signed-in viewer is served.**
/// The positive control every refusal below is measured against.
#[tokio::test]
async fn the_trusted_ingress_over_https_is_served() {
    let (log, _guard) = capture();
    let app = app(true);
    let cookie = app.session_cookie("u-v", &["lw-a-viewers"]);
    let response = get(
        &app,
        "/api/v1/session",
        Some(INGRESS),
        &[
            ("cookie", &cookie),
            ("x-forwarded-proto", "https"),
            ("x-forwarded-for", "203.0.113.50, 10.42.0.17"),
        ],
    )
    .await;
    assert_eq!(response.status, 200, "{}", response.text());
    let (record, _) = log.record(&response.header("x-request-id").unwrap());
    // The trusted peer's forwarded client is RECORDED — and only recorded.
    assert_eq!(record["forwardedFor"], "203.0.113.50");
    assert_eq!(record["peer"], INGRESS);
}

/// **A pod that dials the console directly is refused, even when it forges
/// the proxy's headers and carries a valid session.** Kubernetes is never
/// called, and the audit record names the real reason.
#[tokio::test]
async fn a_direct_peer_is_refused_whatever_it_claims() {
    let (log, _guard) = capture();
    let app = app(true);
    let cookie = app.session_cookie("u-v", &["lw-a-viewers"]);
    let response = get(
        &app,
        &format!("/api/v1/namespaces/{NS_A}/backups"),
        Some(POD_ELSEWHERE),
        &[
            ("cookie", &cookie),
            ("x-forwarded-proto", "https"),
            ("x-forwarded-for", "10.42.0.17"),
        ],
    )
    .await;
    response.assert_problem(421, "misdirected_request");
    assert!(app.app.fake.requests().is_empty());
    let (record, notes) = log.record(&response.header("x-request-id").unwrap());
    assert_eq!(record["failureCode"], "untrusted_entry_point");
    assert_eq!(notes["entryPoint"], "peerNotTrusted");
    // A forged X-Forwarded-For from an untrusted peer is not even recorded.
    assert_eq!(record["forwardedFor"], "");
}

/// **The trusted peer must vouch for HTTPS, exactly once.** Missing, `http`,
/// and two conflicting headers are each refused.
#[tokio::test]
async fn the_trusted_peer_must_assert_https_exactly_once() {
    let (log, _guard) = capture();
    let app = app(true);
    let cookie = app.session_cookie("u-v", &["lw-a-viewers"]);
    for proto in [None, Some("http"), Some("https, http"), Some("")] {
        let mut headers = vec![("cookie", cookie.as_str())];
        if let Some(proto) = proto {
            headers.push(("x-forwarded-proto", proto));
        }
        let response = get(&app, "/api/v1/session", Some(INGRESS), &headers).await;
        response.assert_problem(421, "misdirected_request");
        let (record, notes) = log.record(&response.header("x-request-id").unwrap());
        assert_eq!(record["failureCode"], "untrusted_entry_point", "{proto:?}");
        assert_eq!(notes["entryPoint"], "forwardedProtoNotHttps", "{proto:?}");
    }
    // Two separate header lines are two claims, not one.
    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/session")
        .header("host", SHARED_HOST)
        .header("cookie", &cookie)
        .header("x-forwarded-proto", "https")
        .header("x-forwarded-proto", "https")
        .extension(PeerAddr(INGRESS.parse().unwrap()))
        .body(Body::empty())
        .unwrap();
    app.app
        .send(request)
        .await
        .assert_problem(421, "misdirected_request");
}

/// **A request with no socket peer at all is not trusted.**
#[tokio::test]
async fn an_unknown_peer_is_not_trusted() {
    let (_log, _guard) = capture();
    let app = app(true);
    get(
        &app,
        "/api/v1/session",
        None,
        &[("x-forwarded-proto", "https")],
    )
    .await
    .assert_problem(421, "misdirected_request");
}

/// **The kubelet's probes stay reachable from the Pod IP**, or a console that
/// requires its ingress would never become ready.
#[tokio::test]
async fn the_probes_are_exempt() {
    let (_log, _guard) = capture();
    let app = app(true);
    let health = get(&app, "/healthz", Some("10.1.0.1"), &[]).await;
    assert_eq!(health.status, 200, "{}", health.text());
}

/// **Forged identity headers are never an identity, through the trusted
/// ingress included.** A trusted peer relaying `X-Remote-User: admin` and
/// friends with no session is `401` — the headers are stripped and recorded by
/// name only.
#[tokio::test]
async fn identity_headers_from_the_trusted_ingress_are_still_not_an_identity() {
    let (log, _guard) = capture();
    let app = app(true);
    let response = get(
        &app,
        &format!("/api/v1/namespaces/{NS_A}/backups"),
        Some(INGRESS),
        &[
            ("x-forwarded-proto", "https"),
            ("x-remote-user", "admin"),
            ("x-remote-groups", "lw-a-viewers"),
            ("x-forwarded-user", "admin"),
            ("x-auth-request-user", "admin"),
        ],
    )
    .await;
    response.assert_problem(401, "unauthenticated");
    assert!(app.app.fake.requests().is_empty());
    let (record, _) = log.record(&response.header("x-request-id").unwrap());
    assert_eq!(
        record["ignoredIdentityHeaders"],
        serde_json::json!([
            "x-auth-request-user",
            "x-forwarded-user",
            "x-remote-groups",
            "x-remote-user"
        ])
    );
    assert_eq!(record["actorId"], "");
    assert!(!record.to_string().contains("\"admin\""));
}

/// **With the flag off, the entry point is exactly what it was.** A peer
/// outside every range, with no forwarded scheme, is served — the
/// backward-compatible default — and its forged `X-Forwarded-For` is still not
/// recorded.
#[tokio::test]
async fn without_the_flag_the_entry_point_is_unchanged() {
    let (log, _guard) = capture();
    let app = app(false);
    let cookie = app.session_cookie("u-v", &["lw-a-viewers"]);
    let response = get(
        &app,
        "/api/v1/session",
        Some(POD_ELSEWHERE),
        &[("cookie", &cookie), ("x-forwarded-for", "198.51.100.1")],
    )
    .await;
    assert_eq!(response.status, 200, "{}", response.text());
    let (record, _) = log.record(&response.header("x-request-id").unwrap());
    assert_eq!(record["forwardedFor"], "");
}

/// **The audited client is the rightmost hop the trusted proxies did not add**
/// (review L4). A client that sends its own `X-Forwarded-For: 198.51.100.66`
/// ahead of the ingress's appended entry is recorded as the address the
/// ingress saw, not as the one it invented.
#[tokio::test]
async fn the_audited_client_is_the_rightmost_untrusted_hop() {
    let (log, _guard) = capture();
    let app = app(true);
    let cookie = app.session_cookie("u-v", &["lw-a-viewers"]);
    let response = get(
        &app,
        "/api/v1/session",
        Some(INGRESS),
        &[
            ("cookie", &cookie),
            ("x-forwarded-proto", "https"),
            // client-invented, then the real client the ingress saw, then a
            // second trusted proxy hop.
            ("x-forwarded-for", "198.51.100.66, 203.0.113.50, 10.42.0.9"),
        ],
    )
    .await;
    assert_eq!(response.status, 200);
    let (record, _) = log.record(&response.header("x-request-id").unwrap());
    assert_eq!(record["forwardedFor"], "203.0.113.50");
}

/// **`/readyz` is exempt too** (review L7): a kubelet dialling the Pod IP from
/// outside every trusted range reaches the readiness handler, whatever it then
/// answers — never the entry point's 421.
#[tokio::test]
async fn the_readiness_probe_is_exempt() {
    let (_log, _guard) = capture();
    let app = app(true);
    let ready = get(&app, "/readyz", Some("10.1.0.1"), &[]).await;
    assert_ne!(ready.status, 421, "{}", ready.text());
    assert!(
        ready.status == 200 || ready.status == 503,
        "{}",
        ready.status
    );
}
