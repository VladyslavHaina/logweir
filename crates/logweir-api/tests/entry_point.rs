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
//!
//! And one thing D0's 2026-10-07 amendment adds (FX-13), at the end of this
//! file: the sign-in limiter counts a request against the forwarded client
//! only when the peer is a trusted proxy under that same decision, and
//! against the peer otherwise. A bucket refuses; it is never an identity.

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

// ------------------------------------------------------------------------
// Chart gap G6: the ingress controller trusted BY ITS SERVICE, not by a /32.
// ------------------------------------------------------------------------

const PROXY_NS: &str = "traefik";
const PROXY_SERVICE: &str = "traefik";

/// One EndpointSlice of `service`, as the EndpointSlice controller writes it.
fn slice(name: &str, service: &str, address_type: &str, endpoints: Value) -> Value {
    serde_json::json!({
        "metadata": {
            "name": name,
            "labels": {"kubernetes.io/service-name": service},
        },
        "addressType": address_type,
        "endpoints": endpoints,
    })
}

fn seed_ingress(fake: &FakeKube, ready: &str) {
    fake.seed(
        "endpointslices",
        PROXY_NS,
        slice(
            "traefik-abcde",
            PROXY_SERVICE,
            "IPv4",
            serde_json::json!([
                // today's pod
                {"addresses": [ready], "conditions": {"ready": true, "serving": true}},
                // a pod shutting down: still serving what it accepted
                {"addresses": ["10.1.0.8"],
                 "conditions": {"ready": false, "serving": true, "terminating": true}},
                // a pod that is not serving at all
                {"addresses": ["10.1.0.9"], "conditions": {"ready": false, "serving": false}},
            ]),
        ),
    );
    fake.seed(
        "endpointslices",
        PROXY_NS,
        slice(
            "traefik-fqdn",
            PROXY_SERVICE,
            "FQDN",
            serde_json::json!([{"addresses": ["traefik.example"], "conditions": {}}]),
        ),
    );
    // Another Service in the same namespace: its pods are NOT the proxy.
    fake.seed(
        "endpointslices",
        PROXY_NS,
        slice(
            "dashboard-xyz",
            "traefik-dashboard",
            "IPv4",
            serde_json::json!([{"addresses": ["10.1.0.50"], "conditions": {"ready": true}}]),
        ),
    );
}

/// **The adapter reads exactly the named Service's SERVING addresses, with one
/// labelled list in the named namespace.** A not-serving endpoint, an FQDN
/// slice and a neighbouring Service's pod are not proxies.
#[tokio::test]
async fn the_proxy_service_read_is_one_labelled_list_of_serving_addresses() {
    let fake = FakeKube::new();
    seed_ingress(&fake, "10.1.0.7");
    let adapter = logweir_api::kube::KubeAdapter::new(fake.client());
    let addresses = adapter
        .list_service_endpoints(PROXY_NS, PROXY_SERVICE)
        .await
        .expect("the list is answered");
    assert_eq!(
        addresses,
        vec![
            "10.1.0.7".parse::<IpAddr>().unwrap(),
            "10.1.0.8".parse::<IpAddr>().unwrap()
        ]
    );
    let requests = fake.requests();
    assert_eq!(requests.len(), 1, "{requests:#?}");
    assert_eq!(requests[0].method, "GET");
    assert_eq!(
        requests[0].path,
        format!("/apis/discovery.k8s.io/v1/namespaces/{PROXY_NS}/endpointslices")
    );
    assert!(
        requests[0]
            .query
            .contains("labelSelector=kubernetes.io%2Fservice-name%3Dtraefik"),
        "{}",
        requests[0].query
    );
    fake.assert_strict();
}

/// **The pod behind the Service is trusted, and follows the pod.** The ingress
/// pod is recreated with a new address: the new one is served after one
/// refresh, the old one is refused, and a pod of a neighbouring Service never
/// was trusted. Before the first read the console is not ready.
#[tokio::test]
async fn the_ingress_is_trusted_through_its_service_and_follows_a_restart() {
    let (log, _guard) = capture();
    let proxies = Arc::new(logweir_api::trusted_proxy::TrustedProxies::new(
        Vec::new(),
        Some(logweir_api::config::ServiceRef {
            namespace: PROXY_NS.into(),
            name: PROXY_SERVICE.into(),
        }),
    ));
    let app = SharedApp::new(
        FakeKube::new(),
        support::idp::MockIdp::new(ISSUER, &[]),
        SharedOptions {
            bindings: support::RoleBindings {
                revision: "entry-rev-1".into(),
                bindings: vec![support::binding(Role::Viewer, NS_A, &["lw-a-viewers"])],
            },
            trusted_proxies: Some(Arc::clone(&proxies)),
            require_trusted_proxy: true,
            ..SharedOptions::default()
        },
    );
    let fake = app.app.fake.clone();
    let adapter = logweir_api::kube::KubeAdapter::new(fake.client());
    let cookie = app.session_cookie("u-v", &["lw-a-viewers"]);
    let through = |peer: &'static str| {
        let cookie = cookie.clone();
        let app = &app;
        async move {
            get(
                app,
                "/api/v1/session",
                Some(peer),
                &[("cookie", &cookie), ("x-forwarded-proto", "https")],
            )
            .await
        }
    };

    assert!(!proxies.ready(), "not ready before the Service is read");
    through("10.1.0.7")
        .await
        .assert_problem(421, "misdirected_request");

    seed_ingress(&fake, "10.1.0.7");
    assert_eq!(proxies.refresh(&adapter).await.unwrap(), Some(2));
    assert!(proxies.ready());
    let served = through("10.1.0.7").await;
    assert_eq!(served.status, 200, "{}", served.text());

    let neighbour = through("10.1.0.50").await;
    neighbour.assert_problem(421, "misdirected_request");
    let (_, notes) = log.record(&neighbour.header("x-request-id").unwrap());
    assert_eq!(notes["entryPoint"], "peerNotTrusted");
    through("10.1.0.9")
        .await
        .assert_problem(421, "misdirected_request");

    // The ingress pod is recreated and comes back on another address.
    seed_ingress(&fake, "10.1.0.11");
    proxies.refresh(&adapter).await.unwrap();
    through("10.1.0.7")
        .await
        .assert_problem(421, "misdirected_request");
    let served = through("10.1.0.11").await;
    assert_eq!(served.status, 200, "{}", served.text());
}

fn proxy_service() -> Option<logweir_api::config::ServiceRef> {
    Some(logweir_api::config::ServiceRef {
        namespace: PROXY_NS.into(),
        name: PROXY_SERVICE.into(),
    })
}

/// **The refresh LOOP, driven through failures, lets the set age out.** The
/// loop reads the Service once, then every read fails: past the staleness
/// window the Service source trusts nobody and is not ready. A loop that
/// re-stamped the last set on a failed refresh (the review's mutant m3b) keeps
/// trusting a departed address forever and fails here. NEGATIVE CONTROL: the
/// same loop over a healthy API keeps the set fresh for the same span.
#[tokio::test]
async fn a_loop_whose_reads_fail_ages_the_set_out() {
    let every = std::time::Duration::from_millis(20);
    let max_age = std::time::Duration::from_millis(300);
    for failing in [false, true] {
        let fake = FakeKube::new();
        seed_ingress(&fake, "10.1.0.7");
        let proxies = Arc::new(
            logweir_api::trusted_proxy::TrustedProxies::new(Vec::new(), proxy_service())
                .with_timing(every, max_age),
        );
        let task = tokio::spawn(
            Arc::clone(&proxies).run(logweir_api::kube::KubeAdapter::new(fake.client())),
        );
        let ip: IpAddr = "10.1.0.7".parse().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !proxies.contains(ip) {
            assert!(
                std::time::Instant::now() < deadline,
                "the first read never landed"
            );
            tokio::time::sleep(every).await;
        }
        if failing {
            fake.inject(support::Fault {
                method: "GET",
                path_contains: "/endpointslices".into(),
                status: 503,
                reason: "ServiceUnavailable",
                delay: None,
                remaining: usize::MAX,
            });
        }
        tokio::time::sleep(max_age * 3).await;
        if failing {
            assert!(
                !proxies.contains(ip) && !proxies.ready(),
                "reads failed for three staleness windows and the set is still trusted"
            );
        } else {
            assert!(
                proxies.contains(ip) && proxies.ready(),
                "a healthy loop must keep the set fresh"
            );
        }
        task.abort();
    }
}

/// **A list that does not fit one page is refused, not trusted half-read**
/// (the review's mutant m1c).
#[tokio::test]
async fn a_proxy_service_with_more_slices_than_one_page_is_refused() {
    let fake = FakeKube::new();
    let pages = logweir_api::kube::MAX_PROXY_SLICES as usize + 1;
    for i in 0..pages {
        fake.seed(
            "endpointslices",
            PROXY_NS,
            slice(
                &format!("traefik-{i:03}"),
                PROXY_SERVICE,
                "IPv4",
                serde_json::json!([{"addresses": [format!("10.2.{}.{}", i / 250, i % 250 + 1)],
                                    "conditions": {"ready": true}}]),
            ),
        );
    }
    let adapter = logweir_api::kube::KubeAdapter::new(fake.client());
    assert_eq!(
        adapter
            .list_service_endpoints(PROXY_NS, PROXY_SERVICE)
            .await,
        Err(logweir_api::kube::KubeFailure::Unavailable)
    );
}

/// **The audited client skips every trusted proxy, the Service's included**
/// (the review's mutant m6). Two chained ingress hops, both endpoints of the
/// Service and neither in a CIDR: the recorded client is the hop before them.
#[tokio::test]
async fn the_audited_client_skips_proxies_trusted_through_the_service() {
    let (log, _guard) = capture();
    let proxies = Arc::new(logweir_api::trusted_proxy::TrustedProxies::new(
        Vec::new(),
        proxy_service(),
    ));
    proxies.replace_at(
        ["10.1.0.7".parse().unwrap(), "10.1.0.8".parse().unwrap()],
        std::time::Instant::now(),
    );
    let app = SharedApp::new(
        FakeKube::new(),
        support::idp::MockIdp::new(ISSUER, &[]),
        SharedOptions {
            bindings: support::RoleBindings {
                revision: "entry-rev-1".into(),
                bindings: vec![support::binding(Role::Viewer, NS_A, &["lw-a-viewers"])],
            },
            trusted_proxies: Some(proxies),
            require_trusted_proxy: true,
            ..SharedOptions::default()
        },
    );
    let cookie = app.session_cookie("u-v", &["lw-a-viewers"]);
    let response = get(
        &app,
        "/api/v1/session",
        Some("10.1.0.7"),
        &[
            ("cookie", &cookie),
            ("x-forwarded-proto", "https"),
            ("x-forwarded-for", "198.51.100.9, 203.0.113.50, 10.1.0.8"),
        ],
    )
    .await;
    assert_eq!(response.status, 200, "{}", response.text());
    let (record, _) = log.record(&response.header("x-request-id").unwrap());
    assert_eq!(record["forwardedFor"], "203.0.113.50");
}

// ------------------------------------------------------------------------
// FX-13: the sign-in limiter's key, through the real router. Behind one
// ingress every request has the ingress as its peer, so a peer key is one
// global budget. These rows use a limiter of three per minute, so a budget is
// spent in four requests; `/auth/login` answers 303 while one lasts.
// ------------------------------------------------------------------------

const PER_WINDOW: u32 = 3;
const SECOND_INGRESS: &str = "10.42.0.18";

fn ingress_range() -> Arc<logweir_api::trusted_proxy::TrustedProxies> {
    Arc::new(logweir_api::trusted_proxy::TrustedProxies::from_cidrs(
        vec![Cidr::parse("10.42.0.0/16").unwrap()],
    ))
}

fn limited_app(
    proxies: Arc<logweir_api::trusted_proxy::TrustedProxies>,
    per_window: u32,
) -> SharedApp {
    app_with_limiter(
        proxies,
        logweir_api::auth::ratelimit::RateLimiter::new(
            std::time::Duration::from_secs(60),
            per_window,
        ),
    )
}

fn app_with_limiter(
    proxies: Arc<logweir_api::trusted_proxy::TrustedProxies>,
    limiter: logweir_api::auth::ratelimit::RateLimiter,
) -> SharedApp {
    SharedApp::new(
        FakeKube::new(),
        support::idp::MockIdp::new(ISSUER, &[]),
        SharedOptions {
            trusted_proxies: Some(proxies),
            login_limiter: Some(limiter),
            ..SharedOptions::default()
        },
    )
}

/// One `GET /auth/login` from `peer`, with `X-Forwarded-For: xff` if given.
async fn sign_in(app: &SharedApp, peer: &str, xff: Option<&str>) -> TestResponse {
    let headers: Vec<(&str, &str)> = xff.map(|v| ("x-forwarded-for", v)).into_iter().collect();
    get(app, "/auth/login", Some(peer), &headers).await
}

/// Spend `PER_WINDOW` sign-ins, each served, then show the next is refused.
async fn spend(app: &SharedApp, peer: &str, xffs: &[Option<&str>]) {
    assert_eq!(xffs.len(), PER_WINDOW as usize);
    for xff in xffs {
        let served = sign_in(app, peer, *xff).await;
        assert_eq!(served.status, 303, "{peer} {xff:?}: {}", served.text());
    }
}

fn assert_limited(response: &TestResponse) {
    response.assert_problem(429, "rate_limited");
    assert!(
        response.header("retry-after").is_some(),
        "a 429 carries Retry-After"
    );
}

/// **Two clients behind one trusted proxy get separate budgets.** Client A
/// spends its three; client B, through the same ingress pod a moment later, is
/// still served, and the audit says the bucket was the forwarded client's —
/// a basis, never the address as an actor.
///
/// NEGATIVE CONTROL, IN THE TEST: the same requests against a console that
/// trusts no proxy reproduce the defect — B is refused because A spent the
/// ingress's one budget. A limiter that ignored the header fails the first
/// half; the control shows the rows can tell the two apart.
#[tokio::test]
async fn two_clients_behind_one_trusted_proxy_get_separate_budgets() {
    let (log, _guard) = capture();
    let app = limited_app(ingress_range(), PER_WINDOW);
    spend(&app, INGRESS, &[Some("203.0.113.50"); 3]).await;
    let a_again = sign_in(&app, INGRESS, Some("203.0.113.50")).await;
    assert_limited(&a_again);
    let (record, notes) = log.record(&a_again.header("x-request-id").unwrap());
    assert_eq!(notes["loginRateKey"], "forwardedClient");
    assert_eq!(record["forwardedFor"], "203.0.113.50");
    assert_eq!(record["actorId"], "", "a bucket is never an actor");
    assert_eq!(record["failureCode"], "rate_limited");

    let b = sign_in(&app, INGRESS, Some("203.0.113.51")).await;
    assert_eq!(b.status, 303, "{}", b.text());
    let (_, notes) = log.record(&b.header("x-request-id").unwrap());
    assert_eq!(notes["loginRateKey"], "forwardedClient");

    // The defect, reproduced: nobody trusted, so the ingress is a peer like
    // any other and its one budget is everyone's.
    let untrusting = limited_app(
        Arc::new(logweir_api::trusted_proxy::TrustedProxies::from_cidrs(
            Vec::new(),
        )),
        PER_WINDOW,
    );
    spend(&untrusting, INGRESS, &[Some("203.0.113.50"); 3]).await;
    let b = sign_in(&untrusting, INGRESS, Some("203.0.113.51")).await;
    assert_limited(&b);
    let (_, notes) = log.record(&b.header("x-request-id").unwrap());
    assert_eq!(notes["loginRateKey"], "peer");
}

/// **The same client through two trusted proxies shares one budget**, whether
/// the second ingress pod received it directly or behind the first (a chain
/// of two). NEGATIVE CONTROL: another client through the second pod is still
/// served, so the refusal is the client's budget, not the pod's.
#[tokio::test]
async fn the_same_client_through_two_trusted_proxies_shares_one_budget() {
    let (_log, _guard) = capture();
    let app = limited_app(ingress_range(), PER_WINDOW);
    let served = sign_in(&app, INGRESS, Some("203.0.113.50")).await;
    assert_eq!(served.status, 303);
    let served = sign_in(&app, SECOND_INGRESS, Some("203.0.113.50")).await;
    assert_eq!(served.status, 303);
    let served = sign_in(&app, SECOND_INGRESS, Some("203.0.113.50, 10.42.0.17")).await;
    assert_eq!(served.status, 303);
    assert_limited(&sign_in(&app, SECOND_INGRESS, Some("203.0.113.50")).await);
    assert_limited(&sign_in(&app, INGRESS, Some("203.0.113.50")).await);

    let other = sign_in(&app, SECOND_INGRESS, Some("203.0.113.51")).await;
    assert_eq!(other.status, 303, "{}", other.text());
}

/// **A forged `X-Forwarded-For` from an untrusted peer is ignored**: a pod
/// that dials the console directly and invents a new client for every request
/// is still counted as itself, and refused on its fourth. A client of the
/// ingress cannot do it either: the leftmost hops it writes are never read.
/// NEGATIVE CONTROL: another untrusted peer is unaffected, and the same
/// invented chain through the ingress is keyed on the hop the ingress added.
#[tokio::test]
async fn a_forged_header_from_an_untrusted_peer_is_ignored() {
    let (log, _guard) = capture();
    let app = limited_app(ingress_range(), PER_WINDOW);
    spend(
        &app,
        POD_ELSEWHERE,
        &[
            Some("198.51.100.1"),
            Some("198.51.100.2"),
            Some("198.51.100.3"),
        ],
    )
    .await;
    let forged = sign_in(&app, POD_ELSEWHERE, Some("198.51.100.4")).await;
    assert_limited(&forged);
    let (record, notes) = log.record(&forged.header("x-request-id").unwrap());
    assert_eq!(notes["loginRateKey"], "peer");
    assert_eq!(
        record["forwardedFor"], "",
        "an untrusted peer's header is not even recorded"
    );

    let neighbour = sign_in(&app, "10.99.3.5", Some("198.51.100.4")).await;
    assert_eq!(neighbour.status, 303, "{}", neighbour.text());

    // Through the ingress, a client that writes a fresh leftmost hop every
    // time is still the hop the ingress appended.
    spend(
        &app,
        INGRESS,
        &[
            Some("198.51.100.11, 203.0.113.60"),
            Some("198.51.100.12, 203.0.113.60"),
            Some("198.51.100.13, 203.0.113.60"),
        ],
    )
    .await;
    assert_limited(&sign_in(&app, INGRESS, Some("198.51.100.14, 203.0.113.60")).await);
}

/// **A trusted set that is not ready, or is older than its `MAX_AGE`, trusts
/// nobody** — the same answer the entry point's gate gets. Before the Service
/// is read, the ingress pod is a plain peer and its forwarded clients share
/// its budget; once read, a new client has its own (the NEGATIVE CONTROL that
/// shows the header is honoured when it should be); once the read is stale,
/// the next new client is the peer's again and refused.
#[tokio::test]
async fn a_trusted_set_that_is_not_ready_or_too_old_trusts_nobody() {
    let (_log, _guard) = capture();
    let proxies = Arc::new(logweir_api::trusted_proxy::TrustedProxies::new(
        Vec::new(),
        proxy_service(),
    ));
    let app = limited_app(Arc::clone(&proxies), PER_WINDOW);
    let pod = "10.1.0.7";
    assert!(!proxies.ready());
    spend(
        &app,
        pod,
        &[
            Some("203.0.113.70"),
            Some("203.0.113.71"),
            Some("203.0.113.72"),
        ],
    )
    .await;
    assert_limited(&sign_in(&app, pod, Some("203.0.113.73")).await);

    proxies.replace_at([pod.parse().unwrap()], std::time::Instant::now());
    assert!(proxies.ready());
    let fresh = sign_in(&app, pod, Some("203.0.113.74")).await;
    assert_eq!(fresh.status, 303, "{}", fresh.text());

    let old = std::time::Instant::now()
        .checked_sub(logweir_api::trusted_proxy::MAX_AGE + std::time::Duration::from_secs(1))
        .expect("the clock is past the window");
    proxies.replace_at([pod.parse().unwrap()], old);
    assert!(!proxies.ready());
    assert_limited(&sign_in(&app, pod, Some("203.0.113.75")).await);
}

/// **A header with no client hop falls back to the peer.** A chain made only
/// of trusted proxies (the request began inside the proxy tier), a hop that is
/// not an address, and no header at all are all counted against the ingress
/// pod itself, and so share its one budget. NEGATIVE CONTROL: a real client
/// through the same pod is still served, so the refusal is the fallback
/// bucket's and not a global one.
#[tokio::test]
async fn a_chain_with_no_client_hop_falls_back_to_the_peer() {
    let (log, _guard) = capture();
    let app = limited_app(ingress_range(), PER_WINDOW);
    spend(
        &app,
        INGRESS,
        &[Some("10.42.0.9, 10.42.0.17"), Some("not-an-address"), None],
    )
    .await;
    let proxies_only = sign_in(&app, INGRESS, Some("10.42.0.9")).await;
    assert_limited(&proxies_only);
    let (_, notes) = log.record(&proxies_only.header("x-request-id").unwrap());
    assert_eq!(notes["loginRateKey"], "peer");
    assert_limited(&sign_in(&app, INGRESS, Some("203.0.113.80:4711")).await);
    assert_limited(&sign_in(&app, INGRESS, None).await);

    let client = sign_in(&app, INGRESS, Some("203.0.113.81")).await;
    assert_eq!(client.status, 303, "{}", client.text());
}

/// **The bound on tracked keys holds under a spray of forwarded addresses**,
/// on a limiter with no ceiling (the table's own bound). A trusted proxy
/// forwards more distinct clients than the table holds, inside one window:
/// the table stops at `MAX_TRACKED_PEERS` and the next new client is refused
/// rather than tracked, with the audit naming that limit. NEGATIVE CONTROL:
/// the table is FULL, not merely small — every sprayed address took its own
/// key, so a limiter that keyed this spray on the peer (one key) fails the
/// equality.
#[tokio::test]
async fn the_bound_on_tracked_keys_holds_under_a_spray_of_forwarded_addresses() {
    use logweir_api::auth::ratelimit::MAX_TRACKED_PEERS;
    let app = limited_app(ingress_range(), 20);
    let limiter = || &app.app.state.shared().expect("shared mode").login_limiter;
    for i in 0..MAX_TRACKED_PEERS {
        let served = sign_in(&app, INGRESS, Some(&sprayed(i))).await;
        assert_eq!(served.status, 303, "client {i}: {}", served.text());
    }
    assert_eq!(limiter().tracked(), MAX_TRACKED_PEERS);
    let (log, _guard) = capture();
    for i in MAX_TRACKED_PEERS..(MAX_TRACKED_PEERS + 64) {
        let refused = sign_in(&app, INGRESS, Some(&sprayed(i))).await;
        assert_limited(&refused);
        if i == MAX_TRACKED_PEERS {
            let (_, notes) = log.record(&refused.header("x-request-id").unwrap());
            assert_eq!(notes["loginRateLimit"], "trackedKeys");
        }
    }
    assert!(
        limiter().tracked() <= MAX_TRACKED_PEERS,
        "the limiter tracked {} keys",
        limiter().tracked()
    );
}

fn sprayed(i: usize) -> String {
    let [_, _, hi, lo] = (i as u32).to_be_bytes();
    format!("198.18.{hi}.{lo}")
}

/// **The shipped limiter stops the same spray at its ceiling** (the FX-13
/// security review: per-client keys must not unbound the provider's load).
/// Of more distinct forwarded clients than the table holds, exactly
/// `LOGIN_CEILING_PER_WINDOW` are served; every later one is refused `429`
/// with `Retry-After`, the audit naming the CEILING, and the table holds only
/// the served keys. NEGATIVE CONTROL: the previous row's limiter, identical
/// but for the ceiling, serves all `MAX_TRACKED_PEERS` of them.
#[tokio::test]
async fn the_ceiling_bounds_a_spray_of_forwarded_clients() {
    use logweir_api::auth::ratelimit::{RateLimiter, LOGIN_CEILING_PER_WINDOW, MAX_TRACKED_PEERS};
    let app = app_with_limiter(ingress_range(), RateLimiter::for_login());
    let mut served = 0;
    for i in 0..(MAX_TRACKED_PEERS + 64) {
        let response = sign_in(&app, INGRESS, Some(&sprayed(i))).await;
        if response.status == 303 {
            served += 1;
        } else {
            assert_limited(&response);
        }
    }
    assert_eq!(served, LOGIN_CEILING_PER_WINDOW);
    let limiter = &app.app.state.shared().expect("shared mode").login_limiter;
    assert_eq!(limiter.tracked(), LOGIN_CEILING_PER_WINDOW as usize);

    let (log, _guard) = capture();
    let refused = sign_in(&app, INGRESS, Some("203.0.113.90")).await;
    assert_limited(&refused);
    assert!(
        refused.text().contains("to this console"),
        "the ceiling's detail does not blame the address: {}",
        refused.text()
    );
    let (record, notes) = log.record(&refused.header("x-request-id").unwrap());
    assert_eq!(notes["loginRateLimit"], "ceiling");
    assert_eq!(notes["loginRateKey"], "forwardedClient");
    assert_eq!(record["failureCode"], "rate_limited");
}

/// **A client rotating IPv6 addresses inside its `/64` spends one budget**
/// (the FX-13 security review). Three sign-ins from three addresses of one
/// `/64`, through the trusted ingress, spend it; a fourth, fresh address of
/// the same `/64` is refused by the KEY. NEGATIVE CONTROL: a client in the
/// next `/64` over is served, so the refusal is the `/64`'s and not a global
/// one.
#[tokio::test]
async fn a_client_rotating_ipv6_addresses_in_its_64_spends_one_budget() {
    let (log, _guard) = capture();
    let app = limited_app(ingress_range(), PER_WINDOW);
    spend(
        &app,
        INGRESS,
        &[
            Some("2001:db8:aa:1::1"),
            Some("2001:db8:aa:1:8a2e:370:7334:1"),
            Some("2001:db8:aa:1:ffff:ffff:ffff:fffe"),
        ],
    )
    .await;
    let rotated = sign_in(&app, INGRESS, Some("2001:db8:aa:1:dead:beef:0:4")).await;
    assert_limited(&rotated);
    let (_, notes) = log.record(&rotated.header("x-request-id").unwrap());
    assert_eq!(notes["loginRateLimit"], "key");
    assert_eq!(notes["loginRateKey"], "forwardedClient");

    let neighbour = sign_in(&app, INGRESS, Some("2001:db8:aa:2::1")).await;
    assert_eq!(neighbour.status, 303, "{}", neighbour.text());
}
