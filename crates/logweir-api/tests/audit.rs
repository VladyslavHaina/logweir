//! The audit record: what it says, and what it must never say.
//!
//! HOW THIS CAPTURES LOGS. Each test installs a thread-local `tracing`
//! subscriber writing JSON into a buffer, drives the real router, and then
//! reads the lines back. Because `#[tokio::test]` runs a current-thread
//! runtime, the router's spans stay on the thread the guard was installed on.
//! Nothing here reads a file or a global logger, so the tests are independent
//! of each other and of any harness-level subscriber.
//!
//! THE REDACTION TESTS ARE WRITTEN AS MUTANTS. Each drives a request that
//! CARRIES a secret of one class — a session cookie, a synchronizer token, an
//! `Authorization` header, plan bytes, approval bytes, a Kubernetes error
//! holding a JWT-shaped string, an object-store URL with userinfo — and then
//! asserts the WHOLE captured log contains none of it. A test that only checked
//! the audit line would miss the ordinary request log next to it, which is
//! where a leak would actually land.

mod support;

use std::io::Write;
use std::sync::{Arc, Mutex};

use axum::body::Body;
use http::Request;
use logweir_api::authz::Role;
use serde_json::Value;
use support::{
    FakeKube, Fault, SharedApp, SharedOptions, TestResponse, ISSUER, NS_A, SHARED_HOST,
    SHARED_ORIGIN,
};

// ------------------------------------------------------------ log capture

#[derive(Clone, Default)]
struct Buffer(Arc<Mutex<Vec<u8>>>);

struct BufferWriter(Arc<Mutex<Vec<u8>>>);

impl Write for BufferWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("the buffer lock holds")
            .extend_from_slice(data);
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
        String::from_utf8_lossy(&self.0.lock().expect("the buffer lock holds")).into_owned()
    }

    /// Every audit record, parsed.
    fn records(&self) -> Vec<Value> {
        self.text()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|line| line["target"] == "logweir_api::audit")
            .filter_map(|line| {
                line["fields"]["audit"]
                    .as_str()
                    .and_then(|audit| serde_json::from_str::<Value>(audit).ok())
            })
            .collect()
    }

    /// The one record for a request id.
    fn record(&self, audit_id: &str) -> Value {
        let all = self.records();
        let found: Vec<&Value> = all.iter().filter(|r| r["auditId"] == audit_id).collect();
        assert_eq!(
            found.len(),
            1,
            "expected exactly one audit record for {audit_id}, got {}:\n{}",
            found.len(),
            self.text()
        );
        found[0].clone()
    }
}

/// Install a capturing subscriber for this thread.
fn capture() -> (Buffer, tracing::subscriber::DefaultGuard) {
    let buffer = Buffer::default();
    // THE PRODUCTION FILTER, at the most verbose setting an operator can ask
    // for. `logweir_api::audit::log_filter_from` is the same function
    // `src/main.rs` runs with, so a dependency that logs an unredacted upstream
    // body at DEBUG fails these tests rather than waiting for a real incident.
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(buffer.clone())
        .with_env_filter(logweir_api::audit::log_filter_from("debug"))
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);
    (buffer, guard)
}

// ------------------------------------------------------------------ setup

fn app_with(fake: FakeKube) -> SharedApp {
    SharedApp::new(
        fake,
        support::idp::MockIdp::new(ISSUER, &[]),
        SharedOptions {
            bindings: support::RoleBindings {
                revision: "audit-rev-7".into(),
                bindings: vec![
                    support::binding(Role::Viewer, NS_A, &["lw-a-viewers"]),
                    support::binding(Role::Operator, NS_A, &["lw-a-operators"]),
                ],
            },
            ..SharedOptions::default()
        },
    )
}

fn seeded() -> FakeKube {
    let fake = FakeKube::new();
    // A REAL Backup fixture — the same one the status tests use — with its
    // archive URL rewritten to carry userinfo, which is redaction class 7.
    let mut backup = support::fixture("backup-valid-exit0.json");
    backup["metadata"] = serde_json::json!({ "name": "backup-1" });
    backup["spec"]["archive"]["url"] =
        serde_json::json!("s3://archive-user:archive-password@bucket/path");
    fake.seed("backups", NS_A, backup);
    fake
}

fn request_id(response: &TestResponse) -> String {
    response
        .header("x-request-id")
        .expect("every response carries a request id")
}

/// **An allowed read produces one complete, correlated allow record.**
#[tokio::test]
async fn an_allowed_read_is_attributed_completely() {
    let (log, _guard) = capture();
    let app = app_with(seeded());
    let cookie = app.session_cookie("u-op", &["lw-a-operators"]);

    let response = app
        .get("/api/v1/namespaces/team-a/backups/backup-1", &cookie)
        .await;
    assert_eq!(response.status.as_u16(), 200);

    let id = request_id(&response);
    // The audit id IS the request id, and the response says so — that is what
    // makes a user-reported request findable in the log.
    assert_eq!(response.json()["requestId"].as_str(), Some(id.as_str()));
    let record = log.record(&id);

    assert_eq!(record["decision"], "allow");
    assert_eq!(record["authenticationMode"], "oidc");
    assert_eq!(record["actorId"], format!("{ISSUER}#u-op"));
    assert_eq!(record["displayClaim"], "u-op display");
    assert_eq!(record["bindingRevision"], "audit-rev-7");
    assert_eq!(record["roles"], serde_json::json!(["operator"]));
    assert_eq!(record["namespace"], "team-a");
    assert_eq!(record["action"], "backup.read");
    assert_eq!(record["resource"], "Backup/backup-1");
    assert_eq!(record["objectName"], "backup-1");
    assert_eq!(record["method"], "GET");
    assert_eq!(record["path"], "/api/v1/namespaces/team-a/backups/backup-1");
    assert_eq!(record["httpStatus"], 200);
    assert_eq!(record["failureCode"], "");
    assert!(record["latencyMs"].is_number());
    assert!(
        record["sessionIdHash"]
            .as_str()
            .unwrap()
            .starts_with("sha256:"),
        "the session id must be hashed"
    );
    assert!(!record["objectUid"].as_str().unwrap().is_empty());
    assert!(!record["objectResourceVersion"].as_str().unwrap().is_empty());

    // The session id itself is nowhere in the log.
    assert!(
        !log.text().contains("sid-u-op"),
        "the session id leaked:\n{}",
        log.text()
    );
}

/// **A denied mutation is a deny record naming the failure, the roles the actor
/// did hold, and the namespace — and the cluster is untouched.**
#[tokio::test]
async fn a_denied_mutation_is_recorded_as_a_deny() {
    let (log, _guard) = capture();
    let fake = FakeKube::new();
    let app = app_with(fake.clone());
    let cookie = app.session_cookie("u-view", &["lw-a-viewers"]);

    let response = app
        .post(
            "/api/v1/namespaces/team-a/schedules",
            &cookie,
            Some(&app.csrf_for("u-view")),
            Some("audit-denied-000001"),
            &support::schedule_body().to_string(),
        )
        .await;
    response.assert_problem(403, "forbidden");
    assert_eq!(fake.count("backupschedules", NS_A), 0);

    let record = log.record(&request_id(&response));
    assert_eq!(record["decision"], "deny");
    assert_eq!(record["failureCode"], "forbidden");
    assert_eq!(record["action"], "schedule.create");
    assert_eq!(record["namespace"], "team-a");
    assert_eq!(record["roles"], serde_json::json!(["viewer"]));
    assert_eq!(record["httpStatus"], 403);
    assert_eq!(record["objectName"], "");
}

/// **The audit record keeps the real reason even when the response hides it.**
///
/// The response for an unbound namespace is 404 `not_found`, by design, so a
/// caller cannot enumerate namespaces. The log must still say
/// `namespace_forbidden`, or the enumeration defence would have cost the
/// operator the ability to see an authorization problem.
#[tokio::test]
async fn an_enumeration_resistant_404_still_records_the_real_reason() {
    let (log, _guard) = capture();
    let app = app_with(FakeKube::new());
    let cookie = app.session_cookie("u-op", &["lw-a-operators"]);

    let response = app
        .get("/api/v1/namespaces/team-b/backups/backup-1", &cookie)
        .await;
    response.assert_problem(404, "not_found");

    let record = log.record(&request_id(&response));
    assert_eq!(record["decision"], "deny");
    assert_eq!(
        record["failureCode"], "namespace_forbidden",
        "the log must carry the reason the response withholds"
    );
    assert_eq!(record["namespace"], "team-b");
    assert_eq!(record["action"], "backup.read");
    assert_eq!(record["roles"], serde_json::json!([]));
    assert_eq!(record["httpStatus"], 404);
}

/// **A create and its replay correlate to the same object, through the same
/// hashes, with the idempotency key itself nowhere in sight.**
#[tokio::test]
async fn a_create_and_its_replay_correlate() {
    let (log, _guard) = capture();
    let fake = FakeKube::new();
    let app = app_with(fake.clone());
    let cookie = app.session_cookie("u-op", &["lw-a-operators"]);
    let csrf = app.csrf_for("u-op");
    let key = "audit-replay-tHeRaWkEy";

    let first = app
        .post(
            "/api/v1/namespaces/team-a/schedules",
            &cookie,
            Some(&csrf),
            Some(key),
            &support::schedule_body().to_string(),
        )
        .await;
    assert_eq!(first.status.as_u16(), 201, "{}", first.code());
    let second = app
        .post(
            "/api/v1/namespaces/team-a/schedules",
            &cookie,
            Some(&csrf),
            Some(key),
            &support::schedule_body().to_string(),
        )
        .await;
    assert_eq!(second.status.as_u16(), 200);

    let a = log.record(&request_id(&first));
    let b = log.record(&request_id(&second));
    assert_eq!(a["decision"], "allow");
    assert_eq!(b["decision"], "allow");
    assert_eq!(a["action"], "schedule.create");
    assert_eq!(a["objectUid"], b["objectUid"], "one key, one object");
    assert_eq!(a["objectName"], b["objectName"]);
    assert_eq!(a["idempotencyKeyHash"], b["idempotencyKeyHash"]);
    assert_eq!(a["requestHash"], b["requestHash"]);
    assert!(a["idempotencyKeyHash"]
        .as_str()
        .unwrap()
        .starts_with("sha256:"));
    assert!(a["requestHash"].as_str().unwrap().starts_with("sha256:"));
    assert_eq!(a["httpStatus"], 201);
    assert_eq!(b["httpStatus"], 200);

    // THE RAW KEY IS NEVER LOGGED. Only its scope hash is.
    assert!(
        !log.text().contains(key),
        "the raw Idempotency-Key leaked:\n{}",
        log.text()
    );
}

/// **A sign-in and its failures are audited without any of the values that made
/// them work or fail.**
#[tokio::test]
async fn the_sign_in_routes_are_audited_without_their_secrets() {
    let (log, _guard) = capture();
    let key = support::idp::TestKey::ec("k-ec-1");
    let idp = support::idp::MockIdp::new(ISSUER, &[&key]);
    let app = SharedApp::new(
        FakeKube::new(),
        idp.clone(),
        SharedOptions {
            bindings: support::default_bindings(),
            ..SharedOptions::default()
        },
    );

    let start = app
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
    assert_eq!(start.status.as_u16(), 303);
    let login_record = log.record(&request_id(&start));
    assert_eq!(login_record["action"], "auth.login");
    assert_eq!(login_record["decision"], "allow");
    assert_eq!(login_record["httpStatus"], 303);

    let location = start.header("location").unwrap();
    let query: std::collections::BTreeMap<String, String> =
        serde_urlencoded::from_str(location.split_once('?').unwrap().1).unwrap();
    let login_cookie = start
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

    let now = chrono::DateTime::parse_from_rfc3339("2026-09-15T12:00:00Z")
        .unwrap()
        .timestamp();
    idp.grant(
        "the-authorization-code-itself",
        support::idp::Grant {
            id_token: key.mint(&serde_json::json!({
                "iss": ISSUER,
                "sub": "u-ada",
                "aud": support::CLIENT_ID,
                "exp": now + 300,
                "iat": now,
                "nonce": query["nonce"],
                "name": "Ada Lovelace",
                "groups": ["lw-a-operators"],
            })),
            code_challenge: None,
            redirect_uri: None,
        },
    );

    let done = app
        .app
        .send(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/auth/callback?code=the-authorization-code-itself&state={}",
                    query["state"]
                ))
                .header("host", SHARED_HOST)
                .header("cookie", &login_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(done.status.as_u16(), 303);
    let callback = log.record(&request_id(&done));
    assert_eq!(callback["action"], "auth.callback");
    assert_eq!(callback["decision"], "allow");
    assert_eq!(callback["actorId"], format!("{ISSUER}#u-ada"));
    assert_eq!(callback["displayClaim"], "Ada Lovelace");
    assert!(callback["sessionIdHash"]
        .as_str()
        .unwrap()
        .starts_with("sha256:"));

    // NOTHING THE FLOW DEPENDED ON IS IN THE LOG.
    let text = log.text();
    for secret in [
        "the-authorization-code-itself",
        query["state"].as_str(),
        query["nonce"].as_str(),
        query["code_challenge"].as_str(),
        "a-client-secret",
        "an-access-token-the-api-must-never-keep",
        "a-refresh-token-the-api-must-never-keep",
        "eyJ",
    ] {
        assert!(
            !text.contains(secret),
            "`{secret}` reached the log:\n{text}"
        );
    }

    // And a refused sign-in records its own reason, by code, not by value.
    let bad = app
        .app
        .send(
            Request::builder()
                .method("GET")
                .uri("/auth/callback?code=never-granted&state=not-a-state")
                .header("host", SHARED_HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(bad.status.as_u16(), 401);
    let refused = log.record(&request_id(&bad));
    assert_eq!(refused["action"], "auth.callback");
    assert_eq!(refused["decision"], "deny");
    assert_eq!(refused["failureCode"], "login_state_absent");
    assert!(!log.text().contains("not-a-state"));
}

/// **Identity-shaped headers are recorded by NAME and never by value.**
#[tokio::test]
async fn ignored_identity_headers_are_recorded_by_name_only() {
    let (log, _guard) = capture();
    let app = app_with(seeded());
    let cookie = app.session_cookie("u-view", &["lw-a-viewers"]);

    let response = app
        .app
        .send(
            Request::builder()
                .method("GET")
                .uri("/api/v1/session")
                .header("host", SHARED_HOST)
                .header("cookie", &cookie)
                .header("x-remote-user", "cluster-admin-impostor")
                .header("x-forwarded-user", "another-impostor")
                .header("x-auth-request-email", "impostor@example.test")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(response.status.as_u16(), 200);

    let record = log.record(&request_id(&response));
    let names: Vec<&str> = record["ignoredIdentityHeaders"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        vec!["x-auth-request-email", "x-forwarded-user", "x-remote-user"]
    );
    let text = log.text();
    for value in [
        "cluster-admin-impostor",
        "another-impostor",
        "impostor@example.test",
    ] {
        assert!(!text.contains(value), "`{value}` reached the log:\n{text}");
    }
}

/// **The seven redaction classes D0 names, each carried by a real request.**
///
/// Each item below is a planted secret: if the corresponding redaction were
/// removed, the assertion at the end of this test would fail with the secret
/// printed in the message. That is what makes these mutants rather than
/// wishes.
#[tokio::test]
async fn nothing_from_seven_secret_classes_reaches_the_log() {
    let (log, _guard) = capture();
    let fake = seeded();
    // A REAL Approval, with its two document fields replaced by recognisable
    // strings: those are redaction class 4.
    let mut approval = support::fixture("approvals-selfattested.json")["items"][0].clone();
    approval["metadata"] = serde_json::json!({ "name": "approval-1" });
    approval["spec"]["approvalBytes"] = serde_json::json!("THE-APPROVAL-DOCUMENT-BYTES");
    approval["spec"]["sidecarBytes"] = serde_json::json!("THE-DSSE-SIDECAR-BYTES");
    fake.seed("approvals", NS_A, approval);
    let app = app_with(fake.clone());
    let cookie = app.session_cookie("u-op", &["lw-a-operators"]);
    let csrf = app.csrf_for("u-op");
    let plan = support::golden_plan();

    // 1 + 2. A session cookie and a synchronizer token on every request below,
    //        plus an Authorization header nobody asked for.
    // 3. A password-shaped query parameter.
    let _ = app
        .app
        .send(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/team-a/backups?password=hunter2-the-password")
                .header("host", SHARED_HOST)
                .header("cookie", &cookie)
                .header(
                    "authorization",
                    "Bearer eyJhbGciOiJSUzI1NiJ9.THE-BEARER-TOKEN.sig",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await;

    // 4. Approval and sidecar bytes, through the one route that serves them.
    let packet = app
        .get(
            "/api/v1/namespaces/team-a/approvals/approval-1/packet",
            &cookie,
        )
        .await;
    assert_eq!(packet.status.as_u16(), 200, "{}", packet.code());

    // 5. Plan bytes, submitted to the one route that takes them.
    let created = app
        .post(
            "/api/v1/namespaces/team-a/restores",
            &cookie,
            Some(&csrf),
            Some("redaction-plan-00001"),
            &support::restore_body(&plan).to_string(),
        )
        .await;
    assert_eq!(created.status.as_u16(), 201, "{}", created.code());
    let create_record = log.record(&request_id(&created));
    assert!(
        create_record["planHash"]
            .as_str()
            .unwrap()
            .starts_with("sha256:"),
        "the plan HASH belongs in the record"
    );

    // 6. A raw Kubernetes error. The fake's injected failure body deliberately
    //    carries a JWT-shaped token.
    fake.inject(Fault {
        method: "GET",
        path_contains: "/backups".to_string(),
        status: 500,
        reason: "InternalError",
        delay: None,
        remaining: 1,
    });
    let failed = app.get("/api/v1/namespaces/team-a/backups", &cookie).await;
    assert!(failed.status.as_u16() >= 500);

    // 7. An object-store URL carrying userinfo, read back through a projection.
    let projected = app
        .get("/api/v1/namespaces/team-a/backups/backup-1", &cookie)
        .await;
    assert_eq!(projected.status.as_u16(), 200);
    assert_eq!(
        projected.json()["item"]["archive"]["url"],
        "s3://redacted@bucket/path",
        "the projection redacts userinfo"
    );

    let text = log.text();
    let planted = [
        ("a session cookie", cookie.split_once('=').unwrap().1),
        ("a synchronizer token", csrf.as_str()),
        ("a bearer token", "THE-BEARER-TOKEN"),
        ("a password", "hunter2-the-password"),
        ("approval document bytes", "THE-APPROVAL-DOCUMENT-BYTES"),
        ("sidecar bytes", "THE-DSSE-SIDECAR-BYTES"),
        ("plan bytes", plan.lines().next().unwrap()),
        (
            "a Kubernetes error's token",
            "eyJhbGciOiJIUzI1NiJ9abcdefghijklmnop",
        ),
        ("object-store userinfo", "archive-user:archive-password"),
    ];
    for (label, secret) in planted {
        assert!(
            !text.contains(secret),
            "{label} reached the log (`{secret}`):\n{text}"
        );
    }
    // The query string as a whole never appears either.
    assert!(
        !text.contains("password="),
        "a query string reached the log:\n{text}"
    );
}

/// **Every request produces exactly one audit record, including the ones that
/// never reach a handler.**
#[tokio::test]
async fn every_request_produces_exactly_one_record() {
    let (log, _guard) = capture();
    let app = app_with(seeded());
    let cookie = app.session_cookie("u-op", &["lw-a-operators"]);

    let probes: Vec<TestResponse> = vec![
        app.get("/api/v1/session", &cookie).await,
        app.get("/healthz", &cookie).await,
        app.get("/ui/index.html", &cookie).await,
        app.get("/apis/logweir.dev/v1alpha1/backups", &cookie).await,
        app.app
            .send(
                Request::builder()
                    .method("GET")
                    .uri("/api/v1/session")
                    .header("host", "evil.test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await,
        app.app
            .send(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/namespaces/team-a/schedules")
                    .header("host", SHARED_HOST)
                    .header("origin", SHARED_ORIGIN)
                    .header("content-type", "application/json")
                    .header("cookie", &cookie)
                    .header("impersonate-user", "root")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await,
    ];

    let ids: Vec<String> = probes.iter().map(request_id).collect();
    assert_eq!(
        log.records().len(),
        ids.len(),
        "one record per request:\n{}",
        log.text()
    );
    for (response, id) in probes.iter().zip(&ids) {
        let record = log.record(id);
        assert_eq!(record["httpStatus"], response.status.as_u16());
        assert!(!record["method"].as_str().unwrap().is_empty());
        assert!(
            record["decision"] == "allow" || record["decision"] == "deny",
            "a record must decide: {record}"
        );
        if response.status.is_client_error() || response.status.is_server_error() {
            assert_eq!(record["decision"], "deny");
            assert!(!record["failureCode"].as_str().unwrap().is_empty());
        }
    }
    // The refusals name their own reasons.
    assert_eq!(log.record(&ids[4])["failureCode"], "http_421");
    assert_eq!(log.record(&ids[5])["failureCode"], "http_400");
}

/// **A dependency cannot print an unredacted upstream body, even at
/// `RUST_LOG=debug`.**
///
/// This is the regression reason for `audit::SILENCED_TARGETS`, recorded as a
/// test rather than as a comment: while writing `nothing_from_seven_secret_
/// classes_reaches_the_log` the JWT-shaped string planted in the fake's failure
/// body appeared in the log — not from this crate, which redacts it, but from
/// `kube_client`'s own `Unsuccessful: ErrorResponse { .. }` line at DEBUG. The
/// redaction was working; the dependency was logging around it.
#[tokio::test]
async fn a_dependencys_debug_logging_cannot_print_the_upstream_body() {
    let (log, _guard) = capture();
    let fake = seeded();
    let app = app_with(fake.clone());
    let cookie = app.session_cookie("u-op", &["lw-a-operators"]);

    fake.inject(Fault {
        method: "GET",
        path_contains: "/backups".to_string(),
        status: 500,
        reason: "InternalError",
        delay: None,
        remaining: 1,
    });
    let failed = app.get("/api/v1/namespaces/team-a/backups", &cookie).await;
    assert_eq!(failed.status.as_u16(), 503);
    assert_eq!(failed.code(), "kubernetes_unavailable");
    // The response says nothing about the upstream message either.
    assert!(!String::from_utf8_lossy(&failed.body).contains("injected failure"));

    let text = log.text();
    assert!(
        text.contains("logweir_api::"),
        "the capture caught nothing at all:\n{text}"
    );
    for target in logweir_api::audit::SILENCED_TARGETS {
        assert!(
            !text.contains(&format!("\"target\":\"{target}")),
            "{target} logged below warn:\n{text}"
        );
    }
    assert!(!text.contains("eyJhbGciOiJIUzI1NiJ9"), "{text}");
    // And this crate DID log the failure, redacted — the silencing is not
    // hiding the diagnosis, only the credential.
    assert!(
        text.contains("[redacted-token]"),
        "the adapter's own redacted line is missing:\n{text}"
    );
}

/// **A forwarded client address is recorded only when the immediate peer is a
/// configured trusted proxy — and it changes no decision either way.**
///
/// D0 allows forwarded values for transport logging and nothing else. The two
/// halves here are the same request from two different peers, so the only
/// difference is the trust decision.
#[tokio::test]
async fn a_forwarded_address_is_recorded_only_for_a_trusted_peer() {
    use logweir_api::config::Cidr;
    use logweir_api::http::PeerAddr;

    async fn probe(peer: [u8; 4], trusted: Vec<Cidr>) -> (Value, String) {
        let (log, _guard) = capture();
        let app = SharedApp::new(
            seeded(),
            support::idp::MockIdp::new(ISSUER, &[]),
            SharedOptions {
                bindings: support::RoleBindings {
                    revision: "fwd".into(),
                    bindings: vec![support::binding(Role::Operator, NS_A, &["lw-a-operators"])],
                },
                trusted_proxy_cidrs: trusted,
                ..SharedOptions::default()
            },
        );
        let cookie = app.session_cookie("u-op", &["lw-a-operators"]);
        let mut request = Request::builder()
            .method("GET")
            .uri("/api/v1/session")
            .header("host", SHARED_HOST)
            .header("cookie", &cookie)
            .header("x-forwarded-for", "203.0.113.9, 198.51.100.2")
            .header("x-forwarded-proto", "http")
            .body(Body::empty())
            .unwrap();
        request
            .extensions_mut()
            .insert(PeerAddr(std::net::IpAddr::from(peer)));
        let response = app.app.send(request).await;
        assert_eq!(response.status.as_u16(), 200);
        let actor = response.json()["actor"]["id"].as_str().unwrap().to_string();
        (log.record(&request_id(&response)), actor)
    }

    let trusted = vec![Cidr::parse("10.0.0.0/8").expect("a constant CIDR")];
    let (recorded, actor_a) = probe([10, 1, 2, 3], trusted.clone()).await;
    assert_eq!(recorded["peer"], "10.1.2.3");
    assert_eq!(
        recorded["forwardedFor"], "203.0.113.9",
        "the first hop of a trusted proxy's header is the transport fact worth logging"
    );

    let (ignored, actor_b) = probe([203, 0, 113, 200], trusted).await;
    assert_eq!(ignored["peer"], "203.0.113.200");
    assert_eq!(
        ignored["forwardedFor"], "",
        "an untrusted peer's X-Forwarded-For must not be recorded at all"
    );

    // AND NEITHER CHANGED THE ACTOR. The header is transport logging; it is not
    // an input to anything.
    assert_eq!(actor_a, actor_b);
    assert_eq!(actor_a, format!("{ISSUER}#u-op"));
}
