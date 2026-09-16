//! The browser trust boundary of shared mode: sessions, the synchronizer CSRF
//! token, `Origin`, `Host`, forged identity headers, CORS and the cookie flags.
//!
//! WHAT THIS FILE IS FOR. Every property here is one a reviewer would otherwise
//! have to take on trust from a paragraph. The release-failure list in D0 names
//! four of them in as many words — "an identity header affects auth", "unsafe
//! method succeeds without exact Origin+CSRF", "an actor crosses an unbound
//! namespace", "credentials enter responses or logs" — and a claim of that kind
//! is worth exactly as much as the test that would fail without it.

mod support;

use axum::body::Body;
use http::Request;
use support::{
    FakeKube, SharedApp, SharedOptions, TestResponse, ISSUER, NS_A, SHARED_HOST, SHARED_ORIGIN,
};

fn app() -> SharedApp {
    SharedApp::new(
        FakeKube::new(),
        support::idp::MockIdp::new(ISSUER, &[]),
        SharedOptions {
            bindings: support::default_bindings(),
            ..SharedOptions::default()
        },
    )
}

/// A raw request builder with a `Host` and optional extras.
struct Raw {
    method: &'static str,
    uri: String,
    headers: Vec<(String, String)>,
    body: String,
}

impl Raw {
    fn get(uri: &str) -> Self {
        Self {
            method: "GET",
            uri: uri.to_string(),
            headers: vec![("host".into(), SHARED_HOST.into())],
            body: String::new(),
        }
    }

    fn post(uri: &str) -> Self {
        Self {
            method: "POST",
            uri: uri.to_string(),
            headers: vec![
                ("host".into(), SHARED_HOST.into()),
                ("origin".into(), SHARED_ORIGIN.into()),
                ("content-type".into(), "application/json".into()),
            ],
            body: "{}".into(),
        }
    }

    fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }

    fn replace(mut self, name: &str, value: &str) -> Self {
        self.headers.retain(|(n, _)| n != name);
        self.header(name, value)
    }

    fn body(mut self, body: &str) -> Self {
        self.body = body.to_string();
        self
    }

    async fn send(self, app: &SharedApp) -> TestResponse {
        let mut builder = Request::builder().method(self.method).uri(&self.uri);
        for (name, value) in &self.headers {
            builder = builder.header(name, value);
        }
        app.app
            .send(builder.body(Body::from(self.body)).unwrap())
            .await
    }
}

const SCHEDULE: &str = "/api/v1/namespaces/team-a/schedules";

fn schedule_body() -> String {
    support::schedule_body().to_string()
}

/// **No session, no API.**
#[tokio::test]
async fn an_api_request_without_a_session_is_unauthenticated() {
    let app = app();
    for path in [
        "/api/v1/session",
        "/api/v1/namespaces",
        "/api/v1/namespaces/team-a/schedules",
        "/api/v1/namespaces/team-a/backups/anything",
    ] {
        let response = Raw::get(path).send(&app).await;
        response.assert_problem(401, "unauthenticated");
    }
    // A mutation without a session is refused too, and creates nothing.
    let response = Raw::post(SCHEDULE)
        .header("idempotency-key", "no-session-0001")
        .body(&schedule_body())
        .send(&app)
        .await;
    assert_eq!(response.status.as_u16(), 401);
    assert_eq!(app.app.fake.count("backupschedules", NS_A), 0);
    app.app.fake.assert_strict();
}

/// **A session past its signed expiry, and a session sealed under a key version
/// this process no longer holds, are both `session_expired`.**
#[tokio::test]
async fn an_expired_session_and_a_rotated_key_are_both_refused() {
    let app = app();
    let cookie = app.session_cookie("u-op", &["lw-a-operators"]);
    assert_eq!(
        app.get("/api/v1/session", &cookie).await.status.as_u16(),
        200
    );

    app.app.clock.advance(901);
    let expired = app.get("/api/v1/session", &cookie).await;
    expired.assert_problem(401, "session_expired");

    // A mutation with the expired session creates nothing.
    let refused = Raw::post(SCHEDULE)
        .header("cookie", &cookie)
        .header("x-csrf-token", &app.csrf_for("u-op"))
        .header("idempotency-key", "expired-session-01")
        .body(&schedule_body())
        .send(&app)
        .await;
    assert_eq!(refused.status.as_u16(), 401);
    assert_eq!(app.app.fake.count("backupschedules", NS_A), 0);

    // A second process holding key version 2 refuses the version-1 cookie.
    let rotated = SharedApp::new(
        FakeKube::new(),
        support::idp::MockIdp::new(ISSUER, &[]),
        SharedOptions {
            bindings: support::default_bindings(),
            key_version: 2,
            ..SharedOptions::default()
        },
    );
    rotated
        .get("/api/v1/session", &cookie)
        .await
        .assert_problem(401, "session_expired");

    // And a process holding a DIFFERENT key of the same version does too.
    let other_key = SharedApp::new(
        FakeKube::new(),
        support::idp::MockIdp::new(ISSUER, &[]),
        SharedOptions {
            bindings: support::default_bindings(),
            key_bytes: vec![0x01; 32],
            ..SharedOptions::default()
        },
    );
    other_key
        .get("/api/v1/session", &cookie)
        .await
        .assert_problem(401, "session_expired");
}

/// **A restarted process reads the sessions the first one issued, because the
/// key is on disk and the session is stateless.**
#[tokio::test]
async fn a_restart_keeps_live_sessions_and_their_csrf_tokens() {
    let fake = FakeKube::new();
    let first = SharedApp::new(
        fake.clone(),
        support::idp::MockIdp::new(ISSUER, &[]),
        SharedOptions {
            bindings: support::default_bindings(),
            ..SharedOptions::default()
        },
    );
    let cookie = first.session_cookie("u-op", &["lw-a-operators"]);
    let token = first.get("/api/v1/session", &cookie).await.json()["csrfToken"]
        .as_str()
        .unwrap()
        .to_string();

    let second = SharedApp::with_clock(
        fake,
        support::idp::MockIdp::new(ISSUER, &[]),
        SharedOptions {
            bindings: support::default_bindings(),
            ..SharedOptions::default()
        },
        first.app.clock.clone(),
    );
    let after = second.get("/api/v1/session", &cookie).await;
    assert_eq!(after.status.as_u16(), 200);
    assert_eq!(after.json()["csrfToken"].as_str(), Some(token.as_str()));

    // And the token still authorises a mutation on the restarted process.
    let created = Raw::post(SCHEDULE)
        .header("cookie", &cookie)
        .header("x-csrf-token", &token)
        .header("idempotency-key", "after-restart-0001")
        .body(&schedule_body())
        .send(&second)
        .await;
    assert_eq!(created.status.as_u16(), 201, "{}", created.code());
}

/// **Every unsafe method needs the exact `Origin`, a JSON content type and this
/// session's synchronizer token — and nothing is created when one is missing.**
#[tokio::test]
async fn an_unsafe_method_needs_origin_content_type_and_the_synchronizer_token() {
    let app = app();
    let cookie = app.session_cookie("u-op", &["lw-a-operators"]);
    let token = app.csrf_for("u-op");
    let other = app.keys.csrf_token("sid-someone-else");

    let refusals: Vec<(&str, Raw, u16, &str)> = vec![
        (
            "no CSRF token",
            Raw::post(SCHEDULE).header("cookie", &cookie),
            403,
            "forbidden",
        ),
        (
            "another session's token",
            Raw::post(SCHEDULE)
                .header("cookie", &cookie)
                .header("x-csrf-token", &other),
            403,
            "forbidden",
        ),
        (
            "an empty token",
            Raw::post(SCHEDULE)
                .header("cookie", &cookie)
                .header("x-csrf-token", ""),
            403,
            "forbidden",
        ),
        (
            "a cross-origin fetch",
            Raw::post(SCHEDULE)
                .replace("origin", "https://evil.test")
                .header("cookie", &cookie)
                .header("x-csrf-token", &token),
            403,
            "origin_mismatch",
        ),
        (
            "no Origin at all",
            {
                let mut raw = Raw::post(SCHEDULE);
                raw.headers.retain(|(n, _)| n != "origin");
                raw.header("cookie", &cookie).header("x-csrf-token", &token)
            },
            403,
            "origin_mismatch",
        ),
        (
            "a cross-site HTML form post",
            Raw::post(SCHEDULE)
                .replace("content-type", "application/x-www-form-urlencoded")
                .replace("origin", "https://evil.test")
                .header("cookie", &cookie),
            403,
            "origin_mismatch",
        ),
        (
            "a same-origin form post",
            Raw::post(SCHEDULE)
                .replace("content-type", "multipart/form-data; boundary=x")
                .header("cookie", &cookie)
                .header("x-csrf-token", &token),
            415,
            "unsupported_media_type",
        ),
    ];

    for (label, raw, status, code) in refusals {
        let response = raw
            .header("idempotency-key", "csrf-refusal-000001")
            .body(&schedule_body())
            .send(&app)
            .await;
        assert_eq!(
            (response.status.as_u16(), response.code().as_str()),
            (status, code),
            "{label}"
        );
        assert_eq!(
            app.app.fake.count("backupschedules", NS_A),
            0,
            "{label} created an object"
        );
    }

    // The same request WITH everything right does create one — so the refusals
    // above are the checks, not a broken route.
    let created = Raw::post(SCHEDULE)
        .header("cookie", &cookie)
        .header("x-csrf-token", &token)
        .header("idempotency-key", "csrf-accepted-00001")
        .body(&schedule_body())
        .send(&app)
        .await;
    assert_eq!(created.status.as_u16(), 201, "{}", created.code());
    assert_eq!(app.app.fake.count("backupschedules", NS_A), 1);
    app.app.fake.assert_strict();
}

/// **Forged identity headers change nothing, are stripped, and are refused
/// outright when they ask for impersonation.**
#[tokio::test]
async fn forged_identity_and_impersonation_headers_have_no_effect() {
    let app = app();
    // A VIEWER in team-a, claiming to be an administrator in every header an
    // authenticating proxy would set.
    let viewer = app.session_cookie("u-view", &["lw-a-viewers"]);
    let forged = [
        ("x-remote-user", "u-admin"),
        ("x-remote-group", "lw-a-admins"),
        ("x-remote-groups", "lw-a-admins"),
        ("x-remote-extra-scopes", "everything"),
        ("x-forwarded-user", "u-admin"),
        ("x-forwarded-email", "admin@example.test"),
        ("x-forwarded-groups", "lw-a-admins"),
        ("x-forwarded-preferred-username", "u-admin"),
        ("x-auth-request-user", "u-admin"),
        ("x-auth-request-email", "admin@example.test"),
        ("x-auth-request-groups", "lw-a-admins"),
    ];

    let mut raw = Raw::get("/api/v1/session").header("cookie", &viewer);
    for (name, value) in forged {
        raw = raw.header(name, value);
    }
    let response = raw.send(&app).await;
    assert_eq!(response.status.as_u16(), 200);
    let body = response.json();
    assert_eq!(body["actor"]["id"], format!("{ISSUER}#u-view"));
    assert_eq!(body["namespaces"][0]["roles"][0], "viewer");
    assert_eq!(
        body["namespaces"][0]["capabilities"]["scheduleCreate"],
        false
    );
    let text = String::from_utf8_lossy(&response.body);
    assert!(
        !text.contains("u-admin"),
        "a forged header reached the body"
    );

    // And the viewer still cannot mutate, however hard the headers insist.
    let mut raw = Raw::post(SCHEDULE)
        .header("cookie", &viewer)
        .header("x-csrf-token", &app.csrf_for("u-view"))
        .header("idempotency-key", "forged-headers-0001")
        .body(&schedule_body());
    for (name, value) in forged {
        raw = raw.header(name, value);
    }
    let response = raw.send(&app).await;
    response.assert_problem(403, "forbidden");
    assert_eq!(app.app.fake.count("backupschedules", NS_A), 0);

    // Impersonation is not ignored, it is refused, and the header is named.
    for header in ["impersonate-user", "impersonate-group", "impersonate-uid"] {
        let response = Raw::get("/api/v1/session")
            .header("cookie", &viewer)
            .header(header, "cluster-admin")
            .send(&app)
            .await;
        response.assert_problem(400, "header_not_allowed");
        assert_eq!(response.json()["errors"][0]["field"], header);
    }
    app.app.fake.assert_strict();
}

/// **A `Host` this listener does not serve is 421, and nothing derives a
/// callback URL or a decision from it.**
#[tokio::test]
async fn host_poisoning_is_refused_and_moves_no_redirect() {
    let app = app();
    let cookie = app.session_cookie("u-op", &["lw-a-operators"]);
    for host in [
        "evil.test",
        "console.test.evil.test",
        "console.test:8443",
        "127.0.0.1",
    ] {
        let response = Raw::get("/api/v1/session")
            .replace("host", host)
            .header("cookie", &cookie)
            .send(&app)
            .await;
        response.assert_problem(421, "misdirected_request");
    }

    // The authorization redirect names the CONFIGURED redirect URI even when
    // the request arrives with a forwarded-host family that says otherwise.
    let response = Raw::get("/auth/login")
        .header("x-forwarded-host", "evil.test")
        .header("x-forwarded-proto", "http")
        .header("forwarded", "host=evil.test;proto=http")
        .send(&app)
        .await;
    // No provider is configured with keys here, but discovery still answers.
    assert!(
        response.status.as_u16() == 303 || response.status.as_u16() == 503,
        "{}",
        response.status
    );
    if let Some(location) = response.header("location") {
        let query: std::collections::BTreeMap<String, String> =
            serde_urlencoded::from_str(location.split_once('?').unwrap().1).unwrap();
        assert_eq!(
            query["redirect_uri"],
            support::REDIRECT_URI,
            "the callback URL came from a header instead of publicBaseUrl"
        );
        assert!(!location.contains("evil.test"), "{location}");
        assert!(location.starts_with(support::ISSUER), "{location}");
    }
}

/// **No response enables credentialed CORS, on any route, for any method.**
#[tokio::test]
async fn no_response_enables_credentialed_cors() {
    let app = app();
    let cookie = app.session_cookie("u-op", &["lw-a-operators"]);
    let probes: Vec<TestResponse> = vec![
        Raw::get("/api/v1/session")
            .header("cookie", &cookie)
            .send(&app)
            .await,
        Raw::get("/api/v1/session")
            .header("cookie", &cookie)
            .header("origin", "https://evil.test")
            .send(&app)
            .await,
        Raw::get("/healthz").send(&app).await,
        Raw::get("/ui/index.html").send(&app).await,
        {
            let mut raw = Raw::get("/api/v1/namespaces");
            raw.method = "OPTIONS";
            raw.header("origin", "https://evil.test")
                .header("access-control-request-method", "POST")
                .header("access-control-request-headers", "x-csrf-token")
                .send(&app)
                .await
        },
        Raw::post(SCHEDULE)
            .replace("origin", "https://evil.test")
            .header("cookie", &cookie)
            .body(&schedule_body())
            .send(&app)
            .await,
    ];
    for response in &probes {
        for header in [
            "access-control-allow-origin",
            "access-control-allow-credentials",
            "access-control-allow-headers",
            "access-control-allow-methods",
            "access-control-expose-headers",
        ] {
            assert_eq!(
                response.header(header),
                None,
                "{header} was sent; a credentialed CORS response is a release failure"
            );
        }
        // And the defence-in-depth headers are on every one of them.
        assert_eq!(
            response.header("x-content-type-options").as_deref(),
            Some("nosniff")
        );
        assert!(response
            .header("content-security-policy")
            .is_some_and(|v| v.contains("frame-ancestors 'none'")));
    }
}

/// **Logout clears the cookie, and it is an unsafe method like every other
/// mutation.**
#[tokio::test]
async fn logout_clears_the_session_and_needs_the_synchronizer_token() {
    let app = app();
    let cookie = app.session_cookie("u-op", &["lw-a-operators"]);

    // A cross-site `<img>` or form cannot do it.
    let without = Raw::post("/api/v1/session/logout")
        .header("cookie", &cookie)
        .send(&app)
        .await;
    without.assert_problem(403, "forbidden");
    assert!(without.header("set-cookie").is_none());

    let done = Raw::post("/api/v1/session/logout")
        .header("cookie", &cookie)
        .header("x-csrf-token", &app.csrf_for("u-op"))
        .send(&app)
        .await;
    assert_eq!(done.status.as_u16(), 204);
    let cleared = done.header("set-cookie").expect("the cookie is cleared");
    assert!(cleared.starts_with("__Host-logweir_session=;"), "{cleared}");
    assert!(cleared.contains("Max-Age=0"), "{cleared}");
    for attribute in ["Path=/", "Secure", "HttpOnly", "SameSite=Lax"] {
        assert!(cleared.contains(attribute), "{attribute}: {cleared}");
    }
    assert!(!cleared.to_ascii_lowercase().contains("domain="));
}

/// **There is no event stream, authenticated or not — and the limiter the
/// stream will need already exists and holds.**
///
/// D0 asks for "unauthenticated SSE" to be refused. The honest state of this
/// build is that the route does not exist: `operationEvents` is advertised
/// `false` and every spelling of the path answers 404 with and without a
/// session, which is the strongest form of "refused". The seam the route will
/// use — per-actor, per-namespace connection slots — is implemented and
/// exercised here so that adding the route is adding a route.
#[tokio::test]
async fn there_is_no_event_stream_yet_and_its_limit_seam_holds() {
    let app = app();
    let cookie = app.session_cookie("u-op", &["lw-a-operators"]);
    for path in [
        "/api/v1/namespaces/team-a/operations/backup/x/events",
        "/api/v1/namespaces/team-a/events",
        "/api/v1/namespaces/team-a/operations/backup/x?watch=true",
        "/api/v1/events",
    ] {
        let anonymous = Raw::get(path).send(&app).await;
        assert!(
            matches!(anonymous.status.as_u16(), 400 | 401 | 404),
            "{path} answered {} unauthenticated",
            anonymous.status
        );
        assert!(
            anonymous.header("content-type").as_deref() != Some("text/event-stream"),
            "{path} opened a stream to an unauthenticated caller"
        );
        let authenticated = Raw::get(path).header("cookie", &cookie).send(&app).await;
        assert!(
            matches!(authenticated.status.as_u16(), 400 | 404),
            "{path} answered {}",
            authenticated.status
        );
        assert!(
            authenticated.header("content-type").as_deref() != Some("text/event-stream"),
            "{path} opened a stream"
        );
    }

    // The capability is advertised false, so a client cannot be told to try.
    let session = app.get("/api/v1/session", &cookie).await.json();
    assert_eq!(session["capabilities"]["operationEvents"], false);

    // The seam.
    let slots = logweir_api::auth::ratelimit::StreamSlots::with_limit(2);
    let held: Vec<_> = (0..2)
        .map(|_| slots.acquire("actor#1", NS_A).expect("under the ceiling"))
        .collect();
    assert!(slots.acquire("actor#1", NS_A).is_none());
    assert!(slots.acquire("actor#2", NS_A).is_some());
    drop(held);
    assert!(slots.acquire("actor#1", NS_A).is_some());
}

/// **A request whose session cookie appears twice is no session at all.**
#[tokio::test]
async fn a_duplicated_session_cookie_is_refused() {
    let app = app();
    let good = app.session_cookie("u-op", &["lw-a-operators"]);
    let admin = app.session_cookie("u-adm", &["lw-a-admins"]);
    let sealed = admin.split_once('=').unwrap().1;

    let response = Raw::get("/api/v1/session")
        .header(
            "cookie",
            &format!("{good}; __Host-logweir_session={sealed}"),
        )
        .send(&app)
        .await;
    response.assert_problem(401, "unauthenticated");
}
