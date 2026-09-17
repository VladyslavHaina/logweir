//! The route boundary, the transport guards, the security headers and the
//! static UI.
//!
//! Every negative here also asserts the fake Kubernetes API saw NO request:
//! a boundary that answers 404 after asking the cluster is not a boundary.

mod support;

use axum::body::Body;
use http::Request;
use support::{TestApp, HOST, NS_A, ORIGIN};

const CSP: &str =
    "default-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'";

#[tokio::test]
async fn kubernetes_shaped_and_unlisted_paths_are_404_and_reach_nothing() {
    let app = TestApp::new();
    for path in [
        "/apis",
        "/apis/logweir.dev/v1alpha1/namespaces/team-a/backups",
        "/apis/logweir.dev/v1alpha1/namespaces/team-a/kafkaclusters",
        "/api",
        "/api/",
        "/api/v1",
        "/api/v1/",
        "/api/v1/raw",
        "/api/v1/raw/apis/logweir.dev/v1alpha1/namespaces/team-a/backups",
        "/api/v1/proxy/namespaces/team-a/services/x",
        "/api/v1/namespaces/team-a",
        "/api/v1/namespaces/team-a/secrets",
        "/api/v1/namespaces/team-a/secrets/source-scram",
        "/api/v1/namespaces/team-a/pods",
        "/api/v1/namespaces/team-a/pods/runner-0/log",
        "/api/v1/namespaces/team-a/pods/runner-0/exec",
        "/api/v1/namespaces/team-a/jobs",
        "/api/v1/namespaces/team-a/configmaps",
        "/api/v1/namespaces/team-a/trustrosters",
        "/api/v1/namespaces/team-a/backups/x/status",
        "/api/v1/namespaces/team-a/operations/secret/x",
        // D2 W12 ADDED `{id}` ROUTES AND NOT A COLLECTION ROUTE. A discovery
        // is always addressed through its connection or by its own id; there
        // is no namespace-wide list of checks, and a client that guessed one
        // gets 404 rather than an unbounded scan.
        "/api/v1/namespaces/team-a/topic-discoveries",
        "/api/v1/namespaces/team-a/destinations/x/topics",
        "/api/v1/namespaces/team-a/backupdestinations",
        "/api/v1/namespaces/team-a/operations/backup/x/events",
        "/api/v1/nodes",
        "/api/v1/session/",
        "/version",
        "/openapi/v2",
        "/metrics",
        "/debug/pprof",
    ] {
        let response = app.get(path).await;
        response.assert_problem(404, "not_found");
    }
    // `POST .../preflights` exists; `GET` on it does not. A readiness result
    // is addressed by id, and there is no namespace-wide list of checks.
    app.get("/api/v1/namespaces/team-a/preflights")
        .await
        .assert_problem(405, "method_not_allowed");
    // `operations/{secret|...}` reaches the handler, which refuses the kind
    // before any lookup. Nothing reached Kubernetes at all.
    assert!(
        app.fake.requests().is_empty(),
        "a boundary refusal reached Kubernetes: {:#?}",
        app.fake.requests()
    );
    app.fake.assert_strict();
}

#[tokio::test]
async fn unimplemented_mutations_have_no_route() {
    let app = TestApp::new();
    // Approval submission, delete and patch: absent. D1 W6 gave `POST
    // .../backups` and `PUT .../schedules/{name}` real routes, so they moved
    // to the list below; the shapes that must NEVER exist are these.
    //
    // `PATCH` and `PUT` ARE NOT THE SAME QUESTION. The edit route is a `PUT`
    // of the whole policy through a typed DTO; a `PATCH` would be a
    // caller-supplied patch document, which is the one thing this API does not
    // accept, and it stays 405.
    for (method, path) in [
        ("POST", "/api/v1/namespaces/team-a/approvals"),
        ("DELETE", "/api/v1/namespaces/team-a/backups/x"),
        ("DELETE", "/api/v1/namespaces/team-a/restores/x"),
        ("DELETE", "/api/v1/namespaces/team-a/schedules/x"),
        ("PATCH", "/api/v1/namespaces/team-a/schedules/x"),
        ("PUT", "/api/v1/namespaces/team-a/backups/x"),
        ("PUT", "/api/v1/namespaces/team-a/restores/x"),
        ("OPTIONS", "/api/v1/namespaces/team-a/schedules"),
    ] {
        let response = app
            .send(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header("host", HOST)
                    .header("origin", ORIGIN)
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await;
        response.assert_problem(405, "method_not_allowed");
        assert!(response.header("allow").is_some(), "{method} {path}");
        assert!(response.header("access-control-allow-origin").is_none());
    }
    assert!(app.fake.requests().is_empty());
}

/// **The two routes D1 W6 added are routed, and refuse before Kubernetes.**
///
/// A negative control for the list above: if `POST .../backups` were dropped
/// from the router, the test above would still pass (it no longer probes it)
/// and this one would fail.
#[tokio::test]
async fn the_manual_run_and_the_policy_edit_are_routed() {
    let app = TestApp::new();
    for (method, path, key) in [
        ("POST", "/api/v1/namespaces/team-a/backups", true),
        ("PUT", "/api/v1/namespaces/team-a/schedules/x", false),
    ] {
        let mut builder = Request::builder()
            .method(method)
            .uri(path)
            .header("host", HOST)
            .header("origin", ORIGIN)
            .header("content-type", "application/json");
        if key {
            builder = builder.header("idempotency-key", "routed-probe-01");
        }
        let response = app.send(builder.body(Body::from("{}")).unwrap()).await;
        assert_ne!(
            response.status.as_u16(),
            405,
            "{method} {path} is not routed"
        );
        // An empty body is a validation failure, and it happens BEFORE any
        // Kubernetes call.
        response.assert_problem(422, "validation_failed");
    }
    assert!(app.fake.requests().is_empty());
    app.fake.assert_strict();
}

#[tokio::test]
async fn every_response_carries_the_security_headers() {
    let app = TestApp::new();
    let responses = vec![
        app.get("/ui/").await,
        app.get("/ui/app.js").await,
        app.get("/api/v1/session").await,
        app.get("/apis").await,
        app.get("/healthz").await,
    ];
    for response in responses {
        assert_eq!(
            response.header("content-security-policy").as_deref(),
            Some(CSP)
        );
        assert_eq!(
            response.header("x-content-type-options").as_deref(),
            Some("nosniff")
        );
        assert_eq!(
            response.header("referrer-policy").as_deref(),
            Some("no-referrer")
        );
        assert_eq!(response.header("x-frame-options").as_deref(), Some("DENY"));
        assert!(response
            .header("permissions-policy")
            .unwrap()
            .contains("camera=()"));
        let id = response.header("x-request-id").expect("a request id");
        assert_eq!(id.len(), 26, "a ULID request id");
        assert!(response.header("cache-control").is_some());
        assert!(response.header("access-control-allow-origin").is_none());
    }
}

#[tokio::test]
async fn a_client_supplied_request_id_is_ignored() {
    let app = TestApp::new();
    let response = app
        .send(
            Request::builder()
                .uri("/api/v1/session")
                .header("host", HOST)
                .header("x-request-id", "attacker-chosen")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_ne!(
        response.header("x-request-id").as_deref(),
        Some("attacker-chosen")
    );
    assert_ne!(response.json()["requestId"], "attacker-chosen");
}

#[tokio::test]
async fn impersonation_headers_are_refused_on_every_route() {
    let app = TestApp::new();
    for (path, header) in [
        ("/api/v1/session", "Impersonate-User"),
        ("/api/v1/namespaces/team-a/backups", "impersonate-group"),
        ("/api/v1/namespaces/team-a/backups", "Impersonate-Uid"),
        (
            "/api/v1/namespaces/team-a/backups",
            "Impersonate-Extra-scopes",
        ),
        ("/ui/", "Impersonate-User"),
        ("/apis", "Impersonate-User"),
    ] {
        let response = app
            .send(
                Request::builder()
                    .uri(path)
                    .header("host", HOST)
                    .header(header, "system:admin")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;
        response.assert_problem(400, "header_not_allowed");
    }
    assert!(app.fake.requests().is_empty());
}

#[tokio::test]
async fn forged_identity_headers_do_not_change_the_actor() {
    let app = TestApp::new();
    let baseline = app.get("/api/v1/session").await.json();
    let forged = app
        .send(
            Request::builder()
                .uri("/api/v1/session")
                .header("host", HOST)
                .header("x-remote-user", "mallory")
                .header("x-forwarded-user", "mallory")
                .header("x-auth-request-user", "mallory")
                .header("x-forwarded-host", "evil.example")
                .header("forwarded", "for=1.2.3.4;host=evil.example")
                .header("authorization", "Bearer eyJhbGciOiJub25lIn0.e30.")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .json();
    assert_eq!(baseline["actor"], forged["actor"]);
    assert_eq!(baseline["namespaces"], forged["namespaces"]);
    assert_eq!(forged["actor"]["id"], "urn:logweir:local-admin#admin");
}

#[tokio::test]
async fn a_host_this_listener_does_not_serve_is_refused() {
    let app = TestApp::new();
    for host in [
        Some("evil.example"),
        Some("evil.example:8484"),
        Some("127.0.0.1:9999"),
        None,
    ] {
        let mut builder = Request::builder().uri("/api/v1/session");
        if let Some(host) = host {
            builder = builder.header("host", host);
        }
        let response = app.send(builder.body(Body::empty()).unwrap()).await;
        response.assert_problem(421, "misdirected_request");
    }
    for host in ["localhost:8484", "[::1]:8484", "127.0.0.1:8484"] {
        let response = app
            .send(
                Request::builder()
                    .uri("/api/v1/session")
                    .header("host", host)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;
        assert_eq!(response.status, 200, "{host}");
    }
}

/// The two probes answer whatever `Host` the kubelet sends, and still say
/// nothing.
///
/// WHY THE EXEMPTION EXISTS: a kubelet HTTP probe addresses the Pod and sends
/// `Host: <podIP>:<port>`. That authority is in no `publicOrigin` and no
/// administrator's configuration, so with the `Host` allowlist applied to
/// `/healthz` and `/readyz` a deployed console would answer both probes 421 and
/// never become ready.
///
/// WHY IT IS SAFE, asserted rather than asserted-about: the bodies are the same
/// bytes for every `Host`, so a rebinding page that reaches them learns nothing
/// it did not already know from the connection succeeding. The exemption is an
/// EXACT path match, so it does not extend to `/healthz/`, to a prefix, or to
/// any `/api/v1` route — those still answer 421. And `Impersonate-*` is still
/// refused on the exempt paths.
#[tokio::test]
async fn the_probes_answer_any_host_and_nothing_else_does() {
    let app = TestApp::new();
    let foreign = ["evil.example", "10.244.1.7:8484", "[fd00::1]:8484"];

    let mut healthz_bodies = std::collections::BTreeSet::new();
    let mut readyz_bodies = std::collections::BTreeSet::new();
    for host in foreign.iter().chain(["127.0.0.1:8484"].iter()) {
        for (path, bodies) in [
            ("/healthz", &mut healthz_bodies),
            ("/readyz", &mut readyz_bodies),
        ] {
            let response = app
                .send(
                    Request::builder()
                        .uri(path)
                        .header("host", *host)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await;
            assert_eq!(response.status, 200, "{path} with Host: {host}");
            bodies.insert(String::from_utf8_lossy(&response.body).into_owned());
        }
    }
    // One distinct body each: the answer does not vary with the Host, so there
    // is nothing for a foreign Host to learn by asking.
    assert_eq!(healthz_bodies.len(), 1, "{healthz_bodies:?}");
    assert_eq!(readyz_bodies.len(), 1, "{readyz_bodies:?}");
    assert_eq!(
        healthz_bodies.iter().next().unwrap(),
        r#"{"status":"ok"}"#,
        "the liveness body is a constant"
    );

    // The exemption is exact. Nothing near those paths inherits it.
    for path in [
        "/healthz/",
        "/healthzz",
        "/readyz/x",
        "/api/v1/session",
        "/ui/",
        "/",
    ] {
        let response = app
            .send(
                Request::builder()
                    .uri(path)
                    .header("host", "evil.example")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;
        response.assert_problem(421, "misdirected_request");
    }

    // And the impersonation refusal is not exempted with it.
    for path in ["/healthz", "/readyz"] {
        let response = app
            .send(
                Request::builder()
                    .uri(path)
                    .header("host", "10.244.1.7:8484")
                    .header("impersonate-user", "system:admin")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;
        response.assert_problem(400, "header_not_allowed");
    }
    app.fake.assert_strict();
}

#[tokio::test]
async fn unsafe_methods_need_the_exact_origin_and_json_before_anything_else() {
    let app = TestApp::new();
    let body = support::schedule_body().to_string();
    let path = format!("/api/v1/namespaces/{NS_A}/schedules");
    for origin in [
        None,
        Some("http://127.0.0.1:8484/"),
        Some("http://localhost:8484"),
        Some("https://127.0.0.1:8484"),
        Some("null"),
        Some("http://evil.example"),
    ] {
        let mut builder = Request::builder()
            .method("POST")
            .uri(&path)
            .header("host", HOST)
            .header("content-type", "application/json")
            .header("idempotency-key", "key-origin-test");
        if let Some(origin) = origin {
            builder = builder.header("origin", origin);
        }
        let response = app
            .send(builder.body(Body::from(body.clone())).unwrap())
            .await;
        response.assert_problem(403, "origin_mismatch");
    }
    for content_type in [
        None,
        Some("text/plain"),
        Some("application/x-www-form-urlencoded"),
        Some("multipart/form-data; boundary=x"),
    ] {
        let mut builder = Request::builder()
            .method("POST")
            .uri(&path)
            .header("host", HOST)
            .header("origin", ORIGIN)
            .header("idempotency-key", "key-origin-test");
        if let Some(content_type) = content_type {
            builder = builder.header("content-type", content_type);
        }
        let response = app
            .send(builder.body(Body::from(body.clone())).unwrap())
            .await;
        response.assert_problem(415, "unsupported_media_type");
    }
    assert!(
        app.fake.requests().is_empty(),
        "a refused unsafe request reached Kubernetes"
    );
}

// ------------------------------------------------------------- static UI

/// The shipped UI files, selected the way `crates/logweir/tests/chart_lint.rs`
/// selects them: everything under `ui/` except Markdown and `ui/tests/`.
fn shipped_ui_files() -> Vec<String> {
    let root = support::repo_root();
    let mut out = Vec::new();
    let mut stack = vec![root.join("ui")];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            let rel = path
                .strip_prefix(&root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            if path.is_dir() {
                if rel != "ui/tests" {
                    stack.push(path);
                }
            } else if !rel.ends_with(".md") {
                out.push(rel);
            }
        }
    }
    out.sort();
    out
}

#[tokio::test]
async fn every_shipped_asset_is_served_byte_for_byte() {
    let app = TestApp::new();
    let files = shipped_ui_files();
    assert_eq!(
        files.len(),
        22,
        "the shipped UI is twenty-two files: {files:?}"
    );
    for rel in &files {
        let source = std::fs::read(support::repo_root().join(rel)).unwrap();
        let url = format!("/{rel}");
        let response = app.get(&url).await;
        assert_eq!(response.status, 200, "{url}");
        assert_eq!(response.body, source, "{url} bytes differ from {rel}");
        let expected_type = if rel.ends_with(".js") {
            "text/javascript; charset=utf-8"
        } else if rel.ends_with(".css") {
            "text/css; charset=utf-8"
        } else {
            "text/html; charset=utf-8"
        };
        assert_eq!(
            response.header("content-type").as_deref(),
            Some(expected_type)
        );
        assert_eq!(
            response.header("cache-control").as_deref(),
            Some("no-cache")
        );
    }
    let index = app.get("/ui/").await;
    assert_eq!(
        index.body,
        std::fs::read(support::repo_root().join("ui/index.html")).unwrap()
    );
}

#[tokio::test]
async fn tests_fixtures_keys_readmes_listings_and_traversals_are_not_served() {
    let app = TestApp::new();
    for path in [
        "/ui/README.md",
        "/ui/tests/index.html",
        "/ui/tests/api.spec.js",
        "/ui/tests/fixtures/approver.pem",
        "/ui/tests/fixtures/approver.pub.pem",
        "/ui/tests/fixtures/plan.golden.yaml",
        "/ui/tests/fixtures/backup-valid-exit0.json",
        "/ui/tests/",
        "/ui/pages/",
        "/ui/pages",
        "/ui/../Cargo.toml",
        "/ui/%2e%2e/Cargo.toml",
        "/ui/pages/../index.html",
        "/ui/pages/%2e%2e/index.html",
        "/ui/./index.html",
        "/ui//index.html",
        "/ui/index.html/",
        "/ui/INDEX.HTML",
        "/ui/app.js%00",
        "/ui/.git/config",
        "/Cargo.toml",
        "/ui/api.js.map",
    ] {
        let response = app.get(path).await;
        response.assert_problem(404, "not_found");
        let body = String::from_utf8_lossy(&response.body);
        assert!(!body.contains("PRIVATE KEY"), "{path} leaked key material");
    }
}

#[tokio::test]
async fn the_ui_root_redirects_are_relative() {
    let app = TestApp::new();
    for path in ["/", "/ui"] {
        let response = app.get(path).await;
        assert_eq!(response.status, 308, "{path}");
        assert_eq!(response.header("location").as_deref(), Some("/ui/"));
    }
}

#[tokio::test]
async fn health_and_readiness() {
    let app = TestApp::new();
    let live = app.get("/healthz").await;
    assert_eq!(
        (live.status.as_u16(), live.json()["status"].as_str()),
        (200, Some("ok"))
    );
    let ready = app.get("/readyz").await;
    assert_eq!(
        (ready.status.as_u16(), ready.json()["status"].as_str()),
        (200, Some("ready"))
    );
    let paths: Vec<String> = app.fake.requests().iter().map(|r| r.path.clone()).collect();
    assert_eq!(
        paths,
        vec![
            "/version",
            "/apis/logweir.dev/v1alpha1/namespaces/team-a/kafkaclusters"
        ]
    );

    // Not ready: no detail leaks.
    let down = TestApp::new();
    down.fake.inject(support::Fault {
        method: "GET",
        path_contains: "/version".into(),
        status: 503,
        reason: "ServiceUnavailable",
        delay: None,
        remaining: 1,
    });
    let response = down.get("/readyz").await;
    response.assert_problem(503, "kubernetes_unavailable");
    assert!(!String::from_utf8_lossy(&response.body).contains("eyJ"));
    app.fake.assert_strict();
}
