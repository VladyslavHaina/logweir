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
        "/api/v1/workspaces/team-a",
        "/api/v1/namespaces/team-a/secrets",
        "/api/v1/namespaces/team-a/secrets/source-scram",
        "/api/v1/namespaces/team-a/pods",
        "/api/v1/namespaces/team-a/pods/runner-0/log",
        "/api/v1/namespaces/team-a/pods/runner-0/exec",
        "/api/v1/workspaces/team-a/jobs",
        "/api/v1/namespaces/team-a/configmaps",
        "/api/v1/workspaces/team-a/trustrosters",
        "/api/v1/workspaces/team-a/backups/x/status",
        "/api/v1/workspaces/team-a/operations/secret/x",
        // `{id}` ROUTES AND NOT A COLLECTION ROUTE. A discovery
        // is always addressed through its connection or by its own id; there
        // is no namespace-wide list of checks, and a client that guessed one
        // gets 404 rather than an unbounded scan.
        "/api/v1/workspaces/team-a/topic-discoveries",
        "/api/v1/workspaces/team-a/destinations/x/topics",
        "/api/v1/workspaces/team-a/backupdestinations",
        // What stays a 404 is a stream for a kind that has none. A transient check
        // finishes in seconds and already has a cancel route; a long-lived
        // connection is a resource it does not need, and the route table is the
        // boundary.
        "/api/v1/workspaces/team-a/operations/discovery/x/events",
        "/api/v1/workspaces/team-a/operations/preflight/x/events",
        "/api/v1/workspaces/team-a/operations/secret/x/events",
        // The cluster-scoped trust read is a READ. There is no write route in
        // v1 and the shape of that is a 405, not a stub that answers 501.
        "/api/v1/trustpolicies",
        "/api/v1/trust-policies/{name}",
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
    app.get("/api/v1/workspaces/team-a/preflights")
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
    // Approval submission, delete and patch: absent. `POST .../backups` and
    // `PUT .../schedules/{name}` are real routes (the list below); the shapes
    // that must NEVER exist are these.
    //
    // `PATCH` and `PUT` ARE NOT THE SAME QUESTION. The edit route is a `PUT`
    // of the whole policy through a typed DTO; a `PATCH` would be a
    // caller-supplied patch document, which is the one thing this API does not
    // accept, and it stays 405.
    for (method, path) in [
        ("POST", "/api/v1/workspaces/team-a/approvals"),
        ("DELETE", "/api/v1/workspaces/team-a/backups/x"),
        ("DELETE", "/api/v1/workspaces/team-a/restores/x"),
        ("DELETE", "/api/v1/workspaces/team-a/schedules/x"),
        ("PATCH", "/api/v1/workspaces/team-a/schedules/x"),
        ("PUT", "/api/v1/workspaces/team-a/backups/x"),
        ("PUT", "/api/v1/workspaces/team-a/restores/x"),
        ("OPTIONS", "/api/v1/workspaces/team-a/schedules"),
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

/// **The edit and manual-run routes are routed, and refuse before Kubernetes.**
///
/// A negative control for the list above: if `POST .../backups` were dropped
/// from the router, the test above would still pass (it no longer probes it)
/// and this one would fail.
#[tokio::test]
async fn the_manual_run_and_the_policy_edit_are_routed() {
    let app = TestApp::new();
    for (method, path, key) in [
        ("POST", "/api/v1/workspaces/team-a/backups", true),
        ("PUT", "/api/v1/workspaces/team-a/schedules/x", false),
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
        ("/api/v1/workspaces/team-a/backups", "impersonate-group"),
        ("/api/v1/workspaces/team-a/backups", "Impersonate-Uid"),
        (
            "/api/v1/workspaces/team-a/backups",
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
    let path = format!("/api/v1/workspaces/{NS_A}/schedules");
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

/// The test UI directory, laid out like `console/dist` (`tests/fixtures/ui`).
fn fixture_ui() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/ui")
}

/// A fresh directory under the system's temporary one.
fn temp_ui(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "logweir-api-ui-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

/// THE CONSOLE'S BUILD IS SERVED AT `/ui/` AND NOTHING ELSE OF ITS DIRECTORY
/// (PROD-18.1 R8). The directory is laid out as `console/dist` is: the page is
/// `/ui/`, `no-cache`; every hashed file is its bytes on disk, `immutable`;
/// fonts and images are selected only among the hashed files. What a build
/// also writes, and anything else, is never served: a page or a source map
/// among the hashed files, an older font format, a font beside the page, a
/// deeper directory, Markdown, the licences in `.vite/`. The old console's
/// paths are gone: no `pages/`, no served `runtime.js`, no `/ui/next/`.
#[tokio::test]
async fn the_console_build_is_served_at_ui_and_nothing_else() {
    let dir = temp_ui("build");
    std::fs::create_dir_all(dir.join("assets/deeper")).unwrap();
    std::fs::create_dir_all(dir.join(".vite")).unwrap();
    std::fs::create_dir_all(dir.join("pages")).unwrap();
    std::fs::create_dir_all(dir.join("next/assets")).unwrap();
    let page =
        b"<!doctype html><script type=\"module\" src=\"./assets/index-AbC_1-x.js\"></script>";
    let write = |rel: &str, bytes: &[u8]| std::fs::write(dir.join(rel), bytes).unwrap();
    write("index.html", page);
    write("assets/index-AbC_1-x.js", b"console.log(1)");
    write("assets/index-DeF.css", b"a{}");
    write("assets/RedHatTextVF-Gh_i.woff2", b"wOF2");
    write("assets/PF-Bkg-Jk.svg", b"<svg/>");
    // A file of the sign-in stylesheet's name never stands in for it (DUI-45).
    write("sign-in.css", b"planted{}");
    for never in [
        "assets/stray.html",
        "assets/index-AbC_1-x.js.map",
        "assets/old.woff",
        "font.woff2",
        "assets/deeper/x.js",
        "README.md",
        ".vite/license.md",
        "pages/backups.js",
        "runtime.js.map",
        "next/index.html",
        "next/assets/index-AbC_1-x.js",
    ] {
        write(never, b"x");
    }
    let app = TestApp::with(
        support::FakeKube::new(),
        support::Options {
            ui_dir: dir.clone(),
            ..Default::default()
        },
    );
    assert_eq!(
        app.state.assets().paths(),
        [
            "assets/PF-Bkg-Jk.svg",
            "assets/RedHatTextVF-Gh_i.woff2",
            "assets/index-AbC_1-x.js",
            "assets/index-DeF.css",
            logweir_api::assets::ICON_PATH,
            "index.html",
            logweir_api::assets::SIGN_IN_CSS_PATH,
        ],
        "exactly the build's page, its hashed files, the compiled-in icon and the sign-in \
         page's rendered stylesheet"
    );
    // DUI-45: the sign-in refusal page's stylesheet, rendered from the console's
    // own font, never the planted file of its name.
    let css = app.get("/ui/sign-in.css").await;
    assert_eq!(css.status, 200);
    assert_eq!(
        css.header("content-type").as_deref(),
        Some("text/css; charset=utf-8")
    );
    let text = String::from_utf8(css.body.clone()).unwrap();
    assert!(
        text.contains("url(\"/ui/assets/RedHatTextVF-Gh_i.woff2\")") && !text.contains("planted"),
        "{text}"
    );
    let index = app.get("/ui/").await;
    assert_eq!(index.status, 200);
    assert_eq!(index.body, page);
    assert_eq!(
        index.header("content-type").as_deref(),
        Some("text/html; charset=utf-8")
    );
    assert_eq!(index.header("cache-control").as_deref(), Some("no-cache"));
    assert_eq!(
        index.header("content-security-policy").as_deref(),
        Some(CSP)
    );
    for (path, kind, bytes) in [
        (
            "/ui/assets/index-AbC_1-x.js",
            "text/javascript; charset=utf-8",
            &b"console.log(1)"[..],
        ),
        (
            "/ui/assets/index-DeF.css",
            "text/css; charset=utf-8",
            b"a{}",
        ),
        ("/ui/assets/RedHatTextVF-Gh_i.woff2", "font/woff2", b"wOF2"),
        ("/ui/assets/PF-Bkg-Jk.svg", "image/svg+xml", b"<svg/>"),
    ] {
        let response = app.get(path).await;
        assert_eq!(response.status, 200, "{path}");
        assert_eq!(response.body, bytes, "{path}");
        assert_eq!(response.header("content-type").as_deref(), Some(kind));
        assert_eq!(
            response.header("cache-control").as_deref(),
            Some(logweir_api::assets::IMMUTABLE),
            "{path}"
        );
        assert_eq!(
            response.header("content-security-policy").as_deref(),
            Some(CSP)
        );
    }
    for path in [
        "/ui/assets/stray.html",
        "/ui/assets/index-AbC_1-x.js.map",
        "/ui/assets/old.woff",
        "/ui/font.woff2",
        "/ui/assets/deeper/x.js",
        "/ui/README.md",
        "/ui/.vite/license.md",
        "/ui/pages/backups.js",
        "/ui/runtime.js",
        "/ui/next/",
        "/ui/next",
        "/ui/next/assets/index-AbC_1-x.js",
        "/ui/assets/",
        "/ui/assets",
        "/ui/index.html/",
        "/ui/assets/../index.html",
        "/ui//index.html",
    ] {
        app.get(path).await.assert_problem(404, "not_found");
    }
    assert!(app.fake.requests().is_empty());
    std::fs::remove_dir_all(&dir).unwrap();
}

/// **THE UI DIRECTORY FOLLOWS NO SYMLINK** (PROD-18.1 R1 review L3). A link
/// inside `assets/` could point anywhere the process can read, and `assets`
/// itself as a link would serve a directory the image never put there.
/// `assets.rs` reads neither: `symlink_metadata` (on a path joined without the
/// trailing slash, which would resolve the link first) and the walk's
/// `file_type` do not follow links, so each is absent, never served.
#[cfg(unix)]
#[tokio::test]
async fn the_ui_directory_follows_no_symlink() {
    let page = b"<!doctype html>";
    let outside = temp_ui("outside").with_extension("js");
    std::fs::write(&outside, b"not part of the console").unwrap();
    let app_for = |dir: &std::path::Path| {
        TestApp::with(
            support::FakeKube::new(),
            support::Options {
                ui_dir: dir.to_path_buf(),
                ..Default::default()
            },
        )
    };

    // 1. A linked file among the hashed files.
    let dir = temp_ui("link-file");
    std::fs::create_dir_all(dir.join("assets")).unwrap();
    std::fs::write(dir.join("index.html"), page).unwrap();
    std::fs::write(dir.join("assets/index-Real.js"), b"console.log(1)").unwrap();
    std::os::unix::fs::symlink(&outside, dir.join("assets/index-Linked.js")).unwrap();
    let app = app_for(&dir);
    // Control: the real file beside it is served.
    assert_eq!(app.get("/ui/assets/index-Real.js").await.status, 200);
    app.get("/ui/assets/index-Linked.js")
        .await
        .assert_problem(404, "not_found");

    // 2. `assets` itself a link to a directory of hashed files.
    let linked = temp_ui("link-dir");
    let elsewhere = linked.join("elsewhere");
    std::fs::create_dir_all(&elsewhere).unwrap();
    std::fs::write(linked.join("index.html"), page).unwrap();
    std::fs::write(elsewhere.join("index-Linked.js"), b"console.log(2)").unwrap();
    std::os::unix::fs::symlink(&elsewhere, linked.join("assets")).unwrap();
    let app2 = app_for(&linked);
    assert_eq!(app2.get("/ui/").await.status, 200);
    assert!(app2
        .state
        .assets()
        .paths()
        .iter()
        .all(|p| !p.starts_with(logweir_api::assets::ASSETS_DIR)));
    app2.get("/ui/assets/index-Linked.js")
        .await
        .assert_problem(404, "not_found");

    // 3. The page itself a link: no page, so the directory is refused at startup.
    let page_link = temp_ui("link-page");
    std::fs::create_dir_all(&page_link).unwrap();
    std::os::unix::fs::symlink(&outside, page_link.join("index.html")).unwrap();
    assert!(logweir_api::assets::StaticAssets::load(&page_link).is_err());

    // Control: the same tree with a real `assets/` serves the file.
    std::fs::remove_file(linked.join("assets")).unwrap();
    std::fs::rename(&elsewhere, linked.join("assets")).unwrap();
    assert_eq!(
        app_for(&linked)
            .get("/ui/assets/index-Linked.js")
            .await
            .status,
        200
    );

    for d in [&dir, &linked, &page_link] {
        std::fs::remove_dir_all(d).unwrap();
    }
    std::fs::remove_file(&outside).unwrap();
}

/// The served policy as `directive -> sources`, and the sources a fetch of
/// `directive` is actually held to (CSP3 §6.8.3: an absent fetch directive
/// falls back to `default-src`).
fn effective_sources(csp: &str, directive: &str) -> Vec<String> {
    let mut map = std::collections::BTreeMap::new();
    for part in csp.split(';') {
        let mut words = part.split_whitespace();
        if let Some(name) = words.next() {
            map.insert(
                name.to_string(),
                words.map(str::to_string).collect::<Vec<_>>(),
            );
        }
    }
    map.get(directive)
        .or_else(|| map.get("default-src"))
        .cloned()
        .unwrap_or_default()
}

/// Every `src="…"` / `href="…"` value in an HTML file, with the tag it sits in.
fn references(html: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (start, _) in html.match_indices('<') {
        let rest = &html[start + 1..];
        let Some(end) = rest.find('>') else { continue };
        let tag = &rest[..end];
        let name: String = tag
            .chars()
            .take_while(char::is_ascii_alphanumeric)
            .collect::<String>()
            .to_ascii_lowercase();
        if name.is_empty() {
            continue;
        }
        for attr in ["src=", "href="] {
            for (at, _) in tag.match_indices(attr) {
                let preceded = tag[..at].ends_with(char::is_whitespace);
                let value = &tag[at + attr.len()..];
                let Some(quote) = value.chars().next().filter(|c| *c == '"' || *c == '\'') else {
                    continue;
                };
                let Some(close) = value[1..].find(quote) else {
                    continue;
                };
                if preceded {
                    out.push((name.clone(), value[1..=close].to_string()));
                }
            }
        }
    }
    out
}

/// A page icon named `data:,` would be refused by this server's policy, which
/// admits no `data:` image (`img-src` falls back to `default-src 'self'`),
/// and log a CSP violation on every console page load. The fix is the icon
/// from `'self'`, NOT a wider policy -- so this holds both halves: the policy
/// a page is held to is exactly `'self'` for every fetch a page makes, and
/// every asset any shipped page names is a same-origin path this server
/// answers 200 for.
///
/// REGRESSION REASONS. Put `data:,` in the page's icon link: the reference
/// walk fails naming it. Drop the compiled-in icon from `assets.rs`: the
/// served-200 arm fails naming `/ui/favicon.svg`. Add `data:` to the policy:
/// the policy arm fails.
#[tokio::test]
async fn no_console_page_names_an_asset_the_served_policy_forbids() {
    let app = TestApp::new();
    let index = app.get("/ui/").await;
    let csp = index
        .header("content-security-policy")
        .expect("the page carries a policy");
    for directive in [
        "img-src",
        "script-src",
        "style-src",
        "font-src",
        "connect-src",
    ] {
        assert_eq!(
            effective_sources(&csp, directive),
            vec!["'self'".to_string()],
            "a page's `{directive}` must be exactly 'self': the icon is served from this \
             origin so that no fetch a page makes needs a wider source ({csp})"
        );
    }
    for token in csp.split(|c: char| c == ';' || c.is_whitespace()) {
        assert!(
            !matches!(
                token,
                "data:" | "blob:" | "*" | "'unsafe-inline'" | "'unsafe-eval'" | "http:" | "https:"
            ),
            "the served policy admits `{token}`: {csp}"
        );
    }

    // The test page names what a build's page names: its script, its
    // stylesheet and the icon, each a path relative to the page.
    let html = std::fs::read_to_string(fixture_ui().join("index.html")).unwrap();
    let mut checked = 0usize;
    for (tag, value) in references(&html) {
        // A fragment is navigation inside the page, never a fetch.
        if tag == "a" || value.starts_with('#') {
            continue;
        }
        let scheme = value
            .split_once(':')
            .map(|(s, _)| s)
            .filter(|s| !s.is_empty() && !s.contains('/') && !s.contains('.'));
        assert!(
            scheme.is_none() && !value.starts_with("//") && !value.starts_with('/'),
            "<{tag}> names `{value}`, which is not a path relative to the page. The served \
             policy holds every fetch to 'self' ({csp}); a `data:` or remote reference is \
             refused by the browser on every load"
        );
        let url = format!("/ui/{}", value.trim_start_matches("./"));
        let response = app.get(&url).await;
        assert_eq!(
            response.status, 200,
            "<{tag}> names `{value}`, and this server answers {} for {url}",
            response.status
        );
        checked += 1;
    }
    assert_eq!(checked, 3, "the page's script, stylesheet and icon");

    let icon = app
        .get(&format!("/ui/{}", logweir_api::assets::ICON_PATH))
        .await;
    assert_eq!(icon.status, 200);
    assert_eq!(
        icon.header("content-type").as_deref(),
        Some("image/svg+xml")
    );
    assert_eq!(icon.body, logweir_api::assets::ICON_SVG.as_bytes());
    assert_eq!(icon.header("content-security-policy").as_deref(), Some(CSP));
    assert_eq!(
        icon.header("x-content-type-options").as_deref(),
        Some("nosniff")
    );
    let body = String::from_utf8(icon.body.clone()).unwrap();
    assert!(
        !body.contains("<script") && !body.contains("<style") && !body.contains("href"),
        "the icon carries no script, no inline style and no reference: {body}"
    );
    assert!(
        index
            .body
            .windows(b"href=\"./favicon.svg\"".len())
            .any(|w| w == b"href=\"./favicon.svg\""),
        "the page names the served icon"
    );
}

#[tokio::test]
async fn tests_fixtures_keys_readmes_listings_and_traversals_are_not_served() {
    let app = TestApp::new();
    for path in [
        "/ui/README.md",
        "/ui/tests/index.html",
        "/ui/fixtures/approver.pem",
        "/ui/assets/",
        "/ui/assets",
        "/ui/../Cargo.toml",
        "/ui/%2e%2e/Cargo.toml",
        "/ui/assets/../index.html",
        "/ui/assets/%2e%2e/index.html",
        "/ui/./index.html",
        "/ui//index.html",
        "/ui/index.html/",
        "/ui/INDEX.HTML",
        "/ui/assets/index-AbC_1-x.js%00",
        "/ui/.git/config",
        "/Cargo.toml",
        "/ui/assets/index-AbC_1-x.js.map",
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
            "/apis/logweir.dev/v1alpha1/namespaces/logweir-system/kafkaclusters"
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
