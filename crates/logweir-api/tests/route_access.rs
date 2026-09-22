//! The route access table, held to the router from both sides (PLAT-17.2).
//!
//! THE GUARD FOR ROUTES THAT DO NOT EXIST YET. Every route this service serves
//! must carry a declaration in `logweir_api::access::ROUTES`, and the layer
//! that enforces it must run before the route's handler. The tests here fail
//! when:
//!
//! * a `.route(` is added to `src/app.rs` without a declaration (source scan);
//! * a declaration names a route the router does not serve (source scan);
//! * a routed path without a declaration reaches its handler (a router built
//!   here around the real layer — it must fail closed, and the handler's
//!   flag must stay false);
//! * a method registered on a declared path without its own declaration
//!   reaches its handler;
//! * any non-public route answers anything but 401 without an identity, or
//!   makes a Kubernetes call first;
//! * any role reaches a route whose declared action its row of D0's matrix
//!   does not hold, or makes a Kubernetes call first — all four roles, every
//!   route, which is the role matrix at the HTTP boundary rather than in the
//!   decision table alone;
//! * an actor reaches a namespace it is not bound in, on any namespaced route.

mod support;

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::body::Body;
use axum::routing::{get, on, MethodFilter, MethodRouter};
use axum::Router;
use http::Request;
use logweir_api::access::{self, Access, ROUTES};
use logweir_api::authz::{Action, Role};
use support::{
    FakeKube, Options, SharedApp, SharedOptions, TestApp, TestClock, TestResponse, NS_A, NS_B,
    SHARED_HOST, SHARED_ORIGIN,
};
use tower::ServiceExt as _;

// ---------------------------------------------------------------- the scan

/// Every `(method, path)` `src/app.rs` registers, read from the source.
///
/// A `.route(` call is found, its parentheses are balanced to the end of the
/// call, the first top-level argument is the path (a literal or one of the two
/// login constants) and every `get(`/`post(`/`put(`/`patch(`/`delete(` in the
/// second is a method.
fn routed_in_source() -> BTreeSet<(String, String)> {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/app.rs"),
    )
    .unwrap();
    let bytes = source.as_bytes();
    let mut out = BTreeSet::new();
    let mut at = 0;
    while let Some(found) = source[at..].find(".route(") {
        let open = at + found + ".route".len();
        let mut depth = 0usize;
        let mut end = open;
        for (offset, byte) in bytes[open..].iter().enumerate() {
            match byte {
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = open + offset;
                        break;
                    }
                }
                _ => {}
            }
        }
        let call = &source[open + 1..end];
        let (path_arg, methods_arg) = call.split_once(',').expect("a route has two arguments");
        let path_arg = path_arg.trim();
        let path = if let Some(literal) = path_arg.strip_prefix('"') {
            literal.trim_end_matches('"').to_string()
        } else if path_arg.ends_with("LOGIN_PATH") {
            logweir_api::auth::login::LOGIN_PATH.to_string()
        } else if path_arg.ends_with("CALLBACK_PATH") {
            logweir_api::auth::login::CALLBACK_PATH.to_string()
        } else {
            panic!("a route path this scan cannot read: {path_arg}");
        };
        let mut any = false;
        for (needle, method) in [
            ("get(", "GET"),
            ("post(", "POST"),
            ("put(", "PUT"),
            ("patch(", "PATCH"),
            ("delete(", "DELETE"),
        ] {
            let mut search = 0;
            while let Some(i) = methods_arg[search..].find(needle) {
                let index = search + i;
                let before = methods_arg[..index].chars().last();
                if !before.is_some_and(|c| c.is_ascii_alphanumeric() || c == '_') {
                    out.insert((method.to_string(), path.clone()));
                    any = true;
                }
                search = index + needle.len();
            }
        }
        assert!(any, "no method found for {path}: {methods_arg}");
        at = end;
    }
    out
}

#[test]
fn every_routed_path_is_declared_and_every_declaration_is_routed() {
    let routed = routed_in_source();
    let declared: BTreeSet<(String, String)> = ROUTES
        .iter()
        .map(|r| (r.method.to_string(), r.path.to_string()))
        .collect();
    let undeclared: Vec<_> = routed.difference(&declared).collect();
    assert!(
        undeclared.is_empty(),
        "routes with no entry in logweir_api::access::ROUTES — add one (Public is a \
         declaration too): {undeclared:?}"
    );
    let unrouted: Vec<_> = declared.difference(&routed).collect();
    assert!(
        unrouted.is_empty(),
        "declarations the router does not serve: {unrouted:?}"
    );
    assert!(routed.len() > 40, "the scan found too little: {routed:?}");
}

/// Each route group in `src/app.rs` is wrapped by the access layer. The
/// runtime tests below prove it for every route; this names the file line to
/// look at when one of them fails.
#[test]
fn every_route_group_carries_the_access_layer() {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/app.rs"),
    )
    .unwrap();
    let groups = source.matches("Router::new()").count();
    let layers = source.matches("crate::access::enforce,").count();
    // One `Router::new()` is the empty `auth` router of localAdmin mode,
    // which has no route to wrap.
    assert_eq!(
        layers,
        groups - 1,
        "{groups} Router::new() groups, {layers} access layers"
    );
}

// ------------------------------------------------------ fail-closed layer

static UNDECLARED_HANDLER_RAN: AtomicBool = AtomicBool::new(false);
static UNDECLARED_METHOD_HANDLER_RAN: AtomicBool = AtomicBool::new(false);

async fn undeclared_handler() -> &'static str {
    UNDECLARED_HANDLER_RAN.store(true, Ordering::SeqCst);
    "reached"
}

async fn undeclared_method_handler() -> &'static str {
    UNDECLARED_METHOD_HANDLER_RAN.store(true, Ordering::SeqCst);
    "reached"
}

async fn declared_handler() -> &'static str {
    "declared"
}

fn guarded(router: Router<logweir_api::app::AppState>) -> Router {
    let fake = FakeKube::new();
    let state = support::app_state(&fake, Options::default(), &TestClock::new());
    router
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            access::enforce,
        ))
        .with_state(state)
}

async fn call(router: Router, method: &str, path: &str) -> http::Response<Body> {
    router
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("host", support::HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
}

/// **A route with no declaration is not served.** NEGATIVE CONTROL: the same
/// router serves a declared route with the same layer, so the refusal is the
/// missing entry and not the layer refusing everything.
#[tokio::test]
async fn an_undeclared_route_fails_closed_and_its_handler_never_runs() {
    let router = guarded(
        Router::new()
            .route("/api/v1/added-by-another-stage", get(undeclared_handler))
            .route("/api/v1/session", get(declared_handler)),
    );
    let refused = call(router.clone(), "GET", "/api/v1/added-by-another-stage").await;
    assert_eq!(refused.status(), 500);
    assert!(
        !UNDECLARED_HANDLER_RAN.load(Ordering::SeqCst),
        "the undeclared route's handler ran"
    );

    let served = call(router, "GET", "/api/v1/session").await;
    assert_eq!(served.status(), 200, "the declared control route");
}

/// **A method nobody declared cannot be smuggled onto a declared path.**
/// `/api/v1/session` is declared for `GET` only; a `DELETE` handler registered
/// beside it is answered 405 by the layer and never runs. NEGATIVE CONTROL: the
/// `GET` on the same path reaches its handler.
#[tokio::test]
async fn an_undeclared_method_on_a_declared_path_never_reaches_its_handler() {
    let router = guarded(Router::new().route(
        "/api/v1/session",
        get(declared_handler).delete(undeclared_method_handler),
    ));
    let refused = call(router.clone(), "DELETE", "/api/v1/session").await;
    assert_eq!(refused.status(), 405);
    assert_eq!(
        refused
            .headers()
            .get("allow")
            .map(|v| v.to_str().unwrap().to_string()),
        Some("GET,HEAD".to_string())
    );
    assert!(
        !UNDECLARED_METHOD_HANDLER_RAN.load(Ordering::SeqCst),
        "a handler under an undeclared method ran"
    );
    assert_eq!(call(router, "GET", "/api/v1/session").await.status(), 200);
}

// ------------------------------------------------------- the whole table

/// A concrete request path for a declared route. A command route's target
/// carries its first verb, so the verb's action is the one decided.
fn concrete(entry: &access::RouteAccess, namespace: &str) -> String {
    let target = match entry.access {
        Access::Command { verbs, .. } => format!("x1{}", verbs[0].0),
        _ => "x1".to_string(),
    };
    let path = entry
        .path
        .replace("{ns}", namespace)
        .replace("{kind}", "backup")
        .replace("{*path}", "index.html");
    // The LAST parameter of a command route is its target.
    if path.contains("{id}") {
        path.replace("{id}", &target)
    } else {
        path.replace("{name}", &target)
    }
}

/// The action the layer decides for this entry, as `concrete` builds it.
fn decided(entry: &access::RouteAccess) -> Vec<Action> {
    match entry.access {
        Access::Command { verbs, .. } => vec![verbs[0].1],
        other => other.actions(),
    }
}

async fn send_as(
    shared: &SharedApp,
    method: &str,
    path: &str,
    cookie: Option<&str>,
    csrf: Option<&str>,
) -> TestResponse {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("host", SHARED_HOST);
    if method != "GET" {
        builder = builder
            .header("origin", SHARED_ORIGIN)
            .header("content-type", "application/json")
            .header("idempotency-key", "route-access-0001");
    }
    if let Some(cookie) = cookie {
        builder = builder.header("cookie", cookie);
    }
    if let Some(csrf) = csrf {
        builder = builder.header("x-csrf-token", csrf);
    }
    let body = if method == "GET" {
        Body::empty()
    } else {
        Body::from("{}")
    };
    shared.app.send(builder.body(body).unwrap()).await
}

fn shared_app() -> SharedApp {
    SharedApp::new(
        FakeKube::new(),
        support::idp::MockIdp::new(support::ISSUER, &[]),
        SharedOptions {
            bindings: support::default_bindings(),
            ..SharedOptions::default()
        },
    )
}

/// **Without an identity, every non-public route is 401 before any
/// Kubernetes call** — the event stream included. NEGATIVE CONTROL: every
/// PUBLIC route answers something other than 401 to the same anonymous
/// request, so the 401s are the declaration and not a blanket refusal.
#[tokio::test]
async fn without_an_identity_every_non_public_route_is_401_before_any_kubernetes_call() {
    let shared = shared_app();
    let mut checked = 0;
    for entry in ROUTES {
        shared.app.fake.clear_requests();
        let path = concrete(entry, NS_A);
        let response = send_as(&shared, entry.method, &path, None, None).await;
        if entry.access == Access::Public {
            // The callback's OWN answer to a request with no login state is a
            // 401 from the sign-in handler, which is exactly what a public
            // route should reach; every other public route answers without
            // one.
            if entry.path != logweir_api::auth::login::CALLBACK_PATH {
                assert_ne!(
                    response.status.as_u16(),
                    401,
                    "public {} {path}",
                    entry.method
                );
            }
            assert_ne!(response.status.as_u16(), 500, "public {path}");
            continue;
        }
        response.assert_problem(401, "unauthenticated");
        assert!(
            shared.app.fake.requests().is_empty(),
            "{} {path} called Kubernetes before authenticating: {:?}",
            entry.method,
            shared.app.fake.requests()
        );
        checked += 1;
    }
    assert!(checked > 40, "{checked}");
}

/// **The role matrix at the HTTP boundary, every route, all four roles.**
///
/// For each declared route and each role bound alone in `team-a`: when the
/// role's row of `Role::allows` lacks any declared action, the answer is
/// `403 forbidden` and the fake API server saw nothing. When the row holds
/// them all, the layer lets the request through — whatever the handler then
/// says about the empty body or the missing object, it is not the access
/// refusal. Both directions are asserted, so a layer that refused everything
/// fails as surely as one that refused nothing.
#[tokio::test]
async fn every_role_is_refused_exactly_the_routes_its_row_does_not_hold() {
    let shared = shared_app();
    let roles = [
        (Role::Viewer, "lw-a-viewers"),
        (Role::Operator, "lw-a-operators"),
        (Role::Approver, "lw-a-approvers"),
        (Role::Administrator, "lw-a-admins"),
    ];
    let mut refused = 0;
    let mut admitted = 0;
    for (role, group) in roles {
        let subject = format!("matrix-{}", role.as_str());
        let cookie = shared.session_cookie(&subject, &[group]);
        let csrf = shared.csrf_for(&subject);
        for entry in ROUTES {
            let actions = decided(entry);
            if actions.is_empty() {
                continue;
            }
            let allowed = match entry.access {
                // Held in at least one bound namespace; this actor has one.
                Access::AnyNamespace(action) => role.allows(action),
                _ => actions.iter().all(|a| role.allows(*a)),
            };
            shared.app.fake.clear_requests();
            let path = concrete(entry, NS_A);
            let response = send_as(&shared, entry.method, &path, Some(&cookie), Some(&csrf)).await;
            if allowed {
                assert!(
                    !(response.status == 403 && response.code() == "forbidden")
                        && response.status != 401,
                    "{:?} was refused {} {path}: {}",
                    role,
                    entry.method,
                    response.text()
                );
                admitted += 1;
            } else {
                response.assert_problem(403, "forbidden");
                assert!(
                    shared.app.fake.requests().is_empty(),
                    "{:?} reached Kubernetes on a refused {} {path}",
                    role,
                    entry.method
                );
                refused += 1;
            }
        }
    }
    assert!(
        refused > 30 && admitted > 60,
        "{refused} refused, {admitted} admitted"
    );
}

/// **A namespace the actor is not bound in answers exactly what a missing
/// object answers, on every namespaced route, before any Kubernetes call.**
/// NEGATIVE CONTROL: the same actor's request to its own namespace on a read
/// route is not a 404 from the layer (it reaches the fake API server).
#[tokio::test]
async fn an_unbound_namespace_is_404_on_every_namespaced_route_before_any_kubernetes_call() {
    let shared = shared_app();
    let cookie = shared.session_cookie("only-in-a", &["lw-a-admins"]);
    let csrf = shared.csrf_for("only-in-a");
    let mut checked = 0;
    for entry in ROUTES {
        if !entry.path.contains("{ns}") {
            continue;
        }
        shared.app.fake.clear_requests();
        let path = concrete(entry, NS_B);
        let response = send_as(&shared, entry.method, &path, Some(&cookie), Some(&csrf)).await;
        response.assert_problem(404, "not_found");
        assert!(
            shared.app.fake.requests().is_empty(),
            "{} {path} reached Kubernetes for an unbound namespace",
            entry.method
        );
        checked += 1;
    }
    assert!(checked > 40, "{checked}");

    shared.app.fake.clear_requests();
    let own = send_as(
        &shared,
        "GET",
        &format!("/api/v1/namespaces/{NS_A}/backups"),
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(own.status, 200, "{}", own.text());
    assert!(!shared.app.fake.requests().is_empty());
}

/// **The two routes without a namespace are decided over every grant.** A
/// viewer may preview a cadence (it reads schedules somewhere) and may not read
/// trust policies (only an administrator may, anywhere). NEGATIVE CONTROL: an
/// administrator reads both.
#[tokio::test]
async fn the_namespaceless_routes_are_decided_over_every_grant() {
    let shared = shared_app();
    let viewer = shared.session_cookie("v-any", &["lw-b-viewers"]);
    let admin = shared.session_cookie("a-any", &["lw-b-admins"]);
    let preview = "/api/v1/cadence-previews?preset=daily&minute=0&hour=3&timeZone=UTC";
    let viewer_preview = send_as(&shared, "GET", preview, Some(&viewer), None).await;
    assert_eq!(viewer_preview.status, 200, "{}", viewer_preview.text());
    send_as(
        &shared,
        "GET",
        "/api/v1/trust-policies",
        Some(&viewer),
        None,
    )
    .await
    .assert_problem(403, "forbidden");
    let admin_trust = send_as(&shared, "GET", "/api/v1/trust-policies", Some(&admin), None).await;
    assert_eq!(admin_trust.status, 200, "{}", admin_trust.text());
}

/// **localAdmin mode keeps working unchanged through the layer.** Every
/// declared route is reachable by the local administrator in a configured
/// namespace (no 401, no access 403), and an unconfigured namespace is still
/// the informative `403 namespace_forbidden` rather than shared mode's 404.
#[tokio::test]
async fn local_admin_mode_passes_the_layer_on_every_route() {
    let app = TestApp::new();
    for entry in ROUTES {
        if entry.access == Access::Public {
            continue;
        }
        let path = concrete(entry, NS_A);
        let request = Request::builder()
            .method(entry.method)
            .uri(&path)
            .header("host", support::HOST)
            .header("origin", support::ORIGIN)
            .header("content-type", "application/json")
            .header("idempotency-key", "route-access-0002")
            .body(if entry.method == "GET" {
                Body::empty()
            } else {
                Body::from("{}")
            })
            .unwrap();
        let response = app.send(request).await;
        assert!(
            response.status != 401 && !(response.status == 403 && response.code() == "forbidden"),
            "{} {path}: {}",
            entry.method,
            response.text()
        );
    }
    app.get("/api/v1/namespaces/elsewhere/backups")
        .await
        .assert_problem(403, "namespace_forbidden");
}

// ------------------------------------------- the layer, with no handler help

static DUMMY_REACHED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

async fn dummy() -> &'static str {
    DUMMY_REACHED.fetch_add(1, Ordering::SeqCst);
    "reached"
}

/// Every declared route, served by a handler that checks NOTHING, behind the
/// real access layer — the shape a route added by a later stage has if its
/// author forgets every check. Whatever this router refuses, the layer
/// refused.
fn layer_only(state: logweir_api::app::AppState) -> Router {
    let mut by_path: std::collections::BTreeMap<&str, MethodRouter<logweir_api::app::AppState>> =
        std::collections::BTreeMap::new();
    for entry in ROUTES {
        let filter = match entry.method {
            "GET" => MethodFilter::GET,
            "POST" => MethodFilter::POST,
            "PUT" => MethodFilter::PUT,
            other => panic!("{other}"),
        };
        let router = by_path
            .remove(entry.path)
            .map_or_else(|| on(filter, dummy), |r| r.on(filter, dummy));
        by_path.insert(entry.path, router);
    }
    let mut router = Router::new();
    for (path, methods) in by_path {
        router = router.route(path, methods);
    }
    router
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            access::enforce,
        ))
        .with_state(state)
}

/// **The layer ALONE enforces the whole matrix.** The same sweeps as above —
/// anonymous, all four roles, an unbound namespace — against handlers that
/// check nothing. A refusal must come with the dummy handler unreached; an
/// admission must reach it. This is the row that fails when a DECLARATION is
/// wrong even though today's handler would still have caught it.
#[tokio::test]
async fn the_layer_alone_enforces_the_matrix_with_handlers_that_check_nothing() {
    let shared = shared_app();
    let router = layer_only(shared.app.state.clone());
    let send =
        |method: &'static str, path: String, cookie: Option<String>, csrf: Option<String>| {
            let router = router.clone();
            async move {
                let mut builder = Request::builder()
                    .method(method)
                    .uri(&path)
                    .header("host", SHARED_HOST);
                if method != "GET" {
                    builder = builder
                        .header("origin", SHARED_ORIGIN)
                        .header("content-type", "application/json");
                }
                if let Some(cookie) = cookie {
                    builder = builder.header("cookie", cookie);
                }
                if let Some(csrf) = csrf {
                    builder = builder.header("x-csrf-token", csrf);
                }
                let before = DUMMY_REACHED.load(Ordering::SeqCst);
                let response = router
                    .oneshot(builder.body(Body::from("{}")).unwrap())
                    .await
                    .unwrap();
                let reached = DUMMY_REACHED.load(Ordering::SeqCst) > before;
                (response.status().as_u16(), reached)
            }
        };
    let mut checked = 0;
    // Anonymous: every non-public route refused, the handler never reached.
    for entry in ROUTES {
        let (status, reached) = send(entry.method, concrete(entry, NS_A), None, None).await;
        if entry.access == Access::Public {
            assert!(
                reached,
                "public {} {} was refused",
                entry.method, entry.path
            );
        } else {
            assert_eq!(
                (status, reached),
                (401, false),
                "{} {}",
                entry.method,
                entry.path
            );
        }
        checked += 1;
    }
    // Each role alone in team-a, and an unbound namespace for each.
    for (role, group) in [
        (Role::Viewer, "lw-a-viewers"),
        (Role::Operator, "lw-a-operators"),
        (Role::Approver, "lw-a-approvers"),
        (Role::Administrator, "lw-a-admins"),
    ] {
        let subject = format!("layer-{}", role.as_str());
        let cookie = shared.session_cookie(&subject, &[group]);
        let csrf = shared.csrf_for(&subject);
        for entry in ROUTES {
            let actions = decided(entry);
            if actions.is_empty() {
                continue;
            }
            let allowed = actions.iter().all(|a| role.allows(*a));
            let (status, reached) = send(
                entry.method,
                concrete(entry, NS_A),
                Some(cookie.clone()),
                Some(csrf.clone()),
            )
            .await;
            if allowed {
                assert_eq!(
                    (status, reached),
                    (200, true),
                    "{role:?} {} {}",
                    entry.method,
                    entry.path
                );
            } else {
                assert_eq!(
                    (status, reached),
                    (403, false),
                    "{role:?} {} {}",
                    entry.method,
                    entry.path
                );
            }
            if entry.path.contains("{ns}") {
                let (status, reached) = send(
                    entry.method,
                    concrete(entry, NS_B),
                    Some(cookie.clone()),
                    Some(csrf.clone()),
                )
                .await;
                assert_eq!(
                    (status, reached),
                    (404, false),
                    "{role:?} reached unbound {} {}",
                    entry.method,
                    entry.path
                );
            }
            checked += 1;
        }
    }
    assert!(checked > 200, "{checked}");
}

/// **No mutating route is declared with an action a viewer holds.** D0:
/// "Viewer never mutates" — asserted on the TABLE, so a POST or PUT declared
/// with a read action fails here whatever its handler does.
#[test]
fn no_mutating_route_is_declared_with_a_viewers_action() {
    for entry in ROUTES {
        if entry.method == "GET" || entry.access == Access::Public {
            continue;
        }
        if entry.path == "/api/v1/session/logout" {
            // Ending one's own session is not a mutation of anything a viewer
            // may not touch.
            continue;
        }
        let actions = entry.access.actions();
        assert!(
            !actions.is_empty() && actions.iter().any(|a| !Role::Viewer.allows(*a)),
            "{} {} is declared with only viewer actions {:?}",
            entry.method,
            entry.path,
            actions
        );
    }
}
