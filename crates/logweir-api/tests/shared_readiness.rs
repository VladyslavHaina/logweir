//! Shared-mode readiness is gated on the OIDC provider (PLAT-17.2, D0
//! §"Helm, RBAC, ingress, and network changes": "The API starts NotReady if
//! OIDC discovery/JWKS cannot initialize").
//!
//! Each row builds its own app, because readiness is cached for five seconds
//! and a verdict from one row must not decide the next.

mod support;

use support::{FakeKube, SharedApp, SharedOptions, ISSUER};

fn app(idp: support::idp::MockIdp) -> SharedApp {
    SharedApp::new(
        FakeKube::new(),
        idp,
        SharedOptions {
            bindings: support::default_bindings(),
            ..SharedOptions::default()
        },
    )
}

async fn readyz(app: &SharedApp) -> support::TestResponse {
    app.app.get("/readyz").await
}

/// **With discovery and a key set available, a shared console is Ready.**
/// The positive control for both refusals below.
#[tokio::test]
async fn a_reachable_provider_with_keys_is_ready() {
    let key = support::idp::TestKey::ec("k1");
    let idp = support::idp::MockIdp::new(ISSUER, &[&key]);
    let response = readyz(&app(idp.clone())).await;
    assert_eq!(response.status, 200, "{}", response.text());
    assert!(idp.discovery_fetches() >= 1 && idp.jwks_fetches() >= 1);
}

/// **An unreachable discovery document keeps the console NotReady**, with a
/// body that names no endpoint and no reason.
#[tokio::test]
async fn an_unreachable_discovery_document_is_not_ready() {
    let key = support::idp::TestKey::ec("k1");
    let idp = support::idp::MockIdp::new(ISSUER, &[&key]);
    idp.set_discovery_down(true);
    let response = readyz(&app(idp)).await;
    response.assert_problem(503, "kubernetes_unavailable");
    assert!(!response.text().contains("idp.test"), "{}", response.text());
}

/// **A provider publishing no key cannot validate a single ID token**, so the
/// console is not Ready either.
#[tokio::test]
async fn a_provider_with_no_key_is_not_ready() {
    let idp = support::idp::MockIdp::new(ISSUER, &[]);
    readyz(&app(idp))
        .await
        .assert_problem(503, "kubernetes_unavailable");
}

/// **A JWKS outage with nothing cached is NotReady** — the outage window only
/// ever extends keys that were once fetched.
#[tokio::test]
async fn a_jwks_outage_with_nothing_cached_is_not_ready() {
    let key = support::idp::TestKey::ec("k1");
    let idp = support::idp::MockIdp::new(ISSUER, &[&key]);
    idp.set_jwks_down(true);
    readyz(&app(idp))
        .await
        .assert_problem(503, "kubernetes_unavailable");
}

/// **Once initialised, a provider outage does not take the console out of
/// rotation, and a signed session keeps working** (D0: the API *starts*
/// NotReady if the provider cannot initialise; a valid session continues to
/// its signed expiry — review M2). The caches are expired and discovery and
/// JWKS both fail; readiness stays true and `/api/v1/session` still answers.
/// NEGATIVE CONTROL: the rows above, where the provider never initialised.
#[tokio::test]
async fn a_provider_outage_after_initialisation_keeps_the_console_ready() {
    let key = support::idp::TestKey::ec("k1");
    let idp = support::idp::MockIdp::new(ISSUER, &[&key]);
    let shared = app(idp.clone());
    let provider = &shared.app.state.shared().expect("shared mode").provider;
    assert!(provider.ready().await, "the provider initialises");

    idp.set_discovery_down(true);
    idp.set_jwks_down(true);
    provider.expire_caches_for_test();
    assert!(
        provider.ready().await,
        "an outage after initialisation took the console NotReady"
    );
    // The same through the probe, whose own 5-second cache is cold here.
    let fresh = app(idp.clone());
    let fresh_provider = &fresh.app.state.shared().unwrap().provider;
    assert!(
        !fresh_provider.ready().await,
        "a replica that never initialised must still start NotReady"
    );

    let cookie = shared.session_cookie("u-v", &["lw-a-viewers"]);
    let session = shared.get("/api/v1/session", &cookie).await;
    assert_eq!(session.status, 200, "{}", session.text());
}

/// **Chart gap G6: a console whose trusted proxy is a Service it has not read
/// is not Ready**, because every browser request would be `421`. After one
/// read of the Service's endpoints it is. NEGATIVE CONTROL: the first row of
/// this file, the same provider with no Service source, is Ready at once.
#[tokio::test]
async fn an_unread_proxy_service_is_not_ready_and_a_read_one_is() {
    let key = support::idp::TestKey::ec("k1");
    let idp = support::idp::MockIdp::new(ISSUER, &[&key]);
    let proxies = std::sync::Arc::new(logweir_api::trusted_proxy::TrustedProxies::new(
        Vec::new(),
        Some(logweir_api::config::ServiceRef {
            namespace: "traefik".into(),
            name: "traefik".into(),
        }),
    ));
    let unread = SharedApp::new(
        FakeKube::new(),
        idp.clone(),
        SharedOptions {
            bindings: support::default_bindings(),
            trusted_proxies: Some(std::sync::Arc::clone(&proxies)),
            ..SharedOptions::default()
        },
    );
    readyz(&unread)
        .await
        .assert_problem(503, "kubernetes_unavailable");

    // A fresh app (readiness is cached per app) over the same set, once read.
    proxies.replace_at(["10.1.0.7".parse().unwrap()], std::time::Instant::now());
    let read = SharedApp::new(
        FakeKube::new(),
        idp,
        SharedOptions {
            bindings: support::default_bindings(),
            trusted_proxies: Some(proxies),
            ..SharedOptions::default()
        },
    );
    let response = readyz(&read).await;
    assert_eq!(response.status, 200, "{}", response.text());
}
