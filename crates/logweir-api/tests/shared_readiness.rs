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
