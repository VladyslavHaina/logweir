//! The OpenID Connect sign-in, end to end and then one refusal at a time.
//!
//! EVERY CASE DRIVES THE REAL ROUTER against a real signing key, through the
//! real `/auth/login` → provider → `/auth/callback` sequence. The provider is
//! `support::idp::MockIdp`: in-process, no socket, no network, real RS256 and
//! ES256 signatures.
//!
//! THE POSITIVE TEST IS FIRST AND THE NEGATIVES REUSE IT. Each refusal below
//! changes exactly one thing about a sign-in that otherwise succeeds, so a test
//! that passes for the wrong reason — because the flow was broken anyway —
//! cannot hide here: `a_complete_sign_in_issues_a_session` is what says the
//! baseline works.

mod support;

use axum::body::Body;
use http::Request;
use serde_json::{json, Value};
use support::idp::{Alg, Grant, MockIdp, TestKey};
use support::{FakeKube, SharedApp, SharedOptions, TestResponse, ISSUER, SHARED_HOST};

/// The moment `support::TestClock` starts at.
fn now() -> i64 {
    chrono::DateTime::parse_from_rfc3339("2026-09-15T12:00:00Z")
        .unwrap()
        .timestamp()
}

/// The claims a well-behaved provider would mint.
fn claims(nonce: &str) -> Value {
    json!({
        "iss": ISSUER,
        "sub": "u-ada",
        "aud": support::CLIENT_ID,
        "exp": now() + 300,
        "iat": now(),
        "auth_time": now() - 30,
        "nonce": nonce,
        "name": "Ada Lovelace",
        "groups": ["lw-a-operators", "lw-b-viewers"],
    })
}

fn app(idp: &MockIdp) -> SharedApp {
    SharedApp::new(
        FakeKube::new(),
        idp.clone(),
        SharedOptions {
            bindings: support::default_bindings(),
            ..SharedOptions::default()
        },
    )
}

/// What `/auth/login` handed the browser.
struct Started {
    cookie: String,
    state: String,
    nonce: String,
    challenge: String,
    location: String,
}

async fn start_login(app: &SharedApp, query: &str) -> (TestResponse, Option<Started>) {
    let response = app
        .app
        .send(
            Request::builder()
                .method("GET")
                .uri(format!("/auth/login{query}"))
                .header("host", SHARED_HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    if response.status.as_u16() != 303 {
        return (response, None);
    }
    let location = response
        .header("location")
        .expect("a redirect has a Location");
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
    let query: std::collections::BTreeMap<String, String> =
        serde_urlencoded::from_str(location.split_once('?').unwrap().1).unwrap();
    let started = Started {
        cookie,
        state: query["state"].clone(),
        nonce: query["nonce"].clone(),
        challenge: query["code_challenge"].clone(),
        location: location.clone(),
    };
    (response, Some(started))
}

async fn finish_login(app: &SharedApp, started: &Started, code: &str) -> TestResponse {
    app.app
        .send(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/auth/callback?code={code}&state={}",
                    started.state
                ))
                .header("host", SHARED_HOST)
                .header("cookie", &started.cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
}

/// Mints one ID token for a case: the published key, another key of the same
/// family, the RSA key, and this login's nonce.
type Mint = Box<dyn Fn(&TestKey, &TestKey, &TestKey, &str) -> String>;

fn session_cookie_of(response: &TestResponse) -> Option<String> {
    response
        .headers
        .get_all("set-cookie")
        .iter()
        .map(|v| v.to_str().unwrap().to_string())
        .find(|c| c.starts_with("__Host-logweir_session=") && !c.contains("Max-Age=0"))
}

/// One complete sign-in, and everything it must and must not do.
#[tokio::test]
async fn a_complete_sign_in_issues_a_session_and_leaks_no_provider_token() {
    let key = TestKey::ec("k-ec-1");
    let idp = MockIdp::new(ISSUER, &[&key]);
    let app = app(&idp);

    let (redirect, started) = start_login(&app, "?next=/ui/schedules").await;
    let started = started.expect("login redirects");
    assert_eq!(redirect.status.as_u16(), 303);

    // The authorization request carries exactly what D0 requires.
    let query: std::collections::BTreeMap<String, String> =
        serde_urlencoded::from_str(started.location.split_once('?').unwrap().1).unwrap();
    assert_eq!(query["response_type"], "code");
    assert_eq!(query["client_id"], support::CLIENT_ID);
    assert_eq!(query["redirect_uri"], support::REDIRECT_URI);
    assert_eq!(query["code_challenge_method"], "S256");
    assert!(query["scope"].split(' ').any(|s| s == "openid"));
    assert!(query["state"].len() >= 40 && query["nonce"].len() >= 40);
    assert_ne!(query["state"], query["nonce"]);
    // The verifier itself is NEVER in the authorization request: that is the
    // whole point of PKCE.
    assert!(!started.location.contains("code_verifier"));

    // The login cookie carries the required attributes and nothing readable.
    let raw = redirect
        .headers
        .get_all("set-cookie")
        .iter()
        .map(|v| v.to_str().unwrap().to_string())
        .find(|c| c.starts_with("__Host-logweir_login="))
        .unwrap();
    for attribute in ["Path=/", "Secure", "HttpOnly", "SameSite=Lax"] {
        assert!(raw.contains(attribute), "{attribute} missing from {raw}");
    }
    assert!(!raw.to_ascii_lowercase().contains("domain="));
    assert!(!raw.contains(&started.state) && !raw.contains(&started.nonce));

    idp.grant(
        "code-1",
        Grant {
            id_token: key.mint(&claims(&started.nonce)),
            code_challenge: Some(started.challenge.clone()),
            redirect_uri: Some(support::REDIRECT_URI.to_string()),
        },
    );
    let done = finish_login(&app, &started, "code-1").await;
    assert_eq!(done.status.as_u16(), 303);
    assert_eq!(done.header("location").as_deref(), Some("/ui/schedules"));

    let session = session_cookie_of(&done).expect("a session cookie is set");
    for attribute in ["Path=/", "Secure", "HttpOnly", "SameSite=Lax", "Max-Age="] {
        assert!(
            attribute_present(&session, attribute),
            "{attribute}: {session}"
        );
    }
    assert!(!session.to_ascii_lowercase().contains("domain="));
    // The login cookie is cleared by the same response.
    assert!(done
        .headers
        .get_all("set-cookie")
        .iter()
        .any(|v| v.to_str().unwrap().starts_with("__Host-logweir_login=;")));

    // NO PROVIDER MATERIAL ANYWHERE IN THE RESPONSE. The mock deliberately
    // returns an access token and a refresh token the API must drop on the
    // floor.
    let whole = format!(
        "{}|{}",
        String::from_utf8_lossy(&done.body),
        done.headers
            .iter()
            .map(|(k, v)| format!("{k}:{}", v.to_str().unwrap_or("")))
            .collect::<Vec<_>>()
            .join("|")
    );
    for forbidden in [
        "an-access-token-the-api-must-never-keep",
        "a-refresh-token-the-api-must-never-keep",
        "a-client-secret",
        "eyJ",
        "u-ada",
        "Ada Lovelace",
    ] {
        assert!(!whole.contains(forbidden), "{forbidden} leaked: {whole}");
    }

    // And the session works: it names the actor, its roles and its CSRF token.
    let cookie = session.split(';').next().unwrap().to_string();
    let me = app.get("/api/v1/session", &cookie).await;
    assert_eq!(me.status.as_u16(), 200);
    let body = me.json();
    assert_eq!(body["actor"]["id"], format!("{ISSUER}#u-ada"));
    assert_eq!(body["actor"]["displayName"], "Ada Lovelace");
    assert_eq!(body["authenticationMode"], "oidc");
    assert_eq!(body["bindingRevision"], "rev-1");
    assert!(body["csrfToken"].is_string());
    assert!(body["expiresAt"].is_string());
    let grants: Vec<(String, Vec<String>)> = body["namespaces"]
        .as_array()
        .unwrap()
        .iter()
        .map(|g| {
            (
                g["name"].as_str().unwrap().to_string(),
                g["roles"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|r| r.as_str().unwrap().to_string())
                    .collect(),
            )
        })
        .collect();
    assert_eq!(
        grants,
        vec![
            ("team-a".to_string(), vec!["operator".to_string()]),
            ("team-b".to_string(), vec!["viewer".to_string()]),
        ]
    );
    app.app.fake.assert_strict();
}

fn attribute_present(cookie: &str, attribute: &str) -> bool {
    cookie
        .split(';')
        .map(str::trim)
        .any(|part| part == attribute || part.starts_with(attribute))
}

/// **Every ID-token check D0 names, one at a time, against a flow that
/// otherwise succeeds.**
///
/// Each case mints a token that differs from the good one in exactly one
/// respect. A refusal is 401 with an `unauthenticated` problem and NO session
/// cookie — the last assertion is the one that matters, because a service that
/// answered 401 and set a cookie anyway would pass a status check.
#[tokio::test]
async fn every_id_token_check_refuses_on_its_own() {
    let key = TestKey::ec("k-ec-1");
    let other = TestKey::ec("k-ec-other");
    let rsa = TestKey::rsa("k-rsa-1");

    let cases: Vec<(&str, Mint)> = vec![
        (
            "wrong issuer",
            Box::new(|k: &TestKey, _, _, n: &str| {
                let mut c = claims(n);
                c["iss"] = json!("https://evil.test/realms/logweir");
                k.mint(&c)
            }),
        ),
        (
            "wrong audience",
            Box::new(|k: &TestKey, _, _, n: &str| {
                let mut c = claims(n);
                c["aud"] = json!("another-client");
                k.mint(&c)
            }),
        ),
        (
            "two audiences without azp",
            Box::new(|k: &TestKey, _, _, n: &str| {
                let mut c = claims(n);
                c["aud"] = json!([support::CLIENT_ID, "another-client"]);
                k.mint(&c)
            }),
        ),
        (
            "algorithm none",
            Box::new(|k: &TestKey, _, _, n: &str| {
                k.mint_as("k-ec-1".into(), "none".into(), &claims(n))
            }),
        ),
        (
            "algorithm not on the allowlist",
            Box::new(|k: &TestKey, _, _, n: &str| {
                k.mint_as("k-ec-1".into(), "HS256".into(), &claims(n))
            }),
        ),
        (
            "signature by another key under the published kid",
            Box::new(|_, o: &TestKey, _, n: &str| {
                o.mint_as("k-ec-1".into(), "ES256".into(), &claims(n))
            }),
        ),
        (
            "key id that was never published",
            Box::new(|k: &TestKey, _, _, n: &str| {
                k.mint_as("k-retired".into(), "ES256".into(), &claims(n))
            }),
        ),
        (
            "algorithm disagrees with the key family",
            Box::new(|_, _, r: &TestKey, n: &str| {
                r.mint_as("k-ec-1".into(), "RS256".into(), &claims(n))
            }),
        ),
        (
            "expired",
            Box::new(|k: &TestKey, _, _, n: &str| {
                let mut c = claims(n);
                c["exp"] = json!(now() - 3600);
                k.mint(&c)
            }),
        ),
        (
            "issued in the future",
            Box::new(|k: &TestKey, _, _, n: &str| {
                let mut c = claims(n);
                c["iat"] = json!(now() + 3600);
                k.mint(&c)
            }),
        ),
        (
            "issued too long ago",
            Box::new(|k: &TestKey, _, _, n: &str| {
                let mut c = claims(n);
                c["iat"] = json!(now() - 4000);
                c["exp"] = json!(now() + 300);
                k.mint(&c)
            }),
        ),
        (
            "another login's nonce",
            Box::new(|k: &TestKey, _, _, _| k.mint(&claims("a-nonce-from-another-login"))),
        ),
        (
            "no nonce at all",
            Box::new(|k: &TestKey, _, _, n: &str| {
                let mut c = claims(n);
                c.as_object_mut().unwrap().remove("nonce");
                k.mint(&c)
            }),
        ),
        (
            "no subject",
            Box::new(|k: &TestKey, _, _, n: &str| {
                let mut c = claims(n);
                c["sub"] = json!("");
                k.mint(&c)
            }),
        ),
        (
            "not a JWS at all",
            Box::new(|_, _, _, _| "not.a.token".to_string()),
        ),
    ];

    for (label, mint) in cases {
        let idp = MockIdp::new(ISSUER, &[&key]);
        let app = app(&idp);
        let (_, started) = start_login(&app, "").await;
        let started = started.expect("login redirects");
        idp.grant(
            "code-1",
            Grant {
                id_token: mint(&key, &other, &rsa, &started.nonce),
                code_challenge: None,
                redirect_uri: None,
            },
        );
        let response = finish_login(&app, &started, "code-1").await;
        assert_eq!(response.status.as_u16(), 401, "{label}");
        assert_eq!(response.code(), "unauthenticated", "{label}");
        assert!(
            session_cookie_of(&response).is_none(),
            "{label}: a refused sign-in set a session cookie"
        );
    }
}

/// **A token signed by a published RSA key is accepted, and the same token with
/// one flipped byte is not.**
///
/// The RS256 arm needs its own test because it is a different `ring` verifier
/// and a different JWKS shape from the ES256 one every other case here uses.
#[tokio::test]
async fn rs256_is_verified_as_carefully_as_es256() {
    let rsa = TestKey::rsa("k-rsa-1");
    let idp = MockIdp::new(ISSUER, &[&rsa]);
    let app = app(&idp);
    let (_, started) = start_login(&app, "").await;
    let started = started.expect("login redirects");

    let good = rsa.mint(&claims(&started.nonce));
    idp.grant(
        "code-ok",
        Grant {
            id_token: good.clone(),
            code_challenge: None,
            redirect_uri: None,
        },
    );
    let response = finish_login(&app, &started, "code-ok").await;
    assert_eq!(
        response.status.as_u16(),
        303,
        "a valid RS256 token signs in"
    );
    assert!(session_cookie_of(&response).is_some());

    // One character of the signature, changed.
    let mut chars: Vec<char> = good.chars().collect();
    let last = chars.len() - 1;
    chars[last] = if chars[last] == 'A' { 'B' } else { 'A' };
    let tampered: String = chars.into_iter().collect();
    let (_, started) = start_login(&app, "").await;
    let started = started.expect("login redirects");
    idp.grant(
        "code-bad",
        Grant {
            id_token: tampered,
            code_challenge: None,
            redirect_uri: None,
        },
    );
    let response = finish_login(&app, &started, "code-bad").await;
    assert_eq!(response.status.as_u16(), 401);
    assert!(session_cookie_of(&response).is_none());
    assert_eq!(rsa.alg, Alg::Rs256);
}

/// **`state`, the login cookie and PKCE each have to be right.**
#[tokio::test]
async fn the_login_state_the_cookie_and_the_pkce_verifier_are_all_checked() {
    let key = TestKey::ec("k-ec-1");
    let idp = MockIdp::new(ISSUER, &[&key]);
    let app = app(&idp);
    let (_, started) = start_login(&app, "").await;
    let started = started.expect("login redirects");
    idp.grant(
        "code-1",
        Grant {
            id_token: key.mint(&claims(&started.nonce)),
            code_challenge: Some(started.challenge.clone()),
            redirect_uri: Some(support::REDIRECT_URI.to_string()),
        },
    );

    // 1. A `state` that is not this login's.
    let wrong_state = app
        .app
        .send(
            Request::builder()
                .method("GET")
                .uri("/auth/callback?code=code-1&state=someone-elses-state")
                .header("host", SHARED_HOST)
                .header("cookie", &started.cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(wrong_state.status.as_u16(), 401);
    assert!(session_cookie_of(&wrong_state).is_none());
    assert_eq!(
        idp.token_calls(),
        0,
        "a state mismatch must be refused BEFORE the code is exchanged"
    );

    // 2. No login cookie: the callback did not start here.
    let no_cookie = app
        .app
        .send(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/auth/callback?code=code-1&state={}",
                    started.state
                ))
                .header("host", SHARED_HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(no_cookie.status.as_u16(), 401);
    assert_eq!(idp.token_calls(), 0);

    // 3. A login cookie from a DIFFERENT login, whose state does not match.
    let (_, other) = start_login(&app, "").await;
    let other = other.expect("login redirects");
    let crossed = app
        .app
        .send(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/auth/callback?code=code-1&state={}",
                    started.state
                ))
                .header("host", SHARED_HOST)
                .header("cookie", &other.cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(crossed.status.as_u16(), 401);
    assert_eq!(idp.token_calls(), 0);

    // 4. The happy path exchanges once, and the provider saw the verifier that
    //    hashes to the challenge in the authorization request.
    let done = finish_login(&app, &started, "code-1").await;
    assert_eq!(done.status.as_u16(), 303);
    assert_eq!(idp.token_calls(), 1);
    let form: std::collections::BTreeMap<String, String> =
        serde_urlencoded::from_str(&idp.last_token_form()).unwrap();
    assert_eq!(form["grant_type"], "authorization_code");
    assert_eq!(form["redirect_uri"], support::REDIRECT_URI);
    assert_eq!(
        MockIdp::challenge(&form["code_verifier"]),
        started.challenge
    );
    assert!(
        !idp.last_token_form().contains("client_secret"),
        "clientSecretBasic must not put the secret in the body"
    );

    // 5. A provider that rejects the verifier (this is what a stolen code
    //    without the verifier looks like) is a refusal, not a session.
    let (_, fresh) = start_login(&app, "").await;
    let fresh = fresh.expect("login redirects");
    idp.grant(
        "code-2",
        Grant {
            id_token: key.mint(&claims(&fresh.nonce)),
            // A challenge from a DIFFERENT verifier.
            code_challenge: Some(MockIdp::challenge("a-verifier-this-login-never-had")),
            redirect_uri: None,
        },
    );
    let refused = finish_login(&app, &fresh, "code-2").await;
    assert_eq!(refused.status.as_u16(), 401);
    assert!(session_cookie_of(&refused).is_none());
}

/// **A login cookie is single-use through this service: the callback clears it,
/// so replaying the same code with the same `state` cannot be completed twice
/// by the same browser.**
#[tokio::test]
async fn a_consumed_login_cookie_is_cleared() {
    let key = TestKey::ec("k-ec-1");
    let idp = MockIdp::new(ISSUER, &[&key]);
    let app = app(&idp);
    let (_, started) = start_login(&app, "").await;
    let started = started.expect("login redirects");
    idp.grant(
        "code-1",
        Grant {
            id_token: key.mint(&claims(&started.nonce)),
            code_challenge: None,
            redirect_uri: None,
        },
    );
    let done = finish_login(&app, &started, "code-1").await;
    assert_eq!(done.status.as_u16(), 303);
    let cleared = done
        .headers
        .get_all("set-cookie")
        .iter()
        .map(|v| v.to_str().unwrap().to_string())
        .find(|c| c.starts_with("__Host-logweir_login="))
        .expect("the login cookie is cleared");
    assert!(cleared.contains("Max-Age=0"), "{cleared}");
    assert!(cleared.contains("Secure") && cleared.contains("HttpOnly"));
}

/// **A login attempt that has gone stale is refused.**
#[tokio::test]
async fn a_stale_login_attempt_is_refused() {
    let key = TestKey::ec("k-ec-1");
    let idp = MockIdp::new(ISSUER, &[&key]);
    let app = app(&idp);
    let (_, started) = start_login(&app, "").await;
    let started = started.expect("login redirects");
    idp.grant(
        "code-1",
        Grant {
            id_token: key.mint(&claims(&started.nonce)),
            code_challenge: None,
            redirect_uri: None,
        },
    );
    app.app.clock.advance(601);
    let response = finish_login(&app, &started, "code-1").await;
    assert_eq!(response.status.as_u16(), 401);
    assert_eq!(idp.token_calls(), 0);
    assert!(session_cookie_of(&response).is_none());
}

/// **A provider key rotation is survived without a restart, and an outage keeps
/// the cached keys working.**
///
/// The refetch-on-unknown-`kid` is what makes rotation work; the rate limit on
/// that refetch is what stops an attacker-chosen `kid` from becoming a request
/// amplifier, and the second half of this test is the evidence for it.
#[tokio::test]
async fn jwks_rotation_and_outage() {
    let old = TestKey::ec("k-old");
    let new = TestKey::ec("k-new");
    let idp = MockIdp::new(ISSUER, &[&old]);
    let app = app(&idp);

    // A first sign-in warms the cache.
    let (_, first) = start_login(&app, "").await;
    let first = first.expect("login redirects");
    idp.grant(
        "c1",
        Grant {
            id_token: old.mint(&claims(&first.nonce)),
            code_challenge: None,
            redirect_uri: None,
        },
    );
    assert_eq!(finish_login(&app, &first, "c1").await.status.as_u16(), 303);
    let after_first = idp.jwks_fetches();
    assert_eq!(after_first, 1, "one JWKS fetch warms the cache");

    // ROTATION: the provider publishes a new key and signs with it.
    idp.publish(&[&new]);
    let (_, second) = start_login(&app, "").await;
    let second = second.expect("login redirects");
    idp.grant(
        "c2",
        Grant {
            id_token: new.mint(&claims(&second.nonce)),
            code_challenge: None,
            redirect_uri: None,
        },
    );
    let rotated = finish_login(&app, &second, "c2").await;
    assert_eq!(
        rotated.status.as_u16(),
        303,
        "an unknown kid must provoke exactly one refetch, which finds the rotated key"
    );
    assert_eq!(idp.jwks_fetches(), after_first + 1);

    // THE REFETCH IS RATE LIMITED: a token naming a kid nobody ever published
    // does not cause a fetch per request.
    let before = idp.jwks_fetches();
    for i in 0..5 {
        let (_, attempt) = start_login(&app, "").await;
        let attempt = attempt.expect("login redirects");
        idp.grant(
            &format!("bad-{i}"),
            Grant {
                id_token: new.mint_as(
                    "kid-nobody-published".into(),
                    "ES256".into(),
                    &claims(&attempt.nonce),
                ),
                code_challenge: None,
                redirect_uri: None,
            },
        );
        let response = finish_login(&app, &attempt, &format!("bad-{i}")).await;
        assert_eq!(response.status.as_u16(), 401);
    }
    assert_eq!(
        idp.jwks_fetches(),
        before,
        "five attacker-chosen key ids must not cause five JWKS fetches"
    );

    // OUTAGE: the cached keys keep a sign-in working.
    idp.set_jwks_down(true);
    let (_, third) = start_login(&app, "").await;
    let third = third.expect("login redirects");
    idp.grant(
        "c3",
        Grant {
            id_token: new.mint(&claims(&third.nonce)),
            code_challenge: None,
            redirect_uri: None,
        },
    );
    assert_eq!(
        finish_login(&app, &third, "c3").await.status.as_u16(),
        303,
        "a JWKS outage must not log everyone out while the cache is fresh"
    );
}

/// **A provider that cannot be reached, or that names another issuer, fails
/// closed.**
#[tokio::test]
async fn a_discovery_outage_or_a_mismatched_issuer_fails_closed() {
    let key = TestKey::ec("k-ec-1");
    let idp = MockIdp::new(ISSUER, &[&key]);
    let app = app(&idp);

    idp.set_discovery_down(true);
    let (response, _) = start_login(&app, "").await;
    assert_eq!(response.status.as_u16(), 503);
    assert_eq!(response.code(), "kubernetes_unavailable");

    idp.set_discovery_down(false);
    idp.set_issuer_override(Some("https://someone-else.test"));
    let (response, _) = start_login(&app, "").await;
    assert_eq!(
        response.status.as_u16(),
        503,
        "a discovery document naming another issuer is not this issuer's metadata"
    );
}

/// **A provider that answers the callback with `error=` is a refusal whose
/// detail names nothing the provider wrote.**
#[tokio::test]
async fn a_provider_error_is_reported_without_echoing_it() {
    let key = TestKey::ec("k-ec-1");
    let idp = MockIdp::new(ISSUER, &[&key]);
    let app = app(&idp);
    let (_, started) = start_login(&app, "").await;
    let started = started.expect("login redirects");
    let response = app
        .app
        .send(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/auth/callback?error=access_denied&error_description=Ada%20is%20fired&state={}",
                    started.state
                ))
                .header("host", SHARED_HOST)
                .header("cookie", &started.cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(response.status.as_u16(), 401);
    let body = String::from_utf8_lossy(&response.body);
    assert!(!body.contains("Ada is fired"), "{body}");
    assert!(!body.contains("access_denied"), "{body}");
    assert_eq!(idp.token_calls(), 0);
}

/// **The login surface is rate limited per peer.**
///
/// The in-process router carries no peer address, so this drives the limiter
/// the route uses rather than the route — the route's use of it is covered by
/// `check_login_rate` having exactly one caller shape, and by the live
/// exercise.
#[tokio::test]
async fn the_login_surface_is_rate_limited() {
    use logweir_api::auth::ratelimit::{Decision, RateLimiter};
    let limiter = RateLimiter::for_login();
    let peer = std::net::IpAddr::from([203, 0, 113, 7]);
    let mut allowed = 0;
    let mut limited = 0;
    for _ in 0..(logweir_api::auth::ratelimit::LOGIN_PER_WINDOW + 5) {
        match limiter.check(peer) {
            Decision::Allowed => allowed += 1,
            Decision::Limited { .. } => limited += 1,
        }
    }
    assert_eq!(allowed, logweir_api::auth::ratelimit::LOGIN_PER_WINDOW);
    assert_eq!(limited, 5);
    // Another peer is unaffected.
    assert_eq!(
        limiter.check(std::net::IpAddr::from([203, 0, 113, 8])),
        Decision::Allowed
    );
}

/// **The administrator's algorithm allowlist is load-bearing: a token that is
/// perfectly valid under a key the provider publishes is still refused when its
/// `alg` is not on the list.**
///
/// REGRESSION REASON. A planted mutant that deleted the allowlist check
/// SURVIVED the fifteen cases above. It survived honestly: `alg: none` and
/// `HS256` have no matching key family, so `select_key` refuses them anyway,
/// and every case there asserts only that the sign-in failed. The check the
/// allowlist actually performs is the one this test drives — RS256 refused by
/// an installation that configured ES256 only, with an RSA key published and
/// the signature genuinely valid — and nothing else in the suite covered it.
#[tokio::test]
async fn an_algorithm_off_the_allowlist_is_refused_even_with_a_published_key() {
    let ec = TestKey::ec("k-ec-1");
    let rsa = TestKey::rsa("k-rsa-1");
    // The provider publishes BOTH keys; the administrator allows only ES256.
    let idp = MockIdp::new(ISSUER, &[&ec, &rsa]);
    let restricted = SharedApp::new(
        FakeKube::new(),
        idp.clone(),
        SharedOptions {
            bindings: support::default_bindings(),
            allowed_algorithms: vec!["ES256".into()],
            ..SharedOptions::default()
        },
    );

    let (_, started) = start_login(&restricted, "").await;
    let started = started.expect("login redirects");
    idp.grant(
        "code-rs256",
        Grant {
            id_token: rsa.mint(&claims(&started.nonce)),
            code_challenge: None,
            redirect_uri: None,
        },
    );
    let refused = finish_login(&restricted, &started, "code-rs256").await;
    assert_eq!(
        refused.status.as_u16(),
        401,
        "RS256 was accepted by an ES256-only installation"
    );
    assert!(session_cookie_of(&refused).is_none());

    // The SAME token signs in where RS256 is allowed, which is what makes the
    // refusal above the allowlist's doing and not a broken RSA path.
    let permissive = SharedApp::new(
        FakeKube::new(),
        idp.clone(),
        SharedOptions {
            bindings: support::default_bindings(),
            allowed_algorithms: vec!["RS256".into(), "ES256".into()],
            ..SharedOptions::default()
        },
    );
    let (_, started) = start_login(&permissive, "").await;
    let started = started.expect("login redirects");
    idp.grant(
        "code-rs256-ok",
        Grant {
            id_token: rsa.mint(&claims(&started.nonce)),
            code_challenge: None,
            redirect_uri: None,
        },
    );
    let accepted = finish_login(&permissive, &started, "code-rs256-ok").await;
    assert_eq!(accepted.status.as_u16(), 303, "{}", accepted.code());
    assert!(session_cookie_of(&accepted).is_some());

    // And an ES256 token still works on the restricted installation.
    let (_, started) = start_login(&restricted, "").await;
    let started = started.expect("login redirects");
    idp.grant(
        "code-es256",
        Grant {
            id_token: ec.mint(&claims(&started.nonce)),
            code_challenge: None,
            redirect_uri: None,
        },
    );
    assert_eq!(
        finish_login(&restricted, &started, "code-es256")
            .await
            .status
            .as_u16(),
        303
    );
}
