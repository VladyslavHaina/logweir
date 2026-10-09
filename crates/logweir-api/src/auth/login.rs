//! `/auth/login`, `/auth/callback` and the logout command.
//!
//! THESE THREE ROUTES EXIST ONLY IN SHARED MODE. `crate::app::router` adds them
//! when the state carries a [`crate::app::SharedMode`]; in localAdmin mode they
//! are not in the route table at all, so they answer 404 like any other path
//! this service does not serve — not 501, and not a stub.
//!
//! NOTHING HERE READS `Host`, `Forwarded` OR `X-Forwarded-*`. The redirect URI
//! is derived once, at startup, from the administrator's exact `publicBaseUrl`;
//! the authorization URL uses that exact string and the token exchange sends
//! the same one again. Host poisoning therefore cannot move the callback, and a
//! provider that is asked for a different `redirect_uri` than the one
//! registered refuses the exchange — two independent reasons the attack fails.
//!
//! `state`, `nonce` AND THE PKCE VERIFIER LIVE IN A SEALED COOKIE, not in
//! process memory: a login begun on one replica must be finishable on another,
//! and a restart between the redirect and the callback must not strand the
//! browser. The cookie is `__Host-` prefixed, `HttpOnly` and ten-minute-lived,
//! and the callback clears it on success and on every refusal (the refusal's
//! clear reaches the browser since FX-32: the problem rendering keeps the
//! handler's `Set-Cookie`).
//!
//! THAT CLEARING IS ADVICE TO THE BROWSER, so on its own it ends the attempt
//! for an honest client and nothing more. What the cookie carries is the
//! `state`↔browser binding that defeats login CSRF — an attacker's code cannot
//! be paired with a victim's cookie, because the `state` in it is not the
//! attacker's — and that is asserted three ways in `tests/oidc_login.rs`
//! (wrong `state`, no cookie, another login's cookie), each also asserting the
//! code was never exchanged.
//!
//! THE STATE IS SINGLE-USE, ON EVERY REPLICA (FX-13a). A sealed cookie cannot
//! remember being used, so someone who keeps a copy of it could once re-drive
//! the callback for its whole 600 seconds, and each callback was a token
//! request to the provider, authenticated as this client. Now, once the
//! cookie has opened and its `state` matches, and BEFORE the code is
//! exchanged, the callback CLAIMS the state: it creates one Kubernetes Event
//! whose name is a keyed hash of the `state`
//! ([`crate::kube::KubeAdapter::claim_sign_in_state`]). The API server's create
//! is atomic per name, so exactly one callback per `state` is ever first —
//! whichever replica it reaches, and however many arrive at once. Every other
//! one is refused `login_state_replayed`, audited, with its cookie cleared and
//! with no token request. A claim that cannot be recorded refuses the sign-in
//! (`login_state_claim_failed`, 503) rather than exchange an unclaimed state,
//! and readiness dry-runs a claim until one succeeds, so a console without the
//! grant never takes a sign-in. The claim names the replica and nothing about
//! the state, and the API server expires it (`--event-ttl`, one hour by
//! default) long after the 600 seconds the state can live.
//!
//! THE BROWSER NEVER SEES A PROVIDER TOKEN, and the redirect that ends a
//! successful login carries no fragment, no query and no credential: it is
//! `303 See Other` to a path on this origin, with the session cookie in a
//! `Set-Cookie` header.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use http::{header, HeaderValue, StatusCode};

use super::keys::CookieKeys;
use super::session::{self, LoginState, SessionClaims};
use crate::app::AppState;
use crate::audit::Decision;
use crate::kube::{KubeAdapter, KubeFailure, SignInClaim, SIGN_IN_CLAIM_PROBE_NAME};
use crate::problem::{ApiError, ProblemCode};

/// The login route.
pub const LOGIN_PATH: &str = "/auth/login";
/// The callback route.
pub const CALLBACK_PATH: &str = "/auth/callback";
/// The path appended to `publicBaseUrl` to form the exact redirect URI.
pub const CALLBACK_SUFFIX: &str = "/auth/callback";
/// Where a successful login lands.
pub const DEFAULT_NEXT: &str = "/ui/";

/// Bytes of entropy in `state`, `nonce`, the PKCE verifier and the session id.
pub const ENTROPY_BYTES: usize = 32;

fn redirect(location: &str, cookies: &[String]) -> Response {
    let mut response = StatusCode::SEE_OTHER.into_response();
    if let Ok(value) = HeaderValue::from_str(location) {
        response.headers_mut().insert(header::LOCATION, value);
    }
    for cookie in cookies {
        if let Ok(value) = HeaderValue::from_str(cookie) {
            response.headers_mut().append(header::SET_COOKIE, value);
        }
    }
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

/// The replica name a claim records when the process has none of its own
/// (`HOSTNAME`, which Kubernetes sets to the Pod's name).
pub const UNNAMED_REPLICA: &str = "logweir-api";

/// FX-13a: where this replica records a redeemed sign-in `state`, as whom,
/// and whether it has shown that it can.
#[derive(Debug)]
pub struct SignInClaims {
    namespace: String,
    replica: String,
    writable: AtomicBool,
}

impl SignInClaims {
    /// Claims recorded in `namespace` (this service's own: the in-cluster
    /// service account's, or the kubeconfig context's) by `replica`. A
    /// replica name that is not a DNS subdomain is recorded as
    /// [`UNNAMED_REPLICA`], so a strange `HOSTNAME` cannot make every claim
    /// invalid.
    #[must_use]
    pub fn new(namespace: impl Into<String>, replica: Option<&str>) -> Self {
        let replica = replica
            .filter(|name| name.len() <= 128 && crate::validate::is_dns_subdomain(name))
            .unwrap_or(UNNAMED_REPLICA)
            .to_string();
        Self {
            namespace: namespace.into(),
            replica,
            writable: AtomicBool::new(false),
        }
    }

    /// The namespace claims are recorded in.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// The replica name claims carry.
    #[must_use]
    pub fn replica(&self) -> &str {
        &self.replica
    }

    /// The claim for one `state`.
    #[must_use]
    pub fn claim_for(&self, keys: &CookieKeys, state: &str, now: DateTime<Utc>) -> SignInClaim {
        SignInClaim::new(
            &self.namespace,
            &keys.sign_in_claim_name(state),
            &self.replica,
            now,
        )
    }

    /// Readiness: whether this replica can record a claim, by a DRY-RUN
    /// create, asked until it succeeds once and then latched.
    ///
    /// The dry run exercises exactly what a sign-in will: the grant, the
    /// namespace, the shape the API server validates and any admission in the
    /// way. A console that would refuse every sign-in with
    /// `login_state_claim_failed` is therefore held out of the rollout
    /// instead. Latched like the provider half of readiness: a later outage
    /// refuses sign-ins by name and does not cut the sessions already issued.
    pub async fn ready(&self, kube: &KubeAdapter, now: DateTime<Utc>) -> bool {
        if self.writable.load(Ordering::Acquire) {
            return true;
        }
        let probe = SignInClaim::new(
            &self.namespace,
            SIGN_IN_CLAIM_PROBE_NAME,
            &self.replica,
            now,
        );
        match kube.claim_sign_in_state(&probe, true).await {
            // A taken name is still an authorized, valid create.
            Ok(()) | Err(KubeFailure::AlreadyExists) => {
                if !self.writable.swap(true, Ordering::AcqRel) {
                    tracing::info!(
                        namespace = %self.namespace,
                        replica = %self.replica,
                        "redeemed sign-in states are recorded as Events in this namespace (FX-13a)"
                    );
                }
                true
            }
            Err(failure) => {
                tracing::warn!(
                    namespace = %self.namespace,
                    failure = ?failure,
                    "this console cannot record a redeemed sign-in state (`create` on `events` in \
                     its own namespace, FX-13a); it stays not ready rather than refuse every \
                     sign-in"
                );
                false
            }
        }
    }
}

/// Where to send the browser after a successful login.
///
/// ONLY A PATH UNDER `/ui/` ON THIS ORIGIN. Anything else — an absolute URL, a
/// protocol-relative `//evil`, a backslash, a control character, a path this
/// service does not serve — collapses to [`DEFAULT_NEXT`]. An open redirect on
/// a login route is how a phishing page borrows a trusted origin, and the whole
/// value of the parameter is convenience.
#[must_use]
pub fn safe_next(raw: Option<&str>) -> String {
    let Some(value) = raw else {
        return DEFAULT_NEXT.to_string();
    };
    let looks_safe = value.len() <= 512
        && value.starts_with("/ui/")
        && !value.starts_with("//")
        && !value.contains('\\')
        && !value.contains("://")
        && value
            .chars()
            .all(|c| !c.is_control() && c != ' ' && c.is_ascii());
    if looks_safe {
        value.to_string()
    } else {
        DEFAULT_NEXT.to_string()
    }
}

/// `GET /auth/login` — start an Authorization Code + PKCE S256 login.
pub async fn login(State(state): State<AppState>, request: axum::extract::Request) -> Response {
    let (parts, _) = request.into_parts();
    let audit = crate::audit::context_of(&parts);
    let Some(shared) = state.shared() else {
        return ApiError::not_found().into_response();
    };
    if let Err(error) = crate::http::check_login_rate(&state, &parts) {
        audit.set_action("auth.login", Decision::Deny);
        audit.set_failure(error.code.as_str());
        return error.into_response();
    }
    audit.set_action("auth.login", Decision::Allow);

    let discovery = match shared.provider.discovery().await {
        Ok(discovery) => discovery,
        Err(error) => {
            audit.set_failure(error.code());
            // The detail says which of a stall (FX-28: `provider_timeout`, the
            // deadline in seconds), a transport cause or an oversized document
            // it was — the same text readiness logs, and like it, no
            // credential: discovery carries none.
            tracing::warn!(
                reason = %error.code(),
                detail = %error,
                "the identity provider is not usable"
            );
            return ApiError::new(
                ProblemCode::KubernetesUnavailable,
                "The identity provider could not be reached. Try again shortly.",
            )
            .into_response();
        }
    };

    let keys = &shared.keys;
    let login_state = LoginState {
        state: keys.random_token(ENTROPY_BYTES),
        nonce: keys.random_token(ENTROPY_BYTES),
        verifier: keys.random_token(ENTROPY_BYTES),
        iat: state.now().timestamp(),
        next: safe_next(
            crate::http::parse_query(parts.uri.query(), &["next"])
                .ok()
                .and_then(|q| q.get("next").cloned())
                .as_deref(),
        ),
    };
    let url = super::oidc::authorization_url(
        &discovery,
        shared.provider.settings(),
        &login_state.state,
        &login_state.nonce,
        &login_state.verifier,
    );
    redirect(&url, &[session::set_login_cookie(keys, &login_state)])
}

/// `GET /auth/callback` — finish a login.
pub async fn callback(State(state): State<AppState>, request: axum::extract::Request) -> Response {
    let (parts, _) = request.into_parts();
    let audit = crate::audit::context_of(&parts);
    let Some(shared) = state.shared() else {
        return ApiError::not_found().into_response();
    };
    if let Err(error) = crate::http::check_login_rate(&state, &parts) {
        audit.set_action("auth.callback", Decision::Deny);
        audit.set_failure(error.code.as_str());
        return error.into_response();
    }
    audit.set_action("auth.callback", Decision::Deny);

    let refuse_as = |problem: ProblemCode, code: &'static str, detail: &'static str| -> Response {
        audit.set_failure(code);
        let mut response = ApiError::new(problem, detail).into_response();
        // Whatever went wrong, the login attempt is over: clear its cookie so
        // a retry starts a fresh `state`/`nonce`/verifier.
        if let Ok(value) = HeaderValue::from_str(&session::clear_cookie(session::LOGIN_COOKIE)) {
            response.headers_mut().append(header::SET_COOKIE, value);
        }
        response
    };
    let refuse = |code: &'static str, detail: &'static str| -> Response {
        refuse_as(ProblemCode::Unauthenticated, code, detail)
    };

    let query = match crate::http::parse_query(
        parts.uri.query(),
        &[
            "code",
            "state",
            "error",
            "error_description",
            "iss",
            "session_state",
        ],
    ) {
        Ok(query) => query,
        Err(_) => {
            return refuse(
                "callback_query_invalid",
                "The callback request is malformed.",
            )
        }
    };
    if query.contains_key("error") {
        // The provider's own `error` code is recorded; its description is not,
        // because providers put arbitrary text there.
        audit.note(
            "providerError",
            query.get("error").map(String::as_str).unwrap_or_default(),
        );
        return refuse(
            "provider_error",
            "The identity provider refused the sign-in.",
        );
    }
    let (Some(code), Some(returned_state)) = (query.get("code"), query.get("state")) else {
        return refuse("callback_incomplete", "The callback request is incomplete.");
    };

    let login_state =
        match session::login_state_from_headers(&shared.keys, &parts.headers, state.now()) {
            Ok(login_state) => login_state,
            Err(session::SessionError::Absent) => return refuse(
                "login_state_absent",
                "This sign-in did not start here, or took too long. Start again at /auth/login.",
            ),
            Err(_) => return refuse(
                "login_state_invalid",
                "This sign-in did not start here, or took too long. Start again at /auth/login.",
            ),
        };
    if !super::keys::constant_time_eq(login_state.state.as_bytes(), returned_state.as_bytes()) {
        return refuse("state_mismatch", "The sign-in state does not match.");
    }

    // FX-13a: CLAIM THE STATE BEFORE THE CODE IS EXCHANGED. Exactly one
    // callback per `state` gets past this line, on any replica: see the
    // module documentation. Nothing below it may run for a state that was not
    // claimed — that includes the token request.
    let claim = shared
        .sign_in_claims
        .claim_for(&shared.keys, &login_state.state, state.now());
    match state.kube().claim_sign_in_state(&claim, false).await {
        Ok(()) => {}
        Err(KubeFailure::AlreadyExists) => {
            tracing::warn!(
                claim = %claim.metadata.name.as_deref().unwrap_or_default(),
                "a sign-in state was presented again after it was redeemed; refused before any \
                 token request (login_state_replayed)"
            );
            return refuse(
                "login_state_replayed",
                "This sign-in was already used. Start again at /auth/login.",
            );
        }
        Err(failure) => {
            tracing::warn!(
                failure = ?failure,
                namespace = %shared.sign_in_claims.namespace(),
                "a sign-in state could not be recorded as redeemed; the sign-in is refused \
                 before any token request (login_state_claim_failed)"
            );
            return refuse_as(
                ProblemCode::KubernetesUnavailable,
                "login_state_claim_failed",
                "The sign-in could not be recorded. Start again at /auth/login.",
            );
        }
    }

    let identity = match shared
        .provider
        .exchange_and_validate(code, &login_state.verifier, &login_state.nonce, state.now())
        .await
    {
        Ok(identity) => identity,
        Err(error) => {
            // The variant's code, never the token, the code or the provider's
            // body. The detail is the variant's own text, which carries none of
            // them either: for a refused exchange it is the provider's HTTP
            // status ("the provider answered HTTP 429"), which is what an
            // operator alerting on the provider throttling this client needs
            // to see (FX-13 review M1) — `code_exchange_failed` alone reads
            // the same for a junk code and for a provider that has stopped
            // serving this client.
            audit.set_failure(error.code());
            tracing::warn!(reason = %error.code(), detail = %error, "a sign-in was refused");
            let mut response = ApiError::new(
                ProblemCode::Unauthenticated,
                "The sign-in could not be completed.",
            )
            .into_response();
            if let Ok(value) = HeaderValue::from_str(&session::clear_cookie(session::LOGIN_COOKIE))
            {
                response.headers_mut().append(header::SET_COOKIE, value);
            }
            return response;
        }
    };

    // THE SESSION CARRIES ONLY BINDABLE GROUPS, AND ONLY IF IT FITS. See
    // `crate::auth::session::MAX_SET_COOKIE_BYTES` and
    // `crate::authz::Authorizer::bindable_groups` (review finding F-2).
    let bindable = state.authorizer().bindable_groups();
    let identity = super::oidc::Identity {
        groups: session::session_groups(&identity.groups, bindable.as_ref()),
        ..identity
    };
    let session_id = shared.keys.random_token(ENTROPY_BYTES);
    let claims = SessionClaims::issue(
        &identity,
        session_id,
        shared.keys.version(),
        state.now(),
        shared.session_max_age_seconds,
    );
    let session_cookie = session::set_cookie(&shared.keys, &claims, state.now());
    if session_cookie.len() > session::MAX_SET_COOKIE_BYTES {
        // A browser would discard this cookie without a word, and the next
        // request would bounce back here forever. Refusing says so once.
        audit.set_actor(
            super::AuthenticationMode::Oidc.as_str(),
            &format!("{}#{}", claims.iss, claims.sub),
            &claims.name,
            Some(&claims.sid),
        );
        audit.set_failure("session_too_large");
        audit.note("sessionCookieBytes", &session_cookie.len().to_string());
        tracing::warn!(
            bytes = session_cookie.len(),
            limit = session::MAX_SET_COOKIE_BYTES,
            groups = claims.groups.len(),
            "refusing a sign-in whose session cookie a browser would discard"
        );
        return refuse(
            "session_too_large",
            "The identity claims for this account do not fit in a session cookie. Reduce the \
             group claims the identity provider sends, or bind fewer groups.",
        );
    }
    audit.set_actor(
        super::AuthenticationMode::Oidc.as_str(),
        &format!("{}#{}", claims.iss, claims.sub),
        &claims.name,
        Some(&claims.sid),
    );
    audit.set_action("auth.callback", Decision::Allow);
    tracing::info!(
        actor = %format!("{}#{}", claims.iss, claims.sub),
        session_sha256 = %super::keys::session_id_hash(&claims.sid),
        groups = claims.groups.len(),
        expires_at = claims.exp,
        "a session was issued"
    );
    redirect(
        &login_state.next,
        &[session_cookie, session::clear_cookie(session::LOGIN_COOKIE)],
    )
}

/// `POST /api/v1/session/logout` — clear the session cookie.
///
/// It is an UNSAFE method on purpose: logging someone out is a state change, so
/// it goes through the exact-`Origin`, JSON-content-type and synchronizer-token
/// checks like every other mutation. A cross-site `<img src=".../logout">`
/// therefore cannot log a user out.
pub async fn logout(State(state): State<AppState>, actor: super::Actor) -> Response {
    actor.audit.set_action("auth.logout", Decision::Allow);
    let name = match state.shared() {
        Some(_) => session::SESSION_COOKIE,
        None => return ApiError::not_found().into_response(),
    };
    let mut response = StatusCode::NO_CONTENT.into_response();
    if let Ok(value) = HeaderValue::from_str(&session::clear_cookie(name)) {
        response.headers_mut().append(header::SET_COOKIE, value);
    }
    let _ = Arc::strong_count(&actor.audit);
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_next_parameter_can_only_be_a_ui_path_on_this_origin() {
        assert_eq!(safe_next(None), DEFAULT_NEXT);
        assert_eq!(safe_next(Some("/ui/schedules")), "/ui/schedules");
        assert_eq!(safe_next(Some("/ui/#/backups")), "/ui/#/backups");
        for hostile in [
            "https://evil.example/",
            "//evil.example/",
            "/\\evil.example",
            "/ui/\\..\\x",
            "/api/v1/session",
            "/",
            "javascript:alert(1)",
            "/ui/\nSet-Cookie: x=1",
            "/ui/ä",
        ] {
            assert_eq!(safe_next(Some(hostile)), DEFAULT_NEXT, "{hostile}");
        }
        assert_eq!(
            safe_next(Some(&format!("/ui/{}", "a".repeat(600)))),
            DEFAULT_NEXT
        );
    }
}
