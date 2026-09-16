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
//! and the callback clears it on success and on every refusal.
//!
//! WHAT THAT CLEARING IS AND IS NOT. It is advice to the browser, so it ends the
//! attempt for an honest client and nothing more: this service keeps no record
//! of a consumed `state`, so someone holding BOTH the login cookie and the code
//! could re-drive the callback. The bound on replaying a code is the provider's
//! single-use code, which is where OAuth puts it. What the cookie DOES carry is
//! the `state`↔browser binding that defeats login CSRF — an attacker's code
//! cannot be paired with a victim's cookie, because the `state` in it is not
//! the attacker's — and that is asserted three ways in `tests/oidc_login.rs`
//! (wrong `state`, no cookie, another login's cookie), each also asserting the
//! code was never exchanged.
//!
//! THE BROWSER NEVER SEES A PROVIDER TOKEN, and the redirect that ends a
//! successful login carries no fragment, no query and no credential: it is
//! `303 See Other` to a path on this origin, with the session cookie in a
//! `Set-Cookie` header.

use std::sync::Arc;

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use http::{header, HeaderValue, StatusCode};

use super::session::{self, LoginState, SessionClaims};
use crate::app::AppState;
use crate::audit::Decision;
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
            tracing::warn!(reason = %error.code(), "the identity provider is not usable");
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

    let refuse = |code: &'static str, detail: &'static str| -> Response {
        audit.set_failure(code);
        let mut response = ApiError::new(ProblemCode::Unauthenticated, detail).into_response();
        // Whatever went wrong, the login attempt is over: clear its cookie so
        // a retry starts a fresh `state`/`nonce`/verifier.
        if let Ok(value) = HeaderValue::from_str(&session::clear_cookie(session::LOGIN_COOKIE)) {
            response.headers_mut().append(header::SET_COOKIE, value);
        }
        response
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

    let identity = match shared
        .provider
        .exchange_and_validate(code, &login_state.verifier, &login_state.nonce, state.now())
        .await
    {
        Ok(identity) => identity,
        Err(error) => {
            // The variant's code, never the token, the code or the provider's
            // body.
            audit.set_failure(error.code());
            tracing::warn!(reason = %error.code(), "a sign-in was refused");
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
