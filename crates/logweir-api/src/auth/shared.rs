//! The shared-mode authenticator: an OIDC session cookie plus a synchronizer
//! CSRF token.
//!
//! THE THREE CHECKS AN UNSAFE METHOD PASSES, IN ORDER, AND WHY THAT ORDER.
//!
//! 1. `crate::http::unsafe_request_guard` — `Origin` exactly equal to the
//!    configured public origin, and `Content-Type: application/json`. This runs
//!    BEFORE any extractor, so a cross-origin form post (which cannot set
//!    either) is refused before this module sees it. There is no credentialed
//!    CORS anywhere, so a cross-origin `fetch` never gets to read a response
//!    either.
//! 2. [`SessionAuthenticator::authenticate`] — the sealed cookie opens, its key
//!    version is current, and it is not past its signed expiry.
//! 3. [`SessionAuthenticator::verify_unsafe`] — `X-CSRF-Token` equals the
//!    synchronizer token derived from THIS session's id, compared in constant
//!    time. `SameSite=Lax` already stops the cross-site form case; the
//!    synchronizer token is what covers the rest, including a same-site page
//!    the browser trusts more than it should.
//!
//! WHY `session_expired` AND NOT `unauthenticated` FOR A ROTATED KEY. A cookie
//! sealed under key version 1 presented to a process holding version 2 is, from
//! the browser's point of view, exactly an expired session: sign in again. It
//! is reported as `session_expired` so the UI does the right thing, and the
//! audit record carries the real reason.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use http::request::Parts;

use super::keys::CookieKeys;
use super::session::{self, SessionError};
use super::{Actor, AuthenticationMode, Authenticator};
use crate::app::Clock;
use crate::problem::{ApiError, FieldError, ProblemCode};

/// Where an unauthenticated browser is sent to sign in.
pub const LOGIN_PATH: &str = "/auth/login";

/// The shared-mode authenticator.
pub struct SessionAuthenticator {
    keys: Arc<CookieKeys>,
    clock: Arc<dyn Clock>,
}

impl SessionAuthenticator {
    /// An authenticator over the session key and a clock.
    #[must_use]
    pub fn new(keys: Arc<CookieKeys>, clock: Arc<dyn Clock>) -> Self {
        Self { keys, clock }
    }

    fn now(&self) -> DateTime<Utc> {
        self.clock.now()
    }

    /// The session claims of a request, if it carries a live session.
    ///
    /// # Errors
    ///
    /// [`SessionError`].
    pub fn claims(&self, parts: &Parts) -> Result<session::SessionClaims, SessionError> {
        session::from_headers(&self.keys, &parts.headers, self.now())
    }
}

impl Authenticator for SessionAuthenticator {
    fn mode(&self) -> AuthenticationMode {
        AuthenticationMode::Oidc
    }

    fn authenticate(&self, parts: &Parts) -> Result<Actor, ApiError> {
        match self.claims(parts) {
            Ok(claims) => Ok(Actor {
                issuer: claims.iss,
                subject: claims.sub,
                display_name: claims.name,
                groups: claims.groups,
                session_id: Some(claims.sid),
                audit: Arc::new(crate::audit::AuditContext::default()),
            }),
            Err(SessionError::Absent) => Err(ApiError::new(
                ProblemCode::Unauthenticated,
                "This request carries no session. Sign in at /auth/login.",
            )),
            Err(SessionError::Expired | SessionError::NotAuthentic) => Err(ApiError::new(
                ProblemCode::SessionExpired,
                "The session is no longer valid. Sign in again at /auth/login.",
            )),
        }
    }

    fn verify_unsafe(&self, parts: &Parts, actor: &Actor) -> Result<(), ApiError> {
        let Some(session_id) = actor.session_id.as_deref() else {
            return Err(ApiError::new(
                ProblemCode::Forbidden,
                "This request has no session to bind a CSRF token to.",
            ));
        };
        // Exactly one header. Two of them is a request assembled by something
        // that is not a browser following the contract.
        let mut values = parts.headers.get_all(session::CSRF_HEADER).iter();
        let presented = match (values.next(), values.next()) {
            (Some(value), None) => value.to_str().unwrap_or_default(),
            _ => "",
        };
        if self.keys.csrf_token_matches(session_id, presented) {
            return Ok(());
        }
        let mut error = ApiError::new(
            ProblemCode::Forbidden,
            "Unsafe requests must carry the session's synchronizer token in X-CSRF-Token; read \
             it from GET /api/v1/session.",
        );
        error.errors.push(FieldError::new(
            session::CSRF_HEADER,
            if presented.is_empty() {
                "required"
            } else {
                "invalid"
            },
            "the synchronizer token does not match this session",
        ));
        Err(error)
    }

    fn session_expiry(&self, parts: &Parts) -> Option<DateTime<Utc>> {
        self.claims(parts).ok().map(|c| c.expires_at())
    }

    fn csrf_token(&self, _parts: &Parts, actor: &Actor) -> Option<String> {
        actor
            .session_id
            .as_deref()
            .map(|sid| self.keys.csrf_token(sid))
    }

    fn login_path(&self) -> Option<&'static str> {
        Some(LOGIN_PATH)
    }
}

#[cfg(test)]
mod tests {
    use super::super::keys::VersionedKey;
    use super::*;
    use crate::auth::session::SessionClaims;

    struct Fixed(DateTime<Utc>);

    impl Clock for Fixed {
        fn now(&self) -> DateTime<Utc> {
            self.0
        }
    }

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(seconds, 0).unwrap()
    }

    fn identity() -> super::super::oidc::Identity {
        super::super::oidc::Identity {
            issuer: "https://idp.example".into(),
            subject: "u-1".into(),
            display_name: "Ada".into(),
            groups: vec!["g-a".into()],
            auth_time: at(900),
        }
    }

    fn authenticator(version: u32, now: i64) -> (SessionAuthenticator, Arc<CookieKeys>) {
        let keys = Arc::new(CookieKeys::new(&VersionedKey::from_parts(
            version,
            vec![0x44; 32],
        )));
        (
            SessionAuthenticator::new(Arc::clone(&keys), Arc::new(Fixed(at(now)))),
            keys,
        )
    }

    fn parts(cookie: Option<&str>, csrf: Option<&str>, method: &str) -> Parts {
        // Assembled field by field: `tests/linkage.rs` forbids the request
        // builder spelling crate-wide, including in tests.
        let mut request = http::Request::new(());
        *request.method_mut() = method.parse().unwrap();
        *request.uri_mut() = "/api/v1/x".parse().unwrap();
        if let Some(cookie) = cookie {
            request
                .headers_mut()
                .insert(http::header::COOKIE, cookie.parse().unwrap());
        }
        if let Some(csrf) = csrf {
            request
                .headers_mut()
                .insert(session::CSRF_HEADER, csrf.parse().unwrap());
        }
        request.into_parts().0
    }

    fn cookie_for(keys: &CookieKeys, now: i64) -> String {
        let claims =
            SessionClaims::issue(&identity(), "sid-1".into(), keys.version(), at(now), 900);
        session::set_cookie(keys, &claims, at(now))
            .split(';')
            .next()
            .unwrap()
            .to_string()
    }

    #[test]
    fn a_live_session_authenticates_and_carries_its_groups() {
        let (auth, keys) = authenticator(1, 1000);
        let cookie = cookie_for(&keys, 1000);
        let actor = auth
            .authenticate(&parts(Some(&cookie), None, "GET"))
            .unwrap();
        assert_eq!(actor.id(), "https://idp.example#u-1");
        assert_eq!(actor.groups, vec!["g-a".to_string()]);
        assert_eq!(actor.session_id.as_deref(), Some("sid-1"));
        assert_eq!(
            auth.session_expiry(&parts(Some(&cookie), None, "GET")),
            Some(at(1900))
        );
    }

    #[test]
    fn no_cookie_is_unauthenticated_and_a_stale_or_foreign_one_is_session_expired() {
        let (auth, keys) = authenticator(1, 1000);
        assert_eq!(
            auth.authenticate(&parts(None, None, "GET"))
                .unwrap_err()
                .code,
            ProblemCode::Unauthenticated
        );
        let cookie = cookie_for(&keys, 1000);

        let (later, _) = authenticator(1, 2000);
        // A different process with the SAME key, past the expiry.
        let later = SessionAuthenticator::new(Arc::clone(&keys), later.clock);
        assert_eq!(
            later
                .authenticate(&parts(Some(&cookie), None, "GET"))
                .unwrap_err()
                .code,
            ProblemCode::SessionExpired
        );

        // A rotated session key.
        let (rotated, _) = authenticator(2, 1000);
        assert_eq!(
            rotated
                .authenticate(&parts(Some(&cookie), None, "GET"))
                .unwrap_err()
                .code,
            ProblemCode::SessionExpired
        );

        // Garbage.
        assert_eq!(
            auth.authenticate(&parts(
                Some("__Host-logweir_session=not-a-cookie"),
                None,
                "GET"
            ))
            .unwrap_err()
            .code,
            ProblemCode::SessionExpired
        );
    }

    #[test]
    fn an_unsafe_method_needs_this_sessions_synchronizer_token() {
        let (auth, keys) = authenticator(1, 1000);
        let cookie = cookie_for(&keys, 1000);
        let actor = auth
            .authenticate(&parts(Some(&cookie), None, "POST"))
            .unwrap();
        let token = keys.csrf_token("sid-1");

        assert!(auth
            .verify_unsafe(&parts(Some(&cookie), Some(&token), "POST"), &actor)
            .is_ok());

        for (label, csrf) in [
            ("absent", None),
            ("empty", Some("")),
            ("wrong", Some("not-the-token")),
            ("another session's", Some(keys.csrf_token("sid-2").as_str())),
        ] {
            let error = auth
                .verify_unsafe(&parts(Some(&cookie), csrf, "POST"), &actor)
                .unwrap_err();
            assert_eq!(error.code, ProblemCode::Forbidden, "{label}");
            assert_eq!(error.errors[0].field, session::CSRF_HEADER, "{label}");
        }

        // Two tokens are no token.
        let mut two = parts(Some(&cookie), Some(&token), "POST");
        two.headers
            .append(session::CSRF_HEADER, token.parse().unwrap());
        assert_eq!(
            auth.verify_unsafe(&two, &actor).unwrap_err().code,
            ProblemCode::Forbidden
        );
    }

    #[test]
    fn the_csrf_token_this_mode_hands_out_is_the_one_it_checks() {
        let (auth, keys) = authenticator(1, 1000);
        let cookie = cookie_for(&keys, 1000);
        let actor = auth
            .authenticate(&parts(Some(&cookie), None, "GET"))
            .unwrap();
        let handed = auth
            .csrf_token(&parts(Some(&cookie), None, "GET"), &actor)
            .expect("shared mode hands out a token");
        assert!(auth
            .verify_unsafe(&parts(Some(&cookie), Some(&handed), "POST"), &actor)
            .is_ok());
        assert_eq!(auth.login_path(), Some(LOGIN_PATH));
    }
}
