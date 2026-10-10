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
//! and the callback clears it on success and on every `401` refusal (the
//! refusal's clear reaches the browser since FX-32: the problem rendering
//! keeps the handler's `Set-Cookie`). A `429` from the sign-in limit and a
//! `405` clear nothing, and should not: neither ends the attempt.
//!
//! THAT CLEARING IS ADVICE TO THE BROWSER, so on its own it ends the attempt
//! for an honest client and nothing more. What the cookie carries is the
//! `state`↔browser binding that defeats login CSRF — an attacker's code cannot
//! be paired with a victim's cookie, because the `state` in it is not the
//! attacker's — and that is asserted three ways in `tests/oidc_login.rs`
//! (wrong `state`, no cookie, another login's cookie), each also asserting the
//! code was never exchanged.
//!
//! A `state` IS SINGLE-USE ON EACH REPLICA (FX-13a), and the provider's
//! single-use code is the backstop across replicas. A sealed cookie cannot
//! remember being used, so before FX-13a someone who kept a copy could drive
//! the callback with it for its whole 600 seconds, each callback a token
//! request to the provider authenticated as this client. Now, once the cookie
//! has opened and its `state` matched, and BEFORE the code is exchanged, the
//! callback redeems the state in this process's [`UsedStates`] — one lock, so
//! of two callbacks with one state on this replica exactly one gets past it —
//! and a state this replica already redeemed is refused `login_state_replayed`,
//! audited, with its cookie cleared and no token request. Nothing is written
//! anywhere else: no Kubernetes object, no shared store. So a replay that
//! reaches ANOTHER replica still reaches the token endpoint: one token request
//! per replica (that callback redeems the state there too), and again after
//! that replica restarts or evicts the entry. The provider refuses an
//! authorization code it has already exchanged (RFC 6749 §4.1.2: a code is
//! single-use), so a replay cannot obtain a second sign-in from a code the
//! provider has exchanged. A CODE THE PROVIDER HAS NOT CONSUMED IS NOT
//! COVERED: when the first callback's exchange fails without the provider
//! consuming the code, the same cookie and URL still sign in on another
//! replica, exactly as before FX-13a. The record never refuses a sign-in for
//! lack of room: when it is full the oldest entry is forgotten, announced
//! once a minute.
//!
//! THE BROWSER NEVER SEES A PROVIDER TOKEN, and the redirect that ends a
//! successful login carries no fragment, no query and no credential: it is
//! `303 See Other` to a path on this origin, with the session cookie in a
//! `Set-Cookie` header.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

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

/// The most redeemed sign-in states one console process remembers (FX-13a):
/// 65,536 entries of a 128-bit digest and an expiry, about 4 MiB at most
/// (a hash set of 131,072 buckets and a deque of 65,536 32-byte slots).
///
/// A BOUND THAT NEVER REFUSES. When the record is full, the OLDEST entry is
/// forgotten to make room ([`Redemption::First`] says so), never the new
/// sign-in: a full record that refused would let anyone who can mint login
/// cookies — one unauthenticated `/auth/login` each — lock every operator out,
/// which FX-13 rules out. A forgotten state can be replayed on this replica
/// once more: that replay costs one token request, and it cannot obtain a
/// second sign-in from a code the provider has exchanged (a code is
/// single-use there).
pub const MAX_USED_STATES: usize = 65_536;

/// What [`UsedStates::redeem`] decided for one callback's `state`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Redemption {
    /// This replica had not redeemed the state: it is now recorded and the
    /// callback may exchange its code.
    First {
        /// A state whose login could still open was forgotten to make room.
        evicted: bool,
        /// The first such eviction this minute: say so in the log.
        announce: bool,
    },
    /// This replica already redeemed the state: refuse, with no token request.
    Replayed,
}

#[derive(Debug, Default)]
struct Record {
    /// The digests this process remembers.
    digests: HashSet<u128>,
    /// The same digests in the order they were redeemed, each with the second
    /// its login state stops opening. The front is forgotten first.
    order: VecDeque<(i64, u128)>,
    /// The minute whose first eviction was announced.
    announced: Option<i64>,
}

/// FX-13a: the sign-in states this console process has redeemed, in memory.
///
/// KEYED BY A DIGEST, NOT THE STATE: 128 bits of SHA-256 over a domain label
/// and the `state`, so the process never keeps the value itself. An entry is
/// forgotten once its login state could no longer open (`iat` plus
/// [`session::LOGIN_STATE_SECONDS`]) and the entries redeemed before it have
/// gone, or earlier when the record is full. From the second its login state
/// could no longer open, the cookie is refused before this record is asked,
/// so forgetting the entry then loses nothing; one that stays longer, behind
/// an older entry that expires later, only holds a slot.
pub struct UsedStates {
    capacity: usize,
    record: Mutex<Record>,
}

// `Debug` is HAND-WRITTEN: the bound and the count, never the up to 65,536
// digests a derived one would print into any `{:?}` that reached it.
impl std::fmt::Debug for UsedStates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UsedStates")
            .field("capacity", &self.capacity)
            .field("len", &self.len())
            .finish()
    }
}

impl Default for UsedStates {
    fn default() -> Self {
        Self::new()
    }
}

impl UsedStates {
    /// A record of [`MAX_USED_STATES`] entries.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(MAX_USED_STATES)
    }

    /// A record of `capacity` entries (at least one). Tests shrink it.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            record: Mutex::new(Record::default()),
        }
    }

    /// The record's bound.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// How many redeemed states this process remembers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().digests.len()
    }

    /// Whether this process remembers no redeemed state.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Redeem one callback's `state`, whose login stops opening at
    /// `expires_at` (Unix seconds), at `now`.
    ///
    /// ONE LOCK, ONE DECISION: the check and the record are made together, so
    /// of two callbacks with one state on this replica exactly one is
    /// [`Redemption::First`], before either reaches the provider.
    pub fn redeem(&self, state: &str, expires_at: i64, now: i64) -> Redemption {
        let digest = Self::digest(state);
        let mut record = self.lock();
        while record.order.front().is_some_and(|(at, _)| *at <= now) {
            if let Some((_, gone)) = record.order.pop_front() {
                record.digests.remove(&gone);
            }
        }
        if record.digests.contains(&digest) {
            return Redemption::Replayed;
        }
        let mut evicted = false;
        while record.digests.len() >= self.capacity {
            let Some((at, oldest)) = record.order.pop_front() else {
                break;
            };
            record.digests.remove(&oldest);
            evicted |= at > now;
        }
        record.digests.insert(digest);
        record.order.push_back((expires_at, digest));
        let minute = now.div_euclid(60);
        let announce = evicted && record.announced != Some(minute);
        if announce {
            record.announced = Some(minute);
        }
        Redemption::First { evicted, announce }
    }

    fn digest(state: &str) -> u128 {
        use sha2::{Digest as _, Sha256};
        let mut hash = Sha256::new();
        hash.update(b"logweir-api/used-sign-in-state/v1\n");
        hash.update(state.as_bytes());
        let bytes = hash.finalize();
        let mut first = [0u8; 16];
        first.copy_from_slice(&bytes[..16]);
        u128::from_be_bytes(first)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Record> {
        self.record
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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

    // FX-13a: REDEEM THE STATE ON THIS REPLICA BEFORE THE CODE IS EXCHANGED.
    // Nothing below this block runs for a state this replica already redeemed
    // — that includes the token request. See the module documentation for
    // what holds across replicas (the provider's single-use code).
    match shared.used_states.redeem(
        &login_state.state,
        login_state.iat + session::LOGIN_STATE_SECONDS,
        state.now().timestamp(),
    ) {
        Redemption::Replayed => {
            tracing::warn!(
                "a sign-in state was presented again after this replica redeemed it; refused \
                 before any token request (login_state_replayed)"
            );
            return refuse(
                "login_state_replayed",
                "This sign-in was already used. Start again at /auth/login.",
            );
        }
        Redemption::First { evicted, announce } => {
            if evicted {
                audit.note("usedSignInStates", "full");
            }
            if announce {
                tracing::warn!(
                    capacity = shared.used_states.capacity(),
                    "this console's record of redeemed sign-in states is full: the oldest is \
                     forgotten to make room, never a new sign-in refused; a replay of a forgotten \
                     state costs one token request, and the provider refuses a code it has \
                     exchanged (audit note usedSignInStates=full); this is logged once a minute"
                );
            }
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

    const T0: i64 = 1_800_000_000;

    /// **A state this replica redeemed is a replay until its login could no
    /// longer open; then, with nothing redeemed before it left, it is
    /// forgotten.**
    #[test]
    fn a_redeemed_state_is_a_replay_until_its_login_expires() {
        let used = UsedStates::with_capacity(10);
        let first = Redemption::First {
            evicted: false,
            announce: false,
        };
        assert_eq!(used.redeem("s-1", T0 + 600, T0), first);
        assert_eq!(used.redeem("s-1", T0 + 600, T0 + 1), Redemption::Replayed);
        assert_eq!(used.redeem("s-1", T0 + 600, T0 + 599), Redemption::Replayed);
        assert_eq!(used.redeem("s-2", T0 + 610, T0 + 10), first);
        assert_eq!(used.len(), 2);
        // At `s-1`'s expiry its cookie can no longer open, so it is swept.
        assert_eq!(used.redeem("s-3", T0 + 1200, T0 + 600), first);
        assert_eq!(used.len(), 2, "`s-1` forgotten, `s-3` recorded");
    }

    /// **A full record forgets its OLDEST entry and never refuses a new
    /// sign-in; a live eviction is announced once a minute.**
    #[test]
    fn a_full_record_evicts_the_oldest_and_never_refuses() {
        let used = UsedStates::with_capacity(2);
        let plain = Redemption::First {
            evicted: false,
            announce: false,
        };
        assert_eq!(used.redeem("a", T0 + 600, T0), plain);
        assert_eq!(used.redeem("b", T0 + 600, T0), plain);
        assert_eq!(
            used.redeem("c", T0 + 600, T0 + 1),
            Redemption::First {
                evicted: true,
                announce: true
            },
            "full: `a`, the oldest, makes room"
        );
        assert_eq!(used.len(), 2, "the bound holds");
        assert_eq!(
            used.redeem("b", T0 + 600, T0 + 2),
            Redemption::Replayed,
            "the newer entries are kept"
        );
        assert_eq!(used.redeem("c", T0 + 600, T0 + 2), Redemption::Replayed);
        assert_eq!(
            used.redeem("d", T0 + 600, T0 + 3),
            Redemption::First {
                evicted: true,
                announce: false
            },
            "announced once a minute, not once an eviction"
        );
        assert_eq!(
            used.redeem("a", T0 + 600, T0 + 61),
            Redemption::First {
                evicted: true,
                announce: true
            },
            "a forgotten state is not refused here again (the provider's code is the \
             backstop), and a new minute announces again"
        );
        // An entry that has expired is swept, not evicted: no announcement.
        let used = UsedStates::with_capacity(1);
        assert_eq!(used.redeem("x", T0 + 600, T0), plain);
        assert_eq!(used.redeem("y", T0 + 1300, T0 + 700), plain);
    }

    /// **Of eight threads redeeming one state at once, exactly one is first.**
    ///
    /// REGRESSION REASON (FX-13a review, M1). The router rows run two
    /// callbacks on a current-thread runtime, where they interleave only at
    /// an `await`. `redeem` has none, so a check and a mark split across two
    /// lock acquisitions passed every one of them, while production runs a
    /// multi-thread runtime. Real threads released together by a barrier see
    /// the split: two of them pass the check before either records the state.
    #[test]
    fn of_eight_threads_redeeming_one_state_exactly_one_is_first() {
        for round in 0..300 {
            let used = Arc::new(UsedStates::with_capacity(8));
            let barrier = Arc::new(std::sync::Barrier::new(8));
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let (used, barrier) = (Arc::clone(&used), Arc::clone(&barrier));
                    std::thread::spawn(move || {
                        barrier.wait();
                        used.redeem("one-state", T0 + 600, T0)
                    })
                })
                .collect();
            let firsts = handles
                .into_iter()
                .map(|handle| handle.join().expect("a redeeming thread panicked"))
                .filter(|redemption| matches!(redemption, Redemption::First { .. }))
                .count();
            assert_eq!(
                firsts, 1,
                "round {round}: {firsts} of eight threads were first"
            );
            assert_eq!(used.len(), 1, "round {round}: one state, one entry");
        }
    }

    /// **The record holds a digest, never the state, and a capacity of zero is
    /// still a record.**
    #[test]
    fn the_record_keeps_a_digest_and_at_least_one_entry() {
        assert_eq!(UsedStates::with_capacity(0).capacity(), 1);
        assert_eq!(UsedStates::new().capacity(), MAX_USED_STATES);
        assert_eq!(MAX_USED_STATES, 65_536);
        let used = UsedStates::with_capacity(4);
        let state = "a-state-of-forty-three-characters-xxxxxxxxx";
        used.redeem(state, T0 + 600, T0);
        let shown = format!("{used:?}");
        assert_eq!(shown, "UsedStates { capacity: 4, len: 1 }");
        assert!(!shown.contains(state), "{shown}");
        assert_ne!(UsedStates::digest(state), UsedStates::digest("another"));
    }

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
