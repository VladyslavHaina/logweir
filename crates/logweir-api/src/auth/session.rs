//! The authenticated, encrypted, stateless session cookie.
//!
//! STATELESS ON PURPOSE. The cookie carries everything a request needs —
//! session id, issuer, subject, the allowed identity claims, issued/expiry/auth
//! times and the key version — sealed with ChaCha20-Poly1305 under the mounted
//! session key. There is no server-side session table, so a restart, a second
//! replica or a rescheduled Pod does not log anyone out and nothing has to be
//! replicated. What it costs is that revocation before expiry is bounded by the
//! expiry itself, which is why [`MAX_SESSION_SECONDS`] is fifteen minutes and
//! why removing a role binding takes effect on the NEXT request (the roles are
//! re-derived per request from the claims plus the current configuration, not
//! frozen into the cookie).
//!
//! WHAT IS NEVER IN IT: an ID token, an access token, a refresh token, a client
//! secret, or anything the provider issued. The browser therefore cannot
//! present a provider credential anywhere, and neither can anyone who steals
//! the cookie.
//!
//! THE COOKIE ATTRIBUTES ARE NOT NEGOTIABLE. `__Host-` prefixed (so a
//! sibling-domain page cannot set it), `Secure`, `HttpOnly`, `SameSite=Lax`,
//! `Path=/`, and NO `Domain`. [`SESSION_ATTRIBUTES`] is one constant used by
//! both the setter and the test, so the two cannot drift.

use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};

use super::keys::CookieKeys;

/// The session cookie's name.
pub const SESSION_COOKIE: &str = "__Host-logweir_session";
/// The login-state cookie's name.
pub const LOGIN_COOKIE: &str = "__Host-logweir_login";
/// The longest a session may live, fifteen minutes.
pub const MAX_SESSION_SECONDS: i64 = 900;
/// The longest a login attempt may take between `/auth/login` and the callback.
pub const LOGIN_STATE_SECONDS: i64 = 600;
/// The synchronizer-token header.
pub const CSRF_HEADER: &str = "x-csrf-token";

/// The largest `Set-Cookie` value this service will emit.
///
/// BROWSERS SILENTLY DROP AN OVERSIZED COOKIE. Chrome and Firefox enforce a
/// ~4096-byte per-cookie ceiling (RFC 6265 §6.1 asks for at least 4096 bytes
/// per cookie and they take that as the cap). A callback that answers `303`
/// with a `Set-Cookie` above it gets no cookie stored, `/ui/` then reads `401`
/// and bounces back to `/auth/login`: an unbreakable sign-in loop with nothing
/// in the log saying why, hitting exactly the large-directory installations
/// shared mode exists for. So the size is measured and the sign-in is REFUSED
/// with a named reason instead. Review finding F-2.
///
/// The margin below 4096 is for the attributes, which are counted here too.
pub const MAX_SET_COOKIE_BYTES: usize = 3900;

/// The attributes both cookies carry, verbatim.
pub const SESSION_ATTRIBUTES: &str = "Path=/; Secure; HttpOnly; SameSite=Lax";

/// The session, as sealed into the cookie.
///
/// The field names are short because every byte is in a cookie a browser sends
/// on every request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionClaims {
    /// The session id. Random, and never logged — only its SHA-256 is.
    pub sid: String,
    /// The OIDC issuer.
    pub iss: String,
    /// The subject within the issuer.
    pub sub: String,
    /// The display claim. Presentation only.
    pub name: String,
    /// The exact group strings the ID token carried.
    pub groups: Vec<String>,
    /// When the session was issued, as a Unix timestamp.
    pub iat: i64,
    /// When the session expires, as a Unix timestamp.
    pub exp: i64,
    /// The provider's authentication time, as a Unix timestamp.
    pub auth: i64,
    /// The session key version that sealed it.
    pub kv: u32,
}

/// The group strings a session will carry: exactly the claimed ones the
/// authorization table could ever match.
///
/// `bindable` is that set; when it is `Some`, everything else is dropped. A
/// claim that cannot grant anything has no business in a cookie — it costs the
/// browser's 4 KB ceiling, and it tells anyone who steals the cookie the whole
/// directory membership of its owner.
///
/// THERE IS NO SIZE BOUND HERE, DELIBERATELY. Dropping a group to make a cookie
/// fit is silently dropping a GRANT: the actor signs in, sees fewer namespaces
/// than it has, and nothing says why. The size decision is made once, in
/// `crate::auth::login::callback`, and its answer is a refusal that names the
/// reason.
#[must_use]
pub fn session_groups(
    claimed: &[String],
    bindable: Option<&std::collections::BTreeSet<String>>,
) -> Vec<String> {
    claimed
        .iter()
        .filter(|group| bindable.is_none_or(|set| set.contains(*group)))
        .cloned()
        .collect()
}

impl SessionClaims {
    /// A new session for a validated identity.
    #[must_use]
    pub fn issue(
        identity: &super::oidc::Identity,
        session_id: String,
        key_version: u32,
        now: DateTime<Utc>,
        max_age_seconds: i64,
    ) -> Self {
        let lifetime = max_age_seconds.clamp(1, MAX_SESSION_SECONDS);
        Self {
            sid: session_id,
            iss: identity.issuer.clone(),
            sub: identity.subject.clone(),
            name: identity.display_name.clone(),
            groups: identity.groups.clone(),
            iat: now.timestamp(),
            exp: (now + TimeDelta::seconds(lifetime)).timestamp(),
            auth: identity.auth_time.timestamp(),
            kv: key_version,
        }
    }

    /// When the session expires.
    #[must_use]
    pub fn expires_at(&self) -> DateTime<Utc> {
        DateTime::from_timestamp(self.exp, 0).unwrap_or_else(Utc::now)
    }

    /// Whether `now` is past the signed expiry.
    #[must_use]
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now.timestamp() >= self.exp
    }
}

/// Why a cookie did not yield a session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionError {
    /// No session cookie was sent.
    Absent,
    /// The cookie is not authentic, or was sealed under another key version.
    NotAuthentic,
    /// The session is past its signed expiry.
    Expired,
}

/// Seal a session into a `Set-Cookie` value.
#[must_use]
pub fn set_cookie(keys: &CookieKeys, claims: &SessionClaims, now: DateTime<Utc>) -> String {
    let plaintext = serde_json::to_vec(claims).expect("session claims are plain JSON");
    let sealed = keys.seal(SESSION_COOKIE, &plaintext);
    let max_age = (claims.exp - now.timestamp()).max(0);
    format!("{SESSION_COOKIE}={sealed}; Max-Age={max_age}; {SESSION_ATTRIBUTES}")
}

/// The `Set-Cookie` value that clears the session.
#[must_use]
pub fn clear_cookie(name: &str) -> String {
    format!("{name}=; Max-Age=0; {SESSION_ATTRIBUTES}")
}

/// Read and open the session cookie.
///
/// # Errors
///
/// [`SessionError`]; a cookie sealed under another key version reads as
/// `NotAuthentic`, which the authenticator reports as `session_expired`
/// because that is what it is from the browser's point of view.
pub fn from_headers(
    keys: &CookieKeys,
    headers: &http::HeaderMap,
    now: DateTime<Utc>,
) -> Result<SessionClaims, SessionError> {
    let value = cookie(headers, SESSION_COOKIE).ok_or(SessionError::Absent)?;
    let plaintext = keys
        .open(SESSION_COOKIE, &value)
        .map_err(|_| SessionError::NotAuthentic)?;
    let claims: SessionClaims =
        serde_json::from_slice(&plaintext).map_err(|_| SessionError::NotAuthentic)?;
    if claims.kv != keys.version() {
        return Err(SessionError::NotAuthentic);
    }
    if claims.is_expired(now) {
        return Err(SessionError::Expired);
    }
    Ok(claims)
}

/// The per-login state, sealed into the login cookie.
///
/// `state`, `nonce` and the PKCE verifier live HERE rather than in server
/// memory for the same reason the session does: a login started on one replica
/// must be finishable on another, and a restart between the redirect and the
/// callback must not strand the browser. The cookie is `__Host-`, `HttpOnly`
/// and short-lived, and it is cleared the moment the callback consumes it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoginState {
    /// The `state` parameter.
    pub state: String,
    /// The `nonce` the ID token must carry.
    pub nonce: String,
    /// The PKCE code verifier.
    pub verifier: String,
    /// When the login started, as a Unix timestamp.
    pub iat: i64,
    /// Where to send the browser after a successful login. Always a path on
    /// this origin; never an absolute URL.
    pub next: String,
}

impl LoginState {
    /// Whether this login attempt is too old.
    #[must_use]
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now.timestamp() >= self.iat + LOGIN_STATE_SECONDS
    }
}

/// Seal a login state into a `Set-Cookie` value.
#[must_use]
pub fn set_login_cookie(keys: &CookieKeys, state: &LoginState) -> String {
    let plaintext = serde_json::to_vec(state).expect("login state is plain JSON");
    let sealed = keys.seal(LOGIN_COOKIE, &plaintext);
    format!("{LOGIN_COOKIE}={sealed}; Max-Age={LOGIN_STATE_SECONDS}; {SESSION_ATTRIBUTES}")
}

/// Read and open the login cookie.
///
/// # Errors
///
/// [`SessionError`].
pub fn login_state_from_headers(
    keys: &CookieKeys,
    headers: &http::HeaderMap,
    now: DateTime<Utc>,
) -> Result<LoginState, SessionError> {
    let value = cookie(headers, LOGIN_COOKIE).ok_or(SessionError::Absent)?;
    let plaintext = keys
        .open(LOGIN_COOKIE, &value)
        .map_err(|_| SessionError::NotAuthentic)?;
    let state: LoginState =
        serde_json::from_slice(&plaintext).map_err(|_| SessionError::NotAuthentic)?;
    if state.is_expired(now) {
        return Err(SessionError::Expired);
    }
    Ok(state)
}

/// The value of one cookie, by exact name.
///
/// A REPEATED NAME IS NO COOKIE AT ALL. A browser sends at most one cookie per
/// name for a `__Host-` prefixed cookie, so two of them means something injected
/// one; picking either would be picking whichever an attacker arranged to be
/// first. Refusing is the only safe answer, and it costs an honest browser
/// nothing.
#[must_use]
pub fn cookie(headers: &http::HeaderMap, name: &str) -> Option<String> {
    let mut found: Option<String> = None;
    for header in headers.get_all(http::header::COOKIE) {
        let Ok(text) = header.to_str() else {
            return None;
        };
        for pair in text.split(';') {
            let pair = pair.trim();
            let Some((key, value)) = pair.split_once('=') else {
                continue;
            };
            if key.trim() != name {
                continue;
            }
            if found.is_some() {
                return None;
            }
            found = Some(value.trim().to_string());
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use base64::Engine as _;
    use ring::aead::CHACHA20_POLY1305;

    use super::super::keys::{OpenError, VersionedKey, B64, SEAL_PREFIX};
    use super::*;

    fn keys(version: u32) -> CookieKeys {
        CookieKeys::new(&VersionedKey::from_parts(version, vec![0x33; 32]))
    }

    fn identity() -> super::super::oidc::Identity {
        super::super::oidc::Identity {
            issuer: "https://idp.example".into(),
            subject: "u-1".into(),
            display_name: "Ada".into(),
            groups: vec!["team-a-operators".into()],
            auth_time: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
        }
    }

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(seconds, 0).unwrap()
    }

    fn with_cookie(value: &str) -> http::HeaderMap {
        let mut headers = http::HeaderMap::new();
        headers.insert(http::header::COOKIE, value.parse().unwrap());
        headers
    }

    #[test]
    fn the_cookie_carries_the_required_attributes_and_no_domain() {
        let k = keys(1);
        let claims = SessionClaims::issue(&identity(), "sid".into(), 1, at(1000), 900);
        let cookie = set_cookie(&k, &claims, at(1000));
        assert!(cookie.starts_with("__Host-logweir_session="));
        for attribute in [
            "Path=/",
            "Secure",
            "HttpOnly",
            "SameSite=Lax",
            "Max-Age=900",
        ] {
            assert!(cookie.contains(attribute), "{attribute} missing: {cookie}");
        }
        assert!(
            !cookie.to_ascii_lowercase().contains("domain="),
            "a __Host- cookie must carry no Domain"
        );
        let cleared = clear_cookie(SESSION_COOKIE);
        assert!(cleared.contains("Max-Age=0") && cleared.contains("Secure"));
    }

    #[test]
    fn a_session_round_trips_and_expires_on_its_own_clock() {
        let k = keys(1);
        let claims = SessionClaims::issue(&identity(), "sid".into(), 1, at(1000), 900);
        let cookie = set_cookie(&k, &claims, at(1000));
        let value = cookie.split(';').next().unwrap().to_string();

        let opened = from_headers(&k, &with_cookie(&value), at(1500)).unwrap();
        assert_eq!(opened, claims);
        assert_eq!(opened.groups, vec!["team-a-operators".to_string()]);

        assert_eq!(
            from_headers(&k, &with_cookie(&value), at(1900)),
            Err(SessionError::Expired),
            "exactly at exp the session is over"
        );
        assert_eq!(
            from_headers(&k, &http::HeaderMap::new(), at(1500)),
            Err(SessionError::Absent)
        );
        assert_eq!(
            from_headers(&keys(2), &with_cookie(&value), at(1500)),
            Err(SessionError::NotAuthentic),
            "a rotated session key ends every live session"
        );
    }

    #[test]
    fn the_maximum_session_age_cannot_be_configured_past_fifteen_minutes() {
        let claims = SessionClaims::issue(&identity(), "sid".into(), 1, at(0), 86_400);
        assert_eq!(claims.exp, MAX_SESSION_SECONDS);
        let short = SessionClaims::issue(&identity(), "sid".into(), 1, at(0), 120);
        assert_eq!(short.exp, 120);
    }

    /// Every field a session cookie's payload may carry, and no other.
    ///
    /// This is the contract stated in two places — this module's header and
    /// `docs/api.md` §Shared mode, "The session and the CSRF token" — written
    /// down once here so that a field ADDED to [`SessionClaims`] fails this
    /// guard until somebody decides it belongs in a browser's cookie. An
    /// undeclared field is provider material until proven otherwise.
    const CARRIED_FIELDS: [&str; 9] = [
        "sid", "iss", "sub", "name", "groups", "iat", "exp", "auth", "kv",
    ];

    /// The names provider-issued material travels under, most specific first
    /// so that the report names the narrowest one that matched.
    const PROVIDER_NAMES: [&str; 8] = [
        "id_token",
        "access_token",
        "refresh_token",
        "client_secret",
        "token",
        "secret",
        "bearer",
        "assertion",
    ];

    /// The cookie value out of a `Set-Cookie` line.
    fn sealed_value(set_cookie: &str) -> String {
        set_cookie
            .split(';')
            .next()
            .and_then(|pair| pair.split_once('='))
            .map(|(_, value)| value.to_string())
            .expect("a Set-Cookie line is `<name>=<value>; <attributes>`")
    }

    /// The ciphertext-and-tag out of a sealed envelope, decoded.
    fn envelope_ciphertext(sealed: &str) -> Vec<u8> {
        let parts: Vec<&str> = sealed.split('.').collect();
        assert_eq!(
            parts.len(),
            4,
            "a sealed value is `lw1.<version>.<nonce>.<ciphertext>`: {sealed}"
        );
        assert_eq!(parts[0], SEAL_PREFIX, "the envelope prefix");
        B64.decode(parts[3])
            .expect("the ciphertext is URL-safe base64 without padding")
    }

    /// A JWT-shaped string, for the mutant below.
    ///
    /// ASSEMBLED AT RUN TIME ON PURPOSE: no source line may carry a contiguous
    /// token-shaped literal, because push protection scans every pushed commit
    /// for one and a test fixture that only LOOKS like a secret still trips it.
    fn jwt_shaped() -> String {
        let header = B64.encode(br#"{"alg":"RS256","kid":"k-1"}"#);
        let body = B64.encode(br#"{"iss":"https://idp.example","sub":"u-1"}"#);
        let signature = B64.encode([0x5au8; 48]);
        format!("{header}.{body}.{signature}")
    }

    /// Whether a string is a JWT: three non-empty base64url segments whose
    /// first decodes to a JSON object naming an algorithm.
    fn is_jwt_shaped(text: &str) -> bool {
        let parts: Vec<&str> = text.split('.').collect();
        if parts.len() != 3 || parts.iter().any(|part| part.is_empty()) {
            return false;
        }
        let Ok(header) = B64.decode(parts[0]) else {
            return false;
        };
        let Ok(header) = serde_json::from_slice::<serde_json::Value>(&header) else {
            return false;
        };
        header.get("alg").is_some()
    }

    /// Every string in a JSON document, with the path it sits at.
    fn strings(value: &serde_json::Value, path: &str, out: &mut Vec<(String, String)>) {
        match value {
            serde_json::Value::String(text) => out.push((path.to_string(), text.clone())),
            serde_json::Value::Array(items) => {
                for (index, item) in items.iter().enumerate() {
                    strings(item, &format!("{path}[{index}]"), out);
                }
            }
            serde_json::Value::Object(map) => {
                for (key, item) in map {
                    let child = if path.is_empty() {
                        key.clone()
                    } else {
                        format!("{path}.{key}")
                    };
                    strings(item, &child, out);
                }
            }
            serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {
            }
        }
    }

    /// Read a DECODED session payload and name any provider material in it.
    ///
    /// THIS IS THE GUARD, AND IT IS STRUCTURAL ON PURPOSE. Its predecessor
    /// searched the sealed cookie VALUE for the substring `u-1`; since `seal`
    /// draws a fresh random nonce and encodes the result in base64url — an
    /// alphabet that contains `u`, `-` and `1` — that three-character sequence
    /// turned up by chance in roughly one run in eight hundred, and the guard
    /// failed for a reason that had nothing to do with the property it names
    /// (FLAKE-APICOOKIE: 2026-09-17, 2026-09-19). Worse, it could never have
    /// caught the thing it was written for: a token sealed INTO the cookie is
    /// indistinguishable ciphertext on the wire, so no substring search over
    /// the wire form would find it. Opening the envelope and reading the
    /// payload asks the real question instead — does what the browser carries
    /// contain anything the provider issued?
    fn provider_material(payload: &[u8]) -> Result<(), String> {
        let value: serde_json::Value = serde_json::from_slice(payload)
            .map_err(|error| format!("the payload is not JSON: {error}"))?;
        let object = value
            .as_object()
            .ok_or_else(|| format!("the payload is not a JSON object: {value}"))?;

        let carried: std::collections::BTreeSet<&str> = object.keys().map(String::as_str).collect();
        let allowed: std::collections::BTreeSet<&str> = CARRIED_FIELDS.into_iter().collect();
        if carried != allowed {
            let extra: Vec<&str> = carried.difference(&allowed).copied().collect();
            let missing: Vec<&str> = allowed.difference(&carried).copied().collect();
            return Err(format!(
                "the payload's fields are not the contract's: extra {extra:?}, missing {missing:?}\
                 ; the cookie carries exactly [{}] (docs/api.md §Shared mode) and a field nobody \
                 declared is provider material until somebody says otherwise",
                CARRIED_FIELDS.join(", ")
            ));
        }

        let mut found = Vec::new();
        strings(&value, "", &mut found);
        for (path, text) in &found {
            let haystack = format!("{path} {text}").to_ascii_lowercase();
            if let Some(name) = PROVIDER_NAMES.iter().find(|name| haystack.contains(**name)) {
                return Err(format!("`{name}` appears at `{path}`: {text}"));
            }
            if is_jwt_shaped(text) {
                return Err(format!("a JWT sits at `{path}`: {text}"));
            }
        }
        Ok(())
    }

    #[test]
    fn the_sealed_cookie_contains_no_provider_material() {
        let k = keys(1);
        let other = CookieKeys::new(&VersionedKey::from_parts(1, vec![0x44; 32]));
        let claims = SessionClaims::issue(&identity(), "sid".into(), 1, at(1000), 900);
        let plaintext = serde_json::to_vec(&claims).unwrap();

        // A FRESH NONCE PER SEAL is what made the old form of this guard fail
        // by chance, so the structural form is asked of many of them: 128
        // independent seals per run, where the old assertion would have gone
        // red about one run in eight hundred.
        for round in 0..128 {
            let cookie = set_cookie(&k, &claims, at(1000));
            let sealed = sealed_value(&cookie);

            // What the browser carries, opened with this test's own key.
            let payload = k
                .open(SESSION_COOKIE, &sealed)
                .unwrap_or_else(|error| panic!("round {round}: the key opens it: {error:?}"));
            assert_eq!(payload, plaintext, "round {round}: the payload round-trips");
            if let Err(offence) = provider_material(&payload) {
                panic!("round {round}: {offence}");
            }

            // And the wire form is opaque: it carries the ciphertext, not the
            // claims, and no other key reads it.
            let ciphertext = envelope_ciphertext(&sealed);
            assert_eq!(
                ciphertext.len(),
                plaintext.len() + CHACHA20_POLY1305.tag_len(),
                "round {round}: the envelope is the payload plus an authentication tag"
            );
            assert_ne!(
                ciphertext[..plaintext.len()],
                plaintext[..],
                "round {round}: the payload is encrypted, not merely encoded"
            );
            assert_eq!(
                other.open(SESSION_COOKIE, &sealed),
                Err(OpenError::NotAuthentic),
                "round {round}: another key does not read the session"
            );
        }
    }

    /// The mutant for the guard above: material the contract forbids, sealed
    /// through the same path, and the same check must catch every shape of it.
    ///
    /// A GUARD WITHOUT A MUTANT IS NOT A GUARD — and this one replaced an
    /// assertion that no mutant could have killed, since every row below seals
    /// to the same opaque base64url the honest claims do.
    #[test]
    fn the_provider_material_check_catches_a_sealed_token() {
        let k = keys(1);
        let claims = SessionClaims::issue(&identity(), "sid".into(), 1, at(1000), 900);
        let honest = serde_json::to_value(&claims).unwrap();
        let jwt = jwt_shaped();

        // The honest payload is the control: the same check, the same path.
        provider_material(&serde_json::to_vec(&honest).unwrap())
            .expect("the claims this service seals carry no provider material");

        let mut in_its_own_field = honest.clone();
        in_its_own_field["id_token"] = serde_json::Value::String(jwt.clone());
        let mut under_an_innocent_name = honest.clone();
        under_an_innocent_name["name"] = serde_json::Value::String(jwt.clone());
        let mut inside_a_group = honest.clone();
        inside_a_group["groups"] = serde_json::Value::Array(vec![serde_json::Value::String(
            format!("access_token={jwt}"),
        )]);
        let mut under_a_short_name = honest.clone();
        under_a_short_name["at"] = serde_json::Value::String(B64.encode([0x11u8; 24]));

        for (label, tainted, expected) in [
            (
                "an ID token in a field of its own",
                in_its_own_field,
                "id_token",
            ),
            (
                "a token smuggled through the display claim",
                under_an_innocent_name,
                "a JWT sits at `name`",
            ),
            (
                "a token smuggled through a group string",
                inside_a_group,
                "access_token",
            ),
            (
                "an opaque blob under an undeclared field",
                under_a_short_name,
                "extra [\"at\"]",
            ),
        ] {
            // Through the same seal, the same Set-Cookie shape and the same
            // reader the product uses, not around them.
            let sealed = k.seal(SESSION_COOKIE, &serde_json::to_vec(&tainted).unwrap());
            let line = format!("{SESSION_COOKIE}={sealed}; Max-Age=900; {SESSION_ATTRIBUTES}");
            let pair = line.split(';').next().unwrap();
            let value = cookie(&with_cookie(pair), SESSION_COOKIE)
                .unwrap_or_else(|| panic!("{label}: the header carries the cookie"));
            let payload = k
                .open(SESSION_COOKIE, &value)
                .unwrap_or_else(|error| panic!("{label}: it opens: {error:?}"));

            let offence = provider_material(&payload)
                .expect_err(&format!("{label}: the guard has to catch it"));
            assert!(
                offence.contains(expected),
                "{label}: `{expected}` is not named in: {offence}"
            );
        }
    }

    #[test]
    fn a_repeated_or_unreadable_cookie_name_yields_nothing() {
        let k = keys(1);
        let claims = SessionClaims::issue(&identity(), "sid".into(), 1, at(1000), 900);
        let value = set_cookie(&k, &claims, at(1000))
            .split(';')
            .next()
            .unwrap()
            .to_string();
        let sealed = value.split_once('=').unwrap().1.to_string();

        assert!(cookie(&with_cookie(&value), SESSION_COOKIE).is_some());
        // Two of them in one header, and two of them in two headers.
        let doubled = format!("{value}; {SESSION_COOKIE}={sealed}");
        assert_eq!(cookie(&with_cookie(&doubled), SESSION_COOKIE), None);
        let mut two_headers = http::HeaderMap::new();
        two_headers.append(http::header::COOKIE, value.parse().unwrap());
        two_headers.append(
            http::header::COOKIE,
            format!("{SESSION_COOKIE}=other").parse().unwrap(),
        );
        assert_eq!(cookie(&two_headers, SESSION_COOKIE), None);
        // A near-miss name is not the cookie.
        let other = format!("x{value}");
        assert_eq!(cookie(&with_cookie(&other), SESSION_COOKIE), None);
        // Other cookies alongside it are ignored.
        let mixed = format!("theme=dark; {value}; lang=en");
        assert!(cookie(&with_cookie(&mixed), SESSION_COOKIE).is_some());
    }

    #[test]
    fn a_login_state_is_sealed_under_its_own_domain_and_expires() {
        let k = keys(1);
        let state = LoginState {
            state: "s".into(),
            nonce: "n".into(),
            verifier: "v".into(),
            iat: 1000,
            next: "/ui/".into(),
        };
        let value = set_login_cookie(&k, &state)
            .split(';')
            .next()
            .unwrap()
            .to_string();
        assert_eq!(
            login_state_from_headers(&k, &with_cookie(&value), at(1100)).unwrap(),
            state
        );
        assert_eq!(
            login_state_from_headers(&k, &with_cookie(&value), at(1000 + LOGIN_STATE_SECONDS)),
            Err(SessionError::Expired)
        );
        // The login cookie's sealed value is not a session cookie: the AEAD's
        // associated data is the cookie name.
        let sealed = value.split_once('=').unwrap().1;
        let as_session = format!("{SESSION_COOKIE}={sealed}");
        assert_eq!(
            from_headers(&k, &with_cookie(&as_session), at(1100)),
            Err(SessionError::NotAuthentic)
        );
    }
}
