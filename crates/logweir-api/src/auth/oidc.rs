//! OpenID Connect: discovery, JWKS, the Authorization Code + PKCE S256 flow
//! and ID-token validation.
//!
//! THE BROWSER NEVER RECEIVES A PROVIDER TOKEN. The code flow keeps the ID
//! token on the server; this module hands [`Identity`] to
//! `crate::auth::session`, which mints the service's own cookie. No access or
//! refresh token is requested, kept, logged or returned — [`TokenResponse`]
//! deliberately deserialises only `id_token`, so there is nothing else in
//! memory to leak.
//!
//! EVERYTHING IS EXACT, AND EVERY CHECK IS SEPARATE. Issuer, audience,
//! redirect URI and `nonce` are compared for equality against configured or
//! per-login values; `alg` must be on the administrator's allowlist and the
//! header's algorithm must match the key's; the signature is verified over the
//! exact signing input; `exp` and `iat` are checked with a bounded skew. Each
//! failure has its own [`OidcError`] variant so a test can tell which check
//! refused, and none of them is reachable by the other's mistake.
//!
//! JWKS ROTATION AND OUTAGE. Keys are cached with the time they were fetched.
//! An unknown `kid` triggers at most one refetch per [`JWKS_MIN_REFETCH`] —
//! that is what makes a provider's key rotation work without a restart, and
//! what stops an attacker-chosen `kid` from becoming an unbounded fetch loop.
//! While the provider is unreachable the cached keys keep working until
//! [`JWKS_MAX_AGE`], after which validation fails closed.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use base64::Engine as _;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::keys::B64;

/// How long a discovery document is reused.
pub const DISCOVERY_MAX_AGE: Duration = Duration::from_secs(3600);
/// The shortest interval between two JWKS fetches provoked by an unknown key
/// id. Without it an attacker-chosen `kid` is a request amplifier.
pub const JWKS_MIN_REFETCH: Duration = Duration::from_secs(60);
/// How long cached JWKS keep working while the provider is unreachable.
pub const JWKS_MAX_AGE: Duration = Duration::from_secs(24 * 3600);
/// The clock skew allowed on `exp` and `iat`.
pub const CLOCK_SKEW: chrono::TimeDelta = chrono::TimeDelta::seconds(60);
/// The oldest `iat` an ID token may carry.
pub const MAX_ID_TOKEN_AGE: chrono::TimeDelta = chrono::TimeDelta::seconds(600);
/// The largest document this module will read from the provider, 512 KiB.
pub const MAX_PROVIDER_BODY: usize = 512 * 1024;
/// The deadline on every provider request.
pub const PROVIDER_DEADLINE: Duration = Duration::from_secs(10);

/// The two signature algorithms this service verifies.
///
/// RS256 and ES256 are the two every OpenID provider in practice offers, and
/// both are verified by `ring`, which is already in the dependency graph.
/// `none` is not a value this list can hold: the allowlist is checked against
/// these names, so an `alg: none` token has no matching entry and is refused
/// before a key is looked up.
pub const SUPPORTED_ALGORITHMS: [&str; 2] = ["RS256", "ES256"];

/// A refusal. Each check has its own variant so tests can name the one they
/// provoked.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum OidcError {
    /// The provider could not be reached, or answered a non-2xx status.
    #[error("the identity provider is unreachable: {0}")]
    ProviderUnreachable(String),
    /// The discovery document or JWKS did not parse, or disagreed with the
    /// configured issuer.
    #[error("the identity provider's metadata is not usable: {0}")]
    ProviderMetadata(String),
    /// The token is not three base64url segments carrying JSON.
    #[error("the ID token is malformed")]
    Malformed,
    /// The `alg` is not on the configured allowlist.
    #[error("the ID token algorithm `{0}` is not allowed")]
    AlgorithmNotAllowed(String),
    /// No JWKS key matches the token's `kid`, even after a refetch.
    #[error("no signing key matches the ID token")]
    UnknownKey,
    /// The signature did not verify.
    #[error("the ID token signature is not valid")]
    BadSignature,
    /// `iss` is not the configured issuer, exactly.
    #[error("the ID token issuer is not the configured issuer")]
    WrongIssuer,
    /// `aud` does not contain the configured client ID, or `azp` disagrees.
    #[error("the ID token audience is not this client")]
    WrongAudience,
    /// `exp` is in the past, or `iat` is in the future or too old.
    #[error("the ID token is expired or not yet valid")]
    Expired,
    /// `nonce` is absent or is not the nonce this login issued.
    #[error("the ID token nonce does not match this login")]
    WrongNonce,
    /// `sub` is absent or empty.
    #[error("the ID token carries no subject")]
    NoSubject,
    /// The token endpoint refused the code, or returned no ID token.
    #[error("the authorization code could not be exchanged: {0}")]
    CodeExchange(String),
}

impl OidcError {
    /// A short, stable code for the audit record. It never carries a value
    /// from the token or from the provider.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            OidcError::ProviderUnreachable(_) => "provider_unreachable",
            OidcError::ProviderMetadata(_) => "provider_metadata",
            OidcError::Malformed => "id_token_malformed",
            OidcError::AlgorithmNotAllowed(_) => "algorithm_not_allowed",
            OidcError::UnknownKey => "unknown_key",
            OidcError::BadSignature => "bad_signature",
            OidcError::WrongIssuer => "wrong_issuer",
            OidcError::WrongAudience => "wrong_audience",
            OidcError::Expired => "token_expired",
            OidcError::WrongNonce => "wrong_nonce",
            OidcError::NoSubject => "no_subject",
            OidcError::CodeExchange(_) => "code_exchange_failed",
        }
    }
}

// ======================================================================
// The HTTP the provider needs, behind a trait
// ======================================================================

/// A boxed future, so the trait stays object-safe without a macro crate.
pub type HttpFuture<'a> = Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send + 'a>>;

/// The three provider requests this module makes, and nothing else.
///
/// It is a trait so the tests can serve a provider in-process with no socket
/// and no network at all, and so the one implementation that opens a socket
/// ([`HyperHttpClient`]) is the only place TLS is configured.
pub trait HttpClient: Send + Sync + 'static {
    /// `GET url`, returning at most [`MAX_PROVIDER_BODY`] bytes.
    fn get<'a>(&'a self, url: &'a str) -> HttpFuture<'a>;

    /// `POST url` with an `application/x-www-form-urlencoded` body and an
    /// optional `Authorization` header.
    fn post_form<'a>(
        &'a self,
        url: &'a str,
        body: &'a str,
        authorization: Option<&'a str>,
    ) -> HttpFuture<'a>;
}

// ======================================================================
// Settings
// ======================================================================

/// How the client authenticates at the token endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenAuthMethod {
    /// HTTP Basic, the OpenID Connect default.
    ClientSecretBasic,
    /// The secret in the form body.
    ClientSecretPost,
}

/// The administrator's OIDC settings. Every string is exact.
#[derive(Clone)]
pub struct OidcSettings {
    /// The issuer, exactly as the provider states it. No trailing slash.
    pub issuer: String,
    /// The client ID, which is also the expected audience.
    pub client_id: String,
    /// The client secret, read from a mounted file. Never logged, never in a
    /// response, never in an error.
    pub client_secret: Secret,
    /// The exact redirect URI, derived from `publicBaseUrl`.
    pub redirect_uri: String,
    /// The allowed `alg` values, a subset of [`SUPPORTED_ALGORITHMS`].
    pub allowed_algorithms: Vec<String>,
    /// The scopes requested at the authorization endpoint.
    pub scopes: Vec<String>,
    /// The exact claim name carrying group membership.
    pub groups_claim: String,
    /// The exact claim name carrying a display name.
    pub display_name_claim: String,
    /// How the client authenticates at the token endpoint.
    pub token_auth_method: TokenAuthMethod,
}

/// A string that does not print itself.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// Wrap a secret.
    #[must_use]
    pub fn new(value: String) -> Self {
        Self(value)
    }

    /// The secret, at the one place that sends it to the token endpoint.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret([redacted])")
    }
}

// ======================================================================
// Discovery and JWKS
// ======================================================================

/// The three endpoints this service uses from the discovery document.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Discovery {
    /// The issuer the provider states. Must equal the configured issuer.
    pub issuer: String,
    /// Where the browser is redirected.
    pub authorization_endpoint: String,
    /// Where the code is exchanged.
    pub token_endpoint: String,
    /// Where the signing keys are published.
    pub jwks_uri: String,
}

/// One JWKS key, in the two shapes this service verifies.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Jwk {
    kty: String,
    #[serde(default)]
    kid: Option<String>,
    #[serde(default, rename = "use")]
    use_: Option<String>,
    #[serde(default)]
    alg: Option<String>,
    // RSA
    #[serde(default)]
    n: Option<String>,
    #[serde(default)]
    e: Option<String>,
    // EC
    #[serde(default)]
    crv: Option<String>,
    #[serde(default)]
    x: Option<String>,
    #[serde(default)]
    y: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct JwkSet {
    keys: Vec<Jwk>,
}

/// The identity a validated ID token carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    /// The issuer, exactly as configured.
    pub issuer: String,
    /// The subject.
    pub subject: String,
    /// The display claim, for presentation only. Never an authorization input.
    pub display_name: String,
    /// The exact group strings, sorted and deduplicated.
    pub groups: Vec<String>,
    /// The provider's `auth_time`, or `iat` when it is absent.
    pub auth_time: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct Header {
    alg: String,
    #[serde(default)]
    kid: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    /// The ONLY field read. Access and refresh tokens are not deserialised, so
    /// they never exist in this process.
    id_token: String,
}

// ======================================================================
// The provider
// ======================================================================

struct Cached<T> {
    value: T,
    fetched: Instant,
}

struct CacheState {
    discovery: Option<Cached<Discovery>>,
    jwks: Option<Cached<Vec<Jwk>>>,
    last_jwks_attempt: Option<Instant>,
}

/// The configured provider, its caches and the HTTP it speaks.
pub struct Provider {
    settings: OidcSettings,
    http: Box<dyn HttpClient>,
    cache: Mutex<CacheState>,
}

impl Provider {
    /// A provider over settings and an HTTP client.
    #[must_use]
    pub fn new(settings: OidcSettings, http: Box<dyn HttpClient>) -> Self {
        Self {
            settings,
            http,
            cache: Mutex::new(CacheState {
                discovery: None,
                jwks: None,
                last_jwks_attempt: None,
            }),
        }
    }

    /// The settings, for the routes that build the authorization URL.
    #[must_use]
    pub fn settings(&self) -> &OidcSettings {
        &self.settings
    }

    /// The discovery document, fetched at most once per
    /// [`DISCOVERY_MAX_AGE`].
    ///
    /// The provider's own `issuer` must equal the configured issuer: a
    /// discovery document that names a different issuer is a misconfiguration
    /// or a redirect, and either way its endpoints are not this issuer's.
    ///
    /// # Errors
    ///
    /// [`OidcError::ProviderUnreachable`] or [`OidcError::ProviderMetadata`].
    pub async fn discovery(&self) -> Result<Discovery, OidcError> {
        if let Some(cached) = self
            .cache
            .lock()
            .expect("the cache lock is never poisoned")
            .discovery
            .as_ref()
            .filter(|c| c.fetched.elapsed() < DISCOVERY_MAX_AGE)
        {
            return Ok(cached.value.clone());
        }
        let url = format!("{}/.well-known/openid-configuration", self.settings.issuer);
        let body = self
            .http
            .get(&url)
            .await
            .map_err(OidcError::ProviderUnreachable)?;
        let discovery: Discovery = serde_json::from_slice(&body).map_err(|e| {
            OidcError::ProviderMetadata(format!(
                "the discovery document is not the expected shape: {}",
                crate::validate::bounded(&e.to_string(), 200)
            ))
        })?;
        if discovery.issuer != self.settings.issuer {
            return Err(OidcError::ProviderMetadata(
                "the discovery document names a different issuer than the configured one"
                    .to_string(),
            ));
        }
        self.cache
            .lock()
            .expect("the cache lock is never poisoned")
            .discovery = Some(Cached {
            value: discovery.clone(),
            fetched: Instant::now(),
        });
        Ok(discovery)
    }

    async fn jwks(&self, force: bool) -> Result<Vec<Jwk>, OidcError> {
        {
            let mut state = self.cache.lock().expect("the cache lock is never poisoned");
            let usable = state
                .jwks
                .as_ref()
                .filter(|c| c.fetched.elapsed() < JWKS_MAX_AGE);
            if let Some(cached) = usable {
                let too_soon = state
                    .last_jwks_attempt
                    .is_some_and(|at| at.elapsed() < JWKS_MIN_REFETCH);
                if !force || too_soon {
                    return Ok(cached.value.clone());
                }
            }
            state.last_jwks_attempt = Some(Instant::now());
        }
        let uri = self.discovery().await?.jwks_uri;
        let fetched = self.http.get(&uri).await;
        match fetched {
            Ok(body) => {
                let set: JwkSet = serde_json::from_slice(&body).map_err(|e| {
                    OidcError::ProviderMetadata(format!(
                        "the JWKS document is not the expected shape: {}",
                        crate::validate::bounded(&e.to_string(), 200)
                    ))
                })?;
                self.cache
                    .lock()
                    .expect("the cache lock is never poisoned")
                    .jwks = Some(Cached {
                    value: set.keys.clone(),
                    fetched: Instant::now(),
                });
                Ok(set.keys)
            }
            Err(reason) => {
                // OUTAGE: keep serving the cached keys until JWKS_MAX_AGE.
                let state = self.cache.lock().expect("the cache lock is never poisoned");
                match state
                    .jwks
                    .as_ref()
                    .filter(|c| c.fetched.elapsed() < JWKS_MAX_AGE)
                {
                    Some(cached) => Ok(cached.value.clone()),
                    None => Err(OidcError::ProviderUnreachable(reason)),
                }
            }
        }
    }

    /// Exchange an authorization code for an ID token, then validate it.
    ///
    /// # Errors
    ///
    /// [`OidcError`]; the detail of a token-endpoint refusal is the provider's
    /// `error` code only, never its body.
    pub async fn exchange_and_validate(
        &self,
        code: &str,
        verifier: &str,
        nonce: &str,
        now: DateTime<Utc>,
    ) -> Result<Identity, OidcError> {
        let token_endpoint = self.discovery().await?.token_endpoint;
        let mut form: Vec<(&str, &str)> = vec![
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", &self.settings.redirect_uri),
            ("client_id", &self.settings.client_id),
            ("code_verifier", verifier),
        ];
        let authorization = match self.settings.token_auth_method {
            TokenAuthMethod::ClientSecretBasic => Some(format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(format!(
                    "{}:{}",
                    urlencode(&self.settings.client_id),
                    urlencode(self.settings.client_secret.expose())
                ))
            )),
            TokenAuthMethod::ClientSecretPost => {
                form.push(("client_secret", self.settings.client_secret.expose()));
                None
            }
        };
        let body = serde_urlencoded::to_string(&form).map_err(|_| {
            OidcError::CodeExchange("the token request could not be encoded".to_string())
        })?;
        let response = self
            .http
            .post_form(&token_endpoint, &body, authorization.as_deref())
            .await
            .map_err(|reason| OidcError::CodeExchange(reason_code(&reason)))?;
        let token: TokenResponse = serde_json::from_slice(&response).map_err(|_| {
            OidcError::CodeExchange("the token response carries no id_token".to_string())
        })?;
        self.validate_id_token(&token.id_token, nonce, now).await
    }

    /// Validate an ID token against the configured issuer, audience, allowed
    /// algorithms, JWKS, expiry and this login's `nonce`.
    ///
    /// # Errors
    ///
    /// [`OidcError`], one variant per failed check.
    pub async fn validate_id_token(
        &self,
        token: &str,
        nonce: &str,
        now: DateTime<Utc>,
    ) -> Result<Identity, OidcError> {
        let (header, payload_bytes, signature, signing_input) = split(token)?;

        // 1. The algorithm, before any key is looked up. `none` cannot be on
        //    the allowlist, so it fails here.
        if !self
            .settings
            .allowed_algorithms
            .iter()
            .any(|a| a == &header.alg)
        {
            return Err(OidcError::AlgorithmNotAllowed(crate::validate::bounded(
                &header.alg,
                32,
            )));
        }

        // 2. The key. An unknown `kid` provokes at most one refetch.
        let mut keys = self.jwks(false).await?;
        if select_key(&keys, header.kid.as_deref(), &header.alg).is_none() {
            keys = self.jwks(true).await?;
        }
        let key =
            select_key(&keys, header.kid.as_deref(), &header.alg).ok_or(OidcError::UnknownKey)?;

        // 3. The signature, over the exact `header.payload` bytes.
        verify(&key, &header.alg, signing_input.as_bytes(), &signature)?;

        // 4. The claims.
        let claims: BTreeMap<String, serde_json::Value> =
            serde_json::from_slice(&payload_bytes).map_err(|_| OidcError::Malformed)?;

        if claims.get("iss").and_then(|v| v.as_str()) != Some(self.settings.issuer.as_str()) {
            return Err(OidcError::WrongIssuer);
        }
        if !audience_matches(&claims, &self.settings.client_id) {
            return Err(OidcError::WrongAudience);
        }
        let exp = claim_time(&claims, "exp").ok_or(OidcError::Expired)?;
        if exp + CLOCK_SKEW <= now {
            return Err(OidcError::Expired);
        }
        let iat = claim_time(&claims, "iat").ok_or(OidcError::Expired)?;
        if iat > now + CLOCK_SKEW || iat + MAX_ID_TOKEN_AGE < now {
            return Err(OidcError::Expired);
        }
        let presented_nonce = claims.get("nonce").and_then(|v| v.as_str()).unwrap_or("");
        if !super::keys::constant_time_eq(presented_nonce.as_bytes(), nonce.as_bytes())
            || nonce.is_empty()
        {
            return Err(OidcError::WrongNonce);
        }
        let subject = claims
            .get("sub")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or(OidcError::NoSubject)?;

        let display_name = claims
            .get(&self.settings.display_name_claim)
            .and_then(|v| v.as_str())
            .map(|s| crate::validate::bounded(s, 128))
            .unwrap_or_else(|| crate::validate::bounded(subject, 128));

        let mut groups: Vec<String> = match claims.get(&self.settings.groups_claim) {
            Some(serde_json::Value::Array(items)) => items
                .iter()
                .filter_map(|v| v.as_str())
                .filter(|s| !s.is_empty() && s.len() <= 256)
                .map(str::to_string)
                .collect(),
            Some(serde_json::Value::String(one)) if !one.is_empty() => vec![one.clone()],
            _ => Vec::new(),
        };
        groups.sort();
        groups.dedup();
        groups.truncate(MAX_GROUPS);

        let auth_time = claim_time(&claims, "auth_time").unwrap_or(iat);

        Ok(Identity {
            issuer: self.settings.issuer.clone(),
            subject: crate::validate::bounded(subject, 256),
            display_name,
            groups,
            auth_time,
        })
    }
}

/// The most group strings a session carries. A provider that emits thousands
/// would otherwise put them all in a cookie.
pub const MAX_GROUPS: usize = 64;

/// `application/x-www-form-urlencoded` encoding of one value.
///
/// RFC 6749 §2.3.1 says the client id and secret are form-encoded before being
/// used as HTTP Basic credentials, which matters whenever either contains a
/// character outside the unreserved set.
fn urlencode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char);
            }
            b' ' => out.push('+'),
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

fn reason_code(reason: &str) -> String {
    crate::validate::bounded(reason, 120)
}

fn claim_time(claims: &BTreeMap<String, serde_json::Value>, name: &str) -> Option<DateTime<Utc>> {
    let seconds = claims.get(name)?.as_i64()?;
    DateTime::from_timestamp(seconds, 0)
}

fn audience_matches(claims: &BTreeMap<String, serde_json::Value>, client_id: &str) -> bool {
    match claims.get("aud") {
        Some(serde_json::Value::String(one)) => one == client_id,
        Some(serde_json::Value::Array(all)) => {
            let contains = all.iter().any(|v| v.as_str() == Some(client_id));
            if !contains {
                return false;
            }
            // OpenID Connect Core §3.1.3.7: with more than one audience the
            // `azp` claim must be present and must be this client.
            if all.len() > 1 {
                return claims.get("azp").and_then(|v| v.as_str()) == Some(client_id);
            }
            true
        }
        _ => false,
    }
}

fn split(token: &str) -> Result<(Header, Vec<u8>, Vec<u8>, String), OidcError> {
    let mut parts = token.split('.');
    let (Some(h), Some(p), Some(s), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(OidcError::Malformed);
    };
    let header_bytes = B64.decode(h).map_err(|_| OidcError::Malformed)?;
    let header: Header = serde_json::from_slice(&header_bytes).map_err(|_| OidcError::Malformed)?;
    let payload = B64.decode(p).map_err(|_| OidcError::Malformed)?;
    let signature = B64.decode(s).map_err(|_| OidcError::Malformed)?;
    Ok((header, payload, signature, format!("{h}.{p}")))
}

fn select_key(keys: &[Jwk], kid: Option<&str>, alg: &str) -> Option<Jwk> {
    let wanted_kty = match alg {
        "RS256" => "RSA",
        "ES256" => "EC",
        _ => return None,
    };
    keys.iter()
        .filter(|k| k.kty == wanted_kty)
        .filter(|k| k.use_.as_deref().is_none_or(|u| u == "sig"))
        .filter(|k| k.alg.as_deref().is_none_or(|a| a == alg))
        .find(|k| match kid {
            // A token WITH a `kid` must match one: picking "the only key"
            // instead would let a token name a retired key and be verified by
            // the current one.
            Some(kid) => k.kid.as_deref() == Some(kid),
            None => true,
        })
        .cloned()
}

fn verify(key: &Jwk, alg: &str, message: &[u8], signature: &[u8]) -> Result<(), OidcError> {
    match alg {
        "RS256" => {
            let n = B64
                .decode(key.n.as_deref().ok_or(OidcError::UnknownKey)?)
                .map_err(|_| OidcError::UnknownKey)?;
            let e = B64
                .decode(key.e.as_deref().ok_or(OidcError::UnknownKey)?)
                .map_err(|_| OidcError::UnknownKey)?;
            ring::signature::RsaPublicKeyComponents { n: &n, e: &e }
                .verify(
                    &ring::signature::RSA_PKCS1_2048_8192_SHA256,
                    message,
                    signature,
                )
                .map_err(|_| OidcError::BadSignature)
        }
        "ES256" => {
            if key.crv.as_deref() != Some("P-256") {
                return Err(OidcError::UnknownKey);
            }
            let x = B64
                .decode(key.x.as_deref().ok_or(OidcError::UnknownKey)?)
                .map_err(|_| OidcError::UnknownKey)?;
            let y = B64
                .decode(key.y.as_deref().ok_or(OidcError::UnknownKey)?)
                .map_err(|_| OidcError::UnknownKey)?;
            if x.len() != 32 || y.len() != 32 {
                return Err(OidcError::UnknownKey);
            }
            let mut point = Vec::with_capacity(65);
            point.push(0x04);
            point.extend_from_slice(&x);
            point.extend_from_slice(&y);
            ring::signature::UnparsedPublicKey::new(
                &ring::signature::ECDSA_P256_SHA256_FIXED,
                point,
            )
            .verify(message, signature)
            .map_err(|_| OidcError::BadSignature)
        }
        other => Err(OidcError::AlgorithmNotAllowed(crate::validate::bounded(
            other, 32,
        ))),
    }
}

// ======================================================================
// PKCE and the authorization URL
// ======================================================================

/// The S256 code challenge for a verifier.
#[must_use]
pub fn code_challenge_s256(verifier: &str) -> String {
    B64.encode(Sha256::digest(verifier.as_bytes()))
}

/// The authorization URL for one login attempt.
#[must_use]
pub fn authorization_url(
    discovery: &Discovery,
    settings: &OidcSettings,
    state: &str,
    nonce: &str,
    verifier: &str,
) -> String {
    let scope = settings.scopes.join(" ");
    let challenge = code_challenge_s256(verifier);
    let query = serde_urlencoded::to_string([
        ("response_type", "code"),
        ("client_id", settings.client_id.as_str()),
        ("redirect_uri", settings.redirect_uri.as_str()),
        ("scope", scope.as_str()),
        ("state", state),
        ("nonce", nonce),
        ("code_challenge", challenge.as_str()),
        ("code_challenge_method", "S256"),
    ])
    .expect("a list of string pairs always form-encodes");
    let separator = if discovery.authorization_endpoint.contains('?') {
        '&'
    } else {
        '?'
    };
    format!("{}{separator}{query}", discovery.authorization_endpoint)
}

// ======================================================================
// The one implementation that opens a socket
// ======================================================================

/// The production HTTP client: hyper over rustls, HTTPS by default.
///
/// PLAIN HTTP IS REACHABLE ONLY FOR A LOOPBACK ISSUER, and only when the
/// administrator set `oidc.insecureLoopbackIssuer`. `crate::config` refuses
/// that combination for any non-loopback host, so a production issuer cannot
/// be downgraded by a configuration typo.
pub struct HyperHttpClient {
    http: hyper_util::client::legacy::Client<
        hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
        http_body_util::Full<bytes::Bytes>,
    >,
}

impl HyperHttpClient {
    /// Build the client, installing the rustls provider the workspace's two
    /// enabled providers would otherwise leave ambiguous.
    ///
    /// # Errors
    ///
    /// A sentence naming what could not be initialised.
    pub fn new(allow_plain_http: bool) -> Result<Self, String> {
        // The workspace graph enables both `ring` and `aws-lc-rs`; with two
        // installed-by-feature providers rustls picks none and panics on first
        // use. `weirkeeper::install_default_crypto_provider` is the one the
        // controller binary and `crate::kube` already call, so the API and the
        // controller agree on the provider.
        let _ = weirkeeper::install_default_crypto_provider();
        let builder = hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .map_err(|e| format!("the system certificate store could not be read: {e}"))?
            .https_or_http();
        let connector = if allow_plain_http {
            builder.enable_http1().build()
        } else {
            hyper_rustls::HttpsConnectorBuilder::new()
                .with_native_roots()
                .map_err(|e| format!("the system certificate store could not be read: {e}"))?
                .https_only()
                .enable_http1()
                .build()
        };
        Ok(Self {
            http: hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .build(connector),
        })
    }

    async fn send(
        &self,
        request: http::Request<http_body_util::Full<bytes::Bytes>>,
    ) -> Result<Vec<u8>, String> {
        use http_body_util::BodyExt as _;
        let response = tokio::time::timeout(PROVIDER_DEADLINE, self.http.request(request))
            .await
            .map_err(|_| "the request exceeded the provider deadline".to_string())?
            .map_err(|e| crate::validate::bounded(&e.to_string(), 160))?;
        let status = response.status();
        let body = http_body_util::Limited::new(response.into_body(), MAX_PROVIDER_BODY)
            .collect()
            .await
            .map_err(|_| "the provider response could not be read".to_string())?
            .to_bytes()
            .to_vec();
        if !status.is_success() {
            // The provider's body is NOT propagated: a token endpoint puts the
            // client secret's failure and sometimes the code in it.
            return Err(format!("the provider answered HTTP {}", status.as_u16()));
        }
        Ok(body)
    }
}

/// Build one provider request.
///
/// It assembles the request field by field rather than through a builder
/// because `tests/linkage.rs` forbids the builder spelling crate-wide: that
/// token is how it catches a hand-built request smuggled past the sealed
/// Kubernetes adapter, and this module — the one place that speaks HTTP to
/// something that is not Kubernetes — earns no exemption from it.
fn provider_request(
    method: http::Method,
    url: &str,
    body: bytes::Bytes,
    content_type: Option<&'static str>,
    authorization: Option<&str>,
) -> Result<http::Request<http_body_util::Full<bytes::Bytes>>, String> {
    let uri: http::Uri = url
        .parse()
        .map_err(|_| "the provider URL is not a valid URI".to_string())?;
    let mut request = http::Request::new(http_body_util::Full::new(body));
    *request.method_mut() = method;
    *request.uri_mut() = uri;
    let headers = request.headers_mut();
    headers.insert(
        http::header::ACCEPT,
        http::HeaderValue::from_static("application/json"),
    );
    if let Some(content_type) = content_type {
        headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static(content_type),
        );
    }
    if let Some(authorization) = authorization {
        headers.insert(
            http::header::AUTHORIZATION,
            http::HeaderValue::from_str(authorization)
                .map_err(|_| "the client credentials are not a valid header".to_string())?,
        );
    }
    Ok(request)
}

impl HttpClient for HyperHttpClient {
    fn get<'a>(&'a self, url: &'a str) -> HttpFuture<'a> {
        Box::pin(async move {
            let request =
                provider_request(http::Method::GET, url, bytes::Bytes::new(), None, None)?;
            self.send(request).await
        })
    }

    fn post_form<'a>(
        &'a self,
        url: &'a str,
        body: &'a str,
        authorization: Option<&'a str>,
    ) -> HttpFuture<'a> {
        Box::pin(async move {
            let request = provider_request(
                http::Method::POST,
                url,
                bytes::Bytes::from(body.to_string()),
                Some("application/x-www-form-urlencoded"),
                authorization,
            )?;
            self.send(request).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_pkce_challenge_is_the_rfc_7636_appendix_b_vector() {
        // RFC 7636 Appendix B.
        assert_eq!(
            code_challenge_s256("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }

    #[test]
    fn the_audience_check_requires_azp_when_there_is_more_than_one() {
        let one =
            serde_json::from_str::<BTreeMap<String, serde_json::Value>>(r#"{"aud":"console"}"#)
                .unwrap();
        assert!(audience_matches(&one, "console"));
        assert!(!audience_matches(&one, "other"));

        let array =
            serde_json::from_str::<BTreeMap<String, serde_json::Value>>(r#"{"aud":["console"]}"#)
                .unwrap();
        assert!(audience_matches(&array, "console"));

        let two = serde_json::from_str::<BTreeMap<String, serde_json::Value>>(
            r#"{"aud":["console","other"]}"#,
        )
        .unwrap();
        assert!(!two.contains_key("azp"));
        assert!(
            !audience_matches(&two, "console"),
            "two audiences without azp must be refused"
        );

        let two_azp = serde_json::from_str::<BTreeMap<String, serde_json::Value>>(
            r#"{"aud":["console","other"],"azp":"console"}"#,
        )
        .unwrap();
        assert!(audience_matches(&two_azp, "console"));
        let wrong_azp = serde_json::from_str::<BTreeMap<String, serde_json::Value>>(
            r#"{"aud":["console","other"],"azp":"other"}"#,
        )
        .unwrap();
        assert!(!audience_matches(&wrong_azp, "console"));
    }

    #[test]
    fn a_token_with_a_kid_never_falls_back_to_another_key() {
        let keys = vec![Jwk {
            kty: "RSA".into(),
            kid: Some("current".into()),
            use_: Some("sig".into()),
            alg: Some("RS256".into()),
            n: Some("x".into()),
            e: Some("AQAB".into()),
            crv: None,
            x: None,
            y: None,
        }];
        assert!(select_key(&keys, Some("current"), "RS256").is_some());
        assert!(
            select_key(&keys, Some("retired"), "RS256").is_none(),
            "a named key id must not be satisfied by a different key"
        );
        // No `kid` at all: the single matching key is used, as the JWS spec
        // allows.
        assert!(select_key(&keys, None, "RS256").is_some());
        // Wrong family for the algorithm.
        assert!(select_key(&keys, Some("current"), "ES256").is_none());
    }

    #[test]
    fn a_secret_never_prints_itself() {
        let s = Secret::new("super-secret".into());
        assert_eq!(format!("{s:?}"), "Secret([redacted])");
        assert!(!format!("{s:?}").contains("super"));
        assert_eq!(s.expose(), "super-secret");
    }

    #[test]
    fn malformed_tokens_are_refused_before_any_key_lookup() {
        for bad in ["", "a", "a.b", "a.b.c.d", "!!!.x.y", "e30.x.y"] {
            assert!(split(bad).is_err(), "{bad}");
        }
        let header = B64.encode(br#"{"alg":"none"}"#);
        let payload = B64.encode(b"{}");
        let ok = split(&format!("{header}.{payload}.")).unwrap();
        assert_eq!(ok.0.alg, "none");
        assert!(
            !SUPPORTED_ALGORITHMS.contains(&"none"),
            "`none` must never be a supported algorithm"
        );
    }
}
