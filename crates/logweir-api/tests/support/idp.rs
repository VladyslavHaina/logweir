//! A complete OpenID Connect provider, in-process, with no socket and no
//! network.
//!
//! WHY IN-PROCESS. `crates/logweir/tests/no_network_in_unit_tests.rs` exists
//! because a test that dials is a test that fails on someone else's Tuesday.
//! This double implements `logweir_api::auth::oidc::HttpClient` — the three
//! requests the provider module makes and no others — so discovery, JWKS and
//! the token exchange are served from memory. There is no listener, no port
//! and nothing to leak.
//!
//! IT SIGNS REAL TOKENS. The ECDSA P-256 key is generated per test run; the
//! RSA key is a throwaway 2048-bit key generated once while writing this file
//! and embedded below, because `ring` can load an RSA key but cannot generate
//! one. Both sign with `ring`, and the API verifies with `ring`, so a
//! signature test is a real signature test rather than a string comparison.
//! NOTHING HERE SIGNS A LOGWEIR ARTEFACT: these keys are for JWS ID tokens
//! only, this file names no `SigningKey` and no `sign_detached`, and
//! `scripts/check-one-signer.sh` is unaffected.
//!
//! EVERY FAILURE MODE IS A KNOB, not a fork of the double: an unknown `kid`, a
//! rotated key set, a JWKS outage, a discovery document naming another issuer,
//! a token-endpoint refusal, a wrong PKCE verifier. The negative tests drive
//! the same code path the positive one does.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use logweir_api::auth::oidc::{HttpClient, HttpFuture};
use ring::rand::SystemRandom;
use ring::signature::{self, EcdsaKeyPair, KeyPair, RsaKeyPair};
use serde_json::{json, Value};

/// URL-safe base64 without padding, the JOSE alphabet.
pub const B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// A throwaway RSA-2048 private key in PKCS#8 DER, base64. Generated for this
/// test file only; it protects nothing and is not a Logweir key of any kind.
const TEST_RSA_PKCS8: &str = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQChE/OrZoI+WA+fmJzeMElYx7SGymYFzAyqzWqG5qpkX0gf7bpKxO4rwDEC6p5u2P/CtlmIJJeshxENAQ6ONpxfuMkK3cfBKjtDoEXyb94S1waWshdcrAyU7aTIi6tGJubZxlI4oWaUld4twt9hF+cuPuNaizQE9Rv1z1UhJfw1E1lzs4LGIyk97GERFDj/vOPeDS/kJ2rC+hFM1TE/YzpogO5WgLYb2xEm3ffl1zCzr+4c2MUjVQKwgkj6tDBDj2PqFVjIG3mU0ax1/xTqRNDdanU6VnRU+Tk/iaWojHkjgDjcSNynnHl2PZ6Fyz8z9bvvF7rC8M+DCDLb0QC7aM0fAgMBAAECggEALH4c2/bcOglMA3r9tZfj4qyDDopgpPBIfXNxHeMgJMp22y7outdrrFURlKsm6Rpyhx+kWmk1JhhG2u80TI8EIaKikahSEWavaQ4f1AgXcN/JN53ouxXhAdAkqKp/vEhpkrTnqDHY6mj9Lmm6FxEpr8n6Ndvmmgn0V7EV5CqgYC5QC2BUTIgfLW+PVQZ7CoVQyZbxLiejSbQn5DhKFm46KiBg19/3SuEpkaI+VNqLKF4jPAXH4mnzKCF+bBUyP70MiN5bzz4h/HsgEml7ZeSgzZVQ/hkY4MXyxnzLf90stEIe3QFBjXoswsBT2gN1c8mrhMHHAyBIfh0zMscK1oXAmQKBgQDbouesenCZ0lgYZl3T/kpUoK5KoKMo7vOGcbSRQznLkgegYYFZE89jILGRCMxgmJYxbxrXBWvGpEeS2H0mv4R9cb8qqe8ycLpS0naMhoj1iuxn2NKW1XU7B1/mJ5gReMLJMZiyPE/TXzvUEYctyPNjQaklmgKy7IyNxWtjO3e0hwKBgQC7vxmFoiky8mcciyafQVlIsfrW2htltTuZxvdFMTW/wGXnDSlKiUmFqqar35/3FrSSrsKI2j0EGiK0hwlZBXTBCcV1Z5rBsseBYi0+ZupS1KhtQXgALOUskAgQD0Rlf0VZrLVuwzx+4EGVIpms3fjqp/gV6g8d4EzGXsq72NBgqQKBgEQpq4KgwR9L42E3K7ll+sWG1HB+qARFHDjGQwat+VrPKCTC/fSaLEuUUucy9tKnqD0RQSAoI4mTZE8Tdsu2tjSEP5LLCFv8Ficr//SesBScF8AmzzxWZLp8EGwKL6yEcNcl2EDAbPmpXZT0F6LC8Z4FO6xavqmutfQtp6U1SHIzAoGBAI3HY2uiKQCbQ7ivcIwlWlpmZWnorXXiJc8cDNFItzFGBu4z5zGteUMiutjieDetAtIefTPBswAtCHZR34JFd4Trbx0ZDyolaznOvSH5sAy7ITHYldl0DeDYJ+6QyPLo6KMupJivgTjC+2O3DFwaCIaUL+nEpoPGRdQr82dl9P55AoGAPL0S0YBh4oPnMAw5FOT0D8dbpz5HPRqvecuEiyjI82hOWxGSziYldw0VwJNRCGlyVFXWdmdsi70drtHzy/UESC3r/E0TJF2+0q4Q67B1Pk0ZhZziqamkJU3SNri+0PkAUVDyDKeZk5SMjaDC6+TQ/bhSzMyUEK+Bdqw7R/YVWRQ=";

/// The RSA modulus of [`TEST_RSA_PKCS8`], base64url.
const TEST_RSA_N: &str = "oRPzq2aCPlgPn5ic3jBJWMe0hspmBcwMqs1qhuaqZF9IH-26SsTuK8AxAuqebtj_wrZZiCSXrIcRDQEOjjacX7jJCt3HwSo7Q6BF8m_eEtcGlrIXXKwMlO2kyIurRibm2cZSOKFmlJXeLcLfYRfnLj7jWos0BPUb9c9VISX8NRNZc7OCxiMpPexhERQ4_7zj3g0v5CdqwvoRTNUxP2M6aIDuVoC2G9sRJt335dcws6_uHNjFI1UCsIJI-rQwQ49j6hVYyBt5lNGsdf8U6kTQ3Wp1OlZ0VPk5P4mlqIx5I4A43Ejcp5x5dj2ehcs_M_W77xe6wvDPgwgy29EAu2jNHw";
/// Its public exponent, base64url.
const TEST_RSA_E: &str = "AQAB";

/// The two algorithms the API verifies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Alg {
    /// RSASSA-PKCS1-v1_5 with SHA-256.
    Rs256,
    /// ECDSA P-256 with SHA-256.
    Es256,
}

impl Alg {
    /// The JOSE name.
    pub const fn name(self) -> &'static str {
        match self {
            Alg::Rs256 => "RS256",
            Alg::Es256 => "ES256",
        }
    }
}

enum Material {
    Rsa(Box<RsaKeyPair>),
    Ec(Box<EcdsaKeyPair>),
}

/// One signing key the provider may publish.
pub struct TestKey {
    /// Its `kid`.
    pub kid: String,
    /// Its algorithm.
    pub alg: Alg,
    material: Material,
    public_jwk: Value,
}

impl TestKey {
    /// A fresh ECDSA P-256 key.
    pub fn ec(kid: &str) -> Self {
        let rng = SystemRandom::new();
        let pkcs8 = EcdsaKeyPair::generate_pkcs8(&signature::ECDSA_P256_SHA256_FIXED_SIGNING, &rng)
            .expect("ring generates a P-256 key");
        let pair = EcdsaKeyPair::from_pkcs8(
            &signature::ECDSA_P256_SHA256_FIXED_SIGNING,
            pkcs8.as_ref(),
            &rng,
        )
        .expect("the generated key loads");
        let point = pair.public_key().as_ref().to_vec();
        assert_eq!(point.len(), 65, "an uncompressed P-256 point is 65 bytes");
        Self {
            kid: kid.to_string(),
            alg: Alg::Es256,
            public_jwk: json!({
                "kty": "EC",
                "crv": "P-256",
                "use": "sig",
                "alg": "ES256",
                "kid": kid,
                "x": B64.encode(&point[1..33]),
                "y": B64.encode(&point[33..65]),
            }),
            material: Material::Ec(Box::new(pair)),
        }
    }

    /// The embedded RSA key, under a chosen `kid`.
    pub fn rsa(kid: &str) -> Self {
        let der = base64::engine::general_purpose::STANDARD
            .decode(TEST_RSA_PKCS8)
            .expect("the embedded test key is base64");
        let pair = RsaKeyPair::from_pkcs8(&der).expect("the embedded test key is PKCS#8");
        Self {
            kid: kid.to_string(),
            alg: Alg::Rs256,
            material: Material::Rsa(Box::new(pair)),
            public_jwk: json!({
                "kty": "RSA",
                "use": "sig",
                "alg": "RS256",
                "kid": kid,
                "n": TEST_RSA_N,
                "e": TEST_RSA_E,
            }),
        }
    }

    /// The public JWK this key publishes.
    pub fn jwk(&self) -> Value {
        self.public_jwk.clone()
    }

    fn sign(&self, message: &[u8]) -> Vec<u8> {
        let rng = SystemRandom::new();
        match &self.material {
            Material::Ec(pair) => pair
                .sign(&rng, message)
                .expect("ECDSA signing does not fail")
                .as_ref()
                .to_vec(),
            Material::Rsa(pair) => {
                let mut signature = vec![0u8; pair.public().modulus_len()];
                pair.sign(&signature::RSA_PKCS1_SHA256, &rng, message, &mut signature)
                    .expect("RSA signing does not fail");
                signature
            }
        }
    }

    /// A JWS over `claims`, with `kid` and `alg` in the header.
    pub fn mint(&self, claims: &Value) -> String {
        self.mint_as(self.kid.clone(), self.alg.name().to_string(), claims)
    }

    /// A JWS whose header says whatever the caller wants — for the tests that
    /// need a token claiming another key or another algorithm.
    pub fn mint_as(&self, kid: String, alg: String, claims: &Value) -> String {
        let header = json!({ "alg": alg, "typ": "JWT", "kid": kid });
        let signing_input = format!(
            "{}.{}",
            B64.encode(serde_json::to_vec(&header).unwrap()),
            B64.encode(serde_json::to_vec(claims).unwrap())
        );
        let signature = self.sign(signing_input.as_bytes());
        format!("{signing_input}.{}", B64.encode(signature))
    }
}

/// What the token endpoint returns for one authorization code.
#[derive(Clone)]
pub struct Grant {
    /// The ID token to hand back.
    pub id_token: String,
    /// The PKCE challenge the verifier must hash to, when the test wants that
    /// checked.
    pub code_challenge: Option<String>,
    /// The redirect URI the exchange must repeat, when the test wants that
    /// checked.
    pub redirect_uri: Option<String>,
}

#[derive(Default)]
struct IdpState {
    published: Vec<Value>,
    grants: BTreeMap<String, Grant>,
    jwks_down: bool,
    discovery_down: bool,
    issuer_override: Option<String>,
    endpoint_overrides: BTreeMap<String, String>,
    jwks_fetches: usize,
    discovery_fetches: usize,
    token_calls: usize,
    last_token_form: String,
}

struct Inner {
    issuer: String,
    state: Mutex<IdpState>,
}

/// The provider double.
#[derive(Clone)]
pub struct MockIdp {
    inner: Arc<Inner>,
}

impl MockIdp {
    /// A provider at `issuer`, publishing `keys`.
    pub fn new(issuer: &str, keys: &[&TestKey]) -> Self {
        let idp = Self {
            inner: Arc::new(Inner {
                issuer: issuer.to_string(),
                state: Mutex::new(IdpState::default()),
            }),
        };
        idp.publish(keys);
        idp
    }

    fn with<R>(&self, f: impl FnOnce(&mut IdpState) -> R) -> R {
        f(&mut self
            .inner
            .state
            .lock()
            .expect("the idp lock is never poisoned"))
    }

    /// Replace the published JWKS. Rotation is this, called twice.
    pub fn publish(&self, keys: &[&TestKey]) {
        let jwks = keys.iter().map(|k| k.jwk()).collect();
        self.with(|s| s.published = jwks);
    }

    /// Register a code the token endpoint will exchange.
    pub fn grant(&self, code: &str, grant: Grant) {
        self.with(|s| {
            s.grants.insert(code.to_string(), grant);
        });
    }

    /// Make the JWKS endpoint fail, as in a provider outage.
    pub fn set_jwks_down(&self, down: bool) {
        self.with(|s| s.jwks_down = down);
    }

    /// Make discovery fail.
    pub fn set_discovery_down(&self, down: bool) {
        self.with(|s| s.discovery_down = down);
    }

    /// Serve a discovery document naming a different issuer.
    pub fn set_issuer_override(&self, issuer: Option<&str>) {
        self.with(|s| s.issuer_override = issuer.map(str::to_string));
    }

    /// Serve a discovery document whose named endpoint is something else — a
    /// compromised or misconfigured provider.
    pub fn set_endpoint_override(&self, endpoint: &str, value: Option<&str>) {
        self.with(|s| match value {
            Some(value) => {
                s.endpoint_overrides
                    .insert(endpoint.to_string(), value.to_string());
            }
            None => {
                s.endpoint_overrides.remove(endpoint);
            }
        });
    }

    /// How many times JWKS has been fetched.
    pub fn jwks_fetches(&self) -> usize {
        self.with(|s| s.jwks_fetches)
    }

    /// How many times discovery has been fetched.
    pub fn discovery_fetches(&self) -> usize {
        self.with(|s| s.discovery_fetches)
    }

    /// How many token exchanges were attempted.
    pub fn token_calls(&self) -> usize {
        self.with(|s| s.token_calls)
    }

    /// The last form body the token endpoint received.
    pub fn last_token_form(&self) -> String {
        self.with(|s| s.last_token_form.clone())
    }

    /// The issuer string.
    pub fn issuer(&self) -> &str {
        &self.inner.issuer
    }

    /// The PKCE challenge for a verifier, as the provider would compute it.
    pub fn challenge(verifier: &str) -> String {
        use sha2::Digest as _;
        B64.encode(sha2::Sha256::digest(verifier.as_bytes()))
    }
}

impl HttpClient for MockIdp {
    fn get<'a>(&'a self, url: &'a str) -> HttpFuture<'a> {
        let idp = self.clone();
        let url = url.to_string();
        Box::pin(async move {
            if url == format!("{}/.well-known/openid-configuration", idp.inner.issuer) {
                return idp.with(|s| {
                    s.discovery_fetches += 1;
                    if s.discovery_down {
                        return Err("the provider answered HTTP 503".to_string());
                    }
                    let issuer = s
                        .issuer_override
                        .clone()
                        .unwrap_or_else(|| idp.inner.issuer.clone());
                    let endpoint = |name: &str, default: String| {
                        s.endpoint_overrides.get(name).cloned().unwrap_or(default)
                    };
                    Ok(serde_json::to_vec(&json!({
                        "issuer": issuer,
                        "authorization_endpoint": endpoint(
                            "authorization_endpoint",
                            format!("{}/authorize", idp.inner.issuer)),
                        "token_endpoint": endpoint(
                            "token_endpoint", format!("{}/token", idp.inner.issuer)),
                        "jwks_uri": endpoint(
                            "jwks_uri", format!("{}/jwks", idp.inner.issuer)),
                        "response_types_supported": ["code"],
                    }))
                    .unwrap())
                });
            }
            if url == format!("{}/jwks", idp.inner.issuer) {
                return idp.with(|s| {
                    s.jwks_fetches += 1;
                    if s.jwks_down {
                        return Err("the provider answered HTTP 503".to_string());
                    }
                    Ok(serde_json::to_vec(&json!({ "keys": s.published })).unwrap())
                });
            }
            Err(format!("the mock provider serves no {url}"))
        })
    }

    fn post_form<'a>(
        &'a self,
        url: &'a str,
        body: &'a str,
        _authorization: Option<&'a str>,
    ) -> HttpFuture<'a> {
        let idp = self.clone();
        let url = url.to_string();
        let body = body.to_string();
        Box::pin(async move {
            if url != format!("{}/token", idp.inner.issuer) {
                return Err(format!("the mock provider serves no {url}"));
            }
            let form: BTreeMap<String, String> =
                serde_urlencoded::from_str(&body).unwrap_or_default();
            idp.with(|s| {
                s.token_calls += 1;
                s.last_token_form = body.clone();
                let code = form.get("code").cloned().unwrap_or_default();
                let Some(grant) = s.grants.get(&code).cloned() else {
                    return Err("the provider answered HTTP 400".to_string());
                };
                if let Some(expected) = &grant.code_challenge {
                    let verifier = form.get("code_verifier").cloned().unwrap_or_default();
                    if &MockIdp::challenge(&verifier) != expected {
                        return Err("the provider answered HTTP 400".to_string());
                    }
                }
                if let Some(expected) = &grant.redirect_uri {
                    if form.get("redirect_uri") != Some(expected) {
                        return Err("the provider answered HTTP 400".to_string());
                    }
                }
                Ok(serde_json::to_vec(&json!({
                    "id_token": grant.id_token,
                    "token_type": "Bearer",
                    "access_token": "an-access-token-the-api-must-never-keep",
                    "refresh_token": "a-refresh-token-the-api-must-never-keep",
                    "expires_in": 300,
                }))
                .unwrap())
            })
        })
    }
}
