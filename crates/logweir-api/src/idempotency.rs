//! Durable-create idempotency, with Kubernetes as the only store.
//!
//! THE NAME IS THE KEY. A durable POST's object name is a fixed per-route
//! prefix plus 26 base32 characters (130 bits) of
//! `SHA-256(scope)`, where the scope is the length-prefixed tuple
//! `(issuer, subject, namespace, route, Idempotency-Key)`. A lost response, a
//! double click or an API restart therefore targets the SAME name, and the API
//! server's `AlreadyExists` is what makes a second object impossible. No
//! process memory is consulted, so a new process — or a second replica — gives
//! the same answer.
//!
//! THE ANNOTATIONS DECIDE REPLAY VERSUS CONFLICT. The created object carries
//! the full scope hash and the canonical request hash. On `AlreadyExists` the
//! existing object is read and compared ([`compare`]):
//!
//! * scope hash equal, request hash equal  → replay: 200, same UID;
//! * scope hash equal, request hash differs → `idempotency_conflict`;
//! * scope hash absent or different         → `state_conflict`. The object
//!   was not created by this scope and is NEVER adopted.
//!
//! THE RAW KEY IS NEVER STORED OR LOGGED. Only hashes derived from it are.

use std::collections::BTreeMap;

use http::HeaderMap;
use sha2::{Digest, Sha256};

use crate::auth::Actor;
use crate::problem::{ApiError, FieldError, ProblemCode};

/// The request header.
pub const HEADER: &str = "idempotency-key";

/// The full SHA-256 of the idempotency scope, `sha256:<hex>`.
pub const ANNOTATION_SCOPE: &str = "api.logweir.dev/idempotency-scope-sha256";
/// The SHA-256 of the canonical validated request, `sha256:<hex>`.
pub const ANNOTATION_REQUEST: &str = "api.logweir.dev/request-sha256";
/// The request ID that created the object (correlation, not proof).
pub const ANNOTATION_REQUEST_ID: &str = "api.logweir.dev/request-id";
/// The non-secret actor ID that created the object (correlation, not proof).
pub const ANNOTATION_ACTOR: &str = "api.logweir.dev/actor";

/// The base32 characters of the scope hash in an object name.
pub const NAME_HASH_CHARS: usize = 26;

/// A validated `Idempotency-Key`: 8–128 visible ASCII characters.
#[derive(Clone, PartialEq, Eq)]
pub struct IdempotencyKey(String);

impl std::fmt::Debug for IdempotencyKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("IdempotencyKey([redacted])")
    }
}

impl IdempotencyKey {
    /// Read and validate the header.
    ///
    /// # Errors
    ///
    /// `idempotency_key_required` when absent; `idempotency_key_invalid` when
    /// repeated, not 8–128 characters, or not visible ASCII.
    pub fn from_headers(headers: &HeaderMap) -> Result<Self, ApiError> {
        let mut values = headers.get_all(HEADER).iter();
        let Some(value) = values.next() else {
            return Err(ApiError::new(
                ProblemCode::IdempotencyKeyRequired,
                "This durable POST requires an Idempotency-Key header.",
            ));
        };
        let invalid = |reason: &str| {
            let mut e = ApiError::new(ProblemCode::IdempotencyKeyInvalid, reason.to_string());
            e.errors.push(FieldError::new(
                "Idempotency-Key",
                "invalid",
                "must be 8 to 128 visible ASCII characters, sent once",
            ));
            e
        };
        if values.next().is_some() {
            return Err(invalid("Idempotency-Key must be sent exactly once."));
        }
        let bytes = value.as_bytes();
        if bytes.len() < 8 || bytes.len() > 128 || !bytes.iter().all(|b| (0x21..=0x7e).contains(b))
        {
            return Err(invalid(
                "Idempotency-Key must be 8 to 128 visible ASCII characters.",
            ));
        }
        Ok(Self(String::from_utf8_lossy(bytes).into_owned()))
    }

    /// Refuse the header on a route that does not accept it.
    ///
    /// `precondition` names the field this route uses instead, or `None` for a
    /// command that has no precondition at all. THE ROUTE'S OWN FIELD, because
    /// a fixed sentence about `expectedResourceVersion` sends a client looking
    /// for a field that does not exist on `:update-access` (which takes
    /// `expectedGeneration`) or on either `:cancel` (which take neither, and
    /// are idempotent instead).
    ///
    /// # Errors
    ///
    /// `idempotency_key_invalid` when the header is present.
    pub fn refuse_on(
        headers: &HeaderMap,
        route: &str,
        precondition: Option<&str>,
    ) -> Result<(), ApiError> {
        if headers.contains_key(HEADER) {
            let instead = match precondition {
                Some(field) => format!("Use {field}."),
                None => {
                    "It is idempotent: repeating it changes nothing and answers 200.".to_string()
                }
            };
            return Err(ApiError::new(
                ProblemCode::IdempotencyKeyInvalid,
                format!(
                    "{route} does not accept Idempotency-Key: it is a command, not a durable \
                     create. {instead}"
                ),
            ));
        }
        Ok(())
    }
}

/// The deterministic identity of one durable create.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateIdentity {
    /// The Kubernetes object name.
    pub name: String,
    /// `sha256:<hex>` of the scope.
    pub scope_hash: String,
    /// `sha256:<hex>` of the canonical request.
    pub request_hash: String,
}

fn push_field(buf: &mut Vec<u8>, value: &str) {
    buf.extend_from_slice(&(value.len() as u64).to_be_bytes());
    buf.extend_from_slice(value.as_bytes());
}

/// Compute the identity of a create.
///
/// `canonical_request` is the JSON serialisation of the VALIDATED request
/// DTO; `route` is the route identifier; `prefix` is the per-route name
/// prefix (it must be a DNS label prefix ending in `-`).
#[must_use]
pub fn identity(
    actor: &Actor,
    namespace: &str,
    route: &str,
    prefix: &str,
    key: &IdempotencyKey,
    canonical_request: &[u8],
) -> CreateIdentity {
    let mut scope = b"logweir-api/idempotency-scope/v1\n".to_vec();
    push_field(&mut scope, &actor.issuer);
    push_field(&mut scope, &actor.subject);
    push_field(&mut scope, namespace);
    push_field(&mut scope, route);
    push_field(&mut scope, &key.0);
    let scope_digest = Sha256::digest(&scope);

    // THE SCOPE DIGEST IS MIXED IN, AND THAT IS WHAT MAKES THE PUBLISHED HASH
    // USELESS AS AN ORACLE. A destination create's canonical request contains
    // an ENTERED CREDENTIAL (`access.<role>.secret.new`), and the resulting
    // hash is written onto the object as `api.logweir.dev/request-sha256` —
    // which anyone who may `get backupdestinations` can read, while every
    // other field of that request is published by `GET /destinations/{name}`.
    // Unsalted, that is an offline confirmation oracle: rebuild the request
    // from the public projection, guess the key pair, hash, compare. The scope
    // digest is derived from the client-chosen `Idempotency-Key`, which is
    // never stored or published in any form, so the hash cannot be recomputed
    // from the object alone.
    //
    // THE REPLAY SEMANTICS ARE UNCHANGED. Both the name and this hash are
    // taken under the SAME scope, so two requests being compared always share
    // it: same key + same request still replays, and same key + different
    // request is still `idempotency_conflict`.
    let mut request = b"logweir-api/request/v2\n".to_vec();
    push_field(&mut request, route);
    request.extend_from_slice(&scope_digest);
    request.extend_from_slice(canonical_request);

    CreateIdentity {
        name: format!(
            "{prefix}{}",
            &base32_lower(&scope_digest)[..NAME_HASH_CHARS]
        ),
        scope_hash: format!("sha256:{}", hex::encode(scope_digest)),
        request_hash: logweir_core::ids::sha256_prefixed(&request),
    }
}

/// The annotations a created object carries.
#[must_use]
pub fn annotations(
    identity: &CreateIdentity,
    request_id: &str,
    actor: &Actor,
) -> BTreeMap<String, String> {
    BTreeMap::from([
        (ANNOTATION_SCOPE.to_string(), identity.scope_hash.clone()),
        (
            ANNOTATION_REQUEST.to_string(),
            identity.request_hash.clone(),
        ),
        (ANNOTATION_REQUEST_ID.to_string(), request_id.to_string()),
        (ANNOTATION_ACTOR.to_string(), actor.id()),
    ])
}

/// What an existing object with the deterministic name means.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplayVerdict {
    /// Same scope, same request: return it with 200.
    Replay,
    /// Same scope, different request: `idempotency_conflict`.
    DifferentRequest,
    /// Not created by this scope: `state_conflict`, never adopted.
    Foreign,
}

/// Compare an existing object's annotations with the identity.
#[must_use]
pub fn compare(
    existing: Option<&BTreeMap<String, String>>,
    identity: &CreateIdentity,
) -> ReplayVerdict {
    let Some(annotations) = existing else {
        return ReplayVerdict::Foreign;
    };
    match annotations.get(ANNOTATION_SCOPE) {
        Some(scope) if scope == &identity.scope_hash => {
            if annotations.get(ANNOTATION_REQUEST) == Some(&identity.request_hash) {
                ReplayVerdict::Replay
            } else {
                ReplayVerdict::DifferentRequest
            }
        }
        _ => ReplayVerdict::Foreign,
    }
}

/// RFC 4648 base32, lowercase, no padding: DNS-label safe.
#[must_use]
pub fn base32_lower(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut out = String::with_capacity(bytes.len().div_ceil(5) * 8);
    let mut buffer: u32 = 0;
    let mut bits = 0u32;
    for &byte in bytes {
        buffer = (buffer << 8) | u32::from(byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(ALPHABET[((buffer >> bits) & 31) as usize] as char);
        }
    }
    if bits > 0 {
        out.push(ALPHABET[((buffer << (5 - bits)) & 31) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actor(subject: &str) -> Actor {
        Actor::new("urn:test", subject, "Test")
    }

    fn key(k: &str) -> IdempotencyKey {
        let mut h = HeaderMap::new();
        h.insert(HEADER, k.parse().unwrap());
        IdempotencyKey::from_headers(&h).unwrap()
    }

    #[test]
    fn base32_matches_rfc4648_vectors() {
        assert_eq!(base32_lower(b"f"), "my");
        assert_eq!(base32_lower(b"fo"), "mzxq");
        assert_eq!(base32_lower(b"foo"), "mzxw6");
        assert_eq!(base32_lower(b"foobar"), "mzxw6ytboi");
    }

    #[test]
    fn the_name_is_a_function_of_the_whole_scope() {
        let body = br#"{"a":1}"#;
        let base = identity(&actor("a"), "ns", "POST x", "sch-", &key("key-00001"), body);
        assert_eq!(base.name.len(), 4 + NAME_HASH_CHARS);
        assert!(crate::validate::is_dns_label(&base.name));
        assert_eq!(
            base,
            identity(&actor("a"), "ns", "POST x", "sch-", &key("key-00001"), body)
        );
        for other in [
            identity(&actor("b"), "ns", "POST x", "sch-", &key("key-00001"), body),
            identity(
                &actor("a"),
                "ns2",
                "POST x",
                "sch-",
                &key("key-00001"),
                body,
            ),
            identity(&actor("a"), "ns", "POST y", "sch-", &key("key-00001"), body),
            identity(&actor("a"), "ns", "POST x", "sch-", &key("key-00002"), body),
        ] {
            assert_ne!(other.name, base.name);
        }
        let changed = identity(
            &actor("a"),
            "ns",
            "POST x",
            "sch-",
            &key("key-00001"),
            br#"{"a":2}"#,
        );
        assert_eq!(changed.name, base.name);
        assert_ne!(changed.request_hash, base.request_hash);
    }

    #[test]
    fn key_validation() {
        let mut h = HeaderMap::new();
        assert_eq!(
            IdempotencyKey::from_headers(&h).unwrap_err().code,
            ProblemCode::IdempotencyKeyRequired
        );
        for bad in ["short", "has space-in-it", &"x".repeat(129)] {
            h.insert(HEADER, bad.parse().unwrap());
            assert_eq!(
                IdempotencyKey::from_headers(&h).unwrap_err().code,
                ProblemCode::IdempotencyKeyInvalid,
                "{bad}"
            );
        }
        h.insert(HEADER, "12345678".parse().unwrap());
        h.append(HEADER, "12345678".parse().unwrap());
        assert_eq!(
            IdempotencyKey::from_headers(&h).unwrap_err().code,
            ProblemCode::IdempotencyKeyInvalid
        );
    }

    /// **The published request hash is not reconstructable from the object.**
    ///
    /// A destination create's canonical request carries an ENTERED CREDENTIAL,
    /// and the hash of it is written onto the object as
    /// `api.logweir.dev/request-sha256` — readable by anyone who may `get
    /// backupdestinations`, while every other field of that request is
    /// published by `GET /destinations/{name}`. Unsalted, that is an offline
    /// confirmation oracle for the credential: rebuild the request from the
    /// public projection, guess the key pair, hash, compare. Mixing the scope
    /// digest in defeats it, because the scope is derived from the
    /// client-chosen `Idempotency-Key`, which is never stored or published in
    /// any form.
    #[test]
    fn the_request_hash_cannot_be_recomputed_from_the_published_body() {
        let body = br#"{"name":"primary","access":{"secretAccessKey":"guessed"}}"#;
        let route = "POST /api/v1/namespaces/{ns}/destinations";
        let published = identity(&actor("a"), "ns", route, "", &key("key-00001"), body);

        // THE ORACLE, ATTEMPTED. Everything an attacker holds — the actor, the
        // namespace, the route and the whole body — reproduces nothing,
        // because the key is the one input the object does not carry.
        let guessed = identity(&actor("a"), "ns", route, "", &key("key-99999"), body);
        assert_ne!(
            guessed.request_hash, published.request_hash,
            "the request hash is a function of the body alone, so the published annotation \
             confirms a guessed credential"
        );

        // AND THE REPLAY SEMANTICS ARE UNCHANGED, because both sides of a
        // comparison always share the scope: the name and the hash are taken
        // under the same one.
        assert_eq!(
            identity(&actor("a"), "ns", route, "", &key("key-00001"), body).request_hash,
            published.request_hash
        );
        let changed = identity(
            &actor("a"),
            "ns",
            route,
            "",
            &key("key-00001"),
            br#"{"name":"primary","access":{"secretAccessKey":"other"}}"#,
        );
        assert_eq!(
            changed.name, published.name,
            "the same key names the same object"
        );
        assert_ne!(
            changed.request_hash, published.request_hash,
            "same key, different request must still be idempotency_conflict"
        );
        assert_eq!(
            compare(Some(&annotations(&published, "req", &actor("a"))), &changed),
            ReplayVerdict::DifferentRequest
        );
    }

    #[test]
    fn replay_verdicts() {
        let id = identity(
            &actor("a"),
            "ns",
            "POST x",
            "sch-",
            &key("key-00001"),
            b"{}",
        );
        let ann = annotations(&id, "req", &actor("a"));
        assert_eq!(compare(Some(&ann), &id), ReplayVerdict::Replay);
        let mut other = ann.clone();
        other.insert(ANNOTATION_REQUEST.into(), "sha256:00".into());
        assert_eq!(compare(Some(&other), &id), ReplayVerdict::DifferentRequest);
        let mut foreign = ann.clone();
        foreign.remove(ANNOTATION_SCOPE);
        assert_eq!(compare(Some(&foreign), &id), ReplayVerdict::Foreign);
        assert_eq!(compare(None, &id), ReplayVerdict::Foreign);
    }
}
