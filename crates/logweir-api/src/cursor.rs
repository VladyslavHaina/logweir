//! Opaque, authenticated list cursors.
//!
//! A cursor is `base64url(payload) "." base64url(HMAC-SHA256(key, payload))`.
//! The payload binds the actor (as a SHA-256 of the actor ID, so no identity
//! appears in a URL), the route, the namespace, the canonical filters, an
//! expiry fifteen minutes after issue, and the Kubernetes continue token.
//!
//! THE ORDER OF CHECKS IS THE CONTRACT. The MAC is verified first, in constant
//! time; a cursor that fails it is `cursor_invalid` whatever else it says, so
//! an attacker cannot learn anything by editing the expiry. Only an
//! authentic cursor is then compared with the request's scope (a mismatch is
//! `cursor_invalid`: another actor, route, namespace or filter set) and only
//! then with the clock (`cursor_expired`).

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// How long a cursor stays valid.
pub const CURSOR_TTL_SECONDS: i64 = 15 * 60;

/// The longest cursor accepted; anything longer is invalid before decoding.
pub const MAX_CURSOR_LEN: usize = 4096;

const VERSION: u8 = 1;

/// The MAC key. At least 32 bytes (enforced by `crate::config`).
#[derive(Clone)]
pub struct CursorKey(Vec<u8>);

impl CursorKey {
    /// A key from raw bytes.
    #[must_use]
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    fn mac(&self) -> Hmac<Sha256> {
        // `new_from_slice` accepts any key length for HMAC.
        <Hmac<Sha256> as Mac>::new_from_slice(&self.0).expect("HMAC accepts keys of any length")
    }
}

impl std::fmt::Debug for CursorKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CursorKey([redacted])")
    }
}

/// What a cursor is bound to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CursorScope {
    /// The actor ID.
    pub actor_id: String,
    /// The route identifier, e.g. `GET /api/v1/namespaces/{ns}/backups`.
    pub route: String,
    /// The namespace.
    pub namespace: String,
    /// The canonical filter string (empty for none).
    pub filters: String,
}

#[derive(Serialize, Deserialize)]
struct Payload {
    v: u8,
    a: String,
    r: String,
    n: String,
    f: String,
    e: i64,
    c: String,
}

/// Why a cursor was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorError {
    /// Malformed, forged, tampered or for another scope.
    Invalid,
    /// Authentic and in scope, but past its expiry.
    Expired,
}

fn actor_digest(actor_id: &str) -> String {
    hex::encode(Sha256::digest(actor_id.as_bytes()))
}

/// Seal a continue token into a cursor.
#[must_use]
pub fn seal(
    key: &CursorKey,
    scope: &CursorScope,
    continue_token: &str,
    now: DateTime<Utc>,
) -> String {
    let payload = Payload {
        v: VERSION,
        a: actor_digest(&scope.actor_id),
        r: scope.route.clone(),
        n: scope.namespace.clone(),
        f: scope.filters.clone(),
        e: now.timestamp() + CURSOR_TTL_SECONDS,
        c: continue_token.to_string(),
    };
    let bytes = serde_json::to_vec(&payload).expect("a cursor payload serialises");
    let mut mac = key.mac();
    mac.update(&bytes);
    let tag = mac.finalize().into_bytes();
    format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(&bytes),
        URL_SAFE_NO_PAD.encode(tag)
    )
}

/// Open a cursor for a scope, returning the Kubernetes continue token.
///
/// # Errors
///
/// [`CursorError::Invalid`] or [`CursorError::Expired`], in the order the
/// module documentation states.
pub fn open(
    key: &CursorKey,
    scope: &CursorScope,
    cursor: &str,
    now: DateTime<Utc>,
) -> Result<String, CursorError> {
    if cursor.is_empty() || cursor.len() > MAX_CURSOR_LEN {
        return Err(CursorError::Invalid);
    }
    let (payload_b64, tag_b64) = cursor.split_once('.').ok_or(CursorError::Invalid)?;
    let payload = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .map_err(|_| CursorError::Invalid)?;
    let tag = URL_SAFE_NO_PAD
        .decode(tag_b64)
        .map_err(|_| CursorError::Invalid)?;
    let mut mac = key.mac();
    mac.update(&payload);
    mac.verify_slice(&tag).map_err(|_| CursorError::Invalid)?;

    let payload: Payload = serde_json::from_slice(&payload).map_err(|_| CursorError::Invalid)?;
    if payload.v != VERSION
        || payload.a != actor_digest(&scope.actor_id)
        || payload.r != scope.route
        || payload.n != scope.namespace
        || payload.f != scope.filters
        || payload.c.is_empty()
    {
        return Err(CursorError::Invalid);
    }
    if now.timestamp() >= payload.e {
        return Err(CursorError::Expired);
    }
    Ok(payload.c)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> CursorScope {
        CursorScope {
            actor_id: "urn:logweir:local-admin#admin".into(),
            route: "GET /api/v1/namespaces/{ns}/backups".into(),
            namespace: "team-a".into(),
            filters: String::new(),
        }
    }

    fn t(s: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + s, 0).unwrap()
    }

    #[test]
    fn a_cursor_round_trips_within_its_scope_and_lifetime() {
        let key = CursorKey::new(vec![7; 32]);
        let c = seal(&key, &scope(), "k8s-continue", t(0));
        assert_eq!(open(&key, &scope(), &c, t(899)).unwrap(), "k8s-continue");
        assert_eq!(open(&key, &scope(), &c, t(900)), Err(CursorError::Expired));
    }

    #[test]
    fn tamper_scope_and_key_changes_are_invalid() {
        let key = CursorKey::new(vec![7; 32]);
        let c = seal(&key, &scope(), "tok", t(0));
        let mut other = scope();
        other.namespace = "team-b".into();
        assert_eq!(open(&key, &other, &c, t(1)), Err(CursorError::Invalid));
        let mut other = scope();
        other.actor_id = "someone-else".into();
        assert_eq!(open(&key, &other, &c, t(1)), Err(CursorError::Invalid));
        let mut other = scope();
        other.filters = "labelSelector=a%3Db".into();
        assert_eq!(open(&key, &other, &c, t(1)), Err(CursorError::Invalid));
        assert_eq!(
            open(&CursorKey::new(vec![8; 32]), &scope(), &c, t(1)),
            Err(CursorError::Invalid)
        );
        // Flip one payload character.
        let mut bytes = c.clone().into_bytes();
        bytes[3] = if bytes[3] == b'A' { b'B' } else { b'A' };
        let flipped = String::from_utf8(bytes).unwrap();
        assert_eq!(
            open(&key, &scope(), &flipped, t(1)),
            Err(CursorError::Invalid)
        );
        // An expired AND tampered cursor is invalid, not expired.
        assert_eq!(
            open(&key, &scope(), &flipped, t(10_000)),
            Err(CursorError::Invalid)
        );
        assert_eq!(
            open(&key, &scope(), "garbage", t(1)),
            Err(CursorError::Invalid)
        );
    }
}
