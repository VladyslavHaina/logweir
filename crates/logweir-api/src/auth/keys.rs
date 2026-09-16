//! Versioned key files, and the authenticated encryption every cookie uses.
//!
//! A KEY FILE CARRIES ITS OWN VERSION, AND THE CONFIGURATION DECLARES WHICH
//! VERSION IT EXPECTS. That pair is what makes "unexpectedly rotated" a
//! startup refusal rather than a silent mass logout: an administrator who
//! rewrites the mounted key without bumping `expectedVersion` in the
//! configuration gets exit 2 and a sentence naming the file, and one who
//! rotates deliberately changes both and knows every live session ends. The
//! version also travels in the cookie, so a cookie minted under key 1 is
//! refused by a process holding key 2 with `session_expired` instead of being
//! silently mis-decrypted.
//!
//! THE FILE IS NEVER THE KEY. Two subkeys are derived from the file bytes with
//! HMAC-SHA-256 under distinct labels — one for the AEAD, one for the CSRF
//! synchronizer token — so the same file cannot be used to forge a token in
//! the other domain, and the AEAD always gets exactly 32 bytes whatever the
//! file's length.
//!
//! THE AEAD IS ChaCha20-Poly1305 FROM `ring`, with a fresh 96-bit random nonce
//! per seal and the cookie's own name plus the key version as associated data.
//! A sealed value therefore cannot be replayed into the other cookie, and a
//! tampered byte fails to open rather than decrypting to something.

use std::path::Path;

use base64::Engine as _;
use hmac::{Hmac, Mac};
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, CHACHA20_POLY1305, NONCE_LEN};
use ring::rand::{SecureRandom, SystemRandom};
use serde::Deserialize;
use sha2::Sha256;

/// The smallest key a file may carry, in bytes.
pub const MIN_KEY_BYTES: usize = 32;

/// The prefix every sealed cookie value carries.
pub const SEAL_PREFIX: &str = "lw1";

/// The HMAC label for the AEAD subkey.
const LABEL_AEAD: &[u8] = b"logweir-api/cookie-aead/v1";
/// The HMAC label for the CSRF subkey.
const LABEL_CSRF: &[u8] = b"logweir-api/csrf-token/v1";

/// The URL-safe, unpadded base64 alphabet every cookie and token uses.
pub const B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// A key file as written.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct KeyFile {
    version: u32,
    key: String,
}

/// Why a key file was refused.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum KeyError {
    /// The file could not be read.
    #[error("cannot read the key file {path}: {reason}")]
    Missing {
        /// The path.
        path: String,
        /// The I/O error.
        reason: String,
    },
    /// The file is not the expected shape, or the key is too short.
    #[error("the key file {path} is malformed: {reason}")]
    Malformed {
        /// The path.
        path: String,
        /// Why.
        reason: String,
    },
    /// The file's version is not the version the configuration expects.
    #[error(
        "the key file {path} carries version {found} but the configuration expects {expected}; \
         refusing to start rather than silently invalidating or mis-reading live cookies (bump \
         `expectedVersion` deliberately when you rotate)"
    )]
    UnexpectedVersion {
        /// The path.
        path: String,
        /// The version in the file.
        found: u32,
        /// The version the configuration declared.
        expected: u32,
    },
}

/// A key file's contents, with its declared version.
#[derive(Clone)]
pub struct VersionedKey {
    version: u32,
    bytes: Vec<u8>,
}

impl std::fmt::Debug for VersionedKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VersionedKey(version={}, [redacted])", self.version)
    }
}

impl VersionedKey {
    /// A key from raw bytes and a version, for tests and for the cursor key's
    /// legacy unversioned file.
    ///
    /// # Panics
    ///
    /// Never; a short key is accepted here because the callers that read files
    /// check the length.
    #[must_use]
    pub fn from_parts(version: u32, bytes: Vec<u8>) -> Self {
        Self { version, bytes }
    }

    /// The declared version.
    #[must_use]
    pub const fn version(&self) -> u32 {
        self.version
    }

    /// The raw key bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Read a versioned key file and refuse a missing, malformed, short or
/// unexpectedly rotated key.
///
/// # Errors
///
/// [`KeyError`] naming the file.
pub fn read_versioned_key(path: &Path, expected_version: u32) -> Result<VersionedKey, KeyError> {
    let text = std::fs::read_to_string(path).map_err(|e| KeyError::Missing {
        path: path.display().to_string(),
        reason: e.to_string(),
    })?;
    let file: KeyFile = serde_yaml::from_str(&text).map_err(|e| KeyError::Malformed {
        path: path.display().to_string(),
        reason: format!(
            "expected `version: <integer>` and `key: <base64>`; {}",
            crate::validate::bounded(&e.to_string(), 200)
        ),
    })?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(file.key.trim())
        .map_err(|_| KeyError::Malformed {
            path: path.display().to_string(),
            reason: "`key` is not standard base64".to_string(),
        })?;
    if bytes.len() < MIN_KEY_BYTES {
        return Err(KeyError::Malformed {
            path: path.display().to_string(),
            reason: format!(
                "`key` decodes to {} bytes; at least {MIN_KEY_BYTES} random bytes are required \
                 (for example `printf 'version: 1\\nkey: \"%s\"\\n' \"$(openssl rand -base64 32)\"`)",
                bytes.len()
            ),
        });
    }
    if file.version != expected_version {
        return Err(KeyError::UnexpectedVersion {
            path: path.display().to_string(),
            found: file.version,
            expected: expected_version,
        });
    }
    Ok(VersionedKey {
        version: file.version,
        bytes,
    })
}

fn derive(key: &[u8], label: &[u8]) -> [u8; 32] {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key).expect("HMAC takes any key length");
    mac.update(label);
    let out = mac.finalize().into_bytes();
    let mut subkey = [0u8; 32];
    subkey.copy_from_slice(&out);
    subkey
}

/// The two subkeys derived from one key file, plus its version.
pub struct CookieKeys {
    version: u32,
    aead: LessSafeKey,
    csrf: [u8; 32],
    random: SystemRandom,
}

impl std::fmt::Debug for CookieKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CookieKeys(version={}, [redacted])", self.version)
    }
}

/// Why a sealed value did not open.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenError {
    /// The value is not the `lw1.<version>.<nonce>.<ciphertext>` shape.
    Malformed,
    /// The value was sealed under a different key version.
    WrongKeyVersion,
    /// Authentication failed: a tampered or foreign value.
    NotAuthentic,
}

impl CookieKeys {
    /// Derive the cookie keys from a versioned key file's bytes.
    #[must_use]
    pub fn new(key: &VersionedKey) -> Self {
        let aead_bytes = derive(key.bytes(), LABEL_AEAD);
        let aead = LessSafeKey::new(
            UnboundKey::new(&CHACHA20_POLY1305, &aead_bytes)
                .expect("the derived subkey is exactly 32 bytes"),
        );
        Self {
            version: key.version(),
            aead,
            csrf: derive(key.bytes(), LABEL_CSRF),
            random: SystemRandom::new(),
        }
    }

    /// The key version, carried in every sealed value and in the cookie.
    #[must_use]
    pub const fn version(&self) -> u32 {
        self.version
    }

    /// `n` cryptographically random bytes.
    ///
    /// # Panics
    ///
    /// If the operating system's randomness source fails, which is not a
    /// condition this service can continue through.
    #[must_use]
    pub fn random_bytes(&self, n: usize) -> Vec<u8> {
        let mut out = vec![0u8; n];
        self.random
            .fill(&mut out)
            .expect("the system randomness source is available");
        out
    }

    /// A URL-safe random token of `n` bytes of entropy.
    #[must_use]
    pub fn random_token(&self, n: usize) -> String {
        B64.encode(self.random_bytes(n))
    }

    /// Seal `plaintext` for one cookie name.
    ///
    /// The returned value is `lw1.<key version>.<nonce>.<ciphertext+tag>`, all
    /// URL-safe base64 without padding, so it is a valid cookie value.
    ///
    /// # Panics
    ///
    /// If the randomness source fails.
    #[must_use]
    pub fn seal(&self, domain: &str, plaintext: &[u8]) -> String {
        let nonce_bytes = self.random_bytes(NONCE_LEN);
        let nonce = Nonce::try_assume_unique_for_key(&nonce_bytes)
            .expect("NONCE_LEN bytes are a valid nonce");
        let mut buffer = plaintext.to_vec();
        self.aead
            .seal_in_place_append_tag(nonce, Aad::from(self.aad(domain)), &mut buffer)
            .expect("ChaCha20-Poly1305 sealing does not fail for in-memory buffers");
        format!(
            "{SEAL_PREFIX}.{}.{}.{}",
            self.version,
            B64.encode(&nonce_bytes),
            B64.encode(&buffer)
        )
    }

    /// Open a sealed value for one cookie name.
    ///
    /// # Errors
    ///
    /// [`OpenError`], which distinguishes "sealed under another key version"
    /// from "not authentic" so that a key rotation reads as an expired session
    /// rather than as an attack.
    pub fn open(&self, domain: &str, value: &str) -> Result<Vec<u8>, OpenError> {
        let mut parts = value.split('.');
        let (Some(prefix), Some(version), Some(nonce), Some(sealed), None) = (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) else {
            return Err(OpenError::Malformed);
        };
        if prefix != SEAL_PREFIX {
            return Err(OpenError::Malformed);
        }
        let version: u32 = version.parse().map_err(|_| OpenError::Malformed)?;
        if version != self.version {
            return Err(OpenError::WrongKeyVersion);
        }
        let nonce_bytes = B64.decode(nonce).map_err(|_| OpenError::Malformed)?;
        let nonce =
            Nonce::try_assume_unique_for_key(&nonce_bytes).map_err(|_| OpenError::Malformed)?;
        let mut buffer = B64.decode(sealed).map_err(|_| OpenError::Malformed)?;
        let opened = self
            .aead
            .open_in_place(nonce, Aad::from(self.aad(domain)), &mut buffer)
            .map_err(|_| OpenError::NotAuthentic)?;
        Ok(opened.to_vec())
    }

    /// The synchronizer CSRF token for a session id.
    ///
    /// It is DERIVED, not stored: any process holding the same key file answers
    /// the same token for the same session, so a restart or a second replica
    /// does not invalidate a browser's token, and nothing has to be kept in
    /// memory for it.
    #[must_use]
    pub fn csrf_token(&self, session_id: &str) -> String {
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(self.csrf.as_ref())
            .expect("HMAC takes any key length");
        mac.update(b"logweir-api/csrf/v1\n");
        mac.update(session_id.as_bytes());
        B64.encode(mac.finalize().into_bytes())
    }

    /// Whether `presented` is the synchronizer token for `session_id`,
    /// compared in constant time.
    #[must_use]
    pub fn csrf_token_matches(&self, session_id: &str, presented: &str) -> bool {
        let expected = self.csrf_token(session_id);
        constant_time_eq(expected.as_bytes(), presented.as_bytes())
    }

    fn aad(&self, domain: &str) -> Vec<u8> {
        let mut aad = b"logweir-api/cookie/v1\n".to_vec();
        aad.extend_from_slice(&self.version.to_be_bytes());
        aad.push(b'\n');
        aad.extend_from_slice(domain.as_bytes());
        aad
    }
}

/// Compare two byte strings without an early return.
#[must_use]
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// The SHA-256 of a session id, for the audit record. The session id itself is
/// never logged.
#[must_use]
pub fn session_id_hash(session_id: &str) -> String {
    logweir_core::ids::sha256_prefixed(session_id.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(version: u32) -> CookieKeys {
        CookieKeys::new(&VersionedKey::from_parts(version, vec![0x11; 32]))
    }

    fn write(dir: &Path, name: &str, body: &str) -> std::path::PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, body).unwrap();
        path
    }

    #[test]
    fn a_sealed_value_opens_only_under_its_own_domain_version_and_bytes() {
        let k = keys(1);
        let sealed = k.seal("__Host-logweir_session", b"who am i");
        assert_eq!(
            k.open("__Host-logweir_session", &sealed).unwrap(),
            b"who am i"
        );

        // Another cookie name is a different associated-data domain.
        assert_eq!(
            k.open("__Host-logweir_login", &sealed),
            Err(OpenError::NotAuthentic)
        );
        // Another key version is reported as such, not as tampering.
        assert_eq!(
            keys(2).open("__Host-logweir_session", &sealed),
            Err(OpenError::WrongKeyVersion)
        );
        // Another key file of the same version does not open it.
        let other = CookieKeys::new(&VersionedKey::from_parts(1, vec![0x22; 32]));
        assert_eq!(
            other.open("__Host-logweir_session", &sealed),
            Err(OpenError::NotAuthentic)
        );
        // One flipped character in the ciphertext.
        let mut tampered: Vec<char> = sealed.chars().collect();
        let last = tampered.len() - 1;
        tampered[last] = if tampered[last] == 'A' { 'B' } else { 'A' };
        let tampered: String = tampered.into_iter().collect();
        assert!(matches!(
            k.open("__Host-logweir_session", &tampered),
            Err(OpenError::NotAuthentic | OpenError::Malformed)
        ));
        for bad in [
            "",
            "lw1",
            "lw2.1.a.b",
            "lw1.x.a.b",
            "lw1.1.!.b",
            "lw1.1.a.b.c",
        ] {
            assert!(k.open("__Host-logweir_session", bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn two_seals_of_the_same_plaintext_differ() {
        let k = keys(1);
        assert_ne!(k.seal("c", b"same"), k.seal("c", b"same"));
    }

    #[test]
    fn the_csrf_token_is_derived_and_domain_separated() {
        let k = keys(1);
        let token = k.csrf_token("sid-1");
        assert_eq!(
            token,
            keys(1).csrf_token("sid-1"),
            "a restart keeps the token"
        );
        assert_ne!(token, k.csrf_token("sid-2"));
        assert!(k.csrf_token_matches("sid-1", &token));
        assert!(!k.csrf_token_matches("sid-2", &token));
        assert!(!k.csrf_token_matches("sid-1", ""));
        assert!(!k.csrf_token_matches("sid-1", &format!("{token}x")));
        // The CSRF subkey is not the AEAD subkey: sealing with the CSRF token
        // as a key would be a different value, and the token is not a seal.
        assert!(!token.starts_with(SEAL_PREFIX));
    }

    #[test]
    fn a_missing_malformed_short_or_rotated_key_file_is_refused() {
        let dir = std::env::temp_dir().join(format!("lw-keys-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        let missing = dir.join("nope.yaml");
        assert!(matches!(
            read_versioned_key(&missing, 1),
            Err(KeyError::Missing { .. })
        ));

        let not_yaml = write(&dir, "bad.yaml", "just a string\n\t- and a tab");
        assert!(matches!(
            read_versioned_key(&not_yaml, 1),
            Err(KeyError::Malformed { .. })
        ));

        let no_version = write(&dir, "noversion.yaml", "key: \"AAAA\"\n");
        assert!(matches!(
            read_versioned_key(&no_version, 1),
            Err(KeyError::Malformed { .. })
        ));

        let short = write(&dir, "short.yaml", "version: 1\nkey: \"AAAAAAAA\"\n");
        assert!(matches!(
            read_versioned_key(&short, 1),
            Err(KeyError::Malformed { .. })
        ));

        let good_body = format!(
            "version: 3\nkey: \"{}\"\n",
            base64::engine::general_purpose::STANDARD.encode([7u8; 32])
        );
        let good = write(&dir, "good.yaml", &good_body);
        assert_eq!(
            read_versioned_key(&good, 3).unwrap().bytes(),
            &[7u8; 32][..]
        );
        // The refusal that makes rotation deliberate.
        assert!(matches!(
            read_versioned_key(&good, 2),
            Err(KeyError::UnexpectedVersion {
                found: 3,
                expected: 2,
                ..
            })
        ));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_key_never_prints_itself() {
        let k = VersionedKey::from_parts(4, vec![0xab; 32]);
        assert_eq!(format!("{k:?}"), "VersionedKey(version=4, [redacted])");
        assert_eq!(
            format!("{:?}", CookieKeys::new(&k)),
            "CookieKeys(version=4, [redacted])"
        );
    }
}
