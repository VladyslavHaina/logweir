use sha2::{Digest, Sha256};

pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

/// "sha256:<hex>" — the form used for engine.digest, approval.plan_hash and
/// target.topic_mapping_sha256.
pub fn sha256_prefixed(bytes: &[u8]) -> String {
    format!("sha256:{}", sha256_hex(bytes))
}

/// ULID: 48-bit big-endian millisecond timestamp + 80 random bits, Crockford
/// base32, 26 characters. Lexicographically sortable, so `run_id` also orders
/// the bucket listing.
pub fn new_run_id() -> String {
    ulid::Ulid::new().to_string()
}
