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

/// **One run id, as an evidence key names it** (FX-36): non-empty ASCII
/// letters, digits, `-` and `_`. A minted run id is a 26-character ULID
/// ([`format_run_id`]); the wider set admits the ids fixtures and older
/// writers use. A key built from it (`logweir/drills/<run id>.json`,
/// `logweir/switchovers/<run id>.record.json`) can then hold no separator, no
/// `.` or `..`, and no byte an object-store path rewrites, so a reader that
/// confines a key to that shape reads exactly the key it names.
#[must_use]
pub fn is_run_id_segment(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// The run id a drill scorecard key names: `Some(id)` for exactly
/// `logweir/drills/<id>.json` with [`is_run_id_segment`], `None` for every
/// other key — the run's other documents (`.offsets.json`, `.teardown.json`,
/// `.positions-mapping.json`) included, as their stems hold a `.`.
#[must_use]
pub fn scorecard_run_id(key: &str) -> Option<&str> {
    key.strip_prefix("logweir/drills/")
        .and_then(|rest| rest.strip_suffix(".json"))
        .filter(|id| is_run_id_segment(id))
}

/// The PURE half of run-id minting: encode a timestamp and 80 bits of
/// randomness as a ULID — 48-bit big-endian millisecond timestamp + 80 random
/// bits, Crockford base32, 26 characters. Lexicographically sortable, so
/// `run_id` also orders the bucket listing.
///
/// Global Constraint 1, and this function's shape IS the constraint. This
/// crate reads no clock and no entropy: both inputs are taken by
/// `crates/logweir` (`logweir::ids::new_run_id`) and passed in, exactly as
/// every timestamp in a scorecard already is. The predecessor of this function
/// called `ulid::Ulid::new()`, which reads `SystemTime::now()` plus
/// `rand::rng()` — falsifying this crate's own module doc ("No I/O, no clock,
/// no network") and the three comments in `crates/logweir` that assert the
/// clock is never read here. Nothing machine-checked that dimension until
/// `scripts/check-pure-core.sh`, which now greps this crate for clock and
/// entropy APIs and refuses `Ulid::new` by name.
///
/// `randomness`'s low 80 bits are what `ulid` uses; higher bits are ignored by
/// `Ulid::from_parts`, which is why the caller may hand it a whole `u128`.
pub fn format_run_id(timestamp_ms: u64, randomness: u128) -> String {
    ulid::Ulid::from_parts(timestamp_ms, randomness).to_string()
}
