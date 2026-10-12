//! Run-id minting. This module is in `crates/logweir` and NOT in
//! `logweir-core` because of **Global Constraint 1**: `logweir-core` reads no
//! clock and no entropy. Both impure inputs are taken here and handed to
//! `logweir_core::ids::format_run_id`, the pure encoder, so the ULID layout
//! stays with the crate that owns the document format while the clock stays
//! out of it. `scripts/check-pure-core.sh` checks that `logweir-core` does not
//! link `ulid`.

/// A fresh run id: 48-bit big-endian millisecond timestamp + 80 random bits.
///
/// Global Constraint 1: the clock and the entropy are read HERE, in
/// `crates/logweir`.
pub fn new_run_id() -> String {
    let now = chrono::Utc::now();
    // A ULID's timestamp field is 48 unsigned bits of Unix milliseconds, so a
    // pre-1970 clock has no representation at all. Clamping at 0 keeps the id
    // well-formed on a host whose clock is absurd; it is not a correctness
    // claim about that clock, and every timestamp IN the scorecard is written
    // separately and unclamped, so a nonsense clock still shows up there.
    let ms = now.timestamp_millis().max(0) as u64;
    logweir_core::ids::format_run_id(ms, random_80_bits())
}

/// The 80 random bits of a ULID, from the OS entropy `ulid` itself uses.
///
/// `Ulid::new()` mints a whole id — timestamp included — from the clock; only
/// its random half is taken, and the clock half it read is discarded in favour
/// of the `chrono::Utc::now()` above, so this crate has exactly one notion of
/// "now". `Ulid::random()` returns the low 80 bits, already masked.
fn random_80_bits() -> u128 {
    ulid::Ulid::new().random()
}
