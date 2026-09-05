//! Run-id minting. This module exists in `crates/logweir` and NOT in
//! `logweir-core` for one reason: **Global Constraint 1**.
//!
//! `logweir-core`'s own module doc says "No I/O, no clock, no network", and
//! three separate comments in this crate say "the clock is read HERE … never
//! in `logweir-core`". `logweir_core::ids::new_run_id` used to call
//! `ulid::Ulid::new()`, which reads `SystemTime::now()` and the OS entropy
//! pool — the one leak in an otherwise genuinely pure crate, and the only
//! timestamp in this build not taken by the impure layer and passed in.
//!
//! Both impure inputs are now taken here. `logweir_core::ids::format_run_id`
//! is the pure encoder they are handed to, so the ULID layout stays defined in
//! the crate that owns the document format while the clock stays out of it.
//! `logweir-core` no longer even LINKS `ulid`, which is what
//! `scripts/check-pure-core.sh` checks — a grep over the crate's source and a
//! `cargo tree` over its dependencies, so the constraint is machine-checked
//! rather than asserted in a comment.

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
