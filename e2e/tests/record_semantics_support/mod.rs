//! PROD-01.1's support code, shared by `e2e/tests/record_semantics.rs`.
//!
//! `oracle` is pure and is also compiled, alone, by the always-on negative
//! controls in `e2e/tests/record_semantics_oracle.rs`. `kafka` does I/O
//! against the compose stack and exists only under the `e2e` feature, like
//! the file that uses it.
pub mod kafka;
pub mod oracle;
