//! Shared by any `crates/logweir/tests/*.rs` that wants it. Declare with
//! `mod support;` as the first line of the test file and reach the members as
//! `support::<module>` — the same idiom `crates/logweir/tests/fixtures/mod.rs`
//! already establishes in this tree (`mod fixtures;` at `approval.rs:1`,
//! `restore_phase.rs:1`, `verify_phase.rs:1` and five more).
//!
//! **`mod support;` is the ONLY include mechanism** (controller ruling, Task
//! 12). Cargo compiles every `tests/*.rs` as its own test binary but does NOT
//! compile `tests/<dir>/*.rs` standalone, so a directory module is reached by
//! declaring it — never by `#[path = "support/exit_code_lint.rs"]`, which
//! would give each consumer its own copy of the module under its own name and
//! defeat the point of there being one helper.
//!
//! Unused items in a given binary are expected — a consumer that wants one
//! module gets the whole directory — hence the allow, which propagates to the
//! child modules below.
#![allow(dead_code)]

pub mod dial_tokens;
pub mod exit_code_lint;
