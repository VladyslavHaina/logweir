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
pub mod doubles;
pub mod exit_code_lint;

use std::io::ErrorKind;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use logweir::drill::DrillError;
use logweir::exit::ExitCode;

pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("the repository root resolves from CARGO_MANIFEST_DIR")
}

pub fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{} is readable: {e}", path.display()))
}

pub fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_logweir"))
}

/// A throwaway tree, deleted on drop. Each gate test builds its own with an
/// `impl Overlay { fn new … }` of its own.
pub struct Overlay {
    pub path: PathBuf,
}

impl Drop for Overlay {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

pub fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// A document's `format_version` is **at least** `min` — the minor that defines
/// the field under test — rather than an exact pin. Receipt and catalog-point
/// versions are renumbered when several format-bumping rows integrate
/// together, and a future renumber only raises the stamped
/// version, so `>= min` survives it where `== min` would break a sibling's CI.
pub fn assert_format_at_least(actual: &str, min: &str, what: &str) {
    let triple = |v: &str| -> (u64, u64, u64) {
        let mut it = v.split('.').map(|p| p.parse::<u64>().unwrap_or(0));
        (
            it.next().unwrap_or(0),
            it.next().unwrap_or(0),
            it.next().unwrap_or(0),
        )
    };
    assert!(
        triple(actual) >= triple(min),
        "{what}: format_version {actual:?} is below the minimum {min:?} that defines this field"
    );
}

pub fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the workspace root is two levels above crates/logweir")
}

/// `@sha256:` followed by exactly 64 lowercase hex characters, and nothing
/// after it.
pub fn is_digest_reference(reference: &str) -> bool {
    let Some((name, digest)) = reference.split_once("@sha256:") else {
        return false;
    };
    !name.is_empty()
        && digest.len() == 64
        && digest
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
}

pub const LIMIT: Duration = Duration::from_secs(20);

pub const EXPECTED: &str = "v1:00000000-0000-0000-0000-000000000001:sha256:abc";

pub const FOREIGN: &str = "v1:99999999-9999-9999-9999-999999999999:sha256:def";

pub fn sentinel() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap().to_string();
    (listener, address)
}

pub fn assert_no_connection(listener: &TcpListener, label: &str) {
    match listener.accept() {
        Err(error) if error.kind() == ErrorKind::WouldBlock => {}
        Ok((_, peer)) => panic!("{label}: the sentinel was dialled from {peer}"),
        Err(error) => panic!("{label}: inspect the sentinel: {error}"),
    }
}

pub fn base(root: &Path) -> Command {
    let tmp = root.join("tmp");
    std::fs::create_dir_all(&tmp).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_logweir"));
    command
        .env_clear()
        .env("TMPDIR", &tmp)
        .env("RUST_LOG", "info")
        .env("LOGWEIR_ENGINE_BIN", root.join("no-engine"))
        .env("LOGWEIR_ENGINE_VERSION", "sentinel-version")
        .env("LOGWEIR_ENGINE_DIGEST", "sha256:sentinel-digest");
    command
}

/// The last non-empty stdout line — what a controller tailing `pods/log`
/// reads.
pub fn last_line(stdout: &str) -> &str {
    stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .next_back()
        .unwrap_or("")
}

/// The exit code the SHIPPED mapping (`DrillError::exit_code`, Global
/// Constraint 11) gives this result. `Ok` is exit 0 by that contract; nothing
/// here invents a code.
pub fn exit_of<T>(r: &Result<T, DrillError>) -> ExitCode {
    match r {
        Ok(_) => ExitCode::Ok,
        Err(e) => e.exit_code(),
    }
}

pub fn guard_message(e: &DrillError) -> String {
    match e {
        DrillError::Guard(g) => g.0.clone(),
        other => panic!("expected a guard refusal, got {other:?}"),
    }
}
