//! GC3's gate, and the tests that keep it a gate rather than a comment.
//!
//! `scripts/check-no-oso.sh` proves two narrow, mechanical properties about the
//! engine subcommands reachable from shipped code:
//!
//! * **Check A — the invocation shape.** Every double-quoted literal that is an
//!   argv token and that sits inside an engine-invocation expression is one of
//!   the four contracted subcommands (plus `run` and the argv furniture
//!   `--config`, `--format`, `json`). This is the gate.
//! * **Check B — the token scan.** The denied subcommand tokens (`list`,
//!   `restore-status`, `offset`, `evidence-verify`, `validation
//!   evidence-verify`) do not appear anywhere under `crates/` without a
//!   justified `// engine-token-ok: <reason>` escape on the same physical line.
//!   This is the secondary: it owns the two-word forms and the tokens built
//!   outside an invocation, which check A cannot see.
//!
//! **The two are not redundant and neither subsumes the other.** Check B's
//! escape does not excuse check A — the refusal fixture below carries a valid
//! `// engine-token-ok:` line comment and is refused anyway, which is what makes
//! "add `list` to `ENGINE_ARGV_ALLOWLIST`" a killable mutant rather than one
//! check silently standing in for the other.
//!
//! Why a Rust test at all, when the script is the gate: because the script's RED
//! side is the half that decays. Every test here writes a throwaway workspace
//! overlay in `std::env::temp_dir()` and points `LOGWEIR_ROOT` at it, which is
//! also what keeps the tracked tree byte-identical across a run — and, more
//! importantly, what keeps `cargo tree` and `cargo metadata` out of `cargo
//! test`. Those two take the package-cache lock; a unit test that takes it is a
//! unit test that can hang behind a concurrent build, and GC22 bounds every test
//! at 15 s. With `LOGWEIR_ROOT` set the script runs the GC3 block only.
//!
//! `crates/logweir/tests/one_signer_gate.rs` is the precedent for a test in this
//! package that shells a repository script against an overlay.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU32, Ordering};

/// The repository root, from this package's manifest directory.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("the repository root resolves from CARGO_MANIFEST_DIR")
}

/// A throwaway workspace overlay, deleted on drop.
struct Overlay(PathBuf);

impl Drop for Overlay {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

impl Overlay {
    /// Write one `crates/x/src/a.rs` containing `body`.
    fn with_source(body: &str) -> Overlay {
        static N: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "logweir-engine-allowlist-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let src = dir.join("crates").join("x").join("src");
        std::fs::create_dir_all(&src).expect("the overlay directory is creatable");
        std::fs::write(src.join("a.rs"), body).expect("the overlay source is writable");
        Overlay(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

/// Run the gate against an overlay. The exit status is read from the child
/// process directly — never from a pipeline and never from `$?` after one.
fn run_gate(overlay: &Overlay) -> Output {
    let root = repo_root();
    Command::new("bash")
        .arg(root.join("scripts").join("check-no-oso.sh"))
        .current_dir(&root)
        .env("LOGWEIR_ROOT", overlay.path())
        .output()
        .expect("the gate script can be spawned")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn report(out: &Output) -> String {
    format!(
        "status={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        text(&out.stdout),
        text(&out.stderr)
    )
}

/// Every run of whitespace collapsed to a single space. Load-bearing for the
/// ADR assertions: the phrase under test wraps across two lines in the file, so
/// an un-normalised "absent" assertion is vacuously true and an un-normalised
/// "present" assertion fails the moment the 79-column wrapping is kept.
fn collapse(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The failure text check A prints. Quotes `ENGINE_RUNTIME_ALLOWLIST` — the four
/// subcommands GC3 states — and never `ENGINE_ARGV_ALLOWLIST`, whose extra
/// tokens are argv furniture.
const CONTRACT: &str = "outside {backup, restore, validate-restore, validation run}";

// ------------------------------------------------------------------- check A

/// The four runtime commands, in the argv shape a real invocation uses. The
/// fifth row is the two-word `validation run`, whose second word is the only
/// member of `ENGINE_ARGV_ALLOWLIST` that no other row exercises.
const PERMITTED_ARGV: [&str; 5] = [
    r#""backup", "--config", p"#,
    r#""restore", "--config", p"#,
    r#""validate-restore", "--config", p"#,
    r#""validation", "--config", p"#,
    r#""validation", "run", "--config", p"#,
];

/// The denied subcommands, each named here as this gate's own fixture input.
const DENIED: [&str; 4] = [
    "list",            // engine-token-ok: this gate's own fixture input, not an invocation
    "restore-status",  // engine-token-ok: this gate's own fixture input, not an invocation
    "offset",          // engine-token-ok: this gate's own fixture input, not an invocation
    "evidence-verify", // engine-token-ok: this gate's own fixture input, not an invocation
];

/// One overlay source file whose only engine invocation carries `args`.
/// `trailer` goes on the **invocation's own physical line**, which is the only
/// line a `// engine-token-ok:` escape can sit on and be seen by check B — an
/// escape on the closing brace escapes the closing brace.
fn invocation(args: &str, trailer: &str) -> String {
    format!(
        "fn spawn(p: &str) {{\n    \
         subprocess::run_engine(&self.binary, &[{args}], &mut |_, _| {{}});{trailer}\n}}\n"
    )
}

#[test]
fn engine_allowlist_permits_the_four_runtime_commands() {
    for args in PERMITTED_ARGV {
        let overlay = Overlay::with_source(&invocation(args, ""));
        let out = run_gate(&overlay);
        assert_eq!(
            out.status.code(),
            Some(0),
            "an invocation naming only contracted subcommands must pass: &[{args}]\n{}",
            report(&out)
        );
    }
}

#[test]
fn engine_allowlist_refuses_a_fifth_command_at_an_invocation_site() {
    for token in DENIED {
        // The fixture carries a VALID `// engine-token-ok:` escape on purpose:
        // check B is thereby satisfied and check A alone decides the exit code.
        // Without it a widened `ENGINE_ARGV_ALLOWLIST` would still be caught by
        // check B, and this test would pass against a hole in the gate.
        let body = invocation(
            &format!(r#""{token}", "--config", p"#),
            " // engine-token-ok: check B is escaped so check A alone decides",
        );
        let overlay = Overlay::with_source(&body);
        let out = run_gate(&overlay);
        assert_eq!(
            out.status.code(),
            Some(1),
            "an invocation naming `{token}` must be refused\n{}",
            report(&out)
        );
        let stdout = text(&out.stdout);
        assert!(
            stdout.contains(CONTRACT),
            "the refusal must quote the four-command contract for `{token}`\n{}",
            report(&out)
        );
        assert!(
            stdout.contains(&format!("engine invocation names `{token}`")),
            "the refusal must name the offending token `{token}`\n{}",
            report(&out)
        );
    }
}

// ------------------------------------------------------------------- check B

/// The prose fixture: a denied token that is an ordinary English word, outside
/// every invocation span, carrying the justification the tracked tree uses.
const PROSE: &str = concat!(
    "fn f() {\n    let s = \"offset\"; ", // engine-token-ok: builds this gate's own fixture input
    "// engine-token-ok: asserts on `compare`'s own mismatch wording, not a kafka-backup CLI subcommand\n}\n"
);

/// The same line with a bare marker and no reason.
const UNJUSTIFIED: &str = concat!(
    "fn f() {\n    let s = \"offset\"; ", // engine-token-ok: builds this gate's own fixture input
    "// engine-token-ok:\n}\n"
);

#[test]
fn engine_allowlist_permits_the_word_offset_in_prose() {
    let overlay = Overlay::with_source(PROSE);
    let out = run_gate(&overlay);
    assert_eq!(
        out.status.code(),
        Some(0),
        "a justified denied token outside every invocation span must pass\n{}",
        report(&out)
    );
}

#[test]
fn engine_allowlist_refuses_an_unjustified_token() {
    let overlay = Overlay::with_source(UNJUSTIFIED);
    let out = run_gate(&overlay);
    assert_eq!(
        out.status.code(),
        Some(1),
        "a bare `// engine-token-ok:` with no reason is not an escape\n{}",
        report(&out)
    );
}

// ---------------------------------------------------------------- the two ADRs

#[test]
fn adr_0002_decision_reads_four_commands() {
    let adr = std::fs::read_to_string(repo_root().join("docs/adr/0002-shell-out.md"))
        .expect("ADR 0002 is readable");
    let flat = collapse(&adr);
    assert!(
        flat.contains(
            "exactly four subcommands reachable from shipped code: \
             `backup`, `restore`, `validate-restore`, `validation run`"
        ),
        "ADR 0002's Decision must read four commands (whitespace-normalised)"
    );
    assert!(
        !flat.contains("exactly three subcommands"),
        "ADR 0002 must no longer fix the count at three (whitespace-normalised)"
    );
}

#[test]
fn adr_0008_records_all_four_amendments() {
    let adr =
        std::fs::read_to_string(repo_root().join("docs/adr/0008-mvp-constraint-amendments.md"))
            .expect("ADR 0008 is readable");
    for heading in [
        "## Amendment A",
        "## Amendment B",
        "## Amendment C",
        "## Amendment D",
    ] {
        assert!(adr.contains(heading), "ADR 0008 must carry `{heading}`");
    }
    assert_eq!(
        adr.matches("RestoreDrill").count(),
        1,
        "`RestoreDrill` is named exactly once in ADR 0008, in Amendment A, as retired"
    );
}

// ------------------------------------------------------- the two allowlists

#[test]
fn the_two_engine_allowlists_are_declared_separately() {
    let script = std::fs::read_to_string(repo_root().join("scripts/check-no-oso.sh"))
        .expect("the gate script is readable");
    assert!(
        script.contains(r#"ENGINE_RUNTIME_ALLOWLIST="backup restore validate-restore validation""#),
        "the CONTRACT allowlist is declared, verbatim"
    );
    assert!(
        script.contains("ENGINE_ARGV_ALLOWLIST="),
        "the argv allowlist is declared separately"
    );

    let message: Vec<&str> = script
        .lines()
        .filter(|l| l.contains("which is outside"))
        .collect();
    assert_eq!(
        message.len(),
        1,
        "exactly one line builds the failure message; found {:?}",
        message
    );
    assert!(
        message[0].contains("ENGINE_RUNTIME_ALLOWLIST"),
        "the failure message quotes the CONTRACT: {}",
        message[0]
    );
    assert!(
        !message[0].contains("ENGINE_ARGV_ALLOWLIST"),
        "the failure message must not quote the argv furniture: {}",
        message[0]
    );
}
