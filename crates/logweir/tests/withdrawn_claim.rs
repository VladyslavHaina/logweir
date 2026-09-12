//! G-SIGN, SECOND HALF — the corpus grep, and the tests that keep it a gate.
//! Task 14.
//!
//! `scripts/check-withdrawn-claim.sh` walks every shipped surface for a fixed
//! list of phrases: the stronger claim about signing that was made once, was
//! RED on the tree it was asserted about, and was WITHDRAWN, plus its near
//! paraphrases. `scripts/check-one-signer.sh:10-19` forbids restating it, in
//! those words, and binds "this script's output, its comments, or the CI step
//! that runs it" by name; the grep is what makes that instruction enforceable
//! on surfaces nobody thought to look at.
//!
//! WHAT IS TRUE, and what every surface in this repository may say, is Global
//! Constraint 27's narrowed position: no control-plane CRATE LINKS the signer
//! (that is `check-one-signer.sh`'s checks 1 and 3), and the CAPABILITY to sign
//! is unbroken while a component holds Job CRUD over the signing key's
//! namespace — residual **O1**, accepted, O0 default (a).
//!
//! THIS FILE DELIBERATELY CONTAINS NONE OF THE PHRASES. Every `*.rs` under
//! `crates/` is walked, and only two paths are exempt, neither of them this
//! one — so the red-side tests below read the phrases OUT OF THE SCRIPT rather
//! than embedding them. That is not a workaround: it also means the probes are
//! the real list, and a phrase added to the script without being enforced
//! fails `the_gate_enforces_every_phrase_on_its_own_list`.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("the repository root resolves from CARGO_MANIFEST_DIR")
}

const GATE: &str = "scripts/check-withdrawn-claim.sh";

fn gate_source() -> String {
    std::fs::read_to_string(repo_root().join(GATE))
        .unwrap_or_else(|e| panic!("{GATE} is readable: {e}"))
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// The gate's own phrase list, read out of the `PHRASES=( … )` array.
fn phrases() -> Vec<String> {
    let src = gate_source();
    let start = src
        .find("PHRASES=(")
        .expect("the gate declares a PHRASES array");
    let rest = &src[start..];
    let end = rest.find("\n)").expect("the PHRASES array is closed");
    let body = &rest[..end];
    let out: Vec<String> = body
        .lines()
        .skip(1)
        .map(str::trim)
        .filter(|l| l.starts_with('"') && l.ends_with('"'))
        .map(|l| l.trim_matches('"').to_string())
        .collect();
    assert!(
        out.len() >= 6,
        "the phrase list must still carry at least the six phrases the plan \
         names; it parsed as {out:?}"
    );
    out
}

/// Run the gate. The exit status is read from the child process directly —
/// never from a pipeline and never from `$?` after one (STANDING RULE 20).
fn run_gate(cwd: &Path, logweir_root: Option<&Path>) -> Output {
    let mut cmd = Command::new("bash");
    cmd.arg(GATE).current_dir(cwd);
    if let Some(r) = logweir_root {
        cmd.env("LOGWEIR_ROOT", r);
    }
    cmd.output().expect("the gate script can be spawned")
}

/// **G-SIGN, second half.** The green side, on the real tree.
#[test]
fn no_shipped_surface_restates_the_withdrawn_cannot_sign_claim() {
    let root = repo_root();
    let out = run_gate(&root, None);
    assert!(
        out.status.success(),
        "{GATE} must be green on this tree; status={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        text(&out.stdout),
        text(&out.stderr)
    );
}

/// The exemption is exactly two paths, as literals.
///
/// A pattern would be the cheap way out for whoever hits this gate first:
/// `scripts/*` exempts every future script, `docs/*` exempts the
/// documentation, and either turns the gate into a comment. The comparison is
/// string equality and there is no third entry.
#[test]
fn the_exemption_list_is_exactly_two_paths() {
    let src = gate_source();
    for expected in [
        "EXEMPT_1=\"scripts/check-one-signer.sh\"",
        "EXEMPT_2=\"scripts/check-withdrawn-claim.sh\"",
    ] {
        assert!(
            src.lines().any(|l| l == expected),
            "{GATE} must exempt the two gate scripts by LITERAL path: the line \
             `{expected}` is missing"
        );
    }
    assert!(
        !src.contains("EXEMPT_3"),
        "the exemption list is exactly TWO paths. A third exemption is how a \
         whole surface — `docs/`, say — leaves the gate's coverage without \
         anyone reading a diff that says so."
    );
    let compare = "  [ \"$1\" = \"$EXEMPT_1\" ] || [ \"$1\" = \"$EXEMPT_2\" ]";
    assert!(
        src.lines().any(|l| l == compare),
        "the exemption must be STRING EQUALITY against the two literals and \
         nothing else — never a glob, never a `case` pattern, never a prefix \
         match. Expected the line `{compare}`."
    );
    for name in ["EXEMPT_1", "EXEMPT_2"] {
        let prefix = format!("{name}=\"");
        let line = src
            .lines()
            .find(|l| l.starts_with(&prefix))
            .expect("the exemption is assigned");
        let value = line[prefix.len()..].trim_end_matches('"');
        assert!(
            !value.contains('*') && !value.contains('?') && !value.contains('['),
            "{name} must be a literal path, not a pattern; it was `{value}`"
        );
    }
}

/// `just lint` is the enforcement point: `ci.yml` mirrors it (green since
/// 2026-09-12), and membership in the recipe is what makes this gate
/// enforced on a laptop before a push.
///
/// It asserts membership and nothing else — not the recipe's length, not its
/// line numbers, not the absence of other arms — so a later task adding a guard
/// to `lint` cannot turn it red. Same shape as
/// `one_signer_gate.rs::just_lint_runs_the_one_signer_gate`.
#[test]
fn the_withdrawn_claim_gate_is_in_just_lint() {
    let justfile = std::fs::read_to_string(repo_root().join("justfile")).expect("justfile is read");
    let mut body = String::new();
    let mut inside = false;
    for line in justfile.lines() {
        if line.starts_with("lint:") {
            inside = true;
            continue;
        }
        if inside {
            if line.starts_with(|c: char| c.is_ascii_lowercase()) {
                break;
            }
            body.push_str(line);
            body.push('\n');
        }
    }
    assert!(inside, "the justfile must still declare a `lint` recipe");
    assert!(
        body.contains("check-withdrawn-claim.sh"),
        "`just lint` is where this gate is enforced; removing it from the recipe \
         silently disarms G-SIGN's second half. The `lint` body was:\n{body}"
    );
}

// --------------------------------------------------------------- the red side
//
// A gate nobody has ever seen fail is indistinguishable from `exit 0`. The two
// tests below build a throwaway overlay in `std::env::temp_dir()`, plant the
// gate's OWN phrases into it, and require the gate to name the file and line.
// The phrases are read out of the script (see the module header) so this file
// never carries one.

/// A throwaway tree, deleted on drop, carrying only the gate and the probes.
struct Overlay {
    path: PathBuf,
}

impl Drop for Overlay {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

impl Overlay {
    fn new(tag: &str) -> Overlay {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a clock after 1970")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "logweir-withdrawn-claim-{tag}-{}-{stamp}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(path.join("scripts")).expect("the overlay is creatable");
        // The gate itself, at the path its own second exemption names — so the
        // overlay also exercises the exemption rather than only the failure.
        std::fs::copy(repo_root().join(GATE), path.join(GATE)).expect("copy the gate");
        Overlay { path }
    }

    fn plant(&self, rel: &str, contents: &str) {
        let p = self.path.join(rel);
        std::fs::create_dir_all(p.parent().expect("a parent directory"))
            .expect("the probe's directory is creatable");
        std::fs::write(&p, contents).expect("the probe is writable");
    }

    /// `LOGWEIR_ROOT` is passed explicitly, which is the overlay entry point
    /// the real gate offers and which nothing else exercises.
    fn run(&self) -> Output {
        run_gate(&self.path, Some(&self.path))
    }
}

/// Every surface the gate claims to walk is actually walked — and the exempt
/// copy of the gate, sitting in the same overlay and full of the phrases, is
/// NOT reported.
///
/// This is the test that kills "exempt `docs/` from the grep" in its other
/// form: dropping a directory from the surface list rather than adding an
/// exemption.
#[test]
fn the_gate_names_every_shipped_surface_it_walks() {
    let probe = phrases()[0].clone();
    let ov = Overlay::new("surfaces");
    let surfaces = [
        "README.md",
        "SECURITY.md",
        "CONTRIBUTING.md",
        "MAINTAINERS.md",
        "TRADEMARKS.md",
        "docs/keys.md",
        "docs/adr/0008-mvp-constraint-amendments.md",
        "config/logweir.yaml",
        "examples/demo.md",
        "ui/index.html",
        ".github/workflows/ci.yml",
        "scripts/check-no-oso.sh",
        "crates/weirkeeper/src/main.rs",
    ];
    for s in surfaces {
        ov.plant(s, &format!("// {probe}\n"));
    }
    let out = ov.run();
    let stderr = text(&out.stderr);
    assert!(
        !out.status.success(),
        "the gate must go red when a shipped surface restates it; status={:?}\nstdout:\n{}\nstderr:\n{stderr}",
        out.status.code(),
        text(&out.stdout)
    );
    for s in surfaces {
        assert!(
            stderr.contains(&format!("{s}:1")),
            "the gate must name `{s}:1` — a surface it claims to walk but does \
             not is worse than no gate. stderr was:\n{stderr}"
        );
    }
    assert!(
        !stderr.contains(&format!("{GATE}:")),
        "the gate must NOT report its own exempt copy: that file exists to \
         forbid the claim and has to quote it. stderr was:\n{stderr}"
    );
}

/// Every phrase on the list is enforced, not just the first one.
///
/// A list whose fourth entry never matched anything would look exactly like a
/// list that worked.
#[test]
fn the_gate_enforces_every_phrase_on_its_own_list() {
    let all = phrases();
    let ov = Overlay::new("phrases");
    for (i, p) in all.iter().enumerate() {
        // Upper-cased: the gate is case-insensitive and nothing else proves it.
        ov.plant(&format!("docs/probe-{i}.md"), &p.to_uppercase());
    }
    let out = ov.run();
    let stderr = text(&out.stderr);
    assert!(
        !out.status.success(),
        "the gate must go red; status={:?}\nstdout:\n{}\nstderr:\n{stderr}",
        out.status.code(),
        text(&out.stdout)
    );
    for (i, p) in all.iter().enumerate() {
        assert!(
            stderr.contains(&format!("docs/probe-{i}.md:1")),
            "phrase {i} on the gate's own list matched nothing: a phrase that is \
             listed but not enforced is a phrase a reviewer thinks is covered. \
             The unenforced entry has {} characters. stderr was:\n{stderr}",
            p.len()
        );
    }
}
