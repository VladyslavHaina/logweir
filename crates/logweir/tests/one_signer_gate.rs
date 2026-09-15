//! G2′ — the link-time single-signer gate, and the eight tests that keep it a
//! gate rather than a comment. Task 7 (Phase 1 line item 1c); the last three
//! are Task 14's, which extracted `crates/logweir-verify`, re-scoped check 2
//! and added check 4.
//!
//! `scripts/check-one-signer.sh` proves narrow, mechanical LINKAGE properties:
//! the set of workspace crates from which `logweir-evidence` is reachable over
//! the dependency graph is exactly `{logweir, e2e}`, and the set from which
//! `logweir-verify` is reachable is on its own separate allowlist. It bounds
//! nobody's CAPABILITY to sign — the scorecard signing key is a Kubernetes
//! Secret, and `create pods` in its namespace is equivalent to holding it
//! (Global Constraint 27, residual **O1**, accepted). The stronger claim was
//! withdrawn once already; the script says so in its own header,
//! `scripts/check-withdrawn-claim.sh` keeps it off every shipped surface, and
//! these tests do not assert more than the script does.
//!
//! Why a Rust test at all, when the script is the gate: because the script's
//! RED side is the half that decays. A gate nobody has ever seen fail is
//! indistinguishable from `exit 0`, so three of the five tests build a throwaway
//! workspace overlay in `std::env::temp_dir()`, drop a probe crate into it that
//! reaches the signer, and require the script to name that crate on stderr.
//! The overlay — rather than a probe written into `crates/`, which the
//! `crates/*` glob in `Cargo.toml` would make a workspace member — is what
//! keeps `Cargo.lock` and the tracked tree byte-identical across a test run.
//!
//! `crates/logweir/tests/extract_engine.rs` is the precedent for a test in this
//! package that shells a repository script.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// The repository root, from this package's manifest directory.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("the repository root resolves from CARGO_MANIFEST_DIR")
}

/// Run the gate. `root` is the working directory; `logweir_root`, when given,
/// is exported as `LOGWEIR_ROOT` so the script walks an overlay instead of the
/// real tree.
///
/// The exit status is read from the child process directly — never from a
/// pipeline and never from `$?` after one.
fn run_gate(script: &Path, cwd: &Path, logweir_root: Option<&Path>) -> Output {
    let mut cmd = Command::new("bash");
    cmd.arg(script).current_dir(cwd);
    if let Some(r) = logweir_root {
        cmd.env("LOGWEIR_ROOT", r);
    }
    cmd.output().expect("the gate script can be spawned")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

#[test]
fn one_signer_script_is_green_on_this_tree() {
    let root = repo_root();
    let out = run_gate(Path::new("scripts/check-one-signer.sh"), &root, None);
    assert!(
        out.status.success(),
        "check-one-signer.sh must be green on the unmodified tree; status={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        text(&out.stdout),
        text(&out.stderr)
    );
}

// --------------------------------------------------------------- the overlay

/// A throwaway copy of the workspace, deleted on drop.
///
/// It carries only what the three checks read: the manifests, the lock, the
/// toolchain pin, `.cargo/`, every source root, and the script itself. It
/// deliberately omits `target/`, `.git/`, `.engine/`, `e2e/fixtures/` and
/// `e2e/compose/` — so the copy is cheap, and so the script is exercised
/// against a tree where some source roots are simply absent.
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
        let root = repo_root();
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a clock after 1970")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "logweir-one-signer-{tag}-{}-{stamp}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(path.join("e2e")).expect("the overlay directory is creatable");

        for f in ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml"] {
            std::fs::copy(root.join(f), path.join(f)).unwrap_or_else(|e| panic!("copy {f}: {e}"));
        }
        for d in [".cargo", "crates", "xtask"] {
            copy_tree(&root.join(d), &path.join(d));
        }
        // The `e2e` package needs its manifest AND a target (`src/lib.rs`), or
        // `cargo metadata` refuses the manifest outright.
        std::fs::copy(root.join("e2e/Cargo.toml"), path.join("e2e/Cargo.toml"))
            .expect("copy e2e/Cargo.toml");
        copy_tree(&root.join("e2e/src"), &path.join("e2e/src"));
        copy_tree(&root.join("e2e/tests"), &path.join("e2e/tests"));
        std::fs::create_dir_all(path.join("scripts")).expect("scripts/ is creatable");
        std::fs::copy(
            root.join("scripts/check-one-signer.sh"),
            path.join("scripts/check-one-signer.sh"),
        )
        .expect("copy the gate script into the overlay");
        // Task 32: check 2's primitive set is DERIVED from the graph minus this
        // classification, and an unclassified direct dependency of
        // `logweir-evidence` is fail-closed to `primitive` and exits 1. The
        // overlay must therefore carry the gate's INPUT as well as the gate, or
        // every test here would fail for a reason none of them is about.
        std::fs::copy(
            root.join("scripts/logweir-evidence-primitives.classify"),
            path.join("scripts/logweir-evidence-primitives.classify"),
        )
        .expect("copy the primitive classification into the overlay");

        Overlay { path }
    }

    /// Write a probe crate under `crates/`, which the `crates/*` glob in
    /// `Cargo.toml` makes a workspace member with no manifest edit.
    ///
    /// `lib_src` is the probe's whole source file. Empty exercises the graph
    /// walk alone; a `use` of the signing API additionally exercises the
    /// source grep, which is the only thing that catches a grep loosened until
    /// it matches nothing.
    ///
    /// `name` is a parameter for one reason: a probe named `zz-…` cannot
    /// detect an allowlist widened to a `logweir*` wildcard, because a `zz-`
    /// name is not what such a wildcard swallows. The transitive test
    /// therefore plants one of each.
    fn probe(&self, name: &str, dep_name: &str, dep_path: &str, lib_src: &str) {
        self.probe_raw(
            name,
            &format!("{dep_name} = {{ path = \"{dep_path}\" }}"),
            lib_src,
        );
    }

    /// The same, with the dependency written out in full — so a probe can take
    /// a REGISTRY dependency (on a signing primitive) rather than a path
    /// dependency on a workspace crate.
    fn probe_raw(&self, name: &str, dep_line: &str, lib_src: &str) {
        let dir = self.path.join("crates").join(name);
        std::fs::create_dir_all(dir.join("src")).expect("the probe directory is creatable");
        std::fs::write(
            dir.join("Cargo.toml"),
            format!(
                "[package]\n\
                 name = \"{name}\"\n\
                 version = \"0.0.0\"\n\
                 edition = \"2021\"\n\
                 publish = false\n\
                 \n\
                 [dependencies]\n\
                 {dep_line}\n"
            ),
        )
        .expect("the probe manifest is writable");
        std::fs::write(dir.join("src/lib.rs"), lib_src).expect("the probe lib.rs is writable");
    }

    fn manifest_of(&self, name: &str) -> String {
        std::fs::read_to_string(self.path.join("crates").join(name).join("Cargo.toml"))
            .expect("the probe manifest is readable")
    }

    fn run_gate(&self) -> Output {
        run_gate(
            Path::new("scripts/check-one-signer.sh"),
            &self.path,
            Some(&self.path),
        )
    }
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap_or_else(|e| panic!("mkdir {}: {e}", to.display()));
    for entry in std::fs::read_dir(from).unwrap_or_else(|e| panic!("read {}: {e}", from.display()))
    {
        let entry = entry.expect("a readable directory entry");
        let src = entry.path();
        let dst = to.join(entry.file_name());
        let ty = entry.file_type().expect("a readable file type");
        if ty.is_dir() {
            copy_tree(&src, &dst);
        } else if ty.is_file() {
            std::fs::copy(&src, &dst).unwrap_or_else(|e| panic!("copy {}: {e}", src.display()));
        }
        // Symlinks are skipped: nothing the three checks read is one.
    }
}

/// Phase-1 demonstrable 5: a throwaway crate that depends on
/// `logweir-evidence` turns the gate red.
///
/// The probe both LINKS the signer and NAMES it, so this one test covers the
/// red side of two of the three checks — and the third assertion is what a
/// grep loosened until it matches nothing has to get past.
#[test]
fn one_signer_script_detects_a_new_signer() {
    let ov = Overlay::new("direct");
    ov.probe(
        "zz-one-signer-probe",
        "logweir-evidence",
        "../logweir-evidence",
        "use logweir_evidence::keys::SigningKey;\n\
         pub fn probe(k: &SigningKey) -> String {\n    \
             k.key_id()\n\
         }\n",
    );
    let out = ov.run_gate();
    let stderr = text(&out.stderr);
    assert!(
        !out.status.success(),
        "the gate must go red when a new crate links the signer; status={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        text(&out.stdout),
        stderr
    );
    assert!(
        stderr.contains("zz-one-signer-probe"),
        "stderr must name the offending crate; stderr was:\n{stderr}"
    );
    assert!(
        stderr.contains("zz-one-signer-probe links the signer and is not on the allowlist"),
        "check 1 — the reverse-dependency walk — must report the probe by name; stderr was:\n{stderr}"
    );
    assert!(
        stderr.contains("zz-one-signer-probe names the signing API in source"),
        "the source grep must also report the probe by name — a grep loosened until it \
         matches nothing is a check that cannot fail; stderr was:\n{stderr}"
    );
}

/// The same overlay, but no probe manifest mentions `logweir-evidence` — each
/// depends on `logweir`, which has a `[lib]`. This is the test that makes
/// check 1 a transitive graph walk rather than a manifest grep.
///
/// TWO PROBES, and the second one's name is the whole reason it exists. A
/// `zz-`-prefixed name cannot detect an allowlist widened to a `logweir*`
/// wildcard — the cheapest way out for whoever hits this gate first — because
/// no wildcard of that shape swallows a `zz-` name. `logweir-one-signer-probe`
/// is the name such a widening would swallow, and it is checked against
/// check 1's OWN message: the other two checks would still report the probe
/// for their own reasons, and a test satisfied by any red at all cannot tell
/// which check is still working.
#[test]
fn one_signer_script_detects_a_transitive_signer() {
    const PROBES: [&str; 2] = ["zz-one-signer-probe", "logweir-one-signer-probe"];

    let ov = Overlay::new("transitive");
    for p in PROBES {
        ov.probe(p, "logweir", "../logweir", "");
        let manifest = ov.manifest_of(p);
        assert!(
            !manifest.contains("logweir-evidence"),
            "the transitive probe {p}'s manifest must never name the signer; it was:\n{manifest}"
        );
    }
    let out = ov.run_gate();
    let stderr = text(&out.stderr);
    assert!(
        !out.status.success(),
        "the gate must go red when a new crate reaches the signer transitively; status={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        text(&out.stdout),
        stderr
    );
    for p in PROBES {
        assert!(
            stderr.contains(&format!("{p} links the signer and is not on the allowlist")),
            "check 1 must report {p} by name: its manifest names only `logweir`, so a manifest \
             grep would not see it, and an allowlist widened to a `logweir*` wildcard would \
             swallow it. stderr was:\n{stderr}"
        );
    }
}

/// The red side of check 2 — the leg that had none until fix round 1, where
/// the reviewer deleted the whole primitives block and all four tests still
/// passed.
///
/// This probe depends on `p256` **directly**, from the registry, and on
/// nothing else. That is a backdoor the other two legs cannot see: it never
/// reaches `logweir-evidence`, so check 1 stays green (asserted below, so the
/// test cannot start passing for the wrong reason), and it names nothing in
/// source, so check 3 stays green. Only the primitive walk can catch it — a
/// crate that reimplements ECDSA P-256 signing against the same primitive the
/// evidence crate uses, without ever linking the evidence crate.
///
/// The `"0.13"` requirement is the one `crates/logweir-evidence/Cargo.toml`
/// carries; it resolves to the version already in `Cargo.lock`, so the overlay
/// needs no network.
#[test]
fn one_signer_script_detects_a_direct_primitive_dependency() {
    let ov = Overlay::new("primitive");
    ov.probe_raw("zz-one-signer-primitive-probe", "p256 = \"0.13\"", "");
    let out = ov.run_gate();
    let stderr = text(&out.stderr);
    let stdout = text(&out.stdout);
    assert!(
        !out.status.success(),
        "the gate must go red when a crate takes a signing primitive directly; status={:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        out.status.code()
    );
    assert!(
        stderr.contains(
            "zz-one-signer-primitive-probe reaches the signing primitive p256 over a normal \
             edge and is not on the allowlist"
        ),
        "check 2 must report the probe by its own message; stderr was:\n{stderr}"
    );
    // Check 1 is GREEN here, and that is the point: if this ever starts
    // failing, the probe has begun reaching the signer and this test would be
    // proving check 1 rather than check 2.
    assert!(
        stdout.contains("ok: the crates reaching logweir-evidence are exactly {e2e,logweir}"),
        "check 1 must stay green for this probe — otherwise this test no longer isolates \
         check 2; stdout was:\n{stdout}"
    );
    // A check that FAILED must not also print its `ok:` line in the same run.
    assert!(
        !stdout.contains("ok: p256 reaches"),
        "check 2 printed an `ok:` line for the primitive it just failed on; stdout was:\n{stdout}"
    );
}

/// `just lint` is the Phase-1 gate (line item 1g): `ci.yml` mirrors the gate
/// (green since 2026-09-12), and membership in this recipe is what makes
/// G2′ enforced on a laptop before a push.
///
/// It asserts membership and nothing else — not the recipe's length, not its
/// line numbers, not the absence of other arms — so a later task adding a
/// guard to `lint` cannot turn it red.
#[test]
fn just_lint_runs_the_one_signer_gate() {
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
        body.contains("check-one-signer.sh"),
        "`just lint` is the Phase-1 gate (line item 1g); removing the recipe from it silently \
         disarms G2′. The `lint` body was:\n{body}"
    );
}

// ------------------------------------------------- Task 14: check 2 re-scoped
//
// `logweir-verify` must depend on `p256` AND `ed25519-dalek` — `VerifyingKey`
// is an enum over them and `verify_detached` matches both arms — so the
// inverted walk at check 2 gains it and check 2 could not pass unamended.
// G-SIGN is `[EDIT-DERIVED]`, so STANDING RULE 21 binds: the widening had to be
// RECORDED rather than performed quietly, because a quiet two-name addition
// would retire the check for `weirkeeper` for good. These two tests are what
// make "recorded" mean something.

/// The whole `#`-comment header at the top of a script, as prose: `# ` prefixes
/// stripped and every run of whitespace collapsed to one space, so an assertion
/// can name a sentence without depending on where the author wrapped it.
fn comment_prose(script: &str) -> String {
    let mut out = String::new();
    for line in script.lines() {
        if line.starts_with("#!") {
            continue;
        }
        let Some(rest) = line.strip_prefix('#') else {
            break;
        };
        out.push(' ');
        out.push_str(rest.trim());
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The value of a `NAME="a b c"` shell assignment, split on whitespace.
fn allowlist(script: &str, name: &str) -> Vec<String> {
    let prefix = format!("{name}=\"");
    let line = script
        .lines()
        .find(|l| l.starts_with(&prefix))
        .unwrap_or_else(|| panic!("scripts/check-one-signer.sh must assign {name}"));
    let value = line[prefix.len()..]
        .strip_suffix('"')
        .unwrap_or_else(|| panic!("{name}'s assignment must be a single double-quoted word list"));
    value.split_whitespace().map(|s| s.to_string()).collect()
}

fn one_signer_source() -> String {
    std::fs::read_to_string(repo_root().join("scripts/check-one-signer.sh"))
        .expect("scripts/check-one-signer.sh is readable")
}

/// Check 2 says what it now claims, and the reason for the re-scope is in the
/// script itself rather than only in a commit message.
///
/// The mutant this kills is not "someone widened the allowlist" — the widening
/// is REQUIRED and correct. It is "someone widened the allowlist without
/// writing down why", and "someone widened it by one name too many".
#[test]
fn check_two_states_the_narrowed_claim() {
    let src = one_signer_source();
    let prose = comment_prose(&src);

    // (1) the re-scope paragraph, naming both primitives and saying in as many
    //     words that the old reading of check 2 has stopped being true.
    assert!(
        prose.contains("p256") && prose.contains("ed25519-dalek"),
        "the header must name both primitive crates in the re-scope paragraph; \
         header prose was:\n{prose}"
    );
    assert!(
        prose.contains("\"reaches a signing primitive\" is no longer a proxy for \"can sign\""),
        "the header must state, in as many words, that reaching a signing \
         primitive has stopped being a proxy for being able to sign — that \
         sentence is the whole justification for the widening (STANDING RULE \
         21). Header prose was:\n{prose}"
    );
    assert!(
        prose.contains("THESE FOUR CRATES AND NO OTHERS REACH THE PRIMITIVE CRATES"),
        "the header must state the narrowed claim check 2 now makes; header \
         prose was:\n{prose}"
    );
    assert!(
        prose.contains("THE FOURTH ALLOWLIST DOES NOT WEAKEN CHECKS 1 AND 3"),
        "the header must say, once, that the fourth allowlist does not weaken \
         checks 1 and 3; header prose was:\n{prose}"
    );

    // (2) exactly the four names, in that order, and no fifth.
    let primitive = allowlist(&src, "ALLOWED_PRIMITIVE");
    assert_eq!(
        primitive,
        vec![
            "logweir-evidence".to_string(),
            "logweir".to_string(),
            "logweir-verify".to_string(),
            "weirkeeper".to_string(),
        ],
        "ALLOWED_PRIMITIVE must be exactly those four names in that order and no \
         fifth. A fifth name is how this check quietly stops covering a crate \
         that has no business reaching a signing primitive."
    );

    // (3) check 2's own heading and `ok:` line point at checks 1 and 3 for the
    //     signing half — so nobody reads a green check 2 as "nothing else can
    //     sign".
    let heading = src
        .lines()
        .find(|l| l.contains("echo \"== cargo tree:"))
        .expect("check 2 still prints a `== cargo tree:` heading");
    assert!(
        heading.contains("checks 1 and 3"),
        "check 2's heading must point at checks 1 and 3 for the signing half; it was:\n{heading}"
    );
    assert!(
        heading.contains("$ALLOWED_PRIMITIVE"),
        "check 2's heading must name the allowlist it is actually testing; it was:\n{heading}"
    );
    let ok_line = src
        .lines()
        .find(|l| l.contains("echo \"ok: $prim reaches"))
        .expect("check 2 still prints an `ok: $prim reaches` line");
    assert!(
        ok_line.contains("checks 1 and 3"),
        "check 2's `ok:` line must point at checks 1 and 3 for the signing half; it was:\n{ok_line}"
    );
}

/// The two allowlists the extraction did NOT touch, asserted byte-for-byte.
///
/// Renamed from a first draft that said "three": `ALLOWED_PRIMITIVE` is exactly
/// the one that HAD to change, so asserting it unchanged was an impossible
/// property. These two did not change, `weirkeeper` is on neither, and the
/// expected bytes are written out here so the assertion is a byte comparison
/// against `fdc73a5` rather than a description of one.
#[test]
fn the_two_original_allowlists_are_unchanged() {
    let src = one_signer_source();
    for expected in [
        "ALLOWED_LINK=\"logweir e2e\"",
        "ALLOWED_SOURCE=\"logweir-evidence logweir e2e\"",
    ] {
        assert!(
            src.lines().any(|l| l == expected),
            "scripts/check-one-signer.sh must still carry the line `{expected}` \
             byte-for-byte as it stood at fdc73a5. The verify-only extraction \
             re-scoped check 2 and added a FOURTH allowlist; it did not touch \
             these two."
        );
    }
    for name in ["ALLOWED_LINK", "ALLOWED_SOURCE"] {
        assert!(
            !allowlist(&src, name).iter().any(|c| c == "weirkeeper"),
            "`weirkeeper` must be absent from {name}: it links the VERIFYING \
             crate and never the signer (spec §8, §10 G-SIGN)"
        );
    }
    // The fourth allowlist exists and is its own list, not an extension of
    // either of those two.
    assert_eq!(
        allowlist(&src, "ALLOWED_VERIFY_LINK"),
        vec![
            "logweir-evidence".to_string(),
            "logweir".to_string(),
            "weirkeeper".to_string(),
            "e2e".to_string(),
        ],
        "ALLOWED_VERIFY_LINK is the fourth, separate allowlist for the crates \
         permitted to link `logweir-verify`"
    );
}

/// The pure layer is four crates now, and the CI job that proves it says so in
/// both places.
///
/// Global Constraint 1 names `crates/logweir-store` and `crates/weirkeeper` as
/// outside the pure layer "by ADR, never by a quiet edit to the grep", and
/// `logweir-verify` as inside it. Both halves are asserted: the four that must
/// be there, and `logweir-store`, which must not.
#[test]
fn the_pure_layer_loop_covers_logweir_verify() {
    let yml = std::fs::read_to_string(repo_root().join(".github/workflows/no-oso.yml"))
        .expect(".github/workflows/no-oso.yml is readable");
    let expected: std::collections::BTreeSet<String> = [
        "logweir-core",
        "logweir-evidence",
        "logweir-kafka",
        "logweir-verify",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();

    // (a) the one-crate-per-line `--no-default-features` build list.
    let singles: std::collections::BTreeSet<String> = yml
        .lines()
        .map(str::trim)
        .filter(|l| l.starts_with("cargo build -p ") && l.ends_with("--no-default-features"))
        .filter_map(|l| l.split_whitespace().nth(3).map(|s| s.to_string()))
        .collect();
    assert_eq!(
        singles, expected,
        "the per-crate `--no-default-features` build list must be exactly the \
         four pure crates"
    );

    // (b) the combined build, on one line.
    let combined = yml
        .lines()
        .map(str::trim)
        .find(|l| {
            l.contains("cargo build -p ")
                && l.contains("--no-default-features")
                && l.matches("-p ").count() > 1
        })
        .expect("no-oso.yml still carries the combined --no-default-features build");
    let combined_names: std::collections::BTreeSet<String> = combined
        .split_whitespace()
        .collect::<Vec<_>>()
        .windows(2)
        .filter(|w| w[0] == "-p")
        .map(|w| w[1].to_string())
        .collect();
    assert_eq!(
        combined_names, expected,
        "the combined build must name the same four crates; the line was:\n{combined}"
    );

    // (c) the forbidden-dependency loop.
    let loop_line = yml
        .lines()
        .map(str::trim)
        .find(|l| l.starts_with("for c in "))
        .expect("no-oso.yml still carries the forbidden-dependency `for c in` loop");
    let loop_names: std::collections::BTreeSet<String> = loop_line
        .trim_start_matches("for c in ")
        .split(';')
        .next()
        .expect("the loop line has a `;`")
        .split_whitespace()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(
        loop_names, expected,
        "the forbidden-dependency loop must cover the same four crates: a crate \
         that is BUILT with --no-default-features but never grepped is half a \
         gate. The line was:\n{loop_line}"
    );

    // (d) the asymmetry Task 13 and Task 14 exist to record.
    for set in [&singles, &combined_names, &loop_names] {
        assert!(
            !set.contains("logweir-store"),
            "`logweir-store` is outside the pure layer BY ADR (docs/architecture.md §E; Global Constraint 1) — it \
             takes object_store with the aws feature and would fail the grep by \
             construction. Adding it here is the quiet edit GC1 forbids."
        );
    }
}
