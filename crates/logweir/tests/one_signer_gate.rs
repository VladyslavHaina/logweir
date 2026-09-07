//! G2′ — the link-time single-signer gate, and the five tests that keep it a
//! gate rather than a comment. Task 7 (Phase 1 line item 1c).
//!
//! `scripts/check-one-signer.sh` proves ONE narrow, mechanical property: the
//! set of workspace crates from which `logweir-evidence` is reachable over the
//! dependency graph is exactly `{logweir, e2e}`. It does **not** prove that the
//! control plane cannot sign — the scorecard signing key is a Kubernetes
//! Secret, and `create pods` in its namespace is equivalent to holding it. The
//! stronger claim was withdrawn once already; the script says so in its own
//! header and these tests do not assert more than it does.
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

/// `just lint` is the Phase-1 gate (line item 1g): `ci.yml` has never executed
/// on any commit, so membership in this recipe is the only thing that makes
/// G2′ enforced rather than asserted.
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
