//! **I29 over the Phase B demo.** Task 24.
//!
//! `crates/logweir/tests/support/exit_code_lint.rs` is Task 12's reusable
//! tokeniser for STANDING RULE 20 — *exit codes are read directly, never
//! through a pipe* — and the controller's ruling at this slot is that it
//! audits **this task's `k8s-demo` recipe and the script it names**. This file
//! is that audit, and it is a `crates/logweir/tests/` file rather than a
//! `crates/weirkeeper/tests/` one for one mechanical reason: `mod support;`
//! reaches a sibling module inside the SAME test target, and no test binary in
//! another package can see it.
//!
//! # Why the rule matters more here than anywhere else in the tree
//!
//! `scripts/k8s-demo.sh` is Phase B's exit criterion. Almost every line in it
//! is a `kubectl` or a `docker` whose status is the whole information — "did
//! the `Backup` reach exit 0", "did the controller roll out" — and a single
//! `kubectl … | grep …` would report **grep's** status instead, turning a
//! failed step into a green demo. That is the exact failure mode the rule
//! exists for, and a demo that reports success it did not have is worse than
//! no demo.

mod support;

use std::path::{Path, PathBuf};

use support::exit_code_lint;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the workspace root resolves")
}

fn read(rel: &str) -> String {
    let p: PathBuf = root().join(rel);
    std::fs::read_to_string(&p).unwrap_or_else(|e| {
        panic!(
            "{} must exist for this test to mean anything: {e}",
            p.display()
        )
    })
}

/// The `k8s-demo` recipe body, from its recipe line to the end of the file.
///
/// It is the LAST recipe in the justfile by STANDING RULE 17 — chain J, slot
/// 17, appended at the end — so "to the end of the file" is exact rather than
/// convenient, and a later task appending after it will find this helper
/// reading two recipes and can narrow it then.
fn k8s_demo_recipe(justfile: &str) -> String {
    let at = justfile
        .find("\nk8s-demo:")
        .expect("`just k8s-demo` is a recipe in the justfile");
    justfile[at + 1..].to_string()
}

/// **STANDING RULE 20, over the demo script.**
///
/// KILLS: any rewrite that pipes a `kubectl`/`docker`/`just`/`logweir` line
/// whose status is load-bearing, or that drops the `rc=$?` after one.
#[test]
fn the_k8s_demo_masks_no_exit_code() {
    let script = read("scripts/k8s-demo.sh");

    // THE SELF-CHECK FIRST. A lint over a file it found no guarded lines in
    // passes vacuously — and would keep passing if the script were rewritten
    // to spell `kubectl` as `"$KUBECTL"`, which is exactly the change that
    // makes the rest of this test meaningless. The number is a floor: the demo
    // runs `kubectl` more than twenty times, `docker` several, and `just`
    // four.
    let guarded = exit_code_lint::logical_lines(&script)
        .into_iter()
        .filter(|l| l.is_guarded())
        .count();
    assert!(
        guarded >= 20,
        "the tokeniser found only {guarded} guarded line(s) in scripts/k8s-demo.sh — a lint that \
         sees nothing cannot fail. Are the tools still invoked as the bare words `kubectl`, \
         `docker` and `just`?"
    );

    exit_code_lint::assert_no_masked_exit_code("scripts/k8s-demo.sh", &script);
}

/// **The recipe itself**, which is two lines and has to obey the same rule.
#[test]
fn the_k8s_demo_recipe_masks_no_exit_code() {
    let justfile = read("justfile");
    let recipe = k8s_demo_recipe(&justfile);
    exit_code_lint::assert_no_masked_exit_code("justfile (k8s-demo)", &recipe);
    assert!(
        recipe.contains("./scripts/k8s-demo.sh"),
        "the recipe runs the script this file lints; got:\n{recipe}"
    );
    assert!(
        recipe.contains("cargo build -p logweir"),
        "plan erratum E9: the script shells `logweir drill approve` from target/debug/logweir, \
         and nothing else in the recipe builds it"
    );
}

/// The script is executable and parses as bash.
///
/// `bash -n` is not run here (a `#[test]` that shells out belongs in a recipe,
/// Global Constraint 22) — this is the cheap half: the file exists, it is a
/// bash script by its own shebang, and it is marked executable, because
/// `just k8s-demo` invokes it as `./scripts/k8s-demo.sh` and a non-executable
/// file fails with a permission error that says nothing about the demo.
#[test]
fn the_k8s_demo_script_is_an_executable_bash_script() {
    let p = root().join("scripts/k8s-demo.sh");
    let body = read("scripts/k8s-demo.sh");
    assert!(
        body.starts_with("#!/usr/bin/env bash"),
        "the script declares bash; got {:?}",
        body.lines().next()
    );
    assert!(
        is_executable(&p),
        "{} must be executable — `just k8s-demo` runs it as `./scripts/k8s-demo.sh`",
        p.display()
    );
}

#[cfg(unix)]
fn is_executable(p: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::metadata(p).is_ok_and(|m| m.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
fn is_executable(_p: &Path) -> bool {
    true
}
