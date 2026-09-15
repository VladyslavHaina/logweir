//! Shared quality entrypoint and behavioral tests for development guard tools.

mod support;

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use support::dial_tokens;
use support::exit_code_lint::{self, Finding};

// ---------------------------------------------------------------- plumbing

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the repository root resolves")
}

fn read(rel: &str) -> String {
    let p = repo_root().join(rel);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

fn justfile() -> String {
    read("justfile")
}

/// A recipe's header line: `name:`, `name: dep`, `name arg="x":`.
fn is_recipe_header(line: &str, name: &str) -> bool {
    if line.starts_with(char::is_whitespace) {
        return false;
    }
    let Some(rest) = line.strip_prefix(name) else {
        return false;
    };
    matches!(rest.chars().next(), Some(':') | Some(' ')) && rest.contains(':')
}

fn recipe_body(just: &str, name: &str) -> Vec<String> {
    let mut lines = just.lines();
    lines
        .by_ref()
        .find(|l| is_recipe_header(l, name))
        .unwrap_or_else(|| panic!("the justfile must declare a `{name}` recipe"));
    lines
        .take_while(|l| l.starts_with(' ') || l.starts_with('\t') || l.trim().is_empty())
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#') && !l.starts_with("#!"))
        .map(|l| l.to_string())
        .collect()
}

#[test]
fn gate_lint_no_masked_exit_codes() {
    let mut corpus: Vec<(String, String)> = vec![("justfile".to_string(), justfile())];
    for entry in std::fs::read_dir(repo_root().join("scripts")).expect("scripts/ is readable") {
        let entry = entry.expect("a readable entry");
        if !entry.file_type().expect("a readable file type").is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        let rel = format!("scripts/{name}");
        corpus.push((rel.clone(), read(&rel)));
    }
    assert!(
        corpus.len() >= 20,
        "only {} files in the corpus — this lint is reading the wrong directory",
        corpus.len()
    );

    // THE SELF-CHECK FIRST. A lint over a corpus it found no guarded lines in
    // passes vacuously, and would keep passing if every tool were spelled
    // `"$DOCKER"`.
    let guarded: usize = corpus
        .iter()
        .map(|(_, text)| {
            exit_code_lint::logical_lines(text)
                .into_iter()
                .filter(exit_code_lint::LogicalLine::is_guarded)
                .count()
        })
        .sum();
    assert!(
        guarded >= 50,
        "the tokeniser found only {guarded} guarded line(s) across the justfile and scripts/ — \
         a lint that sees nothing cannot fail. Are `docker`, `kubectl`, `just`, `curl` and \
         `logweir` still invoked as bare first words?"
    );

    let mut piped: Vec<String> = Vec::new();
    for (label, text) in &corpus {
        for f in exit_code_lint::findings(text) {
            if matches!(f, Finding::PipedStatus { .. }) {
                piped.push(format!("{label}: {f}"));
            }
        }
    }
    assert!(
        piped.is_empty(),
        "{} line(s) send a load-bearing exit code through a pipe (STANDING RULE 20, Global \
         Constraint 11). The pipe's status is reported, not the command's.\n\n{}",
        piped.len(),
        piped.join("\n\n")
    );
}

// The workflow and local entrypoint share one implementation. Behavior and
// generated-file checks belong in that implementation, not duplicated YAML.
#[test]
fn local_and_ci_use_the_same_quality_script() {
    assert_eq!(
        recipe_body(&justfile(), "gate"),
        ["bash scripts/ci-check.sh"]
    );
    let ci = read(".github/workflows/ci.yml");
    assert!(ci.lines().any(
        |line| !line.trim_start().starts_with('#') && line.contains("bash scripts/ci-check.sh")
    ));
    let script = read("scripts/ci-check.sh");
    assert!(script.contains("set -euo pipefail"));
    assert_eq!(script.matches("cargo test --locked --workspace").count(), 1);
    assert!(!script.contains("--release"));
    assert!(!script.contains("time-unit-suite.sh"));
}

#[test]
fn ci_uses_the_complete_compose_setup() {
    let ci = read(".github/workflows/ci.yml");
    assert!(ci
        .lines()
        .any(|line| !line.trim_start().starts_with('#') && line.contains("just e2e-up")));
    let setup = recipe_body(&justfile(), "e2e-up").join("\n");
    for required in ["--wait", "minio-setup", "topic-setup", "scram-setup"] {
        assert!(
            setup.contains(required),
            "compose setup is missing {required}"
        );
    }
}

/// **I32 is verified here, never rewritten** (Task 1's to close; Task 32 only
/// certifies that it is closed).
#[test]
fn gate_lint_the_engine_allowlists_are_two_constants() {
    let src = read("scripts/check-no-oso.sh");

    assert!(
        src.contains("ENGINE_RUNTIME_ALLOWLIST=\"backup restore validate-restore validation\""),
        "scripts/check-no-oso.sh must carry ENGINE_RUNTIME_ALLOWLIST verbatim — it is the GC3 \
         CONTRACT, the four subcommand tokens, and the failure text quotes it"
    );
    assert!(
        src.contains("ENGINE_ARGV_ALLOWLIST="),
        "scripts/check-no-oso.sh must carry a SEPARATE ENGINE_ARGV_ALLOWLIST. Two constants, not \
         one: the argv allowlist additionally admits `run` and the furniture every invocation \
         carries (`--config`, `--format`, `json`), and merging them would let a contract token \
         be confused with a flag."
    );
    assert!(
        src.contains("ROOT=\"${LOGWEIR_ROOT:-$(cd \"$(dirname \"$0\")/..\" && pwd)}\""),
        "scripts/check-no-oso.sh must resolve its root through LOGWEIR_ROOT, so a test can point \
         it at a temp-workspace overlay"
    );
    assert!(
        src.contains("if [ -z \"${LOGWEIR_ROOT:-}\" ]; then"),
        "the two cargo checks and the linkage grep must be enclosed in `if [ -z \
         \"${{LOGWEIR_ROOT:-}}\" ]` — they take the package-cache lock, and a #[test] that takes \
         it can hang behind a concurrent build (Global Constraint 22)"
    );
}

// ================================================ the derived primitive set

/// A throwaway workspace overlay, so the signer gate can be run against a tree
/// this test controls. Deleted on drop.
struct Overlay {
    path: PathBuf,
}

impl Drop for Overlay {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
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
            "logweir-gate-lint-{tag}-{}-{stamp}",
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
        std::fs::copy(root.join("e2e/Cargo.toml"), path.join("e2e/Cargo.toml"))
            .expect("copy e2e/Cargo.toml");
        copy_tree(&root.join("e2e/src"), &path.join("e2e/src"));
        copy_tree(&root.join("e2e/tests"), &path.join("e2e/tests"));
        std::fs::create_dir_all(path.join("scripts")).expect("scripts/ is creatable");
        for f in [
            "scripts/check-one-signer.sh",
            "scripts/logweir-evidence-primitives.classify",
        ] {
            std::fs::copy(root.join(f), path.join(f)).unwrap_or_else(|e| panic!("copy {f}: {e}"));
        }
        Overlay { path }
    }

    /// Give `logweir-evidence` a fourth path dependency — a crate nobody has
    /// classified.
    fn add_unclassified_dependency(&self, name: &str) {
        let probe = self.path.join("crates").join(name);
        std::fs::create_dir_all(probe.join("src")).expect("the probe directory is creatable");
        std::fs::write(
            probe.join("Cargo.toml"),
            format!(
                "[package]\nname = \"{name}\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\
                 publish = false\n\n[dependencies]\n"
            ),
        )
        .expect("the probe manifest is writable");
        std::fs::write(probe.join("src/lib.rs"), "").expect("the probe lib.rs is writable");

        let manifest = self.path.join("crates/logweir-evidence/Cargo.toml");
        let text = std::fs::read_to_string(&manifest).expect("the evidence manifest is readable");
        let patched = text.replacen(
            "[dependencies]",
            &format!("[dependencies]\n{name} = {{ path = \"../{name}\" }}"),
            1,
        );
        assert_ne!(
            text, patched,
            "the evidence manifest must have [dependencies]"
        );
        std::fs::write(&manifest, patched).expect("the evidence manifest is writable");
    }

    fn classify(&self, text: &str) {
        let p = self
            .path
            .join("scripts/logweir-evidence-primitives.classify");
        let mut body = std::fs::read_to_string(&p).expect("the classification is readable");
        body.push('\n');
        body.push_str(text);
        body.push('\n');
        std::fs::write(&p, body).expect("the classification is writable");
    }

    fn run_signer_gate(&self) -> Output {
        Command::new("bash")
            .arg("scripts/check-one-signer.sh")
            .current_dir(&self.path)
            .env("LOGWEIR_ROOT", &self.path)
            .output()
            .expect("the signer gate runs")
    }
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_string()
}

/// **The `PRIMITIVES` literal is gone and the set comes from the graph.**
///
/// The overlay gives `logweir-evidence` a fourth, unclassified path dependency.
/// Fail-closed means the gate treats it as a signing primitive and exits **1**
/// naming it — so a third signing crate is a diff a reviewer reads, not a silent
/// pass. Restoring `PRIMITIVES="p256 ed25519-dalek"` makes this test fail at
/// assertion time: the overlay's fourth crate is no longer named and the gate
/// exits 0 where 1 is required.
///
/// `cargo metadata --no-deps` over a path-only overlay performs no resolution
/// and reaches no network.
#[test]
fn one_signer_primitives_come_from_the_graph() {
    // The literal is gone from the source, stated separately so its return is
    // named as itself rather than diagnosed from a rc.
    let src = read("scripts/check-one-signer.sh");
    // CODE, not prose: the header explains why the literal is gone and quotes
    // it to do so, and a substring search over the whole file would read that
    // explanation as the defect it describes.
    let code: String = src
        .lines()
        .filter(|l| !l.trim_start().starts_with('#'))
        .collect::<Vec<&str>>()
        .join("\n");
    assert!(
        !code.contains("PRIMITIVES=\"p256 ed25519-dalek\""),
        "scripts/check-one-signer.sh has gone back to a fixed PRIMITIVES literal. A third signing \
         primitive added to logweir-evidence would then pass this gate in silence, which is the \
         carried defect Task 32 closed."
    );
    assert!(
        code.contains("PRIMITIVES_CLASSIFY=\"scripts/logweir-evidence-primitives.classify\""),
        "check 2 must derive its primitive set from the graph minus the checked-in classification"
    );

    let ov = Overlay::new("primitives");
    ov.add_unclassified_dependency("zz-probe-primitive");
    let out = ov.run_signer_gate();
    let combined = format!("{}{}", text(&out.stdout), text(&out.stderr));

    assert_eq!(
        out.status.code(),
        Some(1),
        "the gate must exit 1 on an unclassified direct dependency of logweir-evidence; it exited \
         {:?}\n{combined}",
        out.status.code()
    );
    assert!(
        combined.contains("zz-probe-primitive"),
        "the failure must NAME the unclassified crate — an exit code that does not say which \
         crate costs a bisect:\n{combined}"
    );
}

/// **The classification's own format is load-bearing: an entry without a reason
/// is refused.**
///
/// The mutant: classify the overlay's fourth crate as `non-primitive` and leave
/// the reason out. The gate must still exit 1 — a classification whose reason
/// nobody wrote is a widening nobody reviewed.
#[test]
fn one_signer_refuses_a_classification_without_a_reason() {
    let ov = Overlay::new("noreason");
    ov.add_unclassified_dependency("zz-probe-noreason");
    ov.classify("zz-probe-noreason non-primitive");

    let out = ov.run_signer_gate();
    let combined = format!("{}{}", text(&out.stdout), text(&out.stderr));
    assert_eq!(
        out.status.code(),
        Some(1),
        "an entry with no `reason:` line must be refused; the gate exited {:?}\n{combined}",
        out.status.code()
    );
    assert!(
        combined.contains("reason"),
        "the refusal must say the reason line is what is missing:\n{combined}"
    );

    // And with a reason, the same classification is accepted — otherwise this
    // test would pass against a gate that refuses every classification.
    let ov2 = Overlay::new("withreason");
    ov2.add_unclassified_dependency("zz-probe-withreason");
    ov2.classify("zz-probe-withreason non-primitive\nreason: a probe crate; it signs nothing.");
    let out2 = ov2.run_signer_gate();
    let combined2 = format!("{}{}", text(&out2.stdout), text(&out2.stderr));
    assert!(
        !combined2.contains("is NOT classified"),
        "a classified crate with a reason must be accepted:\n{combined2}"
    );
}

/// **The parity gate names the pinned toolchain before it builds.**
///
/// A source-reading test: the `RUSTUP_TOOLCHAIN` export precedes the build line,
/// and the build line is `cargo build --release -p logweir`. The execution half
/// is recorded once in the task report rather than run here — a release build
/// exceeds Global Constraint 22's 15 s per-test bound, and
/// `crates/logweir-core/tests/fixture_regen.rs` stays the only test in the
/// workspace that spawns a nested cargo.
#[test]
fn parity_gate_names_the_pinned_toolchain_before_building() {
    let src = read("scripts/check-verifier-parity.sh");

    let build = src
        .find("cargo build --release -p logweir")
        .expect("the parity gate must BUILD the binary it needs rather than refusing");
    let export = src
        .find("export RUSTUP_TOOLCHAIN=")
        .expect("the parity gate must export the pinned toolchain");
    assert!(
        export < build,
        "the RUSTUP_TOOLCHAIN export must precede the cargo build. A cargo reached through \
         rustup's shim with no override in scope resolves rustup's DEFAULT channel and SYNCS IT \
         FROM THE NETWORK, from inside a lint gate (STANDING RULE 7, Global Constraint 17)."
    );
    assert!(
        src[..export].contains("rust-toolchain.toml"),
        "the exported pin must be read from rust-toolchain.toml's `channel`, not invented"
    );
    // The interpreter chain above it is untouched, in its original order.
    let chain = [
        "if [ -n \"${LOGWEIR_PYTHON:-}\" ]; then",
        "elif [ -n \"${LOGWEIR_E2E_PYTHON:-}\" ]; then",
        "elif [ -x \"$ROOT/.e2e/venv/bin/python3\" ]; then",
    ];
    let mut at = 0usize;
    for step in chain {
        let i = src[at..]
            .find(step)
            .unwrap_or_else(|| panic!("the interpreter chain must still read `{step}`"));
        at += i + step.len();
    }
    assert!(
        at < export,
        "the interpreter chain must stay above the build block — it is untouched by this change"
    );
}

/// **The tokeniser, not the slice, is doing the work.**
///
/// Two fixtures over a copy of `no_network_in_unit_tests.rs`'s text: one where a
/// REAL token is moved into a `//` comment inside the array (the tokeniser must
/// not see it, so the agreement fails), and one where a harmless extra token
/// appears in a comment while all six real entries remain (it passes).
///
/// The first fixture is the mutant the old substring search survived.
#[test]
fn the_ureq_token_lists_agree_under_a_comment() {
    let audit = read("crates/logweir/tests/no_network_in_unit_tests.rs");
    let notify = read("crates/logweir/tests/notify.rs");

    // The six, read from notify.rs's own literal rather than restated here — a
    // second copy of the list is the defect this whole pair of tests is about.
    let forbidden = dial_tokens::array_elements(&notify, "const FORBIDDEN");
    assert_eq!(
        forbidden.len(),
        6,
        "notify.rs's FORBIDDEN must still be the six ureq entry points; parsed {forbidden:?}"
    );

    // The unmodified tree agrees.
    let base = dial_tokens::array_elements(&audit, "const DIAL_TOKENS");
    let missing: Vec<&String> = forbidden.iter().filter(|t| !base.contains(*t)).collect();
    assert!(
        missing.is_empty(),
        "the unmodified audit is already narrower than FORBIDDEN: {missing:?}"
    );

    // FIXTURE 1 — a REAL token moved into a `//` comment inside the array.
    //
    // The victim is taken from the parsed set rather than written out here.
    // Two reasons, and both are load-bearing: a literal copy of a dial token in
    // this file would have to be allow-listed in the very audit it is testing
    // (`no_network_in_unit_tests.rs` walks `crates/logweir/tests/`), and a
    // hardcoded victim goes stale the day the list changes while still passing.
    let victim = forbidden
        .iter()
        .next()
        .expect("FORBIDDEN is not empty")
        .clone();
    let quoted = format!("\"{victim}\",");
    assert!(
        audit.contains(&quoted),
        "the audit must still carry {quoted} as an element for this fixture to move it"
    );
    let commented = audit.replacen(&quoted, &format!("// moved here: {quoted}"), 1);
    let seen = dial_tokens::array_elements(&commented, "const DIAL_TOKENS");
    assert!(
        !seen.contains(&victim),
        "a token that appears only inside a `//` comment must NOT be read as an array element — \
         that is exactly what the old substring search could not tell apart, and it is the \
         mutant this test exists to kill. Moved {victim:?}; parsed: {seen:?}"
    );

    // FIXTURE 2 — an extra token in a comment, all six real entries intact.
    let extra = "a-token-that-is-only-prose";
    let decorated = audit.replacen(
        "const DIAL_TOKENS",
        &format!("// see also \"{extra}\" — not an element, just prose\nconst DIAL_TOKENS"),
        1,
    );
    let kept = dial_tokens::array_elements(&decorated, "const DIAL_TOKENS");
    let still_missing: Vec<&String> = forbidden.iter().filter(|t| !kept.contains(*t)).collect();
    assert!(
        still_missing.is_empty(),
        "a comment beside the array must not remove elements from it: {still_missing:?}"
    );
    assert!(
        !kept.contains(extra),
        "a token mentioned only in a comment must not become an element either: {kept:?}"
    );
}

// ============================================ the time-unit-suite fixtures

/// A throwaway tree that `scripts/time-unit-suite.sh` can be run in, with a
/// stub `cargo` (and optionally a stub `rustup`) first on `$PATH` — so the
/// harness's control flow is exercised without compiling anything.
struct SuiteFixture {
    path: PathBuf,
}

impl Drop for SuiteFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

impl SuiteFixture {
    fn new(tag: &str) -> SuiteFixture {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a clock after 1970")
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("logweir-tus-{tag}-{}-{stamp}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        for d in ["scripts", "bin", "tmp"] {
            std::fs::create_dir_all(path.join(d)).expect("the fixture directory is creatable");
        }
        std::fs::copy(
            repo_root().join("scripts/time-unit-suite.sh"),
            path.join("scripts/time-unit-suite.sh"),
        )
        .expect("copy the harness into the fixture");
        std::fs::write(
            path.join("Cargo.toml"),
            "[workspace.package]\nrust-version = \"1.89\"\n",
        )
        .expect("the fixture manifest is writable");
        let fixture = SuiteFixture { path };
        fixture.stub_nc();
        fixture
    }

    /// A stub `nc` that answers "closed" for every port.
    ///
    /// The harness refuses to time the suite while the broker port or the MinIO
    /// port answers on the loopback address (M10), and it asks `nc` — the real one, first on the host's
    /// `PATH`. These fixtures exercise the harness's RED PATHS, not the host:
    /// run under `just e2e` or `ci.yml`'s `e2e` job, where the compose stack is
    /// up by design, the real `nc` made all three red-path fixtures fail on the
    /// refusal instead (the fourth CI run, 2026-09-12). With the stub the
    /// fixtures are stack-agnostic; the refusal stays real for every real run,
    /// because a real run has no fixture `bin/` on its `PATH`.
    fn stub_nc(&self) {
        self.stub("nc", "#!/bin/sh\nexit 1\n");
    }

    fn with_toolchain_pin(self) -> SuiteFixture {
        std::fs::copy(
            repo_root().join("rust-toolchain.toml"),
            self.path.join("rust-toolchain.toml"),
        )
        .expect("copy rust-toolchain.toml into the fixture");
        self
    }

    fn stub(&self, name: &str, body: &str) {
        let p = self.path.join("bin").join(name);
        std::fs::write(&p, body).expect("the stub is writable");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755))
                .expect("the stub is executable");
        }
    }

    /// A stub `mktemp` that honours `$TMPDIR` even with no template.
    ///
    /// **Without this the log-leak check below is vacuous**, and that was
    /// measured rather than assumed: BSD `mktemp` (this is macOS) ignores
    /// `$TMPDIR` unless it is given a template or `-t`, and puts a bare
    /// `mktemp`'s file in `confstr(_CS_DARWIN_USER_TEMP_DIR)` instead — so a
    /// before/after listing of the fixture's own `$TMPDIR` stayed empty whether
    /// the harness removed its log or leaked it. A check that cannot fail is
    /// this build's signature defect; the stub is how this one can.
    fn stub_mktemp(&self) {
        let witness = self.path.join("mktemp-witness");
        self.stub(
            "mktemp",
            &format!(
                "#!/bin/sh\n\
                 d=\"${{TMPDIR:-/tmp}}\"\n\
                 if [ \"${{1:-}}\" = -d ]; then p=\"$d/tmpd.$$\"; mkdir -p \"$p\"; \
                 echo \"$p\" >> {w}; echo \"$p\"; exit 0; fi\n\
                 i=0\n\
                 while :; do p=\"$d/tmp.$$.$i\"; [ -e \"$p\" ] || break; i=$((i+1)); done\n\
                 : > \"$p\"\n\
                 echo \"$p\" >> {w}\n\
                 echo \"$p\"\n",
                w = witness.display()
            ),
        );
    }

    /// Every path the stub `mktemp` handed out, in order.
    fn mktemp_witness(&self) -> Vec<PathBuf> {
        let p = self.path.join("mktemp-witness");
        std::fs::read_to_string(&p)
            .unwrap_or_default()
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(PathBuf::from)
            .collect()
    }

    /// A stub `cargo`. `suite_rc` is what `cargo test --workspace` exits with;
    /// `slow` makes it take about two seconds, which is how a fixture exceeds a
    /// one-second budget without doing any work.
    fn stub_cargo(&self, suite_rc: i32, slow: bool) {
        let sleep = if slow { "sleep 2" } else { ":" };
        self.stub(
            "cargo",
            &format!(
                "#!/bin/sh\n\
                 # A stub. No compiler, no package cache, no network.\n\
                 case \"$*\" in\n\
                 \x20 *--no-run*) exit 0 ;;\n\
                 \x20 *build*) exit 0 ;;\n\
                 \x20 *\"test --workspace\"*)\n\
                 \x20   echo \"thread 'probe' panicked at crates/logweir/tests/probe.rs:7:1:\"\n\
                 \x20   echo boom\n\
                 \x20   {sleep}\n\
                 \x20   exit {suite_rc} ;;\n\
                 esac\n\
                 exit 0\n"
            ),
        );
    }

    fn run(&self, env: &[(&str, &str)]) -> Output {
        let path_var = format!(
            "{}:{}",
            self.path.join("bin").display(),
            std::env::var("PATH").unwrap_or_default()
        );
        let mut cmd = Command::new("bash");
        cmd.arg("scripts/time-unit-suite.sh")
            .current_dir(&self.path)
            .env("PATH", path_var)
            .env("TMPDIR", self.path.join("tmp"))
            .env_remove("LOGWEIR_TIME_BUDGET_SECS")
            .env_remove("LOGWEIR_UNIT_SUITE_BUDGET_SECS")
            .env_remove("LOGWEIR_UNIT_TEST_BUDGET_SECS");
        for (k, v) in env {
            cmd.env(k, v);
        }
        cmd.output().expect("the harness runs")
    }
}

/// **Every red carries the load average.** This plan runs up to three agents at
/// `CARGO_BUILD_JOBS=4`, and the same tree has measured 20-35 s idle and
/// 105-129 s under two concurrent builds. Without this line a load-induced red
/// and a regression are the same transcript.
#[test]
fn time_unit_suite_prints_load_on_a_red() {
    let fx = SuiteFixture::new("load").with_toolchain_pin();
    fx.stub_cargo(101, false);
    let out = fx.run(&[]);
    let combined = format!("{}{}", text(&out.stdout), text(&out.stderr));
    assert_ne!(
        out.status.code(),
        Some(0),
        "the fixture must be red:\n{combined}"
    );
    assert!(
        combined.contains("load average"),
        "a red run must print the load average:\n{combined}"
    );
}

/// **`LOGWEIR_TIME_BUDGET_SECS` is an alias for both budgets, and the explicit
/// variables win.** The resolved values are printed, with where each came from.
#[test]
fn time_unit_suite_honours_the_time_budget_alias() {
    // The alias alone: both budgets are 1, and a two-second suite is over it.
    let fx = SuiteFixture::new("alias").with_toolchain_pin();
    fx.stub_cargo(0, true);
    let out = fx.run(&[("LOGWEIR_TIME_BUDGET_SECS", "1")]);
    let combined = format!("{}{}", text(&out.stdout), text(&out.stderr));
    assert_eq!(
        out.status.code(),
        Some(1),
        "with the alias at 1s a two-second suite must FAIL; it exited {:?}\n{combined}",
        out.status.code()
    );
    assert!(
        combined.contains("suite budget    1s") && combined.contains("per-test budget 1s"),
        "both resolved budgets must be printed as 1s:\n{combined}"
    );
    assert!(
        combined.contains("LOGWEIR_TIME_BUDGET_SECS (alias)"),
        "the printed budget must name where it came from:\n{combined}"
    );

    // The explicit variable wins over the alias.
    let fx2 = SuiteFixture::new("alias-override").with_toolchain_pin();
    fx2.stub_cargo(0, false);
    let out2 = fx2.run(&[
        ("LOGWEIR_TIME_BUDGET_SECS", "1"),
        ("LOGWEIR_UNIT_SUITE_BUDGET_SECS", "600"),
    ]);
    let combined2 = format!("{}{}", text(&out2.stdout), text(&out2.stderr));
    assert!(
        combined2.contains("suite budget    600s (from LOGWEIR_UNIT_SUITE_BUDGET_SECS)"),
        "an explicit LOGWEIR_UNIT_SUITE_BUDGET_SECS must beat the alias:\n{combined2}"
    );
    assert!(
        combined2.contains("per-test budget 1s"),
        "the per-test budget must still come from the alias:\n{combined2}"
    );
}

/// **Item (b): the suite log is removed on EVERY path, including the red one.**
///
/// Source-reading — the trap naming `$suite_log` is installed before the
/// `$SUITE_CMD` line — plus a fixture red run that leaves no file behind in its
/// own `$TMPDIR`.
#[test]
fn time_unit_suite_removes_its_log_on_a_red() {
    let src = read("scripts/time-unit-suite.sh");
    let mktemp = src
        .find("suite_log=\"$(mktemp)\"")
        .expect("the harness must still capture the suite output to a temp file");
    let trap = src
        .find("trap 'rm -f \"$suite_log\"' EXIT")
        .expect("a trap naming $suite_log must be installed for it");
    let run = src
        .find("$SUITE_CMD > \"$suite_log\"")
        .expect("the harness must still run the suite into that file");
    assert!(
        mktemp < trap && trap < run,
        "the $suite_log trap must be installed immediately after the mktemp and BEFORE the suite \
         runs. Inside the `suite_rc -eq 0` branch it covers only the green path, and every red \
         run — the run an agent repeats most — leaks a temp file."
    );

    let fx = SuiteFixture::new("log").with_toolchain_pin();
    fx.stub_cargo(101, false);
    fx.stub_mktemp();
    let out = fx.run(&[]);
    let combined = format!("{}{}", text(&out.stdout), text(&out.stderr));
    assert_ne!(
        out.status.code(),
        Some(0),
        "the fixture must be red:\n{combined}"
    );

    // THE SELF-CHECK FIRST: the stub must have been the `mktemp` the harness
    // reached, or the survivor check below compares two empty sets for ever.
    let handed_out = fx.mktemp_witness();
    assert!(
        !handed_out.is_empty(),
        "the stub mktemp was never called, so this test proved nothing about the log's \
         lifetime. Is `$PATH` still pointing at the fixture's bin/?\n{combined}"
    );
    let survivors: Vec<&PathBuf> = handed_out.iter().filter(|p| p.exists()).collect();
    assert!(
        survivors.is_empty(),
        "a red run leaked {} of the {} temp file(s) it created: {survivors:?}\n\
         Every red run is a run an agent repeats; a trap that only covers the green path \
         leaks one file per repetition.\n{combined}",
        survivors.len(),
        handed_out.len()
    );
}

/// **Item (b), second half: the suite's output is streamed once.** The verdict
/// prints a REFERENCE into the output it already produced, not a second copy of
/// part of it.
#[test]
fn time_unit_suite_streams_the_suite_once_on_a_red() {
    let src = read("scripts/time-unit-suite.sh");
    // CODE ONLY, for the same reason as above: the comment beside the verdict
    // explains that the log was already streamed by the `cat` line, and a
    // count over prose would read that sentence as a second stream.
    let code: String = src
        .lines()
        .filter(|l| !l.trim_start().starts_with('#'))
        .collect::<Vec<&str>>()
        .join("\n");
    let streams = code.matches("cat \"$suite_log\"").count();
    assert_eq!(
        streams, 1,
        "the suite log must be streamed exactly once; found {streams} `cat \"$suite_log\"`"
    );
    assert!(
        !code.contains("p \"$suite_log\""),
        "the verdict must not re-print slices of the log with `sed -n …p \"$suite_log\"` — the \
         same bytes appearing twice in one transcript is how a reader ends up debugging the echo"
    );
    assert!(
        !code.contains("tail -n 5 \"$suite_log\""),
        "the verdict must not re-print the tail of the log either"
    );
    assert!(
        code.contains("first panic at suite-log line"),
        "the verdict must print a REFERENCE — `first panic at suite-log line <N>: <site>`"
    );

    // And it actually prints one.
    let fx = SuiteFixture::new("stream").with_toolchain_pin();
    fx.stub_cargo(101, false);
    let out = fx.run(&[]);
    let combined = format!("{}{}", text(&out.stdout), text(&out.stderr));
    assert!(
        combined.contains("first panic at suite-log line"),
        "a red run must name where the first panic is in the output it streamed:\n{combined}"
    );
    assert!(
        combined.contains("crates/logweir/tests/probe.rs:7:1"),
        "the reference must name the panic site the suite reported:\n{combined}"
    );
}

/// **Item (c): the MSRV fallback never exports a two-component channel.**
///
/// `Cargo.toml`'s `rust-version` is `"1.89"` — two components — and handing
/// rustup a channel it may not have installed makes rustup SYNC IT FROM THE
/// NETWORK, from inside a lint gate. The fallback resolves the highest
/// INSTALLED `1.89.x` instead, and exits 1 naming the floor when there is none.
#[test]
fn msrv_fallback_never_exports_a_two_component_toolchain() {
    let src = read("scripts/time-unit-suite.sh");
    let fallback_start = src
        .find("MSRV_FLOOR=")
        .expect("the fallback must read the MSRV floor from Cargo.toml");
    let fallback_end = src
        .find("export RUSTUP_TOOLCHAIN=")
        .expect("the harness must export the pin");
    let fallback = &src[fallback_start..fallback_end];
    assert!(
        fallback.contains("rustup toolchain list"),
        "the fallback must select from the toolchains rustup ALREADY HAS:\n{fallback}"
    );
    assert!(
        fallback.contains("\\.[0-9]+"),
        "the fallback must require a third component before exporting anything:\n{fallback}"
    );
    assert!(
        fallback.contains("exit 1"),
        "with no matching toolchain installed the fallback must exit 1 — a lint gate does not \
         install a toolchain:\n{fallback}"
    );
    assert!(
        !fallback.contains("PINNED_TOOLCHAIN=\"$MSRV_FLOOR\""),
        "the fallback must never export the two-component floor unfiltered:\n{fallback}"
    );

    // Fixture A: a 1.89.x is installed, and the highest one is chosen.
    let fx = SuiteFixture::new("msrv-ok");
    fx.stub_cargo(0, false);
    fx.stub(
        "rustup",
        "#!/bin/sh\nif [ \"$1\" = toolchain ] && [ \"$2\" = list ]; then\n\
         printf '%s\\n' '1.89.0-aarch64-apple-darwin (default)' '1.89.3-aarch64-apple-darwin' \
         '1.97.1-aarch64-apple-darwin'\nfi\nexit 0\n",
    );
    let out = fx.run(&[]);
    let combined = format!("{}{}", text(&out.stdout), text(&out.stderr));
    assert!(
        combined.contains("toolchain pinned to 1.89.3"),
        "the fallback must resolve the HIGHEST installed 1.89.x, in full:\n{combined}"
    );

    // Fixture B: none installed — refuse, naming the floor.
    let fx2 = SuiteFixture::new("msrv-missing");
    fx2.stub_cargo(0, false);
    fx2.stub(
        "rustup",
        "#!/bin/sh\nif [ \"$1\" = toolchain ] && [ \"$2\" = list ]; then\n\
         printf '%s\\n' '1.97.1-aarch64-apple-darwin (default)'\nfi\nexit 0\n",
    );
    let out2 = fx2.run(&[]);
    let combined2 = format!("{}{}", text(&out2.stdout), text(&out2.stderr));
    assert_eq!(
        out2.status.code(),
        Some(1),
        "with no 1.89.x installed the harness must REFUSE; it exited {:?}\n{combined2}",
        out2.status.code()
    );
    assert!(
        combined2.contains("1.89"),
        "the refusal must name the floor it could not satisfy:\n{combined2}"
    );
    assert!(
        !combined2.contains("toolchain pinned to 1.89\n"),
        "it must not have exported the two-component floor:\n{combined2}"
    );
}
