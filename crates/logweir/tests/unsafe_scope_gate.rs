//! **The `unsafe` perimeter gate, and the negative controls that keep it a
//! gate** (PROD-04.0b; OD-6 (a2); `decisions/PROD-04.0-admin-path.md` §7.4).
//!
//! `scripts/check-unsafe-scope.sh` is the gate: exactly one crate,
//! `crates/logweir-rdkafka-ffi`, may contain `unsafe`, only in its `src/`,
//! only reached through `logweir-kafka`, with a `// SAFETY:` comment on every
//! block; every other crate root forbids `unsafe_code`. These tests are its
//! red side. Each builds a throwaway overlay of the workspace in the temp
//! directory (the `one_signer_gate.rs` pattern: a probe written into the real
//! `crates/` would become a workspace member and change `Cargo.lock`), plants
//! violations, and requires the gate to exit 1 NAMING each planted file. A
//! gate that has never been seen failing is indistinguishable from `exit 0`.
//!
//! | test | plants | the gate must say |
//! |---|---|---|
//! | `the_gate_is_green_on_this_tree` | nothing | exit 0, "the perimeter holds" |
//! | `a_root_without_forbid_is_named` | a lib root and an example root without the attribute; a root with it commented out | each file, "without #![forbid(unsafe_code)]" |
//! | `code_shaped_unsafe_outside_the_perimeter_is_named` | eighteen files outside the perimeter's `src/`, one construct each (the review's six and M2's four included), one under a nested `upstream/`, one inside the perimeter's own `tests/` | each `file:line` |
//! | `rustc_forbids_unsafe_in_every_target_through_the_workspace_lints` | the workspace table relaxed; a member without `[lints] workspace = true`; the perimeter taking it | each named |
//! | `prose_strings_and_chars_are_not_code` | the word in comments, strings, raw strings, byte strings; char literals and lifetimes beside it | exit 0 |
//! | `the_perimeter_is_exactly_one_crate_reached_only_through_logweir_kafka` | a second crate without `forbid` or the lints opt-in; a crate depending on the perimeter; the perimeter depending on another crate | each named |
//! | `the_perimeter_keeps_its_lints_and_its_safety_comments` | the perimeter root without its two lints, and with `forbid`; an `unsafe` block without a SAFETY comment; one with a SAFETY comment too short; an `unsafe fn` without `# Safety` | each named |
//! | `just_lint_runs_the_unsafe_scope_gate` | — | the `lint` recipe names the script |
//!
//! Every assertion reads the exit status from the child directly, never
//! through a pipe.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const PERIMETER: &str = "crates/logweir-rdkafka-ffi";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("the repository root resolves from CARGO_MANIFEST_DIR")
}

fn run_gate(cwd: &Path, root: Option<&Path>) -> Output {
    let mut cmd = Command::new("bash");
    cmd.arg("scripts/check-unsafe-scope.sh").current_dir(cwd);
    if let Some(r) = root {
        cmd.env("LOGWEIR_ROOT", r);
    }
    cmd.output().expect("the gate script can be spawned")
}

fn text(o: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    )
}

/// A throwaway copy of the workspace's manifests and sources, deleted on drop.
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
        let (src, dst) = (entry.path(), to.join(entry.file_name()));
        let ty = entry.file_type().expect("a readable file type");
        if ty.is_dir() {
            if entry.file_name() == "target" {
                continue;
            }
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
            "logweir-unsafe-scope-{tag}-{}-{stamp}",
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
        std::fs::copy(
            root.join("scripts/check-unsafe-scope.sh"),
            path.join("scripts/check-unsafe-scope.sh"),
        )
        .expect("copy the gate into the overlay");
        Overlay { path }
    }

    fn write(&self, rel: &str, body: &str) {
        let p = self.path.join(rel);
        std::fs::create_dir_all(p.parent().expect("a parent")).expect("mkdir");
        std::fs::write(&p, body).unwrap_or_else(|e| panic!("write {rel}: {e}"));
    }

    fn read(&self, rel: &str) -> String {
        std::fs::read_to_string(self.path.join(rel)).unwrap_or_else(|e| panic!("read {rel}: {e}"))
    }

    /// Replaces the first `from` in `rel` with `to`, and fails if there is none
    /// (a control whose edit did not apply proves nothing).
    fn edit(&self, rel: &str, from: &str, to: &str) {
        let body = self.read(rel);
        assert!(body.contains(from), "{rel} no longer contains {from:?}");
        self.write(rel, &body.replacen(from, to, 1));
    }

    /// A workspace crate under `crates/`, which the `crates/*` glob makes a
    /// member with no manifest edit.
    fn probe_crate(&self, name: &str, dependencies: &str, lib: &str) {
        self.write(
            &format!("crates/{name}/Cargo.toml"),
            &format!(
                "[package]\nname = \"{name}\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\
                 publish = false\n\n[dependencies]\n{dependencies}\n"
            ),
        );
        self.write(&format!("crates/{name}/src/lib.rs"), lib);
    }

    fn gate(&self) -> Output {
        run_gate(&self.path, Some(&self.path))
    }
}

/// The gate exited 1 and said each of `needles`.
fn assert_red(out: &Output, needles: &[&str]) {
    let all = text(out);
    assert_eq!(
        out.status.code(),
        Some(1),
        "the gate must exit 1 on a planted violation:\n{all}"
    );
    for n in needles {
        assert!(all.contains(n), "the gate's output must name {n:?}:\n{all}");
    }
}

#[test]
fn the_gate_is_green_on_this_tree() {
    let out = run_gate(&repo_root(), None);
    assert!(
        out.status.success(),
        "check-unsafe-scope.sh must be green on the unmodified tree:\n{}",
        text(&out)
    );
    assert!(text(&out).contains("the perimeter holds"), "{}", text(&out));
}

#[test]
fn a_root_without_forbid_is_named() {
    let ov = Overlay::new("roots");
    ov.edit(
        "crates/logweir-store/src/lib.rs",
        "#![forbid(unsafe_code)]\n",
        "",
    );
    ov.edit(
        "crates/weirkeeper/examples/emit_crds.rs",
        "#![forbid(unsafe_code)]\n",
        "",
    );
    // A commented-out attribute is not an attribute.
    ov.edit(
        "xtask/src/main.rs",
        "#![forbid(unsafe_code)]",
        "// #![forbid(unsafe_code)]",
    );
    assert_red(
        &ov.gate(),
        &[
            "crates/logweir-store/src/lib.rs: lib target root of `logweir-store` without #![forbid(unsafe_code)]",
            "crates/weirkeeper/examples/emit_crds.rs: example target root of `weirkeeper` without",
            "xtask/src/main.rs: bin target root of `xtask` without",
        ],
    );
}

#[test]
fn code_shaped_unsafe_outside_the_perimeter_is_named() {
    let ov = Overlay::new("shapes");
    // One file per construct, each in an integration-test target (a crate of
    // its own, which no root attribute reaches). The six marked "review" are
    // the constructs PROD-04.0b's review found passing the first gate (M1),
    // and `export_name`, `link_section`, `warn` and `expect` are M2's.
    let probes = [
        ("block", "pub fn f(p: *const u8) -> u8 {\n    unsafe { *p }\n}\n"),
        ("function", "pub unsafe fn g() {}\n"),
        ("impl", "pub struct S;\nunsafe impl Send for S {}\n"),
        ("trait", "pub unsafe trait T {}\n"),
        ("foreign", "extern \"C\" {\n    fn abs(x: i32) -> i32;\n}\n"),
        ("nomangle", "#[no_mangle]\npub extern \"C\" fn h() {}\n"),
        ("exportname", "#[export_name = \"rd_kafka_destroy\"]\npub extern \"C\" fn e() {}\n"),
        ("linksection", "#[link_section = \".text\"]\npub static X: u8 = 0;\n"),
        ("allow", "#[allow(unsafe_code)]\npub fn k() {}\n"),
        ("warn", "#![warn(unsafe_code)]\npub fn w() {}\n"),
        ("expect", "#[expect(unsafe_code)]\npub fn x() {}\n"),
        ("attribute", "#[unsafe(no_mangle)]\npub fn m() {}\n"),
        // review S1: a macro whose expansion is the `unsafe` block
        ("macro", "macro_rules! s { ($($t:tt)*) => { unsafe $($t)* }; }\npub fn r(p: *const u8) -> u8 { s!({ *p }) }\n"),
        // review S2: the attribute behind `cfg_attr`
        ("cfgattr", "#[cfg_attr(all(), no_mangle)]\npub extern \"C\" fn c() {}\n"),
        // review S3: text the scan never reads
        ("include", "include!(\"sneak.in\");\n"),
        // review S5: module-level assembly needs no `unsafe` token
        ("globalasm", "std::arch::global_asm!(\"nop\");\n"),
        // review S6: the perimeter's exempt source compiled into another crate
        ("pathin", "#[path = \"../../logweir-rdkafka-ffi/src/zz_not_a_module.rs\"]\nmod m;\n"),
        ("pathout", "#[path = \"/etc/zz_outside.rs\"]\nmod o;\n"),
    ];
    for (name, body) in probes {
        ov.write(&format!("crates/logweir-store/tests/zz_{name}.rs"), body);
    }
    // review S4: a directory named like a root-level prune target, deeper down.
    ov.write(
        "crates/logweir-store/tests/upstream/mod.rs",
        "pub fn u(p: *const u8) -> u8 { unsafe { *p } }\n",
    );
    // The perimeter's own integration tests are a crate of their own and
    // outside its `src/`: they use the safe API like everyone else.
    ov.write(
        &format!("{PERIMETER}/tests/zz_inside_the_package.rs"),
        "#[test]\nfn t() {\n    let x = 1u8;\n    // SAFETY: a long enough comment that names nothing real at all.\n    let _ = unsafe { *(&x as *const u8) };\n}\n",
    );
    assert_red(
        &ov.gate(),
        &[
            "crates/logweir-store/tests/zz_block.rs:2: an `unsafe` token",
            "crates/logweir-store/tests/zz_function.rs:1: an `unsafe` token",
            "crates/logweir-store/tests/zz_impl.rs:2: an `unsafe` token",
            "crates/logweir-store/tests/zz_trait.rs:1: an `unsafe` token",
            "crates/logweir-store/tests/zz_foreign.rs:1: a foreign `extern` block",
            "crates/logweir-store/tests/zz_nomangle.rs:1: an `unsafe_code` attribute",
            "crates/logweir-store/tests/zz_exportname.rs:1: an `unsafe_code` attribute",
            "crates/logweir-store/tests/zz_linksection.rs:1: an `unsafe_code` attribute",
            "crates/logweir-store/tests/zz_allow.rs:1: a relaxed `unsafe_code` lint",
            "crates/logweir-store/tests/zz_warn.rs:1: a relaxed `unsafe_code` lint",
            "crates/logweir-store/tests/zz_expect.rs:1: a relaxed `unsafe_code` lint",
            "crates/logweir-store/tests/zz_attribute.rs:1: an `unsafe` token",
            "crates/logweir-store/tests/zz_macro.rs:1: an `unsafe` token",
            "crates/logweir-store/tests/zz_cfgattr.rs:1: an `unsafe_code` attribute",
            "crates/logweir-store/tests/zz_include.rs:1: an `include!`",
            "crates/logweir-store/tests/zz_globalasm.rs:1: module-level assembly",
            "crates/logweir-store/tests/zz_pathin.rs:1: a `#[path]` into the perimeter or out of the tree",
            "crates/logweir-store/tests/zz_pathout.rs:1: a `#[path]` into the perimeter or out of the tree",
            "crates/logweir-store/tests/upstream/mod.rs:1: an `unsafe` token",
            "crates/logweir-rdkafka-ffi/tests/zz_inside_the_package.rs:5: an `unsafe` token",
        ],
    );
}

/// **Rustc is the first layer** (PROD-04.0b's fix round, review M1): the
/// root's `[workspace.lints.rust] unsafe_code = "forbid"`, taken by every
/// member but the perimeter, makes rustc refuse `unsafe` in every target. The
/// gate requires the table and each opt-in, and refuses the perimeter taking it.
#[test]
fn rustc_forbids_unsafe_in_every_target_through_the_workspace_lints() {
    let ov = Overlay::new("lints");
    ov.edit(
        "Cargo.toml",
        "unsafe_code = \"forbid\"",
        "unsafe_code = \"warn\"",
    );
    ov.edit(
        "crates/logweir-store/Cargo.toml",
        "[lints]\nworkspace = true\n",
        "",
    );
    let perimeter_manifest = format!("{PERIMETER}/Cargo.toml");
    let body = ov.read(&perimeter_manifest);
    ov.write(
        &perimeter_manifest,
        &format!("{body}\n[lints]\nworkspace = true\n"),
    );
    assert_red(
        &ov.gate(),
        &[
            "Cargo.toml: no `[workspace.lints.rust]` table with `unsafe_code = \"forbid\"`",
            "crates/logweir-store/Cargo.toml: `logweir-store` lacks `[lints] workspace = true`",
            "crates/logweir-rdkafka-ffi/Cargo.toml: the perimeter takes the workspace lints",
        ],
    );
}

#[test]
fn prose_strings_and_chars_are_not_code() {
    let ov = Overlay::new("prose");
    ov.write(
        "crates/logweir-store/tests/zz_prose.rs",
        concat!(
            "//! An unsafe method needs CSRF; `unsafe { }` in prose is prose.\n",
            "/* a block comment: unsafe fn x() {} /* nested: unsafe impl */ still comment */\n",
            "/// unsafe trait in a doc comment\n",
            "pub fn f<'a>(s: &'a str) -> (&'a str, char, char, &'static [u8]) {\n",
            "    let _ = \"unsafe { *p } and extern \\\"C\\\" { }\";\n",
            "    let _ = r##\"unsafe impl Send for S {} \"# still raw\"##;\n",
            "    let _ = b\"unsafe fn\";\n",
            "    let _ = \"#[no_mangle] #[allow(unsafe_code)]\";\n",
            "    (s, '{', '\\'', b\"unsafe {\")\n",
            "}\n",
        ),
    );
    let out = ov.gate();
    assert!(
        out.status.success(),
        "comments, strings, raw strings, byte strings and chars are not code:\n{}",
        text(&out)
    );
}

#[test]
fn the_perimeter_is_exactly_one_crate_reached_only_through_logweir_kafka() {
    let ov = Overlay::new("perimeter");
    // A second crate that does not forbid `unsafe`.
    ov.probe_crate("zz-second-perimeter", "", "pub fn f() {}\n");
    // A crate that reaches the perimeter directly instead of through logweir-kafka.
    ov.probe_crate(
        "zz-direct-user",
        "logweir-rdkafka-ffi = { path = \"../logweir-rdkafka-ffi\" }",
        "#![forbid(unsafe_code)]\npub fn f() {}\n",
    );
    // The perimeter taking a dependency beyond rdkafka.
    ov.edit(
        &format!("{PERIMETER}/Cargo.toml"),
        "[dependencies]\n",
        "[dependencies]\nlogweir-core = { path = \"../logweir-core\" }\n",
    );
    assert_red(
        &ov.gate(),
        &[
            "crates/zz-second-perimeter/src/lib.rs: lib target root of `zz-second-perimeter` without",
            "`zz-second-perimeter` lacks `[lints] workspace = true`",
            "`zz-direct-user` depends on the perimeter; only ['logweir-kafka'] may",
            "the perimeter depends on ['logweir-core']; it may depend on ['rdkafka'] only",
        ],
    );
}

#[test]
fn the_perimeter_keeps_its_lints_and_its_safety_comments() {
    let ov = Overlay::new("lints");
    let lib = format!("{PERIMETER}/src/lib.rs");
    ov.edit(&lib, "#![deny(unsafe_op_in_unsafe_fn)]\n", "");
    ov.edit(&lib, "#![deny(clippy::undocumented_unsafe_blocks)]\n", "");
    ov.edit(
        &lib,
        "#![deny(missing_docs)]\n",
        "#![deny(missing_docs)]\n#![forbid(unsafe_code)]\n",
    );
    ov.write(
        &format!("{PERIMETER}/src/zz_probe.rs"),
        concat!(
            "pub fn a(p: *const u8) -> u8 {\n",
            "    unsafe { *p }\n",
            "}\n",
            "pub fn b(p: *const u8) -> u8 {\n",
            "    // SAFETY: fine.\n",
            "    unsafe { *p }\n",
            "}\n",
            "/// Reads a byte.\n",
            "pub unsafe fn c(p: *const u8) -> u8 {\n",
            "    // SAFETY: the caller guarantees `p` is valid for a one-byte read.\n",
            "    unsafe { *p }\n",
            "}\n",
        ),
    );
    assert_red(
        &ov.gate(),
        &[
            "the perimeter's root lacks #![deny(unsafe_op_in_unsafe_fn)]",
            "the perimeter's root lacks #![deny(clippy::undocumented_unsafe_blocks)]",
            "the perimeter forbids `unsafe`, so it is empty",
            "crates/logweir-rdkafka-ffi/src/zz_probe.rs:2: `unsafe {` without a `SAFETY:` comment",
            "crates/logweir-rdkafka-ffi/src/zz_probe.rs:6: the `SAFETY:` comment is too short",
            "crates/logweir-rdkafka-ffi/src/zz_probe.rs:9: `unsafe fn` without a `# Safety` comment",
        ],
    );
}

#[test]
fn just_lint_runs_the_unsafe_scope_gate() {
    let justfile =
        std::fs::read_to_string(repo_root().join("justfile")).expect("the justfile is readable");
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
        body.lines()
            .any(|l| l.trim() == "./scripts/check-unsafe-scope.sh"),
        "`just lint` must run the unsafe perimeter gate (OD-6, ADR 0004); the recipe was:\n{body}"
    );
}

/// **AP-04.1-4's gate (T1).** rust-rdkafka 0.36.2's `GroupInfo::members` is
/// undefined behaviour on a group without members; the workspace `clippy.toml`
/// lists it under `disallowed-methods`, so `cargo clippy -D warnings` refuses
/// any call. This fails when the entry is missing while `Cargo.lock` carries
/// rdkafka below 0.37 (where the defect is fixed). The mutant that deletes the
/// entry makes it red; a call to the method makes clippy red (recorded in
/// the PROD-04.0b report).
#[test]
fn the_unsound_group_member_accessor_is_fenced_while_rdkafka_is_below_0_37() {
    let lock = std::fs::read_to_string(repo_root().join("Cargo.lock")).expect("Cargo.lock");
    let mut versions = Vec::new();
    let mut in_rdkafka = false;
    for line in lock.lines() {
        if line == "[[package]]" {
            in_rdkafka = false;
        } else if line == "name = \"rdkafka\"" {
            in_rdkafka = true;
        } else if in_rdkafka {
            if let Some(v) = line.strip_prefix("version = \"") {
                versions.push(v.trim_end_matches('"').to_string());
                in_rdkafka = false;
            }
        }
    }
    assert!(
        !versions.is_empty(),
        "Cargo.lock locks no rdkafka: this gate reads the wrong file"
    );
    let below_0_37 = versions.iter().any(|v| {
        let mut parts = v.split('.').map(|p| p.parse::<u64>().unwrap_or(u64::MAX));
        matches!((parts.next(), parts.next()), (Some(0), Some(minor)) if minor < 37)
    });
    if !below_0_37 {
        return;
    }
    let clippy = std::fs::read_to_string(repo_root().join("clippy.toml")).unwrap_or_default();
    let fenced = clippy
        .lines()
        .filter(|l| !l.trim_start().starts_with('#'))
        .any(|l| l.contains("path = \"rdkafka::groups::GroupInfo::members\""));
    assert!(
        fenced,
        "Cargo.lock carries rdkafka {versions:?} (below 0.37, T1), and clippy.toml no longer \
         lists rdkafka::groups::GroupInfo::members under disallowed-methods"
    );
}
