// e2e/tests/check_image.rs
#![cfg(feature = "e2e")]
//! `scripts/check-image.sh` — the image smoke gate, tested by BREAKING the image.
//!
//! The assertions this file exercises are the ones that catch a runtime image
//! whose `docker images` row looks healthy and whose engine runs, while
//! `logweir` itself dies at the dynamic loader. That has actually happened
//! here (`Dockerfile:167-177` pastes the failure text), and the assertions that
//! catch it lived only in `.github/workflows/release.yml`, a workflow that has
//! never executed on any commit including the tag. A gate that has never run
//! is not a gate; these tests are how it is observed to fail.
//!
//! CHECK 6 (Task 8b) IS THE ONE ASSERTION WITH NO PRE-HISTORY IN release.yml.
//! It asserts the shipped `logweir` is an x86-64 ELF, which became a thing that
//! could go wrong the moment the builder stage stopped being emulated and
//! started cross-compiling (STANDING RULE 10). It is numbered 6 and runs first;
//! `scripts/check-image.sh` explains both, and
//! `check_image_rejects_an_image_whose_logweir_is_not_x86_64` is how it is
//! observed to fail.
//!
//! EVERY TEST THAT NEEDS AN IMAGE IS `#[ignore]`d. Building the `linux/amd64`
//! image is a whole-workspace `cargo build` and must not be dragged into
//! `just e2e` or `cargo test --workspace`; it was tens of minutes under
//! emulation before Task 8b and is minutes after it, which is still not a unit
//! test. `just smoke` runs them explicitly with `--ignored`. The one test that
//! needs no image — `check_image_refuses_a_wrong_argument_count` — is
//! deliberately NOT ignored, so the script's argument contract is checked on
//! every `just e2e`.
//!
//! GR2: the `#![cfg(feature = "e2e")]` gate above keeps the default,
//! Docker-free `cargo test` green, and `e2e/Cargo.toml` gains no `[[test]]`
//! section — `--test check_image` is cargo's auto-discovery.
//!
//! A MISSING BASE IMAGE IS A FAILURE, NEVER A SILENT SKIP (`require_base`).
//! A test that quietly passes because its precondition was absent is the same
//! defect class as a workflow that never ran.

use std::path::PathBuf;
use std::process::Command;

/// Cargo runs an integration test with cwd = the owning package root (`e2e/`),
/// not the repo root, so the repo root is resolved from CARGO_MANIFEST_DIR.
/// One level up from `e2e/`, unlike `crates/<name>/`'s two.
fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .canonicalize()
        .expect("the repository root is resolvable from CARGO_MANIFEST_DIR")
}

/// The image under test. `LOGWEIR_SMOKE_IMAGE` exists so a later task can point
/// these same tests at a digest reference (`ghcr.io/<owner>/logweir@sha256:…`)
/// rather than at the local tag `just image` produces.
fn base_image() -> String {
    std::env::var("LOGWEIR_SMOKE_IMAGE").unwrap_or_else(|_| "logweir:check".to_string())
}

/// Refuse, naming the reason, when the base image is not in the local daemon.
/// Deliberately a panic and not a `return`: see the module comment.
fn require_base() -> String {
    let base = base_image();
    let out = Command::new("docker")
        .args(["image", "inspect", &base])
        .output()
        .expect("docker is on PATH");
    assert!(
        out.status.success(),
        "the base image `{base}` is not in the local daemon; run `just smoke` \
         (which runs `just image` first), or set LOGWEIR_SMOKE_IMAGE to an image \
         that is present. A missing base image is a failure, not a skip."
    );
    base
}

/// A temp directory made without a crate dependency: `e2e`'s dev-dependencies
/// are fixed by this task's scope and `tempfile` is not among them. Unique per
/// call by pid + a monotonic counter, which is enough because these tests run
/// with `--test-threads=1` under `just smoke`.
fn temp_dir(label: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU32, Ordering};
    static N: AtomicU32 = AtomicU32::new(0);
    let n = N.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!(
        "logweir-check-image-{label}-{}-{n}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("a temp directory is creatable");
    dir
}

/// Build a one-layer overlay on top of the freshly built image.
///
/// `--platform linux/amd64` is mandatory and not a default: the base is an
/// amd64 image, and on the arm64 dev host an unqualified `docker build` would
/// either fail or (worse) produce something whose architecture nobody stated.
///
/// Every overlay body must `USER root` before its `RUN` and restore
/// `USER 65532:65532` after, because `Dockerfile:189` runs the image as an
/// unprivileged uid and the round-trip check depends on that staying true.
fn build_overlay(tag: &str, body: &str) {
    let base = require_base();
    let dir = temp_dir(tag);
    let dockerfile = dir.join("Dockerfile");
    let content = format!("FROM {base}\n{body}\n");
    std::fs::write(&dockerfile, content).expect("the overlay Dockerfile is writable");

    let out = Command::new("docker")
        .args([
            "build",
            "--platform",
            "linux/amd64",
            "-t",
            tag,
            "-f",
            dockerfile.to_str().expect("utf-8 path"),
            dir.to_str().expect("utf-8 path"),
        ])
        .output()
        .expect("docker is on PATH");
    let ok = out.status.success();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(ok, "the overlay `{tag}` must build; docker said:\n{stderr}");
}

/// Remove an overlay image so a smoke run does not leak tags into the daemon.
/// Best-effort: a failure here is not a property of the gate.
fn remove_overlay(tag: &str) {
    let _ = Command::new("docker").args(["rmi", "-f", tag]).output();
}

/// Run `scripts/check-image.sh` with the given arguments.
///
/// THE EXIT STATUS IS READ FROM `Output::status`, NEVER THROUGH A PIPE — that
/// is the whole point of the script taking its reference as an argument and
/// exiting 0/1, and a `| grep` here would hide it exactly as the release
/// workflow's original shape did.
fn run_check_args(args: &[&str]) -> (i32, String) {
    let mut cmd = Command::new("bash");
    cmd.arg("scripts/check-image.sh");
    for a in args {
        cmd.arg(a);
    }
    let out = cmd
        .current_dir(repo_root())
        .output()
        .expect("bash is on PATH");
    let code = out
        .status
        .code()
        .expect("scripts/check-image.sh exited normally, not by signal");
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    (code, stderr)
}

fn run_check(image_ref: &str) -> (i32, String) {
    run_check_args(&[image_ref])
}

/// Assert the gate REJECTED an image and said why. Both halves matter: a
/// non-zero exit with no evidence is a gate nobody can act on, and the stderr
/// token is what a later task (and a human reading CI output) matches.
///
/// EVERY needle must appear. Asserting the CHECK NUMBER as well as the symptom
/// is not decoration: a downstream check often fails for the same underlying
/// reason and prints the same symptom, so a single-symptom assertion cannot
/// tell "check N caught it" from "check N was deleted and check N+1 tripped
/// over the wreckage". That is a real mutant, not a hypothetical — see
/// `check_image_rejects_an_image_with_an_unresolved_library`.
fn assert_rejected_naming(tag: &str, needles: &[&str]) {
    let (code, stderr) = run_check(tag);
    assert_ne!(
        code, 0,
        "scripts/check-image.sh accepted the broken image `{tag}`; stderr was:\n{stderr}"
    );
    for needle in needles {
        assert!(
            stderr.contains(needle),
            "scripts/check-image.sh rejected `{tag}` but its stderr does not contain \
             `{needle}`, so the failure is not identifiable; stderr was:\n{stderr}"
        );
    }
}

fn assert_rejected(tag: &str, needle: &str) {
    assert_rejected_naming(tag, &[needle]);
}

// ------------------------------------------------------------------ check 6

/// TASK 8b's ASSERTION, observed failing. STANDING RULE 10 made the builder
/// stage run on `$BUILDPLATFORM` and cross-compile to
/// `x86_64-unknown-linux-gnu`, which removed a 3027-second emulated compile and
/// created exactly one new way to ship a broken image: a `logweir` built for
/// the BUILD machine's architecture instead of the target's.
///
/// THE OVERLAY WRITES A 20-BYTE ELF HEADER, not a real foreign binary, and
/// that is deliberate. A real aarch64 binary cannot be smuggled into an amd64
/// image on this host — `docker build --platform linux/amd64` will not pull an
/// arm64 base, and a real cross-architecture binary would also fail checks 1
/// and 3, so the test could not tell which check caught it. A synthetic header
/// isolates check 6 exactly: valid magic, valid ELFCLASS64, `e_machine` =
/// 0x00b7 (EM_AARCH64) at offset 18. Delete check 6 and this test fails,
/// because check 1 then rejects the file for being unreadable by `ldd` and
/// says nothing about the architecture. Loosen check 6 to the magic alone and
/// it fails too, because the magic here is correct.
///
/// The bytes, in order: `7f 45 4c 46` (magic), `02` (ELFCLASS64), `01` (LSB),
/// `01` (EI_VERSION), seven zero bytes of OSABI/ABIVERSION/pad plus two more,
/// `02 00` (ET_EXEC), `b7 00` (EM_AARCH64).
#[test]
#[ignore = "needs a locally built linux/amd64 image; run `just smoke`"]
fn check_image_rejects_an_image_whose_logweir_is_not_x86_64() {
    let tag = "logweir-check-broken:wrong-arch";
    build_overlay(
        tag,
        "USER root\n\
         RUN printf '\\177ELF\\002\\001\\001\\000\\000\\000\\000\\000\\000\\000\\000\\000\\002\\000\\267\\000' \
         > /usr/local/bin/logweir\n\
         USER 65532:65532",
    );
    assert_rejected_naming(tag, &["check 6 (ELF)", "e_machine", "b7 00"]);
    remove_overlay(tag);
}

// ------------------------------------------------------------------ check 1

/// The shipped defect, reproduced. `libsasl2.so.2` is a dynamic dependency of
/// LOGWEIR's own binary (rdkafka links librdkafka with SASL), not the engine's,
/// which is why an image missing it still passes every other look.
///
/// The stderr must NAME the library: `release.yml:143` says "this line NAMES
/// the missing library", and T0-17's acceptance repeats it.
///
/// AND IT MUST NAME CHECK 1. Asserting only `libsasl2.so.2` does not test the
/// `ldd` arm at all: with check 1 deleted the image still fails check 3, and
/// the dynamic loader's own message ("libsasl2.so.2: cannot open shared object
/// file") reaches stderr through docker, so the single-symptom assertion passed
/// against a script that had lost the arm entirely. MEASURED — that is mutant
/// M1, and it survived until this second needle was added. Check 1's whole
/// value over check 3 is that it names the library from `ldd` rather than from
/// a crash, so "check 1 (ldd)" is the property, not a formatting detail.
#[test]
#[ignore = "needs a locally built linux/amd64 image; run `just smoke`"]
fn check_image_rejects_an_image_with_an_unresolved_library() {
    let tag = "logweir-check-broken:no-libsasl2";
    build_overlay(
        tag,
        "USER root\nRUN rm -f /usr/lib/*/libsasl2.so.2*\nUSER 65532:65532",
    );
    assert_rejected_naming(tag, &["libsasl2.so.2", "check 1 (ldd)"]);
    remove_overlay(tag);
}

// ------------------------------------------------------------------ check 2

/// Deleting the engine leaves checks 1 (logweir's own linkage) intact, so this
/// overlay isolates check 2 exactly.
#[test]
#[ignore = "needs a locally built linux/amd64 image; run `just smoke`"]
fn check_image_rejects_an_image_whose_engine_is_missing() {
    let tag = "logweir-check-broken:no-engine";
    build_overlay(
        tag,
        "USER root\nRUN rm -f /usr/local/bin/kafka-backup\nUSER 65532:65532",
    );
    assert_rejected_naming(tag, &["kafka-backup --version", "check 2"]);
    remove_overlay(tag);
}

// ------------------------------------------------------------------ check 3

/// `/bin/false` is a REAL dynamic ELF, so `ldd` still resolves and check 1
/// still passes — the isolation this test needs. A shell-script shim would
/// make `ldd` exit non-zero and fail check 1 instead, which is why the brief
/// forbids one.
#[test]
#[ignore = "needs a locally built linux/amd64 image; run `just smoke`"]
fn check_image_rejects_an_image_whose_logweir_cli_fails() {
    let tag = "logweir-check-broken:cli-fails";
    build_overlay(
        tag,
        "USER root\nRUN cp /bin/false /usr/local/bin/logweir\nUSER 65532:65532",
    );
    assert_rejected_naming(tag, &["logweir --version", "check 3"]);
    remove_overlay(tag);
}

// ------------------------------------------------------------------ check 4

/// `kafka-backup` answers `--version` and resolves its libraries, so checks
/// 1-3 all pass and only the approval round-trip fails. This is the check that
/// proves an operator holding nothing but the image can mint the approval
/// `drill run` refuses to start without.
#[test]
#[ignore = "needs a locally built linux/amd64 image; run `just smoke`"]
fn check_image_rejects_an_image_that_cannot_mint_an_approval() {
    let tag = "logweir-check-broken:no-approve";
    build_overlay(
        tag,
        "USER root\nRUN cp /usr/local/bin/kafka-backup /usr/local/bin/logweir\nUSER 65532:65532",
    );
    assert_rejected_naming(tag, &["drill approve", "check 4"]);
    remove_overlay(tag);
}

// ------------------------------------------------------------------ check 5

/// GC15: the upstream MIT licence must be IN the image that redistributes the
/// upstream binary.
///
/// `kafka-backup/LICENSE` IS ASSERTED IN THE MESSAGE, not just "a licence".
/// Check 5 was one `test -s A && test -s B` until Task 8b, so its failure said
/// a licence was missing without saying which — and the remedy differs by
/// file. The split is only real if the message names the file, so the test
/// requires the path.
#[test]
#[ignore = "needs a locally built linux/amd64 image; run `just smoke`"]
fn check_image_rejects_an_image_missing_the_upstream_licence() {
    let tag = "logweir-check-broken:no-licence";
    build_overlay(
        tag,
        "USER root\nRUN rm -f /usr/share/licenses/kafka-backup/LICENSE\nUSER 65532:65532",
    );
    assert_rejected_naming(
        tag,
        &[
            "licence",
            "check 5",
            "/usr/share/licenses/kafka-backup/LICENSE",
        ],
    );
    remove_overlay(tag);
}

/// The other half of the split (Task 8b). GC15 governs Logweir's own
/// redistribution as much as upstream's, and before the split an image that
/// carried the upstream MIT copy but had lost `LICENSE`/`NOTICE` failed with a
/// message that pointed at the wrong file.
#[test]
#[ignore = "needs a locally built linux/amd64 image; run `just smoke`"]
fn check_image_rejects_an_image_missing_the_logweir_licence() {
    let tag = "logweir-check-broken:no-own-licence";
    build_overlay(
        tag,
        "USER root\nRUN rm -f /usr/share/licenses/logweir/LICENSE\nUSER 65532:65532",
    );
    assert_rejected_naming(
        tag,
        &["licence", "check 5", "/usr/share/licenses/logweir/LICENSE"],
    );
    remove_overlay(tag);
}

// ------------------------------------------------------ the working tree

/// ACCEPTANCE 8, MECHANISED. `release.yml` wrote its scratch directory as
/// `.imgcheck/` INSIDE THE REPOSITORY and removed it with a plain `rm -rf` on
/// the success path only — so a run that failed at any check left the approval,
/// the spec copy and A PRIVATE KEY sitting in the working tree. This script
/// uses `mktemp -d` plus `trap ... EXIT` instead, which is the one deliberate
/// divergence from release.yml's behaviour, and this test is what keeps it.
///
/// The FAILING path is the one tested, because the succeeding path was never
/// the one that leaked. Compared before and after rather than asserted empty:
/// a developer running `just smoke` mid-task has their own edits outstanding.
#[test]
#[ignore = "needs a locally built linux/amd64 image; run `just smoke`"]
fn check_image_leaves_the_tree_clean_after_a_failed_run() {
    fn porcelain() -> String {
        let out = Command::new("git")
            .args(["status", "--porcelain"])
            .current_dir(repo_root())
            .output()
            .expect("git is on PATH");
        assert!(out.status.success(), "git status --porcelain failed");
        String::from_utf8_lossy(&out.stdout).to_string()
    }

    let tag = "logweir-check-broken:tree-clean";
    // The licence overlay fails at check 5, i.e. AFTER check 4 has created the
    // key, the spec copy and the approval — the only ordering in which a
    // repository-resident scratch directory would still be on disk.
    build_overlay(
        tag,
        "USER root\nRUN rm -f /usr/share/licenses/kafka-backup/LICENSE\nUSER 65532:65532",
    );
    let before = porcelain();
    let (code, _stderr) = run_check(tag);
    assert_ne!(
        code, 0,
        "the overlay must be rejected for this test to mean anything"
    );
    let after = porcelain();
    assert_eq!(
        before, after,
        "scripts/check-image.sh changed the working tree on its FAILURE path; \
         its mktemp -d + `trap ... EXIT` is what stops the approver key and the \
         minted approval being left in the repository"
    );
    remove_overlay(tag);
}

// ------------------------------------------------------- the docker exit code

/// M7's dedicated case. Check 1 inspects `ldd`'s TEXT, because `ldd` exits 0
/// even with unresolved libraries — so the only thing standing between a
/// container that cannot start at all and a green gate is that check 1 reads
/// DOCKER's exit status rather than swallowing it. Remove `/bin/sh` and the
/// `docker run --entrypoint /bin/sh` cannot start; if the script accepted that,
/// an image with no usable shell would pass its own linkage check by producing
/// no output.
///
/// `RUN` uses `/bin/sh` itself, so the removal is in exec form.
#[test]
#[ignore = "needs a locally built linux/amd64 image; run `just smoke`"]
fn check_image_rejects_an_image_with_no_shell() {
    let tag = "logweir-check-broken:no-shell";
    build_overlay(
        tag,
        "USER root\nRUN [\"/bin/rm\", \"-f\", \"/bin/sh\"]\nUSER 65532:65532",
    );
    assert_rejected(tag, "check 1");
    remove_overlay(tag);
}

// ------------------------------------------------------------- the good image

/// The other direction, and the one that makes every rejection above mean
/// something: the freshly built image is ACCEPTED, with the exit code compared
/// to 0 exactly rather than merely "not one of the failures".
#[test]
#[ignore = "needs a locally built linux/amd64 image; run `just smoke`"]
fn check_image_accepts_the_freshly_built_image() {
    let base = require_base();
    let (code, stderr) = run_check(&base);
    assert_eq!(code, 0, "stderr: {stderr}");
}

/// `imagePullPolicy: Never` (Tasks 16/17) needs THIS image in the daemon, and
/// the engine layer has no arm64 manifest, so an image that built for the host
/// architecture is not the shipped artifact even though it may pass every
/// other check here.
#[test]
#[ignore = "needs a locally built linux/amd64 image; run `just smoke`"]
fn smoke_builds_amd64() {
    let base = require_base();
    let out = Command::new("docker")
        .args(["image", "inspect", "--format", "{{.Architecture}}", &base])
        .output()
        .expect("docker is on PATH");
    assert!(
        out.status.success(),
        "docker image inspect failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let arch = String::from_utf8_lossy(&out.stdout).trim().to_string();
    assert_eq!(
        arch, "amd64",
        "`{base}` is a {arch} image; `just image` must build --platform linux/amd64"
    );
}

// --------------------------------------------------------- the argument contract

/// NOT `#[ignore]`d, deliberately: this one needs no image, so the script's
/// arity contract is checked on every `just e2e` rather than only on the rare
/// `just smoke`. Zero arguments must not silently default to a tag, and a
/// second argument must not be ignored — both are how a caller that meant to
/// pass a pushed digest ends up asserting something else.
#[test]
fn check_image_refuses_a_wrong_argument_count() {
    let (code, stderr) = run_check_args(&[]);
    assert_ne!(code, 0, "zero arguments must be refused; stderr: {stderr}");
    assert!(
        stderr.contains("usage"),
        "zero arguments must print a usage line to stderr; stderr was:\n{stderr}"
    );

    let (code, stderr) = run_check_args(&["logweir:check", "logweir:check"]);
    assert_ne!(code, 0, "two arguments must be refused; stderr: {stderr}");
    assert!(
        stderr.contains("usage"),
        "two arguments must print a usage line to stderr; stderr was:\n{stderr}"
    );
}

/// A reference the daemon does not hold is an operational failure with a named
/// reason, and it must be reported BEFORE any check runs — otherwise every
/// `docker run` below fails for the same reason and the first check gets the
/// blame.
#[test]
fn check_image_refuses_a_reference_the_daemon_does_not_hold() {
    let (code, stderr) = run_check("logweir:not-a-real-tag");
    assert_ne!(
        code, 0,
        "an absent reference must be refused; stderr: {stderr}"
    );
    assert!(
        stderr.contains("local daemon"),
        "an absent reference must name the daemon; stderr was:\n{stderr}"
    );
    assert!(
        !stderr.contains("check 1"),
        "an absent reference must be refused before check 1 runs; stderr was:\n{stderr}"
    );
}
