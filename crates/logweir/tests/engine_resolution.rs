//! `logweir doctor` and `logweir drill run` must resolve the engine the SAME
//! way.
//!
//! They did not. `doctor` walked `$LOGWEIR_ENGINE_BIN` ->
//! `/usr/local/bin/kafka-backup` -> a `$PATH` scan; `drill run` used the
//! literal `.engine/kafka-backup`, and `Command::new` performs no `$PATH`
//! search for a path containing `/`. `README.md` and `docs/quickstart.md` both
//! promise `$PATH` is enough. So the documented install produced a `doctor`
//! that printed `ok engine binary` and `ok engine version` and a `drill run`
//! that read the target cluster and the archive manifest through phases 0-4
//! and then died at phase 5, the first time it tried to execute an engine it
//! had never been told how to find.
use std::path::{Path, PathBuf};
use std::process::Command;

fn repo_root() -> PathBuf {
    // The test binary's cwd is the package directory.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

fn fake_engine_dir() -> (tempfile::TempDir, PathBuf) {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let exe = dir.path().join("kafka-backup");
    std::fs::write(&exe, "#!/bin/sh\necho 'kafka-backup 0.21.0'\n").unwrap();
    std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
    (dir, exe)
}

/// Driven through the COMPILED BINARY, in a working directory with no
/// `.engine/`, with the engine reachable ONLY through `$PATH` — the install
/// `README.md` documents — and with `$LOGWEIR_ENGINE_BIN` explicitly removed.
///
/// `doctor` must name the file on `$PATH`, and must name it as the path it
/// found rather than as a bare name, because that string is what an operator
/// compares against what the drill later reports.
///
/// The spec is `drill-bad-storage.yaml` so check 6 fails synchronously on a
/// local path instead of waiting out an S3 timeout: checks 1 and 2 are the
/// subject, and `doctor` prints them before it short-circuits.
#[test]
fn doctor_resolves_an_engine_installed_only_on_path() {
    let (bin_dir, exe) = fake_engine_dir();
    let cwd = tempfile::tempdir().unwrap();
    let root = repo_root();

    let out = Command::new(env!("CARGO_BIN_EXE_logweir"))
        .args(["doctor", "--spec"])
        .arg(root.join("e2e/fixtures/drill-bad-storage.yaml"))
        .arg("--allowed-clusters")
        .arg(root.join("e2e/fixtures/allowed-clusters-empty.json"))
        .arg("--approver-key")
        .arg(root.join("e2e/fixtures/signed/public.pem"))
        .current_dir(cwd.path())
        .env_remove("LOGWEIR_ENGINE_BIN")
        .env("PATH", bin_dir.path())
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stdout.contains(&format!("ok    engine binary      {}", exe.display())),
        "doctor did not resolve the engine on $PATH.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("ok    engine version"),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

/// THE AGREEMENT, enforced structurally rather than by hoping two functions
/// stay in step: there is exactly ONE engine resolution in this crate, in
/// `engine_bin.rs`, and every other module goes through it.
///
/// A behavioural test cannot reach the drill's half without a live broker and
/// a live archive, so a mutant that reverted `drill::context` to its own
/// `$LOGWEIR_ENGINE_BIN`-or-`.engine/kafka-backup` default would survive one.
/// This test does not care where the resolution is CALLED from — it asserts
/// that nowhere else can define one.
#[test]
fn the_engine_is_resolved_in_exactly_one_module() {
    // Each needle is a way to build a second, divergent resolution: the
    // environment variable, and the two path literals the two old chains
    // disagreed about.
    const NEEDLES: [&str; 3] = [
        "LOGWEIR_ENGINE_BIN",
        ".engine/kafka-backup",
        "/usr/local/bin/kafka-backup",
    ];
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();
    let mut scanned = 0usize;
    visit(&src, &mut |file| {
        if file.file_name().and_then(|n| n.to_str()) == Some("engine_bin.rs") {
            return;
        }
        scanned += 1;
        let text = std::fs::read_to_string(file).unwrap();
        for (n, line) in text.lines().enumerate() {
            let code = line.trim_start();
            // Prose ABOUT the old chain is the point of the fix and must stay
            // readable; only executable lines can build a second resolution.
            if code.starts_with("//") {
                continue;
            }
            for needle in NEEDLES {
                if code.contains(needle) {
                    offenders.push(format!("{}:{}: {}", file.display(), n + 1, line.trim()));
                }
            }
        }
    });
    assert!(
        scanned > 5,
        "the scan found only {scanned} source file(s); it is not looking where it thinks it is"
    );
    assert!(
        offenders.is_empty(),
        "a second engine resolution exists outside `engine_bin.rs`, so `doctor` and \
         `drill run` can disagree about which binary they mean:\n{}",
        offenders.join("\n")
    );
}

fn visit(dir: &Path, f: &mut impl FnMut(&Path)) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let p = entry.unwrap().path();
        if p.is_dir() {
            visit(&p, f);
        } else if p.extension().and_then(|e| e.to_str()) == Some("rs") {
            f(&p);
        }
    }
}
