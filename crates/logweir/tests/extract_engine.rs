//! Task 13's own coverage: the extraction and the pin, nothing else.
//! `logweir doctor` (which would otherwise seem the natural home for a
//! "does the engine run" check) is Task 14 — its inputs (`DrillSpec`,
//! `AllowedClusters`) don't exist yet.
use std::process::Command;

fn repo_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

#[test]
fn the_extraction_script_records_a_pinned_digest_in_the_canonical_form() {
    let root = repo_root();
    let p = root.join("third_party/kafka-backup-binary.digest");
    let d = std::fs::read_to_string(&p)
        .unwrap_or_else(|e| panic!("run `just engine` first: {} ({e})", p.display()));
    let d = d.trim();
    assert!(
        d.starts_with("sha256:") && d.len() == 71,
        "the pin must be `sha256:<64 hex>`, never a mutable tag (Global Constraint 7); got `{d}`"
    );
    assert!(d[7..].chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn the_extracted_engine_reports_the_pinned_version() {
    let root = repo_root();
    let bin = root.join(".engine/kafka-backup");

    // `--version` is a flag, not a subcommand: it prints a string and exits and
    // touches no cluster or bucket, so Global Constraint 3's subcommand
    // enumeration does not reach it (controller ruling GR8).
    //
    // PLATFORM NOTE: `osodevops/kafka-backup` publishes linux/amd64 ONLY, so
    // `.engine/kafka-backup` is a linux/amd64 ELF. On Linux (every CI runner,
    // and any real deployment host) it is exec'd directly below -- this is
    // the exact code path `OsoCliEngine` uses at drill time, and is the
    // strongest verification available. On a non-Linux dev host (e.g. an
    // Apple Silicon workstation) a direct exec is impossible: it fails
    // ENOEXEC, not "the wrong version" -- Rosetta translates amd64 *inside* a
    // Linux container, it does not let Darwin exec a foreign ELF. Rather than
    // let that ENOEXEC masquerade as a version mismatch, or silently skip the
    // check, this test instead bind-mounts this SAME local file (not a fresh
    // registry pull -- the actual `docker cp` output, so a corrupted or
    // truncated extraction is still caught here) into a throwaway
    // `debian:bookworm-slim` container and execs it there via `docker run
    // --platform linux/amd64`: a real execution of the real extracted bytes,
    // printed loudly so nobody mistakes it for the native path being
    // silently dropped.
    let s = if cfg!(target_os = "linux") {
        let out = Command::new(&bin)
            .arg("--version")
            .output()
            .expect("run `just engine` first");
        String::from_utf8_lossy(&out.stdout).to_string() + &String::from_utf8_lossy(&out.stderr)
    } else {
        assert!(
            bin.exists(),
            "run `just engine` first: {} missing",
            bin.display()
        );
        eprintln!(
            "NOTE: {} is a linux/amd64 ELF and cannot be exec'd on {} (ENOEXEC). \
             Verifying it by bind-mounting the SAME local file into a \
             debian:bookworm-slim container via `docker run --platform \
             linux/amd64` instead of a native exec -- see this test's doc \
             comment. This still execs the real extracted bytes; it is a \
             different execution route, not a skip.",
            bin.display(),
            std::env::consts::OS,
        );
        let out = Command::new("docker")
            .args([
                "run",
                "--rm",
                "--platform",
                "linux/amd64",
                "-v",
                &format!("{}:/kafka-backup:ro", bin.display()),
                "debian:bookworm-slim",
                "/kafka-backup",
                "--version",
            ])
            .output()
            .unwrap_or_else(|e| panic!("docker run failed ({e}); is Docker running?"));
        String::from_utf8_lossy(&out.stdout).to_string() + &String::from_utf8_lossy(&out.stderr)
    };
    assert!(
        s.contains("0.21.0"),
        "engine floor is v0.21.0 (Global Constraint 8), got: {s}"
    );
}

/// GC15: an MIT-upstream binary with no CLA/DCO is uncapped relicensing risk
/// unless the licence text ships alongside it. The image itself carries NO
/// LICENSE file (verified against the pinned digest), so this is the only
/// check that would catch `scripts/extract-engine.sh`'s curl fallback
/// silently failing or being pointed at the wrong tag.
#[test]
fn third_party_license_mit_is_present_and_matches_upstream() {
    let root = repo_root();
    let p = root.join("third_party/LICENSE-MIT");
    let body = std::fs::read_to_string(&p)
        .unwrap_or_else(|e| panic!("run `just engine` first: {} ({e})", p.display()));
    assert!(
        !body.trim().is_empty(),
        "third_party/LICENSE-MIT exists but is empty (Global Constraint 15)"
    );
    assert!(
        body.contains("MIT License") && body.contains("OSO DevOps"),
        "third_party/LICENSE-MIT doesn't look like upstream's MIT licence, got: {body}"
    );
}
