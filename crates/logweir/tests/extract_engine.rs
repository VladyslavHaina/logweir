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

// ---------------------------------------------------------------------------
// Task 30: the script's TWO MODES, read from its source, and the one execution
// of it that reaches no daemon.
//
// These are source-reading tests on purpose. Running the default mode for real
// pulls an image, and STANDING RULE 7 is explicit that a lint or unit gate
// never reaches the network. The refusal path below is the exception that
// proves it: it runs the script, and exits before the first `docker` word.

fn extract_engine_sh() -> String {
    let p = repo_root().join("scripts/extract-engine.sh");
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("cannot read {}: {e}", p.display()))
}

/// THE DEFAULT MODE PULLS BY DIGEST (Global Constraint 7), and three things
/// about that line are asserted rather than assumed:
///
/// 1. the digest comes from `third_party/kafka-backup-binary.digest` — the
///    checked-in pin — and not from a tag;
/// 2. the pull still carries `--platform linux/amd64`. It is REQUIRED on Apple
///    Silicon (`osodevops/kafka-backup` publishes linux/amd64 only, so without
///    it the daemon asks for arm64 and fails with "no matching manifest") and
///    a no-op on an amd64 runner. A digest conversion that drops it breaks
///    every macOS contributor;
/// 3. the `org.opencontainers.image.revision == EXPECTED_REVISION` comparison
///    survives. Under a digest pull it is NOT redundant — it becomes a
///    digest→commit binding and is the only tag-drift detector in the tree.
///
/// And the fourth, which is what makes the default mode a *pin* at all: the
/// write to the digest file happens ONLY inside the `OSO_REFRESH` branch.
#[test]
fn extract_engine_pulls_by_digest_in_the_default_mode() {
    let src = extract_engine_sh();
    let lines: Vec<&str> = src.lines().collect();

    // (1) the pin is read from the checked-in file …
    assert!(
        src.contains(r#"DIGEST_FILE="third_party/kafka-backup-binary.digest""#),
        "the script must name third_party/kafka-backup-binary.digest as its pin"
    );
    assert!(
        src.contains(r#"DIGEST="$(cat "$DIGEST_FILE")""#),
        "the default mode must read the pinned digest OUT of $DIGEST_FILE; \
         nothing in the script does"
    );

    // … and (1)+(2) the pull is by digest, on the same line as the platform.
    let pull = lines
        .iter()
        .find(|l| {
            l.contains("docker pull") && l.contains(r#""osodevops/kafka-backup@${DIGEST}""#)
        })
        .unwrap_or_else(|| {
            panic!(
                "no digest-pinned pull line (`docker pull … \"osodevops/kafka-backup@${{DIGEST}}\"`) \
                 in scripts/extract-engine.sh. A tag pull is not a pin: Docker Hub tags are \
                 mutable (Global Constraint 7)."
            )
        });
    assert!(
        pull.contains("--platform linux/amd64"),
        "the digest-pinned pull dropped `--platform linux/amd64`, which is REQUIRED on Apple \
         Silicon (upstream publishes linux/amd64 only) and a no-op on an amd64 runner. The line \
         reads: {pull}"
    );

    // (3) the revision comparison is still there, and still compares.
    assert!(
        src.contains(r#"org.opencontainers.image.revision"#)
            && src.contains(r#"if [ "$ACTUAL_REVISION" != "$EXPECTED_REVISION" ]"#),
        "the org.opencontainers.image.revision == EXPECTED_REVISION comparison must survive the \
         digest conversion: under a digest pull it is a digest→commit binding, and it is the only \
         tag-drift detector in the tree"
    );
    assert!(
        src.contains(r#"assert_revision "osodevops/kafka-backup@${DIGEST}""#),
        "the revision must be read off the DIGEST-PINNED reference — that is what makes it a \
         digest→commit binding rather than a statement about a tag"
    );

    // (4) the write to the pin is inside the OSO_REFRESH branch, and nowhere
    //     else. `REFRESH` is OSO_REFRESH and nothing else, so the branch below
    //     cannot be re-pointed at a friendlier variable.
    assert!(
        src.contains(r#"REFRESH="${OSO_REFRESH:-0}""#),
        "the refresh mode must be OSO_REFRESH, spelled once"
    );
    let branch_open = lines
        .iter()
        .position(|l| *l == r#"if [ "$REFRESH" = "1" ]; then"#)
        .expect("the OSO_REFRESH branch must open with an unindented `if [ \"$REFRESH\" = \"1\" ]; then`");
    let branch_else = branch_open
        + 1
        + lines[branch_open + 1..]
            .iter()
            .position(|l| *l == "else")
            .expect("the OSO_REFRESH branch must have an unindented `else` (the default mode)");

    let writes: Vec<usize> = lines
        .iter()
        .enumerate()
        .filter(|(_, l)| {
            let l = l.trim();
            !l.starts_with('#')
                && (l.contains(r#"> "$DIGEST_FILE""#)
                    || l.contains("> third_party/kafka-backup-binary.digest"))
        })
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        1,
        writes.len(),
        "the pinned digest must be written in exactly one place; found {:?} (1-based lines {:?})",
        writes,
        writes.iter().map(|i| i + 1).collect::<Vec<_>>()
    );
    assert!(
        writes[0] > branch_open && writes[0] < branch_else,
        "the write to the pinned-digest file (line {}) must live INSIDE the OSO_REFRESH branch \
         (lines {}..{}). Outside it, the default mode rewrites the pin it is supposed to be \
         pinned BY, and `just engine` on a stale tag silently re-points the whole repository.",
        writes[0] + 1,
        branch_open + 2,
        branch_else
    );
}

/// NO PIN, NO PULL. With `third_party/kafka-backup-binary.digest` absent the
/// default mode refuses — exit 1, naming the file and naming `OSO_REFRESH=1` as
/// the only way to create it — rather than falling back to the mutable tag.
///
/// EXECUTED, not read: the script runs against a fixture tree via LOGWEIR_ROOT.
/// It reaches no `docker` call on this path and so needs no daemon, and that is
/// asserted rather than asserted-by-comment: a `docker` shim is put FIRST on
/// PATH which records its own invocation, and the recording must not exist.
#[test]
fn extract_engine_refuses_without_a_pinned_digest() {
    let root = repo_root();
    let tmp = std::env::temp_dir().join(format!(
        "logweir-t30-no-digest-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let bin = tmp.join("bin");
    std::fs::create_dir_all(tmp.join("third_party")).unwrap();
    std::fs::create_dir_all(&bin).unwrap();

    // A `docker` that records the fact it was called. If the refusal path ever
    // reaches the daemon, this file appears and the assertion below fails.
    let marker = tmp.join("docker-was-called");
    let shim = bin.join("docker");
    std::fs::write(
        &shim,
        format!("#!/bin/sh\ntouch '{}'\nexit 0\n", marker.display()),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let out = Command::new("bash")
        .arg(root.join("scripts/extract-engine.sh"))
        .env("LOGWEIR_ROOT", &tmp)
        .env("PATH", path)
        .env_remove("OSO_REFRESH")
        .output()
        .expect("the extraction script must be runnable");

    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    let code = out.status.code();
    let _ = std::fs::remove_dir_all(&tmp);

    assert_eq!(
        Some(1),
        code,
        "the default mode must exit 1 with no pinned digest; it exited {code:?}. stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("third_party/kafka-backup-binary.digest"),
        "the refusal must NAME the missing file; it said:\n{stderr}"
    );
    assert!(
        stderr.contains("OSO_REFRESH=1"),
        "the refusal must name OSO_REFRESH=1 as the only way to create the pin; it said:\n{stderr}"
    );
    assert!(
        !marker.exists(),
        "the refusal path called `docker`. It must refuse BEFORE contacting a registry: a lint or \
         unit gate never reaches the network (STANDING RULE 7), which is what lets this test run \
         with no daemon at all."
    );
}

/// ONE INTERPRETER-RESOLUTION ORDER IN THE REPOSITORY. `just verify-py` and
/// `scripts/check-verifier-parity.sh` must resolve the Python interpreter in
/// the same order — `$LOGWEIR_PYTHON`, `$LOGWEIR_E2E_PYTHON`,
/// `.e2e/venv/bin/python3`, `python3` — so nobody has to discover a fourth name
/// for the same thing. `verify-py` used to be a hardcoded system `python3`,
/// which is precisely the interpreter that does not have `cryptography`.
///
/// FILE NOTE: Task 30's Files block permits two test files, and this is the
/// script-side one — `workflow_lint.rs` is about `.github/workflows/`, this
/// test is about `scripts/` and the `justfile`. No third test file is added
/// (Global Constraint 38).
#[test]
fn verify_py_resolves_the_interpreter_like_the_parity_gate() {
    let root = repo_root();

    let justfile = std::fs::read_to_string(root.join("justfile")).expect("justfile");
    let parity = std::fs::read_to_string(root.join("scripts/check-verifier-parity.sh"))
        .expect("scripts/check-verifier-parity.sh");

    // The `verify-py` recipe body: every indented line under the target, which
    // is one line by construction (`just` runs each line in its own shell, so
    // the resolution and the invocation cannot be split across two).
    let recipe: Vec<&str> = justfile
        .lines()
        .skip_while(|l| l.trim_end() != "verify-py:")
        .skip(1)
        .take_while(|l| l.starts_with(char::is_whitespace) && !l.trim().is_empty())
        .collect();
    assert_eq!(
        1,
        recipe.len(),
        "the verify-py recipe must be ONE line — `just` runs each recipe line in its own shell, so \
         a resolution on one line and an invocation on the next resolve nothing. It reads: {recipe:?}"
    );
    assert!(
        recipe[0].contains("docs/test_verify_scorecard.py"),
        "the verify-py recipe must run docs/test_verify_scorecard.py; it reads: {}",
        recipe[0]
    );

    // The parity gate's if/elif chain, by symbol rather than by line number.
    let start = parity
        .lines()
        .position(|l| l.contains(r#"if [ -n "${LOGWEIR_PYTHON:-}" ]"#))
        .expect("check-verifier-parity.sh must resolve $LOGWEIR_PYTHON in an if/elif chain");
    let chain: Vec<&str> = parity
        .lines()
        .skip(start)
        .take_while(|l| l.trim_end() != "fi")
        .collect();

    let from_parity = interpreter_tokens(&chain.join("\n"));
    let from_justfile = interpreter_tokens(recipe[0]);

    assert_eq!(
        from_parity, from_justfile,
        "`just verify-py` and scripts/check-verifier-parity.sh must resolve the interpreter in the \
         SAME order. The gate resolves {from_parity:?}; the recipe resolves {from_justfile:?}.\n\
         recipe: {}\nchain:\n{}",
        recipe[0],
        chain.join("\n")
    );
    assert_eq!(
        vec![
            "LOGWEIR_PYTHON",
            "LOGWEIR_E2E_PYTHON",
            ".e2e/venv/bin/python3",
            "python3"
        ],
        from_parity,
        "the resolution order itself changed; both readers agree, but on a different order than \
         the one every document in this repository states"
    );
}

/// The ordered interpreter names in `text`, consecutive repeats collapsed.
/// Longest match wins at each position, so `$ROOT/.e2e/venv/bin/python3` is the
/// venv and not a bare `python3`, and `LOGWEIR_E2E_PYTHON` is itself rather
/// than a prefix of something else.
fn interpreter_tokens(text: &str) -> Vec<String> {
    let mut candidates = [
        "LOGWEIR_E2E_PYTHON",
        "LOGWEIR_PYTHON",
        ".e2e/venv/bin/python3",
        "python3",
    ];
    candidates.sort_by_key(|c| std::cmp::Reverse(c.len()));

    let mut out: Vec<String> = Vec::new();
    let mut rest = text;
    while !rest.is_empty() {
        match candidates.iter().find(|c| rest.starts_with(**c)) {
            Some(hit) => {
                if out.last().map(String::as_str) != Some(*hit) {
                    out.push((*hit).to_string());
                }
                rest = &rest[hit.len()..];
            }
            None => {
                let step = rest.chars().next().map(char::len_utf8).unwrap_or(1);
                rest = &rest[step..];
            }
        }
    }
    out
}
