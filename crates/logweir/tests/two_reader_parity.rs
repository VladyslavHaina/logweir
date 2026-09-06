//! Two-reader parity over the invariant corpus (Tasks 4 and 5, T0-6 / T0-3).
//!
//! Logweir ships TWO readers of a signed scorecard: `logweir drill verify`
//! (Rust, `crates/logweir/src/verify.rs`) and `docs/verify_scorecard.py` (the
//! auditor's independent check). `docs/verify_scorecard.py`'s own module
//! comment says that if the two disagree "the signed-scorecard format is
//! broken, not merely this script", and `check_invariants`'s docstring claims
//! its arms mirror `Scorecard::validate_invariants` "ARM FOR ARM, IN ORDER".
//! Until Task 4 that claim was false — `partial_reason: ""` was accepted and
//! signed by Rust and refused by the script — so this file exists to make the
//! claim checkable instead of merely asserted.
//!
//! THE TWO READERS DO NOT SHARE AN EXIT-CODE SPACE and are not meant to.
//! `drill verify` follows Global Constraint 11 (`4` = signing-or-lock /
//! self-contradicting document, `crates/logweir/src/exit.rs`); the script's own
//! contract is 0 VALID / 1 INVALID / 2 could-not-run. So parity is asserted as
//!
//!   * the same VERDICT (both accept, or both refuse),
//!   * each with its own reader-specific code, taken from `index.json` so the
//!     assertion is exact and not a "non-zero" weakening, and
//!   * a BYTE-IDENTICAL invariant reason once each reader's fixed prefix is
//!     stripped.
//!
//! The corpus lives in `e2e/fixtures/invariants/` as UNSIGNED documents and is
//! signed here, at test time, into a temp dir with the checked-in throwaway
//! fixture key. That is deliberate: `drill verify` checks the signature BEFORE
//! it parses and before it calls `validate_invariants`, so an unsigned case
//! would exit `4` from the signature path and this test would pass for the
//! wrong reason. Signing here is NOT a re-mint — nothing under
//! `e2e/fixtures/signed/` is written (ruling R-G; Task 2 owns the single
//! re-mint).
//!
//! Task 5 extends this by adding entries to `index.json`, never by editing
//! this file.

use logweir_evidence::keys::SigningKey;
use logweir_evidence::sign::sign_detached;
use logweir_evidence::PAYLOAD_TYPE_SCORECARD;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::Command;

/// `docs/verify_scorecard.py` and the corpus paths are workspace-relative, so
/// both child processes are run from the workspace root.
fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/logweir has a grandparent")
        .to_path_buf()
}

fn corpus() -> PathBuf {
    root().join("e2e/fixtures/invariants")
}

/// The interpreter that can run the auditor's verifier, resolved in the SAME
/// order and by the same names as `scripts/check-verifier-parity.sh` and
/// `e2e/tests/harness/mod.rs::python`, so nobody has to discover a fourth name
/// for the same thing.
fn python() -> PathBuf {
    for var in ["LOGWEIR_PYTHON", "LOGWEIR_E2E_PYTHON"] {
        if let Ok(p) = std::env::var(var) {
            if !p.is_empty() {
                return PathBuf::from(p);
            }
        }
    }
    let venv = root().join(".e2e/venv/bin/python3");
    if venv.exists() {
        return venv;
    }
    PathBuf::from("python3")
}

/// NOT skipped when `python3` or `cryptography` is absent. A skipped parity
/// check is exactly the "documented guarantee the code does not deliver" this
/// work exists to remove: the claim would go unchecked and nothing would say
/// so. A verifier that never ran is not agreement.
fn require_python() -> PathBuf {
    let py = python();
    let probe = Command::new(&py)
        .args(["-c", "import cryptography"])
        .output()
        .unwrap_or_else(|e| {
            panic!(
                "cannot run {} — the two-reader parity claim needs BOTH readers, so this \
                 test FAILS rather than skipping. Set $LOGWEIR_PYTHON (or \
                 $LOGWEIR_E2E_PYTHON, or create .e2e/venv) to point at a python3 with the \
                 `cryptography` package. Underlying error: {e}",
                py.display()
            )
        });
    assert!(
        probe.status.success(),
        "{} cannot import `cryptography`, so docs/verify_scorecard.py cannot run \
         (pip install cryptography, or point $LOGWEIR_PYTHON at an interpreter that has \
         it). This is NOT skipped: the second reader is the point.",
        py.display()
    );
    py
}

fn read_json(path: &Path) -> Value {
    let bytes = std::fs::read(path)
        .unwrap_or_else(|e| panic!("the corpus is incomplete: {}: {e}", path.display()));
    serde_json::from_slice(&bytes)
        .unwrap_or_else(|e| panic!("{} is not valid JSON: {e}", path.display()))
}

fn entries() -> Vec<Value> {
    read_json(&corpus().join("index.json"))
        .as_array()
        .expect("index.json is a JSON array")
        .clone()
}

fn field<'a>(entry: &'a Value, key: &str) -> &'a Value {
    entry
        .get(key)
        .unwrap_or_else(|| panic!("index.json entry is missing required field {key:?}: {entry}"))
}

fn s<'a>(entry: &'a Value, key: &str) -> &'a str {
    field(entry, key)
        .as_str()
        .unwrap_or_else(|| panic!("index.json field {key:?} is not a string: {entry}"))
}

fn i(entry: &Value, key: &str) -> i64 {
    field(entry, key)
        .as_i64()
        .unwrap_or_else(|| panic!("index.json field {key:?} is not an integer: {entry}"))
}

/// The Rust reader's fixed prefix, assembled from the two places that actually
/// produce it rather than hard-coded from a brief: `crates/logweir/src/
/// verify.rs`'s `eprintln!("SIGNATURE VALID but the document is
/// self-contradicting: {e}")` wrapping `InvariantError`'s `Display`, which is
/// `#[error("scorecard invariant violated: {0}")]` in
/// `crates/logweir-core/src/scorecard.rs`.
const RUST_OUTER_PREFIX: &str = "SIGNATURE VALID but the document is self-contradicting: ";
const RUST_INNER_PREFIX: &str = "scorecard invariant violated: ";
/// `docs/verify_scorecard.py`'s single refusal form: `print(f"INVALID: {problem}")`.
const PYTHON_PREFIX: &str = "INVALID: ";

/// The first line of `stderr` that carries an invariant refusal, with this
/// reader's own prefix stripped. `None` when the reader did not refuse on an
/// invariant.
fn strip(stderr: &str, prefixes: &[&str]) -> Option<String> {
    stderr.lines().find_map(|line| {
        let mut rest = line;
        for p in prefixes {
            rest = rest.strip_prefix(p)?;
        }
        Some(rest.to_string())
    })
}

#[test]
fn two_reader_parity_over_the_invariant_corpus() {
    let py = require_python();
    let root = root();
    let key = SigningKey::from_pem_file(&root.join("e2e/fixtures/signed/signing.pem"))
        .expect("the checked-in throwaway fixture signing key");
    let pubkey = root.join("e2e/fixtures/signed/public.pem");

    let entries = entries();
    assert!(
        !entries.is_empty(),
        "e2e/fixtures/invariants/index.json is empty; a walker over nothing proves nothing"
    );

    // Every case is walked and every mismatch collected, so one run reports the
    // whole disagreement rather than only its first symptom.
    let mut failures: Vec<String> = Vec::new();

    for entry in &entries {
        let id = s(entry, "id");
        let want_rust = i(entry, "rust_exit");
        let want_python = i(entry, "python_exit");
        let want_reason = s(entry, "reason");

        // The signed payload is the file's bytes EXACTLY as written. Never
        // re-serialise after writing, or the two readers verify different bytes.
        let bytes = std::fs::read(corpus().join(s(entry, "file"))).unwrap_or_else(|e| {
            panic!("the corpus is incomplete: case {id}'s document is unreadable: {e}")
        });
        let sidecar = sign_detached(&key, PAYLOAD_TYPE_SCORECARD, &bytes).expect("sign the case");

        let dir = tempfile::tempdir().expect("tempdir");
        let sc_path = dir.path().join("case.json");
        let sig_path = dir.path().join("case.sig");
        std::fs::write(&sc_path, &bytes).expect("write the case");
        std::fs::write(&sig_path, serde_json::to_vec(&sidecar).expect("sidecar"))
            .expect("write sig");

        let rust = Command::new(env!("CARGO_BIN_EXE_logweir"))
            .current_dir(&root)
            .args(["drill", "verify", "--scorecard"])
            .arg(&sc_path)
            .arg("--signature")
            .arg(&sig_path)
            .arg("--public-key")
            .arg(&pubkey)
            .output()
            .expect("run drill verify");
        let python = Command::new(&py)
            .current_dir(&root)
            .arg("docs/verify_scorecard.py")
            .arg(&sc_path)
            .arg(&sig_path)
            .arg(&pubkey)
            .output()
            .expect("run docs/verify_scorecard.py");

        // `Output::status.code()` is the real process status. It is never read
        // through a pipe, where the pipeline's last stage would mask it.
        let rust_code = rust.status.code();
        let python_code = python.status.code();
        let rust_err = String::from_utf8_lossy(&rust.stderr).to_string();
        let python_err = String::from_utf8_lossy(&python.stderr).to_string();

        if rust_code != Some(want_rust as i32) {
            failures.push(format!(
                "{id}: drill verify exited {rust_code:?}, index.json expects {want_rust}\n\
                         rust stderr: {}",
                rust_err.trim()
            ));
        }
        if python_code != Some(want_python as i32) {
            failures.push(format!(
                "{id}: verify_scorecard.py exited {python_code:?}, index.json expects \
                 {want_python}\n        python stderr: {}",
                python_err.trim()
            ));
        }

        let rust_reason = strip(&rust_err, &[RUST_OUTER_PREFIX, RUST_INNER_PREFIX]);
        let python_reason = strip(&python_err, &[PYTHON_PREFIX]);

        if want_reason.is_empty() {
            // The accept-control. Without it a walker that only ever asserts
            // refusals passes against a reader that refuses everything.
            if rust_reason.is_some() || python_reason.is_some() {
                failures.push(format!(
                    "{id}: index.json says this document is ACCEPTED, but a reader refused it \
                     on an invariant\n        rust:   {rust_reason:?}\n        python: \
                     {python_reason:?}"
                ));
            }
            continue;
        }

        match (&rust_reason, &python_reason) {
            (Some(r), Some(p)) => {
                if r != p {
                    failures.push(format!(
                        "{id}: the two readers refuse with DIFFERENT text — the claim that \
                         they mirror each other arm for arm is false here\n        rust:   \
                         {r:?}\n        python: {p:?}"
                    ));
                }
                if r != want_reason {
                    failures.push(format!(
                        "{id}: drill verify's reason is not the one index.json records\n        \
                         got:  {r:?}\n        want: {want_reason:?}"
                    ));
                }
                if p != want_reason {
                    failures.push(format!(
                        "{id}: verify_scorecard.py's reason is not the one index.json records\n\
                         \x20       got:  {p:?}\n        want: {want_reason:?}"
                    ));
                }
            }
            _ => failures.push(format!(
                "{id}: index.json records a refusal reason, but at least one reader produced no \
                 invariant refusal line\n        rust:   {rust_reason:?}\n        python: \
                 {python_reason:?}\n        rust stderr:   {}\n        python stderr: {}",
                rust_err.trim(),
                python_err.trim()
            )),
        }
    }

    assert!(
        failures.is_empty(),
        "the two readers disagree on {} point(s) over {} corpus case(s):\n  - {}",
        failures.len(),
        entries.len(),
        failures.join("\n  - ")
    );
}

/// The body of `Scorecard::validate_invariants`, sliced from the source text
/// exactly as `awk '/pub fn validate_invariants/,/^    }$/'` does: from the
/// line carrying the signature to the first subsequent line that is exactly
/// four spaces and a closing brace.
fn validate_invariants_body() -> String {
    let src = std::fs::read_to_string(root().join("crates/logweir-core/src/scorecard.rs"))
        .expect("read scorecard.rs");
    let lines: Vec<&str> = src.lines().collect();
    let start = lines
        .iter()
        .position(|l| l.contains("pub fn validate_invariants"))
        .expect("scorecard.rs declares `pub fn validate_invariants`");
    let end = lines[start + 1..]
        .iter()
        .position(|l| *l == "    }")
        .map(|i| start + 1 + i)
        .expect("`validate_invariants` closes at column 4");
    lines[start..=end].join("\n")
}

/// Addendum A2. Every `return Err(InvariantError` STATEMENT in
/// `validate_invariants` is either named by a refusing corpus case or listed,
/// with a reason, in `uncovered-arms.json` — never both and never neither.
///
/// The unit is the STATEMENT, not the conceptual arm, because that is the unit
/// the text scan can actually measure: Task 2's evidence arm is one idea and
/// four statements. Naming each uncovered statement by its own message, rather
/// than asserting a bare number, is what keeps the accounting honest — if an
/// arm is reworded, moved or duplicated, assertion (a) fails loudly instead of
/// the arithmetic silently re-balancing.
///
/// This is the test that turns red when Task 5 adds an arm and forgets its case.
#[test]
fn every_invariant_arm_has_a_corpus_case() {
    let body = validate_invariants_body();
    let n = body.matches("return Err(InvariantError").count();

    let entries = entries();
    let mut arms: Vec<String> = entries
        .iter()
        .filter(|e| !s(e, "reason").is_empty())
        .map(|e| {
            field(e, "arm")
                .as_str()
                .unwrap_or_else(|| {
                    panic!("a refusing index.json entry's `arm` is not a string: {e}")
                })
                .to_string()
        })
        .collect();
    arms.sort();
    arms.dedup();

    let uncovered = read_json(&corpus().join("uncovered-arms.json"))
        .as_array()
        .expect("uncovered-arms.json is a JSON array")
        .clone();

    // (a) every named fragment really occurs in the body, at least
    //     `occurrences` times. A padded or stale list cannot close the
    //     arithmetic by lying about a statement that is not there.
    let mut missing: Vec<String> = Vec::new();
    for arm in &arms {
        let found = body.matches(arm.as_str()).count();
        if found == 0 {
            missing.push(format!(
                "index.json names arm {arm:?}, which appears nowhere in \
                 `validate_invariants`'s body"
            ));
        }
    }
    let mut uncovered_total = 0usize;
    for u in &uncovered {
        let message = u
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("uncovered-arms.json entry has no string `message`: {u}"));
        assert!(
            u.get("why")
                .and_then(Value::as_str)
                .is_some_and(|w| !w.is_empty()),
            "uncovered-arms.json entry for {message:?} must say WHY the corpus does not \
             carry it"
        );
        let occurrences = u
            .get("occurrences")
            .map(|v| {
                v.as_u64()
                    .unwrap_or_else(|| panic!("`occurrences` is not an integer: {u}"))
                    as usize
            })
            .unwrap_or(1);
        uncovered_total += occurrences;
        let found = body.matches(message).count();
        if found < occurrences {
            missing.push(format!(
                "uncovered-arms.json claims {occurrences} statement(s) with message \
                 {message:?}, but the body carries {found}"
            ));
        }
    }
    assert!(
        missing.is_empty(),
        "e2e/fixtures/invariants/ names arms that `validate_invariants` does not have:\n  - {}",
        missing.join("\n  - ")
    );

    // (b) an arm is covered or uncovered, never both.
    let uncovered_messages: Vec<&str> = uncovered
        .iter()
        .filter_map(|u| u.get("message").and_then(Value::as_str))
        .collect();
    let both: Vec<&String> = arms
        .iter()
        .filter(|a| uncovered_messages.contains(&a.as_str()))
        .collect();
    assert!(
        both.is_empty(),
        "these arms are BOTH covered by a corpus case and listed as uncovered: {both:?}"
    );

    // (c) the accounting closes.
    assert_eq!(
        arms.len() + uncovered_total,
        n,
        "`validate_invariants` has {n} `return Err(InvariantError` statement(s), but the \
         corpus accounts for {} ({} named by index.json cases + {} listed in \
         uncovered-arms.json). Delta {}. Every statement needs a corpus case or an entry in \
         e2e/fixtures/invariants/uncovered-arms.json saying why it has none.\n  covered: \
         {arms:?}",
        arms.len() + uncovered_total,
        arms.len(),
        uncovered_total,
        n as i64 - (arms.len() + uncovered_total) as i64,
    );
}
