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
//! this file. Task 5c did the same for six more arms — six documents, six
//! entries, no change to the walker's case handling.
//!
//! Task 5c DOES add a second walker,
//! `two_reader_parity_on_documents_refused_before_the_invariants`, over
//! `e2e/fixtures/invariants/shape-index.json`. That is not an extension of the
//! corpus by other means; it is a different claim about a different class of
//! document. A scorecard missing a whole required block is refused by Rust at
//! DESERIALISATION, so `strip` below finds no invariant line and the walker
//! above cannot express the case at all (measured: it reports "at least one
//! reader produced no invariant refusal line — rust: None"). The second walker
//! asserts what those documents CAN pin: both readers refuse, at the recorded
//! exit codes, each with its own recorded text, and NEITHER on an invariant.
//! (Only the RUST half of "neither on an invariant" is prefix-checked; the
//! script emits one `INVALID: ` prefix for both classes, so the Python half is
//! pinned by the whole-string `python_reason` equality on the line above it
//! rather than by a prefix scan. Same property, different mechanism.)
//!
//! Task 5d adds a THIRD walker, `every_required_block_has_a_shape_corpus_case`,
//! which runs no reader at all. It is the shape layer's
//! `every_invariant_arm_has_a_corpus_case`: `Scorecard`'s required blocks,
//! `docs/verify_scorecard.py`'s `REQUIRED_BLOCKS` and `shape-index.json` must
//! be the same list in the same order, with one corpus case each. It exists
//! because Task 5c's review deleted a shape check, its corpus case and its
//! pytest in ONE edit and every gate stayed green — and it catches that where
//! the invariant corpus cannot, because its fixed point is the Rust struct
//! rather than the file the check was deleted from.

//! Task 10 fix round 1 adds a FOURTH kind of test —
//! `a_whole_drill_outside_the_manifest_bound_signs_fail_integrity_and_both_readers_accept_it`
//! — which walks no corpus at all: it runs a WHOLE DRILL over the doubles and
//! puts the document that drill actually signed through both readers. It lives
//! here and not in `windowed_reconciliation.rs` (Task 10's own file) for one
//! reason: this file owns the ONE resolver for "the python3 that can run the
//! auditor's verifier" that `every_gate_resolves_the_auditors_interpreter_the_same_way`
//! checks, and a sixth resolver in a sixth file is precisely the defect that
//! test exists to prevent.

mod fixtures;

use logweir::drill::{execute_with, DrillError};
use logweir::exit::ExitCode;
use logweir_core::outcome::{IntegrityResult, Outcome};
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

/// The interpreter that can run the auditor's verifier.
///
/// Order: `$LOGWEIR_PYTHON`, then `$LOGWEIR_E2E_PYTHON`, then
/// `.e2e/venv/bin/python3`, then `python3` — the same names in the same order
/// as `scripts/check-verifier-parity.sh`, `scripts/check-invariant-corpus.sh`
/// and `e2e/tests/harness/mod.rs::python`, so nobody has to discover a fifth
/// name for the same thing and no two gates can end up checking the parity
/// claim against DIFFERENT second readers.
///
/// The harness was the outlier until Task 4 fix round 1: it read
/// `LOGWEIR_E2E_PYTHON` only, so setting `$LOGWEIR_PYTHON` — the name the
/// README and `scripts/demo.sh` document — moved every gate except that one.
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
    let entries: Vec<Value> = read_json(&corpus().join("index.json"))
        .as_array()
        .expect("index.json is a JSON array")
        .clone();
    // Ids name cases in every failure message and key the temp files in
    // `scripts/check-invariant-corpus.sh`, so a duplicate makes one gate report
    // the wrong document under the other's expectations. Task 5 adds cases;
    // this fails loudly rather than staying latent.
    let mut seen: Vec<&str> = Vec::new();
    for e in &entries {
        let id = s(e, "id");
        assert!(
            !seen.contains(&id),
            "index.json has a duplicate id {id:?}; every id must be unique"
        );
        seen.push(id);
    }
    entries
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

/// Sign `bytes` with the checked-in throwaway fixture key into a fresh temp
/// dir and run BOTH readers over the pair. Returns (rust, python) `Output`s.
///
/// The signed payload is the file's bytes EXACTLY as written; nothing is
/// re-serialised, or the two readers would verify different bytes. The
/// `TempDir` is returned with them so the caller keeps it alive.
fn run_both_readers(
    key: &SigningKey,
    py: &Path,
    root: &Path,
    bytes: &[u8],
) -> (
    tempfile::TempDir,
    std::process::Output,
    std::process::Output,
) {
    let sidecar = sign_detached(key, PAYLOAD_TYPE_SCORECARD, bytes).expect("sign the case");
    let dir = tempfile::tempdir().expect("tempdir");
    let sc_path = dir.path().join("case.json");
    let sig_path = dir.path().join("case.sig");
    std::fs::write(&sc_path, bytes).expect("write the case");
    std::fs::write(&sig_path, serde_json::to_vec(&sidecar).expect("sidecar")).expect("write sig");
    let pubkey = root.join("e2e/fixtures/signed/public.pem");

    let rust = Command::new(env!("CARGO_BIN_EXE_logweir"))
        .current_dir(root)
        .args(["drill", "verify", "--scorecard"])
        .arg(&sc_path)
        .arg("--signature")
        .arg(&sig_path)
        .arg("--public-key")
        .arg(&pubkey)
        .output()
        .expect("run drill verify");
    let python = Command::new(py)
        .current_dir(root)
        .arg("docs/verify_scorecard.py")
        .arg(&sc_path)
        .arg(&sig_path)
        .arg(&pubkey)
        .output()
        .expect("run docs/verify_scorecard.py");
    (dir, rust, python)
}

/// TWO-READER PARITY ON DOCUMENTS NEITHER READER REACHES AN INVARIANT ON —
/// the other half of the corpus, and the one `index.json` structurally cannot
/// hold.
///
/// `sample` and `evidence` are NON-optional fields of
/// `logweir_core::scorecard::Scorecard`, so a document missing one is refused
/// by `serde_json` at DESERIALISATION: `drill verify` exits 1 with `signature
/// verified but the payload is not a scorecard: missing field ...` and never
/// calls `validate_invariants`. `docs/verify_scorecard.py` has no such layer
/// and asserts the same shape in its block-presence loop instead.
///
/// Until Task 5c that loop listed `evidence` and NOT `sample`, and the gap was
/// a real disagreement, not a cosmetic one: on the very document this test
/// walks, `drill verify` exited 1 while the script printed `VALID` and exited
/// 0 — the disagreement `docs/verify_scorecard.py`'s own module comment says
/// is impossible. Task 5's §10 and addendum A1 forbade closing it and
/// `uncovered-arms.json` recorded it as a READER ASYMMETRY "pinned by nothing
/// in either direction". This test is that pin.
///
/// WHY IT IS NOT AN `index.json` CASE, measured rather than argued. Adding one
/// and running `two_reader_parity_over_the_invariant_corpus` gives:
///
/// ```text
/// probe_no_sample_block: index.json records a refusal reason, but at least one
///     reader produced no invariant refusal line
///     rust:   None
///     python: Some("the document has no sample block; it is not a drill scorecard")
/// ```
///
/// `strip` finds no line under the two Rust invariant prefixes, because there
/// is no invariant refusal to find — and `every_invariant_arm_has_a_corpus_case`
/// would additionally reject the entry's `arm`, since no such statement exists
/// in `validate_invariants`'s body. So the claim these documents carry is a
/// DIFFERENT claim, and it is asserted differently: both readers refuse, each
/// with its own recorded text, and NEITHER on an invariant. That last clause is
/// the load-bearing one — it is what says the two refusals really are the two
/// shape layers agreeing, and it is what fails if some later change smuggles
/// one of these into invariant space in only one reader.
#[test]
fn two_reader_parity_on_documents_refused_before_the_invariants() {
    let py = require_python();
    let root = root();
    let key = SigningKey::from_pem_file(&root.join("e2e/fixtures/signed/signing.pem"))
        .expect("the checked-in throwaway fixture signing key");

    let cases = shape_entries();
    assert!(
        !cases.is_empty(),
        "e2e/fixtures/invariants/shape-index.json is empty; a walker over nothing proves nothing"
    );

    let mut failures: Vec<String> = Vec::new();
    for entry in &cases {
        let id = s(entry, "id");
        let bytes = std::fs::read(corpus().join(s(entry, "file"))).unwrap_or_else(|e| {
            panic!("the corpus is incomplete: case {id}'s document is unreadable: {e}")
        });
        let (_dir, rust, python) = run_both_readers(&key, &py, &root, &bytes);

        // Never read through a pipe: `Output::status.code()` is the real status.
        let rust_code = rust.status.code();
        let python_code = python.status.code();
        let rust_err = String::from_utf8_lossy(&rust.stderr).to_string();
        let python_err = String::from_utf8_lossy(&python.stderr).to_string();

        let want_rust = i(entry, "rust_exit");
        let want_python = i(entry, "python_exit");
        if rust_code != Some(want_rust as i32) {
            failures.push(format!(
                "{id}: drill verify exited {rust_code:?}, shape-index.json expects {want_rust}\n\
                 \x20       rust stderr: {}",
                rust_err.trim()
            ));
        }
        if python_code != Some(want_python as i32) {
            failures.push(format!(
                "{id}: verify_scorecard.py exited {python_code:?}, shape-index.json expects \
                 {want_python}\n        python stderr: {}",
                python_err.trim()
            ));
        }

        // The Rust half is serde's own message. Only the STABLE part is
        // recorded — the reader, the verdict and the field it names. The
        // trailing `at line N column M` is deliberately not pinned: it moves
        // with the document's byte length and says nothing about the claim.
        let want_rust_reason = s(entry, "rust_reason");
        if !rust_err.lines().any(|l| l.starts_with(want_rust_reason)) {
            failures.push(format!(
                "{id}: drill verify's refusal is not the one shape-index.json records\n        \
                 want prefix: {want_rust_reason:?}\n        got stderr:  {}",
                rust_err.trim()
            ));
        }
        // The Python half is this repository's own wording, so it is pinned
        // WHOLE.
        let want_python_reason = s(entry, "python_reason");
        if strip(&python_err, &[PYTHON_PREFIX]).as_deref() != Some(want_python_reason) {
            failures.push(format!(
                "{id}: verify_scorecard.py's refusal is not the one shape-index.json records\n\
                 \x20       want: {want_python_reason:?}\n        got stderr: {}",
                python_err.trim()
            ));
        }

        // NEITHER reader refuses these on an INVARIANT. If one ever does, the
        // document belongs in `index.json` with an `arm`, under the walker
        // above that compares the two texts byte for byte — and this test
        // failing is how anybody finds out.
        let rust_invariant = strip(&rust_err, &[RUST_OUTER_PREFIX, RUST_INNER_PREFIX]);
        if rust_invariant.is_some() {
            failures.push(format!(
                "{id}: drill verify refused this on an INVARIANT ({rust_invariant:?}). It is now \
                 an index.json case, not a shape case"
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "the two readers disagree on {} point(s) over {} shape case(s):\n  - {}",
        failures.len(),
        cases.len(),
        failures.join("\n  - ")
    );
}

/// The `shape-index.json` entries, deduplicated on `id` exactly as `entries()`
/// does for `index.json` — the two indexes are signed into one shared temp dir
/// by `scripts/check-invariant-corpus.sh`, so a duplicate id there compares one
/// document against another's expectations.
fn shape_entries() -> Vec<Value> {
    let cases: Vec<Value> = read_json(&corpus().join("shape-index.json"))
        .as_array()
        .expect("shape-index.json is a JSON array")
        .clone();
    let mut seen: Vec<&str> = Vec::new();
    for e in &cases {
        let id = s(e, "id");
        assert!(
            !seen.contains(&id),
            "shape-index.json has a duplicate id {id:?}; every id must be unique"
        );
        seen.push(id);
    }
    cases
}

/// The lines of `text` between the first line that is exactly `open` and the
/// first subsequent line that is exactly `close`, both excluded.
///
/// Untrimmed equality on both ends, the same rule `validate_invariants_body`
/// and `resolver_body` use and for the same reason: a `trim()` would close a
/// block at the first INNER delimiter.
fn between<'a>(text: &'a str, what: &str, open: &str, close: &str) -> Vec<&'a str> {
    let lines: Vec<&str> = text.lines().collect();
    let start = lines
        .iter()
        .position(|l| *l == open)
        .unwrap_or_else(|| panic!("no line exactly {open:?} in {what}; it moved or was renamed"));
    let end = lines[start + 1..]
        .iter()
        .position(|l| *l == close)
        .map(|i| start + 1 + i)
        .unwrap_or_else(|| {
            panic!("{what}: the block opened at {open:?} is never closed by {close:?}")
        });
    lines[start + 1..end].to_vec()
}

/// THE REQUIRED BLOCKS OF `logweir_core::scorecard::Scorecard`, IN DECLARATION
/// ORDER, read out of the struct itself.
///
/// A "block" is a field whose type is EXACTLY one of the structs declared
/// beside it in `crates/logweir-core/src/scorecard.rs`. That rule is what
/// excludes `format_version` (a `String`), `last_phase_completed` (an `i8`),
/// `outcome` (an enum declared in another module), `requested_at` (a
/// `DateTime<Utc>`), `phases` (a `Vec<PhaseRecord>`) and the three `Option`
/// fields — none of which is a JSON object — while keeping the eleven that are.
/// None of the eleven carries `#[serde(default)]`, so `serde_json` refuses a
/// document missing any one of them at deserialisation.
///
/// This is the FIXED POINT the shape corpus is measured against, and it is the
/// whole reason the walker below can catch a coordinated deletion. Deleting a
/// check from `docs/verify_scorecard.py` together with its corpus case and its
/// pytest leaves the struct untouched, so the count no longer closes and the
/// walker fails — which is exactly what `uncovered-arms.json`'s own README says
/// the invariant corpus cannot do, because there `n` is counted from the same
/// file the arm is deleted from.
fn required_blocks() -> Vec<String> {
    let src = std::fs::read_to_string(root().join("crates/logweir-core/src/scorecard.rs"))
        .expect("read scorecard.rs");
    let declared: Vec<&str> = src
        .lines()
        .filter_map(|l| l.strip_prefix("pub struct "))
        .filter_map(|rest| {
            rest.split(|c: char| c == '{' || c == '(' || c == '<' || c.is_whitespace())
                .find(|t| !t.is_empty())
        })
        .collect();
    assert!(
        declared.contains(&"Scorecard"),
        "crates/logweir-core/src/scorecard.rs no longer declares `pub struct Scorecard`"
    );

    let mut blocks: Vec<String> = Vec::new();
    for line in between(&src, "scorecard.rs", "pub struct Scorecard {", "}") {
        let code = line.trim();
        let Some(decl) = code.strip_prefix("pub ") else {
            continue;
        };
        let Some((name, ty)) = decl.split_once(": ") else {
            continue;
        };
        let Some(ty) = ty.strip_suffix(',') else {
            continue;
        };
        if declared.contains(&ty) {
            blocks.push(name.to_string());
        }
    }
    assert!(
        blocks.len() >= 7,
        "only {} block field(s) found in `Scorecard`; the field-parsing rule in \
         `required_blocks` no longer matches the struct's formatting: {blocks:?}",
        blocks.len()
    );
    blocks
}

/// EVERY REQUIRED FIELD OF `logweir_core::scorecard::Scorecard`, IN DECLARATION
/// ORDER, as (name, type) — read out of the struct itself.
///
/// "Required" here means exactly what it means to serde: the field carries no
/// `#[serde(default)]`, so `serde_json` refuses a document that omits it at
/// DESERIALISATION. `required_blocks()` above is the subset whose type is a
/// struct declared in the same file; `required_non_block_fields()` below is the
/// complement, and Task 5e exists because that complement was checked by
/// neither reader (Task 5d's review, finding F3: `run_id`, `requested_at` or
/// `phases` absent was `drill verify` exit 1 against `VALID` from the script).
///
/// The `#[serde(default)]` test is on the ATTRIBUTE LINES that precede a field,
/// which is why this cannot reuse `required_blocks`'s one-line rule: a
/// `#[schemars(...)]` line must not be mistaken for one, and the flag has to be
/// cleared at each field.
fn scorecard_required_fields() -> Vec<(String, String)> {
    let src = std::fs::read_to_string(root().join("crates/logweir-core/src/scorecard.rs"))
        .expect("read scorecard.rs");
    let mut out: Vec<(String, String)> = Vec::new();
    let mut defaulted = false;
    for line in between(&src, "scorecard.rs", "pub struct Scorecard {", "}") {
        let code = line.trim();
        if code.is_empty() || code.starts_with("//") {
            continue;
        }
        if code.starts_with('#') {
            // `#[serde(default)]` and `#[serde(default, rename = ...)]` both mark
            // a field serde will synthesise, so a document may omit it.
            if code.contains("serde(default") {
                defaulted = true;
            }
            continue;
        }
        if let Some(decl) = code.strip_prefix("pub ") {
            if let Some((name, ty)) = decl.split_once(": ") {
                if let Some(ty) = ty.strip_suffix(',') {
                    if !defaulted {
                        out.push((name.to_string(), ty.to_string()));
                    }
                }
            }
        }
        defaulted = false;
    }
    assert!(
        out.len() >= 12,
        "only {} required field(s) found in `Scorecard`; the field-parsing rule in \
         `scorecard_required_fields` no longer matches the struct's formatting: {out:?}",
        out.len()
    );
    out
}

/// THE JSON TYPE A RUST TYPE IMPLIES, for a required non-block field of
/// `Scorecard` (Task 5f, from Task 5e's review's wrong-type residual).
///
/// This is the whole of the type half of the derivation, and it lives HERE — in
/// a file outside `docs/`, keyed on the struct's own type text — for the same
/// reason the name half does. `docs/verify_scorecard.py`'s `REQUIRED_FIELDS`
/// carries these strings as its second element and is compared against them; a
/// required non-block field of a Rust type this function does not map fails
/// loudly rather than reaching the Python reader with a guessed type or none.
///
/// 1.7.0 declined the type check on the grounds that "these six carry five
/// different Rust types and share no JSON shape". They share no JSON shape, but
/// each Rust type implies exactly one, which is the difference between a guess
/// and a rule: `String` and `DateTime<Utc>` are strings on the wire, `Outcome`
/// is a unit enum with `#[serde(rename_all = "kebab-case")]` and is therefore
/// also a string, `i8` is a number, and a `Vec<T>` is an array.
fn json_type_of(name: &str, rust_ty: &str) -> &'static str {
    match rust_ty {
        "String" => "string",
        // chrono serialises an RFC 3339 string; `requested_at: 5` is
        // `invalid type: integer `5`, expected ...` from serde.
        "DateTime<Utc>" => "string",
        // Declared in crates/logweir-core/src/outcome.rs, not beside `Scorecard`
        // — which is exactly why it is not a "block" — with unit variants and
        // `#[serde(rename_all = "kebab-case")]`.
        "Outcome" => "string",
        "i8" | "i16" | "i32" | "i64" | "u8" | "u16" | "u32" | "u64" => "integer",
        "bool" => "boolean",
        t if t.starts_with("Vec<") => "array",
        other => panic!(
            "`Scorecard`'s required non-block field {name:?} has Rust type {other:?}, which \
             `json_type_of` does not map to a JSON type. Add the mapping here AND in \
             `scripts/check-invariant-corpus.sh`, and give \
             `docs/verify_scorecard.py`'s REQUIRED_FIELDS the matching entry — a field \
             with no mapping is a field the Python reader type-checks by guesswork or not \
             at all."
        ),
    }
}

/// The required fields of `Scorecard` that are NOT blocks, in declaration order,
/// as (name, the JSON type its Rust type implies).
///
/// The fixed point `REQUIRED_FIELDS` in `docs/verify_scorecard.py` is measured
/// against, exactly as `required_blocks()` is the one `REQUIRED_BLOCKS` is
/// measured against. Every block must also be a required field — asserted here,
/// because the two parsers read the same struct by different rules and a silent
/// disagreement between them would weaken both lists at once. (That assertion is
/// also the ONLY thing that makes either list `#[serde(default)]`-aware where
/// `required_blocks`'s one-line rule is not: a block that gained a default would
/// leave `required_blocks` and enter neither list. `scripts/check-invariant-
/// corpus.sh` carries the same guard since Task 5f — before it, the shell gate
/// would have passed that edit while this walker failed it.)
fn required_non_block_fields() -> Vec<(String, String)> {
    let blocks = required_blocks();
    let fields = scorecard_required_fields();
    for b in &blocks {
        assert!(
            fields.iter().any(|(n, _)| n == b),
            "`required_blocks` calls {b:?} a required block but \
             `scorecard_required_fields` does not list it as a required field; the two \
             parsers disagree about `Scorecard`"
        );
    }
    let out: Vec<(String, String)> = fields
        .into_iter()
        .filter(|(n, _)| !blocks.contains(n))
        .map(|(n, ty)| {
            let json = json_type_of(&n, &ty).to_string();
            (n, json)
        })
        .collect();
    assert!(
        !out.is_empty(),
        "`Scorecard` has no required non-block field; the parsing rule no longer matches \
         the struct"
    );
    out
}

/// EVERY `u64` FIELD THE DOCUMENT CARRIES, as (dotted name, the Rust type is
/// `Option<u64>`), IN SERDE'S DECLARATION ORDER — derived from
/// `crates/logweir-core/src/scorecard.rs`.
///
/// THIS IS THE FIXED POINT `U64_FIELDS` DID NOT HAVE (Task 5d's review, finding
/// F1). 1.6.0's only struct-derived check on that list was a `def test_*` in
/// `docs/test_verify_scorecard.py` — the same file a coordinated deletion
/// touches — so deleting one entry, its pytest case and that test in one edit
/// left pytest, this walker and the corpus shell gate all green, with
/// `integrity.mismatches: 2**64` back to `drill verify` exit 1 against `VALID`
/// from the script. Read here instead, out of a file the deletion does not
/// touch.
///
/// HOW THE DOTTED NAME IS BUILT: walk `Scorecard`'s own fields in declaration
/// order; a field whose type is a struct declared in this file contributes that
/// struct's `u64` fields under the field's name, a `Vec<T>` of one contributes
/// them under `<field>[]`, and an `Option<T>` of one under `<field>`. Every
/// `u64` declared anywhere in the file must be reached by one of those three
/// shapes — a `u64` added to a struct the document reaches some other way fails
/// the closure assertion below rather than silently going unbounded on the
/// Python side.
fn rust_u64_fields() -> Vec<(String, bool)> {
    let src = std::fs::read_to_string(root().join("crates/logweir-core/src/scorecard.rs"))
        .expect("read scorecard.rs");
    let declared: Vec<&str> = src
        .lines()
        .filter_map(|l| l.strip_prefix("pub struct "))
        .filter_map(|rest| {
            rest.split(|c: char| c == '{' || c == '(' || c == '<' || c.is_whitespace())
                .find(|t| !t.is_empty())
        })
        .collect();

    // (struct, its `u64` fields in declaration order, each with its optionality)
    let mut u64_of: Vec<(&str, Vec<(String, bool)>)> = Vec::new();
    let mut current: Option<&str> = None;
    for line in src.lines() {
        if let Some(rest) = line.strip_prefix("pub struct ") {
            current = rest
                .split(|c: char| c == '{' || c == '(' || c == '<' || c.is_whitespace())
                .find(|t| !t.is_empty());
            if let Some(name) = current {
                u64_of.push((name, Vec::new()));
            }
            continue;
        }
        if line == "}" {
            current = None;
            continue;
        }
        if current.is_none() {
            continue;
        }
        let code = line.trim();
        let Some(decl) = code.strip_prefix("pub ") else {
            continue;
        };
        let Some((name, ty)) = decl.split_once(": ") else {
            continue;
        };
        let Some(ty) = ty.strip_suffix(',') else {
            continue;
        };
        let optional = match ty {
            "u64" => false,
            "Option<u64>" => true,
            _ => continue,
        };
        u64_of
            .last_mut()
            .expect("a struct is open")
            .1
            .push((name.to_string(), optional));
    }

    let mut out: Vec<(String, bool)> = Vec::new();
    let mut reached: Vec<&str> = Vec::new();
    for line in between(&src, "scorecard.rs", "pub struct Scorecard {", "}") {
        let code = line.trim();
        let Some(decl) = code.strip_prefix("pub ") else {
            continue;
        };
        let Some((name, ty)) = decl.split_once(": ") else {
            continue;
        };
        let Some(ty) = ty.strip_suffix(',') else {
            continue;
        };
        let (owner, prefix) = if declared.contains(&ty) {
            (ty, name.to_string())
        } else if let Some(inner) = ty.strip_prefix("Vec<").and_then(|t| t.strip_suffix('>')) {
            if !declared.contains(&inner) {
                continue;
            }
            (inner, format!("{name}[]"))
        } else if let Some(inner) = ty.strip_prefix("Option<").and_then(|t| t.strip_suffix('>')) {
            if !declared.contains(&inner) {
                continue;
            }
            (inner, name.to_string())
        } else {
            continue;
        };
        reached.push(owner);
        if let Some((_, fields)) = u64_of.iter().find(|(s, _)| *s == owner) {
            for (field, optional) in fields {
                out.push((format!("{prefix}.{field}"), *optional));
            }
        }
    }

    // CLOSURE, counted a second and independent way off the raw text: if a `u64`
    // is declared in a struct no `Scorecard` field reaches by one of the three
    // shapes above, it is a document field with no bound on the Python side and
    // this is where that is said out loud.
    let flat = src
        .lines()
        .filter(|l| {
            let c = l.trim();
            c.starts_with("pub ") && (c.ends_with(": u64,") || c.ends_with(": Option<u64>,"))
        })
        .count();
    let unreached: Vec<&str> = u64_of
        .iter()
        .filter(|(s, fields)| !fields.is_empty() && !reached.contains(s))
        .map(|(s, _)| *s)
        .collect();
    assert_eq!(
        out.len(),
        flat,
        "crates/logweir-core/src/scorecard.rs declares {flat} `u64` document field(s) but \
         only {} are reachable from `Scorecard` by a struct field, a `Vec<T>` or an \
         `Option<T>`. Unreached struct(s): {unreached:?}. Extend `rust_u64_fields` (and \
         `docs/verify_scorecard.py`'s `_u64_fields`) to reach them, or the new field has \
         no domain check in the Python reader at all.",
        out.len(),
    );
    out
}

/// `docs/verify_scorecard.py`'s `U64_FIELDS` tuple, in source order, parsed into
/// the same (dotted name, optional) shape `rust_u64_fields` returns.
fn python_u64_fields() -> Vec<(String, bool)> {
    let src = std::fs::read_to_string(root().join("docs/verify_scorecard.py"))
        .expect("read verify_scorecard.py");
    between(&src, "verify_scorecard.py", "U64_FIELDS = (", ")")
        .iter()
        .filter_map(|l| {
            let code = l.trim();
            let inner = code.strip_prefix('(')?.strip_suffix("),")?;
            let (name, optional) = inner.split_once(", ")?;
            let name = name.strip_prefix('"')?.strip_suffix('"')?;
            let optional = match optional {
                "True" => true,
                "False" => false,
                _ => return None,
            };
            Some((name.to_string(), optional))
        })
        .collect()
}

/// `docs/verify_scorecard.py`'s `REQUIRED_FIELDS` tuple, in source order, parsed
/// into the same (name, JSON type) shape `required_non_block_fields` returns.
///
/// The second element arrived in 1.8.0 (Task 5f). Before it the tuple was bare
/// names and the loop asserted presence only, which left `run_id: 42`,
/// `phases: "x"` and `requested_at: 5` at `drill verify` exit 1 against `VALID`
/// from the script.
fn python_required_fields() -> Vec<(String, String)> {
    let src = std::fs::read_to_string(root().join("docs/verify_scorecard.py"))
        .expect("read verify_scorecard.py");
    between(&src, "verify_scorecard.py", "REQUIRED_FIELDS = (", ")")
        .iter()
        .filter_map(|l| {
            let code = l.trim();
            let inner = code.strip_prefix('(')?.strip_suffix("),")?;
            let (name, ty) = inner.split_once(", ")?;
            let name = name.strip_prefix('"')?.strip_suffix('"')?;
            let ty = ty.strip_prefix('"')?.strip_suffix('"')?;
            Some((name.to_string(), ty.to_string()))
        })
        .collect()
}

/// `docs/verify_scorecard.py`'s `REQUIRED_BLOCKS` tuple, in source order — the
/// block-presence loop's list, hoisted to a constant so both this walker and
/// `scripts/check-invariant-corpus.sh` can read it without parsing a `for`.
fn python_required_blocks() -> Vec<String> {
    let src = std::fs::read_to_string(root().join("docs/verify_scorecard.py"))
        .expect("read verify_scorecard.py");
    between(&src, "verify_scorecard.py", "REQUIRED_BLOCKS = (", ")")
        .iter()
        .filter_map(|l| {
            let code = l.trim();
            code.strip_prefix('"')
                .and_then(|r| r.strip_suffix("\","))
                .map(str::to_string)
        })
        .collect()
}

/// `check_invariants`'s body with every whole-line `#` comment removed, for the
/// same reason `code_only` strips `//` from the Rust: a fragment that survives
/// only because a comment quotes it is not a check that exists.
fn check_invariants_body() -> String {
    let src = std::fs::read_to_string(root().join("docs/verify_scorecard.py"))
        .expect("read verify_scorecard.py");
    let lines: Vec<&str> = src.lines().collect();
    let start = lines
        .iter()
        .position(|l| l.starts_with("def check_invariants("))
        .expect("verify_scorecard.py declares `def check_invariants(`");
    let end = lines[start + 1..]
        .iter()
        .position(|l| l.starts_with("def "))
        .map(|i| start + 1 + i)
        .unwrap_or(lines.len());
    lines[start..end]
        .iter()
        .filter(|l| !l.trim_start().starts_with('#'))
        .copied()
        .collect::<Vec<_>>()
        .join("\n")
}

/// CLOSED ARITHMETIC FOR `shape-index.json` — the mirror of
/// `every_invariant_arm_has_a_corpus_case` over the shape layer (Task 5d, from
/// Task 5c's review finding F2).
///
/// The invariant arms have closed arithmetic and a per-arm Rust unit test; the
/// shape checks had neither, and the review measured what that cost: deleting
/// the `records_expected` type check from `docs/verify_scorecard.py` TOGETHER
/// WITH its corpus case and both of its pytest tests left the walker at 0, the
/// corpus shell gate at 0 (reporting "agrees … on all 22 cases", one fewer than
/// before and no complaint), pytest at 0 and `just lint` green. Deleting the
/// `integrity`, `measured` or `engine` entry from the block-presence loop was
/// silent on its own, because no corpus document was missing those blocks.
///
/// THE FIXED POINT IS THE RUST STRUCT, not either index — that is what makes
/// this catch a coordinated deletion where the invariant corpus cannot.
/// `Scorecard` declares eleven required blocks; `REQUIRED_BLOCKS` in
/// `docs/verify_scorecard.py` must name all eleven, in the same order, and
/// `shape-index.json` must carry exactly one `block:` case for each. Delete a
/// loop entry, its corpus case and its pytest in one edit and the struct still
/// says eleven: the count no longer closes and this test says so, with both
/// lists printed.
///
/// The order is asserted too, and it is not cosmetic. serde reports the first
/// missing field in declaration order, so on a document missing several blocks
/// the two readers name the same one only if this order holds — measured in the
/// review as `measured` from `drill verify` and `integrity` from the script on
/// one document. `no_measured_and_no_integrity_blocks` is the case that pins
/// it, and its `check` records which pair it is about.
///
/// `check` on a shape case says WHAT the case protects, in one of three forms:
///
/// * `block:<name>` — one of the eleven. Exactly one case per block, both ways.
/// * `field:<name>` — one of the required NON-block fields, ABSENT. Exactly one
///   case per field, both ways (`every_required_non_block_field_has_a_shape_
///   corpus_case`).
/// * `type:<name>` — one of the required NON-block fields, present but of the
///   wrong JSON type. Exactly one case per field, both ways
///   (`every_required_non_block_field_has_a_type_shape_corpus_case`, Task 5f).
/// * `null:<dotted name>` — one of the non-`Option` `u64` fields set to `null`.
///   Exactly one case per field, both ways
///   (`every_non_option_u64_field_has_a_null_shape_corpus_case`, Task 5f). This
///   is the kind that replaced three `message:` cases sharing one fragment,
///   which is Task 5e's review finding F1: the fragment pinned the LINE and
///   nothing pinned the CASES, so reverting the line and deleting all three in
///   one edit balanced.
/// * `message:<fragment>` — a literal that must appear in `check_invariants`'s
///   code (comments stripped). Several cases may name one fragment; a check
///   deleted out from under them fails here. This is the `arm` field of
///   `index.json`, playing the same role on the shape layer. It is the WEAKEST
///   kind, because it closes over the code and not over the corpus; prefer a
///   kind with its own arithmetic wherever the struct can supply one.
/// * `order:<a>,<b>[,…]` — a multi-missing document. Every name must be a
///   block, and the two recorded refusals must name the SAME block: the first
///   of them in the struct's declaration order.
#[test]
fn every_required_block_has_a_shape_corpus_case() {
    let blocks = required_blocks();
    let non_block = required_non_block_fields();
    let non_option_u64: Vec<String> = rust_u64_fields()
        .into_iter()
        .filter(|(_, optional)| !*optional)
        .map(|(n, _)| n)
        .collect();
    let loop_blocks = python_required_blocks();
    let body = check_invariants_body();
    let cases = shape_entries();

    // (a) the two lists are the same list, in the same order. Printed in full
    //     on a mismatch: a bare count tells nobody which block moved or went.
    assert_eq!(
        loop_blocks,
        blocks,
        "docs/verify_scorecard.py's REQUIRED_BLOCKS and \
         `logweir_core::scorecard::Scorecard` disagree.\n  python ({}): {loop_blocks:?}\n  \
         rust   ({}): {blocks:?}\nEvery non-optional block field of the struct must be named \
         by the block-presence loop, in the struct's own declaration order — serde reports \
         the FIRST missing field in that order, so any other order makes the two readers \
         name different blocks on a document missing several.",
        loop_blocks.len(),
        blocks.len(),
    );

    // (b) exactly one `block:` case per block, and no `block:` case naming
    //     something that is not one.
    let mut covered: Vec<String> = Vec::new();
    let mut failures: Vec<String> = Vec::new();
    for entry in &cases {
        let id = s(entry, "id");
        let check = entry
            .get("check")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("shape-index.json entry {id:?} has no string `check`"));
        let (kind, rest) = check
            .split_once(':')
            .unwrap_or_else(|| panic!("{id}: `check` {check:?} is not <kind>:<value>"));
        match kind {
            "block" => {
                if !blocks.iter().any(|b| b == rest) {
                    failures.push(format!(
                        "{id}: `check` names block {rest:?}, which `Scorecard` does not \
                         declare as a required block"
                    ));
                } else if covered.iter().any(|b| b == rest) {
                    failures.push(format!(
                        "{id}: block {rest:?} already has a shape case; exactly one per block"
                    ));
                } else {
                    covered.push(rest.to_string());
                }
            }
            "message" => {
                if !body.contains(rest) {
                    failures.push(format!(
                        "{id}: `check` names the fragment {rest:?}, which appears nowhere in \
                         `check_invariants`'s code"
                    ));
                }
            }
            "order" => {
                let named: Vec<&str> = rest.split(',').collect();
                if named.len() < 2 {
                    failures.push(format!(
                        "{id}: an `order:` case is about a document missing SEVERAL blocks; \
                         {rest:?} names {}",
                        named.len()
                    ));
                }
                let unknown: Vec<&&str> = named
                    .iter()
                    .filter(|n| !blocks.iter().any(|b| b == *n))
                    .collect();
                if !unknown.is_empty() {
                    failures.push(format!("{id}: `check` names non-blocks {unknown:?}"));
                } else {
                    // The first of them in the STRUCT's order is the block both
                    // readers must name. This asserts the recorded pair says so;
                    // `two_reader_parity_on_documents_refused_before_the_invariants`
                    // is what proves the readers really do.
                    let first = blocks
                        .iter()
                        .find(|b| named.contains(&b.as_str()))
                        .expect("at least one named block");
                    let want_python =
                        format!("the document has no {first} block; it is not a drill scorecard");
                    if s(entry, "python_reason") != want_python {
                        failures.push(format!(
                            "{id}: the recorded python_reason does not name {first:?}, the \
                             first of {named:?} in the struct's declaration order\n        \
                             got:  {:?}\n        want: {want_python:?}",
                            s(entry, "python_reason")
                        ));
                    }
                    if !s(entry, "rust_reason").ends_with(&format!("missing field `{first}`")) {
                        failures.push(format!(
                            "{id}: the recorded rust_reason does not name {first:?}: {:?}",
                            s(entry, "rust_reason")
                        ));
                    }
                }
            }
            "field" | "type" => {
                // The coverage arithmetic for these two kinds lives in
                // `every_required_non_block_field_has_a_shape_corpus_case` and
                // `every_required_non_block_field_has_a_type_shape_corpus_case`;
                // what is checked here is that the name is a real required
                // non-block field, so a typo cannot sit in the index looking
                // covered.
                if !non_block.iter().any(|(f, _)| f == rest) {
                    failures.push(format!(
                        "{id}: `check` names field {rest:?}, which `Scorecard` does not \
                         declare as a required non-block field"
                    ));
                }
            }
            "null" => {
                // Likewise: the arithmetic is
                // `every_non_option_u64_field_has_a_null_shape_corpus_case`, and
                // what is checked here is that the dotted name really is a
                // non-`Option` `u64` field of the struct.
                if !non_option_u64.iter().any(|f| f == rest) {
                    failures.push(format!(
                        "{id}: `check` names {rest:?}, which `Scorecard` does not declare \
                         as a plain (non-`Option`) `u64` document field"
                    ));
                }
            }
            other => failures.push(format!(
                "{id}: unknown `check` kind {other:?}; use block:, field:, type:, null:, \
                 message: or order:"
            )),
        }
    }

    let mut uncovered: Vec<&String> = blocks
        .iter()
        .filter(|b| !covered.iter().any(|c| c == *b))
        .collect();
    uncovered.sort();
    assert!(
        failures.is_empty() && uncovered.is_empty(),
        "the shape corpus does not account for `Scorecard`'s required blocks.\n  \
         required ({}): {blocks:?}\n  covered  ({}): {covered:?}\n  MISSING a shape case: \
         {uncovered:?}\nEvery required block needs a `shape-index.json` case whose `check` is \
         \"block:<name>\", and every such case needs a block. Deleting a check from \
         docs/verify_scorecard.py together with its case and its pytest is what this \
         arithmetic exists to catch.{}",
        blocks.len(),
        covered.len(),
        if failures.is_empty() {
            String::new()
        } else {
            format!("\n  - {}", failures.join("\n  - "))
        }
    );
}

/// CLOSED ARITHMETIC FOR THE REQUIRED NON-BLOCK FIELDS (Task 5e, from Task 5d's
/// review finding F3) — the mirror of `every_required_block_has_a_shape_corpus_
/// case` over the half of `Scorecard`'s required fields that are not blocks.
///
/// `Scorecard` has six: `format_version`, `run_id`, `outcome`,
/// `last_phase_completed`, `requested_at` and `phases`. A "block" is a field
/// whose type is a struct declared beside it, so none of these could ever join
/// `REQUIRED_BLOCKS` — and the review measured what that cost: with `run_id`,
/// `requested_at` or `phases` deleted from `unmodified_example.json` and signed,
/// `drill verify` exited 1 with `missing field ...` and
/// `docs/verify_scorecard.py` printed `VALID` and exited 0.
///
/// The fixed point is the struct, for the same reason as the block walker:
/// deleting a name from `REQUIRED_FIELDS` together with its corpus case and its
/// pytest leaves `Scorecard` saying six, so the count no longer closes and this
/// test says so with both lists printed.
#[test]
fn every_required_non_block_field_has_a_shape_corpus_case() {
    let fields = required_non_block_fields();
    let named = python_required_fields();
    let cases = shape_entries();

    assert_eq!(
        named,
        fields,
        "docs/verify_scorecard.py's REQUIRED_FIELDS and \
         `logweir_core::scorecard::Scorecard` disagree.\n  python ({}): {named:?}\n  \
         rust   ({}): {fields:?}\nEvery required field of the struct that is NOT a block \
         must be named by the field-presence loop, in the struct's own declaration order, \
         WITH the JSON type its Rust type implies (`json_type_of`). A required field is \
         one carrying no `#[serde(default)]`: serde refuses a document missing it — or \
         carrying it at the wrong type — at deserialisation, so a reader that does not \
         check it prints VALID over bytes `drill verify` exits 1 on.",
        named.len(),
        fields.len(),
    );

    let names: Vec<String> = fields.iter().map(|(n, _)| n.clone()).collect();
    let (covered, failures, uncovered) = shape_coverage(&cases, "field", &names);
    assert!(
        failures.is_empty() && uncovered.is_empty(),
        "the shape corpus does not account for `Scorecard`'s required non-block fields.\n  \
         required ({}): {names:?}\n  covered  ({}): {covered:?}\n  MISSING a shape case: \
         {uncovered:?}\nEvery required non-block field needs a `shape-index.json` case whose \
         `check` is \"field:<name>\".{}",
        names.len(),
        covered.len(),
        if failures.is_empty() {
            String::new()
        } else {
            format!("\n  - {}", failures.join("\n  - "))
        }
    );
}

/// Exactly one `shape-index.json` case per member of `universe` under `check`
/// kind `kind`, and no case under that kind naming something outside it.
///
/// Returns (covered, failures, uncovered). Three `check` kinds want precisely
/// this arithmetic over three different struct-derived universes, and writing it
/// three times is how the three drift.
fn shape_coverage(
    cases: &[Value],
    kind: &str,
    universe: &[String],
) -> (Vec<String>, Vec<String>, Vec<String>) {
    let prefix = format!("{kind}:");
    let mut covered: Vec<String> = Vec::new();
    let mut failures: Vec<String> = Vec::new();
    for entry in cases {
        let id = s(entry, "id");
        let check = entry
            .get("check")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("shape-index.json entry {id:?} has no string `check`"));
        let Some(name) = check.strip_prefix(prefix.as_str()) else {
            continue;
        };
        if !universe.iter().any(|f| f == name) {
            failures.push(format!(
                "{id}: `check` names {name:?}, which is not one of the {} name(s) \
                 `crates/logweir-core/src/scorecard.rs` supplies for kind {kind:?}",
                universe.len()
            ));
        } else if covered.iter().any(|f| f == name) {
            failures.push(format!(
                "{id}: {name:?} already has a {kind}: shape case; exactly one per name"
            ));
        } else {
            covered.push(name.to_string());
        }
    }
    let mut uncovered: Vec<String> = universe
        .iter()
        .filter(|f| !covered.iter().any(|c| c == *f))
        .cloned()
        .collect();
    uncovered.sort();
    (covered, failures, uncovered)
}

/// CLOSED ARITHMETIC FOR THE WRONG-TYPE CASES (Task 5f, from the wrong-type
/// paragraph of Task 5e's review) — one `type:` case per required non-block
/// field, the type half of what `every_required_non_block_field_has_a_shape_
/// corpus_case` does for the presence half.
///
/// 1.7.0 checked presence and recorded the type gap as a residual, and the
/// review measured it at `78bf570` on documents derived from
/// `unmodified_example.json` and signed: `run_id: 42` was `drill verify` exit 1
/// (`invalid type: integer 42, expected a string`) against `VALID` from the
/// script, `phases: "x"` and `requested_at: 5` likewise, and `outcome: 7`
/// refused here on an INVARIANT about `engine.matrix_verdict`.
///
/// The list comparison that carries the TYPES lives in the test above (they are
/// the second element of the same tuple). What this test adds is that every one
/// of them is EXERCISED by a document both readers refuse: deleting the type
/// check from `check_invariants` leaves six corpus documents printing `VALID`,
/// and deleting the six cases with it no longer balances, because `Scorecard`
/// still declares six required non-block fields.
#[test]
fn every_required_non_block_field_has_a_type_shape_corpus_case() {
    let fields = required_non_block_fields();
    let names: Vec<String> = fields.iter().map(|(n, _)| n.clone()).collect();
    let cases = shape_entries();
    let body = check_invariants_body();

    let (covered, mut failures, uncovered) = shape_coverage(&cases, "type", &names);

    // The loop must actually READ the type. A `for name in REQUIRED_FIELDS:`
    // over the pair tuple would iterate two-element tuples and every `name not
    // in doc` would be False, which is a check that passes on every document —
    // so the unpack is checked, not assumed. Collected rather than asserted on
    // its own, so a coordinated edit reports the missing cases as well as the
    // missing line.
    for fragment in [
        "for name, want in REQUIRED_FIELDS:",
        "_JSON_TYPES[want]",
        "_JSON_TYPE_WORDS[want]",
    ] {
        if !body.contains(fragment) {
            failures.push(format!(
                "docs/verify_scorecard.py's `check_invariants` no longer contains \
                 {fragment:?}, so the required non-block fields are no longer TYPE-checked. \
                 Every `type:` case is then a document this script prints VALID over while \
                 `drill verify` exits 1."
            ));
        }
    }

    assert!(
        failures.is_empty() && uncovered.is_empty(),
        "the shape corpus does not account for the TYPE of `Scorecard`'s required \
         non-block fields.\n  required ({}): {fields:?}\n  covered  ({}): {covered:?}\n  \
         MISSING a wrong-type shape case: {uncovered:?}\nEvery required non-block field \
         needs a `shape-index.json` case whose `check` is \"type:<name>\": one document \
         carrying that field at a JSON type its Rust type refuses.{}",
        names.len(),
        covered.len(),
        if failures.is_empty() {
            String::new()
        } else {
            format!("\n  - {}", failures.join("\n  - "))
        }
    );
}

/// CLOSED ARITHMETIC FOR THE OPTIONALITY FLAG'S **USE** (Task 5f, from Task 5e's
/// review finding F1) — one `null:` case per non-`Option` `u64` field.
///
/// Task 5e gave `U64_FIELDS` closed arithmetic on its LIST: the names, the order
/// and the `Option<u64>` flag are all re-derived from
/// `crates/logweir-core/src/scorecard.rs` by
/// `every_u64_field_has_the_same_domain_check_in_both_readers`. Its USE had
/// none, and the review measured exactly that. One edit at `78bf570` — revert
/// `if optional and value is None:` to `if value is None:`, leaving every flag
/// present and correct, and delete the three `null` corpus cases and both null
/// pytests — left the walker at 0, the shell gate at 0 ("all 42 cases", three
/// fewer and no complaint) and pytest at 0, with `sample.records_restored: null`
/// back to `drill verify` exit 1 against `VALID` from the script.
///
/// The three cases were `message:` cases sharing one fragment, and a `message:`
/// case closes over the CODE only: delete the line and the cases together and
/// nothing counts what went. This test counts them against the struct instead —
/// six plain `u64` fields, six `null:` cases — so the same edit fails here, with
/// the missing ones printed by name.
#[test]
fn every_non_option_u64_field_has_a_null_shape_corpus_case() {
    let plain: Vec<String> = rust_u64_fields()
        .into_iter()
        .filter(|(_, optional)| !*optional)
        .map(|(n, _)| n)
        .collect();
    assert!(
        !plain.is_empty(),
        "`Scorecard` declares no plain (non-`Option`) `u64` field; `rust_u64_fields`'s \
         parsing rule no longer matches the struct"
    );
    let cases = shape_entries();

    let (covered, mut more, uncovered) = shape_coverage(&cases, "null", &plain);

    // The guard the cases exist to exercise. A `null:` case whose line is gone
    // is a document that prints VALID, so the fragment is checked here and not
    // left to the three `message:` cases that used to carry it. Collected into
    // the same failure list rather than asserted on its own, so the one-edit
    // revert reports BOTH halves — the line that went and the cases that went
    // with it — in one message.
    let body = check_invariants_body();
    if !body.contains("if optional and value is None:") {
        more.push(
            "docs/verify_scorecard.py's `check_invariants` no longer contains `if optional \
             and value is None:`, so the `Option<u64>` flag is derived, compared, printed \
             on a mismatch — and never read. That is the one-edit revert Task 5e's review \
             measured: every `null:` case is then a document this script prints VALID over \
             while `drill verify` exits 1 with `invalid type: null, expected u64`."
                .to_string(),
        );
    }
    // Both readers must REFUSE, and the pair is recorded here so
    // `two_reader_parity_on_documents_refused_before_the_invariants` can prove
    // it against the real readers. A `null:` case recorded as an accept would
    // close the arithmetic while asserting the opposite of the claim.
    for entry in &cases {
        let id = s(entry, "id");
        let check = entry.get("check").and_then(Value::as_str).unwrap_or("");
        if !check.starts_with("null:") {
            continue;
        }
        if i(entry, "rust_exit") == 0 || i(entry, "python_exit") == 0 {
            more.push(format!(
                "{id}: a null: case records an ACCEPT (rust_exit {}, python_exit {}); a \
                 plain `u64` field set to null must be REFUSED by both readers",
                i(entry, "rust_exit"),
                i(entry, "python_exit"),
            ));
        }
    }

    assert!(
        more.is_empty() && uncovered.is_empty(),
        "the shape corpus does not account for `null` on `Scorecard`'s plain `u64` \
         fields.\n  plain u64 ({}): {plain:?}\n  covered   ({}): {covered:?}\n  MISSING a \
         null shape case: {uncovered:?}\nEvery non-`Option` `u64` field needs a \
         `shape-index.json` case whose `check` is \"null:<dotted name>\": one document \
         setting that field to null, which `serde_json` refuses with `invalid type: null, \
         expected u64`. This is what makes the optionality flag's USE checkable, not just \
         its list.{}",
        plain.len(),
        covered.len(),
        if more.is_empty() {
            String::new()
        } else {
            format!("\n  - {}", more.join("\n  - "))
        }
    );
}

/// CLOSED ARITHMETIC FOR THE u64 DOMAIN CHECKS (Task 5e, from Task 5d's review
/// finding F1), and for their NULLABILITY (finding F4).
///
/// Deliverable 2 of Task 5d bounded all eleven `u64` fields but anchored the
/// list only in `docs/test_verify_scorecard.py`, which is inside the blast
/// radius of the deletion it was meant to catch. This test reads
/// `crates/logweir-core/src/scorecard.rs` instead. Delete an entry from
/// `U64_FIELDS`, its pytest case and `test_the_u64_field_list_matches_the_rust_
/// struct` in one edit and the struct still declares eleven: the lists differ
/// and both are printed.
///
/// The `Option<u64>` flag is part of the comparison and not decoration. It is
/// what decides whether `null` is accepted, and it is the only thing standing
/// between `sample.records_restored: null` and a `VALID` banner over a document
/// `drill verify` refuses with `invalid type: null, expected u64`.
#[test]
fn every_u64_field_has_the_same_domain_check_in_both_readers() {
    let rust = rust_u64_fields();
    let python = python_u64_fields();
    assert!(
        !rust.is_empty(),
        "no `u64` document field found in crates/logweir-core/src/scorecard.rs; \
         `rust_u64_fields`'s parsing rule no longer matches the file"
    );
    assert_eq!(
        python,
        rust,
        "docs/verify_scorecard.py's U64_FIELDS and `logweir_core::scorecard::Scorecard` \
         disagree.\n  python ({}): {python:?}\n  rust   ({}): {rust:?}\nEvery `u64` field of \
         the document needs an entry, in the struct's declaration order, and the second \
         element must be `True` exactly for `Option<u64>`. Python's `int` is unbounded, so \
         a field with no entry has NO domain check at all; and a non-`Option` field marked \
         optional accepts a `null` that `serde_json` refuses with `invalid type: null, \
         expected u64`.",
        python.len(),
        rust.len(),
    );
}

/// `validate_invariants`'s body with every whole-line `//` comment removed.
///
/// Fragment matching runs against THIS, not the raw slice. The raw slice
/// carries the function's (deliberately quote-heavy) comments, so a fragment
/// that survives only because a comment happens to quote it would count as a
/// statement that exists. `n` is still derived from the raw slice exactly as
/// addendum A2 specifies, and `every_invariant_arm_has_a_corpus_case` asserts
/// the two bases agree on `n` — which is what proves no comment can inflate
/// the count either.
fn code_only(body: &str) -> String {
    body.lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
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
    let raw = validate_invariants_body();
    // Addendum A2 fixes how `n` is obtained: count the literal in the sliced
    // body. Keep that, and additionally require that stripping comments does
    // not change it — a comment that quoted the literal would otherwise inflate
    // the target the corpus has to hit.
    let n = raw.matches("return Err(InvariantError").count();
    let body = code_only(&raw);
    assert_eq!(
        body.matches("return Err(InvariantError").count(),
        n,
        "a COMMENT in `validate_invariants` contains `return Err(InvariantError`, so the \
         statement count is inflated by prose"
    );

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

/// One resolver's own body, sliced from the source text: from the line that is
/// exactly `open` to the first subsequent line that is exactly `close`.
///
/// Exact, untrimmed equality on both ends — the same rule
/// `validate_invariants_body` above uses, and for the same reason. A `trim()`
/// here would close a Rust body at the first INNER `}`.
fn resolver_body<'a>(text: &'a str, rel: &str, open: &str, close: &str) -> Vec<&'a str> {
    let mut lines = text.lines().skip_while(|l| *l != open).peekable();
    assert!(
        lines.peek().is_some(),
        "no line exactly {open:?} in {rel}; the interpreter resolution moved or was renamed"
    );
    let mut body: Vec<&str> = Vec::new();
    for line in lines {
        let end = !body.is_empty() && line == close;
        body.push(line);
        if end {
            return body;
        }
    }
    panic!("the resolver opened at {open:?} in {rel} is never closed by a line {close:?}");
}

/// THE INTERPRETER AGREEMENT, enforced structurally rather than by four doc
/// comments hoping to stay in step.
///
/// Four places in this repository resolve "the python3 that can run the
/// auditor's verifier", and each one's comment claimed all four agreed. Task
/// 4's review found that claim FALSE: `e2e/tests/harness/mod.rs::python` read
/// `$LOGWEIR_E2E_PYTHON` only, so a developer who set `$LOGWEIR_PYTHON` — the
/// name `README.md` and `scripts/demo.sh` document — got one interpreter in
/// the three parity gates and a DIFFERENT one in the e2e harness. A two-reader
/// claim checked against two different second readers is not one claim.
///
/// That is the exact defect class this stage exists to remove (a documented
/// guarantee the code does not deliver), so the fix is not "correct the
/// comment" — it is a check. No behavioural test can reach this property: two
/// of the four resolvers are shell, and the e2e harness's half needs a live
/// stack. So each resolver's own body is read as TEXT and the four names are
/// required to appear in the same order in all four.
///
/// Only executable lines are scanned. Prose about the old chain is the point of
/// the fix and must stay readable.
#[test]
fn every_gate_resolves_the_auditors_interpreter_the_same_way() {
    const ORDER: [&str; 4] = [
        "LOGWEIR_PYTHON",
        "LOGWEIR_E2E_PYTHON",
        ".e2e/venv/bin/python3",
        "python3",
    ];
    // Longest first: `.e2e/venv/bin/python3` ENDS WITH `python3`, so a
    // shortest-first scan would report the fallback where the venv is.
    let mut by_length: Vec<&str> = ORDER.to_vec();
    by_length.sort_by_key(|n| std::cmp::Reverse(n.len()));

    // (file, the line that opens the resolver, the line that closes it, the
    // language's comment marker).
    let resolvers: [(&str, &str, &str, &str); 5] = [
        (
            "crates/logweir/tests/two_reader_parity.rs",
            "fn python() -> PathBuf {",
            "}",
            "//",
        ),
        // FIVE since Task 5b: the backup receipt's corpus walker is its own
        // test binary (GC22's per-test bound), so it carries its own resolver
        // and would otherwise be the one gate nothing checked.
        (
            "crates/logweir/tests/two_reader_parity_receipt.rs",
            "fn python() -> PathBuf {",
            "}",
            "//",
        ),
        (
            "e2e/tests/harness/mod.rs",
            "fn python() -> PathBuf {",
            "}",
            "//",
        ),
        (
            "scripts/check-verifier-parity.sh",
            "if [ -n \"${LOGWEIR_PYTHON:-}\" ]; then",
            "fi",
            "#",
        ),
        (
            "scripts/check-invariant-corpus.sh",
            "if [ -n \"${LOGWEIR_PYTHON:-}\" ]; then",
            "fi",
            "#",
        ),
    ];

    for (rel, open, close, comment) in resolvers {
        let text = std::fs::read_to_string(root().join(rel))
            .unwrap_or_else(|e| panic!("{rel} is checked in: {e}"));
        let body = resolver_body(&text, rel, open, close);

        let mut seen: Vec<&str> = Vec::new();
        for line in &body {
            let code = line.trim_start();
            if code.starts_with(comment) {
                continue;
            }
            let mut i = 0usize;
            while i < code.len() {
                // `get` rather than `code[i..]`: a byte index that lands inside
                // a multi-byte character returns None instead of panicking.
                let hit = by_length
                    .iter()
                    .find(|n| code.get(i..).is_some_and(|t| t.starts_with(**n)));
                match hit {
                    Some(n) => {
                        // Consecutive repeats are one step of the chain
                        // mentioned twice (`elif [ -n "$X" ]; then PY="$X"`).
                        if seen.last() != Some(n) {
                            seen.push(*n);
                        }
                        i += n.len();
                    }
                    None => i += 1,
                }
            }
        }
        assert_eq!(
            seen,
            ORDER.to_vec(),
            "{rel} resolves the auditor's interpreter as {seen:?}, but every other gate \
             resolves it as {ORDER:?}. All FIVE must agree, or the two-reader parity claim \
             is checked against two different second readers.\nbody:\n{}",
            body.join("\n")
        );
    }
}

// ---------------------------------------------------------------------------
// Task 10 fix round 1 (review F5): a WHOLE DRILL through both readers.

/// The bound's failure text for the orchestrator fixture, spelled out here
/// rather than assembled from the same `format!` the production code uses — an
/// expectation built by the code under test cannot disagree with it.
///
/// Both instants were computed with
/// `python3 -c 'import datetime as d; print(int(d.datetime.fromisoformat(s).timestamp()*1000))'`
/// (plan errata E6/E7): `fixtures::FIXTURE_WINDOW_START` `2026-08-29T00:00:00Z`
/// = 1787961600000, which is also the archive floor plan construction binds,
/// and `FIXTURE_WINDOW_END` `2026-08-30T02:00:00Z` = 1788055200000, the
/// window's INCLUSIVE end. The fixture manifest's single segment spans exactly
/// that window and claims `FIXTURE_WINDOW_RECORDS` = 500 records, so it is
/// WHOLLY INSIDE and both bounds are 500; the target holds 400.
const WHOLE_DRILL_BOUND_FAILURE: &str = "restored 400 records but the manifest bounds the window \
                                         [1787961600000, 1788055200000] at [500, 500]";

/// **GC11 at the document level, for guard G-WIN's second half.** One whole
/// drill — every phase, over the doubles — whose restored count falls OUTSIDE
/// the manifest's bound: the document is written AND signed, says
/// `outcome: fail-integrity`, carries the bound's VERBATIM failure text in
/// `integrity.partial_reason`, the process status is **2**, and BOTH readers
/// accept the signed bytes.
///
/// Task 10's review (F5) found that chain covered only in pieces.
/// `windowed_reconciliation.rs::restored_count_is_inside_the_manifest_bound`
/// reaches exit 2 through `phase8_score::decide` and `ExitCode::from` DIRECTLY,
/// over a phase-7 outcome rather than a drill; the orchestrator's
/// write-and-sign is covered end to end for the mismatch and objective routes
/// (`a_scored_drill_that_does_not_pass_exits_2_after_running_every_phase`),
/// which the bound joins at the identical `IntegrityResult::Fail`. Nothing
/// drove a whole drill to `fail-integrity` THROUGH the count and then read the
/// signed bytes back. This does, with both readers, which is the only form in
/// which "the scorecard both readers accept says fail-integrity" is a checked
/// claim rather than a composition of two.
///
/// The two readers are exercised the way an auditor would:
///
/// * **Rust, IN PROCESS** — `logweir::verify::verify_scorecard`, which is the
///   whole of `drill verify` minus its printer (`verify::run` resolves the
///   payload type and calls exactly this), so the signature check, every
///   scorecard invariant and the derived approval claim all run.
/// * **Python, by direct call** — `docs/verify_scorecard.py` as a child
///   process over the same three files, through this file's own
///   `require_python()`, so the second reader here is the same interpreter
///   every other parity gate resolves.
///
/// Both are handed the `--out` artifact and the sidecar beside it — the files
/// an operator actually has — and the PUBLIC half of the run's signing key,
/// derived from the key the drill signed with rather than assumed to be a
/// checked-in twin of it.
///
/// Kills the mutant "let a bound failure keep `Outcome::Pass`" (and its
/// weaker cousin "report the bound in a log line instead of the document"):
/// the outcome assertion and the `partial_reason` assertion fail on the SIGNED
/// bytes, and both readers are then reading a document that no longer says
/// what happened.
#[test]
fn a_whole_drill_outside_the_manifest_bound_signs_fail_integrity_and_both_readers_accept_it() {
    // Resolved first: a parity claim that silently skipped its second reader
    // would be the "documented guarantee the code does not deliver" this file
    // exists to remove.
    let py = require_python();

    let f = fixtures::orchestrator_fixture(fixtures::Drill::RestoresOutsideTheManifestBound);
    let err = execute_with(&f.args, &f.run_id, &f.ctx)
        .expect_err("a restored count outside the manifest's bound must never report success");
    let sc = match &err {
        DrillError::NotPass(sc) => sc.clone(),
        other => panic!(
            "a count outside the bound is a drill RESULT, never an operational failure: {other:?}"
        ),
    };

    // It is a WHOLE drill: phases 6 and 7 both ran and the run was scored, so
    // this is the final gate rather than an early return.
    let phases: Vec<i8> = sc.phases.iter().map(|p| p.phase).collect();
    assert!(
        phases.contains(&6) && phases.contains(&7),
        "the count bound is read at the END of phase 7; a run that never restored proves \
         nothing about it: {phases:?}"
    );
    assert!(
        sc.measured.rto_seconds.is_some() && sc.measured.rpo_seconds.is_some(),
        "the drill must have been SCORED"
    );
    // ...and every objective was MET, so `fail-integrity` is attributable to
    // the count and to nothing else. `decide` tests `integrity.result` BEFORE
    // any objective, so without this the outcome would be the same either way
    // and the test would not know which variable produced it.
    assert_eq!(
        sc.objectives.met,
        Some(true),
        "every objective must be met, or the outcome is over-determined: {:?}",
        sc.objectives
    );
    assert_eq!(
        sc.integrity.mismatches, 0,
        "the sample must reconcile PERFECTLY: the count is the only failing variable"
    );
    assert_eq!(sc.integrity.result, IntegrityResult::Fail);

    // The exit code, read from the same `From` impl the binary uses — never
    // through a pipe (STANDING RULE 20, GC11).
    let code = ExitCode::from(err);
    assert_eq!(code, ExitCode::DrillNotPass);
    assert_eq!(
        code as i32, 2,
        "a result that is not a pass is exit 2, with the document always written and signed"
    );

    // The SIGNED BYTES, not the in-memory copy: the struct has grown phase 9
    // by now, so what the readers get is what phase 8 froze.
    let signed_bytes = std::fs::read(&f.out).expect("--out was written");
    let uploaded = f
        .ctx
        .store
        .get(&format!("logweir/drills/{}.json", f.run_id))
        .expect("phase 8 uploaded the scorecard")
        .0;
    assert_eq!(
        signed_bytes, uploaded,
        "the local artifact and the uploaded object must be the same bytes"
    );
    let on_the_wire: logweir_core::scorecard::Scorecard =
        serde_json::from_slice(&signed_bytes).expect("the signed bytes are a Scorecard");
    assert_eq!(
        on_the_wire.outcome,
        Outcome::FailIntegrity,
        "the SIGNED document must say fail-integrity"
    );
    assert_eq!(on_the_wire.outcome.wire_name(), "fail-integrity");
    let reason = on_the_wire
        .integrity
        .partial_reason
        .as_deref()
        .expect("a failing bound must say so IN THE SIGNED DOCUMENT");
    assert!(
        reason.contains(WHOLE_DRILL_BOUND_FAILURE),
        "the signed reason must carry the bound's exact failure text.\n  expected to \
         contain: {WHOLE_DRILL_BOUND_FAILURE}\n  got: {reason}"
    );

    // ---- both readers, over those exact bytes ----
    let sig_path = f.out.with_extension("sig");
    assert!(
        sig_path.exists(),
        "phase 8 writes the sidecar beside --out; without it neither reader can run"
    );
    // The PUBLIC half of the key this run signed with, written into the
    // fixture's own temp dir. Derived from the signing key the drill used, so
    // a reader accepting these bytes is evidence about THIS run.
    let pub_path = f.out.with_file_name("run-signing.pub.pem");
    fixtures::write_pub(
        &SigningKey::from_pem_file(&f.args.signing_key).expect("the run's signing key"),
        &pub_path,
    );

    // Reader 1 — Rust, in process.
    let media = logweir::verify::resolve_payload_type("scorecard").expect("the short name");
    assert_eq!(media, PAYLOAD_TYPE_SCORECARD);
    match logweir::verify::verify_scorecard(&f.out, &sig_path, &pub_path, media) {
        Ok(logweir::verify::Verdict::Scorecard(report)) => {
            assert!(
                report.signature_valid,
                "the Rust reader must find the signature valid"
            );
            assert!(
                report.invariants_ok,
                "a fail-integrity document whose count is outside the bound is a VALID \
                 document — the bound is a finding, not a contradiction"
            );
            assert_eq!(report.run_id, f.run_id);
            assert_eq!(
                report.outcome,
                Outcome::FailIntegrity,
                "the reader must report the outcome the document carries"
            );
        }
        Ok(other) => panic!("a scorecard must verify as a scorecard, got {other:?}"),
        Err(code) => panic!(
            "the Rust reader REFUSED the drill's own signed scorecard (exit {}); a document \
             Logweir signs and its own verifier rejects is the broken format \
             docs/verify_scorecard.py's module comment describes",
            code as i32
        ),
    }

    // Reader 2 — the auditor's Python verifier, by direct call. Run from the
    // workspace root because the script's own paths are relative to it.
    let out = Command::new(&py)
        .current_dir(root())
        .arg("docs/verify_scorecard.py")
        .arg(&f.out)
        .arg(&sig_path)
        .arg(&pub_path)
        .output()
        .expect("run docs/verify_scorecard.py");
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    // `Output::status.code()` is the real process status, never a pipeline's.
    assert_eq!(
        out.status.code(),
        Some(0),
        "the auditor's verifier must ACCEPT the drill's own signed scorecard\n  \
         stderr: {}\n  stdout: {}",
        stderr.trim(),
        String::from_utf8_lossy(&out.stdout).trim()
    );
    assert!(
        strip(&stderr, &[PYTHON_PREFIX]).is_none(),
        "the auditor's verifier refused an invariant on a document it exited 0 for: {stderr}"
    );
    // And the second reader really did read THIS document, rather than exiting
    // 0 over something it never parsed.
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(
        stdout.contains("fail-integrity"),
        "the auditor's verifier must report the outcome it read: {stdout}"
    );
}

// ---------------------------------------------------------------------------
// Task 10b: the normal point-in-time shape, through both readers.

/// **Task 10b guard (vi).** One whole drill — every phase, over the doubles —
/// whose archive segment STRADDLES the sample window's end, and whose
/// fingerprint set is exactly the records the window holds. It is a CORRECT
/// restore, and it must score `outcome: pass`, exit **0**, with signed bytes
/// BOTH readers accept.
///
/// This is the false negative Task 11's review reproduced on the real stack,
/// at whole-drill scale and in process. Before Task 10b,
/// `phase7_verify::verdict_for_selection` derived `claimed` from every
/// OVERLAPPING segment's whole `record_count` — `min(25, 3) = 3` here — while
/// the archive can only offer the 2 records inside the window, so
/// `compared >= claimed` was unreachable, the selection was `Unverified`
/// ("a short sample is coverage the drill did not obtain"), and the run signed
/// `fail-integrity` with `mismatches: 0` about its own sample. Measured
/// 2026-09-10 against engine 0.21.0 (`sha256:8ff5be71…`) on Kafka 3.7.1, on
/// all three partitions of a three-partition `point_in_time` restore.
///
/// Every other variable is pinned so the verdict can come from `claimed`
/// alone: the sample reconciles 2/2, the restored count (2) is inside the
/// manifest's `[0, 3]` bound, and every objective is met.
///
/// `sample.records_expected` is deliberately NOT part of the fix and is
/// asserted here at its uncapped value: `phase4_sample` leaves it as the
/// canary the drill set out to reconcile (25), the reconciliation compares
/// against `claimed` and never against it, and a pass over 2 sampled records
/// out of a 25-record ask is the correct signed document — the same 6-of-75
/// shape the real stack produced.
///
/// Kills mutant **M1** (restore the overlap sum): the run goes back to
/// `fail-integrity` and exit 2, and both readers are then handed a document
/// that says a correct restore failed.
#[test]
fn a_whole_drill_sampling_across_a_straddling_segment_signs_pass_and_both_readers_accept_it() {
    // Resolved first: a parity claim that silently skipped its second reader
    // would be the "documented guarantee the code does not deliver" this file
    // exists to remove.
    let py = require_python();

    let f = fixtures::orchestrator_fixture(fixtures::Drill::SamplesAcrossAStraddlingSegment);
    let sc = execute_with(&f.args, &f.run_id, &f.ctx).unwrap_or_else(|e| {
        panic!(
            "a restore that returned every record its window holds is a PASS; the drill \
             refused it: {e:?}"
        )
    });

    // It is a WHOLE drill: phases 6 and 7 both ran and the run was scored.
    let phases: Vec<i8> = sc.phases.iter().map(|p| p.phase).collect();
    assert!(
        phases.contains(&6) && phases.contains(&7),
        "the claim is read at the END of phase 7; a run that never restored proves \
         nothing about it: {phases:?}"
    );
    assert!(
        sc.measured.rto_seconds.is_some() && sc.measured.rpo_seconds.is_some(),
        "the drill must have been SCORED"
    );
    assert_eq!(sc.outcome, Outcome::Pass);
    assert_eq!(sc.integrity.result, IntegrityResult::Pass);
    assert_eq!(
        (
            sc.integrity.records_sampled,
            sc.integrity.records_sampled_matching,
            sc.integrity.mismatches
        ),
        (
            fixtures::STRADDLER_IN_WINDOW_RECORDS as u64,
            fixtures::STRADDLER_IN_WINDOW_RECORDS as u64,
            0
        ),
        "every record the window holds was reconciled and all of them matched: {:?}",
        sc.integrity
    );
    assert_eq!(
        sc.integrity.partial_reason, None,
        "a pass may carry no finding; the short-sample text is the one that used to be here"
    );
    assert_eq!(
        sc.sample.records_expected,
        fixtures::FIXTURE_SAMPLE_RECORDS as u64,
        "`sample.records_expected` is the canary the drill ASKED for and is deliberately \
         uncapped by what the window can supply (phase4_sample; phase7_verify's own doc \
         says so). Task 10b does not touch it: a pass over {} of {} is the correct \
         document, not a contradiction",
        sc.integrity.records_sampled,
        sc.sample.records_expected
    );
    assert_eq!(sc.objectives.met, Some(true));

    // The exit code, read from the same `From` impl the binary uses — never
    // through a pipe (STANDING RULE 20, GC11).
    assert_eq!(
        ExitCode::Ok as i32,
        0,
        "a pass is exit 0 (GC11); `execute_with` returning Ok rather than \
         `DrillError::NotPass` IS that path, and the failing shape above takes the other one"
    );

    // The SIGNED BYTES, not the in-memory copy.
    let signed_bytes = std::fs::read(&f.out).expect("--out was written");
    let uploaded = f
        .ctx
        .store
        .get(&format!("logweir/drills/{}.json", f.run_id))
        .expect("phase 8 uploaded the scorecard")
        .0;
    assert_eq!(
        signed_bytes, uploaded,
        "the local artifact and the uploaded object must be the same bytes"
    );
    let on_the_wire: logweir_core::scorecard::Scorecard =
        serde_json::from_slice(&signed_bytes).expect("the signed bytes are a Scorecard");
    assert_eq!(
        on_the_wire.outcome,
        Outcome::Pass,
        "the SIGNED document must say pass"
    );
    assert_eq!(on_the_wire.outcome.wire_name(), "pass");
    assert_eq!(on_the_wire.integrity.partial_reason, None);

    // ---- both readers, over those exact bytes ----
    let sig_path = f.out.with_extension("sig");
    assert!(
        sig_path.exists(),
        "phase 8 writes the sidecar beside --out; without it neither reader can run"
    );
    let pub_path = f.out.with_file_name("run-signing.pub.pem");
    fixtures::write_pub(
        &SigningKey::from_pem_file(&f.args.signing_key).expect("the run's signing key"),
        &pub_path,
    );

    // Reader 1 — Rust, in process.
    let media = logweir::verify::resolve_payload_type("scorecard").expect("the short name");
    assert_eq!(media, PAYLOAD_TYPE_SCORECARD);
    match logweir::verify::verify_scorecard(&f.out, &sig_path, &pub_path, media) {
        Ok(logweir::verify::Verdict::Scorecard(report)) => {
            assert!(
                report.signature_valid,
                "the Rust reader must find the signature valid"
            );
            assert!(
                report.invariants_ok,
                "a pass over 2 of a 25-record ask is a VALID document: `records_sampled` \
                 may not EXCEED `records_expected`, and 2 does not"
            );
            assert_eq!(report.run_id, f.run_id);
            assert_eq!(report.outcome, Outcome::Pass);
        }
        Ok(other) => panic!("a scorecard must verify as a scorecard, got {other:?}"),
        Err(code) => panic!(
            "the Rust reader REFUSED the drill's own signed scorecard (exit {}); a document \
             Logweir signs and its own verifier rejects is the broken format \
             docs/verify_scorecard.py's module comment describes",
            code as i32
        ),
    }

    // Reader 2 — the auditor's Python verifier, by direct call.
    let out = Command::new(&py)
        .current_dir(root())
        .arg("docs/verify_scorecard.py")
        .arg(&f.out)
        .arg(&sig_path)
        .arg(&pub_path)
        .output()
        .expect("run docs/verify_scorecard.py");
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    // `Output::status.code()` is the real process status, never a pipeline's.
    assert_eq!(
        out.status.code(),
        Some(0),
        "the auditor's verifier must ACCEPT the drill's own signed scorecard\n  \
         stderr: {}\n  stdout: {}",
        stderr.trim(),
        String::from_utf8_lossy(&out.stdout).trim()
    );
    assert!(
        strip(&stderr, &[PYTHON_PREFIX]).is_none(),
        "the auditor's verifier refused an invariant on a document it exited 0 for: {stderr}"
    );
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(
        stdout.contains("pass"),
        "the auditor's verifier must report the outcome it read: {stdout}"
    );
}
