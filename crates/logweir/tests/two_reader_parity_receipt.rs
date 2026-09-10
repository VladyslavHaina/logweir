//! Two-reader parity over the BACKUP RECEIPT corpus (Task 5b).
//!
//! The receipt's twin of `crates/logweir/tests/two_reader_parity.rs`, and a
//! SEPARATE TEST BINARY on purpose (critique A F18). That file shells the
//! Python reader once per case over fifteen-plus documents and measures 5–12 s;
//! Global Constraint 22's bound is **per `#[test]`** and is 15 s
//! (`scripts/time-unit-suite.sh:35`), so seven more documents in the same
//! `#[test]` would land at ≈17 s. Two binaries, two `#[test]`s, two measured
//! wall clocks — both recorded in the task report.
//!
//! # What is asserted here, and what is asserted elsewhere
//!
//! * `two_reader_parity_over_the_backup_receipt_corpus` — every case in
//!   `e2e/fixtures/invariants/backup-receipt-index.json` reaches the recorded
//!   `rust_exit` and `python_exit` from the two readers, **and** both readers
//!   produce the recorded refusal text, byte for byte, once each reader's own
//!   prefix is stripped.
//! * `the_two_payload_type_resolvers_agree` — the two `resolve_payload_type`
//!   implementations are one contract: the same four short names mapping to
//!   the same four media types, a full media type passing through in both, and
//!   byte-identical error text on a value neither accepts.
//! * `the_corpus_index_carries_the_arm_field` — interface **I31**'s six-field
//!   shape, and `arm` byte-equal to the reason its arm returns.
//! * `the_receipt_window_and_the_crds_window_are_both_half_open` — the F3
//!   agreement: `covered.to_ms` and `status.windowCovered.toMs` describe the
//!   same range under the same rule, which is the whole point of moving arm 4
//!   to a strict `<`.
//!
//! The ARM ARITHMETIC — every arm of `BackupReceipt::validate_invariants`
//! accounted for by a corpus case, and the two readers implementing the same
//! four arms — is `scripts/check-invariant-corpus.sh`'s, because it must be
//! runnable by an auditor with python3 and no Rust toolchain. The per-arm
//! message assertions are `crates/logweir-core/tests/backup_receipt.rs`'s,
//! which is the only thing that survives an arm deleted from BOTH readers
//! together with its corpus case (`e2e/fixtures/invariants/README.md` argues
//! that limit in full for the scorecard corpus; it holds identically here).
//!
//! Global Constraint 22: no test here dials a socket. Both readers are child
//! processes over local files, and STANDING RULE 18's dial-token audit is
//! unaffected — this file names no client constructor and no compose endpoint.

use logweir_evidence::keys::SigningKey;
use logweir_evidence::sign::sign_detached;
use logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT;
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
/// as `scripts/check-verifier-parity.sh`, `scripts/check-invariant-corpus.sh`,
/// `crates/logweir/tests/two_reader_parity.rs::python` and
/// `e2e/tests/harness/mod.rs::python`, so no two gates can check the parity
/// claim against DIFFERENT second readers.
/// `two_reader_parity.rs::every_gate_resolves_the_auditors_interpreter_the_same_way`
/// is what keeps that true; this resolver is written in the same shape for it
/// to recognise.
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
    read_json(&corpus().join("backup-receipt-index.json"))
        .as_array()
        .expect("backup-receipt-index.json is a JSON array")
        .clone()
}

fn field<'a>(entry: &'a Value, key: &str) -> &'a Value {
    entry.get(key).unwrap_or_else(|| {
        panic!("backup-receipt-index.json entry is missing required field {key:?}: {entry}")
    })
}

fn s<'a>(entry: &'a Value, key: &str) -> &'a str {
    field(entry, key)
        .as_str()
        .unwrap_or_else(|| panic!("field {key:?} is not a string: {entry}"))
}

fn i(entry: &Value, key: &str) -> i64 {
    field(entry, key)
        .as_i64()
        .unwrap_or_else(|| panic!("field {key:?} is not an integer: {entry}"))
}

/// The Rust reader's prefix in front of a self-contradicting document's reason,
/// taken from the one place that produces it: `crates/logweir/src/verify.rs`'s
/// `eprintln!("SIGNATURE VALID but the document is self-contradicting: {e}")`.
///
/// There is no inner prefix for this document type, unlike the scorecard's
/// `scorecard invariant violated: `: `BackupReceipt::validate_invariants`
/// returns a bare `String`, because the string IS the interface the second
/// reader reproduces byte for byte and a wrapper type would put a prefix in
/// front of it that Python has no way to reproduce.
const RUST_PREFIX: &str = "SIGNATURE VALID but the document is self-contradicting: ";
/// `docs/verify_scorecard.py`'s single refusal form: `print(f"INVALID: {problem}")`.
const PYTHON_PREFIX: &str = "INVALID: ";

/// The first line of `stderr` carrying `prefix`, with it stripped. `None` when
/// the reader did not refuse.
fn strip(stderr: &str, prefix: &str) -> Option<String> {
    stderr
        .lines()
        .find_map(|line| line.strip_prefix(prefix).map(str::to_string))
}

/// Sign `bytes` under the RECEIPT media type with the checked-in throwaway
/// fixture key and run both readers over the pair.
///
/// The signed payload is the file's bytes EXACTLY as written; nothing is
/// re-serialised, or the two readers would verify different bytes. Signing
/// under the receipt's own media type is not a detail: a receipt signed under
/// the scorecard's type is refused at the `payloadType` comparison, and every
/// case would then "agree" for the wrong reason.
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
    let sidecar =
        sign_detached(key, PAYLOAD_TYPE_BACKUP_RECEIPT, bytes).expect("sign the receipt case");
    let dir = tempfile::tempdir().expect("tempdir");
    let doc_path = dir.path().join("case.json");
    let sig_path = dir.path().join("case.sig");
    std::fs::write(&doc_path, bytes).expect("write the case");
    std::fs::write(&sig_path, serde_json::to_vec(&sidecar).expect("sidecar")).expect("write sig");
    let pubkey = root.join("e2e/fixtures/signed/public.pem");

    let rust = Command::new(env!("CARGO_BIN_EXE_logweir"))
        .current_dir(root)
        .args([
            "drill",
            "verify",
            "--payload-type",
            "backup-receipt",
            "--scorecard",
        ])
        .arg(&doc_path)
        .arg("--signature")
        .arg(&sig_path)
        .arg("--public-key")
        .arg(&pubkey)
        .output()
        .expect("run drill verify");
    let python = Command::new(py)
        .current_dir(root)
        .arg("docs/verify_scorecard.py")
        .args(["--payload-type", "backup-receipt"])
        .arg(&doc_path)
        .arg(&sig_path)
        .arg(&pubkey)
        .output()
        .expect("run docs/verify_scorecard.py");
    (dir, rust, python)
}

#[test]
fn two_reader_parity_over_the_backup_receipt_corpus() {
    let py = require_python();
    let root = root();
    let key = SigningKey::from_pem_file(&root.join("e2e/fixtures/signed/signing.pem"))
        .expect("the checked-in throwaway fixture signing key");

    let entries = entries();
    assert!(
        !entries.is_empty(),
        "e2e/fixtures/invariants/backup-receipt-index.json is empty; a walker over \
         nothing proves nothing"
    );

    // Every case is walked and every mismatch collected, so one run reports the
    // whole disagreement rather than only its first symptom.
    let mut failures: Vec<String> = Vec::new();

    for entry in &entries {
        let id = s(entry, "id");
        let want_rust = i(entry, "rust_exit");
        let want_python = i(entry, "python_exit");
        let want_reason = s(entry, "reason");

        let bytes = std::fs::read(corpus().join(s(entry, "file"))).unwrap_or_else(|e| {
            panic!("the corpus is incomplete: case {id}'s document is unreadable: {e}")
        });
        let (_dir, rust, python) = run_both_readers(&key, &py, &root, &bytes);

        // `Output::status.code()` is the real process status, never read
        // through a pipe where the pipeline's last stage would mask it.
        let rust_code = rust.status.code();
        let python_code = python.status.code();
        let rust_err = String::from_utf8_lossy(&rust.stderr).to_string();
        let python_err = String::from_utf8_lossy(&python.stderr).to_string();

        if rust_code != Some(want_rust as i32) {
            failures.push(format!(
                "{id}: drill verify exited {rust_code:?}, backup-receipt-index.json expects \
                 {want_rust}\n        rust stderr: {}",
                rust_err.trim()
            ));
        }
        if python_code != Some(want_python as i32) {
            failures.push(format!(
                "{id}: verify_scorecard.py exited {python_code:?}, \
                 backup-receipt-index.json expects {want_python}\n        python stderr: {}",
                python_err.trim()
            ));
        }

        let rust_reason = strip(&rust_err, RUST_PREFIX);
        let python_reason = strip(&python_err, PYTHON_PREFIX);

        if want_reason.is_empty() {
            // The accept-control. Without it a walker that only ever asserts
            // refusals passes against a reader that refuses everything.
            if rust_reason.is_some() || python_reason.is_some() {
                failures.push(format!(
                    "{id}: the index says this receipt is ACCEPTED, but a reader refused \
                     it\n        rust:   {rust_reason:?}\n        python: {python_reason:?}"
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
                        "{id}: drill verify's reason is not the one the index records\n     \
                         \x20  got:  {r:?}\n        want: {want_reason:?}"
                    ));
                }
                if p != want_reason {
                    failures.push(format!(
                        "{id}: verify_scorecard.py's reason is not the one the index \
                         records\n        got:  {p:?}\n        want: {want_reason:?}"
                    ));
                }
            }
            _ => failures.push(format!(
                "{id}: the index records a refusal reason, but at least one reader produced \
                 no refusal line\n        rust:   {rust_reason:?}\n        python: \
                 {python_reason:?}\n        rust stderr:   {}\n        python stderr: {}",
                rust_err.trim(),
                python_err.trim()
            )),
        }
    }

    assert!(
        failures.is_empty(),
        "the two readers disagree on {} point(s) over {} backup-receipt corpus case(s):\n  \
         - {}",
        failures.len(),
        entries.len(),
        failures.join("\n  - ")
    );
}

/// `PAYLOAD_TYPES` as `docs/verify_scorecard.py` declares it, read out of the
/// SOURCE TEXT rather than by importing the module.
///
/// Reading the source is the point: the claim is about the shipped script's own
/// declaration, and an import would additionally need a python3 with
/// `cryptography` for a question that is pure text.
fn python_payload_types() -> Vec<(String, String)> {
    let src = std::fs::read_to_string(root().join("docs/verify_scorecard.py"))
        .expect("read docs/verify_scorecard.py");
    let body = src
        .split_once("PAYLOAD_TYPES = {")
        .expect("docs/verify_scorecard.py declares PAYLOAD_TYPES")
        .1
        .split_once("\n}")
        .expect("the PAYLOAD_TYPES dict closes at column 0")
        .0;
    let mut out = Vec::new();
    for line in body.lines() {
        let line = line.trim();
        // `"scorecard": PAYLOAD_TYPE,` names the constant declared above the
        // dict; the other three carry their literal.
        let Some((key, value)) = line.split_once(": ") else {
            continue;
        };
        let key = key.trim().trim_matches('"');
        let value = value.trim().trim_end_matches(',').trim_matches('"');
        if key.is_empty() || key.starts_with('#') {
            continue;
        }
        let value = if value == "PAYLOAD_TYPE" {
            // The scorecard's media type, taken from ITS declaration in the
            // same file, so this helper never carries a media type of its own.
            src.split_once("PAYLOAD_TYPE = \"")
                .expect("docs/verify_scorecard.py declares PAYLOAD_TYPE")
                .1
                .split_once('"')
                .expect("the literal closes")
                .0
                .to_string()
        } else {
            value.to_string()
        };
        out.push((key.to_string(), value));
    }
    out
}

/// The `TYPES` table as `crates/logweir/src/verify.rs` declares it, read out of
/// the source text — the Rust half of the same question, and the way this test
/// notices a FIFTH entry on either side rather than only a changed mapping.
fn rust_payload_types() -> Vec<(String, String)> {
    let src = std::fs::read_to_string(root().join("crates/logweir/src/verify.rs"))
        .expect("read crates/logweir/src/verify.rs");
    let verify_src = std::fs::read_to_string(root().join("crates/logweir-verify/src/lib.rs"))
        .expect("read crates/logweir-verify/src/lib.rs");
    let body = src
        .split_once("const TYPES: [(&str, &str); 4] = [")
        .expect("crates/logweir/src/verify.rs declares the TYPES table")
        .1
        .split_once("];")
        .expect("the TYPES table closes")
        .0;
    let mut out = Vec::new();
    for line in body.lines() {
        let line = line.trim();
        if !line.starts_with('(') {
            continue;
        }
        let inner = line.trim_start_matches('(').trim_end_matches("),");
        let (short, constant) = inner.split_once(", ").expect("a (short, CONSTANT) pair");
        // The media type is resolved from the crate that DECLARES the
        // constants, so this helper carries no media-type literal either and a
        // constant moved to another crate is a failure here rather than a
        // silent agreement.
        let decl = format!("pub const {}: &str =", constant.trim());
        let value = verify_src
            .split_once(&decl)
            .unwrap_or_else(|| {
                panic!(
                    "crates/logweir-verify/src/lib.rs does not declare {}; the four media \
                     types are declared in the VERIFY-ONLY crate (chain V, Task 14) and \
                     nowhere else",
                    constant.trim()
                )
            })
            .1
            .split_once('"')
            .expect("the literal opens")
            .1
            .split_once('"')
            .expect("the literal closes")
            .0
            .to_string();
        out.push((short.trim_matches('"').to_string(), value));
    }
    out
}

#[test]
fn the_two_payload_type_resolvers_agree() {
    let py = require_python();
    let root = root();

    // (a) the same four keys mapping to the same four values.
    //
    // Compared as SETS (sorted), because the two declarations are ordered for
    // two different reasons and both are right: Python's dict leads with
    // `scorecard`, which is the DEFAULT payload type, while Rust's table is in
    // the sorted order its own error message lists. The order of the Rust
    // table is asserted separately below, against that message.
    let rust = rust_payload_types();
    let python = python_payload_types();
    assert_eq!(rust.len(), 4, "the Rust map has four entries: {rust:?}");
    let mut rust_sorted = rust.clone();
    rust_sorted.sort();
    let mut python_sorted = python.clone();
    python_sorted.sort();
    assert_eq!(
        rust_sorted, python_sorted,
        "the two readers' payload-type maps differ. They must have the same four short \
         names mapping to the same four media types."
    );
    assert_eq!(
        rust, rust_sorted,
        "`crates/logweir/src/verify.rs`'s TYPES table must be declared in SORTED order: \
         `resolve_payload_type`'s error message lists the short names by walking it, and \
         the Python half produces that list with `\", \".join(sorted(PAYLOAD_TYPES))`. \
         Reordering the table changes the message and breaks the byte-identity asserted \
         below."
    );

    // (b) every short name and every full media type resolves, in Rust, to the
    //     value Python's map holds — and the full media type PASSES THROUGH,
    //     which is the clause Task 5 deliberately left to this test.
    for (short, media) in &python {
        assert_eq!(
            logweir::verify::resolve_payload_type(short),
            Ok(media.as_str()),
            "the Rust resolver disagrees with docs/verify_scorecard.py on {short:?}"
        );
        assert_eq!(
            logweir::verify::resolve_payload_type(media),
            Ok(media.as_str()),
            "a FULL media type must pass through both resolvers unchanged: {media:?}"
        );
    }

    // (c) byte-identical error text on a value neither accepts.
    //
    //     `not-a-type` is the brief's row. The three quoting cases beside it
    //     are what make the claim more than an accident: Rust's `{:?}` quotes
    //     with `"` and Python's `!r` prefers `'` and switches to `"` when the
    //     value contains a `'` and no `"`, so a single value with no quote in
    //     it would have passed under either convention.
    for value in [
        "not-a-type",
        "backup_receipt",
        "it's",
        "say \"hi\"",
        "both'\"",
    ] {
        let rust_err = match logweir::verify::resolve_payload_type(value) {
            Ok(t) => panic!("the Rust resolver accepted {value:?} as {t:?}"),
            Err(e) => e,
        };
        let out = Command::new(&py)
            .current_dir(&root)
            .arg("docs/verify_scorecard.py")
            .args(["--payload-type", value])
            .arg("e2e/fixtures/signed/backup-receipt.json")
            .arg("e2e/fixtures/signed/backup-receipt.sig")
            .arg("e2e/fixtures/signed/public.pem")
            .output()
            .expect("run docs/verify_scorecard.py");
        assert_eq!(
            out.status.code(),
            Some(1),
            "an unknown --payload-type is a bad command line, not a bad artifact"
        );
        let python_err = String::from_utf8_lossy(&out.stderr)
            .lines()
            .find_map(|l| l.strip_prefix(PYTHON_PREFIX).map(str::to_string))
            .unwrap_or_else(|| {
                panic!(
                    "the script must refuse {value:?} in its single INVALID form, got: {}",
                    String::from_utf8_lossy(&out.stderr)
                )
            });
        assert_eq!(
            rust_err, python_err,
            "the two resolvers refuse {value:?} with DIFFERENT text. The message is the \
             interface: `unknown --payload-type <repr>; use one of <sorted short names> \
             or a full media type`, with CPython's quoting on both sides."
        );
    }

    // (d) and the Rust resolver really ERRORS on an unknown value rather than
    //     passing it through — the mutant this row kills is a resolver whose
    //     fallback returns its input.
    assert!(
        logweir::verify::resolve_payload_type("not-a-type").is_err(),
        "an unknown --payload-type must be an error, never a passthrough of anything that \
         happens to contain a slash"
    );
}

#[test]
fn the_corpus_index_carries_the_arm_field() {
    // Interface **I31**: SIX fields, and `arm` on every entry — including the
    // accept-control, where it is the empty string beside an empty `reason`.
    //
    // Critique A F25: the scorecard corpus index already had six fields and a
    // five-field shape was named for it by mistake. `arm` is the join the
    // closed arithmetic in `scripts/check-invariant-corpus.sh` uses; naming a
    // five-field shape would leave that arithmetic nothing to join on.
    const SIX: [&str; 6] = ["id", "file", "rust_exit", "python_exit", "reason", "arm"];
    let entries = entries();
    let mut ids: Vec<&str> = Vec::new();
    for entry in &entries {
        let object = entry
            .as_object()
            .unwrap_or_else(|| panic!("a backup-receipt-index.json entry is not an object"));
        let mut got: Vec<&str> = object.keys().map(String::as_str).collect();
        got.sort();
        let mut want = SIX;
        want.sort();
        assert_eq!(
            got,
            want.to_vec(),
            "backup-receipt-index.json entry {:?} does not carry exactly the six fields \
             interface I31 fixes",
            object.get("id")
        );

        let id = s(entry, "id");
        assert!(
            !ids.contains(&id),
            "duplicate id {id:?}: ids key the temp files every gate signs into, so a \
             duplicate makes one gate compare the wrong document against another's \
             expectations"
        );
        ids.push(id);

        // `arm` IS the refusal text for this corpus. The receipt's four
        // messages interpolate — a `format_version`, an exit code, two topic
        // sets, two timestamps — so unlike the scorecard's `arm` there is no
        // shorter verbatim fragment of the source to name, and the join in
        // `scripts/check-invariant-corpus.sh` is on the message SKELETON
        // instead. Keeping the two fields byte-equal is what makes that join
        // well-defined.
        assert_eq!(
            s(entry, "arm"),
            s(entry, "reason"),
            "{id}: `arm` must be byte-equal to the reason string its arm returns"
        );

        // The document exists and is a JSON object. A corpus entry naming a
        // file nobody wrote is a case that never runs.
        let file = corpus().join(s(entry, "file"));
        assert!(
            read_json(&file).is_object(),
            "{}: a corpus case must be a JSON object",
            file.display()
        );
    }
    assert!(
        entries.iter().any(|e| s(e, "reason").is_empty()),
        "the corpus needs its ACCEPT case: without one, a reader that refused every \
         receipt would walk it green"
    );
}

/// **Task 5's review, finding F3 — the agreement, pinned.**
///
/// `config/crd/backups.yaml` documents `status.windowCovered.toMs` as the
/// EXCLUSIVE end of the covered window, and Task 17's reconciler copies the
/// receipt's two integers into exactly those two fields. Task 5 shipped the
/// receipt's arm 4 as `<=`, with a test asserting `from_ms == to_ms` was a
/// legal "instantaneous window" — so the two documents described one range
/// under rules that disagreed, and nothing in the tree would have noticed.
///
/// This test is the join. It reads both files' own text: the CRD's description
/// must still say the end is exclusive, and the receipt's arm 4 must be the
/// strict comparison with the message that says so. A mutant on either side —
/// reverting the arm to `>`, or re-describing the CRD field as inclusive —
/// fails here at assertion time.
///
/// Reading the CRD's YAML as text rather than through `weirkeeper`'s types is
/// deliberate: this crate does not depend on the control plane, and the claim
/// is about the shipped document an adopter applies.
#[test]
fn the_receipt_window_and_the_crds_window_are_both_half_open() {
    let crd = std::fs::read_to_string(root().join("config/crd/backups.yaml"))
        .expect("read config/crd/backups.yaml");
    assert!(
        crd.contains("Exclusive end of the covered window"),
        "config/crd/backups.yaml no longer documents status.windowCovered.toMs as the \
         EXCLUSIVE end of the window. The receipt's invariant 4 is strict BECAUSE the CRD \
         says this; if the CRD's semantics move, the receipt's arm and \
         crates/logweir-core/tests/backup_receipt.rs::arm_4_refuses_an_empty_window move \
         with it, in the same commit."
    );
    assert!(
        crd.contains("Inclusive start of the covered window"),
        "config/crd/backups.yaml no longer documents status.windowCovered.fromMs as the \
         INCLUSIVE start; a window with two exclusive ends is not the half-open range \
         either document describes"
    );

    let receipt = std::fs::read_to_string(root().join("crates/logweir-core/src/backup_receipt.rs"))
        .expect("read crates/logweir-core/src/backup_receipt.rs");
    assert!(
        receipt.contains("if self.covered.from_ms >= self.covered.to_ms {"),
        "BackupReceipt::validate_invariants's arm 4 is no longer the STRICT comparison. \
         `from_ms > to_ms` accepts `from_ms == to_ms`, i.e. an EMPTY half-open range, \
         which the CRD's exclusive `toMs` says covers no record."
    );
    assert!(
        receipt
            .contains("the covered window's end is EXCLUSIVE, so an empty range covers no record"),
        "arm 4's message no longer says the end is exclusive. The message is the interface \
         docs/verify_scorecard.py reproduces byte for byte, and it is where an operator \
         reads which convention the two integers follow."
    );

    // …and the writer converts, in the one place that measures the window.
    // Without this the arm would refuse a legitimate single-record backup:
    // the manifest's newest `end_timestamp` is INCLUSIVE.
    let phase_run = std::fs::read_to_string(root().join("crates/logweir/src/backup/phase_run.rs"))
        .expect("read crates/logweir/src/backup/phase_run.rs");
    assert!(
        phase_run.contains("saturating_add(1)"),
        "crates/logweir/src/backup/phase_run.rs no longer converts the manifest's inclusive \
         newest timestamp to an EXCLUSIVE bound, so a backup of a topic whose records share \
         one millisecond would produce a receipt its own invariant 4 refuses"
    );
}
