//! The unit suite dials nothing. Task 5b.
//!
//! `cargo test --workspace` with the compose stack down took ~20 minutes and
//! was twice mistaken for a hang. Most of that was cargo fingerprinting an
//! artifact directory five tasks of mutation rounds had grown to 873,349 files
//! (`scripts/check-deps-count.sh` is the gate for that half). What was left,
//! once the tree was clean, was **33 seconds in two tests**, neither of which
//! is about brokers or buckets:
//!
//! | test | measured | what it was waiting on |
//! |---|---|---|
//! | `tests/doctor.rs::check_7_…` | 26.60 s | 20 s of `rdkafka_reader.rs:16`'s `const T`, plus ~6.5 s below |
//! | `src/doctor.rs::tests::check_storage_reports_skipped_…` | 6.70 s | ten `object_store` retries against **`http://169.254.169.254`** |
//!
//! That second row is worth reading twice. Everyone, this brief included,
//! assumed the storage wait was a connect to `localhost:9000`. It was not:
//! with no AWS credentials in the environment, `AmazonS3Builder::from_env()`
//! falls through to the EC2 instance-metadata credential provider, and the
//! retries are against the link-local metadata address — traffic off the
//! loopback interface, from a unit test, on a workstation that is not an EC2
//! instance (Global Constraint 17). The configured endpoint was never reached.
//!
//! ## What this file is, and what it is not
//!
//! It is the CHEAP, ALWAYS-ON half: a grep over the source tree, costing
//! milliseconds, that fails when a dialling constructor or a compose-stack
//! endpoint appears in a file that is not `e2e`-gated and not on an explicit
//! allow-list. **A grep proves a constructor is absent, not that a socket is
//! closed.** The expensive half is `scripts/time-unit-suite.sh`, which bounds
//! the whole suite at 120 s AND every individual test at 5 s — a test that
//! opens a socket to something that is not there blows the second bound on
//! its own, whatever it is spelled like. Neither half alone is the acceptance.
//!
//! The allow-list is BY PATH and every entry carries its reason. Adding one is
//! meant to be a reviewable event; a prefix rule such as `contains("src/")`
//! would let a new dialling test into `crates/logweir/tests/` unnoticed, which
//! is why `a_synthetic_dialling_test_is_flagged` below asserts the classifier
//! flags a fabricated offender rather than only asserting the real tree is
//! clean. A check that cannot fail is this build's signature defect.

use std::path::{Path, PathBuf};

/// Tokens that mean "this file opens a connection".
///
/// **`bootstrap_servers` is deliberately NOT here**, though it was in the
/// brief's list. It is a `DrillSpec` FIELD NAME: it occurs in
/// `logweir-core/src/spec.rs`, which compiles with `--no-default-features`
/// and carries no Kafka client at all and therefore cannot dial anything, and
/// in the two OSO render modules and their snapshots. Including it would have
/// forced an allow-list long enough to stop meaning anything — the same defect
/// as a `contains("src/")` prefix, reached from the other end.
///
/// What is left are things that only appear where a client is really built:
/// the two constructors, the compose stack's own endpoints, and ureq's
/// AGENTLESS request builders (which carry no timeout — see
/// `crates/logweir/tests/notify.rs`).
const DIAL_TOKENS: [&str; 13] = [
    "RdKafkaReader::connect(",
    "Store::from_url(",
    "Store::read_only_from_url(",
    "localhost:9092",
    "127.0.0.1:9092",
    "localhost:9000",
    "127.0.0.1:9000",
    // ureq entry points that carry the crate's DEFAULT configuration, i.e. no
    // overall or read timeout.
    //
    // These six are EXACTLY the `FORBIDDEN` list in
    // `crates/logweir/tests/notify.rs`, and `the_two_ureq_token_lists_agree`
    // in that file now asserts it rather than a comment claiming it. Fix
    // round 1 said "the two lists now match" and they did not: bare
    // `Agent::new(` — `use ureq::Agent;` and then `Agent::new()` — was in the
    // crate-local list and missing here, and the re-reviewer walked a probe
    // straight through this gap into `crates/logweir/tests/`, which the
    // crate-local test does not walk. An audit whose comment asserts a
    // property it does not have is the defect F6 was raised about, arriving
    // in the fix for F6.
    "ureq::post(",
    "ureq::get(",
    "ureq::request(",
    "ureq::Agent::new(",
    "Agent::new(",
    "ureq::agent(",
];

/// Paths whose match is expected, each with the reason it is expected.
///
/// Relative to the workspace root, `/`-separated. Production modules whose
/// job IS to dial come first; the rest are files where the token is a string
/// fed to a double, never a client.
const ALLOWED: [(&str, &str); 17] = [
    (
        "crates/logweir-kafka/src/rdkafka_reader.rs",
        "the broker client itself — this is where connecting to Kafka lives",
    ),
    (
        "crates/logweir/src/doctor.rs",
        "production: doctor's checks 6 and 7 dial BY DESIGN; its #[cfg(test)] spec \
         literals are fed to StubReader and to the pure evaluate_* halves",
    ),
    (
        "crates/logweir/src/drill/mod.rs",
        "production: the orchestrator's reader and its two Store handles",
    ),
    (
        "crates/logweir/src/backup/mod.rs",
        "production: `run` constructs the backup path's reader, its read-only archive \
         handle and (Task 5b) the WRITABLE evidence handle the signed receipt is put \
         through; `run_with` takes them as parameters and names none of the three",
    ),
    (
        "crates/logweir/tests/backup_run.rs",
        "test support: builds BackupSpec YAML strings whose bootstrap_servers is \
         localhost:9092, handed to a ClusterReader double; constructs no client",
    ),
    (
        "crates/logweir/src/drill/phase0_admit.rs",
        "a `localhost:9092` inside a #[cfg(test)] in-memory DrillSpec handed to a \
         stub reader — the spec is data, no client is constructed",
    ),
    (
        "crates/logweir/tests/fixtures/mod.rs",
        "test support: builds DrillSpec YAML strings for FakeReader/RecordingStore; \
         constructs no client (measured: the binaries that use it run in <0.1 s)",
    ),
    (
        "crates/logweir-store/tests/storage.rs",
        "Store::from_url over tempdir filesystem URLs — no endpoint, no network \
         (measured 0.05 s for the whole binary)",
    ),
    (
        "crates/logweir-engine-oso/tests/engine.rs",
        "Store::read_only_from_url over tempdir filesystem archives — no endpoint, \
         no network (measured 0.07 s for the whole binary)",
    ),
    (
        "crates/logweir/tests/no_network_in_unit_tests.rs",
        "this file: it is the one that has to spell the tokens out",
    ),
    (
        "crates/logweir/tests/notify.rs",
        "names ureq's agentless builders in the assertion message of the structural \
         test that FORBIDS them; posts only to 127.0.0.1 with a bound",
    ),
    (
        "crates/logweir/tests/auth_binding.rs",
        "test support: builds `DrillSpec`/`BackupSpec` YAML whose `bootstrap_servers` is \
         localhost:9092, handed to a `ClusterReader` double and to the renderers; \
         constructs no client (Task 6, chain N)",
    ),
    (
        "crates/logweir/tests/topic_preflight.rs",
        "test support: `DrillSpec` YAML whose `bootstrap_servers` is `localhost:9092`, handed to \
         `ClusterReader`/`TopicCreator` doubles; constructs no client (Task 8, chain N)",
    ),
    (
        "crates/weirkeeper/src/main.rs",
        "production: the controller's single read-only archive handle, built once at start",
    ),
    (
        "crates/weirkeeper/tests/retention.rs",
        "read-only handles over a filesystem archive copied into a scratch directory — no \
         endpoint, no network",
    ),
    (
        "crates/logweir/tests/restore_mode.rs",
        "test support: `DrillSpec` YAML whose `target.bootstrap_servers` is `localhost:9092`, \
         handed to `ClusterReader`/`TopicCreator`/`TopicDeleter` doubles; constructs no client \
         (Task 9b, chain N)",
    ),
    (
        "crates/logweir/tests/windowed_reconciliation.rs",
        "test support: a `RestorePlan` whose `target_bootstrap` is `localhost:9092`, handed to \
         a `ClusterReader` double alongside a `DataEngine` double and an in-memory `Store`; \
         constructs no client (Task 10, chain N)",
    ),
];

/// A file whose first lines carry `#![cfg(feature = "e2e")]` is out of the
/// default set entirely and may dial as much as it likes — that is what the
/// `e2e` suite is for. Checked as an inner attribute anywhere in the file,
/// because that is the only form that gates a whole test crate.
fn is_e2e_gated(contents: &str) -> bool {
    contents.contains("#![cfg(feature = \"e2e\")]")
}

/// PURE, and the whole point: it takes a path and a body and names the
/// offending tokens, so `a_synthetic_dialling_test_is_flagged` can prove the
/// classifier has teeth without anyone having to commit a dialling test to
/// find out.
fn offences(rel_path: &str, contents: &str) -> Vec<&'static str> {
    if is_e2e_gated(contents) {
        return Vec::new();
    }
    if ALLOWED.iter().any(|(p, _)| *p == rel_path) {
        return Vec::new();
    }
    DIAL_TOKENS
        .iter()
        .filter(|t| contents.contains(**t))
        .copied()
        .collect()
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

fn rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            rs_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

/// Every `.rs` under `crates/*/src/` and `crates/*/tests/`. `e2e/` is not
/// walked at all — it is the suite that is supposed to dial.
fn walk() -> Vec<(String, String)> {
    let root = workspace_root();
    let mut files = Vec::new();
    for c in std::fs::read_dir(root.join("crates")).unwrap().flatten() {
        let p = c.path();
        if !p.is_dir() {
            continue;
        }
        rs_files(&p.join("src"), &mut files);
        rs_files(&p.join("tests"), &mut files);
    }
    files.sort();
    files
        .into_iter()
        .map(|p| {
            let rel = p
                .strip_prefix(&root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            let body = std::fs::read_to_string(&p).unwrap();
            (rel, body)
        })
        .collect()
}

/// THE AUDIT. In the DEFAULT feature set on purpose — a check that only runs
/// under `--features e2e` would never run on the suite it is about.
#[test]
fn no_default_set_source_file_opens_a_connection() {
    let files = walk();
    let mut flagged: Vec<String> = Vec::new();
    for (rel, body) in &files {
        let bad = offences(rel, body);
        if !bad.is_empty() {
            flagged.push(format!("{rel}  ->  {}", bad.join(", ")));
        }
    }
    assert!(
        flagged.is_empty(),
        "{} file(s) in the DEFAULT feature set name a dialling constructor or a \
         compose-stack endpoint. Either point the test at one of the doubles the \
         tree already has (StubReader / FakeReader / RecordingStore / \
         Store::in_memory / the pure evaluate_target + evaluate_storage halves of \
         doctor's checks), or move it behind `#![cfg(feature = \"e2e\")]`. Adding a \
         path to ALLOWED in this file is the third option and is meant to be \
         argued for, not reached for.\n  {}",
        flagged.len(),
        flagged.join("\n  ")
    );
}

/// M3's self-check: an audit that walked nothing would report a clean tree
/// forever. 94 `.rs` files existed under `crates/*/{src,tests}` at Task 5b's
/// base commit (53 src + 36 tests + 5 nested); 60 is a floor loose enough
/// that deleting a module is not a false alarm and tight enough that a broken
/// walk cannot hide behind it.
#[test]
fn the_audit_actually_walks_the_tree() {
    let files = walk();
    assert!(
        files.len() >= 60,
        "the audit walked only {} files — a walk that visits nothing asserts \
         nothing, which is exactly the shape of check this task exists to stop \
         shipping",
        files.len()
    );
    assert!(
        files
            .iter()
            .any(|(p, _)| p == "crates/logweir/src/doctor.rs"),
        "the walk did not reach crates/logweir/src/doctor.rs, the file the whole \
         audit is about"
    );
}

/// M2's self-check: the classifier must FLAG something, not merely fail to
/// flag the tree as it stands today. The mutant this kills is an allow-list
/// widened to a prefix (`contains("src/")`, or `contains("tests/")`), under
/// which a new dialling test lands unnoticed and the audit stays green.
#[test]
fn a_synthetic_dialling_test_is_flagged() {
    // Not on the allow-list, not e2e-gated: the shape of a test somebody adds
    // next month.
    let bad = offences(
        "crates/logweir/tests/a_new_test_someone_adds.rs",
        "#[test]\nfn t() { let r = RdKafkaReader::connect(&[\"localhost:9092\".into()], a); }\n",
    );
    assert_eq!(
        bad,
        vec!["RdKafkaReader::connect(", "localhost:9092"],
        "the classifier did not flag a plainly dialling test file"
    );

    // The same file, gated: allowed, and the gate is the ONLY thing that
    // changed between the two.
    let gated = offences(
        "crates/logweir/tests/a_new_test_someone_adds.rs",
        "#![cfg(feature = \"e2e\")]\n#[test]\nfn t() { RdKafkaReader::connect(&[], a); }\n",
    );
    assert!(gated.is_empty(), "an e2e-gated file must not be flagged");

    // And an allow-listed path stays quiet — otherwise the audit is
    // unsatisfiable and someone deletes it.
    let allowed = offences(
        "crates/logweir/src/doctor.rs",
        "RdKafkaReader::connect(&sp.target.bootstrap_servers, AuthConfig::Plaintext)",
    );
    assert!(
        allowed.is_empty(),
        "an allow-listed path must not be flagged"
    );
}

/// Every allow-list entry must still MATCH something. An entry whose file has
/// stopped dialling is a standing permission nobody needs, and the next file
/// to inherit that path inherits the permission with it.
#[test]
fn every_allow_list_entry_is_still_earned() {
    let root = workspace_root();
    let mut stale = Vec::new();
    for (p, why) in ALLOWED {
        assert!(!why.is_empty(), "{p} has no reason recorded");
        let full = root.join(p);
        assert!(full.exists(), "allow-listed path {p} does not exist");
        let body = std::fs::read_to_string(&full).unwrap();
        if !DIAL_TOKENS.iter().any(|t| body.contains(t)) {
            stale.push(p);
        }
    }
    assert!(
        stale.is_empty(),
        "these allow-list entries no longer match any dial token and should be \
         removed rather than left standing: {stale:?}"
    );
}
