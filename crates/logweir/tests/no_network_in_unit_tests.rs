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
const DIAL_TOKENS: [&str; 16] = [
    "RdKafkaReader::connect(",
    "Store::from_url(",
    "Store::read_only_from_url(",
    // D2 W2's EXPLICIT constructors (`crates/logweir-store/src/lib.rs`).
    // Dialling in exactly the sense the two above are: each returns a handle
    // whose first method call opens a socket. They are the ones
    // destination-backed code uses from W4 onwards, so leaving them out would
    // point this gate at the shrinking half of the surface.
    "Store::from_url_with(",
    "Store::read_only_with(",
    "localhost:9092",
    "127.0.0.1:9092",
    "localhost:9000",
    "127.0.0.1:9000",
    // The dead-loopback address D2 §3.5 pins the instance-metadata endpoint
    // to, and the one `logweir-store`'s own tests use to prove a request is
    // refused before the transport. Nothing listens on port 1, so a file that
    // names it is a file that expects a connection to fail — which is still a
    // file that opens one, and is still worth seeing on the way in.
    //
    // The `http://` form ONLY, and that is deliberate rather than sloppy.
    // `crates/weirkeeper/tests/linkage.rs:690-699` uses `https://127.0.0.1:1`
    // as an apiserver address it never contacts, and says in a comment that it
    // relies on not being a dial token; a bare `127.0.0.1:1` would flag it and
    // the cheapest fix would be to delete this token again. The plaintext twin
    // is the one this repository actually dials.
    "http://127.0.0.1:1",
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
const ALLOWED: [(&str, &str); 24] = [
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
        "crates/logweir/src/probe.rs",
        "production: the cluster probe dials BY DESIGN; it is the whole subcommand \
         (interface I14, Task 15c). Its pure half (`outcome`) and its reader seam \
         (`probe`) take a value and a `&dyn ClusterReader`, so the whole contract is \
         testable with no socket; `run`/`dial` are the only functions here that name \
         the constructor, and they are the SECOND sanctioned construction site after \
         `logweir-kafka`'s own reader",
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
        "crates/logweir/src/catalog/cli.rs",
        "production: PLAT-15.1's two operator subcommands. `run_sync` and `run_list` are \
         the ONLY functions in `crates/logweir/src/catalog/` that name a store \
         constructor — every other function there, including both testable seams \
         (`sync_with`, `list_with`), takes a `&Store` it does not build, which is what \
         keeps `crates/logweir/tests/catalog.rs` socket-free against `Store::in_memory`. \
         `Store::from_url` and not `from_url_with` on purpose: these are an operator's \
         commands run with the operator's own ambient credentials, and the explicit \
         constructor is for destination-backed Jobs (D-SEAMS S5)",
    ),
    (
        "crates/logweir/tests/catalog.rs",
        "PLAT-15.1: ONE `Store::from_url` over a TEMPDIR filesystem URL — no endpoint and \
         no network, the same case `crates/logweir-store/tests/storage.rs` is listed for. \
         It is the failure injection `a_catalog_write_that_fails_leaves_the_run_and_its_\
         receipt_untouched` needs: a regular file planted at `logweir/catalog/v1/points` \
         makes the catalog put fail with a store error that is NOT AlreadyExists while \
         `logweir/backups/` stays writable, which `Store::in_memory` cannot do. Every \
         other row in the file uses `Store::in_memory` (measured: the whole binary runs \
         in 0.3 s)",
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
        "crates/logweir-store/src/lib.rs",
        "production: the store crate IS where object-store handles are built, so it \
         defines all four constructors by name; `127.0.0.1:1` is DEAD_METADATA_ENDPOINT, \
         the address D2 §3.5 pins the instance-metadata endpoint to precisely so that a \
         credential chain reaching it is refused instead of picking up a node role",
    ),
    (
        "crates/logweir-store/tests/options.rs",
        "D2 W2 explicit store construction: builds handles to assert what they are \
         CONFIGURED with (read back off AmazonS3Builder, no request), and issues exactly \
         two `get`s against http://127.0.0.1:1 — one refused by reqwest's https_only \
         before any socket, one by the closed port. Whole binary measured at 0.01 s",
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
        // D2 W7 (PLAT-08.1). THIS FILE IS W4'S BY D2 §13.2 AND THIS ONE ENTRY IS
        // W7'S — kept to a single narrow row for that reason.
        "crates/weirkeeper/src/evidence_store.rs",
        "production: `StoreCache::get_or_build` is the ONE place in the control plane \
         that builds a per-destination read-only evidence handle (D2 §3.10's \
         allowlisted `ControllerIdentity` path), and it names `Store::read_only_with` \
         because addressing, transport and the CA must come from the destination and \
         never from the controller's environment. It is the explicit twin of \
         `main.rs`'s one `read_only_from_url`, listed above. The CONSTRUCTION opens no \
         socket — `AmazonS3Builder::build()` configures a client and dials nothing — \
         and it happens inside `tokio::task::spawn_blocking`, which \
         `crates/weirkeeper/tests/retention.rs::no_store_call_is_made_outside_spawn_blocking` \
         pins together with the fact that this is the only \
         `Store::read_only_with` site in the crate. There is exactly one such call \
         here, in one function, and the cache is the reason a second one would be a \
         reviewable event rather than a convenience",
    ),
    (
        "crates/logweir/tests/restore_mode.rs",
        "test support: `DrillSpec` YAML whose `target.bootstrap_servers` is `localhost:9092`, \
         handed to `ClusterReader`/`TopicCreator`/`TopicDeleter` doubles; constructs no client \
         (Task 9b, chain N)",
    ),
    (
        "crates/weirkeeper/tests/verification.rs",
        "read-only handles over a filesystem evidence tree in a scratch directory — no endpoint, \
         no network",
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

// ===========================================================================
// THE ONE-CONSTRUCTION-SITE RULE, DERIVED — Task 16b, plan erratum E11(e)
// ===========================================================================
//
// Interface **I1**: a client's auth is built in ONE place,
// `AuthConfig::from_spec`, and no call site pins an arm itself. Task 6 landed
// that rule with a guard that iterated a **fixed three-file list**
// (`auth_binding.rs`'s `no_construction_site_hardcodes_plaintext`, over
// `drill/mod.rs`, `doctor.rs`, `backup/mod.rs`). Task 15c then added a FOURTH
// sanctioned site, `probe.rs`, and the Task 6 row did not notice — it could
// not: a list is not a rule. Task 15c disclosed this and wrote its own,
// stronger guard inside `cluster_probe.rs`, so nothing was ever unprotected;
// but the next site would have been unguarded again, and the tree would then
// have carried TWO guards and no derivation. Task 15c's review raised it as
// **M-2**.
//
// This is the one guard. It DERIVES its file list by walking
// `crates/logweir/src/**` and `crates/weirkeeper/src/**`, so a fifth site is
// caught by existing, and the allowlist below is the only place a sanctioned
// site is named. Task 15c's row is folded in whole — its `fn_bodies`-counted
// "exactly one function may name the constructor, and it is THAT one" is
// asserted here for every sanctioned site rather than for `probe.rs` alone,
// which is stronger than either of the two guards it replaces.
//
// WHY IT LIVES IN THIS FILE. This file already owns the walk, and it is the
// one path on `ALLOWED` whose reason is "it is the one that has to spell the
// tokens out" — a guard about constructor names has to name them, and any
// other home would need a standing permission for a file that constructs
// nothing.

/// The tokens that mean "a Kafka client's auth is built here".
///
/// The first is the dial itself; the second is interface I1's ONE
/// constructor; the last two are the arms a call site must never pin — the
/// defect Task 6 found in two of three sites, under which a spec asking for
/// `scramSha512` was recorded in the receipt and then dialled unauthenticated.
const CONSTRUCTION_TOKENS: [&str; 4] = [
    "RdKafkaReader::connect(",
    "AuthConfig::from_spec",
    "AuthConfig::Plaintext",
    "AuthConfig::ScramSha512",
];

/// The arms no call site may pin. A subset of [`CONSTRUCTION_TOKENS`], named
/// separately because the offence is different: naming `from_spec` is the
/// RULE, naming an arm is the BREACH.
const PINNED_ARMS: [&str; 2] = ["AuthConfig::Plaintext", "AuthConfig::ScramSha512"];

/// The SANCTIONED construction sites: the file, the one function in it that
/// may name the dialling constructor, and why that site exists.
///
/// Four entries, and `crates/weirkeeper/**` contributes NONE of them — the
/// controller never dials a broker itself, which is why a probe is a Job. That
/// absence is asserted rather than assumed: the walk covers weirkeeper's whole
/// `src`, so a `RdKafkaReader::connect(` appearing there would be an
/// unsanctioned site and would fail this guard by name.
const CONSTRUCTION_SITES: [(&str, &str, &str); 4] = [
    (
        "crates/logweir/src/drill/mod.rs",
        "fn context(",
        "`drill::context` — the orchestrator builds the target reader once, before any \
         phase runs, and hands it down as a `&dyn ClusterReader`",
    ),
    (
        "crates/logweir/src/doctor.rs",
        "fn check_target(",
        "`doctor::check_target` — check 7 dials the target BY DESIGN; that is the whole \
         check, and its pure half `evaluate_target` takes the answer as a value",
    ),
    (
        "crates/logweir/src/backup/mod.rs",
        "pub fn run(",
        "`backup::run` — the backup path's source reader; `run_with` takes it as a \
         parameter and names no constructor, which is what keeps the unit suite socketless",
    ),
    (
        "crates/logweir/src/probe.rs",
        "fn dial(",
        "`probe::dial` — interface I14's subcommand IS a dial (Task 15c); its pure half \
         `outcome` and its seam `probe` take a value and a `&dyn ClusterReader`",
    ),
];

/// The bodies of every `fn` in a source file, keyed by its signature line.
///
/// Brace-counted rather than regexed: "exactly one function names the
/// constructor" is a claim about a BODY, and a line-based scan cannot tell
/// which function a line is inside. Lifted verbatim from the guard Task 15c
/// wrote in `cluster_probe.rs`, which this row replaces.
fn fn_bodies(src: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    // The running BYTE offset of each line. Every index below lands on a `{`
    // or a `}`, both ASCII, so slicing by them is always on a char boundary
    // even though these files carry multi-byte punctuation in their comments.
    let mut line_start = 0usize;
    for line in src.lines() {
        let start = line_start;
        line_start += line.len() + 1;
        let t = line.trim_start();
        if !(t.starts_with("fn ") || t.starts_with("pub fn ") || t.starts_with("pub(crate) fn ")) {
            continue;
        }
        let Some(open) = src[start..].find('{').map(|i| start + i) else {
            continue;
        };
        let mut depth = 0usize;
        let mut end = open;
        for (i, c) in src[open..].char_indices() {
            match c {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = open + i;
                        break;
                    }
                }
                _ => {}
            }
        }
        out.push((t.to_string(), src[open..=end].to_string()));
    }
    out
}

/// `src` with comment lines removed.
///
/// Every one of these files legitimately DISCUSSES the arm it must not pin —
/// `probe.rs` explains in prose why an `AuthConfig::ScramSha512` there would
/// be the defect — and a scan that could not tell prose from code would force
/// the reasoning out of the source.
fn code_of(src: &str) -> String {
    src.lines()
        .filter(|l| {
            let t = l.trim_start();
            !(t.starts_with("//") || t.starts_with("///") || t.starts_with("//!"))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// PURE, and the whole point: one file's path and body in, its offences
/// against interface I1 out. `a_construction_site_planted_in_an_unlisted_file_is_flagged`
/// drives this directly, so the guard's teeth are proved without anyone having
/// to commit a fifth construction site to find out.
fn construction_offences(rel_path: &str, contents: &str) -> Vec<String> {
    let code = code_of(contents);
    if !CONSTRUCTION_TOKENS.iter().any(|t| code.contains(t)) {
        return Vec::new();
    }
    let Some((_, sanctioned_fn, _)) = CONSTRUCTION_SITES.iter().find(|(p, _, _)| *p == rel_path)
    else {
        let named: Vec<&str> = CONSTRUCTION_TOKENS
            .iter()
            .filter(|t| code.contains(**t))
            .copied()
            .collect();
        return vec![format!(
            "{rel_path} builds a Kafka client's auth ({}) and is not a sanctioned construction \
             site. Interface I1 says there is ONE place a client's auth is built; a fifth site \
             is a decision, not an edit, and belongs in CONSTRUCTION_SITES with its reason",
            named.join(", ")
        )];
    };

    let mut out = Vec::new();
    if !code.contains("AuthConfig::from_spec") {
        out.push(format!(
            "{rel_path} is a sanctioned construction site but does not build its auth through \
             `AuthConfig::from_spec` (interface I1)"
        ));
    }
    for arm in PINNED_ARMS {
        if code.contains(arm) {
            out.push(format!(
                "{rel_path} pins `{arm}` in CODE: a spec asking for the other mode would be \
                 recorded in the document and then dialled as this one"
            ));
        }
    }
    let dialling: Vec<String> = fn_bodies(&code)
        .into_iter()
        .filter(|(_, body)| body.contains("RdKafkaReader::connect("))
        .map(|(sig, _)| sig)
        .collect();
    if dialling.len() != 1 {
        out.push(format!(
            "{rel_path}: exactly one function may name the dialling constructor; found \
             {dialling:?}"
        ));
    } else if !dialling[0].starts_with(sanctioned_fn) {
        out.push(format!(
            "{rel_path}: the constructor is named by {:?}, not by the sanctioned `{sanctioned_fn}`. \
             A dial that moved into a function the pure rows drive would put a socket back in the \
             default suite",
            dialling[0]
        ));
    }
    out
}

/// Every `.rs` under the two crates whose sources may construct a client.
fn construction_walk() -> Vec<(String, String)> {
    let root = workspace_root();
    let mut files = Vec::new();
    for crate_src in [
        root.join("crates/logweir/src"),
        root.join("crates/weirkeeper/src"),
    ] {
        rs_files(&crate_src, &mut files);
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

/// **THE GUARD.** No file under `crates/logweir/src/**` or
/// `crates/weirkeeper/src/**` builds a Kafka client's auth except the four
/// sanctioned sites, each through `AuthConfig::from_spec`, each pinning no
/// arm, each confining the dialling constructor to the one function named
/// beside it.
///
/// Derived, not listed: this is Task 6's row and Task 15c's row, merged, with
/// the file list computed from the tree instead of typed out. Plan erratum
/// **E11(e)**, review finding **M-2**.
#[test]
fn the_one_construction_site_rule_is_derived_from_the_tree() {
    let files = construction_walk();
    let mut offences: Vec<String> = Vec::new();
    for (rel, body) in &files {
        offences.extend(construction_offences(rel, body));
    }
    assert!(
        offences.is_empty(),
        "{} breach(es) of the ONE-construction-site rule (interface I1):\n  {}",
        offences.len(),
        offences.join("\n  ")
    );

    // EVERY SANCTIONED SITE WAS ACTUALLY SEEN. Without this, deleting a site —
    // or breaking the walk — leaves an allowlist entry standing for a file
    // nothing checks, and the guard reports a clean tree forever.
    for (path, _, reason) in CONSTRUCTION_SITES {
        let (_, body) = files
            .iter()
            .find(|(rel, _)| rel == path)
            .unwrap_or_else(|| panic!("the walk did not reach the sanctioned site {path}"));
        assert!(
            code_of(body).contains("AuthConfig::from_spec"),
            "{path} is on the sanctioned list ({reason}) but no longer constructs anything; \
             remove the entry in the same commit that removed the site"
        );
    }
}

/// The walk reaches both crates, and a walk that visits nothing asserts
/// nothing.
#[test]
fn the_construction_walk_covers_both_crates() {
    let files = construction_walk();
    assert!(
        files.len() >= 30,
        "the construction walk visited only {} files",
        files.len()
    );
    for expected in [
        "crates/logweir/src/probe.rs",
        "crates/weirkeeper/src/controllers/kafka_cluster.rs",
    ] {
        assert!(
            files.iter().any(|(rel, _)| rel == expected),
            "the walk did not reach {expected}; `crates/weirkeeper/src/**` is walked because the \
             controller links `logweir-kafka` (Global Constraint 27, since Task 15c) and could \
             therefore construct a client"
        );
    }
}

/// The guard has TEETH: a construction site planted in a file that is not on
/// the list is flagged, and so is each way a sanctioned one can go wrong.
///
/// THE MUTANT M-2 IS ABOUT. Task 6's guard iterated a fixed three-file list,
/// so this plant — a fourth file that dials — was invisible to it. Here it is
/// one offence, by name.
#[test]
fn a_construction_site_planted_in_an_unlisted_file_is_flagged() {
    // 1. A fifth site, in a file nobody listed.
    let planted = construction_offences(
        "crates/logweir/src/a_new_module_someone_adds.rs",
        "fn go() {\n    let auth = AuthConfig::from_spec(&spec, None).unwrap();\n    \
         let r = RdKafkaReader::connect(&servers, auth);\n}\n",
    );
    assert_eq!(
        planted.len(),
        1,
        "an unlisted construction site must be exactly one offence: {planted:?}"
    );
    assert!(
        planted[0].contains("not a sanctioned construction site"),
        "and it must say so: {}",
        planted[0]
    );

    // 2. THE SAME PLANT IN WEIRKEEPER. The controller never dials; a client
    //    constructed there is the same offence and is reached by the same walk.
    let in_controller = construction_offences(
        "crates/weirkeeper/src/controllers/a_new_reconciler.rs",
        "fn reconcile() {\n    let r = RdKafkaReader::connect(&servers, auth);\n}\n",
    );
    assert_eq!(in_controller.len(), 1, "{in_controller:?}");

    // 3. A sanctioned site that PINS AN ARM — Task 6's original defect.
    let pinned = construction_offences(
        "crates/logweir/src/doctor.rs",
        "fn check_target() {\n    let auth = AuthConfig::Plaintext;\n    \
         let r = RdKafkaReader::connect(&servers, auth);\n}\n",
    );
    assert_eq!(pinned.len(), 2, "{pinned:?}");
    assert!(
        pinned.iter().any(|o| o.contains("AuthConfig::from_spec")),
        "it stopped using the ONE constructor: {pinned:?}"
    );
    assert!(
        pinned
            .iter()
            .any(|o| o.contains("pins `AuthConfig::Plaintext`")),
        "and it pinned an arm: {pinned:?}"
    );

    // 4. A sanctioned site whose dial MOVED OUT of its one function — Task
    //    15c's own claim, now asserted for all four sites.
    let moved = construction_offences(
        "crates/logweir/src/probe.rs",
        "fn dial() {\n    let a = AuthConfig::from_spec(&s, None);\n}\n\
         fn outcome() {\n    let r = RdKafkaReader::connect(&servers, auth);\n}\n",
    );
    assert_eq!(moved.len(), 1, "{moved:?}");
    assert!(
        moved[0].contains("not by the sanctioned `fn dial(`"),
        "{}",
        moved[0]
    );

    // 5. PROSE IS NOT CODE. `backup/mod.rs` names the constructor in a doc
    //    comment explaining the rule; a guard that could not tell would force
    //    the reasoning out of the source.
    assert!(
        construction_offences(
            "crates/logweir/src/a_documented_module.rs",
            "/// This module never names RdKafkaReader::connect( or AuthConfig::Plaintext.\n\
             fn go() {}\n",
        )
        .is_empty(),
        "a comment mentioning the tokens is not a construction site"
    );
}
