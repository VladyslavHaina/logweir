//! Retention **reporting**, and guard **G-RET**.
//!
//! READ `retention_reconciler_holds_no_writable_archive_handle` FIRST. It is
//! the guard, and it has three arms: the handle the reconciler builds refuses
//! to put, a whole reporting run changes not one byte of the archive, and the
//! report carries the removal command as a STRING rather than running it.
//! Everything else in this file exists to make those assertions meaningful —
//! that the ordering comes from the manifest body and not the key, that the
//! two rules are a union with a stated reason, that the gate has teeth and no
//! exemptions, and that no store call escapes `spawn_blocking`.
//!
//! NOTHING HERE DIALS ANYTHING. Every handle is
//! `Store::read_only_from_url` over a **filesystem** URL — the checked-in
//! `tests/fixtures/retention-archive/` tree copied into a scratch directory
//! under `std::env::temp_dir()` — and the one `kube` test uses
//! `weirkeeper::testing::mock_client_recording_bodies`, whose transport is a
//! `tower` closure. No socket, no Job, no `kubectl`, and nothing near Global
//! Constraint 22's 15 s per-test bound. `crates/logweir/tests/no_network_in_unit_tests.rs`
//! carries this file's allow-list entry with that reason.
//!
//! WHY THE ZERO-WRITES PROPERTY IS OBSERVED AT THE FILESYSTEM AND NOT THROUGH
//! A RECORDER. `Store` is a concrete struct with no trait seam
//! (`crates/logweir-store/src/lib.rs`'s `pub struct Store`), and introducing
//! one would forfeit the by-type property this whole guard rests on: the
//! reporting path is safe because it holds a `Store` that cannot put, not
//! because a double happened to record no puts. So the observation is a
//! before/after directory walk over real bytes.
//!
//! `tempfile` IS NOT ADDED TO THIS CRATE'S MANIFEST. Global Constraint 38
//! closes the workspace graph, and `std::env::temp_dir()` plus a removal
//! guard is the same test.

use std::path::{Path, PathBuf};

use chrono::{DateTime, TimeZone as _, Utc};
use logweir_core::engine::StorageUrl;
use logweir_store::{Store, StoreError};
use weirkeeper::conditions::apply_merge_patch;
use weirkeeper::crds::backup_schedule::{BackupSchedule, BackupScheduleStatus, Retention};
use weirkeeper::retention::{
    evaluate, RemovalReason, RetentionReport, ARCHIVE_URL_ENV, NO_RULE_NOTE,
};
use weirkeeper::testing::{mock_client_recording_bodies, Route, SeenBody};

// ---------------------------------------------------------------------------
// The fixture archive, and the instant it is written against
// ---------------------------------------------------------------------------

/// `2026-09-09T00:00:00Z`, in epoch milliseconds.
///
/// EVERY FIXTURE WINDOW IS AN OFFSET FROM THIS, and the offsets are checked in
/// as literal integers in the five `manifest.json` bodies. A test that read
/// `Utc::now()` instead would make `keep_days` unassertable: the five sets
/// would drift across the seven-day cutoff as the calendar moved.
const NOW_MS: i64 = 1_788_912_000_000;

/// The five checked-in sets, in the order their WINDOWS put them — newest
/// first — beside the offset from [`NOW_MS`] each manifest declares.
///
/// THE DIRECTORY NAMES SORT IN A DIFFERENT ORDER FROM THE WINDOWS, and that is
/// the whole point of the fixture: `backup-001 … backup-005` lexically is
/// `backup-005, backup-002, backup-004, backup-003, backup-001` by window. A
/// `sort_by_key(|s| s.backup_id)` mutant produces a completely different
/// report over the same archive.
const BY_WINDOW: [(&str, i64); 5] = [
    ("backup-005", 1_788_908_400_000), // now − 1 h
    ("backup-002", 1_788_822_000_000), // now − 25 h
    ("backup-004", 1_788_735_600_000), // now − 49 h
    ("backup-003", 1_788_649_200_000), // now − 73 h
    ("backup-001", 1_788_192_000_000), // now − 200 h, i.e. older than 7 days
];

/// The archive URL the rendered commands name, and the one the acceptance
/// spells out.
const ARCHIVE_URL: &str = "s3://kafka-backups/mvp-demo";

fn now() -> DateTime<Utc> {
    Utc.timestamp_millis_opt(NOW_MS)
        .single()
        .expect("the fixture instant exists")
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/weirkeeper sits two levels under the workspace root")
        .to_path_buf()
}

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/retention-archive")
}

/// A scratch copy of the checked-in archive, removed when it drops.
///
/// NAMED FROM `std::process::id()` PLUS A PER-TEST TAG. The pid alone is not
/// unique inside one test binary: `cargo test` runs these tests as threads of
/// a single process, so two tests sharing a pid-only directory would race —
/// one removing the tree the other was walking. `Drop` removes it whether the
/// test passed, failed or panicked.
struct Scratch {
    root: PathBuf,
}

impl Scratch {
    /// Copy the fixture archive to `<temp>/logweir-t19-<pid>-<tag>/<under>`.
    ///
    /// `under` is the archive's own prefix inside the scratch root, so a test
    /// can reproduce a real `s3://bucket/prefix` layout on a filesystem.
    fn of(tag: &str, under: &str) -> Self {
        let root = std::env::temp_dir().join(format!("logweir-t19-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let dest = if under.is_empty() {
            root.clone()
        } else {
            root.join(under)
        };
        std::fs::create_dir_all(&dest).expect("the scratch directory is creatable");
        copy_tree(&fixture_dir(), &dest);
        Self { root }
    }

    fn path(&self) -> &Path {
        &self.root
    }

    /// The handle the reconciler builds: read-only, over this tree.
    ///
    /// `read_only_from_url` AND NEVER `from_url`. The write-path constructor's
    /// `LOGWEIR_ROOT` guard refuses any prefix that is not exactly `logweir/`,
    /// and an archive is never under `logweir/` — which is the whole reason
    /// the read-only constructor exists (Global Constraint 6).
    fn read_only_handle(&self) -> Store {
        Store::read_only_from_url(&StorageUrl::Filesystem {
            path: self.root.clone(),
        })
        .expect("a filesystem handle over an existing directory builds")
    }

    /// `(relative path, byte length, modified time)` for every file in the
    /// tree, sorted — the observation both write-freedom arms compare.
    fn walk(&self) -> Vec<(String, u64, std::time::SystemTime)> {
        let mut out = Vec::new();
        walk_into(&self.root, &self.root, &mut out);
        out.sort();
        out
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn copy_tree(from: &Path, to: &Path) {
    for entry in std::fs::read_dir(from).expect("the checked-in fixture directory is readable") {
        let entry = entry.expect("a fixture directory entry is readable");
        let target = to.join(entry.file_name());
        if entry.path().is_dir() {
            std::fs::create_dir_all(&target).expect("the scratch subdirectory is creatable");
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("the fixture file is copyable");
        }
    }
}

fn walk_into(root: &Path, dir: &Path, out: &mut Vec<(String, u64, std::time::SystemTime)>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in rd.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk_into(root, &path, out);
            continue;
        }
        let meta = std::fs::metadata(&path).expect("a scratch file's metadata is readable");
        out.push((
            path.strip_prefix(root)
                .expect("every walked path is under the scratch root")
                .to_string_lossy()
                .replace('\\', "/"),
            meta.len(),
            meta.modified().expect("a scratch file has an mtime"),
        ));
    }
}

/// `{keepLast, keepDays}` as the CRD spells it.
fn rule(keep_last: Option<i64>, keep_days: Option<i64>) -> Retention {
    Retention {
        keep_last,
        keep_days,
    }
}

// ---------------------------------------------------------------------------
// Source-reading helpers
// ---------------------------------------------------------------------------

fn source(relative: &str) -> Option<String> {
    std::fs::read_to_string(repo_root().join(relative)).ok()
}

fn required_source(relative: &str) -> String {
    source(relative)
        .unwrap_or_else(|| panic!("{relative} must exist for this assertion to mean anything"))
}

/// `src` with every comment, string and character literal replaced by spaces,
/// positions and line structure preserved. **LIFETIMES ARE LEFT ALONE.**
///
/// WHY SANITISE AT ALL. Two of the assertions below are about where a token
/// appears in CODE, and this file's own subjects are files whose doc comments
/// discuss exactly those tokens at length. Brace matching has the same
/// problem from the other end: `format!("{a}")` would unbalance any counter
/// that did not know it was inside a string.
///
/// # A `'` IS NOT ALWAYS A CHARACTER LITERAL, AND THAT WAS A REAL BLINDNESS
///
/// This function used to treat every `'` as the opener of a character literal
/// and blank forward to the next one. Rust spells LIFETIMES with the same
/// byte, so a signature like `-> BoxFuture<'static, …>` blanked every byte
/// from that tick to the following tick anywhere later in the file —
/// including, in `controllers/backup.rs`, the whole `reconcile` closure that
/// clones the archive handle. **Measured** (Task 17 re-review, concern 1): a
/// planted `store.get("logweir/probe")` outside `spawn_blocking` in
/// `controllers/backup.rs` left [`no_store_call_is_made_outside_spawn_blocking`]
/// green at 32 passed / 0 failed and the whole `weirkeeper` suite green at
/// 143 passed / 0 failed, while the same plant in
/// `controllers/backup_schedule.rs` — a file with no lifetime before the
/// planted line — failed at 30/2. A guard that is blind wherever a lifetime
/// appears is worse than no guard, because the ledger records it as closed
/// (STANDING RULE 21).
///
/// THE RULE, at a `'`: if the next byte is a backslash, or the byte two on is
/// a closing `'`, it is a character literal and the literal is consumed.
/// Anything else is a lifetime, and **only the tick** is consumed so the code
/// after it is still scanned. `'a'`, `'\n'` and `'\''` are literals;
/// `'static`, `'a` and `'_` are not. `the_sanitizer_knows_a_lifetime_from_a_character_literal`
/// tests this before anything trusts it.
fn sanitize(src: &str) -> String {
    let b = src.as_bytes();
    let mut out = vec![b' '; b.len()];
    let mut i = 0;
    // Keep newlines so a failure message can name a line number.
    for (j, ch) in b.iter().enumerate() {
        if *ch == b'\n' {
            out[j] = b'\n';
        }
    }
    while i < b.len() {
        // A line comment, which covers `//`, `///` and `//!`.
        if b[i] == b'/' && i + 1 < b.len() && b[i + 1] == b'/' {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        // A block comment, nested as Rust allows.
        if b[i] == b'/' && i + 1 < b.len() && b[i + 1] == b'*' {
            let mut depth = 1;
            i += 2;
            while i < b.len() && depth > 0 {
                if b[i] == b'/' && i + 1 < b.len() && b[i + 1] == b'*' {
                    depth += 1;
                    i += 2;
                } else if b[i] == b'*' && i + 1 < b.len() && b[i + 1] == b'/' {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            continue;
        }
        // A raw string: `r`, optional `b`, any number of `#`, then `"`.
        if b[i] == b'r' || (b[i] == b'b' && i + 1 < b.len() && b[i + 1] == b'r') {
            let mut k = if b[i] == b'b' { i + 2 } else { i + 1 };
            let hashes = {
                let start = k;
                while k < b.len() && b[k] == b'#' {
                    k += 1;
                }
                k - start
            };
            if k < b.len() && b[k] == b'"' {
                k += 1;
                let close = format!("\"{}", "#".repeat(hashes));
                while k < b.len() {
                    if b[k] == b'"' && src[k..].starts_with(&close) {
                        k += close.len();
                        break;
                    }
                    k += 1;
                }
                i = k;
                continue;
            }
        }
        // A normal string.
        if b[i] == b'"' {
            i += 1;
            while i < b.len() {
                if b[i] == b'\\' {
                    i += 2;
                    continue;
                }
                if b[i] == b'"' {
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        // A character literal — OR a lifetime. See the doc comment for the
        // rule and for the measurement that made it necessary.
        if b[i] == b'\'' {
            let is_char_literal = b.get(i + 1) == Some(&b'\\') || b.get(i + 2) == Some(&b'\'');
            if is_char_literal {
                i += 1;
                while i < b.len() {
                    if b[i] == b'\\' {
                        i += 2;
                        continue;
                    }
                    if b[i] == b'\'' {
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                continue;
            }
            // A LIFETIME. Consume the tick and nothing else.
            i += 1;
            continue;
        }
        out[i] = b[i];
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The `(start, end)` byte span of the balanced `{…}` block that begins at or
/// after `from`, over already-sanitised text.
fn block_after(text: &str, from: usize) -> Option<(usize, usize)> {
    let b = text.as_bytes();
    let open = (from..b.len()).find(|&i| b[i] == b'{')?;
    let mut depth = 0usize;
    for (i, ch) in b.iter().enumerate().skip(open) {
        match ch {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some((open, i));
                }
            }
            _ => {}
        }
    }
    None
}

/// The `(start, end)` byte span of the balanced `(…)` group that begins at or
/// after `from`, over already-sanitised text.
fn group_after(text: &str, from: usize) -> Option<(usize, usize)> {
    let b = text.as_bytes();
    let open = (from..b.len()).find(|&i| b[i] == b'(')?;
    let mut depth = 0usize;
    for (i, ch) in b.iter().enumerate().skip(open) {
        match ch {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some((open, i));
                }
            }
            _ => {}
        }
    }
    None
}

fn occurrences(text: &str, needle: &str) -> Vec<usize> {
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(rel) = text[from..].find(needle) {
        out.push(from + rel);
        from += rel + 1;
    }
    out
}

fn line_of(text: &str, index: usize) -> usize {
    text[..index].matches('\n').count() + 1
}

/// Whether `token`'s occurrence at `at` starts a NEW identifier rather than
/// ending a longer one.
///
/// # `store.` IS A SUBSTRING OF `restore.spec`, AND THAT MADE THE GUARD LIE
///
/// [`STORE_CALL_TOKENS`] holds bare substrings, and [`occurrences`] finds them
/// anywhere. Every reconciler until Task 20 named its object `backup` or
/// `schedule`, so nothing collided; `controllers/restore.rs` names its object
/// `restore`, and the very first run of the guard against it reported **eight
/// offences on lines that touch no `Store` at all** — `restore.spec`,
/// `restore.status`, `restore.name_any()`. A guard whose failure mode is a
/// false positive is a guard the next implementer edits the token list to
/// silence, and then it is blind for real.
///
/// THE RULE: when a token begins with an identifier byte, the byte before its
/// occurrence must not be one. `.` IS DELIBERATELY ALLOWED — `self.store.get(…)`
/// and `ctx.store.get(…)` are exactly the calls this scan exists to catch, so
/// treating a leading `.` as "part of a longer name" would trade a false
/// positive for a false negative. Tokens that themselves begin with `.`
/// (`.manifest_facts(`) are unaffected: their own first byte is the boundary.
fn token_at_word_boundary(text: &str, at: usize, token: &str) -> bool {
    let first = token.as_bytes()[0];
    if !(first.is_ascii_alphanumeric() || first == b'_') {
        return true;
    }
    match at.checked_sub(1).map(|i| text.as_bytes()[i]) {
        None => true,
        Some(prev) => !(prev.is_ascii_alphanumeric() || prev == b'_'),
    }
}

/// [`sanitize`] is TESTED BEFORE IT IS TRUSTED, and the case it is here for is
/// the first one.
///
/// **Task 20, ruling 1.** Every source-reading assertion in this file is only
/// as good as this function, and its previous `'`-handling made
/// [`no_store_call_is_made_outside_spawn_blocking`] blind to any code that
/// followed a lifetime — measured on a real plant in
/// `controllers/backup.rs`, which stayed green at 32 passed / 0 failed while
/// the identical plant in a lifetime-free file failed at 30/2. A sanitizer
/// with no test of its own is how a guard comes to assert nothing while the
/// ledger records it as closed (STANDING RULE 21).
#[test]
fn the_sanitizer_knows_a_lifetime_from_a_character_literal() {
    // THE REGRESSION, exactly. `'static` must not swallow the code after it.
    let lifetime = "fn f() -> BoxFuture<'static, u8> { let store = store.clone(); }";
    assert!(
        sanitize(lifetime).contains("store.clone()"),
        "a lifetime tick must NOT blank the code after it — this is the exact blindness that let \
         a planted `store.get(…)` survive this file's I13 guard. Got: {}",
        sanitize(lifetime)
    );
    // An anonymous and a named lifetime, both in a signature this crate has.
    assert!(
        sanitize("pub type O<'a> = &'a (dyn Fn() + 'a); fn g(_: &'_ u8) { store.get(k) }")
            .contains("store.get(k)"),
        "`'a` and `'_` are lifetimes too"
    );

    // …and a character literal still IS consumed, in all three shapes.
    assert!(
        !sanitize("if c == 'x' { store.get(k) } else { nothing() }").contains("'x'"),
        "a plain character literal is blanked"
    );
    assert!(
        sanitize("if c == 'x' { store.get(k) }").contains("store.get(k)"),
        "and blanking it does not eat the code after it"
    );
    assert!(
        sanitize("if c == '\\n' { store.get(k) }").contains("store.get(k)"),
        "an escaped character literal must not swallow the rest of the line"
    );
    assert!(
        sanitize("if c == '\\'' { store.get(k) }").contains("store.get(k)"),
        "an escaped-QUOTE character literal must not swallow the rest of the line"
    );

    // The three cases that were already right, kept as a floor.
    assert!(
        !sanitize("let s = \"store.get(\";").contains("store.get("),
        "a string literal IS blanked"
    );
    assert!(
        !sanitize("// store.get(\n").contains("store.get("),
        "a line comment IS blanked"
    );
    assert!(
        !sanitize("/* store.get( */").contains("store.get("),
        "a block comment IS blanked"
    );
    assert!(
        !sanitize("let s = r#\"store.get(\"#;").contains("store.get("),
        "a raw string IS blanked"
    );
    // Line structure survives, which is what makes `line_of` mean anything.
    assert_eq!(
        sanitize("a\nb\n").matches('\n').count(),
        2,
        "newlines are preserved so a failure message can name a line number"
    );
}

/// [`token_at_word_boundary`] is tested before the scan trusts it, in BOTH
/// directions.
///
/// **Task 20, ruling 1.** The false positive it exists for is real and was
/// measured: `restore.spec` contains `store.`, and the scan reported eight
/// offences against `controllers/restore.rs` on lines that touch no `Store`.
/// The false NEGATIVE it must not introduce is `self.store.get(…)`, which is
/// exactly the call the scan exists to catch — so `.` before the token is
/// allowed and an identifier byte is not.
#[test]
fn the_store_token_scan_tells_a_call_from_a_longer_identifier() {
    let at = |text: &str, token: &str| {
        occurrences(text, token)
            .into_iter()
            .filter(|&i| token_at_word_boundary(text, i, token))
            .count()
    };

    // FALSE POSITIVES that must be zero.
    assert_eq!(at("let n = restore.spec.plan_bytes;", "store."), 0);
    assert_eq!(at("restore.status.as_ref()", "store."), 0);
    assert_eq!(at("let x = restore.name_any();", "store."), 0);
    assert_eq!(at("let s = MyStore::new();", "Store::"), 0);

    // REAL CALLS that must all still count — including the two receiver
    // shapes whose token is preceded by a `.`.
    assert_eq!(at("store.get(k)", "store."), 1);
    assert_eq!(at("let _ = store.get(k);", "store."), 1);
    assert_eq!(at("self.store.get(k)", "store."), 1);
    assert_eq!(at("ctx.store.get(k)", "store."), 1);
    assert_eq!(at("Store::read_only_from_url(&loc)", "Store::"), 1);
    // A token that begins with `.` is its own boundary and is unaffected.
    assert_eq!(at("handle.manifest_facts(key)", ".manifest_facts("), 1);
}

// ===========================================================================
// G-RET — the guard, and its three arms
// ===========================================================================

/// **G-RET.** The retention reconciler holds no writable archive handle.
///
/// The three arms are also `#[test]`s of their own, so a failure names the
/// property that broke; this one runs all three under the guard's name, so the
/// ledger entry and the test name are the same string.
#[test]
fn retention_reconciler_holds_no_writable_archive_handle() {
    arm_the_handle_refuses_to_put("guard-put");
    arm_zero_object_store_writes("guard-writes");
    arm_the_command_is_reported_not_run("guard-command");
}

/// ARM 1 — the handle the reconciler builds returns `StoreError::ReadOnly`
/// from `put_create_only`, asserted **on the variant**.
#[test]
fn the_handle_refuses_to_put() {
    arm_the_handle_refuses_to_put("arm1");
}

fn arm_the_handle_refuses_to_put(tag: &str) {
    let scratch = Scratch::of(tag, "");
    let store = scratch.read_only_handle();

    // `logweir/…` is the ONE key a writable handle would accept, so refusing
    // it proves the refusal is the read-only flag and not the LOGWEIR_ROOT
    // assertion firing on an archive-shaped key. That ordering is the point:
    // `put_create_only` checks `read_only` FIRST, before the assertion, so a
    // read-only handle built over an archive prefix can never reach a
    // codepath that writes.
    match store.put_create_only("logweir/drills/should-never-exist.json", b"{}") {
        Err(StoreError::ReadOnly(key)) => assert!(
            key.contains("should-never-exist.json"),
            "the refusal must name the key it refused: {key}"
        ),
        other => panic!(
            "the retention path's handle must refuse to put with StoreError::ReadOnly — a \
             handle that can write is guard G-RET failing, whatever any run happened to do. \
             Got: {other:?}"
        ),
    }

    // And nothing landed. A refusal that wrote first would be worse than no
    // refusal at all.
    assert!(
        !scratch
            .path()
            .join("logweir/drills/should-never-exist.json")
            .exists(),
        "the refused put must not have created the object"
    );
}

/// ARM 2 — a whole reporting run performs **zero** object-store writes: no new
/// key, no modified mtime, no removal.
#[test]
fn a_reporting_run_performs_zero_object_store_writes() {
    arm_zero_object_store_writes("arm2");
}

fn arm_zero_object_store_writes(tag: &str) {
    let scratch = Scratch::of(tag, "");
    let store = scratch.read_only_handle();

    let before = scratch.walk();
    assert_eq!(
        before.len(),
        5,
        "the checked-in archive is five manifests; a walk that found {} is not observing the \
         fixture this arm is about",
        before.len()
    );

    let report = evaluate(&store, ARCHIVE_URL, "", &rule(Some(3), Some(7)), now())
        .expect("the fixture archive evaluates");
    assert!(
        !report.sets_that_would_be_removed.is_empty(),
        "an arm asserting a REPORTING run wrote nothing must be a run that found something to \
         report, or it proves nothing"
    );

    let after = scratch.walk();
    assert_eq!(
        before, after,
        "a retention evaluation must change NOT ONE BYTE of the archive: no new key, no \
         modified mtime, no removal. Global Constraint 6 stands unamended and no Logweir \
         component in tag 1 holds any object-store delete capability — retention REPORTS."
    );
}

/// ARM 3 — the report contains the command rather than running it.
#[test]
fn the_report_contains_the_command_rather_than_running_it() {
    arm_the_command_is_reported_not_run("arm3");
}

fn arm_the_command_is_reported_not_run(tag: &str) {
    let scratch = Scratch::of(tag, "");
    let store = scratch.read_only_handle();

    let before = scratch.walk();
    let report = evaluate(&store, ARCHIVE_URL, "", &rule(Some(3), Some(7)), now())
        .expect("the fixture archive evaluates");

    assert_eq!(
        report.aws_cli[0], "aws s3 rm 's3://kafka-backups/mvp-demo/backup-003/' --recursive",
        "the first rendered command must name the first set the policy would remove, spelled \
         exactly as an operator would run it"
    );
    assert_eq!(
        report.mc_cli[0], "mc rm --recursive --force 'local/kafka-backups/mvp-demo/backup-003/'",
        "the `mc` spelling is rendered from the same archive URL"
    );
    assert_eq!(
        report.aws_cli.len(),
        report.sets_that_would_be_removed.len(),
        "one command per set, in the same order"
    );

    assert_eq!(
        before,
        scratch.walk(),
        "producing the command must not run it: the archive is unchanged by the render"
    );
}

/// The report path renders the command; an operator runs it. A
/// source-reading assertion, because a behavioural test can only observe the
/// processes one run happened to spawn.
#[test]
fn the_report_path_spawns_no_process() {
    let src = required_source("crates/weirkeeper/src/retention.rs");
    assert!(
        src.len() > 2_000,
        "the source-reading assertion read {} bytes — a scan of a truncated or moved file \
         asserts nothing",
        src.len()
    );
    for token in ["Command::new", "std::process::Command", "process::Command"] {
        assert!(
            !src.contains(token),
            "crates/weirkeeper/src/retention.rs names `{token}`. The report RENDERS the \
             removal command as a string into BackupSchedule.status.retentionReport; an \
             operator runs it, and the adopter's own bucket lifecycle policy does the \
             deleting. A report that executes what it reports is the reaper this design \
             replaced."
        );
    }
}

// ===========================================================================
// The evaluation
// ===========================================================================

/// The order comes from the manifest BODY, never from the key string.
///
/// The fixture's directory names sort in a different order from their covered
/// windows, so a `sort by backup_id` mutant produces a completely different
/// report over the same five files — and the assertion is on the ordered
/// `backup_id` list, so the failure names the sets.
#[test]
fn retention_reads_the_window_from_the_manifest_not_from_the_key() {
    let scratch = Scratch::of("window", "");
    let store = scratch.read_only_handle();
    let report = evaluate(&store, ARCHIVE_URL, "", &rule(Some(3), Some(7)), now())
        .expect("the fixture archive evaluates");

    let by_window: Vec<&str> = BY_WINDOW.iter().map(|(id, _)| *id).collect();
    let lexical: Vec<&str> = {
        let mut v = by_window.clone();
        v.sort_unstable();
        v
    };
    assert_ne!(
        by_window, lexical,
        "the fixture must order differently by window than by name, or this test cannot fail"
    );

    assert_eq!(
        report.sets_kept,
        vec!["backup-005", "backup-002", "backup-004"],
        "`sets_kept` follows the WINDOWS, newest first. Lexically the first three would be \
         {lexical:?}[..3] — a key-derived sort keeps the wrong three sets and reports the \
         wrong two as removable."
    );
    let removed: Vec<&str> = report
        .sets_that_would_be_removed
        .iter()
        .map(|s| s.backup_id.as_str())
        .collect();
    assert_eq!(
        removed,
        vec!["backup-003", "backup-001"],
        "`sets_that_would_be_removed` follows the windows too, newest first"
    );

    // And the instant on each entry is the manifest's own `end_timestamp`,
    // not a value derived from the key — which carries no timestamp at all,
    // so a key-derived value would be the epoch.
    for (id, ms) in BY_WINDOW {
        if let Some(entry) = report
            .sets_that_would_be_removed
            .iter()
            .find(|s| s.backup_id == id)
        {
            assert_eq!(
                entry.newest_record_at.timestamp_millis(),
                ms,
                "{id}'s newest_record_at is the manifest body's maximum end_timestamp"
            );
        }
    }
}

/// `keepLast` and `keepDays` are a **union**, and the reported reason is the
/// one an operator acts on.
#[test]
fn keep_last_and_keep_days_are_a_union_with_a_stated_reason() {
    let scratch = Scratch::of("union", "");
    let store = scratch.read_only_handle();
    let report = evaluate(&store, ARCHIVE_URL, "", &rule(Some(3), Some(7)), now())
        .expect("the fixture archive evaluates");

    assert_eq!(report.keep_last, Some(3));
    assert_eq!(report.keep_days, Some(7));
    assert_eq!(
        report.sets_kept,
        vec!["backup-005", "backup-002", "backup-004"],
        "sets 1-3 by window are inside keepLast: 3 and inside keepDays: 7"
    );
    assert!(
        report.note.is_none(),
        "a configured rule leaves `note` unset; the note is for the no-rule case"
    );

    let removed = &report.sets_that_would_be_removed;
    assert_eq!(
        removed.len(),
        2,
        "two of five sets are selected: {removed:?}"
    );

    // SET 4 — beyond keepLast only. Its rank is part of the reason, because
    // "beyond the last three" is unreadable without saying which one it is.
    assert_eq!(removed[0].backup_id, "backup-003");
    assert_eq!(
        removed[0].reason,
        RemovalReason::BeyondKeepLast { rank: 4 },
        "set 4 by window is beyond keepLast: 3 and is NOT older than keepDays: 7 (its window \
         ends 73 h before the evaluation instant), so the reason is BeyondKeepLast at rank 4"
    );

    // SET 5 — beyond keepLast AND older than keepDays. BOTH rules select it,
    // and the reported reason is the AGE, because that is the reason an
    // operator acts on: a set that is too old stays too old however the
    // count changes.
    assert_eq!(removed[1].backup_id, "backup-001");
    assert_eq!(
        removed[1].reason,
        RemovalReason::OlderThanKeepDays { days: 7 },
        "set 5 by window is both beyond keepLast: 3 (rank 5) and older than keepDays: 7 (its \
         window ends 200 h before the evaluation instant). When both rules select a set the \
         report says OlderThanKeepDays — reporting BeyondKeepLast here would tell an operator \
         to fix a count when the set is simply too old."
    );
}

/// No rule configured removes nothing, and says why in exactly one sentence.
#[test]
fn no_retention_rule_removes_nothing() {
    let scratch = Scratch::of("norule", "");
    let store = scratch.read_only_handle();
    let report =
        evaluate(&store, ARCHIVE_URL, "", &rule(None, None), now()).expect("the archive evaluates");

    assert!(
        report.sets_that_would_be_removed.is_empty(),
        "with neither bound set, no set is removable: {:?}",
        report.sets_that_would_be_removed
    );
    assert_eq!(
        report.note.as_deref(),
        Some(NO_RULE_NOTE),
        "the note is the exact sentence, because it is rendered into a status block a UI shows"
    );
    assert_eq!(
        report.note.as_deref(),
        Some("no retention rule is configured; nothing would be removed"),
        "and the constant is that sentence — spelled out here so a change to the constant is \
         a change a reviewer sees"
    );
    assert_eq!(report.sets_kept.len(), 5, "all five sets are kept");
    assert!(report.aws_cli.is_empty() && report.mc_cli.is_empty());
}

/// A bound that is not a non-negative count is **not applied**, and the report
/// says so rather than clamping it.
///
/// `keepLast: -1` clamped to 0 would report every set in the archive as
/// removable, in a status block whose other fields are `aws s3 rm` commands.
#[test]
fn a_negative_retention_bound_is_not_applied_and_the_report_says_so() {
    let scratch = Scratch::of("negative", "");
    let store = scratch.read_only_handle();
    let report = evaluate(&store, ARCHIVE_URL, "", &rule(Some(-1), None), now())
        .expect("the archive evaluates");

    assert_eq!(
        report.keep_last, None,
        "`keep_last` reports the bound AS IT WAS APPLIED, and a negative count is not applied"
    );
    assert!(
        report.sets_that_would_be_removed.is_empty(),
        "a bound that was not applied must select nothing — a clamp to 0 would select all five"
    );
    assert_eq!(report.note.as_deref(), Some(NO_RULE_NOTE));
}

// ===========================================================================
// Interface I13 — `Store` is blocking
// ===========================================================================

/// Every source file that touches the archive from a reconciler, and the two
/// properties interface **I13** is.
///
/// SIX FILES ARE NAMED; the ones that do not exist at this slot are skipped
/// and counted, so the scan cannot silently shrink to nothing.
///
/// # `evidence_store.rs` IS THE SIXTH, AND IT IS THE ONE THAT CONSTRUCTS
///
/// D2 §3.10's amendment. `crate::evidence_store::StoreCache::get_or_build`
/// builds a read-only handle per allowlisted `ControllerIdentity` destination,
/// which makes it the SECOND `Store` construction site in this crate after
/// `main.rs` — and a constructor is exactly where `Store` builds and drives its
/// own current-thread runtime, so it is the shape this guard exists for. It is
/// named here so the async-region walk covers the file, and
/// [`no_store_call_is_made_outside_spawn_blocking`]'s second assertion admits
/// EXACTLY ONE `Store::read_only_with(` site, in this file, beside the one
/// `Store::read_only_from_url(` site in `main.rs`.
const I13_FILES: [&str; 6] = [
    "crates/weirkeeper/src/retention.rs",
    "crates/weirkeeper/src/controllers/backup_schedule.rs",
    "crates/weirkeeper/src/controllers/backup.rs",
    "crates/weirkeeper/src/controllers/restore.rs",
    "crates/weirkeeper/src/verification.rs",
    "crates/weirkeeper/src/evidence_store.rs",
];

/// Tokens that mean "this line calls into `Store`".
///
/// `observe_archive(` NAMES NO `Store` AT ALL, AND THAT IS WHY IT IS HERE.
/// `controllers::backup::observe_archive` is the single non-async function
/// holding a reconciler's two `store.get` calls, so its CALL SITE is where
/// I13 is kept or broken while the site itself mentions neither `Store` nor
/// `store.`. Added at Task 20 (ruling 1): `tests/backup_controller.rs`'s
/// module-local twin already had it, and a token list two guards disagree
/// about is a token list one of them is blind through.
///
/// `observe_scorecard(` IS THE `Restore` TWIN, AND ITS ABSENCE MADE THIS
/// GUARD BLIND TO THIS TASK'S OWN FILE. `controllers::restore::observe_scorecard`
/// holds the single `Store::get` on the `Restore` path, and its call site —
/// inside `reconcile`'s `async move` oracle — names neither `Store` nor
/// `store.` either. Added at Task 20 fix round 1 (review H1), MEASURED BOTH
/// WAYS on the review's own plant: `observe_scorecard(&handle, &key)` called
/// directly in that oracle, outside `spawn_blocking`, left
/// [`no_store_call_is_made_outside_spawn_blocking`] GREEN at **34 passed / 0
/// failed** against the seven-token list, and FAILS at **33 passed / 1
/// failed** naming `crates/weirkeeper/src/controllers/restore.rs:2336` with
/// this eighth token in place; the unplanted tree is **34 / 0** either way.
/// The shipped call IS inside `spawn_blocking` — this was a guard-coverage
/// defect, and the task-20 report's M6/P4 claim that the fixed guard already
/// caught this plant was FALSE as measured (STANDING RULE 21: a guard the
/// ledger records as closed while it asserts nothing is worse than none).
///
/// The re-plant of `observe_archive(` in `controllers/backup.rs` was measured
/// in the same round and still dies: **33 passed / 1 failed** naming
/// `crates/weirkeeper/src/controllers/backup.rs`, so the seventh token's own
/// coverage is intact and the eighth is additive.
///
/// `verify_evidence(` IS THE TASK-24 TWIN OF THE TWO ABOVE, AND IT IS ADDED
/// WITH THE FILE IT GUARDS. `verification::verify_evidence` holds the two
/// `store.get` calls of the verification path and its call site —
/// `verification::verify_oracle`'s `async move` block — names neither `Store`
/// nor `store.`, so without this ninth token a mutant that called it directly
/// from that block would leave this guard green and panic with *Cannot start
/// a runtime from within a runtime* at the first verification. Same defect
/// class as `observe_scorecard(`'s, same fix, and measured the same way (see
/// the task-24 report).
const STORE_CALL_TOKENS: [&str; 9] = [
    "Store::",
    "store.",
    ".manifest_facts(",
    ".list_manifests(",
    ".list_manifest_keys(",
    "retention::evaluate(",
    "observe_archive(",
    "observe_scorecard(",
    "verify_evidence(",
];

/// Every marker that opens an ASYNC REGION, as this scan understands one.
///
/// AN `async fn` BODY IS NOT THE ONLY PLACE A NESTED RUNTIME PANICS. A future
/// built by an `async move { … }` block — which is exactly the shape
/// `controllers::backup::reconcile`'s archive oracle takes, and the shape
/// `controllers::restore`'s takes — is polled on the reconciler's runtime
/// wherever it was written, so a `Store` call inside one is the same fatal
/// call whether or not the enclosing `fn` is `async`. The original walk saw
/// only `async fn `, which meant an oracle closure declared inside a
/// SYNCHRONOUS `fn` was invisible to it. Added at Task 20 (ruling 1).
const ASYNC_REGION_MARKERS: [&str; 3] = ["async fn ", "async move {", "async {"];

/// **Interface I13.** No `Store` call is made outside `spawn_blocking`, and
/// the handle is constructed exactly once, in `main.rs`.
///
/// WHY THIS IS A TEST AND NOT A COMMENT. Every `Store` method drives its own
/// current-thread runtime, and `Runtime::block_on` from a thread already
/// driving one panics with *Cannot start a runtime from within a runtime*;
/// `kube`'s `Controller` drives every reconciler ON a runtime. A direct call
/// therefore COMPILES CLEANLY and dies at the first retention reconcile — the
/// one failure mode a type checker cannot see and a green unit suite does not
/// reach.
///
/// # WHAT TASK 20 FIXED HERE, AND WHY IT WAS NOT COSMETIC
///
/// Three defects, all found by planting a mutant and watching it survive
/// (Task 17 re-review, concern 1 — accepted, and routed to this task):
/// [`sanitize`] blanked everything after a LIFETIME tick, the region walk saw
/// only `async fn ` and not `async move {`, and [`STORE_CALL_TOKENS`] did not
/// name `observe_archive(` — the one call whose site holds two `store.get`s
/// while mentioning no `Store`. Each is fixed at its own declaration, with
/// the measurement in that declaration's doc comment, and
/// [`the_sanitizer_knows_a_lifetime_from_a_character_literal`] tests the
/// sanitizer before this scan trusts it.
#[test]
fn no_store_call_is_made_outside_spawn_blocking() {
    let mut scanned = 0;
    let mut offences: Vec<String> = Vec::new();
    // Counted so a walk that stopped finding regions cannot pass silently:
    // this scan's whole verdict is "the token is inside an async region and
    // outside every `spawn_blocking`", and a region list that came back empty
    // makes every token unreachable and the assertion vacuous.
    let mut regions_seen = 0usize;

    for relative in I13_FILES {
        let Some(raw) = source(relative) else {
            continue;
        };
        scanned += 1;
        let text = sanitize(&raw);

        // Every ASYNC REGION in the file, as a byte span: `async fn ` bodies
        // AND `async move {` / `async {` blocks. See
        // `ASYNC_REGION_MARKERS` for why the blocks are not optional.
        let mut async_bodies: Vec<(usize, usize)> = Vec::new();
        for marker in ASYNC_REGION_MARKERS {
            for start in occurrences(&text, marker) {
                if let Some(span) = block_after(&text, start) {
                    async_bodies.push(span);
                }
            }
        }
        regions_seen += async_bodies.len();
        // Every `spawn_blocking(…)` argument group, as a byte span.
        let blocking: Vec<(usize, usize)> = occurrences(&text, "spawn_blocking(")
            .into_iter()
            .filter_map(|i| group_after(&text, i))
            .collect();

        for token in STORE_CALL_TOKENS {
            for at in occurrences(&text, token) {
                // `store.` is a substring of `restore.spec` — see
                // `token_at_word_boundary` for the eight false positives that
                // made this check necessary.
                if !token_at_word_boundary(&text, at, token) {
                    continue;
                }
                let inside_async = async_bodies.iter().any(|(s, e)| at > *s && at < *e);
                if !inside_async {
                    continue;
                }
                if blocking.iter().any(|(s, e)| at > *s && at < *e) {
                    continue;
                }
                offences.push(format!(
                    "{relative}:{} names `{token}` inside an async region and outside every \
                     `spawn_blocking(…)` closure",
                    line_of(&text, at)
                ));
            }
        }
    }

    assert!(
        regions_seen >= 5,
        "the walk found {regions_seen} async regions across {scanned} scanned files. The three \
         files that exist at this slot hold several `async fn`s and at least one `async move` \
         block each, so a count this low means the sanitizer or the brace matcher is broken and \
         every token below is unreachable — the shape that let a planted `store.get(…)` survive \
         this guard at 32 passed / 0 failed."
    );

    // EVERY NAMED FILE, NOT "AT LEAST TWO" — Task 24.
    //
    // `I13_FILES` named `crates/weirkeeper/src/verification.rs` from the slot
    // it was written, against a file that did not exist yet, and the walk
    // `continue`s past a file it cannot read. With a floor of two, that entry
    // asserted NOTHING while reading as coverage — the shape STANDING RULE 21
    // calls worse than no guard, because the ledger records it as closed. All
    // five exist as of this task, so the floor is all five and a file deleted
    // or renamed out from under this list fails here instead of going quiet.
    assert_eq!(
        scanned,
        I13_FILES.len(),
        "the scan reached {scanned} of the {} named files. Every one of them exists at this \
         slot, so a scan that found fewer is a walk that skipped a file whose `Store` calls \
         nobody then checked: {I13_FILES:?}",
        I13_FILES.len()
    );
    assert!(
        offences.is_empty(),
        "interface I13: every `Store` call from a reconciler goes through \
         `tokio::task::spawn_blocking(move || …).await`, because `Store` drives its own \
         current-thread runtime and `kube` drives reconcilers ON one — a direct call panics \
         with *Cannot start a runtime from within a runtime* at the first reconcile, having \
         compiled cleanly.\n  {}",
        offences.join("\n  ")
    );

    // ---- SECOND ASSERTION: the handle is constructed exactly ONCE ----------
    //
    // A handle rebuilt per reconcile discards the connection pool on every
    // reconcile AND reintroduces the nested-runtime panic, because a
    // constructor is where `Store` builds and drives its runtime.
    let mut construction_sites: Vec<String> = Vec::new();
    let mut sanctioned_sites: Vec<String> = Vec::new();
    let mut writable_sites: Vec<String> = Vec::new();
    for (relative, raw) in weirkeeper_sources() {
        let text = sanitize(&raw);
        for at in occurrences(&text, "Store::read_only_from_url(") {
            construction_sites.push(format!("{relative}:{}", line_of(&text, at)));
        }
        // D2 §3.10's amendment: the EXPLICIT read-only constructor, which the
        // `ControllerIdentity` evidence cache needs because its whole point is
        // that neither addressing nor transport nor credentials come from this
        // process's environment.
        for at in occurrences(&text, "Store::read_only_with(") {
            sanctioned_sites.push(format!("{relative}:{}", line_of(&text, at)));
        }
        for at in occurrences(&text, "Store::from_url(") {
            writable_sites.push(format!("{relative}:{}", line_of(&text, at)));
        }
        for at in occurrences(&text, "Store::from_url_with(") {
            writable_sites.push(format!("{relative}:{}", line_of(&text, at)));
        }
    }
    assert_eq!(
        construction_sites.len(),
        1,
        "the controller's archive handle is constructed exactly once, in \
         `crates/weirkeeper/src/main.rs`, before the tokio runtime exists, and shared as \
         `Arc<Store>` (interface I13). Found: {construction_sites:?}"
    );
    assert!(
        construction_sites[0].starts_with("crates/weirkeeper/src/main.rs:"),
        "the one construction site must be `main.rs` — a handle built anywhere else is a \
         handle built per reconcile. Found: {construction_sites:?}"
    );
    // ---- THE ONE SANCTIONED SECOND SITE (D2 §3.10, W7) --------------------
    //
    // `main.rs` builds the global handle before the runtime exists. The
    // evidence cache cannot: its handles are per-DESTINATION, and a destination
    // is an object that arrives long after `main` returned. So it constructs
    // inside `spawn_blocking`, which the FIRST assertion above already
    // requires of it (the file is in `I13_FILES`), and this assertion pins the
    // site to exactly one function in exactly one file.
    //
    // A SECOND ENTRY HERE IS A REVIEWABLE EVENT AND NOT A CONVENIENCE. Every
    // such site is a tokio runtime and a connection pool built per call unless
    // something caches it; the cache is the reason this one is admitted.
    assert_eq!(
        sanctioned_sites.len(),
        1,
        "the EXPLICIT read-only constructor `Store::read_only_with` is called from exactly one \
         place in this crate — `evidence_store::StoreCache::get_or_build`, inside \
         `spawn_blocking` — because every call to it builds a tokio runtime and a connection \
         pool that only that cache keeps. Found: {sanctioned_sites:?}"
    );
    assert!(
        sanctioned_sites[0].starts_with("crates/weirkeeper/src/evidence_store.rs:"),
        "the one sanctioned `Store::read_only_with` site is `evidence_store.rs`. Found: \
         {sanctioned_sites:?}"
    );
    assert!(
        writable_sites.is_empty(),
        "guard G-RET: this crate must never name the WRITABLE constructor, in either its \
         environment-reading (`Store::from_url`) or its explicit (`Store::from_url_with`) \
         form. Found: {writable_sites:?}"
    );
}

/// Every `.rs` under `crates/weirkeeper/src`, as `(relative path, body)`.
fn weirkeeper_sources() -> Vec<(String, String)> {
    let root = repo_root();
    let mut files = Vec::new();
    rs_files(&root.join("crates/weirkeeper/src"), &mut files);
    files.sort();
    assert!(
        files.len() >= 10,
        "the source walk found {} files under crates/weirkeeper/src — a walk that visits \
         nothing asserts nothing",
        files.len()
    );
    files
        .into_iter()
        .map(|p| {
            let rel = p
                .strip_prefix(&root)
                .expect("every walked path is under the workspace root")
                .to_string_lossy()
                .replace('\\', "/");
            let body = std::fs::read_to_string(&p).expect("a walked source file is readable");
            (rel, body)
        })
        .collect()
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

// ===========================================================================
// The gate
// ===========================================================================

const GATE: &str = "scripts/check-no-archive-write.sh";

fn gate_source() -> String {
    required_source(GATE)
}

/// One of the gate's two token lists, read out of its own heredoc.
///
/// TWO LISTS, BECAUSE THERE ARE TWO ROOTS (Task 19 review, G-RET (c)).
/// `TOKENS_CONTROL_PLANE` covers `crates/weirkeeper/src`, where a Kubernetes
/// `api.delete(` is legitimate and the delete token must therefore be
/// receiver-anchored; `TOKENS_STORE` covers `crates/logweir-store/src`, which
/// holds no Kubernetes client and where an unanchored `.delete(` is correct.
fn gate_tokens(list: &str) -> Vec<String> {
    let src = gate_source();
    let opener = format!("{list}=\"$(cat <<'EOF'");
    let start = src
        .find(&opener)
        .unwrap_or_else(|| panic!("the gate declares `{list}` in a heredoc a test can read"));
    let rest = &src[start..];
    let body_start = rest
        .find('\n')
        .expect("the heredoc opener ends in a newline")
        + 1;
    let body_end = rest[body_start..]
        .find("\nEOF")
        .expect("the heredoc is terminated")
        + body_start;
    rest[body_start..body_end]
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect()
}

/// Membership in `just lint` is what makes this a gate on a laptop before a
/// push: `ci.yml` mirrors `just gate` (green since 2026-09-12). Same shape as
/// `one_signer_gate.rs::just_lint_runs_the_one_signer_gate`.
#[test]
fn the_no_archive_write_gate_is_in_just_lint() {
    let justfile = required_source("justfile");
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
        body.contains("check-no-archive-write.sh"),
        "`just lint` is where a guard becomes enforced rather than asserted; removing the \
         recipe line silently disarms G-RET. The `lint` body was:\n{body}"
    );
}

/// The gate has **no path exemption list**, and its delete token is
/// receiver-anchored rather than the bare word.
///
/// THESE TWO PROPERTIES ARE ONE PROPERTY. A gate that fires on its first run
/// against a correct implementation gets an exemption added or a token
/// removed, and either outcome deletes the part of the grep that catches a
/// real object-store delete. The comment strip and the receiver anchor are
/// what make the grep precise enough not to need an exemption list.
#[test]
fn the_gate_has_no_path_exemptions() {
    let src = gate_source();

    // No shell variable whose NAME says "some paths do not count". Asserted
    // on the assignment and not on the word, because the script's own prose
    // has to be able to say "there is no path exemption list".
    for line in src.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with('#') {
            continue;
        }
        if let Some((lhs, _)) = trimmed.split_once('=') {
            let name = lhs.trim().trim_start_matches("export ").trim();
            let looks_like_an_exemption = name
                .chars()
                .all(|c| c.is_ascii_uppercase() || c == '_' || c.is_ascii_digit())
                && ["ALLOW", "EXEMPT", "SKIP", "IGNORE", "EXCLUDE"]
                    .iter()
                    .any(|w| name.contains(w));
            assert!(
                !looks_like_an_exemption,
                "{GATE} assigns `{name}`, which reads as a path exemption list. There is \
                 none, deliberately: an exemption added to silence a doc comment removes the \
                 part of the grep that catches a real object-store delete. Line: {line}"
            );
        }
    }

    let tokens = gate_tokens("TOKENS_CONTROL_PLANE");
    assert!(
        tokens.len() >= 5,
        "the gate's control-plane token list is {tokens:?} — a list this short is not covering \
         the write surface it claims to"
    );
    for expected in [
        "Store::from_url\\(",
        "put_create_only\\(",
        "PutMode",
        "\\.put\\(",
        "\\.put_opts\\(",
    ] {
        assert!(
            tokens.iter().any(|t| t == expected),
            "the gate must grep for /{expected}/ in the control plane; it greps {tokens:?}"
        );
    }

    // ------------------------------------------------------------------
    // The control plane's delete tokens are ANCHORED, every one of them.
    // ------------------------------------------------------------------
    //
    // An unanchored `\.delete\(` here would fire on `api.delete(` on a Job —
    // a correct, legitimate call — and a gate that fires on a correct
    // implementation gets an exemption added or a token removed. So every
    // control-plane token that is about deleting must either name a receiver
    // (an alternation ending in `\.delete\(`) or name a method no Kubernetes
    // client has.
    let unanchored_methods = ["delete_objects\\(", "delete_stream\\("];
    let delete_tokens: Vec<&String> = tokens.iter().filter(|t| t.contains("delete")).collect();
    assert!(
        delete_tokens.len() >= 2,
        "the control plane must grep for more than one delete spelling — the review's plants \
         showed `delete_objects(` and a `cargo fmt`-split chain both slipped past a single \
         token: {delete_tokens:?}"
    );
    for delete in &delete_tokens {
        if unanchored_methods.contains(&delete.as_str()) {
            continue;
        }
        assert!(
            delete.ends_with("\\.delete\\("),
            "a control-plane delete token must match `.delete(` and not a bare word, or be one \
             of {unanchored_methods:?}: {delete}"
        );
        assert!(
            delete.contains('|'),
            "a control-plane `.delete(` token must be RECEIVER-ANCHORED — an alternation of the \
             receiver names an object-store handle is spelled with — so `api.delete(` on a Job \
             stays legal: {delete}"
        );
        assert_ne!(
            delete.trim(),
            "\\.delete\\(",
            "the control plane must never grep an UNANCHORED `.delete(`: `api.delete(` on a Job \
             and a ConfigMap is a correct call this controller makes"
        );
    }
    assert!(
        delete_tokens
            .iter()
            .any(|t| t.contains("store") && t.contains("Store")),
        "one control-plane delete token must anchor on a store-shaped receiver name: \
         {delete_tokens:?}"
    );
    assert!(
        delete_tokens
            .iter()
            .any(|t| t.contains("(s|st)") || t.contains("(st|s)")),
        "and one must anchor on the bare abbreviations the review planted — `s.delete(` and \
         `st.delete(` both passed the first version of this gate: {delete_tokens:?}"
    );
    for expected in unanchored_methods {
        assert!(
            tokens.iter().any(|t| t == expected),
            "`object_store`'s bulk delete /{expected}/ needs no receiver anchor — no Kubernetes \
             client has that method name — and the review planted it: {tokens:?}"
        );
    }

    // ------------------------------------------------------------------
    // The STORE crate's list is deliberately unanchored, and has no puts.
    // ------------------------------------------------------------------
    //
    // `crates/logweir-store/src` is the crate that HOLDS the object-store
    // handle and the crate where a delete method would be added — the
    // review's plant of `.delete(` there left the first version of this gate
    // at rc 0, because its root was `crates/weirkeeper/src` alone. There is no
    // Kubernetes client in that crate, so no anchor is needed or wanted.
    let store_tokens = gate_tokens("TOKENS_STORE");
    assert!(
        store_tokens.iter().any(|t| t == "\\.delete\\("),
        "the store crate's list must grep an UNANCHORED `.delete(`: that crate holds no \
         Kubernetes client, so there is no legitimate delete in it to protect. Got: \
         {store_tokens:?}"
    );
    assert!(
        store_tokens
            .iter()
            .any(|t| t.contains("fn") && t.contains("delete")),
        "and it must forbid a delete method DEFINITION — `fn delete…` — because that is the \
         edit that would give the whole workspace a delete capability: {store_tokens:?}"
    );
    for put in [
        "put_create_only\\(",
        "\\.put\\(",
        "\\.put_opts\\(",
        "PutMode",
    ] {
        assert!(
            !store_tokens.iter().any(|t| t == put),
            "the store crate's list must NOT contain /{put}/: `put_create_only` is DEFINED \
             there, and it is the one write Global Constraint 6 allows. A gate that fires on \
             the correct implementation gets edited until it fires on nothing."
        );
    }

    for t in tokens.iter().chain(store_tokens.iter()) {
        assert_ne!(
            t.trim(),
            "delete",
            "the bare word `delete` must never be a token: it is a Kubernetes verb this \
             controller legitimately holds on Jobs and ConfigMaps, and an ordinary English \
             word in the doc comments this design requires"
        );
    }
}

/// The DEFAULT, argument-free run scans the store crate too.
/// **Task 19 review, G-RET (c).**
///
/// WHY THIS PLANTS IN THE REAL TREE. The reviewer's finding was not that the
/// gate's regexes were weak — it was that its ROOT was `crates/weirkeeper/src`
/// alone, so a `.delete(` added to `crates/logweir-store/src` (the crate that
/// actually holds the `Arc<dyn ObjectStore>`) left `just lint` green. A test
/// over a scratch fixture cannot observe that: the property is about which
/// directories the no-argument invocation walks. So this writes one file into
/// the real store crate's `src/`, runs the gate with **no arguments**, and
/// removes the file in a `Drop` guard that runs on a panic too.
///
/// The plant is a NEW, undeclared `.rs` file rather than an edit to
/// `lib.rs`: no `mod` declaration names it, so it is invisible to `rustc`
/// while it exists, and its removal cannot lose a byte of real source.
#[test]
fn the_gate_scans_the_store_crate_by_default() {
    let plant = repo_root().join(format!(
        "crates/logweir-store/src/zz_g_ret_plant_{}.rs",
        std::process::id()
    ));
    struct Unplant(PathBuf);
    impl Drop for Unplant {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    // The baseline first: a clean tree passes, so the rc 1 below is the plant
    // and not some pre-existing hit.
    let (rc, out) = run_gate_default();
    assert_eq!(
        rc,
        Some(0),
        "the clean tree must pass the gate before this test plants anything.\n{out}"
    );

    {
        let _unplant = Unplant(plant.clone());
        std::fs::write(
            &plant,
            "pub fn prune(s: &Store, k: &str) {\n    s.delete(k);\n}\n",
        )
        .expect("the store crate's src/ is writable");
        let (rc, out) = run_gate_default();
        assert_eq!(
            rc,
            Some(1),
            "a `.delete(` planted in crates/logweir-store/src must fail the DEFAULT run. That \
             crate is where the object-store handle lives and where a delete method would be \
             added; a gate rooted only at crates/weirkeeper/src leaves it unguarded and \
             `just lint` stays green.\n{out}"
        );
        assert!(
            out.contains("logweir-store"),
            "and the failure must name the store crate: {out}"
        );
    }

    let (rc, out) = run_gate_default();
    assert_eq!(
        rc,
        Some(0),
        "the plant must be gone: this test leaves the tree exactly as it found it.\n{out}"
    );
    assert!(!plant.exists(), "the plant file is removed");
}

/// The gate passes a doc comment that says delete, and fires on the same word
/// in code with a `Store` receiver.
///
/// ONE IMPLEMENTATION, TWO ENTRY POINTS. The gate takes an optional root so
/// this test can point the SAME script at a two-file fixture; the argument
/// cannot make the default run skip anything, which is why it is not an
/// exemption.
#[test]
fn the_gate_passes_a_doc_comment_that_says_delete() {
    let scratch = std::env::temp_dir().join(format!("logweir-t19-{}-gate", std::process::id()));
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch).expect("the scratch directory is creatable");
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(scratch.clone());

    // ARM 1 — a file whose ONLY `delete` is in a `///` doc comment, alongside
    // the exact sentences this design requires and a legitimate Kubernetes
    // `delete` on a Job. Zero hits.
    std::fs::write(
        scratch.join("prose.rs"),
        "/// The orphan case does not delete, does not repair.\n\
         /// No Logweir component in tag 1 holds any delete capability against object storage.\n\
         //! It never reaches store.delete( ), put_create_only( or PutMode.\n\
         pub async fn reap(api: &Api<Job>) {\n    \
             api.delete(\"j\", &Default::default()).await.ok();\n\
         }\n",
    )
    .expect("the fixture file is writable");
    let (rc, out) = run_gate(&scratch);
    assert_eq!(
        rc,
        Some(0),
        "a doc comment that says delete, and a Kubernetes `api.delete(` on a Job, must both \
         pass: a gate that fires on a correct implementation gets an exemption added or a \
         token removed, and either outcome disarms it.\n{out}"
    );

    // ARM 2 — the same word, outside a comment, on a `Store` receiver. One hit.
    std::fs::write(
        scratch.join("offender.rs"),
        "pub fn prune(store: &Store, key: &str) {\n    store.delete(key);\n}\n",
    )
    .expect("the fixture file is writable");
    let (rc, out) = run_gate(&scratch);
    assert_eq!(
        rc,
        Some(1),
        "a delete on a Store-shaped receiver must fail the gate.\n{out}"
    );
    assert!(
        out.contains("offender.rs:2"),
        "the failure must name the file and line it found: {out}"
    );
    assert!(
        !out.contains("prose.rs"),
        "and must NOT name the doc-comment file: {out}"
    );
}

/// Run the gate against `root`, returning `(exit code, stdout+stderr)`.
///
/// The status comes from `Output::status`, never through a pipe (STANDING
/// RULE 20).
/// Run the gate against a fixture CONTROL-PLANE root.
///
/// THE STORE ROOT IS A SCRATCH DIRECTORY TOO, and it has to be: this binary's
/// `the_gate_scans_the_store_crate_by_default` plants a file in the real
/// `crates/logweir-store/src` and `cargo test` runs these tests as threads of
/// one process, so a fixture run that let the store root default would
/// intermittently see that plant. Both roots are therefore explicit and
/// hermetic, and the DEFAULT roots are exercised by `run_gate_default` alone.
fn run_gate(root: &Path) -> (Option<i32>, String) {
    let store = root.join("store-root");
    std::fs::create_dir_all(&store).expect("the fixture store root is creatable");
    std::fs::write(store.join("clean.rs"), "pub fn ok() {}\n")
        .expect("the fixture store root gets one benign file");
    let mut cmd = std::process::Command::new("bash");
    cmd.arg(repo_root().join(GATE)).arg(root).arg(&store);
    finish_gate(cmd)
}

/// Run the gate with NO arguments — i.e. exactly as `just lint` runs it, over
/// both of its default roots.
fn run_gate_default() -> (Option<i32>, String) {
    let mut cmd = std::process::Command::new("bash");
    cmd.arg(repo_root().join(GATE));
    finish_gate(cmd)
}

fn finish_gate(mut cmd: std::process::Command) -> (Option<i32>, String) {
    let out = cmd
        .current_dir(repo_root())
        .output()
        .expect("bash runs the gate");
    let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&out.stderr));
    (out.status.code(), text)
}

// ===========================================================================
// The status block
// ===========================================================================

const NS: &str = "logweir-t19";
const UID: &str = "3f1c8a5e-0000-4000-8000-000000000019";

/// A `BackupSchedule` whose archive is the scratch tree's own prefix and whose
/// retention rule selects exactly one of the five sets. It carries a
/// `resourceVersion`, as every object the API server returns does, because the
/// status patch sends it back as its compare-and-swap precondition.
fn schedule_json(retention: &str) -> String {
    format!(
        r#"{{
  "apiVersion": "logweir.dev/v1alpha1",
  "kind": "BackupSchedule",
  "metadata": {{ "name": "nightly", "namespace": "{NS}", "uid": "{UID}", "generation": 1, "resourceVersion": "17" }},
  "spec": {{
    "schedule": "0 0 * * *",
    "sourceRef": {{ "name": "prod" }},
    "topics": ["orders"],
    "archive": {{ "url": "{ARCHIVE_URL}" }},
    "retention": {retention},
    "suspend": true
  }}
}}"#
    )
}

/// The owned-`Backup` LIST that every reconcile of a non-`Allow` schedule reads:
/// the reconciler filters it by controller UID to find the active child. A
/// suspended schedule that has never fired owns none, so the truthful answer is
/// an empty list.
fn no_backups_route() -> Route {
    Route {
        method: "GET",
        path_suffix: "/namespaces/logweir-t19/backups",
        status: 200,
        body: serde_json::json!({
            "apiVersion": "logweir.dev/v1alpha1",
            "kind": "BackupList",
            "metadata": { "resourceVersion": "1" },
            "items": []
        })
        .to_string(),
    }
}

/// The report lands on the schedule's status, through the reconciler.
///
/// `suspend: true` SO NO `Backup` IS CREATED, and the route table therefore
/// holds exactly two routes: the owned-`Backup` LIST every reconcile reads, and
/// the status `PATCH`. The double panics on a request it was not given a route
/// for, so "the reconciler asked for nothing else" is a property of this table
/// rather than a hope — and it makes the point that the retention report is
/// refreshed on EVERY reconcile, including one that fires nothing.
///
/// NOT `#[tokio::test]`, AND THE REASON **IS** INTERFACE I13. The handle is
/// built BEFORE the runtime exists, exactly as `main.rs` builds it: `Store`'s
/// constructors drive their own current-thread runtime, and building one
/// inside `#[tokio::test]`'s runtime panics with *Cannot start a runtime from
/// within a runtime* — which is what this test did, at
/// `crates/logweir-store/src/lib.rs`'s `build_backend`, before it was written
/// this way. That panic is I13 observed rather than argued, and the shape of
/// this test is the shape `main` has to have.
#[test]
fn the_retention_report_lands_on_the_schedule_status() {
    // The archive lives under its own prefix inside the scratch root, exactly
    // as `s3://kafka-backups/mvp-demo` describes, so the prefix the reconciler
    // derives from `spec.archive.url` is the one it must list under.
    //
    // BOTH OF THESE LINES ARE OUTSIDE THE RUNTIME BUILT BELOW.
    let scratch = Scratch::of("status", "mvp-demo");
    let store = std::sync::Arc::new(scratch.read_only_handle());

    let schedule: BackupSchedule = serde_json::from_str(&schedule_json(r#"{ "keepDays": 7 }"#))
        .expect("the fixture is a BackupSchedule");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the test runtime builds");
    let bodies = rt.block_on(async {
        let (client, _recorder, bodies) = mock_client_recording_bodies(vec![
            no_backups_route(),
            Route {
                method: "PATCH",
                path_suffix: "/backupschedules/nightly/status",
                status: 200,
                body: schedule_json(r#"{ "keepDays": 7 }"#),
            },
        ]);

        weirkeeper::controllers::backup_schedule::reconcile_schedule_with_archive(
            &schedule,
            &client,
            Some(&store),
            now(),
        )
        .await
        .expect("the reconcile succeeds");
        bodies
    });

    let status = patched_status(&bodies.lock().expect("the body recorder is readable"));
    let report = &status["retentionReport"];
    assert!(
        !report.is_null(),
        "the status patch must carry the whole retention report as a typed block, so \
         `kubectl get backupschedule -o yaml` shows it and the UI reads it with no extra \
         call. Got: {status}"
    );

    let removed = report["setsThatWouldBeRemoved"]
        .as_array()
        .unwrap_or_else(|| panic!("setsThatWouldBeRemoved is an array: {report}"));
    assert_eq!(
        removed.len(),
        1,
        "one of the five checked-in sets is older than keepDays: 7. Got: {report}"
    );
    assert_eq!(removed[0]["backupId"], "backup-001");
    assert_eq!(removed[0]["reason"], "OlderThanKeepDays");
    assert_eq!(removed[0]["days"], 7);
    assert_eq!(
        report["awsCli"][0], "aws s3 rm 's3://kafka-backups/mvp-demo/backup-001/' --recursive",
        "the command is a RENDERED STRING in the status. Logweir prints it, an operator runs \
         it, and no Logweir component in tag 1 holds any delete capability against object \
         storage."
    );
    assert_eq!(report["setsKept"].as_array().map(Vec::len), Some(4));
    assert_eq!(report["keepDays"], 7);
    assert!(
        report["note"].is_null(),
        "a configured rule leaves the note unset: {report}"
    );

    // And the archive is untouched by the reconcile, not merely by `evaluate`.
    assert_eq!(
        scratch.walk().len(),
        5,
        "the reconcile wrote nothing into the archive"
    );
}

/// A schedule with no archive handle writes no `retentionReport` key at all.
///
/// AN ABSENT KEY, NOT A `null`. A `Merge` patch carrying `retentionReport:
/// null` would DELETE a report the controller had already written, so a
/// controller that lost its archive handle would erase the last good report
/// rather than leave it standing.
#[tokio::test]
async fn no_archive_handle_means_no_retention_key_in_the_patch() {
    let schedule: BackupSchedule = serde_json::from_str(&schedule_json(r#"{ "keepDays": 7 }"#))
        .expect("the fixture is a BackupSchedule");
    let (client, _recorder, bodies) = mock_client_recording_bodies(vec![
        no_backups_route(),
        Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: schedule_json(r#"{ "keepDays": 7 }"#),
        },
    ]);

    weirkeeper::controllers::backup_schedule::reconcile_schedule_with_archive(
        &schedule,
        &client,
        None,
        now(),
    )
    .await
    .expect("the reconcile succeeds with no archive handle");

    let status = patched_status(&bodies.lock().expect("the body recorder is readable"));
    assert!(
        status.get("retentionReport").is_none(),
        "with no archive handle the patch must omit the key entirely — a null would delete a \
         report already on the object. Got: {status}"
    );
    // And the slot half is unaffected: a controller with no archive still runs
    // schedules.
    assert!(
        status.get("conditions").is_some(),
        "the schedule's own status is written regardless: {status}"
    );
}

/// The one `status` object out of the recorded `PATCH` bodies.
fn patched_status(bodies: &[SeenBody]) -> serde_json::Value {
    let patch = bodies
        .iter()
        .find(|b| b.method == "PATCH")
        .expect("the reconciler patches /status");
    let v: serde_json::Value =
        serde_json::from_str(&patch.body).expect("a recorded PATCH body is JSON");
    v["status"].clone()
}

/// The environment variable naming the controller's archive is documented and
/// is the one `main.rs` reads.
///
/// A controller whose archive is configured by an environment variable nobody
/// documents is a controller whose retention report is silently absent.
#[test]
fn the_archive_url_environment_variable_is_the_one_main_reads() {
    assert_eq!(ARCHIVE_URL_ENV, "LOGWEIR_ARCHIVE_URL");
    let main = sanitize(&required_source("crates/weirkeeper/src/main.rs"));
    assert!(
        main.contains("ARCHIVE_URL_ENV"),
        "`main.rs` must read the documented constant rather than a second spelling of the same \
         variable name"
    );
    let docs = required_source("docs/kubernetes.md");
    assert!(
        docs.contains(ARCHIVE_URL_ENV),
        "docs/kubernetes.md must name {ARCHIVE_URL_ENV}: an operator whose retention report \
         is missing has no other way to find out why"
    );
}

/// **`LOGWEIR_ARCHIVE_URL=""` IS UNSET, NOT A MISCONFIGURATION** — plan
/// erratum **E19(e)**, Task 24, and the row review finding 2 says was missing.
///
/// # The measurement
///
/// The shipped `config/manager/deployment.yaml` carries
/// `LOGWEIR_ARCHIVE_URL: ""` — the default install, retention and verification
/// display switched off. `std::env::var` returns **`Ok("")`** for a Kubernetes
/// `env:` entry with an empty `value:`, never `Err(NotPresent)`, so the empty
/// string used to reach `storage_url_for`, which correctly refused a URL with
/// no `://`, and the controller logged an **ERROR** naming an unreadable
/// archive URL on every clean start of the default install. Nothing was wrong
/// with the install.
///
/// It was invisible for as long as it was there because the same Deployment
/// pinned no `RUST_LOG` — see
/// `tests/verification.rs::the_deployment_sets_a_log_level`, its twin. Both
/// halves of defect 3 are one Deployment.
///
/// KILLS: reading the variable with a bare `std::env::var(...).ok()`, or with
/// any predicate that treats `Ok("")` as a configured archive. The `Err` arm
/// and the whitespace arm are here for the same reason: `value: " "` in a
/// manifest is the same operator saying the same thing.
#[test]
fn an_empty_archive_url_is_unset_and_not_an_error() {
    use weirkeeper::retention::configured_archive_url;

    assert_eq!(
        configured_archive_url(Ok(String::new())),
        None,
        "`env: [{{name: LOGWEIR_ARCHIVE_URL, value: \"\"}}]` reads back as `Ok(\"\")`, and it \
         means NO ARCHIVE. Sending it on is the ERROR line the default install used to print on \
         every clean start."
    );
    assert_eq!(
        configured_archive_url(Ok("   ".to_string())),
        None,
        "and so does a value that is only whitespace"
    );
    assert_eq!(
        configured_archive_url(Err(std::env::VarError::NotPresent)),
        None,
        "an absent variable is the same answer by the same name"
    );
    assert_eq!(
        configured_archive_url(Ok("  s3://kafka-backups/k8s-demo  ".to_string())),
        Some("s3://kafka-backups/k8s-demo".to_string()),
        "a REAL value is returned trimmed, so a manifest's trailing newline is not a URL"
    );

    // AND THE TWO ARMS DIVERGE WHERE IT MATTERS: the value that means "no
    // archive" is not one `storage_url_for` is ever asked about, and the one
    // that does reach it parses.
    assert!(
        weirkeeper::retention::storage_url_for("").is_err(),
        "the empty string IS refused by `storage_url_for` — which is correct, and is exactly why \
         it must never be handed to it"
    );
    assert!(weirkeeper::retention::storage_url_for("s3://kafka-backups/k8s-demo").is_ok());

    // …AND `main.rs` DECIDES THROUGH THIS FUNCTION, not through a second copy
    // of the predicate. The extraction is the whole point: a decision behind
    // `fn main` is reachable from no test.
    let main = sanitize(&required_source("crates/weirkeeper/src/main.rs"));
    assert!(
        main.contains("configured_archive_url("),
        "`main.rs` must reach this decision through `retention::configured_archive_url`; a \
         re-inlined predicate there would be untested again"
    );
}

/// The retention report never claims anything was deleted, and the shipped
/// document says who does the deleting.
#[test]
fn the_shipped_document_states_that_logweir_deletes_nothing() {
    let docs = required_source("docs/kubernetes.md");
    // WHITESPACE-COLLAPSED, because the sentence this asserts is one a
    // Markdown document line-wraps and a reader must never have to keep a
    // shipped sentence on one physical line to keep a test green.
    let flat = docs.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(
        flat.contains(
            "no Logweir component in tag 1 holds any delete capability against object storage"
        ),
        "docs/kubernetes.md must state the capability plainly: Logweir prints the commands, an \
         operator runs them"
    );
    assert!(
        flat.contains("Logweir prints them. An operator runs them."),
        "and it must say who runs the rendered commands"
    );
    assert!(
        flat.contains("32-character budget") && flat.contains("63 characters"),
        "docs/kubernetes.md must state the schedule-name budget the 63-character cap leaves \
         (Task 18 review carry, chain W): 15 + 1 + 15 fixed characters leave 32"
    );
    assert!(
        flat.contains("more than **one hour** before the controller looked"),
        "docs/kubernetes.md must name Task 18's one-hour missed-slot horizon, which chain W's \
         slot-7 owner could not carry"
    );
}

/// `RetentionReport::to_status` is a faithful projection: nothing in the
/// report is dropped on the way to the status block.
#[test]
fn the_status_block_carries_the_whole_report() {
    let report = RetentionReport {
        evaluated_at: now(),
        keep_last: Some(3),
        keep_days: Some(7),
        sets_kept: vec!["a".into()],
        sets_that_would_be_removed: vec![
            weirkeeper::retention::RemovableSet {
                backup_id: "b".into(),
                newest_record_at: now(),
                reason: RemovalReason::BeyondKeepLast { rank: 4 },
            },
            weirkeeper::retention::RemovableSet {
                backup_id: "c".into(),
                newest_record_at: now(),
                reason: RemovalReason::OlderThanKeepDays { days: 7 },
            },
        ],
        aws_cli: vec!["aws s3 rm 's3://b/c/' --recursive".into()],
        mc_cli: vec!["mc rm --recursive --force 'local/b/c/'".into()],
        skipped: vec![weirkeeper::retention::SkippedManifest {
            key: "x/manifest.json".into(),
            reason: "is not a backup manifest".into(),
        }],
        note: None,
    };
    let status = report.to_status();
    assert_eq!(status.keep_last, Some(3));
    assert_eq!(status.keep_days, Some(7));
    assert_eq!(status.sets_kept.as_deref(), Some(&["a".to_string()][..]));
    let removed = status
        .sets_that_would_be_removed
        .as_ref()
        .expect("the projection keeps the removable sets");
    assert_eq!(removed.len(), 2);
    assert_eq!(removed[0].reason, "BeyondKeepLast");
    assert_eq!(removed[0].rank, Some(4));
    assert_eq!(removed[0].days, None);
    assert_eq!(removed[1].reason, "OlderThanKeepDays");
    assert_eq!(removed[1].days, Some(7));
    assert_eq!(removed[1].rank, None);
    assert_eq!(status.aws_cli.as_ref().map(Vec::len), Some(1));
    assert_eq!(status.mc_cli.as_ref().map(Vec::len), Some(1));
    let skipped = status
        .skipped
        .as_ref()
        .expect("the projection keeps the skipped manifests");
    assert_eq!(skipped.len(), 1, "the skip is reported, never dropped");
    assert_eq!(skipped[0].key, "x/manifest.json");
    assert_eq!(skipped[0].reason, "is not a backup manifest");

    // AN EMPTY EVALUATION IS `Some([])`, NEVER `None`: "no evaluation
    // happened" is said by the whole block being absent, and a UI has to be
    // able to tell the two apart.
    let empty = RetentionReport {
        evaluated_at: now(),
        keep_last: None,
        keep_days: None,
        sets_kept: vec![],
        sets_that_would_be_removed: vec![],
        aws_cli: vec![],
        mc_cli: vec![],
        skipped: vec![],
        note: Some(NO_RULE_NOTE.to_string()),
    }
    .to_status();
    assert_eq!(empty.sets_that_would_be_removed, Some(vec![]));
    assert_eq!(empty.skipped, Some(vec![]));
    assert_eq!(empty.note.as_deref(), Some(NO_RULE_NOTE));
}

// ===========================================================================
// The rendered commands are shell-quoted — Task 19 review, finding F-1
// ===========================================================================

/// Ask the REAL shell how many arguments the rendered command has.
///
/// WHY `/bin/sh` AND NOT A PARSER WRITTEN HERE. The claim under test is
/// "an operator can paste this", and the authority on what a pasted string
/// means is the shell that would parse it — not a second implementation of
/// POSIX quoting written by the same hand as the first, which would be free to
/// be wrong in the same direction.
///
/// NEITHER `aws` NOR `mc` CAN RUN. Both are defined as shell FUNCTIONS that
/// print their own argv and nothing else, and a function shadows every `PATH`
/// lookup; `PATH` is additionally set to empty, so there is no path by which
/// the real tools could be reached even if the shadowing failed. The argv is
/// printed NUL-separated because one of the values under test IS a newline.
fn argv_of(rendered: &str) -> Vec<String> {
    let script = format!(
        "aws() {{ printf '%s\\0' \"$@\"; }}\nmc() {{ printf '%s\\0' \"$@\"; }}\n{rendered}\n"
    );
    let out = std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg(&script)
        .env_clear()
        .env("PATH", "")
        .output()
        .expect("/bin/sh parses the rendered command");
    assert!(
        out.status.success(),
        "the rendered command must PARSE and run the stub cleanly. Rendered:\n{rendered}\n\
         stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let mut args: Vec<String> = out
        .stdout
        .split(|b| *b == 0)
        .map(|s| String::from_utf8_lossy(s).into_owned())
        .collect();
    assert_eq!(
        args.pop().as_deref(),
        Some(""),
        "every argument is terminated by a NUL, so the split leaves one empty tail"
    );
    args
}

/// Both renderers turn `backup_id` into EXACTLY ONE path argument, whatever is
/// in it.
///
/// This is the assertion the whole finding comes down to. Before the fix,
/// `backup_id = "a;rm -rf ~"` rendered
/// `aws s3 rm s3://kafka-backups/mvp-demo/a;rm -rf ~/ --recursive` — which the
/// shell reads as TWO commands, the second of them `rm -rf ~/ --recursive`, in
/// a status field documented as "the exact commands an operator would run".
fn assert_one_path_argument(what: &str, backup_id: &str) {
    let aws = weirkeeper::retention::cli_rm(ARCHIVE_URL, backup_id);
    let argv = argv_of(&aws);
    assert_eq!(
        argv,
        vec![
            "s3".to_string(),
            "rm".to_string(),
            format!("s3://kafka-backups/mvp-demo/{backup_id}/"),
            "--recursive".to_string(),
        ],
        "cli_rm must render {what} as ONE path argument and nothing else. Rendered:\n{aws}"
    );

    let mc = weirkeeper::retention::mc_rm(ARCHIVE_URL, backup_id);
    let argv = argv_of(&mc);
    assert_eq!(
        argv,
        vec![
            "rm".to_string(),
            "--recursive".to_string(),
            "--force".to_string(),
            format!("local/kafka-backups/mvp-demo/{backup_id}/"),
        ],
        "mc_rm must render {what} as ONE path argument and nothing else. Rendered:\n{mc}"
    );
}

/// A `;` in a key rendered a SECOND, destructive command. This is F-1's own
/// reproduction.
#[test]
fn a_semicolon_in_a_backup_id_is_a_path_and_not_a_second_command() {
    assert_one_path_argument("a semicolon", "a;rm -rf ~");
}

#[test]
fn a_space_in_a_backup_id_is_one_argument_and_not_two() {
    assert_one_path_argument("a space", "has space");
}

#[test]
fn a_dollar_sign_in_a_backup_id_is_not_expanded() {
    assert_one_path_argument("a variable reference", "$HOME");
}

#[test]
fn a_backtick_in_a_backup_id_is_not_command_substituted() {
    assert_one_path_argument("a command substitution", "a`id`b");
}

#[test]
fn a_newline_in_a_backup_id_is_one_argument() {
    assert_one_path_argument("a newline", "a\nb");
}

/// The one character single quoting cannot contain: `'` → `'\''`.
#[test]
fn a_single_quote_in_a_backup_id_closes_escapes_and_reopens() {
    assert_one_path_argument("a single quote", "it's");
    assert_eq!(
        weirkeeper::retention::shell_quote("it's"),
        r"'it'\''s'",
        "the POSIX form is close, escaped literal quote, reopen — there is no backslash escape \
         for a quote INSIDE single quotes"
    );
}

/// The happy path is still legible, and the quoting is unconditional.
///
/// NO "DOES THIS NEED QUOTING?" BRANCH. That branch is where the holes grow:
/// every such predicate is a list of characters someone believed were safe.
#[test]
fn the_ordinary_command_is_quoted_too_and_still_reads_as_one_command() {
    let aws = weirkeeper::retention::cli_rm(ARCHIVE_URL, "backup-003");
    assert_eq!(
        aws, "aws s3 rm 's3://kafka-backups/mvp-demo/backup-003/' --recursive",
        "the quoting is unconditional"
    );
    assert_eq!(
        argv_of(&aws)[2],
        "s3://kafka-backups/mvp-demo/backup-003/",
        "and an ordinary id still parses to the same one path"
    );
    let mc = weirkeeper::retention::mc_rm(ARCHIVE_URL, "backup-003");
    assert_eq!(
        mc, "mc rm --recursive --force 'local/kafka-backups/mvp-demo/backup-003/'",
        "the `mc` spelling is quoted the same way"
    );
}

/// The BUCKET and PREFIX are quoted too, not only the id.
///
/// They come from `spec.archive.url`, which is also not Logweir's to trust.
#[test]
fn the_bucket_and_prefix_are_quoted_as_well_as_the_backup_id() {
    let hostile = "s3://buck et/pre;fix";
    let aws = weirkeeper::retention::cli_rm(hostile, "id");
    assert_eq!(
        argv_of(&aws),
        vec![
            "s3".to_string(),
            "rm".to_string(),
            "s3://buck et/pre;fix/id/".to_string(),
            "--recursive".to_string(),
        ],
        "a space or a `;` in the archive URL is part of the path, not a word break. \
         Rendered:\n{aws}"
    );
    let mc = weirkeeper::retention::mc_rm(hostile, "id");
    assert_eq!(
        argv_of(&mc),
        vec![
            "rm".to_string(),
            "--recursive".to_string(),
            "--force".to_string(),
            "local/buck et/pre;fix/id/".to_string(),
        ],
        "and the same for the `mc` target. Rendered:\n{mc}"
    );
}

// ===========================================================================
// One malformed manifest does not lose the report — Task 19 review, F-5
// ===========================================================================

/// N manifests with one malformed → N−1 evaluated, one skipped, rest correct.
///
/// THE FAILURE THIS REPLACES. `evaluate` returned `Err` on the first manifest
/// that did not parse, so an archive of five good backup sets plus one stray
/// `x/manifest.json` — the exact case `StoreError::NotAManifest`'s own doc
/// comment names, "a sibling JSON object was picked up by the
/// `/manifest.json` filter" — produced NO retention report at all: the
/// reconciler warned and omitted the whole block, which in the status is
/// indistinguishable from "no evaluation has happened".
#[test]
fn one_malformed_manifest_is_skipped_and_the_rest_of_the_archive_is_still_reported() {
    let scratch = Scratch::of("f5-skip", "");
    let store = scratch.read_only_handle();

    // The five checked-in sets, evaluated with no stray sibling — the answer
    // the skip must not disturb.
    let clean = evaluate(&store, ARCHIVE_URL, "", &rule(Some(3), Some(7)), now())
        .expect("the fixture archive evaluates");
    assert!(clean.skipped.is_empty(), "the fixture archive is readable");

    // N = 6: the five sets plus one sibling JSON object that is not a
    // manifest at all.
    let stray = scratch.path().join("stray");
    std::fs::create_dir_all(&stray).expect("the scratch subdirectory is creatable");
    std::fs::write(stray.join("manifest.json"), br#"{"hello":"world"}"#)
        .expect("the stray sibling is writable");

    let before = scratch.walk();
    let report = evaluate(&store, ARCHIVE_URL, "", &rule(Some(3), Some(7)), now())
        .expect("ONE malformed manifest must not cost the whole report");

    // N − 1 evaluated…
    assert_eq!(
        report.sets_kept.len() + report.sets_that_would_be_removed.len(),
        5,
        "six manifest keys, one unreadable — five sets must still be evaluated. Got kept \
         {:?} and removable {:?}",
        report.sets_kept,
        report.sets_that_would_be_removed
    );

    // …and the five are evaluated EXACTLY as they were without the stray, so
    // "keep the rest correct" is asserted and not assumed.
    assert_eq!(
        report.sets_kept, clean.sets_kept,
        "the skip must not change which sets are kept"
    );
    assert_eq!(
        report.sets_that_would_be_removed, clean.sets_that_would_be_removed,
        "nor which are removable, nor their ranks: ranks are counted over the sets that were \
         READ, so a skip can only ever move a surviving set to a lower rank — it can never \
         invent a removal"
    );
    assert_eq!(report.aws_cli, clean.aws_cli, "nor the rendered commands");
    assert_eq!(report.mc_cli, clean.mc_cli);

    // …plus ONE skipped entry, with the key and the reason.
    assert_eq!(
        report.skipped.len(),
        1,
        "the unreadable key is RECORDED, never silent. Got: {:?}",
        report.skipped
    );
    assert_eq!(
        report.skipped[0].key, "stray/manifest.json",
        "the entry names the key exactly as the archive listed it"
    );
    assert!(
        report.skipped[0]
            .reason
            .contains("is not a backup manifest"),
        "and the reason keeps the `NotAManifest` distinction Task 13 landed rather than \
         flattening it: {}",
        report.skipped[0].reason
    );
    assert!(
        report.skipped[0].reason.contains("no `topics` key"),
        "including WHY it is not a manifest: {}",
        report.skipped[0].reason
    );

    // A skipped set is in neither list, and no command names it.
    assert!(
        !report.sets_kept.iter().any(|s| s == "stray"),
        "a skipped key is not a kept set"
    );
    assert!(
        !report
            .sets_that_would_be_removed
            .iter()
            .any(|s| s.backup_id == "stray"),
        "and it is certainly not reported as removable — a set nobody could read must never \
         appear beside an `aws s3 rm`"
    );
    assert!(
        !report.aws_cli.iter().any(|c| c.contains("stray")),
        "and no rendered command names it: {:?}",
        report.aws_cli
    );

    // G-RET still: reading a broken manifest changes nothing.
    assert_eq!(
        before,
        scratch.walk(),
        "skipping an unreadable manifest writes nothing and removes nothing"
    );
}

/// Every kind of unreadable manifest is skipped, and each keeps its own
/// reason.
///
/// FOUR SHAPES, THREE OF THEM DISTINCT ERRORS. `{"hello":"world"}` and
/// `{"topics":5}` are `NotAManifest` (Task 13's carry: the TYPE, never the
/// value); a manifest-SHAPED body that bounds no window is `Backend`; and a
/// body that is not JSON at all is an `Io` parse failure. All four are facts
/// about one object, and none of them is a reason to stop reporting on the
/// others.
#[test]
fn every_unreadable_manifest_shape_is_skipped_with_its_own_reason() {
    let scratch = Scratch::of("f5-shapes", "");
    let store = scratch.read_only_handle();

    for (dir, body) in [
        ("no-topics", &br#"{"hello":"world"}"#[..]),
        ("topics-not-an-array", &br#"{"topics":5}"#[..]),
        (
            "no-segment",
            &br#"{"topics":[{"name":"t","partitions":[]}]}"#[..],
        ),
        ("not-json", &b"}{"[..]),
    ] {
        let d = scratch.path().join(dir);
        std::fs::create_dir_all(&d).expect("the scratch subdirectory is creatable");
        std::fs::write(d.join("manifest.json"), body).expect("the fixture body is writable");
    }

    let report = evaluate(&store, ARCHIVE_URL, "", &rule(Some(3), Some(7)), now())
        .expect("four unreadable manifests must not cost the report either");

    assert_eq!(
        report.sets_kept.len() + report.sets_that_would_be_removed.len(),
        5,
        "the five real sets are still evaluated"
    );
    assert_eq!(
        report.skipped.len(),
        4,
        "each unreadable key is its own entry: {:?}",
        report.skipped
    );

    let reason_for = |dir: &str| -> String {
        report
            .skipped
            .iter()
            .find(|s| s.key == format!("{dir}/manifest.json"))
            .unwrap_or_else(|| panic!("{dir} is skipped: {:?}", report.skipped))
            .reason
            .clone()
    };
    assert!(
        reason_for("no-topics").contains("declares no `topics` key"),
        "{}",
        reason_for("no-topics")
    );
    assert!(
        reason_for("topics-not-an-array").contains("`topics` is a number, not an array"),
        "the TYPE and never the value (Task 13 carry): {}",
        reason_for("topics-not-an-array")
    );
    assert!(
        reason_for("no-segment").contains("segment"),
        "a manifest-shaped body bounding no window is a different fact: {}",
        reason_for("no-segment")
    );
    assert!(
        !reason_for("not-json").is_empty(),
        "and a body that is not JSON still gets a reason"
    );
}

/// An archive nobody can LIST still fails hard.
///
/// THE ASYMMETRY IS DELIBERATE. A skipped manifest is a report with a named
/// gap; a failed list is a report about an unknown number of sets, which is
/// not a report at all. `evaluate` therefore returns `Err` for the list and
/// never for an individual manifest.
#[test]
fn a_list_failure_is_still_an_error_and_keeps_its_variant() {
    let scratch = Scratch::of("f5-list", "");
    let store = Store::read_only_from_url(&StorageUrl::Filesystem {
        path: scratch.path().join("does-not-exist"),
    });
    match store {
        // A handle over a missing directory either refuses to build or fails
        // the list; both are the hard failure this asserts, and neither is a
        // skip.
        Err(_) => {}
        Ok(store) => {
            let e = evaluate(&store, ARCHIVE_URL, "", &rule(Some(3), None), now())
                .expect_err("a list that cannot enumerate the archive is an error");
            assert!(
                matches!(
                    e,
                    StoreError::Io(_) | StoreError::Backend(_) | StoreError::NotFound(_)
                ),
                "the list failure keeps a variant rather than being flattened (F-6): {e:?}"
            );
        }
    }

    // AND THE MAPPING IS THE ONE F-6 ASKED FOR, asserted on the source
    // because `EngineError::Unsupported` is answered by a backend this test
    // cannot construct — `object_store`'s filesystem store supports listing.
    // `.map_err(|e| StoreError::Io(e.to_string()))` collapsed "this backend
    // cannot do that at all" into "storage said no", which is the
    // `NotFound`-versus-`Io` defect `StoreError`'s own doc comments argue
    // about, one enum along.
    let src = sanitize(&required_source("crates/weirkeeper/src/retention.rs"));
    assert!(
        src.contains("EngineError::Unsupported") && src.contains("StoreError::Backend"),
        "`evaluate` must map `EngineError::Unsupported` onto `StoreError::Backend` rather than \
         flattening every list failure to `Io` (Task 19 review, F-6)"
    );
    assert!(
        !src.contains("StoreError::Io(e.to_string())"),
        "and it must not flatten the list error to `Io` by stringifying it"
    );
}

// ===========================================================================
// A two-slash `file://` URL is refused — Task 19 review, finding F-7
// ===========================================================================

/// `file://relative/x` is an error, not `/relative/x`.
#[test]
fn a_two_slash_file_url_is_refused_rather_than_reinterpreted_as_a_root() {
    let e = weirkeeper::retention::storage_url_for("file://relative/x")
        .expect_err("a two-slash `file://` URL names no absolute path");
    assert!(
        e.contains("file:///absolute/path"),
        "the error must name the form an adopter should have written: {e}"
    );

    // The three-slash form still resolves, and to the path it names.
    match weirkeeper::retention::storage_url_for("file:///tmp/x")
        .expect("`file:///tmp/x` is an absolute path")
    {
        StorageUrl::Filesystem { path } => assert_eq!(path, std::path::PathBuf::from("/tmp/x")),
        other => panic!("a `file://` URL builds a filesystem handle, got {other:?}"),
    }
}

/// Every delete spelling the review planted, and every legitimate one it
/// showed must keep passing. **Task 19 review, G-RET (c).**
///
/// WHY A MATRIX AND NOT A SENTENCE. The reviewer's finding was a table of
/// eighteen plants, of which eight passed a gate that claimed to catch them.
/// STANDING RULE 21 says a guard whose mutant passes is worse than no guard,
/// so the widened token list and the `cargo fmt` chain join both need a test
/// that dies when they are narrowed back. The left column is what must FAIL
/// the gate; the right column is what must still pass, and it is the half that
/// keeps the gate from being edited into uselessness the first time it fires
/// on a correct Kubernetes delete.
#[test]
fn the_gate_catches_every_delete_spelling_the_review_planted() {
    let scratch = std::env::temp_dir().join(format!("logweir-t19-{}-plants", std::process::id()));
    let _ = std::fs::remove_dir_all(&scratch);
    let cp = scratch.join("cp");
    std::fs::create_dir_all(&cp).expect("the scratch directory is creatable");
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(scratch.clone());

    let probe = cp.join("probe.rs");
    let run = |body: &str| -> (Option<i32>, String) {
        std::fs::write(&probe, body).expect("the probe file is writable");
        run_gate(&cp)
    };

    // MUST FAIL. The six the first version of this gate MISSED are marked.
    for (what, body) in [
        (
            "store.delete(",
            "pub fn f(store: &Store) { store.delete(\"k\"); }\n",
        ),
        (
            "self.inner.delete(",
            "pub fn f(&self) { self.inner.delete(\"k\"); }\n",
        ),
        (
            "archive.delete(",
            "pub fn f(archive: &Store) { archive.delete(\"k\"); }\n",
        ),
        // MISSED: an abbreviated receiver.
        ("s.delete(", "pub fn f(s: &Store) { s.delete(\"k\"); }\n"),
        ("st.delete(", "pub fn f(st: &Store) { st.delete(\"k\"); }\n"),
        (
            "obj.delete(",
            "pub fn f(obj: &Store) { obj.delete(\"k\"); }\n",
        ),
        // MISSED: the method name, not the receiver.
        (
            "delete_objects(",
            "pub fn f(x: &Store) { x.delete_objects(v); }\n",
        ),
        (
            "delete_stream(",
            "pub fn f(x: &Store) { x.delete_stream(v); }\n",
        ),
        // MISSED: a chain `cargo fmt` split across two lines. The receiver and
        // the method are on DIFFERENT lines, which is what a receiver-anchored
        // per-line regex cannot see.
        (
            "a fmt-split store chain",
            "pub fn f(store: &Store) {\n    store\n        .delete(\"k\");\n}\n",
        ),
        (
            "a fmt-split self.inner chain",
            "pub fn f(&self) {\n    self.inner\n        .delete(\"k\");\n}\n",
        ),
        // The write surface, unchanged from round zero.
        (
            "Store::from_url(",
            "pub fn f() { let s = Store::from_url(&u); }\n",
        ),
        (
            "put_create_only(",
            "pub fn f(s: &S) { s.put_create_only(\"k\", b\"\"); }\n",
        ),
        ("PutMode", "pub fn f() { let m = PutMode::Create; }\n"),
        (".put(", "pub fn f(s: &S) { s.put(&p, b); }\n"),
        (".put_opts(", "pub fn f(s: &S) { s.put_opts(&p, b, o); }\n"),
    ] {
        let (rc, out) = run(body);
        assert_eq!(
            rc,
            Some(1),
            "the gate must catch `{what}`. This is the plant matrix the reviewer built; \
             narrowing the token list or dropping the chain join re-opens exactly these \
             holes.\n{out}"
        );
    }

    // MUST PASS. Every one of these is a correct call this control plane
    // either makes today or will make, and a gate that fires on any of them
    // gets an exemption added or a token deleted.
    for (what, body) in [
        (
            "api.delete( on a Job",
            "pub async fn f(api: &Api<Job>) { api.delete(\"j\", &dp).await.ok(); }\n",
        ),
        (
            "configmaps.delete(",
            "pub async fn f(configmaps: &Api<ConfigMap>) { configmaps.delete(\"c\", &dp).await; }\n",
        ),
        (
            "secrets.delete(",
            "pub async fn f(secrets: &Api<Secret>) { secrets.delete(\"s\", &dp).await; }\n",
        ),
        (
            "a fmt-split jobs chain",
            "pub async fn f(jobs: &Api<Job>) {\n    jobs\n        .delete(\"j\", &dp).await;\n}\n",
        ),
        (
            "api.delete_collection(",
            "pub async fn f(api: &Api<Job>) { api.delete_collection(&dp, &lp).await; }\n",
        ),
        (
            "Store::read_only_from_url(",
            "pub fn f() { let s = Store::read_only_from_url(&u); }\n",
        ),
    ] {
        let (rc, out) = run(body);
        assert_eq!(
            rc,
            Some(0),
            "the gate must NOT fire on `{what}`: `delete` is a Kubernetes verb this controller \
             legitimately holds on Jobs and ConfigMaps. A gate that fires on a correct \
             implementation gets edited until it fires on nothing.\n{out}"
        );
    }
}

/// **Task 16b, plan erratum E11(d), review finding M-1.** A steady schedule
/// **with an archive configured** does not rewrite `evaluatedAt`, and its
/// second pass sends no patch at all.
///
/// # The finding this pins
///
/// `retentionReport.evaluatedAt = now` was written on every pass whenever the
/// controller held an archive handle — the shipped configuration. That single
/// field made the whole status differ on every reconcile, so the patch bumped
/// `resourceVersion`, the schedule's own watch fired, and the reconciler spun:
/// the same defect as the two unconditional `lastTransitionTime` writes,
/// reached through a different field. Measured live at `376a09e` on
/// docker-desktop with `LOGWEIR_ARCHIVE_URL` set: ~3,850 own-object
/// `resourceVersion` bumps in 90 s, per schedule.
///
/// The rule is the `metav1.Condition` rule generalised: a "when computed"
/// timestamp moves when the thing it timestamps moves. The comparison is
/// `crds::backup_schedule::RetentionReport::same_findings_as`, which compares
/// every field of the report EXCEPT the instant.
///
/// A `#[test]` and not a `#[tokio::test]`, for the reason
/// `the_retention_report_lands_on_the_schedule_status` gives: `Store` drives
/// its own current-thread runtime (interface I13).
#[test]
fn a_steady_schedule_with_an_archive_does_not_rewrite_evaluated_at() {
    let scratch = Scratch::of("steady", "mvp-demo");
    let store = std::sync::Arc::new(scratch.read_only_handle());
    let body = schedule_json(r#"{ "keepDays": 7 }"#);
    let schedule: BackupSchedule =
        serde_json::from_str(&body).expect("the fixture is a BackupSchedule");

    let route = || {
        vec![
            no_backups_route(),
            Route {
                method: "PATCH",
                path_suffix: "/backupschedules/nightly/status",
                status: 200,
                body: schedule_json(r#"{ "keepDays": 7 }"#),
            },
        ]
    };
    // The recorder logs every request, the LIST included; these rows count
    // WRITES, which is what spins a watch.
    let writes = |seen: &[SeenBody]| seen.iter().filter(|b| b.method != "GET").count();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the test runtime builds");

    // PASS 1 — the report is written, `evaluatedAt` is this pass's clock.
    let first = rt.block_on(async {
        let (client, _rec, bodies) = mock_client_recording_bodies(route());
        weirkeeper::controllers::backup_schedule::reconcile_schedule_with_archive(
            &schedule,
            &client,
            Some(&store),
            now(),
        )
        .await
        .expect("the first reconcile succeeds");
        let seen = bodies.lock().expect("readable").clone();
        (patched_status(&seen), writes(&seen))
    });
    assert_eq!(first.1, 1, "the first pass writes once");
    assert_eq!(
        first.0["retentionReport"]["evaluatedAt"],
        serde_json::json!(now()),
        "the first evaluation stamps its own clock: {}",
        first.0
    );

    // The object as the API server now holds it.
    let mut stored = serde_json::Value::Null;
    apply_merge_patch(&mut stored, &first.0);
    let mut steady: BackupSchedule =
        serde_json::from_str(&body).expect("the fixture is a BackupSchedule");
    steady.status = Some(
        serde_json::from_value::<BackupScheduleStatus>(stored)
            .expect("the patched status is a BackupScheduleStatus"),
    );

    // PASS 2 — A DIFFERENT CLOCK, half an hour later, over the SAME archive.
    // In the same 24 h slot: the schedule is `suspend: true` so no slot
    // decision moves, and the archive is unchanged.
    //
    // HALF AN HOUR AND NOT SIX SINCE PLAT-05.2. D1 §6.7 re-inventories a
    // schedule's retained history every 60 minutes and records when in
    // `status.history.inventoriedAt`, so a second pass six hours out DOES move
    // the status — once, because it took a real inventory. That is 24 writes a
    // day, four orders of magnitude below the ~3,850-in-90-s defect this test
    // is about, and it is guarded on its own in `tests/schedule_history.rs`
    // (`the_hourly_inventory_is_the_only_thing_that_moves_a_settled_status`).
    // This pass stays inside the inventory window so that the only thing it
    // measures is the retention report's own idempotence.
    let later = now() + chrono::Duration::minutes(30);
    let second = rt.block_on(async {
        let (client, _rec, bodies) = mock_client_recording_bodies(route());
        weirkeeper::controllers::backup_schedule::reconcile_schedule_with_archive(
            &steady,
            &client,
            Some(&store),
            later,
        )
        .await
        .expect("the second reconcile succeeds");
        let seen = bodies.lock().expect("readable").clone();
        writes(&seen)
    });
    assert_eq!(
        second, 0,
        "the archive found exactly what it found before, so NOTHING is written — not the \
         report, not the condition, not a `PATCH` at all. Before this, one schedule with an \
         archive configured bumped its own resourceVersion ~3,850 times in 90 s"
    );

    // And the archive is still read-only.
    assert_eq!(
        scratch.walk().len(),
        5,
        "neither reconcile wrote into the archive"
    );
}

// ===========================================================================
// A `file://` archive reports its own sets — plan erratum E13(d)
// ===========================================================================

/// The controller's OWN composition, for one archive URL, end to end.
///
/// WHY THE HANDLE IS BUILT THIS WAY AND NOT FROM A `StorageUrl` LITERAL. The
/// defect E13(d) names is a DISAGREEMENT BETWEEN TWO PARSERS of the same
/// string: `main.rs:136` builds the controller's one handle from
/// [`weirkeeper::retention::storage_url_for`], and
/// `controllers::backup_schedule.rs` lists under
/// `bucket_and_prefix(url).1`. Every other test in this file hands
/// `Scratch::read_only_handle` a `StorageUrl::Filesystem` it wrote itself and
/// a prefix string it chose itself, so none of them can see the disagreement —
/// which is exactly how a `file://` archive reached a live cluster reporting
/// nothing. This helper spells the composition the controller performs, from
/// the URL and nothing else.
fn report_as_the_controller_composes_it(
    archive_url: &str,
    retention: &Retention,
) -> RetentionReport {
    let storage = weirkeeper::retention::storage_url_for(archive_url)
        .unwrap_or_else(|e| panic!("`{archive_url}` is a storage URL: {e}"));
    let store = Store::read_only_from_url(&storage)
        .expect("a read-only filesystem handle over an existing directory builds");
    let prefix = weirkeeper::retention::bucket_and_prefix(archive_url).1;
    evaluate(&store, archive_url, &prefix, retention, now()).expect("the archive lists")
}

/// `file://<root>/kafka-backups/mvp-demo` — the shape Task 16b's reviewer had
/// to work around on a live cluster — reports all five sets.
///
/// BEFORE THE FIX THIS REPORT WAS EMPTY, and the emptiness was silent: the
/// reconcile succeeds, the `retentionReport` block is written, and every list
/// in it is `[]`, which in a status is indistinguishable from an archive with
/// no backups in it. The split returned the scratch root's FIRST path segment
/// as a "bucket" (`private`, or `var`, depending on the temp dir) and handed
/// the whole remainder back as a listing prefix, so `evaluate` listed
/// `<root>/<root-minus-one-segment>/…` under a handle already rooted at
/// `<root>` and found nothing. Measured at `c585b77`: `sets_kept` 0,
/// `sets_that_would_be_removed` 0, `skipped` 0 — **0 of 5 sets accounted
/// for**.
#[test]
fn a_file_archive_under_a_sub_prefix_reports_its_five_sets() {
    let scratch = Scratch::of("e13d-prefix", "kafka-backups/mvp-demo");
    let archive_url = format!(
        "file://{}",
        scratch
            .path()
            .join("kafka-backups/mvp-demo")
            .to_str()
            .expect("the scratch path is UTF-8")
    );
    let report = report_as_the_controller_composes_it(&archive_url, &rule(Some(2), None));

    let accounted = report.sets_kept.len() + report.sets_that_would_be_removed.len();
    assert_eq!(
        accounted, 5,
        "all five fixture sets are accounted for over a `file://` archive. Got kept={:?} \
         removable={:?} skipped={:?} for {archive_url}",
        report.sets_kept, report.sets_that_would_be_removed, report.skipped
    );
    assert_eq!(
        report.sets_kept,
        vec!["backup-005".to_string(), "backup-002".to_string()],
        "and the two kept are the two newest BY WINDOW, exactly as over `s3://`"
    );
    assert_eq!(
        report.sets_that_would_be_removed.len(),
        3,
        "the other three are removable under `keepLast: 2`"
    );
    assert!(
        report.skipped.is_empty(),
        "nothing is unreadable in the fixture archive: {:?}",
        report.skipped
    );
}

/// `file://<root>` — an archive at the filesystem root of the handle, with no
/// sub-prefix at all — reports its five sets too.
///
/// THE SECOND ARM MATTERS BECAUSE THE OLD SPLIT WAS WRONG HERE AS WELL, and
/// for a reason a one-segment URL would hide: there is no sub-prefix to
/// double, but the old split still carved the ROOT PATH's own first segment
/// off as a bucket and returned the rest of the absolute path as a listing
/// prefix. Measured at `c585b77`: **0 of 5**.
#[test]
fn a_file_archive_at_the_handles_root_reports_its_five_sets() {
    let scratch = Scratch::of("e13d-root", "");
    let archive_url = format!(
        "file://{}",
        scratch.path().to_str().expect("the scratch path is UTF-8")
    );
    let report = report_as_the_controller_composes_it(&archive_url, &rule(Some(2), None));

    let accounted = report.sets_kept.len() + report.sets_that_would_be_removed.len();
    assert_eq!(
        accounted, 5,
        "all five fixture sets are accounted for with no sub-prefix. Got kept={:?} \
         removable={:?} skipped={:?} for {archive_url}",
        report.sets_kept, report.sets_that_would_be_removed, report.skipped
    );
}

/// The invariant the defect broke, stated directly: **the listing prefix and
/// the handle's own root come from the same URL and must agree.**
///
/// `Store::read_only_from_url` roots the backend per
/// `StorageUrl`, and `StorageUrl::prefix()` is what is left over for the
/// listing — the bucket for `s3`/`gs`, and for `Filesystem` the WHOLE path
/// with `prefix()` returning `""`. `bucket_and_prefix(url).1` is what the
/// reconciler actually lists under. If those two ever disagree the report is
/// silently wrong, so this row compares them per scheme rather than pinning a
/// literal.
#[test]
fn the_listing_prefix_agrees_with_the_root_the_handle_is_built_from() {
    for url in [
        "s3://kafka-backups/mvp-demo",
        "s3://kafka-backups",
        "gs://kafka-backups/mvp-demo",
        // ALL FOUR SUPPORTED SCHEMES, since Global Constraint 9 fixes the set
        // at `s3`, `gs`, `az` and `file` and the defect was on whichever one
        // this row did not enumerate. The `az` rows were excluded while the
        // split disagreed with the handle; they are the fix round's own gate.
        "az://acct/container/pfx",
        "az://acct/container",
        "az://acct/container/a/b",
        "file:///srv/archive/kafka-backups/mvp-demo",
        "file:///srv",
    ] {
        let storage = weirkeeper::retention::storage_url_for(url)
            .unwrap_or_else(|e| panic!("`{url}` is a storage URL: {e}"));
        assert_eq!(
            weirkeeper::retention::bucket_and_prefix(url).1,
            storage.prefix(),
            "`{url}`: the prefix the reconciler lists under must be the prefix left over by \
             the URL the handle is built from, or the report is over a path that holds nothing"
        );
    }
}

/// **`file://` — the root is the whole path and there is no prefix.**
#[test]
fn the_file_split_is_the_whole_path_as_the_root_and_no_prefix() {
    assert_eq!(
        weirkeeper::retention::bucket_and_prefix("file:///a/b/c"),
        ("/a/b/c".to_string(), String::new()),
        "a `file://` URL names no bucket: `LocalFileSystem::new_with_prefix` is rooted at the \
         whole path, so nothing is left over to list under"
    );
    assert_eq!(
        weirkeeper::retention::bucket_and_prefix("file:///a/b/c/"),
        ("/a/b/c".to_string(), String::new()),
        "a trailing slash is not a path segment"
    );
}

/// **`s3://` — unchanged: the bucket, then the remainder.**
#[test]
fn the_s3_split_is_the_bucket_and_the_remainder() {
    assert_eq!(
        weirkeeper::retention::bucket_and_prefix("s3://kafka-backups/mvp-demo"),
        ("kafka-backups".to_string(), "mvp-demo".to_string())
    );
    assert_eq!(
        weirkeeper::retention::bucket_and_prefix("s3://kafka-backups/a/b"),
        ("kafka-backups".to_string(), "a/b".to_string())
    );
    assert_eq!(
        weirkeeper::retention::bucket_and_prefix("s3://kafka-backups"),
        ("kafka-backups".to_string(), String::new())
    );
}

/// **`gs://` — unchanged: the bucket, then the remainder.**
#[test]
fn the_gs_split_is_the_bucket_and_the_remainder() {
    assert_eq!(
        weirkeeper::retention::bucket_and_prefix("gs://kafka-backups/mvp-demo"),
        ("kafka-backups".to_string(), "mvp-demo".to_string())
    );
    // THE THREE-SEGMENT CASE IS WHAT GIVES THIS ROW TEETH. A two-segment URL
    // splits the same way whether the split is taken at the FIRST `/` or the
    // LAST, so a row spelling only `gs://bucket/prefix` survives a
    // `rsplit_once` mutant. Measured: with `split_once` -> `rsplit_once` this
    // row is GREEN without the line below and RED with it.
    assert_eq!(
        weirkeeper::retention::bucket_and_prefix("gs://kafka-backups/a/b"),
        ("kafka-backups".to_string(), "a/b".to_string())
    );
    assert_eq!(
        weirkeeper::retention::bucket_and_prefix("gs://kafka-backups"),
        ("kafka-backups".to_string(), String::new())
    );
}

/// **`az://` — the CONTAINER is the root, and the remainder is the prefix.**
///
/// THIS ROW REPLACES `the_az_split_is_unchanged_and_still_disagrees_with_the_handle`,
/// whose own doc comment instructed exactly that once the controller ruled:
/// "fix `bucket_and_prefix` and DELETE THIS ASSERTION rather than inverting
/// it". The assertion it carried — that the split and the handle DISAGREE —
/// is now false by construction, and the agreement is asserted for all three
/// `az://` shapes by
/// `the_listing_prefix_agrees_with_the_root_the_handle_is_built_from` above.
///
/// `MicrosoftAzureBuilder::with_container_name` roots the backend at the
/// CONTAINER, two segments in, so the scheme-blind `("acct", "container/pfx")`
/// listed `container/pfx` INSIDE the container and reported nothing — the
/// same silent empty report E13(d) named for `file://`, on a supported
/// backend.
#[test]
fn the_az_split_is_the_container_and_the_remainder() {
    assert_eq!(
        weirkeeper::retention::bucket_and_prefix("az://acct/container/pfx"),
        ("container".to_string(), "pfx".to_string()),
        "the ACCOUNT is not part of either half: the handle is rooted at the container, and \
         an `mc` alias for Azure IS the account endpoint"
    );
    // THE THREE-SEGMENT CASE IS WHAT GIVES THIS ROW TEETH, for the same
    // reason the `gs://` row spells one: a two-segment tail splits the same
    // way at the FIRST `/` and at the LAST, so `az://acct/container/pfx`
    // alone survives a `split_once` -> `rsplit_once` mutant.
    assert_eq!(
        weirkeeper::retention::bucket_and_prefix("az://acct/container/a/b"),
        ("container".to_string(), "a/b".to_string()),
        "interior slashes are preserved in the prefix"
    );
    assert_eq!(
        weirkeeper::retention::bucket_and_prefix("az://acct/container"),
        ("container".to_string(), String::new()),
        "the two-segment form was broken too — there was no `az://` URL that worked"
    );
    assert_eq!(
        weirkeeper::retention::bucket_and_prefix("az://acct/container/pfx/"),
        ("container".to_string(), "pfx".to_string()),
        "a trailing slash is not a path segment"
    );
    // AN ACCOUNT WITH NO CONTAINER IS LEFT ON THE GENERIC SPLIT, exactly as
    // the refused two-slash `file://` form is: `storage_url_for` refuses it,
    // so no handle is ever built over it and no report is ever rendered from
    // it. There is nothing for this function to be right about.
    assert!(
        weirkeeper::retention::storage_url_for("az://acct").is_err(),
        "`az://acct` names no container, and the handle builder says so"
    );
    assert_eq!(
        weirkeeper::retention::bucket_and_prefix("az://acct"),
        ("acct".to_string(), String::new()),
        "and the split leaves that form on the generic path rather than inventing a container"
    );
    // The `mc` target follows `.0`: `local` is the ACCOUNT endpoint, so the
    // first element after the alias is the container.
    assert_eq!(
        weirkeeper::retention::mc_rm("az://acct/container/pfx", "backup-003"),
        "mc rm --recursive --force 'local/container/pfx/backup-003/'",
        "`local/acct/container/…` would name container `acct` to `mc`"
    );
}

/// A filesystem archive's `mc` target is the PATH, with no `local/` alias.
///
/// `local` is an `mc` ALIAS — a configured endpoint — and a `file://` archive
/// has no endpoint to alias: `mc` operates on a local path directly. Once the
/// split returns the absolute root, `format!("local/{bucket}")` would render
/// `local//srv/archive/…`, a double slash under an alias naming nothing.
#[test]
fn a_filesystem_archives_mc_target_is_the_path_and_not_an_alias() {
    assert_eq!(
        weirkeeper::retention::mc_rm("file:///srv/archive", "backup-003"),
        "mc rm --recursive --force '/srv/archive/backup-003/'",
        "a local path is already a complete `mc` target"
    );
    // And the `s3://` spelling is untouched.
    assert_eq!(
        weirkeeper::retention::mc_rm(ARCHIVE_URL, "backup-003"),
        "mc rm --recursive --force 'local/kafka-backups/mvp-demo/backup-003/'"
    );
}

/// **The printed remedy names the ARCHIVE SCHEME'S OWN TOOL.**
///
/// One row per scheme, each pinning the exact string, because this field is
/// documented — here, in the CRD's own field description and in
/// `docs/kubernetes.md` — as "the exact commands an operator would run", and
/// Task 19's review established that copy-and-paste IS the intended workflow.
/// A rendered string an operator cannot run is the whole finding: before this,
/// `cli_rm` interpolated the archive URL into `aws s3 rm` for EVERY scheme, so
/// a `file://` archive's report carried
/// `aws s3 rm 'file:///srv/archive/backup-003/' --recursive` — `aws s3 rm`
/// takes an `S3Uri`, and a `file://` URI is not one.
///
/// THE ROWS ASSERT THE STRING AND NOT AN `argv`. The `argv_of` helper above
/// runs the rendered command under `/bin/sh` against a stub function, which is
/// the right instrument for `aws` and `mc` and emphatically the WRONG one for
/// a line beginning `rm -rf`: a test that executes a removal command to check
/// its shape is the reaper this whole design replaced. The one-shell-word
/// property these rows depend on is `shell_quote`'s, asserted unconditionally
/// and per hostile character by the F-1 rows above.
#[test]
fn an_s3_archives_remedy_is_the_aws_cli_and_is_byte_identical() {
    assert_eq!(
        weirkeeper::retention::cli_rm("s3://kafka-backups/mvp-demo", "backup-003"),
        "aws s3 rm 's3://kafka-backups/mvp-demo/backup-003/' --recursive",
        "the `aws` CLI takes the `s3://` URL itself as the target — unchanged, byte for byte"
    );
    assert_eq!(
        weirkeeper::retention::cli_rm("s3://kafka-backups/mvp-demo/", "backup-003"),
        "aws s3 rm 's3://kafka-backups/mvp-demo/backup-003/' --recursive",
        "exactly one slash between the archive URL and the id, whatever the spec's trailing \
         slash looked like"
    );
}

#[test]
fn a_file_archives_remedy_is_rm_rf_on_the_archive_path() {
    let rendered = weirkeeper::retention::cli_rm("file:///srv/archive", "backup-003");
    assert_eq!(
        rendered, "rm -rf '/srv/archive/backup-003/'",
        "there is no cloud CLI for a filesystem archive and a backup set is a directory, so \
         the scheme's own tool is the shell — and the path is ONE shell word"
    );
    assert!(
        !rendered.starts_with("aws "),
        "THE WHOLE FINDING: `aws s3 rm` takes an `S3Uri`, so `aws s3 rm 'file:///…'` is not a \
         command an operator can run, in a field documented as the exact commands they would \
         run. Rendered:\n{rendered}"
    );
    assert_eq!(
        weirkeeper::retention::cli_rm("file:///srv/archive/kafka-backups/mvp-demo", "backup-003"),
        "rm -rf '/srv/archive/kafka-backups/mvp-demo/backup-003/'",
        "a sub-prefix is part of the path: `bucket_and_prefix` roots a `file://` archive at \
         the WHOLE path and leaves no prefix over"
    );
}

#[test]
fn a_gs_archives_remedy_is_the_gsutil_command() {
    assert_eq!(
        weirkeeper::retention::cli_rm("gs://kafka-backups/mvp-demo", "backup-003"),
        "gsutil -m rm -r 'gs://kafka-backups/mvp-demo/backup-003/'",
        "Google's CLI takes the `gs://` URL as the target: `-r` recurses, and `-m` \
         parallelises what is usually thousands of segment objects"
    );
}

#[test]
fn an_az_archives_remedy_is_the_az_cli_delete_batch() {
    assert_eq!(
        weirkeeper::retention::cli_rm("az://acct/container/pfx", "backup-003"),
        "az storage blob delete-batch --account-name 'acct' --source 'container' \
         --pattern 'pfx/backup-003/*'",
        "Azure's CLI takes no `az://` URL: the account and the container are separate \
         arguments — the same fact that roots the archive handle at the container — and the \
         key prefix is a glob"
    );
    assert_eq!(
        weirkeeper::retention::cli_rm("az://acct/container", "backup-003"),
        "az storage blob delete-batch --account-name 'acct' --source 'container' \
         --pattern 'backup-003/*'",
        "NO LEADING SLASH when there is no prefix: a blob name does not begin with one, and \
         `'/backup-003/*'` would match nothing"
    );
}
