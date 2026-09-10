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
use weirkeeper::crds::backup_schedule::{BackupSchedule, Retention};
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
/// positions and line structure preserved.
///
/// WHY SANITISE AT ALL. Two of the assertions below are about where a token
/// appears in CODE, and this file's own subjects are files whose doc comments
/// discuss exactly those tokens at length. Brace matching has the same
/// problem from the other end: `format!("{a}")` would unbalance any counter
/// that did not know it was inside a string.
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
        // A normal string or character literal.
        if b[i] == b'"' || b[i] == b'\'' {
            let quote = b[i];
            i += 1;
            while i < b.len() {
                if b[i] == b'\\' {
                    i += 2;
                    continue;
                }
                if b[i] == quote {
                    i += 1;
                    break;
                }
                i += 1;
            }
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
        report.aws_cli[0], "aws s3 rm s3://kafka-backups/mvp-demo/backup-003/ --recursive",
        "the first rendered command must name the first set the policy would remove, spelled \
         exactly as an operator would run it"
    );
    assert_eq!(
        report.mc_cli[0], "mc rm --recursive --force local/kafka-backups/mvp-demo/backup-003/",
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
/// FIVE FILES ARE NAMED; the ones that do not exist at this slot are skipped
/// and counted, so the scan cannot silently shrink to nothing.
const I13_FILES: [&str; 5] = [
    "crates/weirkeeper/src/retention.rs",
    "crates/weirkeeper/src/controllers/backup_schedule.rs",
    "crates/weirkeeper/src/controllers/backup.rs",
    "crates/weirkeeper/src/controllers/restore.rs",
    "crates/weirkeeper/src/verification.rs",
];

/// Tokens that mean "this line calls into `Store`".
const STORE_CALL_TOKENS: [&str; 6] = [
    "Store::",
    "store.",
    ".manifest_facts(",
    ".list_manifests(",
    ".list_manifest_keys(",
    "retention::evaluate(",
];

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
#[test]
fn no_store_call_is_made_outside_spawn_blocking() {
    let mut scanned = 0;
    let mut offences: Vec<String> = Vec::new();

    for relative in I13_FILES {
        let Some(raw) = source(relative) else {
            continue;
        };
        scanned += 1;
        let text = sanitize(&raw);

        // Every `async fn` body in the file, as a byte span.
        let mut async_bodies: Vec<(usize, usize)> = Vec::new();
        for start in occurrences(&text, "async fn ") {
            if let Some(span) = block_after(&text, start) {
                async_bodies.push(span);
            }
        }
        // Every `spawn_blocking(…)` argument group, as a byte span.
        let blocking: Vec<(usize, usize)> = occurrences(&text, "spawn_blocking(")
            .into_iter()
            .filter_map(|i| group_after(&text, i))
            .collect();

        for token in STORE_CALL_TOKENS {
            for at in occurrences(&text, token) {
                let inside_async = async_bodies.iter().any(|(s, e)| at > *s && at < *e);
                if !inside_async {
                    continue;
                }
                if blocking.iter().any(|(s, e)| at > *s && at < *e) {
                    continue;
                }
                offences.push(format!(
                    "{relative}:{} names `{token}` inside an `async fn` body and outside every \
                     `spawn_blocking(…)` closure",
                    line_of(&text, at)
                ));
            }
        }
    }

    assert!(
        scanned >= 2,
        "the scan reached {scanned} of the {} named files. `retention.rs` and \
         `controllers/backup_schedule.rs` both exist at this slot, so a scan that found fewer \
         than two is a broken walk asserting nothing.",
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
    let mut writable_sites: Vec<String> = Vec::new();
    for (relative, raw) in weirkeeper_sources() {
        let text = sanitize(&raw);
        for at in occurrences(&text, "Store::read_only_from_url(") {
            construction_sites.push(format!("{relative}:{}", line_of(&text, at)));
        }
        for at in occurrences(&text, "Store::from_url(") {
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
    assert!(
        writable_sites.is_empty(),
        "guard G-RET: this crate must never name the WRITABLE constructor. Found: \
         {writable_sites:?}"
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

/// The token list the gate greps for, read out of its own heredoc.
fn gate_tokens() -> Vec<String> {
    let src = gate_source();
    let start = src
        .find("TOKENS=\"$(cat <<'EOF'")
        .expect("the gate declares its token list in a heredoc a test can read");
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

/// Membership in `just lint` is what makes this a gate: `ci.yml` has never
/// executed on any commit. Same shape as
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

    let tokens = gate_tokens();
    assert!(
        tokens.len() >= 5,
        "the gate's token list is {tokens:?} — a list this short is not covering the write \
         surface it claims to"
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
            "the gate must grep for /{expected}/; it greps {tokens:?}"
        );
    }

    let delete_tokens: Vec<&String> = tokens.iter().filter(|t| t.contains("delete")).collect();
    assert_eq!(
        delete_tokens.len(),
        1,
        "exactly one token is about deleting: {delete_tokens:?}"
    );
    let delete = delete_tokens[0];
    assert!(
        delete.ends_with("\\.delete\\("),
        "the delete token must match `.delete(` and not a bare word: {delete}"
    );
    assert!(
        delete.contains("store") && delete.contains("Store") && delete.contains('|'),
        "the delete token must be RECEIVER-ANCHORED — an alternation of Store/ObjectStore-shaped \
         receivers before `.delete(` — so `api.delete(` on a Job stays legal: {delete}"
    );
    for t in &tokens {
        assert_ne!(
            t.trim(),
            "delete",
            "the bare word `delete` must never be a token: it is a Kubernetes verb this \
             controller legitimately holds on Jobs and ConfigMaps, and an ordinary English \
             word in the doc comments this design requires"
        );
    }
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
fn run_gate(root: &Path) -> (Option<i32>, String) {
    let out = std::process::Command::new("bash")
        .arg(repo_root().join(GATE))
        .arg(root)
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
/// retention rule selects exactly one of the five sets.
fn schedule_json(retention: &str) -> String {
    format!(
        r#"{{
  "apiVersion": "logweir.dev/v1alpha1",
  "kind": "BackupSchedule",
  "metadata": {{ "name": "nightly", "namespace": "{NS}", "uid": "{UID}", "generation": 1 }},
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

/// The report lands on the schedule's status, through the reconciler.
///
/// `suspend: true` SO NO `Backup` IS CREATED, and the route table therefore
/// holds exactly one route: the status `PATCH`. The double panics on a request
/// it was not given a route for, so "the reconciler asked for nothing else"
/// is a property of this table rather than a hope — and it makes the point
/// that the retention report is refreshed on EVERY reconcile, including one
/// that fires nothing.
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
        let (client, _recorder, bodies) = mock_client_recording_bodies(vec![Route {
            method: "PATCH",
            path_suffix: "/backupschedules/nightly/status",
            status: 200,
            body: schedule_json(r#"{ "keepDays": 7 }"#),
        }]);

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
        report["awsCli"][0], "aws s3 rm s3://kafka-backups/mvp-demo/backup-001/ --recursive",
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
    let (client, _recorder, bodies) = mock_client_recording_bodies(vec![Route {
        method: "PATCH",
        path_suffix: "/backupschedules/nightly/status",
        status: 200,
        body: schedule_json(r#"{ "keepDays": 7 }"#),
    }]);

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
        aws_cli: vec!["aws s3 rm s3://b/c/ --recursive".into()],
        mc_cli: vec!["mc rm --recursive --force local/b/c/".into()],
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
        note: Some(NO_RULE_NOTE.to_string()),
    }
    .to_status();
    assert_eq!(empty.sets_that_would_be_removed, Some(vec![]));
    assert_eq!(empty.note.as_deref(), Some(NO_RULE_NOTE));
}
