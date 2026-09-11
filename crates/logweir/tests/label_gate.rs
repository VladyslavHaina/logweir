//! The label gate and the tag-1 checklist — Task 29b, spec §16 clauses 9 and 8.
//!
//! `scripts/check-unverified-labels.sh` is the gate: it walks the tree, anchors
//! on the bracket token, and refuses a mark that carries no description or that
//! sits on a line which also asserts the thing is true. These tests are the
//! half a shell script cannot hold on its own.
//!
//! WHY A RUST TEST AT ALL, when the script is the gate. Three reasons, and each
//! is a defect that has happened in this repository before:
//!
//! 1. A gate that is not in `just lint` is documentation. `.github/workflows/ci.yml`
//!    has never executed on any commit, so membership in the `lint` recipe is
//!    the only thing that makes a script run — `just_lint_runs_the_label_gate`
//!    is the same test, for the same reason, as
//!    `one_signer_gate.rs::just_lint_runs_the_one_signer_gate`.
//! 2. The two marks this task closed were closed by EDITING PROSE, and prose
//!    reverts. `the_two_pre_existing_marks_are_closed` pins both.
//! 3. `docs/tag1-checklist.md` is the ledger a tag is cut against, and its
//!    failure mode is a row that reads `closed` with nothing behind it. Two
//!    tests read it back.
//!
//! NO SUBPROCESS AND NO NETWORK (Global Constraint 22). Everything here reads
//! checked-in files. The RED side of the shell gate — a bare mark, a
//! contradicted line, a two-word description, a bare-word mention, a backticked
//! quotation — is exercised by running the script against temp trees through
//! `LOGWEIR_ROOT`, and those runs are recorded in the task report; a test that
//! shelled out would put a filesystem walk of the whole repository inside the
//! 15 s per-test budget for no assertion the script does not already make.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// The bracket token, assembled at compile time from two pieces so this file
/// does not itself carry a mark that the gate would then have to judge.
const TOKEN: &str = concat!("[", "UNVERIFIED");

/// The repo-root idiom, from `crates/logweir/tests/extract_engine.rs`.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("the repository root resolves from CARGO_MANIFEST_DIR")
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{} is readable: {e}", path.display()))
}

/// Byte offsets of every occurrence of the bracket token in `text`.
fn token_offsets(text: &str) -> Vec<usize> {
    let mut found = Vec::new();
    let mut from = 0;
    while let Some(at) = text[from..].find(TOKEN) {
        let at = from + at;
        found.push(at);
        from = at + TOKEN.len();
    }
    found
}

/// The occurrences that are BARE — the token closed immediately by `]`, with
/// no description at all — and that are **not** quotations.
///
/// Rule 0 of the gate: the bare token wrapped in backticks is a quotation of a
/// mark used as a noun in prose ("Spec §15's sixth `[UNVERIFIED]` mark"), not a
/// mark, and nine of them live in this tree. A test that did not make the same
/// distinction the gate makes would assert something the gate does not.
fn bare_marks(text: &str) -> Vec<usize> {
    let mut found = Vec::new();
    for at in token_offsets(text) {
        let after = &text[at + TOKEN.len()..];
        if !after.starts_with(']') {
            continue; // it carries something; rule 1 judges what
        }
        let quoted = text[..at].ends_with('`') && after.starts_with("]`");
        if !quoted {
            found.push(at);
        }
    }
    found
}

/// The one-based line number of a byte offset, for an assertion message that
/// names a place rather than a number.
fn line_of(text: &str, offset: usize) -> usize {
    text[..offset].lines().count().max(1)
}

// ------------------------------------------------ the two pre-existing marks

/// **The two marks that were defective before this task are closed, in place.**
///
/// `docs/adr/0004-kafka-client.md` carried a bare `[UNVERIFIED]` with no
/// description at all, quoted out of a task brief; `docs/stability.md` carried
/// a mark with no dash and a fifteen-character description. Neither was a
/// label: a mark with nothing behind it is a claim that something is unproven,
/// with no statement of what would prove it, which is exactly what spec §16
/// clause 9 exists to forbid.
///
/// The assertion is the gate's own two rules, applied to the two files by name,
/// so a later edit that reverts either one fails here as well as in `just lint`.
#[test]
fn the_two_pre_existing_marks_are_closed() {
    for relative in ["docs/adr/0004-kafka-client.md", "docs/stability.md"] {
        let text = read(relative);

        let dashed = format!("{TOKEN} \u{2014}");
        assert!(
            text.contains(&dashed),
            "{relative} must carry a mark written with an em dash and a description \
             (spec §16 clause 9). Task 29b closed the one this file had; a revert \
             to the bare form is what this assertion catches."
        );

        let bare = bare_marks(&text);
        let places: Vec<usize> = bare.iter().map(|at| line_of(&text, *at)).collect();
        assert!(
            bare.is_empty(),
            "{relative} carries a bare mark with no description, at line(s) {places:?}. \
             A bare token inside backticks is a QUOTATION of a mark and is allowed \
             (docs/stability.md quotes one when it describes the NetworkPolicy probe); \
             these are not quoted, so they are marks with nothing behind them."
        );
    }
}

// ---------------------------------------------------------- the gate is a gate

/// **`just lint` runs the label gate.**
///
/// `.github/workflows/ci.yml` has never executed on any commit, so a workflow
/// step is documentation and membership in the `lint` recipe is what makes a
/// script a gate. Deleting the line from `justfile` leaves
/// `cargo test --workspace` entirely green, which is the whole reason this test
/// exists — it is the `one_signer_gate.rs::just_lint_runs_the_one_signer_gate`
/// pattern, and it asserts membership and nothing else: not the recipe's
/// length, not the line's position, not the absence of other arms, so a later
/// task adding a guard to `lint` cannot turn it red.
#[test]
fn just_lint_runs_the_label_gate() {
    let justfile = read("justfile");
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
        body.contains("check-unverified-labels.sh"),
        "`just lint` is where the label gate runs; removing the line silently retires \
         spec §16 clause 9's only mechanical check, and nothing in \
         `cargo test --workspace` would notice. The `lint` body was:\n{body}"
    );
}

// --------------------------------------------------------- the tag-1 checklist

/// One numbered row of `docs/tag1-checklist.md`'s table.
struct Row {
    number: u32,
    status: String,
    closer: String,
    line: String,
}

/// Every numbered row of the checklist table, in file order.
///
/// A row is a table line whose first cell is a number; the legend table above
/// it (whose first cells are the three status literals) is therefore not a row,
/// and neither are the per-clause note headings below.
fn checklist_rows(text: &str) -> Vec<Row> {
    let mut rows = Vec::new();
    for line in text.lines() {
        if !line.starts_with('|') {
            continue;
        }
        let cells: Vec<&str> = line.split('|').collect();
        // "" | number | clause | status | closer | ""
        if cells.len() < 6 {
            continue;
        }
        let Ok(number) = cells[1].trim().parse::<u32>() else {
            continue;
        };
        rows.push(Row {
            number,
            status: cells[3].trim().trim_matches('`').trim().to_string(),
            closer: cells[4].trim().to_string(),
            line: line.to_string(),
        });
    }
    rows
}

/// Every backticked token in a closer cell, with a `bash `/`just ` prefix and a
/// `::test_name` suffix stripped — what is left is the thing that has to exist.
fn closers(cell: &str) -> Vec<String> {
    cell.split('`')
        .skip(1)
        .step_by(2)
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(|t| {
            let t = t.strip_prefix("bash ").unwrap_or(t);
            let t = t.strip_prefix("just ").unwrap_or(t);
            match t.find("::") {
                Some(at) => t[..at].to_string(),
                None => t.to_string(),
            }
        })
        .collect()
}

/// **Ten rows, one per spec §16 clause, each with a status literal and a
/// closer — and every `closed` row's named path exists on disk.**
///
/// Spec §16 lists ten clauses and the checklist is the ledger a tag is cut
/// against, so a missing row is a clause nobody is tracking. The status
/// grammar admits exactly three literals, because a fourth is where "mostly
/// done" gets written down.
///
/// The last assertion is the one that makes a tick mean something: a row may
/// read `closed` only if the command or the transcript it names is in the tree
/// TODAY. Naming a test a later task will write is the same defect as naming
/// nothing at all.
#[test]
fn the_tag1_checklist_has_one_row_per_spec_clause() {
    let text = read("docs/tag1-checklist.md");
    let rows = checklist_rows(&text);

    let numbers: Vec<u32> = rows.iter().map(|r| r.number).collect();
    assert_eq!(
        numbers,
        (1..=10).collect::<Vec<u32>>(),
        "docs/tag1-checklist.md carries one row per spec §16 clause, numbered 1-10 in \
         order; found {numbers:?}"
    );

    let root = repo_root();
    for row in &rows {
        let n = row.number;
        let ok_status = row.status == "closed"
            || row.status == "open"
            || (row.status.starts_with("blocked: ") && row.status.len() > "blocked: ".len());
        assert!(
            ok_status,
            "row {n}'s status is \"{}\"; the grammar is exactly `closed`, \
             `blocked: <reason>` or `open`",
            row.status
        );

        let named = closers(&row.closer);
        assert!(
            !named.is_empty(),
            "row {n} names no closer and no recorded evidence. A status with nothing \
             beside it is the defect this file exists to remove. The cell was: {}",
            row.closer
        );

        if row.status == "closed" {
            for target in &named {
                assert!(
                    root.join(target).exists(),
                    "row {n} reads `closed` and names `{target}`, which is not in the tree. \
                     A closed row must name something a reader can run or open today; if the \
                     closer belongs to a later task, the row is not closed yet."
                );
            }
        }
    }
}

/// **No blocked row is written as closed, and the five that are blocked today
/// are the five that cannot be closed from this tree.**
///
/// Tag-1 STANDING RULE 22: a mark that is blocked is recorded as blocked, never
/// as closed. The same rule governs a clause. Clause 1 needs images published
/// to a registry the author does not control; clauses 2, 4 and 6 need a git
/// remote that does not exist; clause 8 is an act by the owner — a formal
/// registry search — and no task in the plan closes it.
///
/// The second assertion is deliberately exact rather than a lower bound. If a
/// later task closes one of the five it must edit this list in the same commit,
/// which is the point: a row moving from `blocked` to `closed` is a claim about
/// the world and should cost a reviewer a line of diff.
#[test]
fn no_blocked_row_is_written_as_closed() {
    let text = read("docs/tag1-checklist.md");
    let rows = checklist_rows(&text);

    let mut blocked = BTreeSet::new();
    for row in &rows {
        let n = row.number;
        if row.line.contains("blocked:") {
            assert!(
                row.status.starts_with("blocked: "),
                "row {n} says \"blocked:\" somewhere in its text but its status cell reads \
                 \"{}\". The status is the field anything mechanical reads.",
                row.status
            );
            assert_ne!(
                row.status, "closed",
                "row {n} is blocked and closed at once; a clause is one or the other"
            );
            blocked.insert(n);
        }
        if row.status.starts_with("blocked: ") {
            blocked.insert(n);
        }
    }

    let expected: BTreeSet<u32> = [1, 2, 4, 6, 8].into_iter().collect();
    assert_eq!(
        blocked, expected,
        "clauses 1 (images not published), 2, 4 and 6 (no remote) and 8 (owner action) are \
         the five that cannot be closed from this tree. A change to this set is a change \
         to what the project claims, and belongs in the same commit as the work that \
         earned it."
    );
}
