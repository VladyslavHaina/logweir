//! Documentation label checks and reusable release-checklist consistency.
//! These read local files; execution results belong in workflow summaries.

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
/// ADR 0004, now consolidated in `docs/architecture.md`, carried a bare
/// `[UNVERIFIED]` with no description, quoted out of a task brief; `docs/stability.md` carried
/// a mark with no dash and a fifteen-character description. Neither was a
/// label: a mark with nothing behind it is a claim that something is unproven,
/// with no statement of what would prove it, which is exactly what spec §16
/// clause 9 exists to forbid.
///
/// The assertion is the gate's own two rules, applied to the two files by name,
/// so a later edit that reverts either one fails here as well as in `just lint`.
#[test]
fn the_two_pre_existing_marks_are_closed() {
    for relative in ["docs/architecture.md", "docs/stability.md"] {
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
/// `.github/workflows/ci.yml` and `just gate` share the check script; a
/// workflow step runs on a push, and membership in the `lint` recipe is what
/// makes a script a gate on a laptop before one. Deleting the line from `justfile` leaves
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

/// Checklist rows have unique consecutive numbers, known statuses and real
/// evidence-source paths. No assertion freezes the current release's result.
#[test]
fn the_release_checklist_has_consistent_rows_and_evidence_sources() {
    let text = read("docs/tag1-checklist.md");
    let rows = checklist_rows(&text);

    let numbers: Vec<u32> = rows.iter().map(|r| r.number).collect();
    assert_eq!(
        numbers,
        (1..=rows.len() as u32).collect::<Vec<u32>>(),
        "release checklist rows must have unique consecutive numbers; found {numbers:?}"
    );

    assert!(
        !rows.is_empty(),
        "the release checklist must contain checks"
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

        for target in &named {
            assert!(
                root.join(target).exists(),
                "row {n} names `{target}`, which is not in the tree. \
                     Evidence sources must be available to the release operator."
            );
        }
    }
}

/// A blocked check cannot simultaneously be reported as closed. Which checks
/// are blocked changes between candidates and is deliberately not pinned here.
#[test]
fn no_blocked_row_is_written_as_closed() {
    for row in checklist_rows(&read("docs/tag1-checklist.md")) {
        if row.line.contains("blocked:") {
            assert!(
                row.status.starts_with("blocked: "),
                "row {} describes a block but its status is {}",
                row.number,
                row.status
            );
        }
    }
}
