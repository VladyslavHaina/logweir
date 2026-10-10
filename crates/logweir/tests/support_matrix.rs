//! **The compatibility matrix cannot claim what no row shows** (PROD-01.2).
//!
//! `docs/support-matrix.md` carries the compatibility contract between its two
//! `compatibility:` markers: one table row per broker, authentication mode,
//! registry, archive backend and managed provider, each with exactly one of
//! four statuses. This file holds the page to three rules.
//!
//! 1. **Every row of the contract has a status from the closed set** —
//!    `supported`, `limited`, `untested`, `unsupported`, bold, in a cell of
//!    its own. A row between the markers without one, or with a word outside
//!    the set, fails.
//! 2. **`supported` and `limited` cite a TEST, wherever on the page they
//!    stand.** Such a row names at least one test of this repository as
//!    `` `path/to/file.rs::test_name` ``, and every reference of that shape
//!    ANYWHERE on the page is to a test: the file is there, and it declares
//!    `fn test_name(` under a `#[test]` attribute. "Supported" on reasoning
//!    alone, on an artifact nobody can re-run, on a test that was renamed
//!    away, or on a helper function that asserts nothing, fails. So does a
//!    status row written after the end marker (PROD-01.2 review, L5: the
//!    first version read only between the markers and accepted any
//!    `fn name(`).
//! 3. **Every check id the page names is in the vocabulary.** A backticked
//!    `category.name` whose category is one of the check categories must be a
//!    `CheckId`, anywhere on the page, so a renamed or invented id cannot sit
//!    in a remedy an operator reads.
//!
//! The expectation is never typed here: the statuses, the test names and the
//! ids are read from the page, the tree and `logweir_core`. Each rule has a
//! planted mutant below, because a lint that cannot fail is the defect this
//! repository's guards exist to refuse.
//!
//! Costs nothing and reaches nothing: it reads checked-in files.

use logweir_core::check_contract::CheckId;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

const PAGE: &str = "docs/support-matrix.md";
const BEGIN: &str = "<!-- compatibility:begin -->";
const END: &str = "<!-- compatibility:end -->";

/// The closed set, in the order the page defines them.
const STATUSES: [&str; 4] = ["supported", "limited", "untested", "unsupported"];

/// The statuses that are a claim something works, and so need a row.
const NEEDS_EVIDENCE: [&str; 2] = ["supported", "limited"];

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("the repository root resolves from CARGO_MANIFEST_DIR")
}

fn page() -> String {
    let path = repo_root().join(PAGE);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{} is readable: {e}", path.display()))
}

/// The text between the two markers.
fn contract(text: &str) -> &str {
    let from = text
        .find(BEGIN)
        .unwrap_or_else(|| panic!("{PAGE} has no `{BEGIN}` marker"))
        + BEGIN.len();
    let to = text[from..]
        .find(END)
        .unwrap_or_else(|| panic!("{PAGE} has no `{END}` marker after `{BEGIN}`"))
        + from;
    &text[from..to]
}

/// The cells of one Markdown table row, trimmed; `None` for a line that is
/// not a body row (a heading row is told apart by the caller, a separator row
/// here).
fn cells(line: &str) -> Option<Vec<&str>> {
    let line = line.trim();
    if !line.starts_with('|') || !line.ends_with('|') || line.len() < 2 {
        return None;
    }
    let inner: Vec<&str> = line[1..line.len() - 1].split('|').map(str::trim).collect();
    let separator = inner
        .iter()
        .all(|c| !c.is_empty() && c.chars().all(|ch| matches!(ch, '-' | ':' | ' ')));
    (!separator).then_some(inner)
}

/// Every body row of every table in `section`: `(line number within the
/// section, the row's text, its cells)`. A table's FIRST row is its header and
/// is skipped.
fn body_rows(section: &str) -> Vec<(usize, &str, Vec<&str>)> {
    let mut rows = Vec::new();
    let mut in_table = false;
    for (i, line) in section.lines().enumerate() {
        match cells(line) {
            Some(c) if in_table => rows.push((i + 1, line, c)),
            // The header row, or the separator under it (`cells` is `None`
            // for a separator, which keeps `in_table` as it is).
            Some(_) => in_table = true,
            None if line.trim().starts_with('|') => {}
            None => in_table = false,
        }
    }
    rows
}

/// The status of a row: the one cell that is exactly `**word**`.
fn status_of(cells: &[&str]) -> Result<&'static str, String> {
    let bold: Vec<&str> = cells
        .iter()
        .filter_map(|c| c.strip_prefix("**").and_then(|c| c.strip_suffix("**")))
        .filter(|c| !c.contains(' '))
        .collect();
    let found: Vec<&'static str> = STATUSES
        .iter()
        .copied()
        .filter(|s| bold.contains(s))
        .collect();
    match found.as_slice() {
        [one] => Ok(one),
        [] => Err(format!(
            "no status cell (one of {STATUSES:?}, bold, alone in its cell); bold single words \
             found: {bold:?}"
        )),
        many => Err(format!("{} status cells: {many:?}", many.len())),
    }
}

/// Every `` `path.rs::test_name` `` reference in `text`.
fn test_references(text: &str) -> Vec<(String, String)> {
    text.split('`')
        .skip(1)
        .step_by(2)
        .filter_map(|code| {
            let (path, name) = code.split_once("::")?;
            (path.ends_with(".rs")
                && !path.contains(' ')
                && !name.is_empty()
                && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
            .then(|| (path.to_string(), name.to_string()))
        })
        .collect()
}

/// Why `path::name` is not a test of this repository, or `None` when it is
/// one: `path` (relative to the repository root) declares `fn name(` as an
/// item, and a `#[test]` attribute stands above it.
///
/// "Declares" is a line that IS the declaration, so a name in a comment or a
/// string is not one. "A test" is `#[test]` (or an async runtime's
/// `#[…::test]`) among the attribute lines directly above: a helper such as
/// `fn capability_plan(` asserts nothing and is not evidence (review L5,
/// mutants D4 and D7).
fn not_a_test(path: &str, name: &str) -> Option<String> {
    let Ok(src) = std::fs::read_to_string(repo_root().join(path)) else {
        return Some(format!(
            "{path} declares no `fn {name}(`: the file is not there"
        ));
    };
    not_a_test_in(&src, path, name)
}

fn not_a_test_in(src: &str, path: &str, name: &str) -> Option<String> {
    let lines: Vec<&str> = src.lines().collect();
    let declares = |line: &str| {
        let line = line.trim_start();
        ["fn ", "pub fn ", "async fn ", "pub async fn "]
            .iter()
            .filter_map(|lead| line.strip_prefix(lead))
            .any(|rest| rest.starts_with(&format!("{name}(")))
    };
    let declared: Vec<usize> = (0..lines.len()).filter(|i| declares(lines[*i])).collect();
    if declared.is_empty() {
        return Some(format!("{path} declares no `fn {name}(`"));
    }
    let is_a_test = |at: &usize| {
        lines[..*at]
            .iter()
            .rev()
            .map(|l| l.trim())
            .take_while(|l| l.starts_with("#[") || l.starts_with("#!["))
            .any(|l| l == "#[test]" || (l.starts_with("#[") && l.contains("::test")))
    };
    if declared.iter().any(is_a_test) {
        None
    } else {
        Some(format!(
            "{path} declares `fn {name}(` with no `#[test]` above it: a helper is not a row"
        ))
    }
}

/// The categories a check id can have, read off the vocabulary itself.
fn check_categories() -> BTreeSet<&'static str> {
    CheckId::ALL.iter().map(|id| id.category()).collect()
}

/// Every backticked `category.name` on the page whose category is a check
/// category: the tokens that CLAIM to be check ids.
fn check_id_claims(text: &str) -> Vec<String> {
    let categories = check_categories();
    text.split('`')
        .skip(1)
        .step_by(2)
        .filter(|code| {
            code.split_once('.').is_some_and(|(category, name)| {
                categories.contains(category)
                    && !name.is_empty()
                    && name.chars().all(|c| c.is_ascii_alphanumeric())
            })
        })
        .map(str::to_string)
        .collect()
}

/// Everything wrong with a page's text, as sentences. Empty means it holds.
fn offences(text: &str) -> Vec<String> {
    let mut bad = Vec::new();
    let section = contract(text);
    let rows = body_rows(section);
    if rows.len() < 20 {
        bad.push(format!(
            "only {} table rows between the markers: the contract's tables are gone, or this \
             lint stopped reading them",
            rows.len()
        ));
    }
    // Rule 1, between the markers: every row has exactly one status.
    for (n, line, cells) in &rows {
        if let Err(why) = status_of(cells) {
            bad.push(format!("contract line {n}: {why}: {line}"));
        }
    }
    // Rule 2, over the WHOLE page: a row that carries a status that claims
    // something works cites a test, whichever side of a marker it is on.
    for (n, line, cells) in body_rows(text) {
        let Ok(status) = status_of(&cells) else {
            continue;
        };
        if NEEDS_EVIDENCE.contains(&status) && test_references(line).is_empty() {
            bad.push(format!(
                "page line {n}: `{status}` with no evidence reference (a \
                 `path.rs::test_name` of this repository): {line}"
            ));
        }
    }
    // ...and every reference on the page, in a table or in prose, is to a
    // test that exists.
    let mut seen = BTreeSet::new();
    for (path, name) in test_references(text) {
        if !seen.insert((path.clone(), name.clone())) {
            continue;
        }
        if let Some(why) = not_a_test(&path, &name) {
            bad.push(format!("the page cites `{path}::{name}`, and {why}"));
        }
    }
    for claim in check_id_claims(text) {
        if CheckId::parse(&claim).is_none() {
            bad.push(format!(
                "`{claim}` reads as a check id and is not in the vocabulary \
                 (logweir_core::check_contract::CheckId)"
            ));
        }
    }
    bad
}

#[test]
fn the_compatibility_matrix_claims_only_what_a_row_shows() {
    let text = page();
    let bad = offences(&text);
    assert!(bad.is_empty(), "{PAGE}:\n  {}", bad.join("\n  "));
    // The lint read real rows: every status of the closed set is in use, and
    // the supported rows name tests.
    let rows = body_rows(contract(&text));
    let used: BTreeSet<&str> = rows
        .iter()
        .filter_map(|(_, _, c)| status_of(c).ok())
        .collect();
    assert_eq!(
        used,
        STATUSES.into_iter().collect(),
        "the contract uses all four statuses"
    );
    let cited: usize = rows
        .iter()
        .filter(|(_, _, c)| status_of(c) == Ok("supported"))
        .map(|(_, line, _)| test_references(line).len())
        .sum();
    assert!(cited >= 10, "the supported rows cite {cited} tests");
    // The page names the capability check ids, and they parse.
    let claims = check_id_claims(&text);
    for id in logweir_core::check_contract::CAPABILITY_CHECKS {
        assert!(
            claims.iter().any(|c| c == id.as_str()),
            "{PAGE} never names `{id}`, the row that announces a missing capability"
        );
    }
}

/// **Each rule can fail.** One planted change per rule, on a copy of the real
/// page, and each must be named by its own sentence.
#[test]
fn every_rule_of_the_matrix_lint_has_a_mutant_that_fails_it() {
    let text = page();
    assert!(offences(&text).is_empty(), "the page holds unmutated");
    let rows = body_rows(contract(&text));
    let (_, supported, _) = rows
        .iter()
        .find(|(_, line, c)| status_of(c) == Ok("supported") && test_references(line).len() == 1)
        .expect("a supported row with exactly one evidence reference");
    let (path, name) = test_references(supported).remove(0);
    let reference = format!("`{path}::{name}`");

    // Rule 2a: a supported row whose evidence is removed.
    let stripped = supported.replace(&reference, "reasoning from the source");
    let d = offences(&text.replacen(supported, &stripped, 1)).join("\n");
    assert!(
        d.contains("`supported` with no evidence reference"),
        "an unevidenced `supported` was not caught:\n{d}"
    );

    // Rule 2b: a reference to a test that does not exist.
    let renamed = supported.replace(&reference, &format!("`{path}::{name}_was_renamed`"));
    let d = offences(&text.replacen(supported, &renamed, 1)).join("\n");
    assert!(
        d.contains(&format!("declares no `fn {name}_was_renamed(`")),
        "a reference to a missing test was not caught:\n{d}"
    );
    // ...and to a file that does not exist.
    let moved = supported.replace(&reference, &format!("`e2e/tests/no_such_file.rs::{name}`"));
    let d = offences(&text.replacen(supported, &moved, 1)).join("\n");
    assert!(
        d.contains("e2e/tests/no_such_file.rs declares no"),
        "a reference into a missing file was not caught:\n{d}"
    );

    // Rule 2c (review L5, mutants D4 and D7): a HELPER cited as the row. It
    // is declared in the file the real row is in, and asserts nothing.
    for helper in ["capability_plan", "dial"] {
        assert!(
            not_a_test("e2e/tests/compat_contract.rs", helper)
                .is_some_and(|why| why.contains("no `#[test]` above it")),
            "`{helper}` is still a helper of the compat rows, declared without `#[test]`"
        );
        let cited = supported.replace(
            &reference,
            &format!("`e2e/tests/compat_contract.rs::{helper}`"),
        );
        let d = offences(&text.replacen(supported, &cited, 1)).join("\n");
        assert!(
            d.contains(&format!(
                "declares `fn {helper}(` with no `#[test]` above it"
            )),
            "the helper `{helper}` passed as evidence:\n{d}"
        );
    }
    // A name that appears only in a comment or a string is not declared.
    let src = "// fn a_row() {}\nlet s = \"fn a_row() {}\";\n#[test]\nfn another_row() {}\n";
    assert!(not_a_test_in(src, "x.rs", "a_row")
        .is_some_and(|why| why.contains("declares no `fn a_row(`")));
    assert_eq!(not_a_test_in(src, "x.rs", "another_row"), None);
    // An ignored test, and an async one, are tests.
    let src = "#[test]\n#[ignore = \"needs a profile\"]\nfn ignored_row() {}\n\
               #[tokio::test]\nasync fn async_row() {}\n\
               #[allow(dead_code)]\nfn helper() {}\n";
    assert_eq!(not_a_test_in(src, "x.rs", "ignored_row"), None);
    assert_eq!(not_a_test_in(src, "x.rs", "async_row"), None);
    assert!(not_a_test_in(src, "x.rs", "helper").is_some());

    // Rule 2d (review L5, mutant D6): a status row written AFTER the end
    // marker is held to the same rule, and so is a reference in prose.
    let after = format!(
        "{text}\n| Endpoint | Status | What | Evidence |\n|---|---|---|---|\n\
         | Somewhere new | **supported** | It was read about. | |\n"
    );
    let d = offences(&after).join("\n");
    assert!(
        d.contains("`supported` with no evidence reference"),
        "a `supported` row outside the markers passed:\n{d}"
    );
    let prose = format!("{text}\nSee `e2e/tests/compat_contract.rs::a_row_nobody_wrote`.\n");
    let d = offences(&prose).join("\n");
    assert!(
        d.contains("declares no `fn a_row_nobody_wrote(`"),
        "a reference in prose to a missing test passed:\n{d}"
    );

    // Rule 1: a status outside the closed set, and a row with two.
    let d = offences(&text.replacen(
        supported,
        &supported.replace("**supported**", "**certified**"),
        1,
    ))
    .join("\n");
    assert!(
        d.contains("no status cell"),
        "an invented status passed:\n{d}"
    );
    let d = offences(&text.replacen(
        supported,
        &supported.replacen("**supported**", "**supported** | **untested**", 1),
        1,
    ))
    .join("\n");
    assert!(d.contains("2 status cells"), "two statuses passed:\n{d}");

    // Rule 3: a check id the vocabulary does not have, anywhere on the page.
    let invented = format!("{text}\nSee `connection.engineProtocols` for the rest.\n");
    let d = offences(&invented).join("\n");
    assert!(
        d.contains("`connection.engineProtocols` reads as a check id and is not in the vocabulary"),
        "an invented check id passed:\n{d}"
    );
    // A dotted configuration key or a file name is NOT a claim to be one.
    let harmless = format!(
        "{text}\n`message.timestamp.type`, `log.retention.ms` and `stack-env.sh` are not ids.\n"
    );
    assert!(offences(&harmless).is_empty(), "{:?}", offences(&harmless));

    // The section going missing is itself an offence, never a silent pass.
    let emptied = format!(
        "{}{BEGIN}\n\nNothing here.\n\n{END}{}",
        &text[..text.find(BEGIN).unwrap()],
        &text[text.find(END).unwrap() + END.len()..]
    );
    let d = offences(&emptied).join("\n");
    assert!(
        d.contains("only 0 table rows"),
        "an empty contract passed:\n{d}"
    );
}

/// The parsers read what they claim to, on small inputs.
#[test]
fn the_matrix_lint_reads_tables_statuses_and_references() {
    let table = "\
intro\n\
| Broker | Status | Evidence |\n\
|---|---|---|\n\
| A | **supported** | `e2e/tests/x.rs::a_row` and `docs/y.md` |\n\
| B | **untested** | no account |\n\
\n\
not a table\n\
| C | D |\n\
|:--|--:|\n\
| e | **limited** |\n";
    let rows = body_rows(table);
    assert_eq!(rows.len(), 3, "{rows:?}");
    assert_eq!(status_of(&rows[0].2), Ok("supported"));
    assert_eq!(status_of(&rows[1].2), Ok("untested"));
    assert_eq!(status_of(&rows[2].2), Ok("limited"));
    assert_eq!(
        test_references(rows[0].1),
        vec![("e2e/tests/x.rs".to_string(), "a_row".to_string())]
    );
    // Bold prose in a cell is not a status; a bold status word inside a
    // sentence is not one either.
    assert!(status_of(&["**not run** yet", "x"]).is_err());
    assert!(status_of(&["it is **supported** here"]).is_err());
    assert!(check_categories().contains("connection") && check_categories().contains("target"));
    assert_eq!(
        check_id_claims("`connection.groupTypes`, `retention.ms`, `target.x y`"),
        vec!["connection.groupTypes".to_string()]
    );
}
