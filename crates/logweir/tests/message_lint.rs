//! No tracker task id in a string the product says to a person (MCP round 2,
//! R2-13).
//!
//! The console's own lint (`ui/tests/console-ux.spec.js`, MCP-24) keeps ids
//! like `PLAT-02.2` out of the strings `ui/` ships, and the second human-like
//! pass still read one on step 5: "The runner still validates its signer
//! before it writes anything (PLAT-02.2)", a remedy the CONTROLLER writes.
//! This is the same rule over every string literal under `crates/*/src` --
//! condition messages, check remedies, problem details, CLI refusals, log
//! lines -- found by a small tokenizer that skips comments, so a doc comment
//! citing a task stays a doc comment.
//!
//! ONE FILE IS OUT OF SCOPE, BY NAME: `crates/logweir-api/src/openapi.rs`. Its
//! summaries are the published schema's developer documentation, which cites
//! the task each route belongs to; no operator reads them at runtime.
//!
//! THE CRD DESCRIPTIONS ARE IN SCOPE (review L7): they are doc comments, but
//! `kubectl explain` prints them, so the emitted CRDs are read as text below.
//! Other doc comments, and the chart templates' YAML comments, are not.

use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("the repository root resolves from CARGO_MANIFEST_DIR")
}

/// The one file whose strings are documentation, not messages.
const OUT_OF_SCOPE: &[&str] = &["crates/logweir-api/src/openapi.rs"];

/// Every string literal in `text`, with the line it starts on. Comments
/// (line, block and nested block), char literals and lifetimes are skipped;
/// plain, byte and raw strings are returned with their escapes as written.
fn literals(text: &str) -> Vec<(usize, String)> {
    let b = text.as_bytes();
    let (mut i, mut line) = (0usize, 1usize);
    let mut out = Vec::new();
    let word = |k: usize| k > 0 && (b[k - 1].is_ascii_alphanumeric() || b[k - 1] == b'_');
    while i < b.len() {
        let c = b[i];
        if c == b'\n' {
            line += 1;
            i += 1;
        } else if text[i..].starts_with("//") {
            i = text[i..].find('\n').map_or(b.len(), |j| i + j);
        } else if text[i..].starts_with("/*") {
            let mut depth = 1;
            i += 2;
            while i < b.len() && depth > 0 {
                if text[i..].starts_with("/*") {
                    depth += 1;
                    i += 2;
                } else if text[i..].starts_with("*/") {
                    depth -= 1;
                    i += 2;
                } else {
                    if b[i] == b'\n' {
                        line += 1;
                    }
                    i += 1;
                }
            }
        } else if !word(i) && (c == b'r' || (c == b'b' && b.get(i + 1) == Some(&b'r'))) && {
            let mut k = i + if c == b'b' { 2 } else { 1 };
            while b.get(k) == Some(&b'#') {
                k += 1;
            }
            b.get(k) == Some(&b'"')
        } {
            let start = i + if c == b'b' { 2 } else { 1 };
            let hashes = text[start..].bytes().take_while(|x| *x == b'#').count();
            let close = format!("\"{}", "#".repeat(hashes));
            let body_start = start + hashes + 1;
            let end = text[body_start..]
                .find(&close)
                .map_or(b.len(), |j| body_start + j);
            let body = &text[body_start..end];
            out.push((line, body.to_string()));
            line += body.matches('\n').count();
            i = (end + close.len()).min(b.len());
        } else if c == b'"' || (c == b'b' && !word(i) && b.get(i + 1) == Some(&b'"')) {
            let start = line;
            i += if c == b'b' { 2 } else { 1 };
            let mut body = String::new();
            while i < b.len() && b[i] != b'"' {
                if b[i] == b'\\' {
                    body.push_str(&text[i..(i + 2).min(b.len())]);
                    if b.get(i + 1) == Some(&b'\n') {
                        line += 1;
                    }
                    i += 2;
                    continue;
                }
                if b[i] == b'\n' {
                    line += 1;
                }
                let ch = text[i..].chars().next().expect("a char");
                body.push(ch);
                i += ch.len_utf8();
            }
            out.push((start, body));
            i += 1;
        } else if c == b'\'' {
            // A char literal is skipped whole; a lifetime is one quote.
            let rest = &text[i + 1..];
            let len = if rest.starts_with('\\') {
                rest.find('\'').map(|j| j + 2)
            } else {
                rest.chars()
                    .next()
                    .filter(|ch| rest[ch.len_utf8()..].starts_with('\''))
                    .map(|ch| ch.len_utf8() + 2)
            };
            i += len.unwrap_or(1);
        } else {
            i += text[i..].chars().next().map_or(1, char::len_utf8);
        }
    }
    out
}

/// Whether `s` names a tracker task: `PLAT-`, `PROD-` or `MCP-` and a digit.
fn names_a_task(s: &str) -> bool {
    ["PLAT-", "PROD-", "MCP-"].iter().any(|prefix| {
        s.match_indices(prefix).any(|(at, _)| {
            let before_ok = at == 0 || !s.as_bytes()[at - 1].is_ascii_alphanumeric();
            before_ok
                && s.as_bytes()
                    .get(at + prefix.len())
                    .is_some_and(u8::is_ascii_digit)
        })
    })
}

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("a source directory reads") {
        let path = entry.expect("an entry").path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn no_string_the_product_says_names_a_tracker_task() {
    let root = repo_root();
    let mut files = Vec::new();
    for krate in std::fs::read_dir(root.join("crates")).expect("crates/ reads") {
        let src = krate.expect("a crate").path().join("src");
        if src.is_dir() {
            rust_sources(&src, &mut files);
        }
    }
    // NOT VACUOUS: the controller, the API and the CLI are all read.
    assert!(
        files.len() > 100,
        "only {} Rust sources were found",
        files.len()
    );
    let mut found = Vec::new();
    let mut read = 0usize;
    for path in files {
        let rel = path
            .strip_prefix(&root)
            .expect("under the root")
            .to_string_lossy()
            .replace('\\', "/");
        if OUT_OF_SCOPE.contains(&rel.as_str()) {
            continue;
        }
        let text = std::fs::read_to_string(&path).expect("a source reads");
        for (line, body) in literals(&text) {
            read += 1;
            if names_a_task(&body) {
                found.push(format!(
                    "{rel}:{line}: {}",
                    body.trim().chars().take(120).collect::<String>()
                ));
            }
        }
    }
    assert!(read > 5000, "only {read} string literals were read");
    assert!(
        found.is_empty(),
        "a string the product shows or logs names a tracker task -- say what it means \
         instead (MCP round 2, R2-13):\n{}",
        found.join("\n")
    );
}

/// The task ids in a CRD's text, `file:line: text`, skipping YAML comments.
fn crd_task_ids(rel: &str, text: &str) -> Vec<String> {
    text.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim_start().starts_with('#') && names_a_task(line))
        .map(|(i, line)| {
            format!(
                "{rel}:{}: {}",
                i + 1,
                line.trim().chars().take(120).collect::<String>()
            )
        })
        .collect()
}

/// THE CRD DESCRIPTIONS TOO (review L7). `kubectl explain` prints every field's
/// description, and they are generated from the CRD types' doc comments -- the
/// one place a doc comment IS a message a person reads. The emitted copies
/// (`config/crd/`) and the chart's byte-identical copies are both read, so a
/// description is caught wherever it is installed from.
#[test]
fn no_crd_description_names_a_tracker_task() {
    let root = repo_root();
    let mut found = Vec::new();
    let mut read = 0usize;
    for dir in ["config/crd", "charts/logweir/crds"] {
        for entry in std::fs::read_dir(root.join(dir)).expect("a CRD directory reads") {
            let path = entry.expect("an entry").path();
            if path.extension().is_none_or(|e| e != "yaml") {
                continue;
            }
            let rel = format!(
                "{dir}/{}",
                path.file_name().expect("a name").to_string_lossy()
            );
            let text = std::fs::read_to_string(&path).expect("a CRD reads");
            if text.contains("kind: CustomResourceDefinition") {
                read += 1;
            }
            found.extend(crd_task_ids(&rel, &text));
        }
    }
    assert!(read >= 20, "only {read} CRDs were read");
    assert!(
        found.is_empty(),
        "a CRD description names a tracker task; say what it means in the type's doc \
         comment and run `just crds` (review L7):\n{}",
        found.join("\n")
    );
    // NEGATIVE CONTROL: the shape the chart carried before this round is caught,
    // and a YAML comment is not.
    let planted = "# PLAT-02.1 is a comment\n  description: The approval policy (PLAT-19.2).\n";
    assert_eq!(crd_task_ids("x.yaml", planted).len(), 1);
}

#[test]
fn the_tokenizer_finds_its_planted_twins_and_skips_what_is_not_a_string() {
    let planted = concat!(
        "/// doc citing PLAT-02.2 is a comment\n",
        "// so is PLAT-03.1\n",
        "/* and PLAT-04.1 /* nested PLAT-05 */ still */\n",
        "fn f<'a>(x: &'a str) -> char { let q = '\"'; 'x' }\n",
        "const A: &str = \"validates its signer (PLAT-02.2), and\";\n",
        "const B: &str = r#\"raw \"quoted\" MCP-24 text\"#;\n",
        "const C: &[u8] = b\"PROD-01 bytes\";\n",
        "const D: &str = \"line one \\\n    continued PLAT-19.2's binding\";\n",
        "const E: &str = \"COMPLAT-1 and PLATE-1 are words\";\n",
    );
    let hits: Vec<(usize, String)> = literals(planted)
        .into_iter()
        .filter(|(_, s)| names_a_task(s))
        .collect();
    let lines: Vec<usize> = hits.iter().map(|(l, _)| *l).collect();
    assert_eq!(lines, vec![5, 6, 7, 8], "{hits:?}");
    assert!(!names_a_task("the connect-an-existing-archive path"));
}
