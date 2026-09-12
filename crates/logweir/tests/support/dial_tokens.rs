//! **The array-literal tokeniser.** Task 32, stage-2 carried item (a).
//!
//! `crates/logweir/tests/notify.rs::the_two_ureq_token_lists_agree` used to
//! read `crates/logweir/tests/no_network_in_unit_tests.rs`'s `DIAL_TOKENS`
//! literal as raw text and ask `list.contains(&format!("\"{t}\""))`. A
//! substring search over source text cannot tell an ARRAY ELEMENT from a
//! mention: a `//` comment inside the literal that quotes a token satisfied it
//! with no real entry behind it. Today the comment at
//! `no_network_in_unit_tests.rs` names the bare-constructor token in
//! **backticks** rather than in double quotes, so the check was right by luck
//! rather than by construction — and the mutant that moves a real token into
//! that comment is exactly the one it had to catch. (This module deliberately
//! spells no dial token out: `no_network_in_unit_tests.rs`'s own audit walks
//! this file, and a helper that has to be allow-listed to describe itself is a
//! helper that has widened the audit to write its doc comment.)
//!
//! So the elements are parsed: comments are removed first, and what is left is
//! read as double-quoted string literals and compared as a **set**.
//!
//! # What this is, and what it is not
//!
//! It is **not a Rust lexer**. It knows three things — a double-quoted string
//! (with `\"` and `\\` escapes), a `//` line comment and a `/* … */` block —
//! which is exactly what an array of string literals contains. It does not
//! know raw strings (`r"…"`, `r#"…"#`), byte strings or character literals, and
//! a `'"'` char literal in the scanned region would be read as opening a
//! string. None of the three arrays this is used on contains any of them, and
//! a future one that does must extend this module rather than work around it.
//!
//! It is a **different tokeniser from `support::exit_code_lint`** and does not
//! duplicate it: that one answers questions about shell *lines* (I29, Task 12),
//! this one answers "what are the elements of this Rust array literal". The
//! controller ruling that Task 32 writes no second exit-code tokeniser is about
//! that helper, and it is reused unchanged.

use std::collections::BTreeSet;

/// The text of the array literal `decl` introduces: from the declaration to
/// the closing `];`, the declaration itself included.
///
/// `None` when the declaration is absent, or present but never closed — both
/// of which the caller must treat as a failure rather than as an empty array,
/// because an empty set would make every "is this token present" question
/// answer the same way.
pub fn array_literal<'a>(src: &'a str, decl: &str) -> Option<&'a str> {
    let start = src.find(decl)?;
    let end = start + src[start..].find("];")?;
    Some(&src[start..end])
}

/// `src` with `//` line comments and `/* … */` blocks removed, string literals
/// left intact.
pub fn strip_comments(src: &str) -> String {
    let b: Vec<char> = src.chars().collect();
    let mut out = String::new();
    let mut i = 0usize;
    let mut in_str = false;
    while i < b.len() {
        let c = b[i];
        if in_str {
            out.push(c);
            if c == '\\' && i + 1 < b.len() {
                out.push(b[i + 1]);
                i += 2;
                continue;
            }
            if c == '"' {
                in_str = false;
            }
            i += 1;
            continue;
        }
        if c == '"' {
            in_str = true;
            out.push(c);
            i += 1;
            continue;
        }
        if c == '/' && i + 1 < b.len() && b[i + 1] == '/' {
            while i < b.len() && b[i] != '\n' {
                i += 1;
            }
            continue;
        }
        if c == '/' && i + 1 < b.len() && b[i + 1] == '*' {
            i += 2;
            while i + 1 < b.len() && !(b[i] == '*' && b[i + 1] == '/') {
                i += 1;
            }
            i = (i + 2).min(b.len());
            continue;
        }
        out.push(c);
        i += 1;
    }
    out
}

/// Every double-quoted string literal in `src`, as a set, with `\"` and `\\`
/// unescaped.
pub fn string_literals(src: &str) -> BTreeSet<String> {
    let b: Vec<char> = src.chars().collect();
    let mut out = BTreeSet::new();
    let mut i = 0usize;
    while i < b.len() {
        if b[i] != '"' {
            i += 1;
            continue;
        }
        i += 1;
        let mut s = String::new();
        while i < b.len() && b[i] != '"' {
            if b[i] == '\\' && i + 1 < b.len() {
                match b[i + 1] {
                    '"' => s.push('"'),
                    '\\' => s.push('\\'),
                    'n' => s.push('\n'),
                    't' => s.push('\t'),
                    other => {
                        s.push('\\');
                        s.push(other);
                    }
                }
                i += 2;
                continue;
            }
            s.push(b[i]);
            i += 1;
        }
        i += 1; // the closing quote
        out.insert(s);
    }
    out
}

/// The **elements** of the array literal `decl` introduces in `src`: comments
/// removed first, so a token that appears only inside a comment is not an
/// element. Panics naming `decl` when the declaration is absent or unclosed —
/// see [`array_literal`] for why that is not an empty set.
pub fn array_elements(src: &str, decl: &str) -> BTreeSet<String> {
    let list = array_literal(src, decl)
        .unwrap_or_else(|| panic!("`{decl}` must still be a closed array literal in this source"));
    string_literals(&strip_comments(list))
}
