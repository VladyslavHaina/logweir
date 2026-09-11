//! **I29 — the exit-code tokeniser.** Task 12.
//!
//! One shell tokeniser for STANDING RULE 20 — *exit codes are read directly,
//! never through a pipe* — shared by every task that ships a script. **Task 28
//! and Task 32 reuse this helper and do not write a second one** (controller
//! ruling, critique C M12): they open with `mod support;` and call
//! `support::exit_code_lint`, and nothing here may be specific to any one
//! script.
//!
//! # The rule, verbatim
//!
//! > no line whose first word is `kubectl`, `curl`, `logweir`, `docker` or
//! > `just` contains an unquoted `|`, and every such line is followed by a
//! > line reading the status directly (ignoring comments and here-docs).
//!
//! Both halves matter and they fail differently. `cmd | grep` reports **grep's**
//! status, so the pipe silently replaces the exit code the rule is about; a
//! guarded command whose status nothing reads has an exit code that exists and
//! is discarded. [`Finding`] names them separately so a failure message says
//! which defect it found.
//!
//! # Decisions this tokeniser makes, and why
//!
//! Each of these is pinned by a fixture in
//! `crates/logweir/tests/mvp_demo_lint.rs::the_exit_code_tokeniser_is_a_reusable_helper`,
//! so a later task cannot flip one silently.
//!
//! * **`||` is not a pipe.** `a | b` *replaces* `a`'s status with `b`'s, which
//!   is the defect rule 20 names. `a || b` reads `a`'s status directly and
//!   branches on it — it is the rule's compliant form, not its violation.
//!   `|&` (bash's pipe-with-stderr) **is** a pipe.
//! * **"Reading the status directly" is `$?` on the NEXT code line**, outside
//!   single quotes. `echo "rc=$?"` reads it (double quotes expand); `echo
//!   'rc=$?'` does not (single quotes are literal). A same-line `cmd; rc=$?`
//!   does **not** satisfy it: "on its own line" is the rule's own name for what
//!   it wants, and a tokeniser that accepted both would stop being able to say
//!   which shape it saw.
//! * **First word means first word.** `LOGWEIR_PYTHON=… logweir …` and
//!   `"$LOGWEIR" …` are not guarded lines — the guard is a literal-prefix rule,
//!   which is what makes it checkable by a tokeniser rather than by a shell.
//!   A script that wants the guard writes the bare word (put the binary's
//!   directory on `$PATH`), and `scripts/mvp-demo.sh` does exactly that and
//!   says so.
//! * **Comments and here-doc bodies are not code.** A `#` that begins a word
//!   outside quotes ends the line; a `<<DELIM` / `<<-DELIM` body is skipped
//!   until its terminator. `<<<` is a here-*string*, not a here-doc, and is
//!   left alone.
//! * **A logical line is the physical lines joined** across a trailing
//!   unquoted `\` and across a quote that is still open at end of line — so a
//!   wrapped `logweir …  \` invocation is one line to this lint, and the status
//!   read that follows it is the next one.
//!
//! # What it is not
//!
//! It is a tokeniser, not a shell. It does not expand anything, does not know
//! about functions, subshells or `set -e`, and cannot tell you whether the
//! status a script reads is then acted on. It answers exactly the two
//! questions the rule asks, over text, in milliseconds, in the default test
//! suite (GC22) — which is what lets it run on every commit.

use std::fmt;

/// The words whose lines this lint guards, verbatim from I29.
pub const GUARDED_FIRST_WORDS: [&str; 5] = ["kubectl", "curl", "logweir", "docker", "just"];

/// One violation of rule 20, with the line that carries it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Finding {
    /// A guarded line carries an unquoted `|`: its exit status is the
    /// right-hand side's, not its own.
    PipedStatus {
        /// 1-based physical line number the logical line starts on.
        line: usize,
        /// The logical line's text, physical continuations joined.
        text: String,
    },
    /// A guarded line is not followed by a line reading `$?`.
    UnreadStatus {
        /// 1-based physical line number the logical line starts on.
        line: usize,
        /// The logical line's text, physical continuations joined.
        text: String,
    },
}

impl Finding {
    /// 1-based physical line number the offending logical line starts on.
    pub fn line(&self) -> usize {
        match self {
            Finding::PipedStatus { line, .. } | Finding::UnreadStatus { line, .. } => *line,
        }
    }

    /// The offending logical line, physical continuations joined.
    pub fn text(&self) -> &str {
        match self {
            Finding::PipedStatus { text, .. } | Finding::UnreadStatus { text, .. } => text,
        }
    }
}

impl fmt::Display for Finding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Finding::PipedStatus { line, text } => write!(
                f,
                "line {line}: an unquoted `|` on a guarded line — the pipe's status is \
                 reported, not this command's (STANDING RULE 20). Capture to a file on \
                 this line and read `$?` on the next.\n    {text}"
            ),
            Finding::UnreadStatus { line, text } => write!(
                f,
                "line {line}: this guarded line's exit status is not read on the line \
                 that follows it (STANDING RULE 20). Put `rc=$?` — or any read of `$?` \
                 outside single quotes — on the next line.\n    {text}"
            ),
        }
    }
}

/// One shell line as this lint sees it: physical continuations joined, comment
/// text and here-doc bodies removed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalLine {
    /// 1-based physical line number this logical line starts on.
    pub line: usize,
    /// The joined physical text, for a human-readable failure message.
    /// Comment text is INCLUDED here, because a message that quotes half a
    /// line is worse than one that quotes all of it.
    pub text: String,
    /// The CODE only: comment text and here-doc bodies removed, physical
    /// continuations joined by a space. This is the text to match against when
    /// asking what a script RUNS, rather than what it mentions.
    pub code: String,
    /// The first whitespace-delimited word of the code, `""` for none.
    pub first_word: String,
    /// An unquoted `|` (or `|&`) appears in the code.
    pub has_pipe: bool,
    /// A `$?` appears in the code outside single quotes.
    pub reads_status: bool,
}

impl LogicalLine {
    /// Whether this line's first word is one of [`GUARDED_FIRST_WORDS`].
    pub fn is_guarded(&self) -> bool {
        GUARDED_FIRST_WORDS.contains(&self.first_word.as_str())
    }
}

/// Every code line of `script`, in order. Blank lines, comment-only lines and
/// here-doc bodies produce nothing: they are not code, and "the next line" in
/// the rule means the next line that is.
pub fn logical_lines(script: &str) -> Vec<LogicalLine> {
    let phys: Vec<&str> = script.lines().collect();
    let mut out: Vec<LogicalLine> = Vec::new();
    let mut i = 0usize;

    while i < phys.len() {
        let start = i;
        let mut code = String::new();
        let mut text = String::new();
        let mut has_pipe = false;
        let mut reads_status = false;
        // Quote state is carried ACROSS physical lines: a string opened on one
        // line and closed on the next is one logical line, and the `|` inside
        // it is quoted on both.
        let mut in_single = false;
        let mut in_double = false;

        loop {
            let raw = phys[i];
            if !text.is_empty() {
                text.push(' ');
            }
            text.push_str(raw.trim());

            let chars: Vec<char> = raw.chars().collect();
            let mut pending_heredocs: Vec<(String, bool)> = Vec::new();
            let mut continues = false;
            let mut j = 0usize;

            while j < chars.len() {
                let c = chars[j];

                if in_single {
                    code.push(c);
                    if c == '\'' {
                        in_single = false;
                    }
                    j += 1;
                    continue;
                }

                if in_double {
                    if c == '\\' && j + 1 < chars.len() {
                        code.push(c);
                        code.push(chars[j + 1]);
                        j += 2;
                        continue;
                    }
                    if c == '$' && j + 1 < chars.len() && chars[j + 1] == '?' {
                        reads_status = true;
                    }
                    code.push(c);
                    if c == '"' {
                        in_double = false;
                    }
                    j += 1;
                    continue;
                }

                match c {
                    // A trailing backslash continues the line; anything else it
                    // precedes is escaped and carries no meaning of its own.
                    '\\' => {
                        if j + 1 == chars.len() {
                            continues = true;
                            j += 1;
                        } else {
                            code.push(c);
                            code.push(chars[j + 1]);
                            j += 2;
                        }
                    }
                    '\'' => {
                        in_single = true;
                        code.push(c);
                        j += 1;
                    }
                    '"' => {
                        in_double = true;
                        code.push(c);
                        j += 1;
                    }
                    // A `#` that BEGINS a word is a comment to end of line. A
                    // `#` inside a word (`local/bucket#1`) is not.
                    '#' if j == 0 || chars[j - 1].is_whitespace() => break,
                    '$' if j + 1 < chars.len() && chars[j + 1] == '?' => {
                        reads_status = true;
                        code.push(c);
                        j += 1;
                    }
                    // `<<<` is a here-STRING: its word is on this line and it
                    // swallows nothing. Consumed whole so the `<<` branch
                    // below cannot read its trailing `<` as a here-doc opener.
                    '<' if chars.get(j + 1) == Some(&'<') && chars.get(j + 2) == Some(&'<') => {
                        code.push_str("<<<");
                        j += 3;
                    }
                    // `<<WORD` / `<<-WORD` / `<<'WORD'` open a here-doc whose
                    // body starts on the NEXT physical line.
                    '<' if chars.get(j + 1) == Some(&'<') => {
                        let mut k = j + 2;
                        let strip_tabs = k < chars.len() && chars[k] == '-';
                        if strip_tabs {
                            k += 1;
                        }
                        while k < chars.len() && (chars[k] == ' ' || chars[k] == '\t') {
                            k += 1;
                        }
                        let quote = match chars.get(k) {
                            Some(&q @ ('\'' | '"')) => {
                                k += 1;
                                Some(q)
                            }
                            _ => None,
                        };
                        let mut delim = String::new();
                        while k < chars.len() {
                            let d = chars[k];
                            match quote {
                                Some(q) => {
                                    k += 1;
                                    if d == q {
                                        break;
                                    }
                                    delim.push(d);
                                }
                                None => {
                                    if d.is_alphanumeric() || d == '_' || d == '.' || d == '-' {
                                        delim.push(d);
                                        k += 1;
                                    } else {
                                        break;
                                    }
                                }
                            }
                        }
                        if !delim.is_empty() {
                            pending_heredocs.push((delim, strip_tabs));
                        }
                        code.push_str("<<");
                        j = k;
                    }
                    '|' => {
                        if chars.get(j + 1) == Some(&'|') {
                            // `||` — a conditional, not a pipe. See the module
                            // doc comment; pinned by a fixture.
                            code.push_str("||");
                            j += 2;
                        } else {
                            // `|` and `|&` both replace the status.
                            has_pipe = true;
                            code.push(c);
                            j += 1;
                        }
                    }
                    _ => {
                        code.push(c);
                        j += 1;
                    }
                }
            }

            i += 1;

            // The here-doc bodies opened on this physical line run until their
            // terminators, and none of it is code.
            for (delim, strip_tabs) in pending_heredocs {
                while i < phys.len() {
                    let body = phys[i];
                    let cmp = if strip_tabs {
                        body.trim_start_matches('\t')
                    } else {
                        body
                    };
                    i += 1;
                    if cmp.trim_end() == delim {
                        break;
                    }
                }
            }

            let keep_going = (continues || in_single || in_double) && i < phys.len();
            if keep_going {
                code.push(' ');
                continue;
            }
            break;
        }

        let trimmed = code.trim();
        if !trimmed.is_empty() {
            let first_word = trimmed
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .to_string();
            out.push(LogicalLine {
                line: start + 1,
                text,
                code: trimmed.to_string(),
                first_word,
                has_pipe,
                reads_status,
            });
        }
    }

    out
}

/// Every violation of rule 20 in `script`, in line order. An empty vector is
/// the pass.
pub fn findings(script: &str) -> Vec<Finding> {
    let lines = logical_lines(script);
    let mut out = Vec::new();
    for (idx, line) in lines.iter().enumerate() {
        if !line.is_guarded() {
            continue;
        }
        if line.has_pipe {
            out.push(Finding::PipedStatus {
                line: line.line,
                text: line.text.clone(),
            });
        }
        let next_reads = lines.get(idx + 1).is_some_and(|n| n.reads_status);
        if !next_reads {
            out.push(Finding::UnreadStatus {
                line: line.line,
                text: line.text.clone(),
            });
        }
    }
    out
}

/// Panic naming every finding, or return having found none. `label` is what
/// the failure message calls the script — a path, usually.
///
/// The message lists **every** finding rather than the first: a script edited
/// into two violations should cost one run to fix, not two.
pub fn assert_no_masked_exit_code(label: &str, script: &str) {
    let found = findings(script);
    assert!(
        found.is_empty(),
        "{label}: {} line(s) mask an exit code (STANDING RULE 20)\n\n{}",
        found.len(),
        found
            .iter()
            .map(Finding::to_string)
            .collect::<Vec<_>>()
            .join("\n\n")
    );
}
