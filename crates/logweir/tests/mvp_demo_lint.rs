//! The DEFAULT-SET half of Task 12: what can be checked about
//! `scripts/mvp-demo.sh` by reading it.
//!
//! **Everything here is in-process and reads files** (GC22): one shell script,
//! two markdown files and a handful of inline fixtures. The demo ITSELF is
//! `#[cfg(feature = "e2e")]`, in `e2e/tests/mvp_demo.rs`, because it wants a
//! broker, a bucket and four minutes. Splitting them that way is what lets the
//! script's two load-bearing textual properties — it runs BOTH readers, and it
//! masks no exit code — be checked on every commit rather than only when
//! somebody has the stack up.
//!
//! `the_exit_code_tokeniser_is_a_reusable_helper` is the test **Task 28 and
//! Task 32 inherit rather than re-derive** (controller ruling, critique C M12).
//! It drives `support::exit_code_lint` over inline fixtures, so a later task
//! that changes the tokeniser sees which shape it broke.

mod support;

use std::path::{Path, PathBuf};
use support::exit_code_lint::{self, Finding};

/// The workspace root, from this crate's manifest directory. No `git`, no
/// walking upwards looking for a marker: `crates/logweir` is a fixed two
/// levels down and a wrong answer here would make every read below fail
/// loudly rather than silently pass.
fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the workspace root resolves from crates/logweir")
}

fn read(rel: &str) -> String {
    let p = root().join(rel);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("{}: {e}", p.display()))
}

/// **The demo RUNS both readers over the receipt** — not mentions them.
///
/// One verifier that shares code with the thing it verifies proves the bytes
/// round-tripped, not that an auditor could check them. The product's claim is
/// two independent readers, and a demo that ran only the Rust one would be
/// demonstrating half of it while saying "verified".
///
/// # Why this is not a `contains`
///
/// It was, and **the mutant survived**. Step 7 of the demo prints the exact
/// command an auditor would run, `echo`-ing the string
/// `verify_scorecard.py --payload-type backup-receipt` — so deleting the
/// EXECUTED Python verification left the needle in the file and the test
/// green. Measured, in this task's own mutation round.
///
/// What makes it decisive is the I29 tokeniser: the needle must appear in the
/// CODE of a logical line (comments and here-doc bodies removed, `echo`'s
/// arguments included but its status not) that is **followed by a line reading
/// `$?`**. A printed instruction has no status anybody reads; an invocation
/// this script cares about always does, because STANDING RULE 20 says so.
///
/// Mutant: verify the receipt with only `logweir drill verify` → this fails on
/// the `verify_scorecard.py` half, naming it.
#[test]
fn mvp_demo_runs_both_readers() {
    let script = read("scripts/mvp-demo.sh");
    let lines = exit_code_lint::logical_lines(&script);
    for needle in [
        "logweir drill verify --payload-type backup-receipt",
        "verify_scorecard.py --payload-type backup-receipt",
    ] {
        let mentioned = script.contains(needle);
        let run = lines
            .windows(2)
            .any(|w| w[0].code.contains(needle) && w[1].reads_status);
        assert!(
            run,
            "scripts/mvp-demo.sh must RUN both readers over the backup receipt. \
             `{needle}` is {} — a line carrying it must be followed by a line reading \
             its exit status, or it is a printed instruction rather than a check",
            if mentioned {
                "in the file, but on no line whose status is read"
            } else {
                "not in the file at all"
            }
        );
    }
}

/// **STANDING RULE 20 over the demo script**, through the I29 tokeniser.
///
/// Mutant: `logweir backup run … | tee log` → this fails at assertion time
/// naming the line, because the pipe's status is what the shell would report.
#[test]
fn mvp_demo_masks_no_exit_code() {
    let script = read("scripts/mvp-demo.sh");

    // THE SELF-CHECK FIRST. A lint over a file it found no guarded lines in
    // passes vacuously, and would keep passing if the script were rewritten to
    // spell the binary `"$LOGWEIR_BIN"` — which is exactly the change that
    // makes the rest of this test meaningless. The number is a floor, not a
    // count: the demo runs `logweir` six times and `docker` several more.
    let guarded = exit_code_lint::logical_lines(&script)
        .into_iter()
        .filter(|l| l.is_guarded())
        .count();
    assert!(
        guarded >= 6,
        "the tokeniser found only {guarded} guarded line(s) in scripts/mvp-demo.sh — \
         a lint that sees nothing cannot fail. Is the binary still invoked as the \
         bare word `logweir`?"
    );

    exit_code_lint::assert_no_masked_exit_code("scripts/mvp-demo.sh", &script);
}

/// **The other two scripts this task touches nothing of, checked anyway.**
///
/// Not in the brief, and cheap: the tokeniser is a repository-wide rule, and
/// running it over the two scripts that were already here proves the helper
/// works on text nobody wrote for it. Both predate I29, so a finding would be
/// a real one — `scripts/demo.sh` and `scripts/e2e-seed.sh` pipe freely, but
/// never on a line whose first word is one of the five.
#[test]
fn the_existing_demo_scripts_mask_no_exit_code_either() {
    for rel in ["scripts/demo.sh", "scripts/e2e-seed.sh"] {
        exit_code_lint::assert_no_masked_exit_code(rel, &read(rel));
    }
}

/// **The reusable half (I29), driven over inline fixtures.**
///
/// This is the test Task 28 and Task 32 inherit rather than re-derive. Each
/// fixture is one decision the tokeniser makes, named, with the verdict fixed
/// beside it — so a change of mind about any of them shows up here as a
/// failing row rather than as a quietly widened lint.
///
/// Two of the mutants in Task 12's brief land here:
/// * make the tokeniser flag a `|` inside single quotes → `single_quoted_pipe`
///   fails;
/// * make it skip here-doc detection → `pipe_inside_a_heredoc` fails.
#[test]
fn the_exit_code_tokeniser_is_a_reusable_helper() {
    /// One fixture: a name, the script text, and the findings it must produce
    /// as `(kind, 1-based line)` pairs — in that order, because the order the
    /// tokeniser reports findings in is part of what makes a failure readable.
    type Fixture = (&'static str, &'static str, &'static [(&'static str, usize)]);

    let cases: &[Fixture] = &[
        // ---------------------------------------------------------- the brief's four
        (
            "clean",
            "logweir drill run --spec plan.yaml\nrc=$?\n",
            &[],
        ),
        (
            "piped_logweir_line",
            "logweir drill run --spec plan.yaml | tee run.log\nrc=$?\n",
            &[("piped", 1)],
        ),
        (
            // The body of a here-doc is DATA. A script that documents the
            // wrong way to do something must not be flagged for quoting it.
            "pipe_inside_a_heredoc",
            "cat > note.txt <<'EOF'\nlogweir drill run --spec plan.yaml | tee run.log\nEOF\nrc=$?\n",
            &[],
        ),
        (
            "single_quoted_pipe",
            "logweir drill run --spec plan.yaml --triggered-by 'a | b'\nrc=$?\n",
            &[],
        ),
        // ------------------------------------------------- the controller's additions
        (
            "pipe_inside_a_comment",
            "# logweir drill run --spec plan.yaml | tee run.log\nlogweir drill show card.json\nrc=$?\n",
            &[],
        ),
        (
            "followed_by_an_echo_of_the_status",
            "logweir drill show card.json\necho \"rc=$?\"\n",
            &[],
        ),
        (
            "not_followed_by_a_status_read",
            "logweir drill show card.json\necho done\n",
            &[("unread", 1)],
        ),
        (
            // Single quotes make `$?` literal text, so this line reads nothing.
            "status_read_inside_single_quotes_is_not_a_read",
            "logweir drill show card.json\necho 'rc=$?'\n",
            &[("unread", 1)],
        ),
        // ------------------------------------------------- the shapes the demo uses
        (
            // A wrapped invocation is ONE line, and the status read that
            // follows it is the next one. Without continuation joining, the
            // demo's every multi-line `logweir` call would be a false positive.
            "continuation_joins_into_one_line",
            "logweir restore run --spec plan.yaml \\\n  --approval approval.json \\\n  --out card.json\nrc=$?\n",
            &[],
        ),
        (
            "a_pipe_on_a_continuation_line_is_still_this_lines_pipe",
            "logweir restore run --spec plan.yaml \\\n  --out card.json | tee run.log\nrc=$?\n",
            &[("piped", 1)],
        ),
        (
            // `a || b` reads a's status and branches on it: the rule's
            // compliant form, not its violation. `a | b` replaces it.
            "a_double_pipe_is_a_conditional_not_a_pipe",
            "logweir drill show card.json || echo \"no scorecard\"\nrc=$?\n",
            &[],
        ),
        (
            "pipe_with_stderr_is_a_pipe",
            "docker compose ps |& cat\nrc=$?\n",
            &[("piped", 1)],
        ),
        (
            // Blank lines and comments between the command and its status read
            // are not code, so they do not break the pairing.
            "blank_and_comment_lines_are_skipped_when_looking_for_the_next_line",
            "just e2e-up\n\n# the stack is up; read what it said\nrc=$?\n",
            &[],
        ),
        (
            // The guard is a literal-prefix rule. A variable-spelled binary is
            // not a guarded line — which is why `scripts/mvp-demo.sh` puts the
            // binary on `$PATH` and writes the bare word.
            "a_variable_spelled_binary_is_not_a_guarded_line",
            "\"$LOGWEIR\" drill run --spec plan.yaml | tee run.log\necho done\n",
            &[],
        ),
        (
            "every_guarded_word_is_guarded",
            "kubectl get pods | head\nrc=$?\ncurl https://example.test | head\nrc=$?\njust lint | head\nrc=$?\ndocker ps | head\nrc=$?\n",
            &[("piped", 1), ("piped", 3), ("piped", 5), ("piped", 7)],
        ),
        (
            // A here-STRING is not a here-doc: nothing is consumed after it.
            "a_here_string_swallows_no_lines",
            "logweir drill show card.json <<< \"x\"\nrc=$?\n",
            &[],
        ),
        (
            "a_guarded_last_line_has_nothing_reading_its_status",
            "logweir drill show card.json\n",
            &[("unread", 1)],
        ),
    ];

    for (name, script, expected) in cases {
        let got: Vec<(&str, usize)> = exit_code_lint::findings(script)
            .iter()
            .map(|f| {
                (
                    match f {
                        Finding::PipedStatus { .. } => "piped",
                        Finding::UnreadStatus { .. } => "unread",
                    },
                    f.line(),
                )
            })
            .collect();
        assert_eq!(
            got,
            expected.to_vec(),
            "fixture `{name}` — the tokeniser's verdict changed.\nscript:\n{script}"
        );
    }
}

/// **The quickstart is ONE command and both places say the same one.**
///
/// `README.md`'s quickstart block and `docs/quickstart.md` are read by
/// different people and drift apart on their own. The controller's ruling is
/// that they say the same thing; this is the test half of it (`just links`
/// proves the cross-links resolve, which is a different property).
#[test]
fn the_readme_and_the_quickstart_both_carry_the_one_command() {
    let readme = read("README.md");
    let quickstart = read("docs/quickstart.md");

    assert!(
        readme.contains("## Quickstart"),
        "README.md has no `## Quickstart` heading; the block the ruling is about is gone"
    );
    for (label, text) in [("README.md", &readme), ("docs/quickstart.md", &quickstart)] {
        assert!(
            text.contains("just mvp-demo"),
            "{label} does not name `just mvp-demo` — the one command Phase A's exit \
             criterion is"
        );
        assert!(
            text.contains("just e2e-up"),
            "{label} does not tell the reader to bring the stack up with `just e2e-up`, \
             which `just mvp-demo` refuses without"
        );
    }
    assert!(
        readme.contains("docs/quickstart.md"),
        "README.md's quickstart block must point at docs/quickstart.md for the longer form"
    );
}
