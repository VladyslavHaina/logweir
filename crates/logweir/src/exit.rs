/// Global Constraint 11 / spec §6 C5. A CronJob, a change-control pipeline and
/// an alert rule all consume these without parsing stdout.
///
/// Every variant is constructed on a live path as of Task 21a — `Ok` and
/// `DrillNotPass` in `crate::drill::run`, `Operational` there and in
/// `main.rs`'s clap-usage arm, `GuardRefused` and `SigningOrLock` through
/// `impl From<DrillError> for ExitCode` — so Task 6's `#[allow(dead_code)]`
/// is gone. If a variant ever becomes unreachable again, `clippy -D warnings`
/// says so rather than an allow hiding it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitCode {
    /// Clean pass.
    Ok = 0,
    /// The drill could not be attempted or continued for a reason that says
    /// nothing about the archive. NO artifact is written.
    Operational = 1,
    /// A drill result that is not a pass. A scorecard IS written and signed.
    DrillNotPass = 2,
    /// Plan refused by a guard, before anything ran.
    GuardRefused = 3,
    /// Signing or lock-proof failed — and NOTHING was uploaded.
    SigningOrLock = 4,
}

impl From<ExitCode> for std::process::ExitCode {
    fn from(c: ExitCode) -> Self {
        std::process::ExitCode::from(c as u8)
    }
}

/// [I9] Prints `refusal-reason=<TerminalState>` on STDOUT, as the process's
/// FINAL stdout line, for exit 3 only.
///
/// # Why stdout, and why last
///
/// The pod log API has no stream selector: `GET /api/v1/namespaces/{ns}/pods/
/// {pod}/log` returns the container's stdout and stderr interleaved into one
/// stream with no marker saying which byte came from which, so **nothing a
/// runner writes on stderr is distinguishable by a controller** (spec §7
/// amendment 4; critique B H9). A refusal reason on stderr is therefore not a
/// machine-readable channel at all — it is a string in a blob a human reads.
/// Being LAST is the other half: a controller tailing the log reads the final
/// line, so the reason must come after the `drill finished` tracing line and
/// after any summary line, which is why the call sits at the end of
/// `crate::drill::exiting` rather than at the refusal site.
///
/// The state is derived, never passed in: `logweir_core::guard::
/// refusal_reason_line` owns the mapping from a refusal message to a terminal
/// state, so a caller cannot invent a fourth state or spell an existing one
/// differently. `logweir-core` does no I/O, which is why the `println!` is
/// here and the string is there.
///
/// Rust's `Stdout` is a `LineWriter`, so the newline flushes; this shares the
/// one global stdout handle with the tracing subscriber's writer, which is
/// what makes "after the tracing line" an ordering and not a race.
pub fn print_refusal_reason(message: &str) {
    // `expect`-free: a closed stdout is not a reason to change the exit code,
    // which GC11 has already decided by the time this is reached.
    let _ = print_refusal_reason_to(&mut std::io::stdout().lock(), message);
}

/// The writer seam `print_refusal_reason` prints through, so a test can assert
/// the EXACT BYTES — the line and its newline — instead of trusting a
/// `println!` nobody can observe.
///
/// It is `pub` rather than `#[cfg(test)]` deliberately: the assertion that
/// matters lives in an integration test
/// (`crates/logweir/tests/topic_preflight.rs`'s
/// `a_target_topic_refusal_prints_its_terminal_state`), which is a separate
/// crate and cannot see a `#[cfg(test)]` item. Task 8 added it; before it,
/// `print_refusal_reason` was a bare `println!` and the "on stdout, last"
/// half of interface **I9** had no in-process coverage at all.
pub fn print_refusal_reason_to<W: std::io::Write>(w: &mut W, message: &str) -> std::io::Result<()> {
    writeln!(w, "{}", logweir_core::guard::refusal_reason_line(message))
}

/// **FX-34.** Both lines a guard refusal prints, in the order the contract
/// fixes: `refusal-detail=<one JSON object>` and then I9's
/// `refusal-reason=<TerminalState>`, which stays the process's FINAL stdout
/// line.
///
/// # Why a second line, and why it is first
///
/// `refusal-reason=` names a state from a closed list, and most refusals have
/// none: the sentence that says WHY was only on the human line, in a pod log
/// that is gone with the pod. The detail line carries the reason code and
/// that sentence in a form a controller can validate before it stores them,
/// and it goes BEFORE the state line so every reader of "the final line",
/// this build's or an older one's, still finds the state there.
///
/// # The line carries the Job's token, when this process was given one
///
/// A plan can start a line of its own in a pod log: the human line on stderr
/// prints an error's text raw, and an error may repeat a plan value that
/// holds a line break. Nothing about where a line stands tells the two apart
/// in one merged log, so a controller honours a `refusal-detail=` line only
/// when it carries the token that controller made for this Job and passed as
/// `--line-token` ([`set_line_token`]). The plan was written before the Job
/// existed and cannot hold it. A run started by hand has no token and prints
/// the line without one.
///
/// Both lines go out in ONE write, at EVERY exit 3 (a refusal that says
/// nothing prints `logweir_core::refusal_detail::NO_SENTENCE`), and the
/// callers (`drill::exiting`, `backup::exiting`) print nothing after them.
/// The human line on stderr and the tracing lines are unchanged.
pub fn print_refusal(run: logweir_core::refusal_detail::RefusingRun, message: &str) {
    // `expect`-free for `print_refusal_reason`'s reason: a closed stdout does
    // not change an exit code GC11 has already decided.
    let _ = print_refusal_to(&mut std::io::stdout().lock(), run, line_token(), message);
}

/// The writer seam [`print_refusal`] prints through, so a test asserts the
/// exact bytes: the detail line (with `token` in it when there is one), the
/// state line, each with its newline, in one `write_all`.
pub fn print_refusal_to<W: std::io::Write>(
    w: &mut W,
    run: logweir_core::refusal_detail::RefusingRun,
    token: Option<&logweir_core::refusal_detail::LineToken>,
    message: &str,
) -> std::io::Result<()> {
    let both = format!(
        "{}\n{}\n",
        logweir_core::refusal_detail::refusal_detail_line(run, token, message),
        logweir_core::guard::refusal_reason_line(message)
    );
    w.write_all(both.as_bytes())
}

/// The line token this process was started with, if any.
///
/// # Why a process-wide value, set once
///
/// The token is a fact about the PROCESS (the Job it is the runner of), read
/// off the command line before anything runs, and used at exactly one place,
/// the last thing a refused run prints. Carrying it there as a parameter
/// would thread it through `report_with`, `report` and both `exiting`s for
/// one use. Set once and never changed: [`set_line_token`] after the first is
/// ignored.
static LINE_TOKEN: std::sync::OnceLock<Option<logweir_core::refusal_detail::LineToken>> =
    std::sync::OnceLock::new();

/// Hold `--line-token`'s value for [`print_refusal`]. `main` calls it once,
/// before dispatch, for the two commands that take the flag. A later call
/// changes nothing.
pub fn set_line_token(token: Option<logweir_core::refusal_detail::LineToken>) {
    let _ = LINE_TOKEN.set(token);
}

fn line_token() -> Option<&'static logweir_core::refusal_detail::LineToken> {
    LINE_TOKEN.get().and_then(Option::as_ref)
}

/// **One line, whatever the text holds** (PROD-15.1 review 2, M1): every
/// line-breaking code point in `text` becomes a visible escape, so a string
/// this process did not write — a plan's `source.backup`, a broker's error, an
/// object key — can never START A LINE of the pod log.
///
/// # Why it matters
///
/// A controller reads a runner's log by KEY at the start of a line
/// (`refusal-reason=`, `failure-reason=`, `target-topics-appeared=`, the
/// evidence keys), and the pod log API has no stream selector, so stderr is
/// in the same stream. An error text printed raw with a line break in it
/// could therefore carry a forged key line: the review's probe put
/// `target-topics-appeared={…}` and `failure-reason=CreatedTopicsLeft` inside
/// a plan string, and the Restore then named two topics the run never
/// touched as its own, with "remove it yourself" beside them.
///
/// # What is escaped
///
/// Every code point some reader treats as a line boundary: `\n`, `\r`,
/// vertical tab, form feed, the three information separators U+001C to
/// U+001E, NEL (U+0085), and the line and paragraph separators U+2028 and
/// U+2029. `\n` and `\r` are written as those two characters; the others as
/// `\u{…}`. Nothing else is touched, so an ordinary message is the bytes it
/// was. A backslash is not doubled: this is a display form for a person, not
/// an encoding anything decodes.
#[must_use]
pub fn one_line(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\u{b}' | '\u{c}' | '\u{1c}' | '\u{1d}' | '\u{1e}' | '\u{85}' | '\u{2028}'
            | '\u{2029}' => out.push_str(&format!("\\u{{{:x}}}", u32::from(c))),
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod one_line_tests {
    use super::one_line;

    /// KILLS: printing an error text raw; escaping `\n` only (a bare `\r`
    /// overwrites the line on a terminal, and other readers split on the
    /// rest); touching an ordinary message.
    #[test]
    fn no_line_breaking_code_point_survives_and_nothing_else_changes() {
        let plain = "operational: the archive holds no backup set with id `nightly`; refusing";
        assert_eq!(one_line(plain), plain);
        assert_eq!(one_line(""), "");
        // The review's forgery (probe C2): two key lines inside a plan string.
        let hostile = "no backup set with id `x\ntarget-topics-appeared={\"left\":[\"payments-prod\"]}\nfailure-reason=CreatedTopicsLeft\n`; refusing";
        let escaped = one_line(hostile);
        assert_eq!(escaped.lines().count(), 1, "{escaped}");
        assert!(
            escaped.contains("`x\\ntarget-topics-appeared={")
                && escaped.contains("\\nfailure-reason="),
            "{escaped}"
        );
        // Every boundary some reader splits on: Rust's `lines`, Python's
        // `splitlines`, a terminal's carriage return.
        for breaker in [
            '\n', '\r', '\u{b}', '\u{c}', '\u{1c}', '\u{1d}', '\u{1e}', '\u{85}', '\u{2028}',
            '\u{2029}',
        ] {
            let text = format!("a{breaker}failure-reason=CreatedTopicsLeft");
            let escaped = one_line(&text);
            assert!(!escaped.contains(breaker), "{:?}", breaker);
            assert!(
                escaped.starts_with("a\\") && escaped.ends_with("failure-reason=CreatedTopicsLeft"),
                "{escaped}"
            );
        }
        assert_eq!(one_line("a\r\nb"), "a\\r\\nb");
        assert_eq!(one_line("a\u{2028}b"), "a\\u{2028}b");
        // Not line breaks: left alone.
        assert_eq!(
            one_line("tab\there, caf\u{e9}, \\n typed"),
            "tab\there, caf\u{e9}, \\n typed"
        );
    }
}
