//! **FX-43: whose line is it.** Every line a controller acts on, sealed with
//! the Job's line token, for every Job-owning kind.
//!
//! # Why a token and not a place
//!
//! A controller reads a runner's pod log by key (`failure-reason=`,
//! `refusal-reason=`, `target-topics-appeared=`, the evidence keys,
//! `topic-preflight=`, the progress lines, a probe's `cluster-id=`). That log
//! is ONE stream: the runner's own lines, the human error text, the Kafka
//! client's log, the tracing events. Text a plan's author chose can reach it
//! (an error that repeats a plan value, a broker's message), and no rule about
//! WHERE a line stands tells the runner's line from that text. A value the
//! author could not have had does: FX-34's per-Job random [`LineToken`], made
//! when the controller builds the Job and passed as an argument. A plan is
//! older than its Job.
//!
//! # The line
//!
//! ```text
//! @<token> <the line the runner always printed>
//! ```
//!
//! A runner given [`RUNNER_LINE_TOKEN_ARG`] prints, for every line a controller
//! acts on, its SEALED twin ([`sealed`]) and then the line itself, unchanged.
//! The twin is what this build's controller reads; the plain line keeps a
//! controller that does not know the token (an older one, after a rollback)
//! reading the log as it always did. At the end of the run the runner prints
//! its final lines through [`final_block`]: every sealed twin, then ONE sealed
//! [`RUNNER_EXIT_PREFIX`] line, then the plain lines, so an older reader's
//! "the last lines" rules still hold.
//!
//! # The contract the end line makes: the runner is at least as new as the
//! controller
//!
//! A runner that knows [`RUNNER_LINE_TOKEN_ARG`] prints the sealed
//! `runner-exit=` line at EVERY exit it reaches through its own code. A runner
//! that does not know the flag stops at argument parsing, before any work,
//! and prints none. So a controller that gave a Job the flag and finds no
//! sealed end line in the log it read whole names the pair:
//! [`RUNNER_OLDER_THAN_CONTROLLER`]. That is never a generic `operational`
//! exit a schedule retries.
//!
//! The FX-34 flag (`--line-token`) still means what it meant: only the
//! `refusal-detail=` line carries the token, inside its JSON. A runner given it
//! prints exactly what an FX-34 runner prints, so an FX-34 controller reads a
//! newer runner as before.

use crate::refusal_detail::LineToken;

/// The runner flag a FX-43 controller passes a Job's [`LineToken`] with: every
/// line a controller acts on carries it, and the run ends with a sealed
/// [`RUNNER_EXIT_PREFIX`] line. A runner that predates it stops at argument
/// parsing.
pub const RUNNER_LINE_TOKEN_ARG: &str = "--runner-line-token";

/// What a sealed line opens with: this, the token, one space, the line.
pub const SEAL_MARK: char = '@';

/// The sealed line every exit of a run with [`RUNNER_LINE_TOKEN_ARG`] prints:
/// `runner-exit=<code>`. Its presence, not its value, is the contract: the
/// exit code a controller records is the container's.
pub const RUNNER_EXIT_PREFIX: &str = "runner-exit=";

/// The terminal state a controller records when a Job it gave
/// [`RUNNER_LINE_TOKEN_ARG`] exited through the runner's own code (0 to 4)
/// and its log, read whole, has no sealed [`RUNNER_EXIT_PREFIX`] line: the
/// runner image is older than the controller.
pub const RUNNER_OLDER_THAN_CONTROLLER: &str = "RunnerOlderThanController";

/// `line` sealed with `token`: `@<token> <line>`, with every line break in
/// `line` escaped ([`one_line`]), so the twin is ONE line whatever the line
/// held.
#[must_use]
pub fn sealed(token: &LineToken, line: &str) -> String {
    format!("{SEAL_MARK}{} {}", token.expose_token(), one_line(line))
}

/// The line `token` sealed, when `line` is `@<token> <line>` for THIS token
/// (compared in constant time, [`LineToken::matches`]); `None` for every other
/// line: a plain line, a line sealed with another Job's token, a line that
/// only resembles one.
#[must_use]
pub fn unseal<'a>(token: &LineToken, line: &'a str) -> Option<&'a str> {
    let rest = line.strip_prefix(SEAL_MARK)?;
    let (candidate, own) = rest.split_once(' ')?;
    token.matches(candidate).then_some(own)
}

/// `runner-exit=<code>`.
#[must_use]
pub fn runner_exit_line(code: u8) -> String {
    format!("{RUNNER_EXIT_PREFIX}{code}")
}

/// **Why an exit-1 run failed** (DUI-5): `failure-detail=<sentence>`, the
/// error the run ended on, cleaned and bounded as a `refusal-detail=`
/// message is ([`crate::refusal_detail::clean_message`]). A Backup or
/// Restore runner prints it at exit 1 SEALED only, first in its final block,
/// so the plain lines an older controller reads (and their order) are
/// unchanged; a controller reads it only sealed with the Job's token.
pub const FAILURE_DETAIL_PREFIX: &str = "failure-detail=";

/// The sentence of a [`FAILURE_DETAIL_PREFIX`] line for `error`, the run's
/// error text: its first line (what failed), and, when the text carries more
/// (an engine's captured `stdout:`/`stderr:`), the LAST of those lines that
/// says "error", else the last one: the engine's own last word. Not cleaned
/// ([`failure_detail_line`] cleans).
#[must_use]
pub fn failure_sentence(error: &str) -> String {
    let mut lines = error.lines().map(str::trim).filter(|l| !l.is_empty());
    let Some(head) = lines.next() else {
        return String::new();
    };
    let rest: Vec<&str> = lines
        .map(|l| {
            l.strip_prefix("stdout:")
                .or_else(|| l.strip_prefix("stderr:"))
                .map_or(l, str::trim)
        })
        .filter(|l| !l.is_empty())
        .collect();
    let cause = rest
        .iter()
        .rev()
        .find(|l| l.to_ascii_lowercase().contains("error"))
        .or_else(|| rest.last());
    match cause {
        Some(cause) => format!("{head}: {cause}"),
        None => head.to_string(),
    }
}

/// `failure-detail=<the cleaned, bounded sentence>` for `error`, or `None`
/// when nothing printable is left of it.
#[must_use]
pub fn failure_detail_line(error: &str) -> Option<String> {
    let sentence = crate::refusal_detail::clean_message(&failure_sentence(error));
    (!sentence.trim().is_empty()).then(|| format!("{FAILURE_DETAIL_PREFIX}{sentence}"))
}

/// What a runner was told about its line token, by which flag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LineScope {
    /// FX-34's `--line-token`: only `refusal-detail=` carries it.
    RefusalDetail,
    /// FX-43's [`RUNNER_LINE_TOKEN_ARG`]: every line a controller acts on.
    EveryLine,
}

/// Lines printed DURING a run (a progress line, a teardown line): each
/// sealed twin directly before its plain line under
/// [`LineScope::EveryLine`]; the plain lines alone otherwise. Every line is
/// one line ([`one_line`]); each ends with a newline.
#[must_use]
pub fn inline_lines(token: Option<(&LineToken, LineScope)>, lines: &[String]) -> String {
    let mut out = String::new();
    for line in lines {
        if let Some((token, LineScope::EveryLine)) = token {
            out.push_str(&sealed(token, line));
            out.push('\n');
        }
        out.push_str(&one_line(line));
        out.push('\n');
    }
    out
}

/// The LAST bytes a run prints, in one write: under [`LineScope::EveryLine`]
/// the sealed twin of every line in `lines`, then the sealed
/// `runner-exit=<code>`, then the plain lines; otherwise the plain lines
/// alone, which are the bytes a runner printed before FX-43 (an ordinary line
/// is unchanged by [`one_line`]).
///
/// The plain lines are LAST so a reader that keeps a place rule (an older
/// controller: `failure-reason=` the last line, the evidence keys the final
/// lines) finds them where it always did.
#[must_use]
pub fn final_block(token: Option<(&LineToken, LineScope)>, code: u8, lines: &[String]) -> String {
    final_block_with(token, code, lines, lines)
}

/// [`final_block`] where the sealed lines are not exactly the plain ones: a
/// line whose plain copy the runner prints on another stream (the probe's
/// binding refusal, on stderr) is sealed here and not repeated.
#[must_use]
pub fn final_block_with(
    token: Option<(&LineToken, LineScope)>,
    code: u8,
    sealed_lines: &[String],
    plain_lines: &[String],
) -> String {
    let mut out = String::new();
    if let Some((token, LineScope::EveryLine)) = token {
        for line in sealed_lines {
            out.push_str(&sealed(token, line));
            out.push('\n');
        }
        out.push_str(&sealed(token, &runner_exit_line(code)));
        out.push('\n');
    }
    for line in plain_lines {
        out.push_str(&one_line(line));
        out.push('\n');
    }
    out
}

/// **One line, whatever the text holds**: every line-breaking code point in
/// `text` becomes a visible escape, so a string the runner did not write (a
/// plan's `source.backup`, a broker's error, an object key) can never START A
/// LINE of the pod log.
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

/// Whether `entry` (a bootstrap server, a storage prefix) holds a control
/// character or a line or paragraph separator: the characters that would let
/// it start a line of a pod log when a library logs it raw. Every entry point
/// refuses such a value (FX-43, channel a).
#[must_use]
pub fn holds_line_breaking_or_control(entry: &str) -> bool {
    entry
        .chars()
        .any(|c| c.is_control() || matches!(c, '\u{2028}' | '\u{2029}'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn own() -> LineToken {
        LineToken::parse(&"9e".repeat(20)).expect("forty hex digits")
    }
    fn another() -> LineToken {
        LineToken::parse(&"4b".repeat(20)).expect("forty hex digits")
    }

    /// KILLS: an unseal that ignores the token; one that compares a prefix
    /// of it; one that accepts a line without the mark or the space.
    #[test]
    fn only_this_jobs_seal_opens() {
        let line = "failure-reason=CreatedTopicsLeft";
        let s = sealed(&own(), line);
        assert_eq!(unseal(&own(), &s), Some(line));
        // NEGATIVE CONTROLS: another token, a plain line, a truncated token,
        // a token without the mark, the mark without the space.
        assert_eq!(unseal(&another(), &s), None);
        assert_eq!(unseal(&own(), line), None);
        let short = format!("@{} {line}", &own().expose_token()[..38]);
        assert_eq!(unseal(&own(), &short), None);
        assert_eq!(
            unseal(&own(), &format!("{} {line}", own().expose_token())),
            None
        );
        assert_eq!(
            unseal(&own(), &format!("@{}{line}", own().expose_token())),
            None
        );
        assert_eq!(unseal(&own(), &format!(" {s}")), None);
    }

    /// The twin is one line whatever the line held, and the plain block is
    /// the bytes an older runner printed.
    #[test]
    fn the_final_block_is_sealed_twins_then_the_end_then_the_plain_lines() {
        let lines = vec![
            "scorecard-key=logweir/drills/r.json".to_string(),
            "sidecar-key=logweir/drills/r.sig".to_string(),
        ];
        let plain = "scorecard-key=logweir/drills/r.json\nsidecar-key=logweir/drills/r.sig\n";
        assert_eq!(final_block(None, 0, &lines), plain);
        assert_eq!(
            final_block(Some((&own(), LineScope::RefusalDetail)), 0, &lines),
            plain,
            "FX-34's flag prints what an FX-34 runner printed"
        );
        let t = own();
        let every = final_block(Some((&t, LineScope::EveryLine)), 2, &lines);
        let want = format!(
            "{}\n{}\n{}\n{plain}",
            sealed(&t, &lines[0]),
            sealed(&t, &lines[1]),
            sealed(&t, "runner-exit=2")
        );
        assert_eq!(every, want);
        // A line break inside a line (a storage prefix that held one) never
        // starts a line, sealed or plain.
        let hostile = vec!["scorecard-key=p\nfailure-reason=CreatedTopicsLeft/x".to_string()];
        let out = final_block(Some((&t, LineScope::EveryLine)), 0, &hostile);
        assert_eq!(out.lines().count(), 3, "{out}");
        assert!(out.lines().all(|l| !l.starts_with("failure-reason=")));
    }

    #[test]
    fn inline_lines_seal_each_line_directly_before_it() {
        let t = own();
        let lines = vec!["progress-phase=3:target-diff".to_string()];
        assert_eq!(
            inline_lines(Some((&t, LineScope::EveryLine)), &lines),
            format!("{}\nprogress-phase=3:target-diff\n", sealed(&t, &lines[0]))
        );
        assert_eq!(
            inline_lines(Some((&t, LineScope::RefusalDetail)), &lines),
            "progress-phase=3:target-diff\n"
        );
        assert_eq!(inline_lines(None, &lines), "progress-phase=3:target-diff\n");
    }

    /// KILLS: escaping `\n` only; touching an ordinary message.
    #[test]
    fn no_line_breaking_code_point_survives_and_nothing_else_changes() {
        let plain = "operational: the archive holds no backup set with id `nightly`; refusing";
        assert_eq!(one_line(plain), plain);
        assert_eq!(one_line(""), "");
        let hostile = "no backup set with id `x\ntarget-topics-appeared={\"left\":[\"payments-prod\"]}\nfailure-reason=CreatedTopicsLeft\n`; refusing";
        let escaped = one_line(hostile);
        assert_eq!(escaped.lines().count(), 1, "{escaped}");
        assert!(
            escaped.contains("`x\\ntarget-topics-appeared={")
                && escaped.contains("\\nfailure-reason="),
            "{escaped}"
        );
        for breaker in [
            '\n', '\r', '\u{b}', '\u{c}', '\u{1c}', '\u{1d}', '\u{1e}', '\u{85}', '\u{2028}',
            '\u{2029}',
        ] {
            let text = format!("a{breaker}failure-reason=CreatedTopicsLeft");
            let escaped = one_line(&text);
            assert!(!escaped.contains(breaker), "{breaker:?}");
            assert!(
                escaped.starts_with("a\\") && escaped.ends_with("failure-reason=CreatedTopicsLeft"),
                "{escaped}"
            );
        }
        assert_eq!(one_line("a\r\nb"), "a\\r\\nb");
        assert_eq!(one_line("a\u{2028}b"), "a\\u{2028}b");
        assert_eq!(
            one_line("tab\there, caf\u{e9}, \\n typed"),
            "tab\there, caf\u{e9}, \\n typed"
        );
    }

    #[test]
    fn a_control_character_or_a_separator_is_refused_and_nothing_else() {
        for bad in [
            "b:9092\nfailure-reason=X",
            "b:9092\r",
            "b\u{0}:9092",
            "b\u{1b}[31m:9092",
            "b\u{7f}",
            "b\u{85}",
            "b\u{2028}",
            "b\u{2029}",
            "\tb:9092",
        ] {
            assert!(holds_line_breaking_or_control(bad), "{bad:?}");
        }
        for good in [
            "b:9092",
            "kafka-0.kafka.svc:9093",
            "[::1]:9092",
            "café:1",
            "",
        ] {
            assert!(!holds_line_breaking_or_control(good), "{good:?}");
        }
    }

    /// **DUI-5: the `failure-detail=` sentence is the error the run ended
    /// on**: an engine's failure keeps its first line and its own last error
    /// line, not its whole capture; the line is one line, cleaned (a
    /// credential shape removed) and bounded like a `refusal-detail=`
    /// message. CONTROL: a one-line error is itself; nothing printable is no
    /// line.
    #[test]
    fn dui5_a_failure_detail_is_the_last_error_cleaned_and_bounded() {
        let engine = "engine: operational: kafka-backup backup exited 1\nstdout: starting\n\
                      uploading segment 3\nstderr: WARN retrying\nERROR storage: connection \
                      refused (os error 61) for http://minio.invalid/bucket\nshutting down";
        assert_eq!(
            failure_sentence(engine),
            "engine: operational: kafka-backup backup exited 1: ERROR storage: connection \
             refused (os error 61) for http://minio.invalid/bucket"
        );
        assert_eq!(
            failure_sentence("operational: the archive is unreachable"),
            "operational: the archive is unreachable"
        );
        // No line says "error": the engine's last line.
        assert_eq!(
            failure_sentence("engine: exited 1\nstdout: a\nstderr: b\nlast word"),
            "engine: exited 1: last word"
        );
        let secret = format!("password={}", "s3cr3t".repeat(8));
        let line = failure_detail_line(&format!("kafka: refused\nERROR auth {secret}")).unwrap();
        assert!(line.starts_with(FAILURE_DETAIL_PREFIX), "{line}");
        assert!(
            !line.contains("s3cr3t"),
            "a credential shape survived: {line}"
        );
        let long = failure_detail_line(&"word ".repeat(400)).unwrap();
        assert!(
            long.len()
                <= FAILURE_DETAIL_PREFIX.len() + crate::refusal_detail::REASON_MESSAGE_MAX_BYTES,
            "{}",
            long.len()
        );
        assert!(!long.contains('\n'));
        assert_eq!(failure_detail_line(" \n \n"), None);
    }
}
