/// Global Constraint 11 / spec §6 C5. A CronJob, a change-control pipeline and
/// an alert rule all consume these without parsing stdout.
///
/// Every variant is constructed on a live path, so `clippy -D warnings`
/// reports one that becomes unreachable.
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

/// **The guard refusal's two lines** (interface I9): `refusal-detail=<one
/// JSON object>` and then `refusal-reason=<TerminalState>`, which stays the
/// FINAL one of the pair.
///
/// # Why stdout, and why last (I9)
///
/// The pod log API has no stream selector: `GET /api/v1/namespaces/{ns}/pods/
/// {pod}/log` returns the container's stdout and stderr interleaved into one
/// stream with no marker saying which byte came from which, so **nothing a
/// runner writes on stderr is distinguishable by a controller** (spec §7
/// amendment 4). Being LAST is the other half: the pair is pushed into the
/// run's [`FinalLines`], which the terminal path prints after the
/// `drill finished` tracing line and after any summary line, in one write.
///
/// The state is derived, never passed in: `logweir_core::guard::
/// refusal_reason_line` owns the mapping from a refusal message to a terminal
/// state, so a caller cannot invent a fourth state or spell an existing one
/// differently.
///
/// # Why a second line, and why it is first
///
/// `refusal-reason=` names a state from a closed list, and most refusals have
/// none: the sentence that says WHY was only on the human line. The detail
/// line carries the reason code and that sentence in a form a controller can
/// validate before it stores them, and it goes BEFORE the state line so every
/// reader of "the final line", this build's or an older one's, still finds
/// the state there.
///
/// # Whose line it is: the Job's token
///
/// The detail line carries the token this process was given, FX-34's
/// `--line-token` or FX-43's `--runner-line-token` ([`set_line_tokens`]), in
/// its JSON; under FX-43's flag the pair is ALSO sealed like every other line
/// a controller acts on (`logweir_core::runner_line`). A run started by hand
/// has no token and prints the pair without one.
///
/// # There is no printer of the state line alone
///
/// An exit-3 path that could print `refusal-reason=` by itself would print a
/// state line and no detail line, so no such printer exists.
#[must_use]
pub fn refusal_lines(
    run: logweir_core::refusal_detail::RefusingRun,
    token: Option<&logweir_core::refusal_detail::LineToken>,
    message: &str,
) -> [String; 2] {
    [
        logweir_core::refusal_detail::refusal_detail_line(run, token, message),
        logweir_core::guard::refusal_reason_line(message),
    ]
}

/// The writer seam over [`refusal_lines`], so a test asserts the exact bytes
/// of the pair an FX-34 runner (or a run with no token) prints: the detail
/// line (with `token` in it when there is one), the state line, each with its
/// newline, in one `write_all`.
///
/// It is `pub` rather than `#[cfg(test)]` deliberately: the assertions that
/// matter live in integration tests (`crates/logweir/tests/refusal_detail.rs`,
/// and `crates/logweir/tests/topic_preflight.rs`'s
/// `a_target_topic_refusal_prints_its_terminal_state`), which are separate
/// crates and cannot see a `#[cfg(test)]` item.
pub fn print_refusal_to<W: std::io::Write>(
    w: &mut W,
    run: logweir_core::refusal_detail::RefusingRun,
    token: Option<&logweir_core::refusal_detail::LineToken>,
    message: &str,
) -> std::io::Result<()> {
    let [detail, state] = refusal_lines(run, token, message);
    w.write_all(format!("{detail}\n{state}\n").as_bytes())
}

/// **The lines a terminal path owes a controller**, printed LAST and in ONE
/// write ([`FinalLines::print`]).
///
/// The terminal paths (`drill::exiting`, `backup::exiting`, the probe) push
/// every line a controller reads by key, in the order they always printed
/// them, and print the block once. Under FX-43's `--runner-line-token` the
/// block is every line SEALED with the Job's token, then the sealed
/// `runner-exit=<code>`, then the plain lines
/// (`logweir_core::runner_line::final_block`); otherwise it is the plain
/// lines, the bytes this runner always printed. Every line goes out on one
/// line whatever it holds (a key built from a plan's storage prefix included).
#[derive(Debug, Default)]
pub struct FinalLines {
    /// Sealed only, FIRST in the sealed block ([`FinalLines::failure_detail`]).
    sealed_first: Vec<String>,
    lines: Vec<String>,
    vouched: Vec<String>,
}

impl FinalLines {
    /// **Why an exit-1 run failed** (DUI-5): the sealed
    /// `failure-detail=` line for `error`
    /// (`logweir_core::runner_line::failure_detail_line`). Sealed only, and
    /// FIRST in the sealed block, so a reader's place rule over the sealed
    /// lines (a Restore's `failure-reason=` last, `target-topics-appeared=`
    /// before it) still holds, and the plain lines an older controller reads
    /// are byte for byte what they were. Without FX-43's token nothing is
    /// printed: the error is already on stderr.
    pub fn failure_detail(&mut self, error: &str) {
        self.sealed_first
            .extend(logweir_core::runner_line::failure_detail_line(error));
    }

    /// One line a controller reads.
    pub fn push(&mut self, line: impl Into<String>) {
        self.lines.push(line.into());
    }

    /// A line whose plain copy this process prints elsewhere (the probe's
    /// binding refusal, on stderr): under FX-43's flag it is sealed in the
    /// block with the others, and it adds nothing to the plain lines.
    pub fn vouch(&mut self, line: impl Into<String>) {
        self.vouched.push(line.into());
    }

    /// Several, in order.
    pub fn extend(&mut self, lines: impl IntoIterator<Item = String>) {
        self.lines.extend(lines);
    }

    /// I9's pair for a guard refusal of `run` ([`refusal_lines`]), carrying
    /// this process's token in the detail line when it was given one.
    pub fn refusal(&mut self, run: logweir_core::refusal_detail::RefusingRun, message: &str) {
        self.extend(refusal_lines(run, line_token(), message));
    }

    /// The block's bytes for exit `code`: pure, for the tests.
    #[must_use]
    pub fn render(&self, code: ExitCode) -> String {
        self.render_with(scoped_token(), code)
    }

    /// [`FinalLines::render`] under `token` rather than this process's.
    #[must_use]
    pub fn render_with(
        &self,
        token: Option<(
            &logweir_core::refusal_detail::LineToken,
            logweir_core::runner_line::LineScope,
        )>,
        code: ExitCode,
    ) -> String {
        let sealed: Vec<String> = self
            .sealed_first
            .iter()
            .chain(&self.lines)
            .chain(&self.vouched)
            .cloned()
            .collect();
        logweir_core::runner_line::final_block_with(token, code as u8, &sealed, &self.lines)
    }

    /// Print the block on stdout, in one write. `expect`-free: a closed stdout
    /// is not a reason to change the exit code, which GC11 has already
    /// decided by the time this is reached.
    pub fn print(self, code: ExitCode) {
        use std::io::Write as _;
        let _ = std::io::stdout()
            .lock()
            .write_all(self.render(code).as_bytes());
    }
}

/// **The check runner's stdout under FX-43's flag** (`logweir check run`): a
/// writer that passes every line through and, before a line that opens with
/// one of `sealed`'s prefixes (the end frame, `refusal-reason=`), writes that
/// line's sealed twin. The check's other frames are not sealed one by one:
/// the end frame declares every stream's digest and the topic lines' count
/// and digest, so the sealed end frame binds them, and sealing each would cost
/// 42 bytes a line against a relay read bounded in bytes. Without the token it
/// is the bytes it was given. [`print_end`] prints the end line after it.
pub struct SealingLines<W: std::io::Write> {
    inner: W,
    sealed: &'static [&'static str],
    token: Option<logweir_core::refusal_detail::LineToken>,
    pending: Vec<u8>,
}

impl<W: std::io::Write> SealingLines<W> {
    /// Seal the lines opening with one of `sealed` written through `inner`,
    /// with this process's FX-43 token ([`set_line_tokens`]).
    pub fn new(inner: W, sealed: &'static [&'static str]) -> Self {
        let token = match scoped_token() {
            Some((t, logweir_core::runner_line::LineScope::EveryLine)) => Some(t.clone()),
            _ => None,
        };
        Self::with_token(inner, sealed, token)
    }

    /// [`SealingLines::new`] with the token given (`None`: pass through).
    pub fn with_token(
        inner: W,
        sealed: &'static [&'static str],
        token: Option<logweir_core::refusal_detail::LineToken>,
    ) -> Self {
        Self {
            inner,
            sealed,
            token,
            pending: Vec::new(),
        }
    }

    fn emit(&mut self, line: &[u8]) -> std::io::Result<()> {
        if let Some(token) = &self.token {
            let text = String::from_utf8_lossy(line);
            let text = text.trim_end_matches(['\n', '\r']);
            if self.sealed.iter().any(|p| text.starts_with(p)) {
                let twin = logweir_core::runner_line::sealed(token, text);
                self.inner.write_all(twin.as_bytes())?;
                self.inner.write_all(b"\n")?;
            }
        }
        self.inner.write_all(line)
    }
}

impl<W: std::io::Write> std::io::Write for SealingLines<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.pending.extend_from_slice(buf);
        while let Some(at) = self.pending.iter().position(|b| *b == b'\n') {
            let line: Vec<u8> = self.pending.drain(..=at).collect();
            self.emit(&line)?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if !self.pending.is_empty() {
            let rest = std::mem::take(&mut self.pending);
            self.emit(&rest)?;
        }
        self.inner.flush()
    }
}

impl<W: std::io::Write> Drop for SealingLines<W> {
    fn drop(&mut self) {
        let _ = std::io::Write::flush(self);
    }
}

/// The sealed `runner-exit=<code>` line alone, on stdout, under FX-43's flag
/// (nothing otherwise): the end of a run whose lines were printed as they
/// were produced (`logweir check run`).
pub fn print_end(code: ExitCode) {
    use std::io::Write as _;
    if let Some((token, logweir_core::runner_line::LineScope::EveryLine)) = scoped_token() {
        let line = logweir_core::runner_line::sealed(
            token,
            &logweir_core::runner_line::runner_exit_line(code as u8),
        );
        let _ = std::io::stdout()
            .lock()
            .write_all(format!("{line}\n").as_bytes());
    }
}

/// Print lines a controller reads DURING a run (a progress line, a teardown
/// line), each sealed directly before itself under FX-43's flag, in one write
/// (`logweir_core::runner_line::inline_lines`).
pub fn print_lines(lines: &[String]) {
    use std::io::Write as _;
    let text = logweir_core::runner_line::inline_lines(scoped_token(), lines);
    let _ = std::io::stdout().lock().write_all(text.as_bytes());
}

/// The line token this process was started with, and by which flag.
///
/// # Why a process-wide value, set once
///
/// The token is a fact about the PROCESS (the Job it is the runner of), read
/// off the command line before anything runs, and used wherever a line a
/// controller reads is printed. Set once and never changed:
/// [`set_line_tokens`] after the first is ignored.
static LINE_TOKEN: std::sync::OnceLock<
    Option<(
        logweir_core::refusal_detail::LineToken,
        logweir_core::runner_line::LineScope,
    )>,
> = std::sync::OnceLock::new();

/// Hold the token of `--line-token` (FX-34: the `refusal-detail=` line only)
/// or of `--runner-line-token` (FX-43: every line a controller acts on, and a
/// sealed `runner-exit=` at the end). clap lets at most one through. `main`
/// calls it once, before dispatch, for the commands that take the flags. A
/// later call changes nothing.
pub fn set_line_tokens(
    refusal_detail: Option<logweir_core::refusal_detail::LineToken>,
    every_line: Option<logweir_core::refusal_detail::LineToken>,
) {
    use logweir_core::runner_line::LineScope;
    let scoped = every_line
        .map(|t| (t, LineScope::EveryLine))
        .or_else(|| refusal_detail.map(|t| (t, LineScope::RefusalDetail)));
    let _ = LINE_TOKEN.set(scoped);
}

fn line_token() -> Option<&'static logweir_core::refusal_detail::LineToken> {
    LINE_TOKEN.get().and_then(Option::as_ref).map(|(t, _)| t)
}

fn scoped_token() -> Option<(
    &'static logweir_core::refusal_detail::LineToken,
    logweir_core::runner_line::LineScope,
)> {
    LINE_TOKEN
        .get()
        .and_then(Option::as_ref)
        .map(|(t, scope)| (t, *scope))
}

/// **One line, whatever the text holds** (`logweir_core::runner_line::one_line`):
/// every line-breaking code point becomes a visible escape, so a string this
/// process did not write (a plan's `source.backup`, a broker's error, an
/// object key) can never START A LINE of the pod log a controller reads by key.
pub use logweir_core::runner_line::one_line;

#[cfg(test)]
mod failure_detail_tests {
    use super::{ExitCode, FinalLines};
    use logweir_core::runner_line::{sealed, unseal, LineScope};

    /// **DUI-5: an exit-1 block carries the sealed `failure-detail=` FIRST,
    /// and nothing else changes.** A Restore's stopped creation step keeps its
    /// place rule (`failure-reason=` the last sealed line,
    /// `target-topics-appeared=` before it), and the plain lines are the ones
    /// an older controller always read. CONTROL: without FX-43's token (none,
    /// or FX-34's) the block is the plain lines alone, with no
    /// `failure-detail=` anywhere.
    ///
    /// KILLS: the detail printed plain (an older reader's tail changes); the
    /// detail sealed after `failure-reason=` (the Restore place rule breaks).
    #[test]
    fn dui5_the_failure_detail_is_sealed_only_and_first() {
        let t = logweir_core::refusal_detail::LineToken::parse(&"3c".repeat(20))
            .expect("forty hex digits");
        let mut last = FinalLines::default();
        last.failure_detail("engine: operational: exited 1\nstderr: ERROR the store refused");
        last.push("target-topics-appeared={}");
        last.push("failure-reason=CreatedTopicsLeft");
        let block = last.render_with(Some((&t, LineScope::EveryLine)), ExitCode::Operational);
        let own: Vec<&str> = block.lines().filter_map(|l| unseal(&t, l)).collect();
        assert_eq!(
            own,
            vec![
                "failure-detail=engine: operational: exited 1: ERROR the store refused",
                "target-topics-appeared={}",
                "failure-reason=CreatedTopicsLeft",
                "runner-exit=1",
            ]
        );
        let plain: Vec<&str> = block.lines().filter(|l| unseal(&t, l).is_none()).collect();
        assert_eq!(
            plain,
            vec![
                "target-topics-appeared={}",
                "failure-reason=CreatedTopicsLeft"
            ]
        );
        assert!(block.starts_with(&sealed(&t, own[0])));
        for token in [None, Some((&t, LineScope::RefusalDetail))] {
            let block = last.render_with(token, ExitCode::Operational);
            assert_eq!(
                block,
                "target-topics-appeared={}\nfailure-reason=CreatedTopicsLeft\n"
            );
        }
    }
}

#[cfg(test)]
mod sealing_lines_tests {
    use super::SealingLines;
    use std::io::Write as _;

    /// **The check runner's stdout under FX-43's flag**: only the lines
    /// opening with a sealed prefix get a sealed twin, directly before them;
    /// every byte written is passed through unchanged, split writes and an
    /// unterminated last line included. Without a token: the bytes alone.
    /// KILLS: a twin after the line (an older reader's "frame after the end
    /// frame"); every frame sealed; the last line lost.
    #[test]
    fn only_the_sealed_kinds_get_a_twin_directly_before_them() {
        let t = logweir_core::refusal_detail::LineToken::parse(&"7f".repeat(20))
            .expect("forty hex digits");
        let input = "logweir-check-topic=a\tb\nlogweir-check-end={\"x\":1}\nrefusal-reason=X";
        let mut out = Vec::new();
        {
            let mut w = SealingLines::with_token(
                &mut out,
                &["logweir-check-end=", "refusal-reason="],
                Some(t.clone()),
            );
            for chunk in input.as_bytes().chunks(7) {
                w.write_all(chunk).expect("a Vec takes it");
            }
        }
        let text = String::from_utf8(out).expect("utf-8");
        let want = format!(
            "logweir-check-topic=a\tb\n{}\nlogweir-check-end={{\"x\":1}}\n{}\nrefusal-reason=X",
            logweir_core::runner_line::sealed(&t, "logweir-check-end={\"x\":1}"),
            logweir_core::runner_line::sealed(&t, "refusal-reason=X"),
        );
        assert_eq!(text, want);
        let mut plain = Vec::new();
        {
            let mut w = SealingLines::with_token(&mut plain, &["refusal-reason="], None);
            w.write_all(input.as_bytes()).expect("a Vec takes it");
        }
        assert_eq!(String::from_utf8(plain).expect("utf-8"), input);
    }
}
