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
    lines: Vec<String>,
    vouched: Vec<String>,
}

impl FinalLines {
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
        let sealed: Vec<String> = self.lines.iter().chain(&self.vouched).cloned().collect();
        logweir_core::runner_line::final_block_with(
            scoped_token(),
            code as u8,
            &sealed,
            &self.lines,
        )
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
