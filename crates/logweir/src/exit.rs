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
    println!("{}", logweir_core::guard::refusal_reason_line(message));
}
