/// Global Constraint 11 / spec §6 C5. A CronJob, a change-control pipeline and
/// an alert rule all consume these without parsing stdout.
///
/// `DrillNotPass` and `GuardRefused` are not constructed until Tasks 14, 17 and
/// 21a land, and `ci.yml` runs `clippy -D warnings`, so the allow below is
/// REQUIRED for this task's gate to be green. **Task 21a deletes this line**;
/// if it is still here after Task 21a, a variant is genuinely dead.
#[allow(dead_code)]
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
