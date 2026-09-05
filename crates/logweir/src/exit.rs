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
