//! D2 §6.3's check catalogue, as the runner reads it: which rows the check Job
//! owns, how long each answer is good for, and whether it gates.
//!
//! # Why the runner carries a table at all
//!
//! Every [`CheckOutcome`] the runner relays reaches
//! `status.result.checks[]` verbatim, and `expiresAt` is what D2 §6.6 uses to
//! decide a result is no longer applicable. A gating or expiry value written
//! at each construction site is a value that drifts between rows; written
//! here, a row that forgets one fails
//! `every_relayed_row_has_a_catalogue_entry`.
//!
//! # Which rows are the runner's, and which are not
//!
//! D2 §6.3's legend assigns an AUTHORITY to every row: **C** controller,
//! **J** check Job, **P** pod status. [`RUNNER_ROWS`] is exactly the rows
//! whose authority contains **J** and which the runner can answer on its own.
//!
//! Three deliberate exclusions, each with a reason:
//!
//! * **`connection.clusterIdentity` and `target.clusterIdentity` (J+C).** The
//!   runner's half is the OBSERVED cluster id, which it publishes as the
//!   `clusterId` fact on `connection.authenticated` / `target.authenticated`.
//!   The verdict needs `KafkaCluster.status.clusterId` and the
//!   `TrustRoster.allowedClusterIds`, which a check Job is deliberately not
//!   given (it holds credentials, not Kubernetes read). Emitting a row the
//!   controller must then overwrite would put two answers with one id into one
//!   result.
//! * **`signer.rostered`, `approval.*`, `plan.bindings`, `plan.names`,
//!   `recoveryPoint.state`, `configuration.*` (C).** Pure controller
//!   knowledge; the runner has no roster, no `Approval` and no policy.
//! * **Every `credentialProjected`, `runner.image` and `runner.pod` row (P).**
//!   Observed from pod status by `weirkeeper::check::waiting`. A runner that
//!   is running cannot report that its own pod did not start.
//!
//! `runner.contract` IS the runner's: reaching the point where a result is
//! built is the proof that this image understands the contract version the
//! plan names, and D2 §4.3 maps the absence of that proof
//! (`check` is an unknown subcommand on an old image) to
//! `RunnerContractUnsupported` from the exit code instead.

use std::time::Duration;

use logweir_core::check_contract::{
    Authority, CheckCode, CheckId, CheckOutcome, CheckScope, CheckState, Gating,
};

/// Fifteen minutes — D2 §6.3's default expiry.
pub const EXPIRY_DEFAULT: Duration = Duration::from_secs(15 * 60);
/// Ten minutes — `connection.topicsDescribable`.
pub const EXPIRY_TOPICS_DESCRIBABLE: Duration = Duration::from_secs(10 * 60);
/// Thirty minutes — the three `archive.*` rows, which read immutable objects.
pub const EXPIRY_ARCHIVE: Duration = Duration::from_secs(30 * 60);
/// **Five** minutes — `target.mappedTopics` and `target.topicCreate`. The
/// shortest in the catalogue, because a topic can appear on the target between
/// a preview and a run and that is exactly the race PLAT-03.2 names.
pub const EXPIRY_TARGET_COLLISION: Duration = Duration::from_secs(5 * 60);

/// Every row this runner may relay, with the gating and expiry D2 §6.3 gives
/// it.
///
/// The third column is `None` for a row whose answer is a function of bytes
/// rather than of time — D2 §6.3 spells it "until bytes change" — so it
/// contributes no `expiresAt` and cannot pull the aggregate's expiry forward.
pub const RUNNER_ROWS: &[(CheckId, Gating, Option<Duration>)] = &[
    (
        CheckId::ConnectionAuthenticated,
        Gating::Blocking,
        Some(EXPIRY_DEFAULT),
    ),
    (
        CheckId::ConnectionTopicsDescribable,
        Gating::Blocking,
        Some(EXPIRY_TOPICS_DESCRIBABLE),
    ),
    (
        CheckId::ConnectionTopicsReadable,
        Gating::ExecutionOnly,
        None,
    ),
    (
        CheckId::DestinationArchiveListable,
        Gating::Blocking,
        Some(EXPIRY_DEFAULT),
    ),
    (
        CheckId::DestinationEvidenceWritable,
        Gating::Blocking,
        Some(EXPIRY_DEFAULT),
    ),
    (
        CheckId::DestinationArchivePrefixWritable,
        Gating::ExecutionOnly,
        None,
    ),
    (
        CheckId::DestinationEvidenceReadable,
        Gating::Advisory,
        Some(EXPIRY_DEFAULT),
    ),
    (
        CheckId::SignerPrivateKeyUsable,
        Gating::Blocking,
        Some(EXPIRY_DEFAULT),
    ),
    (
        CheckId::RunnerContract,
        Gating::Blocking,
        Some(EXPIRY_DEFAULT),
    ),
    (CheckId::PlanParse, Gating::Blocking, None),
    (
        CheckId::ArchiveBackupSet,
        Gating::Blocking,
        Some(EXPIRY_ARCHIVE),
    ),
    (
        CheckId::ArchiveCoverage,
        Gating::Blocking,
        Some(EXPIRY_ARCHIVE),
    ),
    (
        CheckId::ArchiveSegments,
        Gating::Blocking,
        Some(EXPIRY_ARCHIVE),
    ),
    (
        CheckId::TargetAuthenticated,
        Gating::Blocking,
        Some(EXPIRY_DEFAULT),
    ),
    (
        CheckId::TargetScratchMarker,
        Gating::Blocking,
        Some(EXPIRY_DEFAULT),
    ),
    (
        CheckId::TargetMappedTopics,
        Gating::Blocking,
        Some(EXPIRY_TARGET_COLLISION),
    ),
    (
        CheckId::TargetTopicCreate,
        Gating::Blocking,
        Some(EXPIRY_TARGET_COLLISION),
    ),
    (
        CheckId::TargetTimestampBound,
        Gating::Blocking,
        Some(EXPIRY_DEFAULT),
    ),
    (CheckId::TargetLogAppendTime, Gating::ExecutionOnly, None),
];

/// This row's catalogue entry, or `None` for a row the runner does not own.
#[must_use]
pub fn entry(id: CheckId) -> Option<(Gating, Option<Duration>)> {
    RUNNER_ROWS
        .iter()
        .find(|(rid, _, _)| *rid == id)
        .map(|(_, g, e)| (*g, *e))
}

/// A [`CheckOutcome`] for a row the runner owns, with the catalogue's gating
/// and expiry already applied.
///
/// **This is the runner's ONE construction site for an outcome.** Going
/// through it is what makes `expiresAt` a property of the row rather than of
/// the call, and what makes `every_relayed_row_has_a_catalogue_entry` a real
/// guard: a row with no entry panics here in tests and is refused in
/// production by being absent from [`RUNNER_ROWS`] in the first place.
///
/// # Panics
/// Never in practice: the panic fires only for a [`CheckId`] absent from
/// [`RUNNER_ROWS`], which is a programming error this crate's own tests catch.
#[must_use]
pub fn outcome(
    id: CheckId,
    state: CheckState,
    code: CheckCode,
    now: chrono::DateTime<chrono::Utc>,
) -> CheckOutcome {
    let (gating, expiry) =
        entry(id).unwrap_or_else(|| panic!("{id} is not a row the check runner owns (D2 §6.3)"));
    let mut out = CheckOutcome::new(id, state, gating, Authority::CheckJob, code);
    out.observed_at = Some(now);
    out.expires_at = expiry.and_then(|d| {
        chrono::Duration::from_std(d)
            .ok()
            .and_then(|d| now.checked_add_signed(d))
    });
    out
}

/// The same, with the gating overridden.
///
/// ONE caller: `destination.evidenceWritable`, which D2 §6.3 makes
/// "**B** if `writeProbe: CreateOnlyMarker`, else **E**". It is the only row
/// in the catalogue whose gating is a function of the plan, and spelling it as
/// an override keeps the table static for every other row.
#[must_use]
pub fn outcome_gated(
    id: CheckId,
    state: CheckState,
    code: CheckCode,
    gating: Gating,
    now: chrono::DateTime<chrono::Utc>,
) -> CheckOutcome {
    let mut out = outcome(id, state, code, now);
    out.gating = gating;
    if gating == Gating::ExecutionOnly {
        // `CheckOutcome::new` forces an execution-only row to `unknown`; an
        // override applied afterwards has to do the same or the row would
        // claim a verdict it is not allowed to have.
        out.state = CheckState::Unknown;
    }
    out
}

/// A scope naming the object a row is about.
#[must_use]
pub fn scope(kind: &str, name: &str, uid: Option<&str>) -> CheckScope {
    CheckScope {
        kind: kind.to_string(),
        name: name.to_string(),
        uid: uid.map(ToString::to_string),
    }
}
