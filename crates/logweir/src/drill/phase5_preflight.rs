//! Phase 5 — Logweir adjudicates the engine's preflight report locally.
//!
//! CONTRACT FOR THE CALLER (implemented by the orchestrator in
//! `crate::drill::run`, Task 21a step 4b — deliberately NOT implemented here,
//! because that function does not exist yet at this point in the task order):
//! on `Verdict::Block` the orchestrator sets `sc.outcome =
//! Outcome::PreflightFailed`, `sc.integrity.level = IntegrityLevel::NotAttempted`,
//! `sc.integrity.result = IntegrityResult::Fail`, `sc.measured.rto_seconds =
//! None`, records every `Finding` into the `notes` of the phase record whose
//! `phase == 5`, leaves `last_phase_completed` at 5, then jumps straight to
//! phase 8 (score + sign + upload) and returns `ExitCode::DrillNotPass` (2).
//! It never reaches phase 6. Exit 1 here would produce no artifact, and this
//! signed document is the NIS2 IR 4.2.3 evidence (spec §11, Global Constraint 11).
//!
//! `PhaseRecord.notes: Vec<String>` (`logweir_core::scorecard::PhaseRecord`)
//! is what the paragraph above writes `Finding`s into. It did not exist when
//! this module was first shipped — Task 17 fix round 1 added it, reversing
//! `task-3-addendum.md` A2 on controller authority, specifically so this
//! contract would be true rather than aspirational. If you are reading this
//! and the field is gone again, this contract is broken; do not re-route
//! findings into `phases[5].outcome` without updating this comment.
//!
//! On `Verdict::Block` the `warnings` vector is DISCARDED, not carried
//! anywhere — `adjudicate` returns `Block { findings }` only, so any
//! header-coverage advisories collected before a blocking finding fired never
//! reach the caller, and therefore never reach the signed scorecard. This is
//! as specified (the brief's Step 3 body, verbatim) and is not a bug: it
//! means a blocking run's `notes` never mention `Partial`/`Missing` header
//! coverage, only the grounds that actually blocked it. A caller wanting
//! warnings preserved on a blocking run would need `adjudicate`'s signature
//! changed; Task 17 does not do that.

use crate::drill::DrillError;
use logweir_core::engine::{BackupSetFacts, CoverageState, PreflightReport, RestorePlan};
use logweir_core::guard::GuardRefusal;

/// The exact prefix of the rendered `restore.yaml` line this guard reads. Two
/// spaces, because `time_window_start` is a key of the `restore:` block
/// (`logweir_engine_oso::render_restore::render`); the golden carries the same
/// indentation, and indentation is part of the contract (plan errata E2/E3).
const RENDERED_WINDOW_START_PREFIX: &str = "  time_window_start: ";

/// **GUARD G-WIN, the refusing half.** Refuses, exit 3, when the RENDERED
/// `time_window_start` is not the archive set's earliest covered timestamp.
///
/// # Why it reads the rendered bytes and not `plan.time_window.0`
///
/// Spec §10's G-WIN row says *the rendered* `time_window_start`. A comparison
/// against the plan field cannot see a printer that ignores the plan:
/// `render_restore::render` is where the integer is actually produced, and a
/// mutant there is invisible to any assertion made about the plan struct. So
/// this function renders the document FIRST, parses the integer off its
/// `  time_window_start: ` line, and compares THAT against a floor it
/// re-derives from the manifest it already holds.
///
/// # Why exit 3, and not ruling R-E's exit 1
///
/// Ruling R-E makes the phase-5/phase-6 render mismatch exit **1**,
/// operational, no artifact — by that point the guards have run and
/// `validate-restore` has already executed. This check is a different animal:
/// it runs **before** the document is written and before the engine is
/// invoked at all, so "refused by a guard, before anything runs" (Global
/// Constraint 11, `crate::exit`) is exactly what describes it. It therefore
/// returns `DrillError::Guard`, which `DrillError::exit_code` maps to
/// `ExitCode::GuardRefused` (3).
pub fn check_rendered_window_floor(
    plan: &RestorePlan,
    facts: &BackupSetFacts,
) -> Result<(), DrillError> {
    // RE-DERIVED from the manifest, never read off the plan: a floor taken
    // from `plan.time_window.0` would make this check compare the plan with
    // itself.
    let floor = facts.earliest_covered_timestamp_ms().ok_or_else(|| {
        DrillError::Guard(GuardRefusal(format!(
            "the archive set `{}` records no segment in its manifest, so the rendered \
             time_window_start cannot be checked against an archive floor",
            plan.set.backup_id
        )))
    })?;
    let doc = logweir_engine_oso::render_restore::render(plan)
        .map_err(|e| DrillError::Operational(format!("rendering restore.yaml: {e}")))?;
    let rendered = doc
        .lines()
        .find_map(|l| l.strip_prefix(RENDERED_WINDOW_START_PREFIX))
        .ok_or_else(|| {
            DrillError::Operational(format!(
                "the rendered restore.yaml carries no `{}` line, so the archive floor cannot \
                 be checked against it",
                RENDERED_WINDOW_START_PREFIX.trim_end()
            ))
        })?
        .trim()
        .parse::<i64>()
        .map_err(|e| {
            DrillError::Operational(format!(
                "the rendered restore.yaml's time_window_start is not an integer: {e}"
            ))
        })?;
    if rendered != floor {
        return Err(DrillError::Guard(GuardRefusal(format!(
            "rendered time_window_start {rendered} is not the archive floor {floor}; a \
             Restore's window start is the archive set's earliest covered timestamp, never \
             the spec's"
        ))));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct Finding {
    pub topic: String,
    pub partition: i32,
    pub state: String,
    pub detail: String,
}

#[derive(Debug)]
pub enum Verdict {
    Proceed { warnings: Vec<Finding> },
    Block { findings: Vec<Finding> },
}

/// LOGWEIR APPLIES ITS OWN BLOCKING POLICY — the engine will not do it for us.
/// `header_preflight: full` produces WARNINGS when offset recovery is not
/// requested (config.rs:934-938), and offset recovery is something §2's
/// non-goals forbid Logweir from ever requesting.
pub fn adjudicate(r: &PreflightReport) -> Verdict {
    let mut findings = Vec::new();
    let mut warnings = Vec::new();

    // Readback (1) of spec §9.3 phase 5: an engine that ignored the key leaves
    // report.header_preflight None, because it defaults to Auto and
    // scan_required is false when offset recovery is not requested.
    if !r.header_preflight_honoured {
        findings.push(Finding {
            topic: "*".into(),
            partition: -1,
            state: "lever-ignored".into(),
            detail: "engine did not honour restore.header_preflight=full \
                     (header_preflight absent, or scan_performed false, or mode != full); \
                     this engine tag is below the declared floor"
                .into(),
        });
    }

    for p in &r.partitions {
        let mk = |state: &str, detail: String| Finding {
            topic: p.topic.clone(),
            partition: p.partition,
            state: state.into(),
            detail,
        };
        match &p.state {
            CoverageState::Full => {}
            // Header coverage only, and v0.1 never requests offset recovery.
            CoverageState::Partial | CoverageState::Missing => warnings.push(mk(
                "header-coverage",
                format!(
                    "tracking-header coverage is {:?}; advisory in v0.1 because the drill \
                     never requests consumer-offset recovery",
                    p.state
                ),
            )),
            // "The manifest references segment objects that are absent from storage."
            CoverageState::DataMissing => findings.push(mk(
                "data_missing",
                format!("segments absent from storage: {}", p.detail),
            )),
            // "A segment exists but could not be decoded."
            CoverageState::Corrupt => findings.push(mk(
                "corrupt",
                format!("segment could not be decoded: {}", p.detail),
            )),
            // Upstream: "Explicitly not a positive pass."
            CoverageState::Empty => findings.push(mk(
                "empty",
                "no records in the selected window for this partition".into(),
            )),
            // Upstream: "Never a positive pass."
            CoverageState::Indeterminate => findings.push(mk(
                "indeterminate",
                format!("coverage undetermined: {}", p.detail),
            )),
            CoverageState::Unknown(s) => findings.push(mk(
                "unknown",
                format!("engine reported an unrecognised coverage state `{s}`"),
            )),
        }
    }

    // dry_run_check_segments widens storage.exists() from the oldest segment
    // per partition to every selected segment (restore/engine.rs:568-572); a
    // genuinely missing NON-OLDEST segment shows up here and nowhere else.
    for e in &r.errors {
        findings.push(Finding {
            topic: "*".into(),
            partition: -1,
            state: "engine-error".into(),
            detail: e.clone(),
        });
    }
    if !r.valid && findings.is_empty() {
        findings.push(Finding {
            topic: "*".into(),
            partition: -1,
            state: "engine-invalid".into(),
            detail: "engine reported valid: false with no error detail".into(),
        });
    }

    if findings.is_empty() {
        Verdict::Proceed { warnings }
    } else {
        Verdict::Block { findings }
    }
}
