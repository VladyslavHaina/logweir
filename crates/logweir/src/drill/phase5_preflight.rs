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

use logweir_core::engine::{CoverageState, PreflightReport};

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
