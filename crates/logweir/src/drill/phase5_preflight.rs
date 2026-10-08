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
use std::collections::BTreeSet;

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
    check_rendered_selection(plan, facts, &StatedSelection::default())
}

/// What the APPROVED SPEC states about the replay selection (PROD-11.1), as
/// phase 5 reads it: never from the plan, whose claim it checks.
#[derive(Debug, Clone, Default)]
pub struct StatedSelection {
    /// `restore.window_start`, epoch milliseconds.
    pub window_start_ms: Option<i64>,
    /// `restore.partitions`, as stated.
    pub partitions: std::collections::BTreeMap<String, Vec<i32>>,
}

impl StatedSelection {
    /// Read off the spec the run was approved with.
    #[must_use]
    pub fn of(spec: &logweir_core::spec::DrillSpec) -> Self {
        Self {
            window_start_ms: spec.restore.window_start.map(|t| t.timestamp_millis()),
            partitions: spec.restore.partitions.clone(),
        }
    }
}

/// **GUARD G-WIN, the refusing half, as amended by PROD-11.1**
/// (`docs/to-do/decisions/PROD-11.1-replay-selection.md` §2).
///
/// For every engine run the plan renders, it reads the RENDERED document and
/// refuses, exit 3, unless:
///
/// 1. its `time_window_start` is the instant re-derived from the SPEC and the
///    manifest: the stated `restore.window_start` when there is one — refused
///    outright when it is earlier than the archive's floor — else the floor;
/// 2. its `source_partitions` is the spec's subset of every topic it names
///    (absent exactly when those topics have none), and its
///    `target.topics.include` is the topics the spec's grouping puts in that
///    run (`ReplaySelection::engine_runs`, the shared function) — so a plan
///    that dropped, merged or swapped a subset is refused, not run;
/// 3. there are exactly as many documents as the spec's grouping has runs.
///
/// The plan's own claims (`window_floor_source`, `source_partitions`) are
/// never read as the expected value: a guard that read the claim could be
/// talked out of refusing by the very plan it is refusing.
pub fn check_rendered_selection(
    plan: &RestorePlan,
    facts: &BackupSetFacts,
    stated: &StatedSelection,
) -> Result<(), DrillError> {
    // RE-DERIVED from the manifest, never read off the plan: a floor taken
    // from `plan.time_window.0` would make this check compare the plan with
    // itself.
    //
    // Over the topics THIS RESTORE NAMES (plan erratum E7(b)) — the keys of
    // the plan's own topic mapping, which is the same set `build_plan` reduced
    // the manifest by, so the independent reading is of the same fact and not
    // of a wider one. `plan.topic_mapping` and not the spec: the plan is what
    // `plan_hash` covers and what the engine is about to be handed.
    let named_topics: BTreeSet<&str> = plan.topic_mapping.keys().map(String::as_str).collect();
    let floor = facts
        .earliest_covered_timestamp_ms(&named_topics)
        .ok_or_else(|| {
            DrillError::Guard(GuardRefusal(format!(
                "the archive set `{}` records no segment in its manifest for any of the topics \
                 this restore names, so the rendered time_window_start cannot be checked \
                 against an archive floor",
                plan.set.backup_id
            )))
        })?;
    // PROD-11.1: the stated start, from the SPEC. Never earlier than the
    // floor: refused here as at plan construction, whatever the plan says.
    if let Some(start_ms) = stated.window_start_ms {
        if start_ms < floor {
            return Err(DrillError::Guard(GuardRefusal(
                logweir_core::replay_selection::SelectionRefusal::StartBeforeCoverage {
                    start_ms,
                    floor_ms: floor,
                }
                .to_string(),
            )));
        }
    }
    // The runs the SPEC's subsets need over the plan's mapped topics, by the
    // shared grouping function.
    let stated_subsets: std::collections::BTreeMap<String, BTreeSet<i32>> = stated
        .partitions
        .iter()
        .filter(|(t, _)| plan.topic_mapping.contains_key(*t))
        .map(|(t, ps)| (t.clone(), ps.iter().copied().collect()))
        .collect();
    let mut expected_runs = logweir_core::replay_selection::ReplaySelection::engine_runs(
        &stated_subsets,
        plan.topic_mapping.keys(),
    );
    if expected_runs.is_empty() {
        expected_runs.push(logweir_core::replay_selection::EngineRunSelection {
            source_partitions: None,
            topics: Vec::new(),
        });
    }
    let docs = logweir_engine_oso::render_restore::render_all(plan)
        .map_err(|e| DrillError::Operational(format!("rendering restore.yaml: {e}")))?;
    if docs.len() != expected_runs.len() {
        return Err(DrillError::Guard(GuardRefusal(format!(
            "the plan renders {} engine run(s) where the approved restore.partitions need {}; a \
             run-wide partition filter applied to the wrong topics would restore partitions \
             nobody approved",
            docs.len(),
            expected_runs.len()
        ))));
    }
    for ((_, doc), expected) in docs.iter().zip(expected_runs.iter()) {
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
        match stated.window_start_ms {
            None if rendered != floor => {
                return Err(DrillError::Guard(GuardRefusal(format!(
                    "rendered time_window_start {rendered} is not the archive floor {floor}; a \
                     Restore's window start is the archive set's earliest covered timestamp, \
                     never the spec's"
                ))));
            }
            Some(start_ms) if rendered != start_ms => {
                return Err(DrillError::Guard(GuardRefusal(format!(
                    "rendered time_window_start {rendered} is not the approved \
                     restore.window_start {start_ms} (the archive floor is {floor}); a Restore's \
                     window starts at the archive's floor or at the start its approved plan \
                     states, and nowhere else"
                ))));
            }
            _ => {}
        }
        let (include, partitions) = rendered_run_selection(doc)?;
        if include != expected.topics || partitions != expected.source_partitions {
            return Err(DrillError::Guard(GuardRefusal(format!(
                "a rendered engine run restores topics {include:?} with source_partitions \
                 {partitions:?}, where the approved restore.partitions put topics {:?} in a run \
                 with source_partitions {:?}; refused before the engine is handed a partition \
                 selection nobody approved",
                expected.topics, expected.source_partitions
            ))));
        }
    }
    Ok(())
}

/// The `target.topics.include` list and the `restore.source_partitions`
/// filter of one RENDERED restore document, read back off its bytes.
fn rendered_run_selection(doc: &str) -> Result<(Vec<String>, Option<Vec<i32>>), DrillError> {
    let unreadable = |why: String| {
        DrillError::Operational(format!(
            "the rendered restore.yaml cannot be read back to check its partition selection: \
             {why}"
        ))
    };
    let v: serde_yaml::Value = serde_yaml::from_str(doc).map_err(|e| unreadable(e.to_string()))?;
    let mut include: Vec<String> = v
        .get("target")
        .and_then(|t| t.get("topics"))
        .and_then(|t| t.get("include"))
        .and_then(serde_yaml::Value::as_sequence)
        .map(|seq| {
            seq.iter()
                .map(|e| match e {
                    serde_yaml::Value::String(s) => s.clone(),
                    other => serde_yaml::to_string(other)
                        .unwrap_or_default()
                        .trim()
                        .to_string(),
                })
                .collect()
        })
        .unwrap_or_default();
    include.sort();
    let partitions = match v.get("restore").and_then(|r| r.get("source_partitions")) {
        None => None,
        Some(seq) => Some(
            seq.as_sequence()
                .ok_or_else(|| unreadable("source_partitions is not a list".into()))?
                .iter()
                .map(|p| {
                    p.as_i64()
                        .and_then(|n| i32::try_from(n).ok())
                        .ok_or_else(|| unreadable(format!("partition {p:?} is not an i32")))
                })
                .collect::<Result<Vec<i32>, DrillError>>()?,
        ),
    };
    Ok((include, partitions))
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
