//! Spec §13. v0.1 has no HTTP surface, so the CLI writes a Prometheus
//! textfile-collector file and node_exporter serves it. `triggered_by` is
//! deliberately NOT a label — unbounded cardinality — and lives in the
//! scorecard instead.
use logweir_core::outcome::{IntegrityLevel, IntegrityResult, Outcome};
use logweir_core::scorecard::Scorecard;
use std::io::Write;
use std::path::Path;

/// One mapping from `Outcome` to its wire string, shared by the metric label,
/// the `drill run` summary line and `drill show`'s table so the three can never
/// disagree.
///
/// It is a one-line delegation to `Outcome::wire_name` and not a second match,
/// which is the fix: this function's own comment used to claim the mapping was
/// "shared ... so the two can never disagree" while `crate::show` rendered a
/// THIRD spelling from Rust `Debug` (`Pass`, `ByteFingerprint/Pass`,
/// `Honoured`). The spelling now lives beside the enum, next to the
/// `#[serde(rename_all)]` attribute that decides the JSON, and is pinned
/// against it variant by variant.
pub fn outcome_str(o: &Outcome) -> &'static str {
    o.wire_name()
}

/// The exit code this scorecard's `outcome` produces, as a number a dashboard
/// can alert on.
///
/// This exists because of a Kubernetes fact verified on a live cluster: a
/// Job's exit code appears ONLY in
/// `pod.status.containerStatuses[].state.terminated.exitCode`; it is absent
/// from Job status, and `kubectl get pods` renders both `1` and `2` as a
/// generic "Error". At the delivery layer the distinction between "Logweir
/// could not do its job" (1, no artifact) and "a drill ran and did not pass"
/// (2, a SIGNED artifact exists) is therefore nearly invisible to an
/// operator — and the second is the most valuable result this product
/// produces.
///
/// So the metrics file states it explicitly. Note what its ABSENCE means as
/// well: this file is written only from `crate::drill::finish`, which runs on
/// the exit-0 and exit-2 paths only. An operational failure (1), a guard
/// refusal (3) and a signing failure (4) leave no metrics file at all, so a
/// stale-or-missing `logweir_drill_exit_code` is itself the signal that no
/// drill result was produced.
fn exit_code_for(o: &Outcome) -> u8 {
    match o {
        Outcome::Pass => crate::exit::ExitCode::Ok as u8,
        // Every non-pass outcome that reaches a WRITTEN scorecard is exit 2:
        // the drill ran, the result is signed, and it is not a pass.
        Outcome::FailObjective | Outcome::FailIntegrity | Outcome::PreflightFailed => {
            crate::exit::ExitCode::DrillNotPass as u8
        }
    }
}

pub fn write_textfile(path: &Path, sc: &Scorecard) -> std::io::Result<()> {
    let mut o = String::new();
    let cluster = &sc.target.cluster_id;
    let outcome = outcome_str(&sc.outcome);
    o.push_str("# HELP logweir_drill_runs_total Drill runs by outcome.\n");
    o.push_str("# TYPE logweir_drill_runs_total counter\n");
    o.push_str(&format!(
        "logweir_drill_runs_total{{cluster=\"{cluster}\",outcome=\"{outcome}\"}} 1\n"
    ));

    o.push_str(
        "# HELP logweir_drill_rto_seconds Measured RTO excluding phase 5.\n\
         # TYPE logweir_drill_rto_seconds gauge\n",
    );
    if let Some(v) = sc.measured.rto_excluding_preflight_seconds {
        o.push_str(&format!(
            "logweir_drill_rto_seconds{{cluster=\"{cluster}\"}} {v}\n"
        ));
    }
    o.push_str(
        "# HELP logweir_drill_rpo_seconds Archive coverage gap at the requested point.\n\
         # TYPE logweir_drill_rpo_seconds gauge\n",
    );
    if let Some(v) = sc.measured.rpo_seconds {
        o.push_str(&format!(
            "logweir_drill_rpo_seconds{{cluster=\"{cluster}\"}} {v}\n"
        ));
    }
    o.push_str(
        "# HELP logweir_drill_objective_met 1 met, 0 missed; absent when unmeasurable.\n\
         # TYPE logweir_drill_objective_met gauge\n",
    );
    for (name, ok) in [
        (
            "rto",
            sc.objectives
                .rto_seconds
                .zip(sc.measured.rto_excluding_preflight_seconds)
                .map(|(w, g)| g <= w),
        ),
        (
            "rpo",
            sc.objectives
                .rpo_seconds
                .zip(sc.measured.rpo_seconds)
                .map(|(w, g)| g <= w),
        ),
        (
            "pass_rate",
            sc.objectives
                .pass_rate
                .zip(sc.integrity.pass_rate_measured)
                .map(|(w, g)| g + f64::EPSILON >= w),
        ),
    ] {
        if let Some(b) = ok {
            o.push_str(&format!(
                "logweir_drill_objective_met{{cluster=\"{cluster}\",objective=\"{name}\"}} {}\n",
                b as u8
            ));
        }
    }
    o.push_str(
        "# HELP logweir_drill_fingerprint_mismatches Records that did not reconcile.\n\
         # TYPE logweir_drill_fingerprint_mismatches gauge\n",
    );
    o.push_str(&format!(
        "logweir_drill_fingerprint_mismatches{{cluster=\"{cluster}\"}} {}\n",
        sc.integrity.mismatches
    ));

    let level = match sc.integrity.level {
        IntegrityLevel::ByteFingerprint => "byte-fingerprint",
        IntegrityLevel::ConsumeOnly => "consume-only",
        IntegrityLevel::NotAttempted => "not-attempted",
    };
    o.push_str(
        "# HELP logweir_drill_integrity_level 1 for the level achieved.\n\
         # TYPE logweir_drill_integrity_level gauge\n",
    );
    o.push_str(&format!(
        "logweir_drill_integrity_level{{cluster=\"{cluster}\",level=\"{level}\"}} 1\n"
    ));

    let result = match sc.integrity.result {
        IntegrityResult::Pass => "pass",
        IntegrityResult::Partial => "partial",
        IntegrityResult::Fail => "fail",
    };
    o.push_str(
        "# HELP logweir_drill_integrity_result 1 for the result recorded.\n\
         # TYPE logweir_drill_integrity_result gauge\n",
    );
    o.push_str(&format!(
        "logweir_drill_integrity_result{{cluster=\"{cluster}\",result=\"{result}\"}} 1\n"
    ));

    o.push_str(
        "# HELP logweir_evidence_lock_verified 1 only after a provider readback.\n\
         # TYPE logweir_evidence_lock_verified gauge\n",
    );
    o.push_str(&format!(
        "logweir_evidence_lock_verified{{cluster=\"{cluster}\"}} {}\n",
        sc.evidence.immutable as u8
    ));

    // T0-3, the display half. Nothing here carried a redaction signal, so a
    // dashboard showed a clean drill over a document that says a field was
    // removed. Emitted UNCONDITIONALLY, including the `0` every v0.1 run
    // writes: a series that only appears when it is non-zero cannot be alerted
    // on with `logweir_drill_redactions > 0`, because an absent series and a
    // whole document look identical to PromQL. `logweir_drill_fingerprint_mismatches`
    // above is unconditional for the same reason.
    //
    // A COUNT, not a path label: `redactions[].path` is document-controlled
    // text and would be unbounded label cardinality — the same rule that keeps
    // `triggered_by` out of this file. The paths are rendered by
    // `drill show`'s qualifiers footer and carried in the notification body.
    o.push_str(
        "# HELP logweir_drill_redactions Scorecard fields removed before signing; 0 for a \
         whole document. Non-zero means BOTH verifiers refuse this document.\n\
         # TYPE logweir_drill_redactions gauge\n",
    );
    o.push_str(&format!(
        "logweir_drill_redactions{{cluster=\"{cluster}\"}} {}\n",
        sc.redactions.len()
    ));

    o.push_str(
        "# HELP logweir_drill_exit_code The process exit code this result produced \
         (0 pass, 2 a signed drill result that is not a pass).\n\
         # TYPE logweir_drill_exit_code gauge\n",
    );
    o.push_str(&format!(
        "logweir_drill_exit_code{{cluster=\"{cluster}\"}} {}\n",
        exit_code_for(&sc.outcome)
    ));

    // Write-then-rename: node_exporter must never read a half-written file.
    let tmp = path.with_extension("prom.tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(o.as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(tmp, path)
}

use logweir_core::engine::PhaseObserver;

/// The observer the orchestrator hands to phase 6. v0.1 has no HTTP surface, so
/// "observing" is structured logging: spec §13 requires the run_id on every
/// line, and the engine's stdout/stderr lines reach the log through here rather
/// than through a phase module.
pub struct PhaseLogger {
    run_id: String,
}

impl PhaseLogger {
    pub fn new(run_id: &str) -> Self {
        Self {
            run_id: run_id.to_string(),
        }
    }
}

impl PhaseObserver for PhaseLogger {
    fn phase_started(&mut self, phase: i8, name: &str) {
        tracing::info!(run_id = %self.run_id, phase = phase as i64, name, "phase started");
    }
    fn phase_finished(&mut self, phase: i8, outcome: &str) {
        tracing::info!(run_id = %self.run_id, phase = phase as i64, outcome, "phase finished");
    }
    fn engine_line(&mut self, stream: &str, line: &str) {
        tracing::info!(run_id = %self.run_id, stream, line, "engine output");
    }
}
