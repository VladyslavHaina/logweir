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
/// disagree. It delegates to `Outcome::wire_name`, which sits beside the
/// `#[serde(rename_all)]` attribute that decides the JSON.
pub fn outcome_str(o: &Outcome) -> &'static str {
    o.wire_name()
}

/// The exit code this scorecard's `outcome` produces, as a number a dashboard
/// can alert on.
///
/// A Job's exit code appears ONLY in
/// `pod.status.containerStatuses[].state.terminated.exitCode`: it is absent
/// from Job status, and `kubectl get pods` renders both `1` and `2` as "Error".
/// Without this metric, "could not do its job" (1, no artifact) and "a drill
/// ran and did not pass" (2, a signed artifact) look nearly the same.
///
/// Every terminal path writes the file, so its existence says nothing.
/// Staleness (`node_textfile_mtime_seconds`) is the absence signal, and
/// `logweir_drill_runs_total` missing from a present file means the run ended
/// before a scorecard existed. See [docs/metrics.md](../../../docs/metrics.md).
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
    // A COMMENT, not a label: a ULID is unbounded cardinality, and
    // node_exporter's textfile collector passes comment lines over. The
    // minimal shape carries the identical line.
    o.push_str(&format!("# logweir run_id={}\n", sc.run_id));
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

    // Beside `fingerprint_mismatches`: both say what went wrong that the
    // outcome alone does not. UNCONDITIONAL, `0` included, as
    // `logweir_drill_redactions` is: to PromQL an absent series and a clean
    // teardown look the same, and `> 0` cannot alert on an absent series. A
    // COUNT, not a topic label: topic names are cluster-controlled text and
    // unbounded cardinality. The names are on the WARN line, the summary line
    // and the signed teardown attestation.
    o.push_str(
        "# HELP logweir_drill_teardown_topics_failed Scratch topics phase 9 could not delete.\n\
         # TYPE logweir_drill_teardown_topics_failed gauge\n",
    );
    o.push_str(&format!(
        "logweir_drill_teardown_topics_failed{{cluster=\"{cluster}\"}} {}\n",
        crate::drill::phase9_teardown::failed_count(sc)
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

    // HOW MUCH was verified, beside how it came out. One series
    // per document carrying `integrity.verification` (format 1.4.0); none for
    // an older one, which is "not recorded" and never read as complete. A
    // complete verification adds whether it COVERED every partition: 0 is a
    // run whose outcome is never `pass` (arm IV-6), and alerting on
    // `logweir_drill_integrity_complete_covered == 0` names that cause
    // where `logweir_drill_runs_total{outcome="fail-integrity"}` cannot.
    if let Some(verification) = sc.integrity.verification.as_ref() {
        let coverage = match verification.coverage.as_str() {
            logweir_core::scorecard::COVERAGE_COMPLETE => "complete",
            _ => "sampled",
        };
        o.push_str(
            "# HELP logweir_drill_integrity_coverage 1 for the coverage the scorecard says it \
             verified: sampled or complete.\n\
             # TYPE logweir_drill_integrity_coverage gauge\n",
        );
        o.push_str(&format!(
            "logweir_drill_integrity_coverage{{cluster=\"{cluster}\",coverage=\"{coverage}\"}} 1\n"
        ));
        if let Some(complete) = verification.complete.as_ref() {
            o.push_str(
                "# HELP logweir_drill_integrity_complete_covered 1 when a complete verification \
                 covered every restored partition, 0 when it did not (never a pass).\n\
                 # TYPE logweir_drill_integrity_complete_covered gauge\n",
            );
            o.push_str(&format!(
                "logweir_drill_integrity_complete_covered{{cluster=\"{cluster}\"}} {}\n",
                u8::from(complete.covered)
            ));
        }
    }

    o.push_str(
        "# HELP logweir_evidence_lock_verified 1 only after a provider readback.\n\
         # TYPE logweir_evidence_lock_verified gauge\n",
    );
    o.push_str(&format!(
        "logweir_evidence_lock_verified{{cluster=\"{cluster}\"}} {}\n",
        sc.evidence.immutable as u8
    ));

    // UNCONDITIONAL, including the `0` every v0.1 run writes: a series that
    // appears only when non-zero cannot be alerted on with
    // `logweir_drill_redactions > 0`, because an absent series and a whole
    // document look identical to PromQL. A COUNT, not a path label:
    // `redactions[].path` is document-controlled text and unbounded
    // cardinality; `drill show`'s qualifiers footer renders the paths.
    o.push_str(
        "# HELP logweir_drill_redactions Scorecard fields removed before signing; 0 for a \
         whole document. Non-zero means BOTH verifiers refuse this document.\n\
         # TYPE logweir_drill_redactions gauge\n",
    );
    o.push_str(&format!(
        "logweir_drill_redactions{{cluster=\"{cluster}\"}} {}\n",
        sc.redactions.len()
    ));

    push_exit_code(&mut o, cluster, exit_code_for(&sc.outcome));
    push_last_run_timestamp(&mut o, cluster);

    write_atomically(path, &o)
}

/// The two families BOTH shapes of the file carry, written once so the shapes
/// cannot drift: a metric name must have exactly one `# HELP` text.
fn push_exit_code(o: &mut String, cluster: &str, code: u8) {
    o.push_str(
        "# HELP logweir_drill_exit_code The process exit code this run produced \
         (0 pass, 1 operational with no artifact, 2 a signed drill result that is not a \
         pass, 3 refused by a guard, 4 signing or lock proof failed).\n\
         # TYPE logweir_drill_exit_code gauge\n",
    );
    o.push_str(&format!(
        "logweir_drill_exit_code{{cluster=\"{cluster}\"}} {code}\n"
    ));
}

/// The one series that says a drill ran AT ALL, and when. The clock is read at
/// every call (Global Constraint 1 allows it in `crates/logweir`), because the
/// value must move when a run happens and stop moving when runs stop.
fn push_last_run_timestamp(o: &mut String, cluster: &str) {
    o.push_str(
        "# HELP logweir_drill_last_run_timestamp_seconds Unix seconds at which this drill run \
         finished and wrote this file.\n\
         # TYPE logweir_drill_last_run_timestamp_seconds gauge\n",
    );
    o.push_str(&format!(
        "logweir_drill_last_run_timestamp_seconds{{cluster=\"{cluster}\"}} {}\n",
        chrono::Utc::now().timestamp()
    ));
}

/// The textfile a terminal path with NO scorecard leaves behind (exits 1, 3, 4).
///
/// A SUBSET of `write_textfile`'s output, never a different vocabulary, so a
/// panel need not know which path produced the file. Absent
/// `logweir_drill_runs_total` is itself the signal that no drill result exists.
///
/// `cluster` is `None` before phase 2 learns the id from the live broker, and
/// is then emitted as `unknown`: the spec file is not parsed here (a new
/// failure surface at the moment the process is already failing), and the
/// label is never omitted, because a series whose label comes and goes is a
/// Prometheus modelling error.
pub fn write_minimal_textfile(
    path: &Path,
    cluster: Option<&str>,
    run_id: &str,
    code: crate::exit::ExitCode,
) -> std::io::Result<()> {
    let cluster = cluster.unwrap_or("unknown");
    let mut o = String::new();
    // A comment, not a label. See `write_textfile`.
    o.push_str(&format!("# logweir run_id={run_id}\n"));
    push_exit_code(&mut o, cluster, code as u8);
    push_last_run_timestamp(&mut o, cluster);
    write_atomically(path, &o)
}

/// Write-then-rename: node_exporter must never read a half-written file.
fn write_atomically(path: &Path, body: &str) -> std::io::Result<()> {
    let tmp = path.with_extension("prom.tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(body.as_bytes())?;
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
