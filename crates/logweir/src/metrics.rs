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
/// So the metrics file states it explicitly. Since T0-7, EVERY terminal path
/// writes it — exits 1, 3 and 4 through `write_minimal_textfile`, exits 0 and 2
/// through `write_textfile` — so the file's own existence no longer carries any
/// information. Two things replace it:
///
/// * **Staleness is the absence signal.** node_exporter publishes the file's
///   mtime as `node_textfile_mtime_seconds`, which carries no Logweir label at
///   all; `time() - node_textfile_mtime_seconds{file=~".*logweir.*"} > 8d` is
///   therefore the query that catches a CronJob whose pod never started, and it
///   works identically on the paths where the cluster id was never learned.
/// * **Absence of `logweir_drill_runs_total` INSIDE a present file** means the
///   run ended before a scorecard existed. That family, and every other
///   scorecard-derived one, is written only by `write_textfile`.
///
/// See [docs/metrics.md](../../../docs/metrics.md).
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
    // R-11a: a COMMENT, not a label. A ULID is unbounded cardinality — strictly
    // worse than `triggered_by`, which this file's own header already refuses —
    // and node_exporter's textfile collector passes comment lines over. The
    // minimal shape carries the identical line, so an operator reading either
    // file with `cat` sees which run wrote it.
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

    push_exit_code(&mut o, cluster, exit_code_for(&sc.outcome));
    push_last_run_timestamp(&mut o, cluster);

    write_atomically(path, &o)
}

/// The two families BOTH shapes of the file carry, written once so the shapes
/// cannot drift. A metric name must have exactly one `# HELP` text, and two
/// copies of that string is two chances for the minimal file to describe
/// `logweir_drill_exit_code` differently from the full one.
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

/// T0-7 rung 2. The one series that says a drill ran AT ALL, and when.
///
/// The value is read from the clock at emit time on every call — never a
/// constant, never a field carried in from elsewhere — because the whole point
/// is that it moves when a run happens and stops moving when runs stop.
/// (Global Constraint 1: the clock is read in `crates/logweir`, and this is in
/// `crates/logweir`.)
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
/// It is deliberately a SUBSET of `write_textfile`'s output, never a different
/// vocabulary: the same two metric names with the same label, so a dashboard
/// panel does not have to know which path produced the file. Everything derived
/// from a scorecard is absent, because no scorecard exists — and absence of
/// `logweir_drill_runs_total` is itself the signal that no drill result was
/// produced.
///
/// `cluster` is `None` when the run never reached phase 2, which is where the
/// id is learned from the live broker (`phase2_target.rs`). It is then emitted
/// as the literal `unknown`: the spec file does not carry a cluster id and is
/// not parsed here (R-11b — a new failure surface at the exact moment the
/// process is already failing), and the label is never omitted, because a
/// series that sometimes has a label and sometimes does not is a Prometheus
/// modelling error.
pub fn write_minimal_textfile(
    path: &Path,
    cluster: Option<&str>,
    run_id: &str,
    code: crate::exit::ExitCode,
) -> std::io::Result<()> {
    let cluster = cluster.unwrap_or("unknown");
    let mut o = String::new();
    // R-11a: a comment, not a label. See `write_textfile`.
    o.push_str(&format!("# logweir run_id={run_id}\n"));
    push_exit_code(&mut o, cluster, code as u8);
    push_last_run_timestamp(&mut o, cluster);
    write_atomically(path, &o)
}

/// Write-then-rename: node_exporter must never read a half-written file.
///
/// ONE atomic writer for both shapes. Two copies of the rename dance is two
/// chances to lose it.
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
