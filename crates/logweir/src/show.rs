//! `logweir drill show` — the fixed-width scorecard table (spec §13) and the
//! raw `--format json` passthrough.
use logweir_core::scorecard::Scorecard;

fn row(out: &mut String, label: &str, value: impl std::fmt::Display) {
    out.push_str(&format!("  {label:<26}  {value}\n"));
}
fn opt<T: std::fmt::Display>(v: Option<T>) -> String {
    v.map(|x| x.to_string()).unwrap_or_else(|| "—".into())
}

/// The fourteen rows of the README image, in this order. Fixed-width so it
/// pastes into a fenced block unchanged.
pub fn render_table(sc: &Scorecard) -> String {
    let mut o = String::new();
    o.push_str(&format!(
        "\nlogweir drill scorecard  ({} v{})\n",
        sc.run_id, sc.format_version
    ));
    o.push_str(&"-".repeat(72));
    o.push('\n');
    row(&mut o, "outcome", format!("{:?}", sc.outcome));
    row(
        &mut o,
        "engine",
        format!(
            "{} {} {}",
            sc.engine.id, sc.engine.version, sc.engine.digest
        ),
    );
    row(
        &mut o,
        "levers",
        format!(
            "header_preflight={:?}  dry_run_check_segments={:?}",
            sc.engine.levers.header_preflight, sc.engine.levers.dry_run_check_segments
        ),
    );
    row(
        &mut o,
        "target",
        format!(
            "{}  marker={}  {} mapping entry/ies",
            sc.target.cluster_id, sc.target.marker_topic, sc.target.topic_mapping_entries
        ),
    );
    row(
        &mut o,
        "approval",
        if sc.approval.self_attested {
            "SELF-ATTESTED — the approval key equals the signing key".to_string()
        } else {
            format!("{} ({})", sc.approval.approver, sc.approval.ticket)
        },
    );
    o.push('\n');
    row(
        &mut o,
        "rto requested→verified",
        format!("{}s", opt(sc.measured.rto_requested_to_verified_seconds)),
    );
    row(
        &mut o,
        "rto approval→verified",
        format!("{}s", opt(sc.measured.rto_seconds)),
    );
    row(
        &mut o,
        "rto restore only",
        format!("{}s", opt(sc.measured.rto_restore_only_seconds)),
    );
    // The starred row is the ONE compared against the objective, because
    // header_preflight: full is a full read and decode of the window that no
    // incident responder performs.
    o.push_str(&format!(
        "* {:<26}  {}s   <- compared against objectives.rto_seconds\n",
        "rto excluding preflight",
        opt(sc.measured.rto_excluding_preflight_seconds)
    ));
    row(
        &mut o,
        "rpo",
        format!(
            "{}s   archive coverage gap at the requested point (NOT source-relative loss)",
            opt(sc.measured.rpo_seconds)
        ),
    );
    o.push('\n');
    row(
        &mut o,
        "integrity",
        format!(
            "{:?}/{:?}  {}/{} matched, {} mismatch(es)",
            sc.integrity.level,
            sc.integrity.result,
            sc.integrity.records_sampled_matching,
            sc.integrity.records_sampled,
            sc.integrity.mismatches
        ),
    );
    row(
        &mut o,
        "target diff",
        format!(
            "{} collision(s), {} would-create ({})",
            sc.target_diff.collisions.len(),
            sc.target_diff.would_create.len(),
            sc.target_diff.level
        ),
    );
    row(
        &mut o,
        "topic parity",
        format!(
            "intended [{}]  unexpected [{}]",
            sc.topic_parity.intentionally_deviated.join(", "),
            sc.topic_parity.unexpected_divergence.join(", ")
        ),
    );
    row(
        &mut o,
        "evidence",
        format!(
            "immutable={}  create_only_enforced={}",
            sc.evidence.immutable, sc.evidence.create_only_enforced
        ),
    );
    o.push('\n');
    o.push_str(&format!("  {}\n", sc.sample.coverage_note));
    o
}

pub fn run(path: &std::path::Path, format: &str) -> crate::exit::ExitCode {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("cannot read {}: {e}", path.display());
            return crate::exit::ExitCode::Operational;
        }
    };
    if format == "json" {
        // The RAW bytes as stored — printing a re-serialisation would show the
        // reader a document nobody signed.
        use std::io::Write as _;
        let _ = std::io::stdout().write_all(&bytes);
        return crate::exit::ExitCode::Ok;
    }
    match serde_json::from_slice::<Scorecard>(&bytes) {
        Ok(sc) => {
            println!("{}", render_table(&sc));
            crate::exit::ExitCode::Ok
        }
        Err(e) => {
            eprintln!("not a scorecard: {e}");
            crate::exit::ExitCode::Operational
        }
    }
}
