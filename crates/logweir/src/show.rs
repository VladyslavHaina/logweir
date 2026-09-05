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
    run_writing(path, format, &mut std::io::stdout())
}

/// The whole of `run`'s behaviour, parameterised over the writer. `run` is a
/// one-line wrapper over this that supplies real stdout; the indirection
/// exists so a test can inspect exactly what would reach stdout — including
/// the "never a re-serialisation" guarantee on `--format json` — without
/// spawning the compiled binary, which does not dispatch `drill show` yet
/// (Task 22 wires `main.rs`).
///
/// Fix round 1 (F1/F2): the earlier version parsed only on the `table` path,
/// so `--format json` printed and exited 0 over ANY file, scorecard or not,
/// and a stdout write failure on that path was silently discarded, still
/// exiting 0. Both are wrong for the same reason: an exit code a CronJob or
/// CI branches on must not claim success over a document that was not
/// rendered, or that is not a scorecard at all. Now every format parses the
/// bytes first, so "is this a scorecard" means the same thing regardless of
/// how the caller asked to see it — and `--format json` still writes
/// `bytes`, the exact slice read from disk, never `sc` re-serialised, so
/// this parse-for-validation cannot regress the passthrough it sits next
/// to: the parsed `sc` is used only to decide the exit code and, on the
/// `table` path, to render.
fn run_writing(
    path: &std::path::Path,
    format: &str,
    out: &mut impl std::io::Write,
) -> crate::exit::ExitCode {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("cannot read {}: {e}", path.display());
            return crate::exit::ExitCode::Operational;
        }
    };
    let sc = match serde_json::from_slice::<Scorecard>(&bytes) {
        Ok(sc) => sc,
        Err(e) => {
            eprintln!("not a scorecard: {e}");
            return crate::exit::ExitCode::Operational;
        }
    };
    match format {
        "json" => {
            // The RAW bytes as stored — printing a re-serialisation would show
            // the reader a document nobody signed.
            if let Err(e) = out.write_all(&bytes) {
                eprintln!("failed to write to stdout: {e}");
                return crate::exit::ExitCode::Operational;
            }
            crate::exit::ExitCode::Ok
        }
        "table" => {
            if let Err(e) = writeln!(out, "{}", render_table(&sc)) {
                eprintln!("failed to write to stdout: {e}");
                return crate::exit::ExitCode::Operational;
            }
            crate::exit::ExitCode::Ok
        }
        other => {
            eprintln!(
                "unknown --format {other:?}; `logweir drill show` accepts \"table\" or \"json\""
            );
            crate::exit::ExitCode::Operational
        }
    }
}

#[cfg(test)]
mod tests {
    //! Coverage for `run`/`run_writing` specifically (fix round 1, F1/F2).
    //! Colocated here rather than in `crates/logweir/tests/show.rs` because
    //! these tests exercise `run_writing`, which is deliberately NOT `pub`
    //! (an integration test cannot see it) — `run`'s own public signature
    //! and behaviour are frozen exactly as task-21b-brief.md Step 3 writes
    //! them, so testing the writer-injected core in place, rather than
    //! widening the public API, is what keeps that true.
    use super::*;

    fn write_temp(contents: &[u8]) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("scorecard.json");
        std::fs::write(&path, contents).unwrap();
        (dir, path)
    }

    const GOOD_SCORECARD: &[u8] = include_bytes!("../../../e2e/fixtures/scorecard-pass.json");

    #[test]
    fn a_good_scorecard_renders_the_table_and_exits_ok() {
        let (_dir, path) = write_temp(GOOD_SCORECARD);
        let mut out = Vec::new();
        let code = run_writing(&path, "table", &mut out);
        assert_eq!(code, crate::exit::ExitCode::Ok);
        let printed = String::from_utf8(out).unwrap();
        assert!(
            printed.contains("outcome"),
            "table format must actually render the table: {printed}"
        );
    }

    /// Exercises the real public `run`, not just `run_writing`, so the thin
    /// wrapper itself is covered — a mutation that stopped `run` from
    /// delegating to `run_writing` (or delegated with the wrong path/format)
    /// would still be caught here. Deliberately uses the missing-file path
    /// rather than a good scorecard: `run`'s real writer is actual process
    /// stdout, which `cargo test`'s output capturing does not suppress for
    /// a direct `Write` call the way it suppresses `println!` (capturing
    /// only intercepts the print! family of macros) — so a success-path
    /// call here would print the rendered table to the test console on
    /// every run. The missing-file path writes nothing and still proves the
    /// delegation: `run_writing` alone (tested above and below) already
    /// covers the success and json-passthrough behaviour in full, with a
    /// `Vec<u8>` writer that never touches real stdout.
    #[test]
    fn run_itself_delegates_to_run_writing_on_a_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("does-not-exist.json");
        assert_eq!(run(&path, "table"), crate::exit::ExitCode::Operational);
    }

    #[test]
    fn a_missing_file_exits_operational_and_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("does-not-exist.json");
        let mut out = Vec::new();
        assert_eq!(
            run_writing(&path, "table", &mut out),
            crate::exit::ExitCode::Operational
        );
        assert!(
            out.is_empty(),
            "nothing must be written when the file cannot be read"
        );
    }

    #[test]
    fn unparseable_bytes_exit_operational_under_table_format() {
        let (_dir, path) = write_temp(b"not json at all");
        let mut out = Vec::new();
        assert_eq!(
            run_writing(&path, "table", &mut out),
            crate::exit::ExitCode::Operational
        );
        assert!(out.is_empty());
    }

    /// F2's headline gap: a non-scorecard file must exit 1 the same way
    /// under `--format json` as it does under `table` — the two formats
    /// must agree on what "this is a scorecard" means. Before this fix,
    /// `--format json` never parsed and so exited 0 over any file at all.
    #[test]
    fn unparseable_bytes_exit_operational_under_json_format_too() {
        let (_dir, path) = write_temp(b"not json at all");
        let mut out = Vec::new();
        assert_eq!(
            run_writing(&path, "json", &mut out),
            crate::exit::ExitCode::Operational
        );
        assert!(
            out.is_empty(),
            "nothing must be written to stdout for a non-scorecard file"
        );
    }

    /// F2: an unrecognised `--format` must be refused, not silently treated
    /// as `table`.
    #[test]
    fn an_unknown_format_is_rejected_not_silently_treated_as_table() {
        let (_dir, path) = write_temp(GOOD_SCORECARD);
        let mut out = Vec::new();
        let code = run_writing(&path, "yaml", &mut out);
        assert_eq!(code, crate::exit::ExitCode::Operational);
        assert!(
            out.is_empty(),
            "an unrecognised format must not silently fall through to the table"
        );
    }

    /// F1/M13's guarantee: `--format json` writes the EXACT bytes read from
    /// disk, never a re-serialisation. The fixture is given odd-but-valid
    /// trailing whitespace after its closing brace (which `serde_json`
    /// tolerates but no `to_string`/`to_vec` call would ever reproduce), so
    /// a mutant that parses-then-reprints is caught even though it would
    /// still be valid JSON describing the same scorecard.
    #[test]
    fn format_json_writes_the_exact_bytes_on_disk_not_a_reserialisation() {
        let mut bytes = GOOD_SCORECARD.to_vec();
        bytes.extend_from_slice(b"\n\n   \n");
        let (_dir, path) = write_temp(&bytes);
        let mut out = Vec::new();
        let code = run_writing(&path, "json", &mut out);
        assert_eq!(code, crate::exit::ExitCode::Ok);
        assert_eq!(
            out, bytes,
            "format json must write the bytes exactly as read from disk, \
             never a re-serialisation of the parsed value"
        );
    }

    /// F2's second item: a write failure must be reported (`Operational`),
    /// never silently discarded while still returning `Ok`. `Vec<u8>` can't
    /// exercise this (it never fails to write), so this uses a writer that
    /// always errors, standing in for a closed pipe or a full disk.
    struct FailingWriter;
    impl std::io::Write for FailingWriter {
        fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("simulated write failure"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_write_failure_is_reported_not_discarded_under_json_format() {
        let (_dir, path) = write_temp(GOOD_SCORECARD);
        assert_eq!(
            run_writing(&path, "json", &mut FailingWriter),
            crate::exit::ExitCode::Operational
        );
    }

    #[test]
    fn a_write_failure_is_reported_not_discarded_under_table_format() {
        let (_dir, path) = write_temp(GOOD_SCORECARD);
        assert_eq!(
            run_writing(&path, "table", &mut FailingWriter),
            crate::exit::ExitCode::Operational
        );
    }
}
