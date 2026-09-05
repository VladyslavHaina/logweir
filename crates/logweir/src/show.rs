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
    // The WIRE spelling, not Rust `Debug`. One binary produced three
    // spellings of the same enum — `Pass` here, `pass` on `drill run`'s stdout
    // line, `pass` in the JSON and the Prometheus labels — so a reader
    // comparing the table against the signed document (which is what this
    // table is FOR) had to translate. `wire_name` is pinned against each
    // enum's own `Serialize` impl in `logweir_core::outcome`.
    row(&mut o, "outcome", sc.outcome.wire_name());
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
            "header_preflight={}  dry_run_check_segments={}",
            sc.engine.levers.header_preflight.wire_name(),
            sc.engine.levers.dry_run_check_segments.wire_name()
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
            "{}/{}  {}/{} matched, {} mismatch(es)",
            sc.integrity.level.wire_name(),
            sc.integrity.result.wire_name(),
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
    o.push_str(&qualifiers(sc));
    o
}

/// Task 22, carried obligation 3.
///
/// The fourteen rows above are spec §13's frozen layout and are NOT touched
/// here — no row is added, removed or reordered. What is added is a FOOTER,
/// below the table, in the same place `sample.coverage_note` already sits.
///
/// The reason is a review finding, not a preference: the frozen table cites
/// `objectives.rto_seconds` in the starred row's annotation but never displays
/// it, never says whether the objectives were `met`, and omits both
/// `integrity.partial_reason` and `engine_subreport.caveat` — which in this
/// repository's own fixture reads that the engine sub-report "corroborates
/// nothing Logweir claims". A reader who saw only the table therefore came away
/// MORE confident than the signed document supports, which is the exact failure
/// this surface exists to prevent.
///
/// Every value below is read from the scorecard; nothing is inferred, and the
/// closing line says plainly that the table is a summary of a signed document
/// rather than the document.
fn qualifiers(sc: &Scorecard) -> String {
    let mut o = String::new();
    o.push('\n');
    o.push_str("  objectives (from the approved plan)\n");
    o.push_str(&format!(
        "    rto_seconds               {:>6}   compared against the starred row above\n",
        opt(sc.objectives.rto_seconds)
    ));
    o.push_str(&format!(
        "    rpo_seconds               {:>6}\n",
        opt(sc.objectives.rpo_seconds)
    ));
    o.push_str(&format!(
        "    pass_rate                 {:>6}   measured {}\n",
        opt(sc.objectives.pass_rate),
        opt(sc.integrity.pass_rate_measured)
    ));
    // `met` is a tri-state and is rendered as one. "unmeasurable" is NOT "met",
    // and collapsing null into either direction is the class of overstatement
    // this footer exists to remove.
    o.push_str(&format!(
        "    met                       {:>6}\n",
        match sc.objectives.met {
            Some(true) => "yes",
            Some(false) => "NO",
            None => "unmeasurable",
        }
    ));
    o.push('\n');
    o.push_str("  qualifiers the fourteen rows above do not carry\n");
    o.push_str(&format!(
        "    integrity.partial_reason  {}\n",
        sc.integrity.partial_reason.as_deref().unwrap_or("—")
    ));
    match &sc.engine_subreport {
        Some(e) => o.push_str(&format!("    engine_subreport.caveat   {}\n", e.caveat)),
        None => o.push_str(
            "    engine_subreport          null — no engine sub-report was retained; \
             this is NOT \"the engine reported nothing wrong\"\n",
        ),
    }
    o.push_str(
        "\n  This table is a SUMMARY of a signed document, not the document. \
         `--format json`\n  prints the signed bytes; docs/verify-a-scorecard.md \
         lists what the summary omits.\n",
    );
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
    // GLOBAL CONSTRAINT 12. `drill show` IS A READER, and the reader contract
    // is "ignore unknown fields, refuse a higher major". Task ownership treated
    // this command as a renderer, so it never inherited the refusal that
    // `drill verify` gets for free from `validate_invariants` — a real
    // scorecard with `format_version` rewritten to `"2.0.0"` printed the full
    // table, header reading `v2.0.0`, `outcome Pass`, and exited 0. Rendering
    // a document under field meanings a future major may have redefined is
    // exactly the overstatement the constraint exists to prevent, and it is
    // worse on the `table` path than anywhere else: the table is the surface a
    // human reads instead of the JSON.
    //
    // BOTH formats, deliberately. `--format json` writes the raw bytes, so it
    // looks like a harmless passthrough — but its exit code is what a CronJob
    // or a CI step branches on, and exit 0 over a document this build cannot
    // honestly interpret is the same claim by a quieter route (the same
    // argument `run_writing`'s own fix round 1 makes about parsing on every
    // format rather than only on `table`).
    //
    // Exit 1, not 4: `show` performs no signature check and makes no claim
    // about one, and `docs/verify-a-scorecard.md` defines 4 as a signature or
    // lock-proof failure. "This reader cannot honestly render this document"
    // is an operational refusal.
    if let Err(e) = sc.refuse_unreadable_major() {
        eprintln!("refusing to render: {e}");
        return crate::exit::ExitCode::Operational;
    }
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

    /// GLOBAL CONSTRAINT 12. The README says "a reader ... refuses a higher
    /// major"; this reader rendered one and exited 0. The mutant that undoes
    /// the fix — deleting the `refuse_unreadable_major` call — turns both
    /// assertions below red at ASSERTION time: the exit code becomes `Ok` and
    /// the table is written.
    fn a_scorecard_at_format_version(v: &str) -> Vec<u8> {
        let mut doc: serde_json::Value = serde_json::from_slice(GOOD_SCORECARD).unwrap();
        doc["format_version"] = serde_json::Value::String(v.into());
        serde_json::to_vec_pretty(&doc).unwrap()
    }

    #[test]
    fn a_higher_major_format_version_is_refused_and_never_rendered() {
        let (_dir, path) = write_temp(&a_scorecard_at_format_version("2.0.0"));
        let mut out = Vec::new();
        assert_eq!(
            run_writing(&path, "table", &mut out),
            crate::exit::ExitCode::Operational,
            "a document from a future major bump must be refused, not rendered"
        );
        assert!(
            out.is_empty(),
            "nothing may be written for a document this reader cannot honestly \
             interpret: {}",
            String::from_utf8_lossy(&out)
        );
    }

    /// The `json` passthrough looks harmless and is not: its EXIT CODE is what
    /// a CronJob or a CI step branches on, so exit 0 over an uninterpretable
    /// document makes the same claim by a quieter route.
    #[test]
    fn a_higher_major_format_version_is_refused_under_json_format_too() {
        let (_dir, path) = write_temp(&a_scorecard_at_format_version("2.0.0"));
        let mut out = Vec::new();
        assert_eq!(
            run_writing(&path, "json", &mut out),
            crate::exit::ExitCode::Operational
        );
        assert!(out.is_empty());
    }

    /// The other half of GC12, and the half a too-eager refusal would break:
    /// a HIGHER MINOR is readable — "readers ignore unknown fields" — so
    /// `1.9.9` still renders. A mutant that refuses on any version difference
    /// rather than on the major turns this red.
    #[test]
    fn a_higher_minor_format_version_still_renders() {
        let (_dir, path) = write_temp(&a_scorecard_at_format_version("1.9.9"));
        let mut out = Vec::new();
        assert_eq!(
            run_writing(&path, "table", &mut out),
            crate::exit::ExitCode::Ok
        );
        assert!(!out.is_empty());
    }

    /// A `format_version` that is not semver at all is refused rather than
    /// rendered under an assumed meaning.
    #[test]
    fn an_unparseable_format_version_is_refused() {
        let (_dir, path) = write_temp(&a_scorecard_at_format_version("not-a-version"));
        let mut out = Vec::new();
        assert_eq!(
            run_writing(&path, "table", &mut out),
            crate::exit::ExitCode::Operational
        );
        assert!(out.is_empty());
    }
}
