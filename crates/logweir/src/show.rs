//! `logweir drill show` — the fixed-width scorecard table (spec §13) and the
//! raw `--format json` passthrough.
use logweir_core::scorecard::Scorecard;
use logweir_core::spec::TargetMode;

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
    // The WIRE spelling, not Rust `Debug`: a reader compares this table with
    // the signed document. `wire_name` is pinned against each enum's own
    // `Serialize` impl in `logweir_core::outcome`.
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
    // The MODE is printed, and `marker=` only when the document carries a
    // marker topic, so a reader can tell a scratch drill from a `newTopic` run.
    row(
        &mut o,
        "target",
        match sc.target.marker_topic.as_deref() {
            Some(m) => format!(
                "{}  mode={}  marker={m}  {} mapping entry/ies",
                sc.target.cluster_id, sc.target.mode, sc.target.topic_mapping_entries
            ),
            None => format!(
                "{}  mode={}  {} mapping entry/ies",
                sc.target.cluster_id, sc.target.mode, sc.target.topic_mapping_entries
            ),
        },
    );
    row(
        &mut o,
        "approval",
        if sc.approval.self_attested {
            // `drill show` never sees a signature or a key (this function takes
            // only a Scorecard), so it can report the document's CLAIM and
            // nothing more. `drill verify` derives the finding by comparing
            // `approval.key_id` against the key that verified the signature and
            // refuses a document whose claim disagrees.
            "SELF-ATTESTED (claimed; run 'drill verify' to check it against the signing key)"
                .to_string()
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
            "intended [{}]  unexpected [{}]{}{}",
            sc.topic_parity.intentionally_deviated.join(", "),
            sc.topic_parity.unexpected_divergence.join(", "),
            parity_assessment(sc.topic_parity.not_assessed.as_deref()),
            reconstruction_assessment(
                sc.target.mode,
                sc.topic_parity.not_reconstructed.as_deref(),
                &sc.topic_parity.intentionally_deviated
            )
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

/// The configuration-parity qualifier on the `topic parity` row.
///
/// ABSENT (a 1.0.0 document, or one whose phase 7 never ran) says the
/// assessment was NOT RECORDED, because an empty `unexpected []` beside it is
/// exactly what a denied capture produces. `Some([])`
/// adds nothing: every topic's configuration was assessed. Otherwise the
/// unassessed topics are listed.
fn parity_assessment(not_assessed: Option<&[String]>) -> String {
    match not_assessed {
        None => "  configuration coverage not recorded".into(),
        Some([]) => String::new(),
        Some(topics) => format!("  not assessed [{}]", topics.join(", ")),
    }
}

/// The reconstruction qualifier on the `topic parity` row, from the
/// same three cases as `crate::verify::reconstruction_line`.
///
/// A format 1.2.0 document lists what it did NOT reconstruct; `[]` adds
/// nothing. A `newTopic` document WITHOUT the field predates 1.2.0, and its
/// writer labelled the settings it did not reconstruct `intended` (the
/// scratch rationale, in every mode), so the row says what that label means
/// there. A scratch drill's `intended` is what it says, in every version.
fn reconstruction_assessment(
    mode: TargetMode,
    not_reconstructed: Option<&[String]>,
    intentionally_deviated: &[String],
) -> String {
    match not_reconstructed {
        Some([]) => String::new(),
        Some(settings) => format!("  not reconstructed [{}]", settings.join(", ")),
        None if mode == TargetMode::NewTopic && !intentionally_deviated.is_empty() => {
            "  intended = NOT reconstructed (a newTopic document before format 1.2.0)".into()
        }
        None => String::new(),
    }
}

/// The `source.time_basis` footer value. ABSENT — a document before
/// 1.3.0 — is NOT RECORDED, never "every selection used the topic's own
/// clock"; an empty block is an em dash; otherwise each non-empty list.
fn time_basis_qualifier(label: Option<&logweir_core::scorecard::TimeBasisLabel>) -> String {
    let Some(label) = label else {
        return "not recorded: whether a time selection read producer timestamps is unknown".into();
    };
    let mut parts = Vec::new();
    if !label.producer_time.is_empty() {
        parts.push(format!(
            "SELECTED BY PRODUCER TIME [{}] (restore.time_basis: producerTime)",
            label.producer_time.join(", ")
        ));
    }
    if !label.not_recorded.is_empty() {
        parts.push(format!(
            "timestamp type NOT RECORDED [{}]",
            label.not_recorded.join(", ")
        ));
    }
    if parts.is_empty() {
        "—".into()
    } else {
        parts.join("  ")
    }
}

/// The `integrity.verification` footer value. ABSENT — a document
/// before 1.4.0 — is NOT RECORDED and read as a sample, never as complete; a
/// complete verification shows whether it covered every partition and its
/// exact counts, an incomplete one why it stopped.
fn verification_qualifier(v: Option<&logweir_core::scorecard::Verification>) -> String {
    let Some(v) = v else {
        return "not recorded: read as a sampled verdict, never a complete one".into();
    };
    match &v.complete {
        None => format!("{} (header order {})", v.coverage, v.header_order),
        Some(c) if c.covered => format!(
            "{}: every selected record compared ({} expected, {} restored, {} missing, {} \
             unexpected, {} duplicates, {} out of order, {} different)",
            v.coverage,
            c.replay.expected,
            c.replay.restored,
            c.replay.missing,
            c.replay.unexpected,
            c.replay.duplicates,
            c.replay.out_of_order,
            c.replay.mismatched
        ),
        Some(c) => format!(
            "{}, INCOMPLETE: {}",
            v.coverage,
            c.incomplete_reason
                .as_deref()
                .unwrap_or("no reason recorded")
        ),
    }
}

/// The `redactions` footer value: an em dash for the whole document every v0.1
/// run writes, and otherwise the count followed by every removed path.
///
/// The PATHS are printed and the `reason` strings are NOT. A `reason` is free
/// text that arrives with the document, so on a redacted document — which by
/// construction is one no Logweir writer produced — it is attacker-controlled
/// text, and this footer is read by a human deciding how much to trust the
/// table above. The path is the whole actionable signal.
fn redactions_line(redactions: &[logweir_core::scorecard::Redaction]) -> String {
    if redactions.is_empty() {
        return "—".into();
    }
    let paths: Vec<&str> = redactions.iter().map(|r| r.path.as_str()).collect();
    format!(
        "{} path(s) REMOVED — {}",
        redactions.len(),
        paths.join(", ")
    )
}

/// The footer below the table. The fourteen rows are spec §13's frozen layout
/// and are not touched, so what they omit that changes how the table should be
/// read (the RTO objective and whether objectives were `met`,
/// `integrity.partial_reason`, `engine_subreport.caveat`, the qualifiers below
/// and redactions) is printed here. Every value is read from the scorecard,
/// and the closing line says the table is a summary of a signed document, not
/// the document.
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
    // Which clock the restore's time selection read, where it was not the
    // topic's own: the rows above say `pass` for a point-in-time restore of a
    // `LogAppendTime` topic either way.
    o.push_str(&format!(
        "    source.time_basis         {}\n",
        time_basis_qualifier(sc.source.time_basis.as_ref())
    ));
    // Whether the `integrity` row above is a sample's verdict or
    // every record's. A qualifier in this footer's sense: the row reads
    // `byte-fingerprint/pass` either way.
    o.push_str(&format!(
        "    integrity.verification    {}\n",
        verification_qualifier(sc.integrity.verification.as_ref())
    ));
    // The topics a sampled verification's `max_partitions` left without
    // a sampled partition (scorecard 1.6.0). A qualifier in this footer's
    // sense — the `integrity` row reads `pass` either way — and printed only
    // when the document names one, so every other table is unchanged.
    if let Some(topics) = sc
        .sample
        .unsampled_topics
        .as_deref()
        .filter(|t| !t.is_empty())
    {
        o.push_str(&format!(
            "    sample.unsampled_topics   {} — no partition sampled (max_partitions); \
             counted, not reconciled\n",
            topics.join(", ")
        ));
    }
    // A second person approved this run in the console (scorecard
    // 1.9.0, or 2.1.0 for a partition subset). The `approval` row above shows
    // the approver and the ticket either way; this says HOW: the mode, who
    // asked, both instants, and that the approval's key is the console's,
    // which is expected in this mode. Printed only for a document carrying
    // the block, so every other table is unchanged.
    if let Some(console) = sc.approval.console.as_ref() {
        for line in console.lines() {
            o.push_str(&format!("    approval.console          {line}\n"));
        }
    }
    // The restore wrote under the source's ORIGINAL topic names
    // (scorecard 1.8.0). A qualifier in this footer's sense — the `target`
    // row reads `mode=newTopic` either way — printed only for a document
    // carrying the block, so every other table is unchanged.
    if let Some(on) = sc.target.original_name.as_ref() {
        o.push_str(&format!(
            "    target.original_name      restored under the original topic names (a new \
             generation of each name); approval subject {} ({}{}); {}; owners looked for in {}: \
             {}{}\n",
            on.approval_subject,
            on.approval_mode,
            if on.confirmation.is_some() {
                ", names typed"
            } else {
                ""
            },
            on.cluster_condition,
            on.owner_detection.join(", "),
            if on.owners.is_empty() {
                "none found".to_string()
            } else {
                on.owners
                    .iter()
                    .map(|o| format!("{} ({} {})", o.topic, o.kind, o.reference))
                    .collect::<Vec<_>>()
                    .join(", ")
            },
            if on.owner_path { "; owner path" } else { "" }
        ));
    }
    // `redactions[]` is a qualifier, not a reason to refuse: `show` renders it
    // and exits 0, and the two verifiers are the ones that refuse.
    o.push_str(&format!(
        "    redactions                {}\n",
        redactions_line(&sc.redactions)
    ));
    if !sc.redactions.is_empty() {
        o.push_str(
            "                              a redacted document is NOT what Logweir signs; \
             `drill verify` and\n                              docs/verify_scorecard.py both \
             REFUSE it. This table is not a check.\n",
        );
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

/// The whole of `run`'s behaviour, parameterised over the writer, so a test
/// can inspect exactly what would reach stdout. Every format parses the bytes
/// first, so "is this a scorecard" means the same under both, and an exit
/// code a CronJob branches on never claims success over a document that is not
/// one or was not rendered. `--format json` still writes `bytes`, the exact
/// slice read from disk, never `sc` re-serialised.
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
    // Global Constraint 12: `drill show` is a reader, so it refuses a higher
    // major rather than render it under field meanings that major may have
    // redefined. Both formats: `--format json`'s exit code is what a CronJob
    // branches on. Exit 1, not 4: `show` checks no signature, and "this reader
    // cannot honestly render this document" is an operational refusal.
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
    //! Coverage for `run`/`run_writing`, here because `run_writing` is not
    //! `pub`.
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

    /// Exercises the real public `run`, so the thin wrapper's delegation is
    /// covered. The missing-file path, because `run`'s writer is real stdout,
    /// which test output capturing does not suppress for a direct `Write`.
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

    /// A non-scorecard file exits 1 under `--format json` as under `table`:
    /// the two formats agree on what "this is a scorecard" means.
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

    /// An unrecognised `--format` must be refused, not silently treated
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

    /// `--format json` writes the EXACT bytes read from
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

    /// A write failure must be reported (`Operational`),
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

    /// Global Constraint 12: a higher major is refused. KILLS: deleting the
    /// `refuse_unreadable_major` call (the exit code becomes `Ok` and the
    /// table is written).
    fn a_scorecard_at_format_version(v: &str) -> Vec<u8> {
        let mut doc: serde_json::Value = serde_json::from_slice(GOOD_SCORECARD).unwrap();
        doc["format_version"] = serde_json::Value::String(v.into());
        serde_json::to_vec_pretty(&doc).unwrap()
    }

    #[test]
    fn a_higher_major_format_version_is_refused_and_never_rendered() {
        // 3.0.0: this build reads major 2 (the partition-subset format), so the
        // first major it does not read is 3.
        let (_dir, path) = write_temp(&a_scorecard_at_format_version("3.0.0"));
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
        let (_dir, path) = write_temp(&a_scorecard_at_format_version("3.0.0"));
        let mut out = Vec::new();
        assert_eq!(
            run_writing(&path, "json", &mut out),
            crate::exit::ExitCode::Operational
        );
        assert!(out.is_empty());
    }

    /// **Major 2 is read for ONE shape.** A 2.0.0
    /// document carrying a partition subset renders; a 2.0.0 document
    /// without one is refused (arm PS-1) and nothing is written, under both
    /// formats. KILLS: `drill show` rendering any 2.x document; refusing the
    /// subset one.
    #[test]
    fn a_2_0_0_document_renders_only_when_it_states_a_partition_subset() {
        let mut doc: serde_json::Value = serde_json::from_slice(GOOD_SCORECARD).unwrap();
        doc["format_version"] = "2.0.0".into();
        let (_dir, path) = write_temp(&serde_json::to_vec_pretty(&doc).unwrap());
        for format in ["table", "json"] {
            let mut out = Vec::new();
            assert_eq!(
                run_writing(&path, format, &mut out),
                crate::exit::ExitCode::Operational,
                "{format}"
            );
            assert!(out.is_empty(), "{format}");
        }
        doc["source"]["selection"] = serde_json::json!({
            "window_end_ms": 1_760_000_005_000i64,
            "partitions": [{"topic": "orders", "partitions": [0]}],
            "engine_runs": 1
        });
        let (_dir, path) = write_temp(&serde_json::to_vec_pretty(&doc).unwrap());
        let mut out = Vec::new();
        assert_eq!(
            run_writing(&path, "table", &mut out),
            crate::exit::ExitCode::Ok
        );
        assert!(!out.is_empty());
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
