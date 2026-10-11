use crate::exit::ExitCode;
use logweir_core::backup_receipt::BackupReceipt;
use logweir_core::outcome::Outcome;
use logweir_core::scorecard::Scorecard;
use logweir_core::spec::TargetMode;
use logweir_evidence::{
    keys::VerifyingKey, verify::verify_detached, Error as EvidenceError, Sidecar,
    PAYLOAD_TYPE_BACKUP_RECEIPT, PAYLOAD_TYPE_CATALOG_POINT, PAYLOAD_TYPE_PUT_RECEIPT,
    PAYLOAD_TYPE_SCORECARD, PAYLOAD_TYPE_TEARDOWN,
};
use std::collections::BTreeMap;
use std::path::Path;

/// `--payload-type <name>` to the media type the sidecar must carry.
///
/// **The Rust twin of `docs/verify_scorecard.py::resolve_payload_type`, byte
/// for byte** (Task 5b). Three clauses, and all three are the Python
/// function's:
///
/// 1. a SHORT NAME maps to the media type of the same constant;
/// 2. a FULL MEDIA TYPE passes straight through — Task 5 deliberately refused
///    this, recording that "accepting a spelling the Python half accepts is a
///    parity claim that has to be made by a test the two readers share"; the
///    test now exists (`crates/logweir/tests/two_reader_parity_receipt.rs::
///    the_two_payload_type_resolvers_agree`), so the clause lands with it;
/// 3. anything else is an **error**, never a silent passthrough of a string
///    that happens to contain a slash: a typo'd media type would otherwise
///    turn into "unexpected payloadType" downstream and read like a bad
///    artifact rather than a bad command line.
///
/// Five arms over the five constants `logweir-verify` DECLARES, so there is
/// no sixth spelling of a media type anywhere in this binary.
///
/// `catalog-point` joined them with decision D3's recovery catalog
/// (PLAT-15.1). It is a SIGNATURE-ONLY document for this command — see
/// `Verdict::SignatureOnly` — because the catalog record's own facts are
/// recomputed from the verified backup receipt, not asserted by the record
/// (D3 §5.2 rule 3), and an exit 0 here must not be read as "this point is
/// available".
pub fn resolve_payload_type(name: &str) -> Result<&'static str, String> {
    const TYPES: [(&str, &str); 5] = [
        ("backup-receipt", PAYLOAD_TYPE_BACKUP_RECEIPT),
        ("catalog-point", PAYLOAD_TYPE_CATALOG_POINT),
        ("receipt", PAYLOAD_TYPE_PUT_RECEIPT),
        ("scorecard", PAYLOAD_TYPE_SCORECARD),
        ("teardown", PAYLOAD_TYPE_TEARDOWN),
    ];
    // The short-name arm.
    if let Some((_, media)) = TYPES.iter().find(|(short, _)| *short == name) {
        return Ok(media);
    }
    // Clause 2: a full media type passes through — returned as the CONSTANT,
    // not as the caller's own string, so a `&'static str` is honest and the
    // value downstream is the one this binary declares.
    if let Some((_, media)) = TYPES.iter().find(|(_, media)| *media == name) {
        return Ok(media);
    }
    // The short names are listed SORTED, which is what Python's
    // `", ".join(sorted(PAYLOAD_TYPES))` produces, and `TYPES` is declared in
    // that order so the two lists cannot come apart. The trailing clause "or
    // a full media type" is Python's too, and it is not decoration: without
    // it the message tells an operator to pass a short name while the
    // function accepts a media type.
    let shorts: Vec<&str> = TYPES.iter().map(|(short, _)| *short).collect();
    Err(format!(
        "unknown --payload-type {}; use one of {} or a full media type",
        python_repr(name),
        shorts.join(", ")
    ))
}

/// `name` as CPython's `repr()` of a `str` renders it.
///
/// The parity claim is BYTE-IDENTICAL refusal text, and the two languages
/// disagree about quoting: `format!("{:?}")` on a Rust `&str` produces
/// `"x"`, while Python's `{name!r}` produces `'x'`. One of them had to move
/// (Task 5's review). Python's moved nothing — `docs/verify_scorecard.py` is
/// the document an auditor is told to read and its message is the one quoted
/// in the interface register — so this function reproduces CPython's rule
/// here instead: single quotes, switching to double quotes when the value
/// contains a single quote and no double quote, with `\`, the active quote
/// and the three whitespace escapes escaped.
///
/// It is a FUNCTION and not an inline `format!` so that
/// `two_reader_parity_receipt.rs::the_two_payload_type_resolvers_agree` can
/// drive both readers over the awkward values (`it's`, `say "hi"`, `both'"`)
/// rather than only over a value with no quote in it, which is the case that
/// would have passed under either convention.
fn python_repr(name: &str) -> String {
    let quote = if name.contains('\'') && !name.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::with_capacity(name.len() + 2);
    out.push(quote);
    for c in name.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c => out.push(c),
        }
    }
    out.push(quote);
    out
}

#[derive(Debug, Clone)]
pub struct VerifyReport {
    pub signature_valid: bool,
    pub invariants_ok: bool,
    pub self_attested: bool,
    pub run_id: String,
    pub outcome: Outcome,
    pub approver: String,
    pub ticket: String,
    pub key_id: String,
    /// `evidence.offset_report_key` and `evidence.offset_report_sha256`, read
    /// PRESENCE-TOLERANTLY (Task 9b — Global Constraint 12's price for two
    /// nested optional fields, this reader's half).
    ///
    /// `None` means the document records no offset-mapping report, which is
    /// the truthful state for a run that signed a document without having
    /// completed a restore. The pair is `Option<(key, digest)>` and not two
    /// fields because `Scorecard::validate_invariants` refuses a document
    /// carrying one without the other — so by the time this value is built,
    /// "one without the other" is unrepresentable, and a type that could
    /// represent it would invite a printer to render half of it.
    pub offset_report: Option<(String, String)>,
    /// `topic_parity.not_assessed` (scorecard 1.1.0, FX-4), carried as read:
    /// `None` is NOT RECORDED (a 1.0.0 document), never "every topic assessed".
    pub not_assessed: Option<Vec<String>>,
    /// `target.mode`, `topic_parity.intentionally_deviated` and
    /// `topic_parity.not_reconstructed` (scorecard 1.2.0, FX-3), carried as
    /// read for [`reconstruction_line`]: `None` is NOT RECORDED, never
    /// "everything reconstructed".
    pub target_mode: TargetMode,
    pub intentionally_deviated: Vec<String>,
    pub not_reconstructed: Option<Vec<String>>,
    /// `source.time_basis` (scorecard 1.3.0, FX-8), carried as read: `None`
    /// is NOT RECORDED (a document before 1.3.0), never "every selection used
    /// the topic's own clock".
    pub time_basis: Option<logweir_core::scorecard::TimeBasisLabel>,
    /// `integrity.verification` (scorecard 1.4.0, PROD-08.1), carried as
    /// read: `None` is NOT RECORDED (a document before 1.4.0), read as a
    /// SAMPLED verdict and never as a complete one. Boxed: the block is the
    /// largest thing a report carries, and `Verdict` holds a report by value.
    pub verification: Option<Box<logweir_core::scorecard::Verification>>,
    /// `sample.unsampled_topics` (scorecard 1.6.0, FX-23), carried as read:
    /// `None` names no unsampled topic (and, before 1.6.0, says nothing).
    pub unsampled_topics: Option<Vec<String>>,
    /// The document's own `format_version`, for [`sampled_pass_lines`].
    pub format_version: String,
    /// `source.selection` (scorecard 1.7.0, PROD-11.1), carried as read:
    /// `None` is a restore of every partition from the archive's floor.
    pub selection: Option<logweir_core::scorecard::SelectionLabel>,
    /// What the document proves about records before a stated start (review
    /// N1), from its `integrity.result` and `integrity.verification`.
    pub before_the_start: logweir_core::scorecard::BeforeTheStart,
    /// What a 2.0.0 document proves about the other partitions of a narrowed
    /// topic (PROD-11.1b), from the same two fields.
    pub outside_the_subset: logweir_core::scorecard::OutsideTheSubset,
    /// `target.original_name` (scorecard 1.8.0, PROD-15.1), carried as read:
    /// `None` is a restore that did not write under the original names.
    /// Boxed: the block is the largest optional one, and `Verdict`'s variants
    /// stay comparable in size.
    pub original_name: Option<Box<logweir_core::scorecard::OriginalNameInfo>>,
    /// `approval.console` (scorecard 1.9.0 and 2.1.0, PROD-16.2), carried as
    /// read: `None` is a run no second person approved in the console.
    pub console_approval: Option<Box<logweir_core::scorecard::ConsoleApprovalInfo>>,
}

/// The `console approval:` line both readers print for a restore a SECOND
/// PERSON APPROVED IN THE CONSOLE (PROD-16.2): the writer's sentence
/// (`ConsoleApprovalInfo::lines`) — the mode, who asked and when, who approved
/// and when, when the request would have expired, and that the console key
/// signed both documents, which is expected in this mode and no other. Nothing
/// for a document without `approval.console`, so every other verdict prints
/// what it always did. `docs/verify_scorecard.py::_console_approval_lines`
/// prints the same line, and `scripts/check-verifier-parity.sh` compares every
/// line starting `console approval:` between the two readers.
#[must_use]
pub fn console_approval_lines(
    block: Option<&logweir_core::scorecard::ConsoleApprovalInfo>,
) -> Vec<String> {
    block
        .map(logweir_core::scorecard::ConsoleApprovalInfo::lines)
        .unwrap_or_default()
}

/// The two `original name:` lines both readers print for a restore under the
/// ORIGINAL topic names (PROD-15.1): the writer's sentences
/// (`OriginalNameInfo::lines`) — the approval subject and the document it was
/// verified in, the cluster condition that admitted it, and where the run
/// looked for a declarative owner and what it found. Nothing for a document
/// without `target.original_name`. `docs/verify_scorecard.py::
/// _original_name_lines` prints the same lines, and
/// `scripts/check-verifier-parity.sh` compares every line starting
/// `original name:` between the two readers.
#[must_use]
pub fn original_name_lines(
    block: Option<&logweir_core::scorecard::OriginalNameInfo>,
) -> Vec<String> {
    block
        .map(logweir_core::scorecard::OriginalNameInfo::lines)
        .unwrap_or_default()
}

/// The replay-selection line both readers print for a narrowed restore
/// (PROD-11.1): the writer's sentence (`SelectionLabel::sentence`), ending in
/// what THIS document proves — about the other partitions of a narrowed topic
/// (`OutsideTheSubset::of`, a 2.0.0 document, PROD-11.1b) and about the
/// records before a stated start (`BeforeTheStart::of`, review N1), each
/// "restored or expected" only over a verdict that proves it. Nothing for a
/// document without `source.selection`: it restored every partition from the
/// archive's floor. `docs/verify_scorecard.py::_selection_lines` prints the
/// same line, and `scripts/check-verifier-parity.sh` compares every line
/// starting `replay selection:` between the two readers.
#[must_use]
pub fn selection_lines(
    selection: Option<&logweir_core::scorecard::SelectionLabel>,
    before: logweir_core::scorecard::BeforeTheStart,
    outside: logweir_core::scorecard::OutsideTheSubset,
) -> Vec<String> {
    selection
        .map(|s| s.sentence(before, outside))
        .into_iter()
        .collect()
}

/// The line both readers print for a SAMPLED `pass` (FX-23 review M2): what
/// that pass proves, which depends on the document's version, because only
/// a build with FX-23's checks writes 1.6.0 and a 1.4.0 or 1.5.0 document is
/// the same bytes whichever build signed it. Nothing for a non-pass or a
/// complete verification. `docs/verify_scorecard.py::_sampled_pass_lines`
/// prints the same line, and `scripts/check-verifier-parity.sh` compares every
/// line starting `sample coverage:` between the two readers.
#[must_use]
pub fn sampled_pass_lines(
    outcome: Outcome,
    verification: Option<&logweir_core::scorecard::Verification>,
    format_version: &str,
) -> Vec<String> {
    sampled_pass_lines_over(outcome, verification, format_version, None)
}

/// [`sampled_pass_lines`] for a document that may carry `source.selection`
/// (PROD-11.1 review H1): over a narrowed restore the guarantee is QUALIFIED
/// by the selection, so the line a 1.7.0 or 2.0.0 reader prints never reads
/// as a pass over the whole archive.
///
/// - **A window start (1.7.0):** the count bound, the per-partition presence
///   and the engine-report check were judged over `[start, end]`, and no
///   record before the start was expected. It says the sampled check does
///   NOT show that no record before the start was restored (review N1): the
///   sample is drawn from the window, and a segment straddling the start
///   counts all of its records into the bound.
/// - **A partition subset (2.0.0, PROD-11.1b):** every SELECTED partition was
///   held to its own count bound, every other partition of a narrowed topic
///   was held EMPTY on the target, and the engine report was checked for the
///   selected partitions; the start clause above follows when the plan
///   states one.
///
/// `docs/verify_scorecard.py::_sampled_pass_lines` prints the same line.
#[must_use]
pub fn sampled_pass_lines_over(
    outcome: Outcome,
    verification: Option<&logweir_core::scorecard::Verification>,
    format_version: &str,
    selection: Option<&logweir_core::scorecard::SelectionLabel>,
) -> Vec<String> {
    let sampled =
        verification.is_none_or(|v| v.coverage == logweir_core::scorecard::COVERAGE_SAMPLED);
    if outcome != Outcome::Pass || !sampled {
        return Vec::new();
    }
    const START_CLAUSE: &str = "no record before the start was expected, and a sampled check \
                                does not prove that none was restored";
    if let Some(window) = selection.filter(|s| s.narrows_partitions()) {
        let from = match window.window_start_ms {
            Some(ms) => format!("epoch-ms {ms}"),
            None => "the archive's floor".to_string(),
        };
        let mut line = format!(
            "sample coverage: a sampled pass over a partition subset from {from} to epoch-ms {}: \
             every selected partition was held to its own count bound over that window, every \
             other partition of a narrowed topic was held empty, max_partitions reached every \
             topic before a second partition of any, and a readable engine report lacking a \
             selected partition with records in that window was refused",
            window.window_end_ms
        );
        if window.window_start_ms.is_some() {
            line.push_str("; ");
            line.push_str(START_CLAUSE);
        }
        return vec![line];
    }
    if let Some(window) = selection {
        return vec![format!(
            "sample coverage: a sampled pass over a replay selection from epoch-ms {} to \
             epoch-ms {}: every mapped partition was held to its own count bound over that \
             window, max_partitions reached every topic before a second partition of any, and a \
             readable engine report lacking a partition with records in that window was \
             refused; {START_CLAUSE}",
            window.window_start_ms.unwrap_or_default(),
            window.window_end_ms
        )];
    }
    if logweir_core::scorecard::proves_fx23_sampled_checks(format_version) {
        vec![
            "sample coverage: a sampled pass at format 1.6.0 or later: every mapped partition \
             was held to its own count bound, max_partitions reached every topic before a \
             second partition of any, and a readable engine report lacking a partition with \
             records in the window was refused"
                .to_string(),
        ]
    } else {
        vec![format!(
            "sample coverage: a sampled pass at format {format_version}, before 1.6.0: it \
             proves the canary and one count bound over every topic together, not a \
             per-partition count bound, a sample of every topic or an engine-report check (a \
             build from before FX-23 may have signed it)"
        )]
    }
}

/// The line both readers print for a scorecard whose sample left topics
/// unsampled (FX-23), or nothing when it names none.
/// `docs/verify_scorecard.py::_unsampled_lines` prints the same line, and
/// `scripts/check-verifier-parity.sh` compares every line starting
/// `sample coverage:` between the two readers.
#[must_use]
pub fn unsampled_lines(unsampled: Option<&[String]>) -> Vec<String> {
    match unsampled {
        Some(topics) if !topics.is_empty() => vec![format!(
            "sample coverage: no partition of {} topic(s) was sampled, because \
             sample.max_partitions is below the number of topics with records in the window: \
             {}; their partitions were held to the count bound only, never reconciled record by \
             record",
            topics.len(),
            topics.join(", ")
        )],
        _ => Vec::new(),
    }
}

/// The verification-coverage lines both readers print for a scorecard
/// (PROD-08.1): what the verdict covered, a complete verification's counts and
/// why it was incomplete, and how many capture gaps and pruned ranges the
/// verified partitions record — or the one line that says the coverage is not
/// recorded. `docs/verify_scorecard.py::_verification_lines` prints the same
/// lines from the same cases, and `scripts/check-verifier-parity.sh` compares
/// every line starting `integrity coverage:` between the two readers.
#[must_use]
pub fn verification_lines(v: Option<&logweir_core::scorecard::Verification>) -> Vec<String> {
    let Some(v) = v else {
        return vec![
            "integrity coverage: not recorded, so this verdict covered a sample, never every \
             record"
                .to_string(),
        ];
    };
    let mut lines = vec![format!(
        "integrity coverage: {} (compared with the {}; header order {}; application validation \
         {})",
        v.coverage, v.comparison_basis, v.header_order, v.application
    )];
    if let Some(c) = &v.complete {
        let r = &c.replay;
        let a = &c.archive;
        lines.push(format!(
            "integrity coverage: {}: {} expected, {} restored, {} matching, {} missing, {} \
             unexpected, {} duplicates, {} out of order, {} different; {} of {} segments \
             verified, {} failed, {} unverified; {} offset holes",
            if c.covered {
                "every selected record compared"
            } else {
                "INCOMPLETE"
            },
            r.expected,
            r.restored,
            r.matching,
            r.missing,
            r.unexpected,
            r.duplicates,
            r.out_of_order,
            r.mismatched,
            a.segments_verified,
            a.segments,
            a.segments_failed.len(),
            a.segments_unverified.len(),
            a.offset_holes
        ));
        if let Some(reason) = &c.incomplete_reason {
            lines.push(format!("integrity coverage: incomplete because {reason}"));
        }
    }
    if !v.gaps.is_empty() || !v.pruned.is_empty() {
        lines.push(format!(
            "integrity coverage: the verified partitions record {} capture gaps and {} pruned \
             ranges",
            v.gaps.len(),
            v.pruned.len()
        ));
    }
    lines
}

/// The configuration-parity line both readers print for a scorecard (FX-4),
/// or `None` when every topic was assessed. It names every `not_assessed`
/// entry as written, FX-21's `replication_factor (notRecorded)` and
/// `partition_count (notRecorded)` included. `docs/verify_scorecard.py` prints
/// the same sentence from the same three cases, and
/// `scripts/check-verifier-parity.sh` compares the two.
#[must_use]
pub fn parity_line(not_assessed: Option<&[String]>) -> Option<String> {
    match not_assessed {
        None => Some(
            "configuration parity: not recorded, so an empty unexpected_divergence proves \
             nothing"
                .to_string(),
        ),
        Some([]) => None,
        Some(topics) => Some(format!(
            "configuration parity: NOT ASSESSED for {}",
            topics.join("; ")
        )),
    }
}

/// The reconstruction line both readers print for a scorecard (FX-3), or
/// `None` when there is nothing it could change in an auditor's reading.
///
/// - **`not_reconstructed` present and non-empty** (format 1.2.0): the source
///   settings the restore did not reconstruct, whatever the mode — a reader
///   reports what the document says.
/// - **absent in a `newTopic` document whose `intentionally_deviated` names
///   anything**: a writer before 1.2.0 applied the scratch rationale in every
///   mode, so those "intended" settings are ones the restore did NOT
///   reconstruct. This re-reads OLD evidence as a WEAKER claim than its label,
///   never a stronger one (product-expansion rule 3), and changes no verdict.
/// - otherwise nothing: `[]` is the claim that nothing was left
///   unreconstructed, and a scratch drill's deviations are intended in every
///   version.
///
/// `docs/verify_scorecard.py::_reconstruction_line` prints the same sentence
/// from the same cases, and `scripts/check-verifier-parity.sh` compares them.
#[must_use]
pub fn reconstruction_line(
    mode: TargetMode,
    not_reconstructed: Option<&[String]>,
    intentionally_deviated: &[String],
) -> Option<String> {
    match not_reconstructed {
        Some([]) => None,
        Some(settings) => Some(format!(
            "reconstruction: source settings NOT RECONSTRUCTED for {}",
            settings.join("; ")
        )),
        None if mode == TargetMode::NewTopic && !intentionally_deviated.is_empty() => {
            Some(format!(
                "reconstruction: not recorded, so the settings this newTopic document labels \
                 intentionally_deviated were NOT reconstructed: {}",
                intentionally_deviated.join("; ")
            ))
        }
        None => None,
    }
}

/// One line per topic of a backup receipt's `config_coverage` (FX-4), or the
/// one line that says it is absent. `docs/verify_scorecard.py` prints the same
/// lines, and `scripts/check-verifier-parity.sh` compares every line starting
/// `config_coverage` between the two readers.
#[must_use]
pub fn coverage_lines(
    block: Option<&BTreeMap<String, logweir_core::backup_receipt::TopicConfigCoverage>>,
) -> Vec<String> {
    let Some(block) = block else {
        return vec![
            "config_coverage: not recorded, so every topic's configuration capture is \
             UNKNOWN, never captured"
                .to_string(),
        ];
    };
    block
        .iter()
        .map(|(topic, entry)| {
            let coverage = match &entry.reason {
                Some(reason) => format!("{} ({reason})", entry.coverage),
                None => entry.coverage.clone(),
            };
            let timestamp = match &entry.timestamp_type {
                Some(t) => format!("message.timestamp.type {} from {}", t.value, t.source),
                None => "message.timestamp.type not recorded".to_string(),
            };
            format!("config_coverage[{topic:?}]: {coverage}, {timestamp}")
        })
        .collect()
}

/// One line per topic of a backup receipt's `generations` (PROD-01.4a), or
/// the one line that says it is absent: the topic ID before and after the
/// capture, and what those two reads say about the point's generation
/// (`logweir_core::topic_identity::within_capture`). `docs/verify_scorecard.py`
/// prints the same lines, and `scripts/check-verifier-parity.sh` compares every
/// line starting `generations` between the two readers.
///
/// An unknown ID is never read as "the same": a topic whose reads recorded no
/// ID says why, and that its generation is not established by ID.
#[must_use]
pub fn generation_lines(
    block: Option<&BTreeMap<String, logweir_core::backup_receipt::TopicIdentity>>,
) -> Vec<String> {
    use logweir_core::topic_identity::{within_capture, WithinCapture};
    let Some(block) = block else {
        return vec![
            "generations: not recorded, so no topic ID is known from this receipt and each \
             topic's generation is UNKNOWN, never the same as another point's"
                .to_string(),
        ];
    };
    let side = |id: &Option<String>, reason: &Option<String>| match (id, reason) {
        (Some(id), _) => id.clone(),
        (None, Some(reason)) => format!("not recorded ({reason})"),
        (None, None) => "not recorded".to_string(),
    };
    block
        .iter()
        .map(|(topic, entry)| {
            let said = match within_capture(entry) {
                WithinCapture::Unchanged { topic_id } => format!(
                    "topic ID {topic_id} before and after the capture ({}), one generation",
                    entry.topic_id_source.as_deref().unwrap_or("no source")
                ),
                WithinCapture::Changed { before, after } => format!(
                    "topic ID CHANGED during the capture ({before} before, {after} after): the \
                     topic was deleted and recreated while it ran, so this point mixes two \
                     generations"
                ),
                WithinCapture::NotEstablished => format!(
                    "topic ID {} before the capture and {} after it, so its generation is not \
                     established by ID and is UNKNOWN",
                    side(&entry.topic_id, &entry.topic_id_reason),
                    side(&entry.topic_id_after, &entry.topic_id_after_reason)
                ),
            };
            format!("generations[{topic:?}]: {said}")
        })
        .collect()
}

/// One line per topic of a backup receipt's `topic_configuration`
/// (PROD-05.1), or the one line that says it is absent.
/// `docs/verify_scorecard.py` prints the same lines, and
/// `scripts/check-verifier-parity.sh` compares every line starting
/// `topic_configuration` between the two readers.
///
/// Counts and classes, never a configuration VALUE: a verify report is pasted
/// into tickets, and a value is the adopter's data.
///
/// How a topic is applied follows `logweir_core::topic_configuration::
/// apply_route`: an owned topic is restored by desired-state export; a topic
/// without an owner is applied through the admin API only where the run
/// LOOKED for one (`detection`, the receipt's `owner_detection`, is not
/// empty), and otherwise its owner was not checked and the line says how it
/// is applied is not known.
#[must_use]
pub fn topic_configuration_lines(
    block: Option<&BTreeMap<String, logweir_core::backup_receipt::TopicConfiguration>>,
    detection: Option<&[String]>,
) -> Vec<String> {
    let Some(block) = block else {
        return vec![
            "topic_configuration: not recorded, so no topic's partition count, replication \
             factor or settings are known to a restore from this receipt"
                .to_string(),
        ];
    };
    let count = |n: Option<u32>| n.map_or_else(|| "not recorded".to_string(), |n| n.to_string());
    // An absent detection is an empty one (receipt arm 21).
    let looked: &[String] = detection.unwrap_or(&[]);
    block
        .iter()
        .map(|(topic, model)| {
            let entries = match &model.entries {
                None => "entries not recorded".to_string(),
                Some(entries) => {
                    let by_class: Vec<String> =
                        logweir_core::topic_configuration::PORTABILITY_CLASSES
                            .iter()
                            .filter_map(|class| {
                                let n =
                                    entries.values().filter(|e| e.portability == *class).count();
                                (n > 0).then(|| format!("{class} {n}"))
                            })
                            .collect();
                    if by_class.is_empty() {
                        format!("{} entries", entries.len())
                    } else {
                        format!("{} entries ({})", entries.len(), by_class.join(", "))
                    }
                }
            };
            let route = match &model.owner {
                Some(o) => format!(
                    "owned by {} ({} {:?}), so restored by desired-state export",
                    o.kind, o.basis, o.reference
                ),
                None if looked.is_empty() => {
                    "owner not checked, so how it is applied is not known".to_string()
                }
                None => format!(
                    "no declarative owner found ({}), so applied through the admin API",
                    looked.join(", ")
                ),
            };
            format!(
                "topic_configuration[{topic:?}]: partitions {}, replication factor {}, \
                 {entries}, {route}",
                count(model.partitions),
                count(model.replication_factor)
            )
        })
        .collect()
}

/// The consumer position lines both readers print for a backup receipt
/// (PROD-04.1): a header with the number of groups and the listing word; the
/// positions document's key, digest and length, and whether it was verified;
/// one line per selected group, in id order — its outcome, and for a
/// captured one its type, both states, members, whether it was active and its
/// position counts; and, only for a VERIFIED document, one line per listed
/// position of each captured group and one with how many partitions have no
/// committed position. NOTHING when the receipt carries no block: the backup
/// selected no group. `docs/verify_scorecard.py` prints the same lines, and
/// `scripts/check-verifier-parity.sh` compares every line starting
/// `consumer_positions` between the two readers.
#[must_use]
pub fn consumer_positions_lines(
    block: Option<&logweir_core::consumer_positions::ConsumerPositions>,
    verified: Option<&logweir_core::consumer_positions::PositionsDocument>,
) -> Vec<String> {
    use logweir_core::consumer_positions as model;
    let Some(block) = block else {
        return Vec::new();
    };
    let mut lines = vec![
        format!(
            "consumer_positions: {} group(s), listing {}",
            block.groups.len(),
            block.listing
        ),
        format!(
            "consumer_positions: positions document {} ({}, {} bytes) {}",
            block.document.key,
            block.document.sha256,
            block.document.bytes,
            if verified.is_some() {
                "verified against this receipt"
            } else {
                "not checked: pass --consumer-positions <file> to verify it and print each \
                 position"
            }
        ),
    ];
    for (id, g) in &block.groups {
        let line = match (g.outcome.as_str(), &g.counts) {
            ("captured", Some(c)) => format!(
                "consumer_positions[{id:?}]: captured {}, state {} (listed {}), {} member(s), {}; \
                 positions: {} related to archived data, {} not related, {} never committed, {} \
                 beyond the end, {} failed, {} not observed",
                g.group_type.as_deref().unwrap_or(""),
                g.state.as_deref().unwrap_or(""),
                g.listed_state.as_deref().unwrap_or(""),
                g.members.unwrap_or(0),
                if g.active == Some(false) {
                    "inactive"
                } else {
                    "active"
                },
                c.related,
                c.not_related,
                c.never_committed,
                c.beyond_end,
                c.failed,
                c.not_observed,
            ),
            (outcome, _) => format!(
                "consumer_positions[{id:?}]: {outcome} ({}){}, no position recorded",
                g.reason.as_deref().unwrap_or(""),
                if g.group_type.as_deref() == Some(model::OTHER_TYPE) {
                    ", group type other"
                } else {
                    ""
                }
            ),
        };
        lines.push(line);
    }
    let Some(doc) = verified else {
        return lines;
    };
    for (id, g) in &doc.groups {
        for e in &g.positions {
            let at = format!("consumer_positions[{id:?}][{:?}:{}]", e.topic, e.partition);
            lines.push(match (e.status.as_str(), e.position) {
                ("captured", Some(p)) => {
                    format!(
                        "{at}: position {p}, {}",
                        e.coverage.as_deref().unwrap_or("")
                    )
                }
                (status, Some(p)) => format!(
                    "{at}: {status} ({}), position {p}",
                    e.reason.as_deref().unwrap_or("")
                ),
                (status, None) => format!(
                    "{at}: {status} ({}), no position",
                    e.reason.as_deref().unwrap_or("")
                ),
            });
        }
        lines.push(format!(
            "consumer_positions[{id:?}][*]: {} other partition(s) with no committed position, \
             never offset 0",
            g.no_committed_position
        ));
    }
    lines
}

/// One line per topic of a backup receipt's `schema_dependency` (PROD-03.0),
/// or the one line that says it is absent — NOT ASSESSED, never "not
/// schema-dependent". `docs/verify_scorecard.py` prints the same lines, and
/// `scripts/check-verifier-parity.sh` compares every line starting
/// `schema_dependency` between the two readers.
///
/// A `schemaDependent` topic says "registry not captured" in as many words:
/// no Logweir build captures a schema registry, so a reader of the restored
/// records needs one the archive does not carry.
#[must_use]
pub fn schema_dependency_lines(
    block: Option<&BTreeMap<String, logweir_core::backup_receipt::TopicSchemaDependency>>,
) -> Vec<String> {
    let Some(block) = block else {
        return vec![
            "schema_dependency: not assessed, so whether any topic's records need a schema \
             registry is not known from this receipt"
                .to_string(),
        ];
    };
    let side = |name: &str, s: &logweir_core::backup_receipt::SideFraming| {
        let non_null = u128::from(s.framed) + u128::from(s.unframed);
        let mut out = format!("{name} framed {} of {non_null} non-null", s.framed);
        if s.dependent {
            out.push_str(", dependent");
        }
        if !s.schema_ids.is_empty() {
            let ids: Vec<String> = s.schema_ids.iter().map(u32::to_string).collect();
            out.push_str(&format!(", schema ids {}", ids.join(", ")));
            let more = s.schema_id_count.saturating_sub(s.schema_ids.len() as u64);
            if more > 0 {
                out.push_str(&format!(" and {more} more"));
            }
        }
        out
    };
    block
        .iter()
        .map(|(topic, e)| {
            let verdict = match e.verdict.as_str() {
                "schemaDependent" => "schema-dependent, registry not captured".to_string(),
                "notDetected" => "no schema framing detected".to_string(),
                _ => format!(
                    "not assessed ({})",
                    e.reason.as_deref().unwrap_or("no reason recorded")
                ),
            };
            match (&e.key, &e.value) {
                (Some(k), Some(v)) => format!(
                    "schema_dependency[{topic:?}]: {verdict}; {} records judged ({}); {}; {}",
                    logweir_core::schema_dependency::judged_records(k),
                    e.basis.as_deref().unwrap_or("no basis recorded"),
                    side("key", k),
                    side("value", v)
                ),
                _ => format!("schema_dependency[{topic:?}]: {verdict}"),
            }
        })
        .collect()
}

/// The time-basis lines both readers print for a scorecard (FX-8): one per
/// non-empty list of `source.time_basis`, the one line that says it was not
/// recorded, or nothing when the restore selected no topic by producer time
/// and none with an unrecorded timestamp type. `docs/verify_scorecard.py`
/// prints the same lines from the same cases, and
/// `scripts/check-verifier-parity.sh` compares every line starting
/// `time basis:` between the two readers.
#[must_use]
pub fn time_basis_lines(label: Option<&logweir_core::scorecard::TimeBasisLabel>) -> Vec<String> {
    let Some(label) = label else {
        return vec![
            "time basis: not recorded, so whether a time selection read a LogAppendTime \
             topic's producer timestamps is unknown"
                .to_string(),
        ];
    };
    let mut lines = Vec::new();
    if !label.producer_time.is_empty() {
        lines.push(format!(
            "time basis: SELECTED BY PRODUCER TIME for {} (recorded as LogAppendTime; the \
             approved plan states restore.time_basis: producerTime)",
            label.producer_time.join(", ")
        ));
    }
    if !label.not_recorded.is_empty() {
        lines.push(format!(
            "time basis: timestamp type NOT RECORDED for {}, so its time selection may have \
             read producer timestamps",
            label.not_recorded.join(", ")
        ));
    }
    lines
}

/// The time-basis lines both readers print for a BACKUP RECEIPT (FX-8): one
/// per topic whose recorded effective `message.timestamp.type` is
/// `LogAppendTime` (FX-4's `config_coverage`). The receipt's format is
/// unchanged; what it already records is said in words, because its covered
/// window — what the catalog and the console offer as recovery points — reads
/// that topic's PRODUCER timestamps. `scripts/check-verifier-parity.sh`
/// compares these lines between the two readers.
#[must_use]
pub fn receipt_time_basis_lines(
    block: Option<&BTreeMap<String, logweir_core::backup_receipt::TopicConfigCoverage>>,
) -> Vec<String> {
    block
        .into_iter()
        .flatten()
        .filter(|(_, entry)| {
            entry
                .timestamp_type
                .as_ref()
                .is_some_and(|t| t.value == logweir_core::time_basis::LOG_APPEND_TIME)
        })
        .map(|(topic, _)| {
            format!(
                "time basis: {topic:?} is LogAppendTime, so the archive holds its producers' \
                 timestamps and the covered window reads them; a restore that selects it by \
                 time is refused unless its plan states restore.time_basis: producerTime"
            )
        })
        .collect()
}

/// What a verification actually established.
///
/// # Why an enum and not four optional fields on `VerifyReport`
///
/// `--payload-type` lets one command verify four different documents, and
/// only one of them has an `outcome`, an approver and a ticket. A
/// `VerifyReport` with those fields nulled for a receipt would print an empty
/// approval line and let a caller ask a scorecard question of a document that
/// cannot answer it. The two verdicts are different claims, so they are
/// different values.
#[derive(Debug, Clone)]
pub enum Verdict {
    /// The signature verified AND every scorecard invariant AND the derived
    /// approval claim held. The full-strength verdict, and the only one
    /// `--payload-type scorecard` (the default) can produce.
    Scorecard(VerifyReport),
    /// The signature verified AND all five of
    /// `logweir_core::backup_receipt::BackupReceipt`'s invariants held. The
    /// backup receipt's full-strength verdict, and the second document type
    /// this command evaluates rather than merely authenticates (Task 5b).
    ///
    /// It carries no `outcome`, approver or ticket for the reason the enum's
    /// own doc comment gives: a receipt cannot answer a scorecard's
    /// questions, and a `VerifyReport` with those fields nulled would print
    /// an empty approval line over a document that never had one.
    BackupReceipt {
        payload_type: String,
        key_id: String,
        backup_id: String,
        run_id: String,
        manifest_key: String,
        /// **FX-7.** `archive.manifest_version_id`, when the receipt pins one:
        /// the object version the manifest digest is over.
        manifest_version_id: Option<String>,
        /// The 1.1.0 block (FX-4), as read; `None` is UNKNOWN coverage.
        config_coverage:
            Option<BTreeMap<String, logweir_core::backup_receipt::TopicConfigCoverage>>,
        /// The 1.3.0 block (PROD-05.1), as read; `None` is NOT RECORDED.
        topic_configuration:
            Option<BTreeMap<String, logweir_core::backup_receipt::TopicConfiguration>>,
        /// The 1.3.0 `owner_detection` (PROD-05.1), as read: where the run
        /// looked for owners. `None` reads as empty (arm 21).
        owner_detection: Option<Vec<String>>,
        /// The 1.5.0 block (PROD-03.0), as read; `None` is NOT ASSESSED.
        schema_dependency:
            Option<BTreeMap<String, logweir_core::backup_receipt::TopicSchemaDependency>>,
        /// The 1.6.0 block (PROD-01.4a), as read; `None` is UNKNOWN for every
        /// topic.
        generations: Option<BTreeMap<String, logweir_core::backup_receipt::TopicIdentity>>,
        /// The 1.7.0 block (PROD-04.1), as read; `None` is "no group
        /// selected".
        consumer_positions: Option<logweir_core::consumer_positions::ConsumerPositions>,
        /// The positions document the block binds, when the reader was given
        /// it and it held against the receipt (arms CP-1 to CP-14); `None`
        /// when it was not given.
        positions: Option<logweir_core::consumer_positions::PositionsDocument>,
    },
    /// The signature verified over these exact bytes under this key, and the
    /// sidecar's `payloadType` is the one asked for. **Nothing about the
    /// document's own consistency was checked**, because the invariant reader
    /// for this document type is not wired into this command — it is the
    /// verdict for the drill put receipt and the teardown attestation, whose
    /// readers are not in tag 1. The printer says so in as many words; an
    /// exit 0 that silently meant less than the scorecard's exit 0 would be
    /// the worst thing this command could do.
    SignatureOnly {
        payload_type: String,
        key_id: String,
    },
    /// **A catalog point record (PROD-01.4a review M1).** The signature
    /// verified, AND the one check this build makes of the record held: every
    /// topic ID it copies is a real topic ID in Kafka's text
    /// (`logweir_core::topic_identity::refuse_copied_topic_ids`). Nothing
    /// else: its other copied facts are worth what the receipt it names is
    /// worth, and that receipt is not fetched here.
    CatalogPoint {
        payload_type: String,
        key_id: String,
    },
}

/// The function the Interfaces block promises. `run` is a thin printer over it,
/// which is what keeps `VerifyReport` and its five fields live under
/// `clippy -D warnings` (there is no external consumer of this crate's library
/// in v0.1).
///
/// `payload_type` is a **resolved media type** — the output of
/// `resolve_payload_type`, not the short name the operator typed. `run`
/// resolves before calling, exactly as `docs/verify_scorecard.py` resolves in
/// `main` and hands `verify()` a media type, so the two readers cannot end up
/// disagreeing about where the mapping happens.
pub fn verify_scorecard(
    scorecard: &Path,
    signature: &Path,
    public_key: &Path,
    payload_type: &str,
) -> Result<Verdict, ExitCode> {
    verify_scorecard_with(scorecard, signature, public_key, payload_type, None)
}

/// The refusal prefix of a positions document that does not hold against its
/// receipt. `two_reader_parity_positions.rs` strips it, as `INVALID: ` is
/// stripped from the second reader.
pub const POSITIONS_REFUSED: &str = "SIGNATURE VALID but the consumer positions document is \
                                     refused: ";

/// [`verify_scorecard`], and — for a backup receipt — the positions document
/// at `positions`, checked against the verified receipt (PROD-04.1, arms
/// CP-1 to CP-14). A document given for any other payload type is a bad
/// command line (exit 1); one that does not hold is exit 4, like a receipt
/// that contradicts itself.
pub fn verify_scorecard_with(
    scorecard: &Path,
    signature: &Path,
    public_key: &Path,
    payload_type: &str,
    positions: Option<&Path>,
) -> Result<Verdict, ExitCode> {
    if positions.is_some() && payload_type != PAYLOAD_TYPE_BACKUP_RECEIPT {
        eprintln!(
            "--consumer-positions applies to --payload-type backup-receipt only: the positions \
             document is checked against the backup receipt that binds it"
        );
        return Err(ExitCode::Operational);
    }
    // verify-as-read: the exact bytes on disk, never a re-serialisation.
    let bytes = match std::fs::read(scorecard) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("cannot read {}: {e}", scorecard.display());
            return Err(ExitCode::Operational);
        }
    };
    let sidecar: Sidecar = match std::fs::read(signature)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
    {
        Some(s) => s,
        None => {
            eprintln!("cannot read a DSSE sidecar from {}", signature.display());
            return Err(ExitCode::Operational);
        }
    };
    let key = match VerifyingKey::from_pem_file(public_key) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("{e}");
            return Err(ExitCode::Operational);
        }
    };
    // **F2 (Task 5's review): a payload-type mismatch is not a bad
    // signature, and this line used to say it was.**
    //
    // `verify_detached` returns `Error::Verify` for a mismatch — the right
    // class (substitution, exit 4) and the right escalation — but the printer
    // below renders every `Verify` as `SIGNATURE INVALID:`, so a
    // genuinely-signed receipt presented as a scorecard was reported to an
    // operator as a forgery. The signature over those bytes may be perfectly
    // valid; what is wrong is that it signs a DIFFERENT KIND OF DOCUMENT.
    //
    // Compared HERE, before `verify_detached`, because this is where both the
    // sidecar and the wanted type are in hand and where the message can be
    // specific. `verify_detached` keeps its own comparison — PAE binds
    // `payload_type` cryptographically and the library must refuse a mismatch
    // for every caller, not only this one — so this is a better message in
    // front of an unchanged rule, never a relaxation of it. Exit code
    // unchanged at 4: `drill_verify_exits_signing_or_lock_on_a_payload_type_
    // mismatch` and `the_signed_receipt_fixture_verifies` both pin it.
    if sidecar.payload_type != payload_type {
        eprintln!(
            "PAYLOAD TYPE MISMATCH: the sidecar signs {}, but this check asked for {}. \
             The signature is not reported as invalid: it may be entirely valid over some \
             OTHER document. A signed document of one type presented in place of another \
             is SUBSTITUTION, which is why this exits 4 rather than 1.",
            sidecar.payload_type, payload_type
        );
        return Err(ExitCode::SigningOrLock);
    }
    // Deferred finding: `logweir-evidence::Error` distinguishes structural
    // corruption of the sidecar (`Malformed` — base64 that will not decode,
    // a DER blob that is invalid or the wrong length) from a definite
    // cryptographic/protocol negative (`Verify` — a well-formed signature
    // that does not match, no signature by the presented key, or a
    // payload_type mismatch, which is evidence of substitution). The former
    // says nothing about whether the archive itself is trustworthy — it is
    // an operational failure, like a truncated file, and must NOT be
    // reported as tampering. The latter is exactly what tampering (or
    // substitution) after signing produces, so it maps to the same exit
    // code the brief assigns a bad signature: 4, "signing or lock-proof
    // failed".
    //
    // `verify_detached` returns the `keyid` of the signature that actually
    // matched and verified — never `sidecar.signatures[0]`, which is not
    // necessarily the signature that was checked when a sidecar carries more
    // than one signature.
    let matched_key_id = match verify_detached(&key, payload_type, &bytes, &sidecar) {
        Ok(id) => id,
        Err(e) => {
            return match e {
                EvidenceError::Malformed(msg) => {
                    eprintln!("cannot verify: the signature data is malformed: {msg}");
                    Err(ExitCode::Operational)
                }
                EvidenceError::Verify(msg) => {
                    eprintln!("SIGNATURE INVALID: {msg}");
                    Err(ExitCode::SigningOrLock)
                }
                EvidenceError::Key(msg) => {
                    eprintln!("{msg}");
                    Err(ExitCode::Operational)
                }
            };
        }
    };
    // **THE DISPATCH** (Task 5b). One command verifies four documents, and
    // the invariant reader that runs is chosen by the RESOLVED media type —
    // never by what the bytes happen to parse as. A scorecard runs the
    // scorecard's arms, a backup receipt runs `BackupReceipt::
    // validate_invariants`'s four, and **neither set runs on the other
    // document**: running the scorecard's arms over a receipt would either
    // fail at deserialisation (reporting a good receipt as a bad scorecard)
    // or, far worse, appear to check something.
    //
    // Note what the branch does NOT have to defend against: a document of
    // the wrong type handed to the default `--payload-type scorecard`. The
    // sidecar's `payloadType` is compared in full above, and a
    // genuinely-signed sidecar for a different kind of document presented in
    // place of this one is refused there as substitution. So the only way to
    // reach a receipt's arms is to have ASKED for a receipt.
    if payload_type == PAYLOAD_TYPE_BACKUP_RECEIPT {
        let receipt: BackupReceipt = match serde_json::from_slice(&bytes) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("signature verified but the payload is not a backup receipt: {e}");
                return Err(ExitCode::Operational);
            }
        };
        // The SAME outer sentence the scorecard's arms are reported under, so
        // `scripts/check-verifier-parity.sh` and
        // `crates/logweir/tests/two_reader_parity_receipt.rs` strip one prefix
        // for both document types. `validate_invariants` returns the bare
        // message (no `InvariantError` wrapper — the receipt's arms return a
        // `String`, because the string IS the interface the second reader
        // reproduces byte for byte), so there is no inner prefix here.
        if let Err(e) = receipt.validate_invariants() {
            eprintln!("SIGNATURE VALID but the document is self-contradicting: {e}");
            return Err(ExitCode::SigningOrLock);
        }
        // PROD-04.1: the positions document, only when given, against the
        // receipt that was just verified — its exact bytes, never a
        // re-serialisation.
        let positions = match positions {
            None => None,
            Some(path) => {
                let doc_bytes = match std::fs::read(path) {
                    Ok(b) => b,
                    Err(e) => {
                        eprintln!("cannot read {}: {e}", path.display());
                        return Err(ExitCode::Operational);
                    }
                };
                let doc: logweir_core::consumer_positions::PositionsDocument =
                    match serde_json::from_slice(&doc_bytes) {
                        Ok(d) => d,
                        Err(e) => {
                            eprintln!(
                                "{POSITIONS_REFUSED}it does not parse as a positions document: \
                                 {e}"
                            );
                            return Err(ExitCode::SigningOrLock);
                        }
                    };
                if let Err(e) = receipt.validate_consumer_positions_document(&doc_bytes, &doc) {
                    eprintln!("{POSITIONS_REFUSED}{e}");
                    return Err(ExitCode::SigningOrLock);
                }
                Some(doc)
            }
        };
        return Ok(Verdict::BackupReceipt {
            payload_type: payload_type.to_string(),
            key_id: matched_key_id,
            backup_id: receipt.backup_id,
            run_id: receipt.run_id,
            manifest_key: receipt.archive.manifest_key,
            manifest_version_id: receipt.archive.manifest_version_id,
            config_coverage: receipt.config_coverage,
            topic_configuration: receipt.topic_configuration,
            owner_detection: receipt.owner_detection,
            schema_dependency: receipt.schema_dependency,
            generations: receipt.generations,
            consumer_positions: receipt.consumer_positions,
            positions,
        });
    }
    if payload_type == PAYLOAD_TYPE_CATALOG_POINT {
        // PROD-01.4a review M1: a record that copies Kafka's reserved topic ID
        // (or any text that is not a real ID) is refused, as the receipt it
        // copies from would be by arm 38. Bytes that are not JSON fall through
        // to the signature-level verdict as before: there is no ID in them.
        if let Ok(record) = serde_json::from_slice::<serde_json::Value>(&bytes) {
            if let Err(e) = logweir_core::topic_identity::refuse_copied_topic_ids(&record) {
                eprintln!("SIGNATURE VALID but the document is self-contradicting: {e}");
                return Err(ExitCode::SigningOrLock);
            }
        }
        return Ok(Verdict::CatalogPoint {
            payload_type: payload_type.to_string(),
            key_id: matched_key_id,
        });
    }
    if payload_type != PAYLOAD_TYPE_SCORECARD {
        return Ok(Verdict::SignatureOnly {
            payload_type: payload_type.to_string(),
            key_id: matched_key_id,
        });
    }
    let sc: Scorecard = match serde_json::from_slice(&bytes) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("signature verified but the payload is not a scorecard: {e}");
            return Err(ExitCode::Operational);
        }
    };
    // Global Constraint 12 (a reader must refuse a higher-major
    // format_version) is enforced inside `validate_invariants`, so a
    // well-signed document from a future major bump is refused here, not
    // silently accepted.
    if let Err(e) = sc.validate_invariants() {
        eprintln!("SIGNATURE VALID but the document is self-contradicting: {e}");
        return Err(ExitCode::SigningOrLock);
    }
    // T0-1. `approval.self_attested` in the document is a CLAIM. The finding is
    // derived from the key that actually verified this signature — the same
    // comparison the writer makes at
    // `crates/logweir/src/drill/phase1_approval.rs`'s
    // `let self_attested = key_id == signing_key.key_id();`. A document that
    // claims otherwise is refused: it is a provenance claim the signature
    // cannot support, so it is the same class as a bad signature (Global
    // Constraint 11, exit 4). NOT an invariant in `logweir-core` — that layer
    // has no key. Position matters: this arm runs AFTER `validate_invariants`,
    // so a self-contradicting document still gets the more fundamental
    // finding above rather than this one.
    let derived_self_attested = sc.approval.key_id == matched_key_id;
    if derived_self_attested != sc.approval.self_attested {
        if sc.approval.self_attested {
            eprintln!(
                "APPROVAL CLAIM NOT VERIFIED: the document claims self_attested=true \
                 but the approval key id {} does not match the verifying key id {}",
                sc.approval.key_id, matched_key_id
            );
        } else {
            // The other direction is a refusal too, and it needs its own
            // sentence: a message that said "does not match" here would state
            // a falsehood in the one line whose whole purpose is to be
            // trustworthy.
            eprintln!(
                "APPROVAL CLAIM NOT VERIFIED: the document claims self_attested=false \
                 but the approval key id {} matches the verifying key id {}",
                sc.approval.key_id, matched_key_id
            );
        }
        return Err(ExitCode::SigningOrLock);
    }
    // Both-or-neither, and `validate_invariants` above has already refused
    // anything else — so `zip` is exhaustive here rather than lossy.
    let offset_report = sc
        .evidence
        .offset_report_key
        .clone()
        .zip(sc.evidence.offset_report_sha256.clone());
    Ok(Verdict::Scorecard(VerifyReport {
        signature_valid: true,
        invariants_ok: true,
        self_attested: derived_self_attested,
        run_id: sc.run_id.clone(),
        outcome: sc.outcome,
        approver: sc.approval.approver.clone(),
        ticket: sc.approval.ticket.clone(),
        key_id: matched_key_id,
        offset_report,
        not_assessed: sc.topic_parity.not_assessed.clone(),
        target_mode: sc.target.mode,
        intentionally_deviated: sc.topic_parity.intentionally_deviated.clone(),
        not_reconstructed: sc.topic_parity.not_reconstructed.clone(),
        time_basis: sc.source.time_basis.clone(),
        verification: sc.integrity.verification.clone().map(Box::new),
        unsampled_topics: sc.sample.unsampled_topics.clone(),
        selection: sc.source.selection.clone(),
        before_the_start: logweir_core::scorecard::BeforeTheStart::of(
            &sc.integrity.result,
            sc.integrity.verification.as_ref(),
        ),
        outside_the_subset: logweir_core::scorecard::OutsideTheSubset::of(
            &sc.integrity.result,
            sc.integrity.verification.as_ref(),
        ),
        format_version: sc.format_version.clone(),
        original_name: sc.target.original_name.clone().map(Box::new),
        console_approval: sc.approval.console.clone().map(Box::new),
    }))
}

fn print_report(r: &VerifyReport) {
    println!("signature: VALID  key {}", r.key_id);
    println!("run_id:    {}", r.run_id);
    // The WIRE spelling, like every other surface. This line was the FOURTH
    // rendering of one enum out of one binary (`Pass` here, `pass` on `drill
    // run`'s stdout, `Pass` in `drill show`'s table, `pass` in the JSON and
    // the Prometheus labels) — and it is the one an auditor reads directly
    // beside the document it is verifying.
    println!("outcome:   {}", r.outcome.wire_name());
    if r.self_attested {
        // R13: the artifact must survive this reading rather than hide it.
        println!("approval:  SELF-ATTESTED — the approval key equals the signing key");
    } else {
        println!("approval:  {} ({})", r.approver, r.ticket);
    }
    // PROD-16.2: who approved and HOW, when a second person approved in the
    // console — the approver above is then a principal the console attested,
    // and the approval's key is the console's, which this line says is
    // expected in this mode. Printed only for a document carrying the block.
    for line in console_approval_lines(r.console_approval.as_deref()) {
        println!("approval:  {line}");
    }
    // The offset report, printed only when the document records one — so a
    // scorecard signed before this field existed prints exactly what it always
    // did. The DIGEST goes beside the key, because a key alone tells an auditor
    // where to look and not whether what they find is what was signed.
    // `docs/verify_scorecard.py` prints the same two facts in the same order.
    if let Some((key, digest)) = &r.offset_report {
        println!("offsets:   {key}");
        println!(
            "           {digest} — the engine's offset MAPPING, uploaded as evidence \
             and applied to nothing"
        );
    }
    // FX-4: an exit 0 is not configuration parity the document does not claim.
    if let Some(line) = parity_line(r.not_assessed.as_deref()) {
        println!("parity:    {line}");
    }
    // FX-3: nor is it a restore that reconstructed the source's settings.
    if let Some(line) = reconstruction_line(
        r.target_mode,
        r.not_reconstructed.as_deref(),
        &r.intentionally_deviated,
    ) {
        println!("parity:    {line}");
    }
    // FX-8: nor a selection by the source topics' own clocks it does not claim.
    for line in time_basis_lines(r.time_basis.as_ref()) {
        println!("time:      {line}");
    }
    // PROD-08.1: nor a verdict over every record when it covered a sample.
    for line in verification_lines(r.verification.as_deref()) {
        println!("coverage:  {line}");
    }
    // FX-23: what a sampled pass proves at this document's version (review
    // M2), and the topics the cap left out.
    for line in sampled_pass_lines_over(
        r.outcome,
        r.verification.as_deref(),
        &r.format_version,
        r.selection.as_ref(),
    )
    .into_iter()
    .chain(unsampled_lines(r.unsampled_topics.as_deref()))
    {
        println!("coverage:  {line}");
    }
    // PROD-11.1: nor a restore of the whole archive when it restored a
    // selection.
    for line in selection_lines(
        r.selection.as_ref(),
        r.before_the_start,
        r.outside_the_subset,
    ) {
        println!("coverage:  {line}");
    }
    // PROD-15.1: nor an ordinary restore when it wrote under the original
    // topic names — and what admitted that.
    for line in original_name_lines(r.original_name.as_deref()) {
        println!("target:    {line}");
    }
}

/// The receipt blocks the verdict prints per topic, beside the identity
/// fields `print_backup_receipt` takes one by one.
struct ReceiptBlocks<'a> {
    /// FX-4's 1.1.0 block; `None` is UNKNOWN coverage.
    config_coverage:
        Option<&'a BTreeMap<String, logweir_core::backup_receipt::TopicConfigCoverage>>,
    /// PROD-05.1's 1.3.0 block; `None` is NOT RECORDED.
    topic_configuration:
        Option<&'a BTreeMap<String, logweir_core::backup_receipt::TopicConfiguration>>,
    /// PROD-05.1's `owner_detection`; `None` reads as empty.
    owner_detection: Option<&'a [String]>,
    /// PROD-03.0's 1.5.0 block; `None` is NOT ASSESSED.
    schema_dependency:
        Option<&'a BTreeMap<String, logweir_core::backup_receipt::TopicSchemaDependency>>,
    /// PROD-01.4a's 1.6.0 block; `None` is UNKNOWN.
    generations: Option<&'a BTreeMap<String, logweir_core::backup_receipt::TopicIdentity>>,
    /// PROD-04.1's 1.7.0 block; `None` when no group was selected.
    consumer_positions: Option<&'a logweir_core::consumer_positions::ConsumerPositions>,
    /// The positions document the block binds, verified; `None` when it was
    /// not given.
    positions: Option<&'a logweir_core::consumer_positions::PositionsDocument>,
}

/// What a `BackupReceipt` verdict prints.
///
/// It says which invariant set ran, in as many words, because the whole point
/// of the `SignatureOnly` line beside it is that the two exit-0s do not mean
/// the same thing. A reader who cannot tell them apart is back where the
/// honest-line work started.
fn print_backup_receipt(
    payload_type: &str,
    key_id: &str,
    backup_id: &str,
    run_id: &str,
    manifest_key: &str,
    manifest_version_id: Option<&str>,
    blocks: &ReceiptBlocks<'_>,
) {
    let config_coverage = blocks.config_coverage;
    let topic_configuration = blocks.topic_configuration;
    println!("signature: VALID  key {key_id}");
    println!("payload:   {payload_type}");
    println!("run_id:    {run_id}");
    println!("backup_id: {backup_id}");
    // The one field an auditor takes away and goes looking with. Empty is
    // legal and means the backup did not exit 0 (invariant 2), so it is
    // spelled rather than printed blank.
    println!(
        "manifest:  {}",
        if manifest_key.trim().is_empty() {
            "none — this receipt is for a backup that did not exit 0"
        } else {
            manifest_key
        }
    );
    // FX-7: on a versioned bucket, WHICH version of that key the digest is
    // over — the second thing an auditor goes looking with
    // (`?versionId=`). Absent means no version was pinned, and nothing is
    // printed rather than a placeholder that could be read as one.
    if let Some(version) = manifest_version_id {
        println!("manifest version: {version} (the object version the manifest digest is over)");
    }
    // FX-4: the configuration capture coverage, one line per topic — or the
    // line that says it was not recorded, which is UNKNOWN and never captured.
    for line in coverage_lines(config_coverage) {
        println!("coverage:  {line}");
    }
    // FX-8: the covered window of a LogAppendTime topic is its producers' time.
    for line in receipt_time_basis_lines(config_coverage) {
        println!("time:      {line}");
    }
    // PROD-05.1: the configuration model, one line per topic — or the line
    // that says it was not recorded, which is never "no configuration".
    for line in topic_configuration_lines(topic_configuration, blocks.owner_detection) {
        println!("model:     {line}");
    }
    // PROD-03.0: the schema dependency, one line per topic — or the line that
    // says it was not assessed, which is never "not schema-dependent".
    for line in schema_dependency_lines(blocks.schema_dependency) {
        println!("schema:    {line}");
    }
    // PROD-01.4a: the topic ID before and after the capture, one line per
    // topic — or the line that says it was not recorded, which is UNKNOWN and
    // never "the same generation".
    for line in generation_lines(blocks.generations) {
        println!("identity:  {line}");
    }
    // PROD-04.1: the consumer position evidence, one line per selected group.
    for line in consumer_positions_lines(blocks.consumer_positions, blocks.positions) {
        println!("groups:    {line}");
    }
    println!(
        "checked:   the signature AND all forty backup-receipt invariants \
         (format_version, exit_code/manifest_key, records/topics, covered window, \
         source.auth.mode, config_coverage's six: its version, its topic set, \
         coverage, reason, timestamp-after-a-read, timestamp value and source, \
         topic_configuration's eight: its version, beside config_coverage, its topic \
         set, entries exactly where the read succeeded, closed source and class, \
         secret and inherited, the owner, counts of at least one, \
         owner_detection's two: its closed set beside the model, an owner only from \
         a source it lists, schema_dependency's eight: its version, its topic set, \
         closed verdict, reason and basis, both sides exactly when judged, the judged \
         count against records, the schema ids, the one-in-ten threshold, and the \
         verdict from its sides, consumer_positions' six: its version, a forward \
         capture window with a closed listing and at least one group, this run's \
         positions document by a well-formed digest, outcome and reason, fields and \
         counts that fit the outcome with no captured group Dead and memberless, and a \
         derived active, and the topic IDs' five: their version, their topic set, \
         Kafka's text and never the zero ID, a reason exactly for a null ID, a source \
         exactly for a recorded one)"
    );
    if blocks.positions.is_some() {
        println!(
            "checked:   AND the positions document's fourteen (CP-1 to CP-14): bound to \
             this receipt by digest and length, its backup and run, its topic set, \
             partitions in order, well-formed marks, a derived changed flag, exactly the \
             captured groups, no position on a changed topic, no capture over an unread \
             topic, every partition accounted for (absence never offset 0), status, value \
             and reason, a derived coverage, and the receipt's counts)"
        );
    }
}

/// What a `SignatureOnly` verdict prints. Separate from `print_report` so the
/// sentence that says how much was checked is a single literal a test can
/// pin, rather than something assembled at the call site.
fn print_signature_only(payload_type: &str, key_id: &str) {
    println!("signature: VALID  key {key_id}");
    println!("payload:   {payload_type}");
    // The honest line. Exit 0 here means "the bytes are signed by this key
    // under this media type", and NOT what exit 0 means for a scorecard.
    println!(
        "checked:   the SIGNATURE only — this build evaluates no invariant \
         for this document type"
    );
}

/// What a `CatalogPoint` verdict prints: the signature, and the one check
/// of the record's own content this build makes — said in as many words, so
/// an exit 0 never reads as a claim about the point's availability or its
/// other copied facts.
fn print_catalog_point(payload_type: &str, key_id: &str) {
    println!("signature: VALID  key {key_id}");
    println!("payload:   {payload_type}");
    println!(
        "checked:   the SIGNATURE, and one check of this document type: every topic ID \
         the record copies (topics[].identity) is a real topic ID in Kafka's text. \
         Nothing else: not that the point is available, nor that its other copied facts \
         are true — verify the backup receipt it names"
    );
}

pub fn run(scorecard: &Path, signature: &Path, public_key: &Path, payload_type: &str) -> ExitCode {
    run_with(scorecard, signature, public_key, payload_type, None)
}

/// [`run`], with the positions document a backup receipt binds
/// (`--consumer-positions`, PROD-04.1).
pub fn run_with(
    scorecard: &Path,
    signature: &Path,
    public_key: &Path,
    payload_type: &str,
    positions: Option<&Path>,
) -> ExitCode {
    // Resolved BEFORE anything is read: a bad `--payload-type` is a bad
    // command line, and reporting it after a file-read failure would blame
    // the artifact for the operator's typo.
    let wanted = match resolve_payload_type(payload_type) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::Operational;
        }
    };
    match verify_scorecard_with(scorecard, signature, public_key, wanted, positions) {
        Ok(Verdict::Scorecard(r)) => {
            print_report(&r);
            ExitCode::Ok
        }
        Ok(Verdict::BackupReceipt {
            payload_type,
            key_id,
            backup_id,
            run_id,
            manifest_key,
            manifest_version_id,
            config_coverage,
            topic_configuration,
            owner_detection,
            schema_dependency,
            generations,
            consumer_positions,
            positions,
        }) => {
            print_backup_receipt(
                &payload_type,
                &key_id,
                &backup_id,
                &run_id,
                &manifest_key,
                manifest_version_id.as_deref(),
                &ReceiptBlocks {
                    config_coverage: config_coverage.as_ref(),
                    topic_configuration: topic_configuration.as_ref(),
                    owner_detection: owner_detection.as_deref(),
                    schema_dependency: schema_dependency.as_ref(),
                    generations: generations.as_ref(),
                    consumer_positions: consumer_positions.as_ref(),
                    positions: positions.as_ref(),
                },
            );
            ExitCode::Ok
        }
        Ok(Verdict::SignatureOnly {
            payload_type,
            key_id,
        }) => {
            print_signature_only(&payload_type, &key_id);
            ExitCode::Ok
        }
        Ok(Verdict::CatalogPoint {
            payload_type,
            key_id,
        }) => {
            print_catalog_point(&payload_type, &key_id);
            ExitCode::Ok
        }
        Err(c) => c,
    }
}
