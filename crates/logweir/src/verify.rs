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
    println!(
        "checked:   the signature AND all twenty-one backup-receipt invariants \
         (format_version, exit_code/manifest_key, records/topics, covered window, \
         source.auth.mode, config_coverage's six: its version, its topic set, \
         coverage, reason, timestamp-after-a-read, timestamp value and source, \
         topic_configuration's eight: its version, beside config_coverage, its topic \
         set, entries exactly where the read succeeded, closed source and class, \
         secret and inherited, the owner, counts of at least one, and \
         owner_detection's two: its closed set beside the model, an owner only from \
         a source it lists)"
    );
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

pub fn run(scorecard: &Path, signature: &Path, public_key: &Path, payload_type: &str) -> ExitCode {
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
    match verify_scorecard(scorecard, signature, public_key, wanted) {
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
        Err(c) => c,
    }
}
